defmodule BroadwayCloudPubSub.Streaming.Producer do
  @moduledoc """
  A Broadway producer that uses the gRPC StreamingPull API to receive
  messages from a Google Cloud Pub/Sub subscription.

  ## Overview

  `StreamingProducer` opens a persistent bidirectional gRPC stream to the
  Pub/Sub service and receives messages as the server pushes them. This is
  more efficient than the default HTTP Pull approach (`BroadwayCloudPubSub.Producer`)
  for workloads that require low latency or high throughput.

  ## Usage

      Broadway.start_link(MyPipeline,
        name: MyPipeline,
        producer: [
          module:
            {BroadwayCloudPubSub.Streaming.Producer,
             goth: MyApp.Goth,
             subscription: "projects/my-project/subscriptions/my-subscription",
             max_outstanding_messages: 1000}
        ],
        processors: [default: [concurrency: 10]]
      )

  ## Options

  #{NimbleOptions.docs(BroadwayCloudPubSub.Streaming.Options.definition())}

  ### Required options

    * `:subscription` - The full subscription name, e.g.
      `"projects/my-project/subscriptions/my-subscription"`.

  ### Auth options

    * `:goth` - The `Goth` module to use for authentication (e.g. `MyApp.Goth`).
    * `:token_generator` - Custom MFArgs token generator as an alternative to `:goth`.

  ### Flow control

    * `:max_outstanding_messages` - Maximum number of unacked messages the server
      will push. Defaults to 1000.
    * `:max_outstanding_bytes` - Maximum total size of unacked messages. Defaults
      to 100 MiB.

  ### Shutdown

    * `:on_shutdown` - What to do with unprocessed messages on shutdown.
      Defaults to `{:nack, 5}` (redeliver after 5 seconds).

  ## Differences from BroadwayCloudPubSub.Producer

    * **Push-based with demand signaling**: Messages arrive via a persistent gRPC
      stream. The producer tracks GenStage demand from downstream consumers and
      signals StreamManager when capacity is available. StreamManager buffers
      messages internally when demand is zero, preventing unbounded mailbox growth.
    * **Flow control**: Controlled by `max_outstanding_messages` / `max_outstanding_bytes`
      on the gRPC stream rather than by `max_number_of_messages` per HTTP request.
      This is the primary backpressure mechanism — the Pub/Sub server will not push
      more than `max_outstanding_messages` unacked messages.
    * **Shutdown**: By default, unprocessed messages are returned to Pub/Sub with a
      short delay (`on_shutdown: {:nack, 5}`), analogous to AMQP channel close behavior.

  ## Telemetry

  This producer emits the following [Telemetry](https://github.com/beam-telemetry/telemetry)
  events:

    * `[:broadway_cloud_pub_sub, :stream, :connect]` - Emitted when a gRPC
      StreamingPull connection is successfully established.

      Measurements: `%{}`

    * `[:broadway_cloud_pub_sub, :stream, :disconnect]` - Emitted when the
      gRPC stream is closed or encounters an error.

      Measurements: `%{reason: term()}`

    * `[:broadway_cloud_pub_sub, :stream, :receive_messages]` - Emitted when
      messages are received from the gRPC stream and forwarded to the producer.

      Measurements: `%{count: pos_integer()}`

    * `[:broadway_cloud_pub_sub, :stream, :ack]` - Emitted when messages are
      acknowledged on the gRPC stream.

      Measurements: `%{count: pos_integer()}`

    * `[:broadway_cloud_pub_sub, :stream, :connection_failure]` - Emitted when
      a connection attempt fails before the stream is established.

      Measurements: `%{reason: term()}`

    * `[:broadway_cloud_pub_sub, :stream, :terminal_error]` - Emitted when a
      non-retryable gRPC error is received (e.g. NOT_FOUND, PERMISSION_DENIED).
      The StreamManager will stop after this event is emitted.

      Measurements: `%{reason: term()}`

    * `[:broadway_cloud_pub_sub, :stream, :ack_buffered]` - Emitted when an
      ack/nack request is buffered because the gRPC stream is temporarily
      unavailable (e.g. during reconnection).

      Measurements: `%{buffer_size: non_neg_integer()}`

  All events include the following metadata:

    * `:name` - the Broadway topology name
    * `:subscription` - the full subscription name

  """

  use GenStage

  alias BroadwayCloudPubSub.Streaming.{StreamManager, Options}

  @behaviour Broadway.Producer

  # --- Broadway.Producer callbacks ---

  @impl Broadway.Producer
  def prepare_for_start(_module, broadway_opts) do
    {producer_module, opts} = broadway_opts[:producer][:module]

    opts =
      opts
      |> Keyword.put(:broadway, broadway_opts)
      |> validate_options!()
      |> assign_client_id()
      |> assign_token_generator()

    # Broadway will start the returned child specs under its supervisor.
    # We use the Broadway pipeline name as the StreamManager's registered name.
    broadway_name = broadway_opts[:name]
    manager_name = Module.concat(broadway_name, StreamManager)

    manager_opts = Keyword.put(opts, :name, manager_name)

    child_spec = %{
      id: StreamManager,
      start: {StreamManager, :start_link, [manager_opts]},
      restart: :permanent
    }

    {[child_spec], put_in(broadway_opts, [:producer, :module], {producer_module, opts})}
  end

  @impl GenStage
  def init(opts) do
    Process.flag(:trap_exit, true)

    config = Map.new(opts)
    ack_ref = config.broadway[:name]
    manager_name = Module.concat(ack_ref, StreamManager)
    manager_pid = Process.whereis(manager_name)

    # Tell the StreamManager our pid so it can forward messages to us
    :ok = StreamManager.set_producer(manager_pid, self())

    # Store ack config in persistent_term for acknowledger lookup
    ack_config = %{on_success: config.on_success, on_failure: config.on_failure}
    :persistent_term.put(ack_ref, {manager_pid, ack_config})

    {:producer,
     %{manager_pid: manager_pid, ack_ref: ack_ref, config: config, draining: false, demand: 0}}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    new_demand = demand + incoming_demand
    StreamManager.notify_demand(state.manager_pid, new_demand)
    {:noreply, [], %{state | demand: new_demand}}
  end

  @impl GenStage
  def handle_info({:stream_messages, messages}, %{demand: demand} = state) do
    new_demand = max(demand - length(messages), 0)
    {:noreply, messages, %{state | demand: new_demand}}
  end

  @impl GenStage
  def handle_info(_, state), do: {:noreply, [], state}

  @impl Broadway.Producer
  def prepare_for_draining(state) do
    StreamManager.stop_receiving(state.manager_pid)
    {:noreply, [], %{state | draining: true}}
  end

  @impl GenStage
  def terminate(_reason, state) do
    %{manager_pid: manager_pid, config: config} = state

    if Process.alive?(manager_pid) do
      # Nack outstanding messages per on_shutdown option
      outstanding = StreamManager.get_outstanding(manager_pid)

      case {config[:on_shutdown], outstanding} do
        {_, []} ->
          :ok

        {:noop, _} ->
          :ok

        {{:nack, delay_seconds}, ack_ids} ->
          StreamManager.modify_deadline(manager_pid, ack_ids, delay_seconds)

        {:nack, ack_ids} ->
          StreamManager.modify_deadline(manager_pid, ack_ids, 0)
      end

      # Flush buffered acks and close stream
      StreamManager.close(manager_pid)
    end

    # Clean up persistent_term
    :persistent_term.erase(state.ack_ref)

    :ok
  end

  # --- Private ---

  defp validate_options!(opts) do
    case NimbleOptions.validate(opts, Options.definition()) do
      {:ok, validated} ->
        validated

      {:error, err} ->
        raise ArgumentError, "invalid Streaming.Producer options: #{Exception.message(err)}"
    end
  end

  defp assign_client_id(opts) do
    Keyword.put_new_lazy(opts, :client_id, fn ->
      :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
    end)
  end

  defp assign_token_generator(opts) do
    if Keyword.has_key?(opts, :token_generator) do
      opts
    else
      generator = Options.make_token_generator(opts)
      Keyword.put(opts, :token_generator, generator)
    end
  end
end
