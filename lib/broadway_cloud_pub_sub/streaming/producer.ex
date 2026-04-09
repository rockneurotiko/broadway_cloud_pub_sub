defmodule BroadwayCloudPubSub.Streaming.Producer do
  @moduledoc """
  A Broadway producer that uses the gRPC StreamingPull API to receive
  messages from a Google Cloud Pub/Sub subscription.

  ## Overview

  This producer opens a persistent bidirectional gRPC stream to the Pub/Sub
  service and receives messages as the server pushes them. This is more
  efficient than the HTTP pull approach (`BroadwayCloudPubSub.Producer`) for
  workloads that require low latency or high throughput.

  The architecture has three layers:

    1. **StreamManager** (GenServer) — owns the gRPC stream and connection
       lifecycle, manages lease extensions, and dispatches messages to the
       Producer when downstream demand is available.
    2. **Producer** (GenStage) — receives messages from StreamManager and
       forwards them to Broadway processors. Tracks demand from downstream
       stages.
    3. **UnaryAckSupervisor** — supervises AckBatcher and UnaryRpcClient, which
       batch and send acknowledgement and deadline-modification requests via
       separate unary RPCs (not on the streaming connection).

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

  ## Acknowledgements

  Use `:on_success` and `:on_failure` to control how messages are acknowledged
  with Pub/Sub. Both can also be changed per-message via
  `Broadway.Message.configure_ack/2`.

  Supported values:

    * `:ack` — acknowledge the message; Pub/Sub removes it from the subscription.
    * `:noop` — do nothing; the message is redelivered after the subscription's
      `ackDeadlineSeconds` expires.
    * `:nack` — equivalent to `{:nack, 0}`; makes the message immediately
      available for redelivery.
    * `{:nack, seconds}` — sets `ackDeadlineSeconds` to `seconds` for the
      message, controlling when it becomes available for redelivery (0–600).

  Acks and deadline modifications are batched by **AckBatcher** and flushed to
  Pub/Sub via unary RPCs at a configurable interval (`:ack_batch_interval_ms`,
  default 100ms) or when the batch reaches `:ack_batch_max_size` (default 2500).
  Batching is done on a separate unary connection, independently of the
  streaming connection.

  ## Flow control

  Flow control is managed at the gRPC stream level via `:max_outstanding_messages`
  and `:max_outstanding_bytes`. The Pub/Sub server will not push more messages
  than these limits allow. This is the primary backpressure mechanism.

  StreamManager also tracks GenStage demand from the Producer and buffers
  messages internally when demand is zero, preventing unbounded mailbox growth.

  ## Lease management

  The producer automatically extends message acknowledgement deadlines before
  they expire. Leases are extended by sending `modifyAckDeadline` requests via
  the AckBatcher. The extension interval is derived from the effective ack
  deadline with randomized jitter to spread out RPC calls.

  Messages are tracked until they are acknowledged, nacked, or until
  `:max_extension_ms` elapses (default 60 minutes), after which the server
  redelivers them. This prevents a stuck consumer from holding messages
  indefinitely.

  ## Exactly-once delivery

  When the subscription has exactly-once delivery enabled, the server signals
  this via `StreamingPullResponse.subscription_properties`. The producer
  detects this automatically and enforces a minimum lease extension interval
  (the server requires at least 60 seconds between extensions for exactly-once
  subscriptions).

  For exactly-once subscriptions, increase `:retry_deadline_ms` to 600,000ms
  (10 minutes) to allow the unary RPC client enough time to retry transient
  ack failures — the server requires successful ack receipt before guaranteeing
  exactly-once semantics.

  ## Message ordering

  Set `enable_message_ordering: true` to route messages with the same
  `ordering_key` to the same Broadway processor, ensuring sequential processing
  per key. The subscription must also have message ordering enabled in Pub/Sub.

  Ordering is enforced via Broadway's `:partition_by` option, which is
  automatically injected into all processor groups when this option is set.

  ## Graceful shutdown

  On shutdown, the producer:

    1. Nacks all buffered messages (received but not yet dispatched to
       processors) per the `:on_shutdown` option (default `{:nack, 5}`).
    2. Stops the gRPC stream to prevent new messages from arriving.
    3. Waits up to `:drain_timeout_ms` (default 30s) for in-flight messages
       (dispatched to processors but not yet acked/nacked) to be processed.
    4. Force-closes the stream after the drain timeout.

  ## Error handling

  gRPC stream errors are classified as retryable or terminal:

    * **Retryable** (e.g. `DEADLINE_EXCEEDED`, `UNAVAILABLE`, `UNAUTHENTICATED`) —
      the stream is closed and reconnected after a backoff delay. A new OAuth2
      token is fetched on each reconnect.
    * **Terminal** (e.g. `NOT_FOUND`, `PERMISSION_DENIED`, `INVALID_ARGUMENT`) —
      the StreamManager stops and Broadway's supervision restarts the pipeline.

  Reconnect backoff is configurable via `:backoff_type`, `:backoff_min`, and
  `:backoff_max`. The default is randomized exponential (`:rand_exp`) starting
  at 100ms and capped at 60s.

  ## Telemetry

  This producer emits the following [Telemetry](https://github.com/beam-telemetry/telemetry)
  events. All events share the top-level prefix `[:broadway_cloud_pub_sub, :streaming]`,
  followed by a layer sub-prefix.

  All event metadata maps include an `:extra` key when the `:telemetry_metadata` option
  is configured. Its value is the static term provided, or the return value of the MFA
  called at emission time.

  ### Stream events — `[:broadway_cloud_pub_sub, :streaming, :stream, ...]`

  Emitted by `StreamManager`. Metadata: `%{name: broadway_name, subscription: subscription}`
  (plus `:extra` when `:telemetry_metadata` is set).

    * `:connect` — gRPC StreamingPull stream successfully established.

      Measurements: `%{}`

    * `:disconnect` — gRPC stream closed or errored.

      Measurements: `%{}`

      Metadata includes: `reason: term()` — the error or close reason
      (e.g. a `GRPC.RPCError`, `:stream_closed`, `:connection_down`).

    * `:receive_messages` — messages received from the stream and forwarded to
      the producer.

      Measurements: `%{count: pos_integer()}`

    * `:ack` — acknowledge request dispatched to AckBatcher.

      Measurements: `%{count: pos_integer()}`

    * `:terminal_error` — non-retryable gRPC error received. StreamManager stops
      after this event.

      Measurements: `%{}`

      Metadata includes: `reason: term()` — the terminal gRPC error.

    * `:connection_failure` — connection attempt failed before the stream was
      established.

      Measurements: `%{}`

      Metadata includes: `reason: term()` — the connection error.

    * `:keepalive` — keep-alive ping sent on the gRPC connection.

      Measurements: `%{deadline: pos_integer()}`

    * `:extend_leases` — lease extension cycle ran; modack requests dispatched
      for outstanding messages.

      Measurements: `%{count: non_neg_integer(), deadline: pos_integer()}`

    * `:lease_expired` — outstanding messages dropped because they exceeded
      `:max_extension_ms`.

      Measurements: `%{count: pos_integer()}`

    * `:drain_timeout` — graceful shutdown drain timed out before all in-flight
      messages were processed.

      Measurements: `%{}`

    * `:drain_complete` — all in-flight messages were processed before the drain
      timeout; stream closed cleanly.

      Measurements: `%{}`

    * `:reconnect` — reconnect scheduled after a retryable stream error.

      Measurements: `%{delay: non_neg_integer()}`

    * `:receipt_modack_stale` — pending exactly-once receipt modacks swept out
      as stale and nacked for fast redelivery.

      Measurements: `%{count: pos_integer()}`

  ### AckBatcher events — `[:broadway_cloud_pub_sub, :streaming, :ack_batcher, ...]`

  Emitted by `AckBatcher`. Metadata: `%{name: broadway_name, subscription: subscription}`
  (plus `:extra` when `:telemetry_metadata` is set).

    * `:flush_deferred` — flush deferred because UnaryRpcClient was not yet
      available (e.g. restarting after a crash).

      Measurements: `%{ack_count: non_neg_integer(), modack_groups: non_neg_integer()}`

    * `:modack_retry_exhausted` — modack ack_ids dropped after reaching the
      maximum retry attempt count.

      Measurements: `%{count: pos_integer()}`

    * `:ack_retry_expired` — ack ack_ids dropped because they exceeded the
      exactly-once retry deadline.

      Measurements: `%{count: pos_integer()}`

    * `:modack_retry_expired` — modack ack_ids dropped because they exceeded the
      exactly-once retry deadline.

      Measurements: `%{count: pos_integer()}`

  ### Unary RPC client events — `[:broadway_cloud_pub_sub, :streaming, :unary, ...]`

  Emitted by `UnaryRpcClient`. Metadata: `%{name: broadway_name, subscription: subscription}`
  (plus `:extra` when `:telemetry_metadata` is set).

    * `:connect` — unary RPC channel reconnected after a failure.

      Measurements: `%{}`

    * `:connection_failure` — unary RPC channel connect attempt failed.

      Measurements: `%{}`

      Metadata includes: `reason: term()` — the connection error.

    * `:permanent_failure` — one or more ack_ids were permanently rejected by
      the server (e.g. ack_id expired). These are dropped and not retried.

      Measurements: `%{count: pos_integer()}`

  ### gRPC client spans — `[:broadway_cloud_pub_sub, :streaming, :grpc_client, ...]`

  Emitted by `GrpcClient` (the default `BroadwayCloudPubSub.Streaming.Client`
  implementation) as `:telemetry.span/3` spans.
  Metadata: `%{name: broadway_name, subscription: subscription, count: ack_count}`
  (plus `:extra` when `:telemetry_metadata` is set).

    * `:ack` — wraps each `Acknowledge` unary RPC call.

      Events: `[:broadway_cloud_pub_sub, :streaming, :grpc_client, :ack, :start | :stop | :exception]`

    * `:modack` — wraps each `ModifyAckDeadline` unary RPC call.

      Events: `[:broadway_cloud_pub_sub, :streaming, :grpc_client, :modack, :start | :stop | :exception]`

  ## Pub/Sub Emulator

  To use with the local Pub/Sub emulator:

      {BroadwayCloudPubSub.Streaming.Producer,
       subscription: "projects/my-project/subscriptions/my-subscription",
       grpc_endpoint: "localhost:8085",
       use_ssl: false,
       token_generator: {MyApp, :emulator_token, []}}

  ## Differences from `BroadwayCloudPubSub.Producer`

    * **Push-based**: Messages arrive via a persistent gRPC stream rather than
      being fetched on demand via HTTP pull requests.
    * **Flow control**: Controlled by `:max_outstanding_messages` and
      `:max_outstanding_bytes` on the gRPC stream level, rather than by
      `:max_number_of_messages` per pull request.
    * **Shutdown behaviour**: By default, unprocessed messages are returned to
      Pub/Sub with a short delay (`on_shutdown: {:nack, 5}`) so they are
      redelivered quickly on rolling deploys. The pull producer does not nack
      on shutdown.
    * **Ack path**: Acks are batched and sent via a separate unary RPC
      connection managed by AckBatcher and UnaryRpcClient, not on the streaming
      connection itself.
    * **Lease extension**: The streaming producer automatically extends message
      deadlines to prevent redelivery while messages are being processed. The
      pull producer relies on the subscription-level ack deadline only.

  """

  use GenStage

  alias BroadwayCloudPubSub.Streaming.{StreamManager, UnaryAckSupervisor, Options}

  @behaviour Broadway.Producer

  # --- Broadway.Producer callbacks ---

  @impl Broadway.Producer
  def prepare_for_start(_module, broadway_opts) do
    {producer_module, opts} = broadway_opts[:producer][:module]

    opts =
      opts
      |> Keyword.put(:broadway, broadway_opts)
      |> Keyword.put(:broadway_name, broadway_opts[:name])
      |> validate_options!()
      |> assign_client_id()
      |> assign_token_generator()

    broadway_name = opts[:broadway_name]

    # Add grpc_client_config to be used by stream manager and unary
    grpc_client = opts[:grpc_client]
    {:ok, client_config} = grpc_client.init(opts)

    opts = Keyword.put(opts, :grpc_client_config, client_config)

    # UnaryAckSupervisor options
    unary_name = Module.concat(broadway_name, UnaryAckSupervisor)
    unary_opts = Keyword.put(opts, :name, unary_name)

    unary_sup_spec = %{
      id: unary_name,
      start: {UnaryAckSupervisor, :start_link, [unary_opts]},
      restart: :permanent,
      type: :supervisor
    }

    # StreamManager options
    stream_manager_name = Module.concat(broadway_name, StreamManager)
    manager_opts = Keyword.put(opts, :name, stream_manager_name)

    manager_spec = %{
      id: stream_manager_name,
      start: {StreamManager, :start_link, [manager_opts]},
      restart: :permanent
    }

    # Broadway options
    options =
      broadway_opts
      |> put_in([:producer, :module], {producer_module, opts})
      |> maybe_inject_partition_by(opts)

    # UnaryAckSupervisor is listed first so it starts before StreamManager,
    # ensuring AckBatcher is alive when the first acks are dispatched.
    {[unary_sup_spec, manager_spec], options}
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

    # Store the manager's *registered name* (not its PID) in persistent_term so
    # the Acknowledger can route acks even after a StreamManager restart. PIDs
    # become stale on restart; names always resolve to the current process.
    ack_config = %{on_success: config.on_success, on_failure: config.on_failure}
    :persistent_term.put(ack_ref, {manager_name, ack_config})

    {:producer,
     %{
       manager_pid: manager_pid,
       manager_name: manager_name,
       ack_ref: ack_ref,
       config: config,
       draining: false,
       demand: 0
     }}
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
    %{manager_pid: manager_pid, config: config} = state

    # Nack buffered messages (not yet dispatched to processors) per on_shutdown config.
    buffered = StreamManager.get_buffered(manager_pid)
    nack_ack_ids(manager_pid, config, buffered)

    # Stop receiving new messages and begin the drain phase.
    StreamManager.stop_receiving(manager_pid)

    {:noreply, [], %{state | draining: true}}
  end

  @impl GenStage
  def terminate(_reason, state) do
    %{manager_pid: manager_pid, config: config} = state

    if Process.alive?(manager_pid) do
      # Nack any messages still in outstanding so they are redelivered promptly
      # instead of waiting for their ack deadline to expire naturally. This
      # covers edge cases like on_failure: :noop (acknowledger does nothing) or
      # the drain timeout firing before all processors complete.
      outstanding = StreamManager.get_outstanding(manager_pid)
      nack_ack_ids(manager_pid, config, outstanding)

      StreamManager.close(manager_pid)
    end

    :persistent_term.erase(state.ack_ref)

    :ok
  end

  # --- Private ---

  # Nack a list of ack_ids per the on_shutdown config. Used by both
  # prepare_for_draining (for buffered messages) and terminate (for
  # remaining outstanding messages).
  defp nack_ack_ids(_manager_pid, _config, []), do: :ok
  defp nack_ack_ids(_manager_pid, %{on_shutdown: :noop}, _ack_ids), do: :ok

  defp nack_ack_ids(manager_pid, %{on_shutdown: {:nack, delay_seconds}}, ack_ids) do
    StreamManager.modify_deadline(manager_pid, ack_ids, delay_seconds)
  end

  defp validate_options!(opts) do
    case NimbleOptions.validate(opts, Options.definition()) do
      {:ok, validated} ->
        # Cross-field validation: backoff_min must not exceed backoff_max.
        # NimbleOptions validates each field independently but cannot express
        # relationships between fields.
        min = validated[:backoff_min]
        max = validated[:backoff_max]

        if min > max do
          raise ArgumentError,
                "invalid Streaming.Producer options: :backoff_min (#{min}) must be <= :backoff_max (#{max})"
        end

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

  # When enable_message_ordering is true, inject a :partition_by function into
  # each processor group so that messages with the same ordering_key are always
  # routed to the same processor (ensuring sequential processing per key).
  #
  # Broadway's :partition_by option accepts a function that takes a Broadway.Message
  # and returns a partition key. Broadway hashes the key and routes all messages
  # with the same hash to the same processor stage. Messages with an empty or nil
  # ordering_key are all routed to partition 0 (unordered messages interleave freely).
  defp maybe_inject_partition_by(broadway_opts, opts) do
    if opts[:enable_message_ordering] do
      processors =
        broadway_opts
        |> Keyword.get(:processors, [])
        |> Enum.map(fn {name, proc_opts} ->
          {name, Keyword.put_new(proc_opts, :partition_by, &__MODULE__.partition_by/1)}
        end)

      Keyword.put(broadway_opts, :processors, processors)
    else
      broadway_opts
    end
  end

  def partition_by(%Broadway.Message{metadata: %{orderingKey: key}}) when is_binary(key) do
    :erlang.phash2(key)
  end

  def partition_by(%Broadway.Message{metadata: %{orderingKey: key}}) when is_integer(key) do
    key
  end

  def partition_by(_) do
    :erlang.unique_integer([:positive])
  end
end
