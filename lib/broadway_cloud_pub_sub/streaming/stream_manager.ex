defmodule BroadwayCloudPubSub.Streaming.StreamManager do
  @moduledoc false

  # GenServer that owns the gRPC bidirectional StreamingPull connection.
  # Responsibilities:
  #   - Connect and reconnect with exponential backoff
  #   - Receive messages from the stream and forward them to the producer
  #   - Route ack/modifyAckDeadline requests to AckBatcher, which sends them
  #     as unary RPCs via UnaryRpcClient (independent of this stream)
  #   - Track outstanding (delivered but not acked) message ack_ids for
  #     lease management and shutdown nacking
  #   - Extend message leases periodically via modifyAckDeadline (through AckBatcher)
  #   - Buffer incoming messages when the producer has no pending demand
  #     (demand-based backpressure via notify_demand/1)
  #   - Send keep-alive pings every 30s to prevent server idle timeout
  #
  # Backpressure design:
  #   The producer calls notify_demand/2 whenever Broadway signals new demand,
  #   passing the total accumulated demand count. StreamManager keeps a
  #   `pending_demand` integer. When `pending_demand` is 0, incoming messages
  #   are stored in `message_buffer` instead of being forwarded. On each
  #   notify_demand/2 or incoming gRPC batch, up to `pending_demand` messages
  #   are flushed to the producer with the rest remaining buffered. The buffer
  #   is naturally bounded by `max_outstanding_messages` (the Pub/Sub server
  #   will not push more unacked messages than that limit).
  #
  # gRPC streaming:
  #   A dedicated `StreamReader` process owns the gRPC stream for both the
  #   Gun and Mint adapters. The reader calls `GRPC.Stub.recv/2` and forwards
  #   decoded messages back as `{:stream_messages, msgs}`. See `StreamReader`
  #   for a detailed explanation of why a separate process is needed.
  #
  # Keep-alive pings:
  #   Google's servers close idle StreamingPull connections after ~60 seconds
  #   of inactivity. We send an empty StreamingPullRequest every 30 seconds to
  #   prevent this via the :send_keepalive timer.
  #
  # Reconnect deduplication:
  #   Multiple events can arrive close together on a disconnect — e.g.
  #   {:stream_error} followed by {:stream_closed} or an {:EXIT} signal.
  #   We track the pending reconnect timer ref in `reconnect_ref` and skip
  #   scheduling if one is already set.
  #
  # Error classification:
  #   gRPC errors are classified as :retryable (reconnect) or :terminal (stop).
  #   Terminal errors (NOT_FOUND, PERMISSION_DENIED, etc.) indicate a permanent
  #   misconfiguration. The GenServer stops and Broadway's supervision restarts it.

  use GenServer
  require Logger

  alias BroadwayCloudPubSub.{Backoff, MessageBuilder}

  alias BroadwayCloudPubSub.Streaming.{
    AckBatcher,
    AckTimeDistribution,
    ErrorClassifier,
    StreamReader
  }

  alias Google.Pubsub.V1.StreamingPullRequest

  # Default keep-alive interval. The server's inactivity timeout is ~60s;
  # pinging at half that prevents closure.
  @default_keepalive_ms 30_000

  @default_drain_timeout_ms 30_000

  # Grace period (seconds) subtracted from the adaptive deadline to compute the
  # lease extension interval. Ensures the modack reaches the server before the
  # current deadline expires.
  @grace_period_seconds 5

  # Minimum ack deadline for exactly-once delivery mode.
  @min_deadline_exactly_once_seconds 60

  defstruct [
    :producer_pid,
    :config,
    :channel,
    :grpc_stream,
    :conn_pid,
    # Pid of the linked StreamReader process that enumerates GRPC.Stub.recv/2
    :reader_pid,
    :backoff,
    :lease_timer,
    # Distribution for tracking message processing times, used to compute the
    # adaptive p99 ack deadline.
    :ack_time_dist,
    :receiving,
    # Timer ref for the pending :connect message. Non-nil means a reconnect is
    # already scheduled — prevents double-scheduling from multiple close signals.
    :reconnect_ref,
    # Timer ref for the periodic :send_keepalive message.
    :keepalive_timer,
    # Registered name of the AckBatcher (not PID) so we survive UnaryAckSupervisor
    # restarts within a supervision cycle.
    :ack_batcher,
    # Whether the producer has asked us to stop (prepare_for_draining called).
    # When true, new incoming messages from the stream are ignored and we close
    # the reader immediately.
    draining: false,
    # Timer ref for the drain timeout. Non-nil means we are waiting for in-flight
    # messages to be acked before closing the stream.
    drain_timer: nil,
    # Whether the subscription has message ordering enabled, as reported by the
    # server in StreamingPullResponse.subscription_properties.
    # Updated dynamically on each response that includes subscription_properties.
    ordering_enabled: false,
    # Whether the subscription has exactly-once delivery enabled, as reported by the
    # server in StreamingPullResponse.subscription_properties.
    # When true, the minimum ack deadline extension is raised from 10s to 60s.
    # Updated dynamically on each response that includes subscription_properties.
    exactly_once_enabled: false,
    # Map of ack_id => %{received_at: monotonic_ms, max_expiry: monotonic_ms}
    # for outstanding (delivered but not yet acked) messages.
    # received_at is used to compute processing duration for the adaptive p99 deadline.
    # max_expiry marks the absolute wall time beyond which we stop extending the lease.
    outstanding: %{},
    # Messages buffered while the producer has no pending demand.
    # Stored as an Erlang :queue for O(1) enqueue and O(1) dequeue.
    # Naturally bounded by max_outstanding_messages (server-side flow control).
    message_buffer: :queue.new(),
    # How many messages the producer can currently accept.
    # Refreshed on each notify_demand/2; decremented when messages are flushed.
    pending_demand: 0
  ]

  # --- Public API ---

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)

    if name do
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      GenServer.start_link(__MODULE__, opts)
    end
  end

  @doc """
  Sets the producer pid. Called by `StreamingProducer.init/1` after the producer
  process starts (after Broadway has started both the StreamManager child and the
  producer process).
  """
  @spec set_producer(GenServer.server(), pid()) :: :ok
  def set_producer(server, producer_pid) do
    GenServer.call(server, {:set_producer, producer_pid})
  end

  @doc """
  Acknowledge (ack) a list of ack_ids. Called by StreamingAcknowledger.
  """
  @spec acknowledge(pid(), [String.t()]) :: :ok
  def acknowledge(pid, ack_ids) when is_list(ack_ids) do
    GenServer.cast(pid, {:acknowledge, ack_ids})
  end

  @doc """
  Modify ack deadline for a list of ack_ids. Used for nack and lease extension.
  """
  @spec modify_deadline(pid(), [String.t()], non_neg_integer()) :: :ok
  def modify_deadline(pid, ack_ids, deadline_seconds) when is_list(ack_ids) do
    GenServer.cast(pid, {:modify_deadline, ack_ids, deadline_seconds})
  end

  @doc """
  Tells the StreamManager to stop forwarding new messages to the producer.
  Called during `prepare_for_draining`. The gRPC stream stays open so
  in-flight acks can still be delivered.
  """
  @spec stop_receiving(pid()) :: :ok
  def stop_receiving(pid) do
    GenServer.call(pid, :stop_receiving)
  end

  @doc """
  Returns all currently outstanding ack_ids (received but not yet acked/nacked).
  Used in `terminate/2` to nack unprocessed messages per the `on_shutdown` option.
  """
  @spec get_outstanding(pid()) :: [String.t()]
  def get_outstanding(pid) do
    GenServer.call(pid, :get_outstanding)
  end

  @doc """
  Flushes any buffered acks and closes the gRPC stream gracefully.
  Called from the producer's `terminate/2`.
  """
  @spec close(pid()) :: :ok
  def close(pid) do
    GenServer.call(pid, :close, 10_000)
  end

  @doc """
  Returns the ack_ids of messages that are buffered in StreamManager but have
  not yet been dispatched to Broadway processors. These are messages received
  from the gRPC stream that are waiting for demand.

  Called from the producer's `prepare_for_draining/1` to nack buffered messages
  during graceful shutdown before they are delivered to the pipeline.
  """
  @spec get_buffered(pid()) :: [String.t()]
  def get_buffered(pid) do
    GenServer.call(pid, :get_buffered)
  end

  @doc """
  Signals the current demand from the producer. The `amount` is the producer's
  total accumulated demand (not a delta). The StreamManager uses it as an upper
  bound for how many buffered messages to flush immediately.

  Called by `Streaming.Producer.handle_demand/2`.
  """
  @spec notify_demand(pid(), non_neg_integer()) :: :ok
  def notify_demand(pid, amount) when is_integer(amount) and amount >= 0 do
    GenServer.cast(pid, {:demand_available, amount})
  end

  # --- GenServer callbacks ---

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)
    config = Map.new(opts)

    backoff =
      Backoff.new(
        type: config.backoff_type,
        min: config.backoff_min,
        max: config.backoff_max
      )

    ack_batcher = Module.concat(config.broadway_name, AckBatcher)

    state = %__MODULE__{
      producer_pid: nil,
      config: config,
      backoff: backoff,
      ack_time_dist: AckTimeDistribution.new(config.stream_ack_deadline_seconds),
      ack_batcher: ack_batcher,
      receiving: true,
      pending_demand: 0
    }

    # Delay connecting until producer tells us its pid via set_producer/2
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:connect, state) do
    state = %{state | reconnect_ref: nil}

    case connect(state) do
      {:ok, new_state} ->
        {:noreply, new_state}

      {:error, reason, new_state} ->
        emit_telemetry(:connection_failure, %{reason: reason}, state.config)
        {:noreply, schedule_reconnect(new_state)}
    end
  end

  # The StreamReader successfully opened the gRPC stream and sends us the
  # stream struct so we can call send_request for acks and lease extensions.
  def handle_info({:stream_opened, reader_pid, grpc_stream}, %{reader_pid: reader_pid} = state) do
    conn_pid = grpc_stream.channel.adapter_payload.conn_pid
    backoff = Backoff.reset(state.backoff)

    state = %{
      state
      | grpc_stream: grpc_stream,
        conn_pid: conn_pid,
        backoff: backoff
    }

    state = schedule_lease_timer(state)
    state = schedule_keepalive_timer(state)
    emit_telemetry(:connect, %{}, state.config)
    {:noreply, state}
  end

  # Stale :stream_opened from a previous reader (race during reconnect) — ignore.
  def handle_info({:stream_opened, _pid, _stream}, state) do
    {:noreply, state}
  end

  # Decoded messages forwarded from the StreamReader
  def handle_info({:stream_messages, messages}, state) do
    if state.receiving and messages != [] do
      broadway_messages = Enum.map(messages, &build_broadway_message(&1, state))
      ack_ids = Enum.map(messages, & &1.ack_id)

      now = now_ms()
      max_extension_ms = state.config.max_extension_ms

      new_outstanding =
        Enum.reduce(ack_ids, state.outstanding, fn ack_id, acc ->
          Map.put(acc, ack_id, %{received_at: now, max_expiry: now + max_extension_ms})
        end)

      # Receipt modack: immediately extend the ack deadline with the current adaptive
      # p99 value. This synchronises the server-side timer with the client-side timer,
      # compensating for network latency between when the server sent the message and
      # when we received it. Matches Go's receiptTicker and Python's receipt modack.
      # Sent as a unary RPC via AckBatcher — independent of the bidi stream.
      adaptive_deadline = AckTimeDistribution.percentile(state.ack_time_dist, 0.99)
      AckBatcher.modack(state.ack_batcher, ack_ids, adaptive_deadline)

      emit_telemetry(:receive_messages, %{count: length(broadway_messages)}, state.config)
      {:noreply, deliver_messages(%{state | outstanding: new_outstanding}, broadway_messages)}
    else
      {:noreply, state}
    end
  end

  # Subscription properties update forwarded from the StreamReader.
  # The server sends these in StreamingPullResponse.subscription_properties on
  # any response (including heartbeats) when the subscription's settings change.
  def handle_info(
        {:subscription_properties,
         %{
           message_ordering_enabled: ordering_enabled,
           exactly_once_delivery_enabled: exactly_once_enabled
         } = _props},
        state
      ) do
    {:noreply,
     %{state | ordering_enabled: ordering_enabled, exactly_once_enabled: exactly_once_enabled}}
  end

  # Stream-level gRPC error reported by the StreamReader.
  # Classify: retryable errors trigger reconnect; terminal errors stop the GenServer.
  def handle_info({:stream_error, error}, state) do
    case ErrorClassifier.classify(error) do
      :terminal ->
        Logger.error("Terminal Cloud Pub/Sub gRPC error — stopping: #{inspect(error)}")

        emit_telemetry(:terminal_error, %{reason: error}, state.config)
        {:stop, {:terminal_error, error}, close_stream(state)}

      :retryable ->
        emit_telemetry(:disconnect, %{reason: error}, state.config)
        {:noreply, schedule_reconnect(reset_connection(state, error))}
    end
  end

  # Server closed the stream normally (StreamReader enumeration exhausted)
  def handle_info({:stream_closed}, state) do
    emit_telemetry(:disconnect, %{reason: :stream_closed}, state.config)

    # The stream ended naturally: the Mint ConnectionProcess already called
    # StreamResponseProcess.done/1 and popped the request_ref from its state
    # when it received the HTTP/2 END_STREAM frame.  Calling GRPC.Stub.cancel
    # now would make the ConnectionProcess try to send :done to the
    # already-stopped StreamResponseProcess, crashing the ConnectionProcess.
    # Nil out grpc_stream so close_stream/1 skips the cancel for this case.
    state = %{state | grpc_stream: nil}

    if state.draining do
      # Mid-drain: do not open a new stream; just clean up reader/channel.
      {:noreply, reset_connection(state, :stream_closed)}
    else
      {:noreply, schedule_reconnect(reset_connection(state, :stream_closed))}
    end
  end

  # StreamReader process exited normally — stream ended cleanly.
  # {:stream_closed} is sent before the exit, so this is a duplicate signal.
  # We only reconnect if grpc_stream is still set (meaning the stream_closed
  # message wasn't processed first).
  def handle_info({:EXIT, pid, :normal}, %{reader_pid: pid} = state) do
    if state.grpc_stream do
      emit_telemetry(:disconnect, %{reason: :stream_closed}, state.config)

      # Same rationale as {:stream_closed}: stream ended naturally, skip cancel.
      state = %{state | grpc_stream: nil}

      if state.draining do
        {:noreply, reset_connection(state, :stream_closed)}
      else
        {:noreply, schedule_reconnect(reset_connection(state, :stream_closed))}
      end
    else
      # Already handled by {:stream_closed} — just clear the reader_pid
      {:noreply, %{state | reader_pid: nil}}
    end
  end

  # StreamReader process crashed — reconnect
  def handle_info({:EXIT, pid, reason}, %{reader_pid: pid} = state) do
    emit_telemetry(:disconnect, %{reason: reason}, state.config)
    {:noreply, schedule_reconnect(reset_connection(state, reason))}
  end

  # Catch-all for other EXIT signals (e.g. from the supervisor during shutdown)
  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  def handle_info(:extend_leases, state) do
    now = now_ms()
    adaptive_deadline = AckTimeDistribution.percentile(state.ack_time_dist, 0.99)

    # When exactly-once delivery is enabled, enforce a higher minimum deadline of 60s.
    effective_deadline =
      if state.exactly_once_enabled do
        max(adaptive_deadline, @min_deadline_exactly_once_seconds)
      else
        adaptive_deadline
      end

    # Partition into still-valid (before max_expiry) and expired (past max_expiry).
    # Expired messages are dropped from lease management — the server will redeliver them.
    {valid, expired} =
      Map.split_with(state.outstanding, fn {_id, info} -> info.max_expiry > now end)

    if map_size(expired) > 0 do
      emit_telemetry(:lease_expired, %{count: map_size(expired)}, state.config)
    end

    emit_telemetry(
      :extend_leases,
      %{count: map_size(valid), deadline: effective_deadline},
      state.config
    )

    if map_size(valid) > 0 do
      AckBatcher.modack(state.ack_batcher, Map.keys(valid), effective_deadline)
    end

    # Schedule next tick: (effective_deadline - grace_period) with jitter, minimum 1s.
    # Jitter factor in [0.8, 0.9) prevents all StreamManagers from extending in lockstep.
    base_interval_ms = max(1_000, (effective_deadline - @grace_period_seconds) * 1_000)
    jitter_factor = 0.8 + :rand.uniform() * 0.1
    next_interval_ms = round(base_interval_ms * jitter_factor)
    timer = Process.send_after(self(), :extend_leases, next_interval_ms)
    {:noreply, %{state | outstanding: valid, lease_timer: timer}}
  end

  # Periodic keep-alive ping: send an empty StreamingPullRequest to prevent the
  # server from closing an idle stream. The server's inactivity timeout is ~60s.
  def handle_info(:send_keepalive, %{grpc_stream: nil} = state) do
    {:noreply, state}
  end

  def handle_info(:send_keepalive, state) do
    adaptive_deadline = AckTimeDistribution.percentile(state.ack_time_dist, 0.99)
    keepalive_request = %StreamingPullRequest{stream_ack_deadline_seconds: adaptive_deadline}

    case send_on_stream(state.grpc_stream, keepalive_request) do
      {:ok, stream} ->
        emit_telemetry(:keepalive, %{deadline: adaptive_deadline}, state.config)
        timer = schedule_keepalive_after(state.config)
        {:noreply, %{state | grpc_stream: stream, keepalive_timer: timer}}

      {:error, reason} ->
        {:noreply, schedule_reconnect(reset_connection(state, {:send_failed, reason}))}
    end
  end

  # Mint adapter signals connection loss to its parent process.
  def handle_info({:elixir_grpc, :connection_down, conn_pid}, %{conn_pid: conn_pid} = state) do
    emit_telemetry(:disconnect, %{reason: :connection_down}, state.config)
    {:noreply, schedule_reconnect(reset_connection(state, :connection_down))}
  end

  # Gun adapter signals connection loss via :gun_down messages.
  def handle_info(
        {:gun_down, conn_pid, _protocol, _reason, _killed_streams},
        %{conn_pid: conn_pid} = state
      ) do
    emit_telemetry(:disconnect, %{reason: :connection_down}, state.config)
    {:noreply, schedule_reconnect(reset_connection(state, :connection_down))}
  end

  def handle_info(:drain_timeout, state) do
    emit_telemetry(:drain_timeout, %{}, state.config)
    state = close_stream(%{state | drain_timer: nil})
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:acknowledge, ack_ids}, state) do
    now = now_ms()

    # Record processing times for the adaptive p99 deadline calculation.
    ack_time_dist =
      Enum.reduce(ack_ids, state.ack_time_dist, fn ack_id, dist ->
        case Map.get(state.outstanding, ack_id) do
          %{received_at: received_at} ->
            duration_s = max(1, div(now - received_at, 1_000))
            AckTimeDistribution.record(dist, duration_s)

          nil ->
            dist
        end
      end)

    new_outstanding = Enum.reduce(ack_ids, state.outstanding, &Map.delete(&2, &1))
    state = %{state | outstanding: new_outstanding, ack_time_dist: ack_time_dist}

    AckBatcher.ack(state.ack_batcher, ack_ids)
    emit_telemetry(:ack, %{count: length(ack_ids)}, state.config)

    {:noreply, maybe_complete_drain(state)}
  end

  def handle_cast({:modify_deadline, ack_ids, deadline_seconds}, state) do
    now = now_ms()

    # On nack (deadline == 0), record processing times and remove from outstanding
    # so they are not lease-extended further. On non-zero deadline changes,
    # keep the ack_ids in outstanding unchanged.
    {new_outstanding, ack_time_dist} =
      if deadline_seconds == 0 do
        dist =
          Enum.reduce(ack_ids, state.ack_time_dist, fn ack_id, acc ->
            case Map.get(state.outstanding, ack_id) do
              %{received_at: received_at} ->
                duration_s = max(1, div(now - received_at, 1_000))
                AckTimeDistribution.record(acc, duration_s)

              nil ->
                acc
            end
          end)

        outstanding = Enum.reduce(ack_ids, state.outstanding, &Map.delete(&2, &1))
        {outstanding, dist}
      else
        {state.outstanding, state.ack_time_dist}
      end

    state = %{state | outstanding: new_outstanding, ack_time_dist: ack_time_dist}

    AckBatcher.modack(state.ack_batcher, ack_ids, deadline_seconds)

    {:noreply, maybe_complete_drain(state)}
  end

  # The producer signals its current total demand. Update pending_demand and
  # flush up to that many buffered messages to the producer.
  def handle_cast({:demand_available, amount}, state) do
    state = %{state | pending_demand: amount}
    {:noreply, flush_demand(state)}
  end

  @impl GenServer
  def handle_call({:set_producer, producer_pid}, _from, state) do
    state = %{state | producer_pid: producer_pid}
    send(self(), :connect)
    {:reply, :ok, state}
  end

  def handle_call(:stop_receiving, _from, state) do
    # Close the reader so no new messages arrive; keep the channel open for AckBatcher.
    state = close_reader(state)
    state = start_drain_timer(state)
    {:reply, :ok, %{state | receiving: false, draining: true}}
  end

  def handle_call(:get_outstanding, _from, state) do
    {:reply, Map.keys(state.outstanding), state}
  end

  def handle_call(:get_buffered, _from, state) do
    ack_ids =
      state.message_buffer
      |> :queue.to_list()
      |> Enum.map(fn %Broadway.Message{acknowledger: {_, _, %{ack_id: id}}} -> id end)

    {:reply, ack_ids, state}
  end

  def handle_call(:close, _from, state) do
    # Best-effort flush before closing. Guard against AckBatcher already being
    # dead during pipeline shutdown (Broadway stops children in reverse start order).
    flush_batcher_if_alive(state.ack_batcher)

    state = close_stream(state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    state
    |> cancel_lease_timer()
    |> cancel_keepalive_timer()
    |> cancel_drain_timer()
    |> close_stream()

    :ok
  end

  # --- Private: connection ---

  defp connect(%{config: config} = state) do
    with {:ok, token} <- fetch_token(config),
         {:ok, channel} <- open_channel(config, token) do
      connect_stream(channel, state)
    else
      {:error, reason} -> {:error, reason, state}
    end
  rescue
    e ->
      {:error, {:connect_failed, Exception.message(e)}, state}
  end

  # Opens the gRPC channel and spawns the StreamReader, which will open the
  # stream and send {:stream_opened, reader_pid, grpc_stream} back to us.
  defp connect_stream(channel, state) do
    with {:ok, reader_pid} <- StreamReader.start_link(self(), channel, state.config) do
      {:ok,
       %{
         state
         | channel: channel,
           reader_pid: reader_pid,
           grpc_stream: nil,
           conn_pid: nil
       }}
    end
  rescue
    e ->
      try do
        GRPC.Stub.disconnect(channel)
      catch
        _, _ -> :ok
      end

      {:error, {:connect_failed, Exception.message(e)}, state}
  end

  defp open_channel(
         %{grpc_endpoint: endpoint, use_ssl: use_ssl, adapter: adapter} = config,
         token
       ) do
    keepalive_interval_ms = Map.get(config, :keepalive_interval_ms, 30_000)

    adapter_opts = [http2_opts: %{keepalive: keepalive_interval_ms, settings_timeout: :infinity}]

    adapter_opts =
      case Map.get(config, :test_pid) do
        nil -> adapter_opts
        pid -> Keyword.put(adapter_opts, :test_pid, pid)
      end

    base_opts = [
      adapter: adapter,
      headers: [{"authorization", "Bearer #{token}"}],
      adapter_opts: adapter_opts
    ]

    opts =
      if use_ssl do
        cred = GRPC.Credential.new(ssl: [cacerts: :public_key.cacerts_get()])
        Keyword.put(base_opts, :cred, cred)
      else
        base_opts
      end

    case GRPC.Stub.connect(endpoint, opts) do
      {:ok, channel} -> {:ok, channel}
      {:error, reason} -> {:error, {:connect_failed, reason}}
    end
  end

  defp send_on_stream(grpc_stream, request) do
    case GRPC.Stub.send_request(grpc_stream, request) do
      %GRPC.Client.Stream{} = stream -> {:ok, stream}
      {:error, reason} -> {:error, reason}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp reset_connection(state, reason) do
    # Drop buffered (not-yet-delivered) messages on disconnect — their ack_ids
    # are in `outstanding`, so remove them to avoid pointless lease-extension
    # attempts for messages that will be redelivered.
    buffered_ack_ids =
      state.message_buffer
      |> :queue.to_list()
      |> Enum.map(fn %Broadway.Message{acknowledger: {_, _, %{ack_id: id}}} -> id end)

    new_outstanding =
      Enum.reduce(buffered_ack_ids, state.outstanding, &Map.delete(&2, &1))

    # Preserve `pending_demand` across reconnection. The producer's demand counter
    # survives the disconnect and it won't re-signal demand it already sent.
    # Clearing it would cause a demand deadlock: the producer has pending demand
    # but thinks it already notified us, while we lost the count.
    close_stream(
      %{
        state
        | message_buffer: :queue.new(),
          outstanding: new_outstanding,
          reconnect_ref: state.reconnect_ref
      },
      reason
    )
  end

  # Overload that does not carry a reason (used by close_stream directly)
  defp close_stream(%{reader_pid: nil, grpc_stream: nil} = state), do: state

  defp close_stream(%{reader_pid: reader_pid, grpc_stream: grpc_stream, channel: channel} = state) do
    # Unlink before killing to prevent the EXIT signal from triggering reconnect.
    if is_pid(reader_pid) do
      Process.unlink(reader_pid)
      Process.exit(reader_pid, :kill)
    end

    if grpc_stream do
      # Guard against cancelling a stream whose StreamResponseProcess is already
      # dead. The Mint ConnectionProcess calls StreamResponseProcess.done/1
      # (a synchronous GenServer.call) as part of handling {:cancel_request, ...}.
      # If the StreamResponseProcess is already gone — because the reader was killed
      # and the linked SRP died with it — the ConnectionProcess crashes with
      # "no process" even though our try/catch protects the StreamManager.
      # Checking liveness first lets us skip the cancel when there's nothing to
      # cancel safely. For Gun-based streams, payload.stream_response_pid is nil
      # so the guard is always true there.
      srp_alive? =
        case grpc_stream do
          %{payload: %{stream_response_pid: pid}} when is_pid(pid) -> Process.alive?(pid)
          _ -> true
        end

      if srp_alive? do
        try do
          GRPC.Stub.cancel(grpc_stream)
        catch
          _, _ -> :ok
        end
      end
    end

    if channel do
      # Only call disconnect if the underlying connection process is alive.
      # When the server closes the channel (e.g. after DEADLINE_EXCEEDED), the
      # adapter's connection process may already be gone. Calling disconnect on
      # a dead channel causes a FunctionClauseError inside grpc's GenServer.
      conn_alive? =
        case state.conn_pid do
          pid when is_pid(pid) -> Process.alive?(pid)
          _ -> true
        end

      if conn_alive? do
        try do
          GRPC.Stub.disconnect(channel)
        catch
          _, _ -> :ok
        end
      end
    end

    # Cancel the keep-alive timer — it will be restarted when the new stream opens.
    state = cancel_keepalive_timer(state)
    %{state | reader_pid: nil, grpc_stream: nil, channel: nil, conn_pid: nil}
  end

  defp close_stream(state, _reason) do
    close_stream(state)
  end

  # --- Private: backoff ---

  defp schedule_reconnect(%{backoff: nil} = _state) do
    raise "StreamManager failed to connect and backoff is :stop — crashing"
  end

  # Deduplication: if a :connect is already pending, skip to prevent the
  # double-reconnect race where {:stream_error} and {:stream_closed} (or {:EXIT})
  # both arrive within a single disconnect.
  defp schedule_reconnect(%{reconnect_ref: ref} = state) when not is_nil(ref) do
    state
  end

  defp schedule_reconnect(%{backoff: backoff} = state) do
    {timeout, new_backoff} = Backoff.backoff(backoff)
    emit_telemetry(:reconnect, %{delay: timeout}, state.config)
    ref = Process.send_after(self(), :connect, timeout)
    %{state | backoff: new_backoff, reconnect_ref: ref}
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  # --- Private: lease management ---

  defp schedule_lease_timer(state) do
    cancel_lease_timer(state)
    # Initial interval: (configured deadline - grace period) with jitter, minimum 1s.
    deadline_s = state.config.stream_ack_deadline_seconds
    base_ms = max(1_000, (deadline_s - @grace_period_seconds) * 1_000)
    jitter_factor = 0.8 + :rand.uniform() * 0.1
    interval_ms = round(base_ms * jitter_factor)
    timer = Process.send_after(self(), :extend_leases, interval_ms)
    %{state | lease_timer: timer}
  end

  defp cancel_lease_timer(%{lease_timer: nil} = state), do: state

  defp cancel_lease_timer(%{lease_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | lease_timer: nil}
  end

  # --- Private: keep-alive ---

  defp schedule_keepalive_timer(state) do
    state = cancel_keepalive_timer(state)
    timer = schedule_keepalive_after(state.config)
    %{state | keepalive_timer: timer}
  end

  defp schedule_keepalive_after(config) do
    interval = Map.get(config, :keepalive_interval_ms, @default_keepalive_ms)
    Process.send_after(self(), :send_keepalive, interval)
  end

  defp cancel_keepalive_timer(%{keepalive_timer: nil} = state), do: state

  defp cancel_keepalive_timer(%{keepalive_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | keepalive_timer: nil}
  end

  # --- Private: drain ---

  # Kill the reader so no new messages arrive from the gRPC stream.
  # The channel stays open for AckBatcher's unary ack/modack RPCs.
  defp close_reader(%{reader_pid: nil} = state), do: state

  defp close_reader(%{reader_pid: reader_pid} = state) do
    Process.unlink(reader_pid)
    Process.exit(reader_pid, :kill)
    %{state | reader_pid: nil}
  end

  defp start_drain_timer(state) do
    timeout = Map.get(state.config, :drain_timeout_ms, @default_drain_timeout_ms)
    timer = Process.send_after(self(), :drain_timeout, timeout)
    %{state | drain_timer: timer}
  end

  defp cancel_drain_timer(%{drain_timer: nil} = state), do: state

  defp cancel_drain_timer(%{drain_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | drain_timer: nil}
  end

  # After each ack/nack, check if we are draining and all outstanding messages
  # have been resolved. If so, cancel the drain timer and close the stream.
  defp maybe_complete_drain(%{draining: true, outstanding: outstanding} = state)
       when map_size(outstanding) == 0 do
    state = cancel_drain_timer(state)

    # Guard against AckBatcher already being dead during pipeline shutdown.
    flush_batcher_if_alive(state.ack_batcher)

    emit_telemetry(:drain_complete, %{}, state.config)
    close_stream(state)
  end

  defp maybe_complete_drain(state), do: state

  # --- Private: message building ---

  # Buffer incoming messages, then flush up to pending_demand to the producer.
  defp deliver_messages(state, messages) do
    new_buffer = Enum.reduce(messages, state.message_buffer, &:queue.in(&1, &2))
    flush_demand(%{state | message_buffer: new_buffer})
  end

  # Flush up to `pending_demand` messages from the buffer to the producer.
  defp flush_demand(%{pending_demand: 0} = state), do: state

  defp flush_demand(state) do
    if :queue.is_empty(state.message_buffer) do
      state
    else
      {remaining, demand_left, batch_reversed} =
        flush_demand_loop(state.message_buffer, state.pending_demand, [])

      send(state.producer_pid, {:stream_messages, Enum.reverse(batch_reversed)})
      %{state | message_buffer: remaining, pending_demand: demand_left}
    end
  end

  defp flush_demand_loop(queue, 0, acc), do: {queue, 0, acc}

  defp flush_demand_loop(queue, n, acc) do
    case :queue.out(queue) do
      {{:value, msg}, rest} -> flush_demand_loop(rest, n - 1, [msg | acc])
      {:empty, _} -> {queue, n, acc}
    end
  end

  defp build_broadway_message(
         %{ack_id: ack_id, message: pubsub_msg, delivery_attempt: delivery_attempt},
         state
       ) do
    # ack_ref is the Broadway pipeline name, used as the persistent_term key.
    ack_ref = state.config.broadway[:name]
    acknowledger = BroadwayCloudPubSub.Streaming.Acknowledger.builder(ack_ref).(ack_id)

    data = pubsub_msg.data
    metadata = build_metadata(pubsub_msg, delivery_attempt)

    %Broadway.Message{
      data: data,
      metadata: metadata,
      acknowledger: acknowledger
    }
  end

  defp build_metadata(msg, delivery_attempt) do
    MessageBuilder.build_metadata(%{
      message_id: msg.message_id,
      ordering_key: msg.ordering_key,
      publish_time: to_datetime(msg.publish_time),
      delivery_attempt: delivery_attempt,
      attributes: Map.new(msg.attributes || [])
    })
  end

  defp to_datetime(nil), do: nil

  defp to_datetime(%{seconds: seconds, nanos: nanos}) do
    DateTime.from_unix!(seconds * 1_000_000_000 + nanos, :nanosecond)
  rescue
    _ -> nil
  end

  # --- Private: auth ---

  defp fetch_token(%{token_generator: {mod, fun, args}}) do
    apply(mod, fun, args)
  end

  # Flush AckBatcher if its process is currently alive. Guards against the
  # batcher being down during pipeline shutdown (Broadway stops children in
  # reverse start order).
  defp flush_batcher_if_alive(nil), do: :ok

  defp flush_batcher_if_alive(batcher) do
    case GenServer.whereis(batcher) do
      nil -> :ok
      pid -> AckBatcher.flush(pid)
    end
  end

  # --- Private: telemetry ---

  defp emit_telemetry(event, measurements, config) do
    metadata = %{
      name: config.broadway[:name],
      subscription: config.subscription
    }

    :telemetry.execute(
      [:broadway_cloud_pub_sub, :stream, event],
      measurements,
      metadata
    )
  end
end
