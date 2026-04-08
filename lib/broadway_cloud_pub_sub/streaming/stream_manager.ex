defmodule BroadwayCloudPubSub.Streaming.StreamManager do
  @moduledoc false

  # GenServer that owns the gRPC bidirectional StreamingPull connection.
  # Manages connection lifecycle, message dispatch with demand-based backpressure,
  # lease extension, keep-alive pings, and graceful drain on shutdown.
  # See decisions.md for design rationale.

  use GenServer
  require Logger

  alias BroadwayCloudPubSub.{Backoff, MessageBuilder}

  alias BroadwayCloudPubSub.Streaming.{
    AckBatcher,
    AckTimeDistribution,
    ErrorClassifier,
    StreamReader,
    Telemetry
  }

  alias Google.Pubsub.V1.StreamingPullRequest

  # The server's inactivity timeout is ~60s; pinging at half that prevents closure.
  @default_keepalive_ms 30_000

  @default_drain_timeout_ms 30_000

  # Exactly-once delivery requires a longer retry window to handle server-side transient failures.
  @exactly_once_retry_deadline_ms 600_000

  # Subtracted from the adaptive deadline when computing the lease extension interval.
  @grace_period_seconds 5

  # Minimum ack deadline enforced by the server for exactly-once subscriptions.
  @min_deadline_exactly_once_seconds 60

  defstruct [
    :producer_pid,
    :config,
    :grpc_client,
    :grpc_client_config,
    :channel,
    :grpc_stream,
    :conn_pid,
    # Pid of the linked StreamReader process.
    :reader_pid,
    :backoff,
    :lease_timer,
    # Tracks message processing times for the adaptive p99 ack deadline.
    :ack_time_dist,
    :receiving,
    # Non-nil when a reconnect is already scheduled — prevents double-scheduling.
    :reconnect_ref,
    :keepalive_timer,
    # Registered name (not PID) so we survive UnaryAckSupervisor restarts.
    :ack_batcher,
    draining: false,
    drain_timer: nil,
    ordering_enabled: false,
    # Updated from StreamingPullResponse.subscription_properties.
    exactly_once_enabled: false,
    # ack_id => %{received_at: monotonic_ms, max_expiry: monotonic_ms}
    outstanding: %{},
    # Buffered messages waiting for producer demand. Bounded by max_outstanding_messages.
    message_buffer: :queue.new(),
    pending_demand: 0,
    # In-flight receipt modack RPCs for exactly-once delivery.
    # ref => %{broadway_messages, ack_ids, received_at}. See decisions.md.
    pending_receipt_modacks: %{}
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
      grpc_client: config.grpc_client,
      grpc_client_config: config.grpc_client_config,
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
        emit_telemetry(:connection_failure, %{}, state.config, %{reason: reason})
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

  # Decoded messages forwarded from the StreamReader.
  def handle_info({:stream_messages, messages}, state) do
    if state.receiving and messages != [] do
      broadway_messages = Enum.map(messages, &build_broadway_message(&1, state))
      ack_ids = Enum.map(messages, & &1.ack_id)

      now = now_ms()
      adaptive_deadline = AckTimeDistribution.percentile(state.ack_time_dist, 0.99)

      if state.exactly_once_enabled do
        # Exactly-once receipt modack gate: hold messages until the receipt modack
        # RPC confirms success. Messages whose modack fails are dropped (server redelivers).
        effective_deadline = max(adaptive_deadline, @min_deadline_exactly_once_seconds)
        ref = make_ref()
        AckBatcher.receipt_modack(state.ack_batcher, ref, self(), ack_ids, effective_deadline)

        pending =
          Map.put(state.pending_receipt_modacks, ref, %{
            broadway_messages: broadway_messages,
            ack_ids: ack_ids,
            received_at: now
          })

        {:noreply, %{state | pending_receipt_modacks: pending}}
      else
        # Standard delivery: fire-and-forget receipt modack, dispatch immediately.
        new_outstanding =
          add_to_outstanding(state.outstanding, ack_ids, now, state.config.max_extension_ms)

        AckBatcher.modack(state.ack_batcher, ack_ids, adaptive_deadline)
        emit_telemetry(:receive_messages, %{count: length(broadway_messages)}, state.config)
        {:noreply, deliver_messages(%{state | outstanding: new_outstanding}, broadway_messages)}
      end
    else
      {:noreply, state}
    end
  end

  # Result of an exactly-once receipt modack RPC sent via AckBatcher.receipt_modack/5.
  # Messages are delivered only if the receipt modack succeeded; otherwise dropped
  # (the server will redeliver them).
  def handle_info({:receipt_modack_result, ref, result}, state) do
    case Map.pop(state.pending_receipt_modacks, ref) do
      {nil, _} ->
        # Stale or unknown ref (e.g. cleared during drain) — ignore.
        {:noreply, state}

      {pending, rest} ->
        state = %{state | pending_receipt_modacks: rest}

        case result do
          {:ok, []} ->
            new_outstanding =
              add_to_outstanding(
                state.outstanding,
                pending.ack_ids,
                pending.received_at,
                state.config.max_extension_ms
              )

            emit_telemetry(
              :receive_messages,
              %{count: length(pending.broadway_messages)},
              state.config
            )

            {:noreply,
             deliver_messages(%{state | outstanding: new_outstanding}, pending.broadway_messages)}

          {:ok, failed_ids} ->
            # Partial success — deliver only messages whose modack succeeded.
            {ok_msgs, ok_ids} =
              partition_succeeded(pending.broadway_messages, pending.ack_ids, failed_ids)

            new_outstanding =
              add_to_outstanding(
                state.outstanding,
                ok_ids,
                pending.received_at,
                state.config.max_extension_ms
              )

            if ok_msgs != [] do
              emit_telemetry(:receive_messages, %{count: length(ok_msgs)}, state.config)
              {:noreply, deliver_messages(%{state | outstanding: new_outstanding}, ok_msgs)}
            else
              {:noreply, %{state | outstanding: new_outstanding}}
            end

          {:error, _reason} ->
            # Total failure — drop all messages (server will redeliver).
            {:noreply, state}
        end
    end
  end

  # Subscription properties forwarded from the StreamReader.
  # Sent by the server on any response when subscription settings change.
  def handle_info(
        {:subscription_properties,
         %{
           message_ordering_enabled: ordering_enabled,
           exactly_once_delivery_enabled: exactly_once_enabled
         } = _props},
        state
      ) do
    # Propagate retry deadline change to AckBatcher when exactly-once status changes.
    if exactly_once_enabled != state.exactly_once_enabled do
      new_deadline =
        if exactly_once_enabled,
          do: @exactly_once_retry_deadline_ms,
          else: Map.get(state.config, :retry_deadline_ms, 60_000)

      AckBatcher.update_retry_deadline(state.ack_batcher, new_deadline)
    end

    {:noreply,
     %{state | ordering_enabled: ordering_enabled, exactly_once_enabled: exactly_once_enabled}}
  end

  # Stream-level gRPC error reported by the StreamReader.
  # Retryable errors trigger reconnect; terminal errors stop the GenServer.
  def handle_info({:stream_error, error}, state) do
    case ErrorClassifier.classify(error) do
      :terminal ->
        Logger.error(
          "Terminal gRPC stream error on subscription #{state.config.subscription} - reason: #{inspect(error)}. Stopping StreamManager."
        )

        emit_telemetry(:terminal_error, %{}, state.config, %{reason: error})
        {:stop, {:terminal_error, error}, close_stream(state)}

      :retryable ->
        emit_telemetry(:disconnect, %{}, state.config, %{reason: error})
        {:noreply, schedule_reconnect(reset_connection(state, error))}
    end
  end

  # Server closed the stream normally (StreamReader enumeration exhausted).
  def handle_info({:stream_closed}, state) do
    emit_telemetry(:disconnect, %{}, state.config, %{reason: :stream_closed})

    # Stream ended naturally; nil out grpc_stream to skip cancel in close_stream/1.
    # See decisions.md for why cancelling after a server-initiated close crashes the Mint ConnectionProcess.
    state = %{state | grpc_stream: nil}

    if state.draining do
      {:noreply, reset_connection(state, :stream_closed)}
    else
      {:noreply, schedule_reconnect(reset_connection(state, :stream_closed))}
    end
  end

  # StreamReader exited normally — {:stream_closed} should arrive first.
  # Only reconnect if grpc_stream is still set (stream_closed not yet processed).
  def handle_info({:EXIT, pid, :normal}, %{reader_pid: pid} = state) do
    if state.grpc_stream do
      emit_telemetry(:disconnect, %{}, state.config, %{reason: :stream_closed})
      # Same rationale as {:stream_closed}: skip cancel on natural close.
      state = %{state | grpc_stream: nil}

      if state.draining do
        {:noreply, reset_connection(state, :stream_closed)}
      else
        {:noreply, schedule_reconnect(reset_connection(state, :stream_closed))}
      end
    else
      # Already handled by {:stream_closed} — just clear the reader_pid.
      {:noreply, %{state | reader_pid: nil}}
    end
  end

  # StreamReader crashed — reconnect.
  def handle_info({:EXIT, pid, reason}, %{reader_pid: pid} = state) do
    emit_telemetry(:disconnect, %{}, state.config, %{reason: reason})
    {:noreply, schedule_reconnect(reset_connection(state, reason))}
  end

  # Catch-all for other EXIT signals (e.g. from the supervisor during shutdown).
  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  def handle_info(:extend_leases, state) do
    {:noreply, do_extend_leases(state)}
  end

  # Periodic keep-alive ping to prevent the server from closing an idle stream.
  def handle_info(:send_keepalive, %{grpc_stream: nil} = state) do
    {:noreply, state}
  end

  def handle_info(:send_keepalive, state) do
    adaptive_deadline = AckTimeDistribution.percentile(state.ack_time_dist, 0.99)
    keepalive_request = %StreamingPullRequest{stream_ack_deadline_seconds: adaptive_deadline}

    case send_on_stream(state.grpc_stream, keepalive_request, state) do
      {:ok, stream} ->
        emit_telemetry(:keepalive, %{deadline: adaptive_deadline}, state.config)
        timer = schedule_keepalive_after(state.config)
        {:noreply, %{state | grpc_stream: stream, keepalive_timer: timer}}

      {:error, reason} ->
        {:noreply, schedule_reconnect(reset_connection(state, {:send_failed, reason}))}
    end
  end

  # Mint adapter signals connection loss.
  def handle_info({:elixir_grpc, :connection_down, conn_pid}, %{conn_pid: conn_pid} = state) do
    emit_telemetry(:disconnect, %{}, state.config, %{reason: :connection_down})
    {:noreply, schedule_reconnect(reset_connection(state, :connection_down))}
  end

  # Gun adapter signals connection loss.
  def handle_info(
        {:gun_down, conn_pid, _protocol, _reason, _killed_streams},
        %{conn_pid: conn_pid} = state
      ) do
    emit_telemetry(:disconnect, %{}, state.config, %{reason: :connection_down})
    {:noreply, schedule_reconnect(reset_connection(state, :connection_down))}
  end

  def handle_info(:drain_timeout, state) do
    emit_telemetry(:drain_timeout, %{}, state.config)
    {:noreply, close_stream(%{state | drain_timer: nil})}
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
    # Nack pending receipt modacks so the server redelivers them quickly.
    state = nack_pending_receipt_modacks(state)
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
    # Best-effort flush; AckBatcher may already be down during pipeline shutdown.
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

  defp connect(state) do
    case state.grpc_client.connect(state.grpc_client_config) do
      {:ok, channel} ->
        connect_stream(channel, state)

      {:error, reason} ->
        {:error, reason, state}
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
      state.grpc_client.disconnect(channel, state.grpc_client_config)
      {:error, {:connect_failed, Exception.message(e)}, state}
  end

  defp send_on_stream(grpc_stream, request, state) do
    state.grpc_client.send_request(grpc_stream, request, state.grpc_client_config)
  end

  defp reset_connection(state, reason) do
    # Drop buffered messages on disconnect; their ack_ids are already in outstanding
    # so removing them avoids pointless lease-extension for messages that will redeliver.
    buffered_ack_ids =
      state.message_buffer
      |> :queue.to_list()
      |> Enum.map(fn %Broadway.Message{acknowledger: {_, _, %{ack_id: id}}} -> id end)

    new_outstanding = Enum.reduce(buffered_ack_ids, state.outstanding, &Map.delete(&2, &1))

    # Preserve pending_demand across reconnection to avoid a demand deadlock.
    # See decisions.md.
    close_stream(
      %{state | message_buffer: :queue.new(), outstanding: new_outstanding},
      reason
    )
  end

  defp close_stream(%{reader_pid: nil, grpc_stream: nil} = state), do: state

  defp close_stream(state) do
    state
    |> stop_reader()
    |> cancel_grpc_stream()
    |> disconnect_channel()
    |> cancel_keepalive_timer()
    |> then(&%{&1 | reader_pid: nil, grpc_stream: nil, channel: nil, conn_pid: nil})
  end

  defp close_stream(state, _reason), do: close_stream(state)

  defp stop_reader(%{reader_pid: nil} = state), do: state

  defp stop_reader(%{reader_pid: reader_pid} = state) do
    # Unlink before killing to prevent the EXIT signal from triggering reconnect.
    Process.unlink(reader_pid)
    Process.exit(reader_pid, :kill)
    state
  end

  defp cancel_grpc_stream(%{grpc_stream: nil} = state), do: state

  defp cancel_grpc_stream(%{grpc_stream: grpc_stream} = state) do
    # Skip cancel if the Mint StreamResponseProcess is already dead — calling
    # cancel would crash the ConnectionProcess. See decisions.md.
    srp_alive? =
      case grpc_stream do
        %{payload: %{stream_response_pid: pid}} when is_pid(pid) -> Process.alive?(pid)
        _ -> true
      end

    if srp_alive? do
      state.grpc_client.cancel(grpc_stream, state.grpc_client_config)
    end

    state
  end

  defp disconnect_channel(%{channel: nil} = state), do: state

  defp disconnect_channel(%{channel: channel} = state) do
    # Only disconnect if the connection process is alive; a dead channel causes
    # a FunctionClauseError inside the gRPC GenServer. See decisions.md.
    conn_alive? =
      case state.conn_pid do
        pid when is_pid(pid) -> Process.alive?(pid)
        _ -> true
      end

    if conn_alive? do
      state.grpc_client.disconnect(channel, state.grpc_client_config)
    end

    state
  end

  # --- Private: backoff ---

  defp schedule_reconnect(%{backoff: nil} = _state) do
    raise "StreamManager failed to connect and backoff is :stop — crashing"
  end

  # Deduplication: skip if a :connect is already pending to prevent the
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

  # --- Private: lease extension ---

  defp do_extend_leases(state) do
    now = now_ms()
    adaptive_deadline = AckTimeDistribution.percentile(state.ack_time_dist, 0.99)

    # Enforce minimum 60s for exactly-once subscriptions.
    effective_deadline =
      if state.exactly_once_enabled,
        do: max(adaptive_deadline, @min_deadline_exactly_once_seconds),
        else: adaptive_deadline

    # Partition into still-valid and expired (past max_expiry — server will redeliver).
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

    # Schedule next tick with jitter in [0.8, 0.9) to spread out concurrent StreamManagers.
    base_interval_ms = max(1_000, (effective_deadline - @grace_period_seconds) * 1_000)
    jitter_factor = 0.8 + :rand.uniform() * 0.1
    timer = Process.send_after(self(), :extend_leases, round(base_interval_ms * jitter_factor))

    state
    |> Map.put(:outstanding, valid)
    |> Map.put(:lease_timer, timer)
    |> sweep_stale_pending_modacks()
  end

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

  # Kill the reader so no new messages arrive; keep the channel open for AckBatcher.
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
  defp maybe_complete_drain(
         %{draining: true, outstanding: outstanding, pending_receipt_modacks: pending} = state
       )
       when map_size(outstanding) == 0 and map_size(pending) == 0 do
    state = cancel_drain_timer(state)
    # AckBatcher may already be down during pipeline shutdown.
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

  # Build outstanding entries for a list of confirmed ack_ids.
  defp add_to_outstanding(outstanding, ack_ids, received_at, max_extension_ms) do
    Enum.reduce(ack_ids, outstanding, fn ack_id, acc ->
      Map.put(acc, ack_id, %{received_at: received_at, max_expiry: received_at + max_extension_ms})
    end)
  end

  # Split broadway_messages into {succeeded_msgs, succeeded_ids} by removing
  # messages whose ack_id is in failed_ids.
  defp partition_succeeded(broadway_messages, all_ack_ids, failed_ids) do
    failed_set = MapSet.new(failed_ids)

    {ok_msgs_reversed, ok_ids_reversed} =
      Enum.zip(broadway_messages, all_ack_ids)
      |> Enum.reduce({[], []}, fn {msg, id}, {msgs_acc, ids_acc} ->
        if MapSet.member?(failed_set, id) do
          {msgs_acc, ids_acc}
        else
          {[msg | msgs_acc], [id | ids_acc]}
        end
      end)

    {Enum.reverse(ok_msgs_reversed), Enum.reverse(ok_ids_reversed)}
  end

  # Stale pending receipt modacks (older than 60s) are nacked for fast redelivery.
  @receipt_modack_stale_ms 60_000

  defp sweep_stale_pending_modacks(state) do
    now = now_ms()
    cutoff = now - @receipt_modack_stale_ms

    {stale, fresh} =
      Map.split_with(state.pending_receipt_modacks, fn {_ref, %{received_at: t}} ->
        t < cutoff
      end)

    if map_size(stale) > 0 do
      stale_ids = stale |> Map.values() |> Enum.flat_map(& &1.ack_ids)
      AckBatcher.modack(state.ack_batcher, stale_ids, 0)
      emit_telemetry(:receipt_modack_stale, %{count: length(stale_ids)}, state.config)
    end

    %{state | pending_receipt_modacks: fresh}
  end

  # Nack all messages held in pending_receipt_modacks so the server redelivers
  # them quickly. Used during drain/shutdown.
  defp nack_pending_receipt_modacks(%{pending_receipt_modacks: pending} = state)
       when map_size(pending) == 0,
       do: state

  defp nack_pending_receipt_modacks(state) do
    pending_ids =
      state.pending_receipt_modacks
      |> Map.values()
      |> Enum.flat_map(& &1.ack_ids)

    {_action, deadline} = state.config.on_shutdown
    AckBatcher.modack(state.ack_batcher, pending_ids, deadline)
    %{state | pending_receipt_modacks: %{}}
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

  defp emit_telemetry(event, measurements, config, extra_metadata \\ %{}) do
    metadata =
      Map.merge(
        %{
          name: config.broadway[:name],
          subscription: config.subscription
        },
        extra_metadata
      )

    Telemetry.execute(:stream, event, measurements, metadata, Map.get(config, :telemetry_metadata))
  end
end
