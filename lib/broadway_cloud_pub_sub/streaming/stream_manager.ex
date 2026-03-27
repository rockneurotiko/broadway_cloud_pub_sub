defmodule BroadwayCloudPubSub.Streaming.StreamManager do
  @moduledoc false

  # GenServer that owns the gRPC bidirectional StreamingPull connection.
  # Responsibilities:
  #   - Connect and reconnect with exponential backoff
  #   - Receive messages from the stream and forward them to the producer
  #   - Accept ack/modifyAckDeadline requests from StreamingAcknowledger and
  #     send them on the stream
  #   - Track outstanding (delivered but not acked) message ack_ids for
  #     lease management and shutdown nacking
  #   - Extend message leases periodically via modifyAckDeadline
  #   - Buffer ack/nack requests during reconnection and replay on connect
  #   - Buffer incoming messages when the producer has no pending demand
  #     (demand-based backpressure via notify_demand/1)
  #   - Send keep-alive pings every 30s to prevent server idle timeout
  #     (matches the Go pingTicker and Python Heartbeater behaviour)
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
  #   for a detailed explanation of why a separate process is needed and how
  #   both adapters behave identically from this module's perspective.
  #
  # Keep-alive pings:
  #   Google's servers close idle StreamingPull connections after ~60 seconds
  #   of inactivity. Both the official Go (pingTicker) and Python (Heartbeater)
  #   libraries send an empty StreamingPullRequest every 30 seconds to prevent
  #   this. We do the same via the :send_keepalive timer. The timer is started
  #   when the stream opens and cancelled when it closes.
  #
  # Reconnect deduplication:
  #   Multiple events can arrive close together on a disconnect — e.g.
  #   {:stream_error} followed by {:stream_closed} or an {:EXIT} signal.
  #   Without deduplication, each would schedule a separate :connect message,
  #   causing two concurrent connection attempts. We track the pending reconnect
  #   timer ref in `reconnect_ref` and skip scheduling if one is already set.
  #
  # Error classification:
  #   gRPC errors are classified as :retryable (reconnect) or :terminal (stop).
  #   Terminal errors (NOT_FOUND, PERMISSION_DENIED, etc.) indicate a permanent
  #   misconfiguration; retrying forever would be counterproductive. The GenServer
  #   stops with {:terminal_error, reason} and Broadway's supervision restarts it,
  #   which will surface the error via normal OTP crash reporting.
  #
  # Skip-backoff optimisation:
  #   If a stream error arrives quickly after the stream opened, we apply the
  #   full exponential backoff. If the stream was alive for >30s before failing
  #   (meaning the server already had time to send a DEADLINE_EXCEEDED), we skip
  #   the backoff sleep and reconnect immediately — matching the Go optimisation.

  use GenServer
  require Logger

  alias BroadwayCloudPubSub.{Backoff, MessageBuilder}
  alias BroadwayCloudPubSub.Streaming.{ErrorClassifier, StreamReader}
  alias Google.Pubsub.V1.StreamingPullRequest

  # Default keep-alive interval — matches Go's pingTicker and Python's Heartbeater.
  # The server's inactivity timeout is ~60s; pinging at half that prevents closure.
  @default_keepalive_ms 30_000

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
    :lease_extension_interval_ms,
    :receiving,
    # Timer ref for the pending :connect message. Non-nil means a reconnect is
    # already scheduled — prevents double-scheduling from multiple close signals.
    :reconnect_ref,
    # Timer ref for the periodic :send_keepalive message.
    :keepalive_timer,
    # Monotonic timestamp (ms) of when the current stream was opened.
    # Used for the skip-backoff optimisation: if the stream ran >30s before
    # failing, we skip the backoff sleep and reconnect immediately.
    :stream_opened_at,
    outstanding: MapSet.new(),
    # Messages buffered while the producer has no pending demand.
    # Naturally bounded by max_outstanding_messages (server-side flow control).
    message_buffer: [],
    # How many messages the producer can currently accept.
    # Refreshed on each notify_demand/2; decremented when messages are flushed.
    pending_demand: 0,
    # Ack/nack requests buffered while the gRPC stream is down (reconnecting).
    # Replayed in FIFO order on successful reconnect. Naturally bounded by
    # max_outstanding_messages — no more acks can arrive than messages delivered.
    ack_buffer: [],
    ack_buffer_size: 0
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

    deadline_s = config.stream_ack_deadline_seconds
    extension_percent = config.lease_extension_percent
    lease_extension_interval_ms = round(deadline_s * extension_percent * 1000)

    state = %__MODULE__{
      producer_pid: nil,
      config: config,
      backoff: backoff,
      lease_extension_interval_ms: lease_extension_interval_ms,
      receiving: true,
      pending_demand: 0
    }

    # Delay connecting until producer tells us its pid via set_producer/2
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:connect, state) do
    # Clear the reconnect_ref — we are now executing the scheduled connect.
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

    pre_flush_state = %{
      state
      | grpc_stream: grpc_stream,
        conn_pid: conn_pid,
        backoff: backoff,
        stream_opened_at: now_ms()
    }

    case flush_ack_buffer(pre_flush_state) do
      {:ok, state} ->
        state = schedule_lease_timer(state)
        state = schedule_keepalive_timer(state)
        emit_telemetry(:connect, %{}, state.config)
        {:noreply, state}

      {:error, reason, state} ->
        {:noreply, schedule_reconnect(reset_connection(state, {:send_failed, reason}))}
    end
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
      new_outstanding = Enum.reduce(ack_ids, state.outstanding, &MapSet.put(&2, &1))
      emit_telemetry(:receive_messages, %{count: length(broadway_messages)}, state.config)
      {:noreply, deliver_messages(%{state | outstanding: new_outstanding}, broadway_messages)}
    else
      {:noreply, state}
    end
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
    {:noreply, schedule_reconnect(reset_connection(state, :stream_closed))}
  end

  # StreamReader process exited normally — stream ended cleanly.
  # {:stream_closed} is sent before the exit, so this is a duplicate signal.
  # We only reconnect if grpc_stream is still set (meaning the stream_closed
  # message wasn't processed first).
  def handle_info({:EXIT, pid, :normal}, %{reader_pid: pid} = state) do
    if state.grpc_stream do
      emit_telemetry(:disconnect, %{reason: :stream_closed}, state.config)
      {:noreply, schedule_reconnect(reset_connection(state, :stream_closed))}
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
    if MapSet.size(state.outstanding) == 0 or state.grpc_stream == nil do
      timer = Process.send_after(self(), :extend_leases, state.lease_extension_interval_ms)
      {:noreply, %{state | lease_timer: timer}}
    else
      ack_ids = MapSet.to_list(state.outstanding)
      deadline = state.config.stream_ack_deadline_seconds
      deadlines = List.duplicate(deadline, length(ack_ids))

      case send_on_stream(state.grpc_stream, %StreamingPullRequest{
             modify_deadline_ack_ids: ack_ids,
             modify_deadline_seconds: deadlines
           }) do
        :ok ->
          timer = Process.send_after(self(), :extend_leases, state.lease_extension_interval_ms)
          {:noreply, %{state | lease_timer: timer}}

        {:error, reason} ->
          {:noreply, schedule_reconnect(reset_connection(state, {:send_failed, reason}))}
      end
    end
  end

  # Periodic keep-alive ping: send an empty StreamingPullRequest to prevent the
  # server from closing an idle stream. Matches Go's pingTicker (30s) and Python's
  # Heartbeater (30s). The server's inactivity timeout is ~60s; pinging at half
  # that gives a comfortable margin.
  def handle_info(:send_keepalive, %{grpc_stream: nil} = state) do
    # Stream is disconnected — don't ping, but reschedule for when it reconnects.
    # (Timer will be cancelled and restarted by close_stream/schedule_keepalive_timer.)
    {:noreply, state}
  end

  def handle_info(:send_keepalive, state) do
    case send_on_stream(state.grpc_stream, %StreamingPullRequest{}) do
      :ok ->
        timer = schedule_keepalive_after(state.config)
        {:noreply, %{state | keepalive_timer: timer}}

      {:error, reason} ->
        {:noreply, schedule_reconnect(reset_connection(state, {:send_failed, reason}))}
    end
  end

  # Mint adapter signals connection loss to its parent process.
  # When the test (or the real stack) routes this signal to StreamManager,
  # treat it the same as a stream error: reset and reconnect.
  def handle_info({:elixir_grpc, :connection_down, conn_pid}, %{conn_pid: conn_pid} = state) do
    emit_telemetry(:disconnect, %{reason: :connection_down}, state.config)
    {:noreply, schedule_reconnect(reset_connection(state, :connection_down))}
  end

  # Gun adapter signals connection loss via :gun_down messages.
  # Guard on the stored conn_pid to ignore stale/other connections.
  def handle_info(
        {:gun_down, conn_pid, _protocol, _reason, _killed_streams},
        %{conn_pid: conn_pid} = state
      ) do
    emit_telemetry(:disconnect, %{reason: :connection_down}, state.config)
    {:noreply, schedule_reconnect(reset_connection(state, :connection_down))}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:acknowledge, ack_ids}, state) do
    new_outstanding = Enum.reduce(ack_ids, state.outstanding, &MapSet.delete(&2, &1))
    state = %{state | outstanding: new_outstanding}

    if state.grpc_stream do
      case send_on_stream(state.grpc_stream, %StreamingPullRequest{ack_ids: ack_ids}) do
        :ok ->
          emit_telemetry(:ack, %{count: length(ack_ids)}, state.config)
          {:noreply, state}

        {:error, reason} ->
          {:noreply, schedule_reconnect(reset_connection(state, {:send_failed, reason}))}
      end
    else
      {:noreply, buffer_ack_request(state, {:ack, ack_ids})}
    end
  end

  def handle_cast({:modify_deadline, ack_ids, deadline_seconds}, state) do
    new_outstanding =
      if deadline_seconds == 0 do
        Enum.reduce(ack_ids, state.outstanding, &MapSet.delete(&2, &1))
      else
        state.outstanding
      end

    deadlines = List.duplicate(deadline_seconds, length(ack_ids))
    state = %{state | outstanding: new_outstanding}

    if state.grpc_stream do
      case send_on_stream(state.grpc_stream, %StreamingPullRequest{
             modify_deadline_ack_ids: ack_ids,
             modify_deadline_seconds: deadlines
           }) do
        :ok ->
          {:noreply, state}

        {:error, reason} ->
          {:noreply, schedule_reconnect(reset_connection(state, {:send_failed, reason}))}
      end
    else
      {:noreply, buffer_ack_request(state, {:modify_deadline, ack_ids, deadline_seconds})}
    end
  end

  # The producer signals its current total demand. Update pending_demand and
  # flush up to that many buffered messages to the producer.
  def handle_cast({:demand_available, amount}, %{message_buffer: []} = state) do
    {:noreply, %{state | pending_demand: amount}}
  end

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
    {:reply, :ok, %{state | receiving: false}}
  end

  def handle_call(:get_outstanding, _from, state) do
    {:reply, MapSet.to_list(state.outstanding), state}
  end

  def handle_call(:close, _from, state) do
    # Best-effort: flush buffered acks before closing. Errors are ignored
    # because we're shutting down regardless.
    state =
      case flush_ack_buffer(state) do
        {:ok, s} -> s
        {:error, _reason, s} -> %{s | ack_buffer: [], ack_buffer_size: 0}
      end

    state = close_stream(state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    cancel_lease_timer(state)
    cancel_keepalive_timer(state)
    close_stream(state)
    :ok
  end

  # --- Private: connection ---

  defp connect(%{config: config} = state) do
    with {:ok, token} <- fetch_token(config),
         {:ok, channel} <- open_channel(config, token) do
      connect_stream(channel, state)
    end
  rescue
    e ->
      {:error, {:connect_failed, Exception.message(e)}, state}
  end

  # Opens the gRPC channel and spawns the StreamReader, which will open the
  # stream and send {:stream_opened, reader_pid, grpc_stream} back to us.
  # The actual grpc_stream struct is stored on {:stream_opened} receipt, not here.
  defp connect_stream(channel, state) do
    reader_pid = StreamReader.start_link(self(), channel, state.config)

    {:ok,
     %{
       state
       | channel: channel,
         reader_pid: reader_pid,
         grpc_stream: nil,
         conn_pid: nil
     }}
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
    adapter_mod =
      case adapter do
        :gun -> GRPC.Client.Adapters.Gun
        :mint -> GRPC.Client.Adapters.Mint
      end

    keepalive_interval_ms = Map.get(config, :keepalive_interval_ms, 30_000)

    base_opts = [
      adapter: adapter_mod,
      headers: [{"authorization", "Bearer #{token}"}],
      adapter_opts: [http2_opts: %{keepalive: keepalive_interval_ms, settings_timeout: :infinity}]
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
    GRPC.Stub.send_request(grpc_stream, request)
    :ok
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp reset_connection(state, reason) do
    # Drop buffered (not-yet-delivered) messages on disconnect. Their ack_ids are
    # in `outstanding`, so extract and remove them before closing the stream to
    # avoid pointless lease-extension attempts for messages that will be redelivered.
    buffered_ack_ids =
      state.message_buffer
      |> Enum.map(fn %Broadway.Message{acknowledger: {_, _, %{ack_id: id}}} -> id end)

    new_outstanding =
      Enum.reduce(buffered_ack_ids, state.outstanding, &MapSet.delete(&2, &1))

    # Preserve `pending_demand` across reconnection. The producer's demand counter
    # survives the disconnect and it won't re-signal demand it already sent.
    # Clearing pending_demand here would cause a demand deadlock: the producer has
    # pending demand but thinks it already notified us, while we lost the count.
    # Buffered messages are dropped (the server will redeliver them), but demand
    # state must carry over so the reconnected stream can deliver immediately.
    #
    # Record the disconnect reason so schedule_reconnect can apply the skip-backoff
    # optimisation: if the stream was alive for >30s, reconnect without delay.
    close_stream(
      %{
        state
        | message_buffer: [],
          outstanding: new_outstanding,
          stream_opened_at: state.stream_opened_at,
          # carry stream_opened_at through for the skip-backoff check
          reconnect_ref: state.reconnect_ref
      },
      reason
    )
  end

  # Overload that does not carry a reason (used by close_stream directly)
  defp close_stream(%{reader_pid: nil, grpc_stream: nil} = state), do: state

  defp close_stream(%{reader_pid: reader_pid, grpc_stream: grpc_stream, channel: channel} = state) do
    # Stop the reader first so it doesn't send more messages while we clean up.
    # Unlink before killing to prevent the EXIT signal from triggering reconnect.
    if is_pid(reader_pid) do
      Process.unlink(reader_pid)
      Process.exit(reader_pid, :kill)
    end

    if grpc_stream do
      try do
        GRPC.Stub.cancel(grpc_stream)
      catch
        _, _ -> :ok
      end
    end

    if channel do
      # Only call disconnect if the underlying connection process is alive.
      # When the server closes the channel (e.g. after DEADLINE_EXCEEDED), the
      # adapter's connection process may already be gone. Calling disconnect on a
      # dead channel causes a FunctionClauseError inside grpc's GenServer.
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

  # close_stream with a reason — delegates to the main close_stream but also
  # stores the reason for the skip-backoff optimisation in schedule_reconnect.
  defp close_stream(state, _reason) do
    close_stream(state)
  end

  # --- Private: backoff ---

  defp schedule_reconnect(%{backoff: nil} = _state) do
    raise "StreamManager failed to connect and backoff is :stop — crashing"
  end

  # Deduplication: if a :connect message is already pending, do not schedule
  # another one. This prevents the double-reconnect race where {:stream_error}
  # and {:stream_closed} (or {:EXIT}) both arrive within a single disconnect.
  defp schedule_reconnect(%{reconnect_ref: ref} = state) when not is_nil(ref) do
    state
  end

  defp schedule_reconnect(%{backoff: backoff, stream_opened_at: opened_at} = state) do
    {timeout, new_backoff} = Backoff.backoff(backoff)

    # Skip-backoff optimisation (matches Go's behaviour):
    # If the stream was alive for more than 30 seconds before failing, the server
    # had time to process a DEADLINE_EXCEEDED (or similar timeout). Adding a
    # backoff delay on top of the already-long blocking period compounds the
    # reconnect latency unnecessarily. Reconnect immediately instead.
    effective_timeout =
      if skip_backoff?(opened_at) do
        0
      else
        timeout
      end

    ref = Process.send_after(self(), :connect, effective_timeout)
    %{state | backoff: new_backoff, reconnect_ref: ref}
  end

  # Returns true if the stream was open long enough that we should skip the
  # exponential backoff sleep. Threshold: 30 seconds (same as Go).
  defp skip_backoff?(nil), do: false

  defp skip_backoff?(opened_at) do
    now_ms() - opened_at >= 30_000
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  # --- Private: lease management ---

  defp schedule_lease_timer(state) do
    cancel_lease_timer(state)
    timer = Process.send_after(self(), :extend_leases, state.lease_extension_interval_ms)
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

  # --- Private: ack buffering ---

  defp buffer_ack_request(%{ack_buffer: buffer, ack_buffer_size: size} = state, request) do
    emit_telemetry(:ack_buffered, %{buffer_size: size + 1}, state.config)
    %{state | ack_buffer: [request | buffer], ack_buffer_size: size + 1}
  end

  defp flush_ack_buffer(%{ack_buffer: [], grpc_stream: _} = state), do: {:ok, state}

  defp flush_ack_buffer(%{ack_buffer: _buffer, grpc_stream: nil} = state), do: {:ok, state}

  defp flush_ack_buffer(%{ack_buffer: buffer, grpc_stream: grpc_stream} = state) do
    result =
      buffer
      |> Enum.reverse()
      |> Enum.reduce_while(:ok, fn entry, :ok ->
        request =
          case entry do
            {:ack, ack_ids} ->
              %StreamingPullRequest{ack_ids: ack_ids}

            {:modify_deadline, ack_ids, deadline_seconds} ->
              deadlines = List.duplicate(deadline_seconds, length(ack_ids))

              %StreamingPullRequest{
                modify_deadline_ack_ids: ack_ids,
                modify_deadline_seconds: deadlines
              }
          end

        case send_on_stream(grpc_stream, request) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    case result do
      :ok -> {:ok, %{state | ack_buffer: [], ack_buffer_size: 0}}
      {:error, reason} -> {:error, reason, state}
    end
  end

  # --- Private: message building ---

  # Buffer incoming messages, then flush up to pending_demand to the producer.
  # Returns updated state.
  defp deliver_messages(state, messages) do
    # Prepend for O(1); reversed on flush.
    new_buffer = Enum.reduce(messages, state.message_buffer, fn msg, acc -> [msg | acc] end)
    flush_demand(%{state | message_buffer: new_buffer})
  end

  # Flush up to `pending_demand` messages from the buffer to the producer.
  # If the buffer is empty or pending_demand is 0, this is a no-op.
  defp flush_demand(%{pending_demand: 0} = state), do: state
  defp flush_demand(%{message_buffer: []} = state), do: state

  defp flush_demand(state) do
    all_messages = Enum.reverse(state.message_buffer)
    to_send = min(state.pending_demand, length(all_messages))
    {batch, rest} = Enum.split(all_messages, to_send)

    send(state.producer_pid, {:stream_messages, batch})

    # Store remainder back in reversed (prepend-friendly) order
    reversed_rest = Enum.reverse(rest)
    %{state | message_buffer: reversed_rest, pending_demand: state.pending_demand - to_send}
  end

  defp build_broadway_message(
         %{ack_id: ack_id, message: pubsub_msg, delivery_attempt: delivery_attempt},
         state
       ) do
    # ack_ref is the Broadway pipeline name — the key used in :persistent_term by the producer
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
