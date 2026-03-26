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
  #
  # Implementation note on gRPC adapter support:
  #   This module supports two gRPC HTTP/2 adapters: Gun and Mint.
  #
  #   Gun (default): GRPC.Client.Adapters.Gun calls :gun.post/3 from the calling
  #   process, making it the gun message owner. Raw {:gun_response,...},
  #   {:gun_data,...} etc. messages arrive in this GenServer's mailbox and are
  #   handled in handle_info/2. Manual gRPC frame decoding is done in-process
  #   via recv_buffer.
  #
  #   Mint: GRPC.Client.Adapters.Mint handles all framing internally via its
  #   ConnectionProcess and StreamResponseProcess. A linked reader process calls
  #   GRPC.Stub.recv/2 to enumerate decoded messages and forwards them back as
  #   {:mint_messages, msgs}. Only {:elixir_grpc, :connection_down, conn_pid}
  #   arrives in this process directly from Mint.
  #   The GenServer traps exits so reader process crashes are handled gracefully.

  use GenServer
  require Logger

  alias BroadwayCloudPubSub.{Backoff, MessageBuilder}
  alias Google.Pubsub.V1.{StreamingPullRequest, StreamingPullResponse}
  alias Google.Pubsub.V1.Subscriber.Stub

  # Maximum acks to buffer while reconnecting
  @max_ack_buffer 10_000

  defstruct [
    :producer_pid,
    :config,
    :channel,
    :grpc_stream,
    :conn_pid,
    :stream_ref,
    # Mint-only: pid of the linked reader process that enumerates GRPC.Stub.recv/2
    :reader_pid,
    :backoff,
    :lease_timer,
    :lease_extension_interval_ms,
    :receiving,
    # Gun-only: binary buffer for reassembling gRPC length-prefixed frames
    recv_buffer: <<>>,
    outstanding: MapSet.new(),
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
      receiving: true
    }

    # Delay connecting until producer tells us its pid via set_producer/2
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:connect, state) do
    case connect(state) do
      {:ok, new_state} ->
        {:noreply, new_state}

      {:error, reason, new_state} ->
        log_connection_failure(reason)
        emit_telemetry(:connection_failure, %{reason: reason}, state.config)
        {:noreply, schedule_reconnect(new_state)}
    end
  end

  # --- Raw Gun protocol messages ---

  # Initial HTTP/2 response headers (200 OK) — connection established
  def handle_info({:gun_response, conn_pid, stream_ref, :nofin, 200, _headers}, state)
      when state.conn_pid == conn_pid and state.stream_ref == stream_ref do
    {:noreply, state}
  end

  # Non-200 initial response — treat as error
  def handle_info({:gun_response, conn_pid, stream_ref, _fin, status, _headers}, state)
      when state.conn_pid == conn_pid and state.stream_ref == stream_ref do
    Logger.error("[StreamManager] gRPC stream got HTTP status #{status}")
    {:noreply, schedule_reconnect(reset_connection(state))}
  end

  # Catch-all for gun_response not matching our stream (e.g. different conn/ref)
  def handle_info({:gun_response, conn_pid, stream_ref, fin, status, _headers}, state) do
    Logger.debug(
      "[StreamManager] Ignoring stale gun_response: conn=#{inspect(conn_pid)} ref=#{inspect(stream_ref)} fin=#{inspect(fin)} status=#{status} (state conn=#{inspect(state.conn_pid)} ref=#{inspect(state.stream_ref)})"
    )

    {:noreply, state}
  end

  # Data chunk(s) from the server
  def handle_info({:gun_data, conn_pid, stream_ref, fin, data}, state)
      when state.conn_pid == conn_pid and state.stream_ref == stream_ref do
    buffer = state.recv_buffer <> data
    {messages, remaining_buffer} = decode_grpc_messages(buffer)

    state = %{state | recv_buffer: remaining_buffer}

    state =
      if state.receiving and messages != [] do
        broadway_messages = Enum.map(messages, &build_broadway_message(&1, state))
        ack_ids = Enum.map(messages, & &1.ack_id)
        new_outstanding = Enum.reduce(ack_ids, state.outstanding, &MapSet.put(&2, &1))
        emit_telemetry(:receive_messages, %{count: length(broadway_messages)}, state.config)
        send(state.producer_pid, {:stream_messages, broadway_messages})
        %{state | outstanding: new_outstanding}
      else
        state
      end

    if fin == :fin do
      {:noreply, schedule_reconnect(reset_connection(state))}
    else
      {:noreply, state}
    end
  end

  # Catch-all for gun_data not matching our stream
  def handle_info({:gun_data, conn_pid, stream_ref, _fin, _data} = _msg, state) do
    Logger.debug(
      "[StreamManager] Ignoring stale gun_data: conn=#{inspect(conn_pid)} ref=#{inspect(stream_ref)} (state conn=#{inspect(state.conn_pid)} ref=#{inspect(state.stream_ref)})"
    )

    {:noreply, state}
  end

  # Trailers — stream ended normally
  def handle_info({:gun_trailers, conn_pid, stream_ref, trailers}, state)
      when state.conn_pid == conn_pid and state.stream_ref == stream_ref do
    grpc_status = trailers |> List.keyfind("grpc-status", 0) |> elem(1)
    grpc_message = trailers |> List.keyfind("grpc-message", 0, {"", ""}) |> elem(1)

    case grpc_status do
      "0" ->
        :ok

      status ->
        Logger.warning(
          "[StreamManager] gRPC stream closed with status #{status}: #{grpc_message}"
        )
    end

    emit_telemetry(:disconnect, %{reason: :stream_closed}, state.config)
    {:noreply, schedule_reconnect(reset_connection(state))}
  end

  # Stream-level error
  def handle_info({:gun_error, conn_pid, stream_ref, reason}, state)
      when state.conn_pid == conn_pid and state.stream_ref == stream_ref do
    Logger.warning("[StreamManager] gRPC stream error: #{inspect(reason)}")
    emit_telemetry(:disconnect, %{reason: reason}, state.config)
    {:noreply, schedule_reconnect(reset_connection(state))}
  end

  # Connection-level error/down
  def handle_info({:gun_down, conn_pid, _protocol, reason, _killed_streams}, state)
      when state.conn_pid == conn_pid do
    Logger.warning("[StreamManager] gRPC connection down: #{inspect(reason)}")
    emit_telemetry(:disconnect, %{reason: reason}, state.config)
    {:noreply, schedule_reconnect(reset_connection(state))}
  end

  # --- Mint adapter messages ---

  # Decoded messages forwarded from the Mint reader process
  def handle_info({:mint_messages, messages}, state) do
    if state.receiving and messages != [] do
      broadway_messages = Enum.map(messages, &build_broadway_message(&1, state))
      ack_ids = Enum.map(messages, & &1.ack_id)
      new_outstanding = Enum.reduce(ack_ids, state.outstanding, &MapSet.put(&2, &1))
      emit_telemetry(:receive_messages, %{count: length(broadway_messages)}, state.config)
      send(state.producer_pid, {:stream_messages, broadway_messages})
      {:noreply, %{state | outstanding: new_outstanding}}
    else
      {:noreply, state}
    end
  end

  # Error reported by the Mint reader process (stream-level gRPC error)
  def handle_info({:mint_stream_error, error}, state) do
    Logger.warning("[StreamManager] Mint stream error: #{inspect(error)}")
    emit_telemetry(:disconnect, %{reason: error}, state.config)
    {:noreply, schedule_reconnect(reset_connection(state))}
  end

  # Connection-down message sent by the Mint adapter's ConnectionProcess
  def handle_info({:elixir_grpc, :connection_down, conn_pid}, state)
      when state.conn_pid == conn_pid do
    Logger.warning("[StreamManager] Mint gRPC connection down")
    emit_telemetry(:disconnect, %{reason: :connection_down}, state.config)
    {:noreply, schedule_reconnect(reset_connection(state))}
  end

  # Mint reader process exited normally — stream ended, reconnect
  def handle_info({:EXIT, pid, :normal}, %{reader_pid: pid} = state) do
    Logger.info("[StreamManager] Mint reader stream ended normally")
    emit_telemetry(:disconnect, %{reason: :stream_closed}, state.config)
    {:noreply, schedule_reconnect(reset_connection(state))}
  end

  # Mint reader process crashed — reconnect
  def handle_info({:EXIT, pid, reason}, %{reader_pid: pid} = state) do
    Logger.warning("[StreamManager] Mint reader crashed: #{inspect(reason)}")
    emit_telemetry(:disconnect, %{reason: reason}, state.config)
    {:noreply, schedule_reconnect(reset_connection(state))}
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

      send_on_stream(state.grpc_stream, %StreamingPullRequest{
        modify_deadline_ack_ids: ack_ids,
        modify_deadline_seconds: deadlines
      })

      timer = Process.send_after(self(), :extend_leases, state.lease_extension_interval_ms)
      {:noreply, %{state | lease_timer: timer}}
    end
  end

  def handle_info(msg, state) do
    Logger.warning("[StreamManager] Unhandled message: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:acknowledge, ack_ids}, state) do
    new_outstanding = Enum.reduce(ack_ids, state.outstanding, &MapSet.delete(&2, &1))
    state = %{state | outstanding: new_outstanding}

    state =
      if state.grpc_stream do
        send_on_stream(state.grpc_stream, %StreamingPullRequest{ack_ids: ack_ids})
        emit_telemetry(:ack, %{count: length(ack_ids)}, state.config)
        state
      else
        buffer_ack_request(state, {:ack, ack_ids})
      end

    {:noreply, state}
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

    state =
      if state.grpc_stream do
        send_on_stream(state.grpc_stream, %StreamingPullRequest{
          modify_deadline_ack_ids: ack_ids,
          modify_deadline_seconds: deadlines
        })

        state
      else
        buffer_ack_request(state, {:modify_deadline, ack_ids, deadline_seconds})
      end

    {:noreply, state}
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
    state = flush_ack_buffer(state)
    state = close_stream(state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def terminate(reason, state) do
    Logger.debug("[StreamManager] terminate: reason=#{inspect(reason)}")
    cancel_lease_timer(state)
    close_stream(state)
    :ok
  end

  # --- Private: connection ---

  defp connect(%{config: config} = state) do
    with {:ok, token} <- fetch_token(config),
         {:ok, channel} <- open_channel(config, token) do
      connect_stream(channel, state)
    end
  end

  # Second phase of connect: open the gRPC stream on the already-open channel.
  # Separated so that if open_stream raises, we can disconnect the channel
  # before propagating the error (preventing stale Gun messages in our mailbox).
  defp connect_stream(channel, %{config: config} = state) do
    {:ok, grpc_stream, conn_pid, stream_ref} = open_stream(channel, state)
    backoff = Backoff.reset(state.backoff)

    state =
      flush_ack_buffer(%{
        state
        | channel: channel,
          grpc_stream: grpc_stream,
          conn_pid: conn_pid,
          stream_ref: stream_ref,
          recv_buffer: <<>>,
          backoff: backoff
      })

    state = maybe_start_reader(state)
    state = schedule_lease_timer(state)
    emit_telemetry(:connect, %{}, config)
    {:ok, state}
  rescue
    e ->
      # open_stream may raise (Stub.streaming_pull / send_request don't
      # return error tuples). If it raised, no gRPC stream was successfully
      # opened, so only disconnect the channel to prevent its Gun/Mint
      # connection from delivering stale messages to our mailbox.
      try do
        GRPC.Stub.disconnect(channel)
      catch
        _, _ -> :ok
      end

      {:error, {:open_stream_raised, Exception.message(e)}, state}
  end

  # For Mint: spawn a linked reader process that enumerates GRPC.Stub.recv/2 and
  # forwards decoded messages back to the StreamManager.
  defp maybe_start_reader(%{config: %{adapter: :mint}, grpc_stream: grpc_stream} = state) do
    manager = self()

    pid =
      spawn_link(fn ->
        {:ok, enum} = GRPC.Stub.recv(grpc_stream)

        enum
        |> Stream.each(fn
          {:ok, %StreamingPullResponse{received_messages: msgs}} when msgs != [] ->
            send(manager, {:mint_messages, msgs})

          {:ok, %StreamingPullResponse{}} ->
            # Heartbeat / empty response — nothing to forward
            :ok

          {:error, error} ->
            send(manager, {:mint_stream_error, error})
        end)
        |> Stream.run()

        # Stream exhausted normally. The reader exits :normal and StreamManager
        # will receive {:EXIT, reader_pid, :normal} due to trap_exit.
      end)

    %{state | reader_pid: pid}
  end

  defp maybe_start_reader(state), do: state

  defp adapter_module(:gun), do: GRPC.Client.Adapters.Gun
  defp adapter_module(:mint), do: GRPC.Client.Adapters.Mint

  defp open_channel(
         %{grpc_endpoint: endpoint, use_ssl: use_ssl, adapter: adapter} = _config,
         token
       ) do
    base_opts = [
      adapter: adapter_module(adapter),
      headers: [{"authorization", "Bearer #{token}"}]
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

  defp open_stream(channel, state) do
    config = state.config
    client_id = Map.fetch!(config, :client_id)

    initial_request = %StreamingPullRequest{
      subscription: config.subscription,
      stream_ack_deadline_seconds: config.stream_ack_deadline_seconds,
      max_outstanding_messages: config.max_outstanding_messages,
      max_outstanding_bytes: config.max_outstanding_bytes,
      client_id: client_id
    }

    grpc_stream = Stub.streaming_pull(channel, [])
    grpc_stream = GRPC.Stub.send_request(grpc_stream, initial_request)

    # Both adapters store the connection process pid in adapter_payload.conn_pid,
    # but the stream_ref field only exists for Gun.
    conn_pid = grpc_stream.channel.adapter_payload.conn_pid

    case config.adapter do
      :gun ->
        stream_ref = grpc_stream.payload.stream_ref
        {:ok, grpc_stream, conn_pid, stream_ref}

      :mint ->
        {:ok, grpc_stream, conn_pid, nil}
    end
  end

  # Decode one or more GRPC length-prefixed messages from the buffer.
  # Returns {[StreamingPullResponse.received_messages], remaining_buffer}
  defp decode_grpc_messages(buffer) do
    decode_grpc_messages(buffer, [])
  end

  defp decode_grpc_messages(buffer, acc) do
    case GRPC.Message.get_message(buffer) do
      {{_flag, encoded}, rest} ->
        case StreamingPullResponse.decode(encoded) do
          %StreamingPullResponse{received_messages: msgs} when msgs != [] ->
            decode_grpc_messages(rest, Enum.reverse(msgs, acc))

          %StreamingPullResponse{} ->
            # Heartbeat/empty response
            decode_grpc_messages(rest, acc)
        end

      false ->
        {Enum.reverse(acc), buffer}
    end
  end

  defp send_on_stream(grpc_stream, request) do
    try do
      GRPC.Stub.send_request(grpc_stream, request)
    catch
      kind, reason ->
        Logger.warning("[StreamManager] Failed to send on stream: #{kind} #{inspect(reason)}")
    end
  end

  defp reset_connection(state) do
    close_stream(state)
  end

  defp close_stream(%{grpc_stream: nil} = state), do: stop_reader(state)

  defp close_stream(%{grpc_stream: grpc_stream, channel: channel, conn_pid: conn_pid} = state) do
    state = stop_reader(state)

    # Cancel the stream (sends RST_STREAM) so Gun stops forwarding data for
    # this stream_ref. end_stream/1 only half-closes the client side and leaves
    # the server free to keep sending.
    try do
      GRPC.Stub.cancel(grpc_stream)
    catch
      _, _ -> :ok
    end

    if channel do
      try do
        GRPC.Stub.disconnect(channel)
      catch
        _, _ -> :ok
      end
    end

    # Force-kill the underlying Gun process synchronously. :gun.shutdown (called
    # by GRPC.Stub.disconnect) is an async cast with a 15-second graceful close
    # period during which Gun continues delivering messages to our mailbox.
    # Killing it immediately eliminates that race window.
    if is_pid(conn_pid), do: Process.exit(conn_pid, :kill)

    # Drain any gun messages from conn_pid that were already in our mailbox
    # before the process died.
    flush_gun_messages(conn_pid)

    %{state | grpc_stream: nil, channel: nil, conn_pid: nil, stream_ref: nil, recv_buffer: <<>>}
  end

  # Kills the Mint reader process (if any) and removes it from state.
  # Unlinks before killing so the EXIT signal does not trigger reconnect logic.
  defp stop_reader(%{reader_pid: pid} = state) when is_pid(pid) do
    Process.unlink(pid)
    Process.exit(pid, :kill)
    %{state | reader_pid: nil}
  end

  defp stop_reader(state), do: state

  # Drains any Gun messages from a specific conn_pid that are already sitting
  # in our mailbox. We pin on ^conn_pid so we never accidentally consume
  # messages from a newly-opened connection. The after 0 makes this
  # non-blocking — it only removes messages already present.
  defp flush_gun_messages(conn_pid) when is_pid(conn_pid) do
    receive do
      {:gun_up, ^conn_pid, _} -> flush_gun_messages(conn_pid)
      {:gun_down, ^conn_pid, _, _, _} -> flush_gun_messages(conn_pid)
      {:gun_response, ^conn_pid, _, _, _, _} -> flush_gun_messages(conn_pid)
      {:gun_data, ^conn_pid, _, _, _} -> flush_gun_messages(conn_pid)
      {:gun_trailers, ^conn_pid, _, _} -> flush_gun_messages(conn_pid)
      {:gun_error, ^conn_pid, _, _} -> flush_gun_messages(conn_pid)
      {:gun_error, ^conn_pid, _} -> flush_gun_messages(conn_pid)
    after
      0 -> :ok
    end
  end

  defp flush_gun_messages(_), do: :ok

  # --- Private: backoff ---

  defp schedule_reconnect(%{backoff: nil} = _state) do
    raise "StreamManager failed to connect and backoff is :stop — crashing"
  end

  defp schedule_reconnect(%{backoff: backoff} = state) do
    {timeout, new_backoff} = Backoff.backoff(backoff)
    Logger.info("[StreamManager] Reconnecting in #{timeout}ms")
    Process.send_after(self(), :connect, timeout)
    %{state | backoff: new_backoff}
  end

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

  # --- Private: ack buffering ---

  defp buffer_ack_request(%{ack_buffer: buffer, ack_buffer_size: size} = state, request) do
    if size < @max_ack_buffer do
      %{state | ack_buffer: [request | buffer], ack_buffer_size: size + 1}
    else
      Logger.warning("[StreamManager] Ack buffer full, dropping oldest ack request")
      %{state | ack_buffer: [request | Enum.drop(buffer, -1)]}
    end
  end

  defp flush_ack_buffer(%{ack_buffer: [], grpc_stream: _} = state), do: state

  defp flush_ack_buffer(%{ack_buffer: _buffer, grpc_stream: nil} = state), do: state

  defp flush_ack_buffer(%{ack_buffer: buffer, grpc_stream: grpc_stream} = state) do
    buffer
    |> Enum.reverse()
    |> Enum.each(fn
      {:ack, ack_ids} ->
        send_on_stream(grpc_stream, %StreamingPullRequest{ack_ids: ack_ids})

      {:modify_deadline, ack_ids, deadline_seconds} ->
        deadlines = List.duplicate(deadline_seconds, length(ack_ids))

        send_on_stream(grpc_stream, %StreamingPullRequest{
          modify_deadline_ack_ids: ack_ids,
          modify_deadline_seconds: deadlines
        })
    end)

    %{state | ack_buffer: [], ack_buffer_size: 0}
  end

  # --- Private: message building ---

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
      subscription: config.subscription,
      config: config
    }

    :telemetry.execute(
      [:broadway_cloud_pub_sub, :stream, event],
      measurements,
      metadata
    )
  end

  defp log_connection_failure(reason) do
    Logger.error("[StreamManager] Failed to connect: #{inspect(reason)}")
  end
end
