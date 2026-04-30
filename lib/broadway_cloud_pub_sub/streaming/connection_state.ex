defmodule BroadwayCloudPubSub.Streaming.ConnectionState do
  @moduledoc false

  # Opaque sub-struct that owns all gRPC connection state for a StreamManager.
  #
  # Encapsulates: channel lifecycle (connect/disconnect), bidirectional stream
  # management (open/cancel), reader process supervision, reconnect backoff with
  # deduplication, and keepalive timer scheduling.
  #
  # All functions accept and return a `%ConnectionState{}`. Side effects (process
  # spawning, timer scheduling, gRPC calls) happen inside the functions, but the
  # caller (StreamManager) controls *when* they are invoked. This keeps connection
  # orchestration concentrated in one module while StreamManager focuses on message
  # lifecycle.

  alias BroadwayCloudPubSub.Backoff
  alias BroadwayCloudPubSub.Streaming.StreamReader

  # The server's inactivity timeout is ~60s; pinging at half that prevents closure.
  @default_keepalive_ms 30_000

  @type t :: %__MODULE__{
          grpc_client: module(),
          grpc_client_config: term(),
          channel: term() | nil,
          grpc_stream: term() | nil,
          conn_pid: pid() | nil,
          reader_pid: pid() | nil,
          backoff: Backoff.t() | nil,
          reconnect_ref: reference() | nil,
          keepalive_timer: reference() | nil
        }

  defstruct [
    :grpc_client,
    :grpc_client_config,
    :channel,
    :grpc_stream,
    :conn_pid,
    :reader_pid,
    :backoff,
    :reconnect_ref,
    :keepalive_timer
  ]

  # --- Construction ---

  @doc """
  Creates a new ConnectionState with the given gRPC client, config, and backoff options.
  """
  @spec new(module(), term(), keyword()) :: t()
  def new(grpc_client, grpc_client_config, backoff_opts) do
    backoff =
      Backoff.new(
        type: Keyword.fetch!(backoff_opts, :type),
        min: Keyword.fetch!(backoff_opts, :min),
        max: Keyword.fetch!(backoff_opts, :max)
      )

    %__MODULE__{
      grpc_client: grpc_client,
      grpc_client_config: grpc_client_config,
      backoff: backoff
    }
  end

  # --- Connection lifecycle ---

  @doc """
  Opens a gRPC channel and spawns a StreamReader that will send
  `{:stream_opened, reader_pid, grpc_stream}` back to `manager_pid`.

  Returns `{:ok, conn}` on success or `{:error, reason, conn}` on failure.
  The `config` map is forwarded to `StreamReader.start_link/3`.
  """
  @spec connect(t(), pid(), map()) :: {:ok, t()} | {:error, term(), t()}
  def connect(%__MODULE__{} = conn, manager_pid, config) do
    case conn.grpc_client.connect(conn.grpc_client_config) do
      {:ok, channel} ->
        connect_stream(conn, channel, manager_pid, config)

      {:error, reason} ->
        {:error, reason, conn}
    end
  rescue
    e ->
      {:error, {:connect_failed, Exception.message(e)}, conn}
  end

  @doc """
  Handles the `{:stream_opened, reader_pid, grpc_stream}` message from StreamReader.

  Stores the stream, extracts the conn_pid for connection-down monitoring,
  and resets the backoff to its initial value (successful connection).
  """
  @spec stream_opened(t(), pid(), term()) :: t()
  def stream_opened(%__MODULE__{} = conn, _reader_pid, grpc_stream) do
    conn_pid = grpc_stream.channel.adapter_payload.conn_pid

    %{
      conn
      | grpc_stream: grpc_stream,
        conn_pid: conn_pid,
        backoff: Backoff.reset(conn.backoff)
    }
  end

  @doc """
  Sends a request on the open bidirectional gRPC stream.

  Returns `{:ok, conn}` with the (possibly updated) stream, or `{:error, reason}`.
  """
  @spec send_on_stream(t(), term()) :: {:ok, t()} | {:error, term()}
  def send_on_stream(%__MODULE__{grpc_stream: nil}, _request) do
    {:error, :no_stream}
  end

  def send_on_stream(%__MODULE__{} = conn, request) do
    case conn.grpc_client.send_request(conn.grpc_stream, request, conn.grpc_client_config) do
      {:ok, stream} -> {:ok, %{conn | grpc_stream: stream}}
      {:error, reason} -> {:error, reason}
    end
  end

  # --- Reconnect ---

  @doc """
  Schedules a reconnect after backoff delay. Returns `{conn, delay}` where
  `delay` is the backoff timeout in milliseconds.

  No-op if a reconnect is already pending (returns `{conn, nil}`).
  Raises if backoff is `:stop` (nil).
  """
  @spec schedule_reconnect(t()) :: {t(), non_neg_integer() | nil}
  def schedule_reconnect(%__MODULE__{backoff: nil}) do
    raise "StreamManager failed to connect and backoff is :stop — crashing"
  end

  def schedule_reconnect(%__MODULE__{reconnect_ref: ref} = conn) when not is_nil(ref) do
    {conn, nil}
  end

  def schedule_reconnect(%__MODULE__{backoff: backoff} = conn) do
    {timeout, new_backoff} = Backoff.backoff(backoff)
    ref = Process.send_after(self(), :connect, timeout)
    {%{conn | backoff: new_backoff, reconnect_ref: ref}, timeout}
  end

  @doc """
  Clears the reconnect_ref when the `:connect` message fires.
  """
  @spec clear_reconnect_ref(t()) :: t()
  def clear_reconnect_ref(%__MODULE__{} = conn) do
    %{conn | reconnect_ref: nil}
  end

  # --- Close / teardown ---

  @doc """
  Fully closes the connection: kills the reader, cancels the gRPC stream,
  disconnects the channel, cancels the keepalive timer.

  Returns `conn` with all connection fields nil'd.
  Safe to call when already closed (no-op).
  """
  @spec close(t()) :: t()
  def close(%__MODULE__{reader_pid: nil, grpc_stream: nil} = conn), do: conn

  def close(%__MODULE__{} = conn) do
    conn
    |> stop_reader()
    |> cancel_grpc_stream()
    |> disconnect_channel()
    |> cancel_keepalive()
    |> then(&%{&1 | reader_pid: nil, grpc_stream: nil, channel: nil, conn_pid: nil})
  end

  @doc """
  Kills the reader process without tearing down the rest of the connection.
  Used during drain to stop new messages while keeping the channel open
  for the AckBatcher to flush pending acks.
  """
  @spec close_reader(t()) :: t()
  def close_reader(%__MODULE__{reader_pid: nil} = conn), do: conn

  def close_reader(%__MODULE__{reader_pid: reader_pid} = conn) do
    Process.unlink(reader_pid)
    Process.exit(reader_pid, :kill)
    %{conn | reader_pid: nil}
  end

  @doc """
  Marks the gRPC stream as closed (sets to nil) without cancelling it.

  Used when the server initiates a close — cancelling a server-closed stream
  crashes the Mint ConnectionProcess. The caller should call `close/1` after
  this if a full teardown is needed.
  """
  @spec mark_stream_closed(t()) :: t()
  def mark_stream_closed(%__MODULE__{} = conn) do
    %{conn | grpc_stream: nil}
  end

  # --- Keepalive ---

  @doc """
  Schedules a keepalive timer. Cancels any existing timer first.
  """
  @spec schedule_keepalive(t(), map()) :: t()
  def schedule_keepalive(%__MODULE__{} = conn, config) do
    conn = cancel_keepalive(conn)
    interval = Map.get(config, :keepalive_interval_ms, @default_keepalive_ms)
    timer = Process.send_after(self(), :send_keepalive, interval)
    %{conn | keepalive_timer: timer}
  end

  @doc """
  Cancels the keepalive timer if one is active.
  """
  @spec cancel_keepalive(t()) :: t()
  def cancel_keepalive(%__MODULE__{keepalive_timer: nil} = conn), do: conn

  def cancel_keepalive(%__MODULE__{keepalive_timer: timer} = conn) do
    Process.cancel_timer(timer)
    %{conn | keepalive_timer: nil}
  end

  # --- Accessors ---

  @doc """
  Returns true if the gRPC stream is open.
  """
  @spec connected?(t()) :: boolean()
  def connected?(%__MODULE__{grpc_stream: nil}), do: false
  def connected?(%__MODULE__{}), do: true

  @doc """
  Returns the reader pid, or nil if no reader is active.
  """
  @spec reader_pid(t()) :: pid() | nil
  def reader_pid(%__MODULE__{reader_pid: pid}), do: pid

  @doc """
  Returns the conn_pid (Mint/Gun connection process), or nil.
  """
  @spec conn_pid(t()) :: pid() | nil
  def conn_pid(%__MODULE__{conn_pid: pid}), do: pid

  # --- Private ---

  defp connect_stream(conn, channel, manager_pid, config) do
    with {:ok, reader_pid} <- StreamReader.start_link(manager_pid, channel, config) do
      {:ok,
       %{
         conn
         | channel: channel,
           reader_pid: reader_pid,
           grpc_stream: nil,
           conn_pid: nil
       }}
    end
  rescue
    e ->
      conn.grpc_client.disconnect(channel, conn.grpc_client_config)
      {:error, {:connect_failed, Exception.message(e)}, conn}
  end

  defp stop_reader(%__MODULE__{reader_pid: nil} = conn), do: conn

  defp stop_reader(%__MODULE__{reader_pid: reader_pid} = conn) do
    # Unlink before killing to prevent the EXIT signal from triggering reconnect.
    Process.unlink(reader_pid)
    Process.exit(reader_pid, :kill)
    conn
  end

  defp cancel_grpc_stream(%__MODULE__{grpc_stream: nil} = conn), do: conn

  defp cancel_grpc_stream(%__MODULE__{grpc_stream: grpc_stream} = conn) do
    # Skip cancel if the Mint StreamResponseProcess is already dead — calling
    # cancel would crash the ConnectionProcess. See decisions.md.
    srp_alive? =
      case grpc_stream do
        %{payload: %{stream_response_pid: pid}} when is_pid(pid) -> Process.alive?(pid)
        _ -> true
      end

    if srp_alive? do
      conn.grpc_client.cancel(grpc_stream, conn.grpc_client_config)
    end

    conn
  end

  defp disconnect_channel(%__MODULE__{channel: nil} = conn), do: conn

  defp disconnect_channel(%__MODULE__{channel: channel} = conn) do
    # Only disconnect if the connection process is alive; a dead channel causes
    # a FunctionClauseError inside the gRPC GenServer. See decisions.md.
    conn_alive? =
      case conn.conn_pid do
        pid when is_pid(pid) -> Process.alive?(pid)
        _ -> true
      end

    if conn_alive? do
      conn.grpc_client.disconnect(channel, conn.grpc_client_config)
    end

    conn
  end
end
