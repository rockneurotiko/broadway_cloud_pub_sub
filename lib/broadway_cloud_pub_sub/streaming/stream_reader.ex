defmodule BroadwayCloudPubSub.Streaming.StreamReader do
  @moduledoc false

  # A short-lived process that owns the gRPC bidirectional streaming connection
  # and forwards decoded messages to the StreamManager.
  #
  # ## Why a separate process?
  #
  # The `grpc_client` library's `GRPC.Stub.recv/2` returns a blocking `Enumerable`.
  # A GenServer cannot block on enumeration (it would stop processing casts,
  # calls, and timers). By spawning a dedicated reader process, the GenServer
  # remains fully responsive while streaming runs concurrently.
  #
  # ## Unified adapter abstraction
  #
  # This module uses only the public `GRPC.Stub` API:
  #
  #   1. `Stub.streaming_pull(channel)` — opens the bidirectional stream
  #   2. `GRPC.Stub.send_request(stream, initial_request)` — sends the initial
  #      StreamingPullRequest (subscription name, flow control settings, etc.)
  #   3. `GRPC.Stub.recv(stream)` — returns an `{:ok, Enumerable.t()}` of
  #      decoded `{:ok, StreamingPullResponse.t()}` items
  #
  # Both the Gun and Mint adapters implement this interface identically from the
  # caller's perspective:
  #
  #   - **Gun**: `:gun.post` is called from this process, so Gun sends all
  #     `{:gun_response, :gun_data, ...}` messages to this process's mailbox.
  #     `GRPC.Stub.recv` returns a `Stream.unfold/2` backed by `:gun.await/3`,
  #     which is a selective receive that processes those mailbox messages.
  #
  #   - **Mint**: `GRPC.Client.Adapters.Mint.ConnectionProcess` owns the TCP
  #     connection. A `StreamResponseProcess` is started per stream. Decoded
  #     messages are enqueued there and served to the caller via
  #     `GenServer.call(:get_response, :infinity)`.
  #
  # In both cases the library handles gRPC frame decoding (5-byte
  # length-prefixed framing + codec decode) and delivers decoded protobuf
  # structs to the caller.
  #
  # ## Message protocol with StreamManager
  #
  # After the stream is opened, this process sends the grpc_stream back:
  #
  #   `{:stream_opened, pid, grpc_stream}`
  #
  # Then it forwards received messages and lifecycle events:
  #
  #   `{:stream_messages, [ReceivedMessage.t()]}` — one or more decoded messages
  #   `{:stream_error, error}`                    — stream-level gRPC error
  #   `{:stream_closed}`                          — server closed stream normally
  #
  # On exit (normal or crash), the StreamManager detects it via the linked
  # process `{:EXIT, pid, reason}` signal (StreamManager traps exits).
  #
  # ## Sending on the stream (acks, deadline modifications)
  #
  # After receiving `{:stream_opened, _pid, grpc_stream}`, the StreamManager
  # calls `GRPC.Stub.send_request(grpc_stream, request)` directly from the
  # GenServer process.
  #
  # - **Gun**: `:gun.data/4` is a fire-and-forget `gen_statem:cast`. It can be
  #   called from any process regardless of who opened the stream.
  # - **Mint**: `ConnectionProcess.stream_request_body/3` is also a GenServer
  #   cast, callable from any process.
  #
  # Both are safe to call from the StreamManager GenServer concurrently with
  # the reader process enumerating the receive stream.

  alias Google.Pubsub.V1.{StreamingPullRequest, StreamingPullResponse}
  alias Google.Pubsub.V1.Subscriber.Stub

  @doc """
  Spawns a linked reader process. The reader opens the gRPC stream and sends
  the stream struct back to `manager` via `{:stream_opened, self(), grpc_stream}`
  before beginning enumeration.

  Returns the reader pid.
  """
  @spec start_link(pid(), GRPC.Channel.t(), map()) :: pid()
  def start_link(manager, channel, config) do
    spawn_link(fn -> run(manager, channel, config) end)
  end

  # --- Private ---

  defp run(manager, channel, config) do
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

    # Notify the manager that the stream is open. The manager needs the
    # grpc_stream struct to call GRPC.Stub.send_request for acks and deadline
    # modifications on the bidirectional stream.
    send(manager, {:stream_opened, self(), grpc_stream})

    case GRPC.Stub.recv(grpc_stream, timeout: :infinity) do
      {:ok, enum} ->
        enumerate(enum, manager)

      {:error, error} ->
        send(manager, {:stream_error, error})
    end
  end

  defp enumerate(enum, manager) do
    enum
    |> Stream.each(fn
      {:ok, %StreamingPullResponse{received_messages: msgs}} when msgs != [] ->
        send(manager, {:stream_messages, msgs})

      {:ok, %StreamingPullResponse{}} ->
        # Heartbeat / empty response — nothing to forward
        :ok

      {:error, error} ->
        send(manager, {:stream_error, error})
    end)
    |> Stream.run()

    # Stream exhausted normally — notify manager before exit.
    # StreamManager will also receive {:EXIT, reader_pid, :normal} and
    # schedule reconnect, but sending {:stream_closed} allows distinguishing
    # normal closes from crashes in logs/telemetry.
    send(manager, {:stream_closed})
  end
end
