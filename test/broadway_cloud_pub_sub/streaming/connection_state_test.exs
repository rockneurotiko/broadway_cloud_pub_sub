defmodule BroadwayCloudPubSub.Streaming.ConnectionStateTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.ConnectionState

  # A mock gRPC client that records calls and returns configurable results.
  defmodule MockGrpcClient do
    @behaviour BroadwayCloudPubSub.Streaming.Client

    def init(opts), do: {:ok, Map.new(opts)}

    def connect(%{connect_result: result}), do: result
    def connect(_config), do: {:ok, :fake_channel}

    def disconnect(_channel, _config), do: :ok

    def streaming_pull(_channel, _config), do: :fake_stream
    def send_request(stream, _request, _config), do: {:ok, stream}
    def recv(_stream, _config), do: {:ok, []}
    def cancel(_stream, _config), do: :ok
    def acknowledge(_channel, _request, _config), do: {:ok, :ok}
    def modify_ack_deadline(_channel, _request, _config), do: {:ok, :ok}
  end

  defp new_conn(overrides \\ %{}) do
    config = Map.merge(%{}, overrides)

    ConnectionState.new(
      MockGrpcClient,
      config,
      type: :exp,
      min: 100,
      max: 60_000
    )
  end

  # Builds a fake grpc_stream struct that satisfies stream_opened's
  # pattern: grpc_stream.channel.adapter_payload.conn_pid
  defp fake_grpc_stream(conn_pid \\ self()) do
    %{channel: %{adapter_payload: %{conn_pid: conn_pid}}}
  end

  describe "new/3" do
    test "creates a ConnectionState with all connection fields nil'd" do
      conn = new_conn()

      assert conn.grpc_client == MockGrpcClient
      assert conn.channel == nil
      assert conn.grpc_stream == nil
      assert conn.conn_pid == nil
      assert conn.reader_pid == nil
      assert conn.reconnect_ref == nil
      assert conn.keepalive_timer == nil
      assert conn.backoff != nil
    end
  end

  describe "stream_opened/3" do
    test "stores stream, extracts conn_pid, resets backoff" do
      conn = new_conn()
      stream = fake_grpc_stream()

      conn = ConnectionState.stream_opened(conn, self(), stream)

      assert conn.grpc_stream == stream
      assert conn.conn_pid == self()
    end
  end

  describe "send_on_stream/2" do
    test "returns {:ok, conn} when stream is open" do
      conn = %{new_conn() | grpc_stream: :fake_stream}

      assert {:ok, updated_conn} = ConnectionState.send_on_stream(conn, :some_request)
      assert updated_conn.grpc_stream == :fake_stream
    end

    test "returns {:error, :no_stream} when stream is nil" do
      conn = new_conn()

      assert {:error, :no_stream} = ConnectionState.send_on_stream(conn, :some_request)
    end
  end

  describe "schedule_reconnect/1" do
    test "schedules a :connect message and returns {conn, delay}" do
      conn = new_conn()

      {conn, delay} = ConnectionState.schedule_reconnect(conn)

      assert is_integer(delay) and delay > 0
      assert conn.reconnect_ref != nil
      assert_receive :connect, delay + 100
    end

    test "is a no-op when reconnect already pending" do
      conn = new_conn()
      {conn, delay1} = ConnectionState.schedule_reconnect(conn)

      {conn2, delay2} = ConnectionState.schedule_reconnect(conn)

      assert delay2 == nil
      assert conn2.reconnect_ref == conn.reconnect_ref
      assert_receive :connect, delay1 + 100
    end

    test "raises when backoff is nil (stop)" do
      conn = %{new_conn() | backoff: nil}

      assert_raise RuntimeError, ~r/backoff is :stop/, fn ->
        ConnectionState.schedule_reconnect(conn)
      end
    end
  end

  describe "clear_reconnect_ref/1" do
    test "nils out the reconnect_ref" do
      conn = new_conn()
      {conn, _delay} = ConnectionState.schedule_reconnect(conn)
      assert conn.reconnect_ref != nil

      conn = ConnectionState.clear_reconnect_ref(conn)
      assert conn.reconnect_ref == nil
    end
  end

  describe "close/1" do
    test "is a no-op when already closed" do
      conn = new_conn()
      closed = ConnectionState.close(conn)

      assert closed.channel == nil
      assert closed.grpc_stream == nil
      assert closed.reader_pid == nil
      assert closed.conn_pid == nil
    end

    test "nils all connection fields when open" do
      conn = %{new_conn() | channel: :fake_channel, grpc_stream: :fake_stream, conn_pid: self()}

      closed = ConnectionState.close(conn)

      assert closed.channel == nil
      assert closed.grpc_stream == nil
      assert closed.conn_pid == nil
      assert closed.reader_pid == nil
    end
  end

  describe "close_reader/1" do
    test "is a no-op when reader_pid is nil" do
      conn = new_conn()
      assert ConnectionState.close_reader(conn) == conn
    end

    test "kills the reader and nils reader_pid" do
      # Spawn a process to act as the reader
      {:ok, reader} = Task.start_link(fn -> Process.sleep(:infinity) end)
      conn = %{new_conn() | reader_pid: reader}

      updated = ConnectionState.close_reader(conn)

      assert updated.reader_pid == nil
      refute Process.alive?(reader)
    end
  end

  describe "mark_stream_closed/1" do
    test "nils grpc_stream without touching other fields" do
      conn = %{new_conn() | grpc_stream: :fake_stream, channel: :fake_channel}

      updated = ConnectionState.mark_stream_closed(conn)

      assert updated.grpc_stream == nil
      assert updated.channel == :fake_channel
    end
  end

  describe "keepalive" do
    test "schedule_keepalive/2 sets a timer" do
      conn = new_conn()
      config = %{keepalive_interval_ms: 50}

      conn = ConnectionState.schedule_keepalive(conn, config)

      assert conn.keepalive_timer != nil
      assert_receive :send_keepalive, 200
    end

    test "cancel_keepalive/1 cancels the timer" do
      conn = new_conn()
      config = %{keepalive_interval_ms: 5_000}
      conn = ConnectionState.schedule_keepalive(conn, config)

      conn = ConnectionState.cancel_keepalive(conn)

      assert conn.keepalive_timer == nil
      refute_receive :send_keepalive, 50
    end

    test "cancel_keepalive/1 is a no-op when no timer" do
      conn = new_conn()
      assert ConnectionState.cancel_keepalive(conn) == conn
    end
  end

  describe "connected?/1" do
    test "returns false when grpc_stream is nil" do
      refute ConnectionState.connected?(new_conn())
    end

    test "returns true when grpc_stream is set" do
      conn = %{new_conn() | grpc_stream: :fake_stream}
      assert ConnectionState.connected?(conn)
    end
  end

  describe "reader_pid/1" do
    test "returns the reader pid" do
      conn = %{new_conn() | reader_pid: self()}
      assert ConnectionState.reader_pid(conn) == self()
    end

    test "returns nil when no reader" do
      assert ConnectionState.reader_pid(new_conn()) == nil
    end
  end

  describe "conn_pid/1" do
    test "returns the conn pid" do
      conn = %{new_conn() | conn_pid: self()}
      assert ConnectionState.conn_pid(conn) == self()
    end

    test "returns nil when no conn" do
      assert ConnectionState.conn_pid(new_conn()) == nil
    end
  end
end
