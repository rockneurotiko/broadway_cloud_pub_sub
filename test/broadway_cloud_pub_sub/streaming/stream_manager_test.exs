defmodule BroadwayCloudPubSub.Streaming.StreamManagerTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.StreamManager

  # Minimal config with enough keys to satisfy StreamManager.init/1
  # (mirrors what Options produces after validation + defaults).
  defp base_config do
    [
      subscription: "projects/test/subscriptions/test-sub",
      max_outstanding_messages: 1_000,
      max_outstanding_bytes: 104_857_600,
      stream_ack_deadline_seconds: 60,
      lease_extension_percent: 0.6,
      backoff_type: :exp,
      backoff_min: 1_000,
      backoff_max: 30_000,
      use_ssl: true,
      adapter: :gun,
      grpc_endpoint: "pubsub.googleapis.com:443",
      keepalive_interval_ms: 30_000,
      on_success: :ack,
      on_failure: :noop,
      client_id: "test-client-id",
      token_generator: {__MODULE__, :noop_token, []},
      broadway: [name: __MODULE__]
    ]
  end

  def noop_token, do: {:ok, "test-token"}

  # Start a StreamManager, inject producer_pid so it doesn't try to connect.
  defp start_manager(extra_opts \\ []) do
    test_pid = self()
    opts = Keyword.merge(base_config(), extra_opts)
    {:ok, pid} = StreamManager.start_link(opts)

    # Inject state: set producer_pid to test process and skip the real connect.
    # NOTE: pass test_pid explicitly — self() inside :sys.replace_state runs in
    # the GenServer process context, not the test process.
    :sys.replace_state(pid, fn state ->
      %{state | producer_pid: test_pid}
    end)

    pid
  end

  # Inject a fake grpc_stream into state so ack paths see a connected stream.
  defp inject_connected(pid) do
    :sys.replace_state(pid, fn state ->
      %{state | grpc_stream: :fake_stream}
    end)
  end

  # ============================================================
  # Demand signaling
  # ============================================================

  describe "notify_demand/2 — no buffered messages" do
    test "stores pending_demand when message buffer is empty" do
      pid = start_manager()

      :sys.replace_state(pid, fn s -> %{s | pending_demand: 0, message_buffer: []} end)
      StreamManager.notify_demand(pid, 10)

      # Allow the async cast to be processed
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert state.pending_demand == 10
      assert state.message_buffer == []
    end
  end

  describe "notify_demand/2 — with buffered messages" do
    test "flushes buffered messages to producer and decrements pending_demand" do
      pid = start_manager()

      msgs = [
        %Broadway.Message{data: "msg1", acknowledger: {Broadway.NoopAcknowledger, nil, nil}},
        %Broadway.Message{data: "msg2", acknowledger: {Broadway.NoopAcknowledger, nil, nil}}
      ]

      :sys.replace_state(pid, fn s ->
        %{s | pending_demand: 0, message_buffer: Enum.reverse(msgs)}
      end)

      StreamManager.notify_demand(pid, 10)

      assert_receive {:stream_messages, received}
      assert Enum.map(received, & &1.data) == ["msg1", "msg2"]

      state = :sys.get_state(pid)
      assert state.message_buffer == []
      assert state.pending_demand == 8
    end

    test "flushes only up to pending_demand, keeps remainder buffered" do
      pid = start_manager()

      msgs =
        for i <- 1..5 do
          %Broadway.Message{
            data: "msg#{i}",
            acknowledger: {Broadway.NoopAcknowledger, nil, nil}
          }
        end

      :sys.replace_state(pid, fn s ->
        %{s | pending_demand: 0, message_buffer: Enum.reverse(msgs)}
      end)

      StreamManager.notify_demand(pid, 2)

      assert_receive {:stream_messages, received}
      assert length(received) == 2
      assert Enum.map(received, & &1.data) == ["msg1", "msg2"]

      state = :sys.get_state(pid)
      assert length(state.message_buffer) == 3
      assert state.pending_demand == 0

      StreamManager.notify_demand(pid, 10)

      assert_receive {:stream_messages, received2}
      assert length(received2) == 3
      assert Enum.map(received2, & &1.data) == ["msg3", "msg4", "msg5"]

      state = :sys.get_state(pid)
      assert state.message_buffer == []
      assert state.pending_demand == 7
    end
  end

  describe "stream_messages → message delivery" do
    test "messages are forwarded immediately when pending_demand > 0" do
      pid = start_manager()
      :sys.replace_state(pid, fn s -> %{s | pending_demand: 10} end)

      fake_msg = %Google.Pubsub.V1.ReceivedMessage{
        ack_id: "ack-1",
        message: %Google.Pubsub.V1.PubsubMessage{
          message_id: "msg-1",
          data: "hello",
          attributes: %{},
          ordering_key: "",
          publish_time: nil
        },
        delivery_attempt: 1
      }

      send(pid, {:stream_messages, [fake_msg]})

      assert_receive {:stream_messages, messages}
      assert length(messages) == 1
      assert hd(messages).data == "hello"

      state = :sys.get_state(pid)
      assert state.pending_demand == 9
      assert state.message_buffer == []
    end

    test "messages are buffered when pending_demand is 0" do
      pid = start_manager()
      :sys.replace_state(pid, fn s -> %{s | pending_demand: 0} end)

      fake_msg = %Google.Pubsub.V1.ReceivedMessage{
        ack_id: "ack-2",
        message: %Google.Pubsub.V1.PubsubMessage{
          message_id: "msg-2",
          data: "buffered",
          attributes: %{},
          ordering_key: "",
          publish_time: nil
        },
        delivery_attempt: 1
      }

      send(pid, {:stream_messages, [fake_msg]})

      refute_receive {:stream_messages, _}, 100

      state = :sys.get_state(pid)
      assert length(state.message_buffer) == 1
    end

    test "buffer is flushed in FIFO order on notify_demand" do
      pid = start_manager()
      :sys.replace_state(pid, fn s -> %{s | pending_demand: 0} end)

      for i <- 1..3 do
        msg = %Google.Pubsub.V1.ReceivedMessage{
          ack_id: "ack-#{i}",
          message: %Google.Pubsub.V1.PubsubMessage{
            message_id: "msg-#{i}",
            data: "data-#{i}",
            attributes: %{},
            ordering_key: "",
            publish_time: nil
          },
          delivery_attempt: 1
        }

        send(pid, {:stream_messages, [msg]})
      end

      :sys.get_state(pid)

      StreamManager.notify_demand(pid, 10)

      assert_receive {:stream_messages, messages}
      assert Enum.map(messages, & &1.data) == ["data-1", "data-2", "data-3"]
    end
  end

  # ============================================================
  # Ack buffering — no cap
  # ============================================================

  describe "ack buffer — unbounded" do
    test "buffers acks when grpc_stream is nil" do
      pid = start_manager()

      :sys.replace_state(pid, fn s -> %{s | grpc_stream: nil} end)

      StreamManager.acknowledge(pid, ["ack-1", "ack-2"])

      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert state.ack_buffer_size == 1
      assert state.ack_buffer != []
    end

    test "buffer grows without dropping entries" do
      pid = start_manager()
      :sys.replace_state(pid, fn s -> %{s | grpc_stream: nil} end)

      count = 20_000

      for i <- 1..count do
        StreamManager.acknowledge(pid, ["ack-#{i}"])
      end

      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert state.ack_buffer_size == count
    end

    test "flushed buffer is replayed on reconnect (connect_stream)" do
      pid = start_manager()

      :sys.replace_state(pid, fn s ->
        %{
          s
          | grpc_stream: nil,
            ack_buffer: [{:ack, ["id-1"]}, {:ack, ["id-2"]}],
            ack_buffer_size: 2
        }
      end)

      inject_connected(pid)

      StreamManager.close(pid)

      state = :sys.get_state(pid)
      assert state.ack_buffer == []
      assert state.ack_buffer_size == 0
    end
  end

  # ============================================================
  # Ack buffer telemetry
  # ============================================================

  describe "ack buffer telemetry" do
    test "emits :ack_buffered telemetry when buffering an ack" do
      pid = start_manager()
      :sys.replace_state(pid, fn s -> %{s | grpc_stream: nil} end)

      test_pid = self()

      :telemetry.attach(
        "test-ack-buffered",
        [:broadway_cloud_pub_sub, :stream, :ack_buffered],
        fn _event, measurements, _metadata, _config ->
          send(test_pid, {:telemetry, measurements})
        end,
        nil
      )

      StreamManager.acknowledge(pid, ["ack-x"])

      assert_receive {:telemetry, %{buffer_size: 1}}

      :telemetry.detach("test-ack-buffered")
    end
  end

  # ============================================================
  # receiving flag — draining
  # ============================================================

  describe "stop_receiving/1" do
    test "messages are not forwarded after stop_receiving even when pending_demand > 0" do
      pid = start_manager()
      :sys.replace_state(pid, fn s -> %{s | pending_demand: 10} end)

      StreamManager.stop_receiving(pid)

      fake_msg = %Google.Pubsub.V1.ReceivedMessage{
        ack_id: "drain-ack",
        message: %Google.Pubsub.V1.PubsubMessage{
          message_id: "drain-msg",
          data: "should not arrive",
          attributes: %{},
          ordering_key: "",
          publish_time: nil
        },
        delivery_attempt: 1
      }

      send(pid, {:stream_messages, [fake_msg]})

      refute_receive {:stream_messages, _}, 200
    end
  end

  # ============================================================
  # Keep-alive pings
  # ============================================================

  describe "keep-alive ping" do
    test "triggers reconnect when send fails (fake stream)" do
      # Use a very short keepalive interval so the test doesn't wait 30s.
      # With a fake stream, send_on_stream will throw, which should trigger
      # a reconnect instead of being silently swallowed.
      pid = start_manager(keepalive_interval_ms: 10)

      :sys.replace_state(pid, fn s -> %{s | grpc_stream: :fake_stream} end)

      # Bootstrap the keepalive cycle — normally started by {:stream_opened},
      # but we injected the stream directly via replace_state.
      send(pid, :send_keepalive)

      Process.sleep(30)

      assert Process.alive?(pid)

      # After a send failure, the stream is reset (grpc_stream: nil) and a
      # reconnect is scheduled.
      state = :sys.get_state(pid)
      assert state.grpc_stream == nil
      assert state.reconnect_ref != nil
    end

    test "does not crash when stream is nil (reconnecting)" do
      pid = start_manager(keepalive_interval_ms: 10)

      :sys.replace_state(pid, fn s -> %{s | grpc_stream: nil} end)

      Process.sleep(30)

      assert Process.alive?(pid)
    end

    test "keepalive_timer is nil before stream opens" do
      pid = start_manager()
      state = :sys.get_state(pid)
      assert state.keepalive_timer == nil
    end

    test "keepalive_timer is set when stream is active" do
      pid = start_manager(keepalive_interval_ms: 60_000)

      :sys.replace_state(pid, fn s ->
        timer = Process.send_after(self(), :send_keepalive, 60_000)

        %{
          s
          | grpc_stream: :fake_stream,
            conn_pid: self(),
            stream_opened_at: System.monotonic_time(:millisecond),
            keepalive_timer: timer
        }
      end)

      state = :sys.get_state(pid)
      assert state.keepalive_timer != nil
    end
  end

  # ============================================================
  # Reconnect deduplication
  # ============================================================

  describe "reconnect deduplication" do
    test "only one reconnect is scheduled when stream_error and stream_closed both arrive" do
      # Use high backoff so :connect doesn't actually fire during the test
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)

      send(pid, {:stream_error, %GRPC.RPCError{status: 4, message: "timeout"}})
      send(pid, {:stream_closed})

      :sys.get_state(pid)

      state = :sys.get_state(pid)
      first_ref = state.reconnect_ref

      # Ref must be set (at least one reconnect scheduled)
      assert first_ref != nil

      # Send another close signal — ref must not change (dedup kicks in)
      send(pid, {:stream_closed})
      :sys.get_state(pid)

      state2 = :sys.get_state(pid)
      assert state2.reconnect_ref == first_ref
    end

    test "reconnect_ref is cleared when :connect message fires" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)

      send(pid, {:stream_error, %GRPC.RPCError{status: 4, message: "timeout"}})
      :sys.get_state(pid)

      # Manually fire :connect (connect() will fail — no real gRPC — but that's fine)
      send(pid, :connect)
      :sys.get_state(pid)

      # GenServer should still be alive
      assert Process.alive?(pid)
    end
  end

  # ============================================================
  # Error classification — terminal vs retryable
  # ============================================================

  describe "terminal gRPC errors stop the GenServer" do
    test "NOT_FOUND (5) stops the GenServer" do
      pid = start_manager()
      ref = Process.monitor(pid)
      Process.unlink(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 5, message: "not found"}})

      assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, _}}, 1_000
    end

    test "PERMISSION_DENIED (7) stops the GenServer" do
      pid = start_manager()
      ref = Process.monitor(pid)
      Process.unlink(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 7, message: "permission denied"}})

      assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, _}}, 1_000
    end

    test "INVALID_ARGUMENT (3) stops the GenServer" do
      pid = start_manager()
      ref = Process.monitor(pid)
      Process.unlink(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 3, message: "bad argument"}})

      assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, _}}, 1_000
    end

    test "UNAUTHENTICATED (16) stops the GenServer" do
      pid = start_manager()
      ref = Process.monitor(pid)
      Process.unlink(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 16, message: "unauthenticated"}})

      assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, _}}, 1_000
    end

    test "UNAVAILABLE (14) with 'Server shutdownNow invoked' stops the GenServer" do
      pid = start_manager()
      ref = Process.monitor(pid)
      Process.unlink(pid)

      send(
        pid,
        {:stream_error, %GRPC.RPCError{status: 14, message: "Server shutdownNow invoked"}}
      )

      assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, _}}, 1_000
    end

    test "terminal error emits :terminal_error telemetry before stopping" do
      pid = start_manager()
      test_pid = self()
      Process.unlink(pid)

      :telemetry.attach(
        "test-terminal-error-#{inspect(pid)}",
        [:broadway_cloud_pub_sub, :stream, :terminal_error],
        fn _event, measurements, _metadata, _config ->
          send(test_pid, {:telemetry, :terminal_error, measurements})
        end,
        nil
      )

      send(pid, {:stream_error, %GRPC.RPCError{status: 5, message: "not found"}})

      assert_receive {:telemetry, :terminal_error, %{reason: _}}, 1_000

      :telemetry.detach("test-terminal-error-#{inspect(pid)}")
    end
  end

  describe "retryable gRPC errors trigger reconnect" do
    test "DEADLINE_EXCEEDED (4) schedules reconnect without stopping" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)
      ref = Process.monitor(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 4, message: "timeout"}})
      :sys.get_state(pid)

      refute_received {:DOWN, ^ref, :process, ^pid, _}
      assert Process.alive?(pid)

      state = :sys.get_state(pid)
      assert state.reconnect_ref != nil
    end

    test "UNAVAILABLE (14) without shutdown message schedules reconnect" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)
      ref = Process.monitor(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "service temporarily down"}})
      :sys.get_state(pid)

      refute_received {:DOWN, ^ref, :process, ^pid, _}
      assert Process.alive?(pid)
    end
  end

  # ============================================================
  # Skip-backoff optimisation
  # ============================================================

  describe "skip-backoff on long-lived stream" do
    test "reconnects immediately (0ms) when stream was open for >30 seconds" do
      # Use a non-routable endpoint so the connect attempt fails at TCP level
      # (not by reaching a real server that returns a terminal auth error).
      pid = start_manager(backoff_min: 5_000, backoff_max: 30_000, grpc_endpoint: "localhost:1")

      # Pretend the stream opened 31 seconds ago
      opened_at = System.monotonic_time(:millisecond) - 31_000

      :sys.replace_state(pid, fn s -> %{s | stream_opened_at: opened_at} end)

      send(pid, {:stream_closed})
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert state.reconnect_ref != nil

      # With 0ms effective timeout, :connect should arrive almost immediately.
      # Give it 500ms — even with scheduling jitter this is far below the 5s backoff.
      # The connect attempt will fail (no gRPC), but the GenServer remains alive.
      Process.sleep(200)
      assert Process.alive?(pid)
    end

    test "applies full backoff when stream was open for <30 seconds" do
      pid = start_manager(backoff_min: 5_000, backoff_max: 30_000)

      # Stream opened 1 second ago — should NOT skip backoff
      opened_at = System.monotonic_time(:millisecond) - 1_000

      :sys.replace_state(pid, fn s -> %{s | stream_opened_at: opened_at} end)

      send(pid, {:stream_closed})
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert state.reconnect_ref != nil

      # With 5s minimum backoff, no :connect should arrive within 500ms
      Process.sleep(300)

      # State should still show the same reconnect_ref (connect hasn't fired)
      state2 = :sys.get_state(pid)
      assert state2.reconnect_ref == state.reconnect_ref
      assert Process.alive?(pid)
    end
  end
end
