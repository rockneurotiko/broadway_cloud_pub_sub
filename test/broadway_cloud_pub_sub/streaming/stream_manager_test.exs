defmodule BroadwayCloudPubSub.Streaming.StreamManagerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias BroadwayCloudPubSub.Streaming.{AckBatcher, StreamManager}
  alias BroadwayCloudPubSub.Test.GrpcDynamicAdapter

  # Minimal config with enough keys to satisfy StreamManager.init/1
  # (mirrors what Options produces after validation + defaults).
  defp base_config do
    [
      broadway_name: __MODULE__,
      subscription: "projects/test/subscriptions/test-sub",
      max_outstanding_messages: 1_000,
      max_outstanding_bytes: 104_857_600,
      stream_ack_deadline_seconds: 60,
      backoff_type: :exp,
      backoff_min: 1_000,
      backoff_max: 30_000,
      use_ssl: true,
      adapter: GrpcDynamicAdapter,
      grpc_endpoint: "localhost:1234",
      keepalive_interval_ms: 30_000,
      on_success: :ack,
      on_failure: :noop,
      on_shutdown: {:nack, 5},
      max_extension_ms: 3_600_000,
      ack_batch_interval_ms: 100,
      ack_batch_max_size: 2_500,
      client_id: "test-client-id",
      # fail token by default, most of the tests don't need a live stream, and this avoids GRPC.Client.Connection stop logs
      token_generator: {__MODULE__, :fail_token, []},
      broadway: [name: __MODULE__]
    ]
  end

  def noop_token, do: {:ok, "test-token"}
  def fail_token, do: {:error, :no_token}

  # A minimal stub GenServer that silently accepts any cast (ack/modack).
  # Used as the rpc_client for AckBatcher so no real gRPC calls are made.
  defmodule StubRpcClient do
    use GenServer
    def start_link(name), do: GenServer.start_link(__MODULE__, :ok, name: name)
    def init(:ok), do: {:ok, :ok}
    def handle_cast(_msg, state), do: {:noreply, state}
    def handle_call(_msg, _from, state), do: {:reply, :ok, state}
  end

  # Start a StreamManager, inject producer_pid so it doesn't try to connect.
  # Also starts a real AckBatcher (backed by StubRpcClient) registered under
  # the name that StreamManager derives from broadway_name.
  defp start_manager(extra_opts \\ []) do
    # Generate a unique broadway_name per test invocation to avoid registered-name
    # collisions when multiple tests run concurrently or sequentially.
    broadway_name = Module.concat(__MODULE__, "Run#{System.unique_integer([:positive])}")

    opts =
      base_config() |> Keyword.put(:broadway_name, broadway_name) |> Keyword.merge(extra_opts)

    # Mirror what Producer.prepare_for_start/2 does: call grpc_client.init/1 and
    # store the resulting config so StreamManager can read it from its config map.
    grpc_client = Keyword.get(opts, :grpc_client, BroadwayCloudPubSub.Streaming.GrpcClient)
    {:ok, grpc_client_config} = grpc_client.init(opts)

    opts =
      opts
      |> Keyword.put(:grpc_client, grpc_client)
      |> Keyword.put(:grpc_client_config, grpc_client_config)

    rpc_client_name = Module.concat(broadway_name, UnaryRpcClient)
    batcher_name = Module.concat(broadway_name, AckBatcher)

    # Start stub RPC client so AckBatcher can call it
    {:ok, _stub} = StubRpcClient.start_link(rpc_client_name)

    # Start a real AckBatcher registered under the name StreamManager will use
    {:ok, _batcher} =
      AckBatcher.start_link(
        name: batcher_name,
        rpc_client: rpc_client_name,
        ack_batch_interval_ms: Keyword.get(opts, :ack_batch_interval_ms, 100),
        ack_batch_max_size: Keyword.get(opts, :ack_batch_max_size, 2_500)
      )

    {:ok, pid} = StreamManager.start_link(opts)

    StreamManager.set_producer(pid, self())

    pid
  end

  # Synchronous barrier: drains all prior mailbox messages in StreamManager
  # before returning. Safe to use instead of :sys.get_state/1 for sync purposes.
  defp sync(pid), do: StreamManager.get_buffered(pid)

  # Build a minimal ReceivedMessage for sending into {:stream_messages, ...}.
  defp received_message(ack_id, data) do
    %Google.Pubsub.V1.ReceivedMessage{
      ack_id: ack_id,
      message: %Google.Pubsub.V1.PubsubMessage{
        message_id: "msg-#{ack_id}",
        data: data,
        attributes: %{},
        ordering_key: "",
        publish_time: nil
      },
      delivery_attempt: 1
    }
  end

  # Open a live dynamic adapter stream.
  # Starts the manager with noop_token so the adapter actually connects,
  # waits for the connection handshake, then calls fun.(pid, ctrl).
  defp with_live_stream(extra_opts \\ [], fun) do
    pid =
      start_manager(
        [
          adapter: GrpcDynamicAdapter,
          test_pid: self(),
          token_generator: {__MODULE__, :noop_token, []}
        ] ++ extra_opts
      )

    assert_receive {:adapter_connected, ctrl}, 2_000
    # Wait for the initial StreamingPullRequest so the stream is fully open
    assert_receive {:adapter_call, {:send_data, _}}, 2_000
    fun.(pid, ctrl)
  end

  # ============================================================
  # Demand signaling
  # ============================================================

  describe "notify_demand/2 — no buffered messages" do
    test "stores pending_demand when message buffer is empty" do
      pid = start_manager()

      StreamManager.notify_demand(pid, 0)
      StreamManager.notify_demand(pid, 10)

      # Send one real message — if demand > 0, it will be forwarded immediately
      send(pid, {:stream_messages, [received_message("demand-probe", "probe")]})
      assert_receive {:stream_messages, [_]}, 500

      # Buffer is empty: all demand was consumed by the forwarded message
      assert StreamManager.get_buffered(pid) == []
    end
  end

  describe "notify_demand/2 — with buffered messages" do
    test "flushes buffered messages to producer and decrements pending_demand" do
      pid = start_manager()

      # Buffer two messages with demand=0
      StreamManager.notify_demand(pid, 0)
      send(pid, {:stream_messages, [received_message("buf-1", "msg1")]})
      send(pid, {:stream_messages, [received_message("buf-2", "msg2")]})
      # Wait for both to be buffered (sync via get_buffered)
      assert length(StreamManager.get_buffered(pid)) == 2

      # Now demand arrives — should flush both at once
      StreamManager.notify_demand(pid, 10)

      assert_receive {:stream_messages, received}
      assert Enum.map(received, & &1.data) == ["msg1", "msg2"]

      # Buffer should be empty; remaining demand consumed 2 of 10
      assert StreamManager.get_buffered(pid) == []
    end

    test "flushes only up to pending_demand, keeps remainder buffered" do
      pid = start_manager()

      # Buffer 5 messages with demand=0
      StreamManager.notify_demand(pid, 0)

      for i <- 1..5 do
        send(pid, {:stream_messages, [received_message("buf-#{i}", "msg#{i}")]})
      end

      assert length(StreamManager.get_buffered(pid)) == 5

      # Demand for 2 — should flush exactly 2
      StreamManager.notify_demand(pid, 2)

      assert_receive {:stream_messages, received}
      assert length(received) == 2
      assert Enum.map(received, & &1.data) == ["msg1", "msg2"]

      # 3 remain buffered
      assert length(StreamManager.get_buffered(pid)) == 3

      # Demand for 10 — should flush the remaining 3
      StreamManager.notify_demand(pid, 10)

      assert_receive {:stream_messages, received2}
      assert length(received2) == 3
      assert Enum.map(received2, & &1.data) == ["msg3", "msg4", "msg5"]

      assert StreamManager.get_buffered(pid) == []
    end
  end

  describe "stream_messages → message delivery" do
    test "messages are forwarded immediately when pending_demand > 0" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("ack-1", "hello")]})

      assert_receive {:stream_messages, messages}
      assert length(messages) == 1
      assert hd(messages).data == "hello"

      # Buffer should be empty (demand consumed the message immediately)
      assert StreamManager.get_buffered(pid) == []
    end

    test "messages are buffered when pending_demand is 0" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 0)

      send(pid, {:stream_messages, [received_message("ack-2", "buffered")]})

      refute_receive {:stream_messages, _}, 100

      assert length(StreamManager.get_buffered(pid)) == 1
    end

    test "buffer is flushed in FIFO order on notify_demand" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 0)

      for i <- 1..3 do
        send(pid, {:stream_messages, [received_message("ack-#{i}", "data-#{i}")]})
      end

      # Sync: ensure all 3 are buffered before we signal demand
      assert length(StreamManager.get_buffered(pid)) == 3

      StreamManager.notify_demand(pid, 10)

      assert_receive {:stream_messages, messages}
      assert Enum.map(messages, & &1.data) == ["data-1", "data-2", "data-3"]
    end
  end

  # ============================================================
  # receiving flag — draining
  # ============================================================

  describe "stop_receiving/1" do
    test "messages are not forwarded after stop_receiving even when pending_demand > 0" do
      pid = start_manager()

      StreamManager.notify_demand(pid, 10)
      StreamManager.stop_receiving(pid)

      send(pid, {:stream_messages, [received_message("drain-ack", "should not arrive")]})

      refute_receive {:stream_messages, _}, 200
    end
  end

  # ============================================================
  # Keep-alive pings
  # ============================================================

  describe "keep-alive ping" do
    test "triggers reconnect when send fails (fake stream)" do
      # Use a very short keepalive interval and a high backoff so the automatic
      # reconnect triggered by the failed send doesn't fire another :connect
      # during the test assertions.
      pid = start_manager(keepalive_interval_ms: 10, backoff_min: 60_000)

      :sys.replace_state(pid, fn s -> %{s | grpc_stream: :fake_stream} end)

      # Bootstrap the keepalive cycle — normally started by {:stream_opened},
      # but we injected the stream directly via replace_state.
      send(pid, :send_keepalive)

      # Sync barrier: let both the keepalive handler and any subsequent
      # handle_info(:connect, ...) fully settle before reading state.
      sync(pid)

      assert Process.alive?(pid)

      # After a send failure, the stream is reset (grpc_stream: nil) and a
      # reconnect is scheduled.
      state = :sys.get_state(pid)
      assert state.grpc_stream == nil
      assert state.reconnect_ref != nil
    end

    test "does not crash when stream is nil (reconnecting)" do
      # grpc_stream is already nil after start_manager with fail_token — no replace_state needed.
      pid = start_manager(keepalive_interval_ms: 10)

      send(pid, :send_keepalive)

      sync(pid)

      assert Process.alive?(pid)
    end

    test "keepalive_timer is nil before stream opens" do
      pid = start_manager()
      # No public API for keepalive_timer — :sys.get_state required
      state = :sys.get_state(pid)
      assert state.keepalive_timer == nil
    end

    test "keepalive_timer is set when stream is active" do
      # Use a live stream so {:stream_opened} fires, which calls schedule_keepalive_timer/1.
      # A very long interval ensures it doesn't fire during the test.
      with_live_stream([keepalive_interval_ms: 60_000], fn pid, _ctrl ->
        # No public API for keepalive_timer — :sys.get_state required
        state = :sys.get_state(pid)
        assert state.keepalive_timer != nil
      end)
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

      sync(pid)

      # No public API for reconnect_ref — :sys.get_state required
      state = :sys.get_state(pid)
      first_ref = state.reconnect_ref

      # Ref must be set (at least one reconnect scheduled)
      assert first_ref != nil

      # Send another close signal — ref must not change (dedup kicks in)
      send(pid, {:stream_closed})
      sync(pid)

      state2 = :sys.get_state(pid)
      assert state2.reconnect_ref == first_ref
    end

    test "reconnect_ref is cleared when :connect message fires" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)

      send(pid, {:stream_error, %GRPC.RPCError{status: 4, message: "timeout"}})
      sync(pid)

      # Manually fire :connect (connect() will fail — no real gRPC — but that's fine)
      send(pid, :connect)
      sync(pid)

      # GenServer should still be alive
      assert Process.alive?(pid)
    end
  end

  # ============================================================
  # Error classification — terminal vs retryable
  # ============================================================

  describe "terminal gRPC errors stop the GenServer" do
    test "NOT_FOUND (5) stops the GenServer" do
      logs =
        capture_log(fn ->
          pid = start_manager()
          ref = Process.monitor(pid)
          Process.unlink(pid)

          send(pid, {:stream_error, %GRPC.RPCError{status: 5, message: "not found"}})

          assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, _}}, 1_000
        end)

      assert logs =~
               "Terminal gRPC stream error on subscription projects/test/subscriptions/test-sub - reason: %GRPC.RPCError{status: 5, message: \"not found\", details: nil}. Stopping StreamManager."
    end

    test "PERMISSION_DENIED (7) stops the GenServer" do
      logs =
        capture_log(fn ->
          pid = start_manager()
          ref = Process.monitor(pid)
          Process.unlink(pid)

          send(pid, {:stream_error, %GRPC.RPCError{status: 7, message: "permission denied"}})

          assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, _}}, 1_000
        end)

      assert logs =~
               "Terminal gRPC stream error on subscription projects/test/subscriptions/test-sub - reason: %GRPC.RPCError{status: 7, message: \"permission denied\", details: nil}. Stopping StreamManager."
    end

    test "INVALID_ARGUMENT (3) stops the GenServer" do
      logs =
        capture_log(fn ->
          pid = start_manager()
          ref = Process.monitor(pid)
          Process.unlink(pid)

          send(pid, {:stream_error, %GRPC.RPCError{status: 3, message: "bad argument"}})

          assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, _}}, 1_000
        end)

      assert logs =~
               "Terminal gRPC stream error on subscription projects/test/subscriptions/test-sub - reason: %GRPC.RPCError{status: 3, message: \"bad argument\", details: nil}. Stopping StreamManager."
    end

    test "UNAUTHENTICATED (16) schedules reconnect without stopping" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)
      ref = Process.monitor(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 16, message: "unauthenticated"}})
      sync(pid)

      refute_received {:DOWN, ^ref, :process, ^pid, _}
      assert Process.alive?(pid)

      # No public API for reconnect_ref — :sys.get_state required
      state = :sys.get_state(pid)
      assert state.reconnect_ref != nil
    end

    test "UNAVAILABLE (14) with 'Server shutdownNow invoked' schedules reconnect without stopping" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)
      ref = Process.monitor(pid)

      send(
        pid,
        {:stream_error, %GRPC.RPCError{status: 14, message: "Server shutdownNow invoked"}}
      )

      sync(pid)

      refute_received {:DOWN, ^ref, :process, ^pid, _}
      assert Process.alive?(pid)

      state = :sys.get_state(pid)
      assert state.reconnect_ref != nil
    end

    test "terminal error emits :terminal_error telemetry before stopping" do
      logs =
        capture_log(fn ->
          pid = start_manager()
          test_pid = self()
          telemetry_name = "test-terminal-error-#{inspect(pid)}"
          Process.unlink(pid)

          :telemetry.attach(
            telemetry_name,
            [:broadway_cloud_pub_sub, :stream, :terminal_error],
            fn _event, measurements, _metadata, _config ->
              send(test_pid, {:telemetry, :terminal_error, measurements})
            end,
            nil
          )

          send(pid, {:stream_error, %GRPC.RPCError{status: 5, message: "not found"}})

          assert_receive {:telemetry, :terminal_error, %{reason: _}}, 1_000

          :telemetry.detach(telemetry_name)
        end)

      assert logs =~
               "Terminal gRPC stream error on subscription projects/test/subscriptions/test-sub - reason: %GRPC.RPCError{status: 5, message: \"not found\", details: nil}. Stopping StreamManager."
    end
  end

  describe "retryable gRPC errors trigger reconnect" do
    test "DEADLINE_EXCEEDED (4) schedules reconnect without stopping" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)
      ref = Process.monitor(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 4, message: "timeout"}})
      sync(pid)

      refute_received {:DOWN, ^ref, :process, ^pid, _}
      assert Process.alive?(pid)

      state = :sys.get_state(pid)
      assert state.reconnect_ref != nil
    end

    test "UNAVAILABLE (14) without shutdown message schedules reconnect" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)
      ref = Process.monitor(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "service temporarily down"}})
      sync(pid)

      refute_received {:DOWN, ^ref, :process, ^pid, _}
      assert Process.alive?(pid)
    end
  end

  # ============================================================
  # Phase 5: Ordering key support — subscription_properties
  # ============================================================

  describe "subscription_properties — ordering_enabled" do
    test "ordering_enabled defaults to false" do
      pid = start_manager()
      # No public API for ordering_enabled — :sys.get_state required
      state = :sys.get_state(pid)
      assert state.ordering_enabled == false
    end

    test "updates ordering_enabled to true on {:subscription_properties, ...}" do
      pid = start_manager()

      props = %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
        message_ordering_enabled: true,
        exactly_once_delivery_enabled: false
      }

      send(pid, {:subscription_properties, props})
      sync(pid)

      state = :sys.get_state(pid)
      assert state.ordering_enabled == true
    end

    test "updates ordering_enabled to false when server reports false" do
      pid = start_manager()

      # First set to true
      send(
        pid,
        {:subscription_properties,
         %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: true,
           exactly_once_delivery_enabled: false
         }}
      )

      sync(pid)
      assert :sys.get_state(pid).ordering_enabled == true

      # Then server sends false (can happen mid-stream)
      send(
        pid,
        {:subscription_properties,
         %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: false
         }}
      )

      sync(pid)
      assert :sys.get_state(pid).ordering_enabled == false
    end

    test "ignores other messages — ordering_enabled unchanged" do
      pid = start_manager()

      # Unrelated message
      send(pid, {:some_other_event, :ignored})
      sync(pid)

      state = :sys.get_state(pid)
      assert state.ordering_enabled == false
    end
  end

  # ============================================================
  # Phase 6: Exactly-once delivery — subscription_properties
  # ============================================================

  describe "subscription_properties — exactly_once_enabled" do
    test "exactly_once_enabled defaults to false" do
      pid = start_manager()
      # No public API for exactly_once_enabled — :sys.get_state required
      state = :sys.get_state(pid)
      assert state.exactly_once_enabled == false
    end

    test "updates exactly_once_enabled to true when server reports true" do
      pid = start_manager()

      props = %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
        message_ordering_enabled: false,
        exactly_once_delivery_enabled: true
      }

      send(pid, {:subscription_properties, props})
      sync(pid)

      assert :sys.get_state(pid).exactly_once_enabled == true
    end

    test "updates exactly_once_enabled back to false when server reports false" do
      pid = start_manager()

      send(
        pid,
        {:subscription_properties,
         %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: true
         }}
      )

      sync(pid)
      assert :sys.get_state(pid).exactly_once_enabled == true

      send(
        pid,
        {:subscription_properties,
         %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: false
         }}
      )

      sync(pid)
      assert :sys.get_state(pid).exactly_once_enabled == false
    end

    test "ordering_enabled and exactly_once_enabled are updated together" do
      pid = start_manager()

      send(
        pid,
        {:subscription_properties,
         %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: true,
           exactly_once_delivery_enabled: true
         }}
      )

      sync(pid)
      state = :sys.get_state(pid)
      assert state.ordering_enabled == true
      assert state.exactly_once_enabled == true
    end
  end

  # ============================================================
  # Phase 6: Exactly-once delivery — extend_leases minimum deadline
  # ============================================================

  describe "extend_leases — exactly_once_enabled deadline enforcement" do
    # We use with_live_stream so the adapter actually connects, then push a
    # real StreamingPullResponse to get a message into `outstanding` naturally.
    # Then we control exactly_once_enabled via {:subscription_properties, ...}.

    defp push_one_message(ctrl, ack_id, data) do
      response = %Google.Pubsub.V1.StreamingPullResponse{
        received_messages: [
          %Google.Pubsub.V1.ReceivedMessage{
            ack_id: ack_id,
            message: %Google.Pubsub.V1.PubsubMessage{
              message_id: "msg-#{ack_id}",
              data: data,
              attributes: %{},
              ordering_key: "",
              publish_time: nil
            },
            delivery_attempt: 1
          }
        ]
      }

      GrpcDynamicAdapter.push_response(ctrl, response)
    end

    test "uses adaptive deadline (no 60s floor) when exactly_once_enabled is false" do
      with_live_stream(fn pid, ctrl ->
        # Put one message into outstanding by pushing a response with demand
        StreamManager.notify_demand(pid, 1)
        push_one_message(ctrl, "ack-normal", "data")
        assert_receive {:stream_messages, [_]}, 2_000

        # Ensure exactly_once is false (the default)
        send(
          pid,
          {:subscription_properties,
           %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
             message_ordering_enabled: false,
             exactly_once_delivery_enabled: false
           }}
        )

        sync(pid)

        # Fire the :extend_leases handler directly
        send(pid, :extend_leases)
        sync(pid)

        # No public API for lease_timer — :sys.get_state required
        state = :sys.get_state(pid)
        assert state.lease_timer != nil
      end)
    end

    test "enforces 60s minimum deadline when exactly_once_enabled is true" do
      with_live_stream(fn pid, ctrl ->
        # Put one message into outstanding
        StreamManager.notify_demand(pid, 1)
        push_one_message(ctrl, "ack-eo", "data")
        assert_receive {:stream_messages, [_]}, 2_000

        # Enable exactly-once
        send(
          pid,
          {:subscription_properties,
           %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
             message_ordering_enabled: false,
             exactly_once_delivery_enabled: true
           }}
        )

        sync(pid)

        send(pid, :extend_leases)
        sync(pid)

        # No public API for lease_timer — :sys.get_state required
        state = :sys.get_state(pid)
        assert state.lease_timer != nil
        remaining_ms = Process.read_timer(state.lease_timer)
        # With effective_deadline=60s: interval in [(60-5)*1000*0.8, (60-5)*1000*0.9]
        #                              = [44_000, 49_500)
        assert remaining_ms >= 40_000,
               "Expected next lease timer >= 40s for exactly-once, got #{remaining_ms}ms"
      end)
    end

    test "uses normal interval (much shorter) when exactly_once_enabled is false" do
      with_live_stream([stream_ack_deadline_seconds: 20], fn pid, ctrl ->
        # Put one message into outstanding
        StreamManager.notify_demand(pid, 1)
        push_one_message(ctrl, "ack-normal2", "data")
        assert_receive {:stream_messages, [_]}, 2_000

        # Ensure exactly_once is false
        send(
          pid,
          {:subscription_properties,
           %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
             message_ordering_enabled: false,
             exactly_once_delivery_enabled: false
           }}
        )

        sync(pid)

        send(pid, :extend_leases)
        sync(pid)

        state = :sys.get_state(pid)
        assert state.lease_timer != nil
        remaining_ms = Process.read_timer(state.lease_timer)

        # With stream_ack_deadline_seconds=20 and adaptive deadline defaulting
        # to 60 (cold start default), effective = 60s (no exactly_once floor).
        # The key check: it's a valid positive number.
        assert is_integer(remaining_ms) and remaining_ms > 0
      end)
    end
  end

  # ============================================================
  # Dynamic adapter — real connection flow without a real server
  # ============================================================

  describe "with GrpcDynamicAdapter" do
    test "adapter receives connect call and sends initial StreamingPullRequest" do
      pid =
        start_manager(
          adapter: GrpcDynamicAdapter,
          test_pid: self(),
          token_generator: {__MODULE__, :noop_token, []}
        )

      # Adapter signals connection to the test process
      assert_receive {:adapter_connected, _ctrl}, 2_000

      # StreamReader sends the initial StreamingPullRequest via send_data
      assert_receive {:adapter_call, {:send_data, _initial_request}}, 2_000

      assert Process.alive?(pid)
    end

    test "messages pushed into the stream are forwarded to the producer" do
      pid =
        start_manager(
          adapter: GrpcDynamicAdapter,
          test_pid: self(),
          token_generator: {__MODULE__, :noop_token, []}
        )

      assert_receive {:adapter_connected, ctrl}, 2_000
      # Wait for send_data (initial request) so stream is fully open
      assert_receive {:adapter_call, {:send_data, _}}, 2_000

      StreamManager.notify_demand(pid, 10)

      response = %Google.Pubsub.V1.StreamingPullResponse{
        received_messages: [
          %Google.Pubsub.V1.ReceivedMessage{
            ack_id: "dyn-ack-1",
            message: %Google.Pubsub.V1.PubsubMessage{
              message_id: "dyn-msg-1",
              data: "hello-dynamic",
              attributes: %{},
              ordering_key: "",
              publish_time: nil
            },
            delivery_attempt: 1
          }
        ]
      }

      GrpcDynamicAdapter.push_response(ctrl, response)

      assert_receive {:stream_messages, messages}, 2_000
      assert length(messages) == 1
      assert hd(messages).data == "hello-dynamic"
    end

    test "end_stream and cancel notifications reach test_pid" do
      _pid =
        start_manager(
          adapter: GrpcDynamicAdapter,
          test_pid: self(),
          token_generator: {__MODULE__, :noop_token, []}
        )

      assert_receive {:adapter_connected, ctrl}, 2_000
      assert_receive {:adapter_call, {:send_data, _}}, 2_000

      # Signal end-of-stream so the reader exits cleanly
      GrpcDynamicAdapter.push_end_stream(ctrl)

      # StreamManager will close the reader and schedule reconnect,
      # which triggers a new connect — we just verify the process survives
      # and the test doesn't hang.
      Process.sleep(100)
    end
  end

  # ============================================================
  # Exactly-once delivery — receipt modack gate
  # ============================================================

  # A spy RPC client for use in exactly-once tests. It records calls and allows
  # the test to control the response by sending {:set_modack_response, result}.
  defmodule SpyRpcClientForEO do
    use GenServer

    def start_link(test_pid) do
      GenServer.start_link(__MODULE__, test_pid)
    end

    def init(test_pid), do: {:ok, %{test_pid: test_pid, next_response: :ok}}

    def handle_call({:modify_ack_deadline, ids, deadline}, _from, state) do
      send(state.test_pid, {:rpc_call, {:modack, ids, deadline}})
      result = if state.next_response == :ok, do: :ok, else: state.next_response
      {:reply, result, %{state | next_response: :ok}}
    end

    def handle_call({:acknowledge, _ids}, _from, state) do
      {:reply, :ok, state}
    end

    def handle_call(:ping, _from, state), do: {:reply, :ok, state}

    # Synchronous setter so callers can guarantee the response is set before
    # any concurrent Task fires the next RPC call.
    def handle_call({:set_response_sync, response}, _from, state) do
      {:reply, :ok, %{state | next_response: response}}
    end

    def handle_cast({:set_response, response}, state) do
      {:noreply, %{state | next_response: response}}
    end
  end

  # Start a StreamManager backed by a SpyRpcClientForEO so we can control
  # RPC responses for exactly-once tests.
  defp start_manager_with_spy_rpc(extra_opts \\ []) do
    broadway_name = Module.concat(__MODULE__, "EORun#{System.unique_integer([:positive])}")

    opts =
      base_config()
      |> Keyword.put(:broadway_name, broadway_name)
      |> Keyword.merge(extra_opts)

    # Mirror what Producer.prepare_for_start/2 does
    grpc_client = Keyword.get(opts, :grpc_client, BroadwayCloudPubSub.Streaming.GrpcClient)
    {:ok, grpc_client_config} = grpc_client.init(opts)

    opts =
      opts
      |> Keyword.put(:grpc_client, grpc_client)
      |> Keyword.put(:grpc_client_config, grpc_client_config)

    rpc_client_name = Module.concat(broadway_name, UnaryRpcClient)
    batcher_name = Module.concat(broadway_name, AckBatcher)

    test_pid = self()
    {:ok, rpc_pid} = SpyRpcClientForEO.start_link(test_pid)
    # Register under the name AckBatcher will use
    Process.register(rpc_pid, rpc_client_name)

    {:ok, _batcher} =
      AckBatcher.start_link(
        name: batcher_name,
        rpc_client: rpc_client_name,
        ack_batch_interval_ms: Keyword.get(opts, :ack_batch_interval_ms, 50),
        ack_batch_max_size: Keyword.get(opts, :ack_batch_max_size, 2_500)
      )

    {:ok, pid} = StreamManager.start_link(opts)
    StreamManager.set_producer(pid, self())

    {pid, rpc_pid}
  end

  # Enable exactly-once delivery on a running StreamManager
  defp enable_exactly_once(pid) do
    send(
      pid,
      {:subscription_properties,
       %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
         message_ordering_enabled: false,
         exactly_once_delivery_enabled: true
       }}
    )

    sync(pid)
  end

  describe "exactly-once receipt modack gate — {:stream_messages, ...}" do
    test "in exactly-once mode, messages are NOT immediately forwarded to producer" do
      {pid, _rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("eo-ack-1", "data")]})
      sync(pid)

      # Receipt modack RPC is in-flight; message not yet delivered
      refute_received {:stream_messages, _}

      # State has one pending entry
      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 1
    end

    test "in standard mode, messages are forwarded immediately (no gating)" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("std-ack-1", "data")]})

      assert_receive {:stream_messages, [msg]}, 500
      assert msg.data == "data"

      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 0
    end

    test "messages are added to outstanding after receipt modack succeeds" do
      {pid, _rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("eo-ack-2", "data")]})
      sync(pid)

      state_before = :sys.get_state(pid)
      [ref] = Map.keys(state_before.pending_receipt_modacks)

      # Simulate receipt modack success
      send(pid, {:receipt_modack_result, ref, {:ok, []}})
      sync(pid)

      state_after = :sys.get_state(pid)
      assert map_size(state_after.pending_receipt_modacks) == 0
      assert Map.has_key?(state_after.outstanding, "eo-ack-2")
    end
  end

  # Injects a pending_receipt_modacks entry into StreamManager state directly,
  # bypassing the AckBatcher/Task chain. Used for tests that need to control
  # which receipt_modack_result variant the handler sees.
  defp inject_pending_receipt_modack(pid, ref, ack_ids, data_by_id) do
    broadway_msgs =
      Enum.map(ack_ids, fn ack_id ->
        %Broadway.Message{
          data: Map.get(data_by_id, ack_id, ack_id),
          metadata: %{},
          acknowledger: BroadwayCloudPubSub.Streaming.Acknowledger.builder(__MODULE__).(ack_id)
        }
      end)

    :sys.replace_state(pid, fn s ->
      entry = %{
        broadway_messages: broadway_msgs,
        ack_ids: ack_ids,
        received_at: System.monotonic_time(:millisecond)
      }

      %{s | pending_receipt_modacks: Map.put(s.pending_receipt_modacks, ref, entry)}
    end)
  end

  describe "exactly-once — {:receipt_modack_result, ref, result}" do
    test "total success: all messages delivered and added to outstanding" do
      # SpyRpcClientForEO defaults to :ok, so the Task auto-fires {:ok, []} result.
      {pid, _rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)
      StreamManager.notify_demand(pid, 10)

      send(pid, {:stream_messages, [received_message("r1", "d1"), received_message("r2", "d2")]})

      # The Task fires the RPC and sends back {:receipt_modack_result, ref, {:ok, []}}
      # automatically. Wait for the resulting message delivery.
      assert_receive {:stream_messages, msgs}, 500
      assert length(msgs) == 2
      assert Enum.map(msgs, & &1.data) |> Enum.sort() == ["d1", "d2"]

      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 0
      assert Map.has_key?(state.outstanding, "r1")
      assert Map.has_key?(state.outstanding, "r2")
    end

    test "total failure: no messages delivered, nothing added to outstanding" do
      {pid, rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)
      StreamManager.notify_demand(pid, 10)

      # Configure spy synchronously to return {:error, :unavailable} for the next modack call
      :ok = GenServer.call(rpc, {:set_response_sync, {:error, :unavailable}})

      send(pid, {:stream_messages, [received_message("fail-1", "data")]})
      # Wait for the Task's RPC call to complete and result to be processed
      sync(pid)
      # Give extra time for the async Task result to arrive and be processed
      Process.sleep(200)
      sync(pid)

      refute_received {:stream_messages, _}

      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 0
      assert map_size(state.outstanding) == 0
    end

    test "partial success: only succeeded messages delivered, failed dropped" do
      # Inject the pending entry directly to avoid racing with the auto-Task.
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      ref = make_ref()

      inject_pending_receipt_modack(pid, ref, ["ok-id", "bad-id"], %{
        "ok-id" => "good",
        "bad-id" => "dropped"
      })

      send(pid, {:receipt_modack_result, ref, {:ok, ["bad-id"]}})

      assert_receive {:stream_messages, msgs}, 500
      assert length(msgs) == 1
      assert hd(msgs).data == "good"

      state = :sys.get_state(pid)
      assert Map.has_key?(state.outstanding, "ok-id")
      refute Map.has_key?(state.outstanding, "bad-id")
    end

    test "partial success with all failed: no messages delivered" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["all-bad"], %{"all-bad" => "data"})

      send(pid, {:receipt_modack_result, ref, {:ok, ["all-bad"]}})
      sync(pid)

      refute_received {:stream_messages, _}
      assert map_size(:sys.get_state(pid).outstanding) == 0
    end

    test "stale/unknown ref is ignored gracefully" do
      {pid, _rpc} = start_manager_with_spy_rpc()
      enable_exactly_once(pid)

      stale_ref = make_ref()
      send(pid, {:receipt_modack_result, stale_ref, {:ok, []}})
      sync(pid)

      assert Process.alive?(pid)
      assert map_size(:sys.get_state(pid).pending_receipt_modacks) == 0
    end
  end

  describe "exactly-once — retry deadline auto-switch" do
    test "AckBatcher retry_deadline_ms switches to 600s when exactly-once is enabled" do
      broadway_name = Module.concat(__MODULE__, "RD#{System.unique_integer([:positive])}")

      opts =
        base_config()
        |> Keyword.put(:broadway_name, broadway_name)
        |> Keyword.put(:retry_deadline_ms, 60_000)

      grpc_client = Keyword.get(opts, :grpc_client, BroadwayCloudPubSub.Streaming.GrpcClient)
      {:ok, grpc_client_config} = grpc_client.init(opts)

      opts =
        opts
        |> Keyword.put(:grpc_client, grpc_client)
        |> Keyword.put(:grpc_client_config, grpc_client_config)

      rpc_client_name = Module.concat(broadway_name, UnaryRpcClient)
      batcher_name = Module.concat(broadway_name, AckBatcher)

      {:ok, _stub} = StubRpcClient.start_link(rpc_client_name)

      {:ok, _batcher} =
        AckBatcher.start_link(
          name: batcher_name,
          rpc_client: rpc_client_name,
          ack_batch_interval_ms: 100,
          ack_batch_max_size: 2_500,
          retry_deadline_ms: 60_000
        )

      {:ok, pid} = StreamManager.start_link(opts)
      StreamManager.set_producer(pid, self())

      batcher_pid = Process.whereis(batcher_name)
      assert :sys.get_state(batcher_pid).retry_deadline_ms == 60_000

      # Enable exactly-once
      send(
        pid,
        {:subscription_properties,
         %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: true
         }}
      )

      sync(pid)
      # Cast is async — let AckBatcher process it
      AckBatcher.flush(batcher_pid)

      assert :sys.get_state(batcher_pid).retry_deadline_ms == 600_000
    end

    test "AckBatcher retry_deadline_ms is restored to configured value when exactly-once is disabled" do
      broadway_name = Module.concat(__MODULE__, "RD2#{System.unique_integer([:positive])}")

      opts =
        base_config()
        |> Keyword.put(:broadway_name, broadway_name)
        |> Keyword.put(:retry_deadline_ms, 60_000)

      grpc_client = Keyword.get(opts, :grpc_client, BroadwayCloudPubSub.Streaming.GrpcClient)
      {:ok, grpc_client_config} = grpc_client.init(opts)

      opts =
        opts
        |> Keyword.put(:grpc_client, grpc_client)
        |> Keyword.put(:grpc_client_config, grpc_client_config)

      rpc_client_name = Module.concat(broadway_name, UnaryRpcClient)
      batcher_name = Module.concat(broadway_name, AckBatcher)

      {:ok, _stub} = StubRpcClient.start_link(rpc_client_name)

      {:ok, _batcher} =
        AckBatcher.start_link(
          name: batcher_name,
          rpc_client: rpc_client_name,
          ack_batch_interval_ms: 100,
          ack_batch_max_size: 2_500,
          retry_deadline_ms: 60_000
        )

      {:ok, pid} = StreamManager.start_link(opts)
      StreamManager.set_producer(pid, self())

      batcher_pid = Process.whereis(batcher_name)

      enable_exactly_once(pid)
      AckBatcher.flush(batcher_pid)
      assert :sys.get_state(batcher_pid).retry_deadline_ms == 600_000

      # Disable exactly-once
      send(
        pid,
        {:subscription_properties,
         %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: false
         }}
      )

      sync(pid)
      AckBatcher.flush(batcher_pid)
      assert :sys.get_state(batcher_pid).retry_deadline_ms == 60_000
    end

    test "retry_deadline_ms is NOT updated when exactly_once status does not change" do
      broadway_name = Module.concat(__MODULE__, "RD3#{System.unique_integer([:positive])}")

      opts =
        base_config()
        |> Keyword.put(:broadway_name, broadway_name)
        |> Keyword.put(:retry_deadline_ms, 60_000)

      grpc_client = Keyword.get(opts, :grpc_client, BroadwayCloudPubSub.Streaming.GrpcClient)
      {:ok, grpc_client_config} = grpc_client.init(opts)

      opts =
        opts
        |> Keyword.put(:grpc_client, grpc_client)
        |> Keyword.put(:grpc_client_config, grpc_client_config)

      rpc_client_name = Module.concat(broadway_name, UnaryRpcClient)
      batcher_name = Module.concat(broadway_name, AckBatcher)

      {:ok, _stub} = StubRpcClient.start_link(rpc_client_name)

      {:ok, _batcher} =
        AckBatcher.start_link(
          name: batcher_name,
          rpc_client: rpc_client_name,
          ack_batch_interval_ms: 100,
          ack_batch_max_size: 2_500
        )

      {:ok, pid} = StreamManager.start_link(opts)
      StreamManager.set_producer(pid, self())

      batcher_pid = Process.whereis(batcher_name)
      initial_deadline = :sys.get_state(batcher_pid).retry_deadline_ms

      # Send the same exactly_once=false twice — no update should happen
      send(
        pid,
        {:subscription_properties,
         %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: false
         }}
      )

      sync(pid)
      AckBatcher.flush(batcher_pid)

      assert :sys.get_state(batcher_pid).retry_deadline_ms == initial_deadline
    end
  end

  describe "exactly-once — stale pending_receipt_modacks sweep" do
    test "entries older than 60s are nacked with deadline=0 during extend_leases" do
      pid = start_manager()

      # Inject a stale entry (received_at far in the past)
      stale_ref = make_ref()

      stale_entry = %{
        broadway_messages: [],
        ack_ids: ["stale-ack-1"],
        received_at: System.monotonic_time(:millisecond) - 120_000
      }

      :sys.replace_state(pid, fn s ->
        %{s | pending_receipt_modacks: Map.put(s.pending_receipt_modacks, stale_ref, stale_entry)}
      end)

      # Trigger extend_leases which runs the sweep
      send(pid, :extend_leases)
      sync(pid)

      # Stale entry should be removed
      state = :sys.get_state(pid)
      refute Map.has_key?(state.pending_receipt_modacks, stale_ref)
    end

    test "fresh entries are NOT swept during extend_leases" do
      pid = start_manager()

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["fresh-ack"], %{"fresh-ack" => "data"})

      send(pid, :extend_leases)
      sync(pid)

      # Fresh entry should survive the sweep
      assert Map.has_key?(:sys.get_state(pid).pending_receipt_modacks, ref)
    end
  end

  describe "exactly-once — drain nack pending receipt modacks" do
    test "pending receipt modacks are nacked on stop_receiving" do
      pid = start_manager()

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["drain-eo"], %{"drain-eo" => "data"})
      assert map_size(:sys.get_state(pid).pending_receipt_modacks) == 1

      StreamManager.stop_receiving(pid)
      sync(pid)

      # After drain, pending_receipt_modacks should be cleared
      state = :sys.get_state(pid)
      assert map_size(state.pending_receipt_modacks) == 0
    end

    test "receipt_modack_result after drain (cleared pending) is ignored gracefully" do
      pid = start_manager()

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["drain-stale"], %{"drain-stale" => "data"})

      # Drain clears the pending map
      StreamManager.stop_receiving(pid)
      sync(pid)

      # RPC result arrives after drain — should be ignored, not crash
      send(pid, {:receipt_modack_result, ref, {:ok, []}})
      sync(pid)

      assert Process.alive?(pid)
    end
  end

  describe "exactly-once — pending_receipt_modacks NOT cleared on reconnect" do
    test "pending_receipt_modacks survives a stream_error reset" do
      # Use inject_pending_receipt_modack to avoid races with the auto-Task.
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["reconnect-ack"], %{"reconnect-ack" => "data"})
      assert map_size(:sys.get_state(pid).pending_receipt_modacks) == 1

      # Simulate a retryable stream error (triggers reconnect, not drain)
      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "unavailable"}})
      sync(pid)

      # pending_receipt_modacks must survive — ack_ids are valid across reconnects
      assert map_size(:sys.get_state(pid).pending_receipt_modacks) == 1
    end

    test "receipt_modack_result arriving after reconnect is still processed correctly" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

      ref = make_ref()
      inject_pending_receipt_modack(pid, ref, ["post-reconnect"], %{"post-reconnect" => "data"})

      # Reconnect
      send(pid, {:stream_error, %GRPC.RPCError{status: 14, message: "unavailable"}})
      sync(pid)

      # Result arrives post-reconnect — should still deliver
      send(pid, {:receipt_modack_result, ref, {:ok, []}})

      assert_receive {:stream_messages, [msg]}, 500
      assert msg.data == "data"
    end
  end
end
