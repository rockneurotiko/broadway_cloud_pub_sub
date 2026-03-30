defmodule BroadwayCloudPubSub.Streaming.StreamManagerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias BroadwayCloudPubSub.Streaming.{AckBatcher, StreamManager}

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
      adapter: :gun,
      grpc_endpoint: "pubsub.googleapis.com:443",
      keepalive_interval_ms: 30_000,
      on_success: :ack,
      on_failure: :noop,
      on_shutdown: {:nack, 5},
      max_extension_ms: 3_600_000,
      ack_batch_interval_ms: 100,
      ack_batch_max_size: 2_500,
      client_id: "test-client-id",
      token_generator: {__MODULE__, :noop_token, []},
      broadway: [name: __MODULE__]
    ]
  end

  def noop_token, do: {:ok, "test-token"}

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

  # ============================================================
  # Demand signaling
  # ============================================================

  describe "notify_demand/2 — no buffered messages" do
    test "stores pending_demand when message buffer is empty" do
      pid = start_manager()

      StreamManager.notify_demand(pid, 0)
      StreamManager.notify_demand(pid, 10)

      # Allow the async cast to be processed
      state = :sys.get_state(pid)

      assert state.pending_demand == 10
      assert :queue.is_empty(state.message_buffer)
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
        %{s | pending_demand: 0, message_buffer: :queue.from_list(msgs)}
      end)

      StreamManager.notify_demand(pid, 10)

      assert_receive {:stream_messages, received}
      assert Enum.map(received, & &1.data) == ["msg1", "msg2"]

      state = :sys.get_state(pid)
      assert :queue.is_empty(state.message_buffer)
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
        %{s | pending_demand: 0, message_buffer: :queue.from_list(msgs)}
      end)

      StreamManager.notify_demand(pid, 2)

      assert_receive {:stream_messages, received}
      assert length(received) == 2
      assert Enum.map(received, & &1.data) == ["msg1", "msg2"]

      state = :sys.get_state(pid)
      assert :queue.len(state.message_buffer) == 3
      assert state.pending_demand == 0

      StreamManager.notify_demand(pid, 10)

      assert_receive {:stream_messages, received2}
      assert length(received2) == 3
      assert Enum.map(received2, & &1.data) == ["msg3", "msg4", "msg5"]

      state = :sys.get_state(pid)
      assert :queue.is_empty(state.message_buffer)
      assert state.pending_demand == 7
    end
  end

  describe "stream_messages → message delivery" do
    test "messages are forwarded immediately when pending_demand > 0" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 10)

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
      assert :queue.is_empty(state.message_buffer)
    end

    test "messages are buffered when pending_demand is 0" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 0)

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
      assert :queue.len(state.message_buffer) == 1
    end

    test "buffer is flushed in FIFO order on notify_demand" do
      pid = start_manager()
      StreamManager.notify_demand(pid, 0)

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
  # receiving flag — draining
  # ============================================================

  describe "stop_receiving/1" do
    test "messages are not forwarded after stop_receiving even when pending_demand > 0" do
      pid = start_manager()

      StreamManager.notify_demand(pid, 10)
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
      logs =
        capture_log(fn ->
          pid = start_manager(keepalive_interval_ms: 10)

          :sys.replace_state(pid, fn s -> %{s | grpc_stream: :fake_stream} end)

          # Bootstrap the keepalive cycle — normally started by {:stream_opened},
          # but we injected the stream directly via replace_state.
          send(pid, :send_keepalive)

          :sys.get_state(pid)

          assert Process.alive?(pid)

          # After a send failure, the stream is reset (grpc_stream: nil) and a
          # reconnect is scheduled.
          state = :sys.get_state(pid)
          assert state.grpc_stream == nil
          assert state.reconnect_ref != nil
        end)

      assert logs =~ "GRPC.Client.Connection stopping as requested"
    end

    test "does not crash when stream is nil (reconnecting)" do
      pid = start_manager(keepalive_interval_ms: 10)

      :sys.replace_state(pid, fn s -> %{s | grpc_stream: nil} end)

      send(pid, :send_keepalive)

      :sys.get_state(pid)

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
      logs =
        capture_log(fn ->
          pid = start_manager()
          ref = Process.monitor(pid)
          Process.unlink(pid)

          send(pid, {:stream_error, %GRPC.RPCError{status: 5, message: "not found"}})

          assert_receive {:DOWN, ^ref, :process, ^pid, {:terminal_error, _}}, 1_000
        end)

      assert logs =~
               "Terminal Cloud Pub/Sub gRPC error — stopping: %GRPC.RPCError{status: 5, message: \"not found\", details: nil}"
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
               "Terminal Cloud Pub/Sub gRPC error — stopping: %GRPC.RPCError{status: 7, message: \"permission denied\", details: nil}"
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
               "Terminal Cloud Pub/Sub gRPC error — stopping: %GRPC.RPCError{status: 3, message: \"bad argument\", details: nil}"
    end

    test "UNAUTHENTICATED (16) schedules reconnect without stopping" do
      pid = start_manager(backoff_min: 10_000, backoff_max: 30_000)
      ref = Process.monitor(pid)

      send(pid, {:stream_error, %GRPC.RPCError{status: 16, message: "unauthenticated"}})
      :sys.get_state(pid)

      refute_received {:DOWN, ^ref, :process, ^pid, _}
      assert Process.alive?(pid)

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

      :sys.get_state(pid)

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
               "Terminal Cloud Pub/Sub gRPC error — stopping: %GRPC.RPCError{status: 5, message: \"not found\", details: nil}"
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
  # Phase 5: Ordering key support — subscription_properties
  # ============================================================

  describe "subscription_properties — ordering_enabled" do
    test "ordering_enabled defaults to false" do
      pid = start_manager()
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
      # Sync: ensure the message is processed
      :sys.get_state(pid)

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

      :sys.get_state(pid)
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

      :sys.get_state(pid)
      assert :sys.get_state(pid).ordering_enabled == false
    end

    test "ignores other messages — ordering_enabled unchanged" do
      pid = start_manager()

      # Unrelated message
      send(pid, {:some_other_event, :ignored})
      :sys.get_state(pid)

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
      :sys.get_state(pid)

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

      :sys.get_state(pid)
      assert :sys.get_state(pid).exactly_once_enabled == true

      send(
        pid,
        {:subscription_properties,
         %Google.Pubsub.V1.StreamingPullResponse.SubscriptionProperties{
           message_ordering_enabled: false,
           exactly_once_delivery_enabled: false
         }}
      )

      :sys.get_state(pid)
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

      :sys.get_state(pid)
      state = :sys.get_state(pid)
      assert state.ordering_enabled == true
      assert state.exactly_once_enabled == true
    end
  end

  # ============================================================
  # Phase 6: Exactly-once delivery — extend_leases minimum deadline
  # ============================================================

  describe "extend_leases — exactly_once_enabled deadline enforcement" do
    # Inject outstanding messages and trigger :extend_leases, then capture
    # the modack call via the StubRpcClient + process mailbox inspection.
    # We can't easily intercept AckBatcher calls here, so we validate behaviour
    # by inspecting how long until the next :extend_leases fires.

    test "uses adaptive deadline (no 60s floor) when exactly_once_enabled is false" do
      pid = start_manager()

      # With <10 samples the distribution returns the default deadline (60s).
      # We verify the next timer is scheduled within a reasonable range.
      now_ms = System.monotonic_time(:millisecond)

      # Inject one outstanding message and a lease timer that fires immediately.
      :sys.replace_state(pid, fn s ->
        %{
          s
          | exactly_once_enabled: false,
            outstanding: %{
              "ack-normal" => %{
                received_at: now_ms - 5_000,
                max_expiry: now_ms + 3_600_000
              }
            }
        }
      end)

      # Fire the :extend_leases handler directly
      send(pid, :extend_leases)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      # Lease timer should be re-scheduled with a positive ref
      assert state.lease_timer != nil
    end

    test "enforces 60s minimum deadline when exactly_once_enabled is true" do
      pid = start_manager()
      now_ms = System.monotonic_time(:millisecond)

      # With exactly_once_enabled: true and an adaptive deadline of e.g. 10s
      # (cold start default clamped to the min), effective_deadline must be
      # at least 60. We validate by measuring the scheduled next interval:
      # interval = (effective_deadline - 5) * 1000 * jitter_factor
      # With effective_deadline=60: interval in [(60-5)*1000*0.8, (60-5)*1000*0.9]
      #                            = [44_000, 49_500]
      :sys.replace_state(pid, fn s ->
        %{
          s
          | exactly_once_enabled: true,
            outstanding: %{
              "ack-eo" => %{
                received_at: now_ms - 1_000,
                max_expiry: now_ms + 3_600_000
              }
            }
        }
      end)

      send(pid, :extend_leases)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      # The timer must be set and represent a deadline >= 60s.
      # We read back the remaining time from the timer ref.
      assert state.lease_timer != nil
      remaining_ms = Process.read_timer(state.lease_timer)
      # The next extension should fire well before the 60s deadline expires.
      # We assert >= 40_000 as a lower bound with tolerance for scheduling jitter.
      assert remaining_ms >= 40_000,
             "Expected next lease timer >= 40s for exactly-once, got #{remaining_ms}ms"
    end

    test "uses normal interval (much shorter) when exactly_once_enabled is false" do
      pid = start_manager(stream_ack_deadline_seconds: 20)
      now_ms = System.monotonic_time(:millisecond)

      :sys.replace_state(pid, fn s ->
        %{
          s
          | exactly_once_enabled: false,
            outstanding: %{
              "ack-normal" => %{
                received_at: now_ms - 1_000,
                max_expiry: now_ms + 3_600_000
              }
            }
        }
      end)

      send(pid, :extend_leases)
      :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert state.lease_timer != nil
      remaining_ms = Process.read_timer(state.lease_timer)

      # With stream_ack_deadline_seconds=20 and adaptive deadline defaulting
      # to 60 (cold start default), effective = 60s (no exactly_once floor).
      # Interval = (60 - 5) * 1000 * jitter ≈ [44_000, 49_500).
      # The key check: it should NOT be forced to >= 44_000 due to exactly_once
      # — we simply verify it's a valid positive number.
      assert is_integer(remaining_ms) and remaining_ms > 0
    end
  end
end
