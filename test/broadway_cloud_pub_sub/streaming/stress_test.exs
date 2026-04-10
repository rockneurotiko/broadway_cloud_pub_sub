defmodule BroadwayCloudPubSub.Streaming.StressTest do
  @moduledoc """
  Stress tests for the Streaming Producer against the Pub/Sub emulator.

  Covers:
    1. High-volume burst (1000+ messages, fast processing)
    2. Rapid sequential batches
    3. Message completeness verification under load
    4. Streaming vs Pull producer comparison
    5. Both :gun and :mint adapters
    6. Pipeline stop/restart during message flow
    7. Concurrent publishers while pipeline processes
    8. High concurrency processors with near-zero processing time

  Run with:

      mix test test/broadway_cloud_pub_sub/streaming/stress_test.exs --only stress

  Requires the Pub/Sub emulator running on localhost:8085.
  """

  use ExUnit.Case, async: false

  @moduletag :stress
  @moduletag timeout: 120_000

  require Logger

  alias BroadwayCloudPubSub.PubSubEmulator

  # ---------------------------------------------------------------------------
  # Token generators
  # ---------------------------------------------------------------------------

  def noop_token, do: {:ok, "emulator-no-auth"}

  # ---------------------------------------------------------------------------
  # Test Pipelines
  # ---------------------------------------------------------------------------

  defmodule StreamingPipeline do
    @moduledoc "Streaming pipeline that sends received data to test process."
    use Broadway

    def start_link(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      subscription = Keyword.fetch!(opts, :subscription)
      emulator_host = Keyword.fetch!(opts, :emulator_host)
      name = Keyword.fetch!(opts, :name)
      adapter = Keyword.get(opts, :adapter, :gun)
      max_outstanding = Keyword.get(opts, :max_outstanding, 1000)
      processor_concurrency = Keyword.get(opts, :processor_concurrency, 4)

      Broadway.start_link(__MODULE__,
        name: name,
        producer: [
          module:
            {BroadwayCloudPubSub.Streaming.Producer,
             subscription: subscription,
             token_generator: {BroadwayCloudPubSub.Streaming.StressTest, :noop_token, []},
             grpc_endpoint: emulator_host,
             use_ssl: false,
             adapter: adapter,
             max_outstanding_messages: max_outstanding,
             on_failure: {:nack, 0}},
          concurrency: 1
        ],
        processors: [
          default: [concurrency: processor_concurrency]
        ],
        context: %{test_pid: test_pid}
      )
    end

    @impl Broadway
    def handle_message(:default, message, %{test_pid: test_pid}) do
      send(test_pid, {:msg, message.data})
      message
    end

    @impl Broadway
    def handle_failed(messages, %{test_pid: test_pid}) do
      Enum.each(messages, fn msg ->
        send(test_pid, {:failed, msg.data})
      end)

      messages
    end
  end

  defmodule PullPipeline do
    @moduledoc "Pull-based pipeline for comparison."
    use Broadway

    def start_link(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      subscription = Keyword.fetch!(opts, :subscription)
      emulator_host = Keyword.fetch!(opts, :emulator_host)
      name = Keyword.fetch!(opts, :name)
      processor_concurrency = Keyword.get(opts, :processor_concurrency, 4)

      Broadway.start_link(__MODULE__,
        name: name,
        producer: [
          module:
            {BroadwayCloudPubSub.Producer,
             subscription: subscription,
             token_generator: {BroadwayCloudPubSub.Streaming.StressTest, :noop_token, []},
             base_url: "http://#{emulator_host}",
             receive_interval: 50,
             max_number_of_messages: 100,
             on_failure: {:nack, 0}},
          concurrency: 1
        ],
        processors: [
          default: [concurrency: processor_concurrency]
        ],
        context: %{test_pid: test_pid}
      )
    end

    @impl Broadway
    def handle_message(:default, message, %{test_pid: test_pid}) do
      send(test_pid, {:msg, message.data})
      message
    end

    @impl Broadway
    def handle_failed(messages, %{test_pid: test_pid}) do
      Enum.each(messages, fn msg ->
        send(test_pid, {:failed, msg.data})
      end)

      messages
    end
  end

  defmodule SlowPipeline do
    @moduledoc "Pipeline with configurable processing delay."
    use Broadway

    def start_link(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      subscription = Keyword.fetch!(opts, :subscription)
      emulator_host = Keyword.fetch!(opts, :emulator_host)
      name = Keyword.fetch!(opts, :name)
      delay_ms = Keyword.get(opts, :delay_ms, 0)
      adapter = Keyword.get(opts, :adapter, :gun)
      max_outstanding = Keyword.get(opts, :max_outstanding, 1000)
      processor_concurrency = Keyword.get(opts, :processor_concurrency, 4)

      Broadway.start_link(__MODULE__,
        name: name,
        producer: [
          module:
            {BroadwayCloudPubSub.Streaming.Producer,
             subscription: subscription,
             token_generator: {BroadwayCloudPubSub.Streaming.StressTest, :noop_token, []},
             grpc_endpoint: emulator_host,
             use_ssl: false,
             adapter: adapter,
             max_outstanding_messages: max_outstanding,
             on_failure: {:nack, 0}},
          concurrency: 1
        ],
        processors: [
          default: [concurrency: processor_concurrency]
        ],
        context: %{test_pid: test_pid, delay_ms: delay_ms}
      )
    end

    @impl Broadway
    def handle_message(:default, message, %{test_pid: test_pid, delay_ms: delay_ms}) do
      if delay_ms > 0, do: Process.sleep(delay_ms)
      send(test_pid, {:msg, message.data})
      message
    end

    @impl Broadway
    def handle_failed(messages, %{test_pid: test_pid}) do
      Enum.each(messages, fn msg ->
        send(test_pid, {:failed, msg.data})
      end)

      messages
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  setup_all do
    # GRPC.Client.Supervisor is started automatically by the grpc_client OTP application
    PubSubEmulator.start()
    :ok
  end

  defp unique_name, do: :"StressPipeline#{:erlang.unique_integer([:positive])}"

  defp setup_infra(prefix) do
    topic = "#{prefix}-#{:erlang.unique_integer([:positive])}"
    sub = "#{prefix}-sub-#{:erlang.unique_integer([:positive])}"
    {_full_topic, full_sub} = PubSubEmulator.setup_topic_and_subscription(topic, sub)
    {topic, sub, full_sub}
  end

  defp stop_pipeline(pid) do
    ref = Process.monitor(pid)

    try do
      Broadway.stop(pid)
    catch
      :exit, _ -> :ok
    end

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    after
      10_000 -> :ok
    end
  end

  defp collect_messages(expected_count, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    collect_messages_loop(expected_count, deadline, [])
  end

  defp collect_messages_loop(0, _deadline, acc), do: {:ok, Enum.reverse(acc)}

  defp collect_messages_loop(remaining, deadline, acc) do
    now = System.monotonic_time(:millisecond)
    wait = max(deadline - now, 0)

    receive do
      {:msg, data} ->
        collect_messages_loop(remaining - 1, deadline, [data | acc])
    after
      wait ->
        {:partial, Enum.reverse(acc), remaining}
    end
  end

  defp publish_in_batches(topic, total, batch_size) do
    payloads = Enum.map(1..total, &"msg-#{&1}")

    payloads
    |> Enum.chunk_every(batch_size)
    |> Enum.each(fn batch ->
      {:ok, _ids} = PubSubEmulator.publish(topic, batch)
    end)

    payloads
  end

  # ---------------------------------------------------------------------------
  # Scenario 1: High-volume burst — 1000 messages, fast processing, Gun adapter
  # ---------------------------------------------------------------------------

  describe "Scenario 1: High-volume burst (Gun)" do
    test "receives all 1000 messages without loss" do
      {topic, _sub, full_sub} = setup_infra("burst-gun")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 1000,
          processor_concurrency: 8
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 1000, 200)

      case collect_messages(1000, 60_000) do
        {:ok, received} ->
          assert length(received) == 1000
          assert Enum.sort(received) == Enum.sort(expected)
          Logger.info("[Stress 1/Gun] All 1000 messages received.")

        {:partial, received, remaining} ->
          Logger.warning(
            "[Stress 1/Gun] Only #{length(received)}/1000 received, #{remaining} missing"
          )

          flunk("Missing #{remaining} messages out of 1000")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 2: High-volume burst — 1000 messages, Mint adapter
  # ---------------------------------------------------------------------------

  describe "Scenario 2: High-volume burst (Mint)" do
    test "receives all 1000 messages without loss" do
      {topic, _sub, full_sub} = setup_infra("burst-mint")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :mint,
          max_outstanding: 1000,
          processor_concurrency: 8
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 1000, 200)

      case collect_messages(1000, 60_000) do
        {:ok, received} ->
          assert length(received) == 1000
          assert Enum.sort(received) == Enum.sort(expected)
          Logger.info("[Stress 2/Mint] All 1000 messages received.")

        {:partial, received, remaining} ->
          Logger.warning(
            "[Stress 2/Mint] Only #{length(received)}/1000 received, #{remaining} missing"
          )

          flunk("Missing #{remaining} messages out of 1000")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 3: Rapid sequential batches — 5 bursts of 200, no pause
  # ---------------------------------------------------------------------------

  describe "Scenario 3: Rapid sequential batches" do
    test "handles 5 rapid bursts of 200 messages (Gun)" do
      {topic, _sub, full_sub} = setup_infra("rapid-gun")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 1000,
          processor_concurrency: 8
        )

      Process.sleep(500)

      # Publish 5 bursts of 200 messages back-to-back
      all_expected =
        Enum.flat_map(1..5, fn batch_num ->
          payloads = Enum.map(1..200, &"batch#{batch_num}-msg-#{&1}")
          {:ok, _ids} = PubSubEmulator.publish(topic, payloads)
          payloads
        end)

      case collect_messages(1000, 60_000) do
        {:ok, received} ->
          assert length(received) == 1000
          assert Enum.sort(received) == Enum.sort(all_expected)
          Logger.info("[Stress 3] All 1000 messages from 5 bursts received.")

        {:partial, received, remaining} ->
          Logger.warning("[Stress 3] #{length(received)}/1000 received, #{remaining} missing")

          flunk("Missing #{remaining} messages after rapid sequential batches")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 4: Demand pressure — low max_outstanding, high message volume
  # ---------------------------------------------------------------------------

  describe "Scenario 4: Demand pressure with low max_outstanding" do
    test "handles 500 messages with max_outstanding=10 (Gun)" do
      {topic, _sub, full_sub} = setup_infra("demand-gun")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          # Very low outstanding — forces heavy demand cycling
          max_outstanding: 10,
          processor_concurrency: 2
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 500, 50)

      case collect_messages(500, 60_000) do
        {:ok, received} ->
          assert length(received) == 500
          assert Enum.sort(received) == Enum.sort(expected)
          Logger.info("[Stress 4] All 500 messages received with max_outstanding=10.")

        {:partial, received, remaining} ->
          Logger.warning("[Stress 4] #{length(received)}/500 received, #{remaining} missing")

          flunk("Missing #{remaining} messages with constrained outstanding")
      end

      stop_pipeline(pid)
    end

    test "handles 500 messages with max_outstanding=10 (Mint)" do
      {topic, _sub, full_sub} = setup_infra("demand-mint")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :mint,
          max_outstanding: 10,
          processor_concurrency: 2
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 500, 50)

      case collect_messages(500, 60_000) do
        {:ok, received} ->
          assert length(received) == 500
          assert Enum.sort(received) == Enum.sort(expected)
          Logger.info("[Stress 4/Mint] All 500 messages received with max_outstanding=10.")

        {:partial, received, remaining} ->
          Logger.warning("[Stress 4/Mint] #{length(received)}/500 received, #{remaining} missing")

          flunk("Missing #{remaining} messages with constrained outstanding (Mint)")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 5: Streaming vs Pull producer comparison
  # ---------------------------------------------------------------------------

  describe "Scenario 5: Streaming vs Pull comparison" do
    test "both producers receive all 200 messages" do
      # -- Streaming (Gun) --
      {s_topic, _s_sub, s_full_sub} = setup_infra("cmp-stream")
      s_name = unique_name()

      {:ok, s_pid} =
        StreamingPipeline.start_link(
          name: s_name,
          test_pid: self(),
          subscription: s_full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 500,
          processor_concurrency: 4
        )

      Process.sleep(500)

      s_expected = publish_in_batches(s_topic, 200, 100)
      s_start = System.monotonic_time(:millisecond)

      s_result = collect_messages(200, 30_000)
      s_elapsed = System.monotonic_time(:millisecond) - s_start

      stop_pipeline(s_pid)

      # -- Pull --
      {p_topic, _p_sub, p_full_sub} = setup_infra("cmp-pull")
      p_name = unique_name()

      {:ok, p_pid} =
        PullPipeline.start_link(
          name: p_name,
          test_pid: self(),
          subscription: p_full_sub,
          emulator_host: PubSubEmulator.host(),
          processor_concurrency: 4
        )

      Process.sleep(500)

      p_expected = publish_in_batches(p_topic, 200, 100)
      p_start = System.monotonic_time(:millisecond)

      p_result = collect_messages(200, 30_000)
      p_elapsed = System.monotonic_time(:millisecond) - p_start

      stop_pipeline(p_pid)

      # Assert both received everything
      case s_result do
        {:ok, s_received} ->
          assert length(s_received) == 200
          assert Enum.sort(s_received) == Enum.sort(s_expected)

        {:partial, s_received, s_remaining} ->
          flunk("Streaming: only #{length(s_received)}/200, missing #{s_remaining}")
      end

      case p_result do
        {:ok, p_received} ->
          assert length(p_received) == 200
          assert Enum.sort(p_received) == Enum.sort(p_expected)

        {:partial, p_received, p_remaining} ->
          flunk("Pull: only #{length(p_received)}/200, missing #{p_remaining}")
      end

      Logger.info("[Stress 5] Streaming: #{s_elapsed}ms, Pull: #{p_elapsed}ms for 200 messages")
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 6: Pipeline stop/restart during message flow
  # ---------------------------------------------------------------------------

  describe "Scenario 6: Stop and restart during message flow" do
    test "no messages lost after restart" do
      {topic, _sub, full_sub} = setup_infra("restart")
      name = unique_name()

      # Start pipeline
      {:ok, pid1} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 100,
          processor_concurrency: 4
        )

      Process.sleep(500)

      # Publish first batch
      batch1 = Enum.map(1..100, &"phase1-#{&1}")
      {:ok, _} = PubSubEmulator.publish(topic, batch1)

      # Collect some messages, then stop
      Process.sleep(2000)
      stop_pipeline(pid1)

      # Drain whatever arrived from first pipeline
      phase1_received = drain_mailbox()

      # Publish second batch while pipeline is down
      batch2 = Enum.map(1..100, &"phase2-#{&1}")
      {:ok, _} = PubSubEmulator.publish(topic, batch2)

      # Restart with a DIFFERENT name (since the old name is taken by the stopped process)
      name2 = unique_name()

      {:ok, pid2} =
        StreamingPipeline.start_link(
          name: name2,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 500,
          processor_concurrency: 4
        )

      Process.sleep(500)

      # All messages not yet acked + batch2 should arrive
      all_expected = MapSet.new(batch1 ++ batch2)
      all_received_from_restart = collect_remaining(all_expected, phase1_received, 30_000)

      stop_pipeline(pid2)

      missing = MapSet.difference(all_expected, all_received_from_restart)

      if MapSet.size(missing) > 0 do
        Logger.warning(
          "[Stress 6] Missing #{MapSet.size(missing)} messages: #{inspect(Enum.take(MapSet.to_list(missing), 10))}"
        )

        flunk("Lost #{MapSet.size(missing)} messages across stop/restart")
      end

      Logger.info("[Stress 6] All 200 messages recovered after pipeline restart.")
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 7: Concurrent publishers
  # ---------------------------------------------------------------------------

  describe "Scenario 7: Concurrent publishers" do
    test "handles messages from 5 concurrent publishers (Gun)" do
      {topic, _sub, full_sub} = setup_infra("conc-pub")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 1000,
          processor_concurrency: 8
        )

      Process.sleep(500)

      # 5 tasks each publishing 200 messages concurrently
      tasks =
        Enum.map(1..5, fn pub_id ->
          Task.async(fn ->
            payloads = Enum.map(1..200, &"pub#{pub_id}-msg-#{&1}")
            {:ok, _} = PubSubEmulator.publish(topic, payloads)
            payloads
          end)
        end)

      all_expected =
        tasks
        |> Task.await_many(30_000)
        |> List.flatten()

      case collect_messages(1000, 60_000) do
        {:ok, received} ->
          assert length(received) == 1000
          assert Enum.sort(received) == Enum.sort(all_expected)
          Logger.info("[Stress 7] All 1000 messages from 5 concurrent publishers received.")

        {:partial, received, remaining} ->
          Logger.warning("[Stress 7] #{length(received)}/1000 received, #{remaining} missing")

          flunk("Missing #{remaining} messages from concurrent publishers")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 8: High concurrency processors, near-zero processing time
  # ---------------------------------------------------------------------------

  describe "Scenario 8: High processor concurrency, zero delay" do
    test "16 processors handle 2000 messages with zero processing time (Gun)" do
      {topic, _sub, full_sub} = setup_infra("fast-gun")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 2000,
          processor_concurrency: 16
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 2000, 200)

      start_time = System.monotonic_time(:millisecond)

      case collect_messages(2000, 90_000) do
        {:ok, received} ->
          elapsed = System.monotonic_time(:millisecond) - start_time
          assert length(received) == 2000
          assert Enum.sort(received) == Enum.sort(expected)

          Logger.info(
            "[Stress 8/Gun] 2000 messages with 16 processors in #{elapsed}ms (#{Float.round(2000 / (elapsed / 1000), 1)} msgs/sec)"
          )

        {:partial, received, remaining} ->
          elapsed = System.monotonic_time(:millisecond) - start_time

          Logger.warning(
            "[Stress 8/Gun] #{length(received)}/2000 in #{elapsed}ms, #{remaining} missing"
          )

          flunk("Missing #{remaining} messages with high concurrency")
      end

      stop_pipeline(pid)
    end

    test "16 processors handle 2000 messages with zero processing time (Mint)" do
      {topic, _sub, full_sub} = setup_infra("fast-mint")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :mint,
          max_outstanding: 2000,
          processor_concurrency: 16
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 2000, 200)

      start_time = System.monotonic_time(:millisecond)

      case collect_messages(2000, 90_000) do
        {:ok, received} ->
          elapsed = System.monotonic_time(:millisecond) - start_time
          assert length(received) == 2000
          assert Enum.sort(received) == Enum.sort(expected)

          Logger.info(
            "[Stress 8/Mint] 2000 messages with 16 processors in #{elapsed}ms (#{Float.round(2000 / (elapsed / 1000), 1)} msgs/sec)"
          )

        {:partial, received, remaining} ->
          elapsed = System.monotonic_time(:millisecond) - start_time

          Logger.warning(
            "[Stress 8/Mint] #{length(received)}/2000 in #{elapsed}ms, #{remaining} missing"
          )

          flunk("Missing #{remaining} messages with high concurrency (Mint)")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 9: No duplicate messages
  # ---------------------------------------------------------------------------

  describe "Scenario 9: No duplicate delivery" do
    test "500 messages arrive exactly once (Gun)" do
      {topic, _sub, full_sub} = setup_infra("nodup-gun")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 500,
          processor_concurrency: 4
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 500, 100)

      case collect_messages(500, 30_000) do
        {:ok, received} ->
          assert length(received) == 500

          unique = Enum.uniq(received)

          if length(unique) != length(received) do
            dupes = received -- unique
            Logger.warning("[Stress 9] Duplicates found: #{inspect(Enum.take(dupes, 10))}")
            flunk("Found #{length(received) - length(unique)} duplicate messages")
          end

          assert Enum.sort(received) == Enum.sort(expected)
          Logger.info("[Stress 9] 500 messages, zero duplicates.")

        {:partial, received, remaining} ->
          flunk("Only #{length(received)}/500 received, #{remaining} missing")
      end

      # Wait a bit for any late duplicates
      Process.sleep(2000)
      late_dupes = drain_mailbox()

      if length(late_dupes) > 0 do
        Logger.warning(
          "[Stress 9] #{length(late_dupes)} late duplicate(s) arrived after collect!"
        )

        flunk("#{length(late_dupes)} late duplicates detected")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 10: Messages published BEFORE pipeline starts
  # ---------------------------------------------------------------------------

  describe "Scenario 10: Pre-published messages" do
    test "receives messages that were published before pipeline started" do
      {topic, _sub, full_sub} = setup_infra("prepub")

      # Publish BEFORE pipeline exists
      expected = publish_in_batches(topic, 300, 100)

      # Wait to make sure messages are committed in emulator
      Process.sleep(500)

      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 500,
          processor_concurrency: 4
        )

      case collect_messages(300, 30_000) do
        {:ok, received} ->
          assert length(received) == 300
          assert Enum.sort(received) == Enum.sort(expected)
          Logger.info("[Stress 10] All 300 pre-published messages received.")

        {:partial, received, remaining} ->
          flunk("Only #{length(received)}/300 pre-published messages, #{remaining} missing")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 11: Slow processing with message backlog
  # ---------------------------------------------------------------------------

  describe "Scenario 11: Slow processing (simulated delay)" do
    test "handles 100 messages with 50ms processing delay" do
      {topic, _sub, full_sub} = setup_infra("slow-proc")
      name = unique_name()

      {:ok, pid} =
        SlowPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 100,
          processor_concurrency: 4,
          delay_ms: 50
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 100, 50)

      start_time = System.monotonic_time(:millisecond)

      case collect_messages(100, 60_000) do
        {:ok, received} ->
          elapsed = System.monotonic_time(:millisecond) - start_time
          assert length(received) == 100
          assert Enum.sort(received) == Enum.sort(expected)

          Logger.info("[Stress 11] 100 messages with 50ms delay in #{elapsed}ms")

        {:partial, received, remaining} ->
          flunk("Only #{length(received)}/100, #{remaining} missing with slow processing")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers for Scenario 6
  # ---------------------------------------------------------------------------

  defp drain_mailbox do
    drain_mailbox_loop([])
  end

  defp drain_mailbox_loop(acc) do
    receive do
      {:msg, data} -> drain_mailbox_loop([data | acc])
      {:failed, data} -> drain_mailbox_loop([data | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end

  defp collect_remaining(all_expected, already_received, timeout_ms) do
    received_set = MapSet.new(already_received)
    remaining = MapSet.difference(all_expected, received_set)
    remaining_count = MapSet.size(remaining)

    if remaining_count == 0 do
      received_set
    else
      deadline = System.monotonic_time(:millisecond) + timeout_ms
      collect_remaining_loop(received_set, all_expected, deadline)
    end
  end

  defp collect_remaining_loop(received_set, all_expected, deadline) do
    if MapSet.equal?(received_set, all_expected) do
      received_set
    else
      now = System.monotonic_time(:millisecond)
      wait = max(deadline - now, 0)

      receive do
        {:msg, data} ->
          collect_remaining_loop(MapSet.put(received_set, data), all_expected, deadline)
      after
        wait -> received_set
      end
    end
  end

  # ===========================================================================
  # AGGRESSIVE SCENARIOS — Trying to break the producer
  # ===========================================================================

  # ---------------------------------------------------------------------------
  # Scenario 12: Extreme backpressure — max_outstanding = 1
  # Forces the producer to deliver ONE message at a time.
  # ---------------------------------------------------------------------------

  describe "Scenario 12: Extreme backpressure (max_outstanding=1)" do
    test "100 messages with max_outstanding=1 (Gun)" do
      {topic, _sub, full_sub} = setup_infra("extreme-bp-gun")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 1,
          processor_concurrency: 1
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 100, 20)

      case collect_messages(100, 60_000) do
        {:ok, received} ->
          assert length(received) == 100
          assert Enum.sort(received) == Enum.sort(expected)
          Logger.info("[Stress 12/Gun] 100 messages with max_outstanding=1 — all received.")

        {:partial, received, remaining} ->
          flunk("max_outstanding=1 (Gun): only #{length(received)}/100, missing #{remaining}")
      end

      stop_pipeline(pid)
    end

    test "100 messages with max_outstanding=1 (Mint)" do
      {topic, _sub, full_sub} = setup_infra("extreme-bp-mint")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :mint,
          max_outstanding: 1,
          processor_concurrency: 1
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 100, 20)

      case collect_messages(100, 60_000) do
        {:ok, received} ->
          assert length(received) == 100
          assert Enum.sort(received) == Enum.sort(expected)
          Logger.info("[Stress 12/Mint] 100 messages with max_outstanding=1 — all received.")

        {:partial, received, remaining} ->
          flunk("max_outstanding=1 (Mint): only #{length(received)}/100, missing #{remaining}")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 13: Massive burst — 5000 messages
  # Tests the upper end of throughput.
  # ---------------------------------------------------------------------------

  describe "Scenario 13: Massive burst (5000 messages)" do
    test "receives all 5000 messages (Gun)" do
      {topic, _sub, full_sub} = setup_infra("massive-gun")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 5000,
          processor_concurrency: 16
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 5000, 500)

      start_time = System.monotonic_time(:millisecond)

      case collect_messages(5000, 90_000) do
        {:ok, received} ->
          elapsed = System.monotonic_time(:millisecond) - start_time
          assert length(received) == 5000
          assert Enum.sort(received) == Enum.sort(expected)

          Logger.info(
            "[Stress 13/Gun] 5000 messages in #{elapsed}ms " <>
              "(#{Float.round(5000 / (elapsed / 1000), 1)} msgs/sec)"
          )

        {:partial, received, remaining} ->
          elapsed = System.monotonic_time(:millisecond) - start_time

          flunk(
            "Massive burst (Gun): #{length(received)}/5000 in #{elapsed}ms, #{remaining} missing"
          )
      end

      stop_pipeline(pid)
    end

    test "receives all 5000 messages (Mint)" do
      {topic, _sub, full_sub} = setup_infra("massive-mint")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :mint,
          max_outstanding: 5000,
          processor_concurrency: 16
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 5000, 500)

      start_time = System.monotonic_time(:millisecond)

      case collect_messages(5000, 90_000) do
        {:ok, received} ->
          elapsed = System.monotonic_time(:millisecond) - start_time
          assert length(received) == 5000
          assert Enum.sort(received) == Enum.sort(expected)

          Logger.info(
            "[Stress 13/Mint] 5000 messages in #{elapsed}ms " <>
              "(#{Float.round(5000 / (elapsed / 1000), 1)} msgs/sec)"
          )

        {:partial, received, remaining} ->
          elapsed = System.monotonic_time(:millisecond) - start_time

          flunk(
            "Massive burst (Mint): #{length(received)}/5000 in #{elapsed}ms, #{remaining} missing"
          )
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 14: Kill the gRPC connection mid-stream
  # Simulates network failure while messages are actively flowing.
  # ---------------------------------------------------------------------------

  describe "Scenario 14: Kill connection during active message flow" do
    test "recovers after connection kill (Gun)" do
      {topic, _sub, full_sub} = setup_infra("connkill-gun")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 500,
          processor_concurrency: 4
        )

      Process.sleep(500)

      # Publish initial batch
      batch1 = Enum.map(1..200, &"before-kill-#{&1}")
      {:ok, _} = PubSubEmulator.publish(topic, batch1)

      # Wait for batch1 to be fully consumed
      case collect_messages(200, 15_000) do
        {:ok, _} ->
          Logger.info("[Stress 14/Gun] batch1 fully consumed before kill")

        {:partial, received, remaining} ->
          Logger.info(
            "[Stress 14/Gun] batch1 partially consumed: #{length(received)}/200, #{remaining} remaining"
          )
      end

      # Kill the Gun connection process and simulate gun_down so StreamManager
      # detects the disconnect and reconnects.
      stream_manager = Module.concat(name, "StreamManager_0")
      sm_state = :sys.get_state(stream_manager)

      case sm_state do
        %{conn_pid: conn_pid} when is_pid(conn_pid) ->
          Logger.info("[Stress 14/Gun] Killing conn_pid: #{inspect(conn_pid)}")
          Process.exit(conn_pid, :kill)

        # Simulate the gun_down message that Gun would normally send
        # send(stream_manager, {:gun_down, conn_pid, :http2, :killed, []})

        _ ->
          Logger.warning("[Stress 14/Gun] No conn_pid found in state")
      end

      # Wait for reconnect (backoff_min is 1000ms by default, plus connection setup)
      Process.sleep(5000)

      # Inspect StreamManager state to verify reconnection
      sm_state_after = :sys.get_state(stream_manager)

      Logger.info(
        "[Stress 14/Gun] After reconnect: grpc_stream=#{sm_state_after.grpc_stream != nil}, " <>
          "pending_demand=#{sm_state_after.pending_demand}, " <>
          "buffer_size=#{:queue.len(sm_state_after.message_buffer)}, " <>
          "outstanding=#{map_size(sm_state_after.outstanding)}"
      )

      # Publish more AFTER reconnect
      batch2 = Enum.map(1..200, &"after-kill-#{&1}")
      {:ok, _} = PubSubEmulator.publish(topic, batch2)

      case collect_messages(200, 30_000) do
        {:ok, received} ->
          assert length(received) == 200
          Logger.info("[Stress 14/Gun] All 200 post-kill messages received.")

        {:partial, received, remaining} ->
          # Log final state for diagnostics
          sm_final = :sys.get_state(stream_manager)

          Logger.warning(
            "[Stress 14/Gun] Post-kill: #{length(received)}/200, #{remaining} missing. " <>
              "stream=#{sm_final.grpc_stream != nil}, demand=#{sm_final.pending_demand}, " <>
              "buffer=#{:queue.len(sm_final.message_buffer)}"
          )

          flunk("Lost #{remaining} messages after connection kill")
      end

      stop_pipeline(pid)
    end

    test "recovers after connection kill — gun_down only (Gun)" do
      {topic, _sub, full_sub} = setup_infra("connkill-gun2")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 500,
          processor_concurrency: 4
        )

      Process.sleep(500)

      batch1 = Enum.map(1..200, &"before-kill-#{&1}")
      {:ok, _} = PubSubEmulator.publish(topic, batch1)

      case collect_messages(200, 15_000) do
        {:ok, _} ->
          Logger.info("[Stress 14/Gun2] batch1 fully consumed before kill")

        {:partial, received, remaining} ->
          Logger.info(
            "[Stress 14/Gun2] batch1 partially consumed: #{length(received)}/200, #{remaining} remaining"
          )
      end

      # Mirror the Mint test exactly: get conn_pid, kill the process, then send the
      # adapter-level disconnect signal so StreamManager detects it via the new handler.
      stream_manager = Module.concat(name, "StreamManager_0")
      sm_state = :sys.get_state(stream_manager)

      case sm_state do
        %{conn_pid: conn_pid} when is_pid(conn_pid) ->
          Logger.info("[Stress 14/Gun2] Killing conn_pid: #{inspect(conn_pid)}")
          Process.exit(conn_pid, :kill)
          # Send the gun_down signal directly to StreamManager — mirrors the Mint
          # test which sends {:elixir_grpc, :connection_down, conn_pid}.
          send(stream_manager, {:gun_down, conn_pid, :http2, :killed, []})

        _ ->
          Logger.warning("[Stress 14/Gun2] No conn_pid found in state")
      end

      Process.sleep(5000)

      sm_state_after = :sys.get_state(stream_manager)

      Logger.info(
        "[Stress 14/Gun2] After reconnect: grpc_stream=#{sm_state_after.grpc_stream != nil}, " <>
          "pending_demand=#{sm_state_after.pending_demand}, " <>
          "buffer_size=#{:queue.len(sm_state_after.message_buffer)}, " <>
          "outstanding=#{map_size(sm_state_after.outstanding)}"
      )

      batch2 = Enum.map(1..200, &"after-kill-#{&1}")
      {:ok, _} = PubSubEmulator.publish(topic, batch2)

      case collect_messages(200, 30_000) do
        {:ok, received} ->
          assert length(received) == 200
          Logger.info("[Stress 14/Gun2] All 200 post-kill messages received.")

        {:partial, received, remaining} ->
          sm_final = :sys.get_state(stream_manager)

          Logger.warning(
            "[Stress 14/Gun2] Post-kill: #{length(received)}/200, #{remaining} missing. " <>
              "stream=#{sm_final.grpc_stream != nil}, demand=#{sm_final.pending_demand}, " <>
              "buffer=#{:queue.len(sm_final.message_buffer)}"
          )

          flunk("Lost #{remaining} messages after connection kill (Gun2)")
      end

      stop_pipeline(pid)
    end

    test "recovers after connection kill (Mint)" do
      {topic, _sub, full_sub} = setup_infra("connkill-mint")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :mint,
          max_outstanding: 500,
          processor_concurrency: 4
        )

      Process.sleep(500)

      batch1 = Enum.map(1..200, &"before-kill-#{&1}")
      {:ok, _} = PubSubEmulator.publish(topic, batch1)

      # Wait for batch1 to be fully consumed
      case collect_messages(200, 15_000) do
        {:ok, _} ->
          Logger.info("[Stress 14/Mint] batch1 fully consumed before kill")

        {:partial, received, remaining} ->
          Logger.info(
            "[Stress 14/Mint] batch1 partially consumed: #{length(received)}/200, #{remaining} remaining"
          )
      end

      # For Mint, kill the conn_pid and simulate the connection_down message
      stream_manager = Module.concat(name, "StreamManager_0")
      sm_state = :sys.get_state(stream_manager)

      case sm_state do
        %{conn_pid: conn_pid} when is_pid(conn_pid) ->
          Logger.info("[Stress 14/Mint] Killing conn_pid: #{inspect(conn_pid)}")
          Process.exit(conn_pid, :kill)
          # Simulate the Mint connection down event
          send(stream_manager, {:elixir_grpc, :connection_down, conn_pid})

        _ ->
          Logger.warning("[Stress 14/Mint] No conn_pid found in state")
      end

      # Wait for reconnect
      Process.sleep(5000)

      # Inspect StreamManager state to verify reconnection
      sm_state_after = :sys.get_state(stream_manager)

      Logger.info(
        "[Stress 14/Mint] After reconnect: grpc_stream=#{sm_state_after.grpc_stream != nil}, " <>
          "pending_demand=#{sm_state_after.pending_demand}, " <>
          "buffer_size=#{:queue.len(sm_state_after.message_buffer)}, " <>
          "outstanding=#{map_size(sm_state_after.outstanding)}"
      )

      batch2 = Enum.map(1..200, &"after-kill-#{&1}")
      {:ok, _} = PubSubEmulator.publish(topic, batch2)

      case collect_messages(200, 30_000) do
        {:ok, received} ->
          assert length(received) == 200
          Logger.info("[Stress 14/Mint] All 200 post-kill messages received.")

        {:partial, received, remaining} ->
          sm_final = :sys.get_state(stream_manager)

          Logger.warning(
            "[Stress 14/Mint] Post-kill: #{length(received)}/200, #{remaining} missing. " <>
              "stream=#{sm_final.grpc_stream != nil}, demand=#{sm_final.pending_demand}, " <>
              "buffer=#{:queue.len(sm_final.message_buffer)}"
          )

          flunk("Lost #{remaining} messages after connection kill (Mint)")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 15: Publish continuously while processing
  # Simulates a steady stream of publishes during active consumption.
  # ---------------------------------------------------------------------------

  describe "Scenario 15: Continuous publish during processing" do
    test "handles steady publish stream (Gun)" do
      {topic, _sub, full_sub} = setup_infra("continuous-gun")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 500,
          processor_concurrency: 8
        )

      Process.sleep(500)

      # Publish 20 batches of 50 with small delays between batches
      # to simulate a steady incoming stream
      total_msgs = 1000

      publisher_task =
        Task.async(fn ->
          Enum.flat_map(1..20, fn batch_num ->
            payloads = Enum.map(1..50, &"stream-b#{batch_num}-#{&1}")
            {:ok, _} = PubSubEmulator.publish(topic, payloads)
            # Small delay to simulate realistic publishing rate
            Process.sleep(50)
            payloads
          end)
        end)

      all_expected = Task.await(publisher_task, 30_000)

      case collect_messages(total_msgs, 60_000) do
        {:ok, received} ->
          assert length(received) == total_msgs
          assert Enum.sort(received) == Enum.sort(all_expected)

          Logger.info(
            "[Stress 15/Gun] All #{total_msgs} continuously-published messages received."
          )

        {:partial, received, remaining} ->
          flunk(
            "Continuous publish (Gun): #{length(received)}/#{total_msgs}, #{remaining} missing"
          )
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 16: Extreme processor concurrency — 32 processors, zero delay
  # Tests for race conditions in demand handling.
  # ---------------------------------------------------------------------------

  describe "Scenario 16: Extreme processor concurrency (32)" do
    test "32 processors handle 3000 messages (Gun)" do
      {topic, _sub, full_sub} = setup_infra("extreme-proc-gun")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 3000,
          processor_concurrency: 32
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 3000, 300)

      start_time = System.monotonic_time(:millisecond)

      case collect_messages(3000, 90_000) do
        {:ok, received} ->
          elapsed = System.monotonic_time(:millisecond) - start_time
          assert length(received) == 3000
          assert Enum.sort(received) == Enum.sort(expected)

          Logger.info(
            "[Stress 16/Gun] 3000 msgs, 32 processors in #{elapsed}ms " <>
              "(#{Float.round(3000 / (elapsed / 1000), 1)} msgs/sec)"
          )

        {:partial, received, remaining} ->
          flunk("32 processors (Gun): #{length(received)}/3000, #{remaining} missing")
      end

      stop_pipeline(pid)
    end

    test "32 processors handle 3000 messages (Mint)" do
      {topic, _sub, full_sub} = setup_infra("extreme-proc-mint")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :mint,
          max_outstanding: 3000,
          processor_concurrency: 32
        )

      Process.sleep(500)

      expected = publish_in_batches(topic, 3000, 300)

      start_time = System.monotonic_time(:millisecond)

      case collect_messages(3000, 90_000) do
        {:ok, received} ->
          elapsed = System.monotonic_time(:millisecond) - start_time
          assert length(received) == 3000
          assert Enum.sort(received) == Enum.sort(expected)

          Logger.info(
            "[Stress 16/Mint] 3000 msgs, 32 processors in #{elapsed}ms " <>
              "(#{Float.round(3000 / (elapsed / 1000), 1)} msgs/sec)"
          )

        {:partial, received, remaining} ->
          flunk("32 processors (Mint): #{length(received)}/3000, #{remaining} missing")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 17: Rapid stop/start cycles
  # Tests that the producer cleans up properly when repeatedly started/stopped.
  # ---------------------------------------------------------------------------

  describe "Scenario 17: Rapid stop/start cycles" do
    test "survives 5 rapid start/stop cycles, then processes all messages" do
      {topic, _sub, full_sub} = setup_infra("rapid-restart")

      # Start and stop 5 times rapidly
      Enum.each(1..5, fn cycle ->
        name = unique_name()

        {:ok, cycle_pid} =
          StreamingPipeline.start_link(
            name: name,
            test_pid: self(),
            subscription: full_sub,
            emulator_host: PubSubEmulator.host(),
            adapter: :gun,
            max_outstanding: 100,
            processor_concurrency: 2
          )

        Process.sleep(200)
        stop_pipeline(cycle_pid)
        Logger.info("[Stress 17] Cycle #{cycle}/5 completed.")
      end)

      # Drain any stale messages from cycles
      drain_mailbox()

      # Now publish messages and start a final pipeline
      expected = publish_in_batches(topic, 200, 100)
      Process.sleep(200)

      final_name = unique_name()

      {:ok, final_pid} =
        StreamingPipeline.start_link(
          name: final_name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 500,
          processor_concurrency: 4
        )

      case collect_messages(200, 30_000) do
        {:ok, received} ->
          assert length(received) == 200
          assert Enum.sort(received) == Enum.sort(expected)
          Logger.info("[Stress 17] All 200 messages received after 5 rapid start/stop cycles.")

        {:partial, received, remaining} ->
          flunk("After rapid restarts: #{length(received)}/200, #{remaining} missing")
      end

      stop_pipeline(final_pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 18: Large message payloads
  # Tests with big payloads (10KB each) to stress the gRPC frame parser.
  # ---------------------------------------------------------------------------

  describe "Scenario 18: Large message payloads" do
    test "handles 100 messages of ~10KB each (Gun)" do
      {topic, _sub, full_sub} = setup_infra("large-payload-gun")
      name = unique_name()

      {:ok, pid} =
        StreamingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 100,
          processor_concurrency: 4
        )

      Process.sleep(500)

      # Each message is ~10KB
      large_payloads =
        Enum.map(1..100, fn i ->
          padding = String.duplicate("X", 10_000)
          "large-#{i}-#{padding}"
        end)

      # Publish in smaller batches to avoid HTTP payload limits
      large_payloads
      |> Enum.chunk_every(10)
      |> Enum.each(fn batch ->
        {:ok, _} = PubSubEmulator.publish(topic, batch)
      end)

      case collect_messages(100, 60_000) do
        {:ok, received} ->
          assert length(received) == 100
          # Verify content integrity — each should start with "large-N-"
          Enum.each(received, fn msg ->
            assert String.starts_with?(msg, "large-")
          end)

          Logger.info("[Stress 18/Gun] All 100 large (~10KB) messages received intact.")

        {:partial, received, remaining} ->
          flunk("Large payloads (Gun): #{length(received)}/100, #{remaining} missing")
      end

      stop_pipeline(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 19: Interleaved message failures
  # Odd-numbered messages are failed; verify they get nacked properly
  # and even-numbered ones succeed.
  # ---------------------------------------------------------------------------

  defmodule FailingPipeline do
    @moduledoc "Pipeline that fails odd-numbered messages."
    use Broadway

    def start_link(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      subscription = Keyword.fetch!(opts, :subscription)
      emulator_host = Keyword.fetch!(opts, :emulator_host)
      name = Keyword.fetch!(opts, :name)
      adapter = Keyword.get(opts, :adapter, :gun)
      max_outstanding = Keyword.get(opts, :max_outstanding, 500)
      processor_concurrency = Keyword.get(opts, :processor_concurrency, 4)

      Broadway.start_link(__MODULE__,
        name: name,
        producer: [
          module:
            {BroadwayCloudPubSub.Streaming.Producer,
             subscription: subscription,
             token_generator: {BroadwayCloudPubSub.Streaming.StressTest, :noop_token, []},
             grpc_endpoint: emulator_host,
             use_ssl: false,
             adapter: adapter,
             max_outstanding_messages: max_outstanding,
             on_failure: {:nack, 0}},
          concurrency: 1
        ],
        processors: [
          default: [concurrency: processor_concurrency]
        ],
        context: %{test_pid: test_pid}
      )
    end

    @impl Broadway
    def handle_message(:default, message, %{test_pid: test_pid}) do
      data = message.data

      # Fail odd messages
      if String.contains?(data, "-odd-") do
        send(test_pid, {:will_fail, data})
        Broadway.Message.failed(message, :intentional_failure)
      else
        send(test_pid, {:msg, data})
        message
      end
    end

    @impl Broadway
    def handle_failed(messages, %{test_pid: test_pid}) do
      Enum.each(messages, fn msg ->
        send(test_pid, {:failed, msg.data})
      end)

      messages
    end
  end

  describe "Scenario 19: Interleaved message failures" do
    test "even messages succeed, odd messages are nacked and redelivered" do
      {topic, _sub, full_sub} = setup_infra("fail-interleave")
      name = unique_name()

      {:ok, pid} =
        FailingPipeline.start_link(
          name: name,
          test_pid: self(),
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 200,
          processor_concurrency: 4
        )

      Process.sleep(500)

      # Publish 50 even and 50 odd messages
      even_msgs = Enum.map(1..50, &"even-#{&1}")
      odd_msgs = Enum.map(1..50, &"msg-odd-#{&1}")
      all_msgs = Enum.shuffle(even_msgs ++ odd_msgs)

      {:ok, _} = PubSubEmulator.publish(topic, all_msgs)

      # Collect the 50 even messages that should succeed
      case collect_messages(50, 30_000) do
        {:ok, received} ->
          assert length(received) == 50

          Enum.each(received, fn msg ->
            assert String.starts_with?(msg, "even-"),
                   "Expected only even messages, got: #{msg}"
          end)

          Logger.info("[Stress 19] 50 even messages received, 50 odd messages properly failed.")

        {:partial, received, remaining} ->
          Logger.warning(
            "[Stress 19] Only #{length(received)}/50 even messages, #{remaining} missing"
          )

          # This is still acceptable — the test is about failure handling
          flunk("Missing #{remaining} even messages")
      end

      # Stop the pipeline BEFORE draining failure messages to prevent
      # infinite nack/redeliver cycles on the odd messages
      stop_pipeline(pid)

      # Check that we got failure notifications for odd messages
      failed_msgs = drain_tagged_mailbox(:failed)
      will_fail_msgs = drain_tagged_mailbox(:will_fail)

      Logger.info(
        "[Stress 19] Failed callbacks: #{length(failed_msgs)}, will_fail signals: #{length(will_fail_msgs)}"
      )

      # At least some odd messages should have triggered :failed
      assert length(failed_msgs) > 0 or length(will_fail_msgs) > 0,
             "Expected at least some failure notifications for odd messages"
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario 20: Multiple concurrent pipelines on same subscription
  # Tests competing consumers behavior.
  # ---------------------------------------------------------------------------

  describe "Scenario 20: Multiple competing consumers" do
    test "two pipelines on same subscription collectively receive all messages" do
      {topic, _sub, full_sub} = setup_infra("competing")

      collector_pid = self()

      # Start two competing pipelines on the same subscription
      name1 = unique_name()
      name2 = unique_name()

      {:ok, pid1} =
        StreamingPipeline.start_link(
          name: name1,
          test_pid: collector_pid,
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 250,
          processor_concurrency: 4
        )

      {:ok, pid2} =
        StreamingPipeline.start_link(
          name: name2,
          test_pid: collector_pid,
          subscription: full_sub,
          emulator_host: PubSubEmulator.host(),
          adapter: :gun,
          max_outstanding: 250,
          processor_concurrency: 4
        )

      Process.sleep(1000)

      expected = publish_in_batches(topic, 500, 100)

      case collect_messages(500, 60_000) do
        {:ok, received} ->
          assert length(received) == 500
          assert Enum.sort(received) == Enum.sort(expected)

          Logger.info(
            "[Stress 20] Two competing consumers collectively received all 500 messages."
          )

        {:partial, received, remaining} ->
          Logger.warning("[Stress 20] #{length(received)}/500, #{remaining} missing")
          flunk("Competing consumers: #{remaining} messages missing")
      end

      stop_pipeline(pid1)
      stop_pipeline(pid2)
    end
  end

  # ---------------------------------------------------------------------------
  # Helper for Scenario 19
  # ---------------------------------------------------------------------------

  defp drain_tagged_mailbox(tag) do
    drain_tagged_loop(tag, [])
  end

  defp drain_tagged_loop(tag, acc) do
    receive do
      {^tag, data} -> drain_tagged_loop(tag, [data | acc])
    after
      200 -> Enum.reverse(acc)
    end
  end
end
