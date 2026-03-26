defmodule BroadwayCloudPubSub.Streaming.ProducerIntegrationTest do
  @moduledoc """
  Integration tests for `StreamingProducer` against the Cloud Pub/Sub emulator.

  These tests require the emulator to be running on `PUBSUB_EMULATOR_HOST`
  (default `localhost:8085`). Run with:

      mix test --only integration

  Or with env:

      PUBSUB_EMULATOR_HOST=localhost:8085 mix test --only integration
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 30_000

  alias BroadwayCloudPubSub.PubSubEmulator

  # No-op token generator for the emulator (no auth required)
  def noop_token, do: {:ok, "emulator-no-auth"}

  # Minimal Broadway pipeline that sends received messages to the test process
  defmodule TestPipeline do
    use Broadway

    def start_link(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      subscription = Keyword.fetch!(opts, :subscription)
      emulator_host = Keyword.fetch!(opts, :emulator_host)
      name = Keyword.fetch!(opts, :name)

      Broadway.start_link(__MODULE__,
        name: name,
        producer: [
          module:
            {BroadwayCloudPubSub.Streaming.Producer,
             subscription: subscription,
             token_generator:
               {BroadwayCloudPubSub.Streaming.ProducerIntegrationTest, :noop_token, []},
             grpc_endpoint: emulator_host,
             use_ssl: false,
             max_outstanding_messages: 100,
             on_failure: {:nack, 0}},
          concurrency: 1
        ],
        processors: [
          default: [concurrency: 2]
        ],
        context: %{test_pid: test_pid}
      )
    end

    @impl Broadway
    def handle_message(:default, message, %{test_pid: test_pid}) do
      require Logger

      Logger.debug(
        "[TestPipeline] handle_message data=#{inspect(message.data)} ack_ref=#{inspect(elem(message.acknowledger, 1))}"
      )

      send(test_pid, {:broadway_message, message.data, message.metadata})
      message
    end

    @impl Broadway
    def handle_failed(messages, %{test_pid: test_pid}) do
      Enum.each(messages, fn msg ->
        send(test_pid, {:broadway_failed, msg.data})
      end)

      messages
    end
  end

  setup_all do
    {:ok, _} = DynamicSupervisor.start_link(strategy: :one_for_one, name: GRPC.Client.Supervisor)
    PubSubEmulator.start()
    :ok
  end

  setup do
    topic_name = "broadway-integration-#{:erlang.unique_integer([:positive])}"
    sub_name = "broadway-integration-sub-#{:erlang.unique_integer([:positive])}"

    {_topic, subscription} =
      PubSubEmulator.setup_topic_and_subscription(topic_name, sub_name, ack_deadline_seconds: 60)

    pipeline_name = :"TestPipeline#{:erlang.unique_integer([:positive])}"

    {:ok, pid} =
      TestPipeline.start_link(
        name: pipeline_name,
        test_pid: self(),
        subscription: subscription,
        emulator_host: PubSubEmulator.host()
      )

    on_exit(fn ->
      ref = Process.monitor(pid)

      try do
        Broadway.stop(pid)
      catch
        :exit, _ -> :ok
      end

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      after
        5_000 -> :ok
      end
    end)

    # Give the pipeline a moment to connect to the emulator
    Process.sleep(500)

    {:ok,
     topic: topic_name,
     sub: sub_name,
     subscription: subscription,
     pipeline: pid,
     pipeline_name: pipeline_name}
  end

  describe "message delivery" do
    test "receives a single published message", %{topic: topic} do
      {:ok, [_msg_id]} = PubSubEmulator.publish(topic, ["hello world"])

      assert_receive {:broadway_message, "hello world", _metadata}, 5_000
    end

    test "receives multiple published messages", %{topic: topic} do
      payloads = Enum.map(1..5, &"message-#{&1}")
      {:ok, _msg_ids} = PubSubEmulator.publish(topic, payloads)

      received =
        Enum.map(1..5, fn _ ->
          receive do
            {:broadway_message, data, _meta} -> data
          after
            5_000 -> flunk("Timed out waiting for message")
          end
        end)

      assert Enum.sort(received) == Enum.sort(payloads)
    end

    test "message metadata contains messageId and publishTime", %{topic: topic} do
      {:ok, [_msg_id]} = PubSubEmulator.publish(topic, ["meta-test"])

      assert_receive {:broadway_message, "meta-test", metadata}, 5_000
      assert is_binary(metadata.messageId)
      assert metadata.messageId != ""
      # publishTime may be nil on some emulator versions — check the key exists
      assert Map.has_key?(metadata, :publishTime)
      assert Map.has_key?(metadata, :attributes)
    end

    test "handles large batches without dropping messages", %{topic: topic} do
      count = 50
      payloads = Enum.map(1..count, &"bulk-msg-#{&1}")
      {:ok, _msg_ids} = PubSubEmulator.publish(topic, payloads)

      received =
        Enum.map(1..count, fn _ ->
          receive do
            {:broadway_message, data, _meta} -> data
          after
            10_000 -> flunk("Timed out waiting for bulk message")
          end
        end)

      assert length(received) == count
      assert Enum.sort(received) == Enum.sort(payloads)
    end
  end

  describe "acknowledgement" do
    test "acked messages are not redelivered", %{topic: topic, sub: sub} do
      {:ok, [_id]} = PubSubEmulator.publish(topic, ["ack-me"])

      assert_receive {:broadway_message, "ack-me", _}, 5_000

      # Wait for ack to be processed, then confirm no pending messages remain
      Process.sleep(500)

      {:ok, messages} = PubSubEmulator.pull(sub, max_messages: 5)
      assert messages == []
    end
  end
end
