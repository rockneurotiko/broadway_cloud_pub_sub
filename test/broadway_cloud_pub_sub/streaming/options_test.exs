defmodule BroadwayCloudPubSub.Streaming.OptionsTest do
  use ExUnit.Case, async: true

  alias BroadwayCloudPubSub.Streaming.Options

  defp validate(opts) do
    # Inject required broadway key for the schema
    opts = Keyword.put_new(opts, :broadway, name: :TestPipeline)
    NimbleOptions.validate(opts, Options.definition())
  end

  describe "subscription" do
    test "is required" do
      assert {:error, err} = validate([])
      assert Exception.message(err) =~ "required"
      assert Exception.message(err) =~ "subscription"
    end

    test "accepts a valid subscription path" do
      assert {:ok, opts} =
               validate(subscription: "projects/my-project/subscriptions/my-sub")

      assert opts[:subscription] == "projects/my-project/subscriptions/my-sub"
    end

    test "rejects an empty string" do
      assert {:error, err} = validate(subscription: "")
      assert Exception.message(err) =~ "non-empty string"
    end

    test "rejects a non-string value" do
      assert {:error, err} = validate(subscription: 123)
      assert Exception.message(err) =~ "non-empty string"
    end
  end

  describe "max_outstanding_messages" do
    test "defaults to 1000" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:max_outstanding_messages] == 1_000
    end

    test "accepts a positive integer" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", max_outstanding_messages: 500)

      assert opts[:max_outstanding_messages] == 500
    end

    test "rejects zero" do
      assert {:error, _} =
               validate(subscription: "projects/p/subscriptions/s", max_outstanding_messages: 0)
    end
  end

  describe "stream_ack_deadline_seconds" do
    test "defaults to 60" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:stream_ack_deadline_seconds] == 60
    end

    test "accepts value within range" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", stream_ack_deadline_seconds: 120)

      assert opts[:stream_ack_deadline_seconds] == 120
    end

    test "rejects value below 10" do
      assert {:error, err} =
               validate(
                 subscription: "projects/p/subscriptions/s",
                 stream_ack_deadline_seconds: 5
               )

      assert Exception.message(err) =~ "between 10 and 600"
    end

    test "rejects value above 600" do
      assert {:error, err} =
               validate(
                 subscription: "projects/p/subscriptions/s",
                 stream_ack_deadline_seconds: 601
               )

      assert Exception.message(err) =~ "between 10 and 600"
    end
  end

  describe "on_success / on_failure" do
    test "on_success defaults to :ack" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:on_success] == :ack
    end

    test "on_failure defaults to :noop" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:on_failure] == :noop
    end

    test "accepts :ack" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_success: :ack)

      assert opts[:on_success] == :ack
    end

    test "accepts :noop" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_success: :noop)

      assert opts[:on_success] == :noop
    end

    test "normalises :nack to {:nack, 0}" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_success: :nack)

      assert opts[:on_success] == {:nack, 0}
    end

    test "accepts {:nack, integer} within range" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_failure: {:nack, 30})

      assert opts[:on_failure] == {:nack, 30}
    end

    test "rejects invalid on_success value" do
      assert {:error, err} =
               validate(subscription: "projects/p/subscriptions/s", on_success: :bad)

      assert Exception.message(err) =~ "on_success"
    end
  end

  describe "on_shutdown" do
    test "defaults to {:nack, 5}" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:on_shutdown] == {:nack, 5}
    end

    test "accepts :noop" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_shutdown: :noop)

      assert opts[:on_shutdown] == :noop
    end

    test "normalises :nack to {:nack, 0}" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_shutdown: :nack)

      assert opts[:on_shutdown] == {:nack, 0}
    end

    test "accepts {:nack, N}" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", on_shutdown: {:nack, 10})

      assert opts[:on_shutdown] == {:nack, 10}
    end

    test "rejects invalid shutdown option" do
      assert {:error, err} =
               validate(subscription: "projects/p/subscriptions/s", on_shutdown: :ack)

      assert Exception.message(err) =~ "on_shutdown"
    end
  end

  describe "backoff_type" do
    test "defaults to :rand_exp" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:backoff_type] == :rand_exp
    end

    test "accepts all valid types" do
      for type <- [:rand_exp, :exp, :rand, :stop] do
        assert {:ok, _} =
                 validate(subscription: "projects/p/subscriptions/s", backoff_type: type)
      end
    end

    test "rejects unknown type" do
      assert {:error, _} =
               validate(subscription: "projects/p/subscriptions/s", backoff_type: :linear)
    end
  end

  describe "grpc_endpoint" do
    test "defaults to pubsub.googleapis.com:443" do
      {:ok, opts} = validate(subscription: "projects/p/subscriptions/s")
      assert opts[:grpc_endpoint] == "pubsub.googleapis.com:443"
    end

    test "accepts custom endpoint" do
      {:ok, opts} =
        validate(subscription: "projects/p/subscriptions/s", grpc_endpoint: "localhost:8085")

      assert opts[:grpc_endpoint] == "localhost:8085"
    end

    test "rejects empty string" do
      assert {:error, err} =
               validate(subscription: "projects/p/subscriptions/s", grpc_endpoint: "")

      assert Exception.message(err) =~ "non-empty string"
    end
  end

  describe "type_ack_option/2" do
    test "converts :nack atom to {:nack, 0}" do
      assert {:ok, {:nack, 0}} = Options.type_ack_option(:nack, [])
    end

    test "passes through :ack" do
      assert {:ok, :ack} = Options.type_ack_option(:ack, [])
    end

    test "passes through :noop" do
      assert {:ok, :noop} = Options.type_ack_option(:noop, [])
    end

    test "passes through {:nack, N} within range" do
      assert {:ok, {:nack, 300}} = Options.type_ack_option({:nack, 300}, [])
    end

    test "rejects {:nack, N} outside range" do
      assert {:error, _} = Options.type_ack_option({:nack, 601}, name: :on_success)
    end
  end

  describe "type_shutdown_option/2" do
    test "converts :nack to {:nack, 0}" do
      assert {:ok, {:nack, 0}} = Options.type_shutdown_option(:nack, [])
    end

    test "accepts :noop" do
      assert {:ok, :noop} = Options.type_shutdown_option(:noop, [])
    end

    test "accepts {:nack, N}" do
      assert {:ok, {:nack, 5}} = Options.type_shutdown_option({:nack, 5}, [])
    end

    test "rejects :ack" do
      assert {:error, _} =
               Options.type_shutdown_option(:ack, name: :on_shutdown)
    end
  end
end
