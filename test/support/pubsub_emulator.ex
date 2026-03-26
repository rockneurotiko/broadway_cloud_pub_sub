defmodule BroadwayCloudPubSub.PubSubEmulator do
  @moduledoc """
  Helpers for integration tests against the Cloud Pub/Sub emulator.

  The emulator must be running on `PUBSUB_EMULATOR_HOST` (default `localhost:8085`).
  It exposes both HTTP/REST and gRPC on the same port, without TLS.

  ## Usage

      @moduletag :integration

      setup do
        BroadwayCloudPubSub.PubSubEmulator.setup_topic_and_subscription(
          "my-test-topic",
          "my-test-sub"
        )
      end
  """

  @default_host "localhost:8085"
  @project "test-project"
  @finch_name BroadwayCloudPubSub.PubSubEmulator.Finch

  @doc "Returns the emulator host:port (from env or default)."
  def host do
    System.get_env("PUBSUB_EMULATOR_HOST", @default_host)
  end

  @doc "Returns the test GCP project ID."
  def project, do: @project

  @doc "Returns the full subscription name."
  def subscription(sub_name) do
    "projects/#{@project}/subscriptions/#{sub_name}"
  end

  @doc "Returns the full topic name."
  def topic(topic_name) do
    "projects/#{@project}/topics/#{topic_name}"
  end

  @doc """
  Starts the internal Finch pool used for emulator REST calls.
  Call this once in your `setup` or `setup_all`.
  """
  def start do
    {:ok, _} =
      Finch.start_link(
        name: @finch_name,
        pools: %{
          :default => [size: 5]
        }
      )

    :ok
  end

  @doc """
  Creates a topic, then a subscription bound to it.
  Deletes them first if they already exist (idempotent).
  Returns `{full_topic, full_sub}` as full resource paths.
  """
  def setup_topic_and_subscription(topic_name, sub_name, opts \\ []) do
    ack_deadline = Keyword.get(opts, :ack_deadline_seconds, 60)
    full_topic = topic(topic_name)
    full_sub = subscription(sub_name)

    # Idempotent: delete if they exist (ignore errors)
    delete_subscription(full_sub)
    delete_topic(full_topic)

    :ok = create_topic(full_topic)
    :ok = create_subscription(full_sub, full_topic, ack_deadline)

    {full_topic, full_sub}
  end

  @doc "Publish messages via the emulator REST API. `messages` is a list of string payloads."
  def publish(topic_name, messages) when is_list(messages) do
    full_topic = topic(topic_name)

    body =
      Jason.encode!(%{
        messages:
          Enum.map(messages, fn msg ->
            %{data: Base.encode64(msg)}
          end)
      })

    url = "http://#{host()}/v1/#{full_topic}:publish"

    case request(:post, url, body) do
      {:ok, 200, response_body} ->
        decoded = Jason.decode!(response_body)
        {:ok, decoded["messageIds"]}

      {:ok, status, body} ->
        {:error, {:http_error, status, body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Pulls messages synchronously via the REST API (for post-ack verification)."
  def pull(sub_name, opts \\ []) do
    max = Keyword.get(opts, :max_messages, 10)
    full_sub = subscription(sub_name)

    body = Jason.encode!(%{maxMessages: max, returnImmediately: true})
    url = "http://#{host()}/v1/#{full_sub}:pull"

    case request(:post, url, body) do
      {:ok, 200, response_body} ->
        decoded = Jason.decode!(response_body)
        {:ok, Map.get(decoded, "receivedMessages", [])}

      {:ok, status, body} ->
        {:error, {:http_error, status, body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- Private REST helpers ---

  defp create_topic(full_topic) do
    url = "http://#{host()}/v1/#{full_topic}"

    case request(:put, url, "{}") do
      {:ok, status, _} when status in [200, 409] -> :ok
      {:ok, status, body} -> {:error, {:http_error, status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp create_subscription(full_sub, full_topic, ack_deadline) do
    url = "http://#{host()}/v1/#{full_sub}"

    body =
      Jason.encode!(%{
        topic: full_topic,
        ackDeadlineSeconds: ack_deadline
      })

    case request(:put, url, body) do
      {:ok, status, _} when status in [200, 409] -> :ok
      {:ok, status, body} -> {:error, {:http_error, status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_topic(full_topic) do
    url = "http://#{host()}/v1/#{full_topic}"
    request(:delete, url, "")
    :ok
  end

  defp delete_subscription(full_sub) do
    url = "http://#{host()}/v1/#{full_sub}"
    request(:delete, url, "")
    :ok
  end

  defp request(method, url, body) do
    headers = [{"content-type", "application/json"}]
    req = Finch.build(method, url, headers, body)

    case Finch.request(req, @finch_name) do
      {:ok, %Finch.Response{status: status, body: resp_body}} ->
        {:ok, status, resp_body}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
