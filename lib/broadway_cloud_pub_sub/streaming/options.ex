defmodule BroadwayCloudPubSub.Streaming.Options do
  @moduledoc """
  Options for `BroadwayCloudPubSub.Streaming.Producer`.
  """

  @default_grpc_endpoint "pubsub.googleapis.com:443"
  @default_max_outstanding_messages 1_000
  @default_max_outstanding_bytes 100 * 1024 * 1024
  @default_stream_ack_deadline_seconds 60
  @default_lease_extension_percent 0.6
  @default_backoff_min 1_000
  @default_backoff_max 30_000
  @default_keepalive_interval_ms 30_000

  definition = [
    # Handled by Broadway.
    broadway: [type: :any, doc: false],
    subscription: [
      type: {:custom, __MODULE__, :type_non_empty_string, [[{:name, :subscription}]]},
      required: true,
      doc: """
      The name of the subscription, including the project.
      For example, if your project is `"my-project"` and your
      subscription is `"my-subscription"`, the full name is
      `"projects/my-project/subscriptions/my-subscription"`.
      """
    ],
    max_outstanding_messages: [
      type: :pos_integer,
      default: @default_max_outstanding_messages,
      doc: """
      The maximum number of outstanding messages (delivered but not yet
      acknowledged) that the server will push. Acts as the primary flow
      control mechanism. Analogous to AMQP `prefetch_count`.
      """
    ],
    max_outstanding_bytes: [
      type: :pos_integer,
      default: @default_max_outstanding_bytes,
      doc: """
      The maximum total size in bytes of outstanding messages. The server
      will not push more messages if the total byte size of outstanding
      messages exceeds this limit. Defaults to 100 MiB.
      """
    ],
    stream_ack_deadline_seconds: [
      type:
        {:custom, __MODULE__, :type_integer_in_range,
         [[{:name, :stream_ack_deadline_seconds}, {:min, 10}, {:max, 600}]]},
      default: @default_stream_ack_deadline_seconds,
      doc: """
      The number of seconds the server will wait before re-delivering an
      unacknowledged message. Must be between 10 and 600. Defaults to 60.
      The producer will extend leases automatically before this deadline.
      """
    ],
    lease_extension_percent: [
      type:
        {:custom, __MODULE__, :type_float_between_0_and_1, [[{:name, :lease_extension_percent}]]},
      default: @default_lease_extension_percent,
      doc: """
      The fraction of `stream_ack_deadline_seconds` at which leases are
      extended. For example, with a deadline of 60s and a percent of 0.6,
      leases are extended every 36s. Must be between 0.0 and 1.0 exclusive.
      Defaults to 0.6.
      """
    ],
    client_id: [
      type: :string,
      doc: """
      An identifier that can be used to distinguish individual instances of
      the producer. If not provided, a unique ID will be generated. Using
      a stable `client_id` across reconnections enables the server to use
      sticky assignment for ordered subscriptions.
      """
    ],
    on_success: [
      type: {:custom, __MODULE__, :type_ack_option, [[{:name, :on_success}]]},
      default: :ack,
      doc: """
      Configures the acknowledgement behaviour for successfully processed
      messages. Defaults to `:ack`.
      """
    ],
    on_failure: [
      type: {:custom, __MODULE__, :type_ack_option, [[{:name, :on_failure}]]},
      default: :noop,
      doc: """
      Configures the acknowledgement behaviour for failed messages.
      Defaults to `:noop`.
      """
    ],
    on_shutdown: [
      type: {:custom, __MODULE__, :type_shutdown_option, [[{:name, :on_shutdown}]]},
      default: {:nack, 5},
      doc: """
      Configures what happens to messages received but not yet processed
      when the producer is shut down.

        * `{:nack, seconds}` - Sends a `modifyAckDeadline` request with the
          given `seconds` for all outstanding messages, making them available
          for redelivery after that delay. The default `{:nack, 5}` provides
          a small delay to avoid thundering herd on rolling deploys.
        * `:nack` - Equivalent to `{:nack, 0}`. Immediately makes unprocessed
          messages available for redelivery.
        * `:noop` - Does nothing. Messages become available after their ack
          deadline expires naturally.

      Defaults to `{:nack, 5}`.
      """
    ],
    goth: [
      type: :atom,
      doc: """
      The `Goth` module to use for authentication. Note that this option only
      applies to the default token generator.
      """
    ],
    token_generator: [
      type: :mfa,
      doc: """
      An MFArgs tuple that will be called before each gRPC connection to fetch
      an authentication token. Should return `{:ok, String.t()} | {:error, any()}`.
      By default this will invoke `Goth.fetch/1` with the `:goth` option.
      """
    ],
    backoff_type: [
      type: {:in, [:rand_exp, :exp, :rand, :stop]},
      default: :rand_exp,
      doc: """
      The backoff strategy used when reconnecting after a stream failure.

        * `:rand_exp` - Randomized exponential backoff (default). Adds jitter
          to prevent thundering herd after mass disconnects.
        * `:exp` - Pure exponential backoff.
        * `:rand` - Random value between `backoff_min` and `backoff_max`.
        * `:stop` - Do not reconnect. The producer will crash after one failure.

      """
    ],
    backoff_min: [
      type: :pos_integer,
      default: @default_backoff_min,
      doc: "Minimum reconnection backoff in milliseconds. Defaults to 1000."
    ],
    backoff_max: [
      type: :pos_integer,
      default: @default_backoff_max,
      doc: "Maximum reconnection backoff in milliseconds. Defaults to 30000."
    ],
    keepalive_interval_ms: [
      type: :pos_integer,
      default: @default_keepalive_interval_ms,
      doc: """
      Interval in milliseconds at which HTTP/2 PING frames are sent on the gRPC
      connection to keep it alive. This prevents Google Cloud's load balancer
      from closing idle connections (which it does after roughly 20 seconds by
      default). Matches the 30-second keepalive interval used by the official
      Python and Go Pub/Sub client libraries. Only applies to the `:gun` adapter.
      Defaults to 30000.
      """
    ],
    adapter: [
      type: {:in, [:gun, :mint]},
      default: :gun,
      doc: """
      The gRPC HTTP/2 adapter to use for the streaming connection.

        * `:gun` — Uses the Gun HTTP/2 client (default). Gun is well-tested
          and is the traditional adapter for the Elixir gRPC library.
        * `:mint` — Uses the Mint HTTP/2 client. Mint may be preferable in
          deployment environments where Gun is not available or not desired.

      Both adapters are provided by the `grpc_client` dependency. The adapter choice
      does not affect the public API or message semantics.
      """
    ],
    grpc_endpoint: [
      type: {:custom, __MODULE__, :type_non_empty_string, [[{:name, :grpc_endpoint}]]},
      default: @default_grpc_endpoint,
      doc: """
      The gRPC endpoint for the Cloud Pub/Sub service. Useful for testing
      with the Pub/Sub emulator (e.g., `"localhost:8085"`).
      """
    ],
    use_ssl: [
      type: :boolean,
      default: true,
      doc: """
      Whether to use TLS when connecting to the gRPC endpoint. Set to `false`
      when connecting to the Pub/Sub emulator, which does not use TLS.
      Defaults to `true`.
      """
    ],

    # Testing options
    test_pid: [type: :pid, doc: false],
    message_server: [type: :pid, doc: false]
  ]

  @definition NimbleOptions.new!(definition)

  def definition do
    @definition
  end

  @acknowledger_definition definition
                           |> Keyword.take([:on_failure, :on_success])
                           |> NimbleOptions.new!()

  def acknowledger_definition do
    @acknowledger_definition
  end

  @doc """
  Builds an MFArgs tuple for a token generator using Goth.
  """
  defdelegate make_token_generator(opts), to: BroadwayCloudPubSub.Options

  # --- Custom type validators ---

  def type_non_empty_string("", [{:name, name}]) do
    {:error, "expected :#{name} to be a non-empty string, got: \"\""}
  end

  def type_non_empty_string(value, _) when is_binary(value) do
    {:ok, value}
  end

  def type_non_empty_string(value, [{:name, name}]) do
    {:error, "expected :#{name} to be a non-empty string, got: #{inspect(value)}"}
  end

  def type_integer_in_range(value, [{:name, _name}, {:min, min}, {:max, max}])
      when is_integer(value) and value >= min and value <= max do
    {:ok, value}
  end

  def type_integer_in_range(value, [{:name, name}, {:min, min}, {:max, max}]) do
    {:error,
     "expected :#{name} to be an integer between #{min} and #{max}, got: #{inspect(value)}"}
  end

  def type_float_between_0_and_1(value, _) when is_float(value) and value > 0.0 and value < 1.0 do
    {:ok, value}
  end

  def type_float_between_0_and_1(value, [{:name, name}]) do
    {:error,
     "expected :#{name} to be a float between 0.0 and 1.0 exclusive, got: #{inspect(value)}"}
  end

  def type_ack_option(:ack, _), do: {:ok, :ack}
  def type_ack_option(:noop, _), do: {:ok, :noop}
  def type_ack_option(:nack, _), do: {:ok, {:nack, 0}}

  def type_ack_option({:nack, value}, _)
      when is_integer(value) and value >= 0 and value <= 600 do
    {:ok, {:nack, value}}
  end

  def type_ack_option(value, [{:name, name}]) do
    {:error,
     "expected :#{name} to be one of :ack, :noop, :nack, or {:nack, integer} where " <>
       "integer is between 0 and 600, got: #{inspect(value)}"}
  end

  def type_shutdown_option(:nack, _), do: {:ok, {:nack, 0}}
  def type_shutdown_option(:noop, _), do: {:ok, :noop}

  def type_shutdown_option({:nack, value}, _)
      when is_integer(value) and value >= 0 and value <= 600 do
    {:ok, {:nack, value}}
  end

  def type_shutdown_option(value, [{:name, name}]) do
    {:error,
     "expected :#{name} to be :nack, :noop, or {:nack, integer} where " <>
       "integer is between 0 and 600, got: #{inspect(value)}"}
  end
end
