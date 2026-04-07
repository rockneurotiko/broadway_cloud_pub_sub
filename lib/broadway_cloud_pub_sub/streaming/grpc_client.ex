defmodule BroadwayCloudPubSub.Streaming.GrpcClient do
  @moduledoc """
  The default gRPC client for `BroadwayCloudPubSub.Streaming.Producer`.

  Implements `BroadwayCloudPubSub.Streaming.Client` using the `grpc_client` library
  with the `Google.Pubsub.V1.Subscriber.Stub` generated stub.

  This module handles:
  - Token fetching and channel connection (with TLS and adapter configuration)
  - Bidirectional `StreamingPull` stream management
  - Unary `Acknowledge` and `ModifyAckDeadline` RPCs with telemetry spans

  ## Telemetry

  This module emits the following telemetry events:

  * `[:broadway_cloud_pub_sub, :streaming, :ack, :start | :stop | :exception]` — emitted
    as a span when sending an `Acknowledge` unary RPC.

    Measurements: as described in `:telemetry.span/3`.
    Metadata: `%{name: broadway_name, subscription: subscription, count: ack_count}`

  * `[:broadway_cloud_pub_sub, :streaming, :modack, :start | :stop | :exception]` — emitted
    as a span when sending a `ModifyAckDeadline` unary RPC.

    Measurements: as described in `:telemetry.span/3`.
    Metadata: `%{name: broadway_name, subscription: subscription, count: ack_count}`
  """

  @behaviour BroadwayCloudPubSub.Streaming.Client

  alias Google.Pubsub.V1.Subscriber.Stub
  alias Google.Pubsub.V1.{AcknowledgeRequest, ModifyAckDeadlineRequest}

  # Default RPC timeout for unary calls.
  @unary_rpc_timeout_ms 30_000

  @impl BroadwayCloudPubSub.Streaming.Client
  def init(opts) do
    {:ok, Map.new(opts)}
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def connect(config) do
    with {:ok, token} <- fetch_token(config) do
      open_channel(config, token)
    end
  rescue
    e -> {:error, {:connect_failed, Exception.message(e)}}
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def disconnect(channel, _config) do
    try do
      GRPC.Stub.disconnect(channel)
    catch
      _, _ -> :ok
    end

    :ok
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def streaming_pull(channel, _config) do
    Stub.streaming_pull(channel, [])
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def send_request(stream, request, _config) do
    case GRPC.Stub.send_request(stream, request) do
      %GRPC.Client.Stream{} = updated_stream -> {:ok, updated_stream}
      {:error, reason} -> {:error, reason}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def recv(stream, _config) do
    GRPC.Stub.recv(stream, timeout: :infinity)
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def cancel(stream, _config) do
    try do
      GRPC.Stub.cancel(stream)
    catch
      _, _ -> :ok
    end

    :ok
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def acknowledge(channel, %AcknowledgeRequest{ack_ids: ack_ids} = request, config) do
    :telemetry.span(
      [:broadway_cloud_pub_sub, :streaming, :ack],
      %{name: config.broadway_name, subscription: config.subscription},
      fn ->
        result = Stub.acknowledge(channel, request, timeout: @unary_rpc_timeout_ms)

        {result,
         %{name: config.broadway_name, subscription: config.subscription, count: length(ack_ids)}}
      end
    )
  end

  @impl BroadwayCloudPubSub.Streaming.Client
  def modify_ack_deadline(
        channel,
        %ModifyAckDeadlineRequest{ack_ids: ack_ids} = request,
        config
      ) do
    :telemetry.span(
      [:broadway_cloud_pub_sub, :streaming, :modack],
      %{name: config.broadway_name, subscription: config.subscription},
      fn ->
        result = Stub.modify_ack_deadline(channel, request, timeout: @unary_rpc_timeout_ms)

        {result,
         %{name: config.broadway_name, subscription: config.subscription, count: length(ack_ids)}}
      end
    )
  end

  # --- Private ---

  defp fetch_token(%{token_generator: {mod, fun, args}}) do
    apply(mod, fun, args)
  end

  defp open_channel(
         %{grpc_endpoint: endpoint, use_ssl: use_ssl, adapter: adapter} = config,
         token
       ) do
    keepalive_interval_ms = Map.get(config, :keepalive_interval_ms, 30_000)

    adapter_opts = [http2_opts: %{keepalive: keepalive_interval_ms, settings_timeout: :infinity}]

    adapter_opts =
      case Map.get(config, :test_pid) do
        nil -> adapter_opts
        pid -> Keyword.put(adapter_opts, :test_pid, pid)
      end

    base_opts = [
      adapter: adapter,
      headers: [{"authorization", "Bearer #{token}"}],
      adapter_opts: adapter_opts
    ]

    opts =
      if use_ssl do
        cred = GRPC.Credential.new(ssl: [cacerts: :public_key.cacerts_get()])
        Keyword.put(base_opts, :cred, cred)
      else
        base_opts
      end

    case GRPC.Stub.connect(endpoint, opts) do
      {:ok, channel} -> {:ok, channel}
      {:error, reason} -> {:error, {:connect_failed, reason}}
    end
  end
end
