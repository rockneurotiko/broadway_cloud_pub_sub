defmodule BroadwayCloudPubSub.Streaming.Telemetry do
  @moduledoc false

  # Centralised telemetry helpers for the streaming Pub/Sub producer.
  #
  # All streaming telemetry events share the top-level prefix
  # `[:broadway_cloud_pub_sub, :streaming]`, then a sub-prefix identifying
  # the emitting layer:
  #
  #   :stream      — StreamManager (stream lifecycle, message flow, acks)
  #   :ack_batcher — AckBatcher (batch flush behaviour, retry exhaustion)
  #   :unary       — UnaryRpcClient (channel lifecycle, permanent failures)
  #   :grpc_client — GrpcClient (unary RPC spans for ack and modack)
  #
  # Usage:
  #
  #   Telemetry.execute(:stream, :connect, %{}, %{name: name, subscription: sub})
  #   Telemetry.span(:grpc_client, :ack, %{name: name, subscription: sub}, fn -> ... end)

  @base [:broadway_cloud_pub_sub, :streaming]

  @doc """
  Executes a telemetry event under `[:broadway_cloud_pub_sub, :streaming, layer, event]`.

  `telemetry_metadata` is the raw `:telemetry_metadata` option value from the producer
  config (a static term, an `{m, f, a}` tuple, or `nil`). When non-nil, its resolved
  value is merged into `metadata` under the `:extra` key.
  """
  @spec execute(atom(), atom(), map(), map(), term()) :: :ok
  def execute(layer, event, measurements, metadata, telemetry_metadata) do
    :telemetry.execute(
      @base ++ [layer, event],
      measurements,
      maybe_put_extra(metadata, resolve_extra(telemetry_metadata))
    )
  end

  @doc """
  Wraps `fun` in a telemetry span under `[:broadway_cloud_pub_sub, :streaming, layer, event]`.

  Emits `:start`, `:stop`, and `:exception` events as per `:telemetry.span/3` semantics.
  `fun` must return `{result, stop_metadata}`.

  `telemetry_metadata` is resolved once and merged into both the start metadata and the
  stop metadata returned by `fun`, under the `:extra` key.
  """
  @spec span(atom(), atom(), map(), (-> {term(), map()}), term()) :: term()
  def span(layer, event, start_metadata, fun, telemetry_metadata) do
    extra = resolve_extra(telemetry_metadata)
    enriched_start = maybe_put_extra(start_metadata, extra)

    :telemetry.span(@base ++ [layer, event], enriched_start, fn ->
      {result, stop_metadata} = fun.()
      {result, maybe_put_extra(stop_metadata, extra)}
    end)
  end

  # --- Private ---

  defp resolve_extra(nil), do: nil
  defp resolve_extra({m, f, a}), do: apply(m, f, a)
  defp resolve_extra(term), do: term

  defp maybe_put_extra(metadata, nil), do: metadata
  defp maybe_put_extra(metadata, extra), do: Map.put(metadata, :extra, extra)
end
