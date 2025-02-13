defmodule BroadwayCloudPubSub.PullWorker do
  @moduledoc false
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def stop(server) do
    GenServer.stop(server)
  end

  @impl true
  def init(opts) do
    state = %{
      client: opts.client,
      config: opts.config,
      retries: opts.max_retries,
      retry_codes: opts.retry_codes,
      retry_delay: opts.retry_delay,
      ack_builder: opts.ack_builder,
      demand: opts.demand,
      max_messages: opts.max_messages,
      metric_prefix: opts.metric_prefix,
      current_request: nil,
      data: [],
      status: nil,
      headers: nil,
      start_time: nil
    }

    send(self(), :pull_messages)
    {:ok, state}
  end

  @impl true
  def handle_info(:pull_messages, %{current_request: nil} = state) do
    # Do not override start_time, so when there are retries it will calculate all

    state =
      if state.start_time do
        state
      else
        state = %{state | start_time: System.monotonic_time()}
        emit_metric(state, :start)
        state
      end

    request = send_pull_request(state)

    {:noreply,
     %{
       state
       | current_request: request,
         data: [],
         status: nil,
         headers: nil
     }}
  end

  def handle_info(:pull_messages, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({ref, {:status, status}}, %{current_request: ref} = state) do
    # Just store the status, we'll use it when building the response
    {:noreply, Map.put(state, :status, status)}
  end

  def handle_info({ref, {:headers, headers}}, %{current_request: ref} = state) do
    # Just store the headers, we'll use them when building the response
    {:noreply, Map.put(state, :headers, headers)}
  end

  def handle_info({ref, {:data, data}}, %{current_request: ref} = state) do
    # Accumulate the data
    accumulated_data = [state.data | data]
    {:noreply, Map.put(state, :data, accumulated_data)}
  end

  def handle_info({ref, :done}, %{current_request: ref} = state) do
    handle_response_done(state)
  end

  def handle_info({ref, {:error, reason}}, %{current_request: ref} = state) do
    Logger.error("Unable to fetch events from Cloud Pub/Sub - reason: #{inspect(reason)}")

    emit_metric(state, :exception)

    send(state.client, {self(), []})
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, ref, _, _, _}, %{current_request: ref} = state) do
    {:stop, :normal, state}
  end

  defp emit_metric(state, :start) do
    # Emit start metric
    :telemetry.execute(
      state.metric_prefix ++ [:start],
      %{system_time: System.system_time(), monotonic_time: state.start_time},
      %{
        telemetry_span_context: :erlang.make_ref(),
        max_messages: state.max_messages,
        demand: state.demand,
        name: state.config.broadway[:name]
      }
    )
  end

  defp emit_metric(state, metric) do
    # Emit telemetry event
    end_time = System.monotonic_time()

    measurements = %{
      duration: end_time - state.start_time,
      monotonic_time: end_time
    }

    metadata = %{
      telemetry_span_context: :erlang.make_ref(),
      max_messages: state.max_messages,
      demand: state.demand,
      name: state.config.broadway[:name]
    }

    :telemetry.execute(
      state.metric_prefix ++ [metric],
      measurements,
      metadata
    )
  end

  defp handle_response_done(%{status: 200} = state) do
    # Decode response from accumulated data
    decoded_data =
      case Jason.decode(state.data) do
        {:ok, response} ->
          response

        error ->
          Logger.error("Failed to decode pubsub response: #{inspect(error)}")
          %{}
      end

    messages = handle_response(decoded_data, state.ack_builder)
    emit_metric(state, :stop)

    send(state.client, {self(), messages})

    # Stop the GenServer after receiving messages
    {:stop, :normal, state}
  end

  defp handle_response_done(%{status: status} = state) do
    case maybe_retry(status, state) do
      :ok ->
        delay = state.retry_delay
        Process.send_after(self(), :pull_messages, delay)

        new_state = %{
          state
          | current_request: nil,
            retries: state.retries - 1
        }

        {:noreply, new_state}

      {:error, reason} ->
        Logger.error("Unable to fetch events from Cloud Pub/Sub - reason: #{reason}")
        send(state.client, {self(), []})
        {:stop, :normal, state}
    end
  end

  defp maybe_retry(status, state) do
    if should_retry(status, state) do
      :ok
    else
      {:error, format_error(state)}
    end
  end

  defp should_retry(status, state) do
    state.retries > 0 and status in state.retry_codes
  end

  defp format_error(%{status: status, data: data} = state)
       when is_integer(status) and is_list(data) do
    url = url(state.config)
    data = to_string(data)

    """
    \nRequest to #{inspect(url)} failed with status #{inspect(status)}, got:
    #{inspect(data)}
    """
  end

  defp format_error(state) do
    url = url(state.config)
    inspect(%{url: url, state: state})
  end

  @impl true
  def terminate(_reason, %{current_request: ref} = state) when not is_nil(ref) do
    Finch.cancel_async_request(ref)

    emit_metric(state, :exception)

    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp send_pull_request(state) do
    max_messages = state.max_messages
    url = url(state.config)

    body = Jason.encode!(%{"maxMessages" => max_messages})
    headers = headers(state.config)

    req = Finch.build(:post, url, headers, body)

    Finch.async_request(req, state.config.finch, receive_timeout: state.config.receive_timeout)
  end

  defp handle_response(%{"receivedMessages" => received_messages}, ack_builder) do
    Enum.map(received_messages, fn msg ->
      pub_sub_msg_to_broadway_msg(msg, ack_builder)
    end)
  end

  defp handle_response(_, _ack_builder), do: []

  defp pub_sub_msg_to_broadway_msg(pub_sub_msg, ack_builder) do
    %{"ackId" => ack_id, "message" => message} = pub_sub_msg
    delivery_attempt = Map.get(pub_sub_msg, "deliveryAttempt")

    {data, metadata} =
      message
      |> decode_message()
      |> Map.pop("data")

    metadata = %{
      attributes: metadata["attributes"],
      deliveryAttempt: delivery_attempt,
      messageId: metadata["messageId"],
      orderingKey: metadata["orderingKey"],
      publishTime: parse_datetime(metadata["publishTime"])
    }

    %Broadway.Message{
      data: data,
      metadata: metadata,
      acknowledger: ack_builder.(ack_id)
    }
  end

  defp parse_datetime(nil), do: nil

  defp parse_datetime(str) when is_binary(str) do
    case DateTime.from_iso8601(str) do
      {:ok, dt, _} ->
        dt

      err ->
        raise "invalid datetime string: #{inspect(err)}"
    end
  end

  defp decode_message(%{"data" => encoded_data} = message) when is_binary(encoded_data) do
    %{message | "data" => Base.decode64!(encoded_data)}
  end

  defp decode_message(%{"data" => nil} = message), do: message
  defp decode_message(%{} = message) when not is_map_key(message, "data"), do: message

  defp headers(config) do
    token = get_token(config)
    [{"authorization", "Bearer #{token}"}, {"content-type", "application/json"}]
  end

  defp url(config) do
    sub = URI.encode(config.subscription)
    path = "/v1/" <> sub <> ":pull"
    config.base_url <> path
  end

  defp get_token(%{token_generator: {m, f, a}}) do
    {:ok, token} = apply(m, f, a)
    token
  end
end
