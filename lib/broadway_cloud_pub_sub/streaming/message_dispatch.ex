defmodule BroadwayCloudPubSub.Streaming.MessageDispatch do
  @moduledoc false

  # Pure-function module for message buffer management, demand-based flushing,
  # Broadway message construction, and outstanding ack_id tracking.
  #
  # Functions accept and return the StreamManager state struct (or relevant
  # fields). StreamManager delegates to this module for message dispatch
  # without mixing buffer logic into GenServer callback bodies.

  alias BroadwayCloudPubSub.MessageBuilder
  alias BroadwayCloudPubSub.Streaming.AckTimeDistribution

  # --- Buffer and demand ---

  @doc """
  Enqueues `messages` into the buffer, then flushes up to `pending_demand`
  messages to the producer process.
  """
  def deliver_messages(state, messages) do
    new_buffer = Enum.reduce(messages, state.message_buffer, &:queue.in(&1, &2))
    flush_demand(%{state | message_buffer: new_buffer})
  end

  @doc """
  Flushes up to `pending_demand` messages from the buffer to the producer.
  No-op when draining, demand is zero, or the buffer is empty.
  """
  def flush_demand(%{draining: true} = state), do: state
  def flush_demand(%{pending_demand: 0} = state), do: state

  def flush_demand(state) do
    if :queue.is_empty(state.message_buffer) do
      state
    else
      {remaining, demand_left, batch_reversed} =
        flush_demand_loop(state.message_buffer, state.pending_demand, [])

      send(state.producer_pid, {:stream_messages, Enum.reverse(batch_reversed)})
      %{state | message_buffer: remaining, pending_demand: demand_left}
    end
  end

  defp flush_demand_loop(queue, 0, acc), do: {queue, 0, acc}

  defp flush_demand_loop(queue, n, acc) do
    case :queue.out(queue) do
      {{:value, msg}, rest} -> flush_demand_loop(rest, n - 1, [msg | acc])
      {:empty, _} -> {queue, n, acc}
    end
  end

  # --- Outstanding tracking ---

  @doc """
  Adds ack_ids to the outstanding map with their received_at and max_expiry.
  """
  def add_to_outstanding(outstanding, ack_ids, received_at, max_extension_ms) do
    Enum.reduce(ack_ids, outstanding, fn ack_id, acc ->
      Map.put(acc, ack_id, %{received_at: received_at, max_expiry: received_at + max_extension_ms})
    end)
  end

  @doc """
  Records processing times for ack_ids in the adaptive p99 distribution, then
  removes them from outstanding. Shared by both ack and modack (nack) paths.
  """
  def record_and_remove_from_outstanding(state, ack_ids) do
    now = System.monotonic_time(:millisecond)

    ack_time_dist =
      Enum.reduce(ack_ids, state.ack_time_dist, fn ack_id, dist ->
        case Map.get(state.outstanding, ack_id) do
          %{received_at: received_at} ->
            duration_s = max(1, div(now - received_at, 1_000))
            AckTimeDistribution.record(dist, duration_s)

          nil ->
            dist
        end
      end)

    new_outstanding = Enum.reduce(ack_ids, state.outstanding, &Map.delete(&2, &1))
    %{state | outstanding: new_outstanding, ack_time_dist: ack_time_dist}
  end

  @doc """
  Extracts ack_ids from buffered Broadway messages.
  """
  def extract_buffered_ack_ids(message_buffer) do
    message_buffer
    |> :queue.to_list()
    |> Enum.map(fn %Broadway.Message{acknowledger: {_, _, %{ack_id: id}}} -> id end)
  end

  @doc """
  Splits broadway_messages into {succeeded_msgs, succeeded_ids} by removing
  messages whose ack_id is in failed_ids.
  """
  def partition_succeeded(broadway_messages, all_ack_ids, failed_ids) do
    failed_set = MapSet.new(failed_ids)

    {ok_msgs_reversed, ok_ids_reversed} =
      Enum.zip(broadway_messages, all_ack_ids)
      |> Enum.reduce({[], []}, fn {msg, id}, {msgs_acc, ids_acc} ->
        if MapSet.member?(failed_set, id) do
          {msgs_acc, ids_acc}
        else
          {[msg | msgs_acc], [id | ids_acc]}
        end
      end)

    {Enum.reverse(ok_msgs_reversed), Enum.reverse(ok_ids_reversed)}
  end

  # --- Message construction ---

  @doc """
  Builds a `Broadway.Message` from a decoded Pub/Sub ReceivedMessage and the
  current StreamManager state.
  """
  def build_broadway_message(
        %{ack_id: ack_id, message: pubsub_msg, delivery_attempt: delivery_attempt},
        state
      ) do
    ack_ref = state.config.ack_ref
    acknowledger = BroadwayCloudPubSub.Streaming.Acknowledger.builder(ack_ref).(ack_id)

    %Broadway.Message{
      data: pubsub_msg.data,
      metadata: build_metadata(pubsub_msg, delivery_attempt),
      acknowledger: acknowledger
    }
  end

  defp build_metadata(msg, delivery_attempt) do
    MessageBuilder.build_metadata(%{
      message_id: msg.message_id,
      ordering_key: msg.ordering_key,
      publish_time: to_datetime(msg.publish_time),
      delivery_attempt: delivery_attempt,
      attributes: Map.new(msg.attributes || [])
    })
  end

  defp to_datetime(nil), do: nil

  defp to_datetime(%{seconds: seconds, nanos: nanos}) do
    DateTime.from_unix!(seconds * 1_000_000_000 + nanos, :nanosecond)
  rescue
    _ -> nil
  end
end
