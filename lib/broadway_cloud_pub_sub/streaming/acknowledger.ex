defmodule BroadwayCloudPubSub.Streaming.Acknowledger do
  @moduledoc false

  # Broadway.Acknowledger for StreamingPull.
  # Delegates ack/nack/modifyAckDeadline to the StreamManager via
  # gRPC requests on the bidirectional stream.

  alias Broadway.Acknowledger
  alias BroadwayCloudPubSub.Streaming.Options

  @behaviour Acknowledger

  @typedoc "Acknowledgement data for a Broadway.Message."
  @type ack_data :: %{
          :ack_id => String.t(),
          optional(:on_failure) => ack_option(),
          optional(:on_success) => ack_option()
        }

  @typedoc "An acknowledgement action."
  @type ack_option :: :ack | :noop | {:nack, 0..600}

  @type ack_ref :: term()

  # The maximum number of ackIds per request.
  # API limit is 524288 bytes (512KiB); ackIds have max 184 bytes each.
  # 524288/184 ~= 2849 → use 2500 with headroom.
  @max_ack_ids_per_request 2_500

  @doc """
  Returns an acknowledger tuple builder function to attach to Broadway.Messages.
  The returned function takes an `ack_id` and returns an acknowledger tuple.
  """
  @spec builder(ack_ref()) :: (String.t() -> {__MODULE__, ack_ref(), ack_data()})
  def builder(ack_ref) do
    &{__MODULE__, ack_ref, %{ack_id: &1}}
  end

  @impl Acknowledger
  def ack(ack_ref, successful, failed) do
    {manager_pid, config} = :persistent_term.get(ack_ref)

    success_actions = group_actions_ack_ids(successful, :on_success, config)
    failure_actions = group_actions_ack_ids(failed, :on_failure, config)

    success_actions
    |> Map.merge(failure_actions, fn _, a, b -> a ++ b end)
    |> dispatch_acks(manager_pid)

    :ok
  end

  @impl Acknowledger
  def configure(_ack_ref, ack_data, options) do
    opts = NimbleOptions.validate!(options, Options.acknowledger_definition())
    ack_data = Map.merge(ack_data, Map.new(opts))
    {:ok, ack_data}
  end

  # --- Private ---

  defp group_actions_ack_ids(messages, key, config) do
    Enum.group_by(messages, &action_for(&1, key, config), &extract_ack_id/1)
  end

  defp action_for(%{acknowledger: {_, _, ack_data}}, key, config) do
    Map.get_lazy(ack_data, key, fn -> default_action(key, config) end)
  end

  defp default_action(:on_success, %{on_success: action}), do: action
  defp default_action(:on_failure, %{on_failure: action}), do: action

  defp extract_ack_id(%{acknowledger: {_, _, %{ack_id: ack_id}}}), do: ack_id

  defp dispatch_acks(actions_and_ids, manager_pid) do
    Enum.each(actions_and_ids, fn {action, ack_ids} ->
      ack_ids
      |> Enum.chunk_every(@max_ack_ids_per_request)
      |> Enum.each(&apply_action(action, &1, manager_pid))
    end)
  end

  defp apply_action(:noop, _ack_ids, _manager_pid), do: :ok

  defp apply_action(:ack, ack_ids, manager_pid) do
    BroadwayCloudPubSub.Streaming.StreamManager.acknowledge(manager_pid, ack_ids)
  end

  defp apply_action({:nack, deadline}, ack_ids, manager_pid) do
    BroadwayCloudPubSub.Streaming.StreamManager.modify_deadline(manager_pid, ack_ids, deadline)
  end
end
