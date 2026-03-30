defmodule BroadwayCloudPubSub.Streaming.AckBatcher do
  @moduledoc false

  # GenServer that accumulates ack and modifyAckDeadline requests and flushes
  # them to UnaryRpcClient on a configurable timer or size threshold.
  #
  # ## Modack grouping
  #
  # ModifyAckDeadline requests carry a single deadline value for all ack IDs in
  # the request. We group modack requests by deadline value so that one unary RPC
  # is sent per unique deadline per flush cycle.
  #
  # ## Flush triggers
  #
  #   1. Timer fires (every ack_batch_interval_ms)
  #   2. Accumulated ack count reaches ack_batch_max_size
  #   3. Explicit `flush/1` call (used during graceful shutdown)
  #
  # ## Relationship to UnaryRpcClient
  #
  # AckBatcher and UnaryRpcClient are siblings under UnaryAckSupervisor. The
  # batcher looks up the RPC client by its registered name derived from the
  # Broadway pipeline name.

  use GenServer

  alias BroadwayCloudPubSub.Streaming.UnaryRpcClient

  defstruct [
    :rpc_client,
    :batch_interval_ms,
    :batch_max_size,
    :timer_ref,
    # Accumulated ack_ids waiting to be flushed.
    ack_ids: [],
    ack_count: 0,
    # Accumulated modacks: %{deadline_seconds => [ack_id]}
    modack_ids: %{}
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)

    if name do
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      GenServer.start_link(__MODULE__, opts)
    end
  end

  @doc """
  Queues ack_ids for acknowledgement. Fire-and-forget.
  """
  @spec ack(GenServer.server(), [String.t()]) :: :ok
  def ack(pid, ack_ids) when is_list(ack_ids) and ack_ids != [] do
    GenServer.cast(pid, {:ack, ack_ids})
  end

  def ack(_pid, []), do: :ok

  @doc """
  Queues ack_ids for a modifyAckDeadline request. Fire-and-forget.
  """
  @spec modack(GenServer.server(), [String.t()], non_neg_integer()) :: :ok
  def modack(pid, ack_ids, deadline_seconds) when is_list(ack_ids) and ack_ids != [] do
    GenServer.cast(pid, {:modack, ack_ids, deadline_seconds})
  end

  def modack(_pid, [], _deadline), do: :ok

  @doc """
  Flushes all pending batches synchronously. Used during graceful shutdown to
  ensure no acks are dropped before the process terminates.
  """
  @spec flush(GenServer.server()) :: :ok
  def flush(pid) do
    GenServer.call(pid, :flush, 15_000)
  end

  # --- GenServer callbacks ---

  @impl GenServer
  def init(opts) do
    config = Map.new(opts)

    state = %__MODULE__{
      rpc_client: config.rpc_client,
      batch_interval_ms: config.ack_batch_interval_ms,
      batch_max_size: config.ack_batch_max_size
    }

    {:ok, schedule_flush(state)}
  end

  @impl GenServer
  def handle_cast({:ack, ack_ids}, state) do
    new_ids = ack_ids ++ state.ack_ids
    new_count = state.ack_count + length(ack_ids)
    state = %{state | ack_ids: new_ids, ack_count: new_count}

    state =
      if new_count >= state.batch_max_size do
        # Size-triggered flush: reschedule the timer so periodic flushing
        # continues. Without rescheduling, timer_ref is left nil after
        # do_flush cancels it and no further periodic flushes ever occur.
        do_flush(state)
      else
        state
      end

    {:noreply, state}
  end

  def handle_cast({:modack, ack_ids, deadline_seconds}, state) do
    new_modack_ids =
      Map.update(state.modack_ids, deadline_seconds, ack_ids, &(ack_ids ++ &1))

    total_modack_count = new_modack_ids |> Map.values() |> Enum.map(&length/1) |> Enum.sum()
    state = %{state | modack_ids: new_modack_ids}

    state =
      if state.ack_count + total_modack_count >= state.batch_max_size do
        do_flush(state)
      else
        state
      end

    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:flush, _from, state) do
    state = do_flush(state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info(:flush_timer, state) do
    state = do_flush(state)
    {:noreply, schedule_flush(state)}
  end

  defp do_flush(state) do
    state = cancel_timer(state)

    # Check if RPC client is available. If not, keep all state and retry on the
    # next timer tick to avoid a noproc crash while UnaryRpcClient is restarting.
    case GenServer.whereis(state.rpc_client) do
      nil ->
        :telemetry.execute(
          [:broadway_cloud_pub_sub, :stream, :flush_deferred],
          %{ack_count: state.ack_count, modack_groups: map_size(state.modack_ids)},
          %{}
        )

        schedule_flush(state)

      _pid ->
        # Each step runs independently — a failure in flush_acks does not
        # prevent flush_modacks from running.
        state
        |> flush_acks()
        |> flush_modacks()
        |> schedule_flush()
    end
  end

  defp flush_acks(%{ack_count: 0} = state), do: state

  defp flush_acks(state) do
    case UnaryRpcClient.acknowledge(state.rpc_client, state.ack_ids) do
      {:ok, []} ->
        %{state | ack_ids: [], ack_count: 0}

      {:ok, remaining_ids} ->
        # Partial success — retain only the failed ack_ids for next flush
        %{state | ack_ids: remaining_ids, ack_count: length(remaining_ids)}

      {:error, {_rpc_error, transient_ids}} when is_list(transient_ids) ->
        # Per-ack-ID partial failure: permanent ids already dropped by
        # UnaryRpcClient. Retain only the transient ids for retry.
        %{state | ack_ids: transient_ids, ack_count: length(transient_ids)}

      {:error, _reason} ->
        # Total failure — retain all ack_ids
        state
    end
  end

  defp flush_modacks(%{modack_ids: modacks} = state) when map_size(modacks) == 0, do: state

  defp flush_modacks(state) do
    # Each deadline group is attempted independently — failure in one group does
    # not prevent the others from being flushed.
    remaining_modacks =
      Enum.reduce(state.modack_ids, %{}, fn {deadline, ids}, remaining ->
        case UnaryRpcClient.modify_ack_deadline(state.rpc_client, ids, deadline) do
          {:ok, []} ->
            remaining

          {:ok, remaining_ids} ->
            # Partial success — retain only the failed ids for this deadline
            Map.put(remaining, deadline, remaining_ids)

          {:error, {_rpc_error, transient_ids}} when is_list(transient_ids) ->
            # Per-ack-ID partial failure: retain only transient ids.
            if transient_ids == [] do
              remaining
            else
              Map.put(remaining, deadline, transient_ids)
            end

          {:error, _reason} ->
            # Total failure for this deadline — retain all ids
            Map.put(remaining, deadline, ids)
        end
      end)

    %{state | modack_ids: remaining_modacks}
  end

  defp schedule_flush(state) do
    state = cancel_timer(state)
    ref = Process.send_after(self(), :flush_timer, state.batch_interval_ms)
    %{state | timer_ref: ref}
  end

  defp cancel_timer(%{timer_ref: nil} = state), do: state

  defp cancel_timer(%{timer_ref: ref} = state) do
    Process.cancel_timer(ref)
    # Drain any :flush_timer message that was already delivered to the mailbox
    # before cancel_timer ran, to prevent an extra flush after the cancel.
    receive do
      :flush_timer -> :ok
    after
      0 -> :ok
    end

    %{state | timer_ref: nil}
  end
end
