defmodule BroadwayCloudPubSub.Streaming.AckBatcher do
  @moduledoc false

  # GenServer that accumulates ack and modifyAckDeadline requests and flushes
  # them to UnaryRpcClient on a timer, size threshold, or explicit flush.
  # Modacks are grouped by deadline value: one unary RPC per unique deadline per flush.

  use GenServer

  alias BroadwayCloudPubSub.Streaming.{Options, Telemetry, UnaryRpcClient}

  @max_modack_attempts 3

  defstruct [
    :rpc_client,
    :broadway_name,
    :subscription,
    :telemetry_metadata,
    :batch_interval_ms,
    :batch_max_size,
    :timer_ref,
    # nil = no deadline. Set to 600_000ms when exactly-once delivery is enabled.
    retry_deadline_ms: nil,
    ack_ids: [],
    ack_count: 0,
    # %{deadline_seconds => [ack_id]}
    modack_ids: %{},
    # Monotonic ms of when each ack_id was first queued; cleaned up on success or expiry.
    ack_first_queued: %{},
    modack_first_queued: %{},
    # Per-ack-ID attempt count; cleaned up each flush via sweep over remaining_modacks.
    modack_attempts: %{}
  ]

  @all_keys [
    :subscription,
    :ack_batch_interval_ms,
    :ack_batch_max_size,
    :retry_deadline_ms,
    :broadway_name,
    :telemetry_metadata,
    :rpc_client
  ]

  @required_keys [
    :subscription,
    :ack_batch_interval_ms,
    :ack_batch_max_size,
    :broadway_name,
    :rpc_client
  ]

  @doc false
  @spec child_opts(keyword()) :: keyword()
  def child_opts(opts) do
    Options.validate_child_opts(opts, @all_keys, @required_keys)
  end

  @doc """
  Updates the retry deadline at runtime. Called by StreamManager when it detects
  a change in exactly-once delivery status from subscription_properties.
  """
  @spec update_retry_deadline(GenServer.server(), pos_integer()) :: :ok
  def update_retry_deadline(pid, retry_deadline_ms) do
    GenServer.cast(pid, {:update_retry_deadline, retry_deadline_ms})
  end

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
  Sends a receipt modack for exactly-once delivery. Spawns a Task that calls
  UnaryRpcClient.modify_ack_deadline/3 and sends the result to `reply_to`
  as `{:receipt_modack_result, ref, result}`.

  Unlike modack/3, this is NOT batched — it runs immediately because
  exactly-once delivery requires confirmation before dispatching messages.
  """
  @spec receipt_modack(GenServer.server(), reference(), pid(), [String.t()], non_neg_integer()) ::
          :ok
  def receipt_modack(pid, ref, reply_to, ack_ids, deadline_seconds) do
    GenServer.cast(pid, {:receipt_modack, ref, reply_to, ack_ids, deadline_seconds})
  end

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
      broadway_name: config[:broadway_name],
      subscription: config[:subscription],
      telemetry_metadata: config[:telemetry_metadata],
      batch_interval_ms: config.ack_batch_interval_ms,
      batch_max_size: config.ack_batch_max_size,
      retry_deadline_ms: config[:retry_deadline_ms]
    }

    {:ok, schedule_flush(state)}
  end

  @impl GenServer
  def handle_cast({:ack, ack_ids}, state) do
    now = System.monotonic_time(:millisecond)
    new_ids = ack_ids ++ state.ack_ids
    new_count = state.ack_count + length(ack_ids)
    # put_new: don't reset timestamp if this ack_id is already being retried
    new_ts = Enum.reduce(ack_ids, state.ack_first_queued, &Map.put_new(&2, &1, now))
    state = %{state | ack_ids: new_ids, ack_count: new_count, ack_first_queued: new_ts}

    state =
      if new_count >= state.batch_max_size do
        do_flush(state)
      else
        state
      end

    {:noreply, state}
  end

  def handle_cast({:modack, ack_ids, deadline_seconds}, state) do
    now = System.monotonic_time(:millisecond)

    new_modack_ids =
      Map.update(state.modack_ids, deadline_seconds, ack_ids, &(ack_ids ++ &1))

    total_modack_count = new_modack_ids |> Map.values() |> Enum.map(&length/1) |> Enum.sum()
    # put_new: don't reset timestamp or attempt count for already-tracked ids
    new_ts = Enum.reduce(ack_ids, state.modack_first_queued, &Map.put_new(&2, &1, now))
    new_attempts = Enum.reduce(ack_ids, state.modack_attempts, &Map.put_new(&2, &1, 0))

    state = %{
      state
      | modack_ids: new_modack_ids,
        modack_first_queued: new_ts,
        modack_attempts: new_attempts
    }

    state =
      if state.ack_count + total_modack_count >= state.batch_max_size do
        do_flush(state)
      else
        state
      end

    {:noreply, state}
  end

  def handle_cast({:update_retry_deadline, retry_deadline_ms}, state) do
    {:noreply, %{state | retry_deadline_ms: retry_deadline_ms}}
  end

  # Receipt modack for exactly-once delivery. Spawns a Task that calls
  # UnaryRpcClient directly (bypassing batching) and sends the result back
  # to the caller. The Task is fire-and-forget from AckBatcher's perspective.
  def handle_cast({:receipt_modack, ref, reply_to, ack_ids, deadline_seconds}, state) do
    rpc_client = state.rpc_client

    Task.start(fn ->
      result = UnaryRpcClient.modify_ack_deadline(rpc_client, ack_ids, deadline_seconds)
      send(reply_to, {:receipt_modack_result, ref, result})
    end)

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
        emit_telemetry(
          :flush_deferred,
          %{ack_count: state.ack_count, modack_groups: map_size(state.modack_ids)},
          state
        )

        schedule_flush(state)

      _pid ->
        # Each step runs independently; a flush_acks failure does not block flush_modacks.
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
        %{state | ack_ids: [], ack_count: 0, ack_first_queued: %{}}

      {:ok, remaining_ids} ->
        state |> put_retained_acks(remaining_ids) |> expire_stale_acks()

      {:error, {_rpc_error, transient_ids}} when is_list(transient_ids) ->
        # Permanent ids already dropped by UnaryRpcClient; retain only transient.
        state |> put_retained_acks(transient_ids) |> expire_stale_acks()

      {:error, _reason} ->
        expire_stale_acks(state)
    end
  end

  defp flush_modacks(%{modack_ids: modacks} = state) when map_size(modacks) == 0, do: state

  defp flush_modacks(state) do
    all_ids = state.modack_ids |> Map.values() |> List.flatten()

    # Increment attempt count for all ids about to be flushed.
    attempts =
      Enum.reduce(all_ids, state.modack_attempts, fn id, acc ->
        Map.update(acc, id, 1, &(&1 + 1))
      end)

    state = %{state | modack_attempts: attempts}

    # Each deadline group is attempted independently.
    remaining_modacks =
      Enum.reduce(state.modack_ids, %{}, fn {deadline, ids}, remaining ->
        case UnaryRpcClient.modify_ack_deadline(state.rpc_client, ids, deadline) do
          {:ok, []} ->
            remaining

          {:ok, remaining_ids} ->
            keep = apply_modack_retry_limit(remaining_ids, state.modack_attempts, state)
            if keep == [], do: remaining, else: Map.put(remaining, deadline, keep)

          {:error, {_rpc_error, transient_ids}} when is_list(transient_ids) ->
            keep = apply_modack_retry_limit(transient_ids, state.modack_attempts, state)
            if keep == [], do: remaining, else: Map.put(remaining, deadline, keep)

          {:error, _reason} ->
            keep = apply_modack_retry_limit(ids, state.modack_attempts, state)
            if keep == [], do: remaining, else: Map.put(remaining, deadline, keep)
        end
      end)

    # Cleanup sweep: bound tracking maps to currently-pending ids only.
    still_pending = remaining_modacks |> Map.values() |> List.flatten() |> MapSet.new()

    clean_attempts =
      Map.filter(state.modack_attempts, fn {id, _} -> MapSet.member?(still_pending, id) end)

    clean_ts =
      Map.filter(state.modack_first_queued, fn {id, _} -> MapSet.member?(still_pending, id) end)

    state = %{
      state
      | modack_ids: remaining_modacks,
        modack_attempts: clean_attempts,
        modack_first_queued: clean_ts
    }

    expire_stale_modacks(state)
  end

  # Drops modack ids that have reached the maximum attempt count and emits telemetry.
  defp apply_modack_retry_limit(ids, attempts, state) do
    {keep, drop} =
      Enum.split_with(ids, fn id -> Map.get(attempts, id, 0) < @max_modack_attempts end)

    if drop != [] do
      emit_telemetry(:modack_retry_exhausted, %{count: length(drop)}, state)
    end

    keep
  end

  # Replaces the pending ack_ids with the given retained set and cleans up
  # ack_first_queued to contain only the retained ids.
  defp put_retained_acks(state, retained_ids) do
    retained_set = MapSet.new(retained_ids)

    clean_ts =
      Map.filter(state.ack_first_queued, fn {id, _} -> MapSet.member?(retained_set, id) end)

    %{state | ack_ids: retained_ids, ack_count: length(retained_ids), ack_first_queued: clean_ts}
  end

  defp expire_stale_acks(%{retry_deadline_ms: nil} = state), do: state

  defp expire_stale_acks(state) do
    now = System.monotonic_time(:millisecond)

    {live, expired} =
      Enum.split_with(state.ack_ids, fn id ->
        case Map.get(state.ack_first_queued, id) do
          nil -> true
          ts -> now - ts < state.retry_deadline_ms
        end
      end)

    if expired != [] do
      emit_telemetry(:ack_retry_expired, %{count: length(expired)}, state)
    end

    clean_ts = Map.drop(state.ack_first_queued, expired)
    %{state | ack_ids: live, ack_count: length(live), ack_first_queued: clean_ts}
  end

  defp expire_stale_modacks(%{retry_deadline_ms: nil} = state), do: state

  defp expire_stale_modacks(state) do
    now = System.monotonic_time(:millisecond)

    {remaining_modacks, expired_count} =
      Enum.reduce(state.modack_ids, {%{}, 0}, fn {deadline, ids}, {acc, dropped} ->
        {live, expired} =
          Enum.split_with(ids, fn id ->
            case Map.get(state.modack_first_queued, id) do
              nil -> true
              ts -> now - ts < state.retry_deadline_ms
            end
          end)

        acc = if live == [], do: acc, else: Map.put(acc, deadline, live)
        {acc, dropped + length(expired)}
      end)

    if expired_count > 0 do
      emit_telemetry(:modack_retry_expired, %{count: expired_count}, state)
    end

    still_pending = remaining_modacks |> Map.values() |> List.flatten() |> MapSet.new()

    clean_ts =
      Map.filter(state.modack_first_queued, fn {id, _} -> MapSet.member?(still_pending, id) end)

    clean_attempts =
      Map.filter(state.modack_attempts, fn {id, _} -> MapSet.member?(still_pending, id) end)

    %{
      state
      | modack_ids: remaining_modacks,
        modack_first_queued: clean_ts,
        modack_attempts: clean_attempts
    }
  end

  defp schedule_flush(state) do
    state = cancel_timer(state)
    ref = Process.send_after(self(), :flush_timer, state.batch_interval_ms)
    %{state | timer_ref: ref}
  end

  defp cancel_timer(%{timer_ref: nil} = state), do: state

  defp cancel_timer(%{timer_ref: ref} = state) do
    Process.cancel_timer(ref)
    # Drain any :flush_timer already in the mailbox to prevent a double flush.
    receive do
      :flush_timer -> :ok
    after
      0 -> :ok
    end

    %{state | timer_ref: nil}
  end

  defp emit_telemetry(event, measurements, state) do
    metadata = %{
      name: state.broadway_name,
      subscription: state.subscription
    }

    Telemetry.execute(:ack_batcher, event, measurements, metadata, state.telemetry_metadata)
  end
end
