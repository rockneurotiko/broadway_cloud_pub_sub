defmodule BroadwayCloudPubSub.Streaming.LeaseManager do
  @moduledoc false

  # Pure-function module for lease extension, adaptive deadline computation,
  # timer scheduling, and stale pending-modack sweeping.
  #
  # Functions accept and return the StreamManager state struct. StreamManager
  # delegates to this module for lease management without mixing timer and
  # deadline logic into GenServer callback bodies.

  alias BroadwayCloudPubSub.Streaming.{AckBatcher, AckTimeDistribution, Telemetry}

  # Subtracted from the adaptive deadline when computing the lease extension interval.
  @grace_period_seconds 5

  # Minimum ack deadline enforced by the server for exactly-once subscriptions.
  @min_deadline_exactly_once_seconds 60

  # Stale pending receipt modacks (older than 60s) are nacked for fast redelivery.
  @receipt_modack_stale_ms 60_000

  # --- Deadline computation ---

  @doc """
  Returns the effective ack deadline in seconds, based on the adaptive p99
  percentile from recorded processing times. For exactly-once subscriptions,
  enforces the server's minimum of 60 seconds.
  """
  def effective_deadline(state) do
    adaptive = AckTimeDistribution.percentile(state.ack_time_dist, 0.99)

    if state.exactly_once_enabled,
      do: max(adaptive, @min_deadline_exactly_once_seconds),
      else: adaptive
  end

  # --- Lease extension ---

  @doc """
  Runs a lease extension cycle: partitions outstanding messages into valid and
  expired, emits telemetry, sends modack requests for valid messages, and
  schedules the next extension tick.
  """
  def do_extend_leases(state) do
    now = System.monotonic_time(:millisecond)
    deadline = effective_deadline(state)

    # Partition into still-valid and expired (past max_expiry — server will redeliver).
    {valid, expired} =
      Map.split_with(state.outstanding, fn {_id, info} -> info.max_expiry > now end)

    if map_size(expired) > 0 do
      emit_telemetry(:lease_expired, %{count: map_size(expired)}, state.config)
    end

    emit_telemetry(
      :extend_leases,
      %{count: map_size(valid), deadline: deadline},
      state.config
    )

    emit_telemetry(
      :pressure_snapshot,
      %{
        outstanding_count: map_size(valid),
        buffered_count: :queue.len(state.message_buffer),
        pending_demand: state.pending_demand
      },
      state.config
    )

    if map_size(valid) > 0 do
      AckBatcher.modack(state.ack_batcher, Map.keys(valid), deadline)
    end

    # Schedule next tick with jitter in [0.8, 0.9) to spread out concurrent StreamManagers.
    base_interval_ms = max(1_000, (deadline - @grace_period_seconds) * 1_000)
    jitter_factor = 0.8 + :rand.uniform() * 0.1
    timer = Process.send_after(self(), :extend_leases, round(base_interval_ms * jitter_factor))

    state
    |> Map.put(:outstanding, valid)
    |> Map.put(:lease_timer, timer)
    |> sweep_stale_pending_modacks()
  end

  # --- Timer management ---

  @doc """
  Schedules the initial lease extension timer based on the configured deadline.
  Cancels any existing timer first.
  """
  def schedule_lease_timer(state) do
    state = cancel_lease_timer(state)
    # Initial interval: (configured deadline - grace period) with jitter, minimum 1s.
    deadline_s = state.config.stream_ack_deadline_seconds
    base_ms = max(1_000, (deadline_s - @grace_period_seconds) * 1_000)
    jitter_factor = 0.8 + :rand.uniform() * 0.1
    interval_ms = round(base_ms * jitter_factor)
    timer = Process.send_after(self(), :extend_leases, interval_ms)
    %{state | lease_timer: timer}
  end

  @doc """
  Cancels the lease extension timer if one is active.
  """
  def cancel_lease_timer(%{lease_timer: nil} = state), do: state

  def cancel_lease_timer(%{lease_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | lease_timer: nil}
  end

  # --- Stale pending modack sweep ---

  @doc """
  Sweeps stale pending receipt modacks (older than 60s) and nacks them for
  fast redelivery. Used during the lease extension cycle.
  """
  def sweep_stale_pending_modacks(state) do
    now = System.monotonic_time(:millisecond)
    cutoff = now - @receipt_modack_stale_ms

    {stale, fresh} =
      Map.split_with(state.pending_receipt_modacks, fn {_ref, %{received_at: t}} ->
        t < cutoff
      end)

    if map_size(stale) > 0 do
      stale_ids = stale |> Map.values() |> Enum.flat_map(& &1.ack_ids)
      AckBatcher.modack(state.ack_batcher, stale_ids, 0)
      emit_telemetry(:receipt_modack_stale, %{count: length(stale_ids)}, state.config)
    end

    %{state | pending_receipt_modacks: fresh}
  end

  # --- Private: telemetry ---

  defp emit_telemetry(event, measurements, config) do
    metadata = %{
      name: config.broadway[:name],
      subscription: config.subscription
    }

    Telemetry.execute(
      :stream,
      event,
      measurements,
      metadata,
      Map.get(config, :telemetry_metadata)
    )
  end
end
