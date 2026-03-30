defmodule BroadwayCloudPubSub.Streaming.UnaryAckSupervisor do
  @moduledoc false

  # Supervisor that owns the AckBatcher and UnaryRpcClient for a single Broadway
  # Streaming.Producer pipeline.
  #
  # Uses :one_for_one so each child restarts independently. AckBatcher accumulates
  # pending ack_ids in its state; restarting it when UnaryRpcClient crashes would
  # permanently lose those buffered acks (messages would be redelivered by the
  # server after deadline expiry, causing duplicate processing).
  #
  # AckBatcher references UnaryRpcClient by its registered name derived from the
  # Broadway pipeline name, so it survives UnaryRpcClient restarts automatically.
  # Any flush attempted while UnaryRpcClient is down is deferred to the next
  # timer tick.
  #
  # Started by prepare_for_start/2 as a Broadway supervisor child before
  # StreamManager, guaranteeing the batcher and RPC client are available when
  # StreamManager first processes ack requests.

  use Supervisor

  alias BroadwayCloudPubSub.Streaming.{AckBatcher, UnaryRpcClient}

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)

    if name do
      Supervisor.start_link(__MODULE__, opts, name: name)
    else
      Supervisor.start_link(__MODULE__, opts)
    end
  end

  @impl Supervisor
  def init(opts) do
    broadway_name = Keyword.fetch!(opts, :broadway_name)
    config = Keyword.fetch!(opts, :config)

    rpc_client_name = Module.concat(broadway_name, UnaryRpcClient)
    batcher_name = Module.concat(broadway_name, AckBatcher)

    rpc_client_opts =
      config
      |> Keyword.put(:name, rpc_client_name)

    batcher_opts =
      config
      |> Keyword.put(:name, batcher_name)
      |> Keyword.put(:rpc_client, rpc_client_name)

    children = [
      %{
        id: UnaryRpcClient,
        start: {UnaryRpcClient, :start_link, [rpc_client_opts]},
        restart: :permanent
      },
      %{
        id: AckBatcher,
        start: {AckBatcher, :start_link, [batcher_opts]},
        restart: :permanent,
        # AckBatcher.flush/1 uses GenServer.call with a 15_000ms timeout.
        # The OTP default shutdown of 5_000ms would kill AckBatcher before
        # an in-progress flush can complete, dropping buffered acks.
        shutdown: 20_000
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
