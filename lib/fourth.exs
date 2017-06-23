import Logger, only: [debug: 1]

defmodule Q do
  use GenStage

  def peek do
    GenStage.cast(__MODULE__, :peek)
  end

  def handle_cast(:peek, state) do
    debug "STATE: #{inspect state}"
    {:noreply, [], state}
  end

  def nq(jobs) do
    GenStage.call(__MODULE__, {:nq, jobs})
  end

  def handle_call({:nq, jobs}, _from, {q, pending_demand}) do
    debug "nqing jobs: #{inspect jobs}, #{inspect q}"

    # nq new jobs
    q = Enum.reduce(jobs, q, fn job, q -> :queue.in(job, q) end)

    # and dispatch pending demand
    {jobs, {q, pending_demand}} = dq_jobs(q, pending_demand, [])
    # instead of emitting [] as the jobs, we are now getting pending_demand number of jobs
    # and returning it

    {:reply, :ok, jobs, {q, pending_demand}}
  end


  def start_link do
    debug "starting q"
    # our state will now have to keep track of a pending demand
    GenStage.start_link(__MODULE__, {:queue.new, _pending_demand = 0}, name: __MODULE__)
  end

  def init(state) do
    # returning this specific tuple marks this process as a producer
    {:producer, state}
  end

  def handle_demand(demand, {q, pending_demand}) when demand > 0 do
    debug "handling demand: #{demand} q: #{inspect q}"
    # dequeue as many jobs from our queue as the demand
    {jobs, {q, pending_demand}} = dq_jobs(q, demand + pending_demand, [])
    # return those jobs and our new queue
    {:noreply, jobs, {q, pending_demand}}
  end

  # this is one of our return paths, since we pop elements and prepend them
  # to our accumulator, the acc will have the elements in the reverse order in which they were popped
  # so, to fix the order of the popped jobs, we need to reverse them
  def dq_jobs(q, 0, acc), do: {Enum.reverse(acc), {q, 0}}
  def dq_jobs(q, n, acc) when n > 0 do
    case :queue.out(q) do
      # :queue.out returns an {:empty, q} when our queue is empty
      # in which case we need to reverse our jobs and return from this function
      {:empty, _} -> {Enum.reverse(acc), {q, n}}
      # however, if we do have elements, we can recursively call ourselves
      # after getting the first element from the queue
      {{:value, job}, q} -> dq_jobs(q, n-1, [job | acc])
    end
  end

end


defmodule Worker do
  use GenStage

  def start_link() do
    debug "starting worker"
    GenStage.start_link(Worker, :ok)
  end

  def init(:ok) do
    # the first atom `:consumer` marks this stage as a consumer
    # we are also doing the subscription in the init, so if this process
    # crashes and is restarted the subscription happens
    # we are also setting the max_demand to 1 because we can only work on one user sync at a time
    # by default GenStage will set max_demand to 1000 which means it will request a 1000 jobs in one go from the producer
    # which is definitely not what we want
    {:consumer, :ok, subscribe_to: [{Q, max_demand: 1}] }
  end

  def handle_events(events, _from, state) do
    IO.inspect({"processing", self(), events})

    # do some heavy lifting
    Process.sleep(1000)

    # consumers don't return events
    {:noreply, [], state}
  end
end

defmodule MyApplication do
  def start() do
    import Supervisor.Spec, warn: false

    debug "setting up the supervisor"

    # Define workers and child supervisors to be supervised
    children = [
      worker(Q, []),
      worker(Worker, [], id: 1),
      worker(Worker, [], id: 2),
    ]

    # See http://elixir-lang.org/docs/stable/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Gsq.Supervisor]
    {:ok, _} = Supervisor.start_link(children, opts)

  end
end

MyApplication.start()
spawn(fn ->
  Enum.each 1..10, fn idx ->
    # nq a few jobs
    jobs = (idx * 10)..((idx+1) * 10)
    Q.nq(jobs |> Enum.to_list)
    Q.peek
    Process.sleep(:timer.seconds 10)
  end
end)

Process.sleep(:infinity)
