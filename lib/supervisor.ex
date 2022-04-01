defmodule BI.Supervisor do
	use Supervisor
	require  Logger
	
	def start_link() do
		Supervisor.start_link(__MODULE__, nil, [name: __MODULE__])
	end
 
	def init(nil) do
		task_process_spec = worker(Game.TaskProcess, [%{}], restart: :permanent)

		children=[
			task_process_spec
		]
		supervise(children, strategy: :one_for_one)
	end

end