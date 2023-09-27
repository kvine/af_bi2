defmodule Game.TaskProcess do
  use GenServer
  require Logger

  # Game.TaskProcess.get_tasks
  def get_tasks() do 
      pid = Game.TaskProcess.get_pid()
      GenServer.call(pid, :query_tasks)
  end 

  # Game.TaskProcess.reload_tasks
  def reload_tasks() do 
      pid = Game.TaskProcess.get_pid()
      GenServer.cast(pid, {:reload})
  end 


  def get_pid() do
    case :erlang.whereis(__MODULE__) do
        :undefined ->
            nil
        pid ->
            pid
    end
  end

  def start_link(data, _opts \\ []) do
      GenServer.start_link(__MODULE__, data, name: __MODULE__)
  end

 
  def init(_data) do
      GenServer.cast(__MODULE__,{:init})
      {:ok, %{}}
  end

  def handle_cast({:init},_state) do 
      state = Game.TaskProcessHelper.init_state()
      
      {:noreply,state}
  end

  def handle_cast({:reload}, _state) do
    state = Game.TaskProcessHelper.init_state()
    {:noreply,state}
  end

  def handle_cast({:complete_task, task, task_index, init_stamp}, state) do
    state = Game.TaskProcessHelper.complete_task(task, task_index, init_stamp, state)
    {:noreply, state}
  end

  def handle_cast({:update_task, task_index, task}, state) do
    case Game.TaskProcessHelper.update_task(task, task_index, state) do
        {:ok, state} ->
            Logger.info("update success")
            {:noreply, state}
        {:error, reason} ->
            Logger.error("update failed reason #{inspect reason}")
            {:noreply, state}
    end
  end

  def handle_cast({:add_task, task}, state) do
    case Game.TaskProcessHelper.add_task(task, state) do
        {:ok, state} ->
            Logger.info("add task success")
            {:noreply, state}
        {:error, reason} ->
            Logger.error("add task failed reason #{inspect reason}")
            {:noreply, state}
    end
  end

  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  #pid = pid = Game.TaskProcess.get_pid()
  #datas = GenServer.call(pid, :query_tasks)
  #{task_index, task} = Enum.at(datas, 0)
  #new_task = Game.TaskProcess.init_task(task.config)
  #GenServer.cast(pid, {:update_task, task_index, new_task})
  #GenServer.cast(pid, {:add_task, new_task})
  #GenServer.cast(pid, {:reload})
  def handle_call(:query_tasks, _from, state) do
    indexs = for i <- 1..Enum.count(state.tasks) do i-1 end

    data = Enum.zip(indexs, state.tasks)
    {:reply, data, state}
  end

  def handle_call(_request, _from, state) do
      {:reply, :ok, state}
  end

  def handle_info({:timeout, timer, msg}, state) do
    if(is_atom(msg)) do
        case Game.TaskProcessHelper.msg_to_taskindex(msg) do
            {:ok, task_index} ->
                case Game.TaskProcessHelper.handle_task(state, task_index, timer) do
                    {:ok, state} ->
                        {:noreply, state}
                    {:error, reason} ->
                        Logger.error("TaskProcess: timeout failed reason #{inspect reason}")
                        {:noreply, state}
                end
            {:error, _} ->
                Logger.error("TaskProcess: timeout no deal msg #{inspect msg}")
                {:noreply, state}
        end
    else
        {:noreply, state}
    end
  end

  def handle_info(_msg, state) do 
      {:noreply, state}
  end

  def terminate(_reason, _state) do
    :ok
  end

  
end
