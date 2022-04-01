defmodule Game.TaskProcessHelper do
    require Logger
    def confings_old() do 
        [
        config(:cur_week,"1","11:05_utc8", BI.WeekTask), #周1上午10点执行本周的数据任务(周一上午10点一周的数据不完整，需要再第二天再执行一次)
        config(:last_week,"2","10:00_utc8", BI.WeekTask), #周2上午10点执行上周的数据任务
        config(:before_yesterday, "everyday", "17:00_utc8", Task.QueryRemoveWorker), #每天下午5点执行uninstall数据
        #每日任务暂不开启
        # config(:last_day,"everyday","18:00_utc8", BI.DayTask),#每日下午6点执行昨日的数据任务
        #test
        # config(:last_week,"6","16:22_utc8", BI.WeekTask), #周1晚上10点执行上周的数据任务
        
    ]
    end 

    def confings() do 
        BI.Task.Protocol.task_process_configs(:any)
    end 

    def config(task_type, week_day, day_time, task_module) do 
        %{
            task_type: task_type, 
            week_day: week_day, 
            day_time: day_time,
            task_module: task_module,                 #具体任务模块名称defmodule
            task_status: task_module.init_status(),    #任务状态数据%{}
        }
    end 

    # Game.TaskProcess.init_tasks(Game.TaskProcess.confings())
    def init_tasks(confings) do 
        if BI.Config.is_test() do 
            #如果处于测试中不初始化任务
            []
        else 
            for i <- confings do 
                init_task(i)
            end 
        end 
    end 
    

    def get_day_time_mills(day_time) do 
        [h_m, utc_span]= String.split(day_time, "_")
        [h,m]= String.split(h_m,":")
        [utc, span]= String.split(utc_span,"utc")
        Logger.info("span=#{inspect span}")
        1000*( String.to_integer(h) * 3600 + String.to_integer(m)* 60 - String.to_integer(span) * 3600)
    end 

    def get_start_time_mills(week_day, day_time) do
        datetime= Timex.now
        cur_week_day= Timex.weekday(datetime)
        target_week_day= if week_day == "everyday" do 
            cur_week_day
        else 
            String.to_integer(week_day)
        end 
        #本周开始的时间
        begin_week_datetime= Timex.beginning_of_week(datetime)
        #本周的计划任务时间
        task_start_time_mills= Timex.to_unix(begin_week_datetime) * 1000 + (target_week_day - 1 ) * BI.Global.one_day_mills() +  get_day_time_mills(day_time)
        #如果当前的时间已经超过了计划的时间 ，需要等下一周或者是下一天
        reserve_time_mills=60*1000#10* 60* 1000 #60 * 10 * 1000
        now=  Timex.to_unix(Timex.now) * 1000
        if now  + reserve_time_mills >= task_start_time_mills  do 
            #下一周或下一天
            if week_day == "everyday" do 
                task_start_time_mills + BI.Global.one_day_mills()
            else 
                task_start_time_mills + BI.Global.one_day_mills() * 7
            end 
        else 
            task_start_time_mills
        end 
    end 


    def init_task(config) do 
        %{
            task_type: _task_type, 
            week_day: week_day, 
            day_time: day_time
        }= config
        start_time_mills= get_start_time_mills(week_day, day_time)
        %{
            create_time: Time.Util.time_string(), 
            start_time: Time.Util.time_string(start_time_mills),
            end_time: "nil",
            state: "undo",
            start_time_mills: start_time_mills,
            config: config,
        }
    end 

    @doc """
        对任务列表中的每个任务创建一个定时器
    """
    #->[]
    def create_tasks_timer(tasks, index \\ 0, acc_timer \\ []) do
        if(index < Enum.count(tasks)) do
            task = Enum.at(tasks, index)
            #记录db 
            DB.TaskStatus.Entity.add_task(task)

            span = task.start_time_mills - Time.Util.curr_mills()
            timer = Time.Util.start_timer(span, taskindex_to_msg(index))

            create_tasks_timer(tasks, index+1, [timer | acc_timer])
        else
            Enum.reverse(acc_timer)
        end
    end

  @doc """
    把任务索引 转换成 原子消息
  """
  #-> atom
  def taskindex_to_msg(index) do
      str = "task_" <> Integer.to_string(index)
      String.to_atom(str)
  end

  @doc """
    把原子消息 转换成 任务索引
  """
  #->{:ok, interger} | {:error, reason}
  def msg_to_taskindex(msg) do
      str = Atom.to_string(msg)

      case String.split(str, "_") do
          ["task", s_index] ->
            {:ok, String.to_integer(s_index)}
          _ ->
            {:error, :format_err}
      end
  end

  @doc """
    初始化
  """
  def init_state() do
      # 按照配置文件生成任务
      tasks= init_tasks(confings())

      #设定定时器
      timers = create_tasks_timer(tasks)

      #根据具体任务写数据库
      Logger.info("TaskProcess: init tasks=#{inspect tasks}")

      state= %{tasks: tasks, timers: timers, init_stamp: Time.Util.time_string()}

      state
  end

  @doc """
    处理定时器任务
  """
  #-> {:ok, %{}}, {:error, reason}
  def handle_task(state, task_index, timer) do
    %{tasks: tasks, timers: timers} = state
    if(task_index < Enum.count(tasks)) do
        if(timer != Enum.at(timers, task_index)) do
            {:error, :invalid_timer}
        else
            task = Enum.at(tasks, task_index)
            task = %{task | state: TaskState.doing}
            #根据具体任务写数据库
            spawn(fn -> 
                doing_task(Game.TaskProcess.get_pid(), task, task_index, state.init_stamp)
            end)
            
            Time.Util.cancle_timer(timer) 

            tasks = List.replace_at(tasks, task_index, task)
            state = %{state | tasks: tasks}
            {:ok, state}
        end
    else
        {:error, :invalid_index}
    end
  end


  def doing_task(pid, task, task_index, init_stamp) do
      DB.TaskStatus.Entity.update_task_status(:doing, task)

      %{task_module: task_module, task_status: task_status, task_type: task_type} = task.config

      {new_status, do_result} = task_module.do_task(task_status, task_type)

    #   new_status= task_status
    #   do_result= TaskState.do_complete

      Logger.info("doing_task, config=#{inspect task.config}")

      config = %{task.config | task_status: new_status}
      task = %{task | end_time: Time.Util.curr_mills(), state: do_result, config: config}

      GenServer.cast(pid, {:complete_task, task, task_index, init_stamp})
  end

  def complete_task(task, task_index, init_stamp, state) do
    Logger.info("complete task #{inspect task_index}")
    DB.TaskStatus.Entity.update_task_status(:complete, task)

    if(state.init_stamp == init_stamp) do
        #更新任务时间，状态
        new_task = init_task(task.config)

        #记录db 
        DB.TaskStatus.Entity.add_task(new_task)
        Logger.info("record new task, new_task=#{inspect new_task}")
        #创建任务定时器, 开始下一任务
        span = new_task.start_time_mills - Time.Util.curr_mills()
        new_timer = Time.Util.start_timer(span, taskindex_to_msg(task_index))

        %{tasks: tasks, timers: timers} = state

        timers = List.replace_at(timers, task_index, new_timer)

        tasks = List.replace_at(tasks, task_index, new_task)

        %{state | tasks: tasks, timers: timers}
    else
        Logger.error("init stamp is not same")
        state
    end
  end

  @doc """
    更新任务
  """
  #-> {:ok, state}, {:error, reason}
  def update_task(new_task, task_index, state) do
    %{tasks: tasks, timers: timers} = state
    if(task_index < Enum.count(tasks)) do
        timer = Enum.at(timers, task_index)
        Time.Util.cancle_timer(timer)

        #创建任务定时器
        span = new_task.start_time_mills - Time.Util.curr_mills()
        if(span <= 0) do
            {:error, :invalid_start_time}
        else
            new_timer = Time.Util.start_timer(span, taskindex_to_msg(task_index))

            timers = List.replace_at(timers, task_index, new_timer)

            tasks = List.replace_at(tasks, task_index, new_task)

            {:ok, %{state | tasks: tasks, timers: timers}}
        end
    else
        {:error, :invalid_index}
    end
  end

  #-> {:ok, state}, {:error, reason}
  def add_task(new_task, state) do
    %{tasks: tasks, timers: timers} = state

    span = new_task.start_time_mills - Time.Util.curr_mills()
    if(span <= 0) do
        {:error, :invalid_start_time}
    else
        task_index = Enum.count(tasks)

        new_timer = Time.Util.start_timer(span, taskindex_to_msg(task_index))

        rev_timers = Enum.reverse(timers)
        rev_tasks = Enum.reverse(tasks)

        timers = Enum.reverse([new_timer | rev_timers])
        tasks = Enum.reverse([new_task | rev_tasks])

        {:ok, %{state | tasks: tasks, timers: timers}}
    end
  end
end