defmodule BI.DayTask do 
require Logger




#任务初试状态
def init_status() do
    %{}
end

#执行完任务后，返回状态数据
#-> {%{}, ""}
def do_task(status, task_type) do 
    case task_type do 
        :last_day -> 
            exe_last_day_task()
        :cur_day -> 
            exe_cur_day_task()
    end 
    {status, TaskState.do_complete}
end



# BI.DayTask.exe_cur_day_task
def exe_cur_day_task() do 
    BI.DayTask.Protocol.exe_cur_day_task(:any)

    # {from, to}= BI.Common.get_cur_day_date_range_string()
    # Logger.debug("exe_cur_day_task: from= #{inspect from}, to=#{inspect to}, start")
    # ops= [:download, :write_to_db, :write_to_es]
    # BI.FromToTask.exe_from_to_task(BI.FromToTask.Cur, from, to, ops)

    # #可以添加gg的策略
    # #ops= [:write_to_db, :write_to_es]
    # # BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, from, to, ops)
    # Logger.debug("exe_cur_day_task: from= #{inspect from}, to=#{inspect to}, finished")
end 



# BI.DayTask.exe_last_day_task
def exe_last_day_task() do 
    BI.DayTask.Protocol.exe_last_day_task(:any)

    # {from, to}= BI.Common.get_last_day_date_string()
    # Logger.debug("exe_last_day_task: from= #{inspect from}, to=#{inspect to}, start")
    # ops= [:download, :write_to_db, :write_to_es]
    # BI.FromToTask.exe_from_to_task(BI.FromToTask.Cur, from, to, ops)

    # #可以添加gg的策略
    # #ops= [:write_to_db, :write_to_es]
    # # BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, from, to, ops)
    # Logger.debug("exe_last_day_task: from= #{inspect from}, to=#{inspect to}, finished")
end 

def exe_shift_day_task(shift) do 
    BI.DayTask.Protocol.exe_shift_day_task(shift)
end 


end 