defmodule BI.WeekTask do 
require Logger

#任务初试状态
def init_status() do
    %{}
end

#执行完任务后，返回状态数据
#-> {%{}, ""}
def do_task(status, task_type) do 
    case task_type do 
        :last_week -> 
            exe_last_week_task()
        :cur_week -> 
            exe_cur_week_task()
    end 
    {status, TaskState.do_complete}
end



# BI.WeekTask.exe_cur_week_task
def exe_cur_week_task() do 
    BI.WeekTask.Protocol.exe_cur_week_task(:any)
    # {from, to}= BI.Common.get_cur_week_date_range_string()
    # Logger.debug("exe_cur_week_task: from= #{inspect from}, to=#{inspect to}, start")
    
    # ops= [:download, :write_to_db, :write_to_es]
    # BI.FromToTask.exe_from_to_task(BI.FromToTask.Cur, from, to, ops)
    
    # #可以添加gg的策略
    # #ops= [:write_to_db, :write_to_es]
    # # BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, from, to, ops)
    # Logger.debug("exe_cur_week_task: from= #{inspect from}, to=#{inspect to}, finished")
end 



# BI.WeekTask.exe_last_week_task
def exe_last_week_task() do 
    BI.WeekTask.Protocol.exe_last_week_task(:any)

    # {from, to}= BI.Common.get_last_week_date_string()
    # Logger.debug("exe_last_week_task: from= #{inspect from}, to=#{inspect to}, start")

    # ops= [:download, :write_to_db, :write_to_es]
    # BI.FromToTask.exe_from_to_task(BI.FromToTask.Cur, from, to, ops)
    
    # #可以添加gg的策略
    # #ops= [:write_to_db, :write_to_es]
    # # BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, from, to, ops)
    # Logger.debug("exe_last_week_task: from= #{inspect from}, to=#{inspect to}, finished")
end 


def exe_shift_week_task(shift) do 
    BI.WeekTask.Protocol.exe_shift_week_task(shift)
end 





end 