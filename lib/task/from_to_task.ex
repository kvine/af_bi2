defmodule BI.FromToTask do 
require Logger


# ops: [:download, :write_to_db, :write_to_es]
def exe_from_to_task(moudle, from, to, ops, types \\[:install, :reinstall, :event, :daily_report]) do 
    if (Enum.member?(ops, :download)) do 
        do_task(moudle, :download, from, to, types)
    end 
    if (Enum.member?(ops, :write_to_db)) do 
        do_task(moudle, :write_to_db, from, to, types)
    end
    if (Enum.member?(ops, :write_to_es)) do 
        do_task(moudle, :write_to_es, from, to, types)
    end  
end 


def do_task(moudle, :download, from, to, types) do 
    moudle.do_task(:download, from, to, types)
end 


def do_task(moudle, :write_to_db, from, to, types) do 
    moudle.do_task(:write_to_db, from, to, types)
end 

@doc """
    写es任务
    对install来说， reinstall_strategy不同不需要考虑
"""
def do_task(moudle, :write_to_es, from, to, types) do 
    moudle.do_task(:write_to_es, from, to, types)
end 




end 