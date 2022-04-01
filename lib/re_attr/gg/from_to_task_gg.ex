defmodule BI.FromToTask.GG do 
require Logger

def do_task(:download, from, to, types) do 
    Logger.info("FromToTask: from=#{inspect from}, to=#{inspect to}, download, start")

    if(Enum.member?(types, :install)) do 
        DownloadCSv.request(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone)  
        DownloadCSv.request(BI.Keys.data_type_install, BI.Keys.source_type_organic, from, to, BI.Config.timezone)  
    end 

    if(Enum.member?(types, :reinstall)) do 
        DownloadCSv.request(BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone) 
        DownloadCSv.request(BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, from, to, BI.Config.timezone)
    end 

    if(Enum.member?(types, :event)) do 
        DownloadCSv.request(BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone) 
        DownloadCSv.request(BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, from, to, BI.Config.timezone) 
    end 

    Logger.info("FromToTask: from=#{inspect from}, to=#{inspect to}, download, end")
end 


def do_task(:write_to_db, from, to, types) do 
    Logger.info("FromToTask: from=#{inspect from}, to=#{inspect to}, write_to_db, start")

    if(Enum.member?(types, :install)) do 
        WriteAFToDB.request(WriteAFToDB.GG, BI.Keys.data_type_install, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :newgg)  
        WriteAFToDB.request(WriteAFToDB.GG, BI.Keys.data_type_install, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :newgg)  
    end 

    if(Enum.member?(types, :reinstall)) do 
        WriteAFToDB.request(WriteAFToDB.GG, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :newgg) 
        WriteAFToDB.request(WriteAFToDB.GG, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :newgg)
    end 

    # 如果"当前的Cur"归因任务有在执行的情况下，此处已经执行过了无需再执行
    # if(Enum.member?(types, :event)) do 
    # WriteAFToDB.request(WriteAFToDB.GG, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :newgg) 
    # WriteAFToDB.request(WriteAFToDB.GG, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :newgg) 
    # end 
    
    Logger.info("FromToTask: from=#{inspect from}, to=#{inspect to}, write_to_db, end")

end 

@doc """
    写es任务
    对install来说， reinstall_strategy不同不需要考虑
"""
def do_task(:write_to_es, from, to, types) do 
    Logger.info("FromToTask: from=#{inspect from}, to=#{inspect to}, write_to_es, start")
    if(Enum.member?(types, :install)) do 
        WriteAFToES.request(WriteAFToES.GG, BI.Keys.data_type_install, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :newgg)  
        WriteAFToES.request(WriteAFToES.GG, BI.Keys.data_type_install, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :newgg)  
    end 

    #reinstall_strategy normal
    if(Enum.member?(types, :reinstall)) do 
        WriteAFToES.request(WriteAFToES.GG, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :newgg) 
        WriteAFToES.request(WriteAFToES.GG, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :newgg) 
    end 

    if(Enum.member?(types, :event)) do 
        WriteAFToES.request(WriteAFToES.GG, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :newgg) 
        WriteAFToES.request(WriteAFToES.GG, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :newgg)
    end

    Logger.info("FromToTask: from=#{inspect from}, to=#{inspect to}, write_to_es, end")
end 




end 