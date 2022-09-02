defmodule BI.FromToTask.Cur do 
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

    if(Enum.member?(types, :daily_report)) do 
        DownloadCSv.request(BI.Keys.data_type_daily_report, BI.Keys.source_type_nil, from, to, BI.Config.timezone) 
    end 

    Logger.info("FromToTask: from=#{inspect from}, to=#{inspect to}, download, end")
end 


def do_task(:write_to_db, from, to, types) do 
    Logger.info("FromToTask: from=#{inspect from}, to=#{inspect to}, write_to_db, start")
    
    if(Enum.member?(types, :install)) do 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_install, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :normal)  
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_install, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :normal)  
    end 

    if(Enum.member?(types, :reinstall)) do 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :normal) 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :normal)
    end 

    if(Enum.member?(types, :event)) do 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :normal) 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :normal) 
    end 

    if(Enum.member?(types, :daily_report)) do 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_daily_report, BI.Keys.source_type_nil, from, to, BI.Config.timezone, :normal) 
    end 


    Logger.info("FromToTask: from=#{inspect from}, to=#{inspect to}, write_to_db, end")

end 

@doc """
    写es任务
    对install来说， reinstall_strategy不同不需要考虑
"""
def do_task(:write_to_es, from, to, types) do 
    Logger.info("FromToTask: from=#{inspect from}, to=#{inspect to}, write_to_es, start")

    if(Enum.member?(types, :install)) do 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_install, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :normal)  
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_install, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :normal)    
    end 

    if(Enum.member?(types, :reinstall)) do 
        #reinstall_strategy normal
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :normal) 
    end 

    if(Enum.member?(types, :event)) do 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :normal)   
    end 

    if(Enum.member?(types, :daily_report)) do 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_daily_report, BI.Keys.source_type_nil, from, to, BI.Config.timezone, :normal) 
    end 

    # #reinstall_strategy gg
    # if(Enum.member?(types, :reinstall)) do 
    # WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :gg) 
    # WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :gg) 
    # end 

    # if(Enum.member?(types, :event)) do 
    # WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone, :gg) 
    # WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, from, to, BI.Config.timezone, :gg)
    # end 
    
    Logger.info("FromToTask: from=#{inspect from}, to=#{inspect to}, write_to_es, end")
end 




end 