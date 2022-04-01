defmodule BI.HistoryTask.GG do 

    def history_20210906_20220206() do 
       
        ops= [:write_to_db, :write_to_es]

        types= [:install, :reinstall]
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2021-09-06", "2021-10-03", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2021-10-04", "2021-10-31", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2021-11-01", "2021-11-28", ops, types)

        types= [:event]
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2021-11-08", "2021-11-14", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2021-11-15", "2021-11-21", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2021-11-22", "2021-11-28", ops, types)
        
        types= [:install, :reinstall, :event]
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2021-11-29", "2021-12-05", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2021-12-06", "2021-12-12", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2021-12-13", "2021-12-19", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2021-12-20", "2021-12-26", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2021-12-27", "2022-01-02", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2022-01-03", "2022-01-09", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2022-01-10", "2022-01-16", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2022-01-17", "2022-01-23", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2022-01-24", "2022-01-30", ops, types)
        BI.FromToTask.exe_from_to_task(BI.FromToTask.GG, "2022-01-31", "2022-02-06", ops, types)
    end 

end