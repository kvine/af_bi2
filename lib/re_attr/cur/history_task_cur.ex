defmodule BI.HistoryTask.Cur do 

    def download_history_data() do 
        # install 
        DownloadCSv.request(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-09-06", "2021-10-03", BI.Config.timezone)  #finish
        DownloadCSv.request(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-10-04", "2021-10-31", BI.Config.timezone)  #finish
        DownloadCSv.request(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-11-01", "2021-11-28", BI.Config.timezone)  #finish

        DownloadCSv.request(BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-09-06", "2021-10-03", BI.Config.timezone) #finish
        DownloadCSv.request(BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-10-04", "2021-10-31", BI.Config.timezone) #finish
        DownloadCSv.request(BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-11-01", "2021-11-28", BI.Config.timezone) #finish

        #reinsall 
        DownloadCSv.request(BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-09-06", "2021-10-03", BI.Config.timezone) #finish
        DownloadCSv.request(BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-10-04", "2021-10-31", BI.Config.timezone) #finish
        DownloadCSv.request(BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-11-01", "2021-11-28", BI.Config.timezone) #finish

        DownloadCSv.request(BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-09-06", "2021-10-03", BI.Config.timezone) #finish
        DownloadCSv.request(BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-10-04", "2021-10-31", BI.Config.timezone) #finish
        DownloadCSv.request(BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-11-01", "2021-11-28", BI.Config.timezone) #finish

        #purchase-event 
        DownloadCSv.request(BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-08", "2021-11-14", BI.Config.timezone) #finish
        DownloadCSv.request(BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-15", "2021-11-21", BI.Config.timezone) #finish
        DownloadCSv.request(BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-22", "2021-11-28", BI.Config.timezone) #finish

        DownloadCSv.request(BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-08", "2021-11-14", BI.Config.timezone) #finish
        DownloadCSv.request(BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-15", "2021-11-21", BI.Config.timezone) #finish
        DownloadCSv.request(BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-22", "2021-11-28", BI.Config.timezone) #finish

    end 


    def write_history_data_to_db() do 
        # install 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-09-06", "2021-10-03", BI.Config.timezone, :normal)  #finish
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-10-04", "2021-10-31", BI.Config.timezone, :normal)  #finish
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-11-01", "2021-11-28", BI.Config.timezone, :normal)  #finish

        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-09-06", "2021-10-03", BI.Config.timezone, :normal)  #finish
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-10-04", "2021-10-31", BI.Config.timezone, :normal)  #finish
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-11-01", "2021-11-28", BI.Config.timezone, :normal)  #finish

        #reinsall 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-09-06", "2021-10-03", BI.Config.timezone, :normal) 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-10-04", "2021-10-31", BI.Config.timezone, :normal) 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-11-01", "2021-11-28", BI.Config.timezone, :normal) 

        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-09-06", "2021-10-03", BI.Config.timezone, :normal)  
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-10-04", "2021-10-31", BI.Config.timezone, :normal)  
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-11-01", "2021-11-28", BI.Config.timezone, :normal)  

        #purchase-event 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-08", "2021-11-14", BI.Config.timezone, :normal)  
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-15", "2021-11-21", BI.Config.timezone, :normal)  
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-22", "2021-11-28", BI.Config.timezone, :normal)  

        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-08", "2021-11-14", BI.Config.timezone, :normal) 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-15", "2021-11-21", BI.Config.timezone, :normal) 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-22", "2021-11-28", BI.Config.timezone, :normal) 
    end 


    # BI.HistoryTask.write_history_data_to_es
    def write_history_data_to_es() do 
        # install 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-09-06", "2021-10-03", BI.Config.timezone, :normal)  
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-10-04", "2021-10-31", BI.Config.timezone, :normal)  
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-11-01", "2021-11-28", BI.Config.timezone, :normal) 

        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-09-06", "2021-10-03", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-10-04", "2021-10-31", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-11-01", "2021-11-28", BI.Config.timezone, :normal) 

        #reinsall 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-09-06", "2021-10-03", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-10-04", "2021-10-31", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-11-01", "2021-11-28", BI.Config.timezone, :normal) 

        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-09-06", "2021-10-03", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-10-04", "2021-10-31", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-11-01", "2021-11-28", BI.Config.timezone, :normal) 

        #purchase-event 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-08", "2021-11-14", BI.Config.timezone, :normal)  
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-15", "2021-11-21", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-22", "2021-11-28", BI.Config.timezone, :normal)  

        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-08", "2021-11-14", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-15", "2021-11-21", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-22", "2021-11-28", BI.Config.timezone, :normal) 
    end 


    # BI.HistoryTask.write_history_data_to_db_with_reinstall_strategy(:gg)
    def write_history_data_to_db_with_reinstall_strategy(:gg) do 
        #install 
        #reinsall 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-09-06", "2021-10-03", BI.Config.timezone, :gg) 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-10-04", "2021-10-31", BI.Config.timezone, :gg) 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-11-01", "2021-11-28", BI.Config.timezone, :gg) 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-11-29", "2021-12-05", BI.Config.timezone, :gg) 
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :gg) 

        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-09-06", "2021-10-03", BI.Config.timezone, :gg)  
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-10-04", "2021-10-31", BI.Config.timezone, :gg)  
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-11-01", "2021-11-28", BI.Config.timezone, :gg)  
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-11-29", "2021-12-05", BI.Config.timezone, :gg)  
        WriteAFToDB.request(WriteAFToDB.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :gg)  

        #purchase-event 
    end 


    # BI.HistoryTask.write_history_data_to_es_with_reinstall_strategy(:gg)
    def write_history_data_to_es_with_reinstall_strategy(:gg) do 
        #install 
        #reinsall 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-09-06", "2021-10-03", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-10-04", "2021-10-31", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-11-01", "2021-11-28", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-11-29", "2021-12-05", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :gg) 

        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-09-06", "2021-10-03", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-10-04", "2021-10-31", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-11-01", "2021-11-28", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-11-29", "2021-12-05", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :gg) 

        #purchase-event 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-08", "2021-11-14", BI.Config.timezone, :gg)  
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-15", "2021-11-21", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-22", "2021-11-28", BI.Config.timezone, :gg)  
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-29", "2021-12-05", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :gg)  

        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-08", "2021-11-14", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-15", "2021-11-21", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-22", "2021-11-28", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-29", "2021-12-05", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :gg) 
    end 


    # BI.HistoryTask.write_purchase_event_data_to_es
    def write_purchase_event_data_to_es() do 

        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-08", "2021-11-14", BI.Config.timezone, :normal)  
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-15", "2021-11-21", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-22", "2021-11-28", BI.Config.timezone, :normal)  
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-29", "2021-12-05", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :normal)  

        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-08", "2021-11-14", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-15", "2021-11-21", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-22", "2021-11-28", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-29", "2021-12-05", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :normal) 


        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-08", "2021-11-14", BI.Config.timezone, :gg)  
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-15", "2021-11-21", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-22", "2021-11-28", BI.Config.timezone, :gg)  
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-11-29", "2021-12-05", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :gg)  

        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-08", "2021-11-14", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-15", "2021-11-21", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-22", "2021-11-28", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-11-29", "2021-12-05", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :gg) 
    end 


    #  BI.HistoryTask.history_20211206_20220116_purchase
    # event: "es13" ,  "gg_es15"
    def history_20211206_20220116_purchase() do 
        #purchase-event 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-12-13", "2021-12-19", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-12-20", "2021-12-26", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-12-27", "2022-01-02", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2022-01-03", "2022-01-09", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2022-01-10", "2022-01-16", BI.Config.timezone, :normal) 

        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-12-13", "2021-12-19", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-12-20", "2021-12-26", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-12-27", "2022-01-02", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2022-01-03", "2022-01-09", BI.Config.timezone, :normal) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2022-01-10", "2022-01-16", BI.Config.timezone, :normal) 



        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-12-13", "2021-12-19", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-12-20", "2021-12-26", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2021-12-27", "2022-01-02", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2022-01-03", "2022-01-09", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, "2022-01-10", "2022-01-16", BI.Config.timezone, :gg) 

        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-12-06", "2021-12-12", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-12-13", "2021-12-19", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-12-20", "2021-12-26", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2021-12-27", "2022-01-02", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2022-01-03", "2022-01-09", BI.Config.timezone, :gg) 
        WriteAFToES.request(WriteAFToES.Cur, BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, "2022-01-10", "2022-01-16", BI.Config.timezone, :gg) 

    end 

    def history_20220117_20220123() do 
        from= "2022-01-17"
        to = "2022-01-23"
        ops= [:download, :write_to_db, :write_to_es]
        BI.FromToTask.exe_from_to_task(BI.FromToTask.Cur, from, to, ops )

        # BI.FromToTask.do_task(BI.FromToTask.Cur, :download, from, to)
        # BI.FromToTask.do_task(BI.FromToTask.Cur, :write_to_db, from, to)
        # BI.FromToTask.do_task(BI.FromToTask.Cur, :write_to_es, from, to)
    end 
   

end 