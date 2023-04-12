defmodule Check.ReinstallInInstall do 
require Logger


   

    @doc """
        检查近某一周的install中是否有新增的
        11-22-11-28号是否有在11-01-11-28号中的新增
    """

    # l= Check.ReinstallInInstall.check_new
    def check_new() do 
        install_ids_maps=
        [
            get_install_ids_map(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-11-01", "2021-11-28", BI.Config.timezone),
            # get_install_ids_map(BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-11-01", "2021-11-28", BI.Config.timezone),
        ]
        
        Logger.info("install_ids_maps=#{inspect install_ids_maps}")
        new_install_ids=     [
            get_install_ids_map(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-11-22", "2021-11-28", BI.Config.timezone),
            # get_reinstall_ids(BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-11-22", "2021-11-28", BI.Config.timezone),
        ]

        Logger.info("new_install_ids=#{inspect new_install_ids}")

        new_install_ids_total= Enum.concat(new_install_ids)
        for install_ids_map<- install_ids_maps do 
            Enum.filter(new_install_ids_total, fn(x)-> Map.get(install_ids_map, x) == nil end)
        end 

    end 


    @doc """
        检查某一周的reinstall中是否在install中
    """
    # Check.ReinstallInInstall.check
    def check() do 
        reinstall_ids= get_reinstall_ids()
        reinstall_ids_total= Enum.concat(reinstall_ids)

        install_ids_maps= get_install_ids_maps()
        for install_ids_map<- install_ids_maps do 
            Enum.filter(reinstall_ids_total, fn(x)-> Map.get(install_ids_map, x) != nil end)
        end 
    end 

    # Check.ReinstallInInstall.check(reinstall_ids_total)
    def check(reinstall_ids_total) do 
        install_ids_maps= get_install_ids_maps()
        for install_ids_map<- install_ids_maps do 
            Enum.filter(reinstall_ids_total, fn(x)-> Map.get(install_ids_map, x) != nil end)
        end 
    end 


    # reinstall_ids_total= Check.ReinstallInInstall.get_total_reinstall_ids()
    def get_total_reinstall_ids() do 
        reinstall_ids= get_reinstall_ids()
        _reinstall_ids_total= Enum.concat(reinstall_ids)
    end 

    def get_reinstall_ids() do 
        [
            get_reinstall_ids(BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-09-06", "2021-10-03", BI.Config.timezone),
            get_reinstall_ids(BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-10-04", "2021-10-31", BI.Config.timezone),
            get_reinstall_ids(BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, "2021-11-01", "2021-11-28", BI.Config.timezone),
            get_reinstall_ids(BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-09-06", "2021-10-03", BI.Config.timezone),
            get_reinstall_ids(BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-10-04", "2021-10-31", BI.Config.timezone),
            get_reinstall_ids(BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, "2021-11-01", "2021-11-28", BI.Config.timezone),
        ]
    end 

    def get_install_ids_maps() do 
        [
            #get_install_ids_map(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-09-06", "2021-10-03", BI.Config.timezone),
            get_install_ids_map(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-10-04", "2021-10-31", BI.Config.timezone),
            # get_install_ids_map(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-11-01", "2021-11-28", BI.Config.timezone),
            # get_install_ids_map(BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-09-06", "2021-10-03", BI.Config.timezone),
            # get_install_ids_map(BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-10-04", "2021-10-31", BI.Config.timezone),
            # get_install_ids_map(BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-11-01", "2021-11-28", BI.Config.timezone),
        ]
    end 

    def get_reinstall_ids(data_type, source_type, from, to, timezone) do 
        path= DownloadCSV.get_save_path(data_type, source_type, from, to, timezone)
        datas= ReadCSV.read_by_did_nil(path)
        # Enum.map(datas,fn(x)-> Map.get(x, BI.Keys.ex_did) end)
        Enum.map(datas,fn(x)-> Map.get(x, BI.Keys.af_appsflyer_id) end)
    end 

    #  m=Check.ReinstallInInstall.get_install_ids_map(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-11-22", "2021-11-28", BI.Config.timezone)
    #  m=Check.ReinstallInInstall.get_install_ids_map(BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-11-22", "2021-11-28", BI.Config.timezone)
    def get_install_ids_map(data_type, source_type, from, to, timezone) do 
        path= DownloadCSV.get_save_path(data_type, source_type, from, to, timezone)
        datas= ReadCSV.read(path)
        Map.new(datas,fn(x)-> { {Map.get(x, BI.Keys.ex_did), Map.get(x, BI.Keys.af_appsflyer_id)}, ""} end)
        # Map.new(datas,fn(x)-> { Map.get(x, BI.Keys.af_appsflyer_id), ""} end)
    end 

    #m=Check.ReinstallInInstall.get_repeated_ids(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, "2021-11-22", "2021-11-28", BI.Config.timezone)
    # m=Check.ReinstallInInstall.get_repeated_ids(BI.Keys.data_type_install, BI.Keys.source_type_organic, "2021-11-22", "2021-11-28", BI.Config.timezone)
    def get_repeated_ids(data_type, source_type, from, to, timezone) do 
        path= DownloadCSV.get_save_path(data_type, source_type, from, to, timezone)
        datas= ReadCSV.read(path)
        {rl,_rm}=
        List.foldl(datas, {[],%{}}, fn(x,{l,m}) ->  
                 key= {Map.get(x, BI.Keys.ex_did), Map.get(x, BI.Keys.af_appsflyer_id), Map.get(x, BI.Keys.af_install_time)}
                 if Map.get(m, key) == nil do 
                    {l, Map.put(m,key,"")}

                 else 
                    {[key|l], m}
                 end 
            end )
        rl
    end 

    # m=Check.ReinstallInInstall.get_repeated_ids(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, BI.Keys.source_type_organic, "2021-11-22", "2021-11-28", BI.Config.timezone)
     def get_repeated_ids(data_type, source_type, source_type2, from, to,  timezone) do 
        path= DownloadCSV.get_save_path(data_type, source_type, from, to, timezone)
        datas1= ReadCSV.read(path)

        path= DownloadCSV.get_save_path(data_type, source_type2, from, to, timezone)
        datas2= ReadCSV.read(path)

        datas= Enum.concat(datas1, datas2)
        {rl,_rm}=
        List.foldl(datas, {[],%{}}, fn(x,{l,m}) ->  
                 key= {Map.get(x, BI.Keys.ex_did), Map.get(x, BI.Keys.af_appsflyer_id)}
                 if Map.get(m, key) == nil do 
                    {l, Map.put(m,key,"")}

                 else 
                    {[key|l], m}
                 end 
            end )
        rl
    end  


end 