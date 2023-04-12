defmodule WriteAFToDB.GG do
require Logger


#  WriteAFToDB.request
def request(data_type, source_type, from, to, timezone, reinstall_strategy) do 
    path= DownloadCSV.get_save_path(data_type, source_type, from, to, timezone)
    datas= ReadCSV.read(path)
    #只处理美国的用户
    datas= Enum.filter(datas, fn(x)-> Map.get(x, BI.Keys.af_country_code) == "US" end)
    do_request(data_type, path, datas, reinstall_strategy) 
end 

# -> {result, cnt, success_cnt}
def do_request(data_type,  path, datas,  reinstall_strategy) do 
    r=  cond do 
            data_type == BI.Keys.data_type_install  -> 
                write_install_data_to_db(datas, reinstall_strategy)
            data_type == BI.Keys.data_type_reinstall  -> 
                write_reinstall_data_to_db(datas, reinstall_strategy)
            data_type == BI.Keys.data_type_purchase_event  ->
                write_purchase_event_data_to_db(datas, reinstall_strategy)
        end  
    Logger.error("write_db: data_type=#{inspect data_type},  path=#{inspect path}, \n result=#{inspect r}")
    r
end 


# //////////////////////////////////////////////////////////
# install 
# //////////////////////////////////////////////////////////

# -> {:ok, term}| {:error, reason}
def write_one_install_data_to_db(data, reinstall_strategy) do 
    db_flag_pre= Map.get(db_flag_pre_config(reinstall_strategy), :install)
    write_flag_comb_id= BI.Common.get_write_flag_comb_id_key(db_flag_pre, :install, data, :db) 
    case DB.WriteAFToDBFlag.is_exist?(write_flag_comb_id) do 
        {:error,reason} -> 
            Logger.error("DB.WriteAFToDBFlag.is_exist error #{inspect reason}")
            {:error,reason} 
        true -> 
            Logger.info("to db exist, #{inspect write_flag_comb_id}")
            {:ok, :exist}
        false -> 
            data= wrap_in_reinstall_item(reinstall_strategy, data, false)
            ##计算data 存储到db
            case DB.AFInReinstallGG.put_item(data) do 
                {:error,reason} -> 
                    # Logger.error("DB.AFInReinstallGG.put_item error  #{inspect reason}")
                    Logger.info("to db error,reason=#{inspect reason}")
                    {:error,reason}  
                {:ok, _ } ->
                    item= %{
                        comb_id: write_flag_comb_id,
                        time: Time.Util.time_string()
                    }
                    DB.WriteAFToDBFlag.put_item(item) 
            end 
    end
end 


# -> {result, cnt, success_cnt}
def write_install_data_to_db(datas, reinstall_strategy) do
    total= length(datas)
    List.foldl(datas, {true, 0, 0}, fn(x, {result, cnt, success_cnt}) ->  
            Logger.info("progress: #{inspect cnt}/#{inspect total}")
            {result, success_cnt}= 
                case write_one_install_data_to_db(x, reinstall_strategy) do 
                    {:ok, _} -> 
                        {result, success_cnt + 1}
                    _ -> 
                        {false, success_cnt}
                end 
            {result, cnt + 1, success_cnt}
    end )
end 


# //////////////////////////////////////////////////////////
# reinstall 
# //////////////////////////////////////////////////////////
# -> {:ok, term}| {:error, reason}
def write_one_reinstall_data_to_db(data, reinstall_strategy) do 
    db_flag_pre= Map.get(db_flag_pre_config(reinstall_strategy), :reinstall)
    write_flag_comb_id= BI.Common.get_write_flag_comb_id_key(db_flag_pre, :reinstall, data, :db) 
    case DB.WriteAFToDBFlag.is_exist?(write_flag_comb_id) do 
        {:error,reason} -> 
            {:error,reason} 
        true -> 
            Logger.info("to db exist, #{inspect write_flag_comb_id}")
           {:ok, :exist}
        false -> 
            data= wrap_in_reinstall_item(reinstall_strategy, data, true)
            case DB.AFInReinstallGG.put_item(data) do 
                {:error,reason} -> 
                    Logger.info("to db error,reason=#{inspect reason}")
                    {:error,reason}  
                {:ok, _ } ->
                    item= %{
                        comb_id: write_flag_comb_id,
                        time: Time.Util.time_string()
                    }
                    DB.WriteAFToDBFlag.put_item(item) 
            end 
    end 
end 


# -> {result, cnt, success_cnt}
def write_reinstall_data_to_db(datas, reinstall_strategy) do  
    total= length(datas)
    List.foldl(datas, {true, 0, 0}, fn(x, {result, cnt, success_cnt}) ->  
            Logger.info("re progress: #{inspect cnt}/#{inspect total}")
            {result, success_cnt}= 
                case write_one_reinstall_data_to_db(x, reinstall_strategy) do 
                    {:ok, _} -> 
                        {result, success_cnt + 1}
                    _ -> 
                        {false, success_cnt}
                end 
            {result, cnt + 1, success_cnt}
    end )
end 



# -> %{}
def wrap_in_reinstall_item(_reinstall_strategy, data, is_install) do 
    did= Map.get(data, BI.Keys.ex_did)
    afid= Map.get(data, BI.Keys.af_appsflyer_id)
    # media_source=  Map.get(data, BI.Keys.af_media_source(), "")
    install_time= Map.get(data, BI.Keys.af_install_time)
    install_time_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format,install_time)
    time_zone=  BI.Timezone.get_af_timezone(install_time_mills)
    install_timestamp= Time.Util.Ex.local_mills_to_global_mills(install_time_mills, time_zone)

    uninstall_timestamp= 
        case get_uninstall_timestamp(did, install_timestamp) do
            nil -> 
                get_uninstall_timestamp_2(did, install_timestamp, BI.Global.one_day_mills())
            timestamp -> 
                timestamp
        end

    {afid_7d, is_reattr_7d}= get_afid(7, BI.Keys.ex_afid_7d, did, afid, install_timestamp, uninstall_timestamp)
    {afid_14d, is_reattr_14d}= get_afid(14, BI.Keys.ex_afid_14d, did, afid, install_timestamp, uninstall_timestamp)

    data 
        |> Map.put(BI.Keys.ex_is_reinstall, is_install)
        |> Map.put(BI.Keys.ex_install_timestamp, install_timestamp)

        |> Map.put(BI.Keys.ex_afid_7d, afid_7d)
        |> Map.put(BI.Keys.ex_afid_14d, afid_14d)
        |> Map.put(BI.Keys.ex_is_reattr_7d, is_reattr_7d)
        |> Map.put(BI.Keys.ex_is_reattr_14d, is_reattr_14d)
end 

#返回距离安装时间最近的卸载时间
#-> nil | number
def get_uninstall_timestamp(did, install_timestamp) do 
    case DB.BQUninstall.Entity.get_items(did,install_timestamp, 1) do
        {:ok, datas} ->
            case datas do
                [] -> nil
                [data] -> Map.get(data, :event_timestamp, nil)
            end
        {:error, _} ->
            nil
    end
end 

#返回距离安装时间最近的卸载时间
#-> nil | number
def get_uninstall_timestamp_2(did, install_timestamp, off) do
    case DB.BQUninstall.Entity.get_items(did, install_timestamp+off, 10) do
        {:ok, datas} ->
            if(datas == []) do
                nil
            else 
                datas = Enum.filter(datas, fn x -> x.event_timestamp >= install_timestamp end)
                if(datas == []) do
                    nil
                else
                    install_timestamp
                end
            end
        {:error, _} ->
            nil
    end
end

#todo  参考“bq数据.txt” 中的说明进行处理
# -> {afid , is_reinstall}
def get_afid(day_window, ex_afid_key, did, afid, install_timestamp, uninstall_timestamp) do 
    dur = day_window * BI.Global.one_day_mills()

    if(uninstall_timestamp == nil) do
        {"nil_unitime", false}
    else
        if(abs(uninstall_timestamp - install_timestamp) <= dur) do
            case DB.AFInReinstallGG.get_items(did, 1, uninstall_timestamp) do  
                {:ok, items} -> 
                    case items do
                        [] ->
                            {"nil", false}
                        [item] ->
                            las_afid = Map.get(item, BI.Keys.af_appsflyer_id, "")
                            case DB.AFInReinstallGG.get_item(did, las_afid) do
                                {:ok, entity} ->
                                    las_media_source=  Map.get(entity, BI.Keys.af_media_source(), "")
                                    if(BI.Common.is_organic(las_media_source)) do
                                        ex_afid = Map.get(entity, ex_afid_key, "nil")
                                        if(WriteAFToES.GG.is_invalid_afid?(ex_afid)) do
                                            {afid, true}
                                        else
                                            case DB.AFInReinstallGG.get_item(did, ex_afid) do
                                                {:ok, ex_entity} ->
                                                    las_media_source = Map.get(ex_entity, BI.Keys.af_media_source(), "")
                                                    if(BI.Common.is_organic(las_media_source)) do
                                                        {afid, true}
                                                    else
                                                        {ex_afid, true}
                                                    end
                                                {:error, _} ->
                                                    {"nil", false}
                                            end
                                        end  
                                    else
                                        {las_afid,true}
                                    end
                                {:error, _} ->
                                    {"nil", false}
                            end
                            
                    end
                {:error, reason} -> 
                    Logger.error("get_afid:: DB.AFInReinstallGG.get_items error #{inspect reason}")
                    {"nil", false}
            end 
        else
            # 超过窗口期的情况，将afid_7d设置为"over_wnd", is_reattr_7d 为false 
            {"over_wnd",false}
        end
    end
end 




# //////////////////////////////////////////////////////////
# purchase event 
# //////////////////////////////////////////////////////////

# -> {:ok, term}| {:error, reason}
def write_one_purchase_event_data_to_db(data, reinstall_strategy) do 
    db_flag_pre= Map.get(db_flag_pre_config(reinstall_strategy), :event)
    write_flag_comb_id= BI.Common.get_write_flag_comb_id_key(db_flag_pre, :event, data, :db) 
    case DB.WriteAFToDBFlag.is_exist?(write_flag_comb_id) do 
        {:error,reason} -> 
            {:error,reason} 
        true -> 
            Logger.info("to db exist, #{inspect write_flag_comb_id}")
            {:ok, :exist} 
        false -> 
            case DB.AFPurchaseEvent.put_item(data) do 
                {:error,reason} -> 
                    Logger.info("to db error,reason=#{inspect reason}")
                    {:error,reason}  
                {:ok, _ } ->
                    item= %{
                        comb_id: write_flag_comb_id,
                        time: Time.Util.time_string()
                    }
                    DB.WriteAFToDBFlag.put_item(item) 
            end 
    end 
end 


def write_purchase_event_data_to_db(datas, reinstall_strategy) do 
    total= length(datas)
    List.foldl(datas, {true, 0, 0}, fn(x, {result, cnt, success_cnt}) ->  
        Logger.info("event progress: #{inspect cnt}/#{inspect total}")
        {result, success_cnt}= 
            case write_one_purchase_event_data_to_db(x, reinstall_strategy) do 
                {:ok, _} -> 
                    {result, success_cnt + 1}
                _ -> 
                    {false, success_cnt}
            end 
        {result, cnt + 1, success_cnt}
    end )
end 



#用于更新db的写入, db会使用名字来检查是否已经写入过数据了，修改了名字后可以重新写入es
def db_flag_pre_config_old(_reinstall_strategy) do
    %{
        install: "newgg_db16", #test
        reinstall: "newgg_db16",
        event: "newgg_db16",
    }
end 


def db_flag_pre_config(reinstall_strategy) do 
    BI.Flag.Protocol.db_flag_pre_config_for_db_gg(reinstall_strategy)
end 










end 