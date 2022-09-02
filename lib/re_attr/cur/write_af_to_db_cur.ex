defmodule WriteAFToDB.Cur do
require Logger


#  WriteAFToDB.request
def request(data_type, source_type, from, to, timezone, reinstall_strategy) do 
    path= DownloadCSv.get_save_path(data_type, source_type, from, to, timezone)
    datas= if BI.Common.is_report_data(data_type) do 
        ReadCSV.read_report_data(path)
    else
        ReadCSV.read(path)
    end 
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
            data_type == BI.Keys.data_type_daily_report -> 
                write_daily_report_data_to_db(datas)
        end  
    Logger.error("write_db: data_type=#{inspect data_type},  path=#{inspect path}, \n result=#{inspect r}")
    r
end 


# -> {:ok, term}| {:error, reason}
def write_one_install_data_to_db(data, reinstall_strategy) do 
    db_flag_pre= Map.get(db_flag_pre_config(reinstall_strategy), :install)
    write_flag_comb_id= BI.Common.get_write_flag_comb_id_key(db_flag_pre, :install, data, :db) 
    case DB.WriteAFToDBFlag.is_exist?(write_flag_comb_id) do 
        {:error,reason} -> 
            {:error,reason} 
        true -> 
            Logger.info("to db exist, #{inspect write_flag_comb_id}")
            {:ok, :exist}
        false -> 
            case DB.AFInstall.put_item(data) do 
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
            data= wrap_reinstall_item(reinstall_strategy, data)
            reinstall_db_module=  BI.Global.get_reinstall_db_module(reinstall_strategy)
            case reinstall_db_module.put_item(data) do 
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




@doc """
    data=%{
        BI.Keys.af_media_source => "",
        BI.Keys.ex_did => "1f516ba0-e6ec-4452-9e62-d0fd3cb84994",
        BI.Keys.af_install_time => "2021-11-21 23:55:46",
    }
    WriteAFToDB.wrap_reinstall_item(data)
"""
def wrap_reinstall_item(:normal, data) do 
    #添加7d，14d，30d，90d内的首次afid
    did= Map.get(data, BI.Keys.ex_did)
    install_time= Map.get(data, BI.Keys.af_install_time)
    install_time_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, install_time)
    {pre_datas, reinstall_cnt}= if did == "nil" do 
            {[],0}
        else     
            get_reinstall_pre_datas(did, install_time_mills, :normal)
        end 
    
    media_source= Map.get(data, BI.Keys.af_media_source, "")
    if media_source != "" do 
        #说明这个重装的用户当前有渠道信息（是因为渠道广告引起的重装，这个玩家的所有的事件都计为该广告渠道）
        default= "self"
            data 
            |> Map.put(BI.Keys.ex_afid_7d, default)
            |> Map.put(BI.Keys.ex_afid_14d, default)
            |> Map.put(BI.Keys.ex_afid_30d, default)
            |> Map.put(BI.Keys.ex_afid_90d, default)
            |> Map.put(BI.Keys.ex_reinstall_cnt, reinstall_cnt)
    else 
        afid_7d= get_pre_day_afid(pre_datas,  7, install_time_mills)
        afid_14d= get_pre_day_afid(pre_datas, 14, install_time_mills)
        afid_30d= get_pre_day_afid(pre_datas, 30, install_time_mills)
        afid_90d= get_pre_day_afid(pre_datas, 90, install_time_mills)

        data 
            |> Map.put(BI.Keys.ex_afid_7d, afid_7d)
            |> Map.put(BI.Keys.ex_afid_14d, afid_14d)
            |> Map.put(BI.Keys.ex_afid_30d, afid_30d)
            |> Map.put(BI.Keys.ex_afid_90d, afid_90d)
            |> Map.put(BI.Keys.ex_reinstall_cnt, reinstall_cnt)
    end
end 

@doc """
    对于gg的策略为：
    考察最近X天窗口期，如果本次reinstall距离上一次install或reinstall未超过窗口期，就将上一次的渠道信息归给当前，
    如果超过了窗口期，就认为是一个新用户（这个新用户在reinstall中加一个flag表示）
"""
def wrap_reinstall_item(:gg, data) do 
    #添加7d，14d，30d，90d内的af_id
    did= Map.get(data, BI.Keys.ex_did)
    install_time= Map.get(data, BI.Keys.af_install_time)
    install_time_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, install_time)
    {pre_datas, reinstall_cnt}= if did == "nil" do 
        {[],0}
    else     
        get_reinstall_pre_datas(did, install_time_mills, :gg)
    end 

    #分别计算7d,14d,30d窗口期的reinstall归因情况
    media_source= Map.get(data, BI.Keys.af_media_source, "")
    if media_source != "" do 
        #说明这个重装的用户当前有渠道信息（是因为渠道广告引起的重装，这个玩家的所有的事件都计为该广告渠道）
        default= "self"
            data 
            |> Map.put(BI.Keys.ex_afid_7d, default)
            |> Map.put(BI.Keys.ex_afid_14d, default)
            |> Map.put(BI.Keys.ex_afid_30d, default)
            |> Map.put(BI.Keys.ex_afid_90d, default)
            |> Map.put(BI.Keys.ex_reinstall_cnt, reinstall_cnt)
    else 
        {afid_7d, new_in7d}= get_reinstall_pre_day_afid(pre_datas,  7, install_time_mills)
        {afid_14d, new_in14d}= get_reinstall_pre_day_afid(pre_datas, 14, install_time_mills)
        {afid_30d, new_in30d}= get_reinstall_pre_day_afid(pre_datas, 30, install_time_mills)
        {afid_90d, new_in90d}= get_reinstall_pre_day_afid(pre_datas, 90, install_time_mills)
        data 
            |> Map.put(BI.Keys.ex_afid_7d, afid_7d) 
            |> Map.put(BI.Keys.ex_afid_14d, afid_14d)
            |> Map.put(BI.Keys.ex_afid_30d, afid_30d)
            |> Map.put(BI.Keys.ex_afid_90d, afid_90d)
            |> Map.put(BI.Keys.ex_reinstall_cnt, reinstall_cnt)
            |> Map.put(BI.Keys.ex_new_in7d, new_in7d)
            |> Map.put(BI.Keys.ex_new_in14d, new_in14d)
            |> Map.put(BI.Keys.ex_new_in30d, new_in30d)
            |> Map.put(BI.Keys.ex_new_in90d, new_in90d)
    end
end 



# -> {af_id, is_new}
def get_reinstall_pre_day_afid(pre_datas, day, install_time_mills) do 
    if pre_datas == [] do 
        {"nil", true}
    else 
        max_span= day * BI.Global.one_day_mills()
        #pre_datas已经是按照时间排好了顺序的, 按顺序处理即可
        {_pre1, pre_datas1}= 
            List.foldl(pre_datas, {nil,[]}, fn(x, {pre, acc}) ->  
                    if pre == nil do 
                        {x, [x|acc]}
                    else 
                        #两个数据<窗口期，更新当前的渠道信息
                        pre_install_time_mills= Map.get(pre, BI.Keys.af_install_time)
                        cur_install_time_mills= Map.get(x, BI.Keys.af_install_time)
                        span= cur_install_time_mills - pre_install_time_mills
                        if span <= max_span do 
                            pre_media_source= Map.get(pre, BI.Keys.af_media_source,"")
                            if BI.Common.is_organic(pre_media_source) do 
                                {x, [x|acc]}
                            else 
                                pre_af_id= Map.get(pre, BI.Keys.af_appsflyer_id)
                                #更新
                                x= Map.put(x, BI.Keys.af_media_source, pre_media_source) |>
                                   Map.put(BI.Keys.af_appsflyer_id, pre_af_id)
                                {x, [x|acc]}
                            end 
                        else 
                            {x, [x|acc]}
                        end 
                    end 
                end )
        #查看最近的一次数据是否和当前的数据在同一个窗口期, 注意pre_datas1是倒序的，第一个就是距离最近的一个
        pre_data= List.first(pre_datas1)
        pre_install_time_mills= Map.get(pre_data, BI.Keys.af_install_time)
        span= install_time_mills - pre_install_time_mills
        if span <= max_span do 
            {Map.get(pre_data, BI.Keys.af_appsflyer_id), false}
        else 
            {"nil",true}
        end 
    end 
end 


# -> []
def get_reinstall_pre_datas(did, install_time_mills, reinstall_strategy) do 
    num=30
    install_datas=  case DB.AFInstall.get_items_with_install_time(did, num) do 
        {:ok, items} -> 
            items 
        {:error,_reason} -> 
            []
    end 
    reinstall_db_module= BI.Global.get_reinstall_db_module(reinstall_strategy)
    reinstall_datas=case reinstall_db_module.get_items_with_install_time(did, num) do 
        {:ok, items} -> 
            items 
        {:error,_reason} -> 
            []
    end 
    pre_datas= Enum.concat(install_datas, reinstall_datas)
    #按时间排序
    
    pre_datas= Enum.map(pre_datas, fn(x) ->  
            it= Map.get(x,BI.Keys.af_install_time)
            it_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, it)
            Map.put(x, BI.Keys.af_install_time, it_mills)
        end )
    pre_datas= Enum.filter(pre_datas, fn(x) ->  
        span= install_time_mills - Map.get(x, BI.Keys.af_install_time)
        span > 0 and  span < 90 * BI.Global.one_day_mills() 
        end)
        
    if length(pre_datas) > 1 do 
        # Logger.info("pre_datas=#{inspect pre_datas}")
    end 
    pre_datas= Enum.sort(pre_datas, fn(x,y)->  Map.get(x, BI.Keys.af_install_time) < Map.get(y, BI.Keys.af_install_time) end )
    reinstall_cnt= get_reinstall_cnt(reinstall_datas, install_time_mills)
    {pre_datas, reinstall_cnt}
end 


@doc """
    获取重装的所有次数（距离3个月内的重装）
"""
def get_reinstall_cnt(reinstall_datas, install_time_mills) do 
    reinstall_datas= Enum.map(reinstall_datas, fn(x) ->  
            it= Map.get(x,BI.Keys.af_install_time)
            it_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, it)
            Map.put(x, BI.Keys.af_install_time, it_mills)
        end )
    reinstall_datas= Enum.filter(reinstall_datas, fn(x) ->  
        span= install_time_mills - Map.get(x, BI.Keys.af_install_time)
        span > 0 and  span < 90 * BI.Global.one_day_mills() 
        end)
    #+1 包括当前重装
    length(reinstall_datas) + 1
end 

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


def write_daily_report_data_to_db(datas) do 
    total= length(datas)
    List.foldl(datas, {true, 0, 0}, fn(x, {result, cnt, success_cnt}) ->  
        Logger.info("daily report progress: #{inspect cnt}/#{inspect total}")
        {result, success_cnt}= 
            case write_one_daily_report_data_to_db(x) do 
                {:ok, _} -> 
                    {result, success_cnt + 1}
                _ -> 
                    {false, success_cnt}
            end 
        {result, cnt + 1, success_cnt}
    end )
end 

# -> {:ok, term}| {:error, reason}
def write_one_daily_report_data_to_db(data) do 
    db_flag_pre= Map.get(db_flag_pre_config(:normal), BI.Keys.atom_daily_report)
    write_flag_comb_id= BI.Common.get_write_flag_comb_id_key(db_flag_pre, BI.Keys.atom_daily_report, data, :db) 
    case DB.WriteAFToDBFlag.is_exist?(write_flag_comb_id) do 
        {:error,reason} -> 
            {:error,reason} 
        true -> 
            Logger.info("to db exist, #{inspect write_flag_comb_id}")
            {:ok, :exist} 
        false -> 
            case DB.AFDailyReport.put_item(data) do 
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



#用于更新db的写入, db会使用名字来检查是否已经写入过数据了，修改了名字后可以重新写入es
def db_flag_pre_config_old(reinstall_strategy) do 
    case reinstall_strategy do 
        :normal -> 
            %{
                install: "db1", #test
                reinstall: "db1",
                event: "db1",
            }
        :gg -> 
            %{
                install: "db1",
                reinstall: "gg_db2", #只需要该数据做区分即可，其他的无需做区分
                event: "db1",
            }
    end 

end 

def db_flag_pre_config(reinstall_strategy) do 
    BI.Flag.Protocol.db_flag_pre_config_for_db(reinstall_strategy)
end 


def get_pre_day_afid(pre_datas, day, install_time_mills) do 
    use_first_most_value= true 
    if use_first_most_value do 
        get_pre_day_afid_by_first_most_value(pre_datas, day, install_time_mills)
    else 
        get_pre_day_afid_by_first_appear(pre_datas, day, install_time_mills)
    end 
end 

def get_pre_day_afid_by_first_appear(pre_datas, day, install_time_mills) do 
    #这个是以窗口期内的第一次重装为准
    install_time_key= BI.Keys.af_install_time
    pre_datas= Enum.filter(pre_datas, fn(x) ->  
        install_time_mills - Map.get(x, install_time_key) < day * BI.Global.one_day_mills() end )
    pre_datas= Enum.sort(pre_datas, fn(x,y) ->   
            Map.get(x, install_time_key) < Map.get(y, install_time_key)
        end )
    if pre_datas == [] do 
        "nil"
    else 
        pre_data= List.first(pre_datas)
        Map.get(pre_data, BI.Keys.af_appsflyer_id)
    end 
end 

def get_pre_day_afid_by_first_most_value(pre_datas, day, install_time_mills) do 
    # Logger.info("get_pre_day_afid_by_first_most_value")
    install_time_key= BI.Keys.af_install_time
    pre_datas= Enum.filter(pre_datas, fn(x) ->  
        install_time_mills - Map.get(x, install_time_key) < day * BI.Global.one_day_mills() end )
    pre_datas= Enum.sort(pre_datas, fn(x,y) ->   
            Map.get(x, install_time_key) < Map.get(y, install_time_key)
        end )
    
    if pre_datas == [] do 
        "nil"
    else 
        non_organic_pre_datas= Enum.filter(pre_datas, fn(x)->  !BI.Common.is_organic(Map.get(x, BI.Keys.af_media_source, "")) end )
        if non_organic_pre_datas == [] do
            pre_data= List.first(pre_datas)
            Map.get(pre_data, BI.Keys.af_appsflyer_id)
        else 
            pre_data= List.first(non_organic_pre_datas)
            Map.get(pre_data, BI.Keys.af_appsflyer_id)
        end 
    end 
end 











end 