defmodule WriteAFToES.Cur do
require Logger

# WriteAFToES.test_request()
def test_request(data_type, source_type, from, to, timezone, reinstall_strategy, af_id) do 
    path= DownloadCSv.get_save_path(data_type, source_type, from, to, timezone)
    datas= if BI.Common.is_report_data(data_type) do 
        ReadCSV.read_report_data(path)
    else
        ReadCSV.read(path)
    end 
    datas= Enum.filter(datas, fn(x)-> Map.get(x, "AppsFlyer ID") == af_id end)
    do_request(data_type, from, to, path, datas, reinstall_strategy) 
end 

#  WriteAFToES.request
# -> {result, cnt, success_cnt}
def request(data_type, source_type, from, to, timezone, reinstall_strategy) do 
    path= DownloadCSv.get_save_path(data_type, source_type, from, to, timezone)
    datas= if BI.Common.is_report_data(data_type) do 
        ReadCSV.read_report_data(path)
    else
        ReadCSV.read(path)
    end 
    datas= ReadCSV.datas_filter_country_by_data_type(datas, data_type)
    do_request(data_type, from, to, path, datas, reinstall_strategy) 
end 


def request_by_did_nil(data_type, source_type, from, to, timezone, reinstall_strategy) do 
    path= DownloadCSv.get_save_path(data_type, source_type, from, to, timezone)
    datas= ReadCSV.read_by_did_nil(path)
    do_request(data_type, from, to,  path, datas, reinstall_strategy) 
end 


# -> {result, cnt, success_cnt}
def do_request(data_type, from, to, path, datas, reinstall_strategy) do 
    cond do 
        data_type == BI.Keys.data_type_install  -> 
            es_event_name= BI.Global.es_event_install(from, to, reinstall_strategy)
            r= write_install_data_to_es(es_event_name, datas, reinstall_strategy)
            Logger.error("write_install_data: path=#{inspect path}, \n result1=#{inspect r}")
            r
        data_type == BI.Keys.data_type_reinstall  -> 
            es_event_name= BI.Global.es_event_reinstall(from, to, reinstall_strategy)
            r= write_reinstall_data_to_es(es_event_name, datas, reinstall_strategy)
            Logger.error("write_reinstall_data: path=#{inspect path}, \n result1=#{inspect r}")
            r
        data_type == BI.Keys.data_type_purchase_event  ->
            es_event_name= BI.Global.es_event_purchase_event(from, to, reinstall_strategy)
            r= write_purchase_event_data_to_es(es_event_name, datas, reinstall_strategy)
            Logger.error("write_purchase_event_data: path=#{inspect path}, \n result1=#{inspect r}")
            r
        data_type == BI.Keys.data_type_daily_report -> 
            es_event_name= BI.Global.es_daily_report_event(from, to, reinstall_strategy)
            r= write_daily_report_data_to_es(es_event_name, datas, reinstall_strategy)
            Logger.error("write_daily_report_data: path=#{inspect path}, \n result1=#{inspect r}")
            r
    end 
end 





# -> {:ok, term}| {:error, reason}
def write_one_purchase_event_data_to_es(es_event_name,data, reinstall_strategy) do
    db_flag_pre= Map.get(db_flag_pre_config(reinstall_strategy), :event)
    write_flag_comb_id= BI.Common.get_write_flag_comb_id_key(db_flag_pre, :event, data, :es) 

    # #test
    # data= wrap_purchase_event_data(es_event_name,data, reinstall_strategy)
    # Logger.error("data=#{inspect data}")
    # {:ok, :test}
    case DB.WriteAFToESFlag.is_exist?(write_flag_comb_id) do 
        {:error,reason} -> 
            {:error,reason} 
        true -> 
           Logger.info("to af exist, #{inspect write_flag_comb_id}")
            {:ok, :exist}
        false -> 
            data= wrap_purchase_event_data(es_event_name,data, reinstall_strategy)
            case ES.Helper.request(data) do 
                {:error,reason} -> 
                    Logger.info("to af error,reason=#{inspect reason}")
                    {:error,reason}  
                {:ok, _ } ->
                    item= %{
                        comb_id: write_flag_comb_id,
                        time: Time.Util.time_string()
                    }
                    DB.WriteAFToESFlag.put_item(item) 
            end 
    end 
end 

# -> {result, cnt, success_cnt}
def write_purchase_event_data_to_es(es_event_name,datas, reinstall_strategy) do
    total= length(datas)
    List.foldl(datas, {true, 0, 0}, fn(x, {result, cnt, success_cnt}) ->  
            Logger.info("event progress: #{inspect cnt}/#{inspect total}")
            {result, success_cnt}= 
                case write_one_purchase_event_data_to_es(es_event_name,x, reinstall_strategy) do 
                    {:ok, _} -> 
                        {result, success_cnt + 1}
                    _ -> 
                        {false, success_cnt}
                end 
            {result, cnt + 1, success_cnt}
    end )
end 


# -> {result, cnt, success_cnt}
def write_daily_report_data_to_es(es_event_name,datas, _reinstall_strategy) do
    total= length(datas)
    List.foldl(datas, {true, 0, 0}, fn(x, {result, cnt, success_cnt}) ->  
            Logger.info("event progress: #{inspect cnt}/#{inspect total}")
            {result, success_cnt}= 
                case write_one_daily_report_data_to_es(es_event_name,x) do 
                    {:ok, _} -> 
                        {result, success_cnt + 1}
                    _ -> 
                        {false, success_cnt}
                end 
            {result, cnt + 1, success_cnt}
    end )
end 


# -> {:ok, term}| {:error, reason}
def write_one_daily_report_data_to_es(es_event_name,data) do
    db_flag_pre= Map.get(db_flag_pre_config(:normal), BI.Keys.atom_daily_report)
    write_flag_comb_id= BI.Common.get_write_flag_comb_id_key(db_flag_pre, BI.Keys.atom_daily_report, data, :es) 
    # case DB.WriteAFToESFlag.is_exist?(write_flag_comb_id) do 
    #     {:error,reason} -> 
    #         {:error,reason} 
    #     true -> 
    #        Logger.info("to af exist, #{inspect write_flag_comb_id}")
    #         {:ok, :exist}
    #     false -> 
            data= wrap_daily_report_event_data(es_event_name,data)
            case ES.Helper.request(data) do 
                {:error,reason} -> 
                    Logger.info("to af error,reason=#{inspect reason}")
                    {:error,reason}  
                {:ok, _ } ->
                    item= %{
                        comb_id: write_flag_comb_id,
                        time: Time.Util.time_string()
                    }
                    DB.WriteAFToESFlag.put_item(item) 
            end 
    # end 
end 

def string_to_float(s) do 
    case Float.parse(s) do 
        :error -> -1
        {v,_} -> v
    end 
end 

#  WriteAFToES.Cur.test_daily_time("2022-09-04")
def test_daily_time(date) do
    date_time= date <> " 00:00:00"
    date_time_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format,date_time)
    Logger.info("date_time_mills=#{inspect date_time_mills}")
    time_zone=  BI.Timezone.get_af_timezone(date_time_mills)
    Logger.info("time_zone=#{inspect time_zone}")
    date_time_stamp= Time.Util.Ex.global_time_string(date_time, time_zone)
    Logger.info("date=#{inspect date}, date_time=#{inspect date_time}, date_time_mills=#{inspect date_time_mills}, time_zone=#{inspect time_zone}")
    Logger.info("date_time_stamp=#{inspect date_time_stamp}")
end 

# -> %{}
def wrap_daily_report_event_data(es_event_name,data) do 
    date= Map.get(data, BI.Keys.af_report_date)
    date_time= date <> " 00:00:00"
    date_time_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format,date_time)
    time_zone=  BI.Timezone.get_af_timezone(date_time_mills)
    
    
    es_data=%{
        date: Map.get(data, BI.Keys.af_report_date),
        # date_time_stamp, #utc时间戳
        date_time_stamp: Time.Util.Ex.global_time_string(date_time, time_zone),
        country: Map.get(data, BI.Keys.af_report_country),
        media_source: Map.get(data, BI.Keys.af_report_media_source),
        campaign: Map.get(data, BI.Keys.af_report_campaign),
        impressions: string_to_float(Map.get(data, BI.Keys.af_report_impressions)),
        clicks: string_to_float(Map.get(data, BI.Keys.af_report_clicks)),
        ctr: string_to_float(Map.get(data, BI.Keys.af_report_ctr)),
        installs: string_to_float(Map.get(data, BI.Keys.af_report_installs)),
        conversion: string_to_float(Map.get(data, BI.Keys.af_report_conversion)),
        cost: string_to_float(Map.get(data, BI.Keys.af_reprot_cost)),
    }
    es_data |> Map.put(:event, es_event_name)
end 


def wrap_purchase_event_data(es_event_name,data, reinstall_strategy) do 
     #基本数据和install一致
    revenue= String.to_float(Map.get(data, BI.Keys.af_revenue, "0"))
    data1= wrap_install_data(es_event_name, data)  
            |> Map.put(:revenue, revenue)
    #查reinstall数据库获取reinstall的re-attrubition
    did=  Map.get(data, BI.Keys.ex_did)
    afid= Map.get(data, BI.Keys.af_appsflyer_id)
    # Logger.info("did=#{inspect did}, afid=#{inspect afid}")

    #如果数据在reinstall中存在, 表示这个数据是重装的用户购买的
    reinstall_db_module= BI.Global.get_reinstall_db_module(reinstall_strategy)
    case reinstall_db_module.get_item_with_media_source(did, afid) do  
        {:ok, db_data} -> 
            data1= data1 |> Map.put(:is_reinstall, true) |> Map.put(:db_data, "ok")
            re_attr_data= get_purchase_event_re_attr_data(data, db_data, did, reinstall_strategy)
            Map.put(data1, :re_attr_data, re_attr_data)
        {:error,:not_exist} -> 
            data1= data1 |> Map.put(:is_reinstall, false) |> Map.put(:db_data, "errnot")
            #如果不是reinstall也附加一下这个信息（用于与reinstall的用户的重归因窗口期一起做集合处理）
            re_attr_data= get_purchase_event_re_attr_data(data, %{}, did, reinstall_strategy)
            Map.put(data1, :re_attr_data, re_attr_data)
        {:error,_reason} -> 
            data1= data1 |> Map.put(:is_reinstall, false) |> Map.put(:db_data, "erroth")
            re_attr_data= get_purchase_event_re_attr_data(data, %{}, did, reinstall_strategy)
            Map.put(data1, :re_attr_data, re_attr_data)
    end 
end 

def get_purchase_event_re_attr_data(data, db_data, did, reinstall_strategy) do 
    _re_attr_data=%{
        #第一层的数据代表的是reinstall本身的渠道信息(因为是事件, 重装用户的事件的原始渠道信息会算为自然量)
        origin: re_attr_data(data),
        #第2-4层的数据代表的是reinstall以day为期进行重新归因的渠道信息
        d7:  get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_7d, BI.Keys.ex_new_in7d, reinstall_strategy),
        d14: get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_14d, BI.Keys.ex_new_in14d, reinstall_strategy),
        d30: get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_30d, BI.Keys.ex_new_in30d, reinstall_strategy),
        d90: get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_90d, BI.Keys.ex_new_in90d, reinstall_strategy),
    }
end 

def write_one_reinstall_data_to_es(es_event_name,data, reinstall_strategy) do
    db_flag_pre= Map.get(db_flag_pre_config(reinstall_strategy), :reinstall)
    write_flag_comb_id= BI.Common.get_write_flag_comb_id_key(db_flag_pre, :reinstall, data, :es) 
    case DB.WriteAFToESFlag.is_exist?(write_flag_comb_id) do 
        {:error,reason} -> 
            {:error,reason} 
        true -> 
           Logger.info("to af exist, #{inspect write_flag_comb_id}")
            {:ok, :exist}
        false -> 
            data= wrap_reinstall_item(es_event_name,data, reinstall_strategy)
            case ES.Helper.request(data) do 
                {:error,reason} -> 
                    Logger.info("to af error,reason=#{inspect reason}")
                    {:error,reason}  
                {:ok, _ } ->
                    item= %{
                        comb_id: write_flag_comb_id,
                        time: Time.Util.time_string()
                    }
                    DB.WriteAFToESFlag.put_item(item) 
            end 
    end 
end 


def write_reinstall_data_to_es(es_event_name,datas, reinstall_strategy) do
    total= length(datas)
    List.foldl(datas, {true, 0, 0}, fn(x, {result, cnt, success_cnt}) ->  
            Logger.info("re progress: #{inspect cnt}/#{inspect total}")
            {result, success_cnt}= 
                case write_one_reinstall_data_to_es(es_event_name,x, reinstall_strategy) do 
                    {:ok, _} -> 
                        {result, success_cnt + 1}
                    _ -> 
                        {false, success_cnt}
                end 
            {result, cnt + 1, success_cnt}
    end )
end 


def wrap_reinstall_item(es_event_name,data, reinstall_strategy) do 
    #基本数据和install一致
    data1= wrap_install_data(es_event_name, data)
    #添加reinstall的额外数据
    #查reinstall数据库获取reinstall的re-attrubition
    did=  Map.get(data, BI.Keys.ex_did)
    afid= Map.get(data, BI.Keys.af_appsflyer_id)
    # Logger.info("did=#{inspect did}, afid=#{inspect afid}")

    reinstall_db_module= BI.Global.get_reinstall_db_module(reinstall_strategy)

    db_data= case  reinstall_db_module.get_item_with_media_source(did, afid) do 
            {:error,_reason} -> 
                %{}
            {:ok, db_data} -> 
                db_data
            end 

    re_attr_data=%{
        #第一层的数据代表的是reinstall本身的渠道信息(如果这个用户是广告买来的，就归因给对应的广告)
        origin: re_attr_data(data),
        #第2-4层的数据代表的是reinstall以day为期进行重新归因的渠道信息
        d7:  get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_7d, BI.Keys.ex_new_in7d, reinstall_strategy),
        d14: get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_14d, BI.Keys.ex_new_in14d, reinstall_strategy),
        d30: get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_30d, BI.Keys.ex_new_in30d, reinstall_strategy),
        d90: get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_90d, BI.Keys.ex_new_in90d, reinstall_strategy),
    }

    reinstall_cnt= Map.get(db_data, BI.Keys.ex_reinstall_cnt, 0)
    re_attr_data= re_attr_data |> Map.put(:re_cnt, reinstall_cnt )
    Map.put(data1, :re_attr_data, re_attr_data)
end 

def get_re_attr_data_by_pre_day_data(data, db_data, did, day_key, new_key, reinstall_strategy) do 
    is_new= Map.get(db_data, new_key, false)
    afid= Map.get(db_data, day_key, "nil")
    return_data= 
        if afid == "self" do 
            #表示重装都是自己
            #如果是reinstall，data中会自带渠道信息，如果是event，此时data中的渠道信息算的是自然量，所以此处应该以db_data中的数据进行处理
            # re_attr_data(data) |> Map.put(:re_attr_type, "self")
            re_attr_data(db_data) |> Map.put(:re_attr_type, "self")
        else 
            if afid == "nil" do 
                #表示没有获取到历史重装记录（一般是重装记录在指定的天数以前, 或者是db_data为%{}）
                re_attr_data(data) |>  Map.put(:re_attr_type, "nil1")
            else 
                #查数据库获取对应的信息
                pre_db_data= get_pre_data(did, afid, reinstall_strategy)
                if pre_db_data == %{} do 
                    re_attr_data(data) |> Map.put(:re_attr_type, "nil2")
                else 
                    re_attr_data(pre_db_data) |>  Map.put(:re_attr_type, "history")
                end 
            end 
        end 
    return_data |> Map.put(:is_new, is_new)
end 


def get_pre_data(did, afid, reinstall_strategy) do 
    case DB.AFInstall.get_item_with_media_source(did, afid) do 
        {:error,:not_exist} -> 
            reinstall_db_module= BI.Global.get_reinstall_db_module(reinstall_strategy)
            case reinstall_db_module.get_item_with_media_source(did, afid) do 
                {:ok, data} -> 
                    data 
                {:error,_reason} -> 
                    %{}
            end 
        {:error, _reason} -> 
            %{}
        {:ok, data} -> 
            data 
    end 
end 


def re_attr_data(data) do 
    install_time= Map.get(data, BI.Keys.af_install_time)
    install_time_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format,install_time)
    time_zone=  BI.Timezone.get_af_timezone(install_time_mills)
    %{
        media_source: BI.Common.convert_to_kibana_media_source(Map.get(data, BI.Keys.af_media_source,"")),
        channel: Map.get(data, BI.Keys.af_channel,""),
        campaign: Map.get(data, BI.Keys.af_campaign,""),
        adset: Map.get(data, BI.Keys.af_adset,""),
        country: Map.get(data, BI.Keys.af_country_code,""),
        app_ver: Map.get(data, BI.Keys.af_app_version,""),
        afid: Map.get(data, BI.Keys.af_appsflyer_id),
        install_time: install_time,
        install_time_stamp: Time.Util.Ex.global_time_string(install_time, time_zone),
    }
end 

# -> {:ok, term}| {:error, reason}
def write_one_install_data_to_es(es_event_name, data, reinstall_strategy) do 
    db_flag_pre= Map.get(db_flag_pre_config(reinstall_strategy), :install)
    write_flag_comb_id= BI.Common.get_write_flag_comb_id_key(db_flag_pre, :install, data, :es) 
    case DB.WriteAFToESFlag.is_exist?(write_flag_comb_id) do 
        {:error,reason} -> 
            {:error,reason} 
        true -> 
           Logger.info("to af exist, #{inspect write_flag_comb_id}")
             {:ok, :exist}
        false -> 
            data= wrap_install_data(es_event_name, data)
            case ES.Helper.request(data) do 
                {:error,reason} -> 
                    Logger.info("to af error,reason=#{inspect reason}")
                    {:error,reason}  
                {:ok, _ } ->
                    item= %{
                        comb_id: write_flag_comb_id,
                        time: Time.Util.time_string()
                    }
                    DB.WriteAFToESFlag.put_item(item) 
            end 
    end 
end 

# -> {result, cnt, success_cnt}
def write_install_data_to_es(es_event_name, datas, reinstall_strategy) do
    total= length(datas)
    List.foldl(datas, {true, 0, 0}, fn(x, {result, cnt, success_cnt}) ->  
            Logger.info("progress: #{inspect cnt}/#{inspect total}")
            {result, success_cnt}= 
                case write_one_install_data_to_es(es_event_name,x, reinstall_strategy) do 
                    {:ok, _} -> 
                        {result, success_cnt + 1}
                    _ -> 
                        {false, success_cnt}
                end 
            {result, cnt + 1, success_cnt}
    end )
end       


@doc """

"""
def wrap_install_data(es_event_name, data) do 
    install_time= Map.get(data, BI.Keys.af_install_time)
    install_time_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format,install_time)
    time_zone=  BI.Timezone.get_af_timezone(install_time_mills)

    event_time= Map.get(data, BI.Keys.af_event_time)
    event_time_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format,event_time)
    event_time_zone=  BI.Timezone.get_af_timezone(event_time_mills)

    %{
        event: es_event_name,
        is_organic:  BI.Common.is_organic(Map.get(data, BI.Keys.af_media_source)),

        did:  Map.get(data, BI.Keys.ex_did),
        did_type: Map.get(data, BI.Keys.ex_did_type),
        afid: Map.get(data, BI.Keys.af_appsflyer_id),
        install_time: install_time, #该时区是utc-7时区，下面转换成真实的utc时间
        install_time_stamp: Time.Util.Ex.global_time_string(install_time, time_zone),
        event_time_stamp: Time.Util.Ex.global_time_string(event_time, event_time_zone),

        media_source: BI.Common.convert_to_kibana_media_source(Map.get(data, BI.Keys.af_media_source,"")),
        channel: Map.get(data, BI.Keys.af_channel,""),
        campaign: Map.get(data, BI.Keys.af_campaign,""),
        adset: Map.get(data, BI.Keys.af_adset,""),
        is_retarg: Map.get(data, BI.Keys.af_is_retargeting,""),
        is_prim_attr: Map.get(data, BI.Keys.af_is_primary_attr,""),
        is_reinstall: false,
        
        attr_tt: Map.get(data, BI.Keys.af_attributed_touch_type),
        region: Map.get(data, BI.Keys.af_region),
        country: Map.get(data, BI.Keys.af_country_code),
        state: Map.get(data, BI.Keys.af_state),
        platform: Map.get(data, BI.Keys.af_platform),
        app_ver: Map.get(data, BI.Keys.af_app_version),
        os_ver: Map.get(data, BI.Keys.af_os_version),
        attr_lookback: Map.get(data, BI.Keys.af_attrbiution_lookback,""),
    }
end 





#用于更新es的写入, db会使用名字来检查是否已经写入过数据了，修改了名字后可以重新写入es
def db_flag_pre_config_old(reinstall_strategy) do 
    case reinstall_strategy do 
        :normal -> 
           %{
                install: "es10",
                reinstall: "es10",
                event: "es13",
            }
        :gg -> 
            %{
                install: "es10",
                reinstall: "gg_es12",
                event: "gg_es15",
            }
    end 
end 

def db_flag_pre_config(reinstall_strategy) do 
    BI.Flag.Protocol.db_flag_pre_config_for_es(reinstall_strategy)
end 


end 