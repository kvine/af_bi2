defmodule BI.Common do 
require  Logger

# BI.Common.get_write_flag_comb_id_key
def get_write_flag_comb_id_key(db_flag_pre, type, data, target_type) do 
    # Logger.info("data=#{inspect data}")
    if is_report_data_by_atom(type) do
        date_country= Map.get(data, BI.Keys.ex_date_country)
        mediasource_campaign= Map.get(data, BI.Keys.ex_mediasource_campaign)
        db_flag_pre<>"__"<>date_country<>"__"<>mediasource_campaign
    else 
        did= Map.get(data, BI.Keys.ex_did)
        afid= Map.get(data, BI.Keys.af_appsflyer_id)
        event_time= Map.get(data, BI.Keys.af_event_time)
        andrid= Map.get(data, BI.Keys.af_android_id)
        if did == nil do 
            Logger.info("did=#{inspect did}, afid=#{inspect afid}, andrid=#{inspect andrid}")
        end 
        is_es= target_type == :es
        case type do 
            :install -> 
                if is_es do 
                    db_flag_pre<>"__"<>"0__"<>did<>"__"<>afid<>"__"<>event_time #af中的install数据中有一些问题（有极少数重复did和afid），这里为了和af控制台一致
                else 
                    db_flag_pre<>"__"<>"0__"<>did<>"__"<>afid
                end 
            :reinstall -> 
                db_flag_pre<>"__"<>"1__"<>did<>"__"<>afid
            :event -> 
                db_flag_pre<>"__"<>"2__"<>did<>"__"<>afid<>"__"<>event_time
        end 
    end 
end 

def is_report_data(data_type) do 
    data_type ==  BI.Keys.data_type_daily_report
end 

def is_report_data_by_atom(data_type) do 
    data_type ==  BI.Keys.atom_daily_report
end 

def is_organic(media_source) do 
    media_source == "" or media_source == "organic"
end 


# BI.Common.convert_to_kibana_media_source
def convert_to_kibana_media_source(media_source) do 
    if media_source == "" do
        "organic"
    else 
        media_source
    end 
end 


def get_cur_week_date_range_string() do 
   get_week_date_range_string(0)
end 

def get_last_week_date_string() do 
    get_week_date_range_string(-1)
end 


@doc """
    获取某一周日期，比如 "2021-11-22", "2021-11-28"
    # BI.Common.get_week_date_range_string(0)
"""
def get_week_date_range_string(shift_week) do 
    datetime= Timex.now(BI.Config.timezone)
    begin_week_datetime= Timex.beginning_of_week(datetime) |> Timex.shift(weeks: shift_week)
    end_week_datetime= Timex.end_of_week(datetime)  |> Timex.shift(weeks: shift_week)

    begin_date_string= :erlang.iolist_to_binary(:io_lib.format("~4..0w-~2..0w-~2..0w", [begin_week_datetime.year, begin_week_datetime.month, begin_week_datetime.day]))
    end_date_string= :erlang.iolist_to_binary(:io_lib.format("~4..0w-~2..0w-~2..0w", [end_week_datetime.year, end_week_datetime.month, end_week_datetime.day]))
    
    {begin_date_string, end_date_string}
end 


# BI.Common.get_cur_day_date_range_string
def get_cur_day_date_range_string() do 
   get_day_date_range_string(0)
end 

def get_last_day_date_string() do 
    get_day_date_range_string(-1)
end 



@doc """
    获取某一周日期，比如 "2021-11-22", "2021-11-28"
    # BI.Common.get_day_date_range_string(0)
"""
def get_day_date_range_string(shift) do
    datetime= Timex.now(BI.Config.timezone)
    begin_day_datetime= Timex.beginning_of_day(datetime) |> Timex.shift(days: shift)
    next_day_shift= shift + 1
    end_day_datetime= Timex.end_of_day(datetime)  |> Timex.shift(days: next_day_shift)

    begin_date_string= :erlang.iolist_to_binary(:io_lib.format("~4..0w-~2..0w-~2..0w", [begin_day_datetime.year, begin_day_datetime.month, begin_day_datetime.day]))
    end_date_string= :erlang.iolist_to_binary(:io_lib.format("~4..0w-~2..0w-~2..0w", [end_day_datetime.year, end_day_datetime.month, end_day_datetime.day]))
    
    {begin_date_string, end_date_string}
end 




end 