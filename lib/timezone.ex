defmodule BI.Timezone do 
require Logger

def utc, do: "UTC"
def america_los_angeles, do: "America/Los_Angeles"


@doc """
    返回类似的时区： "utc", "utc-8"
    mills= Time.Util.Ex.convert_format_time_to_time_mills("YYYY-MM-DD HH:MM:SS","2021-11-20 23:53:11")
    BI.Timezone.get_timezone(BI.Timezone.america_los_angeles(), mills)
"""
def get_timezone( tz, mills) do 
    # Logger.info("mills=#{inspect mills}")
    {:ok, datetime}= DateTime.from_unix(mills*1000,:microsecond)
    timezone= Timex.timezone(tz,datetime)
    offset_utc_int= div(timezone.offset_utc + timezone.offset_std, 3600)
    "utc#{inspect offset_utc_int}"
end 

# BI.Timezone.get_af_timezone
def get_af_timezone(mills) do 
    get_timezone(BI.Config.timezone, mills) 
end 



end 