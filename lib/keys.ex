defmodule BI.Keys do 

###########################################################################
### AF 普通原始数据中的属性名
###########################################################################
    def af_media_source, do: "Media Source"
    def af_channel, do: "Channel"
    def af_campaign, do: "Campaign"
    def af_campaign_id, do: "Campaign ID"
    def af_adset, do: "Adset"

    def af_appsflyer_id, do: "AppsFlyer ID"
    def af_install_time, do: "Install Time"
    def af_event_time, do: "Event Time"
    def af_android_id, do: "Android ID"
    def af_advertising_id, do: "Advertising ID"
    def af_imei, do: "IMEI"

    def af_is_retargeting, do: "Is Retargeting"
    def af_is_primary_attr, do: "Is Primary Attribution"

    def af_attributed_touch_type, do: "Attributed Touch Type"
    def af_region, do: "Region"
    def af_country_code, do: "Country Code"
    def af_state, do: "State"
    def af_platform, do: "Platform"
    def af_app_version, do: "App Version"
    def af_os_version, do: "OS Version"
    def af_attrbiution_lookback, do: "Attribution Lookback"
    def af_revenue, do: "Event Revenue"
    
###########################################################################
### AF 聚合报告中原始数据的属性名
###########################################################################
    def af_report_media_source, do: "Media Source (pid)"
    def af_report_date, do: "Date"
    def af_report_country, do: "Country"
    def af_report_campaign, do: "Campaign (c)"
    def af_report_impressions, do: "Impressions"
    def af_report_clicks, do: "Clicks"
    def af_report_ctr, do: "CTR"
    def af_report_installs, do: "Installs"
    def af_report_conversion, do: "Conversion Rate"
    def af_reprot_cost, do: "Total Cost"

###########################################################################
### 自定义相关
###########################################################################
    def af_time_format, do: "YYYY-MM-DD HH:MM:SS"


    def ex_afid_7d, do: "afid_7d"
    def ex_afid_14d, do: "afid_14d"
    def ex_afid_30d, do: "afid_30d"
    def ex_afid_90d, do: "afid_90d"
    def ex_new_in7d, do: "new_in7d"
    def ex_new_in14d, do: "new_in14d"
    def ex_new_in30d, do: "new_in30d"
    def ex_new_in90d, do: "new_in90d"
    def ex_did, do: "did"
    def ex_did_type, do: "did_type"
    def ex_did_type_adsid, do: "t_adsid"
    def ex_did_type_andid, do: "t_andid"
    def ex_did_type_imei, do: "t_imei"
    def ex_reinstall_cnt, do: "reinstall_cnt"

    def ex_is_reattr_7d, do: "is_reattr_7d"
    def ex_is_reattr_14d, do: "is_reattr_14d"
    def ex_is_reinstall, do: "is_reinstall"
    def ex_install_timestamp, do: "install_timestamp"
   
    def ex_is_reattr, do: "is_reattr"


    def ex_date_country, do: "date_country"
    def ex_mediasource_campaign, do: "mediasource_campaign"


###########################################################################
### 数据类型定义
###########################################################################
    def data_type_install, do: "install"
    def data_type_reinstall, do: "reinstall"
    def data_type_daily_report, do: "daily-report"

    def data_type_purchase_event, do: "purchase-event"
    def source_type_organic, do: "organic"
    def source_type_non_organic, do: "non-organic"
    def source_type_nil, do: "type-nil"

    def atom_install, do: :install 
    def atom_reinstall, do: :reinstall
    def atom_event, do: :event
    def atom_daily_report, do: :daily_report


end 