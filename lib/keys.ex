defmodule BI.Keys do 

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



    def data_type_install, do: "install"
    def data_type_reinstall, do: "reinstall"
    def data_type_purchase_event, do: "purchase-event"
    def source_type_organic, do: "organic"
    def source_type_non_organic, do: "non-organic"



end 