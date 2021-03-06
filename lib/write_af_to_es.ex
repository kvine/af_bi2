defmodule WriteAFToES do
require Logger

#  WriteAFToES.request
# -> {result, cnt, success_cnt}
def request(moudle, data_type, source_type, from, to, timezone, reinstall_strategy) do 
    moudle.request(data_type, source_type, from, to, timezone, reinstall_strategy)
end 



# # WriteAFToES.test_request()
# def test_request(data_type, source_type, from, to, timezone, reinstall_strategy, af_id) do 
#     path= DownloadCSv.get_save_path(data_type, source_type, from, to, timezone)
#     datas= ReadCSV.read(path)
#     datas= Enum.filter(datas, fn(x)-> Map.get(x, "AppsFlyer ID") == af_id end)
#     do_request(data_type, from, to, path, datas, reinstall_strategy) 
# end 



# # #  WriteAFToES.request
# # # -> {result, cnt, success_cnt}
# # def request(data_type, source_type, from, to, timezone, reinstall_strategy) do 
# #     path= DownloadCSv.get_save_path(data_type, source_type, from, to, timezone)
# #     datas= ReadCSV.read(path)
# #     do_request(data_type, from, to, path, datas, reinstall_strategy) 
# # end 


# def request_by_did_nil(data_type, source_type, from, to, timezone, reinstall_strategy) do 
#     path= DownloadCSv.get_save_path(data_type, source_type, from, to, timezone)
#     datas= ReadCSV.read_by_did_nil(path)
#     do_request(data_type, from, to,  path, datas, reinstall_strategy) 
# end 


# # -> {result, cnt, success_cnt}
# def do_request(data_type, from, to, path, datas, reinstall_strategy) do 
#     cond do 
#         data_type == BI.Keys.data_type_install  -> 
#             es_event_name= BI.Global.es_event_install(from, to, reinstall_strategy)
#             r= write_install_data_to_es(es_event_name, datas, reinstall_strategy)
#             Logger.error("write_install_data: path=#{inspect path}, \n result1=#{inspect r}")
#             r
#         data_type == BI.Keys.data_type_reinstall  -> 
#             es_event_name= BI.Global.es_event_reinstall(from, to, reinstall_strategy)
#             r= write_reinstall_data_to_es(es_event_name, datas, reinstall_strategy)
#             Logger.error("write_reinstall_data: path=#{inspect path}, \n result1=#{inspect r}")
#             r
#         data_type == BI.Keys.data_type_purchase_event  ->
#             es_event_name= BI.Global.es_event_purchase_event(from, to, reinstall_strategy)
#             r= write_purchase_event_data_to_es(es_event_name, datas, reinstall_strategy)
#             Logger.error("write_purchase_event_data: path=#{inspect path}, \n result1=#{inspect r}")
#             r
#     end 
# end 





# # -> {:ok, term}| {:error, reason}
# def write_one_purchase_event_data_to_es(es_event_name,data, reinstall_strategy) do
#     db_flag_pre= Map.get(db_flag_pre_config(reinstall_strategy), :event)
#     write_flag_comb_id= BI.Common.get_write_flag_comb_id_key(db_flag_pre, :event, data, :es) 

#     # #test
#     # data= wrap_purchase_event_data(es_event_name,data, reinstall_strategy)
#     # Logger.error("data=#{inspect data}")
#     # {:ok, :test}
#     case DB.WriteAFToESFlag.is_exist?(write_flag_comb_id) do 
#         {:error,reason} -> 
#             {:error,reason} 
#         true -> 
#             # Logger.info("exist")
#             {:ok, :exist}
#         false -> 
#             data= wrap_purchase_event_data(es_event_name,data, reinstall_strategy)
#             case ES.Helper.request(data) do 
#                 {:error,reason} -> 
#                     {:error,reason}  
#                 {:ok, _ } ->
#                     item= %{
#                         comb_id: write_flag_comb_id,
#                         time: Time.Util.time_string()
#                     }
#                     DB.WriteAFToESFlag.put_item(item) 
#             end 
#     end 
# end 

# # -> {result, cnt, success_cnt}
# def write_purchase_event_data_to_es(es_event_name,datas, reinstall_strategy) do
#     total= length(datas)
#     List.foldl(datas, {true, 0, 0}, fn(x, {result, cnt, success_cnt}) ->  
#             Logger.info("event progress: #{inspect cnt}/#{inspect total}")
#             {result, success_cnt}= 
#                 case write_one_purchase_event_data_to_es(es_event_name,x, reinstall_strategy) do 
#                     {:ok, _} -> 
#                         {result, success_cnt + 1}
#                     _ -> 
#                         {false, success_cnt}
#                 end 
#             {result, cnt + 1, success_cnt}
#     end )
# end 


# def wrap_purchase_event_data(es_event_name,data, reinstall_strategy) do 
#      #???????????????install??????
#     revenue= String.to_float(Map.get(data, BI.Keys.af_revenue, "0"))
#     data1= wrap_install_data(es_event_name, data)  
#             |> Map.put(:revenue, revenue)
#     #???reinstall???????????????reinstall???re-attrubition
#     did=  Map.get(data, BI.Keys.ex_did)
#     afid= Map.get(data, BI.Keys.af_appsflyer_id)
#     # Logger.info("did=#{inspect did}, afid=#{inspect afid}")

#     #???????????????reinstall?????????, ?????????????????????????????????????????????
#     reinstall_db_module= BI.Global.get_reinstall_db_module(reinstall_strategy)
#     case reinstall_db_module.get_item_with_media_source(did, afid) do  
#         {:ok, db_data} -> 
#             data1= data1 |> Map.put(:is_reinstall, true) |> Map.put(:db_data, "ok")
#             re_attr_data= get_purchase_event_re_attr_data(data, db_data, did, reinstall_strategy)
#             Map.put(data1, :re_attr_data, re_attr_data)
#         {:error,:not_exist} -> 
#             data1= data1 |> Map.put(:is_reinstall, false) |> Map.put(:db_data, "errnot")
#             #????????????reinstall???????????????????????????????????????reinstall??????????????????????????????????????????????????????
#             re_attr_data= get_purchase_event_re_attr_data(data, %{}, did, reinstall_strategy)
#             Map.put(data1, :re_attr_data, re_attr_data)
#         {:error,_reason} -> 
#             data1= data1 |> Map.put(:is_reinstall, false) |> Map.put(:db_data, "erroth")
#             re_attr_data= get_purchase_event_re_attr_data(data, %{}, did, reinstall_strategy)
#             Map.put(data1, :re_attr_data, re_attr_data)
#     end 
# end 

# def get_purchase_event_re_attr_data(data, db_data, did, reinstall_strategy) do 
#     _re_attr_data=%{
#         #??????????????????????????????reinstall?????????????????????(???????????????, ????????????????????????????????????????????????????????????)
#         origin: re_attr_data(data),
#         #???2-4????????????????????????reinstall???day???????????????????????????????????????
#         d7:  get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_7d, BI.Keys.ex_new_in7d, reinstall_strategy),
#         d14: get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_14d, BI.Keys.ex_new_in14d, reinstall_strategy),
#         d30: get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_30d, BI.Keys.ex_new_in30d, reinstall_strategy),
#         d90: get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_90d, BI.Keys.ex_new_in90d, reinstall_strategy),
#     }
# end 

# def write_one_reinstall_data_to_es(es_event_name,data, reinstall_strategy) do
#     db_flag_pre= Map.get(db_flag_pre_config(reinstall_strategy), :reinstall)
#     write_flag_comb_id= BI.Common.get_write_flag_comb_id_key(db_flag_pre, :reinstall, data, :es) 
#     case DB.WriteAFToESFlag.is_exist?(write_flag_comb_id) do 
#         {:error,reason} -> 
#             {:error,reason} 
#         true -> 
#             # Logger.info("exist")
#             {:ok, :exist}
#         false -> 
#             data= wrap_reinstall_item(es_event_name,data, reinstall_strategy)
#             case ES.Helper.request(data) do 
#                 {:error,reason} -> 
#                     {:error,reason}  
#                 {:ok, _ } ->
#                     item= %{
#                         comb_id: write_flag_comb_id,
#                         time: Time.Util.time_string()
#                     }
#                     DB.WriteAFToESFlag.put_item(item) 
#             end 
#     end 
# end 


# def write_reinstall_data_to_es(es_event_name,datas, reinstall_strategy) do
#     total= length(datas)
#     List.foldl(datas, {true, 0, 0}, fn(x, {result, cnt, success_cnt}) ->  
#             Logger.info("re progress: #{inspect cnt}/#{inspect total}")
#             {result, success_cnt}= 
#                 case write_one_reinstall_data_to_es(es_event_name,x, reinstall_strategy) do 
#                     {:ok, _} -> 
#                         {result, success_cnt + 1}
#                     _ -> 
#                         {false, success_cnt}
#                 end 
#             {result, cnt + 1, success_cnt}
#     end )
# end 


# def wrap_reinstall_item(es_event_name,data, reinstall_strategy) do 
#     #???????????????install??????
#     data1= wrap_install_data(es_event_name, data)
#     #??????reinstall???????????????
#     #???reinstall???????????????reinstall???re-attrubition
#     did=  Map.get(data, BI.Keys.ex_did)
#     afid= Map.get(data, BI.Keys.af_appsflyer_id)
#     # Logger.info("did=#{inspect did}, afid=#{inspect afid}")

#     reinstall_db_module= BI.Global.get_reinstall_db_module(reinstall_strategy)

#     db_data= case  reinstall_db_module.get_item_with_media_source(did, afid) do 
#             {:error,_reason} -> 
#                 %{}
#             {:ok, db_data} -> 
#                 db_data
#             end 

#     re_attr_data=%{
#         #??????????????????????????????reinstall?????????????????????(??????????????????????????????????????????????????????????????????)
#         origin: re_attr_data(data),
#         #???2-4????????????????????????reinstall???day???????????????????????????????????????
#         d7:  get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_7d, BI.Keys.ex_new_in7d, reinstall_strategy),
#         d14: get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_14d, BI.Keys.ex_new_in14d, reinstall_strategy),
#         d30: get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_30d, BI.Keys.ex_new_in30d, reinstall_strategy),
#         d90: get_re_attr_data_by_pre_day_data(data, db_data, did,  BI.Keys.ex_afid_90d, BI.Keys.ex_new_in90d, reinstall_strategy),
#     }

#     reinstall_cnt= Map.get(db_data, BI.Keys.ex_reinstall_cnt, 0)
#     re_attr_data= re_attr_data |> Map.put(:re_cnt, reinstall_cnt )
#     Map.put(data1, :re_attr_data, re_attr_data)
# end 

# def get_re_attr_data_by_pre_day_data(data, db_data, did, day_key, new_key, reinstall_strategy) do 
#     is_new= Map.get(db_data, new_key, false)
#     afid= Map.get(db_data, day_key, "nil")
#     return_data= 
#         if afid == "self" do 
#             #????????????????????????
#             #?????????reinstall???data????????????????????????????????????event?????????data????????????????????????????????????????????????????????????db_data????????????????????????
#             # re_attr_data(data) |> Map.put(:re_attr_type, "self")
#             re_attr_data(db_data) |> Map.put(:re_attr_type, "self")
#         else 
#             if afid == "nil" do 
#                 #???????????????????????????????????????????????????????????????????????????????????????, ?????????db_data???%{}???
#                 re_attr_data(data) |>  Map.put(:re_attr_type, "nil1")
#             else 
#                 #?????????????????????????????????
#                 pre_db_data= get_pre_data(did, afid, reinstall_strategy)
#                 if pre_db_data == %{} do 
#                     re_attr_data(data) |> Map.put(:re_attr_type, "nil2")
#                 else 
#                     re_attr_data(pre_db_data) |>  Map.put(:re_attr_type, "history")
#                 end 
#             end 
#         end 
#     return_data |> Map.put(:is_new, is_new)
# end 


# def get_pre_data(did, afid, reinstall_strategy) do 
#     case DB.AFInstall.get_item_with_media_source(did, afid) do 
#         {:error,:not_exist} -> 
#             reinstall_db_module= BI.Global.get_reinstall_db_module(reinstall_strategy)
#             case reinstall_db_module.get_item_with_media_source(did, afid) do 
#                 {:ok, data} -> 
#                     data 
#                 {:error,_reason} -> 
#                     %{}
#             end 
#         {:error, _reason} -> 
#             %{}
#         {:ok, data} -> 
#             data 
#     end 
# end 


# def re_attr_data(data) do 
#     install_time= Map.get(data, BI.Keys.af_install_time)
#     install_time_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format,install_time)
#     time_zone=  BI.Timezone.get_af_timezone(install_time_mills)
#     %{
#         media_source: BI.Common.convert_to_kibana_media_source(Map.get(data, BI.Keys.af_media_source,"")),
#         channel: Map.get(data, BI.Keys.af_channel,""),
#         campaign: Map.get(data, BI.Keys.af_campaign,""),
#         adset: Map.get(data, BI.Keys.af_adset,""),
#         country: Map.get(data, BI.Keys.af_country_code,""),
#         app_ver: Map.get(data, BI.Keys.af_app_version,""),
#         afid: Map.get(data, BI.Keys.af_appsflyer_id),
#         install_time: install_time,
#         install_time_stamp: Time.Util.Ex.global_time_string(install_time, time_zone),
#     }
# end 

# # -> {:ok, term}| {:error, reason}
# def write_one_install_data_to_es(es_event_name, data, reinstall_strategy) do 
#     db_flag_pre= Map.get(db_flag_pre_config(reinstall_strategy), :install)
#     write_flag_comb_id= BI.Common.get_write_flag_comb_id_key(db_flag_pre, :install, data, :es) 
#     case DB.WriteAFToESFlag.is_exist?(write_flag_comb_id) do 
#         {:error,reason} -> 
#             {:error,reason} 
#         true -> 
#             # Logger.info("exist")
#              {:ok, :exist}
#         false -> 
#             data= wrap_install_data(es_event_name, data)
#             case ES.Helper.request(data) do 
#                 {:error,reason} -> 
#                     {:error,reason}  
#                 {:ok, _ } ->
#                     item= %{
#                         comb_id: write_flag_comb_id,
#                         time: Time.Util.time_string()
#                     }
#                     DB.WriteAFToESFlag.put_item(item) 
#             end 
#     end 
# end 

# # -> {result, cnt, success_cnt}
# def write_install_data_to_es(es_event_name, datas, reinstall_strategy) do
#     total= length(datas)
#     List.foldl(datas, {true, 0, 0}, fn(x, {result, cnt, success_cnt}) ->  
#             Logger.info("progress: #{inspect cnt}/#{inspect total}")
#             {result, success_cnt}= 
#                 case write_one_install_data_to_es(es_event_name,x, reinstall_strategy) do 
#                     {:ok, _} -> 
#                         {result, success_cnt + 1}
#                     _ -> 
#                         {false, success_cnt}
#                 end 
#             {result, cnt + 1, success_cnt}
#     end )
# end       


# @doc """

# """
# def wrap_install_data(es_event_name, data) do 
#     install_time= Map.get(data, BI.Keys.af_install_time)
#     install_time_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format,install_time)
#     time_zone=  BI.Timezone.get_af_timezone(install_time_mills)

#     event_time= Map.get(data, BI.Keys.af_event_time)
#     event_time_mills= Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format,event_time)
#     event_time_zone=  BI.Timezone.get_af_timezone(event_time_mills)

#     %{
#         event: es_event_name,
#         is_organic:  BI.Common.is_organic(Map.get(data, BI.Keys.af_media_source)),

#         did:  Map.get(data, BI.Keys.ex_did),
#         did_type: Map.get(data, BI.Keys.ex_did_type),
#         afid: Map.get(data, BI.Keys.af_appsflyer_id),
#         install_time: install_time, #????????????utc-7?????????????????????????????????utc??????
#         install_time_stamp: Time.Util.Ex.global_time_string(install_time, time_zone),
#         event_time_stamp: Time.Util.Ex.global_time_string(event_time, event_time_zone),

#         media_source: BI.Common.convert_to_kibana_media_source(Map.get(data, BI.Keys.af_media_source,"")),
#         channel: Map.get(data, BI.Keys.af_channel,""),
#         campaign: Map.get(data, BI.Keys.af_campaign,""),
#         adset: Map.get(data, BI.Keys.af_adset,""),
#         is_retarg: Map.get(data, BI.Keys.af_is_retargeting,""),
#         is_prim_attr: Map.get(data, BI.Keys.af_is_primary_attr,""),
#         is_reinstall: false,
        
#         attr_tt: Map.get(data, BI.Keys.af_attributed_touch_type),
#         region: Map.get(data, BI.Keys.af_region),
#         country: Map.get(data, BI.Keys.af_country_code),
#         state: Map.get(data, BI.Keys.af_state),
#         platform: Map.get(data, BI.Keys.af_platform),
#         app_ver: Map.get(data, BI.Keys.af_app_version),
#         os_ver: Map.get(data, BI.Keys.af_os_version),
#         attr_lookback: Map.get(data, BI.Keys.af_attrbiution_lookback,""),
#     }
# end 





# #????????????es?????????, db?????????????????????????????????????????????????????????????????????????????????????????????es
# def db_flag_pre_config(reinstall_strategy) do 
#     case reinstall_strategy do 
#         :normal -> 
#            %{
#                 install: "es10",
#                 reinstall: "es10",
#                 event: "es13",
#             }
#         :gg -> 
#             %{
#                 install: "es10",
#                 reinstall: "gg_es12",
#                 event: "gg_es15",
#             }
#     end 
# end 


end 