defmodule BI.Config do 

#是否启用测试，开启测试后，数据库会使用测试数据库，es服务器会使用对应测试的es服务器
def is_test, do: Application.get_env(:af_bi, :is_test, "nil")

#这个时区与af的时区保持一致，每个项目可能不一样， 常见的还有 "UTC"
# 注意导出的原始报告中，如果时区是洛杉矶的时区，这个时区会变化, 冬季的时候会变成utc-8慢一个小时
def timezone, do: Application.get_env(:af_bi, :timezone, "nil")

#应用id，对应kibana上的id，与包名相同
def appid, do: Application.get_env(:af_bi, :appid, "nil")

#选择es服务器的id，部分项目共用同一个es服务器， 没有明确说明，这个id不要改
def es_appid_normal, do: Application.get_env(:af_bi, :es_appid_normal, "nil")

#选择es服务器的id，测试用，可以随意选择自己的测试es
def es_appid_test, do: Application.get_env(:af_bi, :es_appid_test, "nil")

#es服务器上事件的前缀（注意必需添加以区分数据在共同es服务器上事件数据）
def event_prefix, do: Application.get_env(:af_bi, :event_prefix, "nil")


######数据表, 不同项目需要定制#######
def db_table_name_install, do: Application.get_env(:af_bi, :table_install, "nil")
def db_table_name_reinstall, do: Application.get_env(:af_bi, :table_reinstall, "nil")
def db_table_name_purchase_event, do: Application.get_env(:af_bi, :table_purchase_event, "nil")
def db_table_name_write_af_to_db_flag, do: Application.get_env(:af_bi, :table_write_af_to_db_flag, "nil")
def db_table_name_write_af_to_es_flag, do: Application.get_env(:af_bi, :table_write_af_to_es_flag, "nil")
def db_table_name_reinstall_gg, do: Application.get_env(:af_bi, :table_reinstall_gg, "nil")
def db_table_name_bq_uninstall, do: Application.get_env(:af_bi, :table_bq_uninstall, "nil")
def db_table_name_task, do: Application.get_env(:af_bi, :table_task, "nil")
def db_table_name_in_reinstall_gg, do: Application.get_env(:af_bi, :table_install_reinstall_gg, "nil")

#####BQ###
def bq_table_prefix, do: Application.get_env(:af_bi, :bq_table_prefix, "nil")
def bq_project_id, do: Application.get_env(:af_bi, :bq_project_id, "nil")
####task###


##project path ###

# BI.Config.project_name
def project_name() do 
    Application.get_env(:af_bi, :project_name, "nil")
end 

# BI.Config.project_path
def project_path() do 
    [path,_]= project_name()
        |> :code.priv_dir()  
        |> Path.join("")
        |> String.split("/_build/") 
    path
end 
###




end 