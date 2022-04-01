defmodule BI.Global do 
require Logger

def appid, do: BI.Config.appid

def es_appid() do 
    if BI.Config.is_test do
        BI.Config.es_appid_test
    else 
       BI.Config.es_appid_normal
    end 
end 

def db_table_name_task_staus() do
    if BI.Config.is_test do
        BI.Config.db_table_name_task<>"_test"
    else 
       BI.Config.db_table_name_task
    end 
end

def db_table_name_uninstall() do 
    BI.Config.db_table_name_bq_uninstall 
end 

def db_table_name_install() do 
    if BI.Config.is_test do
        BI.Config.db_table_name_install<>"_test"
    else 
       BI.Config.db_table_name_install
    end 
end 

def db_table_name_reinstall() do 
    if BI.Config.is_test do
        BI.Config.db_table_name_reinstall<>"_test"
    else 
       BI.Config.db_table_name_reinstall
    end 
end 

def db_table_name_reinstall_gg() do 
    if BI.Config.is_test do
        BI.Config.db_table_name_reinstall_gg<>"_test"
    else 
       BI.Config.db_table_name_reinstall_gg
    end 
end 

def db_table_name_purchase_event() do 
    if BI.Config.is_test do
        BI.Config.db_table_name_purchase_event<>"_test"
    else 
       BI.Config.db_table_name_purchase_event
    end  
end 


def db_table_name_write_af_to_db_flag() do 
    if BI.Config.is_test do
        BI.Config.db_table_name_write_af_to_db_flag<>"_test"
    else 
       BI.Config.db_table_name_write_af_to_db_flag
    end  
end 


def db_table_name_write_af_to_es_flag() do 
    if BI.Config.is_test do
        BI.Config.db_table_name_write_af_to_es_flag<>"_test"
    else 
       BI.Config.db_table_name_write_af_to_es_flag
    end 
end 

def db_table_name_in_reinstall_gg() do 
    if BI.Config.is_test do
        BI.Config.db_table_name_in_reinstall_gg<>"_test"
    else 
       BI.Config.db_table_name_in_reinstall_gg
    end 
end 


def es_event_install(from, to, strategy) do 
    #golf是默认的前缀，所以不需要附加
    name= "af_install_"
    if BI.Config.event_prefix == "golf_" do 
        wrap_es_event_name_with_reinstall_strategy(name, from, to, strategy)
    else 
        wrap_es_event_name_with_reinstall_strategy(BI.Config.event_prefix<>name, from, to, strategy)
    end 
end 

def es_event_reinstall(from, to, strategy) do 
    name= "af_reinstall_"
    if BI.Config.event_prefix == "golf_" do 
        wrap_es_event_name_with_reinstall_strategy(name, from, to, strategy)
    else 
        wrap_es_event_name_with_reinstall_strategy(BI.Config.event_prefix<>name, from, to, strategy)
    end 
end 

def es_event_purchase_event(from, to, strategy) do
    name= "af_purevent_"
    if BI.Config.event_prefix == "golf_" do 
        wrap_es_event_name_with_reinstall_strategy(name, from, to, strategy)
    else 
        wrap_es_event_name_with_reinstall_strategy(BI.Config.event_prefix<>name, from, to, strategy)
    end 
end 


def wrap_es_event_name_with_reinstall_strategy(event_name, from, to, strategy) do 
    cond do 
        strategy == :normal  ->  
            event_name<>from<>"_"<>to
        strategy == :gg -> 
            "gg_"<>event_name<>from<>"_"<>to
        strategy == :newgg -> 
            "newgg11_"<>event_name<>from<>"_"<>to
    end 
end 

# BI.Global.get_reinstall_db_module(reinstall_strategy)
def get_reinstall_db_module(reinstall_strategy) do 
    _db_moudle= case reinstall_strategy do 
        :normal ->  
            # Logger.info("normal")
            DB.AFReinstall
        :gg -> 
        # Logger.info("gg")
            DB.AFReinstallGG
    end 
end 

def one_day_mills() do 
    86400000 #   3600 * 1000 * 24
end


def api_token_v1() do 
    Application.get_env(:af_bi, :af_api_token_v1, "nil")
end 


def api_token_v2() do 
    Application.get_env(:af_bi, :af_api_token_v2, "nil")
end 




end 