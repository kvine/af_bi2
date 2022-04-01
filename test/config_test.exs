defmodule ConfigTest do 
  use ExUnit.Case
  doctest BI.Config

  setup_all do
    {:ok, module1: BI.Config, module2: BI.ConfigOld}
  end

  test "config", state do
    # assert state[:module1].is_test == state[:module2].is_test
    # assert state[:module1].timezone == state[:module2].timezone
    assert dotest(state[:module1], state[:module2], :is_test)
    assert dotest(state[:module1], state[:module2], :timezone)
    assert dotest(state[:module1], state[:module2], :appid)
    assert dotest(state[:module1], state[:module2], :es_appid_normal)
    assert dotest(state[:module1], state[:module2], :es_appid_test)
    assert dotest(state[:module1], state[:module2], :event_prefix)

    assert dotest(state[:module1], state[:module2], :db_table_name_install)
    assert dotest(state[:module1], state[:module2], :db_table_name_reinstall)
    assert dotest(state[:module1], state[:module2], :db_table_name_purchase_event)
    assert dotest(state[:module1], state[:module2], :db_table_name_write_af_to_db_flag)
    assert dotest(state[:module1], state[:module2], :db_table_name_write_af_to_es_flag)
    assert dotest(state[:module1], state[:module2], :db_table_name_reinstall_gg)
    assert dotest(state[:module1], state[:module2], :db_table_name_bq_uninstall)
    assert dotest(state[:module1], state[:module2], :db_table_name_task)
    assert dotest(state[:module1], state[:module2], :db_table_name_in_reinstall_gg)


    assert dotest(state[:module1], state[:module2], :bq_table_prefix)
    assert dotest(state[:module1], state[:module2], :bq_project_id)
    assert dotest(state[:module1], state[:module2], :project_name)
    assert dotest(state[:module1], state[:module2], :project_path)
  end

  def dotest(module1, module2, fun) do
      apply(module1, fun, []) ==  apply(module2, fun, [])
  end 

  
  test "flag" do 
    #cur
    assert WriteAFToDB.Cur.db_flag_pre_config(:normal) == WriteAFToDB.Cur.db_flag_pre_config_old(:normal)
    assert WriteAFToDB.Cur.db_flag_pre_config(:gg) == WriteAFToDB.Cur.db_flag_pre_config_old(:gg)

    assert WriteAFToES.Cur.db_flag_pre_config(:normal) == WriteAFToES.Cur.db_flag_pre_config_old(:normal)
    assert WriteAFToES.Cur.db_flag_pre_config(:gg) == WriteAFToES.Cur.db_flag_pre_config_old(:gg)
    
    #gg
    assert WriteAFToDB.GG.db_flag_pre_config(:normal) == WriteAFToDB.GG.db_flag_pre_config_old(:normal)
    assert WriteAFToDB.GG.db_flag_pre_config(:gg) == WriteAFToDB.GG.db_flag_pre_config_old(:gg)

    assert WriteAFToES.GG.db_flag_pre_config(:normal) == WriteAFToES.GG.db_flag_pre_config_old(:normal)
    assert WriteAFToES.GG.db_flag_pre_config(:gg) == WriteAFToES.GG.db_flag_pre_config_old(:gg)

  end 


  test "task" do 
    assert Game.TaskProcessHelper.confings() == Game.TaskProcessHelper.confings_old() 

  end 







end 