defmodule Test.DBRead do 

    @doc """
      测试读压力
      Test.DBRead.excute
    """
  def excute() do 
      table_name="golf_bi_write_af_to_db_flag"
      DB.ScanEx.scan_whole_table(table_name,&items_handle_callback/1)
  end 


  def items_handle_callback(item) do 

  end 

end 