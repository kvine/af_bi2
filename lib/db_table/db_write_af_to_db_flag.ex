defmodule DB.WriteAFToDBFlag do 

    @doc """
        用于处理是否已经写入数据库的判断
        如果为普通数据：
            hash: type__did__afid__(event_time)
            type: 0 表示的是 install， 1表示的是reinstall， 2表示的是event
        如果为聚合数据： 
            hash: date_country__mediasource_campaign

    """

    @table_name  BI.Global.db_table_name_write_af_to_db_flag
    
    # DB.WriteAFToDBFlag.create_table()
    def create_table() do 
        DB.Helper.create_table(@table_name, 
        [comb_id: :hash], 
        %{"comb_id" => :string}, 150, 150)
    end 

    
    def put_item(item) do 
        DB.Helper.put_item(@table_name, item)
    end 

    # DB.WriteAFToDBFlag.get_item("cc64a6a9-1e34-4352-afa4-f6a4ef2fbf98","1637567587873-866860313320110626")
    def get_item(comb_id) do 
        DB.Helper.get_item(@table_name, [comb_id: comb_id], []) 
    end 
   

    # -> true | false | {:error,reason}
    def is_exist?(comb_id) do
      DB.Helper.is_item_exist?(@table_name, [comb_id: comb_id], [consistent_read: true])
    end



end 