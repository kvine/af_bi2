defmodule DB.AFDailyReport do 

    @doc """
        daily report 表
        属性值为从AF的聚合报告中获取的数据
        额外附加数据： 
            date_country:   日期+国家
            media_source-campaign:  渠道+广告campaign
        索引： hash: date_country  rang: media_source-campaign
    """

    @table_name BI.Global.db_table_name_daily_report()
    
    # DB.AFDailyReport.create_table()
    def create_table() do 
        hash_key= BI.Keys.ex_date_country
        range_key= BI.Keys.ex_mediasource_campaign
        DB.Helper.create_table(@table_name, 
            %{hash_key => :hash, range_key => :range},
            %{hash_key => :string, range_key => :string}, 150, 150)
    end 

   
    def put_item(item) do 
        DB.Helper.put_item(@table_name, item)
    end 

    # DB.AFDailyReport.get_item("","")
    def get_item(date_country, mediasource_campaign) do 
        key= get_key(date_country, mediasource_campaign)
        DB.Helper.get_item(@table_name, key, []) 
    end 

    def get_key(date_country, mediasource_campaign) do 
        %{
            BI.Keys.ex_date_country => date_country, 
            BI.Keys.ex_mediasource_campaign => mediasource_campaign
        }
    end 
  
   

end 