defmodule DB.AFPurchaseEvent do 

    @doc """
        af install 表
        属性值为从AF的数据中获取的数据
        额外附加数据： 
            did:   从AF的数据中提取的设备id，在此基础上建立索引，用于查找
            did_type: 依据AF的数据获取的did_type: t_adsid|t_andid|t_imei
        索引： hash: did  rang: "Event Time"
    """

    @table_name BI.Global.db_table_name_purchase_event()
    
    # DB.AFPurchaseEvent.create_table()
    def create_table() do 
        DB.Helper.create_table(@table_name, 
        [did: :hash, "Event Time": :range], 
        %{ BI.Keys.ex_did => :string, BI.Keys.af_event_time => :string}, 150, 150)
    end 


    def put_item(item) do 
        DB.Helper.put_item(@table_name, item)
    end 

    # DB.AFPurchaseEvent.get_item("cc64a6a9-1e34-4352-afa4-f6a4ef2fbf98","")
    def get_item(did, afid) do 
        key= get_key(did, afid)
        DB.Helper.get_item(@table_name, key, []) 
    end 

    def get_key(did, afid) do 
        %{
            BI.Keys.ex_did => did, 
            BI.Keys.af_event_time => afid
        }
    end 

   

end 