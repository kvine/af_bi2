defmodule DB.AFReinstall do 

    @doc """
        af reinstall 表
        属性值为从AF的数据中获取的数据
        额外附加数据： 
            did:   从AF的数据中提取的设备id，在此基础上建立索引，用于查找
            did_type: 依据AF的数据获取的did_type: t_adsid|t_andid|t_imei
        额外计算： 
            7Day标准的重归因的id
            14Day标准的重归因的id
            30Day标准的重归因的id
            90Day标准的重归因的id
            afid_7d, afid_14d, afid_30d, afid_90d

        索引： hash: did  rang: "AppsFlyer ID"
    """

    @table_name BI.Global.db_table_name_reinstall()
    
    # DB.AFReinstall.create_table()
    def create_table() do 
        DB.Helper.create_table(@table_name, 
        [did: :hash, "AppsFlyer ID": :range], 
        %{ BI.Keys.ex_did => :string, BI.Keys.af_appsflyer_id => :string}, 150, 150)
    end 

    
    def put_item(item) do 
        DB.Helper.put_item(@table_name, item)
    end 

    # DB.AFReinstall.get_item("cc64a6a9-1e34-4352-afa4-f6a4ef2fbf98","1637567587873-866860313320110626")
    def get_item(did, afid) do 
        key= get_key(did, afid)
        DB.Helper.get_item(@table_name, key, []) 
    end 

    def get_key(did, afid) do 
        %{
            BI.Keys.ex_did => did, 
            BI.Keys.af_appsflyer_id => afid
        }
    end 

    # DB.AFReinstall.get_items_with_install_time("e7cc59b2-aa77-435d-8208-c48f5e21b132",10)
    def get_items_with_install_time(did,num) do 
         opts=[
            limit: num,
            expression_attribute_values: [key: did],
            key_condition_expression: "did = :key",
            consistent_read: true
        ] |> Keyword.merge( DB.AFInstall.opts(:install_time))
        case DB.Helper.query(num,@table_name,opts) do 
            {:error, :not_exist} -> 
                {:ok, []}
            {:error,reason}->
                {:error,reason}
            {:ok,items}-> 
                {:ok,items}
        end
    end    



    # DB.AFReinstall.get_item_with_media_source("81686aec-6e2e-4320-886f-4bde70a8525b","1637565522923-7304215371601446632")
    def get_item_with_media_source(did, afid) do 
        key= get_key(did, afid)
        opts= [
             consistent_read: true
        ] |> Keyword.merge(DB.AFInstall.opts(:media_source))
        DB.Helper.get_item(@table_name, key, opts) 
    end 


end 