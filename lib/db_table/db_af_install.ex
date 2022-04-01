defmodule DB.AFInstall do 

    @doc """
        af install 表
        属性值为从AF的数据中获取的数据
        额外附加数据： 
            did:   从AF的数据中提取的设备id，在此基础上建立索引，用于查找
            did_type: 依据AF的数据获取的did_type: t_adsid|t_andid|t_imei
        索引： hash: did  rang: "AppsFlyer ID"
    """

    @table_name BI.Global.db_table_name_install()
    
    # DB.AFInstall.create_table()
    def create_table() do 
        DB.Helper.create_table(@table_name, 
        [did: :hash, "AppsFlyer ID": :range], 
        %{ BI.Keys.ex_did => :string, BI.Keys.af_appsflyer_id => :string}, 150, 150)
    end 


    def put_item(item) do 
        DB.Helper.put_item(@table_name, item)
    end 

    # DB.AFInstall.get_item("cc64a6a9-1e34-4352-afa4-f6a4ef2fbf98","1637567587873-866860313320110626")
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


    # DB.AFInstall.get_items_with_install_time("cc64a6a9-1e34-4352-afa4-f6a4ef2fbf981",10)
    def get_items_with_install_time(did,num) do 
         opts=[
            limit: num,
            expression_attribute_values: [key: did],
            key_condition_expression: "did = :key",
            consistent_read: true,
        ] |> Keyword.merge(opts(:install_time))
        case DB.Helper.query(num,@table_name,opts) do 
            {:error, :not_exist} -> 
                {:ok, []}
            {:error,reason}->
                {:error,reason}
            {:ok,items}-> 
                {:ok,items}
        end
    end 


    # DB.AFInstall.get_item_with_media_source("cc64a6a9-1e34-4352-afa4-f6a4ef2fbf98","1637567587873-866860313320110626")
    def get_item_with_media_source(did, afid) do 
        key= get_key(did, afid)
        opts= [
            consistent_read: true
        ] |> Keyword.merge(opts(:media_source))
        DB.Helper.get_item(@table_name, key, opts) 
    end 

    def opts(:install_time) do 
        [
        projection_expression: 
          "#var1,
           #var2,
           #var3
          ",
        expression_attribute_names: 
            %{
            "#var1" => BI.Keys.af_install_time,
            "#var2" => BI.Keys.af_appsflyer_id,
            "#var3" => BI.Keys.af_media_source
            },
      ]
    end

    def opts(:media_source) do 
        [
        projection_expression: 
          "#var1,
           #var2,
           #var3,
           #var4,
           #var5,
           #var6,
           #var7,
           #var8,
           #var9,
           #var10,
           #var11,
           #var12,
           #var13,
           #var14,
           #var15,
           #var16,
           #var17,
           #var18
          ",
        expression_attribute_names: 
            %{
            "#var1" => BI.Keys.af_media_source,
            "#var2" => BI.Keys.af_channel,
            "#var3" => BI.Keys.af_campaign,
            "#var4" => BI.Keys.af_campaign_id,
            "#var5" => BI.Keys.af_adset,
            "#var6" => BI.Keys.af_country_code,
            "#var7" => BI.Keys.af_app_version,
            "#var8" => BI.Keys.af_appsflyer_id,
            "#var9" => BI.Keys.af_install_time,
            "#var10" => BI.Keys.ex_afid_7d,
            "#var11" => BI.Keys.ex_afid_14d,
            "#var12" => BI.Keys.ex_afid_30d,
            "#var13" => BI.Keys.ex_afid_90d,
            "#var14" => BI.Keys.ex_new_in7d,
            "#var15" => BI.Keys.ex_new_in14d,
            "#var16" => BI.Keys.ex_new_in30d,
            "#var17" => BI.Keys.ex_new_in90d,
            "#var18" => BI.Keys.ex_reinstall_cnt,
            },
      ]
    end 

    
   

end 