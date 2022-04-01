defmodule DB.AFInReinstallGG do 

    @doc """
        af in reinstall 表
        属性值为从AF的数据中获取的数据 (install & reinstall 数据放一表)
        额外附加数据： 
            did:   从AF的数据中提取的设备id，在此基础上建立索引，用于查找
            did_type: 依据AF的数据获取的did_type: t_adsid|t_andid|t_imei
            af_install: 是否是af安装， 如果是从af的install中获取的数据为false，否则为true
        额外计算： 
            7Day标准的重归因的id
            14Day标准的重归因的id
          
            afid_7d, afid_14d

            按照7d和14d计算是否是重装
            reinstall_7d, reinstall_14d 
            

        主键： hash: did  rang: "AppsFlyer ID"
        索引： hash: did  rang: install_time 用于快速查找uninstall前的一次数据
    """

    @table_name BI.Global.db_table_name_in_reinstall_gg()
    @index_name "install_timestamp_index"
    
    # DB.AFInReinstallGG.create_table()
    def create_table() do 
        global_secondary_index=[
            DB.HelperEx.index_config(@index_name,
                %{
                    "did" => "hash",
                    "install_timestamp" => "range"
                }, 
                10,10,
                %{projection_type: "KEYS_ONLY"})
        ]

        DB.Helper.create_table(@table_name, 
        [did: :hash, "AppsFlyer ID": :range], 
        %{ BI.Keys.ex_did => :string, BI.Keys.af_appsflyer_id => :string, BI.Keys.ex_install_timestamp => :number}, 10, 10, global_secondary_index)
    end 

    
    def put_item(item) do 
        DB.Helper.put_item(@table_name, item)
    end 

  
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

    @doc """
        用于获取小于 uninstall_time(search_install_timestamp) 时间的item
    """
    # ->  {:ok, term} | {:error, term}
    def get_items(did, num, search_install_timestamp \\ 0) do 
        opts=[
            limit: num,
            index_name: @index_name,
            expression_attribute_values: [key: did, search_install_timestamp: search_install_timestamp],
            key_condition_expression: "did = :key AND install_timestamp < :search_install_timestamp",
            scan_index_forward: false
        ]
        DB.Helper.query(num,@table_name,opts,[])
    end

    @doc """
        获取基本的数据，用于kibana的处理
    """
    # DB.AFInReinstallGG.get_item_with_media_source("81686aec-6e2e-4320-886f-4bde70a8525b","1637565522923-7304215371601446632")
    def get_item_with_media_source(did, afid) do 
        key= get_key(did, afid)
        opts= [
            consistent_read: true
        ] |> Keyword.merge(opts(:media_source))
        DB.Helper.get_item(@table_name, key, opts) 
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
           #var15
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
            "#var12" => BI.Keys.ex_is_reattr_7d,
            "#var13" => BI.Keys.ex_is_reattr_14d,
            "#var14" => BI.Keys.ex_is_reinstall,
            "#var15" => BI.Keys.ex_install_timestamp,
            },
      ]
    end 


end 