
defmodule ReadCSV do 
require Logger

    @doc """
        为了减少log服务器的请求处理，只处理有效国家的数据
        如果是内购事件数据，数据量少，不过滤
        否则，进行过滤只保留美国
    """
    def datas_filter_country_by_data_type(datas, data_type) do 
        cond do 
            data_type == BI.Keys.data_type_install  -> 
                Enum.filter(datas, fn(x)-> Map.get(x, BI.Keys.af_country_code) == "US" end)
            data_type == BI.Keys.data_type_reinstall  -> 
                Enum.filter(datas, fn(x)-> Map.get(x, BI.Keys.af_country_code) == "US" end)
            data_type == BI.Keys.data_type_purchase_event  ->
               datas
            data_type == BI.Keys.data_type_daily_report -> 
                Enum.filter(datas, fn(x)->  Map.get(x, BI.Keys.af_report_country) == "US" end)
        end 
    end 
  
################################################################################
### 聚合数据读取
################################################################################
    def read_report_data(path) do 
        datas= 
            File.stream!(path) |> 
            CSV.decode(headers: true) |>
            Enum.to_list() |>  
            Keyword.get_values(:ok) |>
            Enum.map(fn(data) -> 
                {date_country, mediasource_campaign}= get_report_data_key(data)
                data |> 
                    Map.put(BI.Keys.ex_date_country, date_country) |> 
                    Map.put(BI.Keys.ex_mediasource_campaign, mediasource_campaign)
            end )
          #过滤掉cost为N/A的数据
        datas= Enum.filter(datas, fn(x) -> Map.get(x, BI.Keys.af_reprot_cost) != "N/A" end ) 
        length1= length(datas)
        Logger.error("report data: total=#{inspect length1}")
        Enum.reverse(datas)
    end 


    # -> {date_country, mediasource_campaign}
    def get_report_data_key(data) do 
        date= Map.get(data, BI.Keys.af_report_date)
        country= Map.get(data, BI.Keys.af_report_country)
        media_source= Map.get(data, BI.Keys.af_report_media_source)
        campaign= Map.get(data, BI.Keys.af_report_campaign)
        key1= date<>"_"<>country
        key2= media_source<>"_"<>campaign
        # Logger.info("key1=#{inspect key1}, key2=#{inspect key2}")
        {key1, key2}
    end 



################################################################################
### 原始数据读取
################################################################################
    # ReadCSV.read("download_data/com.sm.golfmaster_installs_2021-11-15_2021-11-21_America_Los_Angeles.csv")
    # ReadCSV.read("download_data/purchase-event_non-organic/purchase-event_non-organic_2021-11-15_2021-11-21_America_Los_Angeles.csv")
    # l=ReadCSV.read("download_data/reinstall_non-organic/reinstall_non-organic_2021-09-06_2021-10-03_America_Los_Angeles.csv")
    # ReadCSV.read("download_data/install_non-organic/install_non-organic_2021-09-06_2021-10-03_America_Los_Angeles.csv")
    # -> []
    def read(path) do 
        datas= do_read(path)
        length1= length(datas)
        datas1= Enum.filter(datas, fn(x) -> Map.get(x, "did") != "nil" end ) 
        length2= length(datas1)
        nil_did_cnt= length1- length2
        Logger.error("total=#{inspect length1} , nil_did_cnt=#{inspect nil_did_cnt}")
        # datas
        Enum.reverse(datas)
    end 



    # ReadCSV.read_by_did_nil("download_data/install_non-organic/install_non-organic_2021-09-06_2021-10-03_America_Los_Angeles.csv")
    # ReadCSV.read_by_did_nil("download_data/install_non-organic/install_non-organic_2021-10-04_2021-10-31_America_Los_Angeles.csv")
    # l=ReadCSV.read_by_did_nil("download_data/install_non-organic/install_non-organic_2021-11-01_2021-11-28_America_Los_Angeles.csv")

    # l=ReadCSV.read_by_did_nil("download_data/install_organic/install_organic_2021-11-01_2021-11-28_America_Los_Angeles.csv")
    # l=ReadCSV.read_by_did_nil("./download_data/install_organic/install_organic_2021-11-29_2021-12-5_America_Los_Angeles.csv")
    # l=ReadCSV.read_by_did_nil("./download_data/install_organic/install_organic_2021-11-22_2021-11-28_America_Los_Angeles.csv")
    def read_by_did_nil(path) do 
        datas= do_read(path)
        datas1= Enum.filter(datas, fn(x) -> Map.get(x, "did") == "nil" end ) 
        length= length(datas1)
        Logger.error("did_nil_cnt=#{inspect length}")
        Enum.reverse(datas1)
    end 

    # l1=ReadCSV.read_by_with_date("download_data/install_organic/install_organic_2021-11-01_2021-11-28_America_Los_Angeles.csv", "2021-11-22","2021-11-29")
    # l2=ReadCSV.read_by_with_date("download_data/install_organic/install_organic_2021-11-22_2021-11-28_America_Los_Angeles.csv","2021-11-22","2021-11-29")
    def read_by_with_date(path,from, to) do 
        datas= do_read(path)
        datas1= Enum.filter(datas, fn(x) ->  
            install_time= Map.get(x, BI.Keys.af_install_time)
            install_time >= from and install_time <= to
            end ) 

        datas1= Enum.filter(datas1, fn(x) -> Map.get(x, BI.Keys.af_media_source) != "" end ) 
        Enum.reverse(datas1)
    end 



    def do_read(path) do 
        # full_path=  BI.Config.project_path() |> Path.join(path)
        File.stream!(path) |> 
        CSV.decode(headers: true) |>
        Enum.to_list() |>  
        Keyword.get_values(:ok) |>
        Enum.map(fn(data) -> 
            {did, did_type}= get_did_and_did_type(data)
            data |> 
            Map.put(BI.Keys.ex_did, did) |> 
            Map.put(BI.Keys.ex_did_type, did_type)
        end )
    end 

    def get_did_and_did_type(data) do 
        adsid= Map.get(data, BI.Keys.af_advertising_id, "")
        andid= Map.get(data,BI.Keys.af_android_id,"")
        imei= Map.get(data,BI.Keys.af_imei,"")

        if adsid != "" do 
            {adsid, BI.Keys.ex_did_type_adsid}
        else 
            if andid != "" do 
                {andid, BI.Keys.ex_did_type_andid}
            else 
                if imei != "" do 
                    {imei, BI.Keys.ex_did_type_imei}
                else 
                    {"nil","nil"}
                end 
            end 
        end 
    end 

end 