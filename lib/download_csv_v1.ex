defmodule DownloadCSV.V1 do 
require Logger

    def non_organic_install_url_template() do 
        "https://hq.appsflyer.com/export/{appid}/installs_report/v5?api_token={token}&from={from}&to={to}&timezone={timezone}&maximum_rows=1000000"
    end 

    def non_organic_reinstall_url_template() do 
        "https://hq.appsflyer.com/export/{appid}/reinstalls/v5?api_token={token}&from={from}&to={to}&timezone={timezone}&maximum_rows=1000000"
    end 

    def non_organic_purchase_event_url_template() do 
        "https://hq.appsflyer.com/export/{appid}/in_app_events_report/v5?api_token={token}&from={from}&to={to}&timezone={timezone}&event_name=af_purchase&maximum_rows=1000000"
    end 


    def organic_install_url_template() do 
        "https://hq.appsflyer.com/export/{appid}/organic_installs_report/v5?api_token={token}&from={from}&to={to}&timezone={timezone}&maximum_rows=1000000"
    end 

    def organic_reinstall_url_template() do 
        "https://hq.appsflyer.com/export/{appid}/reinstalls_organic/v5?api_token={token}&from={from}&to={to}&timezone={timezone}&maximum_rows=1000000"
    end 

    def organic_purchase_event_url_template() do 
        "https://hq.appsflyer.com/export/{appid}/organic_in_app_events_report/v5?api_token={token}&from={from}&to={to}&timezone={timezone}&event_name=af_purchase&maximum_rows=1000000"
    end 

    def daily_report_template() do 
        "https://hq.appsflyer.com/export/{appid}/geo_by_date_report/v5?api_token={token}&from={from}&to={to}&timezone={timezone}"
    end 




    # DownloadCSV.V1.request()
    def request_test() do 
        data_type= BI.Keys.data_type_purchase_event
        source_type= BI.Keys.source_type_non_organic
        from= "2021-11-15"
        to= "2021-11-21"
        timezone= "America%2fLos_Angeles"
        request(data_type, source_type, from, to, timezone)
    end 

    
    def request(data_type, source_type, from, to, timezone) do 
         url= get_url(data_type, source_type, BI.Global.api_token_v1, from, to, timezone) 
            |> String.to_charlist()
        save_path=  DownloadCSV.get_save_path(data_type, source_type, from, to, timezone) |> String.to_charlist
        Logger.info("url=#{inspect url}")
        Logger.info("save_path=#{inspect save_path}")
        if File.exists?(save_path) do 
            Logger.info("file exist, delete")
            File.rm(save_path)
        end 
        headers= []
        {:ok, :saved_to_file} =  DownloadCSV.download(url, headers, save_path, 0, 2, 61_000)
        Logger.error("download success! path=#{inspect save_path}")
    end 


    @doc """
        from= "2021-11-15"
        to= "2021-11-21"
        timezone= "America%2fLos_Angeles"
        DownloadCSV.V1.get_url("purchase-event","organic", BI.Global.api_token_v1, from, to, timezone)
    """
    def get_url(data_type, source_type, token, from, to, timezone) do 
        tmp_url= 
            cond do 
                data_type == BI.Keys.data_type_install()  -> 
                    cond do 
                        source_type == BI.Keys.source_type_organic() -> 
                            organic_install_url_template()
                        source_type == BI.Keys.source_type_non_organic() -> 
                            non_organic_install_url_template()
                    end 
                data_type == BI.Keys.data_type_reinstall()  -> 
                    cond do 
                        source_type == BI.Keys.source_type_organic() -> 
                            organic_reinstall_url_template()
                        source_type == BI.Keys.source_type_non_organic() -> 
                            non_organic_reinstall_url_template()
                    end 
                data_type == BI.Keys.data_type_purchase_event()  -> 
                    cond do 
                        source_type == BI.Keys.source_type_organic() -> 
                            organic_purchase_event_url_template()
                        source_type == BI.Keys.source_type_non_organic() -> 
                            non_organic_purchase_event_url_template()
                    end 
                data_type == BI.Keys.data_type_daily_report() ->
                    daily_report_template()
            end 
        #先给decode下，防止之前误写的格式有问题
        timezone=  URI.decode(timezone) |> URI.encode_www_form
        tmp_url |> String.replace("{token}", token) 
                |> String.replace("{from}", from) 
                |> String.replace("{to}", to)
                |> String.replace("{timezone}", timezone)
                |> String.replace("{appid}", BI.Global.appid)
    end 
    


end 