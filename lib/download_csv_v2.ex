defmodule DownloadCSV.V2 do 
require Logger

    def non_organic_install_url_template() do 
        "https://hq1.appsflyer.com/api/raw-data/export/app/{appid}/installs_report/v5?from={from}&to={to}&timezone={timezone}&maximum_rows=1000000"
    end 

    def non_organic_reinstall_url_template() do 
        "https://hq1.appsflyer.com/api/raw-data/export/app/{appid}/reinstalls/v5?from={from}&to={to}&timezone={timezone}&maximum_rows=1000000"
    end 

    def non_organic_purchase_event_url_template() do 
        "https://hq1.appsflyer.com/api/raw-data/export/app/{appid}/in_app_events_report/v5?from={from}&to={to}&timezone={timezone}&event_name=af_purchase&maximum_rows=1000000"
    end 


    def organic_install_url_template() do 
        "https://hq1.appsflyer.com/api/raw-data/export/app/{appid}/organic_installs_report/v5?from={from}&to={to}&timezone={timezone}&maximum_rows=1000000"
    end 

    def organic_reinstall_url_template() do 
        "https://hq1.appsflyer.com/api/raw-data/export/app/{appid}/reinstalls_organic/v5?from={from}&to={to}&timezone={timezone}&maximum_rows=1000000"
    end 

    def organic_purchase_event_url_template() do 
        "https://hq1.appsflyer.com/api/raw-data/export/app/{appid}/organic_in_app_events_report/v5?from={from}&to={to}&timezone={timezone}&event_name=af_purchase&maximum_rows=1000000"
   
    end 

    def daily_report_template() do 
        "https://hq1.appsflyer.com/api/agg-data/export/app/{appid}/geo_by_date_report/v5?from={from}&to={to}&timezone={timezone}"
    end 

    # DownloadCSV.V2.save_path_template
    def save_path_template() do 
        project_path= BI.Config.project_path()
        Path.join(project_path, download_path())
    end 

    def download_path() do 
        "download_data/{data_type}_{source_type}/{data_type}_{source_type}_{from}_{to}_{timezone}.csv"
    end 


    # DownloadCSV.V2.request()
    def request_test() do 
        data_type= BI.Keys.data_type_purchase_event
        source_type= BI.Keys.source_type_non_organic
        from= "2021-11-15"
        to= "2021-11-21"
        timezone= "America%2fLos_Angeles"
        request(data_type, source_type, from, to, timezone) 
    end 

    
    def request(data_type, source_type, from, to, timezone) do 
         url= get_url(data_type, source_type, from, to, timezone) 
            |> String.to_charlist()
        save_path=  get_save_path(save_path_template(), data_type, source_type, from, to, timezone) |> String.to_charlist
        Logger.info("url=#{inspect url}")
        Logger.info("save_path=#{inspect save_path}")
        if File.exists?(save_path) do 
            Logger.info("file exist, delete")
            File.rm(save_path)
        end 
        authorization = ("Bearer " <> BI.Global.api_token_v2) |> String.to_charlist
        headers= [{'accept', 'text/csv'}, {'authorization', authorization}]
        {:ok, :saved_to_file} =  DownloadCSV.download(url, headers, save_path, 0, 2, 61_000)
        Logger.error("download success! path=#{inspect save_path}")
    end 


    def get_save_path(data_type, source_type, from, to, timezone) do
        get_save_path(save_path_template(), data_type, source_type, from, to, timezone)
    end 
    def get_save_path(save_path_template, data_type, source_type, from, to, timezone) do
        timezone= URI.decode(timezone) |> String.replace("/", "_")
        save_path_template
            |> String.replace("{data_type}", data_type) 
                |> String.replace("{source_type}", source_type) 
                |> String.replace("{from}", from)
                |> String.replace("{to}", to)
                |> String.replace("{to}", to)
                |> String.replace("{timezone}", timezone)
    end 

    @doc """
        from= "2021-11-15"
        to= "2021-11-21"
        timezone= "America%2fLos_Angeles"
        DownloadCSV.V2.get_url("purchase-event","organic", from, to, timezone)
    """
    def get_url(data_type, source_type, from, to, timezone) do 
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
        tmp_url |> String.replace("{from}", from) 
                |> String.replace("{to}", to)
                |> String.replace("{timezone}", timezone)
                |> String.replace("{appid}", BI.Global.appid)
    end 
    


end 