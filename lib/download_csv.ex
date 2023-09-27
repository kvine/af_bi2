defmodule DownloadCSV do 
require Logger

    def request(data_type, source_type, from, to, timezone) do 
        use_af_api_token_v2= Application.get_env(:af_bi, :use_af_api_token_v2, true)
        if use_af_api_token_v2 do 
            DownloadCSV.V2.request(data_type, source_type, from, to, timezone)
        else 
            DownloadCSV.V1.request(data_type, source_type, from, to, timezone)
        end 
   end 

   
    @doc """
        下载文件，如果下载失败，可再尝试下载，最多下载 max_download_cnt
        为了防止尝试过多，一般设置  max_download_cnt 为2次
        sleep_time_mills 时间设置为61s  61_000, 主要考虑到 kibana的后台拉取数据可能有时间限制
    """
    def download(url, headers, save_path, download_cnt, max_download_cnt, sleep_time_mills) do 
        case :httpc.request(:get, {url, headers}, [], [stream: save_path ]) do 
            {:ok, :saved_to_file} -> 
                {:ok, :saved_to_file}
            {:ok, result} -> 
                if download_cnt < max_download_cnt do 
                    Logger.error("download error: ok not match, result=#{inspect result}, wait #{inspect sleep_time_mills} to next download: #{inspect download_cnt + 1 }")
                    Process.sleep(sleep_time_mills)
                    download(url, headers, save_path, download_cnt + 1, max_download_cnt, sleep_time_mills)
                else 
                    Logger.error("download error: ok but not match , result=#{inspect result}")
                    {:ok, result} 
                end 
            {:error, reason} -> 
                if download_cnt < max_download_cnt do 
                    Logger.error("download error, reason=#{inspect reason}, wait #{inspect sleep_time_mills} to next download: #{inspect download_cnt + 1 }")
                    Process.sleep(sleep_time_mills)
                    download(url, headers, save_path, download_cnt + 1, max_download_cnt, sleep_time_mills)
                else 
                    Logger.error("download error, reason=#{inspect reason}")
                    {:error, reason}
                end 
        end 
    end 


     # DownloadCSV.save_path_template
     def save_path_template() do 
        project_path= BI.Config.project_path()
        Path.join(project_path, download_path())
    end 

    def download_path() do 
        "download_data/{data_type}_{source_type}/{data_type}_{source_type}_{from}_{to}_{timezone}.csv"
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


end 