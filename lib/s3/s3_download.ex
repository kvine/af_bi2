defmodule BI.S3.Download do 
require Logger

# BI.S3.Download.download("2021-10-04", "2021-10-31")
  def download(from, to) do 
    download(BI.Keys.data_type_install, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone)
    download(BI.Keys.data_type_install, BI.Keys.source_type_organic, from, to, BI.Config.timezone)
    download(BI.Keys.data_type_reinstall, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone)
    download(BI.Keys.data_type_reinstall, BI.Keys.source_type_organic, from, to, BI.Config.timezone)
    download(BI.Keys.data_type_purchase_event, BI.Keys.source_type_non_organic, from, to, BI.Config.timezone)
    download(BI.Keys.data_type_purchase_event, BI.Keys.source_type_organic, from, to, BI.Config.timezone)
  end 


  def download(data_type, source_type, from, to, timezone) do 
      s3_path= DownloadCSV.get_save_path(s3_path_template(), data_type, source_type, from, to, timezone)
      local_path= DownloadCSV.get_save_path(local_path_template(), data_type, source_type, from, to, timezone) 
      Tool.S3.download(s3_path, local_path)
      Logger.info("s3_path=#{inspect s3_path}")
      Logger.info("local_path=#{inspect s3_path}")
  end 


  def s3_path_template() do 
      DownloadCSV.download_path |> 
        String.replace(BI.S3.Config.local_file_parent_path,
          Path.join(BI.S3.Config.s3_file_parent_path, BI.S3.Config.project_name))
  end 


  def local_path_template() do 
      DownloadCSV.save_path_template
  end 


end 