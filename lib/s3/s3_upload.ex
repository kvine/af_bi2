defmodule BI.S3.Upload do 
require Logger

    # BI.S3.Upload.upload("bi_golf", "/Users/xs/Documents/git/company/vs_team/golf-af-bi-server/golf_af_bi/download_data/purchase-event_organic/purchase-event_organic_2021-11-08_2021-11-14_America_Los_Angeles.csv")
    def upload(project_name, local_path) do 
      Tool.S3.upload(local_path, get_s3_path(project_name, local_path))
    end 

    # BI.S3.Upload.upload()
    def upload() do 
        local_paths= check_need_upload_files()
        for local_path <- local_paths do 
          Tool.S3.upload(local_path, get_s3_path(BI.S3.Config.project_name(), local_path))
        end 
    end 


    @doc """
      检查有哪些文件需要上传
    """
    # BI.S3.Upload.check_need_upload_files
    # -> []
    def check_need_upload_files() do 
        path= Path.join(BI.Config.project_path(), BI.S3.Config.local_file_parent_path)
        files= get_local_files(path)
        Enum.filter(files, fn(x) ->  
            s3_path= get_s3_path(BI.S3.Config.project_name(), x)
            s3_file_md5= Tool.S3.get_s3_file_md5(s3_path)
            local_file_md5= Tool.S3.get_local_file_md5(x)
            r= s3_file_md5 != local_file_md5
            Logger.info("r=#{inspect r}")
            r 
          end )
    end 


    # BI.S3.Upload.get_3month_ago_files
    def get_3month_ago_files() do 
          path= Path.join(BI.Config.project_path(), BI.S3.Config.local_file_parent_path)
          files= get_local_files(path)
          basenames= Enum.map(files, fn(x)-> Path.basename(x) end)
          now= Time.Util.curr_mills()
          three_month=1000* 3600* 24 * 30 * 3
          Enum.filter(files, fn(x)->  
              basename= Path.basename(x, ".csv")
              [type,source_type, date1, date2|_t]= String.split(basename, "_")
                [y,m,d]= Enum.map(String.split(date2, "-"), fn(x) ->  String.to_integer(x) end)
                Time.String
                time= Time.Util.date_to_mills({{y,m,d}, {0,0,0}})
                now - time > three_month
            end)
    end 

    @doc """
      删除前必须检查是否已经上传到s3上
    """
    # BI.S3.Upload.delete_3month_ago_files
    def delete_3month_ago_files() do 
          local_files= get_3month_ago_files()
          need_upload_files= 
            List.foldl(local_files, [], fn(x, acc) ->  
              s3_path= get_s3_path(BI.S3.Config.project_name(), x)
              s3_file_md5= Tool.S3.get_s3_file_md5(s3_path)
              local_file_md5= Tool.S3.get_local_file_md5(x)
              r= s3_file_md5 == local_file_md5
              if r do 
                File.rm(x)
                Logger.info("can rm=#{inspect x}")
                acc 
              else 
                  [x|acc]
              end 
            end)
          Logger.info("need upload first files= #{inspect need_upload_files}")
          need_upload_files
    end 

    # -> string
    def get_s3_path(project_name, local_path) do 
      [_, valid_path]= String.split(local_path, BI.S3.Config.local_file_parent_path<>"/")
      s3_path= Path.join(BI.S3.Config.s3_file_parent_path, [project_name<>"/", valid_path])
    end 


    # BI.S3.Upload.get_local_files("/Users/xs/Documents/git/company/vs_team/golf-af-bi-server/golf_af_bi/download_data/")
    def get_local_files(path) do 
      if File.dir?(path) do 
        {:ok, childs}= File.ls(path)
        ignore_files= BI.S3.Config.ignore_files
        childs= Enum.filter(childs, fn(x)-> !Enum.member?(ignore_files, x) end)
        do_get_local_files(path, childs, [], ignore_files)
      else
        [path] 
      end 
    end 


    # -> []
    def do_get_local_files(parent, [], files, ignores) do 
        files
    end 
    def do_get_local_files(parent,[h|t], files, ignores) do
        cur_path= Path.join(parent, h)
        if File.dir?(cur_path) do 
            {:ok, childs}= File.ls(cur_path)
            childs= Enum.filter(childs, fn(x)-> !Enum.member?(ignores, x) end)
            files= do_get_local_files(cur_path, childs,files, ignores)
            do_get_local_files(parent, t, files, ignores)
        else 
            files= [cur_path|files]
            do_get_local_files(parent, t, files, ignores)
        end 
    end 
    
end 