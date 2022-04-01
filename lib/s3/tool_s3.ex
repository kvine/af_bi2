defmodule Tool.S3 do 
  @doc """
    提供本地文件下载和上传功能
  """
  @bucket_name Application.get_env(:af_bi, :s3_bucket_name, "nil")
  @auth :authenticated_read

  
  def get_bucket_name() do 
        @bucket_name
  end 

  # Tool.S3.download("af_origin_data/fold.html", "/Users/xs/Downloads/fold.html")
  #-> {:ok, :md5_same} | {:ok, :done} | {:error, reason}
  def download(s3_path, local_path) do 
      s3_file_md5= get_s3_file_md5(s3_path)
      if !File.exists?(local_path) ||  get_local_file_md5(local_path) != s3_file_md5 do 
          ExAws.S3.download_file(@bucket_name, s3_path, local_path) |>  ExAws.request
      else 
          {:ok, :md5_same}
      end 
  end 

  # Tool.S3.upload("/Users/xs/Downloads/fold.html", "af_origin_data/fold.html")
  def upload(local_path, s3_path) do 
      s3_file_md5= get_s3_file_md5(s3_path)
      if get_local_file_md5(local_path) == s3_file_md5 do 
          {:ok, :md5_same}
      else 
          case do_upload_rs_to_s3(local_path, s3_path, @auth)do 
            {:error, reason} -> 
              {:error, reason}
            _ -> 
              {:ok, :success}
          end 
      end 
  end 


  # -> {:error,reason} | term
  def do_upload_rs_to_s3(local_path, s3_path, auth) do
      opts=[{:acl,auth},{:content_type,get_content_type(local_path)}]
      ExAws.S3.put_object(@bucket_name, s3_path, File.read!(local_path),opts)
      |> ExAws.request!
  end


  # -> string (mime type)
  def get_content_type(path) do 
      suffix= List.last(String.split(path,"."))
      type= MIME.type(suffix)
      # Logger.info("type= #{inspect type}")
  end

  # Tool.S3.get_s3_file_md5("test/fold.html1")
  def get_s3_file_md5(path) do 
      # response = ExAws.S3.get_object(@bucket_name, path) |> ExAws.request!
      response = ExAws.S3.list_objects(@bucket_name, prefix: path) |> ExAws.stream!
      list = Enum.to_list(response)
      if list == [] do 
         nil
      else 
        JSON.decode!(List.first(list).e_tag)
      end 
  end 


  # -> string
  def get_local_file_md5(path) do 
        bin= File.read!(path) 
        md5(bin)
  end


  # -> string 
  def md5(data) do
      :erlang.md5(data)
      |> :erlang.bitstring_to_list
      |> Enum.map(&(:io_lib.format("~2.16.0b", [&1])))
      |> List.flatten
      |> :erlang.list_to_bitstring
  end




end 