defmodule BI.S3.Config do 

  # def project_name, do: "bi_golf"  #test 

  def project_name, do: Atom.to_string(BI.Config.project_name())
   
  def local_file_parent_path, do: "download_data"
  def s3_file_parent_path, do: "af_origin_data"

  def ignore_files, do: [".DS_Store"]

end