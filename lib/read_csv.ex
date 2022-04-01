
defmodule ReadCSV do 
require Logger

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