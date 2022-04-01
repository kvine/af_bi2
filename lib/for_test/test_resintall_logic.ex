defmodule Test.ReinstallLogic do 

# Test.ReinstallLogic.test
    def test() do 
      organic1= %{
        BI.Keys.af_media_source => "",
        BI.Keys.af_appsflyer_id => "1",
        BI.Keys.af_install_time => Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, "2021-10-01 00:00:00")
      }

      organic2= %{
        BI.Keys.af_media_source => "",
        BI.Keys.af_appsflyer_id => "2",
        BI.Keys.af_install_time => Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, "2021-10-03 00:00:00")
      }

      organic3= %{
        BI.Keys.af_media_source => "",
        BI.Keys.af_appsflyer_id => "3",
        BI.Keys.af_install_time => Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, "2021-10-25 00:00:00")
      }

      gg1= %{
        BI.Keys.af_media_source => "gg",
        BI.Keys.af_appsflyer_id => "gg1",
        BI.Keys.af_install_time => Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, "2021-10-08 00:00:00")
      }

      gg2= %{
        BI.Keys.af_media_source => "gg",
        BI.Keys.af_appsflyer_id => "gg2",
        BI.Keys.af_install_time => Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, "2021-09-15 00:00:02")
      }

      fb1= %{
        BI.Keys.af_media_source => "fb",
        BI.Keys.af_appsflyer_id => "fb1",
        BI.Keys.af_install_time => Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, "2021-10-18 00:00:00")
      }

      fb2= %{
        BI.Keys.af_media_source => "fb",
        BI.Keys.af_appsflyer_id => "fb2",
        BI.Keys.af_install_time => Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, "2021-10-27 00:00:00")
      }

      pre_datas= [organic1, organic2, organic3, gg1, gg2, fb1, fb2]

      pre_datas= Enum.sort(pre_datas,fn(x,y)-> Map.get(x, BI.Keys.af_install_time) < Map.get(y, BI.Keys.af_install_time) end )
      install_time_mills= 
        Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, "2021-10-28 23:57:33")

      WriteAFToDB.Cur.get_reinstall_pre_day_afid(pre_datas, 7, install_time_mills)
    end 

  #Test.ReinstallLogic.test_list_empty
    def test_list_empty() do 
        pre_datas=[]
        install_time_mills= 
        Time.Util.Ex.convert_format_time_to_time_mills(BI.Keys.af_time_format, "2021-10-03 23:57:33")
        WriteAFToDB.Cur.get_reinstall_pre_day_afid(pre_datas, 7, install_time_mills)
    end 
end 