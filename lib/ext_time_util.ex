defmodule Time.Util.Ex do 
    

    @doc """
        format_time: "YYYY-MM-DD HH:MM:SS" "2021-11-21 23:53:11"

        Time.Util.Ex.convert_format_time_to_time_mills("YYYY-MM-DD HH:MM:SS","2021-11-21 23:53:11")
    """
    # -> time_mills 
    def convert_format_time_to_time_mills("YYYY-MM-DD HH:MM:SS", format_time) do 
        [y_m_d_str, h_mm_s_str]= String.split(format_time, " ")
        l1= String.split(y_m_d_str, "-")
        l2= String.split(h_mm_s_str,":")
        
        l1= Enum.map(l1, fn(x)-> String.to_integer(x) end )
        l2= Enum.map(l2, fn(x)-> String.to_integer(x) end )
        [y,m,d]= l1 
        [h,mm,s]=l2 
       
        Time.Util.date_to_mills({{y,m,d}, {h,mm,s}})
        
    end 
    
    @doc """
        format_time: "YYYY-MM-DD HH:MM:SS" "2021-11-21 23:53:11"
         ->  "2021-12-01T13:14:42.873+08:00"
        Time.Util.Ex.global_time_string("2021-11-21 23:53:11","utc-7")
    """
    def global_time_string(format_time,time_zone) do 
        mills= convert_format_time_to_time_mills("YYYY-MM-DD HH:MM:SS", format_time)
        mills= local_mills_to_global_mills(mills, time_zone)
        format_time_string(:global,mills)
    end 


    @doc """
        将时间戳转换为全局或本地格式的时间字符串
        mills= Time.Util.Ex.convert_format_time_to_time_mills("YYYY-MM-DD HH:MM:SS","2021-11-21 23:53:11")
        Time.Util.Ex.format_time_string(:global,mills)
        mills= Time.Util.Ex.local_mills_to_global_mills(mills, "utc-7")
        Time.Util.Ex.format_time_string(:local,mills)
    """
    def format_time_string(:global,mills) do 
        {{y,m,d},{hh,mm,ss}}= Time.Util.mills_to_date(mills)
        millis1 = rem(mills , 1000)
        zone = "Z"
        result = :io_lib.format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w.~3..0w~s",
            [y, m, d, hh, mm, ss, millis1,zone])
        :erlang.iolist_to_binary(result)
    end 

    def format_time_string(:local, mills) do 
        Time.Util.time_string(mills)
    end 


    @doc """
        将本地时区的时间戳转换为全局时间的时间戳
    """
    def local_mills_to_global_mills(mills, time_zone) do
        [_, offset]= String.split(time_zone, "utc")
        offset= if offset == "" do 
                    0
                else 
                    String.to_integer(offset)
                end
        mills - offset * 3600 * 1000
    end 


end