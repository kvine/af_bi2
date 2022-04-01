defmodule DB.ScanEx do
    # -> {:error, reason} | {:ok,readed_cnt}
    def scan_whole_table(table_name, items_handle_callback, decode_opts \\ []) do 
        opts = [{:limit, 200}]
        do_scan(table_name, opts, decode_opts, items_handle_callback)
    end

    # -> {:error, reason} | {:ok,readed_cnt}
    def scan_whole_table_with_last_key(table_name, items_handle_callback, last_evaluated_key, decode_opts \\ []) do 
        opts = [{:limit, 200}] |> Keyword.put(:exclusive_start_key,last_evaluated_key)
        do_scan(table_name, opts, decode_opts, items_handle_callback)
    end
    

     # -> {:error, reason} | {:ok,readed_cnt}
     defp do_scan(table_name, opts, decode_opts, items_handle_callback, readed_cnt \\ 0) do 
        response = ExAws.Dynamo.scan(table_name, opts) |> ExAws.request
        # IO.inspect(response)
        case response do 
            {:error,reason} -> 
                {:error,reason}
            {:ok,datas} ->  #Map.keys(datas) =[\"Count\", \"Items\", \"LastEvaluatedKey\", \"ScannedCount\"]"
                items= DB.Helper.decode_datas(datas,decode_opts)

                items_handle_callback.(items)

                count = Map.get(datas,"Count")
                # IO.inspect("start, keys=#{inspect Map.keys(datas)}")
                last_evaluated_key= Map.get(datas,"LastEvaluatedKey",nil)
                IO.inspect("last_evaluated_key::  #{inspect last_evaluated_key}")
                #1.update opts
                opts= Keyword.put(opts,:exclusive_start_key,last_evaluated_key)

                limit = Keyword.get(opts, :limit, 0)

                readed_cnt = readed_cnt + count
                IO.inspect("readed_cnt: #{inspect readed_cnt}")
                if(limit == count) do
                    do_scan(table_name,opts,decode_opts,items_handle_callback, readed_cnt) 
                else
                    {:ok, readed_cnt}
                end
        end
    end
end