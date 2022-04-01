defmodule Bi.QueryBQData do
    use Timex
    require Logger

    def table_prefix() do
        BI.Config.bq_table_prefix
    end

    @doc """
        Bi.QueryBQData.get_remove_data("2022-01-09")
    """
    #-> {:ok, []} | {:error, info}
    def get_remove_data(date) do
        tup_date = str_date_to_tuple(date)
        sigil_date = Timex.to_date(tup_date)
        {:ok, date_1} = Timex.format(sigil_date, "{YYYY}-{0M}-{0D}")
        date = String.replace(date_1, "-", "")
        
        query_remove_data(date)
    end

    @doc """
        Bi.QueryBQData.get_remove_data("2022-01-03", "2022-01-09")
    """
    #-> {:ok,[]} | {:error,reason}
    def get_remove_data(from, to) do
        tup_from = str_date_to_tuple(from)
        tup_to = str_date_to_tuple(to)

        #{2016, 2, 29} ==>  ~D[2016-02-29]
        sigil_from = Timex.to_date(tup_from)
        sigil_to = Timex.to_date(tup_to)

        recursive_get_bq_data(sigil_from, sigil_to)
    end

    # @doc """
    #     sigil_from: ~D[2016-02-29]
    #     sigil_to: ~D[2016-02-29]
    # """
    #-> {:ok, datas} | {:error, reason}
    defp recursive_get_bq_data(sigil_from, sigil_to, acc_res \\ []) do
        {:ok, date_1} = Timex.format(sigil_from, "{YYYY}-{0M}-{0D}")
        date = String.replace(date_1, "-", "")
        case Timex.compare(sigil_from, sigil_to) do
            1 ->
                Logger.error("sigil_from > sigil_to, error")
                {:error, :invalid_sigil_from}
            0 ->
                res = query_remove_data(date)
                acc_res = [res | acc_res]
                {:ok, Enum.reverse(acc_res)}
            -1 ->
                res = query_remove_data(date)
                acc_res = [res | acc_res]

                recursive_get_bq_data(Timex.shift(sigil_from, days: 1), sigil_to, acc_res)
        end
    end

    @doc """
        {2022,1,1} = str_date_to_tuple("2022-01-01")
    """
    def str_date_to_tuple(date) do
        [s_y, s_m, s_z] = String.split(date, "-")
        {String.to_integer(s_y), String.to_integer(s_m), String.to_integer(s_z)}
    end

    #-> {:ok, []} | {:error, info}
    defp query_remove_data(date) do
        t_prefix = table_prefix()
        sql = "SELECT device.advertising_id, event_name, event_timestamp, event_date FROM [" <> t_prefix <> ".events_" <>  date <> "] where event_name = 'app_remove' LIMIT 100000"
        Bi.BigQuery.sync_query(sql)
    end
end