defmodule Task.QueryRemoveWorker do
    require Logger
    #任务初试状态
    def init_status() do
        %{}
    end

    #执行完任务后，返回状态数据
    #-> {%{}, ""}
    def do_task(status, task_type) do
        {date, _} = 
            case task_type do
                :last_week ->
                    BI.Common.get_week_date_range_string(0)
                :last_day  ->
                    BI.Common.get_last_day_date_string()
                :cur_day ->
                    BI.Common.get_cur_day_date_range_string()
                :before_yesterday -> 
                    BI.Common.get_day_date_range_string(-2)
            end

        case Bi.QueryBQData.get_remove_data(date) do
            {:ok, datas} ->
                case DB.BQUninstall.Entity.batch_write_items(datas) do
                    {:ok, _} ->
                        {status, TaskState.do_complete}
                    {:error, reason} ->
                        Logger.error("Task.QueryRemoveWorker batch write db failed #{inspect reason}")
                        {status, TaskState.do_failed} 
                end
            {:error, info} ->
                Logger.error("Task.QueryRemoveWorker failed #{inspect info}")
                {status, TaskState.do_failed}
        end
    end

    #Task.QueryRemoveWorker.insert_previous_data("2022-01-01","2022-02-09")
    def insert_previous_data(from, to) do
        case Bi.QueryBQData.get_remove_data(from, to) do
            {:ok, lis_datas} ->
                res = Enum.map(lis_datas, fn res_datas -> 
                    case res_datas do
                        {:ok, datas} ->
                            case DB.BQUninstall.Entity.batch_write_items(datas) do
                                {:ok, term} ->
                                    {:ok, term}
                                {:error, reason} ->
                                    {:error,reason} 
                            end
                        {:error, info} ->
                            {:error,info}
                        end
                    end)
                {:ok, res}
            {:error, info} ->
                {:error, info}
        end
    end
end