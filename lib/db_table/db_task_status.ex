defmodule DB.TaskStatus do
    require Logger

    defmodule Attrs do
        defmacro create_time, do: :create_time
        defmacro start_time, do: :start_time
        defmacro end_time, do: :end_time
        defmacro state, do: :state
        defmacro task_type, do: :task_type
        defmacro week_start_date, do: :week_start_date
        defmacro week_day, do: :week_day
    end

    defmodule Entity do
        @table_name BI.Global.db_table_name_task_staus()
        @index_name_week_start_date "index-week_start_date"

        require DB.TaskStatus.Attrs
        alias DB.TaskStatus.Attrs

        @derive [ExAws.Dynamo.Encodable]
        defstruct [
            Attrs.create_time(),
            Attrs.start_time(),
            Attrs.end_time(),
            Attrs.state(),
            Attrs.task_type(),
            Attrs.week_start_date(),
            Attrs.week_day()
        ]

        def create_table() do
            index_week_start = DB.HelperEx.index_config(@index_name_week_start_date, %{"start_time" => "range", "week_start_date" => "hash"}, 10, 10, %{projection_type: "ALL"})

            DB.Helper.create_table(@table_name, 
            [create_time: :hash, start_time: :range], 
            %{ "create_time" => :string, "start_time" => :string, "week_start_date" => :string}, 15, 15, [index_week_start])
        end

        def get_key(create_time, start_time) do
            [create_time: create_time, start_time: start_time]
        end

        #-> {:ok, term} | {:error, term}
        def do_update(create_time, start_time, updateAttrs, entity) do
            attrValues = get_attrValues(updateAttrs, entity)
            key = get_key(create_time, start_time)

            DB.Helper.update_item_by_attrValues(@table_name, key, attrValues)
        end

        #-> [{attr, value}]
        def get_attrValues(updateAttrs, entity) do
            for attr <- updateAttrs do
                {attr, Map.get(entity, attr)}
            end
        end

        #-> {:ok, term} | {:error, term}
        def add_task(task) do
            {date,_} = Time.Util.mills_to_date(task.start_time_mills)
            tx_date = Timex.to_date(date)
            week_start_date_sigil = Timex.beginning_of_week(tx_date)
            {:ok, week_start_date} = Timex.format(week_start_date_sigil, "{YYYY}-{0M}-{0D}")

            entity = %Entity{
                Attrs.create_time() => task.create_time,
                Attrs.start_time() => task.start_time,
                Attrs.end_time() => task.end_time,
                Attrs.state() => task.state,
                Attrs.task_type() => task.config.task_type,
                Attrs.week_start_date() => week_start_date,
                Attrs.week_day() => task.config.week_day
            }

            DB.Helper.put_item(@table_name, entity)
        end

        #-> {:ok, term} | {:error, term}
        def update_task_status(:doing, task) do
            entity = %Entity{
                Attrs.state() => task.state
            }

            updateAttrs = [Attrs.state()]
            do_update(task.create_time, task.start_time, updateAttrs, entity)
        end

        #-> {:ok, term} | {:error, term}
        def update_task_status(:complete, task) do
            entity = %Entity{
                Attrs.state() => task.state,
                Attrs.end_time() => Time.Util.time_string(task.end_time)
            }

            updateAttrs = [Attrs.state(), Attrs.end_time()]
            do_update(task.create_time, task.start_time, updateAttrs, entity)
        end

    end
end