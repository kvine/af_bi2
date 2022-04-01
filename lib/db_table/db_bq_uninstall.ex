defmodule DB.BQUninstall do
    require Logger

    defmodule Attrs do
        defmacro advertising_id, do: :advertising_id
        defmacro event_timestamp, do: :event_timestamp
        defmacro event_date, do: :event_date
    end

    defmodule Entity do
        @table_name BI.Global.db_table_name_uninstall()
    

        require DB.BQUninstall.Attrs
        alias DB.BQUninstall.Attrs

        @derive [ExAws.Dynamo.Encodable]
        defstruct [
            Attrs.advertising_id(),
            Attrs.event_timestamp(),
            Attrs.event_date()
        ]

        # DB.BQUninstall.Entity.create_table()
        def create_table() do
            DB.Helper.create_table(@table_name, 
            [advertising_id: :hash, event_timestamp: :range], 
            %{ "advertising_id" => :string, "event_timestamp" => :number}, 15, 15)
        end

        def get_key(advertising_id, event_timestamp) do
            [advertising_id: advertising_id, event_timestamp: event_timestamp]
        end

        def init_entity(advertising_id, event_timestamp, event_date) do
            %Entity{
                Attrs.advertising_id() => advertising_id,
                Attrs.event_timestamp() => event_timestamp,
                Attrs.event_date() => event_date
            }
        end

        #-> {:ok, term} | {:error, reason}
        def add_entity(advertising_id, event_timestamp, event_date) do
            entity = init_entity(advertising_id, event_timestamp, event_date)

            DB.Helper.put_item(@table_name, entity)
        end

        #{:ok, datas} = Bi.QueryBQData.get_remove_data("2022-01-02")
        # DB.BQUninstall.Entity.batch_write_items(datas)
        def batch_write_items(datas) do
            datas = Enum.uniq(datas)
            items = Enum.map(datas, fn data -> 
                [{"device_advertising_id", device_advertising_id}, {"event_name", _}, {"event_timestamp", event_timestamp}, {"event_date", event_date}] = data
                time_stamp = String.to_integer(String.slice(event_timestamp, 0, String.length(event_timestamp)-3)) #-3 等价于除以1000取整
                DB.BQUninstall.Entity.init_entity(device_advertising_id, time_stamp, event_date)
            end)

            items_valid = Enum.filter(items, fn item -> (item.advertising_id != "" && item.event_timestamp != "") end)

            put_items = Enum.map(items_valid, fn item -> [put_request: [item: item]] end)

            DB.Helper.put_items(@table_name, put_items)
        end

        #DB.BQUninstall.Entity.get_items("ec5d1b9e-d9f9-40d9-a5f5-41b13464bd57", 1642768659431,1)
        #-> {:ok, []}, {:error, reason}
        def get_items(ad_id, search_timestamp, num) do
            opts=[
                limit: num,
                expression_attribute_values: [key: ad_id, event_timestamp: search_timestamp],
                key_condition_expression: "advertising_id = :key AND event_timestamp < :event_timestamp",
                scan_index_forward: false
            ]

            DB.Helper.query(num, @table_name, opts, [as: Entity])
        end
    end
end