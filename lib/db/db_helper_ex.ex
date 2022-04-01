defmodule  DB.HelperEx do
   require Logger


    @doc """
     获取两个map中共同存在但value不同的key
    """
    # DB.HelperEx.get_different_attrs(%{id: 1}, %{id: 2, t: 3})
    # -> list  
    def get_different_attrs(old_entity,new_entity) do 
         key_list = Map.keys(new_entity)
         Enum.reduce(key_list,[],fn x, acc -> 
            if Map.has_key?(old_entity,x) do 
                case Map.get(new_entity,x) == Map.get(old_entity,x) do 
                  true -> 
                    acc
                  false -> 
                    [x|acc]
                end
            else
              acc
            end
          end)
    end

    # DB.HelperEx.index_config("mirror_ver",[mirror_ver: :hash],10,10,%{projection_type: "KEYS_ONLY"})
    # -> map
    def index_config(index_name,key_schema,read_units, write_units,projection) do 
      %{
          index_name: index_name,
          key_schema: build_key_schema(key_schema),
          provisioned_throughput: %{
                      read_capacity_units: read_units,
                      write_capacity_units: write_units,
                      },
          projection: projection
      }
  end


  # DB.HelperEx.build_key_schema([id: 1, name: 2])
  def build_key_schema(key_schema) do
    l= Enum.map(key_schema, fn({attr, type}) ->
      %{
        "attribute_name" => attr,
        "key_type" => type |> ExAws.Utils.upcase
      }
    end)
    #必需保证hashkey在前
    Enum.sort(l,fn(a,b)-> a["key_type"] < b["key_type"] end)
  end


end
