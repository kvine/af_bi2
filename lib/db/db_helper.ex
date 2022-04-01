defmodule  DB.Helper do
   require Logger
   alias ExAws.Dynamo
 
   @doc "create_table"
   @spec create_table(
    table_name      :: binary,
    key_schema      :: Dynamo.key_schema,
    key_definitions :: Dynamo.key_definitions,
    read_capacity   :: pos_integer,
    write_capacity  :: pos_integer,
    global_indexes  :: [Map.t],
    local_indexes   :: [Map.t]) :: {:ok, term} | {:error, term}
    def create_table(name, key_schema, key_definitions, read_capacity, write_capacity, global_indexes \\ [], local_indexes \\ []) do
            Dynamo.create_table(name, key_schema, key_definitions, read_capacity, write_capacity, global_indexes, local_indexes) |> ExAws.request
    end

    @doc "Describe table"
    @spec describe_table(name :: binary)  :: {:ok, term} | {:error, term}
    def describe_table(name) do
        Dynamo.describe_table(name) |> ExAws.request
    end

    @doc "Put item in table"
    @spec put_item(table_name :: binary, record :: map(), opts :: Dynamo.put_item_opts) :: {:ok, term} | {:error, term}
    def put_item(name, record, opts \\ []) do
        Dynamo.put_item(name,record,opts) |> ExAws.request
    end

    #注意每次不能超过25条
  def put_items(name,items,opts \\ []) do
    {items1,items2}= Enum.split(items,25)
    result=Dynamo.batch_write_item(%{name => items1},opts) |> ExAws.request
#    Logger.debug("put_items result:#{inspect result}")
    case result do
      {:ok,map_result} ->
        map_unprocess=Map.get(map_result,"UnprocessedItems",Map.new())
#        Logger.debug("map_unprocess:#{inspect map_unprocess}")
        case map_unprocess == Map.new() do
          true ->
            case length(items2)>0 do
              true ->
                put_items(name,items2,opts)
              _->
                result
            end
          _->
            result
        end
      _->
        result
    end
  end

    @doc "Delete item in table"
    @spec delete_item(table_name :: binary, primary_key :: Dynamo.primary_key, opts :: Dynamo.delete_item_opts) :: {:ok, term} | {:error, term}
    def delete_item(name, primary_key, opts \\ []) do
            Dynamo.delete_item(name,primary_key,opts) |> ExAws.request
    end

    @spec update_item(table_name :: binary, keys :: map, opt :: Dynamo.update_item_opts) :: {:ok, term} | {:error, term}
    def update_item(table_name, keys, opt) do
            Dynamo.update_item(table_name, keys, opt) |> ExAws.request
    end

    @doc "Update item in table extend"
    @type updateAttrVaules :: [
        {attr_name :: binary, attr_value :: any}
    ]
    @spec update_item(table_name :: binary, keys :: map, updateAttrVaules :: updateAttrVaules) :: {:ok, term} | {:error, term}
    def update_item_by_attrValues(table_name,keys,updateAttrVaules) do 
        ## 剔除掉为null的数据
        updateAttrVaules= Enum.filter(updateAttrVaules,fn(x) ->
                                 {_,value}=x 
                                 is_not_null?(value)
                                 end)
        case updateAttrVaules do 
            [] -> 
                {:ok,%{}}
            _ -> 
                update_item(table_name,keys,get_update_opts(updateAttrVaules))
        end
      
    end

    @doc """
      因为数据库中更新或写数据不允许为null数据，需要做剔除
      二进制数据为<<>> 等同于 “”
      关于nil，会被编码为 "NULL"类型的数据
      所以只需处理 “”
      ***********注意是key
      {"ValidationException",
          "One or more parameter values are not valid. The update expression attempted to update a secondary index key to a value that is not supported. The AttributeValue for a key attribute cannot contain an empty string value."}}
    """
    # -> true | false
    def is_not_null?(value) do 
        value != ""
    end

    #  -> [update_expression:,expression_attribute_names:,expression_attribute_values:]
    def get_update_opts(updateAttrVaules) do 
        attrParas= get_attr_paras(updateAttrVaules)
        {tagList,attrTagList,valueTagList}=attrParas
        # get opts
        expression ="SET " <> get_expression_str(tagList)
        expression_attribute_names= Enum.into(attrTagList, %{})
        expression_attribute_values= Enum.into(valueTagList, %{})
        # return opts
        [update_expression: expression, 
        expression_attribute_names: expression_attribute_names,
        expression_attribute_values: expression_attribute_values]
    end


    # -> {{attrTag,valueTag},{attrTag,attr},{valueTag,value}}
    def get_attr_para(updateAttr,n) do 
        {attr,value}=updateAttr
        e1={"#t_attr_#{n}",":t_value_#{n}"}
        e2={"#t_attr_#{n}",attr}
        e3={String.to_atom("t_value_#{n}"),value}
        {e1,e2,e3}
    end

    # -> {[{attrTag,valueTag}],[{attrTag,attr}],[{valueTag,value}]}
    def get_attr_paras(updateAttrVaules) do 
       {{tagList,attrTagList,valueTagList},_n}=
                    List.foldl(updateAttrVaules,
                            {{[],[],[]},1},
                            fn(x,{{list1,list2,list3},acc2}) ->
                                {e1,e2,e3}=get_attr_para(x,acc2)
                                {{[e1|list1],[e2|list2],[e3|list3]},acc2+1} end)
        {Enum.reverse(tagList), Enum.reverse(attrTagList),Enum.reverse(valueTagList)}
    end


    #  -> string
    def get_expression_str(tagList) do 
        count=Enum.count(tagList)
        {expression,_n}=
            List.foldr(tagList,{"",1},fn(x,{acc1,acc2}) -> 
                                    {t_attr,t_value}=x
                                    case acc2 do
                                        ^count-> {("" <> t_attr <> "=" <> t_value ) <> acc1 ,acc2+1}
                                        _ ->  {(", " <> t_attr <> "=" <> t_value ) <> acc1 ,acc2+1}
                                    end
                                    end)
        expression                        
    end

    @doc "Get item in table "
    @spec get_item(table_name :: binary, keys :: map, opts :: Dynamo.get_item_opts, decode_opts :: [as: atom])   
    :: {:ok, term} | {:error, term}
    def get_item(table_name, keys, opts \\ [],decode_opts \\ []) do
        response = Dynamo.get_item(table_name, keys, opts) |> ExAws.request
        case response do
            {:error,_reason} ->
                    response
            {:ok,items} ->
                    # Logger.info("items=#{inspect items}")
                    if Map.has_key?(items, "Item") do
                        case decode_opts do 
                            [] -> 
                                # ExAws.Dynamo.decode_item(items["Item"]
                                # 函数有问题，此处直接用 ExAws.Dynamo.Decoder.decode 解码
                                {:ok, ExAws.Dynamo.Decoder.decode(items["Item"])}
                            _ -> 
                                {:ok, ExAws.Dynamo.decode_item(items["Item"],decode_opts)}
                        end
                    else
                        {:error, :not_exist}
                    end
        end
    end

    @doc "Get items in table "
    @spec get_item(table_name :: binary, keys :: map, opts :: Dynamo.get_item_opts, decode_opts :: [as: atom])   
    :: {:ok, [term]} | {:error, term}
    def batch_get_item(table_name, keys, opts,decode_opts \\ []) do
        do_batch_get_item(nil,table_name,keys,opts,decode_opts)
    end

    # -> :: {:ok, [term]} | {:error, term}
    def do_batch_get_item(nil,table_name, keys, opts ,decode_opts) do
        # Logger.info("start")
        {keys1,keys2}= Enum.split(keys,60)
        if keys1 != [] do 
            data= %{table_name => [{:keys,keys1}]}
            response = wrap_batch_get_item_request(Dynamo.batch_get_item(data, opts), opts, table_name) |> ExAws.request
            case response do
                {:error,_reason} ->
                    response
                {:ok,datas} ->
                    items= decode_batch_datas(datas,table_name,decode_opts)
                    if keys2 != [] do 
                        last_result= %{ items: items}
                        do_batch_get_item(last_result,table_name, keys2, opts ,decode_opts)
                    else 
                        {:ok,items}
                    end
            end 
        else 
            {:ok,[]}
        end
    end
    def do_batch_get_item(last_result,table_name, keys, opts ,decode_opts) do
        Logger.info("page...")
        {keys1,keys2}= Enum.split(keys,60)
        if keys1 != [] do 
            data= %{table_name => [{:keys,keys1}]}
            response = wrap_batch_get_item_request(Dynamo.batch_get_item(data, opts), opts, table_name) |> ExAws.request
            case response do
                {:error,_reason} ->
                    response
                {:ok,datas} ->
                    items= decode_batch_datas(datas,table_name,decode_opts)
                    items=  Enum.concat(items, last_result.items)
                    if keys2 != [] do 
                        last_result= %{ items: items}
                        do_batch_get_item(last_result,table_name, keys2, opts ,decode_opts)
                    else 
                        {:ok,items}
                    end
            end 
        else 
            {:ok,last_result.items}
        end
    end


    # -> []
    def decode_batch_datas(datas,table_name,decode_opts) do 
        if Map.has_key?(datas["Responses"],table_name) do 
            items= Map.get(datas["Responses"],table_name)
            decode_items= for i <- items do 
                            case decode_opts do 
                                [] -> 
                                    ExAws.Dynamo.Decoder.decode(i)
                                _ -> 
                                    ExAws.Dynamo.decode_item(i,decode_opts)
                            end
                        end
            decode_items
        else
            []
        end
    end
        
    @doc """
        因为dynamo中的batch_get_item不支持 projection_expression, 这里对请求进行一个包装处理
        可以直接修改dynamo.ex这个库中的batch_get_item函数，对dynamized_table_query 添加 opts
        "   opts= build_opts(opts)
            request_items = data
            |> Enum.reduce(%{}, fn {table_name, table_query}, query ->
            keys = table_query[:keys]
            |> Enum.map(&encode_values/1)

            dynamized_table_query = table_query
            ...
            |> Map.merge(opts)

            Map.put(query, table_name, dynamized_table_query)
            end)"
        这里简单的做在外层实现
        注意这里的batch处理只针对单个表

        ###
        data= %{"golf_user" => [{:keys,[[id: "test"], [id: "10973314559845"]]}]}
        opts= [projection_expression: "id, coin, #name1", expression_attribute_names: %{"#name1" => "name"}]
        request= ExAws.Dynamo.batch_get_item(data, opts) 

        DB.Helper.wrap_batch_get_item_request(request, [], "golf_user")
        DB.Helper.wrap_batch_get_item_request(request, [projection_expression: "id, coin, #name1"], "golf_user")
    """
    def wrap_batch_get_item_request(request, opts, table_name) do 
        case  Keyword.fetch(opts, :projection_expression)  do 
            :error -> 
                request
            {:ok, values} -> 
                data= Map.get(request, :data)
                opts_map= Map.delete(data, "RequestItems")
                table_data= data["RequestItems"][table_name]
                table_data= Map.merge(table_data, opts_map)
                request_items= Map.put(data["RequestItems"], table_name, table_data)

                # data= %{
                #     "RequestItems" => request_items
                # }
                data= Map.put(data,"RequestItems", request_items )
                r = Map.put(request, :data, data )
                Logger.info("r= #{inspect r}")
                r
        end
    end

    @doc """
        check item exist in table
    """
    @spec is_item_exist?(table_name :: binary, keys :: map) :: boolean() | {:error,term}
    def is_item_exist?(table_name, keys, opts \\ []) do
        response = Dynamo.get_item(table_name, keys, opts) |> ExAws.request
        case response do
            {:error,reason} ->
                    {:error,reason}
            {:ok,items} ->
                    if Map.has_key?(items, "Item") do
                        true
                    else
                        false
                    end
        end
    end

    @doc """
        query items 
    """
     @spec get_item(num :: pos_integer,able_name :: binary, opts :: Dynamo.query_opts, decode_opts :: [as: atom]) 
       :: {:ok, term} | {:error, term}
    def query(num,table_name, opts, decode_opts \\ []) do 
            opts= safe_query_or_scan_opts(opts)
            do_query_or_scan(nil,num,table_name,opts,decode_opts,&Dynamo.query/2)
    end

    @doc """
        scan items 
    """
    @spec scan(num :: pos_integer, table_name :: binary, opts :: Dynamo.scan_opts, decode_opts :: [as: atom]) 
    :: {:ok, term} | {:error, term}
    def scan(num,table_name, opts, decode_opts \\ []) do 
        opts= safe_query_or_scan_opts(opts)
        do_query_or_scan(nil,num,table_name,opts,decode_opts,&Dynamo.scan/2)
    end

    # -> opts
    def safe_query_or_scan_opts(opts) do 
        case Keyword.get(opts,:limit) do 
            nil -> 
                opts
            num -> 
                if num > 100 do 
                    Keyword.put(opts,:limit, 100)
                else 
                    opts
                end
        end
    end

     # -> {:error, reason} | {:ok,items}
     defp do_query_or_scan(nil, num, table_name,opts,decode_opts, handle_fun) do 
        # response = Dynamo.query(table_name, opts) |> ExAws.request
        response = handle_fun.(table_name, opts) |> ExAws.request
        # IO.inspect("start")
        # IO.inspect(response)
        case response do 
            {:error,reason} -> 
                {:error,reason}
            {:ok,datas} ->
                items= decode_datas(datas,decode_opts)
                count= Map.get(datas,"Count")
                # IO.inspect("start, datas=#{inspect datas}", limit: :infinity)
                last_evaluated_key= Map.get(datas,"LastEvaluatedKey",nil)
                last_result=%{
                    items: items,
                    count: count,
                    last_evaluated_key: last_evaluated_key
                }
                do_query_or_scan(last_result,num,table_name,opts,decode_opts,handle_fun) 
        end
    end
    defp do_query_or_scan(last_result,num,table_name,opts,decode_opts,handle_fun) do 
        %{items: items, count: count, last_evaluated_key: last_evaluated_key}= last_result
        need_page=  count < num  and last_evaluated_key != nil
        if !need_page do 
            if items == [] do 
                {:error,:not_exist}
            else 
                {l1,_l2}= Enum.split(items,num)
                {:ok,l1}
            end
        else
            ## page 
            #1.update opts
            opts= Keyword.put(opts,:exclusive_start_key,last_evaluated_key)
            # response = Dynamo.query(table_name, opts) |> ExAws.request
            response = handle_fun.(table_name, opts) |> ExAws.request
            IO.inspect("page....")
            # IO.inspect("page....#{inspect opts}")
            # IO.inspect(response)
            case response do 
                {:error,reason} -> 
                    {:error,reason}
                {:ok,datas} ->
                    items1= decode_datas(datas,decode_opts)
                    count1= Map.get(datas,"Count")
                    last_evaluated_key1= Map.get(datas,"LastEvaluatedKey",nil)
                    items= Enum.concat(items,items1)
                    count= last_result.count + count1
                    last_result=%{
                        items: items,
                        count: count,
                        last_evaluated_key: last_evaluated_key1
                    }
                    do_query_or_scan(last_result,num,table_name,opts,decode_opts,handle_fun)
            end 
        end
    end


    # -> items
    def decode_datas(datas,decode_opts \\[]) do 
        if Map.has_key?(datas, "Items") do
            case decode_opts do 
                [] -> 
                    for item <- datas["Items"] do 
                        ExAws.Dynamo.Decoder.decode(item) 
                    end
                _ -> 
                    for item <- datas["Items"] do 
                        ExAws.Dynamo.decode_item(item,decode_opts)
                    end
            end
        else
            []
        end
    end 


    
    @spec batch_delete(table_name :: binary, keys :: [Dynamo.primary_key], opts :: Dynamo.batch_write_item_opts) 
    :: {:ok, term} | {:error, term}
    def batch_delete(table_name,keys,opts \\ []) do 
            write_items= for i<- keys do 
                [delete_request: [key: i]]
            end
            do_batch_delete(table_name,write_items,opts,{:ok,%{}})
    end

    
    def do_batch_delete(_table_name,[],_opts,result) do 
            result
    end
    def do_batch_delete(table_name,write_items,opts,result) do
            case result do
                {:error,reason} -> 
                    {:error,reason}
                {:ok,_} -> 
                    #每次只能调用25个
                    {l1,l2}= Enum.split(write_items,25)
                    data= %{table_name => l1}
                    result= ExAws.Dynamo.batch_write_item(data,opts) |> ExAws.request
                    do_batch_delete(table_name,l2,opts,result)
            end
    end


    
# ////////////////////////////////////// test //////////////////////////////////////////////////////
   @table_name "golf_user_test"

    def test_create_table() do
        create_table(@table_name, [id: :hash], %{"id" => :string}, 1, 1)
    end

    def test_update(id) do 
        keys=[id: id]
        updateAttrVaules=[{"name","t"},{"score",101}]
        update_item_by_attrValues(@table_name,keys,updateAttrVaules)
    end

    def test_get_item(id) do 
        get_item(@table_name,[id: id])
    end

    def test_update_name(id, name) do
        opt = [ update_expression: "SET #n = :n_val", 
                expression_attribute_names: %{"#n" => "name"},
                expression_attribute_values: %{n_val: name}]
         update_item(@table_name, [{:id,id}], opt)
    end

    def test_init_new(id, token, name) do
        opt = [update_expression: "SET #t = :t_val,#n = :n_val", 
        expression_attribute_names: %{"#t" => "token", "#n" => "name"},
        expression_attribute_values: %{t_val: token, n_val: name}]
        update_item(@table_name, %{id: id}, opt)
    end

    # DB.Helper.test_add("111","123",100,100)
    def test_add(id,user_id,_score,add_score) do 
        opt = [update_expression: "SET #t= #t + :t_val", 
        expression_attribute_names: %{"#t" => "score"},
        expression_attribute_values: %{t_val: add_score}]
        update_item("golf_world_ldbd_test", %{id: id, user_id: user_id}, opt)
    end

   
# ////////////////////////////////////// test //////////////////////////////////////////////////////

end