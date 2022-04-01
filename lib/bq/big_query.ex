defmodule Bi.BigQuery do
  #
  #执行数据查询之前，需要先改环境变量
  #sudo vim ~/.bash_profile
  #export GOOGLE_APPLICATION_CREDENTIALS="xxxx/golf-master-3d-b7e08-07ff8d1cd505.json"
  #
    
  def test_sql() do
      sql = "SELECT TOP(corpus, 10) as title, COUNT(*) as unique_words
              FROM [publicdata:samples.shakespeare]"
      sync_query(sql)
  end

  defp project_id() do
    BI.Config.bq_project_id
  end

  #->{:ok, []}, {:error, info}
  def sync_query(sql) do
    try do
      sync_query(project_id(), sql)
    rescue
      _ ->
        {:error, :query_failed}
    catch
      :exit,_ ->
        {:error, :exit}
    end
  end

  #->{:ok, []}, {:error, info}
  defp sync_query(project_id, sql) do
    # Fetch access token
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    conn = GoogleApi.BigQuery.V2.Connection.new(token.token)

    # Make the API request
    query_res = GoogleApi.BigQuery.V2.Api.Jobs.bigquery_jobs_query(
      conn,
      project_id,
      [body: %GoogleApi.BigQuery.V2.Model.QueryRequest{ query: sql }]
    )

    case query_res do
      {:error, info} ->
          {:error, info}
      {:ok, response} ->
        datas = deal_v2_mode_query_res_data(response)
        {:ok, datas}
    end
  end

  #-> []
  defp deal_v2_mode_query_res_data(response) do
    rows = response.rows
    fields_name = Enum.map(response.schema.fields, fn field -> field.name end)
    
    lis_data = List.foldl(rows, [], fn row, acc -> 
        lis_cell_val = Enum.map(row.f, fn cell -> cell.v end)
        
        [Enum.zip(fields_name, lis_cell_val) | acc]
    end)

    Enum.reverse(lis_data)
  end

end