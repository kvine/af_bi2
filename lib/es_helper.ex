defmodule ES.Helper do 
require Logger
    @doc """
        提供写es，http请求
    """

    
    def url,  do: Application.get_env(:af_bi, :es_url, "nil")

    #  ES.Helper.request(%{media_sourde: "fb", event: "test-2021-12-05_2021-12-15"})

    # -> {:ok, body} | {:error, reason}
    def request(data) do 
        # data= Map.put(data, "prefix", "test")

        data= Map.put(data, :appid, BI.Global.es_appid) |> Map.put(:targets, ["es"])
        headers= ["Content-Type": "text/plain"]
        body= get_req_body(data)
        # Logger.info("body=#{inspect body}")
        r= HTTPotion.post(url(),[body: body, headers: headers, timeout: 5_000])
        handle_response(r)
    end 

    def get_req_body(request) do 
        request |> JSON.encode!()
    end

    def handle_response(response) do 
        case response do 
            %HTTPotion.Response{status_code: 200, body: body} ->
                {:ok, body}
            %HTTPotion.Response{status_code: status_code,body: body} ->
                {:error,{status_code,body}}
            %HTTPotion.ErrorResponse{message: message} ->
                {:error,message}
            _ -> 
                {:error,:other_reason}
        end
    end

end 
