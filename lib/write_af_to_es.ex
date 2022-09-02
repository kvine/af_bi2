defmodule WriteAFToES do
require Logger

#  WriteAFToES.request
# -> {result, cnt, success_cnt}
def request(moudle, data_type, source_type, from, to, timezone, reinstall_strategy) do 
    moudle.request(data_type, source_type, from, to, timezone, reinstall_strategy)
end 


end 