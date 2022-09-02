defmodule WriteAFToDB do
require Logger

#  WriteAFToDB.request
def request(moulde, data_type, source_type, from, to, timezone, reinstall_strategy) do 
    moulde.request(data_type, source_type, from, to, timezone, reinstall_strategy)
end 


end 