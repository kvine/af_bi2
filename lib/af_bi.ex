defmodule AfBi do
  @moduledoc """
  Documentation for AfBi.
  """

  def start(_args, _opts) do

    {:ok, pid} = BI.Supervisor.start_link()

    {:ok,pid}
  end

end
