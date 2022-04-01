defprotocol BI.Task.Protocol do
  @fallback_to_any true
  def task_process_configs(_any)
end 