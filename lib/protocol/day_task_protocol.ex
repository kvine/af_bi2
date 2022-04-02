defprotocol BI.DayTask.Protocol do
  @fallback_to_any true
  def exe_cur_day_task(_any)
  def exe_last_day_task(_any)
  def exe_shift_day_task(shift)
end 