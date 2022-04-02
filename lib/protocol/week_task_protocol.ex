defprotocol BI.WeekTask.Protocol do
  @fallback_to_any true
  def exe_cur_week_task(_any)
  def exe_last_week_task(_any)
  def exe_shift_week_task(shift)
end 