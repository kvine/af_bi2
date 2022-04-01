defprotocol BI.Flag.Protocol do
  @fallback_to_any true
  def db_flag_pre_config_for_db(reinstall_strategy)
  def db_flag_pre_config_for_es(reinstall_strategy)
  def db_flag_pre_config_for_db_gg(reinstall_strategy)
  def db_flag_pre_config_for_es_gg(reinstall_strategy)
end 