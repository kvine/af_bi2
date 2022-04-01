defprotocol BI.Config.Protocols do
  @fallback_to_any true
  def is_test(t)
end 