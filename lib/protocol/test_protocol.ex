# defmodule Test.Protocols do 
#   # Test.Protocols.t1()
#   def t1() do 
#       BI.Config.Protocols.is_test(5)
#   end 

#   # Test.Protocols.t2()
#   def t2() do 
#       BI.Flag.Protocol.db_flag_pre_config_for_db(:gg)
#   end 

# end 


#   defimpl BI.Config.Protocols, for: Any do
#       def is_test(t) do 
#           "test"
#       end 
#   end  


# defimpl BI.Flag.Protocol, for: Any do 
#       def db_flag_pre_config_for_db(reinstall_strategy) do 
#             case reinstall_strategy do 
#               :normal -> 
#                   %{
#                       install: "db1", #test
#                       reinstall: "db1",
#                       event: "db1",
#                   }
#               :gg -> 
#                   %{
#                       install: "db1",
#                       reinstall: "gg_db2", #只需要该数据做区分即可，其他的无需做区分
#                       event: "db1",
#                   }
#           end 
#       end 

#       def db_flag_pre_config_for_es(reinstall_strategy) do 
#            case reinstall_strategy do 
#               :normal -> 
#                 %{
#                       install: "es10",
#                       reinstall: "es10",
#                       event: "es13",
#                   }
#               :gg -> 
#                   %{
#                       install: "es10",
#                       reinstall: "gg_es12",
#                       event: "gg_es15",
#                   }
#           end 
#       end 


#       def db_flag_pre_config_for_db_gg(reinstall_strategy) do 
#               %{
#                 install: "newgg_db16", #test
#                 reinstall: "newgg_db16",
#                 event: "newgg_db16",
#             }
#       end 

#       def db_flag_pre_config_for_es_gg(reinstall_strategy)  do 
#             %{
#                 install: "newgg_es16", #test
#                 reinstall: "newgg_es16",
#                 event: "newgg_es16",
#             }
#       end 


#   end 


# defimpl BI.Task.Protocol, for: Any do 
#     def task_process_configs(_any) do 
#       [
#         Game.TaskProcessHelper.config(:cur_week,"1","11:05_utc8", BI.WeekTask), #周1上午10点执行本周的数据任务(周一上午10点一周的数据不完整，需要再第二天再执行一次)
#         Game.TaskProcessHelper.config(:last_week,"2","10:00_utc8", BI.WeekTask), #周2上午10点执行上周的数据任务
#         Game.TaskProcessHelper.config(:before_yesterday, "everyday", "17:00_utc8", Task.QueryRemoveWorker), #每天下午5点执行uninstall数据
#       ]
#     end

# end 