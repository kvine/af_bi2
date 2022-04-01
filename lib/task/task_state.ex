defmodule TaskState do
    def ready, do: "undo"
    def doing, do: "doing"
    def do_complete, do: "do_complete"
    def do_failed, do: "do_failed"
end