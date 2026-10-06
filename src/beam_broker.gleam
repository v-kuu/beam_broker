import beam_broker/supervisor
import gleam/erlang/process
import logging

pub fn main() -> Nil {
  logging.configure()
  logging.log(logging.Info, "Starting beam_broker")
  case supervisor.start_orchestrator() {
    Ok(_) -> process.sleep_forever()
    Error(_) -> logging.log(logging.Error, "Failed to start orchestrator")
  }
}
