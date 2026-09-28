import beam_broker
import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/static_supervisor as supervisor

pub fn start_topic_supervisor(
  reporters_name: process.Name(_),
) -> actor.StartResult(_) {
  let reporter_factory_supervisor =
    factory.worker_child(todo as "beam_broker.start_reporter_actor")
    |> factory.named(reporters_name)
    |> factory.supervised

  let control_plane =
    todo as "a process that commands the supervisor to spawn children"

  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(reporter_factory_supervisor)
  |> supervisor.add(control_plane)
  |> supervisor.start
}

pub type Message(element) {
  Shutdown
  Append(String)
  Read(offset: Int, limit: Int, replyto: process.Pid)
  Crash
}

fn handle_message(
  topic: List(String),
  message: Message(e),
) -> actor.Next(List(String), Message(e)) {
  case message {
    Shutdown -> actor.stop()

    Append(value) -> {
      let appended = [value, ..topic]
      actor.continue(appended)
    }

    Read(_, _, _) -> {
      case topic {
        [] -> {
          actor.continue([])
        }
        [first, ..] -> {
          actor.send(todo, todo)
          actor.continue(topic)
        }
      }
    }

    Crash -> {
      actor.stop_abnormal("Debug crash")
    }
  }
}
