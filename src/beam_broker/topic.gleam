import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/static_supervisor as supervisor
import gleam/string_tree

pub fn start_topic_supervisor(
  reporters_name: process.Name(_),
) -> actor.StartResult(_) {
  let reporter_factory_supervisor =
    factory.worker_child(start_topic_actor)
    |> factory.named(reporters_name)
    |> factory.supervised

  let control_plane =
    todo as "a process that commands the supervisor to spawn children"

  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(reporter_factory_supervisor)
  |> supervisor.add(control_plane)
  |> supervisor.start
}

fn start_topic_actor(name: process.Name(Message)) {
  let assert Ok(started) =
    actor.new(string_tree.new())
    |> actor.named(name)
    |> actor.on_message(handle_message)
    |> actor.start

  Ok(started)
}

pub type Message {
  Shutdown
  Append(String)
  Read(offset: Int, limit: Int, replyto: process.Pid)
  Crash
}

fn handle_message(
  topic: string_tree.StringTree,
  message: Message,
) -> actor.Next(string_tree.StringTree, Message) {
  case message {
    Shutdown -> actor.stop()

    Append(input) -> {
      let appended = string_tree.append(topic, input)
      actor.continue(appended)
    }

    Read(_, _, _) -> {
      case string_tree.is_empty(topic) {
        True -> {
          actor.continue(topic)
        }
        False -> {
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
