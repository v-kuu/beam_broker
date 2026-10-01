import gleam/erlang/process
import gleam/otp/actor
import gleam/string_tree

/// Start a topic actor
///
/// Topic is an event log where other actors can publish to,
/// and consume from, events. A topic is a prerequisite for
/// both publishers and consumers
///
pub fn start_topic_actor(name: process.Name(Message)) {
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
  Read(offset: Int, limit: Int, replyto: process.Subject(Result(String, Nil)))
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

    Read(_, _, replyto) -> {
      case string_tree.is_empty(topic) {
        True -> {
          actor.continue(topic)
        }
        False -> {
          actor.send(replyto, Ok(string_tree.to_string(topic)))
          actor.continue(topic)
        }
      }
    }

    Crash -> {
      actor.stop_abnormal("Debug crash")
    }
  }
}
