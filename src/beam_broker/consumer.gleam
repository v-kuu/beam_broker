import gleam/erlang/process
import gleam/io
import gleam/otp/actor

pub type Args =
  #(process.Name(Message), process.Subject(Result(String, Nil)))

/// Start a consumer actor
///
/// Consumer is what receives events published to
/// the topic it subscribes to
pub fn start_consumer_actor(
  name: process.Name(Message),
  topic: process.Subject(Result(String, Nil)),
) {
  actor.new(topic)
  |> actor.named(name)
  |> actor.on_message(handle_message)
  |> actor.start
}

pub type Message {
  Poll
}

fn handle_message(
  topic: process.Subject(Result(String, Nil)),
  message: Message,
) {
  case message {
    Poll -> {
      case process.receive(topic, within: 10) {
        Ok(rcvd) -> {
          case rcvd {
            Ok(value) -> {
              io.println(value)
              //poll again
              actor.continue(topic)
            }
            Error(_) -> actor.continue(topic)
          }
        }
        Error(_) -> actor.continue(topic)
      }
    }
  }
}
