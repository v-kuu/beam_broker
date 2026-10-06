import beam_broker/protocol.{type ConsumerMessage, type TopicMessage}
import gleam/erlang/process
import gleam/io
import gleam/otp/actor

pub type Args {
  Args(name: process.Name(ConsumerMessage), topic: process.Name(TopicMessage))
}

/// Start a consumer actor
///
/// Consumer is what receives events published to
/// the topic it subscribes to
///
pub fn start_actor(args: Args) {
  actor.new(args)
  |> actor.named(args.name)
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(state: Args, message: ConsumerMessage) {
  let return = process.named_subject(state.name)
  case message {
    protocol.Poll -> {
      let subject = process.named_subject(state.topic)
      process.send(subject, protocol.Read(0, 0, state.name))

      process.send_after(return, 5000, protocol.Poll)
      actor.continue(state)
    }
    protocol.Receive(response) -> {
      io.println(response)
      actor.continue(state)
    }
  }
}
