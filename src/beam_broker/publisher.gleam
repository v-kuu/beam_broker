import beam_broker/protocol.{type PublisherMessage, type TopicMessage}
import gleam/erlang/process
import gleam/otp/actor

pub type Args {
  Args(name: process.Name(PublisherMessage), topic: process.Name(TopicMessage))
}

type State {
  State(topic: process.Name(TopicMessage))
}

/// Start a publisher actor
///
/// Publisher is what pushes events to its topic
///
pub fn start_actor(args: Args) {
  let state = State(args.topic)
  actor.new(state)
  |> actor.named(args.name)
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(state: State, message: PublisherMessage) {
  case message {
    protocol.Write(input) -> {
      let subject = process.named_subject(state.topic)
      process.send(subject, protocol.Append(input))
      actor.continue(state)
    }
  }
}
