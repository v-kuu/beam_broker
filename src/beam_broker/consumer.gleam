import beam_broker/protocol.{type ConsumerMessage, type TopicMessage}
import gleam/erlang/process.{type Subject}
import gleam/io
import gleam/otp/actor

pub type Args {
  Args(topic: Subject(TopicMessage))
}

type State {
  State(topic: Subject(TopicMessage), own: Subject(ConsumerMessage))
}

/// Start a consumer actor
///
/// Consumer is what receives events published to
/// the topic it subscribes to
///
pub fn start_actor(args: Args) {
  actor.new_with_initialiser(5000, fn(own) {
    Ok(
      actor.initialised(State(topic: args.topic, own: own))
      |> actor.returning(own),
    )
  })
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(state: State, message: ConsumerMessage) {
  case message {
    protocol.Poll -> {
      process.send(state.topic, protocol.Read(state.own))
      process.send_after(state.own, 5000, protocol.Poll)
      actor.continue(state)
    }
    protocol.Receive(response) -> {
      io.println(response)
      actor.continue(state)
    }
  }
}
