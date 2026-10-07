import beam_broker/protocol.{type BrokerMessage, type Request, type Response}
import gleam/erlang/process
import gleam/otp/actor

pub type Args {
  Args(name: process.Name(BrokerMessage))
}

type State {
  State(name: process.Name(BrokerMessage))
}

/// Start a broker actor
///
/// Broker is the middleware that passes instructions
/// from the API layer to the core program
pub fn start_actor(args: Args) {
  let state = State(name: args.name)
  actor.new(state)
  |> actor.named(args.name)
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(state: State, message: BrokerMessage) {
  case message {
    protocol.Request(request, reply_to) -> {
      case request {
        protocol.CreateTopic(name) -> todo
        protocol.SubscribeToTopic(topic) -> todo
        protocol.RegisterPublisher(topic) -> todo
        protocol.Publish(name, event) -> todo
        protocol.ListTopics -> todo
      }
    }
  }
}
