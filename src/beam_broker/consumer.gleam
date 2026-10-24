import beam_broker/protocol.{type ConsumerMessage, type TopicMessage}
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/otp/actor

pub type Args {
  Args(
    name: String,
    reply_to: Subject(protocol.BrokerMessage),
    topic: Subject(TopicMessage),
  )
}

type State {
  State(
    topic: Subject(TopicMessage),
    own: Subject(ConsumerMessage),
    events: List(String),
    event_index: Int,
  )
}

/// Start a consumer actor
///
/// Consumer is what receives events published to
/// the topic it subscribes to
///
pub fn start_actor(args: Args) {
  let started =
    actor.new_with_initialiser(5000, fn(own) {
      Ok(
        actor.initialised(State(
          topic: args.topic,
          own: own,
          events: list.new(),
          event_index: 0,
        ))
        |> actor.returning(own),
      )
    })
    |> actor.on_message(handle_message)
    |> actor.start

  case started {
    Ok(result) -> {
      process.send(
        args.reply_to,
        protocol.RegisterConsumer(args.name, result.data),
      )
      process.send(result.data, protocol.Poll)
    }
    Error(_) -> Nil
  }
  started
}

fn handle_message(state: State, message: ConsumerMessage) {
  case message {
    protocol.Poll -> {
      case
        process.call(state.topic, 1000, fn(reply_to) {
          protocol.Read(reply_to, state.event_index)
        })
      {
        Ok(received) -> {
          let new_events = list.append(state.events, received.events)
          let new_state =
            State(..state, events: new_events, event_index: received.new_index)
          process.send_after(state.own, 1000, protocol.Poll)
          actor.continue(new_state)
        }
        Error(_) -> {
          process.send_after(state.own, 1000, protocol.Poll)
          actor.continue(state)
        }
      }
    }
    protocol.GetEvents(reply_to) -> {
      process.send(reply_to, state.events)
      actor.continue(state)
    }
    protocol.ResyncConsumer(topic) -> {
      let new_state = State(..state, topic: topic)
      actor.continue(new_state)
    }
  }
}
