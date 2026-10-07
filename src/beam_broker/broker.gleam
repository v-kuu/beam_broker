import beam_broker/protocol.{type BrokerMessage, type Request, type Response}
import beam_broker/topic
import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory

pub type Args {
  Args(
    name: process.Name(BrokerMessage),
    topic: process.Name(
      factory.Message(topic.Args, Subject(protocol.TopicMessage)),
    ),
    consumer: String,
    publisher: String,
  )
}

type State {
  State(
    topic: process.Name(
      factory.Message(topic.Args, Subject(protocol.TopicMessage)),
    ),
    consumer: String,
    publisher: String,
    topics: dict.Dict(String, Subject(protocol.TopicMessage)),
  )
}

/// Start a broker actor
///
/// Broker is the middleware that passes instructions
/// from the API layer to the core program
pub fn start_actor(args: Args) {
  let state =
    State(
      topics: dict.new(),
      topic: args.topic,
      consumer: args.consumer,
      publisher: args.publisher,
    )
  actor.new(state)
  |> actor.named(args.name)
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(state: State, message: BrokerMessage) {
  case message {
    protocol.Request(request, reply_to) -> {
      case request {
        protocol.CreateTopic(name) -> create_topic(name, state, reply_to)
        protocol.SubscribeToTopic(topic) -> todo
        protocol.RegisterPublisher(topic) -> todo
        protocol.Publish(name, event) -> todo
        protocol.ListTopics -> todo
      }
    }
  }
  actor.continue(state)
}

fn create_topic(
  name: String,
  state: State,
  reply_to: Subject(Response),
) -> State {
  case dict.get(state.topics, name) {
    Ok(_) -> {
      process.send(reply_to, protocol.TopicAlreadyExists)
      state
    }
    Error(_) -> create_new_topic(name, state, reply_to)
  }
}

fn create_new_topic(
  name: String,
  state: State,
  reply_to: Subject(Response),
) -> State {
  let supervisor = factory.get_by_name(state.topic)
  case factory.start_child(supervisor, topic.Args(file_path: name)) {
    Ok(started) -> {
      let subject = started.data
      let new_topics = dict.insert(state.topics, name, subject)
      process.send(reply_to, protocol.TopicCreated)
      State(..state, topics: new_topics)
    }
    Error(_) -> {
      state
    }
  }
}
