import beam_broker/consumer
import beam_broker/protocol.{type BrokerMessage}
import beam_broker/publisher
import beam_broker/topic
import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import logging

pub type Args {
  Args(
    name: process.Name(BrokerMessage),
    topic: process.Name(
      factory.Message(topic.Args, Subject(protocol.TopicMessage)),
    ),
    consumer: process.Name(
      factory.Message(consumer.Args, Subject(protocol.ConsumerMessage)),
    ),
    publisher: process.Name(
      factory.Message(publisher.Args, Subject(protocol.PublisherMessage)),
    ),
  )
}

type State {
  State(
    topic: process.Name(
      factory.Message(topic.Args, Subject(protocol.TopicMessage)),
    ),
    consumer: process.Name(
      factory.Message(consumer.Args, Subject(protocol.ConsumerMessage)),
    ),
    publisher: process.Name(
      factory.Message(publisher.Args, Subject(protocol.PublisherMessage)),
    ),
    topics: dict.Dict(String, Subject(protocol.TopicMessage)),
    consumers: dict.Dict(String, Subject(protocol.ConsumerMessage)),
    publishers: dict.Dict(String, Subject(protocol.PublisherMessage)),
    own: Subject(BrokerMessage),
  )
}

/// Start a broker actor
///
/// Broker is the middleware that passes instructions
/// from the API layer to the core program
pub fn start_actor(args: Args) {
  actor.new_with_initialiser(5000, fn(own) {
    Ok(
      actor.initialised(State(
        topics: dict.new(),
        consumers: dict.new(),
        publishers: dict.new(),
        topic: args.topic,
        consumer: args.consumer,
        publisher: args.publisher,
        own: own,
      ))
      |> actor.returning(own),
    )
  })
  |> actor.named(args.name)
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(state: State, message: BrokerMessage) {
  case message {
    protocol.RegisterTopic(name, subject) ->
      actor.continue(register_topic(name, subject, state))
    protocol.RegisterConsumer(name, subject) ->
      actor.continue(register_consumer(name, subject, state))
    protocol.RegisterPublisher(name, subject) ->
      actor.continue(register_publisher(name, subject, state))
    protocol.ClientRequest(request) -> {
      case request {
        protocol.CreateTopic(reply_to, name) ->
          actor.continue(create_topic(name, state, reply_to))
        protocol.CreateConsumer(reply_to, topic) ->
          actor.continue(create_consumer(topic, state, reply_to))
        protocol.CreatePublisher(reply_to, topic) ->
          actor.continue(create_publisher(topic, state, reply_to))
        protocol.PublishEvent(reply_to, publisher, event) ->
          actor.continue(publish(publisher, event, state, reply_to))
        protocol.ReadEvents(reply_to, consumer) ->
          actor.continue(get_events(consumer, state, reply_to))
        protocol.ListTopics(reply_to) ->
          actor.continue(list_topics(state, reply_to))
        protocol.ListPublishers(reply_to) ->
          actor.continue(list_publishers(state, reply_to))
        protocol.ListConsumers(reply_to) ->
          actor.continue(list_consumers(state, reply_to))
      }
    }
  }
}

fn create_topic(
  name: String,
  state: State,
  reply_to: Subject(Result(Nil, protocol.CreateTopicError)),
) -> State {
  case dict.get(state.topics, name) {
    Ok(_) -> {
      process.send(reply_to, Error(protocol.TopicAlreadyExists))
      logging.log(logging.Warning, "topic already initialized: " <> name)
      state
    }
    Error(_) -> new_topic(name, state, reply_to)
  }
}

fn new_topic(
  name: String,
  state: State,
  reply_to: Subject(Result(Nil, protocol.CreateTopicError)),
) -> State {
  let supervisor = factory.get_by_name(state.topic)
  case
    factory.start_child(
      supervisor,
      topic.Args(file_path: name, reply_to: state.own),
    )
  {
    Ok(_) -> {
      process.send(reply_to, Ok(Nil))
      state
    }
    Error(_) -> {
      logging.log(logging.Error, "failed to start topic actor: " <> name)
      process.send(reply_to, Error(protocol.TopicFailedToStart))
      state
    }
  }
}

fn register_topic(
  name: String,
  subject: process.Subject(protocol.TopicMessage),
  state: State,
) -> State {
  let new_topics = dict.insert(state.topics, name, subject)
  logging.log(logging.Info, "registered topic: " <> name)
  case dict.get(state.consumers, name) {
    Ok(found) -> process.send(found, protocol.ResyncConsumer(subject))
    Error(_) -> Nil
  }
  case dict.get(state.publishers, name) {
    Ok(found) -> process.send(found, protocol.ResyncPublisher(subject))
    Error(_) -> Nil
  }
  State(..state, topics: new_topics)
}

fn create_consumer(
  topic: String,
  state: State,
  reply_to: Subject(Result(Nil, protocol.CreateConsumerError)),
) {
  case dict.get(state.topics, topic) {
    Ok(found) -> {
      case dict.get(state.consumers, topic) {
        Ok(_) -> {
          logging.log(
            logging.Warning,
            "consumer already initialized: " <> topic,
          )
          process.send(reply_to, Error(protocol.ConsumerAlreadyExists))
          state
        }
        Error(_) -> new_consumer(found, topic, state, reply_to)
      }
    }
    Error(_) -> {
      process.send(reply_to, Error(protocol.ConsumerTargetNotFound))
      logging.log(logging.Warning, "topic not found: " <> topic)
      state
    }
  }
}

fn new_consumer(
  topic: Subject(protocol.TopicMessage),
  topic_name: String,
  state: State,
  reply_to: Subject(Result(Nil, protocol.CreateConsumerError)),
) -> State {
  let supervisor = factory.get_by_name(state.consumer)
  case
    factory.start_child(
      supervisor,
      consumer.Args(name: topic_name, reply_to: state.own, topic: topic),
    )
  {
    Ok(_) -> {
      process.send(reply_to, Ok(Nil))
      state
    }
    Error(_) -> {
      logging.log(
        logging.Error,
        "failed to start consumer actor for topic: " <> topic_name,
      )
      process.send(reply_to, Error(protocol.ConsumerFailedToStart))

      state
    }
  }
}

fn register_consumer(
  name: String,
  subject: process.Subject(protocol.ConsumerMessage),
  state: State,
) -> State {
  let new_consumers = dict.insert(state.consumers, name, subject)
  logging.log(logging.Info, "registered consumer: " <> name)
  State(..state, consumers: new_consumers)
}

fn create_publisher(
  topic: String,
  state: State,
  reply_to: Subject(Result(Nil, protocol.CreatePublisherError)),
) {
  case dict.get(state.topics, topic) {
    Ok(found) ->
      case dict.get(state.publishers, topic) {
        Ok(_) -> {
          logging.log(
            logging.Warning,
            "consumer already initialized: " <> topic,
          )
          process.send(reply_to, Error(protocol.PublisherAlreadyExists))
          state
        }
        Error(_) -> new_publisher(topic, found, state, reply_to)
      }
    Error(_) -> {
      process.send(reply_to, Error(protocol.PublisherTargetNotFound))
      logging.log(logging.Warning, "topic not found: " <> topic)
      state
    }
  }
}

fn new_publisher(
  name: String,
  topic: Subject(protocol.TopicMessage),
  state: State,
  reply_to: Subject(Result(Nil, protocol.CreatePublisherError)),
) -> State {
  let supervisor = factory.get_by_name(state.publisher)
  case factory.start_child(supervisor, publisher.Args(name, topic, state.own)) {
    Ok(_) -> {
      process.send(reply_to, Ok(Nil))
      state
    }
    Error(_) -> {
      logging.log(
        logging.Error,
        "failed to initialize publisher actor for topic: " <> name,
      )
      process.send(reply_to, Error(protocol.PublisherFailedToStart))
      state
    }
  }
}

fn register_publisher(
  name: String,
  subject: Subject(protocol.PublisherMessage),
  state: State,
) -> State {
  let new_publishers = dict.insert(state.publishers, name, subject)
  logging.log(logging.Info, "registered publisher: " <> name)
  State(..state, publishers: new_publishers)
}

fn publish(
  publisher: String,
  event: String,
  state: State,
  reply_to: Subject(Result(Nil, protocol.PublishError)),
) -> State {
  case dict.get(state.publishers, publisher) {
    Ok(found) -> {
      case process.call(found, 1000, fn(own) { protocol.Write(own, event) }) {
        Ok(_) -> process.send(reply_to, Ok(Nil))
        Error(_) -> process.send(reply_to, Error(protocol.WriteFailed))
      }
      state
    }
    Error(_) -> {
      process.send(reply_to, Error(protocol.PublisherNotFound))
      logging.log(logging.Warning, "publisher not found: " <> publisher)
      state
    }
  }
}

fn get_events(
  consumer: String,
  state: State,
  reply_to: Subject(Result(List(String), protocol.ConsumeError)),
) {
  case dict.get(state.consumers, consumer) {
    Ok(found) -> {
      let events =
        process.call(found, 1000, fn(own) { protocol.GetEvents(own) })
      process.send(reply_to, Ok(events))
      state
    }
    Error(_) -> {
      process.send(reply_to, Error(protocol.ConsumerNotFound))
      logging.log(logging.Warning, "consumer not found: " <> consumer)
      state
    }
  }
}

fn list_topics(state: State, reply_to: Subject(List(String))) {
  process.send(reply_to, dict.keys(state.topics))
  state
}

fn list_publishers(state: State, reply_to: Subject(List(String))) {
  process.send(reply_to, dict.keys(state.publishers))
  state
}

fn list_consumers(state: State, reply_to: Subject(List(String))) {
  process.send(reply_to, dict.keys(state.consumers))
  state
}
