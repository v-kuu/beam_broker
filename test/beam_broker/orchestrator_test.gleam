import beam_broker/broker
import beam_broker/consumer
import beam_broker/protocol.{ClientRequest}
import beam_broker/publisher
import beam_broker/topic
import gleam/erlang/process
import gleam/list
import gleam/otp/factory_supervisor as factory
import gleam/otp/static_supervisor as supervisor
import gleam/otp/supervision
import gleam/string
import gleeunit
import simplifile

pub fn main() {
  gleeunit.main()
}

pub fn child_registration_test() {
  let test_topic = "registration_test.txt"
  let _ = simplifile.delete(test_topic)
  let broker_subject = mock_orchestrator(test_topic)

  // topic
  case
    process.call(broker_subject, 1000, fn(reply_to) {
      ClientRequest(protocol.CreateTopic(reply_to, test_topic))
    })
  {
    Ok(_) -> Nil
    Error(_) -> panic as "failed to create topic"
  }
  let topics =
    process.call(broker_subject, 1000, fn(reply_to) {
      ClientRequest(protocol.ListTopics(reply_to))
    })
  assert list.length(topics) == 1
  assert list.contains(topics, test_topic)

  // publisher
  case
    process.call(broker_subject, 1000, fn(reply_to) {
      ClientRequest(protocol.CreatePublisher(reply_to, test_topic))
    })
  {
    Ok(_) -> Nil
    Error(_) -> panic as "failed to create publisher"
  }
  let publishers =
    process.call(broker_subject, 1000, fn(reply_to) {
      ClientRequest(protocol.ListPublishers(reply_to))
    })
  assert list.length(publishers) == 1
  assert list.contains(publishers, test_topic)

  // consumer
  case
    process.call(broker_subject, 1000, fn(reply_to) {
      ClientRequest(protocol.CreateConsumer(reply_to, test_topic))
    })
  {
    Ok(_) -> Nil
    Error(_) -> panic as "failed to create consumer"
  }
  let consumers =
    process.call(broker_subject, 1000, fn(reply_to) {
      ClientRequest(protocol.ListConsumers(reply_to))
    })
  assert list.length(consumers) == 1
  assert list.contains(consumers, test_topic)
  let _ = simplifile.delete(test_topic)
}

pub fn publish_read_test() {
  let test_topic = "publish_read_test.txt"
  let _ = simplifile.delete(test_topic)
  let broker_subject = mock_orchestrator(test_topic)

  case
    process.call(broker_subject, 1000, fn(reply_to) {
      ClientRequest(protocol.CreateTopic(reply_to, test_topic))
    })
  {
    Ok(_) -> Nil
    Error(_) -> panic as "failed to create topic"
  }
  case
    process.call(broker_subject, 1000, fn(reply_to) {
      ClientRequest(protocol.CreatePublisher(reply_to, test_topic))
    })
  {
    Ok(_) -> Nil
    Error(_) -> panic as "failed to create publisher"
  }
  case
    process.call(broker_subject, 1000, fn(reply_to) {
      ClientRequest(protocol.CreateConsumer(reply_to, test_topic))
    })
  {
    Ok(_) -> Nil
    Error(_) -> panic as "failed to create consumer"
  }

  let payload = "Hello world!"

  // publish
  case
    process.call(broker_subject, 1000, fn(reply_to) {
      ClientRequest(protocol.PublishEvent(reply_to, test_topic, payload))
    })
  {
    Ok(_) -> Nil
    Error(_) -> panic as "failed to publish event"
  }

  process.sleep(2000)
  // wait for consumer to poll for the event

  // read
  let assert Ok(events) =
    process.call(broker_subject, 1000, fn(reply_to) {
      ClientRequest(protocol.ReadEvents(reply_to, test_topic))
    })
  assert list.length(events) == 1
  let assert Ok(found) = list.first(events)
  assert string.contains(found, payload)
  let _ = simplifile.delete(test_topic)
}

fn mock_orchestrator(
  test_name: String,
) -> process.Subject(protocol.BrokerMessage) {
  let topic_reference = process.new_name(test_name <> "topic_sup")
  let consumer_reference = process.new_name(test_name <> "consumer_sup")
  let publisher_reference = process.new_name(test_name <> "pub_sup")
  let broker_reference = process.new_name(test_name <> "broker")

  let topic_supervisor =
    factory.worker_child(topic.start_actor)
    |> factory.named(topic_reference)
    |> factory.supervised

  let consumer_supervisor =
    factory.worker_child(consumer.start_actor)
    |> factory.named(consumer_reference)
    |> factory.supervised

  let publisher_supervisor =
    factory.worker_child(publisher.start_actor)
    |> factory.named(publisher_reference)
    |> factory.supervised

  let broker_args =
    broker.Args(
      name: broker_reference,
      topic: topic_reference,
      consumer: consumer_reference,
      publisher: publisher_reference,
    )

  let broker_worker =
    supervision.worker(fn() { broker.start_actor(broker_args) })

  case
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(topic_supervisor)
    |> supervisor.add(consumer_supervisor)
    |> supervisor.add(publisher_supervisor)
    |> supervisor.add(broker_worker)
    |> supervisor.start
  {
    Ok(_) -> Nil
    Error(_) -> panic as "failed to start orchestrator"
  }

  process.named_subject(broker_reference)
}
