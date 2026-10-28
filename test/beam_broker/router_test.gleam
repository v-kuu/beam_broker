import beam_broker/broker
import beam_broker/consumer
import beam_broker/protocol
import beam_broker/publisher
import beam_broker/router
import beam_broker/topic
import gleam/erlang/process
import gleam/http
import gleam/list
import gleam/otp/factory_supervisor as factory
import gleam/otp/static_supervisor as supervisor
import gleam/otp/supervision
import gleeunit
import simplifile
import wisp/simulate

pub fn main() {
  gleeunit.main()
}

pub fn topics_post_test() {
  let test_name = "topics_post_test"
  let ctx = router.Context(mock_orchestrator(test_name))

  // ok, duplicate, empty
  let test_cases = [#(test_name, 200), #(test_name, 400), #("", 400)]

  test_cases
  |> list.each(fn(test_case) {
    let #(payload, expected_status) = test_case

    let req =
      simulate.browser_request(http.Post, "/api/topics")
      |> simulate.form_body([#("topic_name", payload)])

    let response = router.handle_request(req, ctx)
    assert response.status == expected_status
  })
  let _ = simplifile.delete_file(test_name)
}

fn mock_orchestrator(
  test_name: String,
) -> process.Name(protocol.BrokerMessage) {
  let topic_reference = process.new_name(test_name <> "_topic_sup")
  let consumer_reference = process.new_name(test_name <> "_consumer_sup")
  let publisher_reference = process.new_name(test_name <> "_pub_sup")
  let broker_reference = process.new_name(test_name <> "_broker")

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

  broker_reference
}
