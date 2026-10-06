import beam_broker/consumer
import beam_broker/publisher
import beam_broker/topic
import gleam/erlang/process
import gleam/otp/factory_supervisor as factory
import gleam/otp/static_supervisor as supervisor

const topic_supervisor_name = "topic_supervisor"

const consumer_supervisor_name = "consumer_supervisor"

const publisher_supervisor_name = "publisher_supervisor"

/// Start the main orchestrator
///
/// Registers all the supervisors and the control plane and starts
/// the core OTP program
///
pub fn start_orchestrator() {
  let topic_supervisor =
    factory.worker_child(topic.start_actor)
    |> factory.named(process.new_name(topic_supervisor_name))
    |> factory.supervised

  let consumer_supervisor =
    factory.worker_child(consumer.start_actor)
    |> factory.named(process.new_name(consumer_supervisor_name))
    |> factory.supervised

  let publisher_supervisor =
    factory.worker_child(publisher.start_actor)
    |> factory.named(process.new_name(publisher_supervisor_name))
    |> factory.supervised

  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(topic_supervisor)
  |> supervisor.add(consumer_supervisor)
  |> supervisor.add(publisher_supervisor)
  |> supervisor.start
}
