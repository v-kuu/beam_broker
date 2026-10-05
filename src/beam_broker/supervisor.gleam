import beam_broker/consumer
import beam_broker/topic
import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/static_supervisor as supervisor

const topic_supervisor_name = "topic_supervisor"

const consumer_supervisor_name = "consumer_supervisor"

type FactoryName(argument, data) =
  process.Name(factory.Message(argument, data))

/// Start a supervisor.
///
/// The name argument of the supervisor can be fetched later with get_by_name
/// to use start_child for spawning a new child process.
/// The actor argument dictates the actor type this supervisor manages
///
pub fn start_supervisor(
  supervisor_name: FactoryName(argument, data),
  start_actor: fn(argument) -> Result(actor.Started(data), actor.StartError),
) {
  factory.worker_child(start_actor)
  |> factory.named(supervisor_name)
  |> factory.supervised
}

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
    factory.worker_child(consumer.start_child)
    |> factory.named(process.new_name(consumer_supervisor_name))
    |> factory.supervised

  let orchestrator =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(topic_supervisor)
    |> supervisor.add(consumer_supervisor)
    |> supervisor.start
  Ok(orchestrator)
}
