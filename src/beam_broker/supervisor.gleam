import beam_broker/topic
import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/static_supervisor as supervisor

const topic_supervisor_name = "topic_supervisor"

type ChildName(msg) =
  process.Name(msg)

type FactoryName(msg, a) =
  process.Name(factory.Message(ChildName(msg), a))

/// Start a supervisor.
///
/// The name argument of the supervisor can be fetched later with get_by_name
/// to use start_child for spawning a new child process.
/// The actor argument dictates the actor type this supervisor manages
///
pub fn start_supervisor(
  supervisor_name: FactoryName(msg, a),
  actor: fn(process.Name(msg)) -> Result(actor.Started(a), actor.StartError),
) {
  let topic_factory_supervisor =
    factory.worker_child(actor)
    |> factory.named(supervisor_name)
    |> factory.supervised

  Ok(topic_factory_supervisor)
}

/// Start the main orchestrator
///
/// Registers all the supervisors and the control plane and starts
/// the core OTP program
///
pub fn start_orchestrator() {
  let assert Ok(topic_supervisor) =
    start_supervisor(
      process.new_name(topic_supervisor_name),
      topic.start_topic_actor,
    )

  let orchestrator =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(topic_supervisor)
    |> supervisor.start
  Ok(orchestrator)
}
