import beam_broker
import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/static_supervisor as supervisor

/// Start the supervision tree.
///
/// The name given as an argument can be used to find a reference to the supervisor with get_by_name
/// to use start_child for spawning a new child process
///
pub fn start_supervision_tree(
  reporters_name: process.Name(_),
) -> actor.StartResult(_) {
  let reporter_factory_supervisor =
    factory.worker_child(todo as "beam_broker.start_reporter_actor")
    |> factory.named(reporters_name)
    |> factory.supervised

  let control_plane =
    todo as "a process that commands the supervisor to spawn children"

  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(reporter_factory_supervisor)
  |> supervisor.add(control_plane)
  |> supervisor.start
}
