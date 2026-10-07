import beam_broker/protocol.{type TopicMessage}
import gleam/erlang/process
import gleam/otp/actor
import logging
import simplifile

pub type Args {
  Args(file_path: String)
}

pub type State {
  State(file_path: String, next_offset: Int)
}

/// Start a topic actor
///
/// Topic is an event log where other actors can publish to,
/// and consume from, events. A topic is a prerequisite for
/// both publishers and consumers
///
pub fn start_actor(args: Args) {
  let state = State(file_path: args.file_path, next_offset: 0)
  let _ = simplifile.create_file(args.file_path)

  actor.new(state)
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(state: State, message: TopicMessage) {
  case message {
    protocol.Append(input) -> {
      case simplifile.append(state.file_path, input) {
        Ok(_) -> actor.continue(state)
        Error(error) -> {
          logging.log(logging.Error, simplifile.describe_error(error))
          actor.continue(state)
        }
      }
    }

    protocol.Read(_, _, replyto) -> {
      let return = process.named_subject(replyto)
      case simplifile.read(state.file_path) {
        Ok(result) -> {
          actor.send(return, protocol.Receive(result))
          actor.continue(state)
        }
        Error(error) -> {
          logging.log(logging.Error, simplifile.describe_error(error))
          actor.continue(state)
        }
      }
    }
  }
}
