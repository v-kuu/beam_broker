import beam_broker/protocol.{type ReadResult, type TopicMessage}
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/otp/actor
import gleam/string
import gleam/time/calendar
import gleam/time/timestamp
import logging
import simplifile
import splitter

pub type Args {
  Args(file_path: String, reply_to: Subject(protocol.BrokerMessage))
}

pub type State {
  State(file_path: String, own: Subject(TopicMessage), event_count: Int)
}

/// Start a topic actor
///
/// Topic is an event log where other actors can publish to,
/// and consume from, events. A topic is a prerequisite for
/// both publishers and consumers
///
pub fn start_actor(
  args: Args,
) -> Result(actor.Started(Subject(TopicMessage)), actor.StartError) {
  let existing_events = load_file(args.file_path)
  case existing_events {
    Error(_) -> Error(actor.InitFailed("failed to load file"))
    Ok(amount) -> {
      let started =
        actor.new_with_initialiser(5000, fn(own) {
          Ok(
            actor.initialised(State(
              file_path: args.file_path,
              own: own,
              event_count: amount,
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
            protocol.RegisterTopic(args.file_path, result.data),
          )
        }
        Error(_) -> Nil
      }
      started
    }
  }
}

fn load_file(file_path: String) -> Result(Int, Nil) {
  case simplifile.create_file(file_path) {
    Ok(_) -> Ok(0)
    Error(error) ->
      case error {
        simplifile.Eexist -> {
          case simplifile.read(file_path) {
            Ok(content) -> {
              let list = split_to_list(content)
              Ok(list.length(list))
            }
            Error(error) -> {
              logging.log(
                logging.Warning,
                "load_file: " <> simplifile.describe_error(error),
              )
              Error(Nil)
            }
          }
        }
        _ -> {
          logging.log(
            logging.Warning,
            "load_file: " <> simplifile.describe_error(error),
          )
          Error(Nil)
        }
      }
  }
}

fn handle_message(state: State, message: TopicMessage) {
  case message {
    protocol.Append(reply_to, input) ->
      actor.continue(append_event(state, input, reply_to))
    protocol.Read(reply_to, consumer_index) ->
      actor.continue(read_events(state, consumer_index, reply_to))
  }
}

fn append_event(
  state: State,
  input: String,
  reply_to: process.Subject(Result(Nil, Nil)),
) -> State {
  let time_stamp =
    timestamp.system_time()
    |> timestamp.to_rfc3339(calendar.utc_offset)
  let event = "[" <> time_stamp <> "]: " <> input <> "\n"
  case simplifile.append(state.file_path, event) {
    Ok(_) -> {
      let new_event_count = state.event_count + 1
      let new_state = State(..state, event_count: new_event_count)
      process.send(reply_to, Ok(Nil))
      new_state
    }
    Error(error) -> {
      logging.log(
        logging.Warning,
        "append_event: " <> simplifile.describe_error(error),
      )
      process.send(reply_to, Error(Nil))
      state
    }
  }
}

fn read_events(
  state: State,
  consumer_index: Int,
  reply_to: process.Subject(Result(ReadResult, Nil)),
) -> State {
  case simplifile.read(state.file_path) {
    Ok(result) -> {
      let unseen_events =
        split_to_list(result)
        |> list.filter(fn(e) { e != "" })
        |> list.drop(consumer_index)
        |> list.map(fn(e) { string.trim(e) })

      process.send(
        reply_to,
        Ok(protocol.ReadResult(
          events: unseen_events,
          new_index: state.event_count,
        )),
      )
      state
    }
    Error(error) -> {
      logging.log(
        logging.Error,
        "read_events: " <> simplifile.describe_error(error),
      )
      actor.send(reply_to, Error(Nil))
      state
    }
  }
}

fn split_to_list(events: String) -> List(String) {
  let line_end = splitter.new(["\n"])
  splitter.split_all(line_end, events)
}
