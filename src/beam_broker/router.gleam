import beam_broker/protocol.{type BrokerMessage, ClientRequest}
import beam_broker/views
import beam_broker/web
import gleam/erlang/process
import gleam/http.{Get, Post}
import gleam/list
import gleam/result
import gleam/string
import lustre/element
import wisp.{type Request, type Response}

pub type Context {
  Context(broker_ref: process.Name(BrokerMessage))
}

pub fn handle_request(request: Request, ctx: Context) -> Response {
  use request <- web.middleware(request)

  case wisp.path_segments(request) {
    [] -> home_page(request)
    ["api", "topics"] -> create_topic(request, ctx)
    ["api", "consumers"] -> create_consumer(request, ctx)
    ["api", "publishers"] -> create_publisher(request, ctx)
    ["api", "publish"] -> publish_event(request, ctx)
    ["api", "topics", "list"] -> list_topics(request, ctx)
    ["api", "publishers", "options"] -> list_publishers(request, ctx)
    ["api", "consumers", "options"] -> list_consumers(request, ctx)
    ["api", "consumer", "events"] -> get_events(request, ctx)
    _ -> wisp.not_found()
  }
}

fn home_page(request: Request) -> Response {
  use <- wisp.require_method(request, Get)
  views.home()
  |> element.to_document_string
  |> wisp.html_response(200)
}

fn create_topic(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Post)
  use form_data <- wisp.require_form(request)
  case list.key_find(form_data.values, "topic_name") {
    Ok(topic_name) -> {
      let trimmed_name = string.trim(topic_name)
      case trimmed_name == "" {
        True ->
          views.error("Topic name cannot be empty")
          |> element.to_string
          |> wisp.html_response(400)
        False -> {
          let subject = process.named_subject(ctx.broker_ref)
          case
            process.call(subject, 1000, fn(reply_to) {
              ClientRequest(protocol.CreateTopic(reply_to, trimmed_name))
            })
          {
            Ok(_) ->
              views.success("Topic '" <> trimmed_name <> "' created!")
              |> element.to_string
              |> wisp.html_response(200)
              |> wisp.set_header("hx-trigger", "topics-updated")
            Error(err) ->
              case err {
                protocol.TopicAlreadyExists ->
                  views.error("Topic already exists")
                  |> element.to_string
                  |> wisp.html_response(400)
                protocol.TopicFailedToStart ->
                  views.error("Topic actor failed to start")
                  |> element.to_string
                  |> wisp.html_response(500)
              }
          }
        }
      }
    }
    Error(Nil) -> wisp.bad_request("no topic_name found")
  }
}

fn create_consumer(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Post)
  use form_data <- wisp.require_form(request)
  case list.key_find(form_data.values, "topic_name") {
    Ok(topic_name) -> {
      let trimmed_name = string.trim(topic_name)
      case trimmed_name == "" {
        True ->
          views.error("Topic name cannot be empty")
          |> element.to_string
          |> wisp.html_response(400)
        False -> {
          let subject = process.named_subject(ctx.broker_ref)
          case
            process.call(subject, 1000, fn(reply_to) {
              ClientRequest(protocol.CreateConsumer(reply_to, trimmed_name))
            })
          {
            Ok(_) ->
              views.success("Consumer '" <> trimmed_name <> "' created!")
              |> element.to_string
              |> wisp.html_response(200)
              |> wisp.set_header("hx-trigger", "consumers-updated")
            Error(err) ->
              case err {
                protocol.ConsumerAlreadyExists ->
                  views.error("Consumer already registered")
                  |> element.to_string
                  |> wisp.html_response(400)
                protocol.ConsumerTargetNotFound ->
                  views.error("Topic not found")
                  |> element.to_string
                  |> wisp.html_response(400)
                protocol.ConsumerFailedToStart ->
                  views.error("Consumer actor failed to start")
                  |> element.to_string
                  |> wisp.html_response(500)
              }
          }
        }
      }
    }
    Error(Nil) -> wisp.bad_request("no topic_name found")
  }
}

fn create_publisher(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Post)
  use form_data <- wisp.require_form(request)
  let name_result =
    list.key_find(form_data.values, "publisher_name")
    |> result.lazy_or(fn() { list.key_find(form_data.values, "topic_name") })
  case name_result {
    Ok(topic_name) -> {
      let trimmed_name = string.trim(topic_name)
      case trimmed_name == "" {
        True ->
          views.error("Topic name cannot be empty")
          |> element.to_string
          |> wisp.html_response(400)
        False -> {
          let subject = process.named_subject(ctx.broker_ref)
          case
            process.call(subject, 1000, fn(reply_to) {
              ClientRequest(protocol.CreatePublisher(reply_to, trimmed_name))
            })
          {
            Ok(_) ->
              views.success("Publisher '" <> trimmed_name <> "' created!")
              |> element.to_string
              |> wisp.html_response(200)
              |> wisp.set_header("hx-trigger", "publishers-updated")
            Error(err) ->
              case err {
                protocol.PublisherAlreadyExists ->
                  views.error("Publisher already registered")
                  |> element.to_string
                  |> wisp.html_response(400)
                protocol.PublisherTargetNotFound ->
                  views.error("Topic not found")
                  |> element.to_string
                  |> wisp.html_response(400)
                protocol.PublisherFailedToStart ->
                  views.error("Publisher actor failed to start")
                  |> element.to_string
                  |> wisp.html_response(500)
              }
          }
        }
      }
    }
    Error(Nil) -> wisp.bad_request("no publisher_name found")
  }
}

fn publish_event(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Post)
  use form_data <- wisp.require_form(request)

  let publisher_result =
    list.key_find(form_data.values, "topic_name")
    |> result.lazy_or(fn() { list.key_find(form_data.values, "publisher_name") })
    |> result.map(string.trim)

  let payload_result =
    list.key_find(form_data.values, "payload")
    |> result.map(string.trim)

  case publisher_result, payload_result {
    Ok(publisher), Ok(payload) if publisher != "" && payload != "" -> {
      let subject = process.named_subject(ctx.broker_ref)
      case
        process.call(subject, 1000, fn(reply_to) {
          ClientRequest(protocol.PublishEvent(reply_to, publisher, payload))
        })
      {
        Ok(_) ->
          views.success("Event published to '" <> publisher <> "'!")
          |> element.to_string
          |> wisp.html_response(200)
        Error(err) ->
          case err {
            protocol.WriteFailed ->
              views.error("Failed to write event")
              |> element.to_string
              |> wisp.html_response(500)
            protocol.PublisherNotFound ->
              views.error("Publisher not found")
              |> element.to_string
              |> wisp.html_response(400)
          }
      }
    }
    _, _ ->
      wisp.bad_request("Missing or empty topic_name/publisher_name or payload")
  }
}

fn list_topics(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Get)
  let subject = process.named_subject(ctx.broker_ref)
  let topics =
    process.call(subject, 1000, fn(reply_to) {
      ClientRequest(protocol.ListTopics(reply_to))
    })
  views.topics_list(topics)
  |> element.to_string
  |> wisp.html_response(200)
}

fn list_publishers(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Get)
  let subject = process.named_subject(ctx.broker_ref)
  let publishers =
    process.call(subject, 1000, fn(reply_to) {
      ClientRequest(protocol.ListPublishers(reply_to))
    })
  views.publisher_options(publishers)
  |> element.to_string
  |> wisp.html_response(200)
}

fn list_consumers(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Get)
  let subject = process.named_subject(ctx.broker_ref)
  let consumers =
    process.call(subject, 1000, fn(reply_to) {
      ClientRequest(protocol.ListConsumers(reply_to))
    })
  views.consumer_options(consumers)
  |> element.to_string
  |> wisp.html_response(200)
}

fn get_events(request: Request, ctx: Context) -> Response {
  use <- wisp.require_method(request, Get)
  let query_params = wisp.get_query(request)

  let consumer_result =
    list.key_find(query_params, "consumer_name")
    |> result.map(string.trim)

  case consumer_result {
    Ok(consumer) if consumer != "" -> {
      let subject = process.named_subject(ctx.broker_ref)
      case
        process.call(subject, 1000, fn(reply_to) {
          ClientRequest(protocol.ReadEvents(reply_to, consumer))
        })
      {
        Ok(events) ->
          views.consumer_events(events)
          |> element.to_string
          |> wisp.html_response(200)
        Error(protocol.ConsumerNotFound) ->
          views.consumer_events_empty()
          |> element.to_string
          |> wisp.html_response(200)
      }
    }
    _ ->
      views.consumer_events_empty()
      |> element.to_string
      |> wisp.html_response(200)
  }
}
