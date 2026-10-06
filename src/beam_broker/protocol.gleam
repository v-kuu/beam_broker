import gleam/erlang/process

pub type TopicMessage {
  Append(String)
  Read(offset: Int, limit: Int, replyto: process.Name(ConsumerMessage))
}

pub type ConsumerMessage {
  Poll
  Receive(response: String)
}

pub type PublisherMessage {
  Publish
}
