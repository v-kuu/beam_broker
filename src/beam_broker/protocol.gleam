import gleam/erlang/process

pub type TopicMessage {
  Shutdown
  Append(String)
  Read(offset: Int, limit: Int, replyto: process.Subject(ConsumerMessage))
  Crash
}

pub type ConsumerMessage {
  Poll
}

pub type PublisherMessage {
  Publish
}
