import gleam/otp/actor

pub type Message(element) {
  Shutdown
  Append(String)
  Read(offset: Int, limit: Int, replyto: Int)
  Crash
}

fn handle_message(message: Message(e)) {
  todo as "build the topic actor"
}
