import beam_broker/views
import gleam/string
import gleeunit
import gleeunit/should
import lustre/element

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn home_page_renders_document_test() {
  let html = views.home() |> element.to_document_string
  string.starts_with(html, "<!doctype html>")
  |> should.be_true

  string.contains(html, "Welcome to Beam Broker")
  |> should.be_true

  string.contains(html, "Control Plane")
  |> should.be_true
}

pub fn topics_list_empty_test() {
  let html = views.topics_list([]) |> element.to_string
  string.contains(html, "No active topics found.")
  |> should.be_true
}

pub fn topics_list_with_items_test() {
  let html = views.topics_list(["kernighan", "ritchie"]) |> element.to_string
  string.contains(html, "<li>kernighan</li>")
  |> should.be_true
  string.contains(html, "<li>ritchie</li>")
  |> should.be_true
}

pub fn publisher_options_empty_test() {
  let html = views.publisher_options([]) |> element.to_string
  string.contains(html, "No publishers registered")
  |> should.be_true
}

pub fn publisher_options_with_items_test() {
  let html = views.publisher_options(["publisher_a"]) |> element.to_string
  string.contains(html, "-- Select a Publisher --")
  |> should.be_true
  string.contains(html, "<option value=\"publisher_a\">publisher_a</option>")
  |> should.be_true
}

pub fn consumer_options_empty_test() {
  let html = views.consumer_options([]) |> element.to_string
  string.contains(html, "No consumers registered")
  |> should.be_true
}

pub fn consumer_options_with_items_test() {
  let html = views.consumer_options(["consumer_1"]) |> element.to_string
  string.contains(html, "-- Select a Consumer --")
  |> should.be_true
  string.contains(html, "<option value=\"consumer_1\">consumer_1</option>")
  |> should.be_true
}

pub fn feedback_renders_test() {
  let success_html = views.success("Created successfully") |> element.to_string
  string.contains(success_html, "Created successfully")
  |> should.be_true

  let error_html = views.error("Failed to create") |> element.to_string
  string.contains(error_html, "Failed to create")
  |> should.be_true
}
