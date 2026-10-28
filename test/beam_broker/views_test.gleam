import beam_broker/views
import gleam/string
import gleeunit
import lustre/element

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn home_page_renders_document_test() {
  let html = views.home() |> element.to_document_string
  assert string.starts_with(html, "<!doctype html>")

  assert string.contains(html, "Welcome to Beam Broker")

  assert string.contains(html, "Control Plane")
}

pub fn topics_list_empty_test() {
  let html = views.topics_list([]) |> element.to_string
  assert string.contains(html, "No active topics found.")
}

pub fn topics_list_with_items_test() {
  let html = views.topics_list(["kernighan", "ritchie"]) |> element.to_string
  assert string.contains(html, "<li>kernighan</li>")
  assert string.contains(html, "<li>ritchie</li>")
}

pub fn publisher_options_empty_test() {
  let html = views.publisher_options([]) |> element.to_string
  assert string.contains(html, "No publishers registered")
}

pub fn publisher_options_with_items_test() {
  let html = views.publisher_options(["publisher_a"]) |> element.to_string
  assert string.contains(html, "-- Select a Publisher --")
  assert string.contains(html, "<option value=\"publisher_a\">publisher_a</option>")
}

pub fn consumer_options_empty_test() {
  let html = views.consumer_options([]) |> element.to_string
  assert string.contains(html, "No consumers registered")
}

pub fn consumer_options_with_items_test() {
  let html = views.consumer_options(["consumer_1"]) |> element.to_string
  assert string.contains(html, "-- Select a Consumer --")
  assert string.contains(html, "<option value=\"consumer_1\">consumer_1</option>")
}

pub fn feedback_renders_test() {
  let success_html = views.success("Created successfully") |> element.to_string
  assert string.contains(success_html, "Created successfully")

  let error_html = views.error("Failed to create") |> element.to_string
  assert string.contains(error_html, "Failed to create")
}
