import gleam/list
import lustre/attribute
import lustre/element.{type Element}
import lustre/element/html

// HTMX attributes helpers
pub fn hx_post(url: String) -> attribute.Attribute(msg) {
  attribute.attribute("hx-post", url)
}

pub fn hx_get(url: String) -> attribute.Attribute(msg) {
  attribute.attribute("hx-get", url)
}

pub fn hx_target(target: String) -> attribute.Attribute(msg) {
  attribute.attribute("hx-target", target)
}

pub fn hx_swap(swap: String) -> attribute.Attribute(msg) {
  attribute.attribute("hx-swap", swap)
}

pub fn hx_trigger(trigger: String) -> attribute.Attribute(msg) {
  attribute.attribute("hx-trigger", trigger)
}

pub fn hx_include(include: String) -> attribute.Attribute(msg) {
  attribute.attribute("hx-include", include)
}

/// The homepage rendered with Lustre
pub fn home() -> Element(msg) {
  html.html([attribute.lang("en-US")], [
    html.head([], [
      html.meta([attribute.charset("utf-8")]),
      html.meta([
        attribute.name("viewport"),
        attribute.content("width=device-width, initial-scale=1"),
      ]),
      html.link([
        attribute.rel("stylesheet"),
        attribute.href("https://cdn.jsdelivr.net/npm/water.css@2/out/dark.css"),
      ]),
      html.script(
        [
          attribute.src(
            "https://cdn.jsdelivr.net/npm/htmx.org@2.0.11/dist/htmx.min.js",
          ),
          attribute.attribute(
            "integrity",
            "sha384-2OatzQy1H+Zd/IIrjr1TcuDGqLXeHhbooAyJY1KdQMKnr4LZ22k31GBLdYKHmVjg",
          ),
          attribute.attribute("crossorigin", "anonymous"),
        ],
        "",
      ),
      html.title([], "Beam Broker"),
    ]),
    html.body([], [
      html.main([], [
        html.h1([], [element.text("Welcome to Beam Broker")]),
        html.p([], [
          element.text(
            "A minikafka style message broker where publishers and consumer pass events through topics",
          ),
        ]),
        html.hr([]),
        html.h2([], [element.text("Control Plane")]),
        html.div(
          [
            attribute.styles([
              #("display", "flex"),
              #("gap", "1.5rem"),
              #("flex-wrap", "wrap"),
            ]),
          ],
          [
            create_topic_section(),
            create_consumer_section(),
            create_publisher_section(),
          ],
        ),
        active_topics_section(),
        html.hr([]),
        publisher_control_section(),
        html.hr([]),
        consumer_stream_section(),
      ]),
    ]),
  ])
}

fn create_topic_section() -> Element(msg) {
  html.section([], [
    html.h3([], [element.text("Create Topic")]),
    html.form(
      [
        hx_post("/api/topics"),
        hx_target("#topic-result"),
        hx_swap("innerHTML"),
      ],
      [
        html.input([
          attribute.type_("text"),
          attribute.name("topic_name"),
          attribute.placeholder("Topic name"),
          attribute.required(True),
        ]),
        html.button([attribute.type_("submit")], [element.text("Create Topic")]),
      ],
    ),
    html.div([attribute.id("topic-result")], []),
  ])
}

fn create_consumer_section() -> Element(msg) {
  html.section([], [
    html.h3([], [element.text("Register Consumer")]),
    html.form(
      [
        hx_post("/api/consumers"),
        hx_target("#consumer-result"),
        hx_swap("innerHTML"),
      ],
      [
        html.input([
          attribute.type_("text"),
          attribute.name("topic_name"),
          attribute.placeholder("Target topic name"),
          attribute.required(True),
        ]),
        html.button([attribute.type_("submit")], [
          element.text("Spawn Consumer"),
        ]),
      ],
    ),
    html.div([attribute.id("consumer-result")], []),
  ])
}

fn create_publisher_section() -> Element(msg) {
  html.section([], [
    html.h3([], [element.text("Register Publisher")]),
    html.form(
      [
        hx_post("/api/publishers"),
        hx_target("#publisher-result"),
        hx_swap("innerHTML"),
      ],
      [
        html.input([
          attribute.type_("text"),
          attribute.name("publisher_name"),
          attribute.placeholder("Target topics name"),
          attribute.required(True),
        ]),
        html.button([attribute.type_("submit")], [
          element.text("Spawn Publisher"),
        ]),
      ],
    ),
    html.div([attribute.id("publisher-result")], []),
  ])
}

fn active_topics_section() -> Element(msg) {
  html.section([], [
    html.h3([], [element.text("Active topics")]),
    html.div(
      [
        attribute.id("topics-list-container"),
        hx_get("/api/topics/list"),
        hx_trigger("load, topics-updated from:body, every 3s"),
        hx_swap("innerHTML"),
      ],
      [html.p([], [element.text("Loading active topics...")])],
    ),
  ])
}

fn publisher_control_section() -> Element(msg) {
  html.section([], [
    html.h2([], [element.text("Publisher Control")]),
    html.h3([], [element.text("Publish Message")]),
    html.form(
      [
        hx_post("/api/publish"),
        hx_target("#publish-result"),
        hx_swap("innerHTML"),
      ],
      [
        html.label([attribute.for("publisher-select")], [
          element.text("Select Publisher:"),
        ]),
        html.select(
          [
            attribute.id("publisher-select"),
            attribute.name("publisher_name"),
            hx_get("/api/publishers/options"),
            hx_target("this"),
            hx_trigger("load, publishers-updated from:body"),
            attribute.required(True),
          ],
          [
            html.option([attribute.value("")], "Loading publishers..."),
          ],
        ),
        html.input([
          attribute.type_("text"),
          attribute.name("payload"),
          attribute.placeholder("Enter message payload..."),
          attribute.required(True),
        ]),
        html.button([attribute.type_("submit")], [
          element.text("Send Message"),
        ]),
      ],
    ),
    html.div([attribute.id("publish-result")], []),
  ])
}

fn consumer_stream_section() -> Element(msg) {
  html.section([], [
    html.h2([], [element.text("Consumer Stream")]),
    html.h3([], [element.text("Live event stream")]),
    html.label([attribute.for("consumer-select")], [
      element.text("Select consumer to monitor:"),
    ]),
    html.select(
      [
        attribute.id("consumer-select"),
        attribute.name("consumer_name"),
        hx_get("/api/consumers/options"),
        hx_trigger("load, consumers-updated from:body"),
      ],
      [
        html.option([attribute.value("")], "Loading consumers..."),
      ],
    ),
    html.div([attribute.style("margin-top", "1rem")], [
      html.label([], [element.text("Output log:")]),
      html.div(
        [
          attribute.id("event-output-box"),
          hx_get("/api/consumer/events"),
          hx_trigger("load, change from:#consumers-select, every 2s"),
          hx_include("#consumer-select"),
          hx_swap("innerHTML"),
          attribute.styles([
            #("border", "1px solid"),
            #("padding", "1rem"),
            #("height", "200px"),
            #("overflow-y", "auto"),
            #("resize", "vertical"),
          ]),
        ],
        [
          element.text("Select a consumer above to view live events..."),
        ],
      ),
    ]),
  ])
}

/// Type-safe view for the active topics list
pub fn topics_list(topics: List(String)) -> Element(msg) {
  case topics {
    [] -> html.p([], [element.text("No active topics found.")])
    _ ->
      html.ul(
        [],
        list.map(topics, fn(topic) { html.li([], [element.text(topic)]) }),
      )
  }
}

/// Type-safe view for publisher select options
pub fn publisher_options(publishers: List(String)) -> Element(msg) {
  case publishers {
    [] -> html.option([attribute.value("")], "No publishers registered")
    _ ->
      element.fragment([
        html.option([attribute.value("")], "-- Select a Publisher --"),
        ..list.map(publishers, fn(publisher_name) {
          html.option([attribute.value(publisher_name)], publisher_name)
        })
      ])
  }
}

/// Type-safe view for consumer select options
pub fn consumer_options(consumers: List(String)) -> Element(msg) {
  case consumers {
    [] -> html.option([attribute.value("")], "No consumers registered")
    _ ->
      element.fragment([
        html.option([attribute.value("")], "-- Select a Consumer --"),
        ..list.map(consumers, fn(consumer_name) {
          html.option([attribute.value(consumer_name)], consumer_name)
        })
      ])
  }
}

/// Type-safe events view
pub fn consumer_events(events: List(String)) -> Element(msg) {
  case events {
    [] -> consumer_events_empty()
    _ ->
      html.div([],
        list.map(events, fn(event_content) {
          html.div([attribute.value("")], [element.text(event_content)])
        })
      )
  }
}

/// Type-safe empty events view
pub fn consumer_events_empty() -> Element(msg) {
  html.div([], [element.text("No events yet...")])
}

/// Type-safe success feedback
pub fn success(message: String) -> Element(msg) {
  html.p([attribute.style("color", "#51cf66")], [element.text(message)])
}

/// Type-safe error feedback
pub fn error(message: String) -> Element(msg) {
  html.p([attribute.style("color", "#ff6b6b")], [element.text(message)])
}
