# queue

`queue` is a small library for NATS JetStream. You describe each message type as a producer and each message handler as a consumer. An `App` builder connects them, in the same way that an axum `Router` connects routes, handlers, and state.

The NATS client stays inside this crate. Your code uses only the types that `queue` exports.

## Contents

- [Requirements](#requirements)
- [Install](#install)
- [Quick start](#quick-start)
- [Concepts](#concepts)
- [Connect to NATS](#connect-to-nats)
- [Streams](#streams)
- [Producers](#producers)
- [Headers](#headers)
- [Consumers](#consumers)
- [State](#state)
- [Start and stop the app](#start-and-stop-the-app)
- [Message encoding](#message-encoding)
- [Errors and logging](#errors-and-logging)
- [Behavior to know](#behavior-to-know)
- [API summary](#api-summary)

## Requirements

You need a NATS server with JetStream turned on. JetStream is off by default. Start the server with the `-js` flag.

The `compose.yml` file in the root of this repository starts a correct server:

```sh
docker compose up -d
```

The library uses Tokio. Your application must run inside a Tokio runtime.

## Install

Add the crate as a path dependency:

```toml
[dependencies]
queue = { path = "crates/queue" }
tokio = { version = "1", features = ["full"] }
```

To send and receive JSON, turn on the `json` feature:

```toml
queue = { path = "crates/queue", features = ["json"] }
```

## Quick start

This example publishes an order, ships it in a consumer, and logs the result.

```rust
use queue::{App, Consumer, Context, Message, Nats, Producer, Result};

#[derive(Clone)]
struct AppState {
    warehouse: String,
}

// A producer: the subject and the message type of an outgoing message.
struct OrderPlaced;

impl Producer for OrderPlaced {
    const SUBJECT: &'static str = "orders.placed";
    type Message = String;
}

// A consumer: what to do when a message arrives.
struct ShipOrder;

impl Consumer<AppState> for ShipOrder {
    const STREAM: &'static str = "ORDERS";
    const DURABLE: &'static str = "ship-order";
    const SUBJECT: &'static str = "orders.placed";
    const CONCURRENCY: usize = 4;
    type Message = String;

    async fn handle(&self, message: Message<String>, ctx: Context<AppState>) -> Result {
        println!("shipping {} from {}", message.body, ctx.state.warehouse);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result {
    let nats = Nats::connect("nats://127.0.0.1:4222").await?;

    let running = App::new(nats.clone())
        .stream("ORDERS", ["orders.>"])
        .producer::<OrderPlaced>()
        .consumer(ShipOrder)
        .header("x-service", "orders")
        .with_state(AppState { warehouse: "gothenburg".into() })
        .start()
        .await?;

    OrderPlaced::publish(&nats, "order-1".into())
        .header("x-trace", "abc")
        .await?;

    running.run().await
}
```

`src/main.rs` in this repository contains a longer example with two consumers.

## Concepts

The library uses these NATS terms:

- A subject is the routing key of a message, for example `orders.placed`. Subjects are dot-separated.
- A wildcard matches part of a subject. `*` matches one token. `>` matches one or more tokens at the end.
- A stream stores the messages that match its subjects.
- A durable consumer is a named reader on a stream. The server remembers its position.
- To ack a message is to tell the server that the message was handled.

The library adds these types:

| Type | What it is |
|---|---|
| `Nats` | The connection handle. Clone it and give it to all code that publishes. |
| `Producer` | A trait for one type of outgoing message. |
| `Consumer` | A trait for one message handler. |
| `App` | The builder that declares streams, producers, consumers, headers, and state. |
| `Running` | The handle to an app that started. |
| `Context` | The state and the `Nats` handle that each consumer call gets. |
| `Message` | A received message with its decoded body. |
| `Headers` | The headers of a message. |

## Connect to NATS

```rust
let nats = Nats::connect("nats://127.0.0.1:4222").await?;
```

`Nats` is cheap to clone. All clones share one connection and one registry of producers and default headers. Give the same handle to the `App` and to all other code that publishes, for example your HTTP handlers.

## Streams

Declare each stream on the app with a name and a list of subjects:

```rust
App::new(nats)
    .stream("ORDERS", ["orders.>"])
    .stream("PAYMENTS", ["payments.*", "refunds.*"])
```

When the app starts, it creates each stream. If the stream exists, the app updates its subjects. The other stream settings use the NATS defaults.

A message goes into a stream only if its subject matches a subject of that stream. If no stream matches, the publish fails.

Two streams cannot have overlapping subjects. NATS rejects this configuration.

## Producers

A producer is a type that implements the `Producer` trait. It is usually an empty struct. It tells the library the subject and the message type.

```rust
struct OrderShipped;

impl Producer for OrderShipped {
    const SUBJECT: &'static str = "orders.shipped";
    type Message = String;
}
```

| Item | Required | Description |
|---|---|---|
| `SUBJECT` | Yes | The subject that messages go to. |
| `type Message` | Yes | The message type. It must implement `Encode`. |
| `fn subject` | No | Returns the subject for one message. The default returns `SUBJECT`. |
| `fn headers` | No | Adds headers to each message from this producer. The default adds none. |
| `fn publish` | Provided | Starts a publish. Do not override it. |

### Register a producer

You must register each producer on the app before it publishes:

```rust
App::new(nats.clone())
    .producer::<OrderPlaced>()
    .producer::<OrderShipped>()
```

Registration stores the producer in the `Nats` handle, so the registration takes effect immediately. If you publish with a producer that is not registered, the publish returns this error:

```text
producer `my_crate::OrderShipped` is not registered
```

### Publish a message

Call `publish` with the `Nats` handle and the message, then await the result:

```rust
OrderShipped::publish(&nats, "order-1".into()).await?;
```

The first argument accepts anything that implements `AsRef<Nats>`. In a consumer, you can give it the `Context`:

```rust
OrderShipped::publish(&ctx, message.body).await?;
```

The publish waits until the server stores the message. If the await returns `Ok`, the message is in the stream.

A publish does nothing until you await it. The compiler warns you if you forget.

### Route on message content

To build the subject from the message, override `subject`. This example sends each order to a subject for its region:

```rust
use std::borrow::Cow;
use queue::Json;

#[derive(serde::Serialize)]
struct Order {
    id: u64,
    region: String,
}

struct OrderPlaced;

impl Producer for OrderPlaced {
    const SUBJECT: &'static str = "orders.placed";
    type Message = Json<Order>;

    fn subject(message: &Json<Order>) -> Cow<'static, str> {
        format!("orders.{}.placed", message.0.region).into()
    }
}
```

Make sure that a stream subject matches each subject that you build. For this example, `orders.>` or `orders.*.placed` matches.

## Headers

Headers are name and value pairs that travel with a message. Names are not case-sensitive. A name has one value. If you set a name again, the new value replaces the old value.

### Add headers to outgoing messages

You can add headers at three levels:

1. On the app, for all messages:

   ```rust
   App::new(nats).header("x-service", "orders")
   ```

2. On the producer, for all messages from that producer:

   ```rust
   impl Producer for OrderShipped {
       const SUBJECT: &'static str = "orders.shipped";
       type Message = String;

       fn headers(message: &String, headers: &mut Headers) {
           headers.insert("x-event", "order-shipped");
           headers.insert("x-order", message.clone());
       }
   }
   ```

3. On one publish:

   ```rust
   OrderShipped::publish(&nats, body)
       .header("x-trace", trace_id)
       .await?;
   ```

The library applies the levels in this order: app, then producer, then publish. If two levels set the same name, the later level wins.

### Read headers on received messages

```rust
let trace = message.header("x-trace");   // Option<&str>
for (name, value) in message.headers().iter() {
    println!("{name}: {value}");
}
```

### The `Headers` type

| Method | Description |
|---|---|
| `Headers::new()` | Makes an empty set. |
| `insert(name, value)` | Sets a header. Replaces an existing value with the same name. |
| `get(name)` | Returns the value, or `None`. |
| `iter()` | Returns all pairs as `(&str, &str)`. |
| `extend(other)` | Inserts all pairs from `other`. |

## Consumers

A consumer is a type that implements the `Consumer<S>` trait. `S` is the state type of the app. The library calls `handle` once for each message.

```rust
struct ShipOrder;

impl Consumer<AppState> for ShipOrder {
    const STREAM: &'static str = "ORDERS";
    const DURABLE: &'static str = "ship-order";
    const SUBJECT: &'static str = "orders.placed";
    const CONCURRENCY: usize = 4;
    type Message = String;

    async fn handle(&self, message: Message<String>, ctx: Context<AppState>) -> Result {
        Ok(())
    }
}
```

| Item | Required | Description |
|---|---|---|
| `STREAM` | Yes | The name of the stream to read from. |
| `DURABLE` | Yes | The name of the durable consumer on the server. |
| `SUBJECT` | Yes | The subject filter. Wildcards are permitted. |
| `CONCURRENCY` | No | The number of messages that this process handles at the same time. The default is 1. |
| `type Message` | Yes | The message type. It must implement `Decode`. |
| `fn handle` | Yes | The handler. Write it as an `async fn`. |

Register each consumer on the app:

```rust
App::new(nats).consumer(ShipOrder)
```

The consumer value moves into the app. You can store fields on the struct, for example a client for another service. The library shares one instance between all concurrent calls through an `Arc`.

### Ack, retry, and drop

The return value of `handle` controls what happens to the message:

| Result | Action |
|---|---|
| `Ok(())` | The library acks the message. The server does not send it again. |
| `Err(_)` | The library naks the message. The server sends it again. |
| The body does not decode | The library terminates the message. The server never sends it again. |

A nak is a negative ack. It tells the server to send the message again.

A message that does not decode will also fail on the next try, so the library does not retry it. It logs an error and drops the message.

### Competing consumers and concurrency

`CONCURRENCY` sets how many messages one process handles at the same time. With `CONCURRENCY = 4`, up to four `handle` calls run in parallel. A fifth message waits until one call finishes.

All processes that use the same `DURABLE` name share one durable consumer. The server gives each message to only one of them. To spread the load, run more instances of your service. Each instance handles up to `CONCURRENCY` messages.

Two consumers with different `DURABLE` names each get a copy of every message that matches their subjects.

Messages can complete out of order when `CONCURRENCY` is more than 1. If the order is important, set `CONCURRENCY` to 1 and run one instance.

### The `Message` type

| Item | Description |
|---|---|
| `body` | The decoded body. It is a public field. |
| `subject()` | The subject that the message came on. |
| `header(name)` | The value of one header, or `None`. |
| `headers()` | All headers. |
| `delivered()` | The delivery attempt. The first delivery is 1. |

You can use `delivered()` to stop after a number of tries:

```rust
if message.delivered() > 5 {
    eprintln!("giving up on {}", message.body);
    return Ok(()); // ack, so the server stops sending it
}
```

### The `Context` type

`Context<S>` has two public fields:

- `state`: a clone of the app state.
- `nats`: the `Nats` handle.

`Context` implements `AsRef<Nats>`, so you can publish with it directly:

```rust
async fn handle(&self, message: Message<String>, ctx: Context<AppState>) -> Result {
    let db = &ctx.state.db;
    OrderShipped::publish(&ctx, message.body).await
}
```

### A consumer for any state

If a consumer does not use the state, make it generic over `S`. Then it works in any app:

```rust
struct LogShipped;

impl<S: Send + Sync> Consumer<S> for LogShipped {
    const STREAM: &'static str = "ORDERS";
    const DURABLE: &'static str = "log-shipped";
    const SUBJECT: &'static str = "orders.shipped";
    type Message = String;

    async fn handle(&self, message: Message<String>, _ctx: Context<S>) -> Result {
        println!("shipped {}", message.body);
        Ok(())
    }
}
```

## State

State is a value that all consumers can read, for example a database pool or a configuration. It works as state works in axum.

Give the state to the app with `with_state`:

```rust
#[derive(Clone)]
struct AppState {
    db: PgPool,
    warehouse: String,
}

App::new(nats)
    .consumer(ShipOrder)
    .with_state(AppState { db, warehouse })
```

The state must implement `Clone`, `Send`, and `Sync`. The library clones it for each message. If the state is large, put it in an `Arc` or use types that are cheap to clone.

You can add consumers before or after `with_state`. The type of the app shows the state that is still missing:

- `App<AppState>` needs an `AppState`. You cannot start it yet.
- `App<()>` has all of its state. You can start it.

If you forget `with_state`, the compiler stops with an error on `start` or `run`.

Add all consumers that need the state before you call `with_state`. A consumer that you add after `with_state` gets the new state type. Usually that type is `()`.

## Start and stop the app

### Start

`start` prepares the app and returns a `Running` handle. It does these steps in order:

1. It creates or updates each declared stream.
2. It creates or updates the durable consumer for each consumer.
3. It starts to read messages in background tasks.

```rust
let running = app.start().await?;
```

If a stream or a consumer cannot be created, `start` returns the error, and nothing runs.

When `start` returns, all streams exist. It is then safe to publish.

### Run

`Running` has two methods. Both wait until the app stops:

| Method | Stops when |
|---|---|
| `run()` | The process gets Ctrl+C. |
| `run_until(signal)` | The `signal` future completes. |

`App::run()` is a shortcut for `start` followed by `Running::run`. Use it when you do not publish from outside a consumer.

```rust
app.run().await?;
```

Use `run_until` to stop on your own signal. This example stops after 30 seconds:

```rust
running
    .run_until(tokio::time::sleep(std::time::Duration::from_secs(30)))
    .await?;
```

### Shutdown

When the app stops, each consumer does these steps:

1. It stops reading new messages.
2. It waits until all calls to `handle` finish.
3. It acks or naks each finished message.

Then `run` or `run_until` returns.

If a consumer stops by itself, the app stops all other consumers and returns an error. For example, this occurs when the connection to the message stream closes.

If you drop the `Running` handle without a call to `run`, all consumers stop immediately.

### Run next to an HTTP server

To publish from an HTTP server, give it a clone of the `Nats` handle. Run the app and the server in the same process:

```rust
let running = App::new(nats.clone())
    .stream("ORDERS", ["orders.>"])
    .producer::<OrderPlaced>()
    .start()
    .await?;

let http = tokio::spawn(serve_http(nats.clone()));
running.run().await?;
```

## Message encoding

The message type of a producer must implement `Encode`. The message type of a consumer must implement `Decode`. The library implements both traits for these types:

| Type | Encoding |
|---|---|
| `String` | UTF-8 bytes. Decoding fails if the bytes are not valid UTF-8. |
| `Vec<u8>` | The raw bytes. |
| `Json<T>` | JSON. Requires the `json` feature. |

### JSON

Turn on the `json` feature. Then wrap a serde type in `Json`:

```rust
use queue::Json;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Order {
    id: u64,
}

struct OrderPlaced;

impl Producer for OrderPlaced {
    const SUBJECT: &'static str = "orders.placed";
    type Message = Json<Order>;
}

OrderPlaced::publish(&nats, Json(Order { id: 1 })).await?;
```

In a consumer, the body is a `Json<Order>`. Use `.0` to get the order:

```rust
async fn handle(&self, message: Message<Json<Order>>, ctx: Context<AppState>) -> Result {
    let Json(order) = message.body;
    println!("order {}", order.id);
    Ok(())
}
```

### Your own encoding

Implement `Encode`, `Decode`, or both on your type:

```rust
use queue::{Decode, Encode, Result};

struct OrderId(u64);

impl Encode for OrderId {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.0.to_be_bytes().to_vec())
    }
}

impl Decode for OrderId {
    fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(OrderId(u64::from_be_bytes(bytes.try_into()?)))
    }
}
```

## Errors and logging

The library uses one error type:

```rust
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T = (), E = Error> = std::result::Result<T, E>;
```

`Result` with no type argument is `Result<()>`. Use it as the return type of `handle`. The `?` operator works with most error types.

The library writes log events with the `tracing` crate. It logs these events:

- A handler returns an error (warning).
- A message does not decode (error).
- The library cannot pull a message or send an ack (warning).

To see the events, install a subscriber, for example `tracing-subscriber`:

```rust
tracing_subscriber::fmt::init();
```

If you do not install a subscriber, the library writes no logs.

## Behavior to know

- The library does not retry a failed publish. If you need retries, write them in your code.
- A nak asks the server to send the message again immediately. The library adds no delay.
- If `handle` panics, the library does not ack the message. The server sends it again after the ack wait time. The NATS default is 30 seconds.
- The server sends a message again without limit until a handler returns `Ok`. Use `delivered()` to stop after a number of tries.
- At start, the app updates an existing durable consumer to the new `SUBJECT`. NATS refuses some changes, for example a change of the delivery policy. If the update fails, `start` returns the error. Then delete the durable consumer with the `nats` CLI, or use a new `DURABLE` name.
- New durable consumers start at the first message in the stream.
- The server can deliver a message more than one time. Write handlers that give the same result when they run two times on the same message.

## API summary

### `App<S>`

| Method | Description |
|---|---|
| `App::new(nats)` | Makes a builder with the `Nats` handle. |
| `.stream(name, subjects)` | Declares a stream. |
| `.producer::<P>()` | Registers a producer. |
| `.consumer(c)` | Adds a consumer. |
| `.header(name, value)` | Adds a header to all outgoing messages. |
| `.with_state(state)` | Gives the state to all consumers that you added. |
| `.start()` | Creates streams and consumers and returns `Running`. Only on `App<()>`. |
| `.run()` | Starts the app and runs it until Ctrl+C. Only on `App<()>`. |

### `Running`

| Method | Description |
|---|---|
| `.run()` | Runs until Ctrl+C. |
| `.run_until(signal)` | Runs until `signal` completes. |

### `Nats`

| Method | Description |
|---|---|
| `Nats::connect(url)` | Connects to a NATS server. |

### `Producer`

| Item | Description |
|---|---|
| `SUBJECT` | The default subject. |
| `type Message` | The message type. |
| `subject(&message)` | The subject for one message. |
| `headers(&message, &mut headers)` | Headers for each message. |
| `publish(nats, message)` | Returns a `Publish` builder. |

### `Publish<P>`

| Method | Description |
|---|---|
| `.header(name, value)` | Adds a header to this message. |
| `.await` | Sends the message and waits until the server stores it. |

### `Consumer<S>`

| Item | Description |
|---|---|
| `STREAM` | The stream to read from. |
| `DURABLE` | The name of the durable consumer. |
| `SUBJECT` | The subject filter. |
| `CONCURRENCY` | Messages that one process handles at the same time. The default is 1. |
| `type Message` | The message type. |
| `handle(&self, message, ctx)` | The handler. |
