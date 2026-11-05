use std::sync::Arc;

use queue::{App, Consumer, Context, Headers, Message, Nats, Producer, Result};

#[derive(Clone)]
struct AppState {
    warehouse: Arc<str>,
}

struct OrderPlaced;

impl Producer for OrderPlaced {
    const SUBJECT: &'static str = "orders.placed";
    type Message = String;
}

struct OrderShipped;

impl Producer for OrderShipped {
    const SUBJECT: &'static str = "orders.shipped";
    type Message = String;

    fn headers(_message: &String, headers: &mut Headers) {
        headers.insert("x-event", "order-shipped");
    }
}

struct ShipOrder;

impl Consumer<AppState> for ShipOrder {
    const STREAM: &'static str = "ORDERS";
    const DURABLE: &'static str = "ship-order";
    const SUBJECT: &'static str = "orders.placed";
    const CONCURRENCY: usize = 4;
    type Message = String;

    async fn handle(&self, message: Message<String>, ctx: Context<AppState>) -> Result {
        println!("shipping {} from {}", message.body, ctx.state.warehouse);
        let trace = message.header("x-trace").unwrap_or("none").to_string();
        OrderShipped::publish(&ctx, message.body)
            .header("x-trace", trace)
            .await
    }
}

/// Works with any state, so it does not need to name `AppState`.
struct LogShipped;

impl<S: Send + Sync> Consumer<S> for LogShipped {
    const STREAM: &'static str = "ORDERS";
    const DURABLE: &'static str = "log-shipped";
    const SUBJECT: &'static str = "orders.shipped";
    type Message = String;

    async fn handle(&self, message: Message<String>, _ctx: Context<S>) -> Result {
        let header = |name| message.header(name).unwrap_or("-");
        println!(
            "shipped {} (service={}, event={}, trace={})",
            message.body,
            header("x-service"),
            header("x-event"),
            header("x-trace"),
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result {
    let nats = Nats::connect("nats://127.0.0.1:4222").await?;

    let running = App::new(nats.clone())
        .stream("ORDERS", ["orders.>"])
        .producer::<OrderPlaced>()
        .producer::<OrderShipped>()
        .consumer(ShipOrder)
        .consumer(LogShipped)
        .header("x-service", "packstack")
        .with_state(AppState {
            warehouse: "gothenburg".into(),
        })
        .start()
        .await?;

    for i in 1..=5 {
        OrderPlaced::publish(&nats, format!("order-{i}"))
            .header("x-trace", format!("trace-{i}"))
            .await?;
    }

    running.run().await
}
