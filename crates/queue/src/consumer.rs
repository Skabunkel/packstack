use std::future::Future;
use std::sync::Arc;

use async_nats::jetstream::{self, AckKind, consumer::pull};
use futures_util::StreamExt;
use tokio::sync::{Semaphore, watch};

use crate::{Decode, Headers, Nats, Result};

/// Handles messages from a durable JetStream pull consumer.
///
/// `S` is the app state, handed to every call through [`Context`].
/// Returning `Ok` acks the message, `Err` naks it for redelivery.
/// Messages that fail to decode are terminated.
///
/// ```ignore
/// struct ShipOrder;
///
/// impl Consumer<AppState> for ShipOrder {
///     const STREAM: &'static str = "ORDERS";
///     const DURABLE: &'static str = "ship-order";
///     const SUBJECT: &'static str = "orders.placed";
///     const CONCURRENCY: usize = 4;
///     type Message = String;
///
///     async fn handle(&self, msg: Message<String>, ctx: Context<AppState>) -> Result {
///         Ok(())
///     }
/// }
/// ```
pub trait Consumer<S = ()>: Send + Sync + 'static {
    /// Stream to consume from. Declare it with [`App::stream`](crate::App::stream).
    const STREAM: &'static str;
    /// Durable name. Instances sharing it compete for messages.
    const DURABLE: &'static str;
    /// Routing key filter; wildcards allowed.
    const SUBJECT: &'static str;
    /// Messages handled at once by this process.
    const CONCURRENCY: usize = 1;

    type Message: Decode + Send + 'static;

    fn handle(
        &self,
        message: Message<Self::Message>,
        ctx: Context<S>,
    ) -> impl Future<Output = Result> + Send;
}

/// Received message with its decoded body.
#[derive(Debug)]
pub struct Message<T> {
    pub body: T,
    subject: String,
    headers: Headers,
    delivered: i64,
}

impl<T> Message<T> {
    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)
    }

    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// Delivery attempt, starting at 1.
    pub fn delivered(&self) -> i64 {
        self.delivered
    }
}

/// Shared state and NATS handle passed to every [`Consumer::handle`] call.
#[derive(Clone)]
pub struct Context<S> {
    pub state: S,
    pub nats: Nats,
}

impl<S> AsRef<Nats> for Context<S> {
    fn as_ref(&self) -> &Nats {
        &self.nats
    }
}

/// Creates or updates the durable consumer, returning its message loop.
pub(crate) async fn start<C, S>(
    consumer: C,
    ctx: Context<S>,
    shutdown: watch::Receiver<bool>,
) -> Result<impl Future<Output = Result> + Send>
where
    C: Consumer<S>,
    S: Clone + Send + Sync + 'static,
{
    let pull = ctx
        .nats
        .jetstream()
        .get_stream(C::STREAM)
        .await?
        .create_consumer(pull::Config {
            durable_name: Some(C::DURABLE.to_string()),
            filter_subject: C::SUBJECT.to_string(),
            ..Default::default()
        })
        .await?;

    Ok(run(Arc::new(consumer), pull, ctx, shutdown))
}

async fn run<C, S>(
    consumer: Arc<C>,
    pull: jetstream::consumer::PullConsumer,
    ctx: Context<S>,
    mut shutdown: watch::Receiver<bool>,
) -> Result
where
    C: Consumer<S>,
    S: Clone + Send + Sync + 'static,
{
    let concurrency = C::CONCURRENCY.max(1);
    let permits = Arc::new(Semaphore::new(concurrency));
    let mut messages = pull.messages().await?;

    loop {
        let permit = permits.clone().acquire_owned().await?;
        let next = tokio::select! {
            _ = shutdown.wait_for(|stop| *stop) => break,
            next = messages.next() => next,
        };
        let message = match next {
            Some(Ok(message)) => message,
            Some(Err(err)) => {
                tracing::warn!(consumer = C::DURABLE, %err, "failed to pull message");
                continue;
            }
            None => return Err(format!("message stream for `{}` closed", C::DURABLE).into()),
        };

        let (consumer, ctx) = (consumer.clone(), ctx.clone());
        tokio::spawn(async move {
            handle(&*consumer, message, ctx).await;
            drop(permit);
        });
    }

    // Let in-flight handlers finish before reporting shutdown.
    let _ = permits.acquire_many(concurrency as u32).await;
    Ok(())
}

async fn handle<C, S>(consumer: &C, message: jetstream::Message, ctx: Context<S>)
where
    C: Consumer<S>,
{
    let body = match C::Message::decode(&message.payload) {
        Ok(body) => body,
        Err(err) => {
            tracing::error!(consumer = C::DURABLE, %err, "dropping undecodable message");
            ack(&message, AckKind::Term).await;
            return;
        }
    };

    let received = Message {
        body,
        subject: message.subject.to_string(),
        headers: Headers::from_nats(message.headers.as_ref()),
        delivered: message.info().map(|info| info.delivered).unwrap_or(1),
    };

    match consumer.handle(received, ctx).await {
        Ok(()) => ack(&message, AckKind::Ack).await,
        Err(err) => {
            tracing::warn!(consumer = C::DURABLE, %err, "handler failed, message will be redelivered");
            ack(&message, AckKind::Nak(None)).await;
        }
    }
}

async fn ack(message: &jetstream::Message, kind: AckKind) {
    if let Err(err) = message.ack_with(kind).await {
        tracing::warn!(%err, "failed to ack message");
    }
}
