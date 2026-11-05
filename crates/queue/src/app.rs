use std::future::Future;
use std::pin::Pin;

use async_nats::jetstream::stream;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinSet};

use crate::consumer::{self, Consumer, Context};
use crate::{Nats, Producer, Result};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type Starter<S> =
    Box<dyn FnOnce(S, Nats, watch::Receiver<bool>) -> BoxFuture<Result<BoxFuture<Result>>> + Send>;

/// Builder that wires streams, producers, consumers and state.
///
/// Like axum's `Router<S>`, `S` is the state still missing. Consumers can be added
/// before [`App::with_state`]; [`App::start`] is only available once it is `()`.
///
/// ```ignore
/// App::new(nats)
///     .stream("ORDERS", ["orders.>"])
///     .producer::<OrderPlaced>()
///     .consumer(ShipOrder)
///     .header("x-service", "packstack")
///     .with_state(state)
///     .run()
///     .await?;
/// ```
pub struct App<S = ()> {
    nats: Nats,
    streams: Vec<(String, Vec<String>)>,
    consumers: Vec<Starter<S>>,
}

impl<S> App<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn new(nats: Nats) -> Self {
        Self {
            nats,
            streams: Vec::new(),
            consumers: Vec::new(),
        }
    }

    /// Declares a stream. Created or updated on start.
    pub fn stream<I>(mut self, name: &str, subjects: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        let subjects = subjects.into_iter().map(Into::into).collect();
        self.streams.push((name.to_string(), subjects));
        self
    }

    /// Allows `P` to publish through this app's [`Nats`] handle.
    pub fn producer<P: Producer>(self) -> Self {
        self.nats.register_producer::<P>();
        self
    }

    /// Header added to every outgoing message.
    pub fn header(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.nats.set_header(name.into(), value.into());
        self
    }

    pub fn consumer<C: Consumer<S>>(mut self, consumer: C) -> Self {
        self.consumers.push(Box::new(move |state, nats, shutdown| {
            Box::pin(async move {
                let run = consumer::start(consumer, Context { state, nats }, shutdown).await?;
                Ok(Box::pin(run) as BoxFuture<Result>)
            })
        }));
        self
    }

    /// Provides the state consumers receive in [`Context::state`].
    pub fn with_state<S2>(self, state: S) -> App<S2> {
        let consumers = self
            .consumers
            .into_iter()
            .map(|start| {
                let state = state.clone();
                Box::new(move |_: S2, nats, shutdown| start(state, nats, shutdown)) as Starter<S2>
            })
            .collect();
        App {
            nats: self.nats,
            streams: self.streams,
            consumers,
        }
    }
}

impl App<()> {
    /// Creates streams and consumers, then starts consuming in the background.
    pub async fn start(self) -> Result<Running> {
        let jetstream = self.nats.jetstream();
        for (name, subjects) in self.streams {
            jetstream
                .create_or_update_stream(stream::Config {
                    name,
                    subjects,
                    ..Default::default()
                })
                .await?;
        }

        let (shutdown, receiver) = watch::channel(false);
        let mut tasks = JoinSet::new();
        for start in self.consumers {
            tasks.spawn(start((), self.nats.clone(), receiver.clone()).await?);
        }
        Ok(Running { tasks, shutdown })
    }

    /// [`App::start`] then [`Running::run`].
    pub async fn run(self) -> Result {
        self.start().await?.run().await
    }
}

/// Started app. Dropping it aborts all consumers.
pub struct Running {
    tasks: JoinSet<Result>,
    shutdown: watch::Sender<bool>,
}

impl Running {
    /// Runs until Ctrl+C.
    pub async fn run(self) -> Result {
        self.run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    }

    /// Runs until `signal` resolves or a consumer fails, then drains in-flight messages.
    pub async fn run_until(mut self, signal: impl Future<Output = ()>) -> Result {
        let mut result = tokio::select! {
            _ = signal => Ok(()),
            Some(joined) = self.tasks.join_next() => flatten(joined).and(Err("consumer stopped".into())),
        };

        let _ = self.shutdown.send(true);
        while let Some(joined) = self.tasks.join_next().await {
            if let Err(err) = flatten(joined) {
                result = result.and(Err(err));
            }
        }
        result
    }
}

fn flatten(joined: Result<Result, JoinError>) -> Result {
    joined?
}
