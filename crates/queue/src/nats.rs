use std::any::{TypeId, type_name};
use std::collections::HashSet;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use async_nats::jetstream;

use crate::{Encode, Headers, Producer, Result};

/// Handle to NATS. Cheap to clone; share it with anything that publishes.
#[derive(Clone)]
pub struct Nats {
    inner: Arc<Inner>,
}

struct Inner {
    jetstream: jetstream::Context,
    registry: RwLock<Registry>,
}

#[derive(Default)]
struct Registry {
    producers: HashSet<TypeId>,
    headers: Headers,
}

impl Nats {
    pub async fn connect(url: &str) -> Result<Self> {
        let client = async_nats::connect(url).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                jetstream: jetstream::new(client),
                registry: RwLock::default(),
            }),
        })
    }

    pub(crate) fn jetstream(&self) -> &jetstream::Context {
        &self.inner.jetstream
    }

    pub(crate) fn register_producer<P: Producer>(&self) {
        self.registry_mut().producers.insert(TypeId::of::<P>());
    }

    pub(crate) fn set_header(&self, name: String, value: String) {
        self.registry_mut().headers.insert(name, value);
    }

    fn registry_mut(&self) -> std::sync::RwLockWriteGuard<'_, Registry> {
        self.inner.registry.write().unwrap_or_else(|e| e.into_inner())
    }

    /// Default headers, or an error if `P` was never registered.
    fn producer_headers<P: Producer>(&self) -> Result<Headers> {
        let registry = self.inner.registry.read().unwrap_or_else(|e| e.into_inner());
        if !registry.producers.contains(&TypeId::of::<P>()) {
            return Err(format!("producer `{}` is not registered", type_name::<P>()).into());
        }
        Ok(registry.headers.clone())
    }
}

impl AsRef<Nats> for Nats {
    fn as_ref(&self) -> &Nats {
        self
    }
}

/// Pending publish from [`Producer::publish`]. Does nothing until awaited.
///
/// Headers are layered: app defaults, then [`Producer::headers`], then [`Publish::header`].
#[must_use = "a publish does nothing until awaited"]
pub struct Publish<P: Producer> {
    nats: Nats,
    message: P::Message,
    headers: Headers,
}

impl<P: Producer> Publish<P> {
    pub(crate) fn new(nats: Nats, message: P::Message) -> Self {
        Self {
            nats,
            message,
            headers: Headers::new(),
        }
    }

    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name, value);
        self
    }

    async fn send(self) -> Result {
        let mut headers = self.nats.producer_headers::<P>()?;
        P::headers(&self.message, &mut headers);
        headers.extend(self.headers);

        let subject = P::subject(&self.message).into_owned();
        let payload = self.message.encode()?;
        self.nats
            .jetstream()
            .publish_with_headers(subject, headers.to_nats(), payload.into())
            .await?
            .await?;
        Ok(())
    }
}

impl<P: Producer> IntoFuture for Publish<P> {
    type Output = Result;
    type IntoFuture = Pin<Box<dyn Future<Output = Result> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.send())
    }
}
