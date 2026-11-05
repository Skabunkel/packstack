//! Small JetStream toolkit with an axum-like builder.
//!
//! - [`Producer`]: a type that describes an outgoing message (subject, payload, headers).
//! - [`Consumer`]: a type that handles messages from a durable pull consumer.
//! - [`App`]: wires streams, producers, consumers and shared state together.
//!
//! The NATS client never leaks out of this crate; everything goes through [`Nats`].

mod app;
mod codec;
mod consumer;
mod headers;
mod nats;
mod producer;

pub use app::{App, Running};
#[cfg(feature = "json")]
pub use codec::Json;
pub use codec::{Decode, Encode};
pub use consumer::{Consumer, Context, Message};
pub use headers::Headers;
pub use nats::{Nats, Publish};
pub use producer::Producer;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T = (), E = Error> = std::result::Result<T, E>;
