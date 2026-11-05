use std::borrow::Cow;

use crate::{Encode, Headers, Nats, Publish};

/// Describes an outgoing JetStream message.
///
/// Register it with [`App::producer`](crate::App::producer) before publishing,
/// otherwise [`Publish`] fails.
///
/// ```ignore
/// struct OrderPlaced;
///
/// impl Producer for OrderPlaced {
///     const SUBJECT: &'static str = "orders.placed";
///     type Message = String;
/// }
///
/// OrderPlaced::publish(&nats, "order-1".into()).header("x-trace", "abc").await?;
/// ```
pub trait Producer: Sized + 'static {
    /// Routing key the message is published on.
    const SUBJECT: &'static str;

    type Message: Encode + Send + 'static;

    /// Override to route on message content, e.g. `orders.{region}.placed`.
    fn subject(_message: &Self::Message) -> Cow<'static, str> {
        Cow::Borrowed(Self::SUBJECT)
    }

    /// Headers added to every message from this producer.
    fn headers(_message: &Self::Message, _headers: &mut Headers) {}

    /// Starts a publish. Add per-message headers on the builder, then `.await` it.
    fn publish(nats: impl AsRef<Nats>, message: Self::Message) -> Publish<Self> {
        Publish::new(nats.as_ref().clone(), message)
    }
}
