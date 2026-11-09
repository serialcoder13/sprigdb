//! Messages as the queue engine stores them: in the encoding they were
//! published in (chapter 50, "Message encoding"), with the few fields the
//! engine reads (headers for headers exchanges, priority, TTL) parsed once
//! at publish.

use std::sync::Arc;

use bytes::Bytes;

use crate::v091::codec::FieldTable;

/// Which protocol's encoding `props` and `body` are in (`__sprig_qm.fmt`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MsgFormat {
    /// `props`: a 0-9-1 content header's property flags and list; `body`:
    /// the body.
    Amqp091,
    /// `props`: the 1.0 `header` and `message-annotations` sections as
    /// published (delivery annotations are dropped); `body`: the bare
    /// message, `properties` through the body sections and `footer`, byte
    /// for byte as published.
    Amqp10,
}

/// What the engine reads from a message.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MessageInfo {
    /// 0-9-1 `headers`, or 1.0 `application-properties` with simple
    /// values: what headers exchanges match and `x-delay` is read from.
    pub headers: FieldTable,
    /// The priority, 0–255.
    pub priority: Option<u8>,
    /// The per-message TTL in milliseconds (0-9-1 `expiration`, 1.0
    /// `header.ttl`).
    pub ttl_ms: Option<u64>,
    /// 0-9-1 `delivery_mode = 2` or 1.0 `header.durable`.
    pub durable: bool,
    /// The message id, as text.
    pub message_id: Option<String>,
    /// The reply address, as published (0-9-1 queue name or 1.0 address).
    pub reply_to: Option<String>,
    /// Bytes of `props` and `body`, for length limits and quotas.
    pub size: u64,
}

/// One published message.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// The encoding.
    pub format: MsgFormat,
    /// The exchange it was published to.
    pub exchange: String,
    /// The routing key it was published with.
    pub routing_key: String,
    /// Properties in the source encoding (see [`MsgFormat`]).
    pub props: Bytes,
    /// The body in the source encoding (see [`MsgFormat`]).
    pub body: Bytes,
    /// Parsed at publish.
    pub info: MessageInfo,
}

/// Where a delivered message sits in its queue: chapter 50's `MsgKey`.
/// The engine settles by it; front ends keep it opaque.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct DeliveryId {
    /// `objects.object_id` of the queue (0 for direct reply-to).
    pub queue: u64,
    /// The lane.
    pub lane: u16,
    /// The priority class.
    pub pclass: u8,
    /// The owner-assigned sequence number.
    pub seq: u64,
}

/// One delivery of a message to a consumer or a `basic.get`.
#[derive(Debug, Clone, PartialEq)]
pub struct Delivery {
    /// What to settle.
    pub id: DeliveryId,
    /// The queue name.
    pub queue: String,
    /// The message, shared by every queue it was routed to.
    pub message: Arc<Message>,
    /// Delivered before (0-9-1 `redelivered`; 1.0 `delivery-count > 0`).
    pub redelivered: bool,
    /// Failed deliveries so far (`x-delivery-count`, 1.0
    /// `header.delivery-count`).
    pub delivery_count: u32,
    /// Delivery-time headers the front end adds without changing the
    /// stored message: `x-sprig-delivery`, `x-death`, `x-stream-offset`,
    /// `x-delivery-count` for quorum queues.
    pub annotations: FieldTable,
}
