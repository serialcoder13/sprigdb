//! The boundary between the AMQP front ends and the queue engine
//! (chapter 50's `sprig-queues`): [`QueueBackend`] and [`Vhost`].
//!
//! The traits follow chapter 50's algorithms so the MariaDB-backed engine
//! (queue owners, writers, the outbox) plugs in without changing the front
//! ends:
//!
//! - **Routing is synchronous** ([`Vhost::publish`]): the route cache is
//!   in memory, so `mandatory` returns and per-channel ordering are
//!   decided at the call, in publish order. The publisher confirm is a
//!   future that resolves after the owner's commit (chapter 50,
//!   "Publishing", steps 5–7), so a channel pipelines publishes and
//!   confirms arrive as commits finish.
//! - **Consumers are credit-based** ([`Vhost::credit`]): 0-9-1 prefetch
//!   and AMQP 1.0 link credit are both credit at the owner (chapter 50,
//!   "Dispatch, credit and consumers"). Deliveries come back as
//!   [`Event`]s on the connection's [`EventSender`].
//! - **Settlements are batched and fire-and-forget** ([`Vhost::settle`]):
//!   the owner applies them in its next write batch.
//! - **Transactions** ([`Vhost::commit_tx`]): a 0-9-1 `tx.commit` hands
//!   its buffered publishes and settlements over in one call, which the
//!   engine makes one MariaDB transaction (chapter 50, "Publishing inside
//!   a database transaction").
//!
//! Permissions (chapter 50, "Security notes") are the engine's to check:
//! it knows the catalog and the grants.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures::future::BoxFuture;
use tokio::sync::mpsc;

use crate::auth::Principal;
use crate::error::MqError;
use crate::message::{Delivery, DeliveryId, Message};
use crate::v091::codec::FieldTable;

/// A connection, unique in the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnKey(pub u64);

impl ConnKey {
    /// A new key.
    #[must_use]
    pub fn next() -> Self {
        static N: AtomicU64 = AtomicU64::new(1);
        ConnKey(N.fetch_add(1, Ordering::Relaxed))
    }
}

/// A consumer (a 0-9-1 `basic.consume` or a 1.0 sending link), unique in
/// the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsumerId(pub u64);

impl ConsumerId {
    /// A new id.
    #[must_use]
    pub fn next() -> Self {
        static N: AtomicU64 = AtomicU64::new(1);
        ConsumerId(N.fetch_add(1, Ordering::Relaxed))
    }
}

/// Who calls, from where.
#[derive(Debug, Clone)]
pub struct OpCtx {
    /// The logged-in user.
    pub principal: Arc<Principal>,
    /// The connection (exclusive queues belong to it).
    pub conn: ConnKey,
    /// The 0-9-1 channel or 1.0 session: lanes are picked by it, so one
    /// channel's messages keep their order.
    pub channel: u16,
}

/// What the engine sends a connection.
#[derive(Debug)]
pub enum Event {
    /// A message for a consumer of this connection.
    Deliver {
        /// The consumer.
        consumer: ConsumerId,
        /// The delivery.
        delivery: Delivery,
    },
    /// The consumer ended on the engine's side: its queue was deleted, or
    /// it timed out (chapter 50, "Cancel notifications").
    Cancelled {
        /// The consumer.
        consumer: ConsumerId,
        /// Why.
        reason: MqError,
    },
    /// A drain finished: the consumer's remaining credit was used up with
    /// nothing to deliver (AMQP 1.0 `drain`).
    Drained {
        /// The consumer.
        consumer: ConsumerId,
        /// Credit given up.
        credit: u32,
    },
    /// Publishing is held back (quotas, write back-pressure): 0-9-1
    /// `connection.blocked`.
    Blocked(String),
    /// Publishing may resume.
    Unblocked,
}

/// Where the engine sends a connection's [`Event`]s. Unbounded: credit
/// bounds the deliveries in it.
pub type EventSender = mpsc::UnboundedSender<Event>;

/// `exchange.declare`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExchangeSpec {
    /// The name.
    pub name: String,
    /// `direct`, `fanout`, `topic`, `headers`, `x-delayed-message`.
    pub kind: String,
    /// Durable.
    pub durable: bool,
    /// Deleted when its last binding goes.
    pub auto_delete: bool,
    /// Not published to by clients.
    pub internal: bool,
    /// `alternate-exchange`, `x-delayed-type`, ...
    pub arguments: FieldTable,
}

/// `queue.declare`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueueSpec {
    /// The name; empty for a server-named queue.
    pub name: String,
    /// Durable.
    pub durable: bool,
    /// Owned by the declaring connection.
    pub exclusive: bool,
    /// Deleted when its last consumer goes.
    pub auto_delete: bool,
    /// `x-*` arguments.
    pub arguments: FieldTable,
}

/// `queue.declare-ok`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDeclared {
    /// The name (server-named queues get theirs here).
    pub name: String,
    /// Ready messages.
    pub message_count: u32,
    /// Consumers.
    pub consumer_count: u32,
}

/// Whether a binding's destination is a queue or an exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DestKind {
    /// `queue.bind`.
    Queue,
    /// `exchange.bind`.
    Exchange,
}

/// A binding.
#[derive(Debug, Clone, PartialEq)]
pub struct BindingSpec {
    /// The source exchange.
    pub source: String,
    /// The destination's name.
    pub destination: String,
    /// Queue or exchange.
    pub dest_kind: DestKind,
    /// The binding key.
    pub key: String,
    /// Arguments (headers exchanges: `x-match` and the values).
    pub arguments: FieldTable,
}

/// The result of a publish.
pub struct Published {
    /// How many queues the message reached (0: unroutable).
    pub routed: u32,
    /// Resolves when every destination committed (`Ok`: `basic.ack`, 1.0
    /// `accepted`), or with the refusal (`basic.nack`, 1.0 `rejected`).
    pub confirm: BoxFuture<'static, Result<(), MqError>>,
}

impl std::fmt::Debug for Published {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Published")
            .field("routed", &self.routed)
            .finish_non_exhaustive()
    }
}

impl Published {
    /// A publish that is already confirmed (unroutable, or a direct
    /// reply).
    #[must_use]
    pub fn done(routed: u32) -> Self {
        Self {
            routed,
            confirm: Box::pin(async { Ok(()) }),
        }
    }
}

/// `basic.consume`, or a 1.0 sending link.
#[derive(Debug, Clone)]
pub struct ConsumeSpec {
    /// The consumer's id (the front end assigns it).
    pub consumer: ConsumerId,
    /// The queue.
    pub queue: String,
    /// The consumer tag (0-9-1) or link name (1.0), for listings.
    pub tag: String,
    /// Settled on delivery (0-9-1 `no_ack`, 1.0 pre-settled).
    pub no_ack: bool,
    /// Refuse other consumers.
    pub exclusive: bool,
    /// `x-priority`.
    pub priority: i32,
    /// Other arguments (`x-stream-offset`, ...).
    pub arguments: FieldTable,
}

/// Why a message is dead-lettered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeadLetterReason {
    /// Rejected without requeue.
    Rejected,
    /// TTL passed.
    Expired,
    /// Dropped by a length limit.
    Maxlen,
    /// Over `x-delivery-limit`.
    DeliveryLimit,
}

impl DeadLetterReason {
    /// The `x-death` reason.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            DeadLetterReason::Rejected => "rejected",
            DeadLetterReason::Expired => "expired",
            DeadLetterReason::Maxlen => "maxlen",
            DeadLetterReason::DeliveryLimit => "delivery_limit",
        }
    }
}

/// How a delivery ends (chapter 50, "Acknowledgements").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settlement {
    /// `basic.ack`, 1.0 `accepted`: delete.
    Ack,
    /// Back to ready at its position. `failed` counts toward
    /// `x-delivery-count`: a reject or crash, not a nack or release.
    Requeue {
        /// Counts as a failed delivery.
        failed: bool,
    },
    /// Dead-letter, or delete when the queue has no dead-letter exchange.
    DeadLetter(DeadLetterReason),
}

/// A 0-9-1 transaction's buffered work.
#[derive(Debug, Clone, Default)]
pub struct TxBatch {
    /// Publishes in order, with their `mandatory` flags.
    pub publishes: Vec<(Message, bool)>,
    /// Settlements in order.
    pub settlements: Vec<(DeliveryId, Settlement)>,
}

/// The queue engine for a gateway.
#[async_trait]
pub trait QueueBackend: Send + Sync + 'static {
    /// Opens virtual host `vhost` (`connection.open`, 1.0 `open.hostname`)
    /// for a connection: maps it to the tenant's logical database and
    /// checks the user may use it. `events` receives the connection's
    /// deliveries and notices until [`Vhost::connection_closed`].
    ///
    /// # Errors
    /// 39250 for an unknown vhost, 39253 for no access.
    async fn open_vhost(
        &self,
        principal: &Principal,
        vhost: &str,
        conn: ConnKey,
        events: EventSender,
    ) -> Result<Arc<dyn Vhost>, MqError>;
}

/// One virtual host, as one connection uses it.
#[async_trait]
pub trait Vhost: Send + Sync + 'static {
    /// The vhost name.
    fn name(&self) -> &str;

    /// `exchange.declare`; `passive` only checks it exists.
    ///
    /// # Errors
    /// 404 (passive, missing), 406 (inequivalent), 403 (reserved names,
    /// permissions), 503 (unknown type).
    async fn declare_exchange(
        &self,
        cx: &OpCtx,
        spec: ExchangeSpec,
        passive: bool,
    ) -> Result<(), MqError>;

    /// `exchange.delete`.
    ///
    /// # Errors
    /// 406 (`if_unused` and bound), 403.
    async fn delete_exchange(&self, cx: &OpCtx, name: &str, if_unused: bool)
    -> Result<(), MqError>;

    /// `queue.declare`; `passive` only checks it exists.
    ///
    /// # Errors
    /// 404, 405 (exclusive elsewhere), 406, 403.
    async fn declare_queue(
        &self,
        cx: &OpCtx,
        spec: QueueSpec,
        passive: bool,
    ) -> Result<QueueDeclared, MqError>;

    /// `queue.delete`: the number of messages deleted.
    ///
    /// # Errors
    /// 406 (`if_unused`/`if_empty`), 405, 403.
    async fn delete_queue(
        &self,
        cx: &OpCtx,
        name: &str,
        if_unused: bool,
        if_empty: bool,
    ) -> Result<u32, MqError>;

    /// `queue.purge`: the number of ready messages removed.
    ///
    /// # Errors
    /// 404, 405, 403.
    async fn purge_queue(&self, cx: &OpCtx, name: &str) -> Result<u32, MqError>;

    /// `queue.bind`, `exchange.bind`.
    ///
    /// # Errors
    /// 404, 403.
    async fn bind(&self, cx: &OpCtx, b: BindingSpec) -> Result<(), MqError>;

    /// `queue.unbind`, `exchange.unbind`.
    ///
    /// # Errors
    /// 404, 403.
    async fn unbind(&self, cx: &OpCtx, b: BindingSpec) -> Result<(), MqError>;

    /// Routes and enqueues one message. Called in publish order per
    /// channel; must not block.
    ///
    /// # Errors
    /// 404 for a missing exchange, 403, 406 (too large, bad delay).
    fn publish(&self, cx: &OpCtx, m: Message, mandatory: bool) -> Result<Published, MqError>;

    /// Starts a consumer with no credit; deliveries follow
    /// [`Vhost::credit`].
    ///
    /// # Errors
    /// 404, 403, 405 (exclusive queue elsewhere), 403 (exclusive consumer
    /// conflict).
    async fn consume(&self, cx: &OpCtx, spec: ConsumeSpec) -> Result<(), MqError>;

    /// Ends a consumer; its unsettled deliveries stay with the connection
    /// until settled or the connection ends.
    async fn cancel(&self, cx: &OpCtx, consumer: ConsumerId);

    /// Adds `credit` deliveries for `consumer`. With `drain`, credit left
    /// when the queue is empty is given up with [`Event::Drained`].
    fn credit(&self, consumer: ConsumerId, credit: u32, drain: bool);

    /// `basic.get`: one message and the ready count after it.
    ///
    /// # Errors
    /// 404, 403, 405.
    async fn get(
        &self,
        cx: &OpCtx,
        queue: &str,
        no_ack: bool,
    ) -> Result<Option<(Delivery, u32)>, MqError>;

    /// Settles deliveries.
    fn settle(&self, cx: &OpCtx, s: Vec<(DeliveryId, Settlement)>);

    /// `tx.commit`: the publishes and settlements as one unit. Returns
    /// each publish's queue count, in order (for `basic.return`).
    ///
    /// # Errors
    /// The first failure; nothing is applied.
    async fn commit_tx(&self, cx: &OpCtx, tx: TxBatch) -> Result<Vec<u32>, MqError>;

    /// The connection ended: exclusive queues go, its consumers end.
    /// Unsettled deliveries were already requeued by the front end.
    async fn connection_closed(&self, conn: ConnKey);
}
