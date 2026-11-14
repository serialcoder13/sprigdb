//! What the queue owner keeps durable: the
//! [`MsgStore`] boundary, served by MariaDB ([`crate::maria::MariaStore`],
//! the `__sprig_q*` tables of a shard schema) and by memory
//! ([`crate::memstore::MemStore`]: temporary queues, tests and the
//! simulation).
//!
//! Every owner write is one [`WriteBatch`] committed atomically, fenced by
//! the owner epoch: the lane state row is updated first, `WHERE
//! owner_epoch = ?`, and a zero row count rolls the batch back
//! ([`StoreError::Fenced`]).

use async_trait::async_trait;
use bytes::Bytes;

/// A message id (UUIDv7 bytes): the dedup key with the queue (`uk_msg`).
pub type MsgId = [u8; 16];

/// A new message id.
#[must_use]
pub fn new_msg_id() -> MsgId {
    *uuid::Uuid::now_v7().as_bytes()
}

/// The milliseconds a UUIDv7 message id was made at.
#[must_use]
pub fn msg_id_ms(id: &MsgId) -> i64 {
    let mut b = [0u8; 8];
    b[2..].copy_from_slice(&id[..6]);
    i64::try_from(u64::from_be_bytes(b)).unwrap_or(i64::MAX)
}

/// The smallest message id made at `ms` (UUIDv7 order is time order):
/// ids below it were made before `ms`.
#[must_use]
pub fn msg_id_floor(ms: i64) -> MsgId {
    let mut a = [0u8; 16];
    let b = u64::try_from(ms.max(0)).unwrap_or(0).to_be_bytes();
    a[..6].copy_from_slice(&b[2..]);
    a
}

/// Why a store call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// The owner epoch moved: another owner took over.
    Fenced,
    /// Anything else; the batch may or may not have committed.
    Failed(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Fenced => f.write_str("owner epoch moved (fenced)"),
            StoreError::Failed(s) => write!(f, "queue store: {s}"),
        }
    }
}

impl std::error::Error for StoreError {}

/// A message row (`__sprig_qm`).
#[derive(Debug, Clone, PartialEq)]
pub struct StoredMsg {
    /// The lane.
    pub lane: u16,
    /// Priority class (0 first).
    pub pclass: u8,
    /// Owner-assigned order.
    pub seq: u64,
    /// The id.
    pub msg_id: MsgId,
    /// 0 = AMQP 0-9-1, 1 = AMQP 1.0.
    pub fmt: u8,
    /// Published to.
    pub exchange: String,
    /// Published with.
    pub routing_key: String,
    /// Properties in the source encoding.
    pub props: Bytes,
    /// The body.
    pub body: Bytes,
    /// Expiry, epoch milliseconds.
    pub expires_at_ms: Option<i64>,
    /// Failed deliveries.
    pub delivery_count: u32,
    /// `x-death` as an encoded AMQP field array (in a one-entry table).
    pub deaths: Option<Bytes>,
}

impl StoredMsg {
    /// `props` and `body` bytes.
    #[must_use]
    pub fn size(&self) -> u64 {
        (self.props.len() + self.body.len()) as u64
    }
}

/// Where a message sits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MsgRef {
    /// The lane.
    pub lane: u16,
    /// Priority class.
    pub pclass: u8,
    /// Sequence.
    pub seq: u64,
    /// The id (the delete checks it).
    pub msg_id: MsgId,
}

/// The owner's per-lane state (`__sprig_qs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LaneState {
    /// Next sequence number.
    pub next_seq: u64,
    /// Highest sequence ever handed out.
    pub delivered_hwm: u64,
    /// Ready messages.
    pub ready_count: u64,
    /// Their bytes.
    pub ready_bytes: u64,
}

/// What a takeover finds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Claim {
    /// The new epoch (the old plus one).
    pub epoch: u64,
    /// The lane's state.
    pub state: LaneState,
    /// Messages on disk (counted, not read from the counters).
    pub count: u64,
    /// Their bytes.
    pub bytes: u64,
}

/// An outbox row's kind (`__sprig_qo.kind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutKind {
    /// A transactional publish: `dest` names queues.
    Publish,
    /// A dead letter: routed by exchange when forwarded.
    DeadLetter,
    /// A delayed message that is due: routed by its exchange's delayed type.
    Delayed,
}

impl OutKind {
    /// The stored name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            OutKind::Publish => "publish",
            OutKind::DeadLetter => "dead_letter",
            OutKind::Delayed => "delayed",
        }
    }

    /// Parses a stored name.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "publish" => OutKind::Publish,
            "dead_letter" => OutKind::DeadLetter,
            "delayed" => OutKind::Delayed,
            _ => return None,
        })
    }
}

/// Where an outbox row goes.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OutDest {
    /// Queues by id, routed at publish.
    Queues(Vec<u64>),
    /// An exchange and key, routed when forwarded.
    Exchange {
        /// The exchange.
        name: String,
        /// The routing key.
        key: String,
    },
}

/// An outbox row (`__sprig_qo`).
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxRow {
    /// Its id (UUIDv7); also the forwarded message's id, so a forward
    /// replayed after a crash is a no-op (`uk_msg`).
    pub out_id: MsgId,
    /// The kind.
    pub kind: OutKind,
    /// Where it goes.
    pub dest: OutDest,
    /// The message's own id (as published).
    pub msg_id: MsgId,
    /// Encoding.
    pub fmt: u8,
    /// Published to.
    pub exchange: String,
    /// Published with.
    pub routing_key: String,
    /// Properties.
    pub props: Bytes,
    /// Body.
    pub body: Bytes,
    /// `x-death` (dead letters).
    pub deaths: Option<Bytes>,
}

/// A delayed message (`__sprig_qd`).
#[derive(Debug, Clone, PartialEq)]
pub struct DelayedRow {
    /// The `x-delayed-message` exchange.
    pub exchange_id: u64,
    /// The exchange's name (routing when due).
    pub exchange: String,
    /// Due, epoch milliseconds.
    pub due_ms: i64,
    /// The id.
    pub msg_id: MsgId,
    /// Routing key.
    pub routing_key: String,
    /// Encoding.
    pub fmt: u8,
    /// Properties.
    pub props: Bytes,
    /// Body.
    pub body: Bytes,
}

/// One owner write.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WriteBatch {
    /// The queue.
    pub queue: u64,
    /// The lane.
    pub lane: u16,
    /// The writer's epoch (the fence).
    pub epoch: u64,
    /// New messages.
    pub inserts: Vec<StoredMsg>,
    /// Acknowledged or dead-lettered messages.
    pub deletes: Vec<MsgRef>,
    /// Requeued messages' new delivery counts.
    pub counts: Vec<(MsgRef, u32)>,
    /// Dead letters, in the same transaction as their deletes.
    pub outbox: Vec<OutboxRow>,
    /// Ids of inserted messages that came from an outbox: recorded in the
    /// forward ledger (`__sprig_qf`) in the same transaction, so a forward
    /// replayed after the message was consumed is still a no-op.
    pub forwarded: Vec<MsgId>,
    /// The lane state after the batch.
    pub state: LaneState,
}

impl WriteBatch {
    /// Rows the batch writes.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.inserts.len() + self.deletes.len() + self.counts.len() + self.outbox.len()
    }
}

/// The durable side of queues in one shard schema (or one memory store).
#[async_trait]
pub trait MsgStore: Send + Sync + 'static {
    /// The store no longer serves its queues here (a shard move fenced or
    /// dropped its tables): open it again where the tenant's map points.
    fn stale(&self) -> bool {
        false
    }

    /// Takes over a lane: `owner_epoch + 1` (the row is created when
    /// missing), the lane state, and the messages on disk counted.
    async fn claim(&self, queue: u64, lane: u16) -> Result<Claim, StoreError>;

    /// Messages of a lane after `after` in `(pclass, seq)` order.
    async fn load(
        &self,
        queue: u64,
        lane: u16,
        after: Option<(u8, u64)>,
        limit: usize,
    ) -> Result<Vec<StoredMsg>, StoreError>;

    /// Which of `ids` the queue already has.
    async fn existing(&self, queue: u64, ids: &[MsgId]) -> Result<Vec<MsgId>, StoreError>;

    /// Commits a batch atomically, or [`StoreError::Fenced`].
    async fn write(&self, b: &WriteBatch) -> Result<(), StoreError>;

    /// Removes every ready message of a lane except `keep` (the unacked
    /// ones), fenced; returns how many went.
    async fn purge(
        &self,
        queue: u64,
        lane: u16,
        epoch: u64,
        keep: &[u64],
    ) -> Result<u64, StoreError>;

    /// Removes a queue's messages and state (queue deleted).
    async fn drop_queue(&self, queue: u64) -> Result<(), StoreError>;

    /// Up to `limit` outbox rows, oldest first.
    async fn outbox(&self, limit: usize) -> Result<Vec<OutboxRow>, StoreError>;

    /// The oldest outbox row's id, if any (a takeover's outbox barrier).
    async fn outbox_head(&self) -> Result<Option<MsgId>, StoreError> {
        Ok(self.outbox(1).await?.first().map(|r| r.out_id))
    }

    /// Deletes forwarded outbox rows.
    async fn outbox_done(&self, ids: &[MsgId]) -> Result<(), StoreError>;

    /// Messages stored for `queue` (ready and delivered, not yet
    /// acknowledged), all lanes: `mq_info`'s count. The
    /// default reads lane 0 through [`MsgStore::load`].
    async fn stored(&self, queue: u64) -> Result<u64, StoreError> {
        Ok(self.load(queue, 0, None, 100_000).await?.len() as u64)
    }

    /// The outbox rows of `ids` that are still there, in the order of
    /// `ids` (the feed-driven forwarder's batch, in commit order).
    async fn outbox_rows(&self, ids: &[MsgId]) -> Result<Vec<OutboxRow>, StoreError> {
        let all = self.outbox(100_000).await?;
        Ok(ids
            .iter()
            .filter_map(|i| all.iter().find(|r| r.out_id == *i).cloned())
            .collect())
    }

    /// Up to `limit` rows the polling forwarder takes when the feed-driven
    /// forwarder runs: rows the owners wrote (dead letters, due delayed
    /// messages) at once, and transactional publishes only when their id
    /// is below `publish_before` (older than the fallback delay).
    async fn outbox_due(
        &self,
        limit: usize,
        publish_before: MsgId,
    ) -> Result<Vec<OutboxRow>, StoreError> {
        Ok(self
            .outbox(limit)
            .await?
            .into_iter()
            .filter(|r| r.kind != OutKind::Publish || r.out_id < publish_before)
            .collect())
    }

    /// Removes forward-ledger rows written before `before_ms` whose outbox
    /// row is gone (nothing can forward them again); how many went.
    async fn purge_ledger(&self, before_ms: i64) -> Result<u64, StoreError> {
        let _ = before_ms;
        Ok(0)
    }

    /// Stores a delayed message.
    async fn delay(&self, row: &DelayedRow) -> Result<(), StoreError>;

    /// Moves delayed messages due by `now_ms` into the outbox (kind
    /// `delayed`), in one transaction; returns how many moved.
    async fn release_due(&self, now_ms: i64, limit: usize) -> Result<usize, StoreError>;

    /// The earliest due time of a delayed message, if any.
    async fn next_due(&self) -> Result<Option<i64>, StoreError>;
}
