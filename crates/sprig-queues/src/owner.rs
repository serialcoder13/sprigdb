//! The queue owner (chapter 50, "The queue owner", "Dispatch, credit and
//! consumers", "Acknowledgements", "Expiry and length limits",
//! "Dead-lettering"; build steps 5–7).
//!
//! One task per queue on the gateway holding the queue's lease. It keeps
//! the head of the queue in memory, decides every delivery there, and
//! writes in batches: everything that arrives while one write is in
//! flight goes into the next (group commit). Every batch carries the lane
//! state and is fenced by the owner epoch in the store, so an owner that
//! lost its lease can never write.
//!
//! **No duplicate without `redelivered`.** A message is handed out only
//! once its sequence number is at or below the persisted `delivered_hwm`
//! (the batch that commits a message also raises the mark over the
//! messages consumers have credit for, so this costs no extra write in
//! the steady state). A takeover marks every message at or below the mark
//! `redelivered`: a message delivered by a crashed or fenced owner always
//! comes back flagged.
//!
//! **Not committed, never delivered.** Published messages wait in the
//! batch until it commits; then they are confirmed and become ready.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use sprig_proto_amqp::backend::{ConnKey, ConsumerId, DeadLetterReason, Settlement};
use sprig_proto_amqp::error::{MqCode, MqError};
use sprig_proto_amqp::message::{Delivery, DeliveryId, Message};
use sprig_proto_amqp::v091::codec::{FieldTable, FieldValue};
use tokio::sync::{mpsc, oneshot};

use crate::msg::{self, fmt_code};
use crate::store::{
    LaneState, MsgId, MsgRef, MsgStore, OutDest, OutKind, OutboxRow, StoreError, StoredMsg,
    WriteBatch,
};
use crate::token::TokenKey;
use crate::topology::{Overflow, QueueCfg, QueueDef};

/// Wall-clock milliseconds (a trait so the simulation can drive time).
pub trait Clock: Send + Sync + 'static {
    /// Milliseconds since the epoch.
    fn now_ms(&self) -> i64;
}

/// The system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
    }
}

/// Where a consumer's deliveries go: a local connection's events, or a
/// peer gateway's stream.
pub trait DeliverySink: Send + Sync + 'static {
    /// Hands a delivery over; `false` if the consumer is gone.
    fn deliver(&self, consumer: ConsumerId, d: Delivery) -> bool;
    /// The consumer ended on the owner's side.
    fn cancelled(&self, consumer: ConsumerId, reason: MqError);
    /// Drained credit.
    fn drained(&self, consumer: ConsumerId, credit: u32);
}

impl DeliverySink for sprig_proto_amqp::backend::EventSender {
    fn deliver(&self, consumer: ConsumerId, delivery: Delivery) -> bool {
        self.send(sprig_proto_amqp::backend::Event::Deliver { consumer, delivery })
            .is_ok()
    }
    fn cancelled(&self, consumer: ConsumerId, reason: MqError) {
        let _ = self.send(sprig_proto_amqp::backend::Event::Cancelled { consumer, reason });
    }
    fn drained(&self, consumer: ConsumerId, credit: u32) {
        let _ = self.send(sprig_proto_amqp::backend::Event::Drained { consumer, credit });
    }
}

/// Who holds a delivery: a connection, local (`origin` 0) or behind a
/// peer link.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Holder {
    /// 0 for this gateway; else the peer link.
    pub origin: u64,
    /// The connection.
    pub conn: ConnKey,
}

/// [`Holder::origin`] of a settlement by delivery token (SQL's
/// `mq_nack`): it settles whoever holds the delivery.
pub const ANY_HOLDER: u64 = u64::MAX;

/// A message to enqueue.
#[derive(Debug, Clone)]
pub struct Incoming {
    /// The message.
    pub msg: Arc<Message>,
    /// Its id.
    pub msg_id: MsgId,
    /// Check `uk_msg` first (forwarded from an outbox: may be a replay).
    pub dedup: bool,
    /// `x-death` (dead letters).
    pub deaths: Option<FieldValue>,
}

/// A consumer.
#[derive(Clone)]
pub struct ConsumerSpec {
    /// Its id.
    pub id: ConsumerId,
    /// Its connection.
    pub holder: Holder,
    /// Where deliveries go.
    pub sink: Arc<dyn DeliverySink>,
    /// Settled on delivery.
    pub no_ack: bool,
    /// Refuses other consumers.
    pub exclusive: bool,
    /// `x-priority`.
    pub priority: i32,
    /// Deliveries the consumer still holds from an earlier owner (a
    /// re-attachment): kept delivered to it during the takeover grace.
    pub held: Vec<DeliveryId>,
    /// The gateway the consumer is on when it came over the relay (`None`:
    /// this gateway). The owner follows its consumers by it.
    pub from: Option<Arc<str>>,
}

impl std::fmt::Debug for ConsumerSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerSpec")
            .field("id", &self.id)
            .field("holder", &self.holder)
            .finish_non_exhaustive()
    }
}

pub(crate) type Reply<T> = oneshot::Sender<Result<T, MqError>>;

/// A command to an owner.
pub enum Cmd {
    /// Enqueue messages; each gets its confirm.
    Enqueue(Vec<(Incoming, Option<Reply<()>>)>),
    /// Add a consumer (no credit yet).
    Attach(ConsumerSpec, Reply<()>),
    /// Credit for a consumer.
    Credit {
        /// The consumer.
        consumer: ConsumerId,
        /// More deliveries.
        credit: u32,
        /// Give up the rest when empty.
        drain: bool,
    },
    /// End a consumer.
    Cancel(ConsumerId),
    /// Settle deliveries held by `holder`.
    Settle(Holder, Vec<(DeliveryId, Settlement)>),
    /// `basic.get`.
    Get {
        /// The connection.
        holder: Holder,
        /// Settle at once.
        no_ack: bool,
        /// Back to the queue unless settled within this (SQL's `mq_get`).
        lease_ms: Option<u64>,
        /// The answer.
        reply: Reply<Option<(Delivery, u32)>>,
    },
    /// Remove the ready messages.
    Purge(Reply<u32>),
    /// Delete the queue (checks first); the owner stops.
    Delete {
        /// Only without consumers.
        if_unused: bool,
        /// Only when empty.
        if_empty: bool,
        /// Ready messages removed.
        reply: Reply<u32>,
    },
    /// Ready messages and consumers.
    Info(oneshot::Sender<(u32, u32)>),
    /// A connection is gone: its consumers end, its deliveries return.
    ConnLost(Holder),
    /// An acknowledgement inside a database transaction starts: no
    /// timeout or redelivery until released.
    Hold {
        /// The delivery.
        id: DeliveryId,
        /// Its message id (from the token).
        msg_id: MsgId,
        /// Answered `Ok` when held, 39258 when no longer delivered.
        reply: Reply<()>,
    },
    /// The transaction ended: `committed` (the row is gone) or not.
    Release {
        /// The delivery.
        id: DeliveryId,
        /// Whether the transaction committed.
        committed: bool,
    },
    /// The transaction holding a delivery ended without telling how:
    /// check the row now (the session's commit hook).
    Verify {
        /// The delivery.
        id: DeliveryId,
    },
    /// The queue's definition changed (policies, arguments).
    Reconfigure(Box<QueueCfg>),
    /// Hand the queue to another gateway (the owner follows its
    /// consumers): write what is in flight, then stop without failing the
    /// commands that wait; the slot passes them on.
    HandOff(String),
    /// Stop (lease lost, gateway stopping).
    Stop,
}

/// What an owner tells the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerEvent {
    /// An auto-delete queue lost its last consumer, or an `x-expires`
    /// queue went unused: the engine deletes it.
    AutoDelete {
        /// The queue.
        queue: u64,
    },
    /// The owner stopped (fenced, store failure, deleted, stopped).
    Stopped {
        /// The queue.
        queue: u64,
        /// The epoch it ran under.
        epoch: u64,
        /// Why.
        reason: String,
    },
    /// Dead letters were written to this store's outbox (also: wake the
    /// forwarder for a takeover's outbox barrier).
    Outbox,
    /// Most consumers have been on gateway `to` for
    /// `queue.owner_move_after_seconds`: the engine may hand the queue
    /// there.
    MoveWanted {
        /// The queue.
        queue: u64,
        /// The gateway.
        to: String,
    },
    /// The queue went from empty to holding ready messages, or back
    /// (reported once when the owner starts, then on each change): the
    /// ready index of fan-in consumers (chapter 42, "Fan-in").
    Ready {
        /// The queue.
        queue: u64,
        /// Ready messages exist.
        ready: bool,
    },
}

/// How an owner ended.
pub enum Exit {
    /// Stopped (fenced, store failure, deleted, lease lost, stopping).
    Stopped,
    /// Handed off to a gateway; the commands that wait are still in `rx`.
    HandedOff {
        /// The new owner.
        to: String,
        /// The commands.
        rx: mpsc::UnboundedReceiver<Cmd>,
    },
}

impl std::fmt::Debug for Exit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Exit::Stopped => f.write_str("Stopped"),
            Exit::HandedOff { to, .. } => write!(f, "HandedOff({to})"),
        }
    }
}

/// Fixed parameters of an owner.
#[derive(Clone)]
pub struct OwnerParams {
    /// The tenant (delivery tokens).
    pub tenant: u64,
    /// The vhost's database (delivery tokens).
    pub db: u32,
    /// The queue.
    pub queue: Arc<QueueDef>,
    /// Its behaviour.
    pub cfg: QueueCfg,
    /// Rows per write batch.
    pub batch_rows: usize,
    /// Bodies kept in memory.
    pub cache_bytes: u64,
    /// Rows read per page-in.
    pub page: usize,
    /// Default consumer timeout (`amqp.consumer_timeout_ms`; 0 = none).
    pub consumer_timeout_ms: u64,
    /// Cap for unlimited prefetch.
    pub max_unacked: u32,
    /// How long an acknowledgement inside a transaction may hold a
    /// delivery before the owner checks the row itself.
    pub hold_check_ms: i64,
    /// Delivery tokens (`x-sprig-delivery`).
    pub tokens: Option<Arc<TokenKey>>,
    /// Time.
    pub clock: Arc<dyn Clock>,
    /// Where events go.
    pub events: mpsc::UnboundedSender<OwnerEvent>,
    /// `queue.takeover_grace_ms`: after a takeover, nothing is dispatched
    /// for this long while consumers re-attach and report what they hold.
    pub grace_ms: i64,
    /// `queue.owner_move_after_seconds` in ms (0: never move).
    pub move_after_ms: i64,
    /// No move before this time (the last move plus
    /// `queue.owner_move_cooldown_seconds`).
    pub no_move_before_ms: i64,
}

impl std::fmt::Debug for OwnerParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnerParams")
            .field("tenant", &self.tenant)
            .field("queue", &self.queue.name)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct Entry {
    msg: Arc<Message>,
    msg_id: MsgId,
    seq: u64,
    pclass: u8,
    redelivered: bool,
    delivery_count: u32,
    expires_at_ms: Option<i64>,
    deaths: Option<FieldValue>,
    size: u64,
    /// Enqueued by an outbox forward: recorded in the forward ledger with
    /// its insert.
    fwd: bool,
}

impl Entry {
    fn key(&self) -> (u8, u64) {
        (self.pclass, self.seq)
    }
    fn r(&self) -> MsgRef {
        MsgRef {
            lane: 0,
            pclass: self.pclass,
            seq: self.seq,
            msg_id: self.msg_id,
        }
    }
}

struct Unacked {
    e: Entry,
    holder: Holder,
    consumer: Option<ConsumerId>,
    deadline_ms: Option<i64>,
    /// Held by a transaction since (ms).
    held_since: Option<i64>,
    /// A requeue arrived while held.
    requeue_after: bool,
    /// Its row was already deleted by a transaction.
    gone: bool,
}

struct Consumer {
    spec: ConsumerSpec,
    credit: u32,
    drain: bool,
}

/// What a slot is doing.
#[derive(Debug, Clone)]
pub enum SlotKind {
    /// Finding the owner.
    Resolving,
    /// Running the owner here; cancelled when it stops.
    Local(sprig_common::CancellationToken),
    /// Relaying to the owner on that gateway.
    Remote(String),
    /// Ended.
    Gone,
}

/// The handle the engine keeps.
#[derive(Clone)]
pub struct OwnerHandle {
    /// The queue.
    pub queue: u64,
    /// Commands.
    pub tx: mpsc::UnboundedSender<Cmd>,
    /// The epoch.
    pub epoch: u64,
    /// What the slot is doing.
    pub kind: tokio::sync::watch::Receiver<SlotKind>,
}

impl std::fmt::Debug for OwnerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnerHandle")
            .field("queue", &self.queue)
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

impl OwnerHandle {
    /// Sends a command; `false` if the owner stopped.
    #[must_use = "a stopped owner drops the command"]
    pub fn send(&self, c: Cmd) -> bool {
        self.tx.send(c).is_ok()
    }

    /// Whether the owner task still runs.
    #[must_use]
    pub fn alive(&self) -> bool {
        !self.tx.is_closed()
    }
}

/// The owner's state.
struct Core {
    p: OwnerParams,
    store: Arc<dyn MsgStore>,
    epoch: u64,
    next_seq: u64,
    hwm: u64,
    hwm_target: u64,
    ready: BTreeMap<(u8, u64), Entry>,
    cache_bytes: u64,
    complete: bool,
    loaded_upto: Option<(u8, u64)>,
    ready_count: u64,
    ready_bytes: u64,
    unacked: BTreeMap<u64, Unacked>,
    consumers: Vec<Consumer>,
    rr: usize,
    had_consumer: bool,
    last_used_ms: i64,
    // The batch being built.
    inserts: Vec<(Entry, Option<Reply<()>>)>,
    deletes: Vec<MsgRef>,
    counts: Vec<(MsgRef, u32)>,
    outbox: Vec<OutboxRow>,
    confirms_now: Vec<Reply<()>>,
    hwm_dirty: bool,
    deleted: bool,
    verify_tx: mpsc::UnboundedSender<(u64, MsgId, bool)>,
    /// Nothing is dispatched before this (takeover grace and outbox
    /// barrier).
    live_at: i64,
    /// The outbox barrier passed (or timed out).
    barrier_done: bool,
    /// `basic.get`s that arrived before `live_at`.
    deferred_gets: Vec<Cmd>,
    /// The gateway most consumers are on, and since when.
    follow: Option<(Arc<str>, i64)>,
    /// A move was asked for (once until the picture changes).
    move_asked: bool,
    /// Set by `HandOff`.
    handoff: Option<String>,
    /// The ready state last reported ([`OwnerEvent::Ready`]).
    reported_ready: Option<bool>,
}

/// Runs the owner of `p.queue` over `store` on commands from `rx`: takes
/// the lane over (epoch + 1), loads the head, and serves until stopped.
/// Commands sent before the takeover finished wait in `rx`; if the
/// takeover fails they are answered with the error.
pub async fn run(p: OwnerParams, store: Arc<dyn MsgStore>, rx: mpsc::UnboundedReceiver<Cmd>) {
    let _ = run_owned(p, store, rx).await;
}

/// [`run`], telling how the owner ended: a hand-off gives the waiting
/// commands back.
pub async fn run_owned(
    p: OwnerParams,
    store: Arc<dyn MsgStore>,
    mut rx: mpsc::UnboundedReceiver<Cmd>,
) -> Exit {
    let claim = match store.claim(p.queue.id, 0).await {
        Ok(c) => c,
        Err(e) => {
            let err = MqError::new(MqCode::OwnerMoved, format!("queue '{}': {e}", p.queue.name));
            fail_all(&mut rx, &err);
            let _ = p.events.send(OwnerEvent::Stopped {
                queue: p.queue.id,
                epoch: 0,
                reason: e.to_string(),
            });
            return Exit::Stopped;
        }
    };
    let (verify_tx, verify_rx) = mpsc::unbounded_channel();
    let now = p.clock.now_ms();
    // A takeover (not the first owner): consumers that held deliveries of
    // the last owner re-attach during the grace, and the outbox barrier
    // lets committed transactional publishes and dead letters in first.
    let takeover = claim.epoch > 1;
    let grace = if takeover && claim.state.delivered_hwm > 0 {
        p.grace_ms.max(0)
    } else {
        0
    };
    let barrier = (takeover && p.grace_ms > 0).then(|| {
        let (tx, rx) = oneshot::channel::<()>();
        let store = Arc::clone(&store);
        let deadline = now + (2 * p.grace_ms).max(1000);
        let clock = Arc::clone(&p.clock);
        let events = p.events.clone();
        tokio::spawn(async move {
            let _ = events.send(OwnerEvent::Outbox);
            loop {
                match store.outbox_head().await {
                    Ok(None) => break,
                    Ok(Some(id)) if crate::store::msg_id_ms(&id) > now => break,
                    _ => {}
                }
                if clock.now_ms() >= deadline {
                    tracing::debug!("outbox barrier timed out");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let _ = tx.send(());
        });
        rx
    });
    let mut core = Core {
        epoch: claim.epoch,
        next_seq: claim.state.next_seq.max(1),
        hwm: claim.state.delivered_hwm,
        hwm_target: claim.state.delivered_hwm,
        ready: BTreeMap::new(),
        cache_bytes: 0,
        complete: false,
        loaded_upto: None,
        ready_count: claim.count,
        ready_bytes: claim.bytes,
        unacked: BTreeMap::new(),
        consumers: Vec::new(),
        rr: 0,
        had_consumer: false,
        last_used_ms: now,
        inserts: Vec::new(),
        deletes: Vec::new(),
        counts: Vec::new(),
        outbox: Vec::new(),
        confirms_now: Vec::new(),
        hwm_dirty: false,
        deleted: false,
        verify_tx,
        live_at: now + grace,
        barrier_done: barrier.is_none(),
        deferred_gets: Vec::new(),
        follow: None,
        move_asked: false,
        handoff: None,
        reported_ready: None,
        store,
        p,
    };
    if let Err(e) = core.page_in().await {
        let err = MqError::new(
            MqCode::OwnerMoved,
            format!("queue '{}': {e}", core.p.queue.name),
        );
        fail_all(&mut rx, &err);
        let _ = core.p.events.send(OwnerEvent::Stopped {
            queue: core.p.queue.id,
            epoch: core.epoch,
            reason: e.to_string(),
        });
        return Exit::Stopped;
    }
    metrics::counter!("sprig_queue_owner_takeovers_total", "reason" => "lease").increment(1);
    core.run(rx, verify_rx, barrier).await
}

/// Answers every waiting command with `err`.
pub fn fail_all(rx: &mut mpsc::UnboundedReceiver<Cmd>, err: &MqError) {
    rx.close();
    while let Ok(c) = rx.try_recv() {
        fail(c, err);
    }
}

/// Answers one command with `err`.
pub fn fail(c: Cmd, err: &MqError) {
    match c {
        Cmd::Enqueue(items) => {
            for (_, r) in items {
                if let Some(r) = r {
                    let _ = r.send(Err(err.clone()));
                }
            }
        }
        Cmd::Attach(_, r) | Cmd::Hold { reply: r, .. } => {
            let _ = r.send(Err(err.clone()));
        }
        Cmd::Get { reply, .. } => {
            let _ = reply.send(Err(err.clone()));
        }
        Cmd::Purge(r) | Cmd::Delete { reply: r, .. } => {
            let _ = r.send(Err(err.clone()));
        }
        Cmd::Info(r) => {
            let _ = r.send((0, 0));
        }
        Cmd::Credit { .. }
        | Cmd::Cancel(_)
        | Cmd::Settle(..)
        | Cmd::ConnLost(_)
        | Cmd::Release { .. }
        | Cmd::Verify { .. }
        | Cmd::Reconfigure(_)
        | Cmd::HandOff(_)
        | Cmd::Stop => {}
    }
}

fn already_settled(what: &str) -> MqError {
    MqError::new(MqCode::AlreadySettled, what.to_owned())
}

impl Core {
    fn now(&self) -> i64 {
        self.p.clock.now_ms()
    }

    /// Past the takeover grace and the outbox barrier: deliveries may
    /// leave.
    fn is_live(&self) -> bool {
        self.barrier_done && self.now() >= self.live_at
    }

    fn id_of(&self, e: &Entry) -> DeliveryId {
        DeliveryId {
            queue: self.p.queue.id,
            lane: 0,
            pclass: e.pclass,
            seq: e.seq,
        }
    }

    fn stored(e: &Entry) -> StoredMsg {
        StoredMsg {
            lane: 0,
            pclass: e.pclass,
            seq: e.seq,
            msg_id: e.msg_id,
            fmt: fmt_code(e.msg.format),
            exchange: e.msg.exchange.clone(),
            routing_key: e.msg.routing_key.clone(),
            props: e.msg.props.clone(),
            body: e.msg.body.clone(),
            expires_at_ms: e.expires_at_ms,
            delivery_count: e.delivery_count,
            deaths: e.deaths.as_ref().map(msg::encode_deaths),
        }
    }

    fn entry_of(&self, m: StoredMsg) -> Option<Entry> {
        let msg = match msg::rebuild(m.fmt, m.exchange, m.routing_key, m.props, m.body) {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(queue = %self.p.queue.name, error = %e, "unreadable stored message skipped");
                return None;
            }
        };
        let deaths = m.deaths.as_deref().and_then(msg::decode_deaths);
        let mut msg = msg;
        if deaths.is_some() {
            msg.info.ttl_ms = None;
        }
        let size = msg.info.size;
        Some(Entry {
            msg: Arc::new(msg),
            msg_id: m.msg_id,
            seq: m.seq,
            pclass: m.pclass,
            redelivered: m.seq <= self.hwm,
            delivery_count: m.delivery_count,
            expires_at_ms: m.expires_at_ms,
            deaths,
            size,
            fwd: false,
        })
    }

    /// Reads the next page of ready messages into the cache.
    async fn page_in(&mut self) -> Result<(), StoreError> {
        if self.complete {
            return Ok(());
        }
        let rows = self
            .store
            .load(self.p.queue.id, 0, self.loaded_upto, self.p.page)
            .await?;
        let n = rows.len();
        for r in rows {
            let k = (r.pclass, r.seq);
            self.loaded_upto = Some(k);
            if self.unacked.contains_key(&r.seq) || self.ready.contains_key(&k) {
                continue;
            }
            if let Some(e) = self.entry_of(r) {
                self.cache_bytes += e.size;
                self.ready.insert(k, e);
            }
        }
        if n < self.p.page {
            self.complete = true;
            self.loaded_upto = None;
        }
        Ok(())
    }

    fn cache_put(&mut self, e: Entry) {
        let k = e.key();
        if !self.complete && self.loaded_upto.is_some_and(|u| k > u) {
            return; // stays on disk; read by a later page-in
        }
        if self.complete && self.cache_bytes + e.size > self.p.cache_bytes && !self.ready.is_empty()
        {
            // The cache is full: the rest waits on disk.
            self.complete = false;
            self.loaded_upto = self.ready.keys().next_back().copied();
            if self.loaded_upto.is_some_and(|u| k > u) {
                return;
            }
        }
        self.cache_bytes += e.size;
        self.ready.insert(k, e);
    }

    fn cache_take(&mut self, k: (u8, u64)) -> Option<Entry> {
        let e = self.ready.remove(&k)?;
        self.cache_bytes = self.cache_bytes.saturating_sub(e.size);
        Some(e)
    }

    fn dec_ready(&mut self, size: u64) {
        self.ready_count = self.ready_count.saturating_sub(1);
        self.ready_bytes = self.ready_bytes.saturating_sub(size);
    }

    fn inc_ready(&mut self, size: u64) {
        self.ready_count += 1;
        self.ready_bytes += size;
    }

    fn batch_rows(&self) -> usize {
        self.inserts.len() + self.deletes.len() + self.counts.len() + self.outbox.len()
    }

    fn needs_write(&self) -> bool {
        self.batch_rows() > 0 || self.hwm_dirty
    }

    fn token(&self, e: &Entry) -> Option<String> {
        self.p.tokens.as_ref().map(|k| {
            k.mint(
                self.p.tenant,
                self.p.db,
                &self.id_of(e),
                &e.msg_id,
                self.epoch,
            )
        })
    }

    fn delivery(&self, e: &Entry) -> Delivery {
        let mut annotations = FieldTable::new();
        if let Some(d) = &e.deaths {
            annotations.insert("x-death", d.clone());
        }
        if self.p.cfg.quorum && e.delivery_count > 0 {
            annotations.insert(
                "x-delivery-count",
                FieldValue::I64(i64::from(e.delivery_count)),
            );
        }
        if let Some(t) = self.token(e) {
            annotations.insert("x-sprig-delivery", FieldValue::str(&t));
        }
        Delivery {
            id: self.id_of(e),
            queue: self.p.queue.name.clone(),
            message: Arc::clone(&e.msg),
            redelivered: e.redelivered,
            delivery_count: e.delivery_count,
            annotations,
        }
    }

    // ------------------------------------------------------------------
    // Enqueue, overflow, dead-lettering.
    // ------------------------------------------------------------------

    fn expires(&self, m: &Message, now: i64) -> Option<i64> {
        let ttl = [self.p.cfg.message_ttl_ms, m.info.ttl_ms]
            .into_iter()
            .flatten()
            .min()?;
        Some(now.saturating_add(i64::try_from(ttl).unwrap_or(i64::MAX)))
    }

    fn full_for(&self, size: u64, extra: u64) -> bool {
        let count = self.ready_count + extra;
        let pending_bytes: u64 = self.inserts.iter().map(|(e, _)| e.size).sum();
        self.p.cfg.max_length.is_some_and(|n| count >= n)
            || self
                .p
                .cfg
                .max_length_bytes
                .is_some_and(|n| self.ready_bytes + pending_bytes + size > n)
    }

    async fn enqueue(&mut self, items: Vec<(Incoming, Option<Reply<()>>)>) {
        // Replays from an outbox: skip what the queue already has.
        let dedup: Vec<MsgId> = items
            .iter()
            .filter(|(i, _)| i.dedup)
            .map(|(i, _)| i.msg_id)
            .collect();
        let mut have: HashSet<MsgId> = HashSet::new();
        if !dedup.is_empty() {
            let mem: HashSet<MsgId> = self
                .ready
                .values()
                .map(|e| e.msg_id)
                .chain(self.unacked.values().map(|u| u.e.msg_id))
                .chain(self.inserts.iter().map(|(e, _)| e.msg_id))
                .collect();
            have.extend(dedup.iter().filter(|i| mem.contains(*i)));
            match self.store.existing(self.p.queue.id, &dedup).await {
                Ok(v) => have.extend(v),
                Err(e) => {
                    for (_, r) in items {
                        if let Some(r) = r {
                            let _ = r.send(Err(MqError::new(MqCode::OwnerMoved, e.to_string())));
                        }
                    }
                    return;
                }
            }
        }
        let now = self.now();
        self.last_used_ms = now;
        for (inc, reply) in items {
            if have.contains(&inc.msg_id) {
                if let Some(r) = reply {
                    let _ = r.send(Ok(()));
                }
                continue;
            }
            have.insert(inc.msg_id);
            let size = inc.msg.info.size;
            let pclass = self.p.cfg.pclass(inc.msg.info.priority);
            let mut msg = inc.msg;
            if inc.deaths.is_some() {
                msg = msg::without_ttl(&msg);
            }
            let mut e = Entry {
                expires_at_ms: self.expires(&msg, now),
                msg,
                msg_id: inc.msg_id,
                seq: 0,
                pclass,
                redelivered: false,
                delivery_count: 0,
                deaths: inc.deaths,
                size,
                fwd: inc.dedup,
            };
            let pending = self.inserts.len() as u64;
            if self.full_for(size, pending) {
                match self.p.cfg.overflow {
                    Overflow::RejectPublish | Overflow::RejectPublishDlx => {
                        if self.p.cfg.overflow == Overflow::RejectPublishDlx {
                            self.dead_letter(e, DeadLetterReason::Maxlen, false);
                        }
                        if let Some(r) = reply {
                            let _ = r.send(Err(MqError::new(
                                MqCode::QueueFull,
                                format!("queue '{}' is full", self.p.queue.name),
                            )));
                        }
                        continue;
                    }
                    Overflow::DropHead => {
                        if self.p.cfg.max_length == Some(0)
                            || self.p.cfg.max_length_bytes.is_some_and(|n| size > n)
                        {
                            // Nothing fits: the new message is the dropped
                            // head.
                            self.dead_letter(e, DeadLetterReason::Maxlen, false);
                            if let Some(r) = reply {
                                self.confirms_now.push(r);
                            }
                            continue;
                        }
                        while self.full_for(size, self.inserts.len() as u64) {
                            if !self.drop_head() {
                                break;
                            }
                        }
                    }
                }
            }
            e.seq = self.next_seq;
            self.next_seq += 1;
            self.inserts.push((e, reply));
        }
    }

    /// Drops the oldest ready message (drop-head): from the cache, else
    /// the oldest one not yet committed.
    fn drop_head(&mut self) -> bool {
        if let Some((&k, _)) = self.ready.iter().next()
            && let Some(e) = self.cache_take(k)
        {
            self.dec_ready(e.size);
            self.dead_letter(e, DeadLetterReason::Maxlen, true);
            return true;
        }
        if self.inserts.is_empty() {
            return false;
        }
        let (e, reply) = self.inserts.remove(0);
        if let Some(r) = reply {
            self.confirms_now.push(r);
        }
        self.dead_letter(e, DeadLetterReason::Maxlen, false);
        true
    }

    /// Dead-letters (or drops) an entry; `stored`: its row exists and is
    /// deleted in the same batch.
    fn dead_letter(&mut self, e: Entry, reason: DeadLetterReason, stored: bool) {
        if stored {
            self.deletes.push(e.r());
        }
        match reason {
            DeadLetterReason::Expired => {
                metrics::counter!("sprig_queue_expired_total").increment(1);
            }
            r => {
                metrics::counter!("sprig_queue_dead_lettered_total", "reason" => r.as_str())
                    .increment(1);
            }
        }
        let Some(dlx) = self.p.cfg.dlx.clone() else {
            return;
        };
        let qname = self.p.queue.name.clone();
        let key = self
            .p
            .cfg
            .dlrk
            .clone()
            .unwrap_or_else(|| e.msg.routing_key.clone());
        // x-death: one entry per (queue, reason), newest first.
        let mut deaths: Vec<FieldValue> = match e.deaths {
            Some(FieldValue::Array(a)) => a,
            _ => Vec::new(),
        };
        let mut count = 1i64;
        deaths.retain(|d| {
            let FieldValue::Table(t) = d else { return true };
            let s = |k: &str| {
                t.get(k)
                    .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
            };
            let same = s("queue").as_deref() == Some(&qname)
                && s("reason").as_deref() == Some(reason.as_str());
            if same {
                count += t.get("count").and_then(FieldValue::as_i64).unwrap_or(0);
            }
            !same
        });
        if reason != DeadLetterReason::Rejected && count > 1 {
            tracing::debug!(
                queue = %qname,
                code = MqCode::DeadLetterCycle.number(),
                "dead-letter cycle: message dropped"
            );
            return;
        }
        let secs = u64::try_from(self.now() / 1000).unwrap_or(0);
        let mut entry = FieldTable::from([
            ("count", FieldValue::I64(count)),
            ("reason", FieldValue::str(reason.as_str())),
            ("queue", FieldValue::str(&qname)),
            ("time", FieldValue::Timestamp(secs)),
            ("exchange", FieldValue::str(&e.msg.exchange)),
            (
                "routing-keys",
                FieldValue::Array(vec![FieldValue::str(&e.msg.routing_key)]),
            ),
        ]);
        if let Some(t) = e.msg.info.ttl_ms {
            entry.insert("original-expiration", FieldValue::str(&t.to_string()));
        }
        deaths.insert(0, FieldValue::Table(entry));
        let out_id = crate::store::new_msg_id();
        self.outbox.push(OutboxRow {
            out_id,
            kind: OutKind::DeadLetter,
            dest: OutDest::Exchange { name: dlx, key },
            msg_id: e.msg_id,
            fmt: fmt_code(e.msg.format),
            exchange: e.msg.exchange.clone(),
            routing_key: e.msg.routing_key.clone(),
            props: e.msg.props.clone(),
            body: e.msg.body.clone(),
            deaths: Some(msg::encode_deaths(&FieldValue::Array(deaths))),
        });
    }

    // ------------------------------------------------------------------
    // Writes.
    // ------------------------------------------------------------------

    /// How far `delivered_hwm` must reach so the messages consumers can
    /// take next are covered.
    fn plan_hwm(&mut self) {
        let credit: u64 = self
            .eligible()
            .iter()
            .map(|&i| u64::from(self.consumers[i].credit))
            .sum();
        if credit == 0 {
            return;
        }
        let mut keys: Vec<(u8, u64)> = self.ready.keys().copied().collect();
        keys.extend(self.inserts.iter().map(|(e, _)| e.key()));
        keys.sort_unstable();
        let take = usize::try_from(credit).unwrap_or(usize::MAX);
        if let Some(max) = keys.iter().take(take).map(|k| k.1).max()
            && max > self.hwm_target
        {
            self.hwm_target = max;
            self.hwm_dirty = true;
        }
    }

    async fn flush(&mut self) -> Result<(), StoreError> {
        self.plan_hwm();
        for r in self.confirms_now.drain(..) {
            let _ = r.send(Ok(()));
        }
        if !self.needs_write() {
            return Ok(());
        }
        let inserts = std::mem::take(&mut self.inserts);
        let mut rows = Vec::with_capacity(inserts.len());
        let mut ready_after = (self.ready_count, self.ready_bytes);
        for (e, _) in &inserts {
            rows.push(Self::stored(e));
            ready_after.0 += 1;
            ready_after.1 += e.size;
        }
        let b = WriteBatch {
            queue: self.p.queue.id,
            lane: 0,
            epoch: self.epoch,
            inserts: rows,
            deletes: std::mem::take(&mut self.deletes),
            counts: std::mem::take(&mut self.counts),
            outbox: std::mem::take(&mut self.outbox),
            forwarded: inserts
                .iter()
                .filter(|(e, _)| e.fwd)
                .map(|(e, _)| e.msg_id)
                .collect(),
            state: LaneState {
                next_seq: self.next_seq,
                delivered_hwm: self.hwm_target.max(self.hwm),
                ready_count: ready_after.0,
                ready_bytes: ready_after.1,
            },
        };
        let started = std::time::Instant::now();
        let res = self.store.write(&b).await;
        metrics::histogram!("sprig_queue_write_batch_rows").record(b.rows() as f64);
        match res {
            Ok(()) => {
                self.hwm = b.state.delivered_hwm;
                self.hwm_dirty = self.hwm_target > self.hwm;
                let n = inserts.len();
                for (e, reply) in inserts {
                    self.inc_ready(e.size);
                    self.cache_put(e);
                    if let Some(r) = reply {
                        let _ = r.send(Ok(()));
                    }
                }
                if n > 0 {
                    metrics::counter!("sprig_queue_published_total", "path" => "direct")
                        .increment(n as u64);
                    metrics::histogram!("sprig_queue_confirm_seconds")
                        .record(started.elapsed().as_secs_f64());
                }
                if !b.outbox.is_empty() {
                    let _ = self.p.events.send(OwnerEvent::Outbox);
                }
                Ok(())
            }
            Err(e) => {
                let err = MqError::new(
                    MqCode::OwnerMoved,
                    format!("queue '{}': {e}", self.p.queue.name),
                );
                for (_, reply) in inserts {
                    if let Some(r) = reply {
                        let _ = r.send(Err(err.clone()));
                    }
                }
                Err(e)
            }
        }
    }

    // ------------------------------------------------------------------
    // Dispatch.
    // ------------------------------------------------------------------

    /// Consumers that may receive now (single active consumer: the first).
    fn eligible(&self) -> Vec<usize> {
        if self.p.cfg.single_active {
            return if self.consumers.is_empty() {
                Vec::new()
            } else {
                vec![0]
            };
        }
        (0..self.consumers.len()).collect()
    }

    fn choose(&mut self) -> Option<usize> {
        let cands = self.eligible();
        let best = cands
            .iter()
            .filter(|&&i| self.consumers[i].credit > 0)
            .map(|&i| self.consumers[i].spec.priority)
            .max()?;
        let n = self.consumers.len();
        for k in 0..n {
            let i = (self.rr + k) % n;
            if cands.contains(&i)
                && self.consumers[i].credit > 0
                && self.consumers[i].spec.priority == best
            {
                self.rr = (i + 1) % n;
                return Some(i);
            }
        }
        None
    }

    /// Pops the next live head, expiring dead heads on the way.
    fn pop_head(&mut self, now: i64) -> Option<Entry> {
        loop {
            let (&k, e) = self.ready.iter().next()?;
            if e.seq > self.hwm {
                self.hwm_dirty = true;
                return None;
            }
            let e = self.cache_take(k)?;
            self.dec_ready(e.size);
            if e.expires_at_ms.is_some_and(|t| t <= now) {
                self.dead_letter(e, DeadLetterReason::Expired, true);
                continue;
            }
            return Some(e);
        }
    }

    fn deadline(&self, now: i64) -> Option<i64> {
        let t = self
            .p
            .cfg
            .consumer_timeout_ms
            .unwrap_or(self.p.consumer_timeout_ms);
        (t > 0).then(|| now.saturating_add(i64::try_from(t).unwrap_or(i64::MAX)))
    }

    fn dispatch(&mut self) {
        if !self.is_live() {
            return;
        }
        let now = self.now();
        loop {
            if self.ready.is_empty() {
                break;
            }
            let Some(ci) = self.choose() else { break };
            let Some(e) = self.pop_head(now) else { break };
            let d = self.delivery(&e);
            let c = &mut self.consumers[ci];
            c.credit -= 1;
            let (id, no_ack, holder, sink) = (
                c.spec.id,
                c.spec.no_ack,
                c.spec.holder,
                Arc::clone(&c.spec.sink),
            );
            metrics::counter!("sprig_queue_delivered_total", "ack_mode" => if no_ack { "auto" } else { "manual" }).increment(1);
            if e.redelivered {
                metrics::counter!("sprig_queue_redelivered_total").increment(1);
            }
            if no_ack {
                self.deletes.push(e.r());
                if !sink.deliver(id, d) {
                    tracing::debug!(queue = %self.p.queue.name, "auto-ack delivery to a gone consumer");
                }
            } else {
                let seq = e.seq;
                self.unacked.insert(
                    seq,
                    Unacked {
                        e,
                        holder,
                        consumer: Some(id),
                        deadline_ms: self.deadline(now),
                        held_since: None,
                        requeue_after: false,
                        gone: false,
                    },
                );
                if !sink.deliver(id, d) {
                    // The consumer's connection is gone: back at once.
                    self.requeue(seq, true);
                }
            }
            self.last_used_ms = now;
        }
        self.finish_drains();
    }

    fn finish_drains(&mut self) {
        if !self.ready.is_empty() && self.ready.keys().next().is_some_and(|k| k.1 <= self.hwm) {
            return;
        }
        if !self.complete {
            return;
        }
        for c in &mut self.consumers {
            if c.drain {
                let credit = std::mem::take(&mut c.credit);
                c.drain = false;
                c.spec.sink.drained(c.spec.id, credit);
            }
        }
    }

    // ------------------------------------------------------------------
    // Settlements.
    // ------------------------------------------------------------------

    fn requeue(&mut self, seq: u64, failed: bool) {
        let Some(mut u) = self.unacked.remove(&seq) else {
            return;
        };
        if u.held_since.is_some() {
            u.requeue_after = true;
            self.unacked.insert(seq, u);
            return;
        }
        if u.gone {
            return;
        }
        let mut e = u.e;
        if failed {
            e.delivery_count += 1;
        }
        if let Some(l) = self.p.cfg.delivery_limit
            && l >= 0
            && i64::from(e.delivery_count) > l
        {
            self.dead_letter(e, DeadLetterReason::DeliveryLimit, true);
            return;
        }
        if failed {
            self.counts.push((e.r(), e.delivery_count));
        }
        e.redelivered = true;
        self.inc_ready(e.size);
        self.cache_bytes += e.size;
        self.ready.insert(e.key(), e);
    }

    fn settle(&mut self, holder: Holder, items: Vec<(DeliveryId, Settlement)>) {
        for (id, how) in items {
            if id.queue != self.p.queue.id {
                continue;
            }
            // A stale settlement (the delivery went back and out again)
            // is ignored.
            if !self
                .unacked
                .get(&id.seq)
                .is_some_and(|u| u.holder == holder || holder.origin == ANY_HOLDER)
            {
                continue;
            }
            match how {
                Settlement::Ack => {
                    let held = self
                        .unacked
                        .get(&id.seq)
                        .is_some_and(|u| u.held_since.is_some());
                    if held {
                        continue; // the transaction deletes it
                    }
                    if let Some(u) = self.unacked.remove(&id.seq)
                        && !u.gone
                    {
                        self.deletes.push(u.e.r());
                        metrics::counter!("sprig_queue_acked_total", "in_txn" => "no").increment(1);
                    }
                }
                Settlement::Requeue { failed } => self.requeue(id.seq, failed),
                Settlement::DeadLetter(r) => {
                    let held = self
                        .unacked
                        .get(&id.seq)
                        .is_some_and(|u| u.held_since.is_some());
                    if held {
                        continue;
                    }
                    if let Some(u) = self.unacked.remove(&id.seq)
                        && !u.gone
                    {
                        self.dead_letter(u.e, r, true);
                    }
                }
            }
        }
    }

    fn remove_consumer(&mut self, id: ConsumerId) {
        let before = self.consumers.len();
        self.consumers.retain(|c| c.spec.id != id);
        if self.rr >= self.consumers.len() {
            self.rr = 0;
        }
        if before != self.consumers.len()
            && self.consumers.is_empty()
            && self.had_consumer
            && self.p.queue.auto_delete
        {
            let _ = self.p.events.send(OwnerEvent::AutoDelete {
                queue: self.p.queue.id,
            });
        }
    }

    fn conn_lost(&mut self, h: Holder) {
        let ids: Vec<ConsumerId> = self
            .consumers
            .iter()
            .filter(|c| c.spec.holder == h)
            .map(|c| c.spec.id)
            .collect();
        // A lease tag's key (R9, r9tests) means the same over every link:
        // its messages go back whichever gateway they were taken through.
        let tag = h.conn.0 & crate::sql::LEASE_TAG_BIT != 0;
        let seqs: Vec<u64> = self
            .unacked
            .iter()
            .filter(|(_, u)| u.holder == h || (tag && u.holder.conn == h.conn))
            .map(|(s, _)| *s)
            .collect();
        if tag {
            metrics::counter!("sprig_queue_lease_tag_returned_total")
                .increment(seqs.len() as u64);
        }
        for s in seqs {
            self.requeue(s, true);
        }
        for id in ids {
            self.remove_consumer(id);
        }
    }

    // ------------------------------------------------------------------
    // Timers.
    // ------------------------------------------------------------------

    fn next_timer(&self) -> Duration {
        let now = self.now();
        let mut next = now + 1000;
        if self.live_at > now {
            next = next.min(self.live_at);
        }
        if let Some(e) = self.ready.values().next()
            && let Some(t) = e.expires_at_ms
        {
            next = next.min(t);
        }
        for u in self.unacked.values() {
            if let Some(d) = u.deadline_ms {
                next = next.min(d);
            }
            if let Some(h) = u.held_since {
                next = next.min(h + self.p.hold_check_ms);
            }
        }
        Duration::from_millis(u64::try_from((next - now).max(1)).unwrap_or(1))
    }

    fn timers(&mut self) {
        let now = self.now();
        // TTL at the head, also without consumers.
        while let Some(e) = self.ready.values().next() {
            if e.expires_at_ms.is_none_or(|t| t > now) {
                break;
            }
            let k = e.key();
            if let Some(e) = self.cache_take(k) {
                self.dec_ready(e.size);
                self.dead_letter(e, DeadLetterReason::Expired, true);
            }
        }
        // Consumer timeouts.
        let late: Vec<(u64, Option<ConsumerId>)> = self
            .unacked
            .iter()
            .filter(|(_, u)| u.held_since.is_none() && u.deadline_ms.is_some_and(|d| d <= now))
            .map(|(s, u)| (*s, u.consumer))
            .collect();
        let mut cancelled = HashSet::new();
        for (seq, c) in late {
            if let Some(c) = c
                && cancelled.insert(c)
                && let Some(con) = self.consumers.iter().find(|x| x.spec.id == c)
            {
                metrics::counter!("sprig_queue_consumer_timeouts_total").increment(1);
                con.spec.sink.cancelled(
                    c,
                    MqError::new(
                        MqCode::ConsumerTimeout,
                        format!(
                            "delivery acknowledgement on queue '{}' timed out",
                            self.p.queue.name
                        ),
                    ),
                );
            }
            self.requeue(seq, false);
        }
        for c in cancelled {
            self.remove_consumer(c);
        }
        // Holds of transactions that never reported back: check the row.
        let stale: Vec<u64> = self
            .unacked
            .iter()
            .filter(|(_, u)| {
                u.held_since
                    .is_some_and(|h| h + self.p.hold_check_ms <= now)
            })
            .map(|(s, _)| *s)
            .collect();
        for seq in stale {
            self.check_hold(seq);
        }
        self.check_follow(now);
        // x-expires.
        if let Some(x) = self.p.cfg.expires_ms
            && self.consumers.is_empty()
            && now - self.last_used_ms >= i64::try_from(x).unwrap_or(i64::MAX)
        {
            self.last_used_ms = now;
            let _ = self.p.events.send(OwnerEvent::AutoDelete {
                queue: self.p.queue.id,
            });
        }
    }

    /// Reads a held delivery's row under a lock (waits for its
    /// transaction): gone means the transaction committed the ack.
    fn check_hold(&mut self, seq: u64) {
        let far = self.now() + 3_600_000;
        let Some(u) = self.unacked.get_mut(&seq) else {
            return;
        };
        if u.held_since.is_none() {
            return;
        }
        let id = u.e.msg_id;
        u.held_since = Some(far); // one check at a time
        let store = Arc::clone(&self.store);
        let q = self.p.queue.id;
        let tx = self.verify_tx.clone();
        tokio::spawn(async move {
            // A locking read waits for an open transaction to end.
            if let Ok(v) = store.existing(q, &[id]).await {
                let _ = tx.send((seq, id, v.is_empty()));
            }
        });
    }

    /// The owner follows its consumers (chapter 50, "The queue owner"):
    /// when more than half of them have been on one other gateway for
    /// `move_after_ms`, and the last move is older than the cooldown, ask
    /// the engine to hand the queue there. Damped: the picture must hold
    /// the whole time, and one ask until it changes.
    fn check_follow(&mut self, now: i64) {
        if self.p.move_after_ms <= 0 || self.consumers.is_empty() || now < self.p.no_move_before_ms
        {
            self.follow = None;
            return;
        }
        let mut by: HashMap<&str, usize> = HashMap::new();
        for c in &self.consumers {
            if let Some(g) = c.spec.from.as_deref() {
                *by.entry(g).or_default() += 1;
            }
        }
        let total = self.consumers.len();
        let top = by
            .into_iter()
            .filter(|(_, n)| n * 2 > total)
            .map(|(g, _)| g)
            .next()
            .map(Arc::<str>::from);
        match (top, &self.follow) {
            (None, _) => {
                self.follow = None;
                self.move_asked = false;
            }
            (Some(g), Some((f, since))) if *f == g => {
                if !self.move_asked && now - since >= self.p.move_after_ms {
                    self.move_asked = true;
                    let _ = self.p.events.send(OwnerEvent::MoveWanted {
                        queue: self.p.queue.id,
                        to: g.to_string(),
                    });
                }
            }
            (Some(g), _) => {
                self.follow = Some((g, now));
                self.move_asked = false;
            }
        }
    }

    fn release(&mut self, seq: u64, gone: bool) {
        let Some(u) = self.unacked.get_mut(&seq) else {
            return;
        };
        u.held_since = None;
        if gone {
            self.unacked.remove(&seq);
            metrics::counter!("sprig_queue_acked_total", "in_txn" => "yes").increment(1);
            return;
        }
        if u.requeue_after {
            u.requeue_after = false;
            self.requeue(seq, false);
        }
    }

    // ------------------------------------------------------------------
    // Commands.
    // ------------------------------------------------------------------

    async fn apply(&mut self, c: Cmd) -> Result<(), StoreError> {
        match c {
            Cmd::Enqueue(items) => self.enqueue(items).await,
            Cmd::Attach(spec, reply) => {
                let ex_held = self.consumers.iter().any(|c| c.spec.exclusive);
                if ex_held || (spec.exclusive && !self.consumers.is_empty()) {
                    let _ = reply.send(Err(MqError::access_refused(format!(
                        "queue '{}' in exclusive use",
                        self.p.queue.name
                    ))));
                } else {
                    let now = self.now();
                    if now < self.live_at {
                        // Takeover grace: what the consumer still holds
                        // from the last owner stays delivered to it.
                        for id in &spec.held {
                            if id.queue != self.p.queue.id || id.seq > self.hwm {
                                continue;
                            }
                            if let Some(e) = self.cache_take((id.pclass, id.seq)) {
                                self.dec_ready(e.size);
                                let seq = e.seq;
                                self.unacked.insert(
                                    seq,
                                    Unacked {
                                        e,
                                        holder: spec.holder,
                                        consumer: Some(spec.id),
                                        deadline_ms: self.deadline(now),
                                        held_since: None,
                                        requeue_after: false,
                                        gone: false,
                                    },
                                );
                            }
                        }
                    }
                    self.consumers.push(Consumer {
                        spec,
                        credit: 0,
                        drain: false,
                    });
                    self.had_consumer = true;
                    self.last_used_ms = self.now();
                    let _ = reply.send(Ok(()));
                }
            }
            Cmd::Credit {
                consumer,
                credit,
                drain,
            } => {
                let max = self.p.max_unacked;
                if let Some(c) = self.consumers.iter_mut().find(|c| c.spec.id == consumer) {
                    c.credit = c.credit.saturating_add(credit).min(max.max(credit));
                    c.drain = drain;
                }
            }
            Cmd::Cancel(id) => self.remove_consumer(id),
            Cmd::Settle(h, items) => self.settle(h, items),
            Cmd::Get {
                holder,
                no_ack,
                lease_ms,
                reply,
            } if !self.is_live() => {
                // Answered once the takeover grace is over.
                self.deferred_gets.push(Cmd::Get {
                    holder,
                    no_ack,
                    lease_ms,
                    reply,
                });
            }
            Cmd::Get {
                holder,
                no_ack,
                lease_ms,
                reply,
            } => {
                self.last_used_ms = self.now();
                // A publish earlier on the channel is in this batch: commit
                // it first, as RabbitMQ's get sees it.
                if !self.inserts.is_empty() {
                    self.flush().await?;
                }
                if self.ready.is_empty() && !self.complete {
                    self.flush().await?;
                    self.page_in().await?;
                }
                // The head must be covered by the mark before it leaves.
                if let Some(e) = self.ready.values().next()
                    && e.seq > self.hwm
                {
                    self.hwm_target = self.hwm_target.max(e.seq);
                    self.hwm_dirty = true;
                    self.flush().await?;
                }
                let now = self.now();
                match self.pop_head(now) {
                    None => {
                        let _ = reply.send(Ok(None));
                    }
                    Some(e) => {
                        let d = self.delivery(&e);
                        let left = u32::try_from(self.ready_count).unwrap_or(u32::MAX);
                        if no_ack {
                            self.deletes.push(e.r());
                        } else {
                            self.unacked.insert(
                                e.seq,
                                Unacked {
                                    e,
                                    holder,
                                    consumer: None,
                                    deadline_ms: lease_ms.map(|l| {
                                        now.saturating_add(i64::try_from(l).unwrap_or(i64::MAX))
                                    }),
                                    held_since: None,
                                    requeue_after: false,
                                    gone: false,
                                },
                            );
                        }
                        let _ = reply.send(Ok(Some((d, left))));
                    }
                }
            }
            Cmd::Purge(reply) => {
                self.flush().await?;
                let keep: Vec<u64> = self.unacked.keys().copied().collect();
                let n = self
                    .store
                    .purge(self.p.queue.id, 0, self.epoch, &keep)
                    .await?;
                self.ready.clear();
                self.cache_bytes = 0;
                self.complete = true;
                self.loaded_upto = None;
                self.ready_count = 0;
                self.ready_bytes = 0;
                let _ = reply.send(Ok(u32::try_from(n).unwrap_or(u32::MAX)));
            }
            Cmd::Delete {
                if_unused,
                if_empty,
                reply,
            } => {
                if if_unused && !self.consumers.is_empty() {
                    let _ = reply.send(Err(MqError::precondition(format!(
                        "queue '{}' in use",
                        self.p.queue.name
                    ))));
                } else if if_empty && self.ready_count + self.inserts.len() as u64 > 0 {
                    let _ = reply.send(Err(MqError::precondition(format!(
                        "queue '{}' is not empty",
                        self.p.queue.name
                    ))));
                } else {
                    let n = u32::try_from(self.ready_count).unwrap_or(u32::MAX);
                    for c in self.consumers.drain(..) {
                        c.spec.sink.cancelled(
                            c.spec.id,
                            MqError::not_found(format!(
                                "queue '{}' was deleted",
                                self.p.queue.name
                            )),
                        );
                    }
                    self.inserts.clear();
                    self.deletes.clear();
                    self.counts.clear();
                    self.hwm_dirty = false;
                    // Dead letters already decided still go out.
                    if !self.outbox.is_empty() {
                        let _ = self.flush().await;
                    }
                    self.store.drop_queue(self.p.queue.id).await?;
                    self.deleted = true;
                    let _ = reply.send(Ok(n));
                }
            }
            Cmd::Info(reply) => {
                let _ = reply.send((
                    u32::try_from(self.ready_count + self.inserts.len() as u64).unwrap_or(u32::MAX),
                    u32::try_from(self.consumers.len()).unwrap_or(u32::MAX),
                ));
            }
            Cmd::ConnLost(h) => self.conn_lost(h),
            Cmd::Hold { id, msg_id, reply } => {
                let now = self.now();
                match self.unacked.get_mut(&id.seq) {
                    Some(u) if u.e.msg_id == msg_id && !u.gone && u.held_since.is_none() => {
                        u.held_since = Some(now);
                        let _ = reply.send(Ok(()));
                    }
                    _ => {
                        let _ = reply.send(Err(already_settled(
                            "the message is no longer delivered (already settled, or returned to the queue)",
                        )));
                    }
                }
            }
            Cmd::Release { id, committed } => self.release(id.seq, committed),
            Cmd::Verify { id } => self.check_hold(id.seq),
            Cmd::Reconfigure(cfg) => self.p.cfg = *cfg,
            Cmd::HandOff(to) => self.handoff = Some(to),
            Cmd::Stop => self.deleted = true,
        }
        Ok(())
    }

    async fn run(
        mut self,
        mut rx: mpsc::UnboundedReceiver<Cmd>,
        mut verify: mpsc::UnboundedReceiver<(u64, MsgId, bool)>,
        mut barrier: Option<oneshot::Receiver<()>>,
    ) -> Exit {
        let reason = loop {
            let wait = self.next_timer();
            let barrier_wait = async {
                match barrier.as_mut() {
                    Some(b) => {
                        let _ = b.await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                c = rx.recv() => match c {
                    None => break "engine gone".to_owned(),
                    Some(c) => {
                        if let Err(e) = self.apply(c).await {
                            break e.to_string();
                        }
                    }
                },
                v = verify.recv() => {
                    if let Some((seq, id, gone)) = v
                        && self.unacked.get(&seq).is_some_and(|u| u.e.msg_id == id)
                    {
                        self.release(seq, gone);
                    }
                }
                () = barrier_wait => {
                    barrier = None;
                    self.barrier_done = true;
                }
                () = tokio::time::sleep(wait) => {}
            }
            let mut failed = None;
            while self.batch_rows() < self.p.batch_rows && !self.deleted && self.handoff.is_none() {
                match rx.try_recv() {
                    Ok(c) => {
                        if let Err(e) = self.apply(c).await {
                            failed = Some(e.to_string());
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            if let Some(f) = failed {
                break f;
            }
            if self.deleted {
                break "stopped".to_owned();
            }
            if self.handoff.is_some() {
                // Drain what is in flight, so nothing is redelivered.
                match self.flush().await {
                    Ok(()) => break "handed off".to_owned(),
                    Err(e) => {
                        self.handoff = None;
                        break e.to_string();
                    }
                }
            }
            self.timers();
            if self.is_live() && !self.deferred_gets.is_empty() {
                let mut failed = None;
                for c in std::mem::take(&mut self.deferred_gets) {
                    if let Err(e) = self.apply(c).await {
                        failed = Some(e.to_string());
                        break;
                    }
                }
                if let Some(f) = failed {
                    break f;
                }
            }
            if let Err(e) = self.settle_loop().await {
                break e.to_string();
            }
            self.report_ready();
        };
        let moved = MqError::new(MqCode::OwnerMoved, reason.clone());
        for (_, r) in self.inserts.drain(..) {
            if let Some(r) = r {
                let _ = r.send(Err(moved.clone()));
            }
        }
        for c in std::mem::take(&mut self.deferred_gets) {
            fail(c, &moved);
        }
        if !self.deleted {
            // The consumers re-attach to the next owner; tell them their
            // queue moved.
            for c in self.consumers.drain(..) {
                c.spec.sink.cancelled(
                    c.spec.id,
                    MqError::new(
                        MqCode::OwnerMoved,
                        format!("queue '{}' owner moved: {reason}", self.p.queue.name),
                    ),
                );
            }
        }
        tracing::debug!(queue = %self.p.queue.name, epoch = self.epoch, %reason, "queue owner stopped");
        let _ = self.p.events.send(OwnerEvent::Stopped {
            queue: self.p.queue.id,
            epoch: self.epoch,
            reason,
        });
        match self.handoff.take() {
            Some(to) if !self.deleted => {
                metrics::counter!("sprig_queue_owner_takeovers_total", "reason" => "follow")
                    .increment(1);
                Exit::HandedOff { to, rx }
            }
            _ => Exit::Stopped,
        }
    }

    /// Tells the engine when the queue went from empty to holding ready
    /// messages or back (and once at the start).
    fn report_ready(&mut self) {
        let ready = self.ready_count > 0;
        if self.reported_ready != Some(ready) {
            self.reported_ready = Some(ready);
            let _ = self.p.events.send(OwnerEvent::Ready {
                queue: self.p.queue.id,
                ready,
            });
        }
    }

    /// Writes, dispatches and pages in until nothing is left to do.
    async fn settle_loop(&mut self) -> Result<(), StoreError> {
        for _ in 0..8 {
            self.flush().await?;
            self.dispatch();
            if !self.complete && self.ready.len() < self.p.page / 2 && !self.needs_write() {
                self.page_in().await?;
                self.dispatch();
            }
            if !self.needs_write() {
                break;
            }
        }
        Ok(())
    }
}

/// Owners on this gateway, by queue.
pub type OwnerMap = HashMap<u64, OwnerHandle>;
