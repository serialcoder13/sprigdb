//! The queue engine of one gateway: [`QueueEngine`] implements
//! `sprig-proto-amqp`'s [`QueueBackend`] (chapter 50, build steps 2–10).
//!
//! What it stands on is behind three traits the gateway implements over
//! MariaDB and etcd (and tests over memory):
//!
//! - [`MqCatalog`]: vhosts, the durable model (catalog snapshots), message
//!   stores, privileges, settings and the tenant's token key;
//! - [`Leases`]: one owner per queue (the etcd lease of chapter 50's
//!   "The queue owner");
//! - [`TempRegistry`]: temporary queues of other gateways.
//!
//! **Owner slots.** Every queue the gateway uses has a slot: a command
//! channel created at once (so `basic.publish`, which must not wait,
//! keeps per-channel order) and a task that takes the lease, takes the
//! queue over and then serves the channel. If another gateway holds the
//! lease, the commands are answered with 39260 (`OWNER_MOVED`); the relay
//! to that gateway is the peer service's (see the crate docs).
//!
//! **The forwarder** of each vhost store moves its outbox: dead letters,
//! due delayed messages and transactional publishes, enqueued with the
//! outbox row's id as the message id, so a replay after a crash is a
//! no-op (`uk_msg`). One gateway forwards a store at a time (a lease).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use async_trait::async_trait;
use sprig_common::CancellationToken;
use sprig_proto_amqp::auth::Principal;
use sprig_proto_amqp::backend::{ConnKey, ConsumerId, EventSender, QueueBackend, Vhost};
use sprig_proto_amqp::error::{MqCode, MqError};
use sprig_proto_amqp::mem::Perm;
use sprig_proto_amqp::message::Delivery;
use tokio::sync::{Notify, mpsc, oneshot};

use crate::memstore::MemStore;
use crate::msg;
use crate::owner::{
    self, Clock, Cmd, ConsumerSpec, DeliverySink, Holder, Incoming, OwnerEvent, OwnerHandle,
    OwnerParams,
};
use crate::settings::QueueSettings;
use crate::store::{MsgStore, OutDest, OutKind, OutboxRow};
use crate::token::TokenKey;
use crate::topology::{
    BindingDef, ExchangeDef, QueueCfg, QueueDef, TempSet, Topology, View,
};

/// A virtual host as the engine knows it: a logical database of a
/// tenant.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VhostRef {
    /// The tenant.
    pub tenant: u64,
    /// The database.
    pub db_id: u32,
    /// Its name.
    pub db: String,
    /// The vhost name the client used.
    pub name: String,
}

/// A change to the durable model, applied as one catalog change.
#[derive(Debug, Clone)]
pub enum TopoChange {
    /// `exchange.declare` (id 0: the catalog assigns one).
    AddExchange(ExchangeDef),
    /// `exchange.delete`, with its bindings.
    DelExchange(String),
    /// `queue.declare` (id 0: the catalog assigns one).
    AddQueue(QueueDef),
    /// `queue.delete`, with its bindings.
    DelQueue(String),
    /// `queue.bind`, `exchange.bind`.
    Bind(BindingDef),
    /// `queue.unbind`, `exchange.unbind`.
    Unbind(BindingDef),
}

/// The catalog side of the engine.
#[async_trait]
pub trait MqCatalog: Send + Sync + 'static {
    /// Maps a vhost to a database of the principal's tenant, checking it
    /// may be used (39250, 39253).
    async fn open(&self, p: &Principal, vhost: &str) -> Result<VhostRef, MqError>;

    /// The durable model as last read (never waits).
    fn topology(&self, v: &VhostRef) -> Arc<Topology>;

    /// The durable model, reloaded if the catalog moved.
    async fn refresh(&self, v: &VhostRef) -> Result<Arc<Topology>, MqError>;

    /// Applies a change in one versioned catalog change; returns the model
    /// after it.
    async fn change(&self, v: &VhostRef, c: TopoChange) -> Result<Arc<Topology>, MqError>;

    /// The message store of the vhost (its database's default shard).
    async fn store(&self, v: &VhostRef) -> Result<Arc<dyn MsgStore>, MqError>;

    /// The tenant's delivery-token key.
    fn token_key(&self, tenant: u64) -> Option<Arc<TokenKey>>;

    /// Whether `p` may do `perm` on the object `name` (`kind` is
    /// `exchange` or `queue`).
    fn allowed(&self, p: &Principal, v: &VhostRef, perm: Perm, kind: &str, name: &str) -> bool;

    /// Settings for the vhost.
    fn settings(&self, v: &VhostRef) -> QueueSettings;
}

/// What [`Leases::acquire`] found.
#[derive(Debug, Clone)]
pub enum Acquired {
    /// This gateway owns it until the token is cancelled.
    Mine(CancellationToken),
    /// Another gateway holds it.
    Held(String),
}

/// Owner leases (`/sprig/dc/<dc>/queue-owners/<tenant>/<object>`).
#[async_trait]
pub trait Leases: Send + Sync + 'static {
    /// This gateway's id.
    fn me(&self) -> &str;
    /// Takes the lease of `object` if free.
    async fn acquire(&self, tenant: u64, object: u64) -> Result<Acquired, MqError>;
    /// Gives it back.
    async fn release(&self, tenant: u64, object: u64);
}

/// Temporary queues of other gateways (and publishing this gateway's).
pub trait TempRegistry: Send + Sync + 'static {
    /// Publishes this gateway's temporary queues of a vhost.
    fn publish(&self, v: &VhostRef, set: &TempSet);
    /// Other gateways' temporary queues of a vhost.
    fn others(&self, v: &VhostRef) -> Option<Arc<TempSet>>;
}

/// No other gateways.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoTemps;

impl TempRegistry for NoTemps {
    fn publish(&self, _v: &VhostRef, _set: &TempSet) {}
    fn others(&self, _v: &VhostRef) -> Option<Arc<TempSet>> {
        None
    }
}

/// What the engine is built from.
#[derive(Clone)]
pub struct EngineDeps {
    /// Catalog, stores, privileges.
    pub catalog: Arc<dyn MqCatalog>,
    /// Owner leases.
    pub leases: Arc<dyn Leases>,
    /// Other gateways' temporary queues.
    pub temps: Arc<dyn TempRegistry>,
    /// Time.
    pub clock: Arc<dyn Clock>,
    /// This gateway's slot (temporary queue ids).
    pub slot: u32,
}

impl std::fmt::Debug for EngineDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineDeps")
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A consumer as the engine tracks it (re-attachment after an owner
/// move).
pub(crate) struct ConsumerRec {
    pub(crate) conn: ConnKey,
    pub(crate) queue: String,
    pub(crate) queue_id: AtomicU64,
    pub(crate) no_ack: bool,
    pub(crate) exclusive: bool,
    pub(crate) priority: i32,
    pub(crate) credit_out: AtomicU32,
}

/// One vhost on this gateway: shared by its connections.
pub(crate) struct VShared {
    pub(crate) r: VhostRef,
    pub(crate) engine: Weak<Inner>,
    /// This gateway's temporary queues.
    pub(crate) temp: Mutex<TempSet>,
    /// Their messages.
    pub(crate) mem: Arc<MemStore>,
    pub(crate) conns: Mutex<HashMap<ConnKey, EventSender>>,
    pub(crate) consumers: Mutex<HashMap<ConsumerId, Arc<ConsumerRec>>>,
    pub(crate) owners: Mutex<HashMap<u64, OwnerHandle>>,
    pub(crate) events: mpsc::UnboundedSender<OwnerEvent>,
    pub(crate) wake: Arc<Notify>,
    pub(crate) blocked: std::sync::atomic::AtomicBool,
    pub(crate) quota: Mutex<(i64, u64)>,
    pub(crate) stop: CancellationToken,
}

impl VShared {
    /// The routing view: durable and temporary queues.
    pub(crate) fn with_view<T>(&self, f: impl FnOnce(View<'_>) -> T) -> Option<T> {
        let e = self.engine.upgrade()?;
        let topo = e.deps.catalog.topology(&self.r);
        let mine = lock(&self.temp);
        let merged;
        let temp = match e.deps.temps.others(&self.r) {
            Some(o) if !o.queues.is_empty() => {
                let mut m = (*o).clone();
                m.queues
                    .extend(mine.queues.iter().map(|(k, v)| (k.clone(), Arc::clone(v))));
                m.bindings.extend(mine.bindings.iter().cloned());
                merged = m;
                &merged
            }
            _ => &*mine,
        };
        Some(f(View { topo: &topo, temp }))
    }

    pub(crate) fn queue(&self, name: &str) -> Option<Arc<QueueDef>> {
        self.with_view(|v| v.queue(name).cloned()).flatten()
    }

    pub(crate) fn queue_by_id(&self, id: u64) -> Option<Arc<QueueDef>> {
        if let Some(q) = lock(&self.temp).queues.values().find(|q| q.id == id) {
            return Some(Arc::clone(q));
        }
        let e = self.engine.upgrade()?;
        e.deps
            .catalog
            .topology(&self.r)
            .queues
            .values()
            .find(|q| q.id == id)
            .cloned()
    }

    pub(crate) fn settings(&self) -> QueueSettings {
        self.engine
            .upgrade()
            .map(|e| e.deps.catalog.settings(&self.r))
            .unwrap_or_default()
    }

    /// The owner slot of `q`, created (and its owner started) if needed.
    pub(crate) fn slot(self: &Arc<Self>, q: &Arc<QueueDef>) -> OwnerHandle {
        let mut owners = lock(&self.owners);
        if let Some(h) = owners.get(&q.id)
            && h.alive()
        {
            return h.clone();
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let h = OwnerHandle {
            queue: q.id,
            tx,
            epoch: 0,
        };
        owners.insert(q.id, h.clone());
        drop(owners);
        tokio::spawn(Arc::clone(self).start_owner(Arc::clone(q), rx));
        h
    }

    /// The running slot of a queue id, if any.
    pub(crate) fn slot_of(&self, id: u64) -> Option<OwnerHandle> {
        lock(&self.owners).get(&id).filter(|h| h.alive()).cloned()
    }

    async fn start_owner(self: Arc<Self>, q: Arc<QueueDef>, mut rx: mpsc::UnboundedReceiver<Cmd>) {
        let Some(e) = self.engine.upgrade() else {
            return;
        };
        let s = e.deps.catalog.settings(&self.r);
        let topo = e.deps.catalog.topology(&self.r);
        let cfg = QueueCfg::of(&q, &topo.policies, s.delivery_limit_default);
        let params = OwnerParams {
            tenant: self.r.tenant,
            queue: Arc::clone(&q),
            cfg,
            batch_rows: s.batch_rows,
            cache_bytes: s.cache_bytes,
            page: 1000,
            consumer_timeout_ms: s.consumer_timeout_ms,
            max_unacked: s.max_unacked,
            hold_check_ms: 5_000,
            tokens: e.deps.catalog.token_key(self.r.tenant),
            clock: Arc::clone(&e.deps.clock),
            events: self.events.clone(),
        };
        if q.temp_on.is_some() {
            let store: Arc<dyn MsgStore> = Arc::clone(&self.mem) as Arc<dyn MsgStore>;
            owner::run(params, store, rx).await;
            return;
        }
        let fail = |rx: &mut mpsc::UnboundedReceiver<Cmd>, err: MqError| {
            owner::fail_all(rx, &err);
            lock(&self.owners).remove(&q.id);
        };
        let lost = match e.deps.leases.acquire(self.r.tenant, q.id).await {
            Ok(Acquired::Mine(t)) => t,
            Ok(Acquired::Held(g)) => {
                fail(
                    &mut rx,
                    MqError::new(
                        MqCode::OwnerMoved,
                        format!(
                            "queue '{}' is served by gateway {g}; connect there (the queue relay between gateways is not available)",
                            q.name
                        ),
                    ),
                );
                return;
            }
            Err(err) => {
                fail(&mut rx, err);
                return;
            }
        };
        let store = match e.deps.catalog.store(&self.r).await {
            Ok(s) => s,
            Err(err) => {
                e.deps.leases.release(self.r.tenant, q.id).await;
                fail(&mut rx, err);
                return;
            }
        };
        // A lost lease stops the owner (its next write would be fenced
        // anyway).
        if let Some(h) = self.slot_of(q.id) {
            let stop = self.stop.clone();
            tokio::spawn(async move {
                tokio::select! {
                    () = lost.cancelled() => { let _ = h.send(Cmd::Stop); }
                    () = stop.cancelled() => { let _ = h.send(Cmd::Stop); }
                }
            });
        }
        owner::run(params, store, rx).await;
        e.deps.leases.release(self.r.tenant, q.id).await;
    }

    /// Re-attaches a consumer whose owner moved.
    pub(crate) fn reattach(self: &Arc<Self>, id: ConsumerId, rec: Arc<ConsumerRec>) {
        let v = Arc::clone(self);
        tokio::spawn(async move {
            for attempt in 0..50u64 {
                tokio::time::sleep(Duration::from_millis(100 + attempt * 50)).await;
                if !lock(&v.consumers).contains_key(&id) {
                    return;
                }
                let Some(q) = v.queue(&rec.queue) else {
                    v.consumer_gone(id, MqError::not_found(format!("queue '{}' was deleted", rec.queue)));
                    return;
                };
                let Some(sender) = lock(&v.conns).get(&rec.conn).cloned() else {
                    return;
                };
                rec.queue_id.store(q.id, Ordering::Relaxed);
                let h = v.slot(&q);
                let (tx, rx) = oneshot::channel();
                let spec = ConsumerSpec {
                    id,
                    holder: Holder {
                        origin: 0,
                        conn: rec.conn,
                    },
                    sink: Arc::new(LocalSink {
                        events: sender,
                        rec: Arc::clone(&rec),
                        vhost: Arc::downgrade(&v),
                    }),
                    no_ack: rec.no_ack,
                    exclusive: rec.exclusive,
                    priority: rec.priority,
                };
                if !h.send(Cmd::Attach(spec, tx)) {
                    continue;
                }
                match rx.await {
                    Ok(Ok(())) => {
                        let credit = rec.credit_out.load(Ordering::Relaxed);
                        let _ = h.send(Cmd::Credit {
                            consumer: id,
                            credit,
                            drain: false,
                        });
                        return;
                    }
                    Ok(Err(e)) if e.code != MqCode::OwnerMoved => {
                        v.consumer_gone(id, e);
                        return;
                    }
                    _ => {}
                }
            }
            v.consumer_gone(
                id,
                MqError::new(MqCode::OwnerMoved, format!("queue '{}' has no owner", rec.queue)),
            );
        });
    }

    /// Ends a consumer on the engine's side and tells its connection.
    pub(crate) fn consumer_gone(&self, id: ConsumerId, reason: MqError) {
        let Some(rec) = lock(&self.consumers).remove(&id) else {
            return;
        };
        if let Some(tx) = lock(&self.conns).get(&rec.conn) {
            let _ = tx.send(sprig_proto_amqp::backend::Event::Cancelled {
                consumer: id,
                reason,
            });
        }
    }

    /// Deletes a queue on the engine's side (auto-delete, `x-expires`,
    /// exclusive queues of a closed connection).
    pub(crate) async fn drop_queue(self: &Arc<Self>, q: &Arc<QueueDef>) {
        let h = self.slot(q);
        let (tx, rx) = oneshot::channel();
        if h.send(Cmd::Delete {
            if_unused: false,
            if_empty: false,
            reply: tx,
        }) {
            let _ = rx.await;
        }
        lock(&self.owners).remove(&q.id);
        self.forget_queue(q).await;
    }

    /// Removes a queue from the model (temporary or durable).
    pub(crate) async fn forget_queue(&self, q: &QueueDef) {
        if q.temp_on.is_some() {
            let set = {
                let mut t = lock(&self.temp);
                t.queues.remove(&q.name);
                t.bindings.retain(|b| {
                    !(b.dest_kind == sprig_proto_amqp::backend::DestKind::Queue && b.dest == q.name)
                });
                t.clone()
            };
            if let Some(e) = self.engine.upgrade() {
                e.deps.temps.publish(&self.r, &set);
            }
        } else if let Some(e) = self.engine.upgrade()
            && let Err(err) = e
                .deps
                .catalog
                .change(&self.r, TopoChange::DelQueue(q.name.clone()))
                .await
        {
            tracing::warn!(queue = %q.name, error = %err, "queue delete not recorded in the catalog");
        }
    }

    /// Enqueues outbox rows (forwarder, `tx.commit`): waits for every
    /// destination's confirm. `Ok` when every row is done.
    pub(crate) async fn forward(self: &Arc<Self>, rows: &[OutboxRow]) -> Result<(), MqError> {
        let mut waits = Vec::new();
        for row in rows {
            let mut m = match msg::rebuild(
                row.fmt,
                row.exchange.clone(),
                row.routing_key.clone(),
                row.props.clone(),
                row.body.clone(),
            ) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, "unreadable outbox row dropped");
                    continue;
                }
            };
            let deaths = row.deaths.as_deref().and_then(msg::decode_deaths);
            if deaths.is_some() {
                m.info.ttl_ms = None;
            }
            let m = Arc::new(m);
            let targets: Vec<Arc<QueueDef>> = match &row.dest {
                OutDest::Queues(ids) => ids.iter().filter_map(|i| self.queue_by_id(*i)).collect(),
                OutDest::Exchange { name, key } => self
                    .with_view(|v| v.route(name, key, &m.info.headers))
                    .unwrap_or_default(),
            };
            if row.kind == OutKind::DeadLetter {
                metrics::counter!("sprig_queue_published_total", "path" => "dead_letter")
                    .increment(targets.len() as u64);
            }
            for q in targets {
                let (tx, rx) = oneshot::channel();
                let h = self.slot(&q);
                if h.send(Cmd::Enqueue(vec![(
                    Incoming {
                        msg: Arc::clone(&m),
                        msg_id: row.out_id,
                        dedup: true,
                        deaths: deaths.clone(),
                    },
                    Some(tx),
                )])) {
                    waits.push(rx);
                }
            }
        }
        for w in waits {
            match w.await {
                Ok(Ok(())) => {}
                // A full queue refuses the message: done, as for a publish.
                Ok(Err(e)) if e.code == MqCode::QueueFull => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(MqError::new(MqCode::OwnerMoved, "owner stopped")),
            }
        }
        Ok(())
    }

    async fn forward_store(self: &Arc<Self>, store: &Arc<dyn MsgStore>, now_ms: i64) -> usize {
        if let Err(e) = store.release_due(now_ms, 500).await {
            tracing::debug!(error = %e, "delayed release failed");
        }
        let rows = match store.outbox(500).await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(error = %e, "outbox read failed");
                return 0;
            }
        };
        if rows.is_empty() {
            return 0;
        }
        let n = rows.len();
        match self.forward(&rows).await {
            Ok(()) => {
                let ids: Vec<_> = rows.iter().map(|r| r.out_id).collect();
                if let Err(e) = store.outbox_done(&ids).await {
                    tracing::debug!(error = %e, "outbox cleanup failed");
                }
                n
            }
            Err(e) => {
                tracing::debug!(error = %e, "outbox forward deferred");
                0
            }
        }
    }

    /// The forwarder: this vhost's memory store always, its durable store
    /// while holding the forwarder lease.
    async fn forwarder(self: Arc<Self>) {
        let Some(e) = self.engine.upgrade() else {
            return;
        };
        let deps = e.deps.clone();
        drop(e);
        let fwd_object = FORWARDER_OBJECT | u64::from(self.r.db_id);
        let mut lease: Option<CancellationToken> = None;
        let mut next_try = 0i64;
        let mut durable: Option<Arc<dyn MsgStore>> = None;
        loop {
            tokio::select! {
                () = self.stop.cancelled() => break,
                () = self.wake.notified() => {}
                () = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
            let now = deps.clock.now_ms();
            let mem: Arc<dyn MsgStore> = Arc::clone(&self.mem) as Arc<dyn MsgStore>;
            while self.forward_store(&mem, now).await > 0 {}
            if lease.as_ref().is_some_and(CancellationToken::is_cancelled) {
                lease = None;
            }
            if lease.is_none() && now >= next_try {
                next_try = now + 2_000;
                if let Ok(Acquired::Mine(t)) = deps.leases.acquire(self.r.tenant, fwd_object).await {
                    lease = Some(t);
                }
            }
            if lease.is_none() {
                continue;
            }
            if durable.is_none() {
                durable = deps.catalog.store(&self.r).await.ok();
            }
            if let Some(s) = &durable {
                while self.forward_store(s, now).await > 0 {}
            }
        }
        if lease.is_some() {
            deps.leases.release(self.r.tenant, fwd_object).await;
        }
    }

    async fn supervise(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<OwnerEvent>) {
        loop {
            let ev = tokio::select! {
                () = self.stop.cancelled() => return,
                ev = rx.recv() => match ev { Some(e) => e, None => return },
            };
            match ev {
                OwnerEvent::Outbox => self.wake.notify_one(),
                OwnerEvent::Stopped { queue, .. } => {
                    let mut o = lock(&self.owners);
                    if o.get(&queue).is_some_and(|h| !h.alive()) {
                        o.remove(&queue);
                    }
                }
                OwnerEvent::AutoDelete { queue } => {
                    if let Some(q) = self.queue_by_id(queue) {
                        let v = Arc::clone(&self);
                        tokio::spawn(async move { v.drop_queue(&q).await });
                    }
                }
            }
        }
    }
}

/// The forwarder lease's object id: this bit plus the database id.
pub const FORWARDER_OBJECT: u64 = 1 << 62;

/// Delivers to a local connection, counting credit for re-attachment.
pub(crate) struct LocalSink {
    pub(crate) events: EventSender,
    pub(crate) rec: Arc<ConsumerRec>,
    pub(crate) vhost: Weak<VShared>,
}

impl DeliverySink for LocalSink {
    fn deliver(&self, consumer: ConsumerId, d: Delivery) -> bool {
        let _ = self
            .rec
            .credit_out
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| Some(c.saturating_sub(1)));
        self.events.deliver(consumer, d)
    }

    fn cancelled(&self, consumer: ConsumerId, reason: MqError) {
        let Some(v) = self.vhost.upgrade() else {
            return;
        };
        if reason.code == MqCode::OwnerMoved && !v.stop.is_cancelled() {
            v.reattach(consumer, Arc::clone(&self.rec));
        } else {
            v.consumer_gone(consumer, reason);
        }
    }

    fn drained(&self, consumer: ConsumerId, credit: u32) {
        let _ = self
            .rec
            .credit_out
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| Some(c.saturating_sub(credit)));
        self.events.drained(consumer, credit);
    }
}

pub(crate) struct Inner {
    pub(crate) deps: EngineDeps,
    vhosts: Mutex<HashMap<(u64, u32), Arc<VShared>>>,
    temp_ids: AtomicU64,
    stop: CancellationToken,
}

impl Inner {
    /// A new temporary queue id: the top bit, this gateway's slot, a
    /// counter.
    pub(crate) fn temp_id(&self) -> u64 {
        (1 << 63) | (u64::from(self.deps.slot & 0x7ffff) << 44) | self.temp_ids.fetch_add(1, Ordering::Relaxed)
    }
}

/// The queue engine of a gateway.
pub struct QueueEngine {
    pub(crate) inner: Arc<Inner>,
}

impl std::fmt::Debug for QueueEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueEngine").finish_non_exhaustive()
    }
}

impl QueueEngine {
    /// An engine over `deps`.
    #[must_use]
    pub fn new(deps: EngineDeps) -> Self {
        Self {
            inner: Arc::new(Inner {
                deps,
                vhosts: Mutex::new(HashMap::new()),
                temp_ids: AtomicU64::new(1),
                stop: CancellationToken::new(),
            }),
        }
    }

    /// Stops every owner and forwarder (the gateway is stopping); their
    /// leases are given back.
    pub fn shutdown(&self) {
        self.inner.stop.cancel();
        for v in lock(&self.inner.vhosts).values() {
            v.stop.cancel();
            for h in lock(&v.owners).values() {
                let _ = h.send(Cmd::Stop);
            }
        }
    }

    /// Sends `connection.blocked` (or unblocked) to every connection: the
    /// tests' stand-in for quotas and write back-pressure.
    pub fn set_blocked(&self, blocked: bool) {
        for v in lock(&self.inner.vhosts).values() {
            for tx in lock(&v.conns).values() {
                let _ = tx.send(if blocked {
                    sprig_proto_amqp::backend::Event::Blocked("low on disk".into())
                } else {
                    sprig_proto_amqp::backend::Event::Unblocked
                });
            }
        }
    }

    pub(crate) fn vshared(&self, r: &VhostRef) -> Arc<VShared> {
        let mut g = lock(&self.inner.vhosts);
        if let Some(v) = g.get(&(r.tenant, r.db_id)) {
            return Arc::clone(v);
        }
        let (etx, erx) = mpsc::unbounded_channel();
        let v = Arc::new(VShared {
            r: r.clone(),
            engine: Arc::downgrade(&self.inner),
            temp: Mutex::new(TempSet::default()),
            mem: Arc::new(MemStore::new()),
            conns: Mutex::new(HashMap::new()),
            consumers: Mutex::new(HashMap::new()),
            owners: Mutex::new(HashMap::new()),
            events: etx,
            wake: Arc::new(Notify::new()),
            blocked: std::sync::atomic::AtomicBool::new(false),
            quota: Mutex::new((0, 0)),
            stop: self.inner.stop.child_token(),
        });
        g.insert((r.tenant, r.db_id), Arc::clone(&v));
        tokio::spawn(Arc::clone(&v).supervise(erx));
        tokio::spawn(Arc::clone(&v).forwarder());
        v
    }

    /// The vhost of a tenant database, if this gateway has opened it.
    pub(crate) fn vhost_of(&self, tenant: u64, db_id: u32) -> Option<Arc<VShared>> {
        lock(&self.inner.vhosts).get(&(tenant, db_id)).cloned()
    }

    /// Wakes the forwarder of a vhost (after a transactional publish
    /// committed).
    pub fn wake_forwarder(&self, tenant: u64, db_id: u32) {
        if let Some(v) = self.vhost_of(tenant, db_id) {
            v.wake.notify_one();
        }
    }
}

#[async_trait]
impl QueueBackend for QueueEngine {
    async fn open_vhost(
        &self,
        principal: &Principal,
        vhost: &str,
        conn: ConnKey,
        events: EventSender,
    ) -> Result<Arc<dyn Vhost>, MqError> {
        let r = self.inner.deps.catalog.open(principal, vhost).await?;
        self.inner.deps.catalog.refresh(&r).await?;
        let v = self.vshared(&r);
        lock(&v.conns).insert(conn, events);
        Ok(Arc::new(crate::vhost::ConnVhost::new(v, conn)))
    }
}
