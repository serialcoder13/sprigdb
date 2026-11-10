//! AMQP fan-in consumers and the ready index (chapter 42, "Fan-in:
//! reading every member's instance"; chapter 50, "Fan-in consumers"):
//!
//! ```text
//! basic.consume(q, arguments {x-sprig-fan-in: true})   (1.0: link property)
//!   in an ancestor P (the declarer, or an intermediate member) of a queue
//!   definition's members, holding SPRIG_FAN_IN, SELECT and DELETE on q
//!   refused (406): exclusive, x-priority, x-stream-offset; and consuming a
//!     definition's name without the argument in a tenant with no instance
//!   lease key inherit-fanin/<def>/<consumer>      (while attached)
//!   scan:  every indexed instance below P (residency deny_remote_access
//!          skipped, counted) is asked for its ready count once (this
//!          starts its owner, which then keeps the index)
//!   watch: inherit-ready/<def>/<tenant>           (the ready index)
//!   for each ready member: one consumer on its instance (internal
//!     principal), the outer consumer's credit spread over them in turn;
//!     a member that drains gives its credit back (drain)
//!   deliveries: x-sprig-tenant added when missing; delivery ids remapped
//!     (instance queue ids repeat across members); settlements go back to
//!     the member's owner
//! ReadyIndex (every gateway, the engine's ReadyHook): a local owner of an
//!   instance reports empty <-> ready; while any inherit-fanin/<def>/ key
//!   exists, the gateway puts or deletes inherit-ready/<def>/<tenant>, at
//!   most once per inherit.ready_index_debounce_ms
//! ```

#![allow(clippy::too_many_lines)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::StreamExt;
use sprig_common::CancellationToken;
use sprig_common::ids::TenantId;
use sprig_inherit::msg::model::db_key;
use sprig_inherit::msg::{Chain, DefKey, DefKind, Definition};
use sprig_meta::{ListOpts, MetaStore, Op, Txn, WatchEvent};
use sprig_proto_amqp::auth::Principal;
use sprig_proto_amqp::backend::{
    ConnKey, ConsumeSpec, ConsumerId, Event, EventSender, OpCtx, QueueBackend,
    QueueSpec as AmqpQueue, Settlement, Vhost,
};
use sprig_proto_amqp::error::MqError;
use sprig_proto_amqp::message::{Delivery, DeliveryId};
use sprig_proto_amqp::v091::codec::{FieldTable, FieldValue};
use sprig_queues::engine::{ReadyHook, VhostRef};
use tokio::sync::{Notify, mpsc};

use super::mq::{TENANT_HEADER, declared_in, internal_principal};
use crate::tenant::TenantServices;

/// The consume argument (and AMQP 1.0 link property).
pub const FAN_IN_ARG: &str = "x-sprig-fan-in";

/// Delivery ids of fan-in deliveries carry this bit in `queue`: an
/// instance's queue id repeats across members, so each (member, queue)
/// gets an id of its own on the reader's connection.
pub const FANIN_ID_BIT: u64 = 1 << 61;

/// How long a member's consumer stays attached with nothing ready.
const IDLE_DETACH: Duration = Duration::from_secs(30);
/// Credit a member holds without delivering for this long goes back to
/// the pool (so a member that drained, or a stale ready entry, doesn't
/// keep the reader's credit).
const IDLE_RECLAIM: Duration = Duration::from_secs(1);
/// Instances asked for their ready count at once during the attach scan.
const SCAN_PARALLEL: usize = 16;
/// The lease of a fan-in consumer's key.
const LEASE_TTL: Duration = Duration::from_secs(10);

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Whether `spec` asks for a fan-in consumer.
#[must_use]
pub fn wants_fan_in(args: &FieldTable) -> bool {
    match args.get(FAN_IN_ARG) {
        Some(FieldValue::Bool(b)) => *b,
        Some(FieldValue::LongStr(s)) => s.eq_ignore_ascii_case(b"true"),
        Some(FieldValue::I8(n)) => *n != 0,
        Some(FieldValue::U8(n)) => *n != 0,
        Some(FieldValue::I32(n)) => *n != 0,
        Some(FieldValue::I64(n)) => *n != 0,
        _ => false,
    }
}

/// The queue definition `name` of database `db` a fan-in consumer in the
/// chain's member may read: one declared at some level of the chain.
#[must_use]
pub fn queue_definition(chain: &Chain, db: &str, name: &str) -> Option<Arc<Definition>> {
    declared_in(chain, db)
        .into_iter()
        .find(|d| d.kind == DefKind::Queue && d.name == name)
}

/// `<tenant>.<object>` (a [`DefKey`]'s display form).
fn parse_def(s: &str) -> Option<DefKey> {
    let (t, o) = s.split_once('.')?;
    Some(DefKey {
        tenant: TenantId(t.parse().ok()?),
        object: o.parse().ok()?,
    })
}

fn ready_prefix(def: DefKey) -> String {
    format!("inherit-ready/{def}/")
}

fn fanin_prefix(def: DefKey) -> String {
    format!("inherit-fanin/{def}/")
}

// ----------------------------------------------------------------------
// The ready index, written by the gateways that run instances' owners.
// ----------------------------------------------------------------------

/// A local owner's report.
#[derive(Debug)]
struct Report {
    tenant: TenantId,
    db: String,
    queue: String,
    ready: bool,
}

/// The engine's [`ReadyHook`]: keeps `inherit-ready/<def>/<tenant>` for
/// the instances whose owners run on this gateway, while a fan-in consumer
/// of the definition is attached anywhere in the DC.
pub struct ReadyIndex {
    tx: mpsc::UnboundedSender<Report>,
}

impl std::fmt::Debug for ReadyIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadyIndex").finish_non_exhaustive()
    }
}

impl ReadyHook for ReadyIndex {
    fn ready(&self, v: &VhostRef, queue: &str, _id: u64, ready: bool) {
        let _ = self.tx.send(Report {
            tenant: TenantId(v.tenant),
            db: v.db.clone(),
            queue: queue.to_owned(),
            ready,
        });
    }
}

impl ReadyIndex {
    /// Starts the index's task (stopped by `stop`).
    #[must_use]
    pub fn start(svc: Arc<TenantServices>, stop: CancellationToken) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run_index(svc, rx, stop));
        Arc::new(Self { tx })
    }
}

/// The definition whose instance `queue` of `tenant`'s database `db` is,
/// if it is one.
async fn instance_def(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    db: &str,
    queue: &str,
) -> Option<DefKey> {
    let cat = super::catalog(svc, tenant).await.ok()?;
    if cat.ancestry.is_empty() && super::state_of(&cat).is_empty() {
        return None;
    }
    let chain = super::chain(svc, &cat).await.ok()?;
    let d = chain.resolve(db, DefKind::Queue, queue)?;
    Some(d.key())
}

async fn run_index(
    svc: Arc<TenantServices>,
    mut rx: mpsc::UnboundedReceiver<Report>,
    stop: CancellationToken,
) {
    let store = svc.locator.dc_store();
    // (tenant, db, queue) -> its definition (None: not an instance).
    let mut defs: HashMap<(TenantId, String, String), Option<DefKey>> = HashMap::new();
    // What local owners last said, per instance.
    let mut known: HashMap<(DefKey, TenantId), bool> = HashMap::new();
    // Definitions with an attached fan-in consumer.
    let mut leases: HashSet<DefKey> = HashSet::new();
    // To write at the next flush; what was written last.
    let mut pending: HashMap<(DefKey, TenantId), bool> = HashMap::new();
    let mut written: HashMap<(DefKey, TenantId), bool> = HashMap::new();
    let debounce = || {
        let ms = svc.settings.get(
            &sprig_inherit::msg::settings::READY_INDEX_DEBOUNCE_MS,
            &sprig_settings::SettingCtx::process(&svc.dc),
            None,
        );
        Duration::from_millis(ms.max(50))
    };
    // Each key is written at once, then at most once per debounce.
    let mut flush = tokio::time::interval(Duration::from_millis(50));
    let mut last_write: HashMap<(DefKey, TenantId), tokio::time::Instant> = HashMap::new();
    let mut refresh = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            r = rx.recv() => {
                let Some(r) = r else { return };
                let k = (r.tenant, db_key(&r.db), r.queue.clone());
                let def = match defs.get(&k) {
                    Some(d) => *d,
                    None => {
                        let d = instance_def(&svc, r.tenant, &r.db, &r.queue).await;
                        if defs.len() > 100_000 {
                            defs.clear();
                        }
                        defs.insert(k, d);
                        d
                    }
                };
                if let Some(def) = def {
                    known.insert((def, r.tenant), r.ready);
                    if leases.contains(&def) {
                        pending.insert((def, r.tenant), r.ready);
                    }
                }
            }
            _ = refresh.tick() => {
                let Ok(now) = fanin_defs(&*store).await else { continue };
                for d in now.difference(&leases) {
                    // A consumer appeared: what local owners know goes out.
                    for ((def, t), ready) in &known {
                        if def == d {
                            pending.insert((*def, *t), *ready);
                        }
                    }
                }
                leases = now;
                written.retain(|(d, _), _| leases.contains(d));
            }
            _ = flush.tick() => {
                let now = tokio::time::Instant::now();
                let gap = debounce();
                for ((def, t), ready) in std::mem::take(&mut pending) {
                    if written.get(&(def, t)) == Some(&ready) {
                        continue;
                    }
                    if last_write.get(&(def, t)).is_some_and(|w| now.duration_since(*w) < gap) {
                        pending.insert((def, t), ready);
                        continue;
                    }
                    last_write.insert((def, t), now);
                    if last_write.len() > 100_000 {
                        last_write.retain(|_, w| now.duration_since(*w) < gap);
                    }
                    let Ok(key) = store.tier().key(&format!("{}{}", ready_prefix(def), t.0)) else {
                        continue;
                    };
                    let op = if ready {
                        Op::Put { key, value: b"1".to_vec(), lease: None }
                    } else {
                        Op::Delete(key)
                    };
                    let ok = store
                        .txn(Txn { when: Vec::new(), then: vec![op], otherwise: Vec::new() })
                        .await
                        .is_ok();
                    if ok {
                        written.insert((def, t), ready);
                        metrics::counter!("sprig_inherit_ready_index_writes_total",
                            "op" => if ready { "put" } else { "delete" }).increment(1);
                    } else {
                        pending.insert((def, t), ready);
                    }
                }
            }
        }
    }
}

/// Removes member `t`'s ready-index key of `def` (its instance is gone).
pub async fn forget_ready(svc: &TenantServices, def: DefKey, t: TenantId) {
    let store = svc.locator.dc_store();
    if let Ok(key) = store.tier().key(&format!("{}{}", ready_prefix(def), t.0)) {
        let _ = store
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Delete(key)],
                otherwise: Vec::new(),
            })
            .await;
    }
}

/// Definitions with at least one `inherit-fanin/<def>/...` key.
async fn fanin_defs(store: &dyn MetaStore) -> sprig_common::Result<HashSet<DefKey>> {
    let prefix = store.tier().key("inherit-fanin/")?;
    let page = store
        .list(
            &prefix,
            ListOpts {
                limit: 100_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| {
            let s = kv.key.as_str().to_owned();
            let rest = s.split("inherit-fanin/").nth(1)?;
            parse_def(rest.split('/').next()?)
        })
        .collect())
}

// ----------------------------------------------------------------------
// Fan-in consumers.
// ----------------------------------------------------------------------

/// One member's consumer under a fan-in consumer.
struct Sub {
    member: TenantId,
    name: String,
    vhost: Arc<dyn Vhost>,
    cx: OpCtx,
    /// The current consumer (none: detached).
    consumer: Option<ConsumerId>,
    /// Credit given to it and not used yet.
    out: u32,
    /// A drain was asked for.
    draining: bool,
    /// Last time it was ready (for detaching idle members).
    last_ready: tokio::time::Instant,
    /// Last delivery, or credit given (for taking idle credit back).
    last_active: tokio::time::Instant,
}

/// What the reader's connection and the task share.
#[derive(Default)]
struct Shared {
    /// Remapped queue id -> (member's vhost, its context, the real id).
    ids: HashMap<u64, (Arc<dyn Vhost>, OpCtx, u64)>,
    /// (member, real queue id) -> remapped id.
    rev: HashMap<(TenantId, u64), u64>,
    /// Credit from the reader's front end, not yet given to a member.
    pool: u32,
    /// The reader asked to drain (AMQP 1.0).
    drain: bool,
    /// Cancelled.
    done: bool,
    /// The members' connections, closed with the reader's.
    conns: Vec<(Arc<dyn Vhost>, ConnKey)>,
}

/// A fan-in consumer of the reader's connection.
pub struct FanIn {
    outer: ConsumerId,
    shared: Arc<Mutex<Shared>>,
    wake: Arc<Notify>,
    stop: CancellationToken,
}

impl std::fmt::Debug for FanIn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanIn")
            .field("outer", &self.outer)
            .finish_non_exhaustive()
    }
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

impl FanIn {
    /// The reader's consumer id.
    #[must_use]
    pub fn consumer(&self) -> ConsumerId {
        self.outer
    }

    /// The reader's front end gave `credit` more.
    pub fn credit(&self, credit: u32, drain: bool) {
        {
            let mut s = lock(&self.shared);
            s.pool = s.pool.saturating_add(credit);
            s.drain |= drain;
        }
        self.wake.notify_one();
    }

    /// Settles deliveries of this consumer; returns the ones that aren't.
    pub fn settle(&self, s: Vec<(DeliveryId, Settlement)>) -> Vec<(DeliveryId, Settlement)> {
        let mut rest = Vec::new();
        let mut by: Vec<(Arc<dyn Vhost>, OpCtx, Vec<(DeliveryId, Settlement)>)> = Vec::new();
        {
            let sh = lock(&self.shared);
            for (id, st) in s {
                let Some((v, cx, real)) = sh.ids.get(&id.queue) else {
                    rest.push((id, st));
                    continue;
                };
                let real_id = DeliveryId {
                    queue: *real,
                    ..id
                };
                if let Some(e) = by.iter_mut().find(|e| Arc::ptr_eq(&e.0, v)) {
                    e.2.push((real_id, st));
                } else {
                    by.push((Arc::clone(v), cx.clone(), vec![(real_id, st)]));
                }
            }
        }
        for (v, cx, list) in by {
            v.settle(&cx, list);
        }
        rest
    }

    /// Whether `queue` (a delivery id's) belongs to this consumer.
    #[must_use]
    pub fn owns(&self, queue: u64) -> bool {
        lock(&self.shared).ids.contains_key(&queue)
    }

    /// Ends the consumer; its unsettled deliveries can still be settled.
    pub fn cancel(&self) {
        lock(&self.shared).done = true;
        self.stop.cancel();
    }

    /// The reader's connection ended (its unsettled deliveries were
    /// settled back through [`FanIn::settle`] first): the members'
    /// connections end too.
    pub async fn close(&self) {
        self.cancel();
        let conns = std::mem::take(&mut lock(&self.shared).conns);
        for (v, c) in conns {
            v.connection_closed(c).await;
        }
    }
}

/// What a fan-in consumer can't be: exclusive (single active), with a
/// priority, or with `x-stream-offset`.
///
/// # Errors
/// 406 (`PRECONDITION_FAILED`).
pub fn refuse_unsupported(spec: &ConsumeSpec) -> Result<(), MqError> {
    if spec.exclusive {
        return Err(MqError::precondition(
            "a fan-in consumer can't be exclusive (single active consumer)",
        ));
    }
    if spec.priority != 0 {
        return Err(MqError::precondition(
            "a fan-in consumer can't have x-priority",
        ));
    }
    if spec.arguments.get("x-stream-offset").is_some() {
        return Err(MqError::precondition(
            "a fan-in consumer can't have x-stream-offset",
        ));
    }
    Ok(())
}

/// Whether `principal` holds `SPRIG_FAN_IN`, `SELECT` and `DELETE` on
/// `db.name` in its tenant (its catalog `cat`).
#[must_use]
pub fn allowed(
    svc: &TenantServices,
    cat: &sprig_catalog::TenantCatalog,
    principal: &Principal,
    db: &str,
    name: &str,
) -> bool {
    use sprig_auth::authz::{Action, Target};
    use sprig_catalog::ddl::{database_key, object_key};
    use sprig_catalog::store::ObjectKind;
    let Some(user) = principal.user_id else {
        return true;
    };
    let g = crate::tenant::authz::grants_of(
        svc,
        cat,
        Some(sprig_common::ids::UserId(
            u32::try_from(user).unwrap_or(u32::MAX),
        )),
    );
    if g.unrestricted() {
        return true;
    }
    let lctn = svc.settings.get(
        &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
        &cat.setting_ctx(&svc.dc),
        Some(&cat.settings),
    ) == 1;
    let db_key = database_key(db, lctn);
    let name_key = object_key(ObjectKind::Table, name, lctn);
    let on = |a: Action| {
        g.allows(a, Target::Tenant)
            || g.allows(a, Target::Database { db_key: &db_key })
            || g.allows(
                a,
                Target::Named {
                    db_key: &db_key,
                    name_key: &name_key,
                },
            )
    };
    g.allows(Action::FanIn, Target::Tenant) && on(Action::Select) && on(Action::Delete)
}

/// Starts a fan-in consumer of `def` (a queue definition of database
/// `db`) for `reader`'s connection: its deliveries go to `events` as
/// `spec.consumer`.
///
/// # Errors
/// The engine isn't running here.
pub fn start(
    svc: &Arc<TenantServices>,
    reader: TenantId,
    def: &Arc<Definition>,
    db: &str,
    spec: &ConsumeSpec,
    channel: u16,
    events: EventSender,
) -> Result<Arc<FanIn>, MqError> {
    let engine = svc
        .mq
        .get()
        .cloned()
        .ok_or_else(|| MqError::not_implemented("the queue engine isn't running here"))?;
    let fan = Arc::new(FanIn {
        outer: spec.consumer,
        shared: Arc::new(Mutex::new(Shared::default())),
        wake: Arc::new(Notify::new()),
        stop: CancellationToken::new(),
    });
    let task = Task {
        svc: Arc::clone(svc),
        engine,
        reader,
        def: Arc::clone(def),
        db: db.to_owned(),
        no_ack: spec.no_ack,
        tag: spec.tag.clone(),
        channel,
        events,
        fan: Arc::clone(&fan),
    };
    tokio::spawn(task.run());
    metrics::gauge!("sprig_inherit_fanin_consumers").increment(1.0);
    Ok(fan)
}

struct Task {
    svc: Arc<TenantServices>,
    engine: Arc<sprig_queues::QueueEngine>,
    reader: TenantId,
    def: Arc<Definition>,
    db: String,
    no_ack: bool,
    tag: String,
    channel: u16,
    events: EventSender,
    fan: Arc<FanIn>,
}

impl Task {
    /// The members below the reader holding an instance, by the index;
    /// members whose residency denies remote access are left out.
    async fn members(&self) -> Vec<(TenantId, String)> {
        let Ok(list) = super::instances_of(&self.svc, self.def.key()).await else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for e in list {
            if e.kind != DefKind::Queue {
                continue;
            }
            if let Some(m) = self.in_scope(e.tenant).await {
                out.push((e.tenant, m));
            }
        }
        out
    }

    /// The member's name if the reader may fan in from it.
    async fn in_scope(&self, t: TenantId) -> Option<String> {
        let d = super::dir_entry(&self.svc, t).await.ok()??;
        if t != self.reader && !d.path_ids().contains(&self.reader.0) {
            return None;
        }
        if sprig_auth::TenantResidency::of(&d.residency).is_some_and(|r| r.deny_remote_access) {
            metrics::counter!("sprig_inherit_fanin_skipped_total", "reason" => "residency")
                .increment(1);
            return None;
        }
        Some(d.name)
    }

    /// Asks each instance for its ready count once (starting its owner,
    /// which keeps the index from then on).
    async fn scan(&self, members: Vec<(TenantId, String)>) -> Vec<TenantId> {
        let name = self.def.name.clone();
        futures::stream::iter(members)
            .map(|(t, tname)| {
                let engine = Arc::clone(&self.engine);
                let db = self.db.clone();
                let name = name.clone();
                async move {
                    let v = engine
                        .sql_vhost(Arc::new(internal_principal(t, &tname)), &db, 0)
                        .await
                        .ok()?;
                    let cx = OpCtx {
                        principal: Arc::new(internal_principal(t, &tname)),
                        conn: ConnKey(sprig_queues::sql::SQL_CONN_BIT),
                        channel: 0,
                    };
                    let d = v
                        .conn_vhost()
                        .declare_queue(
                            &cx,
                            AmqpQueue {
                                name,
                                durable: true,
                                ..AmqpQueue::default()
                            },
                            true,
                        )
                        .await
                        .ok()?;
                    (d.message_count > 0).then_some(t)
                }
            })
            .buffer_unordered(SCAN_PARALLEL)
            .filter_map(|x| async move { x })
            .collect()
            .await
    }

    async fn run(self) {
        let store = self.svc.locator.dc_store();
        let stop = self.fan.stop.clone();
        // The lease key: owners keep the ready index while it exists.
        let lease = match store.grant_lease(LEASE_TTL).await {
            Ok(l) => Some(l),
            Err(e) => {
                tracing::warn!(error = %e, "fan-in consumer: no lease; the ready index isn't kept");
                None
            }
        };
        let me = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let lease_key = store
            .tier()
            .key(&format!("{}{}-{me}", fanin_prefix(self.def.key()), std::process::id()))
            .ok();
        if let (Some(l), Some(k)) = (lease, lease_key.clone()) {
            let _ = store
                .txn(Txn {
                    when: Vec::new(),
                    then: vec![Op::Put {
                        key: k,
                        value: self.reader.0.to_string().into_bytes(),
                        lease: Some(l),
                    }],
                    otherwise: Vec::new(),
                })
                .await;
            let s2 = Arc::clone(&store);
            let st = stop.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = st.cancelled() => break,
                        () = tokio::time::sleep(LEASE_TTL / 3) => {}
                    }
                    if !matches!(s2.keep_alive(l).await, Ok(Some(_))) {
                        tracing::warn!("fan-in consumer's lease lost");
                    }
                }
                let _ = s2.revoke_lease(l).await;
            });
        }
        // Ready members: the index; and, until each is found idle once,
        // what the attach scan saw (owners that started before this
        // consumer's lease never wrote the index).
        let ready: Arc<Mutex<BTreeSet<TenantId>>> = Arc::new(Mutex::new(BTreeSet::new()));
        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel::<Event>();
        let members = self.members().await;
        let n_members = members.len();
        let mut hints: BTreeSet<TenantId> = self.scan(members).await.into_iter().collect();
        tracing::debug!(def = %self.def.key(), members = n_members, ready = hints.len(), "fan-in consumer attached");
        {
            let ready = Arc::clone(&ready);
            let wake = Arc::clone(&self.fan.wake);
            let st = stop.clone();
            let store = Arc::clone(&store);
            let prefix = ready_prefix(self.def.key());
            tokio::spawn(async move { watch_ready(&*store, &prefix, &ready, &wake, &st).await });
        }
        let mut subs: HashMap<TenantId, Sub> = HashMap::new();
        let mut by_consumer: HashMap<ConsumerId, TenantId> = HashMap::new();
        let mut names: HashMap<TenantId, Option<String>> = HashMap::new();
        let mut warned: HashSet<TenantId> = HashSet::new();
        let mut rr: usize = 0;
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                () = self.fan.wake.notified() => {}
                _ = tick.tick() => {}
                ev = sub_rx.recv() => {
                    let Some(ev) = ev else { break };
                    self.on_event(ev, &mut subs, &by_consumer);
                    while let Ok(ev) = sub_rx.try_recv() {
                        self.on_event(ev, &mut subs, &by_consumer);
                    }
                }
            }
            let now_ready: BTreeSet<TenantId> =
                lock(&ready).iter().chain(hints.iter()).copied().collect();
            // Attach to ready members.
            for t in &now_ready {
                if subs.get(t).is_some_and(|s| s.consumer.is_some()) {
                    continue;
                }
                let name = match names.get(t) {
                    Some(n) => n.clone(),
                    None => {
                        let n = self.in_scope(*t).await;
                        names.insert(*t, n.clone());
                        n
                    }
                };
                let Some(tname) = name else {
                    if warned.insert(*t) {
                        tracing::warn!(member = %t, "fan-in: a ready instance outside the reader's scope (skipped)");
                    }
                    continue;
                };
                if !subs.contains_key(t) {
                    let p = internal_principal(*t, &tname);
                    let conn = ConnKey::next();
                    let v = match self
                        .engine
                        .open_vhost(&p, &self.db, conn, sub_tx.clone())
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            if warned.insert(*t) {
                                tracing::warn!(member = %t, error = %e, "fan-in: member's vhost not opened (retried)");
                            }
                            continue;
                        }
                    };
                    lock(&self.fan.shared).conns.push((Arc::clone(&v), conn));
                    subs.insert(
                        *t,
                        Sub {
                            member: *t,
                            name: tname.clone(),
                            vhost: v,
                            cx: OpCtx {
                                principal: Arc::new(p),
                                conn,
                                channel: self.channel,
                            },
                            consumer: None,
                            out: 0,
                            draining: false,
                            last_ready: tokio::time::Instant::now(),
                            last_active: tokio::time::Instant::now(),
                        },
                    );
                }
                let Some(s) = subs.get_mut(t) else { continue };
                let id = ConsumerId::next();
                let r = s
                    .vhost
                    .consume(
                        &s.cx,
                        ConsumeSpec {
                            consumer: id,
                            queue: self.def.name.clone(),
                            tag: format!("{}@{}", self.tag, s.name),
                            no_ack: self.no_ack,
                            exclusive: false,
                            priority: 0,
                            arguments: FieldTable::new(),
                        },
                    )
                    .await;
                match r {
                    Ok(()) => {
                        tracing::debug!(member = %t, "fan-in: attached");
                        s.consumer = Some(id);
                        s.out = 0;
                        s.last_ready = tokio::time::Instant::now();
                        by_consumer.insert(id, *t);
                    }
                    Err(e) => {
                        if warned.insert(*t) {
                            tracing::warn!(member = %t, error = %e, "fan-in: member's queue not consumable (retried)");
                        }
                    }
                }
            }
            let now = tokio::time::Instant::now();
            // Members no longer ready give their credit back; long-idle
            // ones are detached.
            for s in subs.values_mut() {
                let Some(c) = s.consumer else { continue };
                let idle = now.duration_since(s.last_active) > IDLE_RECLAIM;
                if s.out > 0 && !s.draining && (idle || !now_ready.contains(&s.member)) {
                    // Credit back to the pool: the member drained, or
                    // holds it without delivering.
                    s.draining = true;
                    s.vhost.credit(c, 0, true);
                    hints.remove(&s.member);
                    continue;
                }
                if now_ready.contains(&s.member) {
                    s.last_ready = now;
                    continue;
                }
                if s.out == 0 && now.duration_since(s.last_ready) > IDLE_DETACH {
                    s.vhost.cancel(&s.cx, c).await;
                    by_consumer.remove(&c);
                    s.consumer = None;
                }
            }
            self.distribute(&mut subs, &now_ready, &mut rr);
        }
        // Cancelled: the members' consumers end; their unsettled
        // deliveries stay settleable through the shared map.
        for s in subs.values_mut() {
            if let Some(c) = s.consumer.take() {
                s.vhost.cancel(&s.cx, c).await;
            }
        }
        if let Some(k) = lease_key {
            let _ = store
                .txn(Txn {
                    when: Vec::new(),
                    then: vec![Op::Delete(k)],
                    otherwise: Vec::new(),
                })
                .await;
        }
        metrics::gauge!("sprig_inherit_fanin_consumers").decrement(1.0);
        // The members' connections close with the reader's (FanIn::close).
    }

    fn on_event(
        &self,
        ev: Event,
        subs: &mut HashMap<TenantId, Sub>,
        by_consumer: &HashMap<ConsumerId, TenantId>,
    ) {
        match ev {
            Event::Deliver {
                consumer,
                delivery,
            } => {
                let Some(t) = by_consumer.get(&consumer).copied() else {
                    return;
                };
                let Some(s) = subs.get_mut(&t) else { return };
                s.out = s.out.saturating_sub(1);
                s.last_active = tokio::time::Instant::now();
                let mapped = {
                    let mut sh = lock(&self.fan.shared);
                    let real = delivery.id.queue;
                    let id = match sh.rev.get(&(t, real)) {
                        Some(id) => *id,
                        None => {
                            let id = FANIN_ID_BIT | NEXT_ID.fetch_add(1, Ordering::Relaxed);
                            sh.rev.insert((t, real), id);
                            sh.ids
                                .insert(id, (Arc::clone(&s.vhost), s.cx.clone(), real));
                            id
                        }
                    };
                    tag(delivery, id, &s.name)
                };
                metrics::counter!("sprig_inherit_fanin_deliveries_total", "kind" => "queue")
                    .increment(1);
                let _ = self.events.send(Event::Deliver {
                    consumer: self.fan.outer,
                    delivery: mapped,
                });
            }
            Event::Drained { consumer, credit } => {
                tracing::debug!(?consumer, credit, "fan-in: member drained");
                if let Some(s) = by_consumer.get(&consumer).and_then(|t| subs.get_mut(t)) {
                    s.out = s.out.saturating_sub(credit);
                    s.draining = false;
                    let mut sh = lock(&self.fan.shared);
                    sh.pool = sh.pool.saturating_add(credit);
                }
            }
            Event::Cancelled { consumer, reason } => {
                if let Some(s) = by_consumer.get(&consumer).and_then(|t| subs.get_mut(t)) {
                    tracing::debug!(member = %s.member, %reason, "fan-in: member's consumer ended");
                    let mut sh = lock(&self.fan.shared);
                    sh.pool = sh.pool.saturating_add(s.out);
                    s.out = 0;
                    s.consumer = None;
                    s.draining = false;
                }
            }
            Event::Blocked(_) | Event::Unblocked => {}
        }
    }

    /// Spreads the pool over the ready, attached members in turn, so each
    /// holds about an equal share of the reader's credit.
    fn distribute(
        &self,
        subs: &mut HashMap<TenantId, Sub>,
        ready: &BTreeSet<TenantId>,
        rr: &mut usize,
    ) {
        let mut sh = lock(&self.fan.shared);
        let mut live: Vec<TenantId> = ready
            .iter()
            .filter(|t| subs.get(t).is_some_and(|s| s.consumer.is_some() && !s.draining))
            .copied()
            .collect();
        if sh.drain {
            // AMQP 1.0 drain: take every member's credit back; when all
            // are back, give the rest up.
            let mut waiting = false;
            for s in subs.values_mut() {
                if let Some(c) = s.consumer
                    && s.out > 0
                {
                    waiting = true;
                    if !s.draining {
                        s.draining = true;
                        s.vhost.credit(c, 0, true);
                    }
                }
            }
            if !waiting {
                let credit = std::mem::take(&mut sh.pool);
                sh.drain = false;
                let _ = self.events.send(Event::Drained {
                    consumer: self.fan.outer,
                    credit,
                });
            }
            return;
        }
        if live.is_empty() || sh.pool == 0 {
            return;
        }
        let n = live.len();
        live.rotate_left(*rr % n);
        *rr = rr.wrapping_add(1);
        let total: u64 = u64::from(sh.pool)
            + live
                .iter()
                .filter_map(|t| subs.get(t))
                .map(|s| u64::from(s.out))
                .sum::<u64>();
        let share = u32::try_from(total.div_ceil(n as u64)).unwrap_or(u32::MAX);
        for t in live {
            if sh.pool == 0 {
                break;
            }
            let Some(s) = subs.get_mut(&t) else { continue };
            let Some(c) = s.consumer else { continue };
            let give = share.saturating_sub(s.out).min(sh.pool);
            if give == 0 {
                continue;
            }
            sh.pool -= give;
            if s.out == 0 {
                s.last_active = tokio::time::Instant::now();
            }
            s.out += give;
            tracing::debug!(member = %t, give, pool = sh.pool, "fan-in: credit");
            s.vhost.credit(c, give, false);
        }
    }
}

/// A member's delivery as the reader sees it: the remapped id, and
/// `x-sprig-tenant` when the message doesn't carry it.
fn tag(mut d: Delivery, id: u64, tenant: &str) -> Delivery {
    d.id.queue = id;
    let has = d.message.info.headers.get(TENANT_HEADER).is_some();
    if !has {
        d.annotations.insert(
            TENANT_HEADER,
            FieldValue::LongStr(bytes::Bytes::copy_from_slice(tenant.as_bytes())),
        );
    }
    d
}

/// Follows `inherit-ready/<def>/` into `ready`.
async fn watch_ready(
    store: &dyn MetaStore,
    prefix: &str,
    ready: &Mutex<BTreeSet<TenantId>>,
    wake: &Notify,
    stop: &CancellationToken,
) {
    let Ok(key) = store.tier().key(prefix) else {
        return;
    };
    let parse = |k: &sprig_meta::Key| {
        let s = k.as_str().to_owned();
        s.rsplit('/').next().and_then(|t| t.parse::<u64>().ok()).map(TenantId)
    };
    'outer: while !stop.is_cancelled() {
        let Ok(page) = store
            .list(
                &key,
                ListOpts {
                    limit: 1_000_000,
                    ..ListOpts::default()
                },
            )
            .await
        else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        {
            let mut r = ready.lock().unwrap_or_else(PoisonError::into_inner);
            for kv in &page.kvs {
                if let Some(t) = parse(&kv.key) {
                    r.insert(t);
                }
            }
        }
        wake.notify_one();
        let Ok(mut w) = store.watch(&key, page.revision + 1).await else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        loop {
            let ev = tokio::select! {
                () = stop.cancelled() => break 'outer,
                ev = w.next() => ev,
            };
            match ev {
                Some(Ok(WatchEvent::Put(kv))) => {
                    tracing::debug!(key = kv.key.as_str(), "fan-in: ready");
                    if let Some(t) = parse(&kv.key) {
                        ready.lock().unwrap_or_else(PoisonError::into_inner).insert(t);
                        wake.notify_one();
                    }
                }
                Some(Ok(WatchEvent::Delete { key, .. })) => {
                    if let Some(t) = parse(&key) {
                        ready.lock().unwrap_or_else(PoisonError::into_inner).remove(&t);
                        wake.notify_one();
                    }
                }
                Some(Ok(WatchEvent::Progress(_))) => {}
                _ => continue 'outer,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_argument_in_its_forms() {
        let mut a = FieldTable::new();
        assert!(!wants_fan_in(&a));
        a.insert(FAN_IN_ARG, FieldValue::Bool(true));
        assert!(wants_fan_in(&a));
        let mut b = FieldTable::new();
        b.insert(FAN_IN_ARG, FieldValue::LongStr(bytes::Bytes::from_static(b"TRUE")));
        assert!(wants_fan_in(&b));
        let mut c = FieldTable::new();
        c.insert(FAN_IN_ARG, FieldValue::Bool(false));
        assert!(!wants_fan_in(&c));
    }

    #[test]
    fn refusals() {
        let base = ConsumeSpec {
            consumer: ConsumerId(1),
            queue: "q".into(),
            tag: "t".into(),
            no_ack: false,
            exclusive: false,
            priority: 0,
            arguments: FieldTable::new(),
        };
        assert!(refuse_unsupported(&base).is_ok());
        let mut s = base.clone();
        s.exclusive = true;
        assert!(refuse_unsupported(&s).is_err());
        let mut s = base.clone();
        s.priority = 5;
        assert!(refuse_unsupported(&s).is_err());
        let mut s = base;
        s.arguments
            .insert("x-stream-offset", FieldValue::LongStr(bytes::Bytes::from_static(b"first")));
        assert!(refuse_unsupported(&s).is_err());
    }
}
