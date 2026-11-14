//! The shared per-cluster reader (chapter 38, "The cluster reader";
//! chapter 50, "Publishing inside a database transaction"; R9, r9cluster
//! and r9feed).
//!
//! A [`SharedFeed`] holds one **hub** per cluster. A hub reads that
//! cluster's binlog once ([`Clusters::open_scope`]) and splits each
//! committed group by tenant. Its consumers see it through **views**
//! ([`SharedFeed::view`]), each a [`Clusters`] whose [`Clusters::open`]
//! subscribes a tenant to the hub instead of starting a dump. A view keeps
//! either some internal tables of every tenant ([`Kept::Tables`]: the
//! queue forwarder's outboxes, the stream sequencers' `__sprig_eo_*`), or
//! everything of the tenants it serves ([`Kept::All`]: change topics), as a
//! dump of their own would. [`crate::TenantTail`] runs on top unchanged:
//! moves, resume tokens, XA branches and applier filtering work as with a
//! dump of its own.
//!
//! ```text
//! hub(cluster), one task while it has subscriptions:
//!   dump scope: the views' tables, plus the tenants some Kept::All
//!     subscription keeps whole; a wider scope reopens the dump at the
//!     hub's position
//!   each committed group: split by tenant (an XA commit or rollback goes
//!     to the tenants its prepare had rows for), kept in the ring, and
//!     offered to each subscription of its tenants that started before it
//!   a subscription whose buffer is full is detached, never waited for:
//!     the others go on
//!   at most every second: the position to every subscription (idle)
//!   a broken stream reconnects from the hub's position; a lost history
//!     (39800) ends every subscription with that error
//!   no subscriptions for 30 s: the hub stops and closes its dump
//!
//! subscribe(tenant, from), and a detached subscription's return:
//!   from at or after the hub's position → listen
//!   from inside the ring → the ring's groups after from, then listen
//!   the hub started under `gather` ago and delivered nothing → it moves
//!     its start back to from (a worker's restart: every sequencer comes
//!     back at once, each from its own checkpoint)
//!   else → a private catch-up dump for the tenant alone (at most
//!     `max_catchup` per cluster; the others wait for one), which hands
//!     over to the hub as soon as the ring covers its position
//! ```
//!
//! One dump per cluster and process, however many tenants and stream
//! instances: the 1,000-tenant run of R9 item 84 had 25 dumps (one per
//! tenant with queues) fill a node's connections, and item 127 counted 76
//! dumps for 75 stream instances.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use sprig_binlog::{Gtid, GtidPos, Value};
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::Instant;

use crate::source::{Clusters, DumpScope, RawTxn, SourceItem, TxnSource, XaMark};

/// Items a subscription buffers before it is detached.
pub const SUB_CHANNEL: usize = 256;
/// Idle positions go to every subscription at most this often.
pub const IDLE_FANOUT: Duration = Duration::from_secs(1);
/// A hub without subscriptions stops after this long.
pub const HUB_LINGER: Duration = Duration::from_secs(30);

type Item = Result<SourceItem>;

/// A shared reader's sizes and limits.
#[derive(Debug, Clone)]
pub struct SharedOpts {
    /// Items a subscription buffers. One whose buffer is full when a group
    /// for it arrives is detached and comes back through the ring or a
    /// catch-up dump: a slow consumer never holds up the others.
    pub sub_channel: usize,
    /// Groups the ring keeps at most.
    pub ring_groups: usize,
    /// Bytes (estimated) the ring keeps at most (chapter 38's
    /// `feed.ring_mb`).
    pub ring_bytes: usize,
    /// Private catch-up dumps per cluster at once (chapter 38's
    /// `feed.max_catchup_readers_per_cluster`).
    pub max_catchup: usize,
    /// For this long after it starts, while it has delivered no group, a
    /// hub moves its start back for a subscription behind it.
    pub gather: Duration,
    /// A return through the ring with more groups than this takes a
    /// catch-up dump instead.
    pub max_backlog: usize,
}

impl Default for SharedOpts {
    fn default() -> Self {
        Self {
            sub_channel: SUB_CHANNEL,
            ring_groups: 50_000,
            ring_bytes: 64 << 20,
            max_catchup: 4,
            gather: Duration::from_secs(10),
            max_backlog: 4_096,
        }
    }
}

/// What a view keeps of its tenants' schemas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kept {
    /// Rows of these internal tables (exact names, or prefixes ending in
    /// `*`), outside catalogs.
    Tables(Arc<[String]>),
    /// Everything: rows of every schema of the tenant (its catalog too),
    /// its DDL and statement-format DML.
    All,
}

/// Whether group `g` lies after `from` (a domain `from` lacks: yes).
fn after(from: &GtidPos, g: &Gtid) -> bool {
    from.get(g.domain).is_none_or(|x| g.seq > x.seq)
}

/// Per domain the newer of `a` and `b`; domains only one names keep its
/// value.
#[must_use]
pub fn newer(a: &GtidPos, b: &GtidPos) -> GtidPos {
    let mut out = a.clone();
    for g in b.iter() {
        match out.get(g.domain) {
            Some(x) if x.seq >= g.seq => {}
            _ => out.update(*g),
        }
    }
    out
}

/// Per domain the older of `a` and `b`; domains only one names keep its
/// value.
#[must_use]
pub fn older(a: &GtidPos, b: &GtidPos) -> GtidPos {
    let mut out = a.clone();
    for g in b.iter() {
        match out.get(g.domain) {
            Some(x) if x.seq <= g.seq => {}
            _ => out.update(*g),
        }
    }
    out
}

/// A group split by tenant: each tenant's rows in binlog order, the DDL to
/// each tenant it names, statement-format DML to its database's tenant.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn split(t: &RawTxn, xa: &mut HashMap<String, HashSet<TenantId>>) -> Vec<(TenantId, RawTxn)> {
    let mut parts: Vec<(TenantId, RawTxn)> = Vec::new();
    let part = |parts: &mut Vec<(TenantId, RawTxn)>, tenant: TenantId| -> usize {
        if let Some(i) = parts.iter().position(|(x, _)| *x == tenant) {
            return i;
        }
        let mut sub = RawTxn::new(t.gtid, t.before.clone(), t.ts);
        sub.stamps.clone_from(&t.stamps);
        sub.apply = t.apply;
        sub.xa.clone_from(&t.xa);
        parts.push((tenant, sub));
        parts.len() - 1
    };
    for r in &t.rows {
        if let Some(tenant) = DumpScope::tenant_of(&r.table.schema) {
            let i = part(&mut parts, tenant);
            parts[i].1.rows.push(r.clone());
        }
    }
    if let Some(d) = &t.ddl {
        for tenant in DumpScope::tenants_named(&d.db, &d.sql) {
            let i = part(&mut parts, tenant);
            parts[i].1.ddl = Some(d.clone());
        }
    }
    if let Some(db) = &t.statement_dml
        && let Some(tenant) = DumpScope::tenant_of(db)
    {
        let i = part(&mut parts, tenant);
        parts[i].1.statement_dml = Some(db.clone());
    }
    match &t.xa {
        Some(XaMark::Prepare(x)) => {
            let seen: Vec<TenantId> = parts.iter().map(|(t, _)| *t).collect();
            xa.entry(x.clone()).or_default().extend(seen);
        }
        Some(XaMark::Commit(x) | XaMark::Rollback(x)) => {
            for held in xa.remove(x).unwrap_or_default() {
                part(&mut parts, held);
            }
        }
        None => {}
    }
    parts
}

/// What `kept` lets through of a tenant's part of a group (`None`:
/// nothing).
#[must_use]
pub fn view(part: &RawTxn, kept: &Kept) -> Option<RawTxn> {
    match kept {
        Kept::All => {
            let empty = part.rows.is_empty()
                && part.ddl.is_none()
                && part.statement_dml.is_none()
                && part.xa.is_none();
            (!empty).then(|| part.clone())
        }
        Kept::Tables(tables) => {
            let rows: Vec<_> = part
                .rows
                .iter()
                .filter(|r| {
                    tables
                        .iter()
                        .any(|p| DumpScope::table_matches(p, &r.table.table))
                        && !matches!(
                            r.table.schema.parse::<SchemaName>(),
                            Ok(SchemaName::Catalog { .. })
                        )
                })
                .cloned()
                .collect();
            if rows.is_empty() && part.xa.is_none() {
                return None;
            }
            let mut v = RawTxn::new(part.gtid, part.before.clone(), part.ts);
            v.stamps.clone_from(&part.stamps);
            v.apply = part.apply;
            v.xa.clone_from(&part.xa);
            v.rows = rows;
            Some(v)
        }
    }
}

fn value_bytes(v: &Value) -> usize {
    match v {
        Value::Decimal(s) => s.len(),
        Value::String(b) | Value::Bytes(b) | Value::Geometry(b) => b.len(),
        _ => 8,
    }
}

/// A group's size in memory, roughly.
fn approx_bytes(t: &RawTxn) -> usize {
    let images: usize = t
        .rows
        .iter()
        .map(|r| {
            [&r.before, &r.after]
                .into_iter()
                .flatten()
                .flat_map(|i| i.0.iter().flatten())
                .map(|v| 16 + value_bytes(v))
                .sum::<usize>()
                + 64
        })
        .sum();
    images + t.ddl.as_ref().map_or(0, |d| d.sql.len()) + 128
}

struct Sub {
    tenant: TenantId,
    kept: Kept,
    from: GtidPos,
    tx: mpsc::Sender<Item>,
    detached: Arc<AtomicBool>,
}

/// One group of the ring.
struct RingEntry {
    gtid: Gtid,
    after: GtidPos,
    parts: Vec<(TenantId, RawTxn)>,
    bytes: usize,
}

/// Whether the dump must be reopened at the hub's position, and why.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
enum Reopen {
    /// No.
    #[default]
    No,
    /// A tenant must be kept whole: the scope widens.
    Scope,
    /// The views' tables widened: the ring starts again too.
    Tables,
}

#[derive(Default)]
struct HubState {
    subs: HashMap<u64, Sub>,
    by_tenant: HashMap<TenantId, Vec<u64>>,
    next_id: u64,
    running: bool,
    /// The position after the last group read.
    pos: Option<GtidPos>,
    /// A subscription started behind `pos` while the hub was gathering:
    /// reopen here.
    rewind: Option<GtidPos>,
    /// The dump's scope must widen: reopen at `pos`.
    reopen: Reopen,
    /// When this run began.
    started: Option<Instant>,
    /// A group went to a subscription in this run.
    delivered: bool,
    /// The newest groups with kept rows, oldest first.
    ring: VecDeque<RingEntry>,
    ring_bytes: usize,
    /// The ring holds every group with kept rows after this position.
    ring_start: GtidPos,
    /// Live `Kept::All` subscriptions per tenant.
    whole: HashMap<TenantId, usize>,
    /// Tenants the open dump keeps whole, and since when.
    scope_whole: HashMap<TenantId, GtidPos>,
}

impl HubState {
    fn remove(&mut self, id: u64) {
        if let Some(s) = self.subs.remove(&id)
            && let Some(v) = self.by_tenant.get_mut(&s.tenant)
        {
            v.retain(|x| *x != id);
            if v.is_empty() {
                self.by_tenant.remove(&s.tenant);
            }
        }
    }

    /// The ring's groups after `from` for `tenant`, as `kept` sees them.
    fn backlog(&self, tenant: TenantId, kept: &Kept, from: &GtidPos) -> VecDeque<SourceItem> {
        self.ring
            .iter()
            .filter(|e| after(from, &e.gtid))
            .filter_map(|e| {
                let (_, part) = e.parts.iter().find(|(t, _)| *t == tenant)?;
                view(part, kept).map(SourceItem::Txn)
            })
            .collect()
    }

    /// Sends the hub's position `pos` to every subscription it has given
    /// everything up to it. Not while a move back is pending: a
    /// subscription registered behind `pos` for it hasn't had the groups
    /// before `pos` yet, and an idle position would make its tail skip
    /// them (found in r9feed's live run).
    fn idle_fanout(&mut self, pos: &GtidPos) {
        if self.rewind.is_some() || self.reopen != Reopen::No {
            return;
        }
        for s in self.subs.values_mut() {
            if pos.reached(&s.from)
                && s.tx
                    .try_send(Ok(SourceItem::Idle { pos: pos.clone() }))
                    .is_ok()
            {
                s.from = pos.clone();
            }
        }
    }

    /// Asks for a reopen (a stronger request is never weakened).
    fn want(&mut self, r: Reopen) {
        self.reopen = self.reopen.max(r);
    }

    fn reset_ring(&mut self, at: &GtidPos) {
        self.ring.clear();
        self.ring_bytes = 0;
        self.ring_start = at.clone();
    }

    fn register(
        &mut self,
        tenant: TenantId,
        kept: Kept,
        from: GtidPos,
        tx: mpsc::Sender<Item>,
        detached: Arc<AtomicBool>,
    ) {
        let id = self.next_id;
        self.next_id += 1;
        self.subs.insert(
            id,
            Sub {
                tenant,
                kept,
                from,
                tx,
                detached,
            },
        );
        self.by_tenant.entry(tenant).or_default().push(id);
    }
}

/// What the views share: their tables, the options.
struct FeedShared {
    inner: Arc<dyn Clusters>,
    tables: Mutex<BTreeSet<String>>,
    opts: SharedOpts,
}

struct Hub {
    cluster: ClusterId,
    feed: Arc<FeedShared>,
    state: Mutex<HubState>,
    wake: Notify,
    catchup: Arc<Semaphore>,
}

/// Counters of one hub (tests, metrics).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HubStats {
    /// Live subscriptions.
    pub subscriptions: usize,
    /// Tenants with a subscription.
    pub tenants: usize,
    /// Whether its dump task runs.
    pub running: bool,
    /// Groups in the ring.
    pub ring: usize,
    /// Catch-up dumps open now.
    pub catchups: usize,
}

/// How a subscription starts.
enum Attach {
    /// From the hub: the ring's groups first.
    Live {
        rx: mpsc::Receiver<Item>,
        backlog: VecDeque<SourceItem>,
        detached: Arc<AtomicBool>,
    },
    /// Its own dump until the ring covers it.
    CatchUp,
}

impl Hub {
    fn lock(&self) -> std::sync::MutexGuard<'_, HubState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn label(&self) -> String {
        self.cluster.to_string()
    }

    /// Starts listening at `from` if the hub can serve it (see the module
    /// docs); `first` allows moving a gathering hub's start back.
    fn attach(
        self: &Arc<Self>,
        tenant: TenantId,
        kept: &Kept,
        from: &GtidPos,
        first: bool,
    ) -> Attach {
        let opts = &self.feed.opts;
        let (tx, rx) = mpsc::channel(opts.sub_channel);
        let detached = Arc::new(AtomicBool::new(false));
        let whole = matches!(kept, Kept::All);
        let mut st = self.lock();
        if !st.running {
            st.running = true;
            st.pos = Some(from.clone());
            st.reset_ring(from);
            st.started = Some(Instant::now());
            st.delivered = false;
            st.rewind = None;
            st.reopen = Reopen::No;
            st.scope_whole.clear();
            st.register(
                tenant,
                kept.clone(),
                from.clone(),
                tx,
                Arc::clone(&detached),
            );
            drop(st);
            let hub = Arc::clone(self);
            let start = from.clone();
            tokio::spawn(async move { hub.run(start).await });
            return Attach::Live {
                rx,
                backlog: VecDeque::new(),
                detached,
            };
        }
        let pos = st.pos.clone().unwrap_or_default();
        if whole && !st.scope_whole.contains_key(&tenant) {
            // The dump doesn't keep this tenant whole yet: it reopens at
            // `pos` with it (the group in flight is read again).
            st.want(Reopen::Scope);
            self.wake.notify_one();
        }
        if from.reached(&pos) {
            st.register(
                tenant,
                kept.clone(),
                from.clone(),
                tx,
                Arc::clone(&detached),
            );
            return Attach::Live {
                rx,
                backlog: VecDeque::new(),
                detached,
            };
        }
        let since = if whole {
            st.scope_whole.get(&tenant).cloned()
        } else {
            Some(st.ring_start.clone())
        };
        let covered =
            from.reached(&st.ring_start) && since.as_ref().is_some_and(|s| from.reached(s));
        if covered {
            let backlog = st.backlog(tenant, kept, from);
            if backlog.len() <= opts.max_backlog {
                metrics::counter!("sprig_feed_resumes_total", "tier" => "ring").increment(1);
                st.register(tenant, kept.clone(), pos, tx, Arc::clone(&detached));
                return Attach::Live {
                    rx,
                    backlog,
                    detached,
                };
            }
        }
        let gathering = !st.delivered
            && st
                .started
                .is_some_and(|s| s.elapsed() < self.feed.opts.gather);
        if first && gathering {
            let r = match &st.rewind {
                Some(r) => older(r, from),
                None => older(&pos, from),
            };
            st.rewind = Some(r);
            self.wake.notify_one();
            st.register(
                tenant,
                kept.clone(),
                from.clone(),
                tx,
                Arc::clone(&detached),
            );
            return Attach::Live {
                rx,
                backlog: VecDeque::new(),
                detached,
            };
        }
        Attach::CatchUp
    }

    fn hold_whole(&self, tenant: TenantId) {
        *self.lock().whole.entry(tenant).or_default() += 1;
    }

    fn release_whole(&self, tenant: TenantId) {
        let mut st = self.lock();
        if let Some(n) = st.whole.get_mut(&tenant) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                st.whole.remove(&tenant);
            }
        }
    }

    fn stats(&self) -> HubStats {
        let st = self.lock();
        HubStats {
            subscriptions: st.subs.len(),
            tenants: st.by_tenant.len(),
            running: st.running,
            ring: st.ring.len(),
            catchups: self
                .feed
                .opts
                .max_catchup
                .max(1)
                .saturating_sub(self.catchup.available_permits()),
        }
    }

    /// Ends every subscription with `e`.
    fn fail_all(&self, e: &SprigError) {
        let subs: Vec<Sub> = {
            let mut st = self.lock();
            st.by_tenant.clear();
            st.subs.drain().map(|(_, s)| s).collect()
        };
        for s in subs {
            let _ = s.tx.try_send(Err(SprigError::new(
                e.code().unwrap_or(SprigCode::FeedHistoryLost),
                e.to_string(),
            )));
        }
    }

    /// The scope of the next dump: the views' tables and the tenants kept
    /// whole now. Updates `scope_whole` (new tenants since `pos`).
    fn scope(&self, st: &mut HubState, pos: &GtidPos, all_new: bool) -> DumpScope {
        let tables: Vec<String> = self
            .feed
            .tables
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .cloned()
            .collect();
        let tenants: BTreeSet<TenantId> = st.whole.keys().copied().collect();
        let mut since = HashMap::new();
        for t in &tenants {
            let s = match st.scope_whole.get(t) {
                Some(s) if !all_new => s.clone(),
                _ => pos.clone(),
            };
            since.insert(*t, s);
        }
        st.scope_whole = since;
        DumpScope {
            tables,
            tenants,
            only: None,
        }
    }

    #[allow(clippy::too_many_lines, clippy::cast_precision_loss)] // the loop of the module docs
    async fn run(self: Arc<Self>, mut pos: GtidPos) {
        let label = self.label();
        let mut xa: HashMap<String, HashSet<TenantId>> = HashMap::new();
        let mut src: Option<Reader> = None;
        let mut scope: Option<DumpScope> = None;
        let mut delay = Duration::from_millis(100);
        let mut last_idle = Instant::now();
        let mut empty_since: Option<Instant> = None;
        tracing::info!(cluster = %self.cluster, %pos, "shared feed reader starts");
        loop {
            // Stop when nobody listens for a while; rewind or widen.
            {
                let mut st = self.lock();
                st.subs.retain(|_, s| !s.tx.is_closed());
                let live: HashSet<u64> = st.subs.keys().copied().collect();
                st.by_tenant.retain(|_, v| {
                    v.retain(|x| live.contains(x));
                    !v.is_empty()
                });
                if st.subs.is_empty() {
                    let since = *empty_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= HUB_LINGER {
                        st.running = false;
                        st.pos = Some(pos.clone());
                        st.rewind = None;
                        st.reopen = Reopen::No;
                        st.reset_ring(&pos);
                        st.scope_whole.clear();
                        tracing::info!(cluster = %self.cluster, "shared feed reader stops (no subscriptions)");
                        return;
                    }
                } else {
                    empty_since = None;
                }
                if let Some(r) = st.rewind.take() {
                    tracing::info!(cluster = %self.cluster, from = %pos, to = %r, "shared feed reader moves its start back");
                    pos = older(&pos, &r);
                    st.pos = Some(pos.clone());
                    st.reset_ring(&pos);
                    src = None;
                    xa.clear();
                    let s = self.scope(&mut st, &pos, true);
                    scope = Some(s);
                    st.reopen = Reopen::No;
                } else if st.reopen != Reopen::No {
                    if std::mem::take(&mut st.reopen) == Reopen::Tables {
                        st.reset_ring(&pos);
                    }
                    src = None;
                    let s = self.scope(&mut st, &pos, false);
                    scope = Some(s);
                } else if scope.is_none() {
                    let s = self.scope(&mut st, &pos, true);
                    scope = Some(s);
                }
            }
            if src.is_none() {
                let sc = Arc::new(scope.clone().unwrap_or_default());
                match self.feed.inner.open_scope(&self.cluster, sc, &pos).await {
                    Ok(s) => {
                        src = Some(Reader::start(s));
                        delay = Duration::from_millis(100);
                        metrics::counter!("sprig_feed_shared_dumps_total", "cluster" => label.clone()).increment(1);
                    }
                    Err(e) if fatal(&e) => {
                        tracing::warn!(cluster = %self.cluster, error = %e, "shared feed reader: history lost");
                        self.fail_all(&e);
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(5));
                        continue;
                    }
                    Err(e) => {
                        tracing::debug!(cluster = %self.cluster, error = %e, "shared feed reader: open failed; retrying");
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(5));
                        continue;
                    }
                }
            }
            let Some(s) = src.as_mut() else { continue };
            // A rewind, a wider scope or the linger check may end the
            // wait; `Reader::next` is cancel-safe, and a reopened dump
            // starts again from `pos`.
            let item = tokio::select! {
                () = self.wake.notified() => continue,
                () = tokio::time::sleep(IDLE_FANOUT * 5) => {
                    // Quiet cluster: keep the subscriptions' positions moving.
                    Ok(SourceItem::Idle { pos: pos.clone() })
                }
                i = s.next() => i,
            };
            match item {
                Ok(SourceItem::Txn(t)) => {
                    let mut st = self.lock();
                    if st.rewind.is_some() || st.reopen != Reopen::No {
                        // Read with the old scope or start: read it again.
                        drop(st);
                        src = None;
                        continue;
                    }
                    let parts = split(&t, &mut xa);
                    let mut detach: Vec<u64> = Vec::new();
                    let mut gone: Vec<u64> = Vec::new();
                    let mut sent = false;
                    {
                        let HubState {
                            subs, by_tenant, ..
                        } = &mut *st;
                        for (tenant, part) in &parts {
                            let Some(ids) = by_tenant.get(tenant) else {
                                continue;
                            };
                            for id in ids {
                                let Some(s) = subs.get_mut(id) else { continue };
                                if !after(&s.from, &t.gtid) {
                                    continue;
                                }
                                let Some(v) = view(part, &s.kept) else {
                                    s.from.update(t.gtid);
                                    continue;
                                };
                                match s.tx.try_send(Ok(SourceItem::Txn(v))) {
                                    Ok(()) => {
                                        // Delivered: a rewind won't send it again.
                                        s.from.update(t.gtid);
                                        sent = true;
                                    }
                                    Err(mpsc::error::TrySendError::Full(_)) => detach.push(*id),
                                    Err(mpsc::error::TrySendError::Closed(_)) => gone.push(*id),
                                }
                            }
                        }
                    }
                    for id in detach {
                        if let Some(s) = st.subs.get(&id) {
                            s.detached.store(true, Ordering::Release);
                            tracing::debug!(cluster = %self.cluster, tenant = %s.tenant, "shared feed reader: a slow subscription is detached");
                        }
                        st.remove(id);
                        metrics::counter!("sprig_feed_shared_detached_total", "cluster" => label.clone()).increment(1);
                    }
                    for id in gone {
                        st.remove(id);
                    }
                    st.delivered |= sent;
                    let after_pos = t.after();
                    if !parts.is_empty() {
                        let bytes = parts.iter().map(|(_, p)| approx_bytes(p)).sum();
                        st.ring_bytes += bytes;
                        st.ring.push_back(RingEntry {
                            gtid: t.gtid,
                            after: after_pos.clone(),
                            parts,
                            bytes,
                        });
                        let (max_groups, max_bytes) =
                            (self.feed.opts.ring_groups, self.feed.opts.ring_bytes);
                        while st.ring.len() > max_groups || st.ring_bytes > max_bytes {
                            let Some(e) = st.ring.pop_front() else { break };
                            st.ring_bytes -= e.bytes;
                            st.ring_start = e.after;
                        }
                    }
                    pos = after_pos;
                    st.pos = Some(pos.clone());
                    drop(st);
                    metrics::counter!("sprig_feed_shared_groups_total", "cluster" => label.clone())
                        .increment(1);
                }
                Ok(SourceItem::Idle { pos: p }) => {
                    pos = p;
                    self.lock().pos = Some(pos.clone());
                }
                Err(e) if fatal(&e) => {
                    tracing::warn!(cluster = %self.cluster, error = %e, "shared feed reader: history lost");
                    self.fail_all(&e);
                    src = None;
                    continue;
                }
                Err(e) => {
                    tracing::debug!(cluster = %self.cluster, error = %e, "shared feed reader: stream broke; reconnecting");
                    src = None;
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(5));
                    continue;
                }
            }
            if last_idle.elapsed() >= IDLE_FANOUT {
                last_idle = Instant::now();
                let mut st = self.lock();
                st.idle_fanout(&pos);
                metrics::gauge!("sprig_feed_shared_subscriptions", "cluster" => label.clone())
                    .set(st.subs.len() as f64);
                metrics::gauge!("sprig_feed_ring_groups", "cluster" => label.clone())
                    .set(st.ring.len() as f64);
            }
        }
    }
}

/// A dump read by a task of its own, its items through a channel: a
/// `select!` over [`Reader::next`] never drops an event half-read (a
/// binlog read isn't cancel-safe mid-packet). Dropping it stops the task.
struct Reader {
    rx: mpsc::Receiver<Item>,
    task: tokio::task::JoinHandle<()>,
}

impl Reader {
    /// Items read ahead at most.
    const AHEAD: usize = 1_024;

    fn start(mut src: Box<dyn TxnSource>) -> Self {
        let (tx, rx) = mpsc::channel(Self::AHEAD);
        let task = tokio::spawn(async move {
            loop {
                let item = src.next().await;
                let end = item.is_err();
                if tx.send(item).await.is_err() || end {
                    return;
                }
            }
        });
        Self { rx, task }
    }

    /// The next item (cancel-safe).
    async fn next(&mut self) -> Item {
        self.rx.recv().await.unwrap_or_else(|| {
            Err(SprigError::new(
                SprigCode::BinlogDisconnected,
                "the shared feed reader's dump task ended",
            ))
        })
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Errors that end every subscription (the history is gone; a restart
/// from the same position can't help).
fn fatal(e: &SprigError) -> bool {
    matches!(
        e.kind,
        ErrorKind::Sprig(SprigCode::FeedHistoryLost | SprigCode::FeedSnapshotRequired)
    )
}

/// A catch-up dump and its slot.
struct CatchUp {
    src: Reader,
    _permit: OwnedSemaphorePermit,
    cluster: String,
    checked: Instant,
}

impl Drop for CatchUp {
    fn drop(&mut self) {
        metrics::gauge!("sprig_feed_catchup_readers", "cluster" => self.cluster.clone())
            .decrement(1.0);
    }
}

enum Mode {
    Live {
        rx: mpsc::Receiver<Item>,
        backlog: VecDeque<SourceItem>,
        detached: Arc<AtomicBool>,
    },
    CatchUp(CatchUp),
    /// Detached, or not served yet: attach again.
    Out,
}

/// One tenant's view of a hub: the hub's groups for it, or its own
/// catch-up dump while it is behind the ring.
struct SubSource {
    hub: Arc<Hub>,
    tenant: TenantId,
    kept: Kept,
    /// The position after the last item returned.
    pos: GtidPos,
    mode: Mode,
}

impl Drop for SubSource {
    fn drop(&mut self) {
        if matches!(self.kept, Kept::All) {
            self.hub.release_whole(self.tenant);
        }
    }
}

impl SubSource {
    fn advance(&mut self, i: &SourceItem) {
        self.pos = match i {
            SourceItem::Txn(t) => newer(&self.pos, &t.after()),
            SourceItem::Idle { pos } => newer(&self.pos, pos),
        };
    }

    fn scope(&self) -> DumpScope {
        match &self.kept {
            Kept::All => DumpScope {
                tables: Vec::new(),
                tenants: [self.tenant].into_iter().collect(),
                only: Some(self.tenant),
            },
            Kept::Tables(t) => DumpScope {
                tables: t.to_vec(),
                tenants: BTreeSet::new(),
                only: Some(self.tenant),
            },
        }
    }

    fn live(&mut self, a: Attach) -> bool {
        match a {
            Attach::Live {
                rx,
                backlog,
                detached,
            } => {
                self.mode = Mode::Live {
                    rx,
                    backlog,
                    detached,
                };
                true
            }
            Attach::CatchUp => false,
        }
    }

    /// Opens a catch-up dump, once a slot is free.
    async fn catch_up(&mut self) -> Result<()> {
        let permit = Arc::clone(&self.hub.catchup)
            .acquire_owned()
            .await
            .map_err(|_| SprigError::internal("the shared reader's catch-up slots are gone"))?;
        // Another catch-up may have moved the ring meanwhile.
        let a = self.hub.attach(self.tenant, &self.kept, &self.pos, false);
        if self.live(a) {
            return Ok(());
        }
        let src = self
            .hub
            .feed
            .inner
            .open_scope(&self.hub.cluster, Arc::new(self.scope()), &self.pos)
            .await?;
        let cluster = self.hub.label();
        metrics::gauge!("sprig_feed_catchup_readers", "cluster" => cluster.clone()).increment(1.0);
        metrics::counter!("sprig_feed_resumes_total", "tier" => "binlog").increment(1);
        tracing::debug!(cluster = %self.hub.cluster, tenant = %self.tenant, pos = %self.pos, "shared feed reader: catch-up dump");
        self.mode = Mode::CatchUp(CatchUp {
            src: Reader::start(src),
            _permit: permit,
            cluster,
            checked: Instant::now(),
        });
        Ok(())
    }
}

/// How often a catch-up dump asks whether the hub can take over.
const REJOIN_EVERY: Duration = Duration::from_millis(100);

#[async_trait::async_trait]
impl TxnSource for SubSource {
    async fn next(&mut self) -> Result<SourceItem> {
        loop {
            match &mut self.mode {
                Mode::Live {
                    rx,
                    backlog,
                    detached,
                } => {
                    if let Some(i) = backlog.pop_front() {
                        self.advance(&i);
                        return Ok(i);
                    }
                    match rx.recv().await {
                        Some(Ok(i)) => {
                            self.advance(&i);
                            return Ok(i);
                        }
                        Some(Err(e)) => return Err(e),
                        None if detached.load(Ordering::Acquire) => self.mode = Mode::Out,
                        None => {
                            return Err(SprigError::new(
                                SprigCode::BinlogDisconnected,
                                "the shared feed reader ended this subscription",
                            ));
                        }
                    }
                }
                Mode::Out => {
                    let a = self.hub.attach(self.tenant, &self.kept, &self.pos, false);
                    if !self.live(a) {
                        self.catch_up().await?;
                    }
                }
                Mode::CatchUp(c) => {
                    let item = c.src.next().await?;
                    let rejoin = c.checked.elapsed() >= REJOIN_EVERY;
                    if rejoin {
                        c.checked = Instant::now();
                    }
                    let out = match item {
                        SourceItem::Txn(t) => {
                            // The dump holds this tenant's rows only.
                            let v = view(&t, &self.kept);
                            self.pos = newer(&self.pos, &t.after());
                            v.map(SourceItem::Txn)
                        }
                        SourceItem::Idle { pos } => {
                            let i = SourceItem::Idle { pos };
                            self.advance(&i);
                            Some(i)
                        }
                    };
                    if rejoin {
                        let a = self.hub.attach(self.tenant, &self.kept, &self.pos, false);
                        if self.live(a) {
                            tracing::debug!(cluster = %self.hub.cluster, tenant = %self.tenant, pos = %self.pos, "shared feed reader: caught up");
                        }
                    }
                    if let Some(o) = out {
                        return Ok(o);
                    }
                }
            }
        }
    }
}

/// One shared reader per process: a hub per cluster, seen through views
/// (see the module docs).
pub struct SharedFeed {
    shared: Arc<FeedShared>,
    hubs: Mutex<HashMap<ClusterId, Arc<Hub>>>,
}

impl std::fmt::Debug for SharedFeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedFeed").finish_non_exhaustive()
    }
}

impl SharedFeed {
    /// A shared reader over `inner`'s cluster-wide sources.
    #[must_use]
    pub fn new(inner: Arc<dyn Clusters>, opts: SharedOpts) -> Arc<Self> {
        Arc::new(Self {
            shared: Arc::new(FeedShared {
                inner,
                tables: Mutex::new(BTreeSet::new()),
                opts,
            }),
            hubs: Mutex::new(HashMap::new()),
        })
    }

    /// A view keeping `kept` of its tenants. New tables widen every
    /// running hub's dump.
    #[must_use]
    pub fn view(self: &Arc<Self>, kept: Kept) -> Arc<SharedClusters> {
        if let Kept::Tables(t) = &kept {
            let mut tables = self
                .shared
                .tables
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let mut wider = false;
            for x in t.iter() {
                wider |= tables.insert(x.clone());
            }
            drop(tables);
            if wider {
                for h in self
                    .hubs
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .values()
                {
                    let mut st = h.lock();
                    if st.running {
                        st.want(Reopen::Tables);
                        h.wake.notify_one();
                    }
                }
            }
        }
        Arc::new(SharedClusters {
            feed: Arc::clone(self),
            kept,
        })
    }

    fn hub(&self, cluster: &ClusterId) -> Arc<Hub> {
        let mut hubs = self.hubs.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(hubs.entry(cluster.clone()).or_insert_with(|| {
            Arc::new(Hub {
                cluster: cluster.clone(),
                feed: Arc::clone(&self.shared),
                state: Mutex::new(HubState::default()),
                wake: Notify::new(),
                catchup: Arc::new(Semaphore::new(self.shared.opts.max_catchup.max(1))),
            })
        }))
    }

    /// A hub's counters (`None`: never opened).
    #[must_use]
    pub fn stats(&self, cluster: &ClusterId) -> Option<HubStats> {
        let hubs = self.hubs.lock().unwrap_or_else(PoisonError::into_inner);
        hubs.get(cluster).map(|h| h.stats())
    }
}

/// A view of a [`SharedFeed`]: a [`Clusters`] whose tenant sources share
/// one dump per cluster (see the module docs).
pub struct SharedClusters {
    feed: Arc<SharedFeed>,
    kept: Kept,
}

impl std::fmt::Debug for SharedClusters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedClusters")
            .field("kept", &self.kept)
            .finish_non_exhaustive()
    }
}

impl SharedClusters {
    /// A reader of its own over `inner`, keeping rows of `tables` (R9,
    /// r9cluster's form: one view).
    #[must_use]
    pub fn new(inner: Arc<dyn Clusters>, tables: &[&str]) -> Self {
        let feed = SharedFeed::new(inner, SharedOpts::default());
        let v = feed.view(Kept::Tables(
            tables.iter().map(|t| (*t).to_owned()).collect(),
        ));
        Self {
            feed,
            kept: v.kept.clone(),
        }
    }

    /// The reader this view shares.
    #[must_use]
    pub fn feed(&self) -> &Arc<SharedFeed> {
        &self.feed
    }

    /// A hub's counters (`None`: never opened).
    #[must_use]
    pub fn stats(&self, cluster: &ClusterId) -> Option<HubStats> {
        self.feed.stats(cluster)
    }
}

#[async_trait::async_trait]
impl Clusters for SharedClusters {
    async fn open(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>> {
        let hub = self.feed.hub(cluster);
        if matches!(self.kept, Kept::All) {
            hub.hold_whole(tenant);
        }
        let mut s = SubSource {
            hub: Arc::clone(&hub),
            tenant,
            kept: self.kept.clone(),
            pos: from.clone(),
            mode: Mode::Out,
        };
        let a = hub.attach(tenant, &self.kept, from, true);
        s.live(a);
        Ok(Box::new(s))
    }

    async fn open_scope(
        &self,
        cluster: &ClusterId,
        scope: Arc<DumpScope>,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>> {
        self.feed
            .shared
            .inner
            .open_scope(cluster, scope, from)
            .await
    }

    async fn position(&self, cluster: &ClusterId) -> Result<GtidPos> {
        self.feed.shared.inner.position(cluster).await
    }
}

#[cfg(test)]
mod tests;
