//! The shared per-cluster reader for internal tables (chapter 38, "The
//! cluster reader"; chapter 50, "Publishing inside a database transaction";
//! R9, r9cluster).
//!
//! [`SharedClusters`] is a [`Clusters`] whose [`Clusters::open`] does not
//! start a binlog dump per tenant: every tenant's source on a cluster is a
//! subscription to one **hub**, which reads that cluster's binlog once
//! ([`Clusters::open_tables`]) keeping only rows of a few internal tables
//! (the queue forwarder's `__sprig_qo` and `__sprig_xo`), and splits each
//! committed group by tenant. [`crate::TenantTail`] runs on top unchanged:
//! moves, resume tokens, XA branches and applier filtering work as with a
//! dump of its own.
//!
//! ```text
//! hub(cluster), one task while it has subscribers:
//!   read the cluster's binlog after its position, internal tables only
//!   each committed group: rows split by the tenant of their schema; an
//!     XA commit or rollback goes to the tenants its prepare had rows for
//!   each tenant with rows: to each of its subscriptions that started
//!     before the group (backpressure: the hub waits for a full channel)
//!   at most every second: the position to every subscription (idle)
//!   a subscription that starts behind the hub's position rewinds the
//!     hub to the older position; the others skip what they already had
//!   a broken stream reconnects from the hub's position; a lost history
//!     (39800) ends every subscription with that error
//!   no subscriptions for 30 s: the hub stops and closes its dump
//! ```
//!
//! One dump per cluster and process, however many tenants: the
//! 1,000-tenant run of R9 item 84 had 25 dumps (one per tenant with
//! queues) fill a node's connections.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use sprig_binlog::{Gtid, GtidPos};
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use tokio::sync::{Notify, mpsc};
use tokio::time::Instant;

use crate::source::{Clusters, RawRow, RawTxn, SourceItem, TxnSource, XaMark};

/// Items a subscription buffers before the hub waits for it.
pub const SUB_CHANNEL: usize = 256;
/// Idle positions go to every subscription at most this often.
pub const IDLE_FANOUT: Duration = Duration::from_secs(1);
/// A hub without subscriptions stops after this long.
pub const HUB_LINGER: Duration = Duration::from_secs(30);

type Item = Result<SourceItem>;

/// Whether group `g` lies after `from` (a domain `from` lacks: yes).
fn after(from: &GtidPos, g: &Gtid) -> bool {
    from.get(g.domain).is_none_or(|x| g.seq > x.seq)
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

/// Whether `a` lies behind `b` in a domain both name.
fn behind(a: &GtidPos, b: &GtidPos) -> bool {
    a.iter()
        .any(|g| b.get(g.domain).is_some_and(|x| g.seq < x.seq))
}

/// The tenant whose shard or reference schema `schema` is.
fn tenant_of(schema: &str) -> Option<TenantId> {
    match schema.parse::<SchemaName>().ok()? {
        SchemaName::Catalog { .. } => None,
        s => Some(s.tenant()),
    }
}

/// A group split by tenant: each tenant's rows, in binlog order.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn split(t: &RawTxn, xa: &mut HashMap<String, HashSet<TenantId>>) -> Vec<(TenantId, RawTxn)> {
    let mut rows: Vec<(TenantId, Vec<RawRow>)> = Vec::new();
    for r in &t.rows {
        let Some(tenant) = tenant_of(&r.table.schema) else {
            continue;
        };
        match rows.iter_mut().find(|(x, _)| *x == tenant) {
            Some((_, v)) => v.push(r.clone()),
            None => rows.push((tenant, vec![r.clone()])),
        }
    }
    let mut tenants: Vec<TenantId> = rows.iter().map(|(t, _)| *t).collect();
    match &t.xa {
        Some(XaMark::Prepare(x)) => {
            xa.entry(x.clone())
                .or_default()
                .extend(tenants.iter().copied());
        }
        Some(XaMark::Commit(x) | XaMark::Rollback(x)) => {
            for held in xa.remove(x).unwrap_or_default() {
                if !tenants.contains(&held) {
                    tenants.push(held);
                }
            }
        }
        None => {}
    }
    tenants
        .into_iter()
        .map(|tenant| {
            let mine = rows
                .iter()
                .find(|(x, _)| *x == tenant)
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
            let mut sub = RawTxn::new(t.gtid, t.before.clone(), t.ts);
            sub.stamps.clone_from(&t.stamps);
            sub.rows = mine;
            sub.apply = t.apply;
            sub.xa.clone_from(&t.xa);
            (tenant, sub)
        })
        .collect()
}

struct Sub {
    tenant: TenantId,
    from: GtidPos,
    tx: mpsc::Sender<Item>,
}

#[derive(Default)]
struct HubState {
    subs: HashMap<u64, Sub>,
    by_tenant: HashMap<TenantId, Vec<u64>>,
    next_id: u64,
    running: bool,
    /// The position after the last group read.
    pos: Option<GtidPos>,
    /// A subscription started behind `pos`: reopen here.
    rewind: Option<GtidPos>,
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
}

struct Hub {
    cluster: ClusterId,
    inner: Arc<dyn Clusters>,
    tables: Arc<[String]>,
    state: Mutex<HubState>,
    rewind: Notify,
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
}

impl Hub {
    fn lock(&self) -> std::sync::MutexGuard<'_, HubState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn subscribe(self: &Arc<Self>, tenant: TenantId, from: &GtidPos) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(SUB_CHANNEL);
        let start = {
            let mut st = self.lock();
            let id = st.next_id;
            st.next_id += 1;
            st.subs.insert(
                id,
                Sub {
                    tenant,
                    from: from.clone(),
                    tx,
                },
            );
            st.by_tenant.entry(tenant).or_default().push(id);
            if st.running {
                if let Some(p) = &st.pos
                    && behind(from, p)
                {
                    let r = match &st.rewind {
                        Some(r) => older(r, from),
                        None => older(p, from),
                    };
                    st.rewind = Some(r);
                    self.rewind.notify_one();
                }
                None
            } else {
                st.running = true;
                let p = match st.pos.take() {
                    Some(p) => older(&p, from),
                    None => from.clone(),
                };
                st.pos = Some(p.clone());
                Some(p)
            }
        };
        if let Some(p) = start {
            let hub = Arc::clone(self);
            tokio::spawn(async move { hub.run(p).await });
        }
        rx
    }

    fn stats(&self) -> HubStats {
        let st = self.lock();
        HubStats {
            subscriptions: st.subs.len(),
            tenants: st.by_tenant.len(),
            running: st.running,
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

    #[allow(clippy::too_many_lines)] // the loop of the module docs
    async fn run(self: Arc<Self>, mut pos: GtidPos) {
        let mut xa: HashMap<String, HashSet<TenantId>> = HashMap::new();
        let mut src: Option<Box<dyn TxnSource>> = None;
        let mut delay = Duration::from_millis(100);
        let mut last_idle = Instant::now();
        let mut empty_since: Option<Instant> = None;
        tracing::info!(cluster = %self.cluster, %pos, "shared feed reader starts");
        loop {
            // Stop when nobody listens for a while.
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
                        tracing::info!(cluster = %self.cluster, "shared feed reader stops (no subscriptions)");
                        return;
                    }
                } else {
                    empty_since = None;
                }
                if let Some(r) = st.rewind.take() {
                    tracing::info!(cluster = %self.cluster, from = %pos, to = %r, "shared feed reader rewinds");
                    pos = older(&pos, &r);
                    st.pos = Some(pos.clone());
                    src = None;
                    xa.clear();
                }
            }
            if src.is_none() {
                match self
                    .inner
                    .open_tables(&self.cluster, Arc::clone(&self.tables), &pos)
                    .await
                {
                    Ok(s) => {
                        src = Some(s);
                        delay = Duration::from_millis(100);
                        metrics::counter!("sprig_feed_shared_dumps_total", "cluster" => self.cluster.to_string()).increment(1);
                    }
                    Err(e) if fatal(&e) => {
                        tracing::warn!(cluster = %self.cluster, error = %e, "shared feed reader: history lost");
                        self.fail_all(&e);
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
            // A rewind or the linger check may end the wait; dropping the
            // source half-read is fine, it is reopened from `pos`.
            let item = tokio::select! {
                () = self.rewind.notified() => continue,
                () = tokio::time::sleep(IDLE_FANOUT * 5) => {
                    // Quiet cluster: keep the subscriptions' positions moving.
                    Ok(SourceItem::Idle { pos: pos.clone() })
                }
                i = s.next() => i,
            };
            match item {
                Ok(SourceItem::Txn(t)) => {
                    let after_pos = t.after();
                    for (tenant, sub) in split(&t, &mut xa) {
                        let targets: Vec<(u64, mpsc::Sender<Item>)> = {
                            let st = self.lock();
                            st.by_tenant
                                .get(&tenant)
                                .into_iter()
                                .flatten()
                                .filter_map(|id| {
                                    let s = st.subs.get(id)?;
                                    after(&s.from, &t.gtid).then(|| (*id, s.tx.clone()))
                                })
                                .collect()
                        };
                        for (id, tx) in targets {
                            if tx.send(Ok(SourceItem::Txn(sub.clone()))).await.is_err() {
                                self.lock().remove(id);
                            } else if let Some(s) = self.lock().subs.get_mut(&id) {
                                // Delivered: a rewind won't send it again.
                                s.from.update(t.gtid);
                            }
                        }
                    }
                    pos = after_pos;
                    self.lock().pos = Some(pos.clone());
                    metrics::counter!("sprig_feed_shared_groups_total", "cluster" => self.cluster.to_string()).increment(1);
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
                for s in st.subs.values_mut() {
                    if pos.reached(&s.from)
                        && s.tx
                            .try_send(Ok(SourceItem::Idle { pos: pos.clone() }))
                            .is_ok()
                    {
                        s.from = pos.clone();
                    }
                }
            }
        }
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

/// One tenant's view of a hub.
struct SubSource {
    rx: mpsc::Receiver<Item>,
}

#[async_trait::async_trait]
impl TxnSource for SubSource {
    async fn next(&mut self) -> Result<SourceItem> {
        match self.rx.recv().await {
            Some(i) => i,
            None => Err(SprigError::new(
                SprigCode::BinlogDisconnected,
                "the shared feed reader ended this subscription",
            )),
        }
    }
}

/// [`Clusters`] whose tenant sources share one dump per cluster (see the
/// module docs). Only rows of `tables` reach the tenants' tails.
pub struct SharedClusters {
    inner: Arc<dyn Clusters>,
    tables: Arc<[String]>,
    hubs: Mutex<HashMap<ClusterId, Arc<Hub>>>,
}

impl std::fmt::Debug for SharedClusters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedClusters")
            .field("tables", &self.tables)
            .finish_non_exhaustive()
    }
}

impl SharedClusters {
    /// Shares `inner`'s cluster-wide sources for rows of `tables`.
    #[must_use]
    pub fn new(inner: Arc<dyn Clusters>, tables: &[&str]) -> Self {
        Self {
            inner,
            tables: tables.iter().map(|t| (*t).to_owned()).collect(),
            hubs: Mutex::new(HashMap::new()),
        }
    }

    fn hub(&self, cluster: &ClusterId) -> Arc<Hub> {
        let mut hubs = self.hubs.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(hubs.entry(cluster.clone()).or_insert_with(|| {
            Arc::new(Hub {
                cluster: cluster.clone(),
                inner: Arc::clone(&self.inner),
                tables: Arc::clone(&self.tables),
                state: Mutex::new(HubState::default()),
                rewind: Notify::new(),
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

#[async_trait::async_trait]
impl Clusters for SharedClusters {
    async fn open(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>> {
        let rx = self.hub(cluster).subscribe(tenant, from);
        Ok(Box::new(SubSource { rx }))
    }

    async fn open_tables(
        &self,
        cluster: &ClusterId,
        tables: Arc<[String]>,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>> {
        self.inner.open_tables(cluster, tables, from).await
    }

    async fn position(&self, cluster: &ClusterId) -> Result<GtidPos> {
        self.inner.position(cluster).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::many_single_char_names)]

    use std::sync::atomic::{AtomicUsize, Ordering};

    use sprig_binlog::{RowImage, Value};

    use super::*;
    use crate::source::{RowKind, TableShape};

    fn g(seq: u64) -> Gtid {
        Gtid {
            domain: 0,
            server: 1,
            seq,
        }
    }

    fn row(schema: &str) -> RawRow {
        RawRow {
            table: Arc::new(TableShape {
                schema: schema.into(),
                table: "__sprig_qo".into(),
                columns: vec!["out_id".into()],
                key: vec![0],
            }),
            kind: RowKind::Insert,
            before: None,
            after: Some(RowImage(vec![Some(Value::Int(1))])),
            hint: None,
        }
    }

    fn txn(seq: u64, schemas: &[&str]) -> RawTxn {
        let mut t = RawTxn::new(g(seq), GtidPos::from_gtids([g(seq - 1)]), 0);
        t.rows = schemas.iter().map(|s| row(s)).collect();
        t
    }

    /// A cluster whose binlog is a fixed list of groups; counts dumps.
    struct Fake {
        groups: Vec<RawTxn>,
        opens: Arc<AtomicUsize>,
    }

    struct FakeSrc {
        groups: Vec<RawTxn>,
    }

    #[async_trait::async_trait]
    impl TxnSource for FakeSrc {
        async fn next(&mut self) -> Result<SourceItem> {
            if self.groups.is_empty() {
                std::future::pending::<()>().await;
            }
            Ok(SourceItem::Txn(self.groups.remove(0)))
        }
    }

    #[async_trait::async_trait]
    impl Clusters for Fake {
        async fn open(
            &self,
            _: &ClusterId,
            _: TenantId,
            _: &GtidPos,
        ) -> Result<Box<dyn TxnSource>> {
            unreachable!("the shared reader opens cluster-wide sources only")
        }

        async fn open_tables(
            &self,
            _: &ClusterId,
            _: Arc<[String]>,
            from: &GtidPos,
        ) -> Result<Box<dyn TxnSource>> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            let groups = self
                .groups
                .iter()
                .filter(|t| after(from, &t.gtid))
                .cloned()
                .collect();
            Ok(Box::new(FakeSrc { groups }))
        }

        async fn position(&self, _: &ClusterId) -> Result<GtidPos> {
            Ok(GtidPos::from_gtids([g(0)]))
        }
    }

    async fn take(src: &mut Box<dyn TxnSource>, n: usize) -> Vec<(u64, usize)> {
        let mut out = Vec::new();
        while out.len() < n {
            match tokio::time::timeout(Duration::from_secs(5), src.next())
                .await
                .unwrap()
                .unwrap()
            {
                SourceItem::Txn(t) => out.push((t.gtid.seq, t.rows.len())),
                SourceItem::Idle { .. } => {}
            }
        }
        out
    }

    #[test]
    fn groups_split_by_tenant_and_xa_follows_its_prepare() {
        let mut xa = HashMap::new();
        let t = txn(
            5,
            &["t7_d1_g1_s1", "t8_d2_g1_s1", "t7_d1_g1_s2", "t9_sprig"],
        );
        let parts = split(&t, &mut xa);
        let shape: Vec<(u64, usize)> = parts.iter().map(|(t, x)| (t.0, x.rows.len())).collect();
        assert_eq!(shape, vec![(7, 2), (8, 1)]);
        let mut p = txn(6, &["t7_d1_g1_s1"]);
        p.xa = Some(XaMark::Prepare("x1".into()));
        assert_eq!(split(&p, &mut xa).len(), 1);
        let mut c = txn(7, &[]);
        c.xa = Some(XaMark::Commit("x1".into()));
        let parts = split(&c, &mut xa);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].0, TenantId(7));
        assert!(xa.is_empty());
        let a = GtidPos::from_gtids([g(5)]);
        let b = GtidPos::from_gtids([g(3)]);
        assert_eq!(older(&a, &b), b);
        assert!(behind(&b, &a) && !behind(&a, &b));
    }

    #[tokio::test]
    async fn many_tenants_share_one_dump_and_late_starters_rewind() {
        let opens = Arc::new(AtomicUsize::new(0));
        let fake = Fake {
            groups: vec![
                txn(1, &["t1_d1_g1_s1"]),
                txn(2, &["t2_d1_g1_s1", "t1_d1_g1_s1"]),
                txn(3, &["t2_d1_g1_s1"]),
                txn(4, &["t1_d1_g1_s1"]),
            ],
            opens: Arc::clone(&opens),
        };
        let shared = SharedClusters::new(Arc::new(fake), &["__sprig_qo"]);
        let c = ClusterId::new("dc-a-c1").unwrap();
        // Tenant 1 from group 1 on; tenant 2 joins later from group 2 on.
        let mut s1 = shared
            .open(&c, TenantId(1), &GtidPos::from_gtids([g(0)]))
            .await
            .unwrap();
        assert_eq!(take(&mut s1, 3).await, vec![(1, 1), (2, 1), (4, 1)]);
        let mut s2 = shared
            .open(&c, TenantId(2), &GtidPos::from_gtids([g(1)]))
            .await
            .unwrap();
        // Tenant 2 started behind the hub: it rewinds, tenant 2 gets 2 and
        // 3, and tenant 1 sees nothing twice.
        assert_eq!(take(&mut s2, 2).await, vec![(2, 1), (3, 1)]);
        let quiet = tokio::time::timeout(Duration::from_millis(300), async {
            loop {
                if let SourceItem::Txn(t) = s1.next().await.unwrap() {
                    return t.gtid.seq;
                }
            }
        })
        .await;
        assert!(quiet.is_err(), "tenant 1 got a group twice: {quiet:?}");
        assert_eq!(
            opens.load(Ordering::SeqCst),
            2,
            "one dump, reopened once to rewind"
        );
        let st = shared.stats(&c).unwrap();
        assert_eq!((st.subscriptions, st.tenants, st.running), (2, 2, true));
        // 1,000 more tenants add subscriptions, not dumps.
        let mut subs = Vec::new();
        for t in 10..1_010 {
            subs.push(
                shared
                    .open(&c, TenantId(t), &GtidPos::from_gtids([g(4)]))
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(shared.stats(&c).unwrap().subscriptions, 1_002);
        assert_eq!(opens.load(Ordering::SeqCst), 2);
        drop(subs);
    }
}
