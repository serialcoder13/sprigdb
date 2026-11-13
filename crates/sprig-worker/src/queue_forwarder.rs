//! `queue_forwarder` (chapter 50, "Publishing inside a database
//! transaction", steps 7–8; chapter 42, "Inside a database transaction";
//! R9, r9fwd; per cluster since r9cluster): one long-running job per
//! cluster (`spec: {"cluster": "<id>"}`) that forwards the transactional
//! publishes of every tenant on the cluster from the change feed, in
//! commit order across each tenant's transactions.
//!
//! ```text
//! serve(cluster):
//!   every 5 s, and at once when a forwarder subscription is written (a
//!   watch; r9fanin): the tenants with an active queue_forwarder subscription
//!     whose map points at this cluster (a running tenant's placement is
//!     read again each minute) → start a tenant task for each new one,
//!     stop the tasks of tenants gone or moved away
//!   every tenant task reads its feed through the worker's shared reader
//!     (sprig_feed::shared): one binlog dump per cluster, whatever the
//!     number of tenants, keeping only __sprig_qo and __sprig_xo rows
//!   report progress every 2 s (tenants, rows); losing the job stops all
//!   write queue-forwarder-beats/<cluster> every 2 s, removed when stopping:
//!     the gateways' fallback leaves rows to the feed while it is fresh
//!     (bugfix-42)
//!
//! tenant task(tenant):
//!   start after the subscription's checkpoint (feed/checkpoints/<tenant>/
//!     queue_forwarder), or at the tenant's position now
//!   tail the tenant's feed (chapter 38) keeping __sprig_qo and __sprig_xo
//!   per committed transaction, in commit order: its outbox inserts
//!     (__sprig_qo rows of kind 'publish'; every __sprig_xo row)
//!   take what the feed has (at most 1,000 rows), then per run of rows of
//!     one outbox table in one shard schema, in order: a gateway forwards
//!     the run (LogicPeer.SinkSql, op outbox_forward) and deletes it; the
//!     next run is sent only after this one's destinations confirmed
//!   acknowledge the position after the last transaction forwarded (at
//!     most once a second after rows were forwarded, at most every 30 s
//!     otherwise, and when stopping)
//! ```
//!
//! **Exactly once.** A run's rows are deleted only after every destination
//! confirmed, and each enqueue writes the forward ledger `__sprig_qf` in
//! the queue owner's batch: a run forwarded again after a crash (the
//! checkpoint is behind) finds its rows gone, or finds them in the ledger.
//! Cross-tenant rows land in their destination's outbox under their out
//! id, which the destination dedups.
//!
//! **The fallback.** The gateways' polling forwarder takes transactional
//! publishes older than `queue.forward_fallback_ms` (2 s) while this job
//! isn't running (its `queue-forwarder-beats/<cluster>` beat is missing or
//! older than 6 s): rows still go, in insert order. While the job runs,
//! the fallback takes only rows older than 30 s
//! (`sprig_queues::engine::FALLBACK_WHILE_FEED_RUNS_MS`): before
//! bugfix-42 it raced the feed for rows the feed was slow to forward (a
//! tenant's first forwards, a loaded machine) and, taking them in insert
//! order, put some after rows committed later. A run that keeps failing is
//! given up after [`RUN_GIVE_UP`] and left to the fallback, so one
//! unreachable destination doesn't hold the tenant's other queues.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde_json::{Value, json};
use sprig_catalog::CatalogLocator;
use sprig_common::Result;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_controller::catalogs::ShardMapLocator;
use sprig_controller::dispatch::JobClass;
use sprig_feed::PlacementView;
use sprig_feed::names::{CatalogPools, CatalogResolver};
use sprig_feed::shared::{Kept, SharedClusters, SharedFeed};
use sprig_feed::subs::{Subscription, Subscriptions};
use sprig_feed::{
    ChangeEvent, Clusters, FeedItem, MetaPlacement, Op, ResumeToken, StartAt, TailOptions,
    TenantTail,
};
use sprig_meta::{ListOpts, MetaStore, Record};
use sprig_sinks::peer::{SinkPeer, SqlOp, SqlRequest, SqlResult};
use tokio::sync::mpsc;

use crate::runtime::{JobCtx, JobHandler};

pub use sprig_controller::queue_forwarder_loop::{JOB_KIND, SUB_NAME};

/// The queue outbox.
pub const QO: &str = "__sprig_qo";
/// The cross-tenant outbox.
pub const XO: &str = "__sprig_xo";

/// Rows taken from the feed per round at most.
pub const MAX_ROUND_ROWS: usize = 1_000;

/// A run that keeps failing this long is left to the polling fallback.
pub const RUN_GIVE_UP: Duration = Duration::from_secs(10);

/// What the job needs.
#[derive(Clone)]
pub struct ForwarderEnv {
    /// This DC's tier (subscriptions, placement).
    pub dc: Arc<dyn MetaStore>,
    /// Tenant catalogs.
    pub locator: Arc<ShardMapLocator>,
    /// Binlog sources: the worker's shared reader (one dump per cluster
    /// for every tenant's outbox rows).
    pub clusters: Arc<SharedClusters>,
    /// The gateways (`LogicPeer`).
    pub peer: Arc<dyn SinkPeer>,
}

impl std::fmt::Debug for ForwarderEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwarderEnv").finish_non_exhaustive()
    }
}

/// `queue_forwarder` (see the module docs).
#[derive(Debug, Clone)]
pub struct QueueForwarder(pub ForwarderEnv);

#[async_trait]
impl JobHandler for QueueForwarder {
    fn kind(&self) -> &'static str {
        JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        if let Some(c) = cx.spec["cluster"].as_str() {
            let cluster = ClusterId::new(c).map_err(|e| e.to_string())?;
            return ClusterForwarder::new(self.0.clone(), cluster)
                .serve(cx)
                .await;
        }
        if cx.spec["tenant"].is_u64() {
            // An r9fwd job (one per tenant): the cluster's job serves the
            // tenant now.
            return Ok(json!({"end": "superseded"}));
        }
        Err("spec.cluster is missing".to_owned())
    }
}

/// A shared reader of its own for the forwarder: one dump per cluster,
/// rows of the two outboxes only.
#[must_use]
pub fn shared_reader(clusters: Arc<dyn Clusters>) -> Arc<SharedClusters> {
    Arc::new(SharedClusters::new(clusters, &[QO, XO]))
}

/// The forwarder's view of the worker's shared reader (R9, r9feed): the
/// same dump as the stream sequencers', rows of the two outboxes.
#[must_use]
pub fn shared_view(feed: &Arc<SharedFeed>) -> Arc<SharedClusters> {
    feed.view(Kept::Tables(vec![QO.to_owned(), XO.to_owned()].into()))
}

/// How often the cluster job looks for tenants.
pub const REFRESH: Duration = Duration::from_secs(5);
/// How often a running tenant's placement is read again.
pub const PLACEMENT_RECHECK: Duration = Duration::from_secs(60);
/// An idle tenant's position is acknowledged at most this often.
pub const IDLE_ACK: Duration = Duration::from_secs(30);

/// The tenants of `subs` with an active forwarder subscription.
#[must_use]
pub fn subscribed(subs: &[Subscription]) -> HashSet<TenantId> {
    subs.iter()
        .filter(|s| s.kind == SUB_NAME && s.state == "active")
        .map(|s| s.tenant)
        .collect()
}

struct Running {
    stop: sprig_common::CancellationToken,
    task: tokio::task::JoinHandle<()>,
    checked: Instant,
}

/// One cluster's forwarder: a task per tenant on one shared dump.
struct ClusterForwarder {
    env: ForwarderEnv,
    cluster: ClusterId,
    placement: MetaPlacement,
    stats: Arc<Stats>,
    running: HashMap<TenantId, Running>,
}

impl ClusterForwarder {
    fn new(env: ForwarderEnv, cluster: ClusterId) -> Self {
        let placement = MetaPlacement::new(Arc::clone(&env.dc));
        Self {
            env,
            cluster,
            placement,
            stats: Arc::new(Stats::default()),
            running: HashMap::new(),
        }
    }

    async fn list_subscriptions(&self) -> Result<Vec<Subscription>> {
        let prefix = self.env.dc.tier().key("feed/subs/")?;
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .env
                .dc
                .list(
                    &prefix,
                    ListOpts {
                        after: after.clone(),
                        limit: 5_000,
                        ..ListOpts::default()
                    },
                )
                .await?;
            for kv in &page.kvs {
                if let Ok(r) = Record::<Subscription>::decode(&kv.value) {
                    out.push(r.body);
                }
            }
            after = page.kvs.last().map(|kv| kv.key.clone());
            if !page.more || after.is_none() {
                return Ok(out);
            }
        }
    }

    /// Starts and stops tenant tasks (see the module docs).
    async fn refresh(&mut self, cancel: &sprig_common::CancellationToken) {
        let subs = match self.list_subscriptions().await {
            Ok(s) => subscribed(&s),
            Err(e) => {
                tracing::warn!(cluster = %self.cluster, error = %e, "queue forwarder: subscriptions not read");
                return;
            }
        };
        // Finished tasks (a tenant gone) start again if still wanted.
        self.running.retain(|_, r| !r.task.is_finished());
        let gone: Vec<TenantId> = self
            .running
            .keys()
            .filter(|t| !subs.contains(t))
            .copied()
            .collect();
        for t in gone {
            if let Some(r) = self.running.remove(&t) {
                r.stop.cancel();
            }
        }
        for t in subs {
            let recheck = self
                .running
                .get(&t)
                .is_none_or(|r| r.checked.elapsed() >= PLACEMENT_RECHECK);
            if !recheck {
                continue;
            }
            let here = match self.placement.current(t).await {
                Ok((c, _)) => c == self.cluster,
                Err(e) => {
                    tracing::debug!(tenant = %t, error = %e, "queue forwarder: placement not read");
                    continue;
                }
            };
            match (here, self.running.get_mut(&t)) {
                (true, Some(r)) => r.checked = Instant::now(),
                (true, None) => {
                    let stop = cancel.child_token();
                    let mut f = Forwarder::new(self.env.clone(), t, Arc::clone(&self.stats));
                    let s = stop.clone();
                    let task = tokio::spawn(async move {
                        let _ = f.serve(&s).await;
                    });
                    self.running.insert(
                        t,
                        Running {
                            stop,
                            task,
                            checked: Instant::now(),
                        },
                    );
                }
                (false, Some(_)) => {
                    // Moved away: the new cluster's job takes it.
                    if let Some(r) = self.running.remove(&t) {
                        r.stop.cancel();
                    }
                }
                (false, None) => {}
            }
        }
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!("sprig_queue_forwarder_tenants", "cluster" => self.cluster.to_string())
            .set(self.running.len() as f64);
    }

    fn progress(&self) -> Value {
        let n = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let hub = self.env.clusters.stats(&self.cluster);
        json!({
            "cluster": self.cluster.to_string(),
            "tenants": self.running.len(),
            "subscriptions": hub.as_ref().map_or(0, |h| h.subscriptions),
            "dump": hub.is_some_and(|h| h.running),
            "forwarded": n(&self.stats.forwarded),
            "gone": n(&self.stats.gone),
            "left": n(&self.stats.left),
            "given_up": n(&self.stats.given_up),
            "runs": n(&self.stats.runs),
        })
    }

    async fn serve(&mut self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let mut last_refresh: Option<Instant> = None;
        tracing::info!(cluster = %self.cluster, "queue forwarder for the cluster starts");
        // A new subscription is picked up at once, not at the next 5 s
        // refresh: its first publishes then go from the feed, in commit
        // order, before the 2 s fallback takes them (decision 88; r9fanin).
        let (wake_tx, mut wake_rx) = mpsc::channel::<()>(1);
        let watcher = watch_subscriptions(Arc::clone(&self.env.dc), wake_tx, cx.cancel.clone());
        loop {
            if last_refresh.is_none_or(|t| t.elapsed() >= REFRESH) {
                self.refresh(&cx.cancel).await;
                last_refresh = Some(Instant::now());
            }
            let held = cx.progress(self.progress()).await.unwrap_or(true);
            if !held {
                break;
            }
            // The gateways' fallback leaves the feed's rows alone while
            // this beat is fresh (bugfix-42).
            let beat = sprig_feed::beat::Beat {
                at_ms: now_ms(),
                tenants: self.running.len() as u64,
            };
            if let Err(e) = sprig_feed::beat::write(&self.env.dc, &self.cluster, beat).await {
                tracing::debug!(cluster = %self.cluster, error = %e, "queue forwarder beat not written");
            }
            tokio::select! {
                () = cx.cancel.cancelled() => break,
                () = tokio::time::sleep(Duration::from_secs(2)) => {}
                Some(()) = wake_rx.recv() => last_refresh = None,
            }
        }
        watcher.abort();
        // The fallback takes over at once (bugfix-42).
        let _ = sprig_feed::beat::clear(&self.env.dc, &self.cluster).await;
        // Stop every tenant task and let it acknowledge its position.
        let tasks: Vec<Running> = self.running.drain().map(|(_, r)| r).collect();
        for r in &tasks {
            r.stop.cancel();
        }
        for r in tasks {
            let _ = tokio::time::timeout(Duration::from_secs(5), r.task).await;
        }
        Err("cancelled".to_owned())
    }
}

/// Whether `key` is a forwarder subscription (`.../feed/subs/<tenant>/queue_forwarder`).
#[must_use]
pub fn is_forwarder_sub(key: &str) -> bool {
    key.rsplit('/').next() == Some(SUB_NAME) && key.contains("/feed/subs/")
}

/// Wakes the cluster job when a forwarder subscription is written (a new
/// tenant), so it starts the tenant's task at once; re-watches after an
/// error. The 5 s refresh stays as the fallback.
fn watch_subscriptions(
    dc: Arc<dyn MetaStore>,
    wake: mpsc::Sender<()>,
    cancel: sprig_common::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    use tokio_stream::StreamExt as _;
    tokio::spawn(async move {
        let Ok(prefix) = dc.tier().key("feed/subs/") else {
            return;
        };
        loop {
            match dc.watch(&prefix, 0).await {
                Ok(mut w) => loop {
                    let ev = tokio::select! {
                        () = cancel.cancelled() => return,
                        e = w.next() => e,
                    };
                    match ev {
                        Some(Ok(sprig_meta::WatchEvent::Put(kv)))
                            if is_forwarder_sub(kv.key.as_str()) =>
                        {
                            let _ = wake.try_send(());
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            tracing::debug!(error = %e, "queue forwarder: subscription watch broke");
                            break;
                        }
                        None => break,
                    }
                },
                Err(e) => {
                    tracing::debug!(error = %e, "queue forwarder: subscriptions not watched");
                }
            }
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    })
}

/// One outbox row the feed showed committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    /// Its shard schema.
    pub schema: String,
    /// [`QO`] or [`XO`].
    pub table: &'static str,
    /// Its out id.
    pub out_id: [u8; 16],
}

/// The outbox row an event inserted, if it is one the forwarder takes:
/// `__sprig_qo` rows of kind `publish` (dead letters and due delayed
/// messages are the owners' and the polling forwarder's), every
/// `__sprig_xo` row.
#[must_use]
pub fn committed(tenant: TenantId, ev: &ChangeEvent) -> Option<Committed> {
    if ev.op != Op::Insert {
        return None;
    }
    let table = match ev.table.as_str() {
        QO => QO,
        XO => XO,
        _ => return None,
    };
    let img = ev.after.as_ref()?;
    let out_id = match img.get("out_id").or_else(|| ev.key.get("out_id"))? {
        sprig_binlog::Value::Bytes(b) | sprig_binlog::Value::String(b) => {
            <[u8; 16]>::try_from(b.as_slice()).ok()?
        }
        _ => return None,
    };
    if table == QO {
        // ENUM('publish', ...): 'publish' is member 1. A row whose kind
        // can't be read is forwarded (a gone row costs a lookup only).
        match img.get("kind") {
            Some(sprig_binlog::Value::Enum(1) | sprig_binlog::Value::Null) | None => {}
            Some(sprig_binlog::Value::String(s)) if s.as_slice() == b"publish" => {}
            Some(_) => return None,
        }
    }
    Some(Committed {
        schema: sprig_shard::shard_schema(tenant, ev.db, ev.shard.group, ev.shard.shard),
        table,
        out_id,
    })
}

/// Consecutive rows of one outbox table in one schema, in commit order:
/// each run is one request, sent after the one before it confirmed.
#[must_use]
pub fn runs(rows: &[Committed]) -> Vec<(String, &'static str, Vec<[u8; 16]>)> {
    let mut out: Vec<(String, &'static str, Vec<[u8; 16]>)> = Vec::new();
    for r in rows {
        match out.last_mut() {
            Some((s, t, ids)) if *s == r.schema && *t == r.table => ids.push(r.out_id),
            _ => out.push((r.schema.clone(), r.table, vec![r.out_id])),
        }
    }
    out
}

fn hex(id: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    id.iter().fold(String::with_capacity(32), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The tenant's catalogs for the feed's name resolution.
struct Catalogs(Arc<ShardMapLocator>);

#[async_trait]
impl CatalogPools for Catalogs {
    async fn pool(&self, tenant: TenantId) -> Result<mysql_async::Pool> {
        Ok(self.0.writer(tenant).await?.pool)
    }
}

/// What the feed task sends: an item, or the error that ended the tail.
type Item = std::result::Result<FeedItem, String>;

/// Counters shared by a cluster job's tenant tasks.
#[derive(Debug, Default)]
struct Stats {
    forwarded: AtomicU64,
    gone: AtomicU64,
    left: AtomicU64,
    given_up: AtomicU64,
    runs: AtomicU64,
}

struct Forwarder {
    env: ForwarderEnv,
    tenant: TenantId,
    subs: Subscriptions,
    stats: Arc<Stats>,
}

impl Forwarder {
    fn new(env: ForwarderEnv, tenant: TenantId, stats: Arc<Stats>) -> Self {
        let subs = Subscriptions::new(Arc::clone(&env.dc));
        Self {
            env,
            tenant,
            subs,
            stats,
        }
    }

    /// Starts the tail in its own task (a tail's `next` isn't cancel-safe,
    /// so the job never drops one half-read).
    async fn open(
        &self,
        start: StartAt,
        stop: sprig_common::CancellationToken,
    ) -> Result<mpsc::Receiver<Item>> {
        let mut tail = TenantTail::start(
            self.tenant,
            start,
            Arc::clone(&self.env.clusters) as Arc<dyn Clusters>,
            Arc::new(MetaPlacement::new(Arc::clone(&self.env.dc))),
            Arc::new(CatalogResolver::new(Arc::new(Catalogs(Arc::clone(
                &self.env.locator,
            ))))),
            TailOptions {
                keep_internal: vec![QO.to_owned(), XO.to_owned()],
                // A thousand tenants' tails read the map: every 5 s each
                // (a move's fence keeps the order; it is noticed later).
                handoff_poll: Duration::from_secs(5),
                ..TailOptions::default()
            },
        )
        .await?;
        let (tx, rx) = mpsc::channel::<Item>(4096);
        tokio::spawn(async move {
            loop {
                let item = tokio::select! {
                    () = stop.cancelled() => return,
                    i = tail.next() => i,
                };
                let end = item.is_err();
                if tx.send(item.map_err(|e| e.to_string())).await.is_err() || end {
                    return;
                }
            }
        });
        Ok(rx)
    }

    async fn start_at(&self) -> StartAt {
        match self.subs.checkpoint(self.tenant, SUB_NAME).await {
            Ok(Some(cp)) => match cp.resume_token() {
                Ok(t) => StartAt::After(t),
                Err(e) => {
                    tracing::warn!(tenant = %self.tenant, error = %e, "queue forwarder checkpoint unreadable; starting now");
                    StartAt::Now
                }
            },
            Ok(None) => StartAt::Now,
            Err(e) => {
                tracing::warn!(tenant = %self.tenant, error = %e, "queue forwarder checkpoint not read; starting now");
                StartAt::Now
            }
        }
    }

    #[allow(clippy::too_many_lines)] // the loop of the module docs
    async fn serve(
        &mut self,
        cancel: &sprig_common::CancellationToken,
    ) -> std::result::Result<Value, String> {
        let mut acked: Option<ResumeToken> = None;
        let mut last_ack = Instant::now();
        let mut unacked_rows = false;
        let mut dirty = false;
        let mut pending_events = 0u64;
        let mut retry = Duration::from_millis(200);
        'outer: loop {
            if cancel.is_cancelled() {
                break;
            }
            // Gone tenants: the subscription is deleted with them.
            if matches!(self.subs.get(self.tenant, SUB_NAME).await, Err(e) if e.code() == Some(sprig_common::SprigCode::FeedSubscriptionNotFound))
            {
                tracing::info!(tenant = %self.tenant, "queue forwarder subscription gone; stopping");
                return Ok(json!({"end": "gone"}));
            }
            let start = match &acked {
                Some(t) => StartAt::After(t.clone()),
                None => self.start_at().await,
            };
            let stop = cancel.child_token();
            let mut rx = match self.open(start, stop.clone()).await {
                Ok(rx) => rx,
                Err(e) => {
                    tracing::warn!(tenant = %self.tenant, error = %e, "queue forwarder: feed not opened; retrying");
                    tokio::select! {
                        () = cancel.cancelled() => break,
                        () = tokio::time::sleep(retry) => {}
                    }
                    retry = (retry * 2).min(Duration::from_secs(5));
                    continue;
                }
            };
            retry = Duration::from_millis(200);
            tracing::info!(tenant = %self.tenant, "queue forwarder reading the feed");
            // Rows of the transaction being read, and of whole ones.
            let mut open_txn: Vec<Committed> = Vec::new();
            loop {
                let first = tokio::select! {
                    () = cancel.cancelled() => { stop.cancel(); break 'outer; }
                    i = rx.recv() => i,
                };
                let Some(first) = first else {
                    stop.cancel();
                    continue 'outer;
                };
                let mut batch: Vec<Committed> = Vec::new();
                let mut token: Option<ResumeToken> = None;
                let mut next = Some(first);
                let mut ended = false;
                while let Some(item) = next.take() {
                    match item {
                        Ok(FeedItem::Event(ev)) => {
                            pending_events += 1;
                            if let Some(c) = committed(self.tenant, &ev) {
                                open_txn.push(c);
                            }
                        }
                        Ok(FeedItem::TxnEnd { token: t }) => {
                            batch.append(&mut open_txn);
                            token = Some(t);
                        }
                        Ok(FeedItem::Progress { token: t }) => {
                            if open_txn.is_empty() {
                                token = Some(t);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(tenant = %self.tenant, error = %e, "queue forwarder: feed broke; reopening");
                            ended = true;
                            break;
                        }
                    }
                    if batch.len() >= MAX_ROUND_ROWS {
                        break;
                    }
                    next = rx.try_recv().ok();
                }
                if !batch.is_empty() {
                    unacked_rows = true;
                }
                if !batch.is_empty() && !self.forward(&batch, cancel).await {
                    stop.cancel();
                    break 'outer;
                }
                if let Some(t) = token {
                    acked = Some(t);
                    dirty = true;
                }
                let due = if unacked_rows {
                    Duration::from_secs(1)
                } else {
                    IDLE_ACK
                };
                if dirty
                    && last_ack.elapsed() >= due
                    && let Some(t) = &acked
                {
                    match self
                        .subs
                        .ack(self.tenant, SUB_NAME, t, pending_events, now_ms())
                        .await
                    {
                        Ok(()) => {
                            pending_events = 0;
                            unacked_rows = false;
                            dirty = false;
                        }
                        Err(e) => {
                            tracing::debug!(tenant = %self.tenant, error = %e, "queue forwarder checkpoint not written");
                        }
                    }
                    last_ack = Instant::now();
                }
                if ended {
                    stop.cancel();
                    open_txn.clear();
                    tokio::select! {
                        () = cancel.cancelled() => break 'outer,
                        () = tokio::time::sleep(Duration::from_millis(200)) => {}
                    }
                    continue 'outer;
                }
            }
        }
        if let Some(t) = &acked {
            let _ = self
                .subs
                .ack(self.tenant, SUB_NAME, t, pending_events, now_ms())
                .await;
        }
        Err("cancelled".to_owned())
    }

    /// Forwards `rows` run by run; `false` when cancelled.
    async fn forward(
        &mut self,
        rows: &[Committed],
        cancel: &sprig_common::CancellationToken,
    ) -> bool {
        for (schema, table, ids) in runs(rows) {
            let req = SqlRequest {
                tenant: self.tenant.0,
                tenant_name: String::new(),
                user_id: 0,
                database: String::new(),
                op: SqlOp::OutboxForward {
                    schema: schema.clone(),
                    table: table.to_owned(),
                    ids: ids.iter().map(hex).collect(),
                },
                max_rows: 0,
                timeout_ms: 0,
            };
            let started = Instant::now();
            let mut wait = Duration::from_millis(50);
            loop {
                if cancel.is_cancelled() {
                    return false;
                }
                match self
                    .env
                    .peer
                    .sql(self.tenant.0, &req)
                    .await
                    .and_then(sprig_sinks::peer::SqlReply::ok)
                {
                    Ok(reply) => {
                        let (f, g, l) = counts(reply.results.first());
                        self.stats.forwarded.fetch_add(f, Ordering::Relaxed);
                        self.stats.gone.fetch_add(g, Ordering::Relaxed);
                        self.stats.left.fetch_add(l, Ordering::Relaxed);
                        self.stats.runs.fetch_add(1, Ordering::Relaxed);
                        metrics::counter!("sprig_queue_forward_feed_rows_total", "result" => "forwarded")
                            .increment(f);
                        metrics::counter!("sprig_queue_forward_feed_rows_total", "result" => "gone")
                            .increment(g);
                        break;
                    }
                    Err(e) if started.elapsed() >= RUN_GIVE_UP => {
                        self.stats
                            .given_up
                            .fetch_add(ids.len() as u64, Ordering::Relaxed);
                        metrics::counter!("sprig_queue_forward_feed_rows_total", "result" => "given_up")
                            .increment(ids.len() as u64);
                        tracing::warn!(tenant = %self.tenant, %schema, table, rows = ids.len(), error = %e, "queue forwarder: run given up; the polling fallback forwards it");
                        break;
                    }
                    Err(e) => {
                        tracing::debug!(tenant = %self.tenant, %schema, error = %e, "queue forwarder: run failed; retrying");
                        tokio::select! {
                            () = cancel.cancelled() => return false,
                            () = tokio::time::sleep(wait) => {}
                        }
                        wait = (wait * 2).min(Duration::from_secs(1));
                    }
                }
            }
        }
        true
    }
}

/// (forwarded, gone, left) from an `outbox_forward` result.
fn counts(r: Option<&SqlResult>) -> (u64, u64, u64) {
    let Some(r) = r else { return (0, 0, 0) };
    let Some(row) = r.rows.first() else {
        return (0, 0, 0);
    };
    let n = |i| {
        SqlResult::text(row, i)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    };
    (n(0), n(1), n(2))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_binlog::Value as V;
    use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId};
    use sprig_feed::{EventId, Image, ShardRef};

    use super::*;

    fn ev(table: &str, op: Op, id: u8, kind: Option<V>) -> ChangeEvent {
        let mut after = vec![("out_id".to_owned(), V::Bytes(vec![id; 16]))];
        if let Some(k) = kind {
            after.push(("kind".to_owned(), k));
        }
        let cluster = ClusterId::new("dc-a-c1").unwrap();
        ChangeEvent {
            id: EventId {
                cluster: cluster.clone(),
                gtid: sprig_binlog::Gtid {
                    domain: 0,
                    server: 1,
                    seq: 1,
                },
                row: 0,
            },
            token: ResumeToken {
                tenant: TenantId(7),
                clamp: 0,
                cluster,
                segment: 0,
                pos: sprig_binlog::GtidPos::new(),
                until: None,
                progress: false,
            },
            tenant: TenantId(7),
            db: DbId(3),
            db_name: "shop".into(),
            object: 0,
            table: table.into(),
            shard: ShardRef {
                group: GroupId(1),
                shard: ShardId(2),
            },
            key: Image(vec![("out_id".to_owned(), V::Bytes(vec![id; 16]))]),
            op,
            before: None,
            after: Some(Image(after)),
            delta: None,
            hlc: 0,
            txn: sprig_binlog::Gtid {
                domain: 0,
                server: 1,
                seq: 1,
            },
            ddl: None,
            hint: None,
        }
    }

    #[test]
    fn takes_publishes_and_cross_tenant_rows_only() {
        let t = TenantId(7);
        let c = committed(t, &ev(QO, Op::Insert, 1, Some(V::Enum(1)))).unwrap();
        assert_eq!(c.table, QO);
        assert_eq!(c.out_id, [1; 16]);
        assert_eq!(
            c.schema,
            sprig_shard::shard_schema(t, DbId(3), GroupId(1), ShardId(2))
        );
        // Dead letters (2) and due delayed messages (3) are the owners'.
        assert!(committed(t, &ev(QO, Op::Insert, 1, Some(V::Enum(2)))).is_none());
        assert!(committed(t, &ev(QO, Op::Insert, 1, Some(V::Enum(3)))).is_none());
        // The forwarder's own delete, and other tables.
        assert!(committed(t, &ev(QO, Op::Delete, 1, Some(V::Enum(1)))).is_none());
        assert!(committed(t, &ev("orders", Op::Insert, 1, None)).is_none());
        let x = committed(t, &ev(XO, Op::Insert, 2, Some(V::Enum(2)))).unwrap();
        assert_eq!((x.table, x.out_id), (XO, [2; 16]));
    }

    #[test]
    fn runs_keep_commit_order() {
        let r = |s: &str, t: &'static str, i: u8| Committed {
            schema: s.into(),
            table: t,
            out_id: [i; 16],
        };
        let rows = vec![
            r("a", QO, 1),
            r("a", QO, 2),
            r("a", XO, 3),
            r("b", QO, 4),
            r("a", QO, 5),
        ];
        let got = runs(&rows);
        let shape: Vec<(String, &str, Vec<u8>)> = got
            .into_iter()
            .map(|(s, t, ids)| (s, t, ids.iter().map(|i| i[0]).collect()))
            .collect();
        assert_eq!(
            shape,
            vec![
                ("a".to_owned(), QO, vec![1, 2]),
                ("a".to_owned(), XO, vec![3]),
                ("b".to_owned(), QO, vec![4]),
                ("a".to_owned(), QO, vec![5]),
            ]
        );
        assert_eq!(hex(&[0xab; 16]).len(), 32);
        let res = SqlResult {
            columns: vec!["forwarded".into(), "gone".into(), "left".into()],
            rows: vec![vec![
                SqlResult::cell(Some(b"5")),
                SqlResult::cell(Some(b"2")),
                SqlResult::cell(Some(b"0")),
            ]],
            ..SqlResult::default()
        };
        assert_eq!(counts(Some(&res)), (5, 2, 0));
    }

    /// r9fanin (decision 88): a new forwarder subscription wakes the
    /// cluster job at once (before, it waited for the next 5 s refresh and
    /// the 2 s fallback took the tenant's first publishes); other
    /// subscriptions don't.
    #[tokio::test]
    async fn a_new_forwarder_subscription_wakes_the_job() {
        use sprig_feed::subs::{Subscription, Subscriptions};
        let dc: Arc<dyn MetaStore> = Arc::new(sprig_meta::MemStore::new(sprig_meta::Tier::Dc(
            sprig_common::ids::DcId::new("dc-a").unwrap(),
        )));
        let (tx, mut rx) = mpsc::channel(1);
        let cancel = sprig_common::CancellationToken::new();
        let w = watch_subscriptions(Arc::clone(&dc), tx, cancel.clone());
        tokio::time::sleep(Duration::from_millis(100)).await;
        let subs = Subscriptions::new(Arc::clone(&dc));
        let sub = |name: &str| Subscription {
            tenant: TenantId(7),
            name: name.into(),
            kind: name.into(),
            ordering: "merged".into(),
            owner_ref: None,
            state: "active".into(),
            created_at_ms: 1,
        };
        subs.register(&sub("search")).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(300), rx.recv())
                .await
                .is_err()
        );
        subs.register(&sub(SUB_NAME)).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap(),
            Some(())
        );
        assert!(is_forwarder_sub(
            "/sprig/dc/dc-a/feed/subs/7/queue_forwarder"
        ));
        assert!(!is_forwarder_sub(
            "/sprig/dc/dc-a/feed/checkpoints/7/queue_forwarder"
        ));
        cancel.cancel();
        let _ = w.await;
    }
}
