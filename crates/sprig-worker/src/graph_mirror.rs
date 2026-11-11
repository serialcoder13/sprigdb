//! `graph_mirror` (chapter 43, "Mirrors and freshness"; build step 14; R8
//! round 5, decision G47): one long-running job per tenant
//! (`spec: {"tenant": <id>}`), keeping the asynchronous mirrors of the
//! tenant's grouped graphs ([`sprig_graph::mirror`]).
//!
//! ```text
//! serve(tenant):
//!   open a binlog reader on every cluster of the DC (registered, not
//!   retired), each after its checkpointed position or at its position now
//!   loop:
//!     every 2 s: the graphs served (grouped native graphs whose
//!       mirror_mode is starting or async, or whose mirrors are building)
//!       and their shard maps; clusters added to the DC get a reader
//!     take what the readers have (at most 512 groups or 50 ms)
//!     per group, in each cluster's commit order: skip the mover's copies
//!       and replays (they carry its position row); hold an XA branch's
//!       rows from its PREPARE to its COMMIT; edge-table rows of a served
//!       graph become EdgeChanges
//!     apply each graph's changes (apply_batch); a batch whose shards
//!       changed epoch meanwhile runs again with the new map
//!     each cluster's position moves past what was applied
//!     every 200 ms (and at least every second): a position row per shard
//!       of each served graph, its cluster's position
//!     every 2 s: the positions as the job's progress (its checkpoint)
//! ```
//!
//! Delivery is at least once: a new owner resumes at the checkpoint and
//! applies again, which is harmless because every step is idempotent and
//! re-checks the edge. Positions written are never ahead of what was
//! applied, so readers that waited on them never miss an edge.
//!
//! **Readers of every cluster.** A shard can move to any cluster, and its
//! writes there must be read from before the move's cutover, so the job
//! reads every cluster of the DC all the time (one binlog dump per
//! cluster and tenant), not only those holding the graph now.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_binlog::GtidPos;
use sprig_binlog::value::Value as BinValue;
use sprig_catalog::CatalogLocator;
use sprig_common::ids::{ClusterId, DbId, GroupId, TenantId};
use sprig_common::names::quote;
use sprig_common::{CancellationToken, Result, SprigError};
use sprig_controller::catalogs::{ClusterPools, ShardMapLocator};
use sprig_controller::dispatch::JobClass;
use sprig_feed::source::{RawRow, RowKind, XaMark};
use sprig_feed::{Clusters, SourceItem, TxnSource};
use sprig_graph::catalog::Tables;
use sprig_graph::maria::{GraphShards, ShardLoc};
use sprig_graph::mirror::{self, ApplyConns, EdgeChange};
use sprig_meta::MetaStore;
use sprig_shard::{ShardMapStore, shard_schema};
use tokio::sync::mpsc;

use crate::runtime::{JobCtx, JobHandler};

pub use sprig_graph::mirror::JOB_KIND;

/// What the job needs.
#[derive(Clone)]
pub struct GraphMirrorEnv {
    /// This DC's tier (shard maps).
    pub dc: Arc<dyn MetaStore>,
    /// `sprig_ctl` pools per cluster.
    pub pools: Arc<dyn ClusterPools>,
    /// Tenant catalogs.
    pub locator: Arc<ShardMapLocator>,
    /// Binlog sources.
    pub clusters: Arc<dyn Clusters>,
}

impl std::fmt::Debug for GraphMirrorEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphMirrorEnv").finish_non_exhaustive()
    }
}

/// `graph_mirror` (see the module docs).
#[derive(Debug, Clone)]
pub struct GraphMirror(pub GraphMirrorEnv);

#[async_trait]
impl JobHandler for GraphMirror {
    fn kind(&self) -> &'static str {
        JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let tenant = TenantId(
            cx.spec["tenant"]
                .as_u64()
                .ok_or_else(|| "spec.tenant is missing".to_owned())?,
        );
        let mut job = Applier::new(self.0.clone(), tenant);
        job.serve(cx).await
    }
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

/// A served graph.
#[derive(Debug, Clone)]
struct Served {
    layout: Arc<GraphShards>,
}

/// Items from the readers: (cluster, the reader's generation, an item or
/// the error that ended it).
type Item = (ClusterId, u64, std::result::Result<SourceItem, String>);

/// An XA branch prepared but not committed yet: its cluster, xid, the
/// position before its PREPARE, and its rows.
type Held = (ClusterId, String, GtidPos, Vec<RawRow>);

/// Connections for [`mirror::apply_batch`]: fresh ones from the pools.
struct PoolConns(Arc<dyn ClusterPools>);

#[async_trait]
impl ApplyConns for PoolConns {
    async fn connect(&mut self, cluster: &str) -> Result<mysql_async::Conn> {
        let id = ClusterId::new(cluster)
            .map_err(|e| SprigError::internal(format!("cluster id {cluster}: {e}")))?;
        let pool = self.0.writer(&id).await?;
        pool.get_conn()
            .await
            .map_err(sprig_graph::catalog::backend_error)
    }
}

struct Applier {
    env: GraphMirrorEnv,
    tenant: TenantId,
    graphs: HashMap<u64, Served>,
    /// Each cluster's applied position.
    pos: BTreeMap<ClusterId, GtidPos>,
    /// Readers running, by cluster: their generation and stop token.
    readers: HashMap<ClusterId, (u64, CancellationToken)>,
    next_gen: u64,
    /// XA branches prepared but not committed, oldest first.
    held: Vec<Held>,
    applied: u64,
}

impl Applier {
    fn new(env: GraphMirrorEnv, tenant: TenantId) -> Self {
        Self {
            env,
            tenant,
            graphs: HashMap::new(),
            pos: BTreeMap::new(),
            readers: HashMap::new(),
            next_gen: 1,
            held: Vec::new(),
            applied: 0,
        }
    }

    #[allow(clippy::too_many_lines)] // the loop of the module docs
    async fn serve(&mut self, cx: &JobCtx) -> std::result::Result<Value, String> {
        // The checkpoint: positions per cluster.
        if let Some(m) = cx.progress.get("pos").and_then(Value::as_object) {
            for (c, p) in m {
                if let (Ok(c), Some(Ok(p))) = (
                    ClusterId::new(c),
                    p.as_str().map(str::parse::<GtidPos>),
                ) {
                    self.pos.insert(c, p);
                }
            }
        }
        let (tx, mut rx) = mpsc::channel::<Item>(1024);
        let mut last_refresh: Option<Instant> = None;
        let mut last_pos_write = Instant::now();
        let mut last_progress = Instant::now();
        let mut dirty = true;
        let result = loop {
            if cx.cancel.is_cancelled() {
                break Err("cancelled".to_owned());
            }
            if last_refresh.is_none_or(|t| t.elapsed() >= Duration::from_secs(2)) {
                if let Err(e) = self.refresh(&tx).await {
                    tracing::warn!(tenant = %self.tenant, error = %e, "graph mirror refresh failed");
                }
                last_refresh = Some(Instant::now());
                dirty = true;
            }
            // Take what the readers have.
            let mut items: Vec<Item> = Vec::new();
            let deadline = tokio::time::sleep(Duration::from_millis(50));
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    () = cx.cancel.cancelled() => break,
                    () = &mut deadline => break,
                    it = rx.recv() => match it {
                        Some(it) => {
                            items.push(it);
                            if items.len() >= 512 {
                                break;
                            }
                        }
                        None => break,
                    },
                }
            }
            if !items.is_empty() {
                match self.handle(items).await {
                    Ok(moved) => dirty |= moved,
                    Err(e) => {
                        // Nothing past the batch was recorded: start the
                        // readers again from the positions (items of the
                        // old readers are ignored by generation).
                        tracing::warn!(tenant = %self.tenant, error = %e, "graph mirror batch failed; rereading");
                        self.stop_readers();
                        self.held.clear();
                        last_refresh = None;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                }
            }
            if (dirty && last_pos_write.elapsed() >= Duration::from_millis(200))
                || last_pos_write.elapsed() >= Duration::from_secs(1)
            {
                match self.write_positions().await {
                    Ok(()) => {
                        last_pos_write = Instant::now();
                        dirty = false;
                    }
                    Err(e) => {
                        tracing::warn!(tenant = %self.tenant, error = %e, "graph mirror positions not written");
                    }
                }
            }
            if last_progress.elapsed() >= Duration::from_secs(2) {
                let pos: serde_json::Map<String, Value> = self
                    .pos
                    .keys()
                    .filter_map(|c| {
                        self.safe_pos(c)
                            .map(|p| (c.to_string(), Value::String(p.to_string())))
                    })
                    .collect();
                match cx
                    .progress(json!({ "pos": pos, "applied": self.applied, "graphs": self.graphs.len() }))
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => break Err("lost the job".to_owned()),
                    Err(e) => tracing::warn!(error = %e, "graph mirror progress not recorded"),
                }
                last_progress = Instant::now();
            }
        };
        self.stop_readers();
        result
    }

    fn stop_readers(&mut self) {
        for (_, (_, t)) in self.readers.drain() {
            t.cancel();
        }
    }

    /// The position of `c` that is safe to record: before the oldest XA
    /// branch still held there (its rows aren't applied yet).
    fn safe_pos(&self, c: &ClusterId) -> Option<GtidPos> {
        self.held
            .iter()
            .find(|h| &h.0 == c)
            .map(|h| h.2.clone())
            .or_else(|| self.pos.get(c).cloned())
    }

    /// The graphs served and their maps; readers for clusters without one.
    async fn refresh(&mut self, tx: &mpsc::Sender<Item>) -> Result<()> {
        let ep = self.env.locator.writer(self.tenant).await?;
        let schema = ep.store.schema().to_owned();
        let mut c = ep
            .pool
            .get_conn()
            .await
            .map_err(sprig_graph::catalog::backend_error)?;
        let rows: Vec<(u64, u32, u32)> = match c
            .query(format!(
                "SELECT graph_id, db_id, group_id FROM {}.graphs WHERE kind = 'native' \
                 AND group_id IS NOT NULL AND state = 'active' \
                 AND (mirror_mode IN ('starting', 'async') OR mirror_state = 'building')",
                quote(&schema)
            ))
            .await
        {
            Ok(r) => r,
            // Before the catalog's graph migration 3: nothing to serve.
            Err(mysql_async::Error::Server(e)) if e.code == 1054 || e.code == 1146 => Vec::new(),
            Err(e) => return Err(sprig_graph::catalog::backend_error(e)),
        };
        drop(c);
        let maps = ShardMapStore::new(Arc::clone(&self.env.dc));
        let mut graphs = HashMap::new();
        for (graph, db, group) in rows {
            let (map, _) = maps.load(self.tenant, GroupId(group)).await?;
            let shards: Vec<ShardLoc> = map
                .ranges
                .iter()
                .map(|r| {
                    let schema = shard_schema(self.tenant, DbId(db), GroupId(group), r.shard);
                    ShardLoc {
                        shard: r.shard.0,
                        lo: r.lo,
                        epoch: r.epoch,
                        cluster: r.cluster.to_string(),
                        tables: Tables::of(&schema, graph),
                        schema,
                    }
                })
                .collect();
            graphs.insert(
                graph,
                Served {
                    layout: Arc::new(GraphShards {
                        graph,
                        db,
                        group,
                        version: map.version,
                        shards,
                        seq: String::new(),
                        mirrors: true,
                        mirror_async: true,
                        mirror_building: false,
                    }),
                },
            );
        }
        self.graphs = graphs;
        // A reader on every cluster of the DC.
        let clusters: HashSet<ClusterId> = self.env.pools.sweepable().await?.into_iter().collect();
        self.readers.retain(|c, (_, t)| {
            let keep = clusters.contains(c) && !t.is_cancelled();
            if !keep {
                t.cancel();
            }
            keep
        });
        for c in clusters {
            if self.readers.contains_key(&c) {
                continue;
            }
            // A reader starts again before the oldest branch it held.
            let from = match self.safe_pos(&c) {
                Some(p) => p,
                None => {
                    let p = self.env.clusters.position(&c).await?;
                    self.pos.insert(c.clone(), p.clone());
                    p
                }
            };
            self.held.retain(|h| h.0 != c);
            self.pos.insert(c.clone(), from.clone());
            let token = CancellationToken::new();
            let gen_ = self.next_gen;
            self.next_gen += 1;
            self.readers.insert(c.clone(), (gen_, token.clone()));
            tokio::spawn(read(
                Arc::clone(&self.env.clusters),
                self.tenant,
                c,
                gen_,
                from,
                tx.clone(),
                token,
            ));
        }
        Ok(())
    }

    /// Applies one take of the readers' items; whether a position moved.
    async fn handle(&mut self, items: Vec<Item>) -> Result<bool> {
        // Changes per graph, and each cluster's position after them.
        let mut changes: BTreeMap<u64, Vec<EdgeChange>> = BTreeMap::new();
        let mut moved: BTreeMap<ClusterId, GtidPos> = BTreeMap::new();
        for (cluster, gen_, it) in items {
            if self.readers.get(&cluster).is_none_or(|r| r.0 != gen_) {
                continue; // a stopped reader's
            }
            let item = match it {
                Ok(i) => i,
                Err(e) => {
                    // The reader ended; it is opened again at the position.
                    if let Some((_, t)) = self.readers.remove(&cluster) {
                        t.cancel();
                    }
                    tracing::warn!(%cluster, error = %e, "graph mirror reader ended");
                    continue;
                }
            };
            match item {
                SourceItem::Idle { pos } => {
                    moved.insert(cluster, pos);
                }
                SourceItem::Txn(t) => {
                    let after = t.after();
                    let take = |held: &mut Vec<Held>, x: &str| -> Vec<RawRow> {
                        match held.iter().position(|h| h.0 == cluster && h.1 == x) {
                            Some(i) => held.remove(i).3,
                            None => Vec::new(),
                        }
                    };
                    let rows = match &t.xa {
                        Some(XaMark::Prepare(x)) => {
                            if !t.apply {
                                self.held
                                    .push((cluster.clone(), x.clone(), t.before.clone(), t.rows));
                            }
                            Vec::new()
                        }
                        Some(XaMark::Commit(x)) => take(&mut self.held, x),
                        Some(XaMark::Rollback(x)) => {
                            take(&mut self.held, x);
                            Vec::new()
                        }
                        None if t.apply => Vec::new(),
                        None => t.rows,
                    };
                    for r in &rows {
                        if let Some((g, c)) = self.edge_change(r) {
                            changes.entry(g).or_default().push(c);
                        }
                    }
                    moved.insert(cluster, after);
                }
            }
        }
        for (graph, ch) in changes {
            self.apply(graph, &ch).await?;
        }
        let any = !moved.is_empty();
        for (c, p) in moved {
            self.pos.insert(c, p);
        }
        Ok(any)
    }

    /// An edge-table row of a served graph as a change.
    fn edge_change(&self, r: &RawRow) -> Option<(u64, EdgeChange)> {
        let graph: u64 = r.table.table.strip_prefix("__sprig_ge_")?.parse().ok()?;
        self.graphs.get(&graph)?;
        if !r.table.schema.starts_with(&format!("t{}_", self.tenant)) {
            return None;
        }
        let (img, live) = match r.kind {
            RowKind::Insert => (r.after.as_ref()?, true),
            RowKind::Delete => (r.before.as_ref()?, false),
            // An edge's ends and type never change.
            RowKind::Update => return None,
        };
        let col = |name: &str| -> Option<u64> {
            let i = r.table.columns.iter().position(|c| c == name)?;
            match img.get(i)? {
                BinValue::UInt(u) => Some(*u),
                #[allow(clippy::cast_sign_loss)] // BIGINT UNSIGNED without metadata
                BinValue::Int(v) => Some(*v as u64),
                _ => None,
            }
        };
        Some((
            graph,
            EdgeChange {
                id: col("id")?,
                src: col("src")?,
                dst: col("dst")?,
                rtype: u32::try_from(col("type")?).ok()?,
                src_rk: col("_sprig_rk")?,
                dst_rk: col("dst_rk")?,
                live,
            },
        ))
    }

    /// Applies `ch` to `graph`'s mirrors, again with a newer map when one
    /// of the shards changed epoch meanwhile.
    async fn apply(&mut self, graph: u64, ch: &[EdgeChange]) -> Result<()> {
        let mut conns = PoolConns(Arc::clone(&self.env.pools));
        let mut attempt = 0;
        loop {
            attempt += 1;
            let Some(served) = self.graphs.get(&graph).cloned() else {
                return Ok(()); // no longer served (dropped, or back to sync)
            };
            let r = mirror::apply_batch(&mut conns, &served.layout, ch, now_us()).await;
            let stale = match &r {
                Ok(a) => {
                    // The map now: re-read so a cutover meanwhile is seen.
                    self.reload(graph).await?;
                    let now = self.graphs.get(&graph).map(|s| Arc::clone(&s.layout));
                    now.is_some_and(|l| !mirror::still_current(&a.touched, &l))
                }
                Err(e) => {
                    let code = match &e.kind {
                        sprig_common::ErrorKind::Backend { errno, .. } => *errno,
                        _ => 0,
                    };
                    if attempt < 10 && matches!(code, 1146 | 1049 | 1205 | 1213 | 1047) {
                        self.reload(graph).await?;
                        tokio::time::sleep(Duration::from_millis(100 * attempt)).await;
                        true
                    } else {
                        return Err(r.err().unwrap_or_else(|| SprigError::internal("apply")));
                    }
                }
            };
            if !stale {
                if let Ok(a) = &r {
                    self.applied += a.upserts + a.deletes;
                    metrics::counter!("sprig_graph_mirror_applied_total").increment(a.upserts + a.deletes);
                }
                return Ok(());
            }
            if attempt >= 10 {
                return Err(SprigError::internal(format!(
                    "graph {graph}: the shard map kept changing under the mirror applier"
                )));
            }
        }
    }

    /// Reloads one graph's shard map.
    async fn reload(&mut self, graph: u64) -> Result<()> {
        let Some(s) = self.graphs.get(&graph).cloned() else {
            return Ok(());
        };
        let maps = ShardMapStore::new(Arc::clone(&self.env.dc));
        let (map, _) = maps.load(self.tenant, GroupId(s.layout.group)).await?;
        if map.version == s.layout.version {
            return Ok(());
        }
        let mut l = (*s.layout).clone();
        l.version = map.version;
        l.shards = map
            .ranges
            .iter()
            .map(|r| {
                let schema = shard_schema(self.tenant, DbId(l.db), GroupId(l.group), r.shard);
                ShardLoc {
                    shard: r.shard.0,
                    lo: r.lo,
                    epoch: r.epoch,
                    cluster: r.cluster.to_string(),
                    tables: Tables::of(&schema, graph),
                    schema,
                }
            })
            .collect();
        self.graphs.insert(
            graph,
            Served {
                layout: Arc::new(l),
            },
        );
        Ok(())
    }

    /// A position row per shard of every served graph.
    async fn write_positions(&mut self) -> Result<()> {
        if self.graphs.is_empty() {
            return Ok(());
        }
        let ep = self.env.locator.writer(self.tenant).await?;
        let schema = ep.store.schema().to_owned();
        let mut c = ep
            .pool
            .get_conn()
            .await
            .map_err(sprig_graph::catalog::backend_error)?;
        for (graph, s) in &self.graphs {
            let rows: Vec<(u32, String, u64, String)> = s
                .layout
                .shards
                .iter()
                .filter_map(|sh| {
                    let c = ClusterId::new(&sh.cluster).ok()?;
                    // A cluster without a reader yet has no position.
                    if !self.readers.contains_key(&c) {
                        return None;
                    }
                    let p = self.safe_pos(&c)?;
                    Some((sh.shard, sh.cluster.clone(), sh.epoch, p.to_string()))
                })
                .collect();
            if rows.len() == s.layout.shards.len() {
                mirror::write_pos(&mut c, &schema, *graph, &rows).await?;
            }
        }
        Ok(())
    }
}

/// One cluster's reader: its items into `tx` until cancelled or broken.
async fn read(
    clusters: Arc<dyn Clusters>,
    tenant: TenantId,
    cluster: ClusterId,
    gen_: u64,
    from: GtidPos,
    tx: mpsc::Sender<Item>,
    stop: CancellationToken,
) {
    let mut src: Box<dyn TxnSource> = match clusters.open(&cluster, tenant, &from).await {
        Ok(s) => s,
        Err(e) => {
            stop.cancel();
            let _ = tx.send((cluster, gen_, Err(e.to_string()))).await;
            return;
        }
    };
    loop {
        let it = tokio::select! {
            () = stop.cancelled() => return,
            it = src.next() => it,
        };
        let end = it.is_err();
        let item = (cluster.clone(), gen_, it.map_err(|e| e.to_string()));
        tokio::select! {
            () = stop.cancelled() => return,
            r = tx.send(item) => if r.is_err() { return; },
        }
        if end {
            stop.cancel();
            return;
        }
    }
}
