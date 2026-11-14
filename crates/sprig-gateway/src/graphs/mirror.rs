//! Asynchronous mirrors in the gateway: mirror modes and freshness.
//!
//! - **Mode changes.** A grouped graph's `graphs.mirror_mode` follows
//!   `graph.mirror` (resolved at the graph's database scope), moved by the
//!   gateway that runs a statement on it, at most every
//!   [`CHECK_EVERY`] per graph:
//!
//!   ```text
//!   want async: register the tenant's `graph_mirror` subscription (the
//!               controller then keeps one applier job for the tenant)
//!     sync     -> starting          (writers still write mirrors; the
//!                                    applier starts serving the graph)
//!     starting -> async             once the applier wrote a position row
//!                                   for every shard of the map after the
//!                                   change to `starting` (it serves the
//!                                   graph, so it sees every later write)
//!   want sync (or off):
//!     starting -> sync
//!     async    -> sync, building    (writers write mirrors again; readers
//!                                    scatter until the applier has applied
//!                                    everything up to the clusters'
//!                                    positions taken after the change)
//!     building -> ready             then; the positions are deleted
//!   ```
//!
//! - **Freshness** ([`CatalogGate`]). A read statement on an `async` graph
//!   passes a gate holding the session's causal target to the store; the
//!   store asks it once, at its first incoming hop. The gate reads the
//!   binlog position of each of the graph's clusters now, and polls the
//!   positions until they reach both that and the session's target, up to
//!   `graph.mirror_max_wait_ms`; a missing or stale row (the applier isn't
//!   serving the graph) answers `false` at once. So mirrors never hide an
//!   edge committed before the statement.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, Freshness, TenantCatalog};
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::catalog::{GraphCatalog, backend_error};
use sprig_graph::maria::GraphShards;
use sprig_graph::mirror::{self, MirrorGate};
use sprig_meta::{Tier, TierView};

use super::{GatewayGraphs, GraphRef};

/// How often one gateway looks at a graph's mode.
pub(crate) const CHECK_EVERY: Duration = Duration::from_secs(2);

use sprig_graph::mirror::SUB_NAME;

/// How long a drain (`async` back to `sync`) waits for the applier.
const DRAIN_WAIT: Duration = Duration::from_secs(120);

/// Per-process state of mode changes.
#[derive(Debug, Default)]
pub(crate) struct MirrorState {
    checked: Mutex<HashMap<(u64, u64), Instant>>,
    subscribed: Mutex<HashSet<u64>>,
    draining: Mutex<HashSet<(u64, u64)>>,
}

/// The gate of one read statement: the session's causal target per
/// cluster, the catalog's positions, a bounded wait.
pub(crate) struct CatalogGate {
    pub(crate) graphs: Arc<GatewayGraphs>,
    pub(crate) tenant: TenantId,
    pub(crate) graph: u64,
    pub(crate) targets: BTreeMap<String, String>,
    pub(crate) wait: Duration,
}

#[async_trait]
impl MirrorGate for CatalogGate {
    async fn fresh(&self, layout: &GraphShards) -> bool {
        let started = Instant::now();
        let r = self.poll(layout, started).await;
        let outcome = match r {
            Ok(true) => "ok",
            Ok(false) => "lag",
            Err(_) => "error",
        };
        metrics::counter!("sprig_graph_mirror_waits_total", "outcome" => outcome).increment(1);
        metrics::histogram!("sprig_graph_mirror_wait_seconds")
            .record(started.elapsed().as_secs_f64());
        r.unwrap_or(false)
    }
}

impl CatalogGate {
    async fn poll(&self, layout: &GraphShards, started: Instant) -> Result<bool> {
        let ep = self.graphs.svc.locator.writer(self.tenant).await?;
        let mut c = ep.pool.get_conn().await.map_err(backend_error)?;
        let schema = ep.store.schema().to_owned();
        let none = BTreeMap::new();
        // The target: every transaction committed on the shards' clusters
        // before the statement (their binlog positions now), and the
        // session's causal target, so the mirrors never hide a committed
        // edge. The first positions are read at the same time (one round
        // trip, not two): positions only grow, so ones read before the
        // target that reach it are fresh, and ones that don't are read
        // again.
        let target = async {
            let mut targets = self.targets.clone();
            let mut asked: HashSet<&str> = HashSet::new();
            for s in &layout.shards {
                if !asked.insert(s.cluster.as_str()) {
                    continue;
                }
                let cluster = ClusterId::new(&s.cluster)
                    .map_err(|e| SprigError::internal(format!("cluster id {}: {e}", s.cluster)))?;
                let pool = self.graphs.pool(&cluster).await?;
                let mut pc = pool.get_conn().await.map_err(backend_error)?;
                let now: Option<String> = pc
                    .query_first("SELECT @@gtid_binlog_pos")
                    .await
                    .map_err(backend_error)?;
                // Both positions: `reached` keeps each domain's highest.
                let t = targets.entry(s.cluster.clone()).or_default();
                if !t.is_empty() {
                    t.push(',');
                }
                t.push_str(&now.unwrap_or_default());
            }
            Ok::<_, SprigError>(targets)
        };
        let (targets, first) =
            futures::join!(target, mirror::read_pos(&mut c, &schema, self.graph));
        let targets = targets?;
        let mut rows = first?;
        // The first read also tells whether the applier serves the graph
        // at all (else scatter without waiting).
        loop {
            if mirror::fresh(&rows, layout, &targets) {
                return Ok(true);
            }
            // Not serving the graph (or behind a map change): no wait.
            if !mirror::fresh(&rows, layout, &none) || started.elapsed() >= self.wait {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
            rows = mirror::read_pos(&mut c, &schema, self.graph).await?;
        }
    }
}

impl GatewayGraphs {
    /// Moves `g`'s mirror mode toward `graph.mirror` (see the
    /// module docs). `true` when the catalog changed, so the caller
    /// resolves the graph again. Failures are logged, never the
    /// statement's.
    pub(crate) async fn follow_mirror_setting(
        self: &Arc<Self>,
        g: &GraphRef,
        cat: &TenantCatalog,
    ) -> bool {
        match self.follow_inner(g, cat).await {
            Ok(changed) => changed,
            Err(e) => {
                tracing::debug!(error = %e, graph = g.graph_id, "graph.mirror not followed");
                false
            }
        }
    }

    async fn follow_inner(self: &Arc<Self>, g: &GraphRef, cat: &TenantCatalog) -> Result<bool> {
        if g.def.kind != "native" || g.def.group_id.is_none() {
            return Ok(false);
        }
        let key = (g.tenant.0, g.graph_id);
        {
            let mut m = self
                .mirrors
                .checked
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if m.get(&key).is_some_and(|t| t.elapsed() < CHECK_EVERY) {
                return Ok(false);
            }
            m.insert(key, Instant::now());
        }
        let ctx = cat.setting_ctx(&self.svc.dc).with_db(g.db_id);
        let want = self
            .svc
            .settings
            .get(&sprig_graph::settings::MIRROR, &ctx, Some(&cat.settings));
        let want_async = want == "async";
        if want_async {
            self.subscribe_mirrors(g.tenant).await?;
        }
        let ready = g.def.mirrors;
        let next = match (want_async, g.def.mirror_mode.as_str()) {
            (true, "sync") => Some(("starting", ready)),
            (true, "starting") => {
                if self.applier_serves(g).await? {
                    Some(("async", true))
                } else {
                    None
                }
            }
            (false, "starting") => Some(("sync", ready)),
            (false, "async") => Some(("sync", false)),
            _ => None,
        };
        let changed = if let Some((mode, ready)) = next {
            self.set_mirror(g, mode, ready).await?;
            tracing::info!(graph = g.graph_id, tenant = %g.tenant, mode, ready, "graph mirror mode changed");
            true
        } else {
            false
        };
        // A drain: back to `sync`, readers scatter until the applier has
        // applied what was written under `async`.
        let drain = matches!(next, Some(("sync", false)))
            || (!want_async && g.def.mirror_mode == "sync" && !ready);
        if drain {
            self.spawn_drain(g.clone());
        }
        Ok(changed)
    }

    /// Writes `mirror_mode` and `mirror_state` in a versioned change.
    async fn set_mirror(&self, g: &GraphRef, mode: &str, ready: bool) -> Result<()> {
        let mut ch = self
            .svc
            .catalog
            .change(g.tenant, VersionKind::Catalog)
            .await?;
        let r = {
            let (store, tx) = ch.tx().parts();
            GraphCatalog::new(store.schema())
                .set_mirror(
                    tx,
                    g.graph_id,
                    mode,
                    if ready { "ready" } else { "building" },
                )
                .await
        };
        if let Err(e) = r {
            let _ = ch.rollback().await;
            return Err(e);
        }
        ch.commit().await?;
        Ok(())
    }

    /// Whether the applier wrote a live position for every shard of `g`'s
    /// map since the graph's mode last changed.
    async fn applier_serves(&self, g: &GraphRef) -> Result<bool> {
        let ep = self.svc.locator.writer(g.tenant).await?;
        let mut c = ep.pool.get_conn().await.map_err(backend_error)?;
        let schema = ep.store.schema().to_owned();
        let rows = mirror::read_pos_since_change(&mut c, &schema, g.graph_id).await?;
        Ok(mirror::fresh(&rows, &g.shards, &BTreeMap::new()))
    }

    /// Registers the tenant's `graph_mirror` subscription once per
    /// process (the controller keeps an applier job for each).
    async fn subscribe_mirrors(&self, tenant: TenantId) -> Result<()> {
        if self
            .mirrors
            .subscribed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&tenant.0)
        {
            return Ok(());
        }
        let home = self.svc.locator.home_of(tenant);
        let dc = self.svc.locator.dc_store();
        let store = if home == self.svc.dc {
            dc
        } else {
            TierView::over(&dc, Tier::Dc(home))
        };
        let subs = sprig_feed::subs::Subscriptions::new(store);
        let sub = sprig_feed::subs::Subscription {
            tenant,
            name: SUB_NAME.into(),
            kind: SUB_NAME.into(),
            ordering: "merged".into(),
            owner_ref: None,
            state: "active".into(),
            created_at_ms: u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis()),
            )
            .unwrap_or(0),
        };
        match subs.register(&sub).await {
            Ok(()) => {}
            Err(e) if e.code() == Some(SprigCode::FeedSubscriptionExists) => {}
            Err(e) => return Err(e),
        }
        self.mirrors
            .subscribed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant.0);
        Ok(())
    }

    /// Waits (in the background) until the applier has applied everything
    /// up to the shards' clusters' positions now, then marks the mirrors
    /// `ready` and deletes the positions.
    fn spawn_drain(self: &Arc<Self>, g: GraphRef) {
        let key = (g.tenant.0, g.graph_id);
        if !self
            .mirrors
            .draining
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key)
        {
            return;
        }
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let r = me.drain(&g).await;
            match &r {
                Ok(true) => tracing::info!(graph = g.graph_id, "graph mirrors drained: ready"),
                Ok(false) => tracing::warn!(
                    graph = g.graph_id,
                    "graph mirrors still building: the applier didn't catch up"
                ),
                Err(e) => {
                    tracing::warn!(graph = g.graph_id, error = %e, "graph mirror drain failed");
                }
            }
            me.mirrors
                .draining
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&key);
        });
    }

    async fn drain(&self, g: &GraphRef) -> Result<bool> {
        // Positions now, taken after the change to `sync` committed.
        let mut targets: BTreeMap<String, String> = BTreeMap::new();
        for s in &g.shards.shards {
            if targets.contains_key(&s.cluster) {
                continue;
            }
            let cluster = ClusterId::new(&s.cluster)
                .map_err(|e| SprigError::internal(format!("cluster id {}: {e}", s.cluster)))?;
            let pool = self.pool(&cluster).await?;
            let mut c = pool.get_conn().await.map_err(backend_error)?;
            let p: Option<String> = c
                .query_first("SELECT @@gtid_binlog_pos")
                .await
                .map_err(backend_error)?;
            targets.insert(s.cluster.clone(), p.unwrap_or_default());
        }
        let started = Instant::now();
        loop {
            let cat = self.catalog(g.tenant, Freshness::Latest).await?;
            let Some(now) = self.resolve(&cat, &g.db, &g.name).await? else {
                return Ok(true); // dropped
            };
            if now.def.mirror_mode != "sync" || now.def.mirrors {
                return Ok(true); // someone else finished, or async again
            }
            let ep = self.svc.locator.writer(g.tenant).await?;
            let mut c = ep.pool.get_conn().await.map_err(backend_error)?;
            let schema = ep.store.schema().to_owned();
            let rows = mirror::read_pos(&mut c, &schema, g.graph_id).await?;
            // The map may have changed since: compare against the targets
            // of the clusters it uses now (a new cluster's position was
            // read by the applier after the change, so any row will do).
            let t: BTreeMap<String, String> = now
                .shards
                .shards
                .iter()
                .filter_map(|s| {
                    targets
                        .get(&s.cluster)
                        .map(|p| (s.cluster.clone(), p.clone()))
                })
                .collect();
            if mirror::fresh(&rows, &now.shards, &t) {
                self.set_mirror(&now, "sync", true).await?;
                mirror::clear_pos(&mut c, &schema, g.graph_id).await?;
                return Ok(true);
            }
            if started.elapsed() > DRAIN_WAIT {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
