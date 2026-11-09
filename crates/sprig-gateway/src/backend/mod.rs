//! Backend connections (chapter 06, build steps 1-10): pools keyed by
//! backend account and node, session-state tracking and replay, pins,
//! writer selection, health checks and failure handling. Read routing is
//! in [`crate::reads`].
//!
//! # API for executors (the relay engine and the pass-through executor)
//!
//! One [`Backends`] per gateway (in `GatewayContext::backends`); one
//! [`BackendSession`] per client session. A statement is: classify, run,
//! stream, finish.
//!
//! ```text
//! // At login (decision D1: the target comes from sprig-auth's account):
//! let mut bs: BackendSession = backends.session(account.backend.clone(), SessionOpts {
//!     persona, found_rows, mariadb_caps, collation: handshake.collation, schema: handshake.db,
//!     ..SessionOpts::default()
//! });
//!
//! // Per statement (text protocol):
//! let info = StmtInfo::classify(sql);           // or a parser's StmtInfo
//! bs.query(&info, sql).await?;                  // routes, leases, syncs, waits; may retry
//! while let Some(p) = bs.next().await? {        // raw backend packets, classified
//!     relay(p);                                 // p.payload is the bytes as received
//! }
//! let outcome: StmtOutcome = bs.finish().await?; // tracking -> session, pins, captures, release
//!
//! // Prepared statements (binary protocol): the backend statement id is
//! // per connection; `build` gets it and returns the COM_STMT_EXECUTE payload.
//! bs.execute(&info, sql, &mut |stmt: &BackendStmt| build_execute(stmt)).await?;
//! // ... next() / finish() as above.
//!
//! // Any other command (COM_FIELD_LIST, COM_STMT_FETCH, COM_STMT_RESET, ...):
//! bs.command(&info, payload).await?;            // then next() / finish()
//!
//! // LOCAL INFILE (only when the client asked; PacketKind::LocalInfile):
//! bs.send_infile_data(chunk).await?; bs.finish_infile().await?;
//!
//! // Session-level:
//! bs.is_pinned(); bs.pins(); bs.thread_id(); bs.kill_query().await?;
//! bs.captured();  // warnings, FOUND_ROWS(), ROW_COUNT() answered by the gateway
//! bs.reset().await?;  // COM_RESET_CONNECTION / COM_CHANGE_USER
//! bs.close().await;   // session end
//! ```
//!
//! What [`BackendSession::query`] does (chapter 06's algorithms):
//!
//! 1. **Route.** A pinned session uses its held connection. A plain read
//!    ([`StmtClass::Read`]) of an unpinned session goes through
//!    [`reads::choose`](crate::reads::choose): a replica whose position
//!    covers the causal target, a replica with the wait-or-fail prefix, a
//!    Galera node (with `wsrep_sync_wait = 1` off the writer), or the
//!    writer. Everything else goes to the shard's writer
//!    ([`WriterSelector`]).
//! 2. **Lease** from the node's pool ([`BackendPools::acquire`]); a reader
//!    that can't be reached is skipped, a writer is retried to the deadline
//!    (39030); an exhausted pool fails with 39031. An idle connection the
//!    server closed (`KILL CONNECTION`, a restart) is discarded unused, so
//!    any statement, a write too, moves to another connection; a pinned
//!    session's held one fails with 1213 or 39033 without sending.
//! 3. **Sync** ([`state::sync_prefix`]): `USE`, `SET SESSION`, user
//!    variables the statement reads, `LAST_INSERT_ID`, then the wait
//!    prefix. Text statements send prefix and statement as one
//!    multi-statement and the prefix's OK packets are consumed here; other
//!    commands get the prefix as its own round trip. A wait timeout (1242)
//!    reruns the statement on the writer; a failed sync discards the
//!    connection and retries once.
//! 4. **First packet.** The statement's first response packet is read
//!    before `query` returns, so a plain read or a prepare whose node fails
//!    before any packet is retried once elsewhere.
//!
//! [`BackendSession::next`] maps a lost connection to chapter 06's errors:
//! 39032 during a commit or an autocommit write, 1213 inside a
//! transaction (rolled back), 39033 when other pinned state is lost, 39034
//! once part of a result was returned, 39030 otherwise.
//! [`BackendSession::finish`] applies session tracking (schema, allowed
//! variables, the commit GTID for read-your-writes, unexplained changes
//! pin `Untracked`), updates pins from the statement's effects and
//! `SERVER_STATUS_IN_TRANS`, and either keeps the connection (pinned) or
//! runs the captures (assigned user variables, `SHOW WARNINGS`,
//! `FOUND_ROWS()`) and returns it to its digest bucket.
//!
//! Names pass through unchanged (decision D2): the schema a client names
//! is the physical schema.

pub mod charset;
pub mod classify;
pub mod cluster;
pub mod conn;
pub mod health;
mod monitor;
pub mod pins;
pub mod pool;
mod session;
pub mod settings;
pub mod state;
mod txn;
pub mod writer;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

pub use classify::{Effect, StmtClass, StmtInfo};
pub use cluster::{ClusterSource, ClusterView, NodeKey, NodeRef, RegistryClusters, StaticClusters};
pub use conn::{BackendConn, BackendStmt, PreparedBudget, Warning};
pub use health::{NodeHealth, NodeRole, NodeStatus, NodeTable};
pub use pins::{PinReason, Pins};
pub use pool::{BackendLease, BackendPools, PoolKey, PoolStats, Release};
pub use session::{BackendSession, Captured, RouteKind, SessionOpts, StmtOutcome};
pub use settings::{BackendSettings, FixedSettings, LiveBackendSettings, PoolMode, SettingsSource};
pub use writer::{TopologyWriters, WriterSelector};

use sprig_auth::accounts::BackendTarget;
use sprig_common::ids::{ClusterId, DcId};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_proto_mysql::client::{ClientConfig, MysqlClient, TlsMode, TlsOptions};
use sprig_provision::credentials::{Credential, TlsMode as CredTls};
use tokio::task::JoinHandle;

use crate::reads::Watermarks;

/// TLS configurations by (skip verification, CA file).
type TlsKey = (bool, Option<PathBuf>);

/// The backend side of one gateway: pools, node states, writers, read
/// watermarks and the monitors behind them.
pub struct Backends {
    dc: DcId,
    clusters: Arc<dyn ClusterSource>,
    settings: Arc<dyn SettingsSource>,
    pools: BackendPools,
    nodes: Arc<NodeTable>,
    writers: TopologyWriters,
    watermarks: Watermarks,
    tls: Mutex<HashMap<TlsKey, Arc<rustls::ClientConfig>>>,
    monitors: Mutex<HashMap<ClusterId, JoinHandle<()>>>,
    monitor_targets: Mutex<HashMap<ClusterId, BackendTarget>>,
    budgets: Mutex<HashMap<NodeKey, Arc<PreparedBudget>>>,
    gateways_in_dc: u32,
    read_spread: std::sync::atomic::AtomicU64,
    shutdown: CancellationToken,
}

impl std::fmt::Debug for Backends {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backends")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

impl Backends {
    /// The backends of a gateway in `dc`, with clusters from `clusters` and
    /// settings from `settings`. Call [`Backends::start`] to run the reaper.
    #[must_use]
    pub fn new(
        dc: DcId,
        clusters: Arc<dyn ClusterSource>,
        settings: Arc<dyn SettingsSource>,
    ) -> Arc<Self> {
        let nodes = Arc::new(NodeTable::new());
        Arc::new(Self {
            dc,
            clusters,
            settings,
            pools: BackendPools::new(),
            writers: TopologyWriters::new(Arc::clone(&nodes)),
            nodes,
            watermarks: Watermarks::new(),
            tls: Mutex::new(HashMap::new()),
            monitors: Mutex::new(HashMap::new()),
            monitor_targets: Mutex::new(HashMap::new()),
            budgets: Mutex::new(HashMap::new()),
            // Registration (chapter 04 step 7) will say how many gateways
            // share a DC; until then this gateway takes the whole share.
            gateways_in_dc: 1,
            read_spread: std::sync::atomic::AtomicU64::new(0),
            shutdown: CancellationToken::new(),
        })
    }

    /// Starts the reaper: idle timeouts, lifetimes and lowered limits
    /// (checked every second and at once on a settings change), and the
    /// pool gauges. Stops at [`Backends::shutdown`].
    pub fn start(self: &Arc<Self>) -> JoinHandle<()> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let mut changes = me.settings.changes();
            loop {
                let settings_changed = async {
                    match changes.as_mut() {
                        Some(rx) => {
                            if rx.changed().await.is_err() {
                                std::future::pending::<()>().await;
                            }
                        }
                        None => std::future::pending::<()>().await,
                    }
                };
                tokio::select! {
                    () = me.shutdown.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                    () = settings_changed => {}
                }
                me.reap();
            }
        })
    }

    /// Runs one reaper pass and refreshes the pool gauges.
    pub fn reap(&self) {
        self.pools.reap(|c| self.limits(c));
        for (cluster, (idle, leased)) in self.pools.totals() {
            let c = cluster.to_string();
            metrics::gauge!("sprig_backend_pool_connections", "cluster" => c.clone(), "state" => "idle")
                .set(f64::from(idle));
            metrics::gauge!("sprig_backend_pool_connections", "cluster" => c, "state" => "leased")
                .set(f64::from(leased));
        }
    }

    /// Stops the reaper and the monitors.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
        for (_, h) in self
            .monitors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
        {
            h.abort();
        }
    }

    /// A session's backend side for a login whose statements go to
    /// `target` (decision D3: one cluster per login).
    #[must_use]
    pub fn session(self: &Arc<Self>, target: BackendTarget, opts: SessionOpts) -> BackendSession {
        BackendSession::new(Arc::clone(self), target, opts)
    }

    /// The pools.
    #[must_use]
    pub fn pools(&self) -> &BackendPools {
        &self.pools
    }

    /// The node states.
    #[must_use]
    pub fn nodes(&self) -> &NodeTable {
        &self.nodes
    }

    /// The settings in force for `cluster`.
    #[must_use]
    pub fn settings(&self, cluster: &ClusterId) -> Arc<BackendSettings> {
        self.settings.settings(Some(cluster))
    }

    /// The view of `cluster`.
    ///
    /// # Errors
    /// As [`ClusterSource::view`].
    pub async fn view(&self, cluster: &ClusterId) -> Result<Arc<ClusterView>> {
        self.clusters.view(cluster).await
    }

    /// The current writer of `cluster`'s shard `shard`.
    ///
    /// # Errors
    /// As [`ClusterSource::view`]; 39100 without a usable writer.
    pub async fn writer(
        &self,
        cluster: &ClusterId,
        shard: sprig_common::ids::ShardId,
    ) -> Result<NodeRef> {
        let view = self.view(cluster).await?;
        self.writers.writer(&view, shard)
    }

    /// The read watermark of an account on a cluster.
    #[must_use]
    pub fn watermark(&self, cluster: &ClusterId, account: &str) -> crate::reads::GtidSet {
        self.watermarks.get(cluster, account)
    }

    fn limits(&self, cluster: &ClusterId) -> pool::Limits {
        let s = self.settings(cluster);
        pool::Limits {
            per_pool: s.max_conns_per_account_node,
            per_node: s.max_conns_per_node,
            max_lifetime: s.max_lifetime,
            idle_timeout: s.idle_timeout,
        }
    }

    fn budget(&self, node: &NodeKey) -> Arc<PreparedBudget> {
        Arc::clone(
            self.budgets
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(node.clone())
                .or_insert_with(|| Arc::new(PreparedBudget::new(8191))),
        )
    }

    /// The node's prepared-statement budget (chapter 06 build step 9).
    #[must_use]
    pub fn prepared_budget(&self, node: &NodeKey) -> Arc<PreparedBudget> {
        self.budget(node)
    }

    /// TLS configuration for a credential's certificate policy, built once.
    fn tls_for(&self, cred: &Credential) -> Result<Arc<rustls::ClientConfig>> {
        let key = (cred.tls == CredTls::SkipVerify, cred.ca_file.clone());
        let mut cache = self.tls.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(c) = cache.get(&key) {
            return Ok(Arc::clone(c));
        }
        let c = TlsOptions {
            ca_file: key.1.clone(),
            skip_verify: key.0,
            client_cert: None,
        }
        .build()
        .map_err(|e| SprigError::new(SprigCode::ConfigInvalid, format!("backend TLS: {e}")))?;
        cache.insert(key, Arc::clone(&c));
        Ok(c)
    }

    /// A one-off connection to a node as `target` (monitors, `KILL`).
    async fn side_connection(
        &self,
        node: &NodeRef,
        target: &BackendTarget,
        timeout: Duration,
    ) -> Result<MysqlClient> {
        let cred = target.credential()?;
        let mut cfg = ClientConfig::new(cred.user.clone(), cred.password());
        cfg.tls = TlsMode::Required;
        cfg.tls_config = Some(self.tls_for(&cred)?);
        cfg.connect_timeout = timeout;
        cfg.multi_statements = false;
        cfg.attrs.push((
            "program_name".to_owned(),
            "sprig-gateway-monitor".to_owned(),
        ));
        MysqlClient::connect(&node.addr, &cfg)
            .await
            .map_err(|e| unavailable(format!("{}: {e}", node.addr)))
    }

    /// `KILL QUERY <thread_id>` on a node, through a side connection as
    /// `target` (the session's own connection is busy).
    ///
    /// # Errors
    /// 39030 if the node can't be reached; the server's error.
    pub async fn kill_query(
        &self,
        node: &NodeKey,
        target: &BackendTarget,
        thread_id: u64,
    ) -> Result<()> {
        let view = self.view(&node.cluster).await?;
        let n = view
            .node(&node.node)
            .cloned()
            .ok_or_else(|| unavailable(format!("no node {node}")))?;
        let mut c = self
            .side_connection(&n, target, Duration::from_secs(2))
            .await?;
        let r = c.query_drop(&format!("KILL QUERY {thread_id}")).await;
        c.quit().await;
        r.map_err(|e| match e.server_error() {
            Some(err) => SprigError::backend(err.code, err.sqlstate, err.message.clone()),
            None => unavailable(e.to_string()),
        })
    }

    /// A failure on real traffic counts at once (chapter 06).
    fn note_failure(&self, node: &NodeKey, kind: &'static str) {
        metrics::counter!("sprig_backend_failures_total", "cluster" => node.cluster.to_string(), "kind" => kind)
            .increment(1);
        let to_down = self.settings(&node.cluster).health_failures_to_down;
        if let Some(h) = self.nodes.record_failure(node, to_down) {
            self.on_health_change(node, h);
        }
    }

    fn on_health_change(&self, node: &NodeKey, h: NodeHealth) {
        let up = if h == NodeHealth::Down { 0.0 } else { 1.0 };
        metrics::gauge!("sprig_backend_node_up", "cluster" => node.cluster.to_string(), "node" => node.node.to_string())
            .set(up);
        match h {
            NodeHealth::Down => {
                tracing::warn!(event = "backend.node_down", node = %node, "backend node down");
                self.pools.close_idle(node);
            }
            NodeHealth::Up => {
                tracing::info!(event = "backend.node_up", node = %node, "backend node up");
            }
            NodeHealth::Suspect => tracing::info!(node = %node, "backend node suspect"),
        }
    }

    /// Starts the monitor of `view`'s cluster (health checks and position
    /// polling) the first time a session uses it, connecting as `target`
    /// (interim: the first login's backend account; chapter 06 uses
    /// `sprig_gw`, which arrives with the catalog).
    fn ensure_monitor(self: &Arc<Self>, view: &Arc<ClusterView>, target: &BackendTarget) {
        let mut mons = self.monitors.lock().unwrap_or_else(PoisonError::into_inner);
        if mons.get(&view.id).is_some_and(|h| !h.is_finished()) {
            return;
        }
        self.monitor_targets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(view.id.clone(), target.clone());
        let h = tokio::spawn(monitor::run(Arc::clone(self), view.id.clone()));
        mons.insert(view.id.clone(), h);
    }

    fn monitor_target(&self, cluster: &ClusterId) -> Option<BackendTarget> {
        self.monitor_targets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            .cloned()
    }
}

/// 39030.
pub(crate) fn unavailable(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::BackendUnavailable, msg)
}
