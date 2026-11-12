//! A logged-in Bolt session (chapter 43, "Bolt front end"): statements,
//! transactions, bookmarks and replica reads.
//!
//! - **Statements.** A `RUN` is parsed, its graph resolved (the default
//!   graph made on its first write), chapter 29's table actions checked,
//!   and run on a `sprig_ctl` connection of the tenant's cluster: one
//!   MariaDB transaction per auto-commit `RUN`, or the explicit
//!   transaction's. Reads whose rows don't depend on each other stream
//!   ([`super::stream`]); the rest are computed whole, within
//!   `bolt.max_buffered_result_mb` (39918).
//! - **Bookmarks** (decision G9's follow-up). After a commit that wrote,
//!   the session adds the connection's `@@last_gtid` to its causal target
//!   (GTID positions per cluster) and answers a signed bookmark carrying
//!   the whole target ([`sprig_graph::bookmark`]). Bookmarks a driver
//!   passes (`BEGIN` or `RUN`'s `bookmarks`) are checked (39917 for a bad
//!   or foreign one) and join the target.
//! - **Replica reads** (chapter 06's read-your-writes). A read in `mode:
//!   "r"` on a primary with replicas goes to a replica, taking turns, after
//!   `MASTER_GTID_WAIT` for the target within `reads.gtid_wait_timeout_ms`;
//!   a replica that doesn't catch up in time, or fails, leaves the read on
//!   the writer, as does `reads.routing = writer`. Writes, and reads without
//!   `mode: "r"`, use the writer.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_auth::authz::Action;
use sprig_auth::mysql::Principal;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::ids::{DbId, GroupId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::access::{FilteredStore, Visible, Writable};
use sprig_graph::ast::{Clause, Query};
use sprig_graph::bookmark::{self, Bookmark};
use sprig_graph::catalog::backend_error;
use sprig_graph::eval::Params;
use sprig_graph::maria::{GraphShards, MariaStore, ShardConns, check_deleted};
use sprig_graph::mirror::MirrorGate;
use sprig_graph::store::GraphStore;
use sprig_proto_bolt::PackValue;
use sprig_proto_bolt::server::{BoltFailure, BoltSession, QueryResult};
use sprig_txn::{BeginOptions, TxnCoordinator};
use tokio::sync::{mpsc, oneshot};

use super::stream::{CHANNEL_ROWS, ChanSink, Msg, Start, summary};
use super::{
    DEFAULT_GRAPH, ISOLATION, LOGICAL_DATABASE, MAX_BUFFERED_RESULT_MB, failure, from_pack, refuse,
    to_pack,
};
use crate::graphs::mirror::CatalogGate;
use crate::graphs::txn::{CoordConns, GraphBackend, ReadConns, Touched, check_epochs};
use crate::graphs::{GatewayGraphs, GraphRef, RouteHolds};
use crate::reads::GtidSet;

/// What an explicit transaction runs on.
enum TxConns {
    /// `mode: "r"`: read-only transactions per cluster.
    Read(ReadConns),
    /// Chapter 18's coordinator.
    Write(Box<TxnCoordinator<GraphBackend>>),
}

/// An open transaction: its connections (away while a streamed result
/// holds them), the graphs it wrote and the nodes it deleted.
struct Tx {
    conns: Option<TxConns>,
    /// The connections, coming back from a streamed result.
    back: Option<oneshot::Receiver<TxConns>>,
    db: Option<String>,
    timeout: Option<Duration>,
    /// `BEGIN`'s `tx_metadata`, for `CALL tx.getMetaData()`.
    metadata: Option<BTreeMap<String, sprig_graph::value::Value>>,
    deleted: Vec<(Arc<GraphShards>, Vec<u64>)>,
    /// Graphs written, with the shards (and their epochs) written.
    touched: Vec<(GraphRef, Vec<Touched>)>,
    /// Statements run so far.
    ran: u32,
}

/// The session's causal target and last bookmark.
#[derive(Debug, Default)]
struct Causal {
    /// GTID positions per cluster.
    gtids: BTreeMap<String, GtidSet>,
    last: Option<String>,
}

pub(crate) struct GwSession {
    principal: Principal,
    /// The gateway session table's entry (`SHOW PROCESSLIST`, `KILL`).
    reg: Option<crate::session::Session>,
    graphs: Arc<GatewayGraphs>,
    backend: Arc<GraphBackend>,
    dc: DcId,
    tx: Option<Tx>,
    causal: Arc<Mutex<Causal>>,
    advertise: String,
    ttl: u64,
}

/// A statement ready to run.
struct Prepared {
    q: Query,
    p: Params,
    g: GraphRef,
    cat: Arc<TenantCatalog>,
    budgets: sprig_graph::Budgets,
    read_mode: bool,
    /// The auto-commit statement's `tx_timeout` (an explicit
    /// transaction's is in its `Tx`).
    timeout: Option<Duration>,
    /// Per-label and per-type grants, when the graph-wide ones don't give
    /// `SELECT` (decision G42) or a write action (round 5, G50).
    visible: Option<Access>,
}

/// What per-label and per-type grants allow a statement: reads filtered
/// by `vis` (all when `None`), writes checked against `writes` (all when
/// `None`).
#[derive(Debug, Clone, Default)]
struct Access {
    vis: Option<Visible>,
    writes: Option<Writable>,
}

/// Runs `q` on `store`, filtered by `vis` when given.
#[allow(clippy::many_single_char_names)] // the executor's names
async fn run_on(
    store: &mut dyn GraphStore,
    vis: Option<Access>,
    q: &Query,
    p: &Params,
    b: &sprig_graph::Budgets,
) -> Result<sprig_graph::Output> {
    // Lazy: a read's row error comes after the rows before it (R8 round 8).
    match vis {
        None => sprig_graph::exec::run_lazy(store, q, p, b).await,
        Some(v) => {
            let mut f = FilteredStore::guarded(store, v.vis, v.writes).await?;
            sprig_graph::exec::run_lazy(&mut f, q, p, b).await
        }
    }
}

/// [`run_on`] for a streamed read.
#[allow(clippy::many_single_char_names)] // the executor's names
async fn stream_on(
    store: &mut dyn GraphStore,
    vis: Option<Access>,
    q: &Query,
    p: &Params,
    b: &sprig_graph::Budgets,
    sink: &mut dyn sprig_graph::RowSink,
) -> Result<sprig_graph::Output> {
    match vis {
        None => sprig_graph::run_stream(store, q, p, b, sink).await,
        Some(v) => {
            let mut f = FilteredStore::guarded(store, v.vis, v.writes).await?;
            sprig_graph::run_stream(&mut f, q, p, b, sink).await
        }
    }
}

enum Prep {
    /// Answered without storage (the default graph before its first
    /// write).
    Done(QueryResult),
    Run(Box<Prepared>),
}

/// The privileges a statement needs.
fn actions(q: &Query) -> Vec<Action> {
    let mut a = vec![];
    let mut add = |x: Action| {
        if !a.contains(&x) {
            a.push(x);
        }
    };
    for part in &q.parts {
        for c in &part.clauses {
            match c {
                Clause::Match { .. }
                | Clause::Return(_)
                | Clause::With(_)
                | Clause::Unwind { .. }
                | Clause::Call { .. } => add(Action::Select),
                Clause::Create(_) | Clause::Merge { .. } => {
                    add(Action::Insert);
                    add(Action::Select);
                }
                Clause::Set(_) | Clause::Remove(_) => add(Action::Update),
                Clause::Delete { .. } => add(Action::Delete),
                Clause::Schema(_) => add(Action::Index),
                Clause::Subquery { query, .. } => {
                    for x in actions(query) {
                        add(x);
                    }
                }
                Clause::Foreach { body, .. } => {
                    let inner = Query {
                        explain: sprig_graph::ast::Explain::None,
                        parts: vec![sprig_graph::ast::SingleQuery {
                            clauses: body.clone(),
                        }],
                        union_all: vec![],
                    };
                    for x in actions(&inner) {
                        add(x);
                    }
                }
            }
        }
    }
    a
}

fn is_default(db: Option<&str>) -> bool {
    db.is_none_or(str::is_empty)
}

fn has_writes(q: &Query) -> bool {
    actions(q).iter().any(|a| *a != Action::Select)
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(0))
}

fn timeout_of(extra: &PackValue) -> Option<Duration> {
    extra
        .get("tx_timeout")
        .and_then(PackValue::as_int)
        .filter(|t| *t > 0)
        .map(|t| Duration::from_millis(u64::try_from(t).unwrap_or(0)))
}

/// `tx_metadata` of `BEGIN` or an auto-commit `RUN`: a map, else none
/// (values that aren't Cypher values leave it out; it is still audited).
fn metadata_of(extra: &PackValue) -> Option<BTreeMap<String, sprig_graph::value::Value>> {
    match extra.get("tx_metadata").map(from_pack) {
        Some(Ok(sprig_graph::value::Value::Map(m))) => Some(m),
        _ => None,
    }
}

/// A lock wait that ended because the transaction's `tx_timeout` passed
/// (the branch's `innodb_lock_wait_timeout` is set from it): Neo4j's
/// `LockClientStopped`, a client error, as Neo4j answers it, rather than
/// the transient 1205 drivers would retry.
fn timed_out_failure(e: &SprigError, timeout: Option<Duration>) -> BoltFailure {
    match (&e.kind, timeout) {
        (
            sprig_common::ErrorKind::Backend { errno: 1205, .. }
            | sprig_common::ErrorKind::Standard { errno: 1205, .. },
            Some(t),
        ) => BoltFailure::new(
            "Neo.ClientError.Transaction.LockClientStopped",
            format!(
                "The transaction has been terminated: it waited for a lock past its timeout of {} ms (tx_timeout)",
                t.as_millis()
            ),
        ),
        // The time budget that fired was the tx_timeout (it caps
        // graph.max_time_ms): Neo4j's transaction timeout.
        (sprig_common::ErrorKind::Sprig(SprigCode::GraphBudgetExceeded), Some(t))
            if e.message
                .ends_with(&format!("(time): graph.max_time_ms = {}", t.as_millis())) =>
        {
            BoltFailure::new(
                "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration",
                format!(
                    "The transaction has been terminated: it ran past its timeout of {} ms (tx_timeout)",
                    t.as_millis()
                ),
            )
        }
        _ => failure(e),
    }
}

/// `EXPLAIN`'s or `PROFILE`'s steps as Neo4j's plan map: a chain of
/// operators (`operatorType`, `args` with the step's text in `Details`,
/// `identifiers`, `children`) under `ProduceResults`, the last step on top.
/// A profile adds `rows` (the result's, on `ProduceResults`) and `dbHits`
/// (0: SprigDB doesn't count store reads per operator).
fn plan_pack(steps: &[String], columns: &[String], rows: Option<usize>) -> PackValue {
    let op = |kind: &str, details: &str, idents: &[String], child: Option<PackValue>, n: usize| {
        let mut m = vec![
            ("operatorType", PackValue::str(kind)),
            (
                "args",
                PackValue::map(vec![
                    ("Details", PackValue::str(details)),
                    ("planner", PackValue::str("SPRIG")),
                    ("runtime", PackValue::str("SPRIG")),
                ]),
            ),
            (
                "identifiers",
                PackValue::List(idents.iter().cloned().map(PackValue::String).collect()),
            ),
            ("children", PackValue::List(child.into_iter().collect())),
        ];
        if rows.is_some() {
            m.push(("rows", PackValue::Int(i64::try_from(n).unwrap_or(i64::MAX))));
            m.push(("dbHits", PackValue::Int(0)));
        }
        PackValue::map(m)
    };
    let mut child = None;
    for s in steps {
        let kind = s.split([':', ' ']).next().unwrap_or("Step");
        child = Some(op(kind, s, &[], child, 0));
    }
    op(
        "ProduceResults",
        &columns.join(", "),
        columns,
        child,
        rows.unwrap_or(0),
    )
}

/// A notification as Bolt's summary map.
fn notification_pack(n: &sprig_graph::exec::Notification) -> PackValue {
    PackValue::map(vec![
        ("code", PackValue::str(n.code)),
        ("title", PackValue::str(n.title)),
        ("description", PackValue::String(n.description.clone())),
        ("severity", PackValue::str(n.severity)),
        ("category", PackValue::str(n.category)),
    ])
}

/// A notification as a GQL status object (Bolt 5.5 on), as Neo4j 5.26
/// sends it.
fn status_pack(n: &sprig_graph::exec::Notification) -> PackValue {
    PackValue::map(vec![
        ("gql_status", PackValue::str(n.gql_status)),
        ("neo4j_code", PackValue::str(n.code)),
        (
            "status_description",
            PackValue::String(n.status_description.clone()),
        ),
        (
            "diagnostic_record",
            PackValue::map(vec![
                ("_classification", PackValue::str(n.category)),
                ("_severity", PackValue::str(n.severity)),
                (
                    "_position",
                    PackValue::map(vec![
                        ("offset", PackValue::Int(0)),
                        ("line", PackValue::Int(1)),
                        ("column", PackValue::Int(1)),
                    ]),
                ),
            ]),
        ),
        ("description", PackValue::String(n.description.clone())),
        ("title", PackValue::str(n.title)),
    ])
}

fn read_mode(extra: &PackValue) -> bool {
    extra.get("mode").and_then(PackValue::as_str) == Some("r")
}

fn lost_conn() -> SprigError {
    SprigError::new(
        SprigCode::BoltProtocol,
        "the transaction's connection was lost by a failed statement; roll back",
    )
}

impl GwSession {
    pub(crate) fn new(
        principal: Principal,
        graphs: Arc<GatewayGraphs>,
        dc: DcId,
        advertise: String,
        ttl: u64,
    ) -> Self {
        Self {
            principal,
            reg: None,
            backend: Arc::new(GraphBackend::new(Arc::clone(&graphs))),
            graphs,
            dc,
            tx: None,
            causal: Arc::new(Mutex::new(Causal::default())),
            advertise,
            ttl,
        }
    }

    /// The logged-in principal.
    pub(super) fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Joins the gateway's session table.
    pub(crate) fn attach(&mut self, s: crate::session::Session) {
        self.reg = Some(s);
    }

    fn tenant(&self) -> TenantId {
        self.principal.tenant.unwrap_or(TenantId(0))
    }

    fn causal(&self) -> std::sync::MutexGuard<'_, Causal> {
        self.causal.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `(logical db, graph)` of a Bolt database name.
    fn split_db(&self, db: Option<&str>, cat: &TenantCatalog) -> (String, String) {
        let ctx = cat.setting_ctx(&self.dc);
        let s = &self.graphs.svc.settings;
        let logical = s
            .get(&LOGICAL_DATABASE, &ctx, Some(&cat.settings))
            .into_owned();
        match db.filter(|d| !d.is_empty()) {
            Some(d) => match d.split_once('.') {
                Some((a, b)) => (a.to_owned(), b.to_owned()),
                None => (logical, d.to_owned()),
            },
            None => (
                logical,
                s.get(&DEFAULT_GRAPH, &ctx, Some(&cat.settings))
                    .into_owned(),
            ),
        }
    }

    /// Checks the bookmarks in `extra` and adds them to the target.
    ///
    /// # Errors
    /// 39917 for a bad or foreign bookmark.
    async fn take_bookmarks(&self, extra: &PackValue) -> Result<()> {
        let list: Vec<&str> = extra
            .get("bookmarks")
            .and_then(PackValue::as_list)
            .unwrap_or_default()
            .iter()
            .filter_map(PackValue::as_str)
            .collect();
        if list.is_empty() {
            return Ok(());
        }
        let tenant = self.tenant();
        let key = self.graphs.bookmark_key(tenant).await?;
        let mut got: Vec<Bookmark> = Vec::with_capacity(list.len());
        for b in list {
            got.push(bookmark::decode(b, tenant.0, &key)?);
        }
        let mut c = self.causal();
        for b in got {
            for (cluster, g) in b.gtids {
                let set = GtidSet::parse(&g).map_err(|_| {
                    SprigError::new(SprigCode::BoltBookmarkInvalid, "invalid bookmark: bad GTID")
                })?;
                c.gtids.entry(cluster).or_default().union(&set);
            }
        }
        Ok(())
    }

    /// After a commit: the positions of the branches that wrote join the
    /// target, and the bookmark of the whole target.
    async fn after_commit(&self) -> Result<String> {
        for (cluster, g) in self.backend.take_positions() {
            if let Ok(set) = GtidSet::parse(&g) {
                self.causal().gtids.entry(cluster).or_default().union(&set);
            }
        }
        let key = self.graphs.bookmark_key(self.tenant()).await?;
        let mut c = self.causal();
        let b = Bookmark {
            tenant: self.tenant().0,
            hlc: now_us(),
            gtids: c
                .gtids
                .iter()
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect(),
        };
        let text = bookmark::encode(&b, &key);
        c.last = Some(text.clone());
        Ok(text)
    }

    /// `bolt.isolation` as SQL.
    fn isolation(&self, cat: &TenantCatalog) -> &'static str {
        let iso = self.graphs.svc.settings.get(
            &ISOLATION,
            &cat.setting_ctx(&self.dc),
            Some(&cat.settings),
        );
        if iso == "repeatable_read" {
            "REPEATABLE READ"
        } else {
            "READ COMMITTED"
        }
    }

    /// Read-only transactions on every cluster of `g` missing from `rc`:
    /// replicas for `mode: "r"` (after the causal wait), else writers.
    async fn open_reads(
        &self,
        rc: &mut ReadConns,
        g: &GraphRef,
        read_mode: bool,
        cat: &TenantCatalog,
    ) -> Result<()> {
        for c in rc.missing(&g.shards) {
            let cluster = ClusterId::new(&c)
                .map_err(|e| SprigError::internal(format!("cluster id {c}: {e}")))?;
            let conn = if read_mode {
                self.read_conn(&cluster, cat).await?
            } else {
                self.writer_conn(&cluster, true, cat).await?
            };
            rc.conns.insert(c, conn);
        }
        Ok(())
    }

    /// Chapter 18's `txn.*` settings for the tenant (round 5:
    /// `txn.non_xa_policy = sprig_2pc` holds graph writes' branches on
    /// clusters without XA, as SQL sessions do).
    fn txn_config(&self, cat: &TenantCatalog) -> sprig_txn::TxnConfig {
        let ctx = cat.setting_ctx(&self.dc);
        let s = &self.graphs.svc.settings;
        sprig_txn::TxnConfig::resolve_with(
            &ctx,
            |k, cx| s.get(k, cx, Some(&cat.settings)),
            |k, cx| s.get(k, cx, Some(&cat.settings)).into_owned(),
            "",
        )
    }

    /// The mirror gate of a read statement on `g` (round 5): `None` unless
    /// the applier keeps `g`'s mirrors.
    fn gate(&self, g: &GraphRef, cat: &TenantCatalog) -> Option<Arc<dyn MirrorGate>> {
        if !g.shards.mirror_async {
            return None;
        }
        let ctx = cat.setting_ctx(&self.dc).with_db(g.db_id);
        let wait = self.graphs.svc.settings.get(
            &sprig_graph::settings::MIRROR_MAX_WAIT_MS,
            &ctx,
            Some(&cat.settings),
        );
        let targets = self
            .causal()
            .gtids
            .iter()
            .map(|(c, s)| (c.clone(), s.to_string()))
            .collect();
        Some(Arc::new(CatalogGate {
            graphs: Arc::clone(&self.graphs),
            tenant: g.tenant,
            graph: g.graph_id,
            targets,
            wait: Duration::from_millis(wait),
        }))
    }

    /// Waits while the mover holds a range of `g`'s map (a cutover), then
    /// answers `g` with the map after it.
    async fn after_holds(&self, g: GraphRef) -> Result<GraphRef> {
        let mut g = g;
        let started = Instant::now();
        while let Some(until) = g.held_until {
            let now = u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis()),
            )
            .unwrap_or(u64::MAX);
            if now > until.saturating_add(5_000) || started.elapsed() > Duration::from_secs(30) {
                return Err(SprigError::new(
                    SprigCode::ShardMapChanged,
                    "the graph's shards are held by a split, merge or move; retry",
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            g = self.graphs.refresh(&g).await?;
        }
        Ok(g)
    }

    /// Starts a transaction on `c` at `bolt.isolation`.
    async fn start_tx(
        &self,
        c: &mut mysql_async::Conn,
        read_only: bool,
        cat: &TenantCatalog,
    ) -> Result<()> {
        let iso = self.graphs.svc.settings.get(
            &ISOLATION,
            &cat.setting_ctx(&self.dc),
            Some(&cat.settings),
        );
        let level = if iso == "repeatable_read" {
            "REPEATABLE READ"
        } else {
            "READ COMMITTED"
        };
        c.query_drop(format!("SET TRANSACTION ISOLATION LEVEL {level}"))
            .await
            .map_err(backend_error)?;
        c.query_drop(if read_only {
            "START TRANSACTION READ ONLY"
        } else {
            "START TRANSACTION"
        })
        .await
        .map_err(backend_error)
    }

    /// A transaction on the writer of `cluster`.
    async fn writer_conn(
        &self,
        cluster: &ClusterId,
        read_only: bool,
        cat: &TenantCatalog,
    ) -> Result<mysql_async::Conn> {
        let pool = self.graphs.session_pool(cluster).await?;
        let mut c = pool.get_conn().await.map_err(backend_error)?;
        self.start_tx(&mut c, read_only, cat).await?;
        Ok(c)
    }

    /// A read-only transaction for a `mode: "r"` read: on a replica that
    /// has the session's causal target, else on the writer.
    async fn read_conn(
        &self,
        cluster: &ClusterId,
        cat: &TenantCatalog,
    ) -> Result<mysql_async::Conn> {
        let ctx = cat.setting_ctx(&self.dc);
        let s = &self.graphs.svc.settings;
        let routing = s.get(&crate::reads::settings::ROUTING, &ctx, Some(&cat.settings));
        if routing != "writer"
            && let Ok(Some(pool)) = self.graphs.replica_pool(cluster).await
        {
            let target = self
                .causal()
                .gtids
                .get(cluster.as_str())
                .cloned()
                .unwrap_or_default();
            let wait = Duration::from_millis(s.get(
                &crate::reads::settings::GTID_WAIT_TIMEOUT_MS,
                &ctx,
                Some(&cat.settings),
            ));
            let outcome = match pool.get_conn().await {
                Err(e) => Err(("error", e.to_string())),
                Ok(mut c) if target.is_empty() => match self.start_tx(&mut c, true, cat).await {
                    Ok(()) => Ok((c, "none")),
                    Err(e) => Err(("error", e.to_string())),
                },
                Ok(mut c) => {
                    let r: std::result::Result<Option<Option<i64>>, _> = c
                        .exec_first(
                            "SELECT MASTER_GTID_WAIT(?, ?)",
                            (target.to_string(), wait.as_secs_f64()),
                        )
                        .await;
                    match r {
                        Ok(Some(Some(0))) => match self.start_tx(&mut c, true, cat).await {
                            Ok(()) => Ok((c, "ok")),
                            Err(e) => Err(("error", e.to_string())),
                        },
                        Ok(Some(Some(_))) => Err(("timeout", String::new())),
                        Ok(_) => Err(("error", "no replication position".into())),
                        Err(e) => Err(("error", e.to_string())),
                    }
                }
            };
            match outcome {
                Ok((c, o)) => {
                    metrics::counter!("sprig_bolt_bookmark_waits_total", "outcome" => o)
                        .increment(1);
                    return Ok(c);
                }
                Err((o, why)) => {
                    metrics::counter!("sprig_bolt_bookmark_waits_total", "outcome" => o)
                        .increment(1);
                    tracing::debug!(%cluster, outcome = o, %why, "Bolt read stays on the writer");
                }
            }
        }
        self.writer_conn(cluster, true, cat).await
    }

    /// The open transaction's connections, back from a streamed result if
    /// one had them.
    async fn tx_conns(&mut self) -> Result<&mut TxConns> {
        let t = self
            .tx
            .as_mut()
            .ok_or_else(|| SprigError::internal("no transaction"))?;
        if t.conns.is_none() {
            let back = t.back.take().ok_or_else(lost_conn)?;
            t.conns = Some(back.await.map_err(|_| lost_conn())?);
        }
        t.conns.as_mut().ok_or_else(lost_conn)
    }

    #[allow(clippy::many_single_char_names, clippy::too_many_lines)] // q, p, g; parse, resolve, authorize, budgets
    async fn prepare(
        &mut self,
        query: &str,
        params: &PackValue,
        extra: &PackValue,
    ) -> Result<Prep> {
        if extra
            .get("imp_user")
            .is_some_and(|v| !matches!(v, PackValue::Null))
        {
            return Err(SprigError::new(
                SprigCode::BoltImpersonation,
                "impersonation (imp_user) is not supported",
            ));
        }
        let q = sprig_graph::parser::parse(query)?;
        let mut p = Params::new();
        for (k, v) in params.as_map().unwrap_or_default() {
            p.insert(k.clone(), from_pack(v)?);
        }
        if self.tx.is_none() {
            self.take_bookmarks(extra).await?;
        }
        let tenant = self.tenant();
        let cat = self.graphs.catalog(tenant, Freshness::Latest).await?;
        let db_req: Option<String> = match &self.tx {
            Some(t) => t.db.clone(),
            None => extra
                .get("db")
                .and_then(PackValue::as_str)
                .map(str::to_owned),
        };
        let (db, name) = self.split_db(db_req.as_deref(), &cat);
        let grants = self.graphs.grants(&cat, self.principal.user_id);
        let mut graph = self.graphs.resolve(&cat, &db, &name).await?;
        let mut cat = cat;
        if graph.is_none() {
            if !(is_default(db_req.as_deref()) && has_writes(&q)) {
                if is_default(db_req.as_deref()) {
                    // The default graph before its first write: empty.
                    let mut mem = sprig_graph::mem::MemStore::new(1);
                    let out =
                        sprig_graph::exec::run(&mut mem, &q, &p, &sprig_graph::Budgets::default())
                            .await?;
                    return Ok(Prep::Done(self.result(out, &name, &cat)?));
                }
                return Err(SprigError::new(
                    SprigCode::GraphNotFound,
                    format!("Graph not found: {db}.{name}"),
                ));
            }
            let lctn = self.graphs.lctn(&cat);
            let db_row = cat
                .database(&sprig_catalog::ddl::database_key(&db, lctn))
                .map(|r| r.db_id)
                .ok_or_else(|| {
                    SprigError::new(
                        SprigCode::GraphNotFound,
                        format!("Database '{db}' does not exist"),
                    )
                })?;
            if !self
                .graphs
                .allows(grants.as_deref(), &cat, &db, db_row, None, Action::Create)
            {
                return Err(SprigError::standard(
                    1142,
                    *b"42000",
                    format!(
                        "CREATE command denied to user '{}' for graph '{db}.{name}'",
                        self.principal.user
                    ),
                ));
            }
            let (g, v) = self.graphs.create(tenant, &db, &name, true).await?;
            cat = self
                .graphs
                .catalog(tenant, Freshness::Cached { min_version: v })
                .await?;
            graph = Some(g);
        }
        let Some(mut g) = graph else {
            return Err(SprigError::internal("graph missing"));
        };
        // Round 5: the graph's mirror mode follows `graph.mirror`.
        if self.graphs.follow_mirror_setting(&g, &cat).await {
            cat = self.graphs.catalog(tenant, Freshness::Latest).await?;
            if let Some(now) = self.graphs.resolve(&cat, &g.db, &g.name).await? {
                g = now;
            }
        }
        let mut visible: Option<Access> = None;
        for a in actions(&q) {
            if !self
                .graphs
                .allows(grants.as_deref(), &cat, &g.db, g.db_id, Some(g.graph_id), a)
            {
                // Reads may rest on grants of single labels and types.
                if a == Action::Select
                    && g.def.kind != "mapped"
                    && let Some(v) = grants
                        .as_deref()
                        .and_then(|gr| GatewayGraphs::visible(gr, g.graph_id))
                {
                    visible.get_or_insert_with(Access::default).vis = Some(v);
                    continue;
                }
                // Writes may rest on them too (round 5): each write is
                // then checked against the labels and types it touches.
                if matches!(a, Action::Insert | Action::Update | Action::Delete)
                    && g.def.kind != "mapped"
                    && let Some(gr) = grants.as_deref()
                    && let Some(w) = GatewayGraphs::writable(gr, g.graph_id, a)
                {
                    // The other write actions as held graph-wide (a label
                    // change is a CREATE or DELETE on that label, whatever
                    // clause makes it).
                    let wide = |x: Action| {
                        self.graphs.allows(
                            grants.as_deref(),
                            &cat,
                            &g.db,
                            g.db_id,
                            Some(g.graph_id),
                            x,
                        )
                    };
                    let (ci, cd, cu) = (
                        wide(Action::Insert),
                        wide(Action::Delete),
                        wide(Action::Update),
                    );
                    let acc = visible.get_or_insert_with(Access::default);
                    let have = acc.writes.get_or_insert_with(|| {
                        let mut wr = Writable {
                            create_all: ci,
                            delete_all: cd,
                            set_all: cu,
                            ..Writable::default()
                        };
                        for x in [Action::Insert, Action::Delete, Action::Update] {
                            if let Some((l, t)) = GatewayGraphs::writable(gr, g.graph_id, x) {
                                match x {
                                    Action::Insert => {
                                        wr.create_labels = l;
                                        wr.create_types = t;
                                    }
                                    Action::Delete => {
                                        wr.delete_labels = l;
                                        wr.delete_types = t;
                                    }
                                    _ => {
                                        wr.set_labels = l;
                                        wr.set_types = t;
                                    }
                                }
                            }
                        }
                        wr
                    });
                    match a {
                        Action::Insert => {
                            have.create_all = false;
                            have.create_labels = w.0;
                            have.create_types = w.1;
                        }
                        Action::Delete => {
                            have.delete_all = false;
                            have.delete_labels = w.0;
                            have.delete_types = w.1;
                        }
                        _ => {
                            have.set_all = false;
                            have.set_labels = w.0;
                            have.set_types = w.1;
                        }
                    }
                    continue;
                }
                return Err(SprigError::standard(
                    1142,
                    *b"42000",
                    format!(
                        "{} command denied to user '{}' for graph '{}.{}'",
                        format!("{a:?}").to_uppercase(),
                        self.principal.user,
                        g.db,
                        g.name
                    ),
                ));
            }
        }
        if g.def.kind == "mapped" {
            if has_writes(&q) {
                return Err(SprigError::new(
                    SprigCode::GraphReadOnlySource,
                    format!(
                        "graph '{}.{}' is mapped over tables: write them in SQL",
                        g.db, g.name
                    ),
                ));
            }
            // An invoker's check on every source table (chapter 43,
            // "Security notes").
            let def = mapped_def(&g)?;
            for (t, o) in self.graphs.mapped_sources(&cat, g.db_id, &def)? {
                if !self.graphs.allows(
                    grants.as_deref(),
                    &cat,
                    &g.db,
                    g.db_id,
                    Some(o),
                    Action::Select,
                ) {
                    return Err(SprigError::standard(
                        1142,
                        *b"42000",
                        format!(
                            "SELECT command denied to user '{}' for table '{}.{t}' (a source of graph '{}')",
                            self.principal.user, g.db, g.name
                        ),
                    ));
                }
            }
        }
        let ctx = cat.setting_ctx(&self.dc).with_db(g.db_id);
        let settings = &self.graphs.svc.settings;
        let mut budgets =
            sprig_graph::settings::budgets(&|s| settings.get(s, &ctx, Some(&cat.settings)));
        let timeout = match &self.tx {
            Some(t) => t.timeout,
            None => timeout_of(extra),
        };
        if let Some(t) = timeout {
            budgets.max_time = budgets.max_time.min(t);
        }
        budgets.tx_metadata = match &self.tx {
            Some(t) => t.metadata.clone(),
            None => metadata_of(extra),
        };
        Ok(Prep::Run(Box::new(Prepared {
            q,
            p,
            g,
            cat,
            budgets,
            read_mode: read_mode(extra),
            timeout,
            visible,
        })))
    }

    #[allow(clippy::too_many_lines)] // auto-commit and transaction, buffered and streamed
    async fn execute(&mut self, pr: Prepared) -> Result<StartOrResult> {
        let Prepared {
            q,
            p,
            g,
            cat,
            budgets,
            read_mode,
            timeout,
            visible,
        } = pr;
        let writes = has_writes(&q);
        if g.def.kind == "mapped" {
            // Read-only SQL over the user's tables, on its own read-only
            // transaction (also inside an explicit Bolt transaction).
            let def = mapped_def(&g)?;
            let mut rc = ReadConns::default();
            if let Err(e) = self.open_reads(&mut rc, &g, read_mode, &cat).await {
                rc.end().await;
                return Err(e);
            }
            let r = match rc.conns.get_mut(g.cluster.as_str()) {
                Some(conn) => {
                    let mut store =
                        sprig_graph::mapped::MappedStore::new(conn, g.graph_id, &g.schema, def);
                    sprig_graph::exec::run(&mut store, &q, &p, &budgets).await
                }
                None => Err(SprigError::internal("no connection for a mapped graph")),
            };
            rc.end().await;
            return Ok(StartOrResult::Result(self.result(r?, &g.name, &cat)?));
        }
        let g = if writes {
            self.after_holds(g).await?
        } else {
            g
        };
        let dict = self.graphs.dictionary(&g);
        let (labels, types) = self.graphs.names_of(&g);
        let ids = self.graphs.counters(&g).await?;
        let streams = sprig_graph::streamable(&q).is_some();
        self.backend.set_isolation(self.isolation(&cat));
        self.backend.set_config(self.txn_config(&cat));
        if self.tx.is_none() {
            if !writes {
                let mut rc = ReadConns {
                    implicit: true,
                    ..ReadConns::default()
                };
                if let Err(e) = self.open_reads(&mut rc, &g, read_mode, &cat).await {
                    rc.end().await;
                    return Err(e);
                }
                let gate = self.gate(&g, &cat);
                if streams {
                    let (tx, rx) = mpsc::channel(CHANNEL_ROWS);
                    let layout = Arc::clone(&g.shards);
                    let indexes = g.def.indexes.clone();
                    tokio::spawn(crate::offload::run_graph_stream(async move {
                        let r = {
                            let mut store =
                                MariaStore::new(&mut rc, layout, labels, types, dict, ids)
                                    .with_schema(indexes, None)
                                    .with_gate(gate);
                            let mut sink = ChanSink { tx: tx.clone() };
                            stream_on(&mut store, visible, &q, &p, &budgets, &mut sink).await
                        };
                        rc.end().await;
                        let end = r.map(|o| summary(o.stats, None)).map_err(|e| failure(&e));
                        let _ = tx.send(Msg::End(end)).await;
                    }));
                    return Ok(StartOrResult::Stream(rx, g.name));
                }
                // A route over a range a mover holds waits for the hold,
                // then plans again (R8 round 8, G69).
                let holds: Arc<dyn sprig_graph::maria::HoldWait> = Arc::new(RouteHolds {
                    graphs: Arc::clone(&self.graphs),
                    g: g.clone(),
                });
                let r = {
                    let mut store =
                        MariaStore::new(&mut rc, Arc::clone(&g.shards), labels, types, dict, ids)
                            .with_schema(g.def.indexes.clone(), Some(self.graphs.schema_ops(&g)))
                            .with_gate(gate)
                            .with_holds(Some(holds));
                    run_on(&mut store, visible.clone(), &q, &p, &budgets).await
                };
                rc.end().await;
                let out = r?;
                self.after_commit().await?;
                return Ok(StartOrResult::Result(self.result(out, &g.name, &cat)?));
            }
            // A write: one coordinated transaction, whose lock waits end
            // with its tx_timeout.
            self.backend.set_lock_wait(timeout);
            let mut coord = self.backend.coordinator(self.tenant());
            let mut touched: Vec<Touched> = Vec::new();
            let r = {
                let mut cc = CoordConns {
                    coord: &mut coord,
                    backend: Arc::clone(&self.backend),
                    db: DbId(g.shards.db),
                    group: GroupId(g.shards.group),
                    touched: &mut touched,
                    batches: Some((Arc::clone(&self.graphs), g.clone())),
                };
                let r = {
                    let mut store =
                        MariaStore::new(&mut cc, Arc::clone(&g.shards), labels, types, dict, ids)
                            .with_schema(g.def.indexes.clone(), Some(self.graphs.schema_ops(&g)));
                    run_on(&mut store, visible.clone(), &q, &p, &budgets).await
                };
                match r {
                    Ok(o) => check_deleted(&mut cc, &g.shards, &o.deleted_nodes)
                        .await
                        .map(|()| o),
                    Err(e) => Err(e),
                }
            };
            let r = match r {
                Ok(o) if g.shards.group != 0 && !touched.is_empty() => {
                    match self.graphs.refresh(&g).await {
                        Ok(now) => check_epochs(&touched, &now.shards).map(|()| o),
                        Err(e) => Err(e),
                    }
                }
                x => x,
            };
            let out = match r {
                Ok(o) => {
                    coord.commit().await?;
                    o
                }
                Err(e) => {
                    let _ = coord.rollback().await;
                    return Err(e);
                }
            };
            self.after_commit().await?;
            return Ok(StartOrResult::Result(self.result(out, &g.name, &cat)?));
        }
        // In the explicit transaction.
        self.tx_conns().await?;
        let Some(t) = self.tx.as_mut() else {
            return Err(SprigError::internal("no transaction"));
        };
        let schema = q
            .parts
            .iter()
            .any(|x| x.clauses.iter().any(|c| matches!(c, Clause::Schema(_))));
        if schema && t.ran > 0 {
            return Err(SprigError::new(
                SprigCode::GraphUnsupported,
                "an index or constraint command must be the first statement of its transaction",
            ));
        }
        t.ran += 1;
        let mut conns = t.conns.take().ok_or_else(lost_conn)?;
        if let TxConns::Read(rc) = &mut conns
            && let Err(e) = self.open_reads(rc, &g, true, &cat).await
        {
            if let Some(t) = self.tx.as_mut() {
                t.conns = Some(conns);
            }
            return Err(e);
        }
        let backend = Arc::clone(&self.backend);
        // Read-only transactions may read asynchronous mirrors.
        let gate = if matches!(conns, TxConns::Read(_)) {
            self.gate(&g, &cat)
        } else {
            None
        };
        if streams {
            let (back_tx, back_rx) = oneshot::channel();
            if let Some(t) = self.tx.as_mut() {
                t.back = Some(back_rx);
            }
            let (tx, rx) = mpsc::channel(CHANNEL_ROWS);
            let layout = Arc::clone(&g.shards);
            let indexes = g.def.indexes.clone();
            tokio::spawn(crate::offload::run_graph_stream(async move {
                let mut touched = Vec::new();
                let r = {
                    let mut sink = ChanSink { tx: tx.clone() };
                    let mut cc;
                    let sc: &mut dyn ShardConns = match &mut conns {
                        TxConns::Read(rc) => rc,
                        TxConns::Write(coord) => {
                            cc = CoordConns {
                                coord,
                                backend,
                                db: DbId(layout.db),
                                group: GroupId(layout.group),
                                touched: &mut touched,
                                batches: None,
                            };
                            &mut cc
                        }
                    };
                    let mut store = MariaStore::new(sc, layout, labels, types, dict, ids)
                        .with_schema(indexes, None)
                        .with_gate(gate);
                    stream_on(&mut store, visible, &q, &p, &budgets, &mut sink).await
                };
                let _ = back_tx.send(conns);
                let end = r.map(|o| summary(o.stats, None)).map_err(|e| failure(&e));
                let _ = tx.send(Msg::End(end)).await;
            }));
            return Ok(StartOrResult::Stream(rx, g.name));
        }
        let ops = self.graphs.schema_ops(&g);
        let mut touched = Vec::new();
        let r = {
            let mut cc;
            let sc: &mut dyn ShardConns = match &mut conns {
                TxConns::Read(rc) => rc,
                TxConns::Write(coord) => {
                    cc = CoordConns {
                        coord,
                        backend,
                        db: DbId(g.shards.db),
                        group: GroupId(g.shards.group),
                        touched: &mut touched,
                        batches: None,
                    };
                    &mut cc
                }
            };
            let mut store = MariaStore::new(sc, Arc::clone(&g.shards), labels, types, dict, ids)
                .with_schema(g.def.indexes.clone(), Some(ops))
                .with_gate(gate);
            run_on(&mut store, visible.clone(), &q, &p, &budgets).await
        };
        let Some(t) = self.tx.as_mut() else {
            return Err(SprigError::internal("no transaction"));
        };
        t.conns = Some(conns);
        let out = r?;
        if !touched.is_empty() {
            match t.touched.iter_mut().find(|(x, _)| x.graph_id == g.graph_id) {
                Some((_, v)) => {
                    for x in touched {
                        if !v.contains(&x) {
                            v.push(x);
                        }
                    }
                }
                None => t.touched.push((g.clone(), touched)),
            }
        }
        if !out.deleted_nodes.is_empty() {
            t.deleted
                .push((Arc::clone(&g.shards), out.deleted_nodes.clone()));
        }
        Ok(StartOrResult::Result(self.result(out, &g.name, &cat)?))
    }

    fn result(
        &self,
        out: sprig_graph::Output,
        graph: &str,
        cat: &TenantCatalog,
    ) -> Result<QueryResult> {
        let limit = usize::try_from(self.graphs.svc.settings.get(
            &MAX_BUFFERED_RESULT_MB,
            &cat.setting_ctx(&self.dc),
            Some(&cat.settings),
        ))
        .unwrap_or(64)
        .saturating_mul(1 << 20);
        let mut bytes = 0usize;
        let mut records = VecDeque::with_capacity(out.rows.len());
        for r in out.rows {
            let rec: Vec<PackValue> = r.iter().map(to_pack).collect();
            bytes += rec.iter().map(PackValue::encoded_len).sum::<usize>();
            if bytes > limit {
                return Err(SprigError::new(
                    SprigCode::BoltResultBufferLimit,
                    format!(
                        "the result is larger than bolt.max_buffered_result_mb = {}",
                        limit >> 20
                    ),
                ));
            }
            records.push_back(rec);
        }
        let mut summary = Vec::new();
        if let Some(steps) = &out.plan {
            let key = if out.profiled { "profile" } else { "plan" };
            summary.push((
                key,
                plan_pack(steps, &out.columns, out.profiled.then_some(records.len())),
            ));
        }
        if !out.notifications.is_empty() {
            summary.push((
                "notifications",
                PackValue::List(out.notifications.iter().map(notification_pack).collect()),
            ));
            // Bolt 5.5 on: the same as GQL status objects (the protocol
            // layer sends one or the other, by version).
            summary.push((
                "statuses",
                PackValue::List(out.notifications.iter().map(status_pack).collect()),
            ));
        }
        let stream = out.late_error.map(|e| {
            Box::new(super::stream::LateError(Some(failure(&e))))
                as Box<dyn sprig_proto_bolt::server::RecordStream>
        });
        Ok(QueryResult {
            fields: out.columns,
            records,
            qtype: out.qtype,
            stats: out.stats.into_iter().collect(),
            db: Some(graph.to_owned()),
            summary,
            stream,
            ..QueryResult::default()
        })
    }

    /// Ends the open transaction, whether its connections are here or
    /// still with a streamed result: everything rolls back.
    fn abandon(&mut self) {
        let Some(t) = self.tx.take() else { return };
        let fut = async move {
            let conns = match (t.conns, t.back) {
                (Some(c), _) => Some(c),
                (None, Some(b)) => b.await.ok(),
                (None, None) => None,
            };
            match conns {
                Some(TxConns::Read(rc)) => rc.end().await,
                Some(TxConns::Write(mut coord)) => {
                    let _ = coord.rollback().await;
                }
                None => {}
            }
        };
        tokio::spawn(fut);
    }
}

/// Whether `e` says the statement used a shard map that changed under it.
fn stale(e: &SprigError) -> bool {
    match e.kind {
        sprig_common::ErrorKind::Backend { errno, .. } => matches!(errno, 1146 | 1049),
        _ => e.code() == Some(SprigCode::ShardMapChanged),
    }
}

/// [`stale`] for a failure a streamed statement reported.
fn stale_failure(f: &BoltFailure) -> bool {
    f.message.contains("(1146)")
        || f.message.contains("(1049)")
        || f.code == "Neo.TransientError.General.DatabaseUnavailable"
}

/// A mapped graph's definition.
fn mapped_def(g: &GraphRef) -> Result<sprig_graph::mapped::MappedDef> {
    serde_json::from_str(g.def.definition.as_deref().unwrap_or("{}"))
        .map_err(|e| SprigError::internal(format!("mapped graph definition: {e}")))
}

/// What `execute` produced.
enum StartOrResult {
    Result(QueryResult),
    Stream(mpsc::Receiver<Msg>, String),
}

impl GwSession {
    /// A `RUN`: prepare and execute, again with a fresh map when the
    /// statement met a shard that moved.
    async fn run_inner(
        &mut self,
        query: &str,
        params: &PackValue,
        extra: &PackValue,
    ) -> std::result::Result<QueryResult, BoltFailure> {
        let started = Instant::now();
        let mut attempt = 0;
        let timeout = self
            .tx
            .as_ref()
            .map_or_else(|| timeout_of(extra), |t| t.timeout);
        let (start, graph) = loop {
            let pr = match self.prepare(query, params, extra).await {
                Ok(Prep::Done(r)) => return Ok(r),
                Ok(Prep::Run(p)) => *p,
                Err(e) => return Err(failure(&e)),
            };
            match self.execute(pr).await {
                Ok(StartOrResult::Result(r)) => return Ok(r),
                Ok(StartOrResult::Stream(rx, g)) => match super::stream::start(rx).await {
                    Ok(s) => break (s, g),
                    // A streamed read that failed before its first row
                    // (nothing reached the client): as below (round 5).
                    Err(f) if self.tx.is_none() && attempt < 5 && stale_failure(&f) => {
                        attempt += 1;
                        metrics::counter!("sprig_graph_stale_map_retries_total").increment(1);
                        tokio::time::sleep(Duration::from_millis(100 << attempt.min(4))).await;
                    }
                    Err(f) => return Err(f),
                },
                // An auto-commit statement that met a shard the mover just
                // split, merged or moved (its schema or table is gone, or
                // the map changed under it) runs again with the new map.
                Err(e) if self.tx.is_none() && attempt < 5 && stale(&e) => {
                    attempt += 1;
                    metrics::counter!("sprig_graph_stale_map_retries_total").increment(1);
                    tokio::time::sleep(Duration::from_millis(100 << attempt.min(4))).await;
                }
                Err(e) => return Err(timed_out_failure(&e, timeout)),
            }
        };
        let t_first = i64::try_from(started.elapsed().as_millis()).unwrap_or(0);
        Ok(match start {
            Start::Rows(fields, first, stream) => QueryResult {
                fields,
                records: VecDeque::from([first]),
                qtype: "r",
                db: Some(graph),
                t_first,
                stream: Some(Box::new(stream)),
                ..QueryResult::default()
            },
            Start::Done(fields, end) => QueryResult {
                fields,
                qtype: "r",
                stats: end.stats,
                db: Some(graph),
                t_first,
                ..QueryResult::default()
            },
        })
    }
}

#[async_trait]
impl BoltSession for GwSession {
    async fn run(
        &mut self,
        query: &str,
        params: &PackValue,
        extra: &PackValue,
    ) -> std::result::Result<QueryResult, BoltFailure> {
        let handle = self.reg.as_ref().map(|s| {
            s.begin_command("Query", Some(query.chars().take(1024).collect()));
            s.set_db(
                extra
                    .get("db")
                    .and_then(PackValue::as_str)
                    .map(str::to_owned),
            );
            Arc::clone(s.handle())
        });
        let r = match handle {
            // On a blocking thread from its first poll (R8 round 10,
            // decision G80; before, guarded like a MySQL statement, G40
            // and decision bugfix-1, which moved it only after 100 ms and
            // while slots lasted).
            Some(h) => {
                Box::pin(crate::offload::run_graph(async {
                    tokio::select! {
                        r = self.run_inner(query, params, extra) => r,
                        () = h.flags().query_killed() => {
                            h.flags().take_kill_query();
                            // Whatever the statement held is dropped: its
                            // connections close and roll back.
                            self.abandon();
                            Err(BoltFailure::new(
                                "Neo.TransientError.Transaction.Terminated",
                                "The transaction has been terminated (KILL)",
                            ))
                        }
                    }
                }))
                .await
            }
            None => self.run_inner(query, params, extra).await,
        };
        if let Some(s) = &self.reg {
            s.end_command();
        }
        r
    }

    async fn begin(&mut self, extra: &PackValue) -> std::result::Result<(), BoltFailure> {
        if extra
            .get("imp_user")
            .is_some_and(|v| !matches!(v, PackValue::Null))
        {
            return Err(refuse(
                SprigCode::BoltImpersonation,
                "impersonation (imp_user) is not supported",
            ));
        }
        let tenant = self.tenant();
        let r: Result<Tx> = async {
            self.take_bookmarks(extra).await?;
            let cat = self.graphs.catalog(tenant, Freshness::Latest).await?;
            self.backend.set_isolation(self.isolation(&cat));
            self.backend.set_config(self.txn_config(&cat));
            self.backend.set_lock_wait(timeout_of(extra));
            let conns = if read_mode(extra) {
                TxConns::Read(ReadConns::default())
            } else {
                let mut coord = self.backend.coordinator(tenant);
                coord.begin(BeginOptions::default())?;
                TxConns::Write(Box::new(coord))
            };
            Ok(Tx {
                conns: Some(conns),
                back: None,
                db: extra
                    .get("db")
                    .and_then(PackValue::as_str)
                    .map(str::to_owned),
                timeout: timeout_of(extra),
                metadata: metadata_of(extra),
                deleted: Vec::new(),
                touched: Vec::new(),
                ran: 0,
            })
        }
        .await;
        self.tx = Some(r.map_err(|e| failure(&e))?);
        Ok(())
    }

    async fn commit(&mut self) -> std::result::Result<Option<String>, BoltFailure> {
        if self.tx.is_none() {
            return Err(BoltFailure::invalid("no open transaction"));
        }
        if let Err(e) = self.tx_conns().await {
            self.abandon();
            return Err(failure(&e));
        }
        let Some(mut t) = self.tx.take() else {
            return Err(BoltFailure::invalid("no open transaction"));
        };
        let Some(conns) = t.conns.take() else {
            return Err(failure(&lost_conn()));
        };
        let mut coord = match conns {
            TxConns::Read(rc) => {
                rc.end().await;
                let b = self.after_commit().await.map_err(|e| failure(&e))?;
                return Ok(Some(b));
            }
            TxConns::Write(c) => c,
        };
        let checked: Result<()> = async {
            for (layout, ids) in &t.deleted {
                let mut touched = Vec::new();
                let mut cc = CoordConns {
                    coord: &mut coord,
                    backend: Arc::clone(&self.backend),
                    db: DbId(layout.db),
                    group: GroupId(layout.group),
                    touched: &mut touched,
                    batches: None,
                };
                check_deleted(&mut cc, layout, ids).await?;
            }
            for (g, touched) in &t.touched {
                if g.shards.group != 0 {
                    let now = self.graphs.refresh(g).await?;
                    check_epochs(touched, &now.shards)?;
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = checked {
            let _ = coord.rollback().await;
            return Err(failure(&e));
        }
        coord.commit().await.map_err(|e| failure(&e))?;
        let b = self.after_commit().await.map_err(|e| failure(&e))?;
        Ok(Some(b))
    }

    async fn rollback(&mut self) -> std::result::Result<(), BoltFailure> {
        self.abandon();
        Ok(())
    }

    fn last_bookmark(&self) -> Option<String> {
        self.causal().last.clone()
    }

    async fn reset(&mut self) {
        self.abandon();
    }

    async fn route(
        &mut self,
        routing: &PackValue,
        extra: &PackValue,
    ) -> std::result::Result<PackValue, BoltFailure> {
        let addr = if self.advertise.is_empty() {
            routing
                .get("address")
                .and_then(PackValue::as_str)
                .unwrap_or("localhost:7687")
                .to_owned()
        } else {
            self.advertise.clone()
        };
        let db = extra
            .get("db")
            .and_then(PackValue::as_str)
            .unwrap_or("neo4j")
            .to_owned();
        let server = |role: &str| {
            PackValue::map([
                (
                    "addresses",
                    PackValue::List(vec![PackValue::str(addr.clone())]),
                ),
                ("role", PackValue::str(role)),
            ])
        };
        Ok(PackValue::map([
            (
                "ttl",
                PackValue::Int(i64::try_from(self.ttl).unwrap_or(300)),
            ),
            ("db", PackValue::String(db)),
            (
                "servers",
                PackValue::List(vec![server("ROUTE"), server("READ"), server("WRITE")]),
            ),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tx_timeouts_answer_neo4j_codes() {
        // The drivers' testkit (R8): a lock wait cut by tx_timeout is
        // LockClientStopped; the time budget set by tx_timeout is
        // TransactionTimedOutClientConfiguration; without a tx_timeout
        // both keep their usual codes.
        let t = Some(Duration::from_millis(250));
        let lock = SprigError::backend(1205, *b"HY000", "Lock wait timeout exceeded");
        assert_eq!(
            timed_out_failure(&lock, t).code,
            "Neo.ClientError.Transaction.LockClientStopped"
        );
        assert_eq!(
            timed_out_failure(&lock, None).code,
            "Neo.TransientError.Transaction.DeadlockDetected"
        );
        let budget = sprig_graph::exec::budget_error("time", "graph.max_time_ms = 250");
        assert_eq!(
            timed_out_failure(&budget, t).code,
            "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration"
        );
        // graph.max_time_ms itself was the lower bound: a budget error.
        let budget = sprig_graph::exec::budget_error("time", "graph.max_time_ms = 100");
        assert_eq!(
            timed_out_failure(&budget, t).code,
            "Neo.ClientError.Statement.SprigGraphBudgetExceeded"
        );
    }

    #[test]
    fn tx_metadata_is_a_map_or_nothing() {
        let extra = PackValue::map([(
            "tx_metadata",
            PackValue::map([
                ("foo", PackValue::Float(1.5)),
                ("bar", PackValue::str("baz")),
            ]),
        )]);
        let m = metadata_of(&extra).expect("a map");
        assert_eq!(m.len(), 2);
        assert_eq!(m["bar"], sprig_graph::value::Value::String("baz".into()));
        assert!(metadata_of(&PackValue::map([("tx_metadata", PackValue::Int(1))])).is_none());
        assert!(metadata_of(&PackValue::Map(vec![])).is_none());
    }

    #[test]
    fn plans_are_neo4j_operator_trees() {
        let steps = vec!["NodeScan n: label".to_owned(), "Filter: n.x > 1".to_owned()];
        let cols = vec!["n".to_owned()];
        let plan = plan_pack(&steps, &cols, None);
        assert_eq!(
            plan.get("operatorType").and_then(PackValue::as_str),
            Some("ProduceResults")
        );
        assert!(plan.get("rows").is_none());
        let Some(PackValue::List(kids)) = plan.get("children") else {
            panic!("children");
        };
        // The last step sits right under ProduceResults.
        assert_eq!(
            kids[0].get("operatorType").and_then(PackValue::as_str),
            Some("Filter")
        );
        let profile = plan_pack(&steps, &cols, Some(3));
        assert_eq!(profile.get("rows"), Some(&PackValue::Int(3)));
        assert_eq!(profile.get("dbHits"), Some(&PackValue::Int(0)));
    }
}
