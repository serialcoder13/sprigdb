//! [`GraphStore`] over MariaDB (chapter 43, "Native graph tables",
//! "Variable-length paths", "Writes and transactions"; build steps 3, 11
//! and 14): one native graph's tables in each of its shard schemas, on the
//! caller's connections and transaction.
//!
//! - **Shards** ([`GraphShards`]). A graph from round 3 is a sharded table
//!   group keyed by `part` (= `id`): node, label and mirror rows live on the
//!   shard of their node's routing hash, edges on their source's shard.
//!   Graphs from rounds 1 and 2 are one shard in the database's first shard
//!   schema. [`ShardConns`] hands out the connection of each shard's
//!   cluster inside the statement's transaction (the gateway's is chapter
//!   18's coordinator; [`OneConn`] is one connection for every shard).
//! - A hop is one statement per batch and shard: outgoing edges by the
//!   clustered `(src, ...)` key on the source's shard, the far node
//!   left-joined when it is local; incoming edges through the mirrors on the
//!   destination's shard, then the edges by id on their sources' shards (or
//!   the `rev` key when the graph has one shard).
//! - [`GraphStore::reach`] is the chapter's recursive CTE with an origin
//!   column, `CYCLE o, id RESTRICT`, `max_recursive_iterations` and the
//!   shard's range: nodes outside it are boundary nodes, returned but not
//!   expanded (incoming searches recurse over the mirrors).
//! - **Mirrors.** Every edge of a grouped graph has a mirror row on its
//!   destination's shard, whatever the shard count, so splits carry them
//!   with no rebuild. Under `sync` (and until the worker's applier serves
//!   the graph) writers keep it in their own transaction; under `async`
//!   (round 5, [`crate::mirror`]) the applier keeps it from the feed, and
//!   a statement reads through mirrors only when its [`MirrorGate`] says
//!   the applier has reached the session's causal target; otherwise
//!   incoming hops scatter the `rev` key, and write statements always do.
//! - Property equalities push down as `JSON_VALUE(props, '$."k"') IN (...)`,
//!   a superset the gateway rechecks.
//! - Ids come from [`CounterIds`]: blocks from the counter table on their
//!   own autocommit connection, so writers never wait on each other's
//!   transactions for ids.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params, Row, Value as MyValue};
use serde_json::Value as Json;
use sprig_common::{Result, SprigCode, SprigError};

use crate::ast::{Dir, Entity, Schema};
use crate::catalog::{Tables, backend_error};
use crate::mirror::MirrorGate;
use sprig_router::graph_route::{
    EDGE_COLS, HopOp, Lit, Route, RouteSpec, ScanOp, Targets, plan_route,
};

use crate::store::{
    EdgeKey, EdgeRow, GraphStore, IdKind, IndexDef, NodeRow, Reach, Scan, SchemaCounts, StoreStats,
    rk_of,
};

/// Labels and relationship types of a graph, and how new ones are made.
#[async_trait]
pub trait Dictionary: Send + Sync {
    /// Creates (or finds) a label (`label = true`) or type: its id.
    async fn create(&self, label: bool, name: &str) -> Result<u32>;
}

/// Index and constraint commands: catalog rows and `ALTER TABLE`s,
/// outside the data transaction (the gateway's).
#[async_trait]
pub trait SchemaOps: Send + Sync {
    /// Runs `cmd`: the counters it changed.
    async fn run(&self, cmd: &Schema) -> Result<SchemaCounts>;
}

/// Ids handed out in blocks from `__sprig_gq_<graph>`.
pub struct CounterIds {
    pool: mysql_async::Pool,
    seq: String,
    block: u64,
    /// Per kind: (next, end).
    cache: Mutex<HashMap<u8, (u64, u64)>>,
}

impl std::fmt::Debug for CounterIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CounterIds")
            .field("seq", &self.seq)
            .finish_non_exhaustive()
    }
}

impl CounterIds {
    /// Counters of `tables` through `pool`, `block` ids at a time.
    #[must_use]
    pub fn new(pool: mysql_async::Pool, tables: &Tables, block: u64) -> Self {
        Self {
            pool,
            seq: tables.seq.clone(),
            block: block.max(1),
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// `n` ids of `kind`.
    ///
    /// # Errors
    /// A backend error.
    pub async fn take(&self, kind: IdKind, n: usize) -> Result<Vec<u64>> {
        let k: u8 = match kind {
            IdKind::Node => 1,
            IdKind::Edge => 2,
        };
        let n64 = u64::try_from(n).unwrap_or(u64::MAX);
        let mut out = Vec::with_capacity(n);
        loop {
            {
                let mut c = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
                let e = c.entry(k).or_insert((0, 0));
                while e.0 < e.1 && out.len() < n {
                    out.push(e.0);
                    e.0 += 1;
                }
            }
            if out.len() == n {
                return Ok(out);
            }
            let want = self.block.max(n64 - u64::try_from(out.len()).unwrap_or(0));
            let mut conn = self.pool.get_conn().await.map_err(backend_error)?;
            conn.exec_drop(
                format!(
                    "UPDATE {} SET next = LAST_INSERT_ID(next + ?) WHERE kind = ?",
                    self.seq
                ),
                (want, k),
            )
            .await
            .map_err(backend_error)?;
            let end: Option<u64> = conn
                .query_first("SELECT LAST_INSERT_ID()")
                .await
                .map_err(backend_error)?;
            let end = end
                .filter(|e| *e >= want)
                .ok_or_else(|| SprigError::internal("graph id counter row missing"))?;
            let mut c = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            c.insert(k, (end - want, end));
        }
    }
}

/// Where one shard of a graph lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLoc {
    /// The shard id in the group's map (1 for an ungrouped graph).
    pub shard: u32,
    /// First routing hash of its range.
    pub lo: u64,
    /// The range's epoch (raised by splits, merges and moves).
    pub epoch: u64,
    /// Its cluster.
    pub cluster: String,
    /// Its schema.
    pub schema: String,
    /// The graph's tables there.
    pub tables: Tables,
}

/// A graph's shards at one map version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphShards {
    /// The graph id.
    pub graph: u64,
    /// The logical database.
    pub db: u32,
    /// The table group (0: an ungrouped graph in one schema).
    pub group: u32,
    /// The map version.
    pub version: u64,
    /// The shards, by `lo`; the first starts at 0.
    pub shards: Vec<ShardLoc>,
    /// The id counter table (in the database's first shard schema).
    pub seq: String,
    /// Mirror rows are kept for every edge.
    pub mirrors: bool,
    /// The mirrors are the applier's (`graph.mirror = async`, round 5):
    /// writers don't write them, and readers use them only after the
    /// statement's [`MirrorGate`] says they are fresh.
    pub mirror_async: bool,
    /// The mirrors are being completed (`mirror_state = building`, after
    /// `async` went back to `sync`): writers keep them, readers don't use
    /// them yet.
    pub mirror_building: bool,
    /// The router's own view of the group (its map cache: shards, epochs,
    /// holds), which plans every read step (R8 round 8, G69); `None`: made
    /// from `shards` ([`Self::group`]).
    pub route: Option<Arc<sprig_router::topology::Group>>,
}

impl GraphShards {
    /// An ungrouped graph: one shard, `schema` on `cluster`, no mirrors.
    #[must_use]
    pub fn single(graph: u64, db: u32, schema: &str, cluster: &str) -> Self {
        let tables = Tables::of(schema, graph);
        Self {
            graph,
            db,
            group: 0,
            version: 0,
            seq: tables.seq.clone(),
            shards: vec![ShardLoc {
                shard: 1,
                lo: 0,
                epoch: 0,
                cluster: cluster.to_owned(),
                schema: schema.to_owned(),
                tables,
            }],
            mirrors: false,
            mirror_async: false,
            mirror_building: false,
            route: None,
        }
    }

    /// The group the router plans this graph's steps over: the router's
    /// own when the gateway resolved the graph, else one made from
    /// `shards` (the same ranges, keyed by `part`, no holds).
    #[must_use]
    pub fn group(&self) -> Arc<sprig_router::topology::Group> {
        if let Some(g) = &self.route {
            return Arc::clone(g);
        }
        Arc::new(sprig_router::topology::Group {
            id: self.group,
            db: self.db,
            version: self.version,
            key_class: sprig_router::topology::KeyClass::Int,
            spec: numeric_spec(),
            shards: self
                .shards
                .iter()
                .map(|s| sprig_router::topology::ShardLoc {
                    shard: s.shard,
                    schema: s.schema.clone(),
                    cluster: s.cluster.clone(),
                    lo: s.lo,
                    epoch: s.epoch,
                    held_until_ms: None,
                })
                .collect(),
        })
    }

    /// The index of the shard holding routing hash `rk`.
    #[must_use]
    pub fn index_of(&self, rk: u64) -> usize {
        self.shards
            .partition_point(|s| s.lo <= rk)
            .saturating_sub(1)
    }

    /// One past the last hash of shard `i`; `None` is 2^64.
    #[must_use]
    pub fn hi(&self, i: usize) -> Option<u64> {
        self.shards.get(i + 1).map(|s| s.lo)
    }

    /// Ids grouped by the shard of their node (`part` = `id`).
    #[must_use]
    pub fn by_shard(&self, ids: &[u64]) -> BTreeMap<usize, Vec<u64>> {
        let mut m: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
        for &i in ids {
            m.entry(self.index_of(rk_of(i))).or_default().push(i);
        }
        m
    }
}

/// A graph's key: one numeric part (`part` = `id`).
fn numeric_spec() -> sprig_shard::KeySpec {
    // One numeric part always makes a spec (`rk_of` relies on it too).
    sprig_shard::KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).expect("a numeric key spec")
}

/// Waits for a mover's hold on a range a route reads (chapter 10,
/// "Cutover"): the gateway's waits for the hold to end in its map cache.
#[async_trait]
pub trait HoldWait: Send + Sync {
    /// Waits until the hold ending at `until_ms` is gone, then answers an
    /// error that makes the statement plan again with the new map (or the
    /// hold's own error when it lasts too long).
    ///
    /// # Errors
    /// 39424 (`ShardMapChanged`) once the hold is gone.
    async fn wait(&self, until_ms: u64) -> Result<()>;
}

/// The connections a statement runs on: one per shard's cluster, inside
/// the statement's transaction.
#[async_trait]
pub trait ShardConns: Send {
    /// The connection for `at`; `write` for writes and locking reads (the
    /// coordinator's `Intent::Write`).
    ///
    /// # Errors
    /// The coordinator's (39440, 39444) or the backend's.
    async fn conn(&mut self, at: &ShardLoc, write: bool) -> Result<&mut Conn>;

    /// Whether writes are reported through [`Self::wrote`] (a coordinator
    /// whose branches may be held by Sprig 2PC records them for redo).
    fn records(&self) -> bool {
        false
    }

    /// A write that succeeded on `at`'s connection, with its values
    /// written into the text ([`inline_params`]); the connection still
    /// holds its affected-row count.
    fn wrote(&mut self, _at: &ShardLoc, _sql: &str) {}

    /// Whether the statement may commit batches of its own (`CALL { } IN
    /// TRANSACTIONS`, R8 round 8): an auto-commit statement's connections.
    fn batches(&self) -> bool {
        false
    }

    /// Commits (`ok`) or rolls back the current batch's transaction; the
    /// next connection asked for starts a new one.
    ///
    /// # Errors
    /// The commit's.
    async fn end_batch(&mut self, _ok: bool) -> Result<()> {
        Err(crate::store::not_implicit())
    }
}

/// `sql` with each `?` placeholder outside quotes replaced by its value
/// as an SQL literal: the text a held branch records for redo (chapter
/// 18's Sprig 2PC).
#[must_use]
pub fn inline_params(sql: &str, params: &Params) -> String {
    let vals: &[MyValue] = match params {
        Params::Positional(v) => v,
        _ => return sql.to_owned(),
    };
    let mut out = String::with_capacity(sql.len() + vals.len() * 8);
    let mut it = vals.iter();
    let mut quote: Option<char> = None;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                out.push(c);
                if c == '\\' && q != '`' {
                    if let Some(n) = chars.next() {
                        out.push(n);
                    }
                } else if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' | '`' => {
                    quote = Some(c);
                    out.push(c);
                }
                '?' => match it.next() {
                    Some(v) => out.push_str(&v.as_sql(false)),
                    None => out.push('?'),
                },
                _ => out.push(c),
            },
        }
    }
    out
}

/// One connection for every shard (tests; every shard on one server).
#[derive(Debug)]
pub struct OneConn<'c>(pub &'c mut Conn);

#[async_trait]
impl ShardConns for OneConn<'_> {
    async fn conn(&mut self, _at: &ShardLoc, _write: bool) -> Result<&mut Conn> {
        Ok(&mut *self.0)
    }
}

/// One graph over its shards, inside the caller's transaction.
pub struct MariaStore<'c> {
    conns: &'c mut dyn ShardConns,
    layout: Arc<GraphShards>,
    labels: BTreeMap<u32, String>,
    types: BTreeMap<u32, String>,
    dict: Arc<dyn Dictionary>,
    ids: Arc<CounterIds>,
    stats: StoreStats,
    /// `_v` of writes: microseconds since the epoch.
    now_us: u64,
    indexes: Vec<IndexDef>,
    schema: Option<Arc<dyn SchemaOps>>,
    /// Whether asynchronous mirrors may be read (a read statement's).
    gate: Option<Arc<dyn MirrorGate>>,
    /// The gate's answer, asked once per statement.
    mirror_ok: Option<bool>,
    /// The group the router plans every read step over (G69).
    group: Arc<sprig_router::topology::Group>,
    /// Who waits for a mover's hold a route meets (auto-commit reads);
    /// `None`: the statement runs on the map it has.
    holds: Option<Arc<dyn HoldWait>>,
    /// The routes this statement ran, for `PROFILE`: per step and plan
    /// kind, (routes, statements, shards).
    routes: BTreeMap<(&'static str, &'static str), (u64, u64, std::collections::BTreeSet<String>)>,
}

impl std::fmt::Debug for MariaStore<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MariaStore")
            .field("graph", &self.layout.graph)
            .field("shards", &self.layout.shards.len())
            .finish_non_exhaustive()
    }
}

fn ids_list(ids: &[u64]) -> String {
    let mut s = String::with_capacity(ids.len() * 8);
    for (i, x) in ids.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "{x}");
    }
    s
}

fn json_col(v: Option<MyValue>) -> Option<Json> {
    match v? {
        MyValue::Bytes(b) => serde_json::from_slice(&b).ok(),
        _ => None,
    }
}

fn u64_col(v: Option<MyValue>) -> u64 {
    match v {
        Some(MyValue::UInt(u)) => u,
        Some(MyValue::Int(i)) => u64::try_from(i).unwrap_or(0),
        Some(MyValue::Bytes(b)) => std::str::from_utf8(&b)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        _ => 0,
    }
}

/// A node from columns `start..start + 5` of `row`, or `None` when the
/// left join found nothing.
fn node_at(row: &mut Row, start: usize) -> Option<NodeRow> {
    let id = row.take::<MyValue, _>(start)?;
    if id == MyValue::NULL {
        return None;
    }
    let labels = json_col(row.take(start + 2)).unwrap_or(Json::Array(vec![]));
    Some(NodeRow {
        id: u64_col(Some(id)),
        rk: u64_col(row.take(start + 1)),
        labels: labels
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(serde_json::Value::as_u64)
                    .filter_map(|x| u32::try_from(x).ok())
                    .collect()
            })
            .unwrap_or_default(),
        props: json_col(row.take(start + 3)).unwrap_or(Json::Object(serde_json::Map::new())),
        types: json_col(row.take(start + 4)),
    })
}

/// An edge from `e.src, e.type, e.dst, e.id, e.dst_rk, e._sprig_rk,
/// e.props, e._types` at `start`.
fn edge_at(row: &mut Row, start: usize) -> EdgeRow {
    EdgeRow {
        src: u64_col(row.take(start)),
        rtype: u32::try_from(u64_col(row.take(start + 1))).unwrap_or(0),
        dst: u64_col(row.take(start + 2)),
        id: u64_col(row.take(start + 3)),
        dst_rk: u64_col(row.take(start + 4)),
        src_rk: u64_col(row.take(start + 5)),
        props: json_col(row.take(start + 6)).unwrap_or(Json::Object(serde_json::Map::new())),
        types: json_col(row.take(start + 7)),
    }
}

fn json_text(j: &Json) -> String {
    j.to_string()
}

/// A pushed-down JSON scalar as literals (a superset the gateway
/// rechecks): strings and integers as themselves; booleans as the texts
/// `JSON_VALUE` may answer (`1` on MariaDB 11.8, `true` elsewhere).
fn scalar_lits(j: &Json) -> Option<Vec<Lit>> {
    match j {
        Json::String(s) => Some(vec![Lit::Str(s.clone())]),
        Json::Number(n) => n.as_i64().map(|i| vec![Lit::Int(i)]),
        Json::Bool(true) => Some(vec![Lit::Str("1".into()), Lit::Str("true".into())]),
        Json::Bool(false) => Some(vec![Lit::Str("0".into()), Lit::Str("false".into())]),
        _ => None,
    }
}

impl<'c> MariaStore<'c> {
    /// The graph of `layout`, on `conns` (inside the caller's transaction),
    /// with the catalog's labels and types.
    #[must_use]
    pub fn new(
        conns: &'c mut dyn ShardConns,
        layout: Arc<GraphShards>,
        labels: BTreeMap<u32, String>,
        types: BTreeMap<u32, String>,
        dict: Arc<dyn Dictionary>,
        ids: Arc<CounterIds>,
    ) -> Self {
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(0));
        Self {
            conns,
            labels,
            types,
            dict,
            ids,
            stats: StoreStats::default(),
            now_us,
            indexes: Vec::new(),
            schema: None,
            gate: None,
            mirror_ok: None,
            group: layout.group(),
            holds: None,
            routes: BTreeMap::new(),
            layout,
        }
    }

    /// Routes that meet a range a mover holds wait through `w` (auto-commit
    /// reads, which then plan again with the map after the cutover).
    #[must_use]
    pub fn with_holds(mut self, w: Option<Arc<dyn HoldWait>>) -> Self {
        self.holds = w;
        self
    }

    /// Plans `op` over `targets` with the router (G69) and runs its
    /// statements in order: per statement, the shard's index in the layout
    /// and its rows. `write`: on the write connection (locking reads).
    async fn route(
        &mut self,
        op: HopOp,
        targets: Targets,
        write: bool,
    ) -> Result<Vec<(usize, Vec<Row>)>> {
        let r = plan_route(&RouteSpec {
            group: Arc::clone(&self.group),
            graph: self.layout.graph,
            op,
            targets,
        });
        if let (Some(until), Some(h)) = (r.held_until_ms, &self.holds) {
            h.wait(until).await?;
        }
        self.note(&r);
        let mut out = Vec::with_capacity(r.targets().len());
        for t in r.targets() {
            let i = self
                .layout
                .shards
                .iter()
                .position(|s| s.shard == t.shard && s.schema == t.at.schema)
                .ok_or_else(|| {
                    SprigError::new(
                        SprigCode::ShardMapChanged,
                        "a graph route names a shard the statement's map doesn't have; retry",
                    )
                })?;
            self.stats.shard_queries += 1;
            let at = self.layout.shards[i].clone();
            let conn = self.conns.conn(&at, write).await?;
            let rows = conn
                .exec(t.sql.as_str(), Params::Empty)
                .await
                .map_err(backend_error)?;
            out.push((i, rows));
        }
        Ok(out)
    }

    /// Counts a route for `PROFILE`.
    fn note(&mut self, r: &Route) {
        let e = self.routes.entry((r.op, r.plan.kind())).or_default();
        e.0 += 1;
        e.1 += u64::try_from(r.targets().len()).unwrap_or(u64::MAX);
        for t in r.targets() {
            if e.2.len() < 16 {
                e.2.insert(format!("s{}@{}", t.shard, t.cluster));
            }
        }
    }

    /// Asynchronous mirrors may be read when `gate` says they are fresh
    /// (read statements; without a gate incoming hops scatter).
    #[must_use]
    pub fn with_gate(mut self, gate: Option<Arc<dyn MirrorGate>>) -> Self {
        self.gate = gate;
        self
    }

    /// Whether incoming hops read the mirrors: a grouped graph with more
    /// than one shard whose mirrors are kept synchronously, or are the
    /// applier's and fresh for this statement.
    async fn read_mirrors(&mut self) -> bool {
        if !self.many() || !self.layout.mirrors || self.layout.mirror_building {
            return false;
        }
        if !self.layout.mirror_async {
            return true;
        }
        if let Some(ok) = self.mirror_ok {
            return ok;
        }
        let ok = match &self.gate {
            Some(g) => g.fresh(&self.layout).await,
            None => false,
        };
        if !ok {
            let reason = if self.gate.is_some() { "lag" } else { "write" };
            metrics::counter!("sprig_graph_mirror_fallbacks_total", "reason" => reason)
                .increment(1);
        }
        self.mirror_ok = Some(ok);
        ok
    }

    /// Whether writers keep the mirrors in their own transaction.
    fn write_mirrors(&self) -> bool {
        self.layout.mirrors && !self.layout.mirror_async
    }

    /// [`GraphStore::reach`] without the mirrors: level by level through
    /// [`GraphStore::expand`] (an incoming scatter of the `rev` key per
    /// level), each node once per origin at its smallest depth, nodes
    /// outside `shard` returned but not expanded, as the CTE does.
    async fn reach_levels(
        &mut self,
        shard: usize,
        seeds: &[(u64, u64)],
        types: &[u32],
        dir: Dir,
        hops: u32,
    ) -> Result<Vec<Reach>> {
        let lo = self.layout.shards[shard].lo;
        let hi = self.layout.hi(shard);
        let inside = |rk: u64| rk >= lo && hi.is_none_or(|h| rk < h);
        let mut best: HashMap<(u64, u64), Reach> = HashMap::new();
        let mut frontier: Vec<(u64, u64)> = seeds.to_vec();
        for d in 1..=hops {
            if frontier.is_empty() {
                break;
            }
            let mut from: Vec<u64> = frontier.iter().map(|(_, n)| *n).collect();
            from.sort_unstable();
            from.dedup();
            let mut by_near: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
            for (e, _) in self.expand(&from, types, dir).await? {
                let (near, far, rk) = if dir == Dir::In {
                    (e.dst, e.src, e.src_rk)
                } else {
                    (e.src, e.dst, e.dst_rk)
                };
                by_near.entry(near).or_default().push((far, rk));
            }
            let mut next = Vec::new();
            for (origin, node) in frontier.drain(..) {
                for &(far, rk) in by_near.get(&node).map_or(&[][..], Vec::as_slice) {
                    if best.contains_key(&(origin, far)) {
                        continue;
                    }
                    best.insert(
                        (origin, far),
                        Reach {
                            origin,
                            node: far,
                            rk,
                            depth: d,
                        },
                    );
                    if inside(rk) {
                        next.push((origin, far));
                    }
                }
            }
            frontier = next;
        }
        Ok(best.into_values().collect())
    }

    /// The graph's indexes (used by scans and `SHOW`), and how schema
    /// commands run.
    #[must_use]
    pub fn with_schema(mut self, indexes: Vec<IndexDef>, ops: Option<Arc<dyn SchemaOps>>) -> Self {
        self.indexes = indexes;
        self.schema = ops;
        self
    }

    /// A duplicate key from an index: the message names the constraint's
    /// label and properties, as Neo4j's does.
    fn duplicate(&self, e: SprigError, what: &str, id: u64) -> SprigError {
        let sprig_common::ErrorKind::Backend { errno: 1062, .. } = e.kind else {
            return e;
        };
        let Some(d) = self
            .indexes
            .iter()
            .find(|d| e.message.contains(&format!("'_sprig_i{}'", d.id)))
        else {
            return e;
        };
        dup_error(d, what, id)
    }

    /// Uniqueness claims are kept (decision G41) for every grouped graph,
    /// whatever its shard count, so splits and moves carry them; graphs
    /// from rounds 1 and 2 are one schema, where MariaDB's `UNIQUE` index
    /// is exact.
    fn claims_kept(&self) -> bool {
        self.layout.group != 0
    }

    /// The uniqueness constraints of `entity` writers maintain claims for
    /// (`building` and `online`).
    fn unique_defs(&self, entity: Entity) -> Vec<IndexDef> {
        if !self.claims_kept() {
            return Vec::new();
        }
        self.indexes
            .iter()
            .filter(|d| {
                d.unique && d.entity == entity && matches!(d.state.as_str(), "building" | "online")
            })
            .cloned()
            .collect()
    }

    /// Claims `key` of constraint `d` for element `owner` (on the shard of
    /// `owner_rk`), in the statement's transaction (decision G41, chapter
    /// 19's `claim` with Pre joined to Main): insert the claim row; when
    /// one is there, share-lock it (the duplicate-key error already holds
    /// a shared lock, so writers queued on one key never deadlock taking
    /// it exclusively) and check its owner with an exclusive read on
    /// the owner's shard (which waits for an in-flight commit of it, and
    /// lets one loser at a time go on to `MERGE`'s `ON MATCH SET` on the
    /// holder: a shared lock there deadlocks two losers upgrading it); a
    /// live owner with the same key is a duplicate (1062), anything else
    /// (an orphan) is taken over.
    async fn claim(
        &mut self,
        d: &IndexDef,
        key: &[u8],
        what: &str,
        owner: u64,
        owner_rk: u64,
    ) -> Result<()> {
        let h = crate::schema::claim_rk(d.id, key);
        let i = self.layout.index_of(h);
        let table = self.t(i).claims.clone();
        let ins = format!(
            "INSERT INTO {table} (idx, k, owner, owner_rk, _sprig_rk) VALUES (?, ?, ?, ?, ?)"
        );
        let vals = || {
            Params::Positional(vec![
                MyValue::from(d.id),
                MyValue::Bytes(key.to_vec()),
                MyValue::from(owner),
                MyValue::from(owner_rk),
                MyValue::from(h),
            ])
        };
        match self.run_at(i, ins.clone(), vals()).await {
            Ok(_) => return Ok(()),
            Err(e) if crate::exec::is_duplicate(&e) => {}
            Err(e) => return Err(e),
        }
        let held = self
            .write_rows_at(
                i,
                format!(
                    "SELECT owner, owner_rk FROM {table} WHERE idx = ? AND k = ? LOCK IN SHARE MODE"
                ),
                Params::Positional(vec![MyValue::from(d.id), MyValue::Bytes(key.to_vec())]),
            )
            .await?;
        let Some(mut row) = held.into_iter().next() else {
            // Released meanwhile: insert again.
            self.run_at(i, ins, vals()).await?;
            return Ok(());
        };
        let (o0, rk0) = (u64_col(row.take(0)), u64_col(row.take(1)));
        if o0 == owner {
            return Ok(());
        }
        let live = match d.entity {
            Entity::Node => self.lock_nodes(&[o0], true).await?.into_iter().any(|n| {
                n.labels.contains(&d.token_id)
                    && crate::schema::claim_key(d, &n.props, n.types.as_ref()).as_deref()
                        == Some(key)
            }),
            Entity::Relationship => {
                let j = self.layout.index_of(rk0);
                let sql = format!(
                    "SELECT {EDGE_COLS} FROM {} e WHERE e.id = {o0} FOR UPDATE",
                    self.t(j).edges
                );
                self.write_rows_at(j, sql, Params::Empty)
                    .await?
                    .into_iter()
                    .map(|mut r| edge_at(&mut r, 0))
                    .any(|e| {
                        e.rtype == d.token_id
                            && crate::schema::claim_key(d, &e.props, e.types.as_ref()).as_deref()
                                == Some(key)
                    })
            }
        };
        if live {
            return Err(dup_error(d, what, o0));
        }
        self.run_at(
            i,
            format!("UPDATE {table} SET owner = ?, owner_rk = ? WHERE idx = ? AND k = ?"),
            Params::Positional(vec![
                MyValue::from(owner),
                MyValue::from(owner_rk),
                MyValue::from(d.id),
                MyValue::Bytes(key.to_vec()),
            ]),
        )
        .await?;
        Ok(())
    }

    /// Releases `key` of `d` held by `owner` (a no-op if someone else
    /// holds it), in the statement's transaction.
    async fn release(&mut self, d: &IndexDef, key: &[u8], owner: u64) -> Result<()> {
        let h = crate::schema::claim_rk(d.id, key);
        let i = self.layout.index_of(h);
        let sql = format!(
            "DELETE FROM {} WHERE idx = ? AND k = ? AND owner = ?",
            self.t(i).claims
        );
        self.run_at(
            i,
            sql,
            Params::Positional(vec![
                MyValue::from(d.id),
                MyValue::Bytes(key.to_vec()),
                MyValue::from(owner),
            ]),
        )
        .await?;
        Ok(())
    }

    /// The claim key of a node under `d`, if it has the label and every
    /// property.
    fn node_key(d: &IndexDef, n: &NodeRow) -> Option<Vec<u8>> {
        if n.labels.contains(&d.token_id) {
            crate::schema::claim_key(d, &n.props, n.types.as_ref())
        } else {
            None
        }
    }

    /// The claim key of a relationship under `d`.
    fn edge_key(d: &IndexDef, e: &EdgeRow) -> Option<Vec<u8>> {
        if e.rtype == d.token_id {
            crate::schema::claim_key(d, &e.props, e.types.as_ref())
        } else {
            None
        }
    }

    /// Edges by id on the shards of their sources' hashes.
    async fn edges_on(&mut self, keys: &[(u64, u64)]) -> Result<Vec<EdgeRow>> {
        let mut by: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
        for (id, rk) in keys {
            by.entry(self.layout.index_of(*rk)).or_default().push(*id);
        }
        let mut out = Vec::new();
        for (i, ids) in by {
            let sql = format!(
                "SELECT {EDGE_COLS} FROM {} e WHERE e.id IN ({})",
                self.t(i).edges,
                ids_list(&ids)
            );
            let rows = self.write_rows_at(i, sql, Params::Empty).await?;
            out.extend(rows.into_iter().map(|mut r| edge_at(&mut r, 0)));
        }
        Ok(out)
    }

    /// An online node index whose first property is `key` on label `l`.
    fn node_index(&self, l: u32, key: &str) -> Option<&IndexDef> {
        self.indexes.iter().find(|d| {
            d.entity == Entity::Node
                && d.token_id == l
                && d.state == "online"
                && d.props.first().is_some_and(|p| p == key)
        })
    }

    fn many(&self) -> bool {
        self.layout.shards.len() > 1
    }

    fn t(&self, i: usize) -> &Tables {
        &self.layout.shards[i].tables
    }

    /// A read on the write connection (locking reads, and reads of rows
    /// the transaction is changing).
    async fn write_rows_at(&mut self, i: usize, sql: String, params: Params) -> Result<Vec<Row>> {
        self.stats.shard_queries += 1;
        let at = self.layout.shards[i].clone();
        let conn = self.conns.conn(&at, true).await?;
        conn.exec(sql, params).await.map_err(backend_error)
    }

    async fn run_at(&mut self, i: usize, sql: String, params: Params) -> Result<u64> {
        self.stats.shard_queries += 1;
        let at = self.layout.shards[i].clone();
        let text = self.conns.records().then(|| inline_params(&sql, &params));
        let conn = self.conns.conn(&at, true).await?;
        conn.exec_drop(sql, params).await.map_err(backend_error)?;
        let n = conn.affected_rows();
        if let Some(t) = text {
            self.conns.wrote(&at, &t);
        }
        Ok(n)
    }

    /// The router's scan step for `s` (G69): an index on the label and a
    /// pushed-down property replaces the label join; property equalities
    /// push down as a superset the gateway rechecks.
    fn scan_op(&self, s: &Scan) -> ScanOp {
        let index = s.label.and_then(|l| {
            s.props_eq.iter().find_map(|(k, vals)| {
                let d = self.node_index(l, k)?;
                let texts: Option<Vec<String>> = vals
                    .iter()
                    .map(|v| crate::schema::index_value(v, d.kind))
                    .collect();
                Some((Tables::index_column(d, 0), texts?))
            })
        });
        let props = s
            .props_eq
            .iter()
            .filter_map(|(k, vals)| {
                let ps = vals
                    .iter()
                    .map(scalar_lits)
                    .collect::<Option<Vec<_>>>()?
                    .concat();
                (!ps.is_empty()).then(|| (k.clone(), ps))
            })
            .collect();
        ScanOp {
            label: s.label,
            index,
            props,
            after: s.after,
            limit: s.limit,
        }
    }

    /// Edges by id with their source node joined (both on the source's
    /// shard): `(source hash, edge id)`, routed by the hash.
    async fn edges_with_src(
        &mut self,
        keys: Vec<(u64, u64)>,
    ) -> Result<Vec<(EdgeRow, Option<NodeRow>)>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for (_, rows) in self
            .route(HopOp::EdgesWithSrc, Targets::ByHash(keys), false)
            .await?
        {
            for mut r in rows {
                let e = edge_at(&mut r, 0);
                let n = node_at(&mut r, 8);
                out.push((e, n));
            }
        }
        Ok(out)
    }

    /// Mirror rows of edges into `dsts` (on the destinations' shards):
    /// `(source hash, edge id)`.
    async fn mirrors_into(&mut self, dsts: &[u64], types: &[u32]) -> Result<Vec<(u64, u64)>> {
        if dsts.is_empty() {
            return Ok(Vec::new());
        }
        let op = HopOp::Mirrors {
            types: types.to_vec(),
        };
        let mut out = Vec::new();
        for (_, rows) in self.route(op, Targets::ByKey(dsts.to_vec()), false).await? {
            for mut r in rows {
                let id = u64_col(r.take(0));
                let rk = u64_col(r.take(1));
                out.push((rk, id));
            }
        }
        Ok(out)
    }
}

/// Node rows of a route's statements.
fn nodes_of(rows: Vec<(usize, Vec<Row>)>) -> Vec<NodeRow> {
    rows.into_iter()
        .flat_map(|(_, rs)| rs)
        .filter_map(|mut r| node_at(&mut r, 0))
        .collect()
}

/// Neo4j's message for a uniqueness violation: the holder, the label (or
/// type) and the properties (1062, `ConstraintValidationFailed`).
fn dup_error(d: &IndexDef, what: &str, id: u64) -> SprigError {
    SprigError::backend(
        1062,
        *b"23000",
        format!(
            "{what}({id}) already exists with {} `{}` and propert{} {}",
            if d.entity == Entity::Node {
                "label"
            } else {
                "type"
            },
            d.token,
            if d.props.len() == 1 { "y" } else { "ies" },
            d.props
                .iter()
                .map(|p| format!("`{p}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    )
}

/// Sorts rows by id and keeps the first `limit`.
fn merge_nodes(mut v: Vec<NodeRow>, limit: Option<usize>) -> Vec<NodeRow> {
    v.sort_by_key(|n| n.id);
    v.dedup_by_key(|n| n.id);
    if let Some(l) = limit {
        v.truncate(l);
    }
    v
}

#[async_trait]
impl GraphStore for MariaStore<'_> {
    fn graph_id(&self) -> u64 {
        self.layout.graph
    }

    fn shards(&self) -> usize {
        self.layout.shards.len()
    }

    fn shard_of(&self, rk: u64) -> usize {
        self.layout.index_of(rk)
    }

    fn stats(&self) -> StoreStats {
        self.stats
    }

    async fn batches(&mut self) -> Result<()> {
        if self.conns.batches() {
            Ok(())
        } else {
            Err(crate::store::not_implicit())
        }
    }

    async fn end_batch(&mut self, ok: bool, deleted: &[u64]) -> Result<()> {
        if ok && !deleted.is_empty() {
            let layout = Arc::clone(&self.layout);
            if let Err(e) = check_deleted(&mut *self.conns, &layout, deleted).await {
                let _ = self.conns.end_batch(false).await;
                return Err(e);
            }
        }
        // Mirrors' freshness is asked again in the next batch.
        self.mirror_ok = None;
        self.conns.end_batch(ok).await
    }

    fn explain_routes(&self) -> Vec<String> {
        sprig_router::graph_route::explain_lines(&self.group, self.layout.mirror_async)
    }

    fn routes(&self) -> Vec<String> {
        self.routes
            .iter()
            .map(|((op, kind), (n, stmts, shards))| {
                format!(
                    "GraphRoute {op}: {n} {kind} plan(s), {stmts} statement(s) on {}",
                    shards.iter().cloned().collect::<Vec<_>>().join(", ")
                )
            })
            .collect()
    }

    async fn scan(&mut self, s: &Scan) -> Result<Vec<NodeRow>> {
        let targets = match &s.ids {
            Some(ids) => Targets::ByKey(ids.clone()),
            None => Targets::All(Vec::new()),
        };
        let op = HopOp::Scan(self.scan_op(s));
        let rows = self.route(op, targets, false).await?;
        Ok(merge_nodes(nodes_of(rows), s.limit))
    }

    async fn nodes(&mut self, ids: &[u64]) -> Result<Vec<NodeRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .route(HopOp::Nodes, Targets::ByKey(ids.to_vec()), false)
            .await?;
        Ok(nodes_of(rows))
    }

    async fn expand(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
    ) -> Result<Vec<(EdgeRow, Option<NodeRow>)>> {
        if from.is_empty() {
            return Ok(Vec::new());
        }
        if dir == Dir::Both {
            return Err(SprigError::internal("expand takes one direction"));
        }
        if dir == Dir::In && self.read_mirrors().await {
            // The mirrors on the destinations' shards, then the edges (with
            // their sources) on the sources' shards.
            let by_src = self.mirrors_into(from, types).await?;
            return self.edges_with_src(by_src).await;
        }
        // Outgoing: each source's shard. Incoming without mirrors: the
        // `rev` key on every shard (one shard, or a scatter).
        let out_dir = dir == Dir::Out;
        let targets = if out_dir {
            Targets::ByKey(from.to_vec())
        } else {
            Targets::All(from.to_vec())
        };
        let op = HopOp::Expand {
            out: out_dir,
            types: types.to_vec(),
        };
        let mut out = Vec::new();
        for (_, rows) in self.route(op, targets, false).await? {
            for mut r in rows {
                let e = edge_at(&mut r, 0);
                let n = node_at(&mut r, 8);
                out.push((e, n));
            }
        }
        Ok(out)
    }

    async fn reach(
        &mut self,
        shard: usize,
        seeds: &[(u64, u64)],
        types: &[u32],
        dir: Dir,
        hops: u32,
    ) -> Result<Vec<Reach>> {
        if seeds.is_empty() || hops == 0 {
            return Ok(Vec::new());
        }
        if shard >= self.layout.shards.len() {
            return Err(SprigError::internal(
                "reach on a shard the graph doesn't have",
            ));
        }
        // Incoming over several shards recurses over the mirrors, whose
        // rows are local to their destination as edges are to their source;
        // lagging (or no) mirrors: level by level over a scatter of `rev`.
        if dir == Dir::In && self.many() && !self.read_mirrors().await {
            return self.reach_levels(shard, seeds, types, dir, hops).await;
        }
        if dir == Dir::Both {
            return Err(SprigError::internal("reach takes one direction"));
        }
        self.stats.cte_queries += 1;
        let op = HopOp::Reach {
            seeds: seeds.to_vec(),
            types: types.to_vec(),
            out: dir == Dir::Out,
            hops,
        };
        let rows = self.route(op, Targets::Shard(shard), false).await?;
        Ok(rows
            .into_iter()
            .flat_map(|(_, rs)| rs)
            .map(|mut r| Reach {
                origin: u64_col(r.take(0)),
                node: u64_col(r.take(1)),
                rk: u64_col(r.take(2)),
                depth: u32::try_from(u64_col(r.take(3))).unwrap_or(u32::MAX),
            })
            .collect())
    }

    async fn edges(&mut self, ids: &[u64]) -> Result<Vec<EdgeRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        // An edge id doesn't name its shard: every shard's `uk_id`.
        let rows = self
            .route(HopOp::Edges, Targets::All(ids.to_vec()), false)
            .await?;
        Ok(rows
            .into_iter()
            .flat_map(|(_, rs)| rs)
            .map(|mut r| edge_at(&mut r, 0))
            .collect())
    }

    async fn edges_touching(&mut self, nodes: &[u64], limit: usize) -> Result<Vec<EdgeRow>> {
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        let mut out: Vec<EdgeRow> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        // Outgoing, on the nodes' shards.
        let op = HopOp::Touching { out: true, limit };
        for (_, rows) in self
            .route(op, Targets::ByKey(nodes.to_vec()), false)
            .await?
        {
            for mut r in rows {
                let e = edge_at(&mut r, 0);
                if seen.insert(e.id) {
                    out.push(e);
                }
            }
        }
        if out.len() > limit {
            return Ok(out);
        }
        // Incoming: mirrors, the `rev` key of one shard, or a scatter.
        if self.read_mirrors().await {
            let by_src = self.mirrors_into(nodes, &[]).await?;
            for (e, _) in self.edges_with_src(by_src).await? {
                if seen.insert(e.id) {
                    out.push(e);
                }
            }
        } else {
            let op = HopOp::Touching { out: false, limit };
            for (_, rows) in self.route(op, Targets::All(nodes.to_vec()), false).await? {
                for mut r in rows {
                    let e = edge_at(&mut r, 0);
                    if seen.insert(e.id) {
                        out.push(e);
                    }
                }
            }
        }
        out.truncate(limit.saturating_add(1));
        Ok(out)
    }

    async fn lock_nodes(&mut self, ids: &[u64], exclusive: bool) -> Result<Vec<NodeRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .route(
                HopOp::LockNodes { exclusive },
                Targets::ByKey(ids.to_vec()),
                true,
            )
            .await?;
        Ok(nodes_of(rows))
    }

    async fn alloc_ids(&mut self, kind: IdKind, n: usize) -> Result<Vec<u64>> {
        self.ids.take(kind, n).await
    }

    async fn insert_nodes(&mut self, rows: &[NodeRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        for d in self.unique_defs(Entity::Node) {
            for r in rows {
                if let Some(k) = Self::node_key(&d, r) {
                    self.claim(&d, &k, "Node", r.id, r.rk).await?;
                }
            }
        }
        let mut by: BTreeMap<usize, Vec<&NodeRow>> = BTreeMap::new();
        for r in rows {
            by.entry(self.layout.index_of(r.rk)).or_default().push(r);
        }
        for (i, part) in by {
            let t = self.t(i).clone();
            let mut sql = format!(
                "INSERT INTO {} (id, part, labels, props, _types, _v, _sprig_rk) VALUES ",
                t.nodes
            );
            let mut params: Vec<MyValue> = Vec::new();
            let mut lsql = format!(
                "INSERT IGNORE INTO {} (label, id, _sprig_rk) VALUES ",
                t.labels
            );
            let mut lparams: Vec<MyValue> = Vec::new();
            for (k, r) in part.iter().enumerate() {
                if k > 0 {
                    sql.push_str(", ");
                }
                sql.push_str("(?, ?, ?, ?, ?, ?, ?)");
                params.extend([
                    MyValue::from(r.id),
                    MyValue::from(r.id),
                    MyValue::from(json_text(&Json::from(r.labels.clone()))),
                    MyValue::from(json_text(&r.props)),
                    r.types
                        .as_ref()
                        .map_or(MyValue::NULL, |t| MyValue::from(json_text(t))),
                    MyValue::from(self.now_us),
                    MyValue::from(r.rk),
                ]);
                for l in &r.labels {
                    if !lparams.is_empty() {
                        lsql.push_str(", ");
                    }
                    lsql.push_str("(?, ?, ?)");
                    lparams.extend([MyValue::from(*l), MyValue::from(r.id), MyValue::from(r.rk)]);
                }
            }
            let first = part[0].id;
            if let Err(e) = self.run_at(i, sql, Params::Positional(params)).await {
                return Err(self.duplicate(e, "Node", first));
            }
            if !lparams.is_empty() {
                self.run_at(i, lsql, Params::Positional(lparams)).await?;
            }
        }
        Ok(())
    }

    async fn insert_edges(&mut self, rows: &[EdgeRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        for d in self.unique_defs(Entity::Relationship) {
            for r in rows {
                if let Some(k) = Self::edge_key(&d, r) {
                    self.claim(&d, &k, "Relationship", r.id, r.src_rk).await?;
                }
            }
        }
        let mut by: BTreeMap<usize, Vec<&EdgeRow>> = BTreeMap::new();
        for r in rows {
            by.entry(self.layout.index_of(r.src_rk))
                .or_default()
                .push(r);
        }
        for (i, part) in by {
            let mut sql = format!(
                "INSERT INTO {} (src, type, dst, id, dst_rk, props, _types, _v, _sprig_rk) VALUES ",
                self.t(i).edges
            );
            let mut params: Vec<MyValue> = Vec::new();
            for (k, r) in part.iter().enumerate() {
                if k > 0 {
                    sql.push_str(", ");
                }
                sql.push_str("(?, ?, ?, ?, ?, ?, ?, ?, ?)");
                params.extend([
                    MyValue::from(r.src),
                    MyValue::from(r.rtype),
                    MyValue::from(r.dst),
                    MyValue::from(r.id),
                    MyValue::from(r.dst_rk),
                    MyValue::from(json_text(&r.props)),
                    r.types
                        .as_ref()
                        .map_or(MyValue::NULL, |t| MyValue::from(json_text(t))),
                    MyValue::from(self.now_us),
                    MyValue::from(r.src_rk),
                ]);
            }
            let first = part[0].id;
            if let Err(e) = self.run_at(i, sql, Params::Positional(params)).await {
                return Err(self.duplicate(e, "Relationship", first));
            }
        }
        if !self.write_mirrors() {
            return Ok(());
        }
        // Mirrors on the destinations' shards, in the same transaction.
        let mut by: BTreeMap<usize, Vec<&EdgeRow>> = BTreeMap::new();
        for r in rows {
            by.entry(self.layout.index_of(r.dst_rk))
                .or_default()
                .push(r);
        }
        for (i, part) in by {
            let mut sql = format!(
                "INSERT INTO {} (dst, type, src, id, src_rk, _v, _sprig_rk) VALUES ",
                self.t(i).mirrors
            );
            let mut params: Vec<MyValue> = Vec::new();
            for (k, r) in part.iter().enumerate() {
                if k > 0 {
                    sql.push_str(", ");
                }
                sql.push_str("(?, ?, ?, ?, ?, ?, ?)");
                params.extend([
                    MyValue::from(r.dst),
                    MyValue::from(r.rtype),
                    MyValue::from(r.src),
                    MyValue::from(r.id),
                    MyValue::from(r.src_rk),
                    MyValue::from(self.now_us),
                    MyValue::from(r.dst_rk),
                ]);
            }
            self.run_at(i, sql, Params::Positional(params)).await?;
        }
        Ok(())
    }

    async fn update_node(&mut self, row: &NodeRow, old_labels: &[u32]) -> Result<()> {
        let defs: Vec<IndexDef> = self
            .unique_defs(Entity::Node)
            .into_iter()
            .filter(|d| row.labels.contains(&d.token_id) || old_labels.contains(&d.token_id))
            .collect();
        if !defs.is_empty() {
            // The row as the transaction sees it (the caller locked it).
            let old = self.lock_nodes(&[row.id], true).await?;
            for d in &defs {
                let was = old.first().and_then(|o| Self::node_key(d, o));
                let now = Self::node_key(d, row);
                if was == now {
                    continue;
                }
                if let Some(k) = &now {
                    self.claim(d, k, "Node", row.id, row.rk).await?;
                }
                if let Some(k) = &was {
                    self.release(d, k, row.id).await?;
                }
            }
        }
        let i = self.layout.index_of(row.rk);
        let t = self.t(i).clone();
        let r = self
            .run_at(
                i,
                format!(
                    "UPDATE {} SET labels = ?, props = ?, _types = ?, _v = ? WHERE id = ?",
                    t.nodes
                ),
                Params::Positional(vec![
                    MyValue::from(json_text(&Json::from(row.labels.clone()))),
                    MyValue::from(json_text(&row.props)),
                    row.types
                        .as_ref()
                        .map_or(MyValue::NULL, |t| MyValue::from(json_text(t))),
                    MyValue::from(self.now_us),
                    MyValue::from(row.id),
                ]),
            )
            .await;
        if let Err(e) = r {
            return Err(self.duplicate(e, "Node", row.id));
        }
        let removed: Vec<u64> = old_labels
            .iter()
            .filter(|l| !row.labels.contains(l))
            .map(|l| u64::from(*l))
            .collect();
        if !removed.is_empty() {
            self.run_at(
                i,
                format!(
                    "DELETE FROM {} WHERE id = ? AND label IN ({})",
                    t.labels,
                    ids_list(&removed)
                ),
                Params::Positional(vec![MyValue::from(row.id)]),
            )
            .await?;
        }
        let added: Vec<u32> = row
            .labels
            .iter()
            .copied()
            .filter(|l| !old_labels.contains(l))
            .collect();
        for l in added {
            self.run_at(
                i,
                format!(
                    "INSERT IGNORE INTO {} (label, id, _sprig_rk) VALUES (?, ?, ?)",
                    t.labels
                ),
                Params::Positional(vec![
                    MyValue::from(l),
                    MyValue::from(row.id),
                    MyValue::from(row.rk),
                ]),
            )
            .await?;
        }
        Ok(())
    }

    async fn update_edge(&mut self, row: &EdgeRow) -> Result<()> {
        let defs: Vec<IndexDef> = self
            .unique_defs(Entity::Relationship)
            .into_iter()
            .filter(|d| d.token_id == row.rtype)
            .collect();
        if !defs.is_empty() {
            let old = self.edges_on(&[(row.id, row.src_rk)]).await?;
            for d in &defs {
                let was = old.first().and_then(|o| Self::edge_key(d, o));
                let now = Self::edge_key(d, row);
                if was == now {
                    continue;
                }
                if let Some(k) = &now {
                    self.claim(d, k, "Relationship", row.id, row.src_rk).await?;
                }
                if let Some(k) = &was {
                    self.release(d, k, row.id).await?;
                }
            }
        }
        let i = self.layout.index_of(row.src_rk);
        let sql = format!(
            "UPDATE {} SET props = ?, _types = ?, _v = ? WHERE id = ?",
            self.t(i).edges
        );
        let r = self
            .run_at(
                i,
                sql,
                Params::Positional(vec![
                    MyValue::from(json_text(&row.props)),
                    row.types
                        .as_ref()
                        .map_or(MyValue::NULL, |t| MyValue::from(json_text(t))),
                    MyValue::from(self.now_us),
                    MyValue::from(row.id),
                ]),
            )
            .await;
        if let Err(e) = r {
            return Err(self.duplicate(e, "Relationship", row.id));
        }
        Ok(())
    }

    async fn delete_edges(&mut self, edges: &[EdgeKey]) -> Result<u64> {
        if edges.is_empty() {
            return Ok(0);
        }
        let defs: Vec<IndexDef> = self
            .unique_defs(Entity::Relationship)
            .into_iter()
            .filter(|d| edges.iter().any(|e| e.rtype == d.token_id))
            .collect();
        if !defs.is_empty() {
            let keys: Vec<(u64, u64)> = edges.iter().map(|e| (e.id, e.src_rk)).collect();
            for e in self.edges_on(&keys).await? {
                for d in &defs {
                    if let Some(k) = Self::edge_key(d, &e) {
                        self.release(d, &k, e.id).await?;
                    }
                }
            }
        }
        let mut n = 0;
        let mut by: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
        for e in edges {
            by.entry(self.layout.index_of(e.src_rk))
                .or_default()
                .push(e.id);
        }
        for (i, ids) in by {
            let sql = format!(
                "DELETE FROM {} WHERE id IN ({})",
                self.t(i).edges,
                ids_list(&ids)
            );
            n += self.run_at(i, sql, Params::Empty).await?;
        }
        if self.write_mirrors() {
            let mut by: BTreeMap<usize, (Vec<u64>, Vec<u64>)> = BTreeMap::new();
            for e in edges {
                let x = by.entry(self.layout.index_of(e.dst_rk)).or_default();
                x.0.push(e.dst);
                x.1.push(e.id);
            }
            for (i, (dsts, ids)) in by {
                let sql = format!(
                    "DELETE FROM {} WHERE dst IN ({}) AND id IN ({})",
                    self.t(i).mirrors,
                    ids_list(&dsts),
                    ids_list(&ids)
                );
                self.run_at(i, sql, Params::Empty).await?;
            }
        }
        Ok(n)
    }

    async fn delete_nodes(&mut self, ids: &[u64]) -> Result<u64> {
        let defs = self.unique_defs(Entity::Node);
        if !defs.is_empty() && !ids.is_empty() {
            for o in self.lock_nodes(ids, true).await? {
                for d in &defs {
                    if let Some(k) = Self::node_key(d, &o) {
                        self.release(d, &k, o.id).await?;
                    }
                }
            }
        }
        let mut n = 0;
        for (i, part) in self.layout.by_shard(ids) {
            let l = ids_list(&part);
            let t = self.t(i).clone();
            self.run_at(
                i,
                format!("DELETE FROM {} WHERE id IN ({l})", t.labels),
                Params::Empty,
            )
            .await?;
            n += self
                .run_at(
                    i,
                    format!("DELETE FROM {} WHERE id IN ({l})", t.nodes),
                    Params::Empty,
                )
                .await?;
        }
        Ok(n)
    }

    async fn label_id(&mut self, name: &str, create: bool) -> Result<Option<u32>> {
        if let Some((id, _)) = self.labels.iter().find(|(_, n)| *n == name) {
            return Ok(Some(*id));
        }
        if !create {
            return Ok(None);
        }
        let id = self.dict.create(true, name).await?;
        self.labels.insert(id, name.to_owned());
        Ok(Some(id))
    }

    async fn type_id(&mut self, name: &str, create: bool) -> Result<Option<u32>> {
        if let Some((id, _)) = self.types.iter().find(|(_, n)| *n == name) {
            return Ok(Some(*id));
        }
        if !create {
            return Ok(None);
        }
        let id = self.dict.create(false, name).await?;
        self.types.insert(id, name.to_owned());
        Ok(Some(id))
    }

    fn label_name(&self, id: u32) -> Option<String> {
        self.labels.get(&id).cloned()
    }

    fn type_name(&self, id: u32) -> Option<String> {
        self.types.get(&id).cloned()
    }

    fn all_labels(&self) -> Vec<String> {
        self.labels.values().cloned().collect()
    }

    fn all_types(&self) -> Vec<String> {
        self.types.values().cloned().collect()
    }

    fn indexes(&self) -> Vec<IndexDef> {
        self.indexes.clone()
    }

    async fn schema(&mut self, cmd: &Schema) -> Result<SchemaCounts> {
        match &self.schema {
            Some(ops) => ops.run(cmd).await,
            None => Err(SprigError::new(
                SprigCode::GraphUnsupported,
                "index and constraint commands need the gateway",
            )),
        }
    }
}

/// The commit check of chapter 43: nodes removed with a plain `DELETE`
/// must have no relationships left (39906): outgoing edges on each node's
/// shard; incoming ones through the mirrors there, the `rev` key of a
/// one-shard graph, or every shard.
///
/// # Errors
/// 39906 naming a node; a backend error.
pub async fn check_deleted(
    conns: &mut dyn ShardConns,
    layout: &GraphShards,
    deleted: &[u64],
) -> Result<()> {
    let many = layout.shards.len() > 1;
    // Asynchronous mirrors may lag: the commit check scatters instead.
    let mirrors = layout.mirrors && !layout.mirror_async && !layout.mirror_building;
    for batch in deleted.chunks(1000) {
        let mut hit: Option<u64> = None;
        for (i, part) in layout.by_shard(batch) {
            let at = &layout.shards[i];
            let l = ids_list(&part);
            let incoming = if many && mirrors {
                format!(
                    " UNION SELECT dst FROM {} WHERE dst IN ({l})",
                    at.tables.mirrors
                )
            } else if many {
                String::new()
            } else {
                format!(
                    " UNION SELECT dst FROM {} WHERE dst IN ({l})",
                    at.tables.edges
                )
            };
            let conn = conns.conn(at, false).await?;
            let h: Option<u64> = conn
                .query_first(format!(
                    "SELECT src FROM {} WHERE src IN ({l}){incoming} LIMIT 1",
                    at.tables.edges
                ))
                .await
                .map_err(backend_error)?;
            if h.is_some() {
                hit = h;
                break;
            }
        }
        if hit.is_none() && many && !mirrors {
            let l = ids_list(batch);
            for at in &layout.shards {
                let conn = conns.conn(at, false).await?;
                let h: Option<u64> = conn
                    .query_first(format!(
                        "SELECT dst FROM {} WHERE dst IN ({l}) LIMIT 1",
                        at.tables.edges
                    ))
                    .await
                    .map_err(backend_error)?;
                if h.is_some() {
                    hit = h;
                    break;
                }
            }
        }
        if let Some(n) = hit {
            // The node is deleted when one of its ends is; report that one.
            let id = if batch.contains(&n) { n } else { batch[0] };
            return Err(SprigError::new(
                SprigCode::GraphNodeHasRelationships,
                format!(
                    "Cannot delete node<{id}>, because it still has relationships. To delete this node, you must first delete its relationships."
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod inline_tests {
    use super::*;

    #[test]
    fn values_go_in_outside_quotes() {
        let p = Params::Positional(vec![
            MyValue::from(7u64),
            MyValue::from("it's {\"a\": \"?\"}"),
            MyValue::NULL,
        ]);
        assert_eq!(
            inline_params(
                "INSERT INTO `s?`.t (a, b, c) VALUES (?, ?, ?) /* '?' */",
                &p
            ),
            "INSERT INTO `s?`.t (a, b, c) VALUES (7, 'it\\'s {\\\"a\\\": \\\"?\\\"}', NULL) /* '?' */"
        );
        assert_eq!(
            inline_params("DELETE FROM t", &Params::Empty),
            "DELETE FROM t"
        );
    }
}
