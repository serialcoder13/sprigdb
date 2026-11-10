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
//! - **Mirrors** (`graph.mirror = sync`): every edge of a grouped graph has a
//!   mirror row written in the same transaction, whatever the shard count,
//!   so splits carry them with no rebuild.
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
use crate::store::{
    EdgeKey, EdgeRow, GraphStore, IdKind, IndexDef, NodeRow, Reach, Scan, SchemaCounts,
    StoreStats, rk_of,
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
        }
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
}

impl std::fmt::Debug for MariaStore<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MariaStore")
            .field("graph", &self.layout.graph)
            .field("shards", &self.layout.shards.len())
            .finish_non_exhaustive()
    }
}

const NODE_COLS: &str = "n.id, n._sprig_rk, n.labels, n.props, n._types";

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

const EDGE_COLS: &str = "e.src, e.type, e.dst, e.id, e.dst_rk, e._sprig_rk, e.props, e._types";

fn json_text(j: &Json) -> String {
    j.to_string()
}

/// A pushed-down JSON scalar as parameters (a superset the gateway
/// rechecks): strings and integers as themselves; booleans as the texts
/// `JSON_VALUE` may answer (`1` on MariaDB 11.8, `true` elsewhere).
fn scalar_params(j: &Json) -> Option<Vec<MyValue>> {
    match j {
        Json::String(s) => Some(vec![MyValue::from(s.as_str())]),
        Json::Number(n) => n.as_i64().map(|i| vec![MyValue::from(i)]),
        Json::Bool(true) => Some(vec![MyValue::from("1"), MyValue::from("true")]),
        Json::Bool(false) => Some(vec![MyValue::from("0"), MyValue::from("false")]),
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
            layout,
            labels,
            types,
            dict,
            ids,
            stats: StoreStats::default(),
            now_us,
            indexes: Vec::new(),
            schema: None,
        }
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

    async fn rows_at(&mut self, i: usize, sql: String, params: Params) -> Result<Vec<Row>> {
        self.stats.shard_queries += 1;
        let at = self.layout.shards[i].clone();
        let conn = self.conns.conn(&at, false).await?;
        conn.exec(sql, params).await.map_err(backend_error)
    }

    async fn locked_rows_at(&mut self, i: usize, sql: String) -> Result<Vec<Row>> {
        self.stats.shard_queries += 1;
        let at = self.layout.shards[i].clone();
        let conn = self.conns.conn(&at, true).await?;
        conn.exec(sql, Params::Empty).await.map_err(backend_error)
    }

    async fn run_at(&mut self, i: usize, sql: String, params: Params) -> Result<u64> {
        self.stats.shard_queries += 1;
        let at = self.layout.shards[i].clone();
        let conn = self.conns.conn(&at, true).await?;
        conn.exec_drop(sql, params).await.map_err(backend_error)?;
        Ok(conn.affected_rows())
    }

    async fn node_rows_at(&mut self, i: usize, sql: String, params: Params) -> Result<Vec<NodeRow>> {
        let rows = self.rows_at(i, sql, params).await?;
        Ok(rows
            .into_iter()
            .filter_map(|mut r| node_at(&mut r, 0))
            .collect())
    }

    /// One shard's scan statement for `s` (with `ids` in place of `s.ids`);
    /// `None` when it can match nothing.
    fn scan_sql(&self, i: usize, s: &Scan, ids: Option<&[u64]>) -> Option<(String, Vec<MyValue>)> {
        let t = self.t(i);
        let mut sql = format!("SELECT {NODE_COLS} FROM {} n", t.nodes);
        let mut params: Vec<MyValue> = Vec::new();
        // An index on the label and a pushed-down property: its column
        // (non-null only for the label) replaces the label join.
        let indexed = s.label.and_then(|l| {
            s.props_eq.iter().find_map(|(k, vals)| {
                let d = self.node_index(l, k)?;
                let texts: Option<Vec<String>> = vals
                    .iter()
                    .map(|v| crate::schema::index_value(v, d.kind))
                    .collect();
                Some((Tables::index_column(d, 0), texts?))
            })
        });
        if let Some((col, texts)) = &indexed {
            sql.push_str(" WHERE 1 = 1");
            if texts.is_empty() {
                return None;
            }
            let marks = vec!["?"; texts.len()].join(", ");
            let _ = write!(sql, " AND n.`{col}` IN ({marks})");
            params.extend(texts.iter().map(|t| MyValue::from(t.as_str())));
        } else if let Some(l) = s.label {
            let _ = write!(sql, " JOIN {} l ON l.id = n.id AND l.label = ?", t.labels);
            params.push(MyValue::from(l));
            sql.push_str(" WHERE 1 = 1");
        } else {
            sql.push_str(" WHERE 1 = 1");
        }
        if let Some(ids) = ids {
            if ids.is_empty() {
                return None;
            }
            let _ = write!(sql, " AND n.id IN ({})", ids_list(ids));
        }
        for (k, vals) in &s.props_eq {
            if k.contains('"') || k.contains('\\') || k.contains('\'') {
                continue; // rechecked only
            }
            let Some(ps) = vals
                .iter()
                .map(scalar_params)
                .collect::<Option<Vec<_>>>()
                .map(|v| v.concat())
            else {
                continue;
            };
            if ps.is_empty() {
                continue;
            }
            let marks = vec!["?"; ps.len()].join(", ");
            let _ = write!(sql, " AND JSON_VALUE(n.props, '$.\"{k}\"') IN ({marks})");
            params.extend(ps);
        }
        if let Some(a) = s.after {
            let _ = write!(sql, " AND n.id > {a} ORDER BY n.id");
        }
        if let Some(l) = s.limit {
            let _ = write!(sql, " LIMIT {l}");
        }
        Some((sql, params))
    }

    /// Edges by id with their source node joined (both on the source's
    /// shard), from the shards `by_shard` names.
    async fn edges_with_src(
        &mut self,
        by_shard: BTreeMap<usize, Vec<u64>>,
    ) -> Result<Vec<(EdgeRow, Option<NodeRow>)>> {
        let mut out = Vec::new();
        for (i, ids) in by_shard {
            for batch in ids.chunks(1000) {
                let t = self.t(i);
                let sql = format!(
                    "SELECT {EDGE_COLS}, {NODE_COLS} FROM {} e LEFT JOIN {} n ON n.id = e.src \
                     WHERE e.id IN ({})",
                    t.edges,
                    t.nodes,
                    ids_list(batch)
                );
                for mut r in self.rows_at(i, sql, Params::Empty).await? {
                    let e = edge_at(&mut r, 0);
                    let n = node_at(&mut r, 8);
                    out.push((e, n));
                }
            }
        }
        Ok(out)
    }

    /// Mirror rows `(src, id, src_rk)` of edges into `dsts` (on the
    /// destinations' shards), grouped by the shard of their source.
    async fn mirrors_into(
        &mut self,
        dsts: &[u64],
        types: &[u32],
    ) -> Result<BTreeMap<usize, Vec<u64>>> {
        let mut out: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
        for (i, ids) in self.layout.by_shard(dsts) {
            let mut sql = format!(
                "SELECT m.id, m.src_rk FROM {} m WHERE m.dst IN ({})",
                self.t(i).mirrors,
                ids_list(&ids)
            );
            if !types.is_empty() {
                let t: Vec<u64> = types.iter().map(|x| u64::from(*x)).collect();
                let _ = write!(sql, " AND m.type IN ({})", ids_list(&t));
            }
            for mut r in self.rows_at(i, sql, Params::Empty).await? {
                let id = u64_col(r.take(0));
                let rk = u64_col(r.take(1));
                out.entry(self.layout.index_of(rk)).or_default().push(id);
            }
        }
        Ok(out)
    }
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

    async fn scan(&mut self, s: &Scan) -> Result<Vec<NodeRow>> {
        let mut out = Vec::new();
        if let Some(ids) = &s.ids {
            for (i, part) in self.layout.by_shard(ids) {
                if let Some((sql, p)) = self.scan_sql(i, s, Some(&part)) {
                    out.extend(self.node_rows_at(i, sql, Params::Positional(p)).await?);
                }
            }
        } else {
            for i in 0..self.layout.shards.len() {
                if let Some((sql, p)) = self.scan_sql(i, s, None) {
                    out.extend(self.node_rows_at(i, sql, Params::Positional(p)).await?);
                }
            }
        }
        Ok(merge_nodes(out, s.limit))
    }

    async fn nodes(&mut self, ids: &[u64]) -> Result<Vec<NodeRow>> {
        let mut out = Vec::new();
        for (i, part) in self.layout.by_shard(ids) {
            let sql = format!(
                "SELECT {NODE_COLS} FROM {} n WHERE n.id IN ({})",
                self.t(i).nodes,
                ids_list(&part)
            );
            out.extend(self.node_rows_at(i, sql, Params::Empty).await?);
        }
        Ok(out)
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
        let type_filter = if types.is_empty() {
            String::new()
        } else {
            let t: Vec<u64> = types.iter().map(|x| u64::from(*x)).collect();
            format!(" AND e.type IN ({})", ids_list(&t))
        };
        if dir == Dir::Both {
            return Err(SprigError::internal("expand takes one direction"));
        }
        if dir == Dir::In && self.many() && self.layout.mirrors {
            // The mirrors on the destinations' shards, then the edges (with
            // their sources) on the sources' shards.
            let by_src = self.mirrors_into(from, types).await?;
            return self.edges_with_src(by_src).await;
        }
        let (near, far) = if dir == Dir::Out {
            ("src", "dst")
        } else {
            ("dst", "src")
        };
        // Outgoing: each source's shard. Incoming without mirrors: the
        // `rev` key on every shard (one shard, or a scatter).
        let plan: Vec<(usize, Vec<u64>)> = if dir == Dir::Out {
            self.layout.by_shard(from).into_iter().collect()
        } else {
            (0..self.layout.shards.len())
                .map(|i| (i, from.to_vec()))
                .collect()
        };
        let mut out = Vec::new();
        for (i, ids) in plan {
            let t = self.t(i);
            let sql = format!(
                "SELECT {EDGE_COLS}, {NODE_COLS} FROM {} e LEFT JOIN {} n ON n.id = e.{far} \
                 WHERE e.{near} IN ({}){type_filter}",
                t.edges,
                t.nodes,
                ids_list(&ids)
            );
            for mut r in self.rows_at(i, sql, Params::Empty).await? {
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
            return Err(SprigError::internal("reach on a shard the graph doesn't have"));
        }
        self.stats.cte_queries += 1;
        // Incoming over several shards recurses over the mirrors, whose
        // rows are local to their destination as edges are to their source.
        let t = self.t(shard);
        let (table, near, far, far_rk) = match dir {
            Dir::Out => (t.edges.clone(), "src", "dst", "dst_rk"),
            Dir::In if self.many() => {
                if !self.layout.mirrors {
                    return Err(SprigError::internal(
                        "an incoming search over several shards needs mirrors",
                    ));
                }
                (t.mirrors.clone(), "dst", "src", "src_rk")
            }
            Dir::In => (t.edges.clone(), "dst", "src", "_sprig_rk"),
            Dir::Both => return Err(SprigError::internal("reach takes one direction")),
        };
        let mut seed_sql = String::new();
        for (i, (o, n)) in seeds.iter().enumerate() {
            if i > 0 {
                seed_sql.push_str(" UNION ALL ");
            }
            let _ = write!(seed_sql, "SELECT {o} AS o, {n} AS n");
        }
        let type_filter = if types.is_empty() {
            String::new()
        } else {
            let t: Vec<u64> = types.iter().map(|x| u64::from(*x)).collect();
            format!(" AND e.type IN ({})", ids_list(&t))
        };
        // Only nodes inside this shard's range continue: the others are
        // boundary nodes for the gateway's frontier.
        let mut range = String::new();
        if self.many() {
            let lo = self.layout.shards[shard].lo;
            let _ = write!(range, " AND r.rk >= {lo}");
            if let Some(hi) = self.layout.hi(shard) {
                let _ = write!(range, " AND r.rk < {hi}");
            }
        }
        let sql = format!(
            "SET STATEMENT max_recursive_iterations = {iters} FOR \
             WITH RECURSIVE r (o, id, rk, d) AS ( \
               SELECT s.o, e.{far}, e.{far_rk}, 1 FROM ({seed_sql}) s \
                 JOIN {table} e ON e.{near} = s.n{type_filter} \
               UNION \
               SELECT r.o, e.{far}, e.{far_rk}, r.d + 1 FROM r \
                 JOIN {table} e ON e.{near} = r.id{type_filter} \
                WHERE r.d < {hops}{range} \
             ) CYCLE o, id RESTRICT \
             SELECT o, id, rk, MIN(d) FROM r GROUP BY o, id, rk",
            iters = hops + 1,
        );
        let rows = self.rows_at(shard, sql, Params::Empty).await?;
        Ok(rows
            .into_iter()
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
        let mut out = Vec::new();
        for i in 0..self.layout.shards.len() {
            let sql = format!(
                "SELECT {EDGE_COLS} FROM {} e WHERE e.id IN ({})",
                self.t(i).edges,
                ids_list(ids)
            );
            let rows = self.rows_at(i, sql, Params::Empty).await?;
            out.extend(rows.into_iter().map(|mut r| edge_at(&mut r, 0)));
        }
        Ok(out)
    }

    async fn edges_touching(&mut self, nodes: &[u64], limit: usize) -> Result<Vec<EdgeRow>> {
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        let mut out: Vec<EdgeRow> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        // Outgoing, on the nodes' shards.
        for (i, part) in self.layout.by_shard(nodes) {
            let sql = format!(
                "SELECT {EDGE_COLS} FROM {} e WHERE e.src IN ({}) LIMIT {limit}",
                self.t(i).edges,
                ids_list(&part)
            );
            for mut r in self.rows_at(i, sql, Params::Empty).await? {
                let e = edge_at(&mut r, 0);
                if seen.insert(e.id) {
                    out.push(e);
                }
            }
            if out.len() > limit {
                return Ok(out);
            }
        }
        // Incoming: mirrors, the `rev` key of one shard, or a scatter.
        if self.many() && self.layout.mirrors {
            let by_src = self.mirrors_into(nodes, &[]).await?;
            for (e, _) in self.edges_with_src(by_src).await? {
                if seen.insert(e.id) {
                    out.push(e);
                }
            }
        } else {
            for i in 0..self.layout.shards.len() {
                let sql = format!(
                    "SELECT {EDGE_COLS} FROM {} e WHERE e.dst IN ({}) LIMIT {limit}",
                    self.t(i).edges,
                    ids_list(nodes)
                );
                for mut r in self.rows_at(i, sql, Params::Empty).await? {
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
        let mut out = Vec::new();
        for (i, part) in self.layout.by_shard(ids) {
            let sql = format!(
                "SELECT {NODE_COLS} FROM {} n WHERE n.id IN ({}) {}",
                self.t(i).nodes,
                ids_list(&part),
                if exclusive {
                    "FOR UPDATE"
                } else {
                    "LOCK IN SHARE MODE"
                }
            );
            let rows = self.locked_rows_at(i, sql).await?;
            out.extend(rows.into_iter().filter_map(|mut r| node_at(&mut r, 0)));
        }
        Ok(out)
    }

    async fn alloc_ids(&mut self, kind: IdKind, n: usize) -> Result<Vec<u64>> {
        self.ids.take(kind, n).await
    }

    async fn insert_nodes(&mut self, rows: &[NodeRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
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
        let mut by: BTreeMap<usize, Vec<&EdgeRow>> = BTreeMap::new();
        for r in rows {
            by.entry(self.layout.index_of(r.src_rk)).or_default().push(r);
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
        if !self.layout.mirrors {
            return Ok(());
        }
        // Mirrors on the destinations' shards, in the same transaction.
        let mut by: BTreeMap<usize, Vec<&EdgeRow>> = BTreeMap::new();
        for r in rows {
            by.entry(self.layout.index_of(r.dst_rk)).or_default().push(r);
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
        if self.layout.mirrors {
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
    for batch in deleted.chunks(1000) {
        let mut hit: Option<u64> = None;
        for (i, part) in layout.by_shard(batch) {
            let at = &layout.shards[i];
            let l = ids_list(&part);
            let incoming = if many && layout.mirrors {
                format!(" UNION SELECT dst FROM {} WHERE dst IN ({l})", at.tables.mirrors)
            } else if many {
                String::new()
            } else {
                format!(" UNION SELECT dst FROM {} WHERE dst IN ({l})", at.tables.edges)
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
        if hit.is_none() && many && !layout.mirrors {
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
