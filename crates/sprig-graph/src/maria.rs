//! [`GraphStore`] over MariaDB (chapter 43, "Native graph tables" and
//! "Variable-length paths"; build steps 3 and 11): one native graph's
//! tables in one shard schema, on the caller's connection and transaction.
//!
//! - A hop is one statement per batch: edges by the clustered `(src, ...)`
//!   key (outgoing) or the `rev` key (incoming), the far node left-joined.
//! - [`GraphStore::reach`] is the chapter's recursive CTE with an origin
//!   column, `CYCLE o, id RESTRICT` and `max_recursive_iterations`, run
//!   with `SET STATEMENT ... FOR`.
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

use crate::ast::Dir;
use crate::catalog::{Tables, backend_error};
use crate::store::{EdgeRow, GraphStore, IdKind, NodeRow, Reach, Scan, StoreStats};

/// Labels and relationship types of a graph, and how new ones are made.
#[async_trait]
pub trait Dictionary: Send + Sync {
    /// Creates (or finds) a label (`label = true`) or type: its id.
    async fn create(&self, label: bool, name: &str) -> Result<u32>;
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

/// One graph in one shard schema, on a connection inside a transaction.
pub struct MariaStore<'c> {
    conn: &'c mut Conn,
    graph: u64,
    t: Tables,
    labels: BTreeMap<u32, String>,
    types: BTreeMap<u32, String>,
    dict: Arc<dyn Dictionary>,
    ids: Arc<CounterIds>,
    stats: StoreStats,
    /// `_v` of writes: microseconds since the epoch.
    now_us: u64,
}

impl std::fmt::Debug for MariaStore<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MariaStore")
            .field("graph", &self.graph)
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

impl<'c> MariaStore<'c> {
    /// Graph `graph` with `tables`, on `conn` (inside the caller's
    /// transaction), with the catalog's labels and types.
    #[must_use]
    pub fn new(
        conn: &'c mut Conn,
        graph: u64,
        tables: Tables,
        labels: BTreeMap<u32, String>,
        types: BTreeMap<u32, String>,
        dict: Arc<dyn Dictionary>,
        ids: Arc<CounterIds>,
    ) -> Self {
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(0));
        Self {
            conn,
            graph,
            t: tables,
            labels,
            types,
            dict,
            ids,
            stats: StoreStats::default(),
            now_us,
        }
    }

    async fn rows(&mut self, sql: String, params: Params) -> Result<Vec<Row>> {
        self.stats.shard_queries += 1;
        self.conn.exec(sql, params).await.map_err(backend_error)
    }

    async fn run(&mut self, sql: String, params: Params) -> Result<u64> {
        self.stats.shard_queries += 1;
        self.conn
            .exec_drop(sql, params)
            .await
            .map_err(backend_error)?;
        Ok(self.conn.affected_rows())
    }

    async fn node_rows(&mut self, sql: String, params: Params) -> Result<Vec<NodeRow>> {
        let rows = self.rows(sql, params).await?;
        Ok(rows
            .into_iter()
            .filter_map(|mut r| node_at(&mut r, 0))
            .collect())
    }
}

/// A pushed-down JSON scalar as a parameter: strings and numbers as
/// themselves, booleans as `true`/`false` text (what `JSON_VALUE` gives).
fn scalar_param(j: &Json) -> Option<MyValue> {
    match j {
        Json::String(s) => Some(MyValue::from(s.as_str())),
        Json::Number(n) => n.as_i64().map(MyValue::from),
        Json::Bool(b) => Some(MyValue::from(if *b { "true" } else { "false" })),
        _ => None,
    }
}

#[async_trait]
impl GraphStore for MariaStore<'_> {
    fn graph_id(&self) -> u64 {
        self.graph
    }

    fn shards(&self) -> usize {
        1
    }

    fn shard_of(&self, _rk: u64) -> usize {
        0
    }

    fn stats(&self) -> StoreStats {
        self.stats
    }

    async fn scan(&mut self, s: &Scan) -> Result<Vec<NodeRow>> {
        let mut sql = format!("SELECT {NODE_COLS} FROM {} n", self.t.nodes);
        let mut params: Vec<MyValue> = Vec::new();
        if let Some(l) = s.label {
            let _ = write!(
                sql,
                " JOIN {} l ON l.id = n.id AND l.label = ?",
                self.t.labels
            );
            params.push(MyValue::from(l));
        }
        sql.push_str(" WHERE 1 = 1");
        if let Some(ids) = &s.ids {
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let _ = write!(sql, " AND n.id IN ({})", ids_list(ids));
        }
        for (k, vals) in &s.props_eq {
            if k.contains('"') || k.contains('\\') || k.contains('\'') {
                continue; // rechecked only
            }
            let ps: Vec<MyValue> = vals.iter().filter_map(scalar_param).collect();
            if ps.len() != vals.len() || ps.is_empty() {
                continue;
            }
            let marks = vec!["?"; ps.len()].join(", ");
            let _ = write!(sql, " AND JSON_VALUE(n.props, '$.\"{k}\"') IN ({marks})");
            params.extend(ps);
        }
        if let Some(l) = s.limit {
            let _ = write!(sql, " LIMIT {l}");
        }
        self.node_rows(sql, Params::Positional(params)).await
    }

    async fn nodes(&mut self, ids: &[u64]) -> Result<Vec<NodeRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT {NODE_COLS} FROM {} n WHERE n.id IN ({})",
            self.t.nodes,
            ids_list(ids)
        );
        self.node_rows(sql, Params::Empty).await
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
        let (near, far) = match dir {
            Dir::Out => ("src", "dst"),
            Dir::In => ("dst", "src"),
            Dir::Both => return Err(SprigError::internal("expand takes one direction")),
        };
        let mut sql = format!(
            "SELECT {EDGE_COLS}, {NODE_COLS} FROM {} e LEFT JOIN {} n ON n.id = e.{far} \
             WHERE e.{near} IN ({})",
            self.t.edges,
            self.t.nodes,
            ids_list(from)
        );
        if !types.is_empty() {
            let t: Vec<u64> = types.iter().map(|x| u64::from(*x)).collect();
            let _ = write!(sql, " AND e.type IN ({})", ids_list(&t));
        }
        let rows = self.rows(sql, Params::Empty).await?;
        Ok(rows
            .into_iter()
            .map(|mut r| {
                let e = edge_at(&mut r, 0);
                let n = node_at(&mut r, 8);
                (e, n)
            })
            .collect())
    }

    async fn reach(
        &mut self,
        _shard: usize,
        seeds: &[(u64, u64)],
        types: &[u32],
        dir: Dir,
        hops: u32,
    ) -> Result<Vec<Reach>> {
        if seeds.is_empty() || hops == 0 {
            return Ok(Vec::new());
        }
        self.stats.cte_queries += 1;
        let (near, far, far_rk) = match dir {
            Dir::Out => ("src", "dst", "dst_rk"),
            Dir::In => ("dst", "src", "_sprig_rk"),
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
        // One shard owns every routing hash in this build, so no node is a
        // boundary node; with several shards the recursive part adds
        // `AND r.rk BETWEEN lo AND hi`.
        let sql = format!(
            "SET STATEMENT max_recursive_iterations = {iters} FOR \
             WITH RECURSIVE r (o, id, rk, d) AS ( \
               SELECT s.o, e.{far}, e.{far_rk}, 1 FROM ({seed_sql}) s \
                 JOIN {edges} e ON e.{near} = s.n{type_filter} \
               UNION \
               SELECT r.o, e.{far}, e.{far_rk}, r.d + 1 FROM r \
                 JOIN {edges} e ON e.{near} = r.id{type_filter} \
                WHERE r.d < {hops} \
             ) CYCLE o, id RESTRICT \
             SELECT o, id, rk, MIN(d) FROM r GROUP BY o, id, rk",
            iters = hops + 1,
            edges = self.t.edges,
        );
        let rows = self.rows(sql, Params::Empty).await?;
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
        let sql = format!(
            "SELECT {EDGE_COLS} FROM {} e WHERE e.id IN ({})",
            self.t.edges,
            ids_list(ids)
        );
        let rows = self.rows(sql, Params::Empty).await?;
        Ok(rows.into_iter().map(|mut r| edge_at(&mut r, 0)).collect())
    }

    async fn edges_touching(&mut self, nodes: &[u64], limit: usize) -> Result<Vec<EdgeRow>> {
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        let l = ids_list(nodes);
        let sql = format!(
            "SELECT {EDGE_COLS} FROM {e} e WHERE e.src IN ({l}) \
             UNION SELECT {EDGE_COLS} FROM {e} e WHERE e.dst IN ({l}) LIMIT {limit}",
            e = self.t.edges
        );
        let rows = self.rows(sql, Params::Empty).await?;
        Ok(rows.into_iter().map(|mut r| edge_at(&mut r, 0)).collect())
    }

    async fn lock_nodes(&mut self, ids: &[u64], exclusive: bool) -> Result<Vec<NodeRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT {NODE_COLS} FROM {} n WHERE n.id IN ({}) {}",
            self.t.nodes,
            ids_list(ids),
            if exclusive {
                "FOR UPDATE"
            } else {
                "LOCK IN SHARE MODE"
            }
        );
        self.node_rows(sql, Params::Empty).await
    }

    async fn alloc_ids(&mut self, kind: IdKind, n: usize) -> Result<Vec<u64>> {
        self.ids.take(kind, n).await
    }

    async fn insert_nodes(&mut self, rows: &[NodeRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut sql = format!(
            "INSERT INTO {} (id, part, labels, props, _types, _v, _sprig_rk) VALUES ",
            self.t.nodes
        );
        let mut params: Vec<MyValue> = Vec::new();
        let mut lsql = format!(
            "INSERT IGNORE INTO {} (label, id, _sprig_rk) VALUES ",
            self.t.labels
        );
        let mut lparams: Vec<MyValue> = Vec::new();
        for (i, r) in rows.iter().enumerate() {
            if i > 0 {
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
        self.run(sql, Params::Positional(params)).await?;
        if !lparams.is_empty() {
            self.run(lsql, Params::Positional(lparams)).await?;
        }
        Ok(())
    }

    async fn insert_edges(&mut self, rows: &[EdgeRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut sql = format!(
            "INSERT INTO {} (src, type, dst, id, dst_rk, props, _types, _v, _sprig_rk) VALUES ",
            self.t.edges
        );
        let mut params: Vec<MyValue> = Vec::new();
        for (i, r) in rows.iter().enumerate() {
            if i > 0 {
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
        self.run(sql, Params::Positional(params)).await?;
        Ok(())
    }

    async fn update_node(&mut self, row: &NodeRow, old_labels: &[u32]) -> Result<()> {
        self.run(
            format!(
                "UPDATE {} SET labels = ?, props = ?, _types = ?, _v = ? WHERE id = ?",
                self.t.nodes
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
        .await?;
        let removed: Vec<u64> = old_labels
            .iter()
            .filter(|l| !row.labels.contains(l))
            .map(|l| u64::from(*l))
            .collect();
        if !removed.is_empty() {
            self.run(
                format!(
                    "DELETE FROM {} WHERE id = ? AND label IN ({})",
                    self.t.labels,
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
            self.run(
                format!(
                    "INSERT IGNORE INTO {} (label, id, _sprig_rk) VALUES (?, ?, ?)",
                    self.t.labels
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
        self.run(
            format!(
                "UPDATE {} SET props = ?, _types = ?, _v = ? WHERE id = ?",
                self.t.edges
            ),
            Params::Positional(vec![
                MyValue::from(json_text(&row.props)),
                row.types
                    .as_ref()
                    .map_or(MyValue::NULL, |t| MyValue::from(json_text(t))),
                MyValue::from(self.now_us),
                MyValue::from(row.id),
            ]),
        )
        .await?;
        Ok(())
    }

    async fn delete_edges(&mut self, ids: &[u64]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }
        self.run(
            format!(
                "DELETE FROM {} WHERE id IN ({})",
                self.t.edges,
                ids_list(ids)
            ),
            Params::Empty,
        )
        .await
    }

    async fn delete_nodes(&mut self, ids: &[u64]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let l = ids_list(ids);
        self.run(
            format!("DELETE FROM {} WHERE id IN ({l})", self.t.labels),
            Params::Empty,
        )
        .await?;
        self.run(
            format!("DELETE FROM {} WHERE id IN ({l})", self.t.nodes),
            Params::Empty,
        )
        .await
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
}

/// The commit check of chapter 43: nodes removed with a plain `DELETE`
/// must have no relationships left (39906).
///
/// # Errors
/// 39906 naming a node; a backend error.
pub async fn check_deleted(conn: &mut Conn, tables: &Tables, deleted: &[u64]) -> Result<()> {
    for batch in deleted.chunks(1000) {
        let l = ids_list(batch);
        let hit: Option<u64> = conn
            .query_first(format!(
                "SELECT src FROM {e} WHERE src IN ({l}) UNION SELECT dst FROM {e} WHERE dst IN ({l}) LIMIT 1",
                e = tables.edges
            ))
            .await
            .map_err(backend_error)?;
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
