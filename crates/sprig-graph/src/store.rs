//! What the executor needs from storage: the [`GraphStore`] trait over one native graph's node, edge
//! and label tables, implemented over MariaDB ([`crate::maria`]) and in
//! memory for tests ([`crate::mem`]).
//!
//! The trait keeps the shard dimension: nodes carry the routing
//! hash `rk` of `part`, edges live on their source's shard with `dst_rk`,
//! and [`GraphStore::reach`] is one shard's recursive CTE, which stops at
//! boundary nodes (an `rk` outside the shard) for the gateway to continue.

use async_trait::async_trait;
use serde_json::Value as Json;
use sprig_common::Result;
use sprig_router::graph_route::CmpOp;
use sprig_shard::{KeyClass, KeySpec, KeyValue, hash_key};

use crate::ast::{Dir, Entity, IndexKind, Schema};

/// A node row.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeRow {
    /// `id`.
    pub id: u64,
    /// Routing hash of `part`.
    pub rk: u64,
    /// Label ids.
    pub labels: Vec<u32>,
    /// `props`.
    pub props: Json,
    /// `_types`.
    pub types: Option<Json>,
}

/// An edge row.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeRow {
    /// `id`.
    pub id: u64,
    /// Source node.
    pub src: u64,
    /// Destination node.
    pub dst: u64,
    /// The source's routing hash (`_sprig_rk`).
    pub src_rk: u64,
    /// `dst_rk`.
    pub dst_rk: u64,
    /// Type id.
    pub rtype: u32,
    /// `props`.
    pub props: Json,
    /// `_types`.
    pub types: Option<Json>,
}

/// What deleting an edge needs: its id, its ends and their routing
/// hashes (the edge lives on its source's shard, its mirror on its
/// destination's).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeKey {
    /// `id`.
    pub id: u64,
    /// Source node.
    pub src: u64,
    /// Destination node.
    pub dst: u64,
    /// Type id.
    pub rtype: u32,
    /// The source's routing hash.
    pub src_rk: u64,
    /// The destination's routing hash.
    pub dst_rk: u64,
}

impl From<&EdgeRow> for EdgeKey {
    fn from(e: &EdgeRow) -> Self {
        Self {
            id: e.id,
            src: e.src,
            dst: e.dst,
            rtype: e.rtype,
            src_rk: e.src_rk,
            dst_rk: e.dst_rk,
        }
    }
}

/// A node scan: the start point of a pattern.
#[derive(Debug, Clone, Default)]
pub struct Scan {
    /// Only nodes with this label (the label table).
    pub label: Option<u32>,
    /// Only these ids.
    pub ids: Option<Vec<u64>>,
    /// `props.<key>` equal to one of the JSON values: a superset filter,
    /// rechecked in the gateway.
    pub props_eq: Vec<(String, Vec<Json>)>,
    /// At most this many rows (the row budget plus one).
    pub limit: Option<usize>,
    /// Paging (streamed results): only ids above this one, in id order.
    pub after: Option<u64>,
    /// Only these property keys (`None`: all), as
    /// [`GraphStore::nodes_part`] reads them; stores without late reads
    /// give all.
    pub keys: Option<Vec<String>>,
}

/// One row of a reach (CTE) call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reach {
    /// The origin node the search started from.
    pub origin: u64,
    /// The node reached.
    pub node: u64,
    /// Its routing hash.
    pub rk: u64,
    /// Hops from the seed (1-based), smallest found in this call.
    pub depth: u32,
}

/// One hop's link without properties ([`GraphStore::links`]): an edge's
/// ends, their routing hashes, its id and type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Link {
    /// Source node.
    pub src: u64,
    /// Destination node.
    pub dst: u64,
    /// The source's routing hash (where the edge lives).
    pub src_rk: u64,
    /// The destination's routing hash.
    pub dst_rk: u64,
    /// `id`.
    pub id: u64,
    /// Type id.
    pub rtype: u32,
}

impl From<&EdgeRow> for Link {
    fn from(e: &EdgeRow) -> Self {
        Self {
            src: e.src,
            dst: e.dst,
            src_rk: e.src_rk,
            dst_rk: e.dst_rk,
            id: e.id,
            rtype: e.rtype,
        }
    }
}

/// What [`GraphStore::links_far`] reads of the far nodes, and which it
/// may leave out (a superset the caller rechecks).
#[derive(Debug, Clone, Copy)]
pub struct FarRead<'a> {
    /// Property keys (`None`: all).
    pub keys: Option<&'a [String]>,
    /// Numeric comparisons of properties the far node must pass.
    pub filter: &'a [(String, CmpOp, f64)],
    /// When not empty, the only far nodes wanted.
    pub only: &'a [u64],
    /// Read the far nodes (else links alone, restricted to `only`).
    pub join: bool,
    /// The relationships' property keys to read too (`Some(&[])`: all).
    pub rel_keys: Option<&'a [String]>,
    /// Numeric comparisons of the relationships' properties.
    pub rel_filter: &'a [(String, CmpOp, f64)],
}

/// A link [`GraphStore::links_far`] gives: the link, its far node when
/// read, and its relationship's properties and types when read.
pub type FarLink = (Link, Option<NodeRow>, Option<(Json, Option<Json>)>);

/// Which id counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdKind {
    /// Node ids.
    Node,
    /// Relationship ids.
    Edge,
}

/// Counters a store keeps for `PROFILE` and tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreStats {
    /// Statements sent to shards.
    pub shard_queries: u64,
    /// Of which recursive CTEs.
    pub cte_queries: u64,
    /// Time spent waiting for shard statements, in microseconds.
    pub sql_us: u64,
    /// Rows the shard statements returned.
    pub sql_rows: u64,
}

/// A property index or uniqueness constraint of a native graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    /// `graph_indexes.index_id`: names its columns (`_sprig_i<id>_<k>`).
    pub id: u32,
    /// The name (shared by a constraint and its index).
    pub name: String,
    /// A uniqueness constraint.
    pub unique: bool,
    /// Range or text.
    pub kind: IndexKind,
    /// Nodes or relationships.
    pub entity: Entity,
    /// The label or relationship type.
    pub token: String,
    /// Its id.
    pub token_id: u32,
    /// The properties.
    pub props: Vec<String>,
    /// `building` or `online`.
    pub state: String,
}

impl IndexDef {
    /// Whether `other` covers the same entity, token and properties.
    #[must_use]
    pub fn same_schema(&self, other: &IndexDef) -> bool {
        self.entity == other.entity && self.token == other.token && self.props == other.props
    }
}

/// The counters a schema command changed (`indexes-added`, ...).
pub type SchemaCounts = Vec<(&'static str, i64)>;

/// `CALL { } IN TRANSACTIONS` where batches can't commit on their own.
#[must_use]
pub fn not_implicit() -> sprig_common::SprigError {
    sprig_common::SprigError::new(
        sprig_common::SprigCode::GraphUnsupported,
        "[Neo.DatabaseError.Transaction.TransactionStartFailed] A query with 'CALL { ... } IN TRANSACTIONS' can only be executed in an implicit transaction, but tried to execute in an explicit transaction.",
    )
}

/// The routing hash of a node's `part` (the shard map's numeric class).
#[must_use]
pub fn rk_of(part: u64) -> u64 {
    // The spec of one numeric part never fails to build.
    KeySpec::new(vec![KeyClass::Numeric])
        .and_then(|s| hash_key(&s, &[KeyValue::Int(i128::from(part))]))
        .map_or(part, |(h, _)| h.0)
}

/// One native graph's storage, inside the caller's transaction.
#[async_trait]
pub trait GraphStore: Send {
    /// The graph id.
    fn graph_id(&self) -> u64;

    /// Number of shards.
    fn shards(&self) -> usize;

    /// The shard owning routing hash `rk`.
    fn shard_of(&self, rk: u64) -> usize;

    /// Statements run so far.
    fn stats(&self) -> StoreStats;

    /// Nodes matching `s`.
    async fn scan(&mut self, s: &Scan) -> Result<Vec<NodeRow>>;

    /// Nodes by id (missing ids are skipped).
    async fn nodes(&mut self, ids: &[u64]) -> Result<Vec<NodeRow>>;

    /// One hop from `from` over `types` (empty: any) in `dir` (`Out` or
    /// `In`), with the far node joined when it is on the same shard.
    async fn expand(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
    ) -> Result<Vec<(EdgeRow, Option<NodeRow>)>>;

    /// One hop from `from` as links only: no properties and no far
    /// nodes (breadth-first searches and trail enumeration read only
    /// ids; the elements they keep are read afterwards).
    async fn links(&mut self, from: &[u64], types: &[u32], dir: Dir) -> Result<Vec<Link>> {
        Ok(self
            .expand(from, types, dir)
            .await?
            .iter()
            .map(|(e, _)| Link::from(e))
            .collect())
    }

    /// Whether hops may read links and far nodes without properties,
    /// reading properties late ([`GraphStore::nodes_part`]); stores that
    /// say no answer every read with whole rows.
    fn late_props(&self) -> bool {
        false
    }

    /// Nodes by id with only `keys` of their properties (`Some(&[])`:
    /// none, just ids and labels; `None`: all). A node without one of
    /// the keys simply lacks it.
    async fn nodes_part(&mut self, ids: &[u64], keys: Option<&[String]>) -> Result<Vec<NodeRow>> {
        let _ = keys;
        self.nodes(ids).await
    }

    /// [`GraphStore::nodes_part`], leaving out (or not) nodes that fail
    /// `filter`: numeric comparisons of properties, a superset the
    /// caller rechecks.
    async fn nodes_where(
        &mut self,
        ids: &[u64],
        keys: Option<&[String]>,
        filter: &[(String, CmpOp, f64)],
    ) -> Result<Vec<NodeRow>> {
        let _ = filter;
        self.nodes_part(ids, keys).await
    }

    /// Edges by `(src_rk, id)` with only `keys` of their properties, as
    /// [`GraphStore::nodes_part`].
    async fn edges_part(
        &mut self,
        at: &[(u64, u64)],
        keys: Option<&[String]>,
    ) -> Result<Vec<EdgeRow>> {
        let _ = keys;
        self.edges_at(at).await
    }

    /// [`GraphStore::links`] both ways in one step where the store can:
    /// each link with `true` when it goes out of `from`.
    async fn links_both(&mut self, from: &[u64], types: &[u32]) -> Result<Vec<(Link, bool)>> {
        let mut v: Vec<(Link, bool)> = self
            .links(from, types, Dir::Out)
            .await?
            .into_iter()
            .map(|l| (l, true))
            .collect();
        v.extend(
            self.links(from, types, Dir::In)
                .await?
                .into_iter()
                .map(|l| (l, false)),
        );
        Ok(v)
    }

    /// [`GraphStore::links_far`] both ways in one step where the store
    /// can: each link with `true` when it goes out of `from`.
    async fn links_far_both(
        &mut self,
        from: &[u64],
        types: &[u32],
        far: &FarRead<'_>,
    ) -> Result<Vec<(FarLink, bool)>> {
        let mut v: Vec<(FarLink, bool)> = self
            .links_far(from, types, Dir::Out, far)
            .await?
            .into_iter()
            .map(|l| (l, true))
            .collect();
        v.extend(
            self.links_far(from, types, Dir::In, far)
                .await?
                .into_iter()
                .map(|l| (l, false)),
        );
        Ok(v)
    }

    /// [`GraphStore::links`] with the far nodes read in the same step
    /// where the store can (`keys` of their properties: `None` all;
    /// links to far nodes failing `filter` may be left out). A link
    /// without its node is for the caller to read by id.
    async fn links_far(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
        far: &FarRead<'_>,
    ) -> Result<Vec<FarLink>> {
        let _ = far;
        Ok(self
            .links(from, types, dir)
            .await?
            .into_iter()
            .map(|l| (l, None, None))
            .collect())
    }

    /// Edges by id whose sources' routing hashes are known: `(src_rk,
    /// id)` (routed to their shards; [`GraphStore::edges`] otherwise).
    async fn edges_at(&mut self, keys: &[(u64, u64)]) -> Result<Vec<EdgeRow>> {
        let ids: Vec<u64> = keys.iter().map(|(_, id)| *id).collect();
        self.edges(&ids).await
    }

    /// One shard's recursive CTE: from `seeds` (origin, node) on `shard`,
    /// up to `hops` hops over `types` in `dir`, each node once per origin
    /// at its smallest depth; nodes outside the shard are returned but not
    /// expanded.
    async fn reach(
        &mut self,
        shard: usize,
        seeds: &[(u64, u64)],
        types: &[u32],
        dir: Dir,
        hops: u32,
    ) -> Result<Vec<Reach>>;

    /// Edges by id.
    async fn edges(&mut self, ids: &[u64]) -> Result<Vec<EdgeRow>>;

    /// Edges touching `nodes` in either direction, at most `limit`.
    async fn edges_touching(&mut self, nodes: &[u64], limit: usize) -> Result<Vec<EdgeRow>>;

    /// Locks node rows: shared (`LOCK IN SHARE MODE`) or exclusive (`FOR
    /// UPDATE`), and returns them (fresh).
    async fn lock_nodes(&mut self, ids: &[u64], exclusive: bool) -> Result<Vec<NodeRow>>;

    /// `n` new ids.
    async fn alloc_ids(&mut self, kind: IdKind, n: usize) -> Result<Vec<u64>>;

    /// Inserts nodes and their label rows.
    async fn insert_nodes(&mut self, rows: &[NodeRow]) -> Result<()>;

    /// Inserts edges.
    async fn insert_edges(&mut self, rows: &[EdgeRow]) -> Result<()>;

    /// Rewrites a node's labels and properties (label rows follow).
    async fn update_node(&mut self, row: &NodeRow, old_labels: &[u32]) -> Result<()>;

    /// Rewrites an edge's properties.
    async fn update_edge(&mut self, row: &EdgeRow) -> Result<()>;

    /// Deletes edges (and their mirrors).
    async fn delete_edges(&mut self, edges: &[EdgeKey]) -> Result<u64>;

    /// Deletes nodes (and their label rows) by id.
    async fn delete_nodes(&mut self, ids: &[u64]) -> Result<u64>;

    /// The id of a label; with `create`, a new label gets one.
    async fn label_id(&mut self, name: &str, create: bool) -> Result<Option<u32>>;

    /// The id of a relationship type; with `create`, a new one gets one.
    async fn type_id(&mut self, name: &str, create: bool) -> Result<Option<u32>>;

    /// A label's name.
    fn label_name(&self, id: u32) -> Option<String>;

    /// A type's name.
    fn type_name(&self, id: u32) -> Option<String>;

    /// Every label, by name.
    fn all_labels(&self) -> Vec<String>;

    /// Every relationship type, by name.
    fn all_types(&self) -> Vec<String>;

    /// `EXPLAIN`'s lines for how the router plans this graph's steps: its
    /// group and shards and each step's rule; none for
    /// stores the router doesn't plan (in memory, mapped graphs).
    fn explain_routes(&self) -> Vec<String> {
        Vec::new()
    }

    /// `PROFILE`'s lines for the routes the statement ran: per step and
    /// plan kind, how many and on which shards.
    fn routes(&self) -> Vec<String> {
        Vec::new()
    }

    /// `CALL { } IN TRANSACTIONS`: whether the statement may
    /// commit batches of its own, checked before the first batch. Only an
    /// auto-commit statement may (Neo4j's rule).
    ///
    /// # Errors
    /// Neo4j's `TransactionStartFailed` (39901) when it may not.
    async fn batches(&mut self) -> Result<()> {
        Err(not_implicit())
    }

    /// Ends the current batch: commits it (`ok`; nodes `deleted` by a
    /// plain `DELETE` in it must have no relationships left) or rolls it
    /// back. The statement's next step starts a new transaction.
    ///
    /// # Errors
    /// The commit's (the batch is then rolled back).
    async fn end_batch(&mut self, ok: bool, deleted: &[u64]) -> Result<()> {
        let _ = (ok, deleted);
        Err(not_implicit())
    }

    /// The graph's indexes and constraints.
    fn indexes(&self) -> Vec<IndexDef> {
        Vec::new()
    }

    /// Runs an index or constraint command (outside the data
    /// transaction): the counters it changed.
    ///
    /// # Errors
    /// 39901 where the store has no schema support; 39908 for a name in
    /// use or an equivalent index; 1062 when existing data violates a new
    /// constraint.
    async fn schema(&mut self, _cmd: &Schema) -> Result<SchemaCounts> {
        Err(sprig_common::SprigError::new(
            sprig_common::SprigCode::GraphUnsupported,
            "index and constraint commands are not supported on this store",
        ))
    }
}
