//! What the executor needs from storage (chapter 43, "Native graph
//! tables"): the [`GraphStore`] trait over one native graph's node, edge
//! and label tables, implemented over MariaDB ([`crate::maria`]) and in
//! memory for tests ([`crate::mem`]).
//!
//! The trait keeps the chapter's shard dimension: nodes carry the routing
//! hash `rk` of `part`, edges live on their source's shard with `dst_rk`,
//! and [`GraphStore::reach`] is one shard's recursive CTE, which stops at
//! boundary nodes (an `rk` outside the shard) for the gateway to continue.

use async_trait::async_trait;
use serde_json::Value as Json;
use sprig_common::Result;
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

/// The routing hash of a node's `part` (chapter 15's numeric class).
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
