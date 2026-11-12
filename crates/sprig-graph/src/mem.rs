//! An in-memory [`GraphStore`] with any number of shards, for tests: the
//! executor's results on 1 shard and on 4 must agree (chapter 43,
//! "Testing"), and cross-shard frontier expansion is exercised without a
//! sharded fleet.
//!
//! Shards own equal ranges of the routing hash (chapter 15's initial
//! split). [`MemStore::reach`] behaves like the CTE: it expands only nodes
//! whose `rk` is in the shard's range and returns boundary nodes without
//! expanding them.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use async_trait::async_trait;
use sprig_common::Result;

use crate::ast::{Dir, Entity, Schema};
use crate::schema::{plan_create, plan_drop, prop_index_value};
use crate::store::{
    EdgeKey, EdgeRow, GraphStore, IdKind, IndexDef, NodeRow, Reach, Scan, SchemaCounts, StoreStats,
};

/// The in-memory store.
#[derive(Debug, Clone, Default)]
pub struct MemStore {
    shards: usize,
    nodes: BTreeMap<u64, NodeRow>,
    edges: BTreeMap<u64, EdgeRow>,
    labels: Vec<String>,
    types: Vec<String>,
    next_node: u64,
    next_edge: u64,
    stats: StoreStats,
    indexes: Vec<IndexDef>,
    /// Statements per shard, for tests.
    pub per_shard: HashMap<usize, u64>,
    /// `CALL { } IN TRANSACTIONS`: the store as the current batch began.
    batch_base: Option<Box<MemStore>>,
}

/// The unique key of `props` (with its `_types`) under `d`: `None` if a
/// property is missing or of a type the index doesn't hold.
fn unique_key(
    d: &IndexDef,
    props: &serde_json::Value,
    types: Option<&serde_json::Value>,
) -> Option<Vec<String>> {
    d.props
        .iter()
        .map(|p| prop_index_value(props, types, p, d.kind))
        .collect()
}

fn duplicate(d: &IndexDef, what: &str, id: u64) -> sprig_common::SprigError {
    sprig_common::SprigError::backend(
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

impl MemStore {
    /// A store with `shards` shards.
    #[must_use]
    pub fn new(shards: usize) -> Self {
        Self {
            shards: shards.max(1),
            next_node: 1,
            next_edge: 1,
            ..Self::default()
        }
    }

    fn hit(&mut self, shard: usize) {
        self.stats.shard_queries += 1;
        *self.per_shard.entry(shard).or_default() += 1;
    }

    fn json_matches(props: &serde_json::Value, key: &str, vals: &[serde_json::Value]) -> bool {
        props.get(key).is_some_and(|v| {
            vals.iter().any(|w| match (v.as_f64(), w.as_f64()) {
                (Some(a), Some(b)) => (a - b).abs() < f64::EPSILON,
                _ => v == w,
            })
        })
    }

    /// Number of nodes.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Number of edges.
    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// The unique constraint `row` (a node, or an edge with `rtype`)
    /// breaks against the other rows, if any.
    fn violates(
        &self,
        id: u64,
        labels: &[u32],
        rtype: Option<u32>,
        props: &serde_json::Value,
        types: Option<&serde_json::Value>,
    ) -> Option<sprig_common::SprigError> {
        for d in self.indexes.iter().filter(|d| d.unique) {
            let applies = match (d.entity, rtype) {
                (Entity::Node, None) => labels.contains(&d.token_id),
                (Entity::Relationship, Some(t)) => t == d.token_id,
                _ => false,
            };
            if !applies {
                continue;
            }
            let Some(k) = unique_key(d, props, types) else {
                continue;
            };
            let clash = match rtype {
                None => self.nodes.values().any(|n| {
                    n.id != id
                        && n.labels.contains(&d.token_id)
                        && unique_key(d, &n.props, n.types.as_ref()).as_ref() == Some(&k)
                }),
                Some(_) => self.edges.values().any(|e| {
                    e.id != id
                        && e.rtype == d.token_id
                        && unique_key(d, &e.props, e.types.as_ref()).as_ref() == Some(&k)
                }),
            };
            if clash {
                let what = if rtype.is_none() {
                    "Node"
                } else {
                    "Relationship"
                };
                return Some(duplicate(d, what, id));
            }
        }
        None
    }
}

/// The shard of `rk` among `n` equal ranges.
#[must_use]
pub fn shard_of_rk(rk: u64, n: usize) -> usize {
    let n = u128::try_from(n.max(1)).unwrap_or(1);
    usize::try_from((u128::from(rk) * n) >> 64).unwrap_or(0)
}

#[async_trait]
impl GraphStore for MemStore {
    fn graph_id(&self) -> u64 {
        1
    }

    async fn batches(&mut self) -> Result<()> {
        self.batch_base = Some(Box::new(Self {
            batch_base: None,
            ..self.clone()
        }));
        Ok(())
    }

    async fn end_batch(&mut self, ok: bool, deleted: &[u64]) -> Result<()> {
        let r = if ok {
            match deleted
                .iter()
                .find(|n| self.edges.values().any(|e| e.src == **n || e.dst == **n))
            {
                Some(n) => Err(sprig_common::SprigError::new(
                    sprig_common::SprigCode::GraphNodeHasRelationships,
                    format!(
                        "Cannot delete node<{n}>, because it still has relationships. To delete this node, you must first delete its relationships."
                    ),
                )),
                None => Ok(()),
            }
        } else {
            Ok(())
        };
        let restore = !ok || r.is_err();
        if restore && let Some(b) = self.batch_base.take() {
            let stats = self.stats;
            *self = (*b).clone();
            self.stats = stats;
            self.batch_base = Some(b);
        } else {
            self.batch_base = Some(Box::new(Self {
                batch_base: None,
                ..self.clone()
            }));
        }
        r
    }

    fn shards(&self) -> usize {
        self.shards
    }

    fn shard_of(&self, rk: u64) -> usize {
        shard_of_rk(rk, self.shards)
    }

    fn stats(&self) -> StoreStats {
        self.stats
    }

    async fn scan(&mut self, s: &Scan) -> Result<Vec<NodeRow>> {
        for sh in 0..self.shards {
            self.hit(sh);
        }
        let mut out = Vec::new();
        for n in self.nodes.values() {
            if s.after.is_some_and(|a| n.id <= a) {
                continue;
            }
            if let Some(ids) = &s.ids
                && !ids.contains(&n.id)
            {
                continue;
            }
            if let Some(l) = s.label
                && !n.labels.contains(&l)
            {
                continue;
            }
            if !s
                .props_eq
                .iter()
                .all(|(k, v)| Self::json_matches(&n.props, k, v))
            {
                continue;
            }
            out.push(n.clone());
            if s.limit.is_some_and(|l| out.len() >= l) {
                break;
            }
        }
        Ok(out)
    }

    async fn nodes(&mut self, ids: &[u64]) -> Result<Vec<NodeRow>> {
        self.stats.shard_queries += 1;
        Ok(ids
            .iter()
            .filter_map(|i| self.nodes.get(i).cloned())
            .collect())
    }

    async fn expand(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
    ) -> Result<Vec<(EdgeRow, Option<NodeRow>)>> {
        let set: HashSet<u64> = from.iter().copied().collect();
        let mut shards: HashSet<usize> = HashSet::new();
        let mut out = Vec::new();
        for e in self.edges.values() {
            if !types.is_empty() && !types.contains(&e.rtype) {
                continue;
            }
            let (near, far, far_rk) = match dir {
                Dir::Out => (e.src, e.dst, e.dst_rk),
                Dir::In => (e.dst, e.src, e.src_rk),
                Dir::Both => continue,
            };
            if set.contains(&near) {
                shards.insert(shard_of_rk(
                    if dir == Dir::Out { e.src_rk } else { e.dst_rk },
                    self.shards,
                ));
                // The far node joins only when it is on the edge's shard.
                let local = shard_of_rk(far_rk, self.shards) == shard_of_rk(e.src_rk, self.shards);
                let far_row = if local {
                    self.nodes.get(&far).cloned()
                } else {
                    None
                };
                out.push((e.clone(), far_row));
            }
        }
        for s in shards {
            self.hit(s);
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
        self.hit(shard);
        self.stats.cte_queries += 1;
        let n = self.shards;
        let mut adj: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
        for e in self.edges.values() {
            if !types.is_empty() && !types.contains(&e.rtype) {
                continue;
            }
            match dir {
                Dir::Out => adj.entry(e.src).or_default().push((e.dst, e.dst_rk)),
                Dir::In => adj.entry(e.dst).or_default().push((e.src, e.src_rk)),
                Dir::Both => {}
            }
        }
        let mut best: HashMap<(u64, u64), Reach> = HashMap::new();
        for &(origin, seed) in seeds {
            let mut q = VecDeque::from([(seed, 0u32)]);
            let mut seen: HashSet<u64> = HashSet::new();
            while let Some((x, d)) = q.pop_front() {
                if d >= hops {
                    continue;
                }
                for &(y, yrk) in adj.get(&x).map_or(&[][..], Vec::as_slice) {
                    if !seen.insert(y) {
                        continue;
                    }
                    best.entry((origin, y)).or_insert(Reach {
                        origin,
                        node: y,
                        rk: yrk,
                        depth: d + 1,
                    });
                    if shard_of_rk(yrk, n) == shard {
                        q.push_back((y, d + 1));
                    }
                }
            }
        }
        Ok(best.into_values().collect())
    }

    async fn edges(&mut self, ids: &[u64]) -> Result<Vec<EdgeRow>> {
        self.stats.shard_queries += 1;
        Ok(ids
            .iter()
            .filter_map(|i| self.edges.get(i).cloned())
            .collect())
    }

    async fn edges_touching(&mut self, nodes: &[u64], limit: usize) -> Result<Vec<EdgeRow>> {
        self.stats.shard_queries += 1;
        let set: HashSet<u64> = nodes.iter().copied().collect();
        Ok(self
            .edges
            .values()
            .filter(|e| set.contains(&e.src) || set.contains(&e.dst))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn lock_nodes(&mut self, ids: &[u64], _exclusive: bool) -> Result<Vec<NodeRow>> {
        self.nodes(ids).await
    }

    async fn alloc_ids(&mut self, kind: IdKind, n: usize) -> Result<Vec<u64>> {
        let c = match kind {
            IdKind::Node => &mut self.next_node,
            IdKind::Edge => &mut self.next_edge,
        };
        let start = *c;
        *c += u64::try_from(n).unwrap_or(0);
        Ok((start..*c).collect())
    }

    async fn insert_nodes(&mut self, rows: &[NodeRow]) -> Result<()> {
        for r in rows {
            if let Some(e) = self.violates(r.id, &r.labels, None, &r.props, r.types.as_ref()) {
                return Err(e);
            }
            self.nodes.insert(r.id, r.clone());
        }
        Ok(())
    }

    async fn insert_edges(&mut self, rows: &[EdgeRow]) -> Result<()> {
        for r in rows {
            if let Some(e) = self.violates(r.id, &[], Some(r.rtype), &r.props, r.types.as_ref()) {
                return Err(e);
            }
            self.edges.insert(r.id, r.clone());
        }
        Ok(())
    }

    async fn update_node(&mut self, row: &NodeRow, _old: &[u32]) -> Result<()> {
        if let Some(e) = self.violates(row.id, &row.labels, None, &row.props, row.types.as_ref()) {
            return Err(e);
        }
        self.nodes.insert(row.id, row.clone());
        Ok(())
    }

    async fn update_edge(&mut self, row: &EdgeRow) -> Result<()> {
        if let Some(e) = self.violates(row.id, &[], Some(row.rtype), &row.props, row.types.as_ref()) {
            return Err(e);
        }
        self.edges.insert(row.id, row.clone());
        Ok(())
    }

    fn indexes(&self) -> Vec<IndexDef> {
        self.indexes.clone()
    }

    async fn schema(&mut self, cmd: &Schema) -> Result<SchemaCounts> {
        if let Schema::Drop { .. } = cmd {
            let Some(d) = plan_drop(&self.indexes, cmd)? else {
                return Ok(vec![]);
            };
            self.indexes.retain(|x| x.name != d.name);
            return Ok(vec![(
                if d.unique {
                    "constraints-removed"
                } else {
                    "indexes-removed"
                },
                1,
            )]);
        }
        let Some(mut d) = plan_create(&self.indexes, cmd)? else {
            return Ok(vec![]);
        };
        d.token_id = match d.entity {
            Entity::Node => self.label_id(&d.token, true).await?,
            Entity::Relationship => self.type_id(&d.token, true).await?,
        }
        .unwrap_or(0);
        d.id = self.indexes.iter().map(|x| x.id).max().unwrap_or(0) + 1;
        d.state = "online".into();
        if d.unique {
            let mut seen = HashSet::new();
            let keys: Vec<Vec<String>> = match d.entity {
                Entity::Node => self
                    .nodes
                    .values()
                    .filter(|n| n.labels.contains(&d.token_id))
                    .filter_map(|n| unique_key(&d, &n.props, n.types.as_ref()))
                    .collect(),
                Entity::Relationship => self
                    .edges
                    .values()
                    .filter(|e| e.rtype == d.token_id)
                    .filter_map(|e| unique_key(&d, &e.props, e.types.as_ref()))
                    .collect(),
            };
            if keys.into_iter().any(|k| !seen.insert(k)) {
                return Err(sprig_common::SprigError::new(
                    sprig_common::SprigCode::GraphDefinitionInvalid,
                    format!(
                        "[Neo.ClientError.Schema.ConstraintCreationFailed] Unable to create constraint {}: existing data has duplicate values",
                        d.name
                    ),
                ));
            }
        }
        let unique = d.unique;
        self.indexes.push(d);
        Ok(vec![(
            if unique {
                "constraints-added"
            } else {
                "indexes-added"
            },
            1,
        )])
    }

    async fn delete_edges(&mut self, edges: &[EdgeKey]) -> Result<u64> {
        let mut n = 0;
        for e in edges {
            n += u64::from(self.edges.remove(&e.id).is_some());
        }
        Ok(n)
    }

    async fn delete_nodes(&mut self, ids: &[u64]) -> Result<u64> {
        let mut n = 0;
        for i in ids {
            n += u64::from(self.nodes.remove(i).is_some());
        }
        Ok(n)
    }

    async fn label_id(&mut self, name: &str, create: bool) -> Result<Option<u32>> {
        if let Some(i) = self.labels.iter().position(|l| l == name) {
            return Ok(u32::try_from(i + 1).ok());
        }
        if !create {
            return Ok(None);
        }
        self.labels.push(name.to_owned());
        Ok(u32::try_from(self.labels.len()).ok())
    }

    async fn type_id(&mut self, name: &str, create: bool) -> Result<Option<u32>> {
        if let Some(i) = self.types.iter().position(|l| l == name) {
            return Ok(u32::try_from(i + 1).ok());
        }
        if !create {
            return Ok(None);
        }
        self.types.push(name.to_owned());
        Ok(u32::try_from(self.types.len()).ok())
    }

    fn label_name(&self, id: u32) -> Option<String> {
        self.labels
            .get(usize::try_from(id).ok()?.checked_sub(1)?)
            .cloned()
    }

    fn type_name(&self, id: u32) -> Option<String> {
        self.types
            .get(usize::try_from(id).ok()?.checked_sub(1)?)
            .cloned()
    }

    fn all_labels(&self) -> Vec<String> {
        self.labels.clone()
    }

    fn all_types(&self) -> Vec<String> {
        self.types.clone()
    }
}
