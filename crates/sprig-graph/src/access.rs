//! Per-label and per-type read grants (chapter 43, "Security notes";
//! chapter 29's `GraphElement`; decision G42): a [`GraphStore`] that shows
//! only what a reader may see, for a reader whose grants name single
//! labels and relationship types rather than the whole graph.
//!
//! - A **node** is visible when one of its labels is readable (a node
//!   without labels is not), as Neo4j's `TRAVERSE`/`READ` on labels works.
//! - A **relationship** is visible when its type is readable and both of
//!   its ends are visible.
//! - Hidden elements are never traversed: every hop goes through the
//!   filter, so a variable-length search runs level by level in the
//!   gateway ([`GraphStore::reach`] here is a breadth-first search over
//!   filtered hops) instead of a recursive CTE that would walk through
//!   hidden nodes. The store reports one shard, so the executor never
//!   treats a node as a boundary node.
//! - `db.labels()` and `db.relationshipTypes()` list only readable names.
//! - **Writes** (round 5, decision G50): a writer without a graph-wide
//!   `INSERT`, `DELETE` or `UPDATE` may hold it for single labels and
//!   types ([`Writable`]; `GRANT CREATE|DELETE|SET PROPERTY ON GRAPH g
//!   NODES L`). Every write is then checked here before it runs: a new
//!   node needs `CREATE` on each of its labels, a new relationship on its
//!   type; a deleted node `DELETE` on each of its labels, a deleted
//!   relationship on its type; a changed property `SET PROPERTY` on one of
//!   the node's labels (or the relationship's type); an added label
//!   `CREATE` on it and a removed label `DELETE` on it. A refused write
//!   fails the statement with 1142 (`Neo.ClientError.Security.Forbidden`),
//!   and its transaction rolls back. Nodes without labels need the
//!   graph-wide privilege. A writer with the graph-wide `SELECT` reads
//!   unfiltered ([`FilteredStore::guarded`] with no [`Visible`]).

use std::collections::{HashMap, HashSet, VecDeque};

use async_trait::async_trait;
use sprig_common::Result;

use crate::ast::{Dir, Schema};
use crate::store::{
    EdgeKey, EdgeRow, GraphStore, IdKind, IndexDef, NodeRow, Reach, Scan, SchemaCounts, StoreStats,
};

/// What a reader may see: readable label and relationship type names.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Visible {
    /// Readable labels.
    pub labels: HashSet<String>,
    /// Readable relationship types.
    pub types: HashSet<String>,
}

/// What a writer may write, by label or type name, for the actions it
/// doesn't hold graph-wide (round 5).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Writable {
    /// `CREATE` everywhere (the graph-wide `INSERT`).
    pub create_all: bool,
    /// `DELETE` everywhere.
    pub delete_all: bool,
    /// `SET PROPERTY` everywhere (the graph-wide `UPDATE`).
    pub set_all: bool,
    /// Labels with `CREATE`.
    pub create_labels: HashSet<String>,
    /// Labels with `DELETE`.
    pub delete_labels: HashSet<String>,
    /// Labels with `SET PROPERTY`.
    pub set_labels: HashSet<String>,
    /// Types with `CREATE`.
    pub create_types: HashSet<String>,
    /// Types with `DELETE`.
    pub delete_types: HashSet<String>,
    /// Types with `SET PROPERTY`.
    pub set_types: HashSet<String>,
}

/// A store showing only what [`Visible`] allows (all when `None`), and
/// writing only what [`Writable`] allows (everything when `None`).
pub struct FilteredStore<'s> {
    inner: &'s mut dyn GraphStore,
    vis: Option<Visible>,
    writes: Option<Writable>,
    /// Ids of the readable types that exist.
    type_ids: Vec<u32>,
}

impl std::fmt::Debug for FilteredStore<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilteredStore")
            .field("visible", &self.vis)
            .finish_non_exhaustive()
    }
}

impl<'s> FilteredStore<'s> {
    /// `inner`, filtered by `vis`.
    ///
    /// # Errors
    /// The inner store's, looking up type ids.
    pub async fn new(inner: &'s mut dyn GraphStore, vis: Visible) -> Result<Self> {
        Self::guarded(inner, Some(vis), None).await
    }

    /// `inner`, reads filtered by `vis` (unfiltered when `None`) and writes
    /// checked against `writes` (unchecked when `None`).
    ///
    /// # Errors
    /// The inner store's, looking up type ids.
    pub async fn guarded(
        inner: &'s mut dyn GraphStore,
        vis: Option<Visible>,
        writes: Option<Writable>,
    ) -> Result<Self> {
        let mut type_ids = Vec::new();
        if let Some(v) = &vis {
            for t in &v.types {
                if let Some(id) = inner.type_id(t, false).await? {
                    type_ids.push(id);
                }
            }
        }
        type_ids.sort_unstable();
        Ok(Self {
            inner,
            vis,
            writes,
            type_ids,
        })
    }

    fn node_ok(&self, n: &NodeRow) -> bool {
        let Some(v) = &self.vis else {
            return true;
        };
        n.labels.iter().any(|l| {
            self.inner
                .label_name(*l)
                .is_some_and(|name| v.labels.contains(&name))
        })
    }

    fn type_ok(&self, t: u32) -> bool {
        let Some(v) = &self.vis else {
            return true;
        };
        self.inner
            .type_name(t)
            .is_some_and(|name| v.types.contains(&name))
    }

    /// The readable types among `types` (empty: any readable one).
    fn types_of(&self, types: &[u32]) -> Vec<u32> {
        if self.vis.is_none() {
            return types.to_vec();
        }
        if types.is_empty() {
            self.type_ids.clone()
        } else {
            types.iter().copied().filter(|t| self.type_ok(*t)).collect()
        }
    }

    fn label_names(&self, ids: &[u32]) -> Vec<String> {
        ids.iter()
            .map(|l| self.inner.label_name(*l).unwrap_or_else(|| format!("#{l}")))
            .collect()
    }

    fn type_name_of(&self, t: u32) -> String {
        self.inner.type_name(t).unwrap_or_else(|| format!("#{t}"))
    }

    /// Checks a node write: `all` held graph-wide, else every (`every`)
    /// or one of the node's labels in `set`.
    fn check_labels(
        what: &str,
        all: bool,
        set: &HashSet<String>,
        labels: &[String],
        every: bool,
    ) -> Result<()> {
        if all {
            return Ok(());
        }
        let ok = if labels.is_empty() {
            false
        } else if every {
            labels.iter().all(|l| set.contains(l))
        } else {
            labels.iter().any(|l| set.contains(l))
        };
        if ok {
            return Ok(());
        }
        let on = if labels.is_empty() {
            "a node without labels".to_owned()
        } else {
            format!("nodes with label(s) {}", labels.join(", "))
        };
        Err(denied(what, &on))
    }

    fn check_type(&self, what: &str, all: bool, set: &HashSet<String>, t: u32) -> Result<()> {
        let name = self.type_name_of(t);
        if all || set.contains(&name) {
            Ok(())
        } else {
            Err(denied(what, &format!("relationships of type {name}")))
        }
    }

    /// The visible nodes among `ids`, by id.
    async fn visible_nodes(&mut self, ids: &[u64]) -> Result<HashMap<u64, NodeRow>> {
        let mut out = HashMap::new();
        if ids.is_empty() {
            return Ok(out);
        }
        for n in self.inner.nodes(ids).await? {
            if self.node_ok(&n) {
                out.insert(n.id, n);
            }
        }
        Ok(out)
    }

    /// Keeps edges of readable types whose ends are visible.
    async fn visible_edges(&mut self, edges: Vec<EdgeRow>) -> Result<Vec<EdgeRow>> {
        let edges: Vec<EdgeRow> = edges
            .into_iter()
            .filter(|e| self.type_ok(e.rtype))
            .collect();
        let mut ends: Vec<u64> = edges.iter().flat_map(|e| [e.src, e.dst]).collect();
        ends.sort_unstable();
        ends.dedup();
        let vis = self.visible_nodes(&ends).await?;
        Ok(edges
            .into_iter()
            .filter(|e| vis.contains_key(&e.src) && vis.contains_key(&e.dst))
            .collect())
    }
}

/// A per-label write refused (1142, Neo4j's `Forbidden`).
fn denied(what: &str, on: &str) -> sprig_common::SprigError {
    sprig_common::SprigError::standard(
        1142,
        *b"42000",
        format!(
            "{what} on {on} is not allowed for this user (GRANT {what} ON GRAPH ... NODES|RELATIONSHIPS ...)"
        ),
    )
}

#[async_trait]
impl GraphStore for FilteredStore<'_> {
    fn graph_id(&self) -> u64 {
        self.inner.graph_id()
    }

    fn shards(&self) -> usize {
        if self.vis.is_none() {
            return self.inner.shards();
        }
        1
    }

    fn shard_of(&self, rk: u64) -> usize {
        if self.vis.is_none() {
            return self.inner.shard_of(rk);
        }
        0
    }

    fn stats(&self) -> StoreStats {
        self.inner.stats()
    }

    async fn scan(&mut self, s: &Scan) -> Result<Vec<NodeRow>> {
        let Some(limit) = s.limit else {
            let rows = self.inner.scan(s).await?;
            return Ok(rows.into_iter().filter(|n| self.node_ok(n)).collect());
        };
        // A limited scan pages on until it has `limit` visible rows, so a
        // short page never looks like the end of the data.
        let mut out = Vec::new();
        let mut page = s.clone();
        loop {
            let rows = self.inner.scan(&page).await?;
            let n = rows.len();
            let last = rows.last().map(|r| r.id);
            out.extend(rows.into_iter().filter(|r| self.node_ok(r)));
            match last {
                Some(l) if n >= limit && out.len() < limit => page.after = Some(l),
                _ => break,
            }
        }
        out.truncate(limit);
        Ok(out)
    }

    async fn nodes(&mut self, ids: &[u64]) -> Result<Vec<NodeRow>> {
        let rows = self.inner.nodes(ids).await?;
        Ok(rows.into_iter().filter(|n| self.node_ok(n)).collect())
    }

    async fn expand(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
    ) -> Result<Vec<(EdgeRow, Option<NodeRow>)>> {
        if self.vis.is_none() {
            return self.inner.expand(from, types, dir).await;
        }
        let types = self.types_of(types);
        if types.is_empty() || from.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self.inner.expand(from, &types, dir).await?;
        // Far nodes the shard didn't join are fetched (and filtered).
        let mut missing: Vec<u64> = rows
            .iter()
            .filter(|(_, n)| n.is_none())
            .map(|(e, _)| if dir == Dir::In { e.src } else { e.dst })
            .collect();
        missing.sort_unstable();
        missing.dedup();
        let fetched = self.visible_nodes(&missing).await?;
        let mut out = Vec::with_capacity(rows.len());
        for (e, n) in rows {
            if !self.type_ok(e.rtype) {
                continue;
            }
            let far = if dir == Dir::In { e.src } else { e.dst };
            let n = match n {
                Some(n) if self.node_ok(&n) => n,
                Some(_) => continue,
                None => match fetched.get(&far) {
                    Some(n) => n.clone(),
                    None => continue,
                },
            };
            out.push((e, Some(n)));
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
        if self.vis.is_none() {
            return self.inner.reach(shard, seeds, types, dir, hops).await;
        }
        // Level by level over filtered hops, each node once per origin at
        // its smallest depth (the CTE's `CYCLE o, id RESTRICT`).
        let mut best: HashMap<(u64, u64), Reach> = HashMap::new();
        let mut seen: HashMap<u64, HashSet<u64>> = HashMap::new();
        let mut frontier: VecDeque<(u64, u64)> = seeds.iter().copied().collect();
        for d in 1..=hops {
            if frontier.is_empty() {
                break;
            }
            let mut from: Vec<u64> = frontier.iter().map(|(_, n)| *n).collect();
            from.sort_unstable();
            from.dedup();
            let mut by_near: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
            for (e, n) in self.expand(&from, types, dir).await? {
                let (near, far) = if dir == Dir::In {
                    (e.dst, e.src)
                } else {
                    (e.src, e.dst)
                };
                let rk = n.map_or(0, |n| n.rk);
                by_near.entry(near).or_default().push((far, rk));
            }
            let mut next = VecDeque::new();
            for (origin, node) in frontier.drain(..) {
                for &(far, rk) in by_near.get(&node).map_or(&[][..], Vec::as_slice) {
                    if !seen.entry(origin).or_default().insert(far) {
                        continue;
                    }
                    best.entry((origin, far)).or_insert(Reach {
                        origin,
                        node: far,
                        rk,
                        depth: d,
                    });
                    next.push_back((origin, far));
                }
            }
            frontier = next;
        }
        Ok(best.into_values().collect())
    }

    async fn edges(&mut self, ids: &[u64]) -> Result<Vec<EdgeRow>> {
        let rows = self.inner.edges(ids).await?;
        if self.vis.is_none() {
            return Ok(rows);
        }
        self.visible_edges(rows).await
    }

    async fn edges_touching(&mut self, nodes: &[u64], limit: usize) -> Result<Vec<EdgeRow>> {
        self.inner.edges_touching(nodes, limit).await
    }

    async fn lock_nodes(&mut self, ids: &[u64], exclusive: bool) -> Result<Vec<NodeRow>> {
        self.inner.lock_nodes(ids, exclusive).await
    }

    async fn alloc_ids(&mut self, kind: IdKind, n: usize) -> Result<Vec<u64>> {
        self.inner.alloc_ids(kind, n).await
    }

    async fn insert_nodes(&mut self, rows: &[NodeRow]) -> Result<()> {
        if let Some(w) = &self.writes {
            for r in rows {
                let names = self.label_names(&r.labels);
                Self::check_labels("CREATE", w.create_all, &w.create_labels, &names, true)?;
            }
        }
        self.inner.insert_nodes(rows).await
    }

    async fn insert_edges(&mut self, rows: &[EdgeRow]) -> Result<()> {
        if let Some(w) = &self.writes {
            for r in rows {
                self.check_type("CREATE", w.create_all, &w.create_types, r.rtype)?;
            }
        }
        self.inner.insert_edges(rows).await
    }

    async fn update_node(&mut self, row: &NodeRow, old_labels: &[u32]) -> Result<()> {
        if self.writes.is_some() {
            let old = self.inner.nodes(&[row.id]).await?.into_iter().next();
            let w = self.writes.as_ref().unwrap_or_else(|| unreachable!());
            let added: Vec<u32> = row
                .labels
                .iter()
                .copied()
                .filter(|l| !old_labels.contains(l))
                .collect();
            let removed: Vec<u32> = old_labels
                .iter()
                .copied()
                .filter(|l| !row.labels.contains(l))
                .collect();
            if !added.is_empty() {
                let n = self.label_names(&added);
                Self::check_labels("CREATE", w.create_all, &w.create_labels, &n, true)?;
            }
            if !removed.is_empty() {
                let n = self.label_names(&removed);
                Self::check_labels("DELETE", w.delete_all, &w.delete_labels, &n, true)?;
            }
            let props_changed = old.as_ref().is_none_or(|o| o.props != row.props);
            if props_changed {
                let n = self.label_names(old_labels);
                Self::check_labels("SET PROPERTY", w.set_all, &w.set_labels, &n, false)?;
            }
        }
        self.inner.update_node(row, old_labels).await
    }

    async fn update_edge(&mut self, row: &EdgeRow) -> Result<()> {
        if let Some(w) = &self.writes {
            self.check_type("SET PROPERTY", w.set_all, &w.set_types, row.rtype)?;
        }
        self.inner.update_edge(row).await
    }

    async fn delete_edges(&mut self, edges: &[EdgeKey]) -> Result<u64> {
        if let Some(w) = &self.writes {
            for e in edges {
                self.check_type("DELETE", w.delete_all, &w.delete_types, e.rtype)?;
            }
        }
        self.inner.delete_edges(edges).await
    }

    async fn delete_nodes(&mut self, ids: &[u64]) -> Result<u64> {
        if self.writes.is_some() && !ids.is_empty() {
            let rows = self.inner.nodes(ids).await?;
            let w = self.writes.as_ref().unwrap_or_else(|| unreachable!());
            for r in &rows {
                let names = self.label_names(&r.labels);
                Self::check_labels("DELETE", w.delete_all, &w.delete_labels, &names, true)?;
            }
        }
        self.inner.delete_nodes(ids).await
    }

    async fn label_id(&mut self, name: &str, create: bool) -> Result<Option<u32>> {
        self.inner.label_id(name, create).await
    }

    async fn type_id(&mut self, name: &str, create: bool) -> Result<Option<u32>> {
        self.inner.type_id(name, create).await
    }

    fn label_name(&self, id: u32) -> Option<String> {
        self.inner.label_name(id)
    }

    fn type_name(&self, id: u32) -> Option<String> {
        self.inner.type_name(id)
    }

    fn all_labels(&self) -> Vec<String> {
        self.inner
            .all_labels()
            .into_iter()
            .filter(|l| self.vis.as_ref().is_none_or(|v| v.labels.contains(l)))
            .collect()
    }

    fn all_types(&self) -> Vec<String> {
        self.inner
            .all_types()
            .into_iter()
            .filter(|t| self.vis.as_ref().is_none_or(|v| v.types.contains(t)))
            .collect()
    }

    fn indexes(&self) -> Vec<IndexDef> {
        self.inner.indexes()
    }

    async fn schema(&mut self, cmd: &Schema) -> Result<SchemaCounts> {
        self.inner.schema(cmd).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::Params;
    use crate::mem::MemStore;
    use crate::value::Value;
    use crate::{Budgets, execute};

    async fn rows(s: &mut dyn GraphStore, q: &str) -> Vec<String> {
        execute(s, q, &Params::new(), &Budgets::default())
            .await
            .unwrap_or_else(|e| panic!("{q}: {e}"))
            .rows
            .iter()
            .map(|r| r.iter().map(Value::display).collect::<Vec<_>>().join("|"))
            .collect()
    }

    async fn guarded_run(
        mem: &mut MemStore,
        w: &Writable,
        q: &str,
    ) -> sprig_common::Result<crate::Output> {
        let mut f = FilteredStore::guarded(mem, None, Some(w.clone()))
            .await
            .expect("guard");
        execute(&mut f, q, &Params::new(), &Budgets::default()).await
    }

    #[tokio::test]
    async fn per_label_writes_are_checked() {
        for shards in [1, 4] {
            let mut mem = MemStore::new(shards);
            rows(
                &mut mem,
                "CREATE (a:Doc {n: 'a'}), (b:Doc:Secret {n: 'b'}), (c:Secret {n: 'c'}),
                 (a)-[:REF]->(b), (a)-[:OWN]->(c)",
            )
            .await;
            let w = Writable {
                create_labels: ["Doc".to_owned()].into(),
                delete_labels: ["Doc".to_owned()].into(),
                set_labels: ["Doc".to_owned()].into(),
                create_types: ["REF".to_owned()].into(),
                delete_types: ["REF".to_owned()].into(),
                ..Writable::default()
            };
            for q in [
                "CREATE (:Doc {n: 'd'})",
                "MATCH (a {n: 'a'}), (d {n: 'd'}) CREATE (a)-[:REF]->(d)",
                "MATCH (a {n: 'a'}) SET a.x = 1",
                "MATCH (b {n: 'b'}) SET b.x = 1",
                "MATCH (a {n: 'a'})-[r:REF]->(d {n: 'd'}) DELETE r",
                "MATCH (d {n: 'd'}) DETACH DELETE d",
            ] {
                guarded_run(&mut mem, &w, q)
                    .await
                    .unwrap_or_else(|e| panic!("{shards}: {q}: {e}"));
            }
            for q in [
                "CREATE (:Secret {n: 'e'})",
                "CREATE (:Doc:Secret {n: 'e'})",
                "CREATE ()",
                "MATCH (a {n: 'a'}), (c {n: 'c'}) CREATE (a)-[:OWN]->(c)",
                "MATCH (c {n: 'c'}) SET c.x = 1",
                "MATCH (a {n: 'a'}) SET a:Secret",
                "MATCH (b {n: 'b'}) REMOVE b:Secret",
                "MATCH ()-[r:REF]->() SET r.w = 1",
                "MATCH ()-[r:OWN]->() DELETE r",
                // Last: in memory a refused statement isn't rolled back.
                "MATCH (b {n: 'b'}) DETACH DELETE b",
            ] {
                let e = guarded_run(&mut mem, &w, q).await.expect_err(q);
                assert_eq!(e.protocol().mysql_errno, 1142, "{q}: {e}");
            }
            // Reads stay whole.
            assert_eq!(
                rows(&mut mem, "MATCH (n) RETURN count(n)").await,
                ["3"],
                "{shards}"
            );
        }
    }

    #[tokio::test]
    async fn hidden_labels_and_types_are_never_traversed() {
        for shards in [1, 4] {
            let mut mem = MemStore::new(shards);
            rows(
                &mut mem,
                "CREATE (a:Person {n: 'a'}), (b:Person {n: 'b'}), (s:Secret {n: 's'}),
                 (c:Person:Secret {n: 'c'}), (d:Person {n: 'd'}), (x {n: 'x'}),
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(s), (s)-[:KNOWS]->(d),
                 (b)-[:KNOWS]->(c), (a)-[:PAYS]->(d), (a)-[:KNOWS]->(x)",
            )
            .await;
            let vis = Visible {
                labels: ["Person".to_owned()].into(),
                types: ["KNOWS".to_owned()].into(),
            };
            let mut f = FilteredStore::new(&mut mem, vis).await.expect("filter");
            assert_eq!(
                rows(&mut f, "MATCH (n) RETURN n.n ORDER BY n.n").await,
                ["a", "b", "c", "d"],
                "{shards}"
            );
            // `s` hides `d` behind it; PAYS is hidden; `x` has no label.
            assert_eq!(
                rows(
                    &mut f,
                    "MATCH (a {n: 'a'})-[*1..5]->(y) RETURN DISTINCT y.n ORDER BY y.n"
                )
                .await,
                ["b", "c"]
            );
            assert_eq!(
                rows(
                    &mut f,
                    "MATCH (a {n: 'a'})-[*]->(y) RETURN count(DISTINCT y)"
                )
                .await,
                ["2"]
            );
            assert_eq!(
                rows(&mut f, "MATCH ()-[r]->() RETURN type(r), count(*)").await,
                ["KNOWS|2"]
            );
            assert_eq!(
                rows(&mut f, "MATCH (d {n: 'd'})<-[r]-(z) RETURN count(z)").await,
                ["0"]
            );
            assert_eq!(rows(&mut f, "CALL db.labels()").await, ["Person"]);
            assert_eq!(rows(&mut f, "CALL db.relationshipTypes()").await, ["KNOWS"]);
            // Unfiltered, the same store sees everything.
            assert_eq!(
                rows(
                    &mut mem,
                    "MATCH (a {n: 'a'})-[*1..5]->(y) RETURN count(DISTINCT y)"
                )
                .await,
                ["5"]
            );
        }
    }
}
