//! Writes (chapter 43, "Writes and transactions"; build step 13):
//! `CREATE`, `MERGE`, `SET`, `REMOVE`, `[DETACH] DELETE`, inside the
//! caller's transaction.
//!
//! - Creating a relationship share-locks its existing endpoints first, and
//!   deleting a node locks it exclusively, so the two serialize: no
//!   dangling edges.
//! - A plain `DELETE n` records `n`; the commit fails with 39906 if it
//!   still has relationships ([`super::Output::deleted_nodes`]).
//! - `DETACH DELETE` past `graph.max_detach_edges` fails with 39907.
//! - `SET` and `REMOVE` lock the node rows (`FOR UPDATE`), re-read them and
//!   write them back whole, so concurrent updates of other keys survive.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use sprig_common::{Result, SprigCode, SprigError};

use super::{Exec, RELS};
use crate::ast::{Dir, Expr, NodePat, PathPattern, RemoveItem, SetItem};
use crate::eval::{Row, eval};
use crate::store::{EdgeKey, EdgeRow, IdKind, NodeRow, rk_of};
use crate::value::{Node, Props, Rel, Value, check_property, props_to_json};

fn syntax(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::GraphSyntax, msg.into())
}

/// A pending node or relationship state during `SET`/`REMOVE`.
struct Pending {
    labels: Vec<String>,
    props: Props,
    old_labels: Vec<u32>,
    edge: Option<EdgeRow>,
    rk: u64,
}

impl Exec<'_> {
    fn props_of(&self, e: Option<&Expr>, row: &Row) -> Result<Props> {
        let Some(e) = e else {
            return Ok(Props::new());
        };
        match eval(e, row, self.params)? {
            Value::Map(m) => {
                let mut p = Props::new();
                for (k, v) in m {
                    check_property(&k, &v)?;
                    if !v.is_null() {
                        p.insert(k, v);
                    }
                }
                Ok(p)
            }
            Value::Null => Ok(Props::new()),
            other => Err(SprigError::new(
                SprigCode::GraphPropertyType,
                format!("a property map is expected, got {}", other.type_name()),
            )),
        }
    }

    async fn label_ids(&mut self, names: &[String]) -> Result<Vec<u32>> {
        let mut ids = Vec::with_capacity(names.len());
        for n in names {
            let id = self
                .store
                .label_id(n, true)
                .await?
                .ok_or_else(|| SprigError::internal("label id not created"))?;
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        Ok(ids)
    }

    /// Creates a node for `np` (unbound) in `row`.
    async fn create_node(&mut self, np: &NodePat, row: &Row) -> Result<Arc<Node>> {
        let labels = match &np.labels {
            None => vec![],
            Some(l) => l
                .plain()
                .ok_or_else(|| syntax("CREATE takes plain labels (:A:B), not label expressions"))?,
        };
        let props = self.props_of(np.props.as_ref(), row)?;
        let label_ids = self.label_ids(&labels).await?;
        let id = self.store.alloc_ids(IdKind::Node, 1).await?[0];
        let rk = rk_of(id);
        let (pj, tj) = props_to_json(&props);
        self.store
            .insert_nodes(&[NodeRow {
                id,
                rk,
                labels: label_ids,
                props: pj,
                types: tj,
            }])
            .await?;
        self.wrote = true;
        self.bump("nodes-created", 1);
        self.bump("labels-added", i64::try_from(labels.len()).unwrap_or(0));
        self.bump("properties-set", i64::try_from(props.len()).unwrap_or(0));
        let mut uniq = Vec::new();
        for l in labels {
            if !uniq.contains(&l) {
                uniq.push(l);
            }
        }
        Ok(Arc::new(Node {
            graph: self.store.graph_id(),
            id,
            rk,
            labels: uniq,
            props,
        }))
    }

    /// Creates the unbound parts of `pat` in `row`, binding them.
    async fn create_path(
        &mut self,
        row: &mut Row,
        pat: &PathPattern,
        locked: &mut HashSet<u64>,
    ) -> Result<()> {
        let mut nodes: Vec<Arc<Node>> = Vec::new();
        for np in std::iter::once(&pat.start).chain(pat.steps.iter().map(|(_, n)| n)) {
            let existing = np.var.as_ref().and_then(|v| row.get(v));
            let n = match existing {
                Some(Value::Node(n)) => {
                    if np.labels.is_some() || np.props.is_some() {
                        return Err(syntax(format!(
                            "Can't create node `{}` with labels or properties here. The variable is already declared in this context",
                            np.var.as_deref().unwrap_or_default()
                        )));
                    }
                    n.clone()
                }
                Some(Value::Null) => {
                    return Err(SprigError::new(
                        SprigCode::GraphPropertyType,
                        format!(
                            "Failed to create relationship: node `{}` is null",
                            np.var.as_deref().unwrap_or_default()
                        ),
                    ));
                }
                Some(_) => {
                    return Err(SprigError::new(
                        SprigCode::GraphPropertyType,
                        format!(
                            "Type mismatch: variable `{}` is not a node",
                            np.var.as_deref().unwrap_or_default()
                        ),
                    ));
                }
                None => {
                    let n = self.create_node(np, row).await?;
                    if let Some(v) = &np.var {
                        row.insert(v.clone(), Value::Node(n.clone()));
                    }
                    locked.insert(n.id);
                    n
                }
            };
            nodes.push(n);
        }
        // Existing endpoints are share-locked before an edge refers to them.
        if !pat.steps.is_empty() {
            let need: Vec<u64> = nodes
                .iter()
                .map(|n| n.id)
                .filter(|i| !locked.contains(i))
                .collect();
            if !need.is_empty() {
                let got = self.store.lock_nodes(&need, false).await?;
                for i in &need {
                    if !got.iter().any(|r| r.id == *i) {
                        return Err(SprigError::new(
                            SprigCode::GraphNotFound,
                            format!("Node with id {i} has been deleted in this transaction"),
                        ));
                    }
                    locked.insert(*i);
                }
            }
        }
        let mut rels = Vec::new();
        for (i, (rp, _)) in pat.steps.iter().enumerate() {
            if rp.var.as_ref().is_some_and(|v| row.contains_key(v)) {
                return Err(syntax(format!(
                    "Can't create relationship `{}`: the variable is already declared",
                    rp.var.as_deref().unwrap_or_default()
                )));
            }
            if rp.range.is_some() {
                return Err(syntax(
                    "Variable length relationships cannot be used in CREATE",
                ));
            }
            let [t] = rp.types.as_slice() else {
                return Err(syntax(
                    "Exactly one relationship type must be specified for CREATE",
                ));
            };
            let (src, dst) = match rp.dir {
                Dir::Out => (&nodes[i], &nodes[i + 1]),
                Dir::In => (&nodes[i + 1], &nodes[i]),
                Dir::Both => {
                    return Err(syntax(
                        "Only directed relationships are supported in CREATE",
                    ));
                }
            };
            let props = self.props_of(rp.props.as_ref(), row)?;
            let tid = self
                .store
                .type_id(t, true)
                .await?
                .ok_or_else(|| SprigError::internal("type id not created"))?;
            let id = self.store.alloc_ids(IdKind::Edge, 1).await?[0];
            let (pj, tj) = props_to_json(&props);
            let e = EdgeRow {
                id,
                src: src.id,
                dst: dst.id,
                src_rk: src.rk,
                dst_rk: dst.rk,
                rtype: tid,
                props: pj,
                types: tj,
            };
            self.store.insert_edges(std::slice::from_ref(&e)).await?;
            self.wrote = true;
            self.bump("relationships-created", 1);
            self.bump("properties-set", i64::try_from(props.len()).unwrap_or(0));
            let rel = Arc::new(Rel {
                graph: self.store.graph_id(),
                id,
                src: src.id,
                dst: dst.id,
                src_rk: src.rk,
                dst_rk: dst.rk,
                rtype: t.clone(),
                props,
            });
            if let Some(v) = &rp.var {
                row.insert(v.clone(), Value::Rel(rel.clone()));
            }
            rels.push(rel);
        }
        if let Some(pv) = &pat.var {
            row.insert(pv.clone(), crate::eval::path_value(nodes, rels));
        }
        Ok(())
    }

    /// `CREATE`.
    pub(crate) async fn create(
        &mut self,
        rows: Vec<Row>,
        pats: &[PathPattern],
    ) -> Result<Vec<Row>> {
        let mut out = Vec::with_capacity(rows.len());
        let mut locked = HashSet::new();
        for mut r in rows {
            for p in pats {
                self.create_path(&mut r, p, &mut locked).await?;
            }
            out.push(r);
            self.check_time()?;
        }
        Ok(out)
    }

    /// `MERGE`: match the whole pattern, else create it; row by row, so a
    /// later row sees what an earlier one created.
    pub(crate) async fn merge(
        &mut self,
        rows: Vec<Row>,
        pat: &PathPattern,
        on_create: &[SetItem],
        on_match: &[SetItem],
    ) -> Result<Vec<Row>> {
        let mut out = Vec::new();
        for r in rows {
            let mut tagged = r.clone();
            tagged.insert(RELS.into(), Value::List(vec![]));
            let found = Box::pin(self.match_pattern(vec![tagged], pat, &[], false, &[])).await?;
            if found.is_empty() {
                let r_again = r.clone();
                let mut nr = r;
                let mut create = pat.clone();
                // `MERGE (a)-[:R]-(b)` creates left to right.
                for (rp, _) in &mut create.steps {
                    if rp.dir == Dir::Both {
                        rp.dir = Dir::Out;
                    }
                }
                let mut locked = HashSet::new();
                match self.create_path(&mut nr, &create, &mut locked).await {
                    Ok(()) => {
                        let mut v = Box::pin(self.set(vec![nr], on_create)).await?;
                        out.append(&mut v);
                    }
                    // A uniqueness constraint: another writer created it
                    // since the match; match again (it has committed, as
                    // the duplicate check waited for it).
                    Err(e) if is_duplicate(&e) => {
                        let mut tagged = r_again.clone();
                        tagged.insert(RELS.into(), Value::List(vec![]));
                        let again =
                            Box::pin(self.match_pattern(vec![tagged], pat, &[], false, &[]))
                                .await?;
                        if again.is_empty() {
                            return Err(e);
                        }
                        let again: Vec<Row> = again
                            .into_iter()
                            .map(|mut x| {
                                x.remove(RELS);
                                x
                            })
                            .collect();
                        let mut v = Box::pin(self.set(again, on_match)).await?;
                        out.append(&mut v);
                    }
                    Err(e) => return Err(e),
                }
            } else {
                let found: Vec<Row> = found
                    .into_iter()
                    .map(|mut x| {
                        x.remove(RELS);
                        x
                    })
                    .collect();
                let mut v = Box::pin(self.set(found, on_match)).await?;
                out.append(&mut v);
            }
            self.check_rows(out.len())?;
        }
        Ok(out)
    }

    /// Loads (and locks) the nodes and relationships the items touch.
    async fn load_pending(&mut self, targets: &[Value]) -> Result<HashMap<(bool, u64), Pending>> {
        let mut node_ids = Vec::new();
        let mut edge_ids = Vec::new();
        for t in targets {
            match t {
                Value::Node(n) if !node_ids.contains(&n.id) => node_ids.push(n.id),
                Value::Rel(r) if !edge_ids.contains(&r.id) => edge_ids.push(r.id),
                _ => {}
            }
        }
        let mut pend = HashMap::new();
        for batch in node_ids.chunks(self.b.batch_nodes.max(1)) {
            for nr in self.store.lock_nodes(batch, true).await? {
                let n = self.node_val(&nr);
                pend.insert(
                    (true, nr.id),
                    Pending {
                        labels: n.labels.clone(),
                        props: n.props.clone(),
                        old_labels: nr.labels.clone(),
                        edge: None,
                        rk: nr.rk,
                    },
                );
            }
        }
        for batch in edge_ids.chunks(self.b.batch_nodes.max(1)) {
            for er in self.store.edges(batch).await? {
                let r = self.rel_val(&er);
                pend.insert(
                    (false, er.id),
                    Pending {
                        labels: vec![],
                        props: r.props.clone(),
                        old_labels: vec![],
                        edge: Some(er),
                        rk: 0,
                    },
                );
            }
        }
        Ok(pend)
    }

    /// Writes the pending states back and refreshes the rows' values.
    async fn flush_pending(
        &mut self,
        rows: &mut [Row],
        pend: HashMap<(bool, u64), Pending>,
    ) -> Result<()> {
        let mut nodes: HashMap<u64, Arc<Node>> = HashMap::new();
        let mut rels: HashMap<u64, Arc<Rel>> = HashMap::new();
        for ((is_node, id), p) in pend {
            let (pj, tj) = props_to_json(&p.props);
            if is_node {
                let label_ids = self.label_ids(&p.labels).await?;
                self.store
                    .update_node(
                        &NodeRow {
                            id,
                            rk: p.rk,
                            labels: label_ids,
                            props: pj,
                            types: tj,
                        },
                        &p.old_labels,
                    )
                    .await?;
                nodes.insert(
                    id,
                    Arc::new(Node {
                        graph: self.store.graph_id(),
                        id,
                        rk: p.rk,
                        labels: p.labels,
                        props: p.props,
                    }),
                );
            } else if let Some(mut e) = p.edge {
                e.props = pj;
                e.types = tj;
                self.store.update_edge(&e).await?;
                let mut r = (*self.rel_val(&e)).clone();
                r.props = p.props;
                rels.insert(id, Arc::new(r));
            }
        }
        self.wrote = true;
        for r in rows.iter_mut() {
            for v in r.values_mut() {
                match v {
                    Value::Node(n) => {
                        if let Some(x) = nodes.get(&n.id) {
                            *v = Value::Node(x.clone());
                        }
                    }
                    Value::Rel(e) => {
                        if let Some(x) = rels.get(&e.id) {
                            *v = Value::Rel(x.clone());
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn target_key(v: &Value) -> Result<Option<(bool, u64)>> {
        Ok(match v {
            Value::Null => None,
            Value::Node(n) => Some((true, n.id)),
            Value::Rel(r) => Some((false, r.id)),
            other => {
                return Err(SprigError::new(
                    SprigCode::GraphPropertyType,
                    format!("can't set properties on a {}", other.type_name()),
                ));
            }
        })
    }

    /// `SET`.
    pub(crate) async fn set(&mut self, mut rows: Vec<Row>, items: &[SetItem]) -> Result<Vec<Row>> {
        if items.is_empty() {
            return Ok(rows);
        }
        let mut targets = Vec::new();
        for r in &rows {
            for it in items {
                let t = match it {
                    SetItem::Prop(e, _, _) => eval(e, r, self.params)?,
                    SetItem::Replace(v, _) | SetItem::Merge(v, _) | SetItem::Labels(v, _) => {
                        r.get(v).cloned().unwrap_or(Value::Null)
                    }
                };
                targets.push(t);
            }
        }
        let mut pend = self.load_pending(&targets).await?;
        let mut ti = 0;
        for r in &rows {
            for it in items {
                let target = &targets[ti];
                ti += 1;
                let Some(key) = Self::target_key(target)? else {
                    continue;
                };
                let Some(p) = pend.get_mut(&key) else {
                    continue; // deleted meanwhile
                };
                // Values see the earlier items of this row.
                let mut view = r.clone();
                for v in view.values_mut() {
                    if Self::target_key(v).ok().flatten() == Some(key) {
                        *v = refreshed(v, p);
                    }
                }
                match it {
                    SetItem::Prop(_, k, e) => {
                        let v = eval(e, &view, self.params)?;
                        check_property(k, &v)?;
                        if v.is_null() {
                            p.props.remove(k);
                        } else {
                            p.props.insert(k.clone(), v);
                        }
                        self.bump("properties-set", 1);
                    }
                    SetItem::Replace(_, e) | SetItem::Merge(_, e) => {
                        let m = match eval(e, &view, self.params)? {
                            Value::Map(m) => m,
                            Value::Node(n) => n.props.clone(),
                            Value::Rel(x) => x.props.clone(),
                            Value::Null => BTreeMap::new(),
                            other => {
                                return Err(SprigError::new(
                                    SprigCode::GraphPropertyType,
                                    format!("SET = needs a map, got {}", other.type_name()),
                                ));
                            }
                        };
                        if matches!(it, SetItem::Replace(..)) {
                            let n = i64::try_from(p.props.len()).unwrap_or(0);
                            p.props.clear();
                            self.bump("properties-set", n);
                        }
                        for (k, v) in m {
                            check_property(&k, &v)?;
                            if v.is_null() {
                                p.props.remove(&k);
                            } else {
                                p.props.insert(k, v);
                            }
                            self.bump("properties-set", 1);
                        }
                    }
                    SetItem::Labels(_, ls) => {
                        if !key.0 {
                            return Err(syntax("labels can only be set on nodes"));
                        }
                        for l in ls {
                            if !p.labels.contains(l) {
                                p.labels.push(l.clone());
                                self.bump("labels-added", 1);
                            }
                        }
                    }
                }
            }
        }
        self.flush_pending(&mut rows, pend).await?;
        Ok(rows)
    }

    /// `REMOVE`.
    pub(crate) async fn remove(
        &mut self,
        mut rows: Vec<Row>,
        items: &[RemoveItem],
    ) -> Result<Vec<Row>> {
        let mut targets = Vec::new();
        for r in &rows {
            for it in items {
                targets.push(match it {
                    RemoveItem::Prop(e, _) => eval(e, r, self.params)?,
                    RemoveItem::Labels(v, _) => r.get(v).cloned().unwrap_or(Value::Null),
                });
            }
        }
        let mut pend = self.load_pending(&targets).await?;
        for (t, it) in targets.iter().zip(items.iter().cycle()) {
            let Some(key) = Self::target_key(t)? else {
                continue;
            };
            let Some(p) = pend.get_mut(&key) else {
                continue;
            };
            match it {
                RemoveItem::Prop(_, k) => {
                    if p.props.remove(k).is_some() {
                        self.bump("properties-set", 1);
                    }
                }
                RemoveItem::Labels(_, ls) => {
                    for l in ls {
                        if let Some(i) = p.labels.iter().position(|x| x == l) {
                            p.labels.remove(i);
                            self.bump("labels-removed", 1);
                        }
                    }
                }
            }
        }
        self.flush_pending(&mut rows, pend).await?;
        Ok(rows)
    }

    /// `[DETACH] DELETE`.
    pub(crate) async fn delete(
        &mut self,
        rows: Vec<Row>,
        exprs: &[Expr],
        detach: bool,
    ) -> Result<Vec<Row>> {
        let mut nodes: Vec<u64> = Vec::new();
        let mut rels: Vec<Rel> = Vec::new();
        let add = |v: &Value, nodes: &mut Vec<u64>, rels: &mut Vec<Rel>| -> Result<()> {
            match v {
                Value::Null => {}
                Value::Node(n) => {
                    if !nodes.contains(&n.id) {
                        nodes.push(n.id);
                    }
                }
                Value::Rel(r) => {
                    if !rels.iter().any(|x| x.id == r.id) {
                        rels.push(r.as_ref().clone());
                    }
                }
                Value::Path(p) => {
                    for n in &p.nodes {
                        if !nodes.contains(&n.id) {
                            nodes.push(n.id);
                        }
                    }
                    for r in &p.rels {
                        if !rels.iter().any(|x| x.id == r.id) {
                            rels.push(r.as_ref().clone());
                        }
                    }
                }
                other => {
                    return Err(SprigError::new(
                        SprigCode::GraphPropertyType,
                        format!("DELETE of a {}", other.type_name()),
                    ));
                }
            }
            Ok(())
        };
        for r in &rows {
            for e in exprs {
                let v = eval(e, r, self.params)?;
                if let Value::List(l) = &v {
                    for x in l {
                        add(x, &mut nodes, &mut rels)?;
                    }
                } else {
                    add(&v, &mut nodes, &mut rels)?;
                }
            }
        }
        if !rels.is_empty() {
            let mut keys = Vec::with_capacity(rels.len());
            for r in &rels {
                keys.push(EdgeKey {
                    id: r.id,
                    src: r.src,
                    dst: r.dst,
                    rtype: self.store.type_id(&r.rtype, false).await?.unwrap_or(0),
                    src_rk: r.src_rk,
                    dst_rk: r.dst_rk,
                });
            }
            let mut n = 0;
            for batch in keys.chunks(self.b.batch_nodes.max(1)) {
                n += self.store.delete_edges(batch).await?;
            }
            self.bump("relationships-deleted", i64::try_from(n).unwrap_or(0));
            self.wrote = true;
        }
        if !nodes.is_empty() {
            for batch in nodes.chunks(self.b.batch_nodes.max(1)) {
                self.store.lock_nodes(batch, true).await?;
            }
            if detach {
                let limit = self.b.max_detach_edges;
                let touching = self.store.edges_touching(&nodes, limit + 1).await?;
                if touching.len() > limit {
                    return Err(SprigError::new(
                        SprigCode::GraphDetachTooLarge,
                        format!(
                            "DETACH DELETE would remove more than graph.max_detach_edges = {limit} relationships"
                        ),
                    ));
                }
                let keys: Vec<EdgeKey> = touching.iter().map(EdgeKey::from).collect();
                let mut n = 0;
                for batch in keys.chunks(self.b.batch_nodes.max(1)) {
                    n += self.store.delete_edges(batch).await?;
                }
                self.bump("relationships-deleted", i64::try_from(n).unwrap_or(0));
            } else {
                self.deleted.extend(nodes.iter().copied());
            }
            let mut n = 0;
            for batch in nodes.chunks(self.b.batch_nodes.max(1)) {
                n += self.store.delete_nodes(batch).await?;
            }
            self.bump("nodes-deleted", i64::try_from(n).unwrap_or(0));
            self.wrote = true;
        }
        Ok(rows)
    }
}

/// A duplicate key: a uniqueness constraint refused the row.
pub(crate) fn is_duplicate(e: &SprigError) -> bool {
    matches!(
        e.kind,
        sprig_common::ErrorKind::Backend { errno: 1062, .. }
            | sprig_common::ErrorKind::Standard { errno: 1062, .. }
    )
}

fn refreshed(v: &Value, p: &Pending) -> Value {
    match v {
        Value::Node(n) => Value::Node(Arc::new(Node {
            labels: p.labels.clone(),
            props: p.props.clone(),
            ..(**n).clone()
        })),
        Value::Rel(r) => Value::Rel(Arc::new(Rel {
            props: p.props.clone(),
            ..(**r).clone()
        })),
        other => other.clone(),
    }
}
