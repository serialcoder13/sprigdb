//! `MATCH`: start points, hops, variable-length expansion and shortest
//! paths (chapter 43, "From patterns to joins", "Variable-length paths",
//! "Frontier expansion across shards", "Shortest paths").

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use serde_json::Value as Json;
use sprig_common::{Result, SprigCode, SprigError};

use super::{Exec, RELS, budget_error, pattern_vars};
use crate::ast::{BinOp, Dir, Expr, NodePat, PathKind, PathPattern, RelPat};
use crate::eval::{Row, eval, path_value};
use crate::store::{EdgeRow, NodeRow, Reach, Scan};
use crate::value::{Node, Rel, Value, parse_element_id};

const PATH_NODES: &str = " pn";
const PATH_RELS: &str = " pr";
const ROW_ID: &str = " rid";

/// The `AND`ed parts of a predicate.
pub(super) fn conjuncts(e: &Expr) -> Vec<&Expr> {
    match e {
        Expr::Binary(BinOp::And, a, b) => {
            let mut v = conjuncts(a);
            v.extend(conjuncts(b));
            v
        }
        other => vec![other],
    }
}

/// Whether `e` uses no variables (constant for the statement).
fn is_const(e: &Expr) -> bool {
    let mut ok = true;
    e.walk(&mut |x| {
        if matches!(x, Expr::Var(_) | Expr::Pattern(_) | Expr::Slot(_)) {
            ok = false;
        }
    });
    ok
}

/// One hop's candidates: (relationship, far node).
type Hops = HashMap<u64, Vec<(Arc<Rel>, Arc<Node>)>>;

/// One side of a bidirectional search: node to (depth, predecessors).
type Side = HashMap<u64, (u32, Vec<(u64, Arc<Rel>)>)>;

/// A trail found by path-mode expansion.
#[derive(Clone)]
struct Trail {
    end: Arc<Node>,
    rels: Vec<Arc<Rel>>,
    nodes: Vec<Arc<Node>>,
}

impl Exec<'_> {
    /// A `MATCH` (or `OPTIONAL MATCH`) clause.
    pub(crate) async fn match_clause(
        &mut self,
        rows: Vec<Row>,
        patterns: &[PathPattern],
        where_: Option<&Expr>,
        optional: bool,
        mult_ok: bool,
    ) -> Result<Vec<Row>> {
        let input = rows;
        let mut rows: Vec<Row> = input
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let mut r = r.clone();
                r.insert(ROW_ID.into(), Value::Int(i64::try_from(i).unwrap_or(0)));
                r.insert(RELS.into(), Value::List(vec![]));
                r
            })
            .collect();
        let conj: Vec<Expr> = where_
            .map(|w| conjuncts(w).into_iter().cloned().collect())
            .unwrap_or_default();
        for (pi, p) in patterns.iter().enumerate() {
            // Types of the other relationship patterns of the clause, for
            // endpoint mode's uniqueness rule.
            let others: Vec<&RelPat> = patterns
                .iter()
                .enumerate()
                .flat_map(|(qi, q)| q.steps.iter().map(move |(r, _)| (qi, r)))
                .filter(|(qi, _)| *qi != pi)
                .map(|(_, r)| r)
                .collect();
            rows = self.match_pattern(rows, p, &conj, mult_ok, &others).await?;
            self.check_rows(rows.len())?;
            self.check_memory(&rows)?;
            if rows.is_empty() {
                break;
            }
        }
        if let Some(w) = where_ {
            rows = self.filter(rows, w).await?;
        }
        if optional {
            let hit: HashSet<i64> = rows
                .iter()
                .filter_map(|r| match r.get(ROW_ID) {
                    Some(Value::Int(i)) => Some(*i),
                    _ => None,
                })
                .collect();
            let new_vars: Vec<String> = patterns
                .iter()
                .flat_map(pattern_vars)
                .filter(|v| input.first().is_none_or(|r| !r.contains_key(v)))
                .collect();
            for (i, r) in input.iter().enumerate() {
                if !hit.contains(&i64::try_from(i).unwrap_or(0)) {
                    let mut r = r.clone();
                    for v in &new_vars {
                        r.insert(v.clone(), Value::Null);
                    }
                    rows.push(r);
                }
            }
            // Keep the input order.
            rows.sort_by_key(|r| match r.get(ROW_ID) {
                Some(Value::Int(i)) => *i,
                _ => i64::MAX,
            });
        }
        for r in &mut rows {
            r.remove(ROW_ID);
            r.remove(RELS);
        }
        Ok(rows)
    }

    /// The indexes of `rows` for which `p` has a match (pattern
    /// predicates): its unbound variables are existential.
    pub(crate) async fn pattern_hits(
        &mut self,
        rows: &[Row],
        p: &PathPattern,
    ) -> Result<HashSet<usize>> {
        let tagged: Vec<Row> = rows
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let mut r = r.clone();
                r.insert(ROW_ID.into(), Value::Int(i64::try_from(i).unwrap_or(0)));
                r.insert(RELS.into(), Value::List(vec![]));
                r
            })
            .collect();
        let out = Box::pin(self.match_pattern(tagged, p, &[], true, &[])).await?;
        Ok(out
            .iter()
            .filter_map(|r| match r.get(ROW_ID) {
                Some(Value::Int(i)) => usize::try_from(*i).ok(),
                _ => None,
            })
            .collect())
    }

    #[allow(clippy::option_option)] // unbound, bound to a non-node, bound to a node
    fn bound_node(row: &Row, var: Option<&String>) -> Option<Option<Arc<Node>>> {
        let v = var?;
        match row.get(v)? {
            Value::Node(n) => Some(Some(n.clone())),
            _ => Some(None),
        }
    }

    /// Whether `n` matches the node pattern's labels and properties.
    fn node_ok(&self, np: &NodePat, n: &Node, row: &Row) -> Result<bool> {
        if let Some(l) = &np.labels
            && !l.matches(&n.labels)
        {
            return Ok(false);
        }
        if let Some(pe) = &np.props {
            match eval(pe, row, self.params)? {
                Value::Map(m) => {
                    for (k, v) in m {
                        let have = n.props.get(&k).cloned().unwrap_or(Value::Null);
                        if have.cypher_eq(&v) != Some(true) {
                            return Ok(false);
                        }
                    }
                }
                Value::Null => {}
                other => {
                    return Err(SprigError::new(
                        SprigCode::GraphPropertyType,
                        format!("a property map is expected, got {}", other.type_name()),
                    ));
                }
            }
        }
        Ok(true)
    }

    fn rel_ok(&self, rp: &RelPat, r: &Rel, row: &Row) -> Result<bool> {
        if !rp.types.is_empty() && !rp.types.contains(&r.rtype) {
            return Ok(false);
        }
        if let Some(pe) = &rp.props
            && let Value::Map(m) = eval(pe, row, self.params)?
        {
            for (k, v) in m {
                let have = r.props.get(&k).cloned().unwrap_or(Value::Null);
                if have.cypher_eq(&v) != Some(true) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// How good a node is as the start point (chapter 43, step 1).
    pub(super) fn score(np: &NodePat, rows: &[Row], conj: &[Expr]) -> u32 {
        let Some(v) = &np.var else {
            return u32::from(np.labels.is_some()) * 10 + u32::from(np.props.is_some()) * 80;
        };
        if rows.first().is_some_and(|r| r.contains_key(v)) {
            return 1000;
        }
        let mut s = 0;
        if np.labels.as_ref().and_then(|l| l.required()).is_some() {
            s += 10;
        }
        if np.props.is_some() {
            s += 80;
        }
        for c in conj {
            if let Expr::Binary(BinOp::Eq | BinOp::In, a, b) = c {
                if let Expr::Func { name, args, .. } = &**a
                    && (name == "id" || name == "elementid")
                    && matches!(args.first(), Some(Expr::Var(x)) if x == v)
                    && is_const(b)
                {
                    s += 500;
                }
                if let Expr::Prop(t, _) = &**a
                    && matches!(&**t, Expr::Var(x) if x == v)
                    && is_const(b)
                {
                    s += 80;
                }
            }
        }
        s
    }

    /// The pattern read right to left.
    fn reversed(p: &PathPattern) -> PathPattern {
        let mut nodes: Vec<NodePat> = vec![p.start.clone()];
        nodes.extend(p.steps.iter().map(|(_, n)| n.clone()));
        let rels: Vec<RelPat> = p.steps.iter().map(|(r, _)| r.clone()).collect();
        nodes.reverse();
        let start = nodes.remove(0);
        let steps = rels
            .into_iter()
            .rev()
            .map(|mut r| {
                r.dir = r.dir.reverse();
                r
            })
            .zip(nodes)
            .collect();
        PathPattern {
            var: p.var.clone(),
            kind: p.kind,
            start,
            steps,
        }
    }

    /// The scan for an unbound start node: label, ids and pushed-down
    /// property equalities (a superset; rechecked).
    pub(super) async fn start_scan(&mut self, np: &NodePat, conj: &[Expr]) -> Result<Option<Scan>> {
        let mut s = Scan {
            limit: Some(self.b.max_rows.saturating_add(1)),
            ..Scan::default()
        };
        if let Some(l) = np.labels.as_ref().and_then(|l| l.required()) {
            match self.store.label_id(l, false).await? {
                Some(id) => s.label = Some(id),
                None => return Ok(None),
            }
        }
        let empty = Row::new();
        if let Some(pe) = &np.props
            && is_const(pe)
            && let Value::Map(m) = eval(pe, &empty, self.params)?
        {
            for (k, v) in m {
                if let Some(j) = scan_json(&v) {
                    s.props_eq.push((k, vec![j]));
                }
            }
        }
        if let Some(v) = &np.var {
            for c in conj {
                let Expr::Binary(op @ (BinOp::Eq | BinOp::In), a, b) = c else {
                    continue;
                };
                if !is_const(b) {
                    continue;
                }
                let vals = match (op, eval(b, &empty, self.params)?) {
                    (BinOp::Eq, x) => vec![x],
                    (BinOp::In, Value::List(l)) => l,
                    _ => continue,
                };
                match &**a {
                    Expr::Func { name, args, .. } if matches!(args.first(), Some(Expr::Var(x)) if x == v) =>
                    {
                        let ids: Vec<u64> = vals
                            .iter()
                            .filter_map(|x| match (name.as_str(), x) {
                                ("id", Value::Int(i)) => u64::try_from(*i).ok(),
                                ("elementid", Value::String(e)) => parse_element_id(e)
                                    .filter(|(k, g, _, _)| *k == 'n' && *g == self.store.graph_id())
                                    .map(|(_, _, _, id)| id),
                                _ => None,
                            })
                            .collect();
                        if name == "id" || name == "elementid" {
                            s.ids = Some(match s.ids.take() {
                                Some(prev) => {
                                    prev.into_iter().filter(|i| ids.contains(i)).collect()
                                }
                                None => ids,
                            });
                        }
                    }
                    Expr::Prop(t, k) if matches!(&**t, Expr::Var(x) if x == v) => {
                        let js: Vec<Json> = vals.iter().filter_map(scan_json).collect();
                        if js.len() == vals.len() {
                            s.props_eq.push((k.clone(), js));
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(Some(s))
    }

    /// One path pattern over `rows`.
    #[allow(clippy::too_many_lines)] // the pattern walk, step by step
    pub(crate) async fn match_pattern(
        &mut self,
        rows: Vec<Row>,
        pat: &PathPattern,
        conj: &[Expr],
        mult_ok: bool,
        others: &[&RelPat],
    ) -> Result<Vec<Row>> {
        if pat.kind != PathKind::Normal {
            return self.shortest(rows, pat).await;
        }
        let (pat, rev) = match pat.steps.last() {
            Some((_, last))
                if Self::score(last, &rows, conj) > Self::score(&pat.start, &rows, conj) =>
            {
                (Self::reversed(pat), true)
            }
            _ => (pat.clone(), false),
        };
        let track = pat.var.is_some();
        // Start points.
        let mut cur: Vec<(Row, Arc<Node>)> = Vec::new();
        let bound = rows
            .first()
            .is_some_and(|r| pat.start.var.as_ref().is_some_and(|v| r.contains_key(v)));
        if bound {
            for r in rows {
                match Self::bound_node(&r, pat.start.var.as_ref()) {
                    Some(Some(n)) => {
                        if self.node_ok(&pat.start, &n, &r)? {
                            cur.push((r, n));
                        }
                    }
                    Some(None) => {
                        if !r
                            .get(pat.start.var.as_deref().unwrap_or_default())
                            .is_some_and(Value::is_null)
                        {
                            return Err(SprigError::new(
                                SprigCode::GraphPropertyType,
                                format!(
                                    "Type mismatch: variable `{}` is not a node",
                                    pat.start.var.as_deref().unwrap_or_default()
                                ),
                            ));
                        }
                    }
                    None => {}
                }
            }
            self.plan.push(format!(
                "Argument {} (bound)",
                pat.start.var.as_deref().unwrap_or_default()
            ));
        } else {
            let Some(mut scan) = self.start_scan(&pat.start, conj).await? else {
                return Ok(Vec::new());
            };
            // Properties that depend on the incoming rows (`UNWIND $rows AS
            // r MATCH (n:L {k: r.k})`): one scan for every row's values, a
            // superset each row rechecks (round 3).
            if let Some(pe) = &pat.start.props
                && !is_const(pe)
                && scan.props_eq.is_empty()
                && !rows.is_empty()
                && rows.len() <= 10_000
            {
                let mut by_key: BTreeMap<String, Vec<Json>> = BTreeMap::new();
                let mut ok = true;
                for r in &rows {
                    match eval(pe, r, self.params) {
                        Ok(Value::Map(m)) => {
                            for (k, v) in m {
                                match scan_json(&v) {
                                    Some(j) => {
                                        let e = by_key.entry(k).or_default();
                                        if !e.contains(&j) {
                                            e.push(j);
                                        }
                                    }
                                    None => ok = false,
                                }
                            }
                        }
                        _ => ok = false,
                    }
                    if !ok {
                        break;
                    }
                }
                if ok {
                    scan.props_eq.extend(by_key);
                }
            }
            self.plan.push(format!(
                "NodeScan {}: {}",
                pat.start.var.as_deref().unwrap_or("(anonymous)"),
                if scan.ids.is_some() {
                    "by id"
                } else if !scan.props_eq.is_empty() {
                    "by property (pushed down, rechecked)"
                } else if scan.label.is_some() {
                    "label table"
                } else {
                    "full scan"
                }
            ));
            let found = self.store.scan(&scan).await?;
            self.check_rows(found.len())?;
            let nodes: Vec<Arc<Node>> = found.iter().map(|r| self.node_val(r)).collect();
            let mut work = 0usize;
            for r in rows {
                // Long in-gateway loops give the runtime back now and then.
                work += nodes.len();
                if work > 20_000 {
                    work = 0;
                    tokio::task::yield_now().await;
                }
                for n in &nodes {
                    if self.node_ok(&pat.start, n, &r)? {
                        let mut nr = r.clone();
                        if let Some(v) = &pat.start.var {
                            nr.insert(v.clone(), Value::Node(n.clone()));
                        }
                        cur.push((nr, n.clone()));
                    }
                }
                self.check_rows(cur.len())?;
            }
        }
        if track {
            for (r, n) in &mut cur {
                r.insert(PATH_NODES.into(), Value::List(vec![Value::Node(n.clone())]));
                r.insert(PATH_RELS.into(), Value::List(vec![]));
            }
        }
        let compatible = |o: &RelPat, rp: &RelPat| {
            o.types.is_empty()
                || rp.types.is_empty()
                || o.types.iter().any(|t| rp.types.contains(t))
        };
        for (si, (rp, np)) in pat.steps.iter().enumerate() {
            if cur.is_empty() {
                break;
            }
            cur = match rp.range {
                None => self.hop(cur, rp, np, track).await?,
                Some((lo, hi)) => {
                    let overlap = others.iter().any(|o| compatible(o, rp))
                        || pat
                            .steps
                            .iter()
                            .enumerate()
                            .any(|(sj, (o, _))| sj != si && compatible(o, rp));
                    let min = lo.unwrap_or(1);
                    let endpoint = mult_ok
                        && !track
                        && rp.var.is_none()
                        && rp.props.is_none()
                        && min <= 1
                        && rp.dir != Dir::Both
                        && !overlap;
                    if endpoint {
                        self.var_endpoints(cur, rp, np, min, hi).await?
                    } else {
                        self.var_paths(cur, rp, np, min, hi, track).await?
                    }
                }
            };
            self.check_rows(cur.len())?;
        }
        let mut out = Vec::with_capacity(cur.len());
        for (mut r, _) in cur {
            if let Some(pv) = &pat.var {
                let mut ns: Vec<Arc<Node>> = match r.remove(PATH_NODES) {
                    Some(Value::List(l)) => l
                        .into_iter()
                        .filter_map(|v| {
                            if let Value::Node(n) = v {
                                Some(n)
                            } else {
                                None
                            }
                        })
                        .collect(),
                    _ => vec![],
                };
                let mut rs: Vec<Arc<Rel>> = match r.remove(PATH_RELS) {
                    Some(Value::List(l)) => l
                        .into_iter()
                        .filter_map(|v| if let Value::Rel(x) = v { Some(x) } else { None })
                        .collect(),
                    _ => vec![],
                };
                if rev {
                    ns.reverse();
                    rs.reverse();
                }
                r.insert(pv.clone(), path_value(ns, rs));
            }
            out.push(r);
        }
        Ok(out)
    }

    /// Distinct node ids of the current rows.
    fn frontier(cur: &[(Row, Arc<Node>)]) -> Vec<u64> {
        let mut seen = HashSet::new();
        cur.iter()
            .map(|(_, n)| n.id)
            .filter(|i| seen.insert(*i))
            .collect()
    }

    async fn type_ids(&mut self, rp: &RelPat) -> Result<Option<Vec<u32>>> {
        let mut ids = Vec::new();
        for t in &rp.types {
            if let Some(i) = self.store.type_id(t, false).await? {
                ids.push(i);
            }
        }
        if !rp.types.is_empty() && ids.is_empty() {
            return Ok(None);
        }
        Ok(Some(ids))
    }

    /// One hop from `ids` in `dir`: near id to (relationship, far node),
    /// in batches of `graph.batch_nodes`; far nodes on other shards are
    /// fetched by id.
    async fn hop_edges(&mut self, ids: &[u64], types: &[u32], dir: Dir) -> Result<Hops> {
        let dirs: &[Dir] = match dir {
            Dir::Both => &[Dir::Out, Dir::In],
            Dir::Out => &[Dir::Out],
            Dir::In => &[Dir::In],
        };
        let mut raw: Vec<(u64, EdgeRow, Option<NodeRow>, u64)> = Vec::new();
        for d in dirs {
            for batch in ids.chunks(self.b.batch_nodes.max(1)) {
                for (e, far) in self.store.expand(batch, types, *d).await? {
                    let (near, far_id) = if *d == Dir::Out {
                        (e.src, e.dst)
                    } else {
                        (e.dst, e.src)
                    };
                    raw.push((near, e, far, far_id));
                }
                self.check_time()?;
            }
        }
        let missing: Vec<u64> = {
            let mut s = HashSet::new();
            raw.iter()
                .filter(|(_, _, f, _)| f.is_none())
                .map(|(_, _, _, id)| *id)
                .filter(|i| s.insert(*i))
                .collect()
        };
        let mut fetched: HashMap<u64, Arc<Node>> = HashMap::new();
        for batch in missing.chunks(self.b.batch_nodes.max(1)) {
            for r in self.store.nodes(batch).await? {
                fetched.insert(r.id, self.node_val(&r));
            }
        }
        let mut out: Hops = HashMap::new();
        let mut seen: HashSet<(u64, u64)> = HashSet::new();
        for (near, e, far, far_id) in raw {
            // A self-loop seen both ways under `Both` counts once.
            if !seen.insert((near, e.id)) {
                continue;
            }
            let far = match far {
                Some(r) => self.node_val(&r),
                None => match fetched.get(&far_id) {
                    Some(n) => n.clone(),
                    None => continue, // dangling: deleted in this transaction
                },
            };
            out.entry(near).or_default().push((self.rel_val(&e), far));
        }
        Ok(out)
    }

    fn row_rels(r: &Row) -> HashSet<u64> {
        match r.get(RELS) {
            Some(Value::List(l)) => l
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => u64::try_from(*i).ok(),
                    _ => None,
                })
                .collect(),
            _ => HashSet::new(),
        }
    }

    fn push_rel(r: &mut Row, id: u64) {
        if let Some(Value::List(l)) = r.get_mut(RELS) {
            l.push(Value::Int(i64::try_from(id).unwrap_or(i64::MAX)));
        }
    }

    fn push_path(r: &mut Row, rels: &[Arc<Rel>], nodes: &[Arc<Node>]) {
        if let Some(Value::List(l)) = r.get_mut(PATH_RELS) {
            l.extend(rels.iter().cloned().map(Value::Rel));
        }
        if let Some(Value::List(l)) = r.get_mut(PATH_NODES) {
            l.extend(nodes.iter().cloned().map(Value::Node));
        }
    }

    /// Binds the far node of a hop, checking a bound variable.
    fn bind_far(&self, r: &mut Row, np: &NodePat, far: &Arc<Node>) -> Result<bool> {
        if let Some(v) = &np.var {
            match r.get(v) {
                Some(Value::Node(b)) => {
                    if b.id != far.id {
                        return Ok(false);
                    }
                }
                Some(Value::Null) => return Ok(false),
                Some(_) => {
                    return Err(SprigError::new(
                        SprigCode::GraphPropertyType,
                        format!("Type mismatch: variable `{v}` is not a node"),
                    ));
                }
                None => {}
            }
        }
        if !self.node_ok(np, far, r)? {
            return Ok(false);
        }
        if let Some(v) = &np.var {
            r.insert(v.clone(), Value::Node(far.clone()));
        }
        Ok(true)
    }

    /// A fixed single hop: tier `join`.
    async fn hop(
        &mut self,
        cur: Vec<(Row, Arc<Node>)>,
        rp: &RelPat,
        np: &NodePat,
        track: bool,
    ) -> Result<Vec<(Row, Arc<Node>)>> {
        let Some(types) = self.type_ids(rp).await? else {
            return Ok(Vec::new());
        };
        metrics::counter!("sprig_graph_shard_queries_total", "tier" => "join").increment(1);
        self.plan.push(format!(
            "Expand {}{}: tier join, far node joined in the same statement",
            rp.var.as_deref().unwrap_or(""),
            if rp.types.is_empty() {
                String::new()
            } else {
                format!(":{}", rp.types.join("|"))
            }
        ));
        let ids = Self::frontier(&cur);
        let hops = self.hop_edges(&ids, &types, rp.dir).await?;
        let mut out = Vec::new();
        for (r, n) in cur {
            let Some(cands) = hops.get(&n.id) else {
                continue;
            };
            let used = Self::row_rels(&r);
            for (rel, far) in cands {
                if used.contains(&rel.id) {
                    continue;
                }
                if let Some(v) = &rp.var {
                    match r.get(v) {
                        Some(Value::Rel(b)) if b.id != rel.id => continue,
                        Some(Value::Null) => continue,
                        _ => {}
                    }
                }
                if !self.rel_ok(rp, rel, &r)? {
                    continue;
                }
                let mut nr = r.clone();
                if !self.bind_far(&mut nr, np, far)? {
                    continue;
                }
                if let Some(v) = &rp.var {
                    nr.insert(v.clone(), Value::Rel(rel.clone()));
                }
                Self::push_rel(&mut nr, rel.id);
                if track {
                    Self::push_path(
                        &mut nr,
                        std::slice::from_ref(rel),
                        std::slice::from_ref(far),
                    );
                }
                out.push((nr, far.clone()));
            }
            self.check_rows(out.len())?;
        }
        Ok(out)
    }

    fn max_hops(&self, hi: Option<u32>) -> Result<(u32, bool)> {
        match hi {
            Some(h) if h > self.b.max_hops => Err(budget_error(
                "hops",
                format!("*..{h} is more than graph.max_hops = {}", self.b.max_hops),
            )),
            Some(h) => Ok((h, false)),
            None => Ok((self.b.max_hops, true)),
        }
    }

    /// Endpoint mode: distinct (origin, endpoint) pairs at their smallest
    /// depth, by per-shard CTE bursts and gateway continuation.
    async fn var_endpoints(
        &mut self,
        cur: Vec<(Row, Arc<Node>)>,
        rp: &RelPat,
        np: &NodePat,
        min: u32,
        hi: Option<u32>,
    ) -> Result<Vec<(Row, Arc<Node>)>> {
        let Some(types) = self.type_ids(rp).await? else {
            return Ok(Vec::new());
        };
        let (max, unbounded) = self.max_hops(hi)?;
        self.plan.push(format!(
            "VarExpand *{min}..{}: endpoint mode, tier cte (bursts of {} hops), gateway continuation across shards",
            hi.map_or_else(String::new, |h| h.to_string()),
            self.b.local_burst_hops
        ));
        let origins: Vec<(u64, u64)> = {
            let mut seen = HashSet::new();
            cur.iter()
                .map(|(_, n)| (n.id, n.rk))
                .filter(|(i, _)| seen.insert(*i))
                .collect()
        };
        let mut best: HashMap<(u64, u64), u32> = HashMap::new();
        // (origin, node, rk, depth) still to expand.
        let mut pending: Vec<(u64, u64, u64, u32)> =
            origins.iter().map(|(o, rk)| (*o, *o, *rk, 0)).collect();
        if min == 0 {
            for (o, _) in &origins {
                best.insert((*o, *o), 0);
            }
        }
        let mut level_hist = 0u32;
        while !pending.is_empty() {
            self.check_time()?;
            if pending.len() > self.b.max_frontier {
                return Err(budget_error(
                    "frontier",
                    format!(
                        "{} nodes in one level, graph.max_frontier = {}",
                        pending.len(),
                        self.b.max_frontier
                    ),
                ));
            }
            metrics::histogram!("sprig_graph_frontier_nodes")
                .record(f64::from(u32::try_from(pending.len()).unwrap_or(u32::MAX)));
            // Group by (shard, depth).
            let mut groups: HashMap<(usize, u32), Vec<(u64, u64)>> = HashMap::new();
            let mut at_limit = Vec::new();
            for (o, n, rk, d) in pending.drain(..) {
                if d >= max {
                    at_limit.push((o, n, rk, d));
                    continue;
                }
                groups
                    .entry((self.store.shard_of(rk), d))
                    .or_default()
                    .push((o, n));
            }
            if unbounded && !at_limit.is_empty() {
                // Exact or an error: does any node at the limit lead
                // somewhere new?
                let ids: Vec<u64> = at_limit.iter().map(|x| x.1).collect();
                let hops = self.hop_edges(&ids, &types, rp.dir).await?;
                let more = at_limit.iter().any(|(o, n, _, d)| {
                    hops.get(n).is_some_and(|c| {
                        c.iter()
                            .any(|(_, f)| best.get(&(*o, f.id)).is_none_or(|b| *b > d + 1))
                    })
                });
                if more {
                    return Err(budget_error(
                        "hops",
                        format!(
                            "an unbounded pattern went past graph.max_hops = {}",
                            self.b.max_hops
                        ),
                    ));
                }
            }
            let mut next = Vec::new();
            let mut keys: Vec<(usize, u32)> = groups.keys().copied().collect();
            keys.sort_unstable();
            for (shard, depth) in keys {
                let seeds = groups.remove(&(shard, depth)).unwrap_or_default();
                let burst = self.b.local_burst_hops.max(1).min(max - depth);
                for batch in seeds.chunks(self.b.batch_nodes.max(1)) {
                    metrics::counter!("sprig_graph_shard_queries_total", "tier" => "cte")
                        .increment(1);
                    let found: Vec<Reach> = self
                        .store
                        .reach(shard, batch, &types, rp.dir, burst)
                        .await?;
                    self.check_rows(found.len())?;
                    for f in found {
                        let d = depth + f.depth;
                        let better = best.get(&(f.origin, f.node)).is_none_or(|b| *b > d);
                        if !better {
                            continue;
                        }
                        best.insert((f.origin, f.node), d);
                        let boundary = self.store.shard_of(f.rk) != shard;
                        // Continue from boundary nodes, and from nodes where
                        // the burst stopped.
                        if (boundary || f.depth == burst) && (d < max || unbounded) {
                            next.push((f.origin, f.node, f.rk, d));
                        }
                    }
                    if best.len() > self.b.max_rows {
                        return Err(budget_error(
                            "rows",
                            format!("more than graph.max_rows = {} endpoints", self.b.max_rows),
                        ));
                    }
                }
            }
            level_hist += 1;
            pending = next;
        }
        metrics::histogram!("sprig_graph_expansion_hops").record(f64::from(level_hist));
        // Fetch the endpoints.
        let mut by_origin: HashMap<u64, Vec<u64>> = HashMap::new();
        for ((o, n), d) in &best {
            if *d >= min && *d <= max {
                by_origin.entry(*o).or_default().push(*n);
            }
        }
        let ids: Vec<u64> = {
            let mut s = HashSet::new();
            by_origin
                .values()
                .flatten()
                .copied()
                .filter(|i| s.insert(*i))
                .collect()
        };
        let mut nodes: HashMap<u64, Arc<Node>> = HashMap::new();
        for batch in ids.chunks(self.b.batch_nodes.max(1)) {
            for r in self.store.nodes(batch).await? {
                nodes.insert(r.id, self.node_val(&r));
            }
        }
        let mut out = Vec::new();
        for (r, n) in cur {
            let Some(ends) = by_origin.get(&n.id) else {
                continue;
            };
            let mut ends = ends.clone();
            ends.sort_unstable();
            for e in ends {
                let Some(far) = nodes.get(&e) else { continue };
                let mut nr = r.clone();
                if self.bind_far(&mut nr, np, far)? {
                    out.push((nr, far.clone()));
                }
            }
            self.check_rows(out.len())?;
        }
        Ok(out)
    }

    /// Path mode: every trail of `min..=max` hops from each origin.
    #[allow(clippy::too_many_arguments)]
    async fn var_paths(
        &mut self,
        cur: Vec<(Row, Arc<Node>)>,
        rp: &RelPat,
        np: &NodePat,
        min: u32,
        hi: Option<u32>,
        track: bool,
    ) -> Result<Vec<(Row, Arc<Node>)>> {
        let Some(types) = self.type_ids(rp).await? else {
            return Ok(Vec::new());
        };
        let (max, unbounded) = self.max_hops(hi)?;
        self.plan.push(format!(
            "VarExpand *{min}..{}: path mode (trails), tier gateway, one statement per level and batch",
            hi.map_or_else(String::new, |h| h.to_string())
        ));
        let origins: Vec<Arc<Node>> = {
            let mut seen = HashSet::new();
            cur.iter()
                .map(|(_, n)| n.clone())
                .filter(|n| seen.insert(n.id))
                .collect()
        };
        let empty = Row::new();
        let mut done: HashMap<u64, Vec<Trail>> = HashMap::new();
        let mut live: Vec<(u64, Trail)> = origins
            .iter()
            .map(|o| {
                (
                    o.id,
                    Trail {
                        end: o.clone(),
                        rels: vec![],
                        nodes: vec![],
                    },
                )
            })
            .collect();
        let mut total = 0usize;
        if min == 0 {
            for (o, t) in &live {
                done.entry(*o).or_default().push(t.clone());
                total += 1;
            }
        }
        let mut level = 0u32;
        while !live.is_empty() {
            self.check_time()?;
            if level >= max {
                if unbounded {
                    let ids: Vec<u64> = live.iter().map(|(_, t)| t.end.id).collect();
                    let hops = self.hop_edges(&ids, &types, rp.dir).await?;
                    let more = live.iter().any(|(_, t)| {
                        hops.get(&t.end.id).is_some_and(|c| {
                            c.iter().any(|(r, _)| !t.rels.iter().any(|x| x.id == r.id))
                        })
                    });
                    if more {
                        return Err(budget_error(
                            "hops",
                            format!(
                                "an unbounded pattern went past graph.max_hops = {}",
                                self.b.max_hops
                            ),
                        ));
                    }
                }
                break;
            }
            if live.len() > self.b.max_frontier {
                return Err(budget_error(
                    "frontier",
                    format!(
                        "{} paths in one level, graph.max_frontier = {}",
                        live.len(),
                        self.b.max_frontier
                    ),
                ));
            }
            let ids: Vec<u64> = {
                let mut s = HashSet::new();
                live.iter()
                    .map(|(_, t)| t.end.id)
                    .filter(|i| s.insert(*i))
                    .collect()
            };
            metrics::counter!("sprig_graph_shard_queries_total", "tier" => "gateway").increment(1);
            let hops = self.hop_edges(&ids, &types, rp.dir).await?;
            let mut next = Vec::new();
            for (o, t) in live {
                let Some(c) = hops.get(&t.end.id) else {
                    continue;
                };
                for (rel, far) in c {
                    if t.rels.iter().any(|x| x.id == rel.id) || !self.rel_ok(rp, rel, &empty)? {
                        continue;
                    }
                    let mut nt = t.clone();
                    nt.rels.push(rel.clone());
                    nt.nodes.push(far.clone());
                    nt.end = far.clone();
                    if level + 1 >= min {
                        done.entry(o).or_default().push(nt.clone());
                        total += 1;
                        if total > self.b.max_rows {
                            return Err(budget_error(
                                "rows",
                                format!("more than graph.max_rows = {} paths", self.b.max_rows),
                            ));
                        }
                    }
                    next.push((o, nt));
                }
            }
            live = next;
            level += 1;
        }
        let mut out = Vec::new();
        for (r, n) in cur {
            let Some(trails) = done.get(&n.id) else {
                continue;
            };
            let used = Self::row_rels(&r);
            for t in trails {
                if t.rels.iter().any(|x| used.contains(&x.id)) {
                    continue;
                }
                if let Some(v) = &rp.var
                    && let Some(Value::List(b)) = r.get(v)
                {
                    let ids: Vec<u64> = b
                        .iter()
                        .filter_map(|x| {
                            if let Value::Rel(r) = x {
                                Some(r.id)
                            } else {
                                None
                            }
                        })
                        .collect();
                    if ids != t.rels.iter().map(|x| x.id).collect::<Vec<_>>() {
                        continue;
                    }
                }
                let mut nr = r.clone();
                if !self.bind_far(&mut nr, np, &t.end)? {
                    continue;
                }
                if let Some(v) = &rp.var {
                    nr.insert(
                        v.clone(),
                        Value::List(t.rels.iter().cloned().map(Value::Rel).collect()),
                    );
                }
                for x in &t.rels {
                    Self::push_rel(&mut nr, x.id);
                }
                if track {
                    Self::push_path(&mut nr, &t.rels, &t.nodes);
                }
                out.push((nr, t.end.clone()));
            }
            self.check_rows(out.len())?;
        }
        Ok(out)
    }

    /// `shortestPath` / `allShortestPaths`: bidirectional breadth-first
    /// search between bound ends, expanding the smaller frontier.
    async fn shortest(&mut self, rows: Vec<Row>, pat: &PathPattern) -> Result<Vec<Row>> {
        let [(rp, end_pat)] = pat.steps.as_slice() else {
            return Err(SprigError::new(
                SprigCode::GraphSyntax,
                "shortestPath needs a pattern of one relationship",
            ));
        };
        let all = pat.kind == PathKind::AllShortest;
        let (lo, hi) = rp.range.unwrap_or((Some(1), Some(1)));
        let min = lo.unwrap_or(1);
        if min > 1 {
            return Err(SprigError::new(
                SprigCode::GraphUnsupported,
                "shortestPath with a minimum length above 1",
            ));
        }
        let (max, unbounded) = self.max_hops(hi)?;
        let Some(types) = self.type_ids(rp).await? else {
            return Ok(Vec::new());
        };
        self.plan.push(format!(
            "ShortestPath{}: bidirectional breadth-first search, tier gateway",
            if all { " (all)" } else { "" }
        ));
        let mut out = Vec::new();
        for r in rows {
            let a = Self::bound_node(&r, pat.start.var.as_ref());
            let b = Self::bound_node(&r, end_pat.var.as_ref());
            let (Some(Some(a)), Some(Some(b))) = (a, b) else {
                if pat
                    .start
                    .var
                    .as_ref()
                    .is_some_and(|v| r.get(v).is_some_and(Value::is_null))
                    || end_pat
                        .var
                        .as_ref()
                        .is_some_and(|v| r.get(v).is_some_and(Value::is_null))
                {
                    continue;
                }
                return Err(SprigError::new(
                    SprigCode::GraphUnsupported,
                    "shortestPath needs both end nodes bound by an earlier pattern",
                ));
            };
            if !self.node_ok(&pat.start, &a, &r)? || !self.node_ok(end_pat, &b, &r)? {
                continue;
            }
            let paths = self
                .bfs_paths(&a, &b, rp, &types, min, max, unbounded, all)
                .await?;
            for (ns, rs) in paths {
                let mut nr = r.clone();
                if let Some(v) = &rp.var {
                    nr.insert(
                        v.clone(),
                        Value::List(rs.iter().cloned().map(Value::Rel).collect()),
                    );
                }
                if let Some(pv) = &pat.var {
                    nr.insert(pv.clone(), path_value(ns, rs));
                }
                out.push(nr);
            }
            self.check_rows(out.len())?;
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn bfs_paths(
        &mut self,
        a: &Arc<Node>,
        b: &Arc<Node>,
        rp: &RelPat,
        types: &[u32],
        min: u32,
        max: u32,
        unbounded: bool,
        all: bool,
    ) -> Result<Vec<(Vec<Arc<Node>>, Vec<Arc<Rel>>)>> {
        if a.id == b.id {
            return Ok(if min == 0 {
                vec![(vec![a.clone()], vec![])]
            } else {
                vec![]
            });
        }
        let empty = Row::new();
        // node -> (depth, predecessors (prev node, rel))
        let mut fwd: Side = HashMap::from([(a.id, (0, vec![]))]);
        let mut bwd: Side = HashMap::from([(b.id, (0, vec![]))]);
        let mut nodes: HashMap<u64, Arc<Node>> =
            HashMap::from([(a.id, a.clone()), (b.id, b.clone())]);
        let mut ff = vec![a.id];
        let mut bf = vec![b.id];
        let (mut df, mut db) = (0u32, 0u32);
        let mut meet: Vec<u64> = Vec::new();
        while df + db < max {
            self.check_time()?;
            if ff.is_empty() || bf.is_empty() {
                return Ok(vec![]);
            }
            let forward = ff.len() <= bf.len();
            let (front, side, other, dir, depth) = if forward {
                (&ff, &mut fwd, &bwd, rp.dir, &mut df)
            } else {
                (&bf, &mut bwd, &fwd, rp.dir.reverse(), &mut db)
            };
            if front.len() > self.b.max_frontier {
                return Err(budget_error(
                    "frontier",
                    format!(
                        "{} nodes in one level, graph.max_frontier = {}",
                        front.len(),
                        self.b.max_frontier
                    ),
                ));
            }
            let ids = front.clone();
            let hops = self.hop_edges(&ids, types, dir).await?;
            *depth += 1;
            let d = *depth;
            let mut next = Vec::new();
            for near in &ids {
                for (rel, far) in hops.get(near).map_or(&[][..], Vec::as_slice) {
                    if !self.rel_ok(rp, rel, &empty)? {
                        continue;
                    }
                    nodes.entry(far.id).or_insert_with(|| far.clone());
                    match side.get_mut(&far.id) {
                        None => {
                            side.insert(far.id, (d, vec![(*near, rel.clone())]));
                            next.push(far.id);
                        }
                        Some((fd, preds)) if *fd == d && all => preds.push((*near, rel.clone())),
                        _ => {}
                    }
                }
            }
            meet = next
                .iter()
                .copied()
                .filter(|n| other.contains_key(n))
                .collect();
            if forward {
                ff = next;
            } else {
                bf = next;
            }
            if !meet.is_empty() {
                break;
            }
        }
        if meet.is_empty() {
            if unbounded && !ff.is_empty() && !bf.is_empty() {
                return Err(budget_error(
                    "hops",
                    format!(
                        "shortestPath went past graph.max_hops = {}",
                        self.b.max_hops
                    ),
                ));
            }
            return Ok(vec![]);
        }
        // The meeting nodes on a shortest path.
        let best = meet.iter().map(|m| fwd[m].0 + bwd[m].0).min().unwrap_or(0);
        let meet: Vec<u64> = meet
            .into_iter()
            .filter(|m| fwd[m].0 + bwd[m].0 == best)
            .collect();
        let limit = if all { self.b.max_rows } else { 1 };
        let mut out = Vec::new();
        for m in meet {
            let left = walk_back(&fwd, m, limit);
            let right = walk_back(&bwd, m, limit);
            for (ln, lr) in &left {
                for (rn, rr) in &right {
                    // a .. m, then m .. b.
                    let mut ns: Vec<u64> = ln.iter().rev().copied().collect();
                    ns.extend(rn.iter().skip(1));
                    let mut rs: Vec<Arc<Rel>> = lr.iter().rev().cloned().collect();
                    rs.extend(rr.iter().cloned());
                    out.push((
                        ns.iter().filter_map(|i| nodes.get(i).cloned()).collect(),
                        rs,
                    ));
                    if out.len() >= limit {
                        return Ok(out);
                    }
                }
            }
        }
        Ok(out)
    }
}

/// Every path from `m` back to its side's origin through the
/// predecessors: node ids from `m`, relationships from `m`'s side.
fn walk_back(side: &Side, m: u64, limit: usize) -> Vec<(Vec<u64>, Vec<Arc<Rel>>)> {
    let mut out = Vec::new();
    let mut stack: Vec<(Vec<u64>, Vec<Arc<Rel>>)> = vec![(vec![m], vec![])];
    while let Some((ns, rs)) = stack.pop() {
        let last = *ns.last().unwrap_or(&m);
        let preds = side
            .get(&last)
            .map(|(_, p)| p.as_slice())
            .unwrap_or_default();
        if preds.is_empty() {
            out.push((ns, rs));
            if out.len() >= limit {
                break;
            }
            continue;
        }
        for (p, r) in preds {
            let mut n2 = ns.clone();
            n2.push(*p);
            let mut r2 = rs.clone();
            r2.push(r.clone());
            stack.push((n2, r2));
        }
    }
    out
}

/// A property value as the JSON a pushed-down equality compares with:
/// only strings, booleans and integers (floats and other types are only
/// rechecked).
fn scan_json(v: &Value) -> Option<Json> {
    match v {
        Value::String(s) => Some(Json::String(s.clone())),
        Value::Int(i) => Some(Json::from(*i)),
        Value::Bool(b) => Some(Json::Bool(*b)),
        _ => None,
    }
}
