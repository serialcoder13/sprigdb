//! `MATCH`: start points, hops, variable-length expansion (with frontier
//! expansion across shards) and shortest paths.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use serde_json::Value as Json;
use sprig_common::{Result, SprigCode, SprigError};

use super::{Exec, RELS, budget_error, pattern_vars};
use crate::ast::{BinOp, Dir, Expr, NodePat, PathKind, PathPattern, RelPat};
use crate::eval::{Row, eval, path_value};
use crate::store::{EdgeRow, Link, NodeRow, Reach, Scan};
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

/// The `OR`ed parts of a predicate.
fn disjuncts<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
    match e {
        Expr::Binary(BinOp::Or, a, b) => {
            disjuncts(a, out);
            disjuncts(b, out);
        }
        other => out.push(other),
    }
}

/// `v.k` as (`v`, `k`).
fn prop_side(p: &Expr) -> Option<(&str, &str)> {
    match p {
        Expr::Prop(t, k) => match &**t {
            Expr::Var(v) => Some((v.as_str(), k.as_str())),
            _ => None,
        },
        _ => None,
    }
}

/// `v.k = e` or `e = v.k`, as (`v`, `k`, `e`).
fn prop_eq(e: &Expr) -> Option<(&str, &str, &Expr)> {
    let Expr::Binary(BinOp::Eq, a, b) = e else {
        return None;
    };
    match (prop_side(a), prop_side(b)) {
        (Some((v, k)), _) => Some((v, k, &**b)),
        (None, Some((v, k))) => Some((v, k, &**a)),
        (None, None) => None,
    }
}

/// The conjuncts the matcher picks start points with: the
/// `WHERE`'s own, `e = v.k` turned into `v.k = e`, and the `v.k IN [e1,
/// e2]` a disjunction implies when each of its branches holds a `v.k =
/// e` (LDBC IC14's `(a.id = x AND b.id = y) OR (a.id = y AND b.id =
/// x)`). They only choose and narrow scans; the `WHERE` is still
/// checked in full.
pub(super) fn scan_conjuncts(w: &Expr) -> Vec<Expr> {
    let mut out = Vec::new();
    for c in conjuncts(w) {
        match prop_eq(c) {
            Some((v, k, e)) => out.push(Expr::Binary(
                BinOp::Eq,
                Box::new(Expr::Prop(Box::new(Expr::Var(v.to_owned())), k.to_owned())),
                Box::new(e.clone()),
            )),
            None => out.push(c.clone()),
        }
        let mut branches = Vec::new();
        disjuncts(c, &mut branches);
        if branches.len() < 2 {
            continue;
        }
        let per: Vec<Vec<(&str, &str, &Expr)>> = branches
            .iter()
            .map(|b| conjuncts(b).into_iter().filter_map(prop_eq).collect())
            .collect();
        let mut done: Vec<(&str, &str)> = Vec::new();
        for &(v, k, _) in &per[0] {
            if done.contains(&(v, k)) {
                continue;
            }
            done.push((v, k));
            let alts: Vec<Expr> = per
                .iter()
                .filter_map(|p| p.iter().find(|(v2, k2, _)| *v2 == v && *k2 == k))
                .map(|(_, _, e)| (*e).clone())
                .collect();
            if alts.len() == per.len() {
                out.push(Expr::Binary(
                    BinOp::In,
                    Box::new(Expr::Prop(Box::new(Expr::Var(v.to_owned())), k.to_owned())),
                    Box::new(Expr::List(alts)),
                ));
            }
        }
    }
    out
}

/// Whether `e` depends only on variables every one of `rows` binds
/// (and on parameters): one value per incoming row.
fn row_bound(e: &Expr, rows: &[Row]) -> bool {
    let Some(first) = rows.first() else {
        return false;
    };
    let mut ok = true;
    e.walk(&mut |x| match x {
        Expr::Var(v) => ok &= first.contains_key(v),
        Expr::Pattern(_)
        | Expr::Slot(_)
        | Expr::Subquery(..)
        | Expr::ListComp { .. }
        | Expr::Quantifier { .. }
        | Expr::Reduce { .. } => ok = false,
        _ => {}
    });
    ok
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

/// One side of a breadth-first search: node to (depth, predecessors
/// (previous node, edge id)).
type Side = HashMap<u64, (u32, Vec<(u64, u64)>)>;

/// A trail found by path-mode expansion: its end, its edges and the
/// nodes after its origin, by id.
#[derive(Clone)]
struct Trail {
    end: u64,
    rels: Vec<u64>,
    nodes: Vec<u64>,
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
        // No incoming rows, no matches: a `MATCH` after one that found
        // nothing must not scan for its first node (LDBC IS7's
        // `OPTIONAL MATCH (m)-...` after an empty `MATCH` read every node
        // of the graph, 2-6 s and a gigabyte, before answering no rows).
        if rows.is_empty() {
            return Ok(rows);
        }
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
        let conj: Vec<Expr> = where_.map(scan_conjuncts).unwrap_or_default();
        if self.late
            && let Some(w) = where_
        {
            self.only = self.only_ids(w, &mut rows).await?;
        }
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
            // The WHERE's conjuncts whose variables are all bound now
            // filter before the next pattern expands every row (LDBC
            // IC3's `MATCH (friend)<-[:HAS_CREATOR]-(message),
            // (message)-[:IS_LOCATED_IN]->(country) WHERE <date range on
            // message>` expanded all 190,000 messages twice). The whole
            // WHERE is still checked at the end.
            if pi + 1 < patterns.len()
                && let Some(w) = where_
                && let Some(first) = rows.first()
            {
                let ready: Vec<&Expr> = conjuncts(w)
                    .into_iter()
                    .filter(|c| {
                        let mut ok = true;
                        c.walk(&mut |x| match x {
                            Expr::Var(v) => ok &= first.contains_key(v),
                            Expr::Pattern(_) | Expr::Subquery(..) => ok = false,
                            Expr::Func { name, .. }
                                if matches!(name.as_str(), "startnode" | "endnode") =>
                            {
                                ok = false;
                            }
                            _ => {}
                        });
                        ok
                    })
                    .collect();
                if !ready.is_empty() {
                    self.late(&mut rows, &ready).await?;
                    let mut kept = Vec::with_capacity(rows.len());
                    for r in rows {
                        let mut keep = true;
                        for c in &ready {
                            // A local variable of a comprehension isn't in
                            // the row: such a conjunct waits for the end.
                            if let Ok(t) = eval(c, &r, self.params).and_then(|v| v.truth())
                                && t != Some(true)
                            {
                                keep = false;
                                break;
                            }
                        }
                        if keep {
                            kept.push(r);
                        }
                    }
                    rows = kept;
                }
            }
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
        if rows.is_empty() {
            return Ok(HashSet::new());
        }
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
                        let have = n.prop(&k)?;
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
                let have = r.prop(&k)?;
                if have.cypher_eq(&v) != Some(true) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// How good a node is as the start point.
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
                    && (is_const(b) || row_bound(b, rows))
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
        if rows.is_empty() {
            return Ok(rows);
        }
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
        // A property map on the start node compares its properties (and
        // maybe the rows' values): read them first.
        let mut rows = rows;
        if let Some(pe) = &pat.start.props {
            let need = Self::map_need(pat.start.var.as_ref(), pe);
            self.late_needs(&mut rows, &need).await?;
        }
        if pat.start.labels.is_some()
            && let Some(v) = &pat.start.var
        {
            let need = HashMap::from([(v.clone(), Self::labels_need())]);
            self.late_needs(&mut rows, &need).await?;
        }
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
            // superset each row rechecks.
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
            // Equalities with values of the incoming rows (`WHERE n.k =
            // r.x`, or what a disjunction implies): one scan for every
            // row's values, then each row meets only the nodes its own
            // value finds.
            let join = self
                .row_pushdown(&pat.start, conj, &mut rows, &mut scan)
                .await?;
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
            let by_key: Option<(&RowJoin, HashMap<crate::value::Key, Vec<usize>>)> =
                join.as_ref().map(|j| {
                    let mut m: HashMap<crate::value::Key, Vec<usize>> = HashMap::new();
                    for (i, n) in nodes.iter().enumerate() {
                        if let Some(v) = n.props.get(&j.key) {
                            m.entry(v.key()).or_default().push(i);
                        }
                    }
                    (j, m)
                });
            let all: Vec<usize> = (0..nodes.len()).collect();
            let mut work = 0usize;
            for (ri, r) in rows.into_iter().enumerate() {
                let picked: Vec<usize> = match &by_key {
                    Some((j, m)) => {
                        let mut v: Vec<usize> = Vec::new();
                        for x in &j.per_row[ri] {
                            if let Some(is) = m.get(&x.key()) {
                                v.extend(is);
                            }
                        }
                        v.sort_unstable();
                        v.dedup();
                        v
                    }
                    None => all.clone(),
                };
                // Long in-gateway loops give the runtime back now and then.
                work += picked.len() + 1;
                if work > 20_000 {
                    work = 0;
                    tokio::task::yield_now().await;
                }
                for n in picked.iter().map(|&i| &nodes[i]) {
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

    /// [`scan_conjuncts`] whose values come from the incoming `rows`
    /// (`n.k = r.x`, `n.k IN r.xs`): every row's values become one more
    /// pushed-down equality of `scan` (a superset each row rechecks),
    /// and the first of them joins rows to the nodes found.
    async fn row_pushdown(
        &mut self,
        np: &NodePat,
        conj: &[Expr],
        rows: &mut [Row],
        scan: &mut Scan,
    ) -> Result<Option<RowJoin>> {
        let Some(v) = &np.var else {
            return Ok(None);
        };
        if rows.is_empty() || rows.len() > 10_000 {
            return Ok(None);
        }
        let mut join = None;
        for c in conj {
            let Expr::Binary(op @ (BinOp::Eq | BinOp::In), a, b) = c else {
                continue;
            };
            let Expr::Prop(t, k) = &**a else { continue };
            if !matches!(&**t, Expr::Var(x) if x == v) || is_const(b) || !row_bound(b, rows) {
                continue;
            }
            let list = *op == BinOp::In;
            let per_row: Vec<Vec<Value>> = self
                .eval_rows(b, rows)
                .await?
                .into_iter()
                .map(|v| match (list, v) {
                    (_, Value::Null)
                    | (true, Value::Bool(_) | Value::Int(_) | Value::String(_)) => {
                        vec![]
                    }
                    (true, Value::List(l)) => l.into_iter().filter(|x| !x.is_null()).collect(),
                    (true, _) => vec![],
                    (false, x) => vec![x],
                })
                .collect();
            let mut js: Vec<Json> = Vec::new();
            let mut ok = true;
            for vals in &per_row {
                for x in vals {
                    match scan_json(x) {
                        Some(jv) => {
                            if !js.contains(&jv) {
                                js.push(jv);
                            }
                        }
                        None => ok = false,
                    }
                }
                if !ok {
                    break;
                }
            }
            if !ok {
                continue;
            }
            scan.props_eq.push((k.clone(), js));
            if join.is_none() {
                join = Some(RowJoin {
                    key: k.clone(),
                    per_row,
                });
            }
        }
        Ok(join)
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

    /// For each conjunct `v IN list` of `w` whose `v` the rows don't bind
    /// and whose list they do: every row's list, when all of them are
    /// node lists, as sorted ids (the nodes `v` may be).
    async fn only_ids(&mut self, w: &Expr, rows: &mut [Row]) -> Result<HashMap<String, Vec<u64>>> {
        let mut out = HashMap::new();
        if rows.is_empty() {
            return Ok(out);
        }
        for c in conjuncts(w) {
            let Expr::Binary(BinOp::In, a, list) = c else {
                continue;
            };
            let Expr::Var(v) = &**a else { continue };
            if rows[0].contains_key(v) || !row_bound(list, rows) {
                continue;
            }
            let lists = self.eval_rows(list, rows).await?;
            let mut ids: Vec<u64> = Vec::new();
            let mut ok = true;
            for l in lists {
                match l {
                    Value::List(xs) => {
                        for x in xs {
                            match x {
                                Value::Node(n) => ids.push(n.id),
                                Value::Null => {}
                                _ => ok = false,
                            }
                        }
                    }
                    Value::Null => {}
                    _ => ok = false,
                }
                if !ok {
                    break;
                }
            }
            if ok {
                ids.sort_unstable();
                ids.dedup();
                out.insert(v.clone(), ids);
            }
        }
        Ok(out)
    }

    /// A need for a node's labels alone.
    fn labels_need() -> super::late::Need {
        super::late::Need::Keys(std::collections::BTreeSet::from([
            super::late::LABELS.to_owned()
        ]))
    }

    /// What checking node pattern `{..}` map `pe` on `var` reads: the
    /// map's keys of the node, and what its values read of the rows.
    fn map_need(var: Option<&String>, pe: &Expr) -> HashMap<String, super::late::Need> {
        let mut need = HashMap::new();
        super::late::needs(pe, true, &mut need);
        if let Some(v) = var {
            let n = match pe {
                Expr::Map(m) => super::late::Need::Keys(m.iter().map(|(k, _)| k.clone()).collect()),
                _ => super::late::Need::All,
            };
            need.insert(v.clone(), n);
        }
        need
    }

    /// [`Exec::hop_edges`] without properties: links, and the far
    /// nodes' ids, hashes and labels (`far`; else stand-ins with the id
    /// and hash alone, for anonymous unlabelled nodes nothing reads).
    #[allow(clippy::too_many_arguments)] // the hop, its far node and its relationship
    async fn hop_light(
        &mut self,
        ids: &[u64],
        types: &[u32],
        dir: Dir,
        far: bool,
        var: Option<&String>,
        extra: Option<&super::late::Need>,
        rel_var: Option<&String>,
    ) -> Result<Hops> {
        let dirs: &[Dir] = match dir {
            Dir::Both => &[Dir::Out, Dir::In],
            Dir::Out => &[Dir::Out],
            Dir::In => &[Dir::In],
        };
        // With the far nodes wanted, they come in the same statement where
        // the store can (shards on one cluster), with the keys and filters
        // the clause is known to need; the rest are read by id.
        let need = self.need_of(var, extra);
        let keys: Option<Vec<String>> = match &need {
            None => Some(Vec::new()),
            Some(super::late::Need::All) => None,
            Some(super::late::Need::Keys(ks)) => Some(
                ks.iter()
                    .filter(|k| !k.starts_with('\u{1}'))
                    .cloned()
                    .collect(),
            ),
        };
        let filter = var
            .and_then(|v| self.push.get(v))
            .cloned()
            .unwrap_or_default();
        let only = var.and_then(|v| self.only.get(v)).cloned();
        // A named far node nothing is known to need yet is bound without
        // being read (its labels too); a later clause reads what it needs.
        let far =
            far || (var.is_some() && (need.is_some() || !filter.is_empty() || only.is_some()));
        if only.as_ref().is_some_and(Vec::is_empty) {
            return Ok(HashMap::new());
        }
        let only = only.unwrap_or_default();
        // The relationship's properties the clause is known to read, and
        // its numeric filters, in the same statement.
        let rel_need = rel_var.and_then(|v| self.prefetch.get(v)).cloned();
        let rel_keys: Option<Vec<String>> = match &rel_need {
            None => None,
            Some(super::late::Need::All) => Some(Vec::new()),
            Some(super::late::Need::Keys(ks)) => {
                let k: Vec<String> = ks
                    .iter()
                    .filter(|k| !k.starts_with('\u{1}'))
                    .cloned()
                    .collect();
                (!k.is_empty()).then_some(k)
            }
        };
        let rel_filter = rel_var
            .and_then(|v| self.push.get(v))
            .cloned()
            .unwrap_or_default();
        let mut rel_props: HashMap<u64, Arc<Rel>> = HashMap::new();
        let mut raw: Vec<(u64, u64, u64, Link)> = Vec::new();
        let mut seen: HashSet<(u64, u64)> = HashSet::new();
        let mut nodes: HashMap<u64, Arc<Node>> = HashMap::new();
        for d in dirs {
            for batch in ids.chunks(self.id_batch()) {
                // Joined far nodes save a round trip, but over many links
                // the joins cost more in MariaDB than reading the nodes by
                // id: only small frontiers join them.
                let join = far && batch.len() <= JOIN_FAR_MAX;
                let found =
                    if join || !only.is_empty() || rel_keys.is_some() || !rel_filter.is_empty() {
                        let fr = crate::store::FarRead {
                            keys: keys.as_deref(),
                            filter: &filter,
                            only: &only,
                            join,
                            rel_keys: rel_keys.as_deref(),
                            rel_filter: &rel_filter,
                        };
                        self.store.links_far(batch, types, *d, &fr).await?
                    } else {
                        self.store
                            .links(batch, types, *d)
                            .await?
                            .into_iter()
                            .map(|l| (l, None, None))
                            .collect()
                    };
                for (l, n, rp) in found {
                    if let Some((props, types)) = rp
                        && !rel_props.contains_key(&l.id)
                    {
                        let r = self.rel_from(&l, &props, types.as_ref(), rel_keys.as_deref());
                        rel_props.insert(l.id, r);
                    }
                    let (near, far_id, far_rk) = if *d == Dir::Out {
                        (l.src, l.dst, l.dst_rk)
                    } else {
                        (l.dst, l.src, l.src_rk)
                    };
                    if let Some(r) = n
                        && !nodes.contains_key(&r.id)
                    {
                        let node = match keys.as_deref() {
                            Some([]) => self.bare_node(&r),
                            k => {
                                let node = self.merged_node(&r, k);
                                self.seen_nodes.insert(node.id, Arc::clone(&node));
                                node
                            }
                        };
                        nodes.insert(r.id, node);
                    }
                    if seen.insert((near, l.id)) {
                        raw.push((near, far_id, far_rk, l));
                    }
                }
                self.check_time()?;
            }
        }
        if far {
            let rest: Vec<u64> = raw
                .iter()
                .map(|(_, f, _, _)| *f)
                .filter(|f| !nodes.contains_key(f))
                .collect();
            if !rest.is_empty() {
                nodes.extend(self.read_nodes(&rest, var, extra).await?);
            }
        }
        let graph = self.store.graph_id();
        let mut out: Hops = HashMap::new();
        for (near, f, rk, l) in raw {
            let far = if far {
                match nodes.get(&f) {
                    Some(n) => Arc::clone(n),
                    None => continue, // dangling: deleted in this transaction
                }
            } else {
                Arc::new(Node {
                    graph,
                    id: f,
                    rk,
                    labels: Vec::new(),
                    props: crate::value::Props::new(),
                    partial: Some(Arc::from(vec![super::late::NO_LABELS.to_owned()])),
                })
            };
            let rel = match rel_props.get(&l.id).or_else(|| self.seen_rels.get(&l.id)) {
                Some(r) => Arc::clone(r),
                None => Arc::new(Rel {
                    graph,
                    id: l.id,
                    src: l.src,
                    dst: l.dst,
                    src_rk: l.src_rk,
                    dst_rk: l.dst_rk,
                    rtype: self
                        .store
                        .type_name(l.rtype)
                        .unwrap_or_else(|| format!("#{}", l.rtype)),
                    props: crate::value::Props::new(),
                    partial: Some(Arc::from(Vec::<String>::new())),
                }),
            };
            out.entry(near).or_default().push((rel, far));
        }
        Ok(out)
    }

    /// A relationship from its link and the properties read with it
    /// (`keys`: those keys; `Some(&[])` or `None`: all).
    fn rel_from(
        &mut self,
        l: &Link,
        props: &serde_json::Value,
        types: Option<&serde_json::Value>,
        keys: Option<&[String]>,
    ) -> Arc<Rel> {
        let partial: crate::value::Partial = match keys {
            Some(k) if !k.is_empty() => {
                // With what the statement read of it before.
                let mut have: Vec<String> = k.to_vec();
                if let Some(old) = self.seen_rels.get(&l.id) {
                    match &old.partial {
                        None => return Arc::clone(old),
                        Some(ok) => {
                            for x in ok.iter() {
                                if !have.contains(x) {
                                    have.push(x.clone());
                                }
                            }
                        }
                    }
                }
                Some(have.into())
            }
            _ => None,
        };
        let mut p = crate::value::props_from_json(props, types);
        if let (Some(_), Some(old)) = (&partial, self.seen_rels.get(&l.id)) {
            for (k, v) in &old.props {
                p.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
        let r = Arc::new(Rel {
            graph: self.store.graph_id(),
            id: l.id,
            src: l.src,
            dst: l.dst,
            src_rk: l.src_rk,
            dst_rk: l.dst_rk,
            rtype: self
                .store
                .type_name(l.rtype)
                .unwrap_or_else(|| format!("#{}", l.rtype)),
            props: p,
            partial,
        });
        self.seen_rels.insert(l.id, Arc::clone(&r));
        r
    }

    /// [`Exec::late_needs`] for the rows of `cur` (and their nodes).
    async fn late_cur(
        &mut self,
        cur: &mut [(Row, Arc<Node>)],
        need: &HashMap<String, super::late::Need>,
    ) -> Result<()> {
        if !self.late || need.is_empty() || cur.is_empty() {
            return Ok(());
        }
        let mut rows: Vec<Row> = cur.iter_mut().map(|(r, _)| std::mem::take(r)).collect();
        self.late_needs(&mut rows, need).await?;
        for ((r, _), nr) in cur.iter_mut().zip(rows) {
            *r = nr;
        }
        Ok(())
    }

    /// Nodes by id for binding to `np`: without properties when they're
    /// read late and `np` compares none here.
    async fn bind_nodes(&mut self, ids: &[u64], np: &NodePat) -> Result<HashMap<u64, Arc<Node>>> {
        if self.late && np.props.is_none() {
            return self.read_nodes(ids, np.var.as_ref(), None).await;
        }
        let mut out = HashMap::new();
        let mut want = Vec::new();
        for id in ids {
            match self.seen_nodes.get(id) {
                Some(n) if n.partial.is_none() => {
                    out.insert(*id, Arc::clone(n));
                }
                _ => want.push(*id),
            }
        }
        want.sort_unstable();
        want.dedup();
        for batch in want.chunks(self.id_batch()) {
            for r in self.store.nodes(batch).await? {
                out.insert(r.id, self.node_val(&r));
            }
        }
        Ok(out)
    }

    /// Relationships by `(src_rk, id)`: when read late, made from their
    /// `links` without a read (properties come when a clause needs them);
    /// else read whole.
    async fn rels_by_key(
        &mut self,
        keys: Vec<(u64, u64)>,
        links: &HashMap<u64, Link>,
    ) -> Result<HashMap<u64, Arc<Rel>>> {
        let mut keys = keys;
        keys.sort_unstable();
        keys.dedup();
        let mut rels: HashMap<u64, Arc<Rel>> = HashMap::new();
        let graph = self.store.graph_id();
        keys.retain(|(_, id)| {
            if let Some(r) = self.seen_rels.get(id) {
                rels.insert(*id, Arc::clone(r));
                return false;
            }
            if self.late
                && let Some(l) = links.get(id)
            {
                rels.insert(
                    *id,
                    Arc::new(Rel {
                        graph,
                        id: l.id,
                        src: l.src,
                        dst: l.dst,
                        src_rk: l.src_rk,
                        dst_rk: l.dst_rk,
                        rtype: self
                            .store
                            .type_name(l.rtype)
                            .unwrap_or_else(|| format!("#{}", l.rtype)),
                        props: crate::value::Props::new(),
                        partial: Some(Arc::from(Vec::<String>::new())),
                    }),
                );
                return false;
            }
            true
        });
        for batch in keys.chunks(self.id_batch()) {
            for e in self.store.edges_at(batch).await? {
                rels.insert(e.id, self.rel_val(&e));
            }
        }
        Ok(rels)
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
        let mut cur = cur;
        let ids = Self::frontier(&cur);
        let hops = if self.late && rp.props.is_none() {
            // Links, and the far nodes' labels when the pattern names or
            // labels them; properties are read when a clause needs them
            // (here, the keys a property map on the far node compares).
            let extra = match &np.props {
                None => None,
                Some(pe) => {
                    let mut need = HashMap::new();
                    super::late::needs(pe, true, &mut need);
                    self.late_cur(&mut cur, &need).await?;
                    Some(match pe {
                        Expr::Map(m) => {
                            super::late::Need::Keys(m.iter().map(|(k, _)| k.clone()).collect())
                        }
                        _ => super::late::Need::All,
                    })
                }
            };
            let far = np.labels.is_some() || np.props.is_some() || track;
            self.hop_light(
                &ids,
                &types,
                rp.dir,
                far,
                np.var.as_ref(),
                extra.as_ref(),
                rp.var.as_ref(),
            )
            .await?
        } else {
            // Property maps on the pattern compare far elements here.
            let mut need = HashMap::new();
            for pe in [np.props.as_ref(), rp.props.as_ref()].into_iter().flatten() {
                super::late::needs(pe, true, &mut need);
            }
            self.late_cur(&mut cur, &need).await?;
            self.hop_edges(&ids, &types, rp.dir).await?
        };
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
        let nodes = self.bind_nodes(&ids, np).await?;
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

    /// Path mode: every trail of `min..=max` hops from each origin. The
    /// levels read links only; the trails' ends (and their relationships
    /// and nodes when the pattern binds them) are read at the end.
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
        let mut links: HashMap<u64, Link> = HashMap::new();
        let mut done: HashMap<u64, Vec<Trail>> = HashMap::new();
        let mut live: Vec<(u64, Trail)> = origins
            .iter()
            .map(|o| {
                (
                    o.id,
                    Trail {
                        end: o.id,
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
                    let ids: Vec<u64> = live.iter().map(|(_, t)| t.end).collect();
                    let hops = self
                        .level_links(&ids, &types, rp, rp.dir, &mut links)
                        .await?;
                    let more = live.iter().any(|(_, t)| {
                        hops.get(&t.end)
                            .is_some_and(|c| c.iter().any(|(_, r)| !t.rels.contains(r)))
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
                    .map(|(_, t)| t.end)
                    .filter(|i| s.insert(*i))
                    .collect()
            };
            metrics::counter!("sprig_graph_shard_queries_total", "tier" => "gateway").increment(1);
            let hops = self
                .level_links(&ids, &types, rp, rp.dir, &mut links)
                .await?;
            let mut next = Vec::new();
            for (o, t) in live {
                let Some(c) = hops.get(&t.end) else {
                    continue;
                };
                for &(far, rel) in c {
                    if t.rels.contains(&rel) {
                        continue;
                    }
                    let mut nt = t.clone();
                    nt.rels.push(rel);
                    nt.nodes.push(far);
                    nt.end = far;
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
        // The elements the rows bind: every trail's end; its relationships
        // when the pattern names them; its nodes too for a path variable.
        let want_rels = rp.var.is_some() || track;
        let mut node_ids: Vec<u64> = Vec::new();
        let mut rel_keys: Vec<(u64, u64)> = Vec::new();
        for ts in done.values() {
            for t in ts {
                node_ids.push(t.end);
                if track {
                    node_ids.extend(t.nodes.iter().copied());
                }
                if want_rels {
                    rel_keys.extend(
                        t.rels
                            .iter()
                            .filter_map(|r| links.get(r).map(|l| (l.src_rk, l.id))),
                    );
                }
            }
        }
        let mut nodes: HashMap<u64, Arc<Node>> =
            origins.iter().map(|n| (n.id, n.clone())).collect();
        node_ids.retain(|i| !nodes.contains_key(i));
        nodes.extend(self.bind_nodes(&node_ids, np).await?);
        let rels = self.rels_by_key(rel_keys, &links).await?;
        let mut out = Vec::new();
        for (r, n) in cur {
            let Some(trails) = done.get(&n.id) else {
                continue;
            };
            let used = Self::row_rels(&r);
            for t in trails {
                if t.rels.iter().any(|x| used.contains(x)) {
                    continue;
                }
                // A trail through an element gone in this transaction.
                let Some(end) = nodes.get(&t.end) else {
                    continue;
                };
                let trels: Vec<Arc<Rel>> = if want_rels {
                    match t.rels.iter().map(|x| rels.get(x).cloned()).collect() {
                        Some(v) => v,
                        None => continue,
                    }
                } else {
                    Vec::new()
                };
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
                    if ids != t.rels {
                        continue;
                    }
                }
                let mut nr = r.clone();
                if !self.bind_far(&mut nr, np, end)? {
                    continue;
                }
                if let Some(v) = &rp.var {
                    nr.insert(
                        v.clone(),
                        Value::List(trels.iter().cloned().map(Value::Rel).collect()),
                    );
                }
                for x in &t.rels {
                    Self::push_rel(&mut nr, *x);
                }
                if track {
                    let tnodes: Option<Vec<Arc<Node>>> =
                        t.nodes.iter().map(|x| nodes.get(x).cloned()).collect();
                    let Some(tnodes) = tnodes else { continue };
                    Self::push_path(&mut nr, &trels, &tnodes);
                }
                out.push((nr, end.clone()));
            }
            self.check_rows(out.len())?;
        }
        Ok(out)
    }

    /// `shortestPath` / `allShortestPaths`: bidirectional breadth-first
    /// search between bound ends, expanding the smaller frontier.
    async fn shortest(&mut self, rows: Vec<Row>, pat: &PathPattern) -> Result<Vec<Row>> {
        // Ends given in the pattern itself (IC14's `allShortestPaths((a:Person
        // {id: 1})-[:KNOWS*0..]-(b:Person {id: 2}))`): each is matched
        // first, as a node pattern of its own.
        let unbound = |np: &NodePat, rows: &[Row]| {
            np.var
                .as_ref()
                .is_none_or(|v| rows.first().is_none_or(|r| !r.contains_key(v)))
        };
        if let [(_, end)] = pat.steps.as_slice()
            && (unbound(&pat.start, &rows) || unbound(end, &rows))
        {
            let mut p = pat.clone();
            let mut rows = rows;
            for (k, np) in [(0, p.start.clone()), (1, end.clone())] {
                if !unbound(&np, &rows) {
                    continue;
                }
                let mut np = np;
                let v = np.var.get_or_insert_with(|| format!(" spe{k}")).clone();
                let one = PathPattern {
                    var: None,
                    kind: PathKind::Normal,
                    start: np.clone(),
                    steps: vec![],
                };
                rows = Box::pin(self.match_pattern(rows, &one, &[], true, &[])).await?;
                if k == 0 {
                    p.start.var = Some(v);
                } else {
                    p.steps[0].1.var = Some(v);
                }
            }
            let out = Box::pin(self.shortest(rows, &p)).await?;
            return Ok(out
                .into_iter()
                .map(|mut r| {
                    r.remove(" spe0");
                    r.remove(" spe1");
                    r
                })
                .collect());
        }
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
        // LDBC IC1's shape: many rows from one start node to
        // different ends, a bounded `shortestPath`. One breadth-first
        // search from the start serves them all, instead of one
        // bidirectional search per row.
        let mut many: HashMap<u64, HashMap<u64, Option<Path>>> = HashMap::new();
        if !all && !unbounded && min == 1 {
            let mut ends: HashMap<u64, (Arc<Node>, HashSet<u64>)> = HashMap::new();
            for r in &rows {
                if let (Some(Some(a)), Some(Some(b))) = (
                    Self::bound_node(r, pat.start.var.as_ref()),
                    Self::bound_node(r, end_pat.var.as_ref()),
                ) && a.id != b.id
                {
                    ends.entry(a.id)
                        .or_insert_with(|| (a.clone(), HashSet::new()))
                        .1
                        .insert(b.id);
                }
            }
            for (aid, (a, targets)) in ends {
                if targets.len() >= MANY_TARGETS {
                    let found = self.bfs_many(&a, &targets, rp, &types, max).await?;
                    let mut m: HashMap<u64, Option<Path>> =
                        targets.into_iter().map(|t| (t, None)).collect();
                    for (t, p) in found {
                        m.insert(t, Some(p));
                    }
                    many.insert(aid, m);
                }
            }
            if !many.is_empty() {
                self.plan.push(format!(
                    "ShortestPath: one breadth-first search per start node for {} start node(s)",
                    many.len()
                ));
            }
        }
        let mut rows = rows;
        for np in [&pat.start, end_pat] {
            if let Some(pe) = &np.props {
                let need = Self::map_need(np.var.as_ref(), pe);
                self.late_needs(&mut rows, &need).await?;
            }
            if np.labels.is_some()
                && let Some(v) = &np.var
            {
                let need = HashMap::from([(v.clone(), Self::labels_need())]);
                self.late_needs(&mut rows, &need).await?;
            }
        }
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
            let paths = match many.get(&a.id).and_then(|m| m.get(&b.id)) {
                Some(p) => p.iter().cloned().collect(),
                None => {
                    self.bfs_paths(&a, &b, rp, &types, min, max, unbounded, all)
                        .await?
                }
            };
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

    /// One hop's links from `ids` in `dir` (both ways for `Both`, a
    /// self-loop once), in batches of `graph.batch_nodes`: near id to
    /// (far id, edge id). Only ids are read; with a property map on the
    /// relationship pattern, the edges are read to check it. Every link
    /// kept is recorded in `links`.
    async fn level_links(
        &mut self,
        ids: &[u64],
        types: &[u32],
        rp: &RelPat,
        dir: Dir,
        links: &mut HashMap<u64, Link>,
    ) -> Result<HashMap<u64, Vec<(u64, u64)>>> {
        let dirs: &[Dir] = match dir {
            Dir::Both => &[Dir::Out, Dir::In],
            Dir::Out => &[Dir::Out],
            Dir::In => &[Dir::In],
        };
        let mut raw: Vec<(u64, u64, Link)> = Vec::new();
        let mut seen: HashSet<(u64, u64)> = HashSet::new();
        for d in dirs {
            for batch in ids.chunks(self.id_batch()) {
                for l in self.store.links(batch, types, *d).await? {
                    let (near, far) = if *d == Dir::Out {
                        (l.src, l.dst)
                    } else {
                        (l.dst, l.src)
                    };
                    if seen.insert((near, l.id)) {
                        raw.push((near, far, l));
                    }
                }
                self.check_time()?;
            }
        }
        if rp.props.is_some() {
            let mut keys: Vec<(u64, u64)> = raw.iter().map(|(_, _, l)| (l.src_rk, l.id)).collect();
            keys.sort_unstable();
            keys.dedup();
            let empty = Row::new();
            let mut ok: HashSet<u64> = HashSet::new();
            for batch in keys.chunks(self.b.batch_nodes.max(1)) {
                for e in self.store.edges_at(batch).await? {
                    if self.rel_ok(rp, &self.rel_val(&e), &empty)? {
                        ok.insert(e.id);
                    }
                }
            }
            raw.retain(|(_, _, l)| ok.contains(&l.id));
        }
        let mut out: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
        for (near, far, l) in raw {
            out.entry(near).or_default().push((far, l.id));
            links.insert(l.id, l);
        }
        Ok(out)
    }

    /// The nodes and relationships of `paths` (node ids, edge ids),
    /// read in batches; `known` nodes aren't read again. A path whose node
    /// or relationship is gone (deleted earlier in this transaction) is
    /// dropped.
    async fn materialize(
        &mut self,
        paths: Vec<IdPath>,
        links: &HashMap<u64, Link>,
        known: &[&Arc<Node>],
    ) -> Result<Vec<Path>> {
        let mut nodes: HashMap<u64, Arc<Node>> =
            known.iter().map(|n| (n.id, Arc::clone(n))).collect();
        let want: Vec<u64> = paths
            .iter()
            .flat_map(|(ns, _)| ns.iter().copied())
            .filter(|i| !nodes.contains_key(i))
            .collect();
        let any = NodePat {
            var: None,
            labels: None,
            props: None,
        };
        nodes.extend(self.bind_nodes(&want, &any).await?);
        let keys: Vec<(u64, u64)> = paths
            .iter()
            .flat_map(|(_, rs)| rs.iter())
            .filter_map(|id| links.get(id).map(|l| (l.src_rk, l.id)))
            .collect();
        let rels = self.rels_by_key(keys, links).await?;
        Ok(paths
            .into_iter()
            .filter_map(|(ns, rs)| {
                let ns: Option<Vec<Arc<Node>>> = ns.iter().map(|i| nodes.get(i).cloned()).collect();
                let rs: Option<Vec<Arc<Rel>>> = rs.iter().map(|i| rels.get(i).cloned()).collect();
                Some((ns?, rs?))
            })
            .collect())
    }

    /// One breadth-first search from `a` over at most `max` hops: a
    /// shortest path to each of `targets` it reaches (the first found at
    /// the smallest depth). It reads links only; the paths kept are read
    /// at the end.
    async fn bfs_many(
        &mut self,
        a: &Arc<Node>,
        targets: &HashSet<u64>,
        rp: &RelPat,
        types: &[u32],
        max: u32,
    ) -> Result<HashMap<u64, Path>> {
        let mut links: HashMap<u64, Link> = HashMap::new();
        let mut side: Side = HashMap::from([(a.id, (0, vec![]))]);
        let mut front = vec![a.id];
        let mut left: HashSet<u64> = targets.clone();
        let mut d = 0u32;
        while d < max && !front.is_empty() && !left.is_empty() {
            self.check_time()?;
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
            let hops = self
                .level_links(&front, types, rp, rp.dir, &mut links)
                .await?;
            d += 1;
            let mut next = Vec::new();
            for near in &front {
                for &(far, rel) in hops.get(near).map_or(&[][..], Vec::as_slice) {
                    if side.contains_key(&far) {
                        continue;
                    }
                    side.insert(far, (d, vec![(*near, rel)]));
                    left.remove(&far);
                    next.push(far);
                }
            }
            front = next;
        }
        let mut found: Vec<(u64, IdPath)> = Vec::new();
        for t in targets {
            if !side.contains_key(t) {
                continue;
            }
            if let Some((ns, rs)) = walk_back(&side, *t, 1).into_iter().next() {
                let ns: Vec<u64> = ns.into_iter().rev().collect();
                let rs: Vec<u64> = rs.into_iter().rev().collect();
                found.push((*t, (ns, rs)));
            }
        }
        let ends: Vec<u64> = found.iter().map(|(t, _)| *t).collect();
        let paths = self
            .materialize(found.into_iter().map(|(_, p)| p).collect(), &links, &[a])
            .await?;
        let mut out = HashMap::new();
        for p in paths {
            if let Some(last) = p.0.last()
                && ends.contains(&last.id)
            {
                out.insert(last.id, p);
            }
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
    ) -> Result<Vec<Path>> {
        if a.id == b.id {
            return Ok(if min == 0 {
                vec![(vec![a.clone()], vec![])]
            } else {
                vec![]
            });
        }
        let mut links: HashMap<u64, Link> = HashMap::new();
        // node -> (depth, predecessors (prev node, edge id))
        let mut fwd: Side = HashMap::from([(a.id, (0, vec![]))]);
        let mut bwd: Side = HashMap::from([(b.id, (0, vec![]))]);
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
            let front = if forward { ff.clone() } else { bf.clone() };
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
            let dir = if forward { rp.dir } else { rp.dir.reverse() };
            let hops = self.level_links(&front, types, rp, dir, &mut links).await?;
            let (side, other, depth) = if forward {
                (&mut fwd, &bwd, &mut df)
            } else {
                (&mut bwd, &fwd, &mut db)
            };
            *depth += 1;
            let d = *depth;
            let mut next = Vec::new();
            for near in &front {
                for &(far, rel) in hops.get(near).map_or(&[][..], Vec::as_slice) {
                    match side.get_mut(&far) {
                        None => {
                            side.insert(far, (d, vec![(*near, rel)]));
                            next.push(far);
                        }
                        Some((fd, preds)) if *fd == d && all => preds.push((*near, rel)),
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
        let mut out: Vec<IdPath> = Vec::new();
        'meet: for m in meet {
            let left = walk_back(&fwd, m, limit);
            let right = walk_back(&bwd, m, limit);
            for (ln, lr) in &left {
                for (rn, rr) in &right {
                    // a .. m, then m .. b.
                    let mut ns: Vec<u64> = ln.iter().rev().copied().collect();
                    ns.extend(rn.iter().skip(1));
                    let mut rs: Vec<u64> = lr.iter().rev().copied().collect();
                    rs.extend(rr.iter().copied());
                    out.push((ns, rs));
                    if out.len() >= limit {
                        break 'meet;
                    }
                }
            }
        }
        self.materialize(out, &links, &[a, b]).await
    }
}

/// Frontiers up to this many nodes read their far nodes in the links
/// statement.
const JOIN_FAR_MAX: usize = 64;

/// A path by ids: its nodes and edges in order.
type IdPath = (Vec<u64>, Vec<u64>);

/// A path: its nodes and relationships in order.
type Path = (Vec<Arc<Node>>, Vec<Arc<Rel>>);

/// Ends per start node from which one search serves every row.
const MANY_TARGETS: usize = 2;

/// Every path from `m` back to its side's origin through the
/// predecessors: node ids from `m`, edge ids from `m`'s side.
fn walk_back(side: &Side, m: u64, limit: usize) -> Vec<(Vec<u64>, Vec<u64>)> {
    let mut out = Vec::new();
    let mut stack: Vec<(Vec<u64>, Vec<u64>)> = vec![(vec![m], vec![])];
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
            r2.push(*r);
            stack.push((n2, r2));
        }
    }
    out
}

/// A property value as the JSON a pushed-down equality compares with:
/// only strings, booleans and integers (floats and other types are only
/// rechecked).
/// A start node's property matched to values of the incoming rows
/// ([`Exec::row_pushdown`]): the key, and each row's values (`null`s
/// match nothing).
struct RowJoin {
    key: String,
    per_row: Vec<Vec<Value>>,
}

fn scan_json(v: &Value) -> Option<Json> {
    match v {
        Value::String(s) => Some(Json::String(s.clone())),
        Value::Int(i) => Some(Json::from(*i)),
        Value::Bool(b) => Some(Json::Bool(*b)),
        _ => None,
    }
}
