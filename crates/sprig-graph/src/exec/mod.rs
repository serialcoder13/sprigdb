//! The graph executor: runs
//! a parsed statement over a [`GraphStore`] under the budgets.
//!
//! Each clause turns a table of binding rows into the next:
//!
//! - `MATCH` ([`matcher`]): a start point per pattern (a bound variable,
//!   ids, an indexed property with pushed-down equalities, the label table,
//!   or a full scan), then one batched shard statement per hop and batch of
//!   `graph.batch_nodes` frontier ids (the far node joined in the same
//!   statement), variable-length hops by recursive CTE bursts in endpoint
//!   mode or trail enumeration in path mode, and bidirectional search for
//!   shortest paths. Predicates are rechecked in the gateway with Cypher's
//!   rules.
//! - Projections (`WITH`, `RETURN`) group, aggregate, sort and page here.
//! - Writes ([`write`]) lock endpoints, insert, update and delete through
//!   the store, inside the caller's transaction.
//!
//! Budgets (`graph.max_hops`, `max_frontier`, `max_rows`, `max_time_ms`,
//! `max_memory_mb`) are checked after every step; past one, the statement
//! fails with 39903 naming it, never with a truncated answer.

mod late;
mod matcher;
mod stream;
mod write;
pub(crate) use write::is_duplicate;

pub use stream::{RowSink, VecSink, run_stream, streamable};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_common::{Result, SprigCode, SprigError};

use crate::ast::{
    BinOp, Clause, Explain, Expr, MapItem, PathPattern, Projection, Query, SubqueryKind,
};
use crate::eval::{Params, Row, eval};
use crate::store::{EdgeRow, GraphStore, NodeRow};
use crate::value::{Key, Node, Rel, Value, props_from_json};
use sprig_router::graph_route::CmpOp;

/// The traversal budgets of one statement.
#[derive(Debug, Clone)]
pub struct Budgets {
    /// `graph.max_hops`.
    pub max_hops: u32,
    /// `graph.max_frontier`.
    pub max_frontier: usize,
    /// `graph.max_rows`.
    pub max_rows: usize,
    /// `graph.max_time_ms`.
    pub max_time: Duration,
    /// `graph.max_memory_mb`, in bytes.
    pub max_memory: usize,
    /// `graph.batch_nodes`.
    pub batch_nodes: usize,
    /// `graph.local_burst_hops`.
    pub local_burst_hops: u32,
    /// `graph.max_detach_edges`.
    pub max_detach_edges: usize,
    /// Not a budget: the Bolt transaction's `tx_metadata`, which
    /// `CALL tx.getMetaData()` returns (an empty map when there is none).
    pub tx_metadata: Option<BTreeMap<String, Value>>,
}

impl Default for Budgets {
    fn default() -> Self {
        Self {
            max_hops: 15,
            max_frontier: 100_000,
            max_rows: 1_000_000,
            max_time: Duration::from_millis(30_000),
            max_memory: 256 << 20,
            batch_nodes: 1000,
            local_burst_hops: 4,
            max_detach_edges: 100_000,
            tx_metadata: None,
        }
    }
}

/// A statement's result.
#[derive(Debug, Clone, Default)]
pub struct Output {
    /// Column names.
    pub columns: Vec<String>,
    /// Rows.
    pub rows: Vec<Vec<Value>>,
    /// Neo4j's update counters (`nodes-created`, ...), non-zero only.
    pub stats: BTreeMap<String, i64>,
    /// Nodes removed with a plain `DELETE`: the commit checks they have no
    /// relationships left (39906).
    pub deleted_nodes: Vec<u64>,
    /// `r`, `w`, `rw` or `s`.
    pub qtype: &'static str,
    /// `EXPLAIN` / `PROFILE`: the steps, with their tier.
    pub plan: Option<Vec<String>>,
    /// `plan` is `PROFILE`'s (the statement ran).
    pub profiled: bool,
    /// Neo4j's notifications for the statement (see [`notifications`]).
    pub notifications: Vec<Notification>,
    /// A read's final `RETURN` failed on a row: `rows` are
    /// the rows before it, and the error comes after them, as Neo4j's lazy
    /// results give it (on `PULL`, after the first records).
    pub late_error: Option<Arc<SprigError>>,
}

/// A Neo4j notification (Bolt `notifications` in the summary).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    /// `Neo.ClientNotification...`.
    pub code: &'static str,
    /// The title.
    pub title: &'static str,
    /// The description.
    pub description: String,
    /// `INFORMATION` or `WARNING`.
    pub severity: &'static str,
    /// `PERFORMANCE`, ...
    pub category: &'static str,
    /// Its GQLSTATUS (Bolt 5.5 on: `statuses` in place of
    /// `notifications`).
    pub gql_status: &'static str,
    /// The GQL status description.
    pub status_description: String,
}

/// The notifications SprigDB gives for a statement. Only Neo4j's
/// `CartesianProduct` for now: one `MATCH` with patterns that share no
/// variable, directly or through variables bound by earlier clauses (the
/// drivers' testkit checks it on `EXPLAIN MATCH (n), (m) RETURN n, m`).
#[must_use]
pub fn notifications(q: &Query) -> Vec<Notification> {
    let mut out = Vec::new();
    for part in &q.parts {
        cartesian_products(&part.clauses, &mut out);
    }
    out
}

fn uf_root(p: &mut [usize], mut i: usize) -> usize {
    while p[i] != i {
        p[i] = p[p[i]];
        i = p[i];
    }
    i
}

fn cartesian_products(clauses: &[Clause], out: &mut Vec<Notification>) {
    let mut bound: std::collections::HashSet<String> = std::collections::HashSet::new();
    for c in clauses {
        match c {
            Clause::Match { patterns, .. } => {
                // Union-find over the patterns; index `patterns.len()`
                // stands for the rows bound before this clause.
                let n = patterns.len();
                let mut parent: Vec<usize> = (0..=n).collect();
                let vars: Vec<Vec<String>> = patterns.iter().map(pattern_vars).collect();
                for i in 0..n {
                    if vars[i].iter().any(|v| bound.contains(v)) {
                        let (a, b) = (uf_root(&mut parent, i), uf_root(&mut parent, n));
                        parent[a] = b;
                    }
                    for j in 0..i {
                        if vars[i].iter().any(|v| vars[j].contains(v)) {
                            let (a, b) = (uf_root(&mut parent, i), uf_root(&mut parent, j));
                            parent[a] = b;
                        }
                    }
                }
                let touches_bound =
                    (0..n).any(|i| uf_root(&mut parent, i) == uf_root(&mut parent, n));
                let mut roots: Vec<usize> = Vec::new();
                for i in 0..n {
                    let r = uf_root(&mut parent, i);
                    if !roots.contains(&r) {
                        roots.push(r);
                    }
                }
                let parts = roots.len() + usize::from(!bound.is_empty() && !touches_bound);
                if parts > 1 && n > 1 {
                    // The identifiers of every part but the first.
                    let first = uf_root(&mut parent, 0);
                    let mut ids: Vec<String> = Vec::new();
                    for (i, vs) in vars.iter().enumerate() {
                        if uf_root(&mut parent, i) != first {
                            for v in vs {
                                if !ids.contains(v) {
                                    ids.push(v.clone());
                                }
                            }
                        }
                    }
                    out.push(Notification {
                        code: "Neo.ClientNotification.Statement.CartesianProduct",
                        title: "This query builds a cartesian product between disconnected patterns.",
                        description: format!(
                            "If a part of a query contains multiple disconnected patterns, this will build a cartesian product between all those parts. This may produce a large amount of data and slow down query processing. While occasionally intended, it may often be possible to reformulate the query that avoids the use of this cross product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH (identifier is: ({}))",
                            ids.join(", ")
                        ),
                        severity: "INFORMATION",
                        category: "PERFORMANCE",
                        gql_status: "03N90",
                        status_description: format!(
                            "info: cartesian product. The disconnected pattern '{}' builds a cartesian product. A cartesian product may produce a large amount of data and slow down query processing.",
                            vars.iter()
                                .map(|vs| format!("({})", vs.join(", ")))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    });
                }
                bound.extend(vars.into_iter().flatten());
            }
            Clause::Unwind { alias, .. } => {
                bound.insert(alias.clone());
            }
            Clause::With(p) => {
                if !p.star {
                    bound.clear();
                }
                bound.extend(p.items.iter().map(|(_, a)| a.clone()));
            }
            Clause::Create(ps) => {
                for p in ps {
                    bound.extend(pattern_vars(p));
                }
            }
            Clause::Merge { pattern, .. } => bound.extend(pattern_vars(pattern)),
            Clause::Call {
                yields: Some(y), ..
            } => bound.extend(y.iter().map(|(_, a)| a.clone())),
            // Names this check doesn't follow: stop rather than guess.
            Clause::Call { .. } | Clause::Subquery { .. } => return,
            _ => {}
        }
    }
}

/// A 39903 budget error, counted by reason.
#[must_use]
pub fn budget_error(reason: &'static str, detail: impl std::fmt::Display) -> SprigError {
    metrics::counter!("sprig_graph_budget_exceeded_total", "reason" => reason).increment(1);
    SprigError::new(
        SprigCode::GraphBudgetExceeded,
        format!("graph budget exceeded ({reason}): {detail}"),
    )
}

/// Runs `q` over `store`.
///
/// # Errors
/// 39900–39909 for graph statement errors; the store's errors.
pub async fn run(
    store: &mut dyn GraphStore,
    q: &Query,
    params: &Params,
    budgets: &Budgets,
) -> Result<Output> {
    run_with(store, q, params, budgets, false).await
}

/// [`run`], but a read's top `RETURN` that fails on a row answers the
/// rows before it with the error in [`Output::late_error`] (Bolt: records,
/// then the failure on `PULL`, as Neo4j's lazy results).
///
/// # Errors
/// As [`run`].
pub async fn run_lazy(
    store: &mut dyn GraphStore,
    q: &Query,
    params: &Params,
    budgets: &Budgets,
) -> Result<Output> {
    run_with(store, q, params, budgets, true).await
}

async fn run_with(
    store: &mut dyn GraphStore,
    q: &Query,
    params: &Params,
    budgets: &Budgets,
    lazy: bool,
) -> Result<Output> {
    let store_late = store.late_props();
    let mut ex = Exec {
        store,
        params,
        b: budgets,
        started: Instant::now(),
        stats: BTreeMap::new(),
        deleted: Vec::new(),
        wrote: false,
        plan: Vec::new(),
        slot: 0,
        star_in_order: false,
        late_ok: lazy && q.explain == Explain::None && q.parts.len() == 1 && !has_writes(q),
        returning: false,
        late_error: None,
        late: ex_late(store_late, q),
        seen_nodes: HashMap::new(),
        seen_rels: HashMap::new(),
        prefetch: HashMap::new(),
        push: HashMap::new(),
        only: HashMap::new(),
        profile: q.explain == Explain::Profile,
        prune: None,
    };
    if q.explain == Explain::Explain {
        let mut plan = explain(q);
        plan.extend(ex.store.explain_routes());
        return Ok(Output {
            qtype: if has_writes(q) { "rw" } else { "r" },
            plan: Some(plan),
            notifications: notifications(q),
            ..Output::default()
        });
    }
    let inits = vec![Row::new(); q.parts.len()];
    let (columns, all) = ex.union_parts(q, inits, &[]).await?;
    let schema = q
        .parts
        .iter()
        .any(|p| p.clauses.iter().any(|c| matches!(c, Clause::Schema(_))));
    let qtype = match (ex.wrote || has_writes(q), columns.is_empty()) {
        _ if schema => "s",
        (true, true) => "w",
        (true, false) => "rw",
        (false, _) => "r",
    };
    let plan = (q.explain == Explain::Profile).then(|| {
        let mut p = ex.plan.clone();
        let s = ex.store.stats();
        p.push(format!(
            "shard statements: {}, of which recursive CTEs: {}",
            s.shard_queries, s.cte_queries
        ));
        p.push(format!(
            "time: {} ms in all, {} ms waiting for {} shard rows",
            ex.started.elapsed().as_millis(),
            s.sql_us / 1000,
            s.sql_rows
        ));
        p.extend(ex.store.routes());
        p
    });
    Ok(Output {
        columns,
        rows: all,
        stats: ex.stats,
        deleted_nodes: ex.deleted,
        qtype,
        plan,
        profiled: q.explain == Explain::Profile,
        notifications: notifications(q),
        late_error: ex.late_error.take().map(Arc::new),
    })
}

/// The numeric comparisons of `where_`'s conjuncts a node read may
/// filter on: `v.k op c` (either way round) with `c` a constant or
/// parameter that is an integer below 2^52 in size or a finite float.
fn pushed(where_: &Expr, params: &Params) -> HashMap<String, Vec<(String, CmpOp, f64)>> {
    let mut out: HashMap<String, Vec<(String, CmpOp, f64)>> = HashMap::new();
    for c in matcher::conjuncts(where_) {
        let Expr::Binary(op, a, b) = c else { continue };
        let op = match op {
            BinOp::Lt => CmpOp::Lt,
            BinOp::Le => CmpOp::Le,
            BinOp::Gt => CmpOp::Gt,
            BinOp::Ge => CmpOp::Ge,
            BinOp::Eq => CmpOp::Eq,
            _ => continue,
        };
        let flip = |o: CmpOp| match o {
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::Le => CmpOp::Ge,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::Ge => CmpOp::Le,
            CmpOp::Eq => CmpOp::Eq,
            CmpOp::NotNumBelow => CmpOp::NotNumAbove,
            CmpOp::NotNumAbove => CmpOp::NotNumBelow,
        };
        let (v, k, e, op) = match (&**a, &**b) {
            (Expr::Prop(t, k), e) if matches!(&**t, Expr::Var(_)) => (t, k, e, op),
            (e, Expr::Prop(t, k)) if matches!(&**t, Expr::Var(_)) => (t, k, e, flip(op)),
            _ => continue,
        };
        let Expr::Var(v) = &**v else { continue };
        if !matches!(e, Expr::Lit(_) | Expr::Param(_)) {
            continue;
        }
        let x = match eval(e, &Row::new(), params) {
            Ok(Value::Int(i)) if i.unsigned_abs() < (1u64 << 52) => {
                #[allow(clippy::cast_precision_loss)] // below 2^52: exact
                let f = i as f64;
                f
            }
            Ok(Value::Float(f)) if f.is_finite() => f,
            _ => continue,
        };
        out.entry(v.clone()).or_default().push((k.clone(), op, x));
    }
    out
}

/// [`Exec::prune`] for projection `p` after a `MATCH` whose incoming rows
/// look like `first`: when every sort key, aliases replaced by their
/// items, reads the same one variable the rows don't bind yet.
fn prune_of(p: &Projection, first: Option<&Row>) -> Option<Prune> {
    let mut var: Option<String> = None;
    let mut keys = Vec::new();
    for (e, asc) in &p.order {
        let e = match e {
            Expr::Var(v) => p
                .items
                .iter()
                .find(|(_, a)| a == v)
                .map_or_else(|| e.clone(), |(ie, _)| ie.clone()),
            other => other.clone(),
        };
        let mut ok = true;
        e.walk(&mut |x| match x {
            Expr::Var(v) => {
                if p.items.iter().any(|(_, a)| a == v) && !matches!(&e, Expr::Var(_)) {
                    ok = false;
                }
                match &var {
                    None => var = Some(v.clone()),
                    Some(w) if w == v => {}
                    Some(_) => ok = false,
                }
            }
            Expr::Pattern(_) | Expr::Subquery(..) | Expr::Slot(_) => ok = false,
            _ => {}
        });
        if !ok || e.has_aggregate() {
            return None;
        }
        keys.push((e, *asc));
    }
    let var = var?;
    if first.is_some_and(|r| r.contains_key(&var)) {
        return None;
    }
    Some(Prune {
        var,
        keys,
        bound: None,
    })
}

/// Incoming rows per chunk of a `MATCH` that keeps only its best rows
/// ([`Exec::match_top`]).
const MATCH_CHUNK: usize = 128;

/// What a `MATCH` with `where_`, followed by `next`, is known to read of
/// the nodes it binds: its `WHERE`'s needs, and the next projection's sort
/// keys (through the items they name by alias) and `WHERE` (a `WITH`'s).
fn prefetch_hint(where_: Option<&Expr>, next: Option<&Clause>) -> HashMap<String, late::Need> {
    let mut need = HashMap::new();
    if let Some(w) = where_ {
        late::needs(w, true, &mut need);
    }
    // A projection without LIMIT evaluates its items for every row: what
    // they read is read with the nodes (a RETURN reads whole what it
    // returns whole).
    if let Some(c @ (Clause::With(p) | Clause::Return(p))) = next
        && p.limit.is_none()
    {
        for (e, _) in &p.items {
            late::needs(e, true, &mut need);
            if let (Clause::Return(_), Expr::Var(v)) = (c, e) {
                late::add(&mut need, v, late::Need::All);
            }
        }
        if let Some(w) = &p.where_ {
            late::needs(w, true, &mut need);
        }
    }
    if let Some(Clause::With(p) | Clause::Return(p)) = next
        && !p.order.is_empty()
    {
        let order: Vec<&Expr> = p.order.iter().map(|(e, _)| e).collect();
        let on = late::needs_of(&order);
        for (v, n) in &on {
            match p.items.iter().find(|(_, a)| a == v) {
                Some((Expr::Var(x), _)) => late::add(&mut need, x, n.clone()),
                Some((e, _)) => late::needs(e, false, &mut need),
                None => late::add(&mut need, v, n.clone()),
            }
        }
        for (e, _) in &p.order {
            if let Expr::Var(v) = e
                && let Some((ie, _)) = p.items.iter().find(|(_, a)| a == v)
            {
                late::needs(ie, true, &mut need);
            }
        }
    }
    need
}

/// Whether `q` reads properties late: a read-only statement on a store
/// that allows it.
pub(crate) fn ex_late(store_late: bool, q: &Query) -> bool {
    store_late && !has_writes(q)
}

/// Whether a statement writes (or changes the schema).
#[must_use]
pub fn has_writes(q: &Query) -> bool {
    q.parts.iter().any(|p| clauses_write(&p.clauses))
}

pub(crate) fn clauses_write(cs: &[Clause]) -> bool {
    cs.iter().any(|c| match c {
        Clause::Create(_)
        | Clause::Merge { .. }
        | Clause::Set(_)
        | Clause::Remove(_)
        | Clause::Delete { .. }
        | Clause::Foreach { .. }
        | Clause::Schema(_) => true,
        Clause::Subquery { query, .. } => has_writes(query),
        _ => false,
    })
}

/// The plan `EXPLAIN` shows: one line per clause and hop, with its tier
/// (`EXPLAIN FORMAT=SPRIG` of `GRAPH_TABLE` shows them too).
#[must_use]
pub fn explain(q: &Query) -> Vec<String> {
    let mut out = Vec::new();
    for part in &q.parts {
        for c in &part.clauses {
            match c {
                Clause::Match {
                    optional, patterns, ..
                } => {
                    for p in patterns {
                        out.push(format!(
                            "{}Match: start {} then {} hop(s)",
                            if *optional { "Optional" } else { "" },
                            p.start.var.as_deref().unwrap_or("(anonymous)"),
                            p.steps.len()
                        ));
                        for (r, _) in &p.steps {
                            out.push(match r.range {
                                None => "  Expand: tier join (one shard statement per batch)".into(),
                                Some(_) => "  VarExpand: tier cte (bursts of graph.local_burst_hops), gateway across shards".into(),
                            });
                        }
                    }
                }
                Clause::Return(_) => out.push("Return".into()),
                Clause::With(_) => out.push("With".into()),
                Clause::Unwind { .. } => out.push("Unwind".into()),
                Clause::Create(_) => out.push("Create".into()),
                Clause::Merge { .. } => out.push("Merge".into()),
                Clause::Set(_) => out.push("Set".into()),
                Clause::Remove(_) => out.push("Remove".into()),
                Clause::Delete { .. } => out.push("Delete".into()),
                Clause::Call { name, .. } => out.push(format!("ProcedureCall {name}")),
                Clause::Subquery { batch: None, .. } => {
                    out.push("CallSubquery: once per row".into());
                }
                Clause::Subquery { batch: Some(_), .. } => out.push(
                    "CallSubqueryInTransactions: once per row, committed per batch of rows".into(),
                ),
                Clause::Foreach { .. } => out.push("Foreach".into()),
                Clause::Schema(_) => out.push("SchemaCommand".into()),
            }
        }
    }
    out
}

/// A pattern predicate or expression subquery lifted out of an
/// expression.
enum Lifted {
    Pattern(PathPattern),
    Subquery(SubqueryKind, Query),
    /// A list comprehension, quantifier or `reduce` with a pattern
    /// predicate or subquery inside, which depends on its own variable
    /// (LDBC IC10 and IC14).
    Deep(Expr),
    /// `startNode(r)` (`true`) or `endNode(r)`: the node, read from the
    /// store (LDBC IC14).
    EndNode(Expr, bool),
}

/// A pattern comprehension's parts, as the parser writes it (one `MATCH`
/// and a `RETURN` of one item named ` pc`): its patterns, `WHERE` and map.
fn comprehension(q: &Query) -> Option<(&[PathPattern], Option<&Expr>, &Expr)> {
    let [part] = q.parts.as_slice() else {
        return None;
    };
    let [
        Clause::Match {
            optional: false,
            patterns,
            where_,
        },
        Clause::Return(p),
    ] = part.clauses.as_slice()
    else {
        return None;
    };
    let [(map, name)] = p.items.as_slice() else {
        return None;
    };
    (name == " pc"
        && !p.distinct
        && !p.star
        && p.order.is_empty()
        && p.skip.is_none()
        && p.limit.is_none()
        && p.where_.is_none()
        && q.union_all.is_empty())
    .then_some((patterns.as_slice(), where_.as_ref(), map))
}

/// Whether `e` has a pattern predicate, an expression subquery, or
/// `startNode()`/`endNode()`, which need the store.
fn has_deep(e: &Expr) -> bool {
    let mut found = false;
    e.walk(&mut |x| {
        found |= matches!(x, Expr::Pattern(_) | Expr::Subquery(..))
            || matches!(x, Expr::Func { name, args, .. }
                if args.len() == 1 && matches!(name.as_str(), "startnode" | "endnode"));
    });
    found
}

/// A procedure's columns, and its rows by input row.
type CallRows = (Vec<String>, Vec<(usize, Vec<Value>)>);

/// Hidden row keys start with a space, so no variable can clash.
pub(crate) const RELS: &str = " rels";

// Flags of one statement's run, each independent.
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct Exec<'a> {
    pub(crate) store: &'a mut dyn GraphStore,
    pub(crate) params: &'a Params,
    pub(crate) b: &'a Budgets,
    started: Instant,
    pub(crate) stats: BTreeMap<String, i64>,
    pub(crate) deleted: Vec<u64>,
    pub(crate) wrote: bool,
    pub(crate) plan: Vec<String>,
    slot: usize,
    /// `RETURN *` after `SHOW ... YIELD` keeps the yield order (Neo4j).
    star_in_order: bool,
    /// The statement is a read whose top `RETURN` may fail late
    /// ([`Output::late_error`]).
    late_ok: bool,
    /// The projection running is that `RETURN`.
    returning: bool,
    /// The error it met.
    late_error: Option<SprigError>,
    /// Hops read no properties; clauses read what they need
    /// ([`late`]).
    pub(crate) late: bool,
    /// The best-read copy of each node and relationship read late.
    pub(crate) seen_nodes: HashMap<u64, Arc<Node>>,
    pub(crate) seen_rels: HashMap<u64, Arc<Rel>>,
    /// While a `MATCH` runs: what its `WHERE` and the next projection's
    /// sort keys read of its variables, read with the nodes it binds.
    pub(crate) prefetch: HashMap<String, late::Need>,
    /// While a `MATCH` runs: numeric comparisons its `WHERE` makes of its
    /// variables' properties, pushed into the reads of the nodes it binds.
    pub(crate) push: HashMap<String, Vec<(String, CmpOp, f64)>>,
    /// While a `MATCH` runs: the only nodes a variable it binds may be
    /// (its `WHERE`'s `v IN list` over every incoming row's list), sorted.
    pub(crate) only: HashMap<String, Vec<u64>>,
    /// `PROFILE`: each clause's time and rows go into the plan.
    pub(crate) profile: bool,
    /// While a `MATCH` keeps only its best rows ([`Exec::match_top`]):
    /// hops that bind the one variable the sort keys read skip far nodes
    /// already worse than the last row kept.
    pub(crate) prune: Option<Prune>,
}

/// [`Exec::prune`]: the variable, the sort keys over it alone with their
/// directions, and the keys of the last row kept so far.
pub(crate) struct Prune {
    pub(crate) var: String,
    pub(crate) keys: Vec<(Expr, bool)>,
    pub(crate) bound: Option<Vec<Value>>,
}

impl Prune {
    /// Whether `ks` sorts strictly after the bound (so its row can't be
    /// kept).
    pub(crate) fn beaten(&self, ks: &[Value]) -> bool {
        let Some(b) = &self.bound else { return false };
        for (i, (_, asc)) in self.keys.iter().enumerate() {
            let o = ks[i].order(&b[i]);
            let o = if *asc { o } else { o.reverse() };
            if o != std::cmp::Ordering::Equal {
                return o == std::cmp::Ordering::Greater;
            }
        }
        false
    }
}

/// The variables of `row` that none of `rest` names: what
/// the rest of the query part never reads, so rows needn't carry it. A
/// name counts as used when it appears anywhere in the clauses' syntax
/// tree (as a variable, a pattern's name, or even a string), and nothing
/// is dropped before a `*` projection.
fn dead_vars(row: Option<&Row>, rest: &[Clause]) -> Vec<String> {
    let Some(row) = row else { return Vec::new() };
    let text = format!("{rest:?}");
    if text.contains("star: true") {
        return Vec::new();
    }
    row.keys()
        .filter(|k| !k.starts_with(' ') && !text.contains(&format!("{k:?}")))
        .cloned()
        .collect()
}

/// Whether duplicates of rows coming out of a `MATCH` can't change the
/// result: the next clause is `WITH`/`RETURN
/// DISTINCT`, or aggregates only with duplicate-insensitive aggregates.
fn dup_insensitive(rest: &[Clause]) -> bool {
    let Some(Clause::With(p) | Clause::Return(p)) = rest.first() else {
        return false;
    };
    if p.distinct && !p.items.iter().any(|(e, _)| e.has_aggregate()) {
        return true;
    }
    let mut ok = true;
    let mut any = false;
    for (e, _) in &p.items {
        e.walk(&mut |x| match x {
            Expr::CountStar => ok = false,
            Expr::Func { name, distinct, .. } if crate::ast::is_aggregate(name) => {
                any = true;
                if !(*distinct || name == "min" || name == "max") {
                    ok = false;
                }
            }
            _ => {}
        });
    }
    any && ok
}

impl Exec<'_> {
    pub(crate) fn check_time(&self) -> Result<()> {
        if self.started.elapsed() > self.b.max_time {
            return Err(budget_error(
                "time",
                format!("graph.max_time_ms = {}", self.b.max_time.as_millis()),
            ));
        }
        Ok(())
    }

    /// `PROFILE`'s line for clause `prev` (its index, start and SQL time
    /// then), which left `rows` rows.
    fn clause_line(&mut self, prev: Option<(usize, Instant, u64)>, rows: usize) {
        if let Some((k, t, sql)) = prev {
            let line = format!(
                "Clause {k}: {rows} rows, {} ms, of which {} ms SQL",
                t.elapsed().as_millis(),
                self.store.stats().sql_us.saturating_sub(sql) / 1000
            );
            self.plan.push(line);
        }
    }

    pub(crate) fn check_rows(&self, n: usize) -> Result<()> {
        if n > self.b.max_rows {
            return Err(budget_error(
                "rows",
                format!("more than graph.max_rows = {} rows", self.b.max_rows),
            ));
        }
        self.check_time()
    }

    /// Rough memory of a binding table: 96 bytes a value.
    /// `graph.max_memory_mb` over `rows`: each row's slots and what its
    /// values hold ([`crate::value::approx_bytes`]), from a sample of up to
    /// 64 rows spread over them (before, 96
    /// bytes a slot, which missed the elements' properties, so one LDBC
    /// statement could hold a gigabyte).
    pub(crate) fn check_memory(&self, rows: &[Row]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let step = rows.len().div_ceil(64);
        let (mut sampled, mut bytes) = (0usize, 0usize);
        for r in rows.iter().step_by(step) {
            sampled += 1;
            bytes += 48
                + r.iter()
                    .map(|(k, v)| 64 + k.len() + crate::value::approx_bytes(v))
                    .sum::<usize>();
        }
        let estimate = (bytes / sampled.max(1)).saturating_mul(rows.len());
        if estimate > self.b.max_memory {
            return Err(budget_error(
                "memory",
                format!("graph.max_memory_mb = {}", self.b.max_memory >> 20),
            ));
        }
        Ok(())
    }

    pub(crate) fn bump(&mut self, key: &str, n: i64) {
        if n != 0 {
            *self.stats.entry(key.to_owned()).or_default() += n;
        }
    }

    pub(crate) fn node_val(&self, r: &NodeRow) -> Arc<Node> {
        Arc::new(Node {
            graph: self.store.graph_id(),
            id: r.id,
            rk: r.rk,
            labels: r
                .labels
                .iter()
                .map(|l| self.store.label_name(*l).unwrap_or_else(|| format!("#{l}")))
                .collect(),
            props: props_from_json(&r.props, r.types.as_ref()),
            partial: None,
        })
    }

    pub(crate) fn rel_val(&self, e: &EdgeRow) -> Arc<Rel> {
        Arc::new(Rel {
            graph: self.store.graph_id(),
            id: e.id,
            src: e.src,
            dst: e.dst,
            src_rk: e.src_rk,
            dst_rk: e.dst_rk,
            rtype: self
                .store
                .type_name(e.rtype)
                .unwrap_or_else(|| format!("#{}", e.rtype)),
            props: props_from_json(&e.props, e.types.as_ref()),
            partial: None,
        })
    }

    fn next_slot(&mut self) -> String {
        self.slot += 1;
        format!(" pp{}", self.slot)
    }

    /// Replaces pattern predicates and subqueries in `e` by slots,
    /// returning them.
    fn lift_patterns(&mut self, e: &Expr, out: &mut Vec<(String, Lifted)>) -> Expr {
        match e {
            Expr::Pattern(p) => {
                let s = self.next_slot();
                out.push((s.clone(), Lifted::Pattern((**p).clone())));
                Expr::Slot(s)
            }
            Expr::Subquery(k, q) => {
                let s = self.next_slot();
                out.push((s.clone(), Lifted::Subquery(*k, (**q).clone())));
                Expr::Slot(s)
            }
            Expr::Prop(x, k) => Expr::Prop(Box::new(self.lift_patterns(x, out)), k.clone()),
            Expr::IsNull(x, n) => Expr::IsNull(Box::new(self.lift_patterns(x, out)), *n),
            Expr::HasLabels(x, l) => {
                Expr::HasLabels(Box::new(self.lift_patterns(x, out)), l.clone())
            }
            Expr::Index(a, b) => Expr::Index(
                Box::new(self.lift_patterns(a, out)),
                Box::new(self.lift_patterns(b, out)),
            ),
            Expr::Map(m) => Expr::Map(
                m.iter()
                    .map(|(k, x)| (k.clone(), self.lift_patterns(x, out)))
                    .collect(),
            ),
            Expr::MapProjection(b, items) => Expr::MapProjection(
                b.clone(),
                items
                    .iter()
                    .map(|i| match i {
                        MapItem::Lit(k, x) => MapItem::Lit(k.clone(), self.lift_patterns(x, out)),
                        other => other.clone(),
                    })
                    .collect(),
            ),
            Expr::Unary(op, x) => Expr::Unary(*op, Box::new(self.lift_patterns(x, out))),
            Expr::Binary(op, a, b) => Expr::Binary(
                *op,
                Box::new(self.lift_patterns(a, out)),
                Box::new(self.lift_patterns(b, out)),
            ),
            Expr::Func { name, args, .. }
                if args.len() == 1 && matches!(name.as_str(), "startnode" | "endnode") =>
            {
                let s = self.next_slot();
                out.push((
                    s.clone(),
                    Lifted::EndNode(args[0].clone(), name == "startnode"),
                ));
                Expr::Slot(s)
            }
            Expr::Func {
                name,
                distinct,
                args,
            } => Expr::Func {
                name: name.clone(),
                distinct: *distinct,
                args: args.iter().map(|a| self.lift_patterns(a, out)).collect(),
            },
            Expr::Case {
                subject,
                whens,
                else_,
            } => Expr::Case {
                subject: subject
                    .as_ref()
                    .map(|s| Box::new(self.lift_patterns(s, out))),
                whens: whens
                    .iter()
                    .map(|(a, b)| (self.lift_patterns(a, out), self.lift_patterns(b, out)))
                    .collect(),
                else_: else_.as_ref().map(|s| Box::new(self.lift_patterns(s, out))),
            },
            Expr::List(l) => Expr::List(l.iter().map(|a| self.lift_patterns(a, out)).collect()),
            Expr::ListComp { .. } | Expr::Quantifier { .. } | Expr::Reduce { .. }
                if has_deep(e) =>
            {
                let s = self.next_slot();
                out.push((s.clone(), Lifted::Deep(e.clone())));
                Expr::Slot(s)
            }
            other => other.clone(),
        }
    }

    /// The values of `e` for each of `rows`, pattern predicates and
    /// subqueries in it resolved first: a pattern predicate once for all
    /// rows, a comprehension over them once for all the rows' elements.
    /// Slots are written into `rows` and removed again.
    pub(crate) async fn eval_rows(&mut self, e: &Expr, rows: &mut [Row]) -> Result<Vec<Value>> {
        self.late(rows, &[e]).await?;
        let mut pats = Vec::new();
        let lifted = if has_deep(e) {
            self.lift_patterns(e, &mut pats)
        } else {
            e.clone()
        };
        for (slot, l) in &pats {
            let vals = Box::pin(self.lifted_values(l, rows)).await?;
            for (r, v) in rows.iter_mut().zip(vals) {
                r.insert(slot.clone(), v);
            }
        }
        let mut out = Vec::with_capacity(rows.len());
        for r in rows.iter_mut() {
            tokio::task::consume_budget().await;
            out.push(eval(&lifted, r, self.params)?);
            for (s, _) in &pats {
                r.remove(s);
            }
        }
        Ok(out)
    }

    /// One lifted predicate's or subquery's value for each of `rows`.
    async fn lifted_values(&mut self, l: &Lifted, rows: &mut [Row]) -> Result<Vec<Value>> {
        // Patterns inside an expression are matches of their own: the
        // enclosing MATCH's hints don't apply to them.
        let prefetch = std::mem::take(&mut self.prefetch);
        let push = std::mem::take(&mut self.push);
        let only = std::mem::take(&mut self.only);
        let prune = self.prune.take();
        let r = Box::pin(self.lifted_values_in(l, rows)).await;
        self.prefetch = prefetch;
        self.push = push;
        self.only = only;
        self.prune = prune;
        r
    }

    async fn lifted_values_in(&mut self, l: &Lifted, rows: &mut [Row]) -> Result<Vec<Value>> {
        let elements = |rows: &[Row],
                        lists: Vec<Value>,
                        var: &str|
         -> Result<(Vec<Row>, Vec<usize>, Vec<bool>)> {
            let (mut inner, mut owner, mut null) =
                (Vec::new(), Vec::new(), vec![false; rows.len()]);
            for (i, (r, l)) in rows.iter().zip(lists).enumerate() {
                match l {
                    Value::Null => null[i] = true,
                    Value::List(items) => {
                        for x in items {
                            let mut ir = r.clone();
                            ir.insert(var.to_owned(), x);
                            inner.push(ir);
                            owner.push(i);
                        }
                    }
                    other => {
                        return Err(SprigError::new(
                            SprigCode::GraphPropertyType,
                            format!("Type mismatch: IN over a {}", other.type_name()),
                        ));
                    }
                }
            }
            Ok((inner, owner, null))
        };
        Ok(match l {
            Lifted::Pattern(p) => {
                let hits = self.pattern_hits(rows, p).await?;
                (0..rows.len())
                    .map(|i| Value::Bool(hits.contains(&i)))
                    .collect()
            }
            Lifted::Subquery(SubqueryKind::Collect, q) if comprehension(q).is_some() => {
                // A pattern comprehension for every row at once (LDBC
                // IC14): one match over all rows, each tagged, then
                // the map expression and the results grouped back.
                let Some((patterns, where_, map)) = comprehension(q) else {
                    return Ok(Vec::new());
                };
                if rows.is_empty() {
                    return Ok(Vec::new());
                }
                // The outer variables the comprehension reads: rows equal
                // on them get the same list, so it runs once per distinct
                // combination (LDBC IC14 ran it for every relationship of
                // every shortest path, the same relationships many times).
                let mut used: Vec<String> = Vec::new();
                {
                    let mut names: Vec<String> = patterns.iter().flat_map(pattern_vars).collect();
                    let mut take = |e: &Expr| {
                        e.walk(&mut |x| {
                            if let Expr::Var(v) = x {
                                names.push(v.clone());
                            }
                        });
                    };
                    if let Some(w) = where_ {
                        take(w);
                    }
                    take(map);
                    for p in patterns {
                        for np in std::iter::once(&p.start).chain(p.steps.iter().map(|(_, n)| n)) {
                            if let Some(pe) = &np.props {
                                take(pe);
                            }
                        }
                        for (rp, _) in &p.steps {
                            if let Some(pe) = &rp.props {
                                take(pe);
                            }
                        }
                    }
                    for n in names {
                        if !n.starts_with(' ') && rows[0].contains_key(&n) && !used.contains(&n) {
                            used.push(n);
                        }
                    }
                }
                let mut group_of: Vec<usize> = Vec::with_capacity(rows.len());
                let mut firsts: Vec<usize> = Vec::new();
                let mut seen: HashMap<Vec<Key>, usize> = HashMap::new();
                for (i, r) in rows.iter().enumerate() {
                    let k: Vec<Key> = used
                        .iter()
                        .map(|v| r.get(v).map_or(Key::Null, Value::key))
                        .collect();
                    let g = *seen.entry(k).or_insert_with(|| {
                        firsts.push(i);
                        firsts.len() - 1
                    });
                    group_of.push(g);
                }
                let mut tagged: Vec<Row> = firsts
                    .iter()
                    .enumerate()
                    .map(|(g, &i)| {
                        // Only what the comprehension reads: the rows it
                        // multiplies don't carry the outer row's paths and
                        // lists.
                        let mut t: Row = rows[i]
                            .iter()
                            .filter(|(k, _)| used.contains(k))
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        t.insert(" pcrow".into(), Value::Int(i64::try_from(g).unwrap_or(0)));
                        t
                    })
                    .collect();
                tagged =
                    Box::pin(self.match_clause(tagged, patterns, where_, false, false)).await?;
                let vals = self.eval_rows(map, &mut tagged).await?;
                let mut per_group: Vec<Vec<Value>> = vec![Vec::new(); firsts.len()];
                for (r, v) in tagged.iter().zip(vals) {
                    if let Some(Value::Int(i)) = r.get(" pcrow")
                        && let Some(o) = usize::try_from(*i).ok().and_then(|i| per_group.get_mut(i))
                    {
                        o.push(v);
                    }
                }
                group_of
                    .into_iter()
                    .map(|g| Value::List(per_group[g].clone()))
                    .collect()
            }
            Lifted::Subquery(k, q) => {
                let mut v = Vec::with_capacity(rows.len());
                for r in rows.iter() {
                    v.push(self.subquery_value(*k, q, r).await?);
                }
                v
            }
            Lifted::Deep(Expr::ListComp {
                var,
                list,
                pred,
                map,
            }) => {
                let lists = self.eval_rows(list, rows).await?;
                let (mut inner, owner, null) = elements(rows, lists, var)?;
                let keep: Vec<bool> = match pred {
                    Some(p) => {
                        let mut keep = Vec::with_capacity(inner.len());
                        for v in self.eval_rows(p, &mut inner).await? {
                            keep.push(v.truth()? == Some(true));
                        }
                        keep
                    }
                    None => vec![true; inner.len()],
                };
                let (mut kept, mut kept_owner) = (Vec::new(), Vec::new());
                for ((r, o), k) in inner.into_iter().zip(owner).zip(keep) {
                    if k {
                        kept.push(r);
                        kept_owner.push(o);
                    }
                }
                let vals = match map {
                    Some(m) => self.eval_rows(m, &mut kept).await?,
                    None => kept
                        .iter_mut()
                        .map(|r| r.remove(var.as_str()).unwrap_or(Value::Null))
                        .collect(),
                };
                let mut out: Vec<Vec<Value>> = vec![Vec::new(); rows.len()];
                for (o, v) in kept_owner.into_iter().zip(vals) {
                    out[o].push(v);
                }
                out.into_iter()
                    .zip(null)
                    .map(|(l, n)| if n { Value::Null } else { Value::List(l) })
                    .collect()
            }
            Lifted::Deep(Expr::Quantifier {
                kind,
                var,
                list,
                pred,
            }) => {
                let lists = self.eval_rows(list, rows).await?;
                let (mut inner, owner, null) = elements(rows, lists, var)?;
                let truths = self.eval_rows(pred, &mut inner).await?;
                let mut per: Vec<Vec<Value>> = vec![Vec::new(); rows.len()];
                for (o, t) in owner.into_iter().zip(truths) {
                    per[o].push(t);
                }
                // The quantifier over the elements' truths.
                let mut out = Vec::with_capacity(rows.len());
                for (ts, n) in per.into_iter().zip(null) {
                    out.push(if n {
                        Value::Null
                    } else {
                        let q = Expr::Quantifier {
                            kind: kind.clone(),
                            var: " q".into(),
                            list: Box::new(Expr::Lit(Value::List(ts))),
                            pred: Box::new(Expr::Var(" q".into())),
                        };
                        eval(&q, &Row::new(), self.params)?
                    });
                }
                out
            }
            Lifted::Deep(Expr::Reduce {
                acc,
                init,
                var,
                list,
                expr,
            }) => {
                let inits = self.eval_rows(init, rows).await?;
                let lists = self.eval_rows(list, rows).await?;
                let mut out = Vec::with_capacity(rows.len());
                for ((r, a), l) in rows.iter().zip(inits).zip(lists) {
                    let items = match l {
                        Value::Null => {
                            out.push(Value::Null);
                            continue;
                        }
                        Value::List(items) => items,
                        other => {
                            return Err(SprigError::new(
                                SprigCode::GraphPropertyType,
                                format!("Type mismatch: IN over a {}", other.type_name()),
                            ));
                        }
                    };
                    let mut a = a;
                    for x in items {
                        let mut ir = [r.clone()];
                        ir[0].insert(acc.clone(), a);
                        ir[0].insert(var.clone(), x);
                        a = self
                            .eval_rows(expr, &mut ir)
                            .await?
                            .pop()
                            .unwrap_or(Value::Null);
                    }
                    out.push(a);
                }
                out
            }
            Lifted::EndNode(arg, start) => {
                let rels = self.eval_rows(arg, rows).await?;
                let mut ids: Vec<u64> = Vec::new();
                for r in &rels {
                    match r {
                        Value::Rel(r) => ids.push(if *start { r.src } else { r.dst }),
                        Value::Null => {}
                        other => {
                            return Err(SprigError::new(
                                SprigCode::GraphPropertyType,
                                format!(
                                    "Type mismatch: {}() of a {}",
                                    if *start { "startNode" } else { "endNode" },
                                    other.type_name()
                                ),
                            ));
                        }
                    }
                }
                ids.sort_unstable();
                ids.dedup();
                let found: HashMap<u64, Arc<Node>> = if ids.is_empty() {
                    HashMap::new()
                } else {
                    self.store
                        .nodes(&ids)
                        .await?
                        .iter()
                        .map(|n| (n.id, self.node_val(n)))
                        .collect()
                };
                rels.iter()
                    .map(|r| match r {
                        Value::Rel(r) => found
                            .get(&if *start { r.src } else { r.dst })
                            .map_or(Value::Null, |n| Value::Node(Arc::clone(n))),
                        _ => Value::Null,
                    })
                    .collect()
            }
            Lifted::Deep(other) => {
                let mut v = Vec::with_capacity(rows.len());
                for r in rows.iter() {
                    v.push(eval(other, r, self.params)?);
                }
                v
            }
        })
    }

    /// Evaluates a predicate with pattern predicates over `rows`, keeping
    /// the rows where it is true.
    pub(crate) async fn filter(&mut self, rows: Vec<Row>, pred: &Expr) -> Result<Vec<Row>> {
        let mut rows = rows;
        let vals = self.eval_rows(pred, &mut rows).await?;
        let mut out = Vec::with_capacity(rows.len());
        for (r, v) in rows.into_iter().zip(vals) {
            if v.truth()? == Some(true) {
                out.push(r);
            }
        }
        Ok(out)
    }

    /// Evaluates `e` over `row`, resolving pattern predicates in it first.
    pub(crate) async fn eval_full(&mut self, e: &Expr, row: &Row) -> Result<Value> {
        // Per-row work gives the runtime back now and then, so one heavy
        // statement never stalls the gateway's other tasks (its watchdog
        // aborts a runtime stuck for 2 s).
        tokio::task::consume_budget().await;
        if !has_deep(e) {
            return eval(e, row, self.params);
        }
        let mut one = [row.clone()];
        Ok(self
            .eval_rows(e, &mut one)
            .await?
            .pop()
            .unwrap_or(Value::Null))
    }

    /// The value of `EXISTS { }`, `COUNT { }` or `COLLECT { }` for `row`.
    async fn subquery_value(&mut self, k: SubqueryKind, q: &Query, row: &Row) -> Result<Value> {
        let mut q = q.clone();
        if k != SubqueryKind::Collect {
            for p in &mut q.parts {
                if !matches!(p.clauses.last(), Some(Clause::Return(_))) {
                    p.clauses.push(Clause::Return(Projection {
                        distinct: false,
                        star: false,
                        items: vec![(Expr::Lit(Value::Int(1)), " one".into())],
                        order: vec![],
                        skip: None,
                        limit: None,
                        where_: None,
                    }));
                }
            }
        }
        let scope: Vec<String> = row
            .keys()
            .filter(|k| !k.starts_with(' '))
            .cloned()
            .collect();
        let mut init = row.clone();
        init.retain(|k, _| !k.starts_with(' '));
        let inits = vec![init; q.parts.len()];
        let (cols, rows) = Box::pin(self.union_parts(&q, inits, &scope)).await?;
        Ok(match k {
            SubqueryKind::Exists => Value::Bool(!rows.is_empty()),
            SubqueryKind::Count => Value::Int(i64::try_from(rows.len()).unwrap_or(i64::MAX)),
            SubqueryKind::Collect => {
                if cols.len() != 1 {
                    return Err(SprigError::new(
                        SprigCode::GraphSyntax,
                        "A COLLECT subquery must end with a single return column.",
                    ));
                }
                Value::List(
                    rows.into_iter()
                        .filter_map(|r| r.into_iter().next())
                        .collect(),
                )
            }
        })
    }

    /// The parts of `q`, each from its initial row, joined by `UNION`.
    pub(crate) async fn union_parts(
        &mut self,
        q: &Query,
        inits: Vec<Row>,
        scope: &[String],
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        let mut columns: Option<Vec<String>> = None;
        let mut all: Vec<Vec<Value>> = Vec::new();
        for (part, init) in q.parts.iter().zip(inits) {
            let (cols, rows) =
                Box::pin(self.clauses(&part.clauses, vec![init], scope.to_vec())).await?;
            if let Some(c) = &columns {
                if *c != cols {
                    return Err(SprigError::new(
                        SprigCode::GraphSyntax,
                        "All sub queries in an UNION must have the same return column names",
                    ));
                }
            } else {
                columns = Some(cols);
            }
            all.extend(rows);
            self.check_rows(all.len())?;
        }
        if q.union_all.iter().any(|a| !a) {
            let mut seen = HashSet::new();
            all.retain(|r| seen.insert(r.iter().map(Value::key).collect::<Vec<Key>>()));
        }
        Ok((columns.unwrap_or_default(), all))
    }

    /// `CALL { }` for each row: the subquery's rows joined to it (a unit
    /// subquery passes the row through).
    async fn call_subquery(
        &mut self,
        rows: Vec<Row>,
        imports: Option<&Vec<String>>,
        q: &Query,
    ) -> Result<(Vec<Row>, Vec<String>)> {
        let mut next = Vec::new();
        let mut new_cols = Vec::new();
        for r in rows {
            let visible = |k: &String| !k.starts_with(' ');
            let mut inits = Vec::with_capacity(q.parts.len());
            for part in &q.parts {
                let init: Row = match imports {
                    Some(v) if v.iter().any(|x| x == "*") => r
                        .iter()
                        .filter(|(k, _)| visible(k))
                        .map(|(k, x)| (k.clone(), x.clone()))
                        .collect(),
                    Some(v) => v
                        .iter()
                        .filter_map(|k| r.get(k).map(|x| (k.clone(), x.clone())))
                        .collect(),
                    None => match part.clauses.first() {
                        // An importing WITH: plain variables of the row.
                        Some(Clause::With(p)) => {
                            let mut m: Row = Row::new();
                            if p.star {
                                m.extend(
                                    r.iter()
                                        .filter(|(k, _)| visible(k))
                                        .map(|(k, x)| (k.clone(), x.clone())),
                                );
                            }
                            for (e, _) in &p.items {
                                if let Expr::Var(v) = e
                                    && let Some(x) = r.get(v)
                                {
                                    m.insert(v.clone(), x.clone());
                                }
                            }
                            m
                        }
                        _ => Row::new(),
                    },
                };
                inits.push(init);
            }
            let scope: Vec<String> = inits
                .first()
                .map(|i| i.keys().cloned().collect())
                .unwrap_or_default();
            let (cols, out) = Box::pin(self.union_parts(q, inits, &scope)).await?;
            if cols.is_empty() {
                next.push(r);
                continue;
            }
            for c in &cols {
                if r.contains_key(c) && !scope.contains(c) {
                    return Err(SprigError::new(
                        SprigCode::GraphSyntax,
                        format!("Variable `{c}` already declared in outer scope"),
                    ));
                }
            }
            for vals in out {
                let mut nr = r.clone();
                for (c, v) in cols.iter().zip(vals) {
                    nr.insert(c.clone(), v);
                }
                next.push(nr);
            }
            self.check_rows(next.len())?;
            new_cols = cols;
        }
        Ok((next, new_cols))
    }

    /// `CALL { } IN TRANSACTIONS`: the rows in batches of
    /// `OF n ROWS` (1,000), each batch's subquery run and committed on its
    /// own transaction; a failed batch rolls back, then `ON ERROR` decides:
    /// `FAIL` ends the statement (earlier batches stay committed, as in
    /// Neo4j), `CONTINUE` gives the batch's rows `null`s and goes on,
    /// `BREAK` gives every later row `null`s too. `REPORT STATUS AS s`
    /// adds each row's batch status.
    async fn call_in_transactions(
        &mut self,
        rows: Vec<Row>,
        imports: Option<&Vec<String>>,
        q: &Query,
        b: &crate::ast::TxBatch,
    ) -> Result<(Vec<Row>, Vec<String>)> {
        use crate::ast::OnError;
        self.store.batches().await?;
        let size = match &b.rows {
            None => 1000,
            Some(e) => match self.eval_full(e, &Row::new()).await? {
                Value::Int(n) if n > 0 => usize::try_from(n).unwrap_or(usize::MAX),
                other => {
                    return Err(SprigError::new(
                        SprigCode::GraphSyntax,
                        format!(
                            "[Neo.ClientError.Statement.ArgumentError] OF ... ROWS needs a positive integer, got {other:?}"
                        ),
                    ));
                }
            },
        };
        let returned = crate::semantic::returns_of(q).unwrap_or_default();
        let status = |started: bool, committed: bool, tx: Option<u64>, err: Option<String>| {
            Value::Map(
                [
                    ("started".to_owned(), Value::Bool(started)),
                    ("committed".to_owned(), Value::Bool(committed)),
                    (
                        "transactionId".to_owned(),
                        tx.map_or(Value::Null, |t| Value::String(format!("sprig-batch-{t}"))),
                    ),
                    (
                        "errorMessage".to_owned(),
                        err.map_or(Value::Null, Value::String),
                    ),
                ]
                .into_iter()
                .collect(),
            )
        };
        let nulls = |r: &Row, st: Value| {
            let mut nr = r.clone();
            for c in &returned {
                nr.insert(c.clone(), Value::Null);
            }
            if let Some(s) = &b.report {
                nr.insert(s.clone(), st);
            }
            nr
        };
        let mut out = Vec::new();
        let mut cols: Vec<String> = returned.clone();
        let mut committed = 0u64;
        let mut broke = false;
        let mut tx = 0u64;
        for chunk in rows.chunks(size) {
            if broke {
                out.extend(
                    chunk
                        .iter()
                        .map(|r| nulls(r, status(false, false, None, None))),
                );
                continue;
            }
            tx += 1;
            let before = self.stats.clone();
            let deleted = self.deleted.len();
            let r = match Box::pin(self.call_subquery(chunk.to_vec(), imports, q)).await {
                Ok(x) => {
                    let done: Vec<u64> = self.deleted[deleted..].to_vec();
                    self.store.end_batch(true, &done).await.map(|()| x)
                }
                Err(e) => {
                    let _ = self.store.end_batch(false, &[]).await;
                    Err(e)
                }
            };
            match r {
                Ok((next, c)) => {
                    committed += 1;
                    // Checked at the batch's commit.
                    self.deleted.truncate(deleted);
                    if !c.is_empty() {
                        cols = c;
                    }
                    out.extend(next.into_iter().map(|mut nr| {
                        if let Some(s) = &b.report {
                            nr.insert(s.clone(), status(true, true, Some(tx), None));
                        }
                        nr
                    }));
                }
                Err(mut e) => {
                    self.stats = before;
                    self.deleted.truncate(deleted);
                    if b.on_error == OnError::Fail {
                        e.message = format!("{} (Transactions committed: {committed})", e.message);
                        return Err(e);
                    }
                    let msg = crate::neo4j::message(&e);
                    out.extend(
                        chunk
                            .iter()
                            .map(|r| nulls(r, status(true, false, Some(tx), Some(msg.clone())))),
                    );
                    broke = b.on_error == OnError::Break;
                }
            }
            self.check_rows(out.len())?;
        }
        if let Some(s) = &b.report {
            cols.push(s.clone());
        }
        Ok((out, cols))
    }

    /// Runs `clauses` over `rows` with `scope` bound: the columns and rows
    /// of the final `RETURN` (none for an update statement).
    pub(crate) async fn clauses(
        &mut self,
        clauses: &[Clause],
        rows: Vec<Row>,
        scope: Vec<String>,
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        let mut rows = rows;
        let mut scope = scope;
        let n = clauses.len();
        let mut prev: Option<(usize, Instant, u64)> = None;
        for (i, c) in clauses.iter().enumerate() {
            self.check_time()?;
            if self.profile {
                self.clause_line(prev, rows.len());
                prev = Some((i, Instant::now(), self.store.stats().sql_us));
            }
            match c {
                Clause::Match {
                    optional,
                    patterns,
                    where_,
                } => {
                    let mult_ok = dup_insensitive(&clauses[i + 1..]);
                    let dead = dead_vars(rows.first(), &clauses[i..]);
                    if !dead.is_empty() {
                        for r in &mut rows {
                            for d in &dead {
                                r.remove(d);
                            }
                        }
                    }
                    if self.late {
                        self.prefetch = prefetch_hint(where_.as_ref(), clauses.get(i + 1));
                        self.push = where_
                            .as_ref()
                            .map(|w| pushed(w, self.params))
                            .unwrap_or_default();
                    }
                    let top = if self.late && rows.len() > MATCH_CHUNK {
                        self.topk_of(clauses.get(i + 1))?
                    } else {
                        None
                    };
                    let r = match top {
                        Some((p, n)) => {
                            self.match_top(
                                rows,
                                patterns,
                                where_.as_ref(),
                                *optional,
                                mult_ok,
                                p,
                                n,
                            )
                            .await
                        }
                        None => {
                            self.match_clause(rows, patterns, where_.as_ref(), *optional, mult_ok)
                                .await
                        }
                    };
                    self.prefetch.clear();
                    self.push.clear();
                    self.only.clear();
                    rows = r?;
                    for p in patterns {
                        for v in pattern_vars(p) {
                            if !scope.contains(&v) {
                                scope.push(v);
                            }
                        }
                    }
                }
                Clause::Unwind { expr, alias } => {
                    // Variables no later clause names are dropped before
                    // each row is copied once per element (LDBC
                    // IC9's `WITH collect(f) AS friends UNWIND friends AS
                    // friend` copied the whole list into every row).
                    let dead = dead_vars(rows.first(), &clauses[i + 1..]);
                    self.late(&mut rows, &[expr]).await?;
                    let mut out = Vec::new();
                    for mut r in rows {
                        let v = self.eval_full(expr, &r).await?;
                        for d in &dead {
                            r.remove(d);
                        }
                        match v {
                            Value::Null => {}
                            Value::List(l) => {
                                for x in l {
                                    let mut nr = r.clone();
                                    nr.insert(alias.clone(), x);
                                    out.push(nr);
                                }
                            }
                            other => {
                                let mut nr = r.clone();
                                nr.insert(alias.clone(), other);
                                out.push(nr);
                            }
                        }
                        self.check_rows(out.len())?;
                    }
                    rows = out;
                    scope.push(alias.clone());
                }
                Clause::With(p) => {
                    let (cols, out) = self.project(rows, p, &scope).await?;
                    let mut out: Vec<Row> = out
                        .into_iter()
                        .map(|vals| cols.iter().cloned().zip(vals).collect())
                        .collect();
                    if let Some(w) = &p.where_ {
                        out = self.filter(out, w).await?;
                    }
                    rows = out;
                    scope = cols;
                }
                Clause::Return(p) => {
                    self.returning = std::mem::take(&mut self.late_ok);
                    let r = self.project(rows, p, &scope).await;
                    self.returning = false;
                    let (cols, mut out) = r?;
                    // What the statement returns is read in full.
                    self.late_rows(&mut out).await?;
                    if self.profile {
                        self.clause_line(prev, out.len());
                    }
                    return Ok((cols, out));
                }
                Clause::Create(pats) => {
                    rows = self.create(rows, pats).await?;
                    for p in pats {
                        for v in pattern_vars(p) {
                            if !scope.contains(&v) {
                                scope.push(v);
                            }
                        }
                    }
                }
                Clause::Merge {
                    pattern,
                    on_create,
                    on_match,
                } => {
                    rows = self.merge(rows, pattern, on_create, on_match).await?;
                    for v in pattern_vars(pattern) {
                        if !scope.contains(&v) {
                            scope.push(v);
                        }
                    }
                }
                Clause::Set(items) => rows = self.set(rows, items).await?,
                Clause::Remove(items) => rows = self.remove(rows, items).await?,
                Clause::Delete { detach, exprs } => {
                    rows = self.delete(rows, exprs, *detach).await?;
                }
                Clause::Subquery {
                    imports,
                    query,
                    batch,
                } => {
                    let (next, cols) = match batch {
                        None => self.call_subquery(rows, imports.as_ref(), query).await?,
                        Some(b) => {
                            self.call_in_transactions(rows, imports.as_ref(), query, b)
                                .await?
                        }
                    };
                    rows = next;
                    for c in cols {
                        if !scope.contains(&c) {
                            scope.push(c);
                        }
                    }
                }
                Clause::Foreach { var, list, body } => {
                    let mut inner = scope.clone();
                    inner.push(var.clone());
                    for r in &rows {
                        let items = match self.eval_full(list, r).await? {
                            Value::Null => Vec::new(),
                            Value::List(l) => l,
                            other => {
                                return Err(crate::value::type_error(format!(
                                    "FOREACH over a {}",
                                    other.type_name()
                                )));
                            }
                        };
                        for x in items {
                            let mut init = r.clone();
                            init.insert(var.clone(), x);
                            Box::pin(self.clauses(body, vec![init], inner.clone())).await?;
                        }
                    }
                }
                Clause::Schema(cmd) => {
                    if n != 1 {
                        return Err(SprigError::new(
                            SprigCode::GraphSyntax,
                            "index and constraint commands stand alone",
                        ));
                    }
                    for (k, v) in self.store.schema(cmd).await? {
                        self.bump(k, v);
                    }
                    self.wrote = true;
                    return Ok((Vec::new(), Vec::new()));
                }
                Clause::Call {
                    name,
                    args,
                    yields,
                    where_,
                } => {
                    let standalone = n == 1;
                    self.star_in_order = name.starts_with("sprig.show.") && yields.is_some();
                    let (cols, out) = self.call(name, args.as_deref(), yields.as_ref(), &rows)?;
                    if standalone {
                        return Ok((cols, out.into_iter().map(|(_, v)| v).collect()));
                    }
                    let mut next = Vec::new();
                    for (ri, vals) in out {
                        let mut r = rows[ri].clone();
                        for (c, v) in cols.iter().zip(vals) {
                            r.insert(c.clone(), v);
                        }
                        next.push(r);
                    }
                    if let Some(w) = where_ {
                        next = self.filter(next, w).await?;
                    }
                    rows = next;
                    scope.extend(cols);
                }
            }
            self.check_rows(rows.len())?;
            self.check_memory(&rows)?;
        }
        // No RETURN: an update statement.
        Ok((Vec::new(), Vec::new()))
    }

    fn call(
        &mut self,
        name: &str,
        args: Option<&[Expr]>,
        yields: Option<&Vec<(String, String)>>,
        rows: &[Row],
    ) -> Result<CallRows> {
        let show = name.starts_with("sprig.show.");
        if !show && args.is_some_and(|a| !a.is_empty()) {
            return Err(SprigError::new(
                SprigCode::GraphSyntax,
                format!("procedure {name} takes no arguments"),
            ));
        }
        let filter = match args.and_then(<[Expr]>::first) {
            Some(Expr::Lit(Value::String(f))) => f.as_str(),
            _ => "ALL",
        };
        let (cols, vals): (Vec<&str>, Vec<Vec<Value>>) = match name {
            "sprig.show.indexes" => crate::schema::show_indexes(&self.store.indexes(), filter),
            "sprig.show.constraints" => {
                crate::schema::show_constraints(&self.store.indexes(), filter)
            }
            "db.labels" => (
                vec!["label"],
                self.store
                    .all_labels()
                    .into_iter()
                    .map(|l| vec![Value::String(l)])
                    .collect(),
            ),
            "db.relationshiptypes" => (
                vec!["relationshipType"],
                self.store
                    .all_types()
                    .into_iter()
                    .map(|l| vec![Value::String(l)])
                    .collect(),
            ),
            "tx.getmetadata" => (
                vec!["metadata"],
                vec![vec![Value::Map(
                    self.b.tx_metadata.clone().unwrap_or_default(),
                )]],
            ),
            "dbms.components" => (
                vec!["name", "versions", "edition"],
                vec![vec![
                    Value::str("Neo4j Kernel"),
                    Value::List(vec![Value::str("5.26.0")]),
                    Value::str("community"),
                ]],
            ),
            other => {
                return Err(SprigError::new(
                    SprigCode::GraphUnsupported,
                    format!("procedure {other} is not supported"),
                ));
            }
        };
        let (out_cols, idx): (Vec<String>, Vec<usize>) = match yields {
            None => (
                cols.iter().map(|c| (*c).to_owned()).collect(),
                (0..cols.len()).collect(),
            ),
            Some(y) => {
                let mut oc = Vec::new();
                let mut ix = Vec::new();
                for (c, a) in y {
                    let Some(i) = cols.iter().position(|x| x == c) else {
                        return Err(SprigError::new(
                            SprigCode::GraphSyntax,
                            format!("Unknown procedure output: `{c}`"),
                        ));
                    };
                    oc.push(a.clone());
                    ix.push(i);
                }
                (oc, ix)
            }
        };
        let mut out = Vec::new();
        for ri in 0..rows.len() {
            for v in &vals {
                out.push((ri, idx.iter().map(|i| v[*i].clone()).collect()));
            }
        }
        Ok((out_cols, out))
    }

    fn const_int(&self, e: Option<&Expr>, what: &str) -> Result<Option<usize>> {
        let Some(e) = e else { return Ok(None) };
        match eval(e, &Row::new(), self.params)? {
            Value::Int(i) if i >= 0 => Ok(usize::try_from(i).ok()),
            other => Err(SprigError::new(
                SprigCode::GraphSyntax,
                format!(
                    "{what} must be a non-negative integer, got {}",
                    other.display()
                ),
            )),
        }
    }

    /// `WITH` / `RETURN`: the column names and rows.
    #[allow(clippy::too_many_lines)] // grouping, sorting and paging in one place
    /// The projection after a `MATCH` and its row count, when the
    /// projection keeps only its first rows in an order of plain items
    /// (`ORDER BY ... LIMIT n`, no aggregate, `DISTINCT`, `*`, pattern
    /// or a `WITH`'s `WHERE`): the `MATCH` may then keep only those.
    fn topk_of<'c>(&self, next: Option<&'c Clause>) -> Result<Option<(&'c Projection, usize)>> {
        let p = match next {
            Some(Clause::Return(p)) => p,
            Some(Clause::With(p)) if p.where_.is_none() => p,
            _ => return Ok(None),
        };
        if p.distinct
            || p.star
            || p.order.is_empty()
            || p.items
                .iter()
                .any(|(e, _)| e.has_aggregate() || has_deep(e))
            || p.order
                .iter()
                .any(|(e, _)| e.has_aggregate() || has_deep(e))
        {
            return Ok(None);
        }
        let Some(l) = self.const_int(p.limit.as_ref(), "LIMIT")? else {
            return Ok(None);
        };
        let skip = self.const_int(p.skip.as_ref(), "SKIP")?.unwrap_or(0);
        Ok(Some((p, skip.saturating_add(l))))
    }

    /// A `MATCH` followed by a projection that keeps its first `n` rows
    /// ([`Exec::topk_of`]): the incoming rows go through in chunks, and
    /// after each only the best `n` rows so far are kept (ties in input
    /// order, as the projection's sort has them), so the clause never
    /// holds every row (LDBC IC9 matched a hundred thousand messages to
    /// keep twenty).
    #[allow(clippy::too_many_arguments)]
    async fn match_top(
        &mut self,
        rows: Vec<Row>,
        patterns: &[PathPattern],
        where_: Option<&Expr>,
        optional: bool,
        mult_ok: bool,
        p: &Projection,
        n: usize,
    ) -> Result<Vec<Row>> {
        let mut kept: Vec<(Vec<Value>, usize, Row)> = Vec::new();
        let mut pos = 0usize;
        let mut rows = rows;
        // Sort keys that read one variable the MATCH binds (aliases
        // replaced by their items): far nodes of that variable already
        // worse than the last row kept needn't become rows. Not under
        // OPTIONAL MATCH, where a row with no match would appear instead.
        self.prune = if optional {
            None
        } else {
            prune_of(p, rows.first())
        };
        while !rows.is_empty() {
            let rest = rows.split_off(MATCH_CHUNK.min(rows.len()));
            let chunk = std::mem::replace(&mut rows, rest);
            let mut out = self
                .match_clause(chunk, patterns, where_, optional, mult_ok)
                .await?;
            let keys = self.sort_keys(&mut out, &p.items, p).await?;
            for (k, r) in keys.into_iter().zip(out) {
                kept.push((k, pos, r));
                pos += 1;
            }
            if kept.len() > n.saturating_mul(2).max(MATCH_CHUNK) {
                Self::keep_top(&mut kept, p, n);
            }
            if kept.len() >= n
                && n > 0
                && let Some(pr) = &mut self.prune
            {
                Self::keep_top(&mut kept, p, n);
                pr.bound = kept.last().map(|(k, _, _)| k.clone());
            }
        }
        self.prune = None;
        Self::keep_top(&mut kept, p, n);
        self.plan.push(format!(
            "Top {n}: the MATCH kept only the rows the next projection keeps, in chunks of {MATCH_CHUNK} incoming rows"
        ));
        Ok(kept.into_iter().map(|(_, _, r)| r).collect())
    }

    /// The first `n` of `rows` in `p`'s order (ties in input order),
    /// with what `items` read of them: the sort keys (and the items they
    /// name by alias) are read and evaluated for every row, the rest of
    /// the items only for the rows kept.
    async fn top_rows(
        &mut self,
        rows: Vec<Row>,
        items: &[(Expr, String)],
        p: &Projection,
        n: usize,
    ) -> Result<Vec<Row>> {
        let mut rows = rows;
        let keys = self.sort_keys(&mut rows, items, p).await?;
        let mut keyed: Vec<(Vec<Value>, usize, Row)> = keys
            .into_iter()
            .zip(rows)
            .enumerate()
            .map(|(pos, (k, r))| (k, pos, r))
            .collect();
        Self::keep_top(&mut keyed, p, n);
        let mut kept: Vec<Row> = keyed.into_iter().map(|(_, _, r)| r).collect();
        let exprs: Vec<&Expr> = items.iter().map(|(e, _)| e).collect();
        self.late(&mut kept, &exprs).await?;
        Ok(kept)
    }

    /// `p`'s sort keys for each of `rows` (reading what they need first);
    /// the keys may name items by alias.
    async fn sort_keys(
        &mut self,
        rows: &mut [Row],
        items: &[(Expr, String)],
        p: &Projection,
    ) -> Result<Vec<Vec<Value>>> {
        let order: Vec<&Expr> = p.order.iter().map(|(e, _)| e).collect();
        // Items the sort keys name by alias.
        let mut named: Vec<usize> = Vec::new();
        for e in &order {
            e.walk(&mut |x| {
                if let Expr::Var(v) = x
                    && let Some(i) = items.iter().position(|(_, a)| a == v)
                    && !named.contains(&i)
                {
                    named.push(i);
                }
            });
        }
        // What the keys read of an alias, the item's variable needs; an
        // item that isn't a variable is read whole when the keys read
        // its alias's properties.
        let order_need = late::needs_of(&order);
        let mut need = order_need.clone();
        for &i in &named {
            let (e, alias) = &items[i];
            match (e, order_need.get(alias)) {
                (Expr::Var(v), Some(n)) => late::add(&mut need, v, n.clone()),
                (Expr::Var(_), None) => {}
                (other, n) => late::needs(other, n.is_none(), &mut need),
            }
        }
        self.late_needs(rows, &need).await?;
        // Each key: its own expression, or (a bare alias) its item's; only
        // keys that use an alias inside a larger expression need the row
        // with the items added.
        let direct: Option<Vec<&Expr>> = order
            .iter()
            .map(|e| {
                if let Expr::Var(v) = e {
                    return Some(items.iter().find(|(_, a)| a == v).map_or(*e, |(ie, _)| ie));
                }
                let mut uses = false;
                e.walk(&mut |x| {
                    if let Expr::Var(v) = x
                        && items.iter().any(|(_, a)| a == v)
                    {
                        uses = true;
                    }
                });
                (!uses).then_some(*e)
            })
            .collect();
        let mut out: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
        for (n, r) in rows.iter().enumerate() {
            if n % 4096 == 4095 {
                tokio::task::yield_now().await;
            }
            let ks = if let Some(d) = &direct {
                let mut ks = Vec::with_capacity(d.len());
                for e in d {
                    ks.push(eval(e, r, self.params)?);
                }
                ks
            } else if named.is_empty() {
                let mut ks = Vec::with_capacity(order.len());
                for e in &order {
                    ks.push(self.eval_full(e, r).await?);
                }
                ks
            } else {
                let mut m = r.clone();
                for &i in &named {
                    let v = self.eval_full(&items[i].0, r).await?;
                    m.insert(items[i].1.clone(), v);
                }
                let mut ks = Vec::with_capacity(order.len());
                for e in &order {
                    ks.push(self.eval_full(e, &m).await?);
                }
                ks
            };
            out.push(ks);
        }
        Ok(out)
    }

    /// Keeps the first `n` of `keyed` (keys, input position, row) in
    /// `p`'s order, sorted; ties keep their input order.
    fn keep_top(keyed: &mut Vec<(Vec<Value>, usize, Row)>, p: &Projection, n: usize) {
        let cmp = |a: &(Vec<Value>, usize, Row), b: &(Vec<Value>, usize, Row)| {
            for (i, (_, asc)) in p.order.iter().enumerate() {
                let o = a.0[i].order(&b.0[i]);
                let o = if *asc { o } else { o.reverse() };
                if o != std::cmp::Ordering::Equal {
                    return o;
                }
            }
            a.1.cmp(&b.1)
        };
        match n {
            0 => keyed.clear(),
            n if n < keyed.len() => {
                keyed.select_nth_unstable_by(n - 1, cmp);
                keyed.truncate(n);
                keyed.sort_unstable_by(cmp);
            }
            _ => keyed.sort_unstable_by(cmp),
        }
    }

    async fn project(
        &mut self,
        rows: Vec<Row>,
        p: &Projection,
        scope: &[String],
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        let mut items: Vec<(Expr, String)> = Vec::new();
        if p.star {
            let mut vars: Vec<String> = scope
                .iter()
                .filter(|v| !v.starts_with(' '))
                .cloned()
                .collect();
            if !std::mem::take(&mut self.star_in_order) {
                vars.sort();
            }
            vars.dedup();
            if vars.is_empty() && p.items.is_empty() {
                return Err(SprigError::new(
                    SprigCode::GraphSyntax,
                    "RETURN * is not allowed when there are no variables in scope",
                ));
            }
            items.extend(vars.into_iter().map(|v| (Expr::Var(v.clone()), v)));
        }
        items.extend(p.items.iter().cloned());
        let cols: Vec<String> = items.iter().map(|(_, a)| a.clone()).collect();
        // ORDER BY ... LIMIT over plain items: sort keys first, then the
        // items only for the rows kept (late materialization).
        let topk = if !p.distinct
            && !p.order.is_empty()
            && self.late
            && !items.iter().any(|(e, _)| e.has_aggregate() || has_deep(e))
            && !p.order.iter().any(|(e, _)| has_deep(e))
        {
            match self.const_int(p.limit.as_ref(), "LIMIT")? {
                Some(l) => Some(
                    self.const_int(p.skip.as_ref(), "SKIP")?
                        .unwrap_or(0)
                        .saturating_add(l),
                ),
                None => None,
            }
        } else {
            None
        };
        if let Some(n) = topk {
            let rows = self.top_rows(rows, &items, p, n).await?;
            let mut out: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
            for r in &rows {
                let mut vals = Vec::with_capacity(items.len());
                for (e, _) in &items {
                    vals.push(self.eval_full(e, r).await?);
                }
                out.push(vals);
            }
            let skip = self.const_int(p.skip.as_ref(), "SKIP")?.unwrap_or(0);
            return Ok((cols, out.into_iter().skip(skip).collect()));
        }
        // The properties the items and sort keys read, for all rows at
        // once.
        let mut rows = rows;
        {
            let exprs: Vec<&Expr> = items
                .iter()
                .map(|(e, _)| e)
                .chain(p.order.iter().map(|(e, _)| e))
                .collect();
            let mut need = late::needs_of(&exprs);
            // ORDER BY may read an item through its alias (`RETURN r AS
            // rel ORDER BY rel.id`): what it reads of the alias, the
            // item's own variable needs (anything else, all of it).
            let order: Vec<&Expr> = p.order.iter().map(|(e, _)| e).collect();
            for (alias, n) in late::needs_of(&order) {
                for (e, a) in &items {
                    if *a != alias {
                        continue;
                    }
                    match e {
                        Expr::Var(v) if *v != alias => late::add(&mut need, v, n.clone()),
                        Expr::Var(_) => {}
                        other => late::needs(other, false, &mut need),
                    }
                }
            }
            self.late_needs(&mut rows, &need).await?;
        }
        {
            let mut seen = HashSet::new();
            for c in &cols {
                if !seen.insert(c) {
                    return Err(SprigError::new(
                        SprigCode::GraphSyntax,
                        format!(
                            "Multiple result columns with the same name are not supported: {c}"
                        ),
                    ));
                }
            }
        }
        let aggregating = items.iter().any(|(e, _)| e.has_aggregate());
        // (projected values, the row ORDER BY sees)
        let mut out: Vec<(Vec<Value>, Row)> = Vec::new();
        if aggregating {
            let mut aggs: Vec<agg::Spec> = Vec::new();
            let lifted: Vec<Expr> = items
                .iter()
                .map(|(e, _)| agg::lift(e, &mut aggs))
                .collect::<Result<_>>()?;
            let key_items: Vec<usize> = (0..items.len())
                .filter(|i| !items[*i].0.has_aggregate())
                .collect();
            let mut groups: Vec<(Row, Vec<Value>, Vec<agg::State>)> = Vec::new();
            let mut index: HashMap<Vec<Key>, usize> = HashMap::new();
            for r in &rows {
                let mut kv = Vec::with_capacity(key_items.len());
                for &i in &key_items {
                    kv.push(self.eval_full(&items[i].0, r).await?);
                }
                let k: Vec<Key> = kv.iter().map(Value::key).collect();
                let gi = if let Some(g) = index.get(&k) {
                    *g
                } else {
                    groups.push((r.clone(), kv, aggs.iter().map(agg::State::new).collect()));
                    index.insert(k, groups.len() - 1);
                    groups.len() - 1
                };
                for (ai, a) in aggs.iter().enumerate() {
                    let v = match &a.arg {
                        Some(e) => Some(self.eval_full(e, r).await?),
                        None => None,
                    };
                    groups[gi].2[ai].add(a, v);
                }
            }
            if groups.is_empty() && key_items.is_empty() {
                groups.push((
                    Row::new(),
                    vec![],
                    aggs.iter().map(agg::State::new).collect(),
                ));
            }
            self.check_rows(groups.len())?;
            for (first, kv, states) in groups {
                let mut gr = first;
                for (slot, v) in key_items.iter().zip(kv) {
                    gr.insert(items[*slot].1.clone(), v);
                }
                for (ai, (a, s)) in aggs.iter().zip(states).enumerate() {
                    gr.insert(agg::slot(ai), s.finish(a, self.params)?);
                }
                let mut vals = Vec::with_capacity(items.len());
                for e in &lifted {
                    vals.push(eval(e, &gr, self.params)?);
                }
                for (c, v) in cols.iter().zip(&vals) {
                    gr.insert(c.clone(), v.clone());
                }
                out.push((vals, gr));
            }
        } else {
            // A read's top RETURN without DISTINCT or ORDER BY evaluates
            // rows as Neo4j's lazy results do: only those SKIP and LIMIT
            // keep, and an error on a row comes after the rows before it.
            let in_order = !p.distinct && p.order.is_empty();
            let lazy = std::mem::take(&mut self.returning) && in_order;
            let needed = if in_order {
                let skip = self.const_int(p.skip.as_ref(), "SKIP")?.unwrap_or(0);
                self.const_int(p.limit.as_ref(), "LIMIT")?
                    .map(|l| skip.saturating_add(l))
            } else {
                None
            };
            // Items with a pattern predicate or subquery, for every row at
            // once (LDBC IC7's `RETURN ..., not((liker)-[:KNOWS]-
            // (person))` ran one search per row, thousands of round trips).
            let mut pre: Vec<Option<std::vec::IntoIter<Value>>> = Vec::with_capacity(items.len());
            for (e, _) in &items {
                pre.push(if !lazy && needed.is_none() && has_deep(e) {
                    Some(self.eval_rows(e, &mut rows).await?.into_iter())
                } else {
                    None
                });
            }
            'rows: for r in rows {
                if needed.is_some_and(|n| out.len() >= n) {
                    break;
                }
                let mut vals = Vec::with_capacity(items.len());
                for (ii, (e, _)) in items.iter().enumerate() {
                    if let Some(it) = pre[ii].as_mut() {
                        vals.push(it.next().unwrap_or(Value::Null));
                        continue;
                    }
                    match self.eval_full(e, &r).await {
                        Ok(v) => vals.push(v),
                        Err(e) if lazy && is_row_error(&e) => {
                            self.late_error = Some(e);
                            break 'rows;
                        }
                        Err(e) => return Err(e),
                    }
                }
                let mut merged = if p.distinct { Row::new() } else { r };
                for (c, v) in cols.iter().zip(&vals) {
                    merged.insert(c.clone(), v.clone());
                }
                out.push((vals, merged));
            }
            if p.distinct {
                let mut seen = HashSet::new();
                out.retain(|(v, _)| seen.insert(v.iter().map(Value::key).collect::<Vec<Key>>()));
            }
        }
        if !p.order.is_empty() {
            let mut keyed = Vec::with_capacity(out.len());
            for (vals, r) in out {
                let mut ks = Vec::with_capacity(p.order.len());
                for (e, _) in &p.order {
                    // After aggregation, aggregates in ORDER BY must be
                    // projected items: match them by text.
                    // So must expressions after DISTINCT (`RETURN DISTINCT
                    // b.name ORDER BY b.name`), whose variables are gone.
                    let v = match items.iter().position(|(ie, _)| ie == e) {
                        Some(i) if aggregating || p.distinct => vals[i].clone(),
                        _ => self.eval_full(e, &r).await?,
                    };
                    ks.push(v);
                }
                keyed.push((ks, keyed.len(), vals, r));
            }
            // Ties keep their input order (the position is the last key),
            // so a partial sort gives what the full stable sort gave.
            let cmp = |a: &(Vec<Value>, usize, Vec<Value>, Row),
                       b: &(Vec<Value>, usize, Vec<Value>, Row)| {
                for (i, (_, asc)) in p.order.iter().enumerate() {
                    let o = a.0[i].order(&b.0[i]);
                    let o = if *asc { o } else { o.reverse() };
                    if o != std::cmp::Ordering::Equal {
                        return o;
                    }
                }
                a.1.cmp(&b.1)
            };
            // ORDER BY ... LIMIT n (LDBC IC9): only the first
            // SKIP + LIMIT rows are put in order.
            let top = self
                .const_int(p.limit.as_ref(), "LIMIT")?
                .map(|l| {
                    self.const_int(p.skip.as_ref(), "SKIP")
                        .map(|s| s.unwrap_or(0).saturating_add(l))
                })
                .transpose()?;
            match top {
                Some(0) => keyed.clear(),
                Some(n) if n < keyed.len() => {
                    keyed.select_nth_unstable_by(n - 1, cmp);
                    keyed.truncate(n);
                    keyed.sort_unstable_by(cmp);
                }
                _ => keyed.sort_unstable_by(cmp),
            }
            out = keyed.into_iter().map(|(_, _, v, r)| (v, r)).collect();
        }
        let skip = self.const_int(p.skip.as_ref(), "SKIP")?.unwrap_or(0);
        let limit = self.const_int(p.limit.as_ref(), "LIMIT")?;
        let rows: Vec<Vec<Value>> = out
            .into_iter()
            .skip(skip)
            .take(limit.unwrap_or(usize::MAX))
            .map(|(v, _)| v)
            .collect();
        Ok((cols, rows))
    }
}

/// Whether `e` is an error of one row's values (a type or arithmetic
/// error, a bad argument), which Neo4j's lazy results report on the row
/// that has it; budget, storage and syntax errors stay errors of the
/// statement.
fn is_row_error(e: &SprigError) -> bool {
    let code = crate::neo4j::status_code(e);
    matches!(
        code.as_str(),
        "Neo.ClientError.Statement.ArithmeticError"
            | "Neo.ClientError.Statement.TypeError"
            | "Neo.ClientError.Statement.ArgumentError"
            | "Neo.ClientError.Statement.InvalidArgument"
    )
}

/// The variables a pattern binds.
pub(crate) fn pattern_vars(p: &PathPattern) -> Vec<String> {
    let mut v = Vec::new();
    if let Some(x) = &p.var {
        v.push(x.clone());
    }
    if let Some(x) = &p.start.var {
        v.push(x.clone());
    }
    for (r, n) in &p.steps {
        if let Some(x) = &r.var {
            v.push(x.clone());
        }
        if let Some(x) = &n.var {
            v.push(x.clone());
        }
    }
    v
}

/// Aggregation.
mod agg {
    use sprig_common::{Result, SprigCode, SprigError};

    use crate::ast::Expr;
    use crate::eval::Params;
    use crate::value::{Key, Value, type_error};

    pub(super) struct Spec {
        pub(super) name: String,
        pub(super) distinct: bool,
        /// `None` for `count(*)`.
        pub(super) arg: Option<Expr>,
        /// The second argument of the percentiles.
        pub(super) extra: Option<Expr>,
    }

    pub(super) fn slot(i: usize) -> String {
        format!(" agg{i}")
    }

    /// Replaces aggregate calls by slots.
    pub(super) fn lift(e: &Expr, specs: &mut Vec<Spec>) -> Result<Expr> {
        Ok(match e {
            Expr::CountStar => {
                specs.push(Spec {
                    name: "count".into(),
                    distinct: false,
                    arg: None,
                    extra: None,
                });
                Expr::Slot(slot(specs.len() - 1))
            }
            Expr::Func {
                name,
                distinct,
                args,
            } if crate::ast::is_aggregate(name) => {
                if args.iter().any(Expr::has_aggregate) {
                    return Err(SprigError::new(
                        SprigCode::GraphSyntax,
                        "Can't use aggregate functions inside of aggregate functions.",
                    ));
                }
                specs.push(Spec {
                    name: name.clone(),
                    distinct: *distinct,
                    arg: args.first().cloned(),
                    extra: args.get(1).cloned(),
                });
                Expr::Slot(slot(specs.len() - 1))
            }
            Expr::Func {
                name,
                distinct,
                args,
            } => Expr::Func {
                name: name.clone(),
                distinct: *distinct,
                args: args.iter().map(|a| lift(a, specs)).collect::<Result<_>>()?,
            },
            Expr::Binary(op, a, b) => {
                Expr::Binary(*op, Box::new(lift(a, specs)?), Box::new(lift(b, specs)?))
            }
            Expr::Unary(op, a) => Expr::Unary(*op, Box::new(lift(a, specs)?)),
            Expr::Prop(a, k) => Expr::Prop(Box::new(lift(a, specs)?), k.clone()),
            Expr::Index(a, b) => Expr::Index(Box::new(lift(a, specs)?), Box::new(lift(b, specs)?)),
            Expr::List(l) => Expr::List(l.iter().map(|a| lift(a, specs)).collect::<Result<_>>()?),
            Expr::Map(m) => Expr::Map(
                m.iter()
                    .map(|(k, a)| Ok((k.clone(), lift(a, specs)?)))
                    .collect::<Result<_>>()?,
            ),
            Expr::MapProjection(b, items) => Expr::MapProjection(
                b.clone(),
                items
                    .iter()
                    .map(|i| match i {
                        crate::ast::MapItem::Lit(k, a) => {
                            Ok(crate::ast::MapItem::Lit(k.clone(), lift(a, specs)?))
                        }
                        other => Ok(other.clone()),
                    })
                    .collect::<Result<_>>()?,
            ),
            Expr::Case {
                subject,
                whens,
                else_,
            } => Expr::Case {
                subject: subject
                    .as_ref()
                    .map(|s| lift(s, specs).map(Box::new))
                    .transpose()?,
                whens: whens
                    .iter()
                    .map(|(a, b)| Ok((lift(a, specs)?, lift(b, specs)?)))
                    .collect::<Result<_>>()?,
                else_: else_
                    .as_ref()
                    .map(|s| lift(s, specs).map(Box::new))
                    .transpose()?,
            },
            other => other.clone(),
        })
    }

    pub(super) struct State {
        count: i64,
        values: Vec<Value>,
        seen: std::collections::HashSet<Key>,
    }

    impl State {
        pub(super) fn new(_s: &Spec) -> Self {
            Self {
                count: 0,
                values: Vec::new(),
                seen: std::collections::HashSet::new(),
            }
        }

        pub(super) fn add(&mut self, s: &Spec, v: Option<Value>) {
            let Some(v) = v else {
                self.count += 1;
                return;
            };
            if v.is_null() || (s.distinct && !self.seen.insert(v.key())) {
                return;
            }
            self.count += 1;
            if s.name != "count" {
                self.values.push(v);
            }
        }

        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        pub(super) fn finish(self, s: &Spec, params: &Params) -> Result<Value> {
            let nums = |vals: &[Value]| -> Result<Vec<f64>> {
                vals.iter()
                    .map(|v| match v {
                        Value::Int(i) => Ok(*i as f64),
                        Value::Float(f) => Ok(*f),
                        other => Err(type_error(format!(
                            "{}() of a {}",
                            s.name,
                            other.type_name()
                        ))),
                    })
                    .collect()
            };
            Ok(match s.name.as_str() {
                "count" => Value::Int(self.count),
                "collect" => Value::List(self.values),
                "sum" => {
                    if self.values.iter().all(|v| matches!(v, Value::Int(_))) {
                        let mut t: i64 = 0;
                        for v in &self.values {
                            if let Value::Int(i) = v {
                                t = t
                                    .checked_add(*i)
                                    .ok_or_else(|| type_error("integer overflow"))?;
                            }
                        }
                        Value::Int(t)
                    } else {
                        Value::Float(nums(&self.values)?.iter().sum())
                    }
                }
                "avg" => {
                    let n = nums(&self.values)?;
                    if n.is_empty() {
                        Value::Null
                    } else {
                        // A running mean, as Neo4j computes it (the last
                        // bits of the result match).
                        let mut m = 0.0;
                        for (i, x) in n.iter().enumerate() {
                            m += (x - m) / (i + 1) as f64;
                        }
                        Value::Float(m)
                    }
                }
                "min" => self
                    .values
                    .into_iter()
                    .min_by(Value::order)
                    .unwrap_or(Value::Null),
                "max" => self
                    .values
                    .into_iter()
                    .max_by(Value::order)
                    .unwrap_or(Value::Null),
                "stdev" | "stdevp" => {
                    let n = nums(&self.values)?;
                    let d = if s.name == "stdev" { 1.0 } else { 0.0 };
                    if (n.len() as f64) - d <= 0.0 {
                        Value::Float(0.0)
                    } else {
                        let m = n.iter().sum::<f64>() / n.len() as f64;
                        let var =
                            n.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (n.len() as f64 - d);
                        Value::Float(var.sqrt())
                    }
                }
                _ => {
                    // percentileDisc / percentileCont.
                    let mut n = nums(&self.values)?;
                    if n.is_empty() {
                        return Ok(Value::Null);
                    }
                    n.sort_by(f64::total_cmp);
                    let p = match s
                        .extra
                        .as_ref()
                        .map(|e| crate::eval::eval(e, &crate::eval::Row::new(), params))
                        .transpose()?
                    {
                        Some(Value::Float(f)) => f,
                        Some(Value::Int(i)) => i as f64,
                        _ => return Err(type_error("a percentile must be a number in [0, 1]")),
                    };
                    if !(0.0..=1.0).contains(&p) {
                        return Err(type_error("a percentile must be a number in [0, 1]"));
                    }
                    let last = (n.len() - 1) as f64;
                    if s.name == "percentiledisc" {
                        let i = ((p * n.len() as f64).ceil() as usize)
                            .saturating_sub(1)
                            .min(n.len() - 1);
                        Value::Float(n[i])
                    } else {
                        let pos = p * last;
                        let (lo, hi) = (pos.floor() as usize, pos.ceil() as usize);
                        Value::Float(n[lo] + (n[hi] - n[lo]) * (pos - pos.floor()))
                    }
                }
            })
        }
    }
}

#[cfg(test)]
mod tests;
