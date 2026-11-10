//! The graph executor (chapter 43, "Algorithms"; build steps 9–13): runs
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

mod matcher;
mod stream;
mod write;

pub use stream::{RowSink, VecSink, run_stream, streamable};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_common::{Result, SprigCode, SprigError};

use crate::ast::{Clause, Explain, Expr, MapItem, PathPattern, Projection, Query, SubqueryKind};
use crate::eval::{Params, Row, eval};
use crate::store::{EdgeRow, GraphStore, NodeRow};
use crate::value::{Key, Node, Rel, Value, props_from_json};

/// The traversal budgets of one statement (chapter 43, "Configuration").
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
/// 39900–39909 as the chapter lists; the store's errors.
pub async fn run(
    store: &mut dyn GraphStore,
    q: &Query,
    params: &Params,
    budgets: &Budgets,
) -> Result<Output> {
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
    };
    if q.explain == Explain::Explain {
        let plan = explain(q);
        return Ok(Output {
            qtype: if has_writes(q) { "rw" } else { "r" },
            plan: Some(plan),
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
        p
    });
    Ok(Output {
        columns,
        rows: all,
        stats: ex.stats,
        deleted_nodes: ex.deleted,
        qtype,
        plan,
    })
}

/// Whether a statement writes (or changes the schema).
#[must_use]
pub fn has_writes(q: &Query) -> bool {
    q.parts.iter().any(|p| clauses_write(&p.clauses))
}

fn clauses_write(cs: &[Clause]) -> bool {
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

/// The plan `EXPLAIN` shows: one line per clause and hop, with its tier.
fn explain(q: &Query) -> Vec<String> {
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
                Clause::Subquery { .. } => out.push("CallSubquery: once per row".into()),
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
}

/// A procedure's columns, and its rows by input row.
type CallRows = (Vec<String>, Vec<(usize, Vec<Value>)>);

/// Hidden row keys start with a space, so no variable can clash.
pub(crate) const RELS: &str = " rels";

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
}

/// Whether duplicates of rows coming out of a `MATCH` can't change the
/// result (chapter 43's `Mult`): the next clause is `WITH`/`RETURN
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
    pub(crate) fn check_memory(&self, rows: &[Row]) -> Result<()> {
        let width = rows.first().map_or(0, HashMap::len) + 1;
        if rows.len().saturating_mul(width).saturating_mul(96) > self.b.max_memory {
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
            other => other.clone(),
        }
    }

    /// Evaluates a predicate with pattern predicates over `rows`, keeping
    /// the rows where it is true.
    pub(crate) async fn filter(&mut self, rows: Vec<Row>, pred: &Expr) -> Result<Vec<Row>> {
        let mut pats = Vec::new();
        let pred = self.lift_patterns(pred, &mut pats);
        let mut rows = rows;
        for (slot, l) in &pats {
            match l {
                Lifted::Pattern(p) => {
                    let hits = self.pattern_hits(&rows, p).await?;
                    for (i, r) in rows.iter_mut().enumerate() {
                        r.insert(slot.clone(), Value::Bool(hits.contains(&i)));
                    }
                }
                Lifted::Subquery(k, q) => {
                    for r in &mut rows {
                        let v = self.subquery_value(*k, q, r).await?;
                        r.insert(slot.clone(), v);
                    }
                }
            }
        }
        let mut out = Vec::with_capacity(rows.len());
        for mut r in rows {
            if eval(&pred, &r, self.params)?.truth()? == Some(true) {
                for (s, _) in &pats {
                    r.remove(s);
                }
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
        let mut has_pattern = false;
        e.walk(&mut |x| has_pattern |= matches!(x, Expr::Pattern(_) | Expr::Subquery(..)));
        if !has_pattern {
            return eval(e, row, self.params);
        }
        let mut pats = Vec::new();
        let lifted = self.lift_patterns(e, &mut pats);
        let mut r = row.clone();
        for (slot, l) in &pats {
            let v = match l {
                Lifted::Pattern(p) => Value::Bool(
                    !self
                        .pattern_hits(std::slice::from_ref(row), p)
                        .await?
                        .is_empty(),
                ),
                Lifted::Subquery(k, q) => self.subquery_value(*k, q, row).await?,
            };
            r.insert(slot.clone(), v);
        }
        eval(&lifted, &r, self.params)
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
        for (i, c) in clauses.iter().enumerate() {
            self.check_time()?;
            match c {
                Clause::Match {
                    optional,
                    patterns,
                    where_,
                } => {
                    let mult_ok = dup_insensitive(&clauses[i + 1..]);
                    rows = self
                        .match_clause(rows, patterns, where_.as_ref(), *optional, mult_ok)
                        .await?;
                    for p in patterns {
                        for v in pattern_vars(p) {
                            if !scope.contains(&v) {
                                scope.push(v);
                            }
                        }
                    }
                }
                Clause::Unwind { expr, alias } => {
                    let mut out = Vec::new();
                    for r in rows {
                        match self.eval_full(expr, &r).await? {
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
                    let (cols, out) = self.project(rows, p, &scope).await?;
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
                Clause::Subquery { imports, query } => {
                    let (next, cols) = self.call_subquery(rows, imports.as_ref(), query).await?;
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
            for r in rows {
                let mut vals = Vec::with_capacity(items.len());
                for (e, _) in &items {
                    vals.push(self.eval_full(e, &r).await?);
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
                keyed.push((ks, vals, r));
            }
            keyed.sort_by(|a, b| {
                for (i, (_, asc)) in p.order.iter().enumerate() {
                    let o = a.0[i].order(&b.0[i]);
                    let o = if *asc { o } else { o.reverse() };
                    if o != std::cmp::Ordering::Equal {
                        return o;
                    }
                }
                std::cmp::Ordering::Equal
            });
            out = keyed.into_iter().map(|(_, v, r)| (v, r)).collect();
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
