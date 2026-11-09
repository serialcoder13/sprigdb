//! The planner (chapter 17, "Routing analysis" to "UPDATE and DELETE"):
//! statement text, as chapter 07's rewrite left it, to a [`Plan`].
//!
//! ```text
//! plan(sql):
//!   no table of the topology named in the text      → NotRouted (caller relays as before)
//!   EXPLAIN FORMAT=SPRIG s                           → Explain(plan(s))
//!   parse (sqlparser, MySQL dialect, no unescaping)  → Reject(parse) on failure
//!   every SELECT block: its tables, the conjuncts that restrict them, the
//!     key equalities that co-locate them → each sharded table's shard set
//!   every sharded table on one shard, one cluster    → Pass (schemas spliced)
//!   SELECT over one co-located component             → Read: Scatter, Merge or Aggregate
//!   SELECT over two tables of different groups       → LookupJoin
//!   INSERT … VALUES                                  → rows grouped by shard → Pass or Write
//!   UPDATE, DELETE                                   → Pass or Write
//!   anything else                                    → Reject(reason)
//! ```

use std::cell::Cell;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Distinct, Expr, FromTable, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator, LimitClause, ObjectName,
    ObjectNamePart, OrderByKind, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableObject, UnaryOperator, Value, visit_expressions, visit_expressions_mut, visit_relations,
    visit_relations_mut,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserOptions};

use crate::plan::{
    AggCall, AggFunc, AggPlan, CmpOp, ColRef, FinalSort, JoinCol, KeyCols, LookupJoinPlan, Operand,
    OutCol, Plan, Pred, ReadKind, ReadPlan, SortKey, Target, WriteKind, WritePlan,
};
use crate::reject::{Reason, Rejection, reject};
use crate::topology::{KeyClass, KeyValue, TableEntry, TableKind, Topology};

type R<T> = Result<T, Rejection>;

/// What the planner reads besides the statement.
#[derive(Clone, Copy)]
pub struct PlanCtx<'a> {
    /// The tenant's layout.
    pub topo: &'a dyn Topology,
    /// The session's current schema (unqualified table names), physical.
    pub default_schema: Option<&'a str>,
    /// The cluster of the session's own backend connection (where
    /// unsharded tables live).
    pub session_cluster: &'a str,
    /// `router.max_route_values`.
    pub max_route_values: usize,
    /// `router.lookup_batch_size`.
    pub lookup_batch: usize,
    /// How `INSERT`s treat `AUTO_INCREMENT` columns that take ID blocks.
    pub ids: IdMode,
}

/// How `INSERT`s treat an `AUTO_INCREMENT` column that takes chapter 19's
/// ID blocks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IdMode {
    /// The session's `sql_mode` holds `NO_AUTO_VALUE_ON_ZERO`: `0` is a
    /// value, not a request for one.
    pub no_auto_value_on_zero: bool,
    /// The values were written in ([`crate::plan::IdFill::finish`]): plan
    /// the statement as it is.
    pub filled: bool,
}

impl std::fmt::Debug for PlanCtx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanCtx")
            .field("default_schema", &self.default_schema)
            .field("session_cluster", &self.session_cluster)
            .finish_non_exhaustive()
    }
}

/// Plans `sql`.
#[must_use]
pub fn plan(sql: &str, cx: &PlanCtx<'_>) -> Plan {
    if !cx.topo.mentions(sql) {
        return Plan::NotRouted;
    }
    if let Some(rest) = strip_explain_sprig(sql) {
        return match plan(rest, cx) {
            Plan::NotRouted => Plan::Reject(Rejection::new(
                Reason::Unsupported,
                "EXPLAIN FORMAT=SPRIG of a statement over unsharded tables",
            )),
            p => Plan::Explain(Box::new(p)),
        };
    }
    // sqlparser doesn't read MariaDB's `LOCK IN SHARE MODE` (and MariaDB
    // 11.8 rejects `FOR SHARE`): plan without it, then put it back on
    // every SELECT the plan sends.
    let (body, lock) = split_share_lock(sql);
    match plan_text(body, cx) {
        Ok(mut p) => {
            if let Some(lock) = lock {
                add_suffix(&mut p, lock);
            }
            p
        }
        Err(r) => Plan::Reject(r),
    }
}

/// R3: `sql` as a gathered read, after the executor refused its plan at
/// run time (39420 `why`: a lookup join of a string with a number, a group
/// key or sort key of a type the gateway can't compare, a sum beyond 38
/// digits). A gathered read the guard refuses, or a statement that isn't
/// a `SELECT`, keeps `why`.
#[must_use]
pub fn plan_gathered(sql: &str, cx: &PlanCtx<'_>, why: &Rejection) -> Plan {
    let (body, lock) = split_share_lock(sql);
    if lock.is_some() || !gather::lifts(why) {
        return Plan::Reject(why.clone());
    }
    match parse(body) {
        Ok(stmt @ Statement::Query(_)) => gather::plan(&stmt, body, cx, why),
        _ => Plan::Reject(why.clone()),
    }
}

/// A prepared statement's template, parsed once (chapter 17, "Prepared
/// statements"): the gateway caches it per statement text and plans each
/// execution from it with the parameters' values, so an execution never
/// parses SQL.
#[derive(Debug, Clone)]
pub struct Template {
    stmt: Statement,
    /// The text the spans refer to (the template without a trailing
    /// `LOCK IN SHARE MODE`).
    body: String,
    lock: Option<&'static str>,
    /// Placeholder positions, by span start: the n-th `?` of the text.
    params: usize,
}

impl Template {
    /// The number of `?` placeholders.
    #[must_use]
    pub fn params(&self) -> usize {
        self.params
    }
}

/// A parameter value, as the planner substitutes it for a placeholder.
#[derive(Debug, Clone, PartialEq)]
pub enum ParamLit {
    /// `NULL`.
    Null,
    /// An integer.
    Int(i128),
    /// A floating-point number (a `DOUBLE` literal, `1.5e0`).
    Double(f64),
    /// A string in the connection's character set (`_<charset> X'…'`).
    Str {
        /// The bytes.
        bytes: Vec<u8>,
        /// The character set name (`utf8mb4`).
        charset: String,
    },
    /// A binary string (`X'…'`).
    Bytes(Vec<u8>),
    /// A literal the caller rendered (`TIMESTAMP'2026-01-02 03:04:05'`).
    Raw(String),
}

/// What [`plan_prepared`] decided: the plan, and whether a single-shard
/// plan's SQL still holds the client's placeholders (run it as the
/// backend's prepared statement with the client's parameters) or holds
/// the values (run it as text).
#[derive(Debug, Clone)]
pub struct PreparedPlan {
    /// The plan.
    pub plan: Plan,
    /// `Plan::Pass` SQL with the template's `?` placeholders.
    pub placeholders: bool,
}

/// Parses a prepared statement's text once; `Ok(None)` when it names no
/// table of the topology (relay it as before).
///
/// # Errors
/// The rejection (reason `parse`) for text the router's parser doesn't
/// read.
pub fn parse_template(sql: &str, topo: &dyn Topology) -> R<Option<Template>> {
    if !topo.mentions(sql) {
        return Ok(None);
    }
    let (body, lock) = split_share_lock(sql);
    let stmt = parse(body)?;
    let mut n = 0usize;
    let _ = visit_expressions(&stmt, |e| {
        if let Expr::Value(v) = e
            && matches!(v.value, Value::Placeholder(_))
        {
            n += 1;
        }
        ControlFlow::<()>::Continue(())
    });
    Ok(Some(Template {
        stmt,
        body: body.to_owned(),
        lock,
        params: n,
    }))
}

impl ParamLit {
    /// The literal as SQL text (as [`plan_prepared`] writes it in).
    #[must_use]
    pub fn sql(&self) -> String {
        let hex = |b: &[u8]| {
            let mut h = String::with_capacity(b.len() * 2 + 3);
            h.push_str("X'");
            for x in b {
                let _ = write!(h, "{x:02X}");
            }
            h.push('\'');
            h
        };
        match self {
            Self::Null => "NULL".into(),
            Self::Int(i) => i.to_string(),
            Self::Double(f) if f.is_finite() => format!("{f:e}"),
            Self::Double(_) => "NULL".into(),
            Self::Str { bytes, charset } => format!("_{charset} {}", hex(bytes)),
            Self::Bytes(b) => hex(b),
            Self::Raw(t) => t.clone(),
        }
    }
}

/// `sql` with its `?` placeholders (outside quotes, backticks and
/// comments) replaced by `params` in order: the text a prepared
/// statement's execution amounts to (chapter 18 records it for a held
/// branch's redo). `None` when the counts differ.
#[must_use]
pub fn render_params(sql: &str, params: &[ParamLit]) -> Option<String> {
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 16 * params.len());
    let mut next = params.iter();
    let mut i = 0;
    let mut last = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b'\'' | b'"' | b'`' => {
                i += 1;
                while i < b.len() && b[i] != c {
                    if b[i] == b'\\' && c != b'`' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'#' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
            }
            b'?' => {
                out.push_str(&sql[last..i]);
                out.push_str(&next.next()?.sql());
                i += 1;
                last = i;
            }
            _ => i += 1,
        }
    }
    out.push_str(&sql[last.min(sql.len())..]);
    next.next().is_none().then_some(out)
}

fn param_expr(p: &ParamLit) -> Expr {
    let hex = |b: &[u8]| {
        let mut h = String::with_capacity(b.len() * 2);
        for x in b {
            let _ = write!(h, "{x:02X}");
        }
        h
    };
    match p {
        ParamLit::Null => Expr::value(Value::Null),
        ParamLit::Int(i) => Expr::value(Value::Number(i.to_string(), false)),
        ParamLit::Double(f) => {
            if f.is_finite() {
                Expr::value(Value::Number(format!("{f:e}"), false))
            } else {
                Expr::value(Value::Null)
            }
        }
        ParamLit::Str { bytes, charset } => Expr::Prefixed {
            prefix: Ident::new(format!("_{charset}")),
            value: Box::new(Expr::value(Value::HexStringLiteral(hex(bytes)))),
        },
        ParamLit::Bytes(b) => Expr::value(Value::HexStringLiteral(hex(b))),
        ParamLit::Raw(t) => Parser::new(&MySqlDialect {})
            .try_with_sql(t)
            .and_then(|mut p| p.parse_expr())
            .unwrap_or_else(|_| Expr::value(Value::Null)),
    }
}

/// Plans one execution of a prepared statement: its placeholders replaced
/// by `params` (in text order) in a copy of the parsed template. A
/// single-shard plan is spliced from the template text and keeps its
/// placeholders; other plans render the values.
#[must_use]
pub fn plan_prepared(t: &Template, params: &[ParamLit], cx: &PlanCtx<'_>) -> PreparedPlan {
    let mut spans = Vec::with_capacity(t.params);
    let _ = visit_expressions(&t.stmt, |e| {
        if let Expr::Value(v) = e
            && matches!(v.value, Value::Placeholder(_))
        {
            spans.push((v.span.start.line, v.span.start.column));
        }
        ControlFlow::<()>::Continue(())
    });
    let mut order = spans.clone();
    order.sort_unstable();
    let mut stmt = t.stmt.clone();
    let _ = visit_expressions_mut(&mut stmt, |e| {
        if let Expr::Value(v) = e
            && matches!(v.value, Value::Placeholder(_))
        {
            let at = (v.span.start.line, v.span.start.column);
            let i = order.binary_search(&at).unwrap_or(usize::MAX);
            *e = params
                .get(i)
                .map_or_else(|| Expr::value(Value::Null), param_expr);
        }
        ControlFlow::<()>::Continue(())
    });
    if params.len() != t.params {
        return PreparedPlan {
            plan: Plan::Reject(Rejection::new(
                Reason::Unsupported,
                "a parameter count that doesn't match the statement",
            )),
            placeholders: false,
        };
    }
    let src = Src {
        text: &t.body,
        template: true,
        kept: Cell::new(false),
    };
    let mut plan = plan_stmt(&stmt, &src, cx).unwrap_or_else(Plan::Reject);
    // R3: a gathered read runs the template's text with the values written
    // in where its placeholders were.
    if let Plan::Reject(r) = &plan
        && matches!(stmt, Statement::Query(_))
        && gather::lifts(r)
    {
        plan = match fill_placeholders(&t.body, &spans, params) {
            Some(text) => gather::plan(&stmt, &text, cx, r),
            None => Plan::Reject(r.clone()),
        };
    }
    if let Some(lock) = t.lock {
        add_suffix(&mut plan, lock);
    }
    PreparedPlan {
        placeholders: matches!(plan, Plan::Pass(_)) && src.kept.get(),
        plan,
    }
}

/// `text` with the `?` placeholder at each of `spans` (line and column, as
/// the parser recorded them) replaced by its parameter's literal, in text
/// order; `None` when a position doesn't hold a `?`.
fn fill_placeholders(text: &str, spans: &[(u64, u64)], params: &[ParamLit]) -> Option<String> {
    let mut at: Vec<(usize, usize)> = Vec::with_capacity(spans.len());
    let mut order = spans.to_vec();
    order.sort_unstable();
    for (i, (line, col)) in order.iter().enumerate() {
        let b = byte_at(text, *line, *col)?;
        if text.as_bytes().get(b) != Some(&b'?') {
            return None;
        }
        at.push((b, i));
    }
    let mut out = String::with_capacity(text.len() + 16 * at.len());
    let mut last = 0;
    for (b, i) in at {
        out.push_str(text.get(last..b)?);
        out.push_str(&param_expr(params.get(i)?).to_string());
        last = b + 1;
    }
    out.push_str(text.get(last..)?);
    Some(out)
}

/// A trailing `LOCK IN SHARE MODE`, split off.
fn split_share_lock(sql: &str) -> (&str, Option<&'static str>) {
    let t = sql.trim_end().trim_end_matches(';').trim_end();
    let words: Vec<(usize, &str)> = t
        .split_whitespace()
        .map(|w| (w.as_ptr() as usize - t.as_ptr() as usize, w))
        .collect();
    if let [.., (at, l), (_, i), (_, s), (_, m)] = words.as_slice()
        && l.eq_ignore_ascii_case("LOCK")
        && i.eq_ignore_ascii_case("IN")
        && s.eq_ignore_ascii_case("SHARE")
        && m.eq_ignore_ascii_case("MODE")
    {
        return (t[..*at].trim_end(), Some(" LOCK IN SHARE MODE"));
    }
    (sql, None)
}

fn add_suffix(p: &mut Plan, lock: &str) {
    // A locking read isn't gathered: the copies would lock nothing.
    if let Plan::Gather(g) = p {
        let r = g.fallback.clone();
        *p = Plan::Reject(r);
        return;
    }
    match p {
        Plan::Pass(t) => t.sql.push_str(lock),
        Plan::Read(r) => r.targets.iter_mut().for_each(|t| t.sql.push_str(lock)),
        Plan::LookupJoin(j) => {
            j.outer
                .targets
                .iter_mut()
                .for_each(|t| t.sql.push_str(lock));
            j.inner_template.push_str(lock);
        }
        Plan::Explain(inner) => add_suffix(inner, lock),
        Plan::ViaLookup(v) => add_suffix(&mut v.plan, lock),
        Plan::NotRouted
        | Plan::Write(_)
        | Plan::ViaParent(_)
        | Plan::NeedIds(_)
        | Plan::Gather(_)
        | Plan::Reject(_) => {}
    }
}

/// `EXPLAIN FORMAT=SPRIG <stmt>`: the statement.
fn strip_explain_sprig(sql: &str) -> Option<&str> {
    let t = sql.trim_start();
    let mut words = t.splitn(2, char::is_whitespace);
    let first = words.next()?;
    if !first.eq_ignore_ascii_case("explain") {
        return None;
    }
    let rest = words.next()?.trim_start();
    let lower = rest.get(..6)?.to_ascii_lowercase();
    if lower != "format" {
        return None;
    }
    let rest = rest[6..].trim_start().strip_prefix('=')?.trim_start();
    if !rest.get(..5)?.eq_ignore_ascii_case("sprig") {
        return None;
    }
    let after = &rest[5..];
    if !after.starts_with(char::is_whitespace) {
        return None;
    }
    Some(after.trim_start())
}

fn parse(sql: &str) -> R<Statement> {
    let parsed = Parser::new(&MySqlDialect {})
        .with_options(ParserOptions::new().with_unescape(false))
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_statements());
    match parsed {
        Ok(mut v) if v.len() == 1 => Ok(v.remove(0)),
        Ok(_) => reject(Reason::Parse, "more than one statement"),
        Err(e) => reject(Reason::Parse, format!("the router's parser: {e}")),
    }
}

/// The text a statement was parsed from: single-shard statements are
/// spliced from it (only schema names change), never re-rendered.
struct Src<'s> {
    text: &'s str,
    /// The text is a prepared statement's template: a single-shard plan
    /// keeps its placeholders, for the backend's own prepared statement.
    template: bool,
    /// Set when a Pass target's SQL was spliced from a template, so it
    /// still holds the client's placeholders.
    kept: Cell<bool>,
}

impl<'s> Src<'s> {
    fn text(text: &'s str) -> Self {
        Self {
            text,
            template: false,
            kept: Cell::new(false),
        }
    }
}

fn plan_text(sql: &str, cx: &PlanCtx<'_>) -> R<Plan> {
    let stmt = parse(sql)?;
    plan_stmt(&stmt, &Src::text(sql), cx)
}

fn plan_stmt(stmt: &Statement, src: &Src<'_>, cx: &PlanCtx<'_>) -> R<Plan> {
    match stmt {
        Statement::Query(q) => match plan_query(stmt, q, src, cx) {
            Ok(p) => Ok(lookups::via_lookup(stmt, p, cx)),
            // R3: what the gateway's operators can't split runs once over
            // copies (a prepared statement's text is filled in by
            // `plan_prepared`).
            Err(r) if !src.template && gather::lifts(&r) => {
                Ok(gather::plan(stmt, src.text, cx, &r))
            }
            Err(r) => Err(r),
        },
        Statement::Insert(_) => plan_insert(stmt, src, cx),
        Statement::Update(_) | Statement::Delete(_) => {
            plan_update_delete(stmt, src, cx).map(|p| lookups::via_lookup(stmt, p, cx))
        }
        Statement::Explain { statement, .. } => {
            // Plain EXPLAIN: one shard's plan (chapter 17, "EXPLAIN").
            let inner = statement.to_string();
            match plan_text(&inner, cx)? {
                Plan::NotRouted => Ok(Plan::NotRouted),
                Plan::Pass(t) => Ok(Plan::Pass(Target {
                    sql: format!("EXPLAIN {}", t.sql),
                    ..t
                })),
                Plan::Read(r) => first_explain(&r.targets),
                Plan::Write(w) => first_explain(&w.targets),
                Plan::LookupJoin(j) => first_explain(&j.outer.targets),
                Plan::ViaLookup(v) => match &v.plan {
                    Plan::Read(r) => first_explain(&r.targets),
                    Plan::Write(w) => first_explain(&w.targets),
                    _ => reject(Reason::Unsupported, "no target to explain"),
                },
                Plan::ViaParent(_) => reject(
                    Reason::Unsupported,
                    "EXPLAIN of an INSERT whose rows follow their parents",
                ),
                // The values aren't taken for an EXPLAIN: the plan with
                // the first rows' placeholder values 1, 2, …
                Plan::NeedIds(f) => match f.finish(1, 1, cx) {
                    Plan::Pass(t) => Ok(Plan::Pass(Target {
                        sql: format!("EXPLAIN {}", t.sql),
                        ..t
                    })),
                    Plan::Write(w) => first_explain(&w.targets),
                    p @ Plan::Reject(_) => Ok(p),
                    _ => reject(
                        Reason::Unsupported,
                        "EXPLAIN of an INSERT whose rows follow their parents",
                    ),
                },
                // Plain EXPLAIN of a gathered read: refused as before.
                Plan::Gather(g) => Ok(Plan::Reject(g.fallback.clone())),
                p @ (Plan::Reject(_) | Plan::Explain(_)) => Ok(p),
            }
        }
        _ => {
            let mut any = false;
            let _ = visit_relations(stmt, |n| {
                if resolve_name(n, cx).is_some() {
                    any = true;
                }
                ControlFlow::<()>::Continue(())
            });
            if any {
                reject(Reason::Unsupported, "this statement on a sharded table")
            } else {
                Ok(Plan::NotRouted)
            }
        }
    }
}

fn first_explain(targets: &[Target]) -> R<Plan> {
    let Some(t) = targets.first() else {
        return reject(Reason::Unsupported, "no target to explain");
    };
    Ok(Plan::Pass(Target {
        sql: format!("EXPLAIN {}", t.sql),
        ..t.clone()
    }))
}

// ---------------------------------------------------------------------------
// Names and occurrences

/// An occurrence of a table name: the first identifier's position.
type Loc = (u64, u64);

fn loc_of(n: &ObjectName) -> Loc {
    match n.0.first() {
        Some(ObjectNamePart::Identifier(i)) => (i.span.start.line, i.span.start.column),
        _ => (0, 0),
    }
}

fn ident_parts(n: &ObjectName) -> Option<Vec<&Ident>> {
    n.0.iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

/// The topology entry a relation name stands for, with its default schema.
fn resolve_name<'a>(n: &ObjectName, cx: &PlanCtx<'a>) -> Option<(&'a TableEntry, String)> {
    let parts = ident_parts(n)?;
    let (schema, table) = match parts.as_slice() {
        [t] => (
            cx.default_schema?.to_ascii_lowercase(),
            t.value.to_ascii_lowercase(),
        ),
        [s, t] => (s.value.to_ascii_lowercase(), t.value.to_ascii_lowercase()),
        _ => return None,
    };
    let e = cx.topo.table(&schema, &table)?;
    Some((e, schema))
}

/// Renders `stmt` with each listed table occurrence moved to its schema.
fn render(
    stmt: &Statement,
    occ: &HashMap<Loc, String>,
    by_name: &HashMap<(String, String), String>,
) -> String {
    let mut s = stmt.clone();
    let _ = visit_relations_mut(&mut s, |n| {
        if let Some(schema) = occ.get(&loc_of(n))
            && let Some(parts) = ident_parts(n)
            && let Some(t) = parts.last()
        {
            let t = (*t).clone();
            *n = ObjectName(vec![
                ObjectNamePart::Identifier(Ident::with_quote('`', schema.clone())),
                ObjectNamePart::Identifier(t),
            ]);
        }
        ControlFlow::<()>::Continue(())
    });
    if !by_name.is_empty() {
        let _ = visit_expressions_mut(&mut s, |e| {
            if let Expr::CompoundIdentifier(parts) = e
                && parts.len() == 3
            {
                let key = (
                    parts[0].value.to_ascii_lowercase(),
                    parts[1].value.to_ascii_lowercase(),
                );
                if let Some(schema) = by_name.get(&key) {
                    parts[0] = Ident::with_quote('`', schema.clone());
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    s.to_string()
}

/// The byte offset of a parser location (1-based line, 1-based column in
/// characters) in `text`.
fn byte_at(text: &str, line: u64, col: u64) -> Option<usize> {
    let mut start = 0usize;
    for _ in 1..line {
        start += text.get(start..)?.find('\n')? + 1;
    }
    let rest = text.get(start..)?;
    let c = usize::try_from(col.checked_sub(1)?).ok()?;
    if c <= rest.len() && rest.as_bytes()[..c].is_ascii() {
        return Some(start + c);
    }
    let mut n = 0;
    for (i, _) in rest.char_indices() {
        if n == c {
            return Some(start + i);
        }
        n += 1;
    }
    (n == c).then_some(start + rest.len())
}

/// The byte range of `id` in `text`, checked to hold that identifier.
fn ident_range(text: &str, id: &Ident) -> Option<(usize, usize)> {
    let (a, b) = (id.span.start, id.span.end);
    if a.line == 0 || b.line == 0 {
        return None;
    }
    let s = byte_at(text, a.line, a.column)?;
    let e = byte_at(text, b.line, b.column)?;
    let got = text.get(s..e)?;
    let bare = got
        .strip_prefix('`')
        .and_then(|g| g.strip_suffix('`'))
        .unwrap_or(got);
    (!id.value.contains('`') && bare.eq_ignore_ascii_case(&id.value)).then_some((s, e))
}

fn quoted(schema: &str) -> String {
    format!("`{}`", schema.replace('`', "``"))
}

/// `text` with each listed table occurrence moved to its schema by editing
/// the text at the parser's spans: what [`render`] does, without printing
/// the statement again (chapter 17's `splice_names`). `None` when a span
/// is missing or doesn't hold the name, so the caller renders instead.
fn splice(
    text: &str,
    stmt: &Statement,
    occ: &HashMap<Loc, String>,
    by_name: &HashMap<(String, String), String>,
) -> Option<String> {
    let mut edits: Vec<(usize, usize, String)> = Vec::with_capacity(occ.len());
    let mut ok = true;
    let _ = visit_relations(stmt, |n| {
        if let Some(schema) = occ.get(&loc_of(n)) {
            match ident_parts(n).as_deref() {
                Some([t]) => match ident_range(text, t) {
                    Some((b, _)) => edits.push((b, b, format!("{}.", quoted(schema)))),
                    None => ok = false,
                },
                Some([q, _]) => match ident_range(text, q) {
                    Some((b, e)) => edits.push((b, e, quoted(schema))),
                    None => ok = false,
                },
                _ => ok = false,
            }
        }
        ControlFlow::<()>::Continue(())
    });
    if !by_name.is_empty() {
        let _ = visit_expressions(stmt, |e| {
            if let Expr::CompoundIdentifier(parts) = e
                && parts.len() == 3
                && let Some(schema) = by_name.get(&(
                    parts[0].value.to_ascii_lowercase(),
                    parts[1].value.to_ascii_lowercase(),
                ))
            {
                match ident_range(text, &parts[0]) {
                    Some((b, e)) => edits.push((b, e, quoted(schema))),
                    None => ok = false,
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    if !ok {
        return None;
    }
    edits.sort_by_key(|e| (e.0, e.1));
    edits.dedup_by_key(|e| (e.0, e.1));
    let mut out = String::with_capacity(text.len() + 48 * edits.len());
    let mut at = 0;
    for (b, e, r) in edits {
        if b < at {
            return None;
        }
        out.push_str(text.get(at..b)?);
        out.push_str(&r);
        at = e;
    }
    out.push_str(text.get(at..)?);
    Some(out)
}

/// A single-shard statement's SQL: spliced from the source text when the
/// statement is the one parsed from it, else rendered.
fn pass_sql(
    stmt: &Statement,
    src: Option<&Src<'_>>,
    occ: &HashMap<Loc, String>,
    by_name: &HashMap<(String, String), String>,
) -> String {
    if let Some(src) = src
        && let Some(t) = splice(src.text, stmt, occ, by_name)
    {
        if src.template {
            src.kept.set(true);
        }
        return t;
    }
    render(stmt, occ, by_name)
}

// ---------------------------------------------------------------------------
// SELECT blocks: relations, conjuncts, co-location

/// A table in a SELECT block's FROM.
#[derive(Debug, Clone)]
struct Rel<'a> {
    entry: Option<&'a TableEntry>,
    default_schema: String,
    table: String,
    alias: Option<String>,
    loc: Loc,
    /// The null-supplying side of an outer join.
    nullable: bool,
    /// The FROM item of the block it sits in (comma-joined items).
    item: usize,
}

impl Rel<'_> {
    fn sharded(&self) -> bool {
        self.entry.is_some_and(|e| !e.is_reference())
    }
    fn reference(&self) -> bool {
        self.entry.is_some_and(TableEntry::is_reference)
    }
    fn names(&self, q: &str) -> bool {
        let q = q.to_ascii_lowercase();
        match &self.alias {
            Some(a) => *a == q,
            None => self.table == q,
        }
    }
}

/// Shards of one group: all, or a set of indexes into `Group::shards`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Tgt {
    All,
    Set(BTreeSet<usize>),
}

impl Tgt {
    fn and(self, o: Self) -> Self {
        match (self, o) {
            (Self::All, x) | (x, Self::All) => x,
            (Self::Set(a), Self::Set(b)) => Self::Set(a.intersection(&b).copied().collect()),
        }
    }
}

/// One analysed SELECT block.
#[derive(Debug)]
struct Block<'a, 'q> {
    rels: Vec<Rel<'a>>,
    where_conj: Vec<&'q Expr>,
    inner_on: Vec<&'q Expr>,
    /// Conjuncts of `LEFT JOIN … ON`, by the joined relation.
    left_on: Vec<(usize, Vec<&'q Expr>)>,
    /// `RIGHT JOIN`, `USING`, `NATURAL`, a derived table or a table
    /// function among the FROM items: only a single-shard plan is exact.
    complex: bool,
    /// Union-find parent per relation (co-location).
    parent: Vec<usize>,
    /// Target per relation.
    tgt: Vec<Tgt>,
}

fn conjuncts<'q>(e: &'q Expr, out: &mut Vec<&'q Expr>) {
    match e {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        Expr::Nested(inner) => conjuncts(inner, out),
        other => out.push(other),
    }
}

fn relation<'a>(tf: &TableFactor, cx: &PlanCtx<'a>, item: usize) -> Option<Rel<'a>> {
    let TableFactor::Table {
        name, alias, args, ..
    } = tf
    else {
        return None;
    };
    if args.is_some() {
        return None;
    }
    let parts = ident_parts(name)?;
    let table = parts.last()?.value.to_ascii_lowercase();
    let (entry, default_schema) = match resolve_name(name, cx) {
        Some((e, s)) => (Some(e), s),
        None => (
            None,
            match parts.as_slice() {
                [s, _] => s.value.to_ascii_lowercase(),
                _ => cx.default_schema.unwrap_or_default().to_ascii_lowercase(),
            },
        ),
    };
    Some(Rel {
        entry,
        default_schema,
        table,
        alias: alias.as_ref().map(|a| a.name.value.to_ascii_lowercase()),
        loc: loc_of(name),
        nullable: false,
        item,
    })
}

#[allow(clippy::unnecessary_wraps)] // rejections from analysis come with R3's forms
fn analyse<'a, 'q>(select: &'q Select, cx: &PlanCtx<'a>) -> R<Block<'a, 'q>> {
    let mut b = Block {
        rels: Vec::new(),
        where_conj: Vec::new(),
        inner_on: Vec::new(),
        left_on: Vec::new(),
        complex: false,
        parent: Vec::new(),
        tgt: Vec::new(),
    };
    for (item, twj) in select.from.iter().enumerate() {
        match relation(&twj.relation, cx, item) {
            Some(r) => b.rels.push(r),
            None => b.complex = true,
        }
        for j in &twj.joins {
            let Some(mut r) = relation(&j.relation, cx, item) else {
                b.complex = true;
                continue;
            };
            let on = |c: &'q JoinConstraint| match c {
                JoinConstraint::On(e) => Some(e),
                _ => None,
            };
            match &j.join_operator {
                JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::CrossJoin(c) => {
                    match c {
                        JoinConstraint::On(e) => conjuncts(e, &mut b.inner_on),
                        JoinConstraint::None => {}
                        _ => b.complex = true,
                    }
                }
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => {
                    if !matches!(c, JoinConstraint::On(_) | JoinConstraint::None) {
                        b.complex = true;
                    }
                    r.nullable = true;
                    let mut v = Vec::new();
                    if let Some(e) = on(c) {
                        conjuncts(e, &mut v);
                    }
                    b.left_on.push((b.rels.len(), v));
                }
                _ => b.complex = true,
            }
            b.rels.push(r);
        }
    }
    if let Some(w) = &select.selection {
        conjuncts(w, &mut b.where_conj);
    }
    b.parent = (0..b.rels.len()).collect();
    // Co-location: equalities between key columns of one group.
    let mut eqs: Vec<&Expr> = b.where_conj.clone();
    eqs.extend(b.inner_on.iter().copied());
    for (_, v) in &b.left_on {
        eqs.extend(v.iter().copied());
    }
    for e in eqs {
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = e
            && let Some((a, c)) = key_rel(&b.rels, left)
                .zip(key_rel(&b.rels, right))
                .or_else(|| parent_edge(&b.rels, left, right))
            && a != c
            && let (Some(ea), Some(ec)) = (b.rels[a].entry, b.rels[c].entry)
            && ea.group.same_layout(&ec.group)
        {
            let (ra, rc) = (find(&mut b.parent, a), find(&mut b.parent, c));
            b.parent[ra] = rc;
        }
    }
    // Targets: conjuncts that restrict the output rows.
    let mut restricting: Vec<&Expr> = b.where_conj.clone();
    restricting.extend(b.inner_on.iter().copied());
    let mut tgt = Vec::with_capacity(b.rels.len());
    for (i, r) in b.rels.iter().enumerate() {
        let mut t = Tgt::All;
        if let Some(e) = r.entry.filter(|e| !e.is_reference()) {
            for c in &restricting {
                if let Some(s) = key_values(&b.rels, i, e, c, cx) {
                    t = t.and(Tgt::Set(s));
                }
            }
        }
        tgt.push(t);
    }
    // A component's shards: the intersection over its members.
    let n = b.rels.len();
    for i in 0..n {
        let ri = find(&mut b.parent, i);
        if ri != i {
            let t = std::mem::replace(&mut tgt[i], Tgt::All);
            let merged = std::mem::replace(&mut tgt[ri], Tgt::All).and(t);
            tgt[ri] = merged;
        }
    }
    for i in 0..n {
        let ri = find(&mut b.parent, i);
        if ri != i {
            tgt[i] = tgt[ri].clone();
        }
    }
    b.tgt = tgt;
    Ok(b)
}

fn find(p: &mut [usize], i: usize) -> usize {
    let mut r = i;
    while p[r] != r {
        r = p[r];
    }
    let mut j = i;
    while p[j] != r {
        let n = p[j];
        p[j] = r;
        j = n;
    }
    r
}

/// The relation a column reference belongs to, by qualifier, or by being
/// some relation's key column when unqualified (MariaDB would reject an
/// ambiguous name, so a valid statement's unqualified key column is that
/// relation's).
fn col_rel(rels: &[Rel<'_>], e: &Expr) -> Option<(usize, String)> {
    match e {
        Expr::Nested(x) => col_rel(rels, x),
        Expr::Identifier(c) => {
            let c = c.value.to_ascii_lowercase();
            if rels.len() == 1 {
                return Some((0, c));
            }
            let mut found = rels
                .iter()
                .enumerate()
                .filter(|(_, r)| r.entry.and_then(TableEntry::key) == Some(c.as_str()));
            let first = found.next()?;
            if found.next().is_some() {
                return None;
            }
            Some((first.0, c))
        }
        Expr::CompoundIdentifier(p) if p.len() == 2 => {
            let i = rels.iter().position(|r| r.names(&p[0].value))?;
            Some((i, p[1].value.to_ascii_lowercase()))
        }
        Expr::CompoundIdentifier(p) if p.len() == 3 => {
            let s = p[0].value.to_ascii_lowercase();
            let t = p[1].value.to_ascii_lowercase();
            let i = rels
                .iter()
                .position(|r| r.alias.is_none() && r.table == t && r.default_schema == s)?;
            Some((i, p[2].value.to_ascii_lowercase()))
        }
        _ => None,
    }
}

/// A parent member joined to its own parent on its one-column foreign key
/// (`l.iid = i.iid`, the parent's referenced column unique): each child row
/// pairs only with its parent, which shares its shard (chapter 15's
/// `colocation_safe` through a parent chain). The two relations, child
/// first.
fn parent_edge(rels: &[Rel<'_>], x: &Expr, y: &Expr) -> Option<(usize, usize)> {
    let one = |c: &(usize, String), p: &(usize, String)| -> bool {
        let (Some(ce), Some(pe)) = (rels[c.0].entry, rels[p.0].entry) else {
            return false;
        };
        let Some(l) = &ce.parent else { return false };
        l.parent_unique
            && l.columns.len() == 1
            && l.columns[0] == c.1
            && l.parent_columns[0] == p.1
            && pe.name == l.table
            && !pe.is_reference()
            && rels[p.0].default_schema == rels[c.0].default_schema
    };
    let (a, b) = (col_rel(rels, x)?, col_rel(rels, y)?);
    if one(&a, &b) {
        Some((a.0, b.0))
    } else if one(&b, &a) {
        Some((b.0, a.0))
    } else {
        None
    }
}

/// The relation whose key column `e` is.
fn key_rel(rels: &[Rel<'_>], e: &Expr) -> Option<usize> {
    let (i, c) = col_rel(rels, e)?;
    (rels[i].entry.and_then(TableEntry::key) == Some(c.as_str())).then_some(i)
}

/// The shards a conjunct allows for relation `i`, if it restricts its key.
fn key_values(
    rels: &[Rel<'_>],
    i: usize,
    entry: &TableEntry,
    c: &Expr,
    cx: &PlanCtx<'_>,
) -> Option<BTreeSet<usize>> {
    let class = entry.group.key_class;
    let lits: Vec<&Expr> = match c {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq | BinaryOperator::Spaceship,
            right,
        } => {
            if key_rel(rels, left) == Some(i) {
                vec![&**right]
            } else if key_rel(rels, right) == Some(i) {
                vec![&**left]
            } else {
                return None;
            }
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if key_rel(rels, expr) == Some(i) => list.iter().collect(),
        Expr::BinaryOp {
            op: BinaryOperator::Or,
            ..
        } => {
            // An OR of equalities on the key is an IN list.
            let mut alts = Vec::new();
            if !or_equalities(rels, i, c, &mut alts) {
                return None;
            }
            alts
        }
        _ => return None,
    };
    if lits.len() > cx.max_route_values {
        return None;
    }
    let mut set = BTreeSet::new();
    for l in lits {
        let Some(v) = key_literal(l, class) else {
            // Not exactly one key value (an expression, NULL, 42.5,
            // '042' against an integer): this conjunct doesn't route.
            return None;
        };
        set.insert(entry.group.shard_of(&v)?);
    }
    Some(set)
}

fn or_equalities<'q>(rels: &[Rel<'_>], i: usize, e: &'q Expr, out: &mut Vec<&'q Expr>) -> bool {
    match e {
        Expr::Nested(x) => or_equalities(rels, i, x, out),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => or_equalities(rels, i, left, out) && or_equalities(rels, i, right, out),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if key_rel(rels, left) == Some(i) {
                out.push(right);
                true
            } else if key_rel(rels, right) == Some(i) {
                out.push(left);
                true
            } else {
                false
            }
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if key_rel(rels, expr) == Some(i) => {
            out.extend(list.iter());
            true
        }
        _ => false,
    }
}

/// The key value a literal stands for under MySQL's comparison rules, when
/// equality with it implies equality of the normalized key (chapter 17's
/// three rules).
pub(crate) fn key_literal(e: &Expr, class: KeyClass) -> Option<KeyValue> {
    match e {
        Expr::Nested(x) => key_literal(x, class),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match key_literal(expr, class)? {
            KeyValue::Int(i) if matches!(class, KeyClass::Int) => Some(KeyValue::Int(-i)),
            _ => None,
        },
        // A string with a UTF-8 or binary introducer (prepared statements'
        // parameters arrive as `_utf8mb4 X'…'`).
        Expr::Prefixed { prefix, value } => {
            let intro = prefix.value.to_ascii_lowercase();
            let bytes = match &**value {
                Expr::Value(v) => match &v.value {
                    Value::HexStringLiteral(h) => hex(h)?,
                    Value::SingleQuotedString(s) => unescape(s),
                    _ => return None,
                },
                _ => return None,
            };
            match (intro.as_str(), class) {
                ("_utf8mb4" | "_utf8mb3" | "_utf8", KeyClass::Str { .. })
                | ("_binary", KeyClass::Binary) => Some(KeyValue::Bytes(bytes)),
                ("_utf8mb4" | "_utf8mb3" | "_utf8" | "_binary", KeyClass::Int) => {
                    match int_text(std::str::from_utf8(&bytes).ok()?, true)? {
                        KeyValue::Int(i) if i.unsigned_abs() < (1u128 << 53) => {
                            Some(KeyValue::Int(i))
                        }
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        Expr::Value(v) => match (&v.value, class) {
            (Value::Number(n, _), KeyClass::Int) => int_text(n, false),
            (Value::SingleQuotedString(s) | Value::DoubleQuotedString(s), KeyClass::Int) => {
                // Compared as DOUBLE: exact below 2^53 for plain integers.
                let v = int_text(s, true)?;
                match v {
                    KeyValue::Int(i) if i.unsigned_abs() < (1u128 << 53) => Some(v),
                    _ => None,
                }
            }
            (
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
                KeyClass::Str { .. } | KeyClass::Binary,
            ) => Some(KeyValue::Bytes(unescape(s))),
            (Value::HexStringLiteral(h), KeyClass::Binary) => Some(KeyValue::Bytes(hex(h)?)),
            _ => None,
        },
        _ => None,
    }
}

/// An integer literal's value: digits with an optional `.0…` tail (a
/// decimal literal equal to an integer); `strict` (strings) allows no
/// tail.
fn int_text(s: &str, strict: bool) -> Option<KeyValue> {
    let (neg, body) = s.strip_prefix('-').map_or((false, s), |b| (true, b));
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    if int.is_empty() || !int.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !frac.is_empty() && (strict || !frac.bytes().all(|b| b == b'0')) {
        return None;
    }
    if strict && body.contains('.') {
        return None;
    }
    let v: i128 = int.parse().ok()?;
    Some(KeyValue::Int(if neg { -v } else { v }))
}

/// MySQL string-literal escapes (the parser keeps them).
fn unescape(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\\' if i + 1 < b.len() => {
                let c = match b[i + 1] {
                    b'0' => 0,
                    b'b' => 8,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'Z' => 26,
                    // `\%` and `\_` keep the backslash outside LIKE.
                    b'%' | b'_' => {
                        out.push(b'\\');
                        b[i + 1]
                    }
                    c => c,
                };
                out.push(c);
                i += 2;
            }
            b'\'' if i + 1 < b.len() && b[i + 1] == b'\'' => {
                out.push(b'\'');
                i += 2;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

fn hex(h: &str) -> Option<Vec<u8>> {
    if !h.len().is_multiple_of(2) {
        return None;
    }
    (0..h.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(h.get(i..i + 2)?, 16).ok())
        .collect()
}

// ---------------------------------------------------------------------------
// Queries

/// Every SELECT block of a query tree, the top one first.
fn blocks<'q>(q: &'q Query, out: &mut Vec<(&'q Select, bool)>, top: bool) {
    fn body<'q>(s: &'q SetExpr, out: &mut Vec<(&'q Select, bool)>, top: bool) {
        match s {
            SetExpr::Select(sel) => out.push((sel, top)),
            SetExpr::Query(q) => blocks(q, out, false),
            SetExpr::SetOperation { left, right, .. } => {
                body(left, out, false);
                body(right, out, false);
            }
            _ => {}
        }
    }
    body(&q.body, out, top);
}

fn collect_queries_stmt<'s>(stmt: &'s Statement, out: &mut Vec<&'s Query>) {
    match stmt {
        Statement::Query(q) => collect_queries(q, out),
        Statement::Insert(i) => {
            if let Some(src) = &i.source {
                collect_queries(src, out);
            }
        }
        _ => {}
    }
    let mut exprs: Vec<&'s Expr> = Vec::new();
    stmt_exprs(stmt, &mut exprs);
    for e in exprs {
        expr_queries(e, out);
    }
}

fn stmt_exprs<'s>(stmt: &'s Statement, out: &mut Vec<&'s Expr>) {
    match stmt {
        Statement::Update(u) => {
            out.extend(u.selection.iter());
            out.extend(u.assignments.iter().map(|a| &a.value));
        }
        Statement::Delete(d) => out.extend(d.selection.iter()),
        _ => {}
    }
}

fn collect_queries<'s>(q: &'s Query, out: &mut Vec<&'s Query>) {
    out.push(q);
    if let Some(w) = &q.with {
        for c in &w.cte_tables {
            collect_queries(&c.query, out);
        }
    }
    set_queries(&q.body, out);
}

fn set_queries<'s>(s: &'s SetExpr, out: &mut Vec<&'s Query>) {
    match s {
        SetExpr::Select(sel) => {
            for twj in &sel.from {
                factor_queries(&twj.relation, out);
                for j in &twj.joins {
                    factor_queries(&j.relation, out);
                    if let JoinOperator::Join(JoinConstraint::On(e))
                    | JoinOperator::Inner(JoinConstraint::On(e))
                    | JoinOperator::Left(JoinConstraint::On(e))
                    | JoinOperator::LeftOuter(JoinConstraint::On(e))
                    | JoinOperator::Right(JoinConstraint::On(e))
                    | JoinOperator::RightOuter(JoinConstraint::On(e)) = &j.join_operator
                    {
                        expr_queries(e, out);
                    }
                }
            }
            for item in &sel.projection {
                if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item
                {
                    expr_queries(e, out);
                }
            }
            if let Some(e) = &sel.selection {
                expr_queries(e, out);
            }
            if let Some(e) = &sel.having {
                expr_queries(e, out);
            }
        }
        SetExpr::Query(q) => collect_queries(q, out),
        SetExpr::SetOperation { left, right, .. } => {
            set_queries(left, out);
            set_queries(right, out);
        }
        _ => {}
    }
}

fn factor_queries<'s>(tf: &'s TableFactor, out: &mut Vec<&'s Query>) {
    match tf {
        TableFactor::Derived { subquery, .. } => collect_queries(subquery, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            factor_queries(&table_with_joins.relation, out);
            for j in &table_with_joins.joins {
                factor_queries(&j.relation, out);
            }
        }
        _ => {}
    }
}

fn expr_queries<'s>(e: &'s Expr, out: &mut Vec<&'s Query>) {
    walk_expr(e, out);
}

/// Subqueries directly inside an expression tree.
fn walk_expr<'s>(e: &'s Expr, out: &mut Vec<&'s Query>) {
    match e {
        Expr::Subquery(q) => collect_queries(q, out),
        Expr::Exists { subquery, .. } => collect_queries(subquery, out),
        Expr::InSubquery { expr, subquery, .. } => {
            walk_expr(expr, out);
            collect_queries(subquery, out);
        }
        Expr::BinaryOp { left, right, .. } => {
            walk_expr(left, out);
            walk_expr(right, out);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::Cast { expr, .. } => walk_expr(expr, out),
        Expr::InList { expr, list, .. } => {
            walk_expr(expr, out);
            for x in list {
                walk_expr(x, out);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            walk_expr(expr, out);
            walk_expr(low, out);
            walk_expr(high, out);
        }
        Expr::Function(f) => {
            if let FunctionArguments::List(l) = &f.args {
                for a in &l.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) = a {
                        walk_expr(x, out);
                    }
                }
            }
            if let FunctionArguments::Subquery(q) = &f.args {
                collect_queries(q, out);
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                walk_expr(o, out);
            }
            for c in conditions {
                walk_expr(&c.condition, out);
                walk_expr(&c.result, out);
            }
            if let Some(x) = else_result {
                walk_expr(x, out);
            }
        }
        _ => {}
    }
}

/// Per occurrence: the entry, its default schema, and its shard targets.
struct Occ<'a> {
    entry: &'a TableEntry,
    default_schema: String,
    table: String,
    tgt: Tgt,
    top: bool,
}

#[allow(clippy::too_many_lines)] // the SELECT pipeline, step by step
fn plan_query(stmt: &Statement, q: &Query, src: &Src<'_>, cx: &PlanCtx<'_>) -> R<Plan> {
    let sql = src.text;
    let mut queries = Vec::new();
    collect_queries_stmt(stmt, &mut queries);
    let mut occs: Vec<(Loc, Occ<'_>)> = Vec::new();
    let mut unsharded = false;
    let mut top_block: Option<Block<'_, '_>> = None;
    let mut complex_with_shards = false;
    for (qi, query) in queries.iter().enumerate() {
        let mut bl = Vec::new();
        blocks(query, &mut bl, qi == 0);
        for (sel, top) in bl {
            let b = analyse(sel, cx)?;
            for (i, r) in b.rels.iter().enumerate() {
                match r.entry {
                    Some(e) => occs.push((
                        r.loc,
                        Occ {
                            entry: e,
                            default_schema: r.default_schema.clone(),
                            table: r.table.clone(),
                            tgt: b.tgt[i].clone(),
                            top,
                        },
                    )),
                    None => unsharded = true,
                }
            }
            if b.complex && b.rels.iter().any(Rel::sharded) {
                complex_with_shards = true;
            }
            if top && top_block.is_none() {
                top_block = Some(b);
            }
        }
    }
    if occs.is_empty() {
        return Ok(Plan::NotRouted);
    }
    // Single shard, one cluster: pass the whole statement.
    if let Some(p) = try_pass(stmt, Some(src), &occs, unsharded, cx) {
        return Ok(p);
    }
    let lower = sql.to_ascii_lowercase();
    if lower.contains("sql_calc_found_rows") {
        return reject(Reason::FoundRows, "SQL_CALC_FOUND_ROWS across shards");
    }
    if lower.contains(":=") {
        return reject(Reason::Variables, "a variable assignment across shards");
    }
    if let Some(w) = &q.with {
        return if w.recursive {
            reject(Reason::RecursiveCte, "WITH RECURSIVE over shards")
        } else {
            reject(Reason::DerivedTable, "a CTE over shards")
        };
    }
    if !matches!(&*q.body, SetExpr::Select(_)) {
        return reject(Reason::SetOperation, "a set operation over shards");
    }
    if occs.iter().any(|(_, o)| !o.top && !o.entry.is_reference()) {
        return reject(Reason::Subquery, "a subquery over shards");
    }
    if complex_with_shards {
        return reject(
            Reason::JoinCondition,
            "RIGHT JOIN, USING, NATURAL or a derived table with shards",
        );
    }
    let SetExpr::Select(select) = &*q.body else {
        return reject(Reason::SetOperation, "a set operation over shards");
    };
    let Some(mut b) = top_block else {
        return reject(Reason::Unsupported, "no SELECT block");
    };
    if select_has_window(select) {
        return reject(Reason::WindowFunction, "a window function across shards");
    }
    // Components of sharded relations.
    let mut comps: Vec<usize> = Vec::new();
    for i in 0..b.rels.len() {
        if b.rels[i].sharded() {
            let r = find(&mut b.parent, i);
            if !comps.contains(&r) {
                comps.push(r);
            }
        }
    }
    match comps.len() {
        1 => {
            let root = comps[0];
            // A preserved side without shards would repeat its rows on
            // every shard.
            for (r, _) in &b.left_on {
                if b.rels[*r].sharded()
                    && !b.rels[..*r]
                        .iter()
                        .any(|x| x.sharded() && x.item == b.rels[*r].item)
                {
                    return reject(
                        Reason::JoinCondition,
                        "an unsharded table's outer join to a sharded one",
                    );
                }
            }
            let entry = b.rels[root].entry.ok_or_else(|| {
                Rejection::new(Reason::Unsupported, "a component without a table")
            })?;
            let shards = match &b.tgt[root] {
                Tgt::All => (0..entry.group.shards.len()).collect(),
                Tgt::Set(s) => s.clone(),
            };
            let members: Vec<usize> = (0..b.rels.len())
                .filter(|&i| b.rels[i].sharded() && find(&mut b.parent, i) == root)
                .collect();
            check_local_cluster(&b, entry, &shards, unsharded, cx)?;
            build_read(stmt, q, select, &b, &members, entry, &shards, cx).map(Plan::Read)
        }
        2 => plan_lookup_join(stmt, q, select, &mut b, &comps, cx),
        _ => reject(
            Reason::JoinDepth,
            "more than two table groups in one statement",
        ),
    }
}

fn check_local_cluster(
    b: &Block<'_, '_>,
    entry: &TableEntry,
    shards: &BTreeSet<usize>,
    unsharded: bool,
    cx: &PlanCtx<'_>,
) -> R<()> {
    if unsharded
        && shards
            .iter()
            .any(|&s| entry.group.shards[s].cluster != cx.session_cluster)
    {
        return reject(
            Reason::CrossCluster,
            "a join with an unsharded table on another cluster",
        );
    }
    // A reference table joins inside each shard: its cluster needs a copy.
    for r in b.rels.iter().filter(|r| r.reference()) {
        let Some(e) = r.entry else { continue };
        if shards
            .iter()
            .any(|&s| !has_copy(e, &entry.group.shards[s].cluster))
        {
            return reject(
                Reason::CrossCluster,
                "a reference table without a copy on a shard's cluster",
            );
        }
    }
    Ok(())
}

/// Whether reference table `e` has a copy on `cluster`.
fn has_copy(e: &TableEntry, cluster: &str) -> bool {
    e.group.shards.iter().any(|s| s.cluster == cluster)
}

fn select_has_window(select: &Select) -> bool {
    let mut found = false;
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item {
            let _ = sqlparser::ast::visit_expressions(e, |x| {
                if let Expr::Function(f) = x
                    && f.over.is_some()
                {
                    found = true;
                }
                ControlFlow::<()>::Continue(())
            });
        }
    }
    found
}

/// Every sharded occurrence on exactly one shard, all on one cluster (the
/// session's when unsharded tables join in): the statement as it is.
fn try_pass(
    stmt: &Statement,
    src: Option<&Src<'_>>,
    occs: &[(Loc, Occ<'_>)],
    unsharded: bool,
    cx: &PlanCtx<'_>,
) -> Option<Plan> {
    let mut cluster: Option<&str> = None;
    let mut shard_id = 0;
    let mut at = crate::plan::ShardAt::default();
    let mut map = HashMap::new();
    let mut by_name = HashMap::new();
    for (loc, o) in occs {
        match &o.entry.kind {
            TableKind::Sharded { .. } => {
                let Tgt::Set(s) = &o.tgt else { return None };
                let mut it = s.iter();
                let (Some(&i), None) = (it.next(), it.next()) else {
                    return None;
                };
                let loc_s = &o.entry.group.shards[i];
                if cluster.is_some_and(|c| c != loc_s.cluster) {
                    return None;
                }
                cluster = Some(&loc_s.cluster);
                shard_id = loc_s.shard;
                at = o.entry.group.at(i);
                map.insert(*loc, loc_s.schema.clone());
                by_name.insert(
                    (o.default_schema.clone(), o.table.clone()),
                    loc_s.schema.clone(),
                );
            }
            TableKind::Reference { schema } => {
                map.insert(*loc, schema.clone());
                by_name.insert((o.default_schema.clone(), o.table.clone()), schema.clone());
            }
        }
    }
    let refs: Vec<&TableEntry> = occs
        .iter()
        .map(|(_, o)| o.entry)
        .filter(|e| e.is_reference())
        .collect();
    // Only reference tables: the session's cluster if it has a copy of
    // each, else the first cluster that does.
    let cluster = match cluster {
        Some(c) => c,
        None if unsharded || refs.iter().all(|e| has_copy(e, cx.session_cluster)) => {
            cx.session_cluster
        }
        None => refs
            .first()?
            .group
            .shards
            .iter()
            .map(|s| s.cluster.as_str())
            .find(|c| refs.iter().all(|e| has_copy(e, c)))?,
    };
    if unsharded && cluster != cx.session_cluster {
        return None;
    }
    if refs.iter().any(|e| !has_copy(e, cluster)) {
        return None;
    }
    Some(Plan::Pass(Target {
        cluster: cluster.to_owned(),
        shard: shard_id,
        sql: pass_sql(stmt, src, &map, &by_name),
        at,
    }))
}

// ---------------------------------------------------------------------------
// Reads over one component

/// Hidden select-list items, as SQL text.
#[derive(Debug, Default)]
struct Hidden(Vec<String>);

impl Hidden {
    fn add(&mut self, text: String) -> ColRef {
        if let Some(i) = self.0.iter().position(|t| *t == text) {
            return ColRef::Hidden(i);
        }
        self.0.push(text);
        ColRef::Hidden(self.0.len() - 1)
    }
    /// `WEIGHT_STRING(e LEVEL 1..3)` and `COLLATION(e)`.
    fn weights(&mut self, e: &str) -> [ColRef; 4] {
        let [a, b, c] = [1, 2, 3].map(|k| self.add(format!("WEIGHT_STRING({e} LEVEL {k})")));
        [a, b, c, self.add(format!("COLLATION({e})"))]
    }
}

fn placeholder(i: usize) -> String {
    format!("__SPRIG_HIDDEN_{i}__")
}

/// Appends the hidden placeholders to a select list.
fn with_hidden(proj: &mut Vec<SelectItem>, hidden: &Hidden) {
    for i in 0..hidden.0.len() {
        proj.push(SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(
            placeholder(i),
        ))));
    }
}

/// Replaces the placeholders with the hidden items' text.
fn fill_hidden(mut sql: String, hidden: &Hidden) -> String {
    for (i, t) in hidden.0.iter().enumerate() {
        sql = sql.replacen(&placeholder(i), &format!("{t} AS __sprig_h{}", i + 1), 1);
    }
    sql
}

/// What an `ORDER BY` or `GROUP BY` item refers to: a visible column (when
/// its position is known) and the expression.
fn resolve_item<'q>(e: &'q Expr, proj: &'q [SelectItem]) -> R<(Option<usize>, &'q Expr)> {
    let wildcard_before = |i: usize| {
        proj[..=i.min(proj.len().saturating_sub(1))]
            .iter()
            .any(|p| {
                matches!(
                    p,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
                )
            })
    };
    if let Expr::Value(v) = e
        && let Value::Number(n, _) = &v.value
    {
        let pos: usize = n
            .parse()
            .map_err(|_| Rejection::new(Reason::Ordering, "a non-integer position"))?;
        if pos == 0 || pos > proj.len() || wildcard_before(pos - 1) {
            return reject(Reason::Ordering, "a position over a wildcard");
        }
        return match &proj[pos - 1] {
            SelectItem::UnnamedExpr(x) | SelectItem::ExprWithAlias { expr: x, .. } => {
                Ok((Some(pos - 1), x))
            }
            _ => reject(Reason::Ordering, "a position over a wildcard"),
        };
    }
    if let Expr::Identifier(id) = e {
        for (i, p) in proj.iter().enumerate() {
            if let SelectItem::ExprWithAlias { expr, alias } = p
                && alias.value.eq_ignore_ascii_case(&id.value)
            {
                return Ok(((!wildcard_before(i)).then_some(i), expr));
            }
        }
    }
    for (i, p) in proj.iter().enumerate() {
        if let SelectItem::UnnamedExpr(x) | SelectItem::ExprWithAlias { expr: x, .. } = p
            && x == e
            && !wildcard_before(i)
        {
            return Ok((Some(i), x));
        }
    }
    Ok((None, e))
}

fn non_deterministic(e: &Expr) -> bool {
    let mut found = false;
    let _ = sqlparser::ast::visit_expressions(e, |x| {
        if let Expr::Function(f) = x {
            let n = f.name.to_string().to_ascii_uppercase();
            if matches!(
                n.as_str(),
                "RAND" | "UUID" | "UUID_SHORT" | "SYSDATE" | "NOW" | "CURRENT_TIMESTAMP"
            ) {
                found = true;
            }
        }
        ControlFlow::<()>::Continue(())
    });
    found
}

/// `LIMIT n OFFSET m` as numbers.
fn limits(q: &Query) -> R<(u64, Option<u64>)> {
    let num = |e: &Expr| -> R<u64> {
        if let Expr::Value(v) = e
            && let Value::Number(n, _) = &v.value
            && let Ok(x) = n.parse::<u64>()
        {
            return Ok(x);
        }
        reject(Reason::Unsupported, "a LIMIT that isn't a number")
    };
    match &q.limit_clause {
        None => Ok((0, None)),
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            if !limit_by.is_empty() {
                return reject(Reason::Unsupported, "LIMIT BY");
            }
            let l = limit.as_ref().map(num).transpose()?;
            let o = offset.as_ref().map(|o| num(&o.value)).transpose()?;
            Ok((o.unwrap_or(0), l))
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            Ok((num(offset)?, Some(num(limit)?)))
        }
    }
}

fn pushed_limit(offset: u64, limit: Option<u64>) -> Option<u64> {
    limit.map(|l| l.saturating_add(offset))
}

fn set_limit(q: &mut Query, n: Option<u64>) {
    q.limit_clause = n.map(|n| LimitClause::LimitOffset {
        limit: Some(Expr::value(Value::Number(n.to_string(), false))),
        offset: None,
        limit_by: Vec::new(),
    });
}

const AGGREGATES: &[&str] = &[
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "GROUP_CONCAT",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
    "STD",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "VARIANCE",
    "VAR_POP",
    "VAR_SAMP",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
];

fn is_aggregate(f: &Function) -> bool {
    f.over.is_none() && AGGREGATES.contains(&f.name.to_string().to_ascii_uppercase().as_str())
}

fn has_aggregate(e: &Expr) -> bool {
    let mut found = false;
    let _ = sqlparser::ast::visit_expressions(e, |x| {
        if let Expr::Function(f) = x
            && is_aggregate(f)
        {
            found = true;
        }
        ControlFlow::<()>::Continue(())
    });
    found
}

/// The per-shard statements of a read over `shards`.
fn targets_for(
    stmt: &Statement,
    b: &Block<'_, '_>,
    members: &[usize],
    entry: &TableEntry,
    shards: &BTreeSet<usize>,
) -> Vec<Target> {
    shards
        .iter()
        .map(|&s| {
            let loc = &entry.group.shards[s];
            let mut map = HashMap::new();
            let mut by_name = HashMap::new();
            for (i, r) in b.rels.iter().enumerate() {
                let schema = match r.entry.map(|e| &e.kind) {
                    Some(TableKind::Sharded { .. }) if members.contains(&i) => loc.schema.clone(),
                    Some(TableKind::Reference { schema }) => schema.clone(),
                    _ => continue,
                };
                map.insert(r.loc, schema.clone());
                by_name.insert((r.default_schema.clone(), r.table.clone()), schema);
            }
            Target {
                cluster: loc.cluster.clone(),
                shard: loc.shard,
                sql: render(stmt, &map, &by_name),
                at: entry.group.at(s),
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn build_read(
    stmt: &Statement,
    q: &Query,
    select: &Select,
    b: &Block<'_, '_>,
    members: &[usize],
    entry: &TableEntry,
    shards: &BTreeSet<usize>,
    cx: &PlanCtx<'_>,
) -> R<ReadPlan> {
    let _ = cx;
    let (offset, limit) = limits(q)?;
    let order_items: Vec<&sqlparser::ast::OrderByExpr> = match &q.order_by {
        None => Vec::new(),
        Some(ob) => match &ob.kind {
            OrderByKind::Expressions(v) => v.iter().collect(),
            OrderByKind::All(_) => return reject(Reason::Ordering, "ORDER BY ALL"),
        },
    };
    for o in &order_items {
        if o.options.nulls_first.is_some()
            || o.with_fill.is_some()
            || matches!(o.options.sort, Some(sqlparser::ast::OrderBySort::Using(_)))
        {
            return reject(Reason::Ordering, "NULLS FIRST or LAST");
        }
        if non_deterministic(&o.expr) {
            return reject(Reason::Ordering, "a non-deterministic ORDER BY");
        }
    }
    let group_exprs: Vec<&Expr> = match &select.group_by {
        GroupByExpr::Expressions(v, mods) => {
            if !mods.is_empty() {
                return reject(Reason::Aggregate, "WITH ROLLUP across shards");
            }
            v.iter().collect()
        }
        GroupByExpr::All(_) => return reject(Reason::Aggregate, "GROUP BY ALL"),
    };
    let distinct = match &select.distinct {
        None | Some(Distinct::All) => false,
        Some(Distinct::Distinct) => true,
        Some(Distinct::On(_)) => return reject(Reason::Aggregate, "DISTINCT ON"),
    };
    let proj_aggs = select.projection.iter().any(|p| match p {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => has_aggregate(e),
        _ => false,
    }) || select.having.as_ref().is_some_and(has_aggregate);
    let aggregated = proj_aggs || !group_exprs.is_empty() || select.having.is_some();
    // Groups that contain a key never span shards: push the whole block.
    let is_member_key = |e: &Expr| key_rel(&b.rels, e).is_some_and(|i| members.contains(&i));
    let key_grouped = !group_exprs.is_empty()
        && group_exprs.iter().any(|g| {
            let resolved = resolve_item(g, &select.projection)
                .map(|(_, x)| x)
                .unwrap_or(g);
            is_member_key(resolved)
        });
    let key_distinct = distinct
        && !proj_aggs
        && group_exprs.is_empty()
        && select.projection.iter().any(|p| match p {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                is_member_key(e)
            }
            _ => false,
        });
    let group = entry.group.id;
    if !aggregated && !distinct || key_grouped || key_distinct {
        // Scatter or Merge.
        let mut hidden = Hidden::default();
        let mut order = Vec::new();
        for o in &order_items {
            let (vis, e) = resolve_item(&o.expr, &select.projection)?;
            let text = e.to_string();
            let value = match vis {
                Some(i) => ColRef::Visible(i),
                None => hidden.add(text.clone()),
            };
            order.push(SortKey {
                cols: KeyCols {
                    value,
                    weights: hidden.weights(&text),
                },
                desc: matches!(o.options.sort, Some(sqlparser::ast::OrderBySort::Desc)),
            });
        }
        // MariaDB sorts GROUP BY output by the group columns.
        if order.is_empty() && key_grouped {
            for g in &group_exprs {
                let (vis, e) = resolve_item(g, &select.projection)?;
                let text = e.to_string();
                let value = match vis {
                    Some(i) => ColRef::Visible(i),
                    None => hidden.add(text.clone()),
                };
                order.push(SortKey {
                    cols: KeyCols {
                        value,
                        weights: hidden.weights(&text),
                    },
                    desc: false,
                });
            }
        }
        let pushed = pushed_limit(offset, limit);
        let mut q2 = q.clone();
        set_limit(&mut q2, pushed);
        if let SetExpr::Select(s2) = &mut *q2.body {
            with_hidden(&mut s2.projection, &hidden);
        }
        let stmt2 = replace_query(stmt, q2);
        let targets = targets_for(&stmt2, b, members, entry, shards)
            .into_iter()
            .map(|t| Target {
                sql: fill_hidden(t.sql, &hidden),
                ..t
            })
            .collect();
        return Ok(ReadPlan {
            kind: if order.is_empty() {
                ReadKind::Scatter
            } else {
                ReadKind::Merge
            },
            targets,
            hidden: hidden.0.len(),
            order,
            agg: None,
            offset,
            limit,
            pushed_limit: pushed,
            group,
        });
    }
    // Split aggregation.
    let mut hidden = Hidden::default();
    let mut groups: Vec<KeyCols> = Vec::new();
    let mut group_src: Vec<(Option<usize>, &Expr)> = Vec::new();
    if distinct {
        if proj_aggs || !group_exprs.is_empty() {
            return reject(Reason::Aggregate, "DISTINCT with aggregates across shards");
        }
        for (i, p) in select.projection.iter().enumerate() {
            match p {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    group_src.push((Some(i), e));
                }
                _ => return reject(Reason::Aggregate, "DISTINCT * across shards"),
            }
        }
    } else {
        for g in &group_exprs {
            group_src.push(resolve_item(g, &select.projection)?);
        }
    }
    for (vis, e) in &group_src {
        let text = e.to_string();
        let value = match vis {
            Some(i) => ColRef::Visible(*i),
            None => hidden.add(text.clone()),
        };
        groups.push(KeyCols {
            value,
            weights: hidden.weights(&text),
        });
    }
    let mut aggs: Vec<(AggCall, Expr)> = Vec::new();
    let mut outputs = Vec::new();
    for (i, p) in select.projection.iter().enumerate() {
        let (SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. }) = p else {
            return reject(Reason::Aggregate, "a wildcard with aggregates");
        };
        if let Expr::Function(f) = e
            && is_aggregate(f)
        {
            let k = agg_call(f, e, Some(ColRef::Visible(i)), &mut hidden, &mut aggs)?;
            outputs.push(OutCol::Agg(k));
        } else if group_src.iter().any(|(v, g)| *v == Some(i) || *g == e)
            || matches!(e, Expr::Value(_))
        {
            outputs.push(OutCol::First);
        } else if has_aggregate(e) {
            return reject(
                Reason::Aggregate,
                format!("an expression over aggregates: {e}"),
            );
        } else {
            return reject(Reason::Aggregate, format!("a column outside GROUP BY: {e}"));
        }
    }
    let having = match &select.having {
        None => None,
        Some(h) => Some(having_pred(
            h,
            select,
            &group_src,
            &groups,
            &mut hidden,
            &mut aggs,
        )?),
    };
    let mut order = Vec::new();
    for o in &order_items {
        let desc = matches!(o.options.sort, Some(sqlparser::ast::OrderBySort::Desc));
        let (vis, e) = resolve_item(&o.expr, &select.projection)?;
        if let Some(i) = vis {
            match outputs[i] {
                OutCol::Agg(k) => {
                    order.push(FinalSort::Agg(k, desc));
                    continue;
                }
                OutCol::First => {
                    if let Some(g) = group_src.iter().position(|(v, x)| *v == Some(i) || *x == e) {
                        order.push(FinalSort::Group(g, desc));
                        continue;
                    }
                    if matches!(e, Expr::Value(_)) {
                        continue;
                    }
                }
            }
        }
        if let Expr::Function(f) = e
            && is_aggregate(f)
        {
            let k = agg_call(f, e, None, &mut hidden, &mut aggs)?;
            order.push(FinalSort::Agg(k, desc));
        } else if let Some(g) = group_src.iter().position(|(_, x)| *x == e) {
            order.push(FinalSort::Group(g, desc));
        } else {
            return reject(Reason::Ordering, format!("ORDER BY {e} after aggregation"));
        }
    }
    let mut q2 = q.clone();
    q2.order_by = None;
    q2.limit_clause = None;
    if let SetExpr::Select(s2) = &mut *q2.body {
        s2.having = None;
        with_hidden(&mut s2.projection, &hidden);
    }
    let stmt2 = replace_query(stmt, q2);
    let targets = targets_for(&stmt2, b, members, entry, shards)
        .into_iter()
        .map(|t| Target {
            sql: fill_hidden(t.sql, &hidden),
            ..t
        })
        .collect();
    Ok(ReadPlan {
        kind: ReadKind::Aggregate,
        targets,
        hidden: hidden.0.len(),
        order: Vec::new(),
        agg: Some(AggPlan {
            groups,
            aggs: aggs.into_iter().map(|(a, _)| a).collect(),
            outputs,
            having,
            order,
        }),
        offset,
        limit,
        pushed_limit: None,
        group,
    })
}

fn replace_query(stmt: &Statement, q: Query) -> Statement {
    match stmt {
        Statement::Query(_) => Statement::Query(Box::new(q)),
        other => other.clone(),
    }
}

/// An aggregate call's partials: an existing equal call, or new ones.
fn agg_call(
    f: &Function,
    e: &Expr,
    visible: Option<ColRef>,
    hidden: &mut Hidden,
    aggs: &mut Vec<(AggCall, Expr)>,
) -> R<usize> {
    if let Some(k) = aggs.iter().position(|(_, x)| x == e) {
        return Ok(k);
    }
    let name = f.name.to_string().to_ascii_uppercase();
    let FunctionArguments::List(list) = &f.args else {
        return reject(Reason::Aggregate, format!("{name} without arguments"));
    };
    if matches!(
        list.duplicate_treatment,
        Some(sqlparser::ast::DuplicateTreatment::Distinct)
    ) {
        return reject(
            Reason::Aggregate,
            format!("{name}(DISTINCT …) across shards"),
        );
    }
    if !list.clauses.is_empty()
        || f.filter.is_some()
        || !f.within_group.is_empty()
        || list.args.len() != 1
    {
        return reject(Reason::Aggregate, format!("this {name} across shards"));
    }
    let arg = match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => x.to_string(),
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if name == "COUNT" => "*".to_owned(),
        _ => return reject(Reason::Aggregate, format!("this {name} argument")),
    };
    let own = |hidden: &mut Hidden| visible.unwrap_or_else(|| hidden.add(e.to_string()));
    let call = match name.as_str() {
        "COUNT" => AggCall {
            func: AggFunc::Count,
            value: own(hidden),
            count: None,
            weights: None,
            scale_from: None,
        },
        "SUM" => AggCall {
            func: AggFunc::Sum,
            value: own(hidden),
            count: None,
            weights: None,
            scale_from: None,
        },
        "MIN" | "MAX" => {
            let value = own(hidden);
            let w = hidden.weights(&e.to_string());
            AggCall {
                func: if name == "MIN" {
                    AggFunc::Min
                } else {
                    AggFunc::Max
                },
                value,
                count: None,
                weights: Some(w),
                scale_from: None,
            }
        }
        "AVG" => {
            let sum = hidden.add(format!("SUM({arg})"));
            let count = hidden.add(format!("COUNT({arg})"));
            let scale_from = Some(visible.unwrap_or_else(|| hidden.add(e.to_string())));
            AggCall {
                func: AggFunc::Avg,
                value: sum,
                count: Some(count),
                weights: None,
                scale_from,
            }
        }
        _ => return reject(Reason::Aggregate, format!("{name} across shards")),
    };
    aggs.push((call, e.clone()));
    Ok(aggs.len() - 1)
}

fn having_pred(
    h: &Expr,
    select: &Select,
    group_src: &[(Option<usize>, &Expr)],
    groups: &[KeyCols],
    hidden: &mut Hidden,
    aggs: &mut Vec<(AggCall, Expr)>,
) -> R<Pred> {
    let operand = |e: &Expr, hidden: &mut Hidden, aggs: &mut Vec<(AggCall, Expr)>| -> R<Operand> {
        match e {
            Expr::Nested(x) => having_operand(x, select, group_src, groups, hidden, aggs),
            _ => having_operand(e, select, group_src, groups, hidden, aggs),
        }
    };
    Ok(match h {
        Expr::Nested(x) => having_pred(x, select, group_src, groups, hidden, aggs)?,
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Pred::And(
                Box::new(having_pred(left, select, group_src, groups, hidden, aggs)?),
                Box::new(having_pred(right, select, group_src, groups, hidden, aggs)?),
            ),
            BinaryOperator::Or => Pred::Or(
                Box::new(having_pred(left, select, group_src, groups, hidden, aggs)?),
                Box::new(having_pred(right, select, group_src, groups, hidden, aggs)?),
            ),
            _ => {
                let op = match op {
                    BinaryOperator::Eq => CmpOp::Eq,
                    BinaryOperator::NotEq => CmpOp::Ne,
                    BinaryOperator::Lt => CmpOp::Lt,
                    BinaryOperator::LtEq => CmpOp::Le,
                    BinaryOperator::Gt => CmpOp::Gt,
                    BinaryOperator::GtEq => CmpOp::Ge,
                    _ => return reject(Reason::Aggregate, format!("HAVING operator {op}")),
                };
                Pred::Cmp(
                    op,
                    operand(left, hidden, aggs)?,
                    operand(right, hidden, aggs)?,
                )
            }
        },
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Pred::Not(Box::new(having_pred(
            expr, select, group_src, groups, hidden, aggs,
        )?)),
        Expr::IsNull(x) => Pred::IsNull(operand(x, hidden, aggs)?, false),
        Expr::IsNotNull(x) => Pred::IsNull(operand(x, hidden, aggs)?, true),
        other => return reject(Reason::Aggregate, format!("HAVING {other}")),
    })
}

fn having_operand(
    e: &Expr,
    select: &Select,
    group_src: &[(Option<usize>, &Expr)],
    groups: &[KeyCols],
    hidden: &mut Hidden,
    aggs: &mut Vec<(AggCall, Expr)>,
) -> R<Operand> {
    match e {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => Ok(Operand::Num(n.clone())),
            Value::Null => Ok(Operand::Null),
            _ => reject(Reason::Aggregate, "a HAVING literal that isn't a number"),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match &**expr {
            Expr::Value(v) => match &v.value {
                Value::Number(n, _) => Ok(Operand::Num(format!("-{n}"))),
                _ => reject(Reason::Aggregate, "a HAVING literal that isn't a number"),
            },
            _ => reject(Reason::Aggregate, "a HAVING expression"),
        },
        Expr::Function(f) if is_aggregate(f) => {
            let k = agg_call(f, e, None, hidden, aggs)?;
            Ok(Operand::Agg(k))
        }
        Expr::Identifier(id) => {
            for (i, p) in select.projection.iter().enumerate() {
                if let SelectItem::ExprWithAlias { expr, alias } = p
                    && alias.value.eq_ignore_ascii_case(&id.value)
                {
                    if let Expr::Function(f) = expr
                        && is_aggregate(f)
                    {
                        let k = agg_call(f, expr, Some(ColRef::Visible(i)), hidden, aggs)?;
                        return Ok(Operand::Agg(k));
                    }
                    if let Some(g) = group_src
                        .iter()
                        .position(|(v, x)| *v == Some(i) || *x == expr)
                    {
                        return Ok(Operand::Col(groups[g].value));
                    }
                }
            }
            match group_src.iter().position(|(_, x)| *x == e) {
                Some(g) => Ok(Operand::Col(groups[g].value)),
                None => reject(Reason::Aggregate, format!("HAVING {e}")),
            }
        }
        _ => match group_src.iter().position(|(_, x)| *x == e) {
            Some(g) => Ok(Operand::Col(groups[g].value)),
            None => reject(Reason::Aggregate, format!("HAVING {e}")),
        },
    }
}

// ---------------------------------------------------------------------------
// Lookup joins

/// Column references in `e`, by relation; `None` for one the planner can't
/// place (unqualified with several relations, or inside a subquery).
fn refs(rels: &[Rel<'_>], e: &Expr) -> Option<BTreeSet<usize>> {
    let mut out = BTreeSet::new();
    let mut ok = true;
    let _ = sqlparser::ast::visit_expressions(e, |x| {
        match x {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => match col_rel(rels, x) {
                Some((i, _)) if !matches!(x, Expr::Identifier(_)) || rels.len() == 1 => {
                    out.insert(i);
                }
                _ => ok = false,
            },
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => ok = false,
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    ok.then_some(out)
}

fn and_all(v: &[&Expr]) -> Option<Expr> {
    let mut it = v.iter();
    let first = (*it.next()?).clone();
    Some(it.fold(first, |acc, e| Expr::BinaryOp {
        left: Box::new(acc),
        op: BinaryOperator::And,
        right: Box::new(Expr::Nested(Box::new((*e).clone()))),
    }))
}

#[allow(clippy::too_many_lines)] // one join, both sides
fn plan_lookup_join(
    _stmt: &Statement,
    q: &Query,
    select: &Select,
    b: &mut Block<'_, '_>,
    comps: &[usize],
    cx: &PlanCtx<'_>,
) -> R<Plan> {
    if b.rels.len() != 2 || comps.len() != 2 {
        return reject(
            Reason::JoinCondition,
            "a lookup join of more than two tables",
        );
    }
    if !matches!(select.group_by, GroupByExpr::Expressions(ref v, _) if v.is_empty())
        || select.having.is_some()
        || select.distinct.is_some()
        || select.projection.iter().any(|p| match p {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                has_aggregate(e)
            }
            _ => true,
        })
    {
        return reject(
            Reason::Aggregate,
            "aggregates or wildcards over a lookup join",
        );
    }
    let left = !b.left_on.is_empty();
    let mut cross: Vec<&Expr> = Vec::new();
    let mut side: [Vec<&Expr>; 2] = [Vec::new(), Vec::new()];
    let mut on_inner: Vec<&Expr> = Vec::new();
    for c in b.where_conj.iter().chain(b.inner_on.iter()) {
        let r = refs(&b.rels, c)
            .ok_or_else(|| Rejection::new(Reason::JoinCondition, format!("can't place {c}")))?;
        match r.len() {
            0 => side[0].push(c),
            1 => side[r.iter().next().copied().unwrap_or(0)].push(c),
            _ => cross.push(c),
        }
    }
    if left {
        for c in &b.left_on[0].1 {
            let r = refs(&b.rels, c)
                .ok_or_else(|| Rejection::new(Reason::JoinCondition, format!("can't place {c}")))?;
            match r.len() {
                1 if r.contains(&1) => on_inner.push(c),
                2 => cross.push(c),
                _ => {
                    return reject(
                        Reason::JoinCondition,
                        "an outer-side condition in LEFT JOIN … ON",
                    );
                }
            }
        }
        if !side[1].is_empty() {
            return reject(
                Reason::JoinCondition,
                "a WHERE condition on the inner side of a LEFT JOIN",
            );
        }
    }
    let [eq] = cross.as_slice() else {
        return reject(
            Reason::JoinCondition,
            "a lookup join needs exactly one equality between the tables",
        );
    };
    let Expr::BinaryOp {
        left: l,
        op: BinaryOperator::Eq,
        right: r,
    } = eq
    else {
        return reject(Reason::JoinCondition, format!("join condition {eq}"));
    };
    let (Some((li, _)), Some((ri, _))) = (col_rel(&b.rels, l), col_rel(&b.rels, r)) else {
        return reject(Reason::JoinCondition, format!("join condition {eq}"));
    };
    if li == ri {
        return reject(Reason::JoinCondition, format!("join condition {eq}"));
    }
    // Outer: the preserved side, else the one restricted by its key.
    let outer = if left {
        0
    } else {
        usize::from(matches!(b.tgt[1], Tgt::Set(_)) && matches!(b.tgt[0], Tgt::All))
    };
    let inner = 1 - outer;
    let (x, y) = if li == outer {
        (&**l, &**r)
    } else {
        (&**r, &**l)
    };
    let mut inner_conds: Vec<&Expr> = side[inner].clone();
    inner_conds.extend(on_inner);
    // Output columns.
    let mut outer_cols: Vec<Expr> = Vec::new();
    let mut inner_cols: Vec<Expr> = Vec::new();
    let mut outputs = Vec::new();
    for p in &select.projection {
        let (SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. }) = p else {
            return reject(Reason::JoinCondition, "a wildcard over a lookup join");
        };
        let rs = refs(&b.rels, e)
            .ok_or_else(|| Rejection::new(Reason::JoinCondition, format!("can't place {e}")))?;
        let item = match p {
            SelectItem::ExprWithAlias { expr, alias } => SelectItem::ExprWithAlias {
                expr: expr.clone(),
                alias: alias.clone(),
            },
            _ => SelectItem::UnnamedExpr(e.clone()),
        };
        let _ = item;
        match rs.len() {
            0 | 1 if !rs.contains(&inner) => {
                outputs.push(JoinCol::Outer(outer_cols.len()));
                outer_cols.push(e.clone());
            }
            1 => {
                outputs.push(JoinCol::Inner(inner_cols.len()));
                inner_cols.push(e.clone());
            }
            _ => return reject(Reason::JoinCondition, format!("{e} reads both tables")),
        }
    }
    // Order: outer columns only.
    let order_items: Vec<sqlparser::ast::OrderByExpr> = match &q.order_by {
        None => Vec::new(),
        Some(ob) => match &ob.kind {
            OrderByKind::Expressions(v) => v.clone(),
            OrderByKind::All(_) => return reject(Reason::Ordering, "ORDER BY ALL"),
        },
    };
    for o in &order_items {
        let resolved = resolve_item(&o.expr, &select.projection)?.1;
        let rs = refs(&b.rels, resolved)
            .ok_or_else(|| Rejection::new(Reason::JoinCondition, "ORDER BY over a lookup join"))?;
        if rs.contains(&inner) {
            return reject(
                Reason::JoinCondition,
                "ORDER BY on the inner side of a lookup join",
            );
        }
    }
    let (offset, limit) = limits(q)?;
    // The outer read: its columns, then x and its weights.
    let mut oq = q.clone();
    oq.limit_clause = None;
    let mut proj: Vec<SelectItem> = outer_cols
        .iter()
        .enumerate()
        .map(|(i, e)| SelectItem::ExprWithAlias {
            expr: e.clone(),
            alias: Ident::new(format!("__sprig_o{i}")),
        })
        .collect();
    proj.push(SelectItem::ExprWithAlias {
        expr: x.clone(),
        alias: Ident::new("__sprig_x"),
    });
    let n_outer = outer_cols.len();
    // ORDER BY items rewritten to the expressions (aliases of the original
    // select list don't exist on the outer side).
    if let Some(ob) = &mut oq.order_by
        && let OrderByKind::Expressions(v) = &mut ob.kind
    {
        for o in v.iter_mut() {
            o.expr = resolve_item(&o.expr, &select.projection)?.1.clone();
        }
    }
    let outer_twj = select
        .from
        .first()
        .ok_or_else(|| Rejection::new(Reason::JoinCondition, "no FROM"))?;
    let outer_factor = if outer == 0 {
        outer_twj.relation.clone()
    } else if let Some(j) = outer_twj.joins.first() {
        j.relation.clone()
    } else {
        select
            .from
            .get(1)
            .map(|t| t.relation.clone())
            .ok_or_else(|| Rejection::new(Reason::JoinCondition, "no outer table"))?
    };
    let inner_factor = if inner == 0 {
        outer_twj.relation.clone()
    } else if let Some(j) = outer_twj.joins.first() {
        j.relation.clone()
    } else {
        select
            .from
            .get(1)
            .map(|t| t.relation.clone())
            .ok_or_else(|| Rejection::new(Reason::JoinCondition, "no inner table"))?
    };
    if let SetExpr::Select(s2) = &mut *oq.body {
        s2.projection = proj;
        s2.from = vec![sqlparser::ast::TableWithJoins {
            relation: outer_factor,
            joins: Vec::new(),
        }];
        s2.selection = and_all(&side[outer]);
    }
    let ostmt = Statement::Query(Box::new(oq.clone()));
    let outer_entry = b.rels[outer]
        .entry
        .ok_or_else(|| Rejection::new(Reason::JoinCondition, "unsharded outer"))?;
    let oblock = Block {
        rels: vec![b.rels[outer].clone()],
        where_conj: Vec::new(),
        inner_on: Vec::new(),
        left_on: Vec::new(),
        complex: false,
        parent: vec![0],
        tgt: vec![b.tgt[outer].clone()],
    };
    let shards = match &b.tgt[outer] {
        Tgt::All => (0..outer_entry.group.shards.len()).collect(),
        Tgt::Set(s) => s.clone(),
    };
    let SetExpr::Select(osel) = &*oq.body else {
        return reject(Reason::Unsupported, "outer side");
    };
    let mut outer_plan = build_read(&ostmt, &oq, osel, &oblock, &[0], outer_entry, &shards, cx)?;
    // x's weights ride as hidden columns: one more item each.
    let mut extra = Hidden::default();
    let xw = extra.weights(&x.to_string());
    let base_hidden = outer_plan.hidden;
    for t in &mut outer_plan.targets {
        // Insert x's weights after the existing hidden columns.
        let mut insert = String::new();
        for (i, s) in extra.0.iter().enumerate() {
            let _ = write!(insert, ", {s} AS __sprig_xw{i}");
        }
        t.sql = insert_before_from(&t.sql, &insert)?;
    }
    outer_plan.hidden += extra.0.len();
    let shift = |c: ColRef| match c {
        ColRef::Hidden(j) => ColRef::Hidden(base_hidden + j),
        v @ ColRef::Visible(_) => v,
    };
    let outer_key = KeyCols {
        value: ColRef::Visible(n_outer),
        weights: xw.map(shift),
    };
    outer_plan.offset = 0;
    outer_plan.limit = if left {
        pushed_limit(offset, limit)
    } else {
        None
    };
    // The inner statement template.
    let inner_entry = b.rels[inner]
        .entry
        .ok_or_else(|| Rejection::new(Reason::JoinCondition, "unsharded inner"))?;
    let inner_by_key = key_rel(&b.rels, y) == Some(inner);
    let TableFactor::Table { name, alias, .. } = &inner_factor else {
        return reject(Reason::JoinCondition, "inner side is not a table");
    };
    let tname = ident_parts(name)
        .and_then(|p| p.last().map(|i| i.value.clone()))
        .unwrap_or_default();
    let alias_txt = alias
        .as_ref()
        .map(|a| format!(" AS {}", a.name))
        .unwrap_or_default();
    let mut cols: Vec<String> = inner_cols.iter().map(ToString::to_string).collect();
    let ytxt = y.to_string();
    cols.push(format!("{ytxt} AS __sprig_h1"));
    for k in 1..=3 {
        cols.push(format!(
            "WEIGHT_STRING({ytxt} LEVEL {k}) AS __sprig_h{}",
            k + 1
        ));
    }
    cols.push(format!("COLLATION({ytxt}) AS __sprig_h5"));
    let mut where_txt = format!("{ytxt} IN (\u{1}K)");
    for c in &inner_conds {
        let _ = write!(where_txt, " AND ({c})");
    }
    let inner_template = format!(
        "SELECT {} FROM `\u{1}S`.`{}`{} WHERE {}",
        cols.join(", "),
        tname.replace('`', "``"),
        alias_txt,
        where_txt
    );
    let n_inner = inner_cols.len();
    let _ = inner_entry.key();
    Ok(Plan::LookupJoin(Box::new(LookupJoinPlan {
        outer: outer_plan,
        outer_key,
        inner_group: inner_entry.group.clone(),
        inner_by_key,
        inner_template,
        inner_hidden: 5,
        inner_key: KeyCols {
            value: ColRef::Visible(n_inner),
            weights: [
                ColRef::Hidden(1),
                ColRef::Hidden(2),
                ColRef::Hidden(3),
                ColRef::Hidden(4),
            ],
        },
        left,
        outputs,
        offset,
        limit,
        batch: cx.lookup_batch.max(1),
    })))
}

/// Inserts `items` at the end of the outermost select list of `sql`.
fn insert_before_from(sql: &str, items: &str) -> R<String> {
    // The outer statement is `SELECT … FROM …` that the planner rendered
    // itself; its first top-level FROM ends the select list.
    let b = sql.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < b.len() {
        let c = b[i];
        if let Some(q) = quote {
            if c == b'\\' {
                i += 2;
                continue;
            }
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' | b'"' | b'`' => quote = Some(c),
            b'(' => depth += 1,
            b')' => depth -= 1,
            b' ' if depth == 0
                && sql[i..].len() >= 6
                && sql[i + 1..i + 5].eq_ignore_ascii_case("FROM")
                && b.get(i + 5) == Some(&b' ') =>
            {
                return Ok(format!("{}{}{}", &sql[..i], items, &sql[i..]));
            }
            _ => {}
        }
        i += 1;
    }
    reject(Reason::Unsupported, "outer statement without FROM")
}

// ---------------------------------------------------------------------------
// Writes

fn plan_insert(stmt: &Statement, src_in: &Src<'_>, cx: &PlanCtx<'_>) -> R<Plan> {
    // A statement whose ID-block values were written in is no longer the
    // text it was parsed from: render it, never splice.
    let text_src: Option<&Src<'_>> = (!cx.ids.filled).then_some(src_in);
    let Statement::Insert(ins) = stmt else {
        return reject(Reason::Unsupported, "not an INSERT");
    };
    let TableObject::TableName(name) = &ins.table else {
        return reject(Reason::Unsupported, "INSERT INTO a table function");
    };
    let Some((entry, default_schema)) = resolve_name(name, cx) else {
        // The target is unsharded; a SELECT source over shards isn't.
        if let Some(src) = &ins.source {
            let mut qs = Vec::new();
            collect_queries(src, &mut qs);
            for q in qs {
                let mut bl = Vec::new();
                blocks(q, &mut bl, false);
                for (sel, _) in bl {
                    if analyse(sel, cx)?.rels.iter().any(|r| r.entry.is_some()) {
                        return reject(Reason::InsertSelect, "INSERT … SELECT from shards");
                    }
                }
            }
        }
        return Ok(Plan::NotRouted);
    };
    let table = entry.name.clone();
    let loc = loc_of(name);
    if ins.returning.is_some() {
        // Allowed on one shard only; checked below.
    }
    let (key, key_ordinal, upsert_safe) = match &entry.kind {
        TableKind::Reference { schema } => {
            if fk::graph_for(cx, &default_schema, &table).is_some()
                && (ins.replace_into || ins.on.is_some() || ins.ignore)
            {
                return reject(
                    Reason::ForeignKey,
                    "REPLACE, INSERT IGNORE or an upsert on a reference table with foreign keys",
                );
            }
            // Copies follow the primary through the change feed (chapter
            // 15's ref_follow, not built yet): a write is exact only where
            // one copy exists.
            let [only] = entry.group.shards.as_slice() else {
                return reject(
                    Reason::CrossCluster,
                    "a reference-table write on several clusters",
                );
            };
            let mut m = HashMap::new();
            m.insert(loc, schema.clone());
            let mut by = HashMap::new();
            by.insert((default_schema, table), schema.clone());
            return Ok(Plan::Pass(Target {
                cluster: only.cluster.clone(),
                shard: 0,
                sql: pass_sql(stmt, text_src, &m, &by),
                at: crate::plan::ShardAt {
                    db: entry.group.db,
                    schema: schema.clone(),
                    ..crate::plan::ShardAt::default()
                },
            }));
        }
        TableKind::Sharded {
            key,
            key_ordinal,
            upsert_safe,
        } => (key.clone(), *key_ordinal, *upsert_safe),
    };
    // Chapter 19: the AUTO_INCREMENT column's values come from the
    // gateway's ID blocks, never from a shard's own counter.
    if !cx.ids.filled
        && let Some(ai) = &entry.auto_inc
    {
        return ids::plan(stmt, src_in, entry, ai, cx);
    }
    if key.is_empty()
        && let Some(link) = &entry.parent
    {
        // Chapter 15: a parent member's rows go where their parent is.
        if fk::graph_for(cx, &default_schema, &entry.name)
            .is_some_and(|(g, t)| g.as_child(t).next().is_some())
        {
            return reject(
                Reason::ForeignKey,
                "an INSERT into a parent member whose foreign key to another group the gateway checks",
            );
        }
        return parents::plan_insert(stmt, text_src, entry, link, &default_schema, cx);
    }
    if (ins.on.is_some() || ins.replace_into) && !upsert_safe {
        return reject(
            Reason::UpsertUnique,
            "an upsert on a table whose unique keys lack the distribution key",
        );
    }
    if let Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(assignments)) = &ins.on
        && assignments.iter().any(|a| assigns_key(&a.target, &key))
    {
        return reject(Reason::KeyChange, "ON DUPLICATE KEY UPDATE of the key");
    }
    if key.is_empty() {
        return reject(
            Reason::InsertKey,
            "an INSERT into a table without a single key column (a parent member or a composite key)",
        );
    }
    lookups::check_gap(entry)?;
    let class = entry.group.key_class;
    // Chapter 15: every row carries its routing hash in `_sprig_rk`. The
    // statement is re-rendered with the column added (no splicing).
    let filled = fill_rk(ins, entry, &key, key_ordinal, class);
    let (stmt, text_src): (&Statement, Option<&Src<'_>>) = match &filled {
        Some(f) => (f, None),
        None => (stmt, text_src),
    };
    let Statement::Insert(ins) = stmt else {
        return reject(Reason::Unsupported, "not an INSERT");
    };
    // Rows and the key's position in them.
    let pos = if ins.columns.is_empty() {
        key_ordinal.ok_or_else(|| {
            Rejection::new(
                Reason::InsertKey,
                "an INSERT without a column list (the key's position is unknown)",
            )
        })?
    } else {
        ins.columns
            .iter()
            .position(|c| {
                ident_parts(c)
                    .and_then(|p| p.last().map(|i| i.value.eq_ignore_ascii_case(&key)))
                    .unwrap_or(false)
            })
            .ok_or_else(|| {
                Rejection::new(Reason::AutoIncrement, "an INSERT without the key column")
            })?
    };
    let Some(src) = &ins.source else {
        if !entry.lookups.is_empty() {
            return reject(
                Reason::Lookup,
                "INSERT … SET on a table with a unique lookup",
            );
        }
        // INSERT … SET k = v, …
        let Some(a) = ins
            .assignments
            .iter()
            .find(|a| assigns_key(&a.target, &key))
        else {
            return reject(Reason::AutoIncrement, "an INSERT without the key column");
        };
        let shard = route_value(&a.value, entry, class)?;
        let near = entry.group.shards.get(shard).map(|s| s.cluster.as_str());
        if let Some(fkw) = fk::insert_work(
            ins,
            entry,
            &[] as &[Box<Vec<Expr>>],
            &default_schema,
            near,
            cx,
        )? {
            let ts = if src_in.template { None } else { text_src };
            return Ok(Plan::Write(WritePlan {
                kind: WriteKind::Insert,
                targets: vec![one_shard_insert(
                    stmt,
                    ts,
                    entry,
                    shard,
                    loc,
                    &default_schema,
                )],
                rows: 1,
                claims: Vec::new(),
                releases: Vec::new(),
                fk: Some(fkw),
                limited: None,
            }));
        }
        return Ok(Plan::Pass(one_shard_insert(
            stmt,
            text_src,
            entry,
            shard,
            loc,
            &default_schema,
        )));
    };
    let SetExpr::Values(values) = &*src.body else {
        return reject(Reason::InsertSelect, "INSERT … SELECT into a sharded table");
    };
    let mut by_shard: Vec<(usize, Vec<usize>)> = Vec::new();
    for (ri, row) in values.rows.iter().enumerate() {
        let Some(v) = row.get(pos) else {
            return reject(Reason::AutoIncrement, "a row without the key value");
        };
        let shard = route_value(v, entry, class)?;
        match by_shard.iter_mut().find(|(s, _)| *s == shard) {
            Some((_, rows)) => rows.push(ri),
            None => by_shard.push((shard, vec![ri])),
        }
    }
    // Chapter 19: the rows' unique lookup values, claimed before the rows
    // are written, in the write's transaction.
    let claims = lookups::insert_claims(ins, entry, pos, &values.rows)?;
    // Chapter 19 (R3): the parents its rows need, locked before they are
    // written.
    let near = by_shard
        .first()
        .and_then(|(s, _)| entry.group.shards.get(*s))
        .map(|s| s.cluster.as_str());
    let fkw = fk::insert_work(ins, entry, &values.rows, &default_schema, near, cx)?;
    if by_shard.len() == 1 {
        if claims.is_empty() && fkw.is_none() {
            let t = one_shard_insert(stmt, text_src, entry, by_shard[0].0, loc, &default_schema);
            return Ok(Plan::Pass(t));
        }
        let ts = if src_in.template { None } else { text_src };
        let t = one_shard_insert(stmt, ts, entry, by_shard[0].0, loc, &default_schema);
        return Ok(Plan::Write(WritePlan {
            kind: WriteKind::Insert,
            targets: vec![t],
            rows: values.rows.len() as u64,
            claims,
            releases: Vec::new(),
            fk: fkw,
            limited: None,
        }));
    }
    if ins.returning.is_some() {
        return reject(Reason::Unsupported, "INSERT … RETURNING across shards");
    }
    // Shards on several clusters commit through chapter 18's coordinator
    // (the gateway's `ShardBackend::begin_write`).
    let mut targets = Vec::new();
    for (shard, rows) in &by_shard {
        let mut s = stmt.clone();
        if let Statement::Insert(i2) = &mut s
            && let Some(src2) = &mut i2.source
            && let SetExpr::Values(v2) = &mut *src2.body
        {
            v2.rows = rows.iter().map(|&r| values.rows[r].clone()).collect();
        }
        targets.push(one_shard_insert(
            &s,
            None,
            entry,
            *shard,
            loc,
            &default_schema,
        ));
    }
    Ok(Plan::Write(WritePlan {
        kind: WriteKind::Insert,
        targets,
        rows: values.rows.len() as u64,
        claims,
        releases: Vec::new(),
        fk: fkw,
        limited: None,
    }))
}

/// The INSERT with `_sprig_rk` added: the column (and, for an INSERT
/// without a column list, the visible columns before it) and each row's
/// routing hash; `None` when it already names `_sprig_rk`, or when a row's
/// hash can't be known here (the caller's routing refuses those rows).
fn fill_rk(
    ins: &sqlparser::ast::Insert,
    entry: &TableEntry,
    key: &str,
    key_ordinal: Option<usize>,
    class: KeyClass,
) -> Option<Statement> {
    const RK: &str = sprig_shard::rk::RK_COLUMN;
    if !entry.fill_rk {
        return None;
    }
    let col = |n: &str| ObjectName(vec![ObjectNamePart::Identifier(Ident::new(n))]);
    let named = |c: &ObjectName, n: &str| {
        ident_parts(c)
            .and_then(|p| p.last().map(|i| i.value.eq_ignore_ascii_case(n)))
            .unwrap_or(false)
    };
    if ins.columns.iter().any(|c| named(c, RK)) {
        return None;
    }
    let hash = |v: &Expr| -> Option<u64> { entry.group.hash_of(&key_literal(v, class)?) };
    let number = |h: u64| Expr::value(Value::Number(h.to_string(), false));
    let mut out = ins.clone();
    let Some(src) = &mut out.source else {
        // INSERT … SET: the key's assignment gives the hash.
        let h = ins
            .assignments
            .iter()
            .find(|a| assigns_key(&a.target, key))
            .and_then(|a| hash(&a.value))?;
        out.assignments.push(sqlparser::ast::Assignment {
            target: sqlparser::ast::AssignmentTarget::ColumnName(col(RK)),
            value: number(h),
        });
        return Some(Statement::Insert(out));
    };
    let SetExpr::Values(values) = &mut *src.body else {
        return None;
    };
    let pos = if ins.columns.is_empty() {
        // Without the table's columns no list can be written: the rows go
        // as they are and the member's back-fill covers them.
        if entry.columns.is_empty() {
            return None;
        }
        out.columns = entry.columns.iter().map(|c| col(c)).collect();
        key_ordinal?
    } else {
        ins.columns.iter().position(|c| named(c, key))?
    };
    let mut hashes = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        if row.len() != out.columns.len() {
            // A count mismatch: MariaDB's own 1136 says so.
            return None;
        }
        match row.get(pos).and_then(hash) {
            Some(h) => hashes.push(h),
            None => return None,
        }
    }
    out.columns.push(col(RK));
    for (row, h) in values.rows.iter_mut().zip(hashes) {
        row.push(number(h));
    }
    Some(Statement::Insert(out))
}

fn assigns_key(t: &sqlparser::ast::AssignmentTarget, key: &str) -> bool {
    match t {
        sqlparser::ast::AssignmentTarget::ColumnName(n) => ident_parts(n)
            .and_then(|p| p.last().map(|i| i.value.eq_ignore_ascii_case(key)))
            .unwrap_or(false),
        sqlparser::ast::AssignmentTarget::Tuple(v) => v.iter().any(|n| {
            ident_parts(n)
                .and_then(|p| p.last().map(|i| i.value.eq_ignore_ascii_case(key)))
                .unwrap_or(false)
        }),
    }
}

fn route_value(v: &Expr, entry: &TableEntry, class: KeyClass) -> R<usize> {
    if let Expr::Value(x) = v
        && matches!(x.value, Value::Null)
    {
        return reject(
            Reason::AutoIncrement,
            "a NULL key (ID blocks come with chapter 19)",
        );
    }
    if matches!(v, Expr::Identifier(i) if i.value.eq_ignore_ascii_case("DEFAULT")) {
        return reject(
            Reason::AutoIncrement,
            "a DEFAULT key (ID blocks come with chapter 19)",
        );
    }
    let kv = key_literal(v, class)
        .ok_or_else(|| Rejection::new(Reason::InsertKey, format!("key value {v}")))?;
    if matches!(kv, KeyValue::Int(0)) {
        return reject(Reason::AutoIncrement, "a zero key (AUTO_INCREMENT)");
    }
    entry
        .group
        .shard_of(&kv)
        .ok_or_else(|| Rejection::new(Reason::InsertKey, format!("key value {v}")))
}

fn one_shard_insert(
    stmt: &Statement,
    src: Option<&Src<'_>>,
    entry: &TableEntry,
    shard: usize,
    loc: Loc,
    default_schema: &str,
) -> Target {
    shard_insert(
        stmt,
        src,
        &entry.group,
        &entry.name,
        shard,
        loc,
        default_schema,
    )
}

/// `stmt` (an `INSERT` into `table`, named at `loc`) for shard `shard` of
/// `group`.
fn shard_insert(
    stmt: &Statement,
    src: Option<&Src<'_>>,
    group: &crate::topology::Group,
    table: &str,
    shard: usize,
    loc: Loc,
    default_schema: &str,
) -> Target {
    let s = &group.shards[shard];
    let mut m = HashMap::new();
    m.insert(loc, s.schema.clone());
    let mut by = HashMap::new();
    by.insert(
        (default_schema.to_owned(), table.to_owned()),
        s.schema.clone(),
    );
    Target {
        cluster: s.cluster.clone(),
        shard: s.shard,
        sql: pass_sql(stmt, src, &m, &by),
        at: group.at(shard),
    }
}

fn plan_update_delete(stmt: &Statement, src: &Src<'_>, cx: &PlanCtx<'_>) -> R<Plan> {
    // Chapter 19 (R3): a write the planner doesn't split over a table
    // whose foreign keys the gateway keeps would skip them.
    let check_single_or = |stmt: &Statement, src: &Src<'_>, cx: &PlanCtx<'_>, reason: Reason| {
        if fk::touches(stmt, cx) {
            return reject(
                Reason::ForeignKey,
                "a multi-table write, or one with a subquery or on a reference table, over tables with foreign keys the gateway keeps",
            );
        }
        check_single_or(stmt, src, cx, reason)
    };
    let (kind, twjs, selection, order_by, limit, assignments): (
        WriteKind,
        Vec<&sqlparser::ast::TableWithJoins>,
        Option<&Expr>,
        bool,
        bool,
        Vec<&sqlparser::ast::Assignment>,
    ) = match stmt {
        Statement::Update(u) => {
            if u.from.is_some() {
                return check_single_or(stmt, src, cx, Reason::MultiTableDml);
            }
            (
                WriteKind::Update,
                vec![&u.table],
                u.selection.as_ref(),
                !u.order_by.is_empty(),
                u.limit.is_some(),
                u.assignments.iter().collect(),
            )
        }
        Statement::Delete(d) => {
            if !d.tables.is_empty() || d.using.is_some() {
                return check_single_or(stmt, src, cx, Reason::MultiTableDml);
            }
            let from = match &d.from {
                FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
            };
            (
                WriteKind::Delete,
                from.iter().collect(),
                d.selection.as_ref(),
                !d.order_by.is_empty(),
                d.limit.is_some(),
                Vec::new(),
            )
        }
        _ => return reject(Reason::Unsupported, "not a write"),
    };
    if twjs.len() != 1 || !twjs[0].joins.is_empty() {
        return check_single_or(stmt, src, cx, Reason::MultiTableDml);
    }
    let Some(rel) = relation(&twjs[0].relation, cx, 0) else {
        return check_single_or(stmt, src, cx, Reason::MultiTableDml);
    };
    let Some(entry) = rel.entry else {
        // An unsharded target; subqueries over shards aren't exact.
        return check_single_or(stmt, src, cx, Reason::Subquery);
    };
    if let TableKind::Reference { .. } = entry.kind {
        return check_single_or(stmt, src, cx, Reason::Subquery);
    }
    let key = entry.key().unwrap_or_default().to_owned();
    if assignments.iter().any(|a| assigns_key(&a.target, &key)) {
        return reject(Reason::KeyChange, "an UPDATE of the distribution key");
    }
    // A parent member's foreign key decides its shard and `_sprig_rk`
    // (chapter 15): changing it is a key change, unless the group has one
    // shard and no `_sprig_rk` yet (nothing depends on it).
    if let Some(link) = &entry.parent
        && (entry.fill_rk || entry.group.shards.len() > 1)
        && link
            .columns
            .iter()
            .any(|c| assignments.iter().any(|a| assigns_key(&a.target, c)))
    {
        return reject(
            Reason::KeyChange,
            "an UPDATE of the foreign key a parent member follows",
        );
    }
    lookups::check_gap(entry)?;
    lookups::check_update(entry, &|c| {
        assignments.iter().any(|a| assigns_key(&a.target, c))
    })?;
    let tail = {
        let (ob, lim) = match stmt {
            Statement::Update(u) => (
                u.order_by
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                u.limit.as_ref().map(ToString::to_string),
            ),
            Statement::Delete(d) => (
                d.order_by
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                d.limit.as_ref().map(ToString::to_string),
            ),
            _ => (Vec::new(), None),
        };
        let mut t = String::new();
        if !ob.is_empty() {
            t = format!("ORDER BY {}", ob.join(", "));
        }
        if let Some(l) = lim {
            if !t.is_empty() {
                t.push(' ');
            }
            let _ = write!(t, "LIMIT {l}");
        }
        t
    };
    let dml = fk::Dml {
        delete: kind == WriteKind::Delete,
        relation: twjs[0].relation.to_string(),
        selection,
        tail,
        assignments: assignments.clone(),
    };
    // Subqueries inside the write.
    let mut nested = Vec::new();
    collect_queries_stmt(stmt, &mut nested);
    for q in &nested {
        let mut bl = Vec::new();
        blocks(q, &mut bl, false);
        for (sel, _) in bl {
            if analyse(sel, cx)?.rels.iter().any(Rel::sharded) {
                return check_single_or(stmt, src, cx, Reason::Subquery);
            }
        }
    }
    let rels = vec![rel.clone()];
    let mut tgt = Tgt::All;
    if let Some(w) = selection {
        let mut cs = Vec::new();
        conjuncts(w, &mut cs);
        for c in cs {
            if let Some(s) = key_values(&rels, 0, entry, c, cx) {
                tgt = tgt.and(Tgt::Set(s));
            }
        }
    }
    let shards: BTreeSet<usize> = match tgt {
        Tgt::All => (0..entry.group.shards.len()).collect(),
        Tgt::Set(s) => s,
    };
    let make = |s: usize, src: Option<&Src<'_>>| {
        let loc = &entry.group.shards[s];
        let mut m = HashMap::new();
        m.insert(rel.loc, loc.schema.clone());
        let mut by = HashMap::new();
        by.insert(
            (rel.default_schema.clone(), rel.table.clone()),
            loc.schema.clone(),
        );
        Target {
            cluster: loc.cluster.clone(),
            shard: loc.shard,
            sql: pass_sql(stmt, src, &m, &by),
            at: entry.group.at(s),
        }
    };
    // Chapter 19: a DELETE reads what its rows hold in unique lookups,
    // and releases it once the transaction commits.
    let shard_list: Vec<usize> = if shards.is_empty() {
        vec![0]
    } else {
        shards.iter().copied().collect()
    };
    let releases = if kind == WriteKind::Delete {
        lookups::delete_releases(stmt, &rel, entry, &shard_list, order_by || limit)?
    } else {
        Vec::new()
    };
    // Chapter 19 (R3): parents of new child values, and the children of
    // the parent rows it changes.
    let near = entry
        .group
        .shards
        .get(shard_list[0])
        .map(|s| s.cluster.as_str());
    let fkw = fk::update_delete_work(&dml, entry, &rel.default_schema, near, cx)?;
    if (!releases.is_empty() || fkw.is_some()) && shard_list.len() == 1 {
        return Ok(Plan::Write(WritePlan {
            kind,
            targets: vec![make(shard_list[0], (!src.template).then_some(src))],
            rows: 0,
            claims: Vec::new(),
            releases,
            fk: fkw,
            limited: None,
        }));
    }
    if shards.len() == 1 {
        let s = shards.iter().next().copied().unwrap_or(0);
        return Ok(Plan::Pass(make(s, Some(src))));
    }
    if shards.is_empty() {
        // Contradictory key predicates: no row matches; any shard says so.
        return Ok(Plan::Pass(make(0, Some(src))));
    }
    if src.text.contains(":=") {
        return reject(Reason::Variables, "a variable assignment across shards");
    }
    if order_by || limit {
        // R3: a locked two-step (chapter 17's step 17).
        let lw = limited_write(
            stmt,
            entry,
            &rel,
            &dml,
            &shards,
            cx,
            &releases,
            fkw.as_deref(),
        )?;
        let targets = lw.0;
        return Ok(Plan::Write(WritePlan {
            kind,
            targets,
            rows: 0,
            claims: Vec::new(),
            releases: Vec::new(),
            fk: None,
            limited: Some(Box::new(lw.1)),
        }));
    }
    Ok(Plan::Write(WritePlan {
        kind,
        targets: shards
            .into_iter()
            .map(|s| make(s, (!src.template).then_some(src)))
            .collect(),
        rows: 0,
        claims: Vec::new(),
        releases,
        fk: fkw,
        limited: None,
    }))
}

/// R3's limited DML over several shards (`UPDATE` or `DELETE … ORDER BY
/// … LIMIT n`): the per-shard statements without the order and limit, each
/// with [`crate::plan::LIMITED_ROWS`] in its `WHERE`, and the read that
/// finds the rows. Exact only when the order is total (it names every
/// primary-key column) and the table keeps no lookup or foreign key.
#[allow(clippy::too_many_arguments)]
fn limited_write(
    stmt: &Statement,
    entry: &TableEntry,
    rel: &Rel<'_>,
    dml: &fk::Dml<'_>,
    shards: &BTreeSet<usize>,
    cx: &PlanCtx<'_>,
    releases: &[crate::plan::Release],
    fkw: Option<&crate::fk::FkWork>,
) -> R<(Vec<Target>, crate::plan::Limited)> {
    use crate::fk::ValClass;
    let refuse = |why: &str| Rejection::new(Reason::LimitedDml, why.to_owned());
    let (order, limit) = match stmt {
        Statement::Update(u) => (&u.order_by, &u.limit),
        Statement::Delete(d) => (&d.order_by, &d.limit),
        _ => return Err(refuse("not a write")),
    };
    if limit.is_none() {
        return Err(refuse("ORDER BY without LIMIT in a multi-shard write"));
    }
    if !releases.is_empty() || !entry.lookups.is_empty() || fkw.is_some() {
        return Err(refuse(
            "a limited write on a table with a unique lookup or a foreign key",
        ));
    }
    let Some(key) = entry.key() else {
        return Err(refuse("a limited write on a table without a key column"));
    };
    if entry.primary.is_empty() || entry.primary.len() != entry.primary_classes.len() {
        return Err(refuse(
            "a limited write on a table whose primary key is unknown",
        ));
    }
    let named: Vec<String> = order
        .iter()
        .filter_map(|o| match &o.expr {
            Expr::Identifier(i) => Some(i.value.to_ascii_lowercase()),
            Expr::CompoundIdentifier(v) => v.last().map(|i| i.value.to_ascii_lowercase()),
            _ => None,
        })
        .collect();
    if !entry.primary.iter().all(|c| named.contains(c)) {
        return Err(refuse(
            "ORDER BY … LIMIT in a multi-shard write whose order isn't total (it must name every primary-key column)",
        ));
    }
    let key_class = match entry.group.key_class {
        KeyClass::Int => ValClass::Num,
        KeyClass::Str { .. } => ValClass::Str,
        KeyClass::Binary => ValClass::Bin,
    };
    let q = |c: &str| format!("`{}`", c.replace('`', "``"));
    let mut classes = vec![key_class];
    classes.extend(entry.primary_classes.iter().copied());
    let mut items = vec![key_class.read_expr(&q(key))];
    items.extend(
        entry
            .primary
            .iter()
            .zip(&entry.primary_classes)
            .map(|(c, k)| k.read_expr(&q(c))),
    );
    let mut sql = format!("SELECT {} FROM {}", items.join(", "), dml.relation);
    if let Some(w) = dml.selection {
        let _ = write!(sql, " WHERE {w}");
    }
    let _ = write!(sql, " {} FOR UPDATE", dml.tail);
    let read = match plan_text(&sql, cx)? {
        p @ (Plan::Pass(_) | Plan::Read(_) | Plan::ViaLookup(_)) => p,
        Plan::Reject(r) => return Err(r),
        other => {
            return Err(refuse(&format!(
                "the rows of a limited write can't be read in order ({})",
                other.kind()
            )));
        }
    };
    // The statement without its order and limit, naming its rows.
    let mut w = stmt.clone();
    let mark = Expr::Identifier(Ident::new(crate::plan::LIMITED_ROWS));
    let with_mark = |sel: &mut Option<Expr>| {
        *sel = Some(match sel.take() {
            Some(e) => Expr::BinaryOp {
                left: Box::new(Expr::Nested(Box::new(e))),
                op: BinaryOperator::And,
                right: Box::new(mark.clone()),
            },
            None => mark.clone(),
        });
    };
    match &mut w {
        Statement::Update(u) => {
            u.order_by.clear();
            u.limit = None;
            with_mark(&mut u.selection);
        }
        Statement::Delete(d) => {
            d.order_by.clear();
            d.limit = None;
            with_mark(&mut d.selection);
        }
        _ => {}
    }
    let all: Vec<usize> = if shards.is_empty() {
        (0..entry.group.shards.len()).collect()
    } else {
        shards.iter().copied().collect()
    };
    let targets = all
        .into_iter()
        .map(|s| {
            let loc = &entry.group.shards[s];
            let mut m = HashMap::new();
            m.insert(rel.loc, loc.schema.clone());
            let mut by = HashMap::new();
            by.insert(
                (rel.default_schema.clone(), rel.table.clone()),
                loc.schema.clone(),
            );
            Target {
                cluster: loc.cluster.clone(),
                shard: loc.shard,
                sql: pass_sql(&w, None, &m, &by),
                at: entry.group.at(s),
            }
        })
        .collect();
    Ok((
        targets,
        crate::plan::Limited {
            read: Box::new(read),
            pk: entry.primary.clone(),
            classes,
            group: std::sync::Arc::clone(&entry.group),
        },
    ))
}

/// A write the planner doesn't split: exact only on one shard.
fn check_single_or(stmt: &Statement, src: &Src<'_>, cx: &PlanCtx<'_>, reason: Reason) -> R<Plan> {
    let mut queries: Vec<&Query> = Vec::new();
    collect_queries_stmt(stmt, &mut queries);
    let mut occs = Vec::new();
    let mut unsharded = false;
    // The write's own tables: route by its WHERE as a SELECT block would.
    let (twjs, selection): (Vec<&sqlparser::ast::TableWithJoins>, Option<&Expr>) = match stmt {
        Statement::Update(u) => (vec![&u.table], u.selection.as_ref()),
        Statement::Delete(d) => (
            match &d.from {
                FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v.iter().collect(),
            },
            d.selection.as_ref(),
        ),
        _ => (Vec::new(), None),
    };
    let Some(fake) = empty_select(twjs.into_iter().cloned().collect(), selection.cloned()) else {
        return reject(reason, "a write over several shards in this form");
    };
    let b = analyse(&fake, cx)?;
    for (i, r) in b.rels.iter().enumerate() {
        match r.entry {
            Some(e) => occs.push((
                r.loc,
                Occ {
                    entry: e,
                    default_schema: r.default_schema.clone(),
                    table: r.table.clone(),
                    tgt: b.tgt[i].clone(),
                    top: true,
                },
            )),
            None => unsharded = true,
        }
    }
    for q in queries {
        let mut bl = Vec::new();
        blocks(q, &mut bl, false);
        for (sel, _) in bl {
            let b = analyse(sel, cx)?;
            for (i, r) in b.rels.iter().enumerate() {
                match r.entry {
                    Some(e) => occs.push((
                        r.loc,
                        Occ {
                            entry: e,
                            default_schema: r.default_schema.clone(),
                            table: r.table.clone(),
                            tgt: b.tgt[i].clone(),
                            top: false,
                        },
                    )),
                    None => unsharded = true,
                }
            }
        }
    }
    if occs.is_empty() {
        return Ok(Plan::NotRouted);
    }
    match try_pass(stmt, Some(src), &occs, unsharded, cx) {
        Some(p) => Ok(p),
        None => reject(reason, "a write over several shards in this form"),
    }
}

fn empty_select(
    from: Vec<sqlparser::ast::TableWithJoins>,
    selection: Option<Expr>,
) -> Option<Select> {
    let Ok(Statement::Query(q)) = Parser::new(&MySqlDialect {})
        .try_with_sql("SELECT 1")
        .and_then(|mut p| p.parse_statement())
    else {
        return None;
    };
    let SetExpr::Select(s) = *q.body else {
        return None;
    };
    Some(Select {
        from,
        selection,
        ..*s
    })
}

mod fk;
mod gather;
mod ids;
mod lookups;
mod parents;

pub(crate) use lookups::decode_read;

#[cfg(test)]
mod tests;
