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

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Distinct, Expr, FromTable, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator, LimitClause, ObjectName,
    ObjectNamePart, OrderByKind, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableObject, UnaryOperator, Value, visit_expressions_mut, visit_relations, visit_relations_mut,
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
    match plan_inner(body, cx) {
        Ok(mut p) => {
            if let Some(lock) = lock {
                add_suffix(&mut p, lock);
            }
            p
        }
        Err(r) => Plan::Reject(r),
    }
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
        Plan::NotRouted | Plan::Write(_) | Plan::Reject(_) => {}
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

fn plan_inner(sql: &str, cx: &PlanCtx<'_>) -> R<Plan> {
    let stmt = parse(sql)?;
    match &stmt {
        Statement::Query(q) => plan_query(&stmt, q, sql, cx),
        Statement::Insert(_) => plan_insert(&stmt, cx),
        Statement::Update(_) | Statement::Delete(_) => plan_update_delete(&stmt, cx),
        Statement::Explain { statement, .. } => {
            // Plain EXPLAIN: one shard's plan (chapter 17, "EXPLAIN").
            let inner = statement.to_string();
            match plan_inner(&inner, cx)? {
                Plan::NotRouted => Ok(Plan::NotRouted),
                Plan::Pass(t) => Ok(Plan::Pass(Target {
                    sql: format!("EXPLAIN {}", t.sql),
                    ..t
                })),
                Plan::Read(r) => first_explain(&r.targets),
                Plan::Write(w) => first_explain(&w.targets),
                Plan::LookupJoin(j) => first_explain(&j.outer.targets),
                p @ (Plan::Reject(_) | Plan::Explain(_)) => Ok(p),
            }
        }
        _ => {
            let mut any = false;
            let _ = visit_relations(&stmt, |n| {
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
            && let (Some(a), Some(c)) = (key_rel(&b.rels, left), key_rel(&b.rels, right))
            && a != c
            && b.rels[a].entry.map(|x| x.group.id) == b.rels[c].entry.map(|x| x.group.id)
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
fn plan_query(stmt: &Statement, q: &Query, sql: &str, cx: &PlanCtx<'_>) -> R<Plan> {
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
    if let Some(p) = try_pass(stmt, &occs, unsharded, cx) {
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
    let needs_session = unsharded || b.rels.iter().any(Rel::reference);
    if needs_session
        && shards
            .iter()
            .any(|&s| entry.group.shards[s].cluster != cx.session_cluster)
    {
        return reject(
            Reason::CrossCluster,
            "a join with an unsharded table on another cluster",
        );
    }
    Ok(())
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
    occs: &[(Loc, Occ<'_>)],
    unsharded: bool,
    cx: &PlanCtx<'_>,
) -> Option<Plan> {
    let mut cluster: Option<&str> = None;
    let mut shard_id = 0;
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
    let cluster = cluster.unwrap_or(cx.session_cluster);
    if (unsharded || occs.iter().any(|(_, o)| o.entry.is_reference()))
        && cluster != cx.session_cluster
    {
        return None;
    }
    Some(Plan::Pass(Target {
        cluster: cluster.to_owned(),
        shard: shard_id,
        sql: render(stmt, &map, &by_name),
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

fn plan_insert(stmt: &Statement, cx: &PlanCtx<'_>) -> R<Plan> {
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
            if entry
                .group
                .shards
                .iter()
                .any(|s| s.cluster != cx.session_cluster)
            {
                return reject(
                    Reason::CrossCluster,
                    "a reference-table write on several clusters",
                );
            }
            let mut m = HashMap::new();
            m.insert(loc, schema.clone());
            let mut by = HashMap::new();
            by.insert((default_schema, table), schema.clone());
            return Ok(Plan::Pass(Target {
                cluster: cx.session_cluster.to_owned(),
                shard: 0,
                sql: render(stmt, &m, &by),
            }));
        }
        TableKind::Sharded {
            key,
            key_ordinal,
            upsert_safe,
        } => (key.clone(), *key_ordinal, *upsert_safe),
    };
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
    let class = entry.group.key_class;
    // Rows and the key's position in them.
    let pos = if ins.columns.is_empty() {
        key_ordinal
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
        // INSERT … SET k = v, …
        let Some(a) = ins
            .assignments
            .iter()
            .find(|a| assigns_key(&a.target, &key))
        else {
            return reject(Reason::AutoIncrement, "an INSERT without the key column");
        };
        let shard = route_value(&a.value, entry, class)?;
        return Ok(Plan::Pass(one_shard_insert(
            stmt,
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
    if by_shard.len() == 1 {
        return Ok(Plan::Pass(one_shard_insert(
            stmt,
            entry,
            by_shard[0].0,
            loc,
            &default_schema,
        )));
    }
    if ins.returning.is_some() {
        return reject(Reason::Unsupported, "INSERT … RETURNING across shards");
    }
    let cluster = &entry.group.shards[by_shard[0].0].cluster;
    if by_shard
        .iter()
        .any(|(s, _)| entry.group.shards[*s].cluster != *cluster)
    {
        return reject(
            Reason::CrossCluster,
            "a write on shards of several clusters",
        );
    }
    let mut targets = Vec::new();
    for (shard, rows) in &by_shard {
        let mut s = stmt.clone();
        if let Statement::Insert(i2) = &mut s
            && let Some(src2) = &mut i2.source
            && let SetExpr::Values(v2) = &mut *src2.body
        {
            v2.rows = rows.iter().map(|&r| values.rows[r].clone()).collect();
        }
        targets.push(one_shard_insert(&s, entry, *shard, loc, &default_schema));
    }
    Ok(Plan::Write(WritePlan {
        kind: WriteKind::Insert,
        targets,
        rows: values.rows.len() as u64,
    }))
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
    entry: &TableEntry,
    shard: usize,
    loc: Loc,
    default_schema: &str,
) -> Target {
    let s = &entry.group.shards[shard];
    let mut m = HashMap::new();
    m.insert(loc, s.schema.clone());
    let mut by = HashMap::new();
    by.insert(
        (default_schema.to_owned(), entry.name.clone()),
        s.schema.clone(),
    );
    Target {
        cluster: s.cluster.clone(),
        shard: s.shard,
        sql: render(stmt, &m, &by),
    }
}

fn plan_update_delete(stmt: &Statement, cx: &PlanCtx<'_>) -> R<Plan> {
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
                return check_single_or(stmt, cx, Reason::MultiTableDml);
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
                return check_single_or(stmt, cx, Reason::MultiTableDml);
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
        return check_single_or(stmt, cx, Reason::MultiTableDml);
    }
    let Some(rel) = relation(&twjs[0].relation, cx, 0) else {
        return check_single_or(stmt, cx, Reason::MultiTableDml);
    };
    let Some(entry) = rel.entry else {
        // An unsharded target; subqueries over shards aren't exact.
        return check_single_or(stmt, cx, Reason::Subquery);
    };
    if let TableKind::Reference { .. } = entry.kind {
        return check_single_or(stmt, cx, Reason::Subquery);
    }
    let key = entry.key().unwrap_or_default().to_owned();
    if assignments.iter().any(|a| assigns_key(&a.target, &key)) {
        return reject(Reason::KeyChange, "an UPDATE of the distribution key");
    }
    // Subqueries inside the write.
    let mut nested = Vec::new();
    collect_queries_stmt(stmt, &mut nested);
    for q in &nested {
        let mut bl = Vec::new();
        blocks(q, &mut bl, false);
        for (sel, _) in bl {
            if analyse(sel, cx)?.rels.iter().any(Rel::sharded) {
                return check_single_or(stmt, cx, Reason::Subquery);
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
    let make = |s: usize| {
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
            sql: render(stmt, &m, &by),
        }
    };
    if shards.len() == 1 {
        let s = shards.iter().next().copied().unwrap_or(0);
        return Ok(Plan::Pass(make(s)));
    }
    if shards.is_empty() {
        // Contradictory key predicates: no row matches; any shard says so.
        return Ok(Plan::Pass(make(0)));
    }
    if order_by || limit {
        return reject(
            Reason::LimitedDml,
            "ORDER BY or LIMIT in a multi-shard write",
        );
    }
    if lower_has(stmt, ":=") {
        return reject(Reason::Variables, "a variable assignment across shards");
    }
    let cluster = &entry.group.shards[*shards.iter().next().unwrap_or(&0)].cluster;
    if shards
        .iter()
        .any(|&s| entry.group.shards[s].cluster != *cluster)
    {
        return reject(
            Reason::CrossCluster,
            "a write on shards of several clusters",
        );
    }
    Ok(Plan::Write(WritePlan {
        kind,
        targets: shards.into_iter().map(make).collect(),
        rows: 0,
    }))
}

fn lower_has(stmt: &Statement, needle: &str) -> bool {
    stmt.to_string().contains(needle)
}

/// A write the planner doesn't split: exact only on one shard.
fn check_single_or(stmt: &Statement, cx: &PlanCtx<'_>, reason: Reason) -> R<Plan> {
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
    match try_pass(stmt, &occs, unsharded, cx) {
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

#[cfg(test)]
mod tests;
