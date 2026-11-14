//! The shape extractor: a statement's AST (sqlparser-rs) to its [`Shape`].
//!
//! Literal positions come from the normalizer ([`Normalized::literals`]):
//! each literal the AST holds is found by its source position, which gives
//! the slot a predicate's `args` name. The AST is parsed from a *view* of
//! the statement with the same byte offsets: comments, hints and
//! executable-comment markers blanked (keeping line breaks), and the tails
//! of long `IN` lists and `VALUES` row lists blanked too, so a bulk insert
//! parses as one row.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sprig_sql::lex::{self, LexOptions, TokenKind};
use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, Insert, JoinConstraint, JoinOperator, LimitClause,
    ObjectName, ObjectNamePart, OnInsert, OrderByExpr, OrderByKind, OrderBySort, Query, Select,
    SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement, TableFactor, TableObject,
    TableWithJoins, UnaryOperator, Update, UpdateTableFromKind, Value, Visit, Visitor,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Location;

use super::resolve::{ColumnClass, ResolvedTable, TableResolver};
use super::types::{
    ColumnRef, Join, JoinKind, JoinOp, OrderItem, PredOp, Predicate, Shape, SortDir, StmtType,
    TableRef, Unsargable, WriteKind, Writes,
};
use crate::fingerprint::{LiteralKind, Normalized};

/// Deepest parenthesis and `CASE` nesting sent to sqlparser-rs; deeper
/// statements take the fallback (as in `sprig-sql`'s AST path:
/// sqlparser-rs recurses several kilobytes of stack per level).
const MAX_NESTING: usize = 12;
/// Most significant tokens sent to sqlparser-rs (after list tails are
/// blanked).
const MAX_TOKENS: usize = 2000;
/// sqlparser-rs's recursion limit.
const RECURSION_LIMIT: usize = 32;

/// What the extractor needs besides the statement.
#[derive(Debug, Clone, Copy)]
pub struct ExtractCtx<'a> {
    /// The lexer options the statement was normalized with.
    pub lex: LexOptions,
    /// The session's current logical database.
    pub current_db: Option<&'a str>,
    /// `lower_case_table_names = 1`.
    pub lower_case_table_names: bool,
}

/// Extracts the shape of one statement. `sql` and `norm` must be the
/// statement and its [`crate::fingerprint::normalize`] result. Never
/// panics: a statement sqlparser-rs can't parse (or that is too deep or
/// long for it) gets a [`Shape::partial`] shape with its tables only.
#[must_use]
pub fn extract(
    sql: &[u8],
    norm: &Normalized,
    ctx: &ExtractCtx<'_>,
    resolver: &dyn TableResolver,
) -> Shape {
    if norm.unparsed || norm.sensitive || !norm.stmt_type.is_indexable() {
        return fallback(norm, ctx, resolver, false);
    }
    let ast = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        from_ast(sql, norm, ctx, resolver)
    }));
    match ast {
        Ok(Some(shape)) => shape,
        _ => fallback(norm, ctx, resolver, true),
    }
}

/// The lexer-level shape: the normalizer's tables only.
fn fallback(
    norm: &Normalized,
    ctx: &ExtractCtx<'_>,
    resolver: &dyn TableResolver,
    partial: bool,
) -> Shape {
    let mut shape = Shape::empty(norm.stmt_type);
    shape.partial = partial;
    for t in &norm.tables {
        let found = if t.cte {
            None
        } else {
            resolver.resolve(t.db.as_deref(), &t.name)
        };
        let alias = unique_alias(&shape, t.alias.as_deref().unwrap_or(&t.name));
        shape.tables.push(TableRef {
            alias,
            object_id: found.as_ref().map(|r| r.object_id),
            db: t.db.clone(),
            table: t.name.clone(),
            db_id: found.as_ref().map(|r| r.db_id),
            derived: t.cte,
        });
    }
    let _ = ctx;
    shape
}

fn unique_alias(shape: &Shape, alias: &str) -> String {
    if shape.table(alias).is_none() {
        return alias.to_owned();
    }
    (2u32..=u32::MAX)
        .map(|n| format!("{alias}#{n}"))
        .find(|a| shape.table(a).is_none())
        .unwrap_or_default()
}

/// The parse view and its position map.
struct View {
    text: String,
    /// Byte offset of each character.
    char_byte: Vec<u32>,
    /// Character index of the start of each line.
    line_char: Vec<u32>,
}

impl View {
    /// The byte offset of a sqlparser-rs location.
    fn byte(&self, loc: Location) -> Option<u32> {
        let line = usize::try_from(loc.line).ok()?.checked_sub(1)?;
        let col = usize::try_from(loc.column).ok()?.checked_sub(1)?;
        let c = *self.line_char.get(line)? as usize + col;
        self.char_byte.get(c).copied()
    }
}

/// Builds the view: trivia blanked, list tails blanked. `None` when the
/// statement isn't UTF-8 or is too deep or long for sqlparser-rs.
#[allow(clippy::too_many_lines, clippy::trivially_copy_pass_by_ref)]
fn view(sql: &[u8], opts: &LexOptions) -> Option<View> {
    let toks = lex::lex(sql, opts).ok()?;
    let mut bytes = sql.to_vec();
    let blank = |b: &mut [u8], r: std::ops::Range<usize>| {
        for x in &mut b[r] {
            if *x != b'\n' {
                *x = b' ';
            }
        }
    };
    let sig: Vec<usize> = (0..toks.len())
        .filter(|&k| !toks[k].kind.is_trivia())
        .collect();
    for t in &toks {
        if t.kind.is_trivia() && t.kind != TokenKind::Whitespace {
            blank(&mut bytes, t.range());
        }
    }
    // Blank `, x, y` after the first element of a literal IN list, and
    // `, (...), (...)` after the first row of VALUES.
    let is = |k: usize, c: &[u8]| sig.get(k).is_some_and(|&t| toks[t].bytes(sql) == c);
    let is_word = |k: usize, w: &str| {
        sig.get(k).is_some_and(|&t| {
            toks[t].kind == TokenKind::Word && toks[t].bytes(sql).eq_ignore_ascii_case(w.as_bytes())
        })
    };
    let is_lit = |k: usize| {
        sig.get(k).is_some_and(|&t| {
            matches!(
                toks[t].kind,
                TokenKind::Number
                    | TokenKind::String
                    | TokenKind::Param
                    | TokenKind::HexNumber
                    | TokenKind::HexString
                    | TokenKind::NationalString
            )
        })
    };
    let mut kept = 0usize;
    let mut blanked = vec![false; sig.len()];
    let mut n = 0;
    while n < sig.len() {
        if is_word(n, "IN") && is(n + 1, b"(") && is_lit(n + 2) {
            // IN (lit, lit, ...)
            let mut k = n + 3;
            while is(k, b",") && is_lit(k + 1) {
                k += 2;
            }
            if is(k, b")") && k > n + 3 {
                let a = toks[sig[n + 3]].start as usize;
                let b = toks[sig[k]].start as usize;
                blank(&mut bytes, a..b);
                for flag in &mut blanked[n + 3..k] {
                    *flag = true;
                }
            }
            n = k;
            continue;
        }
        if (is_word(n, "VALUES") || is_word(n, "VALUE")) && is(n + 1, b"(") {
            // Find the end of the first row, then of the last.
            let close = |from: usize| {
                let mut d = 0usize;
                for k in from..sig.len() {
                    if is(k, b"(") {
                        d += 1;
                    } else if is(k, b")") {
                        d -= 1;
                        if d == 0 {
                            return Some(k);
                        }
                    }
                }
                None
            };
            if let Some(first_end) = close(n + 1) {
                let mut last = first_end;
                while is(last + 1, b",") && is(last + 2, b"(") {
                    match close(last + 2) {
                        Some(c) => last = c,
                        None => break,
                    }
                }
                if last > first_end {
                    let a = toks[sig[first_end + 1]].start as usize;
                    let b = toks[sig[last]].end as usize;
                    blank(&mut bytes, a..b);
                    for flag in &mut blanked[first_end + 1..=last] {
                        *flag = true;
                    }
                }
                n = last + 1;
                continue;
            }
        }
        n += 1;
    }
    let mut depth = 0usize;
    let mut max_depth = 0usize;
    for (k, &t) in sig.iter().enumerate() {
        if blanked[k] {
            continue;
        }
        kept += 1;
        let b = toks[t].bytes(sql);
        let open = b == b"(" || toks[t].kind == TokenKind::Word && b.eq_ignore_ascii_case(b"CASE");
        let close = b == b")" || toks[t].kind == TokenKind::Word && b.eq_ignore_ascii_case(b"END");
        if open {
            depth += 1;
            max_depth = max_depth.max(depth);
        } else if close {
            depth = depth.saturating_sub(1);
        }
    }
    if kept > MAX_TOKENS || max_depth > MAX_NESTING {
        return None;
    }
    let text = String::from_utf8(bytes).ok()?;
    let mut char_byte = Vec::with_capacity(text.len() + 1);
    let mut line_char = vec![0u32];
    for (ci, (bi, ch)) in text.char_indices().enumerate() {
        char_byte.push(u32::try_from(bi).ok()?);
        if ch == '\n' {
            line_char.push(u32::try_from(ci + 1).ok()?);
        }
    }
    char_byte.push(u32::try_from(text.len()).ok()?);
    Some(View {
        text,
        char_byte,
        line_char,
    })
}

fn from_ast(
    sql: &[u8],
    norm: &Normalized,
    ctx: &ExtractCtx<'_>,
    resolver: &dyn TableResolver,
) -> Option<Shape> {
    let view = view(sql, &ctx.lex)?;
    let mut stmts = Parser::new(&MySqlDialect {})
        .with_recursion_limit(RECURSION_LIMIT)
        .try_with_sql(&view.text)
        .ok()?
        .parse_statements()
        .ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let stmt = stmts.pop()?;
    let mut ex = Ex {
        sql,
        norm,
        ctx,
        resolver,
        view: &view,
        shape: Shape::empty(norm.stmt_type),
        resolved: Vec::new(),
        scopes: Vec::new(),
        ctes: Vec::new(),
    };
    match &stmt {
        Statement::Query(q) => {
            ex.query(q, true, JoinKind::Inner);
        }
        Statement::Insert(i) => ex.insert(i),
        Statement::Update(u) => ex.update(u),
        Statement::Delete(d) => ex.delete(d),
        _ => return None,
    }
    ex.finish();
    Some(ex.shape)
}

/// One `SELECT`'s names: the tables it can see.
#[derive(Default)]
struct Scope {
    /// (qualifier as written, index into `shape.tables`).
    names: Vec<(String, usize)>,
    /// Select-list aliases, for `ORDER BY alias`.
    aliases: Vec<(String, Expr)>,
    /// Projection expressions in order, for `ORDER BY 1`.
    projection: Vec<Expr>,
}

struct Ex<'a> {
    sql: &'a [u8],
    norm: &'a Normalized,
    ctx: &'a ExtractCtx<'a>,
    resolver: &'a dyn TableResolver,
    view: &'a View,
    shape: Shape,
    /// Parallel to `shape.tables`.
    resolved: Vec<Option<ResolvedTable>>,
    scopes: Vec<Scope>,
    /// CTE names in scope (lower case).
    ctes: Vec<String>,
}

/// A resolved column: table index (if known) and lower-case name.
type Col = (Option<usize>, String);

fn ident_text(i: &Ident) -> String {
    i.value.clone()
}

fn name_parts(n: &ObjectName) -> Option<Vec<&Ident>> {
    n.0.iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

fn last_name(n: &ObjectName) -> String {
    n.0.last()
        .and_then(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.to_uppercase()),
            ObjectNamePart::Function(_) => None,
        })
        .unwrap_or_default()
}

/// Splits top-level `AND` conjuncts, through parentheses.
fn conjuncts(e: &Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        Expr::Nested(inner)
            if matches!(
                **inner,
                Expr::BinaryOp {
                    op: BinaryOperator::And,
                    ..
                }
            ) =>
        {
            conjuncts(inner, out);
        }
        Expr::Nested(inner) => conjuncts(inner, out),
        other => out.push(other.clone()),
    }
}

/// Splits `OR` terms, through parentheses.
fn or_terms<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
    match e {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            or_terms(left, out);
            or_terms(right, out);
        }
        Expr::Nested(inner) => or_terms(inner, out),
        other => out.push(other),
    }
}

/// Collects, without entering subqueries: column references, value
/// positions, and the top-level subqueries.
#[derive(Default)]
struct Scan {
    depth: usize,
    columns: Vec<Vec<Ident>>,
    values: Vec<Location>,
    subqueries: Vec<Sub>,
}

/// A subquery found in an expression.
enum Sub {
    In {
        expr: Box<Expr>,
        query: Box<Query>,
        negated: bool,
    },
    Exists {
        query: Box<Query>,
        negated: bool,
    },
    Scalar(Box<Query>),
}

impl Visitor for Scan {
    type Break = ();

    fn pre_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
        self.depth = self.depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if self.depth > 0 {
            return ControlFlow::Continue(());
        }
        match e {
            Expr::Identifier(i) if is_column_ident(e) => self.columns.push(vec![i.clone()]),
            Expr::CompoundIdentifier(parts) => self.columns.push(parts.clone()),
            Expr::Value(v) => self.values.push(v.span.start),
            Expr::TypedString(t) => self.values.push(t.value.span.start),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => self.subqueries.push(Sub::In {
                expr: expr.clone(),
                query: subquery.clone(),
                negated: *negated,
            }),
            Expr::Exists { subquery, negated } => self.subqueries.push(Sub::Exists {
                query: subquery.clone(),
                negated: *negated,
            }),
            Expr::Subquery(q) => self.subqueries.push(Sub::Scalar(q.clone())),
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// An unquoted identifier that names a column (not `@var`, not a bare
/// keyword-like function such as `CURRENT_TIMESTAMP`).
fn is_column_ident(e: &Expr) -> bool {
    match e {
        Expr::Identifier(i) => {
            let keyword_like = i.quote_style.is_none()
                && [
                    "CURRENT_TIMESTAMP",
                    "CURRENT_DATE",
                    "CURRENT_TIME",
                    "LOCALTIME",
                    "LOCALTIMESTAMP",
                    "CURRENT_USER",
                    "NULL",
                    "UNKNOWN",
                    "DEFAULT",
                ]
                .iter()
                .any(|k| i.value.eq_ignore_ascii_case(k));
            !i.value.starts_with('@') && !keyword_like
        }
        _ => false,
    }
}

fn scan(e: &Expr) -> Scan {
    let mut s = Scan::default();
    let _ = e.visit(&mut s);
    s
}

impl Ex<'_> {
    // ---- tables and scopes ----------------------------------------------

    fn lower(&self, s: &str) -> String {
        if self.ctx.lower_case_table_names {
            s.to_lowercase()
        } else {
            s.to_owned()
        }
    }

    fn add_table(&mut self, name: &ObjectName, alias: Option<&Ident>) {
        let Some(parts) = name_parts(name) else {
            return;
        };
        let (db, table) = match parts.as_slice() {
            [t] => (None, ident_text(t)),
            [d, t] => (Some(ident_text(d)), ident_text(t)),
            _ => return,
        };
        let table = self.lower(&table);
        let cte = db.is_none() && self.ctes.iter().any(|c| c.eq_ignore_ascii_case(&table));
        let db = if cte {
            None
        } else {
            db.map(|d| self.lower(&d))
                .or_else(|| self.ctx.current_db.map(|d| self.lower(d)))
        };
        let resolved = if cte {
            None
        } else {
            self.resolver.resolve(db.as_deref(), &table)
        };
        let written = alias.map_or_else(|| table.clone(), |a| self.lower(&a.value));
        let alias = unique_alias(&self.shape, &written);
        self.shape.tables.push(TableRef {
            alias,
            object_id: resolved.as_ref().map(|r| r.object_id),
            db,
            table,
            db_id: resolved.as_ref().map(|r| r.db_id),
            derived: cte,
        });
        self.resolved.push(resolved);
        let idx = self.shape.tables.len() - 1;
        if let Some(s) = self.scopes.last_mut() {
            s.names.push((written, idx));
        }
    }

    fn add_derived(&mut self, alias: Option<&Ident>) {
        let written = alias.map_or_else(|| "derived".to_owned(), |a| self.lower(&a.value));
        let alias = unique_alias(&self.shape, &written);
        self.shape.tables.push(TableRef {
            alias,
            object_id: None,
            db: None,
            table: written.clone(),
            db_id: None,
            derived: true,
        });
        self.resolved.push(None);
        let idx = self.shape.tables.len() - 1;
        if let Some(s) = self.scopes.last_mut() {
            s.names.push((written, idx));
        }
    }

    /// Adds a `FROM` item; returns the indexes of the tables it added.
    fn factor(&mut self, tf: &TableFactor) -> Vec<usize> {
        let before = self.shape.tables.len();
        match tf {
            TableFactor::Table { name, alias, .. } => {
                self.add_table(name, alias.as_ref().map(|a| &a.name));
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                self.query(subquery, false, JoinKind::Inner);
                self.add_derived(alias.as_ref().map(|a| &a.name));
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.table_with_joins(table_with_joins, JoinKind::Inner);
            }
            _ => {}
        }
        (before..self.shape.tables.len()).collect()
    }

    fn table_with_joins(&mut self, twj: &TableWithJoins, first_kind: JoinKind) {
        let mut prev = self.factor(&twj.relation);
        let _ = first_kind;
        for j in &twj.joins {
            let added = self.factor(&j.relation);
            let (kind, constraint) = match &j.join_operator {
                JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::StraightJoin(c) => {
                    (JoinKind::Inner, Some(c))
                }
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (JoinKind::Left, Some(c)),
                JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (JoinKind::Right, Some(c)),
                JoinOperator::CrossJoin(c) => (JoinKind::Cross, Some(c)),
                _ => (JoinKind::Inner, None),
            };
            match constraint {
                Some(JoinConstraint::On(e)) => self.where_clause(e, kind),
                Some(JoinConstraint::Using(cols)) => {
                    for c in cols {
                        let Some(parts) = name_parts(c) else { continue };
                        let Some(last) = parts.last() else { continue };
                        let col = last.value.to_lowercase();
                        if let (Some(&l), Some(&r)) = (prev.last(), added.first()) {
                            self.join(&(Some(l), col.clone()), &(Some(r), col), JoinOp::Eq, kind);
                        }
                    }
                }
                _ => {}
            }
            prev = added;
        }
    }

    /// Finds a column reference in the scopes, innermost first.
    fn col(&self, parts: &[Ident]) -> Option<Col> {
        let col = parts.last()?.value.to_lowercase();
        match parts.len() {
            1 => {
                for s in self.scopes.iter().rev() {
                    if s.names.len() == 1 {
                        return Some((Some(s.names[0].1), col));
                    }
                    let owners: Vec<usize> = s
                        .names
                        .iter()
                        .filter(|(_, i)| {
                            self.resolved[*i]
                                .as_ref()
                                .is_some_and(|r| self.resolver.has_column(r, &col) == Some(true))
                        })
                        .map(|(_, i)| *i)
                        .collect();
                    if owners.len() == 1 {
                        return Some((Some(owners[0]), col));
                    }
                    if !s.names.is_empty() {
                        return Some((None, col));
                    }
                }
                Some((None, col))
            }
            2 => {
                let q = self.lower(&parts[0].value);
                for s in self.scopes.iter().rev() {
                    if let Some((_, i)) = s.names.iter().find(|(n, _)| *n == q) {
                        return Some((Some(*i), col));
                    }
                }
                for s in self.scopes.iter().rev() {
                    if let Some((_, i)) = s.names.iter().find(|(n, _)| n.eq_ignore_ascii_case(&q)) {
                        return Some((Some(*i), col));
                    }
                }
                Some((None, col))
            }
            3 => {
                let db = self.lower(&parts[0].value);
                let t = self.lower(&parts[1].value);
                let i = self
                    .shape
                    .tables
                    .iter()
                    .position(|x| x.db.as_deref() == Some(&db) && x.table == t);
                Some((i, col))
            }
            _ => None,
        }
    }

    /// A plain column expression.
    fn as_col(&self, e: &Expr) -> Option<Col> {
        match e {
            Expr::Identifier(i) if is_column_ident(e) => self.col(std::slice::from_ref(i)),
            Expr::CompoundIdentifier(p) => self.col(p),
            Expr::Nested(x) => self.as_col(x),
            _ => None,
        }
    }

    fn colref(&self, c: &Col) -> ColumnRef {
        ColumnRef {
            table: c.0.map(|i| self.shape.tables[i].alias.clone()),
            col: c.1.clone(),
        }
    }

    // ---- literals ---------------------------------------------------------

    /// The slot of the literal at a source location.
    fn slot_at(&self, loc: Location) -> Option<u32> {
        let b = self.view.byte(loc)?;
        let lits = &self.norm.literals;
        let k = lits.partition_point(|l| l.span.start <= b);
        let l = lits.get(k.checked_sub(1)?)?;
        (b < l.span.end).then_some(l.slot)
    }

    /// The slots of the literals in an expression, ascending.
    fn slots(&self, s: &Scan) -> Vec<u32> {
        let set: BTreeSet<u32> = s.values.iter().filter_map(|l| self.slot_at(*l)).collect();
        set.into_iter().collect()
    }

    /// `Some(slots)` when the expression holds no column of this query
    /// (literals, placeholders, functions of them, uncorrelated
    /// subqueries).
    fn constant(&self, e: &Expr) -> Option<Vec<u32>> {
        let s = scan(e);
        s.columns.is_empty().then(|| self.slots(&s))
    }

    /// The literal at the slot of a plain value expression, when it is a
    /// string.
    fn string_literal(&self, e: &Expr) -> Option<Vec<u8>> {
        let Expr::Value(v) = e else { return None };
        let b = self.view.byte(v.span.start)?;
        let l = self
            .norm
            .literals
            .iter()
            .find(|l| l.span.start <= b && b < l.span.end)?;
        if l.kind != LiteralKind::String {
            return None;
        }
        lex::string_value(l.text(self.sql), self.ctx.lex.no_backslash_escapes)
    }

    // ---- predicates -------------------------------------------------------

    fn pred(&mut self, c: &Col, op: PredOp, args: Vec<u32>) {
        let p = Predicate {
            column: self.colref(c),
            op,
            args,
        };
        if !self.shape.predicates.contains(&p) {
            self.shape.predicates.push(p);
        }
    }

    fn join(&mut self, l: &Col, r: &Col, op: JoinOp, kind: JoinKind) {
        let (l, r) = (self.colref(l), self.colref(r));
        self.join_refs(l, r, op, kind);
    }

    fn join_refs(&mut self, l: ColumnRef, r: ColumnRef, op: JoinOp, kind: JoinKind) {
        let j = Join { l, r, op, kind };
        if !self.shape.joins.contains(&j) {
            self.shape.joins.push(j);
        }
    }

    fn unsargable_all(&mut self, e: &Expr, why: &Unsargable) {
        let s = scan(e);
        for parts in &s.columns {
            if let Some(c) = self.col(parts) {
                self.pred(&c, PredOp::Unsargable { why: why.clone() }, Vec::new());
            }
        }
    }

    /// A `WHERE` or `ON` clause: its conjuncts, then its subqueries.
    #[allow(clippy::many_single_char_names)]
    fn where_clause(&mut self, e: &Expr, kind: JoinKind) {
        let mut cs = Vec::new();
        conjuncts(e, &mut cs);
        for c in &cs {
            self.conjunct(c, kind);
            let s = scan(c);
            for sub in s.subqueries {
                match sub {
                    Sub::In {
                        expr,
                        query,
                        negated,
                    } => {
                        let k = if negated {
                            JoinKind::Anti
                        } else {
                            JoinKind::Semi
                        };
                        let r = self.query(&query, false, k);
                        if let (Some(l), Some(r)) = (self.as_col(&expr), r) {
                            let l = self.colref(&l);
                            self.join_refs(l, r, JoinOp::Eq, k);
                        }
                    }
                    Sub::Exists { query, negated } => {
                        let k = if negated {
                            JoinKind::Anti
                        } else {
                            JoinKind::Semi
                        };
                        self.query(&query, false, k);
                    }
                    Sub::Scalar(q) => {
                        self.query(&q, false, JoinKind::Inner);
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn conjunct(&mut self, e: &Expr, kind: JoinKind) {
        match e {
            Expr::Nested(x) => self.conjunct(x, kind),
            Expr::BinaryOp {
                op: BinaryOperator::Or,
                ..
            } => self.or_group(e),
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Eq
                | BinaryOperator::Spaceship
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => self.compare(left, op, right, kind),
                _ => self.unsargable_all(e, &Unsargable::Expression),
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => match (self.as_col(expr), self.constant(low), self.constant(high)) {
                (Some(c), Some(mut a), Some(b)) => {
                    a.extend(b);
                    let op = if *negated {
                        PredOp::Ne
                    } else {
                        PredOp::Range { lo: true, hi: true }
                    };
                    self.pred(&c, op, a);
                }
                (None, ..) => self.wrapped(expr, &PredOp::Range { lo: true, hi: true }, Vec::new()),
                _ => self.unsargable_all(e, &Unsargable::Expression),
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut slots = BTreeSet::new();
                let mut all_const = true;
                for item in list {
                    match self.constant(item) {
                        Some(s) => slots.extend(s),
                        None => all_const = false,
                    }
                }
                let slots: Vec<u32> = slots.into_iter().collect();
                let cols: Vec<Col> = match &**expr {
                    Expr::Tuple(items) => items.iter().filter_map(|i| self.as_col(i)).collect(),
                    other => self.as_col(other).into_iter().collect(),
                };
                let list_slot =
                    slots.len() == 1 && self.norm.list_slots.binary_search(&slots[0]).is_ok();
                // A wrapped column in a list: `JSON_VALUE(v, '$.x') IN (...)`.
                if all_const && cols.is_empty() && !*negated && !matches!(&**expr, Expr::Tuple(_)) {
                    self.wrapped(expr, &PredOp::In { list: list_slot }, slots);
                    return;
                }
                if !all_const || cols.is_empty() {
                    self.unsargable_all(e, &Unsargable::Expression);
                    return;
                }
                for c in cols {
                    let op = if *negated {
                        PredOp::Ne
                    } else {
                        PredOp::In { list: list_slot }
                    };
                    self.pred(&c, op, slots.clone());
                }
            }
            Expr::InSubquery { expr, .. } => {
                if self.as_col(expr).is_none() {
                    self.unsargable_all(expr, &Unsargable::Expression);
                }
            }
            Expr::Like {
                negated,
                expr,
                pattern,
                ..
            }
            | Expr::ILike {
                negated,
                expr,
                pattern,
                ..
            } => {
                self.like(e, expr, pattern, *negated);
            }
            Expr::IsNull(x) => match self.as_col(x) {
                Some(c) => self.pred(&c, PredOp::IsNull, Vec::new()),
                None => self.wrapped(x, &PredOp::IsNull, Vec::new()),
            },
            Expr::IsNotNull(x) => match self.as_col(x) {
                Some(c) => self.pred(&c, PredOp::NotNull, Vec::new()),
                None => self.wrapped(x, &PredOp::NotNull, Vec::new()),
            },
            Expr::MatchAgainst { columns, .. } => {
                for n in columns {
                    if let Some(parts) = name_parts(n) {
                        let parts: Vec<Ident> = parts.into_iter().cloned().collect();
                        if let Some(c) = self.col(&parts) {
                            self.pred(&c, PredOp::Fulltext, Vec::new());
                        }
                    }
                }
            }
            Expr::Function(f) => {
                let name = last_name(&f.name);
                if name.starts_with("ST_") || name.starts_with("MBR") {
                    let s = scan(e);
                    let slots = self.slots(&s);
                    for parts in &s.columns {
                        if let Some(c) = self.col(parts) {
                            self.pred(&c, PredOp::Spatial, slots.clone());
                        }
                    }
                } else {
                    self.unsargable_all(e, &Unsargable::Function(name));
                }
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => {
                let s = scan(expr);
                let slots = self.slots(&s);
                let simple = matches!(
                    strip_nested(expr),
                    Expr::BinaryOp {
                        op: BinaryOperator::Eq,
                        ..
                    } | Expr::InList { .. }
                        | Expr::Between { .. }
                        | Expr::Like { .. }
                );
                for parts in &s.columns {
                    if let Some(c) = self.col(parts) {
                        if simple {
                            self.pred(&c, PredOp::Ne, slots.clone());
                        } else {
                            self.pred(
                                &c,
                                PredOp::Unsargable {
                                    why: Unsargable::Expression,
                                },
                                Vec::new(),
                            );
                        }
                    }
                }
            }
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                // A bare boolean column: `WHERE active` is `active <> 0`.
                if let Some(c) = self.as_col(e) {
                    self.pred(&c, PredOp::Ne, Vec::new());
                }
            }
            Expr::Exists { .. } | Expr::Value(_) => {}
            other => self.unsargable_all(other, &Unsargable::Expression),
        }
    }

    /// `col op value`, `value op col`, `a.x = b.y`, or a wrapped column.
    fn compare(&mut self, l: &Expr, op: &BinaryOperator, r: &Expr, kind: JoinKind) {
        let lc = self.as_col(l);
        let rc = self.as_col(r);
        match (lc, rc) {
            (Some(a), Some(b)) => {
                if a.0.is_some() && b.0.is_some() && a.0 != b.0 {
                    let jop = match op {
                        BinaryOperator::Eq | BinaryOperator::Spaceship => Some(JoinOp::Eq),
                        BinaryOperator::NotEq => None,
                        _ => Some(JoinOp::Range),
                    };
                    if let Some(jop) = jop {
                        self.join(&a, &b, jop, kind);
                    }
                } else {
                    let why = Unsargable::Expression;
                    self.pred(&a, PredOp::Unsargable { why: why.clone() }, Vec::new());
                    self.pred(&b, PredOp::Unsargable { why }, Vec::new());
                }
            }
            (Some(c), None) => self.col_vs(&c, op, r, false),
            (None, Some(c)) => self.col_vs(&c, op, l, true),
            (None, None) => {
                // A wrapped column against a value: `LOWER(email) = ?`.
                if let Some(args) = self.constant(r) {
                    self.wrapped(l, &cmp_op(op, false), args);
                } else if let Some(args) = self.constant(l) {
                    self.wrapped(r, &cmp_op(op, true), args);
                } else {
                    self.unsargable_all(l, &Unsargable::Expression);
                    self.unsargable_all(r, &Unsargable::Expression);
                }
            }
        }
    }

    /// A plain column compared with `other`.
    fn col_vs(&mut self, c: &Col, op: &BinaryOperator, other: &Expr, flipped: bool) {
        let s = scan(other);
        let slots = self.slots(&s);
        // `a.x = b.y + 1` and the like: still a lookup on `a.x` when
        // `b` is read first.
        if s.columns.is_empty() && self.type_conversion(c, other) {
            self.pred(
                c,
                PredOp::Unsargable {
                    why: Unsargable::TypeConversion,
                },
                Vec::new(),
            );
            return;
        }
        self.pred(c, cmp_op(op, flipped), slots);
    }

    /// A string column compared with a number.
    fn type_conversion(&self, c: &Col, other: &Expr) -> bool {
        let numeric = match strip_nested(other) {
            Expr::Value(v) => matches!(v.value, Value::Number(..)),
            Expr::UnaryOp {
                op: UnaryOperator::Minus | UnaryOperator::Plus,
                expr,
            } => {
                matches!(strip_nested(expr), Expr::Value(v) if matches!(v.value, Value::Number(..)))
            }
            _ => false,
        };
        numeric
            && c.0
                .and_then(|i| self.resolved[i].as_ref())
                .and_then(|r| self.resolver.column_class(r, &c.1))
                == Some(ColumnClass::String)
    }

    /// A condition on a wrapped column: a function, `COLLATE`, a JSON path
    /// or arithmetic. `op` is what the condition would be on the column.
    fn wrapped(&mut self, e: &Expr, op: &PredOp, args: Vec<u32>) {
        match strip_nested(e) {
            Expr::Collate { expr, .. } => {
                if self.as_col(expr).is_some() {
                    self.unsargable_all(expr, &Unsargable::Collation);
                } else {
                    self.unsargable_all(expr, &Unsargable::Expression);
                }
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Arrow | BinaryOperator::LongArrow,
                right,
            } => match (self.as_col(left), string_value(right)) {
                (Some(c), Some(path)) => {
                    let func = if matches!(
                        strip_nested(e),
                        Expr::BinaryOp {
                            op: BinaryOperator::LongArrow,
                            ..
                        }
                    ) {
                        "->>"
                    } else {
                        "->"
                    };
                    self.json_pred(&c, path, func, op, args);
                }
                _ => self.unsargable_all(e, &Unsargable::Expression),
            },
            Expr::Function(f) => {
                let name = last_name(&f.name);
                if let Some((c, path)) = self.json_call(&name, f) {
                    self.json_pred(&c, path, &name, op, args);
                } else if name == "JSON_UNQUOTE" {
                    // JSON_UNQUOTE(JSON_EXTRACT(doc, '$.x')).
                    if let Some(inner) = func_args(f).into_iter().next() {
                        self.wrapped(inner, op, args);
                    }
                } else {
                    self.unsargable_all(e, &Unsargable::Function(name));
                }
            }
            Expr::Cast { expr, .. } => {
                self.unsargable_all(expr, &Unsargable::Function("CAST".to_owned()));
            }
            Expr::Convert { expr, .. } => {
                self.unsargable_all(expr, &Unsargable::Function("CONVERT".to_owned()));
            }
            other => self.unsargable_all(other, &Unsargable::Expression),
        }
    }

    fn json_pred(&mut self, c: &Col, path: String, func: &str, op: &PredOp, args: Vec<u32>) {
        self.pred(
            c,
            PredOp::JsonPath {
                path,
                func: Some(func.to_owned()),
                cmp: Some(Box::new(op.clone())),
            },
            args,
        );
    }

    /// `JSON_VALUE(col, '$.x')`, `JSON_EXTRACT(col, '$.x')`: the column and
    /// the path.
    fn json_call(&self, name: &str, f: &sqlparser::ast::Function) -> Option<(Col, String)> {
        if !matches!(name, "JSON_VALUE" | "JSON_EXTRACT" | "JSON_QUERY") {
            return None;
        }
        let args = func_args(f);
        let c = self.as_col(args.first()?)?;
        let path = string_value(args.get(1)?)?;
        Some((c, path))
    }

    fn like(&mut self, whole: &Expr, expr: &Expr, pattern: &Expr, negated: bool) {
        let Some(c) = self.as_col(expr) else {
            self.wrapped(expr, &PredOp::Like, Vec::new());
            return;
        };
        let Some(args) = self.constant(pattern) else {
            self.unsargable_all(whole, &Unsargable::Expression);
            return;
        };
        if negated {
            self.pred(&c, PredOp::Ne, args);
            return;
        }
        if let Some(v) = self.string_literal(strip_nested(pattern)) {
            if matches!(v.first(), Some(b'%' | b'_')) {
                self.pred(
                    &c,
                    PredOp::Unsargable {
                        why: Unsargable::LeadingWildcard,
                    },
                    args,
                );
            } else {
                self.pred(&c, PredOp::Prefix, args);
            }
            return;
        }
        // CONCAT('%', ?) and the like.
        if let Expr::Function(f) = strip_nested(pattern)
            && last_name(&f.name) == "CONCAT"
            && let Some(v) = func_args(f)
                .first()
                .and_then(|a| self.string_literal(strip_nested(a)))
            && matches!(v.first(), Some(b'%' | b'_'))
        {
            self.pred(
                &c,
                PredOp::Unsargable {
                    why: Unsargable::LeadingWildcard,
                },
                args,
            );
            return;
        }
        self.pred(&c, PredOp::Like, args);
    }

    /// An `OR` conjunct: an `in` when every term is an equality or `IN` of
    /// constants on one column; otherwise an `OR` group.
    fn or_group(&mut self, e: &Expr) {
        let mut terms = Vec::new();
        or_terms(e, &mut terms);
        let mut col: Option<Col> = None;
        let mut slots = BTreeSet::new();
        let mut same = true;
        for t in &terms {
            let (c, s) = match strip_nested(t) {
                Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                } => match (self.as_col(left), self.as_col(right)) {
                    (Some(c), None) => (Some(c), self.constant(right)),
                    (None, Some(c)) => (Some(c), self.constant(left)),
                    _ => (None, None),
                },
                Expr::InList {
                    expr,
                    list,
                    negated: false,
                } => {
                    let mut all = Some(Vec::new());
                    for i in list {
                        match (self.constant(i), all.as_mut()) {
                            (Some(s), Some(a)) => a.extend(s),
                            _ => all = None,
                        }
                    }
                    (self.as_col(expr), all)
                }
                _ => (None, None),
            };
            match (c, s) {
                (Some(c), Some(s)) if col.as_ref().is_none_or(|x| *x == c) => {
                    col = Some(c);
                    slots.extend(s);
                }
                _ => {
                    same = false;
                    break;
                }
            }
        }
        if same && let Some(c) = col {
            self.pred(&c, PredOp::In { list: false }, slots.into_iter().collect());
            return;
        }
        self.shape.or_groups += 1;
        self.unsargable_all(e, &Unsargable::Or);
    }

    // ---- queries ------------------------------------------------------------

    /// Walks a query in a new scope; returns its first projected column.
    fn query(&mut self, q: &Query, top: bool, kind: JoinKind) -> Option<ColumnRef> {
        let saved_ctes = self.ctes.len();
        if let Some(with) = &q.with {
            for cte in &with.cte_tables {
                self.query(&cte.query, false, JoinKind::Inner);
                self.ctes.push(cte.alias.name.value.to_lowercase());
            }
        }
        let (first, scope) = self.set_expr(&q.body, top, kind);
        if top {
            if let Some(scope) = scope {
                self.scopes.push(scope);
                if let Some(ob) = &q.order_by {
                    match &ob.kind {
                        OrderByKind::Expressions(items) => self.order_by(items),
                        OrderByKind::All(_) => self.shape.order_by_expr = true,
                    }
                }
                self.scopes.pop();
            }
            self.shape.limit |= q.limit_clause.is_some() || q.fetch.is_some();
            let count = match &q.limit_clause {
                Some(LimitClause::LimitOffset { limit: Some(e), .. }) => Some(e),
                Some(LimitClause::OffsetCommaLimit { limit, .. }) => Some(limit),
                _ => None,
            };
            self.limit_arg(count);
        }
        self.ctes.truncate(saved_ctes);
        first
    }

    fn set_expr(
        &mut self,
        body: &SetExpr,
        top: bool,
        kind: JoinKind,
    ) -> (Option<ColumnRef>, Option<Scope>) {
        match body {
            SetExpr::Select(s) => self.select(s, top, kind),
            SetExpr::Query(q) => (self.query(q, top, kind), None),
            SetExpr::SetOperation { left, right, .. } => {
                let (first, _) = self.set_expr(left, false, kind);
                let _ = self.set_expr(right, false, kind);
                (first, None)
            }
            _ => (None, None),
        }
    }

    fn select(
        &mut self,
        s: &Select,
        top: bool,
        kind: JoinKind,
    ) -> (Option<ColumnRef>, Option<Scope>) {
        self.scopes.push(Scope::default());
        for twj in &s.from {
            self.table_with_joins(twj, JoinKind::Inner);
        }
        if let Some(w) = &s.selection {
            self.where_clause(w, kind);
        }
        let mut first = None;
        for (n, item) in s.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    if n == 0 {
                        first = self.as_col(e).map(|c| self.colref(&c));
                    }
                    self.read_columns(e);
                    if let Some(scope) = self.scopes.last_mut() {
                        scope.projection.push(e.clone());
                        if let SelectItem::ExprWithAlias { alias, .. } = item {
                            scope.aliases.push((alias.value.to_lowercase(), e.clone()));
                        }
                    }
                }
                SelectItem::ExprWithAliases { expr, .. } => self.read_columns(expr),
                SelectItem::Wildcard(_) => {
                    let names: Vec<usize> = self
                        .scopes
                        .last()
                        .map(|s| s.names.iter().map(|n| n.1).collect())
                        .unwrap_or_default();
                    for i in names {
                        let a = self.shape.tables[i].alias.clone();
                        self.select_col(&a, "*");
                    }
                }
                SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(n),
                    _,
                ) => {
                    if let Some(parts) = name_parts(n) {
                        let mut parts: Vec<Ident> = parts.into_iter().cloned().collect();
                        parts.push(Ident::new("*"));
                        if let Some((Some(i), _)) = self.col(&parts) {
                            let a = self.shape.tables[i].alias.clone();
                            self.select_col(&a, "*");
                        }
                    }
                }
                SelectItem::QualifiedWildcard(..) => {}
            }
            // Scalar subqueries in the select list.
            if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item {
                for sub in scan(e).subqueries {
                    let (Sub::Scalar(q) | Sub::Exists { query: q, .. } | Sub::In { query: q, .. }) =
                        sub;
                    self.query(&q, false, JoinKind::Inner);
                }
            }
        }
        if top {
            match &s.group_by {
                GroupByExpr::Expressions(items, _) => {
                    for g in items {
                        match self.as_col(g) {
                            Some(c) => {
                                let r = self.colref(&c);
                                self.shape.group_by.push(r);
                            }
                            None => self.shape.group_by_expr = true,
                        }
                    }
                }
                GroupByExpr::All(_) => self.shape.group_by_expr = true,
            }
        }
        let scope = self.scopes.pop();
        (first, scope)
    }

    /// Records the slot of a `LIMIT` row count that is one literal.
    fn limit_arg(&mut self, e: Option<&Expr>) {
        if let Some(Expr::Value(v)) = e.map(strip_nested) {
            self.shape.limit_arg = self.slot_at(v.span.start);
        }
    }

    fn select_col(&mut self, alias: &str, col: &str) {
        let v = self.shape.select.entry(alias.to_owned()).or_default();
        if !v.iter().any(|c| c == col) {
            v.push(col.to_owned());
            v.sort_unstable();
        }
    }

    /// Records the columns an expression reads in the select map.
    fn read_columns(&mut self, e: &Expr) {
        let s = scan(e);
        for parts in &s.columns {
            if let Some(c) = self.col(parts) {
                let alias =
                    c.0.map_or_else(|| "?".to_owned(), |i| self.shape.tables[i].alias.clone());
                self.select_col(&alias, &c.1);
            }
        }
    }

    fn order_by(&mut self, items: &[OrderByExpr]) {
        for o in items {
            let dir = match o.options.sort {
                Some(OrderBySort::Desc) => SortDir::Desc,
                _ => SortDir::Asc,
            };
            let mut e = &o.expr;
            // ORDER BY 1 and ORDER BY alias.
            let scope = self.scopes.last();
            let resolved: Option<Expr> = match e {
                Expr::Value(v) => match &v.value {
                    Value::Number(n, _) => n.parse::<usize>().ok().and_then(|k| {
                        scope.and_then(|s| s.projection.get(k.checked_sub(1)?).cloned())
                    }),
                    _ => None,
                },
                Expr::Identifier(i) => scope.and_then(|s| {
                    s.aliases
                        .iter()
                        .find(|(a, _)| *a == i.value.to_lowercase())
                        .map(|(_, x)| x.clone())
                }),
                _ => None,
            };
            if let Some(r) = &resolved {
                e = r;
            }
            if let Expr::Function(f) = strip_nested(e)
                && last_name(&f.name).starts_with("VEC_DISTANCE")
            {
                let s = scan(e);
                let slots = self.slots(&s);
                let name = last_name(&f.name);
                for parts in &s.columns {
                    if let Some(c) = self.col(parts) {
                        self.pred(&c, PredOp::Vector, slots.clone());
                        // The distance function, for automatic indexing's
                        // vector candidates: which metric an index would
                        // need.
                        self.pred(
                            &c,
                            PredOp::Unsargable {
                                why: Unsargable::Function(name.clone()),
                            },
                            Vec::new(),
                        );
                    }
                }
                self.shape.order_by_expr = true;
                continue;
            }
            match self.as_col(e) {
                Some(c) => {
                    let column = self.colref(&c);
                    self.shape.order_by.push(OrderItem { column, dir });
                }
                None => self.shape.order_by_expr = true,
            }
        }
    }

    // ---- writes -------------------------------------------------------------

    fn insert(&mut self, i: &Insert) {
        self.scopes.push(Scope::default());
        if let TableObject::TableName(n) = &i.table {
            self.add_table(n, None);
        }
        let target = self.shape.tables.first().map(|t| t.alias.clone());
        let mut set: Vec<ColumnRef> = i
            .columns
            .iter()
            .filter_map(|c| name_parts(c)?.last().map(|x| x.value.to_lowercase()))
            .map(|c| ColumnRef {
                table: target.clone(),
                col: c,
            })
            .collect();
        let mut kind = if i.replace_into {
            WriteKind::Replace
        } else {
            WriteKind::Insert
        };
        for a in &i.assignments {
            set.extend(self.assignment_cols(&a.target, target.as_deref()));
        }
        if let Some(OnInsert::DuplicateKeyUpdate(assignments)) = &i.on {
            kind = WriteKind::Upsert;
            for a in assignments {
                set.extend(self.assignment_cols(&a.target, target.as_deref()));
            }
        }
        dedup_cols(&mut set);
        let all_columns = i.columns.is_empty() && i.assignments.is_empty();
        if let Some(src) = &i.source
            && !matches!(*src.body, SetExpr::Values(_))
        {
            self.query(src, false, JoinKind::Inner);
        }
        self.scopes.pop();
        self.shape.writes = Some(Writes {
            kind,
            targets: target.into_iter().collect(),
            set,
            all_columns,
        });
    }

    fn assignment_cols(&self, t: &AssignmentTarget, default: Option<&str>) -> Vec<ColumnRef> {
        let names: Vec<&ObjectName> = match t {
            AssignmentTarget::ColumnName(n) => vec![n],
            AssignmentTarget::Tuple(ns) => ns.iter().collect(),
        };
        names
            .into_iter()
            .filter_map(|n| {
                let parts: Vec<Ident> = name_parts(n)?.into_iter().cloned().collect();
                if parts.len() == 1
                    && let Some(d) = default
                {
                    return Some(ColumnRef::new(d, parts[0].value.to_lowercase()));
                }
                self.col(&parts).map(|c| self.colref(&c))
            })
            .collect()
    }

    fn update(&mut self, u: &Update) {
        self.scopes.push(Scope::default());
        self.table_with_joins(&u.table, JoinKind::Inner);
        if let Some(UpdateTableFromKind::BeforeSet(f) | UpdateTableFromKind::AfterSet(f)) = &u.from
        {
            for twj in f {
                self.table_with_joins(twj, JoinKind::Inner);
            }
        }
        if let Some(w) = &u.selection {
            self.where_clause(w, JoinKind::Inner);
        }
        let single = (self.shape.tables.len() == 1).then(|| self.shape.tables[0].alias.clone());
        let mut set = Vec::new();
        for a in &u.assignments {
            set.extend(self.assignment_cols(&a.target, single.as_deref()));
            self.read_columns(&a.value);
        }
        dedup_cols(&mut set);
        let mut targets: Vec<String> = Vec::new();
        for c in &set {
            if let Some(t) = &c.table
                && !targets.contains(t)
            {
                targets.push(t.clone());
            }
        }
        self.order_by(&u.order_by);
        self.shape.limit |= u.limit.is_some();
        self.limit_arg(u.limit.as_ref());
        self.scopes.pop();
        self.shape.writes = Some(Writes {
            kind: WriteKind::Update,
            targets,
            set,
            all_columns: false,
        });
    }

    fn delete(&mut self, d: &Delete) {
        self.scopes.push(Scope::default());
        let from = match &d.from {
            FromTable::WithFromKeyword(f) | FromTable::WithoutKeyword(f) => f,
        };
        for twj in from {
            self.table_with_joins(twj, JoinKind::Inner);
        }
        if let Some(using) = &d.using {
            for twj in using {
                self.table_with_joins(twj, JoinKind::Inner);
            }
        }
        if let Some(w) = &d.selection {
            self.where_clause(w, JoinKind::Inner);
        }
        let mut targets = Vec::new();
        for n in &d.tables {
            if let Some(parts) = name_parts(n) {
                let mut parts: Vec<Ident> = parts.into_iter().cloned().collect();
                parts.push(Ident::new("*"));
                if let Some((Some(i), _)) = self.col(&parts) {
                    targets.push(self.shape.tables[i].alias.clone());
                }
            }
        }
        if targets.is_empty() {
            targets.extend(self.shape.tables.first().map(|t| t.alias.clone()));
        }
        self.order_by(&d.order_by);
        self.shape.limit |= d.limit.is_some();
        self.limit_arg(d.limit.as_ref());
        self.scopes.pop();
        self.shape.writes = Some(Writes {
            kind: WriteKind::Delete,
            targets,
            set: Vec::new(),
            all_columns: true,
        });
    }

    fn finish(&mut self) {
        if self.shape.stmt_type == StmtType::Other {
            self.shape.stmt_type = self.norm.stmt_type;
        }
    }
}

/// Removes repeated columns, keeping the first of each.
fn dedup_cols(v: &mut Vec<ColumnRef>) {
    let mut seen = Vec::with_capacity(v.len());
    v.retain(|c| {
        if seen.contains(c) {
            false
        } else {
            seen.push(c.clone());
            true
        }
    });
}

/// A string value's text (JSON paths are kept in the normalized text,
/// so they are not literals there).
fn string_value(e: &Expr) -> Option<String> {
    match strip_nested(e) {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn strip_nested(e: &Expr) -> &Expr {
    match e {
        Expr::Nested(x) => strip_nested(x),
        other => other,
    }
}

/// The plain expression arguments of a function call.
fn func_args(f: &sqlparser::ast::Function) -> Vec<&Expr> {
    match &f.args {
        FunctionArguments::List(l) => l
            .args
            .iter()
            .filter_map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                | FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } => Some(e),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The predicate operator of `col op value` (`flipped`: `value op col`).
fn cmp_op(op: &BinaryOperator, flipped: bool) -> PredOp {
    let (lt, gt) = if flipped {
        (false, true)
    } else {
        (true, false)
    };
    match op {
        BinaryOperator::Eq | BinaryOperator::Spaceship => PredOp::Eq,
        BinaryOperator::NotEq => PredOp::Ne,
        BinaryOperator::Lt | BinaryOperator::LtEq => PredOp::Range { lo: !lt, hi: lt },
        BinaryOperator::Gt | BinaryOperator::GtEq => PredOp::Range { lo: !gt, hi: gt },
        _ => PredOp::Unsargable {
            why: Unsargable::Expression,
        },
    }
}
