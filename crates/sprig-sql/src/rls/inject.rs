//! The rewrite for tables: every reference to a protected table gets its
//! policies' predicate, before any join, so MariaDB evaluates them per shard
//! and the router prunes with them.
//!
//! ```text
//! inject(text):
//!   no token names a protected table           → None (nothing to do)
//!   parse the parse view (one statement)       → else 39366
//!   SELECT, WITH, TABLE, set operations, subqueries, CTEs, derived tables:
//!     every SELECT block, after its subqueries:
//!       each protected table factor (not a CTE of the scope):
//!         preserved by every join around it    → (P) AND (where)   in the block's WHERE
//!         nullable side of an outer join       → (P) AND (on)      in that join's ON
//!         ... whose join has USING or NATURAL  → (SELECT * FROM t WHERE (P)) AS t
//!   UPDATE, DELETE: the same, in the statement's WHERE and ONs; target
//!     tables get their UPDATE or DELETE policies with the SELECT ones
//!   INSERT, REPLACE (VALUES, SET, SELECT), ON DUPLICATE KEY UPDATE:
//!     sources as SELECT; the target's checks are MariaDB triggers
//!   anything else naming a protected table     → 39366
//!   verify: the edited parse view parses to the expected AST, else 39366
//! ```
//!
//! The text is edited, not regenerated: the policy predicate comes first and
//! the client's own condition stays in parentheses, so comments, hints and
//! literals survive byte for byte and nothing the client wrote can change what
//! the predicate means. The predicate is the same as a filtered derived table
//! (a filter on the base relation below every join), in the form the router can
//! push to shards and prune with; the derived table remains for `USING` and
//! `NATURAL` joins on the nullable side.

use std::ops::ControlFlow;

use sprig_common::SprigError;
use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Expr, FromTable, Ident, JoinConstraint, JoinOperator,
    ObjectName, ObjectNamePart, Query, Select, SetExpr, Spanned, Statement, TableFactor,
    TableObject, TableWithJoins, Visit, VisitMut, VisitorMut,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::text::{Edit, Positions, Toks, apply, map_edits, quote};
use super::unsupported;
use crate::lex::{self, LexOptions, TokenKind, ident_value};
use crate::names::rewrite::ast::parse_view;

/// Which policies a reference needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Cmd {
    /// `SELECT` policies' `USING`.
    Select,
    /// `UPDATE` policies' `USING`.
    Update,
    /// `DELETE` policies' `USING`.
    Delete,
}

/// What the rewrite asks about tables (the gateway answers from the
/// session's compiled policies).
pub trait Resolver {
    /// Whether some protected table is called `word` (lower case), in any
    /// database: a cheap filter before parsing.
    fn mentions(&self, word: &str) -> bool;

    /// The key of the protected table a reference names (`db` as written,
    /// `None` for the current database), or `None` if the reference
    /// doesn't bind the session (unprotected, a temporary table, a
    /// bypass).
    fn protected(&self, db: Option<&str>, table: &str) -> Option<usize>;

    /// The predicate for `key` and `cmds` (AND-ed), columns qualified with
    /// `qual`, session values bound.
    ///
    /// # Errors
    /// The policy's (39364) if it can't be rendered.
    fn predicate(&self, key: usize, cmds: &[Cmd], qual: &str) -> Result<String, SprigError>;

    /// Whether a relation that isn't protected must not be read here (a
    /// view over a protected table: MariaDB would read it unfiltered).
    fn refused_relation(&self, db: Option<&str>, name: &str) -> bool {
        let _ = (db, name);
        false
    }

    /// Whether a called function is stored code that reads a protected
    /// table.
    fn refused_function(&self, name: &str) -> bool {
        let _ = name;
        false
    }

    /// Whether the reference to protected table `key` under `alias` is a
    /// layer read inside a merged view of an inheritable object (chapter
    /// 42), whose rows the gateway filtered once, on the merged records
    /// (chapter 29, "Data inheritance"). Such a reference gets no predicate:
    /// a filter on each layer row would drop overrides and hide markers and
    /// show the parent's values instead.
    fn merged_layer(&self, key: usize, alias: &str) -> bool {
        let _ = (key, alias);
        false
    }
}

/// A rewritten statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Injected {
    /// The text to plan and send.
    pub sql: String,
    /// Protected tables the predicates filter.
    pub filtered: Vec<usize>,
    /// Protected tables the statement writes (MariaDB's triggers check the
    /// new rows).
    pub written: Vec<usize>,
    /// Protected tables read only as layers of merged views
    /// ([`Resolver::merged_layer`]): MariaDB's policy views must let their
    /// rows through for this statement.
    pub merged: Vec<usize>,
}

type R<T> = Result<T, SprigError>;

/// Rewrites one statement for the session behind `r` (see the module
/// docs); `Ok(None)` when it names no protected table.
///
/// # Errors
/// 39366 for a statement that can't be policed exactly; the resolver's.
pub fn inject(sql: &str, lex_opts: &LexOptions, r: &dyn Resolver) -> R<Option<Injected>> {
    let all = lex::lex(sql.as_bytes(), lex_opts).map_err(|_| unsupported("statement too long"))?;
    let mentioned = all.iter().any(|t| {
        t.kind.is_ident_like()
            && r.mentions(
                &String::from_utf8_lossy(&ident_value(t.bytes(sql.as_bytes()))).to_lowercase(),
            )
    });
    if !mentioned {
        return Ok(None);
    }
    let mut view = parse_view(sql, &all)
        .ok_or_else(|| unsupported("a statement over a protected table that doesn't parse"))?;
    let orig_pos = Positions::new(sql);
    let toks = Toks::new(sql.as_bytes(), &all);
    // sqlparser-rs doesn't read MariaDB's `LOCK IN SHARE MODE`: the parse
    // view blanks it (the edits never touch it).
    if let Some(lock) = share_lock(&toks) {
        let vp = Positions::new(&view);
        let a = vp.offset(orig_pos.location(toks.start(lock)));
        let b = vp.offset(orig_pos.location(toks.end(lock + 3)));
        if let (Some(a), Some(b)) = (a, b) {
            let blank: String = view[a..b].chars().map(|_| ' ').collect();
            view.replace_range(a..b, &blank);
        }
    }
    let mut stmts = Parser::new(&MySqlDialect {})
        .with_recursion_limit(64)
        .try_with_sql(&view)
        .and_then(|mut p| p.parse_statements())
        .map_err(|_| unsupported("a statement over a protected table that doesn't parse"))?;
    if stmts.len() != 1 {
        return Err(unsupported("several statements over a protected table"));
    }
    let mut stmt = stmts.pop().ok_or_else(|| unsupported("no statement"))?;
    let mut cx = Cx {
        r,
        pos: &orig_pos,
        toks: &toks,
        edits: Vec::new(),
        filtered: Vec::new(),
        written: Vec::new(),
        merged: Vec::new(),
    };
    // Stored functions reading protected tables run unfiltered in MariaDB.
    let mut calls = Calls(Vec::new());
    let _ = Visit::visit(&stmt, &mut calls);
    if calls.0.iter().any(|f| r.refused_function(f)) {
        return Err(unsupported(
            "a stored function that reads a protected table, for a session bound by its policies",
        ));
    }
    cx.statement(&mut stmt)?;
    let mut merged = std::mem::take(&mut cx.merged);
    merged.sort_unstable();
    merged.dedup();
    if cx.edits.is_empty() && cx.written.is_empty() {
        if !merged.is_empty() {
            return Ok(Some(Injected {
                sql: sql.to_owned(),
                filtered: cx.filtered,
                written: Vec::new(),
                merged,
            }));
        }
        // The name was a column, an alias or a CTE.
        return Ok(None);
    }
    // Verify: the edited parse view must parse to the expected AST.
    let view_pos = Positions::new(&view);
    let view_edits = map_edits(&orig_pos, &view_pos, &cx.edits)
        .ok_or_else(|| unsupported("an edit position is unknown"))?;
    let edited_view = apply(&view, &view_edits)?;
    let reparsed = Parser::new(&MySqlDialect {})
        .with_recursion_limit(64)
        .try_with_sql(&edited_view)
        .and_then(|mut p| p.parse_statements())
        .map_err(|_| unsupported("the rewritten statement doesn't parse"))?;
    if reparsed.len() != 1 || reparsed[0].to_string() != stmt.to_string() {
        return Err(unsupported(
            "the rewritten statement doesn't match its plan",
        ));
    }
    let out = apply(sql, &cx.edits)?;
    let mut filtered = cx.filtered;
    filtered.sort_unstable();
    filtered.dedup();
    let mut written = cx.written;
    written.sort_unstable();
    written.dedup();
    Ok(Some(Injected {
        sql: out,
        filtered,
        written,
        merged,
    }))
}

/// CTE names visible at a point (exact spelling).
type Scope = Vec<String>;

/// What a statement does to the tables of a FROM tree.
#[derive(Clone)]
struct Role {
    /// The write command for targets, if any.
    write: Option<Cmd>,
    /// Exposed names (lower case) of the write targets; `None`: every
    /// protected table of the tree is a target.
    targets: Option<Vec<String>>,
}

impl Role {
    fn read() -> Self {
        Self {
            write: None,
            targets: Some(Vec::new()),
        }
    }

    fn cmds(&self, exposed: &str) -> Vec<Cmd> {
        match self.write {
            None => vec![Cmd::Select],
            Some(w) => {
                let t = self
                    .targets
                    .as_ref()
                    .is_none_or(|v| v.iter().any(|n| n == &exposed.to_lowercase()));
                if t {
                    vec![Cmd::Select, w]
                } else {
                    vec![Cmd::Select]
                }
            }
        }
    }
}

struct Cx<'a> {
    r: &'a dyn Resolver,
    pos: &'a Positions<'a>,
    toks: &'a Toks<'a>,
    edits: Vec<Edit>,
    filtered: Vec<usize>,
    written: Vec<usize>,
    merged: Vec<usize>,
}

/// Processes the first-level subqueries of a node (each fully, after the
/// visitor has walked it).
struct FirstLevel<'c, 'a> {
    cx: &'c mut Cx<'a>,
    scope: &'c Scope,
    depth: usize,
    err: Option<SprigError>,
}

impl VisitorMut for FirstLevel<'_, '_> {
    type Break = ();

    fn pre_visit_query(&mut self, _q: &mut Query) -> ControlFlow<()> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        self.depth -= 1;
        if self.depth == 0
            && let Err(e) = self.cx.query(q, self.scope)
        {
            self.err = Some(e);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

fn parts(n: &ObjectName) -> Option<Vec<&Ident>> {
    n.0.iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

fn nested(e: Expr) -> Expr {
    Expr::Nested(Box::new(e))
}

fn and(p: Expr, rest: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(nested(p)),
        op: BinaryOperator::And,
        right: Box::new(nested(rest)),
    }
}

fn parse_pred(p: &str) -> R<Expr> {
    super::expr::parse_rendered(p).ok_or_else(|| unsupported("a policy predicate doesn't parse"))
}

/// A protected reference waiting for its place.
struct Pred {
    text: String,
    key: usize,
}

/// Where each element of a join tree's predicates go.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Place {
    /// The enclosing target (the block's WHERE, or an outer ON).
    Outer,
    /// The ON of this tree's join `m`.
    Join(usize),
}

fn join_side(op: &JoinOperator) -> R<(Option<&JoinConstraint>, Side)> {
    Ok(match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::StraightJoin(c) => (Some(c), Side::Inner),
        JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (Some(c), Side::Left),
        JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (Some(c), Side::Right),
        _ => return Err(unsupported("a join type MariaDB doesn't have")),
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Inner,
    Left,
    Right,
}

fn is_clause_end(t: &Toks<'_>, k: usize) -> bool {
    const WORDS: &[&str] = &[
        "GROUP",
        "HAVING",
        "WINDOW",
        "ORDER",
        "LIMIT",
        "PROCEDURE",
        "INTO",
        "UNION",
        "EXCEPT",
        "INTERSECT",
        "RETURNING",
        "FETCH",
        "OFFSET",
    ];
    WORDS.iter().any(|w| t.word(k, w))
        || t.word(k, "FOR") && (t.word(k + 1, "UPDATE") || t.word(k + 1, "SHARE"))
        || t.word(k, "LOCK") && t.word(k + 1, "IN")
        || t.punct(k, ";")
}

fn is_on_end(t: &Toks<'_>, k: usize) -> bool {
    const JOINS: &[&str] = &[
        "JOIN",
        "INNER",
        "CROSS",
        "LEFT",
        "RIGHT",
        "NATURAL",
        "STRAIGHT_JOIN",
        "FULL",
    ];
    (JOINS.iter().any(|w| t.word(k, w)) && !t.punct(k + 1, "("))
        || t.word(k, "WHERE")
        || t.word(k, "SET")
        || t.punct(k, ",")
        || is_clause_end(t, k)
}

impl Cx<'_> {
    fn off(&self, loc: sqlparser::tokenizer::Location) -> R<usize> {
        self.pos
            .offset(loc)
            .ok_or_else(|| unsupported("a statement position is unknown"))
    }

    fn tok_at(&self, loc: sqlparser::tokenizer::Location) -> R<usize> {
        let o = self.off(loc)?;
        self.toks
            .at(o)
            .ok_or_else(|| unsupported("a statement position is unknown"))
    }

    #[allow(clippy::too_many_lines, clippy::many_single_char_names)] // one arm per statement
    fn statement(&mut self, s: &mut Statement) -> R<()> {
        match s {
            Statement::Query(q) => self.query(q, &Vec::new()),
            Statement::Insert(ins) => {
                if let TableObject::TableName(n) = &ins.table {
                    let ids = parts(n).ok_or_else(|| unsupported("an INSERT target"))?;
                    let (db, t) = match ids.as_slice() {
                        [t] => (None, t.value.as_str()),
                        [d, t] => (Some(d.value.as_str()), t.value.as_str()),
                        _ => return Err(unsupported("an INSERT target")),
                    };
                    if let Some(k) = self.r.protected(db, t) {
                        self.written.push(k);
                    }
                } else if self.mentions_any(&ins.table.to_string()) {
                    return Err(unsupported("an INSERT target"));
                }
                self.first_level(ins, &Vec::new())
            }
            Statement::Update(u) => {
                let single = u.table.joins.is_empty();
                let targets = if single {
                    None
                } else {
                    let mut v = Vec::new();
                    for a in &u.assignments {
                        match &a.target {
                            AssignmentTarget::ColumnName(n) => {
                                let p = parts(n).ok_or_else(|| unsupported("a SET target"))?;
                                if p.len() < 2 {
                                    v.clear();
                                    break;
                                }
                                v.push(p[p.len() - 2].value.to_lowercase());
                            }
                            AssignmentTarget::Tuple(_) => {
                                return Err(unsupported("a tuple SET"));
                            }
                        }
                    }
                    // An unqualified target in a multi-table UPDATE could
                    // be any table's: every protected table is a target.
                    (!v.is_empty()).then_some(v)
                };
                if u.from.is_some() {
                    return Err(unsupported("UPDATE ... FROM"));
                }
                let role = Role {
                    write: Some(Cmd::Update),
                    targets,
                };
                let scope = Vec::new();
                self.first_level(u, &scope)?;
                let preds = self.tree(&mut u.table, &scope, &role)?;
                let at = self.tok_at(u.update_token.0.span.start)?;
                self.stmt_where(at, &mut u.selection, &preds, &["SET"])
            }
            Statement::Delete(d) => {
                let scope = Vec::new();
                self.first_level(d, &scope)?;
                let (trees, target_names): (&mut Vec<TableWithJoins>, Option<Vec<String>>) =
                    match (&mut d.using, &mut d.from) {
                        (Some(using), FromTable::WithFromKeyword(from)) => {
                            let names = from
                                .iter()
                                .map(|t| match &t.relation {
                                    TableFactor::Table { name, .. } => parts(name)
                                        .and_then(|p| p.last().map(|i| i.value.to_lowercase())),
                                    _ => None,
                                })
                                .collect::<Option<Vec<_>>>()
                                .ok_or_else(|| unsupported("a DELETE target"))?;
                            (using, Some(names))
                        }
                        (
                            None,
                            FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from),
                        ) => {
                            let names = if d.tables.is_empty() {
                                None
                            } else {
                                Some(
                                    d.tables
                                        .iter()
                                        .map(|n| {
                                            parts(n).and_then(|p| {
                                                p.last().map(|i| i.value.to_lowercase())
                                            })
                                        })
                                        .collect::<Option<Vec<_>>>()
                                        .ok_or_else(|| unsupported("a DELETE target"))?,
                                )
                            };
                            (from, names)
                        }
                        _ => return Err(unsupported("a DELETE form")),
                    };
                let role = Role {
                    write: Some(Cmd::Delete),
                    targets: target_names,
                };
                let mut preds = Vec::new();
                for t in trees.iter_mut() {
                    preds.extend(self.tree(t, &scope, &role)?);
                }
                let at = self.tok_at(d.delete_token.0.span.start)?;
                self.stmt_where(at, &mut d.selection, &preds, &[])
            }
            Statement::Explain { .. } | Statement::ExplainTable { .. } => Err(unsupported(
                "EXPLAIN of a statement over a protected table (its estimates count hidden rows)",
            )),
            _ => Err(unsupported("this statement over a protected table")),
        }
    }

    fn mentions_any(&self, s: &str) -> bool {
        s.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
            .any(|w| self.r.mentions(&w.to_lowercase()))
    }

    fn first_level<N: VisitMut + ?Sized>(&mut self, node: &mut N, scope: &Scope) -> R<()> {
        let mut v = FirstLevel {
            cx: self,
            scope,
            depth: 0,
            err: None,
        };
        let _ = node.visit(&mut v);
        match v.err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn query(&mut self, q: &mut Query, scope: &Scope) -> R<()> {
        let mut inner = scope.clone();
        if let Some(w) = &mut q.with {
            if w.recursive {
                inner.extend(w.cte_tables.iter().map(|c| c.alias.name.value.clone()));
                for c in &mut w.cte_tables {
                    self.query(&mut c.query, &inner)?;
                }
            } else {
                for c in &mut w.cte_tables {
                    self.query(&mut c.query, &inner)?;
                    inner.push(c.alias.name.value.clone());
                }
            }
        }
        self.set_expr(&mut q.body, &inner)?;
        if let Some(o) = &mut q.order_by {
            self.first_level(o, &inner)?;
        }
        if let Some(l) = &mut q.limit_clause {
            self.first_level(l, &inner)?;
        }
        Ok(())
    }

    fn set_expr(&mut self, s: &mut SetExpr, scope: &Scope) -> R<()> {
        match s {
            SetExpr::Select(sel) => self.select(sel, scope),
            SetExpr::Query(q) => self.query(q, scope),
            SetExpr::SetOperation { left, right, .. } => {
                self.set_expr(left, scope)?;
                self.set_expr(right, scope)
            }
            SetExpr::Values(v) => self.first_level(v, scope),
            SetExpr::Table(t) => {
                let name = t.table_name.clone().unwrap_or_default();
                if self.r.protected(t.schema_name.as_deref(), &name).is_some()
                    && !scope.contains(&name)
                {
                    return Err(unsupported("TABLE of a protected table"));
                }
                Ok(())
            }
            _ => {
                if self.mentions_any(&s.to_string()) {
                    return Err(unsupported("this query form over a protected table"));
                }
                Ok(())
            }
        }
    }

    fn select(&mut self, sel: &mut Select, scope: &Scope) -> R<()> {
        self.first_level(sel, scope)?;
        let role = Role::read();
        let mut preds = Vec::new();
        for t in &mut sel.from {
            preds.extend(self.tree(t, scope, &role)?);
        }
        if preds.is_empty() {
            return Ok(());
        }
        let p = combine(&preds);
        let st = self.tok_at(sel.select_token.0.span.start)?;
        let from = self.toks.scan(st + 1, |t, k| t.word(k, "FROM"));
        if !self.toks.word(from, "FROM") {
            return Err(unsupported("a SELECT without FROM"));
        }
        if let Some(orig) = sel.selection.take() {
            let w = self
                .toks
                .scan(from + 1, |t, k| t.word(k, "WHERE") || is_clause_end(t, k));
            if !self.toks.word(w, "WHERE") {
                return Err(unsupported("a WHERE clause wasn't found"));
            }
            let end = self.toks.scan(w + 1, is_clause_end);
            self.wrap(w + 1, end, &p);
            sel.selection = Some(and(parse_pred(&p)?, orig));
        } else {
            let end = self.toks.scan(from + 1, is_clause_end);
            self.edits.push(Edit {
                start: self.toks.end(end - 1),
                end: self.toks.end(end - 1),
                with: format!(" WHERE ({p})"),
            });
            sel.selection = Some(nested(parse_pred(&p)?));
        }
        self.filtered.extend(preds.iter().map(|x| x.key));
        Ok(())
    }

    /// `(p) AND (` before token `a`, `)` after token `end - 1`.
    fn wrap(&mut self, a: usize, end: usize, p: &str) {
        self.edits.push(Edit {
            start: self.toks.start(a),
            end: self.toks.start(a),
            with: format!("({p}) AND ("),
        });
        self.edits.push(Edit {
            start: self.toks.end(end - 1),
            end: self.toks.end(end - 1),
            with: ")".to_owned(),
        });
    }

    /// The WHERE of an UPDATE or DELETE starting at token `at`; `after`
    /// names a keyword the WHERE follows (`SET`).
    fn stmt_where(
        &mut self,
        at: usize,
        selection: &mut Option<Expr>,
        preds: &[Pred],
        after: &[&str],
    ) -> R<()> {
        if preds.is_empty() {
            return Ok(());
        }
        let p = combine(preds);
        let mut from = at + 1;
        for w in after {
            let k = self.toks.scan(from, |t, k| t.word(k, w));
            if !self.toks.word(k, w) {
                return Err(unsupported("a clause wasn't found"));
            }
            from = k + 1;
        }
        let stmt_end = |t: &Toks<'_>, k: usize| {
            t.word(k, "ORDER") || t.word(k, "LIMIT") || t.word(k, "RETURNING") || t.punct(k, ";")
        };
        let w = self
            .toks
            .scan(from, |t, k| t.word(k, "WHERE") || stmt_end(t, k));
        if let Some(orig) = selection.take() {
            if !self.toks.word(w, "WHERE") {
                return Err(unsupported("a WHERE clause wasn't found"));
            }
            let end = self.toks.scan(w + 1, stmt_end);
            self.wrap(w + 1, end, &p);
            *selection = Some(and(parse_pred(&p)?, orig));
        } else {
            if self.toks.word(w, "WHERE") {
                return Err(unsupported("an unexpected WHERE"));
            }
            self.edits.push(Edit {
                start: self.toks.end(w - 1),
                end: self.toks.end(w - 1),
                with: format!(" WHERE ({p})"),
            });
            *selection = Some(nested(parse_pred(&p)?));
        }
        self.filtered.extend(preds.iter().map(|x| x.key));
        Ok(())
    }

    /// One join tree: predicates for nullable sides go into their joins'
    /// ONs here; the rest are returned for the enclosing target.
    #[allow(clippy::many_single_char_names)]
    fn tree(&mut self, t: &mut TableWithJoins, scope: &Scope, role: &Role) -> R<Vec<Pred>> {
        let n = t.joins.len();
        let mut sides = Vec::with_capacity(n);
        for j in &t.joins {
            sides.push(join_side(&j.join_operator)?.1);
        }
        // The innermost outer join that nullifies element e (0: the base
        // relation, k: join k-1's relation).
        let place = |e: usize| -> Place {
            if e > 0 && sides[e - 1] == Side::Left {
                return Place::Join(e - 1);
            }
            // Every element left of a RIGHT JOIN is on its nullable side.
            (e..n)
                .find(|&m| sides[m] == Side::Right)
                .map_or(Place::Outer, Place::Join)
        };
        let mut out = Vec::new();
        let mut per_join: Vec<Vec<Pred>> = (0..n).map(|_| Vec::new()).collect();
        for e in 0..=n {
            let pl = place(e);
            let using = match pl {
                Place::Join(m) => !matches!(
                    join_side(&t.joins[m].join_operator)?.0,
                    Some(JoinConstraint::On(_))
                ),
                Place::Outer => false,
            };
            let factor = if e == 0 {
                &mut t.relation
            } else {
                &mut t.joins[e - 1].relation
            };
            let mine = self.factor(factor, scope, role, using)?;
            match pl {
                Place::Outer => out.extend(mine),
                Place::Join(m) => per_join[m].extend(mine),
            }
        }
        for (m, preds) in per_join.into_iter().enumerate() {
            if preds.is_empty() {
                continue;
            }
            let p = combine(&preds);
            let (JoinOperator::Join(c)
            | JoinOperator::Inner(c)
            | JoinOperator::Left(c)
            | JoinOperator::LeftOuter(c)
            | JoinOperator::Right(c)
            | JoinOperator::RightOuter(c)
            | JoinOperator::CrossJoin(c)
            | JoinOperator::StraightJoin(c)) = &mut t.joins[m].join_operator
            else {
                return Err(unsupported("a join type MariaDB doesn't have"));
            };
            let JoinConstraint::On(orig) = c else {
                return Err(unsupported("a protected join tree under USING or NATURAL"));
            };
            let e_start = orig.span().start;
            let mut k = self.tok_at(e_start)?;
            while k > 0 && self.toks.punct(k - 1, "(") {
                k -= 1;
            }
            if k == 0 || !self.toks.word(k - 1, "ON") {
                return Err(unsupported("a join's ON wasn't found"));
            }
            let end = self.toks.scan(k, is_on_end);
            self.wrap(k, end, &p);
            let o = std::mem::replace(orig, Expr::Value(sqlparser::ast::Value::Null.into()));
            *orig = and(parse_pred(&p)?, o);
            self.filtered.extend(preds.iter().map(|x| x.key));
        }
        Ok(out)
    }

    /// A FROM element's own predicates (a nested join's are placed
    /// inside it); `derive`: its join has USING or NATURAL, so a protected
    /// table becomes a filtered derived table instead.
    #[allow(clippy::too_many_lines, clippy::many_single_char_names)]
    fn factor(
        &mut self,
        f: &mut TableFactor,
        scope: &Scope,
        role: &Role,
        derive: bool,
    ) -> R<Vec<Pred>> {
        match f {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                partitions,
                json_path,
                sample,
                index_hints,
                ..
            } => {
                let ids = parts(name).ok_or_else(|| unsupported("a table name"))?;
                let (db, t) = match ids.as_slice() {
                    [t] => (None, *t),
                    [d, t] => (Some(*d), *t),
                    _ => return Err(unsupported("a table name")),
                };
                if db.is_none() && scope.contains(&t.value) {
                    return Ok(Vec::new());
                }
                let Some(key) = self.r.protected(db.map(|d| d.value.as_str()), &t.value) else {
                    if self
                        .r
                        .refused_relation(db.map(|d| d.value.as_str()), &t.value)
                    {
                        return Err(unsupported(
                            "a view over a protected table, for a session bound by its policies",
                        ));
                    }
                    return Ok(Vec::new());
                };
                if args.is_some() || json_path.is_some() || sample.is_some() || version.is_some() {
                    return Err(unsupported("a protected table with this table clause"));
                }
                let exposed = alias.as_ref().map_or(&t.value, |a| &a.name.value).clone();
                if !derive
                    && role.write.is_none()
                    && alias.is_some()
                    && self.r.merged_layer(key, &exposed)
                {
                    self.merged.push(key);
                    return Ok(Vec::new());
                }
                let cmds = role.cmds(&exposed);
                if derive {
                    if !with_hints.is_empty() || !partitions.is_empty() || !index_hints.is_empty() {
                        return Err(unsupported("hints on a protected table in a USING join"));
                    }
                    let name_q = ids
                        .iter()
                        .map(|i| quote(&i.value))
                        .collect::<Vec<_>>()
                        .join(".");
                    let p = self.r.predicate(key, &cmds, &name_q)?;
                    let start = self.off(ids[0].span.start)?;
                    let last = alias.as_ref().map_or(t, |a| &a.name);
                    let li = self.tok_at(last.span.start)?;
                    let end = self.toks.end(li);
                    let replacement = format!(
                        "(SELECT * FROM {name_q} WHERE ({p})) AS {}",
                        quote(&exposed)
                    );
                    self.edits.push(Edit {
                        start,
                        end,
                        with: replacement.clone(),
                    });
                    let parsed = Parser::new(&MySqlDialect {})
                        .try_with_sql(&format!("SELECT * FROM {replacement}"))
                        .and_then(|mut p| p.parse_statements())
                        .map_err(|_| unsupported("a derived table doesn't parse"))?;
                    let Some(Statement::Query(q)) = parsed.into_iter().next() else {
                        return Err(unsupported("a derived table doesn't parse"));
                    };
                    let SetExpr::Select(s) = *q.body else {
                        return Err(unsupported("a derived table doesn't parse"));
                    };
                    let Some(twj) = s.from.into_iter().next() else {
                        return Err(unsupported("a derived table doesn't parse"));
                    };
                    *f = twj.relation;
                    self.filtered.push(key);
                    return Ok(Vec::new());
                }
                let qual = match alias {
                    Some(a) => quote(&a.name.value),
                    None => ids
                        .iter()
                        .map(|i| quote(&i.value))
                        .collect::<Vec<_>>()
                        .join("."),
                };
                let text = self.r.predicate(key, &cmds, &qual)?;
                Ok(vec![Pred { text, key }])
            }
            TableFactor::Derived { .. } => Ok(Vec::new()),
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                if derive {
                    // A nested join under USING: protected tables inside
                    // can't be placed exactly.
                    let mut probe = Vec::new();
                    let _ = Visit::visit(&**table_with_joins, &mut NameProbe(&mut probe));
                    if probe
                        .iter()
                        .any(|(d, t)| self.r.protected(d.as_deref(), t).is_some())
                    {
                        return Err(unsupported("a protected table in a nested USING join"));
                    }
                    return Ok(Vec::new());
                }
                self.tree(table_with_joins, scope, role)
            }
            other => {
                if self.mentions_any(&other.to_string()) {
                    return Err(unsupported("this FROM element over a protected table"));
                }
                Ok(Vec::new())
            }
        }
    }
}

/// Names of called functions (last part, as written).
struct Calls(Vec<String>);

impl sqlparser::ast::Visitor for Calls {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if let Expr::Function(f) = e
            && let Some(ObjectNamePart::Identifier(i)) = f.name.0.last()
        {
            self.0.push(i.value.clone());
        }
        ControlFlow::Continue(())
    }
}

struct NameProbe<'a>(&'a mut Vec<(Option<String>, String)>);

impl sqlparser::ast::Visitor for NameProbe<'_> {
    type Break = ();

    fn pre_visit_relation(&mut self, n: &ObjectName) -> ControlFlow<()> {
        if let Some(p) = parts(n) {
            match p.as_slice() {
                [t] => self.0.push((None, t.value.clone())),
                [d, t] => self.0.push((Some(d.value.clone()), t.value.clone())),
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }
}

/// The first token of a trailing `LOCK IN SHARE MODE`.
fn share_lock(t: &Toks<'_>) -> Option<usize> {
    let mut n = t.toks.len();
    if n > 0 && t.punct(n - 1, ";") {
        n -= 1;
    }
    let i = n.checked_sub(4)?;
    (t.word(i, "LOCK") && t.word(i + 1, "IN") && t.word(i + 2, "SHARE") && t.word(i + 3, "MODE"))
        .then_some(i)
}

fn combine(preds: &[Pred]) -> String {
    if preds.len() == 1 {
        return preds[0].text.clone();
    }
    preds
        .iter()
        .map(|p| format!("({})", p.text))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Whether `sql` reads or writes a protected table in a way no rewrite
/// covers: the user variable the MariaDB triggers read (`@sprig_rls`), in
/// any spelling.
#[must_use]
pub fn names_context_variable(sql: &[u8], lex_opts: &LexOptions) -> bool {
    let Ok(all) = lex::lex(sql, lex_opts) else {
        return true;
    };
    all.iter().any(|t| {
        if t.kind != TokenKind::UserVar {
            return false;
        }
        let raw = &t.bytes(sql)[1..];
        let v = match raw.first() {
            Some(b'\'' | b'"') => {
                lex::string_value(raw, lex_opts.no_backslash_escapes).unwrap_or_default()
            }
            _ => ident_value(raw).into_owned(),
        };
        String::from_utf8_lossy(&v)
            .to_ascii_lowercase()
            .starts_with("sprig_rls")
    })
}

#[cfg(test)]
mod tests;
