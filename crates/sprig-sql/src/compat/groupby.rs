//! MySQL 8.4's `ONLY_FULL_GROUP_BY` on port 3306 (chapter 28, build step
//! 8), with the `DISTINCT` rule that goes with it, and MySQL's
//! aggregate-in-`ORDER BY` rule, which MariaDB doesn't have.
//!
//! MariaDB never runs `ONLY_FULL_GROUP_BY` for a 3306 session
//! (`decisions/r4-interim.md` T1): its version has no functional
//! dependencies (MDEV-11588), so it refuses queries MySQL accepts, such as
//! selecting `c.name` grouped by `c.id`. The gateway applies MySQL's rule
//! itself, before the statement is sent:
//!
//! - **1055** (`ER_WRONG_FIELD_WITH_GROUP`): with `GROUP BY`, an
//!   expression of the select list, of `ORDER BY` or of a window's
//!   `PARTITION BY` / `ORDER BY` holds a column that is neither grouped nor
//!   functionally dependent on the grouped columns.
//! - **1140** (`ER_MIX_OF_GROUP_FUNC_AND_FIELDS_V2`): an aggregate without
//!   `GROUP BY` next to a column that isn't constant.
//! - **3065** (with the mode): `SELECT DISTINCT` ordered by a column that
//!   isn't in the select list.
//! - **3029** (in every `sql_mode`, as on MySQL): `ORDER BY` with an
//!   aggregate in a query that doesn't aggregate.
//!
//! Functional dependencies follow MySQL's rules, as measured against MySQL
//! 8.4.11: grouped columns; `col = constant` and `col = col` in `WHERE` and
//! in inner joins' `ON` (only `=`, also `NOT (a <> b)` and a one-value
//! `IN`); a primary key or a unique key of `NOT NULL` columns covered by
//! the set gives the whole table; in a `LEFT JOIN` an equality in `ON`
//! gives the inner table's column from the outer side's, never the other
//! way; `USING` columns; derived tables and CTEs that MySQL merges carry
//! their tables' dependencies, and a grouped derived table is keyed by its
//! grouped columns. `WITH ROLLUP` uses the grouped columns only. A constant
//! is a literal, a parameter, or arithmetic and `CAST` over them: not a
//! user variable, `NOW()` or a subquery.
//!
//! **When in doubt, accept.** A query whose analysis would need something
//! the gateway doesn't model (a view's definition, a table function, a
//! table it can't read the definition of, an ambiguous name) is sent as is:
//! MariaDB then runs it as MySQL 5.6 would. Refusing a query MySQL accepts
//! would break a working application; accepting one MySQL refuses breaks
//! none.
//!
//! [`check`] is a pure function of the statement, the session's current
//! database and the definitions [`Meta`] gives. A first pass decides the
//! common shapes (every expression grouped by its text) without any
//! definitions; otherwise [`Outcome::Need`] names the tables whose
//! definitions the caller should load before asking again.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use sqlparser::ast::{
    BinaryOperator, Cte, Expr, GroupByExpr, GroupByWithModifier, Ident, JoinConstraint,
    JoinOperator, NamedWindowExpr, ObjectName, ObjectNamePart, OrderBy, OrderByKind, Query, Select,
    SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement, TableFactor, TableWithJoins,
    UnaryOperator, Value, Visit, Visitor, WindowType,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::lex::{LexOptions, lex};
use crate::names::rewrite::ast::parse_view;

/// One column of a table's definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMeta {
    /// The name as defined.
    pub name: String,
    /// `NOT NULL`.
    pub not_null: bool,
    /// Not `INVISIBLE` (`SELECT *` lists it).
    pub visible: bool,
}

/// What [`check`] needs of a table or view: its columns and, for a table,
/// its unique keys.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableMeta {
    /// The columns, in order.
    pub columns: Vec<ColumnMeta>,
    /// Unique keys (the primary key among them), as indexes into
    /// `columns`. Keys with a nullable column are left out by [`check`].
    pub keys: Vec<Vec<usize>>,
    /// A view: its dependencies aren't modelled.
    pub view: bool,
}

/// The definitions [`check`] reads.
pub trait Meta: Send + Sync {
    /// The table `db`.`table` (logical names): `None` when not loaded yet,
    /// `Some(None)` when it can't be read (missing, sharded, a temporary
    /// table), `Some(Some(m))` otherwise.
    fn table(&self, db: &str, table: &str) -> Option<Option<Arc<TableMeta>>>;
}

/// No definitions at all (the first pass).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoMeta;

impl Meta for NoMeta {
    fn table(&self, _db: &str, _table: &str) -> Option<Option<Arc<TableMeta>>> {
        None
    }
}

/// The session state [`check`] reads.
#[derive(Clone, Copy)]
pub struct Check<'a> {
    /// The session's current (logical) database.
    pub current_db: Option<&'a str>,
    /// `ONLY_FULL_GROUP_BY` is in the client's `sql_mode`.
    pub only_full_group_by: bool,
    /// Table definitions.
    pub meta: &'a dyn Meta,
}

/// MySQL's error for a refused statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// 1055, 1140, 3065 or 3029.
    pub code: u16,
    /// Its SQLSTATE.
    pub sqlstate: &'static str,
    /// MySQL 8.4's message.
    pub message: String,
}

/// What [`check`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Send the statement.
    Accept,
    /// Refuse it with MySQL's error.
    Refuse(Refusal),
    /// Load these tables' definitions (logical database, table) and ask
    /// again.
    Need(Vec<(String, String)>),
}

/// Aggregate functions (MySQL 8.4 and MariaDB's own).
const AGGREGATES: &[&str] = &[
    "AVG",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
    "COUNT",
    "GROUP_CONCAT",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
    "MAX",
    "MIN",
    "STD",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "SUM",
    "VAR_POP",
    "VAR_SAMP",
    "VARIANCE",
];

/// Functions whose arguments are exempt without making the query
/// aggregate.
const EXEMPT: &[&str] = &["ANY_VALUE", "GROUPING"];

/// Statements longer than this (significant tokens) aren't checked.
const MAX_TOKENS: usize = 6000;

/// Whether the text can need the check at all: a cheap scan before any
/// parsing (`GROUP`, `DISTINCT`, `HAVING` or an aggregate's name).
#[must_use]
pub fn may_need(sql: &[u8]) -> bool {
    const WORDS: &[&[u8]] = &[
        b"group",
        b"distinct",
        b"having",
        b"count",
        b"sum",
        b"max",
        b"min",
        b"avg",
        b"std",
        b"bit_",
        b"var",
        b"json_arrayagg",
        b"json_objectagg",
    ];
    if sql.len() < 8 {
        return false;
    }
    let lower = sql.to_ascii_lowercase();
    WORDS
        .iter()
        .any(|w| lower.windows(w.len()).any(|x| x == *w))
}

/// Checks one statement (see the module docs).
#[must_use]
pub fn check(src: &[u8], opts: &LexOptions, c: &Check<'_>) -> Outcome {
    let Some(stmt) = parse(src, *opts) else {
        return Outcome::Accept;
    };
    if !checked_kind(&stmt) {
        return Outcome::Accept;
    }
    // First pass: without definitions.
    let mut fast = Analyze::new(c, Mode::Fast);
    match stmt.visit(&mut fast) {
        ControlFlow::Break(r) => return Outcome::Refuse(r),
        ControlFlow::Continue(()) if !fast.need_full => return Outcome::Accept,
        ControlFlow::Continue(()) => {}
    }
    // The definitions of every table the statement names.
    let mut names = Relations::default();
    let _ = stmt.visit(&mut names);
    let mut need = BTreeSet::new();
    for name in &names.tables {
        if names.ctes.contains(&name.to_ascii_lowercase_last()) {
            continue;
        }
        let Some((db, table)) = db_table(name, c.current_db) else {
            continue;
        };
        if c.meta.table(&db, &table).is_none() {
            need.insert((db, table));
        }
    }
    if !need.is_empty() {
        return Outcome::Need(need.into_iter().collect());
    }
    let mut full = Analyze::new(c, Mode::Full);
    match stmt.visit(&mut full) {
        ControlFlow::Break(r) => Outcome::Refuse(r),
        ControlFlow::Continue(()) => Outcome::Accept,
    }
}

fn parse(src: &[u8], opts: LexOptions) -> Option<Statement> {
    let text = std::str::from_utf8(src).ok()?;
    let tokens = lex(src, &opts).ok()?;
    let toks: Vec<_> = tokens.into_iter().collect();
    if toks.iter().filter(|t| !t.kind.is_trivia()).count() > MAX_TOKENS {
        return None;
    }
    let view = parse_view(text, &toks)?;
    let mut stmts = Parser::new(&MySqlDialect {})
        .with_recursion_limit(64)
        .try_with_sql(&view)
        .ok()?
        .parse_statements()
        .ok()?;
    if stmts.len() != 1 {
        return None;
    }
    stmts.pop()
}

/// The statements MySQL checks when it prepares them. `CREATE VIEW` isn't
/// one: MySQL checks a view's query when it is read.
fn checked_kind(s: &Statement) -> bool {
    match s {
        Statement::Query(_)
        | Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::CreateTable(_) => true,
        Statement::Explain { statement, .. } => checked_kind(statement),
        _ => false,
    }
}

trait LastLower {
    fn to_ascii_lowercase_last(&self) -> String;
}

impl LastLower for ObjectName {
    fn to_ascii_lowercase_last(&self) -> String {
        self.0
            .last()
            .and_then(ObjectNamePart::as_ident)
            .map(|i| i.value.to_ascii_lowercase())
            .unwrap_or_default()
    }
}

/// `(database, table)` of a relation name, with the current database for
/// an unqualified one.
fn db_table(name: &ObjectName, current: Option<&str>) -> Option<(String, String)> {
    let parts: Vec<&Ident> = name
        .0
        .iter()
        .map(ObjectNamePart::as_ident)
        .collect::<Option<_>>()?;
    match parts.as_slice() {
        [t] => Some((current?.to_owned(), t.value.clone())),
        [d, t] => Some((d.value.clone(), t.value.clone())),
        _ => None,
    }
}

/// Every relation name and every CTE name in a statement.
#[derive(Default)]
struct Relations {
    tables: Vec<ObjectName>,
    ctes: HashSet<String>,
}

impl Visitor for Relations {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        if let Some(w) = &q.with {
            for cte in &w.cte_tables {
                self.ctes.insert(cte.alias.name.value.to_ascii_lowercase());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, r: &ObjectName) -> ControlFlow<()> {
        self.tables.push(r.clone());
        ControlFlow::Continue(())
    }
}

// ---- Scopes -------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A base table: keys apply.
    Base,
    /// A view: its columns are known, its dependencies aren't.
    View,
    /// A derived table or CTE.
    Derived,
}

#[derive(Debug, Clone)]
struct Col {
    name: String,
    lower: String,
    visible: bool,
}

#[derive(Debug, Clone)]
struct Entry {
    /// The name that qualifies its columns (alias, or the table name).
    name: String,
    /// The database, for base tables and views, as the message names it.
    db: Option<String>,
    /// The table's own name (unaliased base tables and views): `db.t.c`.
    table: Option<String>,
    aliased: bool,
    kind: Kind,
    cols: Vec<Col>,
    /// Inside a merged derived table: not reachable by name.
    hidden: bool,
}

/// How a name resolved in a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Res {
    /// A column of this scope.
    Atom(usize),
    /// Not a column of this scope (an outer reference, or unknown to
    /// MariaDB too).
    NotHere,
    /// More than one column matches, or the scope can't tell.
    Unsure,
}

/// The `FROM` clause of one query block: its tables and the functional
/// dependencies among their columns.
#[derive(Debug, Default)]
struct Scope {
    entries: Vec<Entry>,
    /// Atom id by (entry, lower-case column).
    atoms: HashMap<(usize, String), usize>,
    atom_of: Vec<(usize, String)>,
    /// `lhs -> rhs` dependencies.
    rules: Vec<(Vec<usize>, usize)>,
    /// `USING` / `NATURAL` columns, unqualified: lower-case name to atom.
    coalesced: Vec<(String, usize)>,
    /// Something the analysis doesn't model: never refuse in this block.
    uncertain: bool,
    /// Select-list aliases of the block (subquery frames resolve them).
    aliases: HashSet<String>,
}

impl Scope {
    fn atom(&mut self, entry: usize, lower: &str) -> usize {
        if let Some(&a) = self.atoms.get(&(entry, lower.to_owned())) {
            return a;
        }
        let a = self.atom_of.len();
        self.atom_of.push((entry, lower.to_owned()));
        self.atoms.insert((entry, lower.to_owned()), a);
        a
    }

    fn rule(&mut self, lhs: Vec<usize>, rhs: usize) {
        if !lhs.contains(&rhs) {
            self.rules.push((lhs, rhs));
        }
    }

    fn both(&mut self, a: usize, b: usize) {
        self.rule(vec![a], b);
        self.rule(vec![b], a);
    }

    fn entry_col(&mut self, e: usize, lower: &str) -> Option<usize> {
        if self.entries[e].cols.iter().any(|c| c.lower == lower) {
            Some(self.atom(e, lower))
        } else {
            None
        }
    }

    /// Resolves a column reference (`c`, `t.c`, `db.t.c`).
    fn resolve(&mut self, parts: &[Ident]) -> Res {
        let lower: Vec<String> = parts.iter().map(|p| p.value.to_ascii_lowercase()).collect();
        match lower.as_slice() {
            [c] => {
                if let Some(&(_, a)) = self.coalesced.iter().find(|(n, _)| n == c) {
                    return Res::Atom(a);
                }
                let hits: Vec<usize> = (0..self.entries.len())
                    .filter(|&e| {
                        !self.entries[e].hidden
                            && self.entries[e].cols.iter().any(|x| &x.lower == c)
                    })
                    .collect();
                match hits.as_slice() {
                    [] => Res::NotHere,
                    [e] => Res::Atom(self.atom(*e, c)),
                    _ => Res::Unsure,
                }
            }
            [q, c] => {
                let hits: Vec<usize> = (0..self.entries.len())
                    .filter(|&e| {
                        !self.entries[e].hidden && self.entries[e].name.eq_ignore_ascii_case(q)
                    })
                    .collect();
                match hits.as_slice() {
                    [] => Res::NotHere,
                    [e] => self.entry_col(*e, c).map_or(Res::NotHere, Res::Atom),
                    _ => Res::Unsure,
                }
            }
            [d, t, c] => {
                let hits: Vec<usize> = (0..self.entries.len())
                    .filter(|&e| {
                        let x = &self.entries[e];
                        !x.hidden
                            && !x.aliased
                            && x.db.as_deref().is_some_and(|v| v.eq_ignore_ascii_case(d))
                            && x.table
                                .as_deref()
                                .is_some_and(|v| v.eq_ignore_ascii_case(t))
                    })
                    .collect();
                match hits.as_slice() {
                    [] => Res::NotHere,
                    [e] => self.entry_col(*e, c).map_or(Res::NotHere, Res::Atom),
                    _ => Res::Unsure,
                }
            }
            _ => Res::Unsure,
        }
    }

    /// The closure of `start` under the rules.
    fn closure(&self, start: &HashSet<usize>) -> HashSet<usize> {
        let mut d = start.clone();
        loop {
            let before = d.len();
            for (lhs, rhs) in &self.rules {
                if !d.contains(rhs) && lhs.iter().all(|a| d.contains(a)) {
                    d.insert(*rhs);
                }
            }
            if d.len() == before {
                return d;
            }
        }
    }

    /// The name MySQL's messages give an atom: `db.t.c`, or `dt.c` for a
    /// derived table.
    fn display(&self, atom: usize) -> String {
        let (e, lower) = &self.atom_of[atom];
        let entry = &self.entries[*e];
        let col = entry
            .cols
            .iter()
            .find(|c| &c.lower == lower)
            .map_or(lower.as_str(), |c| c.name.as_str());
        match (&entry.kind, &entry.db) {
            (Kind::Base | Kind::View, Some(db)) => format!("{db}.{}.{col}", entry.name),
            _ => format!("{}.{col}", entry.name),
        }
    }

    /// Copies `inner`'s entries (hidden), atoms and rules into this scope;
    /// returns the atom map.
    fn absorb(&mut self, inner: &Scope) -> Vec<usize> {
        let base = self.entries.len();
        for e in &inner.entries {
            let mut e = e.clone();
            e.hidden = true;
            self.entries.push(e);
        }
        let map: Vec<usize> = inner
            .atom_of
            .iter()
            .map(|(e, c)| self.atom(base + e, c))
            .collect();
        for (lhs, rhs) in &inner.rules {
            self.rule(lhs.iter().map(|a| map[*a]).collect(), map[*rhs]);
        }
        if inner.uncertain {
            self.uncertain = true;
        }
        map
    }
}

/// A side of an equality.
enum Side {
    Col(usize),
    Const,
    Other,
}

/// How an equality's dependencies apply.
#[derive(Clone, Copy)]
enum EqMode<'a> {
    /// `WHERE` and inner joins: both ways.
    Both,
    /// An outer join's `ON`: only into the inner side's entries.
    Into(&'a HashSet<usize>),
}

fn conjuncts<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
    match e {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        Expr::Nested(x) => conjuncts(x, out),
        _ => out.push(e),
    }
}

/// `a = b` in its forms MySQL reads as an equality.
fn equality(e: &Expr) -> Option<(&Expr, &Expr)> {
    match e {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => Some((left, right)),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => match strip(expr) {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::NotEq,
                right,
            } => Some((left, right)),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } if list.len() == 1 => Some((expr, &list[0])),
        Expr::Nested(x) => equality(x),
        _ => None,
    }
}

fn strip(e: &Expr) -> &Expr {
    match e {
        Expr::Nested(x) => strip(x),
        _ => e,
    }
}

/// A constant as MySQL's dependency rules see one.
fn is_const(e: &Expr) -> bool {
    match e {
        Expr::Value(_) | Expr::TypedString { .. } => true,
        Expr::Nested(expr) | Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } => is_const(expr),
        Expr::BinaryOp { left, op, right } => {
            matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
                    | BinaryOperator::StringConcat
            ) && is_const(left)
                && is_const(right)
        }
        _ => false,
    }
}

/// A user variable (`@v`), which sqlparser-rs reads as an identifier.
fn user_var(i: &Ident) -> bool {
    i.quote_style.is_none() && i.value.starts_with('@')
}

fn column_parts(e: &Expr) -> Option<&[Ident]> {
    match e {
        Expr::Identifier(i) if !user_var(i) => Some(std::slice::from_ref(i)),
        Expr::CompoundIdentifier(v) => Some(v),
        Expr::Nested(x) => column_parts(x),
        _ => None,
    }
}

/// How a merged derived table's output column follows from its tables.
enum Out {
    /// The inner column itself.
    Same(usize),
    /// An expression of these inner columns.
    From(Vec<usize>),
    /// Nothing known.
    Opaque,
}

/// Builds scopes; owns the CTE stack and the definitions.
struct Builder<'a, 'c> {
    c: &'a Check<'c>,
    ctes: &'a [Vec<Cte>],
}

impl Builder<'_, '_> {
    fn cte(&self, name: &str) -> Option<&Cte> {
        self.ctes
            .iter()
            .rev()
            .flat_map(|level| level.iter())
            .find(|c| c.alias.name.value.eq_ignore_ascii_case(name))
    }

    /// The scope of a block's `FROM` with its `WHERE` (`None` for a block
    /// with no `FROM`).
    fn block(&self, sel: &Select, depth: usize) -> Scope {
        let mut s = Scope::default();
        if depth > 8 {
            s.uncertain = true;
            return s;
        }
        let mut deferred: Vec<(&JoinOperator, HashSet<usize>, HashSet<usize>)> = Vec::new();
        for twj in &sel.from {
            self.join_tree(&mut s, twj, &mut deferred, depth);
        }
        for (op, left, right) in deferred {
            Self::constraint(&mut s, op, &left, &right);
        }
        if let Some(w) = &sel.selection {
            add_equalities(&mut s, w, EqMode::Both);
        }
        for item in &sel.projection {
            if let SelectItem::ExprWithAlias { alias, .. } = item {
                s.aliases.insert(alias.value.to_ascii_lowercase());
            }
        }
        s
    }

    /// Adds a `FROM` item and its joins; returns the entries it added.
    fn join_tree<'s>(
        &self,
        s: &mut Scope,
        twj: &'s TableWithJoins,
        deferred: &mut Vec<(&'s JoinOperator, HashSet<usize>, HashSet<usize>)>,
        depth: usize,
    ) -> HashSet<usize> {
        let mut cur = self.factor(s, &twj.relation, deferred, depth);
        for j in &twj.joins {
            let new = self.factor(s, &j.relation, deferred, depth);
            deferred.push((&j.join_operator, cur.clone(), new.clone()));
            cur.extend(new);
        }
        cur
    }

    fn factor<'s>(
        &self,
        s: &mut Scope,
        f: &'s TableFactor,
        deferred: &mut Vec<(&'s JoinOperator, HashSet<usize>, HashSet<usize>)>,
        depth: usize,
    ) -> HashSet<usize> {
        let mut added = HashSet::new();
        match f {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } => {
                let single = name.0.len() == 1;
                let last = name.to_ascii_lowercase_last();
                if single && let Some(cte) = self.cte(&last) {
                    let shown = alias
                        .as_ref()
                        .map_or_else(|| cte.alias.name.value.clone(), |a| a.name.value.clone());
                    let cte_cols: Vec<Ident> =
                        cte.alias.columns.iter().map(|c| c.name.clone()).collect();
                    let col_alias = alias
                        .as_ref()
                        .filter(|a| !a.columns.is_empty())
                        .map_or(cte_cols, |a| {
                            a.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>()
                        });
                    added.insert(self.derived(s, &cte.query, shown, &col_alias, depth));
                    return added;
                }
                let Some((db, table)) = db_table(name, self.c.current_db) else {
                    s.uncertain = true;
                    return added;
                };
                let Some(Some(meta)) = self.c.meta.table(&db, &table) else {
                    s.uncertain = true;
                    return added;
                };
                let shown_db = match name.0.len() {
                    2 => name.0[0].as_ident().map_or(db.clone(), |i| i.value.clone()),
                    _ => db,
                };
                let cols: Vec<Col> = meta
                    .columns
                    .iter()
                    .map(|c| Col {
                        name: c.name.clone(),
                        lower: c.name.to_ascii_lowercase(),
                        visible: c.visible,
                    })
                    .collect();
                let e = s.entries.len();
                s.entries.push(Entry {
                    name: alias
                        .as_ref()
                        .map_or_else(|| table.clone(), |a| a.name.value.clone()),
                    db: Some(shown_db),
                    table: Some(table),
                    aliased: alias.is_some(),
                    kind: if meta.view { Kind::View } else { Kind::Base },
                    cols,
                    hidden: false,
                });
                if meta.view {
                    s.uncertain = true;
                } else {
                    for key in &meta.keys {
                        if key.is_empty()
                            || key
                                .iter()
                                .any(|&i| meta.columns.get(i).is_none_or(|c| !c.not_null))
                        {
                            continue;
                        }
                        let lhs: Vec<usize> = key
                            .iter()
                            .map(|&i| s.atom(e, &meta.columns[i].name.to_ascii_lowercase()))
                            .collect();
                        for c in &meta.columns {
                            let rhs = s.atom(e, &c.name.to_ascii_lowercase());
                            s.rule(lhs.clone(), rhs);
                        }
                    }
                }
                added.insert(e);
            }
            TableFactor::Derived {
                lateral: false,
                subquery,
                alias: Some(alias),
                ..
            } => {
                let cols: Vec<Ident> = alias.columns.iter().map(|c| c.name.clone()).collect();
                added.insert(self.derived(s, subquery, alias.name.value.clone(), &cols, depth));
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias: None,
                ..
            } => {
                added = self.join_tree(s, table_with_joins, deferred, depth);
            }
            _ => s.uncertain = true,
        }
        added
    }

    /// A derived table or CTE reference; returns its entry.
    #[allow(clippy::too_many_lines)] // the cases of MySQL's merge, one by one
    fn derived(
        &self,
        s: &mut Scope,
        q: &Query,
        name: String,
        col_alias: &[Ident],
        depth: usize,
    ) -> usize {
        let e = s.entries.len();
        s.entries.push(Entry {
            name,
            db: None,
            table: None,
            aliased: true,
            kind: Kind::Derived,
            cols: Vec::new(),
            hidden: false,
        });
        let mut level: Vec<Cte> = Vec::new();
        if let Some(w) = &q.with {
            if w.recursive {
                s.uncertain = true;
                return e;
            }
            level.clone_from(&w.cte_tables);
        }
        let mut stack = self.ctes.to_vec();
        stack.push(level);
        let inner_b = Builder {
            c: self.c,
            ctes: &stack,
        };
        let SetExpr::Select(sel) = q.body.as_ref() else {
            // A union and the like: columns from its first block, no
            // dependencies.
            match first_select(&q.body) {
                Some(first) => {
                    let mut inner = inner_b.block(first, depth + 1);
                    let names = output_names(first, &mut inner);
                    set_cols(s, e, names, col_alias);
                }
                None => s.uncertain = true,
            }
            return e;
        };
        let mut inner = inner_b.block(sel, depth + 1);
        let Some(names) = output_names(sel, &mut inner) else {
            s.uncertain = true;
            return e;
        };
        set_cols(s, e, Some(names.clone()), col_alias);
        if s.entries[e].cols.len() != names.len() {
            s.uncertain = true;
            return e;
        }
        let grouped = !group_exprs(&sel.group_by).is_empty();
        let mut agg = AggScan::default();
        for item in &sel.projection {
            let _ = item.visit(&mut agg);
        }
        if let Some(h) = &sel.having {
            let _ = h.visit(&mut agg);
        }
        let outs: Vec<usize> = (0..names.len())
            .map(|i| {
                let lower = s.entries[e].cols[i].lower.clone();
                s.atom(e, &lower)
            })
            .collect();
        if agg.window || rollup(&sel.group_by) {
            return e;
        }
        if grouped {
            // Keyed by its grouped columns, when they're all output.
            let mut key = Vec::new();
            for g in group_exprs(&sel.group_by) {
                let pos = names.iter().position(|(_, ex)| {
                    ex.as_ref()
                        .is_some_and(|x| x.to_string().eq_ignore_ascii_case(&g.to_string()))
                });
                match pos {
                    Some(p) => key.push(outs[p]),
                    None => return e,
                }
            }
            for &o in &outs {
                s.rule(key.clone(), o);
            }
            return e;
        }
        if agg.aggregate {
            return e;
        }
        // Merged: the inner tables' dependencies, through the output
        // columns' expressions (resolved first, so the atom map covers
        // them).
        let resolved: Vec<Out> = names
            .iter()
            .map(|(_, ex)| {
                let Some(ex) = ex else {
                    return Out::Opaque;
                };
                if let Some(parts) = column_parts(ex) {
                    return match inner.resolve(parts) {
                        Res::Atom(a) => Out::Same(a),
                        _ => Out::Opaque,
                    };
                }
                let mut refs = RefScan::default();
                let _ = ex.visit(&mut refs);
                if refs.subquery {
                    return Out::Opaque;
                }
                let mut lhs = Vec::new();
                for r in &refs.refs {
                    match inner.resolve(r) {
                        Res::Atom(a) => lhs.push(a),
                        Res::NotHere => {}
                        Res::Unsure => return Out::Opaque,
                    }
                }
                Out::From(lhs)
            })
            .collect();
        let map = s.absorb(&inner);
        for (i, r) in resolved.into_iter().enumerate() {
            match r {
                Out::Same(a) => s.both(outs[i], map[a]),
                Out::From(lhs) => s.rule(lhs.into_iter().map(|a| map[a]).collect(), outs[i]),
                Out::Opaque => {}
            }
        }
        e
    }

    /// A join's `ON` / `USING` / `NATURAL`.
    fn constraint(s: &mut Scope, op: &JoinOperator, left: &HashSet<usize>, right: &HashSet<usize>) {
        let (c, inner): (&JoinConstraint, Option<&HashSet<usize>>) = match op {
            JoinOperator::Join(c)
            | JoinOperator::Inner(c)
            | JoinOperator::CrossJoin(c)
            | JoinOperator::StraightJoin(c) => (c, None),
            JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (c, Some(right)),
            JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (c, Some(left)),
            _ => {
                s.uncertain = true;
                return;
            }
        };
        match c {
            JoinConstraint::On(e) => {
                add_equalities(s, e, inner.map_or(EqMode::Both, EqMode::Into));
            }
            JoinConstraint::Using(names) => {
                for n in names {
                    let Some(Some(id)) = n.0.last().map(ObjectNamePart::as_ident) else {
                        s.uncertain = true;
                        continue;
                    };
                    Self::using(s, &id.value.to_ascii_lowercase(), left, right, inner);
                }
            }
            JoinConstraint::Natural => {
                let names: Vec<String> = left
                    .iter()
                    .flat_map(|&e| s.entries[e].cols.iter().map(|c| c.lower.clone()))
                    .filter(|c| {
                        right
                            .iter()
                            .any(|&e| s.entries[e].cols.iter().any(|x| &x.lower == c))
                    })
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                for n in names {
                    Self::using(s, &n, left, right, inner);
                }
            }
            JoinConstraint::None => {}
        }
    }

    fn using(
        s: &mut Scope,
        col: &str,
        left: &HashSet<usize>,
        right: &HashSet<usize>,
        inner: Option<&HashSet<usize>>,
    ) {
        let find = |s: &mut Scope, side: &HashSet<usize>| {
            let mut hits: Vec<usize> = side
                .iter()
                .copied()
                .filter(|&e| s.entries[e].cols.iter().any(|c| c.lower == col))
                .collect();
            hits.sort_unstable();
            match hits.as_slice() {
                [e] => Some(s.atom(*e, col)),
                _ => None,
            }
        };
        let (Some(l), Some(r)) = (find(s, left), find(s, right)) else {
            s.uncertain = true;
            return;
        };
        match inner {
            None => {
                s.both(l, r);
                s.coalesced.push((col.to_owned(), l));
            }
            Some(i) if std::ptr::eq(i, right) => {
                s.rule(vec![l], r);
                s.coalesced.push((col.to_owned(), l));
            }
            Some(_) => {
                s.rule(vec![r], l);
                s.coalesced.push((col.to_owned(), r));
            }
        }
    }
}

fn first_select(body: &SetExpr) -> Option<&Select> {
    match body {
        SetExpr::Select(s) => Some(s),
        SetExpr::SetOperation { left, .. } => first_select(left),
        SetExpr::Query(q) => first_select(&q.body),
        _ => None,
    }
}

fn set_cols(
    s: &mut Scope,
    e: usize,
    names: Option<Vec<(String, Option<Expr>)>>,
    col_alias: &[Ident],
) {
    let Some(names) = names else {
        s.uncertain = true;
        return;
    };
    let cols: Vec<Col> = names
        .iter()
        .enumerate()
        .map(|(i, (n, _))| {
            let name = col_alias
                .get(i)
                .map_or_else(|| n.clone(), |a| a.value.clone());
            Col {
                lower: name.to_ascii_lowercase(),
                name,
                visible: true,
            }
        })
        .collect();
    s.entries[e].cols = cols;
}

/// A block's output columns: name and expression (`None` for a column
/// from `*`, whose expression is the inner column itself).
fn output_names(sel: &Select, inner: &mut Scope) -> Option<Vec<(String, Option<Expr>)>> {
    let mut out = Vec::new();
    for item in &sel.projection {
        match item {
            SelectItem::UnnamedExpr(e) => {
                let name = match column_parts(e) {
                    Some(p) => p.last()?.value.clone(),
                    None => e.to_string(),
                };
                out.push((name, Some(e.clone())));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                out.push((alias.value.clone(), Some(expr.clone())));
            }
            SelectItem::Wildcard(_) => {
                for (ei, entry) in inner.entries.clone().iter().enumerate() {
                    if entry.hidden {
                        continue;
                    }
                    for c in entry.cols.iter().filter(|c| c.visible) {
                        out.push((c.name.clone(), Some(qualified(&entry.name, &c.name))));
                        let _ = inner.atom(ei, &c.lower);
                    }
                }
            }
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(n), _) => {
                let q = n.to_ascii_lowercase_last();
                let entry = inner
                    .entries
                    .iter()
                    .find(|x| !x.hidden && x.name.eq_ignore_ascii_case(&q))?
                    .clone();
                for c in entry.cols.iter().filter(|c| c.visible) {
                    out.push((c.name.clone(), Some(qualified(&entry.name, &c.name))));
                }
            }
            _ => return None,
        }
    }
    Some(out)
}

fn qualified(t: &str, c: &str) -> Expr {
    Expr::CompoundIdentifier(vec![Ident::new(t), Ident::new(c)])
}

#[allow(clippy::many_single_char_names)]
fn add_equalities(s: &mut Scope, e: &Expr, mode: EqMode<'_>) {
    let mut cs = Vec::new();
    conjuncts(e, &mut cs);
    for c in cs {
        let Some((l, r)) = equality(c) else {
            continue;
        };
        let side = |s: &mut Scope, x: &Expr| -> Side {
            if let Some(p) = column_parts(x) {
                return match s.resolve(p) {
                    Res::Atom(a) => Side::Col(a),
                    // An outer reference is a constant here.
                    Res::NotHere => Side::Const,
                    Res::Unsure => Side::Other,
                };
            }
            if is_const(x) {
                Side::Const
            } else {
                Side::Other
            }
        };
        let (l, r) = (side(s, l), side(s, r));
        let inside = |s: &Scope, a: usize, set: &HashSet<usize>| set.contains(&s.atom_of[a].0);
        match (mode, l, r) {
            (EqMode::Both, Side::Col(a), Side::Col(b)) => s.both(a, b),
            (EqMode::Both, Side::Col(a), Side::Const)
            | (EqMode::Both, Side::Const, Side::Col(a)) => {
                s.rule(Vec::new(), a);
            }
            (EqMode::Into(set), Side::Col(a), Side::Col(b)) => {
                match (inside(s, a, set), inside(s, b, set)) {
                    (true, true) => s.both(a, b),
                    (true, false) => s.rule(vec![b], a),
                    (false, true) => s.rule(vec![a], b),
                    (false, false) => {}
                }
            }
            (EqMode::Into(set), Side::Col(a), Side::Const)
            | (EqMode::Into(set), Side::Const, Side::Col(a)) => {
                if inside(s, a, set) {
                    s.rule(Vec::new(), a);
                }
            }
            _ => {}
        }
    }
}

fn group_exprs(g: &GroupByExpr) -> &[Expr] {
    match g {
        GroupByExpr::Expressions(v, _) => v,
        GroupByExpr::All(_) => &[],
    }
}

fn rollup(g: &GroupByExpr) -> bool {
    match g {
        GroupByExpr::Expressions(_, m) | GroupByExpr::All(m) => {
            m.iter().any(|x| matches!(x, GroupByWithModifier::Rollup))
        }
    }
}

fn order_exprs(o: Option<&OrderBy>) -> Vec<&Expr> {
    match o.map(|o| &o.kind) {
        Some(OrderByKind::Expressions(v)) => v.iter().map(|x| &x.expr).collect(),
        _ => Vec::new(),
    }
}

fn func_name(f: &sqlparser::ast::Function) -> String {
    f.name.to_ascii_lowercase_last().to_ascii_uppercase()
}

// ---- Expression scans ---------------------------------------------------

/// Column references outside aggregates (no scope).
#[derive(Default)]
struct RefScan {
    refs: Vec<Vec<Ident>>,
    agg: usize,
    subquery: bool,
}

impl Visitor for RefScan {
    type Break = ();

    fn pre_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
        self.subquery = true;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        match e {
            Expr::Function(f) if f.over.is_none() => {
                let n = func_name(f);
                if AGGREGATES.contains(&n.as_str()) || EXEMPT.contains(&n.as_str()) {
                    self.agg += 1;
                }
            }
            Expr::Identifier(i) if self.agg == 0 && !user_var(i) => self.refs.push(vec![i.clone()]),
            Expr::CompoundIdentifier(v) if self.agg == 0 => self.refs.push(v.clone()),
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if let Expr::Function(f) = e
            && f.over.is_none()
        {
            let n = func_name(f);
            if AGGREGATES.contains(&n.as_str()) || EXEMPT.contains(&n.as_str()) {
                self.agg -= 1;
            }
        }
        ControlFlow::Continue(())
    }
}

/// Whether an expression aggregates at its own level, or holds a window
/// function.
#[derive(Default)]
struct AggScan {
    aggregate: bool,
    window: bool,
    depth: usize,
    subquery: bool,
}

impl Visitor for AggScan {
    type Break = ();

    fn pre_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
        self.depth += 1;
        self.subquery = true;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if self.depth == 0
            && let Expr::Function(f) = e
        {
            if f.over.is_some() {
                self.window = true;
            } else if AGGREGATES.contains(&func_name(f).as_str()) {
                self.aggregate = true;
            }
        }
        ControlFlow::Continue(())
    }
}

/// One column reference found in a block's expression.
struct Ref {
    parts: Vec<Ident>,
}

/// Column references of one expression that belong to the block: outside
/// aggregates and window specifications, and the outer references of its
/// subqueries.
struct Scan<'a, 'c> {
    b: &'a Builder<'a, 'c>,
    frames: Vec<Scope>,
    agg: usize,
    window_specs: HashSet<*const Expr>,
    in_window: usize,
    refs: Vec<Ref>,
    /// Inline window specifications at the block's level, in order.
    windows: Vec<Vec<Expr>>,
    unsure: bool,
}

impl<'a, 'c> Scan<'a, 'c> {
    fn new(b: &'a Builder<'a, 'c>) -> Self {
        Self {
            b,
            frames: Vec::new(),
            agg: 0,
            window_specs: HashSet::new(),
            in_window: 0,
            refs: Vec::new(),
            windows: Vec::new(),
            unsure: false,
        }
    }

    fn take(&mut self, parts: &[Ident]) {
        if self.agg > 0 || self.in_window > 0 {
            return;
        }
        // Resolved by a subquery's own tables (innermost first)?
        for f in self.frames.iter_mut().rev() {
            if parts.len() == 1 && f.aliases.contains(&parts[0].value.to_ascii_lowercase()) {
                return;
            }
            match f.resolve(parts) {
                Res::Atom(_) | Res::Unsure => return,
                Res::NotHere if f.uncertain && parts.len() == 1 => return,
                Res::NotHere => {}
            }
        }
        self.refs.push(Ref {
            parts: parts.to_vec(),
        });
    }
}

impl Visitor for Scan<'_, '_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        let frame = match first_select(&q.body) {
            Some(sel) if matches!(q.body.as_ref(), SetExpr::Select(_)) => {
                let mut stack = self.b.ctes.to_vec();
                if let Some(w) = &q.with {
                    stack.push(w.cte_tables.clone());
                }
                let inner = Builder {
                    c: self.b.c,
                    ctes: &stack,
                };
                inner.block(sel, 1)
            }
            _ => Scope {
                uncertain: true,
                ..Scope::default()
            },
        };
        self.frames.push(frame);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
        self.frames.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if self.window_specs.contains(&std::ptr::from_ref(e)) {
            self.in_window += 1;
        }
        match e {
            Expr::Function(f) => match &f.over {
                None => {
                    let n = func_name(f);
                    if AGGREGATES.contains(&n.as_str()) || EXEMPT.contains(&n.as_str()) {
                        self.agg += 1;
                    }
                }
                Some(WindowType::WindowSpec(spec)) => {
                    let exprs: Vec<&Expr> = spec
                        .partition_by
                        .iter()
                        .chain(spec.order_by.iter().map(|o| &o.expr))
                        .collect();
                    for x in &exprs {
                        self.window_specs.insert(std::ptr::from_ref(*x));
                    }
                    if self.frames.is_empty() && self.agg == 0 && self.in_window == 0 {
                        self.windows.push(exprs.into_iter().cloned().collect());
                    }
                }
                Some(WindowType::NamedWindow(_)) => {}
            },
            Expr::Identifier(i) if !user_var(i) => self.take(std::slice::from_ref(i)),
            Expr::CompoundIdentifier(v) => self.take(v),
            Expr::QualifiedWildcard(..) | Expr::Wildcard(_) if self.agg == 0 => self.unsure = true,
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if self.window_specs.contains(&std::ptr::from_ref(e)) {
            self.in_window -= 1;
        }
        if let Expr::Function(f) = e
            && f.over.is_none()
        {
            let n = func_name(f);
            if AGGREGATES.contains(&n.as_str()) || EXEMPT.contains(&n.as_str()) {
                self.agg -= 1;
            }
        }
        ControlFlow::Continue(())
    }
}

// ---- The analysis -------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Without definitions: accept by text, or ask for the full pass.
    Fast,
    /// With every definition loaded.
    Full,
}

/// Visits every query of a statement; checks its blocks after their
/// subqueries (as MySQL prepares them).
struct Analyze<'c> {
    c: &'c Check<'c>,
    mode: Mode,
    ctes: Vec<Vec<Cte>>,
    need_full: bool,
    /// Query nesting: 1 for the statement's outermost query.
    depth: usize,
}

impl<'c> Analyze<'c> {
    fn new(c: &'c Check<'c>, mode: Mode) -> Self {
        Self {
            c,
            mode,
            ctes: Vec::new(),
            need_full: false,
            depth: 0,
        }
    }
}

impl Visitor for Analyze<'_> {
    type Break = Refusal;

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<Refusal> {
        self.depth += 1;
        self.ctes.push(
            q.with
                .as_ref()
                .map(|w| w.cte_tables.clone())
                .unwrap_or_default(),
        );
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, q: &Query) -> ControlFlow<Refusal> {
        let r = match q.body.as_ref() {
            SetExpr::Select(sel) => self.block(sel, q.order_by.as_ref()),
            body => {
                let mut leaves = Vec::new();
                leaf_selects(body, &mut leaves);
                leaves.into_iter().try_for_each(|s| self.block(s, None))
            }
        };
        self.ctes.pop();
        self.depth -= 1;
        r
    }
}

fn leaf_selects<'q>(body: &'q SetExpr, out: &mut Vec<&'q Select>) {
    match body {
        SetExpr::Select(s) => out.push(s),
        SetExpr::SetOperation { left, right, .. } => {
            leaf_selects(left, out);
            leaf_selects(right, out);
        }
        _ => {}
    }
}

/// One item to check: an expression, or a column `*` expanded to.
enum Item<'s> {
    Expr(&'s Expr),
    Star(usize),
}

fn refuse_1055(k: usize, clause: &str, col: &str) -> Refusal {
    Refusal {
        code: 1055,
        sqlstate: "42000",
        message: format!(
            "Expression #{k} of {clause} is not in GROUP BY clause and contains nonaggregated \
             column '{col}' which is not functionally dependent on columns in GROUP BY clause; \
             this is incompatible with sql_mode=only_full_group_by"
        ),
    }
}

fn refuse_1140(k: usize, col: &str) -> Refusal {
    Refusal {
        code: 1140,
        sqlstate: "42000",
        message: format!(
            "In aggregated query without GROUP BY, expression #{k} of SELECT list contains \
             nonaggregated column '{col}'; this is incompatible with sql_mode=only_full_group_by"
        ),
    }
}

impl Analyze<'_> {
    fn block(&mut self, sel: &Select, order_by: Option<&OrderBy>) -> ControlFlow<Refusal> {
        let groups = group_exprs(&sel.group_by);
        let explicit = !groups.is_empty();
        let mut agg = AggScan::default();
        for item in &sel.projection {
            let _ = item.visit(&mut agg);
        }
        if let Some(h) = &sel.having {
            let _ = h.visit(&mut agg);
        }
        let aggregated = explicit || agg.aggregate;
        let orders = order_exprs(order_by);
        // 3029: an aggregate in ORDER BY of a query that doesn't aggregate.
        if !aggregated {
            for (k, o) in orders.iter().enumerate() {
                let mut a = AggScan::default();
                let _ = o.visit(&mut a);
                if a.aggregate {
                    return ControlFlow::Break(Refusal {
                        code: 3029,
                        sqlstate: "HY000",
                        message: format!(
                            "Expression #{} of ORDER BY contains aggregate function and applies \
                             to the result of a non-aggregated query",
                            k + 1
                        ),
                    });
                }
            }
        }
        let distinct = sel.distinct.is_some() && !orders.is_empty() && self.c.only_full_group_by;
        let grouping = aggregated && self.c.only_full_group_by;
        if !distinct && !grouping {
            return ControlFlow::Continue(());
        }
        match self.mode {
            Mode::Fast => {
                if !Self::fast_ok(sel, &orders, explicit, distinct, grouping) {
                    self.need_full = true;
                }
                ControlFlow::Continue(())
            }
            Mode::Full => self.full(sel, &orders, explicit, distinct, grouping),
        }
    }

    /// The first pass: every checked expression is grouped by its text.
    fn fast_ok(
        sel: &Select,
        orders: &[&Expr],
        explicit: bool,
        distinct: bool,
        grouping: bool,
    ) -> bool {
        let single = sel.from.len() == 1
            && sel.from[0].joins.is_empty()
            && matches!(sel.from[0].relation, TableFactor::Table { .. });
        let key = |parts: &[Ident]| -> String {
            if single {
                parts
                    .last()
                    .map(|p| p.value.to_ascii_lowercase())
                    .unwrap_or_default()
            } else {
                parts
                    .iter()
                    .map(|p| p.value.to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join(".")
            }
        };
        let mut items: Vec<(Option<String>, &Expr)> = Vec::new();
        for item in &sel.projection {
            match item {
                SelectItem::UnnamedExpr(e) => items.push((None, e)),
                SelectItem::ExprWithAlias { expr, alias } => {
                    items.push((Some(alias.value.to_ascii_lowercase()), expr));
                }
                _ => return false,
            }
        }
        let texts: Vec<String> = items
            .iter()
            .map(|(_, e)| e.to_string().to_ascii_lowercase())
            .collect();
        // Orders that name a select item (position, alias, same text).
        let names_item = |o: &Expr| -> bool {
            if let Expr::Value(v) = o
                && matches!(v.value, Value::Number(..))
            {
                return true;
            }
            if let Expr::Identifier(i) = o
                && items
                    .iter()
                    .any(|(a, _)| a.as_deref() == Some(i.value.to_ascii_lowercase().as_str()))
            {
                return true;
            }
            texts.contains(&o.to_string().to_ascii_lowercase())
        };
        let scan = |e: &Expr| -> Option<Vec<String>> {
            let mut r = RefScan::default();
            let _ = e.visit(&mut r);
            if r.subquery {
                return None;
            }
            let mut w = Windowed::default();
            let _ = e.visit(&mut w);
            if w.found {
                return None;
            }
            Some(r.refs.iter().map(|p| key(p)).collect())
        };
        if distinct && !orders.iter().all(|o| names_item(o)) {
            return false;
        }
        if !grouping {
            return true;
        }
        if !explicit {
            // Implicit grouping: no column outside aggregates.
            return items
                .iter()
                .all(|(_, e)| scan(e).is_some_and(|r| r.is_empty()));
        }
        let group_texts: HashSet<String> = group_exprs(&sel.group_by)
            .iter()
            .map(|g| g.to_string().to_ascii_lowercase())
            .collect();
        let mut group_cols = HashSet::new();
        for g in group_exprs(&sel.group_by) {
            match column_parts(g) {
                Some(p) => {
                    group_cols.insert(key(p));
                }
                // Positions and expressions: let the full pass resolve.
                None => {
                    if matches!(g, Expr::Value(_)) {
                        return false;
                    }
                }
            }
        }
        // A GROUP BY name that is also an alias: the full pass decides.
        if group_exprs(&sel.group_by).iter().any(|g| {
            matches!(g, Expr::Identifier(i) if items.iter().any(|(a, _)| a.as_deref() == Some(i.value.to_ascii_lowercase().as_str())))
        }) {
            return false;
        }
        let grouped = |e: &Expr| -> bool {
            group_texts.contains(&e.to_string().to_ascii_lowercase())
                || scan(e).is_some_and(|r| r.iter().all(|x| group_cols.contains(x)))
        };
        if !items.iter().all(|(_, e)| grouped(e)) {
            return false;
        }
        orders.iter().all(|o| names_item(o) || grouped(o)) && sel.named_window.is_empty()
    }

    #[allow(clippy::too_many_lines, clippy::many_single_char_names)] // MySQL's rule, clause by clause
    fn full(
        &mut self,
        sel: &Select,
        orders: &[&Expr],
        explicit: bool,
        distinct: bool,
        grouping: bool,
    ) -> ControlFlow<Refusal> {
        let b = Builder {
            c: self.c,
            ctes: &self.ctes,
        };
        let mut s = b.block(sel, 0);
        if s.uncertain {
            return ControlFlow::Continue(());
        }
        // In the outermost query a name no table has is an unknown column:
        // MySQL resolves names before this check, so its 1054 (which
        // MariaDB gives too) comes first.
        if self.depth == 1 && unknown_names(&b, &mut s, sel, orders) {
            return ControlFlow::Continue(());
        }
        // The select list, `*` expanded.
        let mut items: Vec<(Option<String>, Item<'_>)> = Vec::new();
        for it in &sel.projection {
            match it {
                SelectItem::UnnamedExpr(e) => items.push((None, Item::Expr(e))),
                SelectItem::ExprWithAlias { expr, alias } => {
                    items.push((Some(alias.value.to_ascii_lowercase()), Item::Expr(expr)));
                }
                SelectItem::Wildcard(_) => {
                    for e in 0..s.entries.len() {
                        if s.entries[e].hidden {
                            continue;
                        }
                        let cols: Vec<String> = s.entries[e]
                            .cols
                            .iter()
                            .filter(|c| c.visible)
                            .map(|c| c.lower.clone())
                            .collect();
                        for c in cols {
                            let a = s.atom(e, &c);
                            items.push((None, Item::Star(a)));
                        }
                    }
                    if !s.coalesced.is_empty() {
                        // MySQL lists USING columns first; numbering would
                        // differ.
                        return ControlFlow::Continue(());
                    }
                }
                SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(n),
                    _,
                ) => {
                    let parts: Vec<Ident> =
                        n.0.iter().filter_map(|p| p.as_ident().cloned()).collect();
                    let Some(q) = parts.last() else {
                        return ControlFlow::Continue(());
                    };
                    let hits: Vec<usize> = (0..s.entries.len())
                        .filter(|&e| {
                            !s.entries[e].hidden && s.entries[e].name.eq_ignore_ascii_case(&q.value)
                        })
                        .collect();
                    let [e] = hits.as_slice() else {
                        return ControlFlow::Continue(());
                    };
                    let cols: Vec<String> = s.entries[*e]
                        .cols
                        .iter()
                        .filter(|c| c.visible)
                        .map(|c| c.lower.clone())
                        .collect();
                    for c in cols {
                        let a = s.atom(*e, &c);
                        items.push((None, Item::Star(a)));
                    }
                }
                _ => return ControlFlow::Continue(()),
            }
        }
        // Canonical text: column references replaced by their atoms.
        let canon = |s: &mut Scope, e: &Expr| -> Option<String> {
            let mut out = e.clone();
            let mut bad = false;
            let _ = sqlparser::ast::visit_expressions_mut(&mut out, |x| {
                if let Some(p) = column_parts(x).map(<[Ident]>::to_vec) {
                    match s.resolve(&p) {
                        Res::Atom(a) => *x = Expr::Identifier(Ident::new(format!("@{a}"))),
                        Res::NotHere => {}
                        Res::Unsure => bad = true,
                    }
                }
                ControlFlow::<()>::Continue(())
            });
            (!bad).then(|| out.to_string().to_ascii_lowercase())
        };
        let item_canon: Vec<Option<String>> = items
            .iter()
            .map(|(_, it)| match it {
                Item::Expr(e) => canon(&mut s, e),
                Item::Star(a) => Some(format!("@{a}")),
            })
            .collect();

        // 3065: DISTINCT ordered by a column not in the select list.
        if distinct {
            let selected: HashSet<usize> = items
                .iter()
                .filter_map(|(_, it)| match it {
                    Item::Star(a) => Some(*a),
                    Item::Expr(e) => column_parts(e).and_then(|p| match s.resolve(p) {
                        Res::Atom(a) => Some(a),
                        _ => None,
                    }),
                })
                .collect();
            for (k, o) in orders.iter().enumerate() {
                if Self::names_item(o, &items, &item_canon, &mut s, &canon) {
                    continue;
                }
                let mut scan = Scan::new(&b);
                let _ = o.visit(&mut scan);
                if scan.unsure {
                    return ControlFlow::Continue(());
                }
                for r in &scan.refs {
                    match s.resolve(&r.parts) {
                        Res::Atom(a) if !selected.contains(&a) => {
                            return ControlFlow::Break(Refusal {
                                code: 3065,
                                sqlstate: "HY000",
                                message: format!(
                                    "Expression #{} of ORDER BY clause is not in SELECT list, \
                                     references column '{}' which is not in SELECT list; this is \
                                     incompatible with DISTINCT",
                                    k + 1,
                                    s.display(a)
                                ),
                            });
                        }
                        Res::Unsure => return ControlFlow::Continue(()),
                        _ => {}
                    }
                }
            }
        }
        if !grouping {
            return ControlFlow::Continue(());
        }

        // The grouped set.
        let with_rollup = rollup(&sel.group_by);
        let mut start = HashSet::new();
        let mut group_texts = HashSet::new();
        for g in group_exprs(&sel.group_by) {
            let target: Option<&Expr> = match g {
                Expr::Value(v) => match &v.value {
                    Value::Number(n, _) => match n
                        .parse::<usize>()
                        .ok()
                        .and_then(|n| n.checked_sub(1))
                        .and_then(|i| items.get(i))
                    {
                        Some((_, Item::Expr(e))) => Some(*e),
                        Some((_, Item::Star(a))) => {
                            start.insert(*a);
                            group_texts.insert(format!("@{a}"));
                            continue;
                        }
                        None => return ControlFlow::Continue(()),
                    },
                    _ => Some(g),
                },
                Expr::Identifier(i) => match s.resolve(std::slice::from_ref(i)) {
                    Res::Atom(_) => Some(g),
                    Res::Unsure => return ControlFlow::Continue(()),
                    Res::NotHere => {
                        let lower = i.value.to_ascii_lowercase();
                        match items
                            .iter()
                            .find(|(a, _)| a.as_deref() == Some(lower.as_str()))
                        {
                            Some((_, Item::Expr(e))) => Some(*e),
                            Some((_, Item::Star(a))) => {
                                start.insert(*a);
                                continue;
                            }
                            // An unknown column: MySQL's 1054 comes first,
                            // and MariaDB gives it.
                            None => return ControlFlow::Continue(()),
                        }
                    }
                },
                _ => Some(g),
            };
            let Some(t) = target else { continue };
            if let Some(p) = column_parts(t) {
                match s.resolve(p) {
                    Res::Atom(a) => {
                        start.insert(a);
                    }
                    Res::Unsure | Res::NotHere => return ControlFlow::Continue(()),
                }
            }
            match canon(&mut s, t) {
                Some(c) => {
                    group_texts.insert(c);
                }
                None => return ControlFlow::Continue(()),
            }
        }
        let closed = if with_rollup {
            start
        } else {
            s.closure(&start)
        };

        // Checks one expression; `Err(None)` when unsure.
        let check_expr = |s: &mut Scope, e: &Expr, win: bool| -> Result<(), Option<usize>> {
            if canon(s, e).is_some_and(|c| group_texts.contains(&c)) {
                return Ok(());
            }
            let mut scan = Scan::new(&b);
            if win {
                // A window's own expressions are checked as given.
            }
            let _ = e.visit(&mut scan);
            if scan.unsure {
                return Err(None);
            }
            for r in &scan.refs {
                match s.resolve(&r.parts) {
                    Res::Atom(a) if !closed.contains(&a) => return Err(Some(a)),
                    Res::Unsure => return Err(None),
                    _ => {}
                }
            }
            Ok(())
        };

        // The select list.
        let mut windows: Vec<Vec<Expr>> = Vec::new();
        for (k, (_, it)) in items.iter().enumerate() {
            let r = match it {
                Item::Star(a) => {
                    if closed.contains(a) {
                        Ok(())
                    } else {
                        Err(Some(*a))
                    }
                }
                Item::Expr(e) => {
                    let mut w = Scan::new(&b);
                    let _ = e.visit(&mut w);
                    windows.extend(w.windows);
                    check_expr(&mut s, e, false)
                }
            };
            match r {
                Ok(()) => {}
                Err(None) => return ControlFlow::Continue(()),
                Err(Some(a)) => {
                    let col = s.display(a);
                    return ControlFlow::Break(if explicit {
                        refuse_1055(k + 1, "SELECT list", &col)
                    } else {
                        refuse_1140(k + 1, &col)
                    });
                }
            }
        }
        if !explicit {
            return ControlFlow::Continue(());
        }
        // Windows: inline ones, then named ones.
        let mut named: Vec<(String, Vec<Expr>)> = windows
            .into_iter()
            .map(|w| ("<unnamed window>".to_owned(), w))
            .collect();
        for def in &sel.named_window {
            if let NamedWindowExpr::WindowSpec(spec) = &def.1 {
                named.push((
                    def.0.value.clone(),
                    spec.partition_by
                        .iter()
                        .cloned()
                        .chain(spec.order_by.iter().map(|o| o.expr.clone()))
                        .collect(),
                ));
            }
        }
        for (name, exprs) in &named {
            for (k, e) in exprs.iter().enumerate() {
                match check_expr(&mut s, e, true) {
                    Ok(()) => {}
                    Err(None) => return ControlFlow::Continue(()),
                    Err(Some(a)) => {
                        let col = s.display(a);
                        return ControlFlow::Break(refuse_1055(
                            k + 1,
                            &format!("PARTITION BY or ORDER BY clause of window '{name}'"),
                            &col,
                        ));
                    }
                }
            }
        }
        // ORDER BY.
        for (k, o) in orders.iter().enumerate() {
            if Self::names_item(o, &items, &item_canon, &mut s, &canon) {
                continue;
            }
            match check_expr(&mut s, o, false) {
                Ok(()) => {}
                Err(None) => return ControlFlow::Continue(()),
                Err(Some(a)) => {
                    let col = s.display(a);
                    return ControlFlow::Break(refuse_1055(k + 1, "ORDER BY clause", &col));
                }
            }
        }
        ControlFlow::Continue(())
    }

    /// Whether an `ORDER BY` expression stands for a select item: its
    /// position, its alias, or the same expression.
    fn names_item(
        o: &Expr,
        items: &[(Option<String>, Item<'_>)],
        item_canon: &[Option<String>],
        s: &mut Scope,
        canon: &dyn Fn(&mut Scope, &Expr) -> Option<String>,
    ) -> bool {
        if let Expr::Value(v) = o
            && matches!(v.value, Value::Number(..))
        {
            return true;
        }
        if let Expr::Identifier(i) = o {
            let lower = i.value.to_ascii_lowercase();
            if items
                .iter()
                .any(|(a, _)| a.as_deref() == Some(lower.as_str()))
            {
                return true;
            }
        }
        canon(s, o).is_some_and(|c| item_canon.iter().any(|x| x.as_deref() == Some(c.as_str())))
    }
}

/// Whether the outermost block names a column none of its tables has
/// (select list, `WHERE`, `ORDER BY`; select aliases allowed in `ORDER BY`).
fn unknown_names(b: &Builder<'_, '_>, s: &mut Scope, sel: &Select, orders: &[&Expr]) -> bool {
    let mut exprs: Vec<(&Expr, bool)> = Vec::new();
    for it in &sel.projection {
        match it {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                exprs.push((e, false));
            }
            _ => {}
        }
    }
    if let Some(w) = &sel.selection {
        exprs.push((w, false));
    }
    exprs.extend(orders.iter().map(|o| (*o, true)));
    for (e, aliases_ok) in exprs {
        let mut scan = Scan::new(b);
        let _ = e.visit(&mut scan);
        for r in &scan.refs {
            if aliases_ok
                && r.parts.len() == 1
                && s.aliases.contains(&r.parts[0].value.to_ascii_lowercase())
            {
                continue;
            }
            if s.resolve(&r.parts) == Res::NotHere {
                return true;
            }
        }
    }
    false
}

/// Whether an expression holds a window function.
#[derive(Default)]
struct Windowed {
    found: bool,
}

impl Visitor for Windowed {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if let Expr::Function(f) = e
            && f.over.is_some()
        {
            self.found = true;
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests;
