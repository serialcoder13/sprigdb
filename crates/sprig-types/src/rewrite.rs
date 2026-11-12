//! Rewriting references to typed and hierarchical tables (chapter 32,
//! "Resolving a table reference", "Polymorphic SELECT", "Hierarchy
//! queries"; build steps 4 and 10).
//!
//! A statement is parsed with sqlparser-rs and walked twice with the same
//! scope rules. The first pass ([`Pass::Collect`]) finds each typed table
//! reference and the fields its query uses, and each `SPRIG_*` call; the
//! second ([`Pass::Rewrite`]) replaces each reference that needs it by the
//! physical tables that hold those fields, qualifies the columns with the
//! new aliases, expands `*`, adds the type filter, and replaces the
//! `SPRIG_*` calls. Between the passes the caller decides each
//! reference's tables ([`Walker::plan_refs`], or by hand for writes) and
//! reads the nodes the `SPRIG_*` calls name.
//!
//! Scopes: a `SELECT`'s typed references are its own `FROM` (and joins).
//! An unqualified column belongs to the innermost scope's typed reference
//! that has the field; if that scope has only typed tables and none has
//! it, the next scope out is tried (a correlated reference). `t.c` and
//! `db.t.c` look up `t` from the innermost scope out. `ORDER BY`, `GROUP
//! BY` and `HAVING` names that are select-list aliases are left alone.
//! Names of CTEs shadow tables.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use sprig_common::names::quote;
use sprig_common::{SprigCode, SprigError};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, JoinConstraint,
    JoinOperator, ObjectName, ObjectNamePart, OrderBy, OrderByExpr, OrderByKind, Query, Select,
    SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins, Value, VisitMut, VisitorMut,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserOptions};

use crate::grammar::ONLY_PREFIX;
use crate::model::{DbTypes, HierarchyDef, KeyClass, Storage, TypeDef, TypeId, upper};

/// The root columns every typed row has and users may name.
pub const TYPE_COLUMN: &str = "_type";
/// See [`TYPE_COLUMN`].
pub const TYPE_PATH_COLUMN: &str = "_type_path";

/// Parses one statement as the rewriter does: MySQL dialect, string
/// escapes kept as written.
///
/// # Errors
/// 39710 with the parser's message.
pub fn parse_one(sql: &str) -> Result<Statement, SprigError> {
    let mut stmts = Parser::new(&MySqlDialect {})
        .with_options(ParserOptions::new().with_unescape(false))
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_statements())
        .map_err(|e| {
            SprigError::new(
                SprigCode::TypeUnsupported,
                format!("a statement on a typed or hierarchical table must parse: {e}"),
            )
        })?;
    if stmts.len() != 1 {
        return Err(SprigError::new(
            SprigCode::TypeUnsupported,
            "one statement at a time on typed or hierarchical tables",
        ));
    }
    Ok(stmts.remove(0))
}

/// Parses an expression the rewriter generated.
pub(crate) fn expr(sql: &str) -> Result<Expr, SprigError> {
    Parser::new(&MySqlDialect {})
        .with_options(ParserOptions::new().with_unescape(false))
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| SprigError::internal(format!("generated expression {sql:?}: {e}")))
}

fn factor(sql: &str) -> Result<TableFactor, SprigError> {
    let Statement::Query(mut q) = parse_one(&format!("SELECT 1 FROM {sql}"))? else {
        return Err(SprigError::internal("generated join"));
    };
    let SetExpr::Select(s) = &mut *q.body else {
        return Err(SprigError::internal("generated join"));
    };
    Ok(s.from.remove(0).relation)
}

/// A string literal: `'` doubled, `\` doubled unless `NO_BACKSLASH_ESCAPES`.
#[must_use]
pub fn quote_str(s: &str, no_backslash_escapes: bool) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' if !no_backslash_escapes => out.push_str("\\\\"),
            _ => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// A binary literal `X'..'`.
#[must_use]
pub fn hex_lit(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2 + 3);
    s.push_str("X'");
    for x in b {
        s.push_str(&format!("{x:02X}"));
    }
    s.push('\'');
    s
}

fn ident_eq(a: &str, b: &str, lctn: bool) -> bool {
    if lctn {
        a.eq_ignore_ascii_case(b)
    } else {
        a == b
    }
}

fn last(n: &ObjectName) -> Option<String> {
    match n.0.last()? {
        ObjectNamePart::Identifier(i) => Some(i.value.clone()),
        ObjectNamePart::Function(_) => None,
    }
}

fn parts(n: &ObjectName) -> Vec<String> {
    n.0.iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.clone()),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

fn qident(s: &str) -> Ident {
    Ident::with_quote('`', s)
}

/// Finds a table's types: `(database as written or None, table)` to the
/// database's types and the type, and to the database's types for
/// hierarchies.
pub trait Lookup: Send + Sync {
    /// The types of database `db` (`None`: the current database).
    fn db(&self, db: Option<&str>) -> Option<Arc<DbTypes>>;
    /// `lower_case_table_names`.
    fn lctn(&self) -> bool;
    /// Every database with types, by logical name (for
    /// `information_schema`).
    fn databases(&self) -> Vec<(String, Arc<DbTypes>)> {
        Vec::new()
    }
}

/// One typed table reference.
#[derive(Debug, Clone)]
pub struct TypedRef {
    /// Its database's types.
    pub db: Arc<DbTypes>,
    /// The database as written.
    pub db_name: Option<String>,
    /// The type.
    pub t: Arc<TypeDef>,
    /// The name the query knows it by: the alias, or the table name.
    pub qualifier: String,
    /// Read with `ONLY`.
    pub only: bool,
    /// Lower-cased field names the query uses.
    pub used: BTreeSet<String>,
    /// The query selects `*` or `q.*`.
    pub wildcard: bool,
    /// The physical tables, when the reference is rewritten.
    pub plan: Option<RefPlan>,
}

impl TypedRef {
    /// The field `name` of the reference: its owner, or `None`. `_type`
    /// and `_type_path` belong to the root.
    #[must_use]
    pub fn owner_of(&self, name: &str) -> Option<TypeId> {
        if name.eq_ignore_ascii_case(TYPE_COLUMN) || name.eq_ignore_ascii_case(TYPE_PATH_COLUMN) {
            return Some(self.t.root);
        }
        self.db.field(self.t.id, name).map(|f| f.owner)
    }

    /// Whether the reference's type has subtypes.
    #[must_use]
    pub fn has_subtypes(&self) -> bool {
        self.db.has_subtypes(self.t.id)
    }
}

/// The physical tables of a rewritten reference.
#[derive(Debug, Clone)]
pub struct RefPlan {
    /// Root first: the type of the table and its alias.
    pub tables: Vec<(Arc<TypeDef>, String)>,
    /// The type filter (`_type = 'T'` or the path range), on the root.
    pub filter: Option<String>,
    /// The extension tables are `LEFT JOIN`ed (a discriminated joined
    /// subtype: its rows are the root's rows its value marks, whether or not
    /// an extension row exists, as when a parent tenant wrote a value only
    /// a child tenant declares).
    pub left: bool,
}

impl RefPlan {
    /// The alias holding field `owner`'s table, or the first table for a
    /// key field.
    #[must_use]
    pub fn alias_for(&self, owner: Option<TypeId>, is_key: bool) -> &str {
        if !is_key
            && let Some(o) = owner
            && let Some((_, a)) = self.tables.iter().find(|(t, _)| t.id == o)
        {
            return a;
        }
        &self.tables[0].1
    }

    /// The alias of the first table (the key's).
    #[must_use]
    pub fn key_alias(&self) -> &str {
        &self.tables[0].1
    }

    /// The join text: `` `db`.`asset` AS `v__r` JOIN `db`.`vehicle` AS
    /// `v__x1` ON ... `` (parenthesized when several tables).
    #[must_use]
    pub fn join_sql(&self, db: Option<&str>, key: &[String]) -> String {
        let table = |t: &TypeDef| match db {
            Some(d) => format!("{}.{}", quote(d), quote(&t.name)),
            None => quote(&t.name),
        };
        let mut s = String::new();
        let (first, first_alias) = &self.tables[0];
        s.push_str(&format!("{} AS {}", table(first), quote(first_alias)));
        for (t, a) in &self.tables[1..] {
            let on: Vec<String> = key
                .iter()
                .map(|k| {
                    format!(
                        "{}.{} = {}.{}",
                        quote(a),
                        quote(k),
                        quote(first_alias),
                        quote(k)
                    )
                })
                .collect();
            s.push_str(&format!(
                " {}JOIN {} AS {} ON {}",
                if self.left { "LEFT " } else { "" },
                table(t),
                quote(a),
                on.join(" AND ")
            ));
        }
        if self.tables.len() > 1 {
            format!("({s})")
        } else {
            s
        }
    }
}

/// Builds a reference's tables: `owners` (types whose own fields the
/// statement uses) plus `extra`, ordered root first, with the filter the
/// mode needs (chapter 32, "Polymorphic SELECT", step 3).
#[must_use]
pub fn build_plan(r: &TypedRef, owners: &BTreeSet<TypeId>, extra: &[TypeId]) -> RefPlan {
    let chain = r.db.chain(r.t.id);
    let root = &chain[0];
    let only = r.only && r.has_subtypes();
    let mut need: BTreeSet<TypeId> = owners.clone();
    need.extend(extra.iter().copied());
    let mut filter = None;
    // ONLY needs the root's `_type`; reading only root columns of a
    // subtype needs the root's path range; otherwise the subtype's own
    // table holds exactly its rows.
    // Single-table storage: the root's table holds every row and column.
    if root.storage == Storage::SingleTable {
        need.clear();
    }
    let root_only = need.iter().all(|o| *o == root.id);
    // A discriminated joined subtype is the root's rows its value marks:
    // read from the root with the path range, its tables LEFT JOINed.
    let open = root.discriminator.is_some() && root.storage == Storage::Joined && !r.t.is_root();
    if only || root_only || r.t.is_root() || open {
        need.insert(root.id);
    } else {
        need.insert(r.t.id);
    }
    let tables_t: Vec<Arc<TypeDef>> = chain
        .iter()
        .filter(|t| need.contains(&t.id))
        .cloned()
        .collect();
    let single = tables_t.len() == 1;
    let tables: Vec<(Arc<TypeDef>, String)> = tables_t
        .into_iter()
        .map(|t| {
            let alias = if single {
                r.qualifier.clone()
            } else if t.is_root() {
                format!("{}__r", r.qualifier)
            } else {
                format!("{}__x{}", r.qualifier, t.depth)
            };
            (t, alias)
        })
        .collect();
    let root_alias = tables
        .iter()
        .find(|(t, _)| t.is_root())
        .map(|(_, a)| a.clone());
    if let Some(ra) = root_alias {
        if only {
            filter = Some(format!(
                "{}.{} = {}",
                quote(&ra),
                quote(TYPE_COLUMN),
                quote_str(&r.t.name, false)
            ));
        } else if !r.t.is_root() && (open || !tables.iter().any(|(t, _)| t.id == r.t.id)) {
            let lo = &r.t.path;
            let hi = String::from_utf8_lossy(&upper(lo.as_bytes())).into_owned();
            filter = Some(format!(
                "({a}.{c} >= {lo} AND {a}.{c} < {hi})",
                a = quote(&ra),
                c = quote(TYPE_PATH_COLUMN),
                lo = quote_str(lo, false),
                hi = quote_str(&hi, false)
            ));
        }
    }
    let left = open && tables.len() > 1;
    RefPlan {
        tables,
        filter,
        left,
    }
}

/// Which `SPRIG_*` function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HierFn {
    /// `SPRIG_SUBTREE(t, k)`: `k` and every row under it.
    Subtree,
    /// `SPRIG_DESCENDANTS(t, k)`: every row under `k`.
    Descendants,
    /// `SPRIG_ANCESTORS(t, k)`: every row above `k`.
    Ancestors,
    /// `SPRIG_DEPTH(t)`.
    Depth,
    /// `SPRIG_PATH(t)`.
    Path,
}

impl HierFn {
    fn of(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "SPRIG_SUBTREE" => Some(HierFn::Subtree),
            "SPRIG_DESCENDANTS" => Some(HierFn::Descendants),
            "SPRIG_ANCESTORS" => Some(HierFn::Ancestors),
            "SPRIG_DEPTH" => Some(HierFn::Depth),
            "SPRIG_PATH" => Some(HierFn::Path),
            _ => None,
        }
    }
}

/// One `SPRIG_*` call.
#[derive(Debug, Clone)]
pub struct HierCall {
    /// The hierarchy.
    pub h: Arc<HierarchyDef>,
    /// The database as written.
    pub db_name: Option<String>,
    /// The table reference's qualifier.
    pub qualifier: String,
    /// The function.
    pub func: HierFn,
    /// The node's key: a literal's SQL text.
    pub key_literal: Option<String>,
    /// The node's key: any other expression's SQL text.
    pub key_expr: Option<String>,
}

/// What [`HierCall`]'s node read found: its path and top key, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRead {
    /// `_sprig_hpath`.
    pub path: Vec<u8>,
    /// `_sprig_rk` as text.
    pub rk: Vec<u8>,
}

/// The two passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pass {
    /// Find references, used fields and calls.
    Collect,
    /// Change the statement.
    Rewrite,
}

struct Frame {
    /// Typed references of this scope (indexes into `refs`).
    refs: Vec<usize>,
    /// Every table reference: qualifier, database, table.
    tables: Vec<(String, Option<String>, String)>,
    /// Select-list aliases, lower-cased.
    aliases: HashSet<String>,
    /// Every table of the scope is typed.
    all_typed: bool,
}

/// Result-metadata renames for a rewritten `SELECT`: a column whose table
/// alias is `alias` shows `table` and `org_table` instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaRename {
    /// The alias in the rewritten statement.
    pub alias: String,
    /// The table name the client wrote (or its alias).
    pub table: String,
    /// The type's table.
    pub org_table: String,
}

/// The two-pass walker (see the module docs).
pub struct Walker<'l> {
    lookup: &'l dyn Lookup,
    /// Every typed reference, in walk order.
    pub refs: Vec<TypedRef>,
    /// Every `SPRIG_*` call, in walk order.
    pub calls: Vec<HierCall>,
    /// Node reads, by call index (for [`Pass::Rewrite`]).
    pub reads: HashMap<usize, Option<NodeRead>>,
    /// Read every typed reference with `ONLY`.
    pub only_all: bool,
    /// A dump tool's read: a single-table hierarchy with a discriminator
    /// dumps every row through its root (the column restores each row's
    /// type) and none through its subtypes.
    pub dump: bool,
    /// `NO_BACKSLASH_ESCAPES`.
    pub no_backslash_escapes: bool,
    pass: Pass,
    next_ref: usize,
    next_call: usize,
    stack: Vec<Frame>,
    ctes: Vec<HashSet<String>>,
    error: Option<SprigError>,
}

impl<'l> Walker<'l> {
    /// A walker over `lookup`'s types.
    #[must_use]
    pub fn new(lookup: &'l dyn Lookup) -> Self {
        Self {
            lookup,
            refs: Vec::new(),
            calls: Vec::new(),
            reads: HashMap::new(),
            only_all: false,
            dump: false,
            no_backslash_escapes: false,
            pass: Pass::Collect,
            next_ref: 0,
            next_call: 0,
            stack: Vec::new(),
            ctes: Vec::new(),
            error: None,
        }
    }

    fn fail(&mut self, e: SprigError) {
        if self.error.is_none() {
            self.error = Some(e);
        }
    }

    fn start(&mut self, pass: Pass) {
        self.pass = pass;
        self.next_ref = 0;
        self.next_call = 0;
        self.stack.clear();
        self.ctes.clear();
    }

    fn finish(&mut self) -> Result<(), SprigError> {
        match self.error.take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Runs `pass` over a query.
    ///
    /// # Errors
    /// What the walk found: 39710 for forms it can't rewrite.
    pub fn query(&mut self, pass: Pass, q: &mut Query) -> Result<(), SprigError> {
        self.start(pass);
        self.walk_query(q);
        self.finish()
    }

    /// Runs `pass` over expressions in the scope of one table reference
    /// (an `UPDATE` or `DELETE` target), which must be `refs[0]` after
    /// [`Walker::register_target`].
    ///
    /// # Errors
    /// As [`Walker::query`].
    pub fn target_exprs(
        &mut self,
        pass: Pass,
        target: usize,
        exprs: &mut [&mut Expr],
        order_by: &mut [OrderByExpr],
    ) -> Result<(), SprigError> {
        self.start(pass);
        self.next_ref = self.refs.len();
        let r = &self.refs[target];
        self.stack.push(Frame {
            refs: vec![target],
            tables: vec![(r.qualifier.clone(), r.db_name.clone(), r.t.name.clone())],
            aliases: HashSet::new(),
            all_typed: true,
        });
        for e in exprs.iter_mut() {
            self.walk_expr(e, false);
        }
        for o in order_by {
            self.walk_expr(&mut o.expr, false);
        }
        self.stack.pop();
        self.finish()
    }

    /// Registers a write's target table as a reference; `None` if it is
    /// not typed.
    pub fn register_target(
        &mut self,
        name: &ObjectName,
        alias: Option<&TableAlias>,
    ) -> Option<usize> {
        let r = self.resolve(name, alias)?;
        self.refs.push(r);
        Some(self.refs.len() - 1)
    }

    /// Decides each SELECT reference's tables from what it uses.
    pub fn plan_refs(&mut self) {
        let dump = self.dump;
        for r in &mut self.refs {
            // A dump of a single-table hierarchy typed by its discriminator:
            // the root's table whole, the subtypes empty. Restored, the
            // column types every row again, whatever order the subtypes
            // come back in.
            if dump && r.t.storage == Storage::SingleTable && r.db.discriminator(r.t.id).is_some() {
                if r.t.is_root() {
                    r.only = false;
                    continue;
                }
                let mut p = build_plan(r, &BTreeSet::new(), &[]);
                p.filter = Some("1 = 0".to_owned());
                r.plan = Some(p);
                continue;
            }
            let only = r.only && r.has_subtypes();
            if r.t.is_root() && !only {
                continue;
            }
            let mut owners: BTreeSet<TypeId> = BTreeSet::new();
            if r.wildcard {
                for f in r.db.all_fields(r.t.id) {
                    if !f.is_key {
                        owners.insert(f.owner);
                    }
                }
            }
            for u in &r.used {
                if r.t.is_key(u)
                    || r.db
                        .key_defs(r.t.id)
                        .iter()
                        .any(|k| k.name.eq_ignore_ascii_case(u))
                {
                    continue;
                }
                if let Some(o) = r.owner_of(u) {
                    owners.insert(o);
                }
            }
            r.plan = Some(build_plan(r, &owners, &[]));
        }
    }

    /// The metadata renames of the planned references.
    #[must_use]
    pub fn meta(&self) -> Vec<MetaRename> {
        let mut out = Vec::new();
        for r in &self.refs {
            let Some(p) = &r.plan else { continue };
            for (_, alias) in &p.tables {
                out.push(MetaRename {
                    alias: alias.clone(),
                    table: r.qualifier.clone(),
                    org_table: r.t.name.clone(),
                });
            }
        }
        out
    }

    fn resolve(&self, name: &ObjectName, alias: Option<&TableAlias>) -> Option<TypedRef> {
        let p = parts(name);
        let (db_name, mut table) = match p.as_slice() {
            [t] => (None, t.clone()),
            [d, t] => (Some(d.clone()), t.clone()),
            _ => return None,
        };
        if db_name.is_none() && self.ctes.iter().any(|s| s.contains(&table.to_lowercase())) {
            return None;
        }
        let mut only = self.only_all;
        if let Some(rest) = table.strip_prefix(ONLY_PREFIX) {
            table = rest.to_owned();
            only = true;
        }
        let db = self.lookup.db(db_name.as_deref())?;
        let t = Arc::clone(db.get(&table, self.lookup.lctn())?);
        let qualifier = alias.map_or_else(|| t.name.clone(), |a| a.name.value.clone());
        Some(TypedRef {
            db,
            db_name,
            t,
            qualifier,
            only,
            used: BTreeSet::new(),
            wildcard: false,
            plan: None,
        })
    }

    fn walk_query(&mut self, q: &mut Query) {
        let mut names = HashSet::new();
        if let Some(with) = &q.with {
            for c in &with.cte_tables {
                names.insert(c.alias.name.value.to_lowercase());
            }
        }
        let recursive = q.with.as_ref().is_some_and(|w| w.recursive);
        if recursive {
            self.ctes.push(names.clone());
        }
        if let Some(with) = &mut q.with {
            for c in &mut with.cte_tables {
                self.walk_query(&mut c.query);
            }
        }
        if !recursive {
            self.ctes.push(names);
        }
        let Query { body, order_by, .. } = q;
        self.walk_set_expr(body, order_by.as_mut());
        self.ctes.pop();
    }

    fn walk_set_expr(&mut self, se: &mut SetExpr, order_by: Option<&mut OrderBy>) {
        match se {
            SetExpr::Select(s) => self.walk_select(s, order_by),
            SetExpr::Query(q) => self.walk_query(q),
            SetExpr::SetOperation { left, right, .. } => {
                self.walk_set_expr(left, None);
                self.walk_set_expr(right, None);
            }
            SetExpr::Values(v) => {
                for row in &mut v.rows {
                    for e in &mut row.content {
                        self.walk_expr(e, false);
                    }
                }
            }
            _ => {}
        }
    }

    fn walk_factor_subqueries(&mut self, f: &mut TableFactor) {
        match f {
            TableFactor::Derived { subquery, .. } => self.walk_query(subquery),
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.walk_factor_subqueries(&mut table_with_joins.relation);
                for j in &mut table_with_joins.joins {
                    self.walk_factor_subqueries(&mut j.relation);
                }
            }
            _ => {}
        }
    }

    fn register_factor(&mut self, f: &TableFactor, frame: &mut Frame) {
        match f {
            TableFactor::Table { name, alias, .. } => {
                let p = parts(name);
                let table = last(name).unwrap_or_default();
                let table = table.strip_prefix(ONLY_PREFIX).unwrap_or(&table).to_owned();
                let qualifier = alias
                    .as_ref()
                    .map_or_else(|| table.clone(), |a| a.name.value.clone());
                let db = (p.len() == 2).then(|| p[0].clone());
                frame.tables.push((qualifier, db, table));
                let id = self.next_ref;
                if self.pass == Pass::Collect {
                    match self.resolve(name, alias.as_ref()) {
                        Some(r) => {
                            self.refs.push(r);
                            self.next_ref += 1;
                            frame.refs.push(id);
                        }
                        None => frame.all_typed = false,
                    }
                } else if self.is_ref_at(id, name, alias.as_ref()) {
                    self.next_ref += 1;
                    frame.refs.push(id);
                } else {
                    frame.all_typed = false;
                }
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.register_factor(&table_with_joins.relation, frame);
                for j in &table_with_joins.joins {
                    self.register_factor(&j.relation, frame);
                }
            }
            TableFactor::Derived { alias, .. } => {
                if let Some(a) = alias {
                    frame
                        .tables
                        .push((a.name.value.clone(), None, String::new()));
                }
                frame.all_typed = false;
            }
            _ => frame.all_typed = false,
        }
    }

    /// In the rewrite pass: whether the factor is reference `id` (the
    /// collect pass registered it at the same position).
    fn is_ref_at(&self, id: usize, name: &ObjectName, alias: Option<&TableAlias>) -> bool {
        let Some(r) = self.refs.get(id) else {
            return false;
        };
        let table = last(name).unwrap_or_default();
        let table = table.strip_prefix(ONLY_PREFIX).unwrap_or(&table);
        let q = alias.map_or(table, |a| a.name.value.as_str());
        q == r.qualifier && self.resolve(name, alias).is_some()
    }

    fn walk_select(&mut self, s: &mut Select, order_by: Option<&mut OrderBy>) {
        for twj in &mut s.from {
            self.walk_factor_subqueries(&mut twj.relation);
            for j in &mut twj.joins {
                self.walk_factor_subqueries(&mut j.relation);
            }
        }
        let mut frame = Frame {
            refs: Vec::new(),
            tables: Vec::new(),
            aliases: HashSet::new(),
            all_typed: true,
        };
        for twj in &s.from {
            self.register_factor(&twj.relation, &mut frame);
            for j in &twj.joins {
                self.register_factor(&j.relation, &mut frame);
            }
        }
        for item in &s.projection {
            if let SelectItem::ExprWithAlias { alias, .. } = item {
                frame.aliases.insert(alias.value.to_lowercase());
            }
        }
        let frame_refs = frame.refs.clone();
        self.stack.push(frame);
        // Wildcards in the collect pass.
        if self.pass == Pass::Collect {
            for item in &s.projection {
                match item {
                    SelectItem::Wildcard(_) => {
                        for &r in &frame_refs {
                            self.refs[r].wildcard = true;
                        }
                    }
                    SelectItem::QualifiedWildcard(
                        SelectItemQualifiedWildcardKind::ObjectName(n),
                        _,
                    ) => {
                        if let Some(r) = self.find_qualified(&parts(n)) {
                            self.refs[r].wildcard = true;
                        }
                    }
                    _ => {}
                }
            }
        }
        for item in &mut s.projection {
            match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    self.walk_expr(e, false);
                }
                _ => {}
            }
        }
        for twj in &mut s.from {
            for j in &mut twj.joins {
                if let Some(JoinConstraint::On(e)) = constraint_mut(&mut j.join_operator) {
                    self.walk_expr(e, false);
                }
            }
        }
        if let Some(e) = &mut s.selection {
            self.walk_expr(e, false);
        }
        if let GroupByExpr::Expressions(exprs, _) = &mut s.group_by {
            for e in exprs {
                self.walk_expr(e, true);
            }
        }
        if let Some(e) = &mut s.having {
            self.walk_expr(e, true);
        }
        if let Some(ob) = order_by
            && let OrderByKind::Expressions(exprs) = &mut ob.kind
        {
            for o in exprs {
                self.walk_expr(&mut o.expr, true);
            }
        }
        let frame = self.stack.pop();
        if self.pass == Pass::Rewrite
            && let Some(frame) = frame
        {
            self.rewrite_select(s, &frame);
        }
    }

    /// Replaces the planned references' factors, expands wildcards and
    /// places filters.
    fn rewrite_select(&mut self, s: &mut Select, frame: &Frame) {
        let planned: Vec<usize> = frame
            .refs
            .iter()
            .copied()
            .filter(|&r| self.refs[r].plan.is_some())
            .collect();
        if planned.is_empty() {
            return;
        }
        // Wildcards.
        let mut projection = Vec::with_capacity(s.projection.len());
        for item in std::mem::take(&mut s.projection) {
            match item {
                SelectItem::Wildcard(opts) => {
                    let mut ri = frame.refs.iter().copied().peekable();
                    for (q, db, table) in &frame.tables {
                        let r = ri.peek().copied().filter(|&r| {
                            &self.refs[r].qualifier == q
                                && self.refs[r].t.name.to_lowercase() == table.to_lowercase()
                        });
                        if let Some(r) = r {
                            ri.next();
                            if self.refs[r].plan.is_some() {
                                projection.extend(self.expansion(r));
                                continue;
                            }
                        }
                        let _ = db;
                        projection.push(SelectItem::QualifiedWildcard(
                            SelectItemQualifiedWildcardKind::ObjectName(ObjectName::from(vec![
                                qident(q),
                            ])),
                            opts.clone(),
                        ));
                    }
                }
                SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(n),
                    opts,
                ) => {
                    match self
                        .find_qualified(&parts(&n))
                        .filter(|r| self.refs[*r].plan.is_some())
                    {
                        Some(r) => projection.extend(self.expansion(r)),
                        None => projection.push(SelectItem::QualifiedWildcard(
                            SelectItemQualifiedWildcardKind::ObjectName(n),
                            opts,
                        )),
                    }
                }
                other => projection.push(other),
            }
        }
        s.projection = projection;
        // Factors and filters.
        let mut where_filters = Vec::new();
        for twj in &mut s.from {
            self.rewrite_twj(twj, &planned, &mut where_filters);
        }
        for f in where_filters {
            match expr(&f) {
                Ok(fe) => {
                    s.selection = Some(match s.selection.take() {
                        Some(w) => Expr::BinaryOp {
                            left: Box::new(Expr::Nested(Box::new(w))),
                            op: sqlparser::ast::BinaryOperator::And,
                            right: Box::new(fe),
                        },
                        None => fe,
                    });
                }
                Err(e) => self.fail(e),
            }
        }
    }

    fn rewrite_twj(
        &mut self,
        twj: &mut TableWithJoins,
        planned: &[usize],
        where_filters: &mut Vec<String>,
    ) {
        // The relation, then each join; a filter goes in the ON of a LEFT
        // JOIN that makes the reference nullable, or of a later RIGHT JOIN.
        let right_join = twj.joins.iter().position(|j| {
            matches!(
                j.join_operator,
                JoinOperator::Right(_) | JoinOperator::RightOuter(_)
            )
        });
        let mut pending: Option<String> = None;
        if let Some(f) = self.rewrite_factor(&mut twj.relation, planned, where_filters) {
            pending = Some(f);
        }
        if let Some(f) = pending.take() {
            match right_join {
                Some(_) => pending = Some(f),
                None => where_filters.push(f),
            }
        }
        for (i, j) in twj.joins.iter_mut().enumerate() {
            let left = matches!(
                j.join_operator,
                JoinOperator::Left(_) | JoinOperator::LeftOuter(_)
            );
            let mut filters = Vec::new();
            if let Some(f) = self.rewrite_factor(&mut j.relation, planned, &mut filters) {
                filters.push(f);
            }
            if Some(i) == right_join
                && let Some(f) = pending.take()
            {
                filters.push(f);
            }
            for f in filters {
                if left || Some(i) == right_join {
                    match (constraint_mut(&mut j.join_operator), expr(&f)) {
                        (Some(JoinConstraint::On(on)), Ok(fe)) => {
                            let old = std::mem::replace(on, Expr::Value(Value::Null.into()));
                            *on = Expr::BinaryOp {
                                left: Box::new(Expr::Nested(Box::new(old))),
                                op: sqlparser::ast::BinaryOperator::And,
                                right: Box::new(fe),
                            };
                        }
                        (_, Err(e)) => self.fail(e),
                        _ => self.fail(SprigError::new(
                            SprigCode::TypeUnsupported,
                            "an outer join of a typed table needs an ON condition",
                        )),
                    }
                } else if right_join.is_some_and(|r| r > i) {
                    pending = Some(match pending.take() {
                        Some(p) => format!("{p} AND {f}"),
                        None => f,
                    });
                } else {
                    where_filters.push(f);
                }
            }
        }
        if let Some(f) = pending {
            where_filters.push(f);
        }
    }

    /// Replaces a planned reference's factor; returns its filter.
    fn rewrite_factor(
        &mut self,
        f: &mut TableFactor,
        planned: &[usize],
        filters: &mut Vec<String>,
    ) -> Option<String> {
        match f {
            TableFactor::Table { name, alias, .. } => {
                let r = planned.iter().copied().find(|&r| {
                    let rr = &self.refs[r];
                    let table = last(name).unwrap_or_default();
                    let table = table.strip_prefix(ONLY_PREFIX).unwrap_or(&table).to_owned();
                    let q = alias
                        .as_ref()
                        .map_or(table.clone(), |a| a.name.value.clone());
                    q == rr.qualifier && table.eq_ignore_ascii_case(&rr.t.name)
                })?;
                let rr = &self.refs[r];
                let plan = rr.plan.as_ref()?;
                let key = rr
                    .db
                    .root_of(rr.t.id)
                    .map(|t| t.key_fields.clone())
                    .unwrap_or_default();
                let sql = plan.join_sql(rr.db_name.as_deref(), &key);
                let filter = plan.filter.clone();
                match factor(&sql) {
                    Ok(nf) => *f = nf,
                    Err(e) => self.fail(e),
                }
                filter
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.rewrite_twj(table_with_joins, planned, filters);
                None
            }
            _ => None,
        }
    }

    /// `alias.f` for every field of reference `r`, inherited first. A
    /// single-table root (read with `ONLY`) keeps `alias.*`: its table is
    /// what clients and dump tools know, every column of the hierarchy.
    fn expansion(&self, r: usize) -> Vec<SelectItem> {
        let rr = &self.refs[r];
        let Some(plan) = &rr.plan else {
            return Vec::new();
        };
        if rr.t.is_root() && rr.t.storage == Storage::SingleTable {
            return vec![SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(ObjectName::from(vec![qident(
                    plan.key_alias(),
                )])),
                sqlparser::ast::WildcardAdditionalOptions::default(),
            )];
        }
        rr.db
            .all_fields(rr.t.id)
            .into_iter()
            .map(|f| {
                let a = plan.alias_for(Some(f.owner), f.is_key);
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![qident(a), qident(&f.name)]))
            })
            .collect()
    }

    /// The reference a qualifier names (`[q]` or `[db, t]`), innermost
    /// scope first.
    fn find_qualified(&self, p: &[String]) -> Option<usize> {
        let lctn = self.lookup.lctn();
        for frame in self.stack.iter().rev() {
            for &r in &frame.refs {
                let rr = &self.refs[r];
                let hit = match p {
                    [q] => ident_eq(q, &rr.qualifier, lctn),
                    [db, t] => {
                        rr.qualifier == rr.t.name
                            && ident_eq(t, &rr.t.name, lctn)
                            && rr.db_name.as_deref().is_none_or(|d| ident_eq(d, db, lctn))
                    }
                    _ => false,
                };
                if hit {
                    return Some(r);
                }
            }
            if frame
                .tables
                .iter()
                .any(|(q, _, _)| p.len() == 1 && ident_eq(q, &p[0], lctn))
            {
                return None; // an untyped table shadows outer references
            }
        }
        None
    }

    /// The reference an unqualified column belongs to.
    fn find_unqualified(&self, col: &str, alias_ok: bool) -> Option<usize> {
        let mut first = true;
        for frame in self.stack.iter().rev() {
            if first && alias_ok && frame.aliases.contains(&col.to_lowercase()) {
                return None;
            }
            for &r in &frame.refs {
                if self.refs[r].owner_of(col).is_some() {
                    return Some(r);
                }
            }
            if !frame.all_typed {
                return None;
            }
            first = false;
        }
        None
    }

    fn walk_expr(&mut self, e: &mut Expr, alias_ok: bool) {
        let mut v = ExprVisitor {
            w: self,
            alias_ok,
            depth: 0,
        };
        let _ = e.visit(&mut v);
    }

    fn on_column(&mut self, e: &mut Expr, alias_ok: bool) {
        let (r, col) = match e {
            Expr::Identifier(id) => match self.find_unqualified(&id.value, alias_ok) {
                Some(r) => (r, id.value.clone()),
                None => return,
            },
            Expr::CompoundIdentifier(ids) if ids.len() >= 2 => {
                let p: Vec<String> = ids[..ids.len() - 1]
                    .iter()
                    .map(|i| i.value.clone())
                    .collect();
                let col = ids[ids.len() - 1].value.clone();
                match self.find_qualified(&p) {
                    Some(r) if self.refs[r].owner_of(&col).is_some() => (r, col),
                    _ => return,
                }
            }
            _ => return,
        };
        match self.pass {
            Pass::Collect => {
                self.refs[r].used.insert(col.to_lowercase());
            }
            Pass::Rewrite => {
                let rr = &self.refs[r];
                let Some(plan) = &rr.plan else { return };
                let is_key = rr
                    .db
                    .key_defs(rr.t.id)
                    .iter()
                    .any(|k| k.name.eq_ignore_ascii_case(&col));
                let a = plan.alias_for(rr.owner_of(&col), is_key).to_owned();
                let quote_style = match e {
                    Expr::Identifier(id) => id.quote_style,
                    Expr::CompoundIdentifier(ids) => ids.last().and_then(|i| i.quote_style),
                    _ => None,
                };
                let mut c = Ident::new(col);
                c.quote_style = quote_style;
                *e = Expr::CompoundIdentifier(vec![qident(&a), c]);
            }
        }
    }

    fn on_function(&mut self, e: &mut Expr) {
        let Expr::Function(f) = e else { return };
        let Some(func) = last(&f.name).and_then(|n| HierFn::of(&n)) else {
            return;
        };
        let FunctionArguments::List(list) = &f.args else {
            self.fail(hier_usage(func));
            return;
        };
        let args: Vec<&Expr> = list
            .args
            .iter()
            .filter_map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                _ => None,
            })
            .collect();
        let want = if matches!(func, HierFn::Depth | HierFn::Path) {
            1
        } else {
            2
        };
        if args.len() != want || args.len() != list.args.len() {
            self.fail(hier_usage(func));
            return;
        }
        let tparts: Vec<String> = match args[0] {
            Expr::Identifier(i) => vec![i.value.clone()],
            Expr::CompoundIdentifier(ids) => ids.iter().map(|i| i.value.clone()).collect(),
            _ => {
                self.fail(hier_usage(func));
                return;
            }
        };
        let id = self.next_call;
        self.next_call += 1;
        match self.pass {
            Pass::Collect => {
                let Some(call) = self.resolve_call(&tparts, func, args.get(1).copied()) else {
                    return;
                };
                self.calls.push(call);
            }
            Pass::Rewrite => {
                let Some(call) = self.calls.get(id).cloned() else {
                    return;
                };
                match self.call_sql(id, &call).and_then(|s| expr(&s)) {
                    Ok(ne) => *e = Expr::Nested(Box::new(ne)),
                    Err(err) => self.fail(err),
                }
            }
        }
    }

    fn resolve_call(
        &mut self,
        tparts: &[String],
        func: HierFn,
        key: Option<&Expr>,
    ) -> Option<HierCall> {
        let lctn = self.lookup.lctn();
        let mut found = None;
        'frames: for frame in self.stack.iter().rev() {
            for (q, db, table) in &frame.tables {
                let hit = match tparts {
                    [x] => ident_eq(x, q, lctn),
                    [d, t] => {
                        ident_eq(t, table, lctn)
                            && db.as_deref().is_none_or(|x| ident_eq(x, d, lctn))
                    }
                    _ => false,
                };
                if hit {
                    found = Some((q.clone(), db.clone(), table.clone()));
                    break 'frames;
                }
            }
        }
        let Some((qualifier, db_name, table)) = found else {
            self.fail(SprigError::new(
                SprigCode::TypeUnsupported,
                format!(
                    "{}: '{}' is not a table of the query",
                    fn_name(func),
                    tparts.join(".")
                ),
            ));
            return None;
        };
        let h = self
            .lookup
            .db(db_name.as_deref())
            .and_then(|d| d.hierarchy(&table, lctn).cloned());
        let Some(h) = h else {
            self.fail(SprigError::new(
                SprigCode::TypeUnsupported,
                format!("{}: table '{table}' has no hierarchy", fn_name(func)),
            ));
            return None;
        };
        let (key_literal, key_expr) = match key {
            None => (None, None),
            Some(Expr::Value(v)) => (Some(v.to_string()), None),
            Some(Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Minus,
                expr,
            }) if matches!(**expr, Expr::Value(_)) => (Some(format!("-{expr}")), None),
            Some(other) => (None, Some(other.to_string())),
        };
        if func == HierFn::Ancestors && key_expr.is_some() {
            self.fail(SprigError::new(
                SprigCode::TypeUnsupported,
                "SPRIG_ANCESTORS needs a literal key",
            ));
            return None;
        }
        Some(HierCall {
            h,
            db_name,
            qualifier,
            func,
            key_literal,
            key_expr,
        })
    }

    fn call_sql(&self, id: usize, c: &HierCall) -> Result<String, SprigError> {
        let q = quote(&c.qualifier);
        let path = format!("{q}.`_sprig_hpath`");
        let rk = format!("{q}.`_sprig_rk`");
        match c.func {
            HierFn::Depth => return Ok(format!("{q}.`_sprig_hdepth`")),
            HierFn::Path => return Ok(format!("CAST({path} AS CHAR)")),
            _ => {}
        }
        if let Some(k) = &c.key_expr {
            let table = match &c.db_name {
                Some(d) => format!("{}.{}", quote(d), quote(&c.h.table)),
                None => quote(&c.h.table),
            };
            let node = |col: &str| {
                format!(
                    "(SELECT {col} FROM {table} AS `sprig_h` WHERE `sprig_h`.{key} = {k})",
                    key = quote(&c.h.key)
                )
            };
            let lo = node("`sprig_h`.`_sprig_hpath`");
            let hi = node(
                "CONCAT(LEFT(`sprig_h`.`_sprig_hpath`, LENGTH(`sprig_h`.`_sprig_hpath`) - 1), '0')",
            );
            let op = if c.func == HierFn::Subtree { ">=" } else { ">" };
            return Ok(format!("{path} {op} {lo} AND {path} < {hi}"));
        }
        let Some(read) = self.reads.get(&id).cloned().flatten() else {
            return Ok("FALSE".into());
        };
        let rk_lit = key_literal(&read.rk, c.h.key_class, self.no_backslash_escapes);
        match c.func {
            HierFn::Subtree | HierFn::Descendants => {
                let op = if c.func == HierFn::Subtree { ">=" } else { ">" };
                Ok(format!(
                    "{path} {op} {} AND {path} < {} AND {rk} = {rk_lit}",
                    hex_lit(&read.path),
                    hex_lit(&upper(&read.path))
                ))
            }
            HierFn::Ancestors => {
                let segs = segments(&read.path);
                if segs.len() < 2 {
                    return Ok("FALSE".into());
                }
                let keys: Vec<String> = segs[..segs.len() - 1]
                    .iter()
                    .map(|s| seg_literal(s, c.h.key_class, self.no_backslash_escapes))
                    .collect();
                Ok(format!(
                    "{q}.{} IN ({}) AND {rk} = {rk_lit}",
                    quote(&c.h.key),
                    keys.join(", ")
                ))
            }
            HierFn::Depth | HierFn::Path => unreachable_sql(),
        }
    }
}

fn unreachable_sql() -> Result<String, SprigError> {
    Err(SprigError::internal("hierarchy function"))
}

fn fn_name(f: HierFn) -> &'static str {
    match f {
        HierFn::Subtree => "SPRIG_SUBTREE",
        HierFn::Descendants => "SPRIG_DESCENDANTS",
        HierFn::Ancestors => "SPRIG_ANCESTORS",
        HierFn::Depth => "SPRIG_DEPTH",
        HierFn::Path => "SPRIG_PATH",
    }
}

fn hier_usage(f: HierFn) -> SprigError {
    let args = if matches!(f, HierFn::Depth | HierFn::Path) {
        "(table)"
    } else {
        "(table, key)"
    };
    SprigError::standard(
        1582,
        *b"42000",
        format!(
            "Incorrect parameter count in the call to native function '{}{args}'",
            fn_name(f)
        ),
    )
}

/// The `ON` constraint of a join operator.
fn constraint_mut(op: &mut JoinOperator) -> Option<&mut JoinConstraint> {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::StraightJoin(c) => Some(c),
        _ => None,
    }
}

struct ExprVisitor<'a, 'l> {
    w: &'a mut Walker<'l>,
    alias_ok: bool,
    depth: usize,
}

impl VisitorMut for ExprVisitor<'_, '_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        if self.depth == 0 {
            self.w.walk_query(q);
        }
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _q: &mut Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        if self.depth == 0 {
            match e {
                Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                    self.w.on_column(e, self.alias_ok);
                }
                Expr::Function(_) => self.w.on_function(e),
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }
}

/// The segments of a path, `/1/7/42/` to `["1", "7", "42"]`.
#[must_use]
pub fn segments(path: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(path)
        .split('/')
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// A key's segment (chapter 32, "Hierarchy columns"): integers in decimal,
/// strings with `%`, `/` and `,` escaped, binary in upper-case hex.
#[must_use]
pub fn segment(key: &[u8], class: KeyClass) -> String {
    match class {
        KeyClass::Int => String::from_utf8_lossy(key).into_owned(),
        KeyClass::Str => String::from_utf8_lossy(key)
            .replace('%', "%25")
            .replace('/', "%2F")
            .replace(',', "%2C"),
        KeyClass::Bin => {
            use std::fmt::Write;
            key.iter().fold(String::new(), |mut s, b| {
                let _ = write!(s, "{b:02X}");
                s
            })
        }
    }
}

/// The SQL expression computing a key's segment in MariaDB.
#[must_use]
pub fn segment_sql(col: &str, class: KeyClass) -> String {
    match class {
        KeyClass::Int => format!("CAST({col} AS CHAR)"),
        KeyClass::Str => {
            format!("REPLACE(REPLACE(REPLACE({col}, '%', '%25'), '/', '%2F'), ',', '%2C')")
        }
        KeyClass::Bin => format!("HEX({col})"),
    }
}

/// The key literal a segment stands for.
#[must_use]
pub fn seg_literal(seg: &str, class: KeyClass, nbe: bool) -> String {
    match class {
        KeyClass::Int => {
            if seg.bytes().all(|b| b.is_ascii_digit() || b == b'-') && !seg.is_empty() {
                seg.to_owned()
            } else {
                quote_str(seg, nbe)
            }
        }
        KeyClass::Str => quote_str(
            &seg.replace("%2C", ",")
                .replace("%2F", "/")
                .replace("%25", "%"),
            nbe,
        ),
        KeyClass::Bin => format!("X'{seg}'"),
    }
}

/// A key value (as the text protocol returns it) as a literal.
#[must_use]
pub fn key_literal(v: &[u8], class: KeyClass, nbe: bool) -> String {
    match class {
        KeyClass::Int if !v.is_empty() && v.iter().all(|b| b.is_ascii_digit() || *b == b'-') => {
            String::from_utf8_lossy(v).into_owned()
        }
        KeyClass::Bin => hex_lit(v),
        _ => match std::str::from_utf8(v) {
            Ok(s) => quote_str(s, nbe),
            Err(_) => hex_lit(v),
        },
    }
}

#[cfg(test)]
pub(crate) mod tests;
