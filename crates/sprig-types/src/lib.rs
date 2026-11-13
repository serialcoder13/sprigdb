//! `sprig-types`: types, inheritance and hierarchies.
//!
//! Layer: 2 data layer. Design: [chapter 32](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/32-types.md) (Types) of the SprigDB low-level design.
//!
//! What exists (R6 order 1, SQL on single-shard tenants):
//!
//! - [`model`] and [`store`]: the type catalog in memory and its four
//!   catalog tables, loaded into every snapshot by [`TypesExtension`]
//!   (build step 1);
//! - [`grammar`]: `INHERITS`, `WITH (SPRIG_...)`, `ONLY`, `ADD | DROP
//!   HIERARCHY`, `@sprig_inheritance` (step 2);
//! - [`ddl`]: type DDL with joined and single-table storage (steps 3 and
//!   6), and discriminator columns: `INHERITS (p) WHERE c = v`, under which
//!   `_type` and `_type_path` are generated from a user column;
//! - [`rewrite`], [`show`] and [`infoschema`]: polymorphic reads and the
//!   metadata schema tools see (step 4), and the `SPRIG_*` hierarchy
//!   predicates (step 10);
//! - [`write`](mod@write): typed writes with MySQL's counts (step 5);
//! - [`hier`]: record hierarchies, backfill and local subtree moves (steps
//!   10 and 11, one shard).
//!
//! [`handle`] is the entry point: the gateway calls it for each statement
//! of a tenant session before chapter 07's name plan, with a [`Runner`]
//! that sends the plan's statements back through the session (logical
//! names, chapter 29's checks, chapter 17's router).
//!
//! Not built: composites and arrays, documents and
//! the other models (they need R5), footprints for chapter 29, rename
//! jobs, cross-shard moves, `$graphLookup`, typed upserts and
//! `INSERT ... SELECT` into subtypes, prepared statements on typed tables.

// The plans read best as the chapter's steps in order, over SQL text built
// with format!; short names (t, h, r, k) follow the chapter's pseudocode.
#![allow(
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::format_push_string,
    clippy::struct_excessive_bools
)]

pub mod ddl;
pub mod exec;
pub mod grammar;
pub mod hier;
pub mod infoschema;
pub mod model;
pub mod rewrite;
pub mod settings;
pub mod show;
pub mod store;
pub mod tabledef;
pub mod write;

use std::sync::Arc;

use sprig_common::Result;
use sprig_common::ids::DbId;
use sprig_sql::lex::LexOptions;
use sqlparser::ast::{ObjectName, ObjectNamePart, Statement, VisitMut, VisitorMut};

pub use exec::{Done, HierChange, Limits, Rows, Runner, Txn};
pub use model::{DbTypes, HierarchyDef, TypeDef, TypesState};
pub use rewrite::{Lookup, MetaRename};
pub use store::{TYPES_MODULE, TypesChange, TypesExtension, TypesStore};

use crate::exec::unsupported;
use crate::grammar::{ONLY_PREFIX, Stmt};
use crate::rewrite::{HierFn, Pass, Walker, parse_one};
use crate::write::WriteOutcome;

/// What a statement needs from the type layer.
pub struct Ctx<'a> {
    /// The tenant's types by database.
    pub lookup: &'a dyn Lookup,
    /// The session's lexing options.
    pub lex: LexOptions,
    /// `@sprig_inheritance = 'only'`.
    pub inheritance_only: bool,
    /// Settings.
    pub limits: Limits,
}

/// What the gateway does with a statement.
#[derive(Debug)]
pub enum Handled {
    /// Not the type layer's: the statement goes on unchanged.
    NotTyped,
    /// Send this statement (logical names) instead; its result is the
    /// client's, with column metadata renamed by `meta`.
    Forward {
        /// The statement.
        sql: String,
        /// Result-metadata renames.
        meta: Vec<MetaRename>,
    },
    /// The plan ran: answer OK.
    Ok(Done),
    /// The plan's answer: a result set.
    Rows(Rows),
    /// `SET @sprig_inheritance`: the session's new mode.
    Inheritance(Option<bool>),
}

/// The tenant's types as a [`Lookup`] over a [`TypesState`]: the caller
/// maps logical database names to ids.
pub struct StateLookup<'a> {
    /// The state.
    pub state: &'a TypesState,
    /// Logical database name (`None`: the current one) to its id.
    pub db_id: &'a (dyn Fn(Option<&str>) -> Option<DbId> + Send + Sync),
    /// `lower_case_table_names`.
    pub lctn: bool,
    /// A database id to its logical name.
    pub db_name: &'a (dyn Fn(DbId) -> Option<String> + Send + Sync),
}

impl Lookup for StateLookup<'_> {
    fn databases(&self) -> Vec<(String, Arc<DbTypes>)> {
        self.state
            .dbs()
            .filter(|(_, d)| d.iter().next().is_some())
            .filter_map(|(id, d)| Some(((self.db_name)(id)?, Arc::clone(d))))
            .collect()
    }

    fn db(&self, db: Option<&str>) -> Option<Arc<DbTypes>> {
        self.state.db((self.db_id)(db)?).cloned()
    }

    fn lctn(&self) -> bool {
        self.lctn
    }
}

/// A quick test on the raw text: could the statement concern the type
/// layer? False for every statement when the tenant has no types and the
/// text has no extension keyword, so plain tenants pay one scan.
#[must_use]
pub fn maybe_typed(sql: &[u8], state: Option<&TypesState>) -> bool {
    let has_types = state.is_some_and(|s| !s.is_empty());
    if has_types {
        return true;
    }
    let upper = sql.to_ascii_uppercase();
    let has = |w: &[u8]| upper.windows(w.len()).any(|x| x == w);
    has(b"INHERITS") || has(b"HIERARCHY") || has(b"SPRIG_") || has(b"CREATE TYPE")
}

/// Whether `sql` names a typed or hierarchical table in any of the
/// tenant's databases, or calls a `SPRIG_*` function
/// ([`grammar::calls_sprig_function`]: only the real names, in call
/// position, so a database named `sprig_dump_1` is not one).
fn names_typed(sql: &[u8], ctx: &Ctx<'_>, state: Option<&TypesState>) -> bool {
    let Some(state) = state else { return false };
    let mut names: Vec<String> = Vec::new();
    for (_, d) in state.dbs() {
        names.extend(d.iter().map(|t| t.name.to_lowercase()));
        names.extend(d.hierarchies().map(|h| h.table.to_lowercase()));
    }
    grammar::identifiers(sql, &ctx.lex)
        .iter()
        .any(|i| names.contains(&i.to_lowercase()))
        || grammar::calls_sprig_function(sql, &ctx.lex)
}

fn strip_only_marks(stmt: &mut Statement) {
    struct V;
    impl VisitorMut for V {
        type Break = ();
        fn pre_visit_relation(&mut self, n: &mut ObjectName) -> std::ops::ControlFlow<()> {
            if let Some(ObjectNamePart::Identifier(i)) = n.0.last_mut()
                && let Some(rest) = i.value.strip_prefix(ONLY_PREFIX)
            {
                i.value = rest.to_owned();
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let _ = stmt.visit(&mut V);
}

fn table_sql(db: Option<&str>, t: &str) -> String {
    match db {
        Some(d) => format!(
            "{}.{}",
            sprig_common::names::quote(d),
            sprig_common::names::quote(t)
        ),
        None => sprig_common::names::quote(t),
    }
}

/// A query on typed tables or with `SPRIG_*` calls.
async fn query(
    sql: &str,
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
    typed: bool,
    info: bool,
) -> Result<Handled> {
    let lctn = ctx.lookup.lctn();
    let is_typed = |db: Option<&str>, n: &str| {
        ctx.lookup.db(db).is_some_and(|d| {
            d.get(n, lctn)
                .is_some_and(|t| d.has_subtypes(t.id) || !t.is_root())
        })
    };
    let marked = grammar::mark_only(sql.as_bytes(), &ctx.lex, &is_typed);
    let text = marked.as_deref().unwrap_or(sql);
    let mut stmt = match parse_one(text) {
        Ok(s) => s,
        // A metadata query sqlparser-rs can't read goes on unchanged.
        Err(_) if !typed && marked.is_none() => return Ok(Handled::NotTyped),
        Err(e) => return Err(e),
    };
    let mut info_changed = false;
    if info {
        let view = if infoschema::reads_columns(&mut stmt) {
            let shape = r
                .query("SELECT * FROM information_schema.COLUMNS WHERE 1 = 0")
                .await?;
            let names: Vec<String> = shape
                .columns
                .iter()
                .map(|c| String::from_utf8_lossy(&c.name).into_owned())
                .collect();
            Some(infoschema::columns_view(&ctx.lookup.databases(), &names))
        } else {
            None
        };
        info_changed = infoschema::rewrite(&mut stmt, view.as_deref());
    }
    let Statement::Query(q) = &mut stmt else {
        return Ok(Handled::NotTyped);
    };
    let mut w = Walker::new(ctx.lookup);
    w.dump = grammar::is_dump_read(sql.as_bytes());
    w.only_all = ctx.inheritance_only || w.dump;
    w.no_backslash_escapes = ctx.lex.no_backslash_escapes;
    w.query(Pass::Collect, q)?;
    w.plan_refs();
    let planned = w.refs.iter().any(|r| r.plan.is_some());
    if !planned && w.calls.is_empty() && marked.is_none() && !info_changed {
        return Ok(Handled::NotTyped);
    }
    // Node reads for the SPRIG_* calls with a literal key.
    for (i, c) in w.calls.clone().iter().enumerate() {
        let (Some(k), true) = (
            &c.key_literal,
            matches!(
                c.func,
                HierFn::Subtree | HierFn::Descendants | HierFn::Ancestors
            ),
        ) else {
            continue;
        };
        let rows = r
            .query(&format!(
                "SELECT `_sprig_hpath`, `_sprig_rk` FROM {} WHERE {} = {k}",
                table_sql(c.db_name.as_deref(), &c.h.table),
                sprig_common::names::quote(&c.h.key)
            ))
            .await?;
        let read = rows.rows.first().and_then(|row| {
            Some(rewrite::NodeRead {
                path: row.first()?.clone()?,
                rk: row.get(1)?.clone()?,
            })
        });
        w.reads.insert(i, read);
    }
    w.query(Pass::Rewrite, q)?;
    strip_only_marks(&mut stmt);
    metrics::counter!("sprig_types_statements_total", "kind" => "select", "plan" => "rewrite")
        .increment(1);
    Ok(Handled::Forward {
        sql: stmt.to_string(),
        meta: w.meta(),
    })
}

fn outcome(o: WriteOutcome) -> Handled {
    match o {
        WriteOutcome::NotTyped => Handled::NotTyped,
        WriteOutcome::Forward(sql) => Handled::Forward {
            sql,
            meta: Vec::new(),
        },
        WriteOutcome::Done(d) => Handled::Ok(d),
    }
}

fn target_hierarchy(
    name: &ObjectName,
    ctx: &Ctx<'_>,
) -> Option<(Option<String>, Arc<HierarchyDef>)> {
    let p: Vec<String> = name
        .0
        .iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.clone()),
            ObjectNamePart::Function(_) => None,
        })
        .collect();
    let (db, t) = match p.as_slice() {
        [t] => (None, t.clone()),
        [d, t] => (Some(d.clone()), t.clone()),
        _ => return None,
    };
    let h = ctx
        .lookup
        .db(db.as_deref())?
        .hierarchy(&t, ctx.lookup.lctn())?
        .clone();
    Some((db, h))
}

/// Handles one statement of a tenant session (see the crate docs).
///
/// `state` is the tenant's [`TypesState`] at the session's snapshot.
///
/// # Errors
/// The chapter's errors (39700-39716), or a step's error with logical
/// names.
#[allow(clippy::too_many_lines)] // one arm per statement kind
pub async fn handle(
    sql: &str,
    state: Option<&TypesState>,
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
) -> Result<Handled> {
    if !maybe_typed(sql.as_bytes(), state) || grammar::names_reserved(sql.as_bytes(), &ctx.lex) {
        return Ok(Handled::NotTyped);
    }
    let lctn = ctx.lookup.lctn();
    let stmt = grammar::classify(sql.as_bytes(), &ctx.lex)?;
    let touches = || names_typed(sql.as_bytes(), ctx, state);
    match stmt {
        Stmt::SetInheritance(v) => Ok(Handled::Inheritance(v)),
        Stmt::CreateType => Err(unsupported(
            "CREATE TYPE (composite and entity types) is not supported yet",
        )),
        Stmt::CreateTable(ct) if ct.is_typed() => ddl::create(&ct, ctx, r).await.map(Handled::Ok),
        Stmt::CreateTable(ct) if ct.like_or_select && touches() => Err(unsupported(
            "CREATE TABLE ... LIKE or ... SELECT naming a typed table",
        )),
        Stmt::CreateOther if touches() => Err(unsupported(
            "a view or routine naming a typed or hierarchical table",
        )),
        Stmt::AddHierarchy(ah) => hier::add(&ah, ctx, r).await.map(Handled::Ok),
        Stmt::DropHierarchy(n) => hier::drop(&n, ctx, r).await.map(Handled::Ok),
        Stmt::AddDiscriminator(d) => ddl::add_discriminator(&d, ctx, r).await.map(Handled::Ok),
        Stmt::DropDiscriminator(n) => ddl::drop_discriminator(&n, ctx, r).await.map(Handled::Ok),
        Stmt::AlterTable(at) => {
            let Some(types) = ctx.lookup.db(at.table.db.as_deref()) else {
                return Ok(Handled::NotTyped);
            };
            match types.get(&at.table.name, lctn).cloned() {
                Some(t) if at.changes => ddl::alter(&at, sql, &t, &types, ctx, r)
                    .await
                    .map(Handled::Ok),
                _ => Ok(Handled::NotTyped),
            }
        }
        Stmt::DropTable(dt) if !dt.temporary => {
            let any = dt.tables.iter().any(|n| {
                ctx.lookup.db(n.db.as_deref()).is_some_and(|d| {
                    d.get(&n.name, lctn).is_some() || d.hierarchy(&n.name, lctn).is_some()
                })
            });
            if any {
                ddl::drop(&dt, ctx, r).await.map(Handled::Ok)
            } else {
                Ok(Handled::NotTyped)
            }
        }
        Stmt::DropDatabase(name) => {
            let Some(types) = ctx.lookup.db(Some(&name)) else {
                return Ok(Handled::NotTyped);
            };
            if types.is_empty() {
                return Ok(Handled::NotTyped);
            }
            let done = r.exec(sql).await?;
            r.commit_types(&TypesChange {
                delete_types: types.iter().map(|t| t.id).collect(),
                delete_hierarchies: types.hierarchies().map(|h| h.object_id).collect(),
                ..TypesChange::default()
            })
            .await?;
            Ok(Handled::Ok(done))
        }
        Stmt::TruncateOrRename => {
            let typed = grammar::identifiers(sql.as_bytes(), &ctx.lex)
                .iter()
                .any(|n| {
                    ctx.lookup
                        .db(None)
                        .is_some_and(|d| d.get(n, lctn).is_some())
                });
            if typed {
                Err(unsupported("TRUNCATE or RENAME of a typed table"))
            } else {
                Ok(Handled::NotTyped)
            }
        }
        Stmt::ShowCreate(n) => {
            let Some(types) = ctx.lookup.db(n.db.as_deref()) else {
                return Ok(Handled::NotTyped);
            };
            match types.get(&n.name, lctn).cloned() {
                Some(t) => show::create(&n, &t, &types, ctx, r)
                    .await
                    .map(Handled::Rows),
                None => Ok(Handled::NotTyped),
            }
        }
        Stmt::ShowColumns(sc) => {
            let Some(types) = ctx.lookup.db(sc.table.db.as_deref()) else {
                return Ok(Handled::NotTyped);
            };
            match types.get(&sc.table.name, lctn).cloned() {
                Some(t) => show::columns(&sc, &t, &types, r).await.map(Handled::Rows),
                None => Ok(Handled::NotTyped),
            }
        }
        Stmt::ShowTables { db } => {
            let Some(types) = ctx.lookup.db(db.as_deref()) else {
                return Ok(Handled::NotTyped);
            };
            if !types.iter().any(|t| !t.is_root()) {
                return Ok(Handled::NotTyped);
            }
            let mut rows = r.query(sql).await?;
            rows.rows = show::order_tables(std::mem::take(&mut rows.rows), &types, lctn);
            Ok(Handled::Rows(rows))
        }
        Stmt::Query => {
            let typed = touches();
            let info = state.is_some_and(|s| !s.is_empty())
                && infoschema::mentions(&grammar::identifiers(sql.as_bytes(), &ctx.lex));
            if !typed && !info {
                return Ok(Handled::NotTyped);
            }
            query(sql, ctx, r, typed, info).await
        }
        Stmt::Insert | Stmt::Update | Stmt::Delete => {
            if !touches() {
                return Ok(Handled::NotTyped);
            }
            let lctn_marked = |db: Option<&str>, n: &str| {
                ctx.lookup.db(db).is_some_and(|d| {
                    d.get(n, lctn)
                        .is_some_and(|t| d.has_subtypes(t.id) || !t.is_root())
                })
            };
            let marked = grammar::mark_only(sql.as_bytes(), &ctx.lex, &lctn_marked);
            let stmt = parse_one(marked.as_deref().unwrap_or(sql))?;
            match &stmt {
                Statement::Insert(ins) => {
                    if let sqlparser::ast::TableObject::TableName(n) = &ins.table
                        && let Some((db, h)) = target_hierarchy(n, ctx)
                    {
                        return hier::insert(ins, sql, &h, db.as_deref(), ctx, r)
                            .await
                            .map(outcome);
                    }
                    write::insert(ins, ctx, r).await.map(outcome)
                }
                Statement::Update(upd) => {
                    if let sqlparser::ast::TableFactor::Table { name, .. } = &upd.table.relation
                        && let Some((db, h)) = target_hierarchy(name, ctx)
                    {
                        return hier::update(upd, sql, &h, db.as_deref(), ctx, r)
                            .await
                            .map(outcome);
                    }
                    write::update(upd, ctx, r).await.map(outcome)
                }
                Statement::Delete(del) => {
                    let from = match &del.from {
                        sqlparser::ast::FromTable::WithFromKeyword(v)
                        | sqlparser::ast::FromTable::WithoutKeyword(v) => v,
                    };
                    if let Some(sqlparser::ast::TableFactor::Table { name, .. }) =
                        from.first().map(|t| &t.relation)
                        && let Some((db, h)) = target_hierarchy(name, ctx)
                    {
                        return hier::delete(del, &h, db.as_deref(), ctx, r)
                            .await
                            .map(outcome);
                    }
                    write::delete(del, ctx, r).await.map(outcome)
                }
                _ => Ok(Handled::NotTyped),
            }
        }
        _ => Ok(Handled::NotTyped),
    }
}

#[cfg(test)]
mod tests;

/// A backend error made the client's: a failed `NOT NULL` check of a
/// single-table subtype (4025, or 3819, naming a `_sprig_nn_` constraint) becomes
/// MySQL's 1048 for the column. `None` for any other error.
#[must_use]
pub fn map_error(
    state: Option<&TypesState>,
    e: &sprig_common::SprigError,
) -> Option<sprig_common::SprigError> {
    // MariaDB's 4025, or MySQL's 3819 on the MySQL persona.
    if !matches!(e.protocol().mysql_errno, 4025 | 3819) {
        return None;
    }
    if e.message.contains(ddl::DISC_CHECK) {
        return Some(sprig_common::SprigError::new(
            sprig_common::SprigCode::TypeDiscriminatorMismatch,
            "the discriminator's value doesn't match the row's type while the hierarchy takes              its discriminator",
        ));
    }
    let at = e.message.find(ddl::NN_PREFIX)?;
    let name: String = e.message[at..]
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    let state = state?;
    for (_, d) in state.dbs() {
        for t in d.iter().filter(|t| !t.is_root()) {
            if let Some(f) = t
                .own_fields
                .iter()
                .find(|f| ddl::nn_name(&t.name, &f.name) == name)
            {
                return Some(sprig_common::SprigError::standard(
                    1048,
                    *b"23000",
                    format!("Column '{}' cannot be null", f.name),
                ));
            }
        }
    }
    None
}

/// The type columns a merged view of an inheritable table computes
/// (chapter 42, typed tables in inheritance groups): for a discriminated
/// root, `_type` and `_type_path` from the discriminator's merged value, by
/// this tenant's types (a child tenant's own subtypes included), as
/// `(column, expression)`. Empty for any other table: `_type` hierarchies
/// keep their type columns as ordinary fields.
#[must_use]
pub fn derived_columns(
    state: &TypesState,
    db: DbId,
    table: &str,
    lctn: bool,
    nbe: bool,
) -> Vec<(String, String)> {
    let Some(d) = state.db(db) else {
        return Vec::new();
    };
    let Some(t) = d.get(table, lctn) else {
        return Vec::new();
    };
    let (true, Some(col)) = (t.is_root(), &t.discriminator) else {
        return Vec::new();
    };
    let map = ddl::DiscMap {
        operand: t
            .disc_operand
            .clone()
            .unwrap_or_else(|| sprig_common::names::quote(col)),
        root: (t.name.clone(), t.path.clone()),
        arms: d
            .descendants(t.id)
            .into_iter()
            .filter_map(|x| Some((x.disc_value.clone()?, x.name.clone(), x.path.clone())))
            .collect(),
    };
    let (te, pe) = map.exprs(nbe);
    vec![
        (rewrite::TYPE_COLUMN.to_owned(), te),
        (rewrite::TYPE_PATH_COLUMN.to_owned(), pe),
    ]
}

/// The names of the tables of `table`'s type tree (its root and every
/// subtype), when `table` is a typed table of a tree; `None` otherwise.
#[must_use]
pub fn type_tree(
    state: &TypesState,
    db: DbId,
    table: &str,
    lctn: bool,
) -> Option<(String, Vec<String>)> {
    let d = state.db(db)?;
    let t = d.get(table, lctn)?;
    let root = d.root_of(t.id)?;
    let mut names = vec![root.name.clone()];
    names.extend(d.descendants(root.id).iter().map(|x| x.name.clone()));
    Some((root.name.clone(), names))
}

/// Whether `table` has a record hierarchy (`ADD HIERARCHY`).
#[must_use]
pub fn has_hierarchy(state: &TypesState, db: DbId, table: &str, lctn: bool) -> bool {
    state
        .db(db)
        .is_some_and(|d| d.hierarchy(table, lctn).is_some())
}

/// Whether any single-table subtype has a `NOT NULL` check
/// ([`map_error`]): writes the type layer leaves alone are still sent
/// through the gateway's error mapping then.
#[must_use]
pub fn has_nn_checks(state: Option<&TypesState>) -> bool {
    state.is_some_and(|s| {
        s.dbs().any(|(_, d)| {
            d.iter().any(|t| {
                !t.is_root()
                    && t.storage == model::Storage::SingleTable
                    && t.own_fields.iter().any(ddl::needs_nn)
            })
        })
    })
}
