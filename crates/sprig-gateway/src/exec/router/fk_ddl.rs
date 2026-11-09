//! Chapter 19's foreign keys on the DDL path (R3, decision Q9's leftover).
//!
//! An `ALTER TABLE child ADD [CONSTRAINT n] FOREIGN KEY (cols) REFERENCES
//! parent (cols) …` whose two tables the router distributes is decided
//! here, before chapter 21's DDL engine runs the statement on the shards:
//!
//! ```text
//! both ends on one shard (one group and database, and the child follows
//! the parent through these columns: a parent member through them, or
//! both keyed by them)                         → native: MariaDB keeps it
//!                                               on every shard; a row records it
//! the parent is a reference table             → reference \  the gateway keeps it:
//! otherwise                                   → gateway   /  1. existing rows are
//!     checked (a gathered read; an orphan → 1452, nothing changes)
//!   2. the clause leaves the shards' statement (an index on the child's
//!      columns takes its place, as MariaDB would add one); an ALTER with
//!      nothing left doesn't reach MariaDB
//!   3. a `foreign_keys` row (state `enforced`, `child_lookup_id` when a
//!      lookup index has exactly the child's columns) in one versioned
//!      catalog change, so every gateway checks it from the next statement
//! DROP FOREIGN KEY n of a key the gateway keeps → its row goes, the
//!   clause leaves the shards' statement
//! ```
//!
//! A key with an end the router doesn't distribute is MariaDB's, as before.

#![allow(clippy::many_single_char_names)] // a scanner's indexes and tokens

use std::fmt::Write as _;
use std::sync::Arc;

use sprig_catalog::VersionKind;
use sprig_common::{Result, SprigError};
use sprig_router::Topology as _;
use sprig_router::topology::TableEntry;
use sprig_shard::fk::{FkAction, FkEnforcement};
use sprig_sql::names::{NameMap, ResultMapper};

use super::super::PassSession;
use super::layout::ShardingState;
use super::write_forms::{Tok, Token, is_word, scan};
use crate::pipeline::{ExecCtx, Wire};

/// One `ADD … FOREIGN KEY` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AddFk {
    /// `CONSTRAINT name`, if given.
    pub name: Option<String>,
    /// The index name after `FOREIGN KEY`, if given.
    pub index: Option<String>,
    /// The child's columns.
    pub columns: Vec<String>,
    /// The parent as written (schema-qualified or not).
    pub parent_text: String,
    /// Its schema, when qualified.
    pub parent_schema: Option<String>,
    /// Its name.
    pub parent: String,
    /// The referenced columns.
    pub parent_columns: Vec<String>,
    /// `ON DELETE`.
    pub on_delete: FkAction,
    /// `ON UPDATE`.
    pub on_update: FkAction,
}

/// One clause of an `ALTER TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Clause {
    /// `ADD … FOREIGN KEY …`.
    AddFk(AddFk),
    /// `DROP FOREIGN KEY [IF EXISTS] name`.
    DropFk(String),
    /// Anything else, as written.
    Other(String),
}

/// An `ALTER TABLE` statement cut into its clauses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Alter {
    /// `ALTER [ONLINE] [IGNORE] TABLE [IF EXISTS] t`.
    pub head: String,
    /// The table's schema, when qualified.
    pub schema: Option<String>,
    /// The table.
    pub table: String,
    /// The table as written.
    pub table_text: String,
    /// The clauses, in order.
    pub clauses: Vec<Clause>,
}

fn ident(text: &str, t: &Token) -> Option<String> {
    match &t.tok {
        Tok::Ident(s) => Some(s.clone()),
        Tok::Word(_) => Some(text[t.start..t.end].to_owned()),
        _ => None,
    }
}

/// `(a, b)` from token `i` (the parenthesis): the names and the index after
/// the closing one.
fn col_list(text: &str, toks: &[Token], i: usize) -> Option<(Vec<String>, usize)> {
    if toks.get(i)?.tok != Tok::Punct(b'(') {
        return None;
    }
    let mut cols = Vec::new();
    let mut j = i + 1;
    while j < toks.len() && toks[j].tok != Tok::Punct(b')') {
        match &toks[j].tok {
            Tok::Punct(b',') => {}
            _ => cols.push(ident(text, &toks[j])?),
        }
        j += 1;
    }
    Some((cols, j + 1))
}

fn action(toks: &[Token], i: usize) -> Option<(FkAction, usize)> {
    let w = |k: usize, s: &str| toks.get(k).is_some_and(|t| is_word(t, s));
    if w(i, "CASCADE") {
        Some((FkAction::Cascade, i + 1))
    } else if w(i, "SET") && w(i + 1, "NULL") {
        Some((FkAction::SetNull, i + 2))
    } else if w(i, "NO") && w(i + 1, "ACTION") {
        Some((FkAction::NoAction, i + 2))
    } else if w(i, "RESTRICT") {
        Some((FkAction::Restrict, i + 1))
    } else {
        None
    }
}

/// One clause's tokens as an `ADD … FOREIGN KEY` or `DROP FOREIGN KEY`.
fn clause(text: &str, toks: &[Token]) -> Clause {
    let other = || {
        Clause::Other(match (toks.first(), toks.last()) {
            (Some(a), Some(b)) => text[a.start..b.end].to_owned(),
            _ => String::new(),
        })
    };
    let w = |k: usize, s: &str| toks.get(k).is_some_and(|t| is_word(t, s));
    if w(0, "DROP") && w(1, "FOREIGN") && w(2, "KEY") {
        let k = if w(3, "IF") && w(4, "EXISTS") { 5 } else { 3 };
        return match toks.get(k).and_then(|t| ident(text, t)) {
            Some(n) if k + 1 == toks.len() => Clause::DropFk(n),
            _ => other(),
        };
    }
    if !w(0, "ADD") {
        return other();
    }
    let mut i = 1;
    let mut name = None;
    if w(i, "CONSTRAINT") {
        i += 1;
        if !w(i, "FOREIGN") {
            name = toks.get(i).and_then(|t| ident(text, t));
            i += 1;
        }
    }
    if !(w(i, "FOREIGN") && w(i + 1, "KEY")) {
        return other();
    }
    i += 2;
    if w(i, "IF") && w(i + 1, "NOT") && w(i + 2, "EXISTS") {
        return other(); // MariaDB's own form: left to it
    }
    let mut index = None;
    if toks.get(i).is_some_and(|t| t.tok != Tok::Punct(b'(')) {
        index = toks.get(i).and_then(|t| ident(text, t));
        i += 1;
    }
    let Some((columns, j)) = col_list(text, toks, i) else {
        return other();
    };
    i = j;
    if !w(i, "REFERENCES") {
        return other();
    }
    i += 1;
    let Some(first) = toks.get(i).and_then(|t| ident(text, t)) else {
        return other();
    };
    let pstart = toks[i].start;
    let mut pend = toks[i].end;
    let (parent_schema, parent) = if toks.get(i + 1).is_some_and(|t| t.tok == Tok::Punct(b'.')) {
        let Some(second) = toks.get(i + 2).and_then(|t| ident(text, t)) else {
            return other();
        };
        pend = toks[i + 2].end;
        i += 3;
        (Some(first), second)
    } else {
        i += 1;
        (None, first)
    };
    let Some((parent_columns, j)) = col_list(text, toks, i) else {
        return other();
    };
    i = j;
    let (mut on_delete, mut on_update) = (FkAction::Restrict, FkAction::Restrict);
    while i < toks.len() {
        if w(i, "ON") && w(i + 1, "DELETE") {
            let Some((a, k)) = action(toks, i + 2) else {
                return other();
            };
            on_delete = a;
            i = k;
        } else if w(i, "ON") && w(i + 1, "UPDATE") {
            let Some((a, k)) = action(toks, i + 2) else {
                return other();
            };
            on_update = a;
            i = k;
        } else {
            return other(); // MATCH …, or something this reader doesn't know
        }
    }
    if columns.is_empty() || columns.len() != parent_columns.len() {
        return other();
    }
    Clause::AddFk(AddFk {
        name,
        index,
        columns,
        parent_text: text[pstart..pend].to_owned(),
        parent_schema,
        parent,
        parent_columns,
        on_delete,
        on_update,
    })
}

/// `text` as an `ALTER TABLE` with its clauses; `None` for any other
/// statement.
pub(super) fn parse_alter(text: &str) -> Option<Alter> {
    let toks = scan(text);
    let w = |k: usize, s: &str| toks.get(k).is_some_and(|t| is_word(t, s));
    if !w(0, "ALTER") {
        return None;
    }
    let mut i = 1;
    while w(i, "ONLINE") || w(i, "IGNORE") {
        i += 1;
    }
    if !w(i, "TABLE") {
        return None;
    }
    i += 1;
    if w(i, "IF") && w(i + 1, "EXISTS") {
        i += 2;
    }
    let first = ident(text, toks.get(i)?)?;
    let tstart = toks[i].start;
    let mut tend = toks[i].end;
    let (schema, table) = if toks.get(i + 1).is_some_and(|t| t.tok == Tok::Punct(b'.')) {
        let second = ident(text, toks.get(i + 2)?)?;
        tend = toks[i + 2].end;
        i += 3;
        (Some(first), second)
    } else {
        i += 1;
        (None, first)
    };
    let mut clauses = Vec::new();
    let mut cur: Vec<Token> = Vec::new();
    for t in &toks[i..] {
        if t.depth == 0 && matches!(t.tok, Tok::Punct(b',' | b';')) {
            if !cur.is_empty() {
                clauses.push(clause(text, &cur));
            }
            cur.clear();
            continue;
        }
        cur.push(t.clone());
    }
    if !cur.is_empty() {
        clauses.push(clause(text, &cur));
    }
    Some(Alter {
        head: text[..tend].to_owned(),
        schema,
        table,
        table_text: text[tstart..tend].to_owned(),
        clauses,
    })
}

fn q(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Who keeps `fk` from `child` to `parent` (module docs).
pub(super) fn enforcement_of(child: &TableEntry, parent: &TableEntry, fk: &AddFk) -> FkEnforcement {
    if parent.is_reference() {
        return FkEnforcement::Reference;
    }
    let lc = |v: &[String]| -> Vec<String> { v.iter().map(|c| c.to_ascii_lowercase()).collect() };
    let (cols, pcols) = (lc(&fk.columns), lc(&fk.parent_columns));
    let same = child.group.same_layout(&parent.group) && !child.is_reference();
    let through_parent = child
        .parent
        .as_ref()
        .is_some_and(|l| l.table == parent.name && l.columns == cols && l.parent_columns == pcols);
    let both_keyed = cols.len() == 1
        && child.key() == Some(cols[0].as_str())
        && parent.key() == Some(pcols[0].as_str());
    if same && (through_parent || both_keyed) {
        FkEnforcement::Native
    } else {
        FkEnforcement::Gateway
    }
}

/// A `foreign_keys` row the DDL path adds.
#[derive(Debug, Clone)]
pub(in crate::exec) struct NewFk {
    name: String,
    child: u64,
    columns: Vec<String>,
    parent: u64,
    parent_columns: Vec<String>,
    on_delete: FkAction,
    on_update: FkAction,
    enforcement: FkEnforcement,
    child_lookup: Option<u64>,
}

/// What the DDL path changes in `foreign_keys` after the statement.
#[derive(Debug, Clone, Default)]
pub(in crate::exec) struct FkChange {
    add: Vec<NewFk>,
    /// `(child object, constraint name)`.
    drop: Vec<(u64, String)>,
}

impl FkChange {
    fn is_empty(&self) -> bool {
        self.add.is_empty() && self.drop.is_empty()
    }
}

/// What the DDL path does with a statement (module docs).
pub(in crate::exec) enum FkDdl {
    /// Not a foreign key the gateway decides: run the statement as it is.
    Keep,
    /// Run `sql` on the shards instead, then record `change`.
    Run(String, FkChange),
    /// Nothing reaches MariaDB; record `change` and answer OK.
    Skip(FkChange),
    /// Refused before anything changed.
    Fail(SprigError),
}

fn cannot_add(child: &str, name: &str, fk: &AddFk) -> SprigError {
    let cols = |v: &[String]| v.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
    SprigError::standard(
        1452,
        *b"23000",
        format!(
            "Cannot add or update a child row: a foreign key constraint fails ({}, CONSTRAINT {} \
             FOREIGN KEY ({}) REFERENCES {} ({}))",
            q(child),
            q(name),
            cols(&fk.columns),
            q(&fk.parent),
            cols(&fk.parent_columns)
        ),
    )
}

impl PassSession {
    /// Decides an `ALTER TABLE`'s foreign-key clauses (module docs).
    #[allow(clippy::too_many_lines)] // the clauses, one kind after the other
    pub(in crate::exec) async fn fk_ddl_plan(
        &mut self,
        cx: &mut ExecCtx<'_>,
        physical: &str,
        mapper: &ResultMapper,
        map: &dyn NameMap,
        wire: Wire,
    ) -> FkDdl {
        let Some(alter) = parse_alter(physical) else {
            return FkDdl::Keep;
        };
        if !alter
            .clauses
            .iter()
            .any(|c| matches!(c, Clause::AddFk(_) | Clause::DropFk(_)))
        {
            return FkDdl::Keep;
        }
        let Some((layout, _)) = self.router_layout().await else {
            return FkDdl::Keep;
        };
        let default_schema = self
            .bs
            .as_ref()
            .and_then(|bs| bs.vars().schema.clone())
            .or_else(|| {
                self.tenant
                    .as_deref()
                    .and_then(|ts| ts.schema_of(ts.current.as_deref()))
            });
        let schema_of = |s: &Option<String>| -> Option<String> {
            s.clone()
                .or_else(|| default_schema.clone())
                .map(|x| x.to_ascii_lowercase())
        };
        let Some(cschema) = schema_of(&alter.schema) else {
            return FkDdl::Keep;
        };
        let ctable = alter.table.to_ascii_lowercase();
        let Some(child) = layout.topo.table(&cschema, &ctable) else {
            return FkDdl::Keep;
        };
        let child = child.clone();
        let Some(&child_obj) = layout.objects.get(&(cschema.clone(), ctable.clone())) else {
            return FkDdl::Keep;
        };
        // The child's keys and lookups as the catalog snapshot has them.
        let (existing, lookups): (Vec<sprig_shard::fk::ForeignKey>, Vec<(u64, Vec<String>)>) = self
            .tenant
            .as_deref()
            .and_then(|ts| ts.catalog())
            .and_then(|cat| {
                let s = cat.ext.get::<ShardingState>(sprig_shard::groups::MODULE)?;
                Some((
                    s.fks
                        .iter()
                        .filter(|f| f.child == child_obj)
                        .cloned()
                        .collect(),
                    s.lookups
                        .iter()
                        .filter(|l| l.object == child_obj)
                        .map(|l| (l.id, l.columns.clone()))
                        .collect(),
                ))
            })
            .unwrap_or_default();
        let mut change = FkChange::default();
        let mut kept: Vec<String> = Vec::new();
        let mut n_auto = existing.len();
        for c in &alter.clauses {
            match c {
                Clause::Other(t) => kept.push(t.clone()),
                Clause::DropFk(name) => {
                    // A key the gateway keeps: its row goes, MariaDB has
                    // nothing to drop. Else MariaDB drops it (a native
                    // key's row goes too).
                    if let Some(f) = existing.iter().find(|f| f.name.eq_ignore_ascii_case(name)) {
                        change.drop.push((child_obj, f.name.clone()));
                        if f.enforcement == FkEnforcement::Native {
                            kept.push(format!("DROP FOREIGN KEY {}", q(name)));
                        }
                    } else {
                        kept.push(format!("DROP FOREIGN KEY {}", q(name)));
                    }
                }
                Clause::AddFk(fk) => {
                    let Some(pschema) = schema_of(&fk.parent_schema) else {
                        return FkDdl::Keep;
                    };
                    let ptable = fk.parent.to_ascii_lowercase();
                    let (Some(parent), Some(&parent_obj)) = (
                        layout.topo.table(&pschema, &ptable),
                        layout.objects.get(&(pschema.clone(), ptable.clone())),
                    ) else {
                        // An end the router doesn't distribute: MariaDB's.
                        return FkDdl::Keep;
                    };
                    let enf = enforcement_of(&child, parent, fk);
                    let name = fk.name.clone().unwrap_or_else(|| {
                        n_auto += 1;
                        format!("{}_ibfk_{n_auto}", alter.table)
                    });
                    if existing.iter().any(|f| f.name.eq_ignore_ascii_case(&name)) {
                        return FkDdl::Fail(SprigError::standard(
                            1826,
                            *b"HY000",
                            format!("Duplicate FOREIGN KEY constraint name '{name}'"),
                        ));
                    }
                    let lc = |v: &[String]| -> Vec<String> {
                        v.iter().map(|c| c.to_ascii_lowercase()).collect()
                    };
                    let child_lookup = lookups
                        .iter()
                        .find(|(_, cols)| lc(cols) == lc(&fk.columns))
                        .map(|(id, _)| *id);
                    change.add.push(NewFk {
                        name: name.clone(),
                        child: child_obj,
                        columns: lc(&fk.columns),
                        parent: parent_obj,
                        parent_columns: lc(&fk.parent_columns),
                        on_delete: fk.on_delete,
                        on_update: fk.on_update,
                        enforcement: enf,
                        child_lookup,
                    });
                    if enf == FkEnforcement::Native {
                        // MariaDB keeps it on every shard, as written.
                        let mut t = format!("ADD CONSTRAINT {} FOREIGN KEY ", q(&name));
                        if let Some(i) = &fk.index {
                            t.push_str(&q(i));
                            t.push(' ');
                        }
                        let _ = write!(
                            t,
                            "({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {}",
                            fk.columns
                                .iter()
                                .map(|c| q(c))
                                .collect::<Vec<_>>()
                                .join(", "),
                            fk.parent_text,
                            fk.parent_columns
                                .iter()
                                .map(|c| q(c))
                                .collect::<Vec<_>>()
                                .join(", "),
                            fk.on_delete.sql(),
                            fk.on_update.sql()
                        );
                        kept.push(t);
                        continue;
                    }
                    // Existing rows must have their parents (1452).
                    let mut conds: Vec<String> = fk
                        .columns
                        .iter()
                        .map(|c| format!("c.{} IS NOT NULL", q(c)))
                        .collect();
                    let join: Vec<String> = fk
                        .columns
                        .iter()
                        .zip(&fk.parent_columns)
                        .map(|(c, p)| format!("p.{} = c.{}", q(p), q(c)))
                        .collect();
                    conds.push(format!(
                        "NOT EXISTS (SELECT 1 FROM {} p WHERE {})",
                        fk.parent_text,
                        join.join(" AND ")
                    ));
                    let check = format!(
                        "SELECT 1 FROM {} c WHERE {} LIMIT 1",
                        alter.table_text,
                        conds.join(" AND ")
                    );
                    match self.capture(cx, &check, mapper, map, wire).await {
                        Ok(cap) if cap.error.is_none() => {
                            if !cap.rows.is_empty() {
                                return FkDdl::Fail(cannot_add(&alter.table, &name, fk));
                            }
                        }
                        Ok(cap) => {
                            return FkDdl::Fail(cap.error.unwrap_or_else(|| {
                                SprigError::internal("the foreign key's check failed")
                            }));
                        }
                        Err(e) => return FkDdl::Fail(e),
                    }
                    // The index MariaDB would add for the key, unless the
                    // primary key starts with its columns.
                    let lcols = lc(&fk.columns);
                    if !(child.primary.len() >= lcols.len()
                        && child.primary[..lcols.len()] == lcols[..])
                    {
                        kept.push(format!(
                            "ADD INDEX IF NOT EXISTS {} ({})",
                            q(fk.index.as_deref().unwrap_or(&name)),
                            fk.columns
                                .iter()
                                .map(|c| q(c))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ));
                    }
                }
            }
        }
        if change.is_empty() {
            return FkDdl::Keep;
        }
        if kept.is_empty() {
            FkDdl::Skip(change)
        } else {
            FkDdl::Run(format!("{} {}", alter.head, kept.join(", ")), change)
        }
    }

    /// Records `change` in the tenant's `foreign_keys` rows, in one
    /// versioned catalog change (every gateway reloads its key graph).
    pub(in crate::exec) async fn fk_ddl_commit(&mut self, change: &FkChange) -> Result<()> {
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let mut c = svc.catalog.change(tenant, VersionKind::Catalog).await?;
        let applied = async {
            use mysql_async::prelude::Queryable as _;
            let (store, tx) = c.tx().parts();
            let fks = format!("{}.foreign_keys", q(store.schema()));
            let db = |e: mysql_async::Error| {
                SprigError::new(sprig_common::SprigCode::CatalogUnavailable, e.to_string())
            };
            for (child, name) in &change.drop {
                tx.exec_drop(
                    format!("DELETE FROM {fks} WHERE child_object_id = ? AND name = ?"),
                    (*child, name.as_str()),
                )
                .await
                .map_err(db)?;
            }
            for f in &change.add {
                let next: Option<u64> = tx
                    .query_first(format!("SELECT COALESCE(MAX(fk_id), 0) + 1 FROM {fks}"))
                    .await
                    .map_err(db)?;
                tx.exec_drop(
                    format!(
                        "INSERT INTO {fks} (fk_id, name, child_object_id, child_columns, \
                         parent_object_id, parent_columns, on_delete, on_update, enforcement, \
                         child_lookup_id, state, updated_at) \
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'enforced', UTC_TIMESTAMP(6))"
                    ),
                    (
                        next.unwrap_or(1),
                        f.name.as_str(),
                        f.child,
                        serde_json::to_string(&f.columns).unwrap_or_default(),
                        f.parent,
                        serde_json::to_string(&f.parent_columns).unwrap_or_default(),
                        f.on_delete.as_str(),
                        f.on_update.as_str(),
                        f.enforcement.as_str(),
                        f.child_lookup,
                    ),
                )
                .await
                .map_err(db)?;
            }
            Ok::<(), SprigError>(())
        }
        .await;
        if let Err(e) = applied {
            let _ = c.rollback().await;
            return Err(e);
        }
        let v = c.commit().await?;
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.changed(Some(v));
            ts.refresh().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alter_clauses_are_read() {
        let a = parse_alter(
            "ALTER TABLE `s`.parts ADD COLUMN x INT, ADD CONSTRAINT fk_s FOREIGN KEY (sid) \
             REFERENCES suppliers (sid) ON DELETE SET NULL ON UPDATE CASCADE, DROP FOREIGN KEY old",
        )
        .unwrap_or_else(|| unreachable!());
        assert_eq!(a.head, "ALTER TABLE `s`.parts");
        assert_eq!(a.schema.as_deref(), Some("s"));
        assert_eq!(a.table, "parts");
        assert_eq!(a.clauses.len(), 3);
        assert_eq!(a.clauses[0], Clause::Other("ADD COLUMN x INT".into()));
        let Clause::AddFk(f) = &a.clauses[1] else {
            unreachable!()
        };
        assert_eq!(f.name.as_deref(), Some("fk_s"));
        assert_eq!(f.columns, vec!["sid".to_owned()]);
        assert_eq!(f.parent, "suppliers");
        assert_eq!(f.on_delete, FkAction::SetNull);
        assert_eq!(f.on_update, FkAction::Cascade);
        assert_eq!(a.clauses[2], Clause::DropFk("old".into()));
        let b = parse_alter("ALTER TABLE c ADD FOREIGN KEY ix (a, b) REFERENCES `d`.`p` (x, y)")
            .unwrap_or_else(|| unreachable!());
        let Clause::AddFk(f) = &b.clauses[0] else {
            unreachable!()
        };
        assert_eq!(f.name, None);
        assert_eq!(f.index.as_deref(), Some("ix"));
        assert_eq!(f.parent_schema.as_deref(), Some("d"));
        assert_eq!(f.parent_text, "`d`.`p`");
        assert_eq!(
            (f.on_delete, f.on_update),
            (FkAction::Restrict, FkAction::Restrict)
        );
        // MATCH and other forms are left to MariaDB.
        let m = parse_alter("ALTER TABLE c ADD FOREIGN KEY (a) REFERENCES p (x) MATCH FULL")
            .unwrap_or_else(|| unreachable!());
        assert!(matches!(m.clauses[0], Clause::Other(_)));
        assert!(parse_alter("CREATE TABLE t (a INT)").is_none());
    }
}
