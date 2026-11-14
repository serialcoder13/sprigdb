//! Global lookup indexes on the DDL path (chapter 19, chapter 21's
//! `ADD UNIQUE` on a sharded table).
//!
//! A unique key on a sharded table that doesn't hold the distribution key
//! is unique only within each shard until a lookup index makes it unique
//! across them. The statements that add or drop such a key are decided
//! here, around the DDL engine's run on every shard:
//!
//! ```text
//! ALTER TABLE t ADD [CONSTRAINT [c]] UNIQUE [KEY|INDEX] [k] (col)
//! CREATE UNIQUE INDEX k ON t (col)
//!   1. the statement runs on every shard (each shard's unique index; a
//!      duplicate inside one shard fails here with 1062, as before)
//!   2. the lookup is built (lookups_sql::build_for_key): back-fill of
//!      the existing rows while writes go on, verification; a value that
//!      repeats across shards fails with 1062
//!   3. on failure the shards' index is dropped again, so the statement
//!      changes nothing
//! ALTER TABLE t DROP {INDEX|KEY} k, DROP INDEX k ON t
//!   the lookup named k is dropped first (writers stop claiming), then the
//!   statement runs on the shards
//! ```
//!
//! A key that holds the distribution key needs no lookup and is left to
//! MariaDB. A key of several columns without it is refused (39420
//! `lookup`): the router follows lookups of one column. `ADD UNIQUE` on
//! a sharded table must be the `ALTER TABLE`'s only kind of clause, so a
//! failed build can be undone exactly. A member that follows its parent
//! (no key column of its own) keeps per-shard uniqueness, as before.

use std::collections::BTreeSet;
use std::sync::Arc;

use sprig_common::ids::{ClusterId, ObjectId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_router::Topology as _;

use super::super::PassSession;
use super::fk_ddl::{Clause, parse_alter};
use super::layout::ShardingState;
use super::write_forms::{Tok, Token, is_word, scan};

/// One unique key a statement adds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AddKey {
    /// Its name, if given.
    pub name: Option<String>,
    /// Its columns; `None` when one isn't a plain column (a prefix, an
    /// expression): no lookup can hold it.
    pub columns: Option<Vec<String>>,
}

/// What a statement does to a table's indexes, as far as lookups care.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct IndexDdl {
    /// The table's schema, when qualified.
    pub schema: Option<String>,
    /// The table.
    pub table: String,
    /// The table as written.
    pub table_text: String,
    /// Unique keys added.
    pub adds: Vec<AddKey>,
    /// Indexes dropped, by name.
    pub drops: Vec<String>,
    /// Other clauses.
    pub other: bool,
}

fn ident(text: &str, t: &Token) -> Option<String> {
    match &t.tok {
        Tok::Ident(s) => Some(s.clone()),
        Tok::Word(_) => Some(text[t.start..t.end].to_owned()),
        _ => None,
    }
}

/// `(a, b)` from token `i`: the plain columns (`None` if any item is more
/// than a name) and the index after the parenthesis.
fn key_cols(text: &str, toks: &[Token], i: usize) -> Option<(Option<Vec<String>>, usize)> {
    if toks.get(i)?.tok != Tok::Punct(b'(') {
        return None;
    }
    let depth = toks[i].depth + 1;
    let mut cols = Some(Vec::new());
    let mut item: Vec<&Token> = Vec::new();
    let mut j = i + 1;
    let flush = |item: &mut Vec<&Token>, cols: &mut Option<Vec<String>>| {
        let plain = match item.as_slice() {
            [t] => ident(text, t),
            [t, d] if is_word(d, "ASC") || is_word(d, "DESC") => ident(text, t),
            _ => None,
        };
        match (plain, cols.as_mut()) {
            (Some(c), Some(v)) => v.push(c),
            _ => *cols = None,
        }
        item.clear();
    };
    while j < toks.len() {
        let t = &toks[j];
        if t.depth == depth - 1 && t.tok == Tok::Punct(b')') {
            flush(&mut item, &mut cols);
            return Some((cols, j + 1));
        }
        if t.depth == depth && t.tok == Tok::Punct(b',') {
            flush(&mut item, &mut cols);
        } else {
            item.push(t);
        }
        j += 1;
    }
    None
}

/// An `ALTER TABLE` clause as an added unique key or a dropped index.
fn clause(text: &str, out: &mut IndexDdl) {
    let toks = scan(text);
    let w = |k: usize, s: &str| toks.get(k).is_some_and(|t| is_word(t, s));
    if w(0, "DROP") && (w(1, "INDEX") || w(1, "KEY")) {
        let k = if w(2, "IF") && w(3, "EXISTS") { 4 } else { 2 };
        if let Some(n) = toks.get(k).and_then(|t| ident(text, t))
            && k + 1 == toks.len()
        {
            out.drops.push(n);
            return;
        }
    }
    if w(0, "ADD") {
        let mut i = 1;
        if w(i, "CONSTRAINT") {
            i += 1;
            if !w(i, "UNIQUE") {
                i += 1; // the constraint's name
            }
        }
        if w(i, "UNIQUE") {
            i += 1;
            if w(i, "INDEX") || w(i, "KEY") {
                i += 1;
            }
            if w(i, "IF") && w(i + 1, "NOT") && w(i + 2, "EXISTS") {
                i += 3;
            }
            let mut name = None;
            if toks.get(i).is_some_and(|t| t.tok != Tok::Punct(b'(')) {
                name = toks.get(i).and_then(|t| ident(text, t));
                i += 1;
            }
            if let Some((columns, _)) = key_cols(text, &toks, i) {
                out.adds.push(AddKey { name, columns });
                return;
            }
        }
    }
    out.other = true;
}

/// `text` as a statement adding unique keys or dropping indexes of one
/// table; `None` for any other statement.
pub(super) fn parse_index_ddl(text: &str) -> Option<IndexDdl> {
    if let Some(a) = parse_alter(text) {
        let mut out = IndexDdl {
            schema: a.schema,
            table: a.table,
            table_text: a.table_text,
            ..IndexDdl::default()
        };
        for c in &a.clauses {
            match c {
                Clause::Other(t) => clause(t, &mut out),
                _ => out.other = true,
            }
        }
        return (!out.adds.is_empty() || !out.drops.is_empty()).then_some(out);
    }
    let toks = scan(text);
    let w = |k: usize, s: &str| toks.get(k).is_some_and(|t| is_word(t, s));
    // `[db.]t` from token i: schema, table, text, next index.
    let name_at = |i: usize| -> Option<(Option<String>, String, String, usize)> {
        let first = ident(text, toks.get(i)?)?;
        if toks.get(i + 1).is_some_and(|t| t.tok == Tok::Punct(b'.')) {
            let second = ident(text, toks.get(i + 2)?)?;
            Some((
                Some(first),
                second,
                text[toks[i].start..toks[i + 2].end].to_owned(),
                i + 3,
            ))
        } else {
            Some((
                None,
                first,
                text[toks[i].start..toks[i].end].to_owned(),
                i + 1,
            ))
        }
    };
    if w(0, "CREATE") {
        let mut i = 1;
        if w(i, "OR") && w(i + 1, "REPLACE") {
            return None; // drops and re-creates: left to MariaDB
        }
        if w(i, "ONLINE") || w(i, "OFFLINE") {
            i += 1;
        }
        if !(w(i, "UNIQUE") && w(i + 1, "INDEX")) {
            return None;
        }
        i += 2;
        if w(i, "IF") && w(i + 1, "NOT") && w(i + 2, "EXISTS") {
            i += 3;
        }
        let name = ident(text, toks.get(i)?)?;
        if !w(i + 1, "ON") {
            return None;
        }
        let (schema, table, table_text, j) = name_at(i + 2)?;
        let (columns, _) = key_cols(text, &toks, j)?;
        return Some(IndexDdl {
            schema,
            table,
            table_text,
            adds: vec![AddKey {
                name: Some(name),
                columns,
            }],
            ..IndexDdl::default()
        });
    }
    if w(0, "DROP") {
        let mut i = 1;
        if w(i, "ONLINE") || w(i, "OFFLINE") {
            i += 1;
        }
        if !w(i, "INDEX") {
            return None;
        }
        i += 1;
        if w(i, "IF") && w(i + 1, "EXISTS") {
            i += 2;
        }
        let name = ident(text, toks.get(i)?)?;
        if !w(i + 1, "ON") {
            return None;
        }
        let (schema, table, table_text, _) = name_at(i + 2)?;
        return Some(IndexDdl {
            schema,
            table,
            table_text,
            drops: vec![name],
            ..IndexDdl::default()
        });
    }
    None
}

/// A lookup to build once the statement ran on the shards.
#[derive(Debug, Clone)]
pub(in crate::exec) struct LookupAdd {
    object: u64,
    table: String,
    name: Option<String>,
    column: String,
    /// The table's index names before the statement.
    before: BTreeSet<String>,
    /// The first shard's schema and cluster (where the key's name is read).
    shard: (String, String),
    /// The table as the physical and the logical statement write it (for
    /// the undo).
    physical_table: String,
    logical_table: String,
}

/// What the DDL path does about lookups (module docs).
pub(in crate::exec) enum LookupDdl {
    /// Nothing.
    Keep,
    /// Refused before anything changed.
    Fail(SprigError),
    /// Drop these lookups (`(table object, key name)`) before the statement.
    Drop(Vec<(u64, String)>),
    /// Build these lookups after the statement.
    Build(Vec<LookupAdd>),
}

/// A build that failed, with the statements that undo the shards' keys:
/// `(physical, logical)` `DROP INDEX` texts.
pub(in crate::exec) struct LookupUndo {
    /// Why.
    pub error: SprigError,
    /// The undo statements.
    pub undo: Vec<(String, String)>,
}

fn refused(why: &str) -> SprigError {
    SprigError::new(
        SprigCode::StatementNotDistributable,
        format!("statement not distributable (lookup): {why}"),
    )
}

fn q(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

impl PassSession {
    /// Decides what a DDL statement does about lookup indexes (module
    /// docs).
    pub(in crate::exec) async fn lookup_ddl_plan(
        &mut self,
        physical: &str,
        logical: &str,
    ) -> LookupDdl {
        let Some(ddl) = parse_index_ddl(physical) else {
            return LookupDdl::Keep;
        };
        let Some((layout, _)) = self.router_layout().await else {
            return LookupDdl::Keep;
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
        let Some(schema) = ddl
            .schema
            .clone()
            .or(default_schema)
            .map(|s| s.to_ascii_lowercase())
        else {
            return LookupDdl::Keep;
        };
        let table = ddl.table.to_ascii_lowercase();
        let Some(entry) = layout.topo.table(&schema, &table) else {
            return LookupDdl::Keep;
        };
        if entry.is_reference() {
            return LookupDdl::Keep;
        }
        let Some(&object) = layout.objects.get(&(schema.clone(), table.clone())) else {
            return LookupDdl::Keep;
        };
        let lookups: Vec<String> = self
            .tenant
            .as_deref()
            .and_then(|ts| ts.catalog())
            .and_then(|cat| {
                let s = cat.ext.get::<ShardingState>(sprig_shard::groups::MODULE)?;
                Some(
                    s.lookups
                        .iter()
                        .filter(|l| l.object == object)
                        .map(|l| l.index_name.clone())
                        .collect(),
                )
            })
            .unwrap_or_default();
        let drops: Vec<(u64, String)> = ddl
            .drops
            .iter()
            .filter_map(|d| {
                lookups
                    .iter()
                    .find(|l| l.eq_ignore_ascii_case(d))
                    .map(|l| (object, l.clone()))
            })
            .collect();
        let Some(key) = entry.key().map(str::to_owned) else {
            // A parent member: per-shard uniqueness, as before; its
            // lookups (none can be made) are still dropped with their key.
            return if drops.is_empty() {
                LookupDdl::Keep
            } else {
                LookupDdl::Drop(drops)
            };
        };
        let mut adds = Vec::new();
        for a in &ddl.adds {
            let Some(cols) = &a.columns else {
                continue; // a prefix or an expression: MariaDB's, per shard
            };
            let lc: Vec<String> = cols.iter().map(|c| c.to_ascii_lowercase()).collect();
            if lc.contains(&key) {
                continue; // unique across shards already
            }
            let [column] = lc.as_slice() else {
                return LookupDdl::Fail(refused(&format!(
                    "a unique key of several columns ({}) without the distribution key {key}: \
                     lookup indexes take one column",
                    lc.join(", ")
                )));
            };
            adds.push((a.name.clone(), column.clone()));
        }
        if adds.is_empty() {
            return if drops.is_empty() {
                LookupDdl::Keep
            } else {
                LookupDdl::Drop(drops)
            };
        }
        if ddl.other || !ddl.drops.is_empty() {
            return LookupDdl::Fail(refused(&format!(
                "ADD UNIQUE on sharded table {table} together with other changes; run it in an \
                 ALTER TABLE of its own"
            )));
        }
        let Some(first) = entry.group.shards.iter().min_by_key(|s| s.shard) else {
            return LookupDdl::Keep;
        };
        let shard = (first.schema.clone(), first.cluster.clone());
        let before = match self.index_names(&shard, &table).await {
            Ok(b) => b,
            Err(e) => return LookupDdl::Fail(e),
        };
        let logical_table =
            parse_index_ddl(logical).map_or_else(|| q(&ddl.table), |l| l.table_text);
        LookupDdl::Build(
            adds.into_iter()
                .map(|(name, column)| LookupAdd {
                    object,
                    table: table.clone(),
                    name,
                    column,
                    before: before.clone(),
                    shard: shard.clone(),
                    physical_table: ddl.table_text.clone(),
                    logical_table: logical_table.clone(),
                })
                .collect(),
        )
    }

    async fn index_names(&self, shard: &(String, String), table: &str) -> Result<BTreeSet<String>> {
        let svc = self
            .tenant
            .as_deref()
            .map(|ts| Arc::clone(ts.services()))
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let cluster = ClusterId::new(&shard.1).map_err(|e| SprigError::internal(e.to_string()))?;
        let pool = svc.locator.ctl_pool(&cluster).await?;
        let mut c = pool
            .get_conn()
            .await
            .map_err(|e| SprigError::new(SprigCode::BackendUnavailable, format!("MariaDB: {e}")))?;
        Ok(
            sprig_shard::lookup::admin::owner_shape(&mut c, &shard.0, table)
                .await?
                .indexes,
        )
    }

    /// Drops the lookups a `DROP INDEX` names, before the statement runs.
    pub(in crate::exec) async fn lookup_ddl_drop(&mut self, drops: &[(u64, String)]) -> Result<()> {
        let (svc, tenant) = self
            .tenant
            .as_deref()
            .map(|ts| (Arc::clone(ts.services()), ts.tenant))
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        for (object, name) in drops {
            crate::tenant::lookups_sql::drop_for_key(&svc, tenant, ObjectId(*object), name).await?;
        }
        Ok(())
    }

    /// Builds the lookups of the keys the statement added. On failure,
    /// the statements that drop the shards' new keys again.
    pub(in crate::exec) async fn lookup_ddl_build(
        &mut self,
        adds: &[LookupAdd],
    ) -> std::result::Result<(), LookupUndo> {
        let (svc, tenant) = match self.tenant.as_deref() {
            Some(ts) => (Arc::clone(ts.services()), ts.tenant),
            None => {
                return Err(LookupUndo {
                    error: SprigError::internal("no tenant"),
                    undo: Vec::new(),
                });
            }
        };
        let mut undo = Vec::new();
        for a in adds {
            // The key's name: as given, else the one MariaDB chose.
            let shape = async {
                let cluster =
                    ClusterId::new(&a.shard.1).map_err(|e| SprigError::internal(e.to_string()))?;
                let pool = svc.locator.ctl_pool(&cluster).await?;
                let mut c = pool.get_conn().await.map_err(|e| {
                    SprigError::new(SprigCode::BackendUnavailable, format!("MariaDB: {e}"))
                })?;
                sprig_shard::lookup::admin::owner_shape(&mut c, &a.shard.0, &a.table).await
            }
            .await;
            let shape = match shape {
                Ok(s) => s,
                Err(error) => return Err(LookupUndo { error, undo }),
            };
            let key = shape
                .unique
                .iter()
                .filter(|(_, cols)| cols.as_slice() == [a.column.clone()])
                .map(|(n, _)| n.clone())
                .find(|n| match &a.name {
                    Some(given) => n.eq_ignore_ascii_case(given),
                    None => !a.before.contains(&n.to_ascii_lowercase()),
                });
            let Some(key) = key else {
                // `ADD UNIQUE IF NOT EXISTS` of a key there already: the
                // statement changed nothing, and neither does the lookup.
                continue;
            };
            let added = !a.before.contains(&key.to_ascii_lowercase());
            if added {
                undo.push((
                    format!("DROP INDEX {} ON {}", q(&key), a.physical_table),
                    format!("DROP INDEX {} ON {}", q(&key), a.logical_table),
                ));
            }
            if let Err(error) = crate::tenant::lookups_sql::build_for_key(
                &svc,
                tenant,
                ObjectId(a.object),
                &a.table,
                &key,
                &a.column,
            )
            .await
            {
                // A lookup of that name already (1061): the key existed
                // and has one; nothing to undo beyond what was added.
                return Err(LookupUndo { error, undo });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_statements_are_read() {
        let a = parse_index_ddl("ALTER TABLE `shop`.customers ADD UNIQUE KEY uk_email (email)")
            .unwrap_or_else(|| unreachable!());
        assert_eq!(a.schema.as_deref(), Some("shop"));
        assert_eq!(a.table, "customers");
        assert_eq!(a.table_text, "`shop`.customers");
        assert_eq!(
            a.adds,
            vec![AddKey {
                name: Some("uk_email".into()),
                columns: Some(vec!["email".into()])
            }]
        );
        assert!(!a.other && a.drops.is_empty());
        let b = parse_index_ddl("ALTER TABLE c ADD CONSTRAINT x UNIQUE (a, `b` DESC)")
            .unwrap_or_else(|| unreachable!());
        assert_eq!(b.adds[0].name, None);
        assert_eq!(b.adds[0].columns, Some(vec!["a".into(), "b".into()]));
        let p = parse_index_ddl("ALTER TABLE c ADD UNIQUE u (email(10))")
            .unwrap_or_else(|| unreachable!());
        assert_eq!(p.adds[0].columns, None);
        let m = parse_index_ddl("ALTER TABLE c ADD COLUMN z INT, ADD UNIQUE (z)")
            .unwrap_or_else(|| unreachable!());
        assert!(m.other && m.adds.len() == 1);
        let d = parse_index_ddl("ALTER TABLE c DROP INDEX IF EXISTS uk, DROP KEY `k2`")
            .unwrap_or_else(|| unreachable!());
        assert_eq!(d.drops, vec!["uk".to_owned(), "k2".to_owned()]);
        let ci = parse_index_ddl("CREATE UNIQUE INDEX uk ON d.t (email) USING BTREE")
            .unwrap_or_else(|| unreachable!());
        assert_eq!(
            (
                ci.schema.as_deref(),
                ci.table.as_str(),
                ci.table_text.as_str()
            ),
            (Some("d"), "t", "d.t")
        );
        assert_eq!(ci.adds[0].columns, Some(vec!["email".into()]));
        let di = parse_index_ddl("DROP INDEX uk ON t").unwrap_or_else(|| unreachable!());
        assert_eq!(di.drops, vec!["uk".to_owned()]);
        for other in [
            "ALTER TABLE c ADD COLUMN z INT",
            "ALTER TABLE c ADD INDEX i (a)",
            "CREATE INDEX i ON t (a)",
            "CREATE TABLE t (a INT UNIQUE)",
            "DROP TABLE t",
        ] {
            assert_eq!(parse_index_ddl(other), None, "{other}");
        }
    }
}
