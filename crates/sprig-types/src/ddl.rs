//! Type DDL (chapter 32, "Joined storage", "State machines"; build step 3):
//! `CREATE TABLE ... INHERITS`, `ALTER TABLE` and `DROP TABLE` on typed
//! tables, and `DROP TABLE` of a table with a hierarchy.
//!
//! The physical statements run through the executor's DDL path (chapter
//! 08's engine, chapter 21's when the tenant has shards), one at a time;
//! the `types` rows follow in one versioned catalog change. A table becomes
//! a root when its first subtype is created: it gains the invisible
//! `_type` and `_type_path` columns, whose defaults describe the root, and
//! the index `_sprig_type_path`. Each subtype's table repeats the root's
//! key, with a foreign key to its parent's table that never cascades.

use std::collections::BTreeSet;
use std::sync::Arc;

use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use crate::Ctx;
use crate::exec::{Done, Runner, unsupported};
use crate::grammar::{AlterTable, CreateTable, DropTable};
use crate::model::{AUTO_INCREMENT, DbTypes, FieldDef, Storage, TypeDef, TypeId, TypeState, upper};
use crate::rewrite::{TYPE_COLUMN, TYPE_PATH_COLUMN, quote_str};
use crate::store::TypesChange;
use crate::tabledef::{Element, TableDef};

/// The index on the root's path column.
pub const TYPE_PATH_INDEX: &str = "_sprig_type_path";
/// The prefix of a subtype's foreign key to its parent.
pub const FK_PREFIX: &str = "_sprig_fk_";
/// The longest type path.
pub const MAX_TYPE_PATH: usize = 255;

fn table_sql(db: Option<&str>, t: &str) -> String {
    match db {
        Some(d) => format!("{}.{}", quote(d), quote(t)),
        None => quote(t),
    }
}

/// The foreign key name of subtype `table`: `_sprig_fk_<table>`, shortened
/// with a hash past 64 characters.
#[must_use]
pub fn fk_name(table: &str) -> String {
    let full = format!("{FK_PREFIX}{table}");
    if full.chars().count() <= 64 {
        return full;
    }
    let h = table.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    let head: String = table.chars().take(45).collect();
    format!("{FK_PREFIX}{head}_{h:08x}")
        .chars()
        .take(64)
        .collect()
}

fn is_type_column(c: &str) -> bool {
    c.eq_ignore_ascii_case(TYPE_COLUMN) || c.eq_ignore_ascii_case(TYPE_PATH_COLUMN)
}

/// The fields a `SHOW CREATE TABLE` (or `CREATE TABLE`) text declares,
/// without the type columns and, unless `keep_keys`, the key columns.
fn fields_of(def: &TableDef, table: &str, key: &[String], keep_keys: bool) -> Vec<FieldDef> {
    def.columns()
        .filter(|c| !is_type_column(&c.name))
        .filter(|c| keep_keys || !key.iter().any(|k| k.eq_ignore_ascii_case(&c.name)))
        .enumerate()
        .map(|(i, c)| FieldDef {
            name: c.name.clone(),
            ordinal: u16::try_from(i + 1).unwrap_or(u16::MAX),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
            default_expr: if c.auto_increment {
                Some(AUTO_INCREMENT.to_owned())
            } else {
                c.default.clone()
            },
            physical_table: table.to_owned(),
        })
        .collect()
}

async fn show_create(
    db: Option<&str>,
    table: &str,
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
) -> Result<TableDef> {
    let rows = r
        .query(&format!("SHOW CREATE TABLE {}", table_sql(db, table)))
        .await?;
    let text = rows
        .rows
        .first()
        .and_then(|row| row.get(1).cloned().flatten())
        .map(|v| String::from_utf8_lossy(&v).into_owned())
        .unwrap_or_default();
    TableDef::parse(&text, &ctx.lex)
        .ok_or_else(|| SprigError::internal(format!("SHOW CREATE TABLE {table} didn't parse")))
}

/// Makes `table` a root: the type columns and their index. Idempotent.
async fn add_type_columns(
    db: Option<&str>,
    table: &str,
    key: &[String],
    r: &mut dyn Runner,
) -> Result<()> {
    let keys: Vec<String> = key.iter().map(|k| quote(k)).collect();
    r.exec(&format!(
        "ALTER TABLE {t} \
         ADD COLUMN IF NOT EXISTS {tc} VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin \
         NOT NULL DEFAULT {name} INVISIBLE, \
         ADD COLUMN IF NOT EXISTS {pc} VARCHAR({MAX_TYPE_PATH}) CHARACTER SET utf8mb4 COLLATE \
         utf8mb4_bin NOT NULL DEFAULT {path} INVISIBLE, \
         ADD INDEX IF NOT EXISTS {ix} ({pc}, {keys})",
        t = table_sql(db, table),
        tc = quote(TYPE_COLUMN),
        pc = quote(TYPE_PATH_COLUMN),
        ix = quote(TYPE_PATH_INDEX),
        name = quote_str(table, false),
        path = quote_str(&format!("/{table}/"), false),
        keys = keys.join(", ")
    ))
    .await?;
    Ok(())
}

/// `CREATE TABLE ... INHERITS (p)` and `CREATE TABLE ... WITH
/// (SPRIG_ABSTRACT = 1)`.
///
/// # Errors
/// 39701, 39702, 39708, 39710, or a step's error.
#[allow(clippy::too_many_lines)] // the chapter's steps in order
pub async fn create(ct: &CreateTable, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<Done> {
    if ct.temporary {
        return Err(unsupported("a temporary table can't be typed"));
    }
    if ct.like_or_select {
        return Err(unsupported(
            "CREATE TABLE ... LIKE or ... SELECT with INHERITS",
        ));
    }
    match ct.storage.as_deref() {
        None | Some("joined") => {}
        Some("single_table") => {
            return Err(unsupported(
                "SPRIG_STORAGE = 'single_table' is not supported yet",
            ));
        }
        Some(other) => return Err(unsupported(format!("SPRIG_STORAGE = '{other}'"))),
    }
    let db = ct.name.db.as_deref();
    let child = ct.name.name.clone();
    if child.chars().count() > 64 || is_type_column(&child) {
        return Err(unsupported(format!("the type name '{child}'")));
    }
    if r.object_id(db, &child).await?.is_some() {
        if ct.if_not_exists {
            return Ok(Done {
                warnings: 1,
                ..Done::default()
            });
        }
        return Err(SprigError::standard(
            1050,
            *b"42S01",
            format!("Table '{child}' already exists"),
        ));
    }
    let types = ctx.lookup.db(db).unwrap_or_default();
    let lctn = ctx.lookup.lctn();
    let def = TableDef::parse(&ct.plain, &ctx.lex)
        .ok_or_else(|| unsupported("a typed table needs a column list"))?;
    let declares_key = def
        .elements
        .iter()
        .any(|(e, _)| matches!(e, Element::PrimaryKey(_)))
        || def.columns().any(|c| c.primary);
    if declares_key && !ct.inherits.is_empty() {
        return Err(unsupported(
            "a subtype inherits its parent's key: drop its PRIMARY KEY",
        ));
    }
    for c in def.columns() {
        if is_type_column(&c.name) {
            return Err(SprigError::new(
                SprigCode::TypeFieldConflict,
                format!("Column '{}' is reserved for the type of a row", c.name),
            ));
        }
    }
    // A root declared on its own: `WITH (SPRIG_ABSTRACT = 1)`.
    let Some(parent_name) = ct.inherits.first() else {
        r.exec(&ct.plain).await?;
        let pdef = show_create(db, &child, ctx, r).await?;
        let key = pdef.primary_key();
        if key.is_empty() {
            return Err(unsupported("a root type needs a primary key"));
        }
        add_type_columns(db, &child, &key, r).await?;
        let (db_id, id) = r
            .object_id(db, &child)
            .await?
            .ok_or_else(|| SprigError::internal("the new table vanished"))?;
        let root = TypeDef {
            id,
            db: db_id,
            name: child.clone(),
            parent: None,
            root: id,
            depth: 0,
            path: format!("/{child}/"),
            is_abstract: ct.is_abstract,
            storage: Storage::Joined,
            key_fields: key.clone(),
            own_fields: fields_of(&pdef, &child, &key, true),
            state: TypeState::Active,
            version: 1,
        };
        r.commit_types(&TypesChange {
            fields: vec![(id, root.own_fields.clone())],
            upsert_types: vec![root],
            ..TypesChange::default()
        })
        .await?;
        return Ok(Done::default());
    };
    if parent_name.db.as_deref() != db && !(parent_name.db.is_none() || db.is_none()) {
        return Err(unsupported("a subtype in another database than its parent"));
    }
    let pdb = parent_name.db.as_deref().or(db);
    let parent_typed = types.get(&parent_name.name, lctn).cloned();
    // The parent's place: an existing type, or a table becoming a root.
    let mut change = TypesChange::default();
    let (root_fields, root_key, parent_path, parent_depth, inherited): (
        Vec<FieldDef>,
        Vec<FieldDef>,
        String,
        u8,
        Vec<String>,
    ) = if let Some(p) = &parent_typed {
        (
            Vec::new(),
            types.key_defs(p.id),
            p.path.clone(),
            p.depth,
            types.all_fields(p.id).into_iter().map(|f| f.name).collect(),
        )
    } else {
        {
            let pdef = show_create(pdb, &parent_name.name, ctx, r).await?;
            let key = pdef.primary_key();
            if key.is_empty() {
                return Err(unsupported(format!(
                    "the parent '{}' needs a primary key",
                    parent_name.name
                )));
            }
            let fields = fields_of(&pdef, &parent_name.name, &key, true);
            let key_defs = key
                .iter()
                .filter_map(|k| {
                    fields
                        .iter()
                        .find(|f| f.name.eq_ignore_ascii_case(k))
                        .cloned()
                })
                .collect();
            let names = fields.iter().map(|f| f.name.clone()).collect();
            (
                fields,
                key_defs,
                format!("/{}/", parent_name.name),
                0,
                names,
            )
        }
    };
    let depth = parent_depth + 1;
    if depth > ctx.limits.type_max_depth {
        return Err(SprigError::new(
            SprigCode::TypePathTooLong,
            format!(
                "Type '{child}' would be {depth} levels deep, over types.max_depth ({})",
                ctx.limits.type_max_depth
            ),
        ));
    }
    let path = format!("{parent_path}{child}/");
    if path.len() > MAX_TYPE_PATH {
        return Err(SprigError::new(
            SprigCode::TypePathTooLong,
            format!("The path of type '{child}' is over {MAX_TYPE_PATH} characters"),
        ));
    }
    for c in def.columns() {
        let clash = inherited.iter().any(|f| f.eq_ignore_ascii_case(&c.name))
            || parent_typed
                .as_ref()
                .is_some_and(|p| types.name_taken(p.id, &c.name));
        if clash {
            return Err(SprigError::new(
                SprigCode::TypeFieldConflict,
                format!(
                    "Column '{}' of '{child}' clashes with a column of '{}'",
                    c.name, parent_name.name
                ),
            ));
        }
    }
    // The extension table: the key, the subtype's own columns, the key
    // constraints.
    let key_names: Vec<String> = root_key.iter().map(|k| k.name.clone()).collect();
    let key_cols: Vec<String> = root_key
        .iter()
        .map(|k| format!("{} {} NOT NULL", quote(&k.name), k.data_type))
        .collect();
    let keys_q: Vec<String> = key_names.iter().map(|k| quote(k)).collect();
    let body = &ct.plain[def.open + 1..def.close];
    let sep = if body.trim().is_empty() { "" } else { ", " };
    let physical = format!(
        "{head}{keys}{sep}{body}, PRIMARY KEY ({kl}), CONSTRAINT {fk} FOREIGN KEY ({kl}) \
         REFERENCES {parent} ({kl}){tail}",
        head = &ct.plain[..=def.open],
        keys = key_cols.join(", "),
        kl = keys_q.join(", "),
        fk = quote(&fk_name(&child)),
        parent = table_sql(pdb, &parent_name.name),
        tail = &ct.plain[def.close..],
    );
    if parent_typed.is_none() {
        add_type_columns(pdb, &parent_name.name, &key_names, r).await?;
    }
    r.exec(&physical).await?;
    let (pdb_id, parent_id) = r
        .object_id(pdb, &parent_name.name)
        .await?
        .ok_or_else(|| SprigError::internal("the parent vanished"))?;
    let (_, child_id) = r
        .object_id(db, &child)
        .await?
        .ok_or_else(|| SprigError::internal("the new table vanished"))?;
    let root_id = parent_typed.as_ref().map_or(parent_id, |p| p.root);
    if parent_typed.is_none() {
        let fields = root_fields;
        change.fields.push((parent_id, fields.clone()));
        change.upsert_types.push(TypeDef {
            id: parent_id,
            db: pdb_id,
            name: parent_name.name.clone(),
            parent: None,
            root: parent_id,
            depth: 0,
            path: parent_path.clone(),
            is_abstract: false,
            storage: Storage::Joined,
            key_fields: key_names.clone(),
            own_fields: fields,
            state: TypeState::Active,
            version: 1,
        });
    }
    let own = fields_of(&def, &child, &[], true);
    change.fields.push((child_id, own.clone()));
    change.upsert_types.push(TypeDef {
        id: child_id,
        db: pdb_id,
        name: child.clone(),
        parent: Some(parent_id),
        root: root_id,
        depth,
        path,
        is_abstract: ct.is_abstract,
        storage: Storage::Joined,
        key_fields: key_names,
        own_fields: own,
        state: TypeState::Active,
        version: 1,
    });
    r.commit_types(&change).await?;
    tracing::info!(type_name = %child, parent = %parent_name.name, "type created");
    Ok(Done::default())
}

/// `ALTER TABLE` of a typed table: refuses renames and clashing columns,
/// runs the statement, and records the type's new fields.
///
/// # Errors
/// 39702, 39710, or the statement's error.
pub async fn alter(
    at: &AlterTable,
    sql: &str,
    t: &Arc<TypeDef>,
    types: &DbTypes,
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
) -> Result<Done> {
    if at.renames {
        return Err(unsupported("renaming a typed table is not supported yet"));
    }
    for c in &at.added {
        if is_type_column(c) || types.name_taken(t.id, c) {
            return Err(SprigError::new(
                SprigCode::TypeFieldConflict,
                format!(
                    "Column '{c}' clashes with a column of type '{}' or its subtypes",
                    t.name
                ),
            ));
        }
    }
    let done = r.exec(sql).await?;
    let db = at.table.db.as_deref();
    let def = show_create(db, &t.name, ctx, r).await?;
    let own = fields_of(&def, &t.name, &t.key_fields, t.is_root());
    r.commit_types(&TypesChange {
        fields: vec![(t.id, own)],
        ..TypesChange::default()
    })
    .await?;
    Ok(done)
}

/// `DROP TABLE` naming typed tables or tables with hierarchies.
///
/// # Errors
/// 39707 for a type with subtypes without `CASCADE`, or a step's error.
pub async fn drop(dt: &DropTable, ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<Done> {
    let lctn = ctx.lookup.lctn();
    let mut plain: Vec<String> = Vec::new();
    let mut hierarchies = Vec::new();
    // (db as written, types, dropped type ids)
    let mut groups: Vec<(Option<String>, Arc<DbTypes>, BTreeSet<TypeId>)> = Vec::new();
    let listed: Vec<(Option<String>, String)> = dt
        .tables
        .iter()
        .map(|n| (n.db.clone(), n.name.to_lowercase()))
        .collect();
    for n in &dt.tables {
        let types = ctx.lookup.db(n.db.as_deref());
        let t = types.as_ref().and_then(|d| d.get(&n.name, lctn).cloned());
        let (Some(types), Some(t)) = (types, t) else {
            if let Some(h) = ctx
                .lookup
                .db(n.db.as_deref())
                .and_then(|d| d.hierarchy(&n.name, lctn).cloned())
            {
                hierarchies.push(h.object_id);
            }
            plain.push(table_sql(n.db.as_deref(), &n.name));
            continue;
        };
        let below = types.descendants(t.id);
        let unlisted: Vec<String> = below
            .iter()
            .filter(|d| !listed.contains(&(n.db.clone(), d.name.to_lowercase())))
            .map(|d| d.name.clone())
            .collect();
        if !unlisted.is_empty() && !dt.cascade {
            return Err(SprigError::new(
                SprigCode::TypeHasDependents,
                format!(
                    "Type '{}' has subtypes ({}): drop them first, or add CASCADE",
                    t.name,
                    unlisted.join(", ")
                ),
            ));
        }
        let mut ids: BTreeSet<TypeId> = below.iter().map(|d| d.id).collect();
        ids.insert(t.id);
        match groups
            .iter_mut()
            .find(|(d, ty, _)| d == &n.db && Arc::ptr_eq(ty, &types))
        {
            Some((_, _, set)) => set.extend(ids),
            None => groups.push((n.db.clone(), types, ids)),
        }
    }
    // Untyped tables first, as one statement (MySQL's own checks apply).
    if !plain.is_empty() {
        r.exec(&format!(
            "DROP TABLE {}{}",
            if dt.if_exists { "IF EXISTS " } else { "" },
            plain.join(", ")
        ))
        .await?;
        if !hierarchies.is_empty() {
            r.commit_types(&TypesChange {
                delete_hierarchies: hierarchies,
                ..TypesChange::default()
            })
            .await?;
        }
    }
    for (db, types, ids) in groups {
        let db = db.as_deref();
        let mut doomed: Vec<Arc<TypeDef>> = ids
            .iter()
            .filter_map(|i| types.by_id(*i).cloned())
            .collect();
        doomed.sort_by(|a, b| b.depth.cmp(&a.depth).then(a.id.cmp(&b.id)));
        r.commit_types(&TypesChange {
            type_states: doomed.iter().map(|t| (t.id, TypeState::Dropping)).collect(),
            ..TypesChange::default()
        })
        .await?;
        for t in &doomed {
            r.exec(&format!("DROP TABLE IF EXISTS {}", table_sql(db, &t.name)))
                .await?;
        }
        // Rows of dropped subtypes whose root stays.
        let tops: Vec<&Arc<TypeDef>> = doomed
            .iter()
            .filter(|t| !t.is_root() && !t.parent.is_some_and(|p| ids.contains(&p)))
            .filter(|t| !ids.contains(&t.root))
            .collect();
        let batch = ctx.limits.write_batch_rows.max(1);
        let nbe = ctx.lex.no_backslash_escapes;
        for t in tops {
            let Some(root) = types.by_id(t.root) else {
                continue;
            };
            // Their rows in the ancestors' extension tables go first
            // (deepest first), then the root rows, by key.
            let ancestors: Vec<Arc<TypeDef>> = types
                .chain(t.id)
                .into_iter()
                .filter(|x| !x.is_root() && x.id != t.id)
                .rev()
                .collect();
            let key: Vec<String> = root.key_fields.clone();
            let cols: Vec<String> = key.iter().map(|k| quote(k)).collect();
            loop {
                let found = r
                    .query(&format!(
                        "SELECT {} FROM {} WHERE {pc} >= {lo} AND {pc} < {hi} LIMIT {batch}",
                        cols.join(", "),
                        table_sql(db, &root.name),
                        pc = quote(TYPE_PATH_COLUMN),
                        lo = quote_str(&t.path, false),
                        hi = quote_str(&String::from_utf8_lossy(&upper(t.path.as_bytes())), false),
                    ))
                    .await?;
                if found.rows.is_empty() {
                    break;
                }
                let keys: Vec<Vec<String>> = found
                    .rows
                    .iter()
                    .map(|row| {
                        (0..key.len())
                            .map(|i| crate::write::literal(&found, i, row[i].as_ref(), nbe))
                            .collect()
                    })
                    .collect();
                for x in ancestors.iter().chain(std::iter::once(&root.clone())) {
                    r.exec(&format!(
                        "DELETE FROM {} WHERE {}",
                        table_sql(db, &x.name),
                        crate::write::key_in(None, &key, &keys)
                    ))
                    .await?;
                }
            }
        }
        r.commit_types(&TypesChange {
            delete_types: doomed.iter().map(|t| t.id).collect(),
            ..TypesChange::default()
        })
        .await?;
        tracing::info!(types = doomed.len(), "types dropped");
    }
    Ok(Done::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fk_names_fit() {
        assert_eq!(fk_name("vehicle"), "_sprig_fk_vehicle");
        let long = "x".repeat(64);
        let n = fk_name(&long);
        assert!(n.len() <= 64 && n.starts_with(FK_PREFIX));
        assert_ne!(fk_name(&long), fk_name(&"y".repeat(64)));
    }
}
