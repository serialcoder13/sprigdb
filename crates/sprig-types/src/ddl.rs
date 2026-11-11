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
use crate::exec::{Done, Runner, Txn, unsupported};
use crate::grammar::{AlterTable, CreateTable, DropTable};
use crate::model::{
    AUTO_INCREMENT, DbTypes, DiscValue, FieldDef, Storage, TypeDef, TypeId, TypeState, upper,
};
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

/// What a discriminated root's generated type columns map: each declared
/// value to its type's name and path; anything else is the root's.
pub(crate) struct DiscMap {
    /// The comparison operand: the column, under its character set's
    /// binary collation for strings (values match byte for byte).
    pub(crate) operand: String,
    /// The root's name and path.
    pub(crate) root: (String, String),
    /// Value, type name, type path.
    pub(crate) arms: Vec<(DiscValue, String, String)>,
}

impl DiscMap {
    /// The generated expressions of `_type` and `_type_path`.
    pub(crate) fn exprs(&self, nbe: bool) -> (String, String) {
        let case = |pick: &dyn Fn(&(DiscValue, String, String)) -> String, other: &str| {
            let mut s = format!("CASE {}", self.operand);
            if self.arms.is_empty() {
                // A CASE needs an arm; NULL never matches.
                s.push_str(" WHEN NULL THEN ");
                s.push_str(&quote_str(other, false));
            }
            for a in &self.arms {
                s.push_str(&format!(" WHEN {} THEN {}", a.0.sql(nbe), pick(a)));
            }
            s.push_str(&format!(" ELSE {} END", quote_str(other, false)));
            s
        };
        (
            case(&|a| quote_str(&a.1, false), &self.root.0),
            case(&|a| quote_str(&a.2, false), &self.root.1),
        )
    }

    /// The predicate "the discriminator holds `v`", as the columns test it.
    pub(crate) fn holds(&self, alias: &str, v: &DiscValue, nbe: bool) -> String {
        let operand = self
            .operand
            .replacen('`', &format!("{}.`", quote(alias)), 1);
        format!("{operand} = {}", v.sql(nbe))
    }
}

/// The comparison operand of discriminator `col` of `table`: the column's
/// character set's `_bin` collation for strings, the column itself for
/// numbers and binary strings.
async fn disc_operand(
    db: Option<&str>,
    table: &str,
    col: &str,
    r: &mut dyn Runner,
) -> Result<String> {
    let rows = r
        .query(&format!(
            "SELECT CHARSET(x.{c}) FROM (SELECT 1) AS d LEFT JOIN {t} AS x ON 1 = 0",
            c = quote(col),
            t = table_sql(db, table)
        ))
        .await?;
    let cs = rows
        .rows
        .first()
        .and_then(|r| r.first().cloned().flatten())
        .map(|v| String::from_utf8_lossy(&v).into_owned())
        .unwrap_or_default();
    if cs.is_empty() || cs.eq_ignore_ascii_case("binary") {
        return Ok(quote(col));
    }
    if !cs.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(SprigError::internal(format!("character set {cs:?}")));
    }
    Ok(format!("({} COLLATE {cs}_bin)", quote(col)))
}

/// The discriminator map of `root`'s hierarchy, with `extra` (a subtype
/// being created) added and the types in `without` (being dropped) left
/// out.
async fn disc_map(
    db: Option<&str>,
    root: &TypeDef,
    types: &DbTypes,
    extra: Option<(DiscValue, String, String)>,
    without: &BTreeSet<TypeId>,
    r: &mut dyn Runner,
) -> Result<DiscMap> {
    let col = root
        .discriminator
        .clone()
        .ok_or_else(|| SprigError::internal("a root without a discriminator"))?;
    let operand = disc_operand(db, &root.name, &col, r).await?;
    let mut arms: Vec<(DiscValue, String, String)> = types
        .descendants(root.id)
        .into_iter()
        .filter(|t| !without.contains(&t.id))
        .filter_map(|t| Some((t.disc_value.clone()?, t.name.clone(), t.path.clone())))
        .collect();
    arms.extend(extra);
    Ok(DiscMap {
        operand,
        root: (root.name.clone(), root.path.clone()),
        arms,
    })
}

/// Gives a discriminated root its generated type columns (replacing them
/// when they exist), then their index. Rows take their types from the
/// discriminator at once: the columns are virtual.
async fn set_disc_columns(
    db: Option<&str>,
    root: &str,
    key: &[String],
    map: &DiscMap,
    replace: bool,
    nbe: bool,
    r: &mut dyn Runner,
) -> Result<()> {
    let t = table_sql(db, root);
    let (te, pe) = map.exprs(nbe);
    let add = format!(
        "ADD COLUMN {tc} VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin AS ({te}) VIRTUAL \
         INVISIBLE, ADD COLUMN {pc} VARCHAR({MAX_TYPE_PATH}) CHARACTER SET utf8mb4 COLLATE \
         utf8mb4_bin AS ({pe}) VIRTUAL INVISIBLE",
        tc = quote(TYPE_COLUMN),
        pc = quote(TYPE_PATH_COLUMN),
    );
    if replace {
        // MariaDB can't drop and add virtual columns and an index on them
        // in one online ALTER: the index goes first and comes back last.
        r.exec(&format!(
            "ALTER TABLE {t} DROP INDEX IF EXISTS {}",
            quote(TYPE_PATH_INDEX)
        ))
        .await?;
        r.exec(&format!(
            "ALTER TABLE {t} DROP COLUMN {}, DROP COLUMN {}, {add}",
            quote(TYPE_COLUMN),
            quote(TYPE_PATH_COLUMN)
        ))
        .await?;
    } else {
        r.exec(&format!("ALTER TABLE {t} {add}")).await?;
    }
    let keys: Vec<String> = key.iter().map(|k| quote(k)).collect();
    r.exec(&format!(
        "ALTER TABLE {t} ADD INDEX IF NOT EXISTS {} ({}, {})",
        quote(TYPE_PATH_INDEX),
        quote(TYPE_PATH_COLUMN),
        keys.join(", ")
    ))
    .await?;
    Ok(())
}

/// Joined storage with a discriminator: gives every row whose value is
/// `v` (the new subtype `new`'s) its rows in the extension tables of
/// `new`'s chain, shallowest first, at their columns' defaults. Runs in a
/// transaction; idempotent.
async fn adopt_rows(
    db: Option<&str>,
    root: &TypeDef,
    chain_below_root: &[(String, String)],
    map: &DiscMap,
    v: &DiscValue,
    nbe: bool,
    r: &mut dyn Runner,
) -> Result<u64> {
    let tx = Txn::begin(r).await?;
    let result: Result<u64> = async {
        let mut n = 0;
        for (table, _) in chain_below_root {
            let keys = |a: &str| -> Vec<String> {
                root.key_fields
                    .iter()
                    .map(|k| format!("{}.{}", quote(a), quote(k)))
                    .collect()
            };
            let on: Vec<String> = root
                .key_fields
                .iter()
                .map(|k| format!("e.{q} = r.{q}", q = quote(k)))
                .collect();
            let d = r
                .exec(&format!(
                    "INSERT INTO {e} ({cols}) SELECT {rk} FROM {root} AS r WHERE {holds} AND NOT \
                     EXISTS (SELECT 1 FROM {e} AS e WHERE {on})",
                    e = table_sql(db, table),
                    cols = root
                        .key_fields
                        .iter()
                        .map(|k| quote(k))
                        .collect::<Vec<_>>()
                        .join(", "),
                    rk = keys("r").join(", "),
                    root = table_sql(db, &root.name),
                    holds = map.holds("r", v, nbe),
                    on = on.join(" AND "),
                ))
                .await?;
            n += d.affected_rows;
        }
        Ok(n)
    }
    .await;
    tx.end(r, result).await
}

/// The column definitions and other elements of a single-table subtype
/// that go into its root table, and the root's columns it adopts.
struct SingleTablePlan {
    /// `ADD ...` clauses for the root.
    adds: Vec<String>,
    /// Adopted root columns, as the root table defines them.
    adopted: Vec<FieldDef>,
    /// The subtype's own fields, in its declared order.
    own: Vec<FieldDef>,
}

fn single_table_plan(
    def: &TableDef,
    text: &str,
    root: &str,
    root_def: &TableDef,
    adoptable: &[String],
) -> Result<SingleTablePlan> {
    let mut adds = Vec::new();
    let mut adopted = Vec::new();
    let mut own = Vec::new();
    for (e, range) in &def.elements {
        match e {
            Element::Column(c) => {
                if adoptable.iter().any(|a| a.eq_ignore_ascii_case(&c.name)) {
                    let Some(pc) = root_def.column(&c.name) else {
                        continue;
                    };
                    let f = field_of(pc, root, own.len());
                    adopted.push(f.clone());
                    own.push(f);
                    continue;
                }
                if !c.nullable && c.default.is_none() && !c.auto_increment {
                    return Err(unsupported(format!(
                        "column '{}' is NOT NULL without a default: in single-table storage every \
                         row of the root table has a subtype's columns, so they must take NULL or \
                         have a default",
                        c.name
                    )));
                }
                adds.push(format!("ADD COLUMN {}", &text[range.clone()]));
                own.push(field_of(c, root, own.len()));
            }
            Element::PrimaryKey(_) => {}
            Element::Other { .. } => adds.push(format!("ADD {}", &text[range.clone()])),
        }
    }
    Ok(SingleTablePlan { adds, adopted, own })
}

fn field_of(c: &crate::tabledef::Column, table: &str, i: usize) -> FieldDef {
    FieldDef {
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
    }
}

/// The storage a `SPRIG_STORAGE` option names.
fn storage_of(opt: Option<&str>) -> Result<Option<Storage>> {
    match opt {
        None => Ok(None),
        Some("joined") => Ok(Some(Storage::Joined)),
        Some("single_table") => Ok(Some(Storage::SingleTable)),
        Some(other) => Err(unsupported(format!("SPRIG_STORAGE = '{other}'"))),
    }
}

fn disc_mismatch(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::TypeDiscriminatorMismatch, msg.into())
}

/// `CREATE TABLE ... INHERITS (p) [WHERE c = v]` and `CREATE TABLE ...
/// WITH (SPRIG_ABSTRACT = 1 | SPRIG_STORAGE = '...' | SPRIG_DISCRIMINATOR =
/// 'c')`.
///
/// # Errors
/// 39701, 39702, 39708, 39710, 39717, or a step's error.
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
    let storage_opt = storage_of(ct.storage.as_deref())?;
    let nbe = ctx.lex.no_backslash_escapes;
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
    // A root declared on its own: `WITH (SPRIG_ABSTRACT = 1)`,
    // `SPRIG_STORAGE` or `SPRIG_DISCRIMINATOR`.
    let Some(parent_name) = ct.inherits.first() else {
        if ct.disc_where.is_some() {
            return Err(unsupported(
                "WHERE column = value names a subtype's value: it needs INHERITS (parent)",
            ));
        }
        if let Some(d) = &ct.discriminator
            && def.column(d).is_none()
        {
            return Err(unknown_column(d, &child));
        }
        if storage_opt == Some(Storage::SingleTable) && ct.discriminator.is_none() {
            return Err(single_needs_discriminator());
        }
        r.exec(&ct.plain).await?;
        let pdef = show_create(db, &child, ctx, r).await?;
        let key = pdef.primary_key();
        if key.is_empty() {
            return Err(unsupported("a root type needs a primary key"));
        }
        let (db_id, id) = r
            .object_id(db, &child)
            .await?
            .ok_or_else(|| SprigError::internal("the new table vanished"))?;
        let disc_col = ct
            .discriminator
            .as_ref()
            .and_then(|d| pdef.column(d))
            .map(|c| c.name.clone());
        let root = TypeDef {
            id,
            db: db_id,
            name: child.clone(),
            parent: None,
            root: id,
            depth: 0,
            path: format!("/{child}/"),
            is_abstract: ct.is_abstract,
            storage: storage_opt.unwrap_or(Storage::Joined),
            key_fields: key.clone(),
            own_fields: fields_of(&pdef, &child, &key, true),
            discriminator: disc_col,
            disc_value: None,
            state: TypeState::Active,
            version: 1,
        };
        if root.discriminator.is_some() {
            let map = disc_map(db, &root, &types, None, &BTreeSet::new(), r).await?;
            set_disc_columns(db, &child, &key, &map, false, nbe, r).await?;
        } else {
            add_type_columns(db, &child, &key, r).await?;
        }
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
    let root_typed = parent_typed.as_ref().and_then(|p| types.root_of(p.id));
    // The root's physical definition: its key, columns and discriminator.
    let root_name = root_typed
        .as_ref()
        .map_or_else(|| parent_name.name.clone(), |t| t.name.clone());
    let storage = match (&root_typed, storage_opt) {
        (Some(rt), Some(s)) if s != rt.storage => {
            return Err(unsupported(format!(
                "SPRIG_STORAGE is the root's: '{}' stores its hierarchy as '{}'",
                rt.name,
                rt.storage.as_str()
            )));
        }
        (Some(rt), _) => rt.storage,
        (None, s) => s.unwrap_or(Storage::Joined),
    };
    // Read when a plain table becomes a root, and in single-table storage
    // (the subtype's columns go into it).
    let root_def = if root_typed.is_none() || storage == Storage::SingleTable {
        show_create(pdb, &root_name, ctx, r).await?
    } else {
        TableDef::parse("CREATE TABLE x ()", &ctx.lex)
            .ok_or_else(|| SprigError::internal("an empty table definition"))?
    };
    // The discriminator: the root's, or the one this statement declares.
    let declared = match (&ct.discriminator, &ct.disc_where) {
        (Some(a), Some((b, _))) if !a.eq_ignore_ascii_case(b) => {
            return Err(unsupported(format!(
                "SPRIG_DISCRIMINATOR = '{a}' and WHERE {b} = ... name different columns"
            )));
        }
        (Some(a), _) => Some(a.clone()),
        (None, Some((b, _))) => Some(b.clone()),
        (None, None) => None,
    };
    let disc_col: Option<String> = match (&root_typed, declared) {
        (Some(rt), Some(d)) => match &rt.discriminator {
            Some(c) if c.eq_ignore_ascii_case(&d) => Some(c.clone()),
            Some(c) => {
                return Err(unsupported(format!(
                    "the discriminator of '{}' is '{c}', not '{d}'",
                    rt.name
                )));
            }
            None => {
                return Err(unsupported(format!(
                    "'{}' is typed without a discriminator: a hierarchy gets its discriminator \
                     when its root becomes typed",
                    rt.name
                )));
            }
        },
        (Some(rt), None) => rt.discriminator.clone(),
        (None, Some(d)) => Some(
            root_def
                .column(&d)
                .ok_or_else(|| unknown_column(&d, &root_name))?
                .name
                .clone(),
        ),
        (None, None) => None,
    };
    if storage == Storage::SingleTable && disc_col.is_none() {
        return Err(single_needs_discriminator());
    }
    let disc_value: Option<DiscValue> = ct.disc_where.as_ref().map(|(_, v)| v.clone());
    if disc_col.is_some() && disc_value.is_none() && !ct.is_abstract {
        return Err(unsupported(format!(
            "'{root_name}' has a discriminator: say which value marks '{child}' (INHERITS ({}) \
             WHERE {} = ...), or make it abstract",
            parent_name.name,
            disc_col.as_deref().unwrap_or_default()
        )));
    }
    if ct.discriminator.is_some() && ct.disc_where.is_none() {
        return Err(unsupported(
            "SPRIG_DISCRIMINATOR goes on the root; a subtype gives its value with WHERE",
        ));
    }
    if let (Some(v), Some(rt)) = (&disc_value, &root_typed)
        && let Some(other) = types
            .descendants(rt.id)
            .into_iter()
            .find(|t| t.disc_value.as_ref().is_some_and(|d| d.same(v)))
    {
        return Err(disc_mismatch(format!(
            "The value {v} already marks type '{}'",
            other.name
        )));
    }
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
        let key = root_def.primary_key();
        if key.is_empty() {
            return Err(unsupported(format!(
                "the parent '{}' needs a primary key",
                parent_name.name
            )));
        }
        let fields = fields_of(&root_def, &parent_name.name, &key, true);
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
    let key_names: Vec<String> = root_key.iter().map(|k| k.name.clone()).collect();
    // Single-table storage adopts the root's own columns of the same name
    // (a Hibernate SINGLE_TABLE table already has every subtype's columns).
    let root_own: Vec<String> = match &root_typed {
        Some(rt) => rt.own_fields.iter().map(|f| f.name.clone()).collect(),
        None => root_fields.iter().map(|f| f.name.clone()).collect(),
    };
    let adoptable: Vec<String> = if storage == Storage::SingleTable {
        def.columns()
            .map(|c| c.name.clone())
            .filter(|c| {
                root_own.iter().any(|o| o.eq_ignore_ascii_case(c))
                    && !key_names.iter().any(|k| k.eq_ignore_ascii_case(c))
                    && !disc_col
                        .as_deref()
                        .is_some_and(|d| d.eq_ignore_ascii_case(c))
            })
            .collect()
    } else {
        Vec::new()
    };
    for c in def.columns() {
        if adoptable.iter().any(|a| a.eq_ignore_ascii_case(&c.name)) {
            continue;
        }
        let clash = inherited.iter().any(|f| f.eq_ignore_ascii_case(&c.name))
            || parent_typed
                .as_ref()
                .is_some_and(|p| types.name_taken(p.id, &c.name))
            || (storage == Storage::SingleTable && root_def.column(&c.name).is_some());
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
    let key_cols: Vec<String> = root_key
        .iter()
        .map(|k| format!("{} {} NOT NULL", quote(&k.name), k.data_type))
        .collect();
    let keys_q: Vec<String> = key_names.iter().map(|k| quote(k)).collect();
    // The root's type columns: plain, or generated from the discriminator
    // (with the new subtype's value already mapped, so its rows are typed
    // at once).
    let the_root = |id: TypeId, db_id| TypeDef {
        id,
        db: db_id,
        name: root_name.clone(),
        parent: None,
        root: id,
        depth: 0,
        path: parent_path.clone(),
        is_abstract: false,
        storage,
        key_fields: key_names.clone(),
        own_fields: root_fields.clone(),
        discriminator: disc_col.clone(),
        disc_value: None,
        state: TypeState::Active,
        version: 1,
    };
    let extra = disc_value.clone().map(|v| (v, child.clone(), path.clone()));
    let mut single_own: Option<Vec<FieldDef>> = None;
    let mut root_own_after: Option<Vec<FieldDef>> = None;
    if storage == Storage::SingleTable {
        let plan = single_table_plan(&def, &ct.plain, &root_name, &root_def, &adoptable)?;
        if parent_typed.is_none() && disc_col.is_none() {
            add_type_columns(pdb, &root_name, &key_names, r).await?;
        }
        // The subtype's empty table: its name, privileges and dump tools.
        r.exec(&format!(
            "{head}{keys}, PRIMARY KEY ({kl}){tail}",
            head = &ct.plain[..=def.open],
            keys = key_cols.join(", "),
            kl = keys_q.join(", "),
            tail = &ct.plain[def.close..],
        ))
        .await?;
        if !plan.adds.is_empty() {
            r.exec(&format!(
                "ALTER TABLE {} {}",
                table_sql(pdb, &root_name),
                plan.adds.join(", ")
            ))
            .await?;
        }
        if !plan.adopted.is_empty() {
            let base: Vec<FieldDef> = root_typed
                .as_ref()
                .map_or_else(|| root_fields.clone(), |rt| rt.own_fields.clone());
            root_own_after = Some(
                base.into_iter()
                    .filter(|f| {
                        !plan
                            .adopted
                            .iter()
                            .any(|a| a.name.eq_ignore_ascii_case(&f.name))
                    })
                    .collect(),
            );
        }
        single_own = Some(plan.own);
        if disc_col.is_some() && (root_typed.is_none() || extra.is_some()) {
            let (pdb_id, parent_id) = r
                .object_id(pdb, &parent_name.name)
                .await?
                .ok_or_else(|| SprigError::internal("the parent vanished"))?;
            let rid = root_typed.as_ref().map_or(parent_id, |t| t.id);
            let root = root_typed
                .as_ref()
                .map_or_else(|| the_root(rid, pdb_id), |t| (**t).clone());
            let map = disc_map(pdb, &root, &types, extra.clone(), &BTreeSet::new(), r).await?;
            set_disc_columns(
                pdb,
                &root_name,
                &key_names,
                &map,
                root_typed.is_some(),
                nbe,
                r,
            )
            .await?;
        }
    } else {
        // The extension table: the key, the subtype's own columns, the key
        // constraints.
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
        if parent_typed.is_none() && disc_col.is_none() {
            add_type_columns(pdb, &parent_name.name, &key_names, r).await?;
        }
        r.exec(&physical).await?;
        if let (Some(_), Some(v)) = (&disc_col, &disc_value) {
            let (pdb_id, parent_id) = r
                .object_id(pdb, &parent_name.name)
                .await?
                .ok_or_else(|| SprigError::internal("the parent vanished"))?;
            let rid = root_typed.as_ref().map_or(parent_id, |t| t.id);
            let root = root_typed
                .as_ref()
                .map_or_else(|| the_root(rid, pdb_id), |t| (**t).clone());
            let map = disc_map(pdb, &root, &types, extra.clone(), &BTreeSet::new(), r).await?;
            // Existing rows with the value get their extension rows before
            // the type columns map them (a failure leaves no half-typed
            // rows), and again after (rows written in between).
            let mut chain: Vec<(String, String)> = parent_typed
                .as_ref()
                .map(|p| {
                    types
                        .chain(p.id)
                        .into_iter()
                        .skip(1)
                        .map(|t| (t.name.clone(), t.path.clone()))
                        .collect()
                })
                .unwrap_or_default();
            chain.push((child.clone(), path.clone()));
            if let Err(e) = adopt_rows(pdb, &root, &chain, &map, v, nbe, r).await {
                let _ = r
                    .exec(&format!("DROP TABLE IF EXISTS {}", table_sql(db, &child)))
                    .await;
                return Err(e);
            }
            set_disc_columns(
                pdb,
                &root_name,
                &key_names,
                &map,
                root_typed.is_some(),
                nbe,
                r,
            )
            .await?;
            adopt_rows(pdb, &root, &chain, &map, v, nbe, r).await?;
        } else if disc_col.is_some() && parent_typed.is_none() {
            // An abstract first subtype: the root's columns, no value yet.
            let (pdb_id, parent_id) = r
                .object_id(pdb, &parent_name.name)
                .await?
                .ok_or_else(|| SprigError::internal("the parent vanished"))?;
            let root = the_root(parent_id, pdb_id);
            let map = disc_map(pdb, &root, &types, None, &BTreeSet::new(), r).await?;
            set_disc_columns(pdb, &root_name, &key_names, &map, false, nbe, r).await?;
        }
    }
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
        let mut root = the_root(parent_id, pdb_id);
        if let Some(f) = root_own_after.take() {
            root.own_fields = f;
        }
        change.fields.push((parent_id, root.own_fields.clone()));
        change.upsert_types.push(root);
    } else if let (Some(f), Some(rt)) = (root_own_after.take(), &root_typed) {
        change.fields.push((rt.id, f));
    }
    let own = single_own.unwrap_or_else(|| fields_of(&def, &child, &[], true));
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
        storage,
        key_fields: key_names,
        own_fields: own,
        discriminator: None,
        disc_value,
        state: TypeState::Active,
        version: 1,
    });
    r.commit_types(&change).await?;
    tracing::info!(type_name = %child, parent = %parent_name.name, "type created");
    Ok(Done::default())
}

fn single_needs_discriminator() -> SprigError {
    unsupported(
        "single-table storage needs a discriminator column (WHERE column = value on the \
         subtypes, or SPRIG_DISCRIMINATOR on the root): dumps restore each row's type from it",
    )
}

fn unknown_column(c: &str, table: &str) -> SprigError {
    SprigError::standard(
        1054,
        *b"42S22",
        format!("Unknown column '{c}' in '{table}'"),
    )
}

/// `ALTER TABLE` of a typed table: refuses renames and clashing columns,
/// runs the statement, and records the type's new fields. A single-table
/// subtype's `ADD COLUMN` goes to its root's table; a discriminator column
/// can't be dropped, changed or renamed.
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
    let root = types
        .root_of(t.id)
        .ok_or_else(|| SprigError::internal("a type without a root"))?;
    if let Some(d) = &root.discriminator
        && t.is_root()
        && at.changed_columns.iter().any(|c| c.eq_ignore_ascii_case(d))
    {
        return Err(unsupported(format!(
            "'{d}' is the discriminator of '{}': it can't be dropped, changed or renamed",
            root.name
        )));
    }
    let db = at.table.db.as_deref();
    let single = root.storage == Storage::SingleTable;
    if single && !t.is_root() {
        if !at.only_add_columns || at.added.is_empty() {
            return Err(unsupported(
                "in single-table storage a subtype's ALTER TABLE may only add columns",
            ));
        }
        // The same clauses, on the root's table.
        let text = format!(
            "{}{}{}",
            &sql[..at.name_span.start],
            table_sql(db, &root.name),
            &sql[at.name_span.end..]
        );
        let added = TableDef::parse(
            &format!("CREATE TABLE x ({})", added_columns_text(sql, at)),
            &ctx.lex,
        );
        if let Some(a) = &added
            && let Some(c) = a
                .columns()
                .find(|c| !c.nullable && c.default.is_none() && !c.auto_increment)
        {
            return Err(unsupported(format!(
                "column '{}' is NOT NULL without a default: in single-table storage every row of                  the root table has a subtype's columns",
                c.name
            )));
        }
        let done = r.exec(&text).await?;
        let after = show_create(db, &root.name, ctx, r).await?;
        let mut own = t.own_fields.clone();
        for c in &at.added {
            if let Some(pc) = after.column(c) {
                own.push(field_of(pc, &root.name, own.len()));
            }
        }
        r.commit_types(&TypesChange {
            fields: vec![(t.id, own)],
            ..TypesChange::default()
        })
        .await?;
        return Ok(done);
    }
    let done = r.exec(sql).await?;
    let def = show_create(db, &t.name, ctx, r).await?;
    let mut own = fields_of(&def, &t.name, &t.key_fields, t.is_root());
    if single && t.is_root() {
        // The root's table also holds its subtypes' columns.
        own.retain(|f| !types.descendants(t.id).iter().any(|d| d.owns(&f.name)));
    }
    r.commit_types(&TypesChange {
        fields: vec![(t.id, own)],
        ..TypesChange::default()
    })
    .await?;
    Ok(done)
}

/// The text after `ADD [COLUMN]` of an `ALTER TABLE` that only adds
/// columns, as a column list: `c1 INT, c2 INT`.
fn added_columns_text(sql: &str, at: &AlterTable) -> String {
    let rest = &sql[at.name_span.end..];
    let mut out = Vec::new();
    // Split on top-level commas, drop the leading ADD [COLUMN] [IF NOT
    // EXISTS] and a wrapping parenthesis.
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut cur = String::new();
    for ch in rest.chars() {
        match quote {
            Some(q) if ch == q => quote = None,
            None if ch == '\'' || ch == '"' || ch == '`' => quote = Some(ch),
            None if ch == '(' => depth += 1,
            None if ch == ')' => depth = depth.saturating_sub(1),
            None if ch == ',' && depth == 0 => {
                out.push(std::mem::take(&mut cur));
                continue;
            }
            Some(_) | None => {}
        }
        cur.push(ch);
    }
    out.push(cur);
    out.iter()
        .map(|c| {
            let mut c = c.trim();
            for w in ["ADD", "COLUMN", "IF NOT EXISTS"] {
                if c.len() >= w.len() && c[..w.len()].eq_ignore_ascii_case(w) {
                    c = c[w.len()..].trim_start();
                }
            }
            let c = c
                .strip_prefix('(')
                .and_then(|x| x.strip_suffix(')'))
                .unwrap_or(c);
            c.to_owned()
        })
        .collect::<Vec<_>>()
        .join(", ")
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
            // (deepest first), then the root rows, by key. Single-table
            // subtypes' tables hold no rows.
            let ancestors: Vec<Arc<TypeDef>> = if root.storage == Storage::SingleTable {
                Vec::new()
            } else {
                types
                    .chain(t.id)
                    .into_iter()
                    .filter(|x| !x.is_root() && x.id != t.id)
                    .rev()
                    .collect()
            };
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
        let mut change = TypesChange {
            delete_types: doomed.iter().map(|t| t.id).collect(),
            ..TypesChange::default()
        };
        // A root that stays: its discriminator stops mapping the dropped
        // values, and in single-table storage the dropped subtypes' columns,
        // which stay in its table, become its own again.
        let roots: BTreeSet<TypeId> = doomed
            .iter()
            .map(|t| t.root)
            .filter(|r| !ids.contains(r))
            .collect();
        for rid in roots {
            let Some(root) = types.by_id(rid).cloned() else {
                continue;
            };
            if root.discriminator.is_some() {
                let map = disc_map(db, &root, &types, None, &ids, r).await?;
                set_disc_columns(db, &root.name, &root.key_fields, &map, true, nbe, r).await?;
            }
            if root.storage == Storage::SingleTable {
                let mut own = root.own_fields.clone();
                for t in doomed.iter().filter(|t| t.root == rid) {
                    own.extend(t.own_fields.iter().cloned());
                }
                change.fields.push((rid, own));
            }
        }
        r.commit_types(&change).await?;
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
