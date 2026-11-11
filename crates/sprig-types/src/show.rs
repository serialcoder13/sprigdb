//! What schema tools see of typed tables (chapter 32, "Polymorphic SELECT",
//! the metadata paragraph; build step 4):
//!
//! - `SHOW CREATE TABLE` shows a root without its type columns and their
//!   index, and a subtype as created: its own columns, `INHERITS
//!   (parent)`, no repeated key and no foreign key to the parent;
//! - `SHOW [FULL] COLUMNS` (and `DESCRIBE`) lists a subtype's inherited
//!   columns first, then its own, and a root without its type columns;
//! - `SHOW [FULL] TABLES` lists parents before their subtypes, so dump
//!   tools create them in order.
//!
//! `information_schema.COLUMNS` still shows the physical tables (see the
//! chapter's As built).

use std::collections::{HashMap, HashSet};

use sprig_common::Result;
use sprig_common::names::quote;

use crate::Ctx;
use crate::ddl::{FK_PREFIX, TYPE_PATH_INDEX};
use crate::exec::{Rows, Runner};
use crate::grammar::{Name, ShowColumns};
use crate::model::{DbTypes, Storage, TypeDef};
use crate::rewrite::{TYPE_COLUMN, TYPE_PATH_COLUMN, quote_str};
use crate::tabledef::{Element, TableDef};

fn table_sql(db: Option<&str>, t: &str) -> String {
    match db {
        Some(d) => format!("{}.{}", quote(d), quote(t)),
        None => quote(t),
    }
}

fn is_type_column(c: &str) -> bool {
    c.eq_ignore_ascii_case(TYPE_COLUMN) || c.eq_ignore_ascii_case(TYPE_PATH_COLUMN)
}

/// The logical `CREATE TABLE` of type `t` from its table's physical one.
#[must_use]
pub fn logical_create(physical: &str, t: &TypeDef, parent: Option<&str>, ctx: &Ctx<'_>) -> String {
    logical_create_from(physical, None, t, parent, None, ctx)
}

/// [`logical_create`], with the columns of a single-table subtype taken
/// from its root's physical text (`root`), and the root's discriminator
/// column (`disc`) for a subtype's `WHERE`.
#[must_use]
pub fn logical_create_from(
    physical: &str,
    root: Option<&str>,
    t: &TypeDef,
    parent: Option<&str>,
    disc: Option<&str>,
    ctx: &Ctx<'_>,
) -> String {
    let Some(def) = TableDef::parse(physical, &ctx.lex) else {
        return physical.to_owned();
    };
    let keep_element = |e: &Element| match e {
        Element::Column(c) => !is_type_column(&c.name) && (t.is_root() || !t.is_key(&c.name)),
        Element::PrimaryKey(_) => t.is_root(),
        Element::Other { name, columns, .. } => {
            let n = name.as_deref().unwrap_or("");
            !n.eq_ignore_ascii_case(TYPE_PATH_INDEX)
                && !n.starts_with(FK_PREFIX)
                && !columns.iter().any(|c| is_type_column(c))
        }
    };
    let mut keep: Vec<&str> = def
        .elements
        .iter()
        .filter(|(e, _)| keep_element(e))
        .map(|(_, r)| &physical[r.clone()])
        .collect();
    // A single-table subtype's own columns live in its root's table.
    if let Some(rt) = root
        && let Some(rdef) = TableDef::parse(rt, &ctx.lex)
    {
        keep = t
            .own_fields
            .iter()
            .filter_map(|f| {
                rdef.elements.iter().find_map(|(e, r)| match e {
                    Element::Column(c) if c.name.eq_ignore_ascii_case(&f.name) => {
                        Some(&rt[r.clone()])
                    }
                    _ => None,
                })
            })
            .collect();
    }
    let mut out = String::with_capacity(physical.len());
    out.push_str(&physical[..=def.open]);
    if !keep.is_empty() {
        out.push_str("\n  ");
        out.push_str(&keep.join(",\n  "));
        out.push('\n');
    }
    out.push(')');
    if let Some(p) = parent {
        out.push_str(&format!(" INHERITS ({})", quote(p)));
        if let (Some(d), Some(v)) = (disc, &t.disc_value) {
            out.push_str(&format!(
                " WHERE {} = {}",
                quote(d),
                v.sql(ctx.lex.no_backslash_escapes)
            ));
        }
    }
    out.push_str(&physical[def.close + 1..]);
    let mut opts = Vec::new();
    if t.is_abstract {
        opts.push("SPRIG_ABSTRACT = 1".to_owned());
    }
    if t.is_root() {
        if t.storage == Storage::SingleTable {
            opts.push("SPRIG_STORAGE = 'single_table'".to_owned());
        }
        if let Some(d) = &t.discriminator {
            opts.push(format!(
                "SPRIG_DISCRIMINATOR = {}",
                quote_str(d, ctx.lex.no_backslash_escapes)
            ));
        }
    }
    if !opts.is_empty() {
        out.push_str(&format!(" WITH ({})", opts.join(", ")));
    }
    out
}

/// `SHOW CREATE TABLE` of a typed table.
///
/// # Errors
/// The statement's.
pub async fn create(
    name: &Name,
    t: &TypeDef,
    types: &DbTypes,
    ctx: &Ctx<'_>,
    r: &mut dyn Runner,
) -> Result<Rows> {
    let mut rows = r
        .query(&format!(
            "SHOW CREATE TABLE {}",
            table_sql(name.db.as_deref(), &name.name)
        ))
        .await?;
    let parent = t
        .parent
        .and_then(|p| types.by_id(p))
        .map(|p| p.name.clone());
    let root = types.root_of(t.id);
    let root_text = match &root {
        Some(rt) if rt.storage == Storage::SingleTable && !t.is_root() => {
            let rows = r
                .query(&format!(
                    "SHOW CREATE TABLE {}",
                    table_sql(name.db.as_deref(), &rt.name)
                ))
                .await?;
            rows.rows
                .first()
                .and_then(|row| row.get(1).cloned().flatten())
                .map(|v| String::from_utf8_lossy(&v).into_owned())
        }
        _ => None,
    };
    let disc = root.and_then(|r| r.discriminator.clone());
    for row in &mut rows.rows {
        if let Some(Some(v)) = row.get_mut(1) {
            let text = String::from_utf8_lossy(v).into_owned();
            *v = logical_create_from(
                &text,
                root_text.as_deref(),
                t,
                parent.as_deref(),
                disc.as_deref(),
                ctx,
            )
            .into_bytes();
        }
    }
    Ok(rows)
}

/// `SHOW [FULL] COLUMNS` of a typed table.
///
/// # Errors
/// The statements'.
pub async fn columns(
    sc: &ShowColumns,
    t: &TypeDef,
    types: &DbTypes,
    r: &mut dyn Runner,
) -> Result<Rows> {
    let filter = sc
        .filter
        .as_deref()
        .map(|f| format!(" {f}"))
        .unwrap_or_default();
    let full = if sc.full { "FULL " } else { "" };
    let db = sc.table.db.as_deref();
    let mut out = Rows::default();
    if t.storage == Storage::SingleTable && !t.is_root() {
        // Every column is the root table's: the type's own, inherited first.
        let root = types
            .root_of(t.id)
            .ok_or_else(|| sprig_common::SprigError::internal("a type without a root"))?;
        let rows = r
            .query(&format!(
                "SHOW {full}COLUMNS FROM {}{filter}",
                table_sql(db, &root.name)
            ))
            .await?;
        out.columns = rows.columns;
        let name_of = |row: &Vec<Option<Vec<u8>>>| {
            row.first()
                .cloned()
                .flatten()
                .map(|v| String::from_utf8_lossy(&v).into_owned())
                .unwrap_or_default()
        };
        for f in types.all_fields(t.id) {
            if let Some(row) = rows
                .rows
                .iter()
                .find(|row| name_of(row).eq_ignore_ascii_case(&f.name))
            {
                out.rows.push(row.clone());
            }
        }
        return Ok(out);
    }
    for x in types.chain(t.id) {
        let rows = r
            .query(&format!(
                "SHOW {full}COLUMNS FROM {}{filter}",
                table_sql(db, &x.name)
            ))
            .await?;
        if out.columns.is_empty() {
            out.columns = rows.columns;
        }
        for row in rows.rows {
            let field = row
                .first()
                .cloned()
                .flatten()
                .map(|v| String::from_utf8_lossy(&v).into_owned())
                .unwrap_or_default();
            if is_type_column(&field) || (!x.is_root() && t.is_key(&field)) {
                continue;
            }
            out.rows.push(row);
        }
    }
    Ok(out)
}

type Row = Vec<Option<Vec<u8>>>;

fn emit(
    row: Row,
    key: &str,
    out: &mut Vec<Row>,
    done: &mut HashSet<String>,
    waiting: &mut HashMap<String, Vec<Row>>,
) {
    out.push(row);
    done.insert(key.to_owned());
    if let Some(kids) = waiting.remove(key) {
        for k in kids {
            let kk = k
                .first()
                .cloned()
                .flatten()
                .map(|v| String::from_utf8_lossy(&v).to_lowercase())
                .unwrap_or_default();
            emit(k, &kk, out, done, waiting);
        }
    }
}

/// Orders table names so each subtype follows its parent, keeping the
/// backend's order otherwise. `rows` are `SHOW [FULL] TABLES` rows.
#[must_use]
pub fn order_tables(
    rows: Vec<Vec<Option<Vec<u8>>>>,
    types: &DbTypes,
    lctn: bool,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let name_of = |row: &Vec<Option<Vec<u8>>>| {
        row.first()
            .cloned()
            .flatten()
            .map(|v| String::from_utf8_lossy(&v).into_owned())
            .unwrap_or_default()
    };
    let parent_of = |name: &str| -> Option<String> {
        let t = types.get(name, lctn)?;
        types.by_id(t.parent?).map(|p| p.name.to_lowercase())
    };
    let present: HashSet<String> = rows.iter().map(|r| name_of(r).to_lowercase()).collect();
    let mut out = Vec::with_capacity(rows.len());
    let mut done: HashSet<String> = HashSet::new();
    let mut waiting: HashMap<String, Vec<Vec<Option<Vec<u8>>>>> = HashMap::new();
    for row in rows {
        let key = name_of(&row).to_lowercase();
        match parent_of(&key) {
            Some(p) if present.contains(&p) && !done.contains(&p) => {
                waiting.entry(p).or_default().push(row);
            }
            _ => emit(row, &key, &mut out, &mut done, &mut waiting),
        }
    }
    // Anything still waiting (a cycle in a corrupt catalog): as is.
    for (_, kids) in waiting {
        out.extend(kids);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::fixtures::assets;

    fn row(n: &str) -> Vec<Option<Vec<u8>>> {
        vec![Some(n.as_bytes().to_vec()), Some(b"BASE TABLE".to_vec())]
    }

    #[test]
    fn subtypes_follow_parents() {
        let db = assets();
        let rows = vec![
            row("building"),
            row("plain"),
            row("truck"),
            row("vehicle"),
            row("asset"),
        ];
        let names: Vec<String> = order_tables(rows, &db, false)
            .iter()
            .map(|r| String::from_utf8_lossy(r[0].as_ref().unwrap()).into_owned())
            .collect();
        assert_eq!(names, ["plain", "asset", "building", "vehicle", "truck"]);
    }
}
