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
use crate::model::{DbTypes, TypeDef};
use crate::rewrite::{TYPE_COLUMN, TYPE_PATH_COLUMN};
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
    let Some(def) = TableDef::parse(physical, &ctx.lex) else {
        return physical.to_owned();
    };
    let keep: Vec<&str> = def
        .elements
        .iter()
        .filter(|(e, _)| match e {
            Element::Column(c) => !is_type_column(&c.name) && (t.is_root() || !t.is_key(&c.name)),
            Element::PrimaryKey(_) => t.is_root(),
            Element::Other { name, columns, .. } => {
                let n = name.as_deref().unwrap_or("");
                !n.eq_ignore_ascii_case(TYPE_PATH_INDEX)
                    && !n.starts_with(FK_PREFIX)
                    && !columns.iter().any(|c| is_type_column(c))
            }
        })
        .map(|(_, r)| &physical[r.clone()])
        .collect();
    let mut out = String::with_capacity(physical.len());
    out.push_str(&physical[..=def.open]);
    out.push_str("\n  ");
    out.push_str(&keep.join(",\n  "));
    out.push('\n');
    out.push(')');
    if let Some(p) = parent {
        out.push_str(&format!(" INHERITS ({})", quote(p)));
    }
    out.push_str(&physical[def.close + 1..]);
    if t.is_abstract {
        out.push_str(" WITH (SPRIG_ABSTRACT = 1)");
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
    for row in &mut rows.rows {
        if let Some(Some(v)) = row.get_mut(1) {
            let text = String::from_utf8_lossy(v).into_owned();
            *v = logical_create(&text, t, parent.as_deref(), ctx).into_bytes();
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
