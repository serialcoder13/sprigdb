//! `information_schema` as schema tools should see typed tables (chapter
//! 32, "Polymorphic SELECT", the metadata paragraph).
//!
//! In a query of a tenant with types, each reference to
//! `information_schema.COLUMNS` becomes a derived table of the same name
//! (or alias): the backend's rows without the subtypes' physical tables and
//! without the roots' `_type` and `_type_path`, plus, for each subtype, its
//! logical columns, inherited first, under its own name and with its own
//! `ORDINAL_POSITION`. `STATISTICS`, `KEY_COLUMN_USAGE`, `TABLE_CONSTRAINTS`
//! and `REFERENTIAL_CONSTRAINTS` lose the rows of the `_sprig_type_path`
//! index and the `_sprig_fk_` foreign keys. Chapter 07 then maps each inner
//! reference as it maps any other, so the persona's shape and the tenant's
//! restriction apply unchanged.
//!
//! The subtype branches list the columns by name, so they need the
//! persona's column list, which the caller reads with `SELECT * FROM
//! information_schema.COLUMNS WHERE 1 = 0`.

use std::ops::ControlFlow;
use std::sync::Arc;

use sprig_common::names::quote;
use sqlparser::ast::{
    Ident, ObjectNamePart, Query, Statement, TableAlias, TableFactor, VisitMut, VisitorMut,
};

use crate::model::DbTypes;
use crate::rewrite::{TYPE_COLUMN, TYPE_PATH_COLUMN, parse_one, quote_str};

/// The views this module rewrites, with the column naming internal
/// objects.
const FILTERED: [(&str, &str); 4] = [
    ("STATISTICS", "INDEX_NAME"),
    ("KEY_COLUMN_USAGE", "CONSTRAINT_NAME"),
    ("TABLE_CONSTRAINTS", "CONSTRAINT_NAME"),
    ("REFERENTIAL_CONSTRAINTS", "CONSTRAINT_NAME"),
];

/// Whether identifiers of a statement name a view this module rewrites.
#[must_use]
pub fn mentions(idents: &[String]) -> bool {
    idents
        .iter()
        .any(|i| i.eq_ignore_ascii_case("information_schema"))
        && idents.iter().any(|i| {
            i.eq_ignore_ascii_case("COLUMNS")
                || FILTERED.iter().any(|(v, _)| i.eq_ignore_ascii_case(v))
        })
}

/// Whether the statement reads `information_schema.COLUMNS`.
#[must_use]
pub fn reads_columns(stmt: &mut Statement) -> bool {
    let mut found = false;
    let mut v = Find(&mut found);
    let _ = stmt.visit(&mut v);
    found
}

struct Find<'a>(&'a mut bool);

impl VisitorMut for Find<'_> {
    type Break = ();
    fn pre_visit_table_factor(&mut self, f: &mut TableFactor) -> ControlFlow<()> {
        if view_of(f).is_some_and(|v| v.eq_ignore_ascii_case("COLUMNS")) {
            *self.0 = true;
        }
        ControlFlow::Continue(())
    }
}

fn view_of(f: &TableFactor) -> Option<String> {
    let TableFactor::Table { name, .. } = f else {
        return None;
    };
    let parts: Vec<&str> = name
        .0
        .iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.as_str()),
            ObjectNamePart::Function(_) => None,
        })
        .collect();
    match parts.as_slice() {
        [db, v] if db.eq_ignore_ascii_case("information_schema") => Some(v.to_uppercase()),
        _ => None,
    }
}

/// The `COLUMNS` view's text for the tenant's typed databases (logical
/// name, types). `columns`: the persona's column list.
#[must_use]
pub fn columns_view(dbs: &[(String, Arc<DbTypes>)], columns: &[String]) -> String {
    let lit = |s: &str| quote_str(s, false);
    let mut hide = Vec::new();
    let mut branches = Vec::new();
    for (db, types) in dbs {
        let roots: Vec<String> = types
            .iter()
            .filter(|t| t.is_root())
            .map(|t| lit(&t.name))
            .collect();
        let subs: Vec<String> = types
            .iter()
            .filter(|t| !t.is_root())
            .map(|t| lit(&t.name))
            .collect();
        if roots.is_empty() {
            continue;
        }
        let mut cond = format!(
            "(TABLE_NAME IN ({}) AND COLUMN_NAME IN ({}, {}))",
            roots.join(", "),
            lit(TYPE_COLUMN),
            lit(TYPE_PATH_COLUMN)
        );
        if !subs.is_empty() {
            cond = format!("TABLE_NAME IN ({}) OR {cond}", subs.join(", "));
        }
        hide.push(format!("(TABLE_SCHEMA = {} AND ({cond}))", lit(db)));
        let mut subtypes: Vec<_> = types.iter().filter(|t| !t.is_root()).cloned().collect();
        subtypes.sort_by(|a, b| a.name.cmp(&b.name));
        for t in subtypes {
            let fields = types.all_fields(t.id);
            let mut picks = Vec::new();
            let mut cases = Vec::new();
            for (i, f) in fields.iter().enumerate() {
                cases.push(format!(
                    "WHEN {} THEN {}",
                    lit(&format!("{}/{}", f.owner_table, f.name)),
                    i + 1
                ));
            }
            let mut by_table: Vec<(String, Vec<String>)> = Vec::new();
            for f in &fields {
                match by_table.iter_mut().find(|(n, _)| *n == f.owner_table) {
                    Some((_, v)) => v.push(lit(&f.name)),
                    None => by_table.push((f.owner_table.clone(), vec![lit(&f.name)])),
                }
            }
            for (table, cols) in &by_table {
                picks.push(format!(
                    "(TABLE_NAME = {} AND COLUMN_NAME IN ({}))",
                    lit(table),
                    cols.join(", ")
                ));
            }
            let select: Vec<String> = columns
                .iter()
                .map(|c| {
                    if c.eq_ignore_ascii_case("TABLE_NAME") {
                        format!("{} AS {}", lit(&t.name), quote(c))
                    } else if c.eq_ignore_ascii_case("ORDINAL_POSITION") {
                        format!(
                            "CASE CONCAT(TABLE_NAME, '/', COLUMN_NAME) {} END AS {}",
                            cases.join(" "),
                            quote(c)
                        )
                    } else {
                        quote(c)
                    }
                })
                .collect();
            branches.push(format!(
                "SELECT {} FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {} AND ({})",
                select.join(", "),
                lit(db),
                picks.join(" OR ")
            ));
        }
    }
    let mut s = String::from("SELECT * FROM information_schema.COLUMNS");
    if !hide.is_empty() {
        s.push_str(&format!(" WHERE NOT ({})", hide.join(" OR ")));
    }
    for b in branches {
        s.push_str(" UNION ALL ");
        s.push_str(&b);
    }
    s
}

fn filtered_view(view: &str, col: &str) -> String {
    format!(
        "SELECT * FROM information_schema.{view} WHERE {col} IS NULL OR LEFT({col}, 7) <> '_sprig_'"
    )
}

/// Replaces the views' references in `stmt`. `columns_sql`: the `COLUMNS`
/// view's text, when the statement reads it.
pub fn rewrite(stmt: &mut Statement, columns_sql: Option<&str>) -> bool {
    struct R<'a> {
        columns: Option<&'a str>,
        changed: bool,
    }
    impl VisitorMut for R<'_> {
        type Break = ();
        fn post_visit_table_factor(&mut self, f: &mut TableFactor) -> ControlFlow<()> {
            let Some(view) = view_of(f) else {
                return ControlFlow::Continue(());
            };
            let sql = if view == "COLUMNS" {
                match self.columns {
                    Some(s) => s.to_owned(),
                    None => return ControlFlow::Continue(()),
                }
            } else if let Some((_, col)) = FILTERED.iter().find(|(v, _)| *v == view) {
                filtered_view(&view, col)
            } else {
                return ControlFlow::Continue(());
            };
            let TableFactor::Table { alias, name, .. } = f else {
                return ControlFlow::Continue(());
            };
            let alias = alias.clone().unwrap_or_else(|| TableAlias {
                explicit: true,
                name: match name.0.last() {
                    Some(ObjectNamePart::Identifier(i)) => i.clone(),
                    _ => Ident::new(view.clone()),
                },
                columns: Vec::new(),
                at: None,
            });
            let Ok(Statement::Query(q)) = parse_one(&sql) else {
                return ControlFlow::Continue(());
            };
            let q: Box<Query> = q;
            *f = TableFactor::Derived {
                lateral: false,
                subquery: q,
                alias: Some(alias),
                sample: None,
            };
            self.changed = true;
            ControlFlow::Continue(())
        }
    }
    let mut r = R {
        columns: columns_sql,
        changed: false,
    };
    let _ = stmt.visit(&mut r);
    r.changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::fixtures::assets;

    #[test]
    fn subtypes_get_their_logical_columns() {
        let cols: Vec<String> = [
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION",
        ]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
        let v = columns_view(&[("shop".into(), Arc::new(assets()))], &cols);
        assert!(v.starts_with(
            "SELECT * FROM information_schema.COLUMNS WHERE NOT ((TABLE_SCHEMA = 'shop' AND \
             (TABLE_NAME IN ("
        ));
        assert!(
            v.contains(
                "SELECT `TABLE_SCHEMA`, 'truck' AS `TABLE_NAME`, `COLUMN_NAME`, CASE \
             CONCAT(TABLE_NAME, '/', COLUMN_NAME) WHEN 'asset/id' THEN 1 WHEN 'asset/name' THEN 2 \
             WHEN 'asset/site_id' THEN 3 WHEN 'vehicle/vin' THEN 4 WHEN 'truck/payload_kg' THEN 5 \
             END AS `ORDINAL_POSITION` FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'shop' \
             AND ((TABLE_NAME = 'asset' AND COLUMN_NAME IN ('id', 'name', 'site_id')) OR \
             (TABLE_NAME = 'vehicle' AND COLUMN_NAME IN ('vin')) OR (TABLE_NAME = 'truck' AND \
             COLUMN_NAME IN ('payload_kg')))"
            ),
            "{v}"
        );
        parse_one(&v).unwrap();
    }

    #[test]
    fn references_become_derived_tables() {
        let mut s = parse_one(
            "SELECT c.COLUMN_NAME FROM information_schema.COLUMNS c JOIN \
             information_schema.STATISTICS ON 1 WHERE c.TABLE_NAME = 'truck'",
        )
        .unwrap();
        assert!(reads_columns(&mut s));
        assert!(rewrite(
            &mut s,
            Some("SELECT * FROM information_schema.COLUMNS")
        ));
        let out = s.to_string();
        assert!(
            out.contains("FROM (SELECT * FROM information_schema.COLUMNS) c"),
            "{out}"
        );
        assert!(
            out.contains("LEFT(INDEX_NAME, 7) <> '_sprig_') AS STATISTICS"),
            "{out}"
        );
        assert!(mentions(&["information_schema".into(), "COLUMNS".into()]));
    }
}
