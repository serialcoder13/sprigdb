//! Embedded SQL (chapter 41, "Embedded data access"): literal statement
//! text bound against the compile-time catalog and re-rendered from its
//! AST, so it keeps its meaning inside a routine created under
//! `sql_mode=ORACLE` (where `||` concatenates and `"x"` is a name).
//!
//! - `?` placeholders become bind variables, in text order; there is no
//!   dynamic SQL.
//! - A read's select list must name its columns (a column, or any
//!   expression with an alias) and each column must have a type the
//!   catalog knows; `SELECT *` falls back to the engine tier.
//! - Names are unqualified: the routine runs in its shard schema.
//!   Qualified names fall back for now.
//! - An `UPDATE` whose `SET` reads a column an earlier assignment of the
//!   same statement set falls back: routines inherit
//!   `SIMULTANEOUS_ASSIGNMENT` from `ORACLE`, so it would read the old
//!   value where MySQL reads the new one.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, FromTable, ObjectName, ObjectNamePart, SelectItem, SetExpr,
    Statement, TableFactor, TableObject, TableWithJoins, Value as SqlValue, visit_expressions_mut,
    visit_relations,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer, Word};

use crate::Reason;
use crate::ir::{BoundSql, Ty};

/// The column types the compiler can see: one shard schema's tables.
pub trait Columns {
    /// The IR type of `table.column`, if both exist and the type maps.
    fn column(&self, table: &str, column: &str) -> Option<Ty>;
    /// Whether the table exists.
    fn has_table(&self, table: &str) -> bool;
}

/// A fixed map of tables to typed columns.
#[derive(Debug, Clone, Default)]
pub struct StaticColumns {
    tables: BTreeMap<String, BTreeMap<String, Ty>>,
}

impl StaticColumns {
    /// No tables.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a table.
    #[must_use]
    pub fn table(mut self, name: &str, cols: &[(&str, Ty)]) -> Self {
        self.tables.insert(
            name.to_ascii_lowercase(),
            cols.iter()
                .map(|(c, t)| (c.to_ascii_lowercase(), *t))
                .collect(),
        );
        self
    }

    /// Adds one column (from `information_schema.COLUMNS`).
    pub fn add(&mut self, table: &str, column: &str, ty: Ty) {
        self.tables
            .entry(table.to_ascii_lowercase())
            .or_default()
            .insert(column.to_ascii_lowercase(), ty);
    }
}

impl Columns for StaticColumns {
    fn column(&self, table: &str, column: &str) -> Option<Ty> {
        self.tables
            .get(&table.to_ascii_lowercase())?
            .get(&column.to_ascii_lowercase())
            .copied()
    }

    fn has_table(&self, table: &str) -> bool {
        self.tables.contains_key(&table.to_ascii_lowercase())
    }
}

/// MariaDB's `DATA_TYPE` to an IR type (chapter 41: `DECIMAL` is
/// `Float` in both tiers). Dates and times read as text.
#[must_use]
pub fn ty_of_column(data_type: &str, column_type: &str) -> Option<Ty> {
    let dt = data_type.to_ascii_lowercase();
    Some(match dt.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => {
            if column_type.to_ascii_lowercase().contains("unsigned") && dt == "bigint" {
                Ty::Float
            } else {
                Ty::Int
            }
        }
        "decimal" | "numeric" | "float" | "double" | "real" => Ty::Float,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set"
        | "date" | "datetime" | "timestamp" | "time" => Ty::Str,
        "json" => Ty::Json,
        _ => return None,
    })
}

/// What the statement must be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Want {
    /// A query (`Query`, `QueryOne`).
    Read,
    /// DML (`Exec`).
    Write,
}

/// A bound statement.
#[derive(Debug, Clone, PartialEq)]
pub struct Bound {
    /// The statement.
    pub stmt: BoundSql,
    /// The number of bind variables.
    pub binds: usize,
    /// For reads: the select list's names and types.
    pub columns: Vec<(String, Ty)>,
}

const BIND: &str = "__sprig_bind_";

fn reason(code: &str, at: &str, detail: impl Into<String>) -> Reason {
    Reason::new(code, at, detail)
}

/// Binds `text` against `cat`. `at` locates the statement in the source
/// for reasons.
///
/// # Errors
/// A placement [`Reason`] when the statement can't be compiled.
pub fn bind(text: &str, want: Want, cat: &dyn Columns, at: &str) -> Result<Bound, Reason> {
    let dialect = MySqlDialect {};
    // Raw string values, so escapes round-trip unchanged.
    let tokens = Tokenizer::new(&dialect, text)
        .with_unescape(false)
        .tokenize()
        .map_err(|e| reason("db.syntax", at, e.to_string()))?;
    let mut n = 0usize;
    let tokens: Vec<Token> = tokens
        .into_iter()
        .map(|t| match t {
            Token::Placeholder(p) if p == "?" => {
                let w = Token::Word(Word {
                    value: format!("{BIND}{n}"),
                    quote_style: None,
                    keyword: sqlparser::keywords::Keyword::NoKeyword,
                });
                n += 1;
                w
            }
            t => t,
        })
        .collect();
    let mut stmts = Parser::new(&dialect)
        .with_tokens(tokens)
        .parse_statements()
        .map_err(|e| reason("db.syntax", at, e.to_string()))?;
    if stmts.len() != 1 {
        return Err(reason(
            "db.syntax",
            at,
            "the text must hold exactly one statement",
        ));
    }
    let mut stmt = stmts.remove(0);
    // `"x"` is a string in MySQL's default mode and a name in ORACLE's.
    // `||` is OR in MySQL's default mode and concatenation in ORACLE's.
    let _ = visit_expressions_mut(&mut stmt, |e| {
        match e {
            SqlExpr::Value(v) => {
                if let SqlValue::DoubleQuotedString(s) = &v.value {
                    v.value = SqlValue::SingleQuotedString(s.clone());
                }
            }
            SqlExpr::BinaryOp { op, .. } if *op == BinaryOperator::StringConcat => {
                *op = BinaryOperator::Or;
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    let mut names = Vec::new();
    let _ = visit_relations(&stmt, |o: &ObjectName| {
        names.push(o.clone());
        ControlFlow::<()>::Continue(())
    });
    let mut tables = BTreeSet::new();
    for o in &names {
        let parts = name_parts(o);
        if parts.len() != 1 {
            return Err(reason(
                "db.qualified_name",
                at,
                format!("{o}: only unqualified names compile for now"),
            ));
        }
        if !cat.has_table(&parts[0]) {
            return Err(reason("db.unknown_object", at, format!("no table {o}")));
        }
        tables.insert(parts[0].to_ascii_lowercase());
    }
    let (columns, writes) = match (&stmt, want) {
        (Statement::Query(q), Want::Read) => {
            let SetExpr::Select(sel) = q.body.as_ref() else {
                return Err(reason("db.shape", at, "only a plain SELECT compiles"));
            };
            if sel.into.is_some() {
                return Err(reason("db.shape", at, "SELECT ... INTO isn't allowed"));
            }
            let aliases = from_aliases(&sel.from);
            let mut cols = Vec::new();
            for item in &sel.projection {
                cols.push(select_column(item, &aliases, cat, at)?);
            }
            let mut seen = BTreeSet::new();
            for (c, _) in &cols {
                if !seen.insert(c.to_ascii_lowercase()) {
                    return Err(reason("db.shape", at, format!("column {c} appears twice")));
                }
            }
            (cols, Vec::new())
        }
        (Statement::Insert(ins), Want::Write) => {
            let TableObject::TableName(t) = &ins.table else {
                return Err(reason("db.shape", at, "INSERT target"));
            };
            (Vec::new(), vec![name_parts(t).join(".")])
        }
        (Statement::Update(up), Want::Write) => {
            check_update_order(&up.assignments, at)?;
            (Vec::new(), table_names(std::slice::from_ref(&up.table)))
        }
        (Statement::Delete(del), Want::Write) => {
            let from = match &del.from {
                FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
            };
            (Vec::new(), table_names(from))
        }
        (_, Want::Read) => return Err(reason("db.shape", at, "Query and QueryOne take a SELECT")),
        (_, Want::Write) => {
            return Err(reason(
                "db.shape",
                at,
                "Exec takes an INSERT, UPDATE or DELETE",
            ));
        }
    };
    let writes: Vec<String> = writes.into_iter().map(|w| w.to_ascii_lowercase()).collect();
    let reads: Vec<String> = tables
        .iter()
        .filter(|t| !writes.contains(t) || want == Want::Read)
        .cloned()
        .collect();
    let rendered = stmt.to_string();
    let mut parts = Vec::with_capacity(n + 1);
    let mut rest = rendered.as_str();
    for i in 0..n {
        let marker = format!("{BIND}{i}");
        let Some(pos) = rest.find(&marker) else {
            return Err(reason(
                "db.shape",
                at,
                "a bind variable was lost while rendering",
            ));
        };
        parts.push(rest[..pos].to_owned());
        rest = &rest[pos + marker.len()..];
    }
    parts.push(rest.to_owned());
    if parts.iter().any(|p| p.contains(BIND)) {
        return Err(reason("db.shape", at, "bind variables are out of order"));
    }
    Ok(Bound {
        stmt: BoundSql {
            parts,
            reads,
            writes,
        },
        binds: n,
        columns,
    })
}

fn name_parts(o: &ObjectName) -> Vec<String> {
    o.0.iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => i.value.clone(),
            ObjectNamePart::Function(f) => f.name.value.clone(),
        })
        .collect()
}

fn table_names(from: &[TableWithJoins]) -> Vec<String> {
    let mut out = Vec::new();
    for t in from {
        for f in std::iter::once(&t.relation).chain(t.joins.iter().map(|j| &j.relation)) {
            if let TableFactor::Table { name, .. } = f {
                out.push(name_parts(name).join("."));
            }
        }
    }
    out
}

/// Alias (or table name) to table name, lower case.
fn from_aliases(from: &[TableWithJoins]) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    for t in from {
        for f in std::iter::once(&t.relation).chain(t.joins.iter().map(|j| &j.relation)) {
            if let TableFactor::Table { name, alias, .. } = f {
                let table = name_parts(name).join(".").to_ascii_lowercase();
                let key = alias
                    .as_ref()
                    .map_or_else(|| table.clone(), |a| a.name.value.to_ascii_lowercase());
                m.insert(key, table);
            }
        }
    }
    m
}

fn column_type(e: &SqlExpr, aliases: &BTreeMap<String, String>, cat: &dyn Columns) -> Option<Ty> {
    match e {
        SqlExpr::Identifier(id) => {
            // The one table that has it.
            let found: Vec<Ty> = aliases
                .values()
                .filter_map(|t| cat.column(t, &id.value))
                .collect();
            (found.len() == 1).then(|| found[0])
        }
        SqlExpr::CompoundIdentifier(ids) if ids.len() == 2 => {
            let t = aliases.get(&ids[0].value.to_ascii_lowercase())?;
            cat.column(t, &ids[1].value)
        }
        SqlExpr::Nested(e) => column_type(e, aliases, cat),
        SqlExpr::Function(f) => {
            let name = f.name.to_string().to_ascii_uppercase();
            match name.as_str() {
                "COUNT" => Some(Ty::Int),
                "SUM" | "AVG" => Some(Ty::Float),
                _ => None,
            }
        }
        SqlExpr::Value(v) => match &v.value {
            SqlValue::Number(s, _) => Some(if s.contains(['.', 'e', 'E']) {
                Ty::Float
            } else {
                Ty::Int
            }),
            SqlValue::SingleQuotedString(_) => Some(Ty::Str),
            _ => None,
        },
        _ => None,
    }
}

fn select_column(
    item: &SelectItem,
    aliases: &BTreeMap<String, String>,
    cat: &dyn Columns,
    at: &str,
) -> Result<(String, Ty), Reason> {
    let (e, name) = match item {
        SelectItem::UnnamedExpr(e) => {
            let name = match e {
                SqlExpr::Identifier(id) => id.value.clone(),
                SqlExpr::CompoundIdentifier(ids) => {
                    ids.last().map(|i| i.value.clone()).unwrap_or_default()
                }
                _ => {
                    return Err(reason(
                        "db.unnamed_column",
                        at,
                        format!("give {e} a name with AS"),
                    ));
                }
            };
            (e, name)
        }
        SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
        _ => {
            return Err(reason(
                "db.select_star",
                at,
                "name the columns instead of *",
            ));
        }
    };
    let Some(ty) = column_type(e, aliases, cat) else {
        return Err(reason(
            "db.untyped_column",
            at,
            format!("can't type column {name}"),
        ));
    };
    Ok((name, ty))
}

fn check_update_order(assignments: &[sqlparser::ast::Assignment], at: &str) -> Result<(), Reason> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    for a in assignments {
        let mut reads = Vec::new();
        let mut v = a.value.clone();
        let _ = visit_expressions_mut(&mut v, |e| {
            match e {
                SqlExpr::Identifier(id) => reads.push(id.value.to_ascii_lowercase()),
                SqlExpr::CompoundIdentifier(ids) => {
                    if let Some(l) = ids.last() {
                        reads.push(l.value.to_ascii_lowercase());
                    }
                }
                _ => {}
            }
            ControlFlow::<()>::Continue(())
        });
        if let Some(r) = reads.iter().find(|r| set.contains(*r)) {
            return Err(reason(
                "db.update_order",
                at,
                format!("SET reads {r} after assigning it; MySQL and ORACLE mode disagree"),
            ));
        }
        let target = a.target.to_string();
        let col = target
            .rsplit('.')
            .next()
            .unwrap_or(&target)
            .trim_matches('`');
        set.insert(col.to_ascii_lowercase());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cat() -> StaticColumns {
        StaticColumns::new()
            .table(
                "products",
                &[
                    ("id", Ty::Int),
                    ("price", Ty::Float),
                    ("stock", Ty::Int),
                    ("name", Ty::Str),
                ],
            )
            .table(
                "orders",
                &[("id", Ty::Int), ("product_id", Ty::Int), ("qty", Ty::Int)],
            )
    }

    #[test]
    fn reads_bind_and_render() {
        let b = bind(
            "SELECT price, stock FROM products WHERE id = ? AND name = \"a\" || ?",
            Want::Read,
            &cat(),
            "step item",
        )
        .unwrap();
        assert_eq!(b.binds, 2);
        assert_eq!(
            b.columns,
            vec![("price".into(), Ty::Float), ("stock".into(), Ty::Int)]
        );
        // `||` is re-rendered as OR, and "a" as a string.
        assert_eq!(
            b.stmt.with_placeholders(),
            "SELECT price, stock FROM products WHERE id = ? AND name = 'a' OR ?"
        );
        assert_eq!(b.stmt.reads, vec!["products".to_owned()]);
    }

    #[test]
    fn joins_aliases_and_aggregates() {
        let b = bind(
            "SELECT p.name, o.qty AS q, COUNT(*) AS n FROM orders o JOIN products p ON p.id = o.product_id WHERE o.id = ?",
            Want::Read,
            &cat(),
            "s",
        )
        .unwrap();
        assert_eq!(
            b.columns,
            vec![
                ("name".into(), Ty::Str),
                ("q".into(), Ty::Int),
                ("n".into(), Ty::Int)
            ]
        );
    }

    #[test]
    fn writes() {
        let b = bind(
            "UPDATE products SET stock = stock - ? WHERE id = ?",
            Want::Write,
            &cat(),
            "s",
        )
        .unwrap();
        assert_eq!(b.stmt.writes, vec!["products".to_owned()]);
        assert_eq!(b.binds, 2);
        let b = bind(
            "INSERT INTO orders (id, product_id, qty) VALUES (?, ?, ?)",
            Want::Write,
            &cat(),
            "s",
        )
        .unwrap();
        assert_eq!(b.stmt.parts.len(), 4);
        let b = bind("DELETE FROM orders WHERE id = ?", Want::Write, &cat(), "s").unwrap();
        assert_eq!(b.stmt.writes, vec!["orders".to_owned()]);
    }

    #[test]
    fn refusals_are_reasons() {
        let r = |s: &str, w| bind(s, w, &cat(), "s").unwrap_err().code;
        assert_eq!(r("SELECT * FROM products", Want::Read), "db.select_star");
        assert_eq!(
            r("SELECT price + 1 FROM products", Want::Read),
            "db.unnamed_column"
        );
        assert_eq!(r("SELECT x FROM nothere", Want::Read), "db.unknown_object");
        assert_eq!(
            r("SELECT price FROM shop.products", Want::Read),
            "db.qualified_name"
        );
        assert_eq!(
            r("UPDATE products SET stock = 1, price = stock", Want::Write),
            "db.update_order"
        );
        assert_eq!(r("SELECT 1; SELECT 2", Want::Read), "db.syntax");
        assert_eq!(r("DELETE FROM products", Want::Read), "db.shape");
    }

    #[test]
    fn strings_keep_their_escapes() {
        let b = bind(
            "SELECT name FROM products WHERE name = 'a\\\\b''c' AND id = ?",
            Want::Read,
            &cat(),
            "s",
        )
        .unwrap();
        assert!(
            b.stmt.parts[0].contains("'a\\\\b''c'"),
            "{:?}",
            b.stmt.parts
        );
    }
}
