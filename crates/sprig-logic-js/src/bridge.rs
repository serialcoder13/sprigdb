//! The `db` interface of the fallback tier (chapter 41, "The `db`
//! interface"; build step 13 in part): what `Sprig.Db.*` hands to its
//! host, and how statements are checked and bound before they leave.
//!
//! The host runs each statement as the caller would: in the gateway,
//! through the tenant session's own pipeline (name virtualization,
//! chapter 29's privilege checks, the session's backend connection and
//! transaction), never through `sprig_ctl`. So a caller without rights on
//! a table gets the same 1142 from engine-tier code as from a client
//! statement or a compiled routine.
//!
//! Statements are bound here, as text: each `?` (outside strings, quoted
//! names and comments) becomes a literal. Strings are sent as
//! `_utf8mb4 X'…'`, which means the same text under every `sql_mode`
//! (`NO_BACKSLASH_ESCAPES`, `ANSI_QUOTES`), doubles in exponent form so
//! they stay `DOUBLE`, as the compiled tier's bind variables are.

use async_trait::async_trait;
use serde_json::{Map as JMap, Number, Value};
use sprig_common::{Result, SprigCode, SprigError};
use std::ops::ControlFlow;

use sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Query, Statement, Visit, Visitor, visit_relations_mut,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer, Word};

/// What a `Sprig.Db.*` call expects back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// `QueryOne`: at most one row.
    One,
    /// `Query`: rows.
    Many,
    /// `Exec`: affected rows.
    Exec,
}

impl Shape {
    /// The function's name, for messages.
    #[must_use]
    pub const fn function(self) -> &'static str {
        match self {
            Shape::One => "Sprig.Db.QueryOne",
            Shape::Many => "Sprig.Db.Query",
            Shape::Exec => "Sprig.Db.Exec",
        }
    }
}

/// How a result column's text becomes a value (the compiled tier's
/// types: `sprig_logic_compile::sql::ty_of_column`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColKind {
    /// Integer types: a JSON integer.
    Int,
    /// `DECIMAL`, `FLOAT`, `DOUBLE`, `BIGINT UNSIGNED`: a JSON number.
    Float,
    /// Text, dates and everything else: a JSON string.
    Str,
    /// `JSON`: the parsed value.
    Json,
}

/// One result column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// The name (alias) the client would see.
    pub name: String,
    /// Its kind.
    pub kind: ColKind,
}

/// A statement's result.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    /// A resultset, as text.
    Rows {
        /// The columns.
        columns: Vec<Column>,
        /// The rows; `None` is SQL `NULL`.
        rows: Vec<Vec<Option<String>>>,
    },
    /// DML: affected rows.
    Affected(u64),
}

/// One statement for the host to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbOp {
    /// What the caller expects.
    pub shape: Shape,
    /// The statement, bound (no placeholders left), with the code's own
    /// (logical) names.
    pub sql: String,
}

/// The host side of `db`: runs statements as the caller.
#[async_trait]
pub trait DbBridge: Send + Sync {
    /// Runs one statement.
    ///
    /// # Errors
    /// The statement's error, as a client would get it (1142, 1062, ...).
    async fn exec(&self, op: DbOp) -> Result<Reply>;
}

/// A bridge with no data: every call fails with 39862 (code that reaches
/// no database).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoDb;

#[async_trait]
impl DbBridge for NoDb {
    async fn exec(&self, op: DbOp) -> Result<Reply> {
        Err(SprigError::new(
            SprigCode::LogicUnsupported,
            format!("{} has no database here", op.shape.function()),
        ))
    }
}

/// The byte offsets of each `?` placeholder in `sql`, skipping strings,
/// quoted names and comments.
#[must_use]
pub fn placeholders(sql: &str) -> Vec<usize> {
    scan(sql).0
}

/// Whether `sql` has a `;` (outside strings, quoted names and comments)
/// followed by anything but blanks: more than one statement.
#[must_use]
pub fn several_statements(sql: &str) -> bool {
    scan(sql).1.iter().any(|&i| !sql[i + 1..].trim().is_empty())
}

/// Placeholders and semicolons outside strings, names and comments.
fn scan(sql: &str) -> (Vec<usize>, Vec<usize>) {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut semis = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            q @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' && q != b'`' {
                        i += 2;
                        continue;
                    }
                    if b[i] == q {
                        if b.get(i + 1) == Some(&q) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'#' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-')
                && b.get(i + 2).is_none_or(u8::is_ascii_whitespace) =>
            {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
            }
            b'?' => {
                out.push(i);
                i += 1;
            }
            b';' => {
                semis.push(i);
                i += 1;
            }
            _ => i += 1,
        }
    }
    (out, semis)
}

/// The names `WITH` defines in a statement.
struct Ctes(Vec<String>);

impl Visitor for Ctes {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        if let Some(w) = &q.with {
            for c in &w.cte_tables {
                self.0.push(c.alias.name.value.to_ascii_lowercase());
            }
        }
        ControlFlow::Continue(())
    }
}

const MARK: &str = "__sprig_bind_";

/// `sql` with every unqualified table name qualified with `db`: the code's
/// own database, as a compiled routine's default database is its own
/// schema, whatever the caller's current database is. Names `WITH`
/// defines stay as they are. Text sqlparser can't read is returned
/// unchanged (MariaDB then resolves its names in the session's current
/// database).
#[must_use]
pub fn qualify(sql: &str, db: &str) -> String {
    let dialect = MySqlDialect {};
    // Raw string values, so escapes round-trip unchanged; placeholders as
    // marker words, so the parser takes them anywhere a value goes.
    let Ok(tokens) = Tokenizer::new(&dialect, sql)
        .with_unescape(false)
        .tokenize()
    else {
        return sql.to_owned();
    };
    let mut n = 0usize;
    let tokens: Vec<Token> = tokens
        .into_iter()
        .map(|t| match t {
            Token::Placeholder(p) if p == "?" => {
                let w = Token::Word(Word {
                    value: format!("{MARK}{n}"),
                    quote_style: None,
                    keyword: sqlparser::keywords::Keyword::NoKeyword,
                });
                n += 1;
                w
            }
            t => t,
        })
        .collect();
    let Ok(mut stmts) = Parser::new(&dialect).with_tokens(tokens).parse_statements() else {
        return sql.to_owned();
    };
    if stmts.len() != 1 {
        return sql.to_owned();
    }
    let mut stmt = stmts.remove(0);
    let mut ctes = Ctes(Vec::new());
    let _ = stmt.visit(&mut ctes);
    let _ = visit_relations_mut(&mut stmt, |o: &mut ObjectName| {
        if let [ObjectNamePart::Identifier(i)] = o.0.as_slice()
            && !ctes.0.contains(&i.value.to_ascii_lowercase())
        {
            o.0.insert(0, ObjectNamePart::Identifier(Ident::with_quote('`', db)));
        }
        ControlFlow::<()>::Continue(())
    });
    let mut out = stmt.to_string();
    for i in (0..n).rev() {
        out = out.replace(&format!("{MARK}{i}"), "?");
    }
    out
}

/// A value as a SQL literal (see the module docs).
#[must_use]
pub fn literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_owned(),
        Value::Bool(true) => "TRUE".to_owned(),
        Value::Bool(false) => "FALSE".to_owned(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(u) = n.as_u64() {
                u.to_string()
            } else {
                format!("{:e}", n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => hex_string(s),
        other => hex_string(&other.to_string()),
    }
}

fn hex_string(s: &str) -> String {
    use std::fmt::Write;
    if s.is_empty() {
        return "_utf8mb4''".to_owned();
    }
    let mut out = String::with_capacity(12 + s.len() * 2);
    out.push_str("_utf8mb4 X'");
    for b in s.bytes() {
        let _ = write!(out, "{b:02X}");
    }
    out.push('\'');
    out
}

/// Checks that `sql` is one statement of the kind `shape` needs and binds
/// `args` into it.
///
/// # Errors
/// 39874 when the placeholders and `args` disagree; 39866 for text that
/// isn't one query (`QueryOne`, `Query`) or one `INSERT`, `REPLACE`,
/// `UPDATE` or `DELETE` (`Exec`), naming `at`.
pub fn bind(shape: Shape, sql: &str, args: &[Value], at: &str) -> Result<DbOp> {
    let marks = placeholders(sql);
    if marks.len() != args.len() {
        return Err(SprigError::new(
            SprigCode::LogicSignatureMismatch,
            format!(
                "{} at {at}: the SQL has {} placeholders but {} args",
                shape.function(),
                marks.len(),
                args.len()
            ),
        ));
    }
    let kind_ok = !several_statements(sql)
        && match Parser::parse_sql(&MySqlDialect {}, sql) {
            Ok(stmts) if stmts.len() == 1 => matches!(
                (&stmts[0], shape),
                (Statement::Query(_), Shape::One | Shape::Many)
                    | (
                        Statement::Insert(_) | Statement::Update { .. } | Statement::Delete(_),
                        Shape::Exec,
                    )
            ),
            // Several statements.
            Ok(_) => false,
            // Text sqlparser can't read still goes to MariaDB, which has the
            // last word on its syntax; only the kind is checked here.
            Err(_) => first_word_fits(sql, shape),
        };
    if !kind_ok {
        return Err(SprigError::new(
            SprigCode::LogicRuntimeError,
            format!(
                "{} at {at} needs {}",
                shape.function(),
                match shape {
                    Shape::One | Shape::Many => "one SELECT statement",
                    Shape::Exec => "one INSERT, REPLACE, UPDATE or DELETE statement",
                }
            ),
        ));
    }
    let mut out = String::with_capacity(sql.len() + args.len() * 8);
    let mut last = 0;
    for (pos, v) in marks.iter().zip(args) {
        out.push_str(&sql[last..*pos]);
        out.push_str(&literal(v));
        last = pos + 1;
    }
    out.push_str(&sql[last..]);
    Ok(DbOp { shape, sql: out })
}

fn first_word_fits(sql: &str, shape: Shape) -> bool {
    let w: String = sql
        .trim_start_matches(|c: char| c.is_whitespace() || c == '(')
        .chars()
        .take_while(char::is_ascii_alphabetic)
        .collect::<String>()
        .to_ascii_uppercase();
    match shape {
        Shape::One | Shape::Many => matches!(w.as_str(), "SELECT" | "WITH" | "VALUES"),
        Shape::Exec => matches!(w.as_str(), "INSERT" | "REPLACE" | "UPDATE" | "DELETE"),
    }
}

/// A result cell as a value of its column's kind.
///
/// # Errors
/// 39866 when a `JSON` column holds text that isn't JSON.
pub fn cell(text: Option<&str>, kind: ColKind) -> Result<Value> {
    let Some(t) = text else {
        return Ok(Value::Null);
    };
    Ok(match kind {
        ColKind::Int => t
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| t.parse::<f64>().map(float))
            .unwrap_or_else(|_| Value::String(t.to_owned())),
        ColKind::Float => t
            .parse::<f64>()
            .map_or_else(|_| Value::String(t.to_owned()), float),
        ColKind::Str => Value::String(t.to_owned()),
        ColKind::Json => serde_json::from_str(t).map_err(|e| {
            SprigError::new(
                SprigCode::LogicRuntimeError,
                format!("a JSON column holds text that isn't JSON: {e}"),
            )
        })?,
    })
}

fn float(x: f64) -> Value {
    Number::from_f64(x).map_or(Value::Null, Value::Number)
}

/// A row as an object of its columns.
///
/// # Errors
/// See [`cell`].
pub fn row_object(columns: &[Column], row: &[Option<String>]) -> Result<Value> {
    let mut m = JMap::new();
    for (c, v) in columns.iter().zip(row) {
        m.insert(c.name.clone(), cell(v.as_deref(), c.kind)?);
    }
    Ok(Value::Object(m))
}

/// The kind of a column from MySQL's protocol type code and flags
/// (`UNSIGNED_FLAG` is 32), for hosts that read column definitions.
#[must_use]
pub const fn kind_of_type(type_code: u8, unsigned: bool) -> ColKind {
    match type_code {
        // TINY, SHORT, LONG, INT24, YEAR
        1 | 2 | 3 | 9 | 13 => ColKind::Int,
        // LONGLONG: BIGINT UNSIGNED is a double, as in the compiled tier.
        8 => {
            if unsigned {
                ColKind::Float
            } else {
                ColKind::Int
            }
        }
        // DECIMAL, FLOAT, DOUBLE, NEWDECIMAL
        0 | 4 | 5 | 246 => ColKind::Float,
        // JSON (MySQL 245; MariaDB sends it as a string with extended
        // metadata, which the host turns into 245).
        245 => ColKind::Json,
        _ => ColKind::Str,
    }
}

/// The kind of a column from `information_schema.COLUMNS.DATA_TYPE` and
/// `COLUMN_TYPE`, for hosts that know them.
#[must_use]
pub fn kind_of_data_type(data_type: &str, column_type: &str) -> ColKind {
    let dt = data_type.to_ascii_lowercase();
    match dt.as_str() {
        "bigint" if column_type.to_ascii_lowercase().contains("unsigned") => ColKind::Float,
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => {
            ColKind::Int
        }
        "decimal" | "numeric" | "float" | "double" | "real" => ColKind::Float,
        "json" => ColKind::Json,
        _ => ColKind::Str,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use serde_json::json;

    #[test]
    fn placeholders_skip_strings_names_and_comments() {
        let s =
            "SELECT '?', \"?\", `?`, 'it''s ?', 'a\\'?' /* ? */ -- ?\n, ? # ?\n FROM t WHERE a = ?";
        let p = placeholders(s);
        assert_eq!(p.len(), 2, "{p:?}");
        assert!(s[p[0]..].starts_with("? # ?"));
    }

    #[test]
    fn binding_renders_literals() {
        let op = bind(
            Shape::Exec,
            "INSERT INTO t VALUES (?, ?, ?, ?, ?)",
            &[
                json!(7),
                json!(2.5),
                json!("it's"),
                Value::Null,
                json!(true),
            ],
            "step s",
        )
        .unwrap();
        assert_eq!(
            op.sql,
            "INSERT INTO t VALUES (7, 2.5e0, _utf8mb4 X'69742773', NULL, TRUE)"
        );
        let e = bind(Shape::One, "SELECT ?", &[], "step s").unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::LogicSignatureMismatch));
        let e = bind(Shape::One, "DELETE FROM t", &[], "step s").unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::LogicRuntimeError));
        let e = bind(Shape::Exec, "DROP TABLE t", &[], "step s").unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::LogicRuntimeError));
        assert!(bind(Shape::Many, "SELECT 1; SELECT 2", &[], "s").is_err());
        assert!(bind(Shape::Many, "SELECT ';' FROM t;  ", &[], "s").is_ok());
        assert!(several_statements("SELECT 1 /* x */; DROP TABLE t"));
    }

    #[test]
    fn qualifying_names() {
        assert_eq!(
            qualify("SELECT price, stock FROM products WHERE id = ?", "shop"),
            "SELECT price, stock FROM `shop`.products WHERE id = ?"
        );
        assert_eq!(
            qualify(
                "INSERT INTO orders (id, note) VALUES (?, CONCAT('it''s \\ ', ?))",
                "shop"
            ),
            "INSERT INTO `shop`.orders (id, note) VALUES (?, CONCAT('it''s \\ ', ?))"
        );
        assert_eq!(
            qualify(
                "WITH c AS (SELECT id FROM t) SELECT * FROM c JOIN other.u ON 1",
                "shop"
            ),
            "WITH c AS (SELECT id FROM `shop`.t) SELECT * FROM c JOIN other.u ON 1"
        );
        assert_eq!(qualify("SELECT ((( FROM", "shop"), "SELECT ((( FROM");
    }

    #[test]
    fn cells_follow_column_kinds() {
        assert_eq!(cell(Some("7"), ColKind::Int).unwrap(), json!(7));
        assert_eq!(cell(Some("19.99"), ColKind::Float).unwrap(), json!(19.99));
        assert_eq!(cell(Some("[1]"), ColKind::Json).unwrap(), json!([1]));
        assert_eq!(cell(None, ColKind::Str).unwrap(), Value::Null);
        assert_eq!(kind_of_type(246, false), ColKind::Float);
        assert_eq!(kind_of_type(8, true), ColKind::Float);
        assert_eq!(kind_of_data_type("datetime", "datetime"), ColKind::Str);
    }
}
