//! Validations, after-commit triggers' row binding, and computed fields.
//!
//! **Binding a row to a body.** A validation's or trigger's parameters are
//! bound by name: a parameter named after a column of the table gets the
//! row's value after the write (for a `DELETE`, the deleted row's);
//! `old_<column>` gets the value before the write (`NULL` for an
//! `INSERT`); `op` gets `insert`, `update` or `delete`. Any other name is
//! 39874.
//!
//! **Enforcement** of a compiled validation ([`plan_validation`]):
//!
//! ```text
//! FOR INSERT, UPDATE, no old_ or op parameter, and the body is one
//! expression whose only failure is `reject` with one constant message
//!     → CHECK (NOT (<reject condition>)) named sprig_lc<binding>_v<version>,
//!       added through the ALTER TABLE path; the gateway maps 4025
//!       on that name to 39863 with the message
//! otherwise
//!     → BEFORE INSERT / UPDATE triggers __sprig_lt<binding>_v<version>_b<i|u>
//!       calling the entry procedure; a `reject` event (or an `error`
//!       event) becomes SIGNAL 39863 with its `message`
//! ```
//!
//! Triggers carry the version in their names
//! (`__sprig_lt<b>_v<v>_b<i|u>`), so a new version's triggers are created next
//! to the old ones and the old ones dropped once every shard has the new
//! ones: for a moment both run, which is stricter, never weaker.
//!
//! A validation placed in the engine tier would need the transaction's
//! pre-commit hook, which isn't built: it is refused with
//! 39862.

use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_compile::ir::{Function, Ty};
use sprig_logic_compile::plsql::quote;
use sprig_logic_compile::pure::{self, Sql};
use sprig_logic_compile::{Columns, StaticColumns};

/// What one parameter is bound to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bound {
    /// The row after the write (the deleted row for a delete).
    New(String),
    /// The row before the write.
    Old(String),
    /// The operation.
    Op,
}

/// Binds `f`'s parameters to `table`'s columns by name (see the module
/// docs).
///
/// # Errors
/// 39874 for a parameter that names no column.
pub fn bind_row(f: &Function, table: &str, cols: &StaticColumns) -> Result<Vec<Bound>> {
    f.params
        .iter()
        .map(|p| {
            let n = p.name.to_ascii_lowercase();
            if cols.column(table, &n).is_some() {
                Ok(Bound::New(n))
            } else if let Some(c) = n.strip_prefix("old_")
                && cols.column(table, c).is_some()
            {
                Ok(Bound::Old(c.to_owned()))
            } else if n == "op" {
                Ok(Bound::Op)
            } else {
                Err(SprigError::new(
                    SprigCode::LogicSignatureMismatch,
                    format!(
                        "parameter {} names no column of {table} (use a column, old_<column> or op)",
                        p.name
                    ),
                ))
            }
        })
        .collect()
}

fn bq(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// How a validation is enforced.
#[derive(Debug, Clone, PartialEq)]
pub enum Enforcement {
    /// A `CHECK` constraint: its name, expression and message.
    Check {
        /// `sprig_lc<binding>_v<version>`.
        name: String,
        /// The constraint's expression (true when the row is accepted).
        expr: String,
        /// The validation's message.
        message: String,
    },
    /// `BEFORE` triggers: one per event.
    Triggers,
}

impl Enforcement {
    /// The catalog's `logic_bindings.enforcement`.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Enforcement::Check { .. } => "check",
            Enforcement::Triggers => "trigger",
        }
    }
}

/// The `CHECK` constraint's name of a validation version.
#[must_use]
pub fn check_name(binding: u64, version: u64) -> String {
    format!("sprig_lc{binding}_v{version}")
}

/// Chooses a compiled validation's enforcement (see the module docs).
#[must_use]
pub fn plan_validation(
    f: &Function,
    bound: &[Bound],
    events: &[String],
    binding: u64,
    version: u64,
) -> (Enforcement, Option<String>) {
    let both = events.iter().any(|e| e == "insert") && events.iter().any(|e| e == "update");
    if !both {
        return (
            Enforcement::Triggers,
            Some("a CHECK covers INSERT and UPDATE together".into()),
        );
    }
    let mut row: Vec<Option<Sql>> = Vec::with_capacity(bound.len());
    for (b, p) in bound.iter().zip(&f.params) {
        match b {
            Bound::New(c) => row.push(Some(Sql {
                text: bq(c),
                ty: p.ty,
            })),
            Bound::Old(_) | Bound::Op => {
                return (
                    Enforcement::Triggers,
                    Some("it reads the row before the write or the operation".into()),
                );
            }
        }
    }
    match pure::reject_check(f, &row) {
        Ok((cond, message)) => (
            Enforcement::Check {
                name: check_name(binding, version),
                expr: format!("NOT ({cond})"),
                message,
            },
            None,
        ),
        Err(why) => (Enforcement::Triggers, Some(why)),
    }
}

/// `ALTER TABLE` text (logical names, for the schema-change path) that adds
/// version `new`'s `CHECK` and drops `old` ones, in one statement.
#[must_use]
pub fn check_alter(db: &str, table: &str, add: Option<(&str, &str)>, drop: &[String]) -> String {
    let mut parts: Vec<String> = drop
        .iter()
        .map(|n| format!("DROP CONSTRAINT IF EXISTS {}", bq(n)))
        .collect();
    if let Some((name, expr)) = add {
        parts.push(format!("ADD CONSTRAINT {} CHECK ({expr})", bq(name)));
    }
    format!("ALTER TABLE {}.{} {}", bq(db), bq(table), parts.join(", "))
}

/// The validation triggers' names of a version, with their events.
#[must_use]
pub fn trigger_names(binding: u64, version: u64, events: &[String]) -> Vec<(String, &'static str)> {
    let mut out = Vec::new();
    for (ev, tag) in [("insert", "bi"), ("update", "bu"), ("delete", "bd")] {
        if events.iter().any(|e| e == ev) {
            out.push((format!("__sprig_lt{binding}_v{version}_{tag}"), ev));
        }
    }
    out
}

/// One argument of the entry procedure, inside a `BEFORE` trigger.
fn trigger_arg(b: &Bound, event: &str) -> String {
    match (b, event) {
        (Bound::Op, _) => format!("'{event}'"),
        (Bound::New(c), "delete") | (Bound::Old(c), "update" | "delete") => {
            format!("OLD.{}", bq(c))
        }
        (Bound::New(c), _) => format!("NEW.{}", bq(c)),
        (Bound::Old(_), _) => "NULL".to_owned(),
    }
}

/// A validation's `BEFORE` trigger (created in the default `sql_mode`):
/// one `CALL` of the entry procedure, then `SIGNAL 39863` for a `reject`
/// or `error` event.
#[must_use]
pub fn validation_trigger(
    schema: &str,
    name: &str,
    table: &str,
    event: &str,
    entry: &str,
    bound: &[Bound],
) -> String {
    let mut args: Vec<String> = bound.iter().map(|b| trigger_arg(b, event)).collect();
    args.push("v__event".into());
    args.push("v__result".into());
    format!(
        "CREATE OR REPLACE TRIGGER {}.{} BEFORE {} ON {}.{} FOR EACH ROW\n\
         BEGIN\n  DECLARE v__event VARCHAR(64);\n  DECLARE v__result LONGTEXT;\n  DECLARE v__msg VARCHAR(128);\n  \
         CALL {}({});\n  IF v__event IN ('reject', 'error') THEN\n    \
         SET v__msg = LEFT(COALESCE(JSON_VALUE(v__result, '$.message'), {}), 128);\n    \
         SIGNAL SQLSTATE 'HY000' SET MYSQL_ERRNO = {}, MESSAGE_TEXT = v__msg;\n  END IF;\nEND",
        bq(schema),
        bq(name),
        event.to_ascii_uppercase(),
        bq(schema),
        bq(table),
        bq(entry),
        args.join(", "),
        quote(&format!("rejected by validation on {table}")),
        SprigCode::LogicValidationFailed.number(),
    )
}

/// The exactly-once markers of compiled after-commit triggers; the mover copies it with the shard.
#[must_use]
pub fn done_table(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.`__sprig_ldone` (\n  binding_id BIGINT UNSIGNED NOT NULL,\n  \
         event_key VARBINARY(64) NOT NULL,\n  done_at DATETIME(6) NOT NULL,\n  \
         PRIMARY KEY (binding_id, event_key),\n  KEY by_time (done_at)\n) ENGINE=InnoDB",
        bq(schema)
    )
}

/// A computed field: `ALTER TABLE t ADD [COLUMN] c TYPE AS
/// (f(a, b)) [VIRTUAL | STORED | PERSISTENT] ...` where `f` is a logic
/// function: the call's place in the text and its arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputedCall {
    /// The table, as written.
    pub table: String,
    /// The table's database, if qualified.
    pub db: Option<String>,
    /// The new column.
    pub column: String,
    /// The function (bare or `db.f`).
    pub function: String,
    /// The function's database, if qualified.
    pub function_db: Option<String>,
    /// The arguments' text.
    pub args: Vec<String>,
    /// Byte range of `f(...)` in the statement.
    pub span: (usize, usize),
    /// `STORED` or `PERSISTENT` was given.
    pub stored: bool,
}

fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

/// An identifier at `i` (bare or backquoted): its text and the end.
fn ident_at(s: &str, i: usize) -> Option<(String, usize)> {
    let b = s.as_bytes();
    if b.get(i) == Some(&b'`') {
        let mut out = String::new();
        let mut j = i + 1;
        while j < b.len() {
            if b[j] == b'`' {
                if b.get(j + 1) == Some(&b'`') {
                    out.push('`');
                    j += 2;
                    continue;
                }
                return Some((out, j + 1));
            }
            let ch = s[j..].chars().next()?;
            out.push(ch);
            j += ch.len_utf8();
        }
        return None;
    }
    let start = i;
    let mut j = i;
    while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_' || b[j] == b'$') {
        j += 1;
    }
    (j > start).then(|| (s[start..j].to_owned(), j))
}

/// A possibly qualified name at `i`.
fn qualified_at(s: &str, i: usize) -> Option<(Option<String>, String, usize)> {
    let (a, j) = ident_at(s, i)?;
    let b = s.as_bytes();
    if b.get(j) == Some(&b'.') {
        let (n, k) = ident_at(s, j + 1)?;
        return Some((Some(a), n, k));
    }
    Some((None, a, j))
}

fn word_at(s: &str, i: usize, w: &str) -> bool {
    s.len() >= i + w.len()
        && s[i..i + w.len()].eq_ignore_ascii_case(w)
        && s.as_bytes()
            .get(i + w.len())
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || *c == b'_'))
}

/// Cheap test: could `sql` add a computed field?
#[must_use]
pub fn is_computed_candidate(sql: &str) -> bool {
    let head: String = sql
        .chars()
        .take(8192)
        .collect::<String>()
        .to_ascii_lowercase();
    let t = head.trim_start();
    t.starts_with("alter") && t.contains(" table ") && t.contains(" add ") && t.contains(" as")
}

/// Splits a call's argument text at top-level commas.
#[allow(clippy::match_same_arms)] // quoted and unquoted text, kept apart for clarity
fn split_args(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for ch in s.chars() {
        match (quote, ch) {
            (Some(q), c) if c == q => {
                quote = None;
                cur.push(c);
            }
            (Some(_), c) => cur.push(c),
            (None, '\'' | '"' | '`') => {
                quote = Some(ch);
                cur.push(ch);
            }
            (None, '(') => {
                depth += 1;
                cur.push(ch);
            }
            (None, ')') => {
                depth -= 1;
                cur.push(ch);
            }
            (None, ',') if depth == 0 => {
                out.push(cur.trim().to_owned());
                cur.clear();
            }
            (None, c) => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_owned());
    }
    out
}

/// Parses `ALTER TABLE [db.]t ADD [COLUMN] c <type> [GENERATED ALWAYS]
/// AS (f(args)) [rest]`; `None` for anything else (ordinary DDL).
#[must_use]
#[allow(clippy::match_same_arms)]
pub fn parse_computed(sql: &str) -> Option<ComputedCall> {
    let b = sql.as_bytes();
    let mut i = skip_ws(b, 0);
    if !word_at(sql, i, "alter") {
        return None;
    }
    i = skip_ws(b, i + 5);
    if word_at(sql, i, "online") {
        i = skip_ws(b, i + 6);
    }
    if word_at(sql, i, "ignore") {
        i = skip_ws(b, i + 6);
    }
    if !word_at(sql, i, "table") {
        return None;
    }
    i = skip_ws(b, i + 5);
    let (db, table, j) = qualified_at(sql, i)?;
    i = skip_ws(b, j);
    if !word_at(sql, i, "add") {
        return None;
    }
    i = skip_ws(b, i + 3);
    if word_at(sql, i, "column") {
        i = skip_ws(b, i + 6);
    }
    let (column, j) = ident_at(sql, i)?;
    // The type, up to AS: words and a parenthesized length.
    let mut k = j;
    let as_at = loop {
        k = skip_ws(b, k);
        if k >= b.len() {
            return None;
        }
        if word_at(sql, k, "as") {
            break k;
        }
        if b[k] == b'(' {
            let close = sql[k..].find(')')?;
            k += close + 1;
            continue;
        }
        let (_, e) = ident_at(sql, k)?;
        k = e;
    };
    let mut i = skip_ws(b, as_at + 2);
    if b.get(i) != Some(&b'(') {
        return None;
    }
    i = skip_ws(b, i + 1);
    let fstart = i;
    let (function_db, function, j) = qualified_at(sql, i)?;
    let open = skip_ws(b, j);
    if b.get(open) != Some(&b'(') {
        return None;
    }
    // Matching close parenthesis.
    let mut depth = 0i32;
    let mut q: Option<u8> = None;
    let mut close = None;
    for (n, &c) in b.iter().enumerate().skip(open) {
        match (q, c) {
            (Some(x), c) if c == x => q = None,
            (Some(_), _) => {}
            (None, b'\'' | b'"' | b'`') => q = Some(c),
            (None, b'(') => depth += 1,
            (None, b')') => {
                depth -= 1;
                if depth == 0 {
                    close = Some(n);
                    break;
                }
            }
            _ => {}
        }
    }
    let close = close?;
    let after = skip_ws(b, close + 1);
    if b.get(after) != Some(&b')') {
        return None;
    }
    let rest = skip_ws(b, after + 1);
    let stored = word_at(sql, rest, "stored") || word_at(sql, rest, "persistent");
    Some(ComputedCall {
        table,
        db,
        column,
        function,
        function_db,
        args: split_args(&sql[open + 1..close]),
        span: (fstart, close + 1),
        stored,
    })
}

/// A computed field's expression: `f`'s body with its parameters bound to
/// the call's argument texts (typed by the parameters).
///
/// # Errors
/// 39874 when the argument count is wrong; 39862 when the body has no
/// expression form (a computed field through a routine or the gateway
/// isn't built).
pub fn computed_expr(f: &Function, call: &ComputedCall) -> Result<String> {
    if call.args.len() != f.params.len() {
        return Err(SprigError::new(
            SprigCode::LogicSignatureMismatch,
            format!(
                "{} takes {} arguments, the computed field passes {}",
                call.function,
                f.params.len(),
                call.args.len()
            ),
        ));
    }
    let args: Vec<Option<Sql>> = call
        .args
        .iter()
        .zip(&f.params)
        .map(|(a, p)| {
            Some(Sql {
                text: format!("({a})"),
                ty: if p.ty == Ty::Null { Ty::Float } else { p.ty },
            })
        })
        .collect();
    pure::computed(f, &args).map(|s| s.text).map_err(|why| {
        SprigError::new(
            SprigCode::LogicUnsupported,
            format!(
                "{} can't be a generated column ({why}); computed fields through a routine aren't built yet",
                call.function
            ),
        )
    })
}

/// The statement with `f(...)` replaced by `expr`.
#[must_use]
pub fn rewrite_computed(sql: &str, call: &ComputedCall, expr: &str) -> String {
    format!("{}{expr}{}", &sql[..call.span.0], &sql[call.span.1..])
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn computed_statements_parse() {
        let s = "ALTER TABLE shop.orders ADD COLUMN net DECIMAL(12, 2) AS (shop.net(price, qty * 2)) STORED";
        let c = parse_computed(s).unwrap();
        assert_eq!(c.db.as_deref(), Some("shop"));
        assert_eq!(c.table, "orders");
        assert_eq!(c.column, "net");
        assert_eq!(c.function, "net");
        assert_eq!(c.function_db.as_deref(), Some("shop"));
        assert_eq!(c.args, ["price", "qty * 2"]);
        assert!(c.stored);
        assert_eq!(
            rewrite_computed(s, &c, "(price * qty)"),
            "ALTER TABLE shop.orders ADD COLUMN net DECIMAL(12, 2) AS ((price * qty)) STORED"
        );
        let v =
            parse_computed("alter table `o` add `n` double generated always as (`f`(a)) virtual")
                .unwrap();
        assert_eq!(v.function, "f");
        assert!(!v.stored);
        assert!(parse_computed("ALTER TABLE t ADD COLUMN c INT AS (a + b)").is_none());
        assert!(parse_computed("ALTER TABLE t ADD INDEX (a)").is_none());
        assert!(is_computed_candidate(
            "ALTER TABLE t ADD COLUMN c INT AS (f(a))"
        ));
    }

    #[test]
    fn checks_and_triggers() {
        assert_eq!(
            check_alter(
                "shop",
                "orders",
                Some(("sprig_lc7_v2", "NOT (x)")),
                &["sprig_lc7_v1".into()]
            ),
            "ALTER TABLE `shop`.`orders` DROP CONSTRAINT IF EXISTS `sprig_lc7_v1`, ADD CONSTRAINT `sprig_lc7_v2` CHECK (NOT (x))"
        );
        let names = trigger_names(7, 2, &["insert".into(), "update".into()]);
        assert_eq!(names[0], ("__sprig_lt7_v2_bi".into(), "insert"));
        let t = validation_trigger(
            "t1_d1_g1_s1",
            "__sprig_lt7_v2_bu",
            "orders",
            "update",
            "__sprig_l31_v2",
            &[
                Bound::New("total".into()),
                Bound::Old("status".into()),
                Bound::Op,
            ],
        );
        assert!(t.contains("BEFORE UPDATE ON `t1_d1_g1_s1`.`orders`"), "{t}");
        assert!(
            t.contains(
                "CALL `__sprig_l31_v2`(NEW.`total`, OLD.`status`, 'update', v__event, v__result);"
            ),
            "{t}"
        );
        assert!(t.contains("MYSQL_ERRNO = 39863"), "{t}");
    }
}
