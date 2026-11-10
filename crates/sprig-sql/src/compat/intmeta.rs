//! Integer result metadata on port 3306 (chapter 28, "Output on port
//! 3306", build step 11; `decisions/mysql-persona-gaps.md` row 34).
//!
//! MySQL 8.4 types every integer an expression computes as `LONGLONG` (8):
//! literals, arithmetic, comparisons, `CAST(.. AS SIGNED)`, integer
//! functions, window functions, and `IF`, `COALESCE`, `CASE` and the like
//! once one of their values is such an expression. MariaDB 11.8 sends
//! `LONG` (3) whenever the value fits in 32 bits. Drivers pick the
//! language type from the column type: MySQL Connector/J's `getObject`
//! gives `Long` on MySQL and `Integer` through MariaDB, `mysql2` and
//! `PyMySQL` report another type code, and a binary row carries 4 bytes
//! instead of 8. MySQL also types `YEAR(..)` as `YEAR` (13), where MariaDB
//! sends `LONG`.
//!
//! A column the backend reports as a table's column, or as a value passed
//! through from one (`MAX(i)`, `COALESCE(i, j)`, a derived table's or
//! CTE's column), keeps its type on both servers. So the gateway can't
//! decide from the metadata alone. [`plan`] reads the statement's select
//! list and says, for each output column, whether MySQL would compute it
//! ([`Fix::LongLong`], [`Fix::Year`]) or pass a column's type through
//! ([`Fix::Keep`]). [`apply`] then changes only expression columns
//! (no `org_table`) that MariaDB sent as `TINY`, `SHORT`, `INT24` or `LONG`,
//! and [`widen_binary_row`] re-encodes binary rows to match. Text rows don't
//! change: the digits are the same.
//!
//! Measured on 10 November 2026 against MySQL 8.4.11 and MariaDB 11.8.9
//! (about 150 select items; the unit tests carry the cases). Lengths are
//! left as MariaDB sends them: MySQL counts a sign for literals (`1` has
//! length 2) and uses 21 for many functions, which no measured driver
//! reads.

use bytes::Bytes;
use sprig_proto_mysql::ColumnDef;
use sprig_proto_mysql::resultset::{
    ColumnFlags, column_type as ct, decode_binary_row, encode_binary_row,
};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, SelectItem,
    SetExpr, Statement, UnaryOperator, Value,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::lex::{LexOptions, lex};
use crate::names::rewrite::ast::parse_view;

/// Statements longer than this many tokens aren't planned (their columns
/// keep MariaDB's types).
const MAX_TOKENS: usize = 20_000;

/// What MySQL 8.4 would send for one output column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fix {
    /// A column's own type, or not known: MariaDB's type stays.
    Keep,
    /// A computed integer: `LONGLONG`.
    LongLong,
    /// `YEAR(..)`: MySQL's `YEAR` type, unsigned.
    Year,
}

/// The fixes for a statement's output columns. Select items before the
/// first `*` count from the start, those after the last `*` from the end;
/// items between two `*` aren't placed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Plan {
    head: Vec<Fix>,
    tail: Vec<Fix>,
    star: bool,
}

impl Plan {
    /// The fix for column `i` of `n`.
    #[must_use]
    pub fn fix(&self, i: usize, n: usize) -> Fix {
        if !self.star {
            return if n == self.head.len() {
                self.head[i]
            } else {
                Fix::Keep
            };
        }
        if i < self.head.len() {
            return self.head[i];
        }
        let from_end = n - i;
        if from_end <= self.tail.len() {
            return self.tail[self.tail.len() - from_end];
        }
        Fix::Keep
    }

    /// Whether any column changes.
    #[must_use]
    pub fn changes_anything(&self) -> bool {
        self.head.iter().chain(&self.tail).any(|f| *f != Fix::Keep)
    }
}

/// Whether `cols` has a column [`apply`] could change: an expression
/// (no `org_table`) of a small integer type. Cheap; call it before
/// [`plan`], which parses the statement.
#[must_use]
pub fn candidates(cols: &[ColumnDef]) -> bool {
    cols.iter().any(small_int_expr)
}

fn small_int_expr(c: &ColumnDef) -> bool {
    c.org_table.is_empty() && matches!(c.col_type, ct::TINY | ct::SHORT | ct::INT24 | ct::LONG)
}

/// The plan for a statement's text (the client's), or `None` when it isn't
/// a query whose select list can be read.
#[must_use]
pub fn plan(sql: &[u8], opts: &LexOptions) -> Option<Plan> {
    let stmt = parse(sql, *opts)?;
    let Statement::Query(query) = &stmt else {
        return None;
    };
    let items = query_items(query)?;
    let mut p = Plan::default();
    let mut seen_star = false;
    let mut after_star = Vec::new();
    for it in items {
        match it {
            Item::Star => {
                seen_star = true;
                after_star.clear();
            }
            Item::Fix(f) if !seen_star => p.head.push(f),
            Item::Fix(f) => after_star.push(f),
        }
    }
    p.star = seen_star;
    p.tail = after_star;
    Some(p)
}

/// Changes `cols` as MySQL would type them (see the module docs). Returns
/// whether anything changed.
pub fn apply(plan: &Plan, cols: &mut [ColumnDef]) -> bool {
    let n = cols.len();
    let mut changed = false;
    for (i, c) in cols.iter_mut().enumerate() {
        if !small_int_expr(c) {
            continue;
        }
        match plan.fix(i, n) {
            Fix::Keep => {}
            Fix::LongLong => {
                c.col_type = ct::LONGLONG;
                changed = true;
            }
            Fix::Year => {
                c.col_type = ct::YEAR;
                c.flags |= ColumnFlags::UNSIGNED.bits();
                changed = true;
            }
        }
    }
    changed
}

/// A binary row encoded for `before` (the backend's columns), re-encoded
/// for `after` (the same columns after [`apply`]). `None` if it can't be
/// decoded; the caller then sends it unchanged.
#[must_use]
pub fn widen_binary_row(row: &Bytes, before: &[ColumnDef], after: &[ColumnDef]) -> Option<Bytes> {
    let vals = decode_binary_row(row, before).ok()?;
    encode_binary_row(&vals, after).ok()
}

/// A select item: a column with its fix, or a `*` (any number of table
/// columns).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Item {
    Fix(Fix),
    Star,
}

fn parse(src: &[u8], opts: LexOptions) -> Option<Statement> {
    let text = std::str::from_utf8(src).ok()?;
    let tokens = lex(src, &opts).ok()?;
    let toks: Vec<_> = tokens.into_iter().collect();
    if toks.iter().filter(|t| !t.kind.is_trivia()).count() > MAX_TOKENS {
        return None;
    }
    let view = parse_view(text, &toks)?;
    let mut stmts = Parser::new(&MySqlDialect {})
        .with_recursion_limit(64)
        .try_with_sql(&view)
        .ok()?
        .parse_statements()
        .ok()?;
    if stmts.len() != 1 {
        return None;
    }
    stmts.pop()
}

fn query_items(q: &Query) -> Option<Vec<Item>> {
    set_items(&q.body)
}

fn set_items(set: &SetExpr) -> Option<Vec<Item>> {
    match set {
        SetExpr::Select(sel) => Some(
            sel.projection
                .iter()
                .map(|it| match it {
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                        Item::Fix(fix_of(e))
                    }
                    _ => Item::Star,
                })
                .collect(),
        ),
        SetExpr::Query(q) => query_items(q),
        SetExpr::SetOperation { left, right, .. } => {
            // A union's column is MySQL's LONGLONG when any branch computes
            // it (measured: `SELECT i FROM t UNION SELECT 2`).
            let lhs = set_items(left)?;
            let rhs = set_items(right)?;
            if lhs.len() != rhs.len() || lhs.contains(&Item::Star) || rhs.contains(&Item::Star) {
                return None;
            }
            Some(
                lhs.into_iter()
                    .zip(rhs)
                    .map(|(a, b)| match (a, b) {
                        (Item::Fix(x), Item::Fix(y)) => Item::Fix(merge(x, y)),
                        _ => Item::Star,
                    })
                    .collect(),
            )
        }
        _ => None,
    }
}

/// Two values that meet in one column (a union, `IF`, `COALESCE`, ...).
fn merge(a: Fix, b: Fix) -> Fix {
    if a == Fix::Keep && b == Fix::Keep {
        Fix::Keep
    } else {
        Fix::LongLong
    }
}

/// Functions whose result type is that of their arguments (when they are
/// all integers): a column passed through keeps its type on MySQL too.
fn passthrough(name: &str) -> bool {
    matches!(
        name,
        "IF" | "IFNULL"
            | "NVL"
            | "COALESCE"
            | "NULLIF"
            | "GREATEST"
            | "LEAST"
            | "MAX"
            | "MIN"
            | "ANY_VALUE"
    )
}

#[allow(clippy::match_same_arms)] // one arm per kind of expression, as measured
fn fix_of(e: &Expr) -> Fix {
    match e {
        // User and system variables: MySQL's integers are LONGLONG, and so
        // are MariaDB's, except a value the gateway answers itself as a
        // literal (`@@max_connections`, chapter 06).
        Expr::Identifier(i) if i.value.starts_with('@') => Fix::LongLong,
        Expr::CompoundIdentifier(p) if p.first().is_some_and(|i| i.value.starts_with('@')) => {
            Fix::LongLong
        }
        // A column, or a derived table's or CTE's column (MySQL keeps LONG
        // for `SELECT x FROM (SELECT 1 AS x) d`).
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Fix::Keep,
        Expr::Nested(inner) => fix_of(inner),
        Expr::Value(v) if matches!(&v.value, Value::Number(n, _) if !n.contains(['.', 'e', 'E'])) => {
            Fix::LongLong
        }
        Expr::Value(v) if matches!(v.value, Value::Boolean(_)) => Fix::LongLong,
        Expr::Value(_) => Fix::Keep,
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => fix_of(expr),
        Expr::Collate { expr, .. } => fix_of(expr),
        Expr::Subquery(q) => match query_items(q).as_deref() {
            Some([Item::Fix(f)]) => *f,
            _ => Fix::Keep,
        },
        Expr::Case {
            conditions,
            else_result,
            ..
        } => conditions
            .iter()
            .map(|w| fix_of(&w.result))
            .chain(else_result.iter().map(|e| fix_of(e)))
            .fold(Fix::Keep, merge),
        Expr::Function(f) => {
            let name = f
                .name
                .0
                .last()
                .and_then(|p| p.as_ident())
                .map(|i| i.value.to_ascii_uppercase())
                .unwrap_or_default();
            if f.over.is_some() {
                // Window functions: MySQL's are LONGLONG even for MAX(i).
                return Fix::LongLong;
            }
            if name == "YEAR" {
                return Fix::Year;
            }
            if !passthrough(&name) {
                return Fix::LongLong;
            }
            let args: Vec<&Expr> = match &f.args {
                FunctionArguments::List(l) => l
                    .args
                    .iter()
                    .filter_map(|a| match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            };
            let values: &[&Expr] = match name.as_str() {
                // The condition isn't a value.
                "IF" => args.get(1..).unwrap_or(&[]),
                // The result is the first argument's.
                "NULLIF" => args.get(..1).unwrap_or(&[]),
                _ => &args,
            };
            values.iter().map(|e| fix_of(e)).fold(Fix::Keep, merge)
        }
        // `@a := 1`, arithmetic, comparisons, logic, bit operations.
        Expr::BinaryOp {
            op: BinaryOperator::StringConcat,
            ..
        } => Fix::Keep,
        // Everything else MariaDB reports as a small integer is computed:
        // `IS NULL`, `IN`, `BETWEEN`, `LIKE`, `EXISTS`, `CAST`, `EXTRACT`,
        // `CEIL`, `POSITION`, ...
        _ => Fix::LongLong,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixes(sql: &str) -> Vec<Fix> {
        let p = plan(sql.as_bytes(), &LexOptions::mysql84()).expect(sql);
        assert!(!p.star, "{sql}");
        p.head
    }

    use Fix::{Keep as K, LongLong as L, Year as Y};

    /// Each case was run on MySQL 8.4.11 and MariaDB 11.8.9 (10 November
    /// 2026): `L` where MySQL said LONGLONG and MariaDB LONG, `K` where both
    /// gave the same type, `Y` for MySQL's YEAR.
    #[test]
    fn literals_and_operators() {
        assert_eq!(fixes("SELECT 1, -1, 0, TRUE, FALSE"), [L, L, L, L, L]);
        for e in [
            "1+1", "1-2", "2*3", "7 DIV 2", "7 % 2", "~0", "1<<2", "5&3", "5|3", "5^3",
        ] {
            assert_eq!(fixes(&format!("SELECT {e}")), [L], "{e}");
        }
        // sqlparser doesn't parse the infix `MOD`: such a statement keeps
        // MariaDB's types.
        assert!(plan(b"SELECT 7 MOD 2", &LexOptions::mysql84()).is_none());
        assert_eq!(
            fixes(
                "SELECT 1=1, 1<2, 1 IS NULL, 1 IN (1,2), 'a' LIKE 'a', NOT 1, 1 AND 0, \
                 1 BETWEEN 0 AND 2, EXISTS (SELECT 1), 1 IN (SELECT 1)"
            ),
            [L; 10]
        );
        assert_eq!(fixes("SELECT (1), (SELECT 1), @a := 2"), [L, L, L]);
        assert_eq!(fixes("SELECT 'a', 1.5, NULL"), [K; 3]);
        assert_eq!(
            fixes("SELECT @a, @@max_connections, @@session.autocommit"),
            [L; 3]
        );
    }

    #[test]
    fn functions() {
        assert_eq!(
            fixes(
                "SELECT LENGTH('ab'), ASCII('a'), CRC32('a'), SIGN(-2), ABS(-2), CEIL(1.2), \
                 FLOOR(1.2), CAST(1 AS SIGNED), CONVERT('1', SIGNED), CONNECTION_ID(), \
                 EXTRACT(YEAR FROM '2020-01-02'), MONTH('2020-01-02'), GET_LOCK('x', 0)"
            ),
            [L; 13]
        );
        assert_eq!(fixes("SELECT YEAR('2020-01-02'), YEAR(dt) FROM t"), [Y, Y]);
        assert_eq!(
            fixes("SELECT ROW_NUMBER() OVER (), MAX(i) OVER (), COUNT(*) OVER () FROM t"),
            [L, L, L]
        );
    }

    #[test]
    fn values_passed_through_keep_their_type() {
        assert_eq!(
            fixes(
                "SELECT ti, t.i, MAX(ti), MIN(si), COALESCE(i, ti), IF(ti, ti, si), \
                 CASE WHEN 1 THEN ti END, GREATEST(ti, si), (SELECT ti FROM t) FROM t"
            ),
            [K; 9]
        );
        assert_eq!(
            fixes(
                "SELECT IFNULL(ti, 0), COALESCE(i, 0), IF(ti, ti, 1), MIN(ti) + 0, bt + 0, \
                 IF(1,2,3), NULLIF(1,2), CASE WHEN 1 THEN 2 ELSE 3 END, MAX(1), MAX(i+0) FROM t"
            ),
            [L; 10]
        );
        // The condition of IF and the second value of NULLIF don't count.
        assert_eq!(fixes("SELECT IF(1, ti, si), NULLIF(ti, 1) FROM t"), [K, K]);
        assert_eq!(fixes("SELECT x, x + 0 FROM (SELECT 1 AS x) d"), [K, L]);
    }

    #[test]
    fn unions_and_stars() {
        assert_eq!(fixes("SELECT i FROM t UNION SELECT 2"), [L]);
        assert_eq!(fixes("SELECT 1 UNION ALL SELECT ti FROM t"), [L]);
        assert_eq!(fixes("SELECT i FROM t UNION SELECT ti FROM t"), [K]);
        let p = plan(b"SELECT *, 1 FROM t", &LexOptions::mysql84()).unwrap();
        assert_eq!((p.fix(0, 4), p.fix(3, 4)), (K, L));
        let p = plan(b"SELECT 1, t.* FROM t", &LexOptions::mysql84()).unwrap();
        assert_eq!((p.fix(0, 4), p.fix(3, 4)), (L, K));
        // A count that doesn't match the select list: nothing changes.
        let p = plan(b"SELECT 1, 2", &LexOptions::mysql84()).unwrap();
        assert_eq!(p.fix(0, 3), K);
    }

    #[test]
    fn other_statements_have_no_plan() {
        for s in [
            "SHOW TABLES",
            "CALL p()",
            "INSERT INTO t VALUES (1)",
            "DO 1",
        ] {
            assert!(plan(s.as_bytes(), &LexOptions::mysql84()).is_none(), "{s}");
        }
    }

    fn col(ty: u8, table: &str) -> ColumnDef {
        ColumnDef {
            org_table: Bytes::copy_from_slice(table.as_bytes()),
            col_type: ty,
            charset: 63,
            flags: (ColumnFlags::BINARY | ColumnFlags::NUM).bits(),
            ..ColumnDef::default()
        }
    }

    #[test]
    fn apply_changes_expressions_only_and_rows_follow() {
        use sprig_proto_mysql::Value as V;
        let p = plan(b"SELECT 1, i, YEAR(d), -5 FROM t", &LexOptions::mysql84()).unwrap();
        let before = vec![
            col(ct::LONG, ""),
            col(ct::LONG, "t"),
            col(ct::LONG, ""),
            col(ct::LONG, ""),
        ];
        let mut after = before.clone();
        assert!(candidates(&after));
        assert!(apply(&p, &mut after));
        assert_eq!(
            after.iter().map(|c| c.col_type).collect::<Vec<_>>(),
            [ct::LONGLONG, ct::LONG, ct::YEAR, ct::LONGLONG]
        );
        assert!(after[2].is_unsigned());
        let row = encode_binary_row(&[V::Int(1), V::Int(7), V::Null, V::Int(-5)], &before).unwrap();
        let out = widen_binary_row(&row, &before, &after).unwrap();
        assert_eq!(
            decode_binary_row(&out, &after).unwrap(),
            [V::Int(1), V::Int(7), V::Null, V::Int(-5)]
        );
        assert_eq!(out.len(), row.len() + 4 + 4);
        // Nothing to change.
        let mut t = vec![col(ct::LONG, "t")];
        assert!(!candidates(&t));
        assert!(!apply(&p, &mut t));
    }
}
