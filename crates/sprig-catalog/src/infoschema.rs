//! `information_schema` reads that open one schema at a time.
//!
//! MariaDB answers a `TABLES`, `VIEWS` or `TRIGGERS` query by listing
//! databases and then each database's tables. It reads only the named
//! database when the query compares the table's lookup column to one
//! constant with `=` (`EXPLAIN` says "Scanned 1 database"). A `LIKE`
//! pattern, an `IN` list, or `TRIGGER_SCHEMA` (the lookup column of
//! `TRIGGERS` is `EVENT_OBJECT_SCHEMA`) makes it read every database on
//! the server: with 10,000 tenants, about 400,000 tables for each such
//! query. Measured on the laptop with 909 tenants (40,000 tables):
//! `TRIGGERS WHERE TRIGGER_SCHEMA LIKE 't1\_d%'` 116 ms,
//! `WHERE EVENT_OBJECT_SCHEMA = 't1_d1_g1_s1'` 0.6 ms; `TABLES WHERE
//! TABLE_SCHEMA LIKE ... AND TABLE_NAME IN (...)` 60 ms, with `=` 0.6 ms.
//!
//! So a read over a tenant's schemas first lists them with
//! [`schemas_like`] (`SHOW DATABASES LIKE`, a directory listing that opens
//! no table, 1.6 ms there), then asks one `UNION ALL` statement with one
//! branch per schema ([`per_schema`]), each branch an `=` comparison.

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use sprig_common::{Result, SprigCode, SprigError};

/// At most this many branches go into one statement.
pub const BRANCHES: usize = 100;

/// `s` as a SQL string literal (quotes and backslashes escaped).
#[must_use]
pub fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// The schemas on the connection's server whose names match the `LIKE`
/// pattern `like` (escape `_` as `\_`), sorted.
///
/// # Errors
/// A backend error.
pub async fn schemas_like(conn: &mut Conn, like: &str) -> Result<Vec<String>> {
    let mut names: Vec<String> = conn
        .query(format!("SHOW DATABASES LIKE {}", literal(like)))
        .await
        .map_err(|e| {
            SprigError::new(
                SprigCode::BackendUnavailable,
                format!("SHOW DATABASES: {e}"),
            )
        })?;
    names.sort();
    Ok(names)
}

/// The statements reading `select` (`SELECT <columns> FROM
/// information_schema.<TABLE>`) for each schema of `schemas`, one
/// `UNION ALL` branch per schema, `BRANCHES` branches a statement: each
/// branch is `<select> WHERE <schema_col> = '<schema>'`, followed by
/// `and` (`" AND ..."`, or empty) when given. `schema_col` must be the
/// table's lookup column: `TABLE_SCHEMA` for `TABLES`, `VIEWS`, `COLUMNS`
/// and `STATISTICS`, `EVENT_OBJECT_SCHEMA` for `TRIGGERS`. Empty for no
/// schemas.
#[must_use]
pub fn per_schema(select: &str, schema_col: &str, schemas: &[String], and: &str) -> Vec<String> {
    schemas
        .chunks(BRANCHES)
        .map(|chunk| {
            chunk
                .iter()
                .map(|s| format!("{select} WHERE {schema_col} = {}{and}", literal(s)))
                .collect::<Vec<_>>()
                .join(" UNION ALL ")
        })
        .collect()
}

/// Runs [`per_schema`]'s statements and returns every row.
///
/// # Errors
/// A backend error.
pub async fn read_per_schema<T>(
    conn: &mut Conn,
    select: &str,
    schema_col: &str,
    schemas: &[String],
    and: &str,
) -> Result<Vec<T>>
where
    T: mysql_async::prelude::FromRow + Send + 'static,
{
    let mut out = Vec::new();
    for q in per_schema(select, schema_col, schemas, and) {
        let rows: Vec<T> = conn.query(q).await.map_err(|e| {
            SprigError::new(
                SprigCode::BackendUnavailable,
                format!("information_schema: {e}"),
            )
        })?;
        out.extend(rows);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_branch_per_schema_with_equality() {
        let s = vec!["t1_d1_g1_s1".to_owned(), "t1_d2_g1_s1".to_owned()];
        let q = per_schema(
            "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS",
            "EVENT_OBJECT_SCHEMA",
            &s,
            "",
        );
        assert_eq!(
            q,
            vec![
                "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = 't1_d1_g1_s1' \
                 UNION ALL SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = 't1_d2_g1_s1'"
                    .to_owned()
            ]
        );
        assert!(per_schema("SELECT 1 FROM x", "TABLE_SCHEMA", &[], "").is_empty());
    }

    #[test]
    fn long_lists_split_and_literals_escape() {
        let s: Vec<String> = (0..250).map(|i| format!("t1_d{i}_g1_s1")).collect();
        let q = per_schema(
            "SELECT 1 FROM information_schema.TABLES",
            "TABLE_SCHEMA",
            &s,
            " AND TABLE_NAME = 'a'",
        );
        assert_eq!(q.len(), 3);
        assert_eq!(q[0].matches("UNION ALL").count(), BRANCHES - 1);
        assert!(q[2].ends_with("TABLE_SCHEMA = 't1_d249_g1_s1' AND TABLE_NAME = 'a'"));
        assert_eq!(literal("a'b\\c"), "'a''b\\\\c'");
    }
}
