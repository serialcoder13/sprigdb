//! Statement text the MariaDB side of the mover sends (chapter 10, "The
//! watermark copy", "The applier", "Verification"). Pure, so it is unit
//! tested without a server.

use sprig_common::names::quote;

use crate::model::{Row, TableMeta, Value};

/// Most parameters one statement takes (MariaDB's limit is 65,535).
pub const MAX_PARAMS: usize = 60_000;

/// `` `schema`.`table` ``.
#[must_use]
pub fn qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", quote(schema), quote(table))
}

fn col_expr(t: &TableMeta, i: usize) -> String {
    let c = &t.columns[i];
    if c.numeric {
        format!("{} + 0", quote(&c.name))
    } else {
        quote(&c.name)
    }
}

/// Every column, in table order (`ENUM`, `SET`, `BIT` as numbers).
#[must_use]
pub fn select_list(t: &TableMeta) -> String {
    (0..t.columns.len())
        .map(|i| col_expr(t, i))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The key columns.
#[must_use]
pub fn key_list(t: &TableMeta) -> String {
    t.pk.iter()
        .map(|&i| quote(&t.columns[i].name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `key > after`, expanded for composite keys (`a > ? OR (a = ? AND b > ?)`),
/// with its parameters.
#[must_use]
pub fn after_cond(t: &TableMeta, after: &[Value]) -> (String, Vec<Value>) {
    lex_cond(t, after, ">", false)
}

/// `key <= last`, expanded the same way.
#[must_use]
pub fn upto_cond(t: &TableMeta, last: &[Value]) -> (String, Vec<Value>) {
    lex_cond(t, last, "<", true)
}

fn lex_cond(t: &TableMeta, vals: &[Value], op: &str, or_equal: bool) -> (String, Vec<Value>) {
    let names: Vec<String> = t.pk.iter().map(|&i| quote(&t.columns[i].name)).collect();
    let mut branches = Vec::new();
    let mut params = Vec::new();
    for i in 0..names.len() {
        let mut parts = Vec::new();
        for (j, n) in names.iter().enumerate().take(i) {
            parts.push(format!("{n} = ?"));
            params.push(vals[j].clone());
        }
        parts.push(format!("{} {op} ?", names[i]));
        params.push(vals[i].clone());
        branches.push(format!("({})", parts.join(" AND ")));
    }
    if or_equal {
        let parts: Vec<String> = names.iter().map(|n| format!("{n} = ?")).collect();
        params.extend(vals.iter().take(names.len()).cloned());
        branches.push(format!("({})", parts.join(" AND ")));
    }
    (format!("({})", branches.join(" OR ")), params)
}

/// A chunk read: up to `limit` rows after `after`, in key order.
#[must_use]
pub fn chunk(t: &TableMeta, after: Option<&Row>, limit: usize) -> (String, Vec<Value>) {
    let (cond, params) = after.map_or((String::new(), Vec::new()), |a| {
        let (c, p) = after_cond(t, a);
        (format!(" WHERE {c}"), p)
    });
    (
        format!(
            "SELECT {} FROM {}{cond} ORDER BY {} LIMIT {limit}",
            select_list(t),
            qualified(&t.schema, &t.name),
            key_list(t)
        ),
        params,
    )
}

/// Rows in `(after, last]`.
#[must_use]
pub fn range(
    t: &TableMeta,
    table: &str,
    exprs: &str,
    after: Option<&Row>,
    last: &Row,
) -> (String, Vec<Value>) {
    let (up, mut params) = upto_cond(t, last);
    let cond = match after {
        Some(a) => {
            let (c, p) = after_cond(t, a);
            let mut all = p;
            all.append(&mut params);
            params = all;
            format!("{c} AND {up}")
        }
        None => up,
    };
    (
        format!(
            "SELECT {exprs} FROM {} WHERE {cond} ORDER BY {}",
            qualified(&t.schema, table),
            key_list(t)
        ),
        params,
    )
}

/// The digest expression of a row: the first 16 hex digits of the MD5 of
/// the copied columns and their null flags, as an unsigned integer
/// (chapter 10, "Verification"). Columns are compared as bytes, so mixed
/// character sets can't fail the `CONCAT_WS`.
#[must_use]
pub fn digest_expr(t: &TableMeta) -> String {
    let cols: Vec<usize> = t.copy_columns().collect();
    let vals: Vec<String> = cols
        .iter()
        .map(|&i| format!("CAST({} AS BINARY)", quote(&t.columns[i].name)))
        .collect();
    let nulls: Vec<String> = cols
        .iter()
        .map(|&i| format!("ISNULL({})", quote(&t.columns[i].name)))
        .collect();
    format!(
        "CAST(CONV(LEFT(MD5(CONCAT_WS(0x1f, {}, {})), 16), 16, 10) AS UNSIGNED)",
        vals.join(", "),
        nulls.join(", ")
    )
}

/// The key columns then the digest, for verification.
#[must_use]
pub fn digest_list(t: &TableMeta) -> String {
    format!("{}, {}", key_list(t), digest_expr(t))
}

/// Digests of up to `limit` rows after `after`.
#[must_use]
pub fn digests(t: &TableMeta, after: Option<&Row>, limit: usize) -> (String, Vec<Value>) {
    let (cond, params) = after.map_or((String::new(), Vec::new()), |a| {
        let (c, p) = after_cond(t, a);
        (format!(" WHERE {c}"), p)
    });
    (
        format!(
            "SELECT {} FROM {}{cond} ORDER BY {} LIMIT {limit}",
            digest_list(t),
            qualified(&t.schema, &t.name),
            key_list(t)
        ),
        params,
    )
}

/// Upserts of `rows` into `schema.table` (the target's name for `t`), in
/// statements under [`MAX_PARAMS`] each.
#[must_use]
pub fn upserts(t: &TableMeta, table: &str, rows: &[Row]) -> Vec<(String, Vec<Value>)> {
    let cols: Vec<usize> = t.copy_columns().collect();
    if cols.is_empty() || rows.is_empty() {
        return Vec::new();
    }
    let names: Vec<String> = cols.iter().map(|&i| quote(&t.columns[i].name)).collect();
    let one = format!("({})", vec!["?"; cols.len()].join(", "));
    let update: Vec<String> = names.iter().map(|n| format!("{n} = VALUES({n})")).collect();
    let per = (MAX_PARAMS / cols.len()).max(1);
    rows.chunks(per)
        .map(|batch| {
            let mut params = Vec::with_capacity(batch.len() * cols.len());
            for r in batch {
                for &i in &cols {
                    params.push(r.get(i).cloned().unwrap_or(Value::NULL));
                }
            }
            (
                format!(
                    "INSERT INTO {} ({}) VALUES {} ON DUPLICATE KEY UPDATE {}",
                    qualified(&t.schema, table),
                    names.join(", "),
                    vec![one.as_str(); batch.len()].join(", "),
                    update.join(", ")
                ),
                params,
            )
        })
        .collect()
}

/// Deletes of `keys` (key values) from `schema.table`.
#[must_use]
pub fn deletes(t: &TableMeta, table: &str, keys: &[Row]) -> Vec<(String, Vec<Value>)> {
    if keys.is_empty() || t.pk.is_empty() {
        return Vec::new();
    }
    let names: Vec<String> = t.pk.iter().map(|&i| quote(&t.columns[i].name)).collect();
    let one = format!(
        "({})",
        names
            .iter()
            .map(|n| format!("{n} <=> ?"))
            .collect::<Vec<_>>()
            .join(" AND ")
    );
    let per = (MAX_PARAMS / names.len()).max(1);
    keys.chunks(per)
        .map(|batch| {
            let params: Vec<Value> = batch.iter().flat_map(|k| k.iter().cloned()).collect();
            (
                format!(
                    "DELETE FROM {} WHERE {}",
                    qualified(&t.schema, table),
                    vec![one.as_str(); batch.len()].join(" OR ")
                ),
                params,
            )
        })
        .collect()
}

/// The upsert of a watermark row (chapter 10, `__sprig_watermark`).
pub const WATERMARK_UPSERT: &str = "INSERT INTO `__sprig_sys`.`__sprig_watermark` \
     (job_id, stream, kind, seq, written_at) VALUES (?, ?, ?, ?, NOW(6)) \
     ON DUPLICATE KEY UPDATE seq = VALUES(seq), written_at = VALUES(written_at)";

/// The source's internal table.
pub const WATERMARK_DDL: &str = "CREATE TABLE IF NOT EXISTS `__sprig_sys`.`__sprig_watermark` (\
     job_id BINARY(16) NOT NULL, stream SMALLINT UNSIGNED NOT NULL, \
     kind ENUM('low','high','tick') NOT NULL, seq BIGINT UNSIGNED NOT NULL, \
     written_at DATETIME(6) NOT NULL, PRIMARY KEY (job_id, stream, kind)) ENGINE=InnoDB";

/// The target's position table.
pub const POS_DDL: &str = "CREATE TABLE IF NOT EXISTS `__sprig_sys`.`__sprig_mover_pos` (\
     job_id BINARY(16) NOT NULL, stream SMALLINT UNSIGNED NOT NULL, \
     target SMALLINT UNSIGNED NOT NULL, token BIGINT UNSIGNED NOT NULL, \
     gtid_pos VARCHAR(2048) NOT NULL, copy_state JSON NOT NULL, \
     updated_at DATETIME(6) NOT NULL, PRIMARY KEY (job_id, stream, target)) ENGINE=InnoDB";

/// The target's XA table (prepared groups are held in memory in R0; the
/// table is created for chapter 18's later use).
pub const XA_DDL: &str = "CREATE TABLE IF NOT EXISTS `__sprig_sys`.`__sprig_mover_xa` (\
     job_id BINARY(16) NOT NULL, stream SMALLINT UNSIGNED NOT NULL, \
     target SMALLINT UNSIGNED NOT NULL, xid VARBINARY(140) NOT NULL, \
     prepare_gtid VARCHAR(64) NOT NULL, changes LONGBLOB NOT NULL, \
     PRIMARY KEY (job_id, stream, target, xid)) ENGINE=InnoDB";

/// The applier session (chapter 10, "The applier").
pub const APPLIER_SESSION: &[&str] = &[
    "SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION'",
    "SET SESSION foreign_key_checks = 0",
    "SET SESSION unique_checks = 1",
    "SET SESSION time_zone = '+00:00'",
    "SET SESSION check_constraint_checks = 0",
    "SET NAMES binary",
];

/// A DDL statement's first keyword, upper-cased.
#[must_use]
pub fn first_word(sql: &str) -> String {
    let s = strip_comments(sql);
    s.split(|c: char| c.is_whitespace() || c == '(')
        .find(|w| !w.is_empty())
        .unwrap_or("")
        .to_ascii_uppercase()
}

fn strip_comments(sql: &str) -> &str {
    let mut s = sql.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("/*")
            && !rest.starts_with('!')
            && !rest.starts_with("M!")
            && let Some(end) = rest.find("*/")
        {
            s = rest[end + 2..].trim_start();
            continue;
        }
        if s.starts_with("--") || s.starts_with('#') {
            match s.find('\n') {
                Some(i) => s = s[i + 1..].trim_start(),
                None => return "",
            }
            continue;
        }
        return s;
    }
}

/// How the stream treats a `QUERY` event's statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {
    /// `BEGIN`.
    Begin,
    /// `COMMIT`.
    Commit,
    /// `ROLLBACK`.
    Rollback,
    /// `XA START`, `XA END`, `XA PREPARE`.
    XaInner,
    /// `XA COMMIT`.
    XaCommit,
    /// `XA ROLLBACK`.
    XaRollback,
    /// A schema change of tables, views, routines, triggers, events,
    /// indexes or databases.
    Ddl,
    /// Row data written as a statement (`binlog_format` isn't `ROW`).
    Dml,
    /// Accounts, grants, `SET`, `FLUSH`, `ANALYZE` and the like: never
    /// applied.
    Other,
}

/// Sorts a `QUERY` event's statement.
#[must_use]
pub fn classify(sql: &str) -> QueryKind {
    let s = strip_comments(sql);
    let mut words = s.split_whitespace().map(str::to_ascii_uppercase);
    let w1 = words.next().unwrap_or_default();
    let w2 = words.next().unwrap_or_default();
    let w3 = words.next().unwrap_or_default();
    match w1.trim_end_matches(';') {
        "BEGIN" => QueryKind::Begin,
        "COMMIT" => QueryKind::Commit,
        "ROLLBACK" => QueryKind::Rollback,
        "XA" => match w2.as_str() {
            "COMMIT" => QueryKind::XaCommit,
            "ROLLBACK" => QueryKind::XaRollback,
            _ => QueryKind::XaInner,
        },
        "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "LOAD" => QueryKind::Dml,
        "CREATE" | "ALTER" | "DROP" => {
            // CREATE OR REPLACE / DEFINER= / ALGORITHM= before the object word.
            let rest: Vec<String> = std::iter::once(w2.clone())
                .chain(std::iter::once(w3))
                .chain(words.take(6))
                .collect();
            if rest.iter().any(|w| {
                matches!(w.as_str(), "USER" | "ROLE" | "SERVER")
                    || w.starts_with("USER")
            }) && !rest.iter().any(|w| matches!(w.as_str(), "TABLE" | "VIEW" | "DATABASE" | "SCHEMA"))
            {
                QueryKind::Other
            } else {
                QueryKind::Ddl
            }
        }
        "RENAME" if w2 != "USER" => QueryKind::Ddl,
        "TRUNCATE" => QueryKind::Ddl,
        _ => QueryKind::Other,
    }
}

/// Whether a DDL statement run in `db` touches one of `schemas`: it ran in
/// one, or names one (quoted or bare).
#[must_use]
pub fn ddl_touches(db: &str, sql: &str, schemas: &[String]) -> bool {
    if schemas.iter().any(|s| s == db) {
        return true;
    }
    schemas.iter().any(|s| {
        sql.contains(&format!("`{s}`")) || {
            let lower = sql.to_ascii_lowercase();
            let s = s.to_ascii_lowercase();
            lower.match_indices(&s).any(|(i, _)| {
                let before = lower[..i].chars().next_back();
                let after = lower[i + s.len()..].chars().next();
                let word = |c: Option<char>| c.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
                !word(before) && !word(after)
            })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ColumnMeta;

    fn t2() -> TableMeta {
        TableMeta {
            schema: "s".into(),
            name: "t".into(),
            columns: vec![
                ColumnMeta { name: "a".into(), generated: false, numeric: false },
                ColumnMeta { name: "b".into(), generated: false, numeric: true },
                ColumnMeta { name: "g".into(), generated: true, numeric: false },
                ColumnMeta { name: "v".into(), generated: false, numeric: false },
            ],
            pk: vec![0, 1],
            create_sql: String::new(),
        }
    }

    #[test]
    fn composite_keys_expand() {
        let t = t2();
        let (c, p) = after_cond(&t, &[Value::Int(1), Value::Int(2)]);
        assert_eq!(c, "((`a` > ?) OR (`a` = ? AND `b` > ?))");
        assert_eq!(p.len(), 3);
        let (c, p) = upto_cond(&t, &[Value::Int(1), Value::Int(2)]);
        assert_eq!(c, "((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ?))");
        assert_eq!(p.len(), 5);
        let (sql, _) = chunk(&t, None, 10);
        assert_eq!(
            sql,
            "SELECT `a`, `b` + 0, `g`, `v` FROM `s`.`t` ORDER BY `a`, `b` LIMIT 10"
        );
    }

    #[test]
    fn upserts_skip_generated_columns_and_split() {
        let t = t2();
        let rows = vec![vec![Value::Int(1), Value::Int(2), Value::Int(9), Value::NULL]; 3];
        let u = upserts(&t, "t", &rows);
        assert_eq!(u.len(), 1);
        assert_eq!(
            u[0].0,
            "INSERT INTO `s`.`t` (`a`, `b`, `v`) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?) \
             ON DUPLICATE KEY UPDATE `a` = VALUES(`a`), `b` = VALUES(`b`), `v` = VALUES(`v`)"
        );
        assert_eq!(u[0].1.len(), 9);
        let many = vec![vec![Value::Int(1), Value::Int(2), Value::Int(9), Value::NULL]; 25_000];
        assert_eq!(upserts(&t, "t", &many).len(), 2);
        let d = deletes(&t, "t", &[vec![Value::Int(1), Value::Int(2)]]);
        assert_eq!(d[0].0, "DELETE FROM `s`.`t` WHERE (`a` <=> ? AND `b` <=> ?)");
    }

    #[test]
    fn queries_are_classified() {
        assert_eq!(classify("BEGIN"), QueryKind::Begin);
        assert_eq!(classify("COMMIT"), QueryKind::Commit);
        assert_eq!(classify("XA COMMIT X'01',X'',1"), QueryKind::XaCommit);
        assert_eq!(classify("ALTER TABLE t ADD c INT"), QueryKind::Ddl);
        assert_eq!(classify("/* x */ CREATE TABLE t (a INT)"), QueryKind::Ddl);
        assert_eq!(
            classify("CREATE DEFINER=`root`@`%` VIEW v AS SELECT 1"),
            QueryKind::Ddl
        );
        assert_eq!(classify("CREATE USER 't1_u1'@'%'"), QueryKind::Other);
        assert_eq!(classify("GRANT SELECT ON `t1_d1`.* TO x"), QueryKind::Other);
        assert_eq!(classify("REVOKE INSERT ON `t1_d1`.* FROM x"), QueryKind::Other);
        assert_eq!(classify("insert into t values (1)"), QueryKind::Dml);
        assert_eq!(classify("TRUNCATE TABLE t"), QueryKind::Ddl);
        assert_eq!(first_word("  -- c\n drop table x"), "DROP");
    }

    #[test]
    fn ddl_touch_checks_names_as_words() {
        let s = vec!["t1_d1_g1_s1".to_owned()];
        assert!(ddl_touches("t1_d1_g1_s1", "ALTER TABLE x ADD c INT", &s));
        assert!(ddl_touches("", "ALTER TABLE `t1_d1_g1_s1`.x ADD c INT", &s));
        assert!(ddl_touches("", "alter table t1_d1_g1_s1.x add c int", &s));
        assert!(!ddl_touches("", "ALTER TABLE t1_d1_g1_s10.x ADD c INT", &s));
        assert!(!ddl_touches("t2_sprig", "ALTER TABLE x ADD c INT", &s));
    }
}
