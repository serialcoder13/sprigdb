//! Sprig 2PC's redo records (chapter 18, "Redo table (R3)" and "Sprig 2PC
//! (R3)"): what a held branch on a cluster without XA wrote, kept outside
//! its transaction so a committed branch whose transaction was lost (the
//! holder died, the node restarted) can be replayed.
//!
//! ```text
//! prepare:  INSERT INTO anchor.__sprig_redo (dtid, seq, …) VALUES (d, 0, …), (d, 1, …)  autocommit
//! commit:   INSERT INTO anchor.__sprig_redo (dtid, seq, ctx) VALUES (d, MARK, 'done');
//!           COMMIT                                       in the held branch, then the rows go
//! abort:    ROLLBACK; DELETE FROM anchor.__sprig_redo WHERE dtid = d
//! recovery: rows without a marker older than txn.resolver_min_age_seconds → decide by
//!           race at the home anchor;
//!           commit: BEGIN; INSERT the marker ('replayed'); each statement (its affected
//!                   rows must match); DELETE the statements; COMMIT
//!                   (a duplicate marker: the holder committed meanwhile)
//!           abort:  DELETE
//!           a 'done' marker: the holder committed; the rows go
//! ```
//!
//! The marker inserted inside the held transaction makes "the data
//! committed" and "the branch is finished" one fact. It is a new key, so
//! neither Galera's certification nor `innodb_snapshot_isolation` refuses
//! it for rows committed after the branch began (deleting the statement
//! rows there would be refused). A replay inserts the same key first: it
//! waits for a holder still finishing (or deadlocks with it, and one of
//! the two is rolled back), so a branch is applied once. A replay's
//! marker stays for `txn.abort_record_retention_hours`, so a stalled
//! holder that wakes later meets it and fails.

use std::fmt::Write as _;

use crate::xid::{Dtid, Gtrid, hex};

/// The redo table's name inside an anchor schema.
pub const REDO_TABLE: &str = "__sprig_redo";

/// The marker row's `seq`: the branch is finished.
pub const MARK_SEQ: u64 = 1 << 63;

/// Who finished a branch, as its marker row says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedoMark {
    /// The holder committed (`ctx` 'done').
    Done,
    /// The resolver replayed it (`ctx` 'replayed').
    Replayed,
}

/// `CREATE TABLE IF NOT EXISTS` of the redo table in `schema`.
#[must_use]
pub fn redo_table_sql(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS `{}`.`{REDO_TABLE}` (\
         dtid BINARY(16) NOT NULL, \
         seq BIGINT UNSIGNED NOT NULL, \
         gtrid VARBINARY(64) NOT NULL, \
         ctx VARBINARY(1024) NOT NULL, \
         stmt LONGBLOB NOT NULL, \
         created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
         PRIMARY KEY (dtid, seq)) ENGINE=InnoDB",
        schema.replace('`', "``")
    )
}

/// One write a held branch ran: its text (physical names, the bytes the
/// session sent) and its replay context.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedoStmt {
    /// The statement.
    pub sql: String,
    /// Rows it changed (replay must change as many).
    pub affected: u64,
    /// The first generated `AUTO_INCREMENT` value, replayed as `insert_id`.
    pub insert_id: u64,
    /// `sql_mode`, `time_zone`: replayed with `SET STATEMENT`.
    pub vars: Vec<(String, String)>,
    /// The session's character set (`SET NAMES` before the statement).
    pub charset: Option<String>,
    /// `@@timestamp` the branch ran under (micros since the epoch).
    pub timestamp_us: Option<u64>,
}

impl RedoStmt {
    /// The `ctx` column: `key=value` lines.
    #[must_use]
    pub fn ctx(&self) -> String {
        let mut s = format!("affected={}\ninsert_id={}", self.affected, self.insert_id);
        if let Some(t) = self.timestamp_us {
            let _ = write!(s, "\nts={t}");
        }
        if let Some(c) = &self.charset {
            let _ = write!(s, "\ncharset={c}");
        }
        for (k, v) in &self.vars {
            let _ = write!(s, "\nvar.{k}={v}");
        }
        s
    }

    /// From the `ctx` column and the statement.
    #[must_use]
    pub fn from_ctx(sql: String, ctx: &str) -> Self {
        let mut r = Self {
            sql,
            ..Self::default()
        };
        for line in ctx.lines() {
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            match k {
                "affected" => r.affected = v.parse().unwrap_or(0),
                "insert_id" => r.insert_id = v.parse().unwrap_or(0),
                "ts" => r.timestamp_us = v.parse().ok(),
                "charset" => r.charset = Some(v.to_owned()),
                _ => {
                    if let Some(name) = k.strip_prefix("var.") {
                        r.vars.push((name.to_owned(), v.to_owned()));
                    }
                }
            }
        }
        r
    }

    /// The statement as replayed: `SET STATEMENT … FOR <sql>` with its
    /// timestamp, `insert_id`, `sql_mode` and `time_zone`.
    #[must_use]
    pub fn replay_sql(&self) -> String {
        let mut set: Vec<String> = Vec::new();
        if let Some(t) = self.timestamp_us {
            set.push(format!("timestamp={}.{:06}", t / 1_000_000, t % 1_000_000));
        }
        if self.insert_id > 0 {
            set.push(format!("insert_id={}", self.insert_id));
        }
        for (k, v) in &self.vars {
            if k.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                set.push(format!(
                    "{k}='{}'",
                    v.replace('\\', "\\\\").replace('\'', "''")
                ));
            }
        }
        if set.is_empty() {
            self.sql.clone()
        } else {
            format!("SET STATEMENT {} FOR {}", set.join(", "), self.sql)
        }
    }
}

/// A held branch's redo rows found on a cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedoBranch {
    /// The anchor schema holding them.
    pub schema: String,
    /// The transaction.
    pub dtid: Dtid,
    /// Its gtrid (tenant, start, home anchor: where it is decided).
    pub gtrid: Gtrid,
    /// Age of the oldest row, seconds.
    pub age_seconds: u64,
    /// Statement rows left.
    pub statements: usize,
    /// The marker, once the branch finished.
    pub mark: Option<RedoMark>,
}

/// What a replay did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Replay {
    /// The statements ran and the rows are gone, in one transaction.
    Applied,
    /// No rows were left: the holder finished (committed or aborted).
    Gone,
    /// A statement changed a different number of rows than it did in the
    /// held branch (another writer changed them meanwhile): nothing was
    /// applied, the rows stay, an operator decides (`sprig_txn_redo_hazards_total`).
    Hazard(String),
}

/// `INSERT INTO schema.__sprig_redo …` of a branch's statements.
#[must_use]
pub fn insert_sql(schema: &str, dtid: Dtid, gtrid: &Gtrid, stmts: &[RedoStmt]) -> String {
    let d = hex(&dtid.to_bytes());
    let g = hex(&gtrid.to_bytes());
    let rows: Vec<String> = stmts
        .iter()
        .enumerate()
        .map(|(i, s)| {
            format!(
                "(X'{d}', {i}, X'{g}', X'{}', X'{}')",
                hex(s.ctx().as_bytes()),
                hex(s.sql.as_bytes())
            )
        })
        .collect();
    format!(
        "INSERT INTO `{}`.`{REDO_TABLE}` (dtid, seq, gtrid, ctx, stmt) VALUES {}",
        schema.replace('`', "``"),
        rows.join(", ")
    )
}

/// The marker row: `INSERT INTO schema.__sprig_redo … (dtid, MARK, ctx)`.
#[must_use]
pub fn mark_sql(schema: &str, dtid: Dtid, gtrid: &Gtrid, mark: RedoMark) -> String {
    format!(
        "INSERT INTO `{}`.`{REDO_TABLE}` (dtid, seq, gtrid, ctx, stmt) VALUES \
         (X'{}', {MARK_SEQ}, X'{}', '{}', '')",
        schema.replace('`', "``"),
        hex(&dtid.to_bytes()),
        hex(&gtrid.to_bytes()),
        match mark {
            RedoMark::Done => "done",
            RedoMark::Replayed => "replayed",
        }
    )
}

/// `DELETE FROM schema.__sprig_redo WHERE dtid = …` (marker included).
#[must_use]
pub fn delete_sql(schema: &str, dtid: Dtid) -> String {
    format!(
        "DELETE FROM `{}`.`{REDO_TABLE}` WHERE dtid = X'{}'",
        schema.replace('`', "``"),
        hex(&dtid.to_bytes())
    )
}

/// Why `sql` would not replay to the same result (chapter 18's determinism
/// rule: what MariaDB treats as unsafe for statement-based replication),
/// or `None`. String literals, quoted names and comments are skipped.
#[must_use]
pub fn unsafe_reason(sql: &str) -> Option<&'static str> {
    let code = code_only(sql);
    let words: &[(&str, &'static str)] = &[
        ("UUID(", "UUID()"),
        ("UUID_SHORT(", "UUID_SHORT()"),
        ("SYSDATE(", "SYSDATE()"),
        ("RAND()", "RAND() without a seed"),
        ("CONNECTION_ID(", "CONNECTION_ID()"),
        ("FOUND_ROWS(", "FOUND_ROWS()"),
        ("ROW_COUNT(", "ROW_COUNT()"),
        ("LAST_INSERT_ID()", "LAST_INSERT_ID()"),
        ("USER(", "a user function"),
        ("CURRENT_USER", "CURRENT_USER"),
        ("LOAD_FILE(", "LOAD_FILE()"),
        ("GET_LOCK(", "GET_LOCK()"),
        ("RELEASE_LOCK(", "RELEASE_LOCK()"),
        ("IS_USED_LOCK(", "IS_USED_LOCK()"),
        ("IS_FREE_LOCK(", "IS_FREE_LOCK()"),
        ("SLEEP(", "SLEEP()"),
        ("NEXTVAL(", "a sequence"),
        ("NEXT VALUE FOR", "a sequence"),
        ("LASTVAL(", "a sequence"),
        ("SETVAL(", "a sequence"),
        ("@", "a user or system variable"),
    ];
    for (w, why) in words {
        if code.contains(w) {
            return Some(why);
        }
    }
    let first = code.split_whitespace().next().unwrap_or("");
    if matches!(first, "UPDATE" | "DELETE")
        && contains_word(&code, "LIMIT")
        && !contains_word(&code, "ORDER")
    {
        return Some("LIMIT in an UPDATE or DELETE without ORDER BY");
    }
    None
}

fn contains_word(code: &str, w: &str) -> bool {
    code.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .any(|x| x == w)
}

/// `sql` upper-cased, with string literals, quoted names and comments
/// replaced by a space, and whitespace before `(` removed.
fn code_only(sql: &str) -> String {
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b'\'' | b'"' | b'`' => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' && c != b'`' {
                        i += 2;
                        continue;
                    }
                    if b[i] == c {
                        if b.get(i + 1) == Some(&c) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
                out.push(' ');
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                out.push(' ');
            }
            b'#' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                out.push(' ');
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
                out.push(' ');
            }
            _ => {
                out.push(char::from(c).to_ascii_uppercase());
                i += 1;
            }
        }
    }
    // `NOW ()` → `NOW()`.
    let mut squeezed = String::with_capacity(out.len());
    let chars: Vec<char> = out.chars().collect();
    for (k, ch) in chars.iter().enumerate() {
        if ch.is_whitespace() {
            let next = chars[k + 1..].iter().find(|c| !c.is_whitespace());
            if matches!(next, Some('(' | ')')) {
                continue;
            }
        }
        squeezed.push(*ch);
    }
    squeezed
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn unsafe_statements_are_named() {
        assert_eq!(
            unsafe_reason("INSERT INTO t VALUES (UUID())"),
            Some("UUID()")
        );
        assert_eq!(
            unsafe_reason("insert into t values (rand ( ))"),
            Some("RAND() without a seed")
        );
        assert_eq!(unsafe_reason("INSERT INTO t VALUES (RAND(7))"), None);
        assert_eq!(
            unsafe_reason("INSERT INTO t VALUES ('UUID()', NOW())"),
            None
        );
        assert_eq!(
            unsafe_reason("UPDATE t SET a = @x"),
            Some("a user or system variable")
        );
        assert_eq!(
            unsafe_reason("DELETE FROM t WHERE a > 1 LIMIT 3"),
            Some("LIMIT in an UPDATE or DELETE without ORDER BY")
        );
        assert_eq!(unsafe_reason("DELETE FROM t ORDER BY a LIMIT 3"), None);
        assert_eq!(unsafe_reason("UPDATE t SET `limit` = 1 /* LIMIT */"), None);
    }

    #[test]
    fn context_round_trips_and_replays() {
        let s = RedoStmt {
            sql: "INSERT INTO `t1_d1_g1_s1`.`t` VALUES (NULL, NOW())".into(),
            affected: 1,
            insert_id: 41,
            vars: vec![
                ("sql_mode".into(), "STRICT_TRANS_TABLES".into()),
                ("time_zone".into(), "+05:30".into()),
            ],
            charset: Some("utf8mb4".into()),
            timestamp_us: Some(1_700_000_000_123_456),
        };
        let back = RedoStmt::from_ctx(s.sql.clone(), &s.ctx());
        assert_eq!(back, s);
        assert_eq!(
            s.replay_sql(),
            "SET STATEMENT timestamp=1700000000.123456, insert_id=41, \
             sql_mode='STRICT_TRANS_TABLES', time_zone='+05:30' FOR INSERT INTO \
             `t1_d1_g1_s1`.`t` VALUES (NULL, NOW())"
        );
        assert!(redo_table_sql("s").contains("PRIMARY KEY (dtid, seq)"));
    }
}
