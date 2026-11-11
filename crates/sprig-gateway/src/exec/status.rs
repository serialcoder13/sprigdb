//! Per-session status counters (chapter 06, "Status variables"; chapter
//! 28, R4 round 3; gaps rows 46 and 61).
//!
//! A client's backend connections are pooled: MariaDB's session counters
//! (`Com_select`, `Questions`, ...) count every session that ever used the
//! connection, and a session that moves between connections sees them jump.
//! MySQL's `SHOW [SESSION] STATUS` counts the session's own statements, and
//! drivers' suites check deltas of them (MySQL Connector/J's `testBug41532`,
//! `testBug48172`, `testBug51666`). The gateway therefore counts each
//! session's statements itself and the relay writes its numbers into those
//! rows ([`super::relay::ConnStatus`]).
//!
//! `Uptime` is the gateway's, the same number `COM_STATISTICS` reports
//! (gaps row 61: PDO's server info and `SHOW STATUS` disagreed), and
//! `Prepared_stmt_count` is the session's open prepared statements: a
//! tenant sees no other session's.

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::Instant;

/// When the gateway's MySQL front end started ([`mark_started`]).
static STARTED: OnceLock<Instant> = OnceLock::new();

/// Records the front end's start; the first call wins.
pub(crate) fn mark_started() -> Instant {
    *STARTED.get_or_init(Instant::now)
}

/// Seconds since [`mark_started`]: `Uptime` in `SHOW STATUS` and
/// `COM_STATISTICS`.
pub(crate) fn uptime_secs() -> u64 {
    mark_started().elapsed().as_secs()
}

/// The status rows the gateway answers for a session, in the order
/// `SHOW STATUS` lists them.
const COUNTED: &[&str] = &[
    "Com_begin",
    "Com_commit",
    "Com_delete",
    "Com_delete_multi",
    "Com_insert",
    "Com_insert_select",
    "Com_replace",
    "Com_replace_select",
    "Com_rollback",
    "Com_select",
    "Com_set_option",
    "Com_show_status",
    "Com_show_variables",
    "Com_stmt_close",
    "Com_stmt_execute",
    "Com_stmt_prepare",
    "Com_stmt_reset",
    "Com_update",
    "Com_update_multi",
];

/// One session's counters.
#[derive(Debug, Clone, Default)]
pub(crate) struct SessionStatus {
    /// Statements the client sent (`Questions`): each statement of a
    /// multi-statement and each prepared-statement command.
    questions: u64,
    counts: BTreeMap<&'static str, u64>,
}

impl SessionStatus {
    /// A text statement (one of a `COM_QUERY`'s), counted as MySQL counts
    /// it: `Questions` and its `Com_*` counter.
    pub(crate) fn statement(&mut self, sql: &[u8]) {
        self.questions += 1;
        if let Some(c) = command_of(sql) {
            self.bump(c);
        }
    }

    /// A prepared-statement command (`Com_stmt_prepare`, ...) and that
    /// counter; an execute also counts in `Questions` and its statement's
    /// own counter, as MySQL's `mysql_execute_command` does.
    pub(crate) fn command(&mut self, name: &'static str, stmt: Option<&[u8]>) {
        // MySQL's `Questions` leaves out prepare, close and reset.
        if name == "Com_stmt_execute" {
            self.questions += 1;
        }
        self.bump(name);
        if let Some(c) = stmt.and_then(command_of) {
            self.bump(c);
        }
    }

    fn bump(&mut self, name: &'static str) {
        *self.counts.entry(name).or_default() += 1;
    }

    /// The rows the relay rewrites: every counter, `Questions`, `Uptime`
    /// and `Prepared_stmt_count` (`prepared` open statements).
    pub(crate) fn rows(&self, prepared: usize) -> Vec<(&'static str, String)> {
        let up = uptime_secs().to_string();
        let mut rows: Vec<(&'static str, String)> = COUNTED
            .iter()
            .map(|&n| (n, self.counts.get(n).copied().unwrap_or(0).to_string()))
            .collect();
        rows.push(("Questions", self.questions.to_string()));
        rows.push(("Uptime", up.clone()));
        rows.push(("Uptime_since_flush_status", up));
        rows.push(("Prepared_stmt_count", prepared.to_string()));
        rows
    }
}

impl super::PassSession {
    /// Counts a request in the session's status before it runs, as MySQL
    /// counts at dispatch.
    pub(super) fn count_request(&mut self, req: &crate::pipeline::Request) {
        use crate::pipeline::Request;
        match req {
            Request::Query { sql, .. } => {
                if sql.contains(&b';') {
                    for piece in super::split::split(sql) {
                        self.status.statement(&piece);
                    }
                } else {
                    self.status.statement(sql);
                }
            }
            Request::Prepare { .. } => self.status.command("Com_stmt_prepare", None),
            Request::Execute { stmt, .. } => {
                let text = self.stmts.get_mut(*stmt).ok().map(|s| s.sql.clone());
                self.status.command("Com_stmt_execute", text.as_deref());
            }
            Request::ResetStmt { .. } => self.status.command("Com_stmt_reset", None),
            Request::CloseStmt { .. } => self.status.command("Com_stmt_close", None),
            _ => {}
        }
    }

    /// `SHOW [SESSION] STATUS` rows: the session's counters, `Uptime` and
    /// `Prepared_stmt_count`.
    pub(super) fn status_rows(&self) -> Vec<(&'static str, String)> {
        self.status.rows(self.stmts.len())
    }
}

/// The significant words at the start of `sql` (comments and `(` skipped),
/// upper-cased, at most `n`.
fn words(sql: &[u8], n: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < sql.len() && out.len() < n {
        let c = sql[i];
        if c.is_ascii_whitespace() || c == b'(' {
            i += 1;
        } else if c == b'/' && sql.get(i + 1) == Some(&b'*') && sql.get(i + 2) != Some(&b'!') {
            i = sql[i + 2..]
                .windows(2)
                .position(|w| w == b"*/")
                .map_or(sql.len(), |p| i + 2 + p + 2);
        } else if c == b'#' || (c == b'-' && sql.get(i + 1) == Some(&b'-')) {
            i = sql[i..]
                .iter()
                .position(|&b| b == b'\n')
                .map_or(sql.len(), |p| i + p + 1);
        } else if c.is_ascii_alphabetic() || c == b'_' {
            let start = i;
            while i < sql.len() && (sql[i].is_ascii_alphanumeric() || sql[i] == b'_') {
                i += 1;
            }
            out.push(String::from_utf8_lossy(&sql[start..i]).to_ascii_uppercase());
        } else {
            break;
        }
    }
    out
}

/// Whether `word` appears in `sql` as a whole keyword outside quotes, at
/// nesting depth 0.
fn has_top_word(sql: &[u8], word: &str) -> bool {
    find_top_word(sql, word).is_some()
}

/// The byte position of the first `word` in `sql` that is a whole keyword
/// outside quotes, at nesting depth 0.
fn find_top_word(sql: &[u8], word: &str) -> Option<usize> {
    let w = word.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    while i < sql.len() {
        let c = sql[i];
        match c {
            b'\'' | b'"' | b'`' => {
                i += 1;
                while i < sql.len() && sql[i] != c {
                    if sql[i] == b'\\' && c != b'`' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ if depth == 0
                && sql.len() - i >= w.len()
                && sql[i..i + w.len()].eq_ignore_ascii_case(w)
                && (i == 0 || !is_word(sql[i - 1]))
                && sql.get(i + w.len()).is_none_or(|&b| !is_word(b)) =>
            {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn is_word(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

/// The `Com_*` counter MySQL 8.4 counts `sql` under; `None` for the
/// statements whose counters the gateway doesn't answer.
fn command_of(sql: &[u8]) -> Option<&'static str> {
    let w = words(sql, 3);
    let first = w.first()?.as_str();
    Some(match first {
        "SELECT" | "TABLE" | "VALUES" => "Com_select",
        // A CTE's statement is the one after it.
        "WITH" => {
            if has_top_word(sql, "UPDATE") {
                "Com_update"
            } else if has_top_word(sql, "DELETE") {
                "Com_delete"
            } else {
                "Com_select"
            }
        }
        "INSERT" if has_top_word(sql, "SELECT") || has_top_word(sql, "TABLE") => {
            "Com_insert_select"
        }
        "INSERT" => "Com_insert",
        "REPLACE" if has_top_word(sql, "SELECT") || has_top_word(sql, "TABLE") => {
            "Com_replace_select"
        }
        "REPLACE" => "Com_replace",
        "UPDATE" if multi_table_update(sql) => "Com_update_multi",
        "UPDATE" => "Com_update",
        "DELETE" if multi_table_delete(sql) => "Com_delete_multi",
        "DELETE" => "Com_delete",
        "BEGIN" => "Com_begin",
        "START" if w.get(1).is_some_and(|s| s == "TRANSACTION") => "Com_begin",
        "COMMIT" => "Com_commit",
        "ROLLBACK" if !has_top_word(sql, "TO") => "Com_rollback",
        "SET" => match w.get(1).map(String::as_str) {
            Some("PASSWORD" | "ROLE" | "DEFAULT" | "RESOURCE") => return None,
            Some("TRANSACTION") => return None,
            _ => "Com_set_option",
        },
        "SHOW" => match (w.get(1).map(String::as_str), w.get(2).map(String::as_str)) {
            (Some("STATUS"), _) | (Some("SESSION" | "LOCAL" | "GLOBAL"), Some("STATUS")) => {
                "Com_show_status"
            }
            (Some("VARIABLES"), _) | (Some("SESSION" | "LOCAL" | "GLOBAL"), Some("VARIABLES")) => {
                "Com_show_variables"
            }
            _ => return None,
        },
        _ => return None,
    })
}

/// `UPDATE a, b SET ...` or `UPDATE a JOIN b ... SET ...`.
fn multi_table_update(sql: &[u8]) -> bool {
    let upto = find_top_word(sql, "SET").unwrap_or(sql.len());
    let head = &sql[..upto];
    head.contains(&b',') || has_top_word(head, "JOIN")
}

/// `DELETE a, b FROM ...`, `DELETE a FROM a JOIN b ...` or `DELETE FROM a
/// USING ...`.
fn multi_table_delete(sql: &[u8]) -> bool {
    let w = words(sql, 2);
    let from_first = w.get(1).is_some_and(|s| {
        matches!(s.as_str(), "FROM" | "LOW_PRIORITY" | "QUICK" | "IGNORE")
    });
    if !from_first {
        return true;
    }
    has_top_word(sql, "USING") || has_top_word(sql, "JOIN")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_count_under_mysqls_commands() {
        let c = |s: &str| command_of(s.as_bytes());
        assert_eq!(c("select 1"), Some("Com_select"));
        assert_eq!(c("/* x */ (SELECT 1) UNION (SELECT 2)"), Some("Com_select"));
        assert_eq!(c("WITH a AS (SELECT 1) SELECT * FROM a"), Some("Com_select"));
        assert_eq!(c("TABLE t"), Some("Com_select"));
        assert_eq!(c("INSERT INTO t VALUES (1)"), Some("Com_insert"));
        assert_eq!(c("INSERT INTO t SELECT * FROM u"), Some("Com_insert_select"));
        assert_eq!(c("insert into t values ('SELECT')"), Some("Com_insert"));
        assert_eq!(c("REPLACE INTO t VALUES (1)"), Some("Com_replace"));
        assert_eq!(c("UPDATE t SET a = 1"), Some("Com_update"));
        assert_eq!(c("UPDATE t, u SET t.a = u.a"), Some("Com_update_multi"));
        assert_eq!(c("UPDATE t JOIN u ON t.id = u.id SET t.a = 1"), Some("Com_update_multi"));
        assert_eq!(c("UPDATE t SET a = 1, b = 2"), Some("Com_update"));
        assert_eq!(c("DELETE FROM t WHERE a = 1"), Some("Com_delete"));
        assert_eq!(c("DELETE t FROM t JOIN u ON t.a = u.a"), Some("Com_delete_multi"));
        assert_eq!(c("DELETE FROM t USING t, u"), Some("Com_delete_multi"));
        assert_eq!(c("BEGIN"), Some("Com_begin"));
        assert_eq!(c("START TRANSACTION READ ONLY"), Some("Com_begin"));
        assert_eq!(c("COMMIT"), Some("Com_commit"));
        assert_eq!(c("ROLLBACK"), Some("Com_rollback"));
        assert_eq!(c("ROLLBACK TO SAVEPOINT a"), None);
        assert_eq!(c("SET autocommit = 0"), Some("Com_set_option"));
        assert_eq!(c("SET NAMES utf8mb4"), Some("Com_set_option"));
        assert_eq!(c("SET PASSWORD = 'x'"), None);
        assert_eq!(c("SHOW SESSION STATUS LIKE 'Com_select'"), Some("Com_show_status"));
        assert_eq!(c("SHOW VARIABLES"), Some("Com_show_variables"));
        assert_eq!(c("SHOW TABLES"), None);
        assert_eq!(c("CREATE TABLE t (a INT)"), None);
    }

    #[test]
    fn a_session_sees_its_own_counts() {
        let mut s = SessionStatus::default();
        s.statement(b"SELECT 1");
        s.statement(b"INSERT INTO t VALUES (1)");
        s.command("Com_stmt_prepare", None);
        s.command("Com_stmt_execute", Some(b"SELECT ?"));
        s.statement(b"SHOW SESSION STATUS");
        let rows: BTreeMap<_, _> = s.rows(1).into_iter().collect();
        assert_eq!(rows["Com_select"], "2");
        assert_eq!(rows["Com_insert"], "1");
        assert_eq!(rows["Com_stmt_execute"], "1");
        assert_eq!(rows["Com_stmt_prepare"], "1");
        assert_eq!(rows["Com_show_status"], "1");
        assert_eq!(rows["Com_update"], "0");
        assert_eq!(rows["Questions"], "5");
        assert_eq!(rows["Prepared_stmt_count"], "1");
        assert!(rows["Uptime"].parse::<u64>().is_ok());
    }
}
