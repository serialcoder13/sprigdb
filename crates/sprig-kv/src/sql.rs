//! The SQL a key-value front end runs, and literals for it.
//!
//! Front ends run their statements through a [`SqlRunner`]: in the gateway
//! that is the user's own executor session (name virtualization, chapter
//! 29's privileges, the router), in tests a plain MariaDB connection.
//! Every value is sent as a hex literal, so statements stay ASCII (the
//! gateway's parser works on UTF-8 text) and binary-safe.

use async_trait::async_trait;

/// One result row, text protocol: `None` is SQL `NULL`.
pub type Row = Vec<Option<Vec<u8>>>;

/// A failed statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlError {
    /// The MySQL error number, when MariaDB or the gateway gave one.
    pub errno: Option<u16>,
    /// The message.
    pub message: String,
}

impl SqlError {
    /// A deadlock (1213) or lock wait timeout (1205): retry the whole unit.
    #[must_use]
    pub fn retryable(&self) -> bool {
        matches!(self.errno, Some(1213 | 1205))
    }
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.errno {
            Some(n) => write!(f, "{n}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

/// Runs SQL for a front end.
#[async_trait]
pub trait SqlRunner: Send {
    /// A statement returning rows.
    async fn query(&mut self, sql: &str) -> Result<Vec<Row>, SqlError>;
    /// A statement returning an affected-row count.
    async fn exec(&mut self, sql: &str) -> Result<u64, SqlError>;
    /// Marks the writes that follow, until called with `false`, as a Redis
    /// command's whose keyspace notifications the command sends itself, so
    /// the notifications from the change feed pass over them (chapter 40;
    /// RLS round 5, RL51). The gateway's runner prefixes each write
    /// statement with chapter 38's comment `o=k`; by default nothing.
    fn notified(&mut self, _on: bool) {}
    /// The client's data center hint (chapter 24: the text after the last
    /// `~` of a Redis `CLIENT SETNAME` name; `None` clears it). The
    /// gateway's runner counts the session's statements as traffic from
    /// that DC; by default nothing.
    fn client_dc(&mut self, _raw: Option<&str>) {}
}

/// `X'...'`, or `''` for empty bytes.
#[must_use]
pub fn lit(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    if b.is_empty() {
        return "''".into();
    }
    let mut s = String::with_capacity(b.len() * 2 + 3);
    s.push_str("X'");
    for x in b {
        s.push(char::from(HEX[usize::from(x >> 4)]));
        s.push(char::from(HEX[usize::from(x & 15)]));
    }
    s.push('\'');
    s
}

/// A backquoted identifier.
#[must_use]
pub fn ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// A cell as an `i64` (0 for `NULL` or text that isn't a number).
#[must_use]
pub fn cell_i64(c: Option<&Vec<u8>>) -> i64 {
    c.and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// A cell as a `u64`.
#[must_use]
pub fn cell_u64(c: Option<&Vec<u8>>) -> u64 {
    c.and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// A cell as an `f64`.
#[must_use]
pub fn cell_f64(c: Option<&Vec<u8>>) -> f64 {
    c.and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals() {
        assert_eq!(lit(b""), "''");
        assert_eq!(lit(b"\x00a'"), "X'006127'");
        assert_eq!(ident("a`b"), "`a``b`");
    }
}
