//! One probe connection to one MariaDB node, with a timeout on every round
//! trip (chapter 03, "Concurrency").
//!
//! Results come back as text, the way `SHOW` returns them, so the parsers in
//! [`crate::probe::parse`] work the same on live rows and on captured
//! fixtures.

use std::fmt;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, Row, Value};
use serde::{Deserialize, Serialize};
use sprig_common::SprigError;
use sprig_common::ids::NodeId;

/// One row of a text result: column names with their values.
pub type TextRow = Vec<(String, Option<String>)>;

/// Where a node is and how to log in.
#[derive(Clone)]
pub struct NodeEndpoint {
    /// The node.
    pub node: NodeId,
    /// Connection options: address, account, TLS.
    pub opts: Opts,
}

impl fmt::Debug for NodeEndpoint {
    // Never print the password.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeEndpoint")
            .field("node", &self.node)
            .field("addr", &self.addr())
            .field("user", &self.opts.user())
            .finish_non_exhaustive()
    }
}

impl NodeEndpoint {
    /// An endpoint from a `mysql://user:pass@host:port` URL.
    ///
    /// # Errors
    /// `Internal` if the URL doesn't parse.
    pub fn from_url(node: NodeId, url: &str) -> sprig_common::Result<Self> {
        let opts = Opts::from_url(url)
            .map_err(|e| SprigError::internal(format!("bad MariaDB URL for {node}: {e}")))?;
        Ok(Self { node, opts })
    }

    /// `host:port`, for logs.
    #[must_use]
    pub fn addr(&self) -> String {
        format!("{}:{}", self.opts.ip_or_hostname(), self.opts.tcp_port())
    }

    /// The binlog reader's view of this endpoint: the same address, account
    /// and TLS (chapter 10). Endpoints built from a cluster's credentials
    /// always carry TLS, so the reader verifies the server against the
    /// credential's CA (`tls = "verify"`, `ca_file`) or encrypts without
    /// verifying (`skip_verify`); an endpoint without TLS options (a
    /// development URL) reads in plain text.
    pub async fn binlog_source(&self) -> sprig_binlog::BinlogSource {
        let src = sprig_binlog::BinlogSource::new(
            self.opts.ip_or_hostname(),
            self.opts.tcp_port(),
            self.opts.user().unwrap_or_default(),
            self.opts.pass().unwrap_or_default(),
        );
        match self.opts.ssl_opts() {
            Some(ssl) => src.with_tls(binlog_tls(ssl).await),
            None => src,
        }
    }
}

/// `mysql_async`'s TLS options as the binlog reader's: the CA files are read
/// now (a CA that can't be read is logged and left out, so verification
/// fails rather than passing). A check that skips only the host name (not
/// something credentials produce) verifies fully.
async fn binlog_tls(ssl: &mysql_async::SslOpts) -> sprig_binlog::BinlogTls {
    let mut tls = if ssl.accept_invalid_certs() {
        sprig_binlog::BinlogTls::skip_verify()
    } else {
        sprig_binlog::BinlogTls::verify(None)
    };
    let mut pem = Vec::new();
    for root in ssl.root_certs() {
        match root.read().await {
            Ok(b) => pem.extend_from_slice(&b),
            Err(e) => tracing::warn!(error = %e, "CA certificate not read"),
        }
    }
    if !ssl.root_certs().is_empty() {
        tls.ca_pem = Some(pem);
    }
    tls.server_name = ssl.tls_hostname_override().map(str::to_owned);
    tls
}

/// How a [`ProbeError::wrong_node`] message starts.
pub const WRONG_NODE: &str = "wrong node:";

/// Why a probe statement or connection failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeError {
    /// MariaDB's error number, for server errors.
    pub errno: Option<u16>,
    /// What happened. Internal detail: logged, never sent to clients.
    pub message: String,
    /// The round trip timed out.
    pub timeout: bool,
}

impl ProbeError {
    pub(crate) fn other(message: impl Into<String>) -> Self {
        Self {
            errno: None,
            message: message.into(),
            timeout: false,
        }
    }

    /// The server returned an error (as opposed to a lost connection or a
    /// timeout): the statement ran and was refused.
    #[must_use]
    pub fn is_server_error(&self) -> bool {
        self.errno.is_some()
    }

    /// Authentication failed (1045), or the address answers as another
    /// server than the record pinned ([`WRONG_NODE`]): the node is
    /// `broken`, not unreachable.
    #[must_use]
    pub fn is_auth_failure(&self) -> bool {
        self.errno == Some(1045) || self.message.starts_with(WRONG_NODE)
    }

    /// The probe reached another server than the one the record pinned.
    #[must_use]
    pub fn wrong_node(expected: &str, found: &str) -> Self {
        Self::other(format!(
            "{WRONG_NODE} the address answers as {found}, not the pinned {expected} \
             (a reused address? update the record's nodes)"
        ))
    }
}

impl fmt::Display for ProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.errno, self.timeout) {
            (_, true) => write!(f, "timed out: {}", self.message),
            (Some(e), _) => write!(f, "error {e}: {}", self.message),
            (None, _) => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for ProbeError {}

impl From<mysql_async::Error> for ProbeError {
    fn from(e: mysql_async::Error) -> Self {
        match e {
            mysql_async::Error::Server(s) => Self {
                errno: Some(s.code),
                message: s.message,
                timeout: false,
            },
            other => Self::other(other.to_string()),
        }
    }
}

impl From<ProbeError> for SprigError {
    fn from(e: ProbeError) -> Self {
        let msg = format!("cluster probe failed: {e}");
        match e.errno {
            Some(errno) => SprigError::backend(errno, *b"HY000", msg),
            None => SprigError::internal(msg),
        }
    }
}

/// A connection used by probes and the heartbeat writer.
pub struct ProbeSession {
    conn: Conn,
    timeout: Duration,
}

impl fmt::Debug for ProbeSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProbeSession")
            .field("id", &self.conn.id())
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl ProbeSession {
    /// Connects, giving up after `timeout`.
    ///
    /// # Errors
    /// The connection or login failed or timed out.
    pub async fn connect(ep: &NodeEndpoint, timeout: Duration) -> Result<Self, ProbeError> {
        let conn = within(timeout, Conn::new(ep.opts.clone())).await?;
        Ok(Self { conn, timeout })
    }

    /// The server's connection id.
    #[must_use]
    pub fn connection_id(&self) -> u32 {
        self.conn.id()
    }

    /// The timeout of every round trip.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Runs a statement and returns every row as text.
    ///
    /// # Errors
    /// The statement failed or timed out.
    pub async fn rows(&mut self, sql: &str) -> Result<Vec<TextRow>, ProbeError> {
        let rows: Vec<Row> = within(self.timeout, self.conn.query(sql)).await?;
        Ok(rows.into_iter().map(text_row).collect())
    }

    /// Runs a statement and returns the first column of the first row.
    ///
    /// # Errors
    /// The statement failed or timed out.
    pub async fn scalar(&mut self, sql: &str) -> Result<Option<String>, ProbeError> {
        let rows = self.rows(sql).await?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next())
            .and_then(|(_, v)| v))
    }

    /// Runs a statement, ignoring any result.
    ///
    /// # Errors
    /// The statement failed or timed out.
    pub async fn exec(&mut self, sql: &str) -> Result<(), ProbeError> {
        within(self.timeout, self.conn.query_drop(sql)).await
    }

    /// Closes the connection politely; errors are ignored.
    pub async fn close(self) {
        let _ = tokio::time::timeout(self.timeout, self.conn.disconnect()).await;
    }
}

async fn within<T>(
    timeout: Duration,
    fut: impl Future<Output = mysql_async::Result<T>>,
) -> Result<T, ProbeError> {
    match tokio::time::timeout(timeout, fut).await {
        Ok(r) => r.map_err(ProbeError::from),
        Err(_) => Err(ProbeError {
            errno: None,
            message: format!("no answer within {} ms", timeout.as_millis()),
            timeout: true,
        }),
    }
}

fn text_row(row: Row) -> TextRow {
    let names: Vec<String> = row
        .columns_ref()
        .iter()
        .map(|c| c.name_str().into_owned())
        .collect();
    names
        .into_iter()
        .zip(row.unwrap())
        .map(|(n, v)| (n, value_text(v)))
        .collect()
}

fn value_text(v: Value) -> Option<String> {
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(String::from_utf8_lossy(&b).into_owned()),
        other => Some(other.as_sql(true).trim_matches('\'').to_owned()),
    }
}

/// A single-quoted SQL string literal, with quotes and backslashes escaped.
#[must_use]
pub fn literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            '\0' => out.push_str("\\0"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_escape() {
        assert_eq!(literal("a'b\\c"), "'a''b\\\\c'");
    }

    #[test]
    fn endpoint_debug_hides_password() {
        let ep = NodeEndpoint::from_url(
            NodeId::new("n1").unwrap(),
            "mysql://root:secret@127.0.0.1:13301",
        )
        .unwrap();
        let s = format!("{ep:?}");
        assert!(!s.contains("secret"), "{s}");
        assert!(s.contains("127.0.0.1:13301"));
    }
}
