//! Backend connection setup (chapter 06, "New connections"; build step
//! 1): connect with TLS, initialize session tracking and timeouts, apply
//! the persona baseline, read the 64-bit thread id and the pool's
//! [`Baseline`], all in one round trip after login. Also the per-connection
//! prepared-statement cache and the captures that run before a release.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_proto_mysql::client::{ClientConfig, ClientError, MysqlClient, Prepared, TlsMode};
use sprig_proto_mysql::{Caps, ColumnDef};
use zeroize::Zeroizing;

use super::cluster::NodeKey;
use super::state::{AppliedState, Baseline, VarValue, allowed_vars, persona, quote_ident};

/// Everything needed to open one backend connection.
#[derive(Clone)]
pub struct ConnectSpec {
    /// The node.
    pub node: NodeKey,
    /// `host:port`.
    pub addr: String,
    /// The backend account.
    pub user: String,
    /// Its password.
    pub password: Zeroizing<String>,
    /// TLS configuration (TLS is always required).
    pub tls: Arc<rustls::ClientConfig>,
    /// Negotiate `CLIENT_FOUND_ROWS`.
    pub found_rows: bool,
    /// MariaDB extended capabilities to negotiate.
    pub mariadb_caps: Caps,
    /// The node is a Galera node: `wsrep_sync_wait` starts at 0.
    pub galera: bool,
    /// zlib compression (`backend.compress_cross_dc` for another DC).
    pub compress: bool,
    /// `backend.connect_timeout_ms`: TCP, TLS and init together.
    pub connect_timeout: Duration,
    /// `backend.net_write_timeout_seconds`.
    pub net_write_timeout_secs: u64,
    /// `wait_timeout` for the connection, above `backend.idle_timeout_seconds`.
    pub wait_timeout_secs: u64,
}

impl std::fmt::Debug for ConnectSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectSpec")
            .field("node", &self.node)
            .field("addr", &self.addr)
            .field("user", &self.user)
            .field("found_rows", &self.found_rows)
            .field("galera", &self.galera)
            .finish_non_exhaustive()
    }
}

impl ConnectSpec {
    /// The gateway-owned variables (chapter 06, "New connections"):
    /// session tracking and timeouts, and `wsrep_sync_wait = 0` on Galera.
    #[must_use]
    pub fn gateway_sql(&self) -> String {
        let wsrep = if self.galera {
            ", wsrep_sync_wait = 0"
        } else {
            ""
        };
        format!(
            "SET SESSION session_track_system_variables = '*', session_track_schema = ON, \
             session_track_state_change = ON, session_track_transaction_info = 'STATE', \
             net_write_timeout = {}, wait_timeout = {}{wsrep}",
            self.net_write_timeout_secs, self.wait_timeout_secs,
        )
    }

    /// The two initialization statements (chapter 06): the gateway-owned
    /// variables, then the persona baseline.
    #[must_use]
    pub fn init_sql(&self) -> String {
        format!(
            "{}; SET NAMES utf8mb4 COLLATE {}, SESSION sql_mode = '{}'",
            self.gateway_sql(),
            persona::COLLATION,
            persona::MARIADB_SQL_MODE
        )
    }
}

/// What [`BackendConn::restore_gateway_vars`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Restored {
    /// The values the client's statement gave the gateway-owned variables.
    pub client: Vec<(String, String)>,
    /// Every allowed variable's value on the connection afterwards.
    pub allowed: Vec<(String, String)>,
}

/// Counts a node's backend prepared statements against this gateway's
/// share of `max_prepared_stmt_count` (chapter 06, "Prepared statements
/// per node"; build step 9).
#[derive(Debug, Default)]
pub struct PreparedBudget {
    used: AtomicU64,
    limit: AtomicU64,
}

impl PreparedBudget {
    /// A budget of `limit` statements.
    #[must_use]
    pub fn new(limit: u64) -> Self {
        Self {
            used: AtomicU64::new(0),
            limit: AtomicU64::new(limit),
        }
    }

    /// Statements prepared now.
    #[must_use]
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    /// The share.
    #[must_use]
    pub fn limit(&self) -> u64 {
        self.limit.load(Ordering::Relaxed)
    }

    /// Sets the share: `max_prepared_stmt_count * pct / 100 / gateways`.
    pub fn set_limit(&self, max_prepared: u64, pct: u32, gateways: u32) {
        let share = max_prepared.saturating_mul(u64::from(pct)) / 100 / u64::from(gateways.max(1));
        self.limit.store(share.max(1), Ordering::Relaxed);
    }

    fn over(&self) -> bool {
        self.used() >= self.limit()
    }

    fn add(&self, n: u64) {
        self.used.fetch_add(n, Ordering::Relaxed);
    }

    fn sub(&self, n: u64) {
        let _ = self
            .used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |u| {
                Some(u.saturating_sub(n))
            });
    }
}

/// A backend prepared statement on one connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendStmt {
    /// The backend statement id.
    pub id: u32,
    /// Parameter count.
    pub params: u16,
    /// Column definitions, as the backend sent them.
    pub columns: Vec<ColumnDef>,
    /// Parameter definitions, as the backend sent them (for `PREPARE_OK`).
    pub param_defs: Vec<ColumnDef>,
    /// Whether an execute has sent parameter types to this statement; a
    /// re-prepared statement needs them again (chapter 05).
    pub types_sent: bool,
}

/// The connection's statement cache: default schema and SQL text to
/// backend statement, least recently used first out.
///
/// The schema is part of the key because MariaDB resolves unqualified
/// names (and `DATABASE()`) against the default schema current at prepare
/// time, not at execute time: a statement prepared for a session in schema
/// `a` must not serve a later session of the same pooled connection in
/// schema `b`.
#[derive(Debug, Default)]
pub struct StmtCache {
    map: HashMap<Vec<u8>, (u64, BackendStmt)>,
    tick: u64,
}

impl StmtCache {
    /// Cached statements.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// The statement for `key` (schema and SQL text, as `stmt_key` makes
    /// it), marked as used.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut BackendStmt> {
        self.tick += 1;
        let tick = self.tick;
        self.map.get_mut(key).map(|(t, s)| {
            *t = tick;
            s
        })
    }

    fn insert(&mut self, key: &[u8], stmt: BackendStmt) {
        self.tick += 1;
        self.map.insert(key.to_vec(), (self.tick, stmt));
    }

    fn pop_lru(&mut self) -> Option<BackendStmt> {
        let key = self
            .map
            .iter()
            .min_by_key(|(_, (t, _))| *t)
            .map(|(k, _)| k.clone())?;
        self.map.remove(&key).map(|(_, s)| s)
    }
}

/// The statement cache's key: the default schema the statement is
/// prepared under (none: `0`; else `1`, the name, `0`), the parse context
/// ([`super::state::AppliedState::parse_context`]) and `0`, then the SQL
/// text.
fn stmt_key(schema: Option<&str>, context: &str, sql: &[u8]) -> Vec<u8> {
    let mut key =
        Vec::with_capacity(sql.len() + context.len() + 1 + schema.map_or(1, |s| s.len() + 2));
    match schema {
        None => key.push(0),
        Some(s) => {
            key.push(1);
            key.extend_from_slice(s.as_bytes());
            key.push(0);
        }
    }
    key.extend_from_slice(context.as_bytes());
    key.push(0);
    key.extend_from_slice(sql);
    key
}

/// One warning, as `SHOW WARNINGS` lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Warning {
    /// `Note`, `Warning` or `Error`.
    pub level: String,
    /// Error number.
    pub code: u16,
    /// Message.
    pub message: String,
}

/// A pooled backend connection (chapter 06, "Pools and connections").
pub struct BackendConn {
    /// The protocol client.
    pub client: MysqlClient,
    /// `CONNECTION_ID()`, for `KILL QUERY`.
    pub thread_id: u64,
    /// The session state this connection holds.
    pub applied: AppliedState,
    /// SQL text to backend statement.
    pub stmt_cache: StmtCache,
    /// When it connected.
    pub created_at: Instant,
    /// The node.
    pub node: NodeKey,
    /// The allowed variables' values right after initialization.
    pub baseline: Arc<Baseline>,
    /// `@@global.max_prepared_stmt_count` at connect.
    pub max_prepared: u64,
    /// What one [`HANDLER_READS_SQL`] adds to the counters it reads,
    /// measured on this connection's first probe (chapter 12, "Sampling
    /// rows examined").
    pub probe_cost: Option<u64>,
    init_sql: Arc<str>,
    gateway_sql: Arc<str>,
    budget: Arc<PreparedBudget>,
}

impl std::fmt::Debug for BackendConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendConn")
            .field("node", &self.node)
            .field("thread_id", &self.thread_id)
            .field("applied", &self.applied)
            .finish_non_exhaustive()
    }
}

impl Drop for BackendConn {
    fn drop(&mut self) {
        self.budget.sub(self.stmt_cache.len() as u64);
    }
}

fn text(v: Option<&Bytes>) -> String {
    v.map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default()
}

/// The `Handler_read_*` counters of the connection's own thread, summed
/// (chapter 12, "Sampling rows examined"): one row, one value. Reading
/// them bumps some of them by a constant, measured per connection.
pub const HANDLER_READS_SQL: &str = "SELECT SUM(VARIABLE_VALUE) FROM information_schema.SESSION_STATUS \
     WHERE VARIABLE_NAME IN ('HANDLER_READ_FIRST', 'HANDLER_READ_KEY', 'HANDLER_READ_LAST', \
     'HANDLER_READ_NEXT', 'HANDLER_READ_PREV', 'HANDLER_READ_RND', 'HANDLER_READ_RND_NEXT')";

/// Opens and initializes a backend connection.
///
/// # Errors
/// The client's error: connect, TLS, login (the server's ERR) or an
/// initialization statement that failed.
pub async fn connect(
    spec: &ConnectSpec,
    budget: Arc<PreparedBudget>,
) -> Result<BackendConn, ClientError> {
    let mut cfg = ClientConfig::new(spec.user.clone(), spec.password.as_str());
    cfg.tls = TlsMode::Required;
    cfg.tls_config = Some(Arc::clone(&spec.tls));
    cfg.found_rows = spec.found_rows;
    // The relay answers a `LOCAL INFILE` request only for the file the
    // client named, and declines any other (chapter 05), so backends may
    // always offer it.
    cfg.local_files = true;
    cfg.compress = spec.compress;
    cfg.extra_caps = spec.mariadb_caps;
    cfg.connect_timeout = spec.connect_timeout;
    cfg.attrs
        .push(("program_name".to_owned(), "sprig-gateway".to_owned()));
    let deadline = Instant::now() + spec.connect_timeout;
    let mut client = MysqlClient::connect(&spec.addr, &cfg).await?;
    let init_sql: Arc<str> = Arc::from(spec.init_sql());
    let gateway_sql: Arc<str> = Arc::from(spec.gateway_sql());
    let left = deadline.saturating_duration_since(Instant::now());
    let sql = format!(
        "{init_sql}; SELECT CONNECTION_ID(), @@global.max_prepared_stmt_count; {}",
        allowed_vars_sql()
    );
    let results = tokio::time::timeout(left, client.query(&sql))
        .await
        .map_err(|_| ClientError::Timeout("backend connection initialization"))??;
    let mut ids = results.iter().filter(|r| !r.columns.is_empty());
    let id_row = ids
        .next()
        .and_then(|r| r.text_rows().ok())
        .and_then(|rows| rows.into_iter().next())
        .ok_or_else(|| ClientError::Unexpected("no CONNECTION_ID() row".to_owned()))?;
    let thread_id = text(id_row.first().and_then(Option::as_ref))
        .parse()
        .map_err(|_| ClientError::Unexpected("CONNECTION_ID() is not a number".to_owned()))?;
    let max_prepared = text(id_row.get(1).and_then(Option::as_ref))
        .parse()
        .unwrap_or(16_382);
    let mut baseline = Baseline::default();
    if let Some(rs) = ids.next() {
        for row in rs.text_rows()? {
            let name = text(row.first().and_then(Option::as_ref));
            let value = text(row.get(1).and_then(Option::as_ref));
            baseline.vars.insert(name, value);
        }
    }
    client.take_changes();
    let applied = AppliedState {
        wsrep_sync_wait: 0,
        ..AppliedState::default()
    };
    Ok(BackendConn {
        client,
        thread_id,
        applied,
        stmt_cache: StmtCache::default(),
        created_at: Instant::now(),
        node: spec.node.clone(),
        baseline: Arc::new(baseline),
        max_prepared,
        probe_cost: None,
        init_sql,
        gateway_sql,
        budget,
    })
}

/// The query that reads every allowed variable's session value.
fn allowed_vars_sql() -> String {
    let names: Vec<String> = allowed_vars()
        .map(|n| format!("'{}'", n.to_ascii_uppercase()))
        .collect();
    format!(
        "SELECT LOWER(VARIABLE_NAME), VARIABLE_VALUE FROM information_schema.SESSION_VARIABLES \
         WHERE VARIABLE_NAME IN ({})",
        names.join(",")
    )
}

impl BackendConn {
    /// How long ago it connected.
    #[must_use]
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// The cached backend statement for `sql`, prepared now if absent.
    /// Above the node's budget, this connection's least recently used
    /// statements are closed first (executes re-prepare them lazily).
    ///
    /// # Errors
    /// The server's ERR for a bad statement; connection errors.
    pub async fn prepare_cached(&mut self, sql: &[u8]) -> Result<&mut BackendStmt, ClientError> {
        let context = self.applied.parse_context(&self.baseline);
        let key = stmt_key(self.applied.schema.as_deref(), &context, sql);
        if self.stmt_cache.get_mut(&key).is_none() {
            while self.budget.over() {
                let Some(old) = self.stmt_cache.pop_lru() else {
                    break;
                };
                self.budget.sub(1);
                metrics::counter!("sprig_backend_prepared_evictions_total", "cluster" => self.node.cluster.to_string()).increment(1);
                self.client.close_statement(old.id).await?;
            }
            let Prepared {
                ok,
                columns,
                params,
            } = self.client.prepare(sql).await?;
            self.budget.add(1);
            self.stmt_cache.insert(
                &key,
                BackendStmt {
                    id: ok.stmt_id,
                    params: ok.params,
                    columns,
                    param_defs: params,
                    types_sent: false,
                },
            );
        }
        self.stmt_cache
            .get_mut(&key)
            .ok_or_else(|| ClientError::Unexpected("statement cache lost an entry".to_owned()))
    }

    /// Reads the values of user variables (a capture before release).
    ///
    /// # Errors
    /// Connection errors; the server's ERR.
    pub async fn capture_user_vars(
        &mut self,
        names: &[String],
    ) -> Result<Vec<(String, VarValue)>, ClientError> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let cols: Vec<String> = names
            .iter()
            .map(|n| format!("@{}", quote_ident(n)))
            .collect();
        let rs = self
            .client
            .query(&format!("SELECT {}", cols.join(", ")))
            .await?;
        let rs = rs
            .into_iter()
            .find(|r| !r.columns.is_empty())
            .ok_or_else(|| ClientError::Unexpected("no user-variable row".to_owned()))?;
        let rows = rs.text_rows()?;
        let row = rows.into_iter().next().unwrap_or_default();
        let mut out = Vec::with_capacity(names.len());
        for (i, name) in names.iter().enumerate() {
            let col = rs.columns.get(i);
            let v = VarValue::from_column(
                col.map_or(0, |c| c.col_type),
                col.map_or(63, |c| c.charset),
                row.get(i).and_then(Option::as_deref),
            );
            out.push((name.clone(), v));
        }
        Ok(out)
    }

    /// The connection's `Handler_read_*` sum ([`HANDLER_READS_SQL`]).
    ///
    /// # Errors
    /// Connection errors; the server's ERR; `Unexpected` for a value that
    /// isn't a number.
    pub async fn handler_reads(&mut self) -> Result<u64, ClientError> {
        let row = self.client.query_first(HANDLER_READS_SQL).await?;
        let v = row
            .and_then(|r| r.into_iter().next().flatten())
            .and_then(|b| {
                let t = std::str::from_utf8(&b).ok()?.trim().to_owned();
                // SUM over a string column comes back as a DOUBLE's text.
                t.parse::<u64>().ok().or_else(|| {
                    t.parse::<f64>()
                        .ok()
                        .filter(|f| f.is_finite() && *f >= 0.0)
                        .map(|f| {
                            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                            let n = f as u64;
                            n
                        })
                })
            })
            .ok_or_else(|| ClientError::Unexpected("Handler_read_* sum".to_owned()))?;
        self.client.take_changes();
        Ok(v)
    }

    /// The first reading of a probe: on the connection's first probe it
    /// reads twice, and the difference is the probe's own cost
    /// ([`BackendConn::probe_cost`]).
    ///
    /// # Errors
    /// As [`BackendConn::handler_reads`].
    pub async fn probe_start(&mut self) -> Result<u64, ClientError> {
        let first = self.handler_reads().await?;
        if self.probe_cost.is_some() {
            return Ok(first);
        }
        let second = self.handler_reads().await?;
        self.probe_cost = Some(second.saturating_sub(first));
        Ok(second)
    }

    /// Rows examined since `before` ([`BackendConn::probe_start`]'s
    /// reading): the second reading minus the first, minus the first
    /// reading's own cost.
    ///
    /// # Errors
    /// As [`BackendConn::handler_reads`].
    pub async fn probe_end(&mut self, before: u64) -> Result<u64, ClientError> {
        let after = self.handler_reads().await?;
        Ok(after
            .saturating_sub(before)
            .saturating_sub(self.probe_cost.unwrap_or(0)))
    }

    /// `SHOW WARNINGS` (a capture before release).
    ///
    /// # Errors
    /// Connection errors; the server's ERR.
    pub async fn capture_warnings(&mut self) -> Result<Vec<Warning>, ClientError> {
        let rs = self.client.query("SHOW WARNINGS").await?;
        let mut out = Vec::new();
        for r in rs.iter().filter(|r| !r.columns.is_empty()) {
            for row in r.text_rows()? {
                out.push(Warning {
                    level: text(row.first().and_then(Option::as_ref)),
                    code: text(row.get(1).and_then(Option::as_ref))
                        .parse()
                        .unwrap_or(0),
                    message: text(row.get(2).and_then(Option::as_ref)),
                });
            }
        }
        Ok(out)
    }

    /// `SELECT FOUND_ROWS()` (a capture before release).
    ///
    /// # Errors
    /// Connection errors; the server's ERR.
    pub async fn capture_found_rows(&mut self) -> Result<u64, ClientError> {
        let row = self.client.query_first("SELECT FOUND_ROWS()").await?;
        Ok(row
            .and_then(|r| r.into_iter().next().flatten())
            .and_then(|b| std::str::from_utf8(&b).ok()?.parse().ok())
            .unwrap_or(0))
    }

    /// After a client statement that set gateway-owned variables (chapter
    /// 06, "Replay rules"): reads the values it gave `names`, puts the
    /// gateway's own values back (initialization's, or the server default
    /// for the ones initialization leaves alone), and reads every allowed
    /// variable, since tracking may have been off for part of the
    /// statement. One round trip.
    ///
    /// # Errors
    /// Connection errors; the server's ERR (the connection's tracking is
    /// then unknown: discard it).
    pub async fn restore_gateway_vars(
        &mut self,
        names: &[String],
    ) -> Result<Restored, ClientError> {
        // Only names from the gateway's fixed list reach this text.
        let names: Vec<&String> = names
            .iter()
            .filter(|n| super::state::settable_gateway_var(n))
            .collect();
        let mut parts = Vec::with_capacity(3);
        if !names.is_empty() {
            let cols: Vec<String> = names.iter().map(|n| format!("@@session.{n}")).collect();
            parts.push(format!("SELECT {}", cols.join(", ")));
        }
        let mut restore = vec![self.gateway_sql.to_string()];
        restore.extend(
            names
                .iter()
                .filter(|n| !self.gateway_sql.contains(&format!("{n} =")))
                .map(|n| format!("{n} = DEFAULT")),
        );
        parts.push(restore.join(", "));
        parts.push(allowed_vars_sql());
        let sql = parts.join("; ");
        let results = self.client.query(&sql).await?;
        self.client.take_changes();
        let mut sets = results.iter().filter(|r| !r.columns.is_empty());
        let mut out = Restored::default();
        if !names.is_empty()
            && let Some(rs) = sets.next()
        {
            let row = rs.text_rows()?.into_iter().next().unwrap_or_default();
            for (i, n) in names.iter().enumerate() {
                out.client
                    .push(((*n).clone(), text(row.get(i).and_then(Option::as_ref))));
            }
        }
        if let Some(rs) = sets.next() {
            for row in rs.text_rows()? {
                out.allowed.push((
                    text(row.first().and_then(Option::as_ref)),
                    text(row.get(1).and_then(Option::as_ref)),
                ));
            }
        }
        Ok(out)
    }

    /// `@@collation_connection`, after a statement changed the character
    /// sets (session tracking reports the character set's name only). It
    /// leaves the statement's warnings in place.
    ///
    /// # Errors
    /// Connection errors; the server's ERR.
    pub async fn capture_collation(&mut self) -> Result<String, ClientError> {
        let row = self
            .client
            .query_first("SELECT @@session.collation_connection")
            .await?;
        self.client.take_changes();
        Ok(text(
            row.as_ref()
                .and_then(|r| r.first())
                .and_then(Option::as_ref),
        ))
    }

    /// `COM_RESET_CONNECTION`, then the initialization again: the
    /// connection is back at the baseline, with no statements, user
    /// variables or temporary tables.
    ///
    /// # Errors
    /// Connection errors; the server's ERR.
    pub async fn reset(&mut self) -> Result<(), ClientError> {
        self.client.reset_connection().await?;
        self.budget.sub(self.stmt_cache.len() as u64);
        self.stmt_cache = StmtCache::default();
        let sql = Arc::clone(&self.init_sql);
        self.client.query_drop(&sql).await?;
        self.client.take_changes();
        self.applied = AppliedState::default();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::stmt_key;

    #[test]
    fn statement_keys_include_the_schema() {
        let sql = b"SELECT v FROM t";
        assert_ne!(stmt_key(Some("a"), "", sql), stmt_key(Some("b"), "", sql));
        assert_ne!(stmt_key(None, "", sql), stmt_key(Some("a"), "", sql));
        assert_eq!(stmt_key(Some("a"), "", sql), stmt_key(Some("a"), "", sql));
        // A statement prepared under other character sets or another
        // sql_mode parses differently: its literals keep the collation
        // they were prepared with.
        assert_ne!(stmt_key(Some("a"), "x", sql), stmt_key(Some("a"), "y", sql));
        // The separator keeps schema and text apart.
        assert_ne!(
            stmt_key(Some("ab"), "", b"c"),
            stmt_key(Some("a"), "", b"bc")
        );
    }
}
