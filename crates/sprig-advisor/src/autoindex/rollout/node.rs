//! What rollout and the observer run on a node: the indexes of a table as
//! MariaDB has them, the `PROCESSLIST` checks (user DDL, the metadata-lock
//! queue, sprig builds), replication state, Galera state, a statement
//! watched and killed when the node or the table needs it, and the guard
//! that kills a build its caller abandoned.
//!
//! **Only automatic indexes.** Every statement that changes an index goes
//! through [`auto_ddl`], which refuses a name that isn't
//! `sprig_auto_<8 hex>`: `system`, `user` and `migrated` indexes are never
//! altered or dropped here, whatever a caller passes.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use sprig_cluster::sql::{NodeEndpoint, ProbeError, ProbeSession, TextRow, literal};
use sprig_common::names::{parse_auto_index, quote};
use sprig_common::{Result, SprigError};

use super::super::model::KeyPart;
use super::ShardTable;
use crate::autoindex::validate::galera::{WsrepState, degraded, wsrep_state};

/// One connection to one node.
pub struct Node {
    /// The node's name, for logs.
    pub name: String,
    pub(crate) s: ProbeSession,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// The value of column `name` in `row`.
pub(crate) fn col<'a>(row: &'a TextRow, name: &str) -> Option<&'a str> {
    row.iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .and_then(|(_, v)| v.as_deref())
}

/// An index on a node, from `information_schema.STATISTICS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeIndex {
    /// The name.
    pub name: String,
    /// Key parts; columns in lower case.
    pub parts: Vec<KeyPart>,
    /// `UNIQUE` or the primary key.
    pub unique: bool,
    /// `IGNORED`.
    pub ignored: bool,
}

/// One `PROCESSLIST` row.
#[derive(Debug, Clone, PartialEq)]
pub struct Thread {
    /// `ID`.
    pub id: u64,
    /// `DB`.
    pub db: Option<String>,
    /// `COMMAND`.
    pub command: String,
    /// `STATE`.
    pub state: String,
    /// `TIME_MS`.
    pub time_ms: f64,
    /// `INFO`.
    pub info: Option<String>,
}

impl Thread {
    /// Waiting for a metadata lock.
    #[must_use]
    pub fn waits_for_mdl(&self) -> bool {
        self.state.to_ascii_lowercase().contains("metadata lock")
    }
}

/// Replication of one replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replication {
    /// Both threads run.
    pub running: bool,
    /// `Seconds_Behind_Master`.
    pub lag: Option<u64>,
    /// What is wrong, when something is.
    pub problem: Option<String>,
}

impl Node {
    /// Connects, with `timeout` on every round trip.
    ///
    /// # Errors
    /// The connection failed.
    pub async fn connect(
        ep: &NodeEndpoint,
        timeout: Duration,
    ) -> std::result::Result<Self, ProbeError> {
        Ok(Self {
            name: ep.node.to_string(),
            s: ProbeSession::connect(ep, timeout).await?,
        })
    }

    /// The server's thread id of this connection.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.s.connection_id()
    }

    /// Runs a statement.
    ///
    /// # Errors
    /// The statement failed.
    pub async fn exec(&mut self, sql: &str) -> std::result::Result<(), ProbeError> {
        self.s.exec(sql).await
    }

    /// One value.
    ///
    /// # Errors
    /// The statement failed.
    pub async fn scalar(&mut self, sql: &str) -> std::result::Result<Option<String>, ProbeError> {
        self.s.scalar(sql).await
    }

    /// Rows as text.
    ///
    /// # Errors
    /// The statement failed.
    pub async fn rows(&mut self, sql: &str) -> std::result::Result<Vec<TextRow>, ProbeError> {
        self.s.rows(sql).await
    }

    /// Closes the connection.
    pub async fn close(self) {
        self.s.close().await;
    }

    /// Every index of the table, by name; empty when the table is missing.
    ///
    /// # Errors
    /// The query failed.
    pub async fn indexes(
        &mut self,
        t: &ShardTable,
    ) -> std::result::Result<BTreeMap<String, NodeIndex>, ProbeError> {
        let rows = self
            .rows(&format!(
                "SELECT INDEX_NAME, COLUMN_NAME, SUB_PART, COLLATION, NON_UNIQUE, IGNORED FROM \
                 information_schema.STATISTICS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} \
                 ORDER BY INDEX_NAME, SEQ_IN_INDEX",
                literal(&t.schema),
                literal(&t.table)
            ))
            .await?;
        let mut out: BTreeMap<String, NodeIndex> = BTreeMap::new();
        for r in &rows {
            let Some(name) = col(r, "INDEX_NAME") else {
                continue;
            };
            let e = out.entry(name.to_owned()).or_insert_with(|| NodeIndex {
                name: name.to_owned(),
                parts: Vec::new(),
                unique: col(r, "NON_UNIQUE") == Some("0"),
                ignored: col(r, "IGNORED").is_some_and(|v| v.eq_ignore_ascii_case("YES")),
            });
            e.parts.push(KeyPart {
                column: col(r, "COLUMN_NAME")
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
                prefix: col(r, "SUB_PART").and_then(|v| v.parse().ok()),
                desc: col(r, "COLLATION") == Some("D"),
            });
        }
        Ok(out)
    }

    /// Whether the table exists.
    ///
    /// # Errors
    /// The query failed.
    pub async fn table_exists(&mut self, t: &ShardTable) -> std::result::Result<bool, ProbeError> {
        Ok(self
            .scalar(&format!(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = {} AND \
                 TABLE_NAME = {}",
                literal(&t.schema),
                literal(&t.table)
            ))
            .await?
            .as_deref()
            == Some("1"))
    }

    /// The table's columns, as one string: a change means user DDL ran.
    ///
    /// # Errors
    /// The query failed.
    pub async fn columns_signature(
        &mut self,
        t: &ShardTable,
    ) -> std::result::Result<String, ProbeError> {
        Ok(self
            .scalar(&format!(
                "SELECT GROUP_CONCAT(CONCAT_WS(':', COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE) \
                 ORDER BY ORDINAL_POSITION SEPARATOR ',') FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
                literal(&t.schema),
                literal(&t.table)
            ))
            .await?
            .unwrap_or_default())
    }

    /// Data plus index bytes of the table (`information_schema.TABLES`).
    ///
    /// # Errors
    /// The query failed.
    pub async fn table_bytes(&mut self, t: &ShardTable) -> std::result::Result<u64, ProbeError> {
        Ok(self
            .scalar(&format!(
                "SELECT IFNULL(DATA_LENGTH, 0) + IFNULL(INDEX_LENGTH, 0) FROM \
                 information_schema.TABLES WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
                literal(&t.schema),
                literal(&t.table)
            ))
            .await?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0))
    }

    /// Other threads (not this connection).
    ///
    /// # Errors
    /// The query failed.
    pub async fn threads(&mut self) -> std::result::Result<Vec<Thread>, ProbeError> {
        let rows = self
            .rows(
                "SELECT ID, DB, COMMAND, STATE, TIME_MS, INFO FROM information_schema.PROCESSLIST \
                 WHERE ID <> CONNECTION_ID()",
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| Thread {
                id: col(r, "ID").and_then(|v| v.parse().ok()).unwrap_or(0),
                db: col(r, "DB").map(str::to_owned),
                command: col(r, "COMMAND").unwrap_or_default().to_owned(),
                state: col(r, "STATE").unwrap_or_default().to_owned(),
                time_ms: col(r, "TIME_MS")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0.0),
                info: col(r, "INFO").map(str::to_owned),
            })
            .collect())
    }

    /// Replication state; `None` when the node isn't a replica.
    ///
    /// # Errors
    /// The query failed.
    pub async fn replication(&mut self) -> std::result::Result<Option<Replication>, ProbeError> {
        let rows = self.rows("SHOW SLAVE STATUS").await?;
        Ok(rows.first().map(|r| {
            let io = col(r, "Slave_IO_Running").unwrap_or_default();
            let sql = col(r, "Slave_SQL_Running").unwrap_or_default();
            let running = io.eq_ignore_ascii_case("Yes") && sql.eq_ignore_ascii_case("Yes");
            let lag = col(r, "Seconds_Behind_Master").and_then(|v| v.parse().ok());
            let problem = if running {
                lag.is_none().then(|| "lag unknown".to_owned())
            } else {
                Some(format!(
                    "replication stopped (IO {io}, SQL {sql}, errno {})",
                    col(r, "Last_SQL_Errno").unwrap_or("0")
                ))
            };
            Replication {
                running,
                lag,
                problem,
            }
        }))
    }

    /// Galera state.
    ///
    /// # Errors
    /// The query failed.
    pub(crate) async fn wsrep(&mut self) -> std::result::Result<WsrepState, ProbeError> {
        let rows = self
            .rows(
                "SHOW GLOBAL STATUS WHERE Variable_name IN ('wsrep_local_state_comment', \
                 'wsrep_cluster_size', 'wsrep_cluster_status', 'wsrep_ready')",
            )
            .await?;
        let m: BTreeMap<String, String> = rows
            .iter()
            .filter_map(|r| {
                Some((
                    col(r, "Variable_name")?.to_ascii_lowercase(),
                    col(r, "Value").unwrap_or_default().to_owned(),
                ))
            })
            .collect();
        Ok(wsrep_state(&m))
    }

    /// `@@read_only`.
    ///
    /// # Errors
    /// The query failed.
    pub async fn read_only(&mut self) -> std::result::Result<bool, ProbeError> {
        Ok(self.scalar("SELECT @@read_only").await?.as_deref() == Some("1"))
    }
}

/// Why a Galera node isn't fit: the cluster not Primary, smaller than
/// `min_size`, or the node not Synced.
pub(crate) fn galera_unfit(s: &WsrepState, min_size: u32) -> Option<String> {
    degraded(s, min_size, true)
}

/// Whether `info` is DDL on `schema.table` from someone other than
/// automatic indexing. `db` is the thread's default database. Statements
/// naming `sprig_auto_` are ours (users can't use the name, chapter 13).
#[must_use]
pub fn is_user_ddl(info: &str, db: Option<&str>, schema: &str, table: &str) -> bool {
    let lower = info.to_ascii_lowercase();
    if lower.contains("sprig_auto_") {
        return false;
    }
    let text = lower.replace('`', "");
    let mut words = text
        .split(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ')' || c == ';')
        .filter(|w| !w.is_empty());
    let Some(first) = words.next() else {
        return false;
    };
    if !matches!(
        first,
        "alter" | "create" | "drop" | "rename" | "truncate" | "optimize" | "repair"
    ) {
        return false;
    }
    let (schema, table) = (schema.to_ascii_lowercase(), table.to_ascii_lowercase());
    let qualified = format!("{schema}.{table}");
    let in_schema = db.is_some_and(|d| d.eq_ignore_ascii_case(&schema));
    words.any(|w| w == qualified || (in_schema && w == table))
}

/// Whether `info` is a sprig index build (`ADD INDEX ... sprig_auto_`).
#[must_use]
pub fn is_sprig_build(info: &str) -> bool {
    let l = info.to_ascii_lowercase();
    l.starts_with("alter table") && l.contains("add index") && l.contains("sprig_auto_")
}

/// Whether thread `t` touches `schema.table` (its statement names it).
fn names_table(t: &Thread, schema: &str, table: &str) -> bool {
    let Some(info) = &t.info else {
        return false;
    };
    let l = info.to_ascii_lowercase().replace('`', "");
    let table = table.to_ascii_lowercase();
    l.contains(&format!("{}.{table}", schema.to_ascii_lowercase()))
        || (t
            .db
            .as_deref()
            .is_some_and(|d| d.eq_ignore_ascii_case(schema))
            && l.split(|c: char| !c.is_alphanumeric() && c != '_' && c != '$')
                .any(|w| w == table))
}

/// What the build watcher looks at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    /// User DDL on the table.
    UserDdl,
    /// Clients queue behind a build that hasn't got its lock yet.
    LockQueue,
    /// A replica lags.
    ReplicaLag,
    /// The cluster degraded.
    Degraded,
}

/// The `PROCESSLIST` verdict for one poll of a build on `t`, run as
/// thread `ours`. `started` turns true once the build was seen past its
/// first metadata-lock wait; until then a queue behind it longer than
/// `queue_kill_after` stops it.
#[must_use]
pub fn judge_threads(
    threads: &[Thread],
    ours: u64,
    t: &ShardTable,
    started: &mut bool,
    queue_kill_after: Duration,
) -> Option<(Stop, String)> {
    for th in threads {
        if th.id == ours {
            continue;
        }
        if let Some(info) = &th.info
            && is_user_ddl(info, th.db.as_deref(), &t.schema, &t.table)
        {
            return Some((
                Stop::UserDdl,
                format!("user DDL on {}.{} (thread {})", t.schema, t.table, th.id),
            ));
        }
    }
    let me = threads.iter().find(|th| th.id == ours)?;
    if !me.waits_for_mdl() {
        if me.command.eq_ignore_ascii_case("Query") {
            *started = true;
        }
        return None;
    }
    if *started {
        // The final lock upgrade: lock_wait_timeout bounds it.
        return None;
    }
    let waiting = Duration::from_secs_f64(me.time_ms.max(0.0) / 1000.0);
    let queued = threads
        .iter()
        .filter(|th| th.id != ours && th.waits_for_mdl() && names_table(th, &t.schema, &t.table))
        .count();
    (queued > 0 && waiting >= queue_kill_after).then(|| {
        (
            Stop::LockQueue,
            format!("{queued} statement(s) queued behind the build's lock wait"),
        )
    })
}

/// What a running build is checked against.
#[async_trait::async_trait]
pub trait BuildCheck: Send {
    /// `Some` when the build must stop.
    async fn check(&mut self) -> Option<(Stop, String)>;
}

/// Runs `sql` on `ddl`, polling `check` every `poll`. When `check` stops
/// the statement and `killable`, it is killed (`KILL QUERY` from a fresh
/// connection to `kill_from`), and the reason returned with the
/// statement's result and its time. If the returned future is dropped
/// while the statement runs, [`KillGuard`] kills it.
pub async fn watched(
    ddl: &mut Node,
    kill_from: &NodeEndpoint,
    sql: &str,
    poll: Duration,
    killable: bool,
    check: &mut dyn BuildCheck,
) -> (
    std::result::Result<(), ProbeError>,
    Option<(Stop, String)>,
    Duration,
) {
    let id = ddl.id();
    let mut guard = KillGuard::arm(kill_from.clone(), id, killable);
    let started = Instant::now();
    let stmt = ddl.s.exec(sql);
    tokio::pin!(stmt);
    let mut stopped: Option<(Stop, String)> = None;
    loop {
        tokio::select! {
            res = &mut stmt => {
                guard.disarm();
                return (res, stopped, started.elapsed());
            }
            () = tokio::time::sleep(poll) => {
                if stopped.is_none() && let Some(why) = check.check().await {
                    tracing::warn!(reason = %why.1, killable, "stopping an index build");
                    if killable {
                        kill_query(kill_from, id).await;
                    }
                    stopped = Some(why);
                }
            }
        }
    }
}

/// `KILL QUERY id` on `ep` from a fresh connection.
pub async fn kill_query(ep: &NodeEndpoint, id: u32) {
    match ProbeSession::connect(ep, Duration::from_secs(10)).await {
        Ok(mut s) => {
            if let Err(e) = s.exec(&format!("KILL QUERY {id}")).await {
                tracing::error!(error = %e, thread = id, "KILL QUERY failed");
            }
            s.close().await;
        }
        Err(e) => tracing::error!(error = %e, thread = id, "KILL QUERY: no connection"),
    }
}

/// Kills a running build when dropped armed: a deposed leader's dropped
/// reconcile must not leave an `ALTER` running and holding a slot.
#[derive(Debug)]
pub struct KillGuard {
    ep: Option<NodeEndpoint>,
    id: u32,
}

impl KillGuard {
    /// Arms (or, `armed = false`, creates disarmed) a guard for thread `id`.
    #[must_use]
    pub fn arm(ep: NodeEndpoint, id: u32, armed: bool) -> Self {
        Self {
            ep: armed.then_some(ep),
            id,
        }
    }

    /// The statement finished: nothing to kill.
    pub fn disarm(&mut self) {
        self.ep = None;
    }
}

impl Drop for KillGuard {
    fn drop(&mut self) {
        if let Some(ep) = self.ep.take()
            && let Ok(h) = tokio::runtime::Handle::try_current()
        {
            let id = self.id;
            tracing::warn!(thread = id, "index build abandoned; killing it");
            h.spawn(async move { kill_query(&ep, id).await });
        }
    }
}

/// The text of an index statement on an automatic index, or `Internal`
/// for any other name: the guard that keeps `user`, `migrated` and
/// `system` indexes untouched.
///
/// # Errors
/// `Internal` when `name` isn't `sprig_auto_<8 hex>`.
pub fn auto_ddl(t: &ShardTable, name: &str, clause: &str) -> Result<String> {
    if parse_auto_index(name).is_none() {
        return Err(SprigError::internal(format!(
            "refusing DDL on index {name}: not an automatic index"
        )));
    }
    Ok(format!(
        "ALTER TABLE {}.{} {clause}",
        quote(&t.schema),
        quote(&t.table)
    ))
}

/// `DROP INDEX IF EXISTS x` on an automatic index.
///
/// # Errors
/// As [`auto_ddl`].
pub fn drop_sql(t: &ShardTable, name: &str) -> Result<String> {
    auto_ddl(t, name, &format!("DROP INDEX IF EXISTS {}", quote(name)))
}

/// `ALTER INDEX x [NOT] IGNORED` on an automatic index.
///
/// # Errors
/// As [`auto_ddl`].
pub fn ignore_sql(t: &ShardTable, name: &str, ignored: bool) -> Result<String> {
    auto_ddl(
        t,
        name,
        &format!(
            "ALTER INDEX {} {}",
            quote(name),
            if ignored { "IGNORED" } else { "NOT IGNORED" }
        ),
    )
}

/// Elapsed ms, saturating.
#[must_use]
pub fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}
