//! The `sinks` catalog module: `sink_connections`, `sink_secrets`, `sinks`,
//! `sink_tasks` and `sink_usage` in `t<tenant>_sprig`, their migration, the
//! snapshot loader ([`SinksExtension`]) and the writes the statements and tasks
//! make.
//!
//! **Storage notes:**
//! - Sinks and connections have no `objects` row yet (as streams and
//!   queues have none): their ids come from the object counter, and
//!   privileges are checked by name (`SPRIG_SINK_ADMIN` on `*.*`).
//! - `sinks.source_ids` holds the whole source (`{"kind": "stream", "id":
//!   41, "name": ..., "partitions": 12}`), `target` the target with its
//!   script, `connection_ids` the references (tenant or platform), and two
//!   more columns hold `transform_name` (the logic function by name) and
//!   `options` (the `WITH (...)` values).
//! - `sink_usage` holds `GRANT USAGE ON [PLATFORM] CONNECTION c TO u`, by
//!   connection name and user id (grants have no connection object yet).

use std::collections::BTreeMap;
use std::sync::Arc;

use mysql_async::Row;
use mysql_async::prelude::Queryable;
use serde_json::{Map, Value};
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{MigrateOpts, Migration, Module, Phase};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, Migrator};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use crate::model::{
    ConnKind, ConnectionDef, ConnectionId, Guarantee, SecretMeta, SinkDef, SinkId, SinkState,
    TaskEpoch, TaskNo, TaskRow, TaskState, err,
};

/// The module name.
pub const MODULE: &str = "sinks";

const DDL_CONNECTIONS: &str = "CREATE TABLE IF NOT EXISTS sink_connections (
  connection_id   BIGINT UNSIGNED NOT NULL,
  db_id           INT UNSIGNED NOT NULL,
  name            VARCHAR(64) NOT NULL,
  kind            ENUM('webhook','kafka','s3','mysql','postgresql') NOT NULL,
  options         JSON NOT NULL,
  hosts           JSON NOT NULL,
  residency_tags  JSON NULL,
  owner_user_id   INT UNSIGNED NOT NULL,
  created_at      DATETIME(3) NOT NULL,
  updated_at      DATETIME(3) NOT NULL,
  PRIMARY KEY (connection_id),
  UNIQUE KEY uk_conn_name (db_id, name)
) ENGINE=InnoDB";

const DDL_SECRETS: &str = "CREATE TABLE IF NOT EXISTS sink_secrets (
  connection_id   BIGINT UNSIGNED NOT NULL,
  name            VARCHAR(64) NOT NULL,
  version         INT UNSIGNED NOT NULL,
  key_version     INT UNSIGNED NOT NULL,
  sealed          VARBINARY(16384) NOT NULL,
  state           ENUM('current','previous','retired') NOT NULL,
  set_by          INT UNSIGNED NOT NULL,
  set_at          DATETIME(3) NOT NULL,
  PRIMARY KEY (connection_id, name, version)
) ENGINE=InnoDB";

const DDL_SINKS: &str = "CREATE TABLE IF NOT EXISTS sinks (
  sink_id         BIGINT UNSIGNED NOT NULL,
  db_id           INT UNSIGNED NOT NULL,
  name            VARCHAR(64) NOT NULL,
  source_kind     ENUM('stream','queue','changes') NOT NULL,
  source_ids      JSON NOT NULL,
  filter_sql      TEXT NULL,
  projection      JSON NULL,
  transform_fn    BIGINT UNSIGNED NULL,
  transform_name  VARCHAR(64) NULL,
  target_kind     ENUM('webhook','kafka','s3','database','script') NOT NULL,
  connection_ids  JSON NOT NULL,
  target          JSON NOT NULL,
  guarantee       ENUM('at_least_once','exactly_once') NOT NULL,
  dead_letter     JSON NULL,
  start_at        JSON NOT NULL,
  owner_user_id   INT UNSIGNED NOT NULL,
  state           ENUM('creating','running','paused','failing','failed','dropping') NOT NULL,
  state_reason    VARCHAR(1024) NULL,
  definition_ver  BIGINT UNSIGNED NOT NULL,
  options         JSON NULL,
  created_at      DATETIME(3) NOT NULL,
  updated_at      DATETIME(3) NOT NULL,
  PRIMARY KEY (sink_id),
  UNIQUE KEY uk_sink_name (db_id, name)
) ENGINE=InnoDB";

const DDL_TASKS: &str = "CREATE TABLE IF NOT EXISTS sink_tasks (
  sink_id         BIGINT UNSIGNED NOT NULL,
  task_no         INT UNSIGNED NOT NULL,
  source_part     JSON NOT NULL,
  epoch           BIGINT UNSIGNED NOT NULL,
  worker_id       VARCHAR(64) NULL,
  pending         JSON NULL,
  state           ENUM('idle','running','retrying','paused','failed') NOT NULL,
  attempts        INT UNSIGNED NOT NULL DEFAULT 0,
  last_error      VARCHAR(2048) NULL,
  last_error_at   DATETIME(3) NULL,
  delivered       BIGINT UNSIGNED NOT NULL DEFAULT 0,
  updated_at      DATETIME(3) NOT NULL,
  PRIMARY KEY (sink_id, task_no)
) ENGINE=InnoDB";

const DDL_USAGE: &str = "CREATE TABLE IF NOT EXISTS sink_usage (
  db_id           INT UNSIGNED NOT NULL,
  connection_name VARCHAR(64) NOT NULL,
  platform        BOOLEAN NOT NULL,
  user_id         INT UNSIGNED NOT NULL,
  granted_by      INT UNSIGNED NOT NULL,
  granted_at      DATETIME(3) NOT NULL,
  PRIMARY KEY (db_id, connection_name, platform, user_id)
) ENGINE=InnoDB";

const DDL_DEAD: &str = "CREATE TABLE IF NOT EXISTS sink_dead_letters (
  dl_id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  sink_id         BIGINT UNSIGNED NOT NULL,
  idem_key        VARCHAR(200) NOT NULL,
  record_key      VARBINARY(4096) NULL,
  record_value    LONGBLOB NULL,
  headers         JSON NULL,
  op              CHAR(1) NULL,
  before_image    JSON NULL,
  after_image     JSON NULL,
  ts_ms           BIGINT NOT NULL,
  position        VARBINARY(2048) NOT NULL,
  reason          VARCHAR(2048) NOT NULL,
  class           VARCHAR(32) NOT NULL,
  attempts        INT UNSIGNED NOT NULL,
  state           ENUM('dead','replaying','replayed','discarded') NOT NULL,
  created_at      DATETIME(3) NOT NULL,
  PRIMARY KEY (dl_id),
  KEY k_sink (sink_id, state, dl_id)
) ENGINE=InnoDB";

/// The module's migrations.
pub static SINKS_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "sink_connections, sink_secrets, sinks, sink_tasks, sink_usage, sink_dead_letters",
    statements: &[
        DDL_CONNECTIONS,
        DDL_SECRETS,
        DDL_SINKS,
        DDL_TASKS,
        DDL_USAGE,
        DDL_DEAD,
    ],
}];

/// The module, for [`Migrator::new`].
pub const SINKS_MODULE: Module = Module {
    name: MODULE,
    migrations: SINKS_MIGRATIONS,
};

/// Applies the module's migrations to catalog `schema`.
///
/// # Errors
/// The migrator's.
pub async fn migrate(c: &mut mysql_async::Conn, schema: &str) -> Result<()> {
    Migrator::new(&[SINKS_MODULE])?
        .run(c, schema, &MigrateOpts::default())
        .await?;
    Ok(())
}

fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => match s.code {
            1205 | 1213 => SprigError::new(SprigCode::CatalogConflict, "catalog row busy; retry"),
            1062 => err(
                SprigCode::SinkExists,
                "a sink or connection with this name exists",
            ),
            code => {
                let mut state = *b"HY000";
                if s.state.len() == 5 {
                    state.copy_from_slice(s.state.as_bytes());
                }
                SprigError::backend(code, state, format!("catalog statement failed ({code})"))
            }
        }
        .with_source(e),
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e),
    }
}

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("sinks catalog: {what}"))
}

fn col<T: mysql_async::prelude::FromValue>(row: &Row, i: usize) -> Result<T> {
    row.get_opt::<T, usize>(i)
        .and_then(std::result::Result::ok)
        .ok_or_else(|| bad(&format!("bad column {i}")))
}

fn json_col<T: serde::de::DeserializeOwned>(row: &Row, i: usize) -> Result<T> {
    let s: String = col(row, i)?;
    serde_json::from_str(&s).map_err(|e| bad(&format!("column {i}: {e}")))
}

fn opt_json_col<T: serde::de::DeserializeOwned>(row: &Row, i: usize) -> Result<Option<T>> {
    let s: Option<String> = col(row, i)?;
    s.map(|s| serde_json::from_str(&s).map_err(|e| bad(&format!("column {i}: {e}"))))
        .transpose()
}

fn to_json<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "null".into())
}

/// A grant of `USAGE` on a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Usage {
    /// The connection's database.
    pub db_id: u32,
    /// Its name.
    pub connection: String,
    /// A platform connection.
    pub platform: bool,
    /// The grantee.
    pub user_id: u32,
}

/// The module in a catalog snapshot.
#[derive(Debug, Clone, Default)]
pub struct SinksState {
    /// The module's tables exist.
    pub migrated: bool,
    /// Connections by (database, name).
    pub connections: BTreeMap<(u32, String), Arc<ConnectionDef>>,
    /// Sinks by (database, name).
    pub sinks: BTreeMap<(u32, String), Arc<SinkDef>>,
    /// `USAGE` grants.
    pub usage: Vec<Usage>,
}

impl ExtState for SinksState {}

impl SinksState {
    /// A connection by database and name.
    #[must_use]
    pub fn connection(&self, db: u32, name: &str) -> Option<&Arc<ConnectionDef>> {
        self.connections.get(&(db, name.to_owned()))
    }

    /// A sink by database and name.
    #[must_use]
    pub fn sink(&self, db: u32, name: &str) -> Option<&Arc<SinkDef>> {
        self.sinks.get(&(db, name.to_owned()))
    }

    /// Whether `user` holds `USAGE` on the connection.
    #[must_use]
    pub fn has_usage(&self, db: u32, conn: &str, platform: bool, user: u32) -> bool {
        self.usage.iter().any(|u| {
            u.db_id == db && u.connection == conn && u.platform == platform && u.user_id == user
        })
    }

    /// Sinks using connection `id`.
    #[must_use]
    pub fn users_of(&self, id: ConnectionId) -> Vec<&Arc<SinkDef>> {
        self.sinks
            .values()
            .filter(|s| {
                s.connections
                    .iter()
                    .any(|c| matches!(c, crate::model::ConnRef::Tenant { id: i, .. } if *i == id))
            })
            .collect()
    }
}

const CONN_COLUMNS: &str = "connection_id, db_id, name, kind, CAST(options AS CHAR), \
     CAST(hosts AS CHAR), CAST(residency_tags AS CHAR), owner_user_id";

const SINK_COLUMNS: &str = "sink_id, db_id, name, CAST(source_ids AS CHAR), filter_sql, \
     CAST(projection AS CHAR), transform_name, CAST(connection_ids AS CHAR), \
     CAST(target AS CHAR), guarantee, CAST(dead_letter AS CHAR), CAST(start_at AS CHAR), \
     owner_user_id, state, state_reason, definition_ver, CAST(options AS CHAR)";

/// Reads and writes the module's tables in one catalog schema.
#[derive(Debug, Clone)]
pub struct SinksStore {
    schema: String,
}

impl SinksStore {
    /// The tables of catalog schema `schema`.
    #[must_use]
    pub fn new(schema: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
        }
    }

    fn t(&self, table: &str) -> String {
        format!("{}.{table}", quote(&self.schema))
    }

    /// Whether the module's tables exist.
    pub async fn migrated<Q: Queryable + Send>(&self, q: &mut Q) -> Result<bool> {
        let n: Option<u64> = q
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'sink_usage'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// Everything a snapshot holds.
    pub async fn load<Q: Queryable + Send>(&self, q: &mut Q) -> Result<SinksState> {
        let mut st = SinksState {
            migrated: true,
            ..SinksState::default()
        };
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT {CONN_COLUMNS} FROM {}",
                self.t("sink_connections")
            ))
            .await
            .map_err(map_err)?;
        let mut conns = Vec::new();
        for r in &rows {
            conns.push(conn_of(r)?);
        }
        let secrets: Vec<(u64, String, u32, u32, String)> = q
            .query(format!(
                "SELECT connection_id, name, version, set_by, \
                 DATE_FORMAT(set_at, '%Y-%m-%d %H:%i:%s.%f') FROM {} WHERE state = 'current' \
                 ORDER BY connection_id, name",
                self.t("sink_secrets")
            ))
            .await
            .map_err(map_err)?;
        for c in &mut conns {
            c.secrets = secrets
                .iter()
                .filter(|s| s.0 == c.id.0)
                .map(|s| SecretMeta {
                    name: s.1.clone(),
                    version: s.2,
                    set_by: s.3,
                    set_at: s.4.chars().take(23).collect(),
                })
                .collect();
        }
        for c in conns {
            st.connections
                .insert((c.db_id, c.name.clone()), Arc::new(c));
        }
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT {SINK_COLUMNS} FROM {} WHERE state <> 'dropping'",
                self.t("sinks")
            ))
            .await
            .map_err(map_err)?;
        for r in &rows {
            let s = sink_of(r)?;
            st.sinks.insert((s.db_id, s.name.clone()), Arc::new(s));
        }
        let usage: Vec<(u32, String, bool, u32)> = q
            .query(format!(
                "SELECT db_id, connection_name, platform, user_id FROM {}",
                self.t("sink_usage")
            ))
            .await
            .map_err(map_err)?;
        st.usage = usage
            .into_iter()
            .map(|(db_id, connection, platform, user_id)| Usage {
                db_id,
                connection,
                platform,
                user_id,
            })
            .collect();
        Ok(st)
    }

    /// One sink by id (workers).
    pub async fn sink<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: SinkId,
    ) -> Result<Option<SinkDef>> {
        let r: Option<Row> = q
            .exec_first(
                format!(
                    "SELECT {SINK_COLUMNS} FROM {} WHERE sink_id = ?",
                    self.t("sinks")
                ),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
        r.as_ref().map(sink_of).transpose()
    }

    /// One connection by id (workers).
    pub async fn connection<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: ConnectionId,
    ) -> Result<Option<ConnectionDef>> {
        let r: Option<Row> = q
            .exec_first(
                format!(
                    "SELECT {CONN_COLUMNS} FROM {} WHERE connection_id = ?",
                    self.t("sink_connections")
                ),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
        r.as_ref().map(conn_of).transpose()
    }

    /// Inserts a connection.
    pub async fn insert_connection<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        c: &ConnectionDef,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (connection_id, db_id, name, kind, options, hosts, residency_tags, \
                 owner_user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, \
                 UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
                self.t("sink_connections")
            ),
            (
                c.id.0,
                c.db_id,
                c.name.as_str(),
                c.kind.as_str(),
                to_json(&c.options),
                to_json(&c.hosts),
                c.residency_tags.as_ref().map(to_json),
                c.owner_user_id,
            ),
        )
        .await
        .map_err(map_err)
    }

    /// Updates a connection's options, hosts and tags.
    pub async fn update_connection<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        c: &ConnectionDef,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET options = ?, hosts = ?, residency_tags = ?, \
                 updated_at = UTC_TIMESTAMP(3) WHERE connection_id = ?",
                self.t("sink_connections")
            ),
            (
                to_json(&c.options),
                to_json(&c.hosts),
                c.residency_tags.as_ref().map(to_json),
                c.id.0,
            ),
        )
        .await
        .map_err(map_err)
    }

    /// Deletes a connection, its secrets and its grants.
    pub async fn delete_connection<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        c: &ConnectionDef,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE connection_id = ?",
                self.t("sink_secrets")
            ),
            (c.id.0,),
        )
        .await
        .map_err(map_err)?;
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE db_id = ? AND connection_name = ? AND platform = FALSE",
                self.t("sink_usage")
            ),
            (c.db_id, c.name.as_str()),
        )
        .await
        .map_err(map_err)?;
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE connection_id = ?",
                self.t("sink_connections")
            ),
            (c.id.0,),
        )
        .await
        .map_err(map_err)
    }

    /// Writes a new version of credential `name`: the old current becomes
    /// previous, older previous versions are retired (their sealed bytes
    /// emptied). Returns the new version.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_secret<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        conn: ConnectionId,
        name: &str,
        version: u32,
        key_version: u32,
        sealed: &[u8],
        set_by: u32,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET state = 'retired', sealed = '' \
                 WHERE connection_id = ? AND name = ? AND state = 'previous'",
                self.t("sink_secrets")
            ),
            (conn.0, name),
        )
        .await
        .map_err(map_err)?;
        q.exec_drop(
            format!(
                "UPDATE {} SET state = 'previous' WHERE connection_id = ? AND name = ? \
                 AND state = 'current'",
                self.t("sink_secrets")
            ),
            (conn.0, name),
        )
        .await
        .map_err(map_err)?;
        q.exec_drop(
            format!(
                "INSERT INTO {} (connection_id, name, version, key_version, sealed, state, set_by, \
                 set_at) VALUES (?, ?, ?, ?, ?, 'current', ?, UTC_TIMESTAMP(3))",
                self.t("sink_secrets")
            ),
            (conn.0, name, version, key_version, sealed, set_by),
        )
        .await
        .map_err(map_err)
    }

    /// The next version of credential `name`.
    pub async fn next_secret_version<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        conn: ConnectionId,
        name: &str,
    ) -> Result<u32> {
        let v: Option<Option<u32>> = q
            .exec_first(
                format!(
                    "SELECT MAX(version) FROM {} WHERE connection_id = ? AND name = ?",
                    self.t("sink_secrets")
                ),
                (conn.0, name),
            )
            .await
            .map_err(map_err)?;
        Ok(v.flatten().unwrap_or(0) + 1)
    }

    /// Sealed credentials of a connection: `(name, version, key version,
    /// sealed, previous)` for current and previous versions; previous ones
    /// only while younger than `previous_hours`.
    pub async fn sealed_secrets<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        conn: ConnectionId,
        previous_hours: u64,
    ) -> Result<Vec<(String, u32, u32, Vec<u8>, bool)>> {
        let rows: Vec<(String, u32, u32, Vec<u8>, String)> = q
            .exec(
                format!(
                    "SELECT s.name, s.version, s.key_version, s.sealed, s.state FROM {t} s \
                     WHERE s.connection_id = ? AND (s.state = 'current' OR (s.state = 'previous' \
                     AND (SELECT c.set_at FROM {t} c WHERE c.connection_id = s.connection_id \
                     AND c.name = s.name AND c.state = 'current') > UTC_TIMESTAMP(3) - INTERVAL ? HOUR))",
                    t = self.t("sink_secrets")
                ),
                (conn.0, previous_hours),
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(|(n, v, kv, s, st)| (n, v, kv, s, st == "previous"))
            .collect())
    }

    /// Inserts a sink.
    pub async fn insert_sink<Q: Queryable + Send>(&self, q: &mut Q, s: &SinkDef) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (sink_id, db_id, name, source_kind, source_ids, filter_sql, \
                 projection, transform_name, target_kind, connection_ids, target, guarantee, \
                 dead_letter, start_at, owner_user_id, state, state_reason, definition_ver, \
                 options, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                 ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
                self.t("sinks")
            ),
            mysql_async::Params::Positional(vec![
                s.id.0.into(),
                s.db_id.into(),
                s.name.as_str().into(),
                s.source.kind().as_str().into(),
                to_json(&s.source).into(),
                s.filter_sql.clone().into(),
                s.projection.as_ref().map(to_json).into(),
                s.transform.clone().into(),
                s.target.kind().as_str().into(),
                to_json(&s.connections).into(),
                to_json(&s.target).into(),
                s.guarantee.as_str().into(),
                s.dead_letter.as_ref().map(to_json).into(),
                to_json(&s.start_at).into(),
                s.owner_user_id.into(),
                s.state.as_str().into(),
                s.state_reason.clone().into(),
                s.definition_ver.into(),
                to_json(&s.options).into(),
            ]),
        )
        .await
        .map_err(map_err)
    }

    /// Rewrites a sink's definition (`ALTER SINK`), bumping its version.
    pub async fn update_sink<Q: Queryable + Send>(&self, q: &mut Q, s: &SinkDef) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET source_ids = ?, target = ?, start_at = ?, owner_user_id = ?, \
                 state = ?, state_reason = ?, definition_ver = definition_ver + 1, options = ?, \
                 updated_at = UTC_TIMESTAMP(3) WHERE sink_id = ?",
                self.t("sinks")
            ),
            (
                to_json(&s.source),
                to_json(&s.target),
                to_json(&s.start_at),
                s.owner_user_id,
                s.state.as_str(),
                s.state_reason.clone(),
                to_json(&s.options),
                s.id.0,
            ),
        )
        .await
        .map_err(map_err)
    }

    /// Sets a sink's state (tasks and statements). Changing only the state
    /// doesn't bump `definition_ver`.
    pub async fn set_state<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: SinkId,
        state: SinkState,
        reason: Option<&str>,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET state = ?, state_reason = ?, updated_at = UTC_TIMESTAMP(3) \
                 WHERE sink_id = ?",
                self.t("sinks")
            ),
            (
                state.as_str(),
                reason.map(|r| r.chars().take(1024).collect::<String>()),
                id.0,
            ),
        )
        .await
        .map_err(map_err)
    }

    /// The state and definition version of a sink (tasks poll it).
    pub async fn state_of<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: SinkId,
    ) -> Result<Option<(SinkState, Option<String>, u64)>> {
        let r: Option<(String, Option<String>, u64)> = q
            .exec_first(
                format!(
                    "SELECT state, state_reason, definition_ver FROM {} WHERE sink_id = ?",
                    self.t("sinks")
                ),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
        Ok(r.and_then(|(s, r, v)| SinkState::parse(&s).map(|s| (s, r, v))))
    }

    /// Every sink's state and reason, read now (workers change them
    /// without a catalog version, so snapshots lag).
    pub async fn states<Q: Queryable + Send>(
        &self,
        q: &mut Q,
    ) -> Result<std::collections::HashMap<u64, (SinkState, Option<String>)>> {
        let rows: Vec<(u64, String, Option<String>)> = q
            .query(format!(
                "SELECT sink_id, state, state_reason FROM {}",
                self.t("sinks")
            ))
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .filter_map(|(id, s, r)| SinkState::parse(&s).map(|s| (id, (s, r))))
            .collect())
    }

    /// Deletes a sink and its tasks.
    pub async fn delete_sink<Q: Queryable + Send>(&self, q: &mut Q, id: SinkId) -> Result<()> {
        q.exec_drop(
            format!("DELETE FROM {} WHERE sink_id = ?", self.t("sink_tasks")),
            (id.0,),
        )
        .await
        .map_err(map_err)?;
        q.exec_drop(
            format!("DELETE FROM {} WHERE sink_id = ?", self.t("sinks")),
            (id.0,),
        )
        .await
        .map_err(map_err)
    }

    /// Grants or revokes `USAGE`.
    pub async fn set_usage<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        u: &Usage,
        grant: bool,
        by: u32,
    ) -> Result<()> {
        if grant {
            q.exec_drop(
                format!(
                    "INSERT IGNORE INTO {} (db_id, connection_name, platform, user_id, granted_by, \
                     granted_at) VALUES (?, ?, ?, ?, ?, UTC_TIMESTAMP(3))",
                    self.t("sink_usage")
                ),
                (u.db_id, u.connection.as_str(), u.platform, u.user_id, by),
            )
            .await
        } else {
            q.exec_drop(
                format!(
                    "DELETE FROM {} WHERE db_id = ? AND connection_name = ? AND platform = ? \
                     AND user_id = ?",
                    self.t("sink_usage")
                ),
                (u.db_id, u.connection.as_str(), u.platform, u.user_id),
            )
            .await
        }
        .map_err(map_err)
    }

    /// Claims task `task_no` for `worker`: creates its row or bumps its
    /// epoch, and returns the new epoch (bumped on every claim).
    pub async fn claim_task<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        sink: SinkId,
        task_no: TaskNo,
        source_part: &Value,
        worker: &str,
    ) -> Result<TaskEpoch> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (sink_id, task_no, source_part, epoch, worker_id, state, updated_at) \
                 VALUES (?, ?, ?, 1, ?, 'running', UTC_TIMESTAMP(3)) ON DUPLICATE KEY UPDATE \
                 epoch = epoch + 1, worker_id = VALUES(worker_id), source_part = VALUES(source_part), \
                 state = 'running', updated_at = VALUES(updated_at)",
                self.t("sink_tasks")
            ),
            (sink.0, task_no.0, source_part.to_string(), worker),
        )
        .await
        .map_err(map_err)?;
        let e: Option<u64> = q
            .exec_first(
                format!(
                    "SELECT epoch FROM {} WHERE sink_id = ? AND task_no = ?",
                    self.t("sink_tasks")
                ),
                (sink.0, task_no.0),
            )
            .await
            .map_err(map_err)?;
        e.map(TaskEpoch).ok_or_else(|| bad("task row vanished"))
    }

    /// The task's last claim: its epoch, worker and source
    /// part, read before a new claim (a queue task's dead predecessor).
    pub async fn task_claim<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        sink: SinkId,
        task_no: TaskNo,
    ) -> Result<Option<(TaskEpoch, Option<String>, Value)>> {
        let r: Option<(u64, Option<String>, Option<String>)> = q
            .exec_first(
                format!(
                    "SELECT epoch, worker_id, CAST(source_part AS CHAR) FROM {} \
                     WHERE sink_id = ? AND task_no = ?",
                    self.t("sink_tasks")
                ),
                (sink.0, task_no.0),
            )
            .await
            .map_err(map_err)?;
        Ok(r.map(|(e, w, p)| {
            (
                TaskEpoch(e),
                w,
                p.and_then(|p| serde_json::from_str(&p).ok())
                    .unwrap_or(Value::Null),
            )
        }))
    }

    /// The task's current epoch.
    pub async fn task_epoch<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        sink: SinkId,
        task_no: TaskNo,
    ) -> Result<Option<TaskEpoch>> {
        let e: Option<u64> = q
            .exec_first(
                format!(
                    "SELECT epoch FROM {} WHERE sink_id = ? AND task_no = ?",
                    self.t("sink_tasks")
                ),
                (sink.0, task_no.0),
            )
            .await
            .map_err(map_err)?;
        Ok(e.map(TaskEpoch))
    }

    /// Records a task's state, error and delivered count, under its epoch
    /// (a stale task writes nothing).
    #[allow(clippy::too_many_arguments)]
    pub async fn report_task<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        sink: SinkId,
        task_no: TaskNo,
        epoch: TaskEpoch,
        state: TaskState,
        error: Option<&str>,
        delivered: u64,
    ) -> Result<bool> {
        let r = q
            .exec_iter(
                format!(
                    "UPDATE {} SET state = ?, delivered = delivered + ?, \
                 attempts = IF(? IS NULL, 0, attempts + 1), \
                 last_error = COALESCE(?, last_error), \
                 last_error_at = IF(? IS NULL, last_error_at, UTC_TIMESTAMP(3)), \
                 updated_at = UTC_TIMESTAMP(3) WHERE sink_id = ? AND task_no = ? AND epoch = ?",
                    self.t("sink_tasks")
                ),
                (
                    state.as_str(),
                    delivered,
                    error,
                    error.map(|e| e.chars().take(2048).collect::<String>()),
                    error,
                    sink.0,
                    task_no.0,
                    epoch.0,
                ),
            )
            .await
            .map_err(map_err)?;
        let n = r.affected_rows();
        r.drop_result().await.map_err(map_err)?;
        Ok(n > 0)
    }

    /// A sink's tasks (`SHOW SINK STATUS`).
    pub async fn tasks<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        sink: SinkId,
    ) -> Result<Vec<TaskRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT task_no, CAST(source_part AS CHAR), epoch, worker_id, \
                     CAST(pending AS CHAR), state, attempts, last_error, delivered FROM {} \
                     WHERE sink_id = ? ORDER BY task_no",
                    self.t("sink_tasks")
                ),
                (sink.0,),
            )
            .await
            .map_err(map_err)?;
        rows.iter()
            .map(|r| {
                Ok(TaskRow {
                    sink,
                    task_no: TaskNo(col(r, 0)?),
                    source_part: json_col(r, 1)?,
                    epoch: TaskEpoch(col(r, 2)?),
                    worker_id: col(r, 3)?,
                    pending: opt_json_col(r, 4)?,
                    state: TaskState::parse(&col::<String>(r, 5)?)
                        .ok_or_else(|| bad("task state"))?,
                    attempts: col(r, 6)?,
                    last_error: col(r, 7)?,
                    delivered: col(r, 8)?,
                })
            })
            .collect()
    }
}

/// A dead letter as listed.
#[derive(Debug, Clone, PartialEq)]
pub struct DeadRow {
    /// Its id.
    pub id: u64,
    /// The record's idempotency key.
    pub idem_key: String,
    /// The reason.
    pub reason: String,
    /// The class.
    pub class: String,
    /// Attempts made.
    pub attempts: u32,
    /// `dead`, `replaying`, `replayed`, `discarded`.
    pub state: String,
    /// When (UTC text).
    pub created_at: String,
}

impl SinksStore {
    /// Writes one dead letter.
    pub async fn insert_dead<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        sink: SinkId,
        rec: &crate::model::SinkRecord,
        info: &crate::runtime::DeadInfo,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (sink_id, idem_key, record_key, record_value, headers, op, \
                 before_image, after_image, ts_ms, position, reason, class, attempts, state, \
                 created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'dead', UTC_TIMESTAMP(3))",
                self.t("sink_dead_letters")
            ),
            mysql_async::Params::Positional(vec![
                sink.0.into(),
                rec.idem_key.as_str().into(),
                rec.key.clone().into(),
                rec.value.clone().into(),
                rec.headers_json().to_string().into(),
                rec.op.map(|o| o.letter()).into(),
                rec.before.as_ref().map(Value::to_string).into(),
                rec.after.as_ref().map(Value::to_string).into(),
                rec.ts_ms.into(),
                rec.pos.encode().into(),
                info.reason.chars().take(2048).collect::<String>().into(),
                info.class.into(),
                info.attempts.into(),
            ]),
        )
        .await
        .map_err(map_err)
    }

    /// Dead letters of a sink, newest last.
    pub async fn dead_letters<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        sink: SinkId,
        limit: u32,
    ) -> Result<Vec<DeadRow>> {
        let rows: Vec<(u64, String, String, String, u32, String, String)> = q
            .exec(
                format!(
                    "SELECT dl_id, idem_key, reason, class, attempts, state, \
                     DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') FROM {} WHERE sink_id = ? \
                     ORDER BY dl_id LIMIT ?",
                    self.t("sink_dead_letters")
                ),
                (sink.0, limit),
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(
                |(id, idem_key, reason, class, attempts, state, created_at)| DeadRow {
                    id,
                    idem_key,
                    reason,
                    class,
                    attempts,
                    state,
                    created_at,
                },
            )
            .collect())
    }

    /// Marks dead letters `replaying` (the task feeds them back) or
    /// `discarded`; `ids` empty means every `dead` one. Returns how many.
    pub async fn mark_dead<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        sink: SinkId,
        ids: &[u64],
        state: &str,
    ) -> Result<u64> {
        let filter = if ids.is_empty() {
            String::new()
        } else {
            format!(
                " AND dl_id IN ({})",
                ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
            )
        };
        let r = q
            .exec_iter(
                format!(
                    "UPDATE {} SET state = ? WHERE sink_id = ? AND state = 'dead'{filter}",
                    self.t("sink_dead_letters")
                ),
                (state, sink.0),
            )
            .await
            .map_err(map_err)?;
        let n = r.affected_rows();
        r.drop_result().await.map_err(map_err)?;
        Ok(n)
    }

    /// Takes up to `limit` dead letters marked `replaying`, as records, and
    /// marks them `replayed`.
    pub async fn take_replays<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        sink: SinkId,
        limit: u32,
    ) -> Result<Vec<crate::model::SinkRecord>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT dl_id, idem_key, record_key, record_value, CAST(headers AS CHAR), op, \
                     CAST(before_image AS CHAR), CAST(after_image AS CHAR), ts_ms, position FROM {} \
                     WHERE sink_id = ? AND state = 'replaying' ORDER BY dl_id LIMIT ?",
                    self.t("sink_dead_letters")
                ),
                (sink.0, limit),
            )
            .await
            .map_err(map_err)?;
        let mut out = Vec::new();
        let mut ids = Vec::new();
        for r in &rows {
            ids.push(col::<u64>(r, 0)?);
            let headers: Option<String> = col(r, 4)?;
            let headers = headers
                .and_then(|h| serde_json::from_str::<Map<String, Value>>(&h).ok())
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| {
                    let b = match v {
                        Value::String(s) => s.into_bytes(),
                        other => other.to_string().into_bytes(),
                    };
                    (k, b)
                })
                .collect();
            let pos: Vec<u8> = col(r, 9)?;
            out.push(crate::model::SinkRecord {
                idem_key: col(r, 1)?,
                key: col(r, 2)?,
                value: col(r, 3)?,
                headers,
                op: col::<Option<String>>(r, 5)?.and_then(|o| crate::model::ChangeOp::parse(&o)),
                before: opt_json_col(r, 6)?,
                after: opt_json_col(r, 7)?,
                ts_ms: col(r, 8)?,
                pos: crate::model::Position::decode(&pos)
                    .ok_or_else(|| bad("dead letter position"))?,
            });
        }
        if !ids.is_empty() {
            self.mark_ids(q, &ids, "replayed").await?;
        }
        Ok(out)
    }

    async fn mark_ids<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        ids: &[u64],
        state: &str,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET state = ? WHERE dl_id IN ({})",
                self.t("sink_dead_letters"),
                ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
            ),
            (state,),
        )
        .await
        .map_err(map_err)
    }
}

fn conn_of(r: &Row) -> Result<ConnectionDef> {
    Ok(ConnectionDef {
        id: ConnectionId(col(r, 0)?),
        db_id: col(r, 1)?,
        name: col(r, 2)?,
        kind: ConnKind::parse(&col::<String>(r, 3)?).ok_or_else(|| bad("connection kind"))?,
        options: json_col::<Map<String, Value>>(r, 4)?,
        hosts: json_col(r, 5)?,
        residency_tags: opt_json_col(r, 6)?,
        owner_user_id: col(r, 7)?,
        secrets: Vec::new(),
    })
}

fn sink_of(r: &Row) -> Result<SinkDef> {
    Ok(SinkDef {
        id: SinkId(col(r, 0)?),
        db_id: col(r, 1)?,
        name: col(r, 2)?,
        source: json_col(r, 3)?,
        filter_sql: col(r, 4)?,
        projection: opt_json_col(r, 5)?,
        transform: col(r, 6)?,
        connections: json_col(r, 7)?,
        target: json_col(r, 8)?,
        guarantee: Guarantee::parse(&col::<String>(r, 9)?).ok_or_else(|| bad("guarantee"))?,
        dead_letter: opt_json_col(r, 10)?,
        start_at: json_col(r, 11)?,
        owner_user_id: col(r, 12)?,
        state: SinkState::parse(&col::<String>(r, 13)?).ok_or_else(|| bad("state"))?,
        state_reason: col(r, 14)?,
        definition_ver: col(r, 15)?,
        options: opt_json_col(r, 16)?.unwrap_or_default(),
    })
}

/// The module in the catalog cache: [`SinksState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct SinksExtension;

#[async_trait::async_trait]
impl CatalogExtension for SinksExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        SINKS_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        let store = SinksStore::new(tx.schema());
        let conn = tx.conn();
        if !store.migrated(conn).await? {
            return Ok(Arc::new(SinksState::default()));
        }
        Ok(Arc::new(store.load(conn).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_is_well_formed() {
        Migrator::new(&[SINKS_MODULE]).unwrap_or_else(|_| unreachable!());
        assert_eq!(SINKS_MODULE.newest(), 1);
    }
}
