//! The catalog DDL and the migration framework (chapter 08, build step 1;
//! "Bootstrap, DDL and migrations").
//!
//! A catalog's tables come from *modules*: the core module [`CORE`] owns
//! the four tables of chapter 08, and every crate that keeps tables in the
//! catalog (auth, sharding, the data models, logic, ...) registers its own
//! module with its own migrations, so `sprig-catalog` never depends on its
//! users.
//!
//! **Formats.** Each module's migrations are numbered 1, 2, 3, ... without
//! gaps; the number is the module's *format* after the migration runs.
//! Migrations come in two phases:
//!
//! - **Expand** is additive and idempotent (`CREATE TABLE IF NOT EXISTS`,
//!   `ADD COLUMN IF NOT EXISTS`, copies). A binary that reads the previous
//!   format still works afterwards.
//! - **Contract** removes what older binaries need (drops, renames). After a
//!   contract to format `F`, only binaries that read `F` can use the catalog.
//!
//! Each module's state is a [`ModuleFormat`]: `format` (the last migration
//! applied) and `compat` (the oldest format a reader must know: the last
//! contract's format, or the first migration's). A binary whose newest
//! format for a module is below `compat` gets 39073
//! `ER_SPRIG_CATALOG_FORMAT_TOO_NEW` ([`check_readable`]).
//!
//! **Gating.** Upgrade orchestration (step 12) decides how far each module
//! may go and passes it as a [`Ceiling`]: expand to `F` once every
//! registered component reads `F`, contract to `F` after
//! `catalog.contract_delay_days` with no older reader registered. The
//! migrator itself applies whatever the ceilings allow, in order, and stops
//! a module at the first migration its ceiling refuses.
//!
//! **Where state lives.** In the catalog's internal keys (chapter 08,
//! "Internal keys"): `sprig.catalog_format` holds every module's
//! [`ModuleFormat`] (one row to read for the format check), and
//! `sprig.migration.<module>` holds the module's format and the history of
//! applied migrations (format, phase, name, time, who).
//!
//! **Concurrency and crashes.** A run takes `GET_LOCK('sprig_migrate.<schema>')`
//! on its connection, so concurrent runs from several processes serialize
//! (writes go to one writer node, also on Galera, so the server-local lock
//! is enough). Each migration is recorded after its statements; a crash in
//! between re-runs the migration, which is why every statement must be
//! idempotent. Migrations only move forward.
//!
//! **Fenced catalogs.** A tenant move's cutover may fence the catalog by
//! renaming its tables to `__sprig_f_<hex16>` (chapter 09, "Fencing
//! primitives"; chapter 10, "Cutover"). The renamed catalog looks empty,
//! so a run would create a second, empty set of tables under the old
//! names and the unfence could not rename the real ones back. A run
//! therefore refuses a schema holding fenced tables (39070, retryable),
//! checking after it read the formats and again before each migration: a
//! fence that renamed the tables before the read is seen, and one after it
//! leaves formats that need nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use sprig_common::names::quote;
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{Result, SprigCode, SprigError};

use crate::sql::{map_err, rfc3339, timed, to_datetime};

/// The core module: the four tables chapter 08 owns.
pub const CORE: &str = "catalog";

/// Internal key holding every module's [`ModuleFormat`].
pub const FORMAT_KEY: &str = "sprig.catalog_format";

/// Internal key holding one module's format and migration history.
#[must_use]
pub fn migration_key(module: &str) -> String {
    format!("sprig.migration.{module}")
}

/// `databases` (chapter 08, Appendix C). `IF NOT EXISTS` keeps the
/// statement idempotent; the columns are exactly the appendix's.
pub const DDL_DATABASES: &str = "CREATE TABLE IF NOT EXISTS `databases` (
  db_id             INT UNSIGNED    NOT NULL,
  name              VARCHAR(64)     NOT NULL,
  name_key          VARCHAR(64)     NOT NULL,
  default_charset   VARCHAR(32)     NOT NULL,
  default_collation VARCHAR(64)     NOT NULL,
  default_group_id  INT UNSIGNED    NOT NULL,
  state             ENUM('creating','active','dropping','dropped') NOT NULL,
  options           JSON            NOT NULL DEFAULT '{}',
  created_at        DATETIME(6)     NOT NULL,
  created_by        INT UNSIGNED    NULL,
  changed_version   BIGINT UNSIGNED NOT NULL,
  live_name VARCHAR(64) AS (IF(state = 'dropped', NULL, name_key)) PERSISTENT,
  PRIMARY KEY (db_id), UNIQUE KEY uk_live_name (live_name),
  KEY k_changed (changed_version)
) ENGINE=InnoDB";

/// `objects` (chapter 08, Appendix C).
pub const DDL_OBJECTS: &str = "CREATE TABLE IF NOT EXISTS objects (
  object_id         BIGINT UNSIGNED NOT NULL,
  db_id             INT UNSIGNED    NOT NULL,
  namespace         ENUM('relation','procedure','function','trigger','event','type',
                         'search_index','logic','graph') NOT NULL,
  name              VARCHAR(64)     NOT NULL,
  name_key          VARCHAR(64)     NOT NULL,
  kind              ENUM('table','view','sequence','collection','kv_namespace',
                         'procedure','function','trigger','event','type',
                         'search_index','logic_function','logic_validation',
                         'logic_trigger','logic_schedule','graph') NOT NULL,
  parent_id         BIGINT UNSIGNED NULL,
  state             ENUM('creating','active','altering','dropping','dropped') NOT NULL,
  schema_version    BIGINT UNSIGNED NOT NULL,
  flags             SET('gipk_added','system_versioned','inheritable','redis','rls')
                    NOT NULL DEFAULT '',
  pending_job_id    BINARY(16)      NULL,
  created_at        DATETIME(6)     NOT NULL,
  changed_version   BIGINT UNSIGNED NOT NULL,
  live_name VARCHAR(64) AS (IF(state = 'dropped', NULL, name_key)) PERSISTENT,
  PRIMARY KEY (object_id), UNIQUE KEY uk_live_name (db_id, namespace, live_name),
  KEY k_parent (parent_id), KEY k_changed (changed_version)
) ENGINE=InnoDB";

/// `settings` (chapter 08, Appendix C).
pub const DDL_SETTINGS: &str = "CREATE TABLE IF NOT EXISTS settings (
  scope_kind        ENUM('tenant','database','object','ancestor') NOT NULL,
  scope_id          BIGINT UNSIGNED NOT NULL,
  setting_key       VARCHAR(128)    NOT NULL,
  value             JSON            NULL,
  bounds            JSON            NULL,
  set_by            VARCHAR(288)    NOT NULL,
  set_at            DATETIME(6)     NOT NULL,
  version           BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (scope_kind, scope_id, setting_key),
  KEY k_version (version)
) ENGINE=InnoDB";

/// `schema_versions` (chapter 08, Appendix C).
pub const DDL_SCHEMA_VERSIONS: &str = "CREATE TABLE IF NOT EXISTS schema_versions (
  object_id         BIGINT UNSIGNED NOT NULL,
  version           BIGINT UNSIGNED NOT NULL,
  ddl               MEDIUMTEXT      NOT NULL,
  create_sql        MEDIUMTEXT      NULL,
  create_sql_hash   BINARY(32)      NULL,
  ddl_job_id        BINARY(16)      NULL,
  applied_by        INT UNSIGNED    NULL,
  applied_at        DATETIME(6)     NOT NULL,
  changed_version   BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (object_id, version)
) ENGINE=InnoDB";

/// The internal keys every catalog starts with: both versions at 0,
/// nothing purged, id counters at 1. Existing rows are kept, so the
/// statement is idempotent. Bootstrap (step 9) adds `sprig.bootstrap`,
/// `sprig.ancestry` and `sprig.ancestor_versions`.
pub const INSERT_INTERNAL_KEYS: &str = "INSERT INTO settings
  (scope_kind, scope_id, setting_key, value, bounds, set_by, set_at, version) VALUES
  ('tenant', 0, 'sprig.catalog_version', '0', NULL, 'system', UTC_TIMESTAMP(6), 0),
  ('tenant', 0, 'sprig.auth_version', '0', NULL, 'system', UTC_TIMESTAMP(6), 0),
  ('tenant', 0, 'sprig.purged_version', '0', NULL, 'system', UTC_TIMESTAMP(6), 0),
  ('tenant', 0, 'sprig.next_db_id', '1', NULL, 'system', UTC_TIMESTAMP(6), 0),
  ('tenant', 0, 'sprig.next_object_id', '1', NULL, 'system', UTC_TIMESTAMP(6), 0)
  ON DUPLICATE KEY UPDATE setting_key = setting_key";

/// The core module's migrations.
pub static CORE_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "databases, objects, settings, schema_versions and the internal keys",
    statements: &[
        DDL_DATABASES,
        DDL_OBJECTS,
        DDL_SETTINGS,
        DDL_SCHEMA_VERSIONS,
        INSERT_INTERNAL_KEYS,
    ],
}];

/// The core module.
pub const CORE_MODULE: Module = Module {
    name: CORE,
    migrations: CORE_MIGRATIONS,
};

/// The phase of a migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    /// Additive and idempotent; older readers keep working.
    Expand,
    /// Removes what older readers need; runs after the contract delay.
    Contract,
}

/// One migration: the statements that take a module to `format`.
#[derive(Debug)]
pub struct Migration {
    /// The module's format after this migration; 1, 2, 3, ... per module.
    pub format: u32,
    /// Expand or contract.
    pub phase: Phase,
    /// What it does, for the history and logs.
    pub name: &'static str,
    /// Statements run in order on the catalog schema (it is the
    /// connection's default database). Each must be idempotent.
    pub statements: &'static [&'static str],
}

/// A module's migrations, registered by the crate that owns its tables.
#[derive(Debug, Clone, Copy)]
pub struct Module {
    /// Module name: `[a-z0-9_]+`, such as `catalog`, `auth`, `search`.
    pub name: &'static str,
    /// Migrations in format order, starting at 1 with an expand.
    pub migrations: &'static [Migration],
}

impl Module {
    /// The newest format this binary knows for the module.
    #[must_use]
    pub fn newest(&self) -> u32 {
        self.migrations.last().map_or(0, |m| m.format)
    }
}

/// One module's recorded state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleFormat {
    /// The last migration applied.
    pub format: u32,
    /// The oldest format a reader must know.
    pub compat: u32,
}

impl ModuleFormat {
    /// The state after applying `m`.
    #[must_use]
    pub fn after(self, m: &Migration) -> Self {
        let compat = match m.phase {
            Phase::Contract => m.format,
            Phase::Expand if self.compat == 0 => m.format,
            Phase::Expand => self.compat,
        };
        Self {
            format: m.format,
            compat,
        }
    }
}

/// Every module's state: the value of `sprig.catalog_format`.
pub type FormatState = BTreeMap<String, ModuleFormat>;

/// One entry of a module's migration history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// The format reached.
    pub format: u32,
    /// The migration's phase.
    pub phase: Phase,
    /// The migration's name.
    pub name: String,
    /// When it was recorded, RFC 3339 UTC.
    pub at: String,
    /// Who ran it (process id, such as `controller:mum1/ctl-2`).
    pub by: String,
}

/// The value of `sprig.migration.<module>`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleHistory {
    /// The module's state.
    #[serde(flatten)]
    pub state: ModuleFormat,
    /// Applied migrations, oldest first.
    #[serde(default)]
    pub history: Vec<HistoryEntry>,
}

/// How far one module may go, per phase (step 12 computes it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ceiling {
    /// Highest format an expand may reach.
    pub expand: u32,
    /// Highest format a contract may reach.
    pub contract: u32,
}

impl Ceiling {
    /// No limit.
    pub const NONE: Ceiling = Ceiling {
        expand: u32::MAX,
        contract: u32::MAX,
    };
    /// Expands only, as far as they go.
    pub const EXPAND_ONLY: Ceiling = Ceiling {
        expand: u32::MAX,
        contract: 0,
    };

    fn allows(self, m: &Migration) -> bool {
        match m.phase {
            Phase::Expand => m.format <= self.expand,
            Phase::Contract => m.format <= self.contract,
        }
    }
}

/// Options of one migration run.
#[derive(Debug, Clone)]
pub struct MigrateOpts {
    /// How long to wait for another process's run to finish.
    pub lock_timeout: Duration,
    /// Deadline of each statement.
    pub statement_timeout: Duration,
    /// Per-module limits; a module without one is limited by
    /// `default_ceiling`.
    pub ceilings: BTreeMap<String, Ceiling>,
    /// The limit of modules not in `ceilings`.
    pub default_ceiling: Ceiling,
    /// Recorded in the history, such as `controller:mum1/ctl-2`.
    pub applied_by: String,
}

impl Default for MigrateOpts {
    /// Expands only, as far as this binary knows: contracts need step
    /// 12's delay and reader checks, so they never run by default.
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(30),
            statement_timeout: Duration::from_secs(300),
            ceilings: BTreeMap::new(),
            default_ceiling: Ceiling::EXPAND_ONLY,
            applied_by: "system".to_owned(),
        }
    }
}

/// A migration named in a report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationRef {
    /// The module.
    pub module: &'static str,
    /// Its format.
    pub format: u32,
    /// Its phase.
    pub phase: Phase,
    /// Its name.
    pub name: &'static str,
}

/// What a run did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateReport {
    /// Migrations applied, in order.
    pub applied: Vec<MigrationRef>,
    /// Per module, the first migration a ceiling held back.
    pub blocked: Vec<MigrationRef>,
    /// Every module's state after the run.
    pub state: FormatState,
}

/// Runs modules' migrations on catalog schemas.
#[derive(Clone)]
pub struct Migrator {
    modules: Vec<Module>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for Migrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Migrator")
            .field("modules", &self.supports())
            .finish_non_exhaustive()
    }
}

impl Migrator {
    /// The core module followed by `extra`, in that order.
    ///
    /// # Errors
    /// `Internal` if a module name is invalid or repeated, or a module's
    /// formats don't run 1, 2, 3, ... starting with an expand.
    pub fn new(extra: &[Module]) -> Result<Self> {
        let mut modules = vec![CORE_MODULE];
        modules.extend_from_slice(extra);
        let mut names = BTreeSet::new();
        for m in &modules {
            validate(m)?;
            if !names.insert(m.name) {
                return Err(SprigError::internal(format!(
                    "catalog module {} registered twice",
                    m.name
                )));
            }
        }
        Ok(Self {
            modules,
            clock: Arc::new(SystemClock),
        })
    }

    /// The same migrator with history times from `clock`.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The newest format this binary reads, per module.
    #[must_use]
    pub fn supports(&self) -> BTreeMap<String, u32> {
        self.modules
            .iter()
            .map(|m| (m.name.to_owned(), m.newest()))
            .collect()
    }

    /// Creates the schema if needed (`utf8mb4_bin`, so names compare
    /// exactly), then applies every pending migration the ceilings allow,
    /// core module first. Safe to call concurrently from several processes
    /// and to repeat. The connection's default database becomes `schema`.
    ///
    /// # Errors
    /// - 39071 `ER_SPRIG_CATALOG_CONFLICT` if another run holds the lock
    ///   past `opts.lock_timeout`;
    /// - 39073 `ER_SPRIG_CATALOG_FORMAT_TOO_NEW` if the catalog needs a
    ///   newer binary;
    /// - 39070 or a backend error if a statement fails; migrations recorded
    ///   before it stay applied.
    pub async fn run(
        &self,
        conn: &mut Conn,
        schema: &str,
        opts: &MigrateOpts,
    ) -> Result<MigrateReport> {
        // The run changes the connection's default database and holds a
        // named lock: the connection is reset when it goes back to its pool,
        // even a catalog pool that keeps its connections as they are.
        conn.reset_connection(true);
        if let Some(state) = self.up_to_date(conn, schema).await {
            return Ok(MigrateReport {
                applied: Vec::new(),
                blocked: Vec::new(),
                state,
            });
        }
        let lock = lock_name(schema)?;
        let got: Option<Option<i64>> = timed(
            opts.lock_timeout + opts.statement_timeout,
            "migration lock",
            async {
                conn.exec_first(
                    "SELECT GET_LOCK(?, ?)",
                    (&lock, opts.lock_timeout.as_secs_f64()),
                )
                .await
                .map_err(map_err)
            },
        )
        .await?;
        match got {
            Some(Some(1)) => {}
            Some(Some(0)) => {
                return Err(SprigError::new(
                    SprigCode::CatalogConflict,
                    "another process is migrating this catalog",
                ));
            }
            other => {
                return Err(SprigError::new(
                    SprigCode::CatalogUnavailable,
                    format!("GET_LOCK returned {other:?}"),
                ));
            }
        }
        let result = self.run_locked(conn, schema, opts).await;
        if let Err(e) = conn.exec_drop("DO RELEASE_LOCK(?)", (&lock,)).await {
            // The lock ends with the connection anyway.
            tracing::warn!(schema, error = %e, "releasing the migration lock failed");
        }
        result
    }

    /// The catalog's state when every module of this migrator is already
    /// at its newest format, read with one statement and no
    /// `information_schema` query or lock; the connection's default
    /// database is then `schema`, as after a run. `None` (the full run
    /// decides) when anything is missing, unreadable or behind. Every
    /// per-tenant loop runs its module's migrations each pass, so at
    /// 10,000 tenants the full run's lock and its three
    /// `information_schema` reads came hundreds of times a second (R9,
    /// r9scale).
    async fn up_to_date(&self, conn: &mut Conn, schema: &str) -> Option<FormatState> {
        let raw: Option<Vec<u8>> = conn
            .exec_first(
                format!(
                    "SELECT value FROM {}.settings WHERE scope_kind = 'tenant' AND scope_id = 0 \
                     AND setting_key = ?",
                    quote(schema)
                ),
                (FORMAT_KEY,),
            )
            .await
            .ok()
            .flatten();
        let state: FormatState = serde_json::from_slice(&raw?).ok()?;
        check_readable(&state, &self.supports()).ok()?;
        let current = self.modules.iter().all(|m| {
            state
                .get(m.name)
                .is_some_and(|s| s.format >= m.newest())
        });
        if !current {
            return None;
        }
        conn.query_drop(format!("USE {}", quote(schema))).await.ok()?;
        Some(state)
    }

    async fn run_locked(
        &self,
        conn: &mut Conn,
        schema: &str,
        opts: &MigrateOpts,
    ) -> Result<MigrateReport> {
        let q = quote(schema);
        // Only when missing: on Galera even a no-op `CREATE DATABASE IF NOT
        // EXISTS` is a total-order DDL, which fails the certification of
        // every catalog transaction in flight on another node (a gateway's
        // DDL publish, measured: 39071 on every CREATE TABLE while the
        // controller's loops re-ran their migrations).
        let exists: Option<i64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
                (schema,),
            )
            .await
            .map_err(map_err)?;
        if exists.unwrap_or(0) == 0 {
            exec_text(
                conn,
                opts,
                &format!(
                    "CREATE DATABASE IF NOT EXISTS {q} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
                ),
            )
            .await?;
        }
        exec_text(conn, opts, &format!("USE {q}")).await?;
        let mut state = read_state(conn, schema).await?;
        check_readable(&state, &self.supports())?;
        refuse_fenced(conn, schema).await?;
        let mut report = MigrateReport {
            applied: Vec::new(),
            blocked: Vec::new(),
            state: FormatState::new(),
        };
        for module in &self.modules {
            let ceiling = opts
                .ceilings
                .get(module.name)
                .copied()
                .unwrap_or(opts.default_ceiling);
            let mut current = state.get(module.name).copied().unwrap_or_default();
            let start = current.format;
            for m in module.migrations.iter().filter(|m| m.format > start) {
                let r = MigrationRef {
                    module: module.name,
                    format: m.format,
                    phase: m.phase,
                    name: m.name,
                };
                if !ceiling.allows(m) {
                    report.blocked.push(r);
                    break;
                }
                refuse_fenced(conn, schema).await?;
                tracing::info!(schema, module = module.name, format = m.format, phase = ?m.phase, name = m.name, "applying catalog migration");
                for stmt in m.statements {
                    exec_text(conn, opts, stmt).await?;
                }
                current = current.after(m);
                state.insert(module.name.to_owned(), current);
                self.record(conn, opts, module.name, m, &state).await?;
                report.applied.push(r);
            }
        }
        report.state = state;
        Ok(report)
    }

    /// Records `m` in `sprig.migration.<module>` and the new state in
    /// `sprig.catalog_format`, in one transaction.
    async fn record(
        &self,
        conn: &mut Conn,
        opts: &MigrateOpts,
        module: &str,
        m: &Migration,
        state: &FormatState,
    ) -> Result<()> {
        let now = self.clock.wall();
        let key = migration_key(module);
        let format_json = serde_json::to_string(state)
            .map_err(|e| SprigError::internal(format!("encoding catalog format: {e}")))?;
        timed(opts.statement_timeout, "migration record", async {
            let mut tx = conn
                .start_transaction(mysql_async::TxOpts::default())
                .await
                .map_err(map_err)?;
            let version: Option<Vec<u8>> = tx
                .exec_first(
                    "SELECT value FROM settings WHERE scope_kind = 'tenant' AND scope_id = 0 \
                     AND setting_key = 'sprig.catalog_version'",
                    (),
                )
                .await
                .map_err(map_err)?;
            let version: u64 = version
                .and_then(|b| serde_json::from_slice(&b).ok())
                .unwrap_or(0);
            let old: Option<Vec<u8>> = tx
                .exec_first(
                    "SELECT value FROM settings WHERE scope_kind = 'tenant' AND scope_id = 0 \
                     AND setting_key = ? FOR UPDATE",
                    (&key,),
                )
                .await
                .map_err(map_err)?;
            let mut hist: ModuleHistory = old
                .and_then(|b| serde_json::from_slice(&b).ok())
                .unwrap_or_default();
            hist.state = state.get(module).copied().unwrap_or_default();
            hist.history.push(HistoryEntry {
                format: m.format,
                phase: m.phase,
                name: m.name.to_owned(),
                at: rfc3339(now),
                by: opts.applied_by.clone(),
            });
            let hist_json = serde_json::to_string(&hist)
                .map_err(|e| SprigError::internal(format!("encoding migration history: {e}")))?;
            for (k, v) in [(key.as_str(), hist_json), (FORMAT_KEY, format_json.clone())] {
                tx.exec_drop(
                    "INSERT INTO settings (scope_kind, scope_id, setting_key, value, bounds, set_by, \
                     set_at, version) VALUES ('tenant', 0, ?, ?, NULL, 'system', ?, ?) \
                     ON DUPLICATE KEY UPDATE value = VALUES(value), set_by = VALUES(set_by), \
                     set_at = VALUES(set_at), version = VALUES(version)",
                    (k, v, to_datetime(now), version),
                )
                .await
                .map_err(map_err)?;
            }
            tx.commit().await.map_err(map_err)
        })
        .await
    }
}

/// Fails with 39070 if `schema` holds tables a rename fence moved away
/// (module docs, "Fenced catalogs").
async fn refuse_fenced(conn: &mut Conn, schema: &str) -> Result<()> {
    let fenced = fenced_tables(conn, schema).await?;
    if fenced.is_empty() {
        return Ok(());
    }
    tracing::warn!(
        schema,
        ?fenced,
        "catalog migrations skipped: the catalog is fenced"
    );
    Err(SprigError::new(
        SprigCode::CatalogUnavailable,
        format!(
            "catalog {schema} is fenced (a tenant move's cutover renamed its tables, such as \
             {}); migrations wait until it is unfenced or the move has finished",
            fenced[0]
        ),
    ))
}

/// The tables of `schema` named like a rename fence's
/// (`__sprig_f_<16 hex digits>`, `sprig_cluster::fence::fenced_name`).
///
/// # Errors
/// A backend error if the read fails.
pub async fn fenced_tables(conn: &mut Conn, schema: &str) -> Result<Vec<String>> {
    let names: Vec<String> = conn
        .exec(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
             AND TABLE_NAME LIKE '\\_\\_sprig\\_f\\_%' ORDER BY TABLE_NAME",
            (schema,),
        )
        .await
        .map_err(map_err)?;
    Ok(names.into_iter().filter(|n| is_fenced_name(n)).collect())
}

/// Whether `table` is a rename fence's name for a table.
#[must_use]
pub fn is_fenced_name(table: &str) -> bool {
    table
        .strip_prefix("__sprig_f_")
        .is_some_and(|h| h.len() == 16 && h.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Runs one statement through the text protocol (DDL can't always be
/// prepared).
async fn exec_text(conn: &mut Conn, opts: &MigrateOpts, stmt: &str) -> Result<()> {
    timed(opts.statement_timeout, "migration statement", async {
        conn.query_drop(stmt).await.map_err(map_err)
    })
    .await
}

/// Reads every module's state from `schema`; empty before the core
/// module's first migration has been recorded.
///
/// # Errors
/// 39070 or a backend error if the reads fail; `Internal` for a bad value.
pub async fn read_state(conn: &mut Conn, schema: &str) -> Result<FormatState> {
    let has_settings: Option<i64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
             AND TABLE_NAME = 'settings'",
            (schema,),
        )
        .await
        .map_err(map_err)?;
    if has_settings.unwrap_or(0) == 0 {
        return Ok(FormatState::new());
    }
    let raw: Option<Vec<u8>> = conn
        .exec_first(
            format!(
                "SELECT value FROM {}.settings WHERE scope_kind = 'tenant' AND scope_id = 0 \
                 AND setting_key = ?",
                quote(schema)
            ),
            (FORMAT_KEY,),
        )
        .await
        .map_err(map_err)?;
    raw.map_or(Ok(FormatState::new()), |b| {
        serde_json::from_slice(&b)
            .map_err(|e| SprigError::internal(format!("bad {FORMAT_KEY} value: {e}")))
    })
}

/// Reads one module's history from `schema`.
///
/// # Errors
/// As [`read_state`].
pub async fn read_history(conn: &mut Conn, schema: &str, module: &str) -> Result<ModuleHistory> {
    let raw: Option<Vec<u8>> = conn
        .exec_first(
            format!(
                "SELECT value FROM {}.settings WHERE scope_kind = 'tenant' AND scope_id = 0 \
                 AND setting_key = ?",
                quote(schema)
            ),
            (migration_key(module),),
        )
        .await
        .map_err(map_err)?;
    raw.map_or(Ok(ModuleHistory::default()), |b| {
        serde_json::from_slice(&b)
            .map_err(|e| SprigError::internal(format!("bad migration history of {module}: {e}")))
    })
}

/// Whether a binary reading formats up to `supports` (per module) may use
/// a catalog in `state`. Modules the binary doesn't know are ignored: it
/// never reads their tables.
///
/// # Errors
/// 39073 `ER_SPRIG_CATALOG_FORMAT_TOO_NEW` naming the first module whose
/// `compat` is newer than the binary.
pub fn check_readable(state: &FormatState, supports: &BTreeMap<String, u32>) -> Result<()> {
    for (module, f) in state {
        if let Some(&newest) = supports.get(module)
            && f.compat > newest
        {
            return Err(SprigError::new(
                SprigCode::CatalogFormatTooNew,
                format!(
                    "catalog module {module} is at format {} (readers need {}); this binary reads up to {newest}",
                    f.format, f.compat
                ),
            ));
        }
    }
    Ok(())
}

fn validate(m: &Module) -> Result<()> {
    let bad = |why: String| {
        Err(SprigError::internal(format!(
            "catalog module {}: {why}",
            m.name
        )))
    };
    if m.name.is_empty()
        || !m
            .name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return bad("name must match [a-z0-9_]+".to_owned());
    }
    for (i, mig) in m.migrations.iter().enumerate() {
        let want = u32::try_from(i + 1).unwrap_or(u32::MAX);
        if mig.format != want {
            return bad(format!(
                "migration {i} has format {}, expected {want}",
                mig.format
            ));
        }
        if mig.statements.is_empty() {
            return bad(format!("migration {} has no statements", mig.format));
        }
    }
    if m.migrations
        .first()
        .is_some_and(|f| f.phase != Phase::Expand)
    {
        return bad("the first migration must be an expand".to_owned());
    }
    Ok(())
}

/// `sprig_migrate.<schema>`; `GET_LOCK` names are at most 64 characters.
fn lock_name(schema: &str) -> Result<String> {
    let name = format!("sprig_migrate.{schema}");
    if schema.is_empty() || name.len() > 64 {
        return Err(SprigError::internal(format!(
            "catalog schema name {schema:?} is empty or too long for a migration lock"
        )));
    }
    Ok(name)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn fenced_names() {
        assert!(is_fenced_name("__sprig_f_00000000deadbeef"));
        assert!(!is_fenced_name("__sprig_f_deadbeef"));
        assert!(!is_fenced_name("__sprig_f_00000000deadbeeg"));
        assert!(!is_fenced_name("databases"));
        assert!(!is_fenced_name("__sprig_sl_ckpt"));
    }

    static GOOD: &[Migration] = &[
        Migration {
            format: 1,
            phase: Phase::Expand,
            name: "a",
            statements: &["SELECT 1"],
        },
        Migration {
            format: 2,
            phase: Phase::Expand,
            name: "b",
            statements: &["SELECT 1"],
        },
        Migration {
            format: 3,
            phase: Phase::Contract,
            name: "c",
            statements: &["SELECT 1"],
        },
    ];
    static GAP: &[Migration] = &[Migration {
        format: 2,
        phase: Phase::Expand,
        name: "a",
        statements: &["SELECT 1"],
    }];
    static CONTRACT_FIRST: &[Migration] = &[Migration {
        format: 1,
        phase: Phase::Contract,
        name: "a",
        statements: &["SELECT 1"],
    }];

    #[test]
    fn modules_are_validated() {
        let good = Module {
            name: "auth",
            migrations: GOOD,
        };
        let m = Migrator::new(&[good]).unwrap();
        assert_eq!(m.supports().get("auth"), Some(&3));
        assert_eq!(m.supports().get(CORE), Some(&1));
        for bad in [
            Module {
                name: "gap",
                migrations: GAP,
            },
            Module {
                name: "first",
                migrations: CONTRACT_FIRST,
            },
            Module {
                name: "Bad-Name",
                migrations: GOOD,
            },
            CORE_MODULE,
        ] {
            assert!(Migrator::new(&[bad]).is_err(), "{}", bad.name);
        }
    }

    #[test]
    fn formats_follow_the_phases() {
        let f = ModuleFormat::default().after(&GOOD[0]);
        assert_eq!(
            f,
            ModuleFormat {
                format: 1,
                compat: 1
            }
        );
        let f = f.after(&GOOD[1]);
        assert_eq!(
            f,
            ModuleFormat {
                format: 2,
                compat: 1
            }
        );
        let f = f.after(&GOOD[2]);
        assert_eq!(
            f,
            ModuleFormat {
                format: 3,
                compat: 3
            }
        );
    }

    #[test]
    fn ceilings_hold_back_contracts() {
        assert!(Ceiling::EXPAND_ONLY.allows(&GOOD[1]));
        assert!(!Ceiling::EXPAND_ONLY.allows(&GOOD[2]));
        assert!(Ceiling::NONE.allows(&GOOD[2]));
        let c = Ceiling {
            expand: 1,
            contract: 9,
        };
        assert!(!c.allows(&GOOD[1]));
    }

    #[test]
    fn newer_catalogs_are_refused_only_past_compat() {
        let supports: BTreeMap<String, u32> = [("auth".to_owned(), 2), (CORE.to_owned(), 1)].into();
        let mut state = FormatState::new();
        state.insert(
            "auth".to_owned(),
            ModuleFormat {
                format: 3,
                compat: 1,
            },
        );
        state.insert(
            "unknown_module".to_owned(),
            ModuleFormat {
                format: 9,
                compat: 9,
            },
        );
        check_readable(&state, &supports).unwrap();
        state.insert(
            "auth".to_owned(),
            ModuleFormat {
                format: 3,
                compat: 3,
            },
        );
        let e = check_readable(&state, &supports).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::CatalogFormatTooNew));
    }

    #[test]
    fn history_round_trips() {
        let h = ModuleHistory {
            state: ModuleFormat {
                format: 2,
                compat: 1,
            },
            history: vec![HistoryEntry {
                format: 2,
                phase: Phase::Expand,
                name: "x".to_owned(),
                at: "2026-11-08T00:00:00.000000Z".to_owned(),
                by: "system".to_owned(),
            }],
        };
        let j = serde_json::to_value(&h).unwrap();
        assert_eq!(j["format"], 2);
        assert_eq!(j["history"][0]["phase"], "expand");
        assert_eq!(serde_json::from_value::<ModuleHistory>(j).unwrap(), h);
        assert!(lock_name(&"x".repeat(60)).is_err());
        assert_eq!(lock_name("t42_sprig").unwrap(), "sprig_migrate.t42_sprig");
    }
}
