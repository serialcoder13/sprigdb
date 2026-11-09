//! The logic catalog (chapter 41, "Catalog tables"; build step 2): the
//! `logic` migration module's five tables in `t<tenant>_sprig`, and
//! [`LogicStore`], the reads and writes the deploy path and `sprigctl
//! logic` use.
//!
//! As built, rows are written with plain statements, not through the
//! catalog's versioned change: `changed_version` records the catalog
//! version the deploy saw, and no cache reads these tables yet.
//! Module ids come from `sprig.next_object_id`.

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use serde::Serialize;
use sprig_catalog::{Migration, Module, Phase};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

/// The module name in the catalog's format records.
pub const MODULE: &str = "logic";

/// `logic_modules`.
pub const DDL_MODULES: &str = "CREATE TABLE IF NOT EXISTS logic_modules (
  module_id         BIGINT UNSIGNED NOT NULL,
  db_id             INT UNSIGNED    NOT NULL,
  name              VARCHAR(64)     NOT NULL,
  kind              ENUM('function','procedure','validation','trigger','schedule',
                         'computed','redis_script','redis_library') NOT NULL,
  runtime           ENUM('sql','javascript','kirun','lua') NOT NULL,
  security          ENUM('invoker','definer') NOT NULL DEFAULT 'invoker',
  definer_user_id   INT UNSIGNED    NULL,
  tier_pref         ENUM('auto','mariadb','engine') NOT NULL DEFAULT 'auto',
  exposure          SET('sql','http','grpc','mongo','redis') NOT NULL DEFAULT 'sql',
  active_version    BIGINT UNSIGNED NULL,
  candidate_version BIGINT UNSIGNED NULL,
  candidate_pct     TINYINT UNSIGNED NOT NULL DEFAULT 0,
  state             ENUM('active','disabled','dropping','dropped') NOT NULL,
  created_by        INT UNSIGNED    NOT NULL,
  created_at        DATETIME(6)     NOT NULL,
  changed_version   BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (module_id),
  UNIQUE KEY uk_name (db_id, kind, name)
) ENGINE=InnoDB";

/// `logic_versions`.
pub const DDL_VERSIONS: &str = "CREATE TABLE IF NOT EXISTS logic_versions (
  module_id         BIGINT UNSIGNED NOT NULL,
  version           BIGINT UNSIGNED NOT NULL,
  source            LONGTEXT        NOT NULL,
  source_sha256     BINARY(32)      NOT NULL,
  signature         JSON            NOT NULL,
  catalog_version   BIGINT UNSIGNED NOT NULL,
  compiler_version  VARCHAR(32)     NOT NULL,
  tier              ENUM('mariadb','engine','refused') NOT NULL,
  placement         JSON            NOT NULL,
  plsql             LONGTEXT        NULL,
  diff_report       JSON            NULL,
  state             ENUM('compiling','compiled','deploying','staged','rolling_out',
                         'active','superseded','retired','failed') NOT NULL,
  ddl_job_id        BIGINT UNSIGNED NULL,
  created_by        INT UNSIGNED    NOT NULL,
  created_at        DATETIME(6)     NOT NULL,
  activated_at      DATETIME(6)     NULL,
  PRIMARY KEY (module_id, version)
) ENGINE=InnoDB";

/// `logic_bindings`.
pub const DDL_BINDINGS: &str = "CREATE TABLE IF NOT EXISTS logic_bindings (
  binding_id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  module_id         BIGINT UNSIGNED NOT NULL,
  object_id         BIGINT UNSIGNED NULL,
  column_name       VARCHAR(64)     NULL,
  timing            ENUM('before_write','after_commit','on_read','on_write') NOT NULL,
  events            SET('insert','update','delete') NOT NULL DEFAULT '',
  when_fields       JSON            NULL,
  condition_sql     TEXT            NULL,
  ordinal           SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  enforcement       ENUM('trigger','check','generated','gateway') NULL,
  subscription_id   BIGINT UNSIGNED NULL,
  state             ENUM('active','paused','dropping') NOT NULL,
  PRIMARY KEY (binding_id),
  KEY by_object (object_id, timing)
) ENGINE=InnoDB";

/// `logic_schedules`.
pub const DDL_SCHEDULES: &str = "CREATE TABLE IF NOT EXISTS logic_schedules (
  module_id         BIGINT UNSIGNED NOT NULL,
  cron              VARCHAR(128)    NOT NULL,
  time_zone         VARCHAR(64)     NOT NULL DEFAULT 'UTC',
  target_module_id  BIGINT UNSIGNED NOT NULL,
  args              JSON            NOT NULL,
  overlap           ENUM('skip','queue_one') NOT NULL DEFAULT 'skip',
  paused            BOOLEAN         NOT NULL DEFAULT FALSE,
  next_fire_at      DATETIME(3)     NOT NULL,
  last_fire_at      DATETIME(3)     NULL,
  last_outcome      ENUM('ok','error','budget','skipped','misfired') NULL,
  running_job_id    BINARY(16)      NULL,
  queued            BOOLEAN         NOT NULL DEFAULT FALSE,
  PRIMARY KEY (module_id),
  KEY by_next (paused, next_fire_at)
) ENGINE=InnoDB";

/// `logic_dead_letters`.
pub const DDL_DEAD_LETTERS: &str = "CREATE TABLE IF NOT EXISTS logic_dead_letters (
  dl_id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  binding_id        BIGINT UNSIGNED NOT NULL,
  version           BIGINT UNSIGNED NOT NULL,
  event_key         VARBINARY(64)   NOT NULL,
  resume_token      VARBINARY(512)  NOT NULL,
  event             JSON            NOT NULL,
  attempts          INT UNSIGNED    NOT NULL,
  error_code        INT UNSIGNED    NOT NULL,
  error_message     TEXT            NOT NULL,
  trace_id          BINARY(16)      NOT NULL,
  last_failed_at    DATETIME(6)     NOT NULL,
  state             ENUM('dead','replaying','replayed','discarded') NOT NULL,
  PRIMARY KEY (dl_id),
  UNIQUE KEY uk_event (binding_id, event_key),
  KEY by_state (state, last_failed_at)
) ENGINE=InnoDB";

/// `logic_log` (R7 order 5, not in the book's five tables): what
/// after-commit triggers and scheduled jobs did, for `sprigctl logic
/// logs`, until the tenant log stream exists.
pub const DDL_LOG: &str = "CREATE TABLE IF NOT EXISTS logic_log (
  log_id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  module_id         BIGINT UNSIGNED NOT NULL,
  version           BIGINT UNSIGNED NULL,
  at                DATETIME(6)     NOT NULL,
  level             ENUM('info','warn','error') NOT NULL,
  source            ENUM('trigger','schedule','deploy') NOT NULL,
  message           TEXT            NOT NULL,
  PRIMARY KEY (log_id),
  KEY by_module (module_id, log_id),
  KEY by_time (at)
) ENGINE=InnoDB";

/// The module's migrations.
pub static LOGIC_MIGRATIONS: &[Migration] = &[
    Migration {
        format: 1,
        phase: Phase::Expand,
        name: "logic_modules, logic_versions, logic_bindings, logic_schedules, logic_dead_letters",
        statements: &[
            DDL_MODULES,
            DDL_VERSIONS,
            DDL_BINDINGS,
            DDL_SCHEDULES,
            DDL_DEAD_LETTERS,
        ],
    },
    Migration {
        format: 2,
        phase: Phase::Expand,
        name: "logic_log",
        statements: &[DDL_LOG],
    },
];

/// The module, for [`Migrator::new`](sprig_catalog::Migrator::new) and
/// tenant bootstrap.
pub const LOGIC_MODULE: Module = Module {
    name: MODULE,
    migrations: LOGIC_MIGRATIONS,
};

/// A `logic_modules` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModuleRow {
    /// The module's id (an object id).
    pub module_id: u64,
    /// The database.
    pub db_id: u32,
    /// The name.
    pub name: String,
    /// `function`, `procedure`, ...
    pub kind: String,
    /// `kirun`, `javascript`, `sql`, `lua`.
    pub runtime: String,
    /// `invoker` or `definer`.
    pub security: String,
    /// `auto`, `mariadb` or `engine`.
    pub tier_pref: String,
    /// The live version.
    pub active_version: Option<u64>,
    /// `active`, `disabled`, `dropping`, `dropped`.
    pub state: String,
    /// Who created it.
    pub created_by: u32,
}

/// A `logic_versions` row (without the source text).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VersionRow {
    /// The module.
    pub module_id: u64,
    /// The version, never reused.
    pub version: u64,
    /// The signature: params and events.
    pub signature: serde_json::Value,
    /// `mariadb`, `engine` or `refused`.
    pub tier: String,
    /// Reasons, route, objects, cost.
    pub placement: serde_json::Value,
    /// The version's state.
    pub state: String,
    /// The compiler that compiled it.
    pub compiler_version: String,
}

/// A new version to record.
#[derive(Debug, Clone)]
pub struct NewVersion<'a> {
    /// The source text.
    pub source: &'a str,
    /// The signature.
    pub signature: serde_json::Value,
    /// The catalog version compiled against.
    pub catalog_version: u64,
    /// `mariadb`, `engine` or `refused`.
    pub tier: &'a str,
    /// Placement as JSON.
    pub placement: serde_json::Value,
    /// The generated text.
    pub plsql: Option<String>,
    /// The deploying user.
    pub created_by: u32,
}

fn map_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        "the logic catalog is unreachable",
    )
    .with_source(e)
}

/// Reads and writes of the logic tables in one catalog schema.
#[derive(Debug, Clone)]
pub struct LogicStore {
    modules: String,
    versions: String,
    bindings: String,
    schedules: String,
    dead: String,
    log: String,
}

/// A `logic_bindings` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BindingRow {
    /// The binding.
    pub binding_id: u64,
    /// Its module.
    pub module_id: u64,
    /// The table (`objects.object_id`).
    pub object_id: Option<u64>,
    /// Computed fields: the column.
    pub column_name: Option<String>,
    /// `before_write`, `after_commit`, `on_read`, `on_write`.
    pub timing: String,
    /// `insert,update,delete`.
    pub events: String,
    /// `FOR UPDATE OF (...)`.
    pub when_fields: Vec<String>,
    /// `trigger`, `check`, `generated`, `gateway`.
    pub enforcement: Option<String>,
    /// `active`, `paused`, `dropping`.
    pub state: String,
}

/// A `logic_schedules` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScheduleRow {
    /// The schedule's module.
    pub module_id: u64,
    /// Cron text.
    pub cron: String,
    /// IANA zone.
    pub time_zone: String,
    /// The procedure it calls.
    pub target_module_id: u64,
    /// Arguments, a JSON array.
    pub args: String,
    /// `skip` or `queue_one`.
    pub overlap: String,
    /// Paused.
    pub paused: bool,
    /// Next fire time, UTC, Unix ms.
    pub next_fire_ms: u64,
    /// Last fire time.
    pub last_fire_ms: Option<u64>,
    /// `ok`, `error`, `budget`, `skipped`, `misfired`.
    pub last_outcome: Option<String>,
    /// The live run's job id, hex.
    pub running_job_id: Option<String>,
    /// A run waits (`queue_one`).
    pub queued: bool,
}

/// A `logic_dead_letters` row.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeadLetterRow {
    /// Its id.
    pub dl_id: u64,
    /// The binding.
    pub binding_id: u64,
    /// The version that failed.
    pub version: u64,
    /// Shard, GTID and row ordinal.
    pub event_key: String,
    /// The event.
    pub event: serde_json::Value,
    /// Attempts so far.
    pub attempts: u32,
    /// The last error's number.
    pub error_code: u32,
    /// Its message.
    pub error_message: String,
    /// When it last failed, `YYYY-MM-DD hh:mm:ss`.
    pub last_failed_at: String,
    /// `dead`, `replaying`, `replayed`, `discarded`.
    pub state: String,
}

/// A `logic_log` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LogRow {
    /// Its id.
    pub log_id: u64,
    /// The module.
    pub module_id: u64,
    /// The version, if one ran.
    pub version: Option<u64>,
    /// When, `YYYY-MM-DD hh:mm:ss.ffffff` UTC.
    pub at: String,
    /// `info`, `warn`, `error`.
    pub level: String,
    /// `trigger`, `schedule`, `deploy`.
    pub source: String,
    /// What happened.
    pub message: String,
}

const MODULE_COLS: &str = "module_id, db_id, name, kind, runtime, security, tier_pref, \
                           active_version, state, created_by";

fn module_row(r: Row) -> Result<ModuleRow> {
    type T = (
        u64,
        u32,
        String,
        String,
        String,
        String,
        String,
        Option<u64>,
        String,
        u32,
    );
    let (
        module_id,
        db_id,
        name,
        kind,
        runtime,
        security,
        tier_pref,
        active_version,
        state,
        created_by,
    ): T = mysql_async::from_row_opt(r)
        .map_err(|e| SprigError::internal(format!("logic_modules row: {e}")))?;
    Ok(ModuleRow {
        module_id,
        db_id,
        name,
        kind,
        runtime,
        security,
        tier_pref,
        active_version,
        state,
        created_by,
    })
}

/// A function or procedure as a caller outside SQL finds it (chapter 37,
/// "Logic functions as endpoints").
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Callable {
    /// The module.
    pub module_id: u64,
    /// Its name, as stored.
    pub name: String,
    /// `function` or `procedure`.
    pub kind: String,
    /// `kirun`, `javascript`, `lua`.
    pub runtime: String,
    /// The protocols it is exposed to (`sql`, `http`, `grpc`, ...).
    pub exposure: Vec<String>,
    /// The active version.
    pub version: Option<u64>,
    /// The active version's tier (`mariadb`, `engine`).
    pub tier: Option<String>,
    /// The active version's signature (params, events), if any.
    pub signature: Option<serde_json::Value>,
}

impl Callable {
    /// Whether it may be called over `protocol`.
    #[must_use]
    pub fn exposed_to(&self, protocol: &str) -> bool {
        self.exposure.iter().any(|p| p == protocol)
    }

    /// The parameter names, in call order.
    #[must_use]
    pub fn params(&self) -> Vec<String> {
        self.signature
            .as_ref()
            .and_then(|s| s["params"].as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|p| p["name"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl LogicStore {
    /// Records a module's `EXPOSE (...)` list (`sql` is always in it).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn set_exposure(
        &self,
        c: &mut Conn,
        module_id: u64,
        exposure: &[String],
    ) -> Result<()> {
        let mut list: Vec<&str> = vec!["sql"];
        for e in exposure {
            if !list.contains(&e.as_str()) {
                list.push(e.as_str());
            }
        }
        c.exec_drop(
            format!(
                "UPDATE {} SET exposure = ? WHERE module_id = ?",
                self.modules
            ),
            (list.join(","), module_id),
        )
        .await
        .map_err(map_err)
    }

    /// The live function or procedure `name` of `db_id` (functions first),
    /// with its exposure and active signature.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn callable(&self, c: &mut Conn, db_id: u32, name: &str) -> Result<Option<Callable>> {
        type R = (
            u64,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<u64>,
            Option<String>,
        );
        let r: Option<R> = c
            .exec_first(
                format!(
                    "SELECT m.module_id, m.name, m.kind, m.runtime, CAST(m.exposure AS CHAR), \
                     CAST(v.signature AS CHAR), m.active_version, CAST(v.tier AS CHAR) \
                     FROM {} m LEFT JOIN {} v \
                     ON v.module_id = m.module_id AND v.version = m.active_version \
                     WHERE m.db_id = ? AND m.name = ? AND m.kind IN ('function', 'procedure') \
                     AND m.state = 'active' ORDER BY m.kind LIMIT 1",
                    self.modules, self.versions
                ),
                (db_id, name),
            )
            .await
            .map_err(map_err)?;
        Ok(r.map(
            |(module_id, name, kind, runtime, exposure, signature, version, tier)| Callable {
                module_id,
                name,
                kind,
                runtime,
                exposure: exposure
                    .split(',')
                    .filter(|x| !x.is_empty())
                    .map(str::to_owned)
                    .collect(),
                signature: signature.and_then(|s| serde_json::from_str(&s).ok()),
                version,
                tier,
            },
        ))
    }

    /// The tables in `schema` (`t<tenant>_sprig`).
    #[must_use]
    pub fn new(schema: &str) -> Self {
        let q = quote(schema);
        Self {
            modules: format!("{q}.logic_modules"),
            versions: format!("{q}.logic_versions"),
            bindings: format!("{q}.logic_bindings"),
            schedules: format!("{q}.logic_schedules"),
            dead: format!("{q}.logic_dead_letters"),
            log: format!("{q}.logic_log"),
        }
    }

    /// The live module of `kind` called `name` in `db_id`.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn module(
        &self,
        c: &mut Conn,
        db_id: u32,
        kind: &str,
        name: &str,
    ) -> Result<Option<ModuleRow>> {
        let r: Option<Row> = c
            .exec_first(
                format!(
                    "SELECT {MODULE_COLS} FROM {} WHERE db_id = ? AND kind = ? AND name = ? AND state <> 'dropped'",
                    self.modules
                ),
                (db_id, kind, name),
            )
            .await
            .map_err(map_err)?;
        r.map(module_row).transpose()
    }

    /// Every live module of `db_id` (all databases with `None`).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn modules(&self, c: &mut Conn, db_id: Option<u32>) -> Result<Vec<ModuleRow>> {
        let rows: Vec<Row> = match db_id {
            Some(d) => c
                .exec(
                    format!(
                        "SELECT {MODULE_COLS} FROM {} WHERE db_id = ? AND state <> 'dropped' ORDER BY db_id, name",
                        self.modules
                    ),
                    (d,),
                )
                .await,
            None => c
                .query(format!(
                    "SELECT {MODULE_COLS} FROM {} WHERE state <> 'dropped' ORDER BY db_id, name",
                    self.modules
                ))
                .await,
        }
        .map_err(map_err)?;
        rows.into_iter().map(module_row).collect()
    }

    /// Inserts a module (its id already allocated).
    ///
    /// # Errors
    /// 39869 when another deploy created it first; 39070 or a backend
    /// error.
    pub async fn create_module(
        &self,
        c: &mut Conn,
        m: &ModuleRow,
        catalog_version: u64,
    ) -> Result<()> {
        let r = c
            .exec_drop(
                format!(
                    "INSERT INTO {} (module_id, db_id, name, kind, runtime, security, tier_pref, \
                     state, created_by, created_at, changed_version) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, UTC_TIMESTAMP(6), ?)",
                    self.modules
                ),
                (
                    m.module_id,
                    m.db_id,
                    &m.name,
                    &m.kind,
                    &m.runtime,
                    &m.security,
                    &m.tier_pref,
                    m.created_by,
                    catalog_version,
                ),
            )
            .await;
        match r {
            Err(mysql_async::Error::Server(s)) if s.code == 1062 => Err(SprigError::new(
                SprigCode::LogicVersionConflict,
                format!("{} {} is being deployed by another session", m.kind, m.name),
            )),
            r => r.map_err(map_err),
        }
    }

    /// Updates a module's declared options for a new version.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn update_options(
        &self,
        c: &mut Conn,
        m: &ModuleRow,
        catalog_version: u64,
    ) -> Result<()> {
        c.exec_drop(
            format!(
                "UPDATE {} SET runtime = ?, security = ?, tier_pref = ?, changed_version = ? WHERE module_id = ?",
                self.modules
            ),
            (&m.runtime, &m.security, &m.tier_pref, catalog_version, m.module_id),
        )
        .await
        .map_err(map_err)
    }

    /// The number the next version gets: one past every existing one.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn next_version(&self, c: &mut Conn, module_id: u64) -> Result<u64> {
        let last: Option<Option<u64>> = c
            .exec_first(
                format!(
                    "SELECT MAX(version) FROM {} WHERE module_id = ?",
                    self.versions
                ),
                (module_id,),
            )
            .await
            .map_err(map_err)?;
        Ok(last.flatten().unwrap_or(0) + 1)
    }

    /// Records version `version` (from [`next_version`](Self::next_version)).
    /// Two deploys racing for one number: the loser gets 39869.
    ///
    /// # Errors
    /// 39869, 39070 or a backend error.
    pub async fn add_version(
        &self,
        c: &mut Conn,
        module_id: u64,
        version: u64,
        v: &NewVersion<'_>,
    ) -> Result<()> {
        let sha = {
            use sha2::Digest;
            sha2::Sha256::digest(v.source.as_bytes()).to_vec()
        };
        let state = if v.tier == "refused" {
            "failed"
        } else {
            "compiled"
        };
        let r = c
            .exec_drop(
                format!(
                    "INSERT INTO {} (module_id, version, source, source_sha256, signature, catalog_version, \
                     compiler_version, tier, placement, plsql, state, created_by, created_at) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6))",
                    self.versions
                ),
                (
                    module_id,
                    version,
                    v.source,
                    sha,
                    v.signature.to_string(),
                    v.catalog_version,
                    sprig_logic_compile::COMPILER_VERSION,
                    v.tier,
                    v.placement.to_string(),
                    v.plsql.as_deref(),
                    state,
                    v.created_by,
                ),
            )
            .await;
        match r {
            Err(mysql_async::Error::Server(s)) if s.code == 1062 => Err(SprigError::new(
                SprigCode::LogicVersionConflict,
                "another deploy of this module took the same version",
            )),
            r => r.map_err(map_err),
        }
    }

    /// Sets a version's state.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn set_version_state(
        &self,
        c: &mut Conn,
        module_id: u64,
        version: u64,
        state: &str,
    ) -> Result<()> {
        c.exec_drop(
            format!(
                "UPDATE {} SET state = ? WHERE module_id = ? AND version = ?",
                self.versions
            ),
            (state, module_id, version),
        )
        .await
        .map_err(map_err)
    }

    /// Makes `version` the active one; the one before is `superseded`.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn activate(
        &self,
        c: &mut Conn,
        module_id: u64,
        version: u64,
        catalog_version: u64,
    ) -> Result<()> {
        c.exec_drop(
            format!(
                "UPDATE {} SET state = 'superseded' WHERE module_id = ? AND state = 'active' AND version <> ?",
                self.versions
            ),
            (module_id, version),
        )
        .await
        .map_err(map_err)?;
        c.exec_drop(
            format!(
                "UPDATE {} SET state = 'active', activated_at = UTC_TIMESTAMP(6) WHERE module_id = ? AND version = ?",
                self.versions
            ),
            (module_id, version),
        )
        .await
        .map_err(map_err)?;
        c.exec_drop(
            format!(
                "UPDATE {} SET active_version = ?, changed_version = ? WHERE module_id = ?",
                self.modules
            ),
            (version, catalog_version, module_id),
        )
        .await
        .map_err(map_err)
    }

    /// A module's versions, newest first.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn versions(&self, c: &mut Conn, module_id: u64) -> Result<Vec<VersionRow>> {
        let rows: Vec<(u64, u64, String, String, String, String, String)> = c
            .exec(
                format!(
                    "SELECT module_id, version, CAST(signature AS CHAR), tier, CAST(placement AS CHAR), state, \
                     compiler_version FROM {} WHERE module_id = ? ORDER BY version DESC",
                    self.versions
                ),
                (module_id,),
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(
                |(module_id, version, sig, tier, pl, state, cv)| VersionRow {
                    module_id,
                    version,
                    signature: serde_json::from_str(&sig).unwrap_or_default(),
                    tier,
                    placement: serde_json::from_str(&pl).unwrap_or_default(),
                    state,
                    compiler_version: cv,
                },
            )
            .collect())
    }

    /// A version's generated text.
    ///
    /// # Errors
    /// 39860 when there is no such version; 39070 or a backend error.
    pub async fn plsql(
        &self,
        c: &mut Conn,
        module_id: u64,
        version: u64,
    ) -> Result<Option<String>> {
        let r: Option<Option<String>> = c
            .exec_first(
                format!(
                    "SELECT plsql FROM {} WHERE module_id = ? AND version = ?",
                    self.versions
                ),
                (module_id, version),
            )
            .await
            .map_err(map_err)?;
        r.ok_or_else(|| SprigError::new(SprigCode::LogicNotFound, format!("no version {version}")))
    }

    /// A module by id.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn module_by_id(&self, c: &mut Conn, module_id: u64) -> Result<Option<ModuleRow>> {
        let r: Option<Row> = c
            .exec_first(
                format!(
                    "SELECT {MODULE_COLS} FROM {} WHERE module_id = ?",
                    self.modules
                ),
                (module_id,),
            )
            .await
            .map_err(map_err)?;
        r.map(module_row).transpose()
    }

    /// A version's source text.
    ///
    /// # Errors
    /// 39860 when there is no such version; 39070 or a backend error.
    pub async fn source(&self, c: &mut Conn, module_id: u64, version: u64) -> Result<String> {
        let r: Option<String> = c
            .exec_first(
                format!(
                    "SELECT source FROM {} WHERE module_id = ? AND version = ?",
                    self.versions
                ),
                (module_id, version),
            )
            .await
            .map_err(map_err)?;
        r.ok_or_else(|| SprigError::new(SprigCode::LogicNotFound, format!("no version {version}")))
    }

    /// A version's signature and tier.
    ///
    /// # Errors
    /// 39860 when there is no such version; 39070 or a backend error.
    pub async fn version_signature(
        &self,
        c: &mut Conn,
        module_id: u64,
        version: u64,
    ) -> Result<(serde_json::Value, String)> {
        let r: Option<(String, String)> = c
            .exec_first(
                format!(
                    "SELECT CAST(signature AS CHAR), tier FROM {} WHERE module_id = ? AND version = ?",
                    self.versions
                ),
                (module_id, version),
            )
            .await
            .map_err(map_err)?;
        let (sig, tier) = r.ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicNotFound,
                format!("no version {version} of module {module_id}"),
            )
        })?;
        Ok((serde_json::from_str(&sig).unwrap_or_default(), tier))
    }

    /// The binding of a module (validations, triggers and computed fields
    /// have one each).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn binding(&self, c: &mut Conn, module_id: u64) -> Result<Option<BindingRow>> {
        let rows = self
            .bindings_where(c, "module_id = ? ORDER BY binding_id LIMIT 1", module_id)
            .await?;
        Ok(rows.into_iter().next())
    }

    /// A binding by id.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn binding_by_id(&self, c: &mut Conn, binding_id: u64) -> Result<Option<BindingRow>> {
        let rows = self.bindings_where(c, "binding_id = ?", binding_id).await?;
        Ok(rows.into_iter().next())
    }

    async fn bindings_where(&self, c: &mut Conn, cond: &str, v: u64) -> Result<Vec<BindingRow>> {
        type T = (
            u64,
            u64,
            Option<u64>,
            Option<String>,
            String,
            String,
            Option<String>,
            Option<String>,
            String,
        );
        let rows: Vec<T> = c
            .exec(
                format!(
                    "SELECT binding_id, module_id, object_id, column_name, timing, events, \
                     CAST(when_fields AS CHAR), enforcement, state FROM {} WHERE {cond}",
                    self.bindings
                ),
                (v,),
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    binding_id,
                    module_id,
                    object_id,
                    column_name,
                    timing,
                    events,
                    when,
                    enforcement,
                    state,
                )| {
                    BindingRow {
                        binding_id,
                        module_id,
                        object_id,
                        column_name,
                        timing,
                        events,
                        when_fields: when
                            .and_then(|w| serde_json::from_str(&w).ok())
                            .unwrap_or_default(),
                        enforcement,
                        state,
                    }
                },
            )
            .collect())
    }

    /// Creates or updates a module's binding; returns its id.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn put_binding(&self, c: &mut Conn, b: &BindingRow) -> Result<u64> {
        let when = (!b.when_fields.is_empty())
            .then(|| serde_json::to_string(&b.when_fields).unwrap_or_default());
        if b.binding_id != 0 {
            c.exec_drop(
                format!(
                    "UPDATE {} SET object_id = ?, column_name = ?, timing = ?, events = ?, when_fields = ?, \
                     enforcement = ?, state = ? WHERE binding_id = ?",
                    self.bindings
                ),
                (
                    b.object_id,
                    &b.column_name,
                    &b.timing,
                    &b.events,
                    when,
                    &b.enforcement,
                    &b.state,
                    b.binding_id,
                ),
            )
            .await
            .map_err(map_err)?;
            return Ok(b.binding_id);
        }
        c.exec_drop(
            format!(
                "INSERT INTO {} (module_id, object_id, column_name, timing, events, when_fields, enforcement, state) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                self.bindings
            ),
            (
                b.module_id,
                b.object_id,
                &b.column_name,
                &b.timing,
                &b.events,
                when,
                &b.enforcement,
                &b.state,
            ),
        )
        .await
        .map_err(map_err)?;
        c.last_insert_id()
            .ok_or_else(|| SprigError::internal("logic_bindings gave no id"))
    }

    /// Every active after-commit binding.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn after_commit_bindings(&self, c: &mut Conn) -> Result<Vec<BindingRow>> {
        self.bindings_where(
            c,
            "timing = 'after_commit' AND state = 'active' AND module_id <> ?",
            0,
        )
        .await
    }

    /// Creates or replaces a schedule.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn put_schedule(&self, c: &mut Conn, s: &ScheduleRow) -> Result<()> {
        c.exec_drop(
            format!(
                "INSERT INTO {} (module_id, cron, time_zone, target_module_id, args, overlap, paused, next_fire_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, TIMESTAMPADD(MICROSECOND, ? * 1000, '1970-01-01 00:00:00')) \
                 ON DUPLICATE KEY UPDATE cron = VALUES(cron), time_zone = VALUES(time_zone), \
                 target_module_id = VALUES(target_module_id), args = VALUES(args), overlap = VALUES(overlap), \
                 paused = VALUES(paused), next_fire_at = VALUES(next_fire_at)",
                self.schedules
            ),
            (
                s.module_id,
                &s.cron,
                &s.time_zone,
                s.target_module_id,
                &s.args,
                &s.overlap,
                s.paused,
                s.next_fire_ms,
            ),
        )
        .await
        .map_err(map_err)
    }

    /// A schedule.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn schedule(&self, c: &mut Conn, module_id: u64) -> Result<Option<ScheduleRow>> {
        type T = (
            u64,
            String,
            String,
            u64,
            String,
            String,
            bool,
            u64,
            Option<u64>,
            Option<String>,
            Option<String>,
            bool,
        );
        let r: Option<T> = c
            .exec_first(
                format!(
                    "SELECT module_id, cron, time_zone, target_module_id, CAST(args AS CHAR), overlap, paused, \
                     (TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', next_fire_at) DIV 1000), \
                     (TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', last_fire_at) DIV 1000), last_outcome, \
                     LOWER(HEX(running_job_id)), queued FROM {} WHERE module_id = ?",
                    self.schedules
                ),
                (module_id,),
            )
            .await
            .map_err(map_err)?;
        Ok(r.map(
            |(
                module_id,
                cron,
                time_zone,
                target_module_id,
                args,
                overlap,
                paused,
                next,
                last,
                outcome,
                running,
                queued,
            )| {
                ScheduleRow {
                    module_id,
                    cron,
                    time_zone,
                    target_module_id,
                    args,
                    overlap,
                    paused,
                    next_fire_ms: next,
                    last_fire_ms: last,
                    last_outcome: outcome,
                    running_job_id: running,
                    queued,
                }
            },
        ))
    }

    /// The fire's compare-and-set (chapter 41, "Scheduled jobs"): only if
    /// the schedule is still due at `seen_next_ms` and no run is live,
    /// record `job` as the live run and move `next_fire_at` on. `false`
    /// when another fire (or a live run) got there first.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn fire(
        &self,
        c: &mut Conn,
        module_id: u64,
        seen_next_ms: u64,
        new_next_ms: u64,
        job: &[u8; 16],
    ) -> Result<bool> {
        c.exec_drop(
            format!(
                "UPDATE {} SET running_job_id = ?, last_fire_at = UTC_TIMESTAMP(3), queued = FALSE, \
                 next_fire_at = TIMESTAMPADD(MICROSECOND, ? * 1000, '1970-01-01 00:00:00') WHERE module_id = ? AND paused = FALSE \
                 AND running_job_id IS NULL \
                 AND (TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', next_fire_at) DIV 1000) = ?",
                self.schedules
            ),
            (job.to_vec(), new_next_ms, module_id, seen_next_ms),
        )
        .await
        .map_err(map_err)?;
        Ok(c.affected_rows() == 1)
    }

    /// Moves a schedule's next fire time on without a run (`skipped` or
    /// `misfired`), and with `queue`, remembers one run to make when the
    /// live one ends.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn pass(
        &self,
        c: &mut Conn,
        module_id: u64,
        seen_next_ms: u64,
        new_next_ms: u64,
        outcome: &str,
        queue: bool,
    ) -> Result<bool> {
        c.exec_drop(
            format!(
                "UPDATE {} SET next_fire_at = TIMESTAMPADD(MICROSECOND, ? * 1000, '1970-01-01 00:00:00'), last_outcome = ?, queued = queued OR ? \
                 WHERE module_id = ? AND (TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', next_fire_at) DIV 1000) = ?",
                self.schedules
            ),
            (new_next_ms, outcome, queue, module_id, seen_next_ms),
        )
        .await
        .map_err(map_err)?;
        Ok(c.affected_rows() == 1)
    }

    /// A queued run (`OVERLAP QUEUE`) takes the slot: like [`fire`](Self::fire)
    /// but without moving `next_fire_at`.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn fire_queued(&self, c: &mut Conn, module_id: u64, job: &[u8; 16]) -> Result<bool> {
        c.exec_drop(
            format!(
                "UPDATE {} SET running_job_id = ?, last_fire_at = UTC_TIMESTAMP(3), queued = FALSE \
                 WHERE module_id = ? AND paused = FALSE AND running_job_id IS NULL AND queued",
                self.schedules
            ),
            (job.to_vec(), module_id),
        )
        .await
        .map_err(map_err)?;
        Ok(c.affected_rows() == 1)
    }

    /// Ends a run: clears `running_job_id` if it is still `job`, records
    /// the outcome. `false` if another run holds the slot.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn run_ended(
        &self,
        c: &mut Conn,
        module_id: u64,
        job: &[u8; 16],
        outcome: &str,
    ) -> Result<bool> {
        c.exec_drop(
            format!(
                "UPDATE {} SET running_job_id = NULL, last_outcome = ? WHERE module_id = ? AND running_job_id = ?",
                self.schedules
            ),
            (outcome, module_id, job.to_vec()),
        )
        .await
        .map_err(map_err)?;
        Ok(c.affected_rows() == 1)
    }

    /// Whether `job` is the schedule's live run.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn is_running(&self, c: &mut Conn, module_id: u64, job: &[u8; 16]) -> Result<bool> {
        let r: Option<u64> = c
            .exec_first(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE module_id = ? AND running_job_id = ?",
                    self.schedules
                ),
                (module_id, job.to_vec()),
            )
            .await
            .map_err(map_err)?;
        Ok(r.unwrap_or(0) == 1)
    }

    /// Records a dead letter, or refreshes one already there for the same
    /// event (state back to `dead`).
    ///
    /// # Errors
    /// 39070 or a backend error.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_dead_letter(
        &self,
        c: &mut Conn,
        binding_id: u64,
        version: u64,
        event_key: &str,
        resume_token: &[u8],
        event: &serde_json::Value,
        attempts: u32,
        error_code: u32,
        error_message: &str,
    ) -> Result<()> {
        let trace = {
            use sha2::Digest;
            let d = sha2::Sha256::digest(format!("{binding_id}/{event_key}").as_bytes());
            d[..16].to_vec()
        };
        c.exec_drop(
            format!(
                "INSERT INTO {} (binding_id, version, event_key, resume_token, event, attempts, error_code, \
                 error_message, trace_id, last_failed_at, state) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6), 'dead') \
                 ON DUPLICATE KEY UPDATE version = VALUES(version), attempts = attempts + VALUES(attempts), \
                 error_code = VALUES(error_code), error_message = VALUES(error_message), \
                 last_failed_at = VALUES(last_failed_at), state = 'dead'",
                self.dead
            ),
            (
                binding_id,
                version,
                event_key.as_bytes().to_vec(),
                resume_token.to_vec(),
                event.to_string(),
                attempts,
                error_code,
                error_message,
                trace,
            ),
        )
        .await
        .map_err(map_err)
    }

    /// Dead letters, oldest first: of one binding (`Some`), in one state
    /// (`Some`).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn dead_letters(
        &self,
        c: &mut Conn,
        binding_id: Option<u64>,
        state: Option<&str>,
        limit: u32,
    ) -> Result<Vec<DeadLetterRow>> {
        type T = (
            u64,
            u64,
            u64,
            Vec<u8>,
            String,
            u32,
            u32,
            String,
            String,
            String,
        );
        let rows: Vec<T> = c
            .exec(
                format!(
                    "SELECT dl_id, binding_id, version, event_key, CAST(event AS CHAR), attempts, error_code, \
                     error_message, DATE_FORMAT(last_failed_at, '%Y-%m-%d %H:%i:%s'), state FROM {} \
                     WHERE (? IS NULL OR binding_id = ?) AND (? IS NULL OR state = ?) ORDER BY dl_id LIMIT ?",
                    self.dead
                ),
                (binding_id, binding_id, state, state, limit),
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    dl_id,
                    binding_id,
                    version,
                    key,
                    event,
                    attempts,
                    error_code,
                    error_message,
                    at,
                    state,
                )| {
                    DeadLetterRow {
                        dl_id,
                        binding_id,
                        version,
                        event_key: String::from_utf8_lossy(&key).into_owned(),
                        event: serde_json::from_str(&event).unwrap_or_default(),
                        attempts,
                        error_code,
                        error_message,
                        last_failed_at: at,
                        state,
                    }
                },
            )
            .collect())
    }

    /// Moves dead letters from one state to another (`dead` to
    /// `replaying` or `discarded`; `replaying` to `replayed`). `ids`
    /// empty: every one of `binding_id` in state `from`. Returns how many
    /// changed.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn set_dead_letters(
        &self,
        c: &mut Conn,
        binding_id: Option<u64>,
        ids: &[u64],
        from: &str,
        to: &str,
    ) -> Result<u64> {
        let list = if ids.is_empty() {
            "TRUE".to_owned()
        } else {
            format!(
                "dl_id IN ({})",
                ids.iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        c.exec_drop(
            format!(
                "UPDATE {} SET state = ? WHERE state = ? AND (? IS NULL OR binding_id = ?) AND {list}",
                self.dead
            ),
            (to, from, binding_id, binding_id),
        )
        .await
        .map_err(map_err)?;
        Ok(c.affected_rows())
    }

    /// Appends to `logic_log` (best effort for callers: they log a
    /// failure and carry on).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn log(
        &self,
        c: &mut Conn,
        module_id: u64,
        version: Option<u64>,
        level: &str,
        source: &str,
        message: &str,
    ) -> Result<()> {
        let m: String = message.chars().take(4000).collect();
        c.exec_drop(
            format!(
                "INSERT INTO {} (module_id, version, at, level, source, message) \
                 VALUES (?, ?, UTC_TIMESTAMP(6), ?, ?, ?)",
                self.log
            ),
            (module_id, version, level, source, m),
        )
        .await
        .map_err(map_err)
    }

    /// The newest log lines, oldest first: of one module (`Some`), after
    /// `after_id`.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn logs(
        &self,
        c: &mut Conn,
        module_id: Option<u64>,
        after_id: u64,
        limit: u32,
    ) -> Result<Vec<LogRow>> {
        type T = (u64, u64, Option<u64>, String, String, String, String);
        let mut rows: Vec<T> = c
            .exec(
                format!(
                    "SELECT log_id, module_id, version, DATE_FORMAT(at, '%Y-%m-%d %H:%i:%s.%f'), level, source, message \
                     FROM {} WHERE (? IS NULL OR module_id = ?) AND log_id > ? ORDER BY log_id DESC LIMIT ?",
                    self.log
                ),
                (module_id, module_id, after_id, limit),
            )
            .await
            .map_err(map_err)?;
        rows.reverse();
        Ok(rows
            .into_iter()
            .map(
                |(log_id, module_id, version, at, level, source, message)| LogRow {
                    log_id,
                    module_id,
                    version,
                    at,
                    level,
                    source,
                    message,
                },
            )
            .collect())
    }

    /// Deletes log lines older than `days`, and replayed or discarded dead
    /// letters older than `dead_days`, at most `limit` rows each.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn purge(&self, c: &mut Conn, days: u32, dead_days: u32, limit: u32) -> Result<()> {
        c.exec_drop(
            format!(
                "DELETE FROM {} WHERE at < UTC_TIMESTAMP(6) - INTERVAL ? DAY LIMIT ?",
                self.log
            ),
            (days, limit),
        )
        .await
        .map_err(map_err)?;
        c.exec_drop(
            format!(
                "DELETE FROM {} WHERE last_failed_at < UTC_TIMESTAMP(6) - INTERVAL ? DAY LIMIT ?",
                self.dead
            ),
            (dead_days, limit),
        )
        .await
        .map_err(map_err)
    }

    /// Marks a module dropped; its name gets `#<id>` so the name can be
    /// used again.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn mark_dropped(
        &self,
        c: &mut Conn,
        module_id: u64,
        catalog_version: u64,
    ) -> Result<()> {
        c.exec_drop(
            format!(
                "UPDATE {} SET state = 'dropped', changed_version = ?, \
                 name = LEFT(CONCAT(name, '#', module_id), 64) WHERE module_id = ?",
                self.modules
            ),
            (catalog_version, module_id),
        )
        .await
        .map_err(map_err)?;
        c.exec_drop(
            format!(
                "UPDATE {} SET state = 'retired' WHERE module_id = ? AND state IN ('active','compiled','superseded')",
                self.versions
            ),
            (module_id,),
        )
        .await
        .map_err(map_err)
    }
}
