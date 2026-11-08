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

/// The module's migrations.
pub static LOGIC_MIGRATIONS: &[Migration] = &[Migration {
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
}];

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

impl LogicStore {
    /// The tables in `schema` (`t<tenant>_sprig`).
    #[must_use]
    pub fn new(schema: &str) -> Self {
        let q = quote(schema);
        Self {
            modules: format!("{q}.logic_modules"),
            versions: format!("{q}.logic_versions"),
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
