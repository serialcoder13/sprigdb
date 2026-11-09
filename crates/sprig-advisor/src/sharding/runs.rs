//! `advisor_runs` (chapter 16, "Data structures"; build step 7): one row
//! per advisor run in the tenant's catalog, its own migration module
//! (`advisor_runs`), migrated on first use like chapter 15's `sharding`.

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use sprig_catalog::migrate::{Migration, Module, Phase};
use sprig_common::{Result, SprigCode, SprigError};

/// The module name.
pub const MODULE: &str = "advisor_runs";

/// `advisor_runs`, as chapter 16 gives it.
pub const DDL_ADVISOR_RUNS: &str = "CREATE TABLE IF NOT EXISTS advisor_runs (
  run_id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  cause            ENUM('first_split','min_shards','periodic','manual','new_object') NOT NULL,
  mode             ENUM('on','recommend','off') NOT NULL,
  status           ENUM('queued','collecting','searching','decided','applying',
                        'applied','superseded','failed') NOT NULL,
  window_start     DATETIME(6) NOT NULL,
  window_end       DATETIME(6) NOT NULL,
  schema_version   BIGINT UNSIGNED NOT NULL,
  units_sampled    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  units_unresolved BIGINT UNSIGNED NOT NULL DEFAULT 0,
  current_score    DOUBLE NULL,
  best_score       DOUBLE NULL,
  runner_up_score  DOUBLE NULL,
  confidence       DOUBLE NULL,
  verdict          ENUM('apply','recommend','wait','no_change') NULL,
  design           JSON NULL,
  alternatives     JSON NULL,
  evidence         JSON NULL,
  plan             JSON NULL,
  decision_id      BIGINT UNSIGNED NULL,
  approved_by      VARCHAR(128) NULL,
  error            TEXT NULL,
  created_at       DATETIME(6) NOT NULL,
  finished_at      DATETIME(6) NULL,
  PRIMARY KEY (run_id),
  KEY by_status (status, created_at)
) ENGINE=InnoDB";

/// The module's migrations.
pub static MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "advisor_runs (chapter 16)",
    statements: &[DDL_ADVISOR_RUNS],
}];

/// The module, for `sprig_catalog::Migrator::new`.
pub const ADVISOR_RUNS_MODULE: Module = Module {
    name: MODULE,
    migrations: MIGRATIONS,
};

fn db_err(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, format!("{what}: {e}"))
}

/// Applies the module to catalog `schema` (idempotent).
///
/// # Errors
/// The migrator's.
pub async fn migrate(c: &mut Conn, schema: &str) -> Result<()> {
    sprig_catalog::Migrator::new(&[ADVISOR_RUNS_MODULE])?
        .run(c, schema, &sprig_catalog::migrate::MigrateOpts::default())
        .await?;
    Ok(())
}

/// A finished search, as recorded.
#[derive(Debug, Clone, PartialEq)]
pub struct RunRecord {
    /// `first_split`, ...
    pub cause: &'static str,
    /// `on`, `recommend` or `off`.
    pub mode: &'static str,
    /// Window bounds, microseconds since the epoch.
    pub window_us: (i64, i64),
    /// The catalog version read.
    pub schema_version: u64,
    /// Units sampled.
    pub units: u64,
    /// The best design's score.
    pub best: f64,
    /// The runner-up's.
    pub runner_up: Option<f64>,
    /// Confidence.
    pub confidence: f64,
    /// `apply`, `recommend`, `wait` or `no_change`.
    pub verdict: &'static str,
    /// The design.
    pub design: serde_json::Value,
    /// Up to three others.
    pub alternatives: serde_json::Value,
    /// Evidence.
    pub evidence: serde_json::Value,
}

/// Inserts a decided run into `schema`'s `advisor_runs`; returns its id.
///
/// # Errors
/// MariaDB's.
pub async fn insert(c: &mut Conn, schema: &str, r: &RunRecord) -> Result<u64> {
    c.exec_drop(
        format!(
            "INSERT INTO `{schema}`.advisor_runs (cause, mode, status, window_start, window_end, \
             schema_version, units_sampled, best_score, runner_up_score, confidence, verdict, \
             design, alternatives, evidence, created_at) VALUES (?, ?, 'decided', \
             FROM_UNIXTIME(? / 1e6), FROM_UNIXTIME(? / 1e6), ?, ?, ?, ?, ?, ?, ?, ?, ?, \
             UTC_TIMESTAMP(6))"
        ),
        vec![
            mysql_async::Value::from(r.cause),
            r.mode.into(),
            r.window_us.0.into(),
            r.window_us.1.into(),
            r.schema_version.into(),
            r.units.into(),
            r.best.into(),
            r.runner_up.into(),
            r.confidence.into(),
            r.verdict.into(),
            r.design.to_string().into(),
            r.alternatives.to_string().into(),
            r.evidence.to_string().into(),
        ],
    )
    .await
    .map_err(|e| db_err("insert an advisor run", &e))?;
    let id: Option<u64> = c
        .query_first("SELECT LAST_INSERT_ID()")
        .await
        .map_err(|e| db_err("run id", &e))?;
    id.ok_or_else(|| SprigError::internal("no run id"))
}

/// Moves a run to `status` (with its plan and error, when given).
///
/// # Errors
/// MariaDB's.
pub async fn set_status(
    c: &mut Conn,
    schema: &str,
    run: u64,
    status: &str,
    plan: Option<&serde_json::Value>,
    error: Option<&str>,
) -> Result<()> {
    let finished = matches!(status, "applied" | "failed" | "superseded");
    c.exec_drop(
        format!(
            "UPDATE `{schema}`.advisor_runs SET status = ?, plan = COALESCE(?, plan), \
             error = COALESCE(?, error), finished_at = IF(?, UTC_TIMESTAMP(6), finished_at) \
             WHERE run_id = ?"
        ),
        (status, plan.map(ToString::to_string), error, finished, run),
    )
    .await
    .map_err(|e| db_err("update an advisor run", &e))
}

/// Sets the decision a run wrote.
///
/// # Errors
/// MariaDB's.
pub async fn set_decision(c: &mut Conn, schema: &str, run: u64, decision: u64) -> Result<()> {
    c.exec_drop(
        format!("UPDATE `{schema}`.advisor_runs SET decision_id = ? WHERE run_id = ?"),
        (decision, run),
    )
    .await
    .map_err(|e| db_err("update an advisor run", &e))
}

/// The newest run's id, status and verdict.
///
/// # Errors
/// MariaDB's.
pub async fn latest(c: &mut Conn, schema: &str) -> Result<Option<(u64, String, Option<String>)>> {
    c.query_first(format!(
        "SELECT run_id, status, verdict FROM `{schema}`.advisor_runs ORDER BY run_id DESC LIMIT 1"
    ))
    .await
    .map_err(|e| db_err("latest advisor run", &e))
}
