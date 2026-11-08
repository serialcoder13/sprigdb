//! The catalog side of tenant bootstrap (chapter 08, "Bootstrap, DDL and
//! migrations"; build step 9).
//!
//! ```text
//! home DC controller, job tenant_bootstrap (idempotent, progress in sprig.bootstrap):
//!   pick a cluster (chapter 11, least busy without a hint); create shardmaps/<id>/0;
//!   CREATE DATABASE t<id>_sprig; run all migrations; insert internal keys;
//!   copy the ancestry and ancestors' settings (tree_settings reconcile);
//!   create the admin user (14) with backend grants (29); state creating → active
//! ```
//!
//! The directory, placement and job sides live in `sprig-controller`
//! (`tenants`); this module creates and drops the catalog schema and keeps
//! the progress record `sprig.bootstrap`. Every step is idempotent:
//! [`create_catalog`] is the migrator's `CREATE ... IF NOT EXISTS` run, and
//! the progress record is rewritten as each step finishes, so a re-run
//! after a crash continues where it stopped. Progress writes are
//! unversioned ("intermediate states commit without a version"): they
//! carry the catalog's committed version, so caches don't reload for them.

use std::time::SystemTime;

use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use sprig_common::ids::ClusterId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use crate::migrate::{MigrateOpts, MigrateReport, Migrator};
use crate::sql::{map_err, rfc3339};
use crate::store::{SettingScope, Store};

/// The internal key holding bootstrap progress.
pub const BOOTSTRAP_KEY: &str = "sprig.bootstrap";

/// How far a bootstrap got.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapStep {
    /// The schema exists and every migration ran.
    Migrated,
    /// The ancestry and ancestors' settings are copied.
    Ancestry,
    /// Other chapters' hooks ran (admin user, backend accounts).
    Hooks,
    /// Done; the directory entry becomes `active`.
    Done,
}

/// The value of `sprig.bootstrap`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapProgress {
    /// The last finished step.
    pub step: BootstrapStep,
    /// The cluster placement chose.
    pub cluster: ClusterId,
    /// Why: `requested` (`--cluster`) or `fewest_tenants` (interim
    /// placement until chapter 11's, M0.6).
    pub placement: String,
    /// The clusters considered, with their tenant counts.
    #[serde(default)]
    pub considered: Vec<(ClusterId, u64)>,
    /// When the step finished, RFC 3339 UTC (for people).
    pub at: String,
}

/// Creates `schema` on `pool`'s server (`utf8mb4_bin`) and runs every
/// pending migration of the core module and `migrator`'s other modules.
/// Safe to repeat and to run concurrently (the migrator serializes runs).
///
/// # Errors
/// As [`Migrator::run`].
pub async fn create_catalog(
    pool: &Pool,
    schema: &str,
    migrator: &Migrator,
    opts: &MigrateOpts,
) -> Result<MigrateReport> {
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    let r = migrator.run(&mut conn, schema, opts).await;
    drop(conn);
    r
}

/// Reads `sprig.bootstrap`; `None` before the first step finished or if
/// the catalog doesn't exist yet.
///
/// # Errors
/// 39070 or a backend error; `Internal` for a bad record.
pub async fn read_progress(pool: &Pool, store: &Store) -> Result<Option<BootstrapProgress>> {
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    let exists: Option<String> = conn
        .exec_first(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
            (store.schema(),),
        )
        .await
        .map_err(map_err)?;
    if exists.is_none() {
        return Ok(None);
    }
    let Some(v) = store
        .setting(&mut conn, SettingScope::Tenant, BOOTSTRAP_KEY)
        .await
        .ok()
        .flatten()
        .and_then(|r| r.value)
    else {
        return Ok(None);
    };
    serde_json::from_value(v)
        .map(Some)
        .map_err(|e| SprigError::internal(format!("bad {BOOTSTRAP_KEY}: {e}")))
}

/// Records `step` in `sprig.bootstrap`, unversioned.
///
/// # Errors
/// 39070 or a backend error; 39072 if the catalog isn't migrated.
#[allow(clippy::too_many_arguments)] // the record's fields
pub async fn write_progress(
    pool: &Pool,
    store: &Store,
    step: BootstrapStep,
    cluster: &ClusterId,
    placement: &str,
    considered: &[(ClusterId, u64)],
    at: SystemTime,
) -> Result<()> {
    let p = BootstrapProgress {
        step,
        cluster: cluster.clone(),
        placement: placement.to_owned(),
        considered: considered.to_vec(),
        at: rfc3339(at),
    };
    let value = serde_json::to_value(&p)
        .map_err(|e| SprigError::internal(format!("encoding {BOOTSTRAP_KEY}: {e}")))?;
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    let committed = store.versions(&mut conn).await?.catalog;
    store
        .put_internal(&mut conn, BOOTSTRAP_KEY, &value, committed, at)
        .await
}

/// Live (not dropped) databases in the catalog; 0 if the catalog doesn't
/// exist.
///
/// # Errors
/// 39070 or a backend error.
pub async fn live_databases(pool: &Pool, store: &Store) -> Result<u64> {
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    let tables: Option<String> = conn
        .exec_first(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
             AND TABLE_NAME = 'databases'",
            (store.schema(),),
        )
        .await
        .map_err(map_err)?;
    if tables.is_none() {
        return Ok(0);
    }
    let n: Option<u64> = conn
        .query_first(format!(
            "SELECT COUNT(*) FROM {}.`databases` WHERE state <> 'dropped'",
            quote(store.schema())
        ))
        .await
        .map_err(map_err)?;
    Ok(n.unwrap_or(0))
}

/// Drops the catalog schema of a tenant being deleted. Refuses while the
/// catalog still lists live databases: their data schemas must go first.
///
/// # Errors
/// 39072 while databases remain; 39070 or a backend error.
pub async fn drop_catalog(pool: &Pool, store: &Store) -> Result<()> {
    let n = live_databases(pool, store).await?;
    if n > 0 {
        return Err(SprigError::new(
            SprigCode::TenantNotReady,
            format!("the tenant still has {n} databases; drop them first"),
        ));
    }
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    conn.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(store.schema())))
        .await
        .map_err(map_err)
}
