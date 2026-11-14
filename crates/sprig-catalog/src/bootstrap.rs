//! The catalog side of tenant bootstrap.
//!
//! ```text
//! home DC controller, job tenant_bootstrap (idempotent, progress in sprig.bootstrap):
//!   pick a cluster (least busy without a hint); create shardmaps/<id>/0;
//!   CREATE DATABASE t<id>_sprig; run all migrations; insert internal keys;
//!   copy the ancestry and ancestors' settings (tree_settings reconcile);
//!   create the admin user with backend grants; state creating → active
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
    /// Other crates' hooks ran (admin user, backend accounts).
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
    /// Why: `requested` (`--cluster`) or `fewest_tenants` (the cluster
    /// with the fewest tenants).
    pub placement: String,
    /// The clusters considered, with their tenant counts.
    #[serde(default)]
    pub considered: Vec<(ClusterId, u64)>,
    /// When the step finished, RFC 3339 UTC (for people).
    pub at: String,
}

/// Who owns an existing catalog schema, as [`claim_catalog`] found it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogClaim {
    /// The schema didn't exist; this call created it with the owner mark.
    Created,
    /// The schema exists and carries this owner's mark (a re-run).
    Ours,
    /// The schema exists with another mark, or none: an earlier tenant's
    /// catalog under the same id. Its comment is given.
    Foreign(String),
}

/// A string literal for MariaDB, with `'` and `\` escaped.
fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// The comment of `schema` on `pool`'s server, which holds its owner mark;
/// `None` if the schema doesn't exist.
///
/// # Errors
/// 39070 or a backend error.
pub async fn catalog_owner(pool: &Pool, schema: &str) -> Result<Option<String>> {
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    let row: Option<Option<String>> = conn
        .exec_first(
            "SELECT SCHEMA_COMMENT FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
            (schema,),
        )
        .await
        .map_err(map_err)?;
    Ok(row.map(Option::unwrap_or_default))
}

/// Creates the catalog schema with `owner` as its comment, the mark that
/// proves whose it is, in the same statement, so no crash leaves an
/// unmarked catalog behind. An existing schema is never adopted unless it
/// carries the same mark: a tenant id is never reused, so a schema already
/// there for a new tenant is an earlier tenant's (left by a deletion that
/// didn't finish, or by a directory whose id counter was lost).
///
/// # Errors
/// 39070 or a backend error.
pub async fn claim_catalog(pool: &Pool, schema: &str, owner: &str) -> Result<CatalogClaim> {
    let judge = |found: String| {
        if found == owner {
            CatalogClaim::Ours
        } else {
            CatalogClaim::Foreign(found)
        }
    };
    if let Some(found) = catalog_owner(pool, schema).await? {
        return Ok(judge(found));
    }
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    let created = conn
        .query_drop(format!(
            "CREATE DATABASE {} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin COMMENT {}",
            quote(schema),
            literal(owner)
        ))
        .await;
    drop(conn);
    match created {
        Ok(()) => Ok(CatalogClaim::Created),
        // 1007: another run created it first; judge its mark.
        Err(mysql_async::Error::Server(e)) if e.code == 1007 => Ok(judge(
            catalog_owner(pool, schema).await?.unwrap_or_default(),
        )),
        Err(e) => Err(map_err(e)),
    }
}

/// The tenant id in a schema name shaped like a tenant's: the catalog
/// `<prefix><id><suffix>` (`t<id>_sprig`) or, when `suffix` starts with a
/// separator such as `_`, any `<prefix><id>_...` (the data schemas
/// `t<id>_d<n>_g<n>_s<n>`). `None` for other names.
#[must_use]
pub fn tenant_id_of(name: &str, prefix: &str, suffix: &str) -> Option<u64> {
    let rest = name.strip_prefix(prefix)?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    let (id, tail) = rest.split_at(digits);
    let shaped = tail == suffix
        || suffix
            .chars()
            .next()
            .is_some_and(|sep| !sep.is_ascii_alphanumeric() && tail.starts_with(sep));
    if id.is_empty() || !shaped {
        return None;
    }
    id.parse().ok()
}

/// The tenant ids of every schema on `pool`'s server shaped like a
/// tenant's ([`tenant_id_of`]): an earlier tenant's catalog or data
/// schemas, which a new tenant's id must stay above.
///
/// # Errors
/// 39070 or a backend error.
pub async fn tenant_schema_ids(pool: &Pool, prefix: &str, suffix: &str) -> Result<Vec<u64>> {
    let like = prefix
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    let mut conn = pool.get_conn().await.map_err(map_err)?;
    let names: Vec<String> = conn
        .exec(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE ?",
            (format!("{like}%"),),
        )
        .await
        .map_err(map_err)?;
    Ok(names
        .iter()
        .filter_map(|n| tenant_id_of(n, prefix, suffix))
        .collect())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_ids_come_from_schemas_shaped_like_a_tenants() {
        let id = |n: &str| tenant_id_of(n, "t", "_sprig");
        assert_eq!(id("t1_sprig"), Some(1));
        assert_eq!(id("t42_d1_g1_s1"), Some(42));
        assert_eq!(id("t7_anything"), Some(7));
        for other in ["t1", "t_sprig", "tx1_sprig", "test", "t1x_sprig", "sprig"] {
            assert_eq!(id(other), None, "{other}");
        }
        // A naming without a separator after the id: only its catalogs.
        let test = |n: &str| tenant_id_of(n, "__sprig_test_tenant_9_ab_", "");
        assert_eq!(test("__sprig_test_tenant_9_ab_3"), Some(3));
        assert_eq!(test("__sprig_test_tenant_9_ab_3_x"), None);
        assert_eq!(test("__sprig_test_tenant_9_ab_"), None);
        assert_eq!(literal("it's a \\ test"), "'it''s a \\\\ test'");
    }
}
