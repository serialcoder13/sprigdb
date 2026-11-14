//! The name pairs stored code reads: the rows of
//! `__sprig_sys.__sprig_names` on each cluster, which the lookups
//! [`sprig_sql::names::stored::substitute`] puts into views and stored
//! programs turn backend names into logical ones with.
//!
//! [`sync`] runs where the gateway already brings a tenant's backend
//! accounts to its snapshot (`AuthzServices::ensure`) and before each
//! `CREATE` of stored code: it creates the table and its `PUBLIC` view once
//! per cluster and process, then writes the tenant's rows when they differ
//! from what this process last wrote (a hash per tenant and cluster):
//!
//! | Row | `name` | `logical` | `account` |
//! |---|---|---|---|
//! | each live user | `t42_u7` | `alice` | `alice@%` |
//! | each shard schema of each live database | `t42_d1_g1_s1` | `shop` | `NULL` |
//!
//! Rows of the tenant that are no longer wanted are deleted. Every
//! statement is idempotent, so gateways may sync the same tenant at once.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{LazyLock, Mutex, PoisonError};

use mysql_async::prelude::Queryable;
use sprig_auth::TenantAuth;
use sprig_catalog::TenantCatalog;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::names::AccountName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_sql::names::stored::{NAMES_TABLE, NameRow, SYS_SCHEMA, setup_sql};

use super::TenantServices;

/// Clusters whose lookup objects this process has created.
static READY: LazyLock<Mutex<HashSet<ClusterId>>> = LazyLock::new(Mutex::default);
/// The hash of the rows last written, per tenant and cluster.
static APPLIED: LazyLock<Mutex<HashMap<(TenantId, ClusterId), u64>>> =
    LazyLock::new(Mutex::default);

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Whether this process has created the lookup objects on `cluster`, so
/// statements may name them (the `information_schema` mapping).
#[must_use]
pub fn ready(cluster: &ClusterId) -> bool {
    lock(&READY).contains(cluster)
}

/// The rows `cat`'s tenant wants (see the module docs).
#[must_use]
pub fn wanted(cat: &TenantCatalog, auth: Option<&TenantAuth>) -> BTreeSet<NameRow> {
    let mut rows = BTreeSet::new();
    if let Some(auth) = auth {
        for u in auth.users() {
            let r = &u.row;
            rows.insert(NameRow {
                name: AccountName::User {
                    tenant: cat.tenant,
                    user: r.user_id,
                }
                .to_string(),
                logical: r.name.clone(),
                account: Some(format!("{}@{}", r.name, r.host)),
            });
        }
    }
    for d in cat
        .databases
        .iter()
        .filter(|d| d.state == sprig_catalog::store::DbState::Active)
    {
        for s in sprig_catalog::ddl::shard_schemas(cat.tenant, d) {
            rows.insert(NameRow {
                name: s.to_string(),
                logical: d.name.clone(),
                account: None,
            });
        }
    }
    rows
}

fn hash_of(rows: &BTreeSet<NameRow>) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    rows.hash(&mut h);
    h.finish()
}

/// Brings `cat`'s tenant's rows on `cluster` to [`wanted`]; a no-op when
/// this process already wrote the same rows there.
///
/// # Errors
/// The cluster's (no `sprig_ctl` pool, a failed statement).
pub async fn sync(svc: &TenantServices, cat: &TenantCatalog, cluster: &ClusterId) -> Result<()> {
    let want = wanted(cat, TenantAuth::of(cat));
    let h = hash_of(&want);
    let key = (cat.tenant, cluster.clone());
    if lock(&APPLIED).get(&key) == Some(&h) {
        return Ok(());
    }
    let pool = svc.locator.ctl_pool(cluster).await?;
    let mut c = pool.get_conn().await.map_err(|e| {
        SprigError::new(
            SprigCode::BackendUnavailable,
            format!("sprig_ctl on {cluster}: {e}"),
        )
    })?;
    let fail = |e: mysql_async::Error| {
        SprigError::internal(format!("stored-code names on {cluster}: {e}"))
    };
    if !lock(&READY).contains(cluster) {
        for s in setup_sql() {
            c.query_drop(s).await.map_err(fail)?;
        }
        lock(&READY).insert(cluster.clone());
    }
    let prefix = format!("t{}\\_%", cat.tenant);
    let have: Vec<(String, String, Option<String>)> = c
        .exec(
            format!(
                "SELECT `name`, `logical`, `account` FROM `{SYS_SCHEMA}`.`{NAMES_TABLE}` WHERE `name` LIKE ?"
            ),
            (prefix,),
        )
        .await
        .map_err(fail)?;
    let have: BTreeSet<NameRow> = have
        .into_iter()
        .map(|(name, logical, account)| NameRow {
            name,
            logical,
            account,
        })
        .collect();
    let wanted_names: HashSet<&str> = want.iter().map(|r| r.name.as_str()).collect();
    for gone in have
        .iter()
        .filter(|r| !wanted_names.contains(r.name.as_str()))
    {
        c.exec_drop(
            format!("DELETE FROM `{SYS_SCHEMA}`.`{NAMES_TABLE}` WHERE `name` = ?"),
            (gone.name.as_str(),),
        )
        .await
        .map_err(fail)?;
    }
    for r in want.difference(&have) {
        c.exec_drop(
            format!(
                "INSERT INTO `{SYS_SCHEMA}`.`{NAMES_TABLE}` (`name`, `logical`, `account`) \
                 VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `logical` = VALUES(`logical`), \
                 `account` = VALUES(`account`)"
            ),
            (r.name.as_str(), r.logical.as_str(), r.account.as_deref()),
        )
        .await
        .map_err(fail)?;
    }
    lock(&APPLIED).insert(key, h);
    Ok(())
}

/// [`sync`], logging a failure instead of returning it: a session goes on
/// without the rows (stored code then reads `NULL` for the names).
pub async fn sync_logged(svc: &TenantServices, cat: &TenantCatalog, cluster: &ClusterId) {
    if let Err(e) = sync(svc, cat, cluster).await {
        tracing::warn!(tenant = %cat.tenant, %cluster, error = %e, "stored-code names not synced");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_follow_the_rows() {
        let mut a = BTreeSet::new();
        a.insert(NameRow {
            name: "t1_u1".into(),
            logical: "app".into(),
            account: Some("app@%".into()),
        });
        let mut b = a.clone();
        assert_eq!(hash_of(&a), hash_of(&b));
        b.insert(NameRow {
            name: "t1_d1_g1_s1".into(),
            logical: "shop".into(),
            account: None,
        });
        assert_ne!(hash_of(&a), hash_of(&b));
    }
}
