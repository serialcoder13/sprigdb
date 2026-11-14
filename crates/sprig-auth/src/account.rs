//! Tenant resolution and account matching.
//!
//! ```text
//! resolve(login_name, sni):
//!   if sni ends with "." + s for some s in listener.tenant_sni_suffixes:
//!      tenant = the label before s; user = login_name, taken whole
//!   else: split login_name at the first ".": tenant = left, user = right
//!   tenant must be in tenant-names/ and able to log in; otherwise the decoy
//! ```
//!
//! [`parse_login`] does the first part without I/O; a [`TenantDirectory`]
//! answers the second. Neither says why a login has no tenant: every
//! failure is [`None`], and the login runs the decoy exchange, so a probe
//! can't tell a missing tenant from a wrong password.
//!
//! Choices made here:
//! - The SNI prefix must be exactly one label that is a valid tenant name;
//!   `a.acme.<suffix>` names no tenant. SNI is compared in lower case (DNS
//!   names are case-insensitive); a dotted login name is not (`ACME.alice`
//!   names no tenant), since MySQL user names are case-sensitive.
//! - The persona's user-name limit (32 on 3306, 80 on 3307) applies to the
//!   user part, so `acme.alice` and `alice` on a tenant endpoint have the
//!   same limit; the whole login name is capped at 255 characters.
//! - A tenant can take logins while `active`, `migrating` or
//!   `failing_over` (during a move, logins use cached
//!   verifiers); `creating`, `fenced`, `deleting` and unknown states name
//!   no tenant.
//! - Suspension (the tenant or any ancestor) is reported with 39303 only
//!   after the credentials verified, so the status doesn't reveal the
//!   tenant to someone without a password.
//!
//! Connection attributes never name a tenant (attributes are
//! hints, never a security control).

pub mod host;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde::Deserialize;
use sprig_common::Result;
use sprig_common::ids::{DcId, TenantId};
use sprig_meta::{Consistency, MetaStore, Record, Versioned};

use crate::store::LoginForm;

pub use host::HostPattern;

/// The longest login name, in characters.
pub const MAX_LOGIN_NAME: usize = 255;

/// Whether `s` is a tenant name: the one rule of
/// [`sprig_common::names::is_tenant_name`], which creating a
/// tenant checks too, so every tenant that can be created can be logged in
/// to.
#[must_use]
pub fn valid_tenant_name(s: &str) -> bool {
    sprig_common::names::is_tenant_name(s)
}

/// A login name split into tenant and user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLogin {
    /// The tenant's name.
    pub tenant: String,
    /// The user name within the tenant.
    pub user: String,
    /// How the name was written: `Qualified` (`acme.alice`, shared
    /// endpoints) or `Plain` (`alice`, tenant endpoints by SNI).
    pub form: LoginForm,
}

/// Splits a login: the tenant from SNI
/// on tenant endpoints (`sni` ends with `.` + one of `sni_suffixes`; the
/// user is the whole login name), else from a dotted login name. `None`
/// when no tenant can be named: the caller runs the decoy.
///
/// `max_user` is the persona's user-name limit in characters.
#[must_use]
pub fn parse_login(
    login_name: &str,
    sni: Option<&str>,
    sni_suffixes: &[String],
    max_user: usize,
) -> Option<ParsedLogin> {
    if login_name.is_empty() || login_name.chars().count() > MAX_LOGIN_NAME {
        return None;
    }
    let ok_user = |u: &str| !u.is_empty() && u.chars().count() <= max_user;
    if let Some(sni) = sni {
        let sni = sni.trim_end_matches('.').to_ascii_lowercase();
        for suffix in sni_suffixes {
            let suffix = suffix.trim_matches('.').to_ascii_lowercase();
            if suffix.is_empty() {
                continue;
            }
            let Some(label) = sni
                .strip_suffix(suffix.as_str())
                .and_then(|p| p.strip_suffix('.'))
            else {
                continue;
            };
            // SNI wins over a dotted name: on a tenant endpoint the whole
            // login name is the user.
            return (valid_tenant_name(label) && ok_user(login_name)).then(|| ParsedLogin {
                tenant: label.to_owned(),
                user: login_name.to_owned(),
                form: LoginForm::Plain,
            });
        }
    }
    let (tenant, user) = login_name.split_once('.')?;
    (valid_tenant_name(tenant) && ok_user(user)).then(|| ParsedLogin {
        tenant: tenant.to_owned(),
        user: user.to_owned(),
        form: LoginForm::Qualified,
    })
}

/// A tenant as logins see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantInfo {
    /// The id.
    pub id: TenantId,
    /// The name.
    pub name: String,
    /// The directory state allows logins (`active`, `migrating`,
    /// `failing_over`).
    pub can_log_in: bool,
    /// The tenant or a tenant on its directory path is suspended (39303).
    pub suspended: bool,
}

/// A tenant's residency as logins see it (credential residency): the
/// directory entry's `residency` block.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TenantResidency {
    /// The allowed DCs; empty: none (a block without a readable set fails
    /// closed).
    pub allowed_dcs: Vec<DcId>,
    /// `residency.deny_remote_access`.
    pub deny_remote_access: bool,
}

impl TenantResidency {
    /// The residency of a directory entry's `residency` value; `None` for
    /// none.
    #[must_use]
    pub fn of(v: &serde_json::Value) -> Option<Self> {
        if v.is_null() {
            return None;
        }
        let allowed_dcs = v
            .get("allowed_dcs")
            .and_then(serde_json::Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(serde_json::Value::as_str)
                    .filter_map(|d| DcId::new(d).ok())
                    .collect()
            })
            .unwrap_or_default();
        Some(Self {
            allowed_dcs,
            deny_remote_access: v
                .get("deny_remote_access")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        })
    }
}

/// Finds tenants by name: `tenant-names/<name>` and `tenants/<id>` in the
/// global tier.
#[async_trait::async_trait]
pub trait TenantDirectory: Send + Sync + 'static {
    /// The tenant named `name`, or `None` if there is none.
    ///
    /// # Errors
    /// The global tier's error if it can't be read; logins then fail with
    /// 39301.
    async fn lookup(&self, name: &str) -> Result<Option<TenantInfo>>;

    /// The residency of the tenant named `name`; `None` for a
    /// tenant without one, or no such tenant. Directories that don't know
    /// residency have none.
    ///
    /// # Errors
    /// The global tier's error if it can't be read.
    async fn residency(&self, name: &str) -> Result<Option<TenantResidency>> {
        let _ = name;
        Ok(None)
    }
}

/// A fixed set of tenants: tests and single-tenant development.
#[derive(Debug, Default)]
pub struct StaticTenants(
    HashMap<String, TenantInfo>,
    Mutex<HashMap<String, TenantResidency>>,
);

impl StaticTenants {
    /// The tenants, keyed by name.
    #[must_use]
    pub fn new(tenants: impl IntoIterator<Item = TenantInfo>) -> Self {
        Self(
            tenants.into_iter().map(|t| (t.name.clone(), t)).collect(),
            Mutex::new(HashMap::new()),
        )
    }

    /// Sets (or with `None` clears) the residency of tenant `name`.
    pub fn set_residency(&self, name: &str, r: Option<TenantResidency>) {
        let mut m = self.1.lock().unwrap_or_else(PoisonError::into_inner);
        match r {
            Some(r) => m.insert(name.to_owned(), r),
            None => m.remove(name),
        };
    }

    /// One active tenant.
    #[must_use]
    pub fn one(id: TenantId, name: &str) -> Self {
        Self::new([TenantInfo {
            id,
            name: name.to_owned(),
            can_log_in: true,
            suspended: false,
        }])
    }
}

#[async_trait::async_trait]
impl TenantDirectory for StaticTenants {
    async fn lookup(&self, name: &str) -> Result<Option<TenantInfo>> {
        Ok(self.0.get(name).cloned())
    }

    async fn residency(&self, name: &str) -> Result<Option<TenantResidency>> {
        Ok(self
            .1
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned())
    }
}

/// `tenant-names/<name>`: the tenant's id.
#[derive(Debug, Deserialize, serde::Serialize)]
struct NameRecord {
    tenant_id: u64,
}

impl Versioned for NameRecord {
    const V: u32 = 1;
}

/// The fields of `tenants/<id>` logins read.
#[derive(Debug, Deserialize, serde::Serialize)]
struct EntryRecord {
    #[serde(default)]
    name: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    suspended: bool,
    /// Tenant ids root first, self included: `"/1/42/77/"`.
    #[serde(default)]
    path: String,
    /// The residency block, read leniently.
    #[serde(default)]
    residency: serde_json::Value,
}

impl Versioned for EntryRecord {
    const V: u32 = 1;
}

/// [`TenantDirectory`] over the global tier's `tenant-names/` and
/// `tenants/` keys, with a short cache (default one second) so a burst of
/// logins reads each entry once. Reads are serializable (any etcd member):
/// a suspension reaches logins within the cache time plus etcd's
/// replication lag.
///
/// `tenant-names/<name>` holds `{"v": 1, "tenant_id": <id>}` (a bare
/// decimal id is accepted too). Suspension is the entry's `suspended`
/// flag, or any ancestor's on its `path`.
pub struct MetaTenants {
    store: Arc<dyn MetaStore>,
    ttl: Duration,
    cache: Mutex<HashMap<String, (Instant, Option<Found>)>>,
}

/// A cached lookup.
type Found = (TenantInfo, Option<TenantResidency>);

impl std::fmt::Debug for MetaTenants {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaTenants")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

/// The cache's default lifetime.
pub const META_TENANTS_TTL: Duration = Duration::from_secs(1);

/// Most cached names, so probes for random names can't grow it without
/// bound.
const META_TENANTS_MAX: usize = 100_000;

impl MetaTenants {
    /// Reads `store` (the global tier), caching entries `ttl`.
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>, ttl: Duration) -> Self {
        Self {
            store,
            ttl,
            cache: Mutex::new(HashMap::new()),
        }
    }

    async fn entry(&self, id: u64) -> Result<Option<EntryRecord>> {
        let key = self.store.tier().key(&format!("tenants/{id}"))?;
        match self.store.get(&key, Consistency::Serializable).await? {
            Some(kv) => Ok(Some(Record::<EntryRecord>::decode(&kv.value)?.body)),
            None => Ok(None),
        }
    }

    async fn read(&self, name: &str) -> Result<Option<Found>> {
        let key = self.store.tier().key(&format!("tenant-names/{name}"))?;
        let Some(kv) = self.store.get(&key, Consistency::Serializable).await? else {
            return Ok(None);
        };
        let id = match std::str::from_utf8(&kv.value)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
        {
            Some(id) => id,
            None => Record::<NameRecord>::decode(&kv.value)?.body.tenant_id,
        };
        let Some(e) = self.entry(id).await? else {
            return Ok(None);
        };
        let mut suspended = e.suspended;
        for anc in e
            .path
            .split('/')
            .filter_map(|p| p.parse::<u64>().ok())
            .filter(|&a| a != id)
        {
            if suspended {
                break;
            }
            suspended = self.entry(anc).await?.is_some_and(|a| a.suspended);
        }
        let residency = TenantResidency::of(&e.residency);
        Ok(Some((
            TenantInfo {
                id: TenantId(id),
                name: if e.name.is_empty() {
                    name.to_owned()
                } else {
                    e.name
                },
                can_log_in: matches!(e.state.as_str(), "active" | "migrating" | "failing_over"),
                suspended,
            },
            residency,
        )))
    }

    async fn found(&self, name: &str) -> Result<Option<Found>> {
        let now = Instant::now();
        if let Some((at, info)) = self
            .cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            && now.saturating_duration_since(*at) < self.ttl
        {
            return Ok(info.clone());
        }
        let info = self.read(name).await?;
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        if cache.len() >= META_TENANTS_MAX {
            let ttl = self.ttl;
            cache.retain(|_, (at, _)| now.saturating_duration_since(*at) < ttl);
            if cache.len() >= META_TENANTS_MAX {
                cache.clear();
            }
        }
        cache.insert(name.to_owned(), (now, info.clone()));
        Ok(info)
    }
}

#[async_trait::async_trait]
impl TenantDirectory for MetaTenants {
    async fn lookup(&self, name: &str) -> Result<Option<TenantInfo>> {
        Ok(self.found(name).await?.map(|f| f.0))
    }

    async fn residency(&self, name: &str) -> Result<Option<TenantResidency>> {
        Ok(self.found(name).await?.and_then(|f| f.1))
    }
}

#[cfg(test)]
mod tests;
