//! The backend account a tenant's sessions use, interim (chapter 14,
//! "Backend target, interim"): chapter 29's per-user backend accounts
//! `t<tenant>_u<user_id>` arrive in M0.5. Until then `[gateway]
//! backends_file` names one backend account per tenant, or a default for
//! every tenant. The cluster is the tenant's catalog cluster, from its
//! shard map (R0 keeps every database there, decision N4), unless the
//! entry names one.
//!
//! ```toml
//! [default]                      # every tenant without its own entry
//! backend_user = "sprig_app"
//! backend_credentials = "file:gateway-backends.toml#backend_app"
//!
//! [tenants.acme]                 # by tenant name
//! backend_user = "acme_app"
//! backend_credentials = "env:ACME_BACKEND"
//! cluster = "dc-a-c1"            # optional
//!
//! [backend_app]                  # a credential table, as file: references use
//! user = "sprig_app"
//! password = "..."
//! ```
//!
//! A relative `file:` path is relative to the file's directory. The file is
//! read at start-up. A verified login of a tenant with neither an entry
//! nor a default fails with 39998, as chapter 14 says.
//!
//! **Per-user backend accounts** (chapter 29, M0.5). With a `[per_user]`
//! table, every tenant without its own `[tenants.<name>]` entry runs each
//! user's statements as that user's own MariaDB account,
//! `t<tenant>_u<user>`, whose password derives from the secret
//! ([`BackendKey`]); the gateway creates and grants the accounts itself
//! (`super::authz`). `[default]` then serves nothing.
//!
//! ```toml
//! [per_user]
//! secret = "file:gateway-backends.toml#backend_secret"   # its `password`
//! tls = "skip_verify"                                    # verify (default)
//! ca_file = "/run/sprig/tls/ca.pem"   # optional: the CA `verify` checks against
//! ```

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use sprig_auth::account::TenantInfo;
use sprig_auth::authz::backend::BackendKey;
use sprig_auth::{AuthUser, BackendResolver, BackendTarget};
use sprig_common::ids::{ClusterId, TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_provision::credentials::{Credential, SecretRef, SecretSource, TlsMode};

use super::locator::GatewayLocator;

/// One entry of the file.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Spec {
    backend_user: String,
    backend_credentials: String,
    #[serde(default)]
    cluster: Option<String>,
}

/// An entry, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    user: String,
    credentials: SecretRef,
    cluster: Option<ClusterId>,
}

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, m.into())
}

impl Spec {
    fn entry(&self, base: &Path, what: &str) -> Result<Entry> {
        if self.backend_user.is_empty() {
            return Err(invalid(format!("{what}: backend_user is empty")));
        }
        let mut credentials: SecretRef = self.backend_credentials.parse()?;
        if let SecretSource::File(p) = &credentials.source
            && p.is_relative()
        {
            credentials.source = SecretSource::File(base.join(p));
        }
        let cluster = self
            .cluster
            .as_deref()
            .map(ClusterId::new)
            .transpose()
            .map_err(|e| invalid(format!("{what}: {e}")))?;
        Ok(Entry {
            user: self.backend_user.clone(),
            credentials,
            cluster,
        })
    }
}

/// `[per_user]`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PerUserSpec {
    secret: String,
    #[serde(default)]
    tls: TlsMode,
    /// The CA `verify` checks the servers against (a private CA, as the
    /// Compose-on-VM bundle makes); unset: the built-in roots.
    #[serde(default)]
    ca_file: Option<std::path::PathBuf>,
}

/// Per-user backend accounts: the key their passwords derive from, and how
/// the gateway checks the servers' certificates.
#[derive(Debug, Clone)]
pub struct PerUser {
    /// The derivation key.
    pub key: BackendKey,
    /// TLS checking of backend connections.
    pub tls: TlsMode,
    /// The CA `tls = "verify"` checks against; `None`: the built-in roots.
    pub ca_file: Option<std::path::PathBuf>,
}

impl PerUser {
    /// The `[per_user]` table; relative paths (the secret's `file:`
    /// reference, `ca_file`) resolve against `base`.
    ///
    /// # Errors
    /// 39998 for an unknown field, a bad `tls`, or a secret that can't be
    /// resolved.
    pub fn from_toml(v: &toml::Value, base: &Path) -> Result<Self> {
        let p: PerUserSpec = v
            .clone()
            .try_into()
            .map_err(|e| invalid(format!("backends file, [per_user]: {e}")))?;
        let mut r: SecretRef = p.secret.parse()?;
        if let SecretSource::File(f) = &r.source
            && f.is_relative()
        {
            r.source = SecretSource::File(base.join(f));
        }
        let c = r.resolve("sprig_backend_secret")?;
        Ok(Self {
            key: BackendKey::new(c.password().as_bytes())?,
            tls: p.tls,
            ca_file: p
                .ca_file
                .map(|f| if f.is_relative() { base.join(f) } else { f }),
        })
    }

    /// The target of `user` of `tenant` on `cluster`.
    #[must_use]
    pub fn target(&self, cluster: ClusterId, tenant: TenantId, user: UserId) -> BackendTarget {
        let acct = sprig_auth::authz::backend::account(tenant, user);
        let password = self.key.password(&acct);
        BackendTarget {
            cluster,
            credentials: SecretRef::resolved({
                let mut c = Credential::new(acct.clone(), password.as_str(), self.tls);
                c.ca_file.clone_from(&self.ca_file);
                c
            }),
            user: acct,
        }
    }
}

/// Backend targets of catalog logins (see the module docs).
pub struct TenantBackends {
    default: Option<Entry>,
    tenants: HashMap<String, Entry>,
    per_user: Option<PerUser>,
    locator: Arc<GatewayLocator>,
}

impl std::fmt::Debug for TenantBackends {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantBackends")
            .field("default", &self.default.as_ref().map(|e| &e.user))
            .field("tenants", &self.tenants.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl TenantBackends {
    /// Nothing configured: every catalog login fails with 39998.
    #[must_use]
    pub fn none(locator: Arc<GatewayLocator>) -> Self {
        Self {
            default: None,
            tenants: HashMap::new(),
            per_user: None,
            locator,
        }
    }

    /// The per-user accounts' settings, when `[per_user]` is configured.
    #[must_use]
    pub fn per_user(&self) -> Option<PerUser> {
        self.per_user.clone()
    }

    /// Parses the file's text; relative `file:` references resolve
    /// against `base`.
    ///
    /// # Errors
    /// 39998 for bad TOML, an unknown field in an entry, an empty user, a
    /// bad credential reference or cluster id.
    pub fn parse(text: &str, base: &Path, locator: Arc<GatewayLocator>) -> Result<Self> {
        let doc: toml::Table = text
            .parse()
            .map_err(|e| invalid(format!("backends file: {e}")))?;
        let spec = |v: &toml::Value, what: &str| -> Result<Entry> {
            let s: Spec = v
                .clone()
                .try_into()
                .map_err(|e| invalid(format!("backends file, {what}: {e}")))?;
            s.entry(base, what)
        };
        let default = doc
            .get("default")
            .map(|v| spec(v, "[default]"))
            .transpose()?;
        let mut tenants = HashMap::new();
        if let Some(t) = doc.get("tenants") {
            let t = t
                .as_table()
                .ok_or_else(|| invalid("backends file: [tenants] must be a table"))?;
            for (name, v) in t {
                tenants.insert(name.clone(), spec(v, &format!("[tenants.{name}]"))?);
            }
        }
        let per_user = match doc.get("per_user") {
            None => None,
            Some(v) => Some(PerUser::from_toml(v, base)?),
        };
        Ok(Self {
            default,
            tenants,
            per_user,
            locator,
        })
    }

    /// Reads `path` (see [`Self::parse`]).
    ///
    /// # Errors
    /// 39998 if the file can't be read or parsed.
    pub fn open(path: &Path, locator: Arc<GatewayLocator>) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| invalid(format!("backends file {}: {e}", path.display())))?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        Self::parse(&text, base, locator)
    }
}

impl BackendResolver for TenantBackends {
    fn backend_for(
        &self,
        tenant: &TenantInfo,
        user_id: sprig_common::ids::UserId,
        cluster: &ClusterId,
    ) -> Option<Result<BackendTarget>> {
        if let Some(p) = &self.per_user
            && !self.tenants.contains_key(&tenant.name)
        {
            return Some(Ok(p.target(cluster.clone(), tenant.id, user_id)));
        }
        let e = self.tenants.get(&tenant.name).or(self.default.as_ref())?;
        Some(Ok(BackendTarget {
            cluster: e.cluster.clone().unwrap_or_else(|| cluster.clone()),
            user: e.user.clone(),
            credentials: e.credentials.clone(),
        }))
    }

    fn backend(&self, tenant: &TenantInfo, user: &AuthUser) -> Result<BackendTarget> {
        if let Some(p) = &self.per_user
            && !self.tenants.contains_key(&tenant.name)
        {
            let cluster = self.locator.cached_cluster(tenant.id).ok_or_else(|| {
                SprigError::new(
                    SprigCode::TenantNotReady,
                    format!("tenant {}'s catalog cluster isn't known yet", tenant.name),
                )
            })?;
            return Ok(p.target(cluster, tenant.id, user.row.user_id));
        }
        let e = self
            .tenants
            .get(&tenant.name)
            .or(self.default.as_ref())
            .ok_or_else(|| {
                invalid(format!(
                    "no backend account configured for tenant {} ([gateway] backends_file)",
                    tenant.name
                ))
            })?;
        let cluster = e
            .cluster
            .clone()
            .or_else(|| self.locator.placement(tenant.id).map(|p| p.cluster))
            .or_else(|| self.locator.cached_cluster(tenant.id))
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::TenantNotReady,
                    format!("tenant {}'s catalog cluster isn't known yet", tenant.name),
                )
            })?;
        Ok(BackendTarget {
            cluster,
            user: e.user.clone(),
            credentials: e.credentials.clone(),
        })
    }
}

#[cfg(test)]
mod tests;
