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

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use sprig_auth::account::TenantInfo;
use sprig_auth::{AuthUser, BackendResolver, BackendTarget};
use sprig_common::ids::ClusterId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_provision::credentials::{SecretRef, SecretSource};

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

/// Backend targets of catalog logins (see the module docs).
pub struct TenantBackends {
    default: Option<Entry>,
    tenants: HashMap<String, Entry>,
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
            locator,
        }
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
        Ok(Self {
            default,
            tenants,
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
    fn backend(&self, tenant: &TenantInfo, _user: &AuthUser) -> Result<BackendTarget> {
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
