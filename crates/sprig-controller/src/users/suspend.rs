//! Tenant suspension through the Admin API (chapter 14, "Suspension";
//! chapter 23's directory flag; chapter 29's `ALTER TENANT c SUSPEND |
//! RESUME`): `POST /admin/v1/tenants/{name}:suspend {"reason": ...}` and
//! `:resume`, which `sprigctl tenant suspend` and `resume` call.
//!
//! ```text
//! suspend(t, reason):
//!   audit spool full → 39970 (fail closed, like every Admin API mutation)
//!   tenants/<t>.suspended = true, suspended_reason, suspended_at   (one CAS)
//!   for t and every tenant below it: an empty auth change (a new auth
//!     version), so every gateway holding the tenant reloads it, sees the
//!     suspension and closes the tenant's sessions at their next safe point
//!     with 39306 where auth.close_sessions_on_revoke is on (chapter 14,
//!     "Caching and revocation"; sprig_gateway::tenant)
//!   audit admin.tenant_suspended in the tenant's stream
//! resume(t): the flag cleared; audit admin.tenant_resumed
//! ```
//!
//! New logins (MySQL, MongoDB, Redis, HTTP Basic, API keys, Studio tokens)
//! refuse with 39303 from the flag itself, which they read on the tenant
//! and every ancestor; they need nothing from the auth change. A tenant's
//! background work stops too: dispatch holds its after-commit and message
//! triggers, sinks and schedules while it is suspended
//! (`sprig-controller::dispatch`).

use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sprig_catalog::{ChangeOpts, VersionKind};
use sprig_common::Result;
use sprig_common::ids::TenantId;

use super::TenantUsers;

/// The longest suspension reason, in characters.
pub const MAX_REASON: usize = 1024;

/// The body of `POST /admin/v1/tenants/{name}:suspend`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuspendTenantRequest {
    /// Why; required, at most [`MAX_REASON`] characters. Shown by `sprigctl
    /// tenant show` and in the audit event.
    #[serde(default)]
    pub reason: String,
}

/// What `:suspend` and `:resume` did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuspendTenantResponse {
    /// The tenant.
    pub tenant: String,
    /// Its id.
    pub tenant_id: TenantId,
    /// Whether it is suspended now.
    pub suspended: bool,
    /// The reason, while suspended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// When it was suspended, while suspended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspended_at: Option<String>,
    /// `false` when the call changed nothing (already suspended with this
    /// reason, or not suspended).
    pub changed: bool,
    /// The tenants below it, which a suspension covers too (their own
    /// flags are unchanged).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree: Vec<String>,
    /// Tenants whose auth change failed (homed in another DC, or a catalog
    /// that couldn't be written): their open sessions stay until they
    /// reconnect, when the login refuses them. New logins are refused
    /// everywhere regardless.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sessions_not_closed: Vec<String>,
}

impl TenantUsers {
    /// Suspends the tenant named `tenant` and, through it, its subtree.
    ///
    /// # Errors
    /// 39792 for an empty or too long reason; 39970 while the audit spool
    /// is full; 39072 for an unknown tenant or one being deleted; the
    /// store's errors.
    pub async fn suspend(
        &self,
        tenant: &str,
        req: SuspendTenantRequest,
    ) -> Result<SuspendTenantResponse> {
        let reason = req.reason.trim();
        if reason.is_empty() {
            return Err(crate::admin::invalid(
                "a reason is required to suspend a tenant",
            ));
        }
        if reason.chars().count() > MAX_REASON {
            return Err(crate::admin::invalid(format!(
                "the reason is longer than {MAX_REASON} characters"
            )));
        }
        sprig_common::audit::check_available()?;
        let id = self.directory.require(tenant).await?.body().tenant_id;
        let before = self.directory.get(id).await?;
        let after = self
            .directory
            .set_suspended(id, Some(reason), self.clock.wall())
            .await?;
        let changed = before.is_none_or(|b| b.mod_revision != after.mod_revision);
        let below = self.directory.descendants(id).await?;
        // The auth change: every gateway holding one of these tenants
        // reloads it and closes its sessions (see the module docs).
        let mut not_closed = Vec::new();
        for (tid, name) in std::iter::once((id, tenant.to_owned())).chain(
            below
                .iter()
                .map(|e| (e.body().tenant_id, e.body().name.clone())),
        ) {
            if let Err(e) = self.touch_auth(tid).await {
                tracing::warn!(tenant = %name, error = %e,
                    "suspension: no auth change; the tenant's open sessions stay until they reconnect");
                not_closed.push(name);
            }
        }
        let b = after.body();
        if changed {
            let detail = serde_json::json!({
                "tenant": tenant,
                "reason": reason,
                "subtree": below.iter().map(|e| e.body().name.clone()).collect::<Vec<_>>(),
            });
            sprig_common::audit::record(
                sprig_common::audit::AuditStream::Tenant(id),
                "admin.tenant_suspended",
                detail.clone(),
            );
            sprig_common::audit::record(
                sprig_common::audit::AuditStream::Platform,
                "admin.tenant_suspended",
                detail,
            );
            tracing::warn!(
                event = "admin.tenant_suspended",
                tenant,
                reason,
                below = below.len(),
                "tenant suspended"
            );
        }
        Ok(SuspendTenantResponse {
            tenant: b.name.clone(),
            tenant_id: id,
            suspended: b.suspended,
            reason: b.suspended_reason.clone(),
            suspended_at: b.suspended_at.clone(),
            changed,
            subtree: below.iter().map(|e| e.body().name.clone()).collect(),
            sessions_not_closed: not_closed,
        })
    }

    /// Resumes the tenant named `tenant`. Tenants below it stay suspended
    /// while an ancestor of theirs (or they themselves) still is.
    ///
    /// # Errors
    /// 39970 while the audit spool is full; 39072 for an unknown tenant or
    /// one being deleted; the store's errors.
    pub async fn resume(&self, tenant: &str) -> Result<SuspendTenantResponse> {
        sprig_common::audit::check_available()?;
        let id = self.directory.require(tenant).await?.body().tenant_id;
        let before = self.directory.get(id).await?;
        let was = before.as_ref().map(|b| b.body().suspended_reason.clone());
        let after = self
            .directory
            .set_suspended(id, None, SystemTime::now())
            .await?;
        let changed = before.is_none_or(|b| b.mod_revision != after.mod_revision);
        let below = self.directory.descendants(id).await?;
        if changed {
            let detail = serde_json::json!({
                "tenant": tenant,
                "was": was.flatten(),
            });
            sprig_common::audit::record(
                sprig_common::audit::AuditStream::Tenant(id),
                "admin.tenant_resumed",
                detail.clone(),
            );
            sprig_common::audit::record(
                sprig_common::audit::AuditStream::Platform,
                "admin.tenant_resumed",
                detail,
            );
            tracing::warn!(event = "admin.tenant_resumed", tenant, "tenant resumed");
        }
        let b = after.body();
        Ok(SuspendTenantResponse {
            tenant: b.name.clone(),
            tenant_id: id,
            suspended: b.suspended,
            reason: None,
            suspended_at: None,
            changed,
            subtree: below.iter().map(|e| e.body().name.clone()).collect(),
            sessions_not_closed: Vec::new(),
        })
    }

    /// An empty auth change of `tenant`: a new auth version, nothing else.
    async fn touch_auth(&self, tenant: TenantId) -> Result<u64> {
        let c = self
            .catalog
            .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
            .await?;
        c.commit().await
    }
}
