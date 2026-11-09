//! Tenant users through the Admin API, interim (M0.3): `POST
//! /admin/v1/tenants/{name}/users` creates a user with a password in the
//! tenant's catalog (chapter 14, "Setting a password"), so a development
//! or test deployment can log in to a tenant before chapter 29's `CREATE
//! USER` exists. `sprigctl tenant user add <tenant> <user> --password ...`
//! calls it.
//!
//! The call migrates the catalog's `auth` module first (idempotent), so
//! tenants bootstrapped before [`AuthHooks`] registered the module get its
//! tables too. Bootstrap migrates it with the core module from now on.
//!
//! Where this differs from the book: chapter 14 creates users with SQL
//! (`CREATE USER`, chapter 29) and the Admin API's `Users` service with an
//! authenticated operator; this endpoint trusts the Admin API token, like
//! every R0 Admin API call (decision N5).

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_auth::admin::{self, NewUser};
use sprig_auth::portable::PortableAccounts;
use sprig_auth::store::AUTH_MODULE;
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::{CatalogLocator, MariaCatalog, Migrator, Module};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};

use crate::tenants::BootstrapHooks;
use crate::tree::{Directory, TenantState};

/// Bootstrap hooks that migrate chapter 14's `auth` module (and chapter
/// 12's `workload` module) with the core catalog, so a new tenant's
/// catalog has its `users` and `verifiers` tables (gateways load them into
/// every snapshot) and its workload tables.
#[derive(Debug, Clone, Copy, Default)]
pub struct AuthHooks;

#[async_trait::async_trait]
impl BootstrapHooks for AuthHooks {
    fn modules(&self) -> Vec<Module> {
        // Chapter 12's workload tables too: gateways write them from a
        // tenant's first statement.
        vec![
            AUTH_MODULE,
            sprig_auth::authz::AUTHZ_MODULE,
            sprig_workload::store::WORKLOAD_MODULE,
        ]
    }
}

/// `POST /admin/v1/tenants/{name}/users`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateUserRequest {
    /// The user name, without the tenant prefix (`app`, not `acme.app`).
    pub user: String,
    /// The host pattern; `%` without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// The password.
    pub password: String,
    /// Also store a `mysql_native_password` verifier (logins use it only
    /// where the tenant's `auth.mysql_native_password` is on).
    #[serde(default)]
    pub native: bool,
}

impl std::fmt::Debug for CreateUserRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateUserRequest")
            .field("user", &self.user)
            .field("host", &self.host)
            .field("native", &self.native)
            .finish_non_exhaustive()
    }
}

/// What a create returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateUserResponse {
    /// The tenant.
    pub tenant: String,
    /// The tenant's id.
    pub tenant_id: TenantId,
    /// The user.
    pub user: String,
    /// The host pattern.
    pub host: String,
    /// The new user's id (its backend account is `t<tenant>_u<id>` from
    /// chapter 29).
    pub user_id: UserId,
    /// The auth version the user committed at.
    pub auth_version: u64,
    /// The name to log in with on a shared endpoint.
    pub login: String,
}

/// The Admin API's user calls.
#[async_trait::async_trait]
pub trait UserAdmin: Send + Sync {
    /// Creates a user in `tenant`'s catalog.
    ///
    /// # Errors
    /// As [`TenantUsers::create`].
    async fn create(&self, tenant: &str, req: CreateUserRequest) -> Result<CreateUserResponse>;

    /// A tenant's accounts for an export (chapter 47): live users with
    /// their verifier strings, and its SQL roles.
    ///
    /// # Errors
    /// As [`TenantUsers::export_accounts`].
    async fn export_accounts(&self, _tenant: &str) -> Result<PortableAccounts> {
        Err(crate::admin::invalid(
            "this controller doesn't export accounts",
        ))
    }

    /// Creates exported accounts in a tenant (chapter 47).
    ///
    /// # Errors
    /// As [`TenantUsers::import_accounts`].
    async fn import_accounts(
        &self,
        _tenant: &str,
        _accounts: PortableAccounts,
    ) -> Result<ImportAccountsResponse> {
        Err(crate::admin::invalid(
            "this controller doesn't import accounts",
        ))
    }
}

/// What `POST /admin/v1/tenants/{name}/accounts` did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportAccountsResponse {
    /// Accounts created, as `'name'@'host'`.
    pub created: Vec<String>,
    /// Accounts that existed and were left alone.
    pub existing: Vec<String>,
    /// The newest auth version written.
    pub auth_version: u64,
}

/// Creates users in tenant catalogs.
pub struct TenantUsers {
    directory: Directory,
    locator: Arc<dyn CatalogLocator>,
    catalog: MariaCatalog,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for TenantUsers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantUsers").finish_non_exhaustive()
    }
}

impl TenantUsers {
    /// Users in the catalogs `locator` finds, changed through `catalog`.
    #[must_use]
    pub fn new(
        directory: Directory,
        locator: Arc<dyn CatalogLocator>,
        catalog: MariaCatalog,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            directory,
            locator,
            catalog,
            clock,
        }
    }

    /// Creates `req.user`@`req.host` with `req.password` in the tenant
    /// named `tenant`.
    ///
    /// # Errors
    /// 39072 for an unknown tenant, or one not `active`; 39792 for an
    /// empty password; 1396 if the account exists; 39998 for a bad name or
    /// host; the catalog's errors (39070, 39071).
    pub async fn create(&self, tenant: &str, req: CreateUserRequest) -> Result<CreateUserResponse> {
        let entry = self.directory.require(tenant).await?;
        let e = entry.body();
        if e.state != TenantState::Active {
            return Err(SprigError::new(
                SprigCode::TenantNotReady,
                format!("tenant {tenant} is {}, not active", e.state.as_str()),
            ));
        }
        if req.password.is_empty() {
            return Err(crate::admin::invalid("a password is required"));
        }
        let id = e.tenant_id;
        // The auth module's tables, for tenants bootstrapped before the
        // hook existed. Idempotent.
        let ep = self.locator.writer(id).await?;
        let mut conn = crate::catalogs::acquire(&ep.pool, "auth tables").await?;
        Migrator::new(&[AUTH_MODULE, sprig_auth::authz::AUTHZ_MODULE])?
            .run(&mut conn, ep.store.schema(), &MigrateOpts::default())
            .await?;
        drop(conn);
        let mut new = NewUser::new(req.user.clone(), req.password.as_bytes());
        if let Some(h) = &req.host {
            new.host.clone_from(h);
        }
        new.source = sprig_auth::store::UserSource::Api;
        new.hashing.native = req.native;
        let mut changed = admin::create_user(&self.catalog, id, &new, &*self.clock).await?;
        // Chapter 29: an operator-made user administers its tenant, as the
        // bootstrap admin does (ALL and SPRIG_TENANT_ADMIN on *.*).
        changed.auth_version =
            sprig_auth::authz::account_sql::grant_tenant_admin(&self.catalog, id, changed.user_id)
                .await?;
        tracing::info!(event = "auth.user_created", tenant, user = %req.user, host = %new.host, user_id = %changed.user_id, "user created");
        Ok(CreateUserResponse {
            tenant: tenant.to_owned(),
            tenant_id: id,
            login: format!("{tenant}.{}", req.user),
            user: req.user,
            host: new.host,
            user_id: changed.user_id,
            auth_version: changed.auth_version,
        })
    }

    async fn active(&self, tenant: &str) -> Result<TenantId> {
        let entry = self.directory.require(tenant).await?;
        let e = entry.body();
        if e.state != TenantState::Active {
            return Err(SprigError::new(
                SprigCode::TenantNotReady,
                format!("tenant {tenant} is {}, not active", e.state.as_str()),
            ));
        }
        Ok(e.tenant_id)
    }

    /// The live users (with verifier strings) and SQL roles of the tenant
    /// named `tenant`, for `sprigctl tenant export` (chapter 47).
    ///
    /// # Errors
    /// 39072 for an unknown or inactive tenant; the catalog's errors.
    pub async fn export_accounts(&self, tenant: &str) -> Result<PortableAccounts> {
        let id = self.active(tenant).await?;
        let accounts = sprig_auth::portable::export(&self.catalog, id).await?;
        sprig_common::audit::record(
            sprig_common::audit::AuditStream::Tenant(id),
            "admin.accounts_exported",
            serde_json::json!({"tenant": tenant, "users": accounts.users.len(), "roles": accounts.roles.len()}),
        );
        Ok(accounts)
    }

    /// Creates `accounts.users` in the tenant named `tenant` with their
    /// verifiers unchanged (accounts that exist are left alone), for
    /// `sprigctl tenant import`. Roles and grants are SQL, replayed
    /// through the gateway.
    ///
    /// # Errors
    /// 39072 for an unknown or inactive tenant; 39970 while the audit
    /// spool is full; 39792 for a malformed account; the catalog's errors.
    pub async fn import_accounts(
        &self,
        tenant: &str,
        accounts: PortableAccounts,
    ) -> Result<ImportAccountsResponse> {
        sprig_common::audit::check_available()?;
        let id = self.active(tenant).await?;
        let ep = self.locator.writer(id).await?;
        let mut conn = crate::catalogs::acquire(&ep.pool, "auth tables").await?;
        Migrator::new(&[AUTH_MODULE, sprig_auth::authz::AUTHZ_MODULE])?
            .run(&mut conn, ep.store.schema(), &MigrateOpts::default())
            .await?;
        drop(conn);
        let mut out = ImportAccountsResponse::default();
        for a in &accounts.users {
            let account = format!("'{}'@'{}'", a.name, a.host);
            match sprig_auth::portable::import(&self.catalog, id, a, &*self.clock).await? {
                Some(c) => {
                    out.auth_version = out.auth_version.max(c.auth_version);
                    if accounts
                        .tenant_admins
                        .iter()
                        .any(|(n, h)| *n == a.name && *h == a.host)
                    {
                        let v = sprig_auth::authz::account_sql::grant_tenant_admin(
                            &self.catalog,
                            id,
                            c.user_id,
                        )
                        .await?;
                        out.auth_version = out.auth_version.max(v);
                    }
                    out.created.push(account);
                }
                None => out.existing.push(account),
            }
        }
        sprig_common::audit::record(
            sprig_common::audit::AuditStream::Tenant(id),
            "admin.accounts_imported",
            serde_json::json!({"tenant": tenant, "created": out.created, "existing": out.existing}),
        );
        tracing::info!(
            event = "admin.accounts_imported",
            tenant,
            created = out.created.len(),
            existing = out.existing.len(),
            "accounts imported"
        );
        Ok(out)
    }
}

#[async_trait::async_trait]
impl UserAdmin for TenantUsers {
    async fn create(&self, tenant: &str, req: CreateUserRequest) -> Result<CreateUserResponse> {
        TenantUsers::create(self, tenant, req).await
    }

    async fn export_accounts(&self, tenant: &str) -> Result<PortableAccounts> {
        TenantUsers::export_accounts(self, tenant).await
    }

    async fn import_accounts(
        &self,
        tenant: &str,
        accounts: PortableAccounts,
    ) -> Result<ImportAccountsResponse> {
        TenantUsers::import_accounts(self, tenant, accounts).await
    }
}
