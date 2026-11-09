//! Authorization (chapter 29, build steps 1 to 6, the R0 part).
//!
//! - [`authorizer`]: the interface the gateway calls for every decision
//!   ([`Authorizer`], [`SessionGrants`]), chosen per tenant by
//!   `auth.authorizer`, and the default engine, [`MysqlGrants`], which is
//!   the rest of this list;
//! - [`privilege`]: actions as stable bits, levels, the MySQL and MariaDB
//!   privilege names (step 1);
//! - [`store`]: the `authz` catalog module (`roles`, `role_edges`,
//!   `privileges`) with typed rows and writes (step 1);
//! - [`state`]: [`AuthzExtension`], loading [`AuthzState`] into every
//!   catalog snapshot at each auth version (step 2);
//! - [`compile`]: [`CompiledPrivileges`] and the checks (step 2);
//! - [`stmt`], [`account_sql`] and [`show`]: the account statements and
//!   `SHOW GRANTS` (steps 3 and 7's basic roles);
//! - [`backend`]: per-user MariaDB accounts granted only on the tenant's
//!   own shard schemas, and their reconciler (step 5);
//! - [`definers`]: stored code runs as a tenant account, never as
//!   `sprig_ctl`, and code created before that is moved;
//! - [`migrate`]: bringing an existing tenant's catalog to the module.
//!
//! The gateway glues these to statements (`sprig-gateway`'s
//! `tenant::authz`), and `sprig-sql` parses the statements
//! (`sprig_sql::account_stmt`) and finds what each statement touches
//! (`sprig_sql::access`).

pub mod account_sql;
pub mod authorizer;
pub mod backend;
pub mod compile;
pub mod definers;
pub mod engine_guard;
pub mod privilege;
pub mod settings;
pub mod show;
pub mod state;
pub mod stmt;
pub mod store;

pub use authorizer::{Authorizer, MysqlGrants, SessionGrants};
pub use compile::{CompiledPrivileges, Target};
pub use privilege::{Action, ActionSet, Level, ObjectType};
pub use state::{AuthzExtension, AuthzState};
pub use store::{AUTHZ_MODULE, Grantee, PrivTarget};

use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::{CatalogLocator, ChangeOpts, MariaCatalog, Migrator, VersionKind};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};

use crate::store::AUTH_MODULE;

/// Migrates `tenant`'s catalog to the `auth` and `authz` modules (both
/// idempotent), then commits an empty auth change so every gateway reloads
/// the tenant with the module (an unmigrated catalog allows everything,
/// see [`AuthzState::unmigrated`]). Returns the auth version.
///
/// # Errors
/// 39070 if the catalog is unreachable; the migrator's and the change's.
pub async fn migrate(
    locator: &dyn CatalogLocator,
    catalog: &MariaCatalog,
    tenant: TenantId,
) -> Result<u64> {
    let ep = locator.writer(tenant).await?;
    let mut conn = ep.pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
    })?;
    Migrator::new(&[AUTH_MODULE, AUTHZ_MODULE])?
        .run(&mut conn, ep.store.schema(), &MigrateOpts::default())
        .await?;
    drop(conn);
    let c = catalog
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    c.commit().await
}
