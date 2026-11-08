//! Helpers for tests in other crates (the gateway, `sprigctl`): enable the
//! `testing` feature. They create real catalog rows through the same calls
//! as production code, so a test logs in exactly as a client would.

use mysql_async::Conn;
use sprig_catalog::migrate::MigrateOpts;
use sprig_catalog::{MariaCatalog, Migrator};
use sprig_common::Result;
use sprig_common::ids::{TenantId, UserId};
use sprig_common::time::SystemClock;

use crate::admin::{self, Hashing, NewUser};
use crate::store::AUTH_MODULE;

/// Creates catalog schema `schema` (if needed) with the core and auth
/// modules migrated. The connection's default database becomes `schema`.
///
/// # Errors
/// The migrator's.
pub async fn migrate(conn: &mut Conn, schema: &str) -> Result<()> {
    Migrator::new(&[AUTH_MODULE])?
        .run(conn, schema, &MigrateOpts::default())
        .await?;
    Ok(())
}

/// Creates `'name'@'host'` with `password` in `tenant`'s catalog (the
/// tenant needs a directory entry, as for any change) and returns its id.
///
/// # Errors
/// As [`admin::create_user`].
pub async fn create_user(
    cat: &MariaCatalog,
    tenant: TenantId,
    name: &str,
    host: &str,
    password: &str,
) -> Result<UserId> {
    let mut new = NewUser::new(name, password.as_bytes());
    new.host = host.to_owned();
    new.hashing = Hashing::default();
    Ok(admin::create_user(cat, tenant, &new, &SystemClock)
        .await?
        .user_id)
}
