//! The pieces chapter 07's name virtualization asks the gateway for: the
//! tenant's [`NameMap`] built from a catalog snapshot, the tenant's
//! sessions for `SHOW PROCESSLIST`, and account names for output.

use std::sync::Arc;

use sprig_auth::TenantAuth;
use sprig_catalog::store::DbState;
use sprig_catalog::{TenantCatalog, ddl::shard_schemas};
use sprig_common::ids::TenantId;
use sprig_common::names::AccountName;
use sprig_sql::names::show::{ProcessRow, SessionSource};
use sprig_sql::names::{AccountResolver, LogicalDb, MemoryNameMap};

use crate::session::SessionTable;

/// The tenant's names at `cat`'s version: every live database with its
/// shard schemas ([`shard_schemas`]). Databases still being created are
/// left out (readers treat `creating` rows as absent).
#[must_use]
pub fn name_map(cat: &TenantCatalog) -> MemoryNameMap {
    let mut dbs: Vec<_> = cat
        .databases
        .iter()
        .filter(|d| d.state == DbState::Active)
        .collect();
    dbs.sort_by_key(|d| d.db_id);
    let mut map = MemoryNameMap::new(cat.tenant).with_version(cat.version);
    for d in dbs {
        map.insert(LogicalDb {
            id: d.db_id,
            name: d.name.clone(),
            shards: shard_schemas(cat.tenant, d),
            reference: None,
        });
    }
    map
}

/// The tenant's sessions on this gateway (chapter 04: the DC's gateways,
/// once registration exists). Until chapter 29's `PROCESS` privilege, a
/// user sees only its own sessions, as MySQL shows a user without it.
pub struct TenantSessions<'a> {
    /// The gateway's sessions.
    pub table: &'a Arc<SessionTable>,
    /// The tenant.
    pub tenant: TenantId,
    /// The user.
    pub user: &'a str,
}

impl SessionSource for TenantSessions<'_> {
    fn processes(&self) -> Vec<ProcessRow> {
        self.table
            .list()
            .into_iter()
            .filter(|r| r.tenant == Some(self.tenant) && r.user.as_deref() == Some(self.user))
            .map(|r| ProcessRow {
                id: u64::from(r.id.0),
                user: r.user.unwrap_or_default(),
                host: r.host.to_string(),
                db: r.db,
                command: r.command.to_owned(),
                time: r.time.as_secs(),
                time_ms: u64::try_from(r.time.as_millis()).unwrap_or(u64::MAX),
                state: Some(r.state.as_str().to_owned()).filter(|_| r.command != "Sleep"),
                info: r.info,
            })
            .collect()
    }
}

/// Backend account names of the tenant (`t42_u7`) as the user names
/// clients know (`alice`), from the snapshot's auth module.
pub struct Accounts<'a>(pub Option<&'a TenantAuth>);

impl AccountResolver for Accounts<'_> {
    fn account_name(&self, account: &AccountName) -> Option<String> {
        match account {
            AccountName::User { user, .. } => self.0?.user(*user).map(|u| u.row.name.clone()),
            _ => None,
        }
    }
}
