//! Whether a sink's owner can still read its source (chapter 51, "Owner
//! lost access"; 39229 `ER_SPRIG_SINK_OWNER_PRIVILEGE_LOST`; authfix2).
//!
//! ```text
//! lost_read(catalog, owner, sources):
//!   the owner dropped                → "the owner ... no longer exists"
//!   its account locked               → "the owner's account is locked"
//!     (not a member's `sprig_inherited`, locked by design)
//!   SELECT on a source missing       → "the owner lost SELECT on db.name"
//!   (the owner's grants with every role granted to it; a source that no
//!    longer exists is left to the sink's own source checks)
//! ```
//!
//! Gateways ask after a grant change commits, workers before each batch.

use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::object_key;
use sprig_catalog::store::ObjectKind;
use sprig_common::ids::{DbId, ObjectId, UserId};

use super::{Action, AuthzState, CompiledPrivileges, Target};
use crate::cache::TenantAuth;

/// The run-as principal a declaring tenant creates in each member for
/// inherited objects (chapter 42): locked, with the definitions' grants.
pub const INHERITED_RUN_AS: &str = "sprig_inherited";

/// What a sink reads, for its owner's `SELECT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadSource {
    /// A table, stream or queue by database and name (privileges are held
    /// by name, as `GRANT SELECT ON db.name` writes them).
    Named {
        /// The database.
        db: u32,
        /// The name.
        name: String,
    },
    /// A table by object id (`CHANGES OF t`).
    Object(u64),
}

/// Why `owner` can no longer read every one of `sources` (see the module
/// docs), or `None` while it can. `lower_case_table_names` keys the
/// names as the tenant does. `None` too when the catalog has no account
/// or grant state to tell (the caller can't decide; nothing pauses).
#[must_use]
pub fn lost_read(
    cat: &TenantCatalog,
    owner: UserId,
    sources: &[ReadSource],
    lower_case_table_names: bool,
) -> Option<String> {
    let auth = TenantAuth::of(cat)?;
    let state = AuthzState::of(cat)?;
    let Some(user) = auth.user(owner) else {
        return Some(format!("the owner (user id {}) no longer exists", owner.0));
    };
    let row = &user.row;
    // A member's `sprig_inherited` (chapter 42's run-as principal for
    // inherited objects) is locked by design: nobody logs in as it, and its
    // rights are the grants the definitions give it, checked below.
    let run_as = row.name == INHERITED_RUN_AS && row.created_by_tenant != 0;
    if row.account_locked && !run_as {
        return Some(format!("the owner '{}'@'{}' is locked", row.name, row.host));
    }
    let c = CompiledPrivileges::compile(state, owner, &state.granted_roles(owner));
    if c.everything {
        return None;
    }
    for s in sources {
        let (db, name) = match s {
            ReadSource::Named { db, name } => (DbId(*db), name.clone()),
            ReadSource::Object(id) => match cat.objects.get(ObjectId(*id)) {
                Some(o) => (o.db_id, o.name.clone()),
                None => continue,
            },
        };
        let Some(d) = cat.databases.get(db) else {
            continue;
        };
        let name_key = object_key(ObjectKind::Table, &name, lower_case_table_names);
        let named = Target::Named {
            db_key: &d.name_key,
            name_key: &name_key,
        };
        if !c.allows(Action::Select, named)
            && !c.allows(
                Action::Select,
                Target::Database {
                    db_key: &d.name_key,
                },
            )
        {
            return Some(format!(
                "the owner '{}'@'{}' lost SELECT on {}.{name}",
                row.name, row.host, d.name
            ));
        }
    }
    None
}
