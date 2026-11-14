//! Whether a sink's owner can still read its source (39229
//! `ER_SPRIG_SINK_OWNER_PRIVILEGE_LOST`).
//!
//! ```text
//! lost_read(catalog, owner, sources):
//!   the owner dropped                → "the owner ... no longer exists"
//!   its account locked               → "the owner's account is locked"
//!     (not a member's `sprig_inherited`, locked by design)
//!   SELECT on a source missing       → "the owner lost SELECT on db.name"
//!   SPRIG_FAN_IN missing (a fan-in   → "the owner lost SPRIG_FAN_IN"
//!    sink's)
//!   (the owner's grants with every role granted to it; a source that no
//!    longer exists is left to the sink's own source checks)
//! ```
//!
//! Gateways ask after a grant change commits, workers before each batch.

use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_common::ids::{DbId, ObjectId, UserId};

use super::{Action, AuthzState, CompiledPrivileges, Target};
use crate::cache::TenantAuth;

/// The run-as principal a declaring tenant creates in each member for
/// inherited objects (data inheritance): locked, with the definitions' grants.
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
    /// A stream or queue by database name and name: a fan-in sink's
    /// definition, whose database is named, not numbered.
    InDatabase {
        /// The database's name.
        db: String,
        /// The name.
        name: String,
    },
    /// `SPRIG_FAN_IN`, which a fan-in sink's owner needs besides `SELECT`.
    FanIn,
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
    // A member's `sprig_inherited` (the run-as principal for inherited
    // objects) is locked by design: nobody logs in as it, and its
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
        let (db_key, db_name, name) = match s {
            ReadSource::FanIn => {
                if !c.allows(Action::FanIn, Target::Tenant) {
                    return Some(format!(
                        "the owner '{}'@'{}' lost SPRIG_FAN_IN",
                        row.name, row.host
                    ));
                }
                continue;
            }
            ReadSource::InDatabase { db, name } => (
                database_key(db, lower_case_table_names),
                db.clone(),
                name.clone(),
            ),
            ReadSource::Named { db, name } => {
                let Some(d) = cat.databases.get(DbId(*db)) else {
                    continue;
                };
                (d.name_key.clone(), d.name.clone(), name.clone())
            }
            ReadSource::Object(id) => {
                let Some(o) = cat.objects.get(ObjectId(*id)) else {
                    continue;
                };
                let Some(d) = cat.databases.get(o.db_id) else {
                    continue;
                };
                (d.name_key.clone(), d.name.clone(), o.name.clone())
            }
        };
        let name_key = object_key(ObjectKind::Table, &name, lower_case_table_names);
        let named = Target::Named {
            db_key: &db_key,
            name_key: &name_key,
        };
        if !c.allows(Action::Select, named)
            && !c.allows(Action::Select, Target::Database { db_key: &db_key })
        {
            return Some(format!(
                "the owner '{}'@'{}' lost SELECT on {db_name}.{name}",
                row.name, row.host
            ));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::{Arc, OnceLock};

    use sprig_catalog::cache::Loaded;
    use sprig_catalog::{ExtState, Snapshot, Versions};
    use sprig_common::ids::TenantId;
    use sprig_settings::{AnySetting, Registry};

    use super::*;
    use crate::authz::privilege::ActionSet;
    use crate::authz::store::{AuthzRows, Grantee, PrivRow, PrivTarget};

    const OWNER: UserId = UserId(7);

    fn registry() -> &'static Registry {
        static R: OnceLock<Registry> = OnceLock::new();
        R.get_or_init(|| {
            Registry::new([&crate::authz::settings::GRANTS_ON_DROP as &dyn AnySetting]).unwrap()
        })
    }

    /// Tenant 42 with user 7 (`ops`) holding `grants` (no databases: a
    /// fan-in source is named by its database's name).
    fn catalog(grants: &[(PrivTarget, &[Action])]) -> TenantCatalog {
        let auth = TenantAuth::build(
            vec![crate::cache::tests::row(OWNER.0, "ops", "%", 1)],
            &[],
            Vec::new(),
        );
        let state = AuthzState::new(AuthzRows {
            privileges: grants
                .iter()
                .map(|(t, a)| PrivRow {
                    priv_id: 0,
                    grantee: Grantee::User(OWNER),
                    target: t.clone(),
                    actions: ActionSet::of(a),
                })
                .collect(),
            ..AuthzRows::default()
        });
        TenantCatalog::build(
            registry(),
            TenantId(42),
            Loaded {
                snapshot: Snapshot {
                    versions: Versions {
                        catalog: 1,
                        auth: 1,
                        purged: 0,
                    },
                    since: None,
                    databases: Vec::new(),
                    objects: Vec::new(),
                    settings: Vec::new(),
                },
                ext: vec![
                    (crate::store::MODULE, Arc::new(auth) as Arc<dyn ExtState>),
                    (
                        crate::authz::store::MODULE,
                        Arc::new(state) as Arc<dyn ExtState>,
                    ),
                ],
            },
        )
    }

    /// A fan-in sink's owner: it pauses when
    /// the owner loses `SPRIG_FAN_IN` or `SELECT` on the definition's name,
    /// as other sinks pause on `SELECT`.
    #[test]
    fn fan_in_owners_need_fan_in_and_select() {
        let reads = [
            ReadSource::FanIn,
            ReadSource::InDatabase {
                db: "app".into(),
                name: "events".into(),
            },
        ];
        let select = (
            PrivTarget::Database("app".into()),
            &[Action::Select] as &[Action],
        );
        let fan_in = (PrivTarget::Tenant, &[Action::FanIn] as &[Action]);
        let both = catalog(&[select.clone(), fan_in.clone()]);
        assert_eq!(lost_read(&both, OWNER, &reads, false), None);
        let why = lost_read(&catalog(&[select]), OWNER, &reads, false).unwrap();
        assert!(why.contains("lost SPRIG_FAN_IN"), "{why}");
        let why = lost_read(&catalog(&[fan_in]), OWNER, &reads, false).unwrap();
        assert!(why.contains("lost SELECT on app.events"), "{why}");
        // A dropped owner.
        let why = lost_read(&both, UserId(8), &reads, false).unwrap();
        assert!(why.contains("no longer exists"), "{why}");
    }
}
