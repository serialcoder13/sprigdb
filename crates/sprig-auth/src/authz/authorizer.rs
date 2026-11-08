//! The authorization interface (chapter 29, "Pluggable authorization"):
//! what the gateway asks of an authorization engine, and the default
//! engine, [`MysqlGrants`] (MySQL 8.4's grant model, the rest of this
//! module tree).
//!
//! The tenant's `auth.authorizer` setting names its engine
//! ([`by_name`]); a session reads it when it first compiles its
//! privileges and keeps that engine. The gateway calls the engine for
//! every decision and never looks at an engine's data itself:
//!
//! | Gateway need | Call |
//! |---|---|
//! | A session's rights at a catalog snapshot | [`Authorizer::session`], cached per auth and catalog version and active roles |
//! | Roles active at login and after `SET ROLE` | [`Authorizer::active_roles`] |
//! | Statement checks: which objects, which actions | [`SessionGrants::allows`] per object a statement touches |
//! | `SHOW DATABASES`, `USE`, `information_schema` | [`SessionGrants::sees_all_databases`], [`SessionGrants::sees_database`] |
//! | Settings statements and `SPRIG_SETTINGS` rows | [`SessionGrants::settings`] |
//! | Rows a statement may see or change | [`SessionGrants::row_filter`]; today every engine returns `None` and the gateway refuses a statement it would have to filter (39366) |
//! | MariaDB grants of the per-user backend accounts | [`Authorizer::backend_grants`] |
//! | Account statements (`GRANT`, `CREATE ROLE`, `SHOW GRANTS`, ...) | [`Authorizer::account_statement`] |
//! | `DROP TABLE`, `DROP DATABASE`, ... | [`Authorizer::objects_dropped`] |
//!
//! **Adding an engine.** Implement [`Authorizer`] and [`SessionGrants`]
//! (the latter for whatever the engine compiles per session), add the
//! name to [`by_name`] and to `auth.authorizer`'s allowed values
//! ([`super::settings::AUTHORIZER`]). An engine with its own catalog
//! tables registers a catalog extension like
//! [`super::AuthzExtension`], so its rows arrive in every snapshot at the
//! auth version. Engines that refine rather than replace MySQL grants
//! (a Supabase-style engine adds row predicates to them) wrap
//! [`MysqlGrants`] and delegate. Chapter 29 sketches a Supabase-style and
//! a Firebase-style engine.

use std::any::Any;
use std::collections::BTreeSet;
use std::sync::{Arc, LazyLock};

use sprig_catalog::store::{DbState, Namespace, ObjectState};
use sprig_catalog::{ChangeOpts, MariaCatalog, TenantCatalog, VersionKind};
use sprig_common::ids::{DbId, ObjectId, RoleId, UserId};
use sprig_common::{Result, SprigError};

use super::account_sql::{self, AccountCtx, Outcome};
use super::backend::{self, BackendState};
use super::compile::{CompiledPrivileges, Target};
use super::privilege::Action;
use super::state::AuthzState;
use super::stmt::AccountStmt;
use super::store::{AuthzStore, PrivTarget};
use crate::cache::TenantAuth;

/// `auth.authorizer`'s value for [`MysqlGrants`].
pub const MYSQL_GRANTS: &str = "mysql_grants";

/// The engine named `name` (an `auth.authorizer` value); `None` for a name
/// this build doesn't know, which the gateway treats as "allow nothing".
#[must_use]
pub fn by_name(name: &str) -> Option<Arc<dyn Authorizer>> {
    match name {
        MYSQL_GRANTS => Some(Arc::new(MysqlGrants)),
        _ => None,
    }
}

/// Who a session's rights are compiled for.
#[derive(Debug, Clone, Copy)]
pub struct Subject<'a> {
    /// The tenant user.
    pub user: UserId,
    /// The active roles ([`Authorizer::active_roles`]).
    pub roles: &'a [RoleId],
}

/// A settings operation (chapter 08) to authorize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsOp<'a> {
    /// Values or bounds of a descendant tenant.
    Descendant,
    /// The tenant's values: writing them, or listing them (`show`).
    Tenant {
        /// `SHOW SPRIG SETTINGS` / `SPRIG_SETTINGS` rather than a write.
        show: bool,
    },
    /// Writing bounds at any scope.
    Bounds,
    /// A database's values.
    Database {
        /// Its name key.
        db_key: &'a str,
        /// Its id.
        db: DbId,
        /// Listing rather than writing.
        show: bool,
    },
    /// A table's values.
    Object {
        /// Its database's name key.
        db_key: &'a str,
        /// Its database.
        db: DbId,
        /// The table.
        object: ObjectId,
        /// Listing rather than writing.
        show: bool,
    },
}

/// What a refused settings operation lacked; the gateway words the error
/// (1227 for the first two, 1044 or 1142 for the target).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsDenied {
    /// `SPRIG_TENANT_ADMIN`.
    TenantAdmin,
    /// `SPRIG_SETTINGS_ADMIN`.
    SettingsAdmin,
    /// A privilege on the database or table itself.
    Target,
}

/// Rows of one table a statement may see or change: a boolean SQL
/// expression over the table's columns, with the session's values (its
/// user, token claims, ...) already bound as literals, as chapter 29's
/// row-level-security rewrite injects it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowFilter {
    /// The predicate.
    pub predicate: String,
}

/// An object a `DROP` removed, as [`Authorizer::objects_dropped`] gets it,
/// with `auth.grants_on_drop` resolved at its database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dropped {
    /// A table, view, routine, ...
    Object {
        /// Its database.
        db: DbId,
        /// The database's name key.
        db_key: String,
        /// The object.
        object: ObjectId,
        /// Its name key.
        name_key: String,
        /// Its namespace (grants held by name are for relations only).
        namespace: Namespace,
        /// `auth.grants_on_drop = keep`.
        keep: bool,
    },
    /// A database and everything in it.
    Database {
        /// The database.
        db: DbId,
        /// Its name key.
        db_key: String,
        /// `auth.grants_on_drop = keep`.
        keep: bool,
    },
}

/// One session's rights at one catalog snapshot, as an engine compiled
/// them. The gateway asks it about every statement.
pub trait SessionGrants: Send + Sync + std::fmt::Debug {
    /// Everything is allowed (a catalog from before authorization, or an
    /// internal principal): the gateway skips its checks.
    fn unrestricted(&self) -> bool;

    /// Whether `action` is allowed on `on` (statement-level checks; for an
    /// object, a grant on any of its columns counts, and MariaDB applies
    /// the exact column rules).
    fn allows(&self, action: Action, on: Target<'_>) -> bool;

    /// Whether every database is listed (`SHOW DATABASES` privilege).
    fn sees_all_databases(&self) -> bool;

    /// Whether database `db_key` is listed and may be `USE`d.
    fn sees_database(&self, db_key: &str, db: Option<DbId>) -> bool;

    /// Settings privileges (chapter 29, "Settings").
    ///
    /// # Errors
    /// What the operation lacked.
    fn settings(&self, op: SettingsOp<'_>) -> std::result::Result<(), SettingsDenied>;

    /// The rows of `table` that `action` may see or change; `None` for
    /// all rows. The hook for row-level engines: today the gateway can't
    /// inject a filter yet, so it refuses a statement on a table that has
    /// one (39366 `ER_SPRIG_RLS_UNSUPPORTED`) rather than run it
    /// unfiltered.
    fn row_filter(&self, table: Target<'_>, action: Action) -> Option<RowFilter> {
        let _ = (table, action);
        None
    }

    /// The engine's own type, for its account statements.
    fn as_any(&self) -> &dyn Any;
}

/// An authorization engine (see the module docs).
#[async_trait::async_trait]
pub trait Authorizer: Send + Sync + std::fmt::Debug {
    /// Its `auth.authorizer` value.
    fn name(&self) -> &'static str;

    /// The roles active for `user`: `chosen` (its `SET ROLE`) narrowed to
    /// those still granted, or, before any choice, those active at login
    /// (every granted role with `all_at_login`).
    fn active_roles(
        &self,
        cat: &TenantCatalog,
        user: UserId,
        chosen: Option<&[RoleId]>,
        all_at_login: bool,
    ) -> Vec<RoleId>;

    /// `subject`'s rights at `cat`.
    fn session(&self, cat: &TenantCatalog, subject: &Subject<'_>) -> Arc<dyn SessionGrants>;

    /// The backend-account derivation hook: the MariaDB grants every
    /// per-user account of the tenant should hold at `cat` (chapter 29,
    /// "Backend accounts and grants"), which the reconciler applies.
    fn backend_grants(&self, cat: &TenantCatalog, auth: &TenantAuth) -> BackendState;

    /// Runs an account statement.
    ///
    /// # Errors
    /// The statement's MySQL error, or the catalog's.
    async fn account_statement(&self, cx: &AccountCtx<'_>, stmt: &AccountStmt) -> Result<Outcome>;

    /// `dropped` were dropped; `cat` is the snapshot from before the drop.
    /// Returns the auth version the engine's change committed at, if it
    /// wrote one.
    ///
    /// # Errors
    /// The catalog's.
    async fn objects_dropped(
        &self,
        catalog: &MariaCatalog,
        cat: &TenantCatalog,
        dropped: &[Dropped],
    ) -> Result<Option<u64>>;
}

// ---- the mysql_grants engine ----

impl SessionGrants for CompiledPrivileges {
    fn unrestricted(&self) -> bool {
        self.everything
    }

    fn allows(&self, action: Action, on: Target<'_>) -> bool {
        CompiledPrivileges::allows(self, action, on)
    }

    fn sees_all_databases(&self) -> bool {
        self.everything || self.tenant.has(Action::ShowDatabases)
    }

    fn sees_database(&self, db_key: &str, db: Option<DbId>) -> bool {
        self.any_in_database(db_key, db)
    }

    fn settings(&self, op: SettingsOp<'_>) -> std::result::Result<(), SettingsDenied> {
        if self.everything || self.tenant_admin() {
            return Ok(());
        }
        let admin = self.tenant.has(Action::SettingsAdmin);
        let ok = match op {
            SettingsOp::Descendant => return Err(SettingsDenied::TenantAdmin),
            SettingsOp::Tenant { show } if show || admin => true,
            SettingsOp::Bounds if admin => true,
            SettingsOp::Tenant { .. } | SettingsOp::Bounds => {
                return Err(SettingsDenied::SettingsAdmin);
            }
            SettingsOp::Database { db_key, db, show } => {
                if show {
                    self.any_in_database(db_key, Some(db))
                } else {
                    CompiledPrivileges::allows(self, Action::Alter, Target::Database { db_key })
                }
            }
            SettingsOp::Object {
                db_key,
                db,
                object,
                show,
            } => {
                if show {
                    self.any_on_object(db_key, db, object)
                } else {
                    CompiledPrivileges::allows(
                        self,
                        Action::Alter,
                        Target::Object { db_key, db, object },
                    )
                }
            }
        };
        if ok {
            Ok(())
        } else {
            Err(SettingsDenied::Target)
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// MySQL 8.4's grant model: users, roles and grants in the `authz` catalog
/// module, compiled per session ([`CompiledPrivileges`]), mirrored into
/// MariaDB ([`backend`]). The default engine.
#[derive(Debug, Clone, Copy, Default)]
pub struct MysqlGrants;

static UNMIGRATED: LazyLock<AuthzState> = LazyLock::new(AuthzState::unmigrated);

fn state_of(cat: &TenantCatalog) -> &AuthzState {
    AuthzState::of(cat).unwrap_or(&UNMIGRATED)
}

/// The live table or view `(db_key, name_key)` of `cat`.
fn relation(cat: &TenantCatalog, db_key: &str, name_key: &str) -> Option<(DbId, ObjectId)> {
    let d = cat
        .database(db_key)
        .filter(|d| d.state == DbState::Active)?;
    let o = cat
        .object(d.db_id, Namespace::Relation, name_key)
        .filter(|o| o.state == ObjectState::Active)?;
    Some((d.db_id, o.object_id))
}

#[async_trait::async_trait]
impl Authorizer for MysqlGrants {
    fn name(&self) -> &'static str {
        MYSQL_GRANTS
    }

    fn active_roles(
        &self,
        cat: &TenantCatalog,
        user: UserId,
        chosen: Option<&[RoleId]>,
        all_at_login: bool,
    ) -> Vec<RoleId> {
        let state = state_of(cat);
        let granted = state.granted_roles(user);
        match chosen {
            Some(c) => c.iter().copied().filter(|r| granted.contains(r)).collect(),
            None if all_at_login => granted,
            None => state.default_roles(user),
        }
    }

    fn session(&self, cat: &TenantCatalog, subject: &Subject<'_>) -> Arc<dyn SessionGrants> {
        let resolve = |d: &str, n: &str| relation(cat, d, n);
        Arc::new(CompiledPrivileges::compile_at(
            state_of(cat),
            &resolve,
            subject.user,
            subject.roles,
        ))
    }

    fn backend_grants(&self, cat: &TenantCatalog, auth: &TenantAuth) -> BackendState {
        backend::desired(cat, state_of(cat), auth)
    }

    async fn account_statement(&self, cx: &AccountCtx<'_>, stmt: &AccountStmt) -> Result<Outcome> {
        account_sql::execute(cx, stmt).await
    }

    async fn objects_dropped(
        &self,
        catalog: &MariaCatalog,
        cat: &TenantCatalog,
        dropped: &[Dropped],
    ) -> Result<Option<u64>> {
        let ops = drop_plan(state_of(cat), cat, dropped);
        if ops.is_empty() {
            return Ok(None);
        }
        let mut c = catalog
            .change_with(cat.tenant, VersionKind::Auth, ChangeOpts::default())
            .await?;
        let r: Result<()> = async {
            let (core, q) = c.tx().parts();
            let store = AuthzStore::new(core.schema());
            for op in &ops {
                match op {
                    DropOp::KeepByName {
                        db,
                        object,
                        db_key,
                        name_key,
                    } => {
                        store
                            .keep_by_name(q, *db, *object, db_key, name_key)
                            .await?;
                    }
                    DropOp::ForgetObject { db, object } => {
                        store.forget_object(q, *db, *object).await?;
                    }
                    DropOp::ForgetNamed { db_key, name_key } => {
                        store.forget_named(q, db_key, name_key.as_deref()).await?;
                    }
                    DropOp::ForgetDatabase { db, db_key } => {
                        store.forget_database(q, *db, db_key).await?;
                    }
                }
            }
            Ok(())
        }
        .await;
        match r {
            Ok(()) => Ok(Some(c.commit().await?)),
            Err(e) => {
                let _ = c.rollback().await;
                let _ = catalog.settle(cat.tenant, VersionKind::Auth).await;
                Err(e)
            }
        }
    }
}

/// A catalog write [`MysqlGrants::objects_dropped`] plans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DropOp {
    /// The object's rows become grants held by name.
    KeepByName {
        /// The database.
        db: DbId,
        /// The object.
        object: ObjectId,
        /// The database's name key.
        db_key: String,
        /// The object's name key.
        name_key: String,
    },
    /// The object's rows are deleted.
    ForgetObject {
        /// The database.
        db: DbId,
        /// The object.
        object: ObjectId,
    },
    /// Grants held by name on the relation (or every relation of the
    /// database, `None`) are deleted.
    ForgetNamed {
        /// The database's name key.
        db_key: String,
        /// The relation's name key.
        name_key: Option<String>,
    },
    /// The database's `db.*` rows and its objects' rows are deleted.
    ForgetDatabase {
        /// The database.
        db: DbId,
        /// Its name key.
        db_key: String,
    },
}

/// What dropping `dropped` does to the rows of `state` (`cat` is the
/// snapshot before the drop): MySQL's rules with `auth.grants_on_drop`.
///
/// ```text
/// routine (any mode)       its rows are deleted (MySQL and MariaDB do)
/// table, view; keep        its rows become grants held by name
/// table, view; drop        its rows and the grants held by its name are deleted
/// database; keep           its db.* rows stay (they are by name); each live
///                          relation of it as "keep"; routine rows deleted
/// database; drop           db.*, every object row, every grant held by name in it
/// ```
#[must_use]
pub fn drop_plan(state: &AuthzState, cat: &TenantCatalog, dropped: &[Dropped]) -> Vec<DropOp> {
    let rows = &state.rows().privileges;
    let has_rows = |object: ObjectId| {
        rows.iter().any(|p| {
            matches!(p.target, PrivTarget::Object { object: o, .. } | PrivTarget::Column { object: o, .. } if o == object)
        })
    };
    let named_in = |db_key: &str, name_key: Option<&str>| {
        rows.iter().any(|p| {
            matches!(&p.target, PrivTarget::Named { db_key: d, name_key: n, .. }
                if d == db_key && name_key.is_none_or(|k| k == n))
        })
    };
    let mut out = Vec::new();
    for d in dropped {
        match d {
            Dropped::Object {
                db,
                db_key,
                object,
                name_key,
                namespace,
                keep,
            } => {
                let relation = *namespace == Namespace::Relation;
                if has_rows(*object) {
                    out.push(if relation && *keep {
                        DropOp::KeepByName {
                            db: *db,
                            object: *object,
                            db_key: db_key.clone(),
                            name_key: name_key.clone(),
                        }
                    } else {
                        DropOp::ForgetObject {
                            db: *db,
                            object: *object,
                        }
                    });
                }
                if relation && !*keep && named_in(db_key, Some(name_key)) {
                    out.push(DropOp::ForgetNamed {
                        db_key: db_key.clone(),
                        name_key: Some(name_key.clone()),
                    });
                }
            }
            Dropped::Database { db, db_key, keep } => {
                let objects: BTreeSet<ObjectId> = rows
                    .iter()
                    .filter_map(|p| match p.target {
                        PrivTarget::Object { db: d, object }
                        | PrivTarget::Column { db: d, object, .. }
                            if d == *db =>
                        {
                            Some(object)
                        }
                        _ => None,
                    })
                    .collect();
                if *keep {
                    for object in objects {
                        let Some(o) = cat
                            .objects
                            .get(object)
                            .filter(|o| o.state == ObjectState::Active)
                        else {
                            continue;
                        };
                        out.push(if o.namespace == Namespace::Relation {
                            DropOp::KeepByName {
                                db: *db,
                                object,
                                db_key: db_key.clone(),
                                name_key: o.name_key.clone(),
                            }
                        } else {
                            DropOp::ForgetObject { db: *db, object }
                        });
                    }
                } else {
                    let db_rows = rows
                        .iter()
                        .any(|p| matches!(&p.target, PrivTarget::Database(n) if n == db_key));
                    if !objects.is_empty() || db_rows {
                        out.push(DropOp::ForgetDatabase {
                            db: *db,
                            db_key: db_key.clone(),
                        });
                    }
                    if named_in(db_key, None) {
                        out.push(DropOp::ForgetNamed {
                            db_key: db_key.clone(),
                            name_key: None,
                        });
                    }
                }
            }
        }
    }
    out
}

/// 39366 `ER_SPRIG_RLS_UNSUPPORTED`: the engine filters rows of `table`
/// and this gateway can't apply the filter yet.
#[must_use]
pub fn row_filter_unsupported(table: &str) -> SprigError {
    SprigError::new(
        sprig_common::SprigCode::RlsUnsupported,
        format!(
            "ER_SPRIG_RLS_UNSUPPORTED: the authorization engine filters rows of '{table}', \
             which this gateway can't apply yet"
        ),
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::OnceLock;
    use std::time::SystemTime;

    use sprig_catalog::cache::Loaded;
    use sprig_catalog::{
        DatabaseRow, ExtState, ObjectFlags, ObjectKind, ObjectRow, Snapshot, Versions,
    };
    use sprig_common::ids::{GroupId, TenantId};
    use sprig_settings::{AnySetting, Registry};

    use super::*;
    use crate::authz::privilege::ActionSet;
    use crate::authz::store::{AuthzRows, Grantee, PrivRow};

    fn registry() -> &'static Registry {
        static R: OnceLock<Registry> = OnceLock::new();
        R.get_or_init(|| {
            Registry::new([&crate::authz::settings::GRANTS_ON_DROP as &dyn AnySetting]).unwrap()
        })
    }

    fn db(id: u32, name: &str, state: DbState) -> DatabaseRow {
        DatabaseRow {
            db_id: DbId(id),
            name: name.to_owned(),
            name_key: name.to_owned(),
            default_charset: "utf8mb4".to_owned(),
            default_collation: "utf8mb4_uca1400_ai_ci".to_owned(),
            default_group_id: GroupId(0),
            state,
            options: serde_json::json!({ "shards": [1] }),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: None,
            changed_version: 1,
        }
    }

    fn obj(id: u64, db: u32, name: &str, kind: ObjectKind) -> ObjectRow {
        ObjectRow {
            object_id: ObjectId(id),
            db_id: DbId(db),
            namespace: kind.namespace(),
            name: name.to_owned(),
            name_key: name.to_owned(),
            kind,
            parent_id: None,
            state: ObjectState::Active,
            schema_version: 1,
            flags: ObjectFlags::empty(),
            pending_job_id: None,
            created_at: SystemTime::UNIX_EPOCH,
            changed_version: 1,
        }
    }

    const APP: UserId = UserId(7);

    fn grant(target: PrivTarget, a: &[Action]) -> PrivRow {
        PrivRow {
            priv_id: 0,
            grantee: Grantee::User(APP),
            target,
            actions: ActionSet::of(a),
        }
    }

    fn named(db: &str, t: &str, column: Option<&str>) -> PrivTarget {
        PrivTarget::Named {
            db_key: db.into(),
            name_key: t.into(),
            column: column.map(str::to_owned),
        }
    }

    /// `shop` (1) with table `orders` (10) and procedure `p` (11); the
    /// rows given.
    fn catalog(objects: Vec<ObjectRow>, privileges: Vec<PrivRow>) -> TenantCatalog {
        let state = AuthzState::new(AuthzRows {
            privileges,
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
                    databases: vec![db(1, "shop", DbState::Active)],
                    objects,
                    settings: Vec::new(),
                },
                ext: vec![(
                    crate::authz::store::MODULE,
                    Arc::new(state) as Arc<dyn ExtState>,
                )],
            },
        )
    }

    fn shop_objects() -> Vec<ObjectRow> {
        vec![
            obj(10, 1, "orders", ObjectKind::Table),
            obj(11, 1, "p", ObjectKind::Procedure),
        ]
    }

    fn orders_grants() -> Vec<PrivRow> {
        vec![
            grant(
                PrivTarget::Object {
                    db: DbId(1),
                    object: ObjectId(10),
                },
                &[Action::Select, Action::Create],
            ),
            grant(
                PrivTarget::Object {
                    db: DbId(1),
                    object: ObjectId(11),
                },
                &[Action::Execute],
            ),
            grant(PrivTarget::Database("shop".into()), &[Action::Insert]),
        ]
    }

    fn table_dropped(keep: bool) -> Dropped {
        Dropped::Object {
            db: DbId(1),
            db_key: "shop".into(),
            object: ObjectId(10),
            name_key: "orders".into(),
            namespace: Namespace::Relation,
            keep,
        }
    }

    #[test]
    fn keep_holds_table_grants_by_name_and_drop_deletes_them() {
        let cat = catalog(shop_objects(), orders_grants());
        let state = state_of(&cat);
        assert_eq!(
            drop_plan(state, &cat, &[table_dropped(true)]),
            vec![DropOp::KeepByName {
                db: DbId(1),
                object: ObjectId(10),
                db_key: "shop".into(),
                name_key: "orders".into(),
            }]
        );
        assert_eq!(
            drop_plan(state, &cat, &[table_dropped(false)]),
            vec![DropOp::ForgetObject {
                db: DbId(1),
                object: ObjectId(10),
            }]
        );
        // A routine's grants go with it in either mode.
        for keep in [true, false] {
            let p = Dropped::Object {
                db: DbId(1),
                db_key: "shop".into(),
                object: ObjectId(11),
                name_key: "p".into(),
                namespace: Namespace::Procedure,
                keep,
            };
            assert_eq!(
                drop_plan(state, &cat, &[p]),
                vec![DropOp::ForgetObject {
                    db: DbId(1),
                    object: ObjectId(11),
                }]
            );
        }
        // Nothing held: nothing written.
        let bare = catalog(shop_objects(), Vec::new());
        assert!(drop_plan(state_of(&bare), &bare, &[table_dropped(true)]).is_empty());
    }

    #[test]
    fn drop_mode_also_deletes_grants_held_by_that_name() {
        let mut rows = orders_grants();
        rows.push(grant(named("shop", "orders", None), &[Action::Update]));
        let cat = catalog(shop_objects(), rows);
        let plan = drop_plan(state_of(&cat), &cat, &[table_dropped(false)]);
        assert!(plan.contains(&DropOp::ForgetNamed {
            db_key: "shop".into(),
            name_key: Some("orders".into()),
        }));
        // keep leaves them.
        let plan = drop_plan(state_of(&cat), &cat, &[table_dropped(true)]);
        assert!(!plan.iter().any(|o| matches!(o, DropOp::ForgetNamed { .. })));
    }

    #[test]
    fn dropping_a_database() {
        let mut rows = orders_grants();
        rows.push(grant(named("shop", "old", None), &[Action::Select]));
        let cat = catalog(shop_objects(), rows);
        let state = state_of(&cat);
        let keep = drop_plan(
            state,
            &cat,
            &[Dropped::Database {
                db: DbId(1),
                db_key: "shop".into(),
                keep: true,
            }],
        );
        assert_eq!(
            keep,
            vec![
                DropOp::KeepByName {
                    db: DbId(1),
                    object: ObjectId(10),
                    db_key: "shop".into(),
                    name_key: "orders".into(),
                },
                DropOp::ForgetObject {
                    db: DbId(1),
                    object: ObjectId(11),
                },
            ]
        );
        let drop = drop_plan(
            state,
            &cat,
            &[Dropped::Database {
                db: DbId(1),
                db_key: "shop".into(),
                keep: false,
            }],
        );
        assert_eq!(
            drop,
            vec![
                DropOp::ForgetDatabase {
                    db: DbId(1),
                    db_key: "shop".into(),
                },
                DropOp::ForgetNamed {
                    db_key: "shop".into(),
                    name_key: None,
                },
            ]
        );
    }

    #[test]
    fn grants_held_by_name_apply_to_a_table_created_again() {
        // `orders` was dropped with keep: SELECT, INSERT and CREATE on it
        // by name, and UPDATE on its column `note`.
        let rows = vec![
            grant(
                named("shop", "orders", None),
                &[Action::Select, Action::Insert, Action::Create],
            ),
            grant(named("shop", "orders", Some("note")), &[Action::Update]),
        ];
        let engine = MysqlGrants;
        let subject = Subject {
            user: APP,
            roles: &[],
        };
        // Before it exists: CREATE on the name, the database is listed,
        // nothing else in it.
        let gone = catalog(Vec::new(), rows.clone());
        let g = engine.session(&gone, &subject);
        let by_name = Target::Named {
            db_key: "shop",
            name_key: "orders",
        };
        assert!(g.allows(Action::Create, by_name));
        assert!(g.allows(Action::Select, by_name));
        assert!(!g.allows(
            Action::Create,
            Target::Named {
                db_key: "shop",
                name_key: "other",
            }
        ));
        assert!(!g.allows(Action::Create, Target::Database { db_key: "shop" }));
        assert!(g.sees_database("shop", Some(DbId(1))));
        assert!(!g.sees_database("crm", None));
        // Created again (a new object id): the grants are on it.
        let again = catalog(vec![obj(20, 1, "orders", ObjectKind::Table)], rows);
        let g = engine.session(&again, &subject);
        let t = Target::Object {
            db_key: "shop",
            db: DbId(1),
            object: ObjectId(20),
        };
        assert!(g.allows(Action::Select, t));
        assert!(g.allows(Action::Insert, t));
        assert!(g.allows(Action::Update, t), "through the column grant");
        assert!(!g.allows(Action::Delete, t));
        let c = g.as_any().downcast_ref::<CompiledPrivileges>().unwrap();
        assert!(c.columns.contains_key(&(ObjectId(20), "note".into())));
        assert!(c.named.is_empty());
    }

    #[test]
    fn backend_grants_follow_the_name() {
        use crate::authz::backend::BackendTarget;
        let auth = TenantAuth::build(
            vec![crate::cache::tests::row(APP.0, "app", "%", 1)],
            &[],
            Vec::new(),
        );
        let acct = backend::account(TenantId(42), APP);
        let tables = |cat: &TenantCatalog| -> Vec<(String, ActionSet)> {
            MysqlGrants
                .backend_grants(cat, &auth)
                .get(&acct)
                .unwrap()
                .iter()
                .filter_map(|(t, a)| match t {
                    BackendTarget::Table(_, n) | BackendTarget::Column(_, n, _) => {
                        Some((n.clone(), *a))
                    }
                    _ => None,
                })
                .collect()
        };
        // Held by name with CREATE, no table: MariaDB gets the grant (it
        // accepts one on a missing table only with CREATE), so the user
        // can create the table again.
        let with_create = vec![grant(
            named("shop", "orders", None),
            &[Action::Select, Action::Create],
        )];
        let gone = catalog(Vec::new(), with_create.clone());
        assert_eq!(
            tables(&gone),
            vec![(
                "orders".to_owned(),
                ActionSet::of(&[Action::Select, Action::Create])
            )]
        );
        // Without CREATE: nothing until the table exists.
        let select_only = vec![grant(named("shop", "orders", None), &[Action::Select])];
        assert!(tables(&catalog(Vec::new(), select_only.clone())).is_empty());
        let again = vec![obj(20, 1, "orders", ObjectKind::Table)];
        assert_eq!(
            tables(&catalog(again, select_only)),
            vec![("orders".to_owned(), ActionSet::of(&[Action::Select]))]
        );
    }

    #[test]
    fn settings_privileges_map_as_before() {
        let cat = catalog(shop_objects(), orders_grants());
        let g = MysqlGrants.session(
            &cat,
            &Subject {
                user: APP,
                roles: &[],
            },
        );
        assert_eq!(
            g.settings(SettingsOp::Descendant),
            Err(SettingsDenied::TenantAdmin)
        );
        assert_eq!(
            g.settings(SettingsOp::Tenant { show: false }),
            Err(SettingsDenied::SettingsAdmin)
        );
        assert_eq!(g.settings(SettingsOp::Tenant { show: true }), Ok(()));
        assert_eq!(
            g.settings(SettingsOp::Bounds),
            Err(SettingsDenied::SettingsAdmin)
        );
        let db = |show| SettingsOp::Database {
            db_key: "shop",
            db: DbId(1),
            show,
        };
        assert_eq!(g.settings(db(true)), Ok(()));
        assert_eq!(g.settings(db(false)), Err(SettingsDenied::Target));
        assert!(g.row_filter(Target::Tenant, Action::Select).is_none());
        assert!(!g.unrestricted());
        assert!(!g.sees_all_databases());
        assert_eq!(MysqlGrants.name(), MYSQL_GRANTS);
        assert!(by_name(MYSQL_GRANTS).is_some());
        assert!(by_name("supabase").is_none());
    }
}
