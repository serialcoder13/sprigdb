//! The `auth.*` keys chapter 29's R0 part reads (chapter 29,
//! "Configuration"), resolved through the tenant's snapshot. Scopes
//! "tree" (tenant and every ancestor, dc, global), except
//! `auth.grants_on_drop` (database, tenant, global).

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// `auth.activate_all_roles_on_login`: MySQL's variable; every granted
/// role is active from login, not only the default roles.
pub static ACTIVATE_ALL_ROLES_ON_LOGIN: Setting<bool> = Setting {
    key: "auth.activate_all_roles_on_login",
    default: false,
    scopes: Scopes::TREE,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 29,
    doc: "Activate every granted role at login (MySQL's activate_all_roles_on_login)",
};
sprig_settings::register!(ACTIVATE_ALL_ROLES_ON_LOGIN);

/// `auth.unsafe_privilege_action`: what `GRANT` does with a server-wide
/// privilege (`SUPER`, `FILE`, ...): `warn` drops it with warning 39350,
/// `error` refuses the statement with 1227.
pub static UNSAFE_PRIVILEGE_ACTION: Setting<Cow<'static, str>> = Setting {
    key: "auth.unsafe_privilege_action",
    default: Cow::Borrowed("warn"),
    scopes: Scopes::TREE,
    hard: HardBounds::one_of(&["warn", "error"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 29,
    doc: "warn (drop server-wide privileges with 39350) or error (refuse with 1227)",
};
sprig_settings::register!(UNSAFE_PRIVILEGE_ACTION);

/// `auth.partial_revokes`: MySQL's `partial_revokes` (R4). On, a
/// database-level `REVOKE` of a privilege the grantee holds at `*.*`
/// stores a restriction (shown as `REVOKE ... ON db.*`); off, it fails
/// with 1141, as MySQL 8.4 does by default.
pub static PARTIAL_REVOKES: Setting<bool> = Setting {
    key: "auth.partial_revokes",
    default: false,
    scopes: Scopes::TREE,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 29,
    doc: "MySQL's partial_revokes: REVOKE on db.* of what *.* gives becomes a restriction",
};
sprig_settings::register!(PARTIAL_REVOKES);

/// `auth.authorizer`: the authorization engine of the tenant's sessions
/// ([`super::authorizer`]). Only `mysql_grants` (MySQL 8.4's grant model)
/// exists; the bound grows as engines are added. Read when a session
/// first compiles its privileges, so a change applies to new sessions.
pub static AUTHORIZER: Setting<Cow<'static, str>> = Setting {
    key: "auth.authorizer",
    default: Cow::Borrowed(super::authorizer::MYSQL_GRANTS),
    scopes: Scopes::TREE,
    hard: HardBounds::one_of(&[super::authorizer::MYSQL_GRANTS]),
    apply: Apply::NewSessions,
    operator_only: false,
    owner: 29,
    doc: "Authorization engine of the tenant's sessions: mysql_grants (MySQL's grant model)",
};
sprig_settings::register!(AUTHORIZER);

/// The scopes of [`GRANTS_ON_DROP`]: database, tenant (and every
/// ancestor) and global; no dc, since whether grants outlive a table
/// shouldn't depend on where the statement ran.
const DB_TENANT_GLOBAL: Scopes = Scopes(0b10_0110);

/// `auth.grants_on_drop`: what `DROP TABLE`, `DROP VIEW` and `DROP
/// DATABASE` do with grants on what they drop. `keep` (MySQL's
/// behaviour): table and column grants are kept by name and apply again
/// to a table created later with the same name (database grants are by
/// name anyway). `drop`: they are deleted with the object. Read at the
/// dropped object's database. Routine grants always go with the routine.
pub static GRANTS_ON_DROP: Setting<Cow<'static, str>> = Setting {
    key: "auth.grants_on_drop",
    default: Cow::Borrowed("keep"),
    scopes: DB_TENANT_GLOBAL,
    hard: HardBounds::one_of(&["keep", "drop"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 29,
    doc: "keep (grants survive DROP TABLE / DROP DATABASE and apply again to a re-created table, as MySQL) or drop (deleted with it)",
};
sprig_settings::register!(GRANTS_ON_DROP);

/// `rls.backend_enforcement` (chapter 29, "Inside MariaDB"): what MariaDB
/// enforces of row-level security besides the gateway's rewrite. `views`:
/// write checks (triggers) and policy views, with the accounts of bound
/// users reading the tables only through the views; `triggers`: write
/// checks only; `off`: neither (the rewrite alone). Read at the protected
/// table's database.
pub static RLS_BACKEND_ENFORCEMENT: Setting<Cow<'static, str>> = Setting {
    key: "rls.backend_enforcement",
    default: Cow::Borrowed("views"),
    scopes: DB_TENANT_GLOBAL,
    hard: HardBounds::one_of(&["views", "triggers", "off"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 29,
    doc: "views (write checks and policy views), triggers (write checks only) or off (the gateway's rewrite alone)",
};
sprig_settings::register!(RLS_BACKEND_ENFORCEMENT);

/// `rls.inline_subtree_max` (chapter 29, "What a policy may reference"):
/// the most tenant ids `SPRIG_TENANT_IN_SUBTREE` and
/// `SPRIG_TENANT_MANAGED` inline into a statement; a larger set is
/// refused (39366) until `__sprig_subtree` exists.
pub static RLS_INLINE_SUBTREE_MAX: Setting<u64> = Setting {
    key: "rls.inline_subtree_max",
    default: 1000,
    scopes: Scopes::TREE,
    hard: HardBounds::range(1, 100_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 29,
    doc: "Tenant ids inlined for SPRIG_TENANT_IN_SUBTREE and SPRIG_TENANT_MANAGED",
};
sprig_settings::register!(RLS_INLINE_SUBTREE_MAX);

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::{DbId, DcId, TenantId};
    use sprig_settings::{AnySetting, Level, MemSettings, Registry, SettingCtx, Value};

    use super::*;

    #[test]
    fn grants_on_drop_resolves_database_over_tenant_over_global() {
        let reg = Registry::new([&GRANTS_ON_DROP as &dyn AnySetting, &AUTHORIZER]).unwrap();
        let mut m = MemSettings::new(&reg);
        let dc = DcId::new("dc-a").unwrap();
        let tenant = SettingCtx::process(&dc).for_tenant(TenantId(42), &[]);
        let shop = tenant.with_db(DbId(1));
        let crm = tenant.with_db(DbId(2));
        let drop = Value::Str("drop".into());
        let keep = Value::Str("keep".into());
        let key = GRANTS_ON_DROP.key;
        // The default is MySQL's.
        assert_eq!(m.get(&GRANTS_ON_DROP, &shop), "keep");
        m.set_value(Level::Global, &shop, key, &drop).unwrap();
        assert_eq!(m.get(&GRANTS_ON_DROP, &crm), "drop");
        m.set_value(Level::Tenant, &shop, key, &keep).unwrap();
        assert_eq!(m.get(&GRANTS_ON_DROP, &crm), "keep");
        m.set_value(Level::Database, &shop, key, &drop).unwrap();
        assert_eq!(m.get(&GRANTS_ON_DROP, &shop), "drop");
        assert_eq!(m.get(&GRANTS_ON_DROP, &crm), "keep");
        // Not per DC, and only the two values.
        assert!(m.set_value(Level::Dc, &shop, key, &drop).is_err());
        assert!(
            m.set_value(Level::Tenant, &shop, key, &Value::Str("purge".into()))
                .is_err()
        );
        // Only the MySQL engine exists.
        assert_eq!(m.get(&AUTHORIZER, &tenant), "mysql_grants");
        assert!(
            m.set_value(
                Level::Tenant,
                &tenant,
                AUTHORIZER.key,
                &Value::Str("supabase".into())
            )
            .is_err()
        );
    }
}

/// `auth.reconcile_interval_seconds` (chapter 29, "Reconciler"): how
/// often a gateway compares a tenant's backend grants on a cluster with
/// MariaDB's even when nothing it watches changed (the drift sweep; also
/// catches a placement change it missed).
pub static RECONCILE_INTERVAL_SECONDS: Setting<u64> = Setting {
    key: "auth.reconcile_interval_seconds",
    default: 300,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(10, 86_400),
    apply: Apply::Live,
    operator_only: true,
    owner: 29,
    doc: "Drift sweep of backend grants per tenant and cluster, seconds",
};
sprig_settings::register!(RECONCILE_INTERVAL_SECONDS);
