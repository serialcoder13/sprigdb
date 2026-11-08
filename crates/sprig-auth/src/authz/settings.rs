//! The `auth.*` keys chapter 29's R0 part reads (chapter 29,
//! "Configuration"). Scopes "tree": tenant (and every ancestor), dc and
//! global, resolved through the tenant's snapshot. All live.

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
