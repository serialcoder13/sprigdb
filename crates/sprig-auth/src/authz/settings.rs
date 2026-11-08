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
