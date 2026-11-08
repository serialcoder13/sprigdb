//! The `auth.*` keys this crate reads so far (chapter 14,
//! "Configuration"). Scopes "tree": tenant (and every ancestor), dc and
//! global, resolved through the tenant's snapshot (chapter 08). All live.
//!
//! Chapter 14 step 8 registers the rest of the table.

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// `auth.require_tls`: refuse users who connected without TLS (check 3d).
pub static REQUIRE_TLS: Setting<bool> = Setting {
    key: "auth.require_tls",
    default: false,
    scopes: Scopes::TREE,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "Refuse users who connected without TLS",
};
sprig_settings::register!(REQUIRE_TLS);

/// `auth.default_password_lifetime_days`: MySQL's
/// `default_password_lifetime`; 0 never expires.
pub static DEFAULT_PASSWORD_LIFETIME_DAYS: Setting<u64> = Setting {
    key: "auth.default_password_lifetime_days",
    default: 0,
    scopes: Scopes::TREE,
    hard: HardBounds::range(0, 65_535),
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "Password lifetime in days for accounts without their own; 0 never expires",
};
sprig_settings::register!(DEFAULT_PASSWORD_LIFETIME_DAYS);

/// `auth.caching_sha2_rounds`: SHA-256 crypt rounds of new verifiers.
pub static CACHING_SHA2_ROUNDS: Setting<u64> = Setting {
    key: "auth.caching_sha2_rounds",
    default: 5000,
    scopes: Scopes::TREE,
    hard: HardBounds::range(5000, 4_095_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "SHA-256 crypt rounds of new caching_sha2_password verifiers",
};
sprig_settings::register!(CACHING_SHA2_ROUNDS);

/// `auth.mysql_native_password`: store and accept `mysql_native_password`
/// verifiers.
pub static MYSQL_NATIVE_PASSWORD: Setting<bool> = Setting {
    key: "auth.mysql_native_password",
    default: false,
    scopes: Scopes::TREE,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "Store and accept mysql_native_password verifiers",
};
sprig_settings::register!(MYSQL_NATIVE_PASSWORD);
