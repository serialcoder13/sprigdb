//! The `auth.*` keys this crate reads (chapter 14, "Configuration").
//! Scopes "tree": tenant (and every ancestor), dc and global, resolved
//! through the tenant's snapshot (chapter 08); "process": dc and global.
//! All live.
//!
//! Keys of later steps (tokens, second factors, impersonation, SCRAM-SHA-1,
//! issuers) are registered with the code that reads them.

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

macro_rules! auth_key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 14,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

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

auth_key!(
    /// `auth.scram_sha256_iterations`: PBKDF2 iterations of new
    /// SCRAM-SHA-256 verifiers (chapter 14, "SCRAM verifiers").
    SCRAM_SHA256_ITERATIONS: u64 = "auth.scram_sha256_iterations", 15_000, Scopes::TREE,
    HardBounds::range(5_000, 1_000_000),
    "PBKDF2 iterations of new SCRAM-SHA-256 verifiers"
);

auth_key!(
    /// `auth.scram_sha512_iterations`: PBKDF2 iterations of new
    /// SCRAM-SHA-512 verifiers.
    SCRAM_SHA512_ITERATIONS: u64 = "auth.scram_sha512_iterations", 15_000, Scopes::TREE,
    HardBounds::range(5_000, 1_000_000),
    "PBKDF2 iterations of new SCRAM-SHA-512 verifiers"
);

auth_key!(
    /// `auth.backfill_verifiers`: a successful login that saw the clear
    /// password (MySQL full authentication, HTTP Basic, Kafka PLAIN) stores
    /// the SCRAM verifiers its password lacks.
    BACKFILL_VERIFIERS: bool = "auth.backfill_verifiers", true, Scopes::TREE,
    HardBounds::NONE,
    "Store missing SCRAM verifiers at a login that saw the clear password"
);

auth_key!(
    /// `auth.failed_login_attempts`: consecutive failures before a
    /// temporary lock (MySQL's `FAILED_LOGIN_ATTEMPTS`); 0 turns tracking
    /// off.
    FAILED_LOGIN_ATTEMPTS: u64 = "auth.failed_login_attempts", 0, Scopes::TREE,
    HardBounds::range(0, 32_767),
    "Failed logins in a row before a temporary lock; 0 turns tracking off"
);

auth_key!(
    /// `auth.password_lock_minutes`: the temporary lock's length; -1 locks
    /// until `ACCOUNT UNLOCK`; 0 turns tracking off.
    PASSWORD_LOCK_MINUTES: i64 = "auth.password_lock_minutes", 0, Scopes::TREE,
    HardBounds::range(-1, 32_767 * 1440),
    "Temporary lock length in minutes; -1 until unlocked; 0 turns tracking off"
);

auth_key!(
    /// `auth.password_history`: MySQL's `password_history`.
    PASSWORD_HISTORY: u64 = "auth.password_history", 0, Scopes::TREE,
    HardBounds::range(0, 4_294_967_295),
    "Earlier passwords a new password may not repeat"
);

auth_key!(
    /// `auth.password_reuse_interval_days`: MySQL's
    /// `password_reuse_interval`.
    PASSWORD_REUSE_INTERVAL_DAYS: u64 = "auth.password_reuse_interval_days", 0, Scopes::TREE,
    HardBounds::range(0, 4_294_967_295),
    "Days within which a password may not be reused"
);

auth_key!(
    /// `auth.password_require_current`: the default of `PASSWORD REQUIRE
    /// CURRENT`.
    PASSWORD_REQUIRE_CURRENT: bool = "auth.password_require_current", false, Scopes::TREE,
    HardBounds::NONE,
    "Password changes must name the current password (REPLACE)"
);

/// The levels of `auth.password_policy`.
pub const PASSWORD_POLICY_LEVELS: &[&str] = &["off", "low", "medium", "strong"];

auth_key!(
    /// `auth.password_policy`: the levels of MySQL's `validate_password`.
    PASSWORD_POLICY: Cow<'static, str> = "auth.password_policy", Cow::Borrowed("off"), Scopes::TREE,
    HardBounds::one_of(PASSWORD_POLICY_LEVELS),
    "off, low, medium or strong: password strength checks"
);

auth_key!(
    /// `auth.delay_after_failures`: failures in a row (account and source
    /// address) before each further failure is delayed.
    DELAY_AFTER_FAILURES: u64 = "auth.delay_after_failures", 3, Scopes::TREE,
    HardBounds::range(0, 1_000_000),
    "Failed logins in a row (account and address) before failures are delayed; 0 never"
);

auth_key!(
    /// `auth.failure_delay_ms`: the first delay, doubled per further
    /// failure.
    FAILURE_DELAY_MS: u64 = "auth.failure_delay_ms", 1000, Scopes::TREE,
    HardBounds::range(0, 3_600_000),
    "First failure delay in milliseconds, doubling per further failure"
);

auth_key!(
    /// `auth.failure_delay_max_ms`: the longest failure delay.
    FAILURE_DELAY_MAX_MS: u64 = "auth.failure_delay_max_ms", 10_000, Scopes::TREE,
    HardBounds::range(0, 3_600_000),
    "Longest failure delay in milliseconds"
);

auth_key!(
    /// `auth.ip_failures_per_minute`: failed logins per source address per
    /// gateway in 60 s before the address is refused.
    IP_FAILURES_PER_MINUTE: u64 = "auth.ip_failures_per_minute", 30, Scopes::PROCESS,
    HardBounds::range(0, 1_000_000),
    "Failed logins per source address per minute before it is refused; 0 never"
);

auth_key!(
    /// `auth.ip_block_seconds`: how long such an address is refused.
    IP_BLOCK_SECONDS: u64 = "auth.ip_block_seconds", 300, Scopes::PROCESS,
    HardBounds::range(1, 86_400),
    "How long an address with too many failed logins is refused"
);

auth_key!(
    /// `auth.close_sessions_on_revoke`: close sessions of dropped or locked
    /// users at their next safe point.
    CLOSE_SESSIONS_ON_REVOKE: bool = "auth.close_sessions_on_revoke", true, Scopes::TREE,
    HardBounds::NONE,
    "Close sessions of dropped or locked users at their next safe point"
);
