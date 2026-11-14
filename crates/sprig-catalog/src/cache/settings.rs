//! The gateway cache's keys. All are live;
//! scopes dc and global.

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// `catalog.cache_max_tenants`.
pub static CACHE_MAX_TENANTS: Setting<u64> = Setting {
    key: "catalog.cache_max_tenants",
    default: 20_000,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 10_000_000),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "Tenants cached per gateway",
};
sprig_settings::register!(CACHE_MAX_TENANTS);

/// `catalog.cache_max_mb`.
pub static CACHE_MAX_MB: Setting<u64> = Setting {
    key: "catalog.cache_max_mb",
    default: 512,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 1_048_576),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "Catalog cache memory per gateway",
};
sprig_settings::register!(CACHE_MAX_MB);

/// `catalog.cache_ttl_seconds`.
pub static CACHE_TTL_SECONDS: Setting<u64> = Setting {
    key: "catalog.cache_ttl_seconds",
    default: 300,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 86_400),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "Backstop for missed watch events: re-read the directory entry after this long",
};
sprig_settings::register!(CACHE_TTL_SECONDS);

/// `catalog.version_wait_ms`.
pub static VERSION_WAIT_MS: Setting<u64> = Setting {
    key: "catalog.version_wait_ms",
    default: 2000,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 600_000),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "How long a loader waits for a version before switching to the writer or accepting a gap",
};
sprig_settings::register!(VERSION_WAIT_MS);
