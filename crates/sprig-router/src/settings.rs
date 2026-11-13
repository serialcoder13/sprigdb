//! Chapter 17's settings (`router.*`), declared next to the code (chapter
//! 03, "Declaring a key"), and [`RouterConfig`], what one statement reads.
//!
//! Only the keys the router uses are registered: R1's, and R3's
//! `router.gather.*` for gathered reads and `router.analytics.*` (with
//! chapter 35's `analytics.datafusion_memory_mb` and
//! `analytics.max_concurrent_per_gateway`) for analytic reads; the cluster-local fallback,
//! pull-out subqueries, backend statement caching and shadow mode wait for
//! their code. Scopes follow the chapter's table: **T** is tenant, dc and
//! global; **G** is dc and global (one value per gateway).

use sprig_settings::{Apply, HardBounds, Scopes, Setting, SettingCtx};

/// T: tenant, dc and global.
const TENANT_DC: Scopes = Scopes(0b11_0100);
/// G: dc and global.
const GATEWAY: Scopes = Scopes::PROCESS;

macro_rules! key {
    ($(#[$doc:meta])* $name:ident = $key:literal, $scopes:expr, $default:expr, $min:expr, $max:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<u64> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: false,
            owner: 17,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key! {
    /// `router.max_parallel_shards`.
    MAX_PARALLEL_SHARDS = "router.max_parallel_shards", TENANT_DC, 32, 1, 1024,
    "Shard queries one statement runs at once"
}
key! {
    /// `router.max_statement_memory_mb`.
    MAX_STATEMENT_MEMORY_MB = "router.max_statement_memory_mb", TENANT_DC, 256, 1, 65_536,
    "Gateway memory for one statement"
}
key! {
    /// `router.max_route_values`.
    MAX_ROUTE_VALUES = "router.max_route_values", TENANT_DC, 1024, 1, 1_000_000,
    "Key combinations routed before scattering"
}
key! {
    /// `router.lookup_batch_size`.
    LOOKUP_BATCH_SIZE = "router.lookup_batch_size", TENANT_DC, 256, 1, 65_536,
    "Keys per IN list in a lookup join"
}
key! {
    /// `router.stale_map_retries`.
    STALE_MAP_RETRIES = "router.stale_map_retries", GATEWAY, 2, 0, 10,
    "Retries after a stale shard map"
}
key! {
    /// `router.plan_cache_entries`.
    PLAN_CACHE_ENTRIES = "router.plan_cache_entries", GATEWAY, 10_000, 0, 10_000_000,
    "Cached plans per gateway"
}

key! {
    /// `router.gather.max_rows` (R3): rows a gathered read may copy; 0
    /// turns gathered reads off (the statements are refused with 39420 as
    /// before).
    GATHER_MAX_ROWS = "router.gather.max_rows", TENANT_DC, 100_000, 0, 100_000_000,
    "Rows a gathered read may copy (0: off)"
}
key! {
    /// `router.gather.max_concurrent` (R3): gathered reads one gateway
    /// runs at once (read when the first one runs).
    GATHER_MAX_CONCURRENT = "router.gather.max_concurrent", GATEWAY, 4, 1, 1024,
    "Gathered reads per gateway at once"
}

key! {
    /// `router.analytics.max_rows` (R3): rows an analytic read may read
    /// from the shards (chapter 17, "Analytic reads (R3)").
    ANALYTICS_MAX_ROWS = "router.analytics.max_rows", TENANT_DC, 50_000_000, 1, 10_000_000_000,
    "Rows an analytic read may read from the shards"
}
key! {
    /// `router.analytics.spill_mb` (R3): disk an analytic read's sorts and
    /// aggregates may spill to; 0 refuses (39421) instead of spilling.
    ANALYTICS_SPILL_MB = "router.analytics.spill_mb", TENANT_DC, 4096, 0, 1_048_576,
    "Disk an analytic read may spill to (0: none)"
}

/// `router.analytics.mode` (R3): `off`, `fallback` or `prefer` (chapter
/// 17, "Analytic reads (R3)", the selection rule).
pub static ANALYTICS_MODE: Setting<std::borrow::Cow<'static, str>> = Setting {
    key: "router.analytics.mode",
    default: std::borrow::Cow::Borrowed("fallback"),
    scopes: TENANT_DC,
    hard: HardBounds::one_of(&["off", "fallback", "prefer"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 17,
    doc: "off, fallback or prefer",
};
sprig_settings::register!(ANALYTICS_MODE);

/// `analytics.datafusion_memory_mb` (chapter 35's key, used from R3 by
/// chapter 17's analytic reads): DataFusion's memory pool per statement.
pub static DATAFUSION_MEMORY_MB: Setting<u64> = Setting {
    key: "analytics.datafusion_memory_mb",
    default: 1024,
    scopes: TENANT_DC,
    hard: HardBounds::range(16, 65_536),
    apply: Apply::Live,
    operator_only: false,
    owner: 35,
    doc: "Memory per DataFusion query",
};
sprig_settings::register!(DATAFUSION_MEMORY_MB);

/// `analytics.max_concurrent_per_gateway` (chapter 35's key): analytic
/// reads one gateway runs at once (read when the first one runs).
pub static ANALYTICS_MAX_CONCURRENT: Setting<u64> = Setting {
    key: "analytics.max_concurrent_per_gateway",
    default: 8,
    scopes: GATEWAY,
    hard: HardBounds::range(1, 1024),
    apply: Apply::Live,
    operator_only: false,
    owner: 35,
    doc: "Queries per gateway",
};
sprig_settings::register!(ANALYTICS_MAX_CONCURRENT);

/// `fk.lock_wait_seconds` (chapter 19, R3): how long a foreign-key probe
/// waits for a row lock (`LOCK IN SHARE MODE WAIT n`) before 1205.
pub static FK_LOCK_WAIT_SECONDS: Setting<u64> = Setting {
    key: "fk.lock_wait_seconds",
    default: 5,
    scopes: Scopes::DATA,
    hard: HardBounds::range(0, 31_536_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 19,
    doc: "Lock wait for foreign-key checks",
};
sprig_settings::register!(FK_LOCK_WAIT_SECONDS);

/// What the router reads for one tenant: chapter 17's keys, resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouterConfig {
    /// `router.max_parallel_shards`.
    pub max_parallel_shards: usize,
    /// `router.max_statement_memory_mb`.
    pub max_statement_memory_mb: u64,
    /// `router.max_route_values`.
    pub max_route_values: usize,
    /// `router.lookup_batch_size`.
    pub lookup_batch: usize,
    /// `router.stale_map_retries`.
    pub stale_map_retries: usize,
    /// `router.plan_cache_entries`.
    pub plan_cache_entries: usize,
    /// `router.gather.max_rows` (0: gathered reads off).
    pub gather_max_rows: u64,
    /// `router.gather.max_concurrent`.
    pub gather_max_concurrent: usize,
    /// `fk.lock_wait_seconds` (chapter 19).
    pub fk_lock_wait_seconds: u64,
    /// Analytic reads (`router.analytics.*`, `analytics.*`).
    pub analytics: crate::analytics::AnalyticsLimits,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            max_parallel_shards: to_usize(MAX_PARALLEL_SHARDS.default),
            max_statement_memory_mb: MAX_STATEMENT_MEMORY_MB.default,
            max_route_values: to_usize(MAX_ROUTE_VALUES.default),
            lookup_batch: to_usize(LOOKUP_BATCH_SIZE.default),
            stale_map_retries: to_usize(STALE_MAP_RETRIES.default),
            plan_cache_entries: to_usize(PLAN_CACHE_ENTRIES.default),
            gather_max_rows: GATHER_MAX_ROWS.default,
            gather_max_concurrent: to_usize(GATHER_MAX_CONCURRENT.default),
            fk_lock_wait_seconds: FK_LOCK_WAIT_SECONDS.default,
            analytics: crate::analytics::AnalyticsLimits {
                mode: crate::analytics::AnalyticsMode::parse(&ANALYTICS_MODE.default),
                memory_mb: DATAFUSION_MEMORY_MB.default,
                spill_mb: ANALYTICS_SPILL_MB.default,
                max_rows: ANALYTICS_MAX_ROWS.default,
                max_concurrent: to_usize(ANALYTICS_MAX_CONCURRENT.default),
            },
        }
    }
}

fn to_usize(v: u64) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

impl RouterConfig {
    /// Resolves every key with `get` (the caller's resolver: a
    /// `MemSettings`, or the gateway's live settings with the tenant's
    /// rows) for `ctx`.
    pub fn resolve(
        ctx: &SettingCtx<'_>,
        get: impl FnMut(&'static Setting<u64>, &SettingCtx<'_>) -> u64,
    ) -> Self {
        Self::resolve_with(ctx, get, |k, _| k.default.clone().into_owned())
    }

    /// [`Self::resolve`], with `text` resolving the text keys
    /// (`router.analytics.mode`).
    pub fn resolve_with(
        ctx: &SettingCtx<'_>,
        mut get: impl FnMut(&'static Setting<u64>, &SettingCtx<'_>) -> u64,
        mut text: impl FnMut(
            &'static Setting<std::borrow::Cow<'static, str>>,
            &SettingCtx<'_>,
        ) -> String,
    ) -> Self {
        Self {
            max_parallel_shards: to_usize(get(&MAX_PARALLEL_SHARDS, ctx)).max(1),
            max_statement_memory_mb: get(&MAX_STATEMENT_MEMORY_MB, ctx).max(1),
            max_route_values: to_usize(get(&MAX_ROUTE_VALUES, ctx)).max(1),
            lookup_batch: to_usize(get(&LOOKUP_BATCH_SIZE, ctx)).max(1),
            stale_map_retries: to_usize(get(&STALE_MAP_RETRIES, ctx)),
            plan_cache_entries: to_usize(get(&PLAN_CACHE_ENTRIES, ctx)),
            gather_max_rows: get(&GATHER_MAX_ROWS, ctx),
            gather_max_concurrent: to_usize(get(&GATHER_MAX_CONCURRENT, ctx)).max(1),
            fk_lock_wait_seconds: get(&FK_LOCK_WAIT_SECONDS, ctx),
            analytics: crate::analytics::AnalyticsLimits {
                mode: crate::analytics::AnalyticsMode::parse(&text(&ANALYTICS_MODE, ctx)),
                memory_mb: get(&DATAFUSION_MEMORY_MB, ctx),
                spill_mb: get(&ANALYTICS_SPILL_MB, ctx),
                max_rows: get(&ANALYTICS_MAX_ROWS, ctx).max(1),
                max_concurrent: to_usize(get(&ANALYTICS_MAX_CONCURRENT, ctx)).max(1),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::{DcId, TenantId};
    use sprig_settings::{Level, MemSettings, Registry, Value};

    use super::*;

    #[test]
    fn keys_are_registered_with_the_books_defaults_and_scopes() {
        let reg = Registry::global().unwrap();
        for (k, d) in [
            ("router.max_parallel_shards", 32),
            ("router.max_statement_memory_mb", 256),
            ("router.max_route_values", 1024),
            ("router.lookup_batch_size", 256),
            ("router.stale_map_retries", 2),
            ("router.plan_cache_entries", 10_000),
            ("router.gather.max_rows", 100_000),
            ("router.gather.max_concurrent", 4),
            ("fk.lock_wait_seconds", 5),
            ("router.analytics.max_rows", 50_000_000),
            ("router.analytics.spill_mb", 4096),
            ("analytics.datafusion_memory_mb", 1024),
            ("analytics.max_concurrent_per_gateway", 8),
        ] {
            let def = reg
                .get(k)
                .unwrap_or_else(|| panic!("{k} is not registered"));
            assert_eq!(def.default_value(), Value::Int(d), "{k}");
        }
        assert_eq!(RouterConfig::default().max_parallel_shards, 32);
    }

    #[test]
    fn a_tenant_value_wins_and_gateway_keys_ignore_tenants() {
        let reg = Registry::global().unwrap();
        let mut s = MemSettings::new(reg);
        let dc = DcId::new("dc-a").unwrap();
        let mut ctx = SettingCtx::process(&dc);
        ctx.tenant = Some(TenantId(7));
        s.set_value(
            Level::Tenant,
            &ctx,
            "router.max_parallel_shards",
            &Value::Int(4),
        )
        .unwrap();
        assert!(
            s.set_value(
                Level::Tenant,
                &ctx,
                "router.stale_map_retries",
                &Value::Int(5)
            )
            .is_err(),
            "router.stale_map_retries is per gateway"
        );
        s.set_value(Level::Dc, &ctx, "router.stale_map_retries", &Value::Int(5))
            .unwrap();
        let c = RouterConfig::resolve(&ctx, |k, cx| s.get(k, cx));
        assert_eq!(c.max_parallel_shards, 4);
        assert_eq!(c.stale_map_retries, 5);
        assert_eq!(c.max_statement_memory_mb, 256);
    }
}
