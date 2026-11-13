//! Chapter 20's `sharding.*` keys, declared next to the code (chapter 03,
//! "Declaring a key"), and [`ShardingSettings`], a typed snapshot for one
//! tenant.
//!
//! `sharding.min_shards` belongs to chapter 15 and is declared here until
//! the crate that owns pre-splitting reads it, as other crates do for keys
//! of chapters without code yet.

use std::borrow::Cow;
use std::time::Duration;

use sprig_settings::{Apply, HardBounds, MemSettings, Scopes, Setting, SettingCtx, TenantSettings};

use crate::settings::GB;

/// Tenant, cluster, dc and global.
const TENANT_OPS: Scopes = Scopes(0b11_1100);
/// Tenant, database, cluster, dc and global.
const DATA_OPS: Scopes = Scopes(0b11_1110);
/// Tenant, dc and global.
const TENANT_DC: Scopes = Scopes(0b11_0100);
/// Dc and global.
const PROCESS: Scopes = Scopes::PROCESS;
/// Tenant, database and global.
const TENANT_DB: Scopes = Scopes(0b10_0110);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $min:expr, $max:expr, $owner:literal, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: true,
            owner: $owner,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

/// `sharding.mode`: `on`, `recommend` (log only) or `off`.
pub static MODE: Setting<Cow<'static, str>> = Setting {
    key: "sharding.mode",
    default: Cow::Borrowed("on"),
    scopes: TENANT_OPS,
    hard: HardBounds::one_of(&["on", "approve", "recommend", "off"]),
    apply: Apply::Live,
    operator_only: true,
    owner: 20,
    doc: "Automatic split, merge and move: on, approve (held for approval), recommend (log only) or off",
};
sprig_settings::register!(MODE);

key! {
    /// `sharding.split_size_gb`.
    SPLIT_SIZE_GB: f64 = "sharding.split_size_gb", 32.0, DATA_OPS, 0, 65_536, 20,
    "Split a shard above this size"
}
key! {
    /// `sharding.split_write_share_pct`.
    SPLIT_WRITE_SHARE_PCT: u32 = "sharding.split_write_share_pct", 50, TENANT_OPS, 1, 100, 20,
    "Or above this share of a hot cluster's writes..."
}
key! {
    /// `sharding.split_sustain_minutes`.
    SPLIT_SUSTAIN_MINUTES: u64 = "sharding.split_sustain_minutes", 30, TENANT_OPS, 1, 10_080, 20,
    "...for this long"
}
key! {
    /// `sharding.merge_size_gb`.
    MERGE_SIZE_GB: f64 = "sharding.merge_size_gb", 4.0, DATA_OPS, 0, 65_536, 20,
    "Merge quiet neighbours both below this"
}
key! {
    /// `sharding.merge_cooldown_hours`.
    MERGE_COOLDOWN_HOURS: f64 = "sharding.merge_cooldown_hours", 24.0, TENANT_DC, 0, 8760, 20,
    "Minimum age of shards before they merge"
}
key! {
    /// `sharding.max_split_ways`.
    MAX_SPLIT_WAYS: u32 = "sharding.max_split_ways", 4, PROCESS, 2, 64, 20,
    "Most children from one size split"
}
key! {
    /// `sharding.max_jobs_per_tenant`.
    MAX_JOBS_PER_TENANT: u32 = "sharding.max_jobs_per_tenant", 2, TENANT_DC, 1, 64, 20,
    "Concurrent split, merge and move jobs"
}
key! {
    /// `sharding.check_interval_seconds`.
    CHECK_INTERVAL_SECONDS: u64 = "sharding.check_interval_seconds", 60, PROCESS, 1, 86_400, 20,
    "How often triggers are evaluated"
}
key! {
    /// `sharding.retire_hours`.
    RETIRE_HOURS: f64 = "sharding.retire_hours", 24.0, TENANT_DC, 0, 8760, 20,
    "Retired shards kept fenced before drop"
}
key! {
    /// `sharding.min_shards` (chapter 15).
    MIN_SHARDS: u32 = "sharding.min_shards", 1, TENANT_DB, 1, 4096, 15,
    "Pre-split count per group"
}

/// What a tenant's shards are allowed to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardingMode {
    /// Jobs run.
    On,
    /// Proposals are logged as recommendations; nothing runs.
    Recommend,
    /// Proposals wait for an approval (chapter 52), then run as under `on`.
    Approve,
    /// Nothing is evaluated.
    Off,
}

impl ShardingMode {
    /// From the key's text.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s {
            "recommend" => Self::Recommend,
            "approve" => Self::Approve,
            "off" => Self::Off,
            _ => Self::On,
        }
    }
}

/// The keys for one tenant (and its home DC), resolved.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ShardingSettings {
    /// `sharding.mode`.
    pub mode: ShardingMode,
    /// `split_size_gb` in bytes.
    pub split_bytes: f64,
    /// `split_write_share_pct` as a ratio.
    pub split_write_share: f64,
    /// `split_sustain_minutes`.
    pub sustain: Duration,
    /// `merge_size_gb` in bytes, kept below a quarter of the split size.
    pub merge_bytes: f64,
    /// `merge_cooldown_hours`.
    pub merge_cooldown: Duration,
    /// `max_split_ways`.
    pub max_split_ways: u32,
    /// `max_jobs_per_tenant`.
    pub max_jobs_per_tenant: u32,
    /// `check_interval_seconds`.
    pub check_interval: Duration,
    /// `retire_hours`.
    pub retire: Duration,
    /// `min_shards`.
    pub min_shards: u32,
    /// `mover.max_copies_per_cluster`.
    pub max_copies_per_cluster: u32,
}

/// One key over the etcd scopes and, when given, a tenant's own rows
/// (RL3, r2harden).
pub(crate) fn layered<T: sprig_settings::SettingValue>(
    s: &MemSettings<'_>,
    rows: Option<&TenantSettings>,
    k: &Setting<T>,
    ctx: &SettingCtx<'_>,
) -> T {
    match rows {
        Some(t) => t
            .resolve(s, k.key, ctx)
            .ok()
            .and_then(|r| T::from_value(&r.value))
            .unwrap_or_else(|| k.default.clone()),
        None => s.get(k, ctx),
    }
}

fn hours(h: f64) -> Duration {
    Duration::from_secs_f64((h * 3600.0).max(0.0))
}

impl ShardingSettings {
    /// The bands' rule (chapter 20, "Configuration"): the merge size stays
    /// below a quarter of the split size whatever is configured, so splits
    /// and merges can't chase each other.
    #[must_use]
    pub fn banded(mut self) -> Self {
        self.merge_bytes = self.merge_bytes.min(self.split_bytes / 4.0);
        self
    }

    /// Resolves the keys in `ctx` (a context with the tenant set).
    #[must_use]
    pub fn resolve(s: &MemSettings<'_>, ctx: &SettingCtx<'_>) -> Self {
        Self::resolve_with(s, None, ctx)
    }

    /// [`Self::resolve`] with the tenant's own catalog rows (tenant,
    /// database, ancestors) layered over the etcd scopes (RL3, r2harden):
    /// what a gateway would resolve.
    #[must_use]
    pub fn resolve_with(
        s: &MemSettings<'_>,
        rows: Option<&TenantSettings>,
        ctx: &SettingCtx<'_>,
    ) -> Self {
        Self {
            mode: ShardingMode::parse(&layered(s, rows, &MODE, ctx)),
            split_bytes: layered(s, rows, &SPLIT_SIZE_GB, ctx) * GB,
            split_write_share: f64::from(layered(s, rows, &SPLIT_WRITE_SHARE_PCT, ctx)) / 100.0,
            sustain: Duration::from_secs(layered(s, rows, &SPLIT_SUSTAIN_MINUTES, ctx) * 60),
            merge_bytes: layered(s, rows, &MERGE_SIZE_GB, ctx) * GB,
            merge_cooldown: hours(layered(s, rows, &MERGE_COOLDOWN_HOURS, ctx)),
            max_split_ways: layered(s, rows, &MAX_SPLIT_WAYS, ctx),
            max_jobs_per_tenant: layered(s, rows, &MAX_JOBS_PER_TENANT, ctx),
            check_interval: Duration::from_secs(layered(s, rows, &CHECK_INTERVAL_SECONDS, ctx)),
            retire: hours(layered(s, rows, &RETIRE_HOURS, ctx)),
            min_shards: layered(s, rows, &MIN_SHARDS, ctx),
            max_copies_per_cluster: 2,
        }
        .banded()
    }
}

impl Default for ShardingSettings {
    fn default() -> Self {
        Self {
            mode: ShardingMode::On,
            split_bytes: SPLIT_SIZE_GB.default * GB,
            split_write_share: f64::from(SPLIT_WRITE_SHARE_PCT.default) / 100.0,
            sustain: Duration::from_secs(SPLIT_SUSTAIN_MINUTES.default * 60),
            merge_bytes: MERGE_SIZE_GB.default * GB,
            merge_cooldown: hours(MERGE_COOLDOWN_HOURS.default),
            max_split_ways: MAX_SPLIT_WAYS.default,
            max_jobs_per_tenant: MAX_JOBS_PER_TENANT.default,
            check_interval: Duration::from_secs(CHECK_INTERVAL_SECONDS.default),
            retire: hours(RETIRE_HOURS.default),
            min_shards: MIN_SHARDS.default,
            max_copies_per_cluster: 2,
        }
        .banded()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::float_cmp)]

    use sprig_common::ids::{DcId, TenantId};
    use sprig_settings::Registry;

    use super::*;

    #[test]
    fn keys_are_registered_with_the_books_defaults() {
        let reg = Registry::global().unwrap();
        for k in [
            "sharding.mode",
            "sharding.split_size_gb",
            "sharding.merge_size_gb",
            "sharding.max_jobs_per_tenant",
            "sharding.min_shards",
        ] {
            assert!(reg.get(k).is_some(), "{k}");
        }
        let d = ShardingSettings::default();
        assert_eq!(d.split_bytes, 32.0 * GB);
        assert_eq!(d.merge_bytes, 4.0 * GB);
        assert_eq!(d.merge_cooldown, Duration::from_secs(24 * 3600));
        let dc = DcId::new("dc-a").unwrap();
        let ctx = SettingCtx {
            dc: &dc,
            cluster: None,
            tenant: Some(TenantId(1)),
            ancestry: &[],
            db: None,
            object: None,
        };
        let mem = MemSettings::new(reg);
        assert_eq!(ShardingSettings::resolve(&mem, &ctx), d);
    }

    #[test]
    fn the_merge_band_stays_under_a_quarter_of_the_split_size() {
        let s = ShardingSettings {
            split_bytes: 8.0 * GB,
            merge_bytes: 4.0 * GB,
            ..ShardingSettings::default()
        }
        .banded();
        assert_eq!(s.merge_bytes, 2.0 * GB);
    }
}
