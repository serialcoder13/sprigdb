//! Chapter 11's settings, declared next to the code (chapter 03, "Declaring
//! a key"), and typed snapshots of them: [`BalancerSettings`] for a DC,
//! [`ClusterLimits`] per cluster and [`TenantKnobs`] per tenant.
//!
//! Three keys belong to other chapters and are declared here until their
//! crates exist, as `sprig-controller` does for the mover's slot key:
//! `migration.auto_size_limit_gb` and `migration.max_cooldown_days`
//! (chapter 24) and `recovery.stability_minutes` (chapter 25).

use std::borrow::Cow;
use std::time::Duration;

use sprig_settings::{Apply, HardBounds, MemSettings, Scopes, Setting, SettingCtx};

/// Cluster, dc and global.
const OPS: Scopes = Scopes::OPERATIONAL;
/// Dc and global.
const PROCESS: Scopes = Scopes::PROCESS;
/// Tenant, cluster, dc and global.
const TENANT_OPS: Scopes = Scopes(0b11_1100);
/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);
/// Tenant, dc and global.
const TENANT_DC_GLOBAL: Scopes = Scopes(0b11_0100);

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

/// `balancer.mode`: `auto`, `recommend`, `dry_run` or `off`.
pub static MODE: Setting<Cow<'static, str>> = Setting {
    key: "balancer.mode",
    default: Cow::Borrowed("auto"),
    scopes: TENANT_OPS,
    hard: HardBounds::one_of(&["auto", "recommend", "dry_run", "off"]),
    apply: Apply::Live,
    operator_only: true,
    owner: 11,
    doc: "auto, recommend, dry_run or off",
};
sprig_settings::register!(MODE);

/// `balancer.provision_mode`: `recommend` or `auto` (R2).
pub static PROVISION_MODE: Setting<Cow<'static, str>> = Setting {
    key: "balancer.provision_mode",
    default: Cow::Borrowed("recommend"),
    scopes: PROCESS,
    hard: HardBounds::one_of(&["recommend", "auto"]),
    apply: Apply::Live,
    operator_only: true,
    owner: 11,
    doc: "recommend or auto (R2): who acts on add_cluster and add_replica",
};
sprig_settings::register!(PROVISION_MODE);

/// `placement.least_busy_weights`.
pub static LEAST_BUSY_WEIGHTS: Setting<Cow<'static, str>> = Setting {
    key: "placement.least_busy_weights",
    default: Cow::Borrowed("storage=0.4,cpu=0.4,connections=0.2"),
    scopes: PROCESS,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: true,
    owner: 11,
    doc: "Spare-capacity weights when no cluster clearly wins (also across DCs)",
};
sprig_settings::register!(LEAST_BUSY_WEIGHTS);

key! {
    /// `balancer.interval_seconds`.
    INTERVAL_SECONDS: u64 = "balancer.interval_seconds", 60, PROCESS, 5, 86_400, 11,
    "Planning period"
}
key! {
    /// `balancer.window_minutes`.
    WINDOW_MINUTES: u64 = "balancer.window_minutes", 30, PROCESS, 1, 10_080, 11,
    "Load EWMA window"
}
key! {
    /// `balancer.load_report_seconds`.
    LOAD_REPORT_SECONDS: u64 = "balancer.load_report_seconds", 10, PROCESS, 1, 3600, 11,
    "Gateway load report period"
}
key! {
    /// `balancer.high_pct`.
    HIGH_PCT: u32 = "balancer.high_pct", 80, OPS, 1, 100, 11,
    "A cluster above this is hot"
}
key! {
    /// `balancer.target_pct`.
    TARGET_PCT: u32 = "balancer.target_pct", 70, OPS, 1, 100, 11,
    "Moves keep targets at or below this"
}
key! {
    /// `balancer.spread_pct`.
    SPREAD_PCT: u32 = "balancer.spread_pct", 25, PROCESS, 1, 100, 11,
    "Fullest minus emptiest that triggers a move"
}
key! {
    /// `balancer.sustain_minutes`.
    SUSTAIN_MINUTES: u64 = "balancer.sustain_minutes", 30, PROCESS, 0, 10_080, 11,
    "Conditions must hold this long"
}
key! {
    /// `balancer.min_gain_pct`.
    MIN_GAIN_PCT: u32 = "balancer.min_gain_pct", 1, PROCESS, 0, 100, 11,
    "Smallest gain worth a move, in points"
}
key! {
    /// `balancer.shard_cooldown_hours`.
    SHARD_COOLDOWN_HOURS: u64 = "balancer.shard_cooldown_hours", 24, TENANT_OPS, 0, 8760, 11,
    "Between moves of one shard"
}
key! {
    /// `balancer.cluster_cooldown_minutes`.
    CLUSTER_COOLDOWN_MINUTES: u64 = "balancer.cluster_cooldown_minutes", 30, OPS, 0, 10_080, 11,
    "Between moves off one cluster"
}
key! {
    /// `balancer.max_concurrent_moves_per_dc`.
    MAX_CONCURRENT_MOVES_PER_DC: u32 = "balancer.max_concurrent_moves_per_dc", 4, PROCESS, 0, 1000, 11,
    "Moves at once per DC"
}
key! {
    /// `balancer.max_moves_per_tenant_per_day`.
    MAX_MOVES_PER_TENANT_PER_DAY: u32 = "balancer.max_moves_per_tenant_per_day", 2, TENANT_OPS, 0, 1000, 11,
    "Moves per tenant per day"
}
key! {
    /// `balancer.storage_hard_pct`.
    STORAGE_HARD_PCT: u32 = "balancer.storage_hard_pct", 90, OPS, 1, 100, 11,
    "Hard storage ceiling"
}
key! {
    /// `balancer.cowrite_min_per_hour`.
    COWRITE_MIN_PER_HOUR: u64 = "balancer.cowrite_min_per_hour", 1, TENANT_OPS, 0, 1_000_000_000, 11,
    "Co-writes that bind groups"
}
key! {
    /// `balancer.weight_cowrite`.
    WEIGHT_COWRITE: f64 = "balancer.weight_cowrite", 0.30, PROCESS, 0, 100, 11,
    "Score weight, split co-writes"
}
key! {
    /// `balancer.weight_xa`.
    WEIGHT_XA: f64 = "balancer.weight_xa", 0.20, PROCESS, 0, 100, 11,
    "Score weight, missing XA"
}
key! {
    /// `balancer.weight_rpo`.
    WEIGHT_RPO: f64 = "balancer.weight_rpo", 0.20, PROCESS, 0, 100, 11,
    "Score weight, RPO mismatch"
}
key! {
    /// `balancer.new_tenant_load`, in GB of storage.
    NEW_TENANT_LOAD: f64 = "balancer.new_tenant_load", 1.0, PROCESS, 0, 1_000_000, 11,
    "Predicted storage of a new tenant, GB"
}
key! {
    /// `placement.clear_win_margin`.
    CLEAR_WIN_MARGIN: f64 = "placement.clear_win_margin", 0.05, PROCESS, 0, 10, 11,
    "Score lead that makes a clear winner"
}
key! {
    /// `placement.max_tables_per_cluster`.
    MAX_TABLES_PER_CLUSTER: u64 = "placement.max_tables_per_cluster", 75_000, OPS, 1, 100_000_000, 11,
    "Tables per cluster, side tables included: the cap new shards respect"
}
key! {
    /// `placement.max_shard_schemas_per_cluster`.
    MAX_SHARD_SCHEMAS_PER_CLUSTER: u64 = "placement.max_shard_schemas_per_cluster", 15_000, OPS, 1, 10_000_000, 11,
    "Shard schemas per cluster, a backstop to the table cap"
}
key! {
    /// `placement.max_schema_creates_per_minute`.
    MAX_SCHEMA_CREATES_PER_MINUTE: u32 = "placement.max_schema_creates_per_minute", 300, OPS, 1, 1_000_000, 11,
    "New shard schemas created on one cluster per minute"
}
key! {
    /// `decisions.retention_days`.
    DECISIONS_RETENTION_DAYS: u32 = "decisions.retention_days", 180, TENANT_GLOBAL, 1, 36_500, 11,
    "Decision log retention"
}
key! {
    /// `migration.auto_size_limit_gb` (chapter 24): larger shards and
    /// tenants need approval to move.
    AUTO_SIZE_LIMIT_GB: u64 = "migration.auto_size_limit_gb", 200, TENANT_DC_GLOBAL, 0, 1_000_000, 24,
    "Larger tenants and shards need approval to move"
}
key! {
    /// `migration.max_cooldown_days` (chapter 24): the cap on a doubled
    /// cooldown.
    MAX_COOLDOWN_DAYS: u64 = "migration.max_cooldown_days", 7, TENANT_DC_GLOBAL, 0, 3650, 24,
    "Cooldown cap"
}
key! {
    /// `recovery.stability_minutes` (chapter 25).
    RECOVERY_STABILITY_MINUTES: u64 = "recovery.stability_minutes", 60, OPS, 0, 10_080, 25,
    "Healthy time before a recovered cluster receives shards by rebalancing"
}

/// `balancer.mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BalancerMode {
    /// Plans run as mover jobs.
    Auto,
    /// Plans are written as `recommended` decisions; no job.
    Recommend,
    /// Plans are written as `dry_run` decisions; nothing runs.
    DryRun,
    /// No planning.
    Off,
}

impl BalancerMode {
    /// Parses the setting's text; anything unknown is `Off`, the safe side.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s {
            "auto" => Self::Auto,
            "recommend" => Self::Recommend,
            "dry_run" => Self::DryRun,
            _ => Self::Off,
        }
    }

    /// The setting's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Recommend => "recommend",
            Self::DryRun => "dry_run",
            Self::Off => "off",
        }
    }
}

/// `placement.least_busy_weights`, parsed.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LeastBusyWeights {
    /// Weight of spare storage.
    pub storage: f64,
    /// Weight of spare CPU (busy time or pressure).
    pub cpu: f64,
    /// Weight of spare connections.
    pub connections: f64,
}

impl Default for LeastBusyWeights {
    fn default() -> Self {
        Self {
            storage: 0.4,
            cpu: 0.4,
            connections: 0.2,
        }
    }
}

impl LeastBusyWeights {
    /// Parses `storage=0.4,cpu=0.4,connections=0.2`; missing names keep
    /// their default, and a text that doesn't parse (or sums to zero) gives
    /// the defaults.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        let mut w = Self::default();
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let Some((k, v)) = part.split_once('=') else {
                return Self::default();
            };
            let Ok(v) = v.trim().parse::<f64>() else {
                return Self::default();
            };
            if !v.is_finite() || v < 0.0 {
                return Self::default();
            }
            match k.trim() {
                "storage" => w.storage = v,
                "cpu" => w.cpu = v,
                "connections" => w.connections = v,
                _ => return Self::default(),
            }
        }
        if w.storage + w.cpu + w.connections <= 0.0 {
            return Self::default();
        }
        w
    }
}

/// The DC-level keys, resolved once per planning run.
#[derive(Debug, Clone, PartialEq)]
pub struct BalancerSettings {
    /// `balancer.mode` at DC level (tenants may override).
    pub mode: BalancerMode,
    /// Planning period.
    pub interval: Duration,
    /// Load EWMA window.
    pub window: Duration,
    /// Gateway report period.
    pub load_report: Duration,
    /// `spread_pct` as a ratio.
    pub spread: f64,
    /// Conditions must hold this long.
    pub sustain: Duration,
    /// `min_gain_pct` as a ratio.
    pub min_gain: f64,
    /// Moves at once per DC.
    pub max_concurrent_moves_per_dc: u32,
    /// Score weights.
    pub weight_cowrite: f64,
    /// Score weight, missing XA.
    pub weight_xa: f64,
    /// Score weight, RPO mismatch.
    pub weight_rpo: f64,
    /// Predicted storage of a new tenant, bytes.
    pub new_tenant_storage_bytes: f64,
    /// Spare-capacity weights.
    pub least_busy: LeastBusyWeights,
    /// Score lead that makes a clear winner.
    pub clear_win_margin: f64,
    /// `provision_mode` is `auto`.
    pub provision_auto: bool,
}

impl Default for BalancerSettings {
    fn default() -> Self {
        Self {
            mode: BalancerMode::parse(&MODE.default),
            interval: Duration::from_secs(INTERVAL_SECONDS.default),
            window: Duration::from_secs(WINDOW_MINUTES.default * 60),
            load_report: Duration::from_secs(LOAD_REPORT_SECONDS.default),
            spread: pct(SPREAD_PCT.default),
            sustain: Duration::from_secs(SUSTAIN_MINUTES.default * 60),
            min_gain: pct(MIN_GAIN_PCT.default),
            max_concurrent_moves_per_dc: MAX_CONCURRENT_MOVES_PER_DC.default,
            weight_cowrite: WEIGHT_COWRITE.default,
            weight_xa: WEIGHT_XA.default,
            weight_rpo: WEIGHT_RPO.default,
            new_tenant_storage_bytes: NEW_TENANT_LOAD.default * GB,
            least_busy: LeastBusyWeights::parse(&LEAST_BUSY_WEIGHTS.default),
            clear_win_margin: CLEAR_WIN_MARGIN.default,
            provision_auto: false,
        }
    }
}

/// Bytes in a GB (the book's GB is 2^30 bytes).
pub const GB: f64 = 1_073_741_824.0;

fn pct(p: u32) -> f64 {
    f64::from(p) / 100.0
}

impl BalancerSettings {
    /// Resolves the DC-level keys in `ctx` (a process context).
    #[must_use]
    pub fn resolve(s: &MemSettings<'_>, ctx: &SettingCtx<'_>) -> Self {
        Self {
            mode: BalancerMode::parse(&s.get(&MODE, ctx)),
            interval: Duration::from_secs(s.get(&INTERVAL_SECONDS, ctx)),
            window: Duration::from_secs(s.get(&WINDOW_MINUTES, ctx) * 60),
            load_report: Duration::from_secs(s.get(&LOAD_REPORT_SECONDS, ctx)),
            spread: pct(s.get(&SPREAD_PCT, ctx)),
            sustain: Duration::from_secs(s.get(&SUSTAIN_MINUTES, ctx) * 60),
            min_gain: pct(s.get(&MIN_GAIN_PCT, ctx)),
            max_concurrent_moves_per_dc: s.get(&MAX_CONCURRENT_MOVES_PER_DC, ctx),
            weight_cowrite: s.get(&WEIGHT_COWRITE, ctx),
            weight_xa: s.get(&WEIGHT_XA, ctx),
            weight_rpo: s.get(&WEIGHT_RPO, ctx),
            new_tenant_storage_bytes: s.get(&NEW_TENANT_LOAD, ctx) * GB,
            least_busy: LeastBusyWeights::parse(&s.get(&LEAST_BUSY_WEIGHTS, ctx)),
            clear_win_margin: s.get(&CLEAR_WIN_MARGIN, ctx),
            provision_auto: s.get(&PROVISION_MODE, ctx) == "auto",
        }
    }
}

/// The per-cluster keys ("ops" scopes).
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ClusterLimits {
    /// `high_pct` as a ratio.
    pub high: f64,
    /// `target_pct` as a ratio.
    pub target: f64,
    /// `storage_hard_pct` as a ratio.
    pub storage_hard: f64,
    /// Between moves off this cluster, seconds.
    pub cooldown_secs: u64,
    /// The table cap.
    pub max_tables: u64,
    /// The shard-schema cap.
    pub max_schemas: u64,
    /// New shard schemas per minute.
    pub max_schema_creates_per_minute: u32,
    /// Healthy time after recovery before rebalancing targets it, seconds.
    pub stability_secs: u64,
    /// `mover.max_copies_per_cluster`: moves reading or writing it at once.
    pub copy_slots: u32,
}

impl Default for ClusterLimits {
    fn default() -> Self {
        Self {
            high: pct(HIGH_PCT.default),
            target: pct(TARGET_PCT.default),
            storage_hard: pct(STORAGE_HARD_PCT.default),
            cooldown_secs: CLUSTER_COOLDOWN_MINUTES.default * 60,
            max_tables: MAX_TABLES_PER_CLUSTER.default,
            max_schemas: MAX_SHARD_SCHEMAS_PER_CLUSTER.default,
            max_schema_creates_per_minute: MAX_SCHEMA_CREATES_PER_MINUTE.default,
            stability_secs: RECOVERY_STABILITY_MINUTES.default * 60,
            copy_slots: 2,
        }
    }
}

impl ClusterLimits {
    /// Resolves the keys in `ctx` (a context with the cluster set);
    /// `copy_slots` is the mover's key, resolved by its owner and passed in.
    #[must_use]
    pub fn resolve(s: &MemSettings<'_>, ctx: &SettingCtx<'_>, copy_slots: u32) -> Self {
        Self {
            high: pct(s.get(&HIGH_PCT, ctx)),
            target: pct(s.get(&TARGET_PCT, ctx)),
            storage_hard: pct(s.get(&STORAGE_HARD_PCT, ctx)),
            cooldown_secs: s.get(&CLUSTER_COOLDOWN_MINUTES, ctx) * 60,
            max_tables: s.get(&MAX_TABLES_PER_CLUSTER, ctx),
            max_schemas: s.get(&MAX_SHARD_SCHEMAS_PER_CLUSTER, ctx),
            max_schema_creates_per_minute: s.get(&MAX_SCHEMA_CREATES_PER_MINUTE, ctx),
            stability_secs: s.get(&RECOVERY_STABILITY_MINUTES, ctx) * 60,
            copy_slots,
        }
    }
}

/// The per-tenant keys.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TenantKnobs {
    /// `balancer.mode` for this tenant.
    pub mode: BalancerMode,
    /// Between moves of one shard, seconds.
    pub shard_cooldown_secs: u64,
    /// Moves per tenant per day.
    pub max_moves_per_day: u32,
    /// Co-writes per hour that bind two groups.
    pub cowrite_min_per_hour: u64,
    /// Shards above this many bytes need approval.
    pub auto_size_limit_bytes: f64,
    /// The cap on a doubled cooldown, seconds.
    pub max_cooldown_secs: u64,
}

impl Default for TenantKnobs {
    fn default() -> Self {
        Self {
            mode: BalancerMode::parse(&MODE.default),
            shard_cooldown_secs: SHARD_COOLDOWN_HOURS.default * 3600,
            max_moves_per_day: MAX_MOVES_PER_TENANT_PER_DAY.default,
            cowrite_min_per_hour: COWRITE_MIN_PER_HOUR.default,
            #[allow(clippy::cast_precision_loss)]
            auto_size_limit_bytes: AUTO_SIZE_LIMIT_GB.default as f64 * GB,
            max_cooldown_secs: MAX_COOLDOWN_DAYS.default * 86_400,
        }
    }
}

impl TenantKnobs {
    /// Resolves the keys in `ctx` (a context with the tenant set).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn resolve(s: &MemSettings<'_>, ctx: &SettingCtx<'_>) -> Self {
        Self {
            mode: BalancerMode::parse(&s.get(&MODE, ctx)),
            shard_cooldown_secs: s.get(&SHARD_COOLDOWN_HOURS, ctx) * 3600,
            max_moves_per_day: s.get(&MAX_MOVES_PER_TENANT_PER_DAY, ctx),
            cowrite_min_per_hour: s.get(&COWRITE_MIN_PER_HOUR, ctx),
            auto_size_limit_bytes: s.get(&AUTO_SIZE_LIMIT_GB, ctx) as f64 * GB,
            max_cooldown_secs: s.get(&MAX_COOLDOWN_DAYS, ctx) * 86_400,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::float_cmp)]

    use sprig_common::ids::{ClusterId, DcId};
    use sprig_settings::{Level, Registry, Value};

    use super::*;

    #[test]
    fn keys_are_registered_and_resolve() {
        let reg = Registry::global().unwrap();
        for k in [
            "balancer.mode",
            "balancer.high_pct",
            "balancer.weight_rpo",
            "placement.max_tables_per_cluster",
            "placement.least_busy_weights",
            "decisions.retention_days",
            "recovery.stability_minutes",
            "migration.auto_size_limit_gb",
        ] {
            assert!(reg.get(k).is_some(), "{k}");
        }
        let mut s = MemSettings::new(reg);
        let dc = DcId::new("mum1").unwrap();
        let c7 = ClusterId::new("mum1-c7").unwrap();
        let ctx = SettingCtx::process(&dc);
        assert_eq!(BalancerSettings::resolve(&s, &ctx), BalancerSettings::default());
        let cctx = SettingCtx {
            cluster: Some(&c7),
            ..ctx
        };
        assert_eq!(ClusterLimits::resolve(&s, &cctx, 2), ClusterLimits::default());
        s.set_value(Level::Cluster, &cctx, "balancer.high_pct", &Value::Int(90))
            .unwrap();
        assert_eq!(ClusterLimits::resolve(&s, &cctx, 2).high, 0.9);
        s.set_value(Level::Dc, &ctx, "balancer.mode", &Value::Str("dry_run".into()))
            .unwrap();
        assert_eq!(BalancerSettings::resolve(&s, &ctx).mode, BalancerMode::DryRun);
        // Hard bounds refuse a mode that doesn't exist.
        assert!(
            s.set_value(Level::Dc, &ctx, "balancer.mode", &Value::Str("fast".into()))
                .is_err()
        );
    }

    #[test]
    fn least_busy_weights_parse() {
        assert_eq!(LeastBusyWeights::parse(""), LeastBusyWeights::default());
        let w = LeastBusyWeights::parse("storage=1, cpu=0 ,connections=0");
        assert_eq!((w.storage, w.cpu, w.connections), (1.0, 0.0, 0.0));
        assert_eq!(LeastBusyWeights::parse("disk=1"), LeastBusyWeights::default());
        assert_eq!(
            LeastBusyWeights::parse("storage=0,cpu=0,connections=0"),
            LeastBusyWeights::default()
        );
        assert_eq!(LeastBusyWeights::parse("cpu=x"), LeastBusyWeights::default());
    }
}
