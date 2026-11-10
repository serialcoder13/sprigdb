//! Chapter 16's `advisor.*` keys (chapter 03, "Declaring a key") and
//! [`AdvisorSettings`], a typed snapshot for one tenant. Every key accepts
//! the tenant and global scopes; `advisor.mode` also dc.

use std::borrow::Cow;
use std::time::Duration;

use sprig_settings::{Apply, HardBounds, MemSettings, Scopes, Setting, SettingCtx, TenantSettings};

/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);
/// Tenant, dc and global.
const TENANT_DC: Scopes = Scopes(0b11_0100);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $min:expr, $max:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: TENANT_GLOBAL,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: true,
            owner: 16,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

/// `advisor.mode`: `on`, `recommend` or `off`.
pub static MODE: Setting<Cow<'static, str>> = Setting {
    key: "advisor.mode",
    default: Cow::Borrowed("on"),
    scopes: TENANT_DC,
    hard: HardBounds::one_of(&["on", "recommend", "off"]),
    apply: Apply::Live,
    operator_only: true,
    owner: 16,
    doc: "Sharding advisor: on, recommend or off",
};
sprig_settings::register!(MODE);

key! {
    /// `advisor.window_days`.
    WINDOW_DAYS: f64 = "advisor.window_days", 7.0, 0, 365, "Statistics window"
}
key! {
    /// `advisor.min_observe_hours`.
    MIN_OBSERVE_HOURS: f64 = "advisor.min_observe_hours", 24.0, 0, 8760, "History needed for a design"
}
key! {
    /// `advisor.min_units`.
    MIN_UNITS: u64 = "advisor.min_units", 10_000, 0, 1_000_000_000, "Sampled units needed"
}
key! {
    /// `advisor.min_confidence`.
    MIN_CONFIDENCE: f64 = "advisor.min_confidence", 0.9, 0, 1, "Confidence needed to apply a first design"
}
key! {
    /// `advisor.max_wait_hours`.
    MAX_WAIT_HOURS: f64 = "advisor.max_wait_hours", 72.0, 0, 8760, "Then apply the best design anyway"
}
key! {
    /// `advisor.txn_weight`.
    TXN_WEIGHT: f64 = "advisor.txn_weight", 2.0, 0, 100, "Weight of a write transaction"
}
key! {
    /// `advisor.lookup_penalty`.
    LOOKUP_PENALTY: f64 = "advisor.lookup_penalty", 0.1, 0, 1, "Credit lost per lookup a statement needs"
}
key! {
    /// `advisor.min_ndv_per_shard`.
    MIN_NDV_PER_SHARD: u64 = "advisor.min_ndv_per_shard", 100, 0, 1_000_000_000, "Distinct values needed per expected shard"
}
key! {
    /// `advisor.max_top_value_pct`.
    MAX_TOP_VALUE_PCT: u32 = "advisor.max_top_value_pct", 25, 1, 100, "Largest share one value may hold"
}
key! {
    /// `advisor.max_moves`.
    MAX_MOVES: u32 = "advisor.max_moves", 200, 0, 100_000, "Search steps per run"
}

/// How the advisor acts on a design.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdvisorMode {
    /// Runs and applies.
    On,
    /// Runs; `apply` becomes `recommend`.
    Recommend,
    /// No runs: a first split uses the structural design.
    Off,
}

/// The keys for one tenant, resolved.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AdvisorSettings {
    /// `advisor.mode`.
    pub mode: AdvisorMode,
    /// `window_days`.
    pub window: Duration,
    /// `min_observe_hours`.
    pub min_observe: Duration,
    /// `min_units`.
    pub min_units: u64,
    /// `min_confidence`.
    pub min_confidence: f64,
    /// `max_wait_hours`.
    pub max_wait: Duration,
    /// `txn_weight`.
    pub txn_weight: f64,
    /// `lookup_penalty`.
    pub lookup_penalty: f64,
    /// `min_ndv_per_shard`.
    pub min_ndv_per_shard: u64,
    /// `max_top_value_pct` as a ratio.
    pub max_top_value: f64,
    /// `max_moves`.
    pub max_moves: u32,
}

/// One key over the etcd scopes and, when given, a tenant's own rows
/// (RL3, r2harden).
fn layered<T: sprig_settings::SettingValue>(
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

impl AdvisorSettings {
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
            mode: match layered(s, rows, &MODE, ctx).as_ref() {
                "recommend" => AdvisorMode::Recommend,
                "off" => AdvisorMode::Off,
                _ => AdvisorMode::On,
            },
            window: hours(layered(s, rows, &WINDOW_DAYS, ctx) * 24.0),
            min_observe: hours(layered(s, rows, &MIN_OBSERVE_HOURS, ctx)),
            min_units: layered(s, rows, &MIN_UNITS, ctx),
            min_confidence: layered(s, rows, &MIN_CONFIDENCE, ctx),
            max_wait: hours(layered(s, rows, &MAX_WAIT_HOURS, ctx)),
            txn_weight: layered(s, rows, &TXN_WEIGHT, ctx),
            lookup_penalty: layered(s, rows, &LOOKUP_PENALTY, ctx),
            min_ndv_per_shard: layered(s, rows, &MIN_NDV_PER_SHARD, ctx),
            max_top_value: f64::from(layered(s, rows, &MAX_TOP_VALUE_PCT, ctx)) / 100.0,
            max_moves: layered(s, rows, &MAX_MOVES, ctx),
        }
    }
}

impl Default for AdvisorSettings {
    fn default() -> Self {
        Self {
            mode: AdvisorMode::On,
            window: hours(WINDOW_DAYS.default * 24.0),
            min_observe: hours(MIN_OBSERVE_HOURS.default),
            min_units: MIN_UNITS.default,
            min_confidence: MIN_CONFIDENCE.default,
            max_wait: hours(MAX_WAIT_HOURS.default),
            txn_weight: TXN_WEIGHT.default,
            lookup_penalty: LOOKUP_PENALTY.default,
            min_ndv_per_shard: MIN_NDV_PER_SHARD.default,
            max_top_value: f64::from(MAX_TOP_VALUE_PCT.default) / 100.0,
            max_moves: MAX_MOVES.default,
        }
    }
}
