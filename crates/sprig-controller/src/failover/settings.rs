//! Chapter 25's `failover.*` and `recovery.*` keys, and a typed snapshot.
//!
//! `recovery.max_rebuilds_per_cluster` is the controller's scheduler key
//! ([`crate::settings::MAX_REBUILDS_PER_CLUSTER`]) and
//! `recovery.stability_minutes` is the balancer's (chapter 11); both are
//! declared once, there.

use std::borrow::Cow;
use std::time::Duration;

use sprig_common::ids::DcId;
use sprig_settings::{Apply, HardBounds, MemSettings, Scopes, Setting, SettingCtx};

/// Tenant, cluster, dc and global.
const TCDG: Scopes = Scopes(0b11_1100);
/// Tenant, dc and global.
const TDG: Scopes = Scopes(0b11_0100);
/// Tenant and global.
const TG: Scopes = Scopes(0b10_0100);
/// Cluster, dc and global.
const CDG: Scopes = Scopes::OPERATIONAL;
/// Dc and global.
const DG: Scopes = Scopes::PROCESS;
/// Global only.
const G: Scopes = Scopes(0b10_0000);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $min:expr, $max:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: true,
            owner: 25,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

/// `failover.mode`: `auto`, or `manual` (the dry run).
pub static MODE: Setting<Cow<'static, str>> = Setting {
    key: "failover.mode",
    default: Cow::Borrowed("auto"),
    scopes: TCDG,
    hard: HardBounds::one_of(&["auto", "manual"]),
    apply: Apply::Live,
    operator_only: true,
    owner: 25,
    doc: "auto or manual (the dry run)",
};
sprig_settings::register!(MODE);

/// `failover.self_fence`: false also disables automatic DC failover.
pub static SELF_FENCE: Setting<bool> = Setting {
    key: "failover.self_fence",
    default: true,
    scopes: DG,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: true,
    owner: 25,
    doc: "False also disables automatic DC failover",
};
sprig_settings::register!(SELF_FENCE);

/// `recovery.traffic_steps_pct`: traffic share per step.
pub static TRAFFIC_STEPS_PCT: Setting<Cow<'static, str>> = Setting {
    key: "recovery.traffic_steps_pct",
    default: Cow::Borrowed("10,25,50,100"),
    scopes: DG,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: true,
    owner: 25,
    doc: "Traffic share per step",
};
sprig_settings::register!(TRAFFIC_STEPS_PCT);

key! {
    /// `failover.rpo_seconds`.
    RPO_SECONDS: u64 = "failover.rpo_seconds", 5, TDG, 0, 86_400,
    "Most data loss accepted automatically"
}
key! {
    /// `failover.dc_lease_seconds`.
    DC_LEASE_SECONDS: u64 = "failover.dc_lease_seconds", 10, DG, 2, 600,
    "DC lease TTL"
}
key! {
    /// `failover.clock_drift_margin_ms`.
    CLOCK_DRIFT_MARGIN_MS: u64 = "failover.clock_drift_margin_ms", 500, G, 0, 60_000,
    "Wait after lease expiry"
}
key! {
    /// `failover.min_sites`.
    MIN_SITES: u32 = "failover.min_sites", 3, G, 2, 64,
    "Sites needed for automatic DC failover"
}
key! {
    /// `failover.fence_lead_ms`.
    FENCE_LEAD_MS: u64 = "failover.fence_lead_ms", 3000, DG, 100, 600_000,
    "Fence this long before a lease could expire"
}
key! {
    /// `failover.watchdog_ms`.
    WATCHDOG_MS: u64 = "failover.watchdog_ms", 1000, DG, 50, 60_000,
    "Stall that triggers a direct fence"
}
key! {
    /// `failover.writer_lease_seconds`.
    WRITER_LEASE_SECONDS: u64 = "failover.writer_lease_seconds", 10, CDG, 2, 600,
    "Writer lease TTL"
}
key! {
    /// `failover.cluster_detect_seconds`.
    CLUSTER_DETECT_SECONDS: u64 = "failover.cluster_detect_seconds", 5, CDG, 1, 3600,
    "Probe failures before a primary is lost"
}
key! {
    /// `failover.observe_settle_ms` (r2opfix): how long an observed writer
    /// change (Kubernetes, provider failover) must hold before it is
    /// published to gateways.
    OBSERVE_SETTLE_MS: u64 = "failover.observe_settle_ms", 2000, CDG, 0, 60000,
    "An observed writer change holds this long before it is published"
}
key! {
    /// `failover.cluster_detect_sources`.
    CLUSTER_DETECT_SOURCES: u32 = "failover.cluster_detect_sources", 2, CDG, 1, 3,
    "Sources that must agree a writer is down"
}
key! {
    /// `failover.min_interval_minutes`.
    MIN_INTERVAL_MINUTES: u64 = "failover.min_interval_minutes", 30, CDG, 0, 10_080,
    "Only hard failures fail over again within it"
}
key! {
    /// `failover.galera_quorum_wait_seconds`.
    GALERA_QUORUM_WAIT_SECONDS: u64 = "failover.galera_quorum_wait_seconds", 30, CDG, 1, 3600,
    "Quorum loss before bootstrap"
}
key! {
    /// `failover.observer_probes`.
    OBSERVER_PROBES: u32 = "failover.observer_probes", 3, G, 1, 20,
    "Probes per observer"
}
key! {
    /// `failover.observer_probe_interval_ms`.
    OBSERVER_PROBE_INTERVAL_MS: u64 = "failover.observer_probe_interval_ms", 1000, G, 10, 60_000,
    "Gap between probes"
}
key! {
    /// `failover.decision_timeout_seconds`.
    DECISION_TIMEOUT_SECONDS: u64 = "failover.decision_timeout_seconds", 120, G, 1, 3600,
    "Then manual_only"
}
key! {
    /// `failover.max_parallel_promotions`.
    MAX_PARALLEL_PROMOTIONS: u32 = "failover.max_parallel_promotions", 16, G, 1, 1024,
    "Concurrent tenant promotions"
}
key! {
    /// `failover.standby_dcs`.
    STANDBY_DCS: u32 = "failover.standby_dcs", 1, TDG, 0, 8,
    "Standby DCs per tenant; 0 disables DC failover"
}
key! {
    /// `failover.quarantine_days`.
    QUARANTINE_DAYS: u32 = "failover.quarantine_days", 30, TG, 1, 3650,
    "Retention of diverged-transaction extracts"
}
key! {
    /// `recovery.rebuild_threshold_gb`.
    REBUILD_THRESHOLD_GB: u64 = "recovery.rebuild_threshold_gb", 50, CDG, 0, 100_000,
    "Binlog to replay above which a node is rebuilt"
}
key! {
    /// `recovery.rebuild_max_mb_per_second`.
    REBUILD_MAX_MB_PER_SECOND: u64 = "recovery.rebuild_max_mb_per_second", 200, CDG, 1, 100_000,
    "Rebuild stream rate cap"
}
key! {
    /// `recovery.max_latency_increase_pct`.
    MAX_LATENCY_INCREASE_PCT: u64 = "recovery.max_latency_increase_pct", 20, CDG, 1, 1000,
    "Live p99 rise that pauses recovery"
}
key! {
    /// `recovery.step_minutes`.
    STEP_MINUTES: u64 = "recovery.step_minutes", 5, DG, 0, 1440,
    "Time per step"
}
key! {
    /// `recovery.step_max_lag_ms`.
    STEP_MAX_LAG_MS: u64 = "recovery.step_max_lag_ms", 1000, CDG, 0, 3_600_000,
    "Lag allowed before read steps"
}
key! {
    /// `recovery.admission_connects_per_second`.
    ADMISSION_CONNECTS_PER_SECOND: u32 = "recovery.admission_connects_per_second", 200, DG, 1, 1_000_000,
    "New connections per gateway at the first step"
}

/// A snapshot of the DC-level failover keys.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct FailoverSettings {
    /// `failover.mode = manual` at the DC or global scope.
    pub manual: bool,
    /// `failover.rpo_seconds` (the DC's; tenants may lower it).
    pub rpo: Duration,
    /// `failover.dc_lease_seconds`.
    pub dc_lease: Duration,
    /// `failover.clock_drift_margin_ms`.
    pub clock_drift_margin: Duration,
    /// `failover.min_sites`.
    pub min_sites: u32,
    /// `failover.fence_lead_ms`.
    pub fence_lead: Duration,
    /// `failover.self_fence`.
    pub self_fence: bool,
    /// `failover.watchdog_ms`.
    pub watchdog: Duration,
    /// `failover.observer_probes`.
    pub observer_probes: u32,
    /// `failover.observer_probe_interval_ms`.
    pub observer_probe_interval: Duration,
    /// `failover.decision_timeout_seconds`.
    pub decision_timeout: Duration,
    /// `failover.max_parallel_promotions`.
    pub max_parallel_promotions: u32,
    /// `recovery.traffic_steps_pct`.
    pub traffic_steps_pct: Vec<u32>,
    /// `recovery.step_minutes`.
    pub step: Duration,
    /// `recovery.admission_connects_per_second`.
    pub admission_per_second: u32,
}

/// `10,25,50,100` -> steps, ending at 100; a bad list gives the default.
#[must_use]
pub fn parse_steps(s: &str) -> Vec<u32> {
    let v: Option<Vec<u32>> = s.split(',').map(|p| p.trim().parse().ok()).collect();
    match v {
        Some(v)
            if !v.is_empty()
                && v.windows(2).all(|w| w[0] < w[1])
                && v.last() == Some(&100)
                && v[0] > 0 =>
        {
            v
        }
        _ => vec![10, 25, 50, 100],
    }
}

impl Default for FailoverSettings {
    fn default() -> Self {
        Self {
            manual: false,
            rpo: Duration::from_secs(RPO_SECONDS.default),
            dc_lease: Duration::from_secs(DC_LEASE_SECONDS.default),
            clock_drift_margin: Duration::from_millis(CLOCK_DRIFT_MARGIN_MS.default),
            min_sites: MIN_SITES.default,
            fence_lead: Duration::from_millis(FENCE_LEAD_MS.default),
            self_fence: SELF_FENCE.default,
            watchdog: Duration::from_millis(WATCHDOG_MS.default),
            observer_probes: OBSERVER_PROBES.default,
            observer_probe_interval: Duration::from_millis(OBSERVER_PROBE_INTERVAL_MS.default),
            decision_timeout: Duration::from_secs(DECISION_TIMEOUT_SECONDS.default),
            max_parallel_promotions: MAX_PARALLEL_PROMOTIONS.default,
            traffic_steps_pct: parse_steps(&TRAFFIC_STEPS_PCT.default),
            step: Duration::from_secs(STEP_MINUTES.default * 60),
            admission_per_second: ADMISSION_CONNECTS_PER_SECOND.default,
        }
    }
}

impl FailoverSettings {
    /// Resolves every key for `dc`.
    #[must_use]
    pub fn resolve(settings: &MemSettings<'_>, dc: &DcId) -> Self {
        let cx = SettingCtx::process(dc);
        Self {
            manual: settings.get(&MODE, &cx) == "manual",
            rpo: Duration::from_secs(settings.get(&RPO_SECONDS, &cx)),
            dc_lease: Duration::from_secs(settings.get(&DC_LEASE_SECONDS, &cx)),
            clock_drift_margin: Duration::from_millis(settings.get(&CLOCK_DRIFT_MARGIN_MS, &cx)),
            min_sites: settings.get(&MIN_SITES, &cx),
            fence_lead: Duration::from_millis(settings.get(&FENCE_LEAD_MS, &cx)),
            self_fence: settings.get(&SELF_FENCE, &cx),
            watchdog: Duration::from_millis(settings.get(&WATCHDOG_MS, &cx)),
            observer_probes: settings.get(&OBSERVER_PROBES, &cx),
            observer_probe_interval: Duration::from_millis(
                settings.get(&OBSERVER_PROBE_INTERVAL_MS, &cx),
            ),
            decision_timeout: Duration::from_secs(settings.get(&DECISION_TIMEOUT_SECONDS, &cx)),
            max_parallel_promotions: settings.get(&MAX_PARALLEL_PROMOTIONS, &cx),
            traffic_steps_pct: parse_steps(&settings.get(&TRAFFIC_STEPS_PCT, &cx)),
            step: Duration::from_secs(settings.get(&STEP_MINUTES, &cx) * 60),
            admission_per_second: settings.get(&ADMISSION_CONNECTS_PER_SECOND, &cx),
        }
    }

    /// The DC lease options these settings give.
    #[must_use]
    pub fn lease_opts(&self) -> sprig_meta::dclease::DcLeaseOpts {
        let mut o = sprig_meta::dclease::DcLeaseOpts::new(self.dc_lease, self.fence_lead);
        o.self_fence = self.self_fence;
        o
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steps_parse_or_fall_back() {
        assert_eq!(parse_steps("10,25,50,100"), vec![10, 25, 50, 100]);
        assert_eq!(parse_steps("5, 100"), vec![5, 100]);
        assert_eq!(parse_steps("50,25,100"), vec![10, 25, 50, 100]);
        assert_eq!(parse_steps("10,50"), vec![10, 25, 50, 100]);
        assert_eq!(parse_steps("x"), vec![10, 25, 50, 100]);
    }

    #[test]
    fn defaults_follow_the_book() {
        let s = FailoverSettings::default();
        assert_eq!(s.dc_lease, Duration::from_secs(10));
        assert_eq!(s.fence_lead, Duration::from_secs(3));
        assert_eq!(s.min_sites, 3);
        assert_eq!(s.rpo, Duration::from_secs(5));
        assert_eq!(s.lease_opts().period, Duration::from_millis(2500));
    }
}
