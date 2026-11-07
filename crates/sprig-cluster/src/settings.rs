//! Chapter 09's settings that steps 1-4 use, declared next to the code
//! (chapter 03, "Declaring a key"), and a typed snapshot of them.

use std::time::Duration;

use sprig_settings::{Apply, HardBounds, MemSettings, Scopes, Setting, SettingCtx};

/// "Ops": cluster, dc and global (chapter 09, "Configuration").
const OPS: Scopes = Scopes::OPERATIONAL;
/// Global only.
const GLOBAL: Scopes = Scopes(0b10_0000);

macro_rules! ops_key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $min:expr, $max:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: OPS,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: true,
            owner: 9,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

ops_key! {
    /// `cluster.health_interval_seconds`.
    HEALTH_INTERVAL_SECONDS: u64 = "cluster.health_interval_seconds", 5, 1, 3600,
    "Light probe period"
}
ops_key! {
    /// `cluster.probe_interval_seconds`.
    PROBE_INTERVAL_SECONDS: u64 = "cluster.probe_interval_seconds", 300, 10, 86_400,
    "Full probe period"
}
ops_key! {
    /// `cluster.probe_timeout_ms`.
    PROBE_TIMEOUT_MS: u64 = "cluster.probe_timeout_ms", 2000, 100, 60_000,
    "Probe statement timeout"
}
ops_key! {
    /// `cluster.unhealthy_after_failures`.
    UNHEALTHY_AFTER_FAILURES: u32 = "cluster.unhealthy_after_failures", 3, 1, 100,
    "Failed probes before a node is unreachable"
}
ops_key! {
    /// `cluster.capability_upgrade_sustain_seconds`.
    CAPABILITY_UPGRADE_SUSTAIN_SECONDS: u64 = "cluster.capability_upgrade_sustain_seconds", 60, 0, 86_400,
    "An upgrade must hold this long"
}
ops_key! {
    /// `cluster.max_replica_lag_seconds`.
    MAX_REPLICA_LAG_SECONDS: u64 = "cluster.max_replica_lag_seconds", 10, 1, 86_400,
    "Lag before a replica is lagging"
}
ops_key! {
    /// `cluster.heartbeat_interval_ms`.
    HEARTBEAT_INTERVAL_MS: u64 = "cluster.heartbeat_interval_ms", 1000, 100, 60_000,
    "Heartbeat period"
}
ops_key! {
    /// `cluster.min_binlog_retention_hours`.
    MIN_BINLOG_RETENTION_HOURS: u64 = "cluster.min_binlog_retention_hours", 24, 1, 8760,
    "Binlog horizon warning"
}
ops_key! {
    /// `cluster.clock_skew_warn_ms`.
    CLOCK_SKEW_WARN_MS: u64 = "cluster.clock_skew_warn_ms", 500, 1, 600_000,
    "Clock skew warning"
}

/// `cluster.lower_case_table_names`: the fleet value, global only.
pub static LOWER_CASE_TABLE_NAMES: Setting<u32> = Setting {
    key: "cluster.lower_case_table_names",
    default: 0,
    scopes: GLOBAL,
    hard: HardBounds::range(0, 2),
    apply: Apply::Live,
    operator_only: true,
    owner: 9,
    doc: "Fleet lower_case_table_names; every cluster must match",
};
sprig_settings::register!(LOWER_CASE_TABLE_NAMES);

/// A snapshot of the keys, resolved for one cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSettings {
    /// Light probe period.
    pub health_interval: Duration,
    /// Full probe period.
    pub probe_interval: Duration,
    /// Probe round-trip timeout.
    pub probe_timeout: Duration,
    /// Failed probes before `unreachable`.
    pub unhealthy_after_failures: u32,
    /// How long an upgrade must hold.
    pub capability_upgrade_sustain: Duration,
    /// Lag before `lagging`, seconds.
    pub max_replica_lag_seconds: u64,
    /// Heartbeat period.
    pub heartbeat_interval: Duration,
    /// Binlog retention below which new mover jobs are refused, hours.
    pub min_binlog_retention_hours: u64,
    /// Clock skew warning, ms.
    pub clock_skew_warn_ms: u64,
    /// Fleet `lower_case_table_names`.
    pub lower_case_table_names: u32,
}

impl Default for ClusterSettings {
    /// The built-in defaults.
    fn default() -> Self {
        Self {
            health_interval: Duration::from_secs(HEALTH_INTERVAL_SECONDS.default),
            probe_interval: Duration::from_secs(PROBE_INTERVAL_SECONDS.default),
            probe_timeout: Duration::from_millis(PROBE_TIMEOUT_MS.default),
            unhealthy_after_failures: UNHEALTHY_AFTER_FAILURES.default,
            capability_upgrade_sustain: Duration::from_secs(
                CAPABILITY_UPGRADE_SUSTAIN_SECONDS.default,
            ),
            max_replica_lag_seconds: MAX_REPLICA_LAG_SECONDS.default,
            heartbeat_interval: Duration::from_millis(HEARTBEAT_INTERVAL_MS.default),
            min_binlog_retention_hours: MIN_BINLOG_RETENTION_HOURS.default,
            clock_skew_warn_ms: CLOCK_SKEW_WARN_MS.default,
            lower_case_table_names: LOWER_CASE_TABLE_NAMES.default,
        }
    }
}

impl ClusterSettings {
    /// Resolves every key in `ctx` (a context with the cluster set).
    #[must_use]
    pub fn resolve(settings: &MemSettings<'_>, ctx: &SettingCtx<'_>) -> Self {
        Self {
            health_interval: Duration::from_secs(settings.get(&HEALTH_INTERVAL_SECONDS, ctx)),
            probe_interval: Duration::from_secs(settings.get(&PROBE_INTERVAL_SECONDS, ctx)),
            probe_timeout: Duration::from_millis(settings.get(&PROBE_TIMEOUT_MS, ctx)),
            unhealthy_after_failures: settings.get(&UNHEALTHY_AFTER_FAILURES, ctx),
            capability_upgrade_sustain: Duration::from_secs(
                settings.get(&CAPABILITY_UPGRADE_SUSTAIN_SECONDS, ctx),
            ),
            max_replica_lag_seconds: settings.get(&MAX_REPLICA_LAG_SECONDS, ctx),
            heartbeat_interval: Duration::from_millis(settings.get(&HEARTBEAT_INTERVAL_MS, ctx)),
            min_binlog_retention_hours: settings.get(&MIN_BINLOG_RETENTION_HOURS, ctx),
            clock_skew_warn_ms: settings.get(&CLOCK_SKEW_WARN_MS, ctx),
            lower_case_table_names: settings.get(&LOWER_CASE_TABLE_NAMES, ctx),
        }
    }
}

#[cfg(test)]
mod tests {
    use sprig_common::ids::{ClusterId, DcId};
    use sprig_settings::{Level, Registry, Value};

    use super::*;

    #[test]
    fn keys_are_registered_and_resolve_per_cluster() {
        let reg = Registry::global().unwrap();
        for k in [
            "cluster.health_interval_seconds",
            "cluster.capability_upgrade_sustain_seconds",
            "cluster.lower_case_table_names",
        ] {
            assert!(reg.get(k).is_some(), "{k}");
        }
        let mut s = MemSettings::new(reg);
        let dc = DcId::new("mum1").unwrap();
        let c7 = ClusterId::new("mum1-c7").unwrap();
        let ctx = SettingCtx {
            cluster: Some(&c7),
            ..SettingCtx::process(&dc)
        };
        assert_eq!(
            ClusterSettings::resolve(&s, &ctx),
            ClusterSettings::default()
        );
        s.set_value(
            Level::Cluster,
            &ctx,
            "cluster.max_replica_lag_seconds",
            &Value::Int(30),
        )
        .unwrap();
        assert_eq!(
            ClusterSettings::resolve(&s, &ctx).max_replica_lag_seconds,
            30
        );
        // The fleet value is global only.
        assert!(
            s.set_value(
                Level::Cluster,
                &ctx,
                "cluster.lower_case_table_names",
                &Value::Int(1)
            )
            .is_err()
        );
    }
}
