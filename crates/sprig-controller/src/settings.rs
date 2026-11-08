//! The settings the controller runtime and the job scheduler read (chapter
//! 23, "Configuration"), declared next to the code (chapter 03, "Declaring
//! a key"), and a typed snapshot of them.
//!
//! The three slot keys belong to chapters 10, 13 and 25; they are declared
//! here until `sprig-mover`, `sprig-advisor` and the recovery loops exist,
//! and move to those crates then.
//!
//! Until the etcd mirror of settings lands (chapter 23 step 8), the role
//! runs on [`ControlSettings::default`]; the runtime reads the snapshot
//! through a `watch` channel so live values need no restart.

use std::time::Duration;

use sprig_common::ids::DcId;
use sprig_settings::{Apply, HardBounds, MemSettings, Scopes, Setting, SettingCtx};

/// Dc and global.
const PROCESS: Scopes = Scopes::PROCESS;
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

key! {
    /// `control.resync_seconds`.
    RESYNC_SECONDS: u64 = "control.resync_seconds", 300, PROCESS, 1, 86_400, 23,
    "Full resync period of every loop"
}
key! {
    /// `control.job_max_attempts`.
    JOB_MAX_ATTEMPTS: u32 = "control.job_max_attempts", 10, PROCESS, 1, 1000, 23,
    "Attempts before a job fails"
}
key! {
    /// `control.tenant_jobs_per_hour`.
    TENANT_JOBS_PER_HOUR: u32 = "control.tenant_jobs_per_hour", 12, TENANT_DC_GLOBAL, 1, 10_000, 23,
    "Automatic jobs a tenant may start per hour"
}
key! {
    /// `control.leader_lease_seconds`.
    LEADER_LEASE_SECONDS: u64 = "control.leader_lease_seconds", 5, PROCESS, 2, 600, 23,
    "DC leader session TTL; used at the next campaign"
}
key! {
    /// `control.global_leader_lease_seconds`.
    GLOBAL_LEADER_LEASE_SECONDS: u64 = "control.global_leader_lease_seconds", 15, Scopes(0b10_0000), 2, 600, 23,
    "Global leader session TTL"
}
key! {
    /// `meta.request_timeout_ms`.
    REQUEST_TIMEOUT_MS: u64 = "meta.request_timeout_ms", 2000, PROCESS, 100, 60_000, 23,
    "Timeout per etcd request"
}
key! {
    /// `mover.max_copies_per_cluster` (chapter 10): the `copy` slot pool.
    MAX_COPIES_PER_CLUSTER: u32 = "mover.max_copies_per_cluster", 2, Scopes::OPERATIONAL, 1, 64, 10,
    "Concurrent copies reading or writing one cluster"
}
key! {
    /// `auto_index.max_builds_per_cluster` (chapter 13): the `build` slot
    /// pool. Chapter 13 accepts every scope; the scheduler resolves it per
    /// cluster.
    MAX_BUILDS_PER_CLUSTER: u32 = "auto_index.max_builds_per_cluster", 1, Scopes::ALL, 1, 64, 13,
    "Concurrent index builds per cluster"
}
key! {
    /// `recovery.max_rebuilds_per_cluster` (chapter 25): the `recovery`
    /// slot pool.
    MAX_REBUILDS_PER_CLUSTER: u32 = "recovery.max_rebuilds_per_cluster", 1, Scopes::OPERATIONAL, 1, 64, 25,
    "Concurrent rebuilds or reseeds"
}
key! {
    /// `tenant.max_depth`: the deepest tenant tree (root at depth 0).
    TENANT_MAX_DEPTH: u32 = "tenant.max_depth", 64, Scopes(0b10_0000), 1, 4096, 23,
    "Deepest tenant tree (root at depth 0)"
}

/// A snapshot of the controller's keys, resolved for one DC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlSettings {
    /// Full resync period of every loop.
    pub resync: Duration,
    /// Attempts before a job fails.
    pub job_max_attempts: u32,
    /// Automatic jobs a tenant may start per hour (the DC's value; the
    /// tenant scope needs the catalog, chapter 08).
    pub tenant_jobs_per_hour: u32,
    /// DC leader session TTL.
    pub leader_lease: Duration,
    /// Global leader session TTL.
    pub global_leader_lease: Duration,
    /// Timeout per etcd request.
    pub request_timeout: Duration,
    /// `copy` slots per cluster.
    pub max_copies_per_cluster: u32,
    /// `build` slots per cluster.
    pub max_builds_per_cluster: u32,
    /// `recovery` slots per cluster.
    pub max_rebuilds_per_cluster: u32,
}

impl Default for ControlSettings {
    /// The built-in defaults.
    fn default() -> Self {
        Self {
            resync: Duration::from_secs(RESYNC_SECONDS.default),
            job_max_attempts: JOB_MAX_ATTEMPTS.default,
            tenant_jobs_per_hour: TENANT_JOBS_PER_HOUR.default,
            leader_lease: Duration::from_secs(LEADER_LEASE_SECONDS.default),
            global_leader_lease: Duration::from_secs(GLOBAL_LEADER_LEASE_SECONDS.default),
            request_timeout: Duration::from_millis(REQUEST_TIMEOUT_MS.default),
            max_copies_per_cluster: MAX_COPIES_PER_CLUSTER.default,
            max_builds_per_cluster: MAX_BUILDS_PER_CLUSTER.default,
            max_rebuilds_per_cluster: MAX_REBUILDS_PER_CLUSTER.default,
        }
    }
}

impl ControlSettings {
    /// Resolves every key for the DC `dc`.
    #[must_use]
    pub fn resolve(settings: &MemSettings<'_>, dc: &DcId) -> Self {
        let ctx = SettingCtx::process(dc);
        Self {
            resync: Duration::from_secs(settings.get(&RESYNC_SECONDS, &ctx)),
            job_max_attempts: settings.get(&JOB_MAX_ATTEMPTS, &ctx),
            tenant_jobs_per_hour: settings.get(&TENANT_JOBS_PER_HOUR, &ctx),
            leader_lease: Duration::from_secs(settings.get(&LEADER_LEASE_SECONDS, &ctx)),
            global_leader_lease: Duration::from_secs(
                settings.get(&GLOBAL_LEADER_LEASE_SECONDS, &ctx),
            ),
            request_timeout: Duration::from_millis(settings.get(&REQUEST_TIMEOUT_MS, &ctx)),
            max_copies_per_cluster: settings.get(&MAX_COPIES_PER_CLUSTER, &ctx),
            max_builds_per_cluster: settings.get(&MAX_BUILDS_PER_CLUSTER, &ctx),
            max_rebuilds_per_cluster: settings.get(&MAX_REBUILDS_PER_CLUSTER, &ctx),
        }
    }
}

#[cfg(test)]
mod tests {
    use sprig_settings::{Level, Registry, Value};

    use super::*;

    #[test]
    fn keys_are_registered_and_resolve() {
        let reg = Registry::global().unwrap();
        for k in [
            "control.resync_seconds",
            "control.job_max_attempts",
            "control.tenant_jobs_per_hour",
            "control.leader_lease_seconds",
            "mover.max_copies_per_cluster",
        ] {
            assert!(reg.get(k).is_some(), "{k}");
        }
        let mut s = MemSettings::new(reg);
        let dc = DcId::new("mum1").unwrap();
        assert_eq!(
            ControlSettings::resolve(&s, &dc),
            ControlSettings::default()
        );
        s.set_value(
            Level::Dc,
            &SettingCtx::process(&dc),
            "control.job_max_attempts",
            &Value::Int(3),
        )
        .unwrap();
        assert_eq!(ControlSettings::resolve(&s, &dc).job_max_attempts, 3);
    }
}
