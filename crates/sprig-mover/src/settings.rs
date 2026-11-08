//! Chapter 10's settings, declared next to the code (chapter 03,
//! "Declaring a key"), and a typed snapshot the engine reads at each chunk
//! and cutover attempt.

use std::borrow::Cow;
use std::time::Duration;

use sprig_settings::{Apply, HardBounds, MemSettings, Scopes, Setting, SettingCtx};

/// "Ops": cluster, dc and global.
const OPS: Scopes = Scopes::OPERATIONAL;
/// Tenant and ops.
const TENANT_OPS: Scopes = Scopes(0b11_1100);
/// Dc and global.
const DC_GLOBAL: Scopes = Scopes::PROCESS;

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $scopes:expr, $default:expr, $min:expr, $max:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: true,
            owner: 10,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key! {
    /// `mover.cutover_max_hold_ms`.
    CUTOVER_MAX_HOLD_MS: u64 = "mover.cutover_max_hold_ms", TENANT_OPS, 2000, 200, 60_000,
    "Longest write hold per cutover"
}
key! {
    /// `mover.ready_max_lag_ms`.
    READY_MAX_LAG_MS: u64 = "mover.ready_max_lag_ms", OPS, 1000, 1, 600_000,
    "Lag needed before cutover"
}
key! {
    /// `mover.ready_sustain_seconds`.
    READY_SUSTAIN_SECONDS: u64 = "mover.ready_sustain_seconds", OPS, 60, 0, 86_400,
    "How long that lag must hold"
}
key! {
    /// `mover.copy_chunk_rows`.
    COPY_CHUNK_ROWS: u64 = "mover.copy_chunk_rows", OPS, 5000, 1, 1_000_000,
    "Rows per chunk"
}
key! {
    /// `mover.max_copies_per_cluster`.
    MAX_COPIES_PER_CLUSTER: u64 = "mover.max_copies_per_cluster", OPS, 2, 1, 64,
    "Concurrent copies reading or writing one cluster"
}
key! {
    /// `mover.txn_grace_ms`.
    TXN_GRACE_MS: u64 = "mover.txn_grace_ms", OPS, 300, 0, 60_000,
    "Grace for open transactions at hold"
}
key! {
    /// `mover.hold_grace_ms`.
    HOLD_GRACE_MS: u64 = "mover.hold_grace_ms", OPS, 500, 0, 60_000,
    "Added to a hold's until"
}
key! {
    /// `mover.flip_reserve_ms`.
    FLIP_RESERVE_MS: u64 = "mover.flip_reserve_ms", OPS, 200, 0, 60_000,
    "Budget kept for commit and release"
}
key! {
    /// `mover.cutover_retry_seconds`.
    CUTOVER_RETRY_SECONDS: u64 = "mover.cutover_retry_seconds", OPS, 30, 0, 86_400,
    "Backoff after an aborted cutover, doubling"
}
key! {
    /// `mover.max_cutover_attempts`.
    MAX_CUTOVER_ATTEMPTS: u64 = "mover.max_cutover_attempts", OPS, 5, 1, 1000,
    "Then pause and alert"
}
key! {
    /// `mover.xa_wait_ms`.
    XA_WAIT_MS: u64 = "mover.xa_wait_ms", OPS, 5000, 0, 600_000,
    "Wait for in-flight distributed transactions"
}
key! {
    /// `mover.apply_batch_rows`.
    APPLY_BATCH_ROWS: u64 = "mover.apply_batch_rows", OPS, 2000, 1, 1_000_000,
    "Rows per target transaction"
}
key! {
    /// `mover.apply_batch_ms`.
    APPLY_BATCH_MS: u64 = "mover.apply_batch_ms", OPS, 50, 0, 60_000,
    "Longest batch accumulation"
}
key! {
    /// `mover.tick_interval_ms`.
    TICK_INTERVAL_MS: u64 = "mover.tick_interval_ms", OPS, 250, 10, 60_000,
    "Lag tick period"
}
key! {
    /// `mover.copy_max_rows_per_sec`.
    COPY_MAX_ROWS_PER_SEC: u64 = "mover.copy_max_rows_per_sec", OPS, 50_000, 1, 100_000_000,
    "Copy rate cap per job"
}
key! {
    /// `mover.copy_max_mb_per_sec`.
    COPY_MAX_MB_PER_SEC: u64 = "mover.copy_max_mb_per_sec", OPS, 64, 1, 100_000,
    "Copy bandwidth cap per job"
}
key! {
    /// `mover.throttle_threads_running`.
    THROTTLE_THREADS_RUNNING: u64 = "mover.throttle_threads_running", OPS, 64, 1, 100_000,
    "Source busy threshold"
}
key! {
    /// `mover.throttle_replica_lag_ms`.
    THROTTLE_REPLICA_LAG_MS: u64 = "mover.throttle_replica_lag_ms", OPS, 2000, 1, 86_400_000,
    "Replica lag threshold"
}
key! {
    /// `mover.throttle_flow_control_pct`.
    THROTTLE_FLOW_CONTROL_PCT: u64 = "mover.throttle_flow_control_pct", OPS, 5, 0, 100,
    "Galera flow control threshold"
}
key! {
    /// `mover.throttle_apply_lag_ms`.
    THROTTLE_APPLY_LAG_MS: u64 = "mover.throttle_apply_lag_ms", OPS, 5000, 1, 86_400_000,
    "Stop copying when catch-up lags this much"
}
key! {
    /// `mover.throttle_p99_increase_pct`.
    THROTTLE_P99_INCREASE_PCT: u64 = "mover.throttle_p99_increase_pct", OPS, 20, 1, 10_000,
    "Pause copying when a cluster's live p99 rises this much over its baseline"
}
key! {
    /// `mover.recovery_rate_pct`.
    RECOVERY_RATE_PCT: u64 = "mover.recovery_rate_pct", OPS, 25, 1, 100,
    "Copy rate, as a share of normal, while a cluster is recovering"
}
key! {
    /// `mover.verify_max_repairs`.
    VERIFY_MAX_REPAIRS: u64 = "mover.verify_max_repairs", OPS, 3, 0, 1000,
    "Repairs before failing"
}
key! {
    /// `mover.job_lease_seconds`.
    JOB_LEASE_SECONDS: u64 = "mover.job_lease_seconds", OPS, 10, 2, 3600,
    "Worker lease"
}
key! {
    /// `mover.max_pause_minutes`.
    MAX_PAUSE_MINUTES: u64 = "mover.max_pause_minutes", OPS, 60, 1, 100_000,
    "Longest tolerated unreachable target"
}
key! {
    /// `mover.source_retain_hours`.
    SOURCE_RETAIN_HOURS: u64 = "mover.source_retain_hours", OPS, 24, 0, 8760,
    "Fenced source kept before drop"
}
key! {
    /// `mover.server_id_base`.
    SERVER_ID_BASE: u64 = "mover.server_id_base", DC_GLOBAL, 4_000_000_000, 1_000_000, 4_199_999_000,
    "First binlog client server id, R0 only (from R1 the feed allocates)"
}

/// `mover.verify_mode`.
pub static VERIFY_MODE: Setting<Cow<'static, str>> = Setting {
    key: "mover.verify_mode",
    default: Cow::Borrowed("full"),
    scopes: TENANT_OPS,
    hard: HardBounds::one_of(&["full", "sample", "off"]),
    apply: Apply::Live,
    operator_only: true,
    owner: 10,
    doc: "full, sample or off",
};
sprig_settings::register!(VERIFY_MODE);

/// A snapshot of the keys for one job.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)] // one field per key above
pub struct MoverSettings {
    pub cutover_max_hold: Duration,
    pub ready_max_lag: Duration,
    pub ready_sustain: Duration,
    pub copy_chunk_rows: usize,
    pub max_copies_per_cluster: u32,
    pub txn_grace: Duration,
    pub hold_grace: Duration,
    pub flip_reserve: Duration,
    pub cutover_retry: Duration,
    pub max_cutover_attempts: u32,
    pub apply_batch_rows: usize,
    pub apply_batch: Duration,
    pub tick_interval: Duration,
    pub copy_max_rows_per_sec: u64,
    pub copy_max_mb_per_sec: u64,
    pub throttle_threads_running: u64,
    pub throttle_replica_lag_ms: u64,
    pub throttle_flow_control_pct: u64,
    pub throttle_apply_lag: Duration,
    pub verify_max_repairs: u32,
    pub job_lease: Duration,
    pub source_retain: Duration,
    pub server_id_base: u32,
}

fn ms(v: u64) -> Duration {
    Duration::from_millis(v)
}

fn usize_of(v: u64) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

fn u32_of(v: u64) -> u32 {
    u32::try_from(v).unwrap_or(u32::MAX)
}

impl Default for MoverSettings {
    fn default() -> Self {
        Self {
            cutover_max_hold: ms(CUTOVER_MAX_HOLD_MS.default),
            ready_max_lag: ms(READY_MAX_LAG_MS.default),
            ready_sustain: Duration::from_secs(READY_SUSTAIN_SECONDS.default),
            copy_chunk_rows: usize_of(COPY_CHUNK_ROWS.default),
            max_copies_per_cluster: u32_of(MAX_COPIES_PER_CLUSTER.default),
            txn_grace: ms(TXN_GRACE_MS.default),
            hold_grace: ms(HOLD_GRACE_MS.default),
            flip_reserve: ms(FLIP_RESERVE_MS.default),
            cutover_retry: Duration::from_secs(CUTOVER_RETRY_SECONDS.default),
            max_cutover_attempts: u32_of(MAX_CUTOVER_ATTEMPTS.default),
            apply_batch_rows: usize_of(APPLY_BATCH_ROWS.default),
            apply_batch: ms(APPLY_BATCH_MS.default),
            tick_interval: ms(TICK_INTERVAL_MS.default),
            copy_max_rows_per_sec: COPY_MAX_ROWS_PER_SEC.default,
            copy_max_mb_per_sec: COPY_MAX_MB_PER_SEC.default,
            throttle_threads_running: THROTTLE_THREADS_RUNNING.default,
            throttle_replica_lag_ms: THROTTLE_REPLICA_LAG_MS.default,
            throttle_flow_control_pct: THROTTLE_FLOW_CONTROL_PCT.default,
            throttle_apply_lag: ms(THROTTLE_APPLY_LAG_MS.default),
            verify_max_repairs: u32_of(VERIFY_MAX_REPAIRS.default),
            job_lease: Duration::from_secs(JOB_LEASE_SECONDS.default),
            source_retain: Duration::from_secs(SOURCE_RETAIN_HOURS.default * 3600),
            server_id_base: u32_of(SERVER_ID_BASE.default),
        }
    }
}

impl MoverSettings {
    /// Resolves every key for `ctx` (the job's tenant and source cluster).
    #[must_use]
    pub fn resolve(s: &MemSettings<'_>, ctx: &SettingCtx<'_>) -> Self {
        Self {
            cutover_max_hold: ms(s.get(&CUTOVER_MAX_HOLD_MS, ctx)),
            ready_max_lag: ms(s.get(&READY_MAX_LAG_MS, ctx)),
            ready_sustain: Duration::from_secs(s.get(&READY_SUSTAIN_SECONDS, ctx)),
            copy_chunk_rows: usize_of(s.get(&COPY_CHUNK_ROWS, ctx)),
            max_copies_per_cluster: u32_of(s.get(&MAX_COPIES_PER_CLUSTER, ctx)),
            txn_grace: ms(s.get(&TXN_GRACE_MS, ctx)),
            hold_grace: ms(s.get(&HOLD_GRACE_MS, ctx)),
            flip_reserve: ms(s.get(&FLIP_RESERVE_MS, ctx)),
            cutover_retry: Duration::from_secs(s.get(&CUTOVER_RETRY_SECONDS, ctx)),
            max_cutover_attempts: u32_of(s.get(&MAX_CUTOVER_ATTEMPTS, ctx)),
            apply_batch_rows: usize_of(s.get(&APPLY_BATCH_ROWS, ctx)),
            apply_batch: ms(s.get(&APPLY_BATCH_MS, ctx)),
            tick_interval: ms(s.get(&TICK_INTERVAL_MS, ctx)),
            copy_max_rows_per_sec: s.get(&COPY_MAX_ROWS_PER_SEC, ctx),
            copy_max_mb_per_sec: s.get(&COPY_MAX_MB_PER_SEC, ctx),
            throttle_threads_running: s.get(&THROTTLE_THREADS_RUNNING, ctx),
            throttle_replica_lag_ms: s.get(&THROTTLE_REPLICA_LAG_MS, ctx),
            throttle_flow_control_pct: s.get(&THROTTLE_FLOW_CONTROL_PCT, ctx),
            throttle_apply_lag: ms(s.get(&THROTTLE_APPLY_LAG_MS, ctx)),
            verify_max_repairs: u32_of(s.get(&VERIFY_MAX_REPAIRS, ctx)),
            job_lease: Duration::from_secs(s.get(&JOB_LEASE_SECONDS, ctx)),
            source_retain: Duration::from_secs(s.get(&SOURCE_RETAIN_HOURS, ctx) * 3600),
            server_id_base: u32_of(s.get(&SERVER_ID_BASE, ctx)),
        }
    }

    /// Settings for fast tests and simulations: small chunks and batches,
    /// short sustain, retry and retention.
    #[must_use]
    pub fn for_tests() -> Self {
        Self {
            ready_sustain: Duration::from_millis(500),
            ready_max_lag: Duration::from_millis(500),
            copy_chunk_rows: 7,
            cutover_retry: Duration::from_millis(500),
            apply_batch_rows: 16,
            source_retain: Duration::ZERO,
            max_cutover_attempts: 20,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::DcId;
    use sprig_settings::Registry;

    use super::*;

    #[test]
    fn keys_are_registered_with_the_books_defaults() {
        let reg = Registry::global().unwrap();
        for k in [
            "mover.cutover_max_hold_ms",
            "mover.copy_chunk_rows",
            "mover.verify_mode",
            "mover.server_id_base",
        ] {
            assert!(reg.get(k).is_some(), "{k}");
        }
        let s = MemSettings::new(reg);
        let dc = DcId::new("mum1").unwrap();
        let got = MoverSettings::resolve(&s, &SettingCtx::process(&dc));
        assert_eq!(got, MoverSettings::default());
        assert_eq!(got.cutover_max_hold, Duration::from_secs(2));
        assert_eq!(got.copy_chunk_rows, 5000);
    }
}
