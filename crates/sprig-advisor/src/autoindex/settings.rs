//! The `auto_index.*` keys, declared next to the code that reads them.
//!
//! Every key accepts all six scopes, except the fast-trigger `scan_*` keys,
//! which accept global, tenant, database and object. All apply live.
//! Keys about shared cluster resources (`build_window`,
//! `abort_replica_lag_seconds`, `max_builds_per_cluster`) can only be
//! tightened at data scopes; that rule is [`policy`](super::policy)'s, not
//! the registry's.
//!
//! `auto_index.max_builds_per_cluster` ([`MAX_BUILDS_PER_CLUSTER`]) is
//! declared here, once, with the rollout that uses it; the controller's
//! job scheduler reads the same key for its `build` slot pool. It keeps the
//! attributes it had when the scheduler declared it: operator-only, bounds
//! 1-64, every scope (data scopes only tighten it, [`policy`](super::policy)).
//!
//! Hard bounds protect the code from nonsense; they are this crate's
//! choice, and wide.

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

type Text = Cow<'static, str>;

/// `on`, `approve`, `recommend` or `off`.
pub const MODES: &[&str] = &["on", "approve", "recommend", "off"];

/// The key of `auto_index.max_builds_per_cluster`.
pub const MAX_BUILDS_PER_CLUSTER_KEY: &str = "auto_index.max_builds_per_cluster";

/// `auto_index.max_builds_per_cluster`: concurrent index
/// builds per cluster, the `build` slot pool of the controller's job
/// scheduler.
pub static MAX_BUILDS_PER_CLUSTER: Setting<u32> = Setting {
    key: MAX_BUILDS_PER_CLUSTER_KEY,
    default: 1,
    scopes: Scopes::ALL,
    hard: HardBounds::range(1, 64),
    apply: Apply::Live,
    operator_only: true,
    owner: 13,
    doc: "Concurrent index builds per cluster",
};
sprig_settings::register!(MAX_BUILDS_PER_CLUSTER);

/// Global, tenant, database and object: the fast-trigger keys.
pub const SCAN_SCOPES: Scopes = Scopes::DATA;

macro_rules! keys {
    ($(
        $(#[$doc:meta])*
        $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $text:literal;
    )*) => {
        $(
            $(#[$doc])*
            pub static $name: Setting<$t> = Setting {
                key: $key,
                default: $default,
                scopes: $scopes,
                hard: $hard,
                apply: Apply::Live,
                operator_only: false,
                owner: 13,
                doc: $text,
            };
            sprig_settings::register!($name);
        )*

        /// Every key this module declares, for tests and documentation.
        pub static ALL: &[&'static dyn sprig_settings::AnySetting] = &[$(&$name),*];
    };
}

const fn f(min: f64, max: f64) -> HardBounds {
    HardBounds {
        min: Some(min),
        max: Some(max),
        allowed: None,
    }
}

keys! {
    /// `auto_index.mode`.
    MODE: Text = "auto_index.mode", Cow::Borrowed("on"), Scopes::ALL,
        HardBounds::one_of(MODES), "on, approve, recommend or off";
    /// `auto_index.window_hours`.
    WINDOW_HOURS: u32 = "auto_index.window_hours", 24, Scopes::ALL,
        HardBounds::range(1, 720), "Trigger window";
    /// `auto_index.include_studio`.
    INCLUDE_STUDIO: bool = "auto_index.include_studio", false, Scopes::ALL,
        HardBounds::NONE, "Count statements run from Studio toward triggers";
    /// `auto_index.min_runs`.
    MIN_RUNS: u32 = "auto_index.min_runs", 50, Scopes::ALL,
        HardBounds::range(1, 1_000_000), "Runs needed in the window";
    /// `auto_index.p95_threshold_ms`.
    P95_THRESHOLD_MS: f64 = "auto_index.p95_threshold_ms", 100.0, Scopes::ALL,
        f(1.0, 3_600_000.0), "Slower than reasonable";
    /// `auto_index.top_n`.
    TOP_N: u32 = "auto_index.top_n", 10, Scopes::ALL,
        HardBounds::range(0, 1000), "Or among the top N by total time...";
    /// `auto_index.min_examined_per_returned`.
    MIN_EXAMINED_PER_RETURNED: f64 = "auto_index.min_examined_per_returned", 100.0, Scopes::ALL,
        f(1.0, 1e12), "...while examining this many rows per row returned";
    /// `auto_index.max_per_table`.
    MAX_PER_TABLE: u32 = "auto_index.max_per_table", 5, Scopes::ALL,
        HardBounds::range(1, 64), "Automatic indexes per table";
    /// `auto_index.max_storage_pct`.
    MAX_STORAGE_PCT: f64 = "auto_index.max_storage_pct", 20.0, Scopes::ALL,
        f(0.0, 1000.0), "Extra storage per table";
    /// `auto_index.min_storage_budget_mb`: the storage budget's floor, so a
    /// table's budget is `max(max_storage_pct of its size, this)` (the
    /// 64 MB default may change).
    MIN_STORAGE_BUDGET_MB: f64 = "auto_index.min_storage_budget_mb", 64.0, Scopes::ALL,
        f(0.0, 1_048_576.0), "Smallest storage budget per table, MB";
    /// `auto_index.min_improvement_pct`.
    MIN_IMPROVEMENT_PCT: f64 = "auto_index.min_improvement_pct", 30.0, Scopes::ALL,
        f(1.0, 99.0), "p95 improvement to accept";
    /// `auto_index.max_regression_pct`.
    MAX_REGRESSION_PCT: f64 = "auto_index.max_regression_pct", 10.0, Scopes::ALL,
        f(0.0, 1000.0), "Largest slowdown of any sampled statement";
    /// `auto_index.observe_days`.
    OBSERVE_DAYS: u32 = "auto_index.observe_days", 7, Scopes::ALL,
        HardBounds::range(1, 365), "Watch period after a build";
    /// `auto_index.usage_sample_reads`: an index is judged on its table's
    /// reads, never on days. It is judged once the table has
    /// had this many reads since it was built or last judged, and an
    /// `IGNORED` index is watched for this many reads before it is dropped.
    USAGE_SAMPLE_READS: u64 = "auto_index.usage_sample_reads", 100_000, Scopes::ALL,
        HardBounds::range(1, 1_000_000_000_000),
        "Reads per usage judgement, and reads watched while IGNORED";
    /// `auto_index.min_usage_ratio`: an index serving fewer than this share
    /// of those reads, whose saved time is less than its write cost, is
    /// removed. A fraction: 0.05 is 5%.
    MIN_USAGE_RATIO: f64 = "auto_index.min_usage_ratio", 0.05, Scopes::ALL,
        f(0.0, 1.0), "Share of the reads an index must serve to be kept";
    /// `auto_index.retire_user_indexes`: whether user and migrated indexes
    /// are removed by the same rule (`on`), only recommended for removal
    /// (`recommend`) or left alone (`off`).
    RETIRE_USER_INDEXES: Text = "auto_index.retire_user_indexes", Cow::Borrowed("on"), Scopes::ALL,
        HardBounds::one_of(MODES), "on, recommend or off for user and migrated indexes";
    /// `auto_index.eval_interval_minutes`.
    EVAL_INTERVAL_MINUTES: u32 = "auto_index.eval_interval_minutes", 60, Scopes::ALL,
        HardBounds::range(1, 10_080), "Evaluation period";
    /// `auto_index.min_examined_samples`.
    MIN_EXAMINED_SAMPLES: u32 = "auto_index.min_examined_samples", 5, Scopes::ALL,
        HardBounds::range(1, 1_000_000), "Probe samples needed for the second trigger";
    /// `auto_index.cooldown_hours`.
    COOLDOWN_HOURS: u32 = "auto_index.cooldown_hours", 24, Scopes::ALL,
        HardBounds::range(0, 8760), "Before re-deciding a fingerprint";
    /// `auto_index.max_cover_columns`.
    MAX_COVER_COLUMNS: u32 = "auto_index.max_cover_columns", 3, Scopes::ALL,
        HardBounds::range(0, 16), "Columns added for covering";
    /// `auto_index.max_key_bytes`. `InnoDB`'s limit is 3072.
    MAX_KEY_BYTES: u32 = "auto_index.max_key_bytes", 512, Scopes::ALL,
        HardBounds::range(16, 3072), "Longest automatic key";
    /// `auto_index.lookup_cost_factor`.
    LOOKUP_COST_FACTOR: f64 = "auto_index.lookup_cost_factor", 2.0, Scopes::ALL,
        f(0.0, 100.0), "Extra cost of a primary-key lookup per entry";
    /// `auto_index.maintenance_cost_ms`.
    MAINTENANCE_COST_MS: f64 = "auto_index.maintenance_cost_ms", 0.05, Scopes::ALL,
        f(0.0, 1000.0), "Cost of one index entry change";
    /// `auto_index.min_net_ms_per_hour`.
    MIN_NET_MS_PER_HOUR: f64 = "auto_index.min_net_ms_per_hour", 100.0, Scopes::ALL,
        f(0.0, 1e12), "Smallest net benefit";
    /// `auto_index.min_benefit_ratio`.
    MIN_BENEFIT_RATIO: f64 = "auto_index.min_benefit_ratio", 2.0, Scopes::ALL,
        f(0.0, 1000.0), "Benefit over write cost";
    /// `auto_index.payback_hours`.
    PAYBACK_HOURS: f64 = "auto_index.payback_hours", 24.0, Scopes::ALL,
        f(0.1, 87_600.0), "Build must pay back within this";
    /// `auto_index.build_rate_mb_per_sec`.
    BUILD_RATE_MB_PER_SEC: f64 = "auto_index.build_rate_mb_per_sec", 20.0, Scopes::ALL,
        f(0.01, 100_000.0), "Build rate before one is measured";
    /// `auto_index.max_build_hours`.
    MAX_BUILD_HOURS: f64 = "auto_index.max_build_hours", 6.0, Scopes::ALL,
        f(0.01, 720.0), "Longest build";
    /// `auto_index.max_new_per_day`.
    MAX_NEW_PER_DAY: u32 = "auto_index.max_new_per_day", 5, Scopes::ALL,
        HardBounds::range(0, 10_000), "New indexes per tenant per day";
    /// `auto_index.replay_samples`.
    REPLAY_SAMPLES: u32 = "auto_index.replay_samples", 20, Scopes::ALL,
        HardBounds::range(1, 1000), "Samples per fingerprint";
    /// `auto_index.replay_rounds`.
    REPLAY_ROUNDS: u32 = "auto_index.replay_rounds", 3, Scopes::ALL,
        HardBounds::range(1, 50), "A/B rounds per sample";
    /// `auto_index.replay_timeout_seconds`.
    REPLAY_TIMEOUT_SECONDS: u32 = "auto_index.replay_timeout_seconds", 10, Scopes::ALL,
        HardBounds::range(1, 3600), "Statement limit during replay";
    /// `auto_index.regression_fingerprints`.
    REGRESSION_FINGERPRINTS: u32 = "auto_index.regression_fingerprints", 20, Scopes::ALL,
        HardBounds::range(0, 1000), "Other fingerprints replayed";
    /// `auto_index.estimate_margin`.
    ESTIMATE_MARGIN: f64 = "auto_index.estimate_margin", 1.5, Scopes::ALL,
        f(1.0, 10.0), "Stricter prediction without a replica";
    /// `auto_index.canary_hours`.
    CANARY_HOURS: u32 = "auto_index.canary_hours", 24, Scopes::ALL,
        HardBounds::range(1, 720), "Canary observation";
    /// `auto_index.build_window`.
    BUILD_WINDOW: Text = "auto_index.build_window", Cow::Borrowed("auto"), Scopes::ALL,
        HardBounds::NONE, "auto or HH:MM-HH:MM UTC";
    /// `auto_index.ddl_lock_wait_seconds`.
    DDL_LOCK_WAIT_SECONDS: u32 = "auto_index.ddl_lock_wait_seconds", 5, Scopes::ALL,
        HardBounds::range(1, 3600), "Lock wait for index DDL";
    /// `auto_index.abort_replica_lag_seconds`.
    ABORT_REPLICA_LAG_SECONDS: u32 = "auto_index.abort_replica_lag_seconds", 300, Scopes::ALL,
        HardBounds::range(1, 86_400), "Abort a build above this lag";
    /// `auto_index.rollout_pause_minutes`.
    ROLLOUT_PAUSE_MINUTES: u32 = "auto_index.rollout_pause_minutes", 10, Scopes::ALL,
        HardBounds::range(0, 1440), "Between shards";
    /// `auto_index.recommend_validate`.
    RECOMMEND_VALIDATE: bool = "auto_index.recommend_validate", true, Scopes::ALL,
        HardBounds::NONE, "Validate in recommend mode";
    /// `auto_index.recommend_ttl_days`.
    RECOMMEND_TTL_DAYS: u32 = "auto_index.recommend_ttl_days", 14, Scopes::ALL,
        HardBounds::range(1, 3650), "Recommendation lifetime";
    /// `auto_index.vector_ann`.
    VECTOR_ANN: Text = "auto_index.vector_ann", Cow::Borrowed("recommend"), Scopes::ALL,
        HardBounds::one_of(MODES), "on, recommend or off for ANN indexes";
    /// `auto_index.vector_min_recall`.
    VECTOR_MIN_RECALL: f64 = "auto_index.vector_min_recall", 0.95, Scopes::ALL,
        f(0.0, 1.0), "Recall@k to accept an ANN index";
    /// `auto_index.lookup_scatter_pct`.
    LOOKUP_SCATTER_PCT: f64 = "auto_index.lookup_scatter_pct", 80.0, Scopes::ALL,
        f(0.0, 100.0), "Scatter share for a lookup index";
    /// `auto_index.field_column_class_pct`.
    FIELD_COLUMN_CLASS_PCT: f64 = "auto_index.field_column_class_pct", 99.0, Scopes::ALL,
        f(50.0, 100.0), "Single-class share for a typed field column, else a pair";
    /// `auto_index.wildcard_recommend_paths`.
    WILDCARD_RECOMMEND_PATHS: u32 = "auto_index.wildcard_recommend_paths", 50, Scopes::ALL,
        HardBounds::range(1, 100_000),
        "Distinct filtered paths that trigger a wildcard recommendation";
    /// `auto_index.recreate_block_days`.
    RECREATE_BLOCK_DAYS: u32 = "auto_index.recreate_block_days", 14, Scopes::ALL,
        HardBounds::range(0, 3650), "Block on re-proposing a definition dropped as harmful";
    /// `auto_index.scan_min_runs`.
    SCAN_MIN_RUNS: u32 = "auto_index.scan_min_runs", 5, SCAN_SCOPES,
        HardBounds::range(1, 100_000), "Fast trigger: large-scan runs needed in the window";
    /// `auto_index.scan_min_examined`.
    SCAN_MIN_EXAMINED: u64 = "auto_index.scan_min_examined", 1_000_000, SCAN_SCOPES,
        HardBounds::range(1, 1_000_000_000_000), "Fast trigger: rows examined per run";
    /// `auto_index.scan_max_returned_ratio`.
    SCAN_MAX_RETURNED_RATIO: f64 = "auto_index.scan_max_returned_ratio", 0.01, SCAN_SCOPES,
        f(0.0, 1.0), "Fast trigger: rows returned per row examined, at most";
    /// `auto_index.scan_min_sessions`. At most 8: the workload store keeps
    /// 8 session hashes per bucket.
    SCAN_MIN_SESSIONS: u32 = "auto_index.scan_min_sessions", 2, SCAN_SCOPES,
        HardBounds::range(1, 8), "Fast trigger: distinct sessions among those runs";
    /// `auto_index.scan_recommend_above_gb`.
    SCAN_RECOMMEND_ABOVE_GB: f64 = "auto_index.scan_recommend_above_gb", 100.0, SCAN_SCOPES,
        f(0.0, 1e9), "Fast trigger only recommends on larger tables";
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_settings::{Registry, ScopeKind};

    use super::*;

    #[test]
    fn every_key_is_in_the_global_registry_and_valid() {
        let reg = Registry::global().unwrap();
        for s in ALL {
            let got = reg.get(s.key()).unwrap();
            assert_eq!(got.owner(), 13);
            assert!(got.key().starts_with("auto_index."));
        }
        // The full key table with min_storage_budget_mb and the three
        // usage keys, without the removed day-based ones:
        // 52 here, and max_builds_per_cluster declared on its own
        // (operator-only).
        assert_eq!(ALL.len(), 52);
        assert!(!ALL.iter().any(|s| s.key() == MAX_BUILDS_PER_CLUSTER_KEY));
        let m = reg.get(MAX_BUILDS_PER_CLUSTER_KEY).unwrap();
        assert_eq!(m.owner(), 13);
        assert_eq!(MAX_BUILDS_PER_CLUSTER.default, 1);
    }

    #[test]
    fn no_key_decides_by_days_of_disuse() {
        // Removal is judged by reads, never by days.
        for gone in [
            "auto_index.unused_drop_days",
            "auto_index.drop_unused_days",
            "auto_index.ignore_before_drop_hours",
            "auto_index.usage_check_hours",
        ] {
            assert!(!ALL.iter().any(|s| s.key() == gone), "{gone}");
        }
        for k in [
            &USAGE_SAMPLE_READS as &dyn sprig_settings::AnySetting,
            &MIN_USAGE_RATIO,
            &RETIRE_USER_INDEXES,
        ] {
            for scope in [
                ScopeKind::Global,
                ScopeKind::Tenant,
                ScopeKind::Database,
                ScopeKind::Object,
            ] {
                assert!(k.scopes().contains(scope), "{} at {scope:?}", k.key());
            }
        }
    }

    #[test]
    fn scan_keys_take_data_scopes_only() {
        for s in ALL {
            let scan = s.key().starts_with("auto_index.scan_");
            assert_eq!(
                !s.scopes().contains(ScopeKind::Cluster),
                scan,
                "{}",
                s.key()
            );
            assert!(s.scopes().contains(ScopeKind::Object));
            assert!(s.scopes().contains(ScopeKind::Global));
        }
    }

    #[test]
    fn defaults_are_kirans() {
        assert_eq!(MIN_RUNS.default, 50);
        assert!((P95_THRESHOLD_MS.default - 100.0).abs() < f64::EPSILON);
        assert_eq!(TOP_N.default, 10);
        assert!((MIN_EXAMINED_PER_RETURNED.default - 100.0).abs() < f64::EPSILON);
        assert_eq!(MAX_PER_TABLE.default, 5);
        assert!((MAX_STORAGE_PCT.default - 20.0).abs() < f64::EPSILON);
        assert!((MIN_STORAGE_BUDGET_MB.default - 64.0).abs() < f64::EPSILON);
        assert!((MIN_IMPROVEMENT_PCT.default - 30.0).abs() < f64::EPSILON);
        assert!((MAX_REGRESSION_PCT.default - 10.0).abs() < f64::EPSILON);
        assert_eq!(USAGE_SAMPLE_READS.default, 100_000);
        assert!((MIN_USAGE_RATIO.default - 0.05).abs() < f64::EPSILON);
        assert_eq!(RETIRE_USER_INDEXES.default, "on");
        assert_eq!(MODE.default, "on");
    }
}
