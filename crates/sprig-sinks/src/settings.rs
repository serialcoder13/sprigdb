//! The `sink.*` settings, registered here.
//! `WITH (...)` on a sink sets a key at the sink's object scope.
//!
//! Not registered: `sink.region_tags` (the built-in table is used).

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

type Text = Cow<'static, str>;

/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);
/// Tenant, dc and global.
const TENANT_DC_GLOBAL: Scopes = Scopes(0b11_0100);
/// Object, tenant and global.
const OBJ_TENANT_GLOBAL: Scopes = Scopes(0b10_0101);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $op:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: $op,
            owner: 51,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `sink.enabled`.
    ENABLED: bool = "sink.enabled", true, TENANT_DC_GLOBAL,
    HardBounds::NONE, false, "Sinks may run at all"
);
key!(
    /// `sink.allowed_kinds`.
    ALLOWED_KINDS: Text = "sink.allowed_kinds", Cow::Borrowed("webhook,kafka,s3,database,script"), TENANT_GLOBAL,
    HardBounds::NONE, false, "Target kinds allowed"
);
key!(
    /// `sink.destination_allowlist`.
    DESTINATION_ALLOWLIST: Text = "sink.destination_allowlist", Cow::Borrowed("[]"), TENANT_GLOBAL,
    HardBounds::NONE, false, "Destinations sinks and sink scripts may reach"
);
key!(
    /// `sink.platform_identity`.
    PLATFORM_IDENTITY: Text = "sink.platform_identity", Cow::Borrowed("off"), TENANT_DC_GLOBAL,
    HardBounds::one_of(&["off", "assume_role", "full"]), true, "Whether sinks may use the worker's cloud identity"
);
key!(
    /// `sink.max_per_tenant`.
    MAX_PER_TENANT: u64 = "sink.max_per_tenant", 50, TENANT_GLOBAL,
    HardBounds::range(0, 100_000), false, "Sinks per tenant"
);
key!(
    /// `sink.partitions_per_task`.
    PARTITIONS_PER_TASK: u64 = "sink.partitions_per_task", 4, Scopes::DATA,
    HardBounds::range(1, 1024), false, "Source partitions or shards one task reads"
);
key!(
    /// `sink.fanin_refresh_seconds`: how often a fan-in sink's task looks
    /// for new member instances.
    FANIN_REFRESH_SECONDS: u64 = "sink.fanin_refresh_seconds", 30, Scopes::DATA,
    HardBounds::range(1, 3600), false, "How often a fan-in sink looks for new member instances"
);
key!(
    /// `sink.queue_lease_ms`: how long a queue source holds
    /// each message it took before it returns to the queue unsettled (a
    /// worker that died, a batch still retrying).
    QUEUE_LEASE_MS: u64 = "sink.queue_lease_ms", 300_000, Scopes::DATA,
    HardBounds::range(1_000, 86_400_000), false, "How long a queue sink holds a message it took before the queue gives it out again"
);
key!(
    /// `sink.fanin_instances_per_task`: member instance partitions one
    /// fan-in task reads.
    FANIN_INSTANCES_PER_TASK: u64 = "sink.fanin_instances_per_task", 256, Scopes::DATA,
    HardBounds::range(1, 100_000), false, "Member instance partitions one fan-in sink task reads"
);
key!(
    /// `sink.lanes`.
    LANES: u64 = "sink.lanes", 8, Scopes::DATA,
    HardBounds::range(1, 256), false, "Ordered lanes per task"
);
key!(
    /// `sink.batch_max_records`.
    BATCH_MAX_RECORDS: u64 = "sink.batch_max_records", 500, Scopes::DATA,
    HardBounds::range(1, 1_000_000), false, "Records per batch"
);
key!(
    /// `sink.batch_max_bytes`.
    BATCH_MAX_BYTES: u64 = "sink.batch_max_bytes", 4_194_304, Scopes::DATA,
    HardBounds::range(1024, 1_073_741_824), false, "Bytes per batch"
);
key!(
    /// `sink.batch_max_wait_ms`.
    BATCH_MAX_WAIT_MS: u64 = "sink.batch_max_wait_ms", 1000, Scopes::DATA,
    HardBounds::range(0, 3_600_000), false, "Longest wait to fill a batch"
);
key!(
    /// `sink.max_inflight_bytes`.
    MAX_INFLIGHT_BYTES: u64 = "sink.max_inflight_bytes", 67_108_864, OBJ_TENANT_GLOBAL,
    HardBounds::range(65_536, 17_179_869_184), false, "Read-ahead per task"
);
key!(
    /// `sink.retry_initial_ms`.
    RETRY_INITIAL_MS: u64 = "sink.retry_initial_ms", 500, Scopes::DATA,
    HardBounds::range(1, 3_600_000), false, "First backoff"
);
key!(
    /// `sink.retry_max_ms`.
    RETRY_MAX_MS: u64 = "sink.retry_max_ms", 60_000, Scopes::DATA,
    HardBounds::range(1, 86_400_000), false, "Longest backoff"
);
key!(
    /// `sink.record_max_attempts`.
    RECORD_MAX_ATTEMPTS: u64 = "sink.record_max_attempts", 10, Scopes::DATA,
    HardBounds::range(1, 1_000_000), false, "Retries of one record before it is dead-lettered"
);
key!(
    /// `sink.pause_after_minutes`.
    PAUSE_AFTER_MINUTES: u64 = "sink.pause_after_minutes", 15, Scopes::DATA,
    HardBounds::range(0, 100_000), false, "Destination-wide failure before pausing"
);
key!(
    /// `sink.auto_resume`.
    AUTO_RESUME: bool = "sink.auto_resume", true, Scopes::DATA,
    HardBounds::NONE, false, "Probe and resume a sink paused by failures"
);
key!(
    /// `sink.probe_interval_seconds`.
    PROBE_INTERVAL_SECONDS: u64 = "sink.probe_interval_seconds", 60, OBJ_TENANT_GLOBAL,
    HardBounds::range(1, 86_400), false, "Probe period while paused"
);
key!(
    /// `sink.dead_letter_retention_hours`.
    DEAD_LETTER_RETENTION_HOURS: u64 = "sink.dead_letter_retention_hours", 168, Scopes::DATA,
    HardBounds::range(1, 87_600), false, "Retention of a default dead-letter stream"
);
key!(
    /// `sink.lag_alert_seconds`.
    LAG_ALERT_SECONDS: u64 = "sink.lag_alert_seconds", 300, OBJ_TENANT_GLOBAL,
    HardBounds::range(1, 8_640_000), false, "Lag that raises SprigSinkLagHigh"
);
key!(
    /// `sink.transform_budget_ms`.
    TRANSFORM_BUDGET_MS: u64 = "sink.transform_budget_ms", 50, Scopes::DATA,
    HardBounds::range(1, 60_000), false, "Transform wall time per record"
);
key!(
    /// `sink.script_budget_ms`.
    SCRIPT_BUDGET_MS: u64 = "sink.script_budget_ms", 30_000, Scopes::DATA,
    HardBounds::range(1, 3_600_000), false, "Script wall time per batch"
);
key!(
    /// `sink.script_max_fetches`.
    SCRIPT_MAX_FETCHES: u64 = "sink.script_max_fetches", 1000, Scopes::DATA,
    HardBounds::range(0, 1_000_000), false, "Outbound calls per script batch"
);
key!(
    /// `sink.tenant_bytes_per_sec`.
    TENANT_BYTES_PER_SEC: u64 = "sink.tenant_bytes_per_sec", 0, TENANT_GLOBAL,
    HardBounds::range(0, i64::MAX), false, "Delivery quota per tenant; 0 = none"
);
key!(
    /// `sink.worker_max_tasks`.
    WORKER_MAX_TASKS: u64 = "sink.worker_max_tasks", 256, Scopes::PROCESS,
    HardBounds::range(1, 100_000), false, "Tasks one worker runs"
);
key!(
    /// `sink.tenant_worker_share_pct`.
    TENANT_WORKER_SHARE_PCT: u64 = "sink.tenant_worker_share_pct", 25, Scopes::PROCESS,
    HardBounds::range(1, 100), false, "Largest share of a worker's tasks one tenant may hold"
);
key!(
    /// `sink.claim_delay_ms`.
    CLAIM_DELAY_MS: u64 = "sink.claim_delay_ms", 2000, Scopes::PROCESS,
    HardBounds::range(0, 600_000), false, "Wait before a worker claims a task it doesn't rank first for"
);
key!(
    /// `sink.http_connect_timeout_ms`.
    HTTP_CONNECT_TIMEOUT_MS: u64 = "sink.http_connect_timeout_ms", 5000, OBJ_TENANT_GLOBAL,
    HardBounds::range(1, 600_000), false, "Connect timeout of webhooks and script fetches"
);
key!(
    /// `sink.http_timeout_ms`.
    HTTP_TIMEOUT_MS: u64 = "sink.http_timeout_ms", 30_000, OBJ_TENANT_GLOBAL,
    HardBounds::range(1, 3_600_000), false, "Request timeout"
);
key!(
    /// `sink.credential_previous_hours`.
    CREDENTIAL_PREVIOUS_HOURS: u64 = "sink.credential_previous_hours", 24, TENANT_GLOBAL,
    HardBounds::range(0, 8760), false, "How long a replaced credential stays usable"
);
key!(
    /// `sink.kafka.offsets_topic`.
    KAFKA_OFFSETS_TOPIC: Text = "sink.kafka.offsets_topic", Cow::Borrowed("__sprig_sink_offsets"), OBJ_TENANT_GLOBAL,
    HardBounds::NONE, false, "Positions topic of exactly-once Kafka sinks"
);
key!(
    /// `sink.kafka.transaction_timeout_ms`.
    KAFKA_TRANSACTION_TIMEOUT_MS: u64 = "sink.kafka.transaction_timeout_ms", 60_000, OBJ_TENANT_GLOBAL,
    HardBounds::range(1000, 900_000), false, "Kafka transaction timeout"
);
key!(
    /// `sink.s3.file_max_bytes`.
    S3_FILE_MAX_BYTES: u64 = "sink.s3.file_max_bytes", 134_217_728, Scopes::DATA,
    HardBounds::range(1024, 5_368_709_120), false, "Roll an S3 file at this size"
);
key!(
    /// `sink.s3.file_max_records`.
    S3_FILE_MAX_RECORDS: u64 = "sink.s3.file_max_records", 1_000_000, Scopes::DATA,
    HardBounds::range(1, 100_000_000), false, "Or at this many records"
);
key!(
    /// `sink.s3.file_max_age_seconds`.
    S3_FILE_MAX_AGE_SECONDS: u64 = "sink.s3.file_max_age_seconds", 300, Scopes::DATA,
    HardBounds::range(1, 86_400), false, "Or at this age"
);
key!(
    /// `sink.s3.part_bytes`.
    S3_PART_BYTES: u64 = "sink.s3.part_bytes", 16_777_216, Scopes::PROCESS,
    HardBounds::range(5_242_880, 5_368_709_120), false, "Multipart part size"
);
key!(
    /// `sink.db.batch_rows`.
    DB_BATCH_ROWS: u64 = "sink.db.batch_rows", 500, Scopes::DATA,
    HardBounds::range(1, 100_000), false, "Rows per statement and transaction"
);
key!(
    /// `sink.db.seen_retention_hours`.
    DB_SEEN_RETENTION_HOURS: u64 = "sink.db.seen_retention_hours", 168, OBJ_TENANT_GLOBAL,
    HardBounds::range(1, 87_600), false, "Keep queue sinks' seen keys this long"
);
key!(
    /// `sink.platform_secret_dir`.
    PLATFORM_SECRET_DIR: Text = "sink.platform_secret_dir", Cow::Borrowed("/etc/sprigdb/platform-secrets"), Scopes::PROCESS,
    HardBounds::NONE, true, "Where workers read platform connections' secret files"
);
key!(
    /// `sink.script_db_max_queries`.
    SCRIPT_DB_MAX_QUERIES: u64 = "sink.script_db_max_queries", 100, Scopes::DATA,
    HardBounds::range(0, 100_000), false, "Reads one script batch may make"
);
key!(
    /// `sink.script_db_max_rows`.
    SCRIPT_DB_MAX_ROWS: u64 = "sink.script_db_max_rows", 10_000, Scopes::DATA,
    HardBounds::range(0, 10_000_000), false, "Rows one script batch may read"
);
key!(
    /// `sink.script_db_budget_ms`.
    SCRIPT_DB_BUDGET_MS: u64 = "sink.script_db_budget_ms", 5000, Scopes::DATA,
    HardBounds::range(1, 3_600_000), false, "Time one script batch may spend in reads"
);

/// The `u64` keys a sink's `WITH (...)` may set, by name without `sink.`.
#[must_use]
pub fn u64_key(name: &str) -> Option<&'static Setting<u64>> {
    let k = format!("sink.{}", name.strip_prefix("sink.").unwrap_or(name));
    U64_KEYS.iter().copied().find(|s| s.key == k)
}

/// The `bool` keys a sink's `WITH (...)` may set.
#[must_use]
pub fn bool_key(name: &str) -> Option<&'static Setting<bool>> {
    let k = format!("sink.{}", name.strip_prefix("sink.").unwrap_or(name));
    [&AUTO_RESUME].into_iter().find(|s| s.key == k)
}

/// Every `u64` key.
pub static U64_KEYS: &[&Setting<u64>] = &[
    &MAX_PER_TENANT,
    &PARTITIONS_PER_TASK,
    &FANIN_REFRESH_SECONDS,
    &FANIN_INSTANCES_PER_TASK,
    &QUEUE_LEASE_MS,
    &LANES,
    &BATCH_MAX_RECORDS,
    &BATCH_MAX_BYTES,
    &BATCH_MAX_WAIT_MS,
    &MAX_INFLIGHT_BYTES,
    &RETRY_INITIAL_MS,
    &RETRY_MAX_MS,
    &RECORD_MAX_ATTEMPTS,
    &PAUSE_AFTER_MINUTES,
    &PROBE_INTERVAL_SECONDS,
    &DEAD_LETTER_RETENTION_HOURS,
    &LAG_ALERT_SECONDS,
    &TRANSFORM_BUDGET_MS,
    &SCRIPT_BUDGET_MS,
    &SCRIPT_MAX_FETCHES,
    &TENANT_BYTES_PER_SEC,
    &WORKER_MAX_TASKS,
    &TENANT_WORKER_SHARE_PCT,
    &CLAIM_DELAY_MS,
    &HTTP_CONNECT_TIMEOUT_MS,
    &HTTP_TIMEOUT_MS,
    &CREDENTIAL_PREVIOUS_HOURS,
    &KAFKA_TRANSACTION_TIMEOUT_MS,
    &S3_FILE_MAX_BYTES,
    &S3_FILE_MAX_RECORDS,
    &S3_FILE_MAX_AGE_SECONDS,
    &S3_PART_BYTES,
    &DB_BATCH_ROWS,
    &DB_SEEN_RETENTION_HOURS,
    &SCRIPT_DB_MAX_QUERIES,
    &SCRIPT_DB_MAX_ROWS,
    &SCRIPT_DB_BUDGET_MS,
];
