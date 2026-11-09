//! The engine's settings (chapter 50, "Configuration"), registered here,
//! and [`QueueSettings`], the values one vhost runs with.

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

type Text = Cow<'static, str>;

/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 50,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `queue.write_batch_max_rows`.
    WRITE_BATCH_MAX_ROWS: u64 = "queue.write_batch_max_rows", 500, Scopes::PROCESS,
    HardBounds::range(1, 100_000), "Rows per queue owner write batch"
);
key!(
    /// `queue.owner_cache_mb`.
    OWNER_CACHE_MB: u64 = "queue.owner_cache_mb", 64, Scopes::ALL,
    HardBounds::range(1, 65_536), "Head cache of one queue owner, in MiB"
);
key!(
    /// `queue.confirm_mode`: `commit` or `replica`.
    CONFIRM_MODE: Text = "queue.confirm_mode", Cow::Borrowed("commit"), Scopes::DATA,
    HardBounds::one_of(&["commit", "replica"]), "Publisher confirms after commit or after a replica applied it"
);
key!(
    /// `queue.delivery_limit_default`.
    DELIVERY_LIMIT_DEFAULT: u64 = "queue.delivery_limit_default", 20, Scopes::DATA,
    HardBounds::range(0, 1_000_000), "Quorum queues' delivery limit"
);
key!(
    /// `queue.delay_max_ms`.
    DELAY_MAX_MS: u64 = "queue.delay_max_ms", 4_294_967_295, TENANT_GLOBAL,
    HardBounds::range(0, 4_294_967_295), "Longest x-delay of a delayed message"
);
key!(
    /// `queue.default_type`.
    DEFAULT_TYPE: Text = "queue.default_type", Cow::Borrowed("classic"), Scopes::DATA,
    HardBounds::one_of(&["classic", "quorum"]), "Queue type when none is declared"
);
key!(
    /// `amqp.consumer_timeout_ms`.
    CONSUMER_TIMEOUT_MS: u64 = "amqp.consumer_timeout_ms", 1_800_000, Scopes::ALL,
    HardBounds::range(0, 604_800_000), "Unacknowledged delivery limit per consumer, in ms (0 = none)"
);
key!(
    /// `amqp.publish_timeout_ms`.
    PUBLISH_TIMEOUT_MS: u64 = "amqp.publish_timeout_ms", 10_000, TENANT_GLOBAL,
    HardBounds::range(100, 600_000), "Nack an unconfirmed publish after this many ms"
);
key!(
    /// `amqp.quota_publish_bytes_per_sec`.
    QUOTA_PUBLISH_BYTES_PER_SEC: u64 = "amqp.quota_publish_bytes_per_sec", 0, TENANT_GLOBAL,
    HardBounds::range(0, i64::MAX), "Publish quota of a tenant, bytes per second (0 = none)"
);
key!(
    /// `queue.takeover_grace_ms`.
    TAKEOVER_GRACE_MS: u64 = "queue.takeover_grace_ms", 2000, Scopes::PROCESS,
    HardBounds::range(0, 60_000), "Re-attachment window after an owner takeover, in ms"
);

/// What one vhost's queues run with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueSettings {
    /// Rows per write batch.
    pub batch_rows: usize,
    /// Head cache per owner.
    pub cache_bytes: u64,
    /// `commit` (false) or `replica` (true).
    pub confirm_replica: bool,
    /// Quorum delivery limit.
    pub delivery_limit_default: i64,
    /// Longest delay.
    pub delay_max_ms: u64,
    /// Default queue type is quorum.
    pub default_quorum: bool,
    /// Consumer timeout (0 = none).
    pub consumer_timeout_ms: u64,
    /// Confirm timeout.
    pub publish_timeout_ms: u64,
    /// Publish quota (0 = none).
    pub quota_bytes_per_sec: u64,
    /// Prefetch 0 cap.
    pub max_unacked: u32,
}

impl Default for QueueSettings {
    fn default() -> Self {
        Self {
            batch_rows: 500,
            cache_bytes: 64 << 20,
            confirm_replica: false,
            delivery_limit_default: 20,
            delay_max_ms: 4_294_967_295,
            default_quorum: false,
            consumer_timeout_ms: 1_800_000,
            publish_timeout_ms: 10_000,
            quota_bytes_per_sec: 0,
            max_unacked: 65_535,
        }
    }
}
