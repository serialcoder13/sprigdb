//! The engine's settings, registered here,
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

key!(
    /// `queue.owner_move_after_seconds`: consumers on another gateway this
    /// long move the owner there (0: never).
    OWNER_MOVE_AFTER_SECONDS: u64 = "queue.owner_move_after_seconds", 60, Scopes::PROCESS,
    HardBounds::range(0, 86_400), "Consumers on another gateway this long move a queue's owner there"
);
key!(
    /// `queue.owner_move_cooldown_seconds`: the damper on owner moves.
    OWNER_MOVE_COOLDOWN_SECONDS: u64 = "queue.owner_move_cooldown_seconds", 300, Scopes::PROCESS,
    HardBounds::range(0, 86_400), "Least time between two moves of one queue's owner"
);

key!(
    /// `queue.forwarder`: who forwards transactional publishes from the
    /// queue outbox. `feed`: the tenant's feed-driven
    /// forwarder (a worker job), in commit order; the gateways' polling
    /// forwarder takes only rows older than `queue.forward_fallback_ms`.
    /// `poll`: the gateways' polling forwarder at once, in insert order.
    FORWARDER: Text = "queue.forwarder", Cow::Borrowed("feed"), Scopes::DATA,
    HardBounds::one_of(&["feed", "poll"]), "Forward transactional publishes from the change feed (commit order) or by polling"
);
key!(
    /// `queue.forward_fallback_ms`: with `queue.forwarder = feed`, a
    /// transactional publish still in the outbox this long is forwarded
    /// by the gateways' polling forwarder (the feed is behind or its
    /// forwarder down); commit order isn't kept for such rows.
    FORWARD_FALLBACK_MS: u64 = "queue.forward_fallback_ms", 2_000, Scopes::DATA,
    HardBounds::range(200, 3_600_000), "Polling forwarder takes outbox rows older than this when the feed forwarder lags, in ms"
);

key!(
    /// `queue.change_exchange`: publish the object's committed changes to
    /// this exchange of the object's database (vhost), as the user who set
    /// it, filtered per change by that user's privileges and row-level
    /// security. Empty: none.
    CHANGE_EXCHANGE: Text = "queue.change_exchange", Cow::Borrowed(""), Scopes(0b00_0001),
    HardBounds::NONE, "Publish the object's changes to this exchange"
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
    /// `queue.takeover_grace_ms`.
    pub takeover_grace_ms: u64,
    /// `queue.owner_move_after_seconds`, in ms (0: never).
    pub owner_move_after_ms: u64,
    /// `queue.owner_move_cooldown_seconds`, in ms.
    pub owner_move_cooldown_ms: u64,
    /// `queue.forwarder = feed`.
    pub forward_feed: bool,
    /// `queue.forward_fallback_ms`.
    pub forward_fallback_ms: u64,
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
            takeover_grace_ms: 2000,
            owner_move_after_ms: 60_000,
            owner_move_cooldown_ms: 300_000,
            forward_feed: true,
            forward_fallback_ms: 2_000,
        }
    }
}
