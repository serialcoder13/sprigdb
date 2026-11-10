//! The keys of inherited messaging objects (chapter 42, "Configuration",
//! the R9 rows), and the cross-tenant publishing keys of chapters 45, 50
//! and 26 that the same code reads: `stream.descendant_publish`,
//! `stream.descendant_key`, `queue.descendant_publish`,
//! `queue.descendant_key` and `residency.cross_tenant_messaging_strict`.

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// Object, tenant and global.
const OBJECT_TENANT_GLOBAL: Scopes = Scopes(0b10_0101);
/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);
/// Global only.
const GLOBAL: Scopes = Scopes(0b10_0000);
/// `on` or `off`.
const ON_OFF: HardBounds = HardBounds::one_of(&["on", "off"]);

macro_rules! text {
    ($name:ident, $key:literal, $default:literal, $scopes:expr, $hard:expr, $op:literal, $owner:literal, $doc:literal) => {
        #[doc = concat!("`", $key, "`: ", $doc)]
        pub static $name: Setting<Cow<'static, str>> = Setting {
            key: $key,
            default: Cow::Borrowed($default),
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: $op,
            owner: $owner,
            doc: $doc,
        };
        sprig_settings::register!($name);
    };
}

macro_rules! number {
    ($name:ident, $key:literal, $default:literal, $scopes:expr, $min:literal, $max:literal, $owner:literal, $doc:literal) => {
        #[doc = concat!("`", $key, "`: ", $doc)]
        pub static $name: Setting<u64> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: false,
            owner: $owner,
            doc: $doc,
        };
        sprig_settings::register!($name);
    };
}

text!(
    INSTANCE_PRECREATE,
    "inherit.instance_precreate",
    "off",
    OBJECT_TENANT_GLOBAL,
    ON_OFF,
    false,
    42,
    "on: create a definition's instances for every member in the background instead of on first use"
);
number!(
    PRECREATE_RATE,
    "inherit.precreate_rate",
    50,
    Scopes::PROCESS,
    1,
    100_000,
    42,
    "Instances a precreate job creates per second"
);
number!(
    INSTANCE_CREATE_WAIT_MS,
    "inherit.instance_create_wait_ms",
    5000,
    TENANT_GLOBAL,
    100,
    600_000,
    42,
    "How long a publish or consumer waits for an instance being created (39899 after)"
);
text!(
    MEMBER_PUBLISH,
    "inherit.member_publish",
    "on",
    OBJECT_TENANT_GLOBAL,
    ON_OFF,
    false,
    42,
    "Whether a member may publish into its own instance; off leaves publishing to ancestors"
);
number!(
    DROP_GRACE_HOURS,
    "inherit.drop_grace_hours",
    168,
    OBJECT_TENANT_GLOBAL,
    0,
    87_600,
    42,
    "How long instances of a dropped definition drain before removal"
);
number!(
    MAX_DEFINITIONS_PER_TENANT,
    "inherit.max_definitions_per_tenant",
    200,
    TENANT_GLOBAL,
    1,
    100_000,
    42,
    "FOR EACH TENANT definitions one tenant may declare"
);
number!(
    MAX_INSTANCES_PER_TENANT,
    "inherit.max_instances_per_tenant",
    1000,
    TENANT_GLOBAL,
    1,
    1_000_000,
    42,
    "Inherited instances one member may hold"
);
number!(
    FAN_IN_SQL_MAX_TENANTS,
    "inherit.fan_in_sql_max_tenants",
    100,
    TENANT_GLOBAL,
    1,
    100_000,
    42,
    "Members one fan-in SELECT may read"
);
number!(
    READY_INDEX_DEBOUNCE_MS,
    "inherit.ready_index_debounce_ms",
    500,
    Scopes::PROCESS,
    0,
    60_000,
    42,
    "Least time between ready-index updates of one queue instance"
);
number!(
    BROADCAST_RATE,
    "inherit.broadcast_rate",
    2000,
    Scopes::PROCESS,
    1,
    1_000_000,
    42,
    "Member deliveries per second a broadcast fan-out makes per DC"
);
number!(
    BROADCASTS_PER_HOUR,
    "inherit.broadcasts_per_hour",
    60,
    OBJECT_TENANT_GLOBAL,
    1,
    100_000,
    42,
    "Broadcasts one definition may take per hour"
);
number!(
    BROADCAST_MAX_BYTES,
    "inherit.broadcast_max_bytes",
    65_536,
    OBJECT_TENANT_GLOBAL,
    1,
    16_777_216,
    42,
    "Largest broadcast record or message"
);
number!(
    BROADCAST_MAX_MEMBERS,
    "inherit.broadcast_max_members",
    100_000,
    OBJECT_TENANT_GLOBAL,
    1,
    10_000_000,
    42,
    "Most members one broadcast may reach"
);
text!(
    BROADCAST_REPLAY,
    "inherit.broadcast_replay",
    "off",
    OBJECT_TENANT_GLOBAL,
    ON_OFF,
    false,
    42,
    "on: a member's new instance gets the broadcasts still in the broadcast log"
);
number!(
    BROADCAST_RETENTION_HOURS,
    "inherit.broadcast_retention_hours",
    168,
    OBJECT_TENANT_GLOBAL,
    1,
    87_600,
    42,
    "How long broadcasts stay in the broadcast log"
);

number!(
    OUTBOX_DRAIN_SECONDS,
    "inherit.outbox_drain_seconds",
    30,
    Scopes::DATA,
    0,
    3600,
    42,
    "How long DROP DATABASE waits for its cross-tenant outbox to be forwarded (39245 after)"
);

text!(
    STREAM_DESCENDANT_PUBLISH,
    "stream.descendant_publish",
    "off",
    Scopes::DATA,
    ON_OFF,
    false,
    45,
    "on: descendants' own sessions may publish into this stream (IN TENANT); the owner's consent"
);
text!(
    STREAM_DESCENDANT_KEY,
    "stream.descendant_key",
    "origin",
    Scopes::DATA,
    HardBounds::one_of(&["origin", "any"]),
    false,
    45,
    "origin: a descendant's record is keyed by its own tenant name (39957 otherwise); any: its own key"
);
text!(
    QUEUE_DESCENDANT_PUBLISH,
    "queue.descendant_publish",
    "off",
    Scopes::DATA,
    ON_OFF,
    false,
    50,
    "on: descendants' own sessions may publish into this exchange; the owner's consent"
);
text!(
    QUEUE_DESCENDANT_KEY,
    "queue.descendant_key",
    "origin",
    Scopes::DATA,
    HardBounds::one_of(&["origin", "any"]),
    false,
    50,
    "origin: a descendant's routing key starts with its own tenant name; any: its own key"
);
text!(
    CROSS_TENANT_MESSAGING_STRICT,
    "residency.cross_tenant_messaging_strict",
    "off",
    GLOBAL,
    ON_OFF,
    true,
    26,
    "on: a client's residency also refuses messages published into another tenant's streams and queues"
);
