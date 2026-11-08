//! The `reads.*` keys (chapter 06, "Configuration"; Appendix B).

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

type Text = Cow<'static, str>;

/// Tenant (and ancestors) and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);

/// `reads.routing`: `auto` or `writer`.
pub static ROUTING: Setting<Text> = Setting {
    key: "reads.routing",
    default: Cow::Borrowed("auto"),
    scopes: Scopes::DATA,
    hard: HardBounds::one_of(&["auto", "writer"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 6,
    doc: "auto or writer",
};
sprig_settings::register!(ROUTING);

/// `reads.causal_scope`: `session` or `tenant_gateway`.
pub static CAUSAL_SCOPE: Setting<Text> = Setting {
    key: "reads.causal_scope",
    default: Cow::Borrowed("tenant_gateway"),
    scopes: TENANT_GLOBAL,
    hard: HardBounds::one_of(&["session", "tenant_gateway"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 6,
    doc: "session or tenant_gateway",
};
sprig_settings::register!(CAUSAL_SCOPE);

/// `reads.max_replica_lag_ms`.
pub static MAX_REPLICA_LAG_MS: Setting<u64> = Setting {
    key: "reads.max_replica_lag_ms",
    default: 1000,
    scopes: Scopes::ALL,
    hard: HardBounds::range(0, 86_400_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 6,
    doc: "Never read from a replica further behind",
};
sprig_settings::register!(MAX_REPLICA_LAG_MS);

/// `reads.replica_position_poll_ms`.
pub static REPLICA_POSITION_POLL_MS: Setting<u64> = Setting {
    key: "reads.replica_position_poll_ms",
    default: 250,
    scopes: Scopes::OPERATIONAL,
    hard: HardBounds::range(10, 60_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 6,
    doc: "Position polling",
};
sprig_settings::register!(REPLICA_POSITION_POLL_MS);

/// `reads.gtid_wait_timeout_ms`.
pub static GTID_WAIT_TIMEOUT_MS: Setting<u64> = Setting {
    key: "reads.gtid_wait_timeout_ms",
    default: 100,
    scopes: Scopes::ALL,
    hard: HardBounds::range(0, 60_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 6,
    doc: "Replica catch-up wait before reading from the writer",
};
sprig_settings::register!(GTID_WAIT_TIMEOUT_MS);

/// `reads.max_galera_lag_ms`.
pub static MAX_GALERA_LAG_MS: Setting<u64> = Setting {
    key: "reads.max_galera_lag_ms",
    default: 1000,
    scopes: Scopes::ALL,
    hard: HardBounds::range(0, 86_400_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 6,
    doc: "A Galera node further behind gets no reads until it catches up",
};
sprig_settings::register!(MAX_GALERA_LAG_MS);

/// `reads.galera_sync_wait_timeout_ms`.
pub static GALERA_SYNC_WAIT_TIMEOUT_MS: Setting<u64> = Setting {
    key: "reads.galera_sync_wait_timeout_ms",
    default: 300,
    scopes: Scopes::ALL,
    hard: HardBounds::range(1, 60_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 6,
    doc: "Galera sync-wait check before reading from the writer",
};
sprig_settings::register!(GALERA_SYNC_WAIT_TIMEOUT_MS);

/// `reads.galera_unconfirmed`: `check` or `writer` (decision D14).
pub static GALERA_UNCONFIRMED: Setting<Text> = Setting {
    key: "reads.galera_unconfirmed",
    default: Cow::Borrowed("check"),
    scopes: Scopes::ALL,
    hard: HardBounds::one_of(&["check", "writer"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 6,
    doc: "check or writer: a Galera read whose write no polled position shows yet",
};
sprig_settings::register!(GALERA_UNCONFIRMED);
