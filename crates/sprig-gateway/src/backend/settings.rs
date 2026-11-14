//! The `backend.*` keys and [`BackendSettings`], the resolved values pools
//! and routing read.
//!
//! Every key is live. [`LiveBackendSettings`] resolves them per cluster
//! ("cluster-scope values resolve against the pool's cluster") and caches
//! the result until the settings mirror changes.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arc_swap::ArcSwap;
use sprig_common::ids::{ClusterId, DcId};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};
use tokio::sync::watch;

use crate::reads::settings as reads;

type Text = Cow<'static, str>;


macro_rules! key {
    ($NAME:ident, $t:ty, $key:literal, $default:expr, $scopes:expr, $hard:expr, $doc:literal) => {
        #[doc = concat!("`", $key, "`: ", $doc, ".")]
        pub static $NAME: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 6,
            doc: $doc,
        };
        sprig_settings::register!($NAME);
    };
}

key!(
    POOL_MODE,
    Text,
    "backend.pool_mode",
    Cow::Borrowed("transaction"),
    Scopes::OPERATIONAL,
    HardBounds::one_of(&["transaction", "session"]),
    "transaction or session"
);
key!(
    MAX_CONNS_PER_NODE,
    u32,
    "backend.max_conns_per_node",
    256,
    Scopes::OPERATIONAL,
    HardBounds::range(1, 100_000),
    "Per gateway, across a node's pools"
);
key!(
    MAX_CONNS_PER_ACCOUNT_NODE,
    u32,
    "backend.max_conns_per_account_node",
    32,
    Scopes::OPERATIONAL,
    HardBounds::range(1, 100_000),
    "Per gateway, one pool"
);
key!(
    ACQUIRE_TIMEOUT_MS,
    u64,
    "backend.acquire_timeout_ms",
    2000,
    Scopes::OPERATIONAL,
    HardBounds::range(1, 600_000),
    "Wait for a connection"
);
key!(
    CONNECT_TIMEOUT_MS,
    u64,
    "backend.connect_timeout_ms",
    2000,
    Scopes::OPERATIONAL,
    HardBounds::range(1, 600_000),
    "TCP, TLS and init"
);
key!(
    IDLE_TIMEOUT_SECONDS,
    u64,
    "backend.idle_timeout_seconds",
    300,
    Scopes::OPERATIONAL,
    HardBounds::range(1, 86_400),
    "Close idle connections"
);
key!(
    MAX_LIFETIME_SECONDS,
    u64,
    "backend.max_lifetime_seconds",
    3600,
    Scopes::OPERATIONAL,
    HardBounds::range(1, 604_800),
    "Retire connections"
);
key!(
    NET_WRITE_TIMEOUT_SECONDS,
    u64,
    "backend.net_write_timeout_seconds",
    600,
    Scopes::OPERATIONAL,
    HardBounds::range(1, 86_400),
    "Tolerates slow clients"
);
key!(
    HEALTH_INTERVAL_MS,
    u64,
    "backend.health_interval_ms",
    2000,
    Scopes::OPERATIONAL,
    HardBounds::range(50, 600_000),
    "Active checks"
);
key!(
    HEALTH_FAILURES_TO_DOWN,
    u32,
    "backend.health_failures_to_down",
    3,
    Scopes::OPERATIONAL,
    HardBounds::range(1, 1000),
    "Failures before Down"
);
key!(
    PREPARED_BUDGET_PCT,
    u32,
    "backend.prepared_budget_pct",
    50,
    Scopes::OPERATIONAL,
    HardBounds::range(1, 100),
    "Share of max_prepared_stmt_count for all gateways"
);
key!(
    COMPRESS_CROSS_DC,
    bool,
    "backend.compress_cross_dc",
    true,
    Scopes::OPERATIONAL,
    HardBounds::NONE,
    "zlib on links to nodes in other DCs"
);

/// `backend.pool_mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolMode {
    /// Borrow per statement, hold while pinned (the default).
    Transaction,
    /// One backend connection per session.
    Session,
}

/// `reads.routing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Routing {
    /// Plain reads may go to replicas and other Galera nodes.
    Auto,
    /// Everything goes to the writer.
    Writer,
}

/// `reads.causal_scope`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CausalScope {
    /// The session's own last commit.
    Session,
    /// Also every commit by the tenant through this gateway.
    TenantGateway,
}

/// `reads.galera_unconfirmed`: where a Galera read goes when it must see a
/// write and no node but the writer has a polled position covering it yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GaleraUnconfirmed {
    /// The least-loaded node that isn't behind, after the bounded check
    /// (the default).
    Check,
    /// The writer.
    Writer,
}

/// Resolved `backend.*` and `reads.*` values for one cluster.
#[derive(Debug, Clone, PartialEq)]
pub struct BackendSettings {
    /// `backend.pool_mode`.
    pub pool_mode: PoolMode,
    /// `backend.max_conns_per_node`.
    pub max_conns_per_node: u32,
    /// `backend.max_conns_per_account_node`.
    pub max_conns_per_account_node: u32,
    /// `backend.acquire_timeout_ms`.
    pub acquire_timeout: Duration,
    /// `backend.connect_timeout_ms`.
    pub connect_timeout: Duration,
    /// `backend.idle_timeout_seconds`.
    pub idle_timeout: Duration,
    /// `backend.max_lifetime_seconds`.
    pub max_lifetime: Duration,
    /// `backend.net_write_timeout_seconds`.
    pub net_write_timeout_secs: u64,
    /// `backend.health_interval_ms`.
    pub health_interval: Duration,
    /// `backend.health_failures_to_down`.
    pub health_failures_to_down: u32,
    /// `backend.prepared_budget_pct`.
    pub prepared_budget_pct: u32,
    /// `backend.compress_cross_dc`.
    pub compress_cross_dc: bool,
    /// `reads.routing`.
    pub routing: Routing,
    /// `reads.causal_scope`.
    pub causal_scope: CausalScope,
    /// `reads.max_replica_lag_ms`.
    pub max_replica_lag: Duration,
    /// `reads.replica_position_poll_ms`.
    pub position_poll: Duration,
    /// `reads.gtid_wait_timeout_ms`.
    pub gtid_wait_timeout: Duration,
    /// `reads.max_galera_lag_ms`.
    pub max_galera_lag: Duration,
    /// `reads.galera_sync_wait_timeout_ms`.
    pub galera_sync_wait_timeout: Duration,
    /// `reads.galera_unconfirmed`.
    pub galera_unconfirmed: GaleraUnconfirmed,
}

impl Default for BackendSettings {
    /// The registry defaults.
    fn default() -> Self {
        Self {
            pool_mode: PoolMode::Transaction,
            max_conns_per_node: MAX_CONNS_PER_NODE.default,
            max_conns_per_account_node: MAX_CONNS_PER_ACCOUNT_NODE.default,
            acquire_timeout: Duration::from_millis(ACQUIRE_TIMEOUT_MS.default),
            connect_timeout: Duration::from_millis(CONNECT_TIMEOUT_MS.default),
            idle_timeout: Duration::from_secs(IDLE_TIMEOUT_SECONDS.default),
            max_lifetime: Duration::from_secs(MAX_LIFETIME_SECONDS.default),
            net_write_timeout_secs: NET_WRITE_TIMEOUT_SECONDS.default,
            health_interval: Duration::from_millis(HEALTH_INTERVAL_MS.default),
            health_failures_to_down: HEALTH_FAILURES_TO_DOWN.default,
            prepared_budget_pct: PREPARED_BUDGET_PCT.default,
            compress_cross_dc: COMPRESS_CROSS_DC.default,
            routing: Routing::Auto,
            causal_scope: CausalScope::TenantGateway,
            max_replica_lag: Duration::from_millis(reads::MAX_REPLICA_LAG_MS.default),
            position_poll: Duration::from_millis(reads::REPLICA_POSITION_POLL_MS.default),
            gtid_wait_timeout: Duration::from_millis(reads::GTID_WAIT_TIMEOUT_MS.default),
            max_galera_lag: Duration::from_millis(reads::MAX_GALERA_LAG_MS.default),
            galera_sync_wait_timeout: Duration::from_millis(
                reads::GALERA_SYNC_WAIT_TIMEOUT_MS.default,
            ),
            galera_unconfirmed: GaleraUnconfirmed::Check,
        }
    }
}

impl BackendSettings {
    /// Resolves every key in `ctx`.
    #[must_use]
    pub fn resolve(live: &LiveSettings, ctx: &SettingCtx<'_>) -> Self {
        let snap = live.snapshot();
        Self {
            pool_mode: if snap.get(&POOL_MODE, ctx) == "session" {
                PoolMode::Session
            } else {
                PoolMode::Transaction
            },
            max_conns_per_node: snap.get(&MAX_CONNS_PER_NODE, ctx),
            max_conns_per_account_node: snap.get(&MAX_CONNS_PER_ACCOUNT_NODE, ctx),
            acquire_timeout: Duration::from_millis(snap.get(&ACQUIRE_TIMEOUT_MS, ctx)),
            connect_timeout: Duration::from_millis(snap.get(&CONNECT_TIMEOUT_MS, ctx)),
            idle_timeout: Duration::from_secs(snap.get(&IDLE_TIMEOUT_SECONDS, ctx)),
            max_lifetime: Duration::from_secs(snap.get(&MAX_LIFETIME_SECONDS, ctx)),
            net_write_timeout_secs: snap.get(&NET_WRITE_TIMEOUT_SECONDS, ctx),
            health_interval: Duration::from_millis(snap.get(&HEALTH_INTERVAL_MS, ctx)),
            health_failures_to_down: snap.get(&HEALTH_FAILURES_TO_DOWN, ctx),
            prepared_budget_pct: snap.get(&PREPARED_BUDGET_PCT, ctx),
            compress_cross_dc: snap.get(&COMPRESS_CROSS_DC, ctx),
            routing: if snap.get(&reads::ROUTING, ctx) == "writer" {
                Routing::Writer
            } else {
                Routing::Auto
            },
            causal_scope: if snap.get(&reads::CAUSAL_SCOPE, ctx) == "session" {
                CausalScope::Session
            } else {
                CausalScope::TenantGateway
            },
            max_replica_lag: Duration::from_millis(snap.get(&reads::MAX_REPLICA_LAG_MS, ctx)),
            position_poll: Duration::from_millis(snap.get(&reads::REPLICA_POSITION_POLL_MS, ctx)),
            gtid_wait_timeout: Duration::from_millis(snap.get(&reads::GTID_WAIT_TIMEOUT_MS, ctx)),
            max_galera_lag: Duration::from_millis(snap.get(&reads::MAX_GALERA_LAG_MS, ctx)),
            galera_sync_wait_timeout: Duration::from_millis(
                snap.get(&reads::GALERA_SYNC_WAIT_TIMEOUT_MS, ctx),
            ),
            galera_unconfirmed: if snap.get(&reads::GALERA_UNCONFIRMED, ctx) == "writer" {
                GaleraUnconfirmed::Writer
            } else {
                GaleraUnconfirmed::Check
            },
        }
    }
}

/// Where pools and routing read their settings.
pub trait SettingsSource: Send + Sync {
    /// The values for `cluster` (process scopes when `None`).
    fn settings(&self, cluster: Option<&ClusterId>) -> Arc<BackendSettings>;

    /// A receiver bumped when any value may have changed, if the source
    /// changes at all.
    fn changes(&self) -> Option<watch::Receiver<u64>>;
}

/// Fixed values, replaceable at run time: tests and development without
/// etcd.
#[derive(Debug)]
pub struct FixedSettings {
    current: ArcSwap<BackendSettings>,
    tx: watch::Sender<u64>,
}

impl FixedSettings {
    /// A source holding `s`.
    #[must_use]
    pub fn new(s: BackendSettings) -> Arc<Self> {
        Arc::new(Self {
            current: ArcSwap::from_pointee(s),
            tx: watch::channel(0).0,
        })
    }

    /// Replaces the values, as a live settings change would.
    pub fn set(&self, s: BackendSettings) {
        self.current.store(Arc::new(s));
        self.tx.send_modify(|g| *g += 1);
    }

    /// The current values.
    #[must_use]
    pub fn get(&self) -> Arc<BackendSettings> {
        self.current.load_full()
    }
}

impl SettingsSource for FixedSettings {
    fn settings(&self, _cluster: Option<&ClusterId>) -> Arc<BackendSettings> {
        self.current.load_full()
    }

    fn changes(&self) -> Option<watch::Receiver<u64>> {
        Some(self.tx.subscribe())
    }
}

/// Resolved values and the mirror generation they were resolved at.
type Cached = (u64, Arc<BackendSettings>);

/// Values from the settings mirror, resolved per cluster and cached until
/// the mirror changes.
#[derive(Debug)]
pub struct LiveBackendSettings {
    live: LiveSettings,
    dc: DcId,
    generation: watch::Receiver<u64>,
    cache: Mutex<HashMap<Option<ClusterId>, Cached>>,
}

impl LiveBackendSettings {
    /// A source over `live` for a process in `dc`.
    #[must_use]
    pub fn new(live: LiveSettings, dc: DcId) -> Arc<Self> {
        let generation = live.changes();
        Arc::new(Self {
            live,
            dc,
            generation,
            cache: Mutex::new(HashMap::new()),
        })
    }
}

impl SettingsSource for LiveBackendSettings {
    fn settings(&self, cluster: Option<&ClusterId>) -> Arc<BackendSettings> {
        let generation = *self.generation.borrow();
        let key = cluster.cloned();
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((g, s)) = cache.get(&key)
            && *g == generation
        {
            return Arc::clone(s);
        }
        let mut ctx = SettingCtx::process(&self.dc);
        ctx.cluster = cluster;
        let s = Arc::new(BackendSettings::resolve(&self.live, &ctx));
        cache.insert(key, (generation, Arc::clone(&s)));
        s
    }

    fn changes(&self) -> Option<watch::Receiver<u64>> {
        Some(self.live.changes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_registered_with_appendix_b_defaults() {
        let r = sprig_settings::Registry::global().unwrap();
        for key in [
            "backend.pool_mode",
            "backend.max_conns_per_node",
            "backend.max_conns_per_account_node",
            "backend.acquire_timeout_ms",
            "backend.connect_timeout_ms",
            "backend.idle_timeout_seconds",
            "backend.max_lifetime_seconds",
            "backend.net_write_timeout_seconds",
            "backend.health_interval_ms",
            "backend.health_failures_to_down",
            "backend.prepared_budget_pct",
            "backend.compress_cross_dc",
            "reads.routing",
            "reads.causal_scope",
            "reads.max_replica_lag_ms",
            "reads.replica_position_poll_ms",
            "reads.gtid_wait_timeout_ms",
            "reads.max_galera_lag_ms",
            "reads.galera_sync_wait_timeout_ms",
            "reads.galera_unconfirmed",
        ] {
            let s = r.get(key).unwrap_or_else(|| panic!("{key} not registered"));
            assert_eq!(s.owner(), 6, "{key}");
        }
        let d = BackendSettings::default();
        assert_eq!(d.max_conns_per_node, 256);
        assert_eq!(d.max_conns_per_account_node, 32);
        assert_eq!(d.acquire_timeout, Duration::from_secs(2));
        assert_eq!(d.gtid_wait_timeout, Duration::from_millis(100));
        assert_eq!(d.position_poll, Duration::from_millis(250));
        assert_eq!(d.max_galera_lag, Duration::from_millis(1000));
        assert_eq!(d.galera_sync_wait_timeout, Duration::from_millis(300));
        assert_eq!(d.galera_unconfirmed, GaleraUnconfirmed::Check);
        assert_eq!(d.pool_mode, PoolMode::Transaction);
    }

    #[test]
    fn fixed_settings_change_live() {
        let f = FixedSettings::new(BackendSettings::default());
        let mut rx = f.changes().unwrap();
        f.set(BackendSettings {
            max_conns_per_node: 3,
            ..BackendSettings::default()
        });
        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();
        assert_eq!(f.settings(None).max_conns_per_node, 3);
    }
}
