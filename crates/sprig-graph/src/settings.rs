//! The `graph.*` settings (chapter 43, "Configuration") this build reads.
//! Keys for features not built yet (partitioning, the plugin, algorithms)
//! are not registered.

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

use crate::exec::Budgets;

/// Tenant, dc and global.
const TENANT_DC_GLOBAL: Scopes = Scopes(0b11_0100);
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
            owner: 43,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `graph.max_hops`.
    MAX_HOPS: u64 = "graph.max_hops", 15, Scopes::DATA, HardBounds::range(1, 1000),
    "Most hops of any graph expansion, unbounded patterns included"
);
key!(
    /// `graph.max_frontier`.
    MAX_FRONTIER: u64 = "graph.max_frontier", 100_000, Scopes::DATA, HardBounds::range(1, 100_000_000),
    "Nodes or paths in one level of a graph expansion"
);
key!(
    /// `graph.max_rows`.
    MAX_ROWS: u64 = "graph.max_rows", 1_000_000, Scopes::DATA, HardBounds::range(1, 1_000_000_000),
    "Rows or paths one graph pattern may produce"
);
key!(
    /// `graph.max_time_ms`.
    MAX_TIME_MS: u64 = "graph.max_time_ms", 30_000, Scopes::DATA, HardBounds::range(1, 86_400_000),
    "Wall time of one graph statement, in milliseconds"
);
key!(
    /// `graph.max_memory_mb`.
    MAX_MEMORY_MB: u64 = "graph.max_memory_mb", 256, TENANT_DC_GLOBAL, HardBounds::range(1, 1_048_576),
    "Gateway memory of one graph statement, in MiB"
);
key!(
    /// `graph.batch_nodes`.
    BATCH_NODES: u64 = "graph.batch_nodes", 1000, Scopes::PROCESS, HardBounds::range(1, 100_000),
    "Frontier ids per shard statement"
);
key!(
    /// `graph.local_burst_hops`.
    LOCAL_BURST_HOPS: u64 = "graph.local_burst_hops", 4, Scopes::DATA, HardBounds::range(1, 64),
    "Hops one per-shard recursive CTE runs"
);
key!(
    /// `graph.max_detach_edges`.
    MAX_DETACH_EDGES: u64 = "graph.max_detach_edges", 100_000, TENANT_GLOBAL, HardBounds::range(1, 100_000_000),
    "Relationships one DETACH DELETE may remove"
);
key!(
    /// `graph.traversal_tier`: as built, `auto` and `cte` behave alike;
    /// `plugin` isn't available.
    TRAVERSAL_TIER: Cow<'static, str> = "graph.traversal_tier", Cow::Borrowed("auto"), Scopes::ALL,
    HardBounds::one_of(&["auto", "cte", "gateway", "plugin"]),
    "Traversal tier: auto, cte, gateway or plugin"
);

key!(
    /// `graph.mirror` (round 5): `async` mirrors are kept by the worker's
    /// applier once it serves the graph; until then, and under `sync` or
    /// `off` (not built: kept as `sync`), writers keep them in their own
    /// transaction.
    MIRROR: Cow<'static, str> = "graph.mirror", Cow::Borrowed("async"), Scopes::DATA,
    HardBounds::one_of(&["off", "async", "sync"]),
    "Mirrors of incoming relationships: off, async or sync"
);
key!(
    /// `graph.mirror_max_wait_ms` (round 5).
    MIRROR_MAX_WAIT_MS: u64 = "graph.mirror_max_wait_ms", 1000, Scopes::DATA, HardBounds::range(0, 60_000),
    "Wait for the mirror applier to reach the session's causal target before scattering"
);

/// The budgets from settings, read through `get`.
#[must_use]
pub fn budgets(get: &dyn Fn(&'static Setting<u64>) -> u64) -> Budgets {
    let n = |s: &'static Setting<u64>| usize::try_from(get(s)).unwrap_or(usize::MAX);
    Budgets {
        max_hops: u32::try_from(get(&MAX_HOPS)).unwrap_or(15),
        max_frontier: n(&MAX_FRONTIER),
        max_rows: n(&MAX_ROWS),
        max_time: std::time::Duration::from_millis(get(&MAX_TIME_MS)),
        max_memory: n(&MAX_MEMORY_MB).saturating_mul(1 << 20),
        batch_nodes: n(&BATCH_NODES),
        local_burst_hops: u32::try_from(get(&LOCAL_BURST_HOPS)).unwrap_or(4),
        max_detach_edges: n(&MAX_DETACH_EDGES),
    }
}
