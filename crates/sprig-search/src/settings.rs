//! `search.*` settings (chapter 39, "Configuration"), registered here so
//! every process that runs search (gateways, workers with the search
//! role) reads the same keys. Process scope (dc and global) for now
//! (decision S8).

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

type Text = Cow<'static, str>;

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: Scopes::PROCESS,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 39,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `search.commit_interval_ms`.
    COMMIT_INTERVAL_MS: u64 = "search.commit_interval_ms", 500, HardBounds::range(10, 60_000),
    "Tantivy commit period"
);
key!(
    /// `search.max_lag_ms`.
    MAX_LAG_MS: u64 = "search.max_lag_ms", 1000, HardBounds::range(10, 3_600_000),
    "Target freshness"
);
key!(
    /// `search.wait_timeout_ms`.
    WAIT_TIMEOUT_MS: u64 = "search.wait_timeout_ms", 2000, HardBounds::range(0, 600_000),
    "Longest consistency wait"
);
key!(
    /// `search.default_consistency`.
    DEFAULT_CONSISTENCY: Text = "search.default_consistency", Cow::Borrowed("stale_ok"), HardBounds::NONE,
    "stale_ok, own_writes or latest"
);
key!(
    /// `search.scoring`.
    SCORING: Text = "search.scoring", Cow::Borrowed("global"), HardBounds::NONE,
    "global or local statistics"
);
key!(
    /// `search.max_window`.
    MAX_WINDOW: u64 = "search.max_window", 10_000, HardBounds::range(1, 10_000_000),
    "Hits read per query"
);
key!(
    /// `search.max_expansions`.
    MAX_EXPANSIONS: u64 = "search.max_expansions", 50, HardBounds::range(1, 10_000),
    "Terms per fuzzy or prefix term"
);
key!(
    /// `search.highlight_chars`.
    HIGHLIGHT_CHARS: u64 = "search.highlight_chars", 150, HardBounds::range(20, 10_000),
    "Fragment length"
);
key!(
    /// `search.writer_memory_mb`.
    WRITER_MEMORY_MB: u64 = "search.writer_memory_mb", 32, HardBounds::range(15, 4096),
    "Tantivy writer budget per partition"
);
key!(
    /// `search.fulltext_ddl`.
    FULLTEXT_DDL: Text = "search.fulltext_ddl", Cow::Borrowed("convert"), HardBounds::NONE,
    "MySQL FULLTEXT DDL: convert, mariadb or reject"
);
key!(
    /// `search.max_indexes_per_object`.
    MAX_INDEXES_PER_OBJECT: u64 = "search.max_indexes_per_object", 4, HardBounds::range(1, 64),
    "Search indexes per object"
);

key!(
    /// `search.copies` (R5 round 6): copies of each index on workers with
    /// the search role; 0 keeps a copy in each gateway that uses the index
    /// (decision S1's mode).
    COPIES: u64 = "search.copies", 2, HardBounds::range(0, 8),
    "Copies per partition, on different workers"
);
key!(
    /// `search.snapshot_interval_minutes`.
    SNAPSHOT_INTERVAL_MINUTES: u64 = "search.snapshot_interval_minutes", 60, HardBounds::range(1, 10_080),
    "Snapshot period"
);
key!(
    /// `search.snapshot_keep`.
    SNAPSHOT_KEEP: u64 = "search.snapshot_keep", 3, HardBounds::range(1, 100),
    "Manifests kept"
);
key!(
    /// `search.query_timeout_ms`.
    QUERY_TIMEOUT_MS: u64 = "search.query_timeout_ms", 5000, HardBounds::range(100, 600_000),
    "Per partition"
);
key!(
    /// `search.overfetch_factor`.
    OVERFETCH_FACTOR: u64 = "search.overfetch_factor", 3, HardBounds::range(1, 100),
    "Page multiplier for residual filters and facets"
);
