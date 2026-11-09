//! The `types.*` and `hierarchy.*` keys this crate reads (chapter 32,
//! "Configuration"). The gateway resolves them at tenant scope for each
//! typed statement ([`crate::exec::Limits`]). `types.rewrite_chunk_rows`,
//! `types.single_table_max_columns`, `types.mariadb_validation` and
//! `hierarchy.backfill_chunk_rows` belong to steps not built yet and are
//! not registered.

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

macro_rules! key {
    ($(#[$doc:meta])* $name:ident = $key:literal, $default:expr, $scopes:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<u64> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 32,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `types.max_depth`: the deepest type hierarchy (8 in Phase 1).
    TYPES_MAX_DEPTH = "types.max_depth", 8, Scopes::DATA, HardBounds::range(1, 8),
    "Deepest type hierarchy"
);
key!(
    /// `types.write_batch_rows`: keys per rewritten write statement.
    TYPES_WRITE_BATCH_ROWS = "types.write_batch_rows", 1000, Scopes::DATA,
    HardBounds::range(1, 100_000),
    "Keys per rewritten write statement on typed tables"
);
key!(
    /// `hierarchy.max_depth`: the default `MAX DEPTH`.
    HIERARCHY_MAX_DEPTH = "hierarchy.max_depth", 64, Scopes::DATA, HardBounds::range(1, 1000),
    "Default MAX DEPTH of a record hierarchy"
);
key!(
    /// `hierarchy.max_move_rows`: the largest subtree moved or cascaded at
    /// once.
    HIERARCHY_MAX_MOVE_ROWS = "hierarchy.max_move_rows", 100_000, Scopes::DATA,
    HardBounds::range(1, 100_000_000),
    "Largest subtree moved or cascaded at once"
);
