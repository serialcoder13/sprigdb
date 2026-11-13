//! The `inherit.*` keys this crate reads (chapter 42, "Configuration").
//! The gateway resolves them at tenant scope for each statement
//! ([`crate::Limits`]). The `copy_*`, `federated_*` keys belong to the
//! materialized and federated modes, which are not built, and are not
//! registered.

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);

/// `inherit.parent_delete`: `keep` or `follow`.
pub static PARENT_DELETE: Setting<Cow<'static, str>> = Setting {
    key: "inherit.parent_delete",
    default: Cow::Borrowed("keep"),
    scopes: Scopes::DATA,
    hard: HardBounds::one_of(&["keep", "follow"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 42,
    doc: "keep or follow: a child's override when the parent deletes the record",
};
sprig_settings::register!(PARENT_DELETE);

/// `inherit.merge`: the merge mode of new shares.
pub static MERGE: Setting<Cow<'static, str>> = Setting {
    key: "inherit.merge",
    default: Cow::Borrowed("auto"),
    scopes: Scopes::DATA,
    hard: HardBounds::one_of(&["auto", "read_through", "materialized", "federated"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 42,
    doc: "auto, read_through, materialized or federated (federated isn't offered in this release)",
};
sprig_settings::register!(MERGE);

/// `inherit.override`: `field` or `record`.
pub static OVERRIDE: Setting<Cow<'static, str>> = Setting {
    key: "inherit.override",
    default: Cow::Borrowed("field"),
    scopes: Scopes::DATA,
    hard: HardBounds::one_of(&["field", "record"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 42,
    doc: "field or record: what an update to an inherited record overrides",
};
sprig_settings::register!(OVERRIDE);

/// `inherit.max_depth`: the longest share chain of one object.
pub static MAX_DEPTH: Setting<u64> = Setting {
    key: "inherit.max_depth",
    default: 16,
    scopes: TENANT_GLOBAL,
    hard: HardBounds::range(1, 64),
    apply: Apply::Live,
    operator_only: false,
    owner: 42,
    doc: "Longest share chain of one object",
};
sprig_settings::register!(MAX_DEPTH);

/// `inherit.read_through_max_depth`: read-through levels in one view.
pub static READ_THROUGH_MAX_DEPTH: Setting<u64> = Setting {
    key: "inherit.read_through_max_depth",
    default: 4,
    scopes: Scopes::DATA,
    hard: HardBounds::range(1, 64),
    apply: Apply::Live,
    operator_only: false,
    owner: 42,
    doc: "Read-through levels before a share must be materialized",
};
sprig_settings::register!(READ_THROUGH_MAX_DEPTH);

/// `inherit.sync_orphan_max_rows`: rows per key orphaned in the deleting
/// transaction (R6 orphans every row synchronously; the key is read for
/// the decision log only).
pub static SYNC_ORPHAN_MAX_ROWS: Setting<u64> = Setting {
    key: "inherit.sync_orphan_max_rows",
    default: 1000,
    scopes: TENANT_GLOBAL,
    hard: HardBounds::range(1, 1_000_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 42,
    doc: "Rows per key orphaned in the deleting transaction",
};
sprig_settings::register!(SYNC_ORPHAN_MAX_ROWS);
