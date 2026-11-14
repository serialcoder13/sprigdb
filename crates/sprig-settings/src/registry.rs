//! The setting registry: every key declared once, next to the code that uses
//! it (chapter 03, "Declaring a key").
//!
//! ```
//! use sprig_settings::{Apply, HardBounds, Scopes, Setting};
//!
//! pub static MIN_RUNS: Setting<u32> = Setting {
//!     key: "auto_index.min_runs",
//!     default: 50,
//!     scopes: Scopes::ALL,
//!     hard: HardBounds::range(1, 1_000_000),
//!     apply: Apply::Live,
//!     operator_only: false,
//!     owner: 13,
//!     doc: "Runs needed in auto_index.window_hours before a fingerprint qualifies",
//! };
//! sprig_settings::register!(MIN_RUNS);
//! ```

use std::collections::BTreeMap;
use std::sync::OnceLock;

use sprig_common::{Result, SprigError};

use crate::scope::ScopeKind;
use crate::value::{SettingValue, Value, ValueKind};

/// When a changed value takes effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Apply {
    /// At once, everywhere.
    Live,
    /// Read once at login: a session never sees a change mid-transaction.
    NewSessions,
    /// A listener re-binds (chapter 04).
    Rebind,
}

/// The scopes a key accepts, as bits: object 0, database 1, tenant 2,
/// cluster 3, dc 4, global 5.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scopes(pub u8);

impl Scopes {
    /// Object, database, tenant, cluster, dc and global.
    pub const ALL: Scopes = Scopes(0b11_1111);
    /// Object, database, tenant and global.
    pub const DATA: Scopes = Scopes(0b10_0111);
    /// Cluster, dc and global.
    pub const OPERATIONAL: Scopes = Scopes(0b11_1000);
    /// Dc and global.
    pub const PROCESS: Scopes = Scopes(0b11_0000);
    /// Tenant (and every ancestor), dc and global: the `auth.*` keys.
    pub const TREE: Scopes = Scopes(0b11_0100);

    /// Whether a value at `kind` is accepted. Ancestors count as tenant; the
    /// built-in default always applies.
    #[must_use]
    pub const fn contains(self, kind: ScopeKind) -> bool {
        let bit = match kind {
            ScopeKind::Object => 0,
            ScopeKind::Database => 1,
            ScopeKind::Tenant | ScopeKind::Ancestor(_) => 2,
            ScopeKind::Cluster => 3,
            ScopeKind::Dc => 4,
            ScopeKind::Global => 5,
            ScopeKind::Default => return true,
        };
        self.0 & (1 << bit) != 0
    }
}

/// Bounds no scope can go outside of, fixed in code. They protect the code
/// from nonsense such as a zero pool size; scope bounds are policy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HardBounds {
    /// Smallest numeric value.
    pub min: Option<f64>,
    /// Largest numeric value.
    pub max: Option<f64>,
    /// The only text values accepted.
    pub allowed: Option<&'static [&'static str]>,
}

impl HardBounds {
    /// No hard bounds.
    pub const NONE: HardBounds = HardBounds {
        min: None,
        max: None,
        allowed: None,
    };

    /// An inclusive numeric range.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub const fn range(min: i64, max: i64) -> Self {
        HardBounds {
            min: Some(min as f64),
            max: Some(max as f64),
            allowed: None,
        }
    }

    /// A fixed set of text values.
    #[must_use]
    pub const fn one_of(values: &'static [&'static str]) -> Self {
        HardBounds {
            min: None,
            max: None,
            allowed: Some(values),
        }
    }
}

/// A declared key of type `T`.
#[derive(Debug)]
pub struct Setting<T: SettingValue> {
    /// Dotted `snake_case`, prefixed by the owning component; units in the name.
    pub key: &'static str,
    /// The built-in default.
    pub default: T,
    /// Scopes that may set it.
    pub scopes: Scopes,
    /// Bounds no scope can go outside of.
    pub hard: HardBounds,
    /// When a change takes effect.
    pub apply: Apply,
    /// `true`: tenants may not set it at all.
    pub operator_only: bool,
    /// The chapter that owns the key.
    pub owner: u8,
    /// One line for Appendix B and `sprigctl config explain`. Required.
    pub doc: &'static str,
}

/// A declared key with its type erased, as the registry holds it.
pub trait AnySetting: Send + Sync + 'static {
    /// The key.
    fn key(&self) -> &'static str;
    /// The registry type.
    fn kind(&self) -> ValueKind;
    /// The built-in default.
    fn default_value(&self) -> Value;
    /// Scopes that may set it.
    fn scopes(&self) -> Scopes;
    /// Hard bounds.
    fn hard(&self) -> HardBounds;
    /// When a change takes effect.
    fn apply(&self) -> Apply;
    /// Tenants may not set it.
    fn operator_only(&self) -> bool;
    /// Owning chapter.
    fn owner(&self) -> u8;
    /// Documentation line.
    fn doc(&self) -> &'static str;
    /// `v` in the key's canonical form, or `None` if it doesn't fit the type.
    fn normalize(&self, v: &Value) -> Option<Value>;
}

impl<T: SettingValue> AnySetting for Setting<T> {
    fn key(&self) -> &'static str {
        self.key
    }
    fn kind(&self) -> ValueKind {
        T::KIND
    }
    fn default_value(&self) -> Value {
        self.default.to_value()
    }
    fn scopes(&self) -> Scopes {
        self.scopes
    }
    fn hard(&self) -> HardBounds {
        self.hard
    }
    fn apply(&self) -> Apply {
        self.apply
    }
    fn operator_only(&self) -> bool {
        self.operator_only
    }
    fn owner(&self) -> u8 {
        self.owner
    }
    fn doc(&self) -> &'static str {
        self.doc
    }
    fn normalize(&self, v: &Value) -> Option<Value> {
        T::from_value(v).map(|t| t.to_value())
    }
}

// SAFETY: linkme places each registered `&'static dyn AnySetting` in one
// link section and reads them back as a slice; the elements are plain
// references with no initialization order. `#[link_section]` counts as unsafe
// code in edition 2024. Tested by `global_registry_collects_registered_keys`.
#[allow(unsafe_code)]
/// Every key registered with [`register!`](crate::register) in the binary.
#[linkme::distributed_slice]
pub static SETTINGS: [&'static dyn AnySetting];

/// Registers a `static` [`Setting`] in the process-wide registry.
#[macro_export]
macro_rules! register {
    ($setting:path) => {
        const _: () = {
            // SAFETY: see `sprig_settings::registry::SETTINGS`.
            #[allow(unsafe_code)]
            #[$crate::linkme::distributed_slice($crate::registry::SETTINGS)]
            #[linkme(crate = $crate::linkme)]
            static REGISTERED: &'static dyn $crate::AnySetting = &$setting;
        };
    };
}

/// What the [`MARKED`] table adds to a key's declaration. Kept apart from
/// [`Setting`], so marking a key doesn't touch its declaration (or every
/// key's, as a new field would).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    /// Registered ahead of its code: nothing reads it yet. It stays
    /// registered, so a stored value isn't rejected and `SET` still
    /// checks it, but listings ([`Registry::iter`]: `SHOW SPRIG
    /// SETTINGS`, the Admin API's key list, the generated settings page)
    /// leave it out until it is built.
    Unbuilt,
    /// Read once when the process starts, though `apply` says otherwise:
    /// a change applies at the next restart. Shown as `restart` where the
    /// apply mode is shown.
    Restart,
}

/// The marked keys, with why (bugfix-89's settings round, smallfix, 14 Nov
/// 2026). Remove a key's line when it is built or made live; the
/// `settings_honoured` test fails on a marked key something reads, and on
/// an unread key that isn't marked.
pub const MARKED: &[(&str, Mark, &str)] = &[
    (
        "auto_index.field_column_class_pct",
        Mark::Unbuilt,
        "chapter 13's field-column class rule",
    ),
    (
        "auto_index.lookup_scatter_pct",
        Mark::Unbuilt,
        "chapter 13's lookup-scatter rule",
    ),
    (
        "auto_index.wildcard_recommend_paths",
        Mark::Unbuilt,
        "chapter 13's wildcard advice",
    ),
    (
        "catalog.ddl_wait_ms",
        Mark::Unbuilt,
        "the DDL path waits the default; no read of the key",
    ),
    (
        "decisions.retention_days",
        Mark::Unbuilt,
        "decision-log retention",
    ),
    (
        "failover.quarantine_days",
        Mark::Unbuilt,
        "the quarantine of a failed-over node",
    ),
    (
        "graph.traversal_tier",
        Mark::Unbuilt,
        "chapter 43's traversal tiers",
    ),
    (
        "inherit.instance_precreate",
        Mark::Unbuilt,
        "instance precreation",
    ),
    (
        "inherit.precreate_rate",
        Mark::Unbuilt,
        "instance precreation",
    ),
    (
        "inherit.sync_orphan_max_rows",
        Mark::Unbuilt,
        "the orphan sync's cap",
    ),
    (
        "mover.max_pause_minutes",
        Mark::Unbuilt,
        "the mover's pause limit",
    ),
    (
        "mover.recovery_rate_pct",
        Mark::Unbuilt,
        "the mover's recovery rate",
    ),
    (
        "mover.throttle_p99_increase_pct",
        Mark::Unbuilt,
        "the mover's p99 throttle",
    ),
    (
        "mover.verify_mode",
        Mark::Unbuilt,
        "the mover's verify modes",
    ),
    ("mover.xa_wait_ms", Mark::Unbuilt, "the mover's XA wait"),
    (
        "queue.descendant_key",
        Mark::Unbuilt,
        "AMQP publishing into an ancestor's exchange",
    ),
    (
        "queue.descendant_publish",
        Mark::Unbuilt,
        "AMQP publishing into an ancestor's exchange",
    ),
    (
        "recovery.max_latency_increase_pct",
        Mark::Unbuilt,
        "recovery throttling",
    ),
    (
        "recovery.rebuild_threshold_gb",
        Mark::Unbuilt,
        "rebuild versus catch-up",
    ),
    (
        "recovery.step_max_lag_ms",
        Mark::Unbuilt,
        "recovery throttling",
    ),
    (
        "search.max_lag_ms",
        Mark::Unbuilt,
        "chapter 39's copy states (stale after lag)",
    ),
    (
        "stream.sequencer_batch_ms",
        Mark::Unbuilt,
        "the stream sequencer's batching",
    ),
    (
        "tenant.ancestry_cache_seconds",
        Mark::Unbuilt,
        "the ancestry cache's expiry",
    ),
    (
        "tenant.copy_max_descendants",
        Mark::Unbuilt,
        "the tenant-copy descendant cap",
    ),
    (
        "redis.sharded_pubsub_hubs",
        Mark::Restart,
        "the hub mesh is built when the gateway starts, and every gateway must agree on the count",
    ),
];

/// The [`Mark`] of `key` in [`MARKED`], if any.
#[must_use]
pub fn mark_of(key: &str) -> Option<Mark> {
    MARKED
        .iter()
        .find(|(k, _, _)| *k == key)
        .map(|(_, m, _)| *m)
}

/// When a change to `def` applies, as listings name it: `live`,
/// `new_sessions`, `rebind`, or `restart` for a [`Mark::Restart`] key.
#[must_use]
pub fn apply_name(def: &dyn AnySetting) -> &'static str {
    if mark_of(def.key()) == Some(Mark::Restart) {
        return "restart";
    }
    match def.apply() {
        Apply::Live => "live",
        Apply::NewSessions => "new_sessions",
        Apply::Rebind => "rebind",
    }
}

/// The validated set of declared keys.
pub struct Registry {
    keys: BTreeMap<&'static str, &'static dyn AnySetting>,
    marks: BTreeMap<&'static str, Mark>,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.keys.keys()).finish()
    }
}

impl Registry {
    /// Builds a registry from `settings`, checking each declaration.
    ///
    /// # Errors
    /// `Internal` for a duplicate key, a badly formed key, a missing `doc`,
    /// a key with no scope, or a default outside its hard bounds.
    pub fn new(settings: impl IntoIterator<Item = &'static dyn AnySetting>) -> Result<Self> {
        let mut keys = BTreeMap::new();
        for s in settings {
            validate(s)?;
            if keys.insert(s.key(), s).is_some() {
                return Err(SprigError::internal(format!(
                    "setting {} declared twice",
                    s.key()
                )));
            }
        }
        let marks = MARKED
            .iter()
            .filter(|(k, _, _)| keys.contains_key(k))
            .map(|(k, m, _)| (*k, *m))
            .collect();
        Ok(Self { keys, marks })
    }

    /// The process-wide registry of every key registered with
    /// [`register!`](crate::register).
    ///
    /// # Errors
    /// `Internal` if a declaration is invalid: a bug, caught by tests.
    pub fn global() -> Result<&'static Registry> {
        static GLOBAL: OnceLock<std::result::Result<Registry, String>> = OnceLock::new();
        GLOBAL
            .get_or_init(|| Registry::new(SETTINGS.iter().copied()).map_err(|e| e.message))
            .as_ref()
            .map_err(|m| SprigError::internal(m.clone()))
    }

    /// The declaration of `key`.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&'static dyn AnySetting> {
        self.keys.get(key).copied()
    }

    /// Every declaration that is built, ordered by key: what listings
    /// show. [`Mark::Unbuilt`] keys are left out; [`Self::get`] and
    /// [`Self::iter_all`] still have them.
    pub fn iter(&self) -> impl Iterator<Item = &'static dyn AnySetting> + '_ {
        self.keys
            .values()
            .copied()
            .filter(|s| self.mark(s.key()) != Some(Mark::Unbuilt))
    }

    /// Every declaration, built or not, ordered by key.
    pub fn iter_all(&self) -> impl Iterator<Item = &'static dyn AnySetting> + '_ {
        self.keys.values().copied()
    }

    /// The key's [`Mark`], if it was registered with one.
    #[must_use]
    pub fn mark(&self, key: &str) -> Option<Mark> {
        self.marks.get(key).copied()
    }

    /// Whether `key` is registered ahead of its code ([`Mark::Unbuilt`]).
    #[must_use]
    pub fn is_unbuilt(&self, key: &str) -> bool {
        self.mark(key) == Some(Mark::Unbuilt)
    }

    /// When a change to `key` applies, as listings name it: `live`,
    /// `new_sessions`, `rebind`, or `restart` for a [`Mark::Restart`] key.
    #[must_use]
    pub fn apply_name(&self, def: &dyn AnySetting) -> &'static str {
        if self.mark(def.key()) == Some(Mark::Restart) {
            return "restart";
        }
        apply_name(def)
    }

    /// With extra marks, for registries of test keys.
    #[must_use]
    pub fn with_marks(mut self, marks: &[(&'static str, Mark)]) -> Self {
        self.marks.extend(marks.iter().copied());
        self
    }
}

fn validate(s: &dyn AnySetting) -> Result<()> {
    let key = s.key();
    let bad = |why: &str| Err(SprigError::internal(format!("setting {key:?}: {why}")));
    let well_formed = key.split('.').count() >= 2
        && key.split('.').all(|part| {
            part.starts_with(|c: char| c.is_ascii_lowercase())
                && part
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        });
    if !well_formed {
        return bad("keys are dotted snake_case with a component prefix");
    }
    if key.starts_with("sprig.") {
        return bad("the sprig. prefix is reserved for internal keys");
    }
    if s.doc().trim().is_empty() {
        return bad("doc is required");
    }
    if s.scopes().0 & Scopes::ALL.0 == 0 {
        return bad("no scope accepts the key");
    }
    let hard = s.hard();
    let default = s.default_value();
    let in_range = default
        .as_f64()
        .is_none_or(|x| hard.min.is_none_or(|m| x >= m) && hard.max.is_none_or(|m| x <= m));
    let in_set = match (&default, hard.allowed) {
        (Value::Str(v), Some(set)) => set.contains(&v.as_str()),
        _ => true,
    };
    if !in_range || !in_set {
        return bad("default is outside the hard bounds");
    }
    Ok(())
}
