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

/// The validated set of declared keys.
pub struct Registry {
    keys: BTreeMap<&'static str, &'static dyn AnySetting>,
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
        Ok(Self { keys })
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

    /// Every declaration, ordered by key.
    pub fn iter(&self) -> impl Iterator<Item = &'static dyn AnySetting> + '_ {
        self.keys.values().copied()
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
