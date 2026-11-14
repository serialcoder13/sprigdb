//! Live settings: mirrors of the etcd scopes, watches and apply callbacks.
//!
//! Every process mirrors its DC's `settings/` and `cluster-settings/`, and
//! the global `settings/`, so a change arrives within one watch round trip
//! and resolution costs no etcd call. [`LiveSettings`] keeps the three
//! [`Mirror`]s and, after each change, rebuilds an immutable snapshot (a
//! [`MemSettings`]) that resolution reads. Then it notifies:
//!
//! - [`LiveSettings::watch`] and [`LiveSettings::subscribe`] receivers of
//!   the keys whose rows changed, at any scope (components re-resolve in
//!   their own contexts);
//! - callbacks registered with [`LiveSettings::on_change`], which apply new
//!   values in place: a pool resizes, a threshold changes.
//!
//! A value that can't be applied is logged, the old one kept, and
//! `SettingNotApplied` raised ([`not_applied`]): an error event and
//! `sprig_settings_apply_failures_total` (`component`). Each changed row
//! counts in `sprig_settings_changes_total` (`scope`).
//!
//! Until the relay exists, the global `settings/` is
//! mirrored straight from the global tier; in single-etcd mode that is
//! the same etcd as the DC tier.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

use sprig_common::ids::ClusterId;
use sprig_common::{CancellationToken, Result};
use sprig_meta::{Key, Mirror};
use tokio::sync::watch;

use crate::etcd::{SettingRecord, SettingsStores};
use crate::registry::{AnySetting, Registry, Setting};
use crate::resolve::{MemSettings, Resolved};
use crate::scope::{Scope, SettingCtx};
use crate::value::SettingValue;

/// A snapshot of every mirrored row, ready for resolution.
pub type Snapshot = Arc<MemSettings<'static>>;

/// Which keys a watch or callback follows: `listener.mysql.bind` names one
/// key; `listener.*` every key that starts with `listener.`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Pattern {
    Exact(String),
    Prefix(String),
}

impl Pattern {
    pub(crate) fn parse(p: &str) -> Self {
        match p.strip_suffix('*') {
            Some(prefix) => Pattern::Prefix(prefix.to_owned()),
            None => Pattern::Exact(p.to_owned()),
        }
    }

    pub(crate) fn matches(&self, key: &str) -> bool {
        match self {
            Pattern::Exact(k) => k == key,
            Pattern::Prefix(p) => key.starts_with(p.as_str()),
        }
    }
}

fn any_match(patterns: &[Pattern], changed: &BTreeSet<&'static str>) -> bool {
    changed
        .iter()
        .any(|k| patterns.iter().any(|p| p.matches(k)))
}

/// A callback applying new values of some keys.
type ApplyFn = dyn Fn(&MemSettings<'static>) -> std::result::Result<(), String> + Send + Sync;

struct Callback {
    id: u64,
    component: &'static str,
    patterns: Vec<Pattern>,
    apply: Arc<ApplyFn>,
}

struct Sub {
    patterns: Vec<Pattern>,
    tx: watch::Sender<u64>,
}

struct Shared {
    registry: &'static Registry,
    stores: SettingsStores,
    snapshot: RwLock<Snapshot>,
    /// Bumped after each rebuild that changed a row; 0 until the first
    /// snapshot is built.
    generation: watch::Sender<u64>,
    subs: Mutex<Vec<Sub>>,
    callbacks: Mutex<Vec<Callback>>,
    next_id: AtomicU64,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Cancels the rebuild task when the last handle drops.
struct StopOnDrop(CancellationToken);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// The `global`, `dc` and `cluster` settings of this process, mirrored from
/// etcd and resolved without etcd calls. Cloning shares them; the mirrors
/// stop when the last clone drops.
#[derive(Clone)]
pub struct LiveSettings {
    shared: Arc<Shared>,
    _stop: Arc<StopOnDrop>,
}

impl std::fmt::Debug for LiveSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveSettings")
            .field("stores", &self.shared.stores)
            .field("generation", &*self.shared.generation.borrow())
            .finish_non_exhaustive()
    }
}

impl LiveSettings {
    /// Starts mirroring the etcd scopes of `stores`, resolving against
    /// `registry`. Values are the registry defaults until
    /// [`ready`](Self::ready).
    ///
    /// # Errors
    /// Internal if a prefix can't be built (a bug).
    pub fn start(registry: &'static Registry, stores: SettingsStores) -> Result<Self> {
        let mirrors = Mirrors {
            global: Mirror::spawn(Arc::clone(&stores.global), stores.global_prefix()?),
            dc: Mirror::spawn(Arc::clone(&stores.dc), stores.dc_prefix()?),
            clusters: Mirror::spawn(Arc::clone(&stores.dc), stores.cluster_prefix()?),
        };
        let shared = Arc::new(Shared {
            registry,
            stores,
            snapshot: RwLock::new(Arc::new(MemSettings::new(registry))),
            generation: watch::channel(0).0,
            subs: Mutex::new(Vec::new()),
            callbacks: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(1),
        });
        let stop = CancellationToken::new();
        tokio::spawn(run(Arc::clone(&shared), mirrors, stop.clone()));
        Ok(Self {
            shared,
            _stop: Arc::new(StopOnDrop(stop)),
        })
    }

    /// Waits until every mirror has its first listing and the first
    /// snapshot is built. Waits for as long as etcd is unreachable.
    pub async fn ready(&self) {
        let mut rx = self.shared.generation.subscribe();
        let _ = rx.wait_for(|g| *g > 0).await;
    }

    /// Whether the first snapshot is built.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        *self.shared.generation.borrow() > 0
    }

    /// The registry keys resolve against.
    #[must_use]
    pub fn registry(&self) -> &'static Registry {
        self.shared.registry
    }

    /// The stores mirrored.
    #[must_use]
    pub fn stores(&self) -> &SettingsStores {
        &self.shared.stores
    }

    /// The current rows, immutable: resolve several keys against one
    /// snapshot to see them consistently.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Arc::clone(
            &self
                .shared
                .snapshot
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// Resolves `key` in `ctx` against the current snapshot.
    ///
    /// # Errors
    /// 39081 if the key is unknown.
    pub fn resolve(&self, key: &str, ctx: &SettingCtx<'_>) -> Result<Resolved> {
        self.snapshot().resolve(key, ctx)
    }

    /// The typed value of `setting` in `ctx`.
    #[must_use]
    pub fn get<T: SettingValue>(&self, setting: &Setting<T>, ctx: &SettingCtx<'_>) -> T {
        self.snapshot().get(setting, ctx)
    }

    /// A receiver bumped whenever a row of `setting` changes at any scope.
    #[must_use]
    pub fn watch(&self, setting: &'static dyn AnySetting) -> watch::Receiver<u64> {
        self.subscribe(&[setting.key()])
    }

    /// A receiver bumped whenever a row of a key matching `patterns`
    /// changes at any scope. A pattern ending in `*` matches by prefix
    /// (`listener.*`); any other names one key. Changes coalesce: the
    /// receiver then re-reads the snapshot.
    #[must_use]
    pub fn subscribe(&self, patterns: &[&str]) -> watch::Receiver<u64> {
        let (tx, rx) = watch::channel(0);
        let mut subs = lock(&self.shared.subs);
        subs.retain(|s| !s.tx.is_closed());
        subs.push(Sub {
            patterns: patterns.iter().map(|p| Pattern::parse(p)).collect(),
            tx,
        });
        rx
    }

    /// A receiver bumped after every change to any row.
    #[must_use]
    pub fn changes(&self) -> watch::Receiver<u64> {
        self.shared.generation.subscribe()
    }

    /// Registers `apply`, called with the new snapshot after every change
    /// to a key matching `patterns` (as in [`subscribe`](Self::subscribe)).
    /// It runs on the settings task, so it must be quick and not block: it
    /// applies values in place. An `Err` keeps the old value and raises
    /// `SettingNotApplied` for `component`. It is not called for the values
    /// current at registration; read those with [`get`](Self::get).
    /// Dropping the returned [`Registration`] removes it.
    pub fn on_change<F>(&self, component: &'static str, patterns: &[&str], apply: F) -> Registration
    where
        F: Fn(&MemSettings<'static>) -> std::result::Result<(), String> + Send + Sync + 'static,
    {
        let id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
        lock(&self.shared.callbacks).push(Callback {
            id,
            component,
            patterns: patterns.iter().map(|p| Pattern::parse(p)).collect(),
            apply: Arc::new(apply),
        });
        Registration {
            id,
            shared: Arc::downgrade(&self.shared),
        }
    }
}

/// A registered callback; dropping it unregisters.
#[must_use = "dropping a Registration unregisters its callback"]
pub struct Registration {
    id: u64,
    shared: std::sync::Weak<Shared>,
}

impl std::fmt::Debug for Registration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registration")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(s) = self.shared.upgrade() {
            lock(&s.callbacks).retain(|c| c.id != self.id);
        }
    }
}

/// Raises `SettingNotApplied`: a component could not apply a changed value
/// and keeps the old one.
pub fn not_applied(component: &'static str, reason: &str) {
    metrics::counter!("sprig_settings_apply_failures_total", "component" => component).increment(1);
    tracing::error!(
        event = "SettingNotApplied",
        component,
        reason,
        "a setting change could not be applied; the old value is kept"
    );
}

struct Mirrors {
    global: Mirror,
    dc: Mirror,
    clusters: Mirror,
}

/// Waits for the mirrors, then rebuilds the snapshot after every change.
async fn run(shared: Arc<Shared>, mirrors: Mirrors, stop: CancellationToken) {
    let m = &mirrors;
    let mut global = m.global.changes();
    let mut dc = m.dc.changes();
    let mut clusters = m.clusters.changes();
    let all_ready = async {
        m.global.ready().await;
        m.dc.ready().await;
        m.clusters.ready().await;
    };
    tokio::select! {
        () = stop.cancelled() => return,
        () = all_ready => {}
    }
    loop {
        global.borrow_and_update();
        dc.borrow_and_update();
        clusters.borrow_and_update();
        rebuild(&shared, m);
        tokio::select! {
            () = stop.cancelled() => return,
            r = global.changed() => if r.is_err() { return },
            r = dc.changed() => if r.is_err() { return },
            r = clusters.changed() => if r.is_err() { return },
        }
    }
}

/// The rows of the three mirrors.
fn build(shared: &Shared, m: &Mirrors) -> MemSettings<'static> {
    let mut rows = MemSettings::new(shared.registry);
    let dc = shared.stores.dc_id().clone();
    add_rows(&mut rows, &m.global, |name| Some((Scope::Global, name)));
    add_rows(&mut rows, &m.dc, |name| Some((Scope::Dc(dc.clone()), name)));
    add_rows(&mut rows, &m.clusters, |rest| {
        let (cluster, name) = rest.split_once('/')?;
        Some((Scope::Cluster(ClusterId::new(cluster).ok()?), name))
    });
    rows
}

/// Adds the rows of `mirror`; `address` maps a key below the prefix to its
/// scope and setting key.
fn add_rows(
    rows: &mut MemSettings<'static>,
    mirror: &Mirror,
    address: impl for<'k> Fn(&'k str) -> Option<(Scope, &'k str)>,
) {
    let prefix: &Key = mirror.prefix();
    for kv in mirror.entries() {
        let Some(rest) = kv.key.as_str().strip_prefix(prefix.as_str()) else {
            continue;
        };
        let Some((scope, name)) = address(rest) else {
            continue;
        };
        match SettingRecord::decode_entry(&kv.value) {
            Ok(entry) => rows.insert_row(scope, name, entry),
            Err(e) => tracing::warn!(key = %kv.key, error = %e, "bad setting record; ignored"),
        }
    }
}

/// Rebuilds the snapshot and notifies watchers and callbacks of the keys
/// whose rows changed.
fn rebuild(shared: &Shared, m: &Mirrors) {
    let new = Arc::new(build(shared, m));
    let old = {
        let mut cur = shared
            .snapshot
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        std::mem::replace(&mut *cur, Arc::clone(&new))
    };
    let first = *shared.generation.borrow() == 0;
    let changed = diff(&old, &new);
    if changed.is_empty() && !first {
        return;
    }
    shared.generation.send_modify(|g| *g += 1);
    if first {
        return;
    }
    let keys: BTreeSet<&'static str> = changed.iter().map(|(_, k)| *k).collect();
    tracing::info!(event = "settings.changed", keys = ?keys, "settings changed");
    {
        let mut subs = lock(&shared.subs);
        subs.retain(|s| !s.tx.is_closed());
        for s in subs.iter() {
            if any_match(&s.patterns, &keys) {
                s.tx.send_modify(|g| *g += 1);
            }
        }
    }
    let callbacks: Vec<(&'static str, Arc<ApplyFn>)> = lock(&shared.callbacks)
        .iter()
        .filter(|c| any_match(&c.patterns, &keys))
        .map(|c| (c.component, Arc::clone(&c.apply)))
        .collect();
    for (component, apply) in callbacks {
        if let Err(reason) = apply(&new) {
            not_applied(component, &reason);
        }
    }
}

/// The rows that differ between two snapshots, counted per scope.
fn diff(old: &MemSettings<'static>, new: &MemSettings<'static>) -> Vec<(Scope, &'static str)> {
    let (old, new) = (old.rows(), new.rows());
    let changed = new
        .iter()
        .filter(|(k, e)| old.get(k) != Some(e))
        .map(|(k, _)| k)
        .chain(old.keys().filter(|k| !new.contains_key(k)));
    let out: Vec<(Scope, &'static str)> = changed.cloned().collect();
    for (scope, _) in &out {
        let label = match scope {
            Scope::Global => "global",
            Scope::Dc(_) => "dc",
            Scope::Cluster(_) => "cluster",
            Scope::Tenant(_) => "tenant",
            Scope::Database(..) => "database",
            Scope::Object(..) => "object",
        };
        metrics::counter!("sprig_settings_changes_total", "scope" => label).increment(1);
    }
    out
}
