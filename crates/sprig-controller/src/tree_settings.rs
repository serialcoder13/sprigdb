//! The `tree_settings` loop: copies ancestors' tenant-scope settings into
//! every descendant's catalog (chapter 08, "Ancestry and ancestors'
//! settings"; build step 6).
//!
//! ```text
//! tree_settings loop (DC leader; keys: tenants homed in this DC):
//!   on a directory event for tenant x (settings_version, path or parent changed):
//!     for each tenant d homed here whose path contains x (the controller's mirror
//!         indexes the directory by path):
//!       enqueue d
//!   reconcile(d):
//!     want = ancestry of d from the directory (parent links, root first)
//!     for each ancestor x in want with x.settings_version > d.ancestor_versions[x]:
//!       rows = x's tenant-scope settings rows at version >= x.settings_version
//!              (a replica of x's catalog in this DC, else x's writer)
//!     change(d, Catalog):                          // one version for the batch
//!       replace d's ancestor rows of each such x; delete rows of tenants no longer in want
//!       sprig.ancestry = want; sprig.ancestor_versions[x] = loaded versions
//! ```
//!
//! "Nearest level wins, also for later changes": for `a → b → c`, a later
//! change at `b` moves `b`'s `settings_version`; the event enqueues `c`
//! (its path contains `b`), which copies `b`'s rows. `c` resolves `a`
//! overlaid by `b` overlaid by `c`, so every key `c` set itself stays
//! `c`'s, and every other key follows `b`.
//!
//! Details:
//! - **Which events count.** Every catalog change moves a directory entry
//!   (`catalog_version`); the loop remembers each tenant's
//!   `settings_version`, path and parent and fans out only when one of
//!   them moved. A tenant seen for the first time counts as moved.
//! - **The path index** is a scan of the controller's mirror of
//!   `tenants/` (fine for R0's directory sizes; chapter 23's open question
//!   7 covers very wide trees).
//! - **Rows at the right version.** An ancestor's rows are read at a
//!   catalog version at or above its directory `settings_version`. A
//!   catalog behind its directory has a gap; the loop closes it with
//!   `settle` (an empty change, chapter 08) and reads again, else retries.
//! - **Fan-out** runs `catalog.tree_sync_concurrency` descendants at once.
//!   Every descendant copies from each ancestor's own catalog, never from
//!   its parent's copy, so the order doesn't matter.
//! - **Ancestry modes.** Only `copy` (and `auto`, which behaves as `copy`)
//!   is implemented. A tenant whose own or freshly copied rows name
//!   `shared` or `reference` is reported `Blocked` rather than copied, since
//!   copying could store settings where residency forbids it. TODO(chapter
//!   08 step 18).
//!
//! Metrics: `sprig_settings_tree_syncs_total` (`outcome`) and
//! `sprig_settings_tree_lag_seconds` (the age of the oldest uncopied change
//! is not measured yet: it reports how long the last reconcile that found
//! a stale copy took).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use sprig_catalog::ancestry::{self, ANCESTOR_VERSIONS_KEY, AncestorRows, AncestryReport};
use sprig_catalog::cache::snapshot::ANCESTRY_KEY;
use sprig_catalog::settings::{ANCESTRY_MODE, TREE_SYNC_CONCURRENCY};
use sprig_catalog::{
    AncestryState, SettingsBackend, SettingsTx, TenantRef, VersionKind, Versioner,
};
use sprig_common::ids::{DcId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode};
use sprig_meta::Mirror;
use sprig_settings::{LiveSettings, SettingCtx};

use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};
use crate::tree::{Directory, DirectoryEntry, StoredEntry, TenantState};

/// Wait before looking again at a descendant whose ancestor's catalog was
/// behind its directory.
const BEHIND_RETRY: Duration = Duration::from_secs(1);

/// What one sync did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncOutcome {
    /// The copy was current; nothing written.
    Current,
    /// Rows or ancestry were written in one change at `version`.
    Copied {
        /// The change's version.
        version: u64,
        /// What changed.
        report: AncestryReport,
    },
    /// An ancestor's catalog is behind its directory; try again shortly.
    Behind(TenantId),
    /// The subtree uses an ancestry mode that isn't implemented.
    Blocked(&'static str),
}

/// Copies one tenant's ancestry and ancestors' rows: the reconcile step,
/// also run by tenant bootstrap.
pub struct TreeSync<B: SettingsBackend>
where
    B::Tx: SettingsTx,
{
    versioner: Versioner<B>,
    directory: Directory,
    clock: Arc<dyn Clock>,
}

impl<B: SettingsBackend> std::fmt::Debug for TreeSync<B>
where
    B::Tx: SettingsTx,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeSync")
            .field("directory", &self.directory)
            .finish_non_exhaustive()
    }
}

impl<B: SettingsBackend> TreeSync<B>
where
    B::Tx: SettingsTx,
{
    /// Syncs catalogs through `versioner`, reading the tree from
    /// `directory`.
    pub fn new(versioner: Versioner<B>, directory: Directory, clock: Arc<dyn Clock>) -> Self {
        Self {
            versioner,
            directory,
            clock,
        }
    }

    /// The versioner.
    #[must_use]
    pub fn versioner(&self) -> &Versioner<B> {
        &self.versioner
    }

    /// The directory.
    #[must_use]
    pub fn directory(&self) -> &Directory {
        &self.directory
    }

    /// Reconciles `entry`'s catalog with the directory: chapter 08's
    /// `reconcile(d)`.
    ///
    /// # Errors
    /// 39072 if the tenant or an ancestor has no catalog (or directory
    /// entry); the catalogs' and the global tier's errors.
    pub async fn sync_entry(&self, entry: &DirectoryEntry) -> Result<SyncOutcome> {
        let d = entry.tenant_id;
        let want = self.directory.ancestry(entry).await?;
        let backend = self.versioner.backend();
        let own = backend.tenant_scope(d).await?;
        let state = AncestryState::from_values(
            d,
            own.internal(ANCESTRY_KEY),
            own.internal(ANCESTOR_VERSIONS_KEY),
        );
        if state.is_current(&want) {
            return Ok(SyncOutcome::Current);
        }
        let mut fresh = Vec::new();
        for x in state.stale(&want) {
            let sv = want
                .iter()
                .find(|(r, _)| r.id == x)
                .map_or(0, |(_, sv)| *sv);
            let mut rows = backend.tenant_scope(x).await?;
            if rows.version < sv {
                // A gap at x: close it, then read again.
                self.versioner.settle(x, VersionKind::Catalog).await?;
                rows = backend.tenant_scope(x).await?;
                if rows.version < sv {
                    return Ok(SyncOutcome::Behind(x));
                }
            }
            fresh.push(AncestorRows {
                tenant: x,
                settings_version: sv,
                rows: rows.copyable(),
            });
        }
        if let Some(why) = unsupported_mode(&own.rows, &fresh) {
            return Ok(SyncOutcome::Blocked(why));
        }
        let refs: Vec<TenantRef> = want.into_iter().map(|(r, _)| r).collect();
        let mut c = self.versioner.change(d, VersionKind::Catalog).await?;
        let version = c.version();
        let now = self.clock.wall();
        match ancestry::apply(c.tx(), d, version, now, &refs, &fresh).await {
            Ok(report) => {
                c.commit().await?;
                tracing::info!(tenant = %d, version, copied = ?report.copied,
                    removed = ?report.removed, rows = report.rows_written,
                    cleared = report.rows_cleared, "ancestors' settings copied");
                Ok(SyncOutcome::Copied { version, report })
            }
            Err(e) => {
                let _ = c.rollback().await;
                Err(e)
            }
        }
    }

    /// [`sync_entry`](Self::sync_entry) for `d`, read from the directory;
    /// `None` if it has no entry.
    ///
    /// # Errors
    /// As [`sync_entry`](Self::sync_entry).
    pub async fn sync(&self, d: TenantId) -> Result<Option<SyncOutcome>> {
        match self.directory.get(d).await? {
            Some(e) => self.sync_entry(e.body()).await.map(Some),
            None => Ok(None),
        }
    }
}

/// The nearest explicit `tenant.ancestry_mode` among the tenant's own row
/// and the freshly read ancestors' rows (parent first), if it names a mode
/// that isn't implemented.
fn unsupported_mode(
    own: &[sprig_catalog::store::SettingRow],
    fresh: &[AncestorRows],
) -> Option<&'static str> {
    let key = ANCESTRY_MODE.key;
    let nearest = own
        .iter()
        .chain(fresh.iter().rev().flat_map(|f| f.rows.iter()))
        .find(|r| r.key == key && r.value.is_some())
        .and_then(|r| r.value.as_ref())
        .and_then(|v| v.as_str().map(str::to_owned));
    match nearest.as_deref() {
        Some("shared") => Some("tenant.ancestry_mode = shared is not implemented yet"),
        Some("reference") => Some("tenant.ancestry_mode = reference is not implemented yet"),
        _ => None,
    }
}

/// What the loop remembers of a tenant's entry, to tell which events count.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    settings_version: u64,
    path: String,
    parent: u64,
}

impl Seen {
    fn of(e: &DirectoryEntry) -> Self {
        Self {
            settings_version: e.settings_version,
            path: e.path.clone(),
            parent: e.parent_tenant_id,
        }
    }
}

/// Whether the loop keeps `state`'s catalogs in sync.
const fn syncable(state: TenantState) -> bool {
    !matches!(state, TenantState::Deleting | TenantState::Failed)
}

/// The `tree_settings` reconciler.
pub struct TreeSettings<B: SettingsBackend>
where
    B::Tx: SettingsTx,
{
    sync: Arc<TreeSync<B>>,
    dc: DcId,
    mirror: Mirror,
    seen: Mutex<HashMap<TenantId, Seen>>,
    live: Option<LiveSettings>,
}

impl<B: SettingsBackend> std::fmt::Debug for TreeSettings<B>
where
    B::Tx: SettingsTx,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeSettings")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

impl<B: SettingsBackend> TreeSettings<B>
where
    B::Tx: SettingsTx,
{
    /// The loop for tenants homed in `dc`, mirroring the directory of
    /// `sync`'s global store. `live` gives `catalog.tree_sync_concurrency`
    /// (its default without it).
    ///
    /// # Errors
    /// `Internal` if the directory prefix can't be built.
    pub fn new(sync: Arc<TreeSync<B>>, dc: DcId, live: Option<LiveSettings>) -> Result<Self> {
        let dir = sync.directory();
        let mirror = Mirror::spawn(Arc::clone(dir.store()), dir.prefix()?);
        Ok(Self {
            sync,
            dc,
            mirror,
            seen: Mutex::new(HashMap::new()),
            live,
        })
    }

    /// Waits until the directory mirror has its first listing.
    pub async fn ready(&self) {
        self.mirror.ready().await;
    }

    fn ours(&self, e: &DirectoryEntry) -> bool {
        e.home_dc == self.dc && syncable(e.state)
    }

    /// Tenants homed here strictly below `x`, from the mirror.
    fn descendants(&self, x: TenantId) -> Vec<TenantId> {
        self.mirror.with(|entries| {
            entries
                .values()
                .filter_map(|kv| StoredEntry::decode(kv).ok())
                .filter(|e| {
                    let b = e.body();
                    b.tenant_id != x && b.path_contains(x) && self.ours(b)
                })
                .map(|e| e.body().tenant_id)
                .collect()
        })
    }

    fn seen(&self) -> std::sync::MutexGuard<'_, HashMap<TenantId, Seen>> {
        self.seen.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[async_trait::async_trait]
impl<B: SettingsBackend> Reconciler for TreeSettings<B>
where
    B::Tx: SettingsTx,
{
    type Key = TenantId;

    fn name(&self) -> &'static str {
        "tree_settings"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::global("tenants/")]
    }

    fn workers(&self) -> usize {
        let n = self
            .live
            .as_ref()
            .map_or(TREE_SYNC_CONCURRENCY.default, |l| {
                l.get(&TREE_SYNC_CONCURRENCY, &SettingCtx::process(&self.dc))
            });
        usize::try_from(n).unwrap_or(1).max(1)
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<TenantId> {
        let dir = self.sync.directory();
        let Some(x) = dir.tenant_of(&ev.key) else {
            return Vec::new();
        };
        let Some(kv) = &ev.kv else {
            self.seen().remove(&x);
            return Vec::new();
        };
        let Ok(stored) = StoredEntry::decode(kv) else {
            return Vec::new();
        };
        let e = stored.body();
        let now = Seen::of(e);
        let moved = self.seen().insert(x, now.clone()) != Some(now.clone());
        if !moved {
            return Vec::new();
        }
        let mut keys = Vec::new();
        if self.ours(e) {
            keys.push(x);
        }
        keys.extend(self.descendants(x));
        keys
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TenantId>> {
        let entries = self.sync.directory().list().await?;
        let mut seen = self.seen();
        Ok(entries
            .iter()
            .map(StoredEntry::body)
            .inspect(|e| {
                seen.insert(e.tenant_id, Seen::of(e));
            })
            .filter(|e| self.ours(e))
            .map(|e| e.tenant_id)
            .collect())
    }

    async fn reconcile(&self, _cx: &LoopCx, key: &TenantId) -> Result<Outcome> {
        let Some(entry) = self.sync.directory().get(*key).await? else {
            return Ok(Outcome::Done);
        };
        let e = entry.body();
        if !self.ours(e) {
            return Ok(Outcome::Done);
        }
        let started = std::time::Instant::now();
        let outcome = match self.sync.sync_entry(e).await {
            Ok(o) => o,
            // Bootstrap copies the ancestry once the catalog exists.
            Err(err)
                if e.state == TenantState::Creating
                    && err.code() == Some(SprigCode::TenantNotReady) =>
            {
                return Ok(Outcome::Done);
            }
            Err(err) => {
                metrics::counter!("sprig_settings_tree_syncs_total", "outcome" => "error")
                    .increment(1);
                return Err(err);
            }
        };
        let (label, next) = match outcome {
            SyncOutcome::Current => ("current", Outcome::Done),
            SyncOutcome::Copied { .. } => {
                metrics::gauge!("sprig_settings_tree_lag_seconds")
                    .set(started.elapsed().as_secs_f64());
                ("copied", Outcome::Done)
            }
            SyncOutcome::Behind(x) => {
                tracing::debug!(tenant = %key, ancestor = %x, "ancestor's catalog is behind its directory");
                ("behind", Outcome::RequeueAfter(BEHIND_RETRY))
            }
            SyncOutcome::Blocked(reason) => ("blocked", Outcome::Blocked { reason }),
        };
        metrics::counter!("sprig_settings_tree_syncs_total", "outcome" => label).increment(1);
        Ok(next)
    }
}

#[cfg(test)]
mod tests;
