//! Version allocation and `change()`, the protocol that commits a
//! versioned change.
//!
//! ```text
//! change(tenant, kind):                     // kind = Catalog or Auth
//!   tx = catalog_writer(tenant).begin()
//!   tx: lock the sprig.<kind>_version row FOR UPDATE
//!   loop:                                   // allocate in the global tier
//!     rec = global.get("tenants/" + tenant)
//!     v = rec.<kind>_version + 1
//!     new = rec with <kind>_version = v, and settings_version = v when the change
//!           touches tenant-scope settings rows
//!     if global.cas(rec.revision, new): break
//!     if the global tier is unreachable: tx.rollback(); fail 39070
//!   caller writes its rows with changed_version (or version) = v
//!   tx: set the version row to v; tx.commit()    // a failed commit leaves a harmless gap
//! ```
//!
//! The pieces:
//! - [`VersionAllocator`] allocates in the global tier. [`DirectoryAllocator`]
//!   does it with a compare-and-swap on `/sprig/g/tenants/<id>` through any
//!   [`MetaStore`], optionally guarded by a [`Leadership`]
//!   (`Leadership::commit_if`), since the global leader is the directory's
//!   only writer. Gateways hold no global credentials and
//!   allocate through the leader's `Global` service instead: another
//!   implementation of the same trait.
//! - [`VersionBackend`] is the catalog side: begin a transaction holding the
//!   version row, then set the row and commit. [`MariaBackend`] is MariaDB;
//!   [`mem::MemBackend`] is an in-memory model for unit and property tests.
//! - [`Versioner::change`] runs the protocol and hands back a [`Change`]:
//!   the open transaction and its version `v`. The caller writes its rows
//!   with `v` and calls [`Change::commit`].
//!
//! Invariants, and why they hold:
//! - **Monotonic, never reused.** The row lock serializes changes per
//!   tenant and kind; a version is allocated while holding it, and the
//!   compare-and-swap makes every allocation unique. Allocation starts above
//!   both the directory's and the catalog's version (the *floor*), so a
//!   directory restored from an old backup can't hand out a version the
//!   catalog already used.
//! - **Readers never see a half-applied change.** A change's rows and its
//!   version row commit in one MariaDB transaction; loaders read in one
//!   consistent snapshot ([`Store::snapshot`]). Commits happen in version
//!   order because the next allocation waits for the lock.
//! - **No lost invalidation.** The directory moves before the catalog
//!   commits, so a gateway watching it reloads after every commit.
//! - **Gaps are explained.** A version allocated but not committed (crash,
//!   lost leadership, failed commit) is a gap: the directory is ahead of the
//!   catalog. It is the only cause. The next change commits above it, and
//!   [`Versioner::settle`] closes it at once by committing the catalog's
//!   version row up to the directory's value (an empty change).
//!   [`Change::rollback`] settles right after rolling back, so a caller
//!   that allocates and then finds nothing to write leaves no gap.
//!
//! Failure cases:
//!
//! | Where | Effect |
//! |---|---|
//! | Before allocation (crash, lock timeout) | Nothing: the transaction rolls back |
//! | Global tier unreachable | Retried until the deadline, then 39070; rolled back, no gap |
//! | Leadership lost before the CAS | 39504, rolled back, no gap |
//! | Crash or error after allocation | Rolled back; gap (an explicit rollback settles it) |
//! | Leadership lost after allocation | Checked before `COMMIT`: 39504, rolled back; gap |
//! | Commit reply lost | Committed or not; either way consistent (a gap at worst) |

use std::sync::Arc;
use std::time::Duration;

use mysql_async::{Transaction, TxOpts};
use serde::{Deserialize, Serialize};
use sprig_common::ids::TenantId;
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Consistency, Key, Leadership, MetaStore, Op, Record, Txn, Versioned};

use crate::sql::{map_err, timed};
use crate::store::{CatalogLocator, PurgeReport, Snapshot, Store};

pub mod mem;
#[cfg(test)]
mod tests;

/// Which per-tenant version a change bumps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum VersionKind {
    /// Objects, databases, settings and versioned extension modules.
    Catalog,
    /// Users, verifiers and privileges.
    Auth,
}

impl VersionKind {
    /// Both kinds.
    pub const ALL: [VersionKind; 2] = [VersionKind::Catalog, VersionKind::Auth];

    /// The internal key of the catalog's version row.
    #[must_use]
    pub const fn key(self) -> &'static str {
        match self {
            VersionKind::Catalog => "sprig.catalog_version",
            VersionKind::Auth => "sprig.auth_version",
        }
    }

    /// The metric label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            VersionKind::Catalog => "catalog",
            VersionKind::Auth => "auth",
        }
    }
}

/// The version fields of a tenant directory entry,
/// `/sprig/g/tenants/<tenant_id>`. The entry's other fields
/// (name, state, home DC, tree fields, ...) are kept as they are.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryVersions {
    /// The last allocated catalog version.
    #[serde(default)]
    pub catalog_version: u64,
    /// The last allocated auth version.
    #[serde(default)]
    pub auth_version: u64,
    /// The catalog version of the last tenant-scope settings change, which
    /// descendants copy.
    #[serde(default)]
    pub settings_version: u64,
}

impl Versioned for DirectoryVersions {
    const V: u32 = 1;
}

impl DirectoryVersions {
    /// The version of `kind`.
    #[must_use]
    pub const fn get(&self, kind: VersionKind) -> u64 {
        match kind {
            VersionKind::Catalog => self.catalog_version,
            VersionKind::Auth => self.auth_version,
        }
    }

    fn set(&mut self, kind: VersionKind, v: u64) {
        match kind {
            VersionKind::Catalog => self.catalog_version = v,
            VersionKind::Auth => self.auth_version = v,
        }
    }
}

/// Allocates per-tenant versions in the global tier.
#[async_trait::async_trait]
pub trait VersionAllocator: Send + Sync + 'static {
    /// Allocates the next version of `kind`: one above both the
    /// directory's value and `floor` (the catalog's committed version).
    /// With `tenant_settings`, a catalog allocation also moves
    /// `settings_version`.
    ///
    /// # Errors
    /// 39070 if the global tier stays unreachable; 39072 if the tenant has
    /// no directory entry; 39504 if a required leadership is lost.
    async fn allocate(
        &self,
        tenant: TenantId,
        kind: VersionKind,
        floor: u64,
        tenant_settings: bool,
    ) -> Result<u64>;

    /// The directory's current version of `kind`.
    ///
    /// # Errors
    /// As [`allocate`](Self::allocate).
    async fn current(&self, tenant: TenantId, kind: VersionKind) -> Result<u64>;

    /// Whether a change may still commit: `Ok` unless the allocator acts
    /// for a leadership that has ended (side effects outside
    /// etcd check the session first).
    ///
    /// # Errors
    /// 39504 `ER_SPRIG_NOT_LEADER`.
    fn check_fence(&self) -> Result<()> {
        Ok(())
    }
}

/// Allocates with a compare-and-swap on the tenant's directory entry.
#[derive(Clone)]
pub struct DirectoryAllocator {
    store: Arc<dyn MetaStore>,
    leader: Option<Leadership>,
    timeout: Duration,
    retry: Duration,
}

impl std::fmt::Debug for DirectoryAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectoryAllocator")
            .field("tier", self.store.tier())
            .field("leader", &self.leader.as_ref().map(|l| l.token))
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl DirectoryAllocator {
    /// Default deadline of one allocation (`meta.request_timeout_ms`).
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

    /// Writes directly through `store`, which must be the global tier's
    /// (single-etcd mode; tests may pass a private tier standing in for
    /// it). Keys are `tenants/<id>` under the store's tier.
    #[must_use]
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self {
            store,
            leader: None,
            timeout: Self::DEFAULT_TIMEOUT,
            retry: Duration::from_millis(50),
        }
    }

    /// Writes only while `leadership` holds, through its session's store:
    /// the global leader allocating for others.
    #[must_use]
    pub fn leader(leadership: Leadership) -> Self {
        Self {
            store: Arc::clone(leadership.session.store()),
            leader: Some(leadership),
            timeout: Self::DEFAULT_TIMEOUT,
            retry: Duration::from_millis(50),
        }
    }

    /// The same allocator with another deadline per allocation.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The directory key of `tenant`.
    ///
    /// # Errors
    /// `Internal` never in practice (the path is always valid).
    pub fn key(&self, tenant: TenantId) -> Result<Key> {
        self.store.tier().key(&format!("tenants/{tenant}"))
    }

    async fn read(&self, key: &Key) -> Result<(Record<DirectoryVersions>, u64)> {
        let kv = self
            .store
            .get(key, Consistency::Linearizable)
            .await?
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::TenantNotReady,
                    "the tenant has no directory entry",
                )
            })?;
        Ok((Record::decode(&kv.value)?, kv.mod_revision))
    }

    /// One attempt: `Some(v)` if the swap committed, `None` if the entry
    /// changed underneath.
    async fn attempt(
        &self,
        key: &Key,
        kind: VersionKind,
        floor: u64,
        tenant_settings: bool,
    ) -> Result<Option<u64>> {
        let (mut rec, rev) = self.read(key).await?;
        let v = rec
            .body
            .get(kind)
            .max(floor)
            .checked_add(1)
            .ok_or_else(|| SprigError::internal("tenant version exhausted"))?;
        rec.body.set(kind, v);
        if tenant_settings && kind == VersionKind::Catalog {
            rec.body.settings_version = v;
        }
        let when = vec![Cmp::ModRev(key.clone(), rev)];
        let then = vec![Op::Put {
            key: key.clone(),
            value: rec.encode()?,
            lease: None,
        }];
        let r = match &self.leader {
            Some(l) => l.commit_if(when, then).await?,
            None => {
                self.store
                    .txn(Txn {
                        when,
                        then,
                        otherwise: Vec::new(),
                    })
                    .await?
            }
        };
        Ok(r.succeeded.then_some(v))
    }
}

/// Whether an allocation error is the caller's to see as is: lost
/// leadership, a missing tenant or a bug. Anything else from the store is
/// the global tier being unreachable.
fn passes_through(e: &SprigError) -> bool {
    matches!(
        e.code(),
        Some(SprigCode::NotLeader | SprigCode::TenantNotReady | SprigCode::Internal)
    )
}

fn unavailable(e: SprigError) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        "the global tier is unreachable; catalog changes are refused",
    )
    .with_source(e)
}

#[async_trait::async_trait]
impl VersionAllocator for DirectoryAllocator {
    async fn allocate(
        &self,
        tenant: TenantId,
        kind: VersionKind,
        floor: u64,
        tenant_settings: bool,
    ) -> Result<u64> {
        let key = self.key(tenant)?;
        let deadline = tokio::time::Instant::now() + self.timeout;
        let mut last: Option<SprigError> = None;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return Err(unavailable(last.unwrap_or_else(|| {
                    SprigError::new(SprigCode::MetaUnavailable, "allocation timed out")
                })));
            }
            match tokio::time::timeout(left, self.attempt(&key, kind, floor, tenant_settings)).await
            {
                Ok(Ok(Some(v))) => return Ok(v),
                // Someone else changed the entry: re-read at once.
                Ok(Ok(None)) => continue,
                Ok(Err(e)) if passes_through(&e) => return Err(e),
                Ok(Err(e)) if !e.retryable && e.code() != Some(SprigCode::MetaUnavailable) => {
                    return Err(unavailable(e));
                }
                Ok(Err(e)) => {
                    tracing::debug!(%tenant, error = %e, "version allocation failed; retrying");
                    last = Some(e);
                }
                Err(_) => {
                    last = Some(SprigError::new(
                        SprigCode::MetaUnavailable,
                        "allocation timed out",
                    ));
                }
            }
            tokio::time::sleep(self.retry.min(left)).await;
        }
    }

    async fn current(&self, tenant: TenantId, kind: VersionKind) -> Result<u64> {
        let key = self.key(tenant)?;
        match tokio::time::timeout(self.timeout, self.read(&key)).await {
            Ok(Ok((rec, _))) => Ok(rec.body.get(kind)),
            Ok(Err(e)) if passes_through(&e) => Err(e),
            Ok(Err(e)) => Err(unavailable(e)),
            Err(_) => Err(unavailable(SprigError::new(
                SprigCode::MetaUnavailable,
                "directory read timed out",
            ))),
        }
    }

    fn check_fence(&self) -> Result<()> {
        match &self.leader {
            Some(l) if !l.is_leading() => Err(SprigError::new(
                SprigCode::NotLeader,
                "leadership lost before the catalog commit",
            )),
            _ => Ok(()),
        }
    }
}

/// The catalog side of [`Versioner::change`].
#[async_trait::async_trait]
pub trait VersionBackend: Send + Sync + 'static {
    /// An open transaction holding a version row.
    type Tx: Send;

    /// Begins a transaction and locks `kind`'s version row; returns the
    /// committed version.
    ///
    /// # Errors
    /// 39070 if the catalog writer is unreachable; 39071 if the lock wait
    /// times out; 39072 if the catalog isn't bootstrapped.
    async fn begin_locked(&self, tenant: TenantId, kind: VersionKind) -> Result<(Self::Tx, u64)>;

    /// Sets the version row to `version` and commits.
    ///
    /// # Errors
    /// 39070 if the commit fails or its outcome is unknown.
    async fn commit(&self, tx: Self::Tx, kind: VersionKind, version: u64) -> Result<()>;

    /// Rolls back. Dropping the transaction must have the same effect
    /// (a crashed process's transaction rolls back).
    ///
    /// # Errors
    /// The backend's error; the rollback happens anyway when the
    /// connection ends.
    async fn rollback(&self, tx: Self::Tx) -> Result<()>;
}

/// A point in [`Versioner::change`] where a test can inject a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Step {
    /// The version row is locked; nothing allocated.
    Locked,
    /// The version is allocated in the global tier; rows not yet written.
    Allocated,
    /// The caller's rows are written; about to commit.
    BeforeCommit,
    /// Committed; about to return (a lost commit reply).
    Committed,
}

/// A test hook called at every [`Step`]. An `Err` aborts the change there
/// the way a crash would: the transaction is dropped, not rolled back.
pub type Failpoint = Arc<dyn Fn(TenantId, VersionKind, Step) -> Result<()> + Send + Sync>;

/// Options of one change.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChangeOpts {
    /// The change writes tenant-scope settings rows, so the directory's
    /// `settings_version` moves too (descendants copy them).
    pub tenant_settings: bool,
}

/// Commits versioned changes to tenant catalogs.
pub struct Versioner<B: VersionBackend> {
    backend: Arc<B>,
    allocator: Arc<dyn VersionAllocator>,
    failpoint: Option<Failpoint>,
}

impl<B: VersionBackend> Clone for Versioner<B> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            allocator: Arc::clone(&self.allocator),
            failpoint: self.failpoint.clone(),
        }
    }
}

impl<B: VersionBackend> std::fmt::Debug for Versioner<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Versioner")
            .field("failpoint", &self.failpoint.is_some())
            .finish_non_exhaustive()
    }
}

impl<B: VersionBackend> Versioner<B> {
    /// Changes through `backend`, allocating with `allocator`.
    pub fn new(backend: B, allocator: Arc<dyn VersionAllocator>) -> Self {
        Self {
            backend: Arc::new(backend),
            allocator,
            failpoint: None,
        }
    }

    /// The same versioner with a failpoint, for crash tests.
    #[must_use]
    #[doc(hidden)]
    pub fn with_failpoint(mut self, f: Failpoint) -> Self {
        self.failpoint = Some(f);
        self
    }

    /// The backend.
    #[must_use]
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// The allocator.
    #[must_use]
    pub fn allocator(&self) -> &Arc<dyn VersionAllocator> {
        &self.allocator
    }

    fn fail(&self, tenant: TenantId, kind: VersionKind, step: Step) -> Result<()> {
        self.failpoint
            .as_ref()
            .map_or(Ok(()), |f| f(tenant, kind, step))
    }

    /// Opens a change of `kind`: locks the version row and allocates the
    /// next version. Write rows with [`Change::version`], then
    /// [`Change::commit`].
    ///
    /// # Errors
    /// As [`VersionBackend::begin_locked`] and [`VersionAllocator::allocate`];
    /// the transaction is rolled back on any error.
    pub async fn change(&self, tenant: TenantId, kind: VersionKind) -> Result<Change<B>> {
        self.change_with(tenant, kind, ChangeOpts::default()).await
    }

    /// [`change`](Self::change) with options.
    ///
    /// # Errors
    /// As [`change`](Self::change).
    pub async fn change_with(
        &self,
        tenant: TenantId,
        kind: VersionKind,
        opts: ChangeOpts,
    ) -> Result<Change<B>> {
        self.lock(tenant, kind).await?.allocate(opts).await
    }

    /// The first half of [`change`](Self::change): locks the version row
    /// and allocates nothing yet. A caller that must check rows before
    /// committing to a change (a setting write checking bounds) reads them
    /// through [`Locked::tx`], then either [`Locked::allocate`]s or
    /// [`Locked::release`]s, so a refused change leaves no gap.
    ///
    /// # Errors
    /// As [`VersionBackend::begin_locked`].
    pub async fn lock(&self, tenant: TenantId, kind: VersionKind) -> Result<Locked<B>> {
        let (tx, previous) = self.backend.begin_locked(tenant, kind).await?;
        if let Err(e) = self.fail(tenant, kind, Step::Locked) {
            drop(tx);
            return Err(e);
        }
        Ok(Locked {
            tx,
            backend: Arc::clone(&self.backend),
            allocator: Arc::clone(&self.allocator),
            failpoint: self.failpoint.clone(),
            tenant,
            kind,
            previous,
        })
    }

    /// Closes a gap: if the directory is ahead of the catalog (a version
    /// was allocated and never committed), commits the catalog's version
    /// row up to the directory's value, so loaders waiting for it stop
    /// waiting. Returns the catalog's version afterwards.
    ///
    /// # Errors
    /// As [`change`](Self::change); `Internal` if the catalog is ahead of
    /// the directory (the next change repairs that by allocating above
    /// both).
    pub async fn settle(&self, tenant: TenantId, kind: VersionKind) -> Result<u64> {
        settle_in(&*self.backend, &*self.allocator, tenant, kind).await
    }
}

/// [`Versioner::settle`] on its parts, for [`Change::rollback`] too.
async fn settle_in<B: VersionBackend>(
    backend: &B,
    allocator: &dyn VersionAllocator,
    tenant: TenantId,
    kind: VersionKind,
) -> Result<u64> {
    let (tx, committed) = backend.begin_locked(tenant, kind).await?;
    let dir = match allocator.current(tenant, kind).await {
        Ok(d) => d,
        Err(e) => {
            let _ = backend.rollback(tx).await;
            return Err(e);
        }
    };
    if dir == committed {
        backend.rollback(tx).await?;
        return Ok(committed);
    }
    if dir < committed {
        let _ = backend.rollback(tx).await;
        return Err(SprigError::internal(format!(
            "tenant {tenant}: catalog {} version {committed} is ahead of the directory's {dir}",
            kind.label()
        )));
    }
    if let Err(e) = allocator.check_fence() {
        let _ = backend.rollback(tx).await;
        return Err(e);
    }
    tracing::debug!(%tenant, kind = kind.label(), from = committed, to = dir, "closing a version gap");
    backend.commit(tx, kind, dir).await?;
    Ok(dir)
}

/// A transaction holding a version row, with no version allocated yet
/// ([`Versioner::lock`]).
#[must_use = "a locked version row must be allocated or released"]
pub struct Locked<B: VersionBackend> {
    tx: B::Tx,
    backend: Arc<B>,
    allocator: Arc<dyn VersionAllocator>,
    failpoint: Option<Failpoint>,
    tenant: TenantId,
    kind: VersionKind,
    previous: u64,
}

impl<B: VersionBackend> std::fmt::Debug for Locked<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Locked")
            .field("tenant", &self.tenant)
            .field("kind", &self.kind)
            .field("previous", &self.previous)
            .finish_non_exhaustive()
    }
}

impl<B: VersionBackend> Locked<B> {
    /// The committed version.
    #[must_use]
    pub fn previous(&self) -> u64 {
        self.previous
    }

    /// The tenant.
    #[must_use]
    pub fn tenant(&self) -> TenantId {
        self.tenant
    }

    /// The open transaction, to read rows in (they can't change while the
    /// row is locked by this transaction, as every versioned change takes
    /// the same lock).
    pub fn tx(&mut self) -> &mut B::Tx {
        &mut self.tx
    }

    /// Allocates the next version in the global tier: the change proper.
    ///
    /// # Errors
    /// As [`VersionAllocator::allocate`]; the transaction is rolled back
    /// on any error.
    pub async fn allocate(self, opts: ChangeOpts) -> Result<Change<B>> {
        let Locked {
            tx,
            backend,
            allocator,
            failpoint,
            tenant,
            kind,
            previous,
        } = self;
        let version = match allocator
            .allocate(tenant, kind, previous, opts.tenant_settings)
            .await
        {
            Ok(v) if v > previous => v,
            Ok(v) => {
                let _ = backend.rollback(tx).await;
                return Err(SprigError::internal(format!(
                    "allocated {} version {v} not above the committed {previous}",
                    kind.label()
                )));
            }
            Err(e) => {
                let _ = backend.rollback(tx).await;
                return Err(e);
            }
        };
        if let Err(e) = failpoint
            .as_ref()
            .map_or(Ok(()), |f| f(tenant, kind, Step::Allocated))
        {
            drop(tx);
            return Err(e);
        }
        Ok(Change {
            tx,
            backend,
            allocator,
            failpoint,
            tenant,
            kind,
            version,
            previous,
        })
    }

    /// Commits the rows written in [`tx`](Self::tx) without allocating a
    /// version: intermediate states commit unversioned. The rows must carry the committed version
    /// ([`previous`](Self::previous)), so incremental loads skip them
    /// until a versioned change touches them. The version row keeps its
    /// value. Checked against the allocator's fence first.
    ///
    /// # Errors
    /// 39504 if the allocator's leadership ended (rolled back); 39070 if
    /// the commit fails.
    pub async fn commit_unversioned(self) -> Result<()> {
        if let Err(e) = self.allocator.check_fence() {
            let _ = self.backend.rollback(self.tx).await;
            return Err(e);
        }
        self.backend.commit(self.tx, self.kind, self.previous).await
    }

    /// Rolls back without allocating: no version, no gap.
    ///
    /// # Errors
    /// The backend's error.
    pub async fn release(self) -> Result<()> {
        self.backend.rollback(self.tx).await
    }
}

/// An open change: a transaction holding the version row, and the version
/// its rows must carry.
#[must_use = "a change must be committed or rolled back"]
pub struct Change<B: VersionBackend> {
    tx: B::Tx,
    backend: Arc<B>,
    allocator: Arc<dyn VersionAllocator>,
    failpoint: Option<Failpoint>,
    tenant: TenantId,
    kind: VersionKind,
    version: u64,
    previous: u64,
}

impl<B: VersionBackend> std::fmt::Debug for Change<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Change")
            .field("tenant", &self.tenant)
            .field("kind", &self.kind)
            .field("version", &self.version)
            .field("previous", &self.previous)
            .finish_non_exhaustive()
    }
}

impl<B: VersionBackend> Change<B> {
    /// The version to write as `changed_version` (or `version`).
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The version committed before this change; `version > previous + 1`
    /// means a gap was skipped.
    #[must_use]
    pub fn previous(&self) -> u64 {
        self.previous
    }

    /// The tenant.
    #[must_use]
    pub fn tenant(&self) -> TenantId {
        self.tenant
    }

    /// The kind.
    #[must_use]
    pub fn kind(&self) -> VersionKind {
        self.kind
    }

    /// The open transaction, to write rows in.
    pub fn tx(&mut self) -> &mut B::Tx {
        &mut self.tx
    }

    /// Sets the version row and commits. Returns the version.
    ///
    /// # Errors
    /// 39504 if the allocator's leadership ended (rolled back: a gap);
    /// 39070 if the commit fails (a gap, or committed if only the reply
    /// was lost).
    pub async fn commit(self) -> Result<u64> {
        let Change {
            tx,
            backend,
            allocator,
            failpoint,
            tenant,
            kind,
            version,
            ..
        } = self;
        let fail = |step| failpoint.as_ref().map_or(Ok(()), |f| f(tenant, kind, step));
        if let Err(e) = fail(Step::BeforeCommit) {
            drop(tx);
            return Err(e);
        }
        if let Err(e) = allocator.check_fence() {
            let _ = backend.rollback(tx).await;
            return Err(e);
        }
        backend.commit(tx, kind, version).await?;
        metrics::counter!("sprig_catalog_changes_total", "kind" => kind.label()).increment(1);
        fail(Step::Committed)?;
        Ok(version)
    }

    /// Rolls back, then closes the gap the allocated version would leave:
    /// the version row is settled up to the directory's value in a short
    /// empty change ([`Versioner::settle`]), unless a later change already
    /// committed above it. Without that, every loader asking for the
    /// directory's version (`Freshness::Latest`) waited
    /// `catalog.version_wait_ms` for a version that never commits: callers
    /// that allocate, find nothing to write and roll back (the document
    /// catalog after each `dropDatabase`, for one) cost every later session
    /// up to 2 s. A failed settle leaves the gap, as before.
    ///
    /// # Errors
    /// The backend's error from the rollback.
    pub async fn rollback(self) -> Result<()> {
        let Change {
            tx,
            backend,
            allocator,
            tenant,
            kind,
            ..
        } = self;
        backend.rollback(tx).await?;
        if let Err(e) = settle_in(&*backend, &*allocator, tenant, kind).await {
            tracing::debug!(%tenant, kind = kind.label(), error = %e,
                "a rolled-back change's version stays a gap");
        }
        Ok(())
    }

    /// Rolls back and leaves the allocated version as a gap (what a crash
    /// leaves); for tests of gap handling.
    ///
    /// # Errors
    /// The backend's error.
    #[doc(hidden)]
    pub async fn abandon(self) -> Result<()> {
        self.backend.rollback(self.tx).await
    }
}

// ---- MariaDB ----

/// Timeouts of the MariaDB backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MariaOpts {
    /// Longest wait for another change's version row lock.
    pub lock_wait: Duration,
    /// Deadline of each round trip other than the lock wait.
    pub statement_timeout: Duration,
}

impl Default for MariaOpts {
    fn default() -> Self {
        Self {
            lock_wait: Duration::from_secs(10),
            statement_timeout: Duration::from_secs(5),
        }
    }
}

/// The catalog writer in MariaDB.
#[derive(Clone)]
pub struct MariaBackend {
    locator: Arc<dyn CatalogLocator>,
    clock: Arc<dyn Clock>,
    opts: MariaOpts,
}

impl std::fmt::Debug for MariaBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MariaBackend")
            .field("opts", &self.opts)
            .finish_non_exhaustive()
    }
}

impl MariaBackend {
    /// Finds catalogs with `locator`.
    #[must_use]
    pub fn new(locator: Arc<dyn CatalogLocator>) -> Self {
        Self {
            locator,
            clock: Arc::new(SystemClock),
            opts: MariaOpts::default(),
        }
    }

    /// The same backend with other timeouts.
    #[must_use]
    pub fn with_opts(mut self, opts: MariaOpts) -> Self {
        self.opts = opts;
        self
    }

    /// The same backend with catalog timestamps from `clock`.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The locator.
    #[must_use]
    pub fn locator(&self) -> &Arc<dyn CatalogLocator> {
        &self.locator
    }

    /// The timeouts.
    #[must_use]
    pub fn opts(&self) -> MariaOpts {
        self.opts
    }
}

/// An open MariaDB transaction on a tenant's catalog writer.
#[derive(Debug)]
pub struct MariaTx {
    tx: Transaction<'static>,
    store: Store,
}

impl MariaTx {
    /// The catalog's store.
    #[must_use]
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The transaction, to run store calls on.
    pub fn conn(&mut self) -> &mut Transaction<'static> {
        &mut self.tx
    }

    /// Both at once: `let (s, q) = tx.parts(); s.put_setting(q, &row).await?`.
    pub fn parts(&mut self) -> (&Store, &mut Transaction<'static>) {
        (&self.store, &mut self.tx)
    }
}

#[async_trait::async_trait]
impl VersionBackend for MariaBackend {
    type Tx = MariaTx;

    async fn begin_locked(&self, tenant: TenantId, kind: VersionKind) -> Result<(MariaTx, u64)> {
        let ep = self.locator.writer(tenant).await?;
        let mut tx = timed(self.opts.statement_timeout, "begin", async {
            ep.pool
                .start_transaction(TxOpts::default())
                .await
                .map_err(map_err)
        })
        .await?;
        let wait = self.opts.lock_wait.as_secs().max(1);
        let v = timed(
            self.opts.lock_wait + self.opts.statement_timeout,
            "version lock",
            ep.store.lock_version(&mut tx, kind, wait),
        )
        .await?;
        Ok((
            MariaTx {
                tx,
                store: ep.store,
            },
            v,
        ))
    }

    async fn commit(&self, mut tx: MariaTx, kind: VersionKind, version: u64) -> Result<()> {
        let now = self.clock.wall();
        timed(self.opts.statement_timeout, "commit", async {
            tx.store.set_version(&mut tx.tx, kind, version, now).await?;
            tx.tx.commit().await.map_err(map_err)
        })
        .await
    }

    async fn rollback(&self, tx: MariaTx) -> Result<()> {
        timed(self.opts.statement_timeout, "rollback", async {
            tx.tx.rollback().await.map_err(map_err)
        })
        .await
    }
}

/// The catalog over MariaDB.
pub type MariaCatalog = Versioner<MariaBackend>;

/// An open versioned change on a MariaDB catalog.
pub type CatalogTx = Change<MariaBackend>;

/// The catalog store interface: versioned changes and
/// snapshots.
#[async_trait::async_trait]
pub trait CatalogStore: Send + Sync {
    /// Locks the version row and allocates the next version globally.
    async fn change(&self, t: TenantId, kind: VersionKind) -> Result<CatalogTx>;
    /// Rows changed after `since` (every row for `None`) in one consistent
    /// snapshot.
    async fn snapshot(&self, t: TenantId, since: Option<u64>) -> Result<Snapshot>;
}

#[async_trait::async_trait]
impl CatalogStore for MariaCatalog {
    async fn change(&self, t: TenantId, kind: VersionKind) -> Result<CatalogTx> {
        Versioner::change(self, t, kind).await
    }

    async fn snapshot(&self, t: TenantId, since: Option<u64>) -> Result<Snapshot> {
        let ep = self.backend().locator().reader(t).await?;
        timed(
            self.backend().opts.statement_timeout * 4,
            "snapshot",
            ep.store.snapshot(&ep.pool, since),
        )
        .await
    }
}

impl MariaCatalog {
    /// Purges tombstones changed at or before `up_to` (clamped to the
    /// committed catalog version), holding the catalog version row so no
    /// change commits meanwhile. Allocates no version: caches at or above
    /// the new `sprig.purged_version` have already applied every purged
    /// tombstone. The reconciler picks `up_to` as the newest version
    /// committed more than `catalog.tombstone_retention_hours` ago.
    ///
    /// # Errors
    /// As [`change`](Versioner::change).
    pub async fn purge(&self, tenant: TenantId, up_to: u64) -> Result<PurgeReport> {
        let (mut tx, committed) = self
            .backend
            .begin_locked(tenant, VersionKind::Catalog)
            .await?;
        let up_to = up_to.min(committed);
        let limit = self.backend.opts.statement_timeout * 4;
        let r = timed(limit, "purge", async {
            let report = tx.store.purge(&mut tx.tx, up_to).await?;
            tx.tx.commit().await.map_err(map_err)?;
            Ok(report)
        })
        .await?;
        Ok(r)
    }
}
