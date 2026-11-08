//! An in-memory [`VersionBackend`]: per tenant, the two version rows with
//! their locks and a log of committed changes. It models what the protocol
//! relies on from MariaDB (a row lock held until commit or rollback, an
//! atomic commit, rollback when a transaction is dropped), so unit and
//! property tests of [`Versioner`](super::Versioner) run without a server.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::OwnedMutexGuard;

use super::{VersionBackend, VersionKind};

/// One committed change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// Its kind.
    pub kind: VersionKind,
    /// Its version.
    pub version: u64,
    /// The rows it wrote.
    pub rows: Vec<String>,
}

/// A tenant's catalog as a reader sees it: always whole changes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemCatalog {
    /// Committed catalog version.
    pub catalog: u64,
    /// Committed auth version.
    pub auth: u64,
    /// Every commit, in commit order.
    pub commits: Vec<Commit>,
}

impl MemCatalog {
    /// The committed version of `kind`.
    #[must_use]
    pub fn version(&self, kind: VersionKind) -> u64 {
        match kind {
            VersionKind::Catalog => self.catalog,
            VersionKind::Auth => self.auth,
        }
    }
}

#[derive(Debug)]
struct Tenant {
    locks: [Arc<tokio::sync::Mutex<()>>; 2],
    state: Mutex<MemCatalog>,
}

/// An open transaction: holds the version row's lock and the staged rows.
/// Dropping it rolls back.
#[derive(Debug)]
pub struct MemTx {
    _guard: OwnedMutexGuard<()>,
    tenant: Arc<Tenant>,
    staged: Vec<String>,
}

impl MemTx {
    /// Stages a row, visible only once the change commits.
    pub fn write(&mut self, row: impl Into<String>) {
        self.staged.push(row.into());
    }
}

/// The in-memory backend.
#[derive(Debug, Default)]
pub struct MemBackend {
    tenants: Mutex<HashMap<TenantId, Arc<Tenant>>>,
}

impl MemBackend {
    /// No tenants.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates `tenant`'s catalog with both versions at 0 (what migration
    /// and bootstrap do in MariaDB).
    pub fn bootstrap(&self, tenant: TenantId) {
        self.lock().entry(tenant).or_insert_with(|| {
            Arc::new(Tenant {
                locks: [
                    Arc::new(tokio::sync::Mutex::new(())),
                    Arc::new(tokio::sync::Mutex::new(())),
                ],
                state: Mutex::new(MemCatalog::default()),
            })
        });
    }

    /// A consistent read of `tenant`'s catalog.
    #[must_use]
    pub fn read(&self, tenant: TenantId) -> Option<MemCatalog> {
        let t = self.lock().get(&tenant).cloned()?;
        let s = t.state.lock().unwrap_or_else(PoisonError::into_inner);
        Some(s.clone())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<TenantId, Arc<Tenant>>> {
        self.tenants.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

const fn slot(kind: VersionKind) -> usize {
    match kind {
        VersionKind::Catalog => 0,
        VersionKind::Auth => 1,
    }
}

#[async_trait::async_trait]
impl VersionBackend for MemBackend {
    type Tx = MemTx;

    async fn begin_locked(&self, tenant: TenantId, kind: VersionKind) -> Result<(MemTx, u64)> {
        let t = self.lock().get(&tenant).cloned().ok_or_else(|| {
            SprigError::new(
                SprigCode::TenantNotReady,
                "the tenant's catalog is not bootstrapped",
            )
        })?;
        let guard = Arc::clone(&t.locks[slot(kind)]).lock_owned().await;
        let v = t
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .version(kind);
        Ok((
            MemTx {
                _guard: guard,
                tenant: t,
                staged: Vec::new(),
            },
            v,
        ))
    }

    async fn commit(&self, tx: MemTx, kind: VersionKind, version: u64) -> Result<()> {
        let mut s = tx
            .tenant
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match kind {
            VersionKind::Catalog => s.catalog = version,
            VersionKind::Auth => s.auth = version,
        }
        s.commits.push(Commit {
            kind,
            version,
            rows: tx.staged,
        });
        Ok(())
    }

    async fn rollback(&self, tx: MemTx) -> Result<()> {
        drop(tx);
        Ok(())
    }
}
