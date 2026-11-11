//! Where the cache loads snapshots from: [`CatalogSource`], and
//! [`MariaSource`] over the tenants' catalog clusters.
//!
//! A load reads, in one consistent read-only transaction, the catalog
//! format (39073 if a module is newer than this binary reads), the rows
//! changed after the previous snapshot's version (every row for a first
//! load, or when the catalog was purged past it) and each versioned
//! module's state ([`CatalogExtension::load`]).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};

use super::ext::{CatalogExtension, SnapshotTx, module_of};
use super::snapshot::{Loaded, TenantCatalog};
use crate::migrate::{FORMAT_KEY, FormatState, Migrator, check_readable};
use crate::sql::{map_err, timed};
use crate::store::CatalogLocator;
use crate::version::VersionKind;

/// Which node of the catalog's cluster a load reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Node {
    /// A healthy local replica, else the writer (the locator decides).
    Replica,
    /// The writer: when a replica lags past `catalog.version_wait_ms`, and
    /// after name misses.
    Writer,
}

impl Node {
    /// The metric label (`source` of `sprig_catalog_loads_total`).
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Node::Replica => "replica",
            Node::Writer => "writer",
        }
    }
}

/// Reads catalog snapshots for the cache.
#[async_trait::async_trait]
pub trait CatalogSource: Send + Sync + 'static {
    /// Reads the rows changed after `prev.version` (every row without
    /// `prev`) and the versioned modules' states in one consistent
    /// snapshot from `node`.
    ///
    /// # Errors
    /// 39070 if the catalog is unreachable; 39072 if the tenant has no
    /// catalog; 39073 if the catalog's format is too new.
    async fn load(
        &self,
        tenant: TenantId,
        prev: Option<&TenantCatalog>,
        node: Node,
    ) -> Result<Loaded>;

    /// The committed catalog and auth versions on `node`, read without a
    /// snapshot: one small query, so a loader waiting for a version polls
    /// this instead of loading in full again and again. `None` when the
    /// source can't tell cheaply (the loader then just loads).
    ///
    /// # Errors
    /// As [`load`](Self::load); the loader falls back to a full load.
    async fn committed(&self, _tenant: TenantId, _node: Node) -> Result<Option<(u64, u64)>> {
        Ok(None)
    }
}

/// Loads from the catalogs' MariaDB clusters through a [`CatalogLocator`].
pub struct MariaSource {
    locator: Arc<dyn CatalogLocator>,
    extensions: Vec<Arc<dyn CatalogExtension>>,
    supports: BTreeMap<String, u32>,
    timeout: Duration,
}

impl std::fmt::Debug for MariaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MariaSource")
            .field(
                "extensions",
                &self
                    .extensions
                    .iter()
                    .map(|e| e.module())
                    .collect::<Vec<_>>(),
            )
            .field("supports", &self.supports)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl MariaSource {
    /// Default deadline of one load.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

    /// A source over `locator`, loading the core rows and every versioned
    /// module of `extensions`.
    ///
    /// # Errors
    /// `Internal` if a module's migrations are malformed or a name repeats.
    pub fn new(
        locator: Arc<dyn CatalogLocator>,
        extensions: Vec<Arc<dyn CatalogExtension>>,
    ) -> Result<Self> {
        let modules: Vec<_> = extensions.iter().map(|e| module_of(&**e)).collect();
        let supports = Migrator::new(&modules)?.supports();
        Ok(Self {
            locator,
            extensions,
            supports,
            timeout: Self::DEFAULT_TIMEOUT,
        })
    }

    /// The same source with another deadline per load.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait::async_trait]
impl CatalogSource for MariaSource {
    async fn load(
        &self,
        tenant: TenantId,
        prev: Option<&TenantCatalog>,
        node: Node,
    ) -> Result<Loaded> {
        let ep = match node {
            Node::Replica => self.locator.reader(tenant).await?,
            Node::Writer => self.locator.writer(tenant).await?,
        };
        timed(self.timeout, "cache load", async {
            let mut tx = ep.store.begin_snapshot(&ep.pool).await?;
            let format: FormatState = match ep.store.internal(&mut tx, FORMAT_KEY).await? {
                Some(v) => serde_json::from_value(v)
                    .map_err(|e| SprigError::internal(format!("bad {FORMAT_KEY}: {e}")))?,
                None => FormatState::new(),
            };
            check_readable(&format, &self.supports)?;
            let snapshot = ep
                .store
                .snapshot_in(&mut tx, prev.map(|p| p.version))
                .await?;
            let full = snapshot.since.is_none();
            let mut ext = Vec::new();
            {
                let mut stx = SnapshotTx::new(&mut tx, &ep.store, snapshot.versions);
                for e in &self.extensions {
                    let Some(kind) = e.versioned() else {
                        continue;
                    };
                    let prev_state = prev.filter(|_| !full).and_then(|p| p.ext.raw(e.module()));
                    let since = match (prev_state, prev) {
                        (Some(_), Some(p)) => Some(match kind {
                            VersionKind::Catalog => p.version,
                            VersionKind::Auth => p.auth_version,
                        }),
                        _ => None,
                    };
                    ext.push((e.module(), e.load(&mut stx, since, prev_state).await?));
                }
            }
            tx.commit().await.map_err(map_err)?;
            Ok(Loaded { snapshot, ext })
        })
        .await
    }

    async fn committed(&self, tenant: TenantId, node: Node) -> Result<Option<(u64, u64)>> {
        let ep = match node {
            Node::Replica => self.locator.reader(tenant).await?,
            Node::Writer => self.locator.writer(tenant).await?,
        };
        timed(self.timeout, "catalog versions", async {
            let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
            let v = ep.store.versions(&mut conn).await?;
            Ok(Some((v.catalog, v.auth)))
        })
        .await
    }
}
