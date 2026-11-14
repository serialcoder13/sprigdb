//! The gateway as a copy's host ([`sprig_search::copy`]): the feed, the
//! DC's clusters, and seeds read as the index's definer.
//!
//! A gateway holds copies itself only when `search.copies` is 0 or the DC
//! has no worker with the search role (see [`super`]); otherwise copies
//! run on workers and this gateway serves their seeds
//! (`definer_page`, through `sprig.internal.v1.SearchSeed`).

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_doc::engine::SqlRunner as _;
use sprig_search::copy::{CopyHost, CopyKnobs, FeedParts, Seeder, record_from_text, seed_page_sql};
use sprig_search::def::IndexDef;
use sprig_search::schema::{Key, Record};

pub use sprig_search::copy::{Applied, Copy as Runtime, Phase, key_part_from_text};

use super::SearchService;
use crate::mongo::sql::SqlSession;

fn unavailable(msg: impl Into<String>) -> SprigError {
    let mut e = SprigError::new(SprigCode::SearchUnavailable, msg);
    e.retryable = true;
    e
}

/// The gateway's side of a copy.
pub(crate) struct GatewayHost(pub Arc<SearchService>);

/// The DC's registered, unretired clusters.
pub(crate) async fn dc_clusters(svc: &SearchService) -> Result<Vec<ClusterId>> {
    let reg = sprig_cluster::registry::ClusterRegistry::new(
        Arc::clone(&svc.deps.tenants.global),
        Arc::clone(&svc.deps.dc_store),
    )?;
    Ok(reg
        .list()
        .await?
        .into_iter()
        .filter(|c| c.body().admin_state != sprig_cluster::record::AdminState::Retired)
        .map(|c| c.body().cluster_id.clone())
        .collect())
}

/// A session of `def`'s definer, exempt from row-level security while it
/// lives (audited as `rls.internal_exempt`).
pub(crate) struct DefinerSession {
    sql: SqlSession,
    _exempt: crate::tenant::rls::ExemptGuard,
}

impl DefinerSession {
    /// Opens it.
    ///
    /// # Errors
    /// 39860 for a definer or tenant that is gone; the session table's.
    pub(crate) async fn open(
        svc: &SearchService,
        tenant: TenantId,
        def: &IndexDef,
    ) -> Result<Self> {
        let principal = crate::triggers::principal_of(
            &svc.deps.login,
            tenant.0,
            &def.tenant_name,
            def.definer_user_id,
        )
        .await?;
        let sql = SqlSession::open(
            &svc.deps.sessions,
            &svc.deps.executor,
            &principal,
            "search",
            0,
            SocketAddr::from(([127, 0, 0, 1], 0)),
            None,
            64 << 20,
        )?;
        // The copy holds every row; queries
        // re-read hits through the querying user's session and policies.
        let exempt = crate::tenant::rls::exempt_internal(
            sql.session_id(),
            tenant.0,
            "search seed",
            &format!("{}.{}", def.db, def.table),
        );
        Ok(Self {
            sql,
            _exempt: exempt,
        })
    }

    /// One page of `def`'s object after `after`.
    ///
    /// # Errors
    /// 39820 when the object is gone; 39825 for other read errors.
    pub(crate) async fn page(
        &mut self,
        def: &IndexDef,
        after: Option<&Key>,
        limit: usize,
    ) -> Result<Vec<Record>> {
        let (cols, stmt) = seed_page_sql(def, after, limit)?;
        let rows = self.sql.query(&stmt).await.map_err(|e| {
            let code = e.errno.unwrap_or(0);
            if code == 1146 {
                SprigError::new(
                    SprigCode::SearchIndexNotFound,
                    format!("the object of search index '{}' is gone", def.name),
                )
            } else {
                unavailable(format!(
                    "seeding search index '{}': {} ({code})",
                    def.name, e.message
                ))
            }
        })?;
        Ok(rows
            .into_iter()
            .map(|r| record_from_text(def, &cols, r))
            .collect())
    }

    /// Closes it.
    pub(crate) async fn close(self) {
        self.sql.close().await;
    }
}

struct LocalSeeder {
    session: DefinerSession,
    def: IndexDef,
}

#[async_trait]
impl Seeder for LocalSeeder {
    async fn page(&mut self, after: Option<&Key>, limit: usize) -> Result<Vec<Record>> {
        self.session.page(&self.def, after, limit).await
    }

    async fn close(self: Box<Self>) {
        self.session.close().await;
    }
}

/// Whether the catalog no longer has `def`'s object.
pub(crate) async fn object_gone(svc: &SearchService, tenant: TenantId, def: &IndexDef) -> bool {
    let Ok(cat) = svc.catalog().snapshot(tenant, 0).await else {
        return false;
    };
    let Some(db) = svc.catalog().db_id(&cat, &def.db) else {
        return true;
    };
    svc.catalog().table_id(&cat, db, &def.table) != Some(def.object_id)
}

#[async_trait]
impl CopyHost for GatewayHost {
    fn knobs(&self) -> CopyKnobs {
        let k = self.0.knobs();
        CopyKnobs {
            commit_interval: k.commit_interval,
            writer_bytes: k.writer_bytes,
        }
    }

    fn feed(&self) -> &FeedParts {
        &self.0.feed
    }

    async fn clusters(&self) -> Result<Vec<ClusterId>> {
        dc_clusters(&self.0).await
    }

    async fn seeder(&self, tenant: TenantId, def: &IndexDef) -> Result<Box<dyn Seeder>> {
        Ok(Box::new(LocalSeeder {
            session: DefinerSession::open(&self.0, tenant, def).await?,
            def: def.clone(),
        }))
    }

    async fn object_gone(&self, tenant: TenantId, def: &IndexDef) -> bool {
        object_gone(&self.0, tenant, def).await
    }
}
