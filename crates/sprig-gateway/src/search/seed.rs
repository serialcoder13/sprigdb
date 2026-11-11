//! The gateway's side of `sprig.internal.v1.SearchSeed` (chapter 39,
//! "Snapshots, seeding and rebuilds"; R5 round 6): a worker building a
//! copy from MariaDB reads the index's object a page at a time through
//! this gateway, as the index's definer, without row-level security
//! (decisions S6 and RL17), with names, privileges and the router as any
//! session has them.
//!
//! ```text
//! SeedPage(tenant, index, generation, after, limit):
//!   the index in the tenant's catalog, else 39820 (the copy stops)
//!   its generation ≠ the copy's: 39825 (retryable; the job restarts the copy)
//!   a session of the definer, exempt; SELECT the page by key; close it
//! ```
//!
//! The peer service is mutual TLS between SprigDB's own processes, so the
//! worker is trusted to name the tenant; the definer comes from the
//! catalog, never from the request.

use sprig_api::proto::internal::search_seed_server::{SearchSeed, SearchSeedServer};
use sprig_api::proto::internal::{SeedPageReply, SeedPageRequest};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_search::copy::{SeedReply, SeedRequest};
use sprig_search::rpc::WireError;
use tonic::{Request, Response, Status};

use super::catalog as scat;
use super::runtime::DefinerSession;

/// The service (looks the search service up per call: the peer listener
/// starts before it).
#[derive(Debug, Clone, Copy, Default)]
pub struct SeedService;

impl SeedService {
    /// The tonic service.
    #[must_use]
    pub fn server() -> SearchSeedServer<Self> {
        SearchSeedServer::new(Self)
    }
}

async fn page(req: &SeedRequest) -> Result<Vec<sprig_search::Record>> {
    let svc = super::service().ok_or_else(|| {
        let mut e = SprigError::new(
            SprigCode::SearchUnavailable,
            "this gateway's search service isn't running",
        );
        e.retryable = true;
        e
    })?;
    let tenant = TenantId(req.tenant);
    let cat = svc.catalog().snapshot(tenant, 0).await?;
    let def = scat::state(&cat).by_id(req.index).ok_or_else(|| {
        SprigError::new(
            SprigCode::SearchIndexNotFound,
            format!("search index {} of tenant {} is gone", req.index, req.tenant),
        )
    })?;
    if def.generation != req.generation {
        let mut e = SprigError::new(
            SprigCode::SearchUnavailable,
            format!(
                "search index '{}' is at generation {}, the copy at {}",
                def.name, def.generation, req.generation
            ),
        );
        e.retryable = true;
        return Err(e);
    }
    if super::runtime::object_gone(svc, tenant, &def).await {
        return Err(SprigError::new(
            SprigCode::SearchIndexNotFound,
            format!("the object of search index '{}' is gone", def.name),
        ));
    }
    if req.limit == 0 {
        return Ok(Vec::new());
    }
    let mut s = DefinerSession::open(svc, tenant, &def).await?;
    let r = s.page(&def, req.after.as_ref(), req.limit).await;
    s.close().await;
    r
}

#[tonic::async_trait]
impl SearchSeed for SeedService {
    async fn seed_page(
        &self,
        request: Request<SeedPageRequest>,
    ) -> std::result::Result<Response<SeedPageReply>, Status> {
        let req: SeedRequest = serde_json::from_str(&request.get_ref().request_json)
            .map_err(|e| Status::invalid_argument(format!("a seed request: {e}")))?;
        let started = std::time::Instant::now();
        let rows = page(&req).await.map_err(|e| WireError::of(&e));
        metrics::histogram!("sprig_search_seed_page_seconds")
            .record(started.elapsed().as_secs_f64());
        let reply = serde_json::to_string(&SeedReply { rows })
            .map_err(|e| Status::internal(format!("a seed reply: {e}")))?;
        Ok(Response::new(SeedPageReply { reply_json: reply }))
    }
}
