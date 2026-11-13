//! Stream sequencers (chapter 45, R9) on chapter 23's job runtime:
//! `stream_sequencer`, one per stream, long-running, submitted by the
//! controller's `streams` loop. The job body is
//! [`sprig_streams::runtime::run_job`]; this module gives it the worker's
//! catalogs, shard pools (as `sprig_ctl`, on the tenant's cluster now) and
//! the tenant's feed.
//!
//! **One dump per cluster (R9, r9feed).** Every sequencer's feed reads
//! through the worker's shared reader ([`sprig_feed::shared`]): an event
//! stream's tail through the view of the stream outboxes
//! (`__sprig_eo_*`), a change topic's through the view that keeps its
//! tenant whole. However many stream instances a cluster holds, the worker
//! opens one binlog dump on it (plus at most four short catch-up dumps for
//! instances far behind).

use std::sync::Arc;

use async_trait::async_trait;
use mysql_async::Pool;
use serde_json::{Value, json};
use sprig_catalog::CatalogLocator;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_controller::catalogs::{ClusterPools, ShardMapLocator};
use sprig_controller::dispatch::JobClass;
use sprig_feed::names::{CatalogPools, CatalogResolver};
use sprig_feed::shared::{Kept, SharedFeed};
use sprig_feed::{Clusters, MetaPlacement, StartAt, TailOptions, TenantTail};
use sprig_meta::MetaStore;
use sprig_streams::StreamId;
use sprig_streams::runtime::{JOB_KIND, JobEnd, JobEnv, Report, run_job};
use sprig_streams::sequencer::{FeedOpener, FeedSource, Start};

use crate::logic::LogicEnv;
use crate::runtime::{JobCtx, JobHandler};

/// What the sequencers share.
#[derive(Clone)]
pub struct StreamsEnv {
    dc: Arc<dyn MetaStore>,
    pools: Arc<dyn ClusterPools>,
    locator: Arc<ShardMapLocator>,
    /// The shared reader's view of the stream outboxes (event streams).
    outboxes: Arc<dyn Clusters>,
    /// The shared reader's view keeping a tenant whole (change topics).
    whole: Arc<dyn Clusters>,
    /// The worker's live settings (global `stream.*`).
    settings: crate::settings::WorkerSettings,
}

impl std::fmt::Debug for StreamsEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamsEnv").finish_non_exhaustive()
    }
}

impl StreamsEnv {
    /// The logic jobs' catalogs and pools, and views of the worker's
    /// shared reader `feed`.
    #[must_use]
    pub fn from_logic(env: &LogicEnv, feed: &Arc<SharedFeed>) -> Self {
        Self {
            dc: Arc::clone(&env.dc),
            pools: Arc::clone(&env.pools),
            locator: Arc::clone(&env.locator),
            outboxes: feed.view(Kept::Tables(vec![OUTBOXES.to_owned()].into())),
            whole: feed.view(Kept::All),
            settings: env.settings.clone(),
        }
    }
}

struct Catalogs(Arc<ShardMapLocator>);

#[async_trait]
impl CatalogPools for Catalogs {
    async fn pool(&self, tenant: TenantId) -> Result<Pool> {
        Ok(self.0.writer(tenant).await?.pool)
    }
}

/// The stream outboxes, as a shared-reader table pattern.
pub const OUTBOXES: &str = "__sprig_eo_*";

/// The tenant's feed, keeping the stream's outbox.
struct Feeds {
    env: StreamsEnv,
    tenant: TenantId,
    keep: Vec<String>,
}

#[async_trait]
impl FeedOpener for Feeds {
    async fn open(&self, start: &Start) -> Result<Box<dyn FeedSource>> {
        let at = match start {
            Start::After(t) => StartAt::After(t.clone()),
            Start::At { cluster, pos } => StartAt::At {
                cluster: cluster.clone(),
                pos: pos.clone(),
            },
        };
        // An event stream keeps its outbox only; a change topic reads the
        // tenant's tables and DDL.
        let outboxes_only = !self.keep.is_empty()
            && self
                .keep
                .iter()
                .all(|k| k.starts_with(sprig_streams::model::OUTBOX_PREFIX));
        let clusters = if outboxes_only {
            Arc::clone(&self.env.outboxes)
        } else {
            Arc::clone(&self.env.whole)
        };
        let tail = TenantTail::start(
            self.tenant,
            at,
            clusters,
            Arc::new(MetaPlacement::new(Arc::clone(&self.env.dc))),
            Arc::new(CatalogResolver::new(Arc::new(Catalogs(Arc::clone(
                &self.env.locator,
            ))))),
            TailOptions {
                keep_internal: self.keep.clone(),
                ..TailOptions::default()
            },
        )
        .await?;
        Ok(Box::new(tail))
    }
}

#[async_trait]
impl JobEnv for StreamsEnv {
    async fn catalog(&self, tenant: TenantId) -> Result<(Pool, String)> {
        let ep = self.locator.writer(tenant).await?;
        Ok((ep.pool, ep.store.schema().to_owned()))
    }

    async fn shard_pool(&self, tenant: TenantId) -> Result<Pool> {
        let cluster = self.locator.cluster_of(tenant).await?.ok_or_else(|| {
            let mut e = SprigError::new(
                SprigCode::TenantNotReady,
                format!("tenant {tenant} has no shard map here"),
            );
            e.retryable = true;
            e
        })?;
        self.pools.writer(&cluster).await
    }

    fn feed(&self, tenant: TenantId, keep_internal: Vec<String>) -> Arc<dyn FeedOpener> {
        Arc::new(Feeds {
            env: self.clone(),
            tenant,
            keep: keep_internal,
        })
    }

    fn dc(&self) -> Arc<dyn MetaStore> {
        Arc::clone(&self.dc)
    }

    async fn tenant_name(&self, _tenant: TenantId) -> Option<String> {
        None
    }

    fn settings(&self) -> Option<(sprig_settings::Snapshot, sprig_common::ids::DcId)> {
        Some((self.settings.snapshot(), self.settings.dc().clone()))
    }
}

/// Progress through the job's lease.
struct Progress<'a>(&'a JobCtx);

#[async_trait]
impl Report for Progress<'_> {
    async fn progress(&self, v: Value) -> bool {
        match self.0.progress(v).await {
            Ok(held) => held,
            Err(e) => {
                tracing::warn!(error = %e, "stream sequencer progress not written");
                true
            }
        }
    }
}

/// `stream_sequencer` (see the module docs).
#[derive(Debug, Clone)]
pub struct StreamSequencer(pub StreamsEnv);

#[async_trait]
impl JobHandler for StreamSequencer {
    fn kind(&self) -> &'static str {
        JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let tenant = TenantId(cx.spec["tenant"].as_u64().ok_or("spec.tenant is missing")?);
        let stream = StreamId(cx.spec["stream"].as_u64().ok_or("spec.stream is missing")?);
        let env: Arc<dyn JobEnv> = Arc::new(self.0.clone());
        // A child token: the sequencer stops itself when its stream is
        // dropped, and the job must still record its end then.
        match run_job(env, tenant, stream, cx.cancel.child_token(), &Progress(cx)).await {
            Ok(JobEnd::Gone) => Ok(json!({"end": "gone", "stream": stream.0})),
            // Fenced or lost: another sequencer runs; the controller
            // replaces this job if none does.
            Ok(e) => Ok(json!({"end": format!("{e:?}"), "stream": stream.0})),
            Err(e) => Err(e.to_string()),
        }
    }
}
