//! Sink tasks (chapter 51, R9) on chapter 23's job runtime: `sink_task`,
//! one job per task of a sink, long-running, submitted by the controller's
//! `sinks` loop from the DC's sink index. The job body is
//! [`sprig_sinks::job::run_sink_task`]; this module gives it the worker's
//! catalogs, shard pools, the global tier (platform connections), the
//! master key file (sink credentials) and the platform secret directory.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use mysql_async::Pool;
use serde_json::{Value, json};
use sprig_catalog::CatalogLocator;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_controller::catalogs::{ClusterPools, ShardMapLocator};
use sprig_controller::dispatch::JobClass;
use sprig_meta::MetaStore;
use sprig_sinks::job::{SinkEnv, run_sink_task};
use sprig_sinks::meta::JOB_KIND;
use sprig_sinks::model::{SinkId, TaskNo};
use sprig_sinks::runtime::{Cancel, TaskEnd};
use sprig_sinks::secrets::SinkKeyring;

use crate::logic::LogicEnv;
use crate::runtime::{JobCtx, JobHandler};

/// What sink tasks share.
#[derive(Clone)]
pub struct SinksEnv {
    dc: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    pools: Arc<dyn ClusterPools>,
    locator: Arc<ShardMapLocator>,
    keyring: Option<Arc<dyn SinkKeyring>>,
    secret_dir: PathBuf,
    worker: String,
    /// The worker's live settings (global `sink.*`).
    settings: crate::settings::WorkerSettings,
}

impl std::fmt::Debug for SinksEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinksEnv")
            .field("worker", &self.worker)
            .finish_non_exhaustive()
    }
}

impl SinksEnv {
    /// The logic jobs' catalogs and pools, the global tier, and the key
    /// file (`[gateway] workload_key_file`, the master secrets chapter 12
    /// and 51 derive their keys from). `SPRIG_SINK_SECRET_DIR` overrides
    /// `sink.platform_secret_dir`.
    #[must_use]
    pub fn new(
        env: &LogicEnv,
        global: Arc<dyn MetaStore>,
        key_file: Option<&std::path::Path>,
        worker: &str,
    ) -> Self {
        let keyring = key_file.and_then(
            |p| match sprig_workload::keyring::MasterKeyring::from_file(p) {
                Ok(k) => Some(Arc::new(k) as Arc<dyn SinkKeyring>),
                Err(e) => {
                    tracing::warn!(error = %e, "sink credential keys not loaded");
                    None
                }
            },
        );
        Self {
            dc: Arc::clone(&env.dc),
            global,
            pools: Arc::clone(&env.pools),
            locator: Arc::clone(&env.locator),
            keyring,
            secret_dir: std::env::var_os("SPRIG_SINK_SECRET_DIR").map_or_else(
                || PathBuf::from("/etc/sprigdb/platform-secrets"),
                PathBuf::from,
            ),
            worker: worker.to_owned(),
            settings: env.settings.clone(),
        }
    }
}

#[async_trait]
impl SinkEnv for SinksEnv {
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

    fn dc(&self) -> Arc<dyn MetaStore> {
        Arc::clone(&self.dc)
    }

    fn global(&self) -> Option<Arc<dyn MetaStore>> {
        Some(Arc::clone(&self.global))
    }

    fn keyring(&self) -> Option<Arc<dyn SinkKeyring>> {
        self.keyring.clone()
    }

    async fn residency_of(
        &self,
        tenant: TenantId,
    ) -> Result<Box<dyn sprig_sinks::policy::Residency>> {
        residency_of(&*self.global, tenant).await
    }

    fn secret_dir(&self) -> PathBuf {
        self.secret_dir.clone()
    }

    fn worker_id(&self) -> String {
        self.worker.clone()
    }

    fn settings(&self) -> Option<(sprig_settings::Snapshot, sprig_common::ids::DcId)> {
        Some((self.settings.snapshot(), self.settings.dc().clone()))
    }
}

/// `sink_task` (see the module docs).
#[derive(Debug, Clone)]
pub struct SinkTask(pub SinksEnv);

#[async_trait]
impl JobHandler for SinkTask {
    fn kind(&self) -> &'static str {
        JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Other
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let tenant = TenantId(cx.spec["tenant"].as_u64().ok_or("spec.tenant is missing")?);
        let sink = SinkId(cx.spec["sink"].as_u64().ok_or("spec.sink is missing")?);
        let task = TaskNo(
            u32::try_from(cx.spec["task"].as_u64().ok_or("spec.task is missing")?)
                .map_err(|_| "spec.task is out of range")?,
        );
        let env: Arc<dyn SinkEnv> = Arc::new(self.0.clone());
        let cancel = Cancel::new();
        let c2 = cancel.clone();
        let token = cx.cancel.clone();
        let watch = tokio::spawn(async move {
            token.cancelled().await;
            c2.cancel();
        });
        let r = run_sink_task(env, tenant, sink, task, cancel).await;
        watch.abort();
        match r {
            Ok(TaskEnd::Stopped) => Ok(json!({"end": "stopped", "sink": sink.0, "task": task.0})),
            Ok(e) => Ok(json!({"end": format!("{e:?}"), "sink": sink.0, "task": task.0})),
            Err(e) => Err(e.to_string()),
        }
    }
}

// The residency of sink destinations moved to `sprig-sinks` (R9, r9more),
// where gateways use it at `CREATE CONNECTION` too.
pub use sprig_sinks::residency::{PolicyResidency, residency_of};
