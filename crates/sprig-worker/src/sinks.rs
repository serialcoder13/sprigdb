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
    /// The DC's gateways (queue sources, the script `db` handle; R9).
    peer: Option<Arc<dyn sprig_sinks::peer::SinkPeer>>,
    /// Chapter 29 (RL31): tenant catalogs for sink owners' checks.
    rls: Option<Arc<RlsCatalogs>>,
}

/// Tenant catalogs with chapter 14's and 29's modules (users, grants,
/// row-level security), cached two seconds per tenant: what a change
/// sink's owner may see, checked before each batch (decision RL31).
struct RlsCatalogs {
    source: sprig_catalog::cache::MariaSource,
    cache: std::sync::Mutex<
        std::collections::HashMap<
            TenantId,
            (std::time::Instant, Arc<sprig_catalog::TenantCatalog>),
        >,
    >,
}

impl RlsCatalogs {
    fn new(locator: Arc<ShardMapLocator>) -> Option<Self> {
        let ext: Vec<Arc<dyn sprig_catalog::CatalogExtension>> = vec![
            Arc::new(sprig_auth::AuthExtension),
            Arc::new(sprig_auth::authz::AuthzExtension),
            Arc::new(sprig_auth::authz::rls::RlsExtension),
        ];
        sprig_catalog::cache::MariaSource::new(locator as Arc<dyn CatalogLocator>, ext)
            .ok()
            .map(|source| Self {
                source,
                cache: std::sync::Mutex::new(std::collections::HashMap::new()),
            })
    }

    async fn catalog(&self, tenant: TenantId) -> Result<Arc<sprig_catalog::TenantCatalog>> {
        use sprig_catalog::cache::CatalogSource;
        if let Some((at, c)) = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&tenant)
            && at.elapsed() < std::time::Duration::from_secs(2)
        {
            return Ok(Arc::clone(c));
        }
        let loaded = self
            .source
            .load(tenant, None, sprig_catalog::cache::Node::Writer)
            .await?;
        let reg = sprig_settings::Registry::global()?;
        let c = Arc::new(sprig_catalog::TenantCatalog::build(reg, tenant, loaded));
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(tenant, (std::time::Instant::now(), Arc::clone(&c)));
        Ok(c)
    }

    /// Whether policies on any of `objects` bind user `owner`.
    async fn binds(&self, tenant: TenantId, owner: u32, objects: &[u64]) -> Result<bool> {
        use sprig_auth::authz::{AuthzState, CompiledPrivileges};
        let cat = self.catalog(tenant).await?;
        let Some(state) = sprig_auth::authz::rls::RlsState::of(&cat) else {
            return Ok(false);
        };
        if !objects.iter().any(|o| {
            state
                .security(sprig_common::ids::ObjectId(*o))
                .is_some_and(|s| s.enabled)
        }) {
            return Ok(false);
        }
        let Some(authz) = AuthzState::of(&cat) else {
            return Ok(false);
        };
        let u = sprig_common::ids::UserId(owner);
        let c = CompiledPrivileges::compile(authz, u, &authz.granted_roles(u));
        let r = sprig_auth::authz::rls::compile(&cat, &c, u);
        Ok(objects
            .iter()
            .any(|o| r.tables.contains_key(&sprig_common::ids::ObjectId(*o))))
    }
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
            peer: None,
            rls: RlsCatalogs::new(Arc::clone(&env.locator)).map(Arc::new),
        }
    }

    /// Statements as sinks' owners go through `gateways` (R9, r9queue).
    #[must_use]
    pub fn with_peer(mut self, gateways: Arc<dyn sprig_sinks::peer::SinkPeer>) -> Self {
        self.peer = Some(gateways);
        self
    }
}

#[async_trait]
impl SinkEnv for SinksEnv {
    async fn catalog(&self, tenant: TenantId) -> Result<(Pool, String)> {
        let ep = self.locator.writer(tenant).await?;
        Ok((ep.pool, ep.store.schema().to_owned()))
    }

    fn peer(&self) -> Option<Arc<dyn sprig_sinks::peer::SinkPeer>> {
        self.peer.clone()
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

    async fn row_policies_bind(
        &self,
        tenant: TenantId,
        owner: u32,
        objects: &[u64],
    ) -> Result<bool> {
        match &self.rls {
            Some(r) => r.binds(tenant, owner, objects).await,
            None => Ok(false),
        }
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
        JobClass::Consumer
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
