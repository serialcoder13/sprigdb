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
use sprig_sinks::job::{SinkEnv, run_sink_task_of};
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
    /// `SPRIG_SINK_SECRET_DIR`, which overrides `sink.platform_secret_dir`.
    secret_dir: Option<PathBuf>,
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
pub(crate) struct RlsCatalogs {
    source: sprig_catalog::cache::MariaSource,
    cache: std::sync::Mutex<
        std::collections::HashMap<
            TenantId,
            (std::time::Instant, Arc<sprig_catalog::TenantCatalog>),
        >,
    >,
}

impl RlsCatalogs {
    pub(crate) fn new(locator: Arc<ShardMapLocator>) -> Option<Self> {
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
    pub(crate) async fn binds(
        &self,
        tenant: TenantId,
        owner: u32,
        objects: &[u64],
    ) -> Result<bool> {
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

    /// Chapter 51 (39229, authfix2): why sink owner `owner` can no longer
    /// read `sources`, from the tenant's catalog (at most 2 s old).
    pub(crate) async fn owner_lost(
        &self,
        tenant: TenantId,
        owner: u32,
        sources: &[sprig_sinks::job::OwnerRead],
        settings: Option<(sprig_settings::Snapshot, sprig_common::ids::DcId)>,
    ) -> Result<Option<String>> {
        use sprig_auth::authz::owner::{ReadSource, lost_read};
        let cat = self.catalog(tenant).await?;
        let lctn = settings.is_some_and(|(snap, dc)| {
            cat.settings
                .resolve(
                    &snap,
                    sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES.key,
                    &cat.settings.ctx(&dc),
                )
                .ok()
                .and_then(|r| r.value.as_f64())
                == Some(1.0)
        });
        let reads: Vec<ReadSource> = sources
            .iter()
            .map(|s| match s {
                sprig_sinks::job::OwnerRead::Named { db, name } => ReadSource::Named {
                    db: *db,
                    name: name.clone(),
                },
                sprig_sinks::job::OwnerRead::Object(id) => ReadSource::Object(*id),
                sprig_sinks::job::OwnerRead::InDatabase { db, name } => ReadSource::InDatabase {
                    db: db.clone(),
                    name: name.clone(),
                },
                sprig_sinks::job::OwnerRead::FanIn => ReadSource::FanIn,
            })
            .collect();
        Ok(lost_read(
            &cat,
            sprig_common::ids::UserId(owner),
            &reads,
            lctn,
        ))
    }

    /// The `@sprig_rls` context of user `user` (chapter 29's JSON, as the
    /// gateway builds it for a session; RL42): what MariaDB's policy views
    /// and write triggers apply while worker-run code of that definer runs.
    /// `None` when the tenant has no row-level security. Claims are absent
    /// (no token) and the subtree sets empty, so policies naming them show
    /// nothing to such code.
    pub(crate) async fn context(&self, tenant: TenantId, user: u32) -> Result<Option<String>> {
        use sprig_auth::authz::{AuthzState, CompiledPrivileges};
        let cat = self.catalog(tenant).await?;
        if sprig_auth::authz::rls::RlsState::of(&cat).is_none() {
            return Ok(None);
        }
        let authz = AuthzState::of(&cat).ok_or_else(|| {
            SprigError::new(
                SprigCode::RlsUnsupported,
                "no grants to compile the definer's policies",
            )
        })?;
        let u = sprig_common::ids::UserId(user);
        let row = sprig_auth::TenantAuth::of(&cat)
            .and_then(|a| a.user(u))
            .map(|x| x.row.clone())
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::RlsUnsupported,
                    format!("the definer (user {user}) is gone"),
                )
            })?;
        let c = CompiledPrivileges::compile(authz, u, &authz.granted_roles(u));
        let r = sprig_auth::authz::rls::compile(&cat, &c, u);
        let v = json!({
            "b": u8::from(r.bypass),
            "x": r.exempt.iter().map(|o| o.0).collect::<Vec<_>>(),
            "p": r.policy_ids,
            "s": {
                "user": format!("{}@{}", row.name, row.host),
                "uid": user,
                "tid": tenant.0,
                "htid": tenant.0,
                "space": (row.home_space_id != 0).then_some(row.home_space_id),
                "claims": Value::Null,
                "attrs": row.attributes.clone().unwrap_or(Value::Null),
                "sub": Vec::<u64>::new(),
                "man": Vec::<u64>::new(),
            }
        });
        Ok(Some(v.to_string()))
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
            secret_dir: std::env::var_os("SPRIG_SINK_SECRET_DIR").map(PathBuf::from),
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
        self.secret_dir.clone().unwrap_or_else(|| {
            PathBuf::from(
                self.settings
                    .get(&sprig_sinks::settings::PLATFORM_SECRET_DIR)
                    .as_ref(),
            )
        })
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
            // RL36: without the catalogs the owner can't be checked, so a
            // change sink doesn't run (fail closed, not open).
            None => Err(sprig_common::SprigError::new(
                sprig_common::SprigCode::RlsUnsupported,
                "ER_SPRIG_RLS_UNSUPPORTED: the worker can't load row-level security catalogs to \
                 check the sink's owner",
            )),
        }
    }

    fn settings(&self) -> Option<(sprig_settings::Snapshot, sprig_common::ids::DcId)> {
        Some((self.settings.snapshot(), self.settings.dc().clone()))
    }

    async fn owner_lost(
        &self,
        tenant: TenantId,
        owner: u32,
        sources: &[sprig_sinks::job::OwnerRead],
    ) -> Result<Option<String>> {
        match &self.rls {
            Some(r) => r.owner_lost(tenant, owner, sources, self.settings()).await,
            None => Ok(None),
        }
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
        // R9 (r9last): a fan-in task's group count (the index's `tasks`
        // when it was submitted); 1 for jobs from before grouping.
        let of = cx.spec["of"]
            .as_u64()
            .map_or(1, |n| u32::try_from(n).unwrap_or(u32::MAX));
        let env: Arc<dyn SinkEnv> = Arc::new(self.0.clone());
        let cancel = Cancel::new();
        let c2 = cancel.clone();
        let token = cx.cancel.clone();
        let watch = tokio::spawn(async move {
            token.cancelled().await;
            c2.cancel();
        });
        let r = run_sink_task_of(env, tenant, sink, task, of, cancel).await;
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
