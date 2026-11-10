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

/// A tenant's residency for sink destinations (chapter 51, "Destinations,
/// allowlist and residency"; chapter 26): the policies of the tenant and
/// its ancestors from their directory entries (`residency.policy`), each
/// evaluated against a destination's tags as if they were a DC's. A
/// tenant with a residency block but no readable policy anywhere on its
/// path admits nothing (fail closed). No block on the path: unpinned.
pub struct PolicyResidency {
    policies: Vec<sprig_controller::residency::policy::Policy>,
    closed: bool,
}

impl std::fmt::Debug for PolicyResidency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolicyResidency")
            .field("policies", &self.policies.len())
            .field("closed", &self.closed)
            .finish()
    }
}

impl PolicyResidency {
    /// From policy texts (own and ancestors'); `closed` when a block
    /// exists without any policy to read.
    ///
    /// # Errors
    /// 39506 for a policy that doesn't parse.
    pub fn new(texts: &[String], closed: bool) -> Result<Self> {
        let mut policies = Vec::new();
        for t in texts {
            policies.push(sprig_controller::residency::policy::Policy::parse(t)?);
        }
        Ok(Self { policies, closed })
    }
}

impl sprig_sinks::policy::Residency for PolicyResidency {
    fn pinned(&self) -> bool {
        true
    }

    fn admits(&self, tags: &serde_json::Map<String, Value>) -> bool {
        if self.closed && self.policies.is_empty() {
            return false;
        }
        let Ok(id) = sprig_common::ids::DcId::new("sink-destination") else {
            return false;
        };
        let dc = sprig_meta::bootstrap::DcRecord {
            dc_id: id,
            kind: "full".into(),
            state: "active".into(),
            dc_epoch: 0,
            tags: tags.clone(),
            global_voter: false,
        };
        self.policies.iter().all(|p| p.admits(&dc))
    }
}

/// [`PolicyResidency`] of `tenant` from the global tier.
///
/// # Errors
/// The store's; 39506 for a bad policy.
pub async fn residency_of(
    global: &dyn MetaStore,
    tenant: TenantId,
) -> Result<Box<dyn sprig_sinks::policy::Residency>> {
    let entry = |id: u64| async move {
        let key = global.tier().key(&format!("tenants/{id}"))?;
        Ok::<_, SprigError>(
            global
                .get(&key, sprig_meta::Consistency::Serializable)
                .await?
                .and_then(|kv| serde_json::from_slice::<Value>(&kv.value).ok()),
        )
    };
    let Some(me) = entry(tenant.0).await? else {
        return Ok(Box::new(sprig_sinks::policy::Unpinned));
    };
    let path: Vec<u64> = me
        .get("path")
        .and_then(Value::as_str)
        .unwrap_or("")
        .split('/')
        .filter_map(|x| x.parse().ok())
        .collect();
    let mut texts = Vec::new();
    let mut block = false;
    for id in path {
        let e = if id == tenant.0 {
            Some(me.clone())
        } else {
            entry(id).await?
        };
        let Some(r) = e
            .as_ref()
            .and_then(|e| e.get("residency"))
            .filter(|r| !r.is_null())
        else {
            continue;
        };
        block = true;
        if let Some(p) = r.get("policy").and_then(Value::as_str)
            && !p.trim().is_empty()
        {
            texts.push(p.to_owned());
        }
    }
    if !block {
        return Ok(Box::new(sprig_sinks::policy::Unpinned));
    }
    Ok(Box::new(PolicyResidency::new(&texts, true)?))
}

#[cfg(test)]
mod residency_tests {
    use super::*;
    use sprig_sinks::policy::Residency as _;

    #[allow(clippy::needless_pass_by_value)]
    fn tags(v: Value) -> serde_json::Map<String, Value> {
        v.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn destinations_against_policies() {
        let r = PolicyResidency::new(&["jurisdiction = \"EU\"".to_owned()], true).unwrap();
        assert!(r.pinned());
        assert!(r.admits(&tags(
            serde_json::json!({"jurisdiction": "EU", "country": "DE"})
        )));
        assert!(!r.admits(&tags(serde_json::json!({"jurisdiction": "US"}))));
        assert!(!r.admits(&tags(serde_json::json!({}))));
        // An ancestor's policy narrows a member's.
        let r = PolicyResidency::new(
            &[
                "jurisdiction = \"EU\"".to_owned(),
                "country in (\"DE\", \"FR\")".to_owned(),
            ],
            true,
        )
        .unwrap();
        assert!(r.admits(&tags(
            serde_json::json!({"jurisdiction": "EU", "country": "FR"})
        )));
        assert!(!r.admits(&tags(
            serde_json::json!({"jurisdiction": "EU", "country": "NL"})
        )));
        // A block without any policy on the path admits nothing.
        let r = PolicyResidency::new(&[], true).unwrap();
        assert!(!r.admits(&tags(serde_json::json!({"jurisdiction": "EU"}))));
    }
}
