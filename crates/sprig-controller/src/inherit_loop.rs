//! The `inherit` loop (chapter 42, "Materialized copies"), on the DC
//! leader, every second:
//!
//! ```text
//! each active feed subscription of kind `inherit` (inherit:f<root>, one per
//!   tenant of a tree that has copies or cross-tenant shares)
//!   without a live `inherit_copy` job for that root → submit one
//!   (spec: root); a job whose worker died is requeued by dispatch, and a
//!   failed one is replaced here, so each tree has one copy worker
//! ```
//!
//! Like logic jobs, the job carries its tree in the spec, not in
//! `tenant_id`, so a long-running worker doesn't count as a tenant's
//! running job for dispatch's per-tenant rules.

use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use serde_json::json;
use sprig_common::Result;
use sprig_feed::subs::Subscription;
use sprig_inherit::subs::{JOB_KIND, KIND, root_of};
use sprig_meta::{ListOpts, Record};

use crate::dispatch::{JobClass, NewJob, list_jobs, submit};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InheritTick;

impl fmt::Display for InheritTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
#[derive(Debug, Default)]
pub struct InheritLoop;

/// The trees that want a copy worker, from the subscriptions' names.
#[must_use]
pub fn wanted(subs: &[Subscription]) -> Vec<u64> {
    let mut out: Vec<u64> = subs
        .iter()
        .filter(|s| s.kind == KIND && s.state == "active")
        .filter_map(|s| root_of(&s.name).map(|r| r.0))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

#[async_trait::async_trait]
impl Reconciler for InheritLoop {
    type Key = InheritTick;

    fn name(&self) -> &'static str {
        "inherit"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<InheritTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<InheritTick>> {
        Ok(vec![InheritTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &InheritTick) -> Result<Outcome> {
        let live: HashSet<u64> = list_jobs(cx.dc.as_ref())
            .await?
            .iter()
            .map(crate::dispatch::StoredJob::job)
            .filter(|j| !j.state.is_final() && j.kind == JOB_KIND)
            .filter_map(|j| j.spec["root"].as_u64())
            .collect();
        let prefix = cx.dc.tier().key("feed/subs/")?;
        let page = cx
            .dc
            .list(
                &prefix,
                ListOpts {
                    limit: 10_000,
                    ..ListOpts::default()
                },
            )
            .await?;
        let subs: Vec<Subscription> = page
            .kvs
            .iter()
            .filter_map(|kv| Record::<Subscription>::decode(&kv.value).ok())
            .map(|r| r.body)
            .collect();
        let l = cx.leader()?;
        for root in wanted(&subs) {
            if live.contains(&root) {
                continue;
            }
            let id = submit(
                l,
                cx.clock.as_ref(),
                NewJob {
                    kind: JOB_KIND.into(),
                    class: JobClass::Other,
                    tenant_id: None,
                    clusters: Vec::new(),
                    priority: 0,
                    cause: "inherit".into(),
                    zone: None,
                    spec: json!({ "root": root }),
                },
            )
            .await?;
            tracing::info!(root, job = %id, "copy worker job submitted");
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(1)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_common::ids::TenantId;

    fn sub(t: u64, name: &str, kind: &str) -> Subscription {
        Subscription {
            tenant: TenantId(t),
            name: name.into(),
            kind: kind.into(),
            ordering: "merged".into(),
            owner_ref: None,
            state: "active".into(),
            created_at_ms: 0,
        }
    }

    #[test]
    fn one_job_per_tree() {
        let subs = vec![
            sub(1, "inherit:f1", "inherit"),
            sub(2, "inherit:f1", "inherit"),
            sub(9, "inherit:f9", "inherit"),
            sub(3, "logic:b4", "logic"),
        ];
        assert_eq!(wanted(&subs), vec![1, 9]);
    }
}

/// Bootstrap hooks (R9, r9scale) that give a tenant created under an
/// ancestor declaring `FOR EACH TENANT` streams, exchanges, queues or
/// bindings the catalog modules its instances will need, with the rest of
/// its catalog: `inherit_msg`, `mq` for the AMQP kinds, `streams` for
/// streams. A member's first publish made these tables (about 15
/// `CREATE TABLE`s) on its path before; at 10,000 tenants that was most
/// of a first queue publish. The instances themselves stay lazy (chapter
/// 42): only empty catalog tables are made here, and only below a
/// declarer. Reading an ancestor's definitions or migrating fails
/// nothing: the first publish migrates as before.
#[derive(Clone)]
pub struct WithMessagingModules<H> {
    inner: H,
    locator: crate::catalogs::ShardMapLocator,
}

impl<H> WithMessagingModules<H> {
    /// `inner`'s hooks, plus the modules above.
    pub fn new(inner: H, locator: crate::catalogs::ShardMapLocator) -> Self {
        Self { inner, locator }
    }

    /// The kinds of active messaging definitions `tenant`'s ancestors
    /// declare (any scope; a member out of a definition's scope gets
    /// empty tables).
    async fn ancestor_kinds(&self, tenant: &crate::tree::DirectoryEntry) -> HashSet<String> {
        use mysql_async::prelude::Queryable;
        let mut kinds = HashSet::new();
        let ancestors = tenant
            .path
            .split('/')
            .filter_map(|s| s.parse::<u64>().ok())
            .filter(|id| *id != tenant.tenant_id.0);
        for a in ancestors {
            let a = sprig_common::ids::TenantId(a);
            let Ok(Some(cluster)) = self.locator.cluster_of(a).await else {
                continue;
            };
            let Ok(pool) = self.locator.pools().writer(&cluster).await else {
                continue;
            };
            let Ok(mut c) = pool.get_conn().await else {
                continue;
            };
            // No `inherited_defs` table (1146): nothing declared there.
            let found: Vec<String> = c
                .query(format!(
                    "SELECT DISTINCT CAST(kind AS CHAR) FROM {}.inherited_defs WHERE state = 'active' \
                     AND kind IN ('stream', 'exchange', 'queue', 'binding')",
                    sprig_common::names::quote(&self.locator.schema(a))
                ))
                .await
                .unwrap_or_default();
            kinds.extend(found);
        }
        kinds
    }
}

#[async_trait::async_trait]
impl<H: crate::tenants::BootstrapHooks> crate::tenants::BootstrapHooks for WithMessagingModules<H> {
    fn modules(&self) -> Vec<sprig_catalog::Module> {
        self.inner.modules()
    }

    async fn catalog_ready(
        &self,
        tenant: &crate::tree::DirectoryEntry,
        catalog: &sprig_catalog::CatalogEndpoint,
    ) -> Result<()> {
        self.inner.catalog_ready(tenant, catalog).await?;
        let kinds = self.ancestor_kinds(tenant).await;
        if kinds.is_empty() {
            return Ok(());
        }
        let mut modules = vec![sprig_inherit::msg::store::MSG_MODULE];
        if ["exchange", "queue", "binding"]
            .iter()
            .any(|k| kinds.contains(*k))
        {
            modules.push(sprig_queues::catalog::MQ_MODULE);
        }
        if kinds.contains("stream") {
            modules.push(sprig_streams::store::STREAMS_MODULE);
        }
        let opts = sprig_catalog::migrate::MigrateOpts {
            applied_by: "controller:inherit".to_owned(),
            ..sprig_catalog::migrate::MigrateOpts::default()
        };
        let run = async {
            let mut c = catalog.pool.get_conn().await.map_err(|e| {
                sprig_common::SprigError::new(
                    sprig_common::SprigCode::CatalogUnavailable,
                    format!("catalog: {e}"),
                )
            })?;
            sprig_catalog::Migrator::new(&modules)?
                .run(&mut c, catalog.store.schema(), &opts)
                .await
        };
        if let Err(e) = run.await {
            tracing::warn!(tenant = %tenant.tenant_id, error = %e, "messaging catalog modules not made at creation; the first publish makes them");
        }
        Ok(())
    }

    async fn dropping(
        &self,
        tenant: &crate::tree::DirectoryEntry,
        catalog: Option<&sprig_catalog::CatalogEndpoint>,
    ) -> Result<()> {
        self.inner.dropping(tenant, catalog).await
    }
}
