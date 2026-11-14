//! The `doc_ttl` loop (document TTL indexes), on the DC leader, every ten
//! seconds:
//!
//! ```text
//! no live `doc_ttl` job in the DC → submit one; a job whose worker died is
//! requeued by dispatch, and a failed or finished one is replaced here
//! ```
//!
//! One job per DC is MongoDB's one TTL monitor per server: each collection
//! is expired by one task, whatever the number of gateways. The job
//! (`sprig_worker::doc_ttl`) expires the TTL indexes of tenants homed in
//! this DC, reading the `doc.*` keys below at every pass.

use std::fmt;
use std::time::Duration;

use serde_json::json;
use sprig_common::Result;
use sprig_settings::{Apply, HardBounds, Scopes, Setting};

use crate::dispatch::{JobClass, NewJob, list_jobs, submit};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// The job's kind.
pub const JOB_KIND: &str = "doc_ttl";

/// `doc.ttl_interval_seconds`: MongoDB's `ttlMonitorSleepSecs`.
pub static TTL_INTERVAL_SECONDS: Setting<u64> = Setting {
    key: "doc.ttl_interval_seconds",
    default: 60,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 86_400),
    apply: Apply::Live,
    operator_only: false,
    owner: 31,
    doc: "TTL pass interval: how often expired documents of TTL indexes are deleted",
};
sprig_settings::register!(TTL_INTERVAL_SECONDS);

/// `doc.multi_write_batch`.
pub static MULTI_WRITE_BATCH: Setting<u64> = Setting {
    key: "doc.multi_write_batch",
    default: 100,
    scopes: Scopes::DATA,
    hard: HardBounds::range(1, 100_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 31,
    doc: "Documents per transaction in multi-document writes (as built: TTL deletes, per tenant)",
};
sprig_settings::register!(MULTI_WRITE_BATCH);

/// `doc.ttl_tenant_deletes_per_second`.
pub static TTL_TENANT_DELETES_PER_SECOND: Setting<u64> = Setting {
    key: "doc.ttl_tenant_deletes_per_second",
    default: 5_000,
    scopes: Scopes::TREE,
    hard: HardBounds::range(0, 10_000_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 31,
    doc: "Most TTL deletes per second for one tenant; 0 for no limit",
};
sprig_settings::register!(TTL_TENANT_DELETES_PER_SECOND);

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TtlTick;

impl fmt::Display for TtlTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
#[derive(Debug, Default)]
pub struct DocTtlLoop;

#[async_trait::async_trait]
impl Reconciler for DocTtlLoop {
    type Key = TtlTick;

    fn name(&self) -> &'static str {
        "doc_ttl"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<TtlTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TtlTick>> {
        Ok(vec![TtlTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &TtlTick) -> Result<Outcome> {
        let l = cx.leader()?;
        let live = list_jobs(cx.dc.as_ref()).await?.iter().any(|j| {
            let job = j.job();
            !job.state.is_final() && job.kind == JOB_KIND
        });
        if !live {
            let id = submit(
                l,
                cx.clock.as_ref(),
                NewJob {
                    kind: JOB_KIND.into(),
                    // Its own pool: one job for as long as the DC runs.
                    class: JobClass::Consumer,
                    tenant_id: None,
                    clusters: Vec::new(),
                    priority: 0,
                    cause: "doc_ttl".into(),
                    zone: None,
                    spec: json!({}),
                },
            )
            .await?;
            tracing::info!(job = %id, "TTL monitor job submitted");
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(10)))
    }
}
