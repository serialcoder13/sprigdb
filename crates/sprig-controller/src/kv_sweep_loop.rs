//! The `kv_sweep` loop (active expiry and garbage collection of key-value
//! data), on the DC leader, every ten seconds:
//!
//! ```text
//! no live `kv_sweep` job in the DC → submit one, its spec the kv.* settings
//! now; a job whose worker died is requeued by dispatch, and a failed or
//! finished one is replaced here
//! ```
//!
//! The job sweeps every cluster of the DC (one task per
//! cluster) rather than one job per shard; the settings are read when the
//! job is submitted.

use std::fmt;
use std::time::Duration;

use serde_json::json;
use sprig_common::Result;
use sprig_common::ids::DcId;
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

use crate::dispatch::{JobClass, NewJob, list_jobs, submit};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// The job's kind.
pub const JOB_KIND: &str = "kv_sweep";

macro_rules! key {
    ($(#[$doc:meta])* $name:ident = $key:literal, $default:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<u64> = Setting {
            key: $key,
            default: $default,
            scopes: Scopes::PROCESS,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 33,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `kv.purge_interval_seconds`.
    PURGE_INTERVAL_SECONDS = "kv.purge_interval_seconds", 10, HardBounds::range(1, 3600),
    "Longest sweeper sleep"
);
key!(
    /// `kv.purge_batch_rows`.
    PURGE_BATCH_ROWS = "kv.purge_batch_rows", 500, HardBounds::range(1, 100_000),
    "Rows per sweeper delete"
);
key!(
    /// `kv.inline_delete_max_elements`.
    INLINE_DELETE_MAX_ELEMENTS = "kv.inline_delete_max_elements", 128, HardBounds::range(0, 1_000_000),
    "Larger collections' elements are deleted after the key row"
);
key!(
    /// `kv.orphan_scan_hours`.
    ORPHAN_SCAN_HOURS = "kv.orphan_scan_hours", 24, HardBounds::range(0, 8760),
    "Hours between scans for orphaned elements; 0 scans every pass"
);

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SweepTick;

impl fmt::Display for SweepTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
pub struct KvSweepLoop {
    live: Option<(LiveSettings, DcId)>,
}

impl fmt::Debug for KvSweepLoop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KvSweepLoop").finish_non_exhaustive()
    }
}

impl KvSweepLoop {
    /// The loop, reading the `kv.*` settings from `live` (defaults
    /// without).
    #[must_use]
    pub fn new(live: Option<(LiveSettings, DcId)>) -> Self {
        Self { live }
    }

    fn spec(&self) -> serde_json::Value {
        let get = |s: &Setting<u64>| match &self.live {
            Some((l, dc)) => l.get(s, &SettingCtx::process(dc)),
            None => s.default,
        };
        json!({
            "purge_interval_seconds": get(&PURGE_INTERVAL_SECONDS),
            "purge_batch_rows": get(&PURGE_BATCH_ROWS),
            "inline_delete_max_elements": get(&INLINE_DELETE_MAX_ELEMENTS),
            "orphan_scan_hours": get(&ORPHAN_SCAN_HOURS),
        })
    }
}

#[async_trait::async_trait]
impl Reconciler for KvSweepLoop {
    type Key = SweepTick;

    fn name(&self) -> &'static str {
        "kv_sweep"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<SweepTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<SweepTick>> {
        Ok(vec![SweepTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &SweepTick) -> Result<Outcome> {
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
                    cause: "kv_sweep".into(),
                    zone: None,
                    spec: self.spec(),
                },
            )
            .await?;
            tracing::info!(job = %id, "key-value sweeper job submitted");
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(10)))
    }
}
