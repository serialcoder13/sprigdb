//! The `search_placement` loop (chapter 39, "Placement of copies"; build
//! step 5; R5 round 6), on the DC leader, every two seconds:
//!
//! ```text
//! each search/indexes/<tenant>/<index> record (a gateway writes it when the
//!   index is first used, with copies = search.copies):
//!   copies 0..copies-1 without a live search_copy job → submit one (class
//!     search: only workers with the search role; spread "search:<t>:<i>", so
//!     the copies of one index land on different workers when they can)
//! each live search_copy job whose record is gone (the index was dropped) or
//!   whose copy ≥ copies → cancelled
//! ```
//!
//! A job whose worker dies is requeued by dispatch and taken by another
//! search worker, whose copy seeds from the surviving copy.
//!
//! The names match `sprig_search::placement` (the controller doesn't link
//! the search crate).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use sprig_common::Result;
use sprig_meta::{ListOpts, Record, Versioned};

use crate::dispatch::{JobClass, NewJob, cancel, list_jobs, submit};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// The copies' job kind.
pub const JOB_KIND: &str = "search_copy";

/// What the loop reads of an index record.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct IndexRec {
    tenant: u64,
    index: u64,
    copies: u32,
}

impl Versioned for IndexRec {
    const V: u32 = 1;
}

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SearchTick;

impl fmt::Display for SearchTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchPlacementLoop;

/// The (tenant, index, copy) jobs to submit and the jobs to cancel, from
/// the wanted copies per index and the live jobs' (tenant, index, copy).
#[must_use]
pub fn plan(
    wanted: &HashMap<(u64, u64), u32>,
    live: &[(sprig_common::ids::JobId, (u64, u64, u32))],
) -> (Vec<(u64, u64, u32)>, Vec<sprig_common::ids::JobId>) {
    let mut have: HashSet<(u64, u64, u32)> = HashSet::new();
    let mut cancels = Vec::new();
    for (id, (t, i, n)) in live {
        match wanted.get(&(*t, *i)) {
            Some(c) if n < c && have.insert((*t, *i, *n)) => {}
            // Gone, past the copies wanted, or a duplicate of a copy.
            _ => cancels.push(*id),
        }
    }
    let mut submits = Vec::new();
    let mut keys: Vec<_> = wanted.iter().collect();
    keys.sort();
    for ((t, i), c) in keys {
        for n in 0..*c {
            if !have.contains(&(*t, *i, n)) {
                submits.push((*t, *i, n));
            }
        }
    }
    (submits, cancels)
}

#[async_trait::async_trait]
impl Reconciler for SearchPlacementLoop {
    type Key = SearchTick;

    fn name(&self) -> &'static str {
        "search_placement"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<SearchTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<SearchTick>> {
        Ok(vec![SearchTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &SearchTick) -> Result<Outcome> {
        let l = cx.leader()?;
        let prefix = cx.dc.tier().key("search/indexes/")?;
        let page = cx
            .dc
            .list(
                &prefix,
                ListOpts {
                    limit: 100_000,
                    ..ListOpts::default()
                },
            )
            .await?;
        let mut wanted: HashMap<(u64, u64), u32> = HashMap::new();
        for kv in &page.kvs {
            if let Ok(r) = Record::<IndexRec>::decode(&kv.value) {
                wanted.insert((r.body.tenant, r.body.index), r.body.copies);
            }
        }
        let mut live = Vec::new();
        for j in &list_jobs(cx.dc.as_ref()).await? {
            let job = j.job();
            if job.state.is_final() || job.kind != JOB_KIND {
                continue;
            }
            let s = &job.spec;
            let (Some(t), Some(i), Some(n)) = (
                s["tenant"].as_u64(),
                s["index"].as_u64(),
                s["copy"].as_u64().and_then(|n| u32::try_from(n).ok()),
            ) else {
                continue;
            };
            live.push((job.job_id, (t, i, n)));
        }
        let (submits, cancels) = plan(&wanted, &live);
        for id in cancels {
            if cancel(l, cx.clock.as_ref(), id).await? {
                tracing::info!(job = %id, "search copy job cancelled (index dropped or fewer copies)");
            }
        }
        for (t, i, n) in submits {
            let id = submit(
                l,
                cx.clock.as_ref(),
                NewJob {
                    kind: JOB_KIND.into(),
                    class: JobClass::Search,
                    tenant_id: None,
                    clusters: Vec::new(),
                    priority: 0,
                    cause: "search_placement".into(),
                    zone: None,
                    spec: json!({"tenant": t, "index": i, "copy": n, "spread": format!("search:{t}:{i}")}),
                },
            )
            .await?;
            tracing::info!(tenant = t, index = i, copy = n, job = %id, "search copy job submitted");
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(2)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_common::ids::JobId;

    #[test]
    fn plan_fills_missing_copies_and_cancels_extra_ones() {
        let j = |n: u128| JobId(uuid::Uuid::from_u128(n));
        let wanted = HashMap::from([((7, 1), 2), ((7, 2), 1)]);
        let live = vec![
            (j(1), (7, 1, 0)),
            (j(2), (7, 1, 0)), // a duplicate of copy 0
            (j(3), (7, 2, 1)), // past copies = 1
            (j(4), (7, 9, 0)), // index 9 dropped
        ];
        let (submits, mut cancels) = plan(&wanted, &live);
        assert_eq!(submits, vec![(7, 1, 1), (7, 2, 0)]);
        cancels.sort();
        assert_eq!(cancels, vec![j(2), j(3), j(4)]);
    }
}
