//! The reconciler that runs with each observation (chapter 13, "Observing
//! and dropping"): it compares `information_schema.STATISTICS` on every
//! shard with `auto_indexes`. MariaDB is the truth for whether an index
//! exists, the catalog for who owns it.
//!
//! ```text
//! for each shard of the object, on the writer:
//!   table gone                                → live rows dropped (table_dropped)
//!   sprig_auto_* index no live row explains   → dropped (stray)
//!   a live row's index differs from its row   → dropped; rolling_out: the shard
//!                                               is rebuilt; serving: row dropped
//!                                               (drift), re-evaluated later
//!   a serving row's index is gone             → what remains dropped; row
//!                                               dropped (drift)
//!   rolling_out row marked built, index gone  → shard back to pending
//!   retiring row's index not IGNORED          → IGNORED again
//! replicas (primary + replicas): a sprig_auto_* index whose definition
//!   differs from the writer's is reported, never written
//! ```
//!
//! `frozen` rows are left alone; `validating` and `recommended` rows
//! explain an index of their name (the canary, or one kept for rollout).

use std::collections::BTreeMap;

use mysql_async::prelude::Queryable;
use sprig_common::Result;
use sprig_common::ids::ObjectId;
use sprig_common::names::parse_auto_index;

use super::{ObserveCx, change, run_on_writer};
use crate::autoindex::rollout::node::{Node, NodeIndex, drop_sql, ignore_sql};
use crate::autoindex::store::{AutoIndexRow, AutoIndexStore, DropReason, IndexState, ShardBuild};

/// What one reconcile found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DriftReport {
    /// Stray `sprig_auto_*` indexes dropped, as `shard/name`.
    pub strays_dropped: Vec<String>,
    /// Rows moved to `dropped`, with the reason.
    pub rows_dropped: Vec<(String, DropReason)>,
    /// Shards of rolling-out rows reset to `pending`, as `name/shard`.
    pub shards_reset: Vec<String>,
    /// `IGNORED` re-applied, as `shard/name`.
    pub ignored_reapplied: Vec<String>,
    /// Replicas whose index differs from the writer's (reported only).
    pub replica_mismatch: Vec<String>,
    /// Statements that failed (retried next time).
    pub errors: Vec<String>,
}

fn live(s: IndexState) -> bool {
    !matches!(s, IndexState::Dropped | IndexState::Rejected)
}

fn serving(s: IndexState) -> bool {
    matches!(
        s,
        IndexState::Observing | IndexState::Active | IndexState::Retiring
    )
}

/// Reconciles one object's automatic indexes on `cx.shards`.
///
/// # Errors
/// Catalog errors; statement failures go to [`DriftReport::errors`].
#[allow(clippy::too_many_lines)] // one rule after another
pub async fn reconcile_object<Q: Queryable + Send>(
    store: &AutoIndexStore,
    q: &mut Q,
    cx: &ObserveCx<'_>,
    object: ObjectId,
) -> Result<DriftReport> {
    let mut rep = DriftReport::default();
    let rows = store.indexes_on(q, object).await?;
    let by_name: BTreeMap<&str, &AutoIndexRow> = rows
        .iter()
        .filter(|r| live(r.state))
        .map(|r| (r.name.as_str(), r))
        .collect();
    let Some(writer) = cx.view.nodes.writer() else {
        rep.errors.push("no writer".into());
        return Ok(rep);
    };
    let mut w = match Node::connect(writer, std::time::Duration::from_secs(10)).await {
        Ok(n) => n,
        Err(e) => {
            rep.errors.push(format!("writer: {e}"));
            return Ok(rep);
        }
    };
    let mut gone: Vec<&AutoIndexRow> = Vec::new();
    let mut dropped_rows: BTreeMap<String, DropReason> = BTreeMap::new();
    for t in cx.shards {
        let exists = match w.table_exists(t).await {
            Ok(e) => e,
            Err(e) => {
                rep.errors.push(format!("{}: {e}", t.shard));
                continue;
            }
        };
        if !exists {
            for r in by_name.values() {
                if r.state != IndexState::Frozen {
                    dropped_rows.insert(r.name.clone(), DropReason::TableDropped);
                }
            }
            continue;
        }
        let have: BTreeMap<String, NodeIndex> = match w.indexes(t).await {
            Ok(h) => h,
            Err(e) => {
                rep.errors.push(format!("{}: {e}", t.shard));
                continue;
            }
        };
        // Indexes on the shard.
        for (name, idx) in &have {
            if parse_auto_index(name).is_none() {
                continue;
            }
            match by_name.get(name.as_str()) {
                None => match run_on_writer(cx, t, &drop_sql(t, name)?).await {
                    Ok(()) => rep.strays_dropped.push(format!("{}/{name}", t.shard)),
                    Err(e) => rep.errors.push(format!("{}/{name}: {e}", t.shard)),
                },
                Some(r) if r.state == IndexState::Frozen => {}
                Some(r) => {
                    if r.definition.parts != idx.parts {
                        if let Err(e) = run_on_writer(cx, t, &drop_sql(t, name)?).await {
                            rep.errors.push(format!("{}/{name}: {e}", t.shard));
                            continue;
                        }
                        if r.state == IndexState::RollingOut {
                            store
                                .set_shard(q, r.index_id, &t.shard, ShardBuild::Pending)
                                .await?;
                            rep.shards_reset.push(format!("{name}/{}", t.shard));
                        } else if serving(r.state) {
                            dropped_rows.insert(r.name.clone(), DropReason::Drift);
                        }
                        continue;
                    }
                    if r.state == IndexState::Retiring
                        && !idx.ignored
                        && cx.view.instant_ignored_toggle
                    {
                        match run_on_writer(cx, t, &ignore_sql(t, name, true)?).await {
                            Ok(()) => rep.ignored_reapplied.push(format!("{}/{name}", t.shard)),
                            Err(e) => rep.errors.push(format!("{}/{name}: {e}", t.shard)),
                        }
                    }
                }
            }
        }
        // Rows whose index should be on the shard.
        for r in by_name.values() {
            if have.contains_key(&r.name) {
                continue;
            }
            if serving(r.state) {
                gone.push(r);
                dropped_rows
                    .entry(r.name.clone())
                    .or_insert(DropReason::Drift);
            } else if r.state == IndexState::RollingOut
                && r.shards.get(&t.shard) == Some(&ShardBuild::Built)
            {
                store
                    .set_shard(q, r.index_id, &t.shard, ShardBuild::Pending)
                    .await?;
                rep.shards_reset.push(format!("{}/{}", r.name, t.shard));
            }
        }
    }
    w.close().await;

    // Rows that drifted: drop what remains, then the row.
    for (name, reason) in dropped_rows {
        let Some(r) = by_name.get(name.as_str()) else {
            continue;
        };
        if reason == DropReason::Drift {
            let mut failed = false;
            for t in cx.shards {
                if let Err(e) = run_on_writer(cx, t, &drop_sql(t, &name)?).await {
                    rep.errors.push(format!("{}/{name}: {e}", t.shard));
                    failed = true;
                }
            }
            if failed {
                continue;
            }
        }
        if store
            .set_state(
                q,
                r.index_id,
                &change(r.state, IndexState::Dropped, cx.now, Some(reason)),
            )
            .await?
        {
            metrics::counter!("sprig_advisor_autoindex_drops_total", "reason" => reason.as_str())
                .increment(1);
            rep.rows_dropped.push((name, reason));
        }
    }
    let _ = gone;

    // Replicas: report only.
    if let crate::autoindex::rollout::Nodes::PrimaryReplica { replicas, .. } = &cx.view.nodes {
        for ep in replicas {
            let Ok(mut n) = Node::connect(ep, std::time::Duration::from_secs(10)).await else {
                rep.errors.push(format!("replica {} unreachable", ep.node));
                continue;
            };
            for t in cx.shards {
                if let Ok(h) = n.indexes(t).await {
                    for (name, idx) in &h {
                        if let Some(r) = by_name.get(name.as_str())
                            && serving(r.state)
                            && r.definition.parts != idx.parts
                        {
                            rep.replica_mismatch
                                .push(format!("{}:{}/{name}", ep.node, t.shard));
                        }
                    }
                }
            }
            n.close().await;
        }
    }
    if !rep.strays_dropped.is_empty() || !rep.rows_dropped.is_empty() {
        tracing::info!(
            tenant = %cx.tenant, object = object.0, strays = ?rep.strays_dropped,
            rows = ?rep.rows_dropped, "automatic index drift reconciled"
        );
    }
    Ok(rep)
}
