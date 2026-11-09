//! Chapter 20's write sketches in the gateway (build step 1): what the
//! router writes, per shard and minute, for load splits.
//!
//! ```text
//! router plans INSERT / REPLACE / UPDATE / DELETE of a sharded tenant:
//!   (plan, notes) = write_notes::capture(plan)       routed hashes
//!   per target shard: one write per note with its hash, or one without
//!   reference-table and unrouted writes: one on their cluster
//! registry write, when a minute completed (or the lease is new):
//!   put gateway-load/<gateway_id> = LoadReport{sketches, cluster_writes}
//!   under the registration lease
//! ```
//!
//! The accumulator is process-wide, as the router's other state is: two
//! gateways in one test process share it. Writes are counted when planned
//! (a stale-map retry plans and counts again; a write that fails still
//! counted). Plans narrowed by a unique lookup (`ViaLookup`) aren't
//! counted yet.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use sprig_balancer::load::{LoadAccumulator, report_key};
use sprig_balancer::model::ShardKey;
use sprig_balancer::shards::HashRange;
use sprig_common::Result;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_meta::{LeaseId, MetaStore, Op, Record};
use sprig_router::Plan;
use sprig_router::write_notes::{self, Note};

/// One write: its shard, cluster and, when keyed, range and hash.
type ShardWrite = (ShardKey, ClusterId, Option<(HashRange, u64)>);

static LOAD: OnceLock<LoadAccumulator> = OnceLock::new();
/// Per gateway id: the lease and newest minute last written.
static WRITTEN: OnceLock<Mutex<HashMap<String, (LeaseId, u64)>>> = OnceLock::new();

/// The gateway's load accumulator.
#[must_use]
pub fn accumulator() -> &'static LoadAccumulator {
    LOAD.get_or_init(LoadAccumulator::new)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Whether `text` writes: its first word (after comments and opening
/// parentheses) is `INSERT`, `REPLACE`, `UPDATE` or `DELETE`.
#[must_use]
pub fn is_write(text: &str) -> bool {
    let mut t = text;
    loop {
        t = t.trim_start_matches(|c: char| c.is_whitespace() || c == '(');
        if let Some(r) = t.strip_prefix("/*") {
            t = r.split_once("*/").map_or("", |x| x.1);
        } else if t.starts_with("-- ") || t.starts_with('#') {
            t = t.split_once('\n').map_or("", |x| x.1);
        } else {
            break;
        }
    }
    let w: String = t
        .chars()
        .take_while(char::is_ascii_alphabetic)
        .take(8)
        .collect::<String>()
        .to_ascii_uppercase();
    matches!(w.as_str(), "INSERT" | "REPLACE" | "UPDATE" | "DELETE")
}

/// Runs the router's planning call `plan` for `text`; for a write of a
/// tenant's session, counts what it routed (module docs). `plan_of`
/// finds the plan in what `plan` returns.
pub fn observe<T>(
    tenant: Option<TenantId>,
    text: &str,
    session_cluster: &str,
    plan: impl FnOnce() -> T,
    plan_of: impl Fn(&T) -> &Plan,
) -> T {
    let Some(tenant) = tenant else {
        return plan();
    };
    if !is_write(text) {
        return plan();
    }
    let (out, notes) = write_notes::capture(plan);
    count(
        accumulator(),
        now_ms(),
        tenant,
        plan_of(&out),
        &notes,
        session_cluster,
    );
    out
}

/// Counts one planned write into `acc` at `at_ms`.
pub fn count(
    acc: &LoadAccumulator,
    at_ms: u64,
    tenant: TenantId,
    plan: &Plan,
    notes: &[Note],
    session_cluster: &str,
) {
    let targets = match plan {
        Plan::Pass(t) => std::slice::from_ref(t),
        Plan::Write(w) => w.targets.as_slice(),
        Plan::NotRouted => {
            if let Ok(c) = ClusterId::new(session_cluster) {
                acc.sketches().record_cluster(at_ms, &c, 1);
            }
            return;
        }
        _ => return,
    };
    let mut writes: Vec<ShardWrite> = Vec::new();
    for t in targets {
        let Ok(cluster) = ClusterId::new(&t.cluster) else {
            continue;
        };
        if t.at.group == 0 {
            // A reference-table or unsharded write: its cluster only.
            acc.sketches().record_cluster(at_ms, &cluster, 1);
            continue;
        }
        let key = ShardKey::new(tenant.0, t.at.group, t.shard);
        let before = writes.len();
        for n in notes
            .iter()
            .filter(|n| n.group == t.at.group && n.shard == t.shard)
        {
            writes.push((
                key,
                cluster.clone(),
                Some((HashRange::new(n.lo, n.hi), n.hash)),
            ));
        }
        if writes.len() == before {
            writes.push((key, cluster, None));
        }
    }
    acc.sketches()
        .record(at_ms, writes.iter().map(|(k, c, h)| (*k, c, *h)));
}

/// The put of this gateway's load report to add to a registration write
/// under `lease`, when a minute completed since the last one written
/// under it; with the mark to pass to [`written`] once the write
/// succeeded.
///
/// # Errors
/// Key or encoding errors.
pub fn report_op(
    store: &dyn MetaStore,
    gateway: &str,
    lease: LeaseId,
) -> Result<Option<(Op, (LeaseId, u64))>> {
    let Some(acc) = LOAD.get() else {
        return Ok(None);
    };
    let now = now_ms();
    let Some(newest) = acc.sketches().newest_reported(now) else {
        return Ok(None);
    };
    let mark = (lease, newest);
    let marks = WRITTEN.get_or_init(Mutex::default);
    if marks
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(gateway)
        == Some(&mark)
    {
        return Ok(None);
    }
    let report = acc.report(gateway, now);
    let key = store.tier().key(&report_key(gateway))?;
    let value = Record::new(report).encode()?;
    Ok(Some((
        Op::Put {
            key,
            value,
            lease: Some(lease),
        },
        mark,
    )))
}

/// Records that the report of `mark` was written for `gateway`.
pub fn written(gateway: &str, mark: (LeaseId, u64)) {
    WRITTEN
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(gateway.to_owned(), mark);
    metrics::counter!("sprig_gateway_load_reports_total").increment(1);
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_router::plan::{ShardAt, Target, WriteKind, WritePlan};

    use super::*;

    fn target(cluster: &str, group: u32, shard: u32, lo: u64) -> Target {
        Target {
            cluster: cluster.into(),
            shard,
            sql: String::new(),
            at: ShardAt {
                group,
                db: 1,
                lo,
                schema: format!("t9_d1_g{group}_s{shard}"),
            },
        }
    }

    fn note(shard: u32, lo: u64, hi: Option<u64>, hash: u64) -> Note {
        Note {
            group: 3,
            shard,
            cluster: "c1".into(),
            lo,
            hi,
            hash,
        }
    }

    #[test]
    fn writes_are_recognized_after_comments() {
        for w in [
            "INSERT INTO t VALUES (1)",
            "  /* x */ update t set a = 1",
            "-- c\nDELETE FROM t",
            "# c\nreplace into t values (1)",
            "(INSERT INTO t VALUES (1))",
        ] {
            assert!(is_write(w), "{w}");
        }
        for r in [
            "SELECT 1",
            "/* INSERT */ SELECT 1",
            "EXPLAIN INSERT INTO t",
            "INSERTX",
            "",
        ] {
            assert!(!is_write(r), "{r}");
        }
    }

    #[test]
    fn routed_writes_count_by_hash_and_others_by_shard_or_cluster() {
        let acc = LoadAccumulator::new();
        let t0 = 7_000 * 60_000 + 1_000;
        let half = 1u64 << 63;
        // A two-row INSERT on shard 4 and one row on shard 5.
        let plan = Plan::Write(WritePlan {
            kind: WriteKind::Insert,
            targets: vec![target("c1", 3, 4, 0), target("c2", 3, 5, half)],
            rows: 3,
            claims: Vec::new(),
            releases: Vec::new(),
            fk: None,
        });
        let notes = vec![
            note(4, 0, Some(half), 10),
            note(4, 0, Some(half), 11),
            note(5, half, None, half + 1),
        ];
        count(&acc, t0, TenantId(9), &plan, &notes, "c1");
        // A scatter UPDATE: no hashes, one write per shard.
        count(&acc, t0, TenantId(9), &plan, &[], "c1");
        // A reference-table write and an unsharded one: their clusters.
        count(
            &acc,
            t0,
            TenantId(9),
            &Plan::Pass(target("c2", 0, 0, 0)),
            &[],
            "c1",
        );
        count(&acc, t0, TenantId(9), &Plan::NotRouted, &[], "c1");
        let r = acc.report("gw-1", t0 + 60_000);
        let s4 = r
            .sketches
            .iter()
            .find(|s| s.shard == ShardKey::new(9, 3, 4))
            .unwrap();
        assert_eq!((s4.writes, s4.hashed()), (3, 2));
        assert_eq!(s4.range, Some(HashRange::new(0, Some(half))));
        let s5 = r
            .sketches
            .iter()
            .find(|s| s.shard == ShardKey::new(9, 3, 5))
            .unwrap();
        assert_eq!((s5.writes, s5.hashed()), (2, 1));
        let totals: Vec<(String, u64)> = r
            .cluster_writes
            .iter()
            .map(|c| (c.cluster.to_string(), c.writes))
            .collect();
        assert_eq!(totals, vec![("c1".into(), 4), ("c2".into(), 3)]);
    }

    #[test]
    fn reads_and_sessions_without_a_tenant_are_not_counted() {
        let plan = observe(
            None,
            "INSERT INTO t VALUES (1)",
            "c1",
            || Plan::NotRouted,
            |p| p,
        );
        assert_eq!(plan, Plan::NotRouted);
        let plan = observe(
            Some(TenantId(1)),
            "SELECT 1",
            "c1",
            || Plan::NotRouted,
            |p| p,
        );
        assert_eq!(plan, Plan::NotRouted);
    }
}
