//! The deleted tenant's records: exact ids only (tenant 3 next to 30, 31
//! and 300), and its mover jobs brought to an end.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use serde_json::json;
use sprig_common::time::SystemClock;
use sprig_meta::conformance::unique_tier;
use sprig_meta::{Consistency, MemStore, Record};

use super::*;

const NEIGHBOURS: [u64; 4] = [3, 30, 31, 300];

fn rels(t: u64) -> Vec<String> {
    vec![
        format!("shardmaps/{t}/0"),
        format!("shardmaps/{t}/902"),
        format!("pins/{t}"),
        format!("promotions/{t}"),
        format!("feed/ownership/{t}/0/00000000000000000003"),
        format!("feed/subs/{t}/orders"),
        format!("feed/checkpoints/{t}/orders"),
        format!("logic/schedules/{t}/7"),
        format!("mover/shard-locks/{t}/g902s1"),
    ]
}

async fn put(store: &dyn MetaStore, rel: &str, value: &[u8]) {
    let r = store
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key: store.tier().key(rel).unwrap(),
                value: value.to_vec(),
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await
        .unwrap();
    assert!(r.succeeded);
}

async fn exists(store: &dyn MetaStore, rel: &str) -> bool {
    store
        .get(&store.tier().key(rel).unwrap(), Consistency::Linearizable)
        .await
        .unwrap()
        .is_some()
}

#[tokio::test]
async fn only_the_tenants_own_keys_go() {
    let store = MemStore::new(unique_tier());
    for t in NEIGHBOURS {
        for rel in rels(t) {
            put(&store, &rel, b"x").await;
        }
    }
    put(&store, "pins/30x", b"x").await;
    let n = purge_meta(&store, TenantId(3), &[]).await.unwrap();
    assert_eq!(n, rels(3).len());
    for rel in rels(3) {
        assert!(!exists(&store, &rel).await, "{rel} left");
    }
    for t in [30, 31, 300] {
        for rel in rels(t) {
            assert!(exists(&store, &rel).await, "{rel} of tenant {t} deleted");
        }
    }
    assert!(exists(&store, "pins/30x").await);
    // Idempotent: a second run (after a crash) finds nothing.
    assert_eq!(purge_meta(&store, TenantId(3), &[]).await.unwrap(), 0);
    // A failed guard deletes nothing.
    let guard = vec![Cmp::Missing(store.tier().key("pins/30").unwrap())];
    let e = purge_meta(&store, TenantId(30), &guard).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::NotLeader));
    assert!(exists(&store, "pins/30").await);
}

fn job(id: &str, tenant: u64, state: &str, retain_until_ms: Option<u64>) -> JobRecord {
    let v = json!({
        "job_id": id, "kind": "move_shard", "tenant_id": tenant,
        "spec": {"kind": "move_shard", "tenant": tenant,
            "sources": [{"cluster": "dc-a-c1", "shard": 0, "schemas": []}],
            "targets": [{"cluster": "dc-a-c2", "shard": 0, "schemas": []}],
            "routing": {"kind": "whole"}, "cutover": {"kind": "auto"},
            "verify": {"kind": "full"}, "requested_by": "test"},
        "state": state, "retain_until_ms": retain_until_ms,
        "created_ms": 1, "updated_ms": 1, "v": 1
    });
    serde_json::from_value(v).unwrap()
}

async fn put_job(store: &dyn MetaStore, rec: &JobRecord) {
    let value = Record::new(rec.clone()).encode().unwrap();
    put(store, &format!("mover/jobs/{}", rec.job_id.0), &value).await;
}

#[tokio::test]
async fn jobs_end_and_finished_records_go() {
    let dc: Arc<dyn MetaStore> = Arc::new(MemStore::new(unique_tier()));
    let jobs = JobStore::new(Arc::clone(&dc), Arc::new(SystemClock));
    let far = jobs.now_ms() + 86_400_000;
    let done = job("01a1bf44-69d0-73fc-a6eb-a52f22157001", 3, "done", None);
    let retaining = job(
        "01a1bf44-69d0-73fc-a6eb-a52f22157002",
        3,
        "retaining",
        Some(far),
    );
    let planned = job("01a1bf44-69d0-73fc-a6eb-a52f22157003", 3, "planned", None);
    let other = job(
        "01a1bf44-69d0-73fc-a6eb-a52f22157004",
        30,
        "retaining",
        Some(far),
    );
    let other_done = job("01a1bf44-69d0-73fc-a6eb-a52f22157005", 30, "done", None);
    for r in [&done, &retaining, &planned, &other, &other_done] {
        put_job(&*dc, r).await;
    }
    let s = settle_jobs(&jobs, TenantId(3)).await.unwrap();
    assert_eq!(s.open.len(), 2);
    assert_eq!(s.finished.len(), 1);
    assert_eq!(s.finished[0].0.job_id, done.job_id);
    // The retention ends now, flagged for the purge; the planned job (no
    // worker yet) rolls back at once.
    let (r, _) = jobs.get(&retaining.job_id).await.unwrap();
    assert!(r.purge_tenant);
    assert!(r.retain_until_ms.unwrap() <= jobs.now_ms());
    let (p, _) = jobs.get(&planned.job_id).await.unwrap();
    assert_eq!(p.state, JobState::RolledBack);
    // Tenant 30's jobs are untouched.
    let (o, _) = jobs.get(&other.job_id).await.unwrap();
    assert!(!o.purge_tenant);
    assert_eq!(o.retain_until_ms, Some(far));
    // Settling again changes nothing more (idempotent).
    let again = settle_jobs(&jobs, TenantId(3)).await.unwrap();
    assert_eq!(again.open.len(), 1);
    assert_eq!(again.finished.len(), 2);
    for (rec, rev) in &again.finished {
        assert!(jobs.remove_finished(rec, *rev, &[]).await.unwrap());
    }
    assert!(jobs.get(&done.job_id).await.is_err());
    assert!(jobs.get(&other_done.job_id).await.is_ok());
    // A running job's record is never removed.
    let (r, rev) = jobs.get(&retaining.job_id).await.unwrap();
    assert!(jobs.remove_finished(&r, rev, &[]).await.is_err());
}

#[test]
fn prefixes_end_with_a_slash() {
    for p in meta_prefixes(TenantId(3)) {
        assert!(p.ends_with("/3/"), "{p}");
    }
    assert_eq!(meta_keys(TenantId(3)), ["pins/3", "promotions/3"]);
}
