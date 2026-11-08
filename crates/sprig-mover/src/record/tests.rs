#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::time::ManualClock;
use sprig_meta::{MemStore, MetaStore, Tier};

use super::*;
use crate::api::{JobFilter, JobKind, JobSpec, JobState, Mover};

fn store() -> JobStore {
    let tier = Tier::Dc(DcId::new("dc-a").unwrap());
    let s: Arc<dyn MetaStore> = Arc::new(MemStore::new(tier));
    JobStore::new(s, Arc::new(ManualClock::new()))
}

fn spec(t: u64) -> JobSpec {
    JobSpec::move_tenant(
        TenantId(t),
        ClusterId::new("dc-a-c1").unwrap(),
        ClusterId::new("dc-a-c2").unwrap(),
    )
}

#[tokio::test]
async fn one_job_per_shard() {
    let m = MetaMover::new(store());
    let a = m.submit(spec(1)).await.unwrap();
    let e = m.submit(spec(1)).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ShardLocked));
    m.submit(spec(2)).await.unwrap();
    // Cancelling a job no worker started ends it and frees the shard.
    m.cancel(&a, "test").await.unwrap();
    assert_eq!(m.status(&a).await.unwrap().state, JobState::RolledBack);
    m.submit(spec(1)).await.unwrap();
    let all = m.list(&JobFilter::default()).await.unwrap();
    assert_eq!(all.len(), 3);
    let active = m
        .list(&JobFilter {
            tenant: Some(TenantId(1)),
            active_only: true,
        })
        .await
        .unwrap();
    assert_eq!(active.len(), 1);
}

#[tokio::test]
async fn specs_r0_cant_run_are_refused() {
    let m = MetaMover::new(store());
    let mut s = spec(3);
    s.kind = JobKind::SplitShard;
    assert!(m.submit(s).await.is_err());
    let same = JobSpec::move_tenant(
        TenantId(3),
        ClusterId::new("dc-a-c1").unwrap(),
        ClusterId::new("dc-a-c1").unwrap(),
    );
    assert!(m.submit(same).await.is_err());
    let cs = JobSpec::copy_swap(
        TenantId(3),
        ClusterId::new("dc-a-c1").unwrap(),
        "t3_d1_g1_s1",
        "t",
        "ADD COLUMN x INT",
    );
    m.submit(cs).await.unwrap();
}

#[tokio::test]
async fn claims_raise_the_token_and_guard_writes() {
    let st = store();
    let rec = st.create(spec(5)).await.unwrap();
    let lease1 = st.meta().grant_lease(Duration::from_secs(5)).await.unwrap();
    let (r, rev) = st.get(&rec.job_id).await.unwrap();
    let mut c1 = st.claim(&r, rev, "w1", lease1).await.unwrap().unwrap();
    assert_eq!(c1.token(), 1);
    // Owned: nobody else claims it.
    let lease2 = st.meta().grant_lease(Duration::from_secs(5)).await.unwrap();
    let (r, rev) = st.get(&rec.job_id).await.unwrap();
    assert!(st.claim(&r, rev, "w2", lease2).await.unwrap().is_none());
    c1.save(|r| r.state = JobState::Copying).await.unwrap();
    // An operator's cancel survives the owner's next write.
    MetaMover::new(st.clone())
        .cancel(&rec.job_id, "op")
        .await
        .unwrap();
    c1.save(|r| r.progress.rows_copied = 7).await.unwrap();
    let (r, _) = st.get(&rec.job_id).await.unwrap();
    assert_eq!(r.cancel.as_deref(), Some("op"));
    assert_eq!(r.progress.rows_copied, 7);
    // The lease ends: w1 can't write; w2 claims with token 2.
    st.meta().revoke_lease(lease1).await.unwrap();
    assert_eq!(
        c1.save(|r| r.progress.rows_copied = 9)
            .await
            .unwrap_err()
            .code(),
        Some(SprigCode::JobLeaseLost)
    );
    let (r, rev) = st.get(&rec.job_id).await.unwrap();
    let mut c2 = st.claim(&r, rev, "w2", lease2).await.unwrap().unwrap();
    assert_eq!(c2.token(), 2);
    // Copy slots: two per cluster.
    let c = ClusterId::new("dc-a-c1").unwrap();
    let (j1, j2, j3) = (JobId::new_v7(), JobId::new_v7(), JobId::new_v7());
    assert!(st.take_slot(&c, &j1, 2, lease2).await.unwrap());
    assert!(st.take_slot(&c, &j1, 2, lease2).await.unwrap());
    assert!(st.take_slot(&c, &j2, 2, lease2).await.unwrap());
    assert!(!st.take_slot(&c, &j3, 2, lease2).await.unwrap());
    st.free_slots(&c, &j1, 2).await.unwrap();
    assert!(st.take_slot(&c, &j3, 2, lease2).await.unwrap());
    // Finishing frees the shard lock.
    c2.finish(JobState::Done, None).await.unwrap();
    MetaMover::new(st.clone()).submit(spec(5)).await.unwrap();
    // A committed job can't be cancelled.
    let m = MetaMover::new(st);
    let e = m.cancel(&rec.job_id, "late").await;
    assert!(e.is_err() || m.status(&rec.job_id).await.unwrap().state == JobState::Done);
}
