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

#[tokio::test]
async fn hash_range_jobs_lock_their_shards_and_the_tenant() {
    use crate::api::HashRange;
    const HALF: u64 = 1 << 63;
    let m = MetaMover::new(store());
    let c = ClusterId::new("dc-a-c1").unwrap();
    let split = |shard: u32, lo: u64, hi: Option<u64>, a: u32, b: u32| {
        let mid = lo / 2 + hi.map_or(HALF, |h| h / 2);
        JobSpec::reshard(
            TenantId(5),
            3,
            &c,
            vec![(
                shard,
                vec![format!("t5_d1_g3_s{shard}")],
                HashRange { lo, hi },
            )],
            &c,
            vec![
                (a, HashRange { lo, hi: Some(mid) }),
                (b, HashRange { lo: mid, hi }),
            ],
        )
    };
    let a = m.submit(split(1, 0, Some(HALF), 3, 4)).await.unwrap();
    assert_eq!(m.status(&a).await.unwrap().kind, JobKind::SplitShard);
    // Another shard of the group splits beside it; the same shard can't.
    m.submit(split(2, HALF, None, 5, 6)).await.unwrap();
    let e = m.submit(split(1, 0, Some(HALF), 7, 8)).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ShardLocked));
    // A whole-tenant move blocks new hash-range jobs.
    m.submit(spec(9)).await.unwrap();
    let mut s9 = split(1, 0, Some(HALF), 3, 4);
    s9.tenant = TenantId(9);
    s9.sources[0].schemas = vec!["t9_d1_g3_s1".into()];
    let e = m.submit(s9).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ShardLocked));
    // Cancelling frees every lock it held.
    m.cancel(&a, "test").await.unwrap();
    m.submit(split(1, 0, Some(HALF), 7, 8)).await.unwrap();
    // Targets must cover the sources exactly.
    let mut bad = split(1, 0, Some(HALF), 9, 10);
    bad.targets[1].range = Some(HashRange {
        lo: 7,
        hi: Some(HALF),
    });
    assert!(m.submit(bad).await.is_err());
}

/// No worker runs a paused job, so a cancel there used to be recorded and
/// never acted on: it now sends the job to `rolling_back`,
/// which a worker claims.
#[tokio::test]
async fn a_cancelled_paused_job_rolls_back() {
    let m = MetaMover::new(store());
    let id = m.submit(spec(21)).await.unwrap();
    m.edit(&id, |r| {
        r.state = JobState::Paused;
        r.paused_from = Some(JobState::Ready);
        Ok(true)
    })
    .await
    .unwrap();
    assert!(!m.status(&id).await.unwrap().state.is_runnable());
    m.cancel(&id, "operator").await.unwrap();
    let (rec, _) = m.store().get(&id).await.unwrap();
    assert_eq!(rec.state, JobState::RollingBack);
    assert!(rec.state.is_runnable());
    assert_eq!(rec.cancel.as_deref(), Some("operator"));
    assert_eq!(rec.paused_from, None);
}

/// Copy slots are freed whatever `mover.max_copies_per_cluster` is now: a
/// slot taken under a larger value isn't left behind.
#[tokio::test]
async fn slots_are_freed_beyond_the_current_maximum() {
    let s = store();
    let c = ClusterId::new("dc-a-c2").unwrap();
    let (a, b, job) = (JobId::new_v7(), JobId::new_v7(), JobId::new_v7());
    let lease = s.dc.grant_lease(Duration::from_secs(60)).await.unwrap();
    // Slot 0 to a, 1 to b, 2 to job (taken while the maximum was 3).
    assert!(s.take_slot(&c, &a, 1, lease).await.unwrap());
    assert!(s.take_slot(&c, &b, 2, lease).await.unwrap());
    assert!(s.take_slot(&c, &job, 3, lease).await.unwrap());
    // Freed with the maximum lowered to 1.
    s.free_slots(&c, &job, 1).await.unwrap();
    assert!(
        s.take_slot(&c, &JobId::new_v7(), 3, lease).await.unwrap(),
        "slot 2 is free"
    );
    assert!(
        !s.take_slot(&c, &JobId::new_v7(), 3, lease).await.unwrap(),
        "all three taken"
    );
    // a's and b's slots stay theirs.
    s.free_slots(&c, &job, 3).await.unwrap();
    assert!(s.take_slot(&c, &a, 1, lease).await.unwrap());
    assert!(s.take_slot(&c, &b, 2, lease).await.unwrap());
}

/// Mutual exclusion in the other direction: a
/// whole-tenant move never starts beside a split, merge or shard move of
/// the tenant, and a tenant with table groups doesn't move whole.
#[tokio::test]
async fn whole_tenant_moves_wait_for_shard_jobs_and_refuse_sharded_tenants() {
    use crate::api::HashRange;
    const HALF: u64 = 1 << 63;
    let js = store();
    let m = MetaMover::new(js.clone());
    let c = ClusterId::new("dc-a-c1").unwrap();
    let split = JobSpec::reshard(
        TenantId(11),
        3,
        &c,
        vec![(
            1,
            vec!["t11_d1_g3_s1".into()],
            HashRange { lo: 0, hi: None },
        )],
        &c,
        vec![
            (
                2,
                HashRange {
                    lo: 0,
                    hi: Some(HALF),
                },
            ),
            (3, HashRange { lo: HALF, hi: None }),
        ],
    );
    let s = m.submit(split).await.unwrap();
    // A running split: the move is refused (retryable) and leaves nothing.
    let e = m.submit(spec(11)).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ShardBusy));
    assert!(e.retryable);
    assert_eq!(m.list(&JobFilter::default()).await.unwrap().len(), 1);
    // A copy-and-swap is a whole-tenant job too.
    let cs = JobSpec::copy_swap(
        TenantId(11),
        c.clone(),
        "t11_d1_g0_s1",
        "t",
        "ADD COLUMN x INT",
    );
    assert_eq!(
        m.submit(cs).await.unwrap_err().code(),
        Some(SprigCode::ShardBusy)
    );
    m.cancel(&s, "test").await.unwrap();
    // The tenant has a group map: refused for good (not retryable).
    let key = js.meta().tier().key("shardmaps/11/3").unwrap();
    js.meta()
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key,
                value: b"{}".to_vec(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
    assert_eq!(js.sharded_groups(TenantId(11)).await.unwrap(), vec![3]);
    let e = m.submit(spec(11)).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ShardedTenantMove));
    assert!(!e.retryable);
    // Group 0 is the catalog's own map: an unsharded tenant still moves.
    let key0 = js.meta().tier().key("shardmaps/12/0").unwrap();
    js.meta()
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: key0,
                value: b"{}".to_vec(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
    m.submit(spec(12)).await.unwrap();
}

/// The advisor's first design holds the tenant lock while it creates the
/// groups' maps: no move and no split starts meanwhile.
#[tokio::test]
async fn the_tenant_lock_keeps_moves_out_while_held() {
    let js = store();
    let m = MetaMover::new(js.clone());
    assert_eq!(js.lock_tenant(TenantId(13), "advisor").await.unwrap(), None);
    // Taking it again as the same holder succeeds (a resumed apply).
    assert_eq!(js.lock_tenant(TenantId(13), "advisor").await.unwrap(), None);
    let e = m.submit(spec(13)).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ShardLocked));
    // Someone else's job holds it: the advisor can't take it.
    let other = m.submit(spec(14)).await.unwrap();
    assert_eq!(
        js.lock_tenant(TenantId(14), "advisor").await.unwrap(),
        Some(other.0.to_string())
    );
    // Releasing as another holder does nothing; as the holder frees it.
    js.unlock_tenant(TenantId(13), "someone").await.unwrap();
    assert!(m.submit(spec(13)).await.is_err());
    js.unlock_tenant(TenantId(13), "advisor").await.unwrap();
    m.submit(spec(13)).await.unwrap();
}

#[tokio::test]
async fn retention_follows_the_setting_and_an_operators_end_stands() {
    let st = store();
    let rec = st.create(spec(9)).await.unwrap();
    let hour = Duration::from_secs(3600);
    let now = st.now_ms();
    let mut r = rec.clone();
    r.state = JobState::Retaining;
    r.retain_from_ms = Some(now - 1000);
    r.retain_until_ms = Some(now - 1000 + 24 * 3_600_000);
    // `mover.source_retain_hours` as it resolves now: 24 h, then 0, then 48.
    assert_eq!(r.retain_until(24 * hour, now), r.retain_until_ms);
    assert_eq!(r.retain_until(Duration::ZERO, now), Some(now - 1000));
    assert_eq!(
        r.retain_until(48 * hour, now),
        Some(now - 1000 + 48 * 3_600_000)
    );
    // A job with its own retention (sharding.retire_hours) keeps it.
    let mut own = r.clone();
    own.spec.retain_ms = Some(5_000_000);
    assert_eq!(own.retain_until(Duration::ZERO, now), own.retain_until_ms);
    // Ended (by an operator or a tenant delete): the end stands.
    let mut ended = r.clone();
    ended.retain_until_ms = Some(now - 1);
    assert_eq!(ended.retain_until(48 * hour, now), Some(now - 1));
    // Records written before r2final have no start: the stored end.
    let mut old = r.clone();
    old.retain_from_ms = None;
    assert_eq!(old.retain_until(Duration::ZERO, now), old.retain_until_ms);
    // A worker holding the job doesn't overwrite an end written meanwhile.
    let lease = st.meta().grant_lease(Duration::from_secs(5)).await.unwrap();
    let (cur, rev) = st.get(&rec.job_id).await.unwrap();
    let mut c = st.claim(&cur, rev, "w1", lease).await.unwrap().unwrap();
    c.save(|x| {
        x.state = JobState::Retaining;
        x.retain_from_ms = r.retain_from_ms;
        x.retain_until_ms = r.retain_until_ms;
    })
    .await
    .unwrap();
    let (mut stored, rev) = st.get(&rec.job_id).await.unwrap();
    stored.retain_until_ms = Some(now - 5);
    let key = st.key(&job_rel(&rec.job_id)).unwrap();
    st.meta()
        .txn(sprig_meta::Txn {
            when: vec![sprig_meta::Cmp::ModRev(key.clone(), rev)],
            then: vec![sprig_meta::Op::Put {
                key,
                value: sprig_meta::Record::new(stored).encode().unwrap(),
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await
        .unwrap();
    c.save(|x| x.progress.rows_applied = 1).await.unwrap();
    let (after, _) = st.get(&rec.job_id).await.unwrap();
    assert_eq!(after.retain_until_ms, Some(now - 5));
}
