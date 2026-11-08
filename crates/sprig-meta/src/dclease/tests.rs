use std::sync::Arc;
use std::time::Duration;

use serde_json::Map;
use sprig_common::ids::DcId;
use sprig_common::time::Clock;

use super::*;
use crate::session::TokioClock;
use crate::{MemStore, Tier};

fn dc() -> DcId {
    DcId::new("fra1").expect("dc")
}

async fn store_with(state: &str) -> (MemStore, Arc<dyn MetaStore>, Arc<dyn Clock>) {
    let clock: Arc<dyn Clock> = Arc::new(TokioClock::new());
    let mem = MemStore::with_clock(Tier::Global, Arc::clone(&clock));
    let store: Arc<dyn MetaStore> = Arc::new(mem.clone());
    put_state(&store, state, 4).await;
    (mem, store, clock)
}

async fn put_state(store: &Arc<dyn MetaStore>, state: &str, epoch: u64) {
    let rec = Record::new(DcRecord {
        dc_id: dc(),
        kind: "full".into(),
        state: state.into(),
        dc_epoch: epoch,
        tags: Map::new(),
        global_voter: true,
    });
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: dc_key(&dc()).expect("key"),
                value: rec.encode().expect("encode"),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .expect("put");
}

async fn set_state(store: &Arc<dyn MetaStore>, state: &str) {
    let kv = store
        .get(&dc_key(&dc()).expect("key"), Consistency::Linearizable)
        .await
        .expect("get")
        .expect("dc");
    let mut rec = Record::<DcRecord>::decode(&kv.value).expect("decode");
    rec.body.state = state.into();
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: dc_key(&dc()).expect("key"),
                value: rec.encode().expect("encode"),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .expect("put");
}

fn lease(store: &Arc<dyn MetaStore>, clock: &Arc<dyn Clock>, holder: &str) -> DcLease {
    DcLease::new(
        dc(),
        holder,
        Arc::clone(store),
        Arc::clone(clock),
        DcLeaseOpts::default(),
    )
}

#[tokio::test(start_paused = true)]
async fn the_leader_takes_the_lease_and_raises_the_epoch() {
    let (_mem, store, clock) = store_with("active").await;
    let l = lease(&store, &clock, "c1");
    assert!(l.permit().fenced, "no lease yet");
    l.tick(false).await.expect("tick");
    assert!(l.permit().fenced, "a follower doesn't take a lease");
    l.tick(true).await.expect("tick");
    assert_eq!(l.dc_epoch(), Some(5));
    let p = l.permit();
    assert!(!p.fenced);
    assert_eq!(p.dc_epoch, 5);
    assert_eq!(p.valid_ms, 5000, "min(ttl/2, ttl - fence_lead)");
    let kv = store
        .get(&lease_key(&dc()).expect("key"), Consistency::Linearizable)
        .await
        .expect("get")
        .expect("lease key");
    assert!(kv.lease.is_some(), "the key is bound to the lease");
    let rec = Record::<DcLeaseRecord>::decode(&kv.value).expect("decode");
    assert_eq!(rec.body.dc_epoch, 5);
    assert_eq!(rec.body.holder, "c1");
}

#[tokio::test(start_paused = true)]
async fn followers_adopt_and_renew_the_same_lease() {
    let (_mem, store, clock) = store_with("active").await;
    let (a, b) = (lease(&store, &clock, "c1"), lease(&store, &clock, "c2"));
    a.tick(true).await.expect("tick");
    b.tick(false).await.expect("tick");
    assert_eq!(b.dc_epoch(), Some(5));
    assert!(!b.permit().fenced);
    // Only b renews from now on; the lease stays alive.
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(2500)).await;
        b.tick(false).await.expect("tick");
    }
    assert!(!b.permit().fenced, "b's keep-alives keep the shared lease");
    assert!(a.permit().fenced, "a stopped renewing: its own deadline passed");
}

#[tokio::test(start_paused = true)]
async fn permits_end_fence_lead_before_etcd_can_expire_the_lease() {
    let (mem, store, clock) = store_with("active").await;
    let l = lease(&store, &clock, "c1");
    l.tick(true).await.expect("tick");
    let last_ok = clock.now();
    mem.set_unavailable(true);
    let mut fenced_at = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let _ = l.tick(true).await;
        if fenced_at.is_none() && l.permit().fenced {
            fenced_at = Some(clock.now() - last_ok);
        }
    }
    let at = fenced_at.expect("fenced");
    assert!(at <= Duration::from_millis(7000), "fenced after {at:?}");
    assert!(at >= Duration::from_millis(6500), "fenced after {at:?}");
}

#[tokio::test(start_paused = true)]
async fn no_permits_unless_active_or_joining() {
    let (_mem, store, clock) = store_with("active").await;
    let l = lease(&store, &clock, "c1");
    l.tick(true).await.expect("tick");
    assert!(!l.permit().fenced);
    for state in ["suspect", "failed", "returning", "draining"] {
        set_state(&store, state).await;
        l.tick(true).await.expect("tick");
        let expect_fenced = state != "draining";
        assert_eq!(l.permit().fenced, expect_fenced, "{state}");
    }
    set_state(&store, "joining").await;
    l.tick(true).await.expect("tick");
    assert!(!l.permit().fenced);
}

#[tokio::test(start_paused = true)]
async fn an_expired_lease_is_taken_again_with_a_new_epoch() {
    let (mem, store, clock) = store_with("active").await;
    let l = lease(&store, &clock, "c1");
    l.tick(true).await.expect("tick");
    mem.set_unavailable(true);
    tokio::time::sleep(Duration::from_secs(12)).await;
    let _ = l.tick(true).await;
    assert!(l.permit().fenced);
    mem.set_unavailable(false);
    // etcd expired the lease meanwhile: the key is gone.
    assert!(
        store
            .get(&lease_key(&dc()).expect("key"), Consistency::Linearizable)
            .await
            .expect("get")
            .is_none()
    );
    l.tick(true).await.expect("tick");
    assert_eq!(l.dc_epoch(), Some(6), "a new lease, a new epoch");
    assert!(!l.permit().fenced);
}

#[tokio::test(start_paused = true)]
async fn without_self_fencing_permits_stay_valid() {
    let (mem, store, clock) = store_with("active").await;
    let opts = DcLeaseOpts {
        self_fence: false,
        ..DcLeaseOpts::default()
    };
    let l = DcLease::new(dc(), "c1", Arc::clone(&store), Arc::clone(&clock), opts);
    l.tick(true).await.expect("tick");
    mem.set_unavailable(true);
    tokio::time::sleep(Duration::from_secs(30)).await;
    let _ = l.tick(true).await;
    assert!(!l.permit().fenced);
}
