#![allow(clippy::unwrap_used, clippy::float_cmp)]

use sprig_meta::MemStore;

use super::*;
use crate::plan::{ApprovalNeeded, Trigger};
use crate::settings::GB;
use crate::testutil::{cid, dc, fleet, load, put};

fn report(stuck: Vec<ClusterId>, approvals: Vec<ApprovalNeeded>) -> PlanReport {
    PlanReport {
        at: 0,
        fullness: BTreeMap::new(),
        spread: 0.0,
        trigger: Trigger::Hot,
        hot: Vec::new(),
        warming: Vec::new(),
        moves: Vec::new(),
        notes: Vec::new(),
        approvals,
        stuck,
    }
}

#[test]
fn stuck_for_an_hour_opens_add_cluster_or_grow_storage() {
    let mut v = fleet(2);
    put(&mut v, 1, 1, 1, load(10.0, 900.0)); // busy is the bottleneck
    put(&mut v, 2, 2, 2, load(95.0, 0.0)); // storage is
    let mut b = RecommendationBook::new();
    let r = report(vec![cid(1), cid(2)], Vec::new());
    assert!(b.observe(&v, &r, 0).is_empty());
    assert!(b.observe(&v, &r, 3599).is_empty());
    let ch = b.observe(&v, &r, 3600);
    assert_eq!(ch.len(), 2);
    let kinds: Vec<RecKind> = b.open().iter().map(|r| r.kind).collect();
    assert_eq!(kinds, vec![RecKind::AddCluster, RecKind::GrowStorage]);
    let add = b.open()[0].clone();
    assert_eq!(add.rec_id, "add_cluster-mum1-c1");
    assert_eq!(add.first_seen, 0);
    assert_eq!(add.tenants, vec![TenantId(1)]);
    // Busy 900 of 1000 with target 0.70: absorb 2/9 of the load, plus 30%.
    assert!((add.need.busy_ms_per_sec - 900.0 * (0.2 / 0.9) * 1.3).abs() < 1e-6);
    // Unchanged: nothing new to write.
    assert!(b.observe(&v, &r, 3660).is_empty());
    // Cleared: closed.
    let ch = b.observe(&v, &report(Vec::new(), Vec::new()), 4000);
    assert_eq!(ch.len(), 2);
    assert!(matches!(&ch[0], RecChange::Closed(r) if r.state == RecState::Closed));
    assert!(b.open().is_empty());
}

#[test]
fn approvals_and_placement_failures() {
    let v = fleet(2);
    let mut b = RecommendationBook::new();
    let a = ApprovalNeeded {
        shard: crate::model::ShardKey::new(7, 1, 3),
        size_bytes: 300.0 * GB,
        from: cid(1),
        to: cid(2),
        gain: 0.2,
    };
    let ch = b.observe(&v, &report(Vec::new(), vec![a]), 10);
    assert_eq!(ch.len(), 1);
    assert_eq!(b.open()[0].kind, RecKind::ApproveMove);
    assert_eq!(b.open()[0].shard.as_deref(), Some("t7/g1/s3 to mum1-c2"));
    let ch = b.placement_failed("mum1", TenantId(9), load(1.0, 0.0), true, vec![], 20);
    assert_eq!(ch.len(), 1);
    let r = b
        .open()
        .into_iter()
        .find(|r| r.kind == RecKind::AddCluster)
        .unwrap();
    assert_eq!(r.reason, "schema_cap");
    assert_eq!(r.cluster, "mum1");
    // Placement-time recommendations survive planning runs ...
    b.observe(&v, &report(Vec::new(), Vec::new()), 30);
    assert_eq!(b.counts().get(&RecKind::AddCluster), Some(&1));
    // ... and close when a placement succeeds.
    assert_eq!(b.placed("mum1").len(), 1);
    assert!(b.counts().is_empty());
}

#[tokio::test]
async fn store_round_trips_in_the_dc_tier() {
    let tier = Tier::Dc(dc("mum1"));
    let mem: Arc<dyn MetaStore> = Arc::new(MemStore::new(tier.clone()));
    let st = RecommendationStore::new(mem, tier);
    let mut b = RecommendationBook::new();
    let ch = b.placement_failed(
        "mum1",
        TenantId(9),
        load(1.0, 0.0),
        false,
        vec!["mum1".into()],
        5,
    );
    st.apply(&ch).await.unwrap();
    let all = st.list().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].reason, "no_capacity");
    assert_eq!(all[0].need.residency, vec!["mum1".to_owned()]);
    st.apply(&b.placed("mum1")).await.unwrap();
    assert_eq!(st.list().await.unwrap()[0].state, RecState::Closed);
}
