#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use std::time::Duration;

use proptest::prelude::*;
use sprig_common::ids::DcId;
use sprig_meta::{MemStore, Tier};

use super::*;
use crate::key::KeyClass;

fn c(n: &str) -> ClusterId {
    ClusterId::new(n).unwrap()
}

#[test]
fn presplit_matches_the_worked_example() {
    let spec = KeySpec::new(vec![KeyClass::Numeric]).unwrap();
    let clusters = [c("c-mum1-a"), c("c-mum1-b"), c("c-mum1-a"), c("c-mum1-c")];
    let m = ShardMap::presplit(TenantId(42), GroupId(3), spec.digest, 4, &clusters).unwrap();
    let los: Vec<u64> = m.ranges.iter().map(|r| r.lo).collect();
    assert_eq!(
        los,
        [
            0,
            0x4000_0000_0000_0000,
            0x8000_0000_0000_0000,
            0xc000_0000_0000_0000
        ]
    );
    let route = |i: i128| {
        route_key(&m, &spec, &[KeyValue::Int(i)])
            .unwrap()
            .range
            .shard
            .0
    };
    // Four ranges, shards 1..4.
    assert_eq!(route(42), 1);
    assert_eq!(route(7), 2);
    assert_eq!(route(1002), 3);
    assert_eq!(route(1001), 4);
    let groups = route_in_list(
        &m,
        &spec,
        &[
            vec![KeyValue::Int(42)],
            vec![KeyValue::Int(7)],
            vec![KeyValue::Int(42)],
        ],
    )
    .unwrap();
    assert_eq!(groups[&ShardId(1)], vec![0, 2]);
    assert_eq!(m.hi(3), None);
    // Round trip through the etcd record.
    assert_eq!(ShardMap::from_record(&m.to_record()).unwrap(), m);
    // A stale spec is refused.
    let other = KeySpec::new(vec![KeyClass::Binary]).unwrap();
    assert!(route_key(&m, &other, &[KeyValue::Bytes(b"x")]).is_err());
}

#[test]
fn validate_refuses_bad_maps() {
    let base = ShardMap::presplit(TenantId(1), GroupId(5), 0, 3, &[c("a-c1")]).unwrap();
    let mut m = base.clone();
    m.ranges[0].lo = 1;
    assert!(m.validate().is_err());
    let mut m = base.clone();
    m.ranges[2].lo = m.ranges[1].lo;
    assert!(m.validate().is_err());
    let mut m = base.clone();
    m.ranges[1].shard = m.ranges[0].shard;
    assert!(m.validate().is_err());
    let mut m = base.clone();
    m.next_shard_id = 2;
    assert!(m.validate().is_err());
    let mut m = base.clone();
    m.ranges[1].state = RangeState::Held;
    assert!(m.validate().is_err());
    let mut m = base;
    m.ranges.clear();
    assert!(m.validate().is_err());
}

proptest! {
    #[test]
    fn route_matches_a_linear_scan(n in 1u32..64, hs in proptest::collection::vec(any::<u64>(), 1..50)) {
        let m = ShardMap::presplit(TenantId(1), GroupId(2), 0, n, &[c("a-c1"), c("a-c2")]).unwrap();
        for h in hs {
            let linear = m.ranges.iter().rposition(|r| r.lo <= h).unwrap();
            prop_assert_eq!(m.range_of(RoutingHash(h)), linear);
            let hi = m.hi(linear).map_or(u128::from(u64::MAX) + 1, u128::from);
            prop_assert!(u128::from(h) < hi);
        }
    }
}

#[tokio::test]
async fn store_and_cache() {
    let tier = Tier::Dc(DcId::new("dc-a").unwrap());
    let s: Arc<dyn MetaStore> = Arc::new(MemStore::new(tier));
    let store = ShardMapStore::new(Arc::clone(&s));
    let cache = ShardMapCache::spawn(Arc::clone(&s)).unwrap();
    cache.ready().await;
    assert_eq!(
        cache.get(TenantId(9), GroupId(1)).unwrap_err().code(),
        Some(SprigCode::ShardMapUnavailable)
    );
    let m = ShardMap::presplit(TenantId(9), GroupId(1), 7, 2, &[c("a-c1"), c("a-c2")]).unwrap();
    let rev = store.create(&m).await.unwrap();
    assert!(store.create(&m).await.is_err());
    let (got, r2) = store.load(TenantId(9), GroupId(1)).await.unwrap();
    assert_eq!((got.clone(), r2), (m.clone(), rev));
    tokio::time::timeout(
        Duration::from_secs(5),
        cache.wait_version(TenantId(9), GroupId(1), 1),
    )
    .await
    .unwrap();
    assert_eq!(*cache.get(TenantId(9), GroupId(1)).unwrap(), m);
    let mut m2 = got;
    m2.ranges[1].cluster = c("a-c3");
    m2.ranges[1].epoch += 1;
    m2.version += 1;
    assert!(store.compare_and_put(rev, &m2).await.unwrap().is_some());
    assert!(store.compare_and_put(rev, &m2).await.unwrap().is_none());
    tokio::time::timeout(
        Duration::from_secs(5),
        cache.wait_version(TenantId(9), GroupId(1), 2),
    )
    .await
    .unwrap();
    assert_eq!(
        cache.get(TenantId(9), GroupId(1)).unwrap().ranges[1].cluster,
        c("a-c3")
    );
    assert_eq!(store.list(TenantId(9)).await.unwrap().len(), 1);
    store.delete(TenantId(9), GroupId(1)).await.unwrap();
    assert!(store.load(TenantId(9), GroupId(1)).await.is_err());
}
