#![allow(clippy::unwrap_used)]

use std::sync::Mutex;

use sprig_common::ids::{DcId, ShardId};
use sprig_meta::{MemStore, MetaStore, Tier};
use sprig_shard::ShardMap;

use super::*;

const T: TenantId = TenantId(3_930_650_001);

fn c(n: &str) -> ClusterId {
    ClusterId::new(n).unwrap()
}

/// Schemas per cluster, and every question asked.
#[derive(Default)]
struct FakeSchemas {
    names: Vec<(ClusterId, String)>,
    asked: Mutex<Vec<ClusterId>>,
}

#[async_trait::async_trait]
impl GroupSchemas for FakeSchemas {
    async fn group_schemas(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        db: DbId,
        group: GroupId,
    ) -> Result<Vec<String>> {
        self.asked.lock().unwrap().push(cluster.clone());
        Ok(group_schemas_of(
            tenant,
            db,
            group,
            self.names
                .iter()
                .filter(|(c, _)| c == cluster)
                .map(|(_, n)| n.clone()),
        ))
    }
}

fn store() -> Arc<dyn MetaStore> {
    Arc::new(MemStore::new(Tier::Dc(DcId::new("mum1").unwrap())))
}

fn locator(s: &Arc<dyn MetaStore>, schemas: Arc<FakeSchemas>) -> MapLocator {
    MapLocator::new(
        ShardMapStore::new(Arc::clone(s)),
        TenantMaps::new(Arc::clone(s)),
        schemas,
    )
}

fn anchor(db: u32, group: u32, hash_point: u64) -> AnchorRef {
    AnchorRef {
        db: DbId(db),
        group: GroupId(group),
        hash_point,
    }
}

/// The tenant map (group 0) on `cluster`.
async fn tenant_map(s: &Arc<dyn MetaStore>, cluster: &ClusterId) {
    let m = ShardMap::presplit(T, GroupId(0), 0, 1, std::slice::from_ref(cluster)).unwrap();
    ShardMapStore::new(Arc::clone(s)).create(&m).await.unwrap();
}

#[tokio::test]
async fn a_sharded_groups_anchor_follows_its_hash_point_after_a_split() {
    let s = store();
    let maps = ShardMapStore::new(Arc::clone(&s));
    let mut m = ShardMap::presplit(T, GroupId(7), 99, 1, &[c("mum1-c1")]).unwrap();
    maps.create(&m).await.unwrap();
    let l = locator(&s, Arc::new(FakeSchemas::default()));
    let home = anchor(2, 7, 0);
    assert_eq!(
        l.locate(T, home).await.unwrap(),
        (c("mum1-c1"), "t3930650001_d2_g7_s1".to_owned())
    );
    // Split into s2 [0, 2^63) on c1 and s3 [2^63, 2^64) on c2.
    let (_, rev) = maps.load(T, GroupId(7)).await.unwrap();
    let mut lo = m.ranges[0].clone();
    lo.shard = ShardId(2);
    lo.epoch = 2;
    let mut hi = lo.clone();
    hi.lo = 1 << 63;
    hi.shard = ShardId(3);
    hi.cluster = c("mum1-c2");
    m.ranges = vec![lo, hi];
    m.next_shard_id = 4;
    m.version += 1;
    maps.compare_and_put(rev, &m).await.unwrap().unwrap();
    assert_eq!(
        l.locate(T, home).await.unwrap(),
        (c("mum1-c1"), "t3930650001_d2_g7_s2".to_owned())
    );
    assert_eq!(
        l.locate(T, anchor(2, 7, (1 << 63) + 5)).await.unwrap(),
        (c("mum1-c2"), "t3930650001_d2_g7_s3".to_owned())
    );
}

#[tokio::test]
async fn a_default_groups_anchor_is_the_one_schema_on_the_tenants_cluster() {
    let s = store();
    tenant_map(&s, &c("mum1-c4")).await;
    let schemas = Arc::new(FakeSchemas {
        names: vec![
            (c("mum1-c4"), "t3930650001_d1_g1_s5".to_owned()),
            (c("mum1-c4"), "t3930650001_d1_g10_s1".to_owned()),
            (c("mum1-c4"), "t39306500011_d1_g1_s1".to_owned()),
            (c("mum1-c4"), "t3930650001_d1_g1_s5x".to_owned()),
            (c("mum1-c9"), "t3930650001_d1_g1_s6".to_owned()),
        ],
        ..FakeSchemas::default()
    });
    let l = locator(&s, Arc::clone(&schemas));
    assert_eq!(
        l.locate(T, anchor(1, 1, 0)).await.unwrap(),
        (c("mum1-c4"), "t3930650001_d1_g1_s5".to_owned())
    );
    assert_eq!(*schemas.asked.lock().unwrap(), vec![c("mum1-c4")]);
}

#[tokio::test]
async fn anything_ambiguous_is_an_error_not_a_guess() {
    let s = store();
    let l = locator(&s, Arc::new(FakeSchemas::default()));
    // Group 0, and a tenant without a map.
    let e = l.locate(T, anchor(0, 0, 0)).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ShardMapUnavailable), "{e}");
    let e = l.locate(T, anchor(1, 1, 0)).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::TenantNotReady), "{e}");
    // No schema, then two schemas, of the default group.
    tenant_map(&s, &c("mum1-c4")).await;
    let e = l.locate(T, anchor(1, 1, 0)).await.unwrap_err();
    assert!(e.to_string().contains("no schema of group 1"), "{e}");
    let two = Arc::new(FakeSchemas {
        names: vec![
            (c("mum1-c4"), "t3930650001_d1_g1_s1".to_owned()),
            (c("mum1-c4"), "t3930650001_d1_g1_s2".to_owned()),
        ],
        ..FakeSchemas::default()
    });
    let e = locator(&s, two)
        .locate(T, anchor(1, 1, 0))
        .await
        .unwrap_err();
    assert!(e.to_string().contains("2 schemas"), "{e}");
}

#[test]
fn group_schemas_keep_exact_matches_only() {
    let got = group_schemas_of(
        TenantId(4),
        DbId(1),
        GroupId(2),
        [
            "t4_d1_g2_s9",
            "t4_d1_g2_s1",
            "t4_d1_g22_s1",
            "t4_d11_g2_s1",
            "t44_d1_g2_s1",
            "t4_d1_g2_s1_x",
            "t4_sprig",
        ]
        .map(String::from),
    );
    assert_eq!(got, ["t4_d1_g2_s1", "t4_d1_g2_s9"]);
}
