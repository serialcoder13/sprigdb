#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use sprig_meta::MemStore;

use super::*;

fn dc(s: &str) -> DcId {
    DcId::new(s).unwrap()
}

fn c(s: &str) -> ClusterId {
    ClusterId::new(s).unwrap()
}

const T: TenantId = TenantId(42);

/// One store for every tier (single-etcd mode): tenant 42 homed in mum1 on
/// mum1-c1, migrating to fra1.
async fn setup(state: &str) -> Arc<dyn MetaStore> {
    let store: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc("mum1"))));
    let key = ShardMap::catalog_key(&Tier::Dc(dc("mum1")), T).unwrap();
    let m = ShardMap::catalog(T, c("mum1-c1"), 3);
    let entry = json!({"v": 1, "tenant_id": 42, "name": "dc_a", "state": state,
        "home_dc": "mum1", "epoch": 3, "catalog_version": 7, "extra": [1],
        "migration": {"to": "fra1", "id": "m1"}});
    store
        .txn(Txn {
            when: vec![],
            then: vec![
                Op::Put {
                    key,
                    value: Record::new(m).encode().unwrap(),
                    lease: None,
                },
                Op::Put {
                    key: directory_key(T).unwrap(),
                    value: serde_json::to_vec(&entry).unwrap(),
                    lease: None,
                },
            ],
            otherwise: vec![],
        })
        .await
        .unwrap();
    store
}

fn change() -> HomeChange {
    HomeChange {
        tenant: T,
        from: dc("mum1"),
        to: dc("fra1"),
        cluster: c("fra1-c1"),
        holder: "job-1#2".to_owned(),
        now_ms: 1000,
    }
}

async fn hold(store: &Arc<dyn MetaStore>, holder: &str) {
    maps_in(store, &dc("mum1"))
        .update(T, |m| {
            map::hold(m, holder, u64::MAX)?;
            Ok(true)
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn commit_moves_maps_and_directory_together() {
    let store = setup("migrating").await;
    hold(&store, "job-1#2").await;
    assert_eq!(
        commit(&store, &change()).await.unwrap(),
        HomeCommit::Committed(4)
    );
    let (x, _) = maps_in(&store, &dc("mum1")).load(T).await.unwrap();
    let (y, _) = maps_in(&store, &dc("fra1")).load(T).await.unwrap();
    assert_eq!(x.role, MapRole::Quarantined);
    assert_eq!(y.role, MapRole::Home);
    for m in [&x, &y] {
        let p = Placement::of(m).unwrap();
        assert_eq!((p.cluster, p.epoch), (c("fra1-c1"), 2));
        assert!(p.hold.is_none());
        assert_eq!(m.tenant_epoch, 4);
    }
    let d = read_home(&*store, T).await.unwrap().unwrap();
    assert_eq!(
        (d.home_dc.as_str(), d.epoch, d.state.as_str()),
        ("fra1", 4, "active")
    );
    assert!(d.migration.is_none());
    assert_eq!(d.raw["extra"], json!([1]));
    assert_eq!(d.raw["catalog_version"], json!(7));
    assert_eq!(d.last_move.as_ref().unwrap()["moves_back"], json!(0));
    // A retry after a lost reply sees its own commit.
    assert_eq!(
        commit(&store, &change()).await.unwrap(),
        HomeCommit::Committed(4)
    );
}

#[tokio::test]
async fn commit_refuses_without_the_hold_or_the_migration() {
    let store = setup("migrating").await;
    // Not held.
    assert!(matches!(
        commit(&store, &change()).await.unwrap(),
        HomeCommit::Rejected(_)
    ));
    // Held by an older owner of the job: the flip needs the exact holder.
    hold(&store, "job-1#1").await;
    assert!(matches!(
        commit(&store, &change()).await.unwrap(),
        HomeCommit::Rejected(_)
    ));
    // Not migrating.
    let store = setup("active").await;
    hold(&store, "job-1#2").await;
    assert!(matches!(
        commit(&store, &change()).await.unwrap(),
        HomeCommit::Rejected(_)
    ));
    assert_eq!(
        read_home(&*store, T)
            .await
            .unwrap()
            .unwrap()
            .home_dc
            .as_str(),
        "mum1"
    );
}

#[tokio::test]
async fn moving_back_overwrites_the_quarantined_map_and_counts() {
    let store = setup("migrating").await;
    hold(&store, "job-1#2").await;
    commit(&store, &change()).await.unwrap();
    // Back to mum1.
    update_home(&*store, T, |e| {
        e.raw.insert("state".into(), json!("migrating"));
        e.raw.insert("migration".into(), json!({"to": "mum1"}));
        Ok(true)
    })
    .await
    .unwrap();
    maps_in(&store, &dc("fra1"))
        .update(T, |m| {
            map::hold(m, "job-2#1", u64::MAX)?;
            Ok(true)
        })
        .await
        .unwrap();
    let back = HomeChange {
        tenant: T,
        from: dc("fra1"),
        to: dc("mum1"),
        cluster: c("mum1-c2"),
        holder: "job-2#1".to_owned(),
        now_ms: 2000,
    };
    assert_eq!(
        commit(&store, &back).await.unwrap(),
        HomeCommit::Committed(5)
    );
    let (x, _) = maps_in(&store, &dc("mum1")).load(T).await.unwrap();
    assert_eq!(x.role, MapRole::Home);
    assert_eq!(Placement::of(&x).unwrap().cluster, c("mum1-c2"));
    let d = read_home(&*store, T).await.unwrap().unwrap();
    assert_eq!(d.last_move.unwrap()["moves_back"], json!(1));
}
