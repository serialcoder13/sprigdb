#![allow(clippy::unwrap_used, clippy::many_single_char_names)]

use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_cluster::record::{Capacity, NodeSpec, ProvisionerKind, SecretRefs, TopologyKind};
use sprig_cluster::registry::Registration;
use sprig_common::ids::NodeId;
use sprig_common::time::SystemClock;
use sprig_meta::bootstrap::DcRecord;
use sprig_meta::{MemStore, Op, Record, Txn};
use sprig_shard::map;

use super::*;

const T: TenantId = TenantId(7);

fn dc(s: &str) -> DcId {
    DcId::new(s).unwrap()
}

struct Bed {
    store: Arc<dyn MetaStore>,
    mum1: DcMoves,
    fra1: DcMoves,
    target: ClusterId,
}

/// Single-etcd mode: one store; tenant 7 (`acme`) homed in mum1 on
/// mum1-c1; fra1 registered with an active cluster.
async fn bed() -> Bed {
    let store: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc("mum1"))));
    let global = TierView::over(&store, Tier::Global);
    let fra1_tier = TierView::over(&store, Tier::Dc(dc("fra1")));
    let reg = ClusterRegistry::new(Arc::clone(&global), Arc::clone(&fra1_tier)).unwrap();
    let c = reg
        .register(Registration {
            name: "fra1-galera".into(),
            topology: TopologyKind::Galera,
            provisioner: ProvisionerKind::External,
            nodes: vec![NodeSpec {
                node: NodeId::new("n1").unwrap(),
                addr: "10.10.2.21:3306".into(),
                zone: None,
            }],
            secrets: SecretRefs {
                ctl: "file:/x".into(),
                repl: "file:/x".into(),
            },
            capacity: Capacity::default(),
            declared: None,
        })
        .await
        .unwrap();
    let target = c.body().cluster_id.clone();
    reg.set_admin_state(&target, AdminState::Active, Vec::new())
        .await
        .unwrap();
    let mut puts = vec![];
    for (d, state) in [("mum1", "active"), ("fra1", "joining")] {
        puts.push(Op::Put {
            key: Tier::Global.key(&format!("dcs/{d}")).unwrap(),
            value: Record::new(DcRecord {
                dc_id: dc(d),
                kind: "full".into(),
                state: state.into(),
                dc_epoch: 0,
                tags: serde_json::Map::new(),
                global_voter: false,
            })
            .encode()
            .unwrap(),
            lease: None,
        });
    }
    puts.push(Op::Put {
        key: ShardMap::catalog_key(&Tier::Dc(dc("mum1")), T).unwrap(),
        value: Record::new(ShardMap::catalog(T, ClusterId::new("mum1-c1").unwrap(), 1))
            .encode()
            .unwrap(),
        lease: None,
    });
    puts.push(Op::Put {
        key: home::directory_key(T).unwrap(),
        value: serde_json::to_vec(&json!({"v": 1, "tenant_id": 7, "name": "acme",
            "state": "active", "home_dc": "mum1", "epoch": 1}))
        .unwrap(),
        lease: None,
    });
    store
        .txn(Txn {
            when: vec![],
            then: puts,
            otherwise: vec![],
        })
        .await
        .unwrap();
    let mk = |d: &str| {
        DcMoves::new(
            dc(d),
            TierView::over(&store, Tier::Dc(dc(d))),
            Arc::clone(&global),
            Arc::new(SystemClock),
            None,
        )
    };
    Bed {
        mum1: mk("mum1"),
        fra1: mk("fra1"),
        store,
        target,
    }
}

fn to(d: &str) -> MoveTenantRequest {
    MoveTenantRequest {
        to: d.to_owned(),
        ..MoveTenantRequest::default()
    }
}

#[tokio::test]
async fn a_move_marks_the_tenant_and_submits_a_job_in_the_target_dc() {
    let b = bed().await;
    // A dry run changes nothing.
    let mut dry = to("fra1");
    dry.dry_run = true;
    let v = b.mum1.start(T, "acme", &dry).await.unwrap();
    assert!(v.dry_run);
    assert_eq!(
        v.migration.unwrap()["target_cluster"],
        json!(b.target.as_str())
    );
    assert_eq!(b.mum1.status(T, "acme").await.unwrap().state, "active");

    let v = b.mum1.start(T, "acme", &to("fra1")).await.unwrap();
    assert_eq!(v.state, "migrating");
    let m = v.migration.unwrap();
    assert_eq!(m["max_pause_ms"], json!(5000));
    let job = v.job.expect("the job is visible through the view");
    assert_eq!(job.target.as_ref(), Some(&b.target));
    // The job lives in fra1's tier, with the budget as its hold.
    let jobs = b
        .fra1
        .mover_in(&dc("fra1"))
        .list(&JobFilter {
            tenant: Some(T),
            active_only: true,
        })
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);
    // A second move is refused while this one runs.
    let e = b.mum1.start(T, "acme", &to("fra1")).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::TenantMoveRefused));

    // The commit (what the job's cutover does), then the new home's loop
    // counts the move once.
    let holder = format!("{}#1", job.id.0);
    home::maps_in(&b.store, &dc("mum1"))
        .update(T, |m| {
            map::hold(m, &holder, u64::MAX)?;
            Ok(true)
        })
        .await
        .unwrap();
    let c = home::commit(
        &b.store,
        &home::HomeChange {
            tenant: T,
            from: dc("mum1"),
            to: dc("fra1"),
            cluster: b.target.clone(),
            holder,
            now_ms: 5_000_000_000_000,
        },
    )
    .await
    .unwrap();
    assert_eq!(c, home::HomeCommit::Committed(2));
    assert_eq!(b.fra1.reconcile_tenant(T).await.unwrap(), Outcome::Done);
    let v = b.fra1.status(T, "acme").await.unwrap();
    assert_eq!((v.state.as_str(), v.home_dc.as_str()), ("active", "fra1"));
    assert_eq!(v.last_move.as_ref().unwrap()["observed"], json!(true));
    assert!(v.duration_ms.is_some());
    let (y, _) = home::maps_in(&b.store, &dc("fra1")).load(T).await.unwrap();
    assert_eq!(y.role, MapRole::Home);
    // From mum1 the tenant is now someone else's.
    let e = b.mum1.start(T, "acme", &to("fra1")).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::TenantMoveRefused));
}

#[tokio::test]
async fn moves_are_refused_when_they_cant_work() {
    let b = bed().await;
    let code = |r: Result<MoveView>| r.unwrap_err().code();
    assert_eq!(
        code(b.mum1.start(T, "acme", &to("mum1")).await),
        Some(SprigCode::TenantMoveRefused)
    );
    assert_eq!(
        code(b.mum1.start(T, "acme", &to("sin1")).await),
        Some(SprigCode::UnknownDc)
    );
    // Asked of a controller outside the home DC.
    assert_eq!(
        code(b.fra1.start(T, "acme", &to("fra1")).await),
        Some(SprigCode::TenantMoveRefused)
    );
    let mut bad = to("fra1");
    bad.cluster = Some("fra1-c99".into());
    assert_eq!(
        code(b.mum1.start(T, "acme", &bad).await),
        Some(SprigCode::TenantMoveRefused)
    );
    // Pinned in the directory.
    home::update_home(&*TierView::over(&b.store, Tier::Global), T, |e| {
        e.raw.insert("pinned_dc".into(), json!("mum1"));
        Ok(true)
    })
    .await
    .unwrap();
    assert_eq!(
        code(b.mum1.start(T, "acme", &to("fra1")).await),
        Some(SprigCode::TenantMoveRefused)
    );
}

#[tokio::test]
async fn a_move_without_a_job_goes_back_to_active() {
    let b = bed().await;
    // A start whose job submission was lost long ago.
    home::update_home(&*TierView::over(&b.store, Tier::Global), T, |e| {
        e.raw.insert("state".into(), json!("migrating"));
        e.raw.insert(
            "migration".into(),
            json!({"id": "m1", "from": "mum1", "to": "fra1", "started_ms": 1, "job": null}),
        );
        Ok(true)
    })
    .await
    .unwrap();
    assert_eq!(b.mum1.reconcile_tenant(T).await.unwrap(), Outcome::Done);
    let v = b.mum1.status(T, "acme").await.unwrap();
    assert_eq!(v.state, "active");
    assert!(v.migration.is_none());
    assert!(v.last_error.is_some());
}
