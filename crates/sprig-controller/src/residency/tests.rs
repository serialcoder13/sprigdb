#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::time::SystemTime;

use serde_json::Map;
use sprig_common::time::SystemClock;
use sprig_meta::MemStore;

use super::*;
use crate::tree::{Directory, NewTenant, TenantState};

fn dc(s: &str) -> DcId {
    DcId::new(s).unwrap()
}

fn rec(id: &str, tags: Value) -> DcRecord {
    DcRecord {
        dc_id: dc(id),
        kind: "full".into(),
        state: "active".into(),
        dc_epoch: 1,
        tags: match tags {
            Value::Object(m) => m,
            _ => Map::new(),
        },
        global_voter: true,
    }
}

fn set(v: &[&str]) -> BTreeSet<DcId> {
    v.iter().map(|d| dc(d)).collect()
}

fn registry() -> Vec<DcRecord> {
    vec![
        rec("fra1", json!({"jurisdiction": "EU", "country": "DE"})),
        rec("ams1", json!({"jurisdiction": "EU", "country": "NL"})),
        rec("sin1", json!({"jurisdiction": "SG"})),
        rec("sin2", json!({"jurisdiction": "SG"})),
    ]
}

fn node(parent: Option<u64>, policy: Option<&str>) -> Node {
    Node {
        parent: parent.map(TenantId),
        policy: policy.map(|p| Policy::parse(p).unwrap()),
    }
}

#[test]
fn a_childs_set_is_within_its_ancestors() {
    let mut nodes = BTreeMap::new();
    nodes.insert(TenantId(1), node(None, Some(r#"jurisdiction = "EU""#)));
    nodes.insert(TenantId(2), node(Some(1), None));
    nodes.insert(TenantId(3), node(Some(2), Some(r#"country = "DE""#)));
    // A child asking for more than its ancestors allow gets the
    // intersection: here nothing.
    nodes.insert(TenantId(4), node(Some(1), Some(r#"jurisdiction = "SG""#)));
    nodes.insert(TenantId(5), node(None, None));
    nodes.insert(TenantId(6), node(Some(5), Some("")));
    let r = resolve_tree(&nodes, &registry());
    assert_eq!(r[&TenantId(1)], Some(set(&["ams1", "fra1"])));
    assert_eq!(r[&TenantId(2)], Some(set(&["ams1", "fra1"])), "inherited");
    assert_eq!(r[&TenantId(3)], Some(set(&["fra1"])));
    assert_eq!(r[&TenantId(4)], Some(BTreeSet::new()));
    assert_eq!(r[&TenantId(5)], None, "no policy anywhere: unrestricted");
    assert_eq!(r[&TenantId(6)], None, "an empty policy adds nothing");
    // Property: every tenant's set is a subset of its parent's.
    for (t, n) in &nodes {
        if let (Some(p), Some(Some(mine))) = (n.parent, r.get(t))
            && let Some(Some(theirs)) = r.get(&p)
        {
            assert!(mine.is_subset(theirs), "{t} widens {p}");
        }
    }
}

#[test]
fn a_cycle_fails_closed() {
    let mut nodes = BTreeMap::new();
    nodes.insert(TenantId(1), node(Some(2), None));
    nodes.insert(TenantId(2), node(Some(1), None));
    let r = resolve_tree(&nodes, &registry());
    // Neither recursion nor an unrestricted answer for a broken tree.
    assert!(r.values().all(|v| v.as_ref().is_some_and(BTreeSet::is_empty)));
}

fn facts() -> Facts {
    Facts {
        tenant: TenantId(7),
        name: "acme".into(),
        home_dc: dc("sin1"),
        state: "active".into(),
        pinned_dc: None,
        standby_dcs: vec![dc("sin2"), dc("ams1")],
        migration_to: None,
        clusters: vec![ClusterFacts {
            id: ClusterId::new("sin1-c1").unwrap(),
            dcs: set(&["sin1"]),
        }],
        feed_subs: Vec::new(),
    }
}

#[test]
fn the_books_flow_plans_the_home_move_and_the_standby() {
    let eu = set(&["ams1", "fra1"]);
    let p = plan(&facts(), &eu, &[dc("fra1"), dc("ams1")]);
    assert_eq!(
        p.steps,
        vec![
            Step::MoveHome {
                from: dc("sin1"),
                to: dc("fra1")
            },
            Step::DropStandby(dc("sin2")),
        ]
    );
    assert!(p.blocking.is_empty());
    // No allowed DC has a cluster: blocked.
    let p = plan(&facts(), &eu, &[]);
    assert_eq!(p.blocking.len(), 1, "{p:?}");
    // Pinned outside: blocked, never moved.
    let mut f = facts();
    f.pinned_dc = Some(dc("sin1"));
    let p = plan(&f, &eu, &[dc("fra1")]);
    assert!(p.steps.iter().all(|s| !matches!(s, Step::MoveHome { .. })));
    assert!(p.blocking[0].contains("unpin"));
}

#[test]
fn clusters_with_copies_outside_and_moves_in_flight_are_planned() {
    let mut f = facts();
    f.home_dc = dc("fra1");
    f.standby_dcs = vec![dc("ams1")];
    f.clusters = vec![ClusterFacts {
        id: ClusterId::new("fra1-c1").unwrap(),
        dcs: set(&["fra1", "sin1"]),
    }];
    f.migration_to = Some(dc("sin2"));
    let p = plan(&f, &set(&["ams1", "fra1"]), &[dc("fra1")]);
    assert_eq!(
        p.steps,
        vec![
            Step::StopMove(dc("sin2")),
            Step::MoveOffCluster {
                cluster: ClusterId::new("fra1-c1").unwrap(),
                outside: set(&["sin1"]),
            },
        ]
    );
    // Compliant: nothing to do.
    f.clusters[0].dcs = set(&["fra1"]);
    f.migration_to = None;
    assert_eq!(plan(&f, &set(&["ams1", "fra1"]), &[]), Plan::default());
}

#[test]
fn a_missing_or_broken_block_fails_closed() {
    let mut raw = Map::new();
    assert_eq!(home::allowed_dcs(&raw), None);
    raw.insert("residency".into(), json!({"policy": "x"}));
    assert_eq!(home::allowed_dcs(&raw), Some(BTreeSet::new()));
    raw.insert("residency".into(), json!({"allowed_dcs": ["fra1", "BAD DC"]}));
    assert_eq!(home::allowed_dcs(&raw), Some(set(&["fra1"])));
    raw.insert("residency".into(), json!(42));
    assert_eq!(home::allowed_dcs(&raw), Some(BTreeSet::new()));
    let b = ResidencyBlock::of(&raw).unwrap();
    assert_eq!(b.state, "violated");
    assert!(b.allowed().is_empty());
}

async fn put(store: &dyn MetaStore, key: sprig_meta::Key, value: Vec<u8>) {
    store
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key,
                value,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await
        .unwrap();
}

struct World {
    mem: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    acme: TenantId,
    child: TenantId,
}

/// Four DCs (EU: fra1, ams1; SG: sin1, sin2), each with one active
/// cluster; `acme` homed in sin1 with a standby in sin2, and its child
/// `acme-eu` homed in fra1.
async fn world() -> World {
    let mem: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc("sin1"))));
    let global = TierView::over(&mem, Tier::Global);
    for r in registry() {
        put(
            &*global,
            sprig_meta::dclease::dc_key(&r.dc_id).unwrap(),
            Record::new(r.clone()).encode().unwrap(),
        )
        .await;
        let reg = ClusterRegistry::new(
            Arc::clone(&global),
            TierView::over(&mem, Tier::Dc(r.dc_id.clone())),
        )
        .unwrap();
        let c = reg
            .register(sprig_cluster::registry::Registration {
                name: "c1".into(),
                topology: sprig_cluster::record::TopologyKind::SingleNode,
                provisioner: sprig_cluster::record::ProvisionerKind::External,
                nodes: vec![sprig_cluster::record::NodeSpec {
                    node: sprig_common::ids::NodeId::new("n1").unwrap(),
                    addr: "127.0.0.1:1".into(),
                    zone: None,
                }],
                secrets: sprig_cluster::record::SecretRefs {
                    ctl: "file:/dev/null".into(),
                    repl: "file:/dev/null".into(),
                },
                capacity: sprig_cluster::record::Capacity::default(),
                declared: None,
            })
            .await
            .unwrap();
        reg.set_admin_state(&c.body().cluster_id, AdminState::Active, Vec::new())
            .await
            .unwrap();
    }
    let dir = Directory::new(Arc::clone(&global));
    let mk = |name: &str, parent: Option<TenantId>, home: &str| NewTenant {
        name: name.into(),
        parent,
        home_dc: dc(home),
        placement_hint: None,
    };
    let acme = dir
        .create(&mk("acme", None, "sin1"), 64, SystemTime::now())
        .await
        .unwrap()
        .body()
        .tenant_id;
    let activate = |t: TenantId| {
        let dir = dir.clone();
        async move {
            dir.update(t, |b| {
                b.state = TenantState::Active;
                Ok(true)
            })
            .await
            .unwrap();
        }
    };
    activate(acme).await;
    let child = dir
        .create(&mk("acme-eu", Some(acme), "fra1"), 64, SystemTime::now())
        .await
        .unwrap()
        .body()
        .tenant_id;
    activate(child).await;
    home::update_home(&*global, acme, |e| {
        e.raw.insert("standby_dcs".into(), json!(["sin2"]));
        Ok(true)
    })
    .await
    .unwrap();
    // acme's map in sin1, its standby map in sin2.
    for (d, role) in [("sin1", MapRole::Home), ("sin2", MapRole::Standby)] {
        let tier = TierView::over(&mem, Tier::Dc(dc(d)));
        let mut m = ShardMap::catalog(acme, ClusterId::new(&format!("{d}-c1")).unwrap(), 1);
        m.role = role;
        put(
            &*tier,
            ShardMap::catalog_key(tier.tier(), acme).unwrap(),
            Record::new(m).encode().unwrap(),
        )
        .await;
    }
    World {
        mem,
        global,
        acme,
        child,
    }
}

fn residency(w: &World, here: &str) -> Residency {
    Residency::single_etcd(dc(here), &w.mem, Arc::new(SystemClock), None)
}

#[tokio::test]
async fn set_policy_dry_run_then_apply() {
    let w = world().await;
    let r = residency(&w, "sin1");
    // Bad policy: 39506.
    let e = r
        .set(
            "acme",
            &ResidencyRequest {
                policy: Some("jurisdiction = EU".into()),
                ..ResidencyRequest::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ResidencyInvalid));
    // No DC: 39507.
    let e = r
        .set(
            "acme",
            &ResidencyRequest {
                policy: Some(r#"jurisdiction = "US""#.into()),
                ..ResidencyRequest::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ResidencyUnsatisfiable));
    // Dry run: the plan, nothing written.
    let eu = ResidencyRequest {
        policy: Some(r#"jurisdiction = "EU""#.into()),
        ..ResidencyRequest::default()
    };
    let v = r.set("acme", &eu).await.unwrap();
    assert!(!v.applied);
    assert_eq!(v.allowed_dcs, Some(vec![dc("ams1"), dc("fra1")]));
    let acme_plan = &v.plans.iter().find(|p| p.tenant == "acme").unwrap().plan;
    assert!(
        acme_plan[0].contains("from sin1 to ams1") || acme_plan[0].contains("from sin1 to fra1"),
        "{acme_plan:?}"
    );
    assert!(acme_plan.iter().any(|l| l.contains("drop the standby in sin2")));
    let child_plan = v.plans.iter().find(|p| p.tenant == "acme-eu").unwrap();
    assert!(child_plan.plan.is_empty(), "fra1 is allowed: {child_plan:?}");
    assert_eq!(r.show("acme").await.unwrap().state, "none");
    // Apply.
    let v = r
        .set(
            "acme",
            &ResidencyRequest {
                apply: true,
                ..eu.clone()
            },
        )
        .await
        .unwrap();
    assert!(v.applied);
    assert_eq!(v.state, "converging");
    let s = r.show("acme").await.unwrap();
    assert_eq!(s.policy, r#"jurisdiction = "EU""#);
    assert_eq!(s.state, "converging");
    // The child got the inherited set.
    let s = r.show("acme-eu").await.unwrap();
    assert_eq!(s.allowed_dcs, Some(vec![dc("ams1"), dc("fra1")]));
    assert_eq!(s.policy, "");
    // The child can't ask for Singapore now (only the intersection: none).
    let e = r
        .set(
            "acme-eu",
            &ResidencyRequest {
                policy: Some(r#"jurisdiction = "SG""#.into()),
                apply: true,
                ..ResidencyRequest::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ResidencyUnsatisfiable));
    assert!(e.message.contains("ancestor acme"), "{}", e.message);
    // Narrowing within the parent works.
    let v = r
        .set(
            "acme-eu",
            &ResidencyRequest {
                policy: Some(r#"country = "DE""#.into()),
                deny_remote_access: Some(true),
                apply: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(v.allowed_dcs, Some(vec![dc("fra1")]));
    assert_eq!(v.state, "compliant");
    let e = home::read_home(&*w.global, w.child).await.unwrap().unwrap();
    assert!(e.residency_allows(&dc("fra1")) && !e.residency_allows(&dc("ams1")));
    assert!(ResidencyBlock::of(&e.raw).unwrap().deny_remote_access);
}

#[tokio::test]
async fn the_loop_drops_the_standby_and_reports_what_is_left() {
    let w = world().await;
    let r = residency(&w, "sin1");
    r.set(
        "acme",
        &ResidencyRequest {
            policy: Some(r#"jurisdiction = "EU""#.into()),
            apply: true,
            ..ResidencyRequest::default()
        },
    )
    .await
    .unwrap();
    // Without a mover the home move can't start: violated, with why.
    let out = r.reconcile_tenant(w.acme).await.unwrap();
    assert_eq!(out, Outcome::RequeueAfter(RECHECK));
    let e = home::read_home(&*w.global, w.acme).await.unwrap().unwrap();
    assert!(standby_dcs(&e.raw).is_empty(), "sin2 dropped");
    let sin2 = TierView::over(&w.mem, Tier::Dc(dc("sin2")));
    let maps = sin2
        .list(
            &sin2.tier().key(&format!("shardmaps/{}/", w.acme)).unwrap(),
            ListOpts::default(),
        )
        .await
        .unwrap();
    assert!(maps.kvs.is_empty(), "the standby map in sin2 is gone");
    let b = ResidencyBlock::of(&e.raw).unwrap();
    assert_eq!(b.state, "converging");
    assert!(b.pending.iter().any(|p| p.contains("move the home")), "{b:?}");
    // Re-tagging sin1 into the EU makes acme compliant where it is.
    let mut sin1 = rec("sin1", json!({"jurisdiction": "EU"}));
    sin1.dc_epoch = 2;
    put(
        &*w.global,
        sprig_meta::dclease::dc_key(&dc("sin1")).unwrap(),
        Record::new(sin1).encode().unwrap(),
    )
    .await;
    let r = residency(&w, "sin1");
    assert_eq!(r.reconcile_tenant(w.acme).await.unwrap(), Outcome::Done);
    let e = home::read_home(&*w.global, w.acme).await.unwrap().unwrap();
    let b = ResidencyBlock::of(&e.raw).unwrap();
    assert_eq!(b.state, "compliant");
    assert_eq!(b.allowed(), set(&["ams1", "fra1", "sin1"]));
    // Removing the policy removes the child's inherited block.
    r.set(
        "acme",
        &ResidencyRequest {
            policy: Some(String::new()),
            apply: true,
            ..ResidencyRequest::default()
        },
    )
    .await
    .unwrap();
    let e = home::read_home(&*w.global, w.child).await.unwrap().unwrap();
    assert_eq!(e.allowed_dcs(), None);
}

#[test]
fn a_child_is_created_only_inside_its_parents_set() {
    let mut raw = Map::new();
    assert!(child_block("acme", &raw, &dc("sin1"), 1).unwrap().is_none());
    raw.insert(
        "residency".into(),
        json!({"policy": "x", "allowed_dcs": ["fra1", "ams1"]}),
    );
    let b = child_block("acme", &raw, &dc("fra1"), 1).unwrap().unwrap();
    assert!(b.policy.is_empty());
    assert_eq!(b.allowed(), set(&["ams1", "fra1"]));
    let e = child_block("acme", &raw, &dc("sin1"), 1).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ResidencyUnsatisfiable));
}

/// The commit points refuse a target outside the residency read in the
/// same transaction: a policy written after the decision (or the move's
/// start) still stops the home change.
#[tokio::test]
async fn commit_points_refuse_a_disallowed_target() {
    use crate::failover::promote::{Guard, NoHooks, Promoted, Promotion, promote};
    let w = world().await;
    // The decision was taken (acme sin1 -> sin2, epoch 1); then an operator
    // restricts acme to the EU.
    let e = home::read_home(&*w.global, w.acme).await.unwrap().unwrap();
    home::update_home(&*w.global, w.acme, |x| {
        x.raw.insert(
            "residency".into(),
            json!({"policy": "jurisdiction = \"EU\"", "allowed_dcs": ["ams1", "fra1"]}),
        );
        Ok(true)
    })
    .await
    .unwrap();
    let p = Promotion::single_etcd(&w.mem, Guard::Store(Arc::clone(&w.global)), Arc::new(NoHooks));
    let r = promote(&p, "f1", w.acme, &dc("sin1"), &dc("sin2"), e.epoch)
        .await
        .unwrap();
    assert!(
        matches!(&r, Promoted::Skipped(why) if why.contains("residency")),
        "{r:?}"
    );
    let after = home::read_home(&*w.global, w.acme).await.unwrap().unwrap();
    assert_eq!(after.home_dc, dc("sin1"), "never committed");
    // A DC move: the directory says migrating to sin2 (started before the
    // policy); its commit is rejected.
    home::update_home(&*w.global, w.acme, |x| {
        x.raw.insert("state".into(), json!("migrating"));
        x.raw.insert("migration".into(), json!({"to": "sin2"}));
        Ok(true)
    })
    .await
    .unwrap();
    let c = home::commit(
        &w.mem,
        &home::HomeChange {
            tenant: w.acme,
            from: dc("sin1"),
            to: dc("sin2"),
            cluster: ClusterId::new("sin2-c1").unwrap(),
            holder: "job#1".into(),
            now_ms: 1,
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(&c, home::HomeCommit::Rejected(why) if why.contains("residency")),
        "{c:?}"
    );
}

/// The move's start refuses a target outside the residency (39550), and a
/// standby or manual failover there (39505).
#[tokio::test]
async fn starts_refuse_a_disallowed_target() {
    let w = world().await;
    home::update_home(&*w.global, w.acme, |x| {
        x.raw.insert(
            "residency".into(),
            json!({"policy": "jurisdiction = \"SG\"", "allowed_dcs": ["sin1", "sin2"]}),
        );
        Ok(true)
    })
    .await
    .unwrap();
    let sin1: Arc<dyn MetaStore> = TierView::over(&w.mem, Tier::Dc(dc("sin1")));
    let moves = DcMoves::new(
        dc("sin1"),
        sin1,
        Arc::clone(&w.global),
        Arc::new(SystemClock),
        None,
    );
    let e = moves
        .start(
            w.acme,
            "acme",
            &MoveTenantRequest {
                to: "fra1".into(),
                dry_run: true,
                ..MoveTenantRequest::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::TenantMoveRefused));
    assert!(e.message.contains("residency"), "{}", e.message);
    let role = crate::failover::role::FailoverRole::new(
        dc("sin1"),
        "ctl-sin1".into(),
        crate::runtime::Stores {
            dc: Arc::clone(&w.mem),
            global: Arc::clone(&w.global),
        },
        Arc::new(SystemClock),
        None,
        true,
    );
    let e = role
        .add_standby(
            "acme",
            &crate::failover::manual::StandbyRequest {
                dc: dc("fra1"),
                cluster: ClusterId::new("fra1-c1").unwrap(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::FailoverRefused));
    assert!(e.message.contains("residency"), "{}", e.message);
    let e = role
        .failover_tenant(
            "acme",
            &crate::failover::manual::FailoverRequest {
                to: dc("fra1"),
                accept_data_loss: true,
                old_home_fenced: true,
                dry_run: true,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::FailoverRefused));
    assert!(e.message.contains("residency"), "{}", e.message);
}

/// The change feed: a subscription can't be registered in a DC tier the
/// tenant's residency excludes, and the loop pauses one made before the
/// policy.
#[tokio::test]
async fn feed_subscriptions_stay_inside_the_residency() {
    use sprig_feed::subs::{Subscription, Subscriptions};
    let w = world().await;
    let sub = |name: &str| Subscription {
        tenant: w.acme,
        name: name.into(),
        kind: "logic".into(),
        ordering: "merged".into(),
        owner_ref: None,
        state: "active".into(),
        created_at_ms: 1,
    };
    let sin2 = Subscriptions::new(TierView::over(&w.mem, Tier::Dc(dc("sin2"))));
    let fra1 = Subscriptions::new(TierView::over(&w.mem, Tier::Dc(dc("fra1"))));
    // Before any policy: anywhere.
    fra1.register(&sub("logic:b1")).await.unwrap();
    sin2.register(&sub("logic:b2")).await.unwrap();
    let r = residency(&w, "sin1");
    r.set(
        "acme",
        &ResidencyRequest {
            policy: Some(r#"jurisdiction = "SG""#.into()),
            apply: true,
            ..ResidencyRequest::default()
        },
    )
    .await
    .unwrap();
    let e = fra1.register(&sub("logic:b3")).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ResidencyUnsatisfiable));
    sin2.register(&sub("logic:b4")).await.unwrap();
    // The loop pauses fra1's old one; sin2's stay active.
    assert_eq!(r.reconcile_tenant(w.acme).await.unwrap(), Outcome::Done);
    assert_eq!(fra1.get(w.acme, "logic:b1").await.unwrap().state, "paused");
    assert_eq!(sin2.get(w.acme, "logic:b2").await.unwrap().state, "active");
    let b = ResidencyBlock::of(&home::read_home(&*w.global, w.acme).await.unwrap().unwrap().raw)
        .unwrap();
    assert_eq!(b.state, "compliant", "{b:?}");
}
