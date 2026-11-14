use std::collections::BTreeMap;
use std::sync::Mutex;

use serde_json::{Map, json};
use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_common::ids::ClusterId;
use sprig_meta::session::TokioClock;
use sprig_meta::{Key, MemStore, TierView, Txn};

use super::*;
use crate::failover::promote::{NoHooks, Promotion};
use crate::failover::record::{StandbyLag, VerdictKind};

fn dc(s: &str) -> DcId {
    DcId::new(s).expect("dc")
}

/// Verdicts by observing site; a site missing from the map doesn't answer.
struct Scripted {
    verdicts: Mutex<BTreeMap<String, VerdictKind>>,
    lag_ms: u32,
    asked: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl ObserverClient for Scripted {
    async fn observe(&self, site: &DcId, _x: &DcId, _id: &str) -> Result<Verdict> {
        self.asked.lock().expect("lock").push(site.to_string());
        let v = self
            .verdicts
            .lock()
            .expect("lock")
            .get(site.as_str())
            .copied()
            .ok_or_else(|| SprigError::internal("no answer"))?;
        Ok(Verdict {
            observer_dc: site.clone(),
            observer_lease_valid: true,
            verdict: v,
            unsafe_writers: vec![],
            lags: if site.as_str() == "ams1" {
                vec![
                    StandbyLag {
                        tenant_id: 42,
                        lag_ms: self.lag_ms,
                    },
                    StandbyLag {
                        tenant_id: 77,
                        lag_ms: 9200,
                    },
                ]
            } else {
                vec![]
            },
        })
    }
}

struct World {
    store: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    leases: BTreeMap<String, sprig_meta::LeaseId>,
}

async fn put(
    store: &Arc<dyn MetaStore>,
    key: Key,
    value: Vec<u8>,
    lease: Option<sprig_meta::LeaseId>,
) {
    let r = store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put { key, value, lease }],
            otherwise: vec![],
        })
        .await
        .expect("put");
    assert!(r.succeeded);
}

async fn world() -> World {
    let clock: Arc<dyn Clock> = Arc::new(TokioClock::new());
    let mem = MemStore::with_clock(Tier::Global, Arc::clone(&clock));
    let store: Arc<dyn MetaStore> = Arc::new(mem);
    let mut leases = BTreeMap::new();
    for (name, kind) in [("fra1", "full"), ("ams1", "full"), ("wit1", "witness")] {
        let rec = Record::new(DcRecord {
            dc_id: dc(name),
            kind: kind.into(),
            state: "active".into(),
            dc_epoch: 1,
            tags: Map::new(),
            global_voter: true,
        });
        put(
            &store,
            dc_key(&dc(name)).expect("k"),
            rec.encode().expect("e"),
            None,
        )
        .await;
        let l = store
            .grant_lease(Duration::from_secs(10))
            .await
            .expect("lease");
        put(
            &store,
            lease_key(&dc(name)).expect("k"),
            b"{\"v\":1,\"lease_id\":1,\"dc_epoch\":1}".to_vec(),
            Some(l),
        )
        .await;
        leases.insert(name.to_owned(), l);
    }
    for (t, standby) in [
        (42u64, json!(["ams1"])),
        (77, json!(["ams1"])),
        (99, json!([])),
    ] {
        let entry = json!({"v": 1, "name": format!("t{t}"), "state": "active", "home_dc": "fra1",
                           "epoch": 3, "standby_dcs": standby});
        put(
            &store,
            Tier::Global.key(&format!("tenants/{t}")).expect("k"),
            serde_json::to_vec(&entry).expect("j"),
            None,
        )
        .await;
        // ams1 holds a standby map.
        let mut m = ShardMap::catalog(TenantId(t), ClusterId::new("ams1-c1").expect("c"), 3);
        m.role = MapRole::Standby;
        let ams = TierView::over(&store, Tier::Dc(dc("ams1")));
        put(
            &ams,
            ShardMap::catalog_key(ams.tier(), TenantId(t)).expect("k"),
            Record::new(m).encode().expect("e"),
            None,
        )
        .await;
    }
    // Keep every lease alive until revoked (the DCs' controllers would).
    let (ka, ids): (Arc<dyn MetaStore>, Vec<sprig_meta::LeaseId>) =
        (Arc::clone(&store), leases.values().copied().collect());
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            for l in &ids {
                let _ = ka.keep_alive(*l).await;
            }
        }
    });
    World {
        store,
        clock,
        leases,
    }
}

fn driver(w: &World, obs: Arc<Scripted>, settings: FailoverSettings) -> GlobalFailover {
    let promotion = Promotion::single_etcd(
        &w.store,
        Guard::Store(Arc::clone(&w.store)),
        Arc::new(NoHooks),
    );
    GlobalFailover::new(
        Arc::clone(&w.store),
        Arc::clone(&w.clock),
        Arc::new(move |_: &DcId| settings.clone()),
        obs,
        promotion,
    )
}

fn scripted(v: &[(&str, VerdictKind)], lag_ms: u32) -> Arc<Scripted> {
    Arc::new(Scripted {
        verdicts: Mutex::new(v.iter().map(|(s, k)| ((*s).to_owned(), *k)).collect()),
        lag_ms,
        asked: Mutex::new(vec![]),
    })
}

async fn dc_state(w: &World, name: &str) -> String {
    let kv = w
        .store
        .get(&dc_key(&dc(name)).expect("k"), Consistency::Linearizable)
        .await
        .expect("g")
        .expect("dc");
    Record::<DcRecord>::decode(&kv.value).expect("d").body.state
}

async fn home(w: &World, t: u64) -> (String, u64, String) {
    let e = sprig_shard::home::read_home(&*w.store, TenantId(t))
        .await
        .expect("r")
        .expect("e");
    (e.home_dc.to_string(), e.epoch, e.state)
}

fn ended(o: &Outcome) -> &FailoverRecord {
    match o {
        Outcome::Ended(f) => f,
        Outcome::NotNeeded => panic!("not needed"),
    }
}

#[tokio::test(start_paused = true)]
async fn a_lost_dc_fails_over_tenants_within_their_rpo() {
    let w = world().await;
    let obs = scripted(
        &[("ams1", VerdictKind::Down), ("wit1", VerdictKind::Down)],
        800,
    );
    let g = driver(&w, Arc::clone(&obs), FailoverSettings::default());
    assert!(g.tick().await.expect("tick").is_empty(), "every lease held");
    w.store
        .revoke_lease(w.leases["fra1"])
        .await
        .expect("revoke");
    let out = g.tick().await.expect("tick");
    let f = ended(&out[0]);
    assert_eq!(f.state, FailoverState::Done, "{f:?}");
    assert_eq!(f.votes.len(), 2);
    assert_eq!(dc_state(&w, "fra1").await, "failed");
    let by: BTreeMap<u64, &TenantOutcome> = f.tenants.iter().map(|t| (t.tenant_id, t)).collect();
    assert_eq!(by[&42].outcome, "promoted");
    assert_eq!(by[&42].lag_ms, Some(800));
    assert_eq!(by[&77].outcome, "skipped", "lag 9.2 s over the 5 s RPO");
    assert_eq!(by[&99].outcome, "skipped", "no standby");
    assert_eq!(home(&w, 42).await, ("ams1".into(), 4, "active".into()));
    assert_eq!(
        home(&w, 77).await.0,
        "fra1",
        "skipped tenants stay homed in the fenced DC"
    );
    let ams = TierView::over(&w.store, Tier::Dc(dc("ams1")));
    let (m, _) = sprig_shard::map::TenantMaps::new(ams)
        .load(TenantId(42))
        .await
        .expect("map");
    assert_eq!(m.role, MapRole::Home);
    assert_eq!(m.tenant_epoch, 4);
    // Recorded.
    let kv = w
        .store
        .get(
            &failover_key(&f.failover_id).expect("k"),
            Consistency::Linearizable,
        )
        .await
        .expect("g")
        .expect("rec");
    assert_eq!(
        Record::<FailoverRecord>::decode(&kv.value)
            .expect("d")
            .body
            .state,
        FailoverState::Done
    );
}

#[tokio::test(start_paused = true)]
async fn an_alive_verdict_blocks_until_the_timeout_then_manual_only() {
    let w = world().await;
    let obs = scripted(
        &[("ams1", VerdictKind::Down), ("wit1", VerdictKind::Alive)],
        0,
    );
    let s = FailoverSettings {
        decision_timeout: Duration::from_secs(10),
        ..FailoverSettings::default()
    };
    let g = driver(&w, obs, s);
    g.tick().await.expect("tick");
    w.store
        .revoke_lease(w.leases["fra1"])
        .await
        .expect("revoke");
    let out = g.tick().await.expect("tick");
    let f = ended(&out[0]);
    assert_eq!(f.state, FailoverState::ManualOnly);
    assert_eq!(home(&w, 42).await.0, "fra1", "nothing promoted");
    assert_eq!(dc_state(&w, "fra1").await, "suspect", "fenced, not failed");
}

#[tokio::test(start_paused = true)]
async fn one_down_verdict_is_not_enough() {
    let w = world().await;
    // wit1 doesn't answer.
    let obs = scripted(&[("ams1", VerdictKind::Down)], 0);
    let s = FailoverSettings {
        decision_timeout: Duration::from_secs(5),
        ..FailoverSettings::default()
    };
    let g = driver(&w, obs, s);
    g.tick().await.expect("tick");
    w.store
        .revoke_lease(w.leases["fra1"])
        .await
        .expect("revoke");
    let f = ended(&g.tick().await.expect("tick")[0]).clone();
    assert_eq!(f.state, FailoverState::ManualOnly);
    assert_eq!(home(&w, 42).await.0, "fra1");
}

#[tokio::test(start_paused = true)]
async fn two_sites_mean_manual_only() {
    let w = world().await;
    // The witness's lease is gone too: only ams1 is available.
    w.store
        .revoke_lease(w.leases["wit1"])
        .await
        .expect("revoke");
    let obs = scripted(&[("ams1", VerdictKind::Down)], 0);
    let g = driver(&w, Arc::clone(&obs), FailoverSettings::default());
    g.tick().await.expect("tick");
    w.store
        .revoke_lease(w.leases["fra1"])
        .await
        .expect("revoke");
    let f = ended(&g.tick().await.expect("tick")[0]).clone();
    assert_eq!(f.state, FailoverState::ManualOnly);
    assert!(
        f.reason
            .as_deref()
            .unwrap_or_default()
            .contains("min_sites"),
        "{f:?}"
    );
    assert!(
        obs.asked.lock().expect("lock").is_empty(),
        "no observation without enough sites"
    );
}

#[tokio::test(start_paused = true)]
async fn a_lease_back_within_the_margin_closes_the_record() {
    let w = world().await;
    let obs = scripted(
        &[("ams1", VerdictKind::Down), ("wit1", VerdictKind::Down)],
        0,
    );
    let g = Arc::new(driver(&w, obs, FailoverSettings::default()));
    g.tick().await.expect("tick");
    w.store
        .revoke_lease(w.leases["fra1"])
        .await
        .expect("revoke");
    let store = Arc::clone(&w.store);
    let back = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let l = store
            .grant_lease(Duration::from_secs(10))
            .await
            .expect("lease");
        put(
            &store,
            lease_key(&dc("fra1")).expect("k"),
            b"{\"v\":1,\"lease_id\":2,\"dc_epoch\":2}".to_vec(),
            Some(l),
        )
        .await;
    });
    let out = g.tick().await.expect("tick");
    back.await.expect("join");
    let f = ended(&out[0]);
    assert_eq!(f.state, FailoverState::Closed);
    assert_eq!(dc_state(&w, "fra1").await, "active");
    assert_eq!(home(&w, 42).await.0, "fra1");
}

#[tokio::test(start_paused = true)]
async fn a_failed_dc_with_a_lease_again_is_returning() {
    let w = world().await;
    let obs = scripted(
        &[("ams1", VerdictKind::Down), ("wit1", VerdictKind::Down)],
        0,
    );
    let g = driver(&w, obs, FailoverSettings::default());
    g.tick().await.expect("tick");
    w.store
        .revoke_lease(w.leases["fra1"])
        .await
        .expect("revoke");
    g.tick().await.expect("tick");
    assert_eq!(dc_state(&w, "fra1").await, "failed");
    let l = w
        .store
        .grant_lease(Duration::from_secs(10))
        .await
        .expect("lease");
    put(
        &w.store,
        lease_key(&dc("fra1")).expect("k"),
        b"{\"v\":1,\"lease_id\":3,\"dc_epoch\":3}".to_vec(),
        Some(l),
    )
    .await;
    g.tick().await.expect("tick");
    assert_eq!(dc_state(&w, "fra1").await, "returning");
}

#[tokio::test(start_paused = true)]
async fn a_new_leader_waits_three_ttls_before_acting_on_a_missing_lease() {
    let w = world().await;
    w.store
        .revoke_lease(w.leases["fra1"])
        .await
        .expect("revoke");
    let obs = scripted(
        &[("ams1", VerdictKind::Down), ("wit1", VerdictKind::Down)],
        0,
    );
    let g = driver(&w, obs, FailoverSettings::default());
    assert!(
        g.tick().await.expect("tick").is_empty(),
        "never saw the lease: wait"
    );
    tokio::time::sleep(Duration::from_secs(31)).await;
    let out = g.tick().await.expect("tick");
    assert_eq!(ended(&out[0]).state, FailoverState::Done);
}

/// Approvals over `w`'s tiers with `failover.mode = approve`
/// set at the global scope.
async fn approvals_over(w: &World) -> Arc<crate::approvals::ControllerApprovals> {
    let ams = TierView::over(&w.store, Tier::Dc(dc("ams1")));
    let live = sprig_settings::LiveSettings::start(
        sprig_settings::Registry::global().expect("registry"),
        sprig_settings::SettingsStores::new(Arc::clone(&w.store), ams).expect("stores"),
    )
    .expect("live");
    live.ready().await;
    put(
        &w.store,
        Tier::Global.key("settings/failover.mode").expect("k"),
        br#"{"v":1,"value":"approve","set_by":"test"}"#.to_vec(),
        None,
    )
    .await;
    let hub = sprig_approvals::Approvals::new(
        Arc::new(sprig_approvals::MemStores::new()),
        Arc::clone(&w.clock),
    );
    let a = Arc::new(crate::approvals::ControllerApprovals::new(
        Arc::new(hub),
        dc("ams1"),
        crate::tree::Directory::new(Arc::clone(&w.store)),
        Some(live),
    ));
    for _ in 0..200 {
        let p = a
            .policy(sprig_approvals::Kind::FailoverDc, None, None, None, None)
            .await;
        if p.mode == sprig_approvals::Mode::Approve {
            return a;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("failover.mode = approve never reached the settings");
}

fn global_items(a: &crate::approvals::ControllerApprovals) -> Arc<dyn sprig_approvals::RecStore> {
    a.hub().stores().global().expect("global store")
}

#[allow(clippy::many_single_char_names)]
#[tokio::test(start_paused = true)]
async fn an_approval_releases_a_held_promotion_only_while_the_checks_allow_it() {
    use sprig_approvals::{Act, Caller, Filter, State};
    let w = world().await;
    let a = approvals_over(&w).await;
    let obs = scripted(
        &[("ams1", VerdictKind::Down), ("wit1", VerdictKind::Down)],
        800,
    );
    let g =
        driver(&w, Arc::clone(&obs), FailoverSettings::default()).with_approvals(Arc::clone(&a));
    assert!(g.tick().await.expect("tick").is_empty());
    w.store
        .revoke_lease(w.leases["fra1"])
        .await
        .expect("revoke");
    // 1. The DC fails, but the promotion waits: tenant 42 stays planned
    //    and fenced in fra1, with the item in its reason.
    let out = g.tick().await.expect("tick");
    let f = ended(&out[0]).clone();
    assert_eq!(f.state, FailoverState::Promoting, "{f:?}");
    let t42 = f.tenants.iter().find(|t| t.tenant_id == 42).expect("42");
    assert_eq!(t42.outcome, "planned");
    assert!(
        t42.reason
            .as_deref()
            .unwrap_or("")
            .starts_with("held for approval as g."),
        "{t42:?}"
    );
    assert_eq!(home(&w, 42).await.0, "fra1");
    let items = global_items(&a)
        .list(&Filter::default())
        .await
        .expect("list");
    assert_eq!(
        items.len(),
        1,
        "one item: tenants 77 and 99 were skipped by the checks"
    );
    let item = items[0].clone();
    assert_eq!(item.kind, sprig_approvals::Kind::FailoverDc);
    assert_eq!(item.level, sprig_approvals::Level::Platform);
    assert_eq!(item.state, State::Open);
    // Passes while held ask the gate at most every HOLD_RECHECK and write
    // nothing.
    for _ in 0..5 {
        g.tick().await.expect("tick");
    }
    assert_eq!(home(&w, 42).await.0, "fra1");
    // 2. A tenant administrator may not approve a platform item.
    let admin = Caller {
        name: "studio:t42.ana".into(),
        tenant: Some(TenantId(42)),
        tenant_admin: true,
        ..Caller::default()
    };
    let approve = Act::Approve {
        reason: "go".into(),
        window: None,
    };
    let e = a
        .hub()
        .act(
            &item.id,
            &approve,
            &admin,
            &[],
            sprig_approvals::ActKnobs::default(),
        )
        .await
        .expect_err("denied");
    assert_eq!(e.code(), Some(sprig_common::SprigCode::ApprovalDenied));
    // 3. The platform approves; the next check (after HOLD_RECHECK)
    //    observes again, finds the DC still down and the standby within
    //    its RPO, and promotes.
    a.hub()
        .act(
            &item.id,
            &approve,
            &Caller::platform("op:test"),
            &[],
            sprig_approvals::ActKnobs::default(),
        )
        .await
        .expect("approve");
    let asked_before = obs.asked.lock().expect("lock").len();
    tokio::time::sleep(HOLD_RECHECK + Duration::from_secs(1)).await;
    let out = g.tick().await.expect("tick");
    let f = ended(&out[0]);
    assert_eq!(f.state, FailoverState::Done, "{f:?}");
    assert!(
        obs.asked.lock().expect("lock").len() > asked_before,
        "observed again before promoting"
    );
    assert_eq!(home(&w, 42).await, ("ams1".into(), 4, "active".into()));
    let done = a.hub().get(&item.id).await.expect("get");
    assert_eq!(done.state, State::Applied, "{done:?}");
}

#[allow(clippy::many_single_char_names)]
#[tokio::test(start_paused = true)]
async fn an_approved_promotion_is_refused_when_the_standby_fell_behind() {
    use sprig_approvals::{Act, Caller, Filter, State};
    let w = world().await;
    let a = approvals_over(&w).await;
    let obs = scripted(
        &[("ams1", VerdictKind::Down), ("wit1", VerdictKind::Down)],
        800,
    );
    let g =
        driver(&w, Arc::clone(&obs), FailoverSettings::default()).with_approvals(Arc::clone(&a));
    g.tick().await.expect("tick");
    w.store
        .revoke_lease(w.leases["fra1"])
        .await
        .expect("revoke");
    g.tick().await.expect("tick");
    let item = global_items(&a)
        .list(&Filter::default())
        .await
        .expect("list")[0]
        .clone();
    a.hub()
        .act(
            &item.id,
            &Act::Approve {
                reason: String::new(),
                window: None,
            },
            &Caller::platform("op:test"),
            &[],
            sprig_approvals::ActKnobs::default(),
        )
        .await
        .expect("approve");
    // Meanwhile the standby's lag grew past the 5 s RPO: the approval
    // can't force the promotion.
    let obs2 = scripted(
        &[("ams1", VerdictKind::Down), ("wit1", VerdictKind::Down)],
        9000,
    );
    let g2 = driver(&w, obs2, FailoverSettings::default()).with_approvals(Arc::clone(&a));
    let out = g2.tick().await.expect("tick");
    let f = ended(&out[0]);
    let t42 = f.tenants.iter().find(|t| t.tenant_id == 42).expect("42");
    assert_eq!(t42.outcome, "skipped", "{t42:?}");
    assert_eq!(home(&w, 42).await.0, "fra1", "still fenced in fra1");
    let r = a.hub().get(&item.id).await.expect("get");
    assert_eq!(r.state, State::Failed, "{r:?}");
}
