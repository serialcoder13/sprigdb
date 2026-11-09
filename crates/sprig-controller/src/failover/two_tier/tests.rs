//! Two tiers: three separate etcds (global, fra1, ams1). The global leader
//! (in the witness) commits, ams1's `tenant_promote` finishes.

use std::sync::Mutex;
use std::time::Duration;

use serde_json::{Map, json};
use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_common::ids::ClusterId;
use sprig_common::time::Clock;
use sprig_meta::MemStore;
use sprig_meta::bootstrap::DcRecord;
use sprig_meta::dclease::{dc_key, lease_key};
use sprig_meta::session::TokioClock;

use super::*;
use crate::failover::global::{ObserverClient, Outcome};
use crate::failover::promote::{Guard, NoHooks, Promotion, TwoTierHooks};
use crate::failover::record::{Verdict, VerdictKind};
use crate::failover::settings::FailoverSettings;

fn dc(s: &str) -> DcId {
    DcId::new(s).expect("dc")
}

async fn put_kv(
    store: &dyn MetaStore,
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

struct Down;

#[async_trait::async_trait]
impl ObserverClient for Down {
    async fn observe(&self, site: &DcId, _x: &DcId, _id: &str) -> Result<Verdict> {
        Ok(Verdict {
            observer_dc: site.clone(),
            observer_lease_valid: true,
            verdict: VerdictKind::Down,
            unsafe_writers: vec![],
            lags: if site.as_str() == "ams1" {
                vec![crate::failover::record::StandbyLag {
                    tenant_id: 42,
                    lag_ms: 0,
                }]
            } else {
                vec![]
            },
        })
    }
}

/// Reports straight to the driver, as the HTTP route does; counts calls.
struct Direct(Arc<GlobalFailover>, Mutex<usize>);

#[async_trait::async_trait]
impl PromotionReporter for Direct {
    async fn done(&self, req: &PromotionDone) -> Result<PromotionDoneReply> {
        *self.1.lock().expect("lock") += 1;
        let activated = self
            .0
            .promotion_done(
                &req.failover_id,
                TenantId(req.tenant_id),
                &req.to,
                req.epoch,
                req.applied.clone(),
            )
            .await?;
        Ok(PromotionDoneReply { activated })
    }
}

/// Positions the standby "stopped" at.
struct Stops;

#[async_trait::async_trait]
impl PromoteHooks for Stops {
    async fn stop_standby(&self, _t: TenantId, _to: &DcId) -> Result<BTreeMap<String, String>> {
        Ok(BTreeMap::from([(
            "fra1-c1".to_owned(),
            "1-101-77".to_owned(),
        )]))
    }
    async fn make_writable(&self, _t: TenantId, _to: &DcId) -> Result<()> {
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn a_two_tier_failover_commits_globally_and_finishes_in_the_standby_dc() {
    let clock: Arc<dyn Clock> = Arc::new(TokioClock::new());
    let global: Arc<dyn MetaStore> =
        Arc::new(MemStore::with_clock(Tier::Global, Arc::clone(&clock)));
    // Each DC's tier is its own etcd: nothing the global leader can reach.
    let ams1: Arc<dyn MetaStore> = Arc::new(MemStore::with_clock(
        Tier::Dc(dc("ams1")),
        Arc::clone(&clock),
    ));
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
        put_kv(
            &*global,
            dc_key(&dc(name)).expect("k"),
            rec.encode().expect("e"),
            None,
        )
        .await;
        let l = global
            .grant_lease(Duration::from_secs(10))
            .await
            .expect("lease");
        put_kv(
            &*global,
            lease_key(&dc(name)).expect("k"),
            b"{\"v\":1,\"lease_id\":1,\"dc_epoch\":1}".to_vec(),
            Some(l),
        )
        .await;
        leases.insert(name, l);
    }
    let (ka, ids): (Arc<dyn MetaStore>, Vec<sprig_meta::LeaseId>) =
        (Arc::clone(&global), leases.values().copied().collect());
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            for l in &ids {
                let _ = ka.keep_alive(*l).await;
            }
        }
    });
    let entry = json!({"v": 1, "name": "t42", "state": "active", "home_dc": "fra1",
                       "epoch": 3, "standby_dcs": ["ams1"], "auth_version": 5});
    put_kv(
        &*global,
        Tier::Global.key("tenants/42").expect("k"),
        serde_json::to_vec(&entry).expect("j"),
        None,
    )
    .await;
    let mut m = ShardMap::catalog(TenantId(42), ClusterId::new("ams1-c1").expect("c"), 3);
    m.role = MapRole::Standby;
    put_kv(
        &*ams1,
        ShardMap::catalog_key(ams1.tier(), TenantId(42)).expect("k"),
        Record::new(m).encode().expect("e"),
        None,
    )
    .await;

    // The global leader runs in wit1 with two-tier hooks.
    let wit_tier: Arc<dyn MetaStore> = Arc::new(MemStore::with_clock(
        Tier::Dc(dc("wit1")),
        Arc::clone(&clock),
    ));
    let promotion = Promotion {
        global: Arc::clone(&global),
        guard: Guard::Store(Arc::clone(&global)),
        dc_tier: Arc::new(move |_: &DcId| Arc::clone(&wit_tier)),
        hooks: Arc::new(TwoTierHooks {
            inner: Arc::new(NoHooks),
            me: dc("wit1"),
        }),
    };
    let g = Arc::new(GlobalFailover::new(
        Arc::clone(&global),
        Arc::clone(&clock),
        Arc::new(|_: &DcId| FailoverSettings::default()),
        Arc::new(Down),
        promotion,
    ));
    assert!(g.tick().await.expect("tick").is_empty(), "every lease held");
    global.revoke_lease(leases["fra1"]).await.expect("revoke");
    let out = g.tick().await.expect("tick");
    let Outcome::Ended(f) = &out[0] else {
        panic!("a decision")
    };
    assert_eq!(f.state, FailoverState::Promoting, "{f:?}");
    assert_eq!(f.tenants[0].outcome, "committed");
    let e = read_home(&*global, TenantId(42))
        .await
        .expect("r")
        .expect("e");
    assert_eq!(
        (e.home_dc.as_str(), e.epoch, e.state.as_str()),
        ("ams1", 4, "failing_over")
    );

    // ams1's tenant_promote finishes steps 2-4 and reports.
    let reporter = Direct(Arc::clone(&g), Mutex::new(0));
    let n = tenant_promote(&*global, &*ams1, &dc("ams1"), &Stops, &reporter)
        .await
        .expect("pass");
    assert_eq!(n, 1);
    let (m, _) = sprig_shard::map::TenantMaps::new(Arc::clone(&ams1))
        .load(TenantId(42))
        .await
        .expect("map");
    assert_eq!((m.role, m.tenant_epoch), (MapRole::Home, 4));
    let e = read_home(&*global, TenantId(42))
        .await
        .expect("r")
        .expect("e");
    assert_eq!(e.state, "active");
    assert_eq!(e.raw["auth_version"], 6);
    // Another pass does nothing.
    assert_eq!(
        tenant_promote(&*global, &*ams1, &dc("ams1"), &Stops, &reporter)
            .await
            .expect("pass"),
        0
    );
    assert_eq!(*reporter.1.lock().expect("lock"), 1);
    // The record takes the positions and ends.
    let out = g.tick().await.expect("tick");
    let f = out
        .iter()
        .find_map(|o| match o {
            Outcome::Ended(f) => Some(f.clone()),
            Outcome::NotNeeded => None,
        })
        .expect("record");
    assert_eq!(f.state, FailoverState::Done, "{f:?}");
    assert_eq!(f.tenants[0].outcome, "promoted");
    assert_eq!(f.tenants[0].applied["fra1-c1"], "1-101-77");
}

#[tokio::test]
async fn a_lost_reply_is_reported_again_and_a_moved_directory_is_left_alone() {
    let global: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
    let ams1: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc("ams1"))));
    // A record with the tenant committed to ams1, but the directory moved
    // on (an operator failed it back): nothing runs.
    let rec = FailoverRecord {
        failover_id: "fo-1-fra1".into(),
        dc_id: dc("fra1"),
        state: FailoverState::Promoting,
        lease_lost_at: String::new(),
        votes: vec![],
        tenants: vec![crate::failover::record::TenantOutcome {
            tenant_id: 42,
            outcome: "committed".into(),
            to: Some(dc("ams1")),
            lag_ms: Some(0),
            reason: None,
            epoch: Some(4),
            applied: BTreeMap::new(),
            errant: None,
        }],
        reason: None,
    };
    put_kv(
        &*global,
        crate::failover::record::failover_key("fo-1-fra1").expect("k"),
        Record::new(rec).encode().expect("e"),
        None,
    )
    .await;
    let entry = json!({"v": 1, "name": "t42", "state": "active", "home_dc": "fra1", "epoch": 5});
    put_kv(
        &*global,
        Tier::Global.key("tenants/42").expect("k"),
        serde_json::to_vec(&entry).expect("j"),
        None,
    )
    .await;
    struct Never;
    #[async_trait::async_trait]
    impl PromotionReporter for Never {
        async fn done(&self, _r: &PromotionDone) -> Result<PromotionDoneReply> {
            panic!("nothing to report")
        }
    }
    struct NoStop;
    #[async_trait::async_trait]
    impl PromoteHooks for NoStop {
        async fn stop_standby(&self, _t: TenantId, _to: &DcId) -> Result<BTreeMap<String, String>> {
            panic!("the standby must not stop")
        }
        async fn make_writable(&self, _t: TenantId, _to: &DcId) -> Result<()> {
            panic!("no write access")
        }
    }
    assert_eq!(
        tenant_promote(&*global, &*ams1, &dc("ams1"), &NoStop, &Never)
            .await
            .expect("pass"),
        0
    );
}
