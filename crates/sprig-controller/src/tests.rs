//! Role tests: bootstrap, verification and election over `MemStore`, and
//! the whole role against the etcd named by `SPRIG_TEST_ETCD` (skipped
//! with a message when it isn't set).

// Test suites: failures are panics with a message.
#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::unchecked_time_subtraction,
    clippy::single_match_else
)]

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;
use sprig_common::telemetry::ProcessState;
use sprig_common::{Health, ProcessConfig};
use sprig_meta::bootstrap::DcRecord;
use sprig_meta::elect::{self, Leader};
use sprig_meta::session::{Session, SessionOpts};
use sprig_meta::{Cmp, EtcdConfig, EtcdStore, Key, ListOpts, Op, Record, Tier, Txn};

use super::*;
use crate::dispatch::{self, JobClass, JobState, NewJob, Registration, RegistrationKind, get_job};
use crate::runtime::tests::{mem, paused};

fn config(dc: &str, id: &str, deployment: &str, bootstrap: bool) -> ProcessConfig {
    ProcessConfig::from_toml_and_env(
        &format!(
            "[node]\ndc = \"{dc}\"\nid = \"{id}\"\n[meta]\ndeployment_id = \"{deployment}\"\n[controller]\nbootstrap = {bootstrap}\nadmin_addr = \"127.0.0.1:0\"\n"
        ),
        Vec::new(),
    )
    .unwrap()
}

struct Started {
    stop: CancellationToken,
    health: Health,
    task: tokio::task::JoinHandle<Result<()>>,
}

fn start(cfg: ProcessConfig, stores: &Stores, clock: &Arc<dyn Clock>) -> Started {
    let ctx = RoleContext {
        config: cfg,
        health: Health::default(),
        shutdown: CancellationToken::new(),
    };
    let (stop, health) = (ctx.shutdown.clone(), ctx.health.clone());
    let task = tokio::spawn(serve(ctx, stores.clone(), clock.clone()));
    Started { stop, health, task }
}

async fn leader_of(store: &dyn MetaStore) -> Option<Leader> {
    elect::current_leader(store, &store.tier().key("leader").unwrap())
        .await
        .unwrap()
}

async fn wait_for<F: Fn() -> bool>(what: &str, f: F) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[test]
fn bootstraps_the_first_dc_and_leads() {
    paused().block_on(async {
        let (_s, stores, clock) = mem("dc-a");
        let a = start(config("dc-a", "ctl-a", "dep-1", true), &stores, &clock);
        wait_for("ready", || a.health.state() == ProcessState::Ready).await;
        let dc = stores
            .global
            .get(
                &Tier::Global.key("dcs/dc-a").unwrap(),
                sprig_meta::Consistency::Linearizable,
            )
            .await
            .unwrap()
            .unwrap();
        let dc = Record::<DcRecord>::decode(&dc.value).unwrap().body;
        assert_eq!(dc.state, "active");
        tokio::time::sleep(Duration::from_secs(1)).await;
        let l = leader_of(&*stores.dc).await.unwrap();
        assert_eq!(l.record.holder, "ctl-a");
        assert_eq!(l.record.dc.as_deref(), Some("dc-a"));

        // Shutdown drains and resigns.
        a.stop.cancel();
        a.task.await.unwrap().unwrap();
        assert_eq!(a.health.state(), ProcessState::Draining);
        assert!(leader_of(&*stores.dc).await.is_none());
    });
}

#[test]
fn waits_for_bootstrap_and_refuses_another_deployment() {
    paused().block_on(async {
        let (_s, stores, clock) = mem("dc-a");
        let missing = start(config("dc-a", "x", "", true), &stores, &clock);
        let e = missing.task.await.unwrap().unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ConfigInvalid));

        let b = start(config("dc-a", "ctl-b", "dep-1", false), &stores, &clock);
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(
            b.health.state(),
            ProcessState::Starting,
            "waits for bootstrap"
        );
        let a = start(config("dc-a", "ctl-a", "dep-1", true), &stores, &clock);
        wait_for("b ready", || b.health.state() == ProcessState::Ready).await;
        wait_for("a ready", || a.health.state() == ProcessState::Ready).await;

        let wrong = start(config("dc-a", "ctl-c", "dep-2", false), &stores, &clock);
        let e = wrong.task.await.unwrap().unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ConfigInvalid));
        let wrong = start(config("dc-a", "ctl-c", "dep-2", true), &stores, &clock);
        let e = wrong.task.await.unwrap().unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ConfigInvalid));

        // Exactly one of a and b leads; stopping it hands over.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let first = leader_of(&*stores.dc).await.unwrap();
        let (leader, other) = if first.record.holder == "ctl-a" {
            (a, b)
        } else {
            (b, a)
        };
        leader.stop.cancel();
        leader.task.await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let second = leader_of(&*stores.dc).await.unwrap();
        assert_ne!(second.record.holder, first.record.holder);
        assert!(second.token > first.token);
        other.stop.cancel();
        other.task.await.unwrap().unwrap();
    });
}

// ---- against etcd -----------------------------------------------------------

/// A DC id no other test or agent uses.
fn unique_dc() -> DcId {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    DcId::new(format!("ctl{:x}-{nanos:x}", std::process::id())).unwrap()
}

async fn delete_prefix(store: &dyn MetaStore, prefix: &Key) {
    let page = store
        .list(
            prefix,
            ListOpts {
                limit: 10_000,
                ..ListOpts::default()
            },
        )
        .await
        .unwrap();
    for kv in page.kvs {
        let _ = store
            .txn(Txn {
                when: vec![],
                then: vec![Op::Delete(kv.key)],
                otherwise: vec![],
            })
            .await;
    }
}

#[test]
fn etcd_controller_bootstraps_leads_dispatches_and_hands_over() {
    let Ok(endpoints) = std::env::var(sprig_meta::conformance::ETCD_ENV) else {
        eprintln!(
            "skipping etcd_controller_bootstraps_leads_dispatches_and_hands_over: set SPRIG_TEST_ETCD"
        );
        return;
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dc = unique_dc();
        let cfg = EtcdConfig::new(endpoints.split(',').map(str::to_owned).collect());
        let dc_store = EtcdStore::connect(Tier::Dc(dc.clone()), &cfg)
            .await
            .unwrap();
        let stores = Stores {
            global: Arc::new(dc_store.with_tier(Tier::Global)),
            dc: Arc::new(dc_store),
        };
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        // The deployment id is one global key on a shared etcd: reuse one
        // that exists, else create ours and remove it afterwards.
        let dep_key = Tier::Global
            .key(&format!("settings/{}", bootstrap::DEPLOYMENT_ID_SETTING))
            .unwrap();
        let existing = stores
            .global
            .get(&dep_key, sprig_meta::Consistency::Linearizable)
            .await
            .unwrap();
        let deployment = match &existing {
            Some(kv) => {
                let rec = Record::<bootstrap::SettingRecord>::decode(&kv.value).unwrap();
                rec.body.value.unwrap().as_str().unwrap().to_owned()
            }
            None => format!("test-{dc}"),
        };

        let a = start(
            config(dc.as_str(), "ctl-a", &deployment, true),
            &stores,
            &clock,
        );
        let b = start(
            config(dc.as_str(), "ctl-b", &deployment, true),
            &stores,
            &clock,
        );
        let leader = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(l) = leader_of(&*stores.dc).await {
                    return l;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
        assert!(["ctl-a", "ctl-b"].contains(&leader.record.holder.as_str()));
        if existing.is_none() {
            let dcs = crate::dc::list(&*stores.global).await.unwrap();
            assert!(dcs.iter().any(|d| d.dc_id == dc && d.state == "active"));
        }

        // A worker registers; an operator's job is queued; dispatch assigns
        // it, the worker claims and finishes it.
        let session = Session::open(
            stores.dc.clone(),
            clock.clone(),
            SessionOpts::new(Duration::from_secs(5)),
        )
        .await
        .unwrap();
        let reg = Registration {
            address: "w1:7401".to_owned(),
            zone: "z1".to_owned(),
            version: "test".to_owned(),
            load: serde_json::Value::Null,
            slots: [(JobClass::Other, 1)].into_iter().collect(),
        };
        dispatch::register(&session, RegistrationKind::Worker, "w1", &reg)
            .await
            .unwrap();
        let id = sprig_common::ids::JobId::new_v7();
        let job = dispatch::Job::queued(
            id,
            NewJob {
                kind: "validate".to_owned(),
                class: JobClass::Other,
                tenant_id: None,
                clusters: vec![],
                priority: 0,
                cause: dispatch::OPERATOR.to_owned(),
                zone: None,
                spec: json!({}),
            },
            &*clock,
        );
        let key = dispatch::job_key(&*stores.dc, id).unwrap();
        stores
            .dc
            .txn(Txn {
                when: vec![Cmp::Missing(key.clone())],
                then: vec![Op::Put {
                    key,
                    value: Record::new(job).encode().unwrap(),
                    lease: None,
                }],
                otherwise: vec![],
            })
            .await
            .unwrap();
        let seen = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let mine = dispatch::assigned_to(&*stores.dc, "w1").await.unwrap();
                if let Some(j) = mine.into_iter().next() {
                    return j;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
        let c = dispatch::claim(&*stores.dc, "w1", seen)
            .await
            .unwrap()
            .unwrap();
        let done = dispatch::finish(&*stores.dc, &*clock, c, Ok(json!({"ok": true})))
            .await
            .unwrap();
        assert_eq!(done, dispatch::Ownership::Held);
        let j = get_job(&*stores.dc, id).await.unwrap().unwrap();
        assert_eq!(j.job().state, JobState::Succeeded);

        // Stopping the leader resigns; the other controller takes over.
        let (first, other) = if leader.record.holder == "ctl-a" {
            (a, b)
        } else {
            (b, a)
        };
        first.stop.cancel();
        first.task.await.unwrap().unwrap();
        let next = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(l) = leader_of(&*stores.dc).await
                    && l.token != leader.token
                {
                    return l;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
        assert_ne!(next.record.holder, leader.record.holder);
        assert!(next.token > leader.token);
        other.stop.cancel();
        other.task.await.unwrap().unwrap();
        session.close().await.unwrap();

        // Clean up: this DC's keys, its DC record, and the deployment id
        // if this test created it.
        delete_prefix(&*stores.dc, &stores.dc.tier().root()).await;
        let dc_key = Tier::Global.key(&format!("dcs/{dc}")).unwrap();
        let _ = stores
            .global
            .txn(Txn {
                when: vec![],
                then: vec![Op::Delete(dc_key)],
                otherwise: vec![],
            })
            .await;
        if existing.is_none() {
            let ids = Tier::Global.key("ids/tenant").unwrap();
            if let Some(kv) = stores
                .global
                .get(&dep_key, sprig_meta::Consistency::Linearizable)
                .await
                .unwrap()
            {
                let _ = stores
                    .global
                    .txn(Txn {
                        when: vec![Cmp::ModRev(dep_key.clone(), kv.mod_revision)],
                        then: vec![Op::Delete(dep_key.clone()), Op::Delete(ids)],
                        otherwise: vec![],
                    })
                    .await;
            }
        }
    });
}
