//! Simulation of the hub over memory stores with a manual clock: raise,
//! people's actions with who-may-act, escalation, auto-apply, events.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use sprig_common::SprigCode;
use sprig_common::ids::{DbId, DcId, ObjectId, TenantId};
use sprig_common::time::ManualClock;

use crate::Caller;
use crate::gate::{ActKnobs, Approvals, Event, Hook, MemStores, Stores};
use crate::machine::{Act, EventKind, TickChange, Verdict};
use crate::model::{Action, Kind, Level, Mode, Proposal, State, Subject};
use crate::policy::Policy;

#[derive(Default)]
struct Events(Mutex<Vec<(EventKind, String)>>);

#[async_trait::async_trait]
impl Hook for Events {
    async fn event(&self, ev: &Event) {
        self.0
            .lock()
            .unwrap()
            .push((ev.kind, ev.rec.notify_level.to_string()));
    }
}

fn dc_move(to: &str) -> Proposal {
    Proposal {
        kind: Kind::DcMove,
        subject: Subject {
            tenant: Some(TenantId(42)),
            tenant_name: Some("acme".into()),
            dc: Some(DcId::new("mum1").unwrap()),
            ..Subject::default()
        },
        target: "t42/home".into(),
        action: Action {
            text: format!("sprigctl tenant move acme --to {to}"),
            machine: serde_json::json!({"to": to}),
        },
        evidence: serde_json::json!({"gain_pct": 41}),
        score: 41.0,
        summary: format!("move acme to {to}: 41% lower latency"),
        emergency: false,
    }
}

#[tokio::test]
async fn a_dc_move_held_at_tenant_level_escalates_to_an_ancestor_then_the_platform() {
    let clock = Arc::new(ManualClock::new());
    let stores = Arc::new(MemStores::new());
    let events = Arc::new(Events::default());
    let hub = Approvals::new(stores.clone(), clock.clone()).with_hook(events.clone());
    let mut pol = Policy::fixed(Mode::Approve, Level::Tenant);
    pol.escalate_after = Some(Duration::from_secs(7200));
    let anc = [TenantId(1), TenantId(9)];

    let v = hub.check(&dc_move("fra1"), &pol, &anc).await.unwrap();
    let Verdict::Wait(id) = v else {
        panic!("{v:?}")
    };
    let store = stores.tenant(TenantId(42));
    let t = Duration::from_secs(86_400);

    // A plain user of acme can't act; an admin of an unrelated tenant can't.
    let user = Caller {
        name: "acme.bob".into(),
        tenant: Some(TenantId(42)),
        ..Caller::default()
    };
    let e = hub
        .act(
            &id,
            &Act::Approve {
                reason: String::new(),
                window: None,
            },
            &user,
            &anc,
            ActKnobs::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::ApprovalDenied));

    clock.advance(Duration::from_secs(7201));
    hub.check(&dc_move("fra1"), &pol, &anc).await.unwrap(); // the decider keeps proposing it
    let ch = hub.tick(&*store, t).await.unwrap();
    assert_eq!(ch.len(), 1);
    assert!(matches!(ch[0].1, TickChange::Escalated(_)));
    assert_eq!(
        hub.get(&id).await.unwrap().notify_level.to_string(),
        "ancestor:9"
    );
    clock.advance(Duration::from_secs(7201));
    hub.check(&dc_move("fra1"), &pol, &anc).await.unwrap();
    hub.tick(&*store, t).await.unwrap();
    clock.advance(Duration::from_secs(7201));
    hub.check(&dc_move("fra1"), &pol, &anc).await.unwrap();
    hub.tick(&*store, t).await.unwrap();
    assert_eq!(
        hub.get(&id).await.unwrap().notify_level.to_string(),
        "platform"
    );

    let op = Caller::platform("op:kiran");
    let r = hub
        .act(
            &id,
            &Act::Approve {
                reason: "ok".into(),
                window: None,
            },
            &op,
            &anc,
            ActKnobs::default(),
        )
        .await
        .unwrap();
    assert_eq!(r.state, State::Approved);
    let v = hub.check(&dc_move("fra1"), &pol, &anc).await.unwrap();
    let Verdict::Proceed(ticket) = v else {
        panic!("{v:?}")
    };
    hub.finished(&ticket, true, "move committed").await.unwrap();
    assert_eq!(hub.get(&id).await.unwrap().state, State::Applied);

    let evs: Vec<_> = events
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|(k, l)| format!("{}@{l}", k.name()))
        .collect();
    assert_eq!(
        evs,
        [
            "recommendation.new@tenant",
            "recommendation.escalated@ancestor:9",
            "recommendation.escalated@ancestor:1",
            "recommendation.escalated@platform",
            "recommendation.approved@platform",
            "recommendation.applying@platform",
            "recommendation.applied@platform",
        ]
    );
}

#[tokio::test]
async fn an_index_approved_by_its_table_owner_and_another_auto_applied() {
    let clock = Arc::new(ManualClock::new());
    let stores = Arc::new(MemStores::new());
    let hub = Approvals::new(stores.clone(), clock.clone());
    let p = |cols: &str| Proposal {
        kind: Kind::IndexCreate,
        subject: Subject {
            tenant: Some(TenantId(42)),
            db: Some(DbId(1)),
            object: Some(ObjectId(7)),
            ..Subject::default()
        },
        target: format!("t42/o7/{cols}"),
        action: Action {
            text: format!("ADD INDEX ({cols})"),
            machine: serde_json::json!({"cols": cols}),
        },
        evidence: serde_json::json!({}),
        score: 100.0,
        summary: "index".into(),
        emergency: false,
    };
    let mut pol = Policy::fixed(Mode::Approve, Level::Table);
    pol.auto_apply_after = Some(Duration::from_secs(1800));
    let Verdict::Wait(a) = hub.check(&p("a"), &pol, &[]).await.unwrap() else {
        panic!()
    };
    let Verdict::Wait(b) = hub.check(&p("b"), &pol, &[]).await.unwrap() else {
        panic!()
    };
    let owner = Caller {
        name: "acme.ana".into(),
        tenant: Some(TenantId(42)),
        table_privilege: true,
        ..Caller::default()
    };
    hub.act(
        &a,
        &Act::Approve {
            reason: String::new(),
            window: None,
        },
        &owner,
        &[],
        ActKnobs::default(),
    )
    .await
    .unwrap();
    clock.advance(Duration::from_secs(1801));
    let store = stores.tenant(TenantId(42));
    let ch = hub
        .tick(&*store, Duration::from_secs(86_400))
        .await
        .unwrap();
    assert_eq!(ch, vec![(b.clone(), TickChange::AutoApproved)]);
    assert!(hub.check(&p("a"), &pol, &[]).await.unwrap().proceed());
    assert!(hub.check(&p("b"), &pol, &[]).await.unwrap().proceed());
}

#[tokio::test]
async fn modes_on_and_off_never_touch_the_store() {
    let clock = Arc::new(ManualClock::new());
    let stores = Arc::new(MemStores::new());
    let hub = Approvals::new(stores.clone(), clock);
    let v = hub
        .check(
            &dc_move("fra1"),
            &Policy::fixed(Mode::On, Level::Tenant),
            &[],
        )
        .await
        .unwrap();
    assert!(v.proceed());
    let v = hub
        .check(
            &dc_move("fra1"),
            &Policy::fixed(Mode::Off, Level::Tenant),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(v, Verdict::Off);
    assert!(stores.of(TenantId(42)).all().is_empty());
}

#[tokio::test]
async fn failover_items_live_in_the_tiers() {
    let clock = Arc::new(ManualClock::new());
    let stores = Arc::new(MemStores::new());
    let hub = Approvals::new(stores.clone(), clock);
    let p = Proposal {
        kind: Kind::FailoverCluster,
        subject: Subject {
            dc: Some(DcId::new("mum1").unwrap()),
            cluster: Some(sprig_common::ids::ClusterId::new("mum1-c1").unwrap()),
            ..Subject::default()
        },
        target: "mum1-c1".into(),
        action: Action {
            text: "promote mum1-c1-r1".into(),
            machine: serde_json::json!({"to": "r1"}),
        },
        evidence: serde_json::json!({}),
        score: 1.0,
        summary: "writer down".into(),
        emergency: false,
    };
    let v = hub
        .check(&p, &Policy::fixed(Mode::Approve, Level::Platform), &[])
        .await
        .unwrap();
    let Verdict::Wait(id) = v else { panic!() };
    assert_eq!(id.to_string(), "dc.mum1.1");
    let dco = Caller::dc_operator("dc:mum1", DcId::new("mum1").unwrap());
    let e = hub
        .act(
            &id,
            &Act::Ignore {
                reason: String::new(),
                for_days: Some(1),
                forever: false,
            },
            &dco,
            &[],
            ActKnobs::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        Some(SprigCode::ApprovalDenied),
        "platform level: DC operators can't act"
    );
}
