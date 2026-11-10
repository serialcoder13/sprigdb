//! The trigger runner over a scripted feed and an in-memory shard: lanes,
//! retries, dead letters, checkpoints, and exactly-once across restarts.

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sprig_binlog::{Gtid, GtidPos, Value as BValue};
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_feed::{ChangeEvent, EventId, FeedItem, Image, Op, ResumeToken, ShardRef};

use super::*;

const T: TenantId = TenantId(9);

fn token(seq: u64, row: u32) -> ResumeToken {
    let g = Gtid {
        domain: 0,
        server: 1,
        seq,
    };
    ResumeToken {
        tenant: T,
        clamp: seq,
        cluster: ClusterId::new("dc-a-c1").unwrap(),
        segment: 0,
        pos: GtidPos::from_gtids([g]),
        until: Some((g, row)),
        progress: false,
    }
}

fn event(seq: u64, table: &str, k: i64, v: i64, op: Op) -> FeedItem {
    let g = Gtid {
        domain: 0,
        server: 1,
        seq,
    };
    let row = Image(vec![
        ("k".into(), BValue::Int(k)),
        ("v".into(), BValue::Int(v)),
    ]);
    FeedItem::Event(Box::new(ChangeEvent {
        id: EventId {
            cluster: ClusterId::new("dc-a-c1").unwrap(),
            gtid: g,
            row: 0,
        },
        token: token(seq, 0),
        tenant: T,
        db: DbId(1),
        db_name: "shop".into(),
        object: 11,
        table: table.into(),
        shard: ShardRef {
            group: GroupId(1),
            shard: ShardId(1),
        },
        key: Image(vec![("k".into(), BValue::Int(k))]),
        op,
        before: (op != Op::Insert).then(|| row.clone()),
        after: (op != Op::Delete).then_some(row),
        delta: (op == Op::Update).then(|| vec!["v".into()]),
        hlc: seq,
        txn: g,
        ddl: None,
        hint: None,
    }))
}

/// A feed from a script, resumable after any token.
struct Script {
    items: VecDeque<FeedItem>,
}

#[async_trait]
impl Feed for Script {
    async fn next(&mut self) -> Result<FeedItem> {
        match self.items.pop_front() {
            Some(i) => Ok(i),
            None => std::future::pending().await,
        }
    }
}

fn script(all: &[FeedItem], after: Option<&ResumeToken>) -> Script {
    let start = match after {
        Some(t) => all.iter().position(|i| i.token() == t).map_or(0, |p| p + 1),
        None => 0,
    };
    Script {
        items: all[start..].iter().cloned().collect(),
    }
}

/// An in-memory shard: markers and the trigger's effects commit together.
#[derive(Default)]
struct Shard {
    markers: BTreeSet<String>,
    effects: Vec<(String, String)>,
    /// Failures to inject per key (non-retryable).
    fail: HashMap<String, u32>,
    /// The order effects arrived in, per row key.
    order: HashMap<String, Vec<String>>,
}

struct Mem(Arc<Mutex<Shard>>);

#[async_trait]
impl Apply for Mem {
    async fn apply(&self, ev: &TriggerEvent) -> Result<Applied> {
        tokio::time::sleep(Duration::from_millis(1)).await;
        let mut s = self.0.lock().unwrap();
        if let Some(n) = s.fail.get_mut(&ev.key)
            && *n > 0
        {
            *n -= 1;
            return Err(SprigError::new(SprigCode::LogicUserError, "boom"));
        }
        if !s.markers.insert(ev.key.clone()) {
            return Ok(Applied::AlreadyDone);
        }
        s.effects.push((ev.key.clone(), ev.op.clone()));
        s.order
            .entry(ev.row_key.clone())
            .or_default()
            .push(ev.key.clone());
        Ok(Applied::Ran(1))
    }

    fn version(&self) -> u64 {
        1
    }
}

#[derive(Default)]
struct Out {
    acked: Mutex<Option<ResumeToken>>,
    dead: Mutex<Vec<(String, u32)>>,
}

#[async_trait]
impl Outcomes for Out {
    async fn dead_letter(
        &self,
        ev: &TriggerEvent,
        _v: u64,
        attempts: u32,
        _e: &SprigError,
    ) -> Result<()> {
        self.dead.lock().unwrap().push((ev.key.clone(), attempts));
        Ok(())
    }

    async fn ack(&self, token: &ResumeToken, _events: u64) -> Result<()> {
        *self.acked.lock().unwrap() = Some(token.clone());
        Ok(())
    }

    async fn log(&self, _level: &str, _message: &str) {}
}

fn filter() -> Filter {
    Filter {
        db: 1,
        object: 11,
        table: "orders".into(),
        events: vec!["insert".into(), "update".into()],
        when_fields: Vec::new(),
    }
}

fn opts() -> RunOpts {
    RunOpts {
        lanes: 4,
        max_attempts: 3,
        retry_initial: Duration::from_millis(5),
        retry_max: Duration::from_millis(20),
        ack_every: Duration::from_millis(10),
        max_in_flight: 64,
        live: None,
    }
}

fn workload(n: u64) -> Vec<FeedItem> {
    let mut v = Vec::new();
    for s in 1..=n {
        let k = i64::try_from(s % 7).unwrap();
        let op = if s % 3 == 0 { Op::Update } else { Op::Insert };
        let table = if s % 5 == 0 { "other" } else { "orders" };
        v.push(event(s, table, k, i64::try_from(s).unwrap(), op));
        if s % 4 == 0 {
            v.push(event(s, "orders", k, 0, Op::Delete));
        }
        v.push(FeedItem::TxnEnd {
            token: token(s, sprig_feed::token::AFTER_TXN),
        });
    }
    v
}

fn wanted(all: &[FeedItem]) -> BTreeSet<String> {
    all.iter()
        .filter_map(|i| match i {
            FeedItem::Event(e) if filter().wants(e) => TriggerEvent::from_change(e).map(|t| t.key),
            _ => None,
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn every_wanted_event_runs_once_in_key_order() {
    let all = workload(60);
    let shard = Arc::new(Mutex::new(Shard::default()));
    let out = Arc::new(Out::default());
    let stats = Arc::new(Stats::default());
    let cancel = CancellationToken::new();
    let feed = script(&all, None);
    let c2 = cancel.clone();
    let h = {
        let shard = Arc::clone(&shard);
        let out = Arc::clone(&out);
        let stats = Arc::clone(&stats);
        tokio::spawn(async move {
            run(
                Box::new(feed),
                &filter(),
                Arc::new(Mem(shard)),
                out,
                &opts(),
                stats,
                &c2,
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_secs(5)).await;
    cancel.cancel();
    h.await.unwrap().unwrap();
    let s = shard.lock().unwrap();
    let got: BTreeSet<String> = s.effects.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(got.len(), s.effects.len(), "no event ran twice");
    assert_eq!(got, wanted(&all));
    // Each row's events ran in feed order.
    for keys in s.order.values() {
        let mut sorted = keys.clone();
        sorted.sort_by_key(|k| {
            k.split('-')
                .nth(2)
                .and_then(|x| x.split(':').next())
                .and_then(|x| x.parse::<u64>().ok())
        });
        assert_eq!(*keys, sorted);
    }
    // The checkpoint reached the end.
    assert_eq!(
        out.acked.lock().unwrap().as_ref(),
        Some(all.last().unwrap().token())
    );
    assert_eq!(
        stats.ran.load(Ordering::Relaxed),
        u64::try_from(got.len()).unwrap()
    );
}

#[tokio::test(start_paused = true)]
async fn crashes_resume_from_the_checkpoint_without_running_twice() {
    let all = workload(200);
    let shard = Arc::new(Mutex::new(Shard::default()));
    let out = Arc::new(Out::default());
    let mut restarts = 0;
    let mut stats_already = 0;
    for round in 0..12u64 {
        let after = out.acked.lock().unwrap().clone();
        let feed = script(&all, after.as_ref());
        let cancel = CancellationToken::new();
        let stats = Arc::new(Stats::default());
        let c2 = cancel.clone();
        let (sh, o, st) = (Arc::clone(&shard), Arc::clone(&out), Arc::clone(&stats));
        let h = tokio::spawn(async move {
            run(
                Box::new(feed),
                &filter(),
                Arc::new(Mem(sh)),
                o,
                &opts(),
                st,
                &c2,
            )
            .await
        });
        // A worker killed at an odd moment: before or after acks.
        tokio::time::sleep(Duration::from_millis(7 + round * 13)).await;
        cancel.cancel();
        h.await.unwrap().unwrap();
        restarts += 1;
        stats_already += stats.already.load(Ordering::Relaxed);
    }
    // A last life runs to the end.
    let after = out.acked.lock().unwrap().clone();
    let feed = script(&all, after.as_ref());
    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    let (sh, o) = (Arc::clone(&shard), Arc::clone(&out));
    let h = tokio::spawn(async move {
        run(
            Box::new(feed),
            &filter(),
            Arc::new(Mem(sh)),
            o,
            &opts(),
            Arc::new(Stats::default()),
            &c2,
        )
        .await
    });
    tokio::time::sleep(Duration::from_secs(10)).await;
    cancel.cancel();
    h.await.unwrap().unwrap();
    let s = shard.lock().unwrap();
    let got: BTreeSet<String> = s.effects.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(got.len(), s.effects.len(), "an event ran twice");
    assert_eq!(got, wanted(&all), "an event was lost");
    assert_eq!(restarts, 12);
    assert!(
        stats_already > 0,
        "the markers never had to stop a re-delivery"
    );
}

#[tokio::test(start_paused = true)]
async fn failures_retry_then_dead_letter_and_the_lane_moves_on() {
    let all = workload(30);
    let keys: Vec<String> = wanted(&all).into_iter().collect();
    let shard = Arc::new(Mutex::new(Shard::default()));
    {
        let mut s = shard.lock().unwrap();
        s.fail.insert(keys[0].clone(), 2); // recovers on the third attempt
        s.fail.insert(keys[1].clone(), 99); // dead letter
    }
    let out = Arc::new(Out::default());
    let stats = Arc::new(Stats::default());
    let cancel = CancellationToken::new();
    let feed = script(&all, None);
    let c2 = cancel.clone();
    let (sh, o, st) = (Arc::clone(&shard), Arc::clone(&out), Arc::clone(&stats));
    let h = tokio::spawn(async move {
        run(
            Box::new(feed),
            &filter(),
            Arc::new(Mem(sh)),
            o,
            &opts(),
            st,
            &c2,
        )
        .await
    });
    tokio::time::sleep(Duration::from_secs(5)).await;
    cancel.cancel();
    h.await.unwrap().unwrap();
    let s = shard.lock().unwrap();
    let got: BTreeSet<String> = s.effects.iter().map(|(k, _)| k.clone()).collect();
    assert!(got.contains(&keys[0]));
    assert!(!got.contains(&keys[1]));
    assert_eq!(got.len(), keys.len() - 1);
    assert_eq!(*out.dead.lock().unwrap(), vec![(keys[1].clone(), 3)]);
    assert_eq!(stats.dead.load(Ordering::Relaxed), 1);
    assert_eq!(
        out.acked.lock().unwrap().as_ref(),
        Some(all.last().unwrap().token())
    );
}

#[test]
fn filters_and_arguments() {
    let FeedItem::Event(e) = event(4, "orders", 3, 40, Op::Update) else {
        unreachable!()
    };
    let mut f = filter();
    assert!(f.wants(&e));
    f.when_fields = vec!["status".into()];
    assert!(!f.wants(&e));
    f.when_fields = vec!["V".into()];
    assert!(f.wants(&e));
    let t = TriggerEvent::from_change(&e).unwrap();
    assert_eq!(t.key, "dc-a-c1:0-1-4:0");
    assert_eq!(
        t.args(&["v".into(), "old_v".into(), "op".into(), "zz".into()]),
        ["40", "40", "'update'", "NULL"]
    );
    assert_eq!(
        TriggerEvent::from_json(&t.to_json())
            .unwrap()
            .args(&["k".into()]),
        ["3"]
    );
    assert_eq!(t.schema(TenantId(9)), "t9_d1_g1_s1");
}

#[test]
fn check_failures_become_39863() {
    remember_check("sprig_lc7_v2", "discount_rules", "discount above 20%");
    let e = SprigError::backend(
        4025,
        *b"23000",
        "CONSTRAINT `sprig_lc7_v2` failed for `shop`.`orders`",
    );
    let m = map_check_error(e);
    assert_eq!(m.code(), Some(SprigCode::LogicValidationFailed));
    assert_eq!(m.message, "discount above 20%");
    let e = SprigError::backend(
        3819,
        *b"HY000",
        "Check constraint 'sprig_lc7_v2' is violated.",
    );
    assert_eq!(map_check_error(e).message, "discount above 20%");
    let other = map_check_error(SprigError::backend(
        4025,
        *b"23000",
        "CONSTRAINT `c1` failed",
    ));
    assert!(other.code().is_none());
    assert_eq!(binding_of(&subscription_name(42)), Some(42));
}
