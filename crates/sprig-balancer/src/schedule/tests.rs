#![allow(clippy::unwrap_used, clippy::float_cmp, clippy::many_single_char_names)]

use std::sync::Mutex;

use super::*;
use crate::decisions::MemDecisionLog;
use crate::plan::{PlacementPolicy, Planner};
use crate::settings::BalancerSettings;
use crate::testutil::{cid, fleet, load, put};

/// Moves finish (or fail) when told to.
#[derive(Default)]
struct Fake {
    jobs: Mutex<Vec<(MoveTicket, MoveOrder, MoveStatus)>>,
}

impl Fake {
    fn finish_all(&self, st: &MoveStatus) {
        for j in self.jobs.lock().unwrap().iter_mut() {
            j.2 = st.clone();
        }
    }
}

#[async_trait::async_trait]
impl MoveExecutor for Fake {
    async fn submit(&self, order: &MoveOrder) -> Result<MoveTicket> {
        let mut g = self.jobs.lock().unwrap();
        let t = MoveTicket(format!("job-{}", g.len()));
        g.push((t.clone(), order.clone(), MoveStatus::Running));
        Ok(t)
    }
    async fn status(&self, ticket: &MoveTicket) -> Result<MoveStatus> {
        Ok(self
            .jobs
            .lock()
            .unwrap()
            .iter()
            .find(|j| &j.0 == ticket)
            .map_or(MoveStatus::Cancelled, |j| j.2.clone()))
    }
    async fn cancel(&self, _ticket: &MoveTicket, _reason: &str) -> Result<()> {
        Ok(())
    }
}

fn hot() -> FleetView {
    let mut v = fleet(3);
    for i in 0..17 {
        put(&mut v, 100 + u64::from(i), i, 1, load(5.0, 10.0));
    }
    v
}

fn auto(_: TenantId) -> BalancerMode {
    BalancerMode::Auto
}

#[tokio::test]
async fn auto_mode_schedules_records_and_cools_down() {
    let mut v = hot();
    let p = Planner::new(BalancerSettings::default());
    let exec = Fake::default();
    let log = MemDecisionLog::new();
    let mut s = Scheduler::new("mum1", "ctl-1");
    p.rebalance(&v, 0);
    s.annotate(&mut v, 3600);
    let r = p.rebalance(&v, 3600);
    assert_eq!(r.moves.len(), 1);
    let out = s.schedule(&r, &v, &auto, 4, &exec, &log, 3600).await;
    assert!(out[0].ticket.is_some());
    let m = &r.moves[0];
    let rows = log.of(m.tenant);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].d.kind, "shard.move");
    assert_eq!(rows[0].d.mode, Mode::Applied);
    assert_eq!(rows[0].d.outcome, Outcome::Scheduled);
    assert_eq!(rows[0].d.reason_code, "cluster_hot");
    assert_eq!(rows[0].d.evidence["to"], m.to.to_string());
    // In flight: locked and reserved on the target; the source cools down.
    let mut w = v.clone();
    s.annotate(&mut w, 3700);
    assert!(w.shards[&m.shards[0]].locked);
    assert_eq!(w.active.len(), 1);
    assert_eq!(w.clusters[&cid(1)].last_move_off, Some(3600));
    assert_eq!(w.tenants[&m.tenant].moves_last_day, 1);
    let r2 = p.rebalance(&w, 3700);
    assert!(r2.moves.is_empty());
    // Done: the shard has history and a 24 h cooldown.
    exec.finish_all(&MoveStatus::Succeeded);
    let done = s.poll(&w, &exec, &log, 4000).await;
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].outcome, Outcome::Succeeded);
    assert!(!done[0].reverted);
    assert_eq!(log.of(m.tenant)[0].d.outcome, Outcome::Succeeded);
    v.apply_move(&m.shards, &m.to);
    s.annotate(&mut v, 4000);
    let sv = &v.shards[&m.shards[0]];
    assert_eq!(sv.cooldown_until, 4000 + 86_400);
    assert_eq!(sv.history.len(), 1);
    assert_eq!(sv.history[0].cluster, cid(1));
    assert!(s.in_flight().is_empty());
    assert_eq!(
        s.moves_total[&("cluster_hot".to_owned(), "succeeded".to_owned())],
        1
    );
}

#[tokio::test]
async fn failures_exclude_the_target_and_moves_back_are_counted() {
    let v = hot();
    let exec = Fake::default();
    let log = MemDecisionLog::new();
    let mut s = Scheduler::new("mum1", "ctl-1");
    let k = ShardKey::new(100, 1, 0);
    let plan = |to: u32, from: u32| crate::plan::PlanReport {
        at: 0,
        fullness: std::collections::BTreeMap::new(),
        spread: 0.0,
        trigger: crate::plan::Trigger::Hot,
        hot: Vec::new(),
        warming: Vec::new(),
        moves: vec![PlannedMove {
            tenant: TenantId(100),
            shards: vec![k],
            from: cid(from),
            to: cid(to),
            reason: MoveReason::ClusterHot,
            gain: 0.1,
            value: 0.1,
            size_bytes: 1.0,
            from_full: (0.9, 0.8),
            to_full: (0.1, 0.2),
            alternatives: Vec::new(),
            load: load(5.0, 0.0),
            judgement: None,
            why: String::new(),
            considered: Vec::new(),
        }],
        notes: Vec::new(),
        approvals: Vec::new(),
        stuck: Vec::new(),
        rejected: Vec::new(),
        weights: crate::judge::Weights::default(),
        pinned: Vec::new(),
    };
    s.schedule(&plan(2, 1), &v, &auto, 4, &exec, &log, 100)
        .await;
    exec.finish_all(&MoveStatus::Failed {
        reason: "verify".to_owned(),
    });
    let done = s.poll(&v, &exec, &log, 200).await;
    assert_eq!(done[0].outcome, Outcome::Failed);
    let mut w = v.clone();
    s.annotate(&mut w, 300);
    assert_eq!(w.shards[&k].excluded, vec![(cid(2), 200 + 86_400)]);

    // c1 -> c3, then back to c1 within 7 days: reverted, cooldown doubles.
    let exec = Fake::default();
    s.schedule(&plan(3, 1), &v, &auto, 4, &exec, &log, 1000)
        .await;
    exec.finish_all(&MoveStatus::Succeeded);
    s.poll(&v, &exec, &log, 2000).await;
    let exec = Fake::default();
    s.schedule(&plan(1, 3), &v, &auto, 4, &exec, &log, 200_000)
        .await;
    exec.finish_all(&MoveStatus::Succeeded);
    let done = s.poll(&v, &exec, &log, 200_100).await;
    assert!(done[0].reverted);
    assert_eq!(s.thrash.reverted, 1);
    let mut w = v.clone();
    s.annotate(&mut w, 200_100);
    assert_eq!(w.shards[&k].cooldown_until, 200_100 + 2 * 86_400);
}

#[tokio::test]
async fn recommend_and_dry_run_write_once_and_run_nothing() {
    let mut v = hot();
    let p = Planner::new(BalancerSettings::default());
    let exec = Fake::default();
    let log = MemDecisionLog::new();
    let mut s = Scheduler::new("mum1", "ctl-1");
    p.rebalance(&v, 0);
    s.annotate(&mut v, 3600);
    let r = p.rebalance(&v, 3600);
    let rec = |_: TenantId| BalancerMode::Recommend;
    let out = s.schedule(&r, &v, &rec, 4, &exec, &log, 3600).await;
    assert!(out[0].ticket.is_none());
    assert!(exec.jobs.lock().unwrap().is_empty());
    let t = r.moves[0].tenant;
    assert_eq!(log.of(t)[0].d.mode, Mode::Recommended);
    assert_eq!(log.of(t)[0].d.outcome, Outcome::Proposed);
    // The same recommendation a minute later is not written again.
    let r = p.rebalance(&v, 3660);
    let out = s.schedule(&r, &v, &rec, 4, &exec, &log, 3660).await;
    assert_eq!(out[0].blocked.as_deref(), Some("already_recommended"));
    assert_eq!(log.of(t).len(), 1);
    // Dry run.
    let dry = |_: TenantId| BalancerMode::DryRun;
    let mut s = Scheduler::new("mum1", "ctl-1");
    let log = MemDecisionLog::new();
    s.schedule(&r, &v, &dry, 4, &exec, &log, 3660).await;
    assert_eq!(log.of(t)[0].d.mode, Mode::DryRun);
    // Off: nothing.
    let off = |_: TenantId| BalancerMode::Off;
    let log = MemDecisionLog::new();
    let out = Scheduler::new("mum1", "c")
        .schedule(&r, &v, &off, 4, &exec, &log, 0)
        .await;
    assert_eq!(out[0].blocked.as_deref(), Some("mode_off"));
    assert!(log.all().is_empty());
}

/// The live balancing run wrote the same recommendation after every
/// shard cooldown: with a short cooldown and a plan that
/// doesn't change, it is written once a day; a new target, a new reason or
/// a net value 25% off writes it again at once.
#[tokio::test]
async fn a_recommendation_is_written_again_only_when_it_changes() {
    let mut v = hot();
    for t in v.tenants.values_mut() {
        t.knobs.shard_cooldown_secs = 60;
    }
    let p = Planner::new(BalancerSettings::default());
    let exec = Fake::default();
    let log = MemDecisionLog::new();
    let mut s = Scheduler::new("mum1", "ctl-1");
    p.rebalance(&v, 0);
    s.annotate(&mut v, 3600);
    let r = p.rebalance(&v, 3600);
    let t = r.moves[0].tenant;
    let rec = |_: TenantId| BalancerMode::Recommend;
    s.schedule(&r, &v, &rec, 4, &exec, &log, 3600).await;
    assert_eq!(log.of(t).len(), 1);
    // A run a minute for six hours, the cooldown passing every time.
    for i in 1..=360 {
        let now = 3600 + 60 * i;
        let out = s.schedule(&r, &v, &rec, 4, &exec, &log, now).await;
        assert_eq!(out[0].blocked.as_deref(), Some("already_recommended"));
    }
    assert_eq!(log.of(t).len(), 1, "written once, not every cooldown");
    // A small change in value is the same advice.
    let mut small = r.clone();
    small.moves[0].value *= 1.1;
    s.schedule(&small, &v, &rec, 4, &exec, &log, 30_000).await;
    assert_eq!(log.of(t).len(), 1);
    // The value moved by more than a quarter: written again.
    let mut big = r.clone();
    big.moves[0].value *= 1.5;
    s.schedule(&big, &v, &rec, 4, &exec, &log, 30_060).await;
    assert_eq!(log.of(t).len(), 2);
    // Another target: written again.
    let mut other = big.clone();
    other.moves[0].to = cid(9);
    s.schedule(&other, &v, &rec, 4, &exec, &log, 30_120).await;
    assert_eq!(log.of(t).len(), 3);
    // Unchanged for a day: written again once.
    s.schedule(&other, &v, &rec, 4, &exec, &log, 30_120 + 86_399)
        .await;
    assert_eq!(log.of(t).len(), 3);
    s.schedule(&other, &v, &rec, 4, &exec, &log, 30_120 + 86_400)
        .await;
    assert_eq!(log.of(t).len(), 4);
}

#[tokio::test]
async fn no_mover_fails_the_decision() {
    let mut v = hot();
    let p = Planner::new(BalancerSettings::default());
    let log = MemDecisionLog::new();
    let mut s = Scheduler::new("mum1", "ctl-1");
    p.rebalance(&v, 0);
    s.annotate(&mut v, 3600);
    let r = p.rebalance(&v, 3600);
    let out = s.schedule(&r, &v, &auto, 4, &NoMover, &log, 3600).await;
    assert_eq!(out[0].blocked.as_deref(), Some("submit_failed"));
    assert_eq!(log.of(r.moves[0].tenant)[0].d.outcome, Outcome::Failed);
}

#[tokio::test]
async fn decisions_say_why_a_move_was_made_or_not() {
    // Made: the hot fleet's move reads "moved ... because ...".
    let mut v = hot();
    let p = Planner::new(BalancerSettings::default());
    let exec = Fake::default();
    let log = MemDecisionLog::new();
    let mut s = Scheduler::new("mum1", "ctl-1");
    p.rebalance(&v, 0);
    s.annotate(&mut v, 3600);
    let r = p.rebalance(&v, 3600);
    s.schedule(&r, &v, &auto, 4, &exec, &log, 3600).await;
    let row = &log.of(r.moves[0].tenant)[0];
    assert!(row.d.summary.starts_with("moved t"), "{}", row.d.summary);
    assert!(row.d.summary.contains("because"), "{}", row.d.summary);
    assert!(row.d.evidence["judgement"]["worth"].as_bool().unwrap());
    assert!(
        !row.d.alternatives.as_ref().unwrap()["considered"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    // Not made: one large tenant and one tiny one on c1 at 55%, c2 at 25%
    // (just over the spread). The large one would invert the pair; the
    // tiny one isn't worth its cost. A rejected decision says so, once.
    let mut v = fleet(2);
    put(&mut v, 1, 1, 1, load(54.5, 0.0));
    put(&mut v, 2, 2, 1, load(0.5, 0.0));
    v.clusters.get_mut(&cid(2)).unwrap().background = load(25.0, 0.0);
    let p = Planner::new(BalancerSettings::default());
    let log = MemDecisionLog::new();
    let mut s = Scheduler::new("mum1", "ctl-1");
    p.rebalance(&v, 0);
    let r = p.rebalance(&v, 3600);
    assert!(r.moves.is_empty(), "{:?}", r.moves);
    assert_eq!(r.stuck, vec![cid(1)]);
    assert_eq!(r.rejected.len(), 1);
    s.schedule(&r, &v, &auto, 4, &exec, &log, 3600).await;
    s.schedule(&r, &v, &auto, 4, &exec, &log, 3660).await;
    let rows = log.of(TenantId(2));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].d.outcome, Outcome::Rejected);
    assert_eq!(rows[0].d.reason_code, "not_worth_it");
    assert!(
        rows[0].d.summary.starts_with("did not move t2/g1/s2"),
        "{}",
        rows[0].d.summary
    );
}

#[tokio::test]
async fn pinned_tenants_get_a_rejected_decision_saying_so() {
    let mut v = hot();
    for t in v.tenants.values_mut() {
        t.pinned = true;
    }
    let p = Planner::new(BalancerSettings::default());
    let log = MemDecisionLog::new();
    let mut s = Scheduler::new("mum1", "ctl-1");
    p.rebalance(&v, 0);
    let r = p.rebalance(&v, 3600);
    s.schedule(&r, &v, &auto, 4, &Fake::default(), &log, 3600)
        .await;
    s.schedule(&r, &v, &auto, 4, &Fake::default(), &log, 3660)
        .await;
    let rows = log.of(TenantId(100));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].d.outcome, Outcome::Rejected);
    assert_eq!(rows[0].d.reason_code, "pinned");
    assert!(
        rows[0].d.summary.contains("pinned by operator"),
        "{}",
        rows[0].d.summary
    );
}
