//! Dispatch tests: single passes on `MemStore` under paused time, and a
//! seeded simulation of the whole scheduler with workers that crash and
//! leaders that change (chapter 23, "Testing": a job has one owner).

// Test suites: failures are panics with a message.
#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::unchecked_time_subtraction,
    clippy::single_match_else
)]

use std::sync::Mutex;

use serde_json::json;
use sprig_meta::elect::{Election, Elector};
use sprig_meta::session::SessionOpts;
use sprig_meta::{MemStore, Tier};
use tokio::time::Instant;

use super::*;
use crate::runtime::tests::{cx_for, mem, paused};
use crate::runtime::{Loop, Runtime, Stores};
use crate::settings::ControlSettings;
use sprig_common::CancellationToken;
use sprig_meta::elect::LeaderRecord;

const TTL: Duration = Duration::from_secs(5);

fn rec(holder: &str) -> LeaderRecord {
    LeaderRecord {
        holder: holder.to_owned(),
        dc: Some("dc-a".to_owned()),
        build: "test".to_owned(),
    }
}

async fn lead(stores: &Stores, clock: &Arc<dyn Clock>) -> Leadership {
    let key = stores.dc.tier().key("leader").unwrap();
    Election::new(stores.dc.clone(), clock.clone(), SessionOpts::new(TTL))
        .campaign(&key, &rec("ctl"))
        .await
        .unwrap()
}

async fn worker(
    stores: &Stores,
    clock: &Arc<dyn Clock>,
    id: &str,
    zone: &str,
    slots: &[(JobClass, u32)],
) -> Session {
    let s = Session::open(stores.dc.clone(), clock.clone(), SessionOpts::new(TTL))
        .await
        .unwrap();
    let reg = Registration {
        address: format!("{id}:7401"),
        zone: zone.to_owned(),
        version: "test".to_owned(),
        load: Value::Null,
        slots: slots.iter().copied().collect(),
    };
    register(&s, RegistrationKind::Worker, id, &reg)
        .await
        .unwrap();
    s
}

fn job(
    kind: &str,
    class: JobClass,
    tenant: Option<u64>,
    clusters: &[&str],
    priority: i32,
    cause: &str,
) -> NewJob {
    NewJob {
        kind: kind.to_owned(),
        class,
        tenant_id: tenant.map(TenantId),
        clusters: clusters
            .iter()
            .map(|c| ClusterId::new(*c).unwrap())
            .collect(),
        priority,
        cause: cause.to_owned(),
        zone: None,
        spec: json!({}),
    }
}

async fn state(store: &dyn MetaStore, id: JobId) -> (JobState, Option<String>) {
    let j = get_job(store, id).await.unwrap().unwrap();
    (
        j.job().state,
        j.job().owner.as_ref().map(|o| o.worker_id.clone()),
    )
}

#[test]
fn assigns_by_priority_within_cluster_slots() {
    paused().block_on(async {
        let (_s, stores, clock) = mem("dc-a");
        let l = lead(&stores, &clock).await;
        let _w = worker(&stores, &clock, "w1", "z1", &[(JobClass::Copy, 8)]).await;
        let low = submit(
            &l,
            &*clock,
            job(
                "copy",
                JobClass::Copy,
                Some(1),
                &["dc-a-c1"],
                0,
                "placement",
            ),
        )
        .await
        .unwrap();
        let high = submit(
            &l,
            &*clock,
            job(
                "copy",
                JobClass::Copy,
                Some(2),
                &["dc-a-c1"],
                5,
                "placement",
            ),
        )
        .await
        .unwrap();
        let mid = submit(
            &l,
            &*clock,
            job(
                "copy",
                JobClass::Copy,
                Some(3),
                &["dc-a-c1", "dc-a-c2"],
                1,
                "placement",
            ),
        )
        .await
        .unwrap();
        let elsewhere = submit(
            &l,
            &*clock,
            job(
                "copy",
                JobClass::Copy,
                Some(4),
                &["dc-a-c3"],
                0,
                "placement",
            ),
        )
        .await
        .unwrap();
        let (cx, _stop) = cx_for(&stores, &clock, Some(l.clone()), ControlSettings::default());

        // mover.max_copies_per_cluster = 2: the two highest on c1 go first.
        let r = dispatch_pass(&cx).await.unwrap();
        let assigned: Vec<JobId> = r.assigned.iter().map(|(j, _)| *j).collect();
        assert_eq!(assigned, vec![high, mid, elsewhere]);
        assert_eq!(state(&*stores.dc, low).await.0, JobState::Queued);

        // The worker claims and finishes one; the slot frees.
        let mine = assigned_to(&*stores.dc, "w1").await.unwrap();
        assert_eq!(mine.len(), 3);
        let seen = mine.into_iter().find(|j| j.job().job_id == high).unwrap();
        let c = claim(&*stores.dc, "w1", seen).await.unwrap().unwrap();
        assert_eq!(state(&*stores.dc, high).await.0, JobState::Running);
        assert_eq!(
            finish(&*stores.dc, &*clock, c, Ok(json!({"rows": 10})))
                .await
                .unwrap(),
            Ownership::Held
        );
        let r = dispatch_pass(&cx).await.unwrap();
        assert_eq!(r.assigned, vec![(low, "w1".to_owned())]);
    });
}

#[test]
fn workers_need_free_slots_and_prefer_their_zone() {
    paused().block_on(async {
        let (_s, stores, clock) = mem("dc-a");
        let l = lead(&stores, &clock).await;
        let _a = worker(
            &stores,
            &clock,
            "wa",
            "z1",
            &[(JobClass::Build, 1), (JobClass::Other, 4)],
        )
        .await;
        let _b = worker(&stores, &clock, "wb", "z2", &[(JobClass::Other, 4)]).await;
        let (cx, _stop) = cx_for(&stores, &clock, Some(l.clone()), ControlSettings::default());

        let mut z2 = job("validate", JobClass::Other, None, &[], 0, "operator");
        z2.zone = Some("z2".to_owned());
        let z2 = submit(&l, &*clock, z2).await.unwrap();
        let b1 = submit(
            &l,
            &*clock,
            job(
                "index_build",
                JobClass::Build,
                Some(1),
                &["c1"],
                0,
                "auto_index",
            ),
        )
        .await
        .unwrap();
        let b2 = submit(
            &l,
            &*clock,
            job(
                "index_build",
                JobClass::Build,
                Some(2),
                &["c2"],
                0,
                "auto_index",
            ),
        )
        .await
        .unwrap();
        let o1 = submit(
            &l,
            &*clock,
            job("validate", JobClass::Other, None, &[], 0, "operator"),
        )
        .await
        .unwrap();
        let r = dispatch_pass(&cx).await.unwrap();
        let by: HashMap<JobId, String> = r.assigned.into_iter().collect();
        assert_eq!(by[&z2], "wb", "same zone first");
        assert_eq!(by[&b1], "wa", "only wa has build slots");
        assert!(!by.contains_key(&b2), "wa's one build slot is taken");
        // Fewest jobs, then lowest id: wa holds one, wb holds one; "wa" < "wb".
        assert_eq!(by[&o1], "wa");
    });
}

/// r9triggers: long-running consumers (stream sequencers, triggers on
/// streams and queues) have their own pool, so a ninth stream's sequencer
/// is assigned on a worker whose eight `Other` slots are all taken.
#[test]
fn consumer_jobs_have_their_own_pool() {
    paused().block_on(async {
        let (_s, stores, clock) = mem("dc-a");
        let l = lead(&stores, &clock).await;
        let _w = worker(
            &stores,
            &clock,
            "w1",
            "z1",
            &[(JobClass::Other, 8), (JobClass::Consumer, CONSUMER_SLOTS)],
        )
        .await;
        let (cx, _stop) = cx_for(&stores, &clock, Some(l.clone()), ControlSettings::default());
        let mut others = Vec::new();
        for _ in 0..9 {
            others.push(
                submit(
                    &l,
                    &*clock,
                    job("validate", JobClass::Other, None, &[], 0, "operator"),
                )
                .await
                .unwrap(),
            );
        }
        let mut consumers = Vec::new();
        for _ in 0..20 {
            consumers.push(
                submit(
                    &l,
                    &*clock,
                    job(
                        "stream_sequencer",
                        JobClass::Consumer,
                        None,
                        &[],
                        0,
                        "streams",
                    ),
                )
                .await
                .unwrap(),
            );
        }
        let r = dispatch_pass(&cx).await.unwrap();
        let by: HashMap<JobId, String> = r.assigned.into_iter().collect();
        assert_eq!(
            others.iter().filter(|j| by.contains_key(j)).count(),
            8,
            "eight Other slots"
        );
        assert!(
            consumers.iter().all(|j| by.contains_key(j)),
            "every sequencer is assigned whatever the Other slots hold"
        );
    });
}

/// R5 round 6: search copies go only to workers with the search role,
/// and the copies of one index (one `spread` key) to different workers,
/// however loaded the other worker is.
#[test]
fn search_copies_spread_over_search_workers() {
    paused().block_on(async {
        let (_s, stores, clock) = mem("dc-a");
        let l = lead(&stores, &clock).await;
        let _w0 = worker(&stores, &clock, "w0", "z1", &[(JobClass::Other, 8)]).await;
        let _w1 = worker(
            &stores,
            &clock,
            "w1",
            "z1",
            &[(JobClass::Search, SEARCH_SLOTS)],
        )
        .await;
        let _w2 = worker(
            &stores,
            &clock,
            "w2",
            "z1",
            &[(JobClass::Search, SEARCH_SLOTS)],
        )
        .await;
        let (cx, _stop) = cx_for(&stores, &clock, Some(l.clone()), ControlSettings::default());
        let copy = |index: u64, n: u32| {
            let mut j = job("search_copy", JobClass::Search, None, &[], 0, "search");
            j.spec = json!({"tenant": 7, "index": index, "copy": n, "spread": format!("search:7:{index}")});
            j
        };
        // Index 1's copy 0 first: w1 (lowest id), then w2 holds fewer jobs.
        let a0 = submit(&l, &*clock, copy(1, 0)).await.unwrap();
        let b0 = submit(&l, &*clock, copy(2, 0)).await.unwrap();
        let c0 = submit(&l, &*clock, copy(3, 0)).await.unwrap();
        let r = dispatch_pass(&cx).await.unwrap();
        let first: HashMap<JobId, String> = r.assigned.into_iter().collect();
        assert_eq!(first.len(), 3, "{first:?}");
        assert!(first.values().all(|w| w != "w0"), "w0 has no search slots");
        // Each second copy lands on the worker without the first, even when
        // that worker holds more jobs.
        let a1 = submit(&l, &*clock, copy(1, 1)).await.unwrap();
        let b1 = submit(&l, &*clock, copy(2, 1)).await.unwrap();
        let c1 = submit(&l, &*clock, copy(3, 1)).await.unwrap();
        let r = dispatch_pass(&cx).await.unwrap();
        let second: HashMap<JobId, String> = r.assigned.into_iter().collect();
        for (x0, x1) in [(a0, a1), (b0, b1), (c0, c1)] {
            assert_ne!(first[&x0], second[&x1], "two copies of one index on one worker");
        }
    });
}

#[test]
fn lost_owners_requeue_with_backoff_then_fail() {
    paused().block_on(async {
        let (_s, stores, clock) = mem("dc-a");
        let l = lead(&stores, &clock).await;
        let settings = ControlSettings {
            job_max_attempts: 2,
            ..ControlSettings::default()
        };
        let (cx, _stop) = cx_for(&stores, &clock, Some(l.clone()), settings);
        let id = submit(
            &l,
            &*clock,
            job("copy", JobClass::Copy, Some(1), &["c1"], 0, "placement"),
        )
        .await
        .unwrap();

        for attempt in 1..=2u32 {
            let w = worker(
                &stores,
                &clock,
                &format!("w{attempt}"),
                "z",
                &[(JobClass::Copy, 1)],
            )
            .await;
            // Wait out any backoff from the previous loss.
            tokio::time::sleep(Duration::from_secs(1)).await;
            let r = dispatch_pass(&cx).await.unwrap();
            assert_eq!(r.assigned.len(), 1, "attempt {attempt}");
            let j = get_job(&*stores.dc, id).await.unwrap().unwrap();
            assert_eq!(j.job().owner.as_ref().unwrap().attempt, attempt);
            let c = claim(&*stores.dc, &format!("w{attempt}"), j)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(c.job.job().state, JobState::Running);
            w.close().await.unwrap();
            let r = dispatch_pass(&cx).await.unwrap();
            if attempt < 2 {
                assert_eq!(r.requeued, vec![id]);
                let j = get_job(&*stores.dc, id).await.unwrap().unwrap();
                assert_eq!((j.job().state, j.job().attempts), (JobState::Queued, 1));
                assert!(j.job().not_before_ms > unix_ms(clock.wall()));
                assert!(r.next_not_before_ms.is_some());
            } else {
                assert_eq!(r.failed, vec![id]);
            }
        }
        let j = get_job(&*stores.dc, id).await.unwrap().unwrap();
        assert_eq!(j.job().state, JobState::Failed);
        assert!(
            j.job()
                .error
                .as_deref()
                .unwrap()
                .contains("job_max_attempts")
        );
    });
}

#[test]
fn a_reassigned_or_cancelled_job_stops_its_old_worker() {
    paused().block_on(async {
        let (_s, stores, clock) = mem("dc-a");
        let l = lead(&stores, &clock).await;
        let (cx, _stop) = cx_for(&stores, &clock, Some(l.clone()), ControlSettings::default());
        let id = submit(
            &l,
            &*clock,
            job("copy", JobClass::Copy, Some(1), &["c1"], 0, "placement"),
        )
        .await
        .unwrap();
        let w1 = worker(&stores, &clock, "w1", "z", &[(JobClass::Copy, 1)]).await;
        dispatch_pass(&cx).await.unwrap();
        let seen = get_job(&*stores.dc, id).await.unwrap().unwrap();
        let mut c1 = claim(&*stores.dc, "w1", seen.clone())
            .await
            .unwrap()
            .unwrap();
        // A second claim from the same stale view loses.
        assert!(claim(&*stores.dc, "w1", seen).await.unwrap().is_none());
        assert_eq!(
            report_progress(&*stores.dc, &mut c1, json!({"pct": 10}))
                .await
                .unwrap(),
            Ownership::Held
        );

        // w1 looks dead to the leader (its registration is gone) while it
        // still runs: the job moves to w2, and w1's next write fails.
        w1.close().await.unwrap();
        let _w2 = worker(&stores, &clock, "w2", "z", &[(JobClass::Copy, 1)]).await;
        dispatch_pass(&cx).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let r = dispatch_pass(&cx).await.unwrap();
        assert_eq!(r.assigned, vec![(id, "w2".to_owned())]);
        assert_eq!(
            report_progress(&*stores.dc, &mut c1, json!({"pct": 20}))
                .await
                .unwrap(),
            Ownership::Lost
        );
        assert_eq!(
            c1.job.job().progress,
            json!({"pct": 10}),
            "local view keeps the last committed progress"
        );

        let seen = get_job(&*stores.dc, id).await.unwrap().unwrap();
        let mut c2 = claim(&*stores.dc, "w2", seen).await.unwrap().unwrap();
        assert!(cancel(&l, &*clock, id).await.unwrap());
        assert!(!cancel(&l, &*clock, id).await.unwrap(), "already final");
        assert_eq!(
            report_progress(&*stores.dc, &mut c2, json!({"pct": 30}))
                .await
                .unwrap(),
            Ownership::Lost
        );
        assert_eq!(state(&*stores.dc, id).await, (JobState::Cancelled, None));
    });
}

#[test]
fn tenant_rules_rate_and_exclusion() {
    paused().block_on(async {
        let (_s, stores, clock) = mem("dc-a");
        let l = lead(&stores, &clock).await;
        let settings = ControlSettings {
            tenant_jobs_per_hour: 2,
            ..ControlSettings::default()
        };
        let (cx, _stop) = cx_for(&stores, &clock, Some(l.clone()), settings);
        let _w = worker(
            &stores,
            &clock,
            "w",
            "z",
            &[
                (JobClass::Other, 50),
                (JobClass::Build, 50),
                (JobClass::Copy, 50),
            ],
        )
        .await;
        let t = Some(7);
        let a = submit(
            &l,
            &*clock,
            job("validate", JobClass::Other, t, &[], 0, "advisor"),
        )
        .await
        .unwrap();
        let b = submit(
            &l,
            &*clock,
            job("validate", JobClass::Other, t, &[], 0, "advisor"),
        )
        .await
        .unwrap();
        let c = submit(
            &l,
            &*clock,
            job("validate", JobClass::Other, t, &[], 0, "advisor"),
        )
        .await
        .unwrap();
        let op = submit(
            &l,
            &*clock,
            job("validate", JobClass::Other, t, &[], 0, OPERATOR),
        )
        .await
        .unwrap();
        let r = dispatch_pass(&cx).await.unwrap();
        let ids: Vec<JobId> = r.assigned.iter().map(|x| x.0).collect();
        assert_eq!(ids, vec![a, b, op], "the operator's job skips the rate");
        assert_eq!(r.rate_limited, vec![c]);
        // An hour later the rate allows it.
        tokio::time::sleep(Duration::from_secs(3601)).await;
        let r = dispatch_pass(&cx).await.unwrap();
        assert_eq!(r.assigned.iter().map(|x| x.0).collect::<Vec<_>>(), vec![c]);

        // One exclusive job per tenant, and nothing else beside it.
        let u = Some(8);
        let m1 = submit(
            &l,
            &*clock,
            job("migrate", JobClass::Copy, u, &["c1"], 0, OPERATOR),
        )
        .await
        .unwrap();
        let m2 = submit(
            &l,
            &*clock,
            job("promote", JobClass::Other, u, &[], 0, OPERATOR),
        )
        .await
        .unwrap();
        let other = submit(
            &l,
            &*clock,
            job("validate", JobClass::Other, u, &[], 0, OPERATOR),
        )
        .await
        .unwrap();
        let r = dispatch_pass(&cx).await.unwrap();
        let ids: Vec<JobId> = r.assigned.iter().map(|x| x.0).collect();
        assert_eq!(ids, vec![m1]);
        assert_eq!(state(&*stores.dc, m2).await.0, JobState::Queued);
        assert_eq!(state(&*stores.dc, other).await.0, JobState::Queued);

        // No index build while a copy runs for the tenant.
        let v = Some(9);
        let copy = submit(
            &l,
            &*clock,
            job("copy", JobClass::Copy, v, &["c1"], 0, "placement"),
        )
        .await
        .unwrap();
        let build = submit(
            &l,
            &*clock,
            job("index_build", JobClass::Build, v, &["c2"], 0, "auto_index"),
        )
        .await
        .unwrap();
        let r = dispatch_pass(&cx).await.unwrap();
        assert_eq!(
            r.assigned.iter().map(|x| x.0).collect::<Vec<_>>(),
            vec![copy]
        );
        assert_eq!(state(&*stores.dc, build).await.0, JobState::Queued);
    });
}

#[test]
fn a_deposed_leader_assigns_nothing() {
    paused().block_on(async {
        let (s, stores, clock) = mem("dc-a");
        let l = lead(&stores, &clock).await;
        let (cx, _stop) = cx_for(&stores, &clock, Some(l.clone()), ControlSettings::default());
        let id = submit(
            &l,
            &*clock,
            job("copy", JobClass::Copy, Some(1), &["c1"], 0, "placement"),
        )
        .await
        .unwrap();
        let _w = worker(&stores, &clock, "w", "z", &[(JobClass::Copy, 1)]).await;
        // Another controller takes over behind this one's back.
        let key = s.tier().key("leader").unwrap();
        s.txn(Txn {
            when: vec![],
            then: vec![Op::Delete(key)],
            otherwise: vec![],
        })
        .await
        .unwrap();
        let _other = lead(&stores, &clock).await;
        let e = dispatch_pass(&cx).await.unwrap_err();
        assert_eq!(e.code(), Some(sprig_common::SprigCode::NotLeader));
        assert!(!l.is_leading());
        assert_eq!(state(&*stores.dc, id).await, (JobState::Queued, None));
    });
}

#[test]
fn gc_deletes_finished_jobs_after_seven_days() {
    paused().block_on(async {
        let (_s, stores, clock) = mem("dc-a");
        let l = lead(&stores, &clock).await;
        let (cx, _stop) = cx_for(&stores, &clock, Some(l.clone()), ControlSettings::default());
        let done = submit(
            &l,
            &*clock,
            job("validate", JobClass::Other, None, &[], 0, OPERATOR),
        )
        .await
        .unwrap();
        let open = submit(
            &l,
            &*clock,
            job("validate", JobClass::Other, None, &[], 0, OPERATOR),
        )
        .await
        .unwrap();
        assert!(cancel(&l, &*clock, done).await.unwrap());
        let gc = JobGc;
        assert_eq!(gc.all_keys(&cx).await.unwrap(), vec![done]);
        let Outcome::RequeueAfter(wait) = gc.reconcile(&cx, &done).await.unwrap() else {
            panic!("a fresh job waits");
        };
        assert!(wait > JOB_RETENTION - Duration::from_secs(1));
        assert_eq!(gc.reconcile(&cx, &open).await.unwrap(), Outcome::Done);
        // Age the finished job by eight days.
        let mut j = get_job(&*stores.dc, done).await.unwrap().unwrap();
        let eight_days = 8 * 24 * 3_600_000;
        j.record.body.finished_ms = Some(unix_ms(clock.wall()) - eight_days);
        let key = job_key(&*stores.dc, done).unwrap();
        stores
            .dc
            .txn(Txn {
                when: vec![],
                then: vec![put_job(key, &j.record).unwrap()],
                otherwise: vec![],
            })
            .await
            .unwrap();
        assert!(l.is_leading());
        assert_eq!(gc.reconcile(&cx, &done).await.unwrap(), Outcome::Done);
        assert!(get_job(&*stores.dc, done).await.unwrap().is_none());
        assert!(get_job(&*stores.dc, open).await.unwrap().is_some());
    });
}

/// Records which (worker, attempt) committed each job write, in commit
/// order.
type Commits = Arc<Mutex<HashMap<JobId, Vec<(String, u32)>>>>;

/// A simulated worker: claims what is assigned to it, writes progress
/// ten times a second apart, then finishes. Stops on `stop` or a lost write.
async fn run_worker(
    stores: Stores,
    clock: Arc<dyn Clock>,
    id: String,
    commits: Commits,
    stop: CancellationToken,
) {
    let Ok(session) = Session::open(stores.dc.clone(), clock.clone(), SessionOpts::new(TTL)).await
    else {
        return;
    };
    let reg = Registration {
        address: id.clone(),
        zone: "z".to_owned(),
        version: "test".to_owned(),
        load: Value::Null,
        slots: [(JobClass::Copy, 2)].into_iter().collect(),
    };
    if register(&session, RegistrationKind::Worker, &id, &reg)
        .await
        .is_err()
    {
        return;
    }
    let store = &*stores.dc;
    while !stop.is_cancelled() && !session.is_lost() {
        for seen in assigned_to(store, &id).await.unwrap_or_default() {
            let Ok(Some(mut c)) = claim(store, &id, seen).await else {
                continue;
            };
            let attempt = c.job.job().owner.as_ref().map_or(0, |o| o.attempt);
            let job_id = c.job.job().job_id;
            let mut lost = false;
            for step in 1..=10 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if stop.is_cancelled() {
                    return; // a crash: no more writes
                }
                match report_progress(store, &mut c, json!({ "step": step })).await {
                    Ok(Ownership::Held) => commits
                        .lock()
                        .unwrap()
                        .entry(job_id)
                        .or_default()
                        .push((id.clone(), attempt)),
                    _ => {
                        lost = true;
                        break;
                    }
                }
            }
            if !lost
                && finish(store, &*clock, c, Ok(json!({"done": true})))
                    .await
                    .is_ok_and(|o| o == Ownership::Held)
            {
                commits
                    .lock()
                    .unwrap()
                    .entry(job_id)
                    .or_default()
                    .push((id.clone(), attempt));
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The whole scheduler in simulation: two controllers, three workers that
/// crash and come back, a leader key deleted now and then. Properties: a
/// job has one owner at a time (once a later attempt commits, no earlier
/// attempt does; one worker per attempt), and every job ends `succeeded`.
#[test]
fn simulation_a_job_has_one_owner() {
    let mut retried = 0;
    for seed in 1..=3u64 {
        retried += paused().block_on(async move {
            let mut retried = 0;
            let (s, stores, clock) = mem("dc-a");
            let commits: Commits = Arc::default();
            // Submit before the controllers start, under a leadership of
            // our own that then resigns.
            let l = lead(&stores, &clock).await;
            let mut ids = Vec::new();
            for i in 0..24u64 {
                let cluster = ["c1", "c2", "c3"][usize::try_from(i % 3).unwrap()];
                let j = job("copy", JobClass::Copy, Some(i % 4), &[cluster], 0, OPERATOR);
                ids.push(submit(&l, &*clock, j).await.unwrap());
            }
            l.resign().await.unwrap();
            let controllers: Vec<_> = ["c1", "c2"]
                .iter()
                .map(|h| {
                    let stop = CancellationToken::new();
                    let rt = Runtime::new(stores.clone(), clock.clone(), rec(h))
                        .with_loop(Loop::new(Dispatch))
                        .with_loop(Loop::new(JobGc));
                    (stop.clone(), tokio::spawn(rt.run(stop)))
                })
                .collect();
            let mut rng = crate::runtime::queue::Jitter::new(seed);
            let mut workers: Vec<(CancellationToken, tokio::task::JoinHandle<()>)> = Vec::new();
            let mut n = 0;
            let spawn_worker = |n: &mut u32| {
                *n += 1;
                let stop = CancellationToken::new();
                let h = tokio::spawn(run_worker(
                    stores.clone(),
                    clock.clone(),
                    format!("w{n}"),
                    commits.clone(),
                    stop.clone(),
                ));
                (stop, h)
            };
            for _ in 0..3 {
                workers.push(spawn_worker(&mut n));
            }
            let t0 = Instant::now();
            while t0.elapsed() < Duration::from_secs(120) {
                tokio::time::sleep(rng.apply(Duration::from_secs(2))).await;
                match rng.apply(Duration::from_secs(10)).as_millis() % 5 {
                    0 => {
                        // A worker crashes; its session ends on its TTL.
                        let (stop, h) = workers.remove(0);
                        stop.cancel();
                        h.abort();
                        workers.push(spawn_worker(&mut n));
                    }
                    1 => {
                        let key = s.tier().key("leader").unwrap();
                        let _ = s
                            .txn(Txn {
                                when: vec![],
                                then: vec![Op::Delete(key)],
                                otherwise: vec![],
                            })
                            .await;
                    }
                    _ => {}
                }
                let all_done =
                    futures::future::join_all(ids.iter().map(|id| get_job(&*stores.dc, *id)))
                        .await
                        .into_iter()
                        .all(|j| {
                            j.ok()
                                .flatten()
                                .is_some_and(|j| j.job().state == JobState::Succeeded)
                        });
                if all_done && t0.elapsed() > Duration::from_secs(30) {
                    break;
                }
            }
            // Let the survivors finish.
            for _ in 0..600 {
                let states =
                    futures::future::join_all(ids.iter().map(|id| get_job(&*stores.dc, *id))).await;
                if states.iter().all(|j| {
                    j.as_ref()
                        .ok()
                        .and_then(Option::as_ref)
                        .is_some_and(|j| j.job().state == JobState::Succeeded)
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            for id in &ids {
                let j = get_job(&*stores.dc, *id).await.unwrap().unwrap();
                retried += usize::from(j.job().attempts > 1);
                assert_eq!(
                    j.job().state,
                    JobState::Succeeded,
                    "seed {seed}: {:?}",
                    j.job()
                );
                let c = commits.lock().unwrap().get(id).cloned().unwrap_or_default();
                assert!(
                    c.windows(2).all(|w| w[0].1 <= w[1].1),
                    "seed {seed}: attempts went back: {c:?}"
                );
                let mut by_attempt: HashMap<u32, HashSet<String>> = HashMap::new();
                for (w, a) in &c {
                    by_attempt.entry(*a).or_default().insert(w.clone());
                }
                assert!(
                    by_attempt.values().all(|ws| ws.len() == 1),
                    "seed {seed}: two owners: {c:?}"
                );
            }
            for (stop, h) in workers {
                stop.cancel();
                h.abort();
            }
            for (stop, h) in controllers {
                stop.cancel();
                h.await.unwrap();
            }
            retried
        });
    }
    assert!(retried > 0, "the schedule must make some jobs change owner");
}

#[test]
fn keys_for_skips_progress_of_running_jobs() {
    let d = Dispatch;
    let store = MemStore::new(Tier::Dc(sprig_common::ids::DcId::new("dc-a").unwrap()));
    let key = store.tier().key("jobs/x").unwrap();
    let mut j = Job {
        job_id: JobId::new_v7(),
        kind: "copy".to_owned(),
        class: JobClass::Copy,
        tenant_id: None,
        clusters: vec![],
        priority: 0,
        state: JobState::Running,
        owner: None,
        attempts: 1,
        not_before_ms: 0,
        cause: OPERATOR.to_owned(),
        zone: None,
        created_at: String::new(),
        started_ms: None,
        finished_ms: None,
        spec: Value::Null,
        progress: Value::Null,
        error: None,
    };
    let ev = |j: &Job| MetaEvent {
        source: crate::runtime::Source::Dc,
        key: key.clone(),
        kv: Some(KeyValue {
            key: key.clone(),
            value: Record::new(j.clone()).encode().unwrap(),
            create_revision: 2,
            mod_revision: 3,
            version: 2,
            lease: None,
        }),
        mod_revision: 3,
    };
    assert!(d.keys_for(&ev(&j)).is_empty());
    j.state = JobState::Succeeded;
    assert_eq!(d.keys_for(&ev(&j)), vec![Pass]);
    assert_eq!(JobGc.keys_for(&ev(&j)), vec![j.job_id]);
    let gone = MetaEvent { kv: None, ..ev(&j) };
    assert_eq!(d.keys_for(&gone), vec![Pass]);
    assert!(JobGc.keys_for(&gone).is_empty());
}
