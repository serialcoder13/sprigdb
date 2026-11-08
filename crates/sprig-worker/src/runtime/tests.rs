use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use sprig_common::ids::{DcId, JobId};
use sprig_common::time::{Clock, SystemClock};
use sprig_controller::dispatch::{self, Job, JobClass, JobState, NewJob, Owner};
use sprig_meta::{MemStore, MetaStore, Op, Record, Tier, Txn};

use super::*;

fn store() -> Arc<dyn MetaStore> {
    Arc::new(MemStore::new(Tier::Dc(DcId::new("dc-t").unwrap())))
}

/// Puts a job assigned to `worker`, as dispatch would.
async fn assign(store: &dyn MetaStore, worker: &str, kind: &str, spec: Value) -> JobId {
    let id = JobId::new_v7();
    let mut job = Job::queued(
        id,
        NewJob {
            kind: kind.into(),
            class: JobClass::Other,
            tenant_id: None,
            clusters: vec![],
            priority: 0,
            cause: "operator".into(),
            zone: None,
            spec,
        },
        &SystemClock,
    );
    job.state = JobState::Assigned;
    job.owner = Some(Owner {
        worker_id: worker.into(),
        attempt: 1,
    });
    job.attempts = 1;
    let key = dispatch::job_key(store, id).unwrap();
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key,
                value: Record::new(job).encode().unwrap(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
    id
}

async fn wait_state(store: &dyn MetaStore, id: JobId, want: JobState) -> Job {
    for _ in 0..200 {
        let j = dispatch::get_job(store, id).await.unwrap().unwrap();
        if j.job().state == want {
            return j.record.body;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("job {id} never reached {want:?}");
}

fn runtime(store: &Arc<dyn MetaStore>) -> Arc<WorkerRuntime> {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    Arc::new(WorkerRuntime::new(Arc::clone(store), clock, "w1".into()).with_handler(Arc::new(Noop)))
}

#[tokio::test]
async fn claims_runs_and_finishes_jobs() {
    let store = store();
    let rt = runtime(&store);
    let stop = CancellationToken::new();
    let task = {
        let rt = Arc::clone(&rt);
        let stop = stop.clone();
        tokio::spawn(async move { rt.run(stop).await })
    };
    let ok = assign(store.as_ref(), "w1", "noop", json!({"sleep_ms": 250})).await;
    let bad = assign(store.as_ref(), "w1", "noop", json!({"fail": true})).await;
    let unknown = assign(store.as_ref(), "w1", "frobnicate", json!({})).await;
    let other = assign(store.as_ref(), "w2", "noop", json!({})).await;
    let j = wait_state(store.as_ref(), ok, JobState::Succeeded).await;
    assert_eq!(j.progress, json!({"slept_ms": 250}));
    let j = wait_state(store.as_ref(), bad, JobState::Failed).await;
    assert_eq!(j.error.as_deref(), Some("asked to fail"));
    let j = wait_state(store.as_ref(), unknown, JobState::Failed).await;
    assert!(j.error.unwrap().contains("frobnicate"));
    // Another worker's job is left alone.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        dispatch::get_job(store.as_ref(), other)
            .await
            .unwrap()
            .unwrap()
            .job()
            .state,
        JobState::Assigned
    );
    // The registration is there while it runs, with its slots.
    let live = dispatch::live_workers(store.as_ref()).await.unwrap();
    assert_eq!(live.len(), 1);
    stop.cancel();
    task.await.unwrap().unwrap();
    assert!(
        dispatch::live_workers(store.as_ref())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_job_changed_under_the_worker_stops_at_once() {
    let store = store();
    let rt = runtime(&store);
    let stop = CancellationToken::new();
    let task = {
        let rt = Arc::clone(&rt);
        let stop = stop.clone();
        tokio::spawn(async move { rt.run(stop).await })
    };
    let id = assign(store.as_ref(), "w1", "noop", json!({"sleep_ms": 60_000})).await;
    wait_state(store.as_ref(), id, JobState::Running).await;
    // An operator cancels it: the record changes under the worker.
    let mut j = dispatch::get_job(store.as_ref(), id)
        .await
        .unwrap()
        .unwrap();
    j.record.body.state = JobState::Cancelled;
    j.record.body.owner = None;
    let key = dispatch::job_key(store.as_ref(), id).unwrap();
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key,
                value: j.record.encode().unwrap(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
    for _ in 0..100 {
        if rt.running() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        rt.running(),
        0,
        "the handler stopped at its next progress write"
    );
    assert_eq!(
        dispatch::get_job(store.as_ref(), id)
            .await
            .unwrap()
            .unwrap()
            .job()
            .state,
        JobState::Cancelled
    );
    stop.cancel();
    task.await.unwrap().unwrap();
}
