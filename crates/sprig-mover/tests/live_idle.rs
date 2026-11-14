//! An idle tenant moves, and cancel works in every state.
//!
//! Needs the Compose stack with its three clusters registered in etcd
//! (`deploy/compose/gateway-dev.sh up`):
//!
//! ```sh
//! SPRIG_TEST_MOVER_LIVE=1 cargo test -p sprig-mover --test live_idle -- --nocapture
//! ```
//!
//! A tenant nobody writes to (no client, no gateway) moves around the
//! three topologies, c1 → c2 → c3 → c1; each move must commit within a
//! minute. Then the same tenant's moves are cancelled in each state a
//! cancel can meet before the commit: `planned`, `copying` while the job
//! waits for a copy slot (another job holds the target's), `catching_up`,
//! `verifying` and `ready` (a manual cutover); each must end `rolled_back`
//! with the tenant still on its source, within 30 s.
//!
//! The tenant id comes from `SPRIG_TEST_TENANT_BASE` (default
//! 3,530,600,000, agent r0fix's range) plus the process id modulo 1,000;
//! the test drops only its own schemas, jobs and map by exact name.

#![allow(clippy::unwrap_used, clippy::too_many_lines, clippy::similar_names)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_catalog::placement::ShardMap;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::CancellationToken;
use sprig_common::ids::{ClusterId, DcId, JobId, TenantId};
use sprig_common::time::SystemClock;
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Op, Record, Tier, Txn};
use sprig_mover::mysql::MysqlEnvFactory;
use sprig_mover::{
    CutoverMode, JobRuntime, JobSpec, JobState, JobStatus, JobStore, MetaMover, Mover,
    MoverSettings, VerifyMode, WorkerOpts,
};
use sprig_shard::{Placement, TenantMaps};

const ROOT_PW: &str = "sprig-dev-root";
const CLUSTERS: [&str; 3] = ["dc-a-c1", "dc-a-c2", "dc-a-c3"];

fn cluster_url(c: &str) -> &'static str {
    match c {
        "dc-a-c1" => "127.0.0.1:13301",
        "dc-a-c2" => "127.0.0.1:13311",
        _ => "127.0.0.1:13321",
    }
}

fn pool(c: &str) -> Pool {
    let opts = Opts::from_url(&format!("mysql://root:{ROOT_PW}@{}", cluster_url(c))).unwrap();
    Pool::new(
        OptsBuilder::from_opts(opts).pool_opts(
            PoolOpts::new()
                .with_constraints(PoolConstraints::new(0, 2).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    )
}

async fn setup(src: &Pool, tenant: TenantId) -> Vec<String> {
    let cat = format!("t{tenant}_sprig");
    let data = format!("t{tenant}_d1_g1_s1");
    let mut c = src.get_conn().await.unwrap();
    for s in [&cat, &data] {
        c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE `{s}`"))
            .await
            .unwrap();
    }
    c.query_drop(format!("USE `{data}`")).await.unwrap();
    c.query_drop(format!(
        "CREATE TABLE `{cat}`.meta (k VARCHAR(64) PRIMARY KEY, v JSON NOT NULL)"
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO `{cat}`.meta VALUES ('a', '{{\"x\": 1}}')"
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "CREATE TABLE `{data}`.items (id INT PRIMARY KEY, v VARCHAR(40) NOT NULL)"
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO `{data}`.items SELECT seq, CONCAT('v', seq) FROM seq_1_to_2000"
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "CREATE TABLE `{data}`.empty_one (id INT PRIMARY KEY)"
    ))
    .await
    .unwrap();
    vec![cat, data]
}

async fn put_map(dc: &Arc<dyn MetaStore>, tenant: TenantId, cluster: &str) {
    let key = ShardMap::catalog_key(dc.tier(), tenant).unwrap();
    let m = ShardMap::catalog(tenant, ClusterId::new(cluster).unwrap(), 1);
    dc.txn(Txn {
        when: vec![],
        then: vec![Op::Put {
            key,
            value: Record::new(m).encode().unwrap(),
            lease: None,
        }],
        otherwise: vec![],
    })
    .await
    .unwrap();
}

async fn items(pool: &Pool, data: &str) -> (u64, String) {
    let mut c = pool.get_conn().await.unwrap();
    let r: Option<(u64, String)> = c
        .query_first(format!(
            "SELECT COUNT(*), CAST(COALESCE(SUM(CRC32(CONCAT(id, v))), 0) AS CHAR) FROM `{data}`.items"
        ))
        .await
        .unwrap();
    r.unwrap()
}

/// Polls `id` until `done(state)` or `limit`; returns the status and the
/// states it saw.
async fn wait_for(
    mover: &MetaMover,
    id: &JobId,
    limit: Duration,
    done: impl Fn(JobState) -> bool,
) -> (JobStatus, Vec<JobState>) {
    let t0 = Instant::now();
    let mut seen: Vec<JobState> = Vec::new();
    loop {
        if let Ok(st) = mover.status(id).await {
            if seen.last() != Some(&st.state) {
                seen.push(st.state);
            }
            if done(st.state) {
                return (st, seen);
            }
            assert!(
                t0.elapsed() < limit,
                "job {} stuck after {limit:?}: {st:?}; states {seen:?}",
                id.0
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_tenant_moves_and_cancel_works_in_every_state() {
    if std::env::var("SPRIG_TEST_MOVER_LIVE").is_err() {
        eprintln!("skipped: set SPRIG_TEST_MOVER_LIVE=1 with the Compose stack up");
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "sprig_mover=info,warn".into()),
        )
        .with_test_writer()
        .try_init();
    let etcd = std::env::var("SPRIG_TEST_ETCD").unwrap_or_else(|_| "http://127.0.0.1:2379".into());
    let cfg = EtcdConfig::new(vec![etcd]);
    let dc_id = DcId::new("dc-a").unwrap();
    let dc: Arc<dyn MetaStore> = Arc::new(EtcdStore::connect(Tier::Dc(dc_id), &cfg).await.unwrap());
    let global: Arc<dyn MetaStore> =
        Arc::new(EtcdStore::connect(Tier::Global, &cfg).await.unwrap());
    let registry = ClusterRegistry::new(global, dc.clone()).unwrap();
    let base: u64 = std::env::var("SPRIG_TEST_TENANT_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3_530_600_000);
    let tenant = TenantId(base + u64::from(std::process::id() % 1000));
    let pools: BTreeMap<&str, Pool> = CLUSTERS.iter().map(|c| (*c, pool(c))).collect();
    let schemas = setup(&pools["dc-a-c1"], tenant).await;
    let data = schemas[1].clone();
    put_map(&dc, tenant, "dc-a-c1").await;
    let expected = items(&pools["dc-a-c1"], &data).await;
    eprintln!("tenant {tenant}: {} rows in {data}", expected.0);

    let settings = MoverSettings {
        ready_sustain: Duration::from_secs(1),
        cutover_retry: Duration::from_secs(1),
        source_retain: Duration::from_secs(1),
        ..MoverSettings::default()
    };
    let maps = TenantMaps::new(dc.clone());
    let factory = Arc::new(MysqlEnvFactory::new(registry, maps.clone(), settings));
    let store = JobStore::new(dc.clone(), Arc::new(SystemClock));
    let mut opts = WorkerOpts::new("r0fix-idle");
    opts.scan_interval = Duration::from_millis(200);
    opts.lease = Duration::from_secs(10);
    let stop = CancellationToken::new();
    let worker =
        tokio::spawn(JobRuntime::new(store.clone(), factory, opts).run(stop.child_token()));
    let mover = MetaMover::new(store);
    let jobs: std::sync::Mutex<Vec<JobId>> = std::sync::Mutex::new(Vec::new());
    let body = std::panic::AssertUnwindSafe(async {
        let mut report = Vec::new();

        // 1. Idle moves around the three topologies.
        let route = ["dc-a-c1", "dc-a-c2", "dc-a-c3", "dc-a-c1"];
        for pair in route.windows(2) {
            let (from, to) = (pair[0], pair[1]);
            let t0 = Instant::now();
            let id = mover
                .submit(JobSpec::move_tenant(
                    tenant,
                    ClusterId::new(from).unwrap(),
                    ClusterId::new(to).unwrap(),
                ))
                .await
                .unwrap();
            jobs.lock().unwrap().push(id);
            // A shared dev worker may claim the job: its `mover.ready_sustain_seconds`
            // is the default 60.
            let (st, seen) = wait_for(&mover, &id, Duration::from_secs(180), |s| {
                s.is_committed() || s.is_terminal() || s == JobState::Paused
            })
            .await;
            assert!(st.state.is_committed(), "idle move {from} → {to}: {st:?}");
            let line = format!(
                "idle move {from} → {to}: committed in {:.1} s ({seen:?}), holds {:?} ms",
                t0.elapsed().as_secs_f64(),
                st.progress.holds_ms
            );
            eprintln!("{line}");
            report.push(line);
            wait_for(&mover, &id, Duration::from_secs(120), JobState::is_terminal).await;
            assert_eq!(
                items(&pools[to], &data).await,
                expected,
                "rows after {from} → {to}"
            );
        }
        let home = Placement::of(&maps.load(tenant).await.unwrap().0)
            .unwrap()
            .cluster;
        assert_eq!(home.as_str(), "dc-a-c1");

        // 2. Cancel in each state before the commit.
        let cases: [(&str, JobState); 5] = [
            ("planned", JobState::Planned),
            ("copying, waiting for a copy slot", JobState::Copying),
            ("catching_up", JobState::CatchingUp),
            ("verifying", JobState::Verifying),
            ("ready, manual cutover", JobState::Ready),
        ];
        for (name, at) in cases {
            let t0 = Instant::now();
            let mut spec = JobSpec::move_tenant(
                tenant,
                ClusterId::new("dc-a-c1").unwrap(),
                ClusterId::new("dc-a-c2").unwrap(),
            );
            spec = spec.with_cutover(CutoverMode::Manual);
            if at == JobState::Verifying {
                spec = spec.with_verify(VerifyMode::Full);
            }
            // A foreign job holds dc-a-c2's copy slots (both, at the default
            // `mover.max_copies_per_cluster` = 2, whichever worker claims the
            // job) for this case, on a short lease.
            let hog = if at == JobState::Copying {
                let lease = dc.grant_lease(Duration::from_secs(60)).await.unwrap();
                let keys: Vec<_> = (0..2)
                    .map(|n| {
                        dc.tier()
                            .key(&format!("mover/slots/copy/dc-a-c2/{n}"))
                            .unwrap()
                    })
                    .collect();
                let r = dc
                    .txn(Txn {
                        when: keys
                            .iter()
                            .map(|k| sprig_meta::Cmp::Missing(k.clone()))
                            .collect(),
                        then: keys
                            .iter()
                            .map(|k| Op::Put {
                                key: k.clone(),
                                value: b"r0fix-hog".to_vec(),
                                lease: Some(lease),
                            })
                            .collect(),
                        otherwise: vec![],
                    })
                    .await
                    .unwrap();
                assert!(
                    r.succeeded,
                    "dc-a-c2's copy slots are busy; try again later"
                );
                Some(lease)
            } else {
                None
            };
            let id = mover.submit(spec).await.unwrap();
            jobs.lock().unwrap().push(id);
            let mut state_at_cancel = JobState::Planned;
            if at != JobState::Planned {
                // A state passed too fast to see is cancelled in `ready` (the
                // manual cutover parks there).
                let (st, _) = wait_for(&mover, &id, Duration::from_secs(180), |s| {
                    s == at || s == JobState::Ready
                })
                .await;
                state_at_cancel = st.state;
                if at == JobState::Copying {
                    // Still waiting a while later: the slot is taken.
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    assert_eq!(mover.status(&id).await.unwrap().state, JobState::Copying);
                }
            }
            let tc = Instant::now();
            mover.cancel(&id, "r0fix test").await.unwrap();
            let (st, seen) =
                wait_for(&mover, &id, Duration::from_secs(30), JobState::is_terminal).await;
            if let Some(l) = hog {
                let _ = dc.revoke_lease(l).await;
            }
            assert_eq!(st.state, JobState::RolledBack, "cancel in {name}: {st:?}");
            let line = format!(
                "cancel in {name} (seen {state_at_cancel}): rolled back {:.1} s after the cancel \
             ({:.1} s in all; {seen:?})",
                tc.elapsed().as_secs_f64(),
                t0.elapsed().as_secs_f64()
            );
            eprintln!("{line}");
            report.push(line);
            let p = Placement::of(&maps.load(tenant).await.unwrap().0).unwrap();
            assert_eq!(p.cluster.as_str(), "dc-a-c1", "after cancel in {name}");
            assert_eq!(items(&pools["dc-a-c1"], &data).await, expected);
        }

        eprintln!("{}", report.join("\n"));
    })
    .catch_unwind()
    .await;

    // Clean up, pass or fail: only what this test made.
    let jobs = jobs.into_inner().unwrap();
    for id in &jobs {
        let _ = mover.cancel(id, "r0fix test cleanup").await;
    }
    for id in &jobs {
        for _ in 0..300 {
            match mover.status(id).await {
                Ok(st) if !st.state.is_terminal() && !st.state.is_committed() => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                _ => break,
            }
        }
    }
    stop.cancel();
    let _ = worker.await;
    for p in pools.values() {
        let mut c = p.get_conn().await.unwrap();
        for s in &schemas {
            let _ = c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`")).await;
        }
    }
    let mut ops = vec![Op::Delete(
        ShardMap::catalog_key(dc.tier(), tenant).unwrap(),
    )];
    for id in &jobs {
        ops.push(Op::Delete(
            dc.tier().key(&format!("mover/jobs/{}", id.0)).unwrap(),
        ));
        ops.push(Op::Delete(
            dc.tier().key(&format!("mover/owners/{}", id.0)).unwrap(),
        ));
    }
    ops.push(Op::Delete(
        dc.tier()
            .key(&format!("mover/shard-locks/{tenant}/0"))
            .unwrap(),
    ));
    let _ = dc
        .txn(Txn {
            when: vec![],
            then: ops,
            otherwise: vec![],
        })
        .await;
    for p in pools.into_values() {
        let _ = p.disconnect().await;
    }
    if let Err(p) = body {
        std::panic::resume_unwind(p);
    }
}
