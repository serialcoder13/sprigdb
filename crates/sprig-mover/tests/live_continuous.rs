//! `CutoverMode::Continuous` (standby copies) on two throwaway single-node
//! clusters.
//!
//! ```sh
//! SPRIG_TEST_DOCKER=1 cargo test -p sprig-mover --test live_continuous -- --nocapture
//! ```
//!
//! Two containers from `mariadb:11.8` (768 MB each), registered in an
//! in-memory control plane; the job runtime runs in the test. A tenant on
//! `src` gets a `move_shard` job with `Continuous` cutover to `dst` while
//! a writer inserts and updates on `src`:
//!
//! 1. the job copies, catches up and turns `streaming` (never
//!    `cutting_over`): the tenant's map still points at `src`, and `src`'s
//!    tables are never fenced; `dst` follows within the measured lag;
//! 2. `src` is frozen (`docker pause`) for 20 s: the job stays
//!    `streaming` (a standby never gives up on a lost source), and catches
//!    up after the thaw;
//! 3. the writer stops; `dst` equals `src` (row count and checksum);
//! 4. `src` is frozen again and the job is stopped (`cut_over`): it ends
//!    `done` at once, with `applied_source_gtid`, and `dst` keeps every
//!    row; writes on `src` afterwards don't reach `dst`;
//! 5. a second `Continuous` job is cancelled while streaming: it ends
//!    `rolled_back` and its target schemas are dropped.

#![allow(clippy::unwrap_used, clippy::too_many_lines, clippy::doc_markdown)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_catalog::placement::ShardMap;
use sprig_cluster::record::{Capacity, NodeSpec, ProvisionerKind, SecretRefs, TopologyKind};
use sprig_cluster::registry::{ClusterRegistry, Registration};
use sprig_common::CancellationToken;
use sprig_common::ids::{DcId, JobId, NodeId, TenantId};
use sprig_common::time::SystemClock;
use sprig_meta::{MemStore, MetaStore, Op, Record, Tier, Txn};
use sprig_mover::mysql::MysqlEnvFactory;
use sprig_mover::{
    CutoverMode, JobRuntime, JobSpec, JobState, JobStatus, JobStore, MetaMover, Mover,
    MoverSettings, WorkerOpts,
};
use sprig_shard::{Placement, TenantMaps};
use sprig_testkit::faults::compose::{OwnContainer, docker};
use sprig_testkit::skip;

const ROOT_PW: &str = "sprig-r2close-root";
/// Rule 8b's range for this agent's port 41406; only on its own containers.
const TENANT: u64 = 4_140_600_002;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .unwrap()
}

fn node(name: &str, domain: u32) -> (OwnContainer, u16) {
    let dir = sprig_testkit::compose::compose_dir().join("mariadb");
    let binlog = format!(
        "{}:/etc/mysql/conf.d/10-binlog.cnf:ro",
        dir.join("binlog.cnf").display()
    );
    let port = free_port();
    let publish = format!("127.0.0.1:{port}:3306");
    let root = format!("MARIADB_ROOT_PASSWORD={ROOT_PW}");
    let sid = format!("--server-id={}", 500 + domain);
    let dom = format!("--gtid-domain-id={domain}");
    let c = OwnContainer::run(
        &format!("r2close-{name}"),
        &[
            "--memory",
            "768m",
            "-p",
            &publish,
            "-e",
            &root,
            "-v",
            &binlog,
            "mariadb:11.8",
            &sid,
            &dom,
            "--innodb-buffer-pool-size=64M",
        ],
    )
    .unwrap();
    (c, port)
}

fn pool(port: u16) -> Pool {
    let opts = Opts::from_url(&format!("mysql://root:{ROOT_PW}@127.0.0.1:{port}")).unwrap();
    Pool::new(
        OptsBuilder::from_opts(opts).pool_opts(
            PoolOpts::new()
                .with_constraints(PoolConstraints::new(0, 3).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    )
}

async fn up(p: &Pool) {
    let t0 = Instant::now();
    loop {
        if let Ok(mut c) = p.get_conn().await
            && c.query_drop("SELECT 1").await.is_ok()
        {
            return;
        }
        assert!(t0.elapsed() < Duration::from_secs(120), "mariadb up");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn checksum(p: &Pool, data: &str) -> Option<(u64, String)> {
    let mut c = tokio::time::timeout(Duration::from_secs(3), p.get_conn())
        .await
        .ok()?
        .ok()?;
    c.query_first(format!(
        "SELECT COUNT(*), CAST(COALESCE(SUM(CRC32(CONCAT(id, v))), 0) AS CHAR) FROM `{data}`.items"
    ))
    .await
    .ok()?
}

async fn schemas_on(p: &Pool, tenant: TenantId) -> Vec<String> {
    let mut c = p.get_conn().await.unwrap();
    c.query(format!(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 't{tenant}\\_%' ORDER BY 1"
    ))
    .await
    .unwrap()
}

async fn wait_state(
    mover: &MetaMover,
    id: &JobId,
    limit: Duration,
    done: impl Fn(JobState) -> bool,
) -> JobStatus {
    let t0 = Instant::now();
    let mut last = None;
    loop {
        if let Ok(st) = mover.status(id).await {
            if last != Some(st.state) {
                eprintln!("  job {}: {} at {:?}", id.0, st.state, t0.elapsed());
                last = Some(st.state);
            }
            if done(st.state) {
                return st;
            }
            assert!(t0.elapsed() < limit, "job stuck: {st:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_continuous_job_streams_survives_a_lost_source_and_stops_in_place() {
    if !skip::flag(skip::DOCKER) {
        skip::report_missing(skip::DOCKER, "live_continuous");
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "sprig_mover=info,warn".into()),
        )
        .with_test_writer()
        .try_init();
    let (src_c, src_port) = node("src", 41);
    let (dst_c, dst_port) = node("dst", 42);
    let (src, dst) = (pool(src_port), pool(dst_port));
    up(&src).await;
    up(&dst).await;
    let work = std::env::temp_dir().join(format!("r2close-continuous-{}", std::process::id()));
    std::fs::create_dir_all(&work).unwrap();
    let creds: PathBuf = work.join("creds.toml");
    std::fs::write(
        &creds,
        format!(
            "[sprig_ctl]\nuser = \"root\"\npassword = \"{ROOT_PW}\"\ntls = \"skip_verify\"\n\
             [sprig_repl]\nuser = \"root\"\npassword = \"{ROOT_PW}\"\ntls = \"skip_verify\"\n"
        ),
    )
    .unwrap();
    let dc_id = DcId::new("fra1").unwrap();
    let dc: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc_id)));
    let global: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
    let registry = ClusterRegistry::new(global, dc.clone()).unwrap();
    let mut ids = Vec::new();
    for (name, port) in [("src", src_port), ("dst", dst_port)] {
        let stored = registry
            .register(Registration {
                name: format!("r2close-{name}"),
                topology: TopologyKind::SingleNode,
                provisioner: ProvisionerKind::External,
                nodes: vec![NodeSpec {
                    node: NodeId::new("n1").unwrap(),
                    addr: format!("127.0.0.1:{port}"),
                    zone: None,
                }],
                secrets: SecretRefs {
                    ctl: format!("file:{}#sprig_ctl", creds.display()),
                    repl: format!("file:{}#sprig_repl", creds.display()),
                },
                capacity: Capacity::default(),
                declared: None,
            })
            .await
            .unwrap();
        ids.push(stored.body().cluster_id.clone());
    }
    let (src_id, dst_id) = (ids[0].clone(), ids[1].clone());
    // The tenant on src: its catalog and one data schema.
    let tenant = TenantId(TENANT);
    let cat = format!("t{tenant}_sprig");
    let data = format!("t{tenant}_d1_g1_s1");
    {
        let mut c = src.get_conn().await.unwrap();
        for s in [&cat, &data] {
            c.query_drop(format!("CREATE DATABASE `{s}`"))
                .await
                .unwrap();
        }
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
            "INSERT INTO `{data}`.items SELECT seq, CONCAT('v', seq) FROM `{data}`.seq_1_to_20000"
        ))
        .await
        .unwrap();
    }
    let key = ShardMap::catalog_key(dc.tier(), tenant).unwrap();
    dc.txn(Txn {
        when: vec![],
        then: vec![Op::Put {
            key,
            value: Record::new(ShardMap::catalog(tenant, src_id.clone(), 1))
                .encode()
                .unwrap(),
            lease: None,
        }],
        otherwise: vec![],
    })
    .await
    .unwrap();

    let settings = MoverSettings {
        ready_sustain: Duration::from_secs(2),
        ..MoverSettings::default()
    };
    let maps = TenantMaps::new(dc.clone());
    let factory = Arc::new(MysqlEnvFactory::new(registry, maps.clone(), settings));
    let store = JobStore::new(dc.clone(), Arc::new(SystemClock));
    let mut opts = WorkerOpts::new("r2close-continuous");
    opts.scan_interval = Duration::from_millis(200);
    opts.lease = Duration::from_secs(10);
    let stop = CancellationToken::new();
    let worker =
        tokio::spawn(JobRuntime::new(store.clone(), factory, opts).run(stop.child_token()));
    let mover = MetaMover::new(store);

    // A writer on src: inserts and updates.
    let halt = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    let wtask = {
        let (p, data, halt, written) = (
            src.clone(),
            data.clone(),
            Arc::clone(&halt),
            Arc::clone(&written),
        );
        tokio::spawn(async move {
            let mut id = 100_000u64;
            while !halt.load(Ordering::SeqCst) {
                if let Ok(Ok(mut c)) =
                    tokio::time::timeout(Duration::from_secs(2), p.get_conn()).await
                {
                    let r = c
                        .exec_drop(
                            format!("INSERT INTO `{data}`.items (id, v) VALUES (?, ?)"),
                            (id, format!("w{id}")),
                        )
                        .await;
                    if r.is_ok() {
                        id += 1;
                        written.fetch_add(1, Ordering::SeqCst);
                        let _ = c
                            .exec_drop(
                                format!("UPDATE `{data}`.items SET v = ? WHERE id = ?"),
                                (format!("u{id}"), id % 20_000 + 1),
                            )
                            .await;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    // 1. Copy, catch up, stream.
    let t0 = Instant::now();
    let id = mover
        .submit(
            JobSpec::move_tenant(tenant, src_id.clone(), dst_id.clone())
                .with_cutover(CutoverMode::Continuous),
        )
        .await
        .unwrap();
    let st = wait_state(&mover, &id, Duration::from_secs(180), |s| {
        s == JobState::Streaming || s.is_terminal() || s == JobState::CuttingOver
    })
    .await;
    assert_eq!(st.state, JobState::Streaming, "{st:?}");
    let to_streaming = t0.elapsed();
    tokio::time::sleep(Duration::from_secs(10)).await;
    let st = mover.status(&id).await.unwrap();
    eprintln!(
        "streaming after {to_streaming:?}: lag {:?} ms, applied {:?}, seen {:?}, rows applied {}",
        st.progress.lag_ms,
        st.progress.applied_source_gtid,
        st.progress.source_seen_ms,
        st.progress.rows_applied
    );
    assert!(st.progress.lag_ms.is_some_and(|l| l < 5_000), "{st:?}");
    assert!(st.progress.applied_source_gtid.is_some(), "{st:?}");
    assert!(st.progress.rows_applied > 100, "changes stream: {st:?}");
    let home = Placement::of(&maps.load(tenant).await.unwrap().0)
        .unwrap()
        .cluster;
    assert_eq!(home, src_id, "the map never moves");
    let fenced: u64 = src
        .get_conn()
        .await
        .unwrap()
        .query_first(format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{data}' AND table_name LIKE '\\_\\_sprig\\_f\\_%'"
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fenced, 0, "src is never fenced");

    // 2. The source is lost for 20 s: still streaming; it catches up.
    docker(&["pause", src_c.name()]).unwrap();
    let frozen = Instant::now();
    tokio::time::sleep(Duration::from_secs(20)).await;
    let st = mover.status(&id).await.unwrap();
    assert_eq!(
        st.state,
        JobState::Streaming,
        "a lost source keeps the job streaming: {st:?}"
    );
    let seen_age = st
        .progress
        .source_seen_ms
        .map(|s| sprig_mover::record::wall_ms(std::time::SystemTime::now()).saturating_sub(s));
    eprintln!(
        "src frozen 20 s: state {}, source last seen {seen_age:?} ms ago, error {:?}",
        st.state, st.error
    );
    docker(&["unpause", src_c.name()]).unwrap();
    let thaw = Instant::now();
    loop {
        let st = mover.status(&id).await.unwrap();
        if st.progress.source_seen_ms.is_some_and(|s| {
            sprig_mover::record::wall_ms(std::time::SystemTime::now()).saturating_sub(s) < 2_000
        }) {
            eprintln!(
                "caught up {:?} after the thaw (frozen {:?} in all), lag {:?} ms",
                thaw.elapsed(),
                frozen.elapsed(),
                st.progress.lag_ms
            );
            break;
        }
        assert!(
            thaw.elapsed() < Duration::from_secs(60),
            "never caught up: {st:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // 3. The writer stops; dst converges to src.
    halt.store(true, Ordering::SeqCst);
    wtask.await.unwrap();
    let want = checksum(&src, &data).await.unwrap();
    let t1 = Instant::now();
    loop {
        if checksum(&dst, &data).await.as_ref() == Some(&want) {
            break;
        }
        assert!(
            t1.elapsed() < Duration::from_secs(30),
            "dst never equal to src"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    eprintln!(
        "dst equals src ({} rows; {} acknowledged inserts) {:?} after the writer stopped",
        want.0,
        written.load(Ordering::SeqCst),
        t1.elapsed()
    );

    // 4. Stop with the source lost: done at once; dst keeps everything.
    docker(&["pause", src_c.name()]).unwrap();
    let t2 = Instant::now();
    mover.cut_over(&id).await.unwrap();
    let st = wait_state(&mover, &id, Duration::from_secs(30), JobState::is_terminal).await;
    docker(&["unpause", src_c.name()]).unwrap();
    assert_eq!(st.state, JobState::Done, "{st:?}");
    eprintln!(
        "stopped {:?} after the request with src frozen; applied_source_gtid {:?}",
        t2.elapsed(),
        st.progress.applied_source_gtid
    );
    assert!(st.progress.applied_source_gtid.is_some());
    assert_eq!(
        checksum(&dst, &data).await,
        Some(want.clone()),
        "dst keeps what it applied"
    );
    let home = Placement::of(&maps.load(tenant).await.unwrap().0)
        .unwrap()
        .cluster;
    assert_eq!(home, src_id, "a stop doesn't move the tenant");
    {
        let mut c = src.get_conn().await.unwrap();
        c.query_drop(format!(
            "INSERT INTO `{data}`.items VALUES (999999, 'after')"
        ))
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        checksum(&dst, &data).await,
        Some(want),
        "nothing streams after the stop"
    );

    // 5. Cancel while streaming: rolled back, targets dropped.
    {
        let mut c = dst.get_conn().await.unwrap();
        for s in [&cat, &data] {
            c.query_drop(format!("DROP DATABASE `{s}`")).await.unwrap();
        }
    }
    let id2 = mover
        .submit(
            JobSpec::move_tenant(tenant, src_id.clone(), dst_id.clone())
                .with_cutover(CutoverMode::Continuous),
        )
        .await
        .unwrap();
    wait_state(&mover, &id2, Duration::from_secs(180), |s| {
        s == JobState::Streaming || s.is_terminal()
    })
    .await;
    assert_eq!(schemas_on(&dst, tenant).await.len(), 2);
    mover.cancel(&id2, "r2close test").await.unwrap();
    let st = wait_state(&mover, &id2, Duration::from_secs(60), JobState::is_terminal).await;
    assert_eq!(st.state, JobState::RolledBack, "{st:?}");
    assert!(
        schemas_on(&dst, tenant).await.is_empty(),
        "the standby copy is dropped"
    );
    assert_eq!(schemas_on(&src, tenant).await.len(), 2, "src untouched");
    eprintln!("cancel while streaming: rolled back, target dropped, source untouched");

    stop.cancel();
    let _ = worker.await;
    drop((src, dst));
    drop((src_c, dst_c));
    let _ = std::fs::remove_dir_all(&work);
}
