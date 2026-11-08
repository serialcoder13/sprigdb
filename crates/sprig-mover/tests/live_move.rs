//! A tenant moves between the Compose clusters under write load (chapter
//! 10, "Testing"; milestone M0.6, done-when 1 and 3).
//!
//! Needs the Compose stack with its three clusters registered in etcd
//! (`deploy/compose/gateway-dev.sh up` registers `dc-a-c1` single,
//! `dc-a-c2` primary + replicas and `dc-a-c3` Galera):
//!
//! ```sh
//! SPRIG_TEST_MOVER_LIVE=1 cargo test -p sprig-mover --test live_move -- --nocapture
//! ```
//!
//! One tenant (a fresh id above 9,000,000, so it can't meet a real one)
//! makes a round of the six ordered topology pairs, c1 → c2 → c3 → c1 →
//! c3 → c2 → c1, while writers route by its map like a gateway: they wait
//! while it is held, and after a fence error re-read it and retry. Every
//! acknowledged write is logged. After each move the target must hold
//! exactly the logged state: no acknowledged write lost, no insert
//! duplicated (the `events` table has a unique writer/sequence pair), and
//! the copied types table equal to the fenced source. Each move's write
//! hold and the longest write latency writers saw during it are printed.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names
)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool};
use sprig_catalog::placement::ShardMap;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::CancellationToken;
use sprig_common::ids::{ClusterId, DcId, TenantId};
use sprig_common::time::SystemClock;
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Op, Record, Tier, Txn};
use sprig_mover::mysql::MysqlEnvFactory;
use sprig_mover::{
    JobRuntime, JobSpec, JobState, JobStore, MetaMover, Mover, MoverSettings, WorkerOpts,
};
use sprig_shard::{Placement, TenantMaps};

const ROOT_PW: &str = "sprig-dev-root";

fn cluster_url(c: &str) -> &'static str {
    match c {
        "dc-a-c1" => "127.0.0.1:13301",
        "dc-a-c2" => "127.0.0.1:13311",
        _ => "127.0.0.1:13321",
    }
}

fn pool(c: &str) -> Pool {
    let opts = Opts::from_url(&format!("mysql://root:{ROOT_PW}@{}", cluster_url(c))).unwrap();
    Pool::new(OptsBuilder::from_opts(opts).client_found_rows(true))
}

/// One logged, acknowledged write per key: `None` = deleted.
#[derive(Default)]
struct Log {
    rows: BTreeMap<i64, Option<(i64, String)>>,
    /// Keys whose last write's outcome isn't known (lost connection on
    /// commit): either value is allowed.
    uncertain: BTreeMap<i64, Vec<Option<(i64, String)>>>,
    events: Vec<(u32, u64)>,
    events_uncertain: Vec<(u32, u64)>,
    max_latency: Duration,
    ops: u64,
}

struct Routing {
    maps: TenantMaps,
    tenant: TenantId,
    pools: BTreeMap<String, Pool>,
}

impl Routing {
    async fn placement(&self) -> Option<Placement> {
        let (m, _) = self.maps.load(self.tenant).await.ok()?;
        Placement::of(&m).ok()
    }
}

fn now_ms() -> u64 {
    sprig_mover::record::wall_ms(std::time::SystemTime::now())
}

/// One writer: keys `k ≡ me (mod of)` of `sbtest1`, and inserts into
/// `events`.
#[allow(clippy::too_many_arguments)]
async fn writer(
    r: Arc<Routing>,
    schema: String,
    me: u32,
    of: u32,
    keys: i64,
    log: Arc<Mutex<Log>>,
    stop: CancellationToken,
    ops: Arc<AtomicU64>,
) {
    let mut seq = 0u64;
    let mut cached: Option<(Placement, Instant)> = None;
    let mut n = 0i64;
    let pace = Duration::from_millis(
        std::env::var("SPRIG_TEST_MOVER_PACE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3),
    );
    while !stop.is_cancelled() {
        tokio::time::sleep(pace).await;
        n += 1;
        let key = i64::from(me) + i64::from(of) * (n % (keys / i64::from(of)).max(1));
        let delete = n % 7 == 0;
        let val = (n, format!("w{me}-{n}"));
        seq += 1;
        let started = Instant::now();
        loop {
            if stop.is_cancelled() {
                return;
            }
            if cached
                .as_ref()
                .is_none_or(|(_, at)| at.elapsed() > Duration::from_millis(100))
            {
                cached = r.placement().await.map(|p| (p, Instant::now()));
            }
            let Some((p, _)) = cached.clone() else {
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            };
            if p.held_at(now_ms()) {
                tokio::time::sleep(Duration::from_millis(5)).await;
                cached = None;
                continue;
            }
            let pool = &r.pools[p.cluster.as_str()];
            let Ok(mut c) = pool.get_conn().await else {
                cached = None;
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            };
            // One transaction: the row change and an event.
            let res: Result<(), mysql_async::Error> = async {
                c.query_drop("BEGIN").await?;
                if delete {
                    c.exec_drop(
                        format!("DELETE FROM `{schema}`.sbtest1 WHERE id = ?"),
                        (key,),
                    )
                    .await?;
                } else {
                    c.exec_drop(
                        format!(
                            "INSERT INTO `{schema}`.sbtest1 (id, k, c) VALUES (?, ?, ?) \
                             ON DUPLICATE KEY UPDATE k = VALUES(k), c = VALUES(c)"
                        ),
                        (key, val.0, val.1.clone()),
                    )
                    .await?;
                }
                c.exec_drop(
                    format!("INSERT INTO `{schema}`.events (writer, seq) VALUES (?, ?)"),
                    (me, seq),
                )
                .await?;
                c.query_drop("COMMIT").await
            }
            .await;
            match res {
                Ok(()) => {
                    let lat = started.elapsed();
                    let mut l = log.lock().unwrap();
                    l.rows.insert(key, (!delete).then(|| val.clone()));
                    l.uncertain.remove(&key);
                    l.events.push((me, seq));
                    l.max_latency = l.max_latency.max(lat);
                    l.ops += 1;
                    ops.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err(e) => {
                    let server = matches!(e, mysql_async::Error::Server(_));
                    if !server {
                        // The commit may or may not have happened.
                        let mut l = log.lock().unwrap();
                        let prev = l.rows.get(&key).cloned().flatten();
                        l.uncertain
                            .entry(key)
                            .or_default()
                            .extend([prev, (!delete).then(|| val.clone())]);
                        l.events_uncertain.push((me, seq));
                        seq += 1;
                    }
                    drop(c);
                    cached = None;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
    }
}

async fn setup(src: &Pool, tenant: TenantId) -> (String, String) {
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
    c.query_drop(format!(
        "CREATE TABLE `{cat}`.meta (k VARCHAR(64) PRIMARY KEY, v JSON NOT NULL)"
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO `{cat}`.meta VALUES ('a', '{{\"x\": 1}}'), ('b', '[1, 2]')"
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "CREATE TABLE `{data}`.sbtest1 (id INT NOT NULL PRIMARY KEY, k INT NOT NULL DEFAULT 0, \
         c CHAR(120) NOT NULL DEFAULT '', pad CHAR(60) NOT NULL DEFAULT 'pad', KEY k_1 (k))"
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "CREATE TABLE `{data}`.events (id BIGINT AUTO_INCREMENT PRIMARY KEY, writer INT NOT NULL, \
         seq BIGINT NOT NULL, UNIQUE KEY ws (writer, seq))"
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "CREATE TABLE `{data}`.types (a INT NOT NULL, b VARCHAR(20) CHARACTER SET utf8mb4 NOT NULL, \
         dec_col DECIMAL(12,3), dt DATETIME(6), ts TIMESTAMP(6) NULL, e ENUM('x','y','z'), \
         st SET('p','q','r'), j JSON, bl BLOB, f DOUBLE, fl FLOAT, bt BIT(5), y YEAR, tm TIME(3), \
         u BIGINT UNSIGNED, gen INT AS (a * 2) VIRTUAL, inv INT INVISIBLE DEFAULT 7, \
         PRIMARY KEY (a, b))"
    ))
    .await
    .unwrap();
    c.query_drop(format!("USE `{data}`")).await.unwrap();
    c.query_drop(format!(
        "INSERT INTO `{data}`.sbtest1 (id, k, c) SELECT seq, seq, CONCAT('init-', seq) \
         FROM seq_1_to_3000"
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO `{data}`.types (a, b, dec_col, dt, ts, e, st, j, bl, f, fl, bt, y, tm, u, inv) \
         SELECT seq, CONCAT('ké', seq), seq / 7, '2026-11-08 01:02:03.456789', \
         FROM_UNIXTIME(1700000000 + seq), 1 + seq % 3, seq % 8, JSON_OBJECT('n', seq), \
         UNHEX(MD5(seq)), seq / 3, seq / 9, seq % 32, 2000 + seq % 100, SEC_TO_TIME(seq), \
         18446744073709551615 - seq, seq FROM seq_1_to_500"
    ))
    .await
    .unwrap();
    (cat, data)
}

/// Removes what killed runs of this test left: jobs, locks, maps and
/// schemas of tenants above 9,000,000.
async fn cleanup_old_runs(dc: &Arc<dyn MetaStore>, pools: &BTreeMap<String, Pool>) {
    let store = JobStore::new(dc.clone(), Arc::new(SystemClock));
    let mut tenants = std::collections::BTreeSet::new();
    for (rec, _) in store.all().await.unwrap_or_default() {
        if rec.tenant_id.0 >= 9_000_000 {
            tenants.insert(rec.tenant_id);
            let id = rec.job_id.0;
            let mut ops = vec![
                Op::Delete(dc.tier().key(&format!("mover/jobs/{id}")).unwrap()),
                Op::Delete(dc.tier().key(&format!("mover/owners/{id}")).unwrap()),
                Op::Delete(
                    dc.tier()
                        .key(&format!("mover/shard-locks/{}/0", rec.tenant_id))
                        .unwrap(),
                ),
            ];
            ops.push(Op::Delete(
                ShardMap::catalog_key(dc.tier(), rec.tenant_id).unwrap(),
            ));
            let _ = dc
                .txn(Txn {
                    when: vec![],
                    then: ops,
                    otherwise: vec![],
                })
                .await;
        }
    }
    for p in pools.values() {
        let Ok(mut c) = p.get_conn().await else {
            continue;
        };
        let names: Vec<String> = c
            .query(
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 't9%'",
            )
            .await
            .unwrap_or_default();
        for n in names {
            if tenants.iter().any(|t| n.starts_with(&format!("t{t}_"))) {
                let _ = c.query_drop(format!("DROP DATABASE IF EXISTS `{n}`")).await;
            }
        }
    }
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

async fn table_digest(pool: &Pool, schema: &str, table: &str) -> String {
    let mut c = pool.get_conn().await.unwrap();
    let q = format!(
        "SELECT COUNT(*), COALESCE(BIT_XOR(CAST(CONV(LEFT(MD5(CONCAT_WS(0x1f, a, b, dec_col, \
         dt, UNIX_TIMESTAMP(ts), e + 0, st + 0, j, HEX(bl), f, fl, bt + 0, y, tm, u, gen, inv)), \
         16), 16, 10) AS UNSIGNED)), 0) FROM `{schema}`.`{table}`"
    );
    let r: Option<(u64, u64)> = c.query_first(q).await.unwrap();
    format!("{r:?}")
}

async fn check_against_log(pool: &Pool, data: &str, log: &Log) -> Result<(), String> {
    let mut c = pool.get_conn().await.map_err(|e| e.to_string())?;
    let rows: Vec<(i64, i64, String)> = c
        .query(format!("SELECT id, k, c FROM `{data}`.sbtest1"))
        .await
        .map_err(|e| e.to_string())?;
    let got: BTreeMap<i64, (i64, String)> = rows.into_iter().map(|(i, k, s)| (i, (k, s))).collect();
    for (key, want) in &log.rows {
        let have = got.get(key).cloned();
        let ok = have == *want
            || log
                .uncertain
                .get(key)
                .is_some_and(|alts| alts.contains(&have));
        if !ok {
            return Err(format!(
                "sbtest1 id {key}: have {have:?}, the log says {want:?}"
            ));
        }
    }
    let ev: Vec<(u32, u64)> = c
        .query(format!("SELECT writer, seq FROM `{data}`.events"))
        .await
        .map_err(|e| e.to_string())?;
    let mut counts: BTreeMap<(u32, u64), u32> = BTreeMap::new();
    for e in ev {
        *counts.entry(e).or_default() += 1;
    }
    for e in &log.events {
        match counts.get(e) {
            Some(1) => {}
            n => {
                return Err(format!(
                    "event {e:?} appears {n:?} times, acknowledged once"
                ));
            }
        }
    }
    let known: std::collections::BTreeSet<_> =
        log.events.iter().chain(&log.events_uncertain).collect();
    if let Some(extra) = counts.keys().find(|e| !known.contains(e)) {
        return Err(format!("event {extra:?} was never written"));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn a_tenant_moves_around_every_topology_pair_under_load() {
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
    let tenant = TenantId(9_000_000 + u64::from(std::process::id() % 100_000));
    let pools: BTreeMap<String, Pool> = ["dc-a-c1", "dc-a-c2", "dc-a-c3"]
        .iter()
        .map(|c| ((*c).to_owned(), pool(c)))
        .collect();
    cleanup_old_runs(&dc, &pools).await;
    let (cat, data) = setup(&pools["dc-a-c1"], tenant).await;
    put_map(&dc, tenant, "dc-a-c1").await;

    let settings = MoverSettings {
        copy_chunk_rows: 500,
        ready_sustain: Duration::from_secs(2),
        cutover_retry: Duration::from_secs(1),
        source_retain: Duration::from_secs(5),
        ..MoverSettings::default()
    };
    let maps = TenantMaps::new(dc.clone());
    let factory = Arc::new(MysqlEnvFactory::new(registry, maps.clone(), settings));
    let store = JobStore::new(dc.clone(), Arc::new(SystemClock));
    let mut opts = WorkerOpts::new("mover-live");
    opts.scan_interval = Duration::from_millis(200);
    let stop_worker = CancellationToken::new();
    let rt = JobRuntime::new(store.clone(), factory, opts);
    let worker = tokio::spawn(rt.run(stop_worker.clone()));
    let mover = MetaMover::new(store);

    let routing = Arc::new(Routing {
        maps: maps.clone(),
        tenant,
        pools: pools.clone(),
    });
    let log = Arc::new(Mutex::new(Log::default()));
    let stop = CancellationToken::new();
    let ops = Arc::new(AtomicU64::new(0));
    let writers: Vec<_> = (0..8)
        .map(|i| {
            tokio::spawn(writer(
                routing.clone(),
                data.clone(),
                i,
                8,
                4000,
                log.clone(),
                stop.clone(),
                ops.clone(),
            ))
        })
        .collect();
    let failed = Arc::new(AtomicBool::new(false));
    let route = [
        "dc-a-c1", "dc-a-c2", "dc-a-c3", "dc-a-c1", "dc-a-c3", "dc-a-c2", "dc-a-c1",
    ];
    let mut report = Vec::new();
    let pairs: usize = std::env::var("SPRIG_TEST_MOVER_PAIRS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6);
    for pair in route.windows(2).take(pairs) {
        let (from, to) = (pair[0], pair[1]);
        tokio::time::sleep(Duration::from_secs(2)).await;
        log.lock().unwrap().max_latency = Duration::ZERO;
        let ops0 = ops.load(Ordering::Relaxed);
        let t0 = Instant::now();
        let id = mover
            .submit(JobSpec::move_tenant(
                tenant,
                ClusterId::new(from).unwrap(),
                ClusterId::new(to).unwrap(),
            ))
            .await
            .unwrap();
        let st = loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let st = mover.status(&id).await.unwrap();
            if st.state == JobState::Retaining
                || st.state.is_terminal()
                || st.state == JobState::Paused
            {
                break st;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(600),
                "move {from} → {to} is stuck: {st:?}"
            );
        };
        assert_eq!(st.state, JobState::Retaining, "move {from} → {to}: {st:?}");
        tokio::time::sleep(Duration::from_millis(500)).await;
        let lat = log.lock().unwrap().max_latency;
        // The copied types table equals the fenced source.
        let fenced = sprig_cluster::fence::fenced_name("types");
        let a = table_digest(&pools[from], &data, &fenced).await;
        let b = table_digest(&pools[to], &data, "types").await;
        if a != b {
            failed.store(true, Ordering::Relaxed);
            eprintln!("types differ after {from} → {to}: source {a}, target {b}");
        }
        let line = format!(
            "{from} → {to}: {:.1} s, rows copied {}, applied {}, holds {:?} ms, attempts {}, \
             verify mismatches {}, writes during the move {}, longest write {} ms",
            t0.elapsed().as_secs_f64(),
            st.progress.rows_copied,
            st.progress.rows_applied,
            st.progress.holds_ms,
            st.progress.cutover_attempts,
            st.progress.verify_mismatches,
            ops.load(Ordering::Relaxed) - ops0,
            lat.as_millis()
        );
        eprintln!("{line}");
        report.push(line);
        // The job drops the retained source after mover.source_retain.
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let st = mover.status(&id).await.unwrap();
            if st.state.is_terminal() {
                assert_eq!(st.state, JobState::Done);
                break;
            }
        }
    }
    stop.cancel();
    for w in writers {
        w.await.unwrap();
    }
    let l = std::mem::take(&mut *log.lock().unwrap());
    let end = Placement::of(&maps.load(tenant).await.unwrap().0).unwrap();
    let r = check_against_log(&pools[end.cluster.as_str()], &data, &l).await;
    eprintln!(
        "{} acknowledged writes, {} uncertain; final cluster {} epoch {}",
        l.ops,
        l.events_uncertain.len(),
        end.cluster,
        end.epoch
    );
    // Clean up.
    stop_worker.cancel();
    let _ = worker.await;
    for p in pools.values() {
        let mut c = p.get_conn().await.unwrap();
        for s in [&cat, &data] {
            let _ = c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`")).await;
        }
    }
    let key = ShardMap::catalog_key(dc.tier(), tenant).unwrap();
    let _ = dc
        .txn(Txn {
            when: vec![],
            then: vec![Op::Delete(key)],
            otherwise: vec![],
        })
        .await;
    r.unwrap();
    assert!(!failed.load(Ordering::Relaxed), "a copied table differed");
    assert_eq!(end.epoch, 1 + pairs.min(6) as u64);
}
