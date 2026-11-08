//! A feed consumer resumes across tenant moves with no lost or duplicated
//! events (chapter 38; chapter 48, R1 done-when 4; chapter 10's invariant
//! 8 on real clusters).
//!
//! Needs the Compose stack with its clusters registered in etcd
//! (`deploy/compose/gateway-dev.sh up`):
//!
//! ```sh
//! SPRIG_TEST_FEED_LIVE=1 cargo test -p sprig-feed --test live_handoff -- --nocapture
//! ```
//!
//! One test tenant (a fresh id, recorded and cleaned up by that id only)
//! lives on `dc-a-c1` with a catalog naming its database `r1feed_shop`.
//! Four writers change `sbtest1` rows and insert an `events (writer, seq)`
//! row in each transaction, logging every acknowledged write. A consumer
//! reads the tenant's feed, makes batches durable and acks them through a
//! subscription. The real mover moves the tenant `c1 → c2` and then `c2 →
//! c3` (Galera) while the writers run. The consumer is killed three
//! times: once before the moves, once during the first move (it restarts
//! only after the cutover committed, so it resumes from a source token
//! across the move), and once after the second. At the end every
//! acknowledged `events` row must appear in the durable feed exactly once,
//! each writer's in order, the replayed `sbtest1` rows must equal the
//! acknowledged ones, and no event may name a physical schema.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::needless_late_init
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_binlog::{BinlogSource, GtidPos, Value};
use sprig_catalog::migrate::{DDL_DATABASES, DDL_OBJECTS};
use sprig_catalog::placement::ShardMap;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::CancellationToken;
use sprig_common::ids::{ClusterId, DcId, JobId, TenantId};
use sprig_common::time::SystemClock;
use sprig_feed::binlog::StaticClusters;
use sprig_feed::names::{CatalogPools, CatalogResolver};
use sprig_feed::subs::{Subscription, Subscriptions};
use sprig_feed::{FeedItem, MetaPlacement, Op as FeedOp, StartAt, TailOptions, TenantTail};
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Op, Record, Tier, Txn};
use sprig_mover::mysql::MysqlEnvFactory;
use sprig_mover::{
    JobRuntime, JobSpec, JobState, JobStore, MetaMover, Mover, MoverSettings, WorkerOpts,
};
use sprig_shard::{Placement, TenantMaps};

const ROOT_PW: &str = "sprig-dev-root";
const SUB: &str = "r1feed:live";

fn addr(c: &str) -> (&'static str, u16) {
    match c {
        "dc-a-c1" => ("127.0.0.1", 13301),
        "dc-a-c2" => ("127.0.0.1", 13311),
        _ => ("127.0.0.1", 13321),
    }
}

fn pool(c: &str) -> Pool {
    let (h, p) = addr(c);
    let opts = Opts::from_url(&format!("mysql://root:{ROOT_PW}@{h}:{p}")).unwrap();
    Pool::new(
        OptsBuilder::from_opts(opts)
            .client_found_rows(true)
            .pool_opts(
                PoolOpts::new()
                    .with_constraints(PoolConstraints::new(0, 8).unwrap())
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            ),
    )
}

fn now_ms() -> u64 {
    sprig_mover::record::wall_ms(std::time::SystemTime::now())
}

/// What the writers had acknowledged.
#[derive(Default)]
struct Log {
    rows: BTreeMap<i64, Option<(i64, String)>>,
    uncertain: BTreeMap<i64, Vec<Option<(i64, String)>>>,
    events: Vec<(i64, i64)>,
    events_uncertain: BTreeSet<(i64, i64)>,
    ops: u64,
}

struct Routing {
    maps: TenantMaps,
    tenant: TenantId,
    pools: BTreeMap<String, Pool>,
}

async fn writer(
    r: Arc<Routing>,
    schema: String,
    me: i64,
    log: Arc<Mutex<Log>>,
    stop: CancellationToken,
) {
    let mut seq = 0i64;
    let mut n = 0i64;
    let mut cached: Option<(Placement, Instant)> = None;
    while !stop.is_cancelled() {
        tokio::time::sleep(Duration::from_millis(4)).await;
        n += 1;
        let key = me + 4 * (n % 500);
        let delete = n % 7 == 0;
        let val = (n, format!("w{me}-{n}"));
        seq += 1;
        loop {
            if stop.is_cancelled() {
                return;
            }
            if cached
                .as_ref()
                .is_none_or(|(_, at)| at.elapsed() > Duration::from_millis(100))
            {
                cached = r
                    .maps
                    .load(r.tenant)
                    .await
                    .ok()
                    .and_then(|(m, _)| Placement::of(&m).ok())
                    .map(|p| (p, Instant::now()));
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
            let Ok(mut c) = r.pools[p.cluster.as_str()].get_conn().await else {
                cached = None;
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            };
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
                    let mut l = log.lock().unwrap();
                    l.rows.insert(key, (!delete).then(|| val.clone()));
                    l.uncertain.remove(&key);
                    l.events.push((me, seq));
                    l.ops += 1;
                    break;
                }
                Err(e) => {
                    if !matches!(e, mysql_async::Error::Server(_)) {
                        // The commit may or may not have happened.
                        let mut l = log.lock().unwrap();
                        let prev = l.rows.get(&key).cloned().flatten();
                        l.uncertain
                            .entry(key)
                            .or_default()
                            .extend([prev, (!delete).then(|| val.clone())]);
                        l.events_uncertain.insert((me, seq));
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

/// The tenant's catalog and data schema on `c1`.
async fn setup(p: &Pool, tenant: TenantId) -> (String, String) {
    let cat = format!("t{tenant}_sprig");
    let data = format!("t{tenant}_d1_g1_s1");
    let mut c = p.get_conn().await.unwrap();
    for s in [&cat, &data] {
        c.query_drop(format!("CREATE DATABASE `{s}`"))
            .await
            .unwrap();
    }
    c.query_drop(format!("USE `{cat}`")).await.unwrap();
    c.query_drop(DDL_DATABASES).await.unwrap();
    c.query_drop(DDL_OBJECTS).await.unwrap();
    c.query_drop(
        "INSERT INTO `databases` (db_id, name, name_key, default_charset, default_collation, \
         default_group_id, state, created_at, changed_version) VALUES \
         (1, 'r1feed_shop', 'r1feed_shop', 'utf8mb4', 'utf8mb4_general_ci', 1, 'active', NOW(6), 1)",
    )
    .await
    .unwrap();
    c.query_drop(
        "INSERT INTO objects (object_id, db_id, namespace, name, name_key, kind, state, \
         schema_version, created_at, changed_version) VALUES \
         (101, 1, 'relation', 'sbtest1', 'sbtest1', 'table', 'active', 1, NOW(6), 1), \
         (102, 1, 'relation', 'events', 'events', 'table', 'active', 1, NOW(6), 1)",
    )
    .await
    .unwrap();
    c.query_drop(format!(
        "CREATE TABLE `{data}`.sbtest1 (id INT NOT NULL PRIMARY KEY, k INT NOT NULL DEFAULT 0, \
         c CHAR(120) NOT NULL DEFAULT '', pad CHAR(60) NOT NULL DEFAULT 'pad')"
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
        "INSERT INTO `{data}`.sbtest1 (id, k, c) SELECT seq, seq, CONCAT('init-', seq) \
         FROM seq_1_to_3000"
    ))
    .await
    .unwrap();
    (cat, data)
}

/// The catalog is on the tenant's current cluster.
struct Catalogs {
    routing: Arc<Routing>,
}

#[async_trait::async_trait]
impl CatalogPools for Catalogs {
    async fn pool(&self, tenant: TenantId) -> sprig_common::Result<Pool> {
        let (m, _) = self.routing.maps.load(tenant).await?;
        let p = Placement::of(&m)?;
        Ok(self.routing.pools[p.cluster.as_str()].clone())
    }
}

/// One durable event, as checked.
#[derive(Debug, Clone)]
enum Seen {
    Event {
        writer: i64,
        seq: i64,
        cluster: String,
    },
    Row {
        id: i64,
        after: Option<(i64, String)>,
    },
}

#[derive(Default)]
struct Durable {
    seen: Vec<Seen>,
    restarts: u32,
    by_cluster: BTreeMap<String, u64>,
}

fn int(v: Option<&Value>) -> Option<i64> {
    match v? {
        Value::Int(i) => Some(*i),
        Value::UInt(u) => i64::try_from(*u).ok(),
        _ => None,
    }
}

fn text(v: Option<&Value>) -> Option<String> {
    v?.as_bytes()
        .map(|b| String::from_utf8_lossy(b).trim_end().to_owned())
}

/// The consumer: reads until `kill`, acks each transaction it made
/// durable; a kill drops the batch in flight.
#[allow(clippy::too_many_arguments)]
async fn consume(
    tenant: TenantId,
    clusters: Arc<StaticClusters>,
    placement: Arc<MetaPlacement>,
    names: Arc<CatalogResolver>,
    subs: Subscriptions,
    first: GtidPos,
    durable: Arc<Mutex<Durable>>,
    leaks: Arc<Mutex<Vec<String>>>,
    kill: CancellationToken,
) {
    let start = match subs.checkpoint(tenant, SUB).await.unwrap() {
        Some(cp) => StartAt::After(cp.resume_token().unwrap()),
        None => StartAt::At {
            cluster: ClusterId::new("dc-a-c1").unwrap(),
            pos: first,
        },
    };
    eprintln!("consumer starts at {start:?}");
    let mut tail = TenantTail::start(
        tenant,
        start,
        clusters,
        placement,
        names,
        TailOptions::default(),
    )
    .await
    .unwrap();
    let physical = format!("t{tenant}_");
    let mut batch: Vec<Seen> = Vec::new();
    let mut n = 0u64;
    loop {
        let item = tokio::select! {
            biased;
            () = kill.cancelled() => return,
            r = tail.next() => r.unwrap(),
        };
        let ack;
        match &item {
            FeedItem::Event(e) => {
                let dbg = format!("{e:?}");
                if dbg.contains(&physical) || dbg.contains("__sprig") || e.db_name != "r1feed_shop"
                {
                    leaks.lock().unwrap().push(dbg.clone());
                }
                match (e.table.as_str(), e.op) {
                    ("events", FeedOp::Insert) => {
                        let a = e.after.as_ref().unwrap();
                        assert_eq!(e.object, 102);
                        batch.push(Seen::Event {
                            writer: int(a.get("writer")).unwrap(),
                            seq: int(a.get("seq")).unwrap(),
                            cluster: e.id.cluster.to_string(),
                        });
                    }
                    ("sbtest1", _) => {
                        assert_eq!(e.object, 101);
                        let id = int(e.key.get("id")).unwrap();
                        let after = e
                            .after
                            .as_ref()
                            .map(|a| (int(a.get("k")).unwrap(), text(a.get("c")).unwrap()));
                        batch.push(Seen::Row { id, after });
                    }
                    other => leaks
                        .lock()
                        .unwrap()
                        .push(format!("unexpected event {other:?}: {dbg}")),
                }
                n += 1;
                // Now and then, ack in the middle of a transaction.
                ack = n.is_multiple_of(97);
            }
            FeedItem::TxnEnd { .. } | FeedItem::Progress { .. } => ack = true,
        }
        if ack {
            let k = batch.len() as u64;
            subs.ack(tenant, SUB, item.token(), k, now_ms())
                .await
                .unwrap();
            let mut d = durable.lock().unwrap();
            for s in &batch {
                if let Seen::Event { cluster, .. } = s {
                    *d.by_cluster.entry(cluster.clone()).or_default() += 1;
                }
            }
            d.seen.append(&mut batch);
        }
    }
}

async fn wait_state(
    mover: &MetaMover,
    id: &sprig_common::ids::JobId,
    want: &[JobState],
) -> JobState {
    let t0 = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Ok(st) = mover.status(id).await
            && (want.contains(&st.state) || st.state.is_terminal() || st.state == JobState::Paused)
        {
            return st.state;
        }
        assert!(t0.elapsed() < Duration::from_secs(600), "job {id:?} stuck");
    }
}

/// What a run found.
struct Outcome {
    errors: Vec<String>,
    leaks: Vec<String>,
    by_cluster: BTreeMap<String, u64>,
    caught_up: bool,
}

async fn connect_dc() -> (Arc<dyn MetaStore>, Arc<dyn MetaStore>) {
    let cfg = EtcdConfig::new(vec!["http://127.0.0.1:2379".into()]);
    let dc: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(Tier::Dc(DcId::new("dc-a").unwrap()), &cfg)
            .await
            .unwrap(),
    );
    let global: Arc<dyn MetaStore> =
        Arc::new(EtcdStore::connect(Tier::Global, &cfg).await.unwrap());
    (dc, global)
}

async fn run(
    tenant: TenantId,
    jobs: Arc<Mutex<Vec<JobId>>>,
    stop_worker: CancellationToken,
) -> Outcome {
    let (dc, global) = connect_dc().await;
    let registry = ClusterRegistry::new(global, dc.clone()).unwrap();
    let names = ["dc-a-c1", "dc-a-c2", "dc-a-c3"];
    let pools: BTreeMap<String, Pool> = names.iter().map(|c| ((*c).to_owned(), pool(c))).collect();
    let mut clusters = StaticClusters::new();
    for c in names {
        let (h, p) = addr(c);
        clusters = clusters.with(
            ClusterId::new(c).unwrap(),
            BinlogSource::new(h, p, "root", ROOT_PW),
        );
    }
    let clusters = Arc::new(clusters);
    let (_cat, data) = setup(&pools["dc-a-c1"], tenant).await;
    let key = ShardMap::catalog_key(dc.tier(), tenant).unwrap();
    dc.txn(Txn {
        when: vec![sprig_meta::Cmp::Missing(key.clone())],
        then: vec![Op::Put {
            key: key.clone(),
            value: Record::new(ShardMap::catalog(
                tenant,
                ClusterId::new("dc-a-c1").unwrap(),
                1,
            ))
            .encode()
            .unwrap(),
            lease: None,
        }],
        otherwise: vec![],
    })
    .await
    .unwrap();
    let maps = TenantMaps::new(dc.clone());
    let routing = Arc::new(Routing {
        maps: maps.clone(),
        tenant,
        pools: pools.clone(),
    });

    // The mover.
    let settings = MoverSettings {
        copy_chunk_rows: 500,
        ready_sustain: Duration::from_secs(2),
        cutover_retry: Duration::from_secs(1),
        source_retain: Duration::from_secs(5),
        ..MoverSettings::default()
    };
    let factory = Arc::new(MysqlEnvFactory::new(registry, maps.clone(), settings));
    let store = JobStore::new(dc.clone(), Arc::new(SystemClock));
    let mut wopts = WorkerOpts::new("r1feed-live");
    wopts.scan_interval = Duration::from_millis(200);
    let worker =
        tokio::spawn(JobRuntime::new(store.clone(), factory, wopts).run(stop_worker.clone()));
    let mover = MetaMover::new(store.clone());

    // The consumer's subscription and start.
    let subs = Subscriptions::new(dc.clone());
    subs.register(&Subscription {
        tenant,
        name: SUB.into(),
        kind: "sink".into(),
        ordering: "merged".into(),
        owner_ref: None,
        state: "active".into(),
        created_at_ms: now_ms(),
    })
    .await
    .unwrap();
    let first = sprig_feed::Clusters::position(&*clusters, &ClusterId::new("dc-a-c1").unwrap())
        .await
        .unwrap();
    let placement = Arc::new(MetaPlacement::new(dc.clone()));
    let resolver = Arc::new(CatalogResolver::new(Arc::new(Catalogs {
        routing: routing.clone(),
    })));
    let durable = Arc::new(Mutex::new(Durable::default()));
    let leaks = Arc::new(Mutex::new(Vec::new()));
    let start_consumer = || {
        let kill = CancellationToken::new();
        let h = tokio::spawn(consume(
            tenant,
            clusters.clone(),
            placement.clone(),
            resolver.clone(),
            subs.clone(),
            first.clone(),
            durable.clone(),
            leaks.clone(),
            kill.clone(),
        ));
        (h, kill)
    };
    let kill_consumer = |c: (tokio::task::JoinHandle<()>, CancellationToken)| async move {
        c.1.cancel();
        c.0.await.unwrap();
    };
    let mut consumer = start_consumer();

    // Writers.
    let log = Arc::new(Mutex::new(Log::default()));
    let stop = CancellationToken::new();
    let writers: Vec<_> = (0..4)
        .map(|i| {
            tokio::spawn(writer(
                routing.clone(),
                data.clone(),
                i,
                log.clone(),
                stop.clone(),
            ))
        })
        .collect();
    let report = Arc::new(Mutex::new(Vec::<String>::new()));
    let failed = Arc::new(AtomicBool::new(false));
    let ops_at = |l: &Arc<Mutex<Log>>| l.lock().unwrap().ops;

    // Kill 1: before any move.
    tokio::time::sleep(Duration::from_secs(3)).await;
    kill_consumer(consumer).await;
    durable.lock().unwrap().restarts += 1;
    consumer = start_consumer();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Move 1, c1 → c2; kill 2 while it copies, restart after its commit.
    let t0 = Instant::now();
    let id = mover
        .submit(JobSpec::move_tenant(
            tenant,
            ClusterId::new("dc-a-c1").unwrap(),
            ClusterId::new("dc-a-c2").unwrap(),
        ))
        .await
        .unwrap();
    jobs.lock().unwrap().push(id);
    let s = wait_state(
        &mover,
        &id,
        &[JobState::CatchingUp, JobState::Verifying, JobState::Ready],
    )
    .await;
    eprintln!(
        "move 1 reached {s} after {:.1} s: killing the consumer",
        t0.elapsed().as_secs_f64()
    );
    kill_consumer(consumer).await;
    durable.lock().unwrap().restarts += 1;
    let s = wait_state(&mover, &id, &[JobState::Retaining]).await;
    assert_eq!(s, JobState::Retaining, "move 1 didn't commit");
    let st = mover.status(&id).await.unwrap();
    report.lock().unwrap().push(format!(
        "move c1 → c2 committed after {:.1} s, holds {:?} ms, writes so far {}",
        t0.elapsed().as_secs_f64(),
        st.progress.holds_ms,
        ops_at(&log)
    ));
    tokio::time::sleep(Duration::from_secs(1)).await;
    consumer = start_consumer();
    // The tenant's shard lock is free once the job ends (the source is
    // retained for mover.source_retain first).
    let s = wait_state(&mover, &id, &[]).await;
    assert_eq!(s, JobState::Done, "move 1 didn't end done");

    // Move 2, c2 → c3 (Galera), with the consumer following live.
    let t1 = Instant::now();
    let id2 = mover
        .submit(JobSpec::move_tenant(
            tenant,
            ClusterId::new("dc-a-c2").unwrap(),
            ClusterId::new("dc-a-c3").unwrap(),
        ))
        .await
        .unwrap();
    jobs.lock().unwrap().push(id2);
    let s = wait_state(&mover, &id2, &[JobState::Retaining]).await;
    assert_eq!(s, JobState::Retaining, "move 2 didn't commit");
    let st = mover.status(&id2).await.unwrap();
    report.lock().unwrap().push(format!(
        "move c2 → c3 committed after {:.1} s, holds {:?} ms, writes so far {}",
        t1.elapsed().as_secs_f64(),
        st.progress.holds_ms,
        ops_at(&log)
    ));
    // Kill 3, after the second move.
    tokio::time::sleep(Duration::from_secs(2)).await;
    kill_consumer(consumer).await;
    durable.lock().unwrap().restarts += 1;
    consumer = start_consumer();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Stop writing; let the consumer catch up with c3.
    stop.cancel();
    for w in writers {
        w.await.unwrap();
    }
    let end = sprig_feed::Clusters::position(&*clusters, &ClusterId::new("dc-a-c3").unwrap())
        .await
        .unwrap();
    let t2 = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let cp = subs.checkpoint(tenant, SUB).await.unwrap().unwrap();
        let tok = cp.resume_token().unwrap();
        if tok.cluster.as_str() == "dc-a-c3" && tok.pos.reached(&end) {
            break;
        }
        if t2.elapsed() > Duration::from_secs(60) {
            failed.store(true, Ordering::Relaxed);
            eprintln!("the consumer didn't catch up: at {tok:?}, c3 at {end}");
            break;
        }
    }
    kill_consumer(consumer).await;

    // Compare.
    let l = std::mem::take(&mut *log.lock().unwrap());
    let d = std::mem::take(&mut *durable.lock().unwrap());
    let mut errors = Vec::new();
    let mut counts: BTreeMap<(i64, i64), u32> = BTreeMap::new();
    let mut last_seq: BTreeMap<i64, i64> = BTreeMap::new();
    let mut rows: BTreeMap<i64, Option<(i64, String)>> = BTreeMap::new();
    let mut n_events = 0u64;
    for s in &d.seen {
        match s {
            Seen::Event { writer, seq, .. } => {
                n_events += 1;
                *counts.entry((*writer, *seq)).or_default() += 1;
                let prev = last_seq.insert(*writer, *seq).unwrap_or(0);
                if *seq <= prev {
                    errors.push(format!("writer {writer}: seq {seq} after {prev}"));
                }
            }
            Seen::Row { id, after } => {
                rows.insert(*id, after.clone());
            }
        }
    }
    for e in &l.events {
        match counts.get(e) {
            Some(1) => {}
            n => errors.push(format!(
                "acknowledged write {e:?} is in the feed {n:?} times"
            )),
        }
    }
    let acked: BTreeSet<_> = l.events.iter().copied().collect();
    for (e, n) in &counts {
        if !acked.contains(e) && !l.events_uncertain.contains(e) {
            errors.push(format!("the feed has {e:?} ({n} times), never written"));
        }
        if *n > 1 {
            errors.push(format!("{e:?} delivered {n} times"));
        }
    }
    for (key, want) in &l.rows {
        let have = rows.get(key).cloned().flatten();
        let ok = have == *want || l.uncertain.get(key).is_some_and(|a| a.contains(&have));
        if !ok {
            errors.push(format!(
                "sbtest1 {key}: the feed replays to {have:?}, acknowledged {want:?}"
            ));
        }
    }
    let leaks = leaks.lock().unwrap().clone();
    eprintln!("---- feed handoff report ----");
    for r in report.lock().unwrap().iter() {
        eprintln!("{r}");
    }
    eprintln!(
        "{} acknowledged transactions ({} uncertain); {} durable events ({} events rows) by cluster {:?}; \
         {} consumer restarts; {} mismatches; {} events naming physical objects",
        l.ops,
        l.events_uncertain.len(),
        d.seen.len(),
        n_events,
        d.by_cluster,
        d.restarts,
        errors.len(),
        leaks.len()
    );
    for e in errors.iter().take(20) {
        eprintln!("  {e}");
    }
    for e in leaks.iter().take(5) {
        eprintln!("  leak: {e}");
    }

    let all: Vec<JobId> = jobs.lock().unwrap().clone();
    for id in all {
        let _ = wait_state(&mover, &id, &[]).await;
    }
    stop_worker.cancel();
    let _ = worker.await;
    Outcome {
        errors,
        leaks,
        by_cluster: d.by_cluster,
        caught_up: !failed.load(Ordering::Relaxed),
    }
}

/// Removes exactly what the run created: the tenant's two schemas, its
/// map, subscription, handoff records and the jobs it submitted.
async fn cleanup(tenant: TenantId, jobs: &[JobId]) {
    let (dc, _) = connect_dc().await;
    for c in ["dc-a-c1", "dc-a-c2", "dc-a-c3"] {
        let p = pool(c);
        if let Ok(mut conn) = p.get_conn().await {
            for s in [format!("t{tenant}_sprig"), format!("t{tenant}_d1_g1_s1")] {
                let _ = conn
                    .query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
                    .await;
            }
        }
        let _ = p.disconnect().await;
    }
    let _ = Subscriptions::new(dc.clone())
        .drop_subscription(tenant, SUB)
        .await;
    let mut ops = vec![Op::Delete(
        ShardMap::catalog_key(dc.tier(), tenant).unwrap(),
    )];
    for id in jobs {
        let id = id.0;
        ops.push(Op::Delete(
            dc.tier().key(&format!("mover/jobs/{id}")).unwrap(),
        ));
        ops.push(Op::Delete(
            dc.tier().key(&format!("mover/owners/{id}")).unwrap(),
        ));
    }
    ops.push(Op::Delete(
        dc.tier()
            .key(&format!("mover/shard-locks/{tenant}/0"))
            .unwrap(),
    ));
    for h in sprig_feed::ownership::history(&dc, tenant, 0)
        .await
        .unwrap_or_default()
    {
        ops.push(Op::Delete(
            sprig_feed::ownership::key(dc.tier(), tenant, 0, h.map_version).unwrap(),
        ));
    }
    let _ = dc
        .txn(Txn {
            when: vec![],
            then: ops,
            otherwise: vec![],
        })
        .await;
    eprintln!("cleaned up tenant {tenant} and jobs {jobs:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_consumer_resumes_across_moves_without_loss_or_duplicates() {
    if std::env::var("SPRIG_TEST_FEED_LIVE").is_err() {
        eprintln!("skipped: set SPRIG_TEST_FEED_LIVE=1 with the Compose stack up");
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "sprig_feed=info,sprig_mover=info,warn".into()),
        )
        .with_test_writer()
        .try_init();
    // A fresh tenant id, recorded: cleanup touches only this id.
    let tenant = TenantId(9_380_000 + now_ms() % 10_000);
    eprintln!("test tenant {tenant}");
    let jobs: Arc<Mutex<Vec<JobId>>> = Arc::default();
    let stop_worker = CancellationToken::new();
    let r = tokio::spawn(run(tenant, jobs.clone(), stop_worker.clone())).await;
    stop_worker.cancel();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let jobs = jobs.lock().unwrap().clone();
    cleanup(tenant, &jobs).await;
    let o = match r {
        Ok(o) => o,
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    };
    assert!(o.caught_up, "the consumer didn't catch up");
    assert!(o.leaks.is_empty(), "events named physical objects");
    assert!(o.errors.is_empty(), "{} mismatches", o.errors.len());
    assert!(
        o.by_cluster.len() == 3,
        "the feed didn't cover all three clusters: {:?}",
        o.by_cluster
    );
}
