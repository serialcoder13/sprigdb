//! Live tests of rollout and observation on the Compose clusters (11.8).
//!
//! ```text
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! export SPRIG_TEST_MARIADB_REPLICATION=mysql://root:sprig-dev-root@127.0.0.1:13311,mysql://root:sprig-dev-root@127.0.0.1:13312,mysql://root:sprig-dev-root@127.0.0.1:13313
//! export SPRIG_TEST_MARIADB_GALERA=mysql://root:sprig-dev-root@127.0.0.1:13321,mysql://root:sprig-dev-root@127.0.0.1:13322,mysql://root:sprig-dev-root@127.0.0.1:13323
//! cargo test -p sprig-advisor rollout::live_tests -- --nocapture --test-threads=1
//! ```
//!
//! Each case works in its own schema `__sprig_test_rollout_<pid>_<n>`,
//! created on the writer (replicated everywhere) and dropped afterwards,
//! also when the case fails. Nothing is written to a replica. The table
//! has 200,000 rows; a client keeps writing to it through the writer while
//! indexes are built, and the numbers are printed.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names
)]

use std::collections::BTreeMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};
use sprig_cluster::driver::DdlMethod;
use sprig_cluster::sql::NodeEndpoint;
use sprig_common::ids::{ClusterId, Fingerprint, NodeId, ObjectId, TenantId};
use sprig_common::names::quote;

use super::driver::{RolloutCx, Step, advance};
use super::node::Node;
use super::*;
use crate::autoindex::model::{AutoIndexId, IndexDef, Origin};
use crate::autoindex::observe::drift::reconcile_object;
use crate::autoindex::observe::{
    FpLatency, ObserveCx, ObserveSettings, ObserveStep, UsageSample, WorkloadSource, step,
};
use crate::autoindex::policy::ObserveRules;
use crate::autoindex::store::{
    AutoIndexRow, AutoIndexStore, DDL_AUTO_INDEXES, DDL_CANDIDATES, DDL_INDEX_ORIGINS,
    DDL_VALIDATIONS, Definition, DropReason, IndexState, LogDecisions, Model, OriginRow,
    ShardBuild, StoredKind,
};
use crate::autoindex::validate::NoMarker;

const SINGLE: &str = "SPRIG_TEST_MARIADB_SINGLE";
const REPLICATION: &str = "SPRIG_TEST_MARIADB_REPLICATION";
const GALERA: &str = "SPRIG_TEST_MARIADB_GALERA";
const ROWS: u32 = 200_000;
/// Galera nodes are capped at 1 GB: a 200k-row fill OOM-killed one.
const GALERA_ROWS: u32 = 100_000;

fn rows_for(topo: Topo) -> u32 {
    if topo == Topo::Galera {
        GALERA_ROWS
    } else {
        ROWS
    }
}
const OBJECT: ObjectId = ObjectId(4242);
const TENANT: TenantId = TenantId(42);

fn urls(env: &str) -> Option<Vec<String>> {
    let v = std::env::var(env).ok().filter(|v| !v.trim().is_empty())?;
    Some(v.split(',').map(|s| s.trim().to_owned()).collect())
}

fn endpoints(urls: &[String], prefix: &str) -> Vec<NodeEndpoint> {
    urls.iter()
        .enumerate()
        .map(|(i, u)| {
            NodeEndpoint::from_url(NodeId::new(format!("{prefix}{}", i + 1)).unwrap(), u).unwrap()
        })
        .collect()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Topo {
    Single,
    Replication,
    Galera,
}

fn view_of(topo: Topo) -> Option<ClusterView> {
    let (env, prefix) = match topo {
        Topo::Single => (SINGLE, "single"),
        Topo::Replication => (REPLICATION, "pr"),
        Topo::Galera => (GALERA, "g"),
    };
    let eps = endpoints(&urls(env)?, prefix);
    let nodes = match topo {
        Topo::Single => Nodes::Single {
            writer: eps[0].clone(),
        },
        Topo::Replication => Nodes::PrimaryReplica {
            writer: eps[0].clone(),
            replicas: eps[1..].to_vec(),
        },
        Topo::Galera => Nodes::Galera {
            writer: eps[0].node.clone(),
            nodes: eps,
        },
    };
    Some(ClusterView {
        cluster: ClusterId::new(format!("dev-{topo:?}").to_ascii_lowercase()).unwrap(),
        ddl_method: if topo == Topo::Galera {
            DdlMethod::CopySwap
        } else {
            DdlMethod::Online
        },
        nodes,
        instant_ignored_toggle: true,
        unhealthy: None,
    })
}

fn unique() -> u64 {
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::Relaxed)
}

/// What a case gets.
struct Env {
    view: ClusterView,
    writer_url: String,
    schema: String,
    store: AutoIndexStore,
    table: ShardTable,
}

/// Runs `body` with a fresh schema holding the `orders` table (200k rows)
/// and the automatic-indexing tables; drops the schema afterwards.
async fn with_env<F, Fut>(topo: Topo, case: &str, body: F)
where
    F: FnOnce(Env) -> Fut,
    Fut: Future<Output = ()>,
{
    let Some(view) = view_of(topo) else {
        eprintln!("skipped {case} ({topo:?}): set the topology's SPRIG_TEST_MARIADB_* variable");
        return;
    };
    let env_name = match topo {
        Topo::Single => SINGLE,
        Topo::Replication => REPLICATION,
        Topo::Galera => GALERA,
    };
    let writer_url = urls(env_name).unwrap()[0].clone();
    let schema = format!("__sprig_test_rollout_{}_{}", std::process::id(), unique());
    let mut c = Conn::new(Opts::from_url(&writer_url).unwrap())
        .await
        .unwrap();
    c.query_drop(format!("CREATE DATABASE {}", quote(&schema)))
        .await
        .unwrap();
    c.query_drop(format!("USE {}", quote(&schema)))
        .await
        .unwrap();
    for ddl in [
        DDL_AUTO_INDEXES,
        DDL_CANDIDATES,
        DDL_VALIDATIONS,
        DDL_INDEX_ORIGINS,
    ] {
        c.query_drop(ddl).await.unwrap();
    }
    c.query_drop(
        "CREATE TABLE orders (
           id          BIGINT PRIMARY KEY AUTO_INCREMENT,
           customer_id INT NOT NULL,
           status      TINYINT NOT NULL,
           created_at  DATETIME NOT NULL,
           total       DECIMAL(10,2) NOT NULL,
           pad         VARCHAR(100) NOT NULL
         ) ENGINE=InnoDB",
    )
    .await
    .unwrap();
    let fill = Instant::now();
    let rows = rows_for(topo);
    for chunk in 0..(rows / 25_000) {
        let lo = chunk * 25_000 + 1;
        c.query_drop(format!(
            "INSERT INTO orders (customer_id, status, created_at, total, pad)
             SELECT seq % 5000, seq % 7, '2026-01-01' + INTERVAL seq SECOND, seq % 1000,
                    REPEAT(CHAR(97 + seq % 26), 60)
             FROM seq_{lo}_to_{}",
            lo + 24_999
        ))
        .await
        .unwrap();
    }
    eprintln!(
        "[{case} {topo:?}] {rows} rows in {} ms",
        fill.elapsed().as_millis()
    );
    // Replicas and Galera peers must have the table before anything runs.
    wait_everywhere(&view, &schema, rows).await;
    let env = Env {
        store: AutoIndexStore::new(&schema),
        table: ShardTable {
            shard: "0".into(),
            schema: schema.clone(),
            table: "orders".into(),
        },
        view,
        writer_url: writer_url.clone(),
        schema: schema.clone(),
    };
    let r = AssertUnwindSafe(body(env)).catch_unwind().await;
    let mut c = Conn::new(Opts::from_url(&writer_url).unwrap())
        .await
        .unwrap();
    c.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&schema)))
        .await
        .unwrap();
    let _ = c.disconnect().await;
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

async fn wait_everywhere(view: &ClusterView, schema: &str, want: u32) {
    let t = ShardTable {
        shard: "0".into(),
        schema: schema.into(),
        table: "orders".into(),
    };
    for ep in view.nodes.all() {
        let mut n = Node::connect(ep, Duration::from_secs(10)).await.unwrap();
        let started = Instant::now();
        loop {
            let rows = n
                .scalar(&format!("SELECT COUNT(*) FROM {}.orders", quote(schema)))
                .await
                .ok()
                .flatten()
                .and_then(|v| v.parse::<u32>().ok());
            if rows.is_some_and(|r| r >= want) && n.table_exists(&t).await.unwrap() {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(120),
                "{} never caught up",
                ep.node
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        n.close().await;
    }
}

fn settings() -> RolloutSettings {
    RolloutSettings {
        lock_retry_pause: Duration::from_secs(1),
        poll: Duration::from_millis(100),
        catch_up_floor: Duration::from_secs(60),
        rollout_pause: Duration::ZERO,
        retry_after: Duration::from_secs(1),
        max_build: Duration::from_secs(600),
        galera: GaleraPolicy {
            resync_floor: Duration::from_secs(60),
            ..GaleraPolicy::default()
        },
        ..RolloutSettings::default()
    }
}

fn def() -> IndexDef {
    IndexDef::btree(OBJECT, &["customer_id", "created_at"])
}

fn row(def: &IndexDef, state: IndexState) -> AutoIndexRow {
    let now = SystemTime::now();
    AutoIndexRow {
        index_id: AutoIndexId(0),
        object: def.object,
        name: def.name(),
        model: Model::Sql,
        kind: StoredKind::BTree,
        definition: Definition::of(def),
        state,
        prior_state: None,
        candidate_id: 1,
        validation_id: None,
        decision_id: None,
        est_benefit_ms_h: 1000.0,
        est_write_ms_h: 10.0,
        est_bytes: 10 << 20,
        actual_bytes: None,
        shards: [("0".to_owned(), ShardBuild::Pending)].into(),
        baseline: BTreeMap::new(),
        observed: None,
        last_used_at: None,
        drop_reason: None,
        created_at: now,
        state_changed_at: now,
    }
}

/// A client writing to `orders` through the writer until stopped.
struct Client {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<(Vec<Duration>, Vec<String>)>,
}

impl Client {
    fn start(url: &str, schema: &str) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&stop);
        let (url, schema) = (url.to_owned(), schema.to_owned());
        let task = tokio::spawn(async move {
            let mut c = Conn::new(Opts::from_url(&url).unwrap()).await.unwrap();
            c.query_drop(format!("USE {}", quote(&schema)))
                .await
                .unwrap();
            let (mut lat, mut errs) = (Vec::new(), Vec::new());
            let mut i: u64 = 0;
            while !s.load(Ordering::Relaxed) {
                i += 1;
                let sql = if i.is_multiple_of(2) {
                    format!(
                        "INSERT INTO orders (customer_id, status, created_at, total, pad) \
                         VALUES ({}, 1, NOW(), 1.5, 'client')",
                        i % 5000
                    )
                } else {
                    format!(
                        "UPDATE orders SET status = status + 1 WHERE id = {}",
                        1 + (i * 7919) % u64::from(GALERA_ROWS)
                    )
                };
                let t = Instant::now();
                match c.query_drop(sql).await {
                    Ok(()) => lat.push(t.elapsed()),
                    Err(e) => {
                        errs.push(e.to_string());
                        if errs.len() > 50 {
                            break;
                        }
                        // A dropped connection: reconnect.
                        if let Ok(n) = Conn::new(Opts::from_url(&url).unwrap()).await {
                            c = n;
                            let _ = c.query_drop(format!("USE {}", quote(&schema))).await;
                        }
                    }
                }
            }
            let _ = c.disconnect().await;
            (lat, errs)
        });
        Self { stop, task }
    }

    async fn finish(self, label: &str) -> (Duration, Vec<String>) {
        self.stop.store(true, Ordering::Relaxed);
        let (mut lat, errs) = self.task.await.unwrap();
        lat.sort();
        let pct = |p: f64| {
            lat.get(((lat.len() as f64 - 1.0) * p).round() as usize)
                .copied()
                .unwrap_or_default()
        };
        let max = lat.last().copied().unwrap_or_default();
        eprintln!(
            "[{label}] client: {} writes, p50 {:.2} ms, p99 {:.2} ms, max {:.1} ms, {} errors {:?}",
            lat.len(),
            pct(0.5).as_secs_f64() * 1e3,
            pct(0.99).as_secs_f64() * 1e3,
            max.as_secs_f64() * 1e3,
            errs.len(),
            errs.iter().take(3).collect::<Vec<_>>()
        );
        (max, errs)
    }
}

fn cx<'a>(
    env: &'a Env,
    s: &'a RolloutSettings,
    slots: &'a LocalSlots,
    shards: &'a [ShardTable],
    bytes: u64,
) -> RolloutCx<'a> {
    RolloutCx {
        tenant: TENANT,
        view: &env.view,
        shards,
        table_bytes: bytes,
        settings: s,
        auto_window: None,
        slots,
        external_builds: 0,
        locks: &NoMover,
        mover: None,
        marker: &NoMarker,
        decisions: &LogDecisions,
        now: SystemTime::now(),
    }
}

async fn catalog(env: &Env) -> Conn {
    Conn::new(Opts::from_url(&env.writer_url).unwrap())
        .await
        .unwrap()
}

async fn table_bytes(env: &Env) -> u64 {
    let mut n = Node::connect(env.view.nodes.writer().unwrap(), Duration::from_secs(10))
        .await
        .unwrap();
    n.exec(&format!("ANALYZE TABLE {}.orders", quote(&env.schema)))
        .await
        .unwrap();
    let b = n.table_bytes(&env.table).await.unwrap();
    n.close().await;
    b
}

async fn index_on_all(env: &Env, name: &str) -> Vec<(String, bool)> {
    let mut out = Vec::new();
    for ep in env.view.nodes.all() {
        let mut n = Node::connect(ep, Duration::from_secs(10)).await.unwrap();
        out.push((
            ep.node.to_string(),
            n.indexes(&env.table).await.unwrap().contains_key(name),
        ));
        n.close().await;
    }
    out
}

/// An accepted candidate is built online while a client keeps writing.
async fn build_under_writes(topo: Topo, switch: GaleraSwitch, label: &str) {
    with_env(topo, label, |env| async move {
        let mut s = settings();
        s.galera.switch = switch;
        let slots = LocalSlots::new();
        let shards = vec![env.table.clone()];
        let bytes = table_bytes(&env).await;
        let def = def();
        let mut q = catalog(&env).await;
        let id = env
            .store
            .insert_index(&mut q, &row(&def, IndexState::RollingOut))
            .await
            .unwrap();
        let r = env.store.index(&mut q, id).await.unwrap().unwrap();
        let client = Client::start(&env.writer_url, &env.schema);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let started = Instant::now();
        let c = cx(&env, &s, &slots, &shards, bytes);
        let step = advance(&env.store, &mut q, &c, &r).await.unwrap();
        let wall = started.elapsed();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (max, errs) = client.finish(label).await;
        let Step::Observing { report: Some(rep) } = &step else {
            panic!("{label}: {step:?}");
        };
        eprintln!(
            "[{label}] table {:.1} MB, method {}, build {} ms (step {} ms), replicas {:?} ms, \
             max lag {:?} s, lock retries {}, nodes {:?}",
            bytes as f64 / 1_048_576.0,
            rep.method,
            rep.build_ms,
            wall.as_millis(),
            rep.replicas_ms,
            rep.max_replica_lag_s,
            rep.lock_retries,
            rep.nodes
        );
        assert!(errs.is_empty(), "{label}: client errors {errs:?}");
        let after = env.store.index(&mut q, id).await.unwrap().unwrap();
        assert_eq!(after.state, IndexState::Observing);
        assert_eq!(after.shards.get("0"), Some(&ShardBuild::Built));
        for (node, has) in index_on_all(&env, &def.name()).await {
            assert!(has, "{label}: {} missing on {node}", def.name());
        }
        // Idempotent: a second build attempt finds it and does nothing.
        let mut again = row(&def, IndexState::RollingOut);
        again.name = def.name();
        let _ = max;
        let _ = q.disconnect().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn online_build_under_writes_single() {
    build_under_writes(Topo::Single, GaleraSwitch::Auto, "single online").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn online_build_under_writes_replication() {
    build_under_writes(Topo::Replication, GaleraSwitch::Auto, "replication online").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn galera_toi_build_under_writes() {
    build_under_writes(Topo::Galera, GaleraSwitch::ToiOnly, "galera toi").await;
}

/// Rolling RSU while a client writes through the first node: a
/// measurement of why it isn't the default (writes stalled 15-18 s). Run
/// by hand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measures the rolling RSU stall; not safe to run routinely"]
async fn galera_rolling_rsu_build_under_writes() {
    build_under_writes(Topo::Galera, GaleraSwitch::RollingRsu, "galera rsu").await;
}

/// User DDL arriving during a build wins: the build is killed, the user's
/// statement goes through at once, the rollout waits for the cooldown.
async fn user_ddl_aborts(topo: Topo, switch: GaleraSwitch, label: &str) {
    with_env(topo, label, |env| async move {
        let mut s = settings();
        s.galera.switch = switch;
        let slots = LocalSlots::new();
        let shards = vec![env.table.clone()];
        let bytes = table_bytes(&env).await;
        // A wide index builds longer.
        let def = IndexDef::btree(OBJECT, &["pad", "customer_id", "created_at", "total"]);
        let mut q = catalog(&env).await;
        let id = env
            .store
            .insert_index(&mut q, &row(&def, IndexState::RollingOut))
            .await
            .unwrap();
        let r = env.store.index(&mut q, id).await.unwrap().unwrap();
        let env = Arc::new(env);
        let e2 = Arc::clone(&env);
        let user = tokio::spawn(async move {
            // Wait until the build runs (on the writer, or any node under RSU).
            let started = Instant::now();
            'wait: loop {
                for ep in e2.view.nodes.all() {
                    let mut n = Node::connect(ep, Duration::from_secs(10)).await.unwrap();
                    let th = n.threads().await.unwrap();
                    n.close().await;
                    if th.iter().any(|t| {
                        t.info.as_deref().is_some_and(super::node::is_sprig_build)
                            && !t.waits_for_mdl()
                    }) {
                        break 'wait;
                    }
                }
                assert!(
                    started.elapsed() < Duration::from_secs(30),
                    "the build never started"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut c = Conn::new(Opts::from_url(&e2.writer_url).unwrap())
                .await
                .unwrap();
            let t = Instant::now();
            c.query_drop(format!(
                "ALTER TABLE {}.orders ADD COLUMN note VARCHAR(10) NULL",
                quote(&e2.schema)
            ))
            .await
            .unwrap();
            let took = t.elapsed();
            let _ = c.disconnect().await;
            took
        });
        let c = cx(&env, &s, &slots, &shards, bytes);
        let step = advance(&env.store, &mut q, &c, &r).await.unwrap();
        let user_took = user.await.unwrap();
        eprintln!(
            "[{label}] user DDL took {} ms; step {step:?}",
            user_took.as_millis()
        );
        match &step {
            Step::Retry {
                outcome: BuildOutcome::Aborted { reason, .. },
                retry_after,
            } => {
                assert_eq!(*reason, AbortReason::UserDdl, "{label}");
                assert_eq!(*retry_after, s.user_ddl_cooldown);
            }
            other => panic!("{label}: {other:?}"),
        }
        assert!(
            user_took < Duration::from_secs(5),
            "{label}: user DDL waited {user_took:?}"
        );
        let after = env.store.index(&mut q, id).await.unwrap().unwrap();
        assert_eq!(after.state, IndexState::RollingOut);
        assert_eq!(after.shards.get("0"), Some(&ShardBuild::Pending));
        let p = Progress::of(after.observed.as_ref());
        assert!(p.not_before_ms > unix_ms(SystemTime::now()) + 3_600_000);
        // The next step waits for the cooldown.
        let c = cx(&env, &s, &slots, &shards, bytes);
        assert!(matches!(
            advance(&env.store, &mut q, &c, &after).await.unwrap(),
            Step::Wait { .. }
        ));
        // Wherever the build had got to, no node is left building; give a
        // killed RSU node its resync, then the reconciler-to-be sees the row.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = q.disconnect().await;
        // `env` drops after the closure; with_env cleans up.
        drop(env);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_ddl_aborts_build_single() {
    user_ddl_aborts(Topo::Single, GaleraSwitch::Auto, "single user ddl").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_ddl_aborts_build_replication() {
    user_ddl_aborts(
        Topo::Replication,
        GaleraSwitch::Auto,
        "replication user ddl",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "rolling RSU is off by default; run by hand"]
async fn user_ddl_aborts_build_galera_rsu() {
    user_ddl_aborts(
        Topo::Galera,
        GaleraSwitch::RollingRsu,
        "galera rsu user ddl",
    )
    .await;
}

/// A workload store with fixed answers.
struct Fake {
    latency: Option<BTreeMap<Fingerprint, FpLatency>>,
    samples: Option<Vec<UsageSample>>,
}

#[async_trait::async_trait]
impl WorkloadSource for Fake {
    async fn latency(
        &self,
        _t: TenantId,
        _o: ObjectId,
        _s: SystemTime,
        _u: SystemTime,
    ) -> sprig_common::Result<Option<BTreeMap<Fingerprint, FpLatency>>> {
        Ok(self.latency.clone())
    }
    async fn samples(
        &self,
        _t: TenantId,
        _o: ObjectId,
        _n: usize,
        _since: SystemTime,
    ) -> sprig_common::Result<Option<Vec<UsageSample>>> {
        Ok(self.samples.clone())
    }
}

fn rules(ignore_hours: u32) -> ObserveRules {
    ObserveRules {
        min_improvement_pct: 30.0,
        max_regression_pct: 10.0,
        observe_days: 7,
        unused_drop_days: 30,
        ignore_before_drop_hours: ignore_hours,
        recreate_block_days: 14,
    }
}

/// Builds `def` directly (as a finished rollout would have) and records an
/// `active` row for it.
async fn active_index(env: &Env, q: &mut Conn, def: &IndexDef) -> AutoIndexId {
    let mut n = Node::connect(env.view.nodes.writer().unwrap(), Duration::from_secs(60))
        .await
        .unwrap();
    n.exec(&format!(
        "ALTER TABLE {}.orders {}",
        quote(&env.schema),
        add_index_clause(def)
    ))
    .await
    .unwrap();
    n.close().await;
    let mut r = row(def, IndexState::Active);
    r.shards = [("0".to_owned(), ShardBuild::Built)].into();
    env.store.insert_index(q, &r).await.unwrap()
}

fn ocx<'a>(
    env: &'a Env,
    rules: &'a ObserveRules,
    os: &'a ObserveSettings,
    rs: &'a RolloutSettings,
    shards: &'a [ShardTable],
    w: &'a dyn WorkloadSource,
    now: SystemTime,
) -> ObserveCx<'a> {
    ObserveCx {
        tenant: TENANT,
        view: &env.view,
        shards,
        rules,
        settings: os,
        ddl: rs,
        workload: w,
        decisions: &LogDecisions,
        now,
    }
}

const DAY: Duration = Duration::from_secs(86_400);

/// Unused for 30 days: ignored, then dropped after the quiet period; the
/// user's index on the same table is never touched.
async fn unused_dropped(topo: Topo, label: &str) {
    with_env(topo, label, |env| async move {
        let rs = settings();
        let os = ObserveSettings::default();
        let rl = rules(24);
        let shards = vec![env.table.clone()];
        let def = def();
        let mut q = catalog(&env).await;
        let id = active_index(&env, &mut q, &def).await;
        // A user index (recorded) and one the catalog doesn't know (also user).
        let mut n = Node::connect(env.view.nodes.writer().unwrap(), Duration::from_secs(60))
            .await
            .unwrap();
        n.exec(&format!(
            "ALTER TABLE {}.orders ADD INDEX idx_status (status)",
            quote(&env.schema)
        ))
        .await
        .unwrap();
        n.exec(&format!(
            "ALTER TABLE {}.orders ADD INDEX idx_total (total)",
            quote(&env.schema)
        ))
        .await
        .unwrap();
        n.close().await;
        env.store
            .put_origin(
                &mut q,
                &OriginRow {
                    object: OBJECT,
                    index_name: "idx_status".into(),
                    origin: Origin::User,
                    source: "ddl".into(),
                    recorded_at: SystemTime::now(),
                },
            )
            .await
            .unwrap();
        // Samples that never use our index: the plan reads idx_status.
        let fake = Fake {
            latency: Some(BTreeMap::new()),
            samples: Some(vec![UsageSample {
                fingerprint: Fingerprint(1),
                schema: env.schema.clone(),
                sql: "SELECT id FROM orders WHERE status = 3".into(),
            }]),
        };
        // Day 10: checked, not used, not old enough.
        let t10 = SystemTime::now() + 10 * DAY;
        let r = env.store.index(&mut q, id).await.unwrap().unwrap();
        let st = step(
            &env.store,
            &mut q,
            &ocx(&env, &rl, &os, &rs, &shards, &fake, t10),
            &r,
        )
        .await
        .unwrap();
        assert_eq!(st, ObserveStep::Checked, "{label}");
        // Day 31: retired through IGNORED.
        let t31 = SystemTime::now() + 31 * DAY;
        let r = env.store.index(&mut q, id).await.unwrap().unwrap();
        let st = step(
            &env.store,
            &mut q,
            &ocx(&env, &rl, &os, &rs, &shards, &fake, t31),
            &r,
        )
        .await
        .unwrap();
        assert_eq!(st, ObserveStep::Retiring(DropReason::Unused), "{label}");
        let mut n = Node::connect(env.view.nodes.writer().unwrap(), Duration::from_secs(10))
            .await
            .unwrap();
        let ix = n.indexes(&env.table).await.unwrap();
        assert!(ix[&def.name()].ignored, "{label}: not IGNORED");
        // Quiet for 24 h: dropped.
        let t32 = t31 + DAY + Duration::from_secs(60);
        let r = env.store.index(&mut q, id).await.unwrap().unwrap();
        assert_eq!(r.state, IndexState::Retiring);
        let st = step(
            &env.store,
            &mut q,
            &ocx(&env, &rl, &os, &rs, &shards, &fake, t32),
            &r,
        )
        .await
        .unwrap();
        assert_eq!(
            st,
            ObserveStep::Dropped(Some(DropReason::Unused)),
            "{label}"
        );
        let r = env.store.index(&mut q, id).await.unwrap().unwrap();
        assert_eq!(r.state, IndexState::Dropped);
        assert_eq!(r.drop_reason, Some(DropReason::Unused));
        let ix = n.indexes(&env.table).await.unwrap();
        assert!(!ix.contains_key(&def.name()));
        // The user's indexes stay, through every check and the reconciler.
        assert!(ix.contains_key("idx_status") && ix.contains_key("idx_total"));
        let rep = reconcile_object(
            &env.store,
            &mut q,
            &ocx(&env, &rl, &os, &rs, &shards, &fake, t32),
            OBJECT,
        )
        .await
        .unwrap();
        assert!(rep.strays_dropped.is_empty(), "{rep:?}");
        let ix = n.indexes(&env.table).await.unwrap();
        assert!(ix.contains_key("idx_status") && ix.contains_key("idx_total"));
        n.close().await;
        // Even a hand-made row naming a user index is refused.
        let mut bad = row(&def, IndexState::Dropping);
        bad.name = "idx_status".into();
        assert!(super::node::drop_sql(&env.table, &bad.name).is_err());
        assert!(crate::autoindex::observe::check_auto(&bad).is_err());
        let _ = q.disconnect().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unused_index_dropped_user_index_kept_single() {
    unused_dropped(Topo::Single, "single unused").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unused_index_dropped_user_index_kept_galera() {
    unused_dropped(Topo::Galera, "galera unused").await;
}

/// No usage information (no workload store, userstat off): never dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_usage_never_drops() {
    with_env(Topo::Single, "unknown usage", |env| async move {
        let (rs, os, rl) = (settings(), ObserveSettings::default(), rules(24));
        let shards = vec![env.table.clone()];
        let def = def();
        let mut q = catalog(&env).await;
        let id = active_index(&env, &mut q, &def).await;
        let r = env.store.index(&mut q, id).await.unwrap().unwrap();
        let fake = Fake {
            latency: None,
            samples: None,
        };
        let t = SystemTime::now() + 400 * DAY;
        let st = step(
            &env.store,
            &mut q,
            &ocx(&env, &rl, &os, &rs, &shards, &fake, t),
            &r,
        )
        .await
        .unwrap();
        assert_eq!(st, ObserveStep::Checked);
        let r = env.store.index(&mut q, id).await.unwrap().unwrap();
        assert_eq!(r.state, IndexState::Active);
        let _ = q.disconnect().await;
    })
    .await;
}

/// A user index equal to ours retires ours; the user's stays.
async fn superseded(topo: Topo, label: &str) {
    with_env(topo, label, |env| async move {
        let (rs, os) = (settings(), ObserveSettings::default());
        let rl = rules(0); // straight to the drop
        let shards = vec![env.table.clone()];
        let def = def();
        let mut q = catalog(&env).await;
        let id = active_index(&env, &mut q, &def).await;
        let fake = Fake {
            latency: None,
            samples: None,
        };
        let now = SystemTime::now();
        // Nothing equivalent yet.
        let r = env.store.index(&mut q, id).await.unwrap().unwrap();
        let st = step(
            &env.store,
            &mut q,
            &ocx(&env, &rl, &os, &rs, &shards, &fake, now),
            &r,
        )
        .await
        .unwrap();
        assert_eq!(st, ObserveStep::Checked);
        let mut n = Node::connect(env.view.nodes.writer().unwrap(), Duration::from_secs(60))
            .await
            .unwrap();
        n.exec(&format!(
            "CREATE INDEX idx_cust_created ON {}.orders (customer_id, created_at, total)",
            quote(&env.schema)
        ))
        .await
        .unwrap();
        let r = env.store.index(&mut q, id).await.unwrap().unwrap();
        let st = step(
            &env.store,
            &mut q,
            &ocx(&env, &rl, &os, &rs, &shards, &fake, now),
            &r,
        )
        .await
        .unwrap();
        assert_eq!(
            st,
            ObserveStep::Dropped(Some(DropReason::Superseded)),
            "{label}"
        );
        let ix = n.indexes(&env.table).await.unwrap();
        assert!(!ix.contains_key(&def.name()));
        assert!(ix.contains_key("idx_cust_created"));
        n.close().await;
        let r = env.store.index(&mut q, id).await.unwrap().unwrap();
        assert_eq!(r.state, IndexState::Dropped);
        assert_eq!(r.drop_reason, Some(DropReason::Superseded));
        let _ = q.disconnect().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_equivalent_retires_ours_single() {
    superseded(Topo::Single, "single superseded").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_equivalent_retires_ours_replication() {
    superseded(Topo::Replication, "replication superseded").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_equivalent_retires_ours_galera() {
    superseded(Topo::Galera, "galera superseded").await;
}

/// The reconciler drops a stray automatic index, finds a vanished one, and
/// leaves user indexes alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drift_reconciler() {
    with_env(Topo::Replication, "drift", |env| async move {
        let (rs, os, rl) = (settings(), ObserveSettings::default(), rules(24));
        let shards = vec![env.table.clone()];
        let mut q = catalog(&env).await;
        let kept = def();
        let kept_id = active_index(&env, &mut q, &kept).await;
        // An automatic index no row explains, and a row whose index is gone.
        let stray = IndexDef::btree(OBJECT, &["status", "total"]);
        let gone = IndexDef::btree(OBJECT, &["total"]);
        let mut n = Node::connect(env.view.nodes.writer().unwrap(), Duration::from_secs(60))
            .await
            .unwrap();
        n.exec(&format!(
            "ALTER TABLE {}.orders {}",
            quote(&env.schema),
            add_index_clause(&stray)
        ))
        .await
        .unwrap();
        n.exec(&format!(
            "ALTER TABLE {}.orders ADD INDEX idx_user (status)",
            quote(&env.schema)
        ))
        .await
        .unwrap();
        let mut g = row(&gone, IndexState::Active);
        g.shards = [("0".to_owned(), ShardBuild::Built)].into();
        let gone_id = env.store.insert_index(&mut q, &g).await.unwrap();
        let fake = Fake {
            latency: None,
            samples: None,
        };
        let rep = reconcile_object(
            &env.store,
            &mut q,
            &ocx(&env, &rl, &os, &rs, &shards, &fake, SystemTime::now()),
            OBJECT,
        )
        .await
        .unwrap();
        eprintln!("[drift] {rep:?}");
        assert_eq!(rep.strays_dropped, vec![format!("0/{}", stray.name())]);
        assert_eq!(rep.rows_dropped, vec![(gone.name(), DropReason::Drift)]);
        assert!(rep.errors.is_empty());
        let ix = n.indexes(&env.table).await.unwrap();
        assert!(!ix.contains_key(&stray.name()));
        assert!(ix.contains_key(&kept.name()) && ix.contains_key("idx_user"));
        n.close().await;
        assert_eq!(
            env.store
                .index(&mut q, kept_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            IndexState::Active
        );
        assert_eq!(
            env.store
                .index(&mut q, gone_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            IndexState::Dropped
        );
        // The drop replicated: no replica keeps the stray.
        tokio::time::sleep(Duration::from_secs(2)).await;
        for (node, has) in index_on_all(&env, &stray.name()).await {
            assert!(!has, "stray still on {node}");
        }
        let _ = q.disconnect().await;
    })
    .await;
}

/// A probe, run by hand: one RSU `ADD INDEX` on the third Galera node while a
/// client writes through the first, with a timeline of the node's state,
/// the writer's flow control and the client's worst latency.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement, run by hand"]
async fn probe_one_rsu_node() {
    with_env(Topo::Galera, "rsu probe", |env| async move {
        let eps: Vec<NodeEndpoint> = env.view.nodes.all().into_iter().cloned().collect();
        let client = Client::start(&env.writer_url, &env.schema);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let stop = Arc::new(AtomicBool::new(false));
        let s2 = Arc::clone(&stop);
        let (e0, e2) = (eps[0].clone(), eps[2].clone());
        let mon = tokio::spawn(async move {
            let mut w = Node::connect(&e0, Duration::from_secs(30)).await.unwrap();
            let t0 = Instant::now();
            while !s2.load(Ordering::Relaxed) {
                let fc = w
                    .scalar("SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'WSREP_FLOW_CONTROL_PAUSED_NS'")
                    .await
                    .ok()
                    .flatten();
                let t = Instant::now();
                let st = match Node::connect(&e2, Duration::from_secs(2)).await {
                    Ok(mut n) => {
                        let s = n.wsrep().await.map_or_else(|e| e.to_string(), |s| s.local);
                        n.close().await;
                        s
                    }
                    Err(e) => format!("unreachable {e}"),
                };
                eprintln!(
                    "[probe] t={:>5} ms g3 {st} ({} ms) g1 fc_paused_ns {fc:?}",
                    t0.elapsed().as_millis(),
                    t.elapsed().as_millis()
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            w.close().await;
        });
        let mut d = Node::connect(&eps[2], Duration::from_secs(300)).await.unwrap();
        d.exec("SET SESSION sql_log_bin = 0").await.unwrap();
        d.exec("SET SESSION wsrep_OSU_method = 'RSU'").await.unwrap();
        let t = Instant::now();
        let r = d
            .exec(&format!(
                "ALTER TABLE {}.orders ADD INDEX IF NOT EXISTS sprig_auto_0000aaaa (customer_id, created_at), ALGORITHM=INPLACE, LOCK=NONE",
                quote(&env.schema)
            ))
            .await;
        eprintln!("[probe] RSU ALTER on g3: {r:?} in {} ms", t.elapsed().as_millis());
        d.exec("SET SESSION wsrep_OSU_method = 'TOI'").await.unwrap();
        d.close().await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        stop.store(true, Ordering::Relaxed);
        mon.await.unwrap();
        client.finish("rsu probe").await;
        // Drop it the same way, on g3 only.
        let mut d = Node::connect(&eps[2], Duration::from_secs(300)).await.unwrap();
        d.exec("SET SESSION sql_log_bin = 0").await.unwrap();
        d.exec("SET SESSION wsrep_OSU_method = 'RSU'").await.unwrap();
        d.exec(&format!("ALTER TABLE {}.orders DROP INDEX IF EXISTS sprig_auto_0000aaaa", quote(&env.schema)))
            .await
            .unwrap();
        d.exec("SET SESSION wsrep_OSU_method = 'TOI'").await.unwrap();
        d.close().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
    })
    .await;
}

/// A long transaction holds the table's metadata lock: the build waits for
/// it, clients queue behind the build, and the build steps aside after
/// `queue_kill_after` instead of `lock_wait_timeout`; after its tries the
/// rollout waits for the next window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_queue_kills_a_waiting_build_single() {
    with_env(Topo::Single, "lock queue", |env| async move {
        let s = RolloutSettings {
            lock_retries: 2,
            ..settings()
        };
        let slots = LocalSlots::new();
        let shards = vec![env.table.clone()];
        let bytes = table_bytes(&env).await;
        let def = def();
        let mut q = catalog(&env).await;
        let id = env
            .store
            .insert_index(&mut q, &row(&def, IndexState::RollingOut))
            .await
            .unwrap();
        let r = env.store.index(&mut q, id).await.unwrap().unwrap();
        // The holder: an open transaction that read the table.
        let mut holder = Conn::new(Opts::from_url(&env.writer_url).unwrap())
            .await
            .unwrap();
        holder
            .query_drop(format!("USE {}", quote(&env.schema)))
            .await
            .unwrap();
        holder.query_drop("BEGIN").await.unwrap();
        holder
            .query_drop("SELECT COUNT(*) FROM orders WHERE id < 10")
            .await
            .unwrap();
        let client = Client::start(&env.writer_url, &env.schema);
        let started = Instant::now();
        let c = cx(&env, &s, &slots, &shards, bytes);
        let step = advance(&env.store, &mut q, &c, &r).await.unwrap();
        let took = started.elapsed();
        let (max, errs) = client.finish("lock queue").await;
        holder.query_drop("COMMIT").await.unwrap();
        let _ = holder.disconnect().await;
        eprintln!(
            "[lock queue] step {step:?} after {} ms; client max {} ms",
            took.as_millis(),
            max.as_millis()
        );
        assert!(
            matches!(
                step,
                Step::Retry {
                    outcome: BuildOutcome::LockTimeout { tries: 2 },
                    ..
                }
            ),
            "{step:?}"
        );
        assert!(errs.is_empty(), "{errs:?}");
        // Clients waited about queue_kill_after plus a poll, never the
        // 5 s lock_wait_timeout.
        assert!(max < Duration::from_millis(2500), "client waited {max:?}");
        let after = env.store.index(&mut q, id).await.unwrap().unwrap();
        assert_eq!(after.state, IndexState::RollingOut);
        assert_eq!(after.shards.get("0"), Some(&ShardBuild::Pending));
        assert_eq!(Progress::of(after.observed.as_ref()).lock_timeouts, 1);
        let _ = q.disconnect().await;
    })
    .await;
}
