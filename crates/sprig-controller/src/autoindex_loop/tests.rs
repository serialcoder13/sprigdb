//! The `auto_index` loop: bootstrap modules, and one tenant's whole flow on
//! the Compose single node with hand-made candidates (trigger evaluation,
//! step 2, isn't built yet): proposed → validated → built under the build
//! slot → observed → active, then unused → ignored → dropped.
//!
//! The live case needs `SPRIG_TEST_MARIADB_SINGLE`
//! (`mysql://root:sprig-dev-root@127.0.0.1:13301`) and works in its own
//! schema `__sprig_test_rollout_loop_<pid>`, dropped afterwards.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};
use sprig_advisor::autoindex::model::{AutoIndexId, IndexDef};
use sprig_advisor::autoindex::observe::{FpLatency, ObserveSettings, UsageSample, WorkloadSource};
use sprig_advisor::autoindex::policy::{Mode, ObserveRules};
use sprig_advisor::autoindex::rollout::driver::Step;
use sprig_advisor::autoindex::rollout::{
    ClusterView, LocalSlots, NoMover, Nodes, RolloutSettings, ShardTable,
};
use sprig_advisor::autoindex::store::{
    AutoIndexStore, Baseline, DDL_AUTO_INDEXES, DDL_CANDIDATES, DDL_INDEX_ORIGINS, DDL_VALIDATIONS,
    Definition, DropReason, IndexState, LogDecisions, Model, StoredKind,
};
use sprig_advisor::autoindex::validate::NoMarker;
use sprig_cluster::driver::DdlMethod;
use sprig_cluster::sql::NodeEndpoint;
use sprig_common::ids::{ClusterId, Fingerprint, NodeId, ObjectId, TenantId};
use sprig_common::names::quote;

use super::*;
use crate::tenants::NoHooks;

#[test]
fn bootstrap_migrates_the_autoindex_module_once() {
    let names = |m: Vec<Module>| m.into_iter().map(|m| m.name).collect::<Vec<_>>();
    assert_eq!(names(WithAutoIndex(NoHooks).modules()), vec!["autoindex"]);
    let twice = WithAutoIndex(WithAutoIndex(NoHooks));
    assert_eq!(names(twice.modules()), vec!["autoindex"]);
    let inner = names(crate::users::AuthHooks.modules());
    let auth = names(WithAutoIndex(crate::users::AuthHooks).modules());
    assert_eq!(auth.len(), inner.len() + 1);
    assert_eq!(auth.last().copied(), Some("autoindex"));
}

const OBJECT: ObjectId = ObjectId(77);
const TENANT: TenantId = TenantId(7);
const TARGET: Fingerprint = Fingerprint(0x9c41_d2e0_7b3a_5f18);

struct TestEnv {
    url: String,
    schema: String,
    object: ObjectEnv,
}

#[async_trait::async_trait]
impl AutoIndexEnv for TestEnv {
    async fn catalog(&self, _tenant: TenantId) -> Result<(Conn, AutoIndexStore)> {
        let c = Conn::new(Opts::from_url(&self.url).unwrap()).await.unwrap();
        Ok((c, AutoIndexStore::new(&self.schema)))
    }
    async fn objects(
        &self,
        _tenant: TenantId,
        objects: &BTreeSet<ObjectId>,
    ) -> Result<BTreeMap<ObjectId, ObjectEnv>> {
        Ok(objects
            .iter()
            .filter(|o| **o == OBJECT)
            .map(|o| (*o, self.object.clone()))
            .collect())
    }
    async fn external_builds(&self, _cluster: &ClusterId) -> Result<u32> {
        Ok(0)
    }
}

/// Trigger evaluation stand-in: writes one `validating` row, as the
/// evaluator does, the first time.
struct HandMade {
    url: String,
    schema: String,
}

fn hand_made_def() -> IndexDef {
    IndexDef::btree(OBJECT, &["customer_id", "created_at"])
}

#[async_trait::async_trait]
impl CandidateSource for HandMade {
    async fn evaluate(&self, _tenant: TenantId, now: SystemTime) -> Result<Evaluated> {
        let mut c = Conn::new(Opts::from_url(&self.url).unwrap()).await.unwrap();
        let store = AutoIndexStore::new(&self.schema);
        let def = hand_made_def();
        if store
            .index_by_name(&mut c, OBJECT, &def.name())
            .await?
            .is_none()
        {
            let row = AutoIndexRow {
                index_id: AutoIndexId(0),
                object: OBJECT,
                name: def.name(),
                model: Model::Sql,
                kind: StoredKind::BTree,
                definition: Definition::of(&def),
                state: IndexState::Validating,
                prior_state: None,
                candidate_id: 1,
                validation_id: None,
                decision_id: None,
                est_benefit_ms_h: 190_500.0,
                est_write_ms_h: 7_200.0,
                est_bytes: 20 << 20,
                actual_bytes: None,
                shards: BTreeMap::new(),
                baseline: [(
                    TARGET.to_string(),
                    Baseline {
                        runs: 18_240,
                        p95_ms: 412.0,
                        mean_ms: 260.0,
                    },
                )]
                .into(),
                observed: None,
                last_used_at: None,
                drop_reason: None,
                created_at: now,
                state_changed_at: now,
            };
            store.insert_index(&mut c, &row).await?;
        }
        let _ = c.disconnect().await;
        Ok(Evaluated::default())
    }
}

struct AcceptAll;

#[async_trait::async_trait]
impl ValidationHook for AcceptAll {
    async fn validate(
        &self,
        _tenant: TenantId,
        _row: &AutoIndexRow,
        _object: &ObjectEnv,
    ) -> Result<Validated> {
        Ok(Validated::Accepted {
            validation_id: None,
        })
    }
}

/// The target fingerprint runs fast now, from a day after the start; its
/// sample is a primary-key lookup, which never uses the index.
struct Workload {
    schema: String,
    reads_from: SystemTime,
}

#[async_trait::async_trait]
impl WorkloadSource for Workload {
    async fn latency(
        &self,
        _t: TenantId,
        _o: ObjectId,
        _s: SystemTime,
        u: SystemTime,
    ) -> Result<Option<BTreeMap<Fingerprint, FpLatency>>> {
        let runs = if u > self.reads_from { 5000 } else { 0 };
        Ok(Some(
            [
                (
                    TARGET,
                    FpLatency {
                        uses_index: Some(true),
                        ..FpLatency::read(runs, 11.0, 8.0)
                    },
                ),
                // Inserts: an index on a table nobody writes is kept.
                (
                    Fingerprint(0x77),
                    FpLatency {
                        write: true,
                        affected_per_run: 1.0,
                        ..FpLatency::read(runs / 10, 0.5, 0.4)
                    },
                ),
            ]
            .into(),
        ))
    }
    async fn samples(
        &self,
        _t: TenantId,
        _o: ObjectId,
        _n: usize,
        _since: SystemTime,
    ) -> Result<Option<Vec<UsageSample>>> {
        Ok(Some(vec![UsageSample {
            fingerprint: TARGET,
            schema: self.schema.clone(),
            sql: "SELECT id FROM orders WHERE id = 1".into(),
        }]))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_tenant_from_candidate_to_drop() {
    let Some(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE")
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    let schema = format!("__sprig_test_rollout_loop_{}", std::process::id());
    let mut c = Conn::new(Opts::from_url(&url).unwrap()).await.unwrap();
    c.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&schema)))
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
        sprig_advisor::autoindex::store::DDL_INDEX_USAGE,
    ] {
        c.query_drop(ddl).await.unwrap();
    }
    c.query_drop(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY AUTO_INCREMENT, customer_id INT NOT NULL, \
         created_at DATETIME NOT NULL, total DECIMAL(10,2) NOT NULL) ENGINE=InnoDB",
    )
    .await
    .unwrap();
    c.query_drop(
        "INSERT INTO orders (customer_id, created_at, total) SELECT seq % 500, \
         '2026-01-01' + INTERVAL seq MINUTE, seq % 100 FROM seq_1_to_50000",
    )
    .await
    .unwrap();

    let r = std::panic::AssertUnwindSafe(async {
        let ep = NodeEndpoint::from_url(NodeId::new("single1").unwrap(), &url).unwrap();
        let rollout = RolloutSettings {
            rollout_pause: Duration::ZERO,
            lock_retry_pause: Duration::from_secs(1),
            retry_after: Duration::from_secs(1),
            ..RolloutSettings::default()
        };
        let env = TestEnv {
            url: url.clone(),
            schema: schema.clone(),
            object: ObjectEnv {
                object: OBJECT,
                view: ClusterView {
                    cluster: ClusterId::new("dev-single").unwrap(),
                    nodes: Nodes::Single { writer: ep },
                    ddl_method: DdlMethod::Online,
                    instant_ignored_toggle: true,
                    unhealthy: None,
                },
                shards: vec![ShardTable {
                    shard: "0".into(),
                    schema: schema.clone(),
                    table: "orders".into(),
                }],
                mode: Mode::On,
                rules: ObserveRules {
                    min_improvement_pct: 30.0,
                    max_regression_pct: 10.0,
                    observe_days: 7,
                    recreate_block_days: 14,
                    usage_sample_reads: 1000,
                    min_usage_ratio: 0.05,
                    retire_user_indexes: Mode::On,
                    maintenance_cost_ms: 0.05,
                },
                rollout,
                observe: ObserveSettings::default(),
                validation: sprig_advisor::autoindex::validate::ValidationSettings::default(),
                db: None,
            },
        };
        let hooks = AutoIndexHooks {
            candidates: Arc::new(HandMade {
                url: url.clone(),
                schema: schema.clone(),
            }),
            validation: Arc::new(AcceptAll),
            workload: Arc::new(Workload {
                schema: schema.clone(),
                reads_from: SystemTime::now() + Duration::from_secs(86_400),
            }),
            locks: Arc::new(NoMover),
            mover: None,
            marker: Arc::new(NoMarker),
            decisions: Arc::new(LogDecisions),
            docs: None,
        };
        let slots = LocalSlots::new();
        let cadence = Cadence::default();
        let mut mem = Memory::default();
        let t0 = SystemTime::now();

        // Pass 1: proposed, validated, built, observing.
        let rep = tenant_pass(&env, &hooks, &slots, TENANT, t0, &mut mem, &cadence)
            .await
            .unwrap();
        assert!(rep.errors.is_empty(), "{:?}", rep.errors);
        assert!(rep.evaluated);
        let id = {
            let (mut q, store) = env.catalog(TENANT).await.unwrap();
            let r = store
                .index_by_name(&mut q, OBJECT, &hand_made_def().name())
                .await
                .unwrap()
                .unwrap();
            let _ = q.disconnect().await;
            r.index_id
        };
        assert_eq!(rep.validated, vec![(id, IndexState::RollingOut)]);
        assert!(
            matches!(rep.rollout.as_slice(), [(i, Step::Observing { .. })] if *i == id),
            "{:?}",
            rep.rollout
        );
        assert!(rep.next_in <= cadence.max_requeue);
        // The build slot was released.
        assert_eq!(slots.held(&env.object.view.cluster), 0);
        // Not due again: no evaluation within eval_every.
        let rep = tenant_pass(&env, &hooks, &slots, TENANT, t0, &mut mem, &cadence)
            .await
            .unwrap();
        assert!(!rep.evaluated);
        assert!(rep.validated.is_empty());

        // Pass after the watch period: active.
        let t8 = t0 + Duration::from_secs(8 * 86_400);
        let rep = tenant_pass(&env, &hooks, &slots, TENANT, t8, &mut mem, &cadence)
            .await
            .unwrap();
        assert!(rep.errors.is_empty(), "{:?}", rep.errors);
        assert!(
            rep.observed
                .iter()
                .any(|(i, s)| *i == id && *s == sprig_advisor::autoindex::observe::ObserveStep::Activated),
            "{:?}",
            rep.observed
        );

        // 5,000 reads that never use it (decision O1: judged by reads, the
        // clock only moves the cadence): ignored, then 5,000 more and dropped.
        let t40 = t0 + Duration::from_secs(40 * 86_400);
        let rep = tenant_pass(&env, &hooks, &slots, TENANT, t40, &mut mem, &cadence)
            .await
            .unwrap();
        assert!(
            rep.observed.iter().any(|(i, s)| *i == id
                && *s == sprig_advisor::autoindex::observe::ObserveStep::Retiring(DropReason::Unused)),
            "{:?}",
            rep.observed
        );
        let t42 = t40 + Duration::from_secs(2 * 86_400);
        let rep = tenant_pass(&env, &hooks, &slots, TENANT, t42, &mut mem, &cadence)
            .await
            .unwrap();
        assert!(
            rep.observed.iter().any(|(i, s)| *i == id
                && *s == sprig_advisor::autoindex::observe::ObserveStep::Dropped(Some(DropReason::Unused))),
            "{:?}",
            rep.observed
        );
        let (mut q, store) = env.catalog(TENANT).await.unwrap();
        let row = store.index(&mut q, id).await.unwrap().unwrap();
        assert_eq!(row.state, IndexState::Dropped);
        let n: Option<u64> = q
            .query_first(format!(
                "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '{schema}' \
                 AND INDEX_NAME LIKE 'sprig\\_auto\\_%'"
            ))
            .await
            .unwrap();
        assert_eq!(n, Some(0));
        let _ = q.disconnect().await;
    })
    .catch_unwind()
    .await;
    let mut c = Conn::new(Opts::from_url(&url).unwrap()).await.unwrap();
    c.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&schema)))
        .await
        .unwrap();
    let _ = c.disconnect().await;
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

/// The R0 catalog-connection deadlock: an `auto_index` pass held a pooled
/// catalog connection while its hooks (the catalog snapshot, evaluation,
/// workload reads) took another from the same pool; with the loop's four
/// workers on a pool of four, every pass waited for a fifth connection
/// forever, and every other catalog call of the cluster timed out.
///
/// Hooks here take a short connection from the shared budget, as the real
/// ones do. Wired the old way (the pass's connection from the shared
/// budget) the workers deadlock; wired the new way (the loop's own budget,
/// `SESSIONS`) many passes and short callers run beside each other, no
/// short caller waits long, and both budgets end empty.
mod budgets {
    use std::sync::atomic::{AtomicU64, Ordering};

    use mysql_async::Pool;
    use sprig_advisor::autoindex::observe::{FpLatency, UsageSample, WorkloadSource};

    use super::*;
    use crate::catalogs::{PoolStats, StaticPools, acquire, pool_stats};

    /// A short catalog call: one connection of `pool`, a few milliseconds.
    async fn leaf(pool: &Pool, what: &str) -> Result<()> {
        let mut c = acquire(pool, what).await?;
        c.query_drop("SELECT SLEEP(0.005)")
            .await
            .map_err(|e| SprigError::internal("leaf").with_source(e))?;
        Ok(())
    }

    struct PoolEnv {
        sessions: Pool,
        shared: Pool,
        schema: String,
    }

    #[async_trait::async_trait]
    impl AutoIndexEnv for PoolEnv {
        async fn catalog(&self, _t: TenantId) -> Result<(Conn, AutoIndexStore)> {
            let c = acquire(&self.sessions, "pass").await?;
            Ok((c, AutoIndexStore::new(&self.schema)))
        }
        async fn objects(
            &self,
            _t: TenantId,
            _o: &BTreeSet<ObjectId>,
        ) -> Result<BTreeMap<ObjectId, ObjectEnv>> {
            leaf(&self.shared, "snapshot").await?;
            Ok(BTreeMap::new())
        }
        async fn external_builds(&self, _c: &ClusterId) -> Result<u32> {
            Ok(0)
        }
    }

    struct Shared(Pool);

    #[async_trait::async_trait]
    impl CandidateSource for Shared {
        async fn evaluate(&self, _t: TenantId, _now: SystemTime) -> Result<Evaluated> {
            leaf(&self.0, "evaluate").await?;
            Ok(Evaluated::default())
        }
    }

    #[async_trait::async_trait]
    impl WorkloadSource for Shared {
        async fn latency(
            &self,
            _t: TenantId,
            _o: ObjectId,
            _s: SystemTime,
            _u: SystemTime,
        ) -> Result<Option<BTreeMap<Fingerprint, FpLatency>>> {
            Ok(None)
        }
        async fn samples(
            &self,
            _t: TenantId,
            _o: ObjectId,
            _n: usize,
            _since: SystemTime,
        ) -> Result<Option<Vec<UsageSample>>> {
            Ok(None)
        }
        async fn objects_read(
            &self,
            _t: TenantId,
            _s: SystemTime,
            _u: SystemTime,
        ) -> Result<Option<BTreeSet<ObjectId>>> {
            leaf(&self.0, "workload").await?;
            Ok(Some(BTreeSet::new()))
        }
    }

    fn hooks(shared: &Pool) -> AutoIndexHooks {
        AutoIndexHooks {
            candidates: Arc::new(Shared(shared.clone())),
            validation: Arc::new(AcceptAll),
            workload: Arc::new(Shared(shared.clone())),
            locks: Arc::new(NoMover),
            mover: None,
            marker: Arc::new(NoMarker),
            decisions: Arc::new(LogDecisions),
            docs: None,
        }
    }

    /// `n` passes, `WORKERS` at a time as the runtime runs them, each a
    /// fresh tenant memory (so evaluation and the user pass run every time).
    async fn passes(env: Arc<PoolEnv>, hooks: AutoIndexHooks, n: usize) -> usize {
        let slots = Arc::new(LocalSlots::new());
        let mut set = tokio::task::JoinSet::new();
        let mut ok = 0;
        for i in 0..n {
            while set.len() >= WORKERS {
                if let Some(Ok(true)) = set.join_next().await {
                    ok += 1;
                }
            }
            let (env, hooks, slots) = (Arc::clone(&env), hooks.clone(), Arc::clone(&slots));
            set.spawn(async move {
                let mut mem = Memory::default();
                let t = TenantId(1 + (i as u64 % 12));
                tenant_pass(
                    &*env,
                    &hooks,
                    &slots,
                    t,
                    SystemTime::now(),
                    &mut mem,
                    &Cadence::default(),
                )
                .await
                .is_ok_and(|r| r.errors.is_empty())
            });
        }
        while let Some(r) = set.join_next().await {
            if matches!(r, Ok(true)) {
                ok += 1;
            }
        }
        ok
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn passes_never_starve_the_shared_catalog_budget() {
        let Some(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE")
            .ok()
            .filter(|v| !v.trim().is_empty())
        else {
            eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
            return;
        };
        let schema = format!("__sprig_test_pool_budget_{}", std::process::id());
        let mut c = Conn::new(Opts::from_url(&url).unwrap()).await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&schema)))
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
            sprig_advisor::autoindex::store::DDL_INDEX_USAGE,
        ] {
            c.query_drop(ddl).await.unwrap();
        }
        let cluster = ClusterId::new("dev-single").unwrap();
        let pool = |max: usize| {
            let p = StaticPools::new()
                .with_url_max(cluster.clone(), &url, max)
                .unwrap();
            futures::executor::block_on(crate::catalogs::ClusterPools::writer(&p, &cluster))
                .unwrap()
        };

        // The old wiring: the pass's connection from the shared budget of
        // four. Within seconds all four are held by passes waiting for a
        // fifth.
        let shared = pool(crate::catalogs::POOL_MAX);
        let env = Arc::new(PoolEnv {
            sessions: shared.clone(),
            shared: shared.clone(),
            schema: schema.clone(),
        });
        let old = tokio::spawn(passes(env, hooks(&shared), 40));
        tokio::time::sleep(Duration::from_secs(3)).await;
        let stuck = pool_stats("old", &cluster, &shared);
        eprintln!(
            "old wiring after 3 s: {stuck:?}, finished: {}",
            old.is_finished()
        );
        assert!(!old.is_finished(), "the old wiring didn't deadlock");
        assert_eq!(stuck.in_use, crate::catalogs::POOL_MAX);
        assert!(stuck.waiting >= WORKERS, "{stuck:?}");
        old.abort();
        let _ = old.await;
        let _ = shared.disconnect().await;

        // The new wiring: the loop's own budget.
        let shared = pool(crate::catalogs::POOL_MAX);
        let sessions = pool(SESSIONS);
        let env = Arc::new(PoolEnv {
            sessions: sessions.clone(),
            shared: shared.clone(),
            schema: schema.clone(),
        });
        let stop = sprig_common::CancellationToken::new();
        let worst = Arc::new(AtomicU64::new(0));
        let calls = Arc::new(AtomicU64::new(0));
        let mut short = Vec::new();
        for _ in 0..8 {
            let (shared, stop, worst, calls) = (
                shared.clone(),
                stop.clone(),
                Arc::clone(&worst),
                Arc::clone(&calls),
            );
            short.push(tokio::spawn(async move {
                while !stop.is_cancelled() {
                    let t0 = std::time::Instant::now();
                    leaf(&shared, "settings").await.unwrap();
                    let ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);
                    worst.fetch_max(ms, Ordering::Relaxed);
                    calls.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        let t0 = std::time::Instant::now();
        let n = 400;
        let ok = tokio::time::timeout(Duration::from_secs(60), passes(env, hooks(&shared), n))
            .await
            .expect("passes stalled with their own budget");
        stop.cancel();
        for s in short {
            s.await.unwrap();
        }
        let took = t0.elapsed();
        let (a, b): (PoolStats, PoolStats) = (
            pool_stats("catalog", &cluster, &shared),
            pool_stats("autoindex", &cluster, &sessions),
        );
        eprintln!(
            "new wiring: {ok}/{n} passes in {:.1} s beside {} short calls, the slowest {} ms; \
             shared {a:?}, sessions {b:?}",
            took.as_secs_f64(),
            calls.load(Ordering::Relaxed),
            worst.load(Ordering::Relaxed)
        );
        assert_eq!(ok, n);
        assert!(worst.load(Ordering::Relaxed) < 2_000);
        assert_eq!((a.in_use, a.waiting, b.in_use, b.waiting), (0, 0, 0, 0));
        let _ = shared.disconnect().await;
        let _ = sessions.disconnect().await;
        c.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&schema)))
            .await
            .unwrap();
        let _ = c.disconnect().await;
    }
}

/// A pass that hangs (here waiting for its catalog connection) gives up at
/// `Cadence::pass_limit`; a build's time doesn't count against it.
#[tokio::test(start_paused = true)]
async fn a_stuck_pass_gives_up_at_its_limit() {
    struct Stuck;

    #[async_trait::async_trait]
    impl AutoIndexEnv for Stuck {
        async fn catalog(&self, _t: TenantId) -> Result<(Conn, AutoIndexStore)> {
            std::future::pending().await
        }
        async fn objects(
            &self,
            _t: TenantId,
            _o: &BTreeSet<ObjectId>,
        ) -> Result<BTreeMap<ObjectId, ObjectEnv>> {
            Ok(BTreeMap::new())
        }
        async fn external_builds(&self, _c: &ClusterId) -> Result<u32> {
            Ok(0)
        }
    }
    let hooks = AutoIndexHooks {
        candidates: Arc::new(NoCandidates),
        validation: Arc::new(AcceptAll),
        workload: Arc::new(sprig_advisor::autoindex::observe::NoWorkload),
        locks: Arc::new(NoMover),
        mover: None,
        marker: Arc::new(NoMarker),
        decisions: Arc::new(LogDecisions),
            docs: None,
    };
    let cadence = Cadence {
        pass_limit: Duration::from_secs(600),
        ..Cadence::default()
    };
    let t0 = tokio::time::Instant::now();
    let e = tenant_pass(
        &Stuck,
        &hooks,
        &LocalSlots::new(),
        TENANT,
        SystemTime::now(),
        &mut Memory::default(),
        &cadence,
    )
    .await
    .unwrap_err();
    assert!(e.to_string().contains("ran over its 600 s limit"), "{e}");
    assert_eq!(t0.elapsed(), Duration::from_secs(600));

    // The budget: a build extends it while it runs, and only by its time.
    let b = PassBudget::new(Duration::from_secs(10));
    b.extend(Duration::from_secs(3600));
    tokio::time::advance(Duration::from_secs(100)).await;
    b.retract(Duration::from_secs(3600 - 100));
    let t1 = tokio::time::Instant::now();
    b.expired().await;
    assert_eq!(t1.elapsed(), Duration::from_secs(10));
}
