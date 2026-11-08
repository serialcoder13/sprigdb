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

/// The target fingerprint runs fast now; usage samples don't use the index.
struct Workload;

#[async_trait::async_trait]
impl WorkloadSource for Workload {
    async fn latency(
        &self,
        _t: TenantId,
        _o: ObjectId,
        _s: SystemTime,
        _u: SystemTime,
    ) -> Result<Option<BTreeMap<Fingerprint, FpLatency>>> {
        Ok(Some(
            [(
                TARGET,
                FpLatency {
                    runs: 5000,
                    p95_ms: 11.0,
                    mean_ms: 8.0,
                    uses_index: Some(true),
                    write: false,
                },
            )]
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
        Ok(Some(Vec::new()))
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
                    unused_drop_days: 30,
                    ignore_before_drop_hours: 24,
                    recreate_block_days: 14,
                },
                rollout,
                observe: ObserveSettings::default(),
                validation: sprig_advisor::autoindex::validate::ValidationSettings::default(),
            },
        };
        let hooks = AutoIndexHooks {
            candidates: Arc::new(HandMade {
                url: url.clone(),
                schema: schema.clone(),
            }),
            validation: Arc::new(AcceptAll),
            workload: Arc::new(Workload),
            locks: Arc::new(NoMover),
            mover: None,
            marker: Arc::new(NoMarker),
            decisions: Arc::new(LogDecisions),
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

        // 31 days unused (usage samples never read it): ignored, then dropped.
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
