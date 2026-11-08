//! The automatic-indexing calls against the Compose etcd and MariaDB
//! single (`SPRIG_TEST_ETCD`, for example `http://127.0.0.1:2379`, and
//! `SPRIG_TEST_MARIADB_SINGLE`, for example
//! `mysql://root:<password>@127.0.0.1:13301`); skipped without both. The
//! tiers are private tiers of that etcd and the tenant's catalog is the
//! schema `__sprig_test_aiadm_<pid>_<tag>_<id>`; all are removed
//! afterwards, also when the case fails.
//!
//! Covers, with hand-made store rows: the module migrated on first use;
//! list, filters and history; show by id and by name; explain (the JSON
//! lookup of candidates by fingerprint, gates against the resolved
//! policy, a skipped table, validations, the index); approve, reject,
//! drop, pin and unpin through the store's conditional updates, refusals
//! (39202) and unknown indexes (39201); decisions recorded.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::collections::BTreeMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use sprig_advisor::autoindex::model::{Evidence as CandEvidence, KeyPart, PartSource, Role};
use sprig_advisor::autoindex::store::{
    Baseline, CandidateStatus, Definition, DropReason, IndexState, Model, ShardBuild, StoredKind,
    ValidationMethod, ValidationRow, Verdict,
};
use sprig_catalog::store::{
    DatabaseRow, DbState, Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState,
};
use sprig_catalog::{DirectoryAllocator, MariaBackend, VersionKind, Versioner};
use sprig_common::ids::{DbId, GroupId, ObjectId};
use sprig_common::time::SystemClock;
use sprig_meta::conformance::{ETCD_ENV, delete_prefix, unique_tier};
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore};
use sprig_settings::SettingsStores;

use super::view::Action;
use super::*;
use crate::admin::types::CreateTenantRequest;
use crate::catalogs::{CatalogNaming, ShardMapLocator, StaticPools};
use crate::tenants::{NoHooks, TenantService};
use crate::tree_settings::TreeSync;

const MARIADB_ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";

fn tag() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    format!(
        "{:x}{:x}",
        (nanos ^ std::process::id()) & 0xff_ffff,
        N.fetch_add(1, Ordering::Relaxed)
    )
}

struct Env {
    dc_store: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    url: String,
    tag: String,
}

impl Env {
    fn prefix(&self) -> String {
        format!("__sprig_test_aiadm_{}_{}_", std::process::id(), self.tag)
    }

    async fn cleanup(&self) {
        if let Ok(mut c) =
            mysql_async::Conn::new(mysql_async::Opts::from_url(&self.url).unwrap()).await
        {
            let like = format!(
                "__sprig\\_test\\_aiadm\\_{}\\_{}\\_%",
                std::process::id(),
                self.tag
            );
            let schemas: Vec<String> = c
                .exec(
                    "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE ?",
                    (like,),
                )
                .await
                .unwrap_or_default();
            for s in schemas {
                let _ = c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`")).await;
            }
            let _ = c.disconnect().await;
        }
        delete_prefix(&*self.dc_store, &self.dc_store.tier().root()).await;
        delete_prefix(&*self.global, &self.global.tier().root()).await;
    }
}

async fn with_env<F, Fut>(body: F)
where
    F: FnOnce(Arc<Env>) -> Fut,
    Fut: Future<Output = ()>,
{
    let (Some(etcd), Some(url)) = (
        std::env::var(ETCD_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty()),
        std::env::var(MARIADB_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty()),
    ) else {
        eprintln!("skipped the autoindex admin live case: set {ETCD_ENV} and {MARIADB_ENV}");
        return;
    };
    let cfg = EtcdConfig::new(vec![etcd]);
    let env = Arc::new(Env {
        dc_store: Arc::new(EtcdStore::connect(unique_tier(), &cfg).await.unwrap()),
        global: Arc::new(EtcdStore::connect(unique_tier(), &cfg).await.unwrap()),
        url,
        tag: tag(),
    });
    let r = AssertUnwindSafe(body(Arc::clone(&env)))
        .catch_unwind()
        .await;
    env.cleanup().await;
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

/// Remembers decisions.
#[derive(Default)]
struct Decisions(Mutex<Vec<IndexDecision>>);

#[async_trait::async_trait]
impl DecisionLog for Decisions {
    async fn record(&self, d: &IndexDecision) -> Result<Option<u64>> {
        self.0.lock().unwrap().push(d.clone());
        Ok(None)
    }
}

const FP: Fingerprint = Fingerprint(0x9c41_d2e0_7b3a_5f18);
const OTHER: Fingerprint = Fingerprint(0x51aa_02c4_e9d0_f337);

fn candidate(
    run: u64,
    object: u64,
    cols: &[&str],
    fps: Vec<(Fingerprint, Role)>,
    status: CandidateStatus,
) -> CandidateRow {
    CandidateRow {
        candidate_id: 0,
        run_id: run,
        object: ObjectId(object),
        def_hash: 1,
        kind: StoredKind::BTree,
        definition: Definition {
            parts: cols.iter().map(|c| KeyPart::col(c)).collect(),
            options: sprig_advisor::autoindex::model::IndexOptions::default(),
        },
        covering: false,
        fingerprints: fps,
        status,
        reason: None,
        benefit_ms_h: Some(190_500.0),
        write_ms_h: Some(7_200.0),
        est_bytes: Some(2_240_000_000),
        build_seconds: Some(112),
        evidence: CandEvidence {
            parts: vec![PartSource::Eq, PartSource::Order],
            selectivity: BTreeMap::from([("customer_id".to_owned(), 8.3e-6)]),
            ..CandEvidence::default()
        },
        created_at: SystemTime::now() - Duration::from_secs(3600),
    }
}

fn index(object: u64, name: &str, cand: u64, state: IndexState) -> AutoIndexRow {
    AutoIndexRow {
        index_id: sprig_advisor::autoindex::model::AutoIndexId(0),
        object: ObjectId(object),
        name: name.to_owned(),
        model: Model::Sql,
        kind: StoredKind::BTree,
        definition: Definition {
            parts: vec![KeyPart::col("customer_id"), KeyPart::col("created_at")],
            options: sprig_advisor::autoindex::model::IndexOptions::default(),
        },
        state,
        prior_state: None,
        candidate_id: cand,
        validation_id: None,
        decision_id: None,
        est_benefit_ms_h: 190_500.0,
        est_write_ms_h: 7_200.0,
        est_bytes: 2_240_000_000,
        actual_bytes: None,
        shards: BTreeMap::from([("1".to_owned(), ShardBuild::Built)]),
        baseline: BTreeMap::from([(
            FP.to_string(),
            Baseline {
                runs: 18_240,
                p95_ms: 412.0,
                mean_ms: 260.0,
            },
        )]),
        observed: None,
        last_used_at: None,
        drop_reason: None,
        created_at: SystemTime::now() - Duration::from_secs(1800),
        state_changed_at: SystemTime::now() - Duration::from_secs(1800),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_autoindex_admin() {
    with_env(|env| async move {
        let registry = Registry::global().unwrap();
        let prefix = env.prefix();
        let naming: CatalogNaming = Arc::new(move |t| format!("{prefix}{t}"));
        let pools = StaticPools::new()
            .with_url(
                sprig_common::ids::ClusterId::new("lt-c1").unwrap(),
                &env.url,
            )
            .unwrap();
        let locator = ShardMapLocator::new(Arc::clone(&env.dc_store), Arc::new(pools), naming);
        let v = Versioner::new(
            MariaBackend::new(Arc::new(locator.clone())),
            Arc::new(DirectoryAllocator::new(Arc::clone(&env.global))),
        );
        let directory = Directory::new(Arc::clone(&env.global));
        let tree = Arc::new(TreeSync::new(
            v.clone(),
            directory.clone(),
            Arc::new(SystemClock),
        ));
        let tenants = TenantService::new(
            Arc::clone(&env.dc_store),
            locator.clone(),
            tree,
            Arc::new(NoHooks),
            Arc::new(SystemClock),
            None,
        )
        .unwrap();
        let name = format!("test-{}-acme", env.tag);
        let t = tenants
            .create(
                &CreateTenantRequest {
                    name: name.clone(),
                    parent: None,
                    cluster: None,
                },
                &[],
            )
            .await
            .unwrap();
        let id = t.tenant.tenant_id;
        // shop.orders (1) and shop.events (2).
        let mut ch = v.change(id, VersionKind::Catalog).await.unwrap();
        let ver = ch.version();
        {
            let (s, q) = ch.tx().parts();
            s.insert_database(
                q,
                &DatabaseRow {
                    db_id: DbId(1),
                    name: "shop".into(),
                    name_key: "shop".into(),
                    default_charset: "utf8mb4".into(),
                    default_collation: "utf8mb4_bin".into(),
                    default_group_id: GroupId(0),
                    state: DbState::Active,
                    options: serde_json::json!({}),
                    created_at: SystemTime::now(),
                    created_by: None,
                    changed_version: ver,
                },
            )
            .await
            .unwrap();
            for (oid, table) in [(1, "orders"), (2, "events")] {
                s.insert_object(
                    q,
                    &ObjectRow {
                        object_id: ObjectId(oid),
                        db_id: DbId(1),
                        namespace: Namespace::Relation,
                        name: table.into(),
                        name_key: table.into(),
                        kind: ObjectKind::Table,
                        parent_id: None,
                        state: ObjectState::Active,
                        schema_version: 1,
                        flags: ObjectFlags::empty(),
                        pending_job_id: None,
                        created_at: SystemTime::now(),
                        changed_version: ver,
                    },
                )
                .await
                .unwrap();
            }
        }
        ch.commit().await.unwrap();

        let stores =
            SettingsStores::new(Arc::clone(&env.global), Arc::clone(&env.dc_store)).unwrap();
        let live = LiveSettings::start(registry, stores).unwrap();
        live.ready().await;
        let decisions = Arc::new(Decisions::default());
        let svc = AutoIndexService::new(
            directory,
            Arc::new(locator.clone()),
            v,
            live,
            Arc::new(SystemClock),
        )
        .with_decisions(Arc::clone(&decisions) as Arc<dyn DecisionLog>);

        // First use migrates the module: an empty list.
        let l = svc.list(&name, &ListIndexesQuery::default()).await.unwrap();
        assert!(l.indexes.is_empty());
        assert_eq!(l.tenant, name);

        // Hand-made evidence, as evaluation, validation and rollout write it.
        let ep = locator.writer(id).await.unwrap();
        let store = AutoIndexStore::new(ep.store.schema());
        let mut c = ep.pool.get_conn().await.unwrap();
        let c_active = store
            .insert_candidate(
                &mut c,
                &candidate(
                    7,
                    1,
                    &["customer_id", "created_at"],
                    vec![(FP, Role::Primary), (OTHER, Role::Merged)],
                    CandidateStatus::Selected,
                ),
            )
            .await
            .unwrap();
        let c_rec = store
            .insert_candidate(
                &mut c,
                &candidate(
                    7,
                    1,
                    &["status"],
                    vec![(OTHER, Role::Primary)],
                    CandidateStatus::Recommended,
                ),
            )
            .await
            .unwrap();
        let c_rec2 = store
            .insert_candidate(
                &mut c,
                &candidate(
                    7,
                    2,
                    &["account_id"],
                    vec![(OTHER, Role::Primary)],
                    CandidateStatus::Recommended,
                ),
            )
            .await
            .unwrap();
        let mut low = candidate(
            6,
            1,
            &["created_at"],
            vec![(FP, Role::Primary)],
            CandidateStatus::Rejected,
        );
        low.reason = Some("below_benefit_ratio".into());
        low.benefit_ms_h = Some(1_650.0);
        low.write_ms_h = Some(40_000.0);
        store.insert_candidate(&mut c, &low).await.unwrap();
        store
            .insert_candidate(
                &mut c,
                &CandidateRow::skipped(
                    7,
                    ObjectId(2),
                    FP,
                    "function:LOWER",
                    vec!["events.source".into()],
                    SystemTime::now(),
                ),
            )
            .await
            .unwrap();
        store
            .insert_validation(
                &mut c,
                &ValidationRow {
                    validation_id: 0,
                    candidate_id: c_active,
                    method: ValidationMethod::Replica,
                    cluster_id: "lt-c1".into(),
                    node: "n2".into(),
                    shard_id: 1,
                    started_at: SystemTime::now() - Duration::from_secs(3000),
                    finished_at: Some(SystemTime::now() - Duration::from_secs(2500)),
                    build_seconds: Some(118),
                    statements: 260,
                    noisy: 3,
                    target_p95_before: Some(405.0),
                    target_p95_after: Some(11.2),
                    worst_regression: Some(3.1),
                    worst_fp: Some(OTHER),
                    recall_at_k: None,
                    verdict: Verdict::Accepted,
                    reason: None,
                    details: serde_json::json!({"statements": [{"sample": 17, "a_ms": 400.0}]}),
                },
            )
            .await
            .unwrap();
        let i_active = store
            .insert_index(
                &mut c,
                &index(1, "sprig_auto_5be0c2a1", c_active, IndexState::Active),
            )
            .await
            .unwrap();
        let i_rec = store
            .insert_index(
                &mut c,
                &index(1, "sprig_auto_1f00d5aa", c_rec, IndexState::Recommended),
            )
            .await
            .unwrap();
        let i_rec2 = store
            .insert_index(
                &mut c,
                &index(2, "sprig_auto_2a2a2a2a", c_rec2, IndexState::Recommended),
            )
            .await
            .unwrap();
        let mut gone = index(2, "sprig_auto_0badc0de", 999, IndexState::Dropped);
        gone.drop_reason = Some(DropReason::NoGain);
        store.insert_index(&mut c, &gone).await.unwrap();
        drop(c);

        // List and filters.
        let names =
            |l: &ListIndexesResponse| l.indexes.iter().map(|i| i.name.clone()).collect::<Vec<_>>();
        let l = svc.list(&name, &ListIndexesQuery::default()).await.unwrap();
        assert_eq!(
            names(&l),
            [
                "sprig_auto_5be0c2a1",
                "sprig_auto_1f00d5aa",
                "sprig_auto_2a2a2a2a"
            ]
        );
        assert_eq!(l.indexes[0].db, "shop");
        assert_eq!(l.indexes[0].table, "orders");
        let all = svc
            .list(
                &name,
                &ListIndexesQuery {
                    all: Some(true),
                    ..ListIndexesQuery::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(all.indexes.len(), 4);
        let rec = svc
            .list(
                &name,
                &ListIndexesQuery {
                    state: Some("recommended".into()),
                    table: Some("events".into()),
                    ..ListIndexesQuery::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(names(&rec), ["sprig_auto_2a2a2a2a"]);

        // Show, by name and id.
        let d = svc
            .show(&name, "shop.orders.sprig_auto_5be0c2a1")
            .await
            .unwrap();
        assert_eq!(d.index.index_id, i_active.0);
        assert_eq!(d.candidate.as_ref().unwrap().candidate_id, c_active);
        assert_eq!(d.validations[0].verdict, "accepted");
        assert_eq!(d.validations[0].improvement_pct, Some(97.2));
        let j = serde_json::to_string(&d).unwrap();
        assert!(
            !j.contains("sample") && !j.contains("a_ms"),
            "no sample details: {j}"
        );
        assert_eq!(
            svc.show(&name, &i_rec.0.to_string())
                .await
                .unwrap()
                .index
                .name,
            "sprig_auto_1f00d5aa"
        );
        let e = svc.show(&name, "sprig_auto_ffffffff").await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::AutoIndexNotFound));

        // Explain: candidates found through the fingerprint list (as
        // primary and as merged), the skipped table, the gates.
        let x = svc.explain_query(&name, &FP.to_string()).await.unwrap();
        assert_eq!(x.candidates.len(), 3, "{x:#?}");
        assert_eq!(
            x.candidates[0].candidate.columns,
            ["customer_id", "created_at"]
        );
        assert_eq!(x.candidates[0].role, "primary");
        assert_eq!(x.candidates[0].index.as_ref().unwrap().state, "active");
        assert_eq!(x.candidates[0].validations.len(), 1);
        assert!(
            x.candidates[0]
                .gates
                .iter()
                .take(4)
                .all(|g| g.passed == Some(true))
        );
        assert!(
            x.summary[0].contains("index sprig_auto_5be0c2a1 (customer_id, created_at) is active"),
            "{:?}",
            x.summary
        );
        assert!(
            x.summary
                .iter()
                .any(|l| l.contains("no candidate on shop.events") && l.contains("LOWER()")),
            "{:?}",
            x.summary
        );
        assert!(
            x.summary.iter().any(|l| l.contains("1 earlier evaluation")),
            "{:?}",
            x.summary
        );
        assert_eq!(x.policies[0].mode, "on");
        let low = x
            .candidates
            .iter()
            .find(|c| c.candidate.run_id == 6)
            .unwrap();
        assert_eq!(low.gates[1].passed, Some(false));
        let other = svc.explain_query(&name, &OTHER.to_string()).await.unwrap();
        assert_eq!(other.candidates.len(), 3);
        assert_eq!(
            other
                .candidates
                .iter()
                .find(|c| c.candidate.candidate_id == c_active)
                .unwrap()
                .role,
            "merged"
        );
        let none = svc.explain_query(&name, "0000000000000001").await.unwrap();
        assert!(none.candidates.is_empty());
        assert!(none.summary[0].starts_with("no candidate recorded"));
        let e = svc.explain_query(&name, "xyz").await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::AdminRequestInvalid));

        // Actions.
        let req = IndexActionRequest {
            reason: "reviewed".into(),
            actor: Some("kiran".into()),
        };
        let a = svc
            .act(&name, &i_rec.0.to_string(), Action::Approve, &req)
            .await
            .unwrap();
        assert!(a.changed);
        assert_eq!(
            (a.from.as_str(), a.to.as_str()),
            ("recommended", "rolling_out")
        );
        let again = svc
            .act(&name, &i_rec.0.to_string(), Action::Approve, &req)
            .await
            .unwrap();
        assert!(!again.changed);
        let r = svc
            .act(&name, "events.sprig_auto_2a2a2a2a", Action::Reject, &req)
            .await
            .unwrap();
        assert_eq!(r.to, "dropped");
        assert_eq!(r.index.drop_reason.as_deref(), Some("dismissed"));
        let p = svc
            .act(&name, &i_active.0.to_string(), Action::Pin, &req)
            .await
            .unwrap();
        assert_eq!(p.to, "frozen");
        let e = svc
            .act(&name, &i_active.0.to_string(), Action::Drop, &req)
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::AutoIndexStateInvalid));
        assert!(e.message.contains("unpin it first"), "{}", e.message);
        let u = svc
            .act(&name, &i_active.0.to_string(), Action::Unpin, &req)
            .await
            .unwrap();
        assert_eq!((u.from.as_str(), u.to.as_str()), ("frozen", "active"));
        let dr = svc
            .act(&name, &i_active.0.to_string(), Action::Drop, &req)
            .await
            .unwrap();
        assert_eq!(dr.to, "dropping");
        assert_eq!(dr.index.drop_reason.as_deref(), Some("operator"));
        let e = svc
            .act(&name, &i_rec2.0.to_string(), Action::Approve, &req)
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::AutoIndexStateInvalid));

        // The rows really moved (rollout reads rolling_out from here).
        let mut c = ep.pool.get_conn().await.unwrap();
        let queued = store
            .indexes_in(&mut c, &[IndexState::RollingOut])
            .await
            .unwrap();
        assert_eq!(
            queued.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
            ["sprig_auto_1f00d5aa"]
        );
        drop(c);
        let kinds: Vec<&str> = decisions.0.lock().unwrap().iter().map(|d| d.kind).collect();
        assert_eq!(
            kinds,
            [
                "index.approve",
                "index.reject",
                "index.pin",
                "index.unpin",
                "index.drop"
            ]
        );
        let d0 = decisions.0.lock().unwrap()[0].clone();
        assert!(
            d0.summary
                .starts_with("op:kiran approve sprig_auto_1f00d5aa on shop.orders"),
            "{}",
            d0.summary
        );
        assert_eq!(d0.evidence["reason"], "reviewed");

        // Unknown tenant.
        let e = svc
            .list("no-such-tenant", &ListIndexesQuery::default())
            .await
            .unwrap_err();
        assert_eq!(e.protocol().mysql_errno, 39072);
    })
    .await;
}
