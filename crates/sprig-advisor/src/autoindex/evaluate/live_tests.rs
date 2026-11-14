//! A live evaluation against the Compose MariaDB (11.8): a real table, its
//! real statistics (collected by the evaluation's own analyze), the
//! autoindex catalog tables, and a synthetic window.
//!
//! `SPRIG_TEST_MARIADB_SINGLE` (for example
//! `mysql://root:<password>@127.0.0.1:13301`) names the server; without
//! it the case is skipped with a message. The case works in its own schema
//! `__sprig_test_autoindex_eval_<pid>_<nanos>_<n>`, dropped afterwards, also
//! when it fails.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, Pool};
use sprig_catalog::Migrator;
use sprig_catalog::migrate::MigrateOpts;
use sprig_common::ids::DcId;
use sprig_common::names::quote;
use sprig_common::time::ManualClock;
use sprig_settings::MemSettings;
use sprig_workload::shape::Shape;

use super::mem::MemDecisions;
use super::synth::{TENANT, directory, probed, registry, sampled, win};
use super::*;
use crate::autoindex::qualify::{SourcedWindow, StaticSource};
use crate::autoindex::store::AUTOINDEX_MODULE;

const MARIADB_ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";
const ORDERS: ObjectId = ObjectId(1201);

fn unique() -> u64 {
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::Relaxed)
}

/// Runs `body` with a pool and a fresh schema; drops the schema
/// afterwards. Skips without `SPRIG_TEST_MARIADB_SINGLE`.
async fn with_schema<F, Fut>(case: &str, body: F)
where
    F: FnOnce(Pool, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let Some(url) = std::env::var(MARIADB_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipped {case}: set {MARIADB_ENV} to run it");
        return;
    };
    let pool = Pool::new(Opts::from_url(&url).unwrap());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let schema = format!(
        "__sprig_test_autoindex_eval_{}_{nanos:x}_{}",
        std::process::id(),
        unique()
    );
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(format!("CREATE DATABASE {}", quote(&schema)))
        .await
        .unwrap();
    drop(c);
    let r = AssertUnwindSafe(body(pool.clone(), schema.clone()))
        .catch_unwind()
        .await;
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE IF EXISTS {}", quote(&schema)))
        .await
        .unwrap();
    drop(c);
    pool.disconnect().await.unwrap();
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

/// 20,000 orders: 500 customers (40 rows each), status `paid` 95%,
/// `open` 4%, `hold` 1%, one a minute from 2026-01-01; a user index on
/// `created_at`, as in example 1. A 300-byte `note` gives the rows
/// a realistic width: a new index must fit in 20% of the table.
const TABLE: &str = "CREATE TABLE orders (
  id          INT PRIMARY KEY AUTO_INCREMENT,
  customer_id INT NOT NULL,
  status      VARCHAR(16) NOT NULL COLLATE utf8mb4_uca1400_ai_ci,
  created_at  DATETIME NOT NULL,
  total       DECIMAL(10,2) NOT NULL,
  note        VARCHAR(400) NOT NULL,
  KEY idx_created (created_at)
) ENGINE=InnoDB";

const FILL: &str = "INSERT INTO orders (customer_id, status, created_at, total, note)
  SELECT seq % 500, ELT(1 + (seq % 25 = 0) + (seq % 100 = 0), 'paid', 'open', 'hold'),
         '2026-01-01' + INTERVAL seq MINUTE, seq / 10, REPEAT('x', 300)
  FROM seq_1_to_20000";

/// Example 1's statement on this table: `SELECT id, total, created_at
/// FROM orders WHERE customer_id = ? AND status IN (...) ORDER BY
/// created_at DESC LIMIT ?`, 2,000 runs in the window, mean 40 ms, every
/// run walking `idx_created` (20,000 rows) to return 18.
fn window() -> FpWindowStats {
    let shape = Shape::from_json(
        &serde_json::json!({
            "v": 1, "type": "select",
            "tables": [{"alias": "o", "object_id": ORDERS.0, "table": "orders"}],
            "predicates": [
                {"t": "o", "col": "customer_id", "op": "eq", "args": [0]},
                {"t": "o", "col": "status", "op": "in", "list": true, "args": [1]}],
            "joins": [],
            "order_by": [{"t": "o", "col": "created_at", "dir": "desc"}],
            "group_by": [], "select": {"o": ["created_at", "id", "total"]},
            "limit": true, "limit_arg": 2, "writes": null, "or_groups": 0})
        .to_string(),
    )
    .unwrap();
    let mut w = probed(
        win(0x9c41, Arc::new(shape), 2_000, 40.0, 120.0),
        40,
        20_000.0,
        18.0,
    );
    w.stats.in_list_max = 2;
    w.samples = (0..20)
        .map(|i| {
            sampled(&[
                Literal::Value(Param::Num(f64::from(i * 7))),
                Literal::List(vec![Param::Text("open".into()), Param::Text("hold".into())]),
                Literal::Value(Param::Num(20.0)),
            ])
        })
        .collect();
    w
}

#[tokio::test]
async fn live_evaluation_proposes_the_expected_index() {
    with_schema(
        "live_evaluation_proposes_the_expected_index",
        |pool, schema| async move {
            let mut c = pool.get_conn().await.unwrap();
            c.query_drop(format!("USE {}", quote(&schema)))
                .await
                .unwrap();
            c.query_drop(TABLE).await.unwrap();
            c.query_drop(FILL).await.unwrap();
            Migrator::new(&[AUTOINDEX_MODULE])
                .unwrap()
                .run(&mut c, &schema, &MigrateOpts::default())
                .await
                .unwrap();
            drop(c);

            let clock = Arc::new(ManualClock::new());
            let evaluator = Evaluator::new(clock.clone());
            let settings = MemSettings::new(registry());
            let dc = DcId::new("mum1").unwrap();
            let dir = directory(&[(ORDERS, None)]);
            let stats = NodeStats {
                pool: pool.clone(),
                tables: [(ORDERS, (schema.clone(), "orders".to_owned()))]
                    .into_iter()
                    .collect(),
                reader: StatsReader::default(),
            };
            let store = CatalogStore::new(pool.clone(), &schema);
            let decisions = MemDecisions::default();
            let src = StaticSource {
                windows: vec![SourcedWindow::client(window())],
                ..StaticSource::default()
            };
            let cx = EvalCx {
                tenant: TENANT,
                settings: &settings,
                ctx: SettingCtx::process(&dc).for_tenant(TENANT, &[]),
                workload: &src,
                objects: &dir,
                stats: &stats,
                store: &store,
                decisions: &decisions,
            };
            let ev = evaluator.evaluate(&cx).await.unwrap();
            assert_eq!(ev.qualified.len(), 1, "{:?}", ev.not_qualified);
            let t = &ev.tables[0];
            assert!(t.error.is_none(), "{:?}", t.error);

            // No statistics yet: the evaluation analyzed the predicate columns.
            let plan = t.analyzed.as_ref().expect("an analyze");
            assert_eq!(plan.columns, ["customer_id", "status"]);
            let n: Option<u64> = pool
                .get_conn()
                .await
                .unwrap()
                .exec_first(
                    "SELECT COUNT(*) FROM mysql.column_stats WHERE db_name = ? \
                 AND table_name = 'orders' AND histogram IS NOT NULL",
                    (&schema,),
                )
                .await
                .unwrap();
            assert_eq!(n, Some(2));

            // B (customer_id, created_at): delivers the order and stops after
            // 20 matches; A (customer_id, status, created_at) adds little more.
            let summary: Vec<_> = t
                .candidates
                .iter()
                .map(|c| {
                    (
                        c.candidate.def.canonical(),
                        c.status,
                        c.reason.clone(),
                        c.estimate
                            .as_ref()
                            .map(|e| (e.benefit_ms_h, e.write_ms_h, e.bytes, e.failed.clone())),
                    )
                })
                .collect();
            assert_eq!(t.queued.len(), 1, "{summary:#?}");
            let q = &t.queued[0];
            let cols: Vec<&str> = q.candidate.def.columns().collect();
            assert_eq!(cols, ["customer_id", "created_at"]);
            assert_eq!(q.route, Route::ValidateThenRollOut);
            assert_eq!(q.target.schema, schema);
            assert_eq!(q.target.table, "orders");
            assert_eq!(q.target.pk, ["id"]);
            // Real selectivities: 40 rows a customer of 20,000; open or hold
            // 5% (a histogram over weighed text).
            let sel = &q.candidate.evidence.selectivity;
            assert!((sel["customer_id"] - 0.002).abs() < 0.0005, "{sel:?}");
            assert!(
                t.candidates.iter().any(|c| c
                    .candidate
                    .evidence
                    .selectivity
                    .get("status")
                    .is_some_and(|s| (s - 0.05).abs() < 0.01)),
                "{:#?}",
                t.candidates
            );

            // The catalog holds the run's candidates and the validation queue.
            let mut c = pool.get_conn().await.unwrap();
            let rows = store
                .tables
                .candidates_of_run(&mut c, ev.run_id.unwrap())
                .await
                .unwrap();
            assert_eq!(rows.len(), t.candidates.len());
            let b = rows
                .iter()
                .find(|r| r.candidate_id == q.candidate_id)
                .unwrap();
            assert_eq!(b.status, CandidateStatus::Selected);
            let idx = store
                .tables
                .index(&mut c, q.index_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(idx.state, IndexState::Validating);
            assert_eq!(idx.name, q.candidate.def.name());
            assert_eq!(idx.candidate_id, q.candidate_id);

            // A second evaluation an hour later: the fingerprint is in its
            // cooldown; nothing new is written.
            clock.advance(std::time::Duration::from_secs(3600));
            let ev2 = evaluator.evaluate(&cx).await.unwrap();
            assert!(ev2.qualified.is_empty() && ev2.tables.is_empty());
        },
    )
    .await;
}

/// The key-value hash view over a namespace `cache`: the
/// directory maps the view object to its element table.
struct KvDirectory {
    metas: BTreeMap<ObjectId, ObjectMeta>,
}

impl ObjectDirectory for KvDirectory {
    fn meta(&self, object: ObjectId) -> Option<ObjectMeta> {
        self.metas.get(&object).cloned()
    }

    fn kv_view(&self, object: ObjectId) -> Option<crate::autoindex::kv::KvTarget> {
        (object == KV_VIEW).then_some(crate::autoindex::kv::KvTarget {
            elements: KV_ELEMS,
            view: crate::autoindex::kv::KvView::Hash,
        })
    }
}

const KV_VIEW: ObjectId = ObjectId(1301);
const KV_ELEMS: ObjectId = ObjectId(1302);

/// SQL over a hash view, looking a field's value up across keys: the
/// evaluation moves it onto the element table and proposes `(e(255),
/// v(64))`, which MariaDB then uses for the view's statement.
#[tokio::test]
async fn live_hash_field_lookup_gets_an_element_table_index() {
    with_schema(
        "live_hash_field_lookup_gets_an_element_table_index",
        |pool, schema| async move {
            let mut c = pool.get_conn().await.unwrap();
            c.query_drop(format!("USE {}", quote(&schema)))
                .await
                .unwrap();
            // The key-value tables and hash view for namespace `cache`, as
            // `sprig_kv::layout` and `sprig_kv::api::view_ddl` make them.
            for s in [
                "CREATE TABLE cache (k VARBINARY(1024) NOT NULL, \
                 type ENUM('string','hash','list','set','zset') NOT NULL DEFAULT 'string', \
                 v LONGBLOB NULL, n BIGINT UNSIGNED NOT NULL DEFAULT 0, \
                 lh BIGINT NOT NULL DEFAULT 0, lt BIGINT NOT NULL DEFAULT 0, \
                 g BIGINT UNSIGNED NOT NULL, _v BIGINT UNSIGNED NOT NULL, \
                 expires_at DATETIME(3) NULL, \
                 _sprig_dk VARBINARY(1024) NOT NULL DEFAULT '' INVISIBLE, \
                 _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, PRIMARY KEY (k), \
                 KEY _sprig_exp (expires_at), KEY _sprig_rk (_sprig_rk)) ENGINE=InnoDB",
                "CREATE TABLE __sprig_ke_cache (k VARBINARY(1024) NOT NULL, \
                 g BIGINT UNSIGNED NOT NULL, o BIGINT NOT NULL, \
                 e VARBINARY(2024) NOT NULL DEFAULT '', v LONGBLOB NULL, \
                 _sprig_dk VARBINARY(1024) NOT NULL DEFAULT '' INVISIBLE, \
                 _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, PRIMARY KEY (k, g, o, e), \
                 KEY _sprig_rk (_sprig_rk)) ENGINE=InnoDB",
                "CREATE SQL SECURITY INVOKER VIEW cache__hash AS SELECT t.k AS k, \
                 e.e AS field, e.v AS value FROM cache AS t JOIN __sprig_ke_cache AS e \
                 ON e.k = t.k AND e.g = t.g WHERE t.type = 'hash' AND \
                 (t.expires_at IS NULL OR t.expires_at > UTC_TIMESTAMP(3))",
            ] {
                c.query_drop(s).await.unwrap();
            }
            // 4,000 hashes of 5 fields; `email` is unique per key.
            c.query_drop(
                "INSERT INTO cache (k, type, n, g, _v) SELECT CONCAT('user:', seq), 'hash', 5, 1, 1 \
                 FROM seq_1_to_4000"
            )
            .await
            .unwrap();
            c.query_drop(
                "INSERT INTO __sprig_ke_cache (k, g, o, e, v) SELECT CONCAT('user:', s.seq), 1, \
                 CRC32(f.f), f.f, IF(f.f = 'email', CONCAT('u', s.seq, '@x.io'), REPEAT('z', 40)) \
                 FROM seq_1_to_4000 s JOIN (SELECT 'email' f UNION ALL SELECT 'name' \
                 UNION ALL SELECT 'city' UNION ALL SELECT 'plan' UNION ALL SELECT 'note') f"
            )
            .await
            .unwrap();
            Migrator::new(&[AUTOINDEX_MODULE])
                .unwrap()
                .run(&mut c, &schema, &MigrateOpts::default())
                .await
                .unwrap();
            let view = "cache__hash".to_owned();
            let elems = "__sprig_ke_cache".to_owned();
            let shape = Shape::from_json(
                &serde_json::json!({
                    "v": 1, "type": "select",
                    "tables": [{"alias": "h", "object_id": KV_VIEW.0, "table": view}],
                    "predicates": [
                        {"t": "h", "col": "field", "op": "eq", "args": [0]},
                        {"t": "h", "col": "value", "op": "eq", "args": [1]}],
                    "joins": [], "order_by": [], "group_by": [],
                    "select": {"h": ["k"]},
                    "limit": false, "writes": null, "or_groups": 0})
                .to_string(),
            )
            .unwrap();
            let w = probed(
                win(0x7a5e, Arc::new(shape), 2_000, 30.0, 60.0),
                40,
                20_000.0,
                1.0,
            );

            let evaluator = Evaluator::new(Arc::new(ManualClock::new()));
            let settings = MemSettings::new(registry());
            let dc = DcId::new("mum1").unwrap();
            let dir = KvDirectory {
                metas: directory(&[(KV_VIEW, None), (KV_ELEMS, None)]),
            };
            let stats = NodeStats {
                pool: pool.clone(),
                tables: [(KV_ELEMS, (schema.clone(), elems.clone()))]
                    .into_iter()
                    .collect(),
                reader: StatsReader::default(),
            };
            let store = CatalogStore::new(pool.clone(), &schema);
            let decisions = MemDecisions::default();
            let src = StaticSource {
                windows: vec![SourcedWindow::client(w)],
                ..StaticSource::default()
            };
            let cx = EvalCx {
                tenant: TENANT,
                settings: &settings,
                ctx: SettingCtx::process(&dc).for_tenant(TENANT, &[]),
                workload: &src,
                objects: &dir,
                stats: &stats,
                store: &store,
                decisions: &decisions,
            };
            let ev = evaluator.evaluate(&cx).await.unwrap();
            assert_eq!(ev.qualified.len(), 1, "{:?}", ev.not_qualified);
            let t = &ev.tables[0];
            assert_eq!(t.object, KV_ELEMS);
            assert!(t.error.is_none(), "{:?}", t.error);
            assert_eq!(t.queued.len(), 1, "{:#?}", t.candidates);
            let q = &t.queued[0];
            assert_eq!(q.candidate.def.ddl_columns(), "`e`(255), `v`(64)");
            assert_eq!(q.target.table, elems);
            eprintln!(
                "hash-field candidate {} route {:?} estimate {:?}",
                q.candidate.def.canonical(),
                q.route,
                t.candidates[0].estimate.as_ref().map(|e| (e.benefit_ms_h, e.write_ms_h, e.bytes))
            );

            // Built as the rollout would, MariaDB uses it for the view.
            let sql = format!(
                "SELECT k FROM {} WHERE field = 'email' AND value = 'u77@x.io'",
                quote(&view)
            );
            let rows_before: Vec<(Option<String>, Option<String>)> = c
                .query(format!("EXPLAIN {sql}"))
                .await
                .unwrap()
                .into_iter()
                .map(|r: mysql_async::Row| (r.get::<Option<String>, _>(2).flatten(), r.get::<Option<String>, _>(5).flatten()))
                .collect();
            c.query_drop(format!(
                "ALTER TABLE {} ADD {}",
                quote(&elems),
                q.candidate.def.add_clause()
            ))
            .await
            .unwrap();
            let explain: Vec<(Option<String>, Option<String>)> = c
                .query(format!("EXPLAIN {sql}"))
                .await
                .unwrap()
                .into_iter()
                .map(|r: mysql_async::Row| (r.get::<Option<String>, _>(2).flatten(), r.get::<Option<String>, _>(5).flatten()))
                .collect();
            let name = q.candidate.def.name();
            assert!(
                explain
                    .iter()
                    .any(|(t, k)| t.as_deref() == Some("e") && k.as_deref() == Some(name.as_str())),
                "before {rows_before:?}, after {explain:?}"
            );
            let got: Vec<String> = c.query(&sql).await.unwrap();
            assert_eq!(got, ["user:77"]);
            eprintln!("EXPLAIN before {rows_before:?}, after {explain:?}");
        },
    )
    .await;
}
