//! Workload capture end to end through the dev gateway: a slow statement run 200 times as `acme.app` reaches
//! acme's catalog with its runs, latency, probed rows examined and sealed
//! parameter samples, read back through the workload store's reader API;
//! `globex` sees none of it.
//!
//! ```sh
//! deploy/compose/gateway-dev.sh up   # catalog mode: acme and globex
//! SPRIG_TEST_M04_GATEWAY=127.0.0.1:13307 \
//! SPRIG_TEST_M04_BACKEND=mysql://root:sprig-dev-root@127.0.0.1:13301 \
//!   cargo test -p sprig-gateway --test workload_live -- --ignored --nocapture
//! ```
//!
//! `SPRIG_TEST_M04_TENANTS` gives the tenant ids as `acme,globex` (default:
//! read from `deploy/compose/.gateway-dev/tenants`), and
//! `SPRIG_TEST_M04_KEYS` the sample key file (default
//! `deploy/compose/workload-keys.dev`). The gateway must flush within a
//! minute (`gateway-dev.sh` sets `workload.flush_seconds = 10`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Pool, SslOpts};
use sprig_catalog::{CatalogEndpoint, StaticLocator, Store};
use sprig_common::ids::TenantId;
use sprig_testkit::{skip, unique_ident};
use sprig_workload::keyring::MasterKeyring;
use sprig_workload::store::{CatalogWorkloadStore, WorkloadStore};
use sprig_workload::window::{FpWindowStats, SampledSlot};

const PASSWORD: &str = "sprig-dev-app";
const GATEWAY: &str = "SPRIG_TEST_M04_GATEWAY";
const BACKEND: &str = "SPRIG_TEST_M04_BACKEND";
const RUNS: u32 = 200;

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn tenants() -> (TenantId, TenantId) {
    let text = skip::var("SPRIG_TEST_M04_TENANTS").unwrap_or_else(|| {
        let f = std::fs::read_to_string(repo().join("deploy/compose/.gateway-dev/tenants"))
            .expect("SPRIG_TEST_M04_TENANTS or deploy/compose/.gateway-dev/tenants");
        let id = |name: &str| {
            f.lines()
                .find_map(|l| l.strip_prefix(&format!("{name} ")))
                .unwrap_or_else(|| panic!("no tenant {name}"))
                .trim()
                .to_owned()
        };
        format!("{},{}", id("acme"), id("globex"))
    });
    let (a, b) = text.split_once(',').unwrap();
    (
        TenantId(a.trim().parse().unwrap()),
        TenantId(b.trim().parse().unwrap()),
    )
}

async fn conn(addr: &str, user: &str, db: Option<&str>) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(PASSWORD))
        .db_name(db)
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("connect {user}@{addr}: {e}"))
}

fn ours<'a>(w: &'a [FpWindowStats], db: &str) -> Option<&'a FpWindowStats> {
    w.iter()
        .find(|f| f.text.contains(&format!("{db}.wl_scan")) && f.text.contains("SLEEP"))
}

#[tokio::test]
#[ignore = "needs the dev gateway (deploy/compose/gateway-dev.sh up)"]
async fn slow_statements_reach_the_tenants_workload_tables_and_only_its() {
    let Some(gw) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, "workload_live");
        return;
    };
    let Some(backend) = skip::var(BACKEND) else {
        skip::report_missing(BACKEND, "workload_live");
        return;
    };
    let (acme, globex) = tenants();
    let db = unique_ident("wl");
    let mut a = conn(&gw, "acme.app", None).await;
    a.query_drop(format!("CREATE DATABASE {db}")).await.unwrap();
    a.query_drop(format!("USE {db}")).await.unwrap();
    a.query_drop("CREATE TABLE wl_scan (id INT PRIMARY KEY, v INT NOT NULL)")
        .await
        .unwrap();
    a.query_drop(
        // Two small recursions (max_recursive_iterations is 1000).
        "INSERT INTO wl_scan WITH RECURSIVE s (n) AS (SELECT 0 UNION ALL \
         SELECT n + 1 FROM s WHERE n < 99) SELECT a.n * 50 + b.n + 1, \
         (a.n * 50 + b.n + 1) % 100 FROM s a JOIN s b ON b.n < 50",
    )
    .await
    .unwrap();

    // 200 runs of a slow statement that scans 5,000 rows: above
    // workload.watch_p95_ms (50) and workload.full_probe_ms (100), so every
    // run after the first is probed, and the first three of the bucket are
    // sampled.
    let started = Instant::now();
    for _ in 0..RUNS {
        let n: Option<(i64, i64)> = a
            .query_first(
                "SELECT (SELECT SLEEP(0.11)) AS s, \
                 (SELECT COUNT(*) FROM wl_scan WHERE v = 7) AS n",
            )
            .await
            .unwrap();
        assert_eq!(n.unwrap().1, 50);
    }
    eprintln!("{RUNS} runs in {:?}", started.elapsed());

    // Read acme's catalog through the reader API until the flush lands.
    let pool = Pool::new(backend.as_str());
    let locator = StaticLocator::new()
        .with(
            acme,
            CatalogEndpoint {
                pool: pool.clone(),
                store: Store::for_tenant(acme),
            },
        )
        .with(
            globex,
            CatalogEndpoint {
                pool: pool.clone(),
                store: Store::for_tenant(globex),
            },
        );
    let store = CatalogWorkloadStore::new(Arc::new(locator));
    let from = SystemTime::now() - Duration::from_secs(3 * 3600);
    let to = SystemTime::now() + Duration::from_secs(3600);
    let deadline = Instant::now() + Duration::from_secs(90);
    let w = loop {
        let w = store.window(acme, from, to).await.unwrap();
        if let Some(f) = ours(&w, &db)
            && f.runs() >= u64::from(RUNS)
        {
            break f.clone();
        }
        assert!(
            Instant::now() < deadline,
            "the fingerprint didn't reach acme's catalog with {RUNS} runs: {:?}",
            ours(&w, &db).map(FpWindowStats::runs)
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    eprintln!(
        "fingerprint {} `{}`: runs {}, mean {:.1} ms, p95 {:.1} ms, probed {}, examined/run {:?}",
        w.id,
        w.text,
        w.runs(),
        w.mean_ms(),
        w.p95_ms(),
        w.stats.examined_samples,
        w.examined_per_run()
    );
    assert_eq!(w.runs(), u64::from(RUNS));
    assert!(w.mean_ms() >= 110.0, "{}", w.mean_ms());
    assert!(w.p95_ms() >= 110.0, "{}", w.p95_ms());
    assert_eq!(w.stats.rows_returned, u64::from(RUNS));
    // Every run but the first was probed (the first creates the entry).
    assert!(
        w.stats.examined_samples >= u64::from(RUNS) - 2,
        "{}",
        w.stats.examined_samples
    );
    // A full scan of 5,000 rows each (plus a row or two of overhead).
    let x = w.examined_per_run().unwrap();
    assert!((5000.0..5100.0).contains(&x), "rows examined per run {x}");
    assert!(w.examined_per_returned().unwrap() >= 5000.0);
    assert!(w.shape.object_ids().len() == 1, "{:?}", w.shape.tables);

    // Samples: stored sealed, opened only with the key.
    let sealed = store.samples(acme, w.id, 50).await.unwrap();
    assert!(!sealed.is_empty(), "no samples");
    assert!(sealed.len() <= 3, "at most samples_per_bucket per gateway");
    let mut raw = pool.get_conn().await.unwrap();
    let plain: u64 = raw
        .query_first(format!(
            "SELECT COUNT(*) FROM t{}_sprig.workload_samples WHERE fingerprint_id = {} \
             AND (INSTR(payload, '0.11') > 0 OR INSTR(payload, '{db}') > 0)",
            acme.0, w.id.0
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(plain, 0, "a sample's payload holds plaintext");
    let keys_path = skip::var("SPRIG_TEST_M04_KEYS").map_or_else(
        || repo().join("deploy/compose/workload-keys.dev"),
        PathBuf::from,
    );
    let keys = MasterKeyring::from_file(&keys_path).unwrap();
    let opened = store.open_samples(&keys, acme, w.id, 50).await.unwrap();
    assert_eq!(opened.len(), sealed.len());
    let o = &opened[0];
    assert_eq!(o.payload.context.database.as_deref(), Some(db.as_str()));
    let texts: Vec<String> = o
        .payload
        .literals
        .slots
        .iter()
        .map(|s| match s {
            SampledSlot::One(v) => String::from_utf8_lossy(&v.text).into_owned(),
            SampledSlot::List(_) => String::from("(...)"),
        })
        .collect();
    assert_eq!(texts, vec!["0.11", "7"]);
    assert!(o.rows_examined.is_some_and(|x| x >= 5000));
    // Another tenant's key opens nothing.
    assert!(
        store
            .open_samples(&keys, globex, w.id, 50)
            .await
            .unwrap()
            .is_empty()
    );

    // globex: nothing of acme's in its catalog, and acme's catalog tables
    // aren't reachable through the gateway.
    let g = store.window(globex, from, to).await.unwrap();
    assert!(ours(&g, &db).is_none());
    assert!(g.iter().all(|f| !f.text.contains(&db)));
    assert!(store.samples(globex, w.id, 50).await.unwrap().is_empty());
    let mut b = conn(&gw, "globex.app", None).await;
    for t in ["workload_fingerprints", "workload_samples"] {
        let r = b
            .query_drop(format!("SELECT * FROM t{}_sprig.{t}", acme.0))
            .await;
        assert!(r.is_err(), "globex reached acme's {t}");
    }
    drop(b);

    a.query_drop(format!("DROP DATABASE {db}")).await.unwrap();
    drop(a);
    drop(raw);
    pool.disconnect().await.unwrap();
}
