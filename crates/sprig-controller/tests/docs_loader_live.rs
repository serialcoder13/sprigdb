//! The docs loader against real clusters: the release's docs pack loaded
//! into `__sprig_docs` on every cluster of a DC, verified, reloaded after a
//! row is edited, and readable (only readable) by the docs-reader account.
//!
//! Needs an etcd whose DC tier has registered clusters, such as the
//! Compose stack's with `deploy/compose/gateway-dev.sh up`, and a docs pack
//! (`cargo run --release -p sprig-docs-gen --no-default-features --bin
//! docs-pack`):
//!
//! ```sh
//! SPRIG_TEST_DOCS_ETCD=http://127.0.0.1:2379 \
//!   cargo test -p sprig-controller --test docs_loader_live -- --nocapture
//! ```
//!
//! `SPRIG_TEST_DOCS_DC` names the DC (`dc-a`); `SPRIG_TEST_DOCS_PACK` the
//! pack (`target/docs/sprig-docs-<version>.pack`). The test writes only
//! `__sprig_docs` and the `sprig_docs_r` account.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

use std::path::PathBuf;
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};
use sprig_cluster::driver::NodeConnector;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::DcId;
use sprig_controller::catalogs::RegistryPools;
use sprig_controller::docs_loader::{DocsClusters, DocsLoader, PackSource, RegistryDocs};
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Tier};
use sprig_provision::credentials::CredentialConnector;
use sprig_testkit::skip;

const ETCD: &str = "SPRIG_TEST_DOCS_ETCD";

fn pack_path() -> PathBuf {
    skip::var("SPRIG_TEST_DOCS_PACK").map_or_else(
        || {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
                "../../target/docs/sprig-docs-{}.pack",
                env!("CARGO_PKG_VERSION")
            ))
        },
        PathBuf::from,
    )
}

async fn conn(opts: mysql_async::Opts) -> Conn {
    Conn::new(OptsBuilder::from_opts(opts)).await.unwrap()
}

fn server_code(e: &mysql_async::Error) -> Option<u16> {
    match e {
        mysql_async::Error::Server(s) => Some(s.code),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loads_verifies_reloads_and_reads_only() {
    let Some(etcd) = skip::var(ETCD) else {
        skip::report_missing(ETCD, "loads_verifies_reloads_and_reads_only");
        return;
    };
    let pack = pack_path();
    assert!(pack.exists(), "no docs pack at {}", pack.display());
    let dc = DcId::new(skip::var("SPRIG_TEST_DOCS_DC").unwrap_or_else(|| "dc-a".into())).unwrap();
    let cfg = EtcdConfig::new(vec![etcd]);
    let dc_store: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(Tier::Dc(dc.clone()), &cfg)
            .await
            .unwrap(),
    );
    let global: Arc<dyn MetaStore> =
        Arc::new(EtcdStore::connect(Tier::Global, &cfg).await.unwrap());
    let registry = ClusterRegistry::new(Arc::clone(&global), Arc::clone(&dc_store)).unwrap();
    let docs = Arc::new(RegistryDocs::new(
        registry.clone(),
        RegistryPools::new(registry.clone(), None).budget("docs", 1),
    ));
    let loader = DocsLoader::new(docs.clone(), None, PackSource::File(pack.clone()));
    let p = loader.pack().unwrap();
    let clusters = docs.clusters().await.unwrap();
    assert!(!clusters.is_empty(), "no clusters in {dc}");
    eprintln!(
        "pack {} ({} chunks); clusters {clusters:?}",
        p.version,
        p.rows.len()
    );

    // 1. Every cluster: loaded (or verified when an earlier run loaded it).
    for c in &clusters {
        let t = std::time::Instant::now();
        let r = loader.sync(c, &p).await.unwrap();
        eprintln!("{c}: {r:?} in {:?}", t.elapsed());
        assert!(r.verified, "{c}: the first pass of a process verifies");
        // A second pass trusts what this process verified.
        let r = loader.sync(c, &p).await.unwrap();
        assert!(!r.loaded && !r.verified, "{c}: {r:?}");
    }

    // 2. An edited row is found by the next process and reloaded.
    let c1 = &clusters[0];
    let rec = registry.get(c1).await.unwrap().unwrap();
    let node = rec.body().nodes[0].clone();
    let ctl = CredentialConnector::ctl(&rec.body().secrets)
        .endpoint(&node)
        .unwrap();
    let mut admin = conn(ctl.opts.clone()).await;
    admin
        .exec_drop(
            "UPDATE `__sprig_docs`.`chunks` SET text = CONCAT(text, ' (edited)') \
             WHERE version = ? ORDER BY chunk_id LIMIT 1",
            (p.version.as_str(),),
        )
        .await
        .unwrap();
    let again = DocsLoader::new(docs.clone(), None, PackSource::File(pack.clone()));
    let r = again.sync(c1, &p).await.unwrap();
    assert!(r.tampered && r.loaded, "{r:?}");
    let edited: u64 = admin
        .query_first("SELECT COUNT(*) FROM `__sprig_docs`.`chunks` WHERE text LIKE '% (edited)'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(edited, 0);
    let state: String = admin
        .exec_first(
            "SELECT CAST(state AS CHAR) FROM `__sprig_docs`.`packs` WHERE version = ?",
            (p.version.as_str(),),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state, "ready");

    // 3. The docs-reader account reads, searches and nothing else.
    let reader = sprig_provision::docs_account::docs_credential(&rec.body().secrets.ctl).unwrap();
    let mut r = conn(reader.endpoint(&node).unwrap().opts).await;
    let n: u64 = r
        .exec_first(
            "SELECT COUNT(*) FROM `__sprig_docs`.`chunks` WHERE version = ?",
            (p.version.as_str(),),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, p.rows.len() as u64);
    let hits: Vec<String> = r
        .query(
            "SELECT page FROM `__sprig_docs`.`chunks` WHERE MATCH(text) AGAINST ('replica lag') LIMIT 5",
        )
        .await
        .unwrap();
    assert!(!hits.is_empty(), "no full-text hits");
    let ids: Vec<String> = r
        .query(
            "SELECT page FROM `__sprig_docs`.`chunks` \
             WHERE MATCH(text, heading_path, terms) AGAINST ('auto_index.mode') LIMIT 5",
        )
        .await
        .unwrap();
    assert!(!ids.is_empty(), "no hits for a setting key");
    let near: Vec<(String, f64)> = r
        .query(
            "SELECT c.page, VEC_DISTANCE_COSINE(c.embedding, o.embedding) AS d \
             FROM `__sprig_docs`.`chunks` c, \
             (SELECT embedding FROM `__sprig_docs`.`chunks` ORDER BY chunk_id LIMIT 1) o \
             ORDER BY d LIMIT 3",
        )
        .await
        .unwrap();
    assert!(near[0].1.abs() < 1e-6, "{near:?}");
    for sql in [
        "UPDATE `__sprig_docs`.`chunks` SET text = 'x' LIMIT 1",
        "DELETE FROM `__sprig_docs`.`chunks` LIMIT 1",
        "INSERT INTO `__sprig_docs`.`packs` VALUES ('x', REPEAT('a', 32), 0, NOW(), 'ready')",
        "DROP TABLE `__sprig_docs`.`packs`",
        "CREATE TABLE `__sprig_docs`.`x` (a INT)",
    ] {
        let e = r.query_drop(sql).await.expect_err(sql);
        assert!(matches!(server_code(&e), Some(1142 | 1044)), "{sql}: {e}");
    }
    // Nothing else: no tenant schema, no system table.
    let e = r
        .query_drop("SELECT COUNT(*) FROM mysql.user")
        .await
        .expect_err("mysql.user");
    assert!(matches!(server_code(&e), Some(1142 | 1044)), "{e}");

    // 4. Only the reader holds a grant on __sprig_docs: tenants' accounts
    //    get nothing.
    let grantees: Vec<String> = admin
        .query("SELECT DISTINCT User FROM mysql.db WHERE Db = '__sprig_docs' OR Db = '\\_\\_sprig\\_docs'")
        .await
        .unwrap();
    assert_eq!(grantees, vec!["sprig_docs_r".to_owned()], "{grantees:?}");
    let tenant_global: u64 = admin
        .query_first(
            "SELECT COUNT(*) FROM mysql.global_priv WHERE User LIKE 't%\\_u%' \
             AND JSON_VALUE(Priv, '$.access') & 1 = 1",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tenant_global, 0, "a tenant account has global SELECT");
    admin.disconnect().await.ok();
    r.disconnect().await.ok();
}
