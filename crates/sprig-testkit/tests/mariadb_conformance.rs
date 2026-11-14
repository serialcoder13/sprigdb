//! Model conformance against real MariaDB: every scenario of
//! `sprig_testkit::sim::mariadb::conformance` runs on the model and on the
//! Compose servers, and the observed outcomes must be equal.
//!
//! Shared targets need `SPRIG_TEST_MARIADB_SINGLE`, `_REPLICATION` or
//! `_GALERA`; scenarios that crash a server or set `read_only` start a
//! throwaway `sprig-sim-*` container and need `SPRIG_TEST_DOCKER=1`.

use sprig_testkit::faults::compose::OwnMariaDb;
use sprig_testkit::sim::mariadb::conformance::{
    RealTarget, Scenario, Target, diff, probe_ids, render, run_model, run_real, scenarios,
};
use sprig_testkit::skip;

async fn check(sc: &Scenario, target: &RealTarget) -> Vec<String> {
    let ids = probe_ids(target, sc.target == Target::Galera)
        .await
        .unwrap_or_else(|e| panic!("{}: {e}", sc.name));
    let model = run_model(sc, &ids);
    let real = run_real(sc, target)
        .await
        .unwrap_or_else(|e| panic!("{}: {e}", sc.name));
    let d = diff(sc, &model, &real);
    if !d.is_empty() || skip::flag("SPRIG_TEST_VERBOSE") {
        eprintln!(
            "{}:\n model:\n{}\n real:\n{}",
            sc.name,
            render(sc, &model),
            render(sc, &real)
        );
    }
    d
}

async fn shared(target: Target, var: &str) {
    let urls = skip::urls(&sprig_testkit::require_env!(var));
    let rt = RealTarget {
        urls,
        container: None,
    };
    let mut diffs = Vec::new();
    for sc in scenarios().iter().filter(|s| s.target == target) {
        diffs.extend(check(sc, &rt).await);
    }
    assert!(
        diffs.is_empty(),
        "model diverges from MariaDB:\n{}",
        diffs.join("\n")
    );
}

#[tokio::test]
async fn single_node() {
    shared(Target::Single, skip::MARIADB_SINGLE).await;
}

#[tokio::test]
async fn primary_and_replicas() {
    shared(Target::Replication, skip::MARIADB_REPLICATION).await;
}

#[tokio::test]
async fn galera() {
    shared(Target::Galera, skip::MARIADB_GALERA).await;
}

#[tokio::test]
async fn throwaway_crash_and_fencing() {
    sprig_testkit::require_env!(skip::DOCKER);
    let db = OwnMariaDb::start(None, 901, 9).await.unwrap();
    let rt = RealTarget {
        urls: vec![db.url.clone()],
        container: Some(db.container.name().to_owned()),
    };
    let mut diffs = Vec::new();
    for sc in scenarios().iter().filter(|s| s.target == Target::Throwaway) {
        diffs.extend(check(sc, &rt).await);
    }
    assert!(
        diffs.is_empty(),
        "model diverges from MariaDB:\n{}",
        diffs.join("\n")
    );
}
