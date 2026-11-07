//! Checks that the configured environment is up: every etcd and MariaDB
//! endpoint of [`Env::from_env`] accepts TCP connections. Runs in CI after
//! Compose starts, so a wrong port in a `SPRIG_TEST_*` variable or in
//! `env.3dc.toml` fails here rather than as a confusing error elsewhere.

use std::time::Duration;

use sprig_testkit::util::wait_for_tcp;
use sprig_testkit::{Env, skip};

#[tokio::test]
async fn every_endpoint_accepts_connections() {
    let env = Env::from_env().unwrap();
    if env.is_empty() {
        skip::report_missing(
            "SPRIG_TEST_ENV_FILE or a SPRIG_TEST_* variable",
            module_path!(),
        );
        return;
    }
    let mut endpoints: Vec<&str> = env.global_etcd.iter().map(String::as_str).collect();
    for dc in &env.dcs {
        endpoints.extend(dc.etcd.iter().map(String::as_str));
        for c in &dc.clusters {
            endpoints.extend(c.endpoints());
        }
    }
    assert!(!endpoints.is_empty());
    for url in endpoints {
        wait_for_tcp(url, Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("environment {:?}: {e}", env.name));
    }
}

/// With the three-DC layout running (`deploy/compose/3dc.sh up -d --wait`),
/// the fixture attaches to it and its published ports match `env.3dc.toml`.
#[test]
fn three_dc_fixture_matches_its_env_file() {
    sprig_testkit::require_env!(skip::DOCKER);
    // Optional even in strict mode: only stage S2 runs the three-DC layout.
    let file = skip::var(skip::ENV_FILE).unwrap_or_default();
    if !file.ends_with("env.3dc.toml") {
        eprintln!("skipped: {} is not the three-DC layout", skip::ENV_FILE);
        return;
    }
    let env = Env::three_dc();
    let stack = sprig_testkit::ComposeProject::three_dc().attach();
    stack.wait_healthy(Duration::from_secs(120)).unwrap();
    for (service, cluster) in [
        ("mum1-db-1", "mum1-c1"),
        ("fra1-db-1", "fra1-c1"),
        ("iad1-db-1", "iad1-c1"),
    ] {
        let addr = stack.port(service, 3306).unwrap();
        let writer = &env.cluster(cluster).unwrap().writer;
        assert_eq!(
            sprig_testkit::util::host_port(writer),
            Some(addr.as_str()),
            "{service}"
        );
    }
}
