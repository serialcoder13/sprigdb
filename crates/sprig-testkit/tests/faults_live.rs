//! Fault injection and Elle against Docker (chapter 46, build step 3).
//!
//! Needs the `faults` profile (`docker compose --profile faults up -d
//! toxiproxy faultd`) and, for Elle, the `sprigdb-elle:dev` image
//! (`docker compose --profile elle build elle`):
//!
//! | Test | Variables |
//! |---|---|
//! | toxiproxy | `SPRIG_TEST_TOXIPROXY`, `SPRIG_TEST_MARIADB_SINGLE` |
//! | faultd, kill, pause | `SPRIG_TEST_FAULTD`, `SPRIG_TEST_DOCKER` |
//! | clock skew (libfaketime) | `SPRIG_TEST_DOCKER` (the first run builds the image, which needs the network) |
//! | Elle | `SPRIG_TEST_ELLE` (plus `SPRIG_TEST_MARIADB_SINGLE` for the workload) |
//!
//! Faults touch only this test's own toxiproxy routes and `sprig-sim-*`
//! containers.

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use serde_json::json;
use sprig_testkit::faults::compose::{OwnContainer, docker};
use sprig_testkit::faults::elle::{ElleCli, ElleModel, ListAppendOpts, list_append};
use sprig_testkit::faults::faultd::Faultd;
use sprig_testkit::faults::history::{History, OpKind};
use sprig_testkit::faults::toxiproxy::Toxiproxy;
use sprig_testkit::faults::{ComposeFaults, Faults, LinkFault, Node};
use sprig_testkit::{require_env, skip, unique_ident, unique_name};

/// The single node as toxiproxy (on the same Docker network) reaches it.
fn single_upstream() -> String {
    skip::var(skip::TOXIPROXY_UPSTREAM).unwrap_or_else(|| "single:3306".to_owned())
}

fn via(url: &str, addr: &str) -> String {
    let hp = sprig_testkit::util::host_port(url).expect("host:port");
    url.replace(hp, addr)
}

async fn conn(url: &str) -> mysql_async::Result<mysql_async::Conn> {
    let o = mysql_async::Opts::from_url(url).expect("url");
    mysql_async::Conn::new(mysql_async::OptsBuilder::from_opts(o).prefer_socket(false)).await
}

async fn round_trip(c: &mut mysql_async::Conn) -> Duration {
    let t = Instant::now();
    let _: Option<u8> = c.query_first("SELECT 1").await.expect("select");
    t.elapsed()
}

#[tokio::test]
async fn toxiproxy_slows_and_cuts_an_own_route() {
    let api = require_env!(skip::TOXIPROXY);
    let single = skip::urls(&require_env!(skip::MARIADB_SINGLE)).remove(0);
    let tp = Toxiproxy::new(&api).unwrap();
    tp.version().await.unwrap();
    let proxy = tp.own_proxy("single", &single_upstream()).await.unwrap();
    let url = via(&single, proxy.host_addr());
    let mut c = conn(&url).await.unwrap();
    let base = round_trip(&mut c).await;

    let faults = ComposeFaults::new(Some(tp.clone()), None);
    let (gw, db) = (Node::from("gateway"), Node::from("single"));
    faults.route(&gw, &db, proxy.name());
    let id = faults
        .link(&gw, &db, LinkFault::latency(Duration::from_millis(200)))
        .await
        .unwrap();
    let slow = round_trip(&mut c).await;
    assert!(
        slow >= Duration::from_millis(200),
        "{slow:?} (base {base:?})"
    );
    faults.heal(id).await.unwrap();
    assert!(round_trip(&mut c).await < Duration::from_millis(200));

    // A partition: the proxy drops its connections and refuses new ones.
    faults
        .partition(std::slice::from_ref(&gw), std::slice::from_ref(&db))
        .await
        .unwrap();
    let r = tokio::time::timeout(Duration::from_secs(5), c.query_drop("SELECT 1")).await;
    assert!(!matches!(r, Ok(Ok(()))), "query went through a partition");
    assert!(conn(&url).await.is_err());
    faults.heal_all().await.unwrap();
    let mut c = conn(&url).await.unwrap();
    round_trip(&mut c).await;
    let _ = c.disconnect().await;
    let name = proxy.name().to_owned();
    drop(proxy);
    assert!(
        !tp.proxies().await.unwrap().contains(&name),
        "proxy {name} leaked"
    );
}

fn ping(from: &str, ip: &str) -> bool {
    docker(&["exec", from, "ping", "-c", "1", "-W", "1", ip]).is_ok()
}

#[tokio::test]
async fn faultd_partitions_and_docker_faults_on_own_containers() {
    require_env!(skip::DOCKER);
    let sidecar = Faultd::new(&require_env!(skip::FAULTD));
    let net = unique_name("sprig-sim-net");
    docker(&["network", "create", &net]).unwrap();
    let cleanup_net = NetGuard(net.clone());
    let a = OwnContainer::run("a", &["--network", &net, "alpine:latest", "sleep", "600"]).unwrap();
    let b = OwnContainer::run("b", &["--network", &net, "alpine:latest", "sleep", "600"]).unwrap();
    let (ip_a, ip_b) = (a.ip().unwrap(), b.ip().unwrap());
    assert!(ping(a.name(), &ip_b));

    let faults = ComposeFaults::new(None, Some(sidecar));
    let (na, nb) = (Node::from(a.name()), Node::from(b.name()));
    faults
        .partition(std::slice::from_ref(&na), std::slice::from_ref(&nb))
        .await
        .unwrap();
    assert!(!ping(a.name(), &ip_b));
    assert!(!ping(b.name(), &ip_a));
    faults.heal_all().await.unwrap();
    assert!(ping(a.name(), &ip_b));

    // Pause freezes the container; heal unpauses it.
    let id = faults.pause(&nb).await.unwrap();
    let paused = docker(&["inspect", "-f", "{{.State.Paused}}", b.name()]).unwrap();
    assert_eq!(paused, "true");
    faults.heal(id).await.unwrap();
    // Kill restarts it: a new start time, still running.
    let started = docker(&["inspect", "-f", "{{.State.StartedAt}}", b.name()]).unwrap();
    faults.kill(&nb).await.unwrap();
    let state = docker(&[
        "inspect",
        "-f",
        "{{.State.Running}} {{.State.StartedAt}}",
        b.name(),
    ])
    .unwrap();
    assert!(
        state.starts_with("true ") && !state.ends_with(&started),
        "{state}"
    );
    drop((a, b));
    drop(cleanup_net);
}

/// Removes a Docker network on drop.
struct NetGuard(String);

impl Drop for NetGuard {
    fn drop(&mut self) {
        let _ = docker(&["network", "rm", &self.0]);
    }
}

#[test]
fn elle_finds_write_skew() {
    let Some(elle) = ElleCli::from_env() else {
        skip::report_missing(skip::ELLE, module_path!());
        return;
    };
    let h = History::new();
    h.record(
        0,
        OpKind::Invoke,
        "txn",
        json!([["append", 1, 1], ["r", 2, null]]),
    );
    h.record(
        0,
        OpKind::Ok,
        "txn",
        json!([["append", 1, 1], ["r", 2, []]]),
    );
    h.record(
        1,
        OpKind::Invoke,
        "txn",
        json!([["append", 2, 1], ["r", 1, null]]),
    );
    h.record(
        1,
        OpKind::Ok,
        "txn",
        json!([["append", 2, 1], ["r", 1, []]]),
    );
    let v = elle
        .check(&h.records(), ElleModel::ListAppend, &["serializable"])
        .unwrap();
    assert_eq!(v.valid, Some(false), "{}", v.detail);
    assert!(
        v.anomalies.iter().any(|a| a.starts_with("G2")),
        "{:?}",
        v.anomalies
    );
}

#[tokio::test]
async fn list_append_on_mariadb_is_serializable() {
    let Some(elle) = ElleCli::from_env() else {
        skip::report_missing(skip::ELLE, module_path!());
        return;
    };
    let single = skip::urls(&require_env!(skip::MARIADB_SINGLE)).remove(0);
    let schema = unique_ident("elle");
    let mut root = conn(&single).await.unwrap();
    root.query_drop(format!("CREATE DATABASE `{schema}`"))
        .await
        .unwrap();
    // Through an own toxiproxy route with latency, when one is available.
    let tp = skip::var(skip::TOXIPROXY).map(|u| Toxiproxy::new(&u).unwrap());
    let proxy = match &tp {
        Some(tp) => Some(tp.own_proxy("elle", &single_upstream()).await.unwrap()),
        None => None,
    };
    let url = match &proxy {
        Some(p) => {
            p.api()
                .add_toxic(
                    p.name(),
                    &sprig_testkit::faults::toxiproxy::Toxic::latency(2, 2),
                )
                .await
                .unwrap();
            via(&single, p.host_addr())
        }
        None => single.clone(),
    };
    let opts = ListAppendOpts {
        processes: 4,
        txns: 40,
        ..ListAppendOpts::default()
    };
    let history = list_append(&url, &schema, &opts).await.unwrap();
    let records = history.records();
    let oks = records.iter().filter(|r| r.kind == OpKind::Ok).count();
    let v = elle.check(&records, ElleModel::ListAppend, &["serializable"]);
    root.query_drop(format!("DROP DATABASE `{schema}`"))
        .await
        .unwrap();
    let _ = root.disconnect().await;
    let v = v.unwrap();
    assert!(oks > 20, "only {oks} transactions committed");
    assert_eq!(v.valid, Some(true), "{}", v.detail);
}

/// The server's wall clock minus ours, in milliseconds.
async fn server_offset_ms(c: &mut mysql_async::Conn) -> i64 {
    let server: String = c
        .query_first("SELECT CAST(UNIX_TIMESTAMP(NOW(6)) AS CHAR)")
        .await
        .expect("now")
        .expect("a row");
    let ours = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after 1970")
        .as_secs_f64();
    let server: f64 = server.parse().expect("a decimal");
    #[allow(clippy::cast_possible_truncation)]
    let ms = ((server - ours) * 1000.0).round() as i64;
    ms
}

/// libfaketime in a throwaway MariaDB: `skew_clock` moves the server's
/// `NOW()` forward and back while it runs; healing restores it. Shared
/// containers are refused.
#[tokio::test]
async fn clock_skew_in_an_own_faketime_container() {
    require_env!(skip::DOCKER);
    let db = sprig_testkit::faults::compose::OwnMariaDb::start_with_faketime(None, 931, 93)
        .await
        .unwrap();
    let mut c = conn(&db.url).await.unwrap();
    let faults = ComposeFaults::new(None, None);
    let node = Node::from(db.container.name());

    let before = server_offset_ms(&mut c).await;
    assert!(before.abs() < 1000, "unskewed: {before} ms");

    let ahead = faults.skew_clock(&node, 30_000, 0).await.unwrap();
    let off = server_offset_ms(&mut c).await;
    assert!((off - 30_000).abs() < 1500, "+30 s: {off} ms");
    faults.heal(ahead).await.unwrap();
    let off = server_offset_ms(&mut c).await;
    assert!(off.abs() < 1500, "healed: {off} ms");

    faults.skew_clock(&node, -20_000, 0).await.unwrap();
    let off = server_offset_ms(&mut c).await;
    assert!((off + 20_000).abs() < 1500, "-20 s: {off} ms");
    // Still a working server.
    let one: Option<u8> = c.query_first("SELECT 1").await.unwrap();
    assert_eq!(one, Some(1));
    faults.heal_all().await.unwrap();
    let off = server_offset_ms(&mut c).await;
    assert!(off.abs() < 1500, "healed: {off} ms");

    assert!(
        faults
            .skew_clock(&Node::from("sprigdb-dev-single-1"), 1000, 0)
            .await
            .is_err()
    );
    drop(c);
}
