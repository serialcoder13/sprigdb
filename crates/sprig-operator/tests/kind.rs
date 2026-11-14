//! The Kubernetes provisioner against a real cluster: `create` and
//! `retire` through [`KubernetesProvisioner`] and the real REST client,
//! on a kind cluster with mariadb-operator.
//!
//! Skipped unless `SPRIG_KIND_API` is set. A run on kind sets:
//!
//! - `SPRIG_KIND_API`: the API server, `https://127.0.0.1:<port>`;
//! - `SPRIG_KIND_CA`: its CA bundle (PEM file);
//! - `SPRIG_KIND_TOKEN`: a file with the operator `ServiceAccount`'s token
//!   (`kubectl create token sprigdb-operator`), so the calls run under the
//!   chart's Role;
//! - `SPRIG_KIND_NS`: the namespace (default `sprigdb`);
//! - `SPRIG_KIND_CREATE`: `<pool>,<pool>,...`: one `create` per pool, each
//!   a `ClusterSpec` naming that `SprigClusterPool`;
//! - `SPRIG_KIND_RETIRE`: `<cluster_id>,...`: one `retire` each;
//! - `SPRIG_KIND_ADMIN`: the controller's Admin API (`http://host:port`),
//!   which `retire` asks to retire the drained record first.
//!
//! The operator itself runs in the cluster; this test only drives the
//! provisioner's two operations and prints what they did.

#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::too_many_lines,
    clippy::cast_precision_loss
)]

use std::path::PathBuf;
use std::sync::Arc;

use sprig_common::SprigCode;
use sprig_common::ids::{ClusterId, DcId};
use sprig_common::time::SystemClock;
use sprig_operator::OperatorConfig;
use sprig_operator::accounts::MysqlAccounts;
use sprig_operator::crd::SPRIG_CLUSTER;
use sprig_operator::kube::{HttpKube, KubeApi, KubeConfig};
use sprig_operator::provisioner::KubernetesProvisioner;
use sprig_operator::reconcile::Reconciler;
use sprig_operator::register::AdminRegistrar;
use sprig_provision::{ClusterSpec, MariaDbVersion, Provisioner, Resources, Topology};

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn provisioner() -> Option<(KubernetesProvisioner, Arc<dyn KubeApi>, String)> {
    let base = env("SPRIG_KIND_API")?;
    let ns = env("SPRIG_KIND_NS").unwrap_or_else(|| "sprigdb".into());
    let cfg = KubeConfig {
        base,
        token_file: env("SPRIG_KIND_TOKEN").map(PathBuf::from),
        ca_pem: env("SPRIG_KIND_CA").map(|p| std::fs::read(p).unwrap()),
        namespace: ns.clone(),
    };
    let kube: Arc<dyn KubeApi> = Arc::new(HttpKube::new(cfg).unwrap());
    let rec = Reconciler::new(
        Arc::clone(&kube),
        Arc::new(MysqlAccounts::new(None)),
        // `retire` retires the record first: SPRIG_KIND_ADMIN is
        // the controller's Admin API.
        Arc::new(
            AdminRegistrar::new(
                &env("SPRIG_KIND_ADMIN").unwrap_or_else(|| "http://127.0.0.1:1".into()),
                None,
            )
            .unwrap(),
        ),
        OperatorConfig::new(&ns),
        Arc::new(SystemClock),
    );
    Some((KubernetesProvisioner::new(Arc::new(rec)), kube, ns))
}

#[tokio::test]
async fn kind_create_and_retire() {
    let Some((prov, kube, ns)) = provisioner() else {
        println!("SPRIG_KIND_API not set: skipped");
        return;
    };
    for pool in env("SPRIG_KIND_CREATE").unwrap_or_default().split(',') {
        if pool.is_empty() {
            continue;
        }
        let spec = ClusterSpec {
            dc: DcId::new("kind1").unwrap(),
            topology: Topology::Single,
            nodes: 1,
            version: MariaDbVersion {
                major: 11,
                minor: 8,
            },
            resources: Resources::default(),
            zones: Vec::new(),
            backup_target: String::new(),
            pool: Some(pool.to_owned()),
        };
        match prov.create(spec).await {
            Ok(job) => {
                let made = kube.list(&SPRIG_CLUSTER, &ns, None).await.unwrap();
                let name = made
                    .iter()
                    .find(|c| {
                        c["metadata"]["annotations"]["sprigdb.io/job-id"].as_str()
                            == Some(job.to_string().as_str())
                    })
                    .map(|c| {
                        c["metadata"]["name"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned()
                    });
                println!("create pool={pool} job={job} cluster={name:?}");
                assert!(name.is_some(), "no SprigCluster carries job {job}");
            }
            Err(e) => {
                println!("create pool={pool} refused: {e}");
                assert_eq!(e.code(), Some(SprigCode::ProvisionNoHosts), "{e}");
            }
        }
    }
    for id in env("SPRIG_KIND_RETIRE").unwrap_or_default().split(',') {
        if id.is_empty() {
            continue;
        }
        let job = prov.retire(&ClusterId::new(id).unwrap()).await.unwrap();
        println!("retire cluster={id} job={job}");
    }
}

/// Acknowledged writes through a gateway while the primary fails over:
/// `SPRIG_KIND_WORKLOAD=<host>:<port>,<user>,<password>,<seconds>,<out file>`.
/// One connection (re-made after an error, 100 ms later) inserts rows
/// `id = 1, 2, ...` into `app.acks` in autocommit; every insert the
/// gateway acknowledged goes to the out file, one id per line, and errors
/// are printed with their time. `SPRIG_KIND_VERIFY=<same>` instead reads
/// the table back and fails if an acknowledged id is missing.
#[tokio::test]
async fn kind_ack_workload() {
    use mysql_async::prelude::Queryable;
    use std::io::Write as _;
    use std::time::{Duration, Instant};
    let (mode, raw) = match (env("SPRIG_KIND_WORKLOAD"), env("SPRIG_KIND_VERIFY")) {
        (Some(w), _) => ("write", w),
        (None, Some(v)) => ("verify", v),
        (None, None) => {
            println!("SPRIG_KIND_WORKLOAD not set: skipped");
            return;
        }
    };
    let f: Vec<&str> = raw.split(',').collect();
    // `a|b|c`: straight to MariaDB, as root over TLS, always to
    // the node that is writable (`@@read_only = 0`), into r2opfix_acks.acks
    // instead of app.acks; measures the database without the gateway.
    let direct: Vec<String> = f[0].split('|').map(str::to_owned).collect();
    let is_direct = direct.len() > 1;
    let table = if is_direct {
        "r2opfix_acks.acks"
    } else {
        "app.acks"
    };
    let opts_for = |addr: &str| {
        let (host, port) = addr.rsplit_once(':').unwrap();
        let mut o = mysql_async::OptsBuilder::default()
            .ip_or_hostname(host.to_owned())
            .tcp_port(port.parse().unwrap())
            .user(Some(f[1].to_owned()))
            .pass(Some(f[2].to_owned()))
            .prefer_socket(false);
        if is_direct {
            o = o.ssl_opts(Some(
                mysql_async::SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ));
        }
        o
    };
    let opts = opts_for(&direct[0]);
    // The writable node among `direct`, or the gateway.
    let connect = |direct: Vec<String>, opts: mysql_async::OptsBuilder| async move {
        if direct.len() < 2 {
            return mysql_async::Conn::new(opts).await;
        }
        let mut last = None;
        for a in &direct {
            let (host, port) = a.rsplit_once(':').unwrap();
            let o = opts
                .clone()
                .ip_or_hostname(host.to_owned())
                .tcp_port(port.parse().unwrap());
            match tokio::time::timeout(Duration::from_secs(2), mysql_async::Conn::new(o)).await {
                Ok(Ok(mut c)) => {
                    let ro: Option<i64> =
                        c.query_first("SELECT @@read_only").await.unwrap_or(Some(1));
                    if ro == Some(0) {
                        c.query_drop("CREATE DATABASE IF NOT EXISTS r2opfix_acks")
                            .await?;
                        c.query_drop("CREATE TABLE IF NOT EXISTS r2opfix_acks.acks (id BIGINT PRIMARY KEY, v VARCHAR(8))").await?;
                        return Ok(c);
                    }
                    let _ = c.disconnect().await;
                }
                Ok(Err(e)) => last = Some(e),
                Err(_) => {}
            }
        }
        Err(last.unwrap_or_else(|| mysql_async::Error::Other("no writable node".into())))
    };
    let secs: u64 = f[3].parse().unwrap();
    let out = PathBuf::from(f[4]);
    if mode == "verify" {
        let mut c = connect(direct.clone(), opts).await.unwrap();
        let have: Vec<i64> = c.query(format!("SELECT id FROM {table}")).await.unwrap();
        let have: std::collections::HashSet<i64> = have.into_iter().collect();
        let acked: Vec<i64> = std::fs::read_to_string(&out)
            .unwrap()
            .lines()
            .filter_map(|l| l.parse().ok())
            .collect();
        let lost: Vec<&i64> = acked.iter().filter(|i| !have.contains(i)).collect();
        println!(
            "verify: acked={} in_table={} lost={} first_lost={:?}",
            acked.len(),
            have.len(),
            lost.len(),
            lost.iter().take(10).collect::<Vec<_>>()
        );
        c.disconnect().await.unwrap();
        assert!(lost.is_empty(), "acknowledged writes lost: {lost:?}");
        return;
    }
    let mut file = std::fs::File::create(&out).unwrap();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(secs);
    let mut conn: Option<mysql_async::Conn> = None;
    let (mut id, mut acked, mut errors) = (0i64, 0u64, 0u64);
    let mut last_ok: Option<Duration> = None;
    let mut longest_gap = Duration::ZERO;
    while Instant::now() < deadline {
        if conn.is_none() {
            match connect(direct.clone(), opts.clone()).await {
                Ok(c) => conn = Some(c),
                Err(e) => {
                    errors += 1;
                    println!("{:>8.3}s connect: {e}", start.elapsed().as_secs_f64());
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }
        id += 1;
        let c = conn.as_mut().unwrap();
        let r = tokio::time::timeout(
            Duration::from_secs(10),
            c.exec_drop(
                format!("INSERT INTO {table} (id, v) VALUES (?, 'w')"),
                (id,),
            ),
        )
        .await;
        match r {
            Ok(Ok(())) => {
                writeln!(file, "{id}").unwrap();
                acked += 1;
                let now = start.elapsed();
                if let Some(l) = last_ok {
                    longest_gap = longest_gap.max(now.saturating_sub(l));
                }
                last_ok = Some(now);
            }
            Ok(Err(e)) => {
                errors += 1;
                println!("{:>8.3}s insert {id}: {e}", start.elapsed().as_secs_f64());
                conn = None;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(_) => {
                errors += 1;
                println!(
                    "{:>8.3}s insert {id}: timed out",
                    start.elapsed().as_secs_f64()
                );
                conn = None;
            }
        }
    }
    file.flush().unwrap();
    if let Some(c) = conn {
        let _ = c.disconnect().await;
    }
    println!(
        "workload: attempted={id} acked={acked} errors={errors} longest_gap_ms={} rate={:.0}/s",
        longest_gap.as_millis(),
        acked as f64 / secs as f64
    );
}
