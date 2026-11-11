//! Chapter 42 through a running gateway (R6 order 2, read-through, single
//! cluster): a three-level tenant tree with spaces inside each tenant,
//! random writes at random layers checked against the Rust reference model
//! after every statement, schema changes while an object is shared
//! (39895), privileges, and what schema tools see.
//!
//! ```sh
//! export SPRIG_TEST_R6I_GATEWAY=127.0.0.1:38306,127.0.0.1:38307   # mysql, mariadb persona
//! export SPRIG_TEST_R6I_TENANTS=r6-inh-a,r6-inh-b,r6-inh-c        # a > b > c in the tenant tree
//! export SPRIG_TEST_R6I_PASSWORD=sprig-dev-app                     # of user `app` in each
//! cargo test -p sprig-inherit --test inherit_live -- --nocapture --test-threads 1
//! ```
//!
//! The two model tests need a deployment without a worker process: the
//! controller's `inherit` loop would give the tree a copy worker, which
//! orphans overrides across tenants while the read-through model expects
//! them hidden, and keeps copying while the materialized test has stopped
//! its own worker (R6 round 5: both tests fail that way,
//! deterministically, and pass without one). They check that themselves
//! (R6 round 6, decision T1): when the deployment's etcd
//! (`SPRIG_TEST_ETCD`, default `http://127.0.0.1:2379`) has a live worker
//! that takes `other` jobs, or the tree has an `inherit_copy` job that
//! isn't finished, they say so and skip (and fail under
//! `SPRIG_TEST_STRICT=1`).
//!
//! Without the variable every test reports what it needs and passes (or
//! fails under `SPRIG_TEST_STRICT=1`). Each test uses its own database
//! (`r6inh_<test>_<persona>`) in every tenant and drops it at the end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::uninlined_format_args,
    clippy::many_single_char_names,
    clippy::single_match_else,
    clippy::ref_option,
    missing_docs
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row, SslOpts, Value};
use sprig_inherit::reference::{EdgeModel, Fields, Filter, Model, ModelError, View};

const GATEWAY: &str = "SPRIG_TEST_R6I_GATEWAY";

fn endpoints(test: &str) -> Option<Vec<String>> {
    match std::env::var(GATEWAY).ok().filter(|v| !v.trim().is_empty()) {
        Some(v) => Some(v.split(',').map(|s| s.trim().to_owned()).collect()),
        None => {
            assert!(
                std::env::var("SPRIG_TEST_STRICT").ok().as_deref() != Some("1"),
                "{test}: {GATEWAY} is not set"
            );
            eprintln!("{test}: skipped, set {GATEWAY}");
            None
        }
    }
}

fn tenants() -> Vec<String> {
    std::env::var("SPRIG_TEST_R6I_TENANTS")
        .unwrap_or_else(|_| "r6-inh-a,r6-inh-b,r6-inh-c".into())
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect()
}

async fn conn_as(addr: &str, user: &str, pass: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(pass))
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("connect {addr} as {user}: {e}"))
}

async fn conn(addr: &str, tenant: &str) -> Conn {
    let pass = std::env::var("SPRIG_TEST_R6I_PASSWORD").unwrap_or_else(|_| "sprig-dev-app".into());
    conn_as(addr, &format!("{tenant}.app"), &pass).await
}

fn persona(i: usize) -> &'static str {
    if i == 0 { "my" } else { "ma" }
}

fn errno(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(s) => s.code,
        other => panic!("not a server error: {other}"),
    }
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::Int(i) => Some(i.to_string()),
        Value::UInt(u) => Some(u.to_string()),
        other => Some(format!("{other:?}")),
    }
}

fn no_leak(sql: &str, s: &str) {
    assert!(
        sprig_sql::names::physical_name_leaks(s.as_bytes()).is_empty(),
        "{sql}: physical name in {s:?}"
    );
    assert!(!s.contains("_sprig_"), "{sql}: internal name in {s:?}");
}

async fn rows(c: &mut Conn, sql: &str) -> Vec<Vec<Option<String>>> {
    let r: Vec<Row> = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let out: Vec<Vec<Option<String>>> = r
        .into_iter()
        .map(|r| r.unwrap().iter().map(text).collect())
        .collect();
    for row in &out {
        for v in row.iter().flatten() {
            no_leak(sql, v);
        }
    }
    out
}

async fn exec(c: &mut Conn, sql: &str) -> u64 {
    c.query_drop(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    c.affected_rows()
}

async fn fails(c: &mut Conn, sql: &str, code: u16) {
    let e = c.query_drop(sql).await.expect_err(sql);
    assert_eq!(errno(&e), code, "{sql}: {e}");
    let m = e.to_string();
    assert!(
        sprig_sql::names::physical_name_leaks(m.as_bytes()).is_empty(),
        "{sql}: physical name in {m:?}"
    );
}

async fn try_exec(c: &mut Conn, sql: &str) -> Result<u64, u16> {
    match c.query_drop(sql).await {
        Ok(()) => Ok(c.affected_rows()),
        Err(e) => {
            no_leak(sql, &e.to_string());
            Err(errno(&e))
        }
    }
}

/// A small deterministic generator.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const DDL: &str = "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(40) NULL, \
                   colour VARCHAR(20) NULL, price INT NULL, region CHAR(2) NULL)";
const FIELDS: [&str; 4] = ["name", "colour", "price", "region"];

/// One space of the test tree: its tenant (connection index), its name,
/// its model id.
#[derive(Clone)]
struct Sp {
    t: usize,
    name: String,
    id: u64,
}

async fn use_space(c: &mut Conn, s: &Sp) {
    exec(c, &format!("USE SPACE `{}`", s.name)).await;
}

async fn read_view(c: &mut Conn, s: &Sp) -> View {
    use_space(c, s).await;
    rows(
        c,
        "SELECT id, name, colour, price, region FROM products ORDER BY id",
    )
    .await
    .into_iter()
    .map(|r| {
        (
            r[0].as_ref().unwrap().parse::<i64>().unwrap(),
            r[1..].to_vec(),
        )
    })
    .collect()
}

fn lit(v: &Option<String>, numeric: bool) -> String {
    match v {
        None => "NULL".into(),
        Some(s) if numeric => s.clone(),
        Some(s) => format!("'{s}'"),
    }
}

async fn setup(conns: &mut [Conn], db: &str, p: &str, t: &[String]) -> Vec<Sp> {
    // Spaces are tenant-wide and kept between runs.
    let sub = |i: usize| format!("{}x{}", ["a", "b", "c"][i], 1);
    // Shares left by an interrupted run.
    for (i, n) in [(0, "sa1"), (0, "sab"), (1, "sbc"), (2, "sc1")] {
        exec(&mut conns[i], &format!("USE SPACE `{}`", t[i])).await;
        exec(&mut conns[i], &format!("DROP SHARE IF EXISTS {n}{p}")).await;
    }
    for (i, c) in conns.iter_mut().enumerate() {
        exec(c, &format!("USE SPACE `{}`", t[i])).await;
        for s in ["DROP DATABASE IF EXISTS", "CREATE DATABASE"] {
            exec(c, &format!("{s} {db}")).await;
        }
        exec(c, &format!("USE {db}")).await;
        exec(c, DDL).await;
        exec(c, "ALTER TABLE products SPRIG INHERITABLE KEY (id)").await;
        match try_exec(c, &format!("CREATE SPACE {} UNDER `{}`", sub(i), t[i])).await {
            Ok(_) | Err(39881) => {}
            Err(e) => panic!("CREATE SPACE: {e}"),
        }
    }
    // a: its sub-space takes price < 50; tenant b takes everything.
    exec(
        &mut conns[0],
        &format!("CREATE SHARE sa1{p} ON products TO SPACE ax1 WHERE price < 50"),
    )
    .await;
    exec(
        &mut conns[0],
        &format!(
            "CREATE SHARE sab{p} ON products TO SPACE `{}` WITH RESHARE",
            t[1]
        ),
    )
    .await;
    // b: to all children (its sub-space and tenant c).
    exec(
        &mut conns[1],
        &format!("CREATE SHARE sbc{p} ON products TO ALL CHILDREN WITH RESHARE"),
    )
    .await;
    // c: its sub-space sees name and price only.
    exec(
        &mut conns[2],
        &format!("CREATE SHARE sc1{p} ON products TO SPACE cx1 COLUMNS (name, price)"),
    )
    .await;
    vec![
        Sp {
            t: 0,
            name: t[0].clone(),
            id: 100,
        },
        Sp {
            t: 0,
            name: "ax1".into(),
            id: 101,
        },
        Sp {
            t: 1,
            name: t[1].clone(),
            id: 200,
        },
        Sp {
            t: 1,
            name: "bx1".into(),
            id: 201,
        },
        Sp {
            t: 2,
            name: t[2].clone(),
            id: 300,
        },
        Sp {
            t: 2,
            name: "cx1".into(),
            id: 301,
        },
    ]
}

fn model() -> Model {
    let mut m = Model::new(4);
    let all = || {
        Some(EdgeModel {
            filter: None,
            columns: None,
        })
    };
    let cheap: Filter = Arc::new(|_, f: &Fields| {
        f[2].as_deref()
            .and_then(|p| p.parse::<i64>().ok())
            .is_some_and(|p| p < 50)
    });
    m.space(100, None, 1, None);
    m.space(
        101,
        Some(100),
        1,
        Some(EdgeModel {
            filter: Some(cheap),
            columns: None,
        }),
    );
    m.space(200, Some(100), 2, all());
    m.space(201, Some(200), 2, all());
    m.space(300, Some(200), 3, all());
    m.space(
        301,
        Some(300),
        3,
        Some(EdgeModel {
            filter: None,
            columns: Some(vec![0, 2]),
        }),
    );
    m
}

async fn teardown(conns: &mut [Conn], db: &str, p: &str, t: &[String]) {
    for (i, name) in [("sa1", 0), ("sab", 0), ("sbc", 1), ("sc1", 2)]
        .iter()
        .map(|(n, i)| (*i, format!("{n}{p}")))
    {
        exec(&mut conns[i], &format!("USE SPACE `{}`", t[i])).await;
        exec(&mut conns[i], &format!("DROP SHARE IF EXISTS {name}")).await;
    }
    for c in conns.iter_mut().rev() {
        exec(c, &format!("DROP DATABASE IF EXISTS {db}")).await;
    }
}

/// Skips (`true`) when a worker process could copy the tree (module
/// docs, decision T1).
async fn worker_copies_the_tree(test: &str) -> bool {
    match Box::pin(copyw::tree_copier(&tenants()[0])).await {
        Ok(None) => false,
        Ok(Some(why)) => {
            assert!(
                std::env::var("SPRIG_TEST_STRICT").ok().as_deref() != Some("1"),
                "{test}: {why}"
            );
            eprintln!(
                "{test}: skipped: {why}. This test needs a deployment without a worker \
                 process (stop the worker, or run it on its own deployment)"
            );
            true
        }
        Err(e) => {
            // No etcd: nothing can dispatch a copy worker either.
            eprintln!("{test}: workers not checked ({e})");
            false
        }
    }
}

#[tokio::test]
async fn merged_views_match_the_model() {
    let Some(eps) = endpoints("merged_views_match_the_model") else {
        return;
    };
    if Box::pin(worker_copies_the_tree("merged_views_match_the_model")).await {
        return;
    }
    model_run(&eps, None).await;
}

/// Chapter 42's reference-model workload. With a worker, every share is
/// materialized (`inherit.merge = materialized`), the copy worker runs in
/// this process, and after each write the test waits until every space's
/// view equals the model (the copies converged), measuring how long that
/// took.
async fn model_run(eps: &[String], mut worker: Option<&mut copyw::Worker>) {
    let t = tenants();
    let materialized = worker.is_some();
    let ops: u64 = std::env::var("SPRIG_TEST_R6I_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(if materialized { 200 } else { 300 });
    for (pi, ep) in eps.iter().enumerate() {
        let p = persona(pi);
        let db = format!("r6inh_{}_{p}", if materialized { "mat" } else { "model" });
        let mut conns = Vec::new();
        for name in &t {
            conns.push(conn(ep, name).await);
        }
        for (i, c) in conns.iter_mut().enumerate() {
            exec(c, &format!("USE SPACE `{}`", t[i])).await;
            if materialized {
                exec(c, "SPRIG SET inherit.merge = 'materialized'").await;
            } else {
                let _ = try_exec(c, "SPRIG RESET inherit.merge").await;
            }
        }
        let spaces = setup(&mut conns, &db, p, &t).await;
        let mut m = model();
        m.async_orphans = materialized;
        let mut lags: Vec<u128> = Vec::new();
        let mut instant = 0u64;
        if let Some(w) = worker.as_deref_mut() {
            w.ensure_running();
            let t0 = Instant::now();
            w.wait_serving(&t, &db, 5).await;
            eprintln!(
                "{p}: 5 copies serving {:.1?} after the shares",
                t0.elapsed()
            );
            // The gateways' copy-state cache.
            tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        }
        let mut g = Lcg(0x5eed_0000 + pi as u64);
        let t0 = Instant::now();
        let (mut writes, mut reads, mut dups) = (0u64, 0u64, 0u64);
        let mut counts: BTreeMap<&str, u64> = BTreeMap::new();
        for step in 0..ops {
            let s = spaces[g.below(spaces.len() as u64) as usize].clone();
            let c = &mut conns[s.t];
            use_space(c, &s).await;
            let k = g.below(30) as i64 + 1;
            let roll = g.below(100);
            let what;
            if roll < 35 {
                what = "insert";
                let f: Fields = vec![
                    Some(format!("n{}", g.below(1000))),
                    Some(format!("c{}", g.below(10))),
                    Some((g.below(100) + 1).to_string()),
                    Some(["IN", "US"][g.below(2) as usize].to_owned()),
                ];
                let sql = format!(
                    "INSERT INTO products VALUES ({k}, {}, {}, {}, {})",
                    lit(&f[0], false),
                    lit(&f[1], false),
                    lit(&f[2], true),
                    lit(&f[3], false)
                );
                let got = try_exec(c, &sql).await;
                let want = m.insert(s.id, k, f);
                match (&got, &want) {
                    (Ok(1), Ok(())) => {}
                    (Err(1062), Err(ModelError::Duplicate(_))) => dups += 1,
                    _ => panic!("step {step} {}: {sql}: got {got:?}, model {want:?}", s.name),
                }
            } else if roll < 65 {
                what = "update";
                let fi = g.below(4) as usize;
                let val = match fi {
                    2 => Some((g.below(100) + 1).to_string()),
                    3 => Some(["IN", "US"][g.below(2) as usize].to_owned()),
                    _ => Some(format!("u{}", g.below(1000))),
                };
                let by_key = roll < 55;
                let sql = format!(
                    "UPDATE products SET {} = {} WHERE {}",
                    FIELDS[fi],
                    lit(&val, fi == 2),
                    if by_key {
                        format!("id = {k}")
                    } else {
                        "price < 40".to_owned()
                    }
                );
                exec(c, &sql).await;
                let pred = move |key: i64, f: &Fields| {
                    if by_key {
                        key == k
                    } else {
                        f[2].as_deref()
                            .and_then(|p| p.parse::<i64>().ok())
                            .is_some_and(|p| p < 40)
                    }
                };
                m.update(s.id, &pred, &[(fi, val)], false);
            } else if roll < 85 {
                what = "delete";
                let by_key = roll < 80;
                let sql = if by_key {
                    format!("DELETE FROM products WHERE id = {k}")
                } else {
                    "DELETE FROM products WHERE region = 'US' AND price > 80".to_owned()
                };
                let got = exec(c, &sql).await;
                let pred = move |key: i64, f: &Fields| {
                    if by_key {
                        key == k
                    } else {
                        f[3].as_deref() == Some("US")
                            && f[2]
                                .as_deref()
                                .and_then(|p| p.parse::<i64>().ok())
                                .is_some_and(|p| p > 80)
                    }
                };
                let want = m.delete(s.id, &pred);
                assert_eq!(got, want as u64, "step {step} {}: {sql}", s.name);
            } else {
                what = "reset";
                let col = (roll >= 95).then(|| g.below(4) as usize);
                let sql = match col {
                    Some(ci) => format!("SPRIG RESET products ({}) WHERE id = {k}", FIELDS[ci]),
                    None => format!("SPRIG RESET products WHERE id = {k}"),
                };
                exec(c, &sql).await;
                let cols = col.map(|ci| vec![ci]);
                m.reset(s.id, &move |key, _| key == k, cols.as_deref());
            }
            writes += 1;
            *counts.entry(what).or_default() += 1;
            if materialized {
                let t0 = Instant::now();
                let mut polls = 0;
                loop {
                    let mut same = true;
                    for x in &spaces {
                        if read_view(&mut conns[x.t], x).await != m.view(x.id) {
                            same = false;
                            break;
                        }
                    }
                    reads += 1;
                    if same {
                        break;
                    }
                    polls += 1;
                    if t0.elapsed() > std::time::Duration::from_secs(30) {
                        for x in &spaces {
                            let got = read_view(&mut conns[x.t], x).await;
                            assert_eq!(
                                got,
                                m.view(x.id),
                                "step {step} ({what} in {}): {} never converged",
                                s.name,
                                x.name
                            );
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                if polls == 0 {
                    instant += 1;
                }
                lags.push(t0.elapsed().as_millis());
                // Put the session back in the written space.
                use_space(&mut conns[s.t], &s).await;
            }
            let c = &mut conns[s.t];
            // A point read (the key pushed into every level) of the key.
            let point = rows(
                c,
                &format!("SELECT name, colour, price, region FROM products WHERE id = {k}"),
            )
            .await;
            assert_eq!(
                point.first().cloned(),
                m.view(s.id).get(&k).cloned(),
                "step {step} ({what}): point read of {k} in {}",
                s.name
            );
            // The written space after every statement; all of them every 10.
            let check: Vec<Sp> = if step % 10 == 9 {
                spaces.clone()
            } else {
                vec![s.clone()]
            };
            for x in &check {
                let got = read_view(&mut conns[x.t], x).await;
                reads += 1;
                let want = m.view(x.id);
                assert_eq!(
                    got, want,
                    "step {step} ({what} in {}): view of {} differs",
                    s.name, x.name
                );
            }
        }
        for x in &spaces {
            assert_eq!(
                read_view(&mut conns[x.t], x).await,
                m.view(x.id),
                "final {}",
                x.name
            );
        }
        if materialized {
            lags.sort_unstable();
            let pct = |q: usize| lags[(lags.len() * q / 100).min(lags.len() - 1)];
            eprintln!(
                "{p}: copies converged after each write: p50 {} ms, p99 {} ms, max {} ms \
                 ({instant} of {} writes needed no wait)",
                pct(50),
                pct(99),
                lags[lags.len() - 1],
                lags.len()
            );
            assert!(
                pct(99) <= 1000,
                "{p}: p99 convergence {} ms is above inherit.copy_max_lag_ms",
                pct(99)
            );
            if let Some(w) = worker.as_deref_mut() {
                copyw::kill_and_resume(w, &mut conns, &spaces, &mut m, &t).await;
            }
        }
        let rows_total: usize = spaces.iter().map(|x| m.view(x.id).len()).sum();
        eprintln!(
            "{p}: {writes} writes ({counts:?}, {dups} refused as duplicates) and {reads} \
             six-level reads matched the model in {:.1?}; {rows_total} visible rows at the end; \
             {} layer rows",
            t0.elapsed(),
            m.rows.len()
        );
        // SPRIG_TEST_R6I_KEEP=1 leaves the databases (and their copies)
        // for the leak suite.
        if std::env::var("SPRIG_TEST_R6I_KEEP").as_deref() == Ok("1") {
            continue;
        }
        teardown(&mut conns, &db, p, &t).await;
        if materialized {
            for (i, c) in conns.iter_mut().enumerate() {
                exec(c, &format!("USE SPACE `{}`", t[i])).await;
                exec(c, "SPRIG RESET inherit.merge").await;
            }
        }
    }
}

#[tokio::test]
async fn materialized_views_converge_to_the_model() {
    let Some(eps) = endpoints("materialized_views_converge_to_the_model") else {
        return;
    };
    if Box::pin(worker_copies_the_tree(
        "materialized_views_converge_to_the_model",
    ))
    .await
    {
        return;
    }
    let mut w = copyw::Worker::new(&tenants()[0]).await;
    model_run(&eps, Some(&mut w)).await;
    w.stop().await;
}

#[tokio::test]
async fn orphans_origin_and_record_mode() {
    let Some(eps) = endpoints("orphans_origin_and_record_mode") else {
        return;
    };
    let t = tenants();
    let ep = &eps[eps.len() - 1];
    let mut a = conn(ep, &t[0]).await;
    let db = "r6inh_orphan";
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    exec(&mut a, &format!("DROP DATABASE IF EXISTS {db}")).await;
    exec(&mut a, &format!("CREATE DATABASE {db}")).await;
    exec(&mut a, &format!("USE {db}")).await;
    exec(&mut a, DDL).await;
    exec(&mut a, "ALTER TABLE products SPRIG INHERITABLE KEY (id)").await;
    for s in ["CREATE SPACE ox1", "CREATE SPACE ox2 UNDER ox1"] {
        match try_exec(&mut a, s).await {
            Ok(_) | Err(39881) => {}
            Err(e) => panic!("{s}: {e}"),
        }
    }
    exec(
        &mut a,
        "CREATE SHARE so1 ON products TO SPACE ox1 WHERE region = 'IN' WITH RESHARE",
    )
    .await;
    exec(&mut a, "USE SPACE ox1").await;
    exec(&mut a, "CREATE SHARE so2 ON products TO SPACE ox2").await;
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    exec(
        &mut a,
        "INSERT INTO products VALUES (1,'P1','red',10,'IN'),(2,'P2','blue',20,'IN'),\
         (3,'P3','green',30,'US')",
    )
    .await;
    exec(&mut a, "USE SPACE ox1").await;
    assert_eq!(
        exec(&mut a, "UPDATE products SET colour = 'navy' WHERE id = 2").await,
        1
    );
    exec(
        &mut a,
        "INSERT INTO products VALUES (9,'P9','black',15,'IN')",
    )
    .await;
    fails(
        &mut a,
        "INSERT INTO products VALUES (1,'x','x',1,'IN')",
        1062,
    )
    .await;
    fails(&mut a, "UPDATE products SET id = 5 WHERE id = 1", 39886).await;
    exec(&mut a, "USE SPACE ox2").await;
    assert_eq!(exec(&mut a, "DELETE FROM products WHERE id = 1").await, 1);
    let ids = |r: Vec<Vec<Option<String>>>| -> Vec<String> {
        r.into_iter().map(|x| x[0].clone().unwrap()).collect()
    };
    assert_eq!(
        ids(rows(&mut a, "SELECT id FROM products ORDER BY id").await),
        ["2", "9"]
    );
    // SPRIG_ORIGIN() names each field's space.
    let o = rows(
        &mut a,
        "SELECT id, SPRIG_ORIGIN() FROM products WHERE id = 2",
    )
    .await;
    let j: serde_json::Value = serde_json::from_str(o[0][1].as_ref().unwrap()).unwrap();
    assert_eq!(j["colour"], "ox1");
    assert_eq!(j["name"], t[0].as_str());
    // The platform deletes P2 (keep): ox1's override is orphaned.
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    assert_eq!(exec(&mut a, "DELETE FROM products WHERE id = 2").await, 1);
    exec(&mut a, "USE SPACE ox2").await;
    let r = rows(
        &mut a,
        "SELECT id, colour, SPRIG_ORIGIN() FROM products WHERE id = 2",
    )
    .await;
    assert_eq!(r[0][1].as_deref(), Some("navy"));
    let j: serde_json::Value = serde_json::from_str(r[0][2].as_ref().unwrap()).unwrap();
    assert_eq!(j["name"], "ox1 (orphaned)");
    // Resetting the orphan removes P2 from ox1 and ox2.
    exec(&mut a, "USE SPACE ox1").await;
    exec(&mut a, "SPRIG RESET products WHERE id = 2").await;
    exec(&mut a, "USE SPACE ox2").await;
    assert_eq!(
        ids(rows(&mut a, "SELECT id FROM products ORDER BY id").await),
        ["9"]
    );
    // Whole-record overrides.
    exec(&mut a, "SPRIG SET inherit.override = 'record'").await;
    exec(&mut a, "USE SPACE ox1").await;
    exec(&mut a, "UPDATE products SET colour = 'pink' WHERE id = 1").await;
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    exec(&mut a, "UPDATE products SET name = 'P1 new' WHERE id = 1").await;
    exec(&mut a, "USE SPACE ox1").await;
    let r = rows(&mut a, "SELECT name, colour FROM products WHERE id = 1").await;
    assert_eq!(
        r[0][0].as_deref(),
        Some("P1"),
        "record override keeps the old name"
    );
    exec(&mut a, "SPRIG RESET inherit.override").await;
    // Parent delete with follow.
    exec(&mut a, "SPRIG SET inherit.parent_delete = 'follow'").await;
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    exec(&mut a, "DELETE FROM products WHERE id = 1").await;
    exec(&mut a, "USE SPACE ox1").await;
    assert!(
        rows(&mut a, "SELECT id FROM products WHERE id = 1")
            .await
            .is_empty()
    );
    exec(&mut a, "SPRIG RESET inherit.parent_delete").await;
    exec(&mut a, "USE SPACE ox1").await;
    exec(&mut a, "DROP SHARE so2").await;
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    exec(&mut a, "DROP SHARE so1").await;
    exec(&mut a, &format!("DROP DATABASE {db}")).await;
}

#[tokio::test]
async fn schema_changes_while_shared() {
    let Some(eps) = endpoints("schema_changes_while_shared") else {
        return;
    };
    let t = tenants();
    let ep = &eps[0];
    let mut a = conn(ep, &t[0]).await;
    let mut b = conn(ep, &t[1]).await;
    let db = "r6inh_ddl";
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    for sh in ["sddl", "sdx"] {
        exec(&mut a, &format!("DROP SHARE IF EXISTS {sh}")).await;
    }
    for c in [&mut a, &mut b] {
        exec(c, &format!("DROP DATABASE IF EXISTS {db}")).await;
        exec(c, &format!("CREATE DATABASE {db}")).await;
        exec(c, &format!("USE {db}")).await;
        exec(c, DDL).await;
        exec(c, "ALTER TABLE products SPRIG INHERITABLE KEY (id)").await;
    }
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    exec(&mut a, "INSERT INTO products VALUES (1,'P1','red',10,'IN')").await;
    exec(
        &mut a,
        &format!(
            "CREATE SHARE sddl ON products TO SPACE `{}` WITH RESHARE",
            t[1]
        ),
    )
    .await;
    // Additive changes pass.
    exec(
        &mut a,
        "ALTER TABLE products ADD COLUMN sku VARCHAR(20) NULL",
    )
    .await;
    exec(&mut a, "ALTER TABLE products ADD INDEX k_sku (sku)").await;
    exec(
        &mut a,
        "ALTER TABLE products MODIFY colour VARCHAR(40) NULL",
    )
    .await;
    exec(
        &mut a,
        "ALTER TABLE products ALTER COLUMN price SET DEFAULT 1",
    )
    .await;
    exec(
        &mut a,
        "ALTER TABLE products ADD COLUMN qty INT NOT NULL DEFAULT 0",
    )
    .await;
    // The child still reads (the new columns are not its own).
    let r = rows(&mut b, "SELECT id, name FROM products").await;
    assert_eq!(r.len(), 1);
    // Destructive ones are refused, naming the share.
    for sql in [
        "ALTER TABLE products DROP COLUMN region",
        "ALTER TABLE products CHANGE colour color VARCHAR(40) NULL",
        "ALTER TABLE products MODIFY colour VARCHAR(10) NULL",
        "ALTER TABLE products MODIFY price BIGINT NOT NULL",
        "ALTER TABLE products ADD COLUMN must INT NOT NULL",
        "ALTER TABLE products ADD UNIQUE KEY u_name (name)",
        "ALTER TABLE products RENAME TO items",
        "RENAME TABLE products TO items",
        "TRUNCATE TABLE products",
        "DROP TABLE products",
        "ALTER TABLE products DROP SPRIG INHERITABLE",
    ] {
        let e = a.query_drop(sql).await.expect_err(sql);
        assert!(matches!(errno(&e), 39895), "{sql}: expected 39895, got {e}");
        assert!(e.to_string().contains("sddl"), "{sql}: {e}");
    }
    fails(&mut a, &format!("DROP DATABASE {db}"), 39895).await;
    // After DROP SHARE the same changes run.
    exec(&mut a, "DROP SHARE sddl").await;
    exec(&mut a, "ALTER TABLE products DROP COLUMN region").await;
    exec(&mut a, "ALTER TABLE products ADD UNIQUE KEY u_name (name)").await;
    // A unique key is unique per layer: a child space may copy the value.
    match try_exec(&mut a, "CREATE SPACE dx1").await {
        Ok(_) | Err(39881) => {}
        Err(e) => panic!("{e}"),
    }
    exec(&mut a, "CREATE SHARE sdx ON products TO SPACE dx1").await;
    exec(&mut a, "USE SPACE dx1").await;
    exec(&mut a, "UPDATE products SET colour = 'teal' WHERE id = 1").await;
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    let create = rows(&mut a, "SHOW CREATE TABLE products").await;
    let text = create[0][1].clone().unwrap();
    assert!(text.contains("PRIMARY KEY (`id`)"), "{text}");
    assert!(text.contains("UNIQUE KEY `u_name` (`name`)"), "{text}");
    exec(&mut a, "DROP SHARE sdx").await;
    exec(&mut a, "DROP SPACE dx1 CASCADE").await;
    exec(&mut a, &format!("DROP DATABASE {db}")).await;
    exec(&mut b, &format!("DROP DATABASE {db}")).await;
}

#[tokio::test]
async fn privileges_and_isolation() {
    let Some(eps) = endpoints("privileges_and_isolation") else {
        return;
    };
    let t = tenants();
    let ep = &eps[eps.len() - 1];
    let mut a = conn(ep, &t[0]).await;
    let mut c = conn(ep, &t[2]).await;
    let mut b = conn(ep, &t[1]).await;
    let db = "r6inh_priv";
    // Leftovers of an interrupted run.
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    exec(&mut a, "DROP SHARE IF EXISTS spx").await;
    exec(&mut a, "DROP SHARE IF EXISTS spa").await;
    exec(&mut b, "DROP SHARE IF EXISTS spb").await;
    for x in [&mut a, &mut b, &mut c] {
        exec(x, &format!("DROP DATABASE IF EXISTS {db}")).await;
        exec(x, &format!("CREATE DATABASE {db}")).await;
        exec(x, &format!("USE {db}")).await;
        exec(x, DDL).await;
        exec(x, "ALTER TABLE products SPRIG INHERITABLE KEY (id)").await;
    }
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    exec(
        &mut a,
        "INSERT INTO products VALUES (1,'P1','red',10,'IN'),(2,'P2','blue',20,'US')",
    )
    .await;
    exec(
        &mut a,
        &format!(
            "CREATE SHARE spa ON products TO SPACE `{}` WHERE region = 'IN' WITH RESHARE",
            t[1]
        ),
    )
    .await;
    exec(&mut b, "CREATE SHARE spb ON products TO ALL CHILDREN").await;
    match try_exec(&mut a, "CREATE SPACE px1").await {
        Ok(_) | Err(39881) => {}
        Err(e) => panic!("{e}"),
    }
    exec(&mut a, "CREATE SHARE spx ON products TO SPACE px1").await;
    // A user homed in px1 acts there and below only.
    let _ = try_exec(&mut a, "DROP USER IF EXISTS limited").await;
    exec(
        &mut a,
        "CREATE USER limited IDENTIFIED BY 'Limited-pass-42'",
    )
    .await;
    exec(
        &mut a,
        &format!("GRANT SELECT, INSERT, UPDATE, DELETE ON {db}.* TO limited"),
    )
    .await;
    exec(&mut a, "ALTER SPACE px1 SET HOME FOR limited").await;
    let mut l = conn_as(ep, &format!("{}.limited", t[0]), "Limited-pass-42").await;
    exec(&mut l, &format!("USE {db}")).await;
    fails(&mut l, &format!("USE SPACE `{}`", t[0]), 39884).await;
    fails(&mut l, "CREATE SPACE nope", 1227).await;
    fails(&mut l, "ALTER SHARE spx WHERE id > 1", 39884).await;
    exec(&mut l, "UPDATE products SET colour = 'mine' WHERE id = 1").await;
    exec(&mut l, "DELETE FROM products WHERE id = 2").await;
    // The parent's layer is untouched.
    let r = rows(&mut a, "SELECT id, colour FROM products ORDER BY id").await;
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][1].as_deref(), Some("red"));
    // A child tenant sees only the share's subset, through the merged view.
    let r = rows(&mut c, "SELECT id FROM products").await;
    assert_eq!(r.len(), 1, "c sees only region IN");
    exec(&mut c, "UPDATE products SET colour = 'cc' WHERE id = 1").await;
    exec(&mut c, "DELETE FROM products WHERE id = 1").await;
    assert_eq!(
        rows(&mut b, "SELECT colour FROM products WHERE id = 1").await[0][0].as_deref(),
        Some("red")
    );
    assert_eq!(
        rows(&mut a, "SELECT colour FROM products WHERE id = 1").await[0][0].as_deref(),
        Some("red")
    );
    // Internal names and the parent's schemas can't be named.
    fails(&mut c, "SELECT _sprig_space FROM products", 1054).await;
    let share_id = rows(&mut b, "SHOW SHARES").await;
    assert!(!share_id.is_empty());
    let e = c
        .query_drop("SELECT * FROM __sprig_share_1")
        .await
        .expect_err("reserved");
    assert!(matches!(errno(&e), 1146 | 1142), "{e}");
    // A parent's share can't be changed by the child, only narrowed.
    fails(&mut b, "ALTER SHARE spa WHERE id > 0", 39887).await;
    exec(&mut b, "ALTER SHARE spa TAKE WHERE price < 5").await;
    assert!(rows(&mut b, "SELECT id FROM products").await.is_empty());
    exec(&mut b, "ALTER SHARE spa TAKE ALL").await;
    assert_eq!(rows(&mut b, "SELECT id FROM products").await.len(), 1);
    // Cleanup.
    exec(&mut a, "DROP USER limited").await;
    exec(&mut b, "DROP SHARE spb").await;
    for s in ["spx", "spa"] {
        exec(&mut a, &format!("DROP SHARE {s}")).await;
    }
    exec(&mut a, "DROP SPACE px1 CASCADE").await;
    for x in [&mut c, &mut b, &mut a] {
        exec(x, &format!("DROP DATABASE {db}")).await;
    }
}

#[tokio::test]
async fn schema_metadata() {
    let Some(eps) = endpoints("schema_metadata") else {
        return;
    };
    let t = tenants();
    for (pi, ep) in eps.iter().enumerate() {
        let mut a = conn(ep, &t[0]).await;
        let db = format!("r6inh_meta_{}", persona(pi));
        exec(&mut a, &format!("DROP DATABASE IF EXISTS {db}")).await;
        exec(&mut a, &format!("CREATE DATABASE {db}")).await;
        exec(&mut a, &format!("USE {db}")).await;
        exec(&mut a, "CREATE TABLE products (id INT PRIMARY KEY, sku VARCHAR(20), name VARCHAR(40), UNIQUE KEY uk_sku (sku))").await;
        exec(&mut a, "ALTER TABLE products SPRIG INHERITABLE KEY (id)").await;
        let create = rows(&mut a, "SHOW CREATE TABLE products").await;
        let text = create[0][1].clone().unwrap();
        assert!(text.contains("PRIMARY KEY (`id`)"), "{text}");
        assert!(text.contains("UNIQUE KEY `uk_sku` (`sku`)"), "{text}");
        let cols = rows(&mut a, "SHOW COLUMNS FROM products").await;
        assert_eq!(cols.len(), 3);
        let info = rows(
            &mut a,
            &format!(
                "SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{db}' \
                 AND TABLE_NAME = 'products' ORDER BY ORDINAL_POSITION"
            ),
        )
        .await;
        assert_eq!(info.len(), 3, "{info:?}");
        assert_eq!(info[0][1].as_deref(), Some("PRI"));
        let tables = rows(&mut a, "SHOW TABLES").await;
        assert_eq!(tables.len(), 1, "{tables:?}");
        exec(&mut a, "INSERT INTO products VALUES (1, 'a', 'x')").await;
        let star = rows(&mut a, "SELECT * FROM products").await;
        assert_eq!(star[0].len(), 3);
        fails(&mut a, "CREATE VIEW v AS SELECT * FROM products", 39891).await;
        exec(&mut a, &format!("DROP DATABASE {db}")).await;
    }
}

/// Decision I7 moved: a share the gateway's snapshot doesn't show yet (one
/// committed by another session an instant earlier) still blocks a
/// destructive change, because chapter 21's prepare phase checks the
/// catalog under its lock. The "other session" here writes the share row
/// straight into the catalog, so no version moves and the gateway's
/// snapshot (and its early check) can't see it.
#[tokio::test]
async fn prepare_phase_sees_a_share_the_snapshot_missed() {
    let Some(eps) = endpoints("prepare_phase_sees_a_share_the_snapshot_missed") else {
        return;
    };
    let t = tenants();
    let w = copyw::Worker::new(&t[0]).await;
    let dsn = std::env::var("SPRIG_TEST_R6I_ROOT_DSN")
        .unwrap_or_else(|_| "mysql://root:sprig-dev-root@127.0.0.1:13301".into());
    let mut root = Conn::new(mysql_async::Opts::from_url(&dsn).unwrap())
        .await
        .unwrap();
    let cat = format!("t{}_sprig", w.root.0);
    let mut a = conn(&eps[0], &t[0]).await;
    let db = "r6inh_prep";
    root.query_drop(format!(
        "DELETE FROM `{cat}`.shares WHERE share_id = 1 AND name = 'sprep'"
    ))
    .await
    .unwrap();
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    exec(&mut a, &format!("DROP DATABASE IF EXISTS {db}")).await;
    exec(&mut a, &format!("CREATE DATABASE {db}")).await;
    exec(&mut a, &format!("USE {db}")).await;
    exec(&mut a, DDL).await;
    exec(&mut a, "ALTER TABLE products SPRIG INHERITABLE KEY (id)").await;
    exec(&mut a, "CREATE TABLE plain (id INT PRIMARY KEY)").await;
    let object: u64 = root
        .query_first(format!(
            "SELECT i.object_id FROM `{cat}`.inheritable_objects i JOIN `{cat}`.objects o ON \
             o.object_id = i.object_id JOIN `{cat}`.`databases` d ON d.db_id = o.db_id WHERE \
             d.name = '{db}' AND o.name = 'products' AND d.state <> 'dropped' AND o.state <> \
             'dropped'"
        ))
        .await
        .unwrap()
        .expect("the object");
    root.query_drop(format!(
        "INSERT INTO `{cat}`.shares (share_id, role, name, object_id, from_space_id, \
         merge_mode, version, state, created_by, created_at, changed_version) VALUES \
         (1, 'granted', 'sprep', {object}, 1, 'read_through', 1, 'active', 0, \
         UTC_TIMESTAMP(6), 0)"
    ))
    .await
    .unwrap();
    let refused = [
        "ALTER TABLE products DROP COLUMN colour",
        "ALTER TABLE products MODIFY name VARCHAR(10) NULL",
        "RENAME TABLE products TO products2",
        "DROP TABLE products",
        "TRUNCATE TABLE products",
        &format!("DROP DATABASE {db}"),
    ];
    let mut checked = 0;
    for sql in refused {
        match a.query_drop(sql).await {
            Err(e) => {
                assert_eq!(errno(&e), 39895, "{sql}: {e}");
                assert!(e.to_string().contains("sprep"), "{sql}: {e}");
                checked += 1;
            }
            Ok(()) => panic!("{sql}: passed while shared"),
        }
    }
    // Additive changes pass; an ordinary table is unaffected.
    exec(
        &mut a,
        "ALTER TABLE products ADD COLUMN note VARCHAR(10) NULL",
    )
    .await;
    exec(&mut a, "DROP TABLE plain").await;
    root.query_drop(format!(
        "DELETE FROM `{cat}`.shares WHERE share_id = 1 AND name = 'sprep'"
    ))
    .await
    .unwrap();
    exec(&mut a, "DROP TABLE products").await;
    exec(&mut a, &format!("DROP DATABASE {db}")).await;
    eprintln!("{checked} destructive statements refused in the prepare phase (39895)");
}

/// The copy worker in this process (chapter 42, "Materialized copies"):
/// sprig-worker's `serve` loop over the live feeds, catalogs and clusters,
/// with the same etcd and clusters as the gateway under test.
mod copyw {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use mysql_async::Conn;
    use sprig_common::CancellationToken;
    use sprig_common::config::MetaConfig;
    use sprig_common::ids::{DcId, TenantId};
    use sprig_inherit::copy::{CopyEnv, Maintainer, copy_schemas, load_states};
    use sprig_inherit::reference::Model;
    use sprig_worker::inherit::{Feeds, LiveEnv, ServeOpts, serve};

    use super::{Lcg, Sp, exec, read_view, use_space};

    pub(crate) struct Worker {
        pub(crate) env: Arc<LiveEnv>,
        pub(crate) root: TenantId,
        pub(crate) epoch: u64,
        handle: Option<tokio::task::JoinHandle<()>>,
        stop: CancellationToken,
    }

    /// Why a worker process could copy the tree rooted at `root_name`, or
    /// `None` (decision T1): a live worker of the DC with slots for
    /// `other` jobs (the class of `inherit_copy`), or an `inherit_copy` job
    /// of the tree that isn't finished.
    pub(crate) async fn tree_copier(root_name: &str) -> Result<Option<String>, String> {
        let meta = MetaConfig {
            dc_seeds: vec![
                std::env::var("SPRIG_TEST_ETCD").unwrap_or_else(|_| "http://127.0.0.1:2379".into()),
            ],
            global_seeds: Vec::new(),
            deployment_id: "sprigdb-dev".into(),
        };
        let tiers = sprig_meta::etcd::connect_tiers(
            DcId::new("dc-a").map_err(|e| e.to_string())?,
            &meta,
            None,
            Duration::from_secs(5),
            true,
        )
        .await
        .map_err(|e| format!("etcd: {e}"))?;
        let dc: Arc<dyn sprig_meta::MetaStore> = Arc::new(tiers.dc);
        let workers: Vec<String> = sprig_controller::dispatch::live_workers(dc.as_ref())
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|w| {
                w.registration.slots.is_empty()
                    || w.registration
                        .slots
                        .get(&sprig_controller::dispatch::JobClass::Other)
                        .is_some_and(|n| *n > 0)
            })
            .map(|w| w.id)
            .collect();
        if !workers.is_empty() {
            return Ok(Some(format!(
                "worker process {} is live and would copy the tree",
                workers.join(", ")
            )));
        }
        let root = match tiers.global {
            Some(g) => sprig_controller::tree::Directory::new(Arc::new(g))
                .list()
                .await
                .map_err(|e| e.to_string())?
                .iter()
                .map(sprig_controller::tree::StoredEntry::body)
                .find(|e| e.name == root_name)
                .map(|e| e.tenant_id.0),
            None => None,
        };
        let jobs = sprig_controller::dispatch::list_jobs(dc.as_ref())
            .await
            .map_err(|e| e.to_string())?;
        Ok(jobs
            .iter()
            .map(sprig_controller::dispatch::StoredJob::job)
            .find(|j| {
                j.kind == sprig_inherit::subs::JOB_KIND
                    && !j.state.is_final()
                    && j.owner.is_some()
                    && root.is_none_or(|r| j.spec["root"].as_u64() == Some(r))
            })
            .map(|j| format!("copy job {} of the tree is owned by a worker", j.job_id)))
    }

    impl Worker {
        pub(crate) async fn new(root_name: &str) -> Self {
            let meta = MetaConfig {
                dc_seeds: vec![
                    std::env::var("SPRIG_TEST_ETCD")
                        .unwrap_or_else(|_| "http://127.0.0.1:2379".into()),
                ],
                global_seeds: Vec::new(),
                deployment_id: "sprigdb-dev".into(),
            };
            let tiers = sprig_meta::etcd::connect_tiers(
                DcId::new("dc-a").unwrap(),
                &meta,
                None,
                Duration::from_secs(5),
                true,
            )
            .await
            .expect("etcd");
            let dc: Arc<dyn sprig_meta::MetaStore> = Arc::new(tiers.dc);
            let global: Arc<dyn sprig_meta::MetaStore> =
                Arc::new(tiers.global.expect("global tier"));
            let registry =
                sprig_cluster::registry::ClusterRegistry::new(Arc::clone(&global), Arc::clone(&dc))
                    .unwrap();
            let pools: Arc<dyn sprig_controller::catalogs::ClusterPools> = Arc::new(
                sprig_controller::catalogs::RegistryPools::new(registry.clone(), None)
                    .budget("inherit_test", 8),
            );
            let locator = Arc::new(sprig_controller::catalogs::ShardMapLocator::new(
                Arc::clone(&dc),
                Arc::clone(&pools),
                sprig_controller::catalogs::default_naming(),
            ));
            let clusters = Arc::new(sprig_worker::logic::RegistryClusters::new(
                registry,
                Arc::clone(&pools),
            ));
            let env = Arc::new(
                LiveEnv::new(
                    Arc::clone(&dc),
                    Arc::clone(&global),
                    locator,
                    pools,
                    clusters,
                )
                .unwrap(),
            );
            let entries = sprig_controller::tree::Directory::new(global)
                .list()
                .await
                .unwrap();
            let root = entries
                .iter()
                .map(sprig_controller::tree::StoredEntry::body)
                .find(|e| e.name == root_name)
                .map(|e| e.tenant_id)
                .expect("root tenant");
            Self {
                env,
                root,
                epoch: 0,
                handle: None,
                stop: CancellationToken::new(),
            }
        }

        pub(crate) fn ensure_running(&mut self) {
            if self.handle.as_ref().is_some_and(|h| !h.is_finished()) {
                return;
            }
            self.start();
        }

        /// Starts a worker with the next epoch.
        pub(crate) fn start(&mut self) {
            self.epoch += 1;
            let env = Arc::clone(&self.env);
            let (root, epoch) = (self.root, self.epoch);
            let stop = CancellationToken::new();
            self.stop = stop.clone();
            self.handle = Some(tokio::spawn(async move {
                let mut m = Maintainer::new(Arc::clone(&env) as Arc<dyn CopyEnv>, root, epoch);
                let feeds: Arc<dyn Feeds> = env;
                let opts = ServeOpts {
                    ack_every: Duration::from_millis(200),
                    refresh_every: Duration::from_secs(1),
                    ..ServeOpts::default()
                };
                let mut noop = |_| {};
                match serve(root, feeds, &mut m, &opts, &stop, &mut noop).await {
                    Ok(s) => eprintln!("copy worker {epoch} stopped: {s:?}, {:?}", m.counts),
                    Err(e) => eprintln!("copy worker {epoch} failed: {e}"),
                }
            }));
        }

        /// A crash: the loop is dropped wherever it is; nothing more is
        /// acknowledged.
        pub(crate) fn kill(&mut self) {
            if let Some(h) = self.handle.take() {
                h.abort();
            }
        }

        pub(crate) async fn stop(&mut self) {
            self.stop.cancel();
            if let Some(h) = self.handle.take() {
                let _ = h.await;
            }
        }

        /// Waits until `n` copies of `db` in the tree's tenants serve.
        pub(crate) async fn wait_serving(&self, names: &[String], db: &str, n: usize) {
            let t0 = Instant::now();
            loop {
                let mut serving = 0;
                for t in self.tenant_ids(names).await {
                    let cat = self.env.catalog(t).await.unwrap();
                    let parent = match cat.ancestry.last() {
                        Some(a) => Some(self.env.catalog(a.id).await.unwrap()),
                        None => None,
                    };
                    let mut c: Conn = self.env.conn(t).await.unwrap();
                    for s in copy_schemas(&cat, parent.as_deref(), false) {
                        if !s.contains(&format!("_d{}_", db_id(&cat, db))) {
                            continue;
                        }
                        serving += load_states(&mut c, t, &s)
                            .await
                            .unwrap()
                            .iter()
                            .filter(|(_, i)| i.serving)
                            .count();
                    }
                }
                if serving >= n {
                    return;
                }
                assert!(
                    t0.elapsed() < Duration::from_secs(60),
                    "only {serving} of {n} copies serve"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        pub(crate) async fn tenant_ids(&self, names: &[String]) -> Vec<TenantId> {
            let mut out = Vec::new();
            for t in self.env.family(self.root).await.unwrap() {
                let _ = names;
                out.push(t);
            }
            out
        }
    }

    fn db_id(cat: &sprig_catalog::TenantCatalog, db: &str) -> u64 {
        cat.databases
            .iter()
            .find(|d| d.name == db)
            .map_or(0, |d| u64::from(d.db_id.0))
    }

    async fn converge(conns: &mut [Conn], spaces: &[Sp], m: &Model, within: Duration) -> Duration {
        let t0 = Instant::now();
        loop {
            let mut same = true;
            for x in spaces {
                if read_view(&mut conns[x.t], x).await != m.view(x.id) {
                    same = false;
                    break;
                }
            }
            if same {
                return t0.elapsed();
            }
            if t0.elapsed() > within {
                for x in spaces {
                    assert_eq!(
                        read_view(&mut conns[x.t], x).await,
                        m.view(x.id),
                        "{} did not converge",
                        x.name
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Worker loss: writes in the top tenant while the worker is dead stay
    /// out of the child tenants' views (they read the copies); a new
    /// worker resumes after the last acknowledged position and they
    /// converge. Then a burst of writes with the worker killed at random
    /// moments and restarted each time; afterwards every view equals the
    /// model (no event missed) and every copy holds one row per key and
    /// share (none twice).
    pub(crate) async fn kill_and_resume(
        w: &mut Worker,
        conns: &mut [Conn],
        spaces: &[Sp],
        m: &mut Model,
        t: &[String],
    ) {
        // 1. Dead worker: a's writes don't reach b and c.
        w.kill();
        let top = spaces[0].clone();
        use_space(&mut conns[0], &top).await;
        let before_b = read_view(&mut conns[1], &spaces[2]).await;
        for k in 1..=10i64 {
            let sql = format!(
                "INSERT INTO products VALUES ({}, 'dead{k}', 'c1', 5, 'IN')",
                100 + k
            );
            exec(&mut conns[0], &sql).await;
            m.insert(
                top.id,
                100 + k,
                vec![
                    Some(format!("dead{k}")),
                    Some("c1".into()),
                    Some("5".into()),
                    Some("IN".into()),
                ],
            )
            .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let stale_b = read_view(&mut conns[1], &spaces[2]).await;
        assert_eq!(
            stale_b, before_b,
            "b's view moved without a worker: not read from its copy"
        );
        w.start();
        let lag = converge(conns, spaces, m, Duration::from_secs(30)).await;
        eprintln!(
            "worker {} resumed; the 10 writes made while it was dead reached every space in {lag:.1?}",
            w.epoch
        );
        // 2. A burst with kills at random moments.
        let mut g = Lcg(0x6b11);
        let (mut kills, n) = (0, 60);
        for i in 0..n {
            let s = &spaces[g.below(spaces.len() as u64) as usize];
            use_space(&mut conns[s.t], s).await;
            let k = g.below(30) as i64 + 1;
            let v = (g.below(100) + 1).to_string();
            let sql = format!("UPDATE products SET price = {v} WHERE id = {k}");
            exec(&mut conns[s.t], &sql).await;
            m.update(s.id, &move |key, _| key == k, &[(2, Some(v))], false);
            if g.below(10) == 0 {
                w.kill();
                kills += 1;
                tokio::time::sleep(Duration::from_millis(g.below(300))).await;
                w.start();
            }
            let _ = i;
        }
        let lag = converge(conns, spaces, m, Duration::from_secs(60)).await;
        eprintln!(
            "{n} writes with {kills} worker kills: every view equal to the model {lag:.1?} after the last"
        );
        // No copy row twice: one per (key, share).
        let mut dup = 0u64;
        for tid in w.tenant_ids(t).await {
            let cat = w.env.catalog(tid).await.unwrap();
            let parent = match cat.ancestry.last() {
                Some(a) => Some(w.env.catalog(a.id).await.unwrap()),
                None => None,
            };
            let mut c = w.env.conn(tid).await.unwrap();
            for s in copy_schemas(&cat, parent.as_deref(), false) {
                let tables: Vec<String> = mysql_async::prelude::Queryable::exec(
                    &mut c,
                    "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND \
                     TABLE_NAME LIKE '\\_\\_sprig\\_ih\\_%' AND TABLE_NAME <> '__sprig_ih_state'",
                    (s.as_str(),),
                )
                .await
                .unwrap();
                for tb in tables {
                    let n: Option<u64> = mysql_async::prelude::Queryable::query_first(
                        &mut c,
                        format!(
                            "SELECT COUNT(*) FROM (SELECT id, _sprig_share FROM `{s}`.`{tb}` \
                             GROUP BY id, _sprig_share HAVING COUNT(*) > 1) d"
                        ),
                    )
                    .await
                    .unwrap();
                    dup += n.unwrap_or(0);
                }
            }
        }
        assert_eq!(dup, 0, "copy rows held twice");
        // Leave the keys the model test owns as they were for the final checks.
        let _ = m.view(top.id);
    }
}
