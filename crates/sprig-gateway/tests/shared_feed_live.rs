//! Live: after-commit triggers, search copies and MongoDB change streams
//! all read through their process's shared reader: one
//! binlog dump per cluster per worker and per gateway, however many
//! consumers. Against a private deployment (a gateway, one worker, the
//! controller, one MariaDB node).
//!
//! ```text
//! SPRIG_SFC_GW=127.0.0.1:37306                  # the gateway's MySQL port
//! SPRIG_SFC_MONGO='mongodb://{tenant}.app:sprig-dev-app@127.0.0.1:37717/?authSource=admin&directConnection=true&tls=true&tlsAllowInvalidCertificates=true'
//! SPRIG_SFC_TENANTS=rfa,rfb,rfc                 # tenants with user app
//! SPRIG_SFC_NODE=mysql://root:pw@127.0.0.1:37301  # the node, for PROCESSLIST
//! SPRIG_SFC_METRICS=127.0.0.1:37185,127.0.0.1:37190  # optional: /metrics
//! SPRIG_SFC_WORKER_PIDFILE=<state>/worker.pid   # optional: kill -9 at 1/3
//! SPRIG_SFC_WORKER_START='<command starting the worker again>'
//! SPRIG_SFC_GATEWAY_PIDFILE=<state>/gateway.pid # optional: kill -9 at 2/3
//! SPRIG_SFC_GATEWAY_START='<command starting the gateway again>'
//! cargo test -p sprig-gateway --test shared_feed_live -- --ignored --nocapture
//! ```
//!
//! Each tenant gets, in database `sfc`:
//!
//! - table `ord` with `SPRIG_SFC_TRIGGERS` (8) after-commit triggers in
//!   JavaScript, each writing `(trigger, id)` into `aud` once per insert;
//! - `SPRIG_SFC_INDEXES` (3) tables `s<j>`, each with a search index;
//! - `SPRIG_SFC_STREAMS` (8) collections `c<k>`, each with a collection
//!   change stream, and one database-wide change stream.
//!
//! Then `SPRIG_SFC_WRITERS` (4) SQL sessions per tenant run
//! `SPRIG_SFC_TXNS` (300) transactions in all, each inserting one row into
//! `ord` and each `s<j>` (every seventh rolls back), while one writer per
//! collection inserts `SPRIG_SFC_DOCS` (150) documents `{_id: i, seq: i}`
//! in order. A sampler counts `Binlog Dump` threads on the node every
//! 0.5 s (and, with `SPRIG_SFC_METRICS`, each process's
//! `sprig_feed_binlog_dumps` by kind). Checks:
//!
//! - every committed `ord` row ran each trigger exactly once, and no
//!   rolled-back one ran;
//! - each search index holds exactly its table's rows;
//! - each change stream saw each document once, in insert order, across
//!   the gateway's `kill -9` (the client resumes from its last token);
//! - at most `SPRIG_SFC_EXPECT_DUMPS` (2: one per process) dumps on the
//!   node at any sample, and no process ever had a dump of kind `tenant`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::StreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::change_stream::event::ResumeToken;
use mongodb::{Client, Collection};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const DB: &str = "sfc";

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn num(k: &str, d: u64) -> u64 {
    var(k).and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn pool(gw: &str, tenant: &str, max: usize) -> Pool {
    let (host, port) = gw.split_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.app")))
        .pass(Some("sprig-dev-app"))
        .ssl_opts(Some(
            mysql_async::SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        ))
        .pool_opts(
            PoolOpts::new()
                .with_constraints(PoolConstraints::new(0, max).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(120)),
        );
    Pool::new(opts)
}

async fn conn(p: &Pool) -> Conn {
    for _ in 0..480 {
        if let Ok(mut c) = p.get_conn().await
            && c.query_drop(format!("USE {DB}")).await.is_ok()
        {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("gateway unreachable");
}

/// `Binlog Dump` threads on the node.
async fn dumps(node: &str) -> u64 {
    let Ok(mut c) = Conn::new(Opts::from_url(node).unwrap()).await else {
        return 0;
    };
    let n: Option<u64> = c
        .query_first(
            "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'",
        )
        .await
        .unwrap_or(None);
    let _ = c.disconnect().await;
    n.unwrap_or(0)
}

/// `sprig_feed_binlog_dumps` of one process by kind (summed over nodes).
async fn dump_gauges(addr: &str) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    let Ok(Ok(mut s)) =
        tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(addr)).await
    else {
        return out;
    };
    let req = format!("GET /metrics HTTP/1.0\r\nHost: {addr}\r\n\r\n");
    if s.write_all(req.as_bytes()).await.is_err() {
        return out;
    }
    let mut body = String::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), s.read_to_string(&mut body)).await;
    for line in body.lines() {
        let Some(rest) = line.strip_prefix("sprig_feed_binlog_dumps{") else {
            continue;
        };
        let Some((labels, value)) = rest.split_once("} ") else {
            continue;
        };
        let kind = labels
            .split(',')
            .find_map(|l| l.strip_prefix("kind=\""))
            .map(|k| k.trim_end_matches('"').to_owned())
            .unwrap_or_default();
        *out.entry(kind).or_default() += value.trim().parse::<f64>().unwrap_or(0.0);
    }
    out
}

fn trigger_code(j: u64) -> String {
    format!(
        "export default function (row, old, op) {{
  db.exec('INSERT INTO aud (trig, id) VALUES (?, ?)', [{j}, row.id]);
}}"
    )
}

fn kill(pidfile: &str, what: &str) {
    let pid = std::fs::read_to_string(pidfile).unwrap();
    let pid = pid.trim();
    println!("kill -9 {what} {pid}");
    std::process::Command::new("kill")
        .args(["-9", pid])
        .status()
        .unwrap();
}

fn start(cmd: &str) {
    let ok = std::process::Command::new("bash")
        .args(["-c", cmd])
        .status()
        .unwrap()
        .success();
    assert!(ok, "{cmd} failed");
}

/// One change stream's documents, by collection, in the order seen.
type Seen = BTreeMap<String, Vec<i64>>;

/// Follows a change stream until `want` inserts arrived per collection
/// (or `stop`), resuming from its last token when the stream breaks.
async fn follow(
    client_uri: String,
    coll: Option<String>,
    want: BTreeMap<String, usize>,
    stop: Arc<AtomicBool>,
    ready: tokio::sync::mpsc::Sender<()>,
) -> (Seen, u64) {
    let mut seen: Seen = BTreeMap::new();
    let mut token: Option<ResumeToken> = None;
    let mut resumes = 0u64;
    let mut first = true;
    'outer: loop {
        let client = match Client::with_uri_str(&client_uri).await {
            Ok(c) => c,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let db = client.database(DB);
        let opened = match (&coll, &token) {
            (Some(c), Some(t)) => {
                db.collection::<Document>(c)
                    .watch()
                    .resume_after(t.clone())
                    .await
            }
            (Some(c), None) => db.collection::<Document>(c).watch().await,
            (None, Some(t)) => db.watch().resume_after(t.clone()).await,
            (None, None) => db.watch().await,
        };
        let mut s = match opened {
            Ok(s) => s.with_type::<Document>(),
            Err(e) => {
                if first {
                    panic!("change stream didn't open: {e}");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        if first {
            first = false;
            let _ = ready.send(()).await;
        }
        loop {
            let done = want
                .iter()
                .all(|(c, n)| seen.get(c).map_or(0, Vec::len) >= *n);
            if done || stop.load(Ordering::Relaxed) {
                break 'outer;
            }
            match tokio::time::timeout(Duration::from_secs(2), s.next()).await {
                Err(_) => {
                    if let Some(t) = s.resume_token() {
                        token = Some(t);
                    }
                }
                Ok(None) => break,
                Ok(Some(Err(e))) => {
                    println!("change stream {coll:?} broke ({e}); resuming");
                    resumes += 1;
                    break;
                }
                Ok(Some(Ok(ev))) => {
                    if ev.get_str("operationType") == Ok("insert") {
                        let c = ev
                            .get_document("ns")
                            .ok()
                            .and_then(|n| n.get_str("coll").ok())
                            .unwrap_or_default()
                            .to_owned();
                        let seq = ev
                            .get_document("fullDocument")
                            .ok()
                            .and_then(|d| match d.get("seq") {
                                Some(Bson::Int32(v)) => Some(i64::from(*v)),
                                Some(Bson::Int64(v)) => Some(*v),
                                _ => None,
                            })
                            .unwrap_or(-1);
                        seen.entry(c).or_default().push(seq);
                    }
                    if let Some(t) = s.resume_token() {
                        token = Some(t);
                    }
                }
            }
        }
    }
    (seen, resumes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs a private deployment (see the module docs)"]
async fn triggers_search_and_change_streams_share_one_dump_per_process() {
    let gw = var("SPRIG_SFC_GW").expect("SPRIG_SFC_GW");
    let mongo = var("SPRIG_SFC_MONGO").expect("SPRIG_SFC_MONGO");
    let node = var("SPRIG_SFC_NODE").expect("SPRIG_SFC_NODE");
    let tenants: Vec<String> = var("SPRIG_SFC_TENANTS")
        .expect("SPRIG_SFC_TENANTS")
        .split(',')
        .map(str::to_owned)
        .collect();
    let metrics: Vec<String> = var("SPRIG_SFC_METRICS")
        .map(|v| v.split(',').map(str::to_owned).collect())
        .unwrap_or_default();
    let triggers = num("SPRIG_SFC_TRIGGERS", 8);
    let indexes = num("SPRIG_SFC_INDEXES", 3);
    let streams = num("SPRIG_SFC_STREAMS", 8);
    let writers = num("SPRIG_SFC_WRITERS", 4);
    let txns = num("SPRIG_SFC_TXNS", 300);
    let docs = num("SPRIG_SFC_DOCS", 150);
    let expect_dumps = num("SPRIG_SFC_EXPECT_DUMPS", 2);
    let pools: Vec<Pool> = tenants
        .iter()
        .map(|t| pool(&gw, t, writers as usize + 2))
        .collect();

    // 1. Setup: tables, triggers, search indexes, collections.
    for (ti, t) in tenants.iter().enumerate() {
        let mut c = loop {
            if let Ok(c) = pools[ti].get_conn().await {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        c.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
        c.query_drop(format!("USE {DB}")).await.unwrap();
        c.query_drop("CREATE TABLE ord (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
            .await
            .unwrap();
        c.query_drop(
            "CREATE TABLE aud (seq BIGINT AUTO_INCREMENT PRIMARY KEY, trig INT NOT NULL, \
             id BIGINT NOT NULL)",
        )
        .await
        .unwrap();
        for j in 0..indexes {
            c.query_drop(format!(
                "CREATE TABLE s{j} (id BIGINT PRIMARY KEY, body TEXT NOT NULL)"
            ))
            .await
            .unwrap();
            c.query_drop(format!("CREATE SEARCH INDEX ix{j} ON s{j} (body TEXT)"))
                .await
                .unwrap();
        }
        for j in 0..triggers {
            c.query_drop(format!(
                "CREATE TRIGGER aud{j} AFTER COMMIT ON ord FOR INSERT LANGUAGE javascript AS $${}$$",
                trigger_code(j)
            ))
            .await
            .unwrap();
        }
        let client = Client::with_uri_str(mongo.replace("{tenant}", t))
            .await
            .unwrap();
        for k in 0..streams {
            client
                .database(DB)
                .create_collection(format!("c{k}"))
                .await
                .unwrap();
        }
        println!("{t}: {triggers} triggers, {indexes} search indexes, {streams} collections");
    }
    // Every trigger runs: probes (negative ids) until each has written one.
    let t0 = Instant::now();
    let mut probe = 0i64;
    for p in &pools {
        let mut c = conn(p).await;
        loop {
            probe -= 1;
            c.query_drop(format!("INSERT INTO ord VALUES ({probe}, 'probe')"))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
            let n: u64 = c
                .query_first("SELECT COUNT(DISTINCT trig) FROM aud WHERE id < 0")
                .await
                .unwrap()
                .unwrap_or(0);
            if n == triggers {
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(180),
                "triggers didn't start"
            );
        }
    }
    println!(
        "all triggers running after {:.1} s",
        t0.elapsed().as_secs_f64()
    );

    // The dump sampler.
    let stop_sampler = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (node, metrics, stop) = (node.clone(), metrics.clone(), Arc::clone(&stop_sampler));
        tokio::spawn(async move {
            let mut hist: BTreeMap<u64, u64> = BTreeMap::new();
            let mut tenant_kind_max = 0f64;
            let mut catchup_max = 0f64;
            while !stop.load(Ordering::Relaxed) {
                let n = dumps(&node).await;
                *hist.entry(n).or_default() += 1;
                for m in &metrics {
                    let g = dump_gauges(m).await;
                    tenant_kind_max = tenant_kind_max.max(*g.get("tenant").unwrap_or(&0.0));
                    catchup_max = catchup_max.max(*g.get("catchup").unwrap_or(&0.0));
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            (hist, tenant_kind_max, catchup_max)
        })
    };

    // 2. Change streams, opened before any document is written.
    let stop_streams = Arc::new(AtomicBool::new(false));
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel(1024);
    let mut followers = Vec::new();
    for t in &tenants {
        let uri = mongo.replace("{tenant}", t);
        let all: BTreeMap<String, usize> = (0..streams)
            .map(|k| (format!("c{k}"), docs as usize))
            .collect();
        for k in 0..streams {
            let want: BTreeMap<String, usize> = [(format!("c{k}"), docs as usize)].into();
            followers.push((
                format!("{t}.c{k}"),
                tokio::spawn(follow(
                    uri.clone(),
                    Some(format!("c{k}")),
                    want,
                    Arc::clone(&stop_streams),
                    ready_tx.clone(),
                )),
            ));
        }
        followers.push((
            format!("{t}.*"),
            tokio::spawn(follow(
                uri,
                None,
                all,
                Arc::clone(&stop_streams),
                ready_tx.clone(),
            )),
        ));
    }
    for _ in 0..followers.len() {
        tokio::time::timeout(Duration::from_secs(60), ready_rx.recv())
            .await
            .expect("change streams didn't open");
    }
    println!(
        "{} change streams open; {} dumps on the node",
        followers.len(),
        dumps(&node).await
    );

    // 3. Writers, with kills along the way.
    let done = Arc::new(AtomicU64::new(0));
    let total = txns * tenants.len() as u64;
    let mut tasks = Vec::new();
    for (ti, p) in pools.iter().enumerate() {
        for w in 0..writers {
            let (p, done) = (p.clone(), Arc::clone(&done));
            tasks.push(tokio::spawn(async move {
                let mut c = conn(&p).await;
                let mut committed = Vec::new();
                let mut rolled = Vec::new();
                let mut i = w;
                while i < txns {
                    let id = i as i64 + 1;
                    let rollback = id % 7 == 0;
                    let mut stmts = vec![
                        "BEGIN".to_owned(),
                        format!("INSERT INTO ord VALUES ({id}, 'row write {id}')"),
                    ];
                    for j in 0..indexes {
                        stmts.push(format!("INSERT INTO s{j} VALUES ({id}, 'row write {id}')"));
                    }
                    stmts.push(if rollback { "ROLLBACK" } else { "COMMIT" }.to_owned());
                    let mut ok = true;
                    for s in &stmts {
                        if c.query_drop(s).await.is_err() {
                            ok = false;
                            break;
                        }
                    }
                    if !ok {
                        // A lost connection (a gateway's kill): the row
                        // tells whether the commit happened; else again.
                        c = conn(&p).await;
                        let there: Option<i64> = c
                            .query_first(format!("SELECT id FROM ord WHERE id = {id}"))
                            .await
                            .unwrap_or(None);
                        if there.is_none() {
                            continue;
                        }
                        committed.push(id);
                    } else if rollback {
                        rolled.push(id);
                    } else {
                        committed.push(id);
                    }
                    done.fetch_add(1, Ordering::Relaxed);
                    i += writers;
                }
                (ti, committed, rolled)
            }));
        }
    }
    let mut doc_tasks = Vec::new();
    for t in &tenants {
        for k in 0..streams {
            let uri = mongo.replace("{tenant}", t);
            doc_tasks.push(tokio::spawn(async move {
                let mut i = 0i64;
                while i < docs as i64 {
                    let Ok(client) = Client::with_uri_str(&uri).await else {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        continue;
                    };
                    let coll: Collection<Document> =
                        client.database(DB).collection(&format!("c{k}"));
                    while i < docs as i64 {
                        match coll.insert_one(doc! {"_id": i, "seq": i}).await {
                            Ok(_) => i += 1,
                            Err(e) if e.to_string().contains("11000") => i += 1,
                            Err(_) => {
                                tokio::time::sleep(Duration::from_millis(300)).await;
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
            }));
        }
    }
    let worker_kill = var("SPRIG_SFC_WORKER_PIDFILE").zip(var("SPRIG_SFC_WORKER_START"));
    let gateway_kill = var("SPRIG_SFC_GATEWAY_PIDFILE").zip(var("SPRIG_SFC_GATEWAY_START"));
    let (mut wk, mut gk) = (worker_kill.is_none(), gateway_kill.is_none());
    let t1 = Instant::now();
    while tasks.iter().any(|t| !t.is_finished()) {
        let n = done.load(Ordering::Relaxed);
        if !wk && n >= total / 3 {
            wk = true;
            let (pf, cmd) = worker_kill.clone().unwrap();
            kill(&pf, "worker");
            start(&cmd);
        }
        if !gk && n >= total * 2 / 3 {
            gk = true;
            let (pf, cmd) = gateway_kill.clone().unwrap();
            kill(&pf, "gateway");
            start(&cmd);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut committed: Vec<BTreeSet<i64>> = vec![BTreeSet::new(); tenants.len()];
    let mut rolled: Vec<BTreeSet<i64>> = vec![BTreeSet::new(); tenants.len()];
    for t in tasks {
        let (ti, c, r) = t.await.unwrap();
        committed[ti].extend(c);
        rolled[ti].extend(r);
    }
    for t in doc_tasks {
        t.await.unwrap();
    }
    println!(
        "writes done in {:.1} s: {} transactions committed, {} rolled back, {} documents",
        t1.elapsed().as_secs_f64(),
        committed.iter().map(BTreeSet::len).sum::<usize>(),
        rolled.iter().map(BTreeSet::len).sum::<usize>(),
        docs * streams * tenants.len() as u64
    );

    // 4. Change streams: each document once, in order.
    let deadline = tokio::time::sleep(Duration::from_secs(120));
    tokio::pin!(deadline);
    let mut failures = Vec::new();
    let mut resumes = 0u64;
    for (name, f) in followers {
        let r = tokio::select! {
            r = f => r.unwrap(),
            () = &mut deadline => {
                stop_streams.store(true, Ordering::Relaxed);
                failures.push(format!("{name}: not complete in time"));
                continue;
            }
        };
        let (seen, n) = r;
        resumes += n;
        let colls: Vec<String> = if name.ends_with(".*") {
            (0..streams).map(|k| format!("c{k}")).collect()
        } else {
            vec![name.rsplit_once('.').unwrap().1.to_owned()]
        };
        for c in colls {
            let got = seen.get(&c).cloned().unwrap_or_default();
            let want: Vec<i64> = (0..docs as i64).collect();
            if got != want {
                failures.push(format!(
                    "{name} {c}: {} events, first differences {:?}",
                    got.len(),
                    got.iter()
                        .zip(&want)
                        .filter(|(a, b)| a != b)
                        .take(3)
                        .collect::<Vec<_>>()
                ));
            }
        }
    }
    println!("change streams checked ({resumes} resumes after breaks)");

    // 5. Triggers: each committed row once per trigger; rolled back none.
    let t2 = Instant::now();
    for (ti, t) in tenants.iter().enumerate() {
        let mut c = conn(&pools[ti]).await;
        let want = committed[ti].len() as u64 * triggers;
        loop {
            let n: u64 = c
                .query_first("SELECT COUNT(*) FROM aud WHERE id > 0")
                .await
                .unwrap()
                .unwrap_or(0);
            if n >= want || t2.elapsed() > Duration::from_secs(180) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let rows: Vec<(u64, i64, u64)> = c
            .query("SELECT trig, id, COUNT(*) FROM aud WHERE id > 0 GROUP BY trig, id")
            .await
            .unwrap();
        let twice = rows.iter().filter(|r| r.2 > 1).count();
        let ghost = rows
            .iter()
            .filter(|r| rolled[ti].contains(&r.1) || !committed[ti].contains(&r.1))
            .count();
        let mut missing = 0;
        for j in 0..triggers {
            let have: BTreeSet<i64> = rows.iter().filter(|r| r.0 == j).map(|r| r.1).collect();
            missing += committed[ti].difference(&have).count();
        }
        println!(
            "{t}: triggers ran {} of {want}, {twice} twice, {ghost} not committed, {missing} missing",
            rows.iter().map(|r| r.2).sum::<u64>()
        );
        if twice + ghost + missing > 0 {
            failures.push(format!(
                "{t}: triggers {twice} twice, {ghost} not committed, {missing} missing"
            ));
        }
        // 6. Search: each index holds exactly its table's rows.
        for j in 0..indexes {
            let mut last = Vec::new();
            let want: Vec<i64> = committed[ti].iter().copied().collect();
            for _ in 0..60 {
                let r: Result<Vec<i64>, _> = c
                    .query(format!(
                        "SELECT id FROM sprig_search('ix{j}', 'row write', \
                         JSON_OBJECT('limit', 100000, 'consistency', 'latest')) AS s ORDER BY id"
                    ))
                    .await;
                if let Ok(ids) = r {
                    last = ids;
                    if last == want {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            if last != want {
                failures.push(format!(
                    "{t}: ix{j} holds {} rows, the table {}",
                    last.len(),
                    want.len()
                ));
            }
        }
    }
    println!("triggers and search checked");

    // 7. Dumps.
    stop_sampler.store(true, Ordering::Relaxed);
    let (hist, tenant_kind_max, catchup_max) = sampler.await.unwrap();
    let max = hist.keys().max().copied().unwrap_or(0);
    println!(
        "Binlog Dump threads on the node per sample: {hist:?}; now {}; per process: \
         most catch-up dumps {catchup_max}, most per-tenant dumps {tenant_kind_max}",
        dumps(&node).await
    );
    if max > expect_dumps {
        failures.push(format!("{max} dumps on the node at once"));
    }
    if tenant_kind_max > 0.0 {
        failures.push(format!("{tenant_kind_max} per-tenant dumps in a process"));
    }
    // Clean up.
    for p in &pools {
        let mut c = conn(p).await;
        for j in 0..triggers {
            let _ = c.query_drop(format!("DROP TRIGGER IF EXISTS aud{j}")).await;
        }
        let _ = c.query_drop(format!("DROP DATABASE IF EXISTS {DB}")).await;
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
