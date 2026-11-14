//! MongoDB point reads (`find` by `_id`) through the executor's point-read
//! path, live: what makes the path fall back to the general one.
//! Each case reads the same documents several times first, so the
//! session's point-read state is warm, then changes something under it
//! and reads again on the same connection:
//!
//! - **revoked privilege:** `REVOKE SELECT` makes the next reads fail
//!   with Unauthorized (13); `GRANT` gives them back;
//! - **row-level security:** a policy enabled on the collection hides
//!   another owner's document from the next point read (a session that
//!   logs in after it reads its own documents through the point path);
//!   disabling it shows it again;
//! - **catalog version:** a collection dropped and created again answers
//!   with the new document, and a dropped one with none;
//! - **move** (with `SPRIG_TEST_MOVE_CMDS`): the tenant moved to another
//!   cluster and back while a client reads in a loop; every read answers
//!   the document.
//!
//! ```sh
//! export SPRIG_TEST_POINT_SQL=127.0.0.1:43306
//! export SPRIG_TEST_POINT_MONGO=127.0.0.1:43737
//! export SPRIG_TEST_POINT_TENANT=acme
//! # optional, as in sprig-proto-mongo's move_live.rs:
//! export SPRIG_TEST_MOVE_CMDS='sprigctl shard move acme --to dc-a-c2 --wait;;sprigctl shard move acme --to dc-a-c1 --wait'
//! cargo test -p sprig-gateway --test mongo_point_live -- --nocapture --test-threads 1
//! ```
//!
//! The tests use database `r5perf2_pt` and users `r5perf2_carol` and
//! `r5perf2_dave`, and drop them at the end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::time::{Duration, Instant};

use mongodb::bson::{Document, doc};
use mysql_async::prelude::Queryable;

const DB: &str = "r5perf2_pt";
const PW: &str = "R5perf2-pt-pw-1";

fn env() -> Option<(String, String)> {
    Some((
        std::env::var("SPRIG_TEST_POINT_SQL").ok()?,
        std::env::var("SPRIG_TEST_POINT_MONGO").ok()?,
    ))
}

fn tenant() -> String {
    std::env::var("SPRIG_TEST_POINT_TENANT").unwrap_or_else(|_| "acme".into())
}

async fn client(addr: &str, user: &str, pw: &str) -> mongodb::Client {
    let uri = format!(
        "mongodb://{}.{user}:{pw}@{addr}/?authSource=admin&directConnection=true&maxPoolSize=1",
        tenant()
    );
    mongodb::Client::with_uri_str(&uri).await.unwrap()
}

async fn sql(addr: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.app", tenant())))
        .pass(Some("sprig-dev-app"))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        _ => -1,
    }
}

type Read = Result<Option<Document>, i32>;

async fn read(c: &mongodb::Collection<Document>, id: i64) -> Read {
    c.find_one(doc! {"_id": id}).await.map_err(|e| code_of(&e))
}

/// Reads `id` until `want` holds or `secs` pass (a change reaches the
/// gateway through the catalog's watch); the last answer.
async fn read_until(
    c: &mongodb::Collection<Document>,
    id: i64,
    secs: u64,
    want: impl Fn(&Read) -> bool,
) -> Read {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let r = read(c, id).await;
        if want(&r) || Instant::now() > deadline {
            return r;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn n_of(r: &Read) -> Option<i64> {
    r.as_ref().ok()?.as_ref()?.get_i64("n").ok()
}

/// Warms the session's point reads of `ids`: each read three times.
async fn warm(c: &mongodb::Collection<Document>, ids: &[i64]) {
    for _ in 0..3 {
        for id in ids {
            let r = read(c, *id).await;
            assert!(r.is_ok(), "warm-up read of {id}: {r:?}");
        }
    }
}

async fn setup(sql_addr: &str, mongo_addr: &str) -> (mysql_async::Conn, mongodb::Client) {
    let mut admin_sql = sql(sql_addr).await;
    for u in ["r5perf2_carol", "r5perf2_dave"] {
        let _ = admin_sql
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }
    let _ = admin_sql
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await;
    let admin = client(mongo_addr, "app", "sprig-dev-app").await;
    let docs: Vec<Document> = (1..=10i64)
        .map(|i| {
            let owner = if i % 2 == 0 {
                "r5perf2_carol@%"
            } else {
                "r5perf2_dave@%"
            };
            doc! {"_id": i, "owner": owner, "n": i}
        })
        .collect();
    admin
        .database(DB)
        .collection::<Document>("pts")
        .insert_many(docs)
        .await
        .unwrap();
    for u in ["r5perf2_carol", "r5perf2_dave"] {
        admin_sql
            .query_drop(format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'"))
            .await
            .unwrap();
        admin_sql
            .query_drop(format!("GRANT SELECT ON {DB}.* TO '{u}'@'%'"))
            .await
            .unwrap();
    }
    (admin_sql, admin)
}

async fn teardown(mut admin_sql: mysql_async::Conn) {
    for u in ["r5perf2_carol", "r5perf2_dave"] {
        let _ = admin_sql
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }
    let _ = admin_sql
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn point_reads_fall_back_when_something_changes() {
    let Some((sql_addr, mongo_addr)) = env() else {
        eprintln!("skipped: set SPRIG_TEST_POINT_SQL and SPRIG_TEST_POINT_MONGO");
        return;
    };
    let (mut admin_sql, admin) = setup(&sql_addr, &mongo_addr).await;
    let mut failed: Vec<String> = Vec::new();
    let mut check = |what: &str, ok: bool, detail: String| {
        if !ok {
            eprintln!("FAIL {what}: {detail}");
            failed.push(format!("{what}: {detail}"));
        }
    };

    // Revoked privilege.
    let carol = client(&mongo_addr, "r5perf2_carol", PW).await;
    let cc = carol.database(DB).collection::<Document>("pts");
    warm(&cc, &[1, 2]).await;
    admin_sql
        .query_drop(format!("REVOKE SELECT ON {DB}.* FROM 'r5perf2_carol'@'%'"))
        .await
        .unwrap();
    let r = read_until(&cc, 2, 10, Result::is_err).await;
    check("revoked: read refused", r == Err(13), format!("{r:?}"));
    // A refused read stays refused (the point path never answers it).
    for _ in 0..5 {
        let r = read(&cc, 2).await;
        check("revoked: still refused", r == Err(13), format!("{r:?}"));
    }
    admin_sql
        .query_drop(format!("GRANT SELECT ON {DB}.* TO 'r5perf2_carol'@'%'"))
        .await
        .unwrap();
    let r = read_until(&cc, 2, 10, |r| n_of(r) == Some(2)).await;
    check("granted again: read", n_of(&r) == Some(2), format!("{r:?}"));

    // Row-level security enabled under a warm session.
    let dave = client(&mongo_addr, "r5perf2_dave", PW).await;
    let dc = dave.database(DB).collection::<Document>("pts");
    warm(&dc, &[3, 4]).await;
    let adb = admin.database(DB);
    let r = adb
        .run_command(doc! {"sprigCreatePolicy": "pts", "name": "own",
        "using": {"owner": "$$USER"}, "check": {"owner": "$$USER"}})
        .await;
    check("policy created", r.is_ok(), format!("{r:?}"));
    let r = adb
        .run_command(doc! {"sprigRowSecurity": "pts", "enable": true})
        .await;
    check("row security enabled", r.is_ok(), format!("{r:?}"));
    // A session whose point reads were prepared before the policy never
    // sees the other owner's document again. (Its prepared reads are
    // refused with 39366 instead of being prepared again under the policy,
    // on the general path too, and before the point path existed: a known
    // gap. A plain `find` of the same session is filtered.)
    let hidden_or_refused = |r: &Read| matches!(r, Ok(None) | Err(39366));
    let r = read_until(&dc, 4, 10, hidden_or_refused).await;
    check(
        "policy: another owner's document not read",
        hidden_or_refused(&r),
        format!("{r:?}"),
    );
    for _ in 0..5 {
        let r = read(&dc, 4).await;
        check(
            "policy: still not read",
            hidden_or_refused(&r),
            format!("{r:?}"),
        );
        let r = read(&dc, 3).await;
        check(
            "policy: own document read or refused",
            n_of(&r) == Some(3) || r == Err(39366),
            format!("{r:?}"),
        );
    }
    // A session that logs in after the policy: hidden from the first read.
    let dave2 = client(&mongo_addr, "r5perf2_dave", PW).await;
    let dc2 = dave2.database(DB).collection::<Document>("pts");
    for _ in 0..5 {
        let r = read(&dc2, 6).await;
        check(
            "policy, new session: hidden",
            matches!(r, Ok(None)),
            format!("{r:?}"),
        );
        let r = read(&dc2, 5).await;
        check(
            "policy, new session: own document read",
            n_of(&r) == Some(5),
            format!("{r:?}"),
        );
    }
    let r = adb
        .run_command(doc! {"sprigRowSecurity": "pts", "enable": false})
        .await;
    check("row security disabled", r.is_ok(), format!("{r:?}"));
    let r = read_until(&dc, 4, 10, |r| n_of(r) == Some(4)).await;
    check(
        "policy off: document read again",
        n_of(&r) == Some(4),
        format!("{r:?}"),
    );
    let r = adb
        .run_command(doc! {"sprigDropPolicy": "pts", "name": "own"})
        .await;
    check("policy dropped", r.is_ok(), format!("{r:?}"));

    // Catalog version: the collection dropped and created again.
    warm(&cc, &[2, 4]).await;
    let coll = adb.collection::<Document>("pts");
    coll.drop().await.unwrap();
    let r = read_until(&cc, 2, 10, |r| matches!(r, Ok(None))).await;
    check(
        "dropped: no document",
        matches!(r, Ok(None)),
        format!("{r:?}"),
    );
    coll.insert_one(doc! {"_id": 2i64, "owner": "x", "n": 200i64})
        .await
        .unwrap();
    let r = read_until(&cc, 2, 10, |r| n_of(r) == Some(200)).await;
    check(
        "created again: the new document",
        n_of(&r) == Some(200),
        format!("{r:?}"),
    );
    for _ in 0..5 {
        let r = read(&cc, 2).await;
        check(
            "created again: stays new",
            n_of(&r) == Some(200),
            format!("{r:?}"),
        );
    }

    drop((carol, dave, dave2, admin));
    teardown(admin_sql).await;
    assert!(
        failed.is_empty(),
        "{} checks failed: {failed:#?}",
        failed.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_reads_follow_a_move() {
    let Some((sql_addr, mongo_addr)) = env() else {
        eprintln!("skipped: set SPRIG_TEST_POINT_SQL and SPRIG_TEST_POINT_MONGO");
        return;
    };
    let Some(cmds) = std::env::var("SPRIG_TEST_MOVE_CMDS")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("skipped: set SPRIG_TEST_MOVE_CMDS");
        return;
    };
    let (admin_sql, admin) = setup(&sql_addr, &mongo_addr).await;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut readers = Vec::new();
    for r in 0..4i64 {
        let c = client(&mongo_addr, "app", "sprig-dev-app").await;
        let stop = std::sync::Arc::clone(&stop);
        readers.push(tokio::spawn(async move {
            let coll = c.database(DB).collection::<Document>("pts");
            let (mut ok, mut bad) = (0u64, Vec::new());
            let mut i = 0i64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let id = (i + r) % 10 + 1;
                i += 1;
                let got = read(&coll, id).await;
                if n_of(&got) == Some(id) {
                    ok += 1;
                } else if bad.len() < 20 {
                    bad.push(format!("{id}: {got:?}"));
                }
            }
            (ok, bad)
        }));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    for cmd in cmds.split(";;") {
        let started = Instant::now();
        // A move's job holds the tenant for a while after its cutover
        // (39131): the next move waits for it.
        loop {
            let c = cmd.to_owned();
            let out = tokio::task::spawn_blocking(move || {
                std::process::Command::new("sh").arg("-c").arg(c).output()
            })
            .await
            .unwrap()
            .unwrap();
            let err = String::from_utf8_lossy(&out.stderr).into_owned();
            if !out.status.success()
                && err.contains("39131")
                && started.elapsed() < Duration::from_secs(300)
            {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
            eprintln!("{cmd}: {} in {:?}", out.status, started.elapsed());
            assert!(out.status.success(), "{cmd}: {err}");
            break;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut total = 0;
    let mut bad_all = Vec::new();
    for h in readers {
        let (ok, bad) = h.await.unwrap();
        total += ok;
        bad_all.extend(bad);
    }
    eprintln!("{total} point reads answered during the moves");
    drop(admin);
    teardown(admin_sql).await;
    assert!(total > 0, "no reads");
    assert!(bad_all.is_empty(), "wrong answers: {bad_all:#?}");
}
