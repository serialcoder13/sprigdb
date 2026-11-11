//! Full-text search through a running gateway (chapter 39, "Testing";
//! chapter 48's R5 done-when "search results are complete within
//! `search.max_lag_ms` and survive the loss of a worker").
//!
//! ```sh
//! deploy/compose/gateway-dev.sh up          # a gateway with the search service
//! export SPRIG_TEST_SEARCH=mysql://acme.app:sprig-dev-app@127.0.0.1:13307
//! cargo test -p sprig-search --test search_live -- --nocapture --test-threads 1
//! ```
//!
//! - `freshness`: rows written at a steady rate are found by a `stale_ok`
//!   search; every row's delay from its commit to its first hit is
//!   measured, and all must be within `search.max_lag_ms` (1000).
//! - `own_writes`: a search with `own_writes` right after each write finds it.
//! - `complete`: the index holds exactly the table's rows (also the check
//!   run after a gateway is killed and restarted, or the tenant moves:
//!   `SPRIG_TEST_SEARCH_KEEP=1` keeps the table for that).
//!
//! Database `r5s_live` is created and dropped (unless kept).

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, Pool};

const VAR: &str = "SPRIG_TEST_SEARCH";

fn pool() -> Option<Pool> {
    let url = std::env::var(VAR).ok()?;
    let opts = Opts::from_url(&url).expect("SPRIG_TEST_SEARCH must be a mysql:// URL");
    Some(Pool::new(
        mysql_async::OptsBuilder::from_opts(opts).pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, 4).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    ))
}

async fn setup(c: &mut Conn) {
    c.query_drop("CREATE DATABASE IF NOT EXISTS r5s_live")
        .await
        .unwrap();
    c.query_drop("USE r5s_live").await.unwrap();
    let exists: Option<u64> = c
        .query_first("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'r5s_live' AND TABLE_NAME = 'docs'")
        .await
        .unwrap();
    if exists.unwrap_or(0) == 0 {
        c.query_drop("CREATE TABLE docs (id BIGINT PRIMARY KEY, body TEXT, grp INT)")
            .await
            .unwrap();
    }
    c.query_drop("CREATE SEARCH INDEX IF NOT EXISTS docs_text ON docs (body TEXT, grp NUMBER WITH (filter = true))")
        .await
        .unwrap();
}

async fn hits(c: &mut Conn, q: &str, consistency: &str) -> u64 {
    c.query_first(format!(
        "SELECT COUNT(*) FROM sprig_search('docs_text', '{q}', JSON_OBJECT('limit', 10000, 'consistency', '{consistency}')) AS s"
    ))
    .await
    .unwrap()
    .unwrap_or(0)
}

fn pct(v: &[u128], p: f64) -> u128 {
    let mut s = v.to_vec();
    s.sort_unstable();
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let i = ((s.len() as f64 - 1.0) * p).round() as usize;
    s[i]
}

#[tokio::test]
async fn a_freshness() {
    let Some(pool) = pool() else {
        eprintln!("{VAR} not set; skipped");
        return;
    };
    let mut w = pool.get_conn().await.unwrap();
    setup(&mut w).await;
    let mut r = pool.get_conn().await.unwrap();
    r.query_drop("USE r5s_live").await.unwrap();
    let base: i64 = w
        .query_first("SELECT COALESCE(MAX(id), 0) + 1 FROM docs")
        .await
        .unwrap()
        .unwrap_or(1);
    let n = std::env::var("SPRIG_TEST_SEARCH_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200i64);
    let mut lags = Vec::new();
    for i in base..base + n {
        w.query_drop(format!(
            "INSERT INTO docs VALUES ({i}, 'fresh row token{i} alpha', {})",
            i % 7
        ))
        .await
        .unwrap();
        let t0 = Instant::now();
        loop {
            if hits(&mut r, &format!("token{i}"), "stale_ok").await == 1 {
                lags.push(t0.elapsed().as_millis());
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "row {i} not found in 10 s"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let over = lags.iter().filter(|l| **l > 1000).count();
    println!(
        "freshness: {} rows, lag p50 {} ms, p99 {} ms, max {} ms, over 1000 ms: {over}",
        lags.len(),
        pct(&lags, 0.5),
        pct(&lags, 0.99),
        lags.iter().max().unwrap()
    );
    assert_eq!(over, 0, "rows found later than search.max_lag_ms");
}

#[tokio::test]
async fn b_own_writes() {
    let Some(pool) = pool() else {
        eprintln!("{VAR} not set; skipped");
        return;
    };
    let mut c = pool.get_conn().await.unwrap();
    setup(&mut c).await;
    let base: i64 = c
        .query_first("SELECT COALESCE(MAX(id), 0) + 1 FROM docs")
        .await
        .unwrap()
        .unwrap_or(1);
    let mut misses = 0;
    let mut waits = Vec::new();
    for i in base..base + 100 {
        c.query_drop(format!(
            "INSERT INTO docs VALUES ({i}, 'own write mine{i}', 1)"
        ))
        .await
        .unwrap();
        let t0 = Instant::now();
        if hits(&mut c, &format!("mine{i}"), "own_writes").await != 1 {
            misses += 1;
        }
        waits.push(t0.elapsed().as_millis());
        // MATCH in the same session waits the same way when converted
        // from FULLTEXT; here the plain search index takes the option.
    }
    println!(
        "own_writes: 100 writes, {misses} missed, wait p50 {} ms, p99 {} ms",
        pct(&waits, 0.5),
        pct(&waits, 0.99)
    );
    assert_eq!(misses, 0);
}

#[tokio::test]
async fn c_complete() {
    let Some(pool) = pool() else {
        eprintln!("{VAR} not set; skipped");
        return;
    };
    let mut c = pool.get_conn().await.unwrap();
    setup(&mut c).await;
    // Every row has "row" or "write"; the search must return exactly the
    // table's ids.
    let want: Vec<i64> = c.query("SELECT id FROM docs ORDER BY id").await.unwrap();
    let got: Vec<i64> = c
        .query(
            "SELECT id FROM sprig_search('docs_text', 'row write', JSON_OBJECT('limit', 100000, 'consistency', 'latest')) AS s ORDER BY id",
        )
        .await
        .unwrap();
    let matched: u64 = c
        .query_first("SELECT COUNT(*) FROM docs WHERE MATCH(body) AGAINST ('row write')")
        .await
        .unwrap()
        .unwrap_or(0);
    println!(
        "complete: table {} rows, search {} hits, MATCH {matched}",
        want.len(),
        got.len()
    );
    assert_eq!(want, got);
    if std::env::var("SPRIG_TEST_SEARCH_KEEP").is_err() {
        c.query_drop("DROP DATABASE r5s_live").await.unwrap();
    }
}

/// R5 round 6: copies on workers survive the loss of one. With
/// `SPRIG_TEST_SEARCH_KILL` (a shell command that kills, with `kill -9`, a
/// search worker holding a copy of `docs_text`), rows are written while
/// searches run before, during and after the kill: every search answers
/// (the other copy serves), every row is found within
/// `search.max_lag_ms`, and once the lost copy is placed again (seeded
/// from its surviving peer) the index still equals the table.
#[tokio::test]
async fn d_worker_loss() {
    let (Some(pool), Ok(kill)) = (pool(), std::env::var("SPRIG_TEST_SEARCH_KILL")) else {
        eprintln!("{VAR} or SPRIG_TEST_SEARCH_KILL not set; skipped");
        return;
    };
    let mut c = pool.get_conn().await.unwrap();
    setup(&mut c).await;
    let ready = |s: &str| s.starts_with("ready on") && s.contains(',');
    let state = async |c: &mut Conn| -> String {
        let rows: Vec<mysql_async::Row> = c
            .query("SHOW SEARCH INDEXES FROM docs")
            .await
            .unwrap();
        rows.first()
            .and_then(|r| r.get::<String, _>("State"))
            .unwrap_or_default()
    };
    // Both copies ready.
    let t0 = Instant::now();
    loop {
        let s = state(&mut c).await;
        if ready(&s) {
            println!("worker loss: before: {s}");
            break;
        }
        assert!(t0.elapsed() < Duration::from_secs(120), "copies not ready: {s}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let base: i64 = c
        .query_first("SELECT COALESCE(MAX(id), 0) + 1 FROM docs")
        .await
        .unwrap()
        .unwrap_or(1);
    let mut r = pool.get_conn().await.unwrap();
    r.query_drop("USE r5s_live").await.unwrap();
    let mut lags = Vec::new();
    let mut failed = 0u32;
    let mut killed_at = None;
    for i in base..base + 150 {
        if i == base + 30 {
            let out = std::process::Command::new("sh")
                .arg("-c")
                .arg(&kill)
                .output()
                .unwrap();
            assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
            killed_at = Some(Instant::now());
            println!("worker loss: killed ({})", String::from_utf8_lossy(&out.stdout).trim());
        }
        c.query_drop(format!(
            "INSERT INTO docs VALUES ({i}, 'loss row token{i} alpha', {})",
            i % 7
        ))
        .await
        .unwrap();
        let t = Instant::now();
        loop {
            match r
                .query_first::<u64, _>(format!(
                    "SELECT COUNT(*) FROM sprig_search('docs_text', 'token{i}', JSON_OBJECT('limit', 10, 'consistency', 'stale_ok')) AS s"
                ))
                .await
            {
                Ok(Some(1)) => {
                    lags.push(t.elapsed().as_millis());
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    failed += 1;
                    eprintln!("search failed: {e}");
                }
            }
            assert!(t.elapsed() < Duration::from_secs(10), "row {i} not found in 10 s");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let over = lags.iter().filter(|l| **l > 1000).count();
    println!(
        "worker loss: {} rows written around the kill, lag p50 {} ms, p99 {} ms, max {} ms, over 1000 ms: {over}, failed searches: {failed}",
        lags.len(),
        pct(&lags, 0.5),
        pct(&lags, 0.99),
        lags.iter().max().unwrap()
    );
    assert_eq!(failed, 0, "searches failed while a copy was lost");
    assert_eq!(over, 0, "rows found later than search.max_lag_ms");
    // The lost copy is placed again and seeded.
    let t1 = Instant::now();
    loop {
        let s = state(&mut c).await;
        if ready(&s) {
            println!(
                "worker loss: two copies again {} ms after the kill: {s}",
                killed_at.unwrap().elapsed().as_millis()
            );
            break;
        }
        assert!(t1.elapsed() < Duration::from_secs(180), "the lost copy wasn't replaced: {s}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let want: Vec<i64> = c.query("SELECT id FROM docs ORDER BY id").await.unwrap();
    let got: Vec<i64> = c
        .query(
            "SELECT id FROM sprig_search('docs_text', 'row write', JSON_OBJECT('limit', 100000, 'consistency', 'latest')) AS s ORDER BY id",
        )
        .await
        .unwrap();
    println!("worker loss: table {} rows, search {} hits", want.len(), got.len());
    assert_eq!(want, got);
}
