//! A criterion-free micro benchmark of the rewrite (the gateway's
//! latency gate leaves the gateway well under 150 µs at p50; the rewrite's share
//! should stay well under 20 µs). Ignored by default; run with
//!
//! ```text
//! cargo test -p sprig-sql --release --test bench -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

mod common;

use std::hint::black_box;
use std::time::Instant;

use sprig_sql::names::{Engine, rewrite};

/// A typical ~200-byte OLTP SELECT with a qualified join, a hint, a
/// comment and a substitution.
const TYPICAL: &str = "SELECT /*+ MAX_EXECUTION_TIME(500) */ o.id, o.total, c.name, DATABASE() AS db \
FROM orders AS o JOIN crm.customers AS c ON c.id = o.customer_id \
WHERE o.created_at > NOW() - INTERVAL 1 DAY AND o.status = 'open' /* app */ ORDER BY o.id LIMIT 20";

fn measure(engine: Engine, sql: &str) -> f64 {
    let map = common::acme();
    let ctx = common::ctx(&map, common::mysql(), engine);
    for _ in 0..2_000 {
        black_box(rewrite(sql.as_bytes(), &ctx).unwrap());
    }
    let n = 20_000u32;
    let start = Instant::now();
    for _ in 0..n {
        black_box(rewrite(black_box(sql.as_bytes()), &ctx).unwrap());
    }
    start.elapsed().as_secs_f64() * 1e6 / f64::from(n)
}

#[test]
#[ignore = "micro benchmark; run in release with --ignored"]
fn rewrite_latency() {
    println!("statement: {} bytes", TYPICAL.len());
    for engine in [Engine::Lexer, Engine::Auto] {
        println!("{engine:?}: {:.2} µs per rewrite", measure(engine, TYPICAL));
    }
    let unqualified = "SELECT id, total FROM orders WHERE id = 42";
    println!(
        "Lexer, short unqualified: {:.2} µs",
        measure(Engine::Lexer, unqualified)
    );
    println!(
        "Auto, short unqualified: {:.2} µs",
        measure(Engine::Auto, unqualified)
    );
}
