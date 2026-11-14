//! Overhead measurements against the overhead budget: 2 µs per statement
//! to normalize and hash, texts up to 4 KB; 0.5 µs to record. Run in
//! release:
//!
//! ```text
//! cargo test --release -p sprig-workload --test perf -- --ignored --nocapture
//! ```

#![allow(clippy::many_single_char_names, clippy::cast_possible_truncation)]

use std::hint::black_box;
use std::time::Instant;

use sprig_common::ids::{DbId, TenantId};
use sprig_proto_mysql::packets::Persona;
use sprig_workload::fingerprint::{FpKind, SqlCtx, SqlFingerprinter, digest, normalize};
use sprig_workload::shape::NoResolver;

const STATEMENTS: &[(&str, &str)] = &[
    (
        "sysbench point select (36 B)",
        "SELECT c FROM sbtest1 WHERE id=5012",
    ),
    (
        "orders example (183 B)",
        "SELECT /* web:42 */ o.id, o.Total FROM `orders` o WHERE o.customer_id = 42 AND o.status IN ('open','hold') ORDER BY o.created_at DESC LIMIT 20",
    ),
    (
        "ORM select with hint and join (300 B)",
        "SELECT /*+ MAX_EXECUTION_TIME(1000) */ `users`.`id`, `users`.`email`, `users`.`created_at`, `profiles`.`bio` FROM `users` INNER JOIN `profiles` ON `profiles`.`user_id` = `users`.`id` WHERE `users`.`tenant_id` = 17 AND `users`.`deleted_at` IS NULL AND `users`.`email` LIKE 'kiran%' ORDER BY `users`.`id` DESC LIMIT 25 OFFSET 50",
    ),
    (
        "sysbench insert (180 B)",
        "INSERT INTO sbtest1 (id, k, c, pad) VALUES (0, 50123, '83868641912-28773972837-60736120486-75162659906-27563526494-20381887404-41576422241-93426793964-56405065102-33518432330', '67847967377-48000963322-62604785301-91415491898-96926520291')",
    ),
];

fn big_statement() -> String {
    let mut s = String::from("SELECT a, b, c FROM t WHERE x IN (");
    for i in 0..600 {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&i.to_string());
    }
    s.push_str(") AND y = 'z' ORDER BY a");
    s
}

fn time<F: FnMut()>(iters: u32, mut f: F) -> f64 {
    for _ in 0..iters / 10 {
        f();
    }
    let t = Instant::now();
    for _ in 0..iters {
        f();
    }
    t.elapsed().as_secs_f64() * 1e6 / f64::from(iters)
}

#[test]
#[ignore = "measurement; run in release"]
fn fingerprint_overhead() {
    let ctx = SqlCtx::new(Persona::MySql84).current_db("shop", DbId(1));
    let fp = SqlFingerprinter::default();
    let big = big_statement();
    let mut all: Vec<(&str, &str)> = STATEMENTS.to_vec();
    all.push(("IN list of 600 (3.0 KB)", &big));
    println!(
        "{:<40} {:>10} {:>10} {:>10} {:>12}",
        "statement", "lex only", "normalize", "+digest", "fingerprint"
    );
    for (name, sql) in all {
        let l = time(20_000, || {
            black_box(sprig_sql::lex::lex(black_box(sql.as_bytes()), &ctx.lex).ok());
        });
        let n = time(20_000, || {
            black_box(normalize(
                black_box(sql.as_bytes()),
                &ctx.lex,
                Some("shop"),
                false,
            ));
        });
        let d = time(20_000, || {
            let x = normalize(black_box(sql.as_bytes()), &ctx.lex, Some("shop"), false);
            black_box(digest(FpKind::Sql, DbId(1), None, &x.text));
        });
        let f = time(20_000, || {
            black_box(fp.sql(TenantId(42), black_box(sql.as_bytes()), &ctx, &NoResolver));
        });
        println!("{name:<40} {l:>8.2}µs {n:>8.2}µs {d:>8.2}µs {f:>10.2}µs");
    }
}

#[test]
#[ignore = "measurement; run in release"]
fn record_overhead() {
    use sprig_common::time::SystemClock;
    use sprig_workload::recorder::{ExecSummary, Recorder, RecorderConfig};
    use std::sync::Arc;

    let ctx = SqlCtx::new(Persona::MySql84).current_db("shop", DbId(1));
    let f = SqlFingerprinter::default();
    let r = Arc::new(Recorder::new(
        RecorderConfig::default(),
        Arc::new(SystemClock),
    ));
    // 1,000 fingerprints over 50 tenants already known.
    let fps: Vec<_> = (0..1000)
        .map(|i| {
            f.sql(
                TenantId(i % 50),
                format!("SELECT c{i} FROM t WHERE id = 1").as_bytes(),
                &ctx,
                &NoResolver,
            )
        })
        .collect();
    let ex = ExecSummary {
        latency_us: 850,
        rows_returned: 1,
        shards: 1,
        clusters: 1,
        ..ExecSummary::default()
    };
    for (i, fp) in fps.iter().enumerate() {
        r.record(TenantId(i as u64 % 50), fp, &ex);
    }
    let mut k = 0usize;
    let one = time(1_000_000, || {
        k = (k + 1) % fps.len();
        r.record(TenantId(k as u64 % 50), black_box(&fps[k]), black_box(&ex));
    });
    let mut k = 0usize;
    let plan = time(1_000_000, || {
        k = (k + 1) % fps.len();
        black_box(r.plan(TenantId(k as u64 % 50), &fps[k]));
    });
    let fps = Arc::new(fps);
    let mut multi = Vec::new();
    for threads in [1usize, 2, 4, 8] {
        let t = std::time::Instant::now();
        let hs: Vec<_> = (0..threads)
            .map(|n| {
                let (r, fps) = (Arc::clone(&r), Arc::clone(&fps));
                std::thread::spawn(move || {
                    let mut x = 0x9e37_79b9_7f4a_7c15u64 ^ (n as u64 + 1);
                    for _ in 0..500_000usize {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        let k = (x % fps.len() as u64) as usize;
                        r.record(TenantId(k as u64 % 50), &fps[k], &ex);
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        let per = t.elapsed().as_secs_f64() * 1e6 / 500_000.0;
        multi.push(format!("{threads} threads {per:.3} µs"));
    }
    println!(
        "record: {one:.3} µs single thread; per record, wall time per thread: {}; plan: {plan:.3} µs",
        multi.join(", ")
    );
}
