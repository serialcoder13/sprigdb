//! The tuning scale test (chapter 09, build step 11; milestone M0.6): a
//! single-node cluster holding many shard schemas, read on uniformly
//! random tables with the default caches, then with the caches
//! [`sprig_cluster::tuning`] plans, reporting table-cache misses per
//! second, `Opened_tables`, read latency and `Memory_used`.
//!
//! Heavy: thousands of schemas on a shared dev server. Run it on its own
//! (holding the shared heavy-job lock), in release:
//!
//! ```sh
//! SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301 \
//!   cargo test -p sprig-cluster --release --test tuning_scale -- --ignored --nocapture
//! ```
//!
//! `SPRIG_SCALE_SCHEMAS` (10000), `SPRIG_SCALE_TABLES` per schema (1),
//! `SPRIG_SCALE_SECONDS` per measurement (20), `SPRIG_SCALE_THREADS` (8),
//! `SPRIG_SCALE_MEMORY_MB` the server's memory for the budget (1024),
//! `SPRIG_SCALE_BUDGET_PCT` (`tuning.memory_budget_pct`, 40 on the 1 GB dev
//! container), `SPRIG_SCALE_KEEP=1` keeps the schemas. Schemas are named
//! `placement_s_NNNNN` and dropped at the end; the cache variables are
//! restored to what they were.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines,
    clippy::similar_names
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_cluster::tuning::{
    Costs, ServerVar, Tuner, TuningInputs, TuningMode, TuningSettings, apply_on_node,
};

fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}

const PREFIX: &str = "placement_s_";

async fn status(pool: &Pool) -> BTreeMap<String, u64> {
    let mut c = pool.get_conn().await.unwrap();
    let rows: Vec<(String, String)> = c
        .query(
            "SHOW GLOBAL STATUS WHERE Variable_name IN ('Opened_tables','Open_tables', \
             'Table_open_cache_misses','Table_open_cache_hits','Opened_table_definitions', \
             'Open_table_definitions','Memory_used','Threads_running', \
             'Innodb_buffer_pool_reads','Innodb_buffer_pool_pages_dirty', \
             'Innodb_buffer_pool_pages_total','Table_open_cache_overflows')",
        )
        .await
        .unwrap();
    rows.into_iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.parse().unwrap_or(0)))
        .collect()
}

async fn vars(pool: &Pool) -> BTreeMap<String, u64> {
    let mut c = pool.get_conn().await.unwrap();
    let rows: Vec<(String, String)> = c
        .query(
            "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('table_open_cache', \
             'table_definition_cache','innodb_open_files','open_files_limit', \
             'stored_program_cache','max_connections','table_open_cache_instances')",
        )
        .await
        .unwrap();
    rows.into_iter()
        .map(|(k, v)| (k, v.parse().unwrap_or(0)))
        .collect()
}

struct Phase {
    name: &'static str,
    secs: f64,
    queries: u64,
    p50_us: u64,
    p99_us: u64,
    misses: u64,
    hits: u64,
    opened_tables: u64,
    opened_defs: u64,
    open_tables_end: u64,
    open_defs_end: u64,
    memory_used_end: u64,
    bp_reads: u64,
    peak_threads: u64,
}

/// Point reads on uniformly random tables for `secs` from `threads`.
async fn read_phase(
    pool: &Pool,
    name: &'static str,
    schemas: u64,
    tables: u64,
    threads: u64,
    secs: u64,
    seed: u64,
) -> Phase {
    let before = status(pool).await;
    let stop = Arc::new(AtomicBool::new(false));
    let count = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for t in 0..threads {
        let pool = pool.clone();
        let stop = Arc::clone(&stop);
        let count = Arc::clone(&count);
        handles.push(tokio::spawn(async move {
            let mut c = pool.get_conn().await.unwrap();
            let mut x = seed ^ (t + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let mut lat = Vec::with_capacity(200_000);
            while !stop.load(Ordering::Relaxed) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let s = x % schemas;
                let tb = (x >> 32) % tables;
                let q = format!("SELECT * FROM {PREFIX}{s:05}.t{tb} WHERE id = 1");
                let t0 = Instant::now();
                let _: Vec<mysql_async::Row> = c.query(q).await.unwrap();
                lat.push(t0.elapsed().as_micros() as u64);
                count.fetch_add(1, Ordering::Relaxed);
            }
            lat
        }));
    }
    // Sample Threads_running while it runs.
    let started = Instant::now();
    let mut peak = 0;
    while started.elapsed() < Duration::from_secs(secs) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let s = status(pool).await;
        peak = peak.max(s.get("threads_running").copied().unwrap_or(0));
    }
    stop.store(true, Ordering::Relaxed);
    let mut lat: Vec<u64> = Vec::new();
    for h in handles {
        lat.extend(h.await.unwrap());
    }
    let elapsed = started.elapsed().as_secs_f64();
    let after = status(pool).await;
    lat.sort_unstable();
    let pct = |p: f64| {
        lat.get(((lat.len() as f64 * p) as usize).min(lat.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0)
    };
    let d = |k: &str| {
        after
            .get(k)
            .copied()
            .unwrap_or(0)
            .saturating_sub(before.get(k).copied().unwrap_or(0))
    };
    Phase {
        name,
        secs: elapsed,
        queries: count.load(Ordering::Relaxed),
        p50_us: pct(0.50),
        p99_us: pct(0.99),
        misses: d("table_open_cache_misses"),
        hits: d("table_open_cache_hits"),
        opened_tables: d("opened_tables"),
        opened_defs: d("opened_table_definitions"),
        open_tables_end: after.get("open_tables").copied().unwrap_or(0),
        open_defs_end: after.get("open_table_definitions").copied().unwrap_or(0),
        memory_used_end: after.get("memory_used").copied().unwrap_or(0),
        bp_reads: d("innodb_buffer_pool_reads"),
        peak_threads: peak,
    }
}

fn print_phase(p: &Phase) {
    println!(
        "{:<16} {:>7.0} q/s  p50 {:>5} us  p99 {:>6} us | misses/s {:>8.1}  Opened_tables/s {:>8.1}  defs opened/s {:>7.1}  hit ratio {:>5.3} | Open_tables {:>6}  Open_defs {:>6}  Memory_used {:>5} MiB  bp reads/s {:>6.1}  peak Threads_running {}",
        p.name,
        p.queries as f64 / p.secs,
        p.p50_us,
        p.p99_us,
        p.misses as f64 / p.secs,
        p.opened_tables as f64 / p.secs,
        p.opened_defs as f64 / p.secs,
        p.hits as f64 / (p.hits + p.misses).max(1) as f64,
        p.open_tables_end,
        p.open_defs_end,
        p.memory_used_end >> 20,
        p.bp_reads as f64 / p.secs,
        p.peak_threads
    );
}

/// Runs `jobs` statements lists on `threads` connections.
async fn run_parallel(pool: &Pool, jobs: Vec<Vec<String>>, threads: usize) {
    let jobs = Arc::new(std::sync::Mutex::new(jobs));
    let mut hs = Vec::new();
    for _ in 0..threads {
        let pool = pool.clone();
        let jobs = Arc::clone(&jobs);
        hs.push(tokio::spawn(async move {
            let mut c = pool.get_conn().await.unwrap();
            loop {
                let next = jobs.lock().unwrap().pop();
                let Some(stmts) = next else { break };
                for s in stmts {
                    c.query_drop(s).await.unwrap();
                }
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
}

/// Waits until the dirty pages are back near where they were before the
/// mass DDL (`baseline`), or stop falling, at most 180 s: reads measured
/// while `InnoDB` flushes the DDL are slow for every table.
async fn wait_for_flush(pool: &Pool, baseline: u64) {
    let t0 = Instant::now();
    let mut last = u64::MAX;
    let mut flat = 0;
    loop {
        let s = status(pool).await;
        let dirty = s
            .get("innodb_buffer_pool_pages_dirty")
            .copied()
            .unwrap_or(0);
        let total = s
            .get("innodb_buffer_pool_pages_total")
            .copied()
            .unwrap_or(1);
        flat = if dirty + total / 200 >= last {
            flat + 1
        } else {
            0
        };
        last = dirty;
        if dirty <= baseline + total / 20 || flat >= 30 || t0.elapsed() > Duration::from_secs(180) {
            println!(
                "flush: {dirty} dirty of {total} pages (before: {baseline}) after {:.0} s",
                t0.elapsed().as_secs_f64()
            );
            return;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "heavy: thousands of schemas; run explicitly holding the heavy-job lock"]
async fn ten_thousand_schemas_without_table_cache_thrashing() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE");
        return;
    };
    let schemas = env_u64("SPRIG_SCALE_SCHEMAS", 10_000);
    let tables = env_u64("SPRIG_SCALE_TABLES", 1).max(1);
    let secs = env_u64("SPRIG_SCALE_SECONDS", 20);
    let threads = env_u64("SPRIG_SCALE_THREADS", 8);
    let memory = env_u64("SPRIG_SCALE_MEMORY_MB", 1024) << 20;
    let budget = u32::try_from(env_u64("SPRIG_SCALE_BUDGET_PCT", 40)).unwrap();
    let keep = std::env::var("SPRIG_SCALE_KEEP").is_ok_and(|v| v == "1");
    let opts = OptsBuilder::from_opts(Opts::from_url(&url).unwrap())
        .pool_opts(PoolOpts::default().with_constraints(PoolConstraints::new(1, 32).unwrap()));
    let pool = Pool::new(opts);
    let original = vars(&pool).await;
    let before = status(&pool).await;
    println!("server variables before: {original:?}");
    println!("status before: {before:?}");

    // 1. Create the schemas.
    let t0 = Instant::now();
    let mut jobs = Vec::new();
    for s in 0..schemas {
        let db = format!("{PREFIX}{s:05}");
        let mut v = vec![format!("CREATE DATABASE IF NOT EXISTS {db}")];
        for t in 0..tables {
            v.push(format!(
                "CREATE TABLE IF NOT EXISTS {db}.t{t} (id INT PRIMARY KEY, a INT, v VARCHAR(32), \
                 KEY ka (a)) ENGINE=InnoDB"
            ));
            v.push(format!(
                "INSERT IGNORE INTO {db}.t{t} VALUES (1, 1, 'x'), (2, 2, 'y')"
            ));
        }
        jobs.push(v);
    }
    run_parallel(&pool, jobs, 8).await;
    println!(
        "created {schemas} schemas x {tables} tables in {:.1} s",
        t0.elapsed().as_secs_f64()
    );
    wait_for_flush(
        &pool,
        before
            .get("innodb_buffer_pool_pages_dirty")
            .copied()
            .unwrap_or(0),
    )
    .await;

    // 2. The defaults (as the server had them), warm then measured.
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop("SET GLOBAL table_open_cache = 2000")
        .await
        .unwrap();
    c.query_drop("SET GLOBAL table_definition_cache = 400")
        .await
        .unwrap();
    drop(c);
    let _ = read_phase(&pool, "defaults warm", schemas, tables, threads, 5, 1).await;
    let defaults = read_phase(&pool, "defaults", schemas, tables, threads, secs, 2).await;

    // 3. The tuning pass: inputs from the server, one schema at a time.
    let t1 = Instant::now();
    let mut c = pool.get_conn().await.unwrap();
    let all: Vec<String> = c
        .query("SELECT schema_name FROM information_schema.SCHEMATA")
        .await
        .unwrap();
    let mut t_total = 0;
    let mut r_total = 0;
    let mut r_max = 0;
    for s in &all {
        let (t, r) = sprig_cluster::tuning::count_schema(&mut c, s)
            .await
            .unwrap();
        t_total += t;
        r_total += r;
        r_max = r_max.max(r);
    }
    let count_secs = t1.elapsed().as_secs_f64();
    let cur = vars(&pool).await;
    let current = BTreeMap::from([
        (
            ServerVar::TableDefinitionCache,
            cur["table_definition_cache"],
        ),
        (ServerVar::TableOpenCache, cur["table_open_cache"]),
        (ServerVar::InnodbOpenFiles, cur["innodb_open_files"]),
        (ServerVar::OpenFilesLimit, cur["open_files_limit"]),
        (ServerVar::StoredProgramCache, cur["stored_program_cache"]),
    ]);
    let inputs = TuningInputs {
        tables: t_total,
        routines: r_total,
        // Every table is read by the workload.
        tables_used: schemas * tables,
        peak_threads_running: defaults.peak_threads,
        max_connections: cur["max_connections"],
        max_routines_per_schema: r_max,
        memory_bytes: Some(memory),
    };
    let settings = TuningSettings {
        mode: TuningMode::Auto,
        memory_budget_pct: budget,
        ..TuningSettings::default()
    };
    let mut tuner = Tuner::new();
    let miss_rate = defaults.misses as f64 / defaults.secs;
    let plan = tuner.plan(
        &inputs,
        &current,
        &Costs::default(),
        &settings,
        true,
        miss_rate,
        0,
    );
    println!(
        "inputs: {inputs:?} (counted {} schemas in {count_secs:.1} s)",
        all.len()
    );
    println!(
        "plan: est. cache memory {} MiB, over budget {}; held {:?}",
        plan.mem_bytes >> 20,
        plan.over_budget,
        plan.held
    );
    for s in plan.statements() {
        println!("  {s}");
    }
    let applied = apply_on_node(&mut c, &plan, false).await.unwrap();
    tuner.applied(&plan, miss_rate, 0);
    println!("applied: {applied:?}");
    drop(c);

    // 4. Tuned: warm (the first pass opens every table once), measured.
    let _ = read_phase(&pool, "tuned warm", schemas, tables, threads, secs, 3).await;
    let tuned = read_phase(&pool, "tuned", schemas, tables, threads, secs, 4).await;

    println!();
    print_phase(&defaults);
    print_phase(&tuned);
    let dm = tuned.memory_used_end as f64 - defaults.memory_used_end as f64;
    let dt = tuned.open_tables_end as f64 - defaults.open_tables_end as f64;
    let dd = tuned.open_defs_end as f64 - defaults.open_defs_end as f64;
    println!(
        "Memory_used +{:.0} MiB for +{dt:.0} open tables and +{dd:.0} definitions: {:.1} KiB per table",
        dm / 1_048_576.0,
        dm / dt.max(1.0) / 1024.0
    );
    let cut = 1.0
        - (tuned.misses as f64 / tuned.secs) / (defaults.misses as f64 / defaults.secs).max(1e-9);
    println!("table-cache misses per second cut by {:.1}%", cut * 100.0);

    // 5. Restore and clean up.
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(format!(
        "SET GLOBAL table_open_cache = {}",
        original["table_open_cache"]
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "SET GLOBAL table_definition_cache = {}",
        original["table_definition_cache"]
    ))
    .await
    .unwrap();
    drop(c);
    // The defaults again, now that the creation's flush is long over: the
    // first measurement may still have paid for it.
    let _ = read_phase(&pool, "defaults again warm", schemas, tables, threads, 5, 5).await;
    let again = read_phase(&pool, "defaults again", schemas, tables, threads, secs, 6).await;
    print_phase(&again);
    if !keep {
        let t2 = Instant::now();
        let jobs = (0..schemas)
            .map(|s| vec![format!("DROP DATABASE IF EXISTS {PREFIX}{s:05}")])
            .collect();
        run_parallel(&pool, jobs, 8).await;
        println!(
            "dropped {schemas} schemas in {:.1} s",
            t2.elapsed().as_secs_f64()
        );
    }
    pool.disconnect().await.unwrap();
    // The check is meaningful only where the defaults thrash.
    if defaults.misses as f64 / defaults.secs > 100.0 {
        assert!(cut >= 0.9, "misses cut by only {:.1}%", cut * 100.0);
    }
}
