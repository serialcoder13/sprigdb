//! ID blocks across two gateways, live.
//!
//! Two `BlockAllocator`s with their own connection pools ("gateways") take
//! values for one sequence from 16 tasks each; every value must be unique,
//! each statement's values consecutive, and each gateway's values
//! increasing. On Galera the two gateways fetch blocks through different
//! nodes, so block fetches race through certification.
//!
//! Needs `SPRIG_TEST_MARIADB_SINGLE` (and `SPRIG_TEST_MARIADB_GALERA` for
//! the Galera half).

#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use std::collections::HashSet;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use sprig_shard::ids::{
    AutoIncSession, BlockAllocator, BlockConfig, CatalogSequences, IdAllocator, Ordering, SeqId,
};
use sprig_shard::lookup::direct::pool;

const SCHEMA: &str = "r1shard_ids";

async fn run(urls: [&str; 2], label: &str) {
    let setup = pool(urls[0], 2).unwrap();
    let mut c = setup.get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE IF EXISTS {SCHEMA}"))
        .await
        .unwrap();
    c.query_drop(format!("CREATE DATABASE {SCHEMA}"))
        .await
        .unwrap();
    c.query_drop(format!("USE {SCHEMA}")).await.unwrap();
    c.query_drop(sprig_shard::ids::DDL_SEQUENCES).await.unwrap();
    c.query_drop("CREATE TABLE items (id BIGINT PRIMARY KEY, gw INT) ENGINE=InnoDB")
        .await
        .unwrap();
    drop(c);
    let src0 = CatalogSequences::new(setup.clone(), SCHEMA);
    src0.create(SeqId(1), 100, Some("id"), 1, Ordering::Blocks)
        .await
        .unwrap();
    let cfg = BlockConfig {
        min: 16,
        max: 4096,
        target: Duration::from_secs(2),
    };
    let gws: Vec<_> = urls
        .iter()
        .map(|u| BlockAllocator::new(CatalogSequences::new(pool(u, 8).unwrap(), SCHEMA), cfg))
        .collect();
    let started = Instant::now();
    let mut tasks = Vec::new();
    for (g, a) in gws.iter().enumerate() {
        for t in 0..16u64 {
            let a = a.clone();
            let ins = pool(urls[g], 2).unwrap();
            tasks.push(tokio::spawn(async move {
                let s = AutoIncSession::default();
                let mut vals = Vec::new();
                let mut c = ins.get_conn().await.unwrap();
                for i in 0..300u64 {
                    let count = 1 + (i * 7 + t) % 5;
                    let run = a.take(SeqId(1), count, &s).await.unwrap();
                    let v: Vec<i128> = run.values().collect();
                    assert!(v.windows(2).all(|w| w[1] == w[0] + 1), "consecutive");
                    if let Some(last) = vals.last() {
                        assert!(v[0] > *last, "increasing per task");
                    }
                    // Every 10th statement really inserts its rows: the
                    // primary key must accept them.
                    if i % 10 == 0 {
                        let rows: Vec<String> = v.iter().map(|x| format!("({x},{g})")).collect();
                        c.query_drop(format!(
                            "INSERT INTO {SCHEMA}.items VALUES {}",
                            rows.join(",")
                        ))
                        .await
                        .unwrap();
                    }
                    vals.extend(v);
                }
                drop(c);
                ins.disconnect().await.unwrap();
                (g, vals)
            }));
        }
    }
    let mut all = HashSet::new();
    let mut total = 0usize;
    for t in tasks {
        let (_, vals) = t.await.unwrap();
        for v in vals {
            total += 1;
            assert!(all.insert(v), "{label}: value {v} handed out twice");
        }
    }
    let elapsed = started.elapsed();
    let blocks: u64 = gws.iter().map(BlockAllocator::blocks_fetched).sum();
    let max = *all.iter().max().unwrap();
    println!(
        "{label}: {total} values from 2 gateways x 16 tasks in {:.2}s, all unique; {blocks} blocks \
         fetched; highest value {max} ({} gap values)",
        elapsed.as_secs_f64(),
        max - i128::try_from(total).unwrap()
    );
    let mut c = setup.get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE {SCHEMA}"))
        .await
        .unwrap();
    drop(c);
    for g in gws {
        drop(g);
    }
    setup.disconnect().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn live_id_blocks_two_gateways() {
    let Ok(single) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE not set");
        return;
    };
    run([single.as_str(), single.as_str()], "single node").await;
    if let Ok(g) = std::env::var("SPRIG_TEST_MARIADB_GALERA") {
        let nodes: Vec<&str> = g.split(',').map(str::trim).collect();
        if nodes.len() >= 2 {
            run([nodes[0], nodes[1]], "galera (gateways on nodes 1 and 2)").await;
        }
    }
}
