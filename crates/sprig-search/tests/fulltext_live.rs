//! MySQL `FULLTEXT` habits on search indexes (chapter 39, R5 round 6):
//! `FULLTEXT KEY` inside `CREATE TABLE` becomes a search index, and
//! `MATCH ... AGAINST (?)` works in server-side prepared statements.
//!
//! ```sh
//! export SPRIG_TEST_SEARCH=mysql://acme.app:sprig-dev-app@127.0.0.1:23307
//! cargo test -p sprig-search --test fulltext_live -- --nocapture
//! ```
//!
//! Database `r5six_ft` is created and dropped.

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, Pool};

fn pool() -> Option<Pool> {
    let url = std::env::var("SPRIG_TEST_SEARCH").ok()?;
    let opts = Opts::from_url(&url).expect("SPRIG_TEST_SEARCH must be a mysql:// URL");
    Some(Pool::new(
        mysql_async::OptsBuilder::from_opts(opts).pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, 2).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    ))
}

#[tokio::test]
async fn fulltext_key_and_prepared_against() {
    let Some(pool) = pool() else {
        eprintln!("SPRIG_TEST_SEARCH not set; skipped");
        return;
    };
    let mut c = pool.get_conn().await.unwrap();
    for s in [
        "DROP DATABASE IF EXISTS r5six_ft",
        "CREATE DATABASE r5six_ft",
        "USE r5six_ft",
        // A dump's table: the FULLTEXT key becomes a search index.
        "CREATE TABLE articles (\n  id INT PRIMARY KEY,\n  title VARCHAR(200),\n  body TEXT,\n  \
         FULLTEXT KEY ft_title_body (title, body),\n  KEY by_title (title)\n) ENGINE=InnoDB",
        "INSERT INTO articles VALUES (1, 'MySQL tutorial', 'DBMS stands for DataBase'), \
         (2, 'How to use MySQL well', 'After you went through a tutorial'), \
         (3, 'Optimizing MySQL', 'In this tutorial we show'), \
         (4, 'Comparing databases', 'Postgres versus MariaDB'), \
         (5, 'Security', 'When configured properly, MySQL is safe')",
    ] {
        c.query_drop(s).await.unwrap();
    }
    let idx: Vec<mysql_async::Row> = c.query("SHOW SEARCH INDEXES FROM articles").await.unwrap();
    let names: Vec<String> = idx
        .iter()
        .filter_map(|r| r.get::<String, _>("Index"))
        .collect();
    assert!(
        names.iter().any(|n| n == "ft_title_body"),
        "the FULLTEXT key is a search index: {names:?}"
    );
    let flavor: Option<String> = idx.first().and_then(|r| r.get("Flavor"));
    assert_eq!(flavor.as_deref(), Some("fulltext_compat"));
    let keys: Vec<String> = c
        .query("SELECT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = 'r5six_ft' AND TABLE_NAME = 'articles'")
        .await
        .unwrap();
    assert!(keys.iter().any(|k| k == "by_title"), "{keys:?}");
    assert!(!keys.iter().any(|k| k == "ft_title_body"), "no MariaDB FULLTEXT index: {keys:?}");
    // Server-side prepared statements (mysql_async's exec prepares).
    let mut got: Vec<i32> = c
        .exec(
            "SELECT id FROM articles WHERE MATCH (title, body) AGAINST (? IN BOOLEAN MODE) ORDER BY id",
            ("+tutorial -optimizing",),
        )
        .await
        .unwrap();
    assert_eq!(got, vec![1, 2]);
    got = c
        .exec(
            "SELECT id FROM articles WHERE MATCH (title, body) AGAINST (?) AND id > ? ORDER BY id",
            ("tutorial", 1),
        )
        .await
        .unwrap();
    assert_eq!(got, vec![2, 3]);
    let scored: Vec<(i32, f64)> = c
        .exec(
            "SELECT id, MATCH (title, body) AGAINST (?) AS s FROM articles WHERE MATCH (title, body) AGAINST (?) ORDER BY s DESC, id",
            ("mysql", "mysql"),
        )
        .await
        .unwrap();
    assert_eq!(scored.len(), 4, "{scored:?}");
    assert!(scored.iter().all(|(_, s)| *s > 0.0), "{scored:?}");
    // A prepared statement runs again with new values.
    let stmt = c
        .prep("SELECT COUNT(*) FROM articles WHERE MATCH (title, body) AGAINST (?)")
        .await
        .unwrap();
    let n1: Option<i64> = c.exec_first(&stmt, ("postgres",)).await.unwrap();
    let n2: Option<i64> = c.exec_first(&stmt, ("security",)).await.unwrap();
    assert_eq!((n1, n2), (Some(1), Some(1)));
    println!("FULLTEXT KEY converted; AGAINST (?) in 4 prepared statements: correct");
    c.query_drop("DROP DATABASE r5six_ft").await.unwrap();
}
