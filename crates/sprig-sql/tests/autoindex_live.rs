//! Chapter 13 step 12 against a real MariaDB: automatic indexes leave no
//! drift in what dumps and schema tools read, `CREATE TABLE ... LIKE`
//! never copies them, the `STATISTICS` filter hides them in any letter
//! case, and the 64-index rule makes room for a user's index. Skipped
//! unless the Compose single node is named:
//!
//! ```sh
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! cargo test -p sprig-sql --test autoindex_live -- --nocapture
//! ```
//!
//! Everything lives in the schema `__sprig_test_aiidx_<pid>`, dropped at
//! the end, also on failure.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use sprig_proto_mysql::client::{ClientConfig, MysqlClient, TlsMode, TlsOptions};
use sprig_sql::lex::LexOptions;
use sprig_sql::names::{
    ExistingIndex, MAX_INDEXES_PER_TABLE, Visibility, create_like_sql, filter_create_table,
    index_changes, make_room,
};

const ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";

async fn connect(url: &str) -> MysqlClient {
    let rest = url.strip_prefix("mysql://").expect("mysql:// URL");
    let (cred, addr) = rest.rsplit_once('@').expect("user@host");
    let (user, pass) = cred.split_once(':').unwrap_or((cred, ""));
    let mut cfg = ClientConfig::new(user, pass);
    cfg.tls = TlsMode::Preferred;
    cfg.tls_config = Some(
        TlsOptions {
            skip_verify: true,
            ..TlsOptions::default()
        }
        .build()
        .unwrap(),
    );
    MysqlClient::connect(addr.trim_end_matches('/'), &cfg)
        .await
        .unwrap()
}

async fn strings(c: &mut MysqlClient, sql: &str) -> Vec<Vec<String>> {
    let rs = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let rs = rs
        .into_iter()
        .find(|r| !r.columns.is_empty())
        .unwrap_or_default();
    rs.text_rows()
        .unwrap()
        .into_iter()
        .map(|r| {
            r.iter()
                .map(|v| String::from_utf8_lossy(v.as_deref().unwrap_or(b"NULL")).into_owned())
                .collect()
        })
        .collect()
}

async fn create_text(c: &mut MysqlClient, table: &str) -> String {
    strings(c, &format!("SHOW CREATE TABLE {table}")).await[0][1].clone()
}

async fn index_names(c: &mut MysqlClient, schema: &str, table: &str) -> Vec<String> {
    strings(
        c,
        &format!(
            "SELECT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '{schema}' \
             AND TABLE_NAME = '{table}' GROUP BY INDEX_NAME ORDER BY INDEX_NAME"
        ),
    )
    .await
    .into_iter()
    .map(|r| r[0].clone())
    .collect::<std::collections::BTreeSet<_>>()
    .into_iter()
    .collect()
}

const HIDDEN: Visibility = Visibility {
    auto_indexes: false,
    internal: false,
    gipk: false,
};

async fn body(c: &mut MysqlClient, schema: &str) {
    c.query_drop(&format!("USE `{schema}`")).await.unwrap();
    c.query_drop(
        "CREATE TABLE orders (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
         customer_id BIGINT NOT NULL, status VARCHAR(16) NOT NULL, created_at DATETIME NOT NULL, \
         total DECIMAL(10,2) NOT NULL DEFAULT 0, KEY by_status (status), \
         CONSTRAINT chk_total CHECK (total >= 0))",
    )
    .await
    .unwrap();
    let before = create_text(c, "orders").await;
    // What automatic indexing adds (SprigDB's own session), in both cases.
    c.query_drop(
        "ALTER TABLE orders ADD INDEX IF NOT EXISTS sprig_auto_5be0c2a1 (customer_id, created_at DESC), \
         ADD INDEX SPRIG_AUTO_0000BEEF (created_at)",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO orders (customer_id, status, created_at) VALUES (1, 'open', NOW())")
        .await
        .unwrap();
    let with_auto = create_text(c, "orders").await;
    assert!(with_auto.contains("sprig_auto_5be0c2a1"), "{with_auto}");

    // 1. No drift: what dumps and ORMs read equals the table without them
    //    (AUTO_INCREMENT aside, which moved with the insert).
    let filtered = filter_create_table(&with_auto, &HIDDEN);
    let strip_ai = |t: &str| {
        t.split(' ')
            .filter(|w| !w.starts_with("AUTO_INCREMENT="))
            .collect::<Vec<_>>()
            .join(" ")
    };
    assert_eq!(strip_ai(&filtered), strip_ai(&before), "{filtered}");
    // Replaying the dump creates the same visible table.
    let replay = filtered.replacen("CREATE TABLE `orders`", "CREATE TABLE `orders_restored`", 1);
    c.query_drop(&replay)
        .await
        .unwrap_or_else(|e| panic!("{replay}: {e}"));
    let restored = create_text(c, "orders_restored").await;
    assert_eq!(
        strip_ai(&restored.replacen("`orders_restored`", "`orders`", 1)),
        strip_ai(&filtered)
    );

    // 2. CREATE TABLE ... LIKE never copies them.
    let sql = create_like_sql(
        &format!("CREATE TABLE `{schema}`.`orders_like`"),
        &with_auto,
    )
    .unwrap();
    c.query_drop(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let like = index_names(c, schema, "orders_like").await;
    assert_eq!(like, ["PRIMARY", "by_status"]);

    // 3. The STATISTICS filter of chapter 07 (rule 5) hides both letter
    //    cases under information_schema's collation.
    let shown = index_names(c, schema, "orders").await;
    assert_eq!(shown.len(), 4, "{shown:?}");
    let hidden: Vec<String> = strings(
        c,
        &format!(
            "SELECT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '{schema}' \
             AND TABLE_NAME = 'orders' AND LEFT(`INDEX_NAME`, 11) <> 'sprig_auto_' \
             GROUP BY INDEX_NAME ORDER BY INDEX_NAME"
        ),
    )
    .await
    .into_iter()
    .map(|r| r[0].clone())
    .collect::<std::collections::BTreeSet<_>>()
    .into_iter()
    .collect();
    assert_eq!(hidden, ["PRIMARY", "by_status"]);

    // 4. The 64-index rule: 62 user indexes, the primary key and one
    //    automatic index fill the table.
    let mut cols = Vec::new();
    for i in 0..62 {
        cols.push(format!("c{i} INT, KEY k{i} (c{i})"));
    }
    c.query_drop(&format!(
        "CREATE TABLE wide (id INT PRIMARY KEY, extra INT, {})",
        cols.join(", ")
    ))
    .await
    .unwrap();
    c.query_drop("ALTER TABLE wide ADD INDEX sprig_auto_00000001 (c0, c1)")
        .await
        .unwrap();
    let names = index_names(c, schema, "wide").await;
    assert_eq!(names.len(), MAX_INDEXES_PER_TABLE);
    let user = "CREATE INDEX by_extra ON wide (extra)";
    // MariaDB itself refuses the 65th index.
    let e = c.query_drop(user).await.unwrap_err();
    assert_eq!(e.server_error().unwrap().code, 1069, "{e}");
    let adding = usize::try_from(
        index_changes(user.as_bytes(), &LexOptions::mariadb())
            .unwrap()
            .net_added(),
    )
    .unwrap();
    let existing: Vec<ExistingIndex> = names
        .iter()
        .map(|n| ExistingIndex {
            name: n.clone(),
            value: None,
        })
        .collect();
    let drop = make_room(&existing, adding);
    assert_eq!(drop, ["sprig_auto_00000001"]);
    for d in &drop {
        c.query_drop(&format!("ALTER TABLE wide DROP INDEX IF EXISTS `{d}`"))
            .await
            .unwrap();
    }
    c.query_drop(user).await.unwrap();
    let names = index_names(c, schema, "wide").await;
    assert_eq!(names.len(), MAX_INDEXES_PER_TABLE);
    assert!(names.contains(&"by_extra".to_owned()));
    // With no automatic index left, the rule drops nothing and MariaDB
    // refuses again.
    let existing: Vec<ExistingIndex> = names
        .iter()
        .map(|n| ExistingIndex {
            name: n.clone(),
            value: None,
        })
        .collect();
    assert!(make_room(&existing, 1).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn autoindex_hiding_live() {
    let Ok(url) = std::env::var(ENV) else {
        eprintln!("skipped: {ENV} is not set");
        return;
    };
    let schema = format!("__sprig_test_aiidx_{}", std::process::id());
    let mut admin = connect(&url).await;
    admin
        .query_drop(&format!("CREATE DATABASE IF NOT EXISTS `{schema}`"))
        .await
        .unwrap();
    let (u, s) = (url.clone(), schema.clone());
    let r = tokio::spawn(async move {
        let mut c = connect(&u).await;
        body(&mut c, &s).await;
        c.quit().await;
    })
    .await;
    admin
        .query_drop(&format!("DROP DATABASE IF EXISTS `{schema}`"))
        .await
        .unwrap();
    admin.quit().await;
    if let Err(e) = r {
        std::panic::resume_unwind(e.into_panic());
    }
}
