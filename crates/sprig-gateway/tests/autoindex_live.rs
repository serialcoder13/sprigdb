//! Chapter 13 step 12 through a running gateway in catalog mode: automatic
//! indexes hidden from `SHOW INDEX`, `SHOW CREATE TABLE` and
//! `information_schema.STATISTICS`, shown with `SET
//! @sprig_show_auto_indexes = 1`, reserved names refused with 39050, and
//! `CREATE TABLE ... LIKE` never copying them. The automatic index is
//! built behind the gateway's back, as automatic indexing does, on the
//! Compose single node:
//!
//! ```sh
//! deploy/compose/gateway-dev.sh up      # a gateway built from this tree
//! export SPRIG_TEST_M03_GATEWAY=127.0.0.1:13306,127.0.0.1:13307
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! cargo test -p sprig-gateway --test autoindex_live -- --nocapture --test-threads 1
//! ```
//!
//! Without both variables the test reports what it needs and passes (or
//! fails under `SPRIG_TEST_STRICT=1`). Its database is dropped at the end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Row, SslOpts};
use sprig_testkit::{skip, unique_ident};

const PASSWORD: &str = "sprig-dev-app";
const GATEWAY: &str = "SPRIG_TEST_M03_GATEWAY";
const MARIADB: &str = "SPRIG_TEST_MARIADB_SINGLE";
const A: &str = "acme.app";

async fn conn(addr: &str, db: Option<&str>) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(A))
        .pass(Some(PASSWORD))
        .db_name(db)
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("connect {A}@{addr}: {e}"))
}

fn errno(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(s) => s.code,
        other => panic!("not a server error: {other}"),
    }
}

/// Column `col` of every row.
async fn column(c: &mut Conn, sql: &str, col: &str) -> Vec<String> {
    let rows: Vec<Row> = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.into_iter()
        .map(|r| {
            let i = r
                .columns_ref()
                .iter()
                .position(|c| c.name_str().eq_ignore_ascii_case(col))
                .unwrap_or_else(|| panic!("{sql}: no column {col}"));
            match r.as_ref(i).unwrap() {
                mysql_async::Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                other => format!("{other:?}"),
            }
        })
        .collect()
}

#[tokio::test]
async fn automatic_indexes_through_the_gateway() {
    let Some(gw) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, "automatic_indexes_through_the_gateway");
        return;
    };
    let Some(root_url) = skip::var(MARIADB) else {
        skip::report_missing(MARIADB, "automatic_indexes_through_the_gateway");
        return;
    };
    let mut root = Conn::new(Opts::from_url(&root_url).unwrap()).await.unwrap();
    for addr in &skip::urls(&gw) {
        let db = unique_ident("ailive");
        let table = unique_ident("orders");
        let mut c = conn(addr, None).await;
        c.query_drop(format!("CREATE DATABASE {db}")).await.unwrap();
        c.query_drop(format!("USE {db}")).await.unwrap();
        c.query_drop(format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, \
             created_at DATETIME NOT NULL, KEY by_customer (customer_id))"
        ))
        .await
        .unwrap();

        // Reserved names (39050), whatever the session shows.
        for sql in [
            format!("CREATE INDEX sprig_auto_00000001 ON {table} (created_at)"),
            format!("ALTER TABLE {table} ADD KEY SPRIG_AUTO_00000002 (created_at)"),
            format!("ALTER TABLE {table} RENAME INDEX by_customer TO sprig_auto_00000003"),
        ] {
            let e = c.query_drop(&sql).await.unwrap_err();
            assert_eq!(errno(&e), 39050, "{sql}: {e}");
        }

        // Automatic indexing builds one on the shard, as sprig_ctl would.
        let schema: Option<String> = root
            .exec_first(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = ?",
                (&table,),
            )
            .await
            .unwrap();
        let schema = schema.expect("the table's shard schema");
        root.query_drop(format!(
            "ALTER TABLE `{schema}`.`{table}` ADD INDEX sprig_auto_5be0c2a1 (customer_id, created_at)"
        ))
        .await
        .unwrap();

        // Hidden by default.
        let keys = column(&mut c, &format!("SHOW INDEX FROM {table}"), "Key_name").await;
        assert!(
            !keys.iter().any(|k| k.starts_with("sprig_auto_")),
            "{keys:?}"
        );
        let create = column(
            &mut c,
            &format!("SHOW CREATE TABLE {table}"),
            "Create Table",
        )
        .await;
        assert!(!create[0].contains("sprig_auto_"), "{}", create[0]);
        let stats = column(
            &mut c,
            &format!(
                "SELECT INDEX_NAME FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = '{table}'"
            ),
            "INDEX_NAME",
        )
        .await;
        assert!(
            !stats.iter().any(|k| k.starts_with("sprig_auto_")),
            "{stats:?}"
        );
        // Dropping it while hidden is refused.
        let e = c
            .query_drop(format!("DROP INDEX sprig_auto_5be0c2a1 ON {table}"))
            .await
            .unwrap_err();
        assert_eq!(errno(&e), 39050);

        // CREATE TABLE ... LIKE never copies it, even when shown.
        c.query_drop("SET @sprig_show_auto_indexes = 1")
            .await
            .unwrap();
        c.query_drop(format!("CREATE TABLE {table}_copy LIKE {table}"))
            .await
            .unwrap();
        let copy = column(&mut c, &format!("SHOW INDEX FROM {table}_copy"), "Key_name").await;
        assert!(
            !copy.iter().any(|k| k.starts_with("sprig_auto_")),
            "{copy:?}"
        );

        // Shown in all three with the session flag.
        let keys = column(&mut c, &format!("SHOW INDEX FROM {table}"), "Key_name").await;
        assert!(keys.contains(&"sprig_auto_5be0c2a1".to_owned()), "{keys:?}");
        let create = column(
            &mut c,
            &format!("SHOW CREATE TABLE {table}"),
            "Create Table",
        )
        .await;
        assert!(create[0].contains("sprig_auto_5be0c2a1"), "{}", create[0]);
        let stats = column(
            &mut c,
            &format!(
                "SELECT INDEX_NAME FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = '{table}'"
            ),
            "INDEX_NAME",
        )
        .await;
        assert!(
            stats.contains(&"sprig_auto_5be0c2a1".to_owned()),
            "{stats:?}"
        );
        // Bad values are refused; the flag is the gateway's.
        let e = c
            .query_drop("SET @sprig_show_auto_indexes = 2")
            .await
            .unwrap_err();
        assert_eq!(errno(&e), 39051);

        // Back off: hidden again, for this session only.
        c.query_drop("SET @sprig_show_auto_indexes = 0")
            .await
            .unwrap();
        let keys = column(&mut c, &format!("SHOW INDEX FROM {table}"), "Key_name").await;
        assert!(
            !keys.iter().any(|k| k.starts_with("sprig_auto_")),
            "{keys:?}"
        );

        // Shown, a user may drop it (chapter 07).
        c.query_drop("SET @sprig_show_auto_indexes = ON")
            .await
            .unwrap();
        c.query_drop(format!("DROP INDEX sprig_auto_5be0c2a1 ON {table}"))
            .await
            .unwrap();
        c.query_drop(format!("DROP DATABASE {db}")).await.unwrap();
        c.disconnect().await.unwrap();
    }
    root.disconnect().await.unwrap();
}
