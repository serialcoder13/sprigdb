//! SQL over REST and gRPC against the MySQL wire: the same statements give the same results and the same
//! errors, with the same name virtualization, on 8080 (HTTP/1.1 REST, h2c
//! gRPC) and 8443 (TLS: REST, gRPC with ALPN h2).
//!
//! Needs a gateway with HTTP listeners and a tenant whose user can log in
//! with a password, such as deploy/compose/m07/stack.sh's:
//!
//! ```sh
//! deploy/compose/m07/stack.sh up
//! export SPRIG_TEST_M7_GATEWAY=127.0.0.1:19306,127.0.0.1:19080,127.0.0.1:19443
//! cargo test -p sprig-gateway --test http_live -- --nocapture --test-threads 1
//! ```
//!
//! The variable lists the MySQL listener, the HTTP listener and the HTTPS
//! listener. Login `m7a.app` / `sprig-dev-app` unless `SPRIG_TEST_M7_USER`
//! and `SPRIG_TEST_M7_PASSWORD` say otherwise.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use std::sync::Arc;

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_api::proto::v1::sql_client::SqlClient;
use sprig_api::proto::v1::transactions_client::TransactionsClient;
use sprig_api::proto::v1::{BeginRequest, EndTransactionRequest, ExecuteResponse, QueryRequest};
use sprig_api::results::cell_bytes;
use sprig_testkit::skip;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

const GATEWAY: &str = "SPRIG_TEST_M7_GATEWAY";
const DB: &str = "m7_diff";
const CA: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/compose/gateway-certs/ca.pem"
);

struct Env {
    mysql: String,
    http: String,
    https: String,
    user: String,
    password: String,
}

fn env(test: &str) -> Option<Env> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let urls = skip::urls(&v);
    assert_eq!(urls.len(), 3, "{GATEWAY}: mysql,http,https");
    Some(Env {
        mysql: urls[0].clone(),
        http: urls[1].clone(),
        https: urls[2].clone(),
        user: std::env::var("SPRIG_TEST_M7_USER").unwrap_or_else(|_| "m7a.app".into()),
        password: std::env::var("SPRIG_TEST_M7_PASSWORD")
            .unwrap_or_else(|_| "sprig-dev-app".into()),
    })
}

/// One result as both sides can show it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Res {
    columns: Vec<String>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    affected: u64,
    last_insert_id: u64,
    warnings: u32,
}

/// A statement's outcome: results, or the error (number, SQLSTATE, text).
type Outcome = Result<Vec<Res>, (u16, String, String)>;

/// The statements, in order; each runs in a fresh session on HTTP (and in
/// one session on the wire), so none depends on session state across
/// statements (pieces with `;` run as one multi-statement text).
const STATEMENTS: &[&str] = &[
    "CREATE TABLE items (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(40) NOT NULL, price DECIMAL(10,2), qty INT DEFAULT 0, data BLOB, created DATETIME, doc JSON)",
    "INSERT INTO items (name, price, qty, data, created, doc) VALUES ('apple', 1.50, 10, 0x00ff10, '2026-11-08 10:00:00', '{\"a\": 1}'), ('pear', 2.25, 5, NULL, NULL, NULL), ('fig', NULL, 0, '', '2025-01-01 00:00:00', '[1, 2]')",
    "SELECT * FROM items ORDER BY id",
    "SELECT name, price * qty AS total FROM items WHERE qty > 0 ORDER BY name",
    "SELECT COUNT(*), SUM(qty), AVG(price), MAX(created) FROM items",
    "UPDATE items SET qty = qty + 1 WHERE name = 'pear'",
    "DELETE FROM items WHERE name = 'nothing'",
    "INSERT INTO items (id, name) VALUES (1, 'dup')",
    "SELECT * FROM missing_table",
    "SELEC 1",
    "SELECT DATABASE()",
    "SHOW TABLES",
    "SHOW CREATE TABLE items",
    "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name",
    "SHOW COLUMNS FROM items",
    "CREATE VIEW v_items AS SELECT id, name FROM items WHERE qty > 0",
    "SELECT * FROM v_items ORDER BY id",
    "SELECT JSON_EXTRACT(doc, '$.a') FROM items WHERE id = 1",
    "SELECT HEX(data), LENGTH(data), data FROM items ORDER BY id",
    "SELECT CAST('2026-01-02' AS DATE), CAST(1.5 AS DECIMAL(5,2)), 1e3, NULL, 'x' COLLATE utf8mb4_bin",
    "SELECT * FROM items WHERE id IN (SELECT id FROM items WHERE price IS NULL)",
    "CREATE TABLE orders (id INT PRIMARY KEY, item_id INT, FOREIGN KEY (item_id) REFERENCES items (id))",
    "INSERT INTO orders VALUES (1, 1), (2, 2)",
    "INSERT INTO orders VALUES (3, 999)",
    "SELECT o.id, i.name FROM orders o JOIN items i ON i.id = o.item_id ORDER BY o.id",
    "SELECT * FROM no_such_db.items",
    "SELECT * FROM mysql.user",
    "CREATE DATABASE t5_reserved",
    "SET @x = 5; SELECT @x + 1",
    "INSERT INTO items (name) VALUES ('kiwi'); SELECT LAST_INSERT_ID()",
    "SELECT VERSION(), @@version_comment",
    "ALTER TABLE items ADD COLUMN note TEXT",
    "SELECT id, note FROM items ORDER BY id",
    "SELECT 1 / 0, 'a' + 1",
    "SELECT id, name FROM items ORDER BY id LIMIT 2 OFFSET 1",
    "EXPLAIN SELECT * FROM items WHERE id = 1",
    "TRUNCATE TABLE orders",
    "DROP VIEW v_items",
    "SELECT name FROM items WHERE name LIKE 'a%' OR name REGEXP '^k'",
    "DROP TABLE orders, items",
];

async fn wire_conn(e: &Env, db: Option<&str>) -> Conn {
    let (host, port) = e.mysql.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(e.user.clone()))
        .pass(Some(e.password.clone()))
        .db_name(db)
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts).await.expect("wire connect")
}

async fn wire(conn: &mut Conn, sql: &str) -> Outcome {
    let mut out = Vec::new();
    let mut r = match conn.query_iter(sql).await {
        Ok(r) => r,
        Err(mysql_async::Error::Server(s)) => return Err((s.code, s.state, s.message)),
        Err(e) => panic!("{sql}: {e}"),
    };
    loop {
        let columns: Vec<String> = r
            .columns_ref()
            .iter()
            .map(|c| c.name_str().into_owned())
            .collect();
        let rows = match r.collect::<mysql_async::Row>().await {
            Ok(rows) => rows,
            Err(mysql_async::Error::Server(s)) => return Err((s.code, s.state, s.message)),
            Err(e) => panic!("{sql}: {e}"),
        };
        let rows = rows
            .into_iter()
            .map(|row| {
                row.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(b),
                        other => Some(other.as_sql(true).into_bytes()),
                    })
                    .collect()
            })
            .collect();
        out.push(Res {
            columns,
            rows,
            affected: r.affected_rows(),
            last_insert_id: r.last_insert_id().unwrap_or(0),
            warnings: u32::from(r.warnings()),
        });
        if r.is_empty() {
            break;
        }
    }
    Ok(out)
}

fn from_proto(r: &ExecuteResponse) -> Vec<Res> {
    r.results
        .iter()
        .map(|res| Res {
            columns: res.columns.iter().map(|c| c.name.clone()).collect(),
            rows: res
                .rows
                .iter()
                .map(|row| {
                    row.values
                        .iter()
                        .zip(&res.columns)
                        .map(|(v, c)| cell_bytes(v, c.binary))
                        .collect()
                })
                .collect(),
            affected: res.affected_rows,
            last_insert_id: res.last_insert_id,
            warnings: res.warnings,
        })
        .collect()
}

fn tls_config() -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    let pem = std::fs::read(CA).expect("deploy/compose/gateway-certs/ca.pem");
    for c in rustls::pki_types::CertificateDer::pem_slice_iter(&pem) {
        roots.add(c.unwrap()).unwrap();
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Arc::new(cfg)
}

use rustls::pki_types::pem::PemObject as _;

/// One REST call over HTTP/1.1 (TLS when `tls`): status and body.
async fn rest(
    addr: &str,
    tls: bool,
    auth: &str,
    method: &str,
    path: &str,
    body: &str,
) -> (u16, Bytes) {
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header("host", addr)
        .header("authorization", auth)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_owned())))
        .unwrap();
    macro_rules! send {
        ($io:expr) => {{
            let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new($io))
                .await
                .unwrap();
            tokio::spawn(conn);
            let resp = tx.send_request(req).await.unwrap();
            let status = resp.status().as_u16();
            (status, resp.into_body().collect().await.unwrap().to_bytes())
        }};
    }
    if tls {
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let stream = tokio_rustls::TlsConnector::from(tls_config())
            .connect(name, tcp)
            .await
            .unwrap();
        send!(stream)
    } else {
        send!(tcp)
    }
}

fn outcome_of_rest(status: u16, body: &[u8]) -> Outcome {
    let v: serde_json::Value = serde_json::from_slice(body).unwrap();
    if status >= 400 {
        let e: sprig_api::status::ErrorBody = serde_json::from_value(v).unwrap();
        return Err((e.code, e.sqlstate, e.message));
    }
    Ok(from_proto(
        &sprig_api::results::execute_from_json(&v).unwrap(),
    ))
}

fn outcome_of_status(s: &tonic::Status) -> (u16, String, String) {
    let e = sprig_api::status::from_status(s);
    let p = e.protocol();
    (
        p.mysql_errno,
        String::from_utf8_lossy(&p.sqlstate).into_owned(),
        e.message,
    )
}

async fn grpc_channel(addr: &str, tls: bool) -> Channel {
    let mut ep =
        Endpoint::from_shared(format!("{}://{addr}", if tls { "https" } else { "http" })).unwrap();
    if tls {
        let pem = std::fs::read(CA).unwrap();
        ep = ep
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(pem))
                    .domain_name("localhost"),
            )
            .unwrap();
    }
    ep.connect().await.unwrap()
}

fn with_auth<T>(msg: T, auth: &str) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut()
        .insert("authorization", auth.parse().unwrap());
    r
}

#[derive(Clone, Copy, Debug)]
enum Path {
    RestHttp,
    RestHttps,
    GrpcH2c,
    GrpcTls,
}

async fn run_over(e: &Env, p: Path, auth: &str, sql: &str, db: &str) -> Outcome {
    let multi = sql.contains("; ");
    let body = serde_json::json!({ "sql": sql, "multi_statements": multi }).to_string();
    let path = format!("/v1/databases/{db}/sql:execute");
    match p {
        Path::RestHttp => {
            let (s, b) = rest(&e.http, false, auth, "POST", &path, &body).await;
            outcome_of_rest(s, &b)
        }
        Path::RestHttps => {
            let (s, b) = rest(&e.https, true, auth, "POST", &path, &body).await;
            outcome_of_rest(s, &b)
        }
        Path::GrpcH2c | Path::GrpcTls => {
            let tls = matches!(p, Path::GrpcTls);
            let ch = grpc_channel(if tls { &e.https } else { &e.http }, tls).await;
            let mut c = SqlClient::new(ch);
            let r = c
                .execute(with_auth(
                    QueryRequest {
                        database: db.to_owned(),
                        sql: sql.to_owned(),
                        transaction: String::new(),
                        multi_statements: multi,
                        ..Default::default()
                    },
                    auth,
                ))
                .await;
            match r {
                Ok(r) => Ok(from_proto(&r.into_inner())),
                Err(s) => Err(outcome_of_status(&s)),
            }
        }
    }
}

fn basic(e: &Env) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", e.user, e.password))
    )
}

#[tokio::test]
async fn rest_and_grpc_match_the_wire() {
    let Some(e) = env("rest_and_grpc_match_the_wire") else {
        return;
    };
    let auth = basic(&e);
    // The wire pass, in one session.
    let mut admin = wire_conn(&e, None).await;
    admin
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await
        .unwrap();
    admin
        .query_drop(format!("CREATE DATABASE {DB}"))
        .await
        .unwrap();
    let mut c = wire_conn(&e, Some(DB)).await;
    let mut expected = Vec::new();
    for sql in STATEMENTS {
        expected.push(wire(&mut c, sql).await);
    }
    drop(c);
    let errors = expected.iter().filter(|o| o.is_err()).count();
    println!("wire: {} statements, {errors} errors", expected.len());
    for p in [
        Path::RestHttp,
        Path::RestHttps,
        Path::GrpcH2c,
        Path::GrpcTls,
    ] {
        admin
            .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
            .await
            .unwrap();
        admin
            .query_drop(format!("CREATE DATABASE {DB}"))
            .await
            .unwrap();
        let mut mismatches = Vec::new();
        for (sql, want) in STATEMENTS.iter().zip(&expected) {
            let got = run_over(&e, p, &auth, sql, DB).await;
            if &got != want {
                mismatches.push(format!("{sql}\n  wire: {want:?}\n  {p:?}: {got:?}"));
            }
        }
        println!(
            "{p:?}: {} statements, {} mismatches",
            STATEMENTS.len(),
            mismatches.len()
        );
        assert!(mismatches.is_empty(), "{p:?}:\n{}", mismatches.join("\n"));
    }
    admin
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await
        .unwrap();
}

#[tokio::test]
async fn transactions_hold_a_session_across_calls() {
    let Some(e) = env("transactions_hold_a_session_across_calls") else {
        return;
    };
    let auth = basic(&e);
    let mut admin = wire_conn(&e, None).await;
    admin
        .query_drop("DROP DATABASE IF EXISTS m7_txn")
        .await
        .unwrap();
    admin.query_drop("CREATE DATABASE m7_txn").await.unwrap();
    admin
        .query_drop("CREATE TABLE m7_txn.t (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    // REST: begin, write, invisible outside, commit, visible.
    let (s, b) = rest(
        &e.http,
        false,
        &auth,
        "POST",
        "/v1/databases/m7_txn/transactions",
        "{}",
    )
    .await;
    assert_eq!(s, 200, "{}", String::from_utf8_lossy(&b));
    let t: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let handle = t["name"].as_str().unwrap().to_owned();
    let body = serde_json::json!({"sql": "INSERT INTO t VALUES (1, 10)", "transaction": handle})
        .to_string();
    let (s, _) = rest(
        &e.http,
        false,
        &auth,
        "POST",
        "/v1/databases/m7_txn/sql:execute",
        &body,
    )
    .await;
    assert_eq!(s, 200);
    let outside = run_over(
        &e,
        Path::RestHttp,
        &auth,
        "SELECT COUNT(*) FROM t",
        "m7_txn",
    )
    .await
    .unwrap();
    assert_eq!(outside[0].rows[0][0].as_deref(), Some(&b"0"[..]));
    let (s, _) = rest(
        &e.http,
        false,
        &auth,
        "POST",
        &format!("/v1/{handle}:commit"),
        "{}",
    )
    .await;
    assert_eq!(s, 200);
    let after = run_over(
        &e,
        Path::RestHttp,
        &auth,
        "SELECT COUNT(*) FROM t",
        "m7_txn",
    )
    .await
    .unwrap();
    assert_eq!(after[0].rows[0][0].as_deref(), Some(&b"1"[..]));
    // A finished handle is unknown.
    let (s, b) = rest(
        &e.http,
        false,
        &auth,
        "POST",
        &format!("/v1/{handle}:commit"),
        "{}",
    )
    .await;
    assert_eq!(s, 409, "{}", String::from_utf8_lossy(&b));
    // gRPC: begin, write, roll back.
    let ch = grpc_channel(&e.http, false).await;
    let mut tx = TransactionsClient::new(ch.clone());
    let h = tx
        .begin(with_auth(
            BeginRequest {
                database: "m7_txn".into(),
                read_only: false,
            },
            &auth,
        ))
        .await
        .unwrap()
        .into_inner()
        .name;
    let mut sql = SqlClient::new(ch);
    sql.execute(with_auth(
        QueryRequest {
            database: "m7_txn".into(),
            sql: "INSERT INTO t VALUES (2, 20)".into(),
            transaction: h.clone(),
            multi_statements: false,
            ..Default::default()
        },
        &auth,
    ))
    .await
    .unwrap();
    tx.rollback(with_auth(EndTransactionRequest { transaction: h }, &auth))
        .await
        .unwrap();
    let after = run_over(&e, Path::GrpcH2c, &auth, "SELECT COUNT(*) FROM t", "m7_txn")
        .await
        .unwrap();
    assert_eq!(after[0].rows[0][0].as_deref(), Some(&b"1"[..]));
    admin
        .query_drop("DROP DATABASE IF EXISTS m7_txn")
        .await
        .unwrap();
}

#[tokio::test]
async fn idle_transaction_handles_expire() {
    let Some(e) = env("idle_transaction_handles_expire") else {
        return;
    };
    let auth = basic(&e);
    let set = |v: &str| format!("{{\"scope\": \"dc\", \"value\": {v}, \"reason\": \"http_live\"}}");
    let (s, b) = rest(
        &e.http,
        false,
        "",
        "PUT",
        "/admin/v1/settings/api.txn.idle_timeout_ms",
        &set("1000"),
    )
    .await;
    assert_eq!(s, 200, "{}", String::from_utf8_lossy(&b));
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let (s, b) = rest(
        &e.http,
        false,
        &auth,
        "POST",
        "/v1/databases/-/transactions",
        "{}",
    )
    .await;
    assert_eq!(s, 200, "{}", String::from_utf8_lossy(&b));
    let t: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let handle = t["name"].as_str().unwrap().to_owned();
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    let body = serde_json::json!({"sql": "SELECT 1", "transaction": handle}).to_string();
    let (s, b) = rest(
        &e.http,
        false,
        &auth,
        "POST",
        "/v1/databases/-/sql:execute",
        &body,
    )
    .await;
    let _ = rest(
        &e.http,
        false,
        "",
        "DELETE",
        "/admin/v1/settings/api.txn.idle_timeout_ms?scope=dc",
        "",
    )
    .await;
    assert_eq!(s, 409, "{}", String::from_utf8_lossy(&b));
    assert!(
        String::from_utf8_lossy(&b).contains("39782"),
        "{}",
        String::from_utf8_lossy(&b)
    );
}

#[tokio::test]
async fn transport_and_credential_errors() {
    let Some(e) = env("transport_and_credential_errors") else {
        return;
    };
    // Wrong password: 401, 1045, WWW-Authenticate.
    let bad = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{}:wrong", e.user))
    );
    let (s, b) = rest(
        &e.http,
        false,
        &bad,
        "POST",
        "/v1/databases/-/sql:execute",
        "{\"sql\":\"SELECT 1\"}",
    )
    .await;
    assert_eq!(s, 401);
    assert!(String::from_utf8_lossy(&b).contains("1045"));
    // gRPC over HTTP/1.1: 415, 39791.
    let tcp = tokio::net::TcpStream::connect(&e.http).await.unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let req = hyper::Request::builder()
        .method("POST")
        .uri("/sprig.v1.Sql/Execute")
        .header("host", &e.http)
        .header("content-type", "application/grpc")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = tx.send_request(req).await.unwrap();
    assert_eq!(resp.status().as_u16(), 415);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert!(String::from_utf8_lossy(&body).contains("39791"));
    // OpenAPI.
    let (s, b) = rest(&e.http, false, "", "GET", "/v1/openapi.json", "").await;
    assert_eq!(s, 200);
    assert!(String::from_utf8_lossy(&b).contains("sql:execute"));
}

/// A failed `USE` must not leave a session on a pooled connection that
/// still has another default schema (a `USE` may borrow such a
/// connection; a failed one closes it).
#[tokio::test]
async fn a_failed_use_never_leaves_another_schema() {
    let Some(e) = env("a_failed_use_never_leaves_another_schema") else {
        return;
    };
    let mut admin = wire_conn(&e, None).await;
    admin
        .query_drop("CREATE DATABASE IF NOT EXISTS m7_use_a")
        .await
        .unwrap();
    admin
        .query_drop("CREATE TABLE IF NOT EXISTS m7_use_a.only_in_a (x INT)")
        .await
        .unwrap();
    drop(admin);
    for _ in 0..20 {
        // Put schema A on pooled connections: sessions that USE it, then end.
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let (host, port) = e.mysql.rsplit_once(':').unwrap();
            let opts = OptsBuilder::default()
                .ip_or_hostname(host)
                .tcp_port(port.parse().unwrap())
                .user(Some(e.user.clone()))
                .pass(Some(e.password.clone()))
                .db_name(Some("m7_use_a"))
                .prefer_socket(false)
                .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
            tasks.push(tokio::spawn(async move {
                let mut c = Conn::new(opts).await.unwrap();
                c.query_drop("SELECT COUNT(*) FROM only_in_a")
                    .await
                    .unwrap();
                c.disconnect().await.unwrap();
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        // A session without a database whose USE fails.
        let mut c = wire_conn(&e, None).await;
        let err = c.query_drop("USE m7_missing_db").await.unwrap_err();
        assert!(
            matches!(&err, mysql_async::Error::Server(s) if s.code == 1049),
            "{err}"
        );
        let db: Option<Option<String>> = c.query_first("SELECT DATABASE()").await.unwrap();
        assert_eq!(db, Some(None), "the session has no database");
        let err = c
            .query_drop("SELECT COUNT(*) FROM only_in_a")
            .await
            .unwrap_err();
        assert!(
            matches!(&err, mysql_async::Error::Server(s) if s.code == 1046),
            "an unqualified name must find no database, not another schema: {err}"
        );
        drop(c);
        // The same over HTTP: a call naming a missing database fails, and the
        // next call without one sees none.
        let auth = basic(&e);
        let r = run_over(&e, Path::RestHttp, &auth, "SELECT 1", "m7_missing_db").await;
        assert_eq!(r.unwrap_err().0, 1049);
        let r = run_over(&e, Path::RestHttp, &auth, "SELECT DATABASE()", "-")
            .await
            .unwrap();
        assert_eq!(r[0].rows[0][0], None);
    }
    let mut admin = wire_conn(&e, None).await;
    admin
        .query_drop("DROP DATABASE IF EXISTS m7_use_a")
        .await
        .unwrap();
}
