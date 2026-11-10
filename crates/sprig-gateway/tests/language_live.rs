//! `language` on `Sql.Query` (chapter 37, "Languages"; tui decision T2):
//! Cypher and GQL reach the graph executor and MongoDB command documents
//! the document engine, over REST and gRPC, with the native protocols'
//! answers and privileges.
//!
//! ```sh
//! deploy/compose/gateway-dev.sh up     # catalog mode, tenant acme
//! # plus listener.http.* (plain HTTP) and listener.mongo.* bound and enabled
//! export SPRIG_TEST_LANG_GATEWAY=127.0.0.1:13306,127.0.0.1:8080,127.0.0.1:27018  # mysql, http, mongo
//! cargo test -p sprig-gateway --test language_live -- --nocapture --test-threads 1
//! ```
//!
//! Login `acme.app` / `sprig-dev-app` unless `SPRIG_TEST_HTTP_USER` and
//! `SPRIG_TEST_HTTP_PASSWORD` say otherwise; that user must be able to
//! create users (it makes `bf4ro`, which may read one table only).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::{Value, json};
use sprig_api::proto::v1::QueryRequest;
use sprig_api::proto::v1::sql_client::SqlClient;
use sprig_testkit::skip;
use tonic::transport::Endpoint;

const GATEWAY: &str = "SPRIG_TEST_LANG_GATEWAY";
const DB: &str = "bugfix4_lang";
const RO_USER: &str = "bf4ro";
const RO_PASSWORD: &str = "bf4-ro-pass-1";

struct Env {
    mysql: String,
    http: String,
    mongo: String,
    user: String,
    password: String,
}

fn env(test: &str) -> Option<Env> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let urls = skip::urls(&v);
    assert_eq!(urls.len(), 3, "{GATEWAY}: mysql,http,mongo");
    Some(Env {
        mysql: urls[0].clone(),
        http: urls[1].clone(),
        mongo: urls[2].clone(),
        user: std::env::var("SPRIG_TEST_HTTP_USER").unwrap_or_else(|_| "acme.app".into()),
        password: std::env::var("SPRIG_TEST_HTTP_PASSWORD")
            .unwrap_or_else(|_| "sprig-dev-app".into()),
    })
}

fn tenant(e: &Env) -> &str {
    e.user.split_once('.').map_or("", |(t, _)| t)
}

fn basic(user: &str, password: &str) -> String {
    let token = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    format!("Basic {token}")
}

async fn wire(e: &Env) -> Conn {
    let (host, port) = e.mysql.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(e.user.clone()))
        .pass(Some(e.password.clone()))
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts).await.expect("wire connect")
}

/// One REST `sql:execute` (or `sql:query` NDJSON): status and JSON.
async fn rest(e: &Env, auth: &str, db: &str, body: &Value, ndjson: bool) -> (u16, Value) {
    let tcp = tokio::net::TcpStream::connect(&e.http).await.unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let path = if ndjson { "sql:query" } else { "sql:execute" };
    let mut b = hyper::Request::builder()
        .method("POST")
        .uri(format!("/v1/databases/{db}/{path}"))
        .header("host", e.http.as_str())
        .header("authorization", auth)
        .header("content-type", "application/json");
    if ndjson {
        b = b.header("accept", "application/x-ndjson");
    }
    let resp = tx
        .send_request(b.body(Full::new(Bytes::from(body.to_string()))).unwrap())
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let v = if ndjson {
        Value::Array(
            bytes
                .split(|b| *b == b'\n')
                .filter(|l| !l.is_empty())
                .map(|l| serde_json::from_slice(l).unwrap())
                .collect(),
        )
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, v)
}

async fn rest_post(e: &Env, auth: &str, path: &str, body: &Value) -> (u16, Value) {
    let tcp = tokio::net::TcpStream::connect(&e.http).await.unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let req = hyper::Request::builder()
        .method("POST")
        .uri(path)
        .header("host", e.http.as_str())
        .header("authorization", auth)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap();
    let resp = tx.send_request(req).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// The first result's rows of a `sql:execute` reply, cells as JSON.
fn rows(v: &Value) -> Vec<Vec<Value>> {
    v["results"][0]["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|r| r.as_array().cloned().unwrap_or_default())
        .collect()
}

fn columns(v: &Value) -> Vec<(String, String)> {
    v["results"][0]["columns"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|c| {
            (
                c["name"].as_str().unwrap_or("").to_owned(),
                c["type"].as_str().unwrap_or("").to_owned(),
            )
        })
        .collect()
}

fn code(v: &Value) -> u64 {
    v["code"].as_u64().unwrap_or(0)
}

async fn setup(e: &Env) -> Conn {
    let mut w = wire(e).await;
    w.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await
        .unwrap();
    w.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
    w.query_drop(format!("CREATE TABLE {DB}.t (id INT PRIMARY KEY)"))
        .await
        .unwrap();
    w.query_drop(format!("USE {DB}")).await.unwrap();
    w.query_drop("CREATE GRAPH g").await.unwrap();
    w.query_drop(format!("DROP USER IF EXISTS {RO_USER}"))
        .await
        .unwrap();
    w.query_drop(format!(
        "CREATE USER {RO_USER} IDENTIFIED BY '{RO_PASSWORD}'"
    ))
    .await
    .unwrap();
    w.query_drop(format!("GRANT SELECT ON {DB}.t TO {RO_USER}"))
        .await
        .unwrap();
    w
}

async fn teardown(w: &mut Conn) {
    let _ = w.query_drop(format!("DROP USER IF EXISTS {RO_USER}")).await;
    let _ = w.query_drop(format!("DROP DATABASE IF EXISTS {DB}")).await;
}

#[tokio::test]
async fn languages_over_rest_and_grpc() {
    let Some(e) = env("languages_over_rest_and_grpc") else {
        return;
    };
    let mut w = setup(&e).await;
    let auth = basic(&e.user, &e.password);
    let graph = format!("{DB}.g");

    // --- Cypher: writes report counters; reads give typed columns.
    let (s, v) = rest(
        &e,
        &auth,
        &graph,
        &json!({"language": "cypher", "sql": "CREATE (a:P {name: 'Asha', n: 1})-[:KNOWS]->(b:P {name: 'Ravi', n: 2})"}),
        false,
    )
    .await;
    assert_eq!(s, 200, "{v}");
    let info = v["results"][0]["info"].as_str().unwrap_or("").to_owned();
    assert!(info.contains("nodes-created: 2"), "{info}");
    assert!(info.contains("relationships-created: 1"), "{info}");
    let (s, v) = rest(
        &e,
        &auth,
        &graph,
        &json!({"language": "cypher", "sql": "MATCH (a:P)-[:KNOWS]->(b:P) RETURN a.name AS a, b.n AS n"}),
        false,
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(
        columns(&v),
        vec![
            ("a".into(), "VARCHAR".into()),
            ("n".into(), "BIGINT".into())
        ]
    );
    assert_eq!(rows(&v), vec![vec![json!("Asha"), json!("2")]]);
    // A path as {"$path"} JSON, as `sprig --protocol bolt` shows it.
    let (s, v) = rest(
        &e,
        &auth,
        &graph,
        &json!({"language": "cypher", "sql": "MATCH p = (:P {name: 'Asha'})-[:KNOWS]->() RETURN p"}),
        false,
    )
    .await;
    assert_eq!(s, 200, "{v}");
    let cell: Value = serde_json::from_str(rows(&v)[0][0].as_str().unwrap()).unwrap();
    let p = cell["$path"].as_array().unwrap();
    assert_eq!(p.len(), 3, "{cell}");
    assert_eq!(p[0]["$node"]["props"]["name"], json!("Asha"));
    assert_eq!(p[1]["$rel"]["type"], json!("KNOWS"));

    // --- GQL: its forms go to the same executor.
    let (s, v) = rest(
        &e,
        &auth,
        &graph,
        &json!({"language": "gql", "sql": "MATCH (a IS P) FILTER a.n > 1 RETURN a.name AS name"}),
        false,
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(rows(&v), vec![vec![json!("Ravi")]]);

    // NDJSON streaming of a graph result, and caps.
    let (s, v) = rest(
        &e,
        &auth,
        &graph,
        &json!({"language": "cypher", "sql": "UNWIND range(1, 50) AS i RETURN i", "max_rows": 7}),
        true,
    )
    .await;
    assert_eq!(s, 200);
    let lines = v.as_array().unwrap();
    let n: usize = lines
        .iter()
        .filter_map(|l| l["rows"]["rows"].as_array())
        .map(Vec::len)
        .sum();
    let end = lines.iter().find(|l| l.get("end").is_some()).unwrap();
    assert_eq!(
        (n, end["end"]["truncated"].as_bool()),
        (7, Some(true)),
        "{v}"
    );

    // Errors keep their numbers.
    let (s, v) = rest(
        &e,
        &auth,
        &graph,
        &json!({"language": "cypher", "sql": "MATCH (n RETURN n"}),
        false,
    )
    .await;
    assert_eq!((s, code(&v)), (400, 39900), "{v}");
    let (s, v) = rest(
        &e,
        &auth,
        &format!("{DB}.nope"),
        &json!({"language": "cypher", "sql": "MATCH (n) RETURN n"}),
        false,
    )
    .await;
    assert_eq!(code(&v), 39902, "{s} {v}");
    // Same privileges as Bolt: bf4ro may read t only, not the graph.
    let ro = basic(&format!("{}.{RO_USER}", tenant(&e)), RO_PASSWORD);
    let (s, v) = rest(
        &e,
        &ro,
        &graph,
        &json!({"language": "cypher", "sql": "MATCH (n:P) RETURN n.name"}),
        false,
    )
    .await;
    assert_eq!((s, code(&v)), (403, 1142), "{v}");

    // --- gRPC, the same answers.
    let ch = Endpoint::from_shared(format!("http://{}", e.http))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = SqlClient::new(ch);
    let mut req = tonic::Request::new(QueryRequest {
        database: graph.clone(),
        sql: "MATCH (a:P {name: 'Ravi'}) RETURN a.n AS n".into(),
        language: "cypher".into(),
        ..QueryRequest::default()
    });
    req.metadata_mut()
        .insert("authorization", auth.parse().unwrap());
    let r = client.execute(req).await.unwrap().into_inner();
    let cells = &r.results[0].rows[0].values;
    assert_eq!(
        cells[0].kind,
        Some(prost_types::value::Kind::StringValue("2".into()))
    );

    // --- Refusals.
    let (s, v) = rest(
        &e,
        &auth,
        DB,
        &json!({"language": "redis", "sql": "GET k"}),
        false,
    )
    .await;
    assert_eq!((s, code(&v)), (400, 39794), "{v}");
    let (s, t) = rest_post(
        &e,
        &auth,
        &format!("/v1/databases/{DB}/transactions"),
        &json!({}),
    )
    .await;
    assert_eq!(s, 200, "{t}");
    let handle = t["name"].as_str().unwrap().to_owned();
    let (s, v) = rest(
        &e,
        &auth,
        DB,
        &json!({"language": "cypher", "sql": "RETURN 1", "transaction": handle}),
        false,
    )
    .await;
    assert_eq!((s, code(&v)), (400, 39794), "{v}");
    let (s, _) = rest_post(&e, &auth, &format!("/v1/{handle}:rollback"), &json!({})).await;
    assert_eq!(s, 200);

    teardown(&mut w).await;
}

#[tokio::test]
async fn mongo_commands_match_the_driver() {
    let Some(e) = env("mongo_commands_match_the_driver") else {
        return;
    };
    let mut w = setup(&e).await;
    let auth = basic(&e.user, &e.password);
    let m = |cmd: Value| json!({"language": "mongo", "sql": cmd.to_string()});

    // Writes: one reply row, `n` as affected rows.
    let docs: Vec<Value> = (0..250)
        .map(|i| json!({"_id": i, "name": format!("u{i}"), "age": i % 50, "at": {"$date": "2026-11-10T00:00:00Z"}}))
        .collect();
    let (s, v) = rest(
        &e,
        &auth,
        DB,
        &m(json!({"insert": "users", "documents": docs})),
        false,
    )
    .await;
    assert_eq!(s, 200, "{v}");
    let n = &v["results"][0]["affected_rows"];
    assert!(n == &json!("250") || n == &json!(250), "{v}");

    // A find over several batches is drained: every document, in order.
    let (s, v) = rest(
        &e,
        &auth,
        DB,
        &m(json!({"find": "users", "filter": {"age": {"$gte": 10}}, "sort": {"_id": 1}, "batchSize": 40})),
        false,
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(columns(&v), vec![("document".into(), "JSON".into())]);
    let got: Vec<Value> = rows(&v)
        .iter()
        .map(|r| serde_json::from_str(r[0].as_str().unwrap()).unwrap())
        .collect();
    assert_eq!(got.len(), 200);

    // The driver on the MongoDB port sees the same documents.
    let url = format!(
        "mongodb://{}:{}@{}/?authSource=admin&directConnection=true",
        e.user, e.password, e.mongo
    );
    let client = mongodb::Client::with_uri_str(&url).await.unwrap();
    let coll = client
        .database(DB)
        .collection::<mongodb::bson::Document>("users");
    let mut cur = coll
        .find(mongodb::bson::doc! {"age": {"$gte": 10}})
        .sort(mongodb::bson::doc! {"_id": 1})
        .await
        .unwrap();
    let mut native = Vec::new();
    while cur.advance().await.unwrap() {
        let d = cur.deserialize_current().unwrap();
        native.push(mongodb::bson::Bson::Document(d).into_relaxed_extjson());
    }
    assert_eq!(got, native, "REST and the driver differ");

    // Aggregation.
    let (s, v) = rest(
        &e,
        &auth,
        DB,
        &m(json!({"aggregate": "users", "pipeline": [{"$match": {"age": 3}}, {"$count": "n"}], "cursor": {}})),
        false,
    )
    .await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(rows(&v), vec![vec![json!(r#"{"n":5}"#)]]);

    // Caps: a truncated cursor.
    let (s, v) = rest(
        &e,
        &auth,
        DB,
        &json!({"language": "mongo", "sql": json!({"find": "users"}).to_string(), "max_rows": 5}),
        true,
    )
    .await;
    assert_eq!(s, 200);
    let lines = v.as_array().unwrap();
    let n: usize = lines
        .iter()
        .filter_map(|l| l["rows"]["rows"].as_array())
        .map(Vec::len)
        .sum();
    let end = lines.iter().find(|l| l.get("end").is_some()).unwrap();
    assert_eq!(
        (n, end["end"]["truncated"].as_bool()),
        (5, Some(true)),
        "{v}"
    );

    // Errors: duplicate key 1062, an unknown command 39795, code refused.
    let (s, v) = rest(
        &e,
        &auth,
        DB,
        &m(json!({"insert": "users", "documents": [{"_id": 1}]})),
        false,
    )
    .await;
    assert_eq!((s, code(&v)), (409, 1062), "{v}");
    let (s, v) = rest(&e, &auth, DB, &m(json!({"noSuchCommand": 1})), false).await;
    assert_eq!((s, code(&v)), (400, 39795), "{v}");
    let (s, v) = rest(
        &e,
        &auth,
        DB,
        &m(json!({"find": "users", "filter": {"$where": "true"}})),
        false,
    )
    .await;
    assert_eq!(s, 400, "$where must be refused: {v}");
    let (s, v) = rest(&e, &auth, DB, &m(json!({"saslStart": 1})), false).await;
    assert_eq!((s, code(&v)), (400, 39794), "{v}");
    let (s, v) = rest(
        &e,
        &auth,
        DB,
        &json!({"language": "mongo", "sql": "db.users.find()"}),
        false,
    )
    .await;
    assert_eq!((s, code(&v)), (400, 39794), "{v}");

    // Same privileges as the MongoDB port: bf4ro can't read users.
    let ro = basic(&format!("{}.{RO_USER}", tenant(&e)), RO_PASSWORD);
    let (s, v) = rest(&e, &ro, DB, &m(json!({"find": "users"})), false).await;
    let ro_url = format!(
        "mongodb://{}.{RO_USER}:{RO_PASSWORD}@{}/?authSource=admin&directConnection=true",
        tenant(&e),
        e.mongo
    );
    let ro_client = mongodb::Client::with_uri_str(&ro_url).await.unwrap();
    let native = ro_client
        .database(DB)
        .collection::<mongodb::bson::Document>("users")
        .find(mongodb::bson::doc! {})
        .await;
    assert!(native.is_err(), "the MongoDB port lets bf4ro read users");
    assert!(s >= 400, "REST lets bf4ro read users: {v}");
    eprintln!(
        "bf4ro over REST: {s} {}; over the MongoDB port: {:?}",
        code(&v),
        native.err().map(|e| e.to_string())
    );

    teardown(&mut w).await;
}
