//! `Search.Query` over REST and gRPC.
//!
//! ```sh
//! export SPRIG_TEST_SEARCH_API=127.0.0.1:23306,127.0.0.1:23080   # MySQL, HTTP
//! cargo test -p sprig-gateway --test search_api_live -- --nocapture
//! ```
//!
//! Login `acme.app` / `sprig-dev-app`. The test makes database
//! `r5seven_api` with a table, its search index and a row-level security
//! policy hiding some rows from the user, then checks:
//!
//! - pages of 4 follow the cursor to the end: every visible matching row
//!   once, best score first, none of the hidden ones;
//! - a filter clause (`range` on a filter field), facets, highlights and
//!   rows; facets count only the rows the user may see,
//!   and every row once the owner is no longer bound (`NO FORCE`);
//! - errors: an unknown index (39820), a bad query (39822), a sort (39823);
//! - gRPC (h2c on the same port) answers the first page as REST does;
//! - a search index on a key-value namespace's JSON values.
//!
//! The database is dropped at the end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::match_same_arms,
    missing_docs
)]

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_api::proto::v1::SearchQueryRequest;
use sprig_api::proto::v1::search_client::SearchClient;

const VAR: &str = "SPRIG_TEST_SEARCH_API";
const USER: &str = "acme.app";
const PASSWORD: &str = "sprig-dev-app";

async fn post(addr: &str, path: &str, body: &Value) -> (u16, Value) {
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{USER}:{PASSWORD}"))
    );
    let req = hyper::Request::builder()
        .method("POST")
        .uri(path)
        .header("host", addr)
        .header("authorization", auth)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let resp = tx.send_request(req).await.unwrap();
    let status = resp.status().as_u16();
    let b = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

fn ids(v: &Value) -> Vec<i64> {
    v["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            let k: Value = serde_json::from_str(h["key"].as_str().unwrap()).unwrap();
            k["id"].as_i64().unwrap()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_query_over_rest_and_grpc() {
    let Ok(v) = std::env::var(VAR) else {
        eprintln!("{VAR} not set; skipped");
        return;
    };
    let (mysql, http) = v.split_once(',').expect("mysql,http");
    let url = format!("mysql://{USER}:{PASSWORD}@{mysql}");
    let pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(&url).unwrap()).pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, 2).unwrap())
                .with_inactive_connection_ttl(std::time::Duration::from_secs(60)),
        ),
    );
    let mut c = pool.get_conn().await.unwrap();
    for s in [
        "DROP DATABASE IF EXISTS r5seven_api",
        "CREATE DATABASE r5seven_api",
        "USE r5seven_api",
        "CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(200), brand VARCHAR(40), price INT)",
    ] {
        c.query_drop(s).await.unwrap();
    }
    // 24 items; every name says "shoe", some twice (they score higher);
    // brand 'hidden' rows are hidden from the user by a policy.
    let mut rows = Vec::new();
    for i in 1..=24 {
        let brand = match i % 4 {
            0 => "hidden",
            1 => "acme",
            2 => "zeta",
            _ => "acme",
        };
        let name = if i % 3 == 0 {
            format!("red running shoe shoe number {i}")
        } else {
            format!("blue walking shoe number {i}")
        };
        rows.push(format!("({i}, '{name}', '{brand}', {})", i * 5));
    }
    c.query_drop(format!("INSERT INTO items VALUES {}", rows.join(", ")))
        .await
        .unwrap();
    c.query_drop(
        "CREATE SEARCH INDEX items_text ON items (name TEXT, brand KEYWORD WITH (facet = true, filter = true), \
         price NUMBER WITH (filter = true))",
    )
    .await
    .unwrap();
    for s in [
        "CREATE POLICY no_hidden ON items AS PERMISSIVE FOR ALL TO PUBLIC USING (brand <> 'hidden')",
        "ALTER TABLE items ENABLE ROW LEVEL SECURITY",
        "ALTER TABLE items FORCE ROW LEVEL SECURITY",
    ] {
        c.query_drop(s).await.unwrap();
    }
    let visible: Vec<i64> = c
        .query("SELECT id FROM items WHERE name LIKE '%shoe%' ORDER BY id")
        .await
        .unwrap();
    assert_eq!(visible.len(), 18, "the policy hides 6 rows: {visible:?}");
    let path = "/v1/databases/r5seven_api/search/items_text:query";

    // Pages of 4 through the cursor.
    let mut got: Vec<i64> = Vec::new();
    let mut scores: Vec<f64> = Vec::new();
    let mut after = String::new();
    let mut pages = 0;
    loop {
        let (st, v) = post(
            http,
            path,
            &json!({"query_string": "shoe", "limit": 4, "after": after, "consistency": "latest"}),
        )
        .await;
        assert_eq!(st, 200, "{v}");
        pages += 1;
        got.extend(ids(&v));
        scores.extend(
            v["hits"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| h["score"].as_f64().unwrap()),
        );
        after = v["after"].as_str().unwrap_or("").to_owned();
        if after.is_empty() {
            break;
        }
        assert!(pages < 20, "the cursor never ends");
    }
    let mut sorted = got.clone();
    sorted.sort_unstable();
    assert_eq!(
        sorted, visible,
        "pages: every visible row once, no hidden one"
    );
    assert!(
        scores.windows(2).all(|w| w[0] >= w[1]),
        "best first: {scores:?}"
    );
    // "shoe shoe" rows (i % 3 == 0) rank above the others.
    let doubles = visible.iter().filter(|i| *i % 3 == 0).count();
    assert!(got[..doubles].iter().all(|i| i % 3 == 0), "{got:?}");
    println!(
        "Search.Query: {} hits in {pages} pages of 4, best first, 6 hidden rows left out",
        got.len()
    );

    // Filter, facets, highlights, rows.
    let (st, v) = post(
        http,
        path,
        &json!({"query": r#"{"text": {"query": "running", "path": "name"}}"#,
                "filter": r#"{"range": {"path": "price", "lte": 60}}"#,
                "facets": r#"{"brands": {"type": "string", "path": "brand", "numBuckets": 5}}"#,
                "highlight": ["name"], "include_row": true, "limit": 50, "consistency": "latest"}),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    let want: Vec<i64> = visible
        .iter()
        .copied()
        .filter(|i| i % 3 == 0 && i * 5 <= 60)
        .collect();
    let mut f = ids(&v);
    f.sort_unstable();
    assert_eq!(f, want, "{v}");
    let h0 = &v["hits"][0];
    let hl: Value = serde_json::from_str(h0["highlights"].as_str().unwrap()).unwrap();
    assert!(
        hl["name"][0]["text"].as_str().unwrap().contains("running"),
        "{hl}"
    );
    let row: Value = serde_json::from_str(h0["row"].as_str().unwrap()).unwrap();
    assert!(row["name"].as_str().unwrap().contains("running"), "{row}");
    // Facets count only the rows the user may see. The
    // matches are rows 3, 6, 9 and 12; row 12 ('hidden') is hidden by the
    // policy, so 'hidden' must not appear and acme counts 2, zeta 1.
    let facets: Value = serde_json::from_str(v["facets"].as_str().unwrap()).unwrap();
    let buckets = |f: &Value| -> Vec<(String, i64)> {
        let mut b: Vec<(String, i64)> = f["brands"]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| {
                (
                    x["_id"].as_str().unwrap().to_owned(),
                    x["count"].as_i64().unwrap(),
                )
            })
            .collect();
        b.sort();
        b
    };
    assert_eq!(
        buckets(&facets),
        vec![("acme".to_owned(), 2), ("zeta".to_owned(), 1)],
        "facets over visible rows only: {facets}"
    );
    assert!(!v["applied"].as_str().unwrap_or("").is_empty());
    println!("Search.Query: filter {} hits, facets {facets}", f.len());
    // Without FORCE the owner manages the table and isn't bound: the
    // copy's counts, which then are exact, include row 12.
    let facet_body = json!({"query": r#"{"text": {"query": "running", "path": "name"}}"#,
        "filter": r#"{"range": {"path": "price", "lte": 60}}"#,
        "facets": r#"{"brands": {"type": "string", "path": "brand", "numBuckets": 5}}"#,
        "limit": 50, "consistency": "latest"});
    c.query_drop("ALTER TABLE items NO FORCE ROW LEVEL SECURITY")
        .await
        .unwrap();
    let (st, v) = post(http, path, &facet_body).await;
    assert_eq!(st, 200, "{v}");
    let unbound: Value = serde_json::from_str(v["facets"].as_str().unwrap()).unwrap();
    assert_eq!(
        buckets(&unbound),
        vec![
            ("acme".to_owned(), 2),
            ("hidden".to_owned(), 1),
            ("zeta".to_owned(), 1)
        ],
        "an unbound owner sees every row: {unbound}"
    );
    // EXPLAIN of a MATCH shows MariaDB's plan and then the
    // search: a SEARCH row naming the index, with its hits and SearchScan.
    let plan: Vec<mysql_async::Row> = c
        .query("EXPLAIN SELECT id FROM items WHERE MATCH (name) AGAINST ('running')")
        .await
        .unwrap();
    let search_row = plan
        .iter()
        .find(|r| r.get::<String, _>("select_type").as_deref() == Some("SEARCH"))
        .expect("a SEARCH row");
    assert_eq!(search_row.get::<String, _>("key").unwrap(), "items_text");
    assert_eq!(search_row.get::<String, _>("type").unwrap(), "fulltext");
    // "running" is in rows 3, 6, ..., 24: 8 rows, every one visible here.
    assert_eq!(search_row.get::<String, _>("rows").unwrap(), "8");
    let extra = search_row.get::<String, _>("Extra").unwrap();
    assert!(
        extra.starts_with("SearchScan(index r5seven_api.items_text") && extra.contains("8 hits"),
        "{extra}"
    );
    assert!(plan.len() >= 2, "MariaDB's plan comes first");
    println!("EXPLAIN: {extra}");
    c.query_drop("ALTER TABLE items FORCE ROW LEVEL SECURITY")
        .await
        .unwrap();
    // A bound user gets no EXPLAIN of the protected table.
    let e = c
        .query_drop("EXPLAIN SELECT id FROM items WHERE MATCH (name) AGAINST ('running')")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("39366"), "{e}");
    let (st, v) = post(http, path, &facet_body).await;
    assert_eq!(st, 200, "{v}");
    let again: Value = serde_json::from_str(v["facets"].as_str().unwrap()).unwrap();
    assert_eq!(buckets(&again), buckets(&facets), "{again}");
    println!("Search.Query facets: bound {facets}, unbound owner {unbound}");

    // Errors.
    for (body, code) in [
        (json!({"query_string": "shoe"}), 0),
        (json!({"query_string": "\"unclosed"}), 39822),
        (json!({"query_string": "shoe", "sort": "price"}), 39823),
    ] {
        let p = if code == 0 {
            "/v1/databases/r5seven_api/search/nope:query"
        } else {
            path
        };
        let (st, v) = post(http, p, &body).await;
        assert!(st >= 400, "{v}");
        let text = v.to_string();
        let want = if code == 0 {
            "39820".to_owned()
        } else {
            code.to_string()
        };
        assert!(text.contains(&want), "{want}: {text}");
    }

    // gRPC: the same first page.
    let ch = tonic::transport::Endpoint::from_shared(format!("http://{http}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut g = SearchClient::new(ch);
    let mut req = tonic::Request::new(SearchQueryRequest {
        database: "r5seven_api".into(),
        index: "items_text".into(),
        query_string: "shoe".into(),
        limit: 4,
        consistency: "latest".into(),
        ..SearchQueryRequest::default()
    });
    req.metadata_mut().insert(
        "authorization",
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{USER}:{PASSWORD}"))
        )
        .parse()
        .unwrap(),
    );
    let r = g.query(req).await.unwrap().into_inner();
    let gids: Vec<i64> = r
        .hits
        .iter()
        .map(|h| {
            let k: Value = serde_json::from_str(&h.key).unwrap();
            k["id"].as_i64().unwrap()
        })
        .collect();
    assert_eq!(gids, got[..4].to_vec());
    assert!(!r.after.is_empty());
    println!("Search.Query over gRPC: first page equal to REST's");

    // A key-value namespace: JSON values by path.
    for (k, note) in [
        ("s1", "remember the red shoe"),
        ("s2", "buy milk"),
        ("s3", "the shoe store closes at six"),
    ] {
        let (st, v) = post(
            http,
            "/v1/databases/r5seven_api/namespaces/notes:put",
            &json!({"key": k, "text": json!({"note": note, "n": 1}).to_string()}),
        )
        .await;
        assert_eq!(st, 200, "{v}");
    }
    let (st, v) = post(
        http,
        "/v1/databases/r5seven_api/namespaces/notes:put",
        &json!({"key": "plain", "text": "a shoe without JSON"}),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    c.query_drop("CREATE SEARCH INDEX note_text ON notes (VALUE '$.note' TEXT)")
        .await
        .unwrap();
    let (st, v) = post(
        http,
        "/v1/databases/r5seven_api/search/note_text:query",
        &json!({"query_string": "shoe", "include_row": true, "highlight": ["note"], "consistency": "latest"}),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    let mut keys: Vec<String> = v["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            let k: Value = serde_json::from_str(h["key"].as_str().unwrap()).unwrap();
            k["k"].as_str().unwrap().to_owned()
        })
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["s1", "s3"], "{v}");
    // A later write is found too.
    let (st, _) = post(
        http,
        "/v1/databases/r5seven_api/namespaces/notes:put",
        &json!({"key": "s2", "text": json!({"note": "new shoe laces"}).to_string()}),
    )
    .await;
    assert_eq!(st, 200);
    let (_, v) = post(
        http,
        "/v1/databases/r5seven_api/search/note_text:query",
        &json!({"query_string": "laces", "consistency": "latest"}),
    )
    .await;
    assert_eq!(v["hits"].as_array().map(Vec::len), Some(1), "{v}");
    println!("Search.Query on a key-value namespace: JSON values by path, writes followed");
    c.query_drop("DROP DATABASE r5seven_api").await.unwrap();
}
