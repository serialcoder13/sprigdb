//! The Analytics API through a gateway (chapters 35 and 37): models and
//! queries over REST and gRPC on one port, compared with the same data's
//! `GROUP BY ... WITH ROLLUP` over the SQL API.
//!
//! - `SPRIG_TEST_KV_HTTP`: the gateway's HTTP listener (`127.0.0.1:23081`).
//! - `SPRIG_TEST_REDIS_SPRIG`: a tenant login, read from the URL
//!   (`redis://acme.app:pw@...`).
//!
//! Skipped when unset. Uses database `r6dimapi`, dropped at the end.

#![allow(
    clippy::format_push_string,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::type_complexity,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::items_after_statements,
    clippy::needless_update,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
)]

use base64::Engine as _;
use serde_json::{Value as J, json};
use sprig_api::proto::v1 as pb;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::metadata::MetadataValue;

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

fn basic(c: &str) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(c)
    )
}

async fn http(addr: &str, auth: &str, method: &str, path: &str, body: Option<&J>) -> (u16, J) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let b = body.map(ToString::to_string).unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
        basic(auth),
        b.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    let text = String::from_utf8_lossy(&out).into_owned();
    let status: u16 = text
        .split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let (head, rest) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut b = String::new();
        let mut r = rest;
        while let Some((len, more)) = r.split_once("\r\n") {
            let n = usize::from_str_radix(len.trim(), 16).unwrap_or(0);
            if n == 0 {
                break;
            }
            b.push_str(&more[..n]);
            r = &more[n + 2..];
        }
        b
    } else {
        rest.to_owned()
    };
    (
        status,
        serde_json::from_str(&body).unwrap_or(J::String(body)),
    )
}

async fn sql(addr: &str, auth: &str, text: &str) -> J {
    let (st, v) = http(
        addr,
        auth,
        "POST",
        "/v1/databases/r6dimapi/sql:execute",
        Some(&json!({"sql": text, "multi_statements": true})),
    )
    .await;
    assert_eq!(st, 200, "{text}: {v}");
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn analytics_over_rest_and_grpc() {
    let (Some(addr), Some(url)) = (var("SPRIG_TEST_KV_HTTP"), var("SPRIG_TEST_REDIS_SPRIG")) else {
        eprintln!("SPRIG_TEST_KV_HTTP or SPRIG_TEST_REDIS_SPRIG not set: skipped");
        return;
    };
    let auth = url
        .trim_start_matches("redis://")
        .rsplit_once('@')
        .unwrap()
        .0
        .to_owned();
    let (st, v) = http(&addr, &auth, "POST", "/v1/databases/-/sql:execute", Some(&json!({"sql": "DROP DATABASE IF EXISTS r6dimapi; CREATE DATABASE r6dimapi", "multi_statements": true}))).await;
    assert_eq!(st, 200, "{v}");
    sql(&addr, &auth, "CREATE TABLE store (store_id INT PRIMARY KEY, country VARCHAR(8) NOT NULL, region VARCHAR(16) NOT NULL); \
        CREATE TABLE sale (id INT PRIMARY KEY, store_id INT NOT NULL, sold_at DATETIME NOT NULL, amount DECIMAL(10,2) NOT NULL, qty INT NOT NULL)").await;
    let mut stores = Vec::new();
    for s in 1..=6 {
        stores.push(format!(
            "({s}, '{}', '{}r{}')",
            ["in", "us"][s % 2],
            ["in", "us"][s % 2],
            s % 3
        ));
    }
    let mut sales = Vec::new();
    for i in 0..400u32 {
        sales.push(format!(
            "({i}, {}, '2026-0{}-1{} 10:00:00', {}.{:02}, {})",
            i % 6 + 1,
            i % 6 + 1,
            i % 9,
            (i * 37) % 300,
            (i * 13) % 100,
            i % 7 + 1
        ));
    }
    sql(
        &addr,
        &auth,
        &format!(
            "INSERT INTO store VALUES {}; INSERT INTO sale VALUES {}",
            stores.join(", "),
            sales.join(", ")
        ),
    )
    .await;
    let base = "/v1/databases/r6dimapi/models";
    let def = json!({"model": "sales", "time_zone": "UTC",
        "dimensions": [{"name": "store", "table": "store", "key": "store_id", "levels": ["country", "region"]},
                       {"name": "date", "builtin": "time"}],
        "facts": [{"name": "sales", "table": "sale", "key": ["id"],
            "links": [{"dimension": "store", "column": "store_id"}, {"dimension": "date", "column": "sold_at"}],
            "measures": [{"name": "revenue", "expr": "amount", "agg": "sum"},
                         {"name": "avg_qty", "expr": "qty", "agg": "avg"},
                         {"name": "stores", "expr": "store_id", "agg": "count_distinct"}]}]});
    let (st, v) = http(
        &addr,
        &auth,
        "POST",
        base,
        Some(&json!({"definition": def})),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(
        (v["model"].clone(), v["version"].clone(), v["state"].clone()),
        (json!("sales"), json!("1"), json!("active"))
    );

    // REST query with totals against WITH ROLLUP over the SQL API.
    let q = json!({"measures": ["revenue", "avg_qty", "stores"], "totals": true,
        "dimensions": [{"dimension": "store", "level": "country"}, {"dimension": "date", "level": "month"}],
        "filters": [{"dimension": "date", "level": "month", "op": "between", "values": ["2026-02", "2026-05"]}]});
    let (st, v) = http(
        &addr,
        &auth,
        "POST",
        &format!("{base}/sales:query"),
        Some(&q),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["plan"], "per_level");
    let have: Vec<Vec<J>> = v["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            r["keys"]
                .as_array()
                .unwrap()
                .iter()
                .chain(r["values"].as_array().unwrap())
                .cloned()
                .collect()
        })
        .collect();
    let reference = sql(&addr, &auth, "SELECT d1.country, EXTRACT(YEAR_MONTH FROM f.sold_at) AS m, SUM(f.amount), AVG(f.qty), COUNT(DISTINCT f.store_id) \
        FROM sale f LEFT JOIN store d1 ON d1.store_id = f.store_id WHERE f.sold_at >= '2026-02-01' AND f.sold_at < '2026-06-01' \
        GROUP BY d1.country, m WITH ROLLUP").await;
    let want: Vec<Vec<J>> = reference["results"][0]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r.as_array().unwrap().clone())
        .collect();
    assert_eq!(have, want);
    assert_eq!(v["rows"].as_array().unwrap().last().unwrap()["grouping"], 3);

    // gRPC: the same answer, explain and drill.
    let ch = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let token: MetadataValue<_> = basic(&auth).parse().unwrap();
    let mut ac = pb::analytics_client::AnalyticsClient::with_interceptor(
        ch,
        move |mut r: tonic::Request<()>| {
            r.metadata_mut().insert("authorization", token.clone());
            Ok(r)
        },
    );
    let lv = |vals: &[&str]| prost_types::ListValue {
        values: vals
            .iter()
            .map(|x| prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue((*x).to_owned())),
            })
            .collect(),
    };
    let gq = pb::AnalyticsQuery {
        database: "r6dimapi".into(),
        model: "sales".into(),
        measures: vec!["revenue".into(), "avg_qty".into(), "stores".into()],
        dimensions: vec![
            pb::LevelRef {
                dimension: "store".into(),
                level: "country".into(),
            },
            pb::LevelRef {
                dimension: "date".into(),
                level: "month".into(),
            },
        ],
        filters: vec![pb::AnalyticsFilter {
            dimension: "date".into(),
            level: "month".into(),
            op: "between".into(),
            values: Some(lv(&["2026-02", "2026-05"])),
        }],
        totals: true,
        ..pb::AnalyticsQuery::default()
    };
    let r = ac.query(gq.clone()).await.unwrap().into_inner();
    let grpc_rows: Vec<Vec<J>> = r
        .rows
        .iter()
        .map(|row| {
            row.keys
                .as_ref()
                .unwrap()
                .values
                .iter()
                .chain(&row.values.as_ref().unwrap().values)
                .map(|x| match &x.kind {
                    Some(prost_types::value::Kind::StringValue(s)) => json!(s),
                    _ => J::Null,
                })
                .collect()
        })
        .collect();
    assert_eq!(grpc_rows, want);
    let ex = ac.explain(gq.clone()).await.unwrap().into_inner();
    assert_eq!(ex.plan, "per_level");
    assert_eq!(ex.sql.len(), 3);
    assert!(ex.sql[0].contains("GROUP BY k1, k2"), "{}", ex.sql[0]);
    let down = ac
        .drill(pb::DrillRequest {
            database: "r6dimapi".into(),
            model: "sales".into(),
            query: Some(gq.clone()),
            dimension: "store".into(),
            member: Some(prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue("in".into())),
            }),
            up: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(down.dimensions[0].level, "region");
    assert_eq!(down.filters.last().unwrap().level, "country");
    // Ordered by revenue, limited, without totals.
    let (st, v) = http(&addr, &auth, "POST", &format!("{base}/sales:query"), Some(&json!({"measures": ["revenue"],
        "dimensions": [{"dimension": "store", "level": "region"}], "order": [{"measure": "revenue", "desc": true}], "limit": 2}))).await;
    assert_eq!(st, 200, "{v}");
    let reference = sql(&addr, &auth, "SELECT d1.region, SUM(f.amount) s FROM sale f LEFT JOIN store d1 ON d1.store_id = f.store_id GROUP BY d1.region ORDER BY s DESC LIMIT 2").await;
    let want: Vec<J> = reference["results"][0]["rows"].as_array().unwrap().clone();
    let have: Vec<J> = v["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| json!([r["keys"][0], r["values"][0]]))
        .collect();
    assert_eq!(have, want);

    // Models: list, update, errors, drop.
    let (st, v) = http(&addr, &auth, "GET", base, None).await;
    assert_eq!(
        (st, v["models"].as_array().map(Vec::len)),
        (200, Some(1)),
        "{v}"
    );
    let mut def2 = def.clone();
    def2["facts"][0]["measures"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name": "units", "expr": "qty", "agg": "sum"}));
    let (st, v) = http(
        &addr,
        &auth,
        "PUT",
        &format!("{base}/sales"),
        Some(&json!({"definition": def2})),
    )
    .await;
    assert_eq!((st, v["version"].clone()), (200, json!("2")), "{v}");
    let (st, v) = http(
        &addr,
        &auth,
        "POST",
        &format!("{base}/sales:query"),
        Some(&json!({"measures": ["nope"]})),
    )
    .await;
    assert_eq!((st, v["code"].clone()), (400, json!(39751)), "{v}");
    let (st, v) = http(&addr, &auth, "GET", &format!("{base}/nope"), None).await;
    assert_eq!((st, v["code"].clone()), (404, json!(39750)), "{v}");
    let mut broken = def.clone();
    broken["model"] = json!("broken");
    broken["dimensions"][0]["table"] = json!("no_such_table");
    let (st, v) = http(
        &addr,
        &auth,
        "POST",
        base,
        Some(&json!({"definition": broken})),
    )
    .await;
    assert_eq!((st, v["code"].clone()), (400, json!(39752)), "{v}");
    let (st, _) = http(&addr, &auth, "DELETE", &format!("{base}/sales"), None).await;
    assert_eq!(st, 200);
    let (st, v) = http(
        &addr,
        &auth,
        "POST",
        "/v1/databases/-/sql:execute",
        Some(&json!({"sql": "DROP DATABASE r6dimapi"})),
    )
    .await;
    assert_eq!(st, 200, "{v}");
}
