//! The Vectors API (chapter 34) and the key-value API's gRPC path (chapter
//! 33) through a gateway, over REST (HTTP/1.1) and gRPC (h2c), with the
//! same answers from both.
//!
//! - `SPRIG_TEST_KV_HTTP`: the gateway's HTTP listener (`127.0.0.1:23081`).
//! - `SPRIG_TEST_REDIS_SPRIG`: a tenant login, read from the URL
//!   (`redis://acme.app:pw@...`).
//! - `SPRIG_TEST_REDIS_SPRIG2`: another tenant's login (isolation).
//!
//! Skipped when unset. Uses database `r6vecapi`, dropped at the end.

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
    clippy::cast_precision_loss,
)]

use base64::Engine as _;
use serde_json::{Value as J, json};
use sprig_api::proto::v1 as pb;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::metadata::MetadataValue;

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

fn creds(url: &str) -> String {
    let body = url.trim_start_matches("redis://");
    body.rsplit_once('@').map_or("", |x| x.0).to_owned()
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

fn id64(s: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(s)
}

struct Rng(u64);
impl Rng {
    fn f(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vectors_over_rest_and_grpc() {
    let (Some(addr), Some(url)) = (var("SPRIG_TEST_KV_HTTP"), var("SPRIG_TEST_REDIS_SPRIG")) else {
        eprintln!("SPRIG_TEST_KV_HTTP or SPRIG_TEST_REDIS_SPRIG not set: skipped");
        return;
    };
    let auth = creds(&url);
    let base = "/v1/databases/r6vecapi/vectorIndexes";
    let _ = http(&addr, &auth, "DELETE", &format!("{base}/emb"), None).await;
    let (st, v) = http(&addr, &auth, "POST", base, Some(&json!({"index": "emb", "dimensions": 4, "similarity": "cosine", "ann_mode": "off", "geo": true}))).await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["dimensions"], 4);
    assert_eq!(v["ann_state"], "none");
    // 300 items with metadata and locations around Mumbai.
    let mut rng = Rng(3);
    let mut items = Vec::new();
    let mut data = Vec::new();
    for i in 0..300 {
        let v: Vec<f64> = (0..4).map(|_| rng.f() * 2.0 - 1.0).collect();
        let loc = (19.07 + (rng.f() - 0.5) * 0.5, 72.87 + (rng.f() - 0.5) * 0.5);
        items.push(json!({"id": id64(&format!("p{i:03}")), "vector": v, "metadata": json!({"cat": i % 4, "name": format!("p{i}")}).to_string(), "location": {"latitude": loc.0, "longitude": loc.1}}));
        data.push((format!("p{i:03}"), v, i % 4, loc));
    }
    let (st, v) = http(
        &addr,
        &auth,
        "POST",
        &format!("{base}/emb:upsert"),
        Some(&json!({"items": items})),
    )
    .await;
    assert_eq!((st, v["upserted"].clone()), (200, json!("300")), "{v}");
    let (st, v) = http(&addr, &auth, "GET", &format!("{base}/emb"), None).await;
    assert_eq!((st, v["rows"].clone()), (200, json!("300")), "{v}");

    // REST search, against brute force (cosine), with a filter and a circle.
    let q = [0.3f64, -0.2, 0.9, 0.1];
    let cos = |a: &[f64]| {
        let dot: f64 = a.iter().zip(&q).map(|(x, y)| x * y).sum();
        let na: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
        let nq: f64 = q.iter().map(|x| x * x).sum::<f64>().sqrt();
        1.0 - dot / (na * nq)
    };
    let near = |la: f64, lo: f64| sprig_geo::region::distance_m(19.07, 72.87, la, lo) <= 10_000.0;
    let mut want: Vec<(f64, String)> = data
        .iter()
        .filter(|(_, _, c, (la, lo))| *c == 1 && near(*la, *lo))
        .map(|(id, v, _, _)| (cos(v), id.clone()))
        .collect();
    want.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    want.truncate(5);
    let body = json!({"query": q, "k": 5, "filter": "{\"cat\": 1}", "geo": {"center": {"latitude": 19.07, "longitude": 72.87}, "radius_m": 10000.0}});
    let (st, v) = http(
        &addr,
        &auth,
        "POST",
        &format!("{base}/emb:search"),
        Some(&body),
    )
    .await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["plan"], "prefilter_exact");
    let got: Vec<String> = v["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            String::from_utf8(
                base64::engine::general_purpose::STANDARD
                    .decode(h["id"].as_str().unwrap())
                    .unwrap(),
            )
            .unwrap()
        })
        .collect();
    assert_eq!(got, want.iter().map(|w| w.1.clone()).collect::<Vec<_>>());
    let h0 = &v["hits"][0];
    assert!((h0["distance"].as_f64().unwrap() - want[0].0).abs() < 1e-5);
    assert!(h0["metadata"].as_str().unwrap().contains("\"cat\":1"));
    assert!(h0["location"]["latitude"].is_number());

    // Errors, with chapter 37's status codes.
    let (st, v) = http(
        &addr,
        &auth,
        "POST",
        &format!("{base}/emb:search"),
        Some(&json!({"query": [1, 2], "k": 3})),
    )
    .await;
    assert_eq!((st, v["code"].clone()), (400, json!(39736)), "{v}");
    let (st, v) = http(
        &addr,
        &auth,
        "POST",
        &format!("{base}/nope:search"),
        Some(&json!({"query": q, "k": 3})),
    )
    .await;
    assert_eq!((st, v["code"].clone()), (404, json!(39735)), "{v}");
    let (st, v) = http(&addr, &auth, "POST", &format!("{base}/emb:search"), Some(&json!({"query": q, "k": 3, "geo": {"center": {"latitude": 95, "longitude": 0}, "radius_m": 1}}))).await;
    assert_eq!((st, v["code"].clone()), (400, json!(39765)), "{v}");

    // gRPC on the same port (h2c), the same answer.
    let ch = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let token: MetadataValue<_> = basic(&auth).parse().unwrap();
    let mut vc = pb::vectors_client::VectorsClient::with_interceptor(ch.clone(), {
        let token = token.clone();
        move |mut r: tonic::Request<()>| {
            r.metadata_mut().insert("authorization", token.clone());
            Ok(r)
        }
    });
    let ll = |la: f64, lo: f64| {
        Some(pb::LatLng {
            latitude: la,
            longitude: lo,
        })
    };
    let r = vc
        .search(pb::SearchVectorsRequest {
            database: "r6vecapi".into(),
            index: "emb".into(),
            query: q.iter().map(|x| *x as f32).collect(),
            k: 5,
            filter: "{\"cat\": 1}".into(),
            geo: Some(pb::GeoFilter {
                center: ll(19.07, 72.87),
                radius_m: 10_000.0,
                ..pb::GeoFilter::default()
            }),
            ..pb::SearchVectorsRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.plan, "prefilter_exact");
    assert_eq!(
        r.hits
            .iter()
            .map(|h| String::from_utf8(h.id.clone()).unwrap())
            .collect::<Vec<_>>(),
        got
    );
    // gRPC upsert, delete, a polygon filter and errors as statuses.
    let up = vc
        .upsert(pb::UpsertVectorsRequest {
            database: "r6vecapi".into(),
            index: "emb".into(),
            items: vec![pb::VectorItem {
                id: b"g1".to_vec(),
                vector: vec![0.3, -0.2, 0.9, 0.1],
                metadata: "{\"cat\": 9}".into(),
                location: ll(19.0, 72.8),
            }],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(up.upserted, 1);
    let r = vc
        .search(pb::SearchVectorsRequest {
            database: "r6vecapi".into(),
            index: "emb".into(),
            query: vec![0.3, -0.2, 0.9, 0.1],
            k: 1,
            geo: Some(pb::GeoFilter {
                polygon: vec![
                    pb::LatLng {
                        latitude: 18.9,
                        longitude: 72.7,
                    },
                    pb::LatLng {
                        latitude: 18.9,
                        longitude: 72.9,
                    },
                    pb::LatLng {
                        latitude: 19.1,
                        longitude: 72.8,
                    },
                ],
                ..pb::GeoFilter::default()
            }),
            ..pb::SearchVectorsRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.hits[0].id, b"g1");
    assert!(r.hits[0].score > 0.999);
    let d = vc
        .delete(pb::DeleteVectorsRequest {
            database: "r6vecapi".into(),
            index: "emb".into(),
            ids: vec![b"g1".to_vec(), b"zz".to_vec()],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(d.deleted, 1);
    let e = vc
        .search(pb::SearchVectorsRequest {
            database: "r6vecapi".into(),
            index: "emb".into(),
            query: vec![1.0],
            k: 1,
            ..pb::SearchVectorsRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    assert!(e.message().contains("dimensions"), "{e:?}");

    // Another tenant can't see the index.
    if let Some(u2) = var("SPRIG_TEST_REDIS_SPRIG2") {
        let (st, _) = http(&addr, &creds(&u2), "GET", &format!("{base}/emb"), None).await;
        assert_ne!(st, 200);
    }

    // The key-value API over gRPC (chapter 33): put, get, exec, delete.
    let mut kc = pb::key_value_client::KeyValueClient::with_interceptor(
        ch,
        move |mut r: tonic::Request<()>| {
            r.metadata_mut().insert("authorization", token.clone());
            Ok(r)
        },
    );
    let put = kc
        .put(pb::KvPutRequest {
            database: "r6vecapi".into(),
            namespace: "kvg".into(),
            key: "/u/1".into(),
            text: "hello".into(),
            ttl_ms: 60_000,
            ..pb::KvPutRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(put.version > 0);
    let g = kc
        .get(pb::KvGetRequest {
            database: "r6vecapi".into(),
            namespace: "kvg".into(),
            key: "/u/1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(g.found);
    assert_eq!(g.entry.as_ref().unwrap().text, "hello");
    assert_eq!(g.entry.as_ref().unwrap().version, put.version);
    let x = kc
        .exec(pb::KvExecRequest {
            database: "r6vecapi".into(),
            namespace: "kvg".into(),
            command: vec!["HSET".into(), "h".into(), "f".into(), "v".into()],
            ..pb::KvExecRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(x.reply.and_then(|r| r.kind), Some(prost_types_number(1.0)));
    let stale = kc
        .put(pb::KvPutRequest {
            database: "r6vecapi".into(),
            namespace: "kvg".into(),
            key: "/u/1".into(),
            text: "x".into(),
            if_version: Some(put.version + 1),
            ..pb::KvPutRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);
    let d = kc
        .delete(pb::KvDeleteRequest {
            database: "r6vecapi".into(),
            namespace: "kvg".into(),
            key: "/u/1".into(),
            ..pb::KvDeleteRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(d.deleted);

    let (st, _) = http(&addr, &auth, "DELETE", &format!("{base}/emb"), None).await;
    assert_eq!(st, 200);
    // Drop the test database through SQL over REST.
    let (st, v) = http(
        &addr,
        &auth,
        "POST",
        "/v1/databases/-/sql:execute",
        Some(&json!({"sql": "DROP DATABASE r6vecapi"})),
    )
    .await;
    assert_eq!(st, 200, "{v}");
}

fn prost_types_number(n: f64) -> prost_types::value::Kind {
    prost_types::value::Kind::NumberValue(n)
}
