//! Live checks through a SprigDB gateway: logins and
//! tenant isolation on the Redis listener, the key-value API over REST,
//! and SQL reads of the same data.
//!
//! - `SPRIG_TEST_REDIS_SPRIG`, `SPRIG_TEST_REDIS_SPRIG2`: the Redis
//!   listener as two tenants' logins (`redis://acme.app:pw@127.0.0.1:26379`).
//! - `SPRIG_TEST_KV_HTTP`: the HTTP listener (`127.0.0.1:26080`), used with
//!   the first URL's login.
//! - `SPRIG_TEST_KV_SQL`: the MySQL listener as the first tenant
//!   (`mysql://acme.app:pw@127.0.0.1:26306`).
//!
//! Skipped when unset. Uses database `r6kv` and Redis keys `r6live:*`,
//! removed at the end.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use base64_lite::encode;
use mysql_async::prelude::Queryable;
use redis::Value;
use serde_json::{Value as J, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) mod base64_lite {
    /// Standard base64 with padding.
    pub(crate) fn encode(b: &[u8]) -> String {
        const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut s = String::new();
        for c in b.chunks(3) {
            let n = (u32::from(c[0]) << 16)
                | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
                | u32::from(*c.get(2).unwrap_or(&0));
            for i in 0..4 {
                if i <= c.len() {
                    s.push(char::from(A[((n >> (18 - 6 * i)) & 63) as usize]));
                } else {
                    s.push('=');
                }
            }
        }
        s
    }
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

fn creds(url: &str) -> (String, String) {
    let body = url.trim_start_matches("redis://");
    let userinfo = body.rsplit_once('@').map_or("", |x| x.0);
    let (u, p) = userinfo.split_once(':').unwrap_or(("", userinfo));
    (u.to_owned(), p.to_owned())
}

async fn http(addr: &str, auth: &str, path: &str, body: &J) -> (u16, J) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let b = body.to_string();
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Basic {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
        encode(auth.as_bytes()),
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
    // Chunked bodies: take the first chunk.
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        rest.split_once("\r\n")
            .map_or("", |x| x.1)
            .rsplit_once("\r\n0\r\n")
            .map_or(rest, |x| x.0)
            .to_owned()
    } else {
        rest.to_owned()
    };
    (
        status,
        serde_json::from_str(&body).unwrap_or(J::String(body)),
    )
}

async fn cmd(
    c: &mut redis::aio::MultiplexedConnection,
    args: &[&str],
) -> redis::RedisResult<Value> {
    let mut q = redis::cmd(args[0]);
    for a in &args[1..] {
        q.arg(*a);
    }
    q.query_async::<Value>(c).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logins_isolation_api_and_sql() {
    let (Some(u1), Some(u2)) = (
        var("SPRIG_TEST_REDIS_SPRIG"),
        var("SPRIG_TEST_REDIS_SPRIG2"),
    ) else {
        eprintln!("SPRIG_TEST_REDIS_SPRIG(2) not set: skipped");
        return;
    };
    let host = u1.rsplit('@').next().unwrap().to_owned();
    // Logins: NOAUTH, WRONGPASS, one-argument AUTH without a default user.
    {
        let mut s = tokio::net::TcpStream::connect(&host).await.unwrap();
        let mut buf = vec![0u8; 512];
        for (send, want) in [
            ("GET x\r\n", "-NOAUTH Authentication required."),
            (
                "AUTH acme.app wrong\r\n",
                "-WRONGPASS invalid username-password pair or user is disabled.",
            ),
            (
                "AUTH onlypass\r\n",
                "-ERR AUTH <password> called without any password configured",
            ),
            ("PING\r\n", "-NOAUTH"),
        ] {
            s.write_all(send.as_bytes()).await.unwrap();
            let n = s.read(&mut buf).await.unwrap();
            let got = String::from_utf8_lossy(&buf[..n]).into_owned();
            assert!(got.starts_with(want), "{send:?}: {got:?}");
        }
    }
    let mut a = redis::Client::open(u1.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let mut b = redis::Client::open(u2.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    // Tenants don't see each other's keys or channels.
    cmd(&mut a, &["SET", "r6live:iso", "acme"]).await.unwrap();
    assert_eq!(
        cmd(&mut b, &["GET", "r6live:iso"]).await.unwrap(),
        Value::Nil
    );
    cmd(&mut b, &["SET", "r6live:iso", "globex"]).await.unwrap();
    assert_eq!(
        cmd(&mut a, &["GET", "r6live:iso"]).await.unwrap(),
        Value::BulkString(b"acme".to_vec())
    );
    assert_eq!(
        cmd(&mut b, &["PUBLISH", "r6live:ch", "x"]).await.unwrap(),
        Value::Int(0)
    );
    let who = cmd(&mut a, &["ACL", "WHOAMI"]).await.unwrap();
    eprintln!("ACL WHOAMI: {who:?}");
    cmd(&mut a, &["HSET", "r6live:h", "f1", "v1", "f2", "v2"])
        .await
        .unwrap();
    cmd(&mut a, &["ZADD", "r6live:z", "1.5", "m1", "-inf", "m0"])
        .await
        .unwrap();

    // The key-value API over REST.
    if let Some(http_addr) = var("SPRIG_TEST_KV_HTTP") {
        let (user, pw) = creds(&u1);
        let auth = format!("{user}:{pw}");
        let p = |rpc: &str| format!("/v1/databases/r6kv/namespaces/cache:{rpc}");
        let (st, v) = http(
            &http_addr,
            &auth,
            &p("put"),
            &json!({"key": "user:7", "text": "Asha"}),
        )
        .await;
        assert_eq!(st, 200, "{v}");
        let ver = v["version"].as_str().unwrap().to_owned();
        let (_, v) = http(&http_addr, &auth, &p("get"), &json!({"key": "user:7"})).await;
        assert_eq!(v["entry"]["text"], "Asha");
        assert_eq!(v["entry"]["version"].as_str().unwrap(), ver);
        // Compare-and-set: a stale version is refused with the current one.
        let (st, v) = http(
            &http_addr,
            &auth,
            &p("put"),
            &json!({"key": "user:7", "text": "B", "if_version": "1"}),
        )
        .await;
        eprintln!("stale put: {st} {v}");
        assert!(v.to_string().contains("39721"), "{v}");
        let (st, _) = http(
            &http_addr,
            &auth,
            &p("put"),
            &json!({"key": "user:7", "text": "B", "if_version": ver}),
        )
        .await;
        assert_eq!(st, 200);
        let (st, v) = http(
            &http_addr,
            &auth,
            &p("put"),
            &json!({"key": "user:7", "text": "C", "if_absent": true}),
        )
        .await;
        assert!(v.to_string().contains("39722"), "{st} {v}");
        for i in 0..5 {
            http(
                &http_addr,
                &auth,
                &p("put"),
                &json!({"key": format!("/users/{i}"), "text": format!("u{i}")}),
            )
            .await;
        }
        let (_, v) = http(
            &http_addr,
            &auth,
            &p("scan"),
            &json!({"prefix": "/users/", "limit": 3}),
        )
        .await;
        assert_eq!(v["entries"].as_array().unwrap().len(), 3, "{v}");
        let tok = v["next_page_token"].as_str().unwrap().to_owned();
        let (_, v) = http(
            &http_addr,
            &auth,
            &p("scan"),
            &json!({"prefix": "/users/", "page_token": tok, "limit": 3}),
        )
        .await;
        assert_eq!(v["entries"].as_array().unwrap().len(), 2, "{v}");
        assert_eq!(v["next_page_token"], "");
        // Exec: structures, replies as JSON.
        let (_, v) = http(
            &http_addr,
            &auth,
            &p("exec"),
            &json!({"command": ["HSET", "h1", "name", "Asha", "city", "Pune"]}),
        )
        .await;
        assert_eq!(v["reply"], 2, "{v}");
        let (_, v) = http(
            &http_addr,
            &auth,
            &p("exec"),
            &json!({"command": ["HGETALL", "h1"]}),
        )
        .await;
        assert_eq!(v["reply"], json!({"city": "Pune", "name": "Asha"}), "{v}");
        let (_, v) = http(
            &http_addr,
            &auth,
            &p("exec"),
            &json!({"command": ["INCR", "h1"]}),
        )
        .await;
        assert!(
            v["reply"]["error"]
                .as_str()
                .unwrap()
                .starts_with("WRONGTYPE"),
            "{v}"
        );
        let (_, v) = http(&http_addr, &auth, &p("get"), &json!({"key": "h1"})).await;
        assert!(v.to_string().contains("39730"), "{v}");
        let (st, v) = http(
            &http_addr,
            &auth,
            "/v1/databases/r6kv/namespaces/bad-name:get",
            &json!({"key": "x"}),
        )
        .await;
        assert_eq!(st, 404, "{v}");
        let (_, v) = http(&http_addr, &auth, &p("delete"), &json!({"key": "user:7"})).await;
        assert_eq!(v["deleted"], true);
        let (_, v) = http(&http_addr, &auth, &p("get"), &json!({"key": "user:7"})).await;
        assert_eq!(v["found"], false);
        eprintln!("REST key-value checks passed");
    }

    // SQL reads the same rows through the SQL view.
    if let Some(sql) = var("SPRIG_TEST_KV_SQL") {
        let mut c = mysql_async::Conn::new(mysql_async::Opts::from_url(&sql).unwrap())
            .await
            .unwrap();
        let v: Option<Vec<u8>> = c
            .query_first("SELECT v FROM redis.db0 WHERE k = 'r6live:iso'")
            .await
            .unwrap();
        assert_eq!(v.as_deref(), Some(&b"acme"[..]));
        let fields: Vec<(String, String)> = c
            .query("SELECT field, value FROM redis.db0__hash WHERE k = 'r6live:h' ORDER BY field")
            .await
            .unwrap();
        assert_eq!(
            fields,
            vec![("f1".into(), "v1".into()), ("f2".into(), "v2".into())]
        );
        let zs: Vec<(String, Option<f64>, i8)> = c
            .query("SELECT member, score, score_class FROM redis.db0__zset WHERE k = 'r6live:z' ORDER BY score_class, score")
            .await
            .unwrap();
        assert_eq!(
            zs,
            vec![("m0".into(), None, -1), ("m1".into(), Some(1.5), 0)]
        );
        if var("SPRIG_TEST_KV_HTTP").is_some() {
            let n: Option<i64> = c
                .query_first("SELECT COUNT(*) FROM r6kv.cache WHERE k LIKE '/users/%'")
                .await
                .unwrap();
            assert_eq!(n, Some(5));
            let h: Vec<(String, String)> = c
                .query("SELECT field, value FROM r6kv.cache__hash WHERE k = 'h1' ORDER BY field")
                .await
                .unwrap();
            assert_eq!(h.len(), 2);
            c.query_drop("DROP DATABASE r6kv").await.unwrap();
        }
        c.disconnect().await.unwrap();
        eprintln!("SQL checks passed");
    }
    cmd(&mut a, &["DEL", "r6live:iso", "r6live:h", "r6live:z"])
        .await
        .unwrap();
    cmd(&mut b, &["DEL", "r6live:iso"]).await.unwrap();
}

/// Small throughput and latency sample (not a benchmark: a debug build on a
/// shared laptop). `SPRIG_TEST_REDIS_BENCH=1` with `SPRIG_TEST_REDIS_SPRIG`
/// (and `SPRIG_TEST_REDIS_REF` for the reference).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn throughput_sample() {
    if var("SPRIG_TEST_REDIS_BENCH").is_none() {
        eprintln!("SPRIG_TEST_REDIS_BENCH not set: skipped");
        return;
    }
    let mut targets = Vec::new();
    if let Some(u) = var("SPRIG_TEST_REDIS_SPRIG") {
        targets.push(("sprig", u));
    }
    if let Some(r) = var("SPRIG_TEST_REDIS_REF") {
        targets.push(("redis", format!("redis://{r}")));
    }
    for (name, url) in targets {
        for op in ["SET", "GET", "INCR", "LPUSH"] {
            let conns = 8;
            let per = 250;
            let start = std::time::Instant::now();
            let mut tasks = Vec::new();
            for c in 0..conns {
                let url = url.clone();
                tasks.push(tokio::spawn(async move {
                    let mut con = redis::Client::open(url.as_str())
                        .unwrap()
                        .get_multiplexed_async_connection()
                        .await
                        .unwrap();
                    let mut lat = Vec::with_capacity(per);
                    for i in 0..per {
                        let k = format!("r6bench:{c}:{}", i % 50);
                        let t = std::time::Instant::now();
                        match op {
                            "SET" => cmd(&mut con, &["SET", &k, "value-0123456789"])
                                .await
                                .unwrap(),
                            "GET" => cmd(&mut con, &["GET", &k]).await.unwrap(),
                            "INCR" => cmd(&mut con, &["INCR", &format!("r6bench:n:{c}")])
                                .await
                                .unwrap(),
                            _ => cmd(&mut con, &["LPUSH", &format!("r6bench:l:{c}"), "x"])
                                .await
                                .unwrap(),
                        };
                        lat.push(t.elapsed().as_micros());
                    }
                    lat
                }));
            }
            let mut all = Vec::new();
            for t in tasks {
                all.extend(t.await.unwrap());
            }
            let secs = start.elapsed().as_secs_f64();
            all.sort_unstable();
            let p =
                |q: f64| all[((all.len() as f64 * q) as usize).min(all.len() - 1)] as f64 / 1000.0;
            eprintln!(
                "{name} {op}: {} ops, {:.0} ops/s with {conns} connections, p50 {:.2} ms, p99 {:.2} ms",
                all.len(),
                all.len() as f64 / secs,
                p(0.5),
                p(0.99)
            );
        }
        let mut con = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let Value::Array(keys) = cmd(&mut con, &["KEYS", "r6bench:*"]).await.unwrap() else {
            panic!()
        };
        for k in keys {
            if let Value::BulkString(k) = k {
                cmd(&mut con, &["DEL", &String::from_utf8_lossy(&k)])
                    .await
                    .unwrap();
            }
        }
    }
}

/// Key locations and `KeyValue.GeoSearch` over REST: 400 keys with locations (and 20 without), then circles,
/// boxes, polygons and nearest-k searches, each against brute force with
/// WGS 84 distances (Karney). Uses database `r6three_kvgeo`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_geo_search() {
    let (Some(u1), Some(http_addr)) = (var("SPRIG_TEST_REDIS_SPRIG"), var("SPRIG_TEST_KV_HTTP"))
    else {
        eprintln!("SPRIG_TEST_REDIS_SPRIG or SPRIG_TEST_KV_HTTP not set: skipped");
        return;
    };
    let (user, pass) = creds(&u1);
    let auth = format!("{user}:{pass}");
    let base = "/v1/databases/r6three_kvgeo/namespaces/places";
    let mut seed = 5u64;
    let mut rnd = |lo: f64, hi: f64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        lo + (hi - lo) * ((seed >> 11) as f64 / (1u64 << 53) as f64)
    };
    let mut pts: Vec<(String, f64, f64)> = Vec::new();
    for i in 0..420 {
        let key = format!("p{i:03}");
        let (lat, lng) = (rnd(18.8, 19.3), rnd(72.7, 73.1));
        let mut body = json!({"key": key, "text": format!("v{i}")});
        if i < 400 {
            body["location"] = json!({"latitude": lat, "longitude": lng});
            pts.push((key, lat, lng));
        }
        let (st, r) = http(&http_addr, &auth, &format!("{base}:put"), &body).await;
        assert_eq!(st, 200, "{r}");
    }
    let dist = sprig_geo::region::distance_m;
    let mut bad = Vec::new();
    let mut checks = 0;
    let keys_of = |r: &J| -> Vec<String> {
        r["hits"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|h| h["entry"]["key"].as_str().unwrap_or("").to_owned())
                    .collect()
            })
            .unwrap_or_default()
    };
    for k in 0..8 {
        let (clat, clng) = (rnd(18.9, 19.2), rnd(72.8, 73.0));
        // A circle, ordered by distance from its center.
        let r_m = rnd(1_000.0, 9_000.0);
        let (st, r) = http(&http_addr, &auth, &format!("{base}:geoSearch"), &json!({
            "region": {"center": {"latitude": clat, "longitude": clng}, "radius_m": r_m}, "limit": 1000})).await;
        let mut want: Vec<(f64, String)> = pts
            .iter()
            .map(|(key, la, lo)| (dist(clat, clng, *la, *lo), key.clone()))
            .filter(|(d, _)| *d <= r_m)
            .collect();
        want.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let want_keys: Vec<String> = want.iter().map(|w| w.1.clone()).collect();
        checks += 1;
        let close = r["hits"].as_array().is_some_and(|a| {
            a.iter()
                .zip(&want)
                .all(|(h, w)| (h["distance_m"].as_f64().unwrap_or(-1.0) - w.0).abs() < 1e-3)
        });
        if st != 200 || keys_of(&r) != want_keys || !close {
            bad.push(format!(
                "circle {k}: {st} {} vs {}",
                keys_of(&r).len(),
                want_keys.len()
            ));
        }
        // A box, in key order.
        let (s, w) = (clat - 0.05, clng - 0.07);
        let (n, e) = (clat + 0.04, clng + 0.06);
        let (st, r) = http(&http_addr, &auth, &format!("{base}:geoSearch"), &json!({
            "region": {"south_west": {"latitude": s, "longitude": w}, "north_east": {"latitude": n, "longitude": e}}, "limit": 1000})).await;
        let mut want: Vec<String> = pts
            .iter()
            .filter(|(_, la, lo)| *la >= s && *la <= n && *lo >= w && *lo <= e)
            .map(|p| p.0.clone())
            .collect();
        want.sort();
        checks += 1;
        if st != 200 || keys_of(&r) != want {
            bad.push(format!("box {k}: {st} {:?} vs {want:?}", keys_of(&r)));
        }
        // A triangle, ordered from `near`.
        let tri = [
            (clat - 0.05, clng - 0.05),
            (clat - 0.03, clng + 0.06),
            (clat + 0.06, clng),
        ];
        let (st, r) = http(&http_addr, &auth, &format!("{base}:geoSearch"), &json!({
            "region": {"polygon": tri.iter().map(|(a, b)| json!({"latitude": a, "longitude": b})).collect::<Vec<_>>()},
            "near": {"latitude": clat, "longitude": clng}, "limit": 1000})).await;
        let region = sprig_geo::region::Region::Polygon(tri.to_vec());
        let mut want: Vec<(f64, String)> = pts
            .iter()
            .filter(|(_, la, lo)| region.contains(*la, *lo))
            .map(|(key, la, lo)| (dist(clat, clng, *la, *lo), key.clone()))
            .collect();
        want.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let want: Vec<String> = want.into_iter().map(|w| w.1).collect();
        checks += 1;
        if st != 200 || keys_of(&r) != want || want.is_empty() {
            bad.push(format!("polygon {k}: {st} {:?} vs {want:?}", keys_of(&r)));
        }
        // The 10 nearest keys anywhere.
        let (st, r) = http(
            &http_addr,
            &auth,
            &format!("{base}:geoSearch"),
            &json!({
            "near": {"latitude": clat, "longitude": clng}, "limit": 10}),
        )
        .await;
        let mut all: Vec<(f64, String)> = pts
            .iter()
            .map(|(key, la, lo)| (dist(clat, clng, *la, *lo), key.clone()))
            .collect();
        all.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let want: Vec<String> = all.into_iter().take(10).map(|w| w.1).collect();
        checks += 1;
        if st != 200 || keys_of(&r) != want {
            bad.push(format!("nearest {k}: {st} {:?} vs {want:?}", keys_of(&r)));
        }
    }
    // A bad region.
    let (st, _) = http(
        &http_addr,
        &auth,
        &format!("{base}:geoSearch"),
        &json!({
        "region": {"center": {"latitude": 95.0, "longitude": 0.0}, "radius_m": 10.0}}),
    )
    .await;
    checks += 1;
    if st != 400 {
        bad.push(format!("bad region: {st}"));
    }
    for b in &bad {
        eprintln!("MISMATCH {b}");
    }
    eprintln!("kv geo: {checks} checks, {} mismatches", bad.len());
    if let Some(sql) = var("SPRIG_TEST_KV_SQL") {
        let mut c = mysql_async::Conn::new(mysql_async::Opts::from_url(&sql).unwrap())
            .await
            .unwrap();
        c.query_drop("DROP DATABASE IF EXISTS r6three_kvgeo")
            .await
            .unwrap();
        c.disconnect().await.unwrap();
    }
    assert!(bad.is_empty());
}
