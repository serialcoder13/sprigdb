//! bugfix-34 (decisions/bugfix-interim.md): `DROP DATABASE` takes the
//! database's sinks, connections and logic triggers with it. Before, the
//! sinks stayed `running` and the triggers `active` in the catalog, their
//! DC index entries and feed subscriptions stayed, and the after-commit
//! trigger's job ran on (rls4's audit).
//!
//! A database with a table and its change topic, a stream, a webhook
//! connection, a sink from the stream and one from the table's changes, a
//! trigger on the stream and an after-commit trigger on the table (both
//! seen working first). After `DROP DATABASE`:
//! - no `sinks` or `sink_connections` row of the database is left, and no
//!   sink index entry;
//! - every `logic_modules` row of the database is `dropped`;
//! - the stream trigger's index entry and the after-commit trigger's feed
//!   subscription are gone, and both triggers' jobs end within 30 s;
//! - a new database of the same name takes the same sink, connection and
//!   trigger names.
//!
//! ```sh
//! SPRIG_TEST_DROPDB_SQL=127.0.0.1:51306 \
//! SPRIG_TEST_DROPDB_TENANT=acme SPRIG_TEST_DROPDB_TENANT_ID=51306000000 \
//! SPRIG_TEST_DROPDB_NODE=127.0.0.1:13301 \
//! SPRIG_TEST_DROPDB_ETCD=127.0.0.1:51379 \
//!   cargo test -p sprig-gateway --test drop_database_live -- --nocapture
//! ```
//!
//! The node is the tenant's catalog cluster (read as `root`); the etcd is
//! the deployment's own (read only).

#![allow(
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs
)]

use std::time::{Duration, Instant};

use base64::Engine as _;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const DB: &str = "bf9drop";

struct Env {
    sql: (String, u16),
    tenant: String,
    tenant_id: u64,
    node: (String, u16),
    etcd: String,
}

fn hostport(v: &str) -> (String, u16) {
    let (h, p) = v.rsplit_once(':').expect("host:port");
    (h.to_owned(), p.parse().expect("port"))
}

fn env() -> Option<Env> {
    Some(Env {
        sql: hostport(&std::env::var("SPRIG_TEST_DROPDB_SQL").ok()?),
        tenant: std::env::var("SPRIG_TEST_DROPDB_TENANT").ok()?,
        tenant_id: std::env::var("SPRIG_TEST_DROPDB_TENANT_ID")
            .ok()?
            .parse()
            .ok()?,
        node: hostport(&std::env::var("SPRIG_TEST_DROPDB_NODE").ok()?),
        etcd: std::env::var("SPRIG_TEST_DROPDB_ETCD").ok()?,
    })
}

async fn login(e: &Env, db: Option<&str>) -> Conn {
    let opts = OptsBuilder::default()
        .ip_or_hostname(e.sql.0.clone())
        .tcp_port(e.sql.1)
        .user(Some(format!("{}.app", e.tenant)))
        .pass(Some("sprig-dev-app"))
        .db_name(db)
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts).await.expect("login")
}

async fn node(e: &Env) -> Conn {
    let opts = OptsBuilder::default()
        .ip_or_hostname(e.node.0.clone())
        .tcp_port(e.node.1)
        .user(Some("root"))
        .pass(Some("sprig-dev-root"))
        .prefer_socket(false);
    Conn::new(opts).await.expect("node")
}

async fn ok(c: &mut Conn, q: &str) {
    if let Err(err) = c.query_drop(q).await {
        panic!("{q}: {err}");
    }
}

/// The values under an etcd prefix (JSON gateway, read only).
async fn etcd_range(e: &Env, prefix: &str) -> Vec<(String, Value)> {
    let b = base64::engine::general_purpose::STANDARD;
    let mut end = prefix.as_bytes().to_vec();
    *end.last_mut().unwrap() += 1;
    let body =
        serde_json::json!({"key": b.encode(prefix), "range_end": b.encode(&end)}).to_string();
    let mut s = tokio::net::TcpStream::connect(&e.etcd).await.expect("etcd");
    let req = format!(
        "POST /v3/kv/range HTTP/1.0\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        e.etcd,
        body.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    let text = String::from_utf8_lossy(&out);
    let json = text.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    let v: Value = serde_json::from_str(json.trim()).unwrap_or(Value::Null);
    v["kvs"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|kv| {
            let k = String::from_utf8(b.decode(kv["key"].as_str().unwrap_or("")).unwrap()).unwrap();
            let val = b
                .decode(kv["value"].as_str().unwrap_or(""))
                .unwrap_or_default();
            (k, serde_json::from_slice(&val).unwrap_or(Value::Null))
        })
        .collect()
}

/// Unfinished jobs of `kind` for `binding` of the tenant.
async fn live_jobs(e: &Env, kind: &str, binding: u64) -> usize {
    etcd_range(e, "/sprig/dc/dc-a/jobs/")
        .await
        .iter()
        .filter(|(_, j)| {
            j["kind"] == kind
                && j["spec"]["tenant"].as_u64() == Some(e.tenant_id)
                && j["spec"]["binding"].as_u64() == Some(binding)
                && !matches!(
                    j["state"].as_str(),
                    Some("succeeded" | "failed" | "cancelled")
                )
        })
        .count()
}

async fn wait_for<F, Fut>(secs: u64, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let end = Instant::now() + Duration::from_secs(secs);
    loop {
        if f().await {
            return true;
        }
        if Instant::now() > end {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn setup(db: &str) -> Vec<String> {
    vec![
        "CREATE TABLE docs (id INT PRIMARY KEY, v TEXT)".to_owned(),
        "ALTER TABLE docs SPRIG SET (stream.change_topic = on)".to_owned(),
        "CREATE TABLE out_t (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)".to_owned(),
        "CREATE STREAM ev PARTITIONS 1".to_owned(),
        "CREATE CONNECTION hook TYPE webhook WITH (url = 'http://127.0.0.1:9/h')".to_owned(),
        "CREATE SINK s_ev FROM STREAM ev TO WEBHOOK CONNECTION hook".to_owned(),
        "CREATE SINK s_docs FROM CHANGES OF docs TO WEBHOOK CONNECTION hook".to_owned(),
        format!(
            "CREATE TRIGGER {db}.t_ev ON STREAM {db}.ev LANGUAGE javascript AS $$\n\
             export default function (ctx) {{\n  db.exec('INSERT INTO {db}.out_t (v) VALUES (?)', ['ev:' + ctx.record.value]);\n}}\n$$"
        ),
        format!(
            r#"CREATE TRIGGER {db}.a_docs AFTER COMMIT ON {db}.docs FOR INSERT LANGUAGE kirun AS '{{"namespace":"App","name":"Ac","parameters":{{"v":{{"parameterName":"v","schema":{{"type":["STRING"]}}}}}},"steps":{{"ins":{{"statementName":"ins","namespace":"Sprig.Db","name":"Exec","parameterMap":{{"sql":{{"s":{{"key":"s","type":"VALUE","value":"INSERT INTO out_t (v) VALUES (?)"}}}},"args":{{"a":{{"key":"a","type":"EXPRESSION","order":1,"expression":"Arguments.v"}}}}}}}}}}}}'"#
        ),
    ]
}

#[tokio::test]
async fn drop_database_takes_its_sinks_and_triggers() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_DROPDB_* not set; skipped");
        return;
    };
    let t = e.tenant_id;
    let cat = format!("t{t}_sprig");
    let mut failed: Vec<String> = Vec::new();
    let mut check = |what: &str, cond: bool, detail: String| {
        if cond {
            eprintln!("ok   {what}");
        } else {
            eprintln!("FAIL {what}: {detail}");
            failed.push(format!("{what}: {detail}"));
        }
    };

    let mut a = login(&e, None).await;
    for q in [
        "GRANT SPRIG_SINK_ADMIN ON *.* TO app".to_owned(),
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("CREATE DATABASE {DB}"),
        "SPRIG SET (sink.destination_allowlist = '[{\"host\": \"127.0.0.1:9\"}]')".to_owned(),
    ] {
        ok(&mut a, &q).await;
    }
    drop(a);
    let mut a = login(&e, Some(DB)).await;
    for q in setup(DB) {
        ok(&mut a, &q).await;
    }
    let mut n = node(&e).await;
    let db_id: u32 = n
        .exec_first(
            format!("SELECT db_id FROM `{cat}`.`databases` WHERE name = ? AND state = 'active'"),
            (DB,),
        )
        .await
        .unwrap()
        .expect("db row");
    let sinks: Vec<(u64, String)> = n
        .exec(
            format!("SELECT sink_id, name FROM `{cat}`.sinks WHERE db_id = ?"),
            (db_id,),
        )
        .await
        .unwrap();
    let modules: Vec<(u64, String, String)> = n
        .exec(
            format!("SELECT module_id, name, state FROM `{cat}`.logic_modules WHERE db_id = ? AND state <> 'dropped'"),
            (db_id,),
        )
        .await
        .unwrap();
    let bindings: Vec<(u64, String)> = n
        .exec(
            format!(
                "SELECT b.binding_id, b.timing FROM `{cat}`.logic_bindings b JOIN `{cat}`.logic_modules m \
                 ON m.module_id = b.module_id WHERE m.db_id = ? AND m.state <> 'dropped'"
            ),
            (db_id,),
        )
        .await
        .unwrap();
    eprintln!("db {db_id}: sinks {sinks:?}, modules {modules:?}, bindings {bindings:?}");
    assert_eq!(sinks.len(), 2, "two sinks made");
    assert_eq!(modules.len(), 2, "two triggers made");
    let msg_b = bindings
        .iter()
        .find(|b| b.1 == "on_message")
        .expect("stream trigger binding")
        .0;
    let ac_b = bindings
        .iter()
        .find(|b| b.1 == "after_commit")
        .expect("after-commit binding")
        .0;

    // Both triggers work.
    ok(
        &mut a,
        "INSERT INTO ev (`key`, `value`) VALUES ('k', 'one')",
    )
    .await;
    ok(&mut a, "INSERT INTO docs VALUES (1, 'two')").await;
    let mut seen: Vec<String>;
    let end = Instant::now() + Duration::from_secs(60);
    let worked = loop {
        seen = a
            .query("SELECT v FROM out_t ORDER BY id")
            .await
            .unwrap_or_default();
        if seen.iter().any(|v| v == "ev:one") && seen.iter().any(|v| v == "two") {
            break true;
        }
        if Instant::now() > end {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    check(
        "both triggers ran before the drop",
        worked,
        format!("{seen:?}"),
    );
    check(
        "the after-commit trigger's job runs before the drop",
        live_jobs(&e, "logic_trigger", ac_b).await == 1,
        String::new(),
    );
    check(
        "the stream trigger's job runs before the drop",
        live_jobs(&e, "logic_stream_trigger", msg_b).await == 1,
        String::new(),
    );

    ok(&mut a, &format!("DROP DATABASE {DB}")).await;

    // The catalog.
    let left_sinks: Vec<(u64, String, String)> = n
        .exec(
            format!("SELECT sink_id, name, state FROM `{cat}`.sinks WHERE db_id = ?"),
            (db_id,),
        )
        .await
        .unwrap();
    check(
        "no sink of the dropped database is left",
        left_sinks.is_empty(),
        format!("{left_sinks:?}"),
    );
    let left_conns: Vec<String> = n
        .exec(
            format!("SELECT name FROM `{cat}`.sink_connections WHERE db_id = ?"),
            (db_id,),
        )
        .await
        .unwrap();
    check(
        "no connection of the dropped database is left",
        left_conns.is_empty(),
        format!("{left_conns:?}"),
    );
    let live_modules: Vec<(String, String)> = n
        .exec(
            format!("SELECT name, state FROM `{cat}`.logic_modules WHERE db_id = ? AND state <> 'dropped'"),
            (db_id,),
        )
        .await
        .unwrap();
    check(
        "every logic module of the dropped database is dropped",
        live_modules.is_empty(),
        format!("{live_modules:?}"),
    );

    // The DC tier.
    let sink_keys: Vec<String> = etcd_range(&e, &format!("/sprig/dc/dc-a/sinks/{t}/"))
        .await
        .into_iter()
        .map(|(k, _)| k)
        .filter(|k| sinks.iter().any(|s| k.ends_with(&format!("/{}", s.0))))
        .collect();
    check(
        "no sink index entry is left",
        sink_keys.is_empty(),
        format!("{sink_keys:?}"),
    );
    let msg_keys = etcd_range(&e, &format!("/sprig/dc/dc-a/logic/msg/{t}/{msg_b}")).await;
    check(
        "the stream trigger's index entry is gone",
        msg_keys.is_empty(),
        format!("{} left", msg_keys.len()),
    );
    let subs = etcd_range(&e, &format!("/sprig/dc/dc-a/feed/subs/{t}/logic:b{ac_b}")).await;
    check(
        "the after-commit trigger's feed subscription is gone",
        subs.is_empty(),
        format!("{} left", subs.len()),
    );

    // The jobs end.
    let ac_end = wait_for(30, || async {
        live_jobs(&e, "logic_trigger", ac_b).await == 0
    })
    .await;
    check(
        "the after-commit trigger's job ends within 30 s",
        ac_end,
        format!("{} running", live_jobs(&e, "logic_trigger", ac_b).await),
    );
    let msg_end = wait_for(30, || async {
        live_jobs(&e, "logic_stream_trigger", msg_b).await == 0
    })
    .await;
    check(
        "the stream trigger's job ends within 30 s",
        msg_end,
        format!(
            "{} running",
            live_jobs(&e, "logic_stream_trigger", msg_b).await
        ),
    );

    // The names can be used again in a new database of the same name.
    drop(a);
    let mut a = login(&e, None).await;
    ok(&mut a, &format!("CREATE DATABASE {DB}")).await;
    drop(a);
    let mut a = login(&e, Some(DB)).await;
    let mut again = Vec::new();
    for q in setup(DB) {
        if let Err(err) = a.query_drop(&q).await {
            again.push(format!("{}: {err}", q.chars().take(60).collect::<String>()));
        }
    }
    check(
        "a new database of the same name takes the same names",
        again.is_empty(),
        format!("{again:?}"),
    );
    ok(&mut a, &format!("DROP DATABASE {DB}")).await;

    assert!(failed.is_empty(), "{} failed: {failed:#?}", failed.len());
}
