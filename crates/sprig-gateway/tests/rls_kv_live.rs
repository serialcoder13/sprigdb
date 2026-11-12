//! Row-level security on key-value namespaces and vectors, live (chapter
//! 29, R6; decision RL35): a key policy on a Redis database's key table
//! binds Redis commands on strings and structures (their element tables
//! follow the key policy), and a policy on a vector-indexed table filters
//! before the ranking, so `LIMIT k` returns `k` visible rows.
//!
//! ```sh
//! export SPRIG_TEST_RLS_SQL=127.0.0.1:33306
//! export SPRIG_TEST_RLS_REDIS=127.0.0.1:33380
//! export SPRIG_TEST_RLS_TENANT=acme
//! cargo test -p sprig-gateway --test rls_kv_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::collections::BTreeSet;

use mysql_async::prelude::Queryable;

const PW: &str = "Rlsk-suite-pw-1";
const VDB: &str = "rlsk_suite";

struct Checks {
    n: usize,
    failed: Vec<String>,
}

impl Checks {
    fn ok(&mut self, what: &str, cond: bool, detail: impl FnOnce() -> String) {
        self.n += 1;
        if !cond {
            let d = detail();
            eprintln!("FAIL {what}: {d}");
            self.failed.push(format!("{what}: {d}"));
        }
    }
}

fn tenant() -> String {
    std::env::var("SPRIG_TEST_RLS_TENANT").unwrap_or_else(|_| "acme".into())
}

async fn sql(addr: &str, user: &str, pw: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.{user}", tenant())))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn redis(addr: &str, user: &str, pw: &str) -> redis::aio::MultiplexedConnection {
    let url = format!("redis://{}.{user}:{pw}@{addr}", tenant());
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

async fn cmd(
    c: &mut redis::aio::MultiplexedConnection,
    args: &[&str],
) -> Result<redis::Value, String> {
    let mut k = redis::cmd(args[0]);
    for a in &args[1..] {
        k.arg(*a);
    }
    k.query_async(c).await.map_err(|e| e.to_string())
}

fn texts(v: &redis::Value) -> BTreeSet<String> {
    match v {
        redis::Value::Array(a) | redis::Value::Set(a) => a
            .iter()
            .filter_map(|x| match x {
                redis::Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
                redis::Value::SimpleString(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        redis::Value::Map(m) => m
            .iter()
            .flat_map(|(k, v)| [k, v])
            .filter_map(|x| match x {
                redis::Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
                _ => None,
            })
            .collect(),
        _ => BTreeSet::new(),
    }
}

/// An SSE `Changes.Watch` of the Redis database as `user`.
async fn sse_open(addr: &str, user: &str, pw: &str) -> tokio::net::TcpStream {
    use base64::Engine as _;
    use tokio::io::AsyncWriteExt;
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let auth =
        base64::engine::general_purpose::STANDARD.encode(format!("{}.{user}:{pw}", tenant()));
    let req = format!(
        "GET /v1/databases/redis/changes HTTP/1.1\r\nhost: {addr}\r\nauthorization: Basic {auth}\r\naccept: text/event-stream\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    s
}

/// The SSE events read from `s` for `secs`.
async fn sse_read(s: &mut tokio::net::TcpStream, secs: u64) -> Vec<serde_json::Value> {
    use tokio::io::AsyncReadExt;
    let end = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    let mut buf = Vec::new();
    let mut chunk = vec![0u8; 65536];
    while let Ok(Ok(n)) = tokio::time::timeout_at(end, s.read(&mut chunk)).await {
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    String::from_utf8_lossy(&buf)
        .split("\n\n")
        .filter_map(|b| {
            b.lines()
                .find_map(|l| l.strip_prefix("data: "))
                .and_then(|d| serde_json::from_str(d.trim()).ok())
        })
        .collect()
}

/// The direct MariaDB probe's connection details: (address, tenant id,
/// root password, backend secret), from `SPRIG_TEST_RLS_DIRECT`,
/// `SPRIG_TEST_RLS_TENANT_ID` and the dev defaults.
fn direct() -> Option<(String, u64, String, String)> {
    let (Ok(addr), Ok(tid)) = (
        std::env::var("SPRIG_TEST_RLS_DIRECT"),
        std::env::var("SPRIG_TEST_RLS_TENANT_ID"),
    ) else {
        eprintln!("direct MariaDB probe skipped (SPRIG_TEST_RLS_DIRECT, SPRIG_TEST_RLS_TENANT_ID)");
        return None;
    };
    Some((
        addr,
        tid.parse().ok()?,
        std::env::var("SPRIG_TEST_RLS_ROOT_PW").unwrap_or_else(|_| "sprig-dev-root".into()),
        std::env::var("SPRIG_TEST_RLS_SECRET")
            .unwrap_or_else(|_| "sprig-dev-backend-accounts-secret".into()),
    ))
}

async fn raw(addr: &str, user: &str, pw: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

/// Alice's backend account and the context the gateway would set for
/// her (policies `names`), with the schemas holding `view`.
async fn alice_direct(
    names: &[&str],
    view: &str,
) -> Option<(mysql_async::Conn, String, Vec<String>)> {
    let (addr, tid, root_pw, secret) = direct()?;
    let mut root = raw(&addr, "root", &root_pw).await;
    let uid: u64 = root
        .query_first(format!(
            "SELECT MAX(user_id) FROM t{tid}_sprig.users WHERE name = 'rlsk_alice'"
        ))
        .await
        .unwrap()
        .expect("rlsk_alice's id");
    let list = names
        .iter()
        .map(|n| format!("'{n}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let pids: Vec<u64> = root
        .query(format!(
            "SELECT policy_id FROM t{tid}_sprig.row_policies WHERE name IN ({list})"
        ))
        .await
        .unwrap();
    let schemas: Vec<String> = root
        .query(format!(
            "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE \
             't{tid}\\_d%' AND TABLE_NAME = '{view}'"
        ))
        .await
        .unwrap();
    drop(root);
    let account = format!("t{tid}_u{uid}");
    let pw = sprig_auth::authz::backend::BackendKey::new(secret.as_bytes())
        .unwrap()
        .password(&account)
        .to_string();
    let b = raw(&addr, &account, &pw).await;
    let ctx = serde_json::json!({
        "b": 0, "x": [], "p": pids,
        "s": {"user": "rlsk_alice@%", "uid": uid, "tid": tid}
    })
    .to_string();
    Some((b, ctx, schemas))
}

async fn keys_of(c: &mut mysql_async::Conn, sql: &str) -> Result<BTreeSet<String>, String> {
    c.query::<Vec<u8>, _>(sql)
        .await
        .map(|v| {
            v.into_iter()
                .map(|k| String::from_utf8_lossy(&k).into_owned())
                .filter(|k| k.starts_with("rlsk"))
                .collect()
        })
        .map_err(|e| e.to_string())
}

/// RL55, straight to MariaDB: the element tables are refused to alice's
/// account; their views show her elements only.
async fn direct_elements(ck: &mut Checks) {
    let Some((mut b, ctx, schemas)) =
        alice_direct(&["rlsk_own"], "__sprig_rls___sprig_ke_db0").await
    else {
        return;
    };
    ck.ok(
        "direct: element policy views exist",
        !schemas.is_empty(),
        || "none".into(),
    );
    let mut seen = BTreeSet::new();
    let mut zseen = BTreeSet::new();
    for s in &schemas {
        for t in ["__sprig_ke_db0", "__sprig_kz_db0"] {
            let e = keys_of(&mut b, &format!("SELECT k FROM `{s}`.{t}")).await;
            ck.ok(
                &format!("direct: element table {t} refused in {s}"),
                e.as_ref().is_err_and(|e| e.contains("1142")),
                || format!("{e:?}"),
            );
        }
        let none = keys_of(
            &mut b,
            &format!("SELECT k FROM `{s}`.__sprig_rls___sprig_ke_db0"),
        )
        .await;
        ck.ok(
            &format!("direct: no context, no elements in {s}"),
            none == Ok(BTreeSet::new()),
            || format!("{none:?}"),
        );
        b.query_drop(format!("SET @sprig_rls = '{ctx}'"))
            .await
            .unwrap();
        let got = keys_of(
            &mut b,
            &format!("SELECT k FROM `{s}`.__sprig_rls___sprig_ke_db0"),
        )
        .await;
        ck.ok(
            &format!("direct: element view read in {s}"),
            got.is_ok(),
            || format!("{got:?}"),
        );
        seen.extend(got.unwrap_or_default());
        let z = keys_of(
            &mut b,
            &format!("SELECT k FROM `{s}`.__sprig_rls___sprig_kz_db0"),
        )
        .await;
        zseen.extend(z.unwrap_or_default());
        // The all-columns view: invisible columns, same rows.
        let h = keys_of(
            &mut b,
            &format!("SELECT k FROM `{s}`.__sprig_rlsh___sprig_ke_db0 WHERE _sprig_rk IS NOT NULL OR _sprig_rk IS NULL"),
        )
        .await;
        ck.ok(
            &format!("direct: element all-columns view filtered in {s}"),
            h.as_ref()
                .is_ok_and(|h| h.iter().all(|k| k.starts_with("rlsk_alice:"))),
            || format!("{h:?}"),
        );
        b.query_drop("SET @sprig_rls = NULL").await.unwrap();
    }
    ck.ok(
        "direct: element views show alice's hash only",
        seen == BTreeSet::from(["rlsk_alice:h".to_owned()]),
        || format!("{seen:?}"),
    );
    ck.ok(
        "direct: sorted-set view shows alice's zset only",
        zseen == BTreeSet::from(["rlsk_alice:z".to_owned()]),
        || format!("{zseen:?}"),
    );
}

/// RL55, straight to MariaDB, with the value-field policy too: the
/// element view applies it through the key row.
async fn direct_elements_value_policy(ck: &mut Checks) {
    let Some((mut b, ctx, schemas)) =
        alice_direct(&["rlsk_own", "rlsk_sets"], "__sprig_rls___sprig_ke_db0").await
    else {
        return;
    };
    let mut seen = BTreeSet::new();
    b.query_drop(format!("SET @sprig_rls = '{ctx}'"))
        .await
        .unwrap();
    for s in &schemas {
        let got = keys_of(
            &mut b,
            &format!("SELECT k FROM `{s}`.__sprig_rls___sprig_ke_db0"),
        )
        .await;
        seen.extend(got.unwrap_or_default());
    }
    ck.ok(
        "direct: element views apply the value-field policy",
        seen == BTreeSet::from(["rlsk_alice:h".to_owned(), "rlsk_bob:set".to_owned()]),
        || format!("{seen:?}"),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_level_security_on_kv_and_vectors() {
    let (Ok(sql_addr), Ok(redis_addr)) = (
        std::env::var("SPRIG_TEST_RLS_SQL"),
        std::env::var("SPRIG_TEST_RLS_REDIS"),
    ) else {
        eprintln!("skipped: set SPRIG_TEST_RLS_SQL and SPRIG_TEST_RLS_REDIS");
        return;
    };
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let mut admin = sql(&sql_addr, "app", "sprig-dev-app").await;
    for u in ["rlsk_alice", "rlsk_bob"] {
        let _ = admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }
    let _ = admin
        .query_drop(format!("DROP DATABASE IF EXISTS {VDB}"))
        .await;
    // Keys as the administrator (creates the Redis database's tables).
    let mut ar = redis(&redis_addr, "app", "sprig-dev-app").await;
    for k in ["rlsk_alice:s1", "rlsk_alice:s2", "rlsk_bob:s1"] {
        cmd(&mut ar, &["SET", k, "v"]).await.unwrap();
    }
    for k in ["rlsk_alice:h", "rlsk_bob:h"] {
        cmd(&mut ar, &["HSET", k, "f1", "a", "f2", "b"])
            .await
            .unwrap();
    }
    cmd(&mut ar, &["SADD", "rlsk_bob:set", "m1", "m2"])
        .await
        .unwrap();
    cmd(&mut ar, &["ZADD", "rlsk_bob:z", "1", "m1"])
        .await
        .unwrap();
    cmd(&mut ar, &["ZADD", "rlsk_alice:z", "2", "m2"])
        .await
        .unwrap();
    for u in ["rlsk_alice", "rlsk_bob"] {
        admin
            .query_drop(format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'"))
            .await
            .unwrap();
        for db in ["redis", VDB] {
            admin
                .query_drop(format!(
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON {db}.* TO '{u}'@'%'"
                ))
                .await
                .unwrap();
        }
    }
    // A key policy: each user's own prefix.
    let _ = admin
        .query_drop("DROP POLICY IF EXISTS rlsk_own ON redis.db0")
        .await;
    for s in [
        "CREATE POLICY rlsk_own ON redis.db0 USING (k LIKE CONCAT(SUBSTRING_INDEX(CURRENT_USER(), '@', 1), ':%'))",
        "ALTER TABLE redis.db0 ENABLE ROW LEVEL SECURITY",
    ] {
        let r = admin.query_drop(s).await;
        ck.ok(&format!("setup: {s}"), r.is_ok(), || format!("{r:?}"));
    }
    let mut a = redis(&redis_addr, "rlsk_alice", PW).await;
    let got = cmd(&mut a, &["KEYS", "rlsk_*"]).await;
    let want: BTreeSet<String> = [
        "rlsk_alice:s1",
        "rlsk_alice:s2",
        "rlsk_alice:h",
        "rlsk_alice:z",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    ck.ok(
        "KEYS shows own keys",
        got.as_ref().map(texts).as_ref() == Ok(&want),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["GET", "rlsk_bob:s1"]).await;
    ck.ok("GET hidden is nil", got == Ok(redis::Value::Nil), || {
        format!("{got:?}")
    });
    let got = cmd(&mut a, &["GET", "rlsk_alice:s1"]).await;
    ck.ok(
        "GET own",
        got == Ok(redis::Value::BulkString(b"v".to_vec())),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["EXISTS", "rlsk_bob:s1", "rlsk_bob:h"]).await;
    ck.ok(
        "EXISTS hidden is 0",
        got == Ok(redis::Value::Int(0)),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["HGETALL", "rlsk_bob:h"]).await;
    ck.ok(
        "HGETALL hidden is empty",
        got.as_ref().map(texts).is_ok_and(|t| t.is_empty()),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["HGET", "rlsk_bob:h", "f1"]).await;
    ck.ok("HGET hidden is nil", got == Ok(redis::Value::Nil), || {
        format!("{got:?}")
    });
    let got = cmd(&mut a, &["HGETALL", "rlsk_alice:h"]).await;
    ck.ok(
        "HGETALL own",
        got.as_ref().map(texts).is_ok_and(|t| t.len() == 4),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["SMEMBERS", "rlsk_bob:set"]).await;
    ck.ok(
        "SMEMBERS hidden is empty",
        got.as_ref().map(texts).is_ok_and(|t| t.is_empty()),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["ZRANGE", "rlsk_bob:z", "0", "-1"]).await;
    ck.ok(
        "ZRANGE hidden is empty",
        got.as_ref().map(texts).is_ok_and(|t| t.is_empty()),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["ZRANGE", "rlsk_alice:z", "0", "-1"]).await;
    ck.ok(
        "ZRANGE own",
        got.as_ref().map(texts) == Ok(BTreeSet::from(["m2".to_owned()])),
        || format!("{got:?}"),
    );
    // Writes: a hidden key can't be changed or deleted.
    let got = cmd(&mut a, &["DEL", "rlsk_bob:s1", "rlsk_bob:h"]).await;
    ck.ok(
        "DEL hidden deletes nothing",
        got == Ok(redis::Value::Int(0)),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["SET", "rlsk_bob:new", "x"]).await;
    ck.ok(
        "SET outside the prefix refused",
        got.as_ref()
            .is_err_and(|e| e.contains("39363") || e.contains("row-level")),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["HSET", "rlsk_bob:h", "f9", "z"]).await;
    ck.ok("HSET on a hidden hash refused", got.is_err(), || {
        format!("{got:?}")
    });
    let got = cmd(&mut a, &["SET", "rlsk_alice:new", "x"]).await;
    ck.ok("SET own", got.is_ok(), || format!("{got:?}"));
    let got = cmd(&mut a, &["DEL", "rlsk_alice:new"]).await;
    ck.ok("DEL own", got == Ok(redis::Value::Int(1)), || {
        format!("{got:?}")
    });
    // The administrator's keys are all still there.
    let got = cmd(&mut ar, &["HGETALL", "rlsk_bob:h"]).await;
    ck.ok(
        "hidden hash untouched",
        got.as_ref().map(texts).is_ok_and(|t| t.len() == 4),
        || format!("{got:?}"),
    );
    let got = cmd(&mut ar, &["GET", "rlsk_bob:s1"]).await;
    ck.ok(
        "hidden string untouched",
        got == Ok(redis::Value::BulkString(b"v".to_vec())),
        || format!("{got:?}"),
    );
    // SQL over the namespace (read-only), the same keys.
    let mut s = sql(&sql_addr, "rlsk_alice", PW).await;
    let n: Option<i64> = s
        .query_first("SELECT COUNT(*) FROM redis.db0 WHERE k LIKE 'rlsk%'")
        .await
        .unwrap();
    ck.ok("SQL count of keys", n == Some(4), || format!("{n:?}"));
    let n: Result<Option<i64>, _> = s
        .query_first("SELECT COUNT(*) FROM redis.__sprig_ke_db0")
        .await;
    ck.ok(
        "element table read through the key policy",
        n.as_ref().map_or(true, |n| *n == Some(2) || n.is_none()),
        || format!("{n:?}"),
    );

    // RL55: change streams over the Redis database: a structure's
    // element rows never stream (internal tables), and its key row's
    // changes reach alice only for her keys.
    if let Ok(http) = std::env::var("SPRIG_TEST_RLS_HTTP") {
        let mut sa = sse_open(&http, "rlsk_alice", PW).await;
        let mut sb = sse_open(&http, "app", "sprig-dev-app").await;
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        for k in ["rlsk_alice:h", "rlsk_bob:h"] {
            cmd(&mut ar, &["HSET", k, "f3", "c"]).await.unwrap();
        }
        let (ta, tb) = tokio::join!(sse_read(&mut sa, 8), sse_read(&mut sb, 8));
        let objs = |t: &[serde_json::Value]| -> Vec<String> {
            t.iter()
                .filter(|e| e["op"] != "heartbeat")
                .map(|e| e["object"].as_str().unwrap_or("").to_owned())
                .collect()
        };
        let (oa, ob) = (objs(&ta), objs(&tb));
        ck.ok(
            "change stream: alice gets her key's change only, no element rows",
            oa == vec!["db0".to_owned()],
            || format!("{oa:?} / {ta:?}"),
        );
        ck.ok(
            "change stream: the administrator gets both keys' changes",
            ob.iter().filter(|o| *o == "db0").count() == 2
                && !ob.iter().any(|o| o.starts_with("__sprig")),
            || format!("{ob:?}"),
        );
        for k in ["rlsk_alice:h", "rlsk_bob:h"] {
            let _ = cmd(&mut ar, &["HDEL", k, "f3"]).await;
        }
    } else {
        eprintln!("change stream check skipped (SPRIG_TEST_RLS_HTTP)");
    }

    // RL55: element policy views. Straight to MariaDB as alice's backend
    // account, the element tables are refused and their views show only
    // the elements of keys her policies show.
    direct_elements(&mut ck).await;

    // RL55: a policy reading more than the key (the key's type) reaches
    // the elements through their key rows; before, it hid every element
    // from the users it bound (RL35), alice's own included.
    let r = admin
        .query_drop("CREATE POLICY rlsk_sets ON redis.db0 FOR SELECT USING (type = 'set')")
        .await;
    ck.ok("setup: value-field policy", r.is_ok(), || format!("{r:?}"));
    let got = cmd(&mut a, &["SMEMBERS", "rlsk_bob:set"]).await;
    ck.ok(
        "value-field policy: SMEMBERS of a set shown by its type",
        got.as_ref().map(texts) == Ok(BTreeSet::from(["m1".to_owned(), "m2".to_owned()])),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["HGETALL", "rlsk_alice:h"]).await;
    ck.ok(
        "value-field policy: own hash still read",
        got.as_ref().map(texts).is_ok_and(|t| t.len() == 4),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["HGETALL", "rlsk_bob:h"]).await;
    ck.ok(
        "value-field policy: other hash still hidden",
        got.as_ref().map(texts).is_ok_and(|t| t.is_empty()),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["ZRANGE", "rlsk_bob:z", "0", "-1"]).await;
    ck.ok(
        "value-field policy: other zset still hidden",
        got.as_ref().map(texts).is_ok_and(|t| t.is_empty()),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["ZRANGE", "rlsk_alice:z", "0", "-1"]).await;
    ck.ok(
        "value-field policy: own zset still read",
        got.as_ref().map(texts) == Ok(BTreeSet::from(["m2".to_owned()])),
        || format!("{got:?}"),
    );
    let got = cmd(&mut a, &["SISMEMBER", "rlsk_bob:set", "m1"]).await;
    ck.ok(
        "value-field policy: SISMEMBER",
        got == Ok(redis::Value::Int(1)),
        || format!("{got:?}"),
    );
    // SQL over the namespace: the key-value layer's read-only views.
    let rows: Result<Vec<(String, String)>, _> = s
        .query("SELECT k, member FROM redis.db0__set WHERE k LIKE 'rlsk%' ORDER BY 1, 2")
        .await;
    ck.ok(
        "value-field policy: SQL over db0__set",
        rows.as_ref().ok()
            == Some(&vec![
                ("rlsk_bob:set".to_owned(), "m1".to_owned()),
                ("rlsk_bob:set".to_owned(), "m2".to_owned()),
            ]),
        || format!("{rows:?}"),
    );
    let rows: Result<Vec<(String, String)>, _> = s
        .query("SELECT k, field FROM redis.db0__hash WHERE k LIKE 'rlsk%' ORDER BY 1, 2")
        .await;
    ck.ok(
        "SQL over db0__hash shows own fields only",
        rows.as_ref().ok()
            == Some(&vec![
                ("rlsk_alice:h".to_owned(), "f1".to_owned()),
                ("rlsk_alice:h".to_owned(), "f2".to_owned()),
            ]),
        || format!("{rows:?}"),
    );
    direct_elements_value_policy(&mut ck).await;
    let _ = admin
        .query_drop("DROP POLICY IF EXISTS rlsk_sets ON redis.db0")
        .await;

    // Vectors: the policy filters before the ranking.
    for q in [
        format!("CREATE DATABASE {VDB}"),
        format!(
            "CREATE TABLE {VDB}.vecs (id INT PRIMARY KEY, owner VARCHAR(64) NOT NULL, \
             v VECTOR(2) NOT NULL, VECTOR INDEX (v))"
        ),
    ] {
        admin.query_drop(&q).await.unwrap();
    }
    let rows: Vec<String> = (1..=40)
        .map(|i| {
            let owner = if i % 4 == 0 {
                "rlsk_alice@%"
            } else {
                "rlsk_bob@%"
            };
            format!("({i}, '{owner}', VEC_FromText('[{i}, 0]'))")
        })
        .collect();
    admin
        .query_drop(format!("INSERT INTO {VDB}.vecs VALUES {}", rows.join(", ")))
        .await
        .unwrap();
    for q in [
        format!("CREATE POLICY own ON {VDB}.vecs USING (owner = CURRENT_USER())"),
        format!("ALTER TABLE {VDB}.vecs ENABLE ROW LEVEL SECURITY"),
    ] {
        admin.query_drop(&q).await.unwrap();
    }
    let got: Result<Vec<i64>, _> = s
        .query(format!(
            "SELECT id FROM {VDB}.vecs ORDER BY VEC_DISTANCE_EUCLIDEAN(v, VEC_FromText('[0, 0]')) LIMIT 5"
        ))
        .await;
    ck.ok(
        "vector top-5: five visible rows, nearest first",
        got.as_ref().ok() == Some(&vec![4, 8, 12, 16, 20]),
        || format!("{got:?}"),
    );
    let got: Result<Vec<i64>, _> = s
        .query(format!(
            "SELECT id FROM {VDB}.vecs ORDER BY VEC_DISTANCE_COSINE(v, VEC_FromText('[1, 0]')), id LIMIT 3"
        ))
        .await;
    ck.ok(
        "vector cosine top-3 visible",
        got.as_ref()
            .is_ok_and(|v| v.len() == 3 && v.iter().all(|i| i % 4 == 0)),
        || format!("{got:?}"),
    );

    // Cleanup.
    let _ = admin
        .query_drop("DROP POLICY IF EXISTS rlsk_own ON redis.db0")
        .await;
    let _ = admin
        .query_drop("DROP POLICY IF EXISTS rlsk_sets ON redis.db0")
        .await;
    let _ = admin
        .query_drop("ALTER TABLE redis.db0 DISABLE ROW LEVEL SECURITY")
        .await;
    for k in [
        "rlsk_alice:s1",
        "rlsk_alice:s2",
        "rlsk_bob:s1",
        "rlsk_alice:h",
        "rlsk_bob:h",
        "rlsk_bob:set",
        "rlsk_bob:z",
        "rlsk_alice:z",
    ] {
        let _ = cmd(&mut ar, &["DEL", k]).await;
    }
    let _ = admin
        .query_drop(format!("DROP DATABASE IF EXISTS {VDB}"))
        .await;
    for u in ["rlsk_alice", "rlsk_bob"] {
        let _ = admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }
    eprintln!(
        "row-level security on key-value and vectors: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
