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
