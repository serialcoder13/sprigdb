//! The key-value sweeper (`sprig_kv::sweep`, chapters 33 and 40) over
//! MariaDB, with keys written by SprigDB's Redis front end in process.
//!
//! - `SPRIG_TEST_KV_MYSQL`: a MariaDB as root (`mysql://root:pw@127.0.0.1:13301`).
//!
//! Skipped when unset. Uses database `r6models_sweep`, dropped at the end.

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
    clippy::cast_possible_truncation
)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_kv::sweep::{self, NsTables, SweepOpts};
use sprig_kv::{Row, SqlError, SqlRunner};
use sprig_proto_redis::{Frame, Limits};
use sprig_redis::session::{Auth, Login};
use sprig_redis::{Config, Env, Session};

const DB: &str = "r6models_sweep";

struct My(mysql_async::Conn);

fn sql_err(e: &mysql_async::Error) -> SqlError {
    match e {
        mysql_async::Error::Server(s) => SqlError {
            errno: Some(s.code),
            message: s.message.clone(),
        },
        other => SqlError {
            errno: None,
            message: other.to_string(),
        },
    }
}

#[async_trait]
impl SqlRunner for My {
    async fn query(&mut self, sql: &str) -> Result<Vec<Row>, SqlError> {
        let rows: Vec<mysql_async::Row> = self.0.query(sql).await.map_err(|e| sql_err(&e))?;
        Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(b),
                        other => Some(other.as_sql(true).into_bytes()),
                    })
                    .collect()
            })
            .collect())
    }

    async fn exec(&mut self, sql: &str) -> Result<u64, SqlError> {
        self.0.query_drop(sql).await.map_err(|e| sql_err(&e))?;
        Ok(self.0.affected_rows())
    }
}

struct TestAuth {
    url: String,
}

#[async_trait]
impl Auth for TestAuth {
    async fn login(&self, _user: Option<&[u8]>, _pass: &[u8]) -> Result<Login, Frame> {
        let opts = mysql_async::Opts::from_url(&self.url).unwrap();
        let conn = mysql_async::Conn::new(opts)
            .await
            .map_err(|e| Frame::err(&e.to_string()))?;
        Ok(Login {
            sql: Box::new(My(conn)),
            scope: "r6models-sweep".into(),
            user: "default".into(),
        })
    }
}

async fn count(c: &mut mysql_async::Conn, t: &str) -> u64 {
    c.query_first(format!("SELECT COUNT(*) FROM `{DB}`.`{t}`"))
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweeper_removes_expired_keys_and_orphans() {
    let Some(url) = std::env::var("SPRIG_TEST_KV_MYSQL")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("SPRIG_TEST_KV_MYSQL not set: skipped");
        return;
    };
    let opts = mysql_async::Opts::from_url(&url).unwrap();
    let mut root = mysql_async::Conn::new(opts.clone()).await.unwrap();
    root.query_drop(format!("DROP DATABASE IF EXISTS `{DB}`"))
        .await
        .unwrap();
    // The front end in process: writes go through the real commands.
    let env = Arc::new(Env::new(
        Config {
            logical_database: DB.into(),
            ..Config::default()
        },
        "r6models-sweep",
    ));
    let auth: Arc<dyn Auth> = Arc::new(TestAuth { url: url.clone() });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((sock, peer)) = listener.accept().await {
            let s = Session::new(Arc::clone(&env), Arc::clone(&auth), &peer.to_string());
            tokio::spawn(sprig_redis::conn::serve(
                sock,
                s,
                Limits::default(),
                std::future::pending(),
            ));
        }
    });
    let client = redis::Client::open(format!("redis://default:pw@{addr}")).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let q = |args: Vec<String>| {
        let mut cmd = redis::cmd(&args[0]);
        for a in &args[1..] {
            cmd.arg(a);
        }
        cmd
    };
    let s = |v: &[&str]| v.iter().map(|x| (*x).to_owned()).collect::<Vec<_>>();
    let _: () = q(s(&["SET", "a", "1", "PX", "300"]))
        .query_async(&mut c)
        .await
        .unwrap();
    let mut hset = s(&["HSET", "h"]);
    for i in 0..10 {
        hset.push(format!("f{i}"));
        hset.push(format!("v{i}"));
    }
    let _: i64 = q(hset).query_async(&mut c).await.unwrap();
    let _: i64 = q(s(&["PEXPIRE", "h", "300"]))
        .query_async(&mut c)
        .await
        .unwrap();
    // Larger than inline_max: its elements go after the key row's commit.
    let mut push = s(&["RPUSH", "big"]);
    for i in 0..300 {
        push.push(format!("i{i}"));
    }
    let _: i64 = q(push).query_async(&mut c).await.unwrap();
    let _: i64 = q(s(&["PEXPIRE", "big", "300"]))
        .query_async(&mut c)
        .await
        .unwrap();
    let _: i64 = q(s(&["ZADD", "z", "1", "m1", "2", "m2"]))
        .query_async(&mut c)
        .await
        .unwrap();
    let _: i64 = q(s(&["PEXPIRE", "z", "300"]))
        .query_async(&mut c)
        .await
        .unwrap();
    let _: () = q(s(&["SET", "live", "1"]))
        .query_async(&mut c)
        .await
        .unwrap();
    let _: i64 = q(s(&["HSET", "livehash", "f", "v"]))
        .query_async(&mut c)
        .await
        .unwrap();
    // An orphaned generation, as a crash between statements could leave.
    root.query_drop(format!(
        "INSERT INTO `{DB}`.`__sprig_ke_db0` (k, g, o, e, v) VALUES ('livehash', 12345, 1, 'x', 'y'), ('gone', 7, 2, 'x', 'y')"
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(count(&mut root, "db0").await, 6);
    assert_eq!(count(&mut root, "__sprig_ke_db0").await, 10 + 300 + 1 + 2);
    assert_eq!(count(&mut root, "__sprig_kz_db0").await, 2);

    let mut sql = My(mysql_async::Conn::new(opts.clone()).await.unwrap());
    let ns = NsTables {
        schema: DB.into(),
        table: "db0".into(),
    };
    // A client holds an expired key: SKIP LOCKED passes it, no wait.
    let mut holder = mysql_async::Conn::new(opts.clone()).await.unwrap();
    holder.query_drop("START TRANSACTION").await.unwrap();
    holder
        .query_drop(format!(
            "SELECT * FROM `{DB}`.`db0` WHERE k = 'a' FOR UPDATE"
        ))
        .await
        .unwrap();
    let o = SweepOpts {
        batch_rows: 2,
        inline_max: 128,
    };
    let started = std::time::Instant::now();
    let swept = sweep::expired(&mut sql, &ns, o).await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(swept.keys, 3, "{swept:?}");
    assert_eq!(swept.elements, 10 + 300 + 2, "{swept:?}");
    holder.query_drop("ROLLBACK").await.unwrap();
    let swept = sweep::expired(&mut sql, &ns, o).await.unwrap();
    assert_eq!(swept.keys, 1);
    assert_eq!(count(&mut root, "db0").await, 2);
    // Orphans: the stray generation of a live key and a key that is gone.
    let (rows, gens) = sweep::orphans(&mut sql, &ns, o).await.unwrap();
    assert_eq!((rows, gens), (2, 2));
    assert_eq!(count(&mut root, "__sprig_ke_db0").await, 1);
    assert_eq!(count(&mut root, "__sprig_kz_db0").await, 0);
    let v: String = q(s(&["HGET", "livehash", "f"]))
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(v, "v");
    // The next expiry, and discovery (which lists tenant schemas only).
    assert_eq!(sweep::next_expiry_ms(&mut sql, &ns).await.unwrap(), None);
    let _: () = q(s(&["SET", "later", "1", "EX", "100"]))
        .query_async(&mut c)
        .await
        .unwrap();
    let ms = sweep::next_expiry_ms(&mut sql, &ns).await.unwrap().unwrap();
    assert!((90_000..=100_000).contains(&ms), "{ms}");
    let found = sweep::discover(&mut sql).await.unwrap();
    assert!(!found.iter().any(|n| n.schema == DB));
    root.query_drop(format!("DROP DATABASE `{DB}`"))
        .await
        .unwrap();
}

/// The worker's `kv_sweep` job through a gateway: expired keys written over
/// the Redis listener leave the physical tables within the purge interval.
///
/// - `SPRIG_TEST_REDIS_SPRIG`: the gateway's Redis listener as a tenant.
/// - `SPRIG_TEST_KV_MYSQL`: the tenant's cluster as root.
/// - `SPRIG_TEST_SWEEP_SCHEMA`: the physical schema of its Redis database.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_sweeps_through_a_gateway() {
    let (Some(url), Some(root_url), Some(schema)) = (
        std::env::var("SPRIG_TEST_REDIS_SPRIG").ok(),
        std::env::var("SPRIG_TEST_KV_MYSQL").ok(),
        std::env::var("SPRIG_TEST_SWEEP_SCHEMA").ok(),
    ) else {
        eprintln!(
            "SPRIG_TEST_REDIS_SPRIG, SPRIG_TEST_KV_MYSQL or SPRIG_TEST_SWEEP_SCHEMA not set: skipped"
        );
        return;
    };
    let client = redis::Client::open(url).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("r6sweep:s")
        .arg("1")
        .arg("PX")
        .arg(300)
        .query_async(&mut c)
        .await
        .unwrap();
    let mut h = redis::cmd("HSET");
    h.arg("r6sweep:h");
    for i in 0..20 {
        h.arg(format!("f{i}")).arg(i);
    }
    let _: i64 = h.query_async(&mut c).await.unwrap();
    let mut l = redis::cmd("RPUSH");
    l.arg("r6sweep:l");
    for i in 0..200 {
        l.arg(i);
    }
    let _: i64 = l.query_async(&mut c).await.unwrap();
    for k in ["r6sweep:h", "r6sweep:l"] {
        let _: i64 = redis::cmd("PEXPIRE")
            .arg(k)
            .arg(300)
            .query_async(&mut c)
            .await
            .unwrap();
    }
    let mut root = mysql_async::Conn::new(mysql_async::Opts::from_url(&root_url).unwrap())
        .await
        .unwrap();
    let rows = |t: &str| format!("SELECT COUNT(*) FROM `{schema}`.`{t}` WHERE k LIKE 'r6sweep:%'");
    let t0 = std::time::Instant::now();
    let mut gone = None;
    while t0.elapsed() < Duration::from_secs(40) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let keys: u64 = root.query_first(rows("db0")).await.unwrap().unwrap();
        let elems: u64 = root
            .query_first(rows("__sprig_ke_db0"))
            .await
            .unwrap()
            .unwrap();
        if keys == 0 && elems == 0 {
            gone = Some(t0.elapsed());
            break;
        }
    }
    let gone = gone.expect("the sweeper never removed the expired rows");
    eprintln!(
        "expired rows swept {:.1} s after writing",
        gone.as_secs_f64()
    );
}
