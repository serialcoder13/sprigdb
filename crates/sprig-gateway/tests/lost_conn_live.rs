//! A backend connection killed between statements (chapter 06, "Failure
//! handling", "When a lost connection is retried"): when nothing pinned the
//! session to it, the next statement runs on a new connection with the
//! session's state replayed, and the client sees nothing. Inside a
//! transaction, with other pinned state, or when the connection dies while
//! a write runs, the client gets the chapter's error and the session goes
//! on. Through both ports, against the single-node cluster.
//!
//! ```sh
//! deploy/compose/gateway-dev.sh up   # SPRIG_GATEWAY_LOGIN_MODE=accounts_file
//! export SPRIG_TEST_GATEWAY=127.0.0.1:13306,127.0.0.1:13307   # mysql, mariadb persona
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! cargo test -p sprig-gateway --test lost_conn_live -- --nocapture --test-threads 1
//! ```
//!
//! The login is `deploy/compose/gateway-accounts.dev.toml`'s `app`
//! (`dc-a-c1`, the single node).

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_proto_mysql::Persona;
use sprig_testkit::{skip, unique_ident};

const GATEWAY: &str = "SPRIG_TEST_GATEWAY";
const USER: &str = "app";
const PASSWORD: &str = "sprig-dev-app";

fn opts(addr: &str, user: &str, pass: &str) -> OptsBuilder {
    let (host, port) = addr.rsplit_once(':').unwrap();
    OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(pass))
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true))
}

struct Rig {
    eps: Vec<(String, Persona)>,
    side: Conn,
}

async fn rig(test: &str) -> Option<Rig> {
    let Some(gw) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let Some(direct) = skip::var(skip::MARIADB_SINGLE) else {
        skip::report_missing(skip::MARIADB_SINGLE, test);
        return None;
    };
    let url = skip::urls(&direct).remove(0);
    let rest = url.strip_prefix("mysql://").unwrap();
    let (cred, addr) = rest.rsplit_once('@').unwrap();
    let (user, pass) = cred.split_once(':').unwrap();
    let side = Conn::new(opts(addr.trim_end_matches('/'), user, pass))
        .await
        .unwrap();
    let eps = skip::urls(&gw)
        .into_iter()
        .zip([Persona::MySql84, Persona::MariaDb])
        .collect();
    Some(Rig { eps, side })
}

async fn gw(addr: &str) -> Conn {
    Conn::new(opts(addr, USER, PASSWORD))
        .await
        .unwrap_or_else(|e| panic!("connect {USER}@{addr}: {e}"))
}

/// The backend thread the session's statement runs on.
async fn thread(c: &mut Conn) -> u64 {
    c.query_first("SELECT @@pseudo_thread_id")
        .await
        .unwrap()
        .unwrap()
}

/// Kills a backend connection from the side, and waits until the server
/// has closed it.
async fn kill(side: &mut Conn, id: u64) {
    side.query_drop(format!("KILL CONNECTION {id}"))
        .await
        .unwrap();
    for _ in 0..50 {
        let n: Option<u64> = side
            .query_first(format!(
                "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = {id}"
            ))
            .await
            .unwrap();
        if n == Some(0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The gateway's reactor sees the close.
    tokio::time::sleep(Duration::from_millis(100)).await;
}

fn code(e: &mysql_async::Error) -> (u16, String) {
    match e {
        mysql_async::Error::Server(s) => (s.code, s.state.clone()),
        other => panic!("expected a server error, got {other}"),
    }
}

/// A schema for one test, through the gateway.
async fn schema(addr: &str, prefix: &str) -> String {
    let name = unique_ident(prefix);
    let mut c = gw(addr).await;
    c.query_drop(format!("CREATE DATABASE `{name}`"))
        .await
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE `{name}`.t (id INT PRIMARY KEY, v VARCHAR(20))"
    ))
    .await
    .unwrap();
    c.disconnect().await.unwrap();
    name
}

async fn drop_schema(addr: &str, name: &str) {
    let mut c = gw(addr).await;
    c.query_drop(format!("DROP DATABASE IF EXISTS `{name}`"))
        .await
        .unwrap();
    c.disconnect().await.unwrap();
}

/// Safe cases: the session holds nothing on its connection, the statement
/// never reached the killed one. A read, an autocommit write, a prepared
/// write and `autocommit = 0` before its first statement each run on a
/// new connection, with the schema, user variables, character set and
/// time zone replayed. The session's own time zone puts its connection in
/// a pool bucket of its own, so its next statement meets the killed one.
#[tokio::test]
async fn statements_after_a_kill_run_on_a_new_connection() {
    let Some(Rig { eps, mut side }) = rig("statements_after_a_kill_run_on_a_new_connection").await
    else {
        return;
    };
    for (addr, persona) in &eps {
        let db = schema(addr, "gwfix_lost_safe").await;
        let mut c = gw(addr).await;
        c.query_drop(format!("USE `{db}`")).await.unwrap();
        for s in [
            "SET @gwfix_v = 'kept'",
            "SET time_zone = '+05:17'",
            "SET NAMES latin1",
        ] {
            c.query_drop(s).await.unwrap();
        }
        let stmt = c.prep("INSERT INTO t VALUES (?, ?)").await.unwrap();
        let steps = [
            "read",
            "autocommit write",
            "prepared write",
            "autocommit = 0",
        ];
        for (i, what) in (1u32..).zip(steps) {
            let ctx = format!("{persona:?} {what}");
            let before = thread(&mut c).await;
            kill(&mut side, before).await;
            match what {
                "read" => {
                    let n: Option<u64> = c
                        .query_first("SELECT COUNT(*) FROM t")
                        .await
                        .unwrap_or_else(|e| panic!("{ctx}: {e}"));
                    assert_eq!(n, Some(0), "{ctx}");
                }
                "autocommit write" => c
                    .query_drop(format!("INSERT INTO t VALUES ({i}, @gwfix_v)"))
                    .await
                    .unwrap_or_else(|e| panic!("{ctx}: {e}")),
                "prepared write" => c
                    .exec_drop(&stmt, (i, "prepared"))
                    .await
                    .unwrap_or_else(|e| panic!("{ctx}: {e}")),
                "autocommit = 0" => {
                    // SET autocommit runs first, on a connection the next
                    // kill then closes: nothing is open yet.
                    c.query_drop("SET autocommit = 0").await.unwrap();
                    let t = thread(&mut c).await;
                    c.query_drop("COMMIT").await.unwrap();
                    kill(&mut side, t).await;
                    c.query_drop(format!("INSERT INTO t VALUES ({i}, 'rolled back')"))
                        .await
                        .unwrap_or_else(|e| panic!("{ctx}: {e}"));
                    c.query_drop("ROLLBACK").await.unwrap();
                    c.query_drop("SET autocommit = 1").await.unwrap();
                }
                _ => unreachable!(),
            }
            let after = thread(&mut c).await;
            assert_ne!(after, before, "{ctx}: still on the killed connection");
            let (schema_now, v, tz, cs): (String, String, String, String) = c
                .query_first("SELECT DATABASE(), @gwfix_v, @@time_zone, @@character_set_client")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                (schema_now.as_str(), v.as_str(), tz.as_str(), cs.as_str()),
                (db.as_str(), "kept", "+05:17", "latin1"),
                "{ctx}: state after the retry"
            );
        }
        // Each write ran exactly once; the rolled-back one not at all.
        let rows: Vec<(u32, String)> = c.query("SELECT id, v FROM t ORDER BY id").await.unwrap();
        assert_eq!(
            rows,
            vec![(2, "kept".to_owned()), (3, "prepared".to_owned())],
            "{persona:?}"
        );
        c.disconnect().await.unwrap();
        drop_schema(addr, &db).await;
    }
    side.disconnect().await.unwrap();
}

/// Unsafe cases, between statements: an open transaction is gone (1213,
/// 40001, rolled back), and so are a temporary table and a `GET_LOCK()`
/// lock (39033, 08S01). The session goes on, outside any transaction.
#[tokio::test]
async fn pinned_state_lost_between_statements_fails_cleanly() {
    let Some(Rig { eps, mut side }) =
        rig("pinned_state_lost_between_statements_fails_cleanly").await
    else {
        return;
    };
    for (addr, persona) in &eps {
        let db = schema(addr, "gwfix_lost_pinned").await;
        let mut c = gw(addr).await;
        c.query_drop(format!("USE `{db}`")).await.unwrap();
        let cases: [(&str, &str, u16, &str); 3] = [
            (
                "transaction",
                "BEGIN; INSERT INTO t VALUES (1, 'lost')",
                1213,
                "40001",
            ),
            (
                "temporary table",
                "CREATE TEMPORARY TABLE tmp (a INT)",
                39033,
                "08S01",
            ),
            ("GET_LOCK", "DO GET_LOCK('gwfix_lost', 0)", 39033, "08S01"),
        ];
        for (what, setup, want, state) in cases {
            let ctx = format!("{persona:?} {what}");
            for s in setup.split("; ") {
                c.query_drop(s).await.unwrap();
            }
            let held = thread(&mut c).await;
            kill(&mut side, held).await;
            let e = c
                .query_drop("INSERT INTO t VALUES (2, 'after')")
                .await
                .expect_err(&ctx);
            assert_eq!(code(&e), (want, state.to_owned()), "{ctx}");
            // Nothing ran: the statement failed before it was sent.
            let n: Option<u64> = c.query_first("SELECT COUNT(*) FROM t").await.unwrap();
            assert_eq!(n, Some(0), "{ctx}: rows");
            assert_ne!(thread(&mut c).await, held, "{ctx}");
        }
        c.disconnect().await.unwrap();
        drop_schema(addr, &db).await;
    }
    side.disconnect().await.unwrap();
}

/// Unsafe case, during a statement: an autocommit write whose connection
/// dies while it runs may have taken effect, so it isn't retried: 39032
/// (08S01, outcome unknown). The session goes on.
#[tokio::test]
async fn a_write_lost_while_running_is_not_retried() {
    let Some(Rig { eps, mut side }) = rig("a_write_lost_while_running_is_not_retried").await else {
        return;
    };
    for (addr, persona) in &eps {
        let db = schema(addr, "gwfix_lost_running").await;
        let mut c = gw(addr).await;
        c.query_drop(format!("USE `{db}`")).await.unwrap();
        let marker = unique_ident("gwfix_marker");
        let sql = format!("INSERT INTO t SELECT 1, '{marker}' FROM DUAL WHERE SLEEP(5) = 0");
        let run = tokio::spawn(async move {
            let r = c.query_drop(sql).await;
            (c, r)
        });
        let mut id = None;
        for _ in 0..100 {
            id = side
                .query_first(format!(
                    "SELECT ID FROM information_schema.PROCESSLIST \
                     WHERE INFO LIKE '%{marker}%' AND INFO NOT LIKE '%PROCESSLIST%'"
                ))
                .await
                .unwrap();
            if id.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let id: u64 = id.expect("the write is running");
        side.query_drop(format!("KILL CONNECTION {id}"))
            .await
            .unwrap();
        let (mut c, r) = run.await.unwrap();
        let e = r.expect_err("the write was cut off");
        assert_eq!(code(&e), (39032, "08S01".to_owned()), "{persona:?}");
        let n: Option<u64> = c.query_first("SELECT COUNT(*) FROM t").await.unwrap();
        assert!(n.is_some(), "{persona:?}: the session goes on");
        c.disconnect().await.unwrap();
        drop_schema(addr, &db).await;
    }
    side.disconnect().await.unwrap();
}
