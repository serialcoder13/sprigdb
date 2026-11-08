//! The pass-through executor end to end (decision D3; chapter 05 build
//! steps 7-9): real clients through a running gateway against each Compose
//! topology. Start the gateway first, then:
//!
//! ```sh
//! deploy/compose/gateway-dev.sh up
//! export SPRIG_TEST_GATEWAY=127.0.0.1:13306,127.0.0.1:13307   # mysql, mariadb persona
//! # Side connections straight to the servers (KILL, replication lag):
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! export SPRIG_TEST_MARIADB_REPLICATION=mysql://root:sprig-dev-root@127.0.0.1:13311,mysql://root:sprig-dev-root@127.0.0.1:13312,mysql://root:sprig-dev-root@127.0.0.1:13313
//! export SPRIG_TEST_MARIADB_GALERA=mysql://root:sprig-dev-root@127.0.0.1:13321,mysql://root:sprig-dev-root@127.0.0.1:13322,mysql://root:sprig-dev-root@127.0.0.1:13323
//! cargo test -p sprig-gateway --test exec_live -- --nocapture --test-threads 1
//! ```
//!
//! The logins are `deploy/compose/gateway-accounts.dev.toml`'s: `app`
//! (single node), `app_repl` (primary and replicas), `app_galera`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    missing_docs
)]

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use bytes::Bytes;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row, SslOpts, Value as MyValue};
use sprig_proto_mysql::client::{
    ClientConfig, ClientError, MysqlClient, PacketKind, TlsMode, TlsOptions,
};
use sprig_proto_mysql::command::{
    BULK_SEND_TYPES_TO_SERVER, BoundParam, BulkExecute, BulkRows, BulkValue, CURSOR_TYPE_READ_ONLY,
    Command, ExecuteParams, FieldList, LongData, ParamValue, StmtExecute,
};
use sprig_proto_mysql::resultset::{column_type, decode_binary_row};
use sprig_proto_mysql::{Caps, ColumnDef, Persona, StatusFlags, Value};
use sprig_testkit::{skip, unique_ident};

const PASSWORD: &str = "sprig-dev-app";
const GATEWAY: &str = "SPRIG_TEST_GATEWAY";

/// A login and the variable with its servers' URLs (side connections).
struct Topo {
    user: &'static str,
    servers: &'static str,
}

const TOPOS: &[Topo] = &[
    Topo {
        user: "app",
        servers: skip::MARIADB_SINGLE,
    },
    Topo {
        user: "app_repl",
        servers: skip::MARIADB_REPLICATION,
    },
    Topo {
        user: "app_galera",
        servers: skip::MARIADB_GALERA,
    },
];

/// The gateway's endpoints: (address, persona).
fn endpoints(test: &str) -> Option<Vec<(String, Persona)>> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let addrs = skip::urls(&v);
    let personas = [Persona::MySql84, Persona::MariaDb];
    Some(addrs.into_iter().zip(personas).collect())
}

fn split_addr(addr: &str) -> (String, u16) {
    let (h, p) = addr.rsplit_once(':').unwrap();
    (h.to_owned(), p.parse().unwrap())
}

fn opts(addr: &str, user: &str, pass: &str, db: Option<&str>) -> OptsBuilder {
    let (host, port) = split_addr(addr);
    OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(pass))
        .db_name(db)
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true))
}

/// A `mysql_async` connection through the gateway.
async fn gw(addr: &str, user: &str, db: Option<&str>) -> Conn {
    Conn::new(opts(addr, user, PASSWORD, db))
        .await
        .unwrap_or_else(|e| panic!("connect {user}@{addr}: {e}"))
}

/// Side connections straight to each server of a topology.
async fn servers(var: &str) -> Option<Vec<Conn>> {
    let v = skip::var(var)?;
    let mut out = Vec::new();
    for u in skip::urls(&v) {
        let rest = u.strip_prefix("mysql://").unwrap();
        let (cred, addr) = rest.rsplit_once('@').unwrap();
        let (user, pass) = cred.split_once(':').unwrap();
        out.push(
            Conn::new(opts(addr.trim_end_matches('/'), user, pass, None))
                .await
                .unwrap(),
        );
    }
    Some(out)
}

/// The protocol client through the gateway, for the commands `mysql_async`
/// doesn't send (cursors, long data, field list, bulk, LOCAL INFILE).
async fn raw(
    addr: &str,
    user: &str,
    db: Option<&str>,
    f: impl FnOnce(&mut ClientConfig),
) -> MysqlClient {
    let mut cfg = ClientConfig::new(user, PASSWORD);
    cfg.tls = TlsMode::Required;
    cfg.tls_config = Some(
        TlsOptions {
            skip_verify: true,
            ..TlsOptions::default()
        }
        .build()
        .unwrap(),
    );
    cfg.database = db.map(str::to_owned);
    cfg.multi_statements = true;
    f(&mut cfg);
    MysqlClient::connect(addr, &cfg).await.unwrap()
}

fn server_code(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(s) => s.code,
        other => panic!("expected a server error, got {other}"),
    }
}

/// A fresh schema for one test on one topology, dropped by [`Db::drop`].
struct Db {
    name: String,
}

impl Db {
    async fn create(addr: &str, user: &str, prefix: &str) -> Self {
        let name = unique_ident(prefix);
        let mut c = gw(addr, user, None).await;
        c.query_drop(format!("CREATE DATABASE `{name}`"))
            .await
            .unwrap();
        c.disconnect().await.unwrap();
        Self { name }
    }

    async fn drop(self, addr: &str, user: &str) {
        let mut c = gw(addr, user, None).await;
        c.query_drop(format!("DROP DATABASE IF EXISTS `{}`", self.name))
            .await
            .unwrap();
        c.disconnect().await.unwrap();
    }
}

/// Every topology through every endpoint.
fn matrix(eps: &[(String, Persona)]) -> Vec<(String, Persona, &'static Topo)> {
    let mut v = Vec::new();
    for (addr, p) in eps {
        for t in TOPOS {
            v.push((addr.clone(), *p, t));
        }
    }
    v
}

const TYPES_TABLE: &str = "CREATE TABLE ty (
    id INT PRIMARY KEY, ti TINYINT, si SMALLINT, mi MEDIUMINT, i INT, bi BIGINT, ubi BIGINT UNSIGNED,
    de DECIMAL(20,6), f FLOAT, d DOUBLE, vc VARCHAR(100), tx TEXT, vb VARBINARY(300), bl BLOB,
    dt DATE, dtt DATETIME(6), ts TIMESTAMP NULL, tm TIME, js JSON, bt BIT(10), en ENUM('a','b'), st SET('x','y'))";
const TYPES_INSERT: &str = "INSERT INTO ty VALUES
    (1, -128, -32768, -8388608, -2147483648, -9223372036854775808, 18446744073709551615,
     -12345678901234.567890, 1.5, -0.00225, 'héllo \\'q\\' 🙂', REPEAT('t🙂', 300), X'00FF7F80', X'000102FFFE',
     '2024-02-29', '2024-02-29 23:59:58.123456', '2024-02-29 12:34:56', '838:59:59', '{\"a\": [1, 2]}', b'1010101010', 'b', 'x,y'),
    (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)";

/// Text and binary results of every common type, compared with the same
/// statements run straight on the cluster's writer.
#[tokio::test]
async fn text_and_binary_results_match_the_backend() {
    let Some(eps) = endpoints("text_and_binary_results_match_the_backend") else {
        return;
    };
    for (addr, persona, topo) in matrix(&eps) {
        let db = Db::create(&addr, topo.user, "gw_types").await;
        let mut c = gw(&addr, topo.user, Some(&db.name)).await;
        c.query_drop(TYPES_TABLE).await.unwrap();
        c.query_drop(TYPES_INSERT).await.unwrap();
        let sel = "SELECT * FROM ty ORDER BY id";
        let text: Vec<Row> = c.query(sel).await.unwrap();
        let bin: Vec<Row> = c.exec(sel, ()).await.unwrap();
        assert_eq!(text.len(), 2, "{persona:?} {}", topo.user);
        let by_id: Vec<Row> = c.exec("SELECT * FROM ty WHERE id = ?", (1,)).await.unwrap();
        assert_eq!(by_id.len(), 1);
        if let Some(mut side) = servers(topo.servers).await {
            let w = &mut side[0];
            let direct_text: Vec<Row> = w
                .query(format!("SELECT * FROM `{}`.ty ORDER BY id", db.name))
                .await
                .unwrap();
            let direct_bin: Vec<Row> = w
                .exec(format!("SELECT * FROM `{}`.ty ORDER BY id", db.name), ())
                .await
                .unwrap();
            let vals = |rows: &[Row]| {
                rows.iter()
                    .map(|r| r.clone().unwrap())
                    .collect::<Vec<Vec<MyValue>>>()
            };
            assert_eq!(
                vals(&text),
                vals(&direct_text),
                "text rows differ ({persona:?}, {})",
                topo.user
            );
            assert_eq!(
                vals(&bin),
                vals(&direct_bin),
                "binary rows differ ({persona:?}, {})",
                topo.user
            );
            // Column metadata: names, types and flags as the server sent them.
            let cols = |rows: &[Row]| {
                rows[0]
                    .columns_ref()
                    .iter()
                    .map(|c| (c.name_str().into_owned(), c.column_type(), c.flags()))
                    .collect::<Vec<_>>()
            };
            assert_eq!(cols(&text), cols(&direct_text));
        }
        c.disconnect().await.unwrap();
        db.drop(&addr, topo.user).await;
    }
}

/// Deterministic incompressible bytes.
fn blob(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

/// Rows above one protocol packet (16 MiB) in both directions, text and
/// binary.
#[tokio::test]
async fn large_multi_packet_rows() {
    let Some(eps) = endpoints("large_multi_packet_rows") else {
        return;
    };
    // The servers need max_allowed_packet above 16 MiB (new connections
    // only: the gateway's pooled connections may predate this).
    for t in TOPOS {
        if let Some(mut side) = servers(t.servers).await {
            for s in &mut side {
                let _ = s
                    .query_drop("SET GLOBAL max_allowed_packet = 67108864")
                    .await;
            }
        }
    }
    for (addr, persona, topo) in matrix(&eps) {
        let db = Db::create(&addr, topo.user, "gw_large").await;
        let mut c = gw(&addr, topo.user, Some(&db.name)).await;
        c.query_drop("CREATE TABLE b (id INT PRIMARY KEY, v LONGBLOB)")
            .await
            .unwrap();
        for (id, len) in [(1u32, 4 << 20), (2, (16 << 20) + 1024)] {
            let data = blob(len, u64::from(id));
            c.exec_drop("INSERT INTO b VALUES (?, ?)", (id, data.clone()))
                .await
                .unwrap();
            let text: Option<Vec<u8>> = c
                .query_first(format!("SELECT v FROM b WHERE id = {id}"))
                .await
                .unwrap();
            assert!(
                text.as_deref() == Some(&data[..]),
                "text {len} bytes ({persona:?}, {})",
                topo.user
            );
            let bin: Option<Vec<u8>> = c
                .exec_first("SELECT v FROM b WHERE id = ?", (id,))
                .await
                .unwrap();
            assert!(
                bin.as_deref() == Some(&data[..]),
                "binary {len} bytes ({persona:?}, {})",
                topo.user
            );
            let len_srv: Option<u64> = c
                .query_first(format!("SELECT LENGTH(v) FROM b WHERE id = {id}"))
                .await
                .unwrap();
            assert_eq!(len_srv, Some(len as u64));
        }
        c.disconnect().await.unwrap();
        db.drop(&addr, topo.user).await;
    }
}

/// Multi-statements: every result streams, an error stops the rest, and a
/// session without multi-statements gets 1064. `CALL` returns several
/// resultsets, through the text and the binary protocol.
#[tokio::test]
async fn multi_statements_and_call() {
    let Some(eps) = endpoints("multi_statements_and_call") else {
        return;
    };
    for (addr, persona, topo) in matrix(&eps) {
        let ctx = format!("{persona:?} {}", topo.user);
        let db = Db::create(&addr, topo.user, "gw_multi").await;
        let mut c = gw(&addr, topo.user, Some(&db.name)).await;
        {
            let mut r = c.query_iter("SELECT 1; SELECT 2, 'two'; CREATE TABLE m (id INT); INSERT INTO m VALUES (1), (2)").await.unwrap();
            let a: Vec<(i64,)> = r.collect().await.unwrap();
            let b: Vec<(i64, String)> = r.collect().await.unwrap();
            assert_eq!(a, [(1,)], "{ctx}");
            assert_eq!(b, [(2, "two".to_owned())], "{ctx}");
            let _: Vec<Row> = r.collect().await.unwrap();
            let _: Vec<Row> = r.collect().await.unwrap();
            assert_eq!(r.affected_rows(), 2, "{ctx}");
            r.drop_result().await.unwrap();
        }
        // An error in the middle: the first result arrives, then the error.
        {
            let mut r = c
                .query_iter("SELECT 10; SELECT nosuch; SELECT 30")
                .await
                .unwrap();
            let a: Vec<(i64,)> = r.collect().await.unwrap();
            assert_eq!(a, [(10,)]);
            let e = r.collect::<Row>().await.unwrap_err();
            assert_eq!(server_code(&e), 1054, "{ctx}");
        }
        let one: Option<i64> = c.query_first("SELECT 1").await.unwrap();
        assert_eq!(one, Some(1), "the session goes on after an error");

        // A procedure with two resultsets, created in a multi-statement
        // whose body has semicolons.
        c.query_drop(
            "CREATE PROCEDURE p(IN n INT) BEGIN SELECT n; SELECT n + 1, 'x'; END; SELECT 'created'",
        )
        .await
        .unwrap();
        {
            let mut r = c.query_iter("CALL p(5)").await.unwrap();
            let a: Vec<(i64,)> = r.collect().await.unwrap();
            let b: Vec<(i64, String)> = r.collect().await.unwrap();
            assert_eq!(a, [(5,)], "{ctx}");
            assert_eq!(b, [(6, "x".to_owned())], "{ctx}");
            r.drop_result().await.unwrap();
        }
        {
            let mut r = c.exec_iter("CALL p(?)", (7,)).await.unwrap();
            let a: Vec<(i64,)> = r.collect().await.unwrap();
            let b: Vec<(i64, String)> = r.collect().await.unwrap();
            assert_eq!(a, [(7,)], "prepared CALL {ctx}");
            assert_eq!(b, [(8, "x".to_owned())], "prepared CALL {ctx}");
            r.drop_result().await.unwrap();
        }
        // OUT and INOUT parameters into user variables, read back by a
        // later plain read, which may run on another node (a replica, a
        // Galera node): the gateway captures them after the call.
        c.query_drop(
            "CREATE PROCEDURE q(IN a INT, OUT b INT, INOUT c INT) BEGIN SET b = a * 10; SET c = c + a; END",
        )
        .await
        .unwrap();
        for _ in 0..5 {
            c.query_drop("SET @c = 5").await.unwrap();
            c.query_drop("CALL q(1, @b, @c)").await.unwrap();
            let bc: Option<(Option<i64>, Option<i64>)> =
                c.query_first("SELECT @b, @c").await.unwrap();
            assert_eq!(bc, Some((Some(10), Some(6))), "OUT/INOUT after CALL {ctx}");
        }
        c.disconnect().await.unwrap();

        // Without multi-statements: 1064, and nothing ran.
        let mut r = raw(&addr, topo.user, Some(&db.name), |cfg| {
            cfg.multi_statements = false;
        })
        .await;
        let e = r
            .query("INSERT INTO m VALUES (3); SELECT 1")
            .await
            .unwrap_err();
        assert_eq!(e.server_error().map(|e| e.code), Some(1064), "{ctx}");
        let n = r
            .query_first("SELECT COUNT(*) FROM m")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n[0].as_deref(), Some(&b"2"[..]), "{ctx}");
        r.quit().await;
        db.drop(&addr, topo.user).await;
    }
}

fn int_param(v: i64) -> BoundParam {
    BoundParam {
        param_type: u16::from(column_type::LONGLONG),
        name: Bytes::new(),
        value: ParamValue::Value(Value::Int(v)),
    }
}

fn execute_cmd(id: u32, flags: u8, bound: bool, params: Vec<BoundParam>) -> Command {
    Command::StmtExecute(StmtExecute {
        stmt_id: id,
        flags,
        iterations: 1,
        raw_params: Bytes::new(),
        params: Some(ExecuteParams {
            new_params_bound: bound,
            params,
        }),
    })
}

/// Runs a binary statement and returns its first resultset's rows.
async fn exec_rows(
    c: &mut MysqlClient,
    cmd: &Command,
) -> Result<(Vec<ColumnDef>, Vec<Vec<Value>>), ClientError> {
    c.send_command(cmd).await?;
    let rs = c.collect().await?;
    let Some(first) = rs.into_iter().find(|r| !r.columns.is_empty()) else {
        return Ok((Vec::new(), Vec::new()));
    };
    let rows = first
        .rows
        .iter()
        .map(|r| decode_binary_row(r, &first.columns).unwrap())
        .collect();
    Ok((first.columns, rows))
}

/// Kills backend connection `id` on whichever server has it.
async fn kill_backend(side: &mut [Conn], id: u64) -> bool {
    let mut killed = false;
    for s in side {
        if s.query_drop(format!("KILL CONNECTION {id}")).await.is_ok() {
            killed = true;
        }
    }
    killed
}

/// A prepared statement keeps working when the session's next execute
/// lands on another backend connection: the backend statement is
/// re-prepared there and the parameter types re-sent, although the client
/// doesn't send them again. Also MariaDB's direct execute (0xFFFFFFFF).
#[tokio::test]
async fn prepared_statements_move_between_backend_connections() {
    let Some(eps) = endpoints("prepared_statements_move_between_backend_connections") else {
        return;
    };
    for (addr, persona, topo) in matrix(&eps) {
        let ctx = format!("{persona:?} {}", topo.user);
        let Some(mut side) = servers(topo.servers).await else {
            skip::report_missing(
                topo.servers,
                "prepared_statements_move_between_backend_connections",
            );
            continue;
        };
        let mut c = raw(&addr, topo.user, None, |_| {}).await;
        let st = c
            .prepare(b"SELECT CAST(? AS SIGNED) + 1, @@pseudo_thread_id")
            .await
            .unwrap();
        assert_eq!(st.ok.params, 1);
        assert_eq!(st.ok.columns, 2);
        let id = st.ok.stmt_id;
        let (_, rows) = exec_rows(&mut c, &execute_cmd(id, 0, true, vec![int_param(41)]))
            .await
            .unwrap();
        assert_eq!(rows[0][0], Value::Int(42), "{ctx}");
        let conn1 = rows[0][1].clone();
        // @@pseudo_thread_id is the backend thread (CONNECTION_ID() is the
        // gateway's connection id).
        let conn1_id = match conn1 {
            Value::UInt(v) => v,
            Value::Int(v) => u64::try_from(v).unwrap(),
            ref other => panic!("{other:?}"),
        };
        // Take the backend connection away.
        assert!(
            kill_backend(&mut side, conn1_id).await,
            "{ctx}: backend connection {conn1_id} not found"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        // No types this time: the gateway must re-bind them.
        let (_, rows) = exec_rows(&mut c, &execute_cmd(id, 0, false, vec![int_param(99)]))
            .await
            .unwrap();
        assert_eq!(rows[0][0], Value::Int(100), "{ctx}");
        assert_ne!(rows[0][1], conn1, "{ctx}: still on the killed connection");
        // And again on whatever connection comes next.
        for i in 0..5 {
            let (_, rows) = exec_rows(&mut c, &execute_cmd(id, 0, false, vec![int_param(i)]))
                .await
                .unwrap();
            assert_eq!(rows[0][0], Value::Int(i + 1), "{ctx}");
        }
        // Closing and preparing again gives a new id; the old one is gone.
        c.close_statement(id).await.unwrap();
        let e = exec_rows(&mut c, &execute_cmd(id, 0, true, vec![int_param(1)]))
            .await
            .unwrap_err();
        assert_eq!(e.server_error().map(|e| e.code), Some(1243), "{ctx}");
        if persona == Persona::MariaDb {
            // Direct execute: prepare, then execute 0xFFFFFFFF (the last one).
            let st2 = c.prepare(b"SELECT CAST(? AS SIGNED) * 2").await.unwrap();
            assert_ne!(st2.ok.stmt_id, u32::MAX);
            let (_, rows) = exec_rows(&mut c, &execute_cmd(u32::MAX, 0, true, vec![int_param(21)]))
                .await
                .unwrap();
            assert_eq!(rows[0][0], Value::Int(42), "{ctx}: direct execute");
        }
        c.quit().await;
    }
}

/// `COM_STMT_SEND_LONG_DATA` pieces are buffered and reach the backend
/// with the execute, wherever it runs.
#[tokio::test]
async fn long_data_reaches_the_backend() {
    let Some(eps) = endpoints("long_data_reaches_the_backend") else {
        return;
    };
    for (addr, persona, topo) in matrix(&eps) {
        let ctx = format!("{persona:?} {}", topo.user);
        let db = Db::create(&addr, topo.user, "gw_longdata").await;
        let mut c = raw(&addr, topo.user, Some(&db.name), |_| {}).await;
        c.query_drop("CREATE TABLE ld (id INT PRIMARY KEY, a TEXT, b LONGBLOB)")
            .await
            .unwrap();
        let st = c.prepare(b"INSERT INTO ld VALUES (?, ?, ?)").await.unwrap();
        let id = st.ok.stmt_id;
        let piece = blob(300_000, 7);
        for p in [
            &piece[..100_000],
            &piece[100_000..200_000],
            &piece[200_000..],
        ] {
            c.send_command(&Command::StmtSendLongData(LongData {
                stmt_id: id,
                param: 2,
                data: Bytes::copy_from_slice(p),
            }))
            .await
            .unwrap();
        }
        c.send_command(&Command::StmtSendLongData(LongData {
            stmt_id: id,
            param: 1,
            data: Bytes::from_static("text 🙂".as_bytes()),
        }))
        .await
        .unwrap();
        let params = vec![
            int_param(1),
            BoundParam {
                param_type: u16::from(column_type::STRING),
                name: Bytes::new(),
                value: ParamValue::LongData,
            },
            BoundParam {
                param_type: u16::from(column_type::BLOB),
                name: Bytes::new(),
                value: ParamValue::LongData,
            },
        ];
        c.send_command(&execute_cmd(id, 0, true, params))
            .await
            .unwrap();
        let rs = c.collect().await.unwrap();
        assert_eq!(rs[0].ok.as_ref().map(|o| o.affected_rows), Some(1), "{ctx}");
        // The next execute has no long data: plain values.
        let mut next = vec![int_param(2)];
        next.push(BoundParam {
            param_type: u16::from(column_type::VAR_STRING),
            name: Bytes::new(),
            value: ParamValue::Value(Value::Bytes(Bytes::from_static(b"plain"))),
        });
        next.push(BoundParam {
            param_type: u16::from(column_type::VAR_STRING),
            name: Bytes::new(),
            value: ParamValue::Value(Value::Null),
        });
        c.send_command(&execute_cmd(id, 0, true, next))
            .await
            .unwrap();
        c.collect().await.unwrap();
        let row = c
            .query_first("SELECT a, SHA2(b, 256), LENGTH(b) FROM ld WHERE id = 1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row[0].as_deref(), Some("text 🙂".as_bytes()), "{ctx}");
        assert_eq!(row[2].as_deref(), Some(&b"300000"[..]), "{ctx}");
        let row2 = c
            .query_first("SELECT a, b FROM ld WHERE id = 2")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row2[0].as_deref(), Some(&b"plain"[..]));
        assert_eq!(row2[1], None);
        let direct = c
            .query_first("SELECT COUNT(*) FROM ld")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(direct[0].as_deref(), Some(&b"2"[..]));
        c.quit().await;
        db.drop(&addr, topo.user).await;
    }
}

/// A cursor: the execute returns only the column definitions with
/// `CURSOR_EXISTS`, `COM_STMT_FETCH` the rows in batches on the same
/// backend connection, the last batch has `LAST_ROW_SENT`.
#[tokio::test]
async fn cursors_and_fetch() {
    let Some(eps) = endpoints("cursors_and_fetch") else {
        return;
    };
    for (addr, persona, topo) in matrix(&eps) {
        let ctx = format!("{persona:?} {}", topo.user);
        let db = Db::create(&addr, topo.user, "gw_cursor").await;
        let mut c = raw(&addr, topo.user, Some(&db.name), |_| {}).await;
        let st = c
            .prepare(b"SELECT seq, CONCAT('row ', seq) FROM seq_1_to_10 WHERE seq >= ?")
            .await
            .unwrap();
        let id = st.ok.stmt_id;
        for round in 0..2 {
            c.send_command(&execute_cmd(
                id,
                CURSOR_TYPE_READ_ONLY,
                true,
                vec![int_param(1)],
            ))
            .await
            .unwrap();
            let rs = c.collect().await.unwrap();
            assert_eq!(rs.len(), 1, "{ctx}");
            assert_eq!(rs[0].columns.len(), 2, "{ctx}");
            assert!(rs[0].rows.is_empty(), "{ctx}: rows came without a fetch");
            assert!(
                rs[0].status.contains(StatusFlags::CURSOR_EXISTS),
                "{ctx}: {:?}",
                rs[0].status
            );
            let cols = rs[0].columns.clone();
            let mut seen = Vec::new();
            // Other statements in between run on the pinned connection.
            let x = c.query_first("SELECT 7").await.unwrap().unwrap();
            assert_eq!(x[0].as_deref(), Some(&b"7"[..]));
            loop {
                c.send_command(&Command::StmtFetch(id, 3)).await.unwrap();
                let mut last = false;
                while let Some(p) = c.next_packet().await.unwrap() {
                    match p.kind {
                        PacketKind::Row => seen.push(decode_binary_row(&p.payload, &cols).unwrap()),
                        PacketKind::End(t) => {
                            last = t.status().contains(StatusFlags::LAST_ROW_SENT);
                        }
                        PacketKind::Err(e) => panic!("{ctx}: fetch failed: {e:?}"),
                        other => panic!("{ctx}: {other:?}"),
                    }
                }
                if last {
                    break;
                }
                assert!(seen.len() <= 10, "{ctx}: too many rows");
            }
            let ids: Vec<Value> = seen.iter().map(|r| r[0].clone()).collect();
            assert_eq!(
                ids,
                (1..=10).map(Value::UInt).collect::<Vec<_>>(),
                "{ctx} round {round}"
            );
            // After the last row, a fetch fails (no open cursor).
            c.send_command(&Command::StmtFetch(id, 1)).await.unwrap();
            let e = c.collect().await.unwrap_err();
            assert!(e.server_error().is_some(), "{ctx}");
        }
        // Reset with an open cursor closes it.
        c.send_command(&execute_cmd(
            id,
            CURSOR_TYPE_READ_ONLY,
            true,
            vec![int_param(5)],
        ))
        .await
        .unwrap();
        c.collect().await.unwrap();
        c.send_command(&Command::StmtReset(id)).await.unwrap();
        c.collect().await.unwrap();
        // Without a cursor flag, rows come at once.
        let (_, rows) = exec_rows(&mut c, &execute_cmd(id, 0, true, vec![int_param(9)]))
            .await
            .unwrap();
        assert_eq!(rows.len(), 2, "{ctx}");
        c.quit().await;
        db.drop(&addr, topo.user).await;
    }
}

/// `LOAD DATA LOCAL INFILE` streams the client's file; a client without
/// `LOCAL_FILES` gets 3948 and nothing is loaded.
#[tokio::test]
async fn local_infile_allowed_and_refused() {
    let Some(eps) = endpoints("local_infile_allowed_and_refused") else {
        return;
    };
    for (addr, persona, topo) in matrix(&eps) {
        let ctx = format!("{persona:?} {}", topo.user);
        let db = Db::create(&addr, topo.user, "gw_infile").await;
        let mut c = raw(&addr, topo.user, Some(&db.name), |cfg| {
            cfg.local_files = true;
        })
        .await;
        c.query_drop("CREATE TABLE li (id INT PRIMARY KEY, v VARCHAR(40))")
            .await
            .unwrap();
        c.send_query(b"LOAD DATA LOCAL INFILE 'rows.csv' INTO TABLE li FIELDS TERMINATED BY ','")
            .await
            .unwrap();
        let p = c.next_packet().await.unwrap().unwrap();
        assert_eq!(p.kind, PacketKind::LocalInfile, "{ctx}");
        assert_eq!(&p.payload[1..], b"rows.csv");
        // Several packets, more than one relay chunk.
        let mut body = String::new();
        for i in 1..=5000 {
            let _ = writeln!(body, "{i},value {i}");
        }
        for chunk in body.as_bytes().chunks(30_000) {
            c.send_infile_data(chunk).await.unwrap();
        }
        c.finish_infile().await.unwrap();
        let rs = c.collect().await.unwrap();
        assert_eq!(
            rs[0].ok.as_ref().map(|o| o.affected_rows),
            Some(5000),
            "{ctx}"
        );
        let n = c
            .query_first("SELECT COUNT(*), MAX(v) FROM li")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n[0].as_deref(), Some(&b"5000"[..]), "{ctx}");
        c.quit().await;

        let mut off = raw(&addr, topo.user, Some(&db.name), |cfg| {
            cfg.local_files = false;
        })
        .await;
        let e = off
            .query("LOAD DATA LOCAL INFILE 'rows.csv' INTO TABLE li")
            .await
            .unwrap_err();
        let want = if persona == Persona::MariaDb {
            4166
        } else {
            3948
        };
        assert_eq!(e.server_error().map(|e| e.code), Some(want), "{ctx}");
        let n = off
            .query_first("SELECT COUNT(*) FROM li")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n[0].as_deref(), Some(&b"5000"[..]));
        off.quit().await;
        db.drop(&addr, topo.user).await;
    }
}

/// `KILL QUERY` from another session stops a running statement on the
/// backend: the client gets 1317 at once, the session and its open
/// transaction go on.
#[tokio::test]
async fn kill_query_mid_statement() {
    let Some(eps) = endpoints("kill_query_mid_statement") else {
        return;
    };
    for (addr, persona, topo) in matrix(&eps) {
        let ctx = format!("{persona:?} {}", topo.user);
        let db = Db::create(&addr, topo.user, "gw_kill").await;
        let mut a = gw(&addr, topo.user, Some(&db.name)).await;
        let mut b = gw(&addr, topo.user, None).await;
        a.query_drop("CREATE TABLE k (id INT PRIMARY KEY) ENGINE = InnoDB")
            .await
            .unwrap();
        a.query_drop("BEGIN").await.unwrap();
        a.query_drop("INSERT INTO k VALUES (1)").await.unwrap();
        let a_id = a.id();
        let started = Instant::now();
        let run = tokio::spawn(async move {
            let r: Result<Vec<Row>, _> = a.query("SELECT SLEEP(30)").await;
            (a, r)
        });
        tokio::time::sleep(Duration::from_millis(700)).await;
        b.query_drop(format!("KILL QUERY {a_id}")).await.unwrap();
        let (mut a, r) = run.await.unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{ctx}: the kill took {:?}",
            started.elapsed()
        );
        assert_eq!(server_code(&r.unwrap_err()), 1317, "{ctx}");
        // The backend statement stopped too.
        if let Some(mut side) = servers(topo.servers).await {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let mut sleeping = 0;
            for s in &mut side {
                let n: Option<u64> = s
                    .query_first("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO LIKE 'SELECT SLEEP(30)%'")
                    .await
                    .unwrap();
                sleeping += n.unwrap_or(0);
            }
            assert_eq!(sleeping, 0, "{ctx}: SLEEP still runs on the backend");
        }
        // The transaction survived the killed statement.
        a.query_drop("INSERT INTO k VALUES (2)").await.unwrap();
        a.query_drop("COMMIT").await.unwrap();
        let n: Option<u64> = b
            .query_first(format!("SELECT COUNT(*) FROM `{}`.k", db.name))
            .await
            .unwrap();
        assert_eq!(n, Some(2), "{ctx}");
        a.disconnect().await.unwrap();
        b.disconnect().await.unwrap();
        db.drop(&addr, topo.user).await;
    }
}

/// Read-your-writes through the gateway: a read right after a write sees
/// it, on every topology; on primary and replicas also while both
/// replicas stop applying (the read then waits or goes to the primary).
#[tokio::test]
async fn read_your_writes_through_the_gateway() {
    let Some(eps) = endpoints("read_your_writes_through_the_gateway") else {
        return;
    };
    for (addr, persona, topo) in matrix(&eps) {
        let ctx = format!("{persona:?} {}", topo.user);
        let db = Db::create(&addr, topo.user, "gw_ryw").await;
        let mut c = gw(&addr, topo.user, Some(&db.name)).await;
        c.query_drop("CREATE TABLE r (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        for i in 0..50 {
            c.query_drop(format!("INSERT INTO r VALUES ({i}, {i})"))
                .await
                .unwrap();
            let n: Option<u64> = c
                .query_first(format!("SELECT COUNT(*) FROM r WHERE id = {i}"))
                .await
                .unwrap();
            assert_eq!(
                n,
                Some(1),
                "{ctx}: row {i} not visible right after its insert"
            );
            let v: Option<i64> = c
                .exec_first("SELECT v FROM r WHERE id = ?", (i,))
                .await
                .unwrap();
            assert_eq!(v, Some(i64::from(i)), "{ctx}: prepared read of row {i}");
        }
        // Another session of the same account sees it too (the
        // tenant-gateway causal scope).
        let mut other = gw(&addr, topo.user, Some(&db.name)).await;
        c.query_drop("INSERT INTO r VALUES (1000, 1)")
            .await
            .unwrap();
        let n: Option<u64> = other
            .query_first("SELECT COUNT(*) FROM r WHERE id = 1000")
            .await
            .unwrap();
        assert_eq!(n, Some(1), "{ctx}: other session");
        other.disconnect().await.unwrap();

        if topo.user == "app_repl"
            && let Some(mut side) = servers(topo.servers).await
        {
            // Replicas stop applying: reads must still see the writes.
            for s in side.iter_mut().skip(1) {
                s.query_drop("STOP SLAVE SQL_THREAD").await.unwrap();
            }
            let lagged = async {
                for i in 2000..2010 {
                    c.query_drop(format!("INSERT INTO r VALUES ({i}, 0)"))
                        .await
                        .unwrap();
                    let n: Option<u64> = c
                        .query_first(format!("SELECT COUNT(*) FROM r WHERE id = {i}"))
                        .await
                        .unwrap();
                    assert_eq!(n, Some(1), "{ctx}: row {i} with lagging replicas");
                }
            };
            let r = tokio::time::timeout(Duration::from_secs(60), lagged).await;
            for s in side.iter_mut().skip(1) {
                s.query_drop("START SLAVE SQL_THREAD").await.unwrap();
            }
            r.expect("reads with lagging replicas hung");
        }
        c.disconnect().await.unwrap();
        db.drop(&addr, topo.user).await;
    }
}

/// `COM_FIELD_LIST`, `COM_INIT_DB` and `COM_RESET_CONNECTION`.
#[tokio::test]
async fn field_list_init_db_and_reset() {
    let Some(eps) = endpoints("field_list_init_db_and_reset") else {
        return;
    };
    for (addr, persona, topo) in matrix(&eps) {
        let ctx = format!("{persona:?} {}", topo.user);
        let db = Db::create(&addr, topo.user, "gw_misc").await;
        let mut c = raw(&addr, topo.user, None, |_| {}).await;
        c.init_db(&db.name).await.unwrap();
        c.query_drop("CREATE TABLE f (id INT PRIMARY KEY, name VARCHAR(20), note TEXT)")
            .await
            .unwrap();
        c.send_command(&Command::FieldList(FieldList {
            table: Bytes::from_static(b"f"),
            wildcard: Bytes::new(),
        }))
        .await
        .unwrap();
        let mut names = Vec::new();
        let mut ended = false;
        while let Some(p) = c.next_packet().await.unwrap() {
            match p.kind {
                PacketKind::ColumnDef => names.push(
                    String::from_utf8(
                        ColumnDef::decode(&p.payload, c.caps())
                            .unwrap()
                            .name
                            .to_vec(),
                    )
                    .unwrap(),
                ),
                PacketKind::End(_) => ended = true,
                other => panic!("{ctx}: {other:?}"),
            }
        }
        assert!(ended);
        assert_eq!(names, ["id", "name", "note"], "{ctx}");
        let e = c.init_db("no_such_schema_x").await.unwrap_err();
        assert_eq!(e.server_error().map(|e| e.code), Some(1049), "{ctx}");
        // Reset: user variables and statements are gone, the schema stays.
        // The baseline differs by topology (Galera sets it to the cluster size).
        let aii = c
            .query_first("SELECT @@SESSION.auto_increment_increment")
            .await
            .unwrap()
            .unwrap();
        c.query_drop("SET @v = 5, SESSION auto_increment_increment = 7")
            .await
            .unwrap();
        // A temporary table pins the backend connection: the reset must
        // reach that connection too.
        c.query_drop("CREATE TEMPORARY TABLE tmp_r (id INT)")
            .await
            .unwrap();
        let before = c
            .query_first("SELECT CONNECTION_ID()")
            .await
            .unwrap()
            .unwrap();
        let st = c.prepare(b"SELECT 1").await.unwrap();
        c.reset_connection().await.unwrap();
        let v = c
            .query_first(
                "SELECT @v, DATABASE(), @@SESSION.auto_increment_increment, CONNECTION_ID()",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v[0], None, "{ctx}: user variable survived the reset");
        assert_eq!(v[1].as_deref(), Some(db.name.as_bytes()));
        assert_eq!(v[2], aii[0], "{ctx}: session variable survived the reset");
        // CONNECTION_ID() is the gateway's connection id, which a reset keeps.
        assert_eq!(v[3], before[0], "{ctx}: connection id changed");
        let e = c.query("SELECT COUNT(*) FROM tmp_r").await.unwrap_err();
        assert_eq!(
            e.server_error().map(|e| e.code),
            Some(1146),
            "{ctx}: temporary table survived"
        );
        let e = exec_rows(&mut c, &execute_cmd(st.ok.stmt_id, 0, true, vec![]))
            .await
            .unwrap_err();
        assert_eq!(e.server_error().map(|e| e.code), Some(1243), "{ctx}");
        c.quit().await;
        db.drop(&addr, topo.user).await;
    }
}

/// MariaDB's `COM_STMT_BULK_EXECUTE` on 3307.
#[tokio::test]
async fn bulk_execute_on_mariadb() {
    let Some(eps) = endpoints("bulk_execute_on_mariadb") else {
        return;
    };
    for (addr, persona, topo) in matrix(&eps) {
        if persona != Persona::MariaDb {
            continue;
        }
        let db = Db::create(&addr, topo.user, "gw_bulk").await;
        let mut c = raw(&addr, topo.user, Some(&db.name), |cfg| {
            cfg.extra_caps = Caps::MARIADB_CLIENT_STMT_BULK_OPERATIONS;
        })
        .await;
        assert!(c.caps().contains(Caps::MARIADB_CLIENT_STMT_BULK_OPERATIONS));
        c.query_drop("CREATE TABLE bu (id INT PRIMARY KEY, v VARCHAR(10))")
            .await
            .unwrap();
        let st = c.prepare(b"INSERT INTO bu VALUES (?, ?)").await.unwrap();
        let rows = (1..=3)
            .map(|i| {
                vec![
                    BulkValue::Value(Value::Int(i)),
                    if i == 2 {
                        BulkValue::Null
                    } else {
                        BulkValue::Value(Value::Bytes(Bytes::from(format!("v{i}"))))
                    },
                ]
            })
            .collect();
        c.send_command(&Command::StmtBulkExecute(BulkExecute {
            stmt_id: st.ok.stmt_id,
            flags: BULK_SEND_TYPES_TO_SERVER,
            raw: Bytes::new(),
            rows: Some(BulkRows {
                types: vec![
                    u16::from(column_type::LONGLONG),
                    u16::from(column_type::VAR_STRING),
                ],
                rows,
            }),
        }))
        .await
        .unwrap();
        let rs = c.collect().await.unwrap();
        assert_eq!(
            rs[0].ok.as_ref().map(|o| o.affected_rows),
            Some(3),
            "{}",
            topo.user
        );
        let n = c
            .query_first("SELECT COUNT(*), COUNT(v) FROM bu")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n[0].as_deref(), Some(&b"3"[..]));
        assert_eq!(n[1].as_deref(), Some(&b"2"[..]));
        c.quit().await;
        db.drop(&addr, topo.user).await;
    }
}

/// Decision D9: a `REPEATABLE READ` update of a row another transaction
/// changed after the snapshot succeeds on 3306 (MySQL has no snapshot
/// isolation check) and fails with 1020 on 3307 (MariaDB's default); a 3306
/// session can ask for MariaDB's behaviour, and keeps it across pooled
/// connections. Not on Galera, whose certification answers such conflicts
/// itself.
#[tokio::test]
async fn snapshot_isolation_per_persona() {
    let Some(eps) = endpoints("snapshot_isolation_per_persona") else {
        return;
    };
    for (addr, persona, topo) in matrix(&eps) {
        if topo.user == "app_galera" {
            continue;
        }
        let ctx = format!("{persona:?} {}", topo.user);
        let db = Db::create(&addr, topo.user, "gw_snapshot").await;
        let mut a = gw(&addr, topo.user, Some(&db.name)).await;
        let mut b = gw(&addr, topo.user, Some(&db.name)).await;
        a.query_drop("CREATE TABLE s (id INT PRIMARY KEY, v INT) ENGINE = InnoDB")
            .await
            .unwrap();
        for (round, strict) in [(0, false), (1, true)] {
            if strict {
                if persona != Persona::MySql84 {
                    break;
                }
                a.query_drop("SET SESSION innodb_snapshot_isolation = ON")
                    .await
                    .unwrap();
                // Statements on other pooled connections in between.
                for _ in 0..3 {
                    let _: Option<i64> = a.query_first("SELECT 1").await.unwrap();
                }
            }
            a.query_drop(format!("REPLACE INTO s VALUES ({round}, 0)"))
                .await
                .unwrap();
            a.query_drop("START TRANSACTION").await.unwrap();
            let _: Option<i64> = a
                .query_first(format!("SELECT v FROM s WHERE id = {round}"))
                .await
                .unwrap();
            b.query_drop(format!("UPDATE s SET v = v + 1 WHERE id = {round}"))
                .await
                .unwrap();
            let r = a
                .query_drop(format!("UPDATE s SET v = v + 10 WHERE id = {round}"))
                .await;
            let want_1020 = strict || persona == Persona::MariaDb;
            match r {
                Ok(()) => {
                    assert!(!want_1020, "{ctx} round {round}: expected 1020");
                    a.query_drop("COMMIT").await.unwrap();
                    let v: Option<i64> = b
                        .query_first(format!("SELECT v FROM s WHERE id = {round}"))
                        .await
                        .unwrap();
                    assert_eq!(v, Some(11), "{ctx}: both updates applied");
                }
                Err(e) => {
                    assert!(want_1020, "{ctx} round {round}: {e}");
                    assert_eq!(server_code(&e), 1020, "{ctx} round {round}");
                    a.query_drop("ROLLBACK").await.unwrap();
                }
            }
        }
        a.disconnect().await.unwrap();
        b.disconnect().await.unwrap();
        db.drop(&addr, topo.user).await;
    }
}

/// Every result set of one text (`COM_QUERY` with several statements), or
/// the error number that stopped it.
async fn text_results(c: &mut Conn, sql: &str) -> Result<Vec<Vec<Row>>, u16> {
    let mut r = c.query_iter(sql).await.map_err(|e| server_code(&e))?;
    let mut out = Vec::new();
    while !r.is_empty() {
        let rows: Vec<Row> = r.collect().await.map_err(|e| server_code(&e))?;
        out.push(rows);
    }
    Ok(out)
}

/// The first column of each result set that has rows, as integers.
fn scalars(sets: &[Vec<Row>]) -> Vec<i64> {
    sets.iter()
        .filter(|s| !s.is_empty())
        .map(|s| s[0].get::<i64, _>(0).unwrap())
        .collect()
}

/// Transactions inside one multi-statement text keep MySQL's semantics
/// whatever state earlier statements left in the session, on a session
/// without a default schema and with qualified names. Before the fix a
/// `CALL db.p()` made session tracking report an empty schema, which the
/// gateway replayed as `USE` on the next backend connection: every later
/// statement failed with 1046.
#[tokio::test]
async fn multi_statement_transactions_after_session_state() {
    let Some(eps) = endpoints("multi_statement_transactions_after_session_state") else {
        return;
    };
    for (addr, persona, topo) in matrix(&eps) {
        let db = Db::create(&addr, topo.user, "gw_mtx").await;
        let t = format!("`{}`.t", db.name);
        let mut setup = gw(&addr, topo.user, None).await;
        setup
            .query_drop(format!("CREATE TABLE {t} (id INT PRIMARY KEY)"))
            .await
            .unwrap();
        setup
            .query_drop(format!(
                "CREATE PROCEDURE `{0}`.p(IN x INT) BEGIN START TRANSACTION; INSERT INTO {t} VALUES (x); END",
                db.name
            ))
            .await
            .unwrap();
        setup
            .query_drop(format!("CREATE PROCEDURE `{}`.q() SELECT 1", db.name))
            .await
            .unwrap();
        setup.disconnect().await.unwrap();

        let states: [(&str, Vec<String>); 4] = [
            ("fresh", vec![]),
            ("after CALL", vec![format!("CALL `{}`.q()", db.name)]),
            (
                "after autocommit off and on",
                vec!["SET autocommit = 0; SET autocommit = 1".to_owned()],
            ),
            (
                "after an error and ROLLBACK",
                vec![
                    format!("BEGIN; INSERT INTO {t} VALUES (1); INSERT INTO {t} VALUES (1)"),
                    "ROLLBACK".to_owned(),
                ],
            ),
        ];
        for (k, (state, before)) in states.iter().enumerate() {
            let ctx = format!("{persona:?} {} {state}", topo.user);
            let mut c = gw(&addr, topo.user, None).await;
            for sql in before {
                let _ = text_results(&mut c, sql).await;
            }
            let id = |n: usize| 1000 * (k + 1) + n;
            let cases = [
                (
                    "BEGIN in the text",
                    format!(
                        "BEGIN; INSERT INTO {t} VALUES ({0}); SELECT COUNT(*) FROM {t} WHERE id = {0}; \
                         ROLLBACK; SELECT COUNT(*) FROM {t} WHERE id = {0}",
                        id(1)
                    ),
                    vec![1, 0],
                ),
                (
                    "transaction begun inside CALL",
                    format!(
                        "CALL `{1}`.p({0}); SELECT COUNT(*) FROM {t} WHERE id = {0}; \
                         ROLLBACK; SELECT COUNT(*) FROM {t} WHERE id = {0}",
                        id(2),
                        db.name
                    ),
                    vec![1, 0],
                ),
                (
                    "START TRANSACTION in an executable comment",
                    format!(
                        "/*!40101 START TRANSACTION */; INSERT INTO {t} VALUES ({0}); \
                         SELECT COUNT(*) FROM {t} WHERE id = {0}; ROLLBACK; \
                         SELECT COUNT(*) FROM {t} WHERE id = {0}",
                        id(3)
                    ),
                    vec![1, 0],
                ),
                (
                    "DDL commits implicitly",
                    format!(
                        "BEGIN; INSERT INTO {t} VALUES ({0}); CREATE TABLE `{1}`.t2_{2} (a INT); \
                         ROLLBACK; SELECT COUNT(*) FROM {t} WHERE id = {0}",
                        id(4),
                        db.name,
                        k
                    ),
                    vec![1],
                ),
            ];
            for (name, sql, want) in cases {
                let got = text_results(&mut c, &sql)
                    .await
                    .unwrap_or_else(|e| panic!("{ctx}, {name}: error {e}"));
                assert_eq!(scalars(&got), want, "{ctx}, {name}");
            }
            c.disconnect().await.unwrap();
        }
        db.drop(&addr, topo.user).await;
    }
}
