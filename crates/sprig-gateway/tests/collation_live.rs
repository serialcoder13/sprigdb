//! The login collation and the character-set statements through the
//! gateway match MariaDB's own behaviour (chapter 06, "New connections",
//! "Login collation"): a client that sends `utf8mb4_general_ci` (45) at
//! login gets what MariaDB 11.8 gives it directly, `character_set_collations`
//! mapping included, so `SELECT 'a' = ?` works and `'ß' = 'ss'` compares
//! the same way. Each case also runs again after the session's backend
//! connection is killed, so its state is replayed onto another one.
//!
//! ```sh
//! deploy/compose/gateway-dev.sh up   # SPRIG_GATEWAY_LOGIN_MODE=accounts_file
//! export SPRIG_TEST_GATEWAY=127.0.0.1:13306,127.0.0.1:13307   # mysql, mariadb persona
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! cargo test -p sprig-gateway --test collation_live -- --nocapture
//! ```
//!
//! The login is `deploy/compose/gateway-accounts.dev.toml`'s `app`
//! (`dc-a-c1`, the single node).

#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::time::Duration;

use bytes::Bytes;
use sprig_proto_mysql::client::{ClientConfig, ClientError, MysqlClient, TlsMode, TlsOptions};
use sprig_proto_mysql::command::{BoundParam, Command, ExecuteParams, ParamValue, StmtExecute};
use sprig_proto_mysql::resultset::{column_type, decode_binary_row};
use sprig_proto_mysql::{Persona, Value};
use sprig_sql::compat::collation::mysql_name;
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_GATEWAY";
const USER: &str = "app";
const PASSWORD: &str = "sprig-dev-app";

/// One case: the handshake collation id, then the statements the client
/// runs before the probes.
struct Case {
    login: u8,
    setup: &'static [&'static str],
}

const CASES: &[Case] = &[
    // utf8mb4_general_ci: utf8mb4's compiled default, mapped by
    // character_set_collations.
    Case {
        login: 45,
        setup: &[],
    },
    // utf8mb4_unicode_ci and utf8mb4_bin: not the default, kept.
    Case {
        login: 224,
        setup: &[],
    },
    Case {
        login: 46,
        setup: &[],
    },
    // utf8mb3_general_ci: mapped too.
    Case {
        login: 33,
        setup: &[],
    },
    // latin1_swedish_ci: latin1 has no mapping.
    Case {
        login: 8,
        setup: &[],
    },
    // utf8mb4_0900_ai_ci (MySQL 8 clients): kept, NO PAD.
    Case {
        login: 255,
        setup: &[],
    },
    Case {
        login: 63,
        setup: &[],
    },
    Case {
        login: 45,
        setup: &["SET NAMES utf8mb4"],
    },
    Case {
        login: 45,
        setup: &["SET NAMES utf8mb4 COLLATE utf8mb4_general_ci"],
    },
    Case {
        login: 224,
        setup: &["SET NAMES utf8mb4"],
    },
    Case {
        login: 45,
        setup: &["SET collation_connection = utf8mb4_unicode_ci"],
    },
    Case {
        login: 45,
        setup: &[
            "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci",
            "SET character_set_connection = utf8mb4",
        ],
    },
    Case {
        login: 45,
        setup: &["SET NAMES latin1"],
    },
];

fn split(url: &str) -> (String, String, String) {
    let rest = url.strip_prefix("mysql://").unwrap();
    let (cred, addr) = rest.rsplit_once('@').unwrap();
    let (user, pass) = cred.split_once(':').unwrap();
    (
        addr.trim_end_matches('/').to_owned(),
        user.to_owned(),
        pass.to_owned(),
    )
}

async fn connect(addr: &str, user: &str, pass: &str, collation: u8) -> MysqlClient {
    let mut cfg = ClientConfig::new(user, pass);
    cfg.tls = TlsMode::Required;
    cfg.tls_config = Some(
        TlsOptions {
            skip_verify: true,
            ..TlsOptions::default()
        }
        .build()
        .unwrap(),
    );
    cfg.collation = collation;
    MysqlClient::connect(addr, &cfg)
        .await
        .unwrap_or_else(|e| panic!("connect {user}@{addr}: {e}"))
}

fn err(e: &ClientError) -> String {
    match e.server_error() {
        Some(p) => format!("ERR {}", p.code),
        None => panic!("{e}"),
    }
}

fn show(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_owned(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        other => format!("{other:?}"),
    }
}

fn text_param(s: &str) -> BoundParam {
    BoundParam {
        param_type: u16::from(column_type::VAR_STRING),
        name: Bytes::new(),
        value: ParamValue::Value(Value::Bytes(Bytes::copy_from_slice(s.as_bytes()))),
    }
}

/// What the session looks like: the character-set variables, a text
/// comparison that differs between `general_ci` and UCA 14.0, and a
/// prepared statement with string parameters.
async fn probe(c: &mut MysqlClient) -> Vec<String> {
    let mut out = Vec::new();
    match c
        .query_first(
            "SELECT @@character_set_client, @@character_set_connection, \
             @@character_set_results, @@collation_connection, 'ß' = 'ss', 'a ' = 'a', \
             COLLATION('a')",
        )
        .await
    {
        Ok(Some(row)) => out.extend(row.iter().map(|v| {
            v.as_ref().map_or("NULL".to_owned(), |b| {
                String::from_utf8_lossy(b).into_owned()
            })
        })),
        Ok(None) => out.push("no row".to_owned()),
        Err(e) => out.push(err(&e)),
    }
    match c
        .prepare(b"SELECT 'a' = ?, COLLATION(?), ? = 'ss', ? = 'a'")
        .await
    {
        Ok(st) => {
            let id = st.ok.stmt_id;
            let cmd = Command::StmtExecute(StmtExecute {
                stmt_id: id,
                flags: 0,
                iterations: 1,
                raw_params: Bytes::new(),
                params: Some(ExecuteParams {
                    new_params_bound: true,
                    params: vec![
                        text_param("a"),
                        text_param("a"),
                        text_param("ß"),
                        text_param("a "),
                    ],
                }),
            });
            c.send_command(&cmd).await.unwrap();
            match c.collect().await {
                Ok(rs) => {
                    let first = rs.into_iter().find(|r| !r.columns.is_empty()).unwrap();
                    let row = decode_binary_row(&first.rows[0], &first.columns).unwrap();
                    out.extend(row.iter().map(|v| format!("ps:{}", show(v))));
                }
                Err(e) => out.push(format!("ps:{}", err(&e))),
            }
            c.close_statement(id).await.unwrap();
        }
        Err(e) => out.push(format!("ps:prepare {}", err(&e))),
    }
    out
}

/// The 3306 persona may report MySQL collation names (chapter 28):
/// compare both sides in those terms there. The `'a ' = 'a'` probes still
/// tell PAD SPACE from NO PAD.
fn as_seen(persona: Persona, v: Vec<String>) -> Vec<String> {
    if persona == Persona::MariaDb {
        return v;
    }
    v.into_iter()
        .map(|s| match s.strip_prefix("ps:") {
            Some(p) => format!("ps:{}", mysql_name(p)),
            None => mysql_name(&s).to_owned(),
        })
        .collect()
}

async fn run_case(c: &mut MysqlClient, case: &Case) -> Vec<String> {
    for s in case.setup {
        c.query_drop(s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    probe(c).await
}

async fn backend_thread(c: &mut MysqlClient) -> u64 {
    let row = c
        .query_first("SELECT @@pseudo_thread_id")
        .await
        .unwrap()
        .unwrap();
    String::from_utf8_lossy(row[0].as_ref().unwrap())
        .parse()
        .unwrap()
}

#[tokio::test]
async fn login_collation_and_set_names_match_mariadb() {
    let test = "login_collation_and_set_names_match_mariadb";
    let Some(gw) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return;
    };
    let Some(direct) = skip::var(skip::MARIADB_SINGLE) else {
        skip::report_missing(skip::MARIADB_SINGLE, test);
        return;
    };
    let (daddr, duser, dpass) = split(&skip::urls(&direct)[0]);
    let mut side = connect(&daddr, &duser, &dpass, 45).await;
    let eps: Vec<(String, Persona)> = skip::urls(&gw)
        .into_iter()
        .zip([Persona::MySql84, Persona::MariaDb])
        .collect();
    let mut failures = Vec::new();
    for case in CASES {
        let mut d = connect(&daddr, &duser, &dpass, case.login).await;
        let want = run_case(&mut d, case).await;
        d.quit().await;
        for (addr, persona) in &eps {
            let ctx = format!("{persona:?} login {} {:?}", case.login, case.setup);
            let mut c = connect(addr, USER, PASSWORD, case.login).await;
            let got = as_seen(*persona, run_case(&mut c, case).await);
            let want_seen = as_seen(*persona, want.clone());
            println!("{ctx}\n  direct  {want_seen:?}\n  gateway {got:?}");
            if got != want_seen {
                failures.push(format!("{ctx}: gateway {got:?}, MariaDB {want_seen:?}"));
            }
            // Replayed onto another backend connection.
            let id = backend_thread(&mut c).await;
            side.query_drop(&format!("KILL CONNECTION {id}"))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            // The gateway never hands out the killed connection: the probe
            // runs on another one (chapter 06, "When a lost connection is
            // retried").
            let again = as_seen(*persona, probe(&mut c).await);
            assert_ne!(backend_thread(&mut c).await, id, "{ctx}: same connection");
            if again != want_seen {
                failures.push(format!(
                    "{ctx} after replay: gateway {again:?}, MariaDB {want_seen:?}"
                ));
            }
            c.quit().await;
        }
    }
    side.quit().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
