//! Row-level security, live (chapter 29, "RLS suite"; build step 26): a
//! differential and adversarial suite through both personas.
//!
//! The administrator (`acme.app`) creates a database, three users and a
//! role, tables `docs` (protected), `members` (read by a policy's
//! subquery) and `notes` (unprotected), and these policies on `docs`:
//!
//! ```text
//! own    PERMISSIVE  ALL     PUBLIC   USING (owner = CURRENT_USER())
//! team   PERMISSIVE  SELECT  reader   USING (team IN (SELECT team FROM members WHERE uid = CURRENT_USER()))
//! small  RESTRICTIVE ALL     PUBLIC   USING (amount < 1000) WITH CHECK (amount < 1000)
//! ```
//!
//! A reference evaluator (the same rules over the seed rows, in Rust)
//! gives each user's visible set; every statement shape must return
//! exactly it, count it, aggregate it, join it, and never reveal or change
//! a hidden row through results, errors, warnings, affected-row counts or
//! `EXPLAIN`.
//!
//! ```sh
//! source target/agent-rls/env.sh && deploy/compose/gateway-dev.sh up
//! export SPRIG_TEST_RLS=127.0.0.1:27306,127.0.0.1:27307   # both personas
//! cargo test -p sprig-gateway --test rls_live -- --nocapture --test-threads 1
//! ```
//!
//! `SPRIG_TEST_RLS_DB` names the database (default `rls_suite`); the test
//! drops it and the users it created when it ends.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::collections::BTreeSet;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row, SslOpts, Value};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_RLS";
const PW: &str = "Rls-suite-pw-1";

#[derive(Debug, Clone)]
struct Doc {
    id: i64,
    owner: &'static str,
    team: i64,
    secret: &'static str,
    amount: i64,
}

fn seed() -> Vec<Doc> {
    let mut v = Vec::new();
    let owners = ["rls_alice@%", "rls_bob@%", "rls_carol@%"];
    for i in 1..=30i64 {
        v.push(Doc {
            id: i,
            owner: owners[usize::try_from(i % 3).unwrap()],
            team: i % 4,
            secret: match i {
                5 => "topsecret",
                7 => "sevensecret",
                _ => "plain",
            },
            amount: if i % 5 == 0 { 5000 } else { i * 10 },
        });
    }
    v
}

/// Members: alice is in team 1, bob in team 2.
const MEMBERS: &[(&str, i64)] = &[("rls_alice@%", 1), ("rls_bob@%", 2)];

/// The reference evaluator: rows `user` sees with `reader` active.
fn visible(user: &str, reader: bool) -> BTreeSet<i64> {
    let me = format!("{user}@%");
    seed()
        .iter()
        .filter(|d| {
            let own = d.owner == me;
            let team = reader && MEMBERS.iter().any(|(u, t)| *u == me && *t == d.team);
            (own || team) && d.amount < 1000
        })
        .map(|d| d.id)
        .collect()
}

/// Rows `user` may update or delete (the `own` policy and `small`).
fn writable(user: &str) -> BTreeSet<i64> {
    let me = format!("{user}@%");
    seed()
        .iter()
        .filter(|d| d.owner == me && d.amount < 1000)
        .map(|d| d.id)
        .collect()
}

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

async fn conn(addr: &str, user: &str, pw: &str, db: Option<&str>) -> Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(pw))
        .db_name(db)
        .prefer_socket(false)
        .stmt_cache_size(0)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("login {user} at {addr}: {e}"))
}

async fn ids(c: &mut Conn, sql: &str) -> Result<BTreeSet<i64>, String> {
    let rows: Vec<Row> = c.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .into_iter()
        .map(|r| r.get::<Option<i64>, usize>(0).flatten().unwrap_or(-1))
        .collect())
}

async fn one(c: &mut Conn, sql: &str) -> Result<Option<String>, String> {
    let r: Option<Row> = c.query_first(sql).await.map_err(|e| e.to_string())?;
    Ok(r.and_then(|r| match r.as_ref(0)? {
        Value::NULL => None,
        Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::Int(i) => Some(i.to_string()),
        Value::UInt(i) => Some(i.to_string()),
        Value::Double(d) => Some(d.to_string()),
        Value::Float(d) => Some(d.to_string()),
        other => Some(format!("{other:?}")),
    }))
}

async fn exec(c: &mut Conn, sql: &str) -> Result<u64, String> {
    c.query_drop(sql).await.map_err(|e| e.to_string())?;
    Ok(c.affected_rows())
}

fn set_text(s: &BTreeSet<i64>) -> String {
    format!("{s:?}")
}

async fn setup(admin: &mut Conn, db: &str) {
    let users = ["rls_alice", "rls_bob", "rls_carol", "rls_dave"];
    for u in users {
        let _ = admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }
    let _ = admin.query_drop("DROP ROLE IF EXISTS 'rls_reader'").await;
    let _ = admin
        .query_drop(format!("DROP DATABASE IF EXISTS {db}"))
        .await;
    for s in [
        format!("CREATE DATABASE {db}"),
        format!(
            "CREATE TABLE {db}.docs (id INT PRIMARY KEY, owner VARCHAR(64) NOT NULL, team INT, \
             secret VARCHAR(32), amount INT, KEY k_secret (secret))"
        ),
        format!("CREATE TABLE {db}.members (uid VARCHAR(64), team INT)"),
        format!("CREATE TABLE {db}.notes (id INT PRIMARY KEY, doc_id INT, body VARCHAR(32))"),
        format!("CREATE TABLE {db}.sink (id INT PRIMARY KEY, secret VARCHAR(32))"),
    ] {
        admin
            .query_drop(&s)
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    let values: Vec<String> = seed()
        .iter()
        .map(|d| {
            format!(
                "({}, '{}', {}, '{}', {})",
                d.id, d.owner, d.team, d.secret, d.amount
            )
        })
        .collect();
    admin
        .query_drop(format!(
            "INSERT INTO {db}.docs VALUES {}",
            values.join(", ")
        ))
        .await
        .unwrap();
    for (u, t) in MEMBERS {
        admin
            .query_drop(format!("INSERT INTO {db}.members VALUES ('{u}', {t})"))
            .await
            .unwrap();
    }
    for i in 1..=30 {
        admin
            .query_drop(format!("INSERT INTO {db}.notes VALUES ({i}, {i}, 'n{i}')"))
            .await
            .unwrap();
    }
    for u in users {
        admin
            .query_drop(format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'"))
            .await
            .unwrap();
        admin
            .query_drop(format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON {db}.* TO '{u}'@'%'"
            ))
            .await
            .unwrap();
    }
    admin.query_drop("CREATE ROLE 'rls_reader'").await.unwrap();
    admin
        .query_drop("GRANT 'rls_reader' TO 'rls_alice'@'%', 'rls_bob'@'%'")
        .await
        .unwrap();
    for s in [
        format!(
            "CREATE POLICY own ON {db}.docs AS PERMISSIVE FOR ALL TO PUBLIC USING (owner = CURRENT_USER())"
        ),
        format!(
            "CREATE POLICY team ON {db}.docs FOR SELECT TO rls_reader USING (team IN (SELECT team FROM members WHERE uid = CURRENT_USER()))"
        ),
        format!(
            "CREATE POLICY small ON {db}.docs AS RESTRICTIVE FOR ALL USING (amount < 1000) WITH CHECK (amount < 1000)"
        ),
        format!("ALTER TABLE {db}.docs ENABLE ROW LEVEL SECURITY"),
    ] {
        admin
            .query_drop(&s)
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
}

async fn teardown(admin: &mut Conn, db: &str) {
    for u in ["rls_alice", "rls_bob", "rls_carol", "rls_dave"] {
        let _ = admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }
    let _ = admin.query_drop("DROP ROLE IF EXISTS 'rls_reader'").await;
    let _ = admin
        .query_drop(format!("DROP DATABASE IF EXISTS {db}"))
        .await;
}

fn tenant() -> String {
    std::env::var("SPRIG_TEST_RLS_TENANT").unwrap_or_else(|_| "acme".into())
}

fn tenant_user(u: &str) -> String {
    format!("{}.{u}", tenant())
}

/// Reads as `user` (role active or not): every shape returns exactly the
/// reference set.
async fn reads(ck: &mut Checks, c: &mut Conn, user: &str, reader: bool, tag: &str) {
    let want = visible(user, reader);
    let all: BTreeSet<i64> = seed().iter().map(|d| d.id).collect();
    let hidden: BTreeSet<i64> = all.difference(&want).copied().collect();
    let n = want.len();
    let sum: i64 = seed()
        .iter()
        .filter(|d| want.contains(&d.id))
        .map(|d| d.amount)
        .sum();
    let shapes: Vec<(&str, String)> = vec![
        ("plain", "SELECT id FROM docs".into()),
        ("where-or", "SELECT id FROM docs WHERE id > 0 OR 1=1".into()),
        ("qualified", "SELECT d.id FROM docs AS d ORDER BY d.id".into()),
        ("limit", "SELECT id FROM docs ORDER BY id LIMIT 100".into()),
        ("subquery-in", "SELECT id FROM notes WHERE doc_id IN (SELECT id FROM docs)".into()),
        ("exists", "SELECT n.id FROM notes n WHERE EXISTS (SELECT 1 FROM docs d WHERE d.id = n.doc_id)".into()),
        ("derived", "SELECT x.id FROM (SELECT * FROM docs) x".into()),
        ("cte", "WITH c AS (SELECT id FROM docs) SELECT id FROM c".into()),
        ("cte-shadow", "WITH docs AS (SELECT * FROM docs) SELECT id FROM docs".into()),
        ("union", "SELECT id FROM docs UNION SELECT id FROM docs".into()),
        ("join", "SELECT d.id FROM docs d JOIN notes n ON n.doc_id = d.id".into()),
        ("left-join-preserved", "SELECT d.id FROM docs d LEFT JOIN notes n ON n.doc_id = d.id".into()),
        ("right-join", "SELECT d.id FROM notes n RIGHT JOIN docs d ON n.doc_id = d.id".into()),
        ("natural", "SELECT id FROM docs NATURAL JOIN (SELECT id FROM notes) n".into()),
        ("using", "SELECT id FROM docs JOIN notes USING (id)".into()),
        ("window", "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) r FROM docs) w".into()),
        ("recursive", "WITH RECURSIVE r AS (SELECT MIN(id) AS id FROM docs UNION ALL SELECT d.id FROM docs d JOIN r ON d.id = (SELECT MIN(id) FROM docs WHERE id > r.id)) SELECT id FROM r".into()),
        ("locking", "SELECT id FROM docs FOR UPDATE".into()),
        ("share", "SELECT id FROM docs LOCK IN SHARE MODE".into()),
        ("comment", "SELECT /* docs */ id FROM docs -- docs\n".into()),
        ("db-qualified", "SELECT id FROM rls_suite.docs".into()),
    ];
    for (name, sql) in &shapes {
        let got = ids(c, sql).await;
        ck.ok(
            &format!("{tag} {user} read {name}"),
            got.as_ref() == Ok(&want),
            || format!("{sql}: got {got:?}, want {}", set_text(&want)),
        );
    }
    // Counting and aggregates.
    let cnt = one(c, "SELECT COUNT(*) FROM docs").await;
    ck.ok(
        &format!("{tag} {user} count"),
        cnt == Ok(Some(n.to_string())),
        || format!("{cnt:?} vs {n}"),
    );
    let s = one(c, "SELECT SUM(amount) FROM docs").await;
    let want_sum = if n == 0 { None } else { Some(sum.to_string()) };
    ck.ok(
        &format!("{tag} {user} sum"),
        s == Ok(want_sum.clone()),
        || format!("{s:?} vs {want_sum:?}"),
    );
    let g = one(c, "SELECT COUNT(DISTINCT team) FROM docs GROUP BY 1=1").await;
    ck.ok(&format!("{tag} {user} group"), g.is_ok(), || {
        format!("{g:?}")
    });
    // Null-extended rows: notes of hidden docs show NULL.
    let rows: Vec<(i64, Option<i64>)> = c
        .query("SELECT n.id, d.id FROM notes n LEFT JOIN docs d ON d.id = n.doc_id ORDER BY n.id")
        .await
        .unwrap_or_default();
    let ok = rows.len() == 30
        && rows.iter().all(|(nid, did)| match did {
            Some(x) => want.contains(x) && x == nid,
            None => !want.contains(nid),
        });
    ck.ok(&format!("{tag} {user} left-join nullable side"), ok, || {
        format!("{rows:?}")
    });
    // Inference by predicate: hidden values never match.
    for h in hidden.iter().take(5) {
        let got = ids(c, &format!("SELECT id FROM docs WHERE id = {h}")).await;
        ck.ok(
            &format!("{tag} {user} point {h}"),
            got == Ok(BTreeSet::new()),
            || format!("{got:?}"),
        );
    }
    if !want.iter().any(|i| *i == 5) {
        let got = one(c, "SELECT COUNT(*) FROM docs WHERE secret = 'topsecret'").await;
        ck.ok(
            &format!("{tag} {user} secret count"),
            got == Ok(Some("0".into())),
            || format!("{got:?}"),
        );
    }
    // Error oracle: an expression that fails only on the hidden row.
    let got = ids(
        c,
        "SELECT id FROM docs WHERE IF(secret = 'topsecret', \
         (SELECT n.id FROM notes n WHERE n.id IN (1, 2) AND n.doc_id <> docs.id - 100), 1) = 1",
    )
    .await;
    if !want.contains(&5) {
        ck.ok(
            &format!("{tag} {user} error oracle"),
            got.as_ref() == Ok(&want),
            || format!("{got:?}"),
        );
    }
    // Warning oracle: hidden secrets must not appear in warnings.
    let _ = c.query_drop("SELECT id FROM docs WHERE secret = 0").await;
    let w: Vec<(String, u32, String)> = c.query("SHOW WARNINGS").await.unwrap_or_default();
    let leaked = w
        .iter()
        .any(|(_, _, m)| m.contains("topsecret") || m.contains("sevensecret"));
    if !want.contains(&5) && !want.contains(&7) {
        ck.ok(&format!("{tag} {user} warning oracle"), !leaked, || {
            format!("{w:?}")
        });
    }
    // Metadata that counts rows is hidden for protected tables.
    let m: Result<Vec<(Option<u64>, Option<u64>)>, _> = c
        .query(
            "SELECT TABLE_ROWS, DATA_LENGTH FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'docs'",
        )
        .await;
    ck.ok(
        &format!("{tag} {user} table rows hidden"),
        m.as_ref()
            .is_ok_and(|v| v.len() == 1 && v[0] == (None, None)),
        || format!("{m:?}"),
    );
    let m: Result<Vec<Option<u64>>, _> = c
        .query(
            "SELECT CARDINALITY FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'docs'",
        )
        .await;
    ck.ok(
        &format!("{tag} {user} cardinality hidden"),
        m.as_ref()
            .is_ok_and(|v| !v.is_empty() && v.iter().all(Option::is_none)),
        || format!("{m:?}"),
    );
    let e = c.query_drop("SHOW TABLE STATUS").await;
    ck.ok(
        &format!("{tag} {user} table status refused"),
        e.as_ref().is_err_and(|e| e.to_string().contains("39366")),
        || format!("{e:?}"),
    );
    // EXPLAIN would show estimates that count hidden rows.
    let e = c.query_drop("EXPLAIN SELECT * FROM docs").await;
    ck.ok(
        &format!("{tag} {user} explain refused"),
        e.as_ref().is_err_and(|e| e.to_string().contains("39366")),
        || format!("{e:?}"),
    );
    // Prepared statements (binary protocol).
    let rows: Result<Vec<i64>, _> = c.exec("SELECT id FROM docs WHERE id > ?", (0,)).await;
    let got: Option<BTreeSet<i64>> = rows.ok().map(|v| v.into_iter().collect());
    ck.ok(
        &format!("{tag} {user} prepared"),
        got.as_ref() == Some(&want),
        || format!("{got:?}"),
    );
    // The context variable is the gateway's.
    let e = c.query_drop("SET @sprig_rls = '{\"b\":1}'").await;
    ck.ok(
        &format!("{tag} {user} context reserved"),
        e.as_ref().is_err_and(|e| e.to_string().contains("39050")),
        || format!("{e:?}"),
    );
}

/// Writes as `user`: only writable rows change; checks refuse bad rows.
async fn writes(ck: &mut Checks, c: &mut Conn, admin: &mut Conn, user: &str, tag: &str) {
    let w = writable(user);
    let me = format!("{user}@%");
    // UPDATE every row: affected = writable rows only.
    let n = exec(c, "UPDATE docs SET team = team").await;
    ck.ok(&format!("{tag} {user} update matched"), n.is_ok(), || {
        format!("{n:?}")
    });
    let n = exec(c, "UPDATE docs SET secret = CONCAT(secret, '')  WHERE 1=1").await;
    ck.ok(&format!("{tag} {user} update all"), n.is_ok(), || {
        format!("{n:?}")
    });
    // A hidden row by key: nothing changes.
    let hidden: Vec<i64> = seed()
        .iter()
        .map(|d| d.id)
        .filter(|i| !w.contains(i))
        .collect();
    for h in hidden.iter().take(3) {
        let n = exec(c, &format!("UPDATE docs SET amount = 1 WHERE id = {h}")).await;
        ck.ok(
            &format!("{tag} {user} update hidden {h}"),
            n == Ok(0),
            || format!("{n:?}"),
        );
        let n = exec(c, &format!("DELETE FROM docs WHERE id = {h}")).await;
        ck.ok(
            &format!("{tag} {user} delete hidden {h}"),
            n == Ok(0),
            || format!("{n:?}"),
        );
    }
    let all_amounts = one(admin, "SELECT SUM(amount) FROM rls_suite.docs").await;
    let want_amounts: i64 = seed().iter().map(|d| d.amount).sum();
    ck.ok(
        &format!("{tag} {user} hidden unchanged"),
        all_amounts == Ok(Some(want_amounts.to_string())),
        || format!("{all_amounts:?}"),
    );
    // WITH CHECK: insert someone else's row, or a large amount.
    let other = if user == "rls_bob" {
        "rls_carol@%"
    } else {
        "rls_bob@%"
    };
    let e = exec(
        c,
        &format!("INSERT INTO docs VALUES (900, '{other}', 0, 'x', 1)"),
    )
    .await;
    ck.ok(
        &format!("{tag} {user} insert other owner"),
        e.as_ref().is_err_and(|e| e.contains("39363")),
        || format!("{e:?}"),
    );
    let id = 901 + i64::from(user.len() as u8);
    let e = exec(
        c,
        &format!("INSERT INTO docs VALUES ({id}, '{me}', 0, 'x', 5000)"),
    )
    .await;
    ck.ok(
        &format!("{tag} {user} insert large"),
        e.as_ref().is_err_and(|e| e.contains("39363")),
        || format!("{e:?}"),
    );
    let e = exec(
        c,
        &format!("INSERT INTO docs VALUES ({id}, '{me}', 0, 'x', 7)"),
    )
    .await;
    ck.ok(&format!("{tag} {user} insert own"), e == Ok(1), || {
        format!("{e:?}")
    });
    // UPDATE moving a row out of the policy fails the check.
    let e = exec(
        c,
        &format!("UPDATE docs SET owner = 'rls_dave@%' WHERE id = {id}"),
    )
    .await;
    ck.ok(
        &format!("{tag} {user} update out"),
        e.as_ref().is_err_and(|e| e.contains("39363")),
        || format!("{e:?}"),
    );
    // Upsert onto a hidden row.
    if let Some(h) = hidden.first() {
        let e = exec(c, &format!("INSERT INTO docs VALUES ({h}, '{me}', 0, 'x', 1) ON DUPLICATE KEY UPDATE amount = 2")).await;
        ck.ok(
            &format!("{tag} {user} upsert hidden"),
            e.as_ref().is_err_and(|e| e.contains("39363")),
            || format!("{e:?}"),
        );
        let e = exec(
            c,
            &format!("REPLACE INTO docs VALUES ({h}, '{me}', 0, 'x', 1)"),
        )
        .await;
        ck.ok(
            &format!("{tag} {user} replace hidden"),
            e.as_ref().is_err_and(|e| e.contains("39363")),
            || format!("{e:?}"),
        );
    }
    // INSERT ... SELECT copies only visible rows.
    let _ = exec(admin, "DELETE FROM rls_suite.sink").await;
    let e = exec(c, "INSERT INTO sink SELECT id, secret FROM docs").await;
    let copied = one(admin, "SELECT COUNT(*) FROM rls_suite.sink").await;
    let want_copied = visible(user, true).len() + 1;
    ck.ok(
        &format!("{tag} {user} insert-select"),
        e.is_ok() && copied == Ok(Some(want_copied.to_string())),
        || format!("{e:?} {copied:?} want {want_copied}"),
    );
    // Multi-table writes.
    let e = exec(
        c,
        "UPDATE docs d JOIN notes n ON n.doc_id = d.id SET d.team = d.team",
    )
    .await;
    // A sharded tenant's router refuses multi-shard multi-table writes
    // (39420), which is not a row-level security matter.
    let sharded = std::env::var("SPRIG_TEST_RLS_SHARDED").is_ok();
    let refused =
        |e: &Result<u64, String>| sharded && e.as_ref().is_err_and(|e| e.contains("39420"));
    ck.ok(
        &format!("{tag} {user} multi-table update"),
        e.is_ok() || refused(&e),
        || format!("{e:?}"),
    );
    let e = exec(
        c,
        "DELETE d FROM docs d JOIN notes n ON n.doc_id = d.id WHERE n.id = 999",
    )
    .await;
    ck.ok(
        &format!("{tag} {user} multi-table delete"),
        e == Ok(0) || refused(&e),
        || format!("{e:?}"),
    );
    // Delete own new row.
    let e = exec(c, &format!("DELETE FROM docs WHERE id = {id}")).await;
    ck.ok(&format!("{tag} {user} delete own"), e == Ok(1), || {
        format!("{e:?}")
    });
}

/// Straight to MariaDB as a bound user's backend account (decision RL18):
/// the account can't read the protected table, and its policy view shows
/// only the rows the session's context allows. Needs
/// `SPRIG_TEST_RLS_DIRECT=host:port` (a cluster holding the tenant),
/// `SPRIG_TEST_RLS_TENANT_ID`, and the dev root password and backend
/// secret (`SPRIG_TEST_RLS_ROOT_PW`, `SPRIG_TEST_RLS_SECRET`).
async fn direct_probe(ck: &mut Checks, db: &str) {
    let (Ok(addr), Ok(tid)) = (
        std::env::var("SPRIG_TEST_RLS_DIRECT"),
        std::env::var("SPRIG_TEST_RLS_TENANT_ID"),
    ) else {
        eprintln!("direct MariaDB probe skipped (SPRIG_TEST_RLS_DIRECT, SPRIG_TEST_RLS_TENANT_ID)");
        return;
    };
    let _ = db;
    let root_pw =
        std::env::var("SPRIG_TEST_RLS_ROOT_PW").unwrap_or_else(|_| "sprig-dev-root".into());
    let secret = std::env::var("SPRIG_TEST_RLS_SECRET")
        .unwrap_or_else(|_| "sprig-dev-backend-accounts-secret".into());
    let mut root = conn(&addr, "root", &root_pw, None).await;
    let uid: u64 = root
        .query_first(format!(
            "SELECT MAX(user_id) FROM t{tid}_sprig.users WHERE name = 'rls_alice'"
        ))
        .await
        .unwrap()
        .expect("rls_alice's id");
    let pids: Vec<u64> = root
        .query(format!(
            "SELECT p.policy_id FROM t{tid}_sprig.row_policies p WHERE p.name IN ('own', 'small')"
        ))
        .await
        .unwrap();
    let schemas: Vec<String> = root
        .query(format!(
            "SELECT DISTINCT v.TABLE_SCHEMA FROM information_schema.TABLES v JOIN \
             information_schema.TABLES t ON t.TABLE_SCHEMA = v.TABLE_SCHEMA AND t.TABLE_NAME = 'docs' \
             WHERE v.TABLE_SCHEMA LIKE 't{tid}\\_d%' AND v.TABLE_NAME = '__sprig_rls_docs'"
        ))
        .await
        .unwrap();
    // A split table has invisible columns and no view (RL18): nothing to
    // probe there.
    if schemas.is_empty() && std::env::var("SPRIG_TEST_RLS_SHARDED").is_ok() {
        eprintln!("direct probe: no policy view next to docs (a split table)");
        return;
    }
    ck.ok("policy views exist", !schemas.is_empty(), || "none".into());
    let account = format!("t{tid}_u{uid}");
    let pw = sprig_auth::authz::backend::BackendKey::new(secret.as_bytes())
        .unwrap()
        .password(&account)
        .to_string();
    let mut b = conn(&addr, &account, &pw, None).await;
    let ctx = serde_json::json!({
        "b": 0, "x": [], "p": pids,
        "s": {"user": "rls_alice@%", "uid": uid, "tid": tid.parse::<u64>().unwrap()}
    })
    .to_string();
    let mut seen = BTreeSet::new();
    for s in &schemas {
        let e = ids(&mut b, &format!("SELECT id FROM `{s}`.docs")).await;
        ck.ok(
            &format!("direct: base table refused in {s}"),
            e.as_ref().is_err_and(|e| e.contains("1142")),
            || format!("{e:?}"),
        );
        let none = ids(&mut b, &format!("SELECT id FROM `{s}`.__sprig_rls_docs")).await;
        ck.ok(
            &format!("direct: no context, no rows in {s}"),
            none == Ok(BTreeSet::new()),
            || format!("{none:?}"),
        );
        b.query_drop(format!("SET @sprig_rls = '{ctx}'"))
            .await
            .unwrap();
        let got = ids(&mut b, &format!("SELECT id FROM `{s}`.__sprig_rls_docs")).await;
        ck.ok(&format!("direct: view read in {s}"), got.is_ok(), || {
            format!("{got:?}")
        });
        seen.extend(got.unwrap_or_default());
        b.query_drop("SET @sprig_rls = NULL").await.unwrap();
    }
    let want = visible("rls_alice", false);
    ck.ok("direct: the view shows alice's rows", seen == want, || {
        format!("{seen:?} vs {want:?}")
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_level_security_suite() {
    let Some(addrs) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, "row_level_security_suite");
        return;
    };
    let addrs = skip::urls(&addrs);
    let db = std::env::var("SPRIG_TEST_RLS_DB").unwrap_or_else(|_| "rls_suite".into());
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    // SPRIG_TEST_RLS_PHASE: `setup` creates everything and stops (a
    // script then shards the tables), `run` uses what is there, `all` does
    // both.
    let phase = std::env::var("SPRIG_TEST_RLS_PHASE").unwrap_or_else(|_| "all".into());
    let app = tenant_user("app");
    let mut admin = conn(&addrs[0], &app, "sprig-dev-app", None).await;
    if phase != "run" {
        setup(&mut admin, &db).await;
    }
    if phase == "setup" {
        return;
    }
    let mut admin_db = conn(&addrs[0], &app, "sprig-dev-app", Some(&db)).await;
    // A run starts from the seed, whatever an earlier run left.
    for s in [
        "ALTER TABLE docs ENABLE ROW LEVEL SECURITY".to_owned(),
        "ALTER TABLE docs NO FORCE ROW LEVEL SECURITY".to_owned(),
        "DROP VIEW IF EXISTS v_docs".to_owned(),
        "DELETE FROM docs".to_owned(),
        "DELETE FROM sink".to_owned(),
    ] {
        admin_db
            .query_drop(&s)
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
    let _ = admin_db
        .query_drop("REVOKE BYPASS RLS ON *.* FROM 'rls_carol'@'%'")
        .await;
    for d in seed() {
        admin_db
            .query_drop(format!(
                "INSERT INTO docs VALUES ({}, '{}', {}, '{}', {})",
                d.id, d.owner, d.team, d.secret, d.amount
            ))
            .await
            .unwrap();
    }
    // Policy statements.
    let sp: Vec<Row> = admin_db.query("SHOW POLICIES").await.unwrap();
    ck.ok("show policies", sp.len() == 3, || {
        format!("{} rows", sp.len())
    });
    let sc = one(&mut admin_db, "SHOW CREATE POLICY own ON docs").await;
    ck.ok("show create policy", sc.is_ok(), || format!("{sc:?}"));
    for (what, sql, code) in [
        ("duplicate", "CREATE POLICY own ON docs USING (1=1)", "1304"),
        (
            "non-deterministic",
            "CREATE POLICY p ON docs USING (id < RAND())",
            "39364",
        ),
        (
            "variable",
            "CREATE POLICY p ON docs USING (id = @x)",
            "39364",
        ),
        (
            "internal",
            "CREATE POLICY p ON docs USING (id IN (SELECT 1 FROM __sprig_x))",
            "39364",
        ),
        (
            "missing table",
            "CREATE POLICY p ON nope USING (1=1)",
            "1146",
        ),
        (
            "cycle",
            "CREATE POLICY p ON members USING (team IN (SELECT team FROM docs))",
            "39365",
        ),
    ] {
        let e = admin_db.query_drop(sql).await;
        ck.ok(
            &format!("policy refused: {what}"),
            e.as_ref().is_err_and(|e| e.to_string().contains(code)),
            || format!("{e:?}"),
        );
    }
    // Admin is a manager (ALTER) of an unforced table: sees everything.
    let got = ids(&mut admin_db, "SELECT id FROM docs").await;
    ck.ok(
        "manager sees all",
        got.as_ref().map(BTreeSet::len) == Ok(30),
        || format!("{got:?}"),
    );
    for addr in &addrs {
        let tag = format!("[{addr}]");
        for user in ["rls_alice", "rls_bob", "rls_carol"] {
            let mut c = conn(addr, &tenant_user(user), PW, Some(&db)).await;
            // Roles are active at login only by default: none here.
            reads(&mut ck, &mut c, user, false, &tag).await;
            if user != "rls_carol" {
                // A statement prepared before SET ROLE carries the old
                // policies: the gateway prepares it again (RL15, round 2)
                // and the execute sees the new role's rows.
                let st = c.prep("SELECT id FROM docs WHERE id > ?").await.unwrap();
                c.query_drop("SET ROLE 'rls_reader'").await.unwrap();
                let r: Result<Vec<i64>, _> = c.exec(&st, (0,)).await;
                let got: Option<BTreeSet<i64>> =
                    r.as_ref().ok().map(|v| v.iter().copied().collect());
                ck.ok(
                    &format!("{tag} {user} stale prepare prepared again"),
                    got.as_ref() == Some(&visible(user, true)),
                    || format!("{r:?}"),
                );
                let _ = c.close(st).await;
                reads(&mut ck, &mut c, user, true, &format!("{tag}+reader")).await;
            }
            writes(&mut ck, &mut c, &mut admin_db, user, &tag).await;
            // Policy DDL needs ALTER.
            let e = c.query_drop("DROP POLICY own ON docs").await;
            ck.ok(
                &format!("{tag} {user} drop policy refused"),
                e.as_ref().is_err_and(|e| e.to_string().contains("1142")),
                || format!("{e:?}"),
            );
            drop(c);
        }
        // A user without any policy for it: sees nothing, writes nothing.
        let mut d = conn(addr, &tenant_user("rls_dave"), PW, Some(&db)).await;
        let got = ids(&mut d, "SELECT id FROM docs").await;
        ck.ok(
            &format!("{tag} dave nothing"),
            got == Ok(BTreeSet::new()),
            || format!("{got:?}"),
        );
        drop(d);
    }
    // Search (chapter 39): the copy is seeded while the creator is bound
    // (FORCE), so only the internal exemption (RL17) lets it hold every
    // row; each user's search then sees exactly its visible rows.
    admin_db
        .query_drop("ALTER TABLE docs FORCE ROW LEVEL SECURITY")
        .await
        .unwrap();
    let ft = admin_db
        .query_drop("CREATE FULLTEXT INDEX ft_secret ON docs (secret)")
        .await;
    admin_db
        .query_drop("ALTER TABLE docs NO FORCE ROW LEVEL SECURITY")
        .await
        .unwrap();
    if ft.is_ok() {
        for (user, want) in [
            ("rls_bob", BTreeSet::from([7i64])),
            ("rls_alice", BTreeSet::new()),
        ] {
            let mut c = conn(&addrs[0], &tenant_user(user), PW, Some(&db)).await;
            // The copy is filled in the background (39824 until then).
            let mut got = Err(String::new());
            for _ in 0..60 {
                got = ids(
                    &mut c,
                    "SELECT id FROM docs WHERE MATCH (secret) AGAINST ('sevensecret')",
                )
                .await;
                if !got.as_ref().is_err_and(|e| e.contains("39824")) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            ck.ok(&format!("search {user}"), got == Ok(want.clone()), || {
                format!("{got:?}")
            });
            let n = one(
                &mut c,
                "SELECT COUNT(*) FROM docs WHERE MATCH (secret) AGAINST ('plain')",
            )
            .await;
            let plain = visible(user, false)
                .iter()
                .filter(|i| ![5, 7].contains(*i))
                .count();
            ck.ok(
                &format!("search count {user}"),
                n == Ok(Some(plain.to_string())),
                || format!("{n:?} vs {plain}"),
            );
            drop(c);
        }
        let _ = admin_db.query_drop("DROP INDEX ft_secret ON docs").await;
    } else {
        ck.ok(
            "search index created",
            std::env::var("SPRIG_TEST_RLS_SHARDED").is_ok(),
            || format!("{ft:?}"),
        );
    }
    // FORCE binds the manager too; BYPASS RLS frees a user.
    admin_db
        .query_drop("ALTER TABLE docs FORCE ROW LEVEL SECURITY")
        .await
        .unwrap();
    let got = ids(&mut admin_db, "SELECT id FROM docs").await;
    ck.ok("forced manager bound", got == Ok(BTreeSet::new()), || {
        format!("{got:?}")
    });
    admin_db
        .query_drop("ALTER TABLE docs NO FORCE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin_db
        .query_drop("GRANT BYPASS RLS ON *.* TO 'rls_carol'@'%'")
        .await
        .unwrap();
    let mut c = conn(&addrs[0], &tenant_user("rls_carol"), PW, Some(&db)).await;
    let got = ids(&mut c, "SELECT id FROM docs").await;
    ck.ok(
        "bypass sees all",
        got.as_ref().map(BTreeSet::len) == Ok(30),
        || format!("{got:?}"),
    );
    drop(c);
    // Views and stored code over the protected table (RL20): under policy
    // views they read the views, so a bound user sees its own rows;
    // dynamic SQL stays refused.
    let sharded = std::env::var("SPRIG_TEST_RLS_SHARDED").is_ok();
    let view = admin_db
        .query_drop("CREATE VIEW v_docs AS SELECT * FROM docs")
        .await;
    for s in [
        "DROP PROCEDURE IF EXISTS p_docs",
        "DROP PROCEDURE IF EXISTS p_dyn",
        "DROP FUNCTION IF EXISTS f_cnt",
        "CREATE PROCEDURE p_docs() SELECT id FROM docs ORDER BY id",
        "CREATE PROCEDURE p_dyn() BEGIN PREPARE s FROM 'SELECT id FROM docs'; EXECUTE s; END",
        "CREATE FUNCTION f_cnt() RETURNS INT READS SQL DATA RETURN (SELECT COUNT(*) FROM docs)",
    ] {
        let r = admin_db.query_drop(s).await;
        ck.ok(&format!("stored code: {s}"), r.is_ok() || sharded, || {
            format!("{r:?}")
        });
    }
    admin_db
        .query_drop(format!("GRANT EXECUTE ON {db}.* TO 'rls_alice'@'%'"))
        .await
        .unwrap();
    let mut a = conn(&addrs[0], &tenant_user("rls_alice"), PW, Some(&db)).await;
    let want = visible("rls_alice", false);
    if view.is_ok() {
        let got = ids(&mut a, "SELECT id FROM v_docs").await;
        ck.ok("view filtered", got == Ok(want.clone()), || {
            format!("{got:?}")
        });
        let got = ids(
            &mut a,
            "SELECT v.id FROM v_docs v JOIN notes n ON n.doc_id = v.id",
        )
        .await;
        ck.ok("view joined", got == Ok(want.clone()), || {
            format!("{got:?}")
        });
        let sc = one(&mut admin_db, "SHOW CREATE VIEW v_docs").await;
        let shown = sc.as_ref().ok().cloned().flatten().unwrap_or_default();
        ck.ok("view text as written", !shown.contains("__sprig"), || {
            shown.clone()
        });
    } else {
        // Views over sharded tables aren't supported (chapter 17).
        eprintln!("view not created: {view:?}");
    }
    if !sharded {
        let got = ids(&mut a, "CALL p_docs()").await;
        ck.ok("procedure filtered", got == Ok(want.clone()), || {
            format!("{got:?}")
        });
        let got = one(&mut a, "SELECT f_cnt()").await;
        ck.ok(
            "function filtered",
            got == Ok(Some(want.len().to_string())),
            || format!("{got:?}"),
        );
        let e = a.query_drop("CALL p_dyn()").await;
        ck.ok(
            "dynamic SQL refused",
            e.as_ref().is_err_and(|e| e.to_string().contains("39366")),
            || format!("{e:?}"),
        );
        // The manager (not bound) still sees every row through them.
        let got = ids(&mut admin_db, "CALL p_docs()").await;
        ck.ok(
            "procedure, manager",
            got.as_ref().map(BTreeSet::len) == Ok(30),
            || format!("{got:?}"),
        );
    }
    direct_probe(&mut ck, &db).await;
    // Disabling security opens the table again.
    admin_db
        .query_drop("ALTER TABLE docs DISABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    let got = ids(&mut a, "SELECT id FROM docs").await;
    ck.ok(
        "disabled",
        got.as_ref().map(BTreeSet::len) == Ok(30),
        || format!("{got:?}"),
    );
    drop(a);
    teardown(&mut admin, &db).await;
    eprintln!(
        "row-level security suite: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
