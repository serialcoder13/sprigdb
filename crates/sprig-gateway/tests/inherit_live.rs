//! `FOR EACH TENANT` through a live private deployment (chapter 42,
//! "Inherited messaging objects"; chapters 45 and 41; R9, r9fet).
//!
//! ```sh
//! export SPRIG_TEST_INHERIT_GATEWAY=127.0.0.1:28306     # MySQL wire
//! export SPRIG_TEST_INHERIT_SYSTEM=r9fetsys             # the declaring tenant
//! export SPRIG_TEST_INHERIT_MEMBERS=r9fetca,r9fetcb,r9fetcc   # its children
//! export SPRIG_TEST_INHERIT_LATE=r9fetcd                # created by the test
//! export SPRIG_TEST_INHERIT_CREATE='<command creating a child tenant $1 with user app>'
//! export SPRIG_TEST_INHERIT_WORKER_PIDFILE=<state>/worker.pid
//! export SPRIG_TEST_INHERIT_WORKER_START='<command that starts a worker>'
//! cargo test -p sprig-gateway --test inherit_live -- --nocapture --test-threads 1
//! ```
//!
//! Every tenant has the user `app` (password `sprig-dev-app`). Kiran's
//! case: the system tenant declares `security.user_events FOR EACH TENANT`
//! and two triggers on it, `sync_users` (JavaScript) and `sync_users_appb`
//! (Lua), with `GRANTS`; it publishes "user added" for each client with
//! `FOR TENANT`, and each client's tables get every event exactly once,
//! written by its own `sprig_inherited`. A client created after the
//! definitions gets its instance and triggers on its first event, with no
//! messaging object added. The worker running the triggers is killed with
//! `kill -9` mid-stream. Disabling `sync_users_appb` in one client stops it
//! there only; a new body in the system tenant reaches every client; a
//! member's own publish is stamped with its tenant; the refusals of the
//! ancestor rule hold.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names,
    missing_docs
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_INHERIT_GATEWAY";
const PASSWORD: &str = "sprig-dev-app";

struct Env {
    addr: String,
    system: String,
    members: Vec<String>,
    late: Option<String>,
    create: Option<String>,
    pidfile: Option<String>,
    start: Option<String>,
}

fn env(test: &str) -> Option<Env> {
    let Some(addr) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let get = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    Some(Env {
        addr,
        system: get("SPRIG_TEST_INHERIT_SYSTEM").unwrap_or_else(|| "r9fetsys".into()),
        members: get("SPRIG_TEST_INHERIT_MEMBERS")
            .unwrap_or_else(|| "r9fetca,r9fetcb,r9fetcc".into())
            .split(',')
            .map(str::to_owned)
            .collect(),
        late: get("SPRIG_TEST_INHERIT_LATE"),
        create: get("SPRIG_TEST_INHERIT_CREATE"),
        pidfile: get("SPRIG_TEST_INHERIT_WORKER_PIDFILE"),
        start: get("SPRIG_TEST_INHERIT_WORKER_START"),
    })
}

async fn conn(addr: &str, tenant: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let user = format!("{tenant}.app");
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(PASSWORD))
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    Conn::new(opts).await.unwrap()
}

/// A statement's rows as text, or its error number and message.
async fn sql(c: &mut Conn, q: &str) -> Result<Vec<Vec<Option<String>>>, (u16, String)> {
    match c.query::<mysql_async::Row, _>(q).await {
        Ok(rows) => Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(&b).into()),
                        other => Some(other.as_sql(true)),
                    })
                    .collect()
            })
            .collect()),
        Err(mysql_async::Error::Server(e)) => Err((e.code, e.message)),
        Err(e) => panic!("{q}: {e}"),
    }
}

async fn ok(c: &mut Conn, q: &str) -> Vec<Vec<Option<String>>> {
    sql(c, q).await.unwrap_or_else(|e| panic!("{q}: {e:?}"))
}

async fn count(c: &mut Conn, q: &str) -> u64 {
    ok(c, q).await[0][0]
        .clone()
        .unwrap_or_default()
        .parse()
        .unwrap_or(0)
}

/// Waits until `q` (a count) reaches `n`, up to `secs`.
async fn wait_count(c: &mut Conn, q: &str, n: u64, secs: u64) -> u64 {
    let end = Instant::now() + Duration::from_secs(secs);
    loop {
        let got = count(c, q).await;
        if got >= n || Instant::now() > end {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn kill_worker(e: &Env) -> bool {
    let (Some(pidfile), Some(_)) = (&e.pidfile, &e.start) else {
        return false;
    };
    let pid = std::fs::read_to_string(pidfile).unwrap();
    let st = std::process::Command::new("kill")
        .args(["-9", pid.trim()])
        .status()
        .unwrap();
    assert!(st.success(), "kill -9 {pid}");
    true
}

fn start_worker(e: &Env) {
    let st = std::process::Command::new("sh")
        .args(["-c", e.start.as_deref().unwrap()])
        .status()
        .unwrap();
    assert!(st.success());
}

const SYNC_USERS: &str = r"
CREATE TRIGGER security.sync_users ON STREAM security.user_events
  FOR EACH TENANT GRANTS (INSERT, UPDATE, SELECT ON app_a.users)
  LANGUAGE javascript AS $$
export default function (ctx) {
  const e = JSON.parse(ctx.record.value);
  db.exec('INSERT INTO app_a.users (id, email, n, origin) VALUES (?, ?, 1, ?) ' +
          'ON DUPLICATE KEY UPDATE n = n + 1',
          [e.user_id, e.email, ctx.record.headers['x-sprig-origin-tenant'] || '']);
}
$$";

const SYNC_USERS_V2: &str = r"
CREATE OR REPLACE TRIGGER security.sync_users ON STREAM security.user_events
  FOR EACH TENANT GRANTS (INSERT, UPDATE, SELECT ON app_a.users)
  LANGUAGE javascript AS $$
export default function (ctx) {
  const e = JSON.parse(ctx.record.value);
  db.exec('INSERT INTO app_a.users (id, email, n, origin) VALUES (?, ?, 100, ?) ' +
          'ON DUPLICATE KEY UPDATE n = n + 100',
          [e.user_id, e.email, ctx.record.headers['x-sprig-origin-tenant'] || '']);
}
$$";

const SYNC_USERS_APPB: &str = r"
CREATE TRIGGER security.sync_users_appb ON STREAM security.user_events
  FOR EACH TENANT GRANTS (INSERT ON app_b.members)
  LANGUAGE lua AS $$
local e = cjson.decode(ctx.record.value)
db.exec('INSERT INTO app_b.members (p, o, user_id) VALUES (?, ?, ?)',
        ctx.record.partition, ctx.record.offset, e.user_id)
$$";

/// The member's application tables (its own data, not messaging objects).
async fn app_tables(c: &mut Conn) {
    for q in [
        "DROP DATABASE IF EXISTS app_a",
        "DROP DATABASE IF EXISTS app_b",
        "DROP DATABASE IF EXISTS security",
        "CREATE DATABASE app_a",
        "CREATE DATABASE app_b",
        "CREATE TABLE app_a.users (id BIGINT PRIMARY KEY, email VARCHAR(200), n INT NOT NULL, origin VARCHAR(64))",
        "CREATE TABLE app_b.members (p INT NOT NULL, o BIGINT NOT NULL, user_id BIGINT NOT NULL, PRIMARY KEY (p, o))",
    ] {
        ok(c, q).await;
    }
}

async fn publish_for(c: &mut Conn, tenant: &str, from: u64, to: u64) {
    let rows: Vec<String> = (from..to)
        .map(|i| {
            format!(
                "({i}, JSON_OBJECT('type', 'user.added', 'user_id', {i}, 'email', 'u{i}@{tenant}.example'))"
            )
        })
        .collect();
    for chunk in rows.chunks(50) {
        ok(
            c,
            &format!(
                "INSERT INTO security.user_events FOR TENANT {tenant} (key, value) VALUES {}",
                chunk.join(", ")
            ),
        )
        .await;
    }
}

#[tokio::test]
async fn for_each_tenant_end_to_end() {
    let Some(e) = env("for_each_tenant_end_to_end") else {
        return;
    };
    let mut sys = conn(&e.addr, &e.system).await;
    let mut members = Vec::new();
    for m in &e.members {
        let mut c = conn(&e.addr, m).await;
        app_tables(&mut c).await;
        members.push((m.clone(), c));
    }
    // A clean declaring tenant.
    let _ = sql(&mut sys, "DROP TRIGGER security.sync_users").await;
    let _ = sql(&mut sys, "DROP TRIGGER security.sync_users_appb").await;
    let _ = sql(
        &mut sys,
        "DROP STREAM security.user_events IMMEDIATE CASCADE",
    )
    .await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS security").await;
    ok(&mut sys, "CREATE DATABASE security").await;

    // 1. The definitions.
    let t0 = Instant::now();
    ok(
        &mut sys,
        "CREATE STREAM security.user_events FOR EACH TENANT KEY (user_id) PARTITIONS 3 RETENTION '7 days'",
    )
    .await;
    ok(&mut sys, SYNC_USERS).await;
    ok(&mut sys, SYNC_USERS_APPB).await;
    let defs = ok(&mut sys, "SHOW INHERITED DEFINITIONS").await;
    println!("definitions ({:?}): {defs:?}", t0.elapsed());
    assert_eq!(defs.len(), 3, "{defs:?}");
    // Nothing exists in members yet.
    for (m, c) in &mut members {
        let inst = ok(c, "SHOW INHERITED INSTANCES").await;
        println!("{m} before any event: {inst:?}");
        assert!(
            inst.iter().all(|r| r[5].as_deref() == Some("none")),
            "{m}: {inst:?}"
        );
    }

    // 2. The system tenant publishes for each client.
    let per = 40u64;
    let t1 = Instant::now();
    for (i, (m, _)) in members.iter().enumerate() {
        let base = 1000 * (i as u64 + 1);
        publish_for(&mut sys, m, base, base + per).await;
    }
    println!(
        "published {} events for {} clients (first publishes created the instances) in {:?}",
        per * members.len() as u64,
        members.len(),
        t1.elapsed()
    );
    for (i, (m, c)) in members.iter_mut().enumerate() {
        let base = 1000 * (i as u64 + 1);
        let a = wait_count(c, "SELECT COUNT(*) FROM app_a.users", per, 90).await;
        let b = wait_count(c, "SELECT COUNT(*) FROM app_b.members", per, 90).await;
        let twice = count(c, "SELECT COUNT(*) FROM app_a.users WHERE n <> 1").await;
        let foreign = count(
            c,
            &format!(
                "SELECT COUNT(*) FROM app_a.users WHERE id < {base} OR id >= {}",
                base + per
            ),
        )
        .await;
        let origin = ok(c, "SELECT DISTINCT origin FROM app_a.users").await;
        println!(
            "{m}: users {a}, members {b}, applied twice {twice}, other clients' {foreign}, origin {origin:?}"
        );
        assert_eq!((a, b, twice, foreign), (per, per, 0, 0), "{m}");
        assert_eq!(origin, vec![vec![Some(e.system.clone())]], "{m}");
        let inst = ok(c, "SHOW INHERITED INSTANCES").await;
        assert!(
            inst.iter().all(|r| r[5].as_deref() == Some("active")),
            "{m}: {inst:?}"
        );
        // The runs were the member's sprig_inherited.
        let g = ok(c, "SHOW GRANTS FOR sprig_inherited").await;
        println!("{m} sprig_inherited: {g:?}");
    }

    // 3. Refusals: the ancestor rule.
    let (m0, m1) = (members[0].0.clone(), members[1].0.clone());
    let c0 = &mut members[0].1;
    let r = sql(
        c0,
        &format!(
            "INSERT INTO security.user_events FOR TENANT {m1} (key, value) VALUES (1, '{{}}')"
        ),
    )
    .await;
    assert_eq!(r.as_ref().err().map(|e| e.0), Some(39896), "sibling: {r:?}");
    let r = sql(
        &mut sys,
        "INSERT INTO security.user_events FOR TENANT no_such_tenant_r9fet (key, value) VALUES (1, '{}')",
    )
    .await;
    assert_eq!(r.as_ref().err().map(|e| e.0), Some(39896), "unknown: {r:?}");
    let r = sql(
        &mut sys,
        "INSERT INTO security.user_events (key, value) VALUES (1, '{}')",
    )
    .await;
    assert_eq!(
        r.as_ref().err().map(|e| e.0),
        Some(39896),
        "no target: {r:?}"
    );
    let r = sql(c0, "DROP STREAM security.user_events").await;
    assert_eq!(
        r.as_ref().err().map(|e| e.0),
        Some(39897),
        "member drop: {r:?}"
    );
    let r = sql(
        c0,
        "ALTER INHERITED STREAM security.user_events SPRIG SET (stream.partitions_default = 3)",
    )
    .await;
    assert_eq!(
        r.as_ref().err().map(|e| e.0),
        Some(39897),
        "not overridable: {r:?}"
    );

    // 4. A member's own publish and override.
    ok(
        c0,
        "INSERT INTO security.user_events (key, value, headers) VALUES (9001, \
         JSON_OBJECT('user_id', 9001, 'email', 'own@x'), JSON_OBJECT('x-sprig-origin-tenant', 'forged'))",
    )
    .await;
    assert_eq!(
        wait_count(
            c0,
            "SELECT COUNT(*) FROM app_a.users WHERE id = 9001",
            1,
            30
        )
        .await,
        1
    );
    let origin = ok(c0, "SELECT origin FROM app_a.users WHERE id = 9001").await;
    assert_eq!(
        origin,
        vec![vec![Some(m0.clone())]],
        "x-sprig-origin-tenant can't be forged"
    );
    ok(
        c0,
        "ALTER INHERITED STREAM security.user_events SPRIG SET (stream.retention_hours = 72)",
    )
    .await;
    let o = ok(c0, "SHOW INHERITED OVERRIDES").await;
    println!("{m0} overrides: {o:?}");
    assert_eq!(o.len(), 1);

    // 5. Disabling sync_users_appb in one client stops it there only.
    let (m2, c2) = (members[2].0.clone(), &mut members[2].1);
    ok(
        c2,
        "ALTER INHERITED TRIGGER security.sync_users_appb DISABLE",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(7)).await;
    publish_for(&mut sys, &m2, 3500, 3510).await;
    publish_for(&mut sys, &m0, 1500, 1510).await;
    let c0 = &mut members[0].1;
    assert_eq!(
        wait_count(c0, "SELECT COUNT(*) FROM app_b.members", per + 1 + 10, 60).await,
        per + 11
    );
    let c2 = &mut members[2].1;
    assert_eq!(
        wait_count(c2, "SELECT COUNT(*) FROM app_a.users", per + 10, 60).await,
        per + 10
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        count(c2, "SELECT COUNT(*) FROM app_b.members").await,
        per,
        "{m2} disabled appb"
    );
    ok(
        c2,
        "ALTER INHERITED TRIGGER security.sync_users_appb ENABLE",
    )
    .await;
    assert_eq!(
        wait_count(c2, "SELECT COUNT(*) FROM app_b.members", per + 10, 60).await,
        per + 10,
        "{m2}: ENABLE resumes where it stopped"
    );

    // 5b. A member revokes what sync_users_appb needs: it waits (blocked,
    // 39242), burns no event, and catches up when the grant returns.
    let c2 = &mut members[2].1;
    ok(
        c2,
        "REVOKE INSERT ON app_b.members FROM 'sprig_inherited'@'%'",
    )
    .await;
    publish_for(&mut sys, &m2, 3600, 3605).await;
    let c2 = &mut members[2].1;
    assert_eq!(
        wait_count(
            c2,
            "SELECT COUNT(*) FROM app_a.users WHERE id >= 3600",
            5,
            60
        )
        .await,
        5
    );
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let inst = ok(c2, "SHOW INHERITED INSTANCES").await;
        if inst.iter().any(|r| {
            r[2].as_deref() == Some("sync_users_appb") && r[5].as_deref() == Some("blocked")
        }) {
            println!("{m2}: sync_users_appb blocked after the REVOKE");
            break;
        }
        assert!(Instant::now() < end, "{m2}: not blocked: {inst:?}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(
        count(c2, "SELECT COUNT(*) FROM app_b.members").await,
        per + 10
    );
    ok(c2, "GRANT INSERT ON app_b.members TO 'sprig_inherited'@'%'").await;
    assert_eq!(
        wait_count(c2, "SELECT COUNT(*) FROM app_b.members", per + 15, 60).await,
        per + 15,
        "{m2}: the waiting events ran once the grant returned"
    );

    // 6. A new body in the system tenant reaches every client.
    ok(&mut sys, SYNC_USERS_V2).await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    for (i, (m, _)) in members.iter().enumerate() {
        publish_for(
            &mut sys,
            m,
            7000 + i as u64 * 100,
            7000 + i as u64 * 100 + 5,
        )
        .await;
    }
    for (i, (m, c)) in members.iter_mut().enumerate() {
        let lo = 7000 + i as u64 * 100;
        let q = format!(
            "SELECT COUNT(*) FROM app_a.users WHERE id >= {lo} AND id < {} AND n = 100",
            lo + 5
        );
        assert_eq!(wait_count(c, &q, 5, 60).await, 5, "{m}: version 2");
    }

    // 7. Exactly once with the worker killed mid-stream.
    if e.pidfile.is_some() {
        let n = 150u64;
        let sys_addr = e.addr.clone();
        let names: Vec<String> = members.iter().map(|(m, _)| m.clone()).collect();
        let sys_name = e.system.clone();
        let pubs = tokio::spawn(async move {
            let mut s = conn(&sys_addr, &sys_name).await;
            for k in 0..n / 10 {
                for (i, m) in names.iter().enumerate() {
                    let base = 20_000 + i as u64 * 10_000 + k * 10;
                    publish_for(&mut s, m, base, base + 10).await;
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(kill_worker(&e));
        println!("worker killed with kill -9");
        tokio::time::sleep(Duration::from_secs(2)).await;
        start_worker(&e);
        pubs.await.unwrap();
        for (i, (m, c)) in members.iter_mut().enumerate() {
            let lo = 20_000 + i as u64 * 10_000;
            let q = format!(
                "SELECT COUNT(*) FROM app_a.users WHERE id >= {lo} AND id < {}",
                lo + n
            );
            let a = wait_count(c, &q, n, 180).await;
            let twice = count(
                c,
                &format!(
                    "SELECT COUNT(*) FROM app_a.users WHERE id >= {lo} AND id < {} AND n <> 100",
                    lo + n
                ),
            )
            .await;
            let b = count(
                c,
                &format!(
                    "SELECT COUNT(*) FROM app_b.members WHERE user_id >= {lo} AND user_id < {}",
                    lo + n
                ),
            )
            .await;
            let b_twice = count(
                c,
                &format!(
                    "SELECT COUNT(*) FROM (SELECT user_id FROM app_b.members WHERE user_id >= {lo} \
                     GROUP BY user_id HAVING COUNT(*) > 1) d"
                ),
            )
            .await;
            println!(
                "{m} after kill -9: users {a}/{n} (twice {twice}), members {b}/{n} (twice {b_twice})"
            );
            assert_eq!((a, twice, b, b_twice), (n, 0, n, 0), "{m}");
        }
    }

    // 8. A tenant created after the definitions: its instance and triggers
    // come with its first event; nothing messaging-related was added.
    if let (Some(late), Some(create)) = (&e.late, &e.create) {
        let st = std::process::Command::new("sh")
            .args(["-c", &format!("{create} {late}")])
            .status()
            .unwrap();
        assert!(st.success(), "creating {late}");
        let mut c = conn(&e.addr, late).await;
        app_tables(&mut c).await;
        let inst = ok(&mut c, "SHOW INHERITED INSTANCES").await;
        assert!(
            inst.iter().all(|r| r[5].as_deref() == Some("none")),
            "{inst:?}"
        );
        // Reading an instance that doesn't exist yet: no records, and no
        // instance created.
        assert_eq!(
            count(&mut c, "SELECT COUNT(*) FROM security.user_events").await,
            0
        );
        let inst = ok(&mut c, "SHOW INHERITED INSTANCES").await;
        assert!(
            inst.iter().all(|r| r[5].as_deref() == Some("none")),
            "a read created an instance: {inst:?}"
        );
        let t = Instant::now();
        publish_for(&mut sys, late, 50_000, 50_020).await;
        let first = t.elapsed();
        let a = wait_count(&mut c, "SELECT COUNT(*) FROM app_a.users", 20, 90).await;
        let b = wait_count(&mut c, "SELECT COUNT(*) FROM app_b.members", 20, 90).await;
        println!(
            "{late} (new): users {a}, members {b}; first publish {first:?}, all applied {:?}",
            t.elapsed()
        );
        assert_eq!((a, b), (20, 20));
        ok(&mut c, "DROP DATABASE app_a").await;
        ok(&mut c, "DROP DATABASE app_b").await;
    }

    // Cleanup: the definitions (every instance goes), the members' tables.
    ok(&mut sys, "DROP TRIGGER security.sync_users").await;
    ok(&mut sys, "DROP TRIGGER security.sync_users_appb").await;
    ok(&mut sys, "DROP STREAM security.user_events IMMEDIATE").await;
    ok(&mut sys, "DROP DATABASE security").await;
    for (_, c) in &mut members {
        let _ = sql(c, "DROP DATABASE IF EXISTS app_a").await;
        let _ = sql(c, "DROP DATABASE IF EXISTS app_b").await;
        let _ = sql(c, "DROP DATABASE IF EXISTS security").await;
    }
}

/// Chapter 45's platform stream: a client publishes up into the system
/// tenant's ordinary stream only with the owner's consent, keyed by its
/// own name.
#[tokio::test]
async fn in_tenant_publish_up() {
    let Some(e) = env("in_tenant_publish_up") else {
        return;
    };
    let mut sys = conn(&e.addr, &e.system).await;
    let (m0, m1) = (e.members[0].clone(), e.members[1].clone());
    let mut c0 = conn(&e.addr, &m0).await;
    let mut c1 = conn(&e.addr, &m1).await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS platform").await;
    ok(&mut sys, "CREATE DATABASE platform").await;
    ok(
        &mut sys,
        "CREATE STREAM platform.client_events KEY (client_code) PARTITIONS 2",
    )
    .await;
    let up = |k: &str| {
        format!(
            "INSERT INTO platform.client_events IN TENANT {} (key, value) VALUES ('{k}', '{{\"type\":\"user.added\"}}')",
            e.system
        )
    };
    // No consent yet: 1142.
    let r = sql(&mut c0, &up(&m0)).await;
    assert_eq!(r.err().map(|e| e.0), Some(1142));
    ok(
        &mut sys,
        "ALTER DATABASE platform SPRIG SET (stream.descendant_publish = 'on')",
    )
    .await;
    // Another client's key: 39957; its own: accepted; none: its name.
    let r = sql(&mut c0, &up(&m1)).await;
    assert_eq!(r.err().map(|e| e.0), Some(39957));
    ok(&mut c0, &up(&m0)).await;
    ok(
        &mut c1,
        &format!(
            "INSERT INTO platform.client_events IN TENANT {} (value) VALUES ('{{}}')",
            e.system
        ),
    )
    .await;
    // Sideways: refused.
    let r = sql(
        &mut c1,
        &format!(
            "INSERT INTO platform.client_events IN TENANT {m0} (key, value) VALUES ('{m1}', '{{}}')"
        ),
    )
    .await;
    assert_eq!(r.err().map(|e| e.0), Some(1142));
    let end = Instant::now() + Duration::from_secs(30);
    let rows = loop {
        let rows = ok(
            &mut sys,
            "SELECT `key`, JSON_VALUE(headers, '$.\"x-sprig-origin-tenant\"') FROM platform.client_events ORDER BY `key`",
        )
        .await;
        if rows.len() >= 2 || Instant::now() > end {
            break rows;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    println!("platform stream: {rows:?}");
    assert_eq!(
        rows,
        vec![
            vec![Some(m0.clone()), Some(m0.clone())],
            vec![Some(m1.clone()), Some(m1.clone())]
        ]
    );
    ok(&mut sys, "DROP DATABASE platform").await;
}

/// Chapter 42's cross-tenant outbox: `FOR TENANT` and `IN TENANT` inside a
/// transaction land exactly once if it commits, never if it rolls back.
#[tokio::test]
async fn cross_tenant_publishes_in_transactions() {
    let Some(e) = env("cross_tenant_publishes_in_transactions") else {
        return;
    };
    let mut sys = conn(&e.addr, &e.system).await;
    let m0 = e.members[0].clone();
    let mut c0 = conn(&e.addr, &m0).await;
    let _ = sql(&mut sys, "DROP STREAM security.tx_events IMMEDIATE CASCADE").await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS security").await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS platform").await;
    let _ = sql(&mut c0, "DROP DATABASE IF EXISTS security").await;
    let _ = sql(&mut c0, "DROP DATABASE IF EXISTS app_x").await;
    ok(&mut sys, "CREATE DATABASE security").await;
    ok(&mut sys, "CREATE DATABASE platform").await;
    ok(&mut sys, "CREATE TABLE platform.audit (id INT PRIMARY KEY)").await;
    ok(
        &mut sys,
        "CREATE STREAM security.tx_events FOR EACH TENANT PARTITIONS 2",
    )
    .await;
    ok(
        &mut sys,
        "CREATE STREAM platform.client_events KEY (client_code) PARTITIONS 2",
    )
    .await;
    ok(
        &mut sys,
        "ALTER DATABASE platform SPRIG SET (stream.descendant_publish = 'on')",
    )
    .await;
    let pubs = |k: u32| {
        format!(
            "INSERT INTO security.tx_events FOR TENANT {m0} (key, value) VALUES ('{k}', 'v{k}')"
        )
    };
    // Rolled back: never delivered (the system's own write goes too).
    ok(&mut sys, "START TRANSACTION").await;
    ok(&mut sys, "INSERT INTO platform.audit VALUES (1)").await;
    ok(&mut sys, &pubs(1)).await;
    ok(&mut sys, "ROLLBACK").await;
    // Committed: delivered once, with the system's write.
    ok(&mut sys, "START TRANSACTION").await;
    ok(&mut sys, "INSERT INTO platform.audit VALUES (2)").await;
    ok(&mut sys, &pubs(2)).await;
    ok(&mut sys, &pubs(3)).await;
    ok(&mut sys, "COMMIT").await;
    // A client publishing up inside its own transaction.
    ok(&mut c0, "CREATE DATABASE app_x").await;
    ok(&mut c0, "CREATE TABLE app_x.users (id INT PRIMARY KEY)").await;
    // The outbox goes into one of the client's databases: its current one.
    ok(&mut c0, "USE app_x").await;
    let up = format!(
        "INSERT INTO platform.client_events IN TENANT {} (value) VALUES ('{{\"type\":\"user.added\"}}')",
        e.system
    );
    ok(&mut c0, "START TRANSACTION").await;
    ok(&mut c0, "INSERT INTO app_x.users VALUES (1)").await;
    ok(&mut c0, &up).await;
    ok(&mut c0, "ROLLBACK").await;
    ok(&mut c0, "START TRANSACTION").await;
    ok(&mut c0, "INSERT INTO app_x.users VALUES (2)").await;
    ok(&mut c0, &up).await;
    ok(&mut c0, "COMMIT").await;
    let t = Instant::now();
    let got = wait_count(&mut c0, "SELECT COUNT(*) FROM security.tx_events", 2, 60).await;
    let up_got = wait_count(
        &mut sys,
        "SELECT COUNT(*) FROM platform.client_events",
        1,
        60,
    )
    .await;
    println!(
        "committed cross-tenant publishes visible after {:?}: {got} into the member, {up_got} up",
        t.elapsed()
    );
    // Some more time for anything that shouldn't come.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let keys = ok(
        &mut c0,
        "SELECT `key` FROM security.tx_events ORDER BY `key`",
    )
    .await;
    assert_eq!(keys, vec![vec![Some("2".into())], vec![Some("3".into())]]);
    assert_eq!(
        count(&mut sys, "SELECT COUNT(*) FROM platform.client_events").await,
        1
    );
    ok(&mut sys, "DROP STREAM security.tx_events IMMEDIATE").await;
    ok(&mut sys, "DROP DATABASE security").await;
    ok(&mut sys, "DROP DATABASE platform").await;
    let _ = sql(&mut c0, "DROP DATABASE IF EXISTS security").await;
    ok(&mut c0, "DROP DATABASE app_x").await;
}

// ---------------------------------------------------------------------------
// Round 3 (r9fet3): DROP DATABASE and the outbox, inherited exchanges, queues
// and bindings, broadcast.
//
//   SPRIG_TEST_INHERIT_AMQP=127.0.0.1:29672      # the gateway's AMQP listener
//   SPRIG_TEST_INHERIT_ROOT=mysql://root:...@127.0.0.1:13301   # the cluster
// ---------------------------------------------------------------------------

use lapin::options::{
    BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Connection, ConnectionProperties};

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.is_empty())
}

/// Creates tenant `name` under the system tenant with the create command.
fn create_tenant(e: &Env, name: &str) {
    let cmd = e.create.as_deref().expect("SPRIG_TEST_INHERIT_CREATE");
    let st = std::process::Command::new("sh")
        .args(["-c", &format!("{cmd} {name} {}", e.system)])
        .status()
        .unwrap();
    assert!(st.success(), "creating {name}");
}

async fn amqp(addr: &str, tenant: &str, vhost: &str) -> Connection {
    Connection::connect(
        &format!("amqp://{tenant}.app:{PASSWORD}@{addr}/{vhost}"),
        ConnectionProperties::default(),
    )
    .await
    .unwrap()
}

fn header(m: &lapin::message::BasicGetMessage, k: &str) -> Option<String> {
    match m.properties.headers().as_ref()?.inner().get(k)? {
        AMQPValue::LongString(s) => Some(s.to_string()),
        AMQPValue::ShortString(s) => Some(s.to_string()),
        _ => None,
    }
}

/// Every message of queue `q` now (acknowledged): body, x-sprig-tenant,
/// x-sprig-origin-tenant.
async fn take_all(
    ch: &lapin::Channel,
    q: &str,
    want: usize,
    secs: u64,
) -> Vec<(String, Option<String>, Option<String>)> {
    let mut out = Vec::new();
    let end = Instant::now() + Duration::from_secs(secs);
    loop {
        match ch
            .basic_get(q.into(), BasicGetOptions { no_ack: true })
            .await
            .unwrap()
        {
            Some(m) => out.push((
                String::from_utf8_lossy(&m.data).into_owned(),
                header(&m, "x-sprig-tenant"),
                header(&m, "x-sprig-origin-tenant"),
            )),
            None if out.len() >= want || Instant::now() > end => return out,
            None => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

async fn publish(
    ch: &lapin::Channel,
    exchange: &str,
    key: &str,
    body: &str,
    tenant: Option<&str>,
) -> bool {
    let mut props = BasicProperties::default().with_delivery_mode(2);
    if let Some(t) = tenant {
        let mut h = FieldTable::default();
        h.insert("x-sprig-tenant".into(), AMQPValue::LongString(t.into()));
        props = props.with_headers(h);
    }
    ch.basic_publish(
        exchange.into(),
        key.into(),
        BasicPublishOptions::default(),
        body.as_bytes(),
        props,
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .is_ack()
}

#[tokio::test]
async fn drop_database_forwards_the_outbox_first() {
    let Some(e) = env("drop_database_forwards_the_outbox_first") else {
        return;
    };
    let Some(root) = var("SPRIG_TEST_INHERIT_ROOT") else {
        eprintln!("no SPRIG_TEST_INHERIT_ROOT: skipped");
        return;
    };
    let mut sys = conn(&e.addr, &e.system).await;
    let m0 = e.members[0].clone();
    let mut c0 = conn(&e.addr, &m0).await;
    let _ = sql(&mut sys, "DROP STREAM obx.drop_events IMMEDIATE CASCADE").await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS obx").await;
    let _ = sql(&mut c0, "DROP DATABASE IF EXISTS obx").await;
    // The outbox of a publish goes into the database it names (here obx,
    // the definition's database in the declarer).
    ok(&mut sys, "CREATE DATABASE obx").await;
    ok(
        &mut sys,
        "CREATE STREAM obx.drop_events FOR EACH TENANT PARTITIONS 1",
    )
    .await;
    ok(&mut sys, "USE obx").await;
    // One transactional publish makes the outbox table (and forwards).
    ok(&mut sys, "START TRANSACTION").await;
    ok(
        &mut sys,
        &format!("INSERT INTO obx.drop_events FOR TENANT {m0} (key, value) VALUES ('0', 'v0')"),
    )
    .await;
    ok(&mut sys, "COMMIT").await;
    assert_eq!(
        wait_count(&mut c0, "SELECT COUNT(*) FROM obx.drop_events", 1, 60).await,
        1
    );
    // Rows a crashed gateway left behind: written straight into the table.
    let mut r = Conn::from_url(&root).await.unwrap();
    // The declaring tenant: the one whose catalog holds the definition.
    let mut sys_id = 0u64;
    let cats: Vec<(String,)> = r
        .query(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = \
             'inherited_defs'",
        )
        .await
        .unwrap();
    for (c,) in cats {
        let n: Option<u64> = r
            .query_first(format!(
                "SELECT COUNT(*) FROM `{c}`.inherited_defs WHERE name = 'drop_events' \
                 AND db_name = 'obx'"
            ))
            .await
            .unwrap();
        if n == Some(1) {
            sys_id = c[1..c.find('_').unwrap()].parse().unwrap();
        }
    }
    assert!(sys_id > 0);
    let schema: String = r
        .query_first(format!(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = '__sprig_xo' \
             AND table_schema LIKE 't{sys_id}\\_d%' ORDER BY create_time DESC LIMIT 1"
        ))
        .await
        .unwrap()
        .unwrap();
    // Two rows pending, for the member's instance of the definition: the
    // drop forwards them first.
    let def_obj: u64 = r
        .query_first(format!(
            "SELECT object_id FROM t{sys_id}_sprig.inherited_defs WHERE name = 'drop_events' \
             AND db_name = 'obx'"
        ))
        .await
        .unwrap()
        .unwrap();
    let m0_id: u64 = {
        let rows: Vec<(String,)> = r
            .query(format!(
                "SELECT table_schema FROM information_schema.tables WHERE table_name = \
                 'inherited_instances' AND table_schema <> 't{sys_id}_sprig'"
            ))
            .await
            .unwrap();
        let mut found = 0;
        for (s,) in rows {
            let id: u64 = s[1..s.find('_').unwrap()].parse().unwrap();
            let n: Option<u64> = r
                .query_first(format!(
                    "SELECT COUNT(*) FROM {s}.inherited_instances WHERE def_object_id = {def_obj} \
                     AND def_tenant_id = {sys_id}"
                ))
                .await
                .unwrap();
            if n == Some(1) {
                // The member the first publish went to (the only instance).
                found = id;
            }
        }
        found
    };
    assert!(m0_id > 0);
    let dest = format!(
        "{{\"to\":\"instance\",\"tenant\":{m0_id},\"def_tenant\":{sys_id},\"def_object\":{def_obj}}}"
    );
    for k in 1..=2 {
        r.query_drop(format!(
            "INSERT INTO `{schema}`.__sprig_xo (out_id, kind, dest, k, v, headers) VALUES \
             (UNHEX(REPLACE(UUID(), '-', '')), 'cross_tenant', '{dest}', 'k{k}', 'pending{k}', \
             JSON_OBJECT('x-sprig-tenant', '{m0}', 'x-sprig-origin-tenant', '{}'))",
            e.system
        ))
        .await
        .unwrap();
    }
    // And one whose destination is gone: it can't be forwarded.
    r.query_drop(format!(
        "INSERT INTO `{schema}`.__sprig_xo (out_id, kind, dest, k, v) VALUES \
         (UNHEX(REPLACE(UUID(), '-', '')), 'cross_tenant', \
         '{{\"to\":\"instance\",\"tenant\":{m0_id},\"def_tenant\":{sys_id},\"def_object\":1}}', \
         'x', 'stuck')"
    ))
    .await
    .unwrap();
    ok(
        &mut sys,
        "ALTER DATABASE obx SPRIG SET (inherit.outbox_drain_seconds = 3)",
    )
    .await;
    let t = Instant::now();
    let refused = sql(&mut sys, "DROP DATABASE obx").await;
    println!(
        "DROP DATABASE with a stuck row: {refused:?} after {:?}",
        t.elapsed()
    );
    let (code, msg) = refused.expect_err("refused while a row is stuck");
    assert_eq!(code, 39245, "{msg}");
    assert!(msg.contains("holds 1 committed"), "{msg}");
    // The two good rows were forwarded meanwhile.
    assert_eq!(
        wait_count(&mut c0, "SELECT COUNT(*) FROM obx.drop_events", 3, 30).await,
        3
    );
    // Remove the stuck row; the drop goes through.
    r.query_drop(format!(
        "DELETE FROM `{schema}`.__sprig_xo WHERE v = 'stuck'"
    ))
    .await
    .unwrap();
    let t = Instant::now();
    ok(&mut sys, "DROP DATABASE obx").await;
    println!("DROP DATABASE after the outbox emptied: {:?}", t.elapsed());
    let vals = ok(
        &mut c0,
        "SELECT CAST(value AS CHAR) FROM obx.drop_events ORDER BY value",
    )
    .await;
    assert_eq!(
        vals,
        vec![
            vec![Some("pending1".into())],
            vec![Some("pending2".into())],
            vec![Some("v0".into())]
        ]
    );
    ok(&mut sys, "DROP STREAM obx.drop_events IMMEDIATE").await;
    let _ = sql(&mut c0, "DROP DATABASE IF EXISTS obx").await;
}

const MAIL_TRIGGER: &str = r"
CREATE TRIGGER r9mq.on_audit ON QUEUE r9mq.audit_q
  FOR EACH TENANT GRANTS (INSERT, UPDATE, SELECT ON app_q.audit)
  LANGUAGE javascript AS $$
export default function (ctx) {
  const b = JSON.parse(ctx.message.body);
  db.exec('INSERT INTO app_q.audit (n, k, runs) VALUES (?, ?, 1) ' +
          'ON DUPLICATE KEY UPDATE runs = runs + 1', [b.n, ctx.message.routing_key]);
}
$$";

#[tokio::test]
async fn inherited_queues_end_to_end() {
    let Some(e) = env("inherited_queues_end_to_end") else {
        return;
    };
    let Some(amqp_addr) = var("SPRIG_TEST_INHERIT_AMQP") else {
        eprintln!("no SPRIG_TEST_INHERIT_AMQP: skipped");
        return;
    };
    let mut sys = conn(&e.addr, &e.system).await;
    let mut ms = Vec::new();
    for m in &e.members {
        let mut c = conn(&e.addr, m).await;
        let _ = sql(&mut c, "DROP DATABASE IF EXISTS r9mq").await;
        let _ = sql(&mut c, "DROP DATABASE IF EXISTS app_q").await;
        ok(&mut c, "CREATE DATABASE app_q").await;
        ok(
            &mut c,
            "CREATE TABLE app_q.audit (n INT PRIMARY KEY, k VARCHAR(64), runs INT NOT NULL)",
        )
        .await;
        ms.push((m.clone(), c));
    }
    let _ = sql(&mut sys, "DROP TRIGGER r9mq.on_audit").await;
    let _ = sql(&mut sys, "DROP EXCHANGE r9mq.jobs IMMEDIATE CASCADE").await;
    let _ = sql(&mut sys, "DROP QUEUE r9mq.welcome_mail IMMEDIATE CASCADE").await;
    let _ = sql(&mut sys, "DROP QUEUE r9mq.audit_q IMMEDIATE CASCADE").await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS r9mq").await;
    ok(&mut sys, "CREATE DATABASE r9mq").await;
    let t = Instant::now();
    for q in [
        "CREATE EXCHANGE r9mq.jobs TYPE topic FOR EACH TENANT",
        "CREATE QUEUE r9mq.welcome_mail FOR EACH TENANT WITH (x_queue_type = 'quorum')",
        "CREATE QUEUE r9mq.audit_q FOR EACH TENANT",
        "BIND QUEUE r9mq.welcome_mail TO EXCHANGE r9mq.jobs KEY 'mail.*' FOR EACH TENANT",
        "BIND QUEUE r9mq.audit_q TO EXCHANGE r9mq.jobs KEY 'audit.#' FOR EACH TENANT",
        MAIL_TRIGGER,
    ] {
        ok(&mut sys, q).await;
    }
    println!("six definitions: {:?}", t.elapsed());
    // Before any use, members show inherited definitions without instances.
    let shown = ok(&mut ms[0].1, "SHOW INHERITED INSTANCES").await;
    assert!(
        shown
            .iter()
            .any(|r| r[2].as_deref() == Some("welcome_mail") && r[5].as_deref() == Some("none")),
        "{shown:?}"
    );
    // Each member's first AMQP connection makes its instances (and the
    // database r9mq, which no member has).
    let mut chans = Vec::new();
    for (m, _) in &ms {
        let t = Instant::now();
        let c = amqp(&amqp_addr, m, "r9mq").await;
        let ch = c.create_channel().await.unwrap();
        ch.queue_declare(
            "welcome_mail".into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
        println!(
            "{m}: first connection with its instances: {:?}",
            t.elapsed()
        );
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .unwrap();
        chans.push((m.clone(), c, ch));
    }
    // The declaring tenant targets each member: the header, or @t:key.
    let sc = amqp(&amqp_addr, &e.system, "r9mq").await;
    let sch = sc.create_channel().await.unwrap();
    sch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for (i, (m, _, _)) in chans.iter().enumerate() {
        for j in 0..5 {
            let body = format!("{m}-{j}");
            let acked = if j % 2 == 0 {
                publish(&sch, "jobs", "mail.welcome", &body, Some(m)).await
            } else {
                publish(&sch, "jobs", &format!("@{m}:mail.welcome"), &body, None).await
            };
            assert!(acked, "publish {i}/{j}");
        }
    }
    // Refusals: no target in the declarer (unroutable, still confirmed),
    // a tenant that isn't a member, a sibling targeting a sibling.
    assert!(publish(&sch, "jobs", "mail.welcome", "nobody", None).await);
    assert!(!publish(&sch, "jobs", "mail.welcome", "ghost", Some("r9f3_nosuch")).await);
    let (m1, _, ch1) = &chans[1];
    let sib = &chans[2].0;
    assert!(
        !publish(
            ch1,
            "jobs",
            &format!("@{sib}:mail.welcome"),
            "sideways",
            None
        )
        .await
    );
    // A member's own publish into its instance, stamped with its name (a
    // forged origin is overwritten).
    {
        let mut h = FieldTable::default();
        h.insert(
            "x-sprig-origin-tenant".into(),
            AMQPValue::LongString("forged".into()),
        );
        let ok = ch1
            .basic_publish(
                "jobs".into(),
                "mail.self".into(),
                BasicPublishOptions::default(),
                format!("{m1}-own").as_bytes(),
                BasicProperties::default().with_headers(h),
            )
            .await
            .unwrap()
            .await
            .unwrap()
            .is_ack();
        assert!(ok);
    }
    for (i, (m, _, ch)) in chans.iter().enumerate() {
        let want = if i == 1 { 6 } else { 5 };
        let got = take_all(ch, "welcome_mail", want, 30).await;
        assert_eq!(got.len(), want, "{m}: {got:?}");
        for (body, t, o) in &got {
            assert!(body.starts_with(m.as_str()), "{m} got {body}");
            assert_eq!(t.as_deref(), Some(m.as_str()), "{body}");
            let origin = if body.ends_with("-own") {
                m.as_str()
            } else {
                e.system.as_str()
            };
            assert_eq!(o.as_deref(), Some(origin), "{body}");
        }
    }
    // A member can't delete an inherited queue (403), but may purge it.
    {
        let c = amqp(&amqp_addr, &chans[0].0, "r9mq").await;
        let ch = c.create_channel().await.unwrap();
        let r = ch
            .queue_delete(
                "welcome_mail".into(),
                lapin::options::QueueDeleteOptions::default(),
            )
            .await;
        assert!(r.is_err(), "deleting an inherited queue: {r:?}");
    }
    // SQL: targeted publishes from the declarer, outside and inside a
    // transaction (rolled back: nothing; committed: once).
    let m0 = chans[0].0.clone();
    ok(&mut sys, "USE r9mq").await;
    ok(
        &mut sys,
        &format!("CALL sprig.mq_publish('jobs', '@{m0}:mail.sql', 'sql-now', '{{}}', 0, 'r9mq')"),
    )
    .await;
    ok(&mut sys, "START TRANSACTION").await;
    ok(
        &mut sys,
        &format!(
            "CALL sprig.mq_publish('jobs', '@{m0}:mail.sql', 'sql-rolled-back', '{{}}', 0, 'r9mq')"
        ),
    )
    .await;
    ok(&mut sys, "ROLLBACK").await;
    ok(&mut sys, "START TRANSACTION").await;
    ok(
        &mut sys,
        &format!(
            "CALL sprig.mq_publish('jobs', '@{m0}:mail.sql', 'sql-committed', '{{}}', 0, 'r9mq')"
        ),
    )
    .await;
    ok(&mut sys, "COMMIT").await;
    let got = take_all(&chans[0].2, "welcome_mail", 2, 30).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let mut more = take_all(&chans[0].2, "welcome_mail", 0, 1).await;
    let mut all = got;
    all.append(&mut more);
    let mut bodies: Vec<String> = all.iter().map(|x| x.0.clone()).collect();
    bodies.sort();
    assert_eq!(
        bodies,
        vec!["sql-committed".to_owned(), "sql-now".to_owned()]
    );
    // The inherited trigger on audit_q runs in each member as its
    // sprig_inherited, exactly once per message, through a worker kill -9.
    let per = 40u64;
    for (m, _, _) in &chans {
        for j in 0..per {
            let body = format!("{{\"n\": {j}}}");
            ok(
                &mut sys,
                &format!(
                    "CALL sprig.mq_publish('jobs', '@{m}:audit.user', '{body}', '{{}}', 0, 'r9mq')"
                ),
            )
            .await;
            if j == per / 2 && m == &chans[0].0 && kill_worker(&e) {
                println!("worker killed with kill -9 at message {j} of {m}");
                tokio::time::sleep(Duration::from_secs(2)).await;
                start_worker(&e);
            }
        }
    }
    let t = Instant::now();
    for (m, c) in &mut ms {
        let got = wait_count(c, "SELECT COUNT(*) FROM app_q.audit", per, 180).await;
        assert_eq!(got, per, "{m}: every audit message ran");
    }
    println!(
        "triggers on the inherited queue caught up in {:?}",
        t.elapsed()
    );
    tokio::time::sleep(Duration::from_secs(5)).await;
    for (m, c) in &mut ms {
        assert_eq!(
            count(c, "SELECT COUNT(*) FROM app_q.audit WHERE runs <> 1").await,
            0,
            "{m}: a message ran twice"
        );
        assert_eq!(count(c, "SELECT COUNT(*) FROM app_q.audit").await, per);
    }
    // A member created after the definitions: its first connection makes
    // everything, its trigger included.
    if e.create.is_some() {
        let late = format!(
            "{}q",
            e.late
                .clone()
                .unwrap_or_else(|| format!("{}late", e.system))
        );
        create_tenant(&e, &late);
        let mut lc = conn(&e.addr, &late).await;
        ok(&mut lc, "CREATE DATABASE app_q").await;
        ok(
            &mut lc,
            "CREATE TABLE app_q.audit (n INT PRIMARY KEY, k VARCHAR(64), runs INT NOT NULL)",
        )
        .await;
        let t = Instant::now();
        for j in 0..10 {
            ok(
                &mut sys,
                &format!(
                    "CALL sprig.mq_publish('jobs', '@{late}:audit.user', '{{\"n\": {j}}}', '{{}}', 0, 'r9mq')"
                ),
            )
            .await;
        }
        let got = wait_count(&mut lc, "SELECT COUNT(*) FROM app_q.audit", 10, 120).await;
        println!(
            "late member {late}: {got} of 10 trigger runs after {:?}",
            t.elapsed()
        );
        assert_eq!(got, 10);
        let c = amqp(&amqp_addr, &late, "r9mq").await;
        let ch = c.create_channel().await.unwrap();
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .unwrap();
        assert!(publish(&ch, "jobs", "mail.late", "late-own", None).await);
        let got = take_all(&ch, "welcome_mail", 1, 30).await;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1.as_deref(), Some(late.as_str()));
    }
    // A broadcast to every member's instance (x-sprig-tenant: *), routed by
    // each member's own bindings.
    let t = Instant::now();
    assert!(publish(&sch, "jobs", "mail.bcast", "to-all", Some("*")).await);
    for (m, _, ch) in &chans {
        let got = take_all(ch, "welcome_mail", 1, 60).await;
        assert_eq!(got.len(), 1, "{m}: {got:?}");
        assert_eq!(got[0].0, "to-all");
        assert_eq!(got[0].1.as_deref(), Some(m.as_str()));
    }
    println!("AMQP broadcast reached every member in {:?}", t.elapsed());
    // Overrides: a member's queue argument, a member's disabled binding.
    ok(
        &mut ms[2].1,
        "ALTER INHERITED QUEUE r9mq.welcome_mail SET (x_message_ttl = 60000)",
    )
    .await;
    ok(
        &mut ms[2].1,
        "ALTER INHERITED BINDING r9mq.`jobs>queue:welcome_mail:mail.*` DISABLE",
    )
    .await;
    let m2 = chans[2].0.clone();
    assert!(publish(&sch, "jobs", &format!("@{m2}:mail.x"), "after-unbind", None).await);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(take_all(&chans[2].2, "welcome_mail", 0, 1).await.is_empty());
    let shown = ok(&mut ms[2].1, "SHOW INHERITED OVERRIDES").await;
    println!("{m2}'s overrides: {shown:?}");
    assert_eq!(shown.len(), 2);
    for (_, c, _) in chans {
        let _ = c.close(200, "done".into()).await;
    }
    let _ = sc.close(200, "done".into()).await;
    ok(&mut sys, "DROP TRIGGER r9mq.on_audit").await;
    ok(&mut sys, "DROP EXCHANGE r9mq.jobs IMMEDIATE CASCADE").await;
    ok(&mut sys, "DROP QUEUE r9mq.welcome_mail IMMEDIATE").await;
    ok(&mut sys, "DROP QUEUE r9mq.audit_q IMMEDIATE").await;
}

#[tokio::test]
async fn broadcast_end_to_end() {
    let Some(e) = env("broadcast_end_to_end") else {
        return;
    };
    let mut sys = conn(&e.addr, &e.system).await;
    let mut ms = Vec::new();
    for m in &e.members {
        ms.push((m.clone(), conn(&e.addr, m).await));
    }
    let _ = sql(&mut sys, "DROP STREAM security.bc_events IMMEDIATE CASCADE").await;
    let _ = sql(&mut sys, "CREATE DATABASE IF NOT EXISTS security").await;
    let _ = sql(&mut sys, "ALTER TENANT SPRIG RESET (inherit.broadcast_replay, inherit.broadcasts_per_hour, inherit.broadcast_max_bytes)").await;
    ok(
        &mut sys,
        "CREATE STREAM security.bc_events FOR EACH TENANT PARTITIONS 2",
    )
    .await;
    let m0 = ms[0].0.clone();
    // Only the first member has an instance before the broadcasts.
    ok(
        &mut sys,
        &format!(
            "INSERT INTO security.bc_events FOR TENANT {m0} (key, value) VALUES ('bk', 'own')"
        ),
    )
    .await;
    let q = "SELECT COUNT(*) FROM security.bc_events";
    assert_eq!(wait_count(&mut ms[0].1, q, 1, 60).await, 1);
    let t = Instant::now();
    ok(
        &mut sys,
        "INSERT INTO security.bc_events FOR ALL TENANTS WITH INSTANCE (key, value) VALUES ('bk', 'with-instance')",
    )
    .await;
    ok(
        &mut sys,
        "INSERT INTO security.bc_events FOR ALL TENANTS (key, value) VALUES ('bk', 'all')",
    )
    .await;
    // Inside transactions: rolled back reaches nobody, committed everybody.
    ok(&mut sys, "START TRANSACTION").await;
    ok(
        &mut sys,
        "INSERT INTO security.bc_events FOR ALL TENANTS (key, value) VALUES ('bk', 'rolled-back')",
    )
    .await;
    ok(&mut sys, "ROLLBACK").await;
    ok(&mut sys, "START TRANSACTION").await;
    ok(
        &mut sys,
        "INSERT INTO security.bc_events FOR ALL TENANTS (key, value) VALUES ('bk', 'in-txn')",
    )
    .await;
    ok(&mut sys, "COMMIT").await;
    let want = |i: usize| if i == 0 { 4 } else { 2 };
    for (i, (m, c)) in ms.iter_mut().enumerate() {
        let got = wait_count(c, q, want(i), 60).await;
        assert_eq!(got, want(i), "{m}");
    }
    println!(
        "three broadcasts reached {} members in {:?}",
        ms.len(),
        t.elapsed()
    );
    tokio::time::sleep(Duration::from_secs(4)).await;
    for (i, (m, c)) in ms.iter_mut().enumerate() {
        let vals = ok(
            c,
            "SELECT CAST(value AS CHAR), JSON_VALUE(headers, '$.\"x-sprig-tenant\"'), \
             JSON_VALUE(headers, '$.\"x-sprig-origin-tenant\"'), \
             JSON_VALUE(headers, '$.\"x-sprig-broadcast\"') IS NOT NULL \
             FROM security.bc_events ORDER BY partition_no, offset_no",
        )
        .await;
        let order: Vec<String> = vals.iter().map(|r| r[0].clone().unwrap()).collect();
        let expect: Vec<&str> = if i == 0 {
            vec!["own", "with-instance", "all", "in-txn"]
        } else {
            vec!["all", "in-txn"]
        };
        assert_eq!(order, expect, "{m}: exactly once, in broadcast order");
        for r in vals.iter().filter(|r| r[0].as_deref() != Some("own")) {
            assert_eq!(r[1].as_deref(), Some(m.as_str()));
            assert_eq!(r[2].as_deref(), Some(e.system.as_str()));
            assert_eq!(r[3].as_deref(), Some("1"));
        }
    }
    let log = ok(
        &mut sys,
        "SHOW INHERITED BROADCASTS FOR STREAM security.bc_events",
    )
    .await;
    println!("broadcast log: {log:?}");
    assert_eq!(log.len(), 3);
    assert!(log.iter().all(|r| r[5].as_deref() == Some("delivered")));
    // Limits (39244).
    ok(
        &mut sys,
        "ALTER TENANT SPRIG SET (inherit.broadcast_max_bytes = 100)",
    )
    .await;
    let big = "x".repeat(200);
    let r = sql(
        &mut sys,
        &format!(
            "INSERT INTO security.bc_events FOR ALL TENANTS (key, value) VALUES ('bk', '{big}')"
        ),
    )
    .await;
    assert_eq!(r.map(|_| ()).unwrap_err().0, 39244);
    ok(
        &mut sys,
        "ALTER TENANT SPRIG SET (inherit.broadcasts_per_hour = 3)",
    )
    .await;
    let r = sql(
        &mut sys,
        "INSERT INTO security.bc_events FOR ALL TENANTS (key, value) VALUES ('bk', 'fourth')",
    )
    .await;
    assert_eq!(r.map(|_| ()).unwrap_err().0, 39244);
    ok(
        &mut sys,
        "ALTER TENANT SPRIG RESET (inherit.broadcasts_per_hour, inherit.broadcast_max_bytes)",
    )
    .await;
    // Tenants created after the broadcasts: nothing by default; with
    // inherit.broadcast_replay = on, the log when their instance is made.
    if e.create.is_some() {
        let base = e
            .late
            .clone()
            .unwrap_or_else(|| format!("{}late", e.system));
        let late1 = format!("{base}b1");
        create_tenant(&e, &late1);
        let mut l1 = conn(&e.addr, &late1).await;
        ok(
            &mut sys,
            &format!(
                "INSERT INTO security.bc_events FOR TENANT {late1} (key, value) VALUES ('bk', 'late-own')"
            ),
        )
        .await;
        assert_eq!(wait_count(&mut l1, q, 1, 60).await, 1);
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(
            count(&mut l1, q).await,
            1,
            "{late1}: no broadcast without replay"
        );
        ok(
            &mut sys,
            "ALTER TENANT SPRIG SET (inherit.broadcast_replay = 'on')",
        )
        .await;
        let late2 = format!("{base}b2");
        create_tenant(&e, &late2);
        let mut l2 = conn(&e.addr, &late2).await;
        let t = Instant::now();
        ok(
            &mut sys,
            &format!(
                "INSERT INTO security.bc_events FOR TENANT {late2} (key, value) VALUES ('bk', 'late-own')"
            ),
        )
        .await;
        let got = wait_count(&mut l2, q, 4, 60).await;
        println!(
            "{late2}: {got} records (its own and 3 replayed) after {:?}",
            t.elapsed()
        );
        assert_eq!(got, 4);
        ok(
            &mut sys,
            "ALTER TENANT SPRIG RESET (inherit.broadcast_replay)",
        )
        .await;
    }
    // Fan-in from SQL: every member's instance as one table, tagged with
    // its tenant. SPRIG_FAN_IN is granted by name only.
    let _ = sql(&mut sys, "REVOKE SPRIG_FAN_IN ON *.* FROM app").await;
    let r = sql(&mut sys, "SELECT COUNT(*) FROM security.bc_events FAN IN").await;
    assert_eq!(
        r.map(|_| ()).unwrap_err().0,
        1227,
        "fan-in without SPRIG_FAN_IN"
    );
    ok(&mut sys, "GRANT SPRIG_FAN_IN ON *.* TO app").await;
    let t = Instant::now();
    let per = ok(
        &mut sys,
        "SELECT tenant, COUNT(*) FROM security.bc_events FAN IN GROUP BY tenant ORDER BY tenant",
    )
    .await;
    println!("fan-in read in {:?}: {per:?}", t.elapsed());
    for (i, (m, _)) in ms.iter().enumerate() {
        let row = per
            .iter()
            .find(|r| r[0].as_deref() == Some(m.as_str()))
            .unwrap_or_else(|| panic!("{m} missing from {per:?}"));
        assert_eq!(row[1].as_deref(), Some(if i == 0 { "4" } else { "2" }));
    }
    let only = ok(
        &mut sys,
        &format!(
            "SELECT tenant, CAST(value AS CHAR) FROM security.bc_events FAN IN WHERE tenant IN ('{m0}') ORDER BY offset_no"
        ),
    )
    .await;
    assert_eq!(only.len(), 4, "{only:?}");
    assert!(only.iter().all(|r| r[0].as_deref() == Some(m0.as_str())));
    ok(
        &mut sys,
        "ALTER TENANT SPRIG SET (inherit.fan_in_sql_max_tenants = 1)",
    )
    .await;
    let r = sql(&mut sys, "SELECT COUNT(*) FROM security.bc_events FAN IN").await;
    assert_eq!(r.map(|_| ()).unwrap_err().0, 1104);
    ok(
        &mut sys,
        "ALTER TENANT SPRIG RESET (inherit.fan_in_sql_max_tenants)",
    )
    .await;
    // A member can't fan in its siblings (it has no descendants).
    let none = sql(
        &mut ms[1].1,
        "SELECT COUNT(*) FROM security.bc_events FAN IN",
    )
    .await;
    println!("a member's own fan-in: {none:?}");
    ok(&mut sys, "REVOKE SPRIG_FAN_IN ON *.* FROM app").await;
    ok(&mut sys, "DROP STREAM security.bc_events IMMEDIATE").await;
}

#[tokio::test]
async fn detach_and_the_end_of_the_drop_grace() {
    let Some(e) = env("detach_and_the_end_of_the_drop_grace") else {
        return;
    };
    let mut sys = conn(&e.addr, &e.system).await;
    let (m0, m1) = (e.members[0].clone(), e.members[1].clone());
    let mut c0 = conn(&e.addr, &m0).await;
    let mut c1 = conn(&e.addr, &m1).await;
    let _ = sql(&mut sys, "DROP STREAM dt.dt_events IMMEDIATE").await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS dt").await;
    for c in [&mut c0, &mut c1] {
        let _ = sql(c, "DROP DATABASE IF EXISTS dt").await;
    }
    ok(&mut sys, "CREATE DATABASE dt").await;
    ok(
        &mut sys,
        "CREATE STREAM dt.dt_events FOR EACH TENANT PARTITIONS 1",
    )
    .await;
    for m in [&m0, &m1] {
        ok(
            &mut sys,
            &format!(
                "INSERT INTO dt.dt_events FOR TENANT {m} (key, value) VALUES ('k', 'from-sys')"
            ),
        )
        .await;
    }
    let q = "SELECT COUNT(*) FROM dt.dt_events";
    assert_eq!(wait_count(&mut c0, q, 1, 60).await, 1);
    assert_eq!(wait_count(&mut c1, q, 1, 60).await, 1);
    // DROP ... IF EXISTS of a definition that isn't there: a note.
    ok(&mut sys, "DROP EXCHANGE IF EXISTS dt.nosuch").await;
    ok(&mut sys, "DROP QUEUE IF EXISTS dt.nosuch").await;
    // Only the member detaches, and only its own instance.
    let r = sql(
        &mut sys,
        &format!("ALTER INHERITED STREAM dt.dt_events FOR TENANT {m0} DETACH"),
    )
    .await;
    assert_eq!(r.map(|_| ()).unwrap_err().0, 39897);
    let notes = ok(&mut c0, "ALTER INHERITED STREAM dt.dt_events DETACH").await;
    println!("detach: {notes:?}");
    // The stream is the member's own now: its publishes take the ordinary
    // path, and its data stays.
    ok(
        &mut c0,
        "INSERT INTO dt.dt_events (key, value) VALUES ('k', 'own')",
    )
    .await;
    assert_eq!(wait_count(&mut c0, q, 2, 60).await, 2);
    // A grace of 0 hours: the next pass (within 30 s) removes the other
    // instances and the definition; the detached one stays.
    ok(
        &mut sys,
        "ALTER TENANT SPRIG SET (inherit.drop_grace_hours = 0)",
    )
    .await;
    ok(&mut sys, "DROP STREAM dt.dt_events").await;
    let r = sql(
        &mut sys,
        &format!("INSERT INTO dt.dt_events FOR TENANT {m1} (key, value) VALUES ('k', 'late')"),
    )
    .await;
    assert_eq!(r.map(|_| ()).unwrap_err().0, 39898, "a dropping definition");
    let t = Instant::now();
    let gone = loop {
        let defs = ok(&mut sys, "SHOW INHERITED DEFINITIONS").await;
        if !defs.iter().any(|r| r[2].as_deref() == Some("dt_events")) {
            break true;
        }
        if t.elapsed() > Duration::from_secs(120) {
            break false;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    println!("grace end removed the definition after {:?}", t.elapsed());
    assert!(gone, "the definition is still there");
    let r = sql(&mut c1, q).await;
    println!("{m1} after the grace: {r:?}");
    assert!(r.is_err() || r.as_ref().unwrap()[0][0].as_deref() == Some("0"));
    assert_eq!(
        count(&mut c0, q).await,
        2,
        "the detached stream keeps its data"
    );
    ok(
        &mut sys,
        "ALTER TENANT SPRIG RESET (inherit.drop_grace_hours)",
    )
    .await;
    ok(&mut c0, "DROP STREAM dt.dt_events").await;
    for c in [&mut c0, &mut c1] {
        let _ = sql(c, "DROP DATABASE IF EXISTS dt").await;
    }
    ok(&mut sys, "DROP DATABASE dt").await;
}
