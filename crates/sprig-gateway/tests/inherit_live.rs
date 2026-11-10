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
