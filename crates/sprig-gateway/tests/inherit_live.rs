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
//! The declaring tenant needs short retries and the hook port allowed, or
//! the dead-letter steps outlast their waits (with the defaults a trigger
//! or sink record takes 10 attempts with backoff up to a minute):
//!
//! ```sh
//! sprigctl settings set logic.trigger_max_attempts 3 --scope tenant:$SYSTEM ...
//! sprigctl settings set logic.trigger_retry_initial_ms 100 --scope tenant:$SYSTEM ...
//! sprigctl settings set sink.record_max_attempts 3 --scope tenant:$SYSTEM ...
//! sprigctl settings set sink.retry_initial_ms 100 --scope tenant:$SYSTEM ...
//! sprigctl settings set sink.destination_allowlist '["127.0.0.1:<SPRIG_TEST_INHERIT_HOOK_PORT>"]' --scope tenant:$SYSTEM ...
//! ```
//!
//! With `SPRIG_TEST_INHERIT_LATE=x`, each test creates its own late member
//! (`x`, `xa`, `xs`, `xq`, `xb1`, `xb2`, `xfi`), so use a new `x` per run.
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
    let defs: Vec<_> = ok(&mut sys, "SHOW INHERITED DEFINITIONS")
        .await
        .into_iter()
        .filter(|r| r[1].as_deref() == Some("security"))
        .collect();
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
    // R9 next: the outbox is empty, but the definition lives in obx (39895,
    // naming it) until it is dropped.
    let (code, msg) = sql(&mut sys, "DROP DATABASE obx").await.unwrap_err();
    assert_eq!(code, 39895, "{msg}");
    assert!(msg.contains("stream obx.drop_events"), "{msg}");
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
    let t = Instant::now();
    ok(&mut sys, "DROP DATABASE obx").await;
    println!("DROP DATABASE after the outbox emptied: {:?}", t.elapsed());
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

/// R9 next (r9next): a member whose broadcast delivery fails gets it later,
/// once and in order, from the saved retry list; and `DROP DATABASE` of a
/// declarer's database is refused while live definitions are in it.
#[tokio::test]
async fn broadcast_retry_and_the_drop_database_guard() {
    let Some(e) = env("broadcast_retry_and_the_drop_database_guard") else {
        return;
    };
    let Some(root) = var("SPRIG_TEST_INHERIT_ROOT") else {
        eprintln!("no SPRIG_TEST_INHERIT_ROOT: skipped");
        return;
    };
    let mut sys = conn(&e.addr, &e.system).await;
    let mut ms = Vec::new();
    for m in &e.members {
        ms.push((m.clone(), conn(&e.addr, m).await));
    }
    // The guard: a database holding live definitions isn't dropped.
    let _ = sql(&mut sys, "DROP STREAM r9nguard.g_events IMMEDIATE CASCADE").await;
    let _ = sql(&mut sys, "DROP QUEUE r9nguard.g_jobs IMMEDIATE CASCADE").await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS r9nguard").await;
    ok(&mut sys, "CREATE DATABASE r9nguard").await;
    ok(
        &mut sys,
        "CREATE STREAM r9nguard.g_events FOR EACH TENANT PARTITIONS 1",
    )
    .await;
    ok(&mut sys, "CREATE QUEUE r9nguard.g_jobs FOR EACH TENANT").await;
    let (code, msg) = sql(&mut sys, "DROP DATABASE r9nguard").await.unwrap_err();
    println!("DROP DATABASE with live definitions: {code} {msg}");
    assert_eq!(code, 39895, "{msg}");
    assert!(
        msg.contains("stream r9nguard.g_events") && msg.contains("queue r9nguard.g_jobs"),
        "{msg}"
    );
    // Dropped (here the stream into its grace, the queue at once): the
    // database goes.
    ok(&mut sys, "DROP STREAM r9nguard.g_events").await;
    let (code, msg) = sql(&mut sys, "DROP DATABASE r9nguard").await.unwrap_err();
    assert_eq!(code, 39895, "{msg}");
    assert!(!msg.contains("g_events") && msg.contains("g_jobs"), "{msg}");
    ok(&mut sys, "DROP QUEUE r9nguard.g_jobs IMMEDIATE").await;
    let t = Instant::now();
    ok(&mut sys, "DROP DATABASE r9nguard").await;
    println!(
        "DROP DATABASE after the definitions were dropped: {:?}",
        t.elapsed()
    );
    let _ = sql(&mut sys, "DROP STREAM r9nguard.g_events IMMEDIATE CASCADE").await;

    // Broadcast retry.
    let _ = sql(&mut sys, "DROP STREAM security.rt_events IMMEDIATE CASCADE").await;
    let _ = sql(&mut sys, "CREATE DATABASE IF NOT EXISTS security").await;
    ok(
        &mut sys,
        "CREATE STREAM security.rt_events FOR EACH TENANT PARTITIONS 1",
    )
    .await;
    let m1 = ms[1].0.clone();
    ok(
        &mut sys,
        &format!(
            "INSERT INTO security.rt_events FOR TENANT {m1} (key, value) VALUES ('k', 'first')"
        ),
    )
    .await;
    let q = "SELECT COUNT(*) FROM security.rt_events";
    assert_eq!(wait_count(&mut ms[1].1, q, 1, 60).await, 1);
    // m1's instance: the only one so far.
    let mut r = Conn::from_url(&root).await.unwrap();
    let cats: Vec<(String,)> = r
        .query(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = \
             'inherited_defs'",
        )
        .await
        .unwrap();
    let (mut sys_id, mut def_obj) = (0u64, 0u64);
    for (c,) in cats {
        let o: Option<u64> = r
            .query_first(format!(
                "SELECT object_id FROM `{c}`.inherited_defs WHERE name = 'rt_events' AND \
                 db_name = 'security' AND state = 'active'"
            ))
            .await
            .unwrap();
        if let Some(o) = o {
            sys_id = c[1..c.find('_').unwrap()].parse().unwrap();
            def_obj = o;
        }
    }
    assert!(sys_id > 0);
    let rows: Vec<(String,)> = r
        .query(format!(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = \
             'inherited_instances' AND table_schema <> 't{sys_id}_sprig'"
        ))
        .await
        .unwrap();
    let (mut m1_id, mut iid) = (0u64, 0u64);
    for (s,) in rows {
        let i: Option<u64> = r
            .query_first(format!(
                "SELECT instance_id FROM `{s}`.inherited_instances WHERE def_object_id = \
                 {def_obj} AND def_tenant_id = {sys_id}"
            ))
            .await
            .unwrap();
        if let Some(i) = i {
            m1_id = s[1..s.find('_').unwrap()].parse().unwrap();
            iid = i;
        }
    }
    assert!(m1_id > 0 && iid > 0);
    let (schema,): (String,) = r
        .query_first(format!(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = \
             '__sprig_eo_{iid}' AND table_schema LIKE 't{m1_id}\\_d%'"
        ))
        .await
        .unwrap()
        .unwrap();
    // m1's outbox goes away for a while: its deliveries fail.
    r.query_drop(format!(
        "RENAME TABLE `{schema}`.`__sprig_eo_{iid}` TO `{schema}`.`__sprig_eo_{iid}_held`"
    ))
    .await
    .unwrap();
    let t = Instant::now();
    for v in ["b1", "b2", "b3"] {
        ok(
            &mut sys,
            &format!(
                "INSERT INTO security.rt_events FOR ALL TENANTS (key, value) VALUES ('k', '{v}')"
            ),
        )
        .await;
    }
    for (i, (m, c)) in ms.iter_mut().enumerate() {
        if i == 1 {
            continue;
        }
        assert_eq!(wait_count(c, q, 3, 60).await, 3, "{m}");
    }
    println!(
        "the other members had the three broadcasts in {:?}",
        t.elapsed()
    );
    // Pending for m1, in the log.
    let end = Instant::now() + Duration::from_secs(30);
    let log = loop {
        let log = ok(
            &mut sys,
            "SHOW INHERITED BROADCASTS FOR STREAM security.rt_events",
        )
        .await;
        if log.iter().all(|r| r[5].as_deref() == Some("delivered")) || Instant::now() > end {
            break log;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    println!("broadcast log while m1 fails: {log:?}");
    assert_eq!(log.len(), 3);
    let before: Vec<u64> = log
        .iter()
        .map(|r| r[6].as_deref().unwrap_or("0").parse().unwrap())
        .collect();
    for row in &log {
        assert_eq!(row[5].as_deref(), Some("delivered"));
        assert_eq!(row[9].as_deref(), Some("1"), "one member pending: {row:?}");
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    // m1's outbox back: the retries deliver the three, in order, once.
    r.query_drop(format!(
        "RENAME TABLE `{schema}`.`__sprig_eo_{iid}_held` TO `{schema}`.`__sprig_eo_{iid}`"
    ))
    .await
    .unwrap();
    let t = Instant::now();
    assert_eq!(wait_count(&mut ms[1].1, q, 4, 120).await, 4);
    println!("m1 caught up {:?} after its outbox came back", t.elapsed());
    tokio::time::sleep(Duration::from_secs(3)).await;
    for (i, (m, c)) in ms.iter_mut().enumerate() {
        let vals: Vec<String> = ok(
            c,
            "SELECT CAST(value AS CHAR) FROM security.rt_events ORDER BY partition_no, offset_no",
        )
        .await
        .into_iter()
        .map(|r| r[0].clone().unwrap())
        .collect();
        let want: Vec<&str> = if i == 1 {
            vec!["first", "b1", "b2", "b3"]
        } else {
            vec!["b1", "b2", "b3"]
        };
        assert_eq!(vals, want, "{m}: exactly once, in broadcast order");
    }
    let log = ok(
        &mut sys,
        "SHOW INHERITED BROADCASTS FOR STREAM security.rt_events",
    )
    .await;
    println!("broadcast log after the retries: {log:?}");
    for (row, b) in log.iter().zip(&before) {
        assert_eq!(row[9].as_deref(), Some("0"), "{row:?}");
        // The retried member counted as delivered (other members of the
        // system tenant, made by earlier tests, count too).
        assert_eq!(
            row[6].as_deref(),
            Some((b + 1).to_string().as_str()),
            "{row:?}"
        );
    }
    ok(&mut sys, "DROP STREAM security.rt_events IMMEDIATE CASCADE").await;
}

const FAN_IN_AUDIT: &str = r"
CREATE TRIGGER r9naudit.fi_audit ON STREAM security.fi_events FAN IN
  LANGUAGE javascript AS $$
export default function (ctx) {
  db.exec('INSERT INTO r9naudit.seen (tenant, p, o, v) VALUES (?, ?, ?, ?)',
          [ctx.record.headers['x-sprig-tenant'] || '', ctx.record.partition,
           ctx.record.offset, ctx.record.value]);
}
$$";

async fn publish_fi(c: &mut Conn, tenant: &str, from: u64, to: u64) {
    let rows: Vec<String> = (from..to).map(|i| format!("('{i}', 'v{i}')")).collect();
    for chunk in rows.chunks(50) {
        ok(
            c,
            &format!(
                "INSERT INTO security.fi_events FOR TENANT {tenant} (key, value) VALUES {}",
                chunk.join(", ")
            ),
        )
        .await;
    }
}

/// R9 next (r9next): a `FAN IN` trigger in the declaring tenant runs once
/// per record of every member's instance (those made after it from their
/// start, those before it from then on), with its own positions per
/// instance, through a `kill -9` of the worker; a member created later
/// joins.
#[tokio::test]
async fn fan_in_trigger_end_to_end() {
    let Some(e) = env("fan_in_trigger_end_to_end") else {
        return;
    };
    let mut sys = conn(&e.addr, &e.system).await;
    let ms = e.members.clone();
    let _ = sql(&mut sys, "DROP TRIGGER r9naudit.fi_audit").await;
    let _ = sql(&mut sys, "DROP STREAM security.fi_events IMMEDIATE CASCADE").await;
    let _ = sql(&mut sys, "CREATE DATABASE IF NOT EXISTS security").await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS r9naudit").await;
    ok(&mut sys, "CREATE DATABASE r9naudit").await;
    ok(
        &mut sys,
        "CREATE TABLE r9naudit.seen (id BIGINT AUTO_INCREMENT PRIMARY KEY, tenant VARCHAR(64), \
         p INT, o BIGINT, v VARCHAR(64))",
    )
    .await;
    ok(
        &mut sys,
        "CREATE STREAM security.fi_events FOR EACH TENANT PARTITIONS 2",
    )
    .await;
    // Before the trigger: m0's instance and 5 records it never sees (in
    // m0's log before the trigger: its start is the log's high watermark).
    publish_fi(&mut sys, &ms[0], 0, 5).await;
    let mut c0 = conn(&e.addr, &ms[0]).await;
    assert_eq!(
        wait_count(&mut c0, "SELECT COUNT(*) FROM security.fi_events", 5, 60).await,
        5
    );
    // SPRIG_FAN_IN is needed.
    let _ = sql(&mut sys, "REVOKE SPRIG_FAN_IN ON *.* FROM app").await;
    let (code, msg) = sql(&mut sys, FAN_IN_AUDIT).await.unwrap_err();
    println!("FAN IN trigger without SPRIG_FAN_IN: {code} {msg}");
    assert_eq!(code, 1227, "{msg}");
    ok(&mut sys, "GRANT SPRIG_FAN_IN ON *.* TO app").await;
    let notes = ok(&mut sys, FAN_IN_AUDIT).await;
    println!("FAN IN trigger: {notes:?}");
    let t = Instant::now();
    for m in &ms {
        publish_fi(&mut sys, m, 100, 140).await;
    }
    let q = "SELECT COUNT(*) FROM r9naudit.seen";
    let half = wait_count(&mut sys, q, 60, 120).await;
    // The worker dies mid-stream.
    let killed = kill_worker(&e);
    println!("{half} runs before the kill -9 (killed: {killed})");
    for m in &ms {
        publish_fi(&mut sys, m, 200, 220).await;
    }
    if killed {
        start_worker(&e);
    }
    let want = (ms.len() * 60) as u64;
    let got = wait_count(&mut sys, q, want, 180).await;
    println!(
        "fan-in trigger: {got} of {want} runs, {:?} after the first publish",
        t.elapsed()
    );
    tokio::time::sleep(Duration::from_secs(5)).await;
    let twice = count(
        &mut sys,
        "SELECT COUNT(*) FROM (SELECT tenant, v FROM r9naudit.seen GROUP BY tenant, v HAVING \
         COUNT(*) > 1) d",
    )
    .await;
    let per = ok(
        &mut sys,
        "SELECT tenant, COUNT(*) FROM r9naudit.seen GROUP BY tenant ORDER BY tenant",
    )
    .await;
    println!("per member: {per:?}, applied twice: {twice}");
    assert_eq!(twice, 0, "exactly once");
    assert_eq!(per.len(), ms.len());
    for r in &per {
        assert_eq!(r[1].as_deref(), Some("60"), "{r:?}");
    }
    let early = count(
        &mut sys,
        "SELECT COUNT(*) FROM r9naudit.seen WHERE v IN ('v0','v1','v2','v3','v4')",
    )
    .await;
    assert_eq!(early, 0, "records before the trigger aren't run");
    // A member created later joins.
    if e.create.is_some() {
        let late = format!(
            "{}fi",
            e.late
                .clone()
                .unwrap_or_else(|| format!("{}late", e.system))
        );
        create_tenant(&e, &late);
        let t = Instant::now();
        publish_fi(&mut sys, &late, 0, 10).await;
        let n = wait_count(
            &mut sys,
            &format!("SELECT COUNT(*) FROM r9naudit.seen WHERE tenant = '{late}'"),
            10,
            90,
        )
        .await;
        println!(
            "{late} (new): {n} of 10 runs, {:?} after its first publish",
            t.elapsed()
        );
        assert_eq!(n, 10);
    }
    let _ = sql(&mut sys, "DROP TRIGGER r9naudit.fi_audit").await;
    ok(&mut sys, "DROP STREAM security.fi_events IMMEDIATE").await;
    ok(&mut sys, "REVOKE SPRIG_FAN_IN ON *.* FROM app").await;
    ok(&mut sys, "DROP DATABASE r9naudit").await;
}

/// The tenant id of `name` from the file `SPRIG_TEST_INHERIT_IDS` (lines
/// `name id`).
fn tenant_id_of(name: &str) -> Option<u64> {
    let f = var("SPRIG_TEST_INHERIT_IDS")?;
    std::fs::read_to_string(f)
        .ok()?
        .lines()
        .filter_map(|l| l.split_once(' '))
        .filter(|(n, _)| *n == name)
        .filter_map(|(_, id)| id.trim().parse().ok())
        .next_back()
}

const DL_TRIGGER: &str = r"
CREATE TRIGGER r9ndl.on_ev ON STREAM r9ndl.ev LANGUAGE javascript AS $$
export default function (ctx) {
  const v = String(ctx.record.value);
  const t = v.startsWith('bad') ? 'r9ndl.later' : 'r9ndl.applied';
  db.exec('INSERT INTO ' + t + ' (v) VALUES (?)', [v]);
}
$$";

/// RC11 (r9next): dead letters of a trigger on a stream are replayed with
/// `sprigctl logic dead-letters replay`, in order, once each (a second
/// replay of the same ones runs nothing), outside the trigger's position.
#[tokio::test]
async fn stream_trigger_dead_letter_replay() {
    let Some(e) = env("stream_trigger_dead_letter_replay") else {
        return;
    };
    let (Some(ctl), Some(sys_id)) = (var("SPRIG_TEST_SPRIGCTL"), tenant_id_of(&e.system)) else {
        eprintln!("no SPRIG_TEST_SPRIGCTL or SPRIG_TEST_INHERIT_IDS: skipped");
        return;
    };
    let root = var("SPRIG_TEST_INHERIT_ROOT").expect("SPRIG_TEST_INHERIT_ROOT");
    let mut sys = conn(&e.addr, &e.system).await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS r9ndl").await;
    ok(&mut sys, "CREATE DATABASE r9ndl").await;
    ok(
        &mut sys,
        "CREATE TABLE r9ndl.applied (id BIGINT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(32))",
    )
    .await;
    ok(&mut sys, "CREATE STREAM r9ndl.ev PARTITIONS 1").await;
    ok(&mut sys, DL_TRIGGER).await;
    let vals = ["g1", "bad1", "g2", "bad2", "g3", "bad3", "g4"];
    for v in vals {
        ok(
            &mut sys,
            &format!("INSERT INTO r9ndl.ev (key, value) VALUES ('k', '{v}')"),
        )
        .await;
    }
    // The good ones run; the bad ones dead-letter after their attempts.
    assert_eq!(
        wait_count(&mut sys, "SELECT COUNT(*) FROM r9ndl.applied", 4, 120).await,
        4
    );
    let cat = format!("{root}/t{sys_id}_sprig");
    let mut r = Conn::from_url(&cat).await.unwrap();
    let end = Instant::now() + Duration::from_secs(120);
    let binding: u64 = loop {
        let b: Option<(u64, u64)> = r
            .query_first(
                "SELECT binding_id, COUNT(*) FROM logic_dead_letters WHERE state = 'dead' \
                 GROUP BY binding_id ORDER BY binding_id DESC LIMIT 1",
            )
            .await
            .unwrap();
        if let Some((b, 3)) = b {
            break b;
        }
        assert!(Instant::now() < end, "three dead letters: {b:?}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    // The cause fixed: the table exists now. Replay.
    ok(
        &mut sys,
        "CREATE TABLE r9ndl.later (id BIGINT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(32))",
    )
    .await;
    let replay = || {
        let out = std::process::Command::new(&ctl)
            .args([
                "logic",
                "dead-letters",
                "replay",
                "--all",
                "--binding",
                &binding.to_string(),
                "--catalog",
                &cat,
            ])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    };
    let t = Instant::now();
    println!("sprigctl: {}", replay().trim());
    assert_eq!(
        wait_count(&mut sys, "SELECT COUNT(*) FROM r9ndl.later", 3, 60).await,
        3
    );
    println!("three dead letters replayed in {:?}", t.elapsed());
    let order: Vec<String> = ok(&mut sys, "SELECT v FROM r9ndl.later ORDER BY id")
        .await
        .into_iter()
        .map(|r| r[0].clone().unwrap())
        .collect();
    assert_eq!(order, ["bad1", "bad2", "bad3"], "in order");
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let n: Option<u64> = r
            .exec_first(
                "SELECT COUNT(*) FROM logic_dead_letters WHERE binding_id = ? AND state = \
                 'replayed'",
                (binding,),
            )
            .await
            .unwrap();
        if n == Some(3) || Instant::now() > end {
            assert_eq!(n, Some(3));
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    // Marked for replay again (by hand): each finds its marker moved and
    // runs nothing.
    r.exec_drop(
        "UPDATE logic_dead_letters SET state = 'replaying' WHERE binding_id = ?",
        (binding,),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert_eq!(count(&mut sys, "SELECT COUNT(*) FROM r9ndl.later").await, 3);
    // The trigger's own position moved on meanwhile: a new record runs.
    ok(
        &mut sys,
        "INSERT INTO r9ndl.ev (key, value) VALUES ('k', 'g5')",
    )
    .await;
    assert_eq!(
        wait_count(&mut sys, "SELECT COUNT(*) FROM r9ndl.applied", 5, 60).await,
        5
    );
    assert_eq!(count(&mut sys, "SELECT COUNT(*) FROM r9ndl.later").await, 3);
    ok(&mut sys, "DROP DATABASE r9ndl").await;
}

// ---------------------------------------------------------------------------
// R9 final (r9final): AMQP fan-in consumers and the ready index.
// ---------------------------------------------------------------------------

fn delivery_header(d: &lapin::message::Delivery, k: &str) -> Option<String> {
    match d.properties.headers().as_ref()?.inner().get(k)? {
        AMQPValue::LongString(s) => Some(s.to_string()),
        AMQPValue::ShortString(s) => Some(s.to_string()),
        _ => None,
    }
}

/// Reads `want` deliveries of `consumer` (acknowledging each), at most
/// `secs`: (x-sprig-tenant, body).
async fn read_fan_in(
    consumer: &mut lapin::Consumer,
    want: usize,
    secs: u64,
) -> Vec<(String, String)> {
    use futures::StreamExt;
    let mut out = Vec::new();
    let end = Instant::now() + Duration::from_secs(secs);
    while out.len() < want {
        let left = end.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        let Ok(Some(Ok(d))) = tokio::time::timeout(left, consumer.next()).await else {
            break;
        };
        let t = delivery_header(&d, "x-sprig-tenant").unwrap_or_default();
        out.push((t, String::from_utf8_lossy(&d.data).into_owned()));
        d.ack(lapin::options::BasicAckOptions::default())
            .await
            .unwrap();
    }
    out
}

/// Publishes `n` messages for member `m` (`@m:work` by the default
/// exchange, bodies `<m>-<i>`).
async fn publish_work(ch: &lapin::Channel, m: &str, from: usize, to: usize) {
    for i in from..to {
        assert!(
            publish(ch, "", &format!("@{m}:work"), &format!("{m}-{i}"), None).await,
            "publish {m}-{i}"
        );
    }
}

fn fan_in_args() -> FieldTable {
    let mut a = FieldTable::default();
    a.insert("x-sprig-fan-in".into(), AMQPValue::Boolean(true));
    a
}

#[tokio::test]
async fn amqp_fan_in_consumer() {
    use lapin::options::{BasicConsumeOptions, BasicQosOptions};
    let Some(e) = env("amqp_fan_in_consumer") else {
        return;
    };
    let Some(amqp_addr) = var("SPRIG_TEST_INHERIT_AMQP") else {
        eprintln!("no SPRIG_TEST_INHERIT_AMQP: skipped");
        return;
    };
    let mut sys = conn(&e.addr, &e.system).await;
    let ms = e.members.clone();
    let _ = sql(&mut sys, "DROP QUEUE r9fq.work IMMEDIATE CASCADE").await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS r9fq").await;
    ok(&mut sys, "CREATE DATABASE r9fq").await;
    ok(&mut sys, "CREATE QUEUE r9fq.work FOR EACH TENANT").await;
    let sc = amqp(&amqp_addr, &e.system, "r9fq").await;
    let sch = sc.create_channel().await.unwrap();
    sch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    // 20 messages wait in each member's instance before any fan-in
    // consumer exists (the attach scan finds them).
    let t = Instant::now();
    for m in &ms {
        publish_work(&sch, m, 0, 20).await;
    }
    println!(
        "60 targeted publishes (3 instances made): {:?}",
        t.elapsed()
    );
    // Consuming the definition's name without the argument: 406.
    {
        let c = amqp(&amqp_addr, &e.system, "r9fq").await;
        let ch = c.create_channel().await.unwrap();
        let r = ch
            .basic_consume(
                "work".into(),
                "plain".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await;
        println!("consume without x-sprig-fan-in: {r:?}");
        assert!(format!("{r:?}").contains("PRECONDITION"), "{r:?}");
    }
    // Without SPRIG_FAN_IN: 403; exclusive or x-priority: 406.
    let _ = sql(&mut sys, "REVOKE SPRIG_FAN_IN ON *.* FROM app").await;
    {
        let c = amqp(&amqp_addr, &e.system, "r9fq").await;
        let ch = c.create_channel().await.unwrap();
        let r = ch
            .basic_consume(
                "work".into(),
                "nogrant".into(),
                BasicConsumeOptions::default(),
                fan_in_args(),
            )
            .await;
        println!("fan-in without SPRIG_FAN_IN: {r:?}");
        assert!(format!("{r:?}").contains("ACCESS"), "{r:?}");
    }
    ok(&mut sys, "GRANT SPRIG_FAN_IN ON *.* TO app").await;
    {
        let c = amqp(&amqp_addr, &e.system, "r9fq").await;
        let ch = c.create_channel().await.unwrap();
        let r = ch
            .basic_consume(
                "work".into(),
                "excl".into(),
                BasicConsumeOptions {
                    exclusive: true,
                    ..BasicConsumeOptions::default()
                },
                fan_in_args(),
            )
            .await;
        assert!(format!("{r:?}").contains("PRECONDITION"), "{r:?}");
        let ch = c.create_channel().await.unwrap();
        let mut a = fan_in_args();
        a.insert("x-priority".into(), AMQPValue::LongInt(5));
        let r = ch
            .basic_consume(
                "work".into(),
                "prio".into(),
                BasicConsumeOptions::default(),
                a,
            )
            .await;
        assert!(format!("{r:?}").contains("PRECONDITION"), "{r:?}");
    }
    // The fan-in consumer, prefetch 10.
    let fc = amqp(&amqp_addr, &e.system, "r9fq").await;
    let fch = fc.create_channel().await.unwrap();
    fch.basic_qos(10, BasicQosOptions::default()).await.unwrap();
    let t = Instant::now();
    let mut consumer = fch
        .basic_consume(
            "work".into(),
            "audit".into(),
            BasicConsumeOptions::default(),
            fan_in_args(),
        )
        .await
        .unwrap();
    let got = read_fan_in(&mut consumer, 60, 60).await;
    println!("fan-in read the waiting 60 in {:?}", t.elapsed());
    let check = |got: &[(String, String)], m: &str, from: usize, to: usize| {
        let mine: Vec<&String> = got.iter().filter(|(t, _)| t == m).map(|(_, b)| b).collect();
        let want: Vec<String> = (from..to).map(|i| format!("{m}-{i}")).collect();
        assert_eq!(
            mine.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            want.iter().map(String::as_str).collect::<Vec<_>>(),
            "{m}: in order, once each"
        );
    };
    assert_eq!(got.len(), 60, "{got:?}");
    for m in &ms {
        check(&got, m, 0, 20);
    }
    // While attached: 10 more each, through the ready index.
    let t = Instant::now();
    for m in &ms {
        publish_work(&sch, m, 20, 30).await;
    }
    let got = read_fan_in(&mut consumer, 30, 60).await;
    println!("30 published while attached, read in {:?}", t.elapsed());
    assert_eq!(got.len(), 30, "{got:?}");
    for m in &ms {
        check(&got, m, 20, 30);
    }
    // A member's own publish (no target) is tagged with its name.
    {
        let c = amqp(&amqp_addr, &ms[0], "r9fq").await;
        let ch = c.create_channel().await.unwrap();
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .unwrap();
        assert!(publish(&ch, "", "work", &format!("{}-own", ms[0]), None).await);
        let got = read_fan_in(&mut consumer, 1, 30).await;
        assert_eq!(got, vec![(ms[0].clone(), format!("{}-own", ms[0]))]);
    }
    // A member created now: its first message reaches the consumer. Each
    // test makes its own late member (`<late>a` here), so the whole file
    // runs with one SPRIG_TEST_INHERIT_LATE.
    if let Some(late) = e.late.as_ref().map(|l| format!("{l}a")) {
        create_tenant(&e, &late);
        let t = Instant::now();
        publish_work(&sch, &late, 0, 10).await;
        let got = read_fan_in(&mut consumer, 10, 60).await;
        println!(
            "late member {late}: 10 read {:?} after its first publish",
            t.elapsed()
        );
        assert_eq!(got.len(), 10, "{got:?}");
        check(&got, &late, 0, 10);
    }
    // Nothing more, and nothing left in the instances (all acknowledged).
    let extra = read_fan_in(&mut consumer, 1, 3).await;
    assert!(extra.is_empty(), "{extra:?}");
    drop(consumer);
    fc.close(0, "done".into()).await.unwrap();
    for m in &ms {
        let c = amqp(&amqp_addr, m, "r9fq").await;
        let ch = c.create_channel().await.unwrap();
        assert!(
            take_all(&ch, "work", 0, 1).await.is_empty(),
            "{m}: left over"
        );
    }
    // Unacknowledged fan-in deliveries go back to their instances when
    // the reader's connection ends.
    for m in &ms {
        publish_work(&sch, m, 30, 32).await;
    }
    {
        use futures::StreamExt;
        let c = amqp(&amqp_addr, &e.system, "r9fq").await;
        let ch = c.create_channel().await.unwrap();
        ch.basic_qos(100, BasicQosOptions::default()).await.unwrap();
        let mut cons = ch
            .basic_consume(
                "work".into(),
                "noack-then-close".into(),
                BasicConsumeOptions::default(),
                fan_in_args(),
            )
            .await
            .unwrap();
        let mut n = 0;
        while n < 6 {
            let d = tokio::time::timeout(Duration::from_secs(30), cons.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(!d.redelivered);
            n += 1;
        }
        c.close(0, "unacked".into()).await.unwrap();
    }
    for m in &ms {
        let c = amqp(&amqp_addr, m, "r9fq").await;
        let ch = c.create_channel().await.unwrap();
        let back = take_all(&ch, "work", 2, 10).await;
        assert_eq!(back.len(), 2, "{m}: requeued {back:?}");
    }
    let _ = sql(&mut sys, "DROP QUEUE r9fq.work IMMEDIATE CASCADE").await;
    ok(&mut sys, "DROP DATABASE r9fq").await;
}

// ---------------------------------------------------------------------------
// R9 final (r9final): fan-in sinks.
//
//   SPRIG_TEST_INHERIT_HOOK_PORT=29881   # a webhook receiver the test runs
// ---------------------------------------------------------------------------

type Received = std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>;

/// A webhook receiver on `port`: every record of every POST (JSON array
/// or NDJSON), answered 200.
async fn hook_receiver(port: u16) -> Received {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let got: Received = std::sync::Arc::default();
    let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    let g = got.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = l.accept().await else {
                return;
            };
            let g = g.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = vec![0u8; 65536];
                loop {
                    let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                        continue;
                    };
                    let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                    let len: usize = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .and_then(|v| v.trim().parse().ok())
                        .unwrap_or(0);
                    while buf.len() < end + 4 + len {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let body: Vec<u8> = buf[end + 4..end + 4 + len].to_vec();
                    buf.drain(..end + 4 + len);
                    let text = String::from_utf8_lossy(&body);
                    let mut recs = Vec::new();
                    if text.trim_start().starts_with('[') {
                        if let Ok(serde_json::Value::Array(a)) = serde_json::from_str(&text) {
                            recs = a;
                        }
                    } else {
                        recs = text
                            .lines()
                            .filter_map(|l| serde_json::from_str(l).ok())
                            .collect();
                    }
                    // A body with "poison" in it is refused for good (422).
                    let reply: &[u8] = if text.contains("poison") {
                        b"HTTP/1.1 422 Unprocessable Entity\r\ncontent-length: 2\r\n\r\n{}"
                    } else {
                        g.lock().unwrap().extend(recs);
                        b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}"
                    };
                    if s.write_all(reply).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    got
}

/// Received records per tenant: (tenant, value text, idempotency key).
fn received(got: &Received) -> Vec<(String, String, String)> {
    got.lock()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["tenant"].as_str().unwrap_or_default().to_owned(),
                match &r["value"] {
                    serde_json::Value::String(s) => s.clone(),
                    v => v.to_string(),
                },
                r["idempotencyKey"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect()
}

async fn wait_received(got: &Received, f: impl Fn(&[(String, String, String)]) -> bool, secs: u64) {
    let end = Instant::now() + Duration::from_secs(secs);
    while !f(&received(got)) && Instant::now() < end {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Publishes `fs-<m>-<i>` for member `m`, i in `from..to`.
async fn publish_fs(c: &mut Conn, m: &str, from: u64, to: u64) {
    let mut v = Vec::new();
    for i in from..to {
        v.push(format!("('k{i}', 'fs-{m}-{i}')"));
    }
    for chunk in v.chunks(100) {
        ok(
            c,
            &format!(
                "INSERT INTO security.fs_events FOR TENANT {m} (`key`, `value`) VALUES {}",
                chunk.join(", ")
            ),
        )
        .await;
    }
}

fn distinct_for(r: &[(String, String, String)], m: &str) -> std::collections::BTreeSet<String> {
    r.iter()
        .filter(|(t, _, _)| t == m)
        .map(|(_, v, _)| v.clone())
        .collect()
}

/// [`distinct_for`] of values `fs-<m>-<i>` with `i` in `range`.
fn distinct_in(
    r: &[(String, String, String)],
    m: &str,
    range: std::ops::Range<u64>,
) -> std::collections::BTreeSet<String> {
    distinct_for(r, m)
        .into_iter()
        .filter(|v| {
            v.rsplit('-')
                .next()
                .and_then(|n| n.parse::<u64>().ok())
                .is_some_and(|n| range.contains(&n))
        })
        .collect()
}

#[tokio::test]
async fn fan_in_sink_end_to_end() {
    let Some(e) = env("fan_in_sink_end_to_end") else {
        return;
    };
    let Some(port) = var("SPRIG_TEST_INHERIT_HOOK_PORT").and_then(|p| p.parse::<u16>().ok()) else {
        eprintln!("no SPRIG_TEST_INHERIT_HOOK_PORT: skipped");
        return;
    };
    let got = hook_receiver(port).await;
    let mut sys = conn(&e.addr, &e.system).await;
    let ms = e.members.clone();
    let _ = sql(&mut sys, "DROP SINK r9fsink.audit").await;
    let _ = sql(&mut sys, "DROP SINK r9fsink.ch").await;
    let _ = sql(&mut sys, "DROP CONNECTION r9fsink.hook").await;
    let _ = sql(&mut sys, "DROP STREAM security.fs_events IMMEDIATE CASCADE").await;
    // Tasks of an earlier run's sinks stop within a few seconds.
    tokio::time::sleep(Duration::from_secs(8)).await;
    got.lock().unwrap().clear();
    let _ = sql(&mut sys, "CREATE DATABASE IF NOT EXISTS security").await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS r9fsink").await;
    ok(&mut sys, "CREATE DATABASE r9fsink").await;
    ok(&mut sys, "GRANT SPRIG_SINK_ADMIN ON *.* TO app").await;
    ok(
        &mut sys,
        "CREATE STREAM security.fs_events FOR EACH TENANT PARTITIONS 2",
    )
    .await;
    // m0's instance with 5 records before the sink: START AT LATEST (the
    // default) skips them.
    publish_fs(&mut sys, &ms[0], 0, 5).await;
    let mut c0 = conn(&e.addr, &ms[0]).await;
    assert_eq!(
        wait_count(&mut c0, "SELECT COUNT(*) FROM security.fs_events", 5, 60).await,
        5
    );
    ok(
        &mut sys,
        &format!("CREATE CONNECTION r9fsink.hook TYPE WEBHOOK WITH (url = 'http://127.0.0.1:{port}', format = 'ndjson', signature = 'none')"),
    )
    .await;
    let create = "CREATE SINK r9fsink.audit FROM STREAM security.fs_events FAN IN \
                  TO WEBHOOK CONNECTION hook PATH '/in' \
                  WITH (batch_max_wait_ms = 100, fanin_refresh_seconds = 2)";
    // SPRIG_FAN_IN is needed; EXACTLY ONCE isn't built for fan-in.
    let _ = sql(&mut sys, "REVOKE SPRIG_FAN_IN ON *.* FROM app").await;
    let (code, msg) = sql(&mut sys, create).await.unwrap_err();
    println!("fan-in sink without SPRIG_FAN_IN: {code} {msg}");
    assert!(msg.contains("SPRIG_FAN_IN"), "{msg}");
    ok(&mut sys, "GRANT SPRIG_FAN_IN ON *.* TO app").await;
    let (code, msg) = sql(
        &mut sys,
        "CREATE SINK r9fsink.eo FROM STREAM security.fs_events FAN IN \
         TO WEBHOOK CONNECTION hook GUARANTEE EXACTLY ONCE",
    )
    .await
    .unwrap_err();
    println!("fan-in sink EXACTLY ONCE: {code} {msg}");
    let t = Instant::now();
    ok(&mut sys, create).await;
    println!("fan-in sink created: {:?}", t.elapsed());
    // Instances of m1 and m2 are made after the sink: from their start.
    for m in &ms {
        publish_fs(&mut sys, m, 100, 130).await;
    }
    wait_received(
        &got,
        |r| ms.iter().all(|m| distinct_in(r, m, 100..130).len() >= 30),
        90,
    )
    .await;
    let r = received(&got);
    println!(
        "first 90: {} records received in {:?}",
        r.len(),
        t.elapsed()
    );
    for m in &ms {
        let d = distinct_in(&r, m, 100..130);
        assert_eq!(d.len(), 30, "{m}: {d:?}");
        // m0's 5 records from before the sink never come.
        assert!(distinct_in(&r, m, 0..5).is_empty(), "{m}");
    }
    // Idempotency keys start with the member's id.
    assert!(r.iter().all(|(_, _, k)| k.contains(':')), "{r:?}");
    // kill -9 of the worker mid-stream: every record still arrives (at
    // least once, duplicates with the same idempotency key).
    got.lock().unwrap().clear();
    for m in &ms {
        publish_fs(&mut sys, m, 1000, 2000).await;
    }
    wait_received(&got, |r| r.len() >= 100, 60).await;
    let killed = kill_worker(&e);
    println!(
        "worker killed: {killed} after {} records",
        received(&got).len()
    );
    if killed {
        start_worker(&e);
    }
    wait_received(
        &got,
        |r| ms.iter().all(|m| distinct_for(r, m).len() >= 1000),
        300,
    )
    .await;
    // Only this round's records (an earlier batch whose position the
    // killed worker hadn't saved comes again: at least once).
    let all = received(&got);
    let again = all.len();
    let r: Vec<(String, String, String)> = all
        .into_iter()
        .filter(|(_, v, _)| {
            v.rsplit('-')
                .next()
                .and_then(|n| n.parse::<u64>().ok())
                .is_some_and(|n| n >= 1000)
        })
        .collect();
    println!("redelivered from before the kill: {}", again - r.len());
    let keys: std::collections::BTreeSet<&str> = r.iter().map(|(_, _, k)| k.as_str()).collect();
    println!(
        "through kill -9: {} deliveries, {} distinct idempotency keys, {} duplicates",
        r.len(),
        keys.len(),
        r.len() - keys.len()
    );
    for m in &ms {
        assert_eq!(distinct_for(&r, m).len(), 1000, "{m}");
    }
    assert_eq!(keys.len(), 3000);
    // A member created now: its records arrive from its instance's start.
    if let Some(late) = e.late.as_ref().map(|l| format!("{l}s")) {
        create_tenant(&e, &late);
        got.lock().unwrap().clear();
        let t = Instant::now();
        publish_fs(&mut sys, &late, 0, 10).await;
        wait_received(&got, |r| distinct_for(r, &late).len() >= 10, 60).await;
        println!(
            "late member {late}: 10 delivered {:?} after its first publish",
            t.elapsed()
        );
        assert_eq!(distinct_for(&received(&got), &late).len(), 10);
    }
    // m0's 5 records from before the sink never came.
    assert!(
        !received(&got)
            .iter()
            .any(|(_, v, _)| v.starts_with(&format!("fs-{}-0", ms[0])) && v.len() < 12)
    );
    ok(&mut sys, "DROP SINK r9fsink.audit").await;
    // CHANGES OF two tables, through their change topics.
    ok(
        &mut sys,
        "CREATE TABLE r9fsink.orders (id INT PRIMARY KEY, st VARCHAR(16))",
    )
    .await;
    ok(
        &mut sys,
        "CREATE TABLE r9fsink.items (id INT PRIMARY KEY, n INT)",
    )
    .await;
    let (code, msg) = sql(
        &mut sys,
        "CREATE SINK r9fsink.ch FROM CHANGES OF orders, items TO WEBHOOK CONNECTION hook",
    )
    .await
    .unwrap_err();
    println!("CHANGES OF without change topics: {code} {msg}");
    ok(
        &mut sys,
        "ALTER TABLE r9fsink.orders SPRIG SET (stream.change_topic = on)",
    )
    .await;
    ok(
        &mut sys,
        "ALTER TABLE r9fsink.items SPRIG SET (stream.change_topic = on)",
    )
    .await;
    ok(
        &mut sys,
        "CREATE SINK r9fsink.ch FROM CHANGES OF orders, items TO WEBHOOK CONNECTION hook \
         WITH (batch_max_wait_ms = 100)",
    )
    .await;
    got.lock().unwrap().clear();
    let t = Instant::now();
    ok(
        &mut sys,
        "INSERT INTO r9fsink.orders VALUES (1, 'new'), (2, 'new')",
    )
    .await;
    ok(
        &mut sys,
        "UPDATE r9fsink.orders SET st = 'paid' WHERE id = 1",
    )
    .await;
    ok(&mut sys, "DELETE FROM r9fsink.orders WHERE id = 2").await;
    ok(&mut sys, "INSERT INTO r9fsink.items VALUES (7, 3)").await;
    let end = Instant::now() + Duration::from_secs(90);
    while got.lock().unwrap().len() < 5 && Instant::now() < end {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let recs = got.lock().unwrap().clone();
    println!(
        "CHANGES OF: {} change records in {:?}",
        recs.len(),
        t.elapsed()
    );
    for r in &recs {
        println!(
            "  {} {} before={} after={}",
            r["op"], r["key"], r["before"], r["after"]
        );
    }
    let ops: Vec<String> = recs
        .iter()
        .map(|r| r["op"].as_str().unwrap_or("?").to_owned())
        .collect();
    assert_eq!(recs.len(), 5, "{recs:?}");
    assert_eq!(ops.iter().filter(|o| *o == "c").count(), 3, "{ops:?}");
    assert!(
        ops.contains(&"u".to_owned()) && ops.contains(&"d".to_owned()),
        "{ops:?}"
    );
    assert!(
        recs.iter()
            .any(|r| r["op"] == "u" && r["after"]["st"] == "paid" && r["before"]["st"] == "new")
    );
    ok(&mut sys, "DROP SINK r9fsink.ch").await;
    // DEAD LETTER TO STREAM: a record the destination refuses for good
    // lands in the dead-letter stream with its sprig.dl.* headers.
    let _ = sql(&mut sys, "DROP SINK r9fsink.dl").await;
    ok(&mut sys, "CREATE STREAM r9fsink.ev PARTITIONS 1").await;
    ok(&mut sys, "CREATE STREAM r9fsink.dead PARTITIONS 1").await;
    // The sink's task starts only after the three records (its worker is
    // down meanwhile): START AT LATEST counts from the sink's creation, so
    // they still come (r9fanin: it counted from the task's first start,
    // seconds later under load, and this step got nothing).
    let killed = kill_worker(&e);
    ok(
        &mut sys,
        "CREATE SINK r9fsink.dl FROM STREAM ev TO WEBHOOK CONNECTION hook \
         DEAD LETTER TO STREAM dead WITH (batch_max_wait_ms = 100)",
    )
    .await;
    got.lock().unwrap().clear();
    for v in ["ok1", "poison1", "ok2"] {
        ok(
            &mut sys,
            &format!("INSERT INTO r9fsink.ev (`key`, `value`) VALUES ('k', '{v}')"),
        )
        .await;
    }
    if killed {
        tokio::time::sleep(Duration::from_secs(3)).await;
        start_worker(&e);
    }
    let n = wait_count(&mut sys, "SELECT COUNT(*) FROM r9fsink.dead", 1, 120).await;
    let dl = ok(
        &mut sys,
        "SELECT CAST(`value` AS CHAR), JSON_UNQUOTE(JSON_EXTRACT(headers, '$.\"sprig.dl.class\"')) FROM r9fsink.dead",
    )
    .await;
    println!("DEAD LETTER TO STREAM: {n} dead letter(s): {dl:?}");
    if n != 1 {
        // What the step saw, for the report (r9cluster's runs ended here
        // with nothing to go on).
        let sinks = sql(&mut sys, "SHOW SINKS FROM r9fsink").await;
        let ev = sql(&mut sys, "SELECT COUNT(*) FROM r9fsink.ev").await;
        let vals: Vec<String> = received(&got).into_iter().map(|(_, v, _)| v).collect();
        println!("  sinks {sinks:?}; records in ev {ev:?}; received {vals:?}");
    }
    assert_eq!(n, 1);
    assert_eq!(dl[0][0].as_deref(), Some("poison1"));
    wait_received(&got, |r| r.len() >= 2, 30).await;
    let vals: Vec<String> = received(&got).into_iter().map(|(_, v, _)| v).collect();
    assert!(
        vals.contains(&"ok1".to_owned()) && vals.contains(&"ok2".to_owned()),
        "{vals:?}"
    );
    ok(&mut sys, "DROP SINK r9fsink.dl").await;
    ok(&mut sys, "DROP CONNECTION r9fsink.hook").await;
    let _ = sql(&mut sys, "DROP STREAM security.fs_events IMMEDIATE CASCADE").await;
    ok(&mut sys, "DROP DATABASE r9fsink").await;
}

const QDL_TRIGGER: &str = r"
CREATE TRIGGER r9fqdl.on_job ON QUEUE r9fqdl.jobs LANGUAGE javascript AS $$
export default function (ctx) {
  const v = String(ctx.message.body);
  const t = v.startsWith('bad') ? 'r9fqdl.later' : 'r9fqdl.applied';
  db.exec('INSERT INTO ' + t + ' (v) VALUES (?)', [v]);
}
$$";

/// RC11 for queue triggers (r9final): a queue trigger's dead letters are
/// replayed with `sprigctl logic dead-letters replay`, in order, once each
/// (marking them again runs nothing), while new messages keep flowing.
#[tokio::test]
async fn queue_trigger_dead_letter_replay() {
    let Some(e) = env("queue_trigger_dead_letter_replay") else {
        return;
    };
    let (Some(ctl), Some(sys_id), Some(amqp_addr)) = (
        var("SPRIG_TEST_SPRIGCTL"),
        tenant_id_of(&e.system),
        var("SPRIG_TEST_INHERIT_AMQP"),
    ) else {
        eprintln!(
            "no SPRIG_TEST_SPRIGCTL, SPRIG_TEST_INHERIT_IDS or SPRIG_TEST_INHERIT_AMQP: skipped"
        );
        return;
    };
    let root = var("SPRIG_TEST_INHERIT_ROOT").expect("SPRIG_TEST_INHERIT_ROOT");
    let mut sys = conn(&e.addr, &e.system).await;
    let _ = sql(&mut sys, "DROP TRIGGER r9fqdl.on_job").await;
    let _ = sql(&mut sys, "DROP DATABASE IF EXISTS r9fqdl").await;
    ok(&mut sys, "CREATE DATABASE r9fqdl").await;
    ok(
        &mut sys,
        "CREATE TABLE r9fqdl.applied (id BIGINT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(32))",
    )
    .await;
    let c = amqp(&amqp_addr, &e.system, "r9fqdl").await;
    let ch = c.create_channel().await.unwrap();
    ch.queue_declare(
        "jobs".into(),
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    ok(&mut sys, QDL_TRIGGER).await;
    let vals = ["g1", "bad1", "g2", "bad2", "g3", "bad3", "g4"];
    for v in vals {
        assert!(publish(&ch, "", "jobs", v, None).await);
    }
    assert_eq!(
        wait_count(&mut sys, "SELECT COUNT(*) FROM r9fqdl.applied", 4, 120).await,
        4
    );
    let cat = format!("{root}/t{sys_id}_sprig");
    let mut r = Conn::from_url(&cat).await.unwrap();
    let end = Instant::now() + Duration::from_secs(120);
    let binding: u64 = loop {
        let b: Option<(u64, u64)> = r
            .query_first(
                "SELECT binding_id, COUNT(*) FROM logic_dead_letters WHERE state = 'dead' \
                 GROUP BY binding_id ORDER BY binding_id DESC LIMIT 1",
            )
            .await
            .unwrap();
        if let Some((b, 3)) = b {
            break b;
        }
        assert!(Instant::now() < end, "three dead letters: {b:?}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    // Nothing left in the queue: the dead letters were taken out of it.
    assert!(take_all(&ch, "jobs", 0, 1).await.is_empty());
    ok(
        &mut sys,
        "CREATE TABLE r9fqdl.later (id BIGINT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(32))",
    )
    .await;
    let replay = || {
        let out = std::process::Command::new(&ctl)
            .args([
                "logic",
                "dead-letters",
                "replay",
                "--all",
                "--binding",
                &binding.to_string(),
                "--catalog",
                &cat,
            ])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    };
    let t = Instant::now();
    println!("sprigctl: {}", replay().trim());
    assert_eq!(
        wait_count(&mut sys, "SELECT COUNT(*) FROM r9fqdl.later", 3, 60).await,
        3
    );
    println!("three queue dead letters replayed in {:?}", t.elapsed());
    let order: Vec<String> = ok(&mut sys, "SELECT v FROM r9fqdl.later ORDER BY id")
        .await
        .into_iter()
        .map(|r| r[0].clone().unwrap())
        .collect();
    assert_eq!(order, ["bad1", "bad2", "bad3"], "in order");
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let n: Option<u64> = r
            .exec_first(
                "SELECT COUNT(*) FROM logic_dead_letters WHERE binding_id = ? AND state = \
                 'replayed'",
                (binding,),
            )
            .await
            .unwrap();
        if n == Some(3) || Instant::now() > end {
            assert_eq!(n, Some(3));
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    r.exec_drop(
        "UPDATE logic_dead_letters SET state = 'replaying' WHERE binding_id = ?",
        (binding,),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert_eq!(
        count(&mut sys, "SELECT COUNT(*) FROM r9fqdl.later").await,
        3
    );
    // New messages still run through the trigger.
    assert!(publish(&ch, "", "jobs", "g5", None).await);
    assert_eq!(
        wait_count(&mut sys, "SELECT COUNT(*) FROM r9fqdl.applied", 5, 60).await,
        5
    );
    assert_eq!(
        count(&mut sys, "SELECT COUNT(*) FROM r9fqdl.later").await,
        3
    );
    let _ = sql(&mut sys, "DROP TRIGGER r9fqdl.on_job").await;
    ok(&mut sys, "DROP DATABASE r9fqdl").await;
}
