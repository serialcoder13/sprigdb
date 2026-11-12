//! Row-level security in definer routines, live (chapter 29; RLS round 6,
//! decision RL57): a `SQL SECURITY DEFINER` procedure or function over a
//! protected table applies its definer's policies, an invoker routine its
//! caller's, and the caller's context comes back after every call, a
//! failed one included.
//!
//! ```sh
//! export SPRIG_TEST_RLS_SQL=127.0.0.1:37306
//! export SPRIG_TEST_RLS_TENANT=acme
//! cargo test -p sprig-gateway --test rls_definer_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use mysql_async::prelude::Queryable;

const PW: &str = "Rlsd-suite-pw-1";
const DB: &str = "rlsd_suite";

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
        .db_name(Some(DB))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn n(c: &mut mysql_async::Conn, q: &str) -> Result<i64, String> {
    c.query_first::<Option<i64>, _>(q)
        .await
        .map(|v| v.flatten().unwrap_or(-1))
        .map_err(|e| e.to_string())
}

async fn pair(c: &mut mysql_async::Conn, q: &str) -> Result<(i64, i64), String> {
    c.query_first::<(i64, i64), _>(q)
        .await
        .map(|v| v.unwrap_or((-1, -1)))
        .map_err(|e| e.to_string())
}

/// A `CALL` returning one count.
async fn call(c: &mut mysql_async::Conn, q: &str) -> Result<i64, String> {
    let mut r = c.query_iter(q).await.map_err(|e| e.to_string())?;
    let rows: Vec<i64> = r.collect().await.map_err(|e| e.to_string())?;
    drop(r);
    Ok(rows.first().copied().unwrap_or(-1))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn definer_routines_apply_their_definers_policies() {
    let Ok(addr) = std::env::var("SPRIG_TEST_RLS_SQL") else {
        eprintln!("skipped: set SPRIG_TEST_RLS_SQL");
        return;
    };
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let (host, port) = addr.split_once(':').unwrap();
    let mut admin = mysql_async::Conn::new(
        mysql_async::OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(format!("{}.app", tenant())))
            .pass(Some("sprig-dev-app"))
            .prefer_socket(false)
            .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true)),
    )
    .await
    .unwrap();
    for q in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        "DROP USER IF EXISTS 'rlsd_alice'@'%'".to_owned(),
        format!("CREATE DATABASE {DB}"),
        format!("CREATE TABLE {DB}.docs (id INT PRIMARY KEY, owner VARCHAR(64) NOT NULL)"),
        format!(
            "INSERT INTO {DB}.docs VALUES (1, 'rlsd_alice@%'), (2, 'app@%'), (3, 'rlsd_bob@%'), (4, 'rlsd_bob@%')"
        ),
        format!("CREATE USER 'rlsd_alice'@'%' IDENTIFIED BY '{PW}'"),
        format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE, CREATE ROUTINE, ALTER ROUTINE ON {DB}.* TO 'rlsd_alice'@'%'"
        ),
        format!("CREATE POLICY own ON {DB}.docs USING (owner = CURRENT_USER())"),
        format!("ALTER TABLE {DB}.docs ENABLE ROW LEVEL SECURITY"),
        // The administrator (the table's manager, unbound) defines these.
        format!(
            "CREATE PROCEDURE {DB}.all_docs() SQL SECURITY DEFINER SELECT COUNT(*) FROM {DB}.docs"
        ),
        format!(
            "CREATE PROCEDURE {DB}.inv_docs() SQL SECURITY INVOKER SELECT COUNT(*) FROM {DB}.docs"
        ),
        format!(
            "CREATE FUNCTION {DB}.total() RETURNS INT READS SQL DATA SQL SECURITY DEFINER \
             RETURN (SELECT COUNT(*) FROM {DB}.docs)"
        ),
        format!(
            "CREATE PROCEDURE {DB}.fails() SQL SECURITY DEFINER BEGIN \
             DECLARE n INT; SELECT COUNT(*) INTO n FROM {DB}.docs; SELECT * FROM {DB}.nosuch; END"
        ),
    ] {
        let r = admin.query_drop(&q).await;
        ck.ok(&format!("setup: {q}"), r.is_ok(), || format!("{r:?}"));
    }
    let mut a = sql(&addr, "rlsd_alice", PW).await;
    let mine = n(&mut a, "SELECT COUNT(*) FROM docs").await;
    ck.ok("alice reads her row", mine == Ok(1), || format!("{mine:?}"));
    // Definer: the administrator's view of the table.
    let got = call(&mut a, "CALL all_docs()").await;
    ck.ok(
        "definer procedure: the definer's rows",
        got == Ok(4),
        || format!("{got:?}"),
    );
    let mine = n(&mut a, "SELECT COUNT(*) FROM docs").await;
    ck.ok(
        "after the call: alice's own rows again",
        mine == Ok(1),
        || format!("{mine:?}"),
    );
    let got = call(&mut a, "CALL inv_docs()").await;
    ck.ok("invoker procedure: the caller's rows", got == Ok(1), || {
        format!("{got:?}")
    });
    let got = pair(&mut a, "SELECT total(), (SELECT COUNT(*) FROM docs)").await;
    ck.ok(
        "definer function: the definer's rows, the rest of the statement the caller's",
        got == Ok((4, 1)),
        || format!("{got:?}"),
    );
    // A failed definer call gives the caller's context back.
    let e = a.query_drop("CALL fails()").await;
    ck.ok(
        "a failing definer procedure fails",
        e.as_ref()
            .is_err_and(|e| e.to_string().contains("1146") || e.to_string().contains("nosuch")),
        || format!("{e:?}"),
    );
    let mine = n(&mut a, "SELECT COUNT(*) FROM docs").await;
    ck.ok(
        "after a failed call: alice's own rows",
        mine == Ok(1),
        || format!("{mine:?}"),
    );
    // Alice's own definer function, called by the administrator: alice's
    // rows (PostgreSQL's SECURITY DEFINER).
    let r = a
        .query_drop(
            "CREATE FUNCTION mine() RETURNS INT READS SQL DATA SQL SECURITY DEFINER \
             RETURN (SELECT COUNT(*) FROM docs)",
        )
        .await;
    ck.ok("alice creates a definer function", r.is_ok(), || {
        format!("{r:?}")
    });
    let got = n(&mut a, "SELECT mine()").await;
    ck.ok("alice's definer function for alice", got == Ok(1), || {
        format!("{got:?}")
    });
    let got = pair(
        &mut admin,
        &format!("SELECT {DB}.mine(), (SELECT COUNT(*) FROM {DB}.docs)"),
    )
    .await;
    ck.ok(
        "alice's definer function for the administrator: alice's rows",
        got == Ok((1, 4)),
        || format!("{got:?}"),
    );
    // A change of the definer's policies applies at once, without
    // redeploying: FORCE binds the administrator to `own` too.
    let r = admin
        .query_drop(format!("ALTER TABLE {DB}.docs FORCE ROW LEVEL SECURITY"))
        .await;
    ck.ok("FORCE", r.is_ok(), || format!("{r:?}"));
    let got = pair(&mut a, "SELECT total(), (SELECT COUNT(*) FROM docs)").await;
    ck.ok(
        "after FORCE: the definer function shows the administrator's own row",
        got == Ok((1, 1)),
        || format!("{got:?}"),
    );
    let _ = admin
        .query_drop(format!("ALTER TABLE {DB}.docs NO FORCE ROW LEVEL SECURITY"))
        .await;
    let got = pair(&mut a, "SELECT total(), (SELECT COUNT(*) FROM docs)").await;
    ck.ok("after NO FORCE: all rows again", got == Ok((4, 1)), || {
        format!("{got:?}")
    });
    // SHOW CREATE shows the routine as written.
    let sc: Result<Option<(String, String, String)>, _> = admin
        .query_first(format!("SHOW CREATE PROCEDURE {DB}.all_docs"))
        .await
        .map(|r: Option<mysql_async::Row>| {
            r.map(|r| {
                (
                    r.get::<String, _>(0).unwrap_or_default(),
                    r.get::<String, _>(1).unwrap_or_default(),
                    r.get::<Option<String>, _>(2).flatten().unwrap_or_default(),
                )
            })
        });
    let text = sc
        .as_ref()
        .ok()
        .and_then(|r| r.as_ref())
        .map(|r| r.2.clone())
        .unwrap_or_default();
    ck.ok(
        "SHOW CREATE PROCEDURE reads as written",
        text.contains("SELECT COUNT(*) FROM")
            && !text.contains("sprig")
            && !text.contains("@sprig_rls"),
        || format!("{sc:?}"),
    );

    // Cleanup.
    let _ = admin
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await;
    let _ = admin
        .query_drop("DROP USER IF EXISTS 'rlsd_alice'@'%'")
        .await;
    eprintln!(
        "row-level security in definer routines: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}

/// `sprigctl settings set|reset <key> --scope <scope>` through the command
/// in `SPRIG_TEST_RLS_CTL` (a private deployment's wrapper).
fn setting(ctl: &str, key: &str, value: Option<&str>, scope: &str) {
    let mut c = std::process::Command::new(ctl);
    match value {
        Some(v) => c.args([
            "settings", "set", key, v, "--scope", scope, "--reason", "rls test",
        ]),
        None => c.args([
            "settings", "reset", key, "--scope", scope, "--reason", "rls test",
        ]),
    };
    let out = c.output().unwrap();
    assert!(
        out.status.success() || value.is_none(),
        "{key}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

async fn ids(c: &mut mysql_async::Conn, q: &str) -> Result<Vec<i64>, String> {
    c.query::<i64, _>(q).await.map_err(|e| e.to_string())
}

/// RLS round 6 (RL58): a subtree above `rls.inline_subtree_max` is read
/// from `__sprig_sys.__sprig_subtree` instead of refused. Needs
/// `SPRIG_TEST_RLS_CTL`, `SPRIG_TEST_RLS_TENANT_ID` and a child tenant's id
/// in `SPRIG_TEST_RLS_SUBTREE_CHILD`; `SPRIG_TEST_RLS_DIRECT` adds the
/// MariaDB-side checks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_subtrees_read_the_subtree_table() {
    let (Ok(addr), Ok(ctl), Ok(tid), Ok(child)) = (
        std::env::var("SPRIG_TEST_RLS_SQL"),
        std::env::var("SPRIG_TEST_RLS_CTL"),
        std::env::var("SPRIG_TEST_RLS_TENANT_ID"),
        std::env::var("SPRIG_TEST_RLS_SUBTREE_CHILD"),
    ) else {
        eprintln!(
            "skipped: set SPRIG_TEST_RLS_SQL, SPRIG_TEST_RLS_CTL, SPRIG_TEST_RLS_TENANT_ID and SPRIG_TEST_RLS_SUBTREE_CHILD"
        );
        return;
    };
    const SDB: &str = "rlst_suite";
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let scope = format!("tenant:{}", tenant());
    let (host, port) = addr.split_once(':').unwrap();
    let opts = |user: &str, pw: &str| {
        mysql_async::OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(format!("{}.{user}", tenant())))
            .pass(Some(pw.to_owned()))
            .prefer_socket(false)
            .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true))
    };
    let mut admin = mysql_async::Conn::new(opts("app", "sprig-dev-app"))
        .await
        .unwrap();
    for q in [
        format!("DROP DATABASE IF EXISTS {SDB}"),
        "DROP USER IF EXISTS 'rlst_alice'@'%'".to_owned(),
        format!("CREATE DATABASE {SDB}"),
        format!("CREATE TABLE {SDB}.orgs (id INT PRIMARY KEY, org BIGINT UNSIGNED NOT NULL)"),
        format!("INSERT INTO {SDB}.orgs VALUES (1, {tid}), (2, {child}), (3, 99)"),
        format!("CREATE USER 'rlst_alice'@'%' IDENTIFIED BY '{PW}'"),
        format!("GRANT SELECT ON {SDB}.* TO 'rlst_alice'@'%'"),
        format!("CREATE POLICY sub ON {SDB}.orgs USING (SPRIG_TENANT_IN_SUBTREE(org))"),
        format!("ALTER TABLE {SDB}.orgs ENABLE ROW LEVEL SECURITY"),
    ] {
        let r = admin.query_drop(&q).await;
        ck.ok(&format!("setup: {q}"), r.is_ok(), || format!("{r:?}"));
    }
    let mut a = mysql_async::Conn::new(opts("rlst_alice", PW))
        .await
        .unwrap();
    let q = format!("SELECT id FROM {SDB}.orgs ORDER BY id");
    let got = ids(&mut a, &q).await;
    ck.ok(
        "inlined subtree: own and child rows",
        got == Ok(vec![1, 2]),
        || format!("{got:?}"),
    );
    // Above the inline limit (the tree has 2 tenants).
    setting(&ctl, "rls.inline_subtree_max", Some("1"), &scope);
    // The setting is live; give it a few seconds to reach the gateway.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    let got = ids(&mut a, &q).await;
    ck.ok(
        "subtree above rls.inline_subtree_max: own and child rows (was 39366)",
        got == Ok(vec![1, 2]),
        || format!("{got:?}"),
    );
    let got = ids(
        &mut a,
        &format!("SELECT COUNT(*) FROM {SDB}.orgs WHERE org = 99"),
    )
    .await;
    ck.ok(
        "other tenants' rows stay hidden",
        got == Ok(vec![0]),
        || format!("{got:?}"),
    );
    let got = ids(&mut admin, &format!("SELECT COUNT(*) FROM {SDB}.orgs")).await;
    ck.ok("the manager reads every row", got == Ok(vec![3]), || {
        format!("{got:?}")
    });
    // Straight to MariaDB: the account reads its own tenant's subtree, and
    // the policy view applies it from the context's flag.
    if let Ok(direct) = std::env::var("SPRIG_TEST_RLS_DIRECT") {
        let root_pw =
            std::env::var("SPRIG_TEST_RLS_ROOT_PW").unwrap_or_else(|_| "sprig-dev-root".into());
        let secret = std::env::var("SPRIG_TEST_RLS_SECRET")
            .unwrap_or_else(|_| "sprig-dev-backend-accounts-secret".into());
        let (dh, dp) = direct.split_once(':').unwrap();
        let raw = |u: String, p: String| {
            mysql_async::OptsBuilder::default()
                .ip_or_hostname(dh)
                .tcp_port(dp.parse().unwrap())
                .user(Some(u))
                .pass(Some(p))
                .prefer_socket(false)
                .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true))
        };
        let mut root = mysql_async::Conn::new(raw("root".into(), root_pw))
            .await
            .unwrap();
        let uid: u64 = root
            .query_first(format!(
                "SELECT MAX(user_id) FROM t{tid}_sprig.users WHERE name = 'rlst_alice'"
            ))
            .await
            .unwrap()
            .unwrap();
        let pid: u64 = root
            .query_first(format!(
                "SELECT policy_id FROM t{tid}_sprig.row_policies WHERE name = 'sub'"
            ))
            .await
            .unwrap()
            .unwrap();
        let schemas: Vec<String> = root
            .query(format!(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE \
                 't{tid}\\_d%' AND TABLE_NAME = '__sprig_rls_orgs'"
            ))
            .await
            .unwrap();
        drop(root);
        let account = format!("t{tid}_u{uid}");
        let pw = sprig_auth::authz::backend::BackendKey::new(secret.as_bytes())
            .unwrap()
            .password(&account)
            .to_string();
        let mut b = mysql_async::Conn::new(raw(account, pw)).await.unwrap();
        let mut sub: Vec<i64> = b
            .query("SELECT CAST(descendant AS SIGNED) FROM `__sprig_sys`.`__sprig_subtree_v`")
            .await
            .unwrap_or_default();
        sub.sort_unstable();
        let mut want = vec![tid.parse::<i64>().unwrap(), child.parse::<i64>().unwrap()];
        want.sort_unstable();
        ck.ok(
            "direct: the account reads its own tenant's subtree",
            sub == want,
            || format!("{sub:?} vs {want:?}"),
        );
        let ctx = serde_json::json!({
            "b": 0, "x": [], "p": [pid],
            "s": {"user": "rlst_alice@%", "uid": uid, "tid": tid.parse::<u64>().unwrap(),
                  "sub": [], "subt": true}
        })
        .to_string();
        b.query_drop(format!("SET @sprig_rls = '{ctx}'"))
            .await
            .unwrap();
        let mut seen = Vec::new();
        for s in &schemas {
            seen.extend(
                ids(&mut b, &format!("SELECT id FROM `{s}`.__sprig_rls_orgs"))
                    .await
                    .unwrap_or_default(),
            );
        }
        seen.sort_unstable();
        ck.ok(
            "direct: the policy view reads the subtree table",
            !schemas.is_empty() && seen == vec![1, 2],
            || format!("{schemas:?}: {seen:?}"),
        );
    }
    setting(&ctl, "rls.inline_subtree_max", None, &scope);
    let _ = admin
        .query_drop(format!("DROP DATABASE IF EXISTS {SDB}"))
        .await;
    let _ = admin
        .query_drop("DROP USER IF EXISTS 'rlst_alice'@'%'")
        .await;
    eprintln!(
        "subtrees above rls.inline_subtree_max: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
