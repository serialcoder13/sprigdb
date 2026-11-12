//! Kiran's example of 11 November 2026 through a running gateway (chapter
//! 42, "Foreign keys, groups and tree-wide ids"): tenants `a → b → c`, `a`'s
//! `t1` references `t2` by foreign key.
//!
//! - Only `a` declares anything: `b` and `c` receive `t1` and `t2` (the
//!   foreign-key group) without DDL.
//! - Auto-increment inserts at all three levels, concurrently, take ids
//!   from one sequence: no id twice, no row hidden by another level's.
//! - Foreign keys are checked against each level's merged view (1452).
//! - `a`'s delete of a `t2` row that `b` or `c` references is refused
//!   (1451), and allowed once they drop their references; a hide in `b` is
//!   refused while `c` references the row.
//! - `t3` added at `a` and `t4` added at `b` (each with a foreign key into
//!   the group) appear at `c`; a column added to `t1` at `b` shows at `c`,
//!   not at `a`.
//! - The same inside one tenant, between spaces.
//!
//! ```sh
//! export SPRIG_TEST_IG_GATEWAY=127.0.0.1:27406,127.0.0.1:27407   # mysql, mariadb persona
//! export SPRIG_TEST_IG_TENANTS=ig-a,ig-b,ig-c                     # a > b > c
//! export SPRIG_TEST_IG_MODE=read_through                          # or materialized
//! cargo test -p sprig-inherit --test groups_live -- --nocapture --test-threads 1
//! ```
//!
//! The materialized mode needs the deployment's worker process (it keeps
//! the copies). Each run uses its own database per persona and mode and
//! drops it at the end. Without the variables every test reports what it
//! needs and passes (fails under `SPRIG_TEST_STRICT=1`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::uninlined_format_args,
    clippy::many_single_char_names,
    clippy::needless_range_loop,
    clippy::similar_names,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row, SslOpts, Value};

const GATEWAY: &str = "SPRIG_TEST_IG_GATEWAY";

fn endpoints(test: &str) -> Option<Vec<String>> {
    if let Some(v) = std::env::var(GATEWAY).ok().filter(|v| !v.trim().is_empty()) {
        Some(v.split(',').map(|s| s.trim().to_owned()).collect())
    } else {
        assert!(
            std::env::var("SPRIG_TEST_STRICT").ok().as_deref() != Some("1"),
            "{test}: {GATEWAY} is not set"
        );
        eprintln!("{test}: skipped, set {GATEWAY}");
        None
    }
}

fn tenants() -> Vec<String> {
    std::env::var("SPRIG_TEST_IG_TENANTS")
        .unwrap_or_else(|_| "ig-a,ig-b,ig-c".into())
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect()
}

fn mode() -> String {
    std::env::var("SPRIG_TEST_IG_MODE").unwrap_or_else(|_| "read_through".into())
}

async fn conn(addr: &str, tenant: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.app")))
        .pass(Some("sprig-dev-app"))
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("connect {addr} as {tenant}: {e}"))
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::Int(i) => Some(i.to_string()),
        Value::UInt(u) => Some(u.to_string()),
        other => Some(format!("{other:?}")),
    }
}

async fn rows(c: &mut Conn, sql: &str) -> Vec<Vec<Option<String>>> {
    let r: Vec<Row> = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    r.into_iter()
        .map(|r| r.unwrap().iter().map(text).collect())
        .collect()
}

async fn exec(c: &mut Conn, sql: &str) -> u64 {
    c.query_drop(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    c.affected_rows()
}

/// The error number and message of a failing statement.
async fn fails(c: &mut Conn, sql: &str, code: u16) -> String {
    let e = c.query_drop(sql).await.expect_err(sql);
    match &e {
        mysql_async::Error::Server(s) => {
            assert_eq!(s.code, code, "{sql}: {e}");
            assert!(!s.message.contains("_sprig_"), "{sql}: {}", s.message);
            s.message.clone()
        }
        other => panic!("{sql}: not a server error: {other}"),
    }
}

async fn try_exec(c: &mut Conn, sql: &str) -> Result<u64, u16> {
    match c.query_drop(sql).await {
        Ok(()) => Ok(c.affected_rows()),
        Err(mysql_async::Error::Server(s)) => Err(s.code),
        Err(e) => panic!("{sql}: {e}"),
    }
}

async fn tables(c: &mut Conn) -> BTreeSet<String> {
    rows(c, "SHOW TABLES")
        .await
        .into_iter()
        .filter_map(|r| r[0].clone())
        .collect()
}

/// Waits (materialized copies lag) until `sql` returns `want` rows.
async fn eventually(c: &mut Conn, sql: &str, want: usize) {
    let t0 = Instant::now();
    loop {
        // Across clusters a copy that isn't serving yet can't be read
        // through (39889): wait for it too.
        let n = match c.query::<Row, _>(sql).await {
            Ok(r) => r.len(),
            Err(mysql_async::Error::Server(s)) if s.code == 39889 => usize::MAX,
            Err(e) => panic!("{sql}: {e}"),
        };
        if n == want {
            return;
        }
        let wait: u64 = std::env::var("SPRIG_TEST_IG_WAIT_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);
        assert!(
            t0.elapsed() < Duration::from_secs(wait),
            "{sql}: {n} rows, want {want}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `sql`'s outcome once a copy that isn't serving yet (39889 across
/// clusters, after a schema change) serves.
async fn settled(c: &mut Conn, sql: &str) -> Result<u64, u16> {
    let t0 = Instant::now();
    loop {
        match try_exec(c, sql).await {
            Err(39889) if t0.elapsed() < Duration::from_secs(180) => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            other => return other,
        }
    }
}

async fn cleanup(c: &mut [Conn], t: &[String], db: &str, shares: &[(usize, &str)]) {
    for (i, s) in shares {
        exec(&mut c[*i], &format!("USE SPACE `{}`", t[*i])).await;
        exec(&mut c[*i], &format!("DROP SHARE IF EXISTS {s}")).await;
    }
    for x in c.iter_mut().rev() {
        let _ = try_exec(x, &format!("DROP DATABASE IF EXISTS {db}")).await;
    }
}

#[tokio::test]
async fn kirans_example() {
    let Some(eps) = endpoints("kirans_example") else {
        return;
    };
    let t = tenants();
    let mode = mode();
    let mat = mode == "materialized";
    for (pi, ep) in eps.iter().enumerate() {
        let p = if pi == 0 { "my" } else { "ma" };
        let db = format!("ig_ex_{p}_{}", if mat { "mat" } else { "rt" });
        let s1 = format!("ig1{p}{}", u8::from(mat));
        let sb = format!("igb{p}{}", u8::from(mat));
        let mut c = Vec::new();
        for name in &t {
            c.push(conn(ep, name).await);
        }
        let shares = [(1, sb.as_str()), (0, s1.as_str())];
        cleanup(&mut c, &t, &db, &shares).await;
        for (i, x) in c.iter_mut().enumerate() {
            exec(x, &format!("USE SPACE `{}`", t[i])).await;
            if mat {
                exec(x, "SPRIG SET inherit.merge = 'materialized'").await;
            } else {
                let _ = try_exec(x, "SPRIG RESET inherit.merge").await;
            }
        }
        // 1. Only a declares: t2, then t1 with a MariaDB foreign key.
        let a = 0;
        exec(&mut c[a], &format!("CREATE DATABASE {db}")).await;
        exec(&mut c[a], &format!("USE {db}")).await;
        exec(
            &mut c[a],
            "CREATE TABLE t2 (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(40) NULL)",
        )
        .await;
        exec(
            &mut c[a],
            "CREATE TABLE t1 (id INT AUTO_INCREMENT PRIMARY KEY, t2_id INT NULL, note \
             VARCHAR(40) NULL, CONSTRAINT fk_t2 FOREIGN KEY (t2_id) REFERENCES t2 (id))",
        )
        .await;
        exec(&mut c[a], "INSERT INTO t2 (name) VALUES ('a-first')").await;
        exec(&mut c[a], "ALTER TABLE t1 SPRIG INHERITABLE KEY (id)").await;
        exec(
            &mut c[a],
            &format!("CREATE SHARE {s1} ON t1 TO ALL CHILDREN WITH RESHARE"),
        )
        .await;
        // b and c receive both tables without DDL; b passes them on.
        exec(&mut c[1], &format!("USE {db}")).await;
        let tb = tables(&mut c[1]).await;
        assert!(tb.contains("t1") && tb.contains("t2"), "b: {tb:?}");
        exec(
            &mut c[1],
            &format!("CREATE SHARE {sb} ON t1 TO ALL CHILDREN WITH RESHARE"),
        )
        .await;
        exec(&mut c[2], &format!("USE {db}")).await;
        let tc = tables(&mut c[2]).await;
        assert!(tc.contains("t1") && tc.contains("t2"), "c: {tc:?}");
        if mat {
            eventually(&mut c[2], "SELECT id FROM t2", 1).await;
        }
        assert_eq!(
            rows(&mut c[2], "SELECT name FROM t2").await,
            vec![vec![Some("a-first".to_owned())]]
        );

        // 2. Concurrent auto-increment inserts at all three levels.
        let per = 40usize;
        let mut jobs = Vec::new();
        for (lvl, name) in t.iter().enumerate() {
            for k in 0..2 {
                let ep = ep.clone();
                let name = name.clone();
                let db = db.clone();
                jobs.push(tokio::spawn(async move {
                    let mut x = conn(&ep, &name).await;
                    exec(&mut x, &format!("USE {db}")).await;
                    let mut got = Vec::new();
                    for i in 0..per {
                        let label = format!("L{lvl}-{k}-{i}");
                        exec(&mut x, &format!("INSERT INTO t2 (name) VALUES ('{label}')")).await;
                        let id: Option<u64> =
                            x.query_first("SELECT LAST_INSERT_ID()").await.unwrap();
                        got.push((id.unwrap(), label, lvl));
                    }
                    got
                }));
            }
        }
        let mut all: Vec<(u64, String, usize)> = Vec::new();
        for j in jobs {
            all.extend(j.await.unwrap());
        }
        let ids: BTreeSet<u64> = all.iter().map(|x| x.0).collect();
        assert_eq!(ids.len(), all.len(), "{p}: duplicate ids across levels");
        // No shadowing: each level's view holds, for every id, the row its
        // own inserter wrote (every level sees a's rows, c sees b's).
        for lvl in 0..3 {
            if mat {
                let want = 1 + all.iter().filter(|x| x.2 <= lvl).count();
                eventually(&mut c[lvl], "SELECT id FROM t2", want).await;
            }
            let view: BTreeMap<u64, String> = rows(&mut c[lvl], "SELECT id, name FROM t2")
                .await
                .into_iter()
                .map(|r| {
                    (
                        r[0].clone().unwrap().parse().unwrap(),
                        r[1].clone().unwrap(),
                    )
                })
                .collect();
            for (id, label, by) in &all {
                if *by <= lvl {
                    assert_eq!(view.get(id), Some(label), "{p}: level {lvl} id {id}");
                } else {
                    assert!(
                        !view.contains_key(id),
                        "{p}: level {lvl} sees a lower level's {id}"
                    );
                }
            }
        }
        eprintln!(
            "{p}/{mode}: {} concurrent inserts at three levels, {} distinct ids, none shadowed",
            all.len(),
            ids.len()
        );
        let a_row = all.iter().find(|x| x.2 == 0).unwrap().0;
        let a_row2 = all.iter().filter(|x| x.2 == 0).nth(1).unwrap().0;
        let b_row = all.iter().find(|x| x.2 == 1).unwrap().0;
        let c_row = all.iter().find(|x| x.2 == 2).unwrap().0;

        // 3. Foreign keys against each level's merged view.
        if mat {
            // Across clusters t1's copy must serve before c can use it.
            eventually(&mut c[2], "SELECT id FROM t1", 0).await;
            eventually(&mut c[1], "SELECT id FROM t1", 0).await;
        }
        exec(
            &mut c[2],
            &format!("INSERT INTO t1 (t2_id, note) VALUES ({a_row}, 'c->a')"),
        )
        .await;
        let c_ref: u64 = c[2]
            .query_first("SELECT LAST_INSERT_ID()")
            .await
            .unwrap()
            .unwrap();
        exec(
            &mut c[2],
            &format!("INSERT INTO t1 (t2_id, note) VALUES ({b_row}, 'c->b')"),
        )
        .await;
        exec(
            &mut c[1],
            &format!("INSERT INTO t1 (t2_id, note) VALUES ({a_row}, 'b->a')"),
        )
        .await;
        let b_ref: u64 = c[1]
            .query_first("SELECT LAST_INSERT_ID()")
            .await
            .unwrap()
            .unwrap();
        fails(
            &mut c[1],
            &format!("INSERT INTO t1 (t2_id, note) VALUES ({c_row}, 'b->c')"),
            1452,
        )
        .await;
        fails(&mut c[2], "INSERT INTO t1 (t2_id) VALUES (999999999)", 1452).await;
        fails(
            &mut c[2],
            &format!("UPDATE t1 SET t2_id = 888888888 WHERE id = {c_ref}"),
            1452,
        )
        .await;
        // An update in c of the row b wrote: c's override references c's own row.
        if mat {
            eventually(
                &mut c[2],
                &format!("SELECT id FROM t1 WHERE id = {b_ref}"),
                1,
            )
            .await;
        }
        exec(
            &mut c[2],
            &format!("UPDATE t1 SET t2_id = {c_row} WHERE id = {b_ref}"),
        )
        .await;
        // Parent and child row in one transaction.
        exec(&mut c[2], "BEGIN").await;
        exec(&mut c[2], "INSERT INTO t2 (name) VALUES ('c-tx')").await;
        let tx_row: u64 = c[2]
            .query_first("SELECT LAST_INSERT_ID()")
            .await
            .unwrap()
            .unwrap();
        exec(
            &mut c[2],
            &format!("INSERT INTO t1 (t2_id, note) VALUES ({tx_row}, 'same tx')"),
        )
        .await;
        exec(&mut c[2], "COMMIT").await;

        // 4. Deletes are refused while a level below references the row.
        let m = fails(
            &mut c[0],
            &format!("DELETE FROM t2 WHERE id = {a_row}"),
            1451,
        )
        .await;
        eprintln!("{p}/{mode}: a's delete refused: {m}");
        // b hides a's row while c references it: refused too.
        fails(
            &mut c[1],
            &format!("DELETE FROM t2 WHERE id = {a_row}"),
            1451,
        )
        .await;
        exec(&mut c[1], &format!("DELETE FROM t1 WHERE id = {b_ref}")).await;
        fails(
            &mut c[0],
            &format!("DELETE FROM t2 WHERE id = {a_row}"),
            1451,
        )
        .await;
        exec(&mut c[2], &format!("DELETE FROM t1 WHERE id = {c_ref}")).await;
        // c's override of b's row now points at c's own row.
        assert_eq!(
            exec(&mut c[0], &format!("DELETE FROM t2 WHERE id = {a_row}")).await,
            1
        );
        // An unreferenced row goes at once.
        assert_eq!(
            exec(&mut c[0], &format!("DELETE FROM t2 WHERE id = {a_row2}")).await,
            1
        );
        // c's own referenced row can't go either.
        fails(
            &mut c[2],
            &format!("DELETE FROM t2 WHERE id = {tx_row}"),
            1451,
        )
        .await;

        // 5. t3 at a and t4 at b join the group and reach c; a column added
        //    at b reaches c, not a.
        exec(
            &mut c[0],
            "CREATE TABLE t3 (id INT AUTO_INCREMENT PRIMARY KEY, t1_id INT NULL, FOREIGN KEY \
             (t1_id) REFERENCES t1 (id))",
        )
        .await;
        exec(
            &mut c[1],
            "CREATE TABLE t4 (id INT AUTO_INCREMENT PRIMARY KEY, t1_id INT NULL, CONSTRAINT \
             fk4 FOREIGN KEY (t1_id) REFERENCES t1 (id))",
        )
        .await;
        exec(
            &mut c[1],
            "ALTER TABLE t1 ADD COLUMN b_extra VARCHAR(10) NULL",
        )
        .await;
        let tc = tables(&mut c[2]).await;
        assert!(tc.contains("t3") && tc.contains("t4"), "c: {tc:?}");
        assert!(!tables(&mut c[0]).await.contains("t4"));
        settled(&mut c[2], "SELECT id, b_extra FROM t1")
            .await
            .unwrap();
        fails(&mut c[0], "SELECT b_extra FROM t1", 1054).await;
        exec(
            &mut c[1],
            "INSERT INTO t1 (note, b_extra) VALUES ('b-new', 'bx')",
        )
        .await;
        if mat {
            eventually(&mut c[2], "SELECT id FROM t1 WHERE b_extra = 'bx'", 1).await;
        }
        settled(&mut c[2], "SELECT b_extra FROM t1").await.unwrap();
        assert!(
            rows(&mut c[2], "SELECT b_extra FROM t1")
                .await
                .iter()
                .any(|r| r[0].as_deref() == Some("bx"))
        );
        // t4's foreign key holds at c: c's rows of t4 must reference c's view of t1.
        assert_eq!(
            settled(&mut c[2], "INSERT INTO t4 (t1_id) VALUES (999999999)").await,
            Err(1452)
        );

        // 6. Refusals.
        // Q3: a column a descendant already has.
        exec(&mut c[2], "ALTER TABLE t1 ADD COLUMN c_own INT NULL").await;
        fails(&mut c[1], "ALTER TABLE t1 ADD COLUMN c_own INT NULL", 39897).await;
        // Leaving the group while shared.
        fails(&mut c[0], "ALTER TABLE t1 DROP FOREIGN KEY fk_t2", 39895).await;
        // Cascades.
        fails(
            &mut c[0],
            "CREATE TABLE t5 (id INT PRIMARY KEY, t1_id INT, FOREIGN KEY (t1_id) REFERENCES t1 \
             (id) ON DELETE CASCADE)",
            39462,
        )
        .await;
        // A child can't drop what it inherits.
        fails(&mut c[2], "DROP TABLE t2", 39897).await;
        assert!(rows(&mut c[0], "SPRIG CHECK IDS t2").await.is_empty());

        cleanup(&mut c, &t, &db, &shares).await;
    }
}

#[tokio::test]
async fn spaces_inside_one_tenant() {
    let Some(eps) = endpoints("spaces_inside_one_tenant") else {
        return;
    };
    let t = tenants();
    let ep = &eps[0];
    let mut a = conn(ep, &t[0]).await;
    let db = "ig_spaces";
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    exec(&mut a, "DROP SHARE IF EXISTS igsp").await;
    exec(&mut a, &format!("DROP DATABASE IF EXISTS {db}")).await;
    exec(&mut a, &format!("CREATE DATABASE {db}")).await;
    exec(&mut a, &format!("USE {db}")).await;
    match try_exec(&mut a, "CREATE SPACE igx").await {
        Ok(_) | Err(39881) => {}
        Err(e) => panic!("{e}"),
    }
    exec(
        &mut a,
        "CREATE TABLE t2 (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20))",
    )
    .await;
    exec(&mut a, "ALTER TABLE t2 SPRIG INHERITABLE KEY (id)").await;
    // A table created with a foreign key onto an inheritable one joins it.
    exec(
        &mut a,
        "CREATE TABLE t1 (id INT AUTO_INCREMENT PRIMARY KEY, t2_id INT, FOREIGN KEY (t2_id) \
         REFERENCES t2 (id))",
    )
    .await;
    exec(&mut a, "CREATE SHARE igsp ON t1 TO SPACE igx").await;
    exec(&mut a, "INSERT INTO t2 (name) VALUES ('root')").await;
    let r: u64 = a
        .query_first("SELECT LAST_INSERT_ID()")
        .await
        .unwrap()
        .unwrap();
    exec(&mut a, "USE SPACE igx").await;
    // t2 reaches igx through t1's share.
    assert_eq!(rows(&mut a, "SELECT name FROM t2").await.len(), 1);
    exec(&mut a, &format!("INSERT INTO t1 (t2_id) VALUES ({r})")).await;
    let child: u64 = a
        .query_first("SELECT LAST_INSERT_ID()")
        .await
        .unwrap()
        .unwrap();
    fails(&mut a, "INSERT INTO t1 (t2_id) VALUES (424242)", 1452).await;
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    let m = fails(&mut a, &format!("DELETE FROM t2 WHERE id = {r}"), 1451).await;
    assert!(m.contains("igx"), "{m}");
    exec(&mut a, "USE SPACE igx").await;
    exec(&mut a, &format!("DELETE FROM t1 WHERE id = {child}")).await;
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    assert_eq!(
        exec(&mut a, &format!("DELETE FROM t2 WHERE id = {r}")).await,
        1
    );
    exec(&mut a, "DROP SHARE igsp").await;
    exec(&mut a, "DROP SPACE igx CASCADE").await;
    exec(&mut a, &format!("DROP DATABASE {db}")).await;
}

#[tokio::test]
async fn migration_reports_collisions() {
    let Some(eps) = endpoints("migration_reports_collisions") else {
        return;
    };
    let t = tenants();
    let ep = &eps[0];
    let mut a = conn(ep, &t[0]).await;
    let mut b = conn(ep, &t[1]).await;
    let db = "ig_migr";
    exec(&mut a, &format!("USE SPACE `{}`", t[0])).await;
    exec(&mut a, "DROP SHARE IF EXISTS igm").await;
    for x in [&mut b, &mut a] {
        exec(x, &format!("DROP DATABASE IF EXISTS {db}")).await;
    }
    // Before the share each tenant counts its own ids (the old behaviour):
    // b's 1 and 2 are a's ids too.
    for (x, who, n) in [(&mut a, "a", 3), (&mut b, "b", 2)] {
        exec(x, &format!("CREATE DATABASE {db}")).await;
        exec(x, &format!("USE {db}")).await;
        exec(
            x,
            "CREATE TABLE t9 (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(10))",
        )
        .await;
        exec(x, "ALTER TABLE t9 SPRIG INHERITABLE KEY (id)").await;
        for i in 0..n {
            exec(x, &format!("INSERT INTO t9 (v) VALUES ('{who}{i}')")).await;
        }
    }
    exec(
        &mut a,
        &format!("CREATE SHARE igm ON t9 TO SPACE `{}`", t[1]),
    )
    .await;
    // The collisions are reported, not renumbered.
    let rep = rows(&mut a, "SPRIG CHECK IDS t9").await;
    let ids: Vec<String> = rep.iter().map(|r| r[0].clone().unwrap()).collect();
    assert_eq!(ids, vec!["1".to_owned(), "2".to_owned()], "{rep:?}");
    assert!(rep[0][1].as_deref().unwrap().contains(&t[1]), "{rep:?}");
    // New ids come from one sequence above every existing one.
    exec(&mut b, "INSERT INTO t9 (v) VALUES ('b-new')").await;
    let nb: u64 = b
        .query_first("SELECT LAST_INSERT_ID()")
        .await
        .unwrap()
        .unwrap();
    exec(&mut a, "INSERT INTO t9 (v) VALUES ('a-new')").await;
    let na: u64 = a
        .query_first("SELECT LAST_INSERT_ID()")
        .await
        .unwrap()
        .unwrap();
    assert!(nb > 3 && na > 3 && na != nb, "{nb} {na}");
    // Existing rows unchanged: b still sees its own 1 and 2.
    let v: Vec<Option<String>> = rows(&mut b, "SELECT v FROM t9 WHERE id IN (1, 2) ORDER BY id")
        .await
        .into_iter()
        .map(|r| r[0].clone())
        .collect();
    assert_eq!(v, vec![Some("b0".into()), Some("b1".into())]);
    exec(&mut a, "DROP SHARE igm").await;
    for x in [&mut b, &mut a] {
        exec(x, &format!("DROP DATABASE {db}")).await;
    }
}

/// Waits until `sql`'s rows satisfy `ok` (the copy worker orphans
/// overrides across tenants asynchronously).
async fn until_rows(
    c: &mut Conn,
    sql: &str,
    ok: impl Fn(&[Vec<Option<String>>]) -> bool,
) -> Vec<Vec<Option<String>>> {
    let t0 = Instant::now();
    loop {
        let got = match c.query::<Row, _>(sql).await {
            Ok(r) => r
                .into_iter()
                .map(|r| r.unwrap().iter().map(text).collect())
                .collect(),
            Err(mysql_async::Error::Server(s)) if s.code == 39889 => Vec::new(),
            Err(e) => panic!("{sql}: {e}"),
        };
        if ok(&got) {
            return got;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(60),
            "{sql}: still {got:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// `sql` once a refusal (`code`) that only lasts while the copy worker
/// catches up has cleared.
async fn passes_after(c: &mut Conn, sql: &str, code: u16) -> u64 {
    let t0 = Instant::now();
    loop {
        match try_exec(c, sql).await {
            Ok(n) => return n,
            Err(e) if e == code && t0.elapsed() < Duration::from_secs(60) => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => panic!("{sql}: {e}"),
        }
    }
}

/// Inheritance round 2 (decision IG7's gaps): an override in a child
/// tenant that the copy worker makes an orphan holds its references as
/// its own, so it gets a claim, and the values its view showed when the
/// parent's record went (not the ones stored when it was written); until
/// the worker has done that, the parent's delete hands the references
/// over. `SPRIG RESET` of an orphan is checked like a delete. Needs the
/// deployment's worker process (it orphans across tenants).
#[tokio::test]
async fn orphans_keep_their_references() {
    let Some(eps) = endpoints("orphans_keep_their_references") else {
        return;
    };
    let t = tenants();
    let mode = mode();
    let mat = mode == "materialized";
    for (pi, ep) in eps.iter().enumerate() {
        let p = if pi == 0 { "my" } else { "ma" };
        let db = format!("ig_or_{p}_{}", if mat { "mat" } else { "rt" });
        let s1 = format!("igo{p}{}", u8::from(mat));
        let sb = format!("igob{p}{}", u8::from(mat));
        let mut c = Vec::new();
        for name in &t {
            c.push(conn(ep, name).await);
        }
        let shares = [(1, sb.as_str()), (0, s1.as_str())];
        cleanup(&mut c, &t, &db, &shares).await;
        for (i, x) in c.iter_mut().enumerate() {
            exec(x, &format!("USE SPACE `{}`", t[i])).await;
            if mat {
                exec(x, "SPRIG SET inherit.merge = 'materialized'").await;
            } else {
                let _ = try_exec(x, "SPRIG RESET inherit.merge").await;
            }
        }
        exec(&mut c[0], &format!("CREATE DATABASE {db}")).await;
        exec(&mut c[0], &format!("USE {db}")).await;
        exec(
            &mut c[0],
            "CREATE TABLE t2 (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(40) NULL)",
        )
        .await;
        exec(
            &mut c[0],
            "CREATE TABLE t1 (id INT AUTO_INCREMENT PRIMARY KEY, t2_id INT NULL, note \
             VARCHAR(40) NULL, CONSTRAINT fk_t2 FOREIGN KEY (t2_id) REFERENCES t2 (id))",
        )
        .await;
        exec(&mut c[0], "ALTER TABLE t1 SPRIG INHERITABLE KEY (id)").await;
        exec(
            &mut c[0],
            &format!("CREATE SHARE {s1} ON t1 TO ALL CHILDREN WITH RESHARE"),
        )
        .await;
        exec(&mut c[1], &format!("USE {db}")).await;
        exec(
            &mut c[1],
            &format!("CREATE SHARE {sb} ON t1 TO ALL CHILDREN WITH RESHARE"),
        )
        .await;
        exec(&mut c[2], &format!("USE {db}")).await;
        let mut ins = Vec::new();
        for v in ["k1", "k2", "k3"] {
            exec(&mut c[0], &format!("INSERT INTO t2 (name) VALUES ('{v}')")).await;
            let k: u64 = c[0]
                .query_first("SELECT LAST_INSERT_ID()")
                .await
                .unwrap()
                .unwrap();
            ins.push(k);
        }
        let (k1, k2, k3) = (ins[0], ins[1], ins[2]);
        exec(
            &mut c[0],
            &format!("INSERT INTO t1 (t2_id, note) VALUES ({k1}, 'a')"),
        )
        .await;
        let r: u64 = c[0]
            .query_first("SELECT LAST_INSERT_ID()")
            .await
            .unwrap()
            .unwrap();

        // A. b and c override a's row R without touching its foreign key;
        //    a then points R at k2 and deletes it.
        for i in [1, 2] {
            until_rows(
                &mut c[i],
                &format!("SELECT id FROM t1 WHERE id = {r}"),
                |g| g.len() == 1,
            )
            .await;
            exec(
                &mut c[i],
                &format!("UPDATE t1 SET note = 'lvl{i}' WHERE id = {r}"),
            )
            .await;
        }
        assert_eq!(
            exec(
                &mut c[0],
                &format!("UPDATE t1 SET t2_id = {k2} WHERE id = {r}"),
            )
            .await,
            1,
            "{p}: a's update of R"
        );
        exec(&mut c[0], &format!("DELETE FROM t1 WHERE id = {r}")).await;
        // Right away (before the worker has made the orphans): refused,
        // the overrides below will hold k2.
        let m = fails(&mut c[0], &format!("DELETE FROM t2 WHERE id = {k2}"), 1451).await;
        eprintln!("{p}/{mode}: a's delete during the handover refused: {m}");
        // The orphans hold k2, what their views showed, not the k1 stored
        // with the overrides. (A materialized copy still holding a's record
        // shows its values for a moment: wait for the orphan; a stale value
        // stays and fails after the wait.)
        for i in [1, 2] {
            let want = vec![Some(k2.to_string()), Some(format!("lvl{i}"))];
            until_rows(
                &mut c[i],
                &format!("SELECT t2_id, note FROM t1 WHERE id = {r}"),
                |g| g.len() == 1 && g[0] == want,
            )
            .await;
        }
        // Their claims refuse a's delete of k2 now; k1 is free.
        let m = fails(&mut c[0], &format!("DELETE FROM t2 WHERE id = {k2}"), 1451).await;
        eprintln!("{p}/{mode}: a's delete after orphaning refused: {m}");
        assert_eq!(
            exec(&mut c[0], &format!("DELETE FROM t2 WHERE id = {k1}")).await,
            1
        );
        // b drops its orphan: c's still references k2.
        exec(&mut c[1], &format!("DELETE FROM t1 WHERE id = {r}")).await;
        fails(&mut c[0], &format!("DELETE FROM t2 WHERE id = {k2}"), 1451).await;
        // c's override, now without a record above, becomes c's orphan
        // (the worker, again filling k2 from b's handover) before c can
        // delete it: until then c's view doesn't show it.
        let mine = format!("\"t2_id\": \"{} (orphaned)\"", t[2]);
        until_rows(
            &mut c[2],
            &format!("SELECT SPRIG_ORIGIN(), t2_id FROM t1 WHERE id = {r}"),
            |g| g.len() == 1 && g[0][0].as_deref().is_some_and(|o| o.contains(&mine)),
        )
        .await;
        let blockers = rows(
            &mut c[0],
            &format!("SPRIG CHECK REFERENCES t2 WHERE id = {k2}"),
        )
        .await;
        assert!(
            blockers
                .iter()
                .any(|b| b[1].as_deref() == Some(t[2].as_str())
                    && b[4].as_deref() == Some(r.to_string().as_str())),
            "{blockers:?}"
        );
        exec(&mut c[2], &format!("DELETE FROM t1 WHERE id = {r}")).await;
        assert_eq!(
            passes_after(&mut c[0], &format!("DELETE FROM t2 WHERE id = {k2}"), 1451).await,
            1
        );

        // B. SPRIG RESET of an orphan someone references is refused.
        until_rows(
            &mut c[1],
            &format!("SELECT id FROM t2 WHERE id = {k3}"),
            |g| g.len() == 1,
        )
        .await;
        exec(
            &mut c[1],
            &format!("UPDATE t2 SET name = 'b3' WHERE id = {k3}"),
        )
        .await;
        assert_eq!(
            exec(&mut c[0], &format!("DELETE FROM t2 WHERE id = {k3}")).await,
            1
        );
        // b's override of k3 becomes b's orphan (with a materialized copy
        // it shows before that, over the copy's record).
        let b_orphan = format!("\"name\": \"{} (orphaned)\"", t[1]);
        until_rows(
            &mut c[1],
            &format!("SELECT SPRIG_ORIGIN(), name FROM t2 WHERE id = {k3}"),
            |g| {
                g.len() == 1
                    && g[0][1].as_deref() == Some("b3")
                    && g[0][0].as_deref().is_some_and(|o| o.contains(&b_orphan))
            },
        )
        .await;
        exec(
            &mut c[1],
            &format!("INSERT INTO t1 (t2_id, note) VALUES ({k3}, 'b->orphan')"),
        )
        .await;
        let b_ref: u64 = c[1]
            .query_first("SELECT LAST_INSERT_ID()")
            .await
            .unwrap()
            .unwrap();
        let m = fails(&mut c[1], "SPRIG RESET t2", 1451).await;
        eprintln!("{p}/{mode}: b's reset of a referenced orphan refused: {m}");
        assert_eq!(
            rows(&mut c[1], &format!("SELECT name FROM t2 WHERE id = {k3}")).await,
            vec![vec![Some("b3".to_owned())]]
        );
        exec(&mut c[1], &format!("DELETE FROM t1 WHERE id = {b_ref}")).await;
        passes_after(&mut c[1], "SPRIG RESET t2", 1451).await;
        assert!(
            rows(&mut c[1], &format!("SELECT name FROM t2 WHERE id = {k3}"))
                .await
                .is_empty()
        );
        eprintln!("{p}/{mode}: orphans keep their references");
        cleanup(&mut c, &t, &db, &shares).await;
    }
}

/// Inheritance round 2 (decision IG5's gap): a type the parent widens
/// while shared (`VARCHAR(10)` → `VARCHAR(60)`) reaches its children and
/// grandchildren at their next statement, so their overrides and own rows
/// can hold the longer values (before: 1406 in the child).
#[tokio::test]
async fn widened_types_reach_children() {
    let Some(eps) = endpoints("widened_types_reach_children") else {
        return;
    };
    let t = tenants();
    let mode = mode();
    let mat = mode == "materialized";
    for (pi, ep) in eps.iter().enumerate() {
        let p = if pi == 0 { "my" } else { "ma" };
        let db = format!("ig_wd_{p}_{}", if mat { "mat" } else { "rt" });
        let s1 = format!("igw{p}{}", u8::from(mat));
        let sb = format!("igwb{p}{}", u8::from(mat));
        let mut c = Vec::new();
        for name in &t {
            c.push(conn(ep, name).await);
        }
        let shares = [(1, sb.as_str()), (0, s1.as_str())];
        cleanup(&mut c, &t, &db, &shares).await;
        for (i, x) in c.iter_mut().enumerate() {
            exec(x, &format!("USE SPACE `{}`", t[i])).await;
            if mat {
                exec(x, "SPRIG SET inherit.merge = 'materialized'").await;
            } else {
                let _ = try_exec(x, "SPRIG RESET inherit.merge").await;
            }
        }
        exec(&mut c[0], &format!("CREATE DATABASE {db}")).await;
        exec(&mut c[0], &format!("USE {db}")).await;
        exec(
            &mut c[0],
            "CREATE TABLE t2 (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(10) NULL, qty \
             SMALLINT NULL)",
        )
        .await;
        exec(&mut c[0], "ALTER TABLE t2 SPRIG INHERITABLE KEY (id)").await;
        exec(
            &mut c[0],
            &format!("CREATE SHARE {s1} ON t2 TO ALL CHILDREN WITH RESHARE"),
        )
        .await;
        exec(&mut c[0], "INSERT INTO t2 (name, qty) VALUES ('short', 1)").await;
        let k: u64 = c[0]
            .query_first("SELECT LAST_INSERT_ID()")
            .await
            .unwrap()
            .unwrap();
        exec(&mut c[1], &format!("USE {db}")).await;
        exec(
            &mut c[1],
            &format!("CREATE SHARE {sb} ON t2 TO ALL CHILDREN WITH RESHARE"),
        )
        .await;
        exec(&mut c[2], &format!("USE {db}")).await;
        until_rows(&mut c[2], "SELECT id FROM t2", |g| g.len() == 1).await;
        // The parent widens two types while shared (additive).
        exec(&mut c[0], "ALTER TABLE t2 MODIFY name VARCHAR(60) NULL").await;
        exec(&mut c[0], "ALTER TABLE t2 MODIFY qty INT NULL").await;
        let long_b = "b".repeat(50);
        let long_c = "c".repeat(50);
        // b's override of a's row and c's own row hold the wider values.
        until_rows(
            &mut c[1],
            &format!("SELECT id FROM t2 WHERE id = {k}"),
            |g| g.len() == 1,
        )
        .await;
        exec(
            &mut c[1],
            &format!("UPDATE t2 SET name = '{long_b}', qty = 100000 WHERE id = {k}"),
        )
        .await;
        settled(
            &mut c[2],
            &format!("INSERT INTO t2 (name, qty) VALUES ('{long_c}', 200000)"),
        )
        .await
        .unwrap();
        let got = until_rows(
            &mut c[2],
            &format!("SELECT name, qty FROM t2 WHERE id = {k}"),
            |g| g.len() == 1 && g[0][0].as_deref() == Some(long_b.as_str()),
        )
        .await;
        assert_eq!(got[0][1].as_deref(), Some("100000"));
        until_rows(
            &mut c[2],
            &format!("SELECT id FROM t2 WHERE name = '{long_c}'"),
            |g| g.len() == 1,
        )
        .await;
        // a's own row is unchanged.
        assert_eq!(
            rows(&mut c[0], &format!("SELECT name FROM t2 WHERE id = {k}")).await,
            vec![vec![Some("short".to_owned())]]
        );
        eprintln!("{p}/{mode}: widened types reach b and c");
        cleanup(&mut c, &t, &db, &shares).await;
    }
}

/// The names `sql` returns (one column), sorted.
async fn names(c: &mut Conn, sql: &str) -> Vec<String> {
    let mut v: Vec<String> = rows(c, sql)
        .await
        .into_iter()
        .filter_map(|r| r[0].clone())
        .collect();
    v.sort();
    v
}

/// Waits until `sql` returns exactly `want` (sorted names), as copies lag.
async fn names_become(c: &mut Conn, sql: &str, want: &[&str]) {
    let want: Vec<String> = want.iter().map(|s| (*s).to_owned()).collect();
    let t0 = Instant::now();
    loop {
        let got = match c.query::<Row, _>(sql).await {
            Ok(r) => {
                let mut v: Vec<String> = r
                    .into_iter()
                    .filter_map(|r| text(&r.unwrap()[0]))
                    .collect();
                v.sort();
                v
            }
            Err(mysql_async::Error::Server(s)) if s.code == 39889 => Vec::new(),
            Err(e) => panic!("{sql}: {e}"),
        };
        if got == want {
            return;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(60),
            "{sql}: {got:?}, want {want:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Kiran's example with a typed table (inheritance plan step 6b and Q2):
/// `a`'s `vehicle` with subtype `truck` (`WHERE vtype = 'TRUCK'`, single
/// table and joined) and `trip` referencing `vehicle` by foreign key, made
/// inheritable and shared. `b` and `c` receive the tables and the types;
/// `b` adds a row with a value no type declares (`'SMALLTRUCK'`) and
/// overrides a truck's payload; `c` declares its own subtype `smalltruck`
/// and so sees `b`'s row as a smalltruck (and a truck), which `a` and `b`
/// don't; ids come from the tree's sequence; `c`'s trip referencing `a`'s
/// truck refuses `a`'s delete (1451) until it is gone.
#[tokio::test]
async fn typed_tables_in_groups() {
    let Some(eps) = endpoints("typed_tables_in_groups") else {
        return;
    };
    let t = tenants();
    let mode = mode();
    let mat = mode == "materialized";
    for (pi, ep) in eps.iter().enumerate() {
        let p = if pi == 0 { "my" } else { "ma" };
        for storage in ["single_table", "joined"] {
            let st = if storage == "joined" { "j" } else { "s" };
            let db = format!("ig_ty_{p}_{st}_{}", if mat { "mat" } else { "rt" });
            let sa = format!("igty{p}{st}{}", u8::from(mat));
            let sb = format!("igtyb{p}{st}{}", u8::from(mat));
            let mut c = Vec::new();
            for name in &t {
                c.push(conn(ep, name).await);
            }
            let shares = [(1, sb.as_str()), (0, sa.as_str())];
            cleanup(&mut c, &t, &db, &shares).await;
            for (i, x) in c.iter_mut().enumerate() {
                exec(x, &format!("USE SPACE `{}`", t[i])).await;
                if mat {
                    exec(x, "SPRIG SET inherit.merge = 'materialized'").await;
                } else {
                    let _ = try_exec(x, "SPRIG RESET inherit.merge").await;
                }
            }
            let t0 = Instant::now();
            // a: the typed tree and a table referencing it.
            exec(&mut c[0], &format!("CREATE DATABASE {db}")).await;
            exec(&mut c[0], &format!("USE {db}")).await;
            exec(
                &mut c[0],
                "CREATE TABLE vehicle (id INT AUTO_INCREMENT PRIMARY KEY, vtype VARCHAR(20) NOT \
                 NULL, name VARCHAR(40) NULL)",
            )
            .await;
            exec(
                &mut c[0],
                &format!(
                    "CREATE TABLE truck (payload_kg INT NULL) INHERITS (vehicle) WHERE vtype = \
                     'TRUCK' WITH (SPRIG_STORAGE = '{storage}')"
                ),
            )
            .await;
            exec(
                &mut c[0],
                "CREATE TABLE trip (id INT AUTO_INCREMENT PRIMARY KEY, vehicle_id INT NULL, note \
                 VARCHAR(40) NULL, CONSTRAINT fk_vehicle FOREIGN KEY (vehicle_id) REFERENCES \
                 vehicle (id))",
            )
            .await;
            exec(&mut c[0], "INSERT INTO vehicle (vtype, name) VALUES ('VAN', 'a-van')").await;
            exec(
                &mut c[0],
                "INSERT INTO truck (name, payload_kg) VALUES ('a-truck', 1000), ('a-truck2', 3000)",
            )
            .await;
            exec(&mut c[0], "ALTER TABLE vehicle SPRIG INHERITABLE KEY (id)").await;
            exec(
                &mut c[0],
                &format!("CREATE SHARE {sa} ON vehicle TO ALL CHILDREN WITH RESHARE"),
            )
            .await;
            let a_truck2: u64 = c[0]
                .query_first("SELECT id FROM truck WHERE name = 'a-truck2'")
                .await
                .unwrap()
                .unwrap();
            // b: the tables and types arrive without DDL.
            exec(&mut c[1], &format!("USE {db}")).await;
            let tb = tables(&mut c[1]).await;
            assert!(
                tb.contains("vehicle") && tb.contains("truck") && tb.contains("trip"),
                "b: {tb:?}"
            );
            names_become(&mut c[1], "SELECT name FROM truck", &["a-truck", "a-truck2"]).await;
            names_become(
                &mut c[1],
                "SELECT name FROM vehicle",
                &["a-truck", "a-truck2", "a-van"],
            )
            .await;
            // b adds an undeclared value (open values, Q2) and a truck, and
            // overrides a's truck's payload.
            exec(
                &mut c[1],
                "INSERT INTO vehicle (vtype, name) VALUES ('SMALLTRUCK', 'b-small')",
            )
            .await;
            let b_small: u64 = c[1]
                .query_first("SELECT LAST_INSERT_ID()")
                .await
                .unwrap()
                .unwrap();
            exec(
                &mut c[1],
                "INSERT INTO truck (name, payload_kg) VALUES ('b-truck', 2000)",
            )
            .await;
            exec(
                &mut c[1],
                "UPDATE truck SET payload_kg = 1500 WHERE name = 'a-truck'",
            )
            .await;
            exec(
                &mut c[1],
                &format!("CREATE SHARE {sb} ON vehicle TO ALL CHILDREN WITH RESHARE"),
            )
            .await;
            assert_eq!(
                names(&mut c[1], "SELECT name FROM truck").await,
                ["a-truck", "a-truck2", "b-truck"]
            );
            assert_eq!(
                names(&mut c[1], "SELECT name FROM ONLY vehicle").await,
                ["a-van", "b-small"]
            );
            assert_eq!(
                rows(&mut c[1], "SELECT payload_kg FROM truck WHERE name = 'a-truck'").await,
                vec![vec![Some("1500".to_owned())]]
            );
            // a sees none of it.
            assert_eq!(
                rows(&mut c[0], "SELECT payload_kg FROM truck WHERE name = 'a-truck'").await,
                vec![vec![Some("1000".to_owned())]]
            );
            assert_eq!(
                names(&mut c[0], "SELECT name FROM vehicle").await,
                ["a-truck", "a-truck2", "a-van"]
            );
            // c: b's view, then c's own subtype adopts b's row.
            exec(&mut c[2], &format!("USE {db}")).await;
            names_become(
                &mut c[2],
                "SELECT name FROM truck",
                &["a-truck", "a-truck2", "b-truck"],
            )
            .await;
            names_become(&mut c[2], "SELECT name FROM ONLY vehicle", &["a-van", "b-small"])
                .await;
            exec(
                &mut c[2],
                "CREATE TABLE smalltruck (axles INT NULL) INHERITS (truck) WHERE vtype = \
                 'SMALLTRUCK'",
            )
            .await;
            assert_eq!(
                names(&mut c[2], "SELECT name FROM smalltruck").await,
                ["b-small"]
            );
            assert_eq!(
                names(&mut c[2], "SELECT name FROM truck").await,
                ["a-truck", "a-truck2", "b-small", "b-truck"]
            );
            assert_eq!(
                names(&mut c[2], "SELECT name FROM ONLY vehicle").await,
                ["a-van"]
            );
            assert_eq!(
                rows(&mut c[2], "SELECT payload_kg FROM truck WHERE name = 'a-truck'").await,
                vec![vec![Some("1500".to_owned())]]
            );
            exec(
                &mut c[2],
                "INSERT INTO smalltruck (name, payload_kg, axles) VALUES ('c-small', 10, 2)",
            )
            .await;
            let c_small: u64 = c[2]
                .query_first("SELECT LAST_INSERT_ID()")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                rows(
                    &mut c[2],
                    &format!("SELECT vtype, axles FROM smalltruck WHERE id = {c_small}")
                )
                .await,
                vec![vec![Some("SMALLTRUCK".to_owned()), Some("2".to_owned())]]
            );
            // b and a don't know smalltruck: b's row stays a vehicle there.
            assert_eq!(
                names(&mut c[1], "SELECT name FROM truck").await,
                ["a-truck", "a-truck2", "b-truck"]
            );
            fails(&mut c[1], "SELECT * FROM smalltruck", 1146).await;
            // Ids: one sequence for the tree.
            let ids: BTreeSet<u64> = rows(&mut c[2], "SELECT id FROM vehicle")
                .await
                .into_iter()
                .map(|r| r[0].clone().unwrap().parse().unwrap())
                .collect();
            assert_eq!(ids.len(), 6, "c's vehicles: {ids:?}");
            assert!(ids.contains(&b_small) && ids.contains(&c_small));
            // Foreign keys against each level's merged view.
            exec(
                &mut c[2],
                &format!("INSERT INTO trip (vehicle_id, note) VALUES ({b_small}, 'c->b-small')"),
            )
            .await;
            exec(
                &mut c[2],
                &format!("INSERT INTO trip (vehicle_id, note) VALUES ({a_truck2}, 'c->a-truck2')"),
            )
            .await;
            fails(
                &mut c[1],
                &format!("INSERT INTO trip (vehicle_id, note) VALUES ({c_small}, 'b->c')"),
                1452,
            )
            .await;
            // a's delete of its truck that c references: refused.
            let m = fails(
                &mut c[0],
                &format!("DELETE FROM truck WHERE id = {a_truck2}"),
                1451,
            )
            .await;
            eprintln!("{p}/{mode}/{storage}: a's delete refused: {m}");
            exec(
                &mut c[2],
                &format!("DELETE FROM trip WHERE vehicle_id = {a_truck2}"),
            )
            .await;
            exec(&mut c[0], &format!("DELETE FROM truck WHERE id = {a_truck2}")).await;
            names_become(
                &mut c[2],
                "SELECT name FROM truck",
                &["a-truck", "b-small", "b-truck", "c-small"],
            )
            .await;
            // A subtype a declares later reaches b and c.
            exec(
                &mut c[0],
                "CREATE TABLE van (doors INT NULL) INHERITS (vehicle) WHERE vtype = 'VAN'",
            )
            .await;
            assert_eq!(names(&mut c[0], "SELECT name FROM van").await, ["a-van"]);
            names_become(&mut c[1], "SELECT name FROM van", &["a-van"]).await;
            names_become(&mut c[2], "SELECT name FROM van", &["a-van"]).await;
            eprintln!(
                "{p}/{mode}/{storage}: typed tree inherited a > b > c, c's own subtype, {} ms",
                t0.elapsed().as_millis()
            );
            cleanup(&mut c, &t, &db, &shares).await;
        }
    }
}
