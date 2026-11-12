//! bugfix-43 and bugfix-44 (decisions/bugfix-interim.md; Studio's ST63):
//! a child tenant and the databases its parent shares, through a gateway.
//!
//! 1. bugfix-43: a parent shares a new database; before the child runs
//!    any statement, a client connects to the child with that database
//!    as its default (the handshake's database, the same path as
//!    `COM_INIT_DB`), and another prepares a statement naming it
//!    (`COM_STMT_PREPARE`). Before, both got 1049 "Unknown database" until
//!    the child ran a text statement such as `SHOW DATABASES`: only
//!    `COM_QUERY` brought the child up to its parent.
//! 2. bugfix-44: the parent ends the share and drops the database. The
//!    child keeps its database of that name with its own rows (Kiran's
//!    `inherit.parent_delete = keep`): it reads and writes them, the
//!    parent's records are gone from it, and it can drop the database.
//! 3. bugfix-51 (the lead's choice on bugfix-44): when the parent drops a
//!    share, the child's overrides of the parent's records become the
//!    child's own rows, visible as written (before, they were hidden), and
//!    the share view `__sprig_share_<id>` leaves the child's schema at once.
//!    With `SPRIG_TEST_NEWSHARE_GRANDCHILD` (a child of the child), the
//!    grandchild's overrides of records that came from the parent become
//!    its own too; with `SPRIG_TEST_NEWSHARE_BACKEND` (`host:port` of the
//!    cluster, root password in `SPRIG_TEST_NEWSHARE_BACKEND_PASSWORD`) the
//!    test also checks the view is gone.
//!
//! ```sh
//! SPRIG_TEST_NEWSHARE_GATEWAY=127.0.0.1:58306 \
//! SPRIG_TEST_NEWSHARE_PARENT=acme SPRIG_TEST_NEWSHARE_CHILD=bf11kid \
//!   cargo test -p sprig-gateway --test inherit_newshare_live -- --nocapture --test-threads 1
//! ```
//!
//! The child is a child tenant of the parent on the same cluster; both
//! have the user `app` (password `sprig-dev-app`). Each run uses its own
//! database names and drops them.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use std::time::{SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};

struct Env {
    addr: String,
    parent: String,
    child: String,
}

fn env() -> Option<Env> {
    let get = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    Some(Env {
        addr: get("SPRIG_TEST_NEWSHARE_GATEWAY")?,
        parent: get("SPRIG_TEST_NEWSHARE_PARENT")?,
        child: get("SPRIG_TEST_NEWSHARE_CHILD")?,
    })
}

fn opts(addr: &str, tenant: &str, db: Option<&str>) -> OptsBuilder {
    let (host, port) = addr.rsplit_once(':').unwrap();
    OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.app")))
        .pass(Some("sprig-dev-app"))
        .db_name(db)
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ))
}

async fn conn(addr: &str, tenant: &str) -> Conn {
    Conn::new(opts(addr, tenant, None)).await.expect("login")
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

fn suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        % 1_000_000_000
}

/// The parent makes database `db` with an inheritable `t1` and shares it
/// to all its children.
async fn share(p: &mut Conn, db: &str) {
    share_with(p, db, "").await;
}

/// [`share`], with `opts` after the share's target (`WITH RESHARE`).
async fn share_with(p: &mut Conn, db: &str, opts: &str) {
    ok(p, &format!("CREATE DATABASE {db}")).await;
    ok(
        p,
        &format!("CREATE TABLE {db}.t1 (id INT PRIMARY KEY, v VARCHAR(40))"),
    )
    .await;
    ok(p, &format!("USE {db}")).await;
    ok(p, "ALTER TABLE t1 SPRIG INHERITABLE KEY (id)").await;
    ok(
        p,
        "INSERT INTO t1 VALUES (1, 'parent one'), (2, 'parent two')",
    )
    .await;
    ok(p, &format!("CREATE SHARE {db}_s ON t1 TO ALL CHILDREN {opts}")).await;
}

#[tokio::test]
async fn a_new_share_is_usable_before_the_childs_first_statement() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        return;
    };
    let db = format!("bf11ns{}", suffix());
    let mut p = conn(&e.addr, &e.parent).await;
    share(&mut p, &db).await;

    // The handshake's database (as COM_INIT_DB): the child's first
    // command on this gateway since the share.
    let r = Conn::new(opts(&e.addr, &e.child, Some(&db))).await;
    let mut c = match r {
        Ok(c) => c,
        Err(mysql_async::Error::Server(s)) => {
            panic!("connecting with database {db}: {} {}", s.code, s.message)
        }
        Err(err) => panic!("connecting with database {db}: {err}"),
    };
    let rows = ok(&mut c, "SELECT DATABASE(), COUNT(*) FROM t1").await;
    assert_eq!(rows[0][0].as_deref(), Some(db.as_str()));
    assert_eq!(rows[0][1].as_deref(), Some("2"), "the parent's records");
    drop(c);

    // COM_STMT_PREPARE first: a second new database. A prepared statement
    // on an inheritable table is refused by chapter 42 (39xxx), but never
    // with 1049 for a database the child has.
    let db2 = format!("{db}b");
    share(&mut p, &db2).await;
    let mut c = conn(&e.addr, &e.child).await;
    match c
        .prep(format!("SELECT COUNT(*) FROM {db2}.t1 WHERE id > ?"))
        .await
    {
        Ok(_) => println!("prepared on {db2}.t1"),
        Err(mysql_async::Error::Server(s)) => {
            assert_ne!(s.code, 1049, "{db2}: {}", s.message);
            println!(
                "prepare on {db2}.t1 refused as chapter 42 says: {} {}",
                s.code, s.message
            );
        }
        Err(err) => panic!("prepare: {err}"),
    }
    let dbs: Vec<String> = ok(&mut c, "SHOW DATABASES")
        .await
        .into_iter()
        .filter_map(|r| r[0].clone())
        .collect();
    assert!(dbs.contains(&db) && dbs.contains(&db2), "{dbs:?}");
    drop(c);

    for d in [&db, &db2] {
        ok(&mut p, &format!("USE {d}")).await;
        ok(&mut p, &format!("DROP SHARE {d}_s")).await;
        ok(&mut p, &format!("DROP DATABASE {d}")).await;
    }
    let mut c = conn(&e.addr, &e.child).await;
    for d in [&db, &db2] {
        ok(&mut c, &format!("DROP DATABASE IF EXISTS {d}")).await;
    }
}

#[tokio::test]
async fn a_child_keeps_its_rows_when_the_parent_drops_a_shared_database() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        return;
    };
    let db = format!("bf11nk{}", suffix());
    let mut p = conn(&e.addr, &e.parent).await;
    share(&mut p, &db).await;
    let mut c = conn(&e.addr, &e.child).await;
    ok(&mut c, &format!("USE {db}")).await;
    ok(&mut c, "INSERT INTO t1 VALUES (10, 'child own')").await;
    let rows = ok(&mut c, "SELECT id FROM t1 ORDER BY id").await;
    assert_eq!(rows.len(), 3, "two inherited and one own: {rows:?}");

    // The parent ends the share (its DROP DATABASE is refused while the
    // table is shared, 39895) and drops the database.
    assert_eq!(
        sql(&mut p, &format!("DROP DATABASE {db}"))
            .await
            .err()
            .map(|e| e.0),
        Some(39895)
    );
    ok(&mut p, &format!("USE {db}")).await;
    ok(&mut p, &format!("DROP SHARE {db}_s")).await;
    ok(&mut p, &format!("DROP DATABASE {db}")).await;

    // The child keeps its database and its own row; the parent's records
    // are gone from it.
    let rows = ok(&mut c, &format!("SELECT id, v FROM {db}.t1 ORDER BY id")).await;
    assert_eq!(
        rows,
        vec![vec![Some("10".to_owned()), Some("child own".to_owned())]]
    );
    ok(&mut c, &format!("INSERT INTO {db}.t1 VALUES (11, 'after')")).await;
    ok(
        &mut c,
        &format!("UPDATE {db}.t1 SET v = 'changed' WHERE id = 10"),
    )
    .await;
    let rows = ok(&mut c, &format!("SELECT COUNT(*) FROM {db}.t1")).await;
    assert_eq!(rows[0][0].as_deref(), Some("2"));
    // A new connection with it as the default database works too.
    let mut c2 = Conn::new(opts(&e.addr, &e.child, Some(&db)))
        .await
        .expect("the child's own database");
    assert_eq!(
        ok(&mut c2, "SELECT COUNT(*) FROM t1").await[0][0].as_deref(),
        Some("2")
    );
    drop(c2);
    // And the child can drop it.
    ok(&mut c, &format!("DROP DATABASE {db}")).await;
    let dbs: Vec<String> = ok(&mut c, "SHOW DATABASES")
        .await
        .into_iter()
        .filter_map(|r| r[0].clone())
        .collect();
    assert!(!dbs.contains(&db), "{dbs:?}");
}

/// `SELECT id, v FROM <db>.t1 ORDER BY id` as `(id, v)` text pairs.
async fn rows_of(c: &mut Conn, db: &str) -> Vec<(String, String)> {
    ok(c, &format!("SELECT id, v FROM {db}.t1 ORDER BY id"))
        .await
        .into_iter()
        .map(|r| (r[0].clone().unwrap(), r[1].clone().unwrap_or_default()))
        .collect()
}

fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
    v.iter()
        .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
        .collect()
}

/// The share views left in the physical schemas of logical database `db`
/// on the backend, when `SPRIG_TEST_NEWSHARE_BACKEND` names it.
async fn share_views(db: &str) -> Option<Vec<String>> {
    let addr = std::env::var("SPRIG_TEST_NEWSHARE_BACKEND").ok()?;
    let pw = std::env::var("SPRIG_TEST_NEWSHARE_BACKEND_PASSWORD").ok()?;
    let (host, port) = addr.rsplit_once(':').unwrap();
    let mut b = Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some("root"))
            .pass(Some(pw)),
    )
    .await
    .expect("backend login");
    let v: Vec<(String, String)> = b
        .query(format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_NAME \
             LIKE '\\_\\_sprig\\_share\\_%' AND TABLE_SCHEMA LIKE '%{db}'"
        ))
        .await
        .unwrap();
    drop(b);
    Some(v.into_iter().map(|(s, t)| format!("{s}.{t}")).collect())
}

#[tokio::test]
async fn a_dropped_share_leaves_the_childs_overrides_as_its_own_rows() {
    let Some(e) = env() else {
        eprintln!("SPRIG_TEST_NEWSHARE_* not set; skipped");
        return;
    };
    let gk = std::env::var("SPRIG_TEST_NEWSHARE_GRANDCHILD")
        .ok()
        .filter(|s| !s.is_empty());
    let db = format!("bf12ds{}", suffix());
    let mut p = conn(&e.addr, &e.parent).await;
    share_with(&mut p, &db, if gk.is_some() { "WITH RESHARE" } else { "" }).await;
    let mut c = conn(&e.addr, &e.child).await;
    ok(&mut c, &format!("USE {db}")).await;
    // The child overrides the parent's record 1 and adds record 10.
    ok(&mut c, "UPDATE t1 SET v = 'kid ovr' WHERE id = 1").await;
    ok(&mut c, "INSERT INTO t1 VALUES (10, 'kid own')").await;
    assert_eq!(
        rows_of(&mut c, &db).await,
        pairs(&[("1", "kid ovr"), ("2", "parent two"), ("10", "kid own")])
    );
    let mut g = None;
    if let Some(gk) = &gk {
        ok(&mut c, &format!("CREATE SHARE {db}_k ON t1 TO ALL CHILDREN")).await;
        let mut gc = conn(&e.addr, gk).await;
        ok(&mut gc, &format!("USE {db}")).await;
        // The grandchild overrides the parent's record 2 (through the
        // child) and the child's override of record 1.
        ok(&mut gc, "UPDATE t1 SET v = 'gk ovr 2' WHERE id = 2").await;
        ok(&mut gc, "UPDATE t1 SET v = 'gk ovr 1' WHERE id = 1").await;
        assert_eq!(
            rows_of(&mut gc, &db).await,
            pairs(&[("1", "gk ovr 1"), ("2", "gk ovr 2"), ("10", "kid own")])
        );
        g = Some(gc);
    }
    if let Some(v) = share_views(&db).await {
        assert!(!v.is_empty(), "no share view before the drop");
        println!("share views before the drop: {v:?}");
    }

    ok(&mut p, &format!("USE {db}")).await;
    ok(&mut p, &format!("DROP SHARE {db}_s")).await;

    // The child's override stays, as written; the parent's record 2 goes.
    let after = rows_of(&mut c, &db).await;
    println!("the child after the drop: {after:?}");
    assert_eq!(after, pairs(&[("1", "kid ovr"), ("10", "kid own")]));
    if let Some(gc) = g.as_mut() {
        let after = rows_of(gc, &db).await;
        println!("the grandchild after the drop: {after:?}");
        assert_eq!(
            after,
            pairs(&[("1", "gk ovr 1"), ("2", "gk ovr 2"), ("10", "kid own")])
        );
    }
    if let Some(v) = share_views(&db).await {
        println!("share views after the drop: {v:?}");
        // The child's own share to the grandchild keeps its view; the
        // dropped share's view is gone.
        assert_eq!(v.len(), usize::from(gk.is_some()), "{v:?}");
    }
    // The row is the child's own: it updates and deletes as one.
    ok(&mut c, "UPDATE t1 SET v = 'kid changed' WHERE id = 1").await;
    assert_eq!(
        rows_of(&mut c, &db).await,
        pairs(&[("1", "kid changed"), ("10", "kid own")])
    );

    ok(&mut p, &format!("DROP DATABASE {db}")).await;
    if let Some(mut gc) = g {
        ok(&mut gc, &format!("DROP DATABASE IF EXISTS {db}")).await;
        ok(&mut c, &format!("DROP SHARE {db}_k")).await;
    }
    ok(&mut c, &format!("DROP DATABASE {db}")).await;
}
