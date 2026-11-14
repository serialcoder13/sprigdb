//! `Plan::Graph`: `GRAPH_TABLE` joined with
//! sharded SQL tables whose shards sit on two clusters, through a gateway,
//! compared with the same statements on one unsharded MariaDB.
//!
//! The test makes, in its own tenant (one without table groups, since the
//! advisor shards only such tenants):
//! - `orders` (sharded by `cust`, an integer) and `customers` (sharded by
//!   `email`, a string), through `SPRIG PIN`, an advisor run and apply, a
//!   split and a move of one shard to the second cluster;
//! - a graph of 60 `Person` nodes with `KNOWS` relationships, on two
//!   shards, one moved to the second cluster;
//! - the same data on the reference MariaDB as plain tables (`persons`,
//!   `knows`, `orders`, `customers` in the schema `r8six_ref`).
//!
//! Each statement runs through the gateway with `GRAPH_TABLE (...)` and on
//! the reference with the equivalent SQL join in its place; the answers
//! must be equal row for row.
//!
//! ```sh
//! export SPRIG_TEST_SQL_ADDR=127.0.0.1:28306 SPRIG_TEST_BOLT_ADDR=127.0.0.1:28687
//! export SPRIG_TEST_GT_USER=globex.app SPRIG_TEST_BOLT_PASSWORD=sprig-dev-app
//! export SPRIG_TEST_SPRIGCTL=target/.../sprigctl SPRIGCTL_ENDPOINT=http://127.0.0.1:28480
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! export SPRIG_TEST_GT_TENANT=globex SPRIG_TEST_GT_MOVE_TO=dc-a-c2
//! cargo test -p sprig-graph --test graph_table_live -- --nocapture
//! ```
//!
//! Without the variables the test skips.

#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::many_single_char_names,
    clippy::type_complexity
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use sprig_proto_bolt::PackValue;
use sprig_proto_bolt::client::{BoltClient, ClientError};
use tokio::net::TcpStream;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

async fn bolt(addr: &str, user: &str, pw: &str) -> Result<BoltClient<TcpStream>, ClientError> {
    let s = TcpStream::connect(addr).await.map_err(|e| ClientError {
        code: String::new(),
        message: e.to_string(),
    })?;
    let mut c = BoltClient::handshake(s, None).await?;
    c.login(user, pw).await?;
    Ok(c)
}

async fn gateway(addr: &str, user: &str, pw: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').expect("host:port");
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().expect("port"))
        .user(Some(user.to_owned()))
        .pass(Some(pw.to_owned()))
        .ssl_opts(Some(
            mysql_async::SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        ));
    mysql_async::Conn::new(opts).await.expect("gateway login")
}

fn sprigctl(bin: &str, args: &[&str]) -> serde_json::Value {
    let out = std::process::Command::new(bin)
        .arg("-o")
        .arg("json")
        .args(args)
        .output()
        .expect("sprigctl");
    assert!(
        out.status.success(),
        "sprigctl {args:?}: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or(serde_json::Value::Null)
}

async fn wait_job(bin: &str, id: &str) -> Duration {
    let t = Instant::now();
    loop {
        let j = sprigctl(bin, &["job", "show", id]);
        let state = j["state"].as_str().unwrap_or_default().to_owned();
        if ["post_cutover", "retaining", "cleaning", "done"].contains(&state.as_str()) {
            return t.elapsed();
        }
        assert!(
            !["rolling_back", "rolled_back", "failed", "cancelled"].contains(&state.as_str()),
            "job {id}: {j}"
        );
        assert!(
            t.elapsed() < Duration::from_secs(600),
            "job {id} stuck: {j}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// `(group, [(shard, cluster)])` of every group `sprigctl shard list` shows.
fn groups(bin: &str, tenant: &str) -> Vec<(String, u64, Vec<(u64, String)>)> {
    let l = sprigctl(bin, &["shard", "list", tenant]);
    l.as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|g| {
            (
                g["name"].as_str().unwrap_or_default().to_owned(),
                g["group"].as_u64().unwrap_or(0),
                g["shards"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .map(|s| {
                        (
                            s["shard"].as_u64().unwrap_or(0),
                            s["cluster"].as_str().unwrap_or_default().to_owned(),
                        )
                    })
                    .collect(),
            )
        })
        .collect()
}

/// Rows as text, in the order they came.
async fn rows(c: &mut mysql_async::Conn, q: &str) -> Result<Vec<String>, String> {
    let r: Vec<mysql_async::Row> = c.query(q).await.map_err(|e| format!("{q}: {e}"))?;
    Ok(r.into_iter()
        .map(|row| {
            (0..row.len())
                .map(|i| match row.as_ref(i) {
                    Some(mysql_async::Value::NULL) | None => "NULL".to_owned(),
                    Some(mysql_async::Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
                    Some(v) => v.as_sql(true).trim_matches('\'').to_owned(),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect())
}

const PERSONS: i64 = 60;

fn email(i: i64) -> String {
    format!("p{i}@example.com")
}

#[tokio::test]
async fn graph_table_joins_sharded_tables_on_two_clusters() {
    let (Some(sql), Some(baddr), Some(user), Some(pw), Some(ctl), Some(reference)) = (
        env("SPRIG_TEST_SQL_ADDR"),
        env("SPRIG_TEST_BOLT_ADDR"),
        env("SPRIG_TEST_GT_USER"),
        env("SPRIG_TEST_BOLT_PASSWORD"),
        env("SPRIG_TEST_SPRIGCTL"),
        env("SPRIG_TEST_MARIADB_SINGLE"),
    ) else {
        eprintln!(
            "skipped: SPRIG_TEST_SQL_ADDR / SPRIG_TEST_BOLT_ADDR / SPRIG_TEST_GT_USER / \
             SPRIG_TEST_BOLT_PASSWORD / SPRIG_TEST_SPRIGCTL / SPRIG_TEST_MARIADB_SINGLE not set"
        );
        return;
    };
    let tenant = env("SPRIG_TEST_GT_TENANT").unwrap_or_else(|| "globex".into());
    let move_to = env("SPRIG_TEST_GT_MOVE_TO").unwrap_or_else(|| "dc-a-c2".into());
    // The data: persons i (0..60) know (i+1) % 60 and (i*7) % 60; person
    // i's customer row has tier i % 5; each person has orders
    // (i, i*4 + 0..i%4) with amounts (the back-fill needs a one-column
    // primary key).
    let mut orders = Vec::new();
    for i in 0..PERSONS {
        for k in 0..(i % 4) {
            orders.push(format!(
                "({i}, {}, {}.{:02}, 'n{}')",
                i * 4 + k,
                i * 3 + k,
                (i * 7 + k) % 100,
                i % 3
            ));
        }
    }
    let customers: Vec<String> = (0..PERSONS)
        .filter(|i| i % 6 != 5)
        .map(|i| format!("('{}', {})", email(i), i % 5))
        .collect();
    let knows: Vec<(i64, i64)> = (0..PERSONS)
        .flat_map(|i| [(i, (i + 1) % PERSONS), (i, (i * 7) % PERSONS)])
        .filter(|(a, b)| a != b)
        .collect();
    let ddl = [
        "CREATE TABLE orders (cust INT NOT NULL, id INT NOT NULL PRIMARY KEY, amount DECIMAL(10,2), note VARCHAR(10), KEY (cust))",
        "CREATE TABLE customers (email VARCHAR(64) NOT NULL PRIMARY KEY, tier INT)",
    ];
    // Reference: one unsharded MariaDB.
    let mut r = mysql_async::Conn::new(mysql_async::Opts::from_url(&reference).expect("url"))
        .await
        .expect("reference login");
    r.query_drop("DROP DATABASE IF EXISTS r8six_ref")
        .await
        .unwrap();
    r.query_drop("CREATE DATABASE r8six_ref").await.unwrap();
    r.query_drop("USE r8six_ref").await.unwrap();
    for d in ddl {
        r.query_drop(d).await.unwrap();
    }
    r.query_drop(
        "CREATE TABLE persons (id BIGINT PRIMARY KEY, name VARCHAR(20), email VARCHAR(64))",
    )
    .await
    .unwrap();
    r.query_drop("CREATE TABLE knows (src BIGINT, dst BIGINT, w BIGINT)")
        .await
        .unwrap();
    r.query_drop(format!("INSERT INTO orders VALUES {}", orders.join(", ")))
        .await
        .unwrap();
    r.query_drop(format!(
        "INSERT INTO customers VALUES {}",
        customers.join(", ")
    ))
    .await
    .unwrap();
    let persons: Vec<String> = (0..PERSONS)
        .map(|i| format!("({i}, 'n{i}', '{}')", email(i)))
        .collect();
    r.query_drop(format!("INSERT INTO persons VALUES {}", persons.join(", ")))
        .await
        .unwrap();
    let kv: Vec<String> = knows
        .iter()
        .map(|(a, b)| format!("({a}, {b}, {})", a * 100 + b))
        .collect();
    r.query_drop(format!("INSERT INTO knows VALUES {}", kv.join(", ")))
        .await
        .unwrap();

    // The gateway: tables, pins, the advisor, a split and a move.
    let mut g = gateway(&sql, &user, &pw).await;
    g.query_drop("CREATE DATABASE IF NOT EXISTS r8six_gt")
        .await
        .unwrap();
    g.query_drop("USE r8six_gt").await.unwrap();
    let have: Vec<String> = g.query("SHOW TABLES").await.unwrap();
    if have.is_empty() {
        for d in ddl {
            g.query_drop(d).await.unwrap();
        }
        g.query_drop(format!("INSERT INTO orders VALUES {}", orders.join(", ")))
            .await
            .unwrap();
        g.query_drop(format!(
            "INSERT INTO customers VALUES {}",
            customers.join(", ")
        ))
        .await
        .unwrap();
    }
    let started = Instant::now();
    // Graph groups (`graph_<id>`) don't count: the tables need theirs.
    let table_groups: Vec<_> = groups(&ctl, &tenant)
        .into_iter()
        .filter(|(n, _, _)| !n.starts_with("graph_"))
        .collect();
    if table_groups.iter().all(|(_, _, s)| s.is_empty()) {
        g.query_drop("SPRIG PIN orders SHARD KEY (cust)")
            .await
            .unwrap();
        g.query_drop("SPRIG PIN customers SHARD KEY (email)")
            .await
            .unwrap();
        let run = sprigctl(&ctl, &["advisor", "run", &tenant]);
        eprintln!("advisor run: {run}");
        let id = run["advisor"]["run"]
            .as_u64()
            .or_else(|| run["advisor"][0].as_u64())
            .or_else(|| run["run"].as_u64())
            .unwrap_or_else(|| panic!("no run id in {run}"));
        let applied = sprigctl(&ctl, &["advisor", "apply", &tenant, &id.to_string()]);
        eprintln!("advisor apply: {applied}");
    }
    // Wait for both tables to be in sharded groups.
    let t = Instant::now();
    let sql_groups = loop {
        let gs: Vec<_> = groups(&ctl, &tenant)
            .into_iter()
            .filter(|(n, _, _)| !n.starts_with("graph_"))
            .collect();
        let ready = sprigctl(&ctl, &["shard", "list", &tenant])
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .all(|g| g["state"].as_str() == Some("active"));
        if ready && gs.iter().map(|(_, _, s)| s.len()).sum::<usize>() >= 2 {
            break gs;
        }
        assert!(t.elapsed() < Duration::from_secs(300), "no groups: {gs:?}");
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    eprintln!("SQL groups after the advisor: {sql_groups:?}");
    for (_, gid, shards) in &sql_groups {
        let gid = gid.to_string();
        let mut shards = shards.clone();
        if shards.len() < 2 {
            let s = shards[0].0.to_string();
            let j = sprigctl(&ctl, &["shard", "split", &tenant, &gid, &s]);
            let id = j["id"].as_str().expect("job id").to_owned();
            eprintln!("split group {gid}: {:?}", wait_job(&ctl, &id).await);
            shards = groups(&ctl, &tenant)
                .into_iter()
                .find(|(_, x, _)| x.to_string() == gid)
                .map(|(_, _, s)| s)
                .unwrap_or_default();
        }
        if shards.iter().all(|(_, c)| *c != move_to) {
            let s = shards.last().expect("a shard").0.to_string();
            let j = sprigctl(
                &ctl,
                &["shard", "move", &tenant, &gid, &s, "--to", &move_to],
            );
            let id = j["id"].as_str().expect("job id").to_owned();
            eprintln!(
                "move group {gid} shard {s}: {:?}",
                wait_job(&ctl, &id).await
            );
        }
    }
    // The graph: two shards, one moved.
    sprigctl(
        &ctl,
        &[
            "settings",
            "set",
            "--scope",
            "database",
            "--tenant",
            &tenant,
            "--db",
            "r8six_gt",
            "sharding.min_shards",
            "2",
            "--reason",
            "graph_table live test",
        ],
    );
    g.query_drop("DROP GRAPH IF EXISTS r8six_gt.g")
        .await
        .unwrap();
    g.query_drop("CREATE GRAPH r8six_gt.g").await.unwrap();
    let mut b = bolt(&baddr, &user, &pw).await.expect("bolt login");
    let db = vec![("db", PackValue::str("r8six_gt.g"))];
    b.query(
        &format!(
            "UNWIND range(0, {}) AS i CREATE (:Person {{id: i, name: 'n' + toString(i), email: 'p' + toString(i) + '@example.com'}})",
            PERSONS - 1
        ),
        vec![],
        db.clone(),
    )
    .await
    .expect("persons");
    b.query(
        &format!(
            "MATCH (a:Person), (b:Person) WHERE a.id <> b.id AND (b.id = (a.id + 1) % {PERSONS} OR b.id = (a.id * 7) % {PERSONS}) CREATE (a)-[:KNOWS {{w: a.id * 100 + b.id}}]->(b)"
        ),
        vec![],
        db.clone(),
    )
    .await
    .expect("knows");
    let gg = groups(&ctl, &tenant)
        .into_iter()
        .find(|(n, _, _)| n.starts_with("graph_"))
        .expect("the graph's group");
    if gg.2.iter().all(|(_, c)| *c != move_to) {
        let s = gg.2.last().expect("a shard").0.to_string();
        let j = sprigctl(
            &ctl,
            &[
                "shard",
                "move",
                &tenant,
                &gg.1.to_string(),
                &s,
                "--to",
                &move_to,
            ],
        );
        let id = j["id"].as_str().expect("job id").to_owned();
        eprintln!("move graph shard {s}: {:?}", wait_job(&ctl, &id).await);
    }
    let layout = groups(&ctl, &tenant);
    eprintln!("layout: {layout:?} (set up in {:?})", started.elapsed());
    for (_, _, shards) in &layout {
        let clusters: std::collections::BTreeSet<&str> =
            shards.iter().map(|(_, c)| c.as_str()).collect();
        assert!(
            clusters.len() >= 2,
            "every group on two clusters: {layout:?}"
        );
    }

    // The statements: GRAPH_TABLE at the gateway, its SQL equivalent on
    // the reference.
    let gt_knows = |cond: &str| {
        format!(
            "GRAPH_TABLE (g MATCH (p IS Person)-[k IS KNOWS]->(f IS Person) WHERE {cond} COLUMNS (p.id AS pid, f.id AS fid, f.email AS femail, k.w AS w))"
        )
    };
    let sql_knows = |cond: &str| {
        format!(
            "(SELECT p.id AS pid, f.id AS fid, f.email AS femail, k.w AS w FROM persons p JOIN knows k ON k.src = p.id JOIN persons f ON f.id = k.dst WHERE {cond})"
        )
    };
    let cases: Vec<(&str, String)> = vec![
        ("join on the integer key", "SELECT x.pid, x.fid, o.id, o.amount FROM {G} AS x JOIN orders o ON o.cust = x.fid ORDER BY x.pid, x.fid, o.id".into()),
        ("aggregate by graph column", "SELECT x.pid, COUNT(*), SUM(o.amount) FROM {G} AS x JOIN orders o ON o.cust = x.fid GROUP BY x.pid ORDER BY x.pid".into()),
        ("join on the string key", "SELECT x.pid, x.femail, c.tier FROM {G} AS x JOIN customers c ON c.email = x.femail ORDER BY x.pid, x.femail".into()),
        ("both groups", "SELECT x.pid, c.tier, o.id FROM {G} AS x JOIN customers c ON c.email = x.femail JOIN orders o ON o.cust = x.fid ORDER BY x.pid, c.tier, o.id, x.fid".into()),
        ("outer join from the graph", "SELECT x.fid, COUNT(o.id) FROM {G} AS x LEFT JOIN orders o ON o.cust = x.fid GROUP BY x.fid ORDER BY x.fid".into()),
        ("top rows by amount", "SELECT o.amount, o.cust, o.id, x.pid FROM {G} AS x JOIN orders o ON o.cust = x.fid ORDER BY o.amount DESC, o.cust, o.id, x.pid LIMIT 7".into()),
        ("a WHERE on the table", "SELECT x.pid, o.note, COUNT(*) FROM {G} AS x, orders o WHERE o.cust = x.fid AND o.note <> 'n1' GROUP BY x.pid, o.note ORDER BY x.pid, o.note".into()),
        ("distinct", "SELECT DISTINCT o.note FROM {G} AS x JOIN orders o ON o.cust = x.fid ORDER BY o.note".into()),
    ];
    let conds = [
        ("p.id < 20", "p.id < 20"),
        ("p.id = 3", "p.id = 3"),
        ("p.id < 0", "p.id < 0"),
        ("p.id >= 0", "p.id >= 0"),
    ];
    let mut compared = 0usize;
    let mut times = Vec::new();
    for (gc, sc) in conds {
        for (name, q) in &cases {
            let gq = q.replace("{G}", &gt_knows(gc));
            let rq = q.replace("{G}", &sql_knows(sc));
            let t = Instant::now();
            let got = rows(&mut g, &gq)
                .await
                .unwrap_or_else(|e| panic!("{name} ({gc}): {e}"));
            times.push(t.elapsed());
            let want = rows(&mut r, &rq)
                .await
                .unwrap_or_else(|e| panic!("reference {name}: {e}"));
            assert_eq!(got, want, "{name} with {gc}:\n{gq}");
            compared += 1;
        }
    }
    // Integer filters on graph columns go into the
    // Cypher; every answer still equals the reference.
    let filtered = [
        "SELECT x.pid, x.fid, o.id FROM {G} AS x JOIN orders o ON o.cust = x.fid WHERE x.fid > 10 AND x.pid <= 40 ORDER BY x.pid, x.fid, o.id",
        "SELECT x.pid, x.fid FROM {G} AS x WHERE x.pid IN (3, 7, 11, 59) AND x.fid <> 4 ORDER BY x.pid, x.fid",
        "SELECT x.pid, COUNT(*) FROM {G} AS x JOIN orders o ON o.cust = x.fid AND x.fid BETWEEN 5 AND 30 GROUP BY x.pid ORDER BY x.pid",
        "SELECT x.fid, COUNT(o.id) FROM {G} AS x LEFT JOIN orders o ON o.cust = x.fid WHERE 20 >= x.fid AND x.femail IS NOT NULL GROUP BY x.fid ORDER BY x.fid",
        "SELECT x.pid, x.w FROM {G} AS x WHERE x.w > 2 AND x.w < 9 ORDER BY x.pid, x.fid",
    ];
    for q in filtered {
        let gq = q.replace("{G}", &gt_knows("p.id >= 0"));
        let rq = q.replace("{G}", &sql_knows("p.id >= 0"));
        let t = Instant::now();
        let got = rows(&mut g, &gq)
            .await
            .unwrap_or_else(|e| panic!("{gq}: {e}"));
        times.push(t.elapsed());
        let want = rows(&mut r, &rq)
            .await
            .unwrap_or_else(|e| panic!("reference {rq}: {e}"));
        assert_eq!(got, want, "pushed filter:\n{gq}");
        compared += 1;
    }
    let ex = rows(
        &mut g,
        &format!(
            "EXPLAIN FORMAT=SPRIG {}",
            filtered[0].replace("{G}", &gt_knows("p.id >= 0"))
        ),
    )
    .await
    .unwrap()
    .join("\n");
    assert!(
        ex.contains("|GraphFilter|") && ex.contains("valueType(f.id)"),
        "{ex}"
    );
    // `?` in a prepared GRAPH_TABLE statement, inside
    // the call (a Cypher value) and outside it (a SQL value).
    let pq = format!(
        "SELECT x.pid, x.fid, o.id FROM {} AS x JOIN orders o ON o.cust = x.fid WHERE o.id > ? ORDER BY x.pid, x.fid, o.id",
        gt_knows("p.id < ? AND f.email <> ?")
    );
    let stmt = g.prep(pq.as_str()).await.expect("prepare with parameters");
    for (lim, mail, min) in [
        (20i64, "nobody", 0i64),
        (45, &email(3) as &str, 40),
        (0, "x", 0),
    ] {
        let got: Vec<(i64, i64, i64)> = g
            .exec(&stmt, (lim, mail.to_owned(), min))
            .await
            .expect("execute with parameters");
        let got: Vec<String> = got.iter().map(|(a, b, c)| format!("{a}|{b}|{c}")).collect();
        let want = rows(
            &mut r,
            &format!(
                "SELECT x.pid, x.fid, o.id FROM {} AS x JOIN orders o ON o.cust = x.fid WHERE o.id > {min} ORDER BY x.pid, x.fid, o.id",
                sql_knows(&format!("p.id < {lim} AND f.email <> '{mail}'"))
            ),
        )
        .await
        .unwrap();
        assert_eq!(got, want, "prepared with ({lim}, {mail}, {min})");
        compared += 1;
    }
    // A scalar aggregate over no graph rows still answers one row.
    let got = rows(
        &mut g,
        &format!(
            "SELECT COUNT(*), SUM(o.amount) FROM {} AS x JOIN orders o ON o.cust = x.fid",
            gt_knows("p.id < 0")
        ),
    )
    .await
    .unwrap();
    assert_eq!(got, vec!["0|NULL".to_owned()]);
    // EXPLAIN shows the graph's shards on both clusters, the key join, and
    // the outer read.
    let ex = rows(
        &mut g,
        &format!(
            "EXPLAIN FORMAT=SPRIG SELECT x.pid, o.id FROM {} AS x JOIN orders o ON o.cust = x.fid",
            gt_knows("p.id < 5")
        ),
    )
    .await
    .unwrap();
    eprintln!("EXPLAIN:\n{}", ex.join("\n"));
    let text = ex.join("\n");
    for want in [
        "|Graph|",
        "|GraphCall|",
        "|GraphShards|",
        "|KeyJoin|",
        "|Route|",
        "tier",
    ] {
        assert!(text.contains(want), "EXPLAIN lacks {want}:\n{text}");
    }
    assert!(
        text.contains("dc-a-c1") && text.contains(&move_to),
        "{text}"
    );
    // A prepared GRAPH_TABLE runs as text at execute time.
    let stmt = g
        .prep(format!(
            "SELECT COUNT(*) FROM {} AS x JOIN orders o ON o.cust = x.fid",
            gt_knows("p.id < 20")
        ))
        .await
        .expect("prepare");
    let n: Option<i64> = g.exec_first(&stmt, ()).await.expect("execute");
    let want: Vec<String> = rows(
        &mut r,
        &format!(
            "SELECT COUNT(*) FROM {} AS x JOIN orders o ON o.cust = x.fid",
            sql_knows("p.id < 20")
        ),
    )
    .await
    .unwrap();
    assert_eq!(n.map(|v| v.to_string()), want.first().cloned());
    let mut sorted = times.clone();
    sorted.sort();
    eprintln!(
        "graph_table_live: {compared} statements equal to the reference; latency p50 {:?}, max {:?}",
        sorted[sorted.len() / 2],
        sorted.last().copied().unwrap_or_default()
    );
    if env("SPRIG_TEST_GT_KEEP").is_none() {
        r.query_drop("DROP DATABASE IF EXISTS r8six_ref")
            .await
            .unwrap();
    }
}

/// A `GRAPH_TABLE` statement that reads no
/// distributed table takes rows past the 16 MB inline limit from session
/// temporary tables instead of failing with 39903.
#[tokio::test]
async fn graph_table_rows_past_the_inline_limit() {
    const N: i64 = 2600;
    const LEN: i64 = 7000;
    let (Some(sql), Some(baddr), Some(user), Some(pw)) = (
        env("SPRIG_TEST_SQL_ADDR"),
        env("SPRIG_TEST_BOLT_ADDR"),
        env("SPRIG_TEST_GT_USER"),
        env("SPRIG_TEST_BOLT_PASSWORD"),
    ) else {
        eprintln!(
            "skipped: SPRIG_TEST_SQL_ADDR / SPRIG_TEST_BOLT_ADDR / SPRIG_TEST_GT_USER not set"
        );
        return;
    };
    let mut g = gateway(&sql, &user, &pw).await;
    g.query_drop("CREATE DATABASE IF NOT EXISTS r8seven_big")
        .await
        .unwrap();
    g.query_drop("DROP GRAPH IF EXISTS r8seven_big.g")
        .await
        .unwrap();
    g.query_drop("CREATE GRAPH r8seven_big.g").await.unwrap();
    let mut b = bolt(&baddr, &user, &pw).await.expect("bolt login");
    let db = vec![("db", PackValue::str("r8seven_big.g"))];
    let s: String = "abcdefghij".repeat(700);
    let started = Instant::now();
    for lo in (1..=N).step_by(200) {
        b.query(
            &format!(
                "UNWIND range({lo}, {}) AS i CREATE (:Big {{id: i, s: $s}})",
                (lo + 199).min(N)
            ),
            vec![("s", PackValue::String(s.clone()))],
            db.clone(),
        )
        .await
        .expect("big nodes");
    }
    eprintln!("{N} nodes of {LEN} characters in {:?}", started.elapsed());
    let q = "SELECT COUNT(*), SUM(LENGTH(x.s)), MIN(x.id), MAX(x.id) FROM GRAPH_TABLE (r8seven_big.g \
             MATCH (n IS Big) COLUMNS (n.id AS id, n.s AS s)) AS x";
    let t = Instant::now();
    let got = rows(&mut g, q).await.expect("past 16 MB");
    eprintln!(
        "about {} MB of rows through temporary tables in {:?}",
        (N * LEN) >> 20,
        t.elapsed()
    );
    assert_eq!(got, vec![format!("{N}|{}|1|{N}", N * LEN)]);
    // Ordered rows with a filter that the graph applies.
    let got = rows(
        &mut g,
        "SELECT x.id, LENGTH(x.s) FROM GRAPH_TABLE (r8seven_big.g MATCH (n IS Big) \
         COLUMNS (n.id AS id, n.s AS s)) AS x WHERE x.id > 2597 ORDER BY x.id",
    )
    .await
    .unwrap();
    assert_eq!(got, ["2598|7000", "2599|7000", "2600|7000"]);
    // The next statement on the session runs as before (the tables are
    // dropped first), and the big statement runs again.
    assert_eq!(rows(&mut g, "SELECT 1").await.unwrap(), ["1"]);
    let got = rows(&mut g, q).await.expect("again");
    assert_eq!(got.len(), 1);
    if env("SPRIG_TEST_GT_KEEP").is_none() {
        g.query_drop("DROP GRAPH IF EXISTS r8seven_big.g")
            .await
            .unwrap();
        g.query_drop("DROP DATABASE IF EXISTS r8seven_big")
            .await
            .unwrap();
    }
}
