//! Row-level security on inherited rows:
//! a tenant's policies filter every row it reads of an inheritable table,
//! its own and those it inherits from parent spaces and parent tenants,
//! on every read path, evaluated on the merged records.
//!
//! Tenants `a > b > c`, and `b`'s space `b1`; table `docs` (a record
//! hierarchy) made in `a` and shared `a > b > (b1, c)`, read through or
//! materialized (`SPRIG_TEST_RLSI_MODE`). Every tenant has its own
//! policies on its `docs`, permissive and restrictive, and users `alice`,
//! `bob` and `eve` (no permissive policy names `eve`); `app` manages the
//! table and is bound by nothing. Overrides at `b`, `b1` and `c` change
//! what the policies decide: a record made invisible, a record given to
//! another owner, a hidden record, a field changed in a space.
//!
//! [`PATHS`] is the regression table: each read path is run for every
//! user at every level, and **no path may return a record the level's
//! policies hide**. Most must also return exactly what they show. A new
//! read path belongs in the table.
//!
//! ```sh
//! export SPRIG_TEST_RLSI_MYSQL=127.0.0.1:38306
//! export SPRIG_TEST_RLSI_MARIADB=127.0.0.1:38307
//! export SPRIG_TEST_RLSI_HTTP=127.0.0.1:38080
//! export SPRIG_TEST_RLSI_TENANTS=rlsia,rlsib,rlsic
//! export SPRIG_TEST_RLSI_MODE=read_through   # or materialized
//! cargo test -p sprig-gateway --test rls_inherit_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mysql_async::prelude::Queryable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PW: &str = "sprig-dev-app";
const USERS: [&str; 3] = ["alice", "bob", "eve"];
/// `a`'s root, `b`'s root, `b`'s space `b1`, `c`'s root.
const LEVELS: usize = 4;
const TENANT_OF: [usize; LEVELS] = [0, 1, 1, 2];
const LEVEL_NAME: [&str; LEVELS] = ["a", "b", "b1", "c"];

struct Env {
    mysql: String,
    mariadb: String,
    http: String,
    t: Vec<String>,
    materialized: bool,
}

fn env() -> Option<Env> {
    let (Ok(mysql), Ok(mariadb), Ok(http), Ok(t)) = (
        std::env::var("SPRIG_TEST_RLSI_MYSQL"),
        std::env::var("SPRIG_TEST_RLSI_MARIADB"),
        std::env::var("SPRIG_TEST_RLSI_HTTP"),
        std::env::var("SPRIG_TEST_RLSI_TENANTS"),
    ) else {
        eprintln!(
            "skipped: set SPRIG_TEST_RLSI_MYSQL, SPRIG_TEST_RLSI_MARIADB, SPRIG_TEST_RLSI_HTTP \
             and SPRIG_TEST_RLSI_TENANTS"
        );
        return None;
    };
    let t: Vec<String> = t.split(',').map(|x| x.trim().to_owned()).collect();
    assert_eq!(t.len(), 3, "three tenants, a > b > c");
    Some(Env {
        mysql,
        mariadb,
        http,
        t,
        materialized: std::env::var("SPRIG_TEST_RLSI_MODE").ok().as_deref() == Some("materialized"),
    })
}

async fn conn(addr: &str, tenant: &str, user: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.{user}")))
        .pass(Some(PW))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("login {tenant}.{user} at {addr}: {e}"))
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

async fn try_exec(c: &mut mysql_async::Conn, s: &str) -> Result<(), String> {
    c.query_drop(s).await.map_err(|e| e.to_string())
}

async fn ids(c: &mut mysql_async::Conn, s: &str) -> Result<BTreeSet<i64>, String> {
    let rows: Vec<mysql_async::Row> = c.query(s).await.map_err(|e| e.to_string())?;
    Ok(rows
        .into_iter()
        .filter_map(|r| r.get_opt::<i64, _>(0).and_then(Result::ok))
        .collect())
}

/// One record of the model.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Rec {
    owner: String,
    visible: i64,
    tier: i64,
    parent: Option<i64>,
}

type View = BTreeMap<i64, Rec>;

/// The merged views the writes below make, per level.
fn model() -> [View; LEVELS] {
    let owners = ["alice", "bob", "carol"];
    let mut a = View::new();
    for id in 1..=12i64 {
        a.insert(
            id,
            Rec {
                owner: owners[usize::try_from((id - 1) % 3).unwrap()].to_owned(),
                visible: i64::from(!matches!(id, 5 | 6)),
                tier: id % 4,
                parent: (id != 1).then_some(1),
            },
        );
    }
    let mut b = a.clone();
    b.get_mut(&1).unwrap().visible = 0; // alice's record made invisible
    b.get_mut(&4).unwrap().owner = "bob".into(); // alice's record given to bob
    b.remove(&7); // a hidden record
    b.insert(1_000_001, rec("alice", 1, 1));
    let mut b1 = b.clone();
    b1.get_mut(&10).unwrap().tier = 0; // alice's record, a field changed
    b1.insert(1_500_001, rec("alice", 1, 2));
    let mut c = b.clone();
    c.get_mut(&2).unwrap().owner = "alice".into(); // bob's record given to alice
    c.insert(2_000_001, rec("bob", 1, 1));
    [a, b, b1, c]
}

fn rec(owner: &str, visible: i64, tier: i64) -> Rec {
    Rec {
        owner: owner.into(),
        visible,
        tier,
        parent: None,
    }
}

/// The writes of [`model`], per level (`app`, bound by nothing).
fn writes(level: usize) -> &'static [&'static str] {
    match level {
        1 => &[
            "UPDATE docs SET visible = 0 WHERE id = 1",
            "UPDATE docs SET owner = 'bob' WHERE id = 4",
            "DELETE FROM docs WHERE id = 7",
            "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (1000001, NULL, \
             'alice', 1, 1)",
        ],
        2 => &[
            "UPDATE docs SET tier = 0 WHERE id = 10",
            "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (1500001, NULL, \
             'alice', 1, 2)",
        ],
        3 => &[
            "UPDATE docs SET owner = 'alice' WHERE id = 2",
            "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (2000001, NULL, 'bob', \
             1, 1)",
        ],
        _ => &[],
    }
}

/// The policies of tenant `t` (0, 1, 2) on its `docs`.
fn policies(t: usize) -> Vec<&'static str> {
    let mut v = vec![
        "CREATE POLICY p_alice ON docs FOR SELECT, UPDATE, DELETE TO alice USING (owner = 'alice')",
        "CREATE POLICY p_bob ON docs FOR SELECT, UPDATE, DELETE TO bob USING (owner = 'bob')",
    ];
    match t {
        0 => v.push(
            "CREATE POLICY p_tier ON docs AS RESTRICTIVE FOR ALL TO PUBLIC USING (tier <= 2)",
        ),
        1 => v.push(
            "CREATE POLICY p_vis ON docs AS RESTRICTIVE FOR ALL TO PUBLIC USING (visible = 1)",
        ),
        _ => {
            v.push("CREATE POLICY p_t3 ON docs FOR SELECT TO alice USING (tier = 3)");
            v.push("CREATE POLICY p_nz ON docs AS RESTRICTIVE FOR ALL TO PUBLIC USING (tier <> 0)");
        }
    }
    v
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cmd {
    Select,
    Update,
    Delete,
}

/// Whether tenant `t`'s policies show `r` to `user` for `cmd` (the
/// write's policies with `SELECT`'s, as the gateway applies them).
fn shows(t: usize, user: &str, r: &Rec, cmd: Cmd) -> bool {
    if user == "app" {
        return true;
    }
    let own = (user == "alice" && r.owner == "alice") || (user == "bob" && r.owner == "bob");
    let restrictive = match t {
        0 => r.tier <= 2,
        1 => r.visible == 1,
        _ => r.tier != 0,
    };
    let select = (own || (t == 2 && user == "alice" && r.tier == 3)) && restrictive;
    match cmd {
        Cmd::Select => select,
        Cmd::Update | Cmd::Delete => select && own && restrictive,
    }
}

fn expected(view: &View, t: usize, user: &str, cmd: Cmd) -> BTreeSet<i64> {
    view.iter()
        .filter(|(_, r)| shows(t, user, r, cmd))
        .map(|(i, _)| *i)
        .collect()
}

/// What a read path answered.
#[derive(Debug)]
enum Got {
    Ids(BTreeSet<i64>),
    Count(u64),
    /// Refused (allowed only for [`Must::SubsetOrRefused`] paths).
    Refused(String),
}

/// What a read path must answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Must {
    /// Exactly the records the policies show (`SELECT`'s).
    Exact,
    /// The number of records the policies show for the command.
    Count(Cmd),
    /// Records the policies show, never another (the hierarchy functions,
    /// whose answer depends on the hierarchy too); an error is allowed.
    SubsetOrRefused,
}

/// The read paths (the regression table).
#[derive(Debug, Clone, Copy)]
enum Path {
    SelectMysql,
    SelectMariadb,
    PointReads,
    InList,
    SelfJoin,
    Derived,
    InSubquery,
    Union,
    Origin,
    Http,
    CountStar,
    UpdateMatched,
    DeleteAffected,
    Subtree,
    Prepared,
    Graph,
    Search,
}

const PATHS: &[(Path, Must)] = &[
    (Path::SelectMysql, Must::Exact),
    (Path::SelectMariadb, Must::Exact),
    (Path::PointReads, Must::Exact),
    (Path::InList, Must::Exact),
    (Path::SelfJoin, Must::Exact),
    (Path::Derived, Must::Exact),
    (Path::InSubquery, Must::Exact),
    (Path::Union, Must::Exact),
    (Path::Origin, Must::Exact),
    (Path::Http, Must::Exact),
    (Path::CountStar, Must::Count(Cmd::Select)),
    (Path::UpdateMatched, Must::Count(Cmd::Update)),
    (Path::DeleteAffected, Must::Count(Cmd::Delete)),
    (Path::Subtree, Must::SubsetOrRefused),
    (Path::Prepared, Must::SubsetOrRefused),
    (Path::Graph, Must::SubsetOrRefused),
    (Path::Search, Must::SubsetOrRefused),
];

struct Fixture<'e> {
    e: &'e Env,
    db: String,
    b1: String,
}

impl Fixture<'_> {
    async fn session(&self, addr: &str, level: usize, user: &str) -> mysql_async::Conn {
        let mut c = conn(addr, &self.e.t[TENANT_OF[level]], user).await;
        must(&mut c, &format!("USE `{}`", self.db)).await;
        if level == 2 {
            must(&mut c, &format!("USE SPACE `{}`", self.b1)).await;
        }
        c
    }

    /// One path for `user` at `level`; `all`: every id the model knows.
    async fn run(&self, p: Path, level: usize, user: &str, all: &BTreeSet<i64>) -> Got {
        let answer = |r: Result<BTreeSet<i64>, String>| match r {
            Ok(s) => Got::Ids(s),
            Err(e) => Got::Refused(e),
        };
        let mysql = &self.e.mysql;
        match p {
            Path::SelectMysql => {
                let mut c = self.session(mysql, level, user).await;
                answer(ids(&mut c, "SELECT id FROM docs").await)
            }
            Path::SelectMariadb => {
                let mut c = self.session(&self.e.mariadb, level, user).await;
                answer(ids(&mut c, "SELECT * FROM docs").await)
            }
            Path::PointReads => {
                let mut c = self.session(mysql, level, user).await;
                let mut out = BTreeSet::new();
                for i in all {
                    match ids(&mut c, &format!("SELECT id FROM docs WHERE id = {i}")).await {
                        Ok(s) => out.extend(s),
                        Err(e) => return Got::Refused(e),
                    }
                }
                Got::Ids(out)
            }
            Path::InList => {
                let mut c = self.session(mysql, level, user).await;
                let list: Vec<String> = all.iter().map(ToString::to_string).collect();
                answer(
                    ids(
                        &mut c,
                        &format!("SELECT id FROM docs WHERE id IN ({})", list.join(", ")),
                    )
                    .await,
                )
            }
            Path::SelfJoin => {
                let mut c = self.session(mysql, level, user).await;
                answer(
                    ids(
                        &mut c,
                        "SELECT d.id FROM docs d LEFT JOIN docs e ON e.id = d.parent_id",
                    )
                    .await,
                )
            }
            Path::Derived => {
                let mut c = self.session(mysql, level, user).await;
                answer(ids(&mut c, "SELECT x.id FROM (SELECT * FROM docs) AS x").await)
            }
            Path::InSubquery => {
                let mut c = self.session(&self.e.mariadb, level, user).await;
                answer(
                    ids(
                        &mut c,
                        "SELECT id FROM docs WHERE id IN (SELECT id FROM docs WHERE owner IS NOT \
                         NULL)",
                    )
                    .await,
                )
            }
            Path::Union => {
                let mut c = self.session(mysql, level, user).await;
                answer(
                    ids(
                        &mut c,
                        "SELECT id FROM docs WHERE tier < 2 UNION SELECT id FROM docs WHERE tier \
                         >= 2",
                    )
                    .await,
                )
            }
            Path::Origin => {
                let mut c = self.session(mysql, level, user).await;
                answer(ids(&mut c, "SELECT id, SPRIG_ORIGIN() FROM docs").await)
            }
            Path::Http => match self.http_ids(level, user).await {
                Ok(s) => Got::Ids(s),
                Err(e) => Got::Refused(e),
            },
            Path::CountStar => {
                let mut c = self.session(mysql, level, user).await;
                match c.query_first::<u64, _>("SELECT COUNT(*) FROM docs").await {
                    Ok(n) => Got::Count(n.unwrap_or(0)),
                    Err(e) => Got::Refused(e.to_string()),
                }
            }
            Path::UpdateMatched | Path::DeleteAffected => {
                let mut c = self.session(mysql, level, user).await;
                must(&mut c, "BEGIN").await;
                let sql = if matches!(p, Path::UpdateMatched) {
                    "UPDATE docs SET body = 'rlsi'"
                } else {
                    // The root has children: a record hierarchy refuses its
                    // delete.
                    "DELETE FROM docs WHERE id <> 1"
                };
                let r = c.query_drop(sql).await;
                let got = match r {
                    Ok(()) => {
                        let info = c.info().into_owned();
                        let matched = info
                            .split("Rows matched: ")
                            .nth(1)
                            .and_then(|x| x.split_whitespace().next())
                            .and_then(|x| x.parse().ok());
                        Got::Count(matched.unwrap_or(c.affected_rows()))
                    }
                    Err(e) => Got::Refused(e.to_string()),
                };
                let _ = c.query_drop("ROLLBACK").await;
                got
            }
            Path::Subtree => {
                let mut c = self.session(mysql, level, user).await;
                answer(ids(&mut c, "SELECT id FROM docs WHERE SPRIG_SUBTREE(docs, 1)").await)
            }
            Path::Prepared => {
                let mut c = self.session(mysql, level, user).await;
                match c
                    .exec::<i64, _, _>("SELECT id FROM docs WHERE id > ?", (0,))
                    .await
                {
                    Ok(v) => Got::Ids(v.into_iter().collect()),
                    Err(e) => Got::Refused(e.to_string()),
                }
            }
            Path::Graph => {
                let mut c = self.session(mysql, level, user).await;
                answer(
                    ids(
                        &mut c,
                        "SELECT id FROM GRAPH_TABLE (rlsi_g MATCH (d:Doc) COLUMNS (d.id AS id)) \
                         AS g",
                    )
                    .await,
                )
            }
            Path::Search => {
                let mut c = self.session(mysql, level, user).await;
                answer(
                    ids(
                        &mut c,
                        "SELECT id FROM docs WHERE MATCH (body) AGAINST ('searchable')",
                    )
                    .await,
                )
            }
        }
    }

    async fn http_ids(&self, level: usize, user: &str) -> Result<BTreeSet<i64>, String> {
        let tcp = tokio::net::TcpStream::connect(&self.e.http)
            .await
            .map_err(|e| e.to_string())?;
        let auth = base64::engine::general_purpose::STANDARD
            .encode(format!("{}.{user}:{PW}", self.e.t[TENANT_OF[level]]));
        let body = serde_json::json!({ "sql": "SELECT id FROM docs" }).to_string();
        let mut req = hyper::Request::builder()
            .method("POST")
            .uri(format!("/v1/databases/{}/sql:execute", self.db))
            .header("host", &self.e.http)
            .header("authorization", format!("Basic {auth}"))
            .header("content-type", "application/json");
        if level == 2 {
            req = req.header("sprig-space", &self.b1);
        }
        let req = req.body(Full::new(Bytes::from(body))).unwrap();
        let (mut tx, c) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(c);
        let resp = tx.send_request(req).await.map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let b = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| e.to_string())?
            .to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&b).map_err(|e| e.to_string())?;
        if status >= 400 {
            return Err(v.to_string());
        }
        let r = sprig_api::results::execute_from_json(&v).map_err(|e| e.to_string())?;
        let mut out = BTreeSet::new();
        for res in &r.results {
            for row in &res.rows {
                if let Some(cell) = row.values.first()
                    && let Some(b) = sprig_api::results::cell_bytes(cell, false)
                    && let Ok(i) = String::from_utf8_lossy(&b).parse::<i64>()
                {
                    out.insert(i);
                }
            }
        }
        Ok(out)
    }
}

/// An NDJSON change stream of `docs`.
async fn open_stream(e: &Env, tenant: &str, user: &str, db: &str) -> tokio::net::TcpStream {
    let mut s = tokio::net::TcpStream::connect(&e.http).await.unwrap();
    let auth = base64::engine::general_purpose::STANDARD.encode(format!("{tenant}.{user}:{PW}"));
    let req = format!(
        "GET /v1/databases/{db}/changes?objects=docs HTTP/1.1\r\nhost: {}\r\nauthorization: \
         Basic {auth}\r\n\r\n",
        e.http
    );
    s.write_all(req.as_bytes()).await.unwrap();
    s
}

/// The keys (`id`) of the events a stream got, heartbeats left out.
async fn stream_keys(s: &mut tokio::net::TcpStream, secs: u64) -> Vec<(String, i64)> {
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    let mut buf = Vec::new();
    let mut chunk = vec![0u8; 65536];
    loop {
        match tokio::time::timeout_at(end, s.read(&mut chunk)).await {
            Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&chunk[..n]),
            _ => break,
        }
    }
    String::from_utf8_lossy(&buf)
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .filter(|v| v.is_object() && v["op"] != "heartbeat")
        .filter_map(|v| {
            let key: serde_json::Value =
                serde_json::from_str(v["key"].as_str().unwrap_or("null")).ok()?;
            Some((v["op"].as_str()?.to_owned(), key["id"].as_i64()?))
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_read_path_returns_an_inherited_row_a_policy_hides() {
    let Some(e) = env() else {
        return;
    };
    let pid = std::process::id() % 10_000;
    let tag = if e.materialized { "m" } else { "r" };
    let db = format!("rlsi_{tag}{pid}");
    let b1 = format!("rlsi_{tag}{pid}_b1");
    let f = Fixture {
        e: &e,
        db: db.clone(),
        b1: b1.clone(),
    };
    let mut app: Vec<mysql_async::Conn> = Vec::new();
    for t in &e.t {
        let mut c = conn(&e.mysql, t, "app").await;
        must(&mut c, &format!("USE SPACE `{t}`")).await;
        app.push(c);
    }
    // Setup: the table in a, shared a > b > (b1, c).
    let _ = try_exec(&mut app[1], &format!("DROP SPACE IF EXISTS `{b1}` CASCADE")).await;
    must(&mut app[1], &format!("CREATE SPACE `{b1}`")).await;
    must(&mut app[0], &format!("CREATE DATABASE `{db}`")).await;
    must(&mut app[0], &format!("USE `{db}`")).await;
    must(
        &mut app[0],
        "CREATE TABLE docs (id INT PRIMARY KEY, parent_id INT NULL, owner VARCHAR(20) NOT NULL, \
         visible INT NOT NULL DEFAULT 1, tier INT NOT NULL DEFAULT 0, body VARCHAR(40) NOT NULL \
         DEFAULT 'searchable')",
    )
    .await;
    must(&mut app[0], "ALTER TABLE docs ADD HIERARCHY (parent_id)").await;
    must(&mut app[0], "ALTER TABLE docs SPRIG INHERITABLE KEY (id)").await;
    let base = &model()[0];
    for (id, r) in base {
        must(
            &mut app[0],
            &format!(
                "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES ({id}, {}, '{}', \
                 {}, {})",
                r.parent.map_or("NULL".to_owned(), |p| p.to_string()),
                r.owner,
                r.visible,
                r.tier
            ),
        )
        .await;
    }
    if e.materialized {
        must(&mut app[0], "SPRIG SET inherit.merge = 'materialized'").await;
    }
    must(
        &mut app[0],
        &format!(
            "CREATE SHARE rlsi_{tag}{pid}_ab ON docs TO SPACE `{}` WITH RESHARE",
            e.t[1]
        ),
    )
    .await;
    if e.materialized {
        must(&mut app[0], "SPRIG RESET inherit.merge").await;
    }
    must(&mut app[1], &format!("USE `{db}`")).await;
    must(
        &mut app[1],
        &format!("CREATE SHARE rlsi_{tag}{pid}_bb ON docs TO SPACE `{b1}`"),
    )
    .await;
    if e.materialized {
        must(&mut app[1], "SPRIG SET inherit.merge = 'materialized'").await;
    }
    must(
        &mut app[1],
        &format!(
            "CREATE SHARE rlsi_{tag}{pid}_bc ON docs TO SPACE `{}`",
            e.t[2]
        ),
    )
    .await;
    if e.materialized {
        must(&mut app[1], "SPRIG RESET inherit.merge").await;
    }
    must(&mut app[2], &format!("USE `{db}`")).await;
    // Users and policies in every tenant.
    for (t, c) in app.iter_mut().enumerate() {
        for u in USERS {
            let _ = try_exec(c, &format!("DROP USER IF EXISTS '{u}'@'%'")).await;
            must(c, &format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'")).await;
            must(
                c,
                &format!("GRANT SELECT, INSERT, UPDATE, DELETE, WATCH ON `{db}`.* TO '{u}'@'%'"),
            )
            .await;
        }
        must(c, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
        for p in policies(t) {
            must(c, p).await;
        }
    }
    // The overrides, hides and own records below a.
    for level in 1..LEVELS {
        let mut c = f.session(&e.mysql, level, "app").await;
        for w in writes(level) {
            must(&mut c, w).await;
        }
    }
    // A mapped graph and a full-text index, where they can be made (their
    // refusal is a pass: the path then has no rows to leak).
    for (t, c) in app.iter_mut().enumerate() {
        let g = try_exec(
            c,
            "CREATE PROPERTY GRAPH rlsi_g VERTEX TABLES (docs KEY (id) LABEL Doc PROPERTIES (id, \
             owner))",
        )
        .await;
        eprintln!("graph in tenant {t}: {g:?}");
    }
    let ft = try_exec(&mut app[0], "CREATE FULLTEXT INDEX rlsi_ft ON docs (body)").await;
    eprintln!("full-text index in a: {ft:?}");

    // The oracle: app, bound by nothing, sees the model at every level
    // (materialized copies converge first).
    let views = model();
    for level in 0..LEVELS {
        let want: BTreeSet<i64> = views[level].keys().copied().collect();
        let mut got = Err(String::new());
        for _ in 0..60 {
            let mut c = f.session(&e.mysql, level, "app").await;
            got = ids(&mut c, "SELECT id FROM docs").await;
            if got.as_ref() == Ok(&want) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert_eq!(got, Ok(want), "app's view at {}", LEVEL_NAME[level]);
    }
    let all: BTreeSet<i64> = views
        .iter()
        .flat_map(|v| v.keys().copied())
        .chain([7])
        .collect();

    // The matrix: every path, user and level.
    let mut leaks: Vec<String> = Vec::new();
    let mut wrong: Vec<String> = Vec::new();
    let mut checks = 0u32;
    let mut refused: BTreeMap<String, u32> = BTreeMap::new();
    for level in 0..LEVELS {
        let t = TENANT_OF[level];
        let view = &views[level];
        for user in USERS.iter().copied().chain(["app"]) {
            let shown = expected(view, t, user, Cmd::Select);
            for (p, must) in PATHS {
                checks += 1;
                let got = f.run(*p, level, user, &all).await;
                let at = format!("{p:?} {user}@{}", LEVEL_NAME[level]);
                match (&got, must) {
                    (Got::Ids(s), _) => {
                        let hidden: Vec<i64> = s.difference(&shown).copied().collect();
                        if !hidden.is_empty() {
                            leaks.push(format!("{at}: returned hidden {hidden:?}"));
                        }
                        if *must == Must::Exact && *s != shown {
                            wrong.push(format!("{at}: {s:?}, want {shown:?}"));
                        }
                    }
                    (Got::Count(n), Must::Count(cmd)) => {
                        let mut want = expected(view, t, user, *cmd);
                        if *cmd == Cmd::Delete {
                            want.remove(&1);
                        }
                        let w = want.len() as u64;
                        if *n > w {
                            leaks.push(format!("{at}: counted {n}, policies show {w}"));
                        } else if *n != w {
                            wrong.push(format!("{at}: counted {n}, want {w}"));
                        }
                    }
                    (Got::Refused(msg), Must::SubsetOrRefused) => {
                        *refused
                            .entry(format!(
                                "{p:?}: {}",
                                msg.chars().take(90).collect::<String>()
                            ))
                            .or_default() += 1;
                    }
                    (other, _) => wrong.push(format!("{at}: {other:?}")),
                }
            }
        }
    }
    eprintln!("{checks} path checks; refused (allowed): {refused:#?}");
    assert!(
        leaks.is_empty(),
        "hidden records returned:\n{}",
        leaks.join("\n")
    );
    assert!(wrong.is_empty(), "wrong answers:\n{}", wrong.join("\n"));

    // Change streams: c's alice gets a's changes only for records c's
    // policies show her; eve, whom no policy names, gets nothing.
    let mut s_alice = open_stream(&e, &e.t[2], "alice", &db).await;
    let mut s_eve = open_stream(&e, &e.t[2], "eve", &db).await;
    let mut s_bob_b = open_stream(&e, &e.t[1], "bob", &db).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    for w in [
        // alice's: shown to alice at c, not to bob at b.
        "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (50, 1, 'alice', 1, 1)",
        // bob's, visible at b: bob at b sees it; alice at c doesn't.
        "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (51, 1, 'bob', 1, 1)",
        // bob's, invisible: hidden by b's restrictive policy.
        "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (52, 1, 'bob', 0, 1)",
        "UPDATE docs SET body = 'changed' WHERE id IN (50, 51, 52)",
        // carol's tier-3 record 3: c's p_t3 shows it to alice.
        "UPDATE docs SET body = 'changed' WHERE id = 3",
        // carol's record 6: nobody's.
        "UPDATE docs SET body = 'changed' WHERE id = 6",
        // alice's record 1: b's override made it invisible, which b's
        // policies hide; c's policies don't read `visible` (a parent's
        // policies don't bind a child's readers), so alice at c sees it.
        "UPDATE docs SET body = 'changed' WHERE id = 1",
    ] {
        must(&mut app[0], w).await;
    }
    let ka = stream_keys(&mut s_alice, 12).await;
    let ke = stream_keys(&mut s_eve, 1).await;
    let kb = stream_keys(&mut s_bob_b, 1).await;
    eprintln!("streams: alice@c {ka:?}; eve@c {ke:?}; bob@b {kb:?}");
    let keys = |v: &[(String, i64)]| -> BTreeSet<i64> { v.iter().map(|(_, k)| *k).collect() };
    let leak_a: Vec<i64> = keys(&ka)
        .into_iter()
        .filter(|k| ![50, 3, 1].contains(k))
        .collect();
    assert!(
        leak_a.is_empty(),
        "alice@c got hidden records' events: {leak_a:?}"
    );
    assert!(
        keys(&ka).contains(&50),
        "alice@c missed her record's insert: {ka:?}"
    );
    assert!(ke.is_empty(), "eve@c got events: {ke:?}");
    let leak_b: Vec<i64> = keys(&kb).into_iter().filter(|k| *k != 51).collect();
    assert!(
        leak_b.is_empty(),
        "bob@b got hidden records' events: {leak_b:?}"
    );
    assert!(keys(&kb).contains(&51), "bob@b missed his record: {kb:?}");

    // Cleanup, best effort: rows below a, shares bottom first, users, the
    // space, the databases.
    for level in (1..LEVELS).rev() {
        let mut c = f.session(&e.mysql, level, "app").await;
        let _ = try_exec(&mut c, "SPRIG RESET docs").await;
        let _ = try_exec(&mut c, "DELETE FROM docs WHERE id > 1000000").await;
    }
    let _ = try_exec(
        &mut app[1],
        &format!("DROP SHARE IF EXISTS rlsi_{tag}{pid}_bc"),
    )
    .await;
    let _ = try_exec(
        &mut app[1],
        &format!("DROP SHARE IF EXISTS rlsi_{tag}{pid}_bb"),
    )
    .await;
    let _ = try_exec(
        &mut app[0],
        &format!("DROP SHARE IF EXISTS rlsi_{tag}{pid}_ab"),
    )
    .await;
    let _ = try_exec(&mut app[1], &format!("DROP SPACE IF EXISTS `{b1}` CASCADE")).await;
    for c in app.iter_mut().rev() {
        let _ = try_exec(c, "DROP GRAPH IF EXISTS rlsi_g").await;
        for u in USERS {
            let _ = try_exec(c, &format!("DROP USER IF EXISTS '{u}'@'%'")).await;
        }
        let _ = try_exec(c, &format!("DROP DATABASE IF EXISTS `{db}`")).await;
    }
}
