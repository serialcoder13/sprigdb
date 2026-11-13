//! Record hierarchies on inheritable tables (decision T22) through a
//! running gateway: spaces `root > s1 > s2` of one tenant share a table
//! with a hierarchy (`parent_id`).
//!
//! After every step, at every level, the stored paths (`SPRIG_PATH`,
//! `SPRIG_DEPTH`) are compared with paths a Rust model computes from that
//! level's merged `parent_id`s, and `SPRIG_SUBTREE`, `SPRIG_DESCENDANTS`
//! and `SPRIG_ANCESTORS` are compared with the model and with a
//! brute-force recursive CTE over the same level's rows.
//!
//! - T22's case: `s1` overrides a row's parent, then the root moves a row
//!   under it; `s1`'s path follows `s1`'s tree.
//! - Random steps at every level: inserts, moves, `parent_id` overrides,
//!   own rows, deletes, resets; cycles only a lower level would see are
//!   refused (39711), as are rows left under a deleted record (39714).
//! - Concurrent writers at two levels; a subtree moving under load.
//! - Row-level security: a user who sees part of a subtree moves it; every
//!   row's path follows.
//! - Both orders of `ADD HIERARCHY` and `SPRIG INHERITABLE`; `DROP
//!   HIERARCHY`; the refusals that remain (39891).
//!
//! ```sh
//! export SPRIG_TEST_HL_GATEWAY=127.0.0.1:33306,127.0.0.1:33307   # mysql, mariadb persona
//! export SPRIG_TEST_HL_TENANT=acme                                # with user app
//! cargo test -p sprig-inherit --test hier_live -- --nocapture --test-threads 1
//! ```
//!
//! Each run uses its own database and spaces per persona and drops them
//! at the end. Without the variables every test reports what it needs and
//! passes (fails under `SPRIG_TEST_STRICT=1`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::uninlined_format_args,
    clippy::many_single_char_names,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row, SslOpts, Value};

const GATEWAY: &str = "SPRIG_TEST_HL_GATEWAY";
const PW: &str = "sprig-dev-app";

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

fn tenant() -> String {
    std::env::var("SPRIG_TEST_HL_TENANT").unwrap_or_else(|_| "acme".into())
}

async fn conn_as(addr: &str, user: &str, pw: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.{user}", tenant())))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("connect {addr} as {user}: {e}"))
}

async fn conn(addr: &str) -> Conn {
    conn_as(addr, "app", PW).await
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

async fn ids(c: &mut Conn, sql: &str) -> BTreeSet<i64> {
    rows(c, sql)
        .await
        .into_iter()
        .filter_map(|r| r[0].as_deref()?.parse().ok())
        .collect()
}

async fn exec(c: &mut Conn, sql: &str) -> u64 {
    c.query_drop(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    c.affected_rows()
}

async fn try_exec(c: &mut Conn, sql: &str) -> Result<u64, (u16, String)> {
    match c.query_drop(sql).await {
        Ok(()) => Ok(c.affected_rows()),
        Err(mysql_async::Error::Server(s)) => Err((s.code, s.message)),
        Err(e) => panic!("{sql}: {e}"),
    }
}

async fn fails(c: &mut Conn, sql: &str, code: u16) -> String {
    match try_exec(c, sql).await {
        Ok(_) => panic!("{sql}: succeeded, expected {code}"),
        Err((got, m)) => {
            assert_eq!(got, code, "{sql}: {m}");
            assert!(!m.contains("_sprig_"), "{sql}: {m}");
            m
        }
    }
}

/// xorshift64*: reproducible steps from a seed.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
    fn pick<T: Clone>(&mut self, v: &[T]) -> Option<T> {
        (!v.is_empty()).then(|| v[self.below(v.len())].clone())
    }
}

/// One persona's database and spaces.
struct Env {
    addr: String,
    db: String,
    s1: String,
    s2: String,
    /// The root space's, `s1`'s and `s2`'s sessions.
    c: Vec<Conn>,
}

impl Env {
    async fn open(addr: &str, tag: &str) -> Self {
        let pid = std::process::id() % 10_000;
        let db = format!("r6t2_hl_{tag}_{pid}");
        let s1 = format!("r6t2_{tag}_{pid}_s1");
        let s2 = format!("r6t2_{tag}_{pid}_s2");
        let mut c0 = conn(addr).await;
        exec(&mut c0, &format!("DROP SPACE IF EXISTS `{s2}` CASCADE")).await;
        exec(&mut c0, &format!("DROP SPACE IF EXISTS `{s1}` CASCADE")).await;
        exec(&mut c0, &format!("DROP DATABASE IF EXISTS `{db}`")).await;
        exec(&mut c0, &format!("CREATE DATABASE `{db}`")).await;
        exec(&mut c0, &format!("USE `{db}`")).await;
        exec(&mut c0, &format!("CREATE SPACE `{s1}`")).await;
        exec(&mut c0, &format!("CREATE SPACE `{s2}` UNDER `{s1}`")).await;
        let mut c = vec![c0];
        for s in [&s1, &s2] {
            let mut x = conn(addr).await;
            exec(&mut x, &format!("USE `{db}`")).await;
            exec(&mut x, &format!("USE SPACE `{s}`")).await;
            c.push(x);
        }
        Self {
            addr: addr.to_owned(),
            db,
            s1,
            s2,
            c,
        }
    }

    async fn session(&self, level: usize) -> Conn {
        let mut x = conn(&self.addr).await;
        exec(&mut x, &format!("USE `{}`", self.db)).await;
        if level > 0 {
            let s = if level == 1 { &self.s1 } else { &self.s2 };
            exec(&mut x, &format!("USE SPACE `{s}`")).await;
        }
        x
    }

    /// Creates `t` with a hierarchy, inheritable, shared root > s1 > s2.
    /// `hierarchy_first`: `ADD HIERARCHY` before `SPRIG INHERITABLE`.
    async fn table(&mut self, t: &str, hierarchy_first: bool) {
        let c = &mut self.c[0];
        exec(
            c,
            &format!(
                "CREATE TABLE {t} (id INT PRIMARY KEY, parent_id INT NULL, name VARCHAR(40) NOT \
                 NULL DEFAULT '', tag CHAR(1) NOT NULL DEFAULT 'a')"
            ),
        )
        .await;
        if hierarchy_first {
            exec(c, &format!("ALTER TABLE {t} ADD HIERARCHY (parent_id)")).await;
            exec(c, &format!("ALTER TABLE {t} SPRIG INHERITABLE KEY (id)")).await;
        } else {
            exec(c, &format!("ALTER TABLE {t} SPRIG INHERITABLE KEY (id)")).await;
        }
        exec(
            c,
            &format!(
                "CREATE SHARE r6t2_{t}_{} ON {t} TO SPACE `{}` WITH RESHARE",
                self.db, self.s1
            ),
        )
        .await;
        exec(
            &mut self.c[1],
            &format!(
                "CREATE SHARE r6t2_{t}_{}_2 ON {t} TO SPACE `{}`",
                self.db, self.s2
            ),
        )
        .await;
    }

    async fn close(mut self) {
        let c = &mut self.c[0];
        exec(c, &format!("DROP DATABASE IF EXISTS `{}`", self.db)).await;
        exec(c, &format!("DROP SPACE IF EXISTS `{}` CASCADE", self.s2)).await;
        exec(c, &format!("DROP SPACE IF EXISTS `{}` CASCADE", self.s1)).await;
    }
}

/// A level's tree from its merged `parent_id`s: every row's path and
/// depth, or the rows in a cycle or under a missing parent.
fn model(links: &BTreeMap<i64, Option<i64>>) -> Result<BTreeMap<i64, (String, i64)>, String> {
    let mut out = BTreeMap::new();
    for &k in links.keys() {
        let mut chain = vec![k];
        let mut cur = k;
        while let Some(Some(p)) = links.get(&cur) {
            if chain.contains(p) {
                return Err(format!("cycle at {k}"));
            }
            if !links.contains_key(p) {
                return Err(format!("{cur}'s parent {p} is missing"));
            }
            chain.push(*p);
            cur = *p;
        }
        chain.reverse();
        let path = format!(
            "/{}/",
            chain
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("/")
        );
        out.insert(k, (path, chain.len() as i64 - 1));
    }
    Ok(out)
}

/// Checks one level (see the module docs); `probes` keys get the subtree
/// queries. Returns the level's links.
async fn check_level(
    c: &mut Conn,
    t: &str,
    level: usize,
    probes: &[i64],
    what: &str,
) -> BTreeMap<i64, Option<i64>> {
    let links: BTreeMap<i64, Option<i64>> = rows(c, &format!("SELECT id, parent_id FROM {t}"))
        .await
        .into_iter()
        .map(|r| {
            (
                r[0].as_deref().unwrap().parse().unwrap(),
                r[1].as_deref().map(|p| p.parse().unwrap()),
            )
        })
        .collect();
    let want = model(&links).unwrap_or_else(|e| panic!("{what}: level {level}: {e}"));
    let got: BTreeMap<i64, (Option<String>, Option<i64>)> = rows(
        c,
        &format!("SELECT id, SPRIG_PATH({t}), SPRIG_DEPTH({t}) FROM {t}"),
    )
    .await
    .into_iter()
    .map(|r| {
        (
            r[0].as_deref().unwrap().parse().unwrap(),
            (r[1].clone(), r[2].as_deref().map(|d| d.parse().unwrap())),
        )
    })
    .collect();
    for (k, (p, d)) in &want {
        let g = got.get(k).cloned().unwrap_or_default();
        assert_eq!(
            (g.0.as_deref(), g.1),
            (Some(p.as_str()), Some(*d)),
            "{what}: level {level}: row {k}'s path"
        );
    }
    assert_eq!(got.len(), want.len(), "{what}: level {level}: rows");
    // `SELECT *` hides the path columns.
    let star: Vec<Row> = c
        .query(format!("SELECT * FROM {t} LIMIT 1"))
        .await
        .unwrap();
    if let Some(r) = star.first() {
        assert_eq!(r.len(), 4, "{what}: level {level}: SELECT * columns");
    }
    for &k in probes {
        let under: BTreeSet<i64> = want
            .iter()
            .filter(|(_, (p, _))| p.contains(&format!("/{k}/")))
            .map(|(x, _)| *x)
            .collect();
        let sub = ids(
            c,
            &format!("SELECT id FROM {t} WHERE SPRIG_SUBTREE({t}, {k})"),
        )
        .await;
        assert_eq!(sub, under, "{what}: level {level}: SPRIG_SUBTREE({k})");
        let desc = ids(
            c,
            &format!("SELECT x.id FROM {t} x WHERE SPRIG_DESCENDANTS(x, {k})"),
        )
        .await;
        let mut d = under.clone();
        d.remove(&k);
        assert_eq!(desc, d, "{what}: level {level}: SPRIG_DESCENDANTS({k})");
        let brute = ids(
            c,
            &format!(
                "WITH RECURSIVE sub (id) AS (SELECT id FROM {t} WHERE id = {k} UNION ALL \
                 SELECT c.id FROM {t} c JOIN sub ON c.parent_id = sub.id) SELECT id FROM sub"
            ),
        )
        .await;
        assert_eq!(brute, under, "{what}: level {level}: recursive CTE of {k}");
        let anc = ids(
            c,
            &format!("SELECT id FROM {t} WHERE SPRIG_ANCESTORS({t}, {k})"),
        )
        .await;
        let mut a = BTreeSet::new();
        if let Some((p, _)) = want.get(&k) {
            a = p
                .split('/')
                .filter_map(|s| s.parse::<i64>().ok())
                .filter(|x| *x != k)
                .collect();
        }
        assert_eq!(anc, a, "{what}: level {level}: SPRIG_ANCESTORS({k})");
    }
    links
}

/// Every level, with `n` random probes each.
async fn check_all(e: &mut Env, t: &str, rng: &mut Rng, n: usize, what: &str) {
    for level in 0..3 {
        let all: Vec<i64> = ids(&mut e.c[level], &format!("SELECT id FROM {t}"))
            .await
            .into_iter()
            .collect();
        let probes: Vec<i64> = (0..n.min(all.len()))
            .filter_map(|_| rng.pick(&all))
            .collect();
        check_level(&mut e.c[level], t, level, &probes, what).await;
    }
}

/// The legitimate refusals of a random step.
fn refusal(code: u16) -> bool {
    matches!(code, 39711..=39714 | 1062 | 1213 | 1205)
}

async fn t22_case(e: &mut Env) {
    let t = "tc";
    e.table(t, true).await;
    // root: 1 > {2, 3}, 2 > 4, 3 > 5
    exec(
        &mut e.c[0],
        &format!(
            "INSERT INTO {t} (id, parent_id) VALUES (1, NULL), (2, 1), (3, 1), (4, 2), (5, 3)"
        ),
    )
    .await;
    let mut rng = Rng(7);
    check_all(e, t, &mut rng, 5, "t22 seed").await;
    // s1 moves 4 under 3: s1's own structure.
    exec(&mut e.c[1], &format!("UPDATE {t} SET parent_id = 3 WHERE id = 4")).await;
    check_all(e, t, &mut rng, 5, "s1 moves 4").await;
    // The root moves 5 under 4: s1 must see /1/3/4/5/, the root /1/2/4/5/.
    exec(&mut e.c[0], &format!("UPDATE {t} SET parent_id = 4 WHERE id = 5")).await;
    let p0 = rows(&mut e.c[0], &format!("SELECT SPRIG_PATH({t}) FROM {t} WHERE id = 5")).await;
    let p1 = rows(&mut e.c[1], &format!("SELECT SPRIG_PATH({t}) FROM {t} WHERE id = 5")).await;
    let p2 = rows(&mut e.c[2], &format!("SELECT SPRIG_PATH({t}) FROM {t} WHERE id = 5")).await;
    assert_eq!(p0[0][0].as_deref(), Some("/1/2/4/5/"));
    assert_eq!(p1[0][0].as_deref(), Some("/1/3/4/5/"));
    assert_eq!(p2[0][0].as_deref(), Some("/1/3/4/5/"));
    check_all(e, t, &mut rng, 5, "root moves 5 under 4").await;
    // A cycle only s1 sees: s1 has 4 under 3; the root moving 3 under 4
    // would put 3 under itself in s1.
    let m = fails(
        &mut e.c[0],
        &format!("UPDATE {t} SET parent_id = 4 WHERE id = 3"),
        39711,
    )
    .await;
    eprintln!("cycle below refused: {m}");
    check_all(e, t, &mut rng, 5, "after the refused cycle").await;
    // s2's own row under 5; the root may not delete 5 then (39714).
    exec(&mut e.c[2], &format!("INSERT INTO {t} (id, parent_id) VALUES (100, 5)")).await;
    check_all(e, t, &mut rng, 6, "s2's own row").await;
    exec(&mut e.c[0], &format!("UPDATE {t} SET parent_id = 1 WHERE id = 5")).await;
    check_all(e, t, &mut rng, 6, "root moves 5 back").await;
    fails(&mut e.c[0], &format!("DELETE FROM {t} WHERE id = 5"), 39714).await;
    // A parent hidden in s1 can't take a child from above (39712).
    exec(&mut e.c[0], &format!("INSERT INTO {t} (id, parent_id) VALUES (6, 1)")).await;
    exec(&mut e.c[1], &format!("DELETE FROM {t} WHERE id = 6")).await;
    fails(
        &mut e.c[0],
        &format!("UPDATE {t} SET parent_id = 6 WHERE id = 2"),
        39712,
    )
    .await;
    // s1 resets its override of 4: 4 is back under 2 in s1 and s2.
    exec(&mut e.c[1], &format!("SPRIG RESET {t} WHERE id = 4")).await;
    check_all(e, t, &mut rng, 6, "s1 resets 4").await;
    // Path-only overrides go with their records.
    exec(&mut e.c[1], &format!("UPDATE {t} SET parent_id = 3 WHERE id = 2")).await;
    check_all(e, t, &mut rng, 6, "s1 moves 2 under 3").await;
    exec(&mut e.c[2], &format!("DELETE FROM {t} WHERE id = 100")).await;
    exec(&mut e.c[0], &format!("DELETE FROM {t} WHERE id = 5")).await;
    check_all(e, t, &mut rng, 6, "root deletes 5").await;
    exec(&mut e.c[0], &format!("DELETE FROM {t} WHERE id = 4")).await;
    check_all(e, t, &mut rng, 6, "root deletes 4").await;
    let left = rows(&mut e.c[2], &format!("SELECT id FROM {t} WHERE id IN (4, 5)")).await;
    assert!(left.is_empty(), "path-only overrides kept deleted rows: {left:?}");
}

async fn random_steps(e: &mut Env, seed: u64, steps: usize) {
    let t = "tr";
    e.table(t, false).await;
    // The other order: inheritable first, then the hierarchy, at the root.
    let mut rng = Rng(seed);
    let mut next = 1i64;
    let mut vals = Vec::new();
    for i in 0..40 {
        let p = if i < 3 {
            "NULL".to_owned()
        } else {
            (1 + rng.below(i) as i64).to_string()
        };
        vals.push(format!("({next}, {p})"));
        next += 1;
    }
    exec(
        &mut e.c[0],
        &format!("INSERT INTO {t} (id, parent_id) VALUES {}", vals.join(", ")),
    )
    .await;
    // s1 changes parents before the hierarchy exists; ADD HIERARCHY must
    // give s1 its own paths.
    exec(&mut e.c[1], &format!("UPDATE {t} SET parent_id = 1 WHERE id = 30")).await;
    exec(&mut e.c[0], &format!("ALTER TABLE {t} ADD HIERARCHY (parent_id)")).await;
    check_all(e, t, &mut rng, 4, "ADD HIERARCHY on an inheritable table").await;
    let mut done = BTreeMap::<&str, usize>::new();
    let mut refused = BTreeMap::<u16, usize>::new();
    for step in 0..steps {
        let level = rng.below(3);
        let all: Vec<i64> = ids(&mut e.c[level], &format!("SELECT id FROM {t}"))
            .await
            .into_iter()
            .collect();
        let a = rng.pick(&all).unwrap_or(1);
        let b = rng.pick(&all).unwrap_or(1);
        let (kind, sql) = match rng.below(10) {
            0..=2 => {
                next += 1;
                (
                    "insert",
                    format!("INSERT INTO {t} (id, parent_id) VALUES ({}, {a})", next + level as i64 * 100_000),
                )
            }
            3..=5 => ("move", format!("UPDATE {t} SET parent_id = {b} WHERE id = {a}")),
            6 => ("top", format!("UPDATE {t} SET parent_id = NULL WHERE id = {a}")),
            7 => ("rename", format!("UPDATE {t} SET name = 'n{step}' WHERE id = {a}")),
            8 => ("delete", format!("DELETE FROM {t} WHERE id = {a}")),
            _ if level > 0 => ("reset", format!("SPRIG RESET {t} WHERE id = {a}")),
            _ => ("move", format!("UPDATE {t} SET parent_id = {b} WHERE id = {a}")),
        };
        match try_exec(&mut e.c[level], &sql).await {
            Ok(_) => *done.entry(kind).or_default() += 1,
            Err((code, m)) => {
                assert!(refusal(code), "step {step} at level {level}: {sql}: {code} {m}");
                *refused.entry(code).or_default() += 1;
            }
        }
        check_all(e, t, &mut rng, 3, &format!("step {step} ({sql} at level {level})")).await;
    }
    eprintln!("random steps: done {done:?}, refused {refused:?}");
}

/// Writers at the root and at s1 at once, then a subtree moving under
/// load while s2 inserts and readers query subtrees.
async fn concurrent(e: &mut Env, ops: usize) {
    let t = "tw";
    e.table(t, true).await;
    let mut vals = vec!["(1, NULL)".to_owned()];
    for i in 2..=200 {
        vals.push(format!("({i}, {})", (i / 3).max(1)));
    }
    exec(
        &mut e.c[0],
        &format!("INSERT INTO {t} (id, parent_id) VALUES {}", vals.join(", ")),
    )
    .await;
    let mut tasks = Vec::new();
    for (w, level) in [(0usize, 0usize), (1, 0), (2, 1), (3, 1)] {
        let mut c = e.session(level).await;
        tasks.push(tokio::spawn(async move {
            let mut rng = Rng(1000 + w as u64);
            let (mut ok, mut no) = (0usize, BTreeMap::<u16, usize>::new());
            for i in 0..ops {
                let a = 2 + rng.below(199) as i64;
                let b = 1 + rng.below(200) as i64;
                let sql = if i % 3 == 0 {
                    format!(
                        "INSERT INTO {t} (id, parent_id) VALUES ({}, {b})",
                        10_000 * (w as i64 + 1) + i as i64
                    )
                } else {
                    format!("UPDATE {t} SET parent_id = {b} WHERE id = {a}")
                };
                match try_exec(&mut c, &sql).await {
                    Ok(_) => ok += 1,
                    Err((code, m)) => {
                        assert!(refusal(code), "{sql}: {code} {m}");
                        *no.entry(code).or_default() += 1;
                    }
                }
            }
            (w, ok, no)
        }));
    }
    let start = Instant::now();
    for h in tasks {
        let (w, ok, no) = h.await.unwrap();
        eprintln!("writer {w}: {ok} done, refused {no:?}");
    }
    eprintln!("two-level writers: {:?}", start.elapsed());
    let mut rng = Rng(99);
    check_all(e, t, &mut rng, 8, "after concurrent writers").await;

    // A big subtree moving back and forth at the root while s2 inserts
    // under it and a reader queries it.
    let mover = {
        let mut c = e.session(0).await;
        tokio::spawn(async move {
            let mut n = 0;
            for i in 0..ops {
                let p = if i % 2 == 0 { 1 } else { 2 };
                if try_exec(&mut c, &format!("UPDATE {t} SET parent_id = {p} WHERE id = 3"))
                    .await
                    .is_ok()
                {
                    n += 1;
                }
            }
            n
        })
    };
    let inserter = {
        let mut c = e.session(2).await;
        tokio::spawn(async move {
            let mut n = 0;
            for i in 0..ops {
                let sql = format!(
                    "INSERT INTO {t} (id, parent_id) VALUES ({}, 3)",
                    900_000 + i as i64
                );
                match try_exec(&mut c, &sql).await {
                    Ok(_) => n += 1,
                    Err((code, m)) => assert!(refusal(code), "{sql}: {code} {m}"),
                }
            }
            n
        })
    };
    let reader = {
        let mut c = e.session(1).await;
        tokio::spawn(async move {
            let deadline = Instant::now() + Duration::from_secs(600);
            let mut n = 0;
            while Instant::now() < deadline {
                let _ = ids(&mut c, &format!("SELECT id FROM {t} WHERE SPRIG_SUBTREE({t}, 3)")).await;
                n += 1;
                if n >= ops {
                    break;
                }
            }
            n
        })
    };
    let start = Instant::now();
    let (m, i, r) = (mover.await.unwrap(), inserter.await.unwrap(), reader.await.unwrap());
    eprintln!(
        "move under load: {m} moves, {i} inserts in s2, {r} subtree reads in s1, {:?}",
        start.elapsed()
    );
    check_all(e, t, &mut rng, 8, "after the move under load").await;
}

/// A user who sees part of a subtree moves it: every row's path follows.
async fn rls(e: &mut Env) {
    let t = "tl";
    e.table(t, true).await;
    exec(
        &mut e.c[0],
        &format!(
            "INSERT INTO {t} (id, parent_id, tag) VALUES (1, NULL, 'a'), (2, 1, 'a'), (3, 2, 'b'), \
             (4, 3, 'a'), (5, 1, 'a')"
        ),
    )
    .await;
    let u = format!("r6t2u{}", std::process::id() % 10_000);
    let c = &mut e.c[0];
    let _ = try_exec(c, &format!("DROP USER IF EXISTS '{u}'@'%'")).await;
    exec(c, &format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'")).await;
    exec(
        c,
        &format!("GRANT SELECT, INSERT, UPDATE, DELETE ON `{}`.* TO '{u}'@'%'", e.db),
    )
    .await;
    exec(
        c,
        &format!("CREATE POLICY only_a ON {t} FOR ALL TO PUBLIC USING (tag = 'a')"),
    )
    .await;
    exec(c, &format!("ALTER TABLE {t} ENABLE ROW LEVEL SECURITY")).await;
    exec(c, &format!("ALTER TABLE {t} FORCE ROW LEVEL SECURITY")).await;
    let mut uc = conn_as(&e.addr, &u, PW).await;
    exec(&mut uc, &format!("USE `{}`", e.db)).await;
    let seen = ids(&mut uc, &format!("SELECT id FROM {t}")).await;
    assert!(!seen.contains(&3), "the policy hides row 3: {seen:?}");
    // Moving 2 moves 3 (hidden) and 4 (under the hidden 3).
    exec(&mut uc, &format!("UPDATE {t} SET parent_id = 5 WHERE id = 2")).await;
    exec(c, &format!("ALTER TABLE {t} NO FORCE ROW LEVEL SECURITY")).await;
    exec(c, &format!("ALTER TABLE {t} DISABLE ROW LEVEL SECURITY")).await;
    let mut rng = Rng(5);
    check_all(e, t, &mut rng, 5, "move under row-level security").await;
    let p = rows(&mut e.c[1], &format!("SELECT SPRIG_PATH({t}) FROM {t} WHERE id = 4")).await;
    assert_eq!(p[0][0].as_deref(), Some("/1/5/2/3/4/"));
    exec(&mut e.c[0], &format!("DROP POLICY only_a ON {t}")).await;
    exec(&mut e.c[0], &format!("DROP USER '{u}'@'%'")).await;
}

/// What stays refused, and `DROP HIERARCHY`.
async fn refusals(e: &mut Env) {
    let t = "tf";
    e.table(t, true).await;
    exec(&mut e.c[0], &format!("INSERT INTO {t} (id, parent_id) VALUES (1, NULL), (2, 1)")).await;
    fails(
        &mut e.c[0],
        &format!("CREATE SHARE r6t2_f_{} ON {t} TO SPACE `{}` WHERE tag = 'a'", e.db, e.s1),
        39891,
    )
    .await;
    // DROP SHARE while s2 has its own row under a shared one.
    exec(&mut e.c[2], &format!("INSERT INTO {t} (id, parent_id) VALUES (50, 2)")).await;
    fails(&mut e.c[1], &format!("DROP SHARE r6t2_{t}_{}_2", e.db), 39891).await;
    exec(&mut e.c[2], &format!("DELETE FROM {t} WHERE id = 50")).await;
    // DROP HIERARCHY on a shared inheritable table: the paths go.
    exec(&mut e.c[0], &format!("ALTER TABLE {t} DROP HIERARCHY")).await;
    let n = rows(&mut e.c[2], &format!("SELECT COUNT(*) FROM {t}")).await;
    assert_eq!(n[0][0].as_deref(), Some("2"));
    let star: Vec<Row> = e.c[2].query(format!("SELECT * FROM {t}")).await.unwrap();
    assert_eq!(star[0].len(), 4);
    // And back, from the root: s1's and s2's paths come with it.
    exec(&mut e.c[0], &format!("ALTER TABLE {t} ADD HIERARCHY (parent_id)")).await;
    let mut rng = Rng(3);
    check_all(e, t, &mut rng, 2, "ADD HIERARCHY again").await;
    // From a space below the owner: refused.
    exec(&mut e.c[0], &format!("ALTER TABLE {t} DROP HIERARCHY")).await;
    fails(
        &mut e.c[1],
        &format!("ALTER TABLE {t} ADD HIERARCHY (parent_id)"),
        39891,
    )
    .await;
}

async fn run(addr: &str, tag: &str) {
    let mut e = Env::open(addr, tag).await;
    let start = Instant::now();
    t22_case(&mut e).await;
    eprintln!("[{tag}] T22's case: {:?}", start.elapsed());
    let steps: usize = std::env::var("SPRIG_TEST_HL_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    let start = Instant::now();
    random_steps(&mut e, 0x5eed + tag.len() as u64, steps).await;
    eprintln!("[{tag}] {steps} random steps: {:?}", start.elapsed());
    let ops: usize = std::env::var("SPRIG_TEST_HL_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    concurrent(&mut e, ops).await;
    rls(&mut e).await;
    refusals(&mut e).await;
    e.close().await;
}

#[tokio::test]
async fn hierarchies_across_spaces_mysql() {
    let Some(eps) = endpoints("hierarchies_across_spaces_mysql") else {
        return;
    };
    run(&eps[0], "my").await;
}

#[tokio::test]
async fn hierarchies_across_spaces_mariadb() {
    let Some(eps) = endpoints("hierarchies_across_spaces_mariadb") else {
        return;
    };
    let Some(addr) = eps.get(1) else {
        eprintln!("hierarchies_across_spaces_mariadb: no second endpoint");
        return;
    };
    run(addr, "ma").await;
}
