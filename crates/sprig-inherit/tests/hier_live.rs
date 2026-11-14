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

async fn conn_t(addr: &str, tenant: &str, user: &str, pw: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.{user}")))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("connect {addr} as {tenant}.{user}: {e}"))
}

async fn conn_as(addr: &str, user: &str, pw: &str) -> Conn {
    conn_t(addr, &tenant(), user, pw).await
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

async fn try_rows(c: &mut Conn, sql: &str) -> Result<Vec<Vec<Option<String>>>, String> {
    let r: Vec<Row> = c.query(sql).await.map_err(|e| format!("{sql}: {e}"))?;
    Ok(r.into_iter()
        .map(|r| r.unwrap().iter().map(text).collect())
        .collect())
}

async fn try_ids(c: &mut Conn, sql: &str) -> Result<BTreeSet<i64>, String> {
    Ok(try_rows(c, sql)
        .await?
        .into_iter()
        .filter_map(|r| r[0].as_deref()?.parse().ok())
        .collect())
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
    /// Shares made, with the level that owns each.
    shares: Vec<(usize, String)>,
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
            shares: Vec::new(),
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
        let (a, b) = (
            format!("r6t2_{t}_{}", self.db),
            format!("r6t2_{t}_{}_2", self.db),
        );
        exec(
            c,
            &format!(
                "CREATE SHARE {a} ON {t} TO SPACE `{}` WITH RESHARE",
                self.s1
            ),
        )
        .await;
        exec(
            &mut self.c[1],
            &format!("CREATE SHARE {b} ON {t} TO SPACE `{}`", self.s2),
        )
        .await;
        self.shares.push((1, b));
        self.shares.push((0, a));
    }

    async fn close(mut self) {
        // The spaces first: their shares go with them.
        let c = &mut self.c[0];
        exec(c, &format!("DROP SPACE IF EXISTS `{}` CASCADE", self.s2)).await;
        exec(c, &format!("DROP SPACE IF EXISTS `{}` CASCADE", self.s1)).await;
        for (_, name) in std::mem::take(&mut self.shares) {
            exec(c, &format!("DROP SHARE IF EXISTS {name}")).await;
        }
        exec(c, &format!("DROP DATABASE IF EXISTS `{}`", self.db)).await;
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
    try_check_level(c, t, probes)
        .await
        .unwrap_or_else(|e| panic!("{what}: level {level}: {e}"))
}

/// [`check_level`], reporting the first difference.
async fn try_check_level(
    c: &mut Conn,
    t: &str,
    probes: &[i64],
) -> Result<BTreeMap<i64, Option<i64>>, String> {
    let links: BTreeMap<i64, Option<i64>> = try_rows(c, &format!("SELECT id, parent_id FROM {t}"))
        .await?
        .into_iter()
        .map(|r| {
            (
                r[0].as_deref().unwrap().parse().unwrap(),
                r[1].as_deref().map(|p| p.parse().unwrap()),
            )
        })
        .collect();
    let want = model(&links)?;
    let got: BTreeMap<i64, (Option<String>, Option<i64>)> = try_rows(
        c,
        &format!("SELECT id, SPRIG_PATH({t}), SPRIG_DEPTH({t}) FROM {t}"),
    )
    .await?
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
        if (g.0.as_deref(), g.1) != (Some(p.as_str()), Some(*d)) {
            return Err(format!("row {k}'s path: got {g:?}, want ({p}, {d})"));
        }
    }
    if got.len() != want.len() {
        return Err(format!("rows: got {}, want {}", got.len(), want.len()));
    }
    // `SELECT *` hides the path columns.
    let star: Vec<Row> = c
        .query(format!("SELECT * FROM {t} LIMIT 1"))
        .await
        .map_err(|e| e.to_string())?;
    if let Some(r) = star.first()
        && r.len() != 4
    {
        return Err(format!("SELECT * has {} columns", r.len()));
    }
    for &k in probes {
        let under: BTreeSet<i64> = want
            .iter()
            .filter(|(_, (p, _))| p.contains(&format!("/{k}/")))
            .map(|(x, _)| *x)
            .collect();
        let sub = try_ids(
            c,
            &format!("SELECT id FROM {t} WHERE SPRIG_SUBTREE({t}, {k})"),
        )
        .await?;
        if sub != under {
            return Err(format!("SPRIG_SUBTREE({k}): got {sub:?}, want {under:?}"));
        }
        let desc = try_ids(
            c,
            &format!("SELECT x.id FROM {t} x WHERE SPRIG_DESCENDANTS(x, {k})"),
        )
        .await?;
        let mut d = under.clone();
        d.remove(&k);
        if desc != d {
            return Err(format!("SPRIG_DESCENDANTS({k}): got {desc:?}, want {d:?}"));
        }
        let brute = try_ids(
            c,
            &format!(
                "WITH RECURSIVE sub (id) AS (SELECT id FROM {t} WHERE id = {k} UNION ALL \
                 SELECT c.id FROM {t} c JOIN sub ON c.parent_id = sub.id) SELECT id FROM sub"
            ),
        )
        .await?;
        if brute != under {
            return Err(format!(
                "recursive CTE of {k}: got {brute:?}, want {under:?}"
            ));
        }
        let anc = try_ids(
            c,
            &format!("SELECT id FROM {t} WHERE SPRIG_ANCESTORS({t}, {k})"),
        )
        .await?;
        let mut a = BTreeSet::new();
        if let Some((p, _)) = want.get(&k) {
            a = p
                .split('/')
                .filter_map(|s| s.parse::<i64>().ok())
                .filter(|x| *x != k)
                .collect();
        }
        if anc != a {
            return Err(format!("SPRIG_ANCESTORS({k}): got {anc:?}, want {a:?}"));
        }
    }
    Ok(links)
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
    exec(
        &mut e.c[1],
        &format!("UPDATE {t} SET parent_id = 3 WHERE id = 4"),
    )
    .await;
    check_all(e, t, &mut rng, 5, "s1 moves 4").await;
    // The root moves 5 under 4: s1 must see /1/3/4/5/, the root /1/2/4/5/.
    exec(
        &mut e.c[0],
        &format!("UPDATE {t} SET parent_id = 4 WHERE id = 5"),
    )
    .await;
    let p0 = rows(
        &mut e.c[0],
        &format!("SELECT SPRIG_PATH({t}) FROM {t} WHERE id = 5"),
    )
    .await;
    let p1 = rows(
        &mut e.c[1],
        &format!("SELECT SPRIG_PATH({t}) FROM {t} WHERE id = 5"),
    )
    .await;
    let p2 = rows(
        &mut e.c[2],
        &format!("SELECT SPRIG_PATH({t}) FROM {t} WHERE id = 5"),
    )
    .await;
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
    exec(
        &mut e.c[2],
        &format!("INSERT INTO {t} (id, parent_id) VALUES (100, 5)"),
    )
    .await;
    check_all(e, t, &mut rng, 6, "s2's own row").await;
    exec(
        &mut e.c[0],
        &format!("UPDATE {t} SET parent_id = 1 WHERE id = 5"),
    )
    .await;
    check_all(e, t, &mut rng, 6, "root moves 5 back").await;
    fails(&mut e.c[0], &format!("DELETE FROM {t} WHERE id = 5"), 39714).await;
    // A parent hidden in s1 can't take a child from above (39712).
    exec(
        &mut e.c[0],
        &format!("INSERT INTO {t} (id, parent_id) VALUES (6, 1)"),
    )
    .await;
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
    exec(
        &mut e.c[1],
        &format!("UPDATE {t} SET parent_id = 3 WHERE id = 2"),
    )
    .await;
    check_all(e, t, &mut rng, 6, "s1 moves 2 under 3").await;
    exec(&mut e.c[2], &format!("DELETE FROM {t} WHERE id = 100")).await;
    exec(&mut e.c[0], &format!("DELETE FROM {t} WHERE id = 5")).await;
    check_all(e, t, &mut rng, 6, "root deletes 5").await;
    exec(&mut e.c[0], &format!("DELETE FROM {t} WHERE id = 4")).await;
    check_all(e, t, &mut rng, 6, "root deletes 4").await;
    let left = rows(
        &mut e.c[2],
        &format!("SELECT id FROM {t} WHERE id IN (4, 5)"),
    )
    .await;
    assert!(
        left.is_empty(),
        "path-only overrides kept deleted rows: {left:?}"
    );
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
    exec(
        &mut e.c[1],
        &format!("UPDATE {t} SET parent_id = 1 WHERE id = 30"),
    )
    .await;
    exec(
        &mut e.c[0],
        &format!("ALTER TABLE {t} ADD HIERARCHY (parent_id)"),
    )
    .await;
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
                    format!(
                        "INSERT INTO {t} (id, parent_id) VALUES ({}, {a})",
                        next + level as i64 * 100_000
                    ),
                )
            }
            3..=5 => (
                "move",
                format!("UPDATE {t} SET parent_id = {b} WHERE id = {a}"),
            ),
            6 => (
                "top",
                format!("UPDATE {t} SET parent_id = NULL WHERE id = {a}"),
            ),
            7 => (
                "rename",
                format!("UPDATE {t} SET name = 'n{step}' WHERE id = {a}"),
            ),
            8 => ("delete", format!("DELETE FROM {t} WHERE id = {a}")),
            _ if level > 0 => ("reset", format!("SPRIG RESET {t} WHERE id = {a}")),
            _ => (
                "move",
                format!("UPDATE {t} SET parent_id = {b} WHERE id = {a}"),
            ),
        };
        match try_exec(&mut e.c[level], &sql).await {
            Ok(_) => *done.entry(kind).or_default() += 1,
            Err((code, m)) => {
                assert!(
                    refusal(code),
                    "step {step} at level {level}: {sql}: {code} {m}"
                );
                *refused.entry(code).or_default() += 1;
            }
        }
        check_all(
            e,
            t,
            &mut rng,
            3,
            &format!("step {step} ({sql} at level {level})"),
        )
        .await;
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
                if try_exec(
                    &mut c,
                    &format!("UPDATE {t} SET parent_id = {p} WHERE id = 3"),
                )
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
                let _ = ids(
                    &mut c,
                    &format!("SELECT id FROM {t} WHERE SPRIG_SUBTREE({t}, 3)"),
                )
                .await;
                n += 1;
                if n >= ops {
                    break;
                }
            }
            n
        })
    };
    let start = Instant::now();
    let (m, i, r) = (
        mover.await.unwrap(),
        inserter.await.unwrap(),
        reader.await.unwrap(),
    );
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
        &format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON `{}`.* TO '{u}'@'%'",
            e.db
        ),
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
    exec(
        &mut uc,
        &format!("UPDATE {t} SET parent_id = 5 WHERE id = 2"),
    )
    .await;
    exec(c, &format!("ALTER TABLE {t} NO FORCE ROW LEVEL SECURITY")).await;
    exec(c, &format!("ALTER TABLE {t} DISABLE ROW LEVEL SECURITY")).await;
    let mut rng = Rng(5);
    check_all(e, t, &mut rng, 5, "move under row-level security").await;
    let p = rows(
        &mut e.c[1],
        &format!("SELECT SPRIG_PATH({t}) FROM {t} WHERE id = 4"),
    )
    .await;
    assert_eq!(p[0][0].as_deref(), Some("/1/5/2/3/4/"));
    exec(&mut e.c[0], &format!("DROP POLICY only_a ON {t}")).await;
    exec(&mut e.c[0], &format!("DROP USER '{u}'@'%'")).await;
}

/// What stays refused, and `DROP HIERARCHY`.
async fn refusals(e: &mut Env) {
    let t = "tf";
    e.table(t, true).await;
    exec(
        &mut e.c[0],
        &format!("INSERT INTO {t} (id, parent_id) VALUES (1, NULL), (2, 1)"),
    )
    .await;
    fails(
        &mut e.c[0],
        &format!(
            "CREATE SHARE r6t2_f_{} ON {t} TO SPACE `{}` WHERE tag = 'a'",
            e.db, e.s1
        ),
        39891,
    )
    .await;
    // DROP SHARE while s2 has its own row under a shared one.
    exec(
        &mut e.c[2],
        &format!("INSERT INTO {t} (id, parent_id) VALUES (50, 2)"),
    )
    .await;
    fails(
        &mut e.c[1],
        &format!("DROP SHARE r6t2_{t}_{}_2", e.db),
        39891,
    )
    .await;
    exec(&mut e.c[2], &format!("DELETE FROM {t} WHERE id = 50")).await;
    // DROP HIERARCHY on a shared inheritable table: the paths go.
    exec(&mut e.c[0], &format!("ALTER TABLE {t} DROP HIERARCHY")).await;
    let n = rows(&mut e.c[2], &format!("SELECT COUNT(*) FROM {t}")).await;
    assert_eq!(n[0][0].as_deref(), Some("2"));
    let star: Vec<Row> = e.c[2].query(format!("SELECT * FROM {t}")).await.unwrap();
    assert_eq!(star[0].len(), 4);
    // And back, from the root: s1's and s2's paths come with it.
    exec(
        &mut e.c[0],
        &format!("ALTER TABLE {t} ADD HIERARCHY (parent_id)"),
    )
    .await;
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

/// A user whose grants name single tables (decision T27): its structural
/// changes are serialized too, since the system takes the lock. One such
/// session holds a move open in a transaction; another's move waits for
/// its commit. Then two such users and two `app` sessions write at once.
async fn table_grants(e: &mut Env, ops: usize) {
    let t = "tg";
    e.table(t, true).await;
    let mut vals = vec!["(1, NULL)".to_owned()];
    for i in 2..=60 {
        vals.push(format!("({i}, {})", (i / 3).max(1)));
    }
    exec(
        &mut e.c[0],
        &format!("INSERT INTO {t} (id, parent_id) VALUES {}", vals.join(", ")),
    )
    .await;
    let u = format!("r6t3g{}", std::process::id() % 10_000);
    let c = &mut e.c[0];
    let _ = try_exec(c, &format!("DROP USER IF EXISTS '{u}'@'%'")).await;
    exec(c, &format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'")).await;
    exec(
        c,
        &format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON `{}`.`{t}` TO '{u}'@'%'",
            e.db
        ),
    )
    .await;
    let mut u1 = conn_as(&e.addr, &u, PW).await;
    exec(&mut u1, &format!("USE `{}`", e.db)).await;
    let mut u2 = conn_as(&e.addr, &u, PW).await;
    exec(&mut u2, &format!("USE `{}`", e.db)).await;
    exec(&mut u1, "BEGIN").await;
    exec(
        &mut u1,
        &format!("UPDATE {t} SET parent_id = 1 WHERE id = 30"),
    )
    .await;
    let waiter = tokio::spawn(async move {
        let t0 = Instant::now();
        exec(
            &mut u2,
            &format!("UPDATE {t} SET parent_id = 1 WHERE id = 40"),
        )
        .await;
        (t0.elapsed(), u2)
    });
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        !waiter.is_finished(),
        "a second structural change ran while the first was open"
    );
    exec(&mut u1, "COMMIT").await;
    let (blocked, _) = waiter.await.unwrap();
    eprintln!("table-grant user: the second move waited {blocked:?} for the first's commit");
    assert!(blocked >= Duration::from_millis(1400), "{blocked:?}");
    let mut rng = Rng(77);
    check_all(e, t, &mut rng, 6, "after the serialized moves").await;
    // Concurrent writers: two table-grant sessions at the root, two app
    // sessions in s1.
    let mut tasks = Vec::new();
    for w in 0..4usize {
        let mut c = if w < 2 {
            let mut x = conn_as(&e.addr, &u, PW).await;
            exec(&mut x, &format!("USE `{}`", e.db)).await;
            x
        } else {
            e.session(1).await
        };
        tasks.push(tokio::spawn(async move {
            let mut rng = Rng(500 + w as u64);
            let (mut ok, mut no) = (0usize, BTreeMap::<u16, usize>::new());
            for i in 0..ops {
                let a = 2 + rng.below(59) as i64;
                let b = 1 + rng.below(60) as i64;
                let sql = if i % 4 == 0 {
                    format!(
                        "INSERT INTO {t} (id, parent_id) VALUES ({}, {b})",
                        20_000 * (w as i64 + 1) + i as i64
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
        eprintln!("table-grant writer {w}: {ok} done, refused {no:?}");
    }
    eprintln!("table-grant writers: {:?}", start.elapsed());
    check_all(e, t, &mut rng, 8, "after the table-grant writers").await;
    exec(&mut e.c[0], &format!("DROP USER '{u}'@'%'")).await;
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
    let start = Instant::now();
    table_grants(&mut e, ops).await;
    eprintln!("[{tag}] table-grant users: {:?}", start.elapsed());
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

// ------------------------------------------------------------ across tenants

/// `a,b,c`: tenants `a > b > c`, each with the user `app` (decision T28).
const TENANTS: &str = "SPRIG_TEST_HL_TENANTS";

fn tenant_line(test: &str) -> Option<Vec<String>> {
    let v: Vec<String> = std::env::var(TENANTS)
        .ok()
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if v.len() == 3 {
        Some(v)
    } else {
        assert!(
            std::env::var("SPRIG_TEST_STRICT").ok().as_deref() != Some("1"),
            "{test}: {TENANTS} is not set"
        );
        eprintln!("{test}: skipped, set {TENANTS}=a,b,c");
        None
    }
}

/// `SPRIG_TEST_HL_MODE=materialized`: the edge from `b` to `c` is a
/// materialized copy (`c` on another cluster).
fn materialized() -> bool {
    std::env::var("SPRIG_TEST_HL_MODE").ok().as_deref() == Some("materialized")
}

/// The levels: `a`'s root, `b`'s root, `b`'s space `b1`, `c`'s root.
const LEVELS: usize = 4;
const TENANT_OF: [usize; LEVELS] = [0, 1, 1, 2];

struct Multi {
    addr: String,
    db: String,
    t: Vec<String>,
    b1: String,
    c: Vec<Conn>,
    /// Shares made: (level, name).
    shares: Vec<(usize, String)>,
    /// Tables made.
    tables: Vec<String>,
}

impl Multi {
    async fn open(addr: &str, t: Vec<String>, tag: &str) -> Self {
        let pid = std::process::id() % 10_000;
        let db = format!("r6t3_{tag}_{pid}");
        let b1 = format!("r6t3_{tag}_{pid}_b1");
        let mut c = Vec::new();
        for lv in 0..LEVELS {
            c.push(conn_t(addr, &t[TENANT_OF[lv]], "app", PW).await);
        }
        for (lv, x) in c.iter_mut().enumerate().take(LEVELS) {
            exec(x, &format!("USE SPACE `{}`", t[TENANT_OF[lv]])).await;
        }
        let _ = try_exec(&mut c[0], "SPRIG RESET inherit.merge").await;
        let _ = try_exec(&mut c[1], "SPRIG RESET inherit.merge").await;
        exec(&mut c[1], &format!("DROP SPACE IF EXISTS `{b1}` CASCADE")).await;
        exec(&mut c[1], &format!("CREATE SPACE `{b1}`")).await;
        exec(&mut c[2], &format!("USE SPACE `{b1}`")).await;
        exec(&mut c[0], &format!("CREATE DATABASE `{db}`")).await;
        exec(&mut c[0], &format!("USE `{db}`")).await;
        Self {
            addr: addr.to_owned(),
            db,
            t,
            b1,
            c,
            shares: Vec::new(),
            tables: Vec::new(),
        }
    }

    async fn session(&self, level: usize) -> Conn {
        let mut x = conn_t(&self.addr, &self.t[TENANT_OF[level]], "app", PW).await;
        exec(&mut x, &format!("USE `{}`", self.db)).await;
        if level == 2 {
            exec(&mut x, &format!("USE SPACE `{}`", self.b1)).await;
        }
        x
    }

    /// `tb` with a hierarchy, inheritable, shared `a > b > (b1, c)`.
    async fn table(&mut self, tb: &str) {
        let a = &mut self.c[0];
        exec(
            a,
            &format!(
                "CREATE TABLE {tb} (id INT PRIMARY KEY, parent_id INT NULL, name VARCHAR(40) NOT \
                 NULL DEFAULT '', tag CHAR(1) NOT NULL DEFAULT 'a')"
            ),
        )
        .await;
        exec(a, &format!("ALTER TABLE {tb} ADD HIERARCHY (parent_id)")).await;
        exec(a, &format!("ALTER TABLE {tb} SPRIG INHERITABLE KEY (id)")).await;
        let tag = &self.db;
        let sab = format!("{tag}_{tb}_ab");
        exec(
            a,
            &format!(
                "CREATE SHARE {sab} ON {tb} TO SPACE `{}` WITH RESHARE",
                self.t[1]
            ),
        )
        .await;
        self.shares.push((0, sab));
        // b receives the table at its next statement, and passes it on.
        exec(&mut self.c[1], &format!("USE `{}`", self.db)).await;
        let sbb = format!("{tag}_{tb}_bb");
        let sbc = format!("{tag}_{tb}_bc");
        exec(
            &mut self.c[1],
            &format!("CREATE SHARE {sbb} ON {tb} TO SPACE `{}`", self.b1),
        )
        .await;
        // Only the edge to c is materialized (a share's mode is set when
        // it is made).
        if materialized() {
            exec(&mut self.c[1], "SPRIG SET inherit.merge = 'materialized'").await;
        }
        exec(
            &mut self.c[1],
            &format!("CREATE SHARE {sbc} ON {tb} TO SPACE `{}`", self.t[2]),
        )
        .await;
        if materialized() {
            exec(&mut self.c[1], "SPRIG RESET inherit.merge").await;
        }
        self.shares.push((1, sbc));
        self.shares.push((1, sbb));
        for lv in [2, 3] {
            exec(&mut self.c[lv], &format!("USE `{}`", self.db)).await;
        }
        self.tables.push(tb.to_owned());
    }

    /// Best effort: the children's own rows and overrides go, then the
    /// shares (bottom first), the space and the databases.
    async fn close(mut self) {
        for tb in self.tables.clone() {
            for lv in [3, 2, 1] {
                let c = &mut self.c[lv];
                let _ = try_exec(c, &format!("SPRIG RESET {tb}")).await;
                let lo = lv as i64 * 1_000_000;
                let _ = try_exec(
                    c,
                    &format!(
                        "DELETE FROM {tb} WHERE id >= {lo} AND id < {}",
                        lo + 1_000_000
                    ),
                )
                .await;
            }
        }
        for (lv, name) in std::mem::take(&mut self.shares) {
            if let Err(e) = try_exec(&mut self.c[lv], &format!("DROP SHARE IF EXISTS {name}")).await
            {
                eprintln!("cleanup: DROP SHARE {name}: {e:?}");
            }
        }
        let _ = try_exec(
            &mut self.c[1],
            &format!("DROP SPACE IF EXISTS `{}` CASCADE", self.b1),
        )
        .await;
        for lv in [3, 1, 0] {
            if let Err(e) = try_exec(
                &mut self.c[lv],
                &format!("DROP DATABASE IF EXISTS `{}`", self.db),
            )
            .await
            {
                eprintln!("cleanup: DROP DATABASE at level {lv}: {e:?}");
            }
        }
    }
}

/// Every level matches its model, once the copy worker has caught up (a
/// child tenant's paths follow a change above asynchronously): polls for
/// `SPRIG_TEST_HL_WAIT_S` seconds (60) at most.
async fn settle(m: &mut Multi, t: &str, rng: &mut Rng, n: usize, what: &str) -> Duration {
    let wait: u64 = std::env::var("SPRIG_TEST_HL_WAIT_S")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let t0 = Instant::now();
    loop {
        let mut bad = None;
        for level in 0..LEVELS {
            let all: Vec<i64> = match try_ids(&mut m.c[level], &format!("SELECT id FROM {t}")).await
            {
                Ok(v) => v.into_iter().collect(),
                Err(e) => {
                    bad = Some((level, e));
                    break;
                }
            };
            let probes: Vec<i64> = (0..n.min(all.len()))
                .filter_map(|_| rng.pick(&all))
                .collect();
            if let Err(e) = try_check_level(&mut m.c[level], t, &probes).await {
                bad = Some((level, e));
                break;
            }
        }
        let Some((level, e)) = bad else {
            return t0.elapsed();
        };
        assert!(
            t0.elapsed() < Duration::from_secs(wait),
            "{what}: level {level} not settled after {wait} s: {e}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn path_at(c: &mut Conn, t: &str, k: i64) -> Option<String> {
    rows(
        c,
        &format!("SELECT SPRIG_PATH({t}) FROM {t} WHERE id = {k}"),
    )
    .await
    .into_iter()
    .next()
    .and_then(|r| r[0].clone())
}

/// T22's case across tenants, and the refusals only a child tenant sees.
async fn tenants_case(m: &mut Multi) {
    let t = "ta";
    m.table(t).await;
    exec(
        &mut m.c[0],
        &format!(
            "INSERT INTO {t} (id, parent_id) VALUES (1, NULL), (2, 1), (3, 1), (4, 2), (5, 3)"
        ),
    )
    .await;
    let mut rng = Rng(11);
    let d = settle(m, t, &mut rng, 5, "seed").await;
    eprintln!("tenants: seed settled in {d:?}");
    // b moves 4 under 3: b's own structure, which b1 and c inherit.
    exec(
        &mut m.c[1],
        &format!("UPDATE {t} SET parent_id = 3 WHERE id = 4"),
    )
    .await;
    settle(m, t, &mut rng, 5, "b moves 4").await;
    // a moves 5 under 4: a sees /1/2/4/5/, every level of b and c
    // /1/3/4/5/ once the worker has reconciled them.
    exec(
        &mut m.c[0],
        &format!("UPDATE {t} SET parent_id = 4 WHERE id = 5"),
    )
    .await;
    let d = settle(m, t, &mut rng, 5, "a moves 5 under 4").await;
    eprintln!("tenants: a's move reached b, b1 and c in {d:?}");
    assert_eq!(
        path_at(&mut m.c[0], t, 5).await.as_deref(),
        Some("/1/2/4/5/")
    );
    for lv in 1..LEVELS {
        assert_eq!(
            path_at(&mut m.c[lv], t, 5).await.as_deref(),
            Some("/1/3/4/5/"),
            "level {lv}"
        );
    }
    // A cycle only b's levels would see is refused in a's statement.
    let msg = fails(
        &mut m.c[0],
        &format!("UPDATE {t} SET parent_id = 4 WHERE id = 3"),
        39711,
    )
    .await;
    eprintln!("tenants: cycle below refused: {msg}");
    // c's own row under 5: a may not delete 5 (39714).
    exec(
        &mut m.c[3],
        &format!("INSERT INTO {t} (id, parent_id) VALUES (3000000, 5)"),
    )
    .await;
    settle(m, t, &mut rng, 6, "c's own row").await;
    exec(
        &mut m.c[0],
        &format!("UPDATE {t} SET parent_id = 1 WHERE id = 5"),
    )
    .await;
    settle(m, t, &mut rng, 6, "a moves 5 back").await;
    let msg = fails(&mut m.c[0], &format!("DELETE FROM {t} WHERE id = 5"), 39714).await;
    eprintln!("tenants: delete above refused: {msg}");
    // A parent hidden in b can't take a child from a (39712).
    exec(
        &mut m.c[0],
        &format!("INSERT INTO {t} (id, parent_id) VALUES (6, 1)"),
    )
    .await;
    settle(m, t, &mut rng, 6, "a inserts 6").await;
    exec(&mut m.c[1], &format!("DELETE FROM {t} WHERE id = 6")).await;
    settle(m, t, &mut rng, 6, "b hides 6").await;
    let msg = fails(
        &mut m.c[0],
        &format!("UPDATE {t} SET parent_id = 6 WHERE id = 2"),
        39712,
    )
    .await;
    eprintln!("tenants: hidden parent refused: {msg}");
    // b resets 4: back under 2 everywhere below a.
    exec(&mut m.c[1], &format!("SPRIG RESET {t} WHERE id = 4")).await;
    settle(m, t, &mut rng, 6, "b resets 4").await;
    // c drops its row; a's delete of 5 goes through, and c's path-only
    // overrides go with it.
    exec(&mut m.c[3], &format!("DELETE FROM {t} WHERE id = 3000000")).await;
    settle(m, t, &mut rng, 6, "c deletes its row").await;
    exec(&mut m.c[0], &format!("DELETE FROM {t} WHERE id = 5")).await;
    settle(m, t, &mut rng, 6, "a deletes 5").await;
}

/// Random steps at the four levels, each checked once settled.
async fn tenants_random(m: &mut Multi, seed: u64, steps: usize) {
    let t = "tr";
    m.table(t).await;
    let mut rng = Rng(seed);
    let mut vals = Vec::new();
    for i in 0..40usize {
        let p = if i < 3 {
            "NULL".to_owned()
        } else {
            (1 + rng.below(i) as i64).to_string()
        };
        vals.push(format!("({}, {p})", i + 1));
    }
    exec(
        &mut m.c[0],
        &format!("INSERT INTO {t} (id, parent_id) VALUES {}", vals.join(", ")),
    )
    .await;
    settle(m, t, &mut rng, 4, "random seed").await;
    let mut next = 1000i64;
    let mut done = BTreeMap::<&str, usize>::new();
    let mut refused = BTreeMap::<u16, usize>::new();
    let start = Instant::now();
    for step in 0..steps {
        let level = rng.below(LEVELS);
        let all: Vec<i64> = ids(&mut m.c[level], &format!("SELECT id FROM {t}"))
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
                    format!(
                        "INSERT INTO {t} (id, parent_id) VALUES ({}, {a})",
                        next + level as i64 * 1_000_000
                    ),
                )
            }
            3..=5 => (
                "move",
                format!("UPDATE {t} SET parent_id = {b} WHERE id = {a}"),
            ),
            6 => (
                "top",
                format!("UPDATE {t} SET parent_id = NULL WHERE id = {a}"),
            ),
            7 => (
                "rename",
                format!("UPDATE {t} SET name = 'n{step}' WHERE id = {a}"),
            ),
            8 => ("delete", format!("DELETE FROM {t} WHERE id = {a}")),
            _ if level > 0 => ("reset", format!("SPRIG RESET {t} WHERE id = {a}")),
            _ => (
                "move",
                format!("UPDATE {t} SET parent_id = {b} WHERE id = {a}"),
            ),
        };
        match try_exec(&mut m.c[level], &sql).await {
            Ok(_) => *done.entry(kind).or_default() += 1,
            Err((code, msg)) => {
                assert!(
                    refusal(code),
                    "step {step} at level {level}: {sql}: {code} {msg}"
                );
                *refused.entry(code).or_default() += 1;
            }
        }
        settle(
            m,
            t,
            &mut rng,
            3,
            &format!("step {step} ({sql} at level {level})"),
        )
        .await;
    }
    eprintln!(
        "tenants: {steps} random steps in {:?}: done {done:?}, refused {refused:?}",
        start.elapsed()
    );
}

/// Writers at two tenant levels at once, then a subtree moving at `a`
/// while `c` inserts under it and `b` reads it.
async fn tenants_concurrent(m: &mut Multi, ops: usize) {
    let t = "tw";
    m.table(t).await;
    let mut vals = vec!["(1, NULL)".to_owned()];
    for i in 2..=200 {
        vals.push(format!("({i}, {})", (i / 3).max(1)));
    }
    exec(
        &mut m.c[0],
        &format!("INSERT INTO {t} (id, parent_id) VALUES {}", vals.join(", ")),
    )
    .await;
    let mut rng = Rng(123);
    settle(m, t, &mut rng, 4, "concurrent seed").await;
    let mut tasks = Vec::new();
    for (w, level) in [(0usize, 0usize), (1, 0), (2, 1), (3, 1)] {
        let mut c = m.session(level).await;
        tasks.push(tokio::spawn(async move {
            let mut rng = Rng(2000 + w as u64);
            let (mut ok, mut no) = (0usize, BTreeMap::<u16, usize>::new());
            for i in 0..ops {
                let a = 2 + rng.below(199) as i64;
                let b = 1 + rng.below(200) as i64;
                let sql = if i % 3 == 0 {
                    format!(
                        "INSERT INTO {t} (id, parent_id) VALUES ({}, {b})",
                        1_000_000 * level as i64 + 100_000 + 1000 * w as i64 + i as i64
                    )
                } else {
                    format!("UPDATE {t} SET parent_id = {b} WHERE id = {a}")
                };
                match try_exec(&mut c, &sql).await {
                    Ok(_) => ok += 1,
                    Err((code, msg)) => {
                        assert!(refusal(code), "{sql}: {code} {msg}");
                        *no.entry(code).or_default() += 1;
                    }
                }
            }
            (w, level, ok, no)
        }));
    }
    let start = Instant::now();
    for h in tasks {
        let (w, level, ok, no) = h.await.unwrap();
        eprintln!("tenants: writer {w} (level {level}): {ok} done, refused {no:?}");
    }
    eprintln!("tenants: writers at a and b: {:?}", start.elapsed());
    let d = settle(m, t, &mut rng, 8, "after the writers at a and b").await;
    eprintln!("tenants: settled {d:?} after the writers");
    let mover = {
        let mut c = m.session(0).await;
        tokio::spawn(async move {
            let mut n = 0;
            for i in 0..ops {
                let p = if i % 2 == 0 { 1 } else { 2 };
                if try_exec(
                    &mut c,
                    &format!("UPDATE {t} SET parent_id = {p} WHERE id = 3"),
                )
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
        let mut c = m.session(3).await;
        tokio::spawn(async move {
            let mut n = 0;
            for i in 0..ops {
                let sql = format!(
                    "INSERT INTO {t} (id, parent_id) VALUES ({}, 3)",
                    3_900_000 + i as i64
                );
                match try_exec(&mut c, &sql).await {
                    Ok(_) => n += 1,
                    Err((code, msg)) => assert!(refusal(code), "{sql}: {code} {msg}"),
                }
            }
            n
        })
    };
    let reader = {
        let mut c = m.session(1).await;
        tokio::spawn(async move {
            let mut n = 0;
            for _ in 0..ops {
                let _ = try_ids(
                    &mut c,
                    &format!("SELECT id FROM {t} WHERE SPRIG_SUBTREE({t}, 3)"),
                )
                .await;
                n += 1;
            }
            n
        })
    };
    let start = Instant::now();
    let (mv, ins, rd) = (
        mover.await.unwrap(),
        inserter.await.unwrap(),
        reader.await.unwrap(),
    );
    eprintln!(
        "tenants: move under load: {mv} moves at a, {ins} inserts at c, {rd} subtree reads at b, \
         {:?}",
        start.elapsed()
    );
    let d = settle(m, t, &mut rng, 8, "after the move under load").await;
    eprintln!("tenants: settled {d:?} after the move under load");
}

/// Row-level security in a child tenant: a user of `b` whose policy hides
/// `b`'s own row 1000003 moves row 2, above it; 1000003 and 1000004 get
/// their paths at every level below. (A child tenant's policies filter its
/// own layer rows; rows it reads from its parent are not filtered by them,
/// a gap of chapter 29 with chapter 42 reported in decision T31.)
async fn tenants_rls(m: &mut Multi) {
    let t = "tl";
    m.table(t).await;
    exec(
        &mut m.c[0],
        &format!(
            "INSERT INTO {t} (id, parent_id, tag) VALUES (1, NULL, 'a'), (2, 1, 'a'), (5, 1, 'a')"
        ),
    )
    .await;
    let mut rng = Rng(9);
    settle(m, t, &mut rng, 5, "rls seed").await;
    exec(
        &mut m.c[1],
        &format!(
            "INSERT INTO {t} (id, parent_id, tag) VALUES (1000003, 2, 'b'), (1000004, 1000003, 'a')"
        ),
    )
    .await;
    settle(m, t, &mut rng, 5, "b's own rows").await;
    let u = format!("r6t3u{}", std::process::id() % 10_000);
    let b = &mut m.c[1];
    let _ = try_exec(b, &format!("DROP USER IF EXISTS '{u}'@'%'")).await;
    exec(b, &format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'")).await;
    exec(
        b,
        &format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON `{}`.* TO '{u}'@'%'",
            m.db
        ),
    )
    .await;
    exec(
        b,
        &format!("CREATE POLICY only_a ON {t} FOR ALL TO PUBLIC USING (tag = 'a')"),
    )
    .await;
    exec(b, &format!("ALTER TABLE {t} ENABLE ROW LEVEL SECURITY")).await;
    exec(b, &format!("ALTER TABLE {t} FORCE ROW LEVEL SECURITY")).await;
    let mut uc = conn_t(&m.addr, &m.t[1], &u, PW).await;
    exec(&mut uc, &format!("USE `{}`", m.db)).await;
    let seen = ids(&mut uc, &format!("SELECT id FROM {t}")).await;
    assert!(
        !seen.contains(&1_000_003),
        "the policy hides row 1000003: {seen:?}"
    );
    exec(
        &mut uc,
        &format!("UPDATE {t} SET parent_id = 5 WHERE id = 2"),
    )
    .await;
    let b = &mut m.c[1];
    exec(b, &format!("ALTER TABLE {t} NO FORCE ROW LEVEL SECURITY")).await;
    exec(b, &format!("ALTER TABLE {t} DISABLE ROW LEVEL SECURITY")).await;
    settle(
        m,
        t,
        &mut rng,
        5,
        "a child tenant's move under row-level security",
    )
    .await;
    for lv in 1..LEVELS {
        assert_eq!(
            path_at(&mut m.c[lv], t, 1_000_004).await.as_deref(),
            Some("/1/5/2/1000003/1000004/"),
            "level {lv}"
        );
    }
    assert_eq!(path_at(&mut m.c[0], t, 2).await.as_deref(), Some("/1/2/"));
    let b = &mut m.c[1];
    exec(b, &format!("DROP POLICY only_a ON {t}")).await;
    exec(b, &format!("DROP USER '{u}'@'%'")).await;
}

/// `hierarchy.max_move_rows` bounds a reconcile (decision T29): `b1` hangs
/// a large subtree under 10, so `b`'s move of 10 rewrites its paths in
/// `b1`; past the bound it is refused (39715), and allowed again once the
/// setting is reset, without a restart.
async fn tenants_max_move(m: &mut Multi) {
    let t = "tm";
    m.table(t).await;
    let mut vals = vec![
        "(1, NULL)".to_owned(),
        "(10, 1)".to_owned(),
        "(20, 1)".to_owned(),
    ];
    for i in 21..=30 {
        vals.push(format!("({i}, 20)"));
    }
    exec(
        &mut m.c[0],
        &format!("INSERT INTO {t} (id, parent_id) VALUES {}", vals.join(", ")),
    )
    .await;
    let mut rng = Rng(31);
    settle(m, t, &mut rng, 4, "max_move seed").await;
    // b1 hangs 20 (and its ten children) under 10.
    exec(
        &mut m.c[2],
        &format!("UPDATE {t} SET parent_id = 10 WHERE id = 20"),
    )
    .await;
    settle(m, t, &mut rng, 4, "b1 moves 20 under 10").await;
    exec(&mut m.c[1], "SPRIG SET hierarchy.max_move_rows = 5").await;
    let msg = fails(
        &mut m.c[1],
        &format!("UPDATE {t} SET parent_id = NULL WHERE id = 10"),
        39715,
    )
    .await;
    assert!(msg.contains("hierarchy.max_move_rows"), "{msg}");
    eprintln!("tenants: reconcile over the bound refused: {msg}");
    exec(&mut m.c[1], "SPRIG RESET hierarchy.max_move_rows").await;
    exec(
        &mut m.c[1],
        &format!("UPDATE {t} SET parent_id = NULL WHERE id = 10"),
    )
    .await;
    settle(m, t, &mut rng, 4, "after the bound was reset").await;
}

async fn run_tenants(addr: &str, tag: &str) {
    let Some(t) = tenant_line("hierarchies_across_tenants") else {
        return;
    };
    let mut m = Multi::open(addr, t, tag).await;
    let start = Instant::now();
    tenants_case(&mut m).await;
    eprintln!(
        "[{tag}] tenants: T22's case and refusals: {:?}",
        start.elapsed()
    );
    let steps: usize = std::env::var("SPRIG_TEST_HL_TSTEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    tenants_random(&mut m, 0xbeef + tag.len() as u64, steps).await;
    let ops: usize = std::env::var("SPRIG_TEST_HL_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    tenants_concurrent(&mut m, ops).await;
    tenants_rls(&mut m).await;
    tenants_max_move(&mut m).await;
    eprintln!("[{tag}] tenants: all: {:?}", start.elapsed());
    m.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hierarchies_across_tenants_mysql() {
    let Some(eps) = endpoints("hierarchies_across_tenants_mysql") else {
        return;
    };
    let tag = if materialized() { "mym" } else { "myr" };
    run_tenants(&eps[0], tag).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hierarchies_across_tenants_mariadb() {
    let Some(eps) = endpoints("hierarchies_across_tenants_mariadb") else {
        return;
    };
    let Some(addr) = eps.get(1) else {
        eprintln!("hierarchies_across_tenants_mariadb: no second endpoint");
        return;
    };
    let tag = if materialized() { "mam" } else { "mar" };
    run_tenants(addr, tag).await;
}
