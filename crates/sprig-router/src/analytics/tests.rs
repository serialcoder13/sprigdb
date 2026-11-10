#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::unnecessary_wraps,
    clippy::unnecessary_literal_bound,
    clippy::too_many_lines
)]

//! The analytic guard, and analytic reads end to end over fake shards
//! (the live differential suite checks them against MariaDB).

use std::collections::HashMap;

use super::translate::{JoinKind, translate};
use super::*;
use crate::exec::{ShardOk, ShardRows};
use crate::gather::{GatherLimits, GatherTable};
use crate::plan::{ShardAt, Target};
use crate::reject::{Reason, Rejection};

fn tables() -> Vec<TableCols> {
    vec![
        TableCols {
            schema: "t1_d1".into(),
            name: "customers".into(),
            columns: vec!["cid".into(), "name".into(), "region".into()],
        },
        TableCols {
            schema: "t1_d1".into(),
            name: "orders".into(),
            columns: vec!["oid".into(), "cid".into(), "amount".into(), "qty".into()],
        },
    ]
}

#[test]
fn the_guard_accepts_joins_aggregates_and_windows() {
    let q = translate(
        "SELECT c.region, COUNT(*), SUM(o.amount) AS total, AVG(o.qty), MIN(c.name) \
         FROM customers c JOIN orders o ON o.cid = c.cid \
         WHERE o.qty > 1 AND c.region IN ('north', 'south') \
         GROUP BY c.region ORDER BY total DESC LIMIT 5",
        &tables(),
    )
    .unwrap();
    assert_eq!(q.scans.len(), 2);
    assert_eq!(q.scans[0].filter, ["(c.region IN ('north', 'south'))"]);
    assert_eq!(q.scans[1].filter, ["(o.qty > 1)"]);
    assert_eq!(q.joins.len(), 1);
    assert_eq!(q.joins[0].kind, JoinKind::Inner);
    assert_eq!(
        q.joins[0].on,
        [(
            ColRef {
                scan: 0,
                col: "cid".into()
            },
            ColRef {
                scan: 1,
                col: "cid".into()
            }
        )]
    );
    assert_eq!(q.order, [OrderKey::Item(2, true)]);
    assert_eq!(q.limit, Some(5));
    // Comma joins with the equality in WHERE; windows.
    let q = translate(
        "SELECT o.oid, ROW_NUMBER() OVER (PARTITION BY c.region ORDER BY o.amount DESC, o.oid) \
         FROM customers c, orders o WHERE c.cid = o.cid ORDER BY 1",
        &tables(),
    )
    .unwrap();
    assert!(q.windowed());
    assert_eq!(q.joins[0].on.len(), 1);
    // LEFT JOIN with a filter on its right side in ON.
    let q = translate(
        "SELECT c.cid, COUNT(o.oid) FROM customers c LEFT JOIN orders o \
         ON o.cid = c.cid AND o.qty > 2 GROUP BY c.cid",
        &tables(),
    )
    .unwrap();
    assert_eq!(q.joins[0].kind, JoinKind::Left);
    assert_eq!(q.scans[1].filter, ["(o.qty > 2)"]);
}

#[test]
fn the_guard_refuses_what_it_cant_show_exact() {
    for sql in [
        "SELECT c.cid + 1 FROM customers c",
        "SELECT UPPER(name) FROM customers",
        "SELECT region, COUNT(*) FROM customers GROUP BY region HAVING COUNT(*) > 1",
        "SELECT * FROM customers",
        "SELECT cid FROM customers WHERE cid IN (SELECT cid FROM orders)",
        "SELECT c.cid FROM customers c RIGHT JOIN orders o ON o.cid = c.cid",
        "SELECT c.cid FROM customers c JOIN orders o ON o.cid > c.cid",
        "SELECT c.cid FROM customers c LEFT JOIN orders o ON o.cid = c.cid WHERE o.qty > 1",
        "SELECT c.cid FROM customers c LEFT JOIN orders o ON o.cid = c.cid AND c.cid > 1",
        "SELECT cid FROM customers ORDER BY cid LIMIT 3 + 1",
        "SELECT cid FROM customers LIMIT 3",
        "SELECT name, COUNT(*) FROM customers GROUP BY region",
        "SELECT cid FROM customers UNION SELECT cid FROM orders",
        "WITH x AS (SELECT cid FROM customers) SELECT cid FROM x",
        "SELECT ROW_NUMBER() OVER () FROM customers",
        "SELECT region, COUNT(*) FROM customers GROUP BY region WITH ROLLUP",
        "SELECT cid FROM customers WHERE name = 'a\\'b'",
        "SELECT GROUP_CONCAT(name) FROM customers",
        "SELECT cid FROM nowhere",
        "SELECT cid FROM customers c JOIN orders o ON o.oid = c.cid",
    ] {
        assert!(translate(sql, &tables()).is_err(), "accepted: {sql}");
    }
    // `cid` is in both tables.
    assert!(
        translate(
            "SELECT cid FROM customers c JOIN orders o ON o.cid = c.cid",
            &tables()
        )
        .unwrap_err()
        .contains("ambiguous")
    );
}

#[test]
fn hints_choose_the_engine() {
    assert_eq!(hint("SELECT /*+ SPRIG_ANALYTICS */ 1"), Some(true));
    assert_eq!(
        hint("SELECT /*+ MAX_EXECUTION_TIME(5) sprig_gather */ 1"),
        Some(false)
    );
    assert_eq!(hint("SELECT /* SPRIG_ANALYTICS */ 1"), None);
    assert_eq!(AnalyticsMode::parse("PREFER"), AnalyticsMode::Prefer);
    assert_eq!(AnalyticsMode::parse("bogus"), AnalyticsMode::Off);
}

// ---------------------------------------------------------------------
// End to end over fake shards.

fn v(s: &str) -> Option<Bytes> {
    Some(Bytes::copy_from_slice(s.as_bytes()))
}

/// A column definition: (name, type, flags, charset, length, decimals).
fn cdef(name: &str, ty: u8, charset: u16, length: u32, decimals: u8) -> ColumnDef {
    ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        org_name: Bytes::copy_from_slice(name.as_bytes()),
        charset,
        col_type: ty,
        length,
        decimals,
        ..ColumnDef::default()
    }
}

fn int(name: &str) -> ColumnDef {
    cdef(name, ct::LONG, 63, 11, 0)
}

fn text(name: &str) -> ColumnDef {
    cdef(name, ct::VAR_STRING, 45, 80, 0)
}

fn dec(name: &str, p: u32, s: u8) -> ColumnDef {
    cdef(name, ct::NEWDECIMAL, 63, p + 2, s)
}

/// utf8mb4_general_ci-like weights: upper case, two bytes a character.
fn weights(s: &[u8]) -> Bytes {
    Bytes::from(
        s.iter()
            .flat_map(|c| [0u8, c.to_ascii_uppercase()])
            .collect::<Vec<u8>>(),
    )
}

/// One fake row: column → value.
type FakeRow = HashMap<String, Option<Bytes>>;

/// A fake world: tables of rows per shard schema, column definitions.
#[derive(Clone)]
struct World {
    /// (shard schema, table) → rows of (column → value).
    rows: HashMap<(String, String), Vec<FakeRow>>,
    defs: HashMap<String, ColumnDef>,
    answer: Vec<ColumnDef>,
    shows: HashMap<String, String>,
    limits: GatherLimits,
}

struct FakeStream {
    world: World,
    rows: Vec<Row>,
    at: usize,
}

/// Evaluates a scan statement (`SELECT e, … FROM schema.t AS a`, names
/// quoted) with `e` a column or a weight level of one (no WHERE here).
fn scan(world: &World, sql: &str) -> Vec<Row> {
    let (head, from) = sql.split_once(" FROM ").unwrap();
    let list = head.strip_prefix("SELECT ").unwrap();
    let from = from.split(" AS ").next().unwrap();
    let (schema, table) = from.split_once('.').unwrap();
    let key = (
        schema.trim_matches('`').to_owned(),
        table.trim_matches('`').to_owned(),
    );
    let rows = world.rows.get(&key).cloned().unwrap_or_default();
    let exprs: Vec<&str> = list.split(", ").collect();
    rows.iter()
        .map(|r| {
            exprs
                .iter()
                .map(|e| {
                    let weight = e.starts_with("WEIGHT_STRING(");
                    let col = e
                        .trim_start_matches("WEIGHT_STRING(")
                        .split(" LEVEL")
                        .next()
                        .unwrap()
                        .rsplit('.')
                        .next()
                        .unwrap()
                        .trim_matches('`');
                    let val = r.get(col).cloned().flatten();
                    if weight {
                        val.map(|b| weights(&b))
                    } else {
                        val
                    }
                })
                .collect()
        })
        .collect()
}

#[async_trait::async_trait]
impl ShardStream for FakeStream {
    async fn start(&mut self, sql: &str) -> Result<Option<Vec<ColumnDef>>> {
        self.rows = scan(&self.world, sql);
        Ok(Some(Vec::new()))
    }
    async fn next_row(&mut self) -> Result<Option<Row>> {
        let r = self.rows.get(self.at).cloned();
        self.at += 1;
        Ok(r)
    }
    fn warnings(&self) -> u16 {
        0
    }
}

struct FakeScratch {
    world: World,
    started: bool,
}

#[async_trait::async_trait]
impl crate::gather::Scratch for FakeScratch {
    fn cluster(&self) -> &str {
        "c1"
    }
    async fn run(&mut self, sql: &str, _b: &mut Budget) -> Result<ShardOutcome> {
        if let Some(rest) = sql.strip_prefix("SELECT ")
            && sql.ends_with(" LIMIT 0")
        {
            let list = rest.split(" FROM ").next().unwrap();
            let columns = list
                .split(", ")
                .map(|e| {
                    let c = e.rsplit('.').next().unwrap().trim_matches('`');
                    self.world.defs.get(c).cloned().unwrap_or_else(|| int(c))
                })
                .collect();
            return Ok(ShardOutcome::Rows(ShardRows {
                columns,
                rows: Vec::new(),
                warnings: 0,
            }));
        }
        Ok(ShardOutcome::Ok(ShardOk::default()))
    }
    async fn start(&mut self, _sql: &str) -> Result<Option<Vec<ColumnDef>>> {
        self.started = true;
        Ok(Some(self.world.answer.clone()))
    }
    async fn next_row(&mut self) -> Result<Option<Row>> {
        Ok(None)
    }
    fn warnings(&self) -> u16 {
        0
    }
    async fn finish(self: Box<Self>, _clean: bool) {}
}

struct FakeBackend {
    world: World,
}

#[async_trait::async_trait]
impl ShardBackend for FakeBackend {
    async fn run(&mut self, _t: &Target, sql: &str, _b: &mut Budget) -> Result<ShardOutcome> {
        if sql.starts_with("SELECT LOWER(COLUMN_NAME)") {
            // Every character column is utf8mb4_general_ci.
            let rows = self
                .world
                .defs
                .iter()
                .filter(|(_, d)| d.col_type == ct::VAR_STRING)
                .map(|(n, _)| vec![v(n), v("utf8mb4_general_ci")])
                .collect();
            return Ok(ShardOutcome::Rows(ShardRows {
                columns: vec![text("c"), text("k")],
                rows,
                warnings: 0,
            }));
        }
        let table = sql.rsplit('.').next().unwrap().trim_matches('`');
        let show = self.world.shows.get(table).cloned().unwrap();
        Ok(ShardOutcome::Rows(ShardRows {
            columns: vec![text("Table"), text("Create Table")],
            rows: vec![vec![v(table), v(&show)]],
            warnings: 0,
        }))
    }
    fn in_transaction(&self) -> bool {
        false
    }
    async fn open_stream(&mut self, _cluster: &str) -> Result<Option<Box<dyn ShardStream>>> {
        Ok(Some(Box::new(FakeStream {
            world: self.world.clone(),
            rows: Vec::new(),
            at: 0,
        })))
    }
    async fn open_scratch(&mut self) -> Result<Option<Box<dyn crate::gather::Scratch>>> {
        Ok(Some(Box::new(FakeScratch {
            world: self.world.clone(),
            started: false,
        })))
    }
    fn gather_limits(&self) -> GatherLimits {
        self.world.limits
    }
}

fn src(cluster: &str, shard: u32, schema: &str) -> Target {
    Target {
        cluster: cluster.into(),
        shard,
        sql: String::new(),
        at: ShardAt {
            group: 1,
            db: 1,
            lo: 0,
            schema: schema.into(),
        },
    }
}

fn row(pairs: &[(&str, Option<&str>)]) -> FakeRow {
    pairs
        .iter()
        .map(|(k, x)| {
            (
                (*k).to_owned(),
                x.map(|s| Bytes::copy_from_slice(s.as_bytes())),
            )
        })
        .collect()
}

fn world(answer: Vec<ColumnDef>) -> World {
    let mut rows = HashMap::new();
    // Customers over two shards on two clusters; 'North' and 'north' are
    // one group under the case-insensitive collation.
    rows.insert(
        ("s1".to_owned(), "customers".to_owned()),
        vec![
            row(&[
                ("cid", Some("1")),
                ("name", Some("ann")),
                ("region", Some("north")),
            ]),
            row(&[
                ("cid", Some("3")),
                ("name", Some("Cy")),
                ("region", Some("South")),
            ]),
            row(&[("cid", Some("5")), ("name", None), ("region", None)]),
        ],
    );
    rows.insert(
        ("s2".to_owned(), "customers".to_owned()),
        vec![
            row(&[
                ("cid", Some("2")),
                ("name", Some("bo")),
                ("region", Some("North ")),
            ]),
            row(&[
                ("cid", Some("4")),
                ("name", Some("al")),
                ("region", Some("south")),
            ]),
        ],
    );
    rows.insert(
        ("s1".to_owned(), "orders".to_owned()),
        vec![
            row(&[
                ("oid", Some("10")),
                ("cid", Some("1")),
                ("amount", Some("10.50")),
                ("qty", Some("1")),
            ]),
            row(&[
                ("oid", Some("11")),
                ("cid", Some("2")),
                ("amount", Some("2.25")),
                ("qty", Some("2")),
            ]),
            row(&[
                ("oid", Some("12")),
                ("cid", Some("3")),
                ("amount", Some("-1.00")),
                ("qty", Some("2")),
            ]),
        ],
    );
    rows.insert(
        ("s2".to_owned(), "orders".to_owned()),
        vec![
            row(&[
                ("oid", Some("13")),
                ("cid", Some("4")),
                ("amount", None),
                ("qty", Some("5")),
            ]),
            row(&[
                ("oid", Some("14")),
                ("cid", Some("1")),
                ("amount", Some("0.75")),
                ("qty", Some("2")),
            ]),
        ],
    );
    let mut defs = HashMap::new();
    for d in [
        int("cid"),
        text("name"),
        text("region"),
        int("oid"),
        dec("amount", 10, 2),
        int("qty"),
    ] {
        defs.insert(String::from_utf8_lossy(&d.name).into_owned(), d);
    }
    let mut shows = HashMap::new();
    shows.insert(
        "customers".to_owned(),
        "CREATE TABLE `customers` (\n  `cid` int(11) NOT NULL,\n  `name` varchar(20) DEFAULT NULL,\n  `region` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`cid`)\n) ENGINE=InnoDB".to_owned(),
    );
    shows.insert(
        "orders".to_owned(),
        "CREATE TABLE `orders` (\n  `oid` int(11) NOT NULL,\n  `cid` int(11) NOT NULL,\n  `amount` decimal(10,2) DEFAULT NULL,\n  `qty` int(11) DEFAULT NULL,\n  PRIMARY KEY (`oid`)\n) ENGINE=InnoDB".to_owned(),
    );
    World {
        rows,
        defs,
        answer,
        shows,
        limits: GatherLimits {
            max_rows: 100,
            max_concurrent: 2,
            analytics: AnalyticsLimits {
                mode: AnalyticsMode::Prefer,
                ..AnalyticsLimits::default()
            },
        },
    }
}

fn plan(sql: &str) -> GatherPlan {
    let t = |name: &str| GatherTable {
        schema: "t1_d1".into(),
        name: name.into(),
        sources: vec![src("c1", 1, "s1"), src("c2", 2, "s2")],
        alias: name.into(),
        filter: None,
        shards_total: 2,
    };
    GatherPlan {
        tables: vec![t("customers"), t("orders")],
        sql: sql.into(),
        reads_unsharded: false,
        fallback: Rejection::new(Reason::JoinCondition, "test"),
    }
}

async fn answer(sql: &str, cols: Vec<ColumnDef>) -> Vec<Vec<Option<String>>> {
    let mut be = FakeBackend { world: world(cols) };
    let mut budget = Budget::mb(16);
    match execute(&plan(sql), &mut be, &mut budget, None)
        .await
        .unwrap()
    {
        Outcome::Done(Output::Rows { rows, .. }) => rows
            .into_iter()
            .map(|r| {
                r.into_iter()
                    .map(|x| x.map(|b| String::from_utf8_lossy(&b).into_owned()))
                    .collect()
            })
            .collect(),
        other => panic!("{sql}: {other:?}"),
    }
}

fn s(x: &str) -> Option<String> {
    Some(x.to_owned())
}

#[tokio::test]
async fn a_join_with_aggregates_matches_mariadbs_answer() {
    // MariaDB: regions 'north'/'North ' are one group (PAD SPACE, case
    // insensitive), NULL first; SUM over DECIMAL(10,2), AVG of an INT
    // with 4 more digits, MIN by the collation.
    let rows = answer(
        "SELECT c.region, COUNT(*), SUM(o.amount), AVG(o.qty), MIN(c.name), COUNT(DISTINCT o.qty) \
         FROM customers c JOIN orders o ON o.cid = c.cid GROUP BY c.region ORDER BY c.region",
        vec![
            text("region"),
            cdef("COUNT(*)", ct::LONGLONG, 63, 21, 0),
            cdef("SUM(o.amount)", ct::NEWDECIMAL, 63, 33, 2),
            cdef("AVG(o.qty)", ct::NEWDECIMAL, 63, 14, 4),
            text("MIN(c.name)"),
            cdef("COUNT(DISTINCT o.qty)", ct::LONGLONG, 63, 21, 0),
        ],
    )
    .await;
    assert_eq!(rows.len(), 2, "{rows:?}");
    let north = &rows[0];
    assert!(matches!(north[0].as_deref(), Some("north" | "North ")));
    assert_eq!(
        &north[1..],
        [s("3"), s("13.50"), s("1.6667"), s("ann"), s("2")]
    );
    let south = &rows[1];
    assert!(matches!(south[0].as_deref(), Some("South" | "south")));
    assert_eq!(
        &south[1..],
        [s("2"), s("-1.00"), s("3.5000"), s("al"), s("2")]
    );
}

#[tokio::test]
async fn windows_and_left_joins_match_mariadbs_answer() {
    let rows = answer(
        "SELECT c.cid, COUNT(o.oid) AS n FROM customers c LEFT JOIN orders o ON o.cid = c.cid \
         GROUP BY c.cid ORDER BY n DESC, c.cid",
        vec![int("cid"), cdef("n", ct::LONGLONG, 63, 21, 0)],
    )
    .await;
    assert_eq!(
        rows,
        [
            vec![s("1"), s("2")],
            vec![s("2"), s("1")],
            vec![s("3"), s("1")],
            vec![s("4"), s("1")],
            vec![s("5"), s("0")],
        ]
    );
    let rows = answer(
        "SELECT o.oid, RANK() OVER (PARTITION BY c.region ORDER BY o.qty DESC) AS r, \
         SUM(o.amount) OVER (PARTITION BY c.region ORDER BY o.oid) AS run \
         FROM customers c JOIN orders o ON o.cid = c.cid ORDER BY o.oid",
        vec![
            int("oid"),
            cdef("r", ct::LONGLONG, 63, 21, 0),
            cdef("run", ct::NEWDECIMAL, 63, 33, 2),
        ],
    )
    .await;
    assert_eq!(
        rows,
        [
            vec![s("10"), s("3"), s("10.50")],
            vec![s("11"), s("1"), s("12.75")],
            vec![s("12"), s("2"), s("-1.00")],
            vec![s("13"), s("1"), s("-1.00")],
            vec![s("14"), s("1"), s("13.50")],
        ]
    );
}

#[tokio::test]
async fn refusals_fall_back_before_anything_is_sent() {
    let mut be = FakeBackend {
        world: world(vec![int("x")]),
    };
    let mut budget = Budget::mb(16);
    let r = execute(
        &plan("SELECT c.cid * 2 FROM customers c"),
        &mut be,
        &mut budget,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(r, Outcome::Refused(_)), "{r:?}");
    // The answer MariaDB describes isn't what the plan computes (a SUM
    // whose scale differs): refused, never a different answer.
    let mut be = FakeBackend {
        world: world(vec![cdef("s", ct::NEWDECIMAL, 63, 33, 4)]),
    };
    let r = execute(
        &plan("SELECT SUM(o.amount) AS s FROM orders o"),
        &mut be,
        &mut budget,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(r, Outcome::Refused(_)), "{r:?}");
    // Past router.analytics.max_rows: 39421.
    let mut w = world(vec![cdef("COUNT(*)", ct::LONGLONG, 63, 21, 0)]);
    w.limits.analytics.max_rows = 3;
    let mut be = FakeBackend { world: w };
    let e = execute(
        &plan("SELECT COUNT(*) FROM orders o"),
        &mut be,
        &mut budget,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::PlanLimitExceeded), "{e}");
}
