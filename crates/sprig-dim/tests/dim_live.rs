//! Dimensional models against plain SQL (chapter 35, "Testing"): API
//! results against MariaDB's own `GROUP BY ... WITH ROLLUP` on random
//! queries; type 2 attribution against a Rust reference; time zones across
//! a daylight-saving change.
//!
//! - `SPRIG_TEST_KV_MYSQL`: a MariaDB 11.8 as root (`mysql://root:pw@127.0.0.1:13301`).
//!
//! Skipped when unset. Uses database `r6models_dim`, dropped at the end.

#![allow(
    clippy::format_push_string,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::type_complexity,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::items_after_statements,
    clippy::needless_update,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use std::collections::BTreeMap;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_dim::query::Query;
use sprig_dim::store;
use sprig_kv::{Row, SqlError, SqlRunner};

const DB: &str = "r6models_dim";

struct My(mysql_async::Conn);

#[async_trait]
impl SqlRunner for My {
    async fn query(&mut self, sql: &str) -> Result<Vec<Row>, SqlError> {
        let rows: Vec<mysql_async::Row> = self.0.query(sql).await.map_err(err)?;
        Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(b),
                        other => Some(other.as_sql(true).trim_matches('\'').as_bytes().to_vec()),
                    })
                    .collect()
            })
            .collect())
    }

    async fn exec(&mut self, sql: &str) -> Result<u64, SqlError> {
        self.0.query_drop(sql).await.map_err(err)?;
        Ok(self.0.affected_rows())
    }
}

fn err(e: mysql_async::Error) -> SqlError {
    match e {
        mysql_async::Error::Server(s) => SqlError {
            errno: Some(s.code),
            message: s.message,
        },
        other => SqlError {
            errno: None,
            message: other.to_string(),
        },
    }
}

struct Rng(u64);
impl Rng {
    fn n(&mut self, m: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % m
    }
}

fn model(tz: &str) -> Value {
    json!({
        "model": "sales", "time_zone": tz,
        "dimensions": [
            {"name": "store", "table": "store", "key": "store_id", "scd": 2,
             "hierarchies": [{"name": "geo", "levels": ["country", "region", "city", "store_id"]}]},
            {"name": "date", "builtin": "time"}
        ],
        "facts": [{"name": "sales", "table": "sales_line", "key": ["order_id", "line_no"],
            "links": [{"dimension": "store", "column": "store_id", "attribution": "current", "time": "sold_at"},
                      {"dimension": "date", "column": "sold_at"}],
            "measures": [
                {"name": "revenue", "expr": "amount", "agg": "sum"},
                {"name": "units", "expr": "qty", "agg": "sum"},
                {"name": "lines", "expr": "line_no", "agg": "count"},
                {"name": "avg_qty", "expr": "qty", "agg": "avg"},
                {"name": "top_line", "expr": "amount", "agg": "max"},
                {"name": "low_line", "expr": "amount", "agg": "min"},
                {"name": "buyers", "expr": "customer_id", "agg": "count_distinct"},
                {"name": "paid", "expr": "amount", "agg": "sum", "filter": "status = 'paid'"}
            ]}]
    })
}

/// The reference statement's expression for a level, as plain SQL.
fn level_sql(dim: &str, level: &str, tz: &str) -> String {
    let t = format!("CONVERT_TZ(f.sold_at, '+00:00', '{tz}')");
    match (dim, level) {
        ("store", l) => format!("d1.{l}"),
        (_, "year") => format!("YEAR({t})"),
        (_, "quarter") => format!("(YEAR({t}) * 10 + QUARTER({t}))"),
        (_, "month") => format!("EXTRACT(YEAR_MONTH FROM {t})"),
        (_, "day") => format!("DATE({t})"),
        _ => unreachable!(),
    }
}

fn agg_sql(m: &str) -> &'static str {
    match m {
        "revenue" => "SUM(f.amount)",
        "units" => "SUM(f.qty)",
        "lines" => "COUNT(f.line_no)",
        "avg_qty" => "AVG(f.qty)",
        "top_line" => "MAX(f.amount)",
        "low_line" => "MIN(f.amount)",
        "buyers" => "COUNT(DISTINCT f.customer_id)",
        "paid" => "SUM(CASE WHEN f.status = 'paid' THEN f.amount END)",
        _ => unreachable!(),
    }
}

fn cells(r: &Row) -> Vec<Option<String>> {
    r.iter()
        .map(|c| c.as_ref().map(|b| String::from_utf8_lossy(b).into_owned()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn analytics_against_with_rollup() {
    let Some(url) = std::env::var("SPRIG_TEST_KV_MYSQL")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("SPRIG_TEST_KV_MYSQL not set: skipped");
        return;
    };
    let mut sql = My(
        mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
            .await
            .unwrap(),
    );
    sql.exec(&format!("DROP DATABASE IF EXISTS {DB}"))
        .await
        .unwrap();
    sql.exec(&format!("CREATE DATABASE {DB}")).await.unwrap();
    sql.exec(&format!("USE {DB}")).await.unwrap();
    sql.exec("CREATE TABLE store (store_id INT PRIMARY KEY, country VARCHAR(8) NOT NULL, region VARCHAR(16) NOT NULL, city VARCHAR(16) NOT NULL)").await.unwrap();
    sql.exec("CREATE TABLE sales_line (order_id INT NOT NULL, line_no INT NOT NULL, store_id INT NOT NULL, sold_at DATETIME NOT NULL, amount DECIMAL(10,2) NOT NULL, qty INT NOT NULL, customer_id INT NOT NULL, status VARCHAR(8) NOT NULL, PRIMARY KEY (order_id, line_no), KEY (sold_at))").await.unwrap();
    // The model turns store into a type 2 dimension (system versioning),
    // while it is still empty.
    let stored = store::put(&mut sql, DB, &model("Asia/Kolkata"), false)
        .await
        .unwrap();
    assert_eq!(stored.version, 1);
    let kind: Vec<Row> = sql.query(&format!("SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{DB}' AND TABLE_NAME = 'store'")).await.unwrap();
    assert_eq!(cells(&kind[0])[0].as_deref(), Some("SYSTEM VERSIONED"));
    // Stores exist from the end of 2024 (history timestamps set by hand).
    sql.exec("SET timestamp = UNIX_TIMESTAMP('2024-12-31 00:00:00')")
        .await
        .unwrap();
    let mut stores = Vec::new();
    for s in 1..=24 {
        let country = ["in", "sg", "us"][s % 3];
        let region = format!("{country}r{}", s % 2);
        let city = format!("{country}c{}", s % 4);
        stores.push((s, country, region.clone(), city.clone()));
        sql.exec(&format!(
            "INSERT INTO store VALUES ({s}, '{country}', '{region}', '{city}')"
        ))
        .await
        .unwrap();
    }
    sql.exec("SET timestamp = DEFAULT").await.unwrap();
    // 3,000 lines over 2025 and 2026.
    let mut rng = Rng(17);
    let mut facts = Vec::new();
    let mut batch = Vec::new();
    for o in 0..1500u64 {
        for line in 1..=2u64 {
            let store_id = rng.n(24) + 1;
            let secs = 1_735_689_600 + rng.n(2 * 365 * 86_400); // from 2025-01-01 UTC
            let amount = format!("{}.{:02}", rng.n(500), rng.n(100));
            let qty = rng.n(9) + 1;
            let cust = rng.n(400);
            let status = ["paid", "open"][rng.n(2) as usize];
            batch.push(format!("({o}, {line}, {store_id}, FROM_UNIXTIME({secs}), {amount}, {qty}, {cust}, '{status}')"));
            facts.push((store_id, secs, amount.parse::<f64>().unwrap()));
        }
    }
    sql.exec("SET time_zone = '+00:00'").await.unwrap();
    for c in batch.chunks(500) {
        sql.exec(&format!("INSERT INTO sales_line VALUES {}", c.join(", ")))
            .await
            .unwrap();
    }

    // 1. Random queries against WITH ROLLUP.
    let levels = [
        ("store", "country"),
        ("store", "region"),
        ("store", "city"),
        ("date", "year"),
        ("date", "quarter"),
        ("date", "month"),
    ];
    let all_measures = [
        "revenue", "units", "lines", "avg_qty", "top_line", "low_line", "buyers", "paid",
    ];
    let mut compared = 0;
    for round in 0..60 {
        let nd = 1 + rng.n(3) as usize;
        let mut dims: Vec<(&str, &str)> = Vec::new();
        while dims.len() < nd {
            let l = levels[rng.n(levels.len() as u64) as usize];
            if !dims.iter().any(|d| d.0 == l.0 && d.1 == l.1) {
                dims.push(l);
            }
        }
        let nm = 1 + rng.n(3) as usize;
        let mut ms: Vec<&str> = Vec::new();
        while ms.len() < nm {
            let m = all_measures[rng.n(all_measures.len() as u64) as usize];
            if !ms.contains(&m) {
                ms.push(m);
            }
        }
        let totals = rng.n(2) == 0;
        let country_filter = rng.n(3) == 0;
        let day_filter = rng.n(3) == 0;
        let mut q = json!({"model": "sales", "measures": ms, "totals": totals,
            "dimensions": dims.iter().map(|(d, l)| json!({"dimension": d, "level": l})).collect::<Vec<_>>()});
        let mut filters = Vec::new();
        let mut wh = Vec::new();
        if country_filter {
            filters.push(json!({"dimension": "store", "level": "country", "op": "in", "values": ["in", "us"]}));
            wh.push("d1.country IN ('in', 'us')".to_owned());
        }
        if day_filter {
            filters.push(json!({"dimension": "date", "level": "day", "op": "between", "values": ["2025-03-01", "2025-09-30"]}));
            wh.push("DATE(CONVERT_TZ(f.sold_at, '+00:00', 'Asia/Kolkata')) BETWEEN '2025-03-01' AND '2025-09-30'".to_owned());
        }
        q["filters"] = json!(filters);
        let query = Query::parse(&q).unwrap();
        let got = store::query(&mut sql, DB, &query, 1_000_000).await.unwrap();
        let keys: Vec<String> = dims
            .iter()
            .enumerate()
            .map(|(i, (d, l))| format!("{} AS k{}", level_sql(d, l, "Asia/Kolkata"), i + 1))
            .collect();
        let aggs: Vec<&str> = ms.iter().map(|m| agg_sql(m)).collect();
        let group: Vec<String> = (1..=dims.len()).map(|i| format!("k{i}")).collect();
        let reference = format!(
            "SELECT {}, {} FROM sales_line f LEFT JOIN store d1 ON d1.store_id = f.store_id {} GROUP BY {}{}",
            keys.join(", "),
            aggs.join(", "),
            if wh.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", wh.join(" AND "))
            },
            group.join(", "),
            if totals {
                " WITH ROLLUP".to_owned()
            } else {
                format!(" ORDER BY {}", group.join(", "))
            }
        );
        let want: Vec<Vec<Option<String>>> = sql
            .query(&reference)
            .await
            .unwrap()
            .iter()
            .map(cells)
            .collect();
        let have: Vec<Vec<Option<String>>> = got
            .rows
            .iter()
            .map(|r| {
                r.keys
                    .iter()
                    .cloned()
                    .chain(r.values.iter().cloned())
                    .collect()
            })
            .collect();
        assert_eq!(
            have, want,
            "round {round}: {q}\n{reference}\nplan {}",
            got.compiled.plan
        );
        if totals {
            // Grouping bits: the grand total has every bit.
            assert_eq!(got.rows.last().unwrap().grouping, (1 << dims.len()) - 1);
        }
        compared += want.len();
    }
    eprintln!("60 random queries, {compared} rows equal to MariaDB's WITH ROLLUP");

    // 2. Type 2 attribution: some stores change region on 2025-07-01.
    sql.exec("SET timestamp = UNIX_TIMESTAMP('2025-07-01 00:00:00')")
        .await
        .unwrap();
    sql.exec("UPDATE store SET region = CONCAT(region, 'x') WHERE store_id % 3 = 0")
        .await
        .unwrap();
    sql.exec("SET timestamp = DEFAULT").await.unwrap();
    let change = 1_751_328_000u64; // 2025-07-01 00:00:00 UTC
    for (attr, expect_old) in [("at_fact_time", true), ("current", false)] {
        let q = Query::parse(
            &json!({"model": "sales", "measures": ["revenue"], "attribution": attr,
            "dimensions": [{"dimension": "store", "level": "region"}]}),
        )
        .unwrap();
        let got = store::query(&mut sql, DB, &q, 1_000_000).await.unwrap();
        let mut want: BTreeMap<String, f64> = BTreeMap::new();
        for (s, secs, amount) in &facts {
            let st = &stores[(*s - 1) as usize];
            let changed = s % 3 == 0;
            let region = if changed && !(expect_old && *secs < change) {
                format!("{}x", st.2)
            } else {
                st.2.clone()
            };
            *want.entry(region).or_insert(0.0) += amount;
        }
        let have: BTreeMap<String, f64> = got
            .rows
            .iter()
            .map(|r| {
                (
                    r.keys[0].clone().unwrap_or_default(),
                    r.values[0].as_deref().unwrap().parse::<f64>().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            have.keys().collect::<Vec<_>>(),
            want.keys().collect::<Vec<_>>(),
            "{attr}"
        );
        for (k, v) in &want {
            assert!(
                (have[k] - v).abs() < 0.005,
                "{attr} {k}: {} vs {v}",
                have[k]
            );
        }
    }

    // 3. Time zones across New York's DST change (2026-03-08 07:00 UTC).
    store::put(
        &mut sql,
        DB,
        &{
            let mut m = model("America/New_York");
            m["model"] = json!("sales_ny");
            m
        },
        false,
    )
    .await
    .unwrap();
    sql.exec(
        "INSERT INTO sales_line VALUES \
        (90001, 1, 1, '2026-03-08 04:59:59', 1.00, 1, 1, 'paid'), \
        (90002, 1, 1, '2026-03-08 05:00:00', 2.00, 1, 1, 'paid'), \
        (90003, 1, 1, '2026-03-09 03:59:59', 4.00, 1, 1, 'paid'), \
        (90004, 1, 1, '2026-03-09 04:00:00', 8.00, 1, 1, 'paid')",
    )
    .await
    .unwrap();
    let q = Query::parse(&json!({"model": "sales_ny", "measures": ["revenue"],
        "filters": [{"dimension": "store", "level": "store_id", "op": "eq", "values": [1]},
                    {"dimension": "date", "level": "day", "op": "between", "values": ["2026-03-08", "2026-03-08"]}],
        "dimensions": [{"dimension": "date", "level": "day"}]})).unwrap();
    let got = store::query(&mut sql, DB, &q, 1_000_000).await.unwrap();
    // Local 2026-03-08 holds 05:00 UTC (EST midnight) to 04:00 UTC next day
    // (EDT midnight): the 2.00 and 4.00 lines, plus that day's random ones.
    let random: f64 = facts
        .iter()
        .filter(|(s, secs, _)| *s == 1 && *secs >= 1_772_946_000 && *secs < 1_773_028_800)
        .map(|f| f.2)
        .sum();
    assert_eq!(got.rows.len(), 1);
    assert_eq!(got.rows[0].keys[0].as_deref(), Some("2026-03-08"));
    let v: f64 = got.rows[0].values[0].as_deref().unwrap().parse().unwrap();
    assert!(
        (v - (6.0 + random)).abs() < 0.005,
        "{v} vs {}",
        6.0 + random
    );
    // Kolkata: local 2026-01-01 starts at 2025-12-31 18:30 UTC.
    let c = sprig_dim::query::compile(
        &store::get(&mut sql, DB, "sales").await.unwrap().model,
        &Query::parse(&json!({"model": "sales", "measures": ["revenue"],
            "filters": [{"dimension": "date", "level": "year", "op": "eq", "values": [2026]}]}))
        .unwrap(),
    )
    .unwrap();
    let bound: Vec<Row> = sql
        .query("SELECT CONVERT_TZ('2026-01-01 00:00:00', 'Asia/Kolkata', '+00:00')")
        .await
        .unwrap();
    assert_eq!(cells(&bound[0])[0].as_deref(), Some("2025-12-31 18:30:00"));
    assert!(
        c.sql
            .contains("CONVERT_TZ('2026-01-01 00:00:00', 'Asia/Kolkata', '+00:00')")
    );

    // 4. Drill, errors, invalid models.
    let m = store::get(&mut sql, DB, "sales").await.unwrap().model;
    let q = Query::parse(&json!({"model": "sales", "measures": ["revenue"], "dimensions": [{"dimension": "store", "level": "country"}]})).unwrap();
    let down = store::drill(&m, &q, "store", Some(&json!("in")), false).unwrap();
    assert_eq!(down.dims[0].1, "region");
    assert_eq!(down.filters[0].level, "country");
    let up = store::drill(&m, &down, "store", None, true).unwrap();
    assert_eq!(up.dims[0].1, "country");
    assert!(up.filters.is_empty());
    let e = store::get(&mut sql, DB, "nope").await.unwrap_err();
    assert_eq!(e.code(), Some(sprig_common::SprigCode::DimModelNotFound));
    let mut bad = model("UTC");
    bad["model"] = json!("bad");
    bad["facts"][0]["measures"][0]["expr"] = json!("no_such_column");
    let e = store::put(&mut sql, DB, &bad, false).await.unwrap_err();
    assert_eq!(e.code(), Some(sprig_common::SprigCode::DimInvalidModel));
    assert!(e.message.contains("no_such_column"), "{}", e.message);
    // A column the model reads disappears: the model becomes invalid.
    sql.exec("ALTER TABLE sales_line ADD COLUMN extra INT NULL")
        .await
        .unwrap();
    let mut m2 = model("UTC");
    m2["model"] = json!("uses_extra");
    m2["facts"][0]["measures"] = json!([{"name": "x", "expr": "extra", "agg": "sum"}]);
    store::put(&mut sql, DB, &m2, false).await.unwrap();
    sql.exec("ALTER TABLE sales_line DROP COLUMN extra")
        .await
        .unwrap();
    let q = Query::parse(&json!({"model": "uses_extra", "measures": ["x"]})).unwrap();
    let e = store::query(&mut sql, DB, &q, 1_000_000).await.unwrap_err();
    assert_eq!(e.code(), Some(sprig_common::SprigCode::DimInvalidModel));
    assert_eq!(
        store::get(&mut sql, DB, "uses_extra").await.unwrap().state,
        "invalid"
    );
    // Too many groups.
    let q = Query::parse(&json!({"model": "sales", "measures": ["revenue"], "dimensions": [{"dimension": "date", "level": "day"}]})).unwrap();
    let e = store::query(&mut sql, DB, &q, 10).await.unwrap_err();
    assert_eq!(e.code(), Some(sprig_common::SprigCode::DimTooManyGroups));
    sql.exec(&format!("DROP DATABASE {DB}")).await.unwrap();
}
