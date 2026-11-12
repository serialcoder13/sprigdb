//! Chapter 32 through a running gateway (R6 order 1, single-shard tenant):
//! types and subtypes, writes through subtypes, polymorphic reads and
//! filters, `ONLY`, abstract and auto-increment roots, schema metadata,
//! and record hierarchies checked against a reference tree kept in Rust.
//!
//! ```sh
//! export SPRIG_TEST_R6_GATEWAY=127.0.0.1:33306,127.0.0.1:33307   # mysql, mariadb persona
//! export SPRIG_TEST_R6_USER=r6-types.r6app SPRIG_TEST_R6_PASSWORD=sprig-dev-app
//! cargo test -p sprig-types --test types_live -- --nocapture --test-threads 1
//! ```
//!
//! Without the variable every test reports what it needs and passes (or
//! fails under `SPRIG_TEST_STRICT=1`). Each test creates its own database
//! (`r6_<test>_<persona>`) and drops it at the end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::uninlined_format_args,
    clippy::single_match_else,
    clippy::many_single_char_names,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row, SslOpts, Value};

const GATEWAY: &str = "SPRIG_TEST_R6_GATEWAY";

fn endpoints(test: &str) -> Option<Vec<String>> {
    match std::env::var(GATEWAY).ok().filter(|v| !v.trim().is_empty()) {
        Some(v) => Some(v.split(',').map(|s| s.trim().to_owned()).collect()),
        None => {
            assert!(
                std::env::var("SPRIG_TEST_STRICT").ok().as_deref() != Some("1"),
                "{test}: {GATEWAY} is not set"
            );
            eprintln!("{test}: skipped, set {GATEWAY}");
            None
        }
    }
}

async fn conn(addr: &str, db: Option<&str>) -> Conn {
    let user = std::env::var("SPRIG_TEST_R6_USER").unwrap_or_else(|_| "r6-types.r6app".into());
    let pass = std::env::var("SPRIG_TEST_R6_PASSWORD").unwrap_or_else(|_| "sprig-dev-app".into());
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(pass))
        .db_name(db)
        .prefer_socket(false)
        .ssl_opts(SslOpts::default().with_danger_accept_invalid_certs(true));
    Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("connect {addr}: {e}"))
}

fn persona(i: usize) -> &'static str {
    if i == 0 { "my" } else { "ma" }
}

fn errno(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(s) => s.code,
        other => panic!("not a server error: {other}"),
    }
}

fn text(v: &Value) -> String {
    match v {
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Value::NULL => "NULL".into(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        other => format!("{other:?}"),
    }
}

async fn rows(c: &mut Conn, sql: &str) -> Vec<Vec<String>> {
    let r: Vec<Row> = c.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let out: Vec<Vec<String>> = r
        .into_iter()
        .map(|r| r.unwrap().iter().map(text).collect())
        .collect();
    for row in &out {
        for v in row {
            assert!(
                sprig_sql::names::physical_name_leaks(v.as_bytes()).is_empty(),
                "{sql}: physical name in {v:?}"
            );
        }
    }
    out
}

async fn ids(c: &mut Conn, sql: &str) -> Vec<i64> {
    rows(c, sql)
        .await
        .iter()
        .map(|r| r[0].parse().unwrap())
        .collect()
}

async fn exec(c: &mut Conn, sql: &str) -> u64 {
    c.query_drop(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    c.affected_rows()
}

async fn fails(c: &mut Conn, sql: &str, code: u16) {
    let e = c.query_drop(sql).await.expect_err(sql);
    assert_eq!(errno(&e), code, "{sql}: {e}");
    assert!(
        sprig_sql::names::physical_name_leaks(e.to_string().as_bytes()).is_empty(),
        "{sql}: physical name in {e}"
    );
}

/// A small deterministic generator.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Debug, Clone)]
struct Asset {
    id: i64,
    ty: &'static str,
    name: String,
    site: i64,
    vin: Option<String>,
    payload: Option<i64>,
    doors: Option<i64>,
    floors: Option<i64>,
}

fn path_of(ty: &str) -> &'static str {
    match ty {
        "asset" => "/asset/",
        "vehicle" => "/asset/vehicle/",
        "truck" => "/asset/vehicle/truck/",
        "car" => "/asset/vehicle/car/",
        _ => "/asset/building/",
    }
}

fn is_a(ty: &str, of: &str) -> bool {
    path_of(ty).starts_with(path_of(of))
}

async fn fresh_db(c: &mut Conn, db: &str) {
    exec(c, &format!("DROP DATABASE IF EXISTS {db}")).await;
    exec(c, &format!("CREATE DATABASE {db}")).await;
    exec(c, &format!("USE {db}")).await;
}

#[tokio::test]
async fn types_and_subtypes() {
    let Some(eps) = endpoints("types_and_subtypes") else {
        return;
    };
    for (pi, ep) in eps.iter().enumerate() {
        let db = format!("r6_types_{}", persona(pi));
        let mut c = conn(ep, None).await;
        fresh_db(&mut c, &db).await;
        let t0 = Instant::now();
        exec(
            &mut c,
            "CREATE TABLE r6_site (id INT PRIMARY KEY, city VARCHAR(20))",
        )
        .await;
        exec(
            &mut c,
            "INSERT INTO r6_site VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f')",
        )
        .await;
        exec(
            &mut c,
            "CREATE TABLE asset (id BIGINT PRIMARY KEY, name VARCHAR(200) NOT NULL, site_id INT)",
        )
        .await;
        exec(
            &mut c,
            "CREATE TABLE vehicle (vin CHAR(17) NOT NULL UNIQUE) INHERITS (asset)",
        )
        .await;
        exec(
            &mut c,
            "CREATE TABLE truck (payload_kg DECIMAL(10,2)) INHERITS (vehicle)",
        )
        .await;
        exec(&mut c, "CREATE TABLE car (doors INT) INHERITS (vehicle)").await;
        exec(
            &mut c,
            "CREATE TABLE building (floors INT NOT NULL) INHERITS (asset)",
        )
        .await;
        let ddl_ms = t0.elapsed().as_millis();
        fails(
            &mut c,
            "CREATE TABLE bad (vin INT) INHERITS (vehicle)",
            39702,
        )
        .await;
        fails(
            &mut c,
            "CREATE TABLE bad (x INT) INHERITS (vehicle, building)",
            39701,
        )
        .await;
        fails(&mut c, "CREATE TABLE bad (x INT) INHERITS (nope)", 1146).await;

        // Writes through every type, in multi-row and single-row inserts.
        let mut g = Lcg(42 + pi as u64);
        let mut model: BTreeMap<i64, Asset> = BTreeMap::new();
        let kinds = ["asset", "vehicle", "truck", "car", "building"];
        let mut next_id = 1i64;
        let t1 = Instant::now();
        let mut statements = 0;
        for batch in 0..40 {
            let ty = kinds[g.below(5) as usize];
            let n = if batch % 3 == 0 {
                1
            } else {
                1 + g.below(9) as usize
            };
            let mut vals = Vec::new();
            for _ in 0..n {
                let a = Asset {
                    id: next_id,
                    ty,
                    name: format!("n{next_id}'q"),
                    site: 1 + g.below(6) as i64,
                    vin: is_a(ty, "vehicle").then(|| format!("V{next_id:05}")),
                    payload: (ty == "truck").then(|| g.below(100) as i64),
                    doors: (ty == "car").then(|| 2 + g.below(3) as i64),
                    floors: (ty == "building").then(|| 1 + g.below(30) as i64),
                };
                next_id += 1;
                let name = a.name.replace('\'', "''");
                vals.push(match ty {
                    "asset" => format!("({}, '{name}', {})", a.id, a.site),
                    "vehicle" => format!(
                        "({}, '{name}', {}, '{}')",
                        a.id,
                        a.site,
                        a.vin.as_ref().unwrap()
                    ),
                    "truck" => format!(
                        "({}, '{name}', {}, '{}', {})",
                        a.id,
                        a.site,
                        a.vin.as_ref().unwrap(),
                        a.payload.unwrap()
                    ),
                    "car" => format!(
                        "({}, '{name}', {}, '{}', {})",
                        a.id,
                        a.site,
                        a.vin.as_ref().unwrap(),
                        a.doors.unwrap()
                    ),
                    _ => format!("({}, '{name}', {}, {})", a.id, a.site, a.floors.unwrap()),
                });
                model.insert(a.id, a);
            }
            let cols = match ty {
                "asset" => "id, name, site_id",
                "vehicle" => "id, name, site_id, vin",
                "truck" => "id, name, site_id, vin, payload_kg",
                "car" => "id, name, site_id, vin, doors",
                _ => "id, name, site_id, floors",
            };
            let affected = exec(
                &mut c,
                &format!("INSERT INTO {ty} ({cols}) VALUES {}", vals.join(", ")),
            )
            .await;
            assert_eq!(affected, n as u64);
            statements += 1;
        }
        let insert_ms = t1.elapsed().as_millis();
        // A column list in another order, and none at all.
        exec(&mut c, &format!("INSERT INTO car (doors, vin, name, id, site_id) VALUES (4, 'V{0:05}', 'x', {0}, 1)", next_id)).await;
        model.insert(
            next_id,
            Asset {
                id: next_id,
                ty: "car",
                name: "x".into(),
                site: 1,
                vin: Some(format!("V{next_id:05}")),
                payload: None,
                doors: Some(4),
                floors: None,
            },
        );
        next_id += 1;
        exec(
            &mut c,
            &format!(
                "INSERT INTO truck VALUES ({0}, 'y', 2, 'V{0:05}', 7)",
                next_id
            ),
        )
        .await;
        model.insert(
            next_id,
            Asset {
                id: next_id,
                ty: "truck",
                name: "y".into(),
                site: 2,
                vin: Some(format!("V{next_id:05}")),
                payload: Some(7),
                doors: None,
                floors: None,
            },
        );
        next_id += 1;

        check(&mut c, &model).await;

        // SELECT * names the logical columns, inherited first.
        let r: Vec<Row> = c
            .query("SELECT * FROM truck ORDER BY id LIMIT 1")
            .await
            .unwrap();
        let cols: Vec<String> = r[0]
            .columns_ref()
            .iter()
            .map(|c| c.name_str().into_owned())
            .collect();
        assert_eq!(cols, ["id", "name", "site_id", "vin", "payload_kg"]);
        let tables: BTreeSet<String> = r[0]
            .columns_ref()
            .iter()
            .map(|c| c.table_str().into_owned())
            .collect();
        assert_eq!(
            tables,
            BTreeSet::from(["truck".to_owned()]),
            "result metadata names the type"
        );
        let star = rows(&mut c, "SELECT * FROM truck ORDER BY id").await;
        let want: Vec<Vec<String>> = model
            .values()
            .filter(|a| a.ty == "truck")
            .map(|a| {
                vec![
                    a.id.to_string(),
                    a.name.clone(),
                    a.site.to_string(),
                    a.vin.clone().unwrap(),
                    format!("{}.00", a.payload.unwrap()),
                ]
            })
            .collect();
        assert_eq!(star, want);

        // Polymorphic filters, joins, subqueries, grouping.
        let t2 = Instant::now();
        for site in 1..=6 {
            let got = ids(
                &mut c,
                &format!("SELECT id FROM vehicle WHERE site_id = {site} ORDER BY id"),
            )
            .await;
            let want: Vec<i64> = model
                .values()
                .filter(|a| is_a(a.ty, "vehicle") && a.site == site)
                .map(|a| a.id)
                .collect();
            assert_eq!(got, want);
        }
        let got = ids(&mut c, "SELECT a.id FROM asset a JOIN vehicle v ON v.id = a.id WHERE v.vin LIKE 'V0001%' ORDER BY a.id").await;
        let want: Vec<i64> = model
            .values()
            .filter(|a| a.vin.as_deref().is_some_and(|v| v.starts_with("V0001")))
            .map(|a| a.id)
            .collect();
        assert_eq!(got, want);
        let got = rows(
            &mut c,
            "SELECT site_id, COUNT(*) FROM vehicle GROUP BY site_id ORDER BY site_id",
        )
        .await;
        let mut counts: BTreeMap<i64, usize> = BTreeMap::new();
        for a in model.values().filter(|a| is_a(a.ty, "vehicle")) {
            *counts.entry(a.site).or_default() += 1;
        }
        let want: Vec<Vec<String>> = counts
            .iter()
            .map(|(s, n)| vec![s.to_string(), n.to_string()])
            .collect();
        assert_eq!(got, want);
        let got = ids(&mut c, "SELECT id FROM asset WHERE id IN (SELECT id FROM truck WHERE payload_kg > 50) ORDER BY id").await;
        let want: Vec<i64> = model
            .values()
            .filter(|a| a.payload.is_some_and(|p| p > 50))
            .map(|a| a.id)
            .collect();
        assert_eq!(got, want);
        let got = rows(&mut c, "SELECT s.id, COUNT(b.id) FROM r6_site s LEFT JOIN building b ON b.site_id = s.id GROUP BY s.id ORDER BY s.id").await;
        let want: Vec<Vec<String>> = (1..=6)
            .map(|s| {
                vec![
                    s.to_string(),
                    model
                        .values()
                        .filter(|a| a.ty == "building" && a.site == s)
                        .count()
                        .to_string(),
                ]
            })
            .collect();
        assert_eq!(got, want);
        let got = rows(&mut c, "SELECT COUNT(*) FROM vehicle v WHERE EXISTS (SELECT 1 FROM r6_site s WHERE s.id = v.site_id AND s.city IN ('a','b'))").await;
        let want = model
            .values()
            .filter(|a| is_a(a.ty, "vehicle") && a.site <= 2)
            .count();
        assert_eq!(got, vec![vec![want.to_string()]]);
        exec(&mut c, "SET @sprig_inheritance = 'only'").await;
        let got = ids(&mut c, "SELECT id FROM vehicle ORDER BY id").await;
        assert_eq!(
            got,
            model
                .values()
                .filter(|a| a.ty == "vehicle")
                .map(|a| a.id)
                .collect::<Vec<_>>()
        );
        exec(&mut c, "SET @sprig_inheritance = DEFAULT").await;
        let read_ms = t2.elapsed().as_millis();

        // Updates: one table forwarded, several tables with MySQL's counts.
        let n = exec(
            &mut c,
            "UPDATE vehicle SET name = CONCAT(name, '!') WHERE site_id = 2",
        )
        .await;
        let mut want = 0;
        for a in model
            .values_mut()
            .filter(|a| is_a(a.ty, "vehicle") && a.site == 2)
        {
            a.name.push('!');
            want += 1;
        }
        assert_eq!(n, want);
        let n = exec(
            &mut c,
            "UPDATE truck SET payload_kg = payload_kg + 1, name = 'T' WHERE site_id = 4",
        )
        .await;
        let mut want = 0;
        for a in model
            .values_mut()
            .filter(|a| a.ty == "truck" && a.site == 4)
        {
            a.payload = a.payload.map(|p| p + 1);
            a.name = "T".into();
            want += 1;
        }
        assert_eq!(n, want);
        assert!(
            c.info().contains(&format!("Rows matched: {want}")) || want == 0,
            "{}",
            c.info()
        );
        let n = exec(&mut c, "UPDATE car SET name = name WHERE site_id = 1").await;
        assert_eq!(n, 0, "unchanged rows are not counted");
        check(&mut c, &model).await;

        // Deletes through a subtype and with ONLY.
        let n = exec(&mut c, "DELETE FROM vehicle WHERE site_id = 5").await;
        let gone: Vec<i64> = model
            .values()
            .filter(|a| is_a(a.ty, "vehicle") && a.site == 5)
            .map(|a| a.id)
            .collect();
        assert_eq!(n, gone.len() as u64);
        for id in gone {
            model.remove(&id);
        }
        let n = exec(&mut c, "DELETE FROM ONLY asset WHERE site_id = 1").await;
        let gone: Vec<i64> = model
            .values()
            .filter(|a| a.ty == "asset" && a.site == 1)
            .map(|a| a.id)
            .collect();
        assert_eq!(n, gone.len() as u64);
        for id in gone {
            model.remove(&id);
        }
        check(&mut c, &model).await;

        // A client transaction: the plan runs under a savepoint.
        exec(&mut c, "BEGIN").await;
        exec(&mut c, &format!("INSERT INTO truck (id, name, site_id, vin, payload_kg) VALUES ({0}, 'z', 1, 'V{0:05}', 1)", next_id)).await;
        fails(
            &mut c,
            &format!(
                "INSERT INTO truck (id, name, site_id, vin) VALUES ({}, 'dup', 1, 'V{:05}')",
                next_id + 1,
                next_id
            ),
            1062,
        )
        .await;
        let in_tx = ids(
            &mut c,
            &format!("SELECT id FROM truck WHERE id >= {next_id}"),
        )
        .await;
        assert_eq!(in_tx, vec![next_id], "the failed insert left nothing");
        exec(&mut c, "ROLLBACK").await;
        assert!(
            ids(
                &mut c,
                &format!("SELECT id FROM asset WHERE id >= {next_id}")
            )
            .await
            .is_empty()
        );
        check(&mut c, &model).await;

        // Refusals.
        fails(
            &mut c,
            "INSERT INTO truck (id, name, vin, _type) VALUES (99999, 'a', 'b', 'x')",
            39705,
        )
        .await;
        fails(&mut c, "UPDATE truck SET id = 99999", 39706).await;
        fails(&mut c, "DROP TABLE vehicle", 39707).await;
        fails(&mut c, "CREATE TYPE money AS (amount DECIMAL(10,2))", 39710).await;

        // Abstract roots and auto-increment keys.
        exec(
            &mut c,
            "CREATE TABLE party (id INT PRIMARY KEY, name VARCHAR(20)) WITH (SPRIG_ABSTRACT = 1)",
        )
        .await;
        exec(&mut c, "CREATE TABLE person (age INT) INHERITS (party)").await;
        fails(&mut c, "INSERT INTO party VALUES (1, 'p')", 39703).await;
        exec(
            &mut c,
            "INSERT INTO person VALUES (1, 'ann', 30), (2, 'bob', 40)",
        )
        .await;
        assert_eq!(
            rows(&mut c, "SELECT id, name FROM party ORDER BY id").await,
            vec![
                vec!["1".to_owned(), "ann".into()],
                vec!["2".into(), "bob".into()]
            ]
        );
        exec(
            &mut c,
            "CREATE TABLE doc (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(20))",
        )
        .await;
        exec(&mut c, "INSERT INTO doc (title) VALUES ('plain')").await;
        exec(
            &mut c,
            "CREATE TABLE memo (body VARCHAR(20)) INHERITS (doc)",
        )
        .await;
        exec(
            &mut c,
            "INSERT INTO memo (title, body) VALUES ('a', 'b'), ('c', 'd')",
        )
        .await;
        let first = c.last_insert_id().unwrap();
        assert_eq!(first, 2);
        assert_eq!(
            rows(&mut c, "SELECT * FROM memo ORDER BY id").await,
            vec![
                vec!["2".to_owned(), "a".into(), "b".into()],
                vec!["3".into(), "c".into(), "d".into()],
            ]
        );

        // What schema tools see.
        let sc = rows(&mut c, "SHOW CREATE TABLE vehicle").await;
        let ddl = &sc[0][1];
        assert!(ddl.contains("INHERITS (`asset`)"), "{ddl}");
        assert!(
            !ddl.contains("_sprig") && !ddl.contains("`id`") && !ddl.contains("FOREIGN KEY"),
            "{ddl}"
        );
        let sc = rows(&mut c, "SHOW CREATE TABLE asset").await;
        assert!(
            !sc[0][1].contains("_type") && !sc[0][1].contains("_sprig"),
            "{}",
            sc[0][1]
        );
        let sc = rows(&mut c, "SHOW CREATE TABLE party").await;
        assert!(sc[0][1].contains("SPRIG_ABSTRACT"), "{}", sc[0][1]);
        let cols: Vec<String> = rows(&mut c, "SHOW COLUMNS FROM truck")
            .await
            .into_iter()
            .map(|r| r[0].clone())
            .collect();
        assert_eq!(cols, ["id", "name", "site_id", "vin", "payload_kg"]);
        let cols: Vec<String> = rows(&mut c, "SHOW FULL COLUMNS FROM asset")
            .await
            .into_iter()
            .map(|r| r[0].clone())
            .collect();
        assert_eq!(cols, ["id", "name", "site_id"]);
        let cols: Vec<String> = rows(&mut c, "DESCRIBE car")
            .await
            .into_iter()
            .map(|r| r[0].clone())
            .collect();
        assert_eq!(cols, ["id", "name", "site_id", "vin", "doors"]);
        let cols: Vec<String> = rows(
            &mut c,
            &format!(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{db}' \
                 AND TABLE_NAME = 'truck' ORDER BY ORDINAL_POSITION"
            ),
        )
        .await
        .into_iter()
        .map(|r| r[0].clone())
        .collect();
        assert_eq!(cols, ["id", "name", "site_id", "vin", "payload_kg"]);
        let cols: Vec<String> = rows(
            &mut c,
            &format!(
                "SELECT c.COLUMN_NAME FROM information_schema.COLUMNS c WHERE c.TABLE_SCHEMA = \
                 '{db}' AND c.TABLE_NAME = 'asset' ORDER BY c.ORDINAL_POSITION"
            ),
        )
        .await
        .into_iter()
        .map(|r| r[0].clone())
        .collect();
        assert_eq!(cols, ["id", "name", "site_id"]);
        let idx = rows(
            &mut c,
            &format!(
                "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '{db}' \
                 AND INDEX_NAME LIKE '%sprig%'"
            ),
        )
        .await;
        assert_eq!(idx[0][0], "0");
        let tables: Vec<String> = rows(&mut c, "SHOW TABLES")
            .await
            .into_iter()
            .map(|r| r[0].clone())
            .collect();
        let pos = |t: &str| tables.iter().position(|x| x == t).unwrap();
        assert!(
            pos("asset") < pos("vehicle")
                && pos("vehicle") < pos("car")
                && pos("vehicle") < pos("truck"),
            "{tables:?}"
        );
        assert!(
            pos("asset") < pos("building")
                && pos("party") < pos("person")
                && pos("doc") < pos("memo"),
            "{tables:?}"
        );
        assert!(
            rows(&mut c, "SELECT * FROM asset LIMIT 1").await[0].len() == 3,
            "type columns are invisible"
        );

        // Dropping a leaf, then a subtree with CASCADE: their rows stay,
        // as rows of the parent (T8, the lead's choice of 12 November).
        exec(&mut c, "DROP TABLE truck").await;
        for a in model.values_mut().filter(|a| a.ty == "truck") {
            a.ty = "vehicle";
            a.payload = None;
        }
        check_without(&mut c, &model, &["truck"]).await;
        let only = ids(&mut c, "SELECT id FROM ONLY vehicle ORDER BY id").await;
        let want: Vec<i64> = model
            .values()
            .filter(|a| a.ty == "vehicle")
            .map(|a| a.id)
            .collect();
        assert_eq!(only, want, "ONLY vehicle after DROP TABLE truck");
        let v = rows(&mut c, "SELECT id, vin FROM ONLY vehicle ORDER BY id").await;
        let want: Vec<Vec<String>> = model
            .values()
            .filter(|a| a.ty == "vehicle")
            .map(|a| vec![a.id.to_string(), a.vin.clone().unwrap()])
            .collect();
        assert_eq!(v, want, "former trucks keep their vehicle fields");
        exec(&mut c, "DROP TABLE vehicle CASCADE").await;
        for a in model.values_mut().filter(|a| is_a(a.ty, "vehicle")) {
            a.ty = "asset";
            a.vin = None;
            a.payload = None;
            a.doors = None;
        }
        let all = rows(&mut c, "SELECT id, _type FROM asset ORDER BY id").await;
        let want: Vec<Vec<String>> = model
            .values()
            .map(|a| vec![a.id.to_string(), a.ty.to_owned()])
            .collect();
        assert_eq!(all, want, "rows of a dropped subtree are the root's");
        fails(&mut c, "SELECT * FROM car", 1146).await;

        eprintln!(
            "types_and_subtypes[{}]: 5 type DDL {ddl_ms} ms; {statements} inserts of {} rows {insert_ms} ms; \
             reads {read_ms} ms",
            persona(pi),
            next_id - 1
        );
        exec(&mut c, &format!("DROP DATABASE {db}")).await;
    }
}

/// The model against polymorphic and ONLY reads.
async fn check(c: &mut Conn, model: &BTreeMap<i64, Asset>) {
    // The parent returns every subtype's rows.
    let all = rows(c, "SELECT id, name, site_id, _type FROM asset ORDER BY id").await;
    let want: Vec<Vec<String>> = model
        .values()
        .map(|a| {
            vec![
                a.id.to_string(),
                a.name.clone(),
                a.site.to_string(),
                a.ty.to_owned(),
            ]
        })
        .collect();
    assert_eq!(all, want);
    for of in ["vehicle", "truck", "car", "building"] {
        let got = ids(c, &format!("SELECT id FROM {of} ORDER BY id")).await;
        let want: Vec<i64> = model
            .values()
            .filter(|a| is_a(a.ty, of))
            .map(|a| a.id)
            .collect();
        assert_eq!(got, want, "{of}");
        let only = ids(c, &format!("SELECT id FROM ONLY {of} ORDER BY id")).await;
        let want: Vec<i64> = model
            .values()
            .filter(|a| a.ty == of)
            .map(|a| a.id)
            .collect();
        assert_eq!(only, want, "ONLY {of}");
    }
    let v = rows(c, "SELECT id, name, site_id, vin FROM vehicle ORDER BY id").await;
    let want: Vec<Vec<String>> = model
        .values()
        .filter(|a| is_a(a.ty, "vehicle"))
        .map(|a| {
            vec![
                a.id.to_string(),
                a.name.clone(),
                a.site.to_string(),
                a.vin.clone().unwrap(),
            ]
        })
        .collect();
    assert_eq!(v, want);
}

async fn check_without(c: &mut Conn, model: &BTreeMap<i64, Asset>, dropped: &[&str]) {
    let got = ids(c, "SELECT id FROM asset ORDER BY id").await;
    assert_eq!(got, model.keys().copied().collect::<Vec<_>>());
    let got = ids(c, "SELECT id FROM vehicle ORDER BY id").await;
    let want: Vec<i64> = model
        .values()
        .filter(|a| is_a(a.ty, "vehicle") && !dropped.contains(&a.ty))
        .map(|a| a.id)
        .collect();
    assert_eq!(got, want);
}

/// The reference tree.
#[derive(Debug, Clone, Default)]
struct Tree {
    parent: BTreeMap<i64, Option<i64>>,
}

impl Tree {
    fn ancestors(&self, n: i64) -> Vec<i64> {
        let mut out = Vec::new();
        let mut cur = self.parent[&n];
        while let Some(p) = cur {
            out.push(p);
            cur = self.parent[&p];
        }
        out
    }
    fn depth(&self, n: i64) -> usize {
        self.ancestors(n).len()
    }
    fn path(&self, n: i64) -> String {
        let mut a = self.ancestors(n);
        a.reverse();
        a.push(n);
        let s: Vec<String> = a.iter().map(ToString::to_string).collect();
        format!("/{}/", s.join("/"))
    }
    fn descendants(&self, n: i64) -> BTreeSet<i64> {
        self.parent
            .keys()
            .copied()
            .filter(|&x| x != n && self.ancestors(x).contains(&n))
            .collect()
    }
}

#[tokio::test]
async fn hierarchies_match_a_reference_tree() {
    let Some(eps) = endpoints("hierarchies_match_a_reference_tree") else {
        return;
    };
    for (pi, ep) in eps.iter().enumerate() {
        let db = format!("r6_hier_{}", persona(pi));
        let mut c = conn(ep, None).await;
        fresh_db(&mut c, &db).await;
        exec(&mut c, "CREATE TABLE org (id INT PRIMARY KEY, parent_id INT NULL, name VARCHAR(40), KEY (parent_id))").await;
        let mut g = Lcg(7 + pi as u64);
        let mut tree = Tree::default();
        // 150 rows before the hierarchy: the backfill gives them paths.
        let mut vals = Vec::new();
        for id in 1..=150i64 {
            let p = if id <= 3 {
                None
            } else {
                Some(1 + g.below(id as u64 - 1) as i64)
            };
            tree.parent.insert(id, p);
            vals.push(format!(
                "({id}, {}, 'n{id}')",
                p.map_or("NULL".into(), |p| p.to_string())
            ));
        }
        exec(
            &mut c,
            &format!("INSERT INTO org VALUES {}", vals.join(", ")),
        )
        .await;
        let t0 = Instant::now();
        exec(
            &mut c,
            "ALTER TABLE org ADD HIERARCHY (parent_id) ON DELETE RESTRICT MAX DEPTH 40",
        )
        .await;
        let backfill_ms = t0.elapsed().as_millis();
        // 150 more through the gateway, in batches whose rows name parents
        // inserted earlier in the same statement.
        let t1 = Instant::now();
        for start in (151..=300i64).step_by(10) {
            let mut vals = Vec::new();
            for id in start..start + 10 {
                let p = 1 + g.below(id as u64 - 1) as i64;
                tree.parent.insert(id, Some(p));
                vals.push(format!("({id}, {p}, 'n{id}')"));
            }
            assert_eq!(
                exec(
                    &mut c,
                    &format!("INSERT INTO org VALUES {}", vals.join(", "))
                )
                .await,
                10
            );
        }
        let insert_ms = t1.elapsed().as_millis();
        fails(
            &mut c,
            "INSERT INTO org VALUES (9001, 8888, 'orphan')",
            39712,
        )
        .await;
        assert!(
            ids(&mut c, "SELECT id FROM org WHERE id = 9001")
                .await
                .is_empty()
        );
        // Moves, some of them under their own subtree.
        let t2 = Instant::now();
        let (mut moved, mut cycles) = (0, 0);
        for _ in 0..60 {
            let n = 1 + g.below(300) as i64;
            let p = if g.below(10) == 0 {
                None
            } else {
                Some(1 + g.below(300) as i64)
            };
            if p == Some(n) {
                continue;
            }
            let sql = format!(
                "UPDATE org SET parent_id = {} WHERE id = {n}",
                p.map_or("NULL".into(), |p| p.to_string())
            );
            let cycle = p.is_some_and(|p| tree.ancestors(p).contains(&n));
            let too_deep = p.is_some_and(|p| {
                let sub = tree
                    .descendants(n)
                    .iter()
                    .map(|d| tree.depth(*d) - tree.depth(n))
                    .max()
                    .unwrap_or(0);
                tree.depth(p) + 1 + sub > 40
            });
            if cycle {
                fails(&mut c, &sql, 39711).await;
                cycles += 1;
            } else if too_deep {
                fails(&mut c, &sql, 39713).await;
            } else {
                exec(&mut c, &sql).await;
                tree.parent.insert(n, p);
                moved += 1;
            }
        }
        let move_ms = t2.elapsed().as_millis();
        // Depth and path of every row.
        let got = rows(
            &mut c,
            "SELECT id, SPRIG_DEPTH(org), SPRIG_PATH(org) FROM org ORDER BY id",
        )
        .await;
        for r in &got {
            let id: i64 = r[0].parse().unwrap();
            assert_eq!(r[1], tree.depth(id).to_string(), "depth of {id}");
            assert_eq!(r[2], tree.path(id), "path of {id}");
        }
        assert_eq!(got.len(), 300);
        // Subtrees, descendants and ancestors of sample nodes.
        let t3 = Instant::now();
        let mut queries = 0;
        for k in [1i64, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 299] {
            let sub = ids(
                &mut c,
                &format!("SELECT id FROM org WHERE SPRIG_SUBTREE(org, {k}) ORDER BY id"),
            )
            .await;
            let mut want: Vec<i64> = tree.descendants(k).into_iter().collect();
            want.push(k);
            want.sort_unstable();
            assert_eq!(sub, want, "subtree of {k}");
            let desc = ids(
                &mut c,
                &format!("SELECT o.id FROM org o WHERE SPRIG_DESCENDANTS(o, {k}) ORDER BY o.id"),
            )
            .await;
            assert_eq!(
                desc,
                tree.descendants(k).into_iter().collect::<Vec<_>>(),
                "descendants of {k}"
            );
            let anc = ids(
                &mut c,
                &format!("SELECT id FROM org WHERE SPRIG_ANCESTORS(org, {k}) ORDER BY id"),
            )
            .await;
            let mut want = tree.ancestors(k);
            want.sort_unstable();
            assert_eq!(anc, want, "ancestors of {k}");
            let deep = rows(&mut c, &format!(
                "SELECT COUNT(*) FROM org WHERE SPRIG_DESCENDANTS(org, {k}) AND SPRIG_DEPTH(org) <= {}",
                tree.depth(k) + 2
            ))
            .await;
            let want = tree
                .descendants(k)
                .iter()
                .filter(|d| tree.depth(**d) <= tree.depth(k) + 2)
                .count();
            assert_eq!(deep, vec![vec![want.to_string()]], "two levels under {k}");
            queries += 4;
        }
        let query_ms = t3.elapsed().as_millis();
        // A non-literal node: the subquery form.
        let n = rows(
            &mut c,
            "SELECT COUNT(*) FROM org a JOIN org b ON SPRIG_SUBTREE(a, b.id) WHERE b.id = 1",
        )
        .await;
        assert_eq!(n[0][0], (tree.descendants(1).len() + 1).to_string());
        // The hierarchy's columns stay hidden.
        assert_eq!(
            rows(&mut c, "SELECT * FROM org WHERE id = 1").await[0].len(),
            3
        );
        let sc = rows(&mut c, "SHOW CREATE TABLE org").await;
        assert!(!sc[0][1].contains("_sprig"), "{}", sc[0][1]);
        fails(&mut c, "SELECT _sprig_hpath FROM org", 1054).await;
        fails(&mut c, "UPDATE org SET id = 5000 WHERE id = 1", 39706).await;
        // RESTRICT: a row with children stays; a leaf goes.
        let parent = (1..=300)
            .find(|n| !tree.descendants(*n).is_empty())
            .unwrap();
        fails(
            &mut c,
            &format!("DELETE FROM org WHERE id = {parent}"),
            39714,
        )
        .await;
        let leaf = (1..=300)
            .rev()
            .find(|n| tree.descendants(*n).is_empty())
            .unwrap();
        assert_eq!(
            exec(&mut c, &format!("DELETE FROM org WHERE id = {leaf}")).await,
            1
        );
        tree.parent.remove(&leaf);
        // CASCADE on another table.
        exec(
            &mut c,
            "CREATE TABLE cat (code VARCHAR(20) PRIMARY KEY, up VARCHAR(20))",
        )
        .await;
        exec(
            &mut c,
            "ALTER TABLE cat ADD HIERARCHY (up) ON DELETE CASCADE",
        )
        .await;
        exec(&mut c, "INSERT INTO cat VALUES ('root', NULL), ('a/b', 'root'), ('c,d', 'a/b'), ('e%', 'c,d'), ('f', 'root')").await;
        let p = rows(&mut c, "SELECT SPRIG_PATH(cat) FROM cat WHERE code = 'e%'").await;
        assert_eq!(p[0][0], "/root/a%2Fb/c%2Cd/e%25/");
        assert_eq!(exec(&mut c, "DELETE FROM cat WHERE code = 'a/b'").await, 1);
        let left: Vec<String> = rows(&mut c, "SELECT code FROM cat ORDER BY code")
            .await
            .into_iter()
            .map(|r| r[0].clone())
            .collect();
        assert_eq!(left, ["f", "root"]);
        // A backfill over a cycle fails and leaves no columns.
        exec(&mut c, "CREATE TABLE loopy (id INT PRIMARY KEY, p INT)").await;
        exec(&mut c, "INSERT INTO loopy VALUES (1, NULL), (2, 3), (3, 2)").await;
        fails(&mut c, "ALTER TABLE loopy ADD HIERARCHY (p)", 39716).await;
        // No hierarchy is left: rows go in unchecked; fixed, it declares.
        exec(&mut c, "INSERT INTO loopy VALUES (4, 99)").await;
        exec(&mut c, "UPDATE loopy SET p = 1 WHERE id IN (2, 4)").await;
        exec(&mut c, "ALTER TABLE loopy ADD HIERARCHY (p)").await;
        let p = rows(
            &mut c,
            "SELECT id, SPRIG_PATH(loopy) FROM loopy ORDER BY id",
        )
        .await;
        assert_eq!(p[2], vec!["3".to_owned(), "/1/2/3/".into()]);
        exec(&mut c, "ALTER TABLE cat DROP HIERARCHY").await;
        exec(&mut c, "INSERT INTO cat VALUES ('g', 'nowhere')").await;
        eprintln!(
            "hierarchies[{}]: backfill of 150 rows {backfill_ms} ms; 15 inserts of 10 rows {insert_ms} ms; \
             {moved} moves and {cycles} refused cycles {move_ms} ms; {queries} hierarchy queries {query_ms} ms",
            persona(pi)
        );
        exec(&mut c, &format!("DROP DATABASE {db}")).await;
    }
}

/// Kiran's discriminator model (inheritance plan section 7, Q2): a row's
/// type is the subtype that declares its `vtype`, else the root.
struct Fleet {
    /// id to vtype.
    rows: BTreeMap<i64, String>,
    /// Declared values: value, type, parent type.
    declared: Vec<(&'static str, &'static str, &'static str)>,
}

impl Fleet {
    fn type_of(&self, v: &str) -> &'static str {
        self.declared
            .iter()
            .find(|d| d.0 == v)
            .map_or("vehicle", |d| d.1)
    }
    fn parent(&self, t: &str) -> Option<&'static str> {
        self.declared.iter().find(|d| d.1 == t).map(|d| d.2)
    }
    fn is_a(&self, t: &str, of: &str) -> bool {
        let mut cur = Some(t);
        while let Some(c) = cur {
            if c == of {
                return true;
            }
            cur = self.parent(c);
        }
        false
    }
    fn types(&self) -> Vec<&'static str> {
        let mut v = vec!["vehicle"];
        v.extend(self.declared.iter().map(|d| d.1));
        v
    }
    fn ids(&self, of: &str, only: bool) -> Vec<i64> {
        self.rows
            .iter()
            .filter(|(_, v)| {
                let t = self.type_of(v);
                if only { t == of } else { self.is_a(t, of) }
            })
            .map(|(k, _)| *k)
            .collect()
    }
}

/// Every type's polymorphic and `ONLY` reads equal the model; in joined
/// storage also through each subtype's own column (so its extension table
/// holds exactly its rows).
async fn check_fleet(c: &mut Conn, m: &Fleet, joined: bool, step: &str) {
    for t in m.types() {
        let got = ids(c, &format!("SELECT id FROM {t} ORDER BY id")).await;
        assert_eq!(got, m.ids(t, false), "{step}: FROM {t}");
        let got = ids(c, &format!("SELECT id FROM ONLY {t} ORDER BY id")).await;
        assert_eq!(got, m.ids(t, true), "{step}: FROM ONLY {t}");
        let own = match t {
            "truck" => Some("payload_kg"),
            "smalltruck" => Some("cap"),
            "car" => Some("seats"),
            _ => None,
        };
        if let (true, Some(col)) = (joined, own) {
            let got = ids(
                c,
                &format!("SELECT id FROM {t} WHERE {col} IS NULL OR {col} IS NOT NULL ORDER BY id"),
            )
            .await;
            assert_eq!(got, m.ids(t, false), "{step}: {t} through {col}");
        }
    }
    let typed = rows(c, "SELECT id, _type FROM vehicle ORDER BY id").await;
    for r in typed {
        let id: i64 = r[0].parse().unwrap();
        assert_eq!(r[1], m.type_of(&m.rows[&id]), "{step}: _type of {id}");
    }
}

#[tokio::test]
async fn discriminator_columns() {
    let Some(eps) = endpoints("discriminator_columns") else {
        return;
    };
    for (pi, ep) in eps.iter().enumerate() {
        for storage in ["single_table", "joined"] {
            let joined = storage == "joined";
            let db = format!("r6_disc_{}_{}", &storage[..1], persona(pi));
            let mut c = conn(ep, None).await;
            fresh_db(&mut c, &db).await;
            // An existing single-table schema, as Hibernate's SINGLE_TABLE
            // with @DiscriminatorColumn(name = "vtype") makes it.
            if joined {
                exec(&mut c, "CREATE TABLE vehicle (id BIGINT PRIMARY KEY, vtype VARCHAR(31) NOT NULL DEFAULT 'VEHICLE', name VARCHAR(50))").await;
                exec(&mut c, "INSERT INTO vehicle VALUES (1,'VEHICLE','v1'),(2,'TRUCK','t2'),(3,'SMALLTRUCK','s3'),(4,'CAR','c4'),(5,'truck','lower5'),(6,'MINI','m6')").await;
            } else {
                exec(&mut c, "CREATE TABLE vehicle (id BIGINT PRIMARY KEY, vtype VARCHAR(31) NOT NULL DEFAULT 'VEHICLE', name VARCHAR(50), payload_kg DECIMAL(10,2), cap INT, seats INT)").await;
                exec(&mut c, "INSERT INTO vehicle VALUES (1,'VEHICLE','v1',NULL,NULL,NULL),(2,'TRUCK','t2',1000,NULL,NULL),(3,'SMALLTRUCK','s3',100,5,NULL),(4,'CAR','c4',NULL,NULL,4),(5,'truck','lower5',NULL,NULL,NULL),(6,'MINI','m6',NULL,NULL,NULL)").await;
            }
            let mut m = Fleet {
                rows: [
                    (1, "VEHICLE"),
                    (2, "TRUCK"),
                    (3, "SMALLTRUCK"),
                    (4, "CAR"),
                    (5, "truck"),
                    (6, "MINI"),
                ]
                .into_iter()
                .map(|(k, v)| (k, v.to_owned()))
                .collect(),
                declared: Vec::new(),
            };
            let t0 = Instant::now();
            let with = if joined {
                String::new()
            } else {
                " WITH (SPRIG_STORAGE = 'single_table')".to_owned()
            };
            exec(&mut c, &format!("CREATE TABLE truck (payload_kg DECIMAL(10,2)) INHERITS (vehicle) WHERE vtype = 'TRUCK'{with}")).await;
            exec(
                &mut c,
                "CREATE TABLE car (seats INT) INHERITS (vehicle) WHERE vtype = 'CAR'",
            )
            .await;
            let declare_ms = t0.elapsed().as_millis();
            m.declared.push(("TRUCK", "truck", "vehicle"));
            m.declared.push(("CAR", "car", "vehicle"));
            check_fleet(&mut c, &m, joined, "declared truck and car").await;
            // Existing data is typed in place.
            if !joined {
                let p = rows(&mut c, "SELECT payload_kg FROM truck WHERE id = 2").await;
                assert_eq!(p, [["1000.00".to_owned()]]);
            }
            // Inserts into a subtype set the value, and refuse another.
            exec(
                &mut c,
                "INSERT INTO truck (id, name, payload_kg) VALUES (7, 't7', 70)",
            )
            .await;
            m.rows.insert(7, "TRUCK".into());
            let v = rows(&mut c, "SELECT vtype FROM vehicle WHERE id = 7").await;
            assert_eq!(v, [["TRUCK".to_owned()]]);
            fails(
                &mut c,
                "INSERT INTO truck (id, vtype) VALUES (8, 'CAR')",
                39717,
            )
            .await;
            fails(
                &mut c,
                "INSERT INTO truck (id, vtype) VALUES (8, 'SMALLTRUCK')",
                39717,
            )
            .await;
            exec(
                &mut c,
                "INSERT INTO car (id, vtype, seats) VALUES (8, 'CAR', 2)",
            )
            .await;
            m.rows.insert(8, "CAR".into());
            // Any value goes into the root, undeclared ones included.
            exec(&mut c, "INSERT INTO vehicle (id, vtype, name) VALUES (9, 'SMALLTRUCK', 's9'), (10, 'TRUCK', 't10')").await;
            m.rows.insert(9, "SMALLTRUCK".into());
            m.rows.insert(10, "TRUCK".into());
            check_fleet(&mut c, &m, joined, "inserts").await;
            // Declaring smalltruck adopts its existing rows at once.
            let t1 = Instant::now();
            exec(
                &mut c,
                "CREATE TABLE smalltruck (cap INT) INHERITS (truck) WHERE vtype = 'SMALLTRUCK'",
            )
            .await;
            let adopt_ms = t1.elapsed().as_millis();
            m.declared.push(("SMALLTRUCK", "smalltruck", "truck"));
            check_fleet(&mut c, &m, joined, "declared smalltruck").await;
            let cap = rows(&mut c, "SELECT cap FROM smalltruck WHERE id = 3").await;
            assert_eq!(cap, [[if joined { "NULL" } else { "5" }.to_owned()]]);
            // Updating vtype moves rows between types.
            exec(&mut c, "UPDATE vehicle SET vtype = 'CAR' WHERE id = 2").await;
            m.rows.insert(2, "CAR".into());
            exec(&mut c, "UPDATE truck SET vtype = 'MINI' WHERE id = 7").await;
            m.rows.insert(7, "MINI".into());
            exec(
                &mut c,
                "UPDATE vehicle SET vtype = 'SMALLTRUCK' WHERE id IN (1, 5)",
            )
            .await;
            m.rows.insert(1, "SMALLTRUCK".into());
            m.rows.insert(5, "SMALLTRUCK".into());
            check_fleet(&mut c, &m, joined, "moves").await;
            let seats = rows(&mut c, "SELECT seats FROM car WHERE id = 2").await;
            assert_eq!(
                seats,
                [["NULL".to_owned()]],
                "a new subtype's columns start at their defaults"
            );
            // Random writes, checked against the model after each.
            let mut g = Lcg(99 + pi as u64 + u64::from(joined));
            let values = ["TRUCK", "SMALLTRUCK", "CAR", "VEHICLE", "MINI", "truck"];
            let mut next = 100i64;
            let t2 = Instant::now();
            for step in 0..60 {
                let v = values[g.below(values.len() as u64) as usize];
                match g.below(5) {
                    0 | 1 => {
                        let t = ["vehicle", "truck", "smalltruck", "car"][g.below(4) as usize];
                        let tv = match t {
                            "truck" => "TRUCK",
                            "smalltruck" => "SMALLTRUCK",
                            "car" => "CAR",
                            _ => v,
                        };
                        let col = if t == "vehicle" { ", vtype" } else { "" };
                        let val = if t == "vehicle" {
                            format!(", '{tv}'")
                        } else {
                            String::new()
                        };
                        exec(
                            &mut c,
                            &format!(
                                "INSERT INTO {t} (id, name{col}) VALUES ({next}, 'r{next}'{val})"
                            ),
                        )
                        .await;
                        m.rows.insert(next, tv.to_owned());
                        next += 1;
                    }
                    2 | 3 => {
                        let Some((&id, _)) =
                            m.rows.iter().nth(g.below(m.rows.len() as u64) as usize)
                        else {
                            continue;
                        };
                        let t = m.type_of(&m.rows[&id]);
                        exec(
                            &mut c,
                            &format!("UPDATE {t} SET vtype = '{v}' WHERE id = {id}"),
                        )
                        .await;
                        m.rows.insert(id, v.to_owned());
                    }
                    _ => {
                        let Some((&id, _)) =
                            m.rows.iter().nth(g.below(m.rows.len() as u64) as usize)
                        else {
                            continue;
                        };
                        let t = m.type_of(&m.rows[&id]);
                        exec(&mut c, &format!("DELETE FROM {t} WHERE id = {id}")).await;
                        m.rows.remove(&id);
                    }
                }
                check_fleet(&mut c, &m, joined, &format!("random step {step}")).await;
            }
            let random_ms = t2.elapsed().as_millis();
            // Schema tools: the WHERE form, the root's options.
            let ct = rows(&mut c, "SHOW CREATE TABLE smalltruck").await;
            assert!(
                ct[0][1].contains("INHERITS (`truck`) WHERE `vtype` = 'SMALLTRUCK'"),
                "{}",
                ct[0][1]
            );
            let ct = rows(&mut c, "SHOW CREATE TABLE vehicle").await;
            assert!(!ct[0][1].contains("_type"), "{}", ct[0][1]);
            let opts = if joined {
                "WITH (SPRIG_DISCRIMINATOR = 'vtype')"
            } else {
                "WITH (SPRIG_STORAGE = 'single_table', SPRIG_DISCRIMINATOR = 'vtype')"
            };
            assert!(ct[0][1].ends_with(opts), "{}", ct[0][1]);
            let cols: Vec<String> = rows(&mut c, "SHOW COLUMNS FROM smalltruck")
                .await
                .into_iter()
                .map(|r| r[0].clone())
                .collect();
            assert_eq!(cols, ["id", "vtype", "name", "payload_kg", "cap"]);
            let cols: Vec<String> = rows(
                &mut c,
                &format!(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{db}' \
                     AND TABLE_NAME = 'smalltruck' ORDER BY ORDINAL_POSITION"
                ),
            )
            .await
            .into_iter()
            .map(|r| r[0].clone())
            .collect();
            assert_eq!(cols, ["id", "vtype", "name", "payload_kg", "cap"]);
            // Refusals.
            fails(&mut c, "ALTER TABLE vehicle DROP COLUMN vtype", 39710).await;
            fails(&mut c, "UPDATE vehicle SET _type = 'car'", 39705).await;
            fails(
                &mut c,
                "CREATE TABLE van (x INT) INHERITS (vehicle) WHERE vtype = 'CAR'",
                39717,
            )
            .await;
            fails(&mut c, "CREATE TABLE van (x INT) INHERITS (vehicle)", 39710).await;
            // A subtype added later by ALTER (single-table: into the root).
            exec(&mut c, "ALTER TABLE car ADD COLUMN doors INT").await;
            exec(&mut c, "UPDATE car SET doors = 4").await;
            let d = rows(&mut c, "SELECT COUNT(*) FROM car WHERE doors = 4").await;
            assert_eq!(d[0][0], m.ids("car", false).len().to_string());
            // Dropping smalltruck keeps its rows (T8, the lead's choice of
            // 12 November): their value is undeclared now, so they are the
            // root's.
            exec(&mut c, "DROP TABLE smalltruck").await;
            m.declared.retain(|d| d.1 != "smalltruck");
            exec(
                &mut c,
                &format!("INSERT INTO vehicle (id, vtype) VALUES ({next}, 'SMALLTRUCK')"),
            )
            .await;
            m.rows.insert(next, "SMALLTRUCK".into());
            check_fleet(&mut c, &m, joined, "dropped smalltruck").await;
            eprintln!(
                "discriminator[{} {storage}]: truck and car declared over 6 rows in {declare_ms} ms; \
                 smalltruck adopted its rows in {adopt_ms} ms; 60 random writes with full checks \
                 in {random_ms} ms; {} rows at the end",
                persona(pi),
                m.rows.len()
            );
            exec(&mut c, &format!("DROP DATABASE {db}")).await;
        }
    }
}

/// The types round of 12 November 2026: a single-table subtype's `NOT
/// NULL` column without a default (a CHECK on the subtype's rows, refused
/// when existing rows can't fill it), and an existing `_type` hierarchy
/// taking a discriminator column and giving it back, online.
#[tokio::test]
async fn not_null_checks_and_discriminator_conversion() {
    let Some(eps) = endpoints("not_null_checks_and_discriminator_conversion") else {
        return;
    };
    for (pi, ep) in eps.iter().enumerate() {
        let db = format!("r6_round_{}", persona(pi));
        let mut c = conn(ep, None).await;
        fresh_db(&mut c, &db).await;

        // 1. NOT NULL in single-table storage.
        exec(
            &mut c,
            "CREATE TABLE fleet (id INT PRIMARY KEY, vtype VARCHAR(20) NOT NULL, name VARCHAR(40))",
        )
        .await;
        exec(
            &mut c,
            "INSERT INTO fleet VALUES (1, 'VAN', 'v1'), (2, 'TRUCK', 't-old')",
        )
        .await;
        // A TRUCK row exists and would have no payload: refused, nothing made.
        fails(
            &mut c,
            "CREATE TABLE lorry (payload INT NOT NULL) INHERITS (fleet) WHERE vtype = 'TRUCK' \
             WITH (SPRIG_STORAGE = 'single_table')",
            39710,
        )
        .await;
        fails(&mut c, "SELECT * FROM lorry", 1146).await;
        exec(&mut c, "DELETE FROM fleet WHERE id = 2").await;
        exec(
            &mut c,
            "CREATE TABLE lorry (payload INT NOT NULL) INHERITS (fleet) WHERE vtype = 'TRUCK' \
             WITH (SPRIG_STORAGE = 'single_table')",
        )
        .await;
        exec(&mut c, "INSERT INTO lorry (id, name, payload) VALUES (3, 'l3', 30)").await;
        fails(&mut c, "INSERT INTO lorry (id, name) VALUES (4, 'l4')", 1048).await;
        fails(
            &mut c,
            "INSERT INTO lorry (id, name, payload) VALUES (4, 'l4', NULL)",
            1048,
        )
        .await;
        fails(
            &mut c,
            "INSERT INTO fleet (id, vtype, name) VALUES (4, 'TRUCK', 'via root')",
            1048,
        )
        .await;
        fails(&mut c, "UPDATE lorry SET payload = NULL", 1048).await;
        // A van becoming a truck needs a payload.
        fails(&mut c, "UPDATE fleet SET vtype = 'TRUCK' WHERE id = 1", 1048).await;
        exec(&mut c, "INSERT INTO fleet (id, vtype, name) VALUES (5, 'CAR', 'c5')").await;
        let n = rows(&mut c, "SELECT COUNT(*) FROM fleet").await;
        assert_eq!(n[0][0], "3");
        // What schema tools see: NOT NULL on the subtype, nothing internal.
        let sc = rows(&mut c, "SHOW CREATE TABLE lorry").await;
        assert!(
            sc[0][1].contains("`payload` int(11) NOT NULL") || sc[0][1].contains("`payload` int NOT NULL"),
            "{}",
            sc[0][1]
        );
        let sc = rows(&mut c, "SHOW CREATE TABLE fleet").await;
        assert!(!sc[0][1].contains("_sprig_") && !sc[0][1].contains("CHECK"), "{}", sc[0][1]);
        let cols = rows(&mut c, "SHOW COLUMNS FROM lorry").await;
        let payload = cols.iter().find(|r| r[0] == "payload").unwrap();
        assert_eq!(payload[2], "NO", "{cols:?}");
        let nn = rows(
            &mut c,
            &format!(
                "SELECT IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{db}' \
                 AND TABLE_NAME = 'lorry' AND COLUMN_NAME = 'payload'"
            ),
        )
        .await;
        assert_eq!(nn, vec![vec!["NO".to_owned()]]);
        let cc = rows(
            &mut c,
            &format!(
                "SELECT COUNT(*) FROM information_schema.CHECK_CONSTRAINTS WHERE \
                 CONSTRAINT_SCHEMA = '{db}'"
            ),
        )
        .await;
        assert_eq!(cc[0][0], "0", "checks are hidden");
        // ALTER: a NOT NULL column existing trucks can't fill is refused;
        // with a default it is added.
        fails(&mut c, "ALTER TABLE lorry ADD COLUMN axles INT NOT NULL", 39710).await;
        exec(&mut c, "ALTER TABLE lorry ADD COLUMN axles INT NOT NULL DEFAULT 2").await;
        // A subtype declared later keeps the checks, and inherits them.
        exec(
            &mut c,
            "CREATE TABLE minilorry (seats INT NOT NULL) INHERITS (lorry) WHERE vtype = 'MINI'",
        )
        .await;
        fails(
            &mut c,
            "INSERT INTO minilorry (id, name, seats) VALUES (6, 'm6', 2)",
            1048,
        )
        .await;
        exec(
            &mut c,
            "INSERT INTO minilorry (id, name, seats, payload) VALUES (6, 'm6', 2, 5)",
        )
        .await;
        fails(&mut c, "INSERT INTO lorry (id, name, payload) VALUES (7, 'l7', NULL)", 1048).await;
        assert_eq!(
            ids(&mut c, "SELECT id FROM lorry ORDER BY id").await,
            vec![3, 6]
        );
        // Dropping the subtype keeps its rows (T8) and its column, nullable.
        exec(&mut c, "DROP TABLE minilorry").await;
        exec(&mut c, "INSERT INTO fleet (id, vtype, name) VALUES (8, 'MINI', 'm8')").await;
        assert_eq!(
            ids(&mut c, "SELECT id FROM ONLY fleet ORDER BY id").await,
            vec![1, 5, 6, 8]
        );

        // 2. A `_type` hierarchy takes a discriminator, and gives it back.
        for joined in [true, false] {
            let storage = if joined { "" } else { " WITH (SPRIG_STORAGE = 'joined')" };
            exec(&mut c, "DROP TABLE IF EXISTS kart CASCADE").await;
            exec(
                &mut c,
                "CREATE TABLE kart (id INT PRIMARY KEY, kind VARCHAR(10) NOT NULL, name \
                 VARCHAR(20))",
            )
            .await;
            exec(
                &mut c,
                &format!("CREATE TABLE gokart (speed INT) INHERITS (kart){storage}"),
            )
            .await;
            exec(&mut c, "CREATE TABLE racer (wing INT) INHERITS (gokart)").await;
            exec(&mut c, "CREATE TABLE pedal (gears INT) INHERITS (kart)").await;
            exec(&mut c, "INSERT INTO kart VALUES (1, 'K', 'k1')").await;
            exec(
                &mut c,
                "INSERT INTO gokart (id, kind, name, speed) VALUES (2, 'GO', 'g2', 20), (3, 'GO', \
                 'g3', 30)",
            )
            .await;
            exec(
                &mut c,
                "INSERT INTO racer (id, kind, name, speed, wing) VALUES (4, 'RACE', 'r4', 40, 1)",
            )
            .await;
            // A row disagreeing with the column: refused, nothing changed.
            exec(&mut c, "INSERT INTO gokart (id, kind, name) VALUES (5, 'RACE', 'odd')").await;
            fails(&mut c, "ALTER TABLE kart ADD DISCRIMINATOR (kind)", 39717).await;
            exec(&mut c, "UPDATE gokart SET kind = 'GO' WHERE id = 5").await;
            // pedal has no rows: its value is given.
            fails(&mut c, "ALTER TABLE kart ADD DISCRIMINATOR (kind)", 39710).await;
            let t0 = Instant::now();
            exec(
                &mut c,
                "ALTER TABLE kart ADD DISCRIMINATOR (kind) VALUES (pedal = 'PEDAL')",
            )
            .await;
            let add_ms = t0.elapsed().as_millis();
            let sc = rows(&mut c, "SHOW CREATE TABLE racer").await;
            assert!(sc[0][1].contains("WHERE `kind` = 'RACE'"), "{}", sc[0][1]);
            let sc = rows(&mut c, "SHOW CREATE TABLE kart").await;
            assert!(sc[0][1].contains("SPRIG_DISCRIMINATOR = 'kind'"), "{}", sc[0][1]);
            assert_eq!(
                ids(&mut c, "SELECT id FROM gokart ORDER BY id").await,
                vec![2, 3, 4, 5]
            );
            // Open values from now on: a new value is the root's, a
            // declared one types its row.
            exec(&mut c, "INSERT INTO kart (id, kind, name) VALUES (6, 'PEDAL', 'p6')").await;
            exec(&mut c, "INSERT INTO kart (id, kind, name) VALUES (7, 'NEW', 'n7')").await;
            assert_eq!(ids(&mut c, "SELECT id FROM pedal ORDER BY id").await, vec![6]);
            assert_eq!(
                ids(&mut c, "SELECT id FROM ONLY kart ORDER BY id").await,
                vec![1, 7]
            );
            fails(
                &mut c,
                "INSERT INTO racer (id, kind, name) VALUES (8, 'GO', 'x')",
                39717,
            )
            .await;
            // And back to `_type`: every row keeps its type.
            let t0 = Instant::now();
            exec(&mut c, "ALTER TABLE kart DROP DISCRIMINATOR").await;
            let drop_ms = t0.elapsed().as_millis();
            let types = rows(&mut c, "SELECT id, _type FROM kart ORDER BY id").await;
            let want: Vec<Vec<String>> = [
                (1, "kart"),
                (2, "gokart"),
                (3, "gokart"),
                (4, "racer"),
                (5, "gokart"),
                (6, "pedal"),
                (7, "kart"),
            ]
            .iter()
            .map(|(i, t)| vec![i.to_string(), (*t).to_owned()])
            .collect();
            assert_eq!(types, want);
            let sc = rows(&mut c, "SHOW CREATE TABLE racer").await;
            assert!(!sc[0][1].contains("WHERE"), "{}", sc[0][1]);
            // kind is an ordinary column again.
            exec(&mut c, "INSERT INTO kart (id, kind, name) VALUES (9, 'RACE', 'k9')").await;
            assert_eq!(
                ids(&mut c, "SELECT id FROM ONLY kart ORDER BY id").await,
                vec![1, 7, 9]
            );
            assert_eq!(ids(&mut c, "SELECT id FROM racer ORDER BY id").await, vec![4]);
            eprintln!(
                "conversion[{} {}]: ADD DISCRIMINATOR {add_ms} ms, DROP DISCRIMINATOR {drop_ms} ms",
                persona(pi),
                if joined { "joined" } else { "joined, explicit" }
            );
        }
        // Single-table storage keeps its discriminator.
        fails(&mut c, "ALTER TABLE fleet DROP DISCRIMINATOR", 39710).await;
        exec(&mut c, &format!("DROP DATABASE {db}")).await;
    }
}
