//! Chapter 28 steps 3 and 4 against a real MariaDB: the persona-shaped
//! `information_schema` providers and `SHOW` rewrites run on MariaDB 11.8,
//! and what a 3306 client sees matches MySQL 8.4 (values captured from
//! MySQL 8.4.11 on 8 November 2026). Skipped unless the Compose single
//! node is named:
//!
//! ```sh
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:sprig-dev-root@127.0.0.1:13301
//! cargo test -p sprig-sql --test compat_live -- --nocapture
//! ```
//!
//! The test creates the schema `t9601_d1_g1_s1` (tenant 9601's `shop`) and
//! drops it at the end, also on failure.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use sprig_common::ids::TenantId;
use sprig_proto_mysql::client::{ClientConfig, MysqlClient, TlsMode, TlsOptions};
use sprig_proto_mysql::packets::Persona;
use sprig_sql::names::{
    Action, ColumnRegistry, MemoryNameMap, OutputCtx, PlanCtx, RewriteCtx, TextRow, map_message,
    physical_name_leaks, plan,
};

const ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";
const SCHEMA: &str = "t9601_d1_g1_s1";

fn tenant() -> MemoryNameMap {
    MemoryNameMap::new(TenantId(9601)).with_db(1, "shop", 1, 1)
}

async fn connect(url: &str) -> MysqlClient {
    let rest = url.strip_prefix("mysql://").expect("mysql:// URL");
    let (cred, addr) = rest.rsplit_once('@').expect("user@host");
    let (user, pass) = cred.split_once(':').unwrap_or((cred, ""));
    let mut cfg = ClientConfig::new(user, pass);
    cfg.tls = TlsMode::Preferred;
    cfg.tls_config = Some(
        TlsOptions {
            skip_verify: true,
            ..TlsOptions::default()
        }
        .build()
        .unwrap(),
    );
    MysqlClient::connect(addr.trim_end_matches('/'), &cfg)
        .await
        .unwrap()
}

const SETUP: &[&str] = &[
    "CREATE TABLE j (id int PRIMARY KEY AUTO_INCREMENT, j json, k json NOT NULL, \
     s varchar(10) DEFAULT 'a''b', n int DEFAULT NULL, \
     d datetime DEFAULT current_timestamp ON UPDATE current_timestamp, \
     d2 datetime(3) DEFAULT current_timestamp(3), e enum('x','y') DEFAULT 'x', \
     b bit(1) DEFAULT b'1', t text, dc decimal(10,2) DEFAULT 1.50, \
     g int GENERATED ALWAYS AS (n + 1) VIRTUAL, f tinyint(1), \
     c varchar(5) COLLATE utf8mb4_uca1400_ai_ci, KEY ix (s) IGNORED) \
     DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
    "CREATE TABLE c (id int PRIMARY KEY, p int, CONSTRAINT fk FOREIGN KEY (p) REFERENCES j (id), \
     CONSTRAINT c_chk_1 CHECK (p > 0))",
];

type View = (Vec<String>, Vec<Vec<Option<String>>>);

/// What a client of `persona` sees for `sql` in a session of tenant 9601
/// in `shop`: column names and rows, after plan, MariaDB and the mapper.
async fn client_view(c: &mut MysqlClient, persona: Persona, sql: &str) -> Result<View, String> {
    let map = tenant();
    let registry = ColumnRegistry::mariadb_11_8();
    let rctx = RewriteCtx::new(&map, persona).current_db(Some("shop"));
    let pctx = PlanCtx::new(rctx, &registry);
    let p = plan(sql.as_bytes(), &pctx).map_err(|e| format!("ERR {} {e}", e.errno()))?;
    let s = p.single().expect("one statement");
    let octx = OutputCtx::new(&map);
    let Action::Forward { sql: fwd, mapper } = &s.action else {
        return Err("not forwarded".into());
    };
    let fwd = String::from_utf8(fwd.clone()).unwrap();
    let rs = match c.query(&fwd).await {
        Ok(rs) => rs,
        Err(e) => {
            let e = e.server_error().expect("server error").clone();
            return Err(format!("ERR {} {}", e.code, map_message(&e.message, &octx)));
        }
    };
    let rs = rs
        .into_iter()
        .find(|r| !r.columns.is_empty())
        .unwrap_or_default();
    let mut defs = rs.columns.clone();
    let mut m = mapper.clone();
    let rows: Vec<TextRow> = m.map_all(&mut defs, rs.text_rows().unwrap(), &octx);
    for r in &rows {
        for v in r.iter().flatten() {
            assert!(physical_name_leaks(v).is_empty(), "{sql}: leak in {v:?}");
        }
    }
    Ok((
        defs.iter()
            .map(|c| String::from_utf8_lossy(&c.name).into_owned())
            .collect(),
        rows.iter()
            .map(|r| {
                r.iter()
                    .map(|v| v.as_ref().map(|b| String::from_utf8_lossy(b).into_owned()))
                    .collect()
            })
            .collect(),
    ))
}

fn col(v: &View, name: &str) -> Vec<Option<String>> {
    let i =
        v.0.iter()
            .position(|n| n.eq_ignore_ascii_case(name))
            .unwrap_or_else(|| panic!("no column {name} in {:?}", v.0));
    v.1.iter().map(|r| r[i].clone()).collect()
}

fn s(v: &[Option<String>]) -> Vec<String> {
    v.iter()
        .map(|x| x.clone().unwrap_or_else(|| "NULL".into()))
        .collect()
}

async fn checks(c: &mut MysqlClient) {
    let my = Persona::MySql84;
    let view = async |c: &mut MysqlClient, p: Persona, sql: &str| {
        client_view(c, p, sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
    };

    // COLUMNS as MySQL 8.4.11 answered for the same table.
    let v = view(
        c,
        my,
        "SELECT COLUMN_NAME, COLUMN_DEFAULT, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, \
         CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE, EXTRA, GENERATION_EXPRESSION \
         FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'shop' AND TABLE_NAME = 'j' \
         ORDER BY ORDINAL_POSITION",
    )
    .await;
    let rows: Vec<String> = v.1.iter().map(|r| s(r).join("|")).collect();
    assert_eq!(
        rows,
        [
            "id|NULL|int|NULL|NULL|NULL|int|auto_increment|",
            "j|NULL|json|NULL|NULL|NULL|json||",
            "k|NULL|json|NULL|NULL|NULL|json||",
            "s|a'b|varchar|10|utf8mb4|utf8mb4_0900_ai_ci|varchar(10)||",
            "n|NULL|int|NULL|NULL|NULL|int||",
            "d|CURRENT_TIMESTAMP|datetime|NULL|NULL|NULL|datetime|DEFAULT_GENERATED on update CURRENT_TIMESTAMP|",
            "d2|CURRENT_TIMESTAMP(3)|datetime|NULL|NULL|NULL|datetime(3)|DEFAULT_GENERATED|",
            "e|x|enum|1|utf8mb4|utf8mb4_0900_ai_ci|enum('x','y')||",
            "b|b'1'|bit|NULL|NULL|NULL|bit(1)||",
            "t|NULL|text|65535|utf8mb4|utf8mb4_0900_ai_ci|text||",
            "dc|1.50|decimal|NULL|NULL|NULL|decimal(10,2)||",
            "g|NULL|int|NULL|NULL|NULL|int|VIRTUAL GENERATED|`n` + 1",
            "f|NULL|tinyint|NULL|NULL|NULL|tinyint(1)||",
            "c|NULL|varchar|5|utf8mb4|utf8mb4_0900_ai_ci|varchar(5)||",
        ]
    );
    // SELECT * has MySQL's columns.
    let v = view(
        c,
        my,
        "SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'shop' LIMIT 1",
    )
    .await;
    assert_eq!(
        v.0,
        sprig_sql::compat::schema::mysql_column_names("COLUMNS").unwrap()
    );
    // The MariaDB persona keeps MariaDB's answer.
    let v = view(
        c,
        Persona::MariaDb,
        "SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'shop' \
         AND TABLE_NAME = 'j' AND COLUMN_NAME = 'j'",
    )
    .await;
    assert_eq!(s(&col(&v, "DATA_TYPE")), ["longtext"]);

    // Checks, constraints and indexes.
    let v = view(
        c,
        my,
        "SELECT CONSTRAINT_NAME, CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS \
         WHERE CONSTRAINT_SCHEMA = 'shop'",
    )
    .await;
    assert_eq!(v.1.len(), 1, "{:?}", v.1);
    assert_eq!(s(&v.1[0]), ["c_chk_1", "(`p` > 0)"]);
    let v = view(
        c,
        my,
        "SELECT TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, ENFORCED FROM \
         information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = 'shop' \
         ORDER BY TABLE_NAME, CONSTRAINT_NAME",
    )
    .await;
    let rows: Vec<String> = v.1.iter().map(|r| s(r).join("|")).collect();
    assert_eq!(
        rows,
        [
            "c|c_chk_1|CHECK|YES",
            "c|fk|FOREIGN KEY|YES",
            "c|PRIMARY|PRIMARY KEY|YES",
            "j|PRIMARY|PRIMARY KEY|YES",
        ]
    );
    let v = view(
        c,
        my,
        "SELECT UPDATE_RULE, DELETE_RULE FROM information_schema.REFERENTIAL_CONSTRAINTS \
         WHERE CONSTRAINT_SCHEMA = 'shop'",
    )
    .await;
    assert_eq!(s(&v.1[0]), ["NO ACTION", "NO ACTION"]);
    let v = view(
        c,
        my,
        "SELECT INDEX_NAME, IS_VISIBLE, EXPRESSION FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = 'shop' AND TABLE_NAME = 'j' ORDER BY INDEX_NAME",
    )
    .await;
    let rows: Vec<String> = v.1.iter().map(|r| s(r).join("|")).collect();
    assert_eq!(rows, ["ix|NO|NULL", "PRIMARY|YES|NULL"]);
    let v = view(
        c,
        my,
        "SELECT TABLE_NAME, TABLE_COLLATION FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = 'shop' ORDER BY TABLE_NAME",
    )
    .await;
    assert_eq!(v.0.len(), 2);
    assert_eq!(s(&col(&v, "TABLE_COLLATION"))[1], "utf8mb4_0900_ai_ci");
    let v = view(c, my, "SELECT * FROM information_schema.SCHEMATA").await;
    assert_eq!(v.0.last().map(String::as_str), Some("DEFAULT_ENCRYPTION"));

    // Keywords: MySQL 8.4's list on 3306.
    let v = view(
        c,
        my,
        "SELECT WORD FROM INFORMATION_SCHEMA.KEYWORDS WHERE RESERVED=1 ORDER BY WORD",
    )
    .await;
    assert_eq!(v.1.len(), 262);

    // mysqldump 8.4's query, collations and character sets.
    let v = view(
        c,
        my,
        "SELECT COLUMN_NAME, JSON_EXTRACT(HISTOGRAM, '$.\"number-of-buckets-specified\"') \
         FROM information_schema.COLUMN_STATISTICS WHERE SCHEMA_NAME = 'shop' AND TABLE_NAME = 'j'",
    )
    .await;
    assert!(v.1.is_empty());
    let v = view(
        c,
        my,
        "SELECT ID, IS_DEFAULT, PAD_ATTRIBUTE FROM information_schema.COLLATIONS \
         WHERE COLLATION_NAME = 'utf8mb4_0900_ai_ci'",
    )
    .await;
    assert_eq!(s(&v.1[0]), ["255", "Yes", "NO PAD"]);
    let v = view(
        c,
        my,
        "SELECT DEFAULT_COLLATE_NAME FROM information_schema.CHARACTER_SETS \
         WHERE CHARACTER_SET_NAME = 'utf8mb4'",
    )
    .await;
    assert_eq!(s(&v.1[0]), ["utf8mb4_0900_ai_ci"]);

    // SHOW COLUMNS and DESCRIBE as MySQL 8.4.11 answered.
    let v = view(c, my, "SHOW FULL COLUMNS FROM j").await;
    assert_eq!(
        v.0,
        [
            "Field",
            "Type",
            "Collation",
            "Null",
            "Key",
            "Default",
            "Extra",
            "Privileges",
            "Comment"
        ]
    );
    let rows: Vec<String> = v.1.iter().map(|r| s(&r[..7]).join("|")).take(6).collect();
    assert_eq!(
        rows,
        [
            "id|int|NULL|NO|PRI|NULL|auto_increment",
            "j|json|NULL|YES||NULL|",
            "k|json|NULL|NO||NULL|",
            "s|varchar(10)|utf8mb4_0900_ai_ci|YES|MUL|a'b|",
            "n|int|NULL|YES||NULL|",
            "d|datetime|NULL|YES||CURRENT_TIMESTAMP|DEFAULT_GENERATED on update CURRENT_TIMESTAMP",
        ]
    );
    let v = view(c, my, "SHOW COLUMNS FROM shop.j WHERE `Type` = 'json'").await;
    assert_eq!(s(&col(&v, "Field")), ["j", "k"]);
    let v = view(c, my, "DESCRIBE j 'd%'").await;
    assert_eq!(s(&col(&v, "Field")), ["d", "d2", "dc"]);
    let e = client_view(c, my, "DESCRIBE nope").await.unwrap_err();
    assert_eq!(e, "ERR 1146 Table 'shop.nope' doesn't exist");
    let e = client_view(c, my, "SHOW COLUMNS FROM nope")
        .await
        .unwrap_err();
    assert_eq!(e, "ERR 1146 Table 'shop.nope' doesn't exist");

    // SHOW CREATE TABLE in MySQL's spelling.
    let v = view(c, my, "SHOW CREATE TABLE j").await;
    let text = col(&v, "Create Table")[0].clone().unwrap();
    assert!(text.contains("  `j` json DEFAULT NULL,\n"), "{text}");
    assert!(text.contains("  `k` json NOT NULL,\n"), "{text}");
    assert!(!text.contains("json_valid"), "{text}");
    assert!(
        text.contains("  `id` int NOT NULL AUTO_INCREMENT,"),
        "{text}"
    );
    assert!(
        text.contains("KEY `ix` (`s`) /*!80000 INVISIBLE */"),
        "{text}"
    );
    assert!(
        text.contains("DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
        "{text}"
    );
    // 3307: MariaDB's text.
    let v = view(c, Persona::MariaDb, "SHOW CREATE TABLE j").await;
    let text = col(&v, "Create Table")[0].clone().unwrap();
    assert!(text.contains("CHECK (json_valid(`j`))"), "{text}");

    // Every inline view opens only the tenant's schema.
    for sql in [
        "EXPLAIN SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'shop'",
        "EXPLAIN SELECT * FROM information_schema.TABLE_CONSTRAINTS",
    ] {
        let map = tenant();
        let registry = ColumnRegistry::mariadb_11_8();
        let rctx = RewriteCtx::new(&map, my).current_db(Some("shop"));
        let p = plan(sql.as_bytes(), &PlanCtx::new(rctx, &registry)).unwrap();
        let Action::Forward { sql: fwd, .. } = &p.single().unwrap().action else {
            panic!()
        };
        let rs = c.query(std::str::from_utf8(fwd).unwrap()).await.unwrap();
        let rs = rs.into_iter().find(|r| !r.columns.is_empty()).unwrap();
        let extra = rs
            .columns
            .iter()
            .position(|d| &d.name[..] == b"Extra")
            .unwrap();
        for r in rs.text_rows().unwrap() {
            let e = r[extra]
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default();
            if e.contains("Scanned") {
                assert!(
                    e.contains("Scanned 0 databases") || e.contains("Scanned 1 database"),
                    "{sql}: {e}"
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn compat_live() {
    let Ok(url) = std::env::var(ENV) else {
        eprintln!("skipped: {ENV} is not set");
        return;
    };
    let mut admin = connect(&url).await;
    admin
        .query_drop(&format!("DROP DATABASE IF EXISTS {SCHEMA}"))
        .await
        .unwrap();
    admin
        .query_drop(&format!("CREATE DATABASE {SCHEMA}"))
        .await
        .unwrap();
    let u = url.clone();
    let body = tokio::spawn(async move {
        let mut c = connect(&u).await;
        c.query_drop(&format!("USE {SCHEMA}")).await.unwrap();
        for s in SETUP {
            c.query_drop(s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
        }
        checks(&mut c).await;
        c.quit().await;
    })
    .await;
    admin
        .query_drop(&format!("DROP DATABASE IF EXISTS {SCHEMA}"))
        .await
        .unwrap();
    admin.quit().await;
    if let Err(e) = body {
        std::panic::resume_unwind(e.into_panic());
    }
}
