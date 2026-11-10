#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::unnecessary_wraps,
    clippy::unnecessary_literal_bound
)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::*;
use crate::exec::{ShardOk, ShardRows};
use crate::plan::ShardAt;

fn v(s: &str) -> Option<Bytes> {
    Some(Bytes::copy_from_slice(s.as_bytes()))
}

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        charset: 45,
        col_type: 253,
        ..ColumnDef::default()
    }
}

fn rows(cols: &[&str], rows: Vec<Row>) -> ShardOutcome {
    ShardOutcome::Rows(ShardRows {
        columns: cols.iter().map(|c| col(c)).collect(),
        rows,
        warnings: 0,
    })
}

const SHOW: &str = "CREATE TABLE `orders` (\n  `oid` int(11) NOT NULL,\n  `cid` int(11) NOT NULL,\n  `note` varchar(20) DEFAULT NULL,\n  `ts` timestamp(3) NULL DEFAULT NULL,\n  `f` float DEFAULT NULL,\n  `g` int(11) GENERATED ALWAYS AS (`oid` * 2) VIRTUAL,\n  PRIMARY KEY (`oid`),\n  KEY `cid` (`cid`),\n  FULLTEXT KEY `ft` (`note`),\n  CONSTRAINT `fk` FOREIGN KEY (`cid`) REFERENCES `customers` (`cid`),\n  CONSTRAINT `chk` CHECK (`oid` > 0)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci\n PARTITION BY HASH (`oid`)\nPARTITIONS 2";

#[test]
fn temporary_ddl_drops_what_a_temporary_table_cant_hold() {
    let d = temp_ddl(SHOW, "t1_d1", "orders").unwrap();
    assert!(
        d.starts_with("CREATE TEMPORARY TABLE `t1_d1`.`orders` (\n"),
        "{d}"
    );
    assert!(d.contains("KEY `cid` (`cid`)\n) ENGINE=InnoDB"), "{d}");
    for gone in ["FOREIGN KEY", "CHECK", "FULLTEXT", "PARTITION"] {
        assert!(!d.contains(gone), "{gone} in {d}");
    }
    assert!(d.contains("GENERATED ALWAYS AS"), "{d}");
    assert!(temp_ddl("CREATE VIEW x AS SELECT 1", "s", "x").is_none());
    let cols = columns_of_ddl(SHOW);
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["oid", "cid", "note", "ts", "f"]);
    assert_eq!(cols[3].copy, Copy::Timestamp);
    assert_eq!(cols[4].copy, Copy::Float);
    let odd = "CREATE TABLE `t` (\n  `a``b` varchar(3) DEFAULT NULL COMMENT 'x GENERATED ALWAYS AS y',\n  `é` int(11) DEFAULT NULL\n) ENGINE=InnoDB";
    let names: Vec<String> = columns_of_ddl(odd).into_iter().map(|c| c.name).collect();
    assert_eq!(names, ["a`b", "é"]);
    let versioned =
        "CREATE TABLE `t` (\n  `a` int(11) DEFAULT NULL\n) ENGINE=InnoDB WITH SYSTEM VERSIONING";
    assert!(temp_ddl(versioned, "s", "t").is_none());
}

#[test]
fn copy_mode_keeps_the_sessions_reading_rules() {
    assert_eq!(
        copy_mode("STRICT_TRANS_TABLES,NO_ZERO_DATE,PIPES_AS_CONCAT").unwrap(),
        "STRICT_TRANS_TABLES,PIPES_AS_CONCAT,NO_AUTO_VALUE_ON_ZERO"
    );
    assert_eq!(copy_mode("").unwrap(), "NO_AUTO_VALUE_ON_ZERO");
    assert!(copy_mode("A'; DROP").is_none());
}

#[test]
fn literals_keep_each_value() {
    assert_eq!(literal(Copy::Number, Some(b"-12.50")).unwrap(), "-12.50");
    assert_eq!(literal(Copy::Number, None).unwrap(), "NULL");
    assert!(literal(Copy::Number, Some(b"1; DROP")).is_err());
    assert_eq!(
        literal(Copy::Float, Some(b"0.10000000149011612")).unwrap(),
        "0.10000000149011612"
    );
    assert_eq!(
        literal(Copy::Timestamp, Some(b"1774747800.25")).unwrap(),
        "FROM_UNIXTIME(1774747800.25)"
    );
    assert_eq!(literal(Copy::Timestamp, Some(b"0.000")).unwrap(), "0");
    assert_eq!(
        literal(Copy::Temporal, Some(b"-12:00:01.5")).unwrap(),
        "'-12:00:01.5'"
    );
    assert!(literal(Copy::Temporal, Some(b"1' OR '1")).is_err());
    assert_eq!(literal(Copy::Bytes, Some(b"a'b")).unwrap(), "X'612762'");
    assert_eq!(Copy::of("int(11) unsigned"), Copy::Number);
    assert_eq!(Copy::of("decimal(10,2)"), Copy::Number);
    assert_eq!(Copy::of("float"), Copy::Float);
    assert_eq!(Copy::of("timestamp(3)"), Copy::Timestamp);
    assert_eq!(Copy::of("datetime(6)"), Copy::Temporal);
    assert_eq!(Copy::of("varchar(20)"), Copy::Bytes);
    assert_eq!(Copy::of("enum('a','b')"), Copy::Bytes);
    assert_eq!(Copy::of("bit(5)"), Copy::Bytes);
}

/// The scratch connection: answers by statement prefix, logs every
/// statement.
struct FakeScratch {
    log: Arc<Mutex<Vec<String>>>,
    rows: Vec<Row>,
    at: usize,
}

#[async_trait::async_trait]
impl Scratch for FakeScratch {
    fn cluster(&self) -> &str {
        "c1"
    }
    async fn run(&mut self, sql: &str, _budget: &mut Budget) -> Result<ShardOutcome> {
        self.log.lock().unwrap().push(sql.to_owned());
        if sql.starts_with("SELECT @@binlog_format") {
            return Ok(rows(
                &["b", "m"],
                vec![vec![v("ROW"), v("STRICT_TRANS_TABLES,NO_ZERO_DATE")]],
            ));
        }
        if sql.starts_with("SHOW FULL COLUMNS") {
            let r = |n: &str, t: &str, e: &str| {
                vec![v(n), v(t), None, v("YES"), v(""), None, v(e), v(""), v("")]
            };
            return Ok(rows(
                &[
                    "Field",
                    "Type",
                    "Collation",
                    "Null",
                    "Key",
                    "Default",
                    "Extra",
                ],
                vec![
                    r("oid", "int(11)", ""),
                    r("cid", "int(11)", ""),
                    r("note", "varchar(20)", ""),
                    r("ts", "timestamp(3)", ""),
                    r("f", "float", ""),
                    r("g", "int(11)", "VIRTUAL GENERATED"),
                ],
            ));
        }
        if sql.contains("INSERT INTO") && sql.contains(" SELECT ") {
            return Ok(ShardOutcome::Ok(ShardOk {
                affected: 3,
                ..ShardOk::default()
            }));
        }
        Ok(ShardOutcome::Ok(ShardOk::default()))
    }
    async fn start(&mut self, sql: &str) -> Result<Option<Vec<ColumnDef>>> {
        self.log.lock().unwrap().push(sql.to_owned());
        Ok(Some(vec![col("n")]))
    }
    async fn next_row(&mut self) -> Result<Option<Row>> {
        self.at += 1;
        Ok(self.rows.get(self.at - 1).cloned())
    }
    fn warnings(&self) -> u16 {
        0
    }
    async fn finish(self: Box<Self>, clean: bool) {
        self.log
            .lock()
            .unwrap()
            .push(format!("finish clean={clean}"));
    }
}

struct Fake {
    log: Arc<Mutex<Vec<String>>>,
    answers: HashMap<String, ShardOutcome>,
    in_txn: bool,
    max_rows: u64,
}

#[async_trait::async_trait]
impl ShardBackend for Fake {
    async fn run(&mut self, t: &Target, sql: &str, _b: &mut Budget) -> Result<ShardOutcome> {
        self.log
            .lock()
            .unwrap()
            .push(format!("[{}] {sql}", t.cluster));
        if sql.starts_with("SHOW CREATE TABLE") {
            return Ok(rows(
                &["Table", "Create Table"],
                vec![vec![v("orders"), v(SHOW)]],
            ));
        }
        self.answers
            .get(sql)
            .cloned()
            .ok_or_else(|| SprigError::internal(format!("no answer for {sql}")))
    }
    fn in_transaction(&self) -> bool {
        self.in_txn
    }
    async fn open_scratch(&mut self) -> Result<Option<Box<dyn Scratch>>> {
        Ok(Some(Box::new(FakeScratch {
            log: Arc::clone(&self.log),
            rows: vec![vec![v("7")]],
            at: 0,
        })))
    }
    fn gather_limits(&self) -> GatherLimits {
        GatherLimits {
            max_rows: self.max_rows,
            max_concurrent: 2,
            ..GatherLimits::default()
        }
    }
}

fn src(cluster: &str, shard: u32) -> Target {
    Target {
        cluster: cluster.into(),
        shard,
        sql: String::new(),
        at: ShardAt {
            group: 9,
            db: 1,
            lo: 0,
            schema: format!("t1_d1_g9_s{shard}"),
        },
    }
}

fn plan() -> GatherPlan {
    GatherPlan {
        tables: vec![GatherTable {
            schema: "t1_d1".into(),
            name: "orders".into(),
            sources: vec![src("c1", 1), src("c2", 2)],
            alias: "o".into(),
            filter: Some("(o.cid > 5)".into()),
            shards_total: 4,
        }],
        sql: "SELECT COUNT(*) OVER () FROM orders o WHERE o.cid > 5".into(),
        reads_unsharded: false,
        fallback: Rejection::new(Reason::WindowFunction, "a window function across shards"),
    }
}

const REMOTE: &str = "SELECT `oid`, `cid`, CAST(`note` AS BINARY), UNIX_TIMESTAMP(`ts`), CAST(`f` AS DOUBLE) FROM `t1_d1_g9_s2`.`orders` AS `o` WHERE (o.cid > 5)";

fn fake(max_rows: u64) -> Fake {
    let mut answers = HashMap::new();
    answers.insert(
        REMOTE.to_owned(),
        rows(
            &["oid", "cid", "note", "ts", "f"],
            vec![
                vec![v("1"), v("6"), v("it's"), v("1774747800.250"), v("0.5")],
                vec![v("2"), v("9"), None, None, None],
            ],
        ),
    );
    Fake {
        log: Arc::new(Mutex::new(Vec::new())),
        answers,
        in_txn: false,
        max_rows,
    }
}

#[tokio::test]
async fn copies_then_runs_the_statement_over_them() {
    let mut be = fake(1000);
    let mut budget = Budget::mb(16);
    let out = execute(&plan(), &mut be, &mut budget, None).await.unwrap();
    let Output::Rows { rows, .. } = out else {
        panic!("rows expected")
    };
    assert_eq!(rows, vec![vec![v("7")]]);
    let log = be.log.lock().unwrap().clone();
    let pos = |needle: &str| {
        log.iter()
            .position(|l| l.contains(needle))
            .unwrap_or_else(|| panic!("{needle} not in {log:#?}"))
    };
    // The temporary table first, from the first shard's definition.
    let create = pos("FOR CREATE TEMPORARY TABLE `t1_d1`.`orders`");
    assert!(
        log[create]
            .starts_with("SET STATEMENT sql_mode='STRICT_TRANS_TABLES,NO_AUTO_VALUE_ON_ZERO' FOR")
    );
    // The shard on the same cluster copies inside MariaDB, without the
    // generated column.
    let local = pos(
        "INSERT INTO `t1_d1`.`orders` (`oid`, `cid`, `note`, `ts`, `f`) SELECT `oid`, `cid`, `note`, `ts`, `f` FROM `t1_d1_g9_s1`.`orders` AS `o` WHERE (o.cid > 5)",
    );
    // The other cluster's rows come back as literals.
    let remote = pos("[c2] SELECT `oid`");
    let values = pos("FROM_UNIXTIME(1774747800.250)");
    assert!(
        log[values].contains(
            "VALUES (1,6,X'69742773',FROM_UNIXTIME(1774747800.250),0.5),(2,9,NULL,NULL,NULL)"
        ),
        "{}",
        log[values]
    );
    assert!(log[values].contains("time_zone='+00:00'"));
    let stmt = pos("SELECT COUNT(*) OVER ()");
    let drop = pos("DROP TEMPORARY TABLE IF EXISTS `t1_d1`.`orders`");
    let finish = pos("finish clean=true");
    assert!(create < local && local < remote && remote < values);
    assert!(values < stmt && stmt < drop && drop < finish);
}

#[tokio::test]
async fn off_or_over_the_row_limit() {
    let mut be = fake(0);
    let mut budget = Budget::mb(16);
    let e = execute(&plan(), &mut be, &mut budget, None)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::StatementNotDistributable));
    assert!(e.message.contains("window_function"), "{}", e.message);
    // 3 rows copied inside MariaDB, 2 more over the limit of 4.
    let mut be = fake(4);
    let e = execute(&plan(), &mut be, &mut budget, None)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::PlanLimitExceeded));
    let log = be.log.lock().unwrap().clone();
    assert!(log.iter().any(|l| l.starts_with("DROP TEMPORARY TABLE")));
    assert!(log.iter().any(|l| l == "finish clean=false"), "{log:#?}");
}

#[tokio::test]
async fn inside_a_transaction_every_shard_is_read_through_it() {
    let mut be = fake(1000);
    be.in_txn = true;
    let local = REMOTE.replace("t1_d1_g9_s2", "t1_d1_g9_s1");
    let answer = be.answers[REMOTE].clone();
    be.answers.insert(local, answer);
    let mut budget = Budget::mb(16);
    execute(&plan(), &mut be, &mut budget, None).await.unwrap();
    let log = be.log.lock().unwrap().clone();
    assert!(
        log.iter().any(|l| l.starts_with("[c1] SELECT `oid`")),
        "{log:#?}"
    );
    assert!(
        !log.iter()
            .any(|l| l.contains(" SELECT `oid`, `cid`, `note`"))
    );
    // Unsharded tables can't be read outside the transaction.
    let mut p = plan();
    p.reads_unsharded = true;
    let e = execute(&p, &mut be, &mut budget, None).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::StatementNotDistributable));
}
