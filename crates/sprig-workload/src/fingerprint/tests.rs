//! Unit tests of the normalizer and digest.

use super::*;

fn norm(sql: &str) -> String {
    normalize(sql.as_bytes(), &LexOptions::mysql84(), Some("shop"), false).text
}

#[test]
fn chapter_example() {
    let sql = "SELECT /* web:42 */ o.id, o.Total FROM `orders` o WHERE o.customer_id = 42\n  AND o.status IN ('open','hold') ORDER BY o.created_at DESC LIMIT 20";
    assert_eq!(
        norm(sql),
        "SELECT o.id, o.total FROM shop.orders o WHERE o.customer_id = ? AND o.status IN (...) ORDER BY o.created_at DESC LIMIT ?"
    );
}

#[test]
fn scratch_print() {
    let cases = [
        "SELECT c FROM sbtest1 WHERE id=10",
        "SELECT c FROM sbtest1 WHERE id BETWEEN 1 AND 100",
        "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 1 AND 100",
        "SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN 1 AND 100 ORDER BY c",
        "UPDATE sbtest1 SET k=k+1 WHERE id=5",
        "UPDATE sbtest1 SET c='abc' WHERE id=-5",
        "DELETE FROM sbtest1 WHERE id=7",
        "INSERT INTO sbtest1 (id, k, c, pad) VALUES (1, 2, 'x', 'y'), (3,4,'z','w')",
        "INSERT INTO t VALUES (1),(2) ON DUPLICATE KEY UPDATE a = VALUES(a), b = b + 1",
        "SELECT * FROM a JOIN b ON a.id = b.a_id LEFT JOIN c.d AS x ON x.k = b.k WHERE a.v IN (1, -2, 3) AND b.t > NOW() - INTERVAL 1 DAY",
        "SELECT a, b FROM t1, t2 t WHERE (a, b) IN ((1,2),(3,4)) LIMIT 10 OFFSET 5",
        "select count(*) from `Users` where `email` = 'x' and created_at > DATE '2020-01-01' and flag = TRUE",
        "/*!40101 SET NAMES utf8mb4 */",
        "SELECT /*+ BKA(t1) MAX_EXECUTION_TIME(1000) */ * FROM t1",
        "SELECT /*M!100100 1, */ 2",
        "WITH x AS (SELECT id FROM orders WHERE total > 10), y AS (SELECT 1) SELECT * FROM x JOIN customers c ON c.id = x.id",
        "SELECT EXTRACT(YEAR FROM created_at) FROM orders",
        "ALTER USER 'bob'@'%' IDENTIFIED BY 'secret'",
        "SELECT 'a' 'b', _utf8mb4'x', X'0A', 0x1F, b'01', 1e5, .5, -1.5e-3 FROM dual",
        "SELECT a-1, a - -1, (1) - 2, f(-3), x * -4",
        "INSERT INTO t SET a = 1, b = 'x'",
        "UPDATE t1 JOIN t2 ON t1.id = t2.id SET t1.a = 1 WHERE t2.b = 2",
        "DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.id WHERE t2.b = 2",
        "SELECT * FROM t WHERE a = ? AND b IN (?, ?, ?)",
        "SELECT * FROM t FOR UPDATE",
        "SELECT IF(a, 1, 2), LEFT(b, 3), REPLACE(c, 'a', 'b') FROM t",
        "REPLACE INTO t (a) VALUES (1)",
        "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.a = t1.a)",
        "SHOW TABLES FROM shop LIKE 'x%'",
        "SELECT * FROM t WHERE x IN (SELECT y FROM u)",
        "INSERT INTO t (a) SELECT b FROM u WHERE c = 1",
        "SELECT `select`.`from` FROM `select`",
        "SELECT * FROM `my db`.`my table` WHERE `col 1` = 1",
    ];
    for c in cases {
        let n = normalize(c.as_bytes(), &LexOptions::mariadb(), Some("shop"), false);
        println!(
            "{c}\n  => {}  [slots {} lits {:?} tables {:?} type {:?} sens {}]\n",
            n.text,
            n.slots,
            n.literals.iter().map(|l| l.slot).collect::<Vec<_>>(),
            n.tables
                .iter()
                .map(|t| format!("{:?}.{}/{:?}", t.db, t.name, t.alias))
                .collect::<Vec<_>>(),
            n.stmt_type,
            n.sensitive
        );
    }
}

/// bugfix-24 (ST48): statements carrying passwords or a connection's
/// credentials keep no literals, so none is ever sampled.
#[test]
fn credentials_and_passwords_are_sensitive() {
    for (sql, sensitive) in [
        ("CREATE USER 'u'@'%' IDENTIFIED BY 'pw'", true),
        ("SET PASSWORD FOR u = 'pw'", true),
        (
            "CREATE CONNECTION c TYPE webhook WITH (url = 'http://h') CREDENTIALS (token = 'tok')",
            true,
        ),
        ("ALTER CONNECTION c SET CREDENTIALS (token = 'tok')", true),
        ("SELECT * FROM t WHERE credentials_id = 'x'", false),
        ("SELECT * FROM t WHERE a = 'x'", false),
    ] {
        let n = normalize(sql.as_bytes(), &LexOptions::mariadb(), Some("shop"), false);
        assert_eq!(n.sensitive, sensitive, "{sql}");
        if sensitive {
            assert!(n.literals.is_empty(), "{sql}: {:?}", n.literals);
        }
    }
}

/// A resolver that knows databases `shop` (1) and `retail` (2), any table.
struct Dbs;

impl crate::shape::TableResolver for Dbs {
    fn resolve(&self, db: Option<&str>, _table: &str) -> Option<crate::shape::ResolvedTable> {
        let db_id = match db? {
            "shop" => DbId(1),
            "retail" => DbId(2),
            _ => return None,
        };
        Some(crate::shape::ResolvedTable {
            object_id: sprig_common::ids::ObjectId(u64::from(db_id.0) * 100),
            db_id,
            physical: None,
        })
    }
}

/// Studio showed `SELECT ... FROM retail.orders WHERE customer_id = ?`
/// twice, once with database "—" and once with `retail`: the id took the
/// session's default database. Now a statement that names its tables is
/// keyed on the first table's database, whatever the session's is.
#[test]
#[allow(clippy::many_single_char_names)]
fn a_qualified_statement_is_one_fingerprint_whatever_the_default_database() {
    use sprig_common::ids::TenantId;
    let f = SqlFingerprinter::new(1024);
    let t = TenantId(7);
    let base = SqlCtx::new(Persona::MySql84);
    let none = base;
    let retail = base.current_db("retail", DbId(2));
    let shop = base.current_db("shop", DbId(1));
    let q = b"SELECT id FROM retail.orders WHERE customer_id = 42";
    let a = f.sql(t, q, &none, &Dbs);
    let b = f.sql(t, q, &retail, &Dbs);
    let c = f.sql(t, q, &shop, &Dbs);
    // The unqualified form in a `retail` session is the same statement.
    let d = f.sql(
        t,
        b"SELECT id FROM orders WHERE customer_id = 7",
        &retail,
        &Dbs,
    );
    assert_eq!(a.id, b.id);
    assert_eq!(a.id, c.id);
    assert_eq!(a.id, d.id);
    assert_eq!((a.db_id, c.db_id, d.db_id), (DbId(2), DbId(2), DbId(2)));
    // Unchanged for the common case: the id a `retail` session had before.
    assert_eq!(b.id, digest(FpKind::Sql, DbId(2), None, &b.text));
    // CTE names aside: the first base table decides.
    let w = b"WITH x AS (SELECT id FROM retail.orders) SELECT * FROM x";
    assert_eq!(f.sql(t, w, &shop, &Dbs).db_id, DbId(2));
    // A database the resolver doesn't know keys on 0, from any session.
    let u = b"SELECT 1 FROM elsewhere.t";
    assert_eq!(f.sql(t, u, &shop, &Dbs).id, f.sql(t, u, &none, &Dbs).id);
    assert_eq!(f.sql(t, u, &shop, &Dbs).db_id, DbId(0));
    // A statement that names no table keeps the session's database: its
    // meaning can depend on it (`SELECT f()` calls the session's `f`).
    let s = b"SELECT f(1)";
    let (s1, s2) = (f.sql(t, s, &shop, &Dbs), f.sql(t, s, &retail, &Dbs));
    assert_ne!(s1.id, s2.id);
    assert_eq!((s1.db_id, s2.db_id), (DbId(1), DbId(2)));
    // lower_case_table_names: `RETAIL.orders` in a `retail` session.
    let lc = SqlCtx {
        lower_case_table_names: true,
        ..retail
    };
    let e = f.sql(
        t,
        b"SELECT id FROM RETAIL.orders WHERE customer_id = 1",
        &lc,
        &Dbs,
    );
    assert_eq!(e.db_id, DbId(2));
}
