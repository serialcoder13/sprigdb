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
