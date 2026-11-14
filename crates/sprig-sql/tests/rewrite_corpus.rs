//! Table-driven rewrite tests per statement kind and persona. Each case runs
//! through both engines, which must agree. `{B}` and `{C}` stand for the
//! physical schemas of `billing` and `crm`; `=` means "unchanged".

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

mod common;

use common::{B, C, PERSONAS, acme, both, ctx, mariadb, mysql};
use sprig_proto_mysql::packets::Persona;
use sprig_sql::lex::LexOptions;
use sprig_sql::names::{Engine, NameMap, StmtKind, SystemSchema, UseTarget, rewrite, unrewrite};

/// Which personas a case runs on.
#[derive(Clone, Copy)]
enum On {
    Both,
    MySql,
    MariaDb,
}

fn expand(expected: &str, sql: &str) -> String {
    if expected == "=" {
        sql.to_owned()
    } else {
        expected.replace("{B}", B).replace("{C}", C)
    }
}

fn run(cases: &[(On, &str, &str)]) {
    let map = acme();
    let mut failures = Vec::new();
    for &(on, sql, expected) in cases {
        for p in PERSONAS {
            let applies = match on {
                On::Both => true,
                On::MySql => p == Persona::MySql84,
                On::MariaDb => p == Persona::MariaDb,
            };
            if !applies {
                continue;
            }
            let got = both(&map, LexOptions::for_persona(p), sql);
            let want = expand(expected, sql);
            if got != want {
                failures.push(format!("[{p:?}] {sql}\n   want: {want}\n    got: {got}"));
            }
        }
    }
    assert!(failures.is_empty(), "\n{}\n", failures.join("\n"));
}

#[test]
fn queries() {
    use On::Both as X;
    run(&[
        (
            X,
            "SELECT * FROM crm.customers",
            "SELECT * FROM {C}.customers",
        ),
        (X, "SELECT * FROM customers", "="),
        (
            X,
            "SELECT c.id FROM crm.customers c",
            "SELECT c.id FROM {C}.customers c",
        ),
        (
            X,
            "SELECT crm.customers.id, crm.customers.* FROM crm.customers",
            "SELECT {C}.customers.id, {C}.customers.* FROM {C}.customers",
        ),
        (
            X,
            "SELECT t.* FROM crm.t AS t",
            "SELECT t.* FROM {C}.t AS t",
        ),
        (
            X,
            "SELECT a FROM billing.t1 JOIN crm.t2 USING (id) LEFT JOIN t3 ON LEFT(t3.a, 1) = crm.t2.b",
            "SELECT a FROM {B}.t1 JOIN {C}.t2 USING (id) LEFT JOIN t3 ON LEFT(t3.a, 1) = {C}.t2.b",
        ),
        (
            X,
            "SELECT * FROM t WHERE id IN (SELECT id FROM crm.u WHERE EXISTS (SELECT 1 FROM billing.v))",
            "SELECT * FROM t WHERE id IN (SELECT id FROM {C}.u WHERE EXISTS (SELECT 1 FROM {B}.v))",
        ),
        (
            X,
            "SELECT * FROM (SELECT * FROM crm.u) AS d",
            "SELECT * FROM (SELECT * FROM {C}.u) AS d",
        ),
        (
            X,
            "WITH x AS (SELECT * FROM crm.u), y AS (SELECT * FROM x) SELECT * FROM y JOIN billing.z ON y.a = z.a",
            "WITH x AS (SELECT * FROM {C}.u), y AS (SELECT * FROM x) SELECT * FROM y JOIN {B}.z ON y.a = z.a",
        ),
        (
            X,
            "WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < (SELECT COUNT(*) FROM crm.t)) SELECT * FROM r",
            "WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < (SELECT COUNT(*) FROM {C}.t)) SELECT * FROM r",
        ),
        (
            X,
            "SELECT a FROM crm.u UNION ALL (SELECT a FROM billing.v) ORDER BY a LIMIT 3",
            "SELECT a FROM {C}.u UNION ALL (SELECT a FROM {B}.v) ORDER BY a LIMIT 3",
        ),
        (
            X,
            "(SELECT 1 FROM crm.a) UNION (SELECT 2 FROM crm.b)",
            "(SELECT 1 FROM {C}.a) UNION (SELECT 2 FROM {C}.b)",
        ),
        (X, "TABLE crm.u", "TABLE {C}.u"),
        (
            X,
            "SELECT crm.f(1), f(2), crm.f (3)",
            "SELECT {C}.f(1), f(2), {C}.f (3)",
        ),
        (
            X,
            "SELECT * FROM crm.a, LATERAL (SELECT * FROM crm.b WHERE b.x = a.x) AS l",
            "SELECT * FROM {C}.a, LATERAL (SELECT * FROM {C}.b WHERE b.x = a.x) AS l",
        ),
        (
            X,
            "SELECT * FROM crm.a WHERE a.id = 1 FOR UPDATE OF crm.a SKIP LOCKED",
            "SELECT * FROM {C}.a WHERE a.id = 1 FOR UPDATE OF {C}.a SKIP LOCKED",
        ),
        (
            X,
            "SELECT * FROM crm.a LOCK IN SHARE MODE",
            "SELECT * FROM {C}.a LOCK IN SHARE MODE",
        ),
        (
            X,
            "SELECT * FROM crm.a USE INDEX (i) JOIN crm.b FORCE INDEX FOR JOIN (j) ON a.x = b.x",
            "SELECT * FROM {C}.a USE INDEX (i) JOIN {C}.b FORCE INDEX FOR JOIN (j) ON a.x = b.x",
        ),
        (
            X,
            "SELECT * FROM (crm.a JOIN crm.b ON a.x = b.x), billing.c NATURAL JOIN crm.d STRAIGHT_JOIN crm.e",
            "SELECT * FROM ({C}.a JOIN {C}.b ON a.x = b.x), {B}.c NATURAL JOIN {C}.d STRAIGHT_JOIN {C}.e",
        ),
        (
            X,
            "SELECT a, (SELECT MAX(b) FROM crm.t2 WHERE t2.k = t1.k) AS m FROM crm.t1 GROUP BY a WITH ROLLUP HAVING COUNT(*) > 1",
            "SELECT a, (SELECT MAX(b) FROM {C}.t2 WHERE t2.k = t1.k) AS m FROM {C}.t1 GROUP BY a WITH ROLLUP HAVING COUNT(*) > 1",
        ),
        (
            X,
            "SELECT CASE WHEN x THEN crm.f() ELSE 0 END, EXTRACT(YEAR FROM d), TRIM(LEADING 'x' FROM s) FROM crm.t",
            "SELECT CASE WHEN x THEN {C}.f() ELSE 0 END, EXTRACT(YEAR FROM d), TRIM(LEADING 'x' FROM s) FROM {C}.t",
        ),
        (
            X,
            "SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS (id FOR ORDINALITY, v INT PATH '$')) AS j JOIN crm.t ON t.id = j.id",
            "SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS (id FOR ORDINALITY, v INT PATH '$')) AS j JOIN {C}.t ON t.id = j.id",
        ),
        (
            X,
            "SELECT a FROM crm.t WINDOW w AS (PARTITION BY b ORDER BY c) ORDER BY a",
            "SELECT a FROM {C}.t WINDOW w AS (PARTITION BY b ORDER BY c) ORDER BY a",
        ),
        (X, "SELECT 1 FROM dual", "="),
        (
            X,
            "SELECT a INTO @x FROM crm.t LIMIT 1",
            "SELECT a INTO @x FROM {C}.t LIMIT 1",
        ),
        (X, "VALUES ROW(1, 2), ROW(3, 4)", "="),
    ]);
}

#[test]
fn writes() {
    use On::{Both as X, MariaDb as Ma};
    run(&[
        (
            X,
            "INSERT INTO crm.t (a, b) VALUES (1, 2) ON DUPLICATE KEY UPDATE b = VALUES(b)",
            "INSERT INTO {C}.t (a, b) VALUES (1, 2) ON DUPLICATE KEY UPDATE b = VALUES(b)",
        ),
        (
            X,
            "INSERT IGNORE crm.t SET a = 1",
            "INSERT IGNORE {C}.t SET a = 1",
        ),
        (
            X,
            "INSERT INTO crm.t SELECT * FROM billing.s WHERE s.x > (SELECT MAX(x) FROM crm.t)",
            "INSERT INTO {C}.t SELECT * FROM {B}.s WHERE s.x > (SELECT MAX(x) FROM {C}.t)",
        ),
        (
            X,
            "INSERT INTO crm.t TABLE billing.s",
            "INSERT INTO {C}.t TABLE {B}.s",
        ),
        (X, "INSERT INTO t (a) VALUES (1)", "="),
        (
            X,
            "REPLACE INTO crm.t VALUES (1)",
            "REPLACE INTO {C}.t VALUES (1)",
        ),
        (
            X,
            "REPLACE crm.t (a) SELECT a FROM crm.u",
            "REPLACE {C}.t (a) SELECT a FROM {C}.u",
        ),
        (
            X,
            "UPDATE crm.a, billing.b SET crm.a.x = billing.b.y WHERE a.id = b.id",
            "UPDATE {C}.a, {B}.b SET {C}.a.x = {B}.b.y WHERE a.id = b.id",
        ),
        (
            X,
            "UPDATE crm.a JOIN crm.b ON a.id = b.id SET a.x = 1 WHERE b.y IN (SELECT y FROM billing.c)",
            "UPDATE {C}.a JOIN {C}.b ON a.id = b.id SET a.x = 1 WHERE b.y IN (SELECT y FROM {B}.c)",
        ),
        (X, "UPDATE LOW_PRIORITY t SET a = 1 ORDER BY b LIMIT 2", "="),
        (
            X,
            "DELETE FROM crm.a WHERE id = 1",
            "DELETE FROM {C}.a WHERE id = 1",
        ),
        (
            X,
            "DELETE a, crm.b FROM crm.a JOIN crm.b ON a.id = b.id",
            "DELETE a, {C}.b FROM {C}.a JOIN {C}.b ON a.id = b.id",
        ),
        (
            X,
            "DELETE FROM a, crm.b USING crm.a JOIN crm.b ON a.id = b.id",
            "DELETE FROM a, {C}.b USING {C}.a JOIN {C}.b ON a.id = b.id",
        ),
        (
            X,
            "DELETE t1.* FROM crm.t AS t1",
            "DELETE t1.* FROM {C}.t AS t1",
        ),
        (X, "DELETE crm.t.* FROM crm.t", "DELETE {C}.t.* FROM {C}.t"),
        (
            X,
            "WITH d AS (SELECT id FROM crm.old) DELETE FROM crm.t WHERE id IN (SELECT id FROM d)",
            "WITH d AS (SELECT id FROM {C}.old) DELETE FROM {C}.t WHERE id IN (SELECT id FROM d)",
        ),
        (
            Ma,
            "DELETE FROM crm.a WHERE id = 1 RETURNING id",
            "DELETE FROM {C}.a WHERE id = 1 RETURNING id",
        ),
        (
            X,
            "INSERT INTO crm.a VALUES (NEXT VALUE FOR crm.s, PREVIOUS VALUE FOR billing.s)",
            "INSERT INTO {C}.a VALUES (NEXT VALUE FOR {C}.s, PREVIOUS VALUE FOR {B}.s)",
        ),
        (
            X,
            "SELECT NEXTVAL(crm.s), LASTVAL(s), SETVAL(billing.s, 10)",
            "SELECT NEXTVAL({C}.s), LASTVAL(s), SETVAL({B}.s, 10)",
        ),
        (
            X,
            "LOAD DATA LOCAL INFILE '/tmp/x.csv' REPLACE INTO TABLE crm.t FIELDS TERMINATED BY ',' (a, @b) SET c = @b",
            "LOAD DATA LOCAL INFILE '/tmp/x.csv' REPLACE INTO TABLE {C}.t FIELDS TERMINATED BY ',' (a, @b) SET c = @b",
        ),
        (
            X,
            "LOAD XML LOCAL INFILE 'x.xml' INTO TABLE crm.t ROWS IDENTIFIED BY '<row>'",
            "LOAD XML LOCAL INFILE 'x.xml' INTO TABLE {C}.t ROWS IDENTIFIED BY '<row>'",
        ),
        (
            X,
            "CALL crm.p(1, (SELECT COUNT(*) FROM billing.t))",
            "CALL {C}.p(1, (SELECT COUNT(*) FROM {B}.t))",
        ),
        (X, "CALL p", "="),
        (X, "CALL crm.p", "CALL {C}.p"),
    ]);
}

#[test]
fn ddl() {
    use On::{Both as X, MariaDb as Ma};
    run(&[
        (
            X,
            "CREATE TABLE crm.t (id INT PRIMARY KEY, p INT, FOREIGN KEY (p) REFERENCES billing.parent (id) ON DELETE CASCADE)",
            "CREATE TABLE {C}.t (id INT PRIMARY KEY, p INT, FOREIGN KEY (p) REFERENCES {B}.parent (id) ON DELETE CASCADE)",
        ),
        (
            X,
            "CREATE TABLE crm.t (p INT REFERENCES crm.parent (id), c INT CHECK (c > 0), g INT AS (p + 1) VIRTUAL)",
            "CREATE TABLE {C}.t (p INT REFERENCES {C}.parent (id), c INT CHECK (c > 0), g INT AS (p + 1) VIRTUAL)",
        ),
        (
            X,
            "CREATE TABLE crm.t LIKE billing.s",
            "CREATE TABLE {C}.t LIKE {B}.s",
        ),
        (
            X,
            "CREATE TABLE crm.t (LIKE billing.s)",
            "CREATE TABLE {C}.t (LIKE {B}.s)",
        ),
        (
            X,
            "CREATE TABLE crm.t ENGINE=InnoDB COMMENT='x' AS SELECT * FROM billing.s",
            "CREATE TABLE {C}.t ENGINE=InnoDB COMMENT='x' AS SELECT * FROM {B}.s",
        ),
        (
            X,
            "CREATE TABLE crm.t SELECT a FROM crm.s",
            "CREATE TABLE {C}.t SELECT a FROM {C}.s",
        ),
        (
            X,
            "CREATE TEMPORARY TABLE IF NOT EXISTS crm.t (a INT) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (10))",
            "CREATE TEMPORARY TABLE IF NOT EXISTS {C}.t (a INT) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (10))",
        ),
        (
            X,
            "ALTER TABLE crm.t ADD COLUMN x INT, RENAME TO crm.t2",
            "ALTER TABLE {C}.t ADD COLUMN x INT, RENAME TO {C}.t2",
        ),
        (
            X,
            "ALTER TABLE crm.t RENAME COLUMN a TO b",
            "ALTER TABLE {C}.t RENAME COLUMN a TO b",
        ),
        (
            X,
            "ALTER TABLE crm.t RENAME AS billing.t",
            "ALTER TABLE {C}.t RENAME AS {B}.t",
        ),
        (
            X,
            "ALTER TABLE crm.t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES billing.p (id)",
            "ALTER TABLE {C}.t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES {B}.p (id)",
        ),
        (
            X,
            "ALTER TABLE crm.t EXCHANGE PARTITION p0 WITH TABLE billing.t2",
            "ALTER TABLE {C}.t EXCHANGE PARTITION p0 WITH TABLE {B}.t2",
        ),
        (
            X,
            "DROP TABLE IF EXISTS crm.a, billing.b, c",
            "DROP TABLE IF EXISTS {C}.a, {B}.b, c",
        ),
        (
            X,
            "DROP TEMPORARY TABLE crm.a",
            "DROP TEMPORARY TABLE {C}.a",
        ),
        (X, "DROP VIEW crm.v", "DROP VIEW {C}.v"),
        (
            X,
            "DROP PROCEDURE IF EXISTS crm.p",
            "DROP PROCEDURE IF EXISTS {C}.p",
        ),
        (X, "DROP FUNCTION crm.f", "DROP FUNCTION {C}.f"),
        (X, "DROP TRIGGER crm.tr", "DROP TRIGGER {C}.tr"),
        (
            X,
            "DROP EVENT IF EXISTS crm.e",
            "DROP EVENT IF EXISTS {C}.e",
        ),
        (X, "TRUNCATE TABLE crm.t", "TRUNCATE TABLE {C}.t"),
        (X, "TRUNCATE crm.t", "TRUNCATE {C}.t"),
        (
            X,
            "RENAME TABLE crm.a TO billing.a, crm.b TO crm.c",
            "RENAME TABLE {C}.a TO {B}.a, {C}.b TO {C}.c",
        ),
        (
            X,
            "CREATE INDEX i ON crm.t (a, b)",
            "CREATE INDEX i ON {C}.t (a, b)",
        ),
        (
            X,
            "CREATE UNIQUE INDEX i USING BTREE ON crm.t ((a + 1))",
            "CREATE UNIQUE INDEX i USING BTREE ON {C}.t ((a + 1))",
        ),
        (X, "DROP INDEX i ON crm.t", "DROP INDEX i ON {C}.t"),
        (
            X,
            "CREATE OR REPLACE ALGORITHM=MERGE DEFINER=`alice`@`%` SQL SECURITY INVOKER VIEW crm.v (a) AS SELECT a FROM billing.t WITH CHECK OPTION",
            "CREATE OR REPLACE ALGORITHM=MERGE DEFINER=`alice`@`%` SQL SECURITY INVOKER VIEW {C}.v (a) AS SELECT a FROM {B}.t WITH CHECK OPTION",
        ),
        (
            X,
            "CREATE DEFINER = 'alice'@'%' VIEW crm.v AS SELECT 1 FROM crm.t",
            "CREATE DEFINER = 'alice'@'%' VIEW {C}.v AS SELECT 1 FROM {C}.t",
        ),
        (
            X,
            "ALTER VIEW crm.v AS SELECT 1 FROM crm.t",
            "ALTER VIEW {C}.v AS SELECT 1 FROM {C}.t",
        ),
        (
            X,
            "ALTER PROCEDURE crm.p COMMENT 'x'",
            "ALTER PROCEDURE {C}.p COMMENT 'x'",
        ),
        (
            Ma,
            "CREATE SEQUENCE crm.s START WITH 1 INCREMENT BY 1",
            "CREATE SEQUENCE {C}.s START WITH 1 INCREMENT BY 1",
        ),
        (
            Ma,
            "DROP SEQUENCE IF EXISTS crm.s",
            "DROP SEQUENCE IF EXISTS {C}.s",
        ),
        (
            Ma,
            "ALTER SEQUENCE crm.s RESTART 5",
            "ALTER SEQUENCE {C}.s RESTART 5",
        ),
        (
            X,
            "ALTER DATABASE crm CHARACTER SET utf8mb4",
            "ALTER DATABASE {C} CHARACTER SET utf8mb4",
        ),
        (X, "ALTER SCHEMA DEFAULT COLLATE utf8mb4_bin", "="),
        (X, "DROP DATABASE crm", "DROP DATABASE {C}"),
        (X, "DROP SCHEMA IF EXISTS nope", "="),
        (X, "DROP DATABASE nope", "ERR 1049 Unknown database 'nope'"),
        (
            X,
            "DROP DATABASE information_schema",
            "ERR 39050 'information_schema' is a reserved name",
        ),
        (X, "CREATE DATABASE IF NOT EXISTS sales", "="),
        (
            X,
            "CREATE DATABASE t42_x",
            "ERR 39050 't42_x' is a reserved name",
        ),
        (
            X,
            "CREATE SCHEMA mysql",
            "ERR 39050 'mysql' is a reserved name",
        ),
        (
            X,
            "ANALYZE TABLE crm.a, billing.b",
            "ANALYZE TABLE {C}.a, {B}.b",
        ),
        (
            X,
            "OPTIMIZE NO_WRITE_TO_BINLOG TABLE crm.a",
            "OPTIMIZE NO_WRITE_TO_BINLOG TABLE {C}.a",
        ),
        (
            X,
            "CHECKSUM TABLE crm.a EXTENDED",
            "CHECKSUM TABLE {C}.a EXTENDED",
        ),
        (X, "REPAIR TABLE crm.a QUICK", "REPAIR TABLE {C}.a QUICK"),
        (
            X,
            "CHECK TABLE crm.a, crm.b FOR UPGRADE",
            "CHECK TABLE {C}.a, {C}.b FOR UPGRADE",
        ),
        (
            X,
            "FLUSH TABLES crm.a, crm.b WITH READ LOCK",
            "FLUSH TABLES {C}.a, {C}.b WITH READ LOCK",
        ),
        (X, "FLUSH TABLES WITH READ LOCK", "="),
        (X, "FLUSH PRIVILEGES", "="),
    ]);
}

#[test]
fn stored_programs() {
    use On::{Both as X, MariaDb as Ma};
    run(&[
        (
            X,
            "CREATE PROCEDURE crm.p(IN a INT, OUT b VARCHAR(10)) COMMENT 'x' SQL SECURITY INVOKER BEGIN \
             DECLARE done INT DEFAULT 0; DECLARE c CURSOR FOR SELECT id FROM billing.t; \
             DECLARE CONTINUE HANDLER FOR NOT FOUND, SQLSTATE '23000' SET done = 1; \
             lbl: LOOP FETCH c INTO a; IF done THEN LEAVE lbl; ELSEIF a > 1 THEN UPDATE crm.u SET x = a; \
             ELSE INSERT INTO crm.log VALUES (a); END IF; END LOOP lbl; \
             CASE a WHEN 1 THEN SELECT CASE WHEN a THEN 1 END FROM crm.v; ELSE DELETE FROM crm.w; END CASE; \
             WHILE a < 10 DO SET a = a + (SELECT 1 FROM crm.one); END WHILE; \
             REPEAT SET a = a - 1; UNTIL a < 0 END REPEAT; END",
            "CREATE PROCEDURE {C}.p(IN a INT, OUT b VARCHAR(10)) COMMENT 'x' SQL SECURITY INVOKER BEGIN \
             DECLARE done INT DEFAULT 0; DECLARE c CURSOR FOR SELECT id FROM {B}.t; \
             DECLARE CONTINUE HANDLER FOR NOT FOUND, SQLSTATE '23000' SET done = 1; \
             lbl: LOOP FETCH c INTO a; IF done THEN LEAVE lbl; ELSEIF a > 1 THEN UPDATE {C}.u SET x = a; \
             ELSE INSERT INTO {C}.log VALUES (a); END IF; END LOOP lbl; \
             CASE a WHEN 1 THEN SELECT CASE WHEN a THEN 1 END FROM {C}.v; ELSE DELETE FROM {C}.w; END CASE; \
             WHILE a < 10 DO SET a = a + (SELECT 1 FROM {C}.one); END WHILE; \
             REPEAT SET a = a - 1; UNTIL a < 0 END REPEAT; END",
        ),
        (
            X,
            "CREATE FUNCTION crm.f(x INT) RETURNS INT DETERMINISTIC RETURN (SELECT COUNT(*) FROM billing.t WHERE a = x)",
            "CREATE FUNCTION {C}.f(x INT) RETURNS INT DETERMINISTIC RETURN (SELECT COUNT(*) FROM {B}.t WHERE a = x)",
        ),
        (
            X,
            "CREATE FUNCTION crm.g() RETURNS VARCHAR(64) CHARACTER SET utf8mb4 READS SQL DATA BEGIN \
             DECLARE r VARCHAR(64); SELECT name INTO r FROM crm.t LIMIT 1; RETURN r; END",
            "CREATE FUNCTION {C}.g() RETURNS VARCHAR(64) CHARACTER SET utf8mb4 READS SQL DATA BEGIN \
             DECLARE r VARCHAR(64); SELECT name INTO r FROM {C}.t LIMIT 1; RETURN r; END",
        ),
        (
            X,
            "CREATE DEFINER = CURRENT_USER TRIGGER crm.tr BEFORE INSERT ON crm.t FOR EACH ROW SET NEW.a = (SELECT MAX(a) FROM billing.s)",
            "CREATE DEFINER = CURRENT_USER TRIGGER {C}.tr BEFORE INSERT ON {C}.t FOR EACH ROW SET NEW.a = (SELECT MAX(a) FROM {B}.s)",
        ),
        (
            X,
            "CREATE TRIGGER tr AFTER UPDATE ON crm.t FOR EACH ROW FOLLOWS tr0 BEGIN IF NEW.a <> OLD.a THEN \
             INSERT INTO billing.audit (a) VALUES (NEW.a); END IF; END",
            "CREATE TRIGGER tr AFTER UPDATE ON {C}.t FOR EACH ROW FOLLOWS tr0 BEGIN IF NEW.a <> OLD.a THEN \
             INSERT INTO {B}.audit (a) VALUES (NEW.a); END IF; END",
        ),
        (
            X,
            "CREATE EVENT IF NOT EXISTS crm.e ON SCHEDULE EVERY 1 HOUR STARTS CURRENT_TIMESTAMP + INTERVAL 1 DAY \
             ON COMPLETION PRESERVE DO DELETE FROM crm.log WHERE ts < NOW() - INTERVAL 7 DAY",
            "CREATE EVENT IF NOT EXISTS {C}.e ON SCHEDULE EVERY 1 HOUR STARTS CURRENT_TIMESTAMP + INTERVAL 1 DAY \
             ON COMPLETION PRESERVE DO DELETE FROM {C}.log WHERE ts < NOW() - INTERVAL 7 DAY",
        ),
        (
            X,
            "ALTER EVENT crm.e RENAME TO crm.e2 DO SELECT 1 FROM billing.t",
            "ALTER EVENT {C}.e RENAME TO {C}.e2 DO SELECT 1 FROM {B}.t",
        ),
        (
            X,
            "CREATE PROCEDURE crm.p() SELECT * FROM crm.t",
            "CREATE PROCEDURE {C}.p() SELECT * FROM {C}.t",
        ),
        (
            Ma,
            "BEGIN NOT ATOMIC DECLARE x INT DEFAULT (SELECT 1 FROM crm.t); SELECT DATABASE() FROM billing.u; END",
            "BEGIN NOT ATOMIC DECLARE x INT DEFAULT (SELECT 1 FROM {C}.t); SELECT 'billing' FROM {B}.u; END",
        ),
        (
            Ma,
            "CREATE PROCEDURE crm.p() BEGIN FOR r IN (SELECT a FROM crm.t) DO INSERT INTO crm.u VALUES (r.a); END FOR; END",
            "CREATE PROCEDURE {C}.p() BEGIN FOR r IN (SELECT a FROM {C}.t) DO INSERT INTO {C}.u VALUES (r.a); END FOR; END",
        ),
    ]);
}

#[test]
fn use_grant_lock_show_and_more() {
    use On::{Both as X, MariaDb as Ma};
    run(&[
        (X, "USE crm", "USE {C}"),
        (X, "USE `crm`", "USE {C}"),
        (X, "USE information_schema", "="),
        (X, "USE nope", "ERR 1049 Unknown database 'nope'"),
        (
            X,
            "GRANT SELECT, INSERT ON crm.* TO 'bob'@'%'",
            "GRANT SELECT, INSERT ON {C}.* TO 'bob'@'%'",
        ),
        (
            X,
            "GRANT SELECT (a, b), UPDATE (c) ON crm.t TO bob",
            "GRANT SELECT (a, b), UPDATE (c) ON {C}.t TO bob",
        ),
        (
            X,
            "GRANT EXECUTE ON PROCEDURE crm.p TO bob",
            "GRANT EXECUTE ON PROCEDURE {C}.p TO bob",
        ),
        (
            X,
            "GRANT ALTER ROUTINE ON FUNCTION crm.f TO bob",
            "GRANT ALTER ROUTINE ON FUNCTION {C}.f TO bob",
        ),
        (
            X,
            "REVOKE ALL PRIVILEGES ON crm.* FROM bob",
            "REVOKE ALL PRIVILEGES ON {C}.* FROM bob",
        ),
        (X, "GRANT ALL ON *.* TO bob", "="),
        (X, "GRANT SELECT ON * TO bob", "="),
        (X, "GRANT SELECT ON t TO bob", "="),
        (
            X,
            "GRANT SELECT ON TABLE crm.t TO bob WITH GRANT OPTION",
            "GRANT SELECT ON TABLE {C}.t TO bob WITH GRANT OPTION",
        ),
        (
            X,
            "GRANT SELECT ON `cr%`.* TO bob",
            "ERR 39051 name rewrite unsupported: a database pattern in GRANT: 'cr%'",
        ),
        (
            X,
            "GRANT SELECT ON `my\\_db`.* TO bob",
            "GRANT SELECT ON `t42_d7_g7_s7`.* TO bob",
        ),
        (X, "GRANT r1, r2 TO bob", "="),
        (
            X,
            "GRANT SELECT ON crm.* TO CURRENT_USER",
            "GRANT SELECT ON {C}.* TO CURRENT_USER",
        ),
        (X, "REVOKE ALL PRIVILEGES, GRANT OPTION FROM bob", "="),
        (
            X,
            "LOCK TABLES crm.a READ, crm.b AS b WRITE, c READ LOCAL, billing.d LOW_PRIORITY WRITE",
            "LOCK TABLES {C}.a READ, {C}.b AS b WRITE, c READ LOCAL, {B}.d LOW_PRIORITY WRITE",
        ),
        (X, "LOCK TABLE crm.a WRITE", "LOCK TABLE {C}.a WRITE"),
        (X, "UNLOCK TABLES", "="),
        (X, "SHOW TABLES FROM crm", "SHOW TABLES FROM {C}"),
        (
            X,
            "SHOW FULL TABLES IN crm LIKE 'a%'",
            "SHOW FULL TABLES IN {C} LIKE 'a%'",
        ),
        (X, "SHOW TABLES", "="),
        (
            X,
            "SHOW COLUMNS FROM t FROM crm",
            "SHOW COLUMNS FROM t FROM {C}",
        ),
        (
            X,
            "SHOW FULL COLUMNS FROM crm.t LIKE 'a%'",
            "SHOW FULL COLUMNS FROM {C}.t LIKE 'a%'",
        ),
        (
            X,
            "SHOW INDEX FROM t IN crm WHERE Key_name = 'x'",
            "SHOW INDEX FROM t IN {C} WHERE Key_name = 'x'",
        ),
        (
            X,
            "SHOW TABLE STATUS FROM crm LIKE 't%'",
            "SHOW TABLE STATUS FROM {C} LIKE 't%'",
        ),
        (X, "SHOW TRIGGERS FROM crm", "SHOW TRIGGERS FROM {C}"),
        (X, "SHOW EVENTS IN crm", "SHOW EVENTS IN {C}"),
        (X, "SHOW OPEN TABLES FROM crm", "SHOW OPEN TABLES FROM {C}"),
        (X, "SHOW CREATE TABLE crm.t", "SHOW CREATE TABLE {C}.t"),
        (X, "SHOW CREATE VIEW crm.v", "SHOW CREATE VIEW {C}.v"),
        (
            X,
            "SHOW CREATE PROCEDURE crm.p",
            "SHOW CREATE PROCEDURE {C}.p",
        ),
        (
            X,
            "SHOW CREATE FUNCTION crm.f",
            "SHOW CREATE FUNCTION {C}.f",
        ),
        (
            X,
            "SHOW CREATE TRIGGER crm.tr",
            "SHOW CREATE TRIGGER {C}.tr",
        ),
        (X, "SHOW CREATE EVENT crm.e", "SHOW CREATE EVENT {C}.e"),
        (X, "SHOW CREATE DATABASE crm", "SHOW CREATE DATABASE {C}"),
        (
            X,
            "SHOW CREATE SCHEMA IF NOT EXISTS crm",
            "SHOW CREATE SCHEMA IF NOT EXISTS {C}",
        ),
        (X, "SHOW DATABASES LIKE 'c%'", "="),
        (X, "SHOW VARIABLES LIKE 'sql_mode'", "="),
        (X, "SHOW PROCESSLIST", "="),
        (Ma, "SHOW PROCEDURE CODE crm.p", "SHOW PROCEDURE CODE {C}.p"),
        (X, "DESCRIBE crm.t", "DESCRIBE {C}.t"),
        (X, "DESC crm.t a%", "DESC {C}.t a%"),
        (
            X,
            "EXPLAIN SELECT * FROM crm.t",
            "EXPLAIN SELECT * FROM {C}.t",
        ),
        (
            X,
            "EXPLAIN FORMAT=JSON DELETE FROM crm.t",
            "EXPLAIN FORMAT=JSON DELETE FROM {C}.t",
        ),
        (X, "EXPLAIN FOR CONNECTION 5", "="),
        (X, "HANDLER crm.t OPEN AS h", "HANDLER {C}.t OPEN AS h"),
        (X, "HANDLER h READ FIRST WHERE a > 1", "="),
        (
            X,
            "SET @x = (SELECT COUNT(*) FROM crm.t)",
            "SET @x = (SELECT COUNT(*) FROM {C}.t)",
        ),
        (
            X,
            "SET SESSION sql_mode = 'ANSI', @@time_zone = '+00:00'",
            "=",
        ),
        (X, "SET NAMES utf8mb4 COLLATE utf8mb4_bin", "="),
        (
            X,
            "DO (SELECT 1 FROM crm.t LIMIT 1)",
            "DO (SELECT 1 FROM {C}.t LIMIT 1)",
        ),
        (
            Ma,
            "SET STATEMENT max_statement_time = 1 FOR SELECT * FROM crm.t",
            "SET STATEMENT max_statement_time = 1 FOR SELECT * FROM {C}.t",
        ),
        (
            X,
            "PREPARE s FROM 'SELECT * FROM crm.t WHERE a = ?'",
            "PREPARE s FROM 'SELECT * FROM {C}.t WHERE a = ?'",
        ),
        (X, "PREPARE s FROM @sql", "="),
        (X, "EXECUTE s USING @a, @b", "="),
        (X, "DEALLOCATE PREPARE s", "="),
        (
            Ma,
            "EXECUTE IMMEDIATE 'DELETE FROM crm.t'",
            "EXECUTE IMMEDIATE 'DELETE FROM {C}.t'",
        ),
        (
            X,
            "PREPARE s FROM 'SELECT ''x'', DATABASE() FROM crm.t'",
            "PREPARE s FROM 'SELECT ''x'', ''billing'' FROM {C}.t'",
        ),
        (X, "START TRANSACTION READ ONLY", "="),
        (X, "BEGIN", "="),
        (X, "COMMIT", "="),
        (X, "XA START 'x'", "="),
        (X, "KILL QUERY 5", "="),
        (X, "CREATE USER 'bob'@'%' IDENTIFIED BY 'pw'", "="),
        (X, "SET PASSWORD FOR CURRENT_USER() = 'x'", "="),
        (X, "SET ROLE ALL", "="),
        (X, "INSTALL SONAME 'x'", "="),
        (
            X,
            "FOO crm.t",
            "ERR 39051 name rewrite unsupported: qualified name crm.t in a position the rewriter does not handle",
        ),
        (
            X,
            "SELECT * FROM nope.t",
            "ERR 1049 Unknown database 'nope'",
        ),
        (
            X,
            "SELECT * FROM t42_d2_g2_s2.t",
            "ERR 1049 Unknown database 't42_d2_g2_s2'",
        ),
        (
            X,
            "SELECT * FROM t43_d1_g1_s1.secret",
            "ERR 1049 Unknown database 't43_d1_g1_s1'",
        ),
        (X, "", "="),
        (X, ";;", "="),
    ]);
}

#[test]
fn system_schemas_are_identified_not_rewritten() {
    use On::Both as X;
    run(&[
        (
            X,
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'crm'",
            "=",
        ),
        (X, "SELECT * FROM performance_schema.threads", "="),
        (X, "SELECT * FROM mysql.time_zone_name", "="),
        (
            X,
            "SELECT * FROM INFORMATION_SCHEMA.columns c JOIN crm.t ON c.TABLE_NAME = t.n",
            "SELECT * FROM INFORMATION_SCHEMA.columns c JOIN {C}.t ON c.TABLE_NAME = t.n",
        ),
        (
            X,
            "SELECT information_schema.tables.table_name FROM information_schema.tables",
            "=",
        ),
        (X, "SELECT sys.format_bytes(1)", "="),
    ]);
    let map = acme();
    let r = rewrite(
        b"SELECT * FROM information_schema.TABLES t JOIN mysql.user u; USE sys",
        &ctx(&map, mysql(), Engine::Auto),
    )
    .unwrap();
    let refs = &r.statements[0].system_refs;
    assert_eq!(refs.len(), 2);
    assert_eq!(refs[0].schema, SystemSchema::InformationSchema);
    assert_eq!(refs[1].schema, SystemSchema::Mysql);
    assert_eq!(
        r.statements[1].use_target,
        Some(UseTarget::System(SystemSchema::Sys))
    );
    // The walker names the object; the AST path does too when they agree.
    let r = rewrite(
        b"SELECT * FROM information_schema.TABLES",
        &ctx(&map, mysql(), Engine::Lexer),
    )
    .unwrap();
    assert_eq!(
        r.statements[0].system_refs[0].object.as_deref(),
        Some("TABLES")
    );
}

#[test]
fn reserved_names() {
    use On::Both as X;
    run(&[
        (
            X,
            "SELECT * FROM crm.__sprig_dtx",
            "ERR 1146 Table 'crm.__sprig_dtx' doesn't exist",
        ),
        (
            X,
            "SELECT * FROM __SPRIG_x",
            "ERR 1146 Table 'billing.__SPRIG_x' doesn't exist",
        ),
        (
            X,
            "SELECT _sprig_rk FROM t",
            "ERR 1054 Unknown column '_sprig_rk' in 'field list'",
        ),
        (
            X,
            "SELECT t._sprig_rk FROM t",
            "ERR 1054 Unknown column '_sprig_rk' in 'field list'",
        ),
        (
            X,
            "SELECT `_Sprig_p_x` FROM t",
            "ERR 1054 Unknown column '_Sprig_p_x' in 'field list'",
        ),
        (
            X,
            "CREATE TABLE __sprig_x (a INT)",
            "ERR 39050 '__sprig_x' is a reserved name",
        ),
        (
            X,
            "CREATE TABLE t (_sprig_x INT)",
            "ERR 39050 '_sprig_x' is a reserved name",
        ),
        (
            X,
            "CALL crm.__sprig_p()",
            "ERR 1305 PROCEDURE crm.__sprig_p does not exist",
        ),
        (
            X,
            "SELECT crm.__sprig_f(1)",
            "ERR 1305 FUNCTION crm.__sprig_f does not exist",
        ),
        (
            X,
            "DROP TRIGGER crm.__sprig_t",
            "ERR 1360 Trigger does not exist",
        ),
        (X, "SELECT sprig_t, `__sprig` FROM t", "="),
    ]);
    // Internal sessions may use them.
    let map = acme();
    let mut c = ctx(&map, mysql(), Engine::Auto);
    c.internal = true;
    let r = rewrite(b"SELECT _sprig_rk FROM crm.__sprig_dtx", &c).unwrap();
    assert_eq!(
        r.sql_lossy(),
        format!("SELECT _sprig_rk FROM {C}.__sprig_dtx")
    );
}

#[test]
fn substitutions() {
    use On::{Both as X, MariaDb as Ma, MySql as My};
    run(&[
        (
            X,
            "SELECT DATABASE(), SCHEMA(), database()",
            "SELECT 'billing', 'billing', 'billing'",
        ),
        (
            X,
            "SELECT CURRENT_USER, CURRENT_USER(), USER(), SESSION_USER(), SYSTEM_USER()",
            "SELECT 'alice@%', 'alice@%', 'alice@203.0.113.7', 'alice@203.0.113.7', 'alice@203.0.113.7'",
        ),
        (
            X,
            "SELECT user, `database` FROM t WHERE u = CURRENT_USER",
            "SELECT user, `database` FROM t WHERE u = 'alice@%'",
        ),
        (
            X,
            "SELECT crm.database(), crm.user()",
            "SELECT {C}.database(), {C}.user()",
        ),
        (X, "SELECT DATABASE ()", "="),
        (X, "SELECT DATABASE(1)", "="),
        (
            My,
            "SELECT mysql_query_attribute_string('tag'), mysql_query_attribute_string('nul'), mysql_query_attribute_string('none')",
            "SELECT 'v''1', NULL, NULL",
        ),
        (Ma, "SELECT mysql_query_attribute_string('tag')", "="),
        (
            My,
            "SELECT mysql_query_attribute_string(@n)",
            "ERR 39051 name rewrite unsupported: mysql_query_attribute_string() needs a string literal",
        ),
        (
            X,
            "CREATE VIEW crm.v AS SELECT DATABASE(), CURRENT_USER()",
            "CREATE VIEW {C}.v AS SELECT DATABASE(), CURRENT_USER()",
        ),
        (
            X,
            "CREATE PROCEDURE crm.p() SELECT USER(), DATABASE()",
            "CREATE PROCEDURE {C}.p() SELECT USER(), DATABASE()",
        ),
        (
            X,
            "CREATE TABLE crm.t AS SELECT DATABASE() AS d",
            "CREATE TABLE {C}.t AS SELECT 'billing' AS d",
        ),
        (
            X,
            "CREATE DEFINER=CURRENT_USER() PROCEDURE crm.p() SELECT 1",
            "CREATE DEFINER=CURRENT_USER() PROCEDURE {C}.p() SELECT 1",
        ),
        (
            X,
            "INSERT INTO crm.t VALUES (DATABASE(), USER())",
            "INSERT INTO {C}.t VALUES ('billing', 'alice@203.0.113.7')",
        ),
        (X, "SET @d = DATABASE()", "SET @d = 'billing'"),
        (X, "USE crm; SELECT DATABASE()", "USE {C}; SELECT 'crm'"),
    ]);
    // No current database: NULL. IGNORE_SPACE: a space before ( is fine.
    let map = acme();
    let mut c = ctx(&map, mysql(), Engine::Auto).current_db(None);
    assert_eq!(
        rewrite(b"SELECT DATABASE()", &c).unwrap().sql_lossy(),
        "SELECT NULL"
    );
    c.ignore_space = true;
    assert_eq!(
        rewrite(b"SELECT DATABASE ()", &c).unwrap().sql_lossy(),
        "SELECT NULL"
    );
    // Literals follow NO_BACKSLASH_ESCAPES.
    let c = ctx(&map, mysql(), Engine::Auto).current_db(Some("o'k\\db"));
    assert_eq!(
        rewrite(b"SELECT DATABASE()", &c).unwrap().sql_lossy(),
        r"SELECT 'o''k\\db'"
    );
    let c = ctx(&map, mysql().no_backslash_escapes(true), Engine::Auto).current_db(Some("o'k\\db"));
    assert_eq!(
        rewrite(b"SELECT DATABASE()", &c).unwrap().sql_lossy(),
        r"SELECT 'o''k\db'"
    );
    // Without an account, account functions stay.
    let mut c = ctx(&map, mysql(), Engine::Auto);
    c.account = None;
    assert_eq!(
        rewrite(b"SELECT USER()", &c).unwrap().sql_lossy(),
        "SELECT USER()"
    );
}

#[test]
fn quoting_and_lexing_corpus() {
    use On::{Both as X, MariaDb as Ma, MySql as My};
    run(&[
        (
            X,
            "SELECT * FROM `crm`.`customers`",
            "SELECT * FROM {C}.`customers`",
        ),
        (
            X,
            "SELECT * FROM `my.db`.t",
            "SELECT * FROM `t42_d3_g3_s3`.t",
        ),
        (
            X,
            "SELECT * FROM `a``b`.t",
            "SELECT * FROM `t42_d4_g4_s4`.t",
        ),
        (
            X,
            "SELECT * FROM Ünïcødé.t JOIN `Ünïcødé`.u",
            "SELECT * FROM `t42_d5_g5_s5`.t JOIN `t42_d5_g5_s5`.u",
        ),
        (
            X,
            "SELECT * FROM `select`.t",
            "SELECT * FROM `t42_d6_g6_s6`.t",
        ),
        (
            X,
            "SELECT * FROM mixed.t",
            "ERR 1049 Unknown database 'mixed'",
        ),
        (
            X,
            "SELECT * FROM crm/* c */.t",
            "SELECT * FROM {C}/* c */.t",
        ),
        (X, "SELECT * FROM crm . t", "SELECT * FROM {C} . t"),
        (
            X,
            "SELECT * FROM crm -- x\n.t",
            "SELECT * FROM {C} -- x\n.t",
        ),
        (X, "SELECT * FROM crm#x\n.t", "SELECT * FROM {C}#x\n.t"),
        (
            X,
            "SELECT * FROM cr/**/m.t",
            "ERR 39051 name rewrite unsupported: qualified name m.t in a position the rewriter does not handle",
        ),
        (
            X,
            "SELECT 'crm.t', \"crm.t\" FROM crm.t",
            "SELECT 'crm.t', \"crm.t\" FROM {C}.t",
        ),
        (
            X,
            "SELECT 'it\\'s crm.t' FROM crm.t",
            "SELECT 'it\\'s crm.t' FROM {C}.t",
        ),
        (
            X,
            "SELECT _utf8mb4'crm.t' COLLATE utf8mb4_bin, N'crm.x' FROM crm.t",
            "SELECT _utf8mb4'crm.t' COLLATE utf8mb4_bin, N'crm.x' FROM {C}.t",
        ),
        (
            X,
            "SELECT 0x1F, X'0A', b'01', 0b11, 1e5, .5, 1.5e-3, 1e+2 FROM crm.t",
            "SELECT 0x1F, X'0A', b'01', 0b11, 1e5, .5, 1.5e-3, 1e+2 FROM {C}.t",
        ),
        (
            X,
            "SELECT crm.t.1col FROM crm.t",
            "SELECT {C}.t.1col FROM {C}.t",
        ),
        (X, "SELECT 1abc FROM crm.t", "SELECT 1abc FROM {C}.t"),
        (X, "SELECT 1--1 FROM crm.t", "SELECT 1--1 FROM {C}.t"),
        (
            X,
            "SELECT 1 -- crm.t\nFROM crm.u",
            "SELECT 1 -- crm.t\nFROM {C}.u",
        ),
        (
            X,
            "SELECT 1 /* crm.t */ FROM crm.u",
            "SELECT 1 /* crm.t */ FROM {C}.u",
        ),
        (
            X,
            "SELECT /*+ MAX_EXECUTION_TIME(10) */ * FROM crm.t",
            "SELECT /*+ MAX_EXECUTION_TIME(10) */ * FROM {C}.t",
        ),
        (
            X,
            "SELECT @`crm.t`, @'crm'.x, @crm.t, @@session.sql_mode FROM crm.t",
            "SELECT @`crm.t`, @'crm'.x, @crm.t, @@session.sql_mode FROM {C}.t",
        ),
        (
            X,
            "SELECT /*!50001 crm.f(), */ 1 FROM crm.t",
            "SELECT /*!50001 {C}.f(), */ 1 FROM {C}.t",
        ),
        (
            X,
            "SELECT /*!*/ 1, /*! crm.f() */ FROM crm.t",
            "SELECT /*!*/ 1, /*! {C}.f() */ FROM {C}.t",
        ),
        (
            My,
            "SELECT 1 /*!80016 , crm.f() */ FROM crm.t",
            "SELECT 1 /*!80016 , {C}.f() */ FROM {C}.t",
        ),
        (
            Ma,
            "SELECT 1 /*!80016 , crm.f() */ FROM crm.t",
            "SELECT 1 /*!80016 , crm.f() */ FROM {C}.t",
        ),
        (
            My,
            "SELECT 1 /*M!100100 , crm.f() */ FROM crm.t",
            "SELECT 1 /*M!100100 , crm.f() */ FROM {C}.t",
        ),
        (
            Ma,
            "SELECT 1 /*M!100100 , crm.f() */ FROM crm.t",
            "SELECT 1 /*M!100100 , {C}.f() */ FROM {C}.t",
        ),
        (
            X,
            "SELECT 1 /*!99999 , crm.f() */ FROM crm.t",
            "SELECT 1 /*!99999 , crm.f() */ FROM {C}.t",
        ),
        (
            X,
            "SELECT 1 /*!999999 , crm.f() */ FROM crm.t",
            "SELECT 1 /*!999999 , crm.f() */ FROM {C}.t",
        ),
        (
            X,
            "/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE */;\nCREATE TABLE crm.t (a INT) /*!50100 PARTITION BY HASH (a) */",
            "/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE */;\nCREATE TABLE {C}.t (a INT) /*!50100 PARTITION BY HASH (a) */",
        ),
        (
            X,
            "/*!50001 CREATE ALGORITHM=UNDEFINED */ /*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */ /*!50001 VIEW `v` AS select `crm`.`t`.`a` AS `a` from `crm`.`t` */",
            "/*!50001 CREATE ALGORITHM=UNDEFINED */ /*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */ /*!50001 VIEW `v` AS select {C}.`t`.`a` AS `a` from {C}.`t` */",
        ),
        (
            X,
            "select\t*\r\nfrom\n  CRM.t",
            "ERR 1049 Unknown database 'CRM'",
        ),
    ]);
}

#[test]
fn sql_mode_changes_tokens() {
    let map = acme();
    let ansi = mysql().ansi_quotes(true);
    assert_eq!(
        both(&map, ansi, "SELECT \"a\" FROM \"crm\".\"t\""),
        format!("SELECT \"a\" FROM {C}.\"t\"")
    );
    assert_eq!(
        both(&map, ansi, "SELECT * FROM \"my.db\".t"),
        "SELECT * FROM `t42_d3_g3_s3`.t"
    );
    assert_eq!(
        both(&map, ansi, "SELECT * FROM \"a`b\".t"),
        "SELECT * FROM `t42_d4_g4_s4`.t"
    );
    assert_eq!(
        both(&map, mysql(), "SELECT \"crm\".\"t\""),
        "SELECT \"crm\".\"t\""
    );
    // NO_BACKSLASH_ESCAPES: 'a\' ends the string, so the name after it is
    // real; with escapes, it is inside the string.
    let nbe = mysql().no_backslash_escapes(true);
    assert_eq!(
        both(&map, nbe, "SELECT 'a\\', crm.t.x FROM crm.t"),
        format!("SELECT 'a\\', {C}.t.x FROM {C}.t")
    );
    assert_eq!(
        both(&map, mysql(), "SELECT 'a\\', crm.t.x FROM crm.t'"),
        "SELECT 'a\\', crm.t.x FROM crm.t'"
    );
    for p in [
        mariadb().ansi_quotes(true),
        mariadb().no_backslash_escapes(true),
    ] {
        assert_eq!(
            both(&map, p, "SELECT 1 FROM crm.t"),
            format!("SELECT 1 FROM {C}.t")
        );
    }
}

#[test]
fn neutralizes_comments_the_backend_would_run() {
    let map = acme();
    let c = ctx(&map, mysql(), Engine::Auto).backend_version(110_803);
    let r = rewrite(
        b"SELECT 1 /*M!100100 , (SELECT * FROM crm.t) */ FROM crm.u",
        &c,
    )
    .unwrap();
    assert_eq!(
        r.sql_lossy(),
        format!("SELECT 1 /*M 100100 , (SELECT * FROM crm.t) */ FROM {C}.u")
    );
    let r = rewrite(b"SELECT 1 /*!100100 , x */", &c).unwrap();
    assert_eq!(r.sql_lossy(), "SELECT 1 /* 100100 , x */");
    // What both run, or neither, is left alone.
    for s in [
        "SELECT /*!50001 1 */",
        "SELECT /*!99999 1 */",
        "SELECT /*!80016 1 */",
    ] {
        assert_eq!(rewrite(s.as_bytes(), &c).unwrap().sql_lossy(), s);
    }
}

#[test]
fn statement_info() {
    let map = acme();
    let sql = b"USE crm; CREATE DATABASE sales; ALTER DATABASE billing COMMENT 'x'; SELECT 1 FROM crm.t;\nDROP DATABASE crm";
    let r = rewrite(sql, &ctx(&map, mysql(), Engine::Auto)).unwrap();
    let kinds: Vec<StmtKind> = r.statements.iter().map(|s| s.kind).collect();
    assert_eq!(
        kinds,
        [
            StmtKind::Use,
            StmtKind::CreateDatabase,
            StmtKind::AlterDatabase,
            StmtKind::Select,
            StmtKind::DropDatabase
        ]
    );
    match &r.statements[0].use_target {
        Some(UseTarget::Database { logical, schema }) => {
            assert_eq!(logical, "crm");
            assert_eq!(schema.to_string(), "t42_d2_g2_s2");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(r.statements[1].database.as_deref(), Some("sales"));
    assert_eq!(r.statements[2].database.as_deref(), Some("billing"));
    assert_eq!(r.statements[4].database.as_deref(), Some("crm"));
    assert_eq!(&sql[r.statements[3].range.clone()], b"SELECT 1 FROM crm.t");
    assert_eq!(r.statements[3].engine, Engine::Ast);
    // A compound body is one statement.
    let r = rewrite(
        b"CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END; SELECT 3",
        &ctx(&map, mysql(), Engine::Lexer),
    )
    .unwrap();
    assert_eq!(r.statements.len(), 2);
}

#[test]
fn round_trips() {
    let map = acme();
    let cases = [
        "SELECT * FROM `crm`.`t` JOIN `billing`.u ON t.a = u.a",
        "INSERT INTO `crm`.t SELECT `billing`.s.a FROM `billing`.s /* keep */",
        "CREATE PROCEDURE `crm`.p() BEGIN UPDATE `billing`.t SET a = 1; END",
        "SELECT * FROM `my.db`.t, `a``b`.u, `Ünïcødé`.v",
    ];
    for sql in cases {
        let r = rewrite(sql.as_bytes(), &ctx(&map, mysql(), Engine::Auto)).unwrap();
        assert!(!r.is_unchanged());
        let back = unrewrite(&r.sql, &map as &dyn NameMap, &mysql());
        assert_eq!(String::from_utf8(back).unwrap(), sql);
    }
    // Other tenants' names and the catalog are not ours to map back.
    let foreign = b"SELECT * FROM t43_d1_g1_s1.t, t42_sprig.x, t42_d1_g1_s1.y";
    assert_eq!(
        String::from_utf8(unrewrite(foreign, &map, &mysql())).unwrap(),
        "SELECT * FROM t43_d1_g1_s1.t, t42_sprig.x, `billing`.y"
    );
}

#[test]
fn edits_report_every_change() {
    let map = acme();
    let sql = b"SELECT DATABASE(), crm.f() FROM billing.t";
    let r = rewrite(sql, &ctx(&map, mysql(), Engine::Auto)).unwrap();
    assert_eq!(r.edits.len(), 3);
    for e in &r.edits {
        let original = std::str::from_utf8(&sql[e.range.clone()]).unwrap();
        assert!(
            ["DATABASE()", "crm", "billing"].contains(&original),
            "{original}"
        );
    }
}

#[test]
fn ast_engine_alone() {
    let map = acme();
    let c = ctx(&map, mysql(), Engine::Ast);
    assert_eq!(
        rewrite(b"SELECT a FROM crm.t JOIN billing.u USING (id)", &c)
            .unwrap()
            .sql_lossy(),
        format!("SELECT a FROM {C}.t JOIN {B}.u USING (id)")
    );
    // Statements sqlparser-rs does not model fail closed on the AST engine.
    assert_eq!(
        rewrite(b"LOCK TABLES crm.t READ", &c).unwrap_err().errno(),
        39051
    );
}
