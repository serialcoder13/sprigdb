-- R4 round 5 (chapter 28): MySQL's JSON operators and JSON constants in
-- comparisons (Django's KT() fields, key lookups and JSON checks),
-- functional key parts mixed with plain ones and their printing in
-- STATISTICS and SHOW INDEX, a descending index over a foreign key's
-- column, lateral aggregates inside expressions (gaps row 71), stored
-- code as written (gaps row 69), USE performance_schema (gaps row 68)
-- and statement digests. The collection-write check (39602) has no MySQL
-- counterpart and is tested in sprig-sql's unit tests and live.
DROP VIEW IF EXISTS c_mj_n;
DROP PROCEDURE IF EXISTS c_mj_p;
DROP TABLE IF EXISTS c_mj_b;
DROP TABLE IF EXISTS c_mj_a;
DROP TABLE IF EXISTS c_mj_j;
DROP TABLE IF EXISTS c_mj_x;
DROP TABLE IF EXISTS c_mj_o;
DROP TABLE IF EXISTS c_mj_c;
DROP TABLE IF EXISTS c_mj_dig;
CREATE TABLE c_mj_j (id INT PRIMARY KEY, d JSON, s LONGTEXT GENERATED ALWAYS AS (d->>'$."status"') STORED, v JSON GENERATED ALWAYS AS (d->'$.v') VIRTUAL, CONSTRAINT c_mj_ck CHECK (JSON_EXTRACT(d, '$."version"') = JSON_EXTRACT('"stable"', '$')));
INSERT INTO c_mj_j (id, d) VALUES (1, '{"status": "ok", "version": "stable", "v": [1, 2], "n": 5}');
INSERT INTO c_mj_j (id, d) VALUES (2, '{"version": "dev"}');
SELECT d->>'$.status', d->'$.v', c_mj_j.d->>'$.n' AS n, s, v FROM c_mj_j;
SELECT id FROM c_mj_j WHERE JSON_EXTRACT(d, '$."version"') = JSON_EXTRACT('"stable"', '$') AND JSON_EXTRACT(d, '$.status') IN (JSON_EXTRACT('"ok"', '$'), JSON_EXTRACT('"x"', '$'));
SELECT id FROM c_mj_j WHERE d->>'$.status' = 'ok' AND JSON_EXTRACT(d, '$.status') <> JSON_EXTRACT('"no"', '$');
CREATE INDEX c_mj_jk ON c_mj_j ((CAST(d->>'$.n' AS SIGNED)));
SHOW CREATE TABLE c_mj_j;
CREATE TABLE c_mj_a (id INT PRIMARY KEY, name VARCHAR(100), height INT, uuid CHAR(32));
CREATE TABLE c_mj_b (id INT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(100), pub_date DATE, CONSTRAINT c_mj_fk FOREIGN KEY (author_id) REFERENCES c_mj_a (id));
CREATE INDEX c_mj_bi ON c_mj_b (author_id DESC, (LOWER(title)) ASC, pub_date);
ALTER TABLE c_mj_a ADD CONSTRAINT c_mj_au UNIQUE (height DESC, uuid, (LOWER(name)) ASC);
CREATE TABLE c_mj_x (id INT PRIMARY KEY, a INT, s VARCHAR(20), KEY c_mj_x1 ((a * 2 + 1)), KEY c_mj_x2 ((CONCAT(s, 'x'))), KEY c_mj_x3 ((a BETWEEN 1 AND 5)));
-- MySQL keeps the foreign key's own index next to c_mj_bi; MariaDB drops it.
SELECT TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, COLLATION, EXPRESSION FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE 'c\_mj\_%' AND INDEX_NAME NOT IN ('PRIMARY', 'c_mj_fk') ORDER BY 1, 2, 3;
SHOW INDEX FROM c_mj_x WHERE Key_name LIKE 'c\_mj\_x%';
DROP INDEX c_mj_bi ON c_mj_b;
ALTER TABLE c_mj_a DROP INDEX c_mj_au;
CREATE INDEX c_mj_bt ON c_mj_b (author_id DESC, title);
DROP INDEX c_mj_bt ON c_mj_b;
SELECT INDEX_NAME, COLUMN_NAME, COLLATION FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'c_mj_b' ORDER BY 1, SEQ_IN_INDEX;
CREATE TABLE c_mj_c (id INT PRIMARY KEY);
CREATE TABLE c_mj_o (cid INT, amt INT);
INSERT INTO c_mj_c VALUES (1), (2), (3);
INSERT INTO c_mj_o VALUES (1, 10), (1, 5), (2, 7);
SELECT MAX(l.t), COUNT(*), SUM(l.t) + 1, MAX(l.t) AS m FROM c_mj_c AS c, LATERAL (SELECT SUM(o.amt) AS t FROM c_mj_o AS o WHERE o.cid = c.id) AS l;
CREATE VIEW c_mj_n AS SELECT MAX(l.t), SUM(l.t) + 1, MAX(l.t) AS m FROM c_mj_c AS c, LATERAL (SELECT SUM(o.amt) AS t FROM c_mj_o AS o WHERE o.cid = c.id) AS l;
SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'c_mj_n' ORDER BY ORDINAL_POSITION;
SELECT * FROM c_mj_n;
DELIMITER //
CREATE PROCEDURE c_mj_p(IN x INT)
BEGIN
  -- the top order of each customer
  SELECT c.id, l.amt FROM c_mj_c AS c, LATERAL (SELECT o.amt FROM c_mj_o AS o WHERE o.cid = c.id ORDER BY o.amt DESC LIMIT 1) AS l WHERE c.id <= x ORDER BY c.id;
END//
DELIMITER ;
SELECT ROUTINE_DEFINITION FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'c_mj_p';
CALL c_mj_p(3);
USE performance_schema;
SELECT DATABASE();
SHOW TABLES LIKE 'events_statements_summary_by_d%';
SELECT COUNT(*) > 0 FROM threads WHERE PROCESSLIST_ID = CONNECTION_ID();
USE @DB@;
SELECT DATABASE();
CREATE TABLE c_mj_dig (id INT PRIMARY KEY, v INT);
INSERT INTO c_mj_dig VALUES (1, 1), (2, 2);
TRUNCATE TABLE performance_schema.events_statements_summary_by_digest;
SELECT v FROM c_mj_dig WHERE id = 1;
SELECT v FROM c_mj_dig WHERE id = 2;
SELECT v FROM c_mj_dig WHERE id = 3;
SELECT nope FROM c_mj_dig;
SELECT SCHEMA_NAME = DATABASE(), COUNT_STAR, SUM_ROWS_SENT, SUM_ERRORS, SUM_TIMER_WAIT > 0, MIN_TIMER_WAIT <= MAX_TIMER_WAIT, FIRST_SEEN <= LAST_SEEN FROM performance_schema.events_statements_summary_by_digest WHERE DIGEST_TEXT LIKE '%c\_mj\_dig%' ORDER BY COUNT_STAR DESC;
SELECT db = DATABASE(), exec_count, err_count FROM sys.statement_analysis WHERE query LIKE '%c\_mj\_dig%' ORDER BY exec_count DESC;
SELECT exec_count, errors, error_pct FROM sys.statements_with_errors_or_warnings WHERE query LIKE '%c\_mj\_dig%';
DROP TABLE c_mj_dig;
DROP VIEW c_mj_n;
DROP PROCEDURE c_mj_p;
DROP TABLE c_mj_b;
DROP TABLE c_mj_a;
DROP TABLE c_mj_j;
DROP TABLE c_mj_x;
DROP TABLE c_mj_o;
DROP TABLE c_mj_c;
