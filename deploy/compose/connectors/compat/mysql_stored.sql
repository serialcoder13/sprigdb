-- The 3306 rules inside views and stored programs, and MySQL's printing
-- of expressions in SHOW CREATE TABLE (chapter 28 steps 10 and 12, R4
-- round 4). 39602 (what needs the gateway) has no MySQL counterpart and
-- is tested in the gateway's unit tests.
DROP VIEW IF EXISTS c_ms_v1;
DROP VIEW IF EXISTS c_ms_v2;
DROP PROCEDURE IF EXISTS c_ms_p;
DROP FUNCTION IF EXISTS c_ms_f;
DROP TABLE IF EXISTS c_ms_o;
DROP TABLE IF EXISTS c_ms_c;
DROP TABLE IF EXISTS c_ms_x;
CREATE TABLE c_ms_c (id INT PRIMARY KEY, name VARCHAR(20));
CREATE TABLE c_ms_o (id INT AUTO_INCREMENT PRIMARY KEY, cid INT, amt INT);
INSERT INTO c_ms_c VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
INSERT INTO c_ms_o (cid, amt) VALUES (1, 10), (1, 20), (1, 15), (2, 5), (3, 7), (3, 8), (3, 9);
CREATE VIEW c_ms_v1 AS SELECT c.name, l.amt FROM c_ms_c AS c, LATERAL (SELECT o.amt FROM c_ms_o AS o WHERE o.cid = c.id ORDER BY o.amt DESC LIMIT 2) AS l;
SELECT * FROM c_ms_v1 ORDER BY name, amt;
CREATE VIEW c_ms_v2 AS SELECT cid, SUM(amt) AS s FROM c_ms_o GROUP BY cid WITH ROLLUP ORDER BY cid DESC;
SELECT * FROM c_ms_v2;
DELIMITER //
CREATE PROCEDURE c_ms_p(IN lim INT)
BEGIN
  DECLARE n INT;
  SELECT c.name, l.total, l.cnt FROM c_ms_c AS c LEFT JOIN LATERAL (SELECT SUM(o.amt) AS total, COUNT(*) AS cnt FROM c_ms_o AS o WHERE o.cid = c.id) AS l ON TRUE ORDER BY c.name;
  IF lim > 0 THEN
    SELECT c.name, l.amt FROM c_ms_c AS c JOIN LATERAL (SELECT o.amt FROM c_ms_o AS o WHERE o.cid = c.id ORDER BY o.amt LIMIT 1) AS l ON TRUE ORDER BY c.name;
  END IF;
END//
CREATE FUNCTION c_ms_f(k INT) RETURNS INT READS SQL DATA
  RETURN (SELECT l.t FROM c_ms_c AS c, LATERAL (SELECT SUM(o.amt) AS t FROM c_ms_o AS o WHERE o.cid = c.id) AS l WHERE c.id = k)//
DELIMITER ;
CALL c_ms_p(1);
SELECT c_ms_f(2), c_ms_f(4);
CREATE TABLE c_ms_x (a INT, b INT, s VARCHAR(20), d DATE,
  g1 INT AS (a + 1), g2 INT AS (a * b - 2) STORED, g3 VARCHAR(40) AS (concat(s, 'x', lower(s))),
  g4 INT AS (CASE WHEN a > 0 THEN 1 ELSE 0 END), g5 INT AS (IF(a IS NULL, 0, a)), g6 BIGINT AS (CAST(a * 2 AS UNSIGNED)),
  g7 INT AS (a BETWEEN 1 AND 5), g8 INT AS (a IN (1, 2, 3)), g9 INT AS (-a), g10 INT AS (a DIV 2 + a MOD 3),
  g11 VARCHAR(30) AS (substr(s, 1, 3)), g12 INT AS ((a > 1 AND b < 2) OR a = 5), g13 INT AS (s LIKE 'ab%'),
  g14 VARCHAR(30) AS (date_format(d, '%Y')), c1 INT DEFAULT (a + 1),
  CONSTRAINT c_ms_ck1 CHECK (a > 0 AND b <> 3), CONSTRAINT c_ms_ck2 CHECK (s IN ('x', 'y')),
  INDEX c_ms_fx ((a * 2)), INDEX c_ms_fl ((lower(s))));
SHOW CREATE TABLE c_ms_x;
-- LATERAL shapes the rewrite can't express, applied in the gateway.
SELECT c.name, l.amt FROM c_ms_c AS c JOIN LATERAL (SELECT o.amt FROM c_ms_o AS o WHERE o.cid >= c.id ORDER BY o.amt DESC LIMIT 2) AS l ON TRUE ORDER BY c.name, l.amt;
SELECT c.name, l.cid, l.n FROM c_ms_c AS c LEFT JOIN LATERAL (SELECT o.cid, COUNT(*) AS n FROM c_ms_o AS o WHERE o.cid < c.id GROUP BY o.cid) AS l ON TRUE ORDER BY c.name, l.cid;
SELECT c.name, l.m FROM c_ms_c AS c, LATERAL (SELECT DISTINCT o.amt % 2 AS m FROM c_ms_o AS o WHERE o.cid = c.id AND o.amt > c.id) AS l WHERE c.id < 4 ORDER BY 1, 2;
SELECT COUNT(*), SUM(l.amt) FROM c_ms_c AS c, LATERAL (SELECT o.amt FROM c_ms_o AS o WHERE o.amt BETWEEN c.id AND c.id * 5) AS l;
SELECT c.name, l.amt FROM c_ms_c AS c JOIN LATERAL (SELECT o.amt FROM c_ms_o AS o WHERE o.cid > c.id + 10) AS l ON TRUE;
DROP TABLE c_ms_x;
DROP FUNCTION c_ms_f;
DROP PROCEDURE c_ms_p;
DROP VIEW c_ms_v2;
DROP VIEW c_ms_v1;
DROP TABLE c_ms_o;
DROP TABLE c_ms_c;
