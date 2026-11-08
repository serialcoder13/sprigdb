-- The statements behind the binlog corpus (see capture.sh). tests/corpus.rs
-- asserts the values written here, so change both together.
SET time_zone = '+00:00';
CREATE DATABASE corpus;
CREATE TABLE corpus.t (
  id INT PRIMARY KEY,
  ti TINYINT, biu BIGINT UNSIGNED, mi MEDIUMINT, f FLOAT, d DOUBLE,
  dec65 DECIMAL(65,30), dec10 DECIMAL(10,2), b13 BIT(13), y YEAR,
  dt DATE, tm3 TIME(3), dtm6 DATETIME(6), ts TIMESTAMP(2) NULL,
  c CHAR(10), bn BINARY(4), vc VARCHAR(300), tx TEXT, bl BLOB, j JSON,
  e ENUM('a','b','c'), s SET('a','b','c','d'), g GEOMETRY,
  u UUID, i4 INET4, i6 INET6, vec VECTOR(2),
  cvc VARCHAR(500) COMPRESSED, ctx TEXT COMPRESSED
);
INSERT INTO corpus.t VALUES (
  1, -128, 18446744073709551615, -8388608, 1.5, -2.25,
  '-12345678901234567890123456789012345.123456789012345678901234567890', '-0.05',
  b'1010101010101', 2155, '2024-02-29', '-838:59:58.999', '2024-02-29 01:02:03.123456',
  '2038-01-19 03:14:07.99', 'héllo', x'00ff', 'vär', 'text', x'deadbeef',
  '{"a": [1, 2.5]}', 'c', 'a,d', ST_GeomFromText('POINT(1 2)', 4326),
  '12345678-9abc-1def-8123-456789abcdef', '10.0.0.0', '2001:db8::1',
  VEC_FromText('[1.5,-2]'), REPEAT('ab', 250), REPEAT('cd', 2000)
);
INSERT INTO corpus.t (id) VALUES (2);
UPDATE corpus.t SET ti = 127, vc = NULL WHERE id = 1;
DELETE FROM corpus.t WHERE id = 2;
CREATE TABLE corpus.x (id INT PRIMARY KEY);
XA START 'corpus1', 'b', 7;
INSERT INTO corpus.x VALUES (1);
XA END 'corpus1', 'b', 7;
XA PREPARE 'corpus1', 'b', 7;
XA COMMIT 'corpus1', 'b', 7;
XA START 'corpus2';
INSERT INTO corpus.x VALUES (2);
XA END 'corpus2';
XA PREPARE 'corpus2';
XA ROLLBACK 'corpus2';
XA START 'corpus3';
INSERT INTO corpus.x VALUES (3);
XA END 'corpus3';
XA COMMIT 'corpus3' ONE PHASE;
SET SESSION binlog_alter_two_phase = 1;
ALTER TABLE corpus.x ADD COLUMN w INT;
SET SESSION binlog_alter_two_phase = 0;
CREATE TABLE corpus.z (id INT PRIMARY KEY, v LONGTEXT);
