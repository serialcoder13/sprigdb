-- MySQL's answers to DDL that MariaDB answers differently, found by
-- Sequelize's and Rails' suites.
DROP TABLE IF EXISTS c_md6_fc, c_md6_fc2, c_md6_fp, c_md6_ct, c_md6_my, c_md6_my2, c_md6_dn, c_md6_nl, c_md6_mu;
DROP PROCEDURE IF EXISTS c_md6_p;
-- A column's utf8mb4_bin reads back as written; text columns' types.
CREATE TABLE c_md6_ct (a varchar(1) COLLATE utf8mb4_bin, b varchar(1) COLLATE utf8mb4_general_ci, c varchar(1), t tinytext, m mediumtext) DEFAULT CHARSET=utf8mb4;
SHOW FULL COLUMNS FROM c_md6_ct;
SELECT COLUMN_NAME, COLLATION_NAME, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'c_md6_ct' ORDER BY ORDINAL_POSITION;
SHOW CREATE DATABASE @DB@;
-- Foreign keys: 1824 and 1826.
CREATE TABLE c_md6_fp (id int PRIMARY KEY);
CREATE TABLE c_md6_fc (id int, p int, CONSTRAINT c_md6_fk1 FOREIGN KEY (p) REFERENCES c_md6_missing (id));
CREATE TABLE c_md6_fc (id int, p int, CONSTRAINT c_md6_fk1 FOREIGN KEY (p) REFERENCES c_md6_fp (id));
CREATE TABLE c_md6_fc2 (id int, p int, CONSTRAINT c_md6_fk1 FOREIGN KEY (p) REFERENCES c_md6_fp (id));
ALTER TABLE c_md6_fc ADD CONSTRAINT c_md6_fk2 FOREIGN KEY (id) REFERENCES c_md6_nope (id);
-- Names longer than 64 characters: 1059.
CREATE TABLE c_md6_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (id int);
RENAME TABLE c_md6_ct TO c_md6_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;
ALTER TABLE c_md6_ct RENAME TO c_md6_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;
CREATE VIEW c_md6_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa AS SELECT 1;
-- MyISAM has no descending indexes: 1178.
CREATE TABLE c_md6_my (a int, b int, KEY k (a DESC, b)) ENGINE=MyISAM;
CREATE TABLE c_md6_my2 (a int, KEY k (a)) ENGINE=MyISAM;
CREATE INDEX k2 ON c_md6_my2 (a DESC);
ALTER TABLE c_md6_my2 ADD UNIQUE INDEX k3 (a DESC);
-- DEFAULT (now()) is an expression default, apart from CURRENT_TIMESTAMP.
CREATE TABLE c_md6_dn (id int, ts datetime DEFAULT (now()), ts2 datetime(6) DEFAULT (now(6)), ts3 timestamp NULL DEFAULT CURRENT_TIMESTAMP, d date DEFAULT (curdate()), lt datetime DEFAULT LOCALTIMESTAMP);
SHOW CREATE TABLE c_md6_dn;
SELECT COLUMN_NAME, COLUMN_DEFAULT, EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'c_md6_dn' ORDER BY ORDINAL_POSITION;
ALTER TABLE c_md6_dn MODIFY ts datetime NULL DEFAULT (now());
ALTER TABLE c_md6_dn ALTER COLUMN lt SET DEFAULT (CURRENT_TIMESTAMP);
SHOW FULL COLUMNS FROM c_md6_dn;
-- A default holding a newline, a backslash and a quote.
CREATE TABLE c_md6_nl (a varchar(20) DEFAULT 'x\ny', b varchar(20) DEFAULT 'a\\nb', c varchar(10) DEFAULT 'it''s');
SELECT COLUMN_NAME, HEX(COLUMN_DEFAULT) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'c_md6_nl' ORDER BY ORDINAL_POSITION;
-- Dropping a column of a multi-column unique index.
CREATE TABLE c_md6_mu (id int PRIMARY KEY, a int, b varchar(20), c int, UNIQUE KEY u (b(10) DESC, a) COMMENT 'cm', KEY k (a, c));
ALTER TABLE c_md6_mu DROP COLUMN a;
SHOW CREATE TABLE c_md6_mu;
-- KILL QUERY of the session's own statement interrupts the KILL.
KILL QUERY CONNECTION_ID();
SELECT 'after kill';
-- A stored program records the creating session's character sets.
SET NAMES latin1;
CREATE PROCEDURE c_md6_p() SELECT 1;
SET NAMES utf8mb4;
SELECT CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'c_md6_p';
DROP PROCEDURE c_md6_p;
CREATE PROCEDURE c_md6_p() SELECT 1;
SELECT CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'c_md6_p';
DROP PROCEDURE c_md6_p;
DROP TABLE IF EXISTS c_md6_fc, c_md6_fp, c_md6_ct, c_md6_my, c_md6_my2, c_md6_dn, c_md6_nl, c_md6_mu;
