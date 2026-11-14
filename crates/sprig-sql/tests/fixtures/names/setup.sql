-- Schemas for the name layer's live tests and fixture capture, as tenant
-- 9999 with databases billing (db 1) and crm (db 2). t9999_d7_g7_s7 is a
-- schema of the tenant's id space the name map doesn't know (a stand-in
-- for another tenant's schema) and t9999_sprig is the tenant catalog:
-- neither may ever show up for the tenant. One statement per line.
DROP DATABASE IF EXISTS t9999_d1_g1_s1;
DROP DATABASE IF EXISTS t9999_d2_g2_s2;
DROP DATABASE IF EXISTS t9999_sprig;
DROP DATABASE IF EXISTS t9999_d7_g7_s7;
CREATE DATABASE t9999_d1_g1_s1;
CREATE DATABASE t9999_d2_g2_s2;
CREATE DATABASE t9999_sprig;
CREATE DATABASE t9999_d7_g7_s7;
CREATE TABLE t9999_sprig.`databases` (id INT PRIMARY KEY);
CREATE TABLE t9999_d7_g7_s7.secret (id INT PRIMARY KEY);
CREATE VIEW t9999_d7_g7_s7.v_secret AS SELECT id FROM t9999_d7_g7_s7.secret;
CREATE PROCEDURE t9999_d7_g7_s7.p_secret() SELECT 1;
CREATE TABLE t9999_d2_g2_s2.accounts (id INT PRIMARY KEY, name VARCHAR(50));
CREATE TABLE t9999_d1_g1_s1.customers (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(100) NOT NULL, email VARCHAR(200), _sprig_rk BIGINT INVISIBLE, PRIMARY KEY (id), UNIQUE KEY email (email), KEY idx_rk (_sprig_rk)) COMMENT='people';
CREATE TABLE t9999_d1_g1_s1.orders (id INT NOT NULL, customer_id INT NOT NULL, account_id INT, total DECIMAL(10,2) DEFAULT 0.00, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, _sprig_p_status VARCHAR(20) INVISIBLE, PRIMARY KEY (id), KEY sprig_auto_3fa91c0e (customer_id, created_at), KEY idx_status (_sprig_p_status), KEY idx_mixed (account_id, _sprig_p_status), CONSTRAINT fk_cust FOREIGN KEY (customer_id) REFERENCES t9999_d1_g1_s1.customers (id), CONSTRAINT fk_acct FOREIGN KEY (account_id) REFERENCES t9999_d2_g2_s2.accounts (id), CONSTRAINT chk_total CHECK (total >= 0), CONSTRAINT chk_status CHECK (_sprig_p_status <> ''));
CREATE TABLE t9999_d1_g1_s1.gipk (my_row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT INVISIBLE, v INT, PRIMARY KEY (my_row_id));
CREATE TABLE t9999_d1_g1_s1.__sprig_lk_orders__email (k VARCHAR(200) PRIMARY KEY);
CREATE TABLE t9999_d1_g1_s1.parts (id INT, d DATE) PARTITION BY RANGE (YEAR(d)) (PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN MAXVALUE);
CREATE VIEW t9999_d1_g1_s1.v_orders AS SELECT o.id, a.name FROM `t9999_d1_g1_s1`.orders o JOIN `t9999_d2_g2_s2`.accounts a ON a.id = o.account_id;
CREATE VIEW t9999_d1_g1_s1.__sprig_v AS SELECT 1 AS x;
CREATE PROCEDURE t9999_d1_g1_s1.p_count(IN n INT) SELECT COUNT(*) FROM `t9999_d1_g1_s1`.orders WHERE id > n;
CREATE FUNCTION t9999_d1_g1_s1.f_double(x INT) RETURNS INT DETERMINISTIC RETURN x * 2;
CREATE PROCEDURE t9999_d1_g1_s1.__sprig_internal() SELECT 1;
CREATE TRIGGER t9999_d1_g1_s1.trg_orders BEFORE INSERT ON t9999_d1_g1_s1.orders FOR EACH ROW SET NEW.total = IFNULL(NEW.total, (SELECT COUNT(*) FROM `t9999_d2_g2_s2`.accounts));
CREATE TRIGGER t9999_d1_g1_s1.__sprig_trg BEFORE UPDATE ON t9999_d1_g1_s1.orders FOR EACH ROW SET NEW.total = NEW.total;
CREATE EVENT t9999_d1_g1_s1.ev_purge ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DISABLE DO DELETE FROM `t9999_d1_g1_s1`.orders WHERE created_at < NOW() - INTERVAL 1 YEAR;
CREATE EVENT t9999_d1_g1_s1.__sprig_ev ON SCHEDULE EVERY 1 DAY STARTS '2030-01-01 00:00:00' DISABLE DO SELECT 1;
