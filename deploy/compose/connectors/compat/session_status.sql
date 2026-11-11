-- Per-session status counters (gaps row 46): a session sees its own
-- statements, wherever its backend connections come from. Com_select
-- isn't compared: the mysql client sends another SELECT at login to a
-- server whose version string isn't MySQL's own.
DROP TABLE IF EXISTS c_ss_t;
CREATE TABLE c_ss_t (a INT);
SHOW SESSION STATUS WHERE Variable_name IN ('Com_insert', 'Com_update', 'Com_delete', 'Com_set_option', 'Com_show_status');
INSERT INTO c_ss_t VALUES (1);
INSERT INTO c_ss_t SELECT a + 1 FROM c_ss_t;
UPDATE c_ss_t SET a = a + 10;
DELETE FROM c_ss_t WHERE a > 11;
SET @x = 1;
SHOW SESSION STATUS WHERE Variable_name IN ('Com_insert', 'Com_insert_select', 'Com_update', 'Com_delete', 'Com_set_option', 'Com_show_status');
SHOW STATUS LIKE 'Com_set_option';
DROP TABLE c_ss_t;
