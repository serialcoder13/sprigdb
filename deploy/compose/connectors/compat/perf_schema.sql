-- performance_schema and sys as MySQL 8.4 clients and tools read them
-- (chapter 28 step 13, R4 round 4). Only values that don't depend on the
-- server's history or its version string are printed.
DROP VIEW IF EXISTS ps_v;
DROP TABLE IF EXISTS ps_t;
CREATE TABLE ps_t (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, a INT, KEY ka (a));
CREATE VIEW ps_v AS SELECT a FROM ps_t;
SELECT VARIABLE_NAME, VARIABLE_VALUE FROM performance_schema.session_variables WHERE VARIABLE_NAME IN ('autocommit', 'performance_schema', 'sql_mode', 'max_execution_time', 'collation_connection', 'character_set_client', 'transaction_isolation', 'gtid_mode') ORDER BY 1;
SET SESSION max_execution_time = 1500;
SELECT VARIABLE_VALUE FROM performance_schema.session_variables WHERE VARIABLE_NAME = 'max_execution_time';
SELECT VARIABLE_VALUE FROM performance_schema.global_variables WHERE VARIABLE_NAME = 'max_execution_time';
SELECT COUNT(*) > 400 FROM performance_schema.global_variables;
SELECT VARIABLE_NAME, VARIABLE_VALUE LIKE '8.4.%' FROM performance_schema.global_variables WHERE VARIABLE_NAME = 'version';
SELECT VARIABLE_NAME FROM performance_schema.session_status WHERE VARIABLE_NAME IN ('Com_select', 'Uptime', 'Threads_connected', 'Bytes_received', 'com_insert') ORDER BY 1;
INSERT INTO ps_t (a) VALUES (1), (2);
INSERT INTO ps_t (a) VALUES (3);
SELECT VARIABLE_VALUE FROM performance_schema.session_status WHERE VARIABLE_NAME = 'Com_insert';
SELECT VARIABLE_VALUE > 0 FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime';
SELECT COUNT(*) FROM performance_schema.status_by_thread WHERE THREAD_ID = PS_CURRENT_THREAD_ID() AND VARIABLE_NAME = 'Com_insert';
SELECT NAME, TYPE, PROCESSLIST_COMMAND, PROCESSLIST_DB = DATABASE() FROM performance_schema.threads WHERE PROCESSLIST_ID = CONNECTION_ID();
SELECT THREAD_ID = PS_CURRENT_THREAD_ID() FROM performance_schema.threads WHERE PROCESSLIST_ID = CONNECTION_ID();
SELECT PS_THREAD_ID(CONNECTION_ID()) = PS_CURRENT_THREAD_ID();
SELECT t.PROCESSLIST_INFO FROM performance_schema.threads AS t WHERE t.PROCESSLIST_ID = CONNECTION_ID();
SELECT COMMAND, INFO FROM performance_schema.processlist WHERE ID = CONNECTION_ID();
SELECT ATTR_NAME, ATTR_VALUE FROM performance_schema.session_account_connect_attrs WHERE PROCESSLIST_ID = CONNECTION_ID() AND ATTR_NAME IN ('_client_name', 'program_name') ORDER BY 1;
SELECT sys_version FROM sys.version;
SELECT command FROM sys.session WHERE conn_id = CONNECTION_ID();
SELECT FORMAT_BYTES(0), FORMAT_BYTES(1023), FORMAT_BYTES(1048575), FORMAT_BYTES(-2048), FORMAT_BYTES(18446744073709551615), FORMAT_BYTES(NULL);
SELECT FORMAT_PICO_TIME(999), FORMAT_PICO_TIME(1500), FORMAT_PICO_TIME(3290000000000), FORMAT_PICO_TIME(60000000000000), FORMAT_PICO_TIME(-1500);
SELECT sys.format_bytes(1536) AS b, sys.format_time(1500) AS t1, sys.format_time(604800000000000000) AS t2, sys.format_time(-1500) AS t3, sys.quote_identifier('a`b') AS q;
SELECT COUNT(*) FROM performance_schema.events_statements_summary_by_digest WHERE SCHEMA_NAME = 'ps_nosuch';
SELECT COUNT(*) FROM performance_schema.data_lock_waits;
SELECT object_type, count FROM sys.schema_object_overview WHERE db = DATABASE() ORDER BY 1;
SELECT table_name, column_name, data_type, is_signed, is_unsigned, max_value FROM sys.schema_auto_increment_columns WHERE table_schema = DATABASE();
SELECT * FROM performance_schema.ps_nosuch;
SELECT * FROM sys.ps_nosuch;
DROP VIEW ps_v;
DROP TABLE ps_t;
