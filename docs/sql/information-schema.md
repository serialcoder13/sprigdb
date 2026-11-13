---
title: information_schema and SHOW
audience: [user, dba]
sidebar:
  order: 10
---

# information_schema and SHOW

SprigDB answers `information_schema`, `SHOW`, `performance_schema` and `sys` as a MySQL 8.4 or MariaDB server would, for your tenant only: you see your databases and tables under the names you gave them, never MariaDB's physical schemas, other tenants' objects or SprigDB's internal objects. This page says what each shows and hides, the views and `SHOW` statements SprigDB adds, and the session variables and functions it adds. Use it when a tool's metadata looks different from what you expect.

## Names in metadata

Every result that names a schema shows the logical name: `shop`, not the physical `t42_d1_g3_s1`. When a table is sharded, metadata shows it once, as one table. This holds in `SHOW` output, `information_schema` views, `SHOW CREATE …` text (including `REFERENCES` and `DEFINER`), error messages and warnings.

Each `information_schema` table that names schemas becomes a view restricted to your tenant's schemas, with logical names in the select list. The rest of your query runs unchanged in MariaDB, so `WHERE`, joins and `ORDER BY` behave as usual.

```sql test
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, total DECIMAL(10,2));
SELECT TABLE_SCHEMA = DATABASE() AS same_name, TABLE_NAME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE();
```

```text output
same_name	TABLE_NAME
1	orders
```

| Table | What you get |
|---|---|
| `SCHEMATA` | The persona's system schemas and your databases |
| `TABLES`, `COLUMNS`, `STATISTICS`, `TABLE_CONSTRAINTS`, `KEY_COLUMN_USAGE`, `REFERENTIAL_CONSTRAINTS`, `CHECK_CONSTRAINTS`, `VIEWS`, `ROUTINES`, `PARAMETERS`, `TRIGGERS`, `EVENTS` and the other per-schema tables | Your objects, with logical names; internal objects hidden |
| `PROCESSLIST` | Your tenant's sessions on the gateways, from SprigDB's session table: all of them with `PROCESS`, otherwise your own |
| Server-wide tables (`INNODB_*`, `GLOBAL_STATUS`, and the like) | Refused with 1142 |
| Tables without schema names (`CHARACTER_SETS`, `COLLATIONS`, `ENGINES`, …) | Passed through |

On the MySQL persona (3306), the tables have MySQL 8.4's shape: `SELECT *` returns MySQL's columns; `COLUMNS` reports `json` for JSON columns and no integer display widths, and shows defaults unquoted with `DEFAULT_GENERATED`; collations use MySQL's names; `STATISTICS` has `IS_VISIBLE` and `EXPRESSION`; `TABLE_CONSTRAINTS` has `ENFORCED`; a foreign key's default rule reads `NO ACTION`; MariaDB's JSON checks don't appear as constraints. 3306 also answers `KEYWORDS` (MySQL 8.4's words), `COLLATIONS` and `CHARACTER_SETS` with MySQL's names, and an empty `COLUMN_STATISTICS`. The MariaDB persona (3307) shows MariaDB's tables.

`information_schema` tables inside a view definition or stored program fail with 39051 `ER_SPRIG_NAME_REWRITE_UNSUPPORTED`, because they would bake physical names into the object.

## What is hidden

| Object | Hidden from | Shown with |
|---|---|---|
| Internal tables, views, routines, triggers and events (`__sprig_…`) | Every `SHOW` and `information_schema` view | `@sprig_show_internal = 1` |
| Internal columns (`_sprig_…`), and indexes and constraints on them | `SHOW COLUMNS`, `SHOW INDEX`, `SHOW CREATE TABLE`, the views; `SELECT *` | `@sprig_show_internal = 1` |
| Automatic indexes (`sprig_auto_…`) | `SHOW INDEX`, `SHOW CREATE TABLE`, `information_schema.STATISTICS` | `@sprig_show_auto_indexes = 1` (or `@sprig_show_internal = 1`) |
| The generated invisible primary key `my_row_id` | The same | `show_gipk_in_create_table_and_information_schema`: `ON` by default on 3306, `OFF` on 3307 |

A reference to an internal object fails as if it didn't exist (1146), and to an internal column as an unknown column (1054). `CREATE TABLE … LIKE` never copies automatic indexes or internal objects, whatever the session shows.

### @sprig_show_auto_indexes

```sql
SET @sprig_show_auto_indexes = {1 | 0 | ON | OFF | TRUE | FALSE}
```

Shows or hides the indexes SprigDB's automatic indexing created, for this session. SprigDB keeps the variable itself; it never reaches MariaDB. It must be set on its own, not together with other variables in one `SET`, and to one of the values shown; otherwise the statement fails with 39051. While it is on, you may `DROP INDEX` an automatic index; otherwise that fails with 39050. See [Automatic indexing](../performance/automatic-indexing.md).

```sql test
SET @sprig_show_auto_indexes = 1;
SHOW INDEX FROM orders;
SET @sprig_show_auto_indexes = 0;
```

### @sprig_show_internal

```sql
SET @sprig_show_internal = 1
```

Shows every internal object, read-only, for diagnosis. Only users holding `ALL` on the tenant may use it.

## Reserved names

| Name | Rule | Error |
|---|---|---|
| Objects starting with `__sprig_` | Can't be created | 39050 `ER_SPRIG_RESERVED_NAME` |
| Columns starting with `_sprig_` | Can't be created | 39050 |
| Indexes starting with `sprig_auto_` (any case) | Can't be created, renamed to or altered; dropped only with `@sprig_show_auto_indexes = 1` | 39050 |
| Databases named `t<digits>_…`, or `information_schema`, `performance_schema`, `sys`, `mysql` | Reserved for physical and system schemas | 39050 |
| User variables starting with `@sprig_` | Belong to the gateway (`@sprig_show_auto_indexes`, `@sprig_show_internal`, `@sprig_space`, `@sprig_inheritance`); `@sprig_rls` can't be named at all | 39050 or 39051 |
| `sprig.…` | SprigDB's own procedures, such as `sprig.commit_offset` and `sprig.mq_publish` | |

```sql test-error
-- 39050 ER_SPRIG_RESERVED_NAME
CREATE TABLE __sprig_notes (id INT PRIMARY KEY);
```

```sql test-error
-- 39050 ER_SPRIG_RESERVED_NAME
CREATE INDEX sprig_auto_customer ON orders (customer_id);
```

## SHOW statements

### MySQL and MariaDB SHOW statements

| Statement | How SprigDB answers |
|---|---|
| `SHOW DATABASES`, `SHOW SCHEMAS` | From the tenant's catalog: the databases you can reach (all with `SHOW DATABASES`); `LIKE` and `WHERE` work |
| `SHOW [FULL] TABLES` | The `Tables_in_<logical name>` header; internal tables left out; typed tables' parents before their subtypes |
| `SHOW TABLE STATUS`, `SHOW OPEN TABLES`, `SHOW TRIGGERS`, `SHOW EVENTS`, `SHOW PROCEDURE STATUS`, `SHOW FUNCTION STATUS`, `SHOW PACKAGE [BODY] STATUS` | `Db` and `Database` columns logical; other schemas' rows and internal objects left out |
| `SHOW [FULL] COLUMNS`, `SHOW FIELDS`, `DESCRIBE` | Internal columns left out; `my_row_id` per the GIPK variable. On 3306, built from the MySQL-shaped `COLUMNS` |
| `SHOW INDEX`, `SHOW KEYS` | Automatic indexes and indexes on hidden columns left out |
| `SHOW CREATE TABLE`, `SHOW CREATE SEQUENCE` | Hidden lines removed; schemas in `REFERENCES` logical; MySQL's spelling on 3306; for inheritable and typed tables, the table as you created it |
| `SHOW CREATE VIEW`, `TRIGGER`, `EVENT`, `DATABASE` | Physical schemas and definers in the text mapped |
| `SHOW CREATE PROCEDURE`, `SHOW CREATE FUNCTION` | The original text you created it with, when SprigDB has it |
| `SHOW [FULL] PROCESSLIST` | Your tenant's sessions from SprigDB's session table: all with `PROCESS`, else your own. Without `FULL`, `Info` is cut to 100 characters |
| `SHOW [SESSION] STATUS` | Counters of your own session's statements (SprigDB counts them; backend connections are pooled). `Uptime` is the gateway's; `Prepared_stmt_count` is your session's |
| `SHOW VARIABLES` | The persona's emulated values (below); everything else from MariaDB, with physical names scrubbed |
| `SHOW WARNINGS`, `SHOW ERRORS` | Messages with logical names; SprigDB's own notes and warnings (`39xxx`) appear here |
| `SHOW GRANTS` | SprigDB's grants; see [Tenants and accounts](tenants-and-accounts.md#show-grants) |
| `SHOW ENGINE …`, `SHOW BINARY LOGS`, `SHOW BINLOG EVENTS`, `SHOW MASTER STATUS`, `SHOW BINLOG STATUS`, `SHOW REPLICA STATUS`, `SHOW SLAVE STATUS`, `SHOW SLAVE HOSTS`, `SHOW REPLICAS` and other server-wide statements | Refused with 1227 |
| Anything else (`SHOW COLLATION`, `SHOW CHARACTER SET`, `SHOW ENGINES`, …) | Passed to MariaDB, with physical names scrubbed from the values |

```sql test-error
-- server-wide: 1227
SHOW ENGINE INNODB STATUS;
```

### SHOW statements SprigDB adds

| Statement | Lists | Reference |
|---|---|---|
| `SHOW SPRIG SETTINGS [LIKE …] [FOR …]` | Settings and their sources | [Settings](settings.md#show-sprig-settings) |
| `SPRIG SHOW PINS`, `SPRIG SHOW ADVICE` | Sharding pins; the sharding advisor's design | [Sharding](sharding.md#sprig-show-pins) |
| `SHOW SPRIG RECOMMENDATIONS` | Recommendations awaiting a decision | [EXPLAIN and workload](explain-and-workload.md#show-sprig-recommendations) |
| `SHOW POLICIES`, `SHOW CREATE POLICY` | Row-level security policies | [Row-level security](row-level-security.md#show-policies) |
| `SHOW SEARCH INDEXES` | Search indexes and their fields | [Search and vectors](search-and-vectors.md#show-search-indexes) |
| `SHOW DEPENDENTS OF FUNCTION \| PROCEDURE` | What a `DROP` would affect | [Logic](logic.md#show-dependents) |
| `SHOW STREAMS` | Streams and change topics | [Streams, queues and sinks](streams-queues-sinks.md#show-streams) |
| `SHOW CONNECTIONS`, `SHOW CREATE CONNECTION`, `SHOW PLATFORM CONNECTIONS` | Sink connections | [Streams, queues and sinks](streams-queues-sinks.md#show-connections) |
| `SHOW SINKS`, `SHOW CREATE SINK`, `SHOW SINK STATUS` | Sinks and their tasks | [Streams, queues and sinks](streams-queues-sinks.md#show-sinks-and-show-sink-status) |
| `SHOW INHERITED DEFINITIONS \| INSTANCES \| OVERRIDES \| BROADCASTS` | `FOR EACH TENANT` objects | [Streams, queues and sinks](streams-queues-sinks.md#show-inherited) |
| `SHOW SPACES`, `SHOW SHARES`, `SHOW CREATE SHARE`, `SHOW INHERITABLE TABLES` | Data inheritance | [Inheritance](inheritance.md#show-shares-show-create-share-and-show-inheritable-tables) |

## SprigDB's information_schema views

| View | Columns | Reference |
|---|---|---|
| `information_schema.SPRIG_SETTINGS` | `SETTING_SCOPE`, `TABLE_SCHEMA`, `TABLE_NAME`, `SETTING_NAME`, `VALUE`, `SOURCE_SCOPE`, `BOUNDS`, `CLAMPED` | [Settings](settings.md#information_schemasprig_settings) |
| `information_schema.SPRIG_ROW_POLICIES` | `TABLE_SCHEMA`, `TABLE_NAME`, `POLICY_NAME`, `POLICY_MODE`, `COMMANDS`, `ROLES`, `USING_EXPR`, `CHECK_EXPR`, `RLS_ENABLED`, `RLS_FORCED` | [Row-level security](row-level-security.md#information_schemasprig_row_policies) |

Both are built per query from the tenant's catalog. They can't be used inside a view or stored program (39051). `SPRIG_SETTINGS` over 10,000 rows fails with 39052.

```sql test
SELECT COUNT(*) > 0 AS has_rows FROM information_schema.SPRIG_SETTINGS WHERE SETTING_SCOPE = 'tenant';
```

```text output
has_rows
1
```

## performance_schema and sys on port 3306

MySQL tools read `performance_schema` and `sys`. On the MySQL persona, SprigDB answers them for your tenant with MySQL 8.4's columns:

| Tables | Data |
|---|---|
| `session_variables`, `global_variables`, `variables_by_thread`, `variables_info` | MariaDB's variables, names in lower case, with the persona's own values written in |
| `session_status`, `global_status`, `status_by_thread` | Your session's counters; the server's global-only values; `Threads_connected` is your tenant's |
| `threads`, `processlist`, `sys.session`, `sys.processlist` (and `x$` forms) | Your tenant's sessions |
| `session_connect_attrs`, `session_account_connect_attrs` | Your connection's attributes |
| `events_statements_summary_by_digest`, `sys.statement_analysis`, `sys.statements_with_errors_or_warnings` (and `x$` forms) | Your tenant's statement fingerprints; see [EXPLAIN and workload](explain-and-workload.md#workload-fingerprints) |
| `sys.schema_object_overview`, `sys.schema_auto_increment_columns` | From your catalog |
| `sys.version`, `sys.memory_global_total`, `sys.metrics` clock rows | Constants |
| Every other table and view of MySQL 8.4 | Empty, with MySQL's columns |

A name MySQL 8.4 doesn't have fails with 1146. `USE performance_schema` and `USE sys` work on 3306. The functions `FORMAT_BYTES()`, `FORMAT_PICO_TIME()`, `PS_CURRENT_THREAD_ID()`, `PS_THREAD_ID()`, and `sys.format_bytes()`, `sys.format_time()`, `sys.format_statement()`, `sys.quote_identifier()`, `sys.version_major()`, `sys.version_minor()`, `sys.version_patch()`, `sys.ps_thread_id()` and `sys.sys_get_config()` work as in MySQL.

On the MariaDB persona (3307), a tenant has no access to MariaDB's own `performance_schema` and `sys`.

## System variables

The persona answers the variables that tell a client which server it talks to:

| Variable | MySQL persona (3306) | MariaDB persona (3307) |
|---|---|---|
| `version`, `VERSION()` | `gateway.mysql_server_version` | `gateway.mariadb_server_version` |
| `version_comment` | `SprigDB` and the release | The same |
| `gtid_mode`, `enforce_gtid_consistency` | `OFF` | Not present, as in MariaDB |
| `sql_mode` | Your MySQL 8.4 `sql_mode`, as you set it; `GLOBAL` shows MySQL 8.4's default | MariaDB's |
| `max_execution_time` | Your session's statement timeout, in milliseconds | Not present |
| `terminology_use_previous` | `NONE` | Not present |
| `max_allowed_packet` | The session's packet limit | The same |

Version strings never contain "MariaDB" on 3306 and always do on 3307, because ORMs and drivers choose dialects by that word. MySQL-only session variables that `mysqldump` and Connector/J set (`information_schema_stats_expiry`, `terminology_use_previous`, `session_track_gtids`) are accepted on 3306 and change nothing.

## Functions SprigDB adds

| Function | Where | Reference |
|---|---|---|
| `sprig_search('index', 'query' [, options])` | Table function in `FROM` | [Search and vectors](search-and-vectors.md#sprig_search) |
| `SPRIG_ORIGIN()` | Select list, on inheritable tables | [Inheritance](inheritance.md#sprig_origin) |
| `SPRIG_SUBTREE`, `SPRIG_DESCENDANTS`, `SPRIG_ANCESTORS`, `SPRIG_DEPTH`, `SPRIG_PATH` | Hierarchy queries | [Inheritance](inheritance.md#hierarchy-functions) |
| `CURRENT_USER`, `SPRIG_USER_ID()`, `SPRIG_TENANT_ID()`, `SPRIG_HOME_TENANT_ID()`, `SPRIG_TENANT_IN_SUBTREE()`, `SPRIG_TENANT_MANAGED()`, `SPRIG_USER_ATTR()`, `SPRIG_CLAIM()`, `SPRIG_SESSION_SPACE()` | Row-level security policy expressions | [Row-level security](row-level-security.md#policy-expressions) |
| `CALL sprig.commit_offset(…)` | Stream offsets | [Streams, queues and sinks](streams-queues-sinks.md#consumer-groups-and-offsets) |
| `CALL sprig.mq_publish(…)` and the other `sprig.mq_*` calls | Queues | [Streams, queues and sinks](streams-queues-sinks.md#queues-and-exchanges) |

`DATABASE()`, `SCHEMA()`, `CURRENT_USER()`, `USER()` and query attributes return logical values: your database name, and your account without SprigDB's backend account names.

## Errors

| Code | Name | When |
|---|---|---|
| 1054 | (`42S22`) | A reference to an internal `_sprig_` column |
| 1142 | (`42000`) | A server-wide `information_schema` table |
| 1146 | (`42S02`) | An internal object, or a `performance_schema` table MySQL doesn't have |
| 1227 | (`42000`) | A server-wide `SHOW` |
| 39050 | `ER_SPRIG_RESERVED_NAME` | Creating a reserved name; naming `@sprig_rls` |
| 39051 | `ER_SPRIG_NAME_REWRITE_UNSUPPORTED` | A name position SprigDB can't map; `information_schema` or a SprigDB view inside a stored object; a bad `@sprig_show_auto_indexes` |
| 39052 | `ER_SPRIG_METADATA_TOO_LARGE` | A metadata answer over its row limit |

## Related

- [MySQL compatibility](overview.md)
- [Settings in SQL](settings.md)
- [Automatic indexing](../performance/automatic-indexing.md)
- [EXPLAIN and workload](explain-and-workload.md)
