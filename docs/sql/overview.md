---
title: SQL overview and MySQL compatibility
audience: [user, dba]
sidebar:
  order: 1
---

# SQL overview and MySQL compatibility

SprigDB speaks MySQL 8.4 on port 3306 and MariaDB on port 3307, and runs your SQL on MariaDB. Most applications, drivers, ORMs and tools work unchanged. This page says what works as on MySQL or MariaDB, what differs and why, and lists every statement SprigDB adds, with a link to its reference page.

## Two personas

| Port | Persona | Use it for |
|---|---|---|
| 3306 | MySQL 8.4 | MySQL drivers, ORMs and tools. SprigDB makes MariaDB answer as MySQL 8.4 does |
| 3307 | MariaDB | MariaDB drivers and tools; MariaDB's own SQL, unchanged |

Both see the same data. On 3306, SprigDB translates where MySQL and MariaDB differ:

- **Server identity.** `VERSION()`, `@@version` and `@@version_comment` name MySQL on 3306 and MariaDB on 3307, so ORMs pick the right dialect. See [System variables](information-schema.md#system-variables).
- **`sql_mode`.** MySQL 8.4's default and its modes, including `ONLY_FULL_GROUP_BY` with MySQL's rules for `GROUP BY` and `ORDER BY`.
- **Queries.** `WITH ROLLUP` with `ORDER BY` and `GROUPING()`, `LATERAL` derived tables, MySQL-only functions, `CAST(… AS JSON)`, MySQL-only executable comments, and `max_execution_time`.
- **DDL.** Functional key parts typed from the table's columns, MySQL's answers about long names and descending index parts, and `SHOW CREATE TABLE` in MySQL's spelling.
- **Spatial.** SRID 4326 with MySQL's axis order, and `ORDER BY ST_Distance(…) LIMIT k` as a fast nearest-neighbour search.
- **Metadata.** `information_schema` with MySQL 8.4's columns, `performance_schema` and `sys` for your tenant, and `EXPLAIN` with MySQL's columns and tree format. See [information_schema and SHOW](information-schema.md) and [EXPLAIN and workload](explain-and-workload.md).
- **Errors.** MySQL's error numbers and wording where they differ from MariaDB's, such as a duplicate key naming `table.PRIMARY`.

The plans are always MariaDB's optimizer's.

## What works unchanged

Within one tenant, these behave as on a single MySQL or MariaDB server:

- `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, upserts, joins, subqueries, CTEs (including recursive ones), window functions, set operations and JSON functions;
- transactions, savepoints, `SELECT … FOR UPDATE`, locking reads and MariaDB's isolation levels;
- prepared statements (text and binary protocol), cursors, multi-statements, `LOAD DATA LOCAL INFILE` (when the client allows it and `sql.local_infile` is on), long data, bulk execute on 3307;
- `CREATE`, `ALTER`, `DROP` and `RENAME` of databases, tables, views, indexes, sequences, procedures, functions, triggers and events; `TRUNCATE`; temporary tables;
- users, roles and grants, with MySQL's statements (see [Tenants and accounts](tenants-and-accounts.md));
- `GET_LOCK()` and the other user-level locks, scoped to your tenant;
- `FLUSH TABLES` and `FLUSH PRIVILEGES` (accepted; there is nothing to flush).

While your tables are unsharded, which is how every tenant starts, every statement runs on one MariaDB server with exactly MariaDB's semantics. The differences below appear only where SprigDB has to translate names or run a statement over several shards.

## What differs

### Names are translated

You name databases and tables as usual (`shop.orders`); MariaDB stores them under physical schema names (`t42_d1_g3_s1`). SprigDB translates schema names only, never table or column names, in every statement, result, `SHOW` output and error message. `DATABASE()` returns `shop`, `CURRENT_USER()` returns your own account, and you never see another tenant's names.

A qualified name in a position SprigDB can't translate fails with 39051 `ER_SPRIG_NAME_REWRITE_UNSUPPORTED` rather than being sent unchanged. An unknown database fails with 1049. Tenant isolation doesn't depend on the rewrite: each tenant's MariaDB accounts are granted only its own schemas.

### Reserved names

| Name | Reserved for | Error when you create one |
|---|---|---|
| Objects starting with `__sprig_` | SprigDB's internal tables, views, routines and triggers | 39050 `ER_SPRIG_RESERVED_NAME` |
| Columns starting with `_sprig_` | SprigDB's internal columns | 39050 |
| Indexes starting with `sprig_auto_` | Automatic indexes | 39050 |
| Databases `t<digits>_…`, `information_schema`, `performance_schema`, `sys`, `mysql` | Physical and system schemas | 39050 |
| User variables starting with `@sprig_` | Session controls the gateway keeps | 39050 or 39051 |
| The `sprig.` routine prefix | SprigDB's calls, such as `sprig.mq_publish` | |

Internal objects are hidden from metadata and fail as missing (1146, 1054) if you name them. See [information_schema and SHOW](information-schema.md#what-is-hidden).

### AUTO_INCREMENT across shards

On a sharded table, `AUTO_INCREMENT` values come from per-table ID blocks that all gateways share. They are unique across shards and follow `auto_increment_increment` and `auto_increment_offset`, but they aren't gap-free and aren't ordered across gateways. `LAST_INSERT_ID()` returns the statement's first value. See [Sharding](sharding.md#auto_increment).

### Foreign keys across table groups

A foreign key between tables on the same shard stays MariaDB's. A foreign key between table groups is kept by SprigDB: existing rows are checked when you add it (1452), and later writes are checked by the gateway. Actions it can't run across shards fail with 39462 `ER_SPRIG_FK_ACTION_NOT_DISTRIBUTED`. See [Sharding](sharding.md#foreign-keys-across-table-groups).

### Statements that can't be distributed

A statement on sharded tables runs on one shard when its conditions pin the distribution key, and otherwise on every shard, with the gateway merging, sorting and aggregating the results. A statement SprigDB can't run with exactly MariaDB's result fails with 39420 `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE`, never with a wrong answer. The message starts with a reason code, such as `window_function` or `key_change`. The reasons and the fixes are in [Sharding](sharding.md#how-statements-behave-on-sharded-tables); `EXPLAIN FORMAT=SPRIG` shows how a statement is distributed.

### Transactions across shards

| Transaction | Commit |
|---|---|
| Writes on one cluster | MariaDB's own commit, exactly as on one server |
| Writes on several clusters | XA two-phase commit: every other cluster prepares, the last commits with SprigDB's decision record, then the prepared branches commit. A crash at any point is finished by SprigDB's resolver |
| A cluster without XA | Decided by `txn.non_xa_policy`: `reject` (default) refuses the write that would add it, with 39440 `ER_SPRIG_CROSS_CLUSTER_WRITE_REJECTED`, before it runs; `best_effort` commits each cluster in turn (39442 `ER_SPRIG_PARTIAL_COMMIT` if only some commit); `sprig_2pc` uses SprigDB's own two-phase commit |

A transaction may span at most `txn.max_branches` clusters (default 32; 39444). Each cluster prepares within `txn.prepare_timeout_ms`, and lock waits with two branches are capped by `txn.distributed_lock_wait_timeout_seconds`. If the outcome of a distributed commit can't be known, the client gets 39441 `ER_SPRIG_COMMIT_OUTCOME_UNKNOWN` and the connection closes; 39443 `ER_SPRIG_DTX_ROLLED_BACK` means the transaction was rolled back and can be retried.

**Isolation.** Each shard gives MariaDB's isolation (`REPEATABLE READ` by default) and locking. A read that spans several shards reads each shard's own snapshot: shards aren't read at one common point in time. Keep data you must read consistently in one table group, which the sharding advisor does for tables that are used together.

**Reads after writes.** A read after your write may go to a replica only once it has applied your write, so you always read your own writes.

### DDL

Schema changes go through SprigDB's catalog, so every gateway sees a new definition at the same moment:

- On a sharded table, a change runs on every shard, all or nothing: a shard that refuses it rolls back the shards already changed. A change that removed data and then fails on a shard is finished later by SprigDB (39480 `ER_SPRIG_DDL_FAILED`). If the shards already disagree on the table's definition, the change is refused with 39483 `ER_SPRIG_DDL_SHARD_DRIFT`.
- One statement may not change tables of different table groups, or a sharded table and another database (39481 `ER_SPRIG_DDL_UNSUPPORTED`): run it once per table.
- DDL inside a transaction commits it first, as in MySQL.
- Stored programs always get your own account as `DEFINER`; see [Definers](tenants-and-accounts.md#definers).
- A DDL statement of a shape the catalog can't handle fails with 39051.

### Accounts

Accounts belong to your tenant and are named without the tenant prefix inside SQL; you log in as `tenant.user`. Server-wide privileges (`SUPER`, `FILE`, replication) are dropped with a warning, and server-wide `SHOW` statements (`SHOW ENGINE`, `SHOW BINARY LOGS`, replica status) fail with 1227. See [Tenants and accounts](tenants-and-accounts.md).

## Statements SprigDB adds

| Statement | Purpose | Reference |
|---|---|---|
| **Settings** | | |
| `ALTER TABLE … SPRIG SET \| RESET (…)` | Table settings | [Settings](settings.md#alter-table--sprig-set) |
| `ALTER DATABASE … SPRIG SET \| RESET (…)` | Database settings | [Settings](settings.md#alter-database--sprig-set) |
| `ALTER TENANT [name] SPRIG SET \| RESET (…)`, `SPRIG SET`, `SPRIG RESET` | Tenant and descendant settings | [Settings](settings.md#alter-tenant--sprig-set) |
| `ALTER … SPRIG SET \| RESET BOUNDS (…)` | Bounds and locks | [Settings](settings.md#bounds) |
| `SHOW SPRIG SETTINGS` | Effective settings and their sources | [Settings](settings.md#show-sprig-settings) |
| **Accounts** | | |
| `GRANT \| REVOKE SPRIG_TENANT_ADMIN, SPRIG_SETTINGS_ADMIN, SPRIG_SINK_ADMIN, SPRIG_FAN_IN, BYPASS RLS, WATCH` | SprigDB's privileges | [Tenants and accounts](tenants-and-accounts.md#sprigdb-privileges) |
| `GRANT \| REVOKE … ON GRAPH g NODES \| RELATIONSHIPS …` | Graph privileges | [Tenants and accounts](tenants-and-accounts.md#graph-privileges) |
| `GRANT \| REVOKE USAGE ON [PLATFORM] CONNECTION` | Use of a sink connection | [Streams, queues and sinks](streams-queues-sinks.md#grant-and-revoke-usage-on-connection) |
| **Sharding** | | |
| `SPRIG PIN`, `SPRIG UNPIN` | Fix a table's place in the sharding design | [Sharding](sharding.md#sprig-pin) |
| `SPRIG SHOW PINS`, `SPRIG SHOW ADVICE` | Pins; the advisor's design | [Sharding](sharding.md#sprig-show-advice) |
| **Row-level security** | | |
| `CREATE \| ALTER \| DROP POLICY` | Row policies | [Row-level security](row-level-security.md#create-policy) |
| `ALTER TABLE … ENABLE \| DISABLE \| FORCE \| NO FORCE ROW LEVEL SECURITY`, `… SPRIG ROW LEVEL SECURITY ON \| OFF` | Switch row-level security | [Row-level security](row-level-security.md#enable-disable-and-force-row-level-security) |
| `SHOW POLICIES`, `SHOW CREATE POLICY` | List policies | [Row-level security](row-level-security.md#show-policies) |
| **Search and vectors** | | |
| `CREATE \| ALTER \| DROP SEARCH INDEX` | Full-text search indexes | [Search and vectors](search-and-vectors.md#create-search-index) |
| `CREATE \| DROP SEARCH ANALYZER` | Custom analyzers | [Search and vectors](search-and-vectors.md#create-and-drop-search-analyzer) |
| `SHOW SEARCH INDEXES` | List search indexes | [Search and vectors](search-and-vectors.md#show-search-indexes) |
| `MATCH … AGAINST` on search indexes, `sprig_search()` | Full-text queries | [Search and vectors](search-and-vectors.md#full-text-queries) |
| **Logic** | | |
| `CREATE [OR REPLACE] FUNCTION \| PROCEDURE … LANGUAGE kirun \| javascript \| lua` | Code in SprigDB's runtimes | [Logic](logic.md#create-function-and-create-procedure) |
| `CREATE VALIDATION` | Row checks before writes | [Logic](logic.md#create-validation) |
| `CREATE TRIGGER … AFTER COMMIT ON` | Code after commit | [Logic](logic.md#create-trigger--after-commit) |
| `CREATE TRIGGER … ON STREAM \| ON QUEUE` | Code per record or message | [Logic](logic.md#create-trigger--on-stream-and-on-queue) |
| `CREATE SCHEDULE` | Cron schedules | [Logic](logic.md#create-schedule) |
| `ALTER TABLE … ADD c type AS (f(…))` | Computed columns from logic functions | [Logic](logic.md#computed-columns) |
| `TEST FUNCTION \| PROCEDURE \| TRIGGER \| VALIDATION` | Sandboxed test runs | [Logic](logic.md#test) |
| `ALTER FUNCTION \| PROCEDURE … ROLLOUT TO \| PROMOTE \| ABORT` | Staged rollouts | [Logic](logic.md#staged-rollouts) |
| `ALTER FUNCTION \| PROCEDURE … ROLLBACK [TO VERSION n]` | Earlier versions | [Logic](logic.md#alter--rollback) |
| `DROP FUNCTION \| PROCEDURE \| VALIDATION \| TRIGGER \| SCHEDULE … [CASCADE]` | Remove code | [Logic](logic.md#drop) |
| `SHOW DEPENDENTS OF FUNCTION \| PROCEDURE` | What a drop would affect | [Logic](logic.md#show-dependents) |
| **Streams, queues and sinks** | | |
| `CREATE \| ALTER \| DROP STREAM`, `SHOW STREAMS` | Event streams | [Streams, queues and sinks](streams-queues-sinks.md#streams) |
| `INSERT INTO stream`, `SELECT … FROM stream` | Publish and read | [Streams, queues and sinks](streams-queues-sinks.md#publishing-with-insert) |
| `CALL sprig.commit_offset(…)` | Commit a consumer offset in your transaction | [Streams, queues and sinks](streams-queues-sinks.md#consumer-groups-and-offsets) |
| `CALL sprig.mq_publish \| mq_get \| mq_ack \| mq_nack \| mq_info \| mq_browse \| mq_purge \| mq_republish \| mq_release` | Queues | [Streams, queues and sinks](streams-queues-sinks.md#queues-and-exchanges) |
| `CREATE \| ALTER \| DROP CONNECTION`, `SHOW CONNECTIONS`, `SHOW CREATE CONNECTION`, `SHOW PLATFORM CONNECTIONS`, `TEST [CREATE] CONNECTION` | Sink destinations | [Streams, queues and sinks](streams-queues-sinks.md#connections) |
| `CREATE \| ALTER \| DROP SINK`, `ALTER SINK … PAUSE \| RESUME`, `SHOW SINKS`, `SHOW CREATE SINK`, `SHOW SINK STATUS`, `TEST [CREATE] SINK` | Sinks | [Streams, queues and sinks](streams-queues-sinks.md#sinks) |
| `… FOR EACH TENANT`, `INSERT … FOR TENANT \| FOR ALL TENANTS \| IN TENANT`, `CREATE EXCHANGE \| QUEUE … FOR EACH TENANT`, `BIND \| UNBIND`, `ALTER INHERITED …`, `SHOW INHERITED …` | Messaging objects for every member tenant | [Streams, queues and sinks](streams-queues-sinks.md#for-each-tenant) |
| **Inheritance** | | |
| `CREATE \| DROP SPACE`, `ALTER SPACE … SET HOME`, `USE SPACE`, `SHOW SPACES` | Spaces | [Inheritance](inheritance.md#spaces-and-layers) |
| `ALTER TABLE … SPRIG INHERITABLE KEY (…)`, `… DROP SPRIG INHERITABLE`, `SHOW INHERITABLE TABLES` | Inheritable tables | [Inheritance](inheritance.md#alter-table--sprig-inheritable) |
| `CREATE \| ALTER \| DROP SHARE`, `SHOW SHARES`, `SHOW CREATE SHARE` | Shares | [Inheritance](inheritance.md#create-share) |
| `SPRIG RESET`, `SPRIG CHECK IDS`, `SPRIG CHECK REFERENCES`, `SPRIG_ORIGIN()` | Overrides and checks | [Inheritance](inheritance.md#sprig-reset) |
| `CREATE TABLE … INHERITS (…)`, `ONLY`, `@sprig_inheritance`, `ADD \| DROP DISCRIMINATOR` | Typed tables | [Inheritance](inheritance.md#typed-tables) |
| `ALTER TABLE … ADD \| DROP HIERARCHY`, `SPRIG_SUBTREE()` and the other hierarchy functions | Record hierarchies | [Inheritance](inheritance.md#record-hierarchies) |
| **Graphs** | | |
| `CREATE \| DROP GRAPH`, `CREATE [OR REPLACE] PROPERTY GRAPH`, `GRAPH_TABLE (…)` | Graphs in SQL | [Graphs in SQL](#graphs-in-sql) below |
| **Metadata, plans and workload** | | |
| `SET @sprig_show_auto_indexes`, `SET @sprig_show_internal` | Show hidden indexes and objects | [information_schema and SHOW](information-schema.md#what-is-hidden) |
| `information_schema.SPRIG_SETTINGS`, `information_schema.SPRIG_ROW_POLICIES` | SprigDB's views | [information_schema and SHOW](information-schema.md#sprigdbs-information_schema-views) |
| `EXPLAIN FORMAT=SPRIG` | Distributed plans | [EXPLAIN and workload](explain-and-workload.md#explain-formatsprig) |
| `SHOW SPRIG RECOMMENDATIONS`, `SPRIG APPROVE \| IGNORE \| SNOOZE RECOMMENDATION` | Decide on recommendations | [EXPLAIN and workload](explain-and-workload.md#recommendations) |

## Graphs in SQL

Graphs are queried with Cypher over Bolt, or with GQL over HTTP; see [Graphs](../data-models/graphs.md). In SQL you can create and drop them, map existing tables as a graph, and query a graph inside a `SELECT`.

### CREATE GRAPH and DROP GRAPH

```sql
CREATE GRAPH [IF NOT EXISTS] [db_name.]graph_name
DROP GRAPH [IF EXISTS] [db_name.]graph_name
```

`CREATE GRAPH` makes a native graph, whose nodes and relationships SprigDB stores. **Privileges:** `CREATE` on the database to create; `DROP` on the graph or its database to drop. `ALTER GRAPH` isn't supported (39901).

```sql test
CREATE GRAPH IF NOT EXISTS social;
DROP GRAPH social;
```

### CREATE PROPERTY GRAPH

```sql
CREATE [OR REPLACE] PROPERTY GRAPH [IF NOT EXISTS] [db_name.]graph_name
    VERTEX TABLES (
        table_name [AS alias] KEY (column)
            [LABEL label]
            [PROPERTIES (expression [AS name], ...) | PROPERTIES ARE ALL COLUMNS | NO PROPERTIES]
        [, ...]
    )
    [EDGE TABLES (
        table_name [AS alias] KEY (column)
            SOURCE KEY (column) REFERENCES vertex (column)
            DESTINATION KEY (column) REFERENCES vertex (column)
            [LABEL label]
            [PROPERTIES (...) | PROPERTIES ARE ALL COLUMNS | NO PROPERTIES]
        [, ...]
    )]
```

A **mapped graph**: SQL/PGQ's way to read existing tables as a graph, without copying. Each vertex and edge table needs one integer key (below 2^48); labels default to the table name and are unique in the graph. The tables must be unsharded (39908). A mapped graph is read-only: writes through it fail with 39904. **Privileges:** `CREATE` on the database and `SELECT` on every table it maps.

```sql test
CREATE TABLE people (id BIGINT PRIMARY KEY, name VARCHAR(100));
CREATE TABLE follows (id BIGINT PRIMARY KEY, src BIGINT NOT NULL, dst BIGINT NOT NULL);
INSERT INTO people VALUES (1, 'Ada'), (2, 'Lin'), (3, 'Sam');
INSERT INTO follows VALUES (10, 1, 2), (11, 2, 3);

CREATE PROPERTY GRAPH net
  VERTEX TABLES (people KEY (id) LABEL Person PROPERTIES ARE ALL COLUMNS)
  EDGE TABLES (follows KEY (id)
               SOURCE KEY (src) REFERENCES people (id)
               DESTINATION KEY (dst) REFERENCES people (id)
               LABEL FOLLOWS NO PROPERTIES);
```

### GRAPH_TABLE

```sql
SELECT ... FROM GRAPH_TABLE (graph_name
    MATCH pattern
    [WHERE condition]
    COLUMNS (expression AS name [, ...])
) [AS alias] ...
```

Runs a graph pattern and returns its matches as a table you can join, filter and aggregate. In a single `SELECT`, SprigDB plans the graph and the rest of the statement together (and `EXPLAIN FORMAT=SPRIG` shows both); in other statements, the graph read runs first and its rows are inlined. Privileges on the graph's labels and relationship types apply.

```sql test
SELECT g.follower, g.followed
FROM GRAPH_TABLE (net
  MATCH (a:Person)-[:FOLLOWS]->(b:Person)
  COLUMNS (a.name AS follower, b.name AS followed)
) AS g
ORDER BY g.follower;
```

```text output
follower	followed
Ada	Lin
Lin	Sam
```

Graph errors are 39900 to 39909: `ER_SPRIG_GRAPH_SYNTAX`, `…_UNSUPPORTED`, `…_NOT_FOUND`, `…_BUDGET_EXCEEDED`, `…_READ_ONLY_SOURCE`, `…_ID_NOT_ROUTABLE`, `…_NODE_HAS_RELATIONSHIPS`, `…_DETACH_TOO_LARGE`, `…_DEFINITION_INVALID` and `…_PROPERTY_TYPE`. See the [error reference](../reference/errors.md).

## Not supported in SQL

- Creating, dropping, listing or switching tenants (`CREATE TENANT`, `SHOW TENANTS`, `USE TENANT`): use [sprigctl](../tools/sprigctl.md), the Admin API or Studio.
- Declaring shard keys, table groups or reference tables in `CREATE TABLE`, and `SHOW SHARDS` or `SHOW TABLE GROUPS`: use pins, the advisor and `sprigctl shard`. See [Sharding](sharding.md).
- Reference tables copied to every cluster: a `REFERENCE` pin leaves the table unsharded, on one cluster.
- Splitting, merging or moving shards in SQL: `sprigctl shard split | merge | rekey`.
- `RENAME USER`, `GRANT PROXY`, password hashes in `IDENTIFIED BY PASSWORD`, resource limits on accounts (1235).
- `CREATE TYPE` (39710).
- Cluster, data-centre and global settings in SQL: `sprigctl settings`.

## Related

- [How SprigDB works](../concepts/overview.md)
- [MySQL and MariaDB clients](../connect/mysql-and-mariadb.md)
- [Shards and table groups](../concepts/shards-and-table-groups.md)
- [Tenants and databases](../concepts/tenants-and-databases.md)
- [Error codes](../reference/errors.md)
