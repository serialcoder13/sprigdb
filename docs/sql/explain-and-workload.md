---
title: EXPLAIN and workload
audience: [user, dba]
sidebar:
  order: 11
---

# EXPLAIN and workload

This page covers how to see what SprigDB does with a statement and how it learns from your workload: `EXPLAIN` in each persona's format, `EXPLAIN FORMAT=SPRIG` for plans across shards, the workload fingerprints SprigDB records for every statement, and the recommendations it makes from them. For tuning, see [Query plans](../performance/query-plans.md) and [Workload](../performance/workload.md).

## EXPLAIN

```sql
{EXPLAIN | DESCRIBE | DESC} [FORMAT = {TRADITIONAL | JSON | TREE | SPRIG}] statement
EXPLAIN ANALYZE [FORMAT = TREE] statement
ANALYZE [FORMAT = JSON] statement            -- MariaDB persona
EXPLAIN table_name                           -- same as DESCRIBE table_name
```

The plans are MariaDB's optimizer's. SprigDB shapes them like the persona's server, and runs `EXPLAIN` of a statement through the same path as the statement: names are mapped, row-level security policies are added, and searches are resolved. So the plan you see is the plan of what really runs.

| Form | MySQL persona (3306) | MariaDB persona (3307) |
|---|---|---|
| `EXPLAIN` (traditional) | MySQL 8.4's twelve columns: `id`, `select_type`, `table`, `partitions` (always `NULL`), `type`, `possible_keys`, `key`, `key_len`, `ref`, `rows`, `filtered`, `Extra`. It runs as MariaDB's `EXPLAIN EXTENDED` | MariaDB's ten columns |
| `EXPLAIN FORMAT=JSON` | MariaDB's JSON (`query_block` with MariaDB's members) | The same |
| `EXPLAIN FORMAT=TREE` | MySQL's tree: one `EXPLAIN` column, one line per iterator (`-> Table scan on t`, `-> Filter: (…)`, `-> Nested loop inner join`, `-> Sort: …`, `-> Limit: n row(s)`), indented four spaces per level | Not available, as in MariaDB |
| `EXPLAIN ANALYZE` | Runs the statement (as MariaDB's `ANALYZE FORMAT=JSON`) and prints the tree with `actual time`, `rows` and `loops` | Use `ANALYZE` |
| `EXPLAIN FORMAT=SPRIG` | SprigDB's distributed plan; see below | The same |

In tree output, `cost` is MariaDB's cost unit, not MySQL's, and `rows` is MariaDB's estimate. MariaDB's plans name no aggregation, so MySQL's `Group aggregate` lines don't appear.

```sql test
CREATE TABLE customers (id BIGINT PRIMARY KEY, name VARCHAR(100), country CHAR(2));
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, total DECIMAL(10,2), KEY (customer_id));
INSERT INTO customers VALUES (1, 'Ada', 'GB'), (2, 'Lin', 'SG');
INSERT INTO orders VALUES (10, 1, 25.00), (11, 1, 40.00), (12, 2, 15.00);

EXPLAIN SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.total > 20;
EXPLAIN FORMAT=JSON SELECT * FROM orders WHERE customer_id = 1;
```

```sql test
EXPLAIN FORMAT=TREE SELECT * FROM orders WHERE customer_id = 1 ORDER BY total DESC LIMIT 1;
EXPLAIN ANALYZE SELECT COUNT(*) FROM orders WHERE total > 20;
```

`FORMAT=JSON` and `ANALYZE` are passed to MariaDB as you wrote them, on both ports and on sharded tables. `EXPLAIN ANALYZE` runs the statement, so on a sharded table it accepts reads only: `EXPLAIN ANALYZE` of an `INSERT`, `UPDATE` or `DELETE` there fails with 39420 `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE`.

### EXPLAIN of searches

`EXPLAIN` of a statement with `MATCH … AGAINST` or `sprig_search()` returns MariaDB's plan of the rewritten statement, then one `SEARCH` row per search: the index, its partitions, the consistency, the parsed query, and the hit count and time. In `FORMAT=JSON` or `TREE`, the searches are in one `{"sprig_search_scans": [...]}` row. The hit count and time are left out when a row-level security policy binds you on the indexed object. See [Search and vectors](search-and-vectors.md#explain-of-searches).

## EXPLAIN FORMAT=SPRIG

```sql
EXPLAIN FORMAT=SPRIG statement
```

Shows how SprigDB's router runs a statement on sharded tables: which shards it goes to, and what the gateway does with the rows. A statement that names no sharded table isn't routed and goes to MariaDB unchanged. A statement the router can't run exactly fails here with 39420 `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE`, as it would when run; see [Sharding](sharding.md#how-statements-behave-on-sharded-tables).

The result has one row per step, as a tree:

| Column | Meaning |
|---|---|
| `id` | The step |
| `parent` | The step it feeds, or `NULL` for the top |
| `operator` | What happens |
| `target` | Where: `gateway`, `group <n>: <k> shards`, or `shard <n>` |
| `detail` | The SQL sent to each shard, or the step's parameters |

| Operator | Meaning |
|---|---|
| `Route` | Run SQL on shards: one (`shard <n>`) or every shard of a group in parallel. `[LIMIT n per shard]` when a limit is pushed down |
| `MergeSort` | Merge the shards' sorted rows in the gateway, on the `ORDER BY` keys |
| `Aggregate` | Combine the shards' partial aggregates: group keys, aggregates and `HAVING` |
| `Limit` | Apply `LIMIT` and `OFFSET` in the gateway |
| `LookupJoin` | Join two tables of different groups in the gateway: read one side, then look the other up in batches (`router.lookup_batch_size`) |
| `Lookup` | Find the shards of keys through a lookup index, or ask every shard |
| `Gather` | Read rows from the shards into the gateway and finish the statement there |
| `Write` | A write over several shards, in one transaction |
| `ParentRoute` | Rows of a child table that follow their parent's key to the parent's shards |
| `IdBlocks` | `AUTO_INCREMENT` values taken from the gateway's ID blocks |

`GRAPH_TABLE` queries show their graph steps in the same table.

```sql
EXPLAIN FORMAT=SPRIG
SELECT c.country, SUM(o.total)
FROM shop.orders o JOIN shop.customers c ON c.id = o.customer_id
GROUP BY c.country
ORDER BY 2 DESC
LIMIT 10;
```

```text
id  parent  operator   target              detail
1   NULL    Limit      gateway             offset 0, count 10
2   1       MergeSort  gateway             1 keys (weights, PAD SPACE)
3   2       Aggregate  gateway             1 group keys, 1 aggregates
4   3       Route      group 3: 4 shards   SELECT c.country, SUM(o.total) … FROM t42_d1_g3_s<n>.orders …
```

## Workload fingerprints

SprigDB records every statement it runs, per tenant, as a **fingerprint**: the statement's normalized text and an id. Automatic indexing, the sharding advisor and the recommendations all learn from these.

Normalization:

- comments are dropped; executable comments the persona runs are kept, normalized; optimizer hints are kept;
- literals and placeholders become `?`, and a sign before a number is folded into it;
- keywords and function names are upper case; columns lower case; table names are qualified with the logical database (`shop.orders`);
- `IN (?, ?, …)` becomes `IN (...)`, and `VALUES (…), (…)` becomes `VALUES (...)`;
- the text is cut at `workload.max_text_bytes` (4096).

The id hashes the normalized text with the database of the first table the statement names, so a statement that qualifies its tables (`shop.orders`) has one fingerprint whatever the session's default database is.

For each fingerprint SprigDB keeps hourly statistics (`workload.bucket_minutes`) for `workload.retention_days` (30): executions, errors, a latency histogram, rows sent and, for sampled executions, rows examined. Fingerprints whose p95 latency passes `workload.watch_p95_ms` are **watched**: SprigDB probes some of their executions (`workload.examined_sample_rate`) and keeps sealed parameter samples (`workload.sample_rate`, `workload.capture_samples`). A tenant keeps at most `workload.max_fingerprints_per_tenant` (10,000); the rest count as "other". See the [settings reference](../reference/settings.md#workload).

### Reading fingerprints in SQL

On the MySQL persona, `performance_schema.events_statements_summary_by_digest` holds one row per fingerprint of your tenant, with MySQL 8.4's columns:

| Column | Value |
|---|---|
| `DIGEST` | The fingerprint's id, 16 hex digits (MySQL's is 64) |
| `DIGEST_TEXT` | The normalized text |
| `COUNT_STAR`, `SUM_ERRORS`, `FIRST_SEEN`, `LAST_SEEN` | Counts and times |
| `SUM_TIMER_WAIT`, `MIN_`, `AVG_`, `MAX_TIMER_WAIT`, `QUANTILE_95`, `QUANTILE_99`, `QUANTILE_999` | Latency in picoseconds, measured by the gateway from sending the statement to the end of the result (so they include the network to MariaDB). Quantiles are the upper bounds of histogram buckets |
| `SUM_ROWS_SENT` | Rows returned |
| `SUM_ROWS_EXAMINED` | Only of the probed executions |
| `QUERY_SAMPLE_TEXT` | Always `NULL`: SprigDB keeps no plain statement text |
| Lock time, temporary tables, sorts, index use, memory, warnings | `0`: MariaDB can't give them per statement |

The totals run from when this gateway first counted the fingerprint, or from your last `TRUNCATE` of the table. The `sys` views `statement_analysis` and `statements_with_errors_or_warnings` (and their `x$` forms) read the same rows.

```sql test
SELECT DIGEST, COUNT_STAR, DIGEST_TEXT
FROM performance_schema.events_statements_summary_by_digest
WHERE DIGEST_TEXT LIKE '%orders%'
ORDER BY SUM_TIMER_WAIT DESC
LIMIT 5;

SELECT query, exec_count, total_latency FROM sys.statement_analysis LIMIT 5;
```

```sql
TRUNCATE TABLE performance_schema.events_statements_summary_by_digest;
```

Fingerprints are also in Studio's workload views and the REST API; see [Workload](../performance/workload.md).

### sprigctl explain

`sprigctl explain` tells you why SprigDB did something:

| Command | Explains |
|---|---|
| `sprigctl explain query <fingerprint>` | Why an index was or wasn't built for a query shape (the fingerprint is the 16-hex-digit `DIGEST`) |
| `sprigctl explain decision` | A recorded decision, such as the `DECISION_ID` of `SPRIG SHOW ADVICE` |
| `sprigctl explain placement` | Why a tenant's data is where it is |

See [sprigctl](../tools/sprigctl.md).

## Recommendations

Every automatic loop (automatic indexing, the sharding advisor, the balancer, failover and others) can act on its own, wait for approval, or only recommend, depending on its mode setting (`auto_index.mode`, `advisor.mode`, `sharding.mode`, …). Items waiting for a person are **recommendations**. In SQL you list them and decide. See [Approvals](../concepts/approvals.md).

### SHOW SPRIG RECOMMENDATIONS

```sql
SHOW SPRIG RECOMMENDATIONS [ALL] [FOR DATABASE db_name | FOR TABLE [db_name.]table_name]
```

Lists the tenant's open recommendations (with `ALL`, closed ones too) that you may see.

| Column | Meaning |
|---|---|
| `ID` | The tenant's number for the item, used in the statements below |
| `KIND` | `index.create`, `index.drop`, `vector.ann`, `shard.split`, `shard.merge`, `shard.move`, `advisor.design`, `advisor.rekey`, `advisor.reference`, `advisor.lookup`, `balancer.move`, `dc.move`, `standby.create`, `standby.drop`, `failover.cluster`, `failover.dc` or `graph.partition` |
| `LEVEL` | Where it sits: `table`, `database`, `tenant`, `dc` or `platform` |
| `STATE` | `open`, `approved`, `applying`, `applied`, `failed`, `ignored`, `snoozed`, `superseded` or `expired` |
| `TARGET` | What it changes |
| `SUMMARY` | What and why |
| `ACTION` | What approving does |
| `AGE_HOURS` | How long it has been open |
| `NOTIFIES` | Who is told about it |
| `AUTO_APPLY_AT` | When it applies on its own, if its mode has a timer |
| `FULL_ID` | The item's global id (`t<tenant>.<n>`), for `sprigctl recommendations` and the API |

```sql test
SHOW SPRIG RECOMMENDATIONS;
SHOW SPRIG RECOMMENDATIONS ALL FOR TABLE orders;
```

### SPRIG APPROVE RECOMMENDATION

```sql
SPRIG APPROVE RECOMMENDATION id [REASON 'text']
```

Approves an open item; the deciding loop applies it on its next cycle. **Privileges:** `ALTER` on the item's table or database (for index items, `INDEX` is enough), or `SPRIG_TENANT_ADMIN` for tenant-level items.

### SPRIG IGNORE RECOMMENDATION

```sql
SPRIG IGNORE RECOMMENDATION id [FOR n DAYS | FOREVER] [REASON 'text']
```

Says no. The advice is suppressed for `n` days, or with `FOREVER` for good; either way, an ignored item comes back early only when its score grows past `approvals.reraise_factor` (2) times the score it had when ignored.

### SPRIG SNOOZE RECOMMENDATION

```sql
SPRIG SNOOZE RECOMMENDATION id FOR n HOURS
```

Takes the item out of the inbox for `n` hours.

```sql
SPRIG APPROVE RECOMMENDATION 17 REASON 'checked the write cost';
SPRIG IGNORE RECOMMENDATION 18 FOR 30 DAYS REASON 'seasonal traffic';
SPRIG SNOOZE RECOMMENDATION 19 FOR 48 HOURS;
```

```sql test-error
-- no such item: 39972 ER_SPRIG_RECOMMENDATION_NOT_FOUND
SPRIG APPROVE RECOMMENDATION 999999999;
```

Each statement answers OK with one row affected. `sprigctl recommendations list | show | approve | ignore | snooze | reopen | comment` and Studio do the same, and more.

## Errors

| Code | Name | When |
|---|---|---|
| 1064 | (`42000`) | Syntax error |
| 39420 | `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` | `EXPLAIN FORMAT=SPRIG` of a statement the router can't run exactly |
| 39421 | `ER_SPRIG_PLAN_LIMIT_EXCEEDED` | The plan exceeds a router limit |
| 39972 | `ER_SPRIG_RECOMMENDATION_NOT_FOUND` | No such recommendation, or one you may not see |
| 39973 | `ER_SPRIG_RECOMMENDATION_STATE` | The item's state doesn't allow the action, such as approving an applied item |
| 39974 | `ER_SPRIG_APPROVAL_DENIED` | You may not act at the item's level |
| 39975 | `ER_SPRIG_RECOMMENDATION_STALE` | Re-validation failed before applying; the item expired |
| 39976 | `ER_SPRIG_APPROVAL_WINDOW_INVALID` | An approval window that is empty, past or longer than 30 days |

## Related

- [Query plans](../performance/query-plans.md)
- [Workload](../performance/workload.md)
- [Automatic indexing](../performance/automatic-indexing.md)
- [Approvals](../concepts/approvals.md)
- [sprigctl](../tools/sprigctl.md)
