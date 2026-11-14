---
title: Query plans
audience: [user, dba]
sidebar:
  order: 2
---

# Query plans

Before a statement runs, the gateway decides which shards it touches and how their answers combine. A statement that touches one shard runs as one MariaDB statement and costs what it would cost on MariaDB alone. A statement that touches many shards fans out and is merged in the gateway, which costs more. This page shows how to read that decision with `EXPLAIN`, what makes a statement scatter, and how to make it single-shard.

If your tenant has never split, every table group has one shard and every statement is single-shard. The plan starts to matter once a group splits. See [Shards and table groups](../concepts/shards-and-table-groups.md).

## Two kinds of EXPLAIN

| Statement | Shows |
|---|---|
| `EXPLAIN FORMAT=SPRIG <statement>` | SprigDB's plan: the routing, how answers merge, the SQL each shard runs, and one shard's MariaDB plan |
| `EXPLAIN <statement>`, `EXPLAIN FORMAT=JSON <statement>` | MariaDB's own plan of the statement on one shard (the first shard it would run on) |

Use `FORMAT=SPRIG` to see whether a statement scatters. Use plain `EXPLAIN` to see whether MariaDB uses an index on the shard. Both work from any MySQL or MariaDB client, from the REST API's `sql:query`, and from Studio's **Explain** button (which runs `EXPLAIN FORMAT=JSON`).

The examples on this page use three tables of a shop, all keyed by customer:

```sql test
CREATE TABLE customers (
  id BIGINT PRIMARY KEY,
  name VARCHAR(100) NOT NULL
);
CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  status VARCHAR(20) NOT NULL,
  total DECIMAL(10,2) NOT NULL,
  placed_at DATETIME NOT NULL,
  KEY (customer_id)
);
CREATE TABLE order_items (
  order_id BIGINT NOT NULL,
  customer_id BIGINT NOT NULL,
  sku VARCHAR(20) NOT NULL,
  qty INT NOT NULL,
  PRIMARY KEY (order_id, sku)
);
INSERT INTO customers VALUES (42, 'Asha'), (43, 'Ben');
INSERT INTO orders VALUES
  (1007, 42, 'open', 25.00, '2026-11-02 10:00:00'),
  (1008, 42, 'paid', 40.00, '2026-11-03 11:30:00'),
  (1009, 43, 'open', 12.50, '2026-11-03 12:00:00');
INSERT INTO order_items VALUES
  (1007, 42, 'TEA-01', 2),
  (1007, 42, 'MUG-02', 1),
  (1009, 43, 'TEA-01', 1);
```

Plain `EXPLAIN` shows how MariaDB runs the statement on a shard, for example whether it uses the `customer_id` index:

```sql test
EXPLAIN SELECT * FROM orders WHERE customer_id = 42;
```

`EXPLAIN FORMAT=SPRIG` of a statement that names no sharded table fails with `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` (39420) and the detail "EXPLAIN FORMAT=SPRIG of a statement over unsharded tables": such a statement goes to MariaDB unchanged, so plain `EXPLAIN` is all there is.

## Read EXPLAIN FORMAT=SPRIG

The result has five columns:

| Column | Meaning |
|---|---|
| `id` | The row's number |
| `parent` | The row it belongs under, or `NULL` for a top row |
| `operator` | What happens at this step |
| `target` | Where it happens: a shard, a table group, the gateway, or the session's cluster |
| `detail` | The SQL sent, or a description of the step |

### A single-shard plan

The plans below come from a tenant whose `orders` table group has been split into four shards; on your tenant, schema and shard names differ.

```sql
EXPLAIN FORMAT=SPRIG
SELECT * FROM orders WHERE customer_id = 42 AND status = 'open';
```

```text
id  parent  operator  target     detail
1   NULL    Route     shard 1    SELECT * FROM `t42_d1_g3_s1`.orders WHERE customer_id = 42 AND status = 'open'
2   1       Shard     shard 1    {"query_block": {"select_id": 1, "table": {"table_name": "orders", "access_type": "ref", ...}}}
```

`Route` to one `shard` means the statement passes straight through: the gateway swaps your database name for the shard's schema name, sends it, and relays the answer without decoding it. The `Shard` row under it is MariaDB's `EXPLAIN FORMAT=JSON` on that shard.

### A scatter plan

```sql
EXPLAIN FORMAT=SPRIG
SELECT id, total FROM orders WHERE placed_at >= '2026-11-01' ORDER BY total DESC LIMIT 10;
```

```text
id  parent  operator   target              detail
1   NULL    Limit      gateway             offset 0, count 10
2   1       MergeSort  gateway             1 keys (weights, PAD SPACE)
3   2       Route      group 3: 4 shards   SELECT id, total, WEIGHT_STRING(...) FROM `t42_d1_g3_s0`.orders WHERE ... ORDER BY ... LIMIT 10  [LIMIT 10 per shard]
4   3       Shard      shard 0             {"query_block": ...}
```

The statement runs on every shard of table group 3 at once (up to `router.max_parallel_shards` at a time), each shard returns its own top 10, and the gateway merges them in the collation's order. The detail shows the first shard's SQL; every shard gets the same statement with its own schema.

### Operators

| Operator | Target | Meaning |
|---|---|---|
| `Route` | `shard N` | The whole statement runs on one shard |
| `Route` | `group G: N shards` | One statement per shard of the group |
| `Limit` | `gateway` | `LIMIT` and `OFFSET` applied after merging; each shard got `offset + count` rows |
| `MergeSort` | `gateway` | The shards' sorted rows merged by sort key, exactly as MariaDB orders them |
| `Aggregate` | `gateway` | Partial aggregates from each shard finished in the gateway (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, grouped, with `HAVING`) |
| `LookupJoin` | `gateway` | A join of two tables in different groups: the outer side's rows are read, then the inner side is fetched in batches of key values (`router.lookup_batch_size`) |
| `Lookup` | a lookup index, or `group G` | Under `LookupJoin`, the inner side's query; at the top, a global lookup index narrowed the statement to the shards holding the values |
| `ParentRoute` | `group G` | Inserted rows follow their parent rows' shards |
| `IdBlocks` | a table and column | Inserted rows take generated key values from the gateway's ID block |
| `Write` / `Shard` | `gateway` / `shard N` | A multi-shard `INSERT`, `UPDATE` or `DELETE`, one statement per shard, in one transaction |
| `Gather` / `Copy` | the session's cluster | The statement can't be split exactly, so it runs once over temporary copies of the rows it reads |
| `Graph`, `GraphCall`, `GraphStep`, `GraphShards`, `KeyJoin` | | A statement with `GRAPH_TABLE`; see [Graphs](../data-models/graphs.md) |
| `Forward` | the session's cluster | No distributed table involved; the statement runs as written |
| `Reject` | `gateway` | The statement can't run with exact results; the reason code follows |

## What makes a statement scatter

The gateway finds, for every sharded table in each `SELECT` block, the conjuncts of the `WHERE` clause that fix its distribution key. A conjunct routes when it is:

- an equality with a literal or a bound parameter: `customer_id = 42`, `42 = customer_id`, `customer_id <=> ?`;
- an `IN` list: `customer_id IN (1, 2, 3)`;
- an `OR` of such equalities: `customer_id = 1 OR customer_id = 2`.

Anything else doesn't restrict the shards. In particular, these scatter:

| Statement shape | Why |
|---|---|
| No condition on the distribution key | Any shard may hold matching rows |
| A range on the key (`customer_id > 100`, `BETWEEN`) | Ranges don't route |
| An expression on the key (`customer_id + 0 = 42`, `CAST(customer_id AS CHAR) = '42'`) | Not one exact key value |
| A value of the wrong class (`'042'` or `42.5` against an integer key, `NULL`) | Not one exact key value |
| More than `router.max_route_values` (1024) key values in an `IN` list | Scattering is cheaper than computing that many routes |
| A join whose tables are in different table groups | The rows don't live together; it becomes a `LookupJoin` or a gathered read |

Joins stay on one shard when every table is in the same table group and the join condition equates their distribution keys (`orders.customer_id = customers.id` when both groups' key is the customer id).

Prepared statements are planned at each execution with their parameter values, so `WHERE customer_id = ?` is single-shard every time.

## How to make a query single-shard

1. **Filter on the distribution key.** Add the key to the statement, even when another column already identifies the row. Once `orders` is split by customer, `WHERE id = 1007` alone scatters, because the order id doesn't say which shard holds the row; adding the customer makes it single-shard:

```sql test
SELECT id, status, total FROM orders WHERE customer_id = 42 AND id = 1007;
```

```text output
id	status	total
1007	open	25.00
```

2. **Join on the key.** Join tables of the same group on their distribution key, and filter on it:

```sql test
SELECT o.id, i.sku, i.qty
FROM orders o JOIN order_items i
  ON i.customer_id = o.customer_id AND i.order_id = o.id
WHERE o.customer_id = 42
ORDER BY i.sku;
```

```text output
id	sku	qty
1007	MUG-02	1
1007	TEA-01	2
```

3. **Put tables that are queried together in one table group.** The [sharding advisor](sharding-advisor.md) does this from your foreign keys, joins and transactions; you can also pin its choice.

4. **Keep small, read-mostly tables out of sharded groups.** Pin tables such as `countries` and `currencies` `UNSHARDED` (or `REFERENCE`) so the advisor leaves them in the database's default group; see [Pins](sharding-advisor.md#pin-a-tables-place). Copies of reference tables on every cluster aren't built yet, so a `REFERENCE` pin currently behaves like `UNSHARDED`.

5. **Let a global lookup index find the shard.** When a table is often read by a unique column other than its key (an order number, an email address), a lookup index maps each value to its shard. `EXPLAIN FORMAT=SPRIG` then shows a top `Lookup` row, and the statement runs only on the shards that hold the values. Automatic indexing doesn't propose lookup indexes yet.

6. **Keep key values literal and of the key's type.** Compare an integer key with an integer, not a string.

Scatter isn't always wrong. A report over every customer has to read every shard; the gateway runs the shards in parallel and pushes `LIMIT` and partial aggregates down, so the cost is mostly the shards' own work.

## Reads that can't be split exactly

Some `SELECT` statements can't be answered by running them on each shard and merging: window functions, subqueries over shards, set operations (`UNION`, `INTERSECT`, `EXCEPT`), derived tables and CTEs over shards, joins of several groups or on arbitrary conditions. The gateway runs these one of two ways:

- **Gathered read** (`Gather` in the plan): the rows each table needs are copied into temporary tables on the session's cluster, filtered by the statement's own conditions where they can be pushed down, and the statement runs once over the copies. A gathered read may copy at most `router.gather.max_rows` (100,000) rows; at most `router.gather.max_concurrent` (4) run at once per gateway.
- **Analytic read**: the same statement runs inside the gateway over streams of the shards' rows, with joins, grouping, aggregates in exact integer and decimal arithmetic, and window functions, and results rendered as MariaDB would. It may read at most `router.analytics.max_rows` (50,000,000) rows and spill at most `router.analytics.spill_mb` (4096 MB) to disk.

`router.analytics.mode` chooses between them:

| Value | Does |
|---|---|
| `fallback` (default) | Gather; when the gather passes `router.gather.max_rows`, or gathering is off, try an analytic read |
| `prefer` | Analytic read first; gather when the analytic read refuses the statement |
| `off` | Gather only |

Two optimizer hints override the mode for one statement: `/*+ SPRIG_GATHER */` forces a gather, and `/*+ SPRIG_ANALYTICS */` asks for an analytic read first.

```sql
SELECT /*+ SPRIG_ANALYTICS */ customer_id,
       SUM(total) OVER (PARTITION BY customer_id ORDER BY placed_at) AS running_total
FROM orders;
```

A gathered read that copies too many rows, with the analytic read refusing too, fails with `ER_SPRIG_PLAN_LIMIT_EXCEEDED` (39421). Plain `EXPLAIN` of a gathered read is refused; use `EXPLAIN FORMAT=SPRIG`.

## Statements that are refused

When a statement can't run with the result one MariaDB server would give, it fails with `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` (39420) and a reason code. It never returns a different answer. The reasons:

| Reason | Statement |
|---|---|
| `join_condition`, `join_types`, `join_depth` | A join across shards the gateway can't split or gather |
| `correlated_subquery`, `subquery`, `derived_table`, `recursive_cte` | A subquery, derived table or recursive CTE across shards that can't be gathered |
| `window_function`, `set_operation`, `aggregate`, `ordering` | A window function, set operation, aggregate or ordering the gateway can't reproduce exactly (for example ordering by a `GEOMETRY` or an unknown collation) |
| `limited_dml` | A multi-shard `UPDATE` or `DELETE` with `ORDER BY` or `LIMIT` |
| `multi_table_dml` | A multi-table `UPDATE` or `DELETE` not on one shard |
| `insert_select` | `INSERT ... SELECT` into a sharded table |
| `upsert_unique` | An upsert or `REPLACE` with a unique key that lacks the distribution key |
| `key_change` | An `UPDATE` that changes a distribution key |
| `insert_key`, `auto_increment` | An `INSERT` row whose key value isn't a literal of the key's type, or is missing where it can't be generated |
| `variables`, `found_rows` | Variable assignment or `SQL_CALC_FOUND_ROWS` across shards |
| `locking` | `LOCK TABLES` or `HANDLER` on a sharded table |
| `routine` | A stored function or UDF in a statement across shards |
| `fulltext_relevance` | `MATCH ... AGAINST` relevance compared across shards |
| `cross_cluster` | Shards on several clusters where the statement needs one |
| `lookup`, `foreign_key` | A write that would leave a lookup index or a foreign key the gateway maintains unchecked |
| `schema_skew` | Shards answered with different columns (a schema change is in flight) |
| `gather` | A read that needs gathering, but the session can't gather it; the detail says why |
| `parse`, `unsupported` | Text the router can't read, or a statement form it doesn't distribute |

Rewrite the statement to route on the key, split it into single-shard statements, or move the work to an analytic read. `sprig_router_rejected_total{reason}` counts refusals by reason.

## Limits on one statement

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `router.max_parallel_shards` | int | 32 | Shard queries one statement runs at once |
| `router.max_route_values` | int | 1024 | Key combinations routed before the statement scatters |
| `router.max_statement_memory_mb` | int | 256 | Gateway memory for one statement's merging; past it, `ER_SPRIG_ROUTER_MEMORY_LIMIT` (39422) |
| `router.lookup_batch_size` | int | 256 | Keys per `IN` list in a lookup join |
| `router.gather.max_rows` | int | 100000 | Rows a gathered read may copy (0 turns gathering off) |
| `router.gather.max_concurrent` | int | 4 | Gathered reads per gateway at once |
| `router.analytics.mode` | text | `fallback` | `off`, `fallback` or `prefer` |
| `router.analytics.max_rows` | int | 50000000 | Rows an analytic read may read |
| `router.analytics.spill_mb` | int | 4096 | Disk an analytic read may spill to (0: none) |
| `router.plan_cache_entries` | int | 10000 | Cached plans per gateway |
| `router.stale_map_retries` | int | 2 | Retries after the shard map changed under a statement |

All but `router.gather.max_concurrent`, `router.plan_cache_entries` and `router.stale_map_retries` can be set per tenant. See [Limits and timeouts](limits-and-timeouts.md) for the limits that apply to every statement.

## Watch plans with metrics

| Metric | Meaning |
|---|---|
| `sprig_router_statements_total{plan}` | Statements by plan: `pass`, `scatter`, `merge`, `aggregate`, `lookup_join`, `write`, `gather`, `analytics`, `reject`, ... |
| `sprig_router_fanout_shards` | Shards per statement |
| `sprig_router_rejected_total{reason}` | Refusals by reason |
| `sprig_router_lookup_routes_total` | Statements narrowed by a lookup index |
| `sprig_router_gather_rows_total` | Rows copied by gathered reads |

A rising share of `scatter`, `merge` and `aggregate` plans after a split usually means a common statement lacks its distribution key. Find it on the [workload](workload.md) views and check it with `EXPLAIN FORMAT=SPRIG`.

## Related

- [Shards and table groups](../concepts/shards-and-table-groups.md)
- [Sharding advisor](sharding-advisor.md)
- [EXPLAIN and workload in SQL](../sql/explain-and-workload.md)
- [Workload](workload.md)
- [Error codes](../reference/errors.md)
