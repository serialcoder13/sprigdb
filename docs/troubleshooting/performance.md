---
title: Performance problems
audience: [user, dba, operator]
sidebar:
  order: 3
---

# Performance problems

This page covers statements that are slow, statements that read more shards than they should, indexes that don't appear, reads that lag behind writes, and statements stopped by a timeout or cap. Start by finding out where the time goes: in MariaDB, in the gateway, or waiting for a turn.

## Where the time goes

Compare two histograms for the protocol in question:

| Metric | Measures |
|---|---|
| `sprig_gateway_query_duration_seconds` | The whole statement as the client sees it: admission, the gateway and MariaDB |
| `sprig_gateway_overhead_seconds` | The same, minus the time spent waiting on MariaDB |

If the overhead is a small part of the duration, the time is in MariaDB: an index or a better query will help. If the overhead is large, the gateway is merging rows from many shards, waiting for an admission slot or a pooled connection, or holding the statement during a shard move. Then look at:

| Metric | High values mean |
|---|---|
| `sprig_gateway_admission_wait_seconds` | Statements queue for a slot under `limits.tenant_active_statements` or `limits.user_active_statements` |
| `sprig_backend_pool_wait_seconds` | All pooled connections to a MariaDB node are busy |
| `sprig_router_fanout_shards` | Statements run on many shards |
| `sprig_gateway_hold_waits_total` | Statements waited for a move's cutover; see [Sharding and moves](sharding-and-moves.md#write-pauses-at-cutover) |

For one slow statement, find its trace at `/debug/traces` on the gateway's telemetry port. Statements slower than `telemetry.trace_slow_ms` (500 ms) are always kept. The trace has one `backend.query` span per round trip to MariaDB, so you can see whether one shard was slow or the gateway spent the time between them.

## Slow queries

### Find them

SprigDB records every statement shape (a *fingerprint*: the statement with its literals removed) per tenant, with runs, latency percentiles and rows examined.

- **REST:** `GET /v1/workload/fingerprints?hours=24&limit=20` lists your tenant's top fingerprints by total time; `GET /v1/workload/fingerprints/{id}/series?days=7` shows one over time.
- **Studio:** the Indexing page shows slow fingerprints and what automatic indexing decided about them.

A fingerprint whose p95 is above `workload.watch_p95_ms` (50 ms) is *watched*: SprigDB samples its parameter values (`workload.sample_rate`) so the index advisor can replay it.

### Read the plan

`EXPLAIN FORMAT=SPRIG` shows how the gateway runs a statement over sharded tables, and includes MariaDB's own plan for each shard:

```sql
EXPLAIN FORMAT=SPRIG
SELECT o.id, o.total FROM orders o WHERE o.customer_id = 42 ORDER BY o.created_at DESC LIMIT 20;
```

```text
id  parent  operator  target              detail
1           Route     group 3: 1 shards   SELECT ... (the statement as sent to the shard)
2   1       Shard     shard s1            {"query_block": {...}}   (MariaDB's EXPLAIN FORMAT=JSON)
```

The output is abridged here; the columns are `id`, `parent`, `operator`, `target` and `detail`.

The operators you will see:

| Operator | Meaning |
|---|---|
| `Route` | The statement runs on the shards listed: `group 3: 1 shards` is ideal |
| `MergeSort` | Rows from several shards are merged on the `ORDER BY` keys in the gateway |
| `Aggregate` | Partial aggregates from several shards are combined in the gateway |
| `Limit` | The gateway applies `LIMIT` and `OFFSET` after merging |
| `Lookup`, `LookupJoin` | A unique lookup index or a join narrowed the shards |
| `Gather` | Rows are copied into the gateway to run a statement shards can't run alone |
| `Write` | A write and the shards it reaches |
| `Reject` | The statement can't be distributed; the reason code follows |
| `Forward` | The statement runs as it is on one cluster |

`EXPLAIN FORMAT=SPRIG` works only for statements over sharded tables. For a tenant whose tables are all unsharded, use MariaDB's `EXPLAIN` as usual: the statement runs on one schema exactly as written.

### Fix

- When the `Shard` row's plan shows a full scan (`"access_type": "ALL"`) or many rows examined, add an index, or let [automatic indexing](#automatic-indexing-doesnt-act) add one.
- When `Route` lists many shards, see [Scatter queries](#scatter-queries).
- When the overhead is in admission or the backend pool, see [Timeouts and caps](#timeouts-and-caps).

## Scatter queries

**Symptom.** `EXPLAIN FORMAT=SPRIG` shows `Route group N: 8 shards` for a query you expected to touch one, and `sprig_router_statements_total{plan="scatter"}` (or `merge`, `aggregate`) is high.

**Cause.** The gateway sends a statement to one shard only when it can compute the shard from the distribution key. It scatters when:

- the `WHERE` clause has no equality or `IN` list on the table group's distribution key;
- the key is compared through a function or an expression (`WHERE LOWER(customer_id) = ...`), or with a range;
- an `IN` list or a combination of key values is longer than `router.max_route_values` (1,024);
- a key value can't be normalised under the key's type or collation (`ER_SPRIG_KEY_VALUE_UNROUTABLE` internally), for example a string that isn't valid in the key's character set;
- a join connects two tables of different table groups.

Scattering is correct, never wrong: the gateway runs the statement on every shard and merges, sorts or aggregates the rows. It is slower and costs more as the shard count grows.

**Confirm.**

```sql
EXPLAIN FORMAT=SPRIG SELECT * FROM orders WHERE status = 'open';
```

```text
id  parent  operator   target              detail
1           Route      group 3: 8 shards   SELECT ... WHERE status = 'open'
```

`sprigctl shard list acme` shows each group's key and shards.

**Fix.**

- Add the distribution key to frequent queries: `WHERE customer_id = ? AND status = 'open'`.
- For a unique column you look rows up by (an email, an order number), a unique lookup index lets the gateway find the shard without scattering; `sprig_router_lookup_routes_total` counts statements it narrowed.
- If the table is in the wrong group, or the group has the wrong key, change it: see [Sharding](../sql/sharding.md) and [Sharding advisor](../performance/sharding-advisor.md). `sprigctl shard rekey` re-keys a group online.
- Small tables that every shard joins against belong in reference tables, copied to every cluster.

A tenant whose groups have one shard each never scatters; the question arises only after a split.

## Missing indexes

**Symptom.** A query is slow, MariaDB's plan in the `Shard` row shows a full scan, and `sprigctl index list` shows no automatic index for the table.

**Fix now.** Create the index yourself. On a sharded table, `CREATE INDEX` runs on every shard as one schema change:

```sql
CREATE INDEX ix_orders_status ON orders (customer_id, status, created_at);
```

Automatic indexing watches your own indexes too. With `auto_index.retire_user_indexes` at its default, `on`, it drops a user index that stops being used; set it to `recommend` to be asked first, or pin an index to keep it as it is: `sprigctl index pin <index>`.

## Automatic indexing doesn't act

Automatic indexing builds an index only when a query shape is slow or heavy enough, for long enough, and when the index passes validation on a replica. When nothing happens, ask SprigDB why:

```sh
sprigctl explain query 9f3c2a7b1d0e4f68     # the fingerprint, 16 hex digits
sprigctl index list --all                    # including rejected and dropped
sprigctl index recommendations               # waiting for approval
```

Over REST: `GET /v1/indexing/queries/{fingerprint}` and `GET /v1/indexing/recommendations`. In Studio, the Indexing page shows the same.

### Common reasons

| Reason | Cause | Fix |
|---|---|---|
| `mode_off` | `auto_index.mode` is `off` for the table, database or tenant | `ALTER TABLE orders SPRIG SET (auto_index.mode = 'on');` |
| Recommendations wait | `auto_index.mode` is `approve` or `recommend`: indexes are proposed, not built | Approve with `sprigctl index approve <id>`, or in Studio's inbox |
| `below_thresholds` | The shape isn't slow enough or frequent enough. It qualifies when it ran at least `auto_index.min_runs` (50) times in `auto_index.window_hours` (24) with p95 above `auto_index.p95_threshold_ms` (100 ms); or is among the top `auto_index.top_n` (10) by total time while examining more than `auto_index.min_examined_per_returned` (100) rows per row returned; or repeatedly scans large tables | Wait for more traffic, or lower the thresholds for the table |
| Studio runs ignored | Statements run from Studio don't count unless `auto_index.include_studio` is on | Run the workload from the application, or turn the setting on |
| `cooldown` | A candidate on the same table was decided recently | Wait; `sprigctl explain query` shows when |
| `unsargable` | Every predicate is one an index can't serve: a function of the column, a leading wildcard `LIKE '%x'` | Rewrite the predicate so the column stands alone |
| Validation failed | The index didn't improve p95 by `auto_index.min_improvement_pct` (30%) on a replica | Nothing to do; the index wouldn't help |
| Limits reached | `auto_index.max_per_table` (5) indexes on the table, `auto_index.max_new_per_day` (5) for the tenant, or `auto_index.max_storage_pct` (20%) extra storage | Drop unused indexes, or raise the limit for the table |
| Build waits | Builds run in `auto_index.build_window`, one at a time per cluster (`auto_index.max_builds_per_cluster`); a build aborts when replica lag passes `auto_index.abort_replica_lag_seconds` (300) | Wait for the window; reduce replica lag |

`sprigctl explain decision <id> --tenant acme` shows the full record of any decision the list points to.

## Replica lag

SprigDB sends plain reads to replicas, but only to replicas that have the data the session needs. When replicas lag, reads move back to the writer, which is correct but loads it.

**How reads are routed.** A read outside a transaction may go to a replica when all of these hold:

- `reads.routing` is `auto` (the default) for the object, database or tenant; `writer` keeps every read on the writer;
- the replica is no more than `reads.max_replica_lag_ms` (1,000 ms) behind;
- the replica has applied the session's last commit. With `reads.causal_scope = tenant_gateway` (the default) it must also have every commit the tenant made through this gateway, so one connection reads what another just wrote.

If no replica has caught up, the gateway waits up to `reads.gtid_wait_timeout_ms` (100 ms) on a replica that should catch up soon, and otherwise reads from the writer. Galera nodes follow the same rule with `reads.max_galera_lag_ms`.

### Symptoms and causes

| Symptom | Likely cause |
|---|---|
| The writer is busy and replicas idle | Replicas lag beyond `reads.max_replica_lag_ms`, or the gateway can't read replication positions |
| Reads are slower by up to 100 ms | Reads wait for a replica to catch up (`reads.gtid_wait_timeout_ms`) |
| An index build or shard move stopped | Replica lag passed `auto_index.abort_replica_lag_seconds` or `mover.throttle_replica_lag_ms` |

### Confirm

| Metric | Shows |
|---|---|
| `sprig_backend_replica_lag_seconds` | Each replica's lag, by `cluster` and `node` |
| `sprig_backend_node_behind` | 1 for nodes too far behind to get reads |
| `sprig_backend_reads_total` | Reads by `target` (writer or replica) and whether they were `causal` |
| `sprig_backend_gtid_wait_seconds`, `sprig_backend_gtid_wait_timeouts_total` | Waits for replicas to catch up, and those that gave up and went to the writer |

`sprigctl cluster show <id>` shows each node's role, health and lag. A replica is reported as lagging past `cluster.max_replica_lag_seconds` (10 seconds).

### Fix

- Find why the replica lags: a long transaction or large batch on the writer, a replica with less I/O or CPU, or `slave_parallel_threads` set too low on MariaDB.
- Break large writes into batches of a few thousand rows.
- If the gateway reads no positions at all, check that the monitoring account `sprig_gw` exists on every node. Without it, gateways can't read replication positions and send no reads to replicas; `sprigctl cluster register` reports it as a failed precheck.
- To trade freshness for load, raise `reads.max_replica_lag_ms` for a table whose readers tolerate it. To keep a table's reads exact and on the writer, set `reads.routing = 'writer'` for it.

## Timeouts and caps

These limits stop a statement rather than let it hurt everyone else. Each fails with its own error.

| Limit | Default | Error when passed | What to do |
|---|---|---|---|
| `limits.queue_timeout_ms` | 5,000 ms | `ER_SPRIG_ADMISSION_TIMEOUT` (39002), retryable | Find what holds the slots; raise `limits.tenant_active_statements` (256) or `limits.user_active_statements` (64) |
| `limits.tenant_memory_mb`, `gateway.memory_limit_mb` | 1,024 MB per tenant per gateway; the gateway's total is 75% of its memory limit | `ER_SPRIG_MEMORY_LIMIT` (39003), retryable | Add `LIMIT`, filter by the distribution key, or raise the limit |
| `router.max_statement_memory_mb` | 256 MB | `ER_SPRIG_ROUTER_MEMORY_LIMIT` (39422) | The rows one statement holds in the gateway while merging; narrow the statement |
| `router.gather.max_rows` | 100,000 rows | `ER_SPRIG_PLAN_LIMIT_EXCEEDED` (39421) | A gathered read copied too many rows; with `router.analytics.mode = fallback` the gateway retries it on the analytic engine first |
| `router.analytics.max_rows` | 50,000,000 rows | the analytic read is refused | Narrow the query, or use the analytics API |
| `analytics.query_timeout_seconds` | 300 s | the analytic query is cancelled | Narrow the query or raise the limit for the database |
| `backend.acquire_timeout_ms` | 2,000 ms | `ER_SPRIG_BACKEND_POOL_TIMEOUT` (39031), retryable | Look for long transactions holding connections; raise `backend.max_conns_per_node` |
| `backend.connect_timeout_ms` | 2,000 ms | `ER_SPRIG_BACKEND_UNAVAILABLE` (39030), retryable | Check the cluster's health |
| `txn.distributed_lock_wait_timeout_seconds` | 10 s | 1205, lock wait timeout | Shorten transactions that span two clusters |
| `router.max_parallel_shards` | 32 | none: more shards run in turn | Raise it only if MariaDB has headroom |
| `studio.statement_timeout_ms` | 30,000 ms | the Studio statement is cancelled | Applies to Studio only |
| `search.query_timeout_ms` | 5,000 ms per partition | the search fails | Narrow the search |
| `graph.max_time_ms`, `graph.max_rows` | 30,000 ms, 1,000,000 rows | `ER_SPRIG_GRAPH_BUDGET_EXCEEDED` | Bound the traversal with fewer hops |

Every value is a setting you can read with `SHOW SPRIG SETTINGS LIKE 'router.%';` and change at the scopes the [settings reference](../reference/settings.md) lists. See [Limits and timeouts](../performance/limits-and-timeouts.md) for the full list.

## Related

- [Query plans](../performance/query-plans.md)
- [Automatic indexing](../performance/automatic-indexing.md)
- [Workload](../performance/workload.md)
- [Read routing](../performance/read-routing.md)
- [Limits and timeouts](../performance/limits-and-timeouts.md)
