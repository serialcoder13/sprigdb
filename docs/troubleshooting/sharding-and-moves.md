---
title: Sharding and move problems
audience: [dba, operator]
sidebar:
  order: 4
---

# Sharding and move problems

This page covers shard moves, splits and merges that are stuck or slow, the short write pause at the end of a move, statements SprigDB refuses to run across shards, and transactions that span clusters. For how sharding works, see [Shards and table groups](../concepts/shards-and-table-groups.md).

## What a move does

Every move, split and merge is a **job** that a worker runs. A job copies rows to the target while it applies the source's binlog, verifies the copy, then cuts over: for a moment it holds new writes, applies the last changes, switches the shard map and releases the writes. Until the cutover commits, the source stays authoritative and the job can be cancelled with nothing lost.

```sh
sprigctl job list --tenant acme
sprigctl job show <job-id>
```

A job passes through these states:

| State | What happens | Can stall because |
|---|---|---|
| `planned` | Waiting for a worker | No worker running, or `mover.max_copies_per_cluster` jobs already copy from the cluster |
| `preparing` | Creating the target schemas and tables | The target is unreachable or incompatible |
| `copying` | Copying rows in chunks, applying the binlog alongside | Throttled to protect the clusters |
| `catching_up` | Applying the binlog until the target is close behind | Writes arrive faster than the job applies them |
| `verifying` | Comparing source and target | Differences that keep coming back |
| `ready` | Caught up and verified; waiting to cut over | A `--manual` move waits for `sprigctl job cutover` |
| `cutting_over` | Inside the write hold | The hold runs over budget and is retried |
| `post_cutover`, `retaining`, `cleaning`, `done` | Committed; the old copy is kept for `mover.source_retain_hours`, then dropped | Rarely; the job only rolls forward |
| `paused` | Stopped by an operator or a guardrail | See the job's last error |
| `rolling_back`, `rolled_back` | Cancelled or failed before the commit; undone | Nothing; the source stayed authoritative |

## A move is stuck or slow

### Confirm what it waits for

```sh
sprigctl job show <job-id>
```

The output gives the state, progress (tables and rows copied), the lag behind the source and the last error. Then check these metrics:

| Metric | Shows |
|---|---|
| `sprig_mover_catchup_lag_seconds` | How far behind the source the job is |
| `sprig_mover_throttled_milliseconds_total` by `reason` | Time the job paused itself, and why |
| `sprig_mover_copy_rows_total`, `sprig_mover_apply_rows_total` | Whether the copy and the binlog apply progress |
| `sprig_mover_cutovers_total` by `outcome` | Cutover attempts that committed or aborted |
| `sprig_mover_errors_total` by `code` | Errors the jobs met, by error name |
| `sprig_mover_verify_mismatches_total` | Rows verification found different |
| `sprig_mover_copy_restarts_total` | Copies that had to start again |

In Studio's operator mode, the **Moves** and **Jobs** pages show the same, with the lag of each move.

### Causes and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Stays `planned` | No worker picked it up | Check that `sprigdb worker` processes run in the data centre and reach etcd |
| `copying` slowly, throttled | A guardrail paused the copy: source threads running above `mover.throttle_threads_running` (64), replica lag above `mover.throttle_replica_lag_ms` (2,000 ms), Galera flow control above `mover.throttle_flow_control_pct` (5%), or catch-up lag above `mover.throttle_apply_lag_ms` (5,000 ms) | Usually right: the job protects live traffic. Run moves at quieter times, or raise the threshold that trips |
| `copying` slowly, not throttled | The copy caps: `mover.copy_max_rows_per_sec` (50,000) and `mover.copy_max_mb_per_sec` (64) per job | Raise the caps for the cluster if it has headroom |
| Never leaves `catching_up` | The job must stay within `mover.ready_max_lag_ms` (1,000 ms) for `mover.ready_sustain_seconds` (60) | Reduce the write rate on the shard for a minute, or move at a quieter time |
| Back to `copying` | `ER_SPRIG_MOVE_SOURCE_DIVERGED` (39134): the source's binlog no longer holds the job's position, because it was purged or a failover lost transactions | Keep binlogs at least `cluster.min_binlog_retention_hours`; let the job restart |
| `rolling_back` or `rolled_back` with `ER_SPRIG_MOVE_VERIFY_FAILED` (39136) | The target still differed after `mover.verify_max_repairs` repairs, so the job rolled back | Find writes that bypass SprigDB (direct MariaDB connections), stop them, then start the move again |
| Error `ER_SPRIG_MOVE_UNSUPPORTED` (39137) | The source logs statements instead of rows, a table has no primary key, or DDL ran during the cutover | Set `binlog_format = ROW`, add primary keys, avoid DDL during moves |
| Error `ER_SPRIG_BINLOG_UNSUPPORTED` (39114) | The replication account's plugin, TLS, binlog checksum or encryption isn't readable | `sprigctl cluster probe <id> --full` shows the fix |
| Error `ER_SPRIG_MOVE_TARGET_INCOMPATIBLE` (39111) | The target already has a schema of that name, or lacks a capability | Drop the leftover schema, or choose another target |
| `paused` after cutover attempts | `mover.max_cutover_attempts` (5) cutovers aborted | See [Write pauses at cutover](#write-pauses-at-cutover) |
| `rolling_back` or `rolled_back` with `gave up after 200 failures` | A step failed 200 times in a row before the cutover, for example because the target cluster was unreachable. Failed steps are retried with a backoff of up to 2 seconds; after the cutover has committed, the job keeps retrying instead | Restore the target cluster, then start the move again |

To give up on a move before its cutover commits:

```sh
sprigctl job cancel <job-id>
```

The job rolls back and the source stays authoritative. After the cutover committed, cancel is refused with `ER_SPRIG_JOB_STATE_INVALID` (39132); move the shard back with a new job instead.

### Errors when starting a job

| Error | Cause | Fix |
|---|---|---|
| `ER_SPRIG_SHARD_LOCKED` (39131) | Another job holds the tenant or shard | Wait for it (`sprigctl job show`), or cancel it |
| `ER_SPRIG_SHARDED_TENANT_MOVE` (39472) | A whole-tenant move of a tenant with table groups | Move its shards one at a time: `sprigctl shard move acme <group> <shard> --to <cluster>` |
| `ER_SPRIG_SPLIT_NOT_ALLOWED` (39471) | The group isn't active, the range is a single hash value, or the split or merge doesn't fit the shard map | Check `sprigctl shard list acme`; merge only adjacent shards |
| `ER_SPRIG_PLACEMENT_UNAVAILABLE` (39120) | No cluster can take the shard | Register or undrain a cluster with room |

### Schema changes during moves

A schema change on a table whose shard is being moved, split or merged waits for the job, up to `ddl.mover_wait_minutes` (120). If the job is still running, the change fails with `ER_SPRIG_SHARD_BUSY` (39470) and the message names the job. Retry when it finishes. `sprig_ddl_mover_waits_total` counts these waits.

Two related errors:

- `ER_SPRIG_DDL_SHARD_DRIFT` (39483): the shards of a table disagree on its definition, so the change is refused and nothing changes. The reconciler repairs drift on its own; `sprig_ddl_shard_skew_total` counts what it finds.
- `ER_SPRIG_DDL_FAILED` (39480): a change ran on some shards and failed on the next. It is finished in the background; don't run it again.

## Write pauses at cutover

**Symptom.** During a move, statements on the moving shard stall for up to about two seconds; a few fail with 1205 (lock wait timeout) or 1213 (deadlock).

**Cause.** At cutover, the job holds new work on the shard while it applies the last changes and switches the map:

- the hold lasts at most `mover.cutover_max_hold_ms` (2,000 ms), plus `mover.hold_grace_ms` (500 ms) in the gateways;
- transactions already open get `mover.txn_grace_ms` (300 ms) to finish;
- new statements wait in the gateway, then run on the new cluster with their session state replayed;
- a statement still held when the hold's deadline passes gets **1205**, which is retryable;
- a transaction left open on the source when the shard moves is rolled back. The client gets **1213** (deadlock, SQLSTATE `40001`), or `ER_SPRIG_SHARD_MAP_CHANGED` (39424, also `40001`) when the transaction's next statement reaches the moved shard. Retry the whole transaction, as after any deadlock;
- a schema change planned against the old layout gets `ER_SPRIG_SHARD_MOVED` (39110, `40001`) and can be retried at once.

If the hold would run over budget, the cutover is aborted (`ER_SPRIG_MOVE_CUTOVER_ABORTED`, 39135), writes are released, and the job tries again after `mover.cutover_retry_seconds` (30 seconds, doubling). After `mover.max_cutover_attempts` (5) it pauses and alerts.

For a move of a tenant to another data centre, the whole tenant pauses, within `migration.write_pause_target_ms` (5,000 ms). A session holding state that can't follow (temporary tables, `GET_LOCK` locks, `LOCK TABLES` or XA) gets `ER_SPRIG_SESSION_MOVED` (39502) and must reconnect.

**Confirm.**

| Metric | Shows |
|---|---|
| `sprig_mover_cutover_hold_seconds` | How long each cutover held writes |
| `sprig_gateway_hold_waits_total`, `sprig_gateway_hold_timeouts_total` | Statements that waited for a hold, and those that got 1205 |
| `sprig_router_txn_map_changed_total` | Transactions rolled back because a shard moved under them |
| `sprig_controller_migration_write_pause_seconds` | Write pauses of moves between data centres |

**Fix.**

- Make the application retry 1205 and 1213, which it should do anyway: see [Reading SprigDB errors](reading-errors.md#retryable-errors).
- Keep transactions short. A transaction open across a cutover delays it or is rolled back.
- If cutovers keep aborting, look for long transactions or a heavy write burst on the shard. Start the move with `--manual`, and run `sprigctl job cutover <job-id>` at a quiet time.
- For a shorter pause, lower `mover.cutover_max_hold_ms`. Cutovers then abort more often under load.

## Statements that can't be distributed

**Symptom.**

```text
ERROR 39420 (HY000): Statement not distributable (window_function): a window function across shards
```

**Cause.** SprigDB never returns an answer that differs from what one unsharded MariaDB would return. When it has no exact plan for a statement across shards, it refuses with `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` (39420). The reason code in parentheses says why. A statement whose rows all lie on one shard never meets this error, so the first fix is always to filter on the distribution key.

**Confirm.** `EXPLAIN FORMAT=SPRIG <statement>` shows a `Reject` row with the reason. `sprig_router_rejected_total` by `reason` shows which reasons your workload meets.

| Reason | Cause | Fix |
|---|---|---|
| `join_condition` | A join across shards without an equality the gateway can use | Join on the distribution key, or co-locate the tables in one group |
| `join_types` | A cross-shard join compares a string with a number | Make the join columns the same type |
| `join_depth` | Too many lookup joins in one statement | Split the statement |
| `correlated_subquery` | A correlated subquery whose rows aren't on the outer row's shard | Rewrite as a join on the distribution key |
| `subquery` | An uncorrelated subquery over several shards | Run it first and pass its result as literals |
| `derived_table`, `recursive_cte` | A derived table, CTE or view, or `WITH RECURSIVE`, over several shards | Filter by the key, or let the gateway gather it when small |
| `window_function` | A window function across shards | Partition by the key and filter to one shard, or use the analytics API |
| `set_operation` | `UNION`, `INTERSECT` or `EXCEPT` across shards | Run the parts separately |
| `aggregate` | An aggregate the gateway can't combine exactly, such as `WITH ROLLUP`, `MIN` or `MAX` of an `ENUM`, or a sum beyond 38 digits | Group by the key, or use the analytics API |
| `ordering` | An order the gateway can't reproduce: `GEOMETRY`, `ENUM`, an unknown collation, a non-deterministic expression | Order by plain columns |
| `limited_dml` | `UPDATE` or `DELETE` with `ORDER BY` or `LIMIT` over several shards | Add the key, or select the ids first |
| `multi_table_dml` | A multi-table `UPDATE` or `DELETE` that isn't on one shard | Split it into single-table statements |
| `insert_select` | `INSERT ... SELECT` into a sharded table that can't be planned | Insert in batches from the application |
| `upsert_unique` | An upsert or `REPLACE` on a unique key that lacks the distribution key | Include the key in the unique key |
| `key_change` | An `UPDATE` that changes a row's distribution key | Delete and insert the row |
| `variables` | Assigning a user variable across shards | Compute in the application |
| `found_rows` | `SQL_CALC_FOUND_ROWS` across shards | Run a separate `COUNT(*)` |
| `auto_increment`, `insert_key` | An `INSERT` row without a usable key value | Supply the key as a literal of the key's type |
| `lookup`, `foreign_key` | A write form that would leave a unique lookup index or a cross-group foreign key unchecked, such as `INSERT IGNORE` or a non-literal key | Use plain `INSERT` with literal values |
| `cross_cluster` | A write, or a join with an unsharded table, needs shards on several clusters at once | See [cross-shard transactions](#cross-shard-transaction-errors) |
| `schema_skew` | Shards answered with different columns during a schema change | Retry once the change finishes |
| `routine` | A stored function or UDF in a statement across shards | Call it on one shard, or compute in the application |
| `fulltext_relevance` | `MATCH ... AGAINST` relevance compared across shards | Use [search indexes](../data-models/search.md) |
| `gather` | A gathered read the session can't run | See the detail in the message |
| `parse` | The router's parser doesn't read the statement | Simplify the syntax |
| `unsupported` | A form the router doesn't distribute, including `EXPLAIN FORMAT=SPRIG` over unsharded tables | Rewrite, or run on one shard |

Some read shapes that can't run shard by shard are *gathered*: the gateway copies the rows it needs (up to `router.gather.max_rows`, 100,000) and runs the statement itself, or hands it to the analytic engine under `router.analytics.mode`. See [Sharding in SQL](../sql/sharding.md).

## Cross-shard transaction errors

A transaction may touch shards on several clusters. SprigDB commits it locally when only one cluster wrote. When two or more wrote, it uses XA two-phase commit, with one cluster as the *last resource* that commits without preparing.

Not every cluster supports XA. A cluster has XA when it is a single node or a primary with replicas whose binlog survives failover. **Galera clusters have no XA**, and neither has a MariaDB 12.3 cluster with replicas that stores its binlog in InnoDB. `sprigctl cluster show <id>` shows the cluster's capability profile.

### The non-XA policy

At most one cluster without XA can take part in a transaction's writes, as the last resource. What happens with a second one depends on `txn.non_xa_policy`:

| Value | Behaviour |
|---|---|
| `reject` (default) | The write that would add a second cluster without XA fails with `ER_SPRIG_CROSS_CLUSTER_WRITE_REJECTED` (39440). The statement didn't run; the transaction is still open |
| `best_effort` | Clusters without XA commit one after another. If one fails after others committed, the client gets `ER_SPRIG_PARTIAL_COMMIT` (39442), naming what committed |
| `sprig_2pc` | Branches on clusters without XA record their writes, so the gateway can prepare them as *held branches* and replay them if needed. Statements that don't replay to the same result are refused at `COMMIT` |

Keep `reject` unless you accept the trade-off of the others. The real fix is placement: keep data that is written together in one table group, so it lands on one cluster.

### Errors

| Error | Cause | What to do |
|---|---|---|
| `ER_SPRIG_CROSS_CLUSTER_WRITE_REJECTED` (39440) | A write would add a second cluster without XA under `reject` | Commit before writing to the other cluster, co-locate the tables, or change the policy |
| `ER_SPRIG_TOO_MANY_BRANCHES` (39444) | The transaction would touch more than `txn.max_branches` (32) clusters | Split it into smaller transactions |
| `ER_SPRIG_DTX_ROLLED_BACK` (39443) | A distributed transaction was rolled back, for example because a prepare failed within `txn.prepare_timeout_ms` | Retry the transaction |
| `ER_SPRIG_COMMIT_OUTCOME_UNKNOWN` (39441) | The connection to a cluster was lost during `COMMIT`; the gateway closes the connection | Reconnect, check whether the changes are there, and repeat only what is missing |
| `ER_SPRIG_PARTIAL_COMMIT` (39442) | Under `best_effort`, only some clusters committed | Repair the missing part from the message |
| `ER_SPRIG_HELD_BRANCH_STATEMENT` (39446) | Under `sprig_2pc`, a statement can't be replayed exactly: `UUID()`, unseeded `RAND()`, `SYSDATE()`, `LIMIT` without `ORDER BY` in a write. The transaction is rolled back | Compute such values in the application and pass them as literals |
| `ER_SPRIG_PHASE2_PENDING` (39449) | The session's previous distributed commit is still being finished on the cluster this statement needs | Retry the statement |
| 1205 | A lock wait over `txn.distributed_lock_wait_timeout_seconds` (10) in a transaction with two branches | Retry; keep distributed transactions short |

### In-doubt transactions

A transaction is *in doubt* when its branches were prepared but the gateway died before finishing them. The resolver on the controller finishes them on its own, after they are `txn.resolver_min_age_seconds` (30) old. To see and, if needed, resolve them:

```sh
sprigctl txn list --tenant acme
sprigctl txn show <dtid>
sprigctl txn resolve <dtid>
```

`sprig_txn_in_doubt_branches` and `sprig_txn_oldest_prepared_age_seconds` show how many there are and how old the oldest is. A prepared branch holds its row locks, so an old one blocks other writers. `ER_SPRIG_TXN_NOT_IN_DOUBT` (39447) means nothing is prepared for that transaction any more.

## Related

- [Shards and table groups](../concepts/shards-and-table-groups.md)
- [Sharding in SQL](../sql/sharding.md)
- [Backups and moves](../operate/backups-and-moves.md)
- [Sharding advisor](../performance/sharding-advisor.md)
- [Reading SprigDB errors](reading-errors.md)
