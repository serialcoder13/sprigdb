---
title: Automatic indexing
audience: [user, dba]
sidebar:
  order: 5
---

# Automatic indexing

SprigDB creates secondary indexes from the statements it sees, proves each one on a copy of your data, builds it online shard by shard, and drops it again if it stops helping. Read this page to understand how it decides, what the indexes look like, and how to keep control. The day-to-day work, with every setting, is in [Working with automatic indexes](../performance/automatic-indexing.md).

## What it does, in one picture

```text
workload ─► qualify ─► design ─► cost gates ─► budget ─► validate ─► roll out ─► observe ─► active
                                                            │                       │
                                       approve / recommend stops here     retire if it doesn't help
```

The controller in each data centre runs one evaluation per tenant every `auto_index.eval_interval_minutes` (60). Builds and validations run on workers.

## How it decides

Every gateway records each statement's **fingerprint** (its shape with the literal values removed), how often it runs, its latency, and how many rows it examined and returned. See [Workload](../performance/workload.md).

An evaluation reads the last `auto_index.window_hours` (24) of that workload. A fingerprint qualifies for a table when one of three triggers holds:

| Trigger | Condition (defaults) |
|---|---|
| Slow | At least `auto_index.min_runs` (50) runs with a p95 above `auto_index.p95_threshold_ms` (100 ms) |
| Heavy | Among the tenant's `auto_index.top_n` (10) fingerprints by total time, examining more than `auto_index.min_examined_per_returned` (100) rows per row returned |
| Large scan | At least `auto_index.scan_min_runs` (5) runs that each examined `auto_index.scan_min_examined` (1,000,000) rows and returned at most 1 % of them, from at least two sessions |

The large-scan trigger catches a full scan of a big table within minutes, without waiting for 50 runs.

Statements you run from Studio don't count, unless `auto_index.include_studio` is on, so exploring data doesn't build indexes.

## How an index is designed

For each qualifying fingerprint and each table it reads, SprigDB builds candidate indexes from the statement's predicates and the column statistics MariaDB keeps:

1. **Equality columns first**, most selective first: `=`, `IS NULL`, `IN` lists of up to 32 values, and the table's join columns.
2. **Then one range column**, the most selective: `<`, `BETWEEN`, `LIKE 'abc%'`, `IS NOT NULL`, longer `IN` lists.
3. **Then the `ORDER BY` or `GROUP BY` columns**, when they can be read in index order, so a sort or grouping needs no extra pass.
4. **Covering columns**, up to `auto_index.max_cover_columns` (3), when the statement reads many rows and the extra columns let it skip the table lookup.

Some rules that follow from how InnoDB works:

- An `ORDER BY` of the primary key after equalities needs no extra column, since secondary index entries end with the primary key.
- `ORDER BY … DESC` gets an ascending index; a backward scan serves it. Only mixed directions get descending parts.
- `BLOB`, `TEXT`, `JSON` and geometry columns are never key columns.
- A candidate that would still read more than 20 % of the table, and gives no useful order, is dropped.
- A key is never longer than `auto_index.max_key_bytes` (512).

Candidates for the same table are merged where one index can serve several statements, and checked against the indexes that already exist. If an existing index already serves the statement, nothing is built.

A predicate that wraps its column in a function, such as `WHERE LOWER(email) = …`, or a `LIKE '%text'`, can't use a B-tree index, and SprigDB doesn't try. `sprigctl explain query <fingerprint>` tells you when that is the reason.

## The cost gates and budgets

Each candidate is priced: the time it would save per hour (its benefit) against the time it adds to writes (its write cost) and its size. It is built only if:

- its net benefit is at least `auto_index.min_net_ms_per_hour` (100 ms per hour);
- its benefit is at least `auto_index.min_benefit_ratio` (2) times its write cost;
- its build would finish within `auto_index.max_build_hours` (6) and pay for itself within `auto_index.payback_hours` (24).

Each table has a budget: at most `auto_index.max_per_table` (5) automatic indexes and `auto_index.max_storage_pct` (20 %) extra storage. Each tenant gets at most `auto_index.max_new_per_day` (5) new indexes a day. A better candidate can replace a weaker automatic index when the budget is full.

## Validation on a replica

Before an index reaches your writers, SprigDB proves it:

1. It builds the index on a **validation node**: a replica taken out for the build, with binary logging off, or a desynced Galera node (three or more nodes). The capability profile says which a cluster offers; see [Clusters and data centres](clusters-and-data-centres.md).
2. It replays sampled real statements with and without the index: the target statements, and up to 20 other statements on the same table to catch regressions.
3. It accepts the index only if the target statements' p95 improves by at least `auto_index.min_improvement_pct` (30 %) and no replayed statement gets more than `auto_index.max_regression_pct` (10 %) slower.
4. It drops the index from the validation node again.

On a cluster with no validation node, SprigDB uses a stricter estimate (`auto_index.estimate_margin`), checked with `EXPLAIN`, then builds on one shard first as a canary and watches it for `auto_index.canary_hours` (24).

## Rollout

An accepted index is built **shard by shard**, on each shard's writer, as an online `ALTER TABLE … ADD INDEX …, ALGORITHM=INPLACE, LOCK=NONE`:

- Builds run in the build window (`auto_index.build_window`: by default the least busy four hours of the past week), at most `auto_index.max_builds_per_cluster` (1) at a time per cluster, with `auto_index.rollout_pause_minutes` (10) between shards.
- A build stops if a replica falls more than `auto_index.abort_replica_lag_seconds` (300) behind.
- **Your DDL always wins.** If you alter the table during a build, the build is killed and retried later. A build that would make client statements queue behind its lock is killed within about a second.
- No build runs on a shard that is being moved.
- On Galera, small tables are built in total order; larger ones are only recommended.

## Observation and dropping

After the build, the index is watched for `auto_index.observe_days` (7). It is kept if its target statements improved and nothing else got slower. Later, an index that serves less than `auto_index.min_usage_ratio` (5 %) of a table's reads, and saves no more than it costs, is retired.

Retiring is cautious and reversible:

1. The index is made `IGNORED` on every shard, which is instant: the optimizer stops using it, but it still exists.
2. If any statement it served gets slower, it is made visible again.
3. Only after `auto_index.usage_sample_reads` (100,000) reads with no regression is it dropped.

A definition that was dropped as harmful, removed by a person, or rejected by validation isn't proposed again for `auto_index.recreate_block_days` (14), so SprigDB never builds and drops the same index in a loop.

The same usage rule can apply to indexes you created yourself: `auto_index.retire_user_indexes` (`on` by default, or `recommend` or `off`). A retired user index can be brought back with `sprigctl index restore`.

## The hidden `sprig_auto_*` indexes

Every automatic index is named `sprig_auto_` followed by eight hex digits, such as `sprig_auto_3f9a01c2`. They are ordinary MariaDB secondary indexes, but they are **hidden** from `SHOW INDEX`, `SHOW CREATE TABLE` and `information_schema.STATISTICS`, so your schema tools and migrations never see indexes they didn't create. To see them in a session:

```sql test
CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  status VARCHAR(20) NOT NULL,
  placed_at DATETIME NOT NULL
);
SET @sprig_show_auto_indexes = 1;
SHOW INDEX FROM orders;
```

The name prefix is reserved. Creating or renaming an index to a `sprig_auto_` name fails with 39050 `ER_SPRIG_RESERVED_NAME`:

```sql test-error
CREATE INDEX sprig_auto_0000abcd ON orders (status);
```

MariaDB allows 64 indexes per table. If your own DDL would pass that limit, SprigDB first drops its lowest-value automatic indexes on the table, so your statement never fails because of them. An automatic index that one of your indexes equals or extends is dropped as superseded.

## Modes

`auto_index.mode` chooses how far automatic indexing goes. Set it for the tenant, a database or a single table:

| Mode | Does |
|---|---|
| `on` (default) | Designs, validates, builds and drops indexes on its own |
| `approve` | Validates, then stops at `recommended` and raises an item in the approvals inbox; approving it starts the rollout |
| `recommend` | Validates, then stops at `recommended` for you to review in Studio or with `sprigctl index` |
| `off` | Proposes nothing new; existing automatic indexes stay |

```sql test
ALTER TABLE orders SPRIG SET (auto_index.mode = 'approve');
SELECT SETTING_NAME, VALUE, SOURCE_SCOPE
FROM information_schema.SPRIG_SETTINGS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders'
  AND SETTING_NAME = 'auto_index.mode';
```

```text output
SETTING_NAME	VALUE	SOURCE_SCOPE
auto_index.mode	approve	object
```

Some indexes stop at `recommended` whatever the mode: a large-scan candidate on a table above `auto_index.scan_recommend_above_gb` (100 GB), an index that can't be built safely (a large table on Galera), and advice such as wildcard document or search indexes. Approximate-nearest-neighbour vector indexes have their own mode, `auto_index.vector_ann` (`recommend` by default).

See [Approvals](approvals.md) for how modes work across every automatic decision, and [Settings and scopes](settings-and-scopes.md) for how a table's value overrides its database's.

## Where to see what it did

| Where | What |
|---|---|
| Studio, **Performance › Automatic indexing** | Indexes with their state, benefit and size; recommendations; fingerprints; "explain a query". Select an index for its evidence: DDL, benefit and write cost, per-shard rollout, validations, history |
| `sprigctl index` | `list`, `show`, `recommendations`, `approve`, `reject`, `pin`, `unpin`, `drop`, `restore` |
| `sprigctl explain query <fingerprint>` | Why an index was or wasn't built for a statement |
| REST | `GET /v1/indexing/indexes`, `GET /v1/indexing/recommendations`, `GET /v1/indexing/queries/{fingerprint}:explain` |
| SQL, in approve mode | `SHOW SPRIG RECOMMENDATIONS`, `SPRIG APPROVE RECOMMENDATION` |
| Metrics | `sprig_advisor_autoindex_*`: runs, candidates, build times, drops by reason, churn |

Every decision is recorded with its evidence: the fingerprints, the selectivities (never values from your data), the cost estimate and the validation results.

## Related

- [Working with automatic indexes](../performance/automatic-indexing.md)
- [Workload](../performance/workload.md)
- [Query plans](../performance/query-plans.md)
- [Approvals](approvals.md)
- [Settings reference](../reference/settings.md#auto_index)
