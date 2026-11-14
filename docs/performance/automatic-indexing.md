---
title: Working with automatic indexes
audience: [dba, operator]
sidebar:
  order: 1
---

# Working with automatic indexes

SprigDB watches the statements your applications run, designs secondary indexes for the slow ones, proves each index on a copy of the data, and builds it shard by shard. This page covers the day-to-day work: seeing what it built, approving or dismissing what it proposes, freezing an index, reading the evidence, finding out why an index wasn't built, and tuning the thresholds and budgets. For the idea behind it, see [Automatic indexing](../concepts/automatic-indexing.md).

## The life of an automatic index

Every automatic index is named `sprig_auto_<8 hex digits>` and moves through these states:

| State | Meaning |
|---|---|
| `validating` | Being built on a validation node (a replica, or a desynced Galera node) and replayed against sampled statements |
| `recommended` | Stopped with its DDL and evidence, waiting for a person (recommend or approve mode, or advice) |
| `rolling_out` | Being built shard by shard |
| `observing` | Built everywhere; watched for `auto_index.observe_days` |
| `active` | Kept |
| `retiring` | Made `IGNORED` on every shard, so the optimizer stops using it; dropped if nothing gets slower |
| `dropping` | Being dropped on each shard |
| `dropped` | Gone; the row stays as history |
| `rejected` | Validation refused it |
| `failed` | Its rollout failed three times |
| `frozen` | Pinned by a person; nothing changes it until it is unpinned |

A dropped index records why it was dropped:

| Drop reason | Why |
|---|---|
| `no_gain` | Its target statements didn't improve by `auto_index.min_improvement_pct` during observation |
| `regression` | A statement whose plan uses it got slower than `auto_index.max_regression_pct` |
| `write_cost` | Writes got slower and the measured benefit was less than twice the measured write cost |
| `unused` | Over `auto_index.usage_sample_reads` reads it served less than `auto_index.min_usage_ratio` of them, and saved no more than it cost |
| `superseded` | A user index equals or extends it on every shard |
| `replaced` | A better automatic index took its place in the table's budget |
| `budget` | The table's budget was lowered below what it uses |
| `index_limit` | A user statement needed room under MariaDB's limit of 64 indexes per table |
| `table_dropped` | The table was dropped |
| `expired` | A recommendation nobody acted on for `auto_index.recommend_ttl_days` |
| `drift` | The reconciler found it changed or missing on a shard |
| `operator` | Someone dropped it |
| `dismissed` | Someone turned the recommendation down |

A definition dropped as `no_gain`, `regression`, `write_cost`, `operator` or `dismissed`, or rejected by validation, is not proposed again for `auto_index.recreate_block_days` (14 days). That stops SprigDB rebuilding an index a person removed, and stops it building and dropping the same index in a loop.

## See automatic indexes

### In SQL

Automatic indexes are hidden from `SHOW INDEX`, `SHOW CREATE TABLE` and `information_schema.STATISTICS`, so schema tools and migrations never see indexes they didn't create. To see them in a session, set the session flag on its own:

```sql test
CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  status VARCHAR(20) NOT NULL,
  total DECIMAL(10,2) NOT NULL,
  placed_at DATETIME NOT NULL
);
SET @sprig_show_auto_indexes = 1;
SHOW INDEX FROM orders;
```

The flag is the gateway's, not MariaDB's. It accepts `1`, `0`, `ON`, `OFF`, `TRUE`, `FALSE` or a quoted `'1'`, and must be the only assignment in its `SET` statement. `CREATE TABLE ... LIKE` never copies automatic indexes, whatever the flag says.

You can't create, rename or alter an index whose name starts with `sprig_auto_`: the statement fails with `ER_SPRIG_RESERVED_NAME` (39050).

```sql test-error
CREATE INDEX sprig_auto_0000abcd ON orders (status);
```

 You may drop one with `DROP INDEX`, but only while `@sprig_show_auto_indexes = 1`; prefer `sprigctl index drop` or Studio, which also block the same index from coming back.

### In Studio

Open **Performance**, then **Automatic indexing**. The page has four tabs:

- **Indexes**: every automatic index of your tenant with its object, columns, state, estimated net benefit (ms saved per hour) and size. Tick **Include dropped and rejected** to see history. Select an index name to open its evidence.
- **Recommendations**: indexes waiting for approval.
- **Fingerprints**: the statements that take the most time (see [Workload](workload.md)).
- **Explain a query**: why an index was or wasn't built for one fingerprint.

Statements you run from Studio don't count toward automatic indexing unless `auto_index.include_studio` is `true`, so exploring data in Studio doesn't build indexes.

Anyone signed in to the tenant can view these pages. The action buttons appear only for users who could create an index in the index's database: `INDEX` or `ALTER` on that database or on `*.*`, or `SPRIG_TENANT_ADMIN`. Others see "view only".

### With sprigctl

Operators use `sprigctl index`, which talks to the controller's Admin API:

```sh
sprigctl index list --tenant acme
sprigctl index list --tenant acme --db shop --table orders --state active
sprigctl index list --tenant acme --all            # also dropped and rejected
sprigctl index show sprig_auto_3f9a01c2 --tenant acme
```

```text
ID   TABLE        NAME                  COLUMNS               STATE     NET MS/H  SIZE
17   shop.orders  sprig_auto_3f9a01c2   customer_id, status   active    4210      38.2 MiB
21   shop.events  sprig_auto_9b77e0d4   kind, created_at      observing 655       12.0 MiB
```

`--state` takes any state from the table above. `--tenant` may come from `SPRIGCTL_TENANT`. Every command accepts `-o json` or `-o yaml`.

An index is named by its id from `index list`, or as `[<db>.][<table>.]sprig_auto_<hex>`. User and migrated indexes that automatic indexing retired, recommended for removal, or that someone pinned, also appear in the list, named `[<db>.][<table>.]<name>` and marked with their origin.

### With the REST API

The same views are available to a tenant's users over HTTP, for their own tenant only:

| Method and path | Does |
|---|---|
| `GET /v1/indexing/indexes?db=&table=&state=&all=` | Lists the tenant's automatic indexes |
| `GET /v1/indexing/recommendations?db=&table=` | Lists recommendations awaiting approval |
| `GET /v1/indexing/indexes/{index}` | One index with its evidence, validations and history |
| `POST /v1/indexing/indexes/{index}:{action}` | `approve` (`apply`), `reject` (`dismiss`), `pin` (`freeze`), `unpin` (`unfreeze`) or `drop` |
| `GET /v1/indexing/queries/{fingerprint}:explain` | Why an index was or wasn't built for a fingerprint |
| `GET /v1/indexing/decisions/{id}` | One decision-log entry |

```sh
curl -s https://gw.example.com:8443/v1/indexing/indexes?db=shop \
  -H 'Authorization: Bearer <token>'
```

An action without the privilege to create the index fails with MySQL error 1142 (`INDEX command denied to user ...`).

## Approve or dismiss a recommendation

An index stops at `recommended` instead of being built when:

- `auto_index.mode` is `recommend` or `approve` for its table;
- it came from the fast large-scan trigger on a table larger than `auto_index.scan_recommend_above_gb` (100 GB);
- the rollout finds no safe way to build it (for example, a large table on a Galera cluster).

With `auto_index.recommend_validate` on (the default) and a validation node available, a recommendation has already been built on a replica and replayed, so its evidence includes measured before-and-after latencies. Otherwise the evidence is an estimate.

To roll a recommendation out:

```sh
sprigctl index recommendations --tenant acme --db shop
sprigctl index approve 21 --tenant acme --reason "checked with the orders team"
```

```text
approve sprig_auto_9b77e0d4 on shop.events (tenant acme): recommended -> rolling_out
```

To turn it down:

```sh
sprigctl index reject 21 --tenant acme --reason "table is being redesigned"
```

`approve` (alias `apply`) moves the index to `rolling_out`; the rollout then builds it under the usual build windows and slots. `reject` (alias `dismiss`) drops the recommendation with reason `dismissed`, which blocks the same definition for `auto_index.recreate_block_days`. Advice can't be approved this way; apply it by hand. A recommendation nobody acts on expires after `auto_index.recommend_ttl_days` (14).

In Studio, the **Approve** and **Dismiss** buttons on the Indexes and Recommendations tabs do the same, and ask for a reason that is recorded with the decision.

### Through the approvals inbox

When `auto_index.mode` is `approve` or `recommend`, each recommendation also appears in the tenant's approvals inbox as an item of kind `index.create`, with notifications, timers and escalation as set by the `approvals.*` settings. Approving the item there rolls the index out exactly as `sprigctl index approve` does:

```sql
SHOW SPRIG RECOMMENDATIONS FOR TABLE shop.orders;
SPRIG APPROVE RECOMMENDATION 17 REASON 'reviewed';
SPRIG IGNORE RECOMMENDATION 18 FOR 30 DAYS REASON 'not now';
SPRIG SNOOZE RECOMMENDATION 19 FOR 48 HOURS;
```

The recommendation numbers here are the inbox's, not index ids. See [Approvals](../concepts/approvals.md) for the inbox, its policies and its notifications.

### What a concurrent change does

Every action is a conditional state change. If the evaluation, rollout or observation loop changed the index while your call ran, the call fails with `ER_SPRIG_AUTO_INDEX_STATE_INVALID` (39202) and changes nothing; look at the index again and retry. An unknown index fails with `ER_SPRIG_AUTO_INDEX_NOT_FOUND` (39201).

## Freeze, unfreeze, drop and restore

To keep an index exactly as it is, whatever the observer would decide, freeze it:

```sh
sprigctl index pin sprig_auto_3f9a01c2 --tenant acme --reason "month-end report"
sprigctl index unpin sprig_auto_3f9a01c2 --tenant acme
```

`pin` (alias `freeze`) moves any live index to `frozen` and remembers its prior state; `unpin` (or `pin --off`) returns it to that state. A frozen index is never retired, replaced or dropped, and still counts against the table's budget.

To remove an automatic index from every shard:

```sh
sprigctl index drop sprig_auto_3f9a01c2 --tenant acme --reason "replaced by a user index" --yes
```

`drop` works on an index that is `rolling_out`, `observing`, `active`, `retiring` or `failed`. It asks for confirmation unless you pass `--yes`, and the same definition is blocked for `auto_index.recreate_block_days`. `--dry-run` prints what would happen and changes nothing.

`restore` undoes a retirement:

```sh
sprigctl index restore shop.orders.idx_orders_placed --tenant acme
```

For a `retiring` index it makes the index visible again (`NOT IGNORED` on every shard) and pins it. For a user or migrated index that automatic indexing dropped for disuse, it recreates the index from its saved definition and pins it. A pinned user index is never judged again until you unpin it.

Every action is recorded in the decision log with the actor (`op:<name>` from `--actor`, `SPRIGCTL_ACTOR` or `$USER`; `studio:<tenant>.<user>` from Studio) and your reason.

## Read the evidence behind an index

`sprigctl index show` prints everything SprigDB knew when it decided:

```sh
sprigctl index show 17 --tenant acme
```

```text
index:         sprig_auto_3f9a01c2 (17)
tenant:        acme
table:         shop.orders
kind:          btree (sql)
columns:       customer_id, status
state:         active since 2026-11-03 09:12:44
estimate:      B 4630 ms/h, Wc 420 ms/h, net 4210 ms/h, 38.2 MiB
shards:        s0 built, s1 built
fingerprints:
  FINGERPRINT       ROLE     RUNS  P95 MS  MEAN MS
  9c1e5f0a77b2d310  primary  1820  240     118
candidate:     88 (run 412): selected
  selectivity:   customer_id 2.10e-5 (histogram), status 2.00e-1 (histogram)
  cost:          B 4630 ms/h, Wc 420 ms/h, net 4630 ms/h, 38.2 MiB, build 41 s
validation:    5 replica on c1/db-2: accepted, target p95 240 -> 9.8 ms (95.9% better), worst regression 2.1%, 120 statements
history:
  ...
```

How to read it:

- **estimate**: `B` is the time the index saves per hour across the statements it serves, `Wc` the time it adds to writes per hour, and `net` the difference. Size is the estimated (or, once built, measured) size across shards.
- **fingerprints**: the query shapes it was built for. `primary` shapes caused it; `merged` shapes are served by it because their candidate was folded into this longer one.
- **selectivity**: the share of rows one value matches, from MariaDB's histograms or a default when there are none. Evidence records selectivities, never values from your data.
- **validation**: how it was proven. `replica` means it was built on a replica with binary logging off, the sampled statements were replayed with and without it, and it was dropped again. `galera_rsu` is the same on a desynced Galera node. `estimate_canary` is used where there is neither: an `EXPLAIN`-checked estimate, then a real build on one shard, observed for `auto_index.canary_hours`.
- **history**: every state change with its time and detail.

In Studio, select the index name to see the same evidence: the DDL, benefit and write cost, the rollout state of each shard, the target fingerprints before and after, the validations and the history.

## Find out why an index wasn't built

Start from the statement's fingerprint (16 hex digits; find it on the Fingerprints tab or with `GET /v1/workload/fingerprints`) and ask:

```sh
sprigctl explain query 9c1e5f0a77b2d310 --tenant acme
```

```text
fingerprint 9c1e5f0a77b2d310 in tenant acme (evidence since 2026-10-13 00:00:00)
  no candidate on shop.orders: a predicate wraps its column in LOWER(), so no index can serve it

policy of shop.orders: on: validate, then roll out (a large-scan candidate on a table above auto_index.scan_recommend_above_gb stops at recommended)
  auto_index.min_runs                    50 (default)
  ...
```

The summary gives the latest evaluation's verdict for each table the statement touches. Each candidate shows the cost gates it passed or failed (`pass` or `FAIL`), its validations and the index it became. The policy block shows the resolved settings for the table and where each value came from.

The common answers:

| Message | What to do |
|---|---|
| It didn't qualify, or no evaluation has run since it did | The statement isn't slow or heavy enough yet, or the next evaluation (every `auto_index.eval_interval_minutes`) hasn't run |
| An existing index, `<name>`, already serves it | Nothing; the index exists |
| `<name>` should serve it but the optimizer doesn't use it | Statistics are refreshed before the next check |
| `<name>` serves it on paper, but the optimizer doesn't use it even with fresh statistics | No duplicate is built; look at the statement's plan |
| A predicate wraps its column in `LOWER()` (or another function) | Rewrite the predicate, or add a generated column and index it |
| Its only filter is `LIKE '%...'` | A B-tree can't serve it; use a [search index](../data-models/search.md) |
| No predicate an index can serve | Nothing an index can do |
| Its net benefit is below `auto_index.min_net_ms_per_hour` | Too little to gain; lower the threshold for this table if you disagree |
| Its benefit is less than `auto_index.min_benefit_ratio` times its write cost | The table is written much more than this statement reads it |
| Building it would take longer than `auto_index.max_build_hours` | Raise the limit for this table, or create the index yourself |
| Its build wouldn't pay back within `auto_index.payback_hours` | As above |
| Not predicted to improve by `auto_index.min_improvement_pct` | Validation or the estimate showed too small a gain |
| The same index was dropped recently | `auto_index.recreate_block_days` holds it back |
| Its fingerprints were decided within `auto_index.cooldown_hours` | Wait for the cooldown |
| The table's budget or the day's cap is used up | Raise `auto_index.max_per_table`, `auto_index.max_storage_pct` or `auto_index.max_new_per_day` |
| No rows-examined probes yet | It is watched from now on; the next evaluation decides |
| The table has no statistics yet | Statistics are collected first |
| `auto_index.mode` is off for the table | Turn it on, or create the index yourself |

`explain query` reads the evidence kept for `workload.retention_days` (30 days). It can't yet tell you which threshold a statement missed when it didn't qualify at all; compare its runs and p95 on the Fingerprints tab with the trigger settings below.

## What makes a statement qualify

Each evaluation looks at the tenant's last `auto_index.window_hours` (24) of workload. A fingerprint qualifies for a table when any of three triggers holds, using that table's settings:

| Trigger | Condition |
|---|---|
| Slow | At least `auto_index.min_runs` (50) runs, and a p95 above `auto_index.p95_threshold_ms` (100 ms) |
| Heavy | Among the tenant's `auto_index.top_n` (10) fingerprints by total time, with at least `auto_index.min_examined_samples` (5) probed runs that examined more than `auto_index.min_examined_per_returned` (100) rows per row returned |
| Large scan | At least `auto_index.scan_min_runs` (5) runs that each examined `auto_index.scan_min_examined` (1,000,000) rows and returned at most `auto_index.scan_max_returned_ratio` (0.01) of them, from at least `auto_index.scan_min_sessions` (2) sessions |

The large-scan trigger catches a full scan within minutes rather than waiting for 50 runs. On a table above `auto_index.scan_recommend_above_gb` it only recommends.

Then each candidate passes the cost gates, is selected under the table's budget, is validated, and is rolled out:

```text
qualify ─► candidates ─► cost gates ─► budget ─► validate ─► roll out ─► observe ─► active
                                                    │                       │
                                         recommend mode stops here    retire if it doesn't help
```

## Choose a mode

`auto_index.mode` can be set for the whole tenant, a database or a single table:

| Mode | Does |
|---|---|
| `on` | Creates, validates, rolls out and drops indexes on its own (default) |
| `approve` | Runs up to validation, stops at `recommended`, and raises an item in the approvals inbox; an approval rolls it out |
| `recommend` | The same, for people who review recommendations with `sprigctl index` or Studio |
| `off` | Evaluates nothing new; existing automatic indexes are kept as they are |

```sql test
-- Review every index on one sensitive table before it is built.
ALTER TABLE orders SPRIG SET (auto_index.mode = 'approve');

-- Allow more automatic indexes on it, and a bigger daily cap.
ALTER TABLE orders SPRIG SET (auto_index.max_per_table = 8, auto_index.max_new_per_day = 10);

-- See every resolved auto_index setting for the table, with its source.
SHOW SPRIG SETTINGS LIKE 'auto_index.%' FOR TABLE orders;

-- Go back to the inherited values.
ALTER TABLE orders SPRIG RESET (auto_index.mode, auto_index.max_per_table, auto_index.max_new_per_day);
```

To turn automatic indexing off for a whole database, name it (or omit the name for the current database):

```sql
ALTER DATABASE shop SPRIG SET (auto_index.mode = 'off');
```

Setting a value on a table needs `ALTER` on it; on the tenant, `SPRIG_SETTINGS_ADMIN`. Operators can do the same with `sprigctl settings set`. See [Settings and scopes](../concepts/settings-and-scopes.md).

A separate setting, `auto_index.retire_user_indexes` (`on`, `recommend` or `off`), applies the usage rule to indexes you created yourself and to migrated ones. With `on`, a user index that serves almost none of the reads is made `IGNORED`, watched, then dropped, and `sprigctl index restore` brings it back. Set it to `recommend` if you want to decide yourself, or `off` to leave user indexes alone.

## Tune thresholds and budgets

Every `auto_index.*` setting resolves for the table a candidate indexes: table, database, tenant and its ancestors, cluster, data centre, global, then the default. Set a value at the narrowest scope that needs it. Keys about shared cluster resources (`max_builds_per_cluster`, `build_window`, `abort_replica_lag_seconds`) can only be tightened at a table, database or tenant scope, never loosened. The `scan_*` keys accept global, tenant, database and table only. Lowering a budget drops nothing at once: the next evaluation sees the table over budget and retires one index per evaluation.

### Triggers

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `auto_index.window_hours` | int | 24 | Workload window each evaluation reads |
| `auto_index.min_runs` | int | 50 | Runs needed for the slow trigger |
| `auto_index.p95_threshold_ms` | float | 100 | p95 above which a statement is slow |
| `auto_index.top_n` | int | 10 | Fingerprints by total time considered for the heavy trigger |
| `auto_index.min_examined_samples` | int | 5 | Probed runs needed for the heavy trigger |
| `auto_index.min_examined_per_returned` | float | 100 | Rows examined per row returned for the heavy trigger |
| `auto_index.scan_min_runs` | int | 5 | Large-scan runs needed |
| `auto_index.scan_min_examined` | int | 1000000 | Rows examined by one large-scan run |
| `auto_index.scan_max_returned_ratio` | float | 0.01 | Largest share of examined rows a large scan returns |
| `auto_index.scan_min_sessions` | int | 2 | Distinct sessions among the large-scan runs (at most 8) |
| `auto_index.scan_recommend_above_gb` | float | 100 | Above this table size, the large-scan trigger only recommends |
| `auto_index.include_studio` | bool | false | Count statements run from Studio |
| `auto_index.cooldown_hours` | int | 24 | Time before a fingerprint is decided again |
| `auto_index.eval_interval_minutes` | int | 60 | How often a tenant is evaluated, and how often reads are counted for usage |

### Design and cost

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `auto_index.max_cover_columns` | int | 3 | Columns added to make an index covering |
| `auto_index.max_key_bytes` | int | 512 | Longest automatic index key |
| `auto_index.lookup_cost_factor` | float | 2 | Extra cost of a primary-key lookup per index entry |
| `auto_index.maintenance_cost_ms` | float | 0.05 | Cost of one index entry change on write |
| `auto_index.min_net_ms_per_hour` | float | 100 | Smallest net benefit worth building |
| `auto_index.min_benefit_ratio` | float | 2 | Benefit must be at least this many times the write cost |
| `auto_index.payback_hours` | float | 24 | The build must pay for itself within this |
| `auto_index.build_rate_mb_per_sec` | float | 20 | Build rate assumed before one is measured |
| `auto_index.max_build_hours` | float | 6 | Longest build allowed |

### Budgets

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `auto_index.max_per_table` | int | 5 | Automatic indexes per table |
| `auto_index.max_storage_pct` | float | 20 | Extra storage per table, as a percentage of the table |
| `auto_index.min_storage_budget_mb` | float | 64 | Storage budget per table never falls below this |
| `auto_index.max_new_per_day` | int | 5 | New indexes built per tenant per day; set on a database or table it also caps that database or table |
| `auto_index.max_builds_per_cluster` | int | 1 | Concurrent index builds and validations per cluster (operator only) |

### Validation

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `auto_index.replay_samples` | int | 20 | Sampled statements replayed per fingerprint |
| `auto_index.replay_rounds` | int | 3 | With-and-without rounds per sample |
| `auto_index.replay_timeout_seconds` | int | 10 | Statement limit during replay |
| `auto_index.regression_fingerprints` | int | 20 | Other fingerprints on the table replayed to catch regressions |
| `auto_index.min_improvement_pct` | float | 30 | p95 improvement needed to accept an index (and to keep it after observation) |
| `auto_index.max_regression_pct` | float | 10 | Largest slowdown of any replayed or observed statement |
| `auto_index.estimate_margin` | float | 1.5 | How much stricter predictions are without a replica |
| `auto_index.canary_hours` | int | 24 | Observation of the canary shard without a replica |
| `auto_index.recommend_validate` | bool | true | Validate before stopping at a recommendation |
| `auto_index.recommend_ttl_days` | int | 14 | Lifetime of a recommendation |

Replay needs parameter samples, which workload capture keeps only when the gateway has a sample key. See [Workload](workload.md).

### Rollout

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `auto_index.build_window` | text | `auto` | `auto` (the least busy four hours of the past seven days) or `HH:MM-HH:MM` in UTC |
| `auto_index.rollout_pause_minutes` | int | 10 | Pause between shards |
| `auto_index.ddl_lock_wait_seconds` | int | 5 | Lock wait of the index DDL |
| `auto_index.abort_replica_lag_seconds` | int | 300 | A build stops when a replica falls further behind |

Builds run on each shard's writer as an online `ALTER TABLE ... ADD INDEX ..., ALGORITHM=INPLACE, LOCK=NONE`. Your DDL always wins: if you alter the table during a build, the build is killed and retried after `auto_index.cooldown_hours`. A build that is still waiting for its first lock while client statements queue behind it is killed within about a second and retried later. Lock timeouts are retried five times, 30 seconds apart; replica lag or a degraded cluster postpones the build; three other failures move the index to `failed`. No build runs while its shard is being moved. On Galera clusters, tables up to 64 MiB are built in total order, and larger ones are only recommended.

### Observation and retirement

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `auto_index.observe_days` | int | 7 | Watch period after a build |
| `auto_index.usage_sample_reads` | int | 100000 | Reads per usage judgement, and reads watched while an index is `IGNORED` |
| `auto_index.min_usage_ratio` | float | 0.05 | Share of reads an index must serve to be kept regardless of its value |
| `auto_index.retire_user_indexes` | text | `on` | `on`, `recommend` or `off` for user and migrated indexes |
| `auto_index.recreate_block_days` | int | 14 | Block on re-proposing a definition dropped as harmful, by a person, or rejected |

Retirement is cautious. The index is first made `IGNORED`, which is instant and reversible. If any statement it served gets slower, it is made visible again and returns to `active`. Only after `auto_index.usage_sample_reads` reads on the table with no regression is it dropped.

### Other index kinds

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `auto_index.vector_ann` | text | `recommend` | `on`, `approve`, `recommend` or `off` for approximate-nearest-neighbour vector indexes |
| `auto_index.vector_min_recall` | float | 0.95 | Recall at k needed to accept an ANN index |

On collections, automatic indexing indexes document paths (shown as paths, such as `status, qty`, rather than columns), up to three paths per index. Each path is indexed as a value-type column plus a number and a string column, whatever mix of types its values have. Paths that cross arrays get no index yet.

Automatic indexing doesn't yet propose global lookup indexes for statements that scatter over shards, or wildcard indexes for collections filtered on many different paths.

## Your own indexes and the 64-index limit

MariaDB allows 64 indexes per table. When a statement of yours would pass that limit, the gateway first drops the lowest-value automatic indexes on the table (reason `index_limit`), so your DDL never fails because of indexes SprigDB added. An automatic index that one of your indexes equals or extends is dropped as `superseded`.

## Watch it with metrics

| Metric | What it tells you |
|---|---|
| `sprig_advisor_autoindex_runs_total` | Evaluations run |
| `sprig_advisor_autoindex_candidates_total{status}` | Candidates by outcome |
| `sprig_advisor_autoindex_build_seconds{method}` | Build durations |
| `sprig_advisor_autoindex_drops_total{reason}` | Drops by reason |
| `sprig_advisor_autoindex_churn_total` | Definitions re-created within 30 days of a drop; a steady rise means settings are fighting each other |

See [Metrics](../reference/metrics.md).

## Related

- [Automatic indexing concepts](../concepts/automatic-indexing.md)
- [Workload](workload.md)
- [Query plans](query-plans.md)
- [Approvals](../concepts/approvals.md)
- [Settings reference](../reference/settings.md)
