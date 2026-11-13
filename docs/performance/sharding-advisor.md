---
title: Sharding advisor
audience: [dba, operator]
sidebar:
  order: 4
---

# Sharding advisor

Before a tenant's data splits across shards for the first time, the sharding advisor decides which tables belong together in a table group and which column each group is split by, its distribution key. A good design keeps each statement and each transaction on one shard. This page explains how the advisor chooses, when it runs, how to read its runs, and how to accept, delay or override its choices with pins.

For table groups, keys and shards in general, see [Shards and table groups](../concepts/shards-and-table-groups.md).

## When the advisor runs

A new tenant has no table groups: every table of a database lives in that database's default schema on the tenant's cluster, and behaves exactly as on MariaDB. The advisor designs table groups once, before the first split. It runs when:

| Cause | When |
|---|---|
| `first_split` | The tenant's default schemas together pass `sharding.split_size_gb` (32 GB), checked every `sharding.check_interval_seconds` (60) |
| `min_shards` | `sharding.min_shards` asks for more than one shard per group from the start |
| `manual` | An operator runs `sprigctl advisor run` |

Once a tenant has table groups, the advisor doesn't redesign them by itself. Changing a group's key after it has split is an online re-key, which operators start with `sprigctl shard rekey` (see [Change a key after the split](#change-a-key-after-the-split)).

## How the advisor chooses

The advisor reads two kinds of input:

- **The schema**: tables, columns, primary and unique keys, declared foreign keys, and sizes, from the tenant's cluster.
- **The workload**: the statements of the last `advisor.window_days` (7) and the transaction footprints, from [workload capture](workload.md). Statements carry no values here; the advisor judges a statement by which columns it fixes with equalities.

It then works in four steps.

1. **Structural design.** Foreign keys form trees. Each tree's root becomes a group's root, keyed by its primary key, and each child joins its parent's group. A child either routes by a column of its own that holds the group's key (a column member, such as `orders.customer_id`) or follows its parent's row through the foreign key (a parent member, such as `order_items` following `orders`).
2. **Key filters.** A candidate key must have a type the router can route (integers, decimals, `CHAR` and `VARCHAR` under one collation, `BINARY` and `VARBINARY`), at least `advisor.min_ndv_per_shard` (100) distinct values per expected shard, and no single value holding more than `advisor.max_top_value_pct` (25%) of the rows. A key that fails would make shards uneven.
3. **Scoring.** Each design is scored by how much of the workload it keeps on one shard. A statement that fixes its table's routing columns, or inserts them, counts in full. One that reaches the shard through a unique key or a foreign key loses `advisor.lookup_penalty` (0.1) per lookup it needs. A write transaction counts `advisor.txn_weight` (2) times a statement, because a transaction across shards costs a distributed commit.
4. **Search.** Starting from the structural design, the advisor tries attaching tables to the groups they are joined with most often, and detaching tables that are rarely used with their group, for up to `advisor.max_moves` (200) steps, and keeps the best design. A join only co-locates two tables when both columns fit the group's key and compare as equal bytes: joining an `INT` column to a `VARCHAR` one, for example, never co-locates.

The advisor also keeps stored code working. A table that a view, procedure, function, package or event names stays in its database's default schema. A table with a trigger moves only together with every table its trigger names. Dynamic SQL that isn't one string literal keeps every table of its schema in place. A run's evidence says which code held which table back.

### Confidence and the verdict

Every run ends with a verdict:

| Verdict | Meaning |
|---|---|
| `apply` | Enough evidence: the design is applied (or, with the advisor in `recommend` or `approve` mode, recommended) |
| `wait` | Not enough evidence yet; the tenant stays as it is and the advisor runs again |
| `recommend` | The design is recorded for a person to apply |
| `no_change` | Nothing to change |

A first design is applied when the window holds at least `advisor.min_observe_hours` (24) of history, at least `advisor.min_units` (10,000) sampled units, and the advisor's confidence reaches `advisor.min_confidence` (0.9). Confidence is the share of 200 resamples of the workload in which the best design still beats the runner-up (1.0 when there is no runner-up). If the evidence still falls short after `advisor.max_wait_hours` (72), the best design is applied anyway, and the run notes that it applied with low confidence.

With `advisor.mode = off`, no runs happen, and a first split uses the structural design from foreign keys alone.

## Choose a mode

| `advisor.mode` | Does |
|---|---|
| `on` | Runs and applies its design (default) |
| `approve` | Runs; an `apply` verdict becomes a recommendation in the approvals inbox (kind `advisor.design`), and an approval applies it |
| `recommend` | Runs; an `apply` verdict becomes `recommend`, for an operator to apply with `sprigctl advisor apply` |
| `off` | Doesn't run; the first split uses the structural design |

`advisor.mode` can be set for a tenant by operators (`sprigctl settings set --scope tenant:acme advisor.mode approve`), or for a data centre or everywhere. All `advisor.*` settings are operator-only at the global scope.

## Read the advisor's runs

### In SQL

`SPRIG SHOW ADVICE` returns the newest run: the run's columns on every row, then one row per table in the design.

```sql test
SPRIG SHOW ADVICE;
```

```text
RUN_ID  CAUSE        VERDICT  CONFIDENCE  ...  GROUP_NAME  GROUP_KEY  TABLE_SCHEMA  TABLE_NAME   ROUTING  KEY_COLUMNS  PARENT_TABLE  PIN
4       first_split  apply    0.93        ...  customers   numeric    shop          customers    columns  id           NULL          NULL
4       first_split  apply    0.93        ...  customers   numeric    shop          orders       columns  customer_id  NULL          NULL
4       first_split  apply    0.93        ...  customers   numeric    shop          order_items  parent   NULL         orders        NULL
4       first_split  apply    0.93        ...  NULL        NULL       shop          countries    NULL     NULL         NULL          unsharded
```

(Some columns are left out above.)

| Column | Meaning |
|---|---|
| `RUN_ID`, `CAUSE`, `MODE`, `STATUS`, `VERDICT` | The run, why it ran, the mode it ran in, how far it got, and its verdict |
| `BEST_SCORE`, `RUNNER_UP_SCORE`, `CONFIDENCE` | The best design's score, the next best, and the confidence between them |
| `UNITS_SAMPLED` | Workload units the run read |
| `DECISION_ID`, `CREATED_AT` | The decision-log entry and the run's time |
| `GROUP_NAME`, `GROUP_KEY` | The group a table joins (named after its root table) and the type class of its key: `numeric`, a string class with its collation, `binary`, ... |
| `TABLE_SCHEMA`, `TABLE_NAME` | The table, by its logical name |
| `ROUTING`, `KEY_COLUMNS`, `PARENT_TABLE` | How its rows find their shard: by its own key columns, or by its parent |
| `PIN` | The pin that placed it, if any |

Rows for tables you have no privilege on are left out. With no run yet, the result is empty.

### In Studio

**Performance**, then **Shards**, shows the tenant's table groups with their members, key columns and shard maps (ranges, clusters and states), and the advisor's last 50 runs with cause, mode, status, verdict, scores and confidence. Select a run to see its design, evidence and plan. Members show only in databases you hold a grant on; advisor runs need `SPRIG_TENANT_ADMIN` or a grant on `*.*` (error 1227 otherwise). The view is read-only.

The same data is available at `GET /v1/shards`, `GET /v1/shards/advisor` and `GET /v1/shards/advisor/{run}`.

### With sprigctl

```sh
sprigctl advisor show acme          # the newest run
sprigctl advisor show acme 4        # one run: design, scores, evidence, plan
```

Every run is also an entry in the tenant's decision log; `sprigctl explain decision` explains it.

## Accept or run the design yourself

To run the advisor now for a tenant that has no table groups yet:

```sh
sprigctl advisor run acme
```

To apply a run whose verdict is `recommend` or `wait`, for example when you are satisfied with a design before the advisor's evidence thresholds are met:

```sh
sprigctl advisor apply acme 4
```

With `advisor.mode = approve`, approve the inbox item instead, from Studio's **Approvals** page or with `SPRIG APPROVE RECOMMENDATION <n>`. See [Approvals](../concepts/approvals.md).

### What applying does

Applying a first design copies no data, because every group still has one shard:

1. The groups are written to the catalog, each with one shard on the tenant's cluster.
2. Under a short hold of the tenant's shard map (at most `mover.cutover_max_hold_ms`), each table moves into its group's schema with one atomic `RENAME TABLE` on the same server. Triggers are re-created with their tables inside the hold.
3. Each table gets a hidden routing-hash column, added online and back-filled; parents are filled before their children.
4. The groups become active, and from then on they may split.

Every step checks what is already done, so a controller that takes over resumes the run.

## Pin a table's place

A pin overrides the advisor for one table. Pins bind every design the advisor makes: the search never moves a pinned table.

| Pin | The table |
|---|---|
| `SHARD KEY (cols)` | Routes by exactly `cols`: as a member of its parent's group when the foreign key reaches the group's key through `cols`, else as the root of its own group. Its key skips the distinct-value and skew filters (only the type is checked) |
| `COLOCATE WITH p ON (cols)` | Joins `p`'s group, with `cols` holding the group's key, in place of any foreign-key parent |
| `UNSHARDED` | Stays in its database's default schema |
| `REFERENCE` | Stays out of sharded groups. Copies of reference tables on every cluster aren't built yet, so it currently stays unsharded, like `UNSHARDED` |

The examples below use these tables:

```sql test
CREATE TABLE customers (id BIGINT PRIMARY KEY, name VARCHAR(100) NOT NULL);
CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  total DECIMAL(10,2) NOT NULL
);
CREATE TABLE events (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  kind VARCHAR(40) NOT NULL
);
CREATE TABLE countries (code CHAR(2) PRIMARY KEY, name VARCHAR(100) NOT NULL);
```

Pin `customers` by its id, keep `orders` and `events` with each customer, and keep `countries` out of the groups:

```sql test
SPRIG PIN customers SHARD KEY (id);
SPRIG PIN orders COLOCATE WITH customers ON (customer_id);
SPRIG PIN events COLOCATE WITH customers ON (customer_id);
SPRIG PIN countries UNSHARDED;
SPRIG SHOW PINS;
```

`SPRIG SHOW PINS` lists `TABLE_SCHEMA`, `TABLE_NAME`, `PIN`, `KEY_COLUMNS`, `PARENT_SCHEMA`, `PARENT_TABLE`, `PINNED_BY` and `PINNED_AT` for the pins of tables you can see. A table name may be qualified (`shop.orders`); an unqualified `COLOCATE WITH` table is in the pinned table's database. A new pin of a table replaces its earlier one. To remove a pin:

```sql test
SPRIG UNPIN events;
```

`SPRIG UNPIN` succeeds with no rows changed when there was no pin.

### Rules a pin must follow

`SPRIG PIN` and `SPRIG UNPIN` need `ALTER` on the table, and `COLOCATE WITH p` needs `ALTER` on `p` too (error 1142 otherwise). A pin that breaks a rule fails with `ER_SPRIG_PIN_CONFLICT` (39410):

- every named column exists (error 1072 otherwise, as MariaDB words a missing key column), appears once, and has a type and collation a key can use;
- a table can't be co-located with itself, with a table pinned `UNSHARDED` or `REFERENCE`, or around a cycle of `COLOCATE` pins;
- a `COLOCATE` table's columns match the key of the group it joins, in number and type class: the root's `SHARD KEY` pin, or the root's current key;
- a table that other tables are co-located with can't be pinned `UNSHARDED` or `REFERENCE`, and a new `SHARD KEY` on it must match their columns.

```sql test-error
SPRIG PIN orders SHARD KEY (no_such_column);
```

```sql test-error
-- countries is pinned UNSHARDED, so nothing can be co-located with it.
SPRIG PIN events COLOCATE WITH countries ON (kind);
```

### When pins take effect

Pins take effect in the next first design. A table in a group that has already split into more than one shard accepts only a pin that matches where it already is; any other pin fails with `ER_SPRIG_PIN_NEEDS_REKEY` (39411), because moving it means re-keying the group.

## Change a key after the split

If a group's key turns out to be wrong after it has split, an operator can re-key it online:

```sh
sprigctl shard rekey acme 3 --key customer_id --member events=cust --shards 4
```

The group's rows are copied to new shards routed by the new key while clients keep reading and writing, caught up from the binary log, verified row by row, and cut over in one change of the shard map. The old shards stay fenced for `sharding.retire_hours` (24) and are then dropped. A re-key needs the group's shards on one cluster, every member able to supply the new key from one column of a routable type, and no member owning a lookup index. `sprig_advisor_rekeys_total{outcome}` counts re-keys started, finished and rolled back.

## Settings

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `advisor.mode` | text | `on` | `on`, `approve`, `recommend` or `off` |
| `advisor.window_days` | float | 7 | Workload window the advisor reads |
| `advisor.min_observe_hours` | float | 24 | History needed before a design is applied |
| `advisor.min_units` | int | 10000 | Sampled units needed |
| `advisor.min_confidence` | float | 0.9 | Confidence needed to apply a first design |
| `advisor.max_wait_hours` | float | 72 | After this, the best design is applied anyway |
| `advisor.min_ndv_per_shard` | int | 100 | Distinct key values needed per expected shard |
| `advisor.max_top_value_pct` | int | 25 | Largest share of rows one key value may hold |
| `advisor.lookup_penalty` | float | 0.1 | Credit lost per lookup a statement needs |
| `advisor.txn_weight` | float | 2 | Weight of a write transaction against a statement |
| `advisor.max_moves` | int | 200 | Search steps per run |
| `sharding.split_size_gb` | float | 32 | Size above which the first split (and every later split) is due |
| `sharding.min_shards` | int | 1 | Shards per group from the start |

`advisor.mode` accepts the tenant, data-centre and global scopes; the other `advisor.*` keys accept tenant and global. Operators set them with `sprigctl settings set`.

## Not supported yet

- Reference tables copied to every cluster: a `REFERENCE` pin keeps the table unsharded.
- Periodic redesigns of existing groups: after the first design, key changes are operator re-keys.

## Related

- [Shards and table groups](../concepts/shards-and-table-groups.md)
- [Query plans](query-plans.md)
- [Sharding in SQL](../sql/sharding.md)
- [Approvals](../concepts/approvals.md)
- [Sharding and moves troubleshooting](../troubleshooting/sharding-and-moves.md)
