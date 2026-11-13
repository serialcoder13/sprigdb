---
title: Sharding in SQL
audience: [dba]
sidebar:
  order: 4
---

# Sharding in SQL

SprigDB shards tables for you: the sharding advisor groups related tables into table groups, chooses each group's distribution key, and the balancer splits, merges and moves shards as data grows. This page covers the SQL you use to steer those choices (pins), to read the advisor's design, and how ordinary statements behave once a table is sharded. For the model itself, see [Shards and table groups](../concepts/shards-and-table-groups.md).

## What is and isn't in SQL

You don't declare sharding in `CREATE TABLE`. There is no `SHARD KEY`, `DISTRIBUTED BY`, table-group or reference-table clause in DDL, and no SQL statement splits, merges or moves a shard. Write ordinary MySQL DDL. Then:

- to fix a table's place in every design the advisor makes, use [`SPRIG PIN`](#sprig-pin);
- to see the design, use [`SPRIG SHOW ADVICE`](#sprig-show-advice) and [`SPRIG SHOW PINS`](#sprig-show-pins);
- to approve or ignore a sharding recommendation, use [`SPRIG APPROVE RECOMMENDATION`](explain-and-workload.md#sprig-approve-recommendation);
- to list groups and shards, split, merge or re-key by hand, use `sprigctl shard list | split | merge | rekey` and `sprigctl advisor run | show | apply` (see [sprigctl](../tools/sprigctl.md)), or Studio's shards view.

`SHOW SHARDS` and `SHOW TABLE GROUPS` don't exist.

## SPRIG PIN

Fixes one table's place in the sharding design.

```sql
SPRIG PIN [db_name.]table_name pin_spec

pin_spec:
    SHARD KEY (column [, column] ...)
  | COLOCATE WITH [db_name.]parent_table ON (column [, column] ...)
  | REFERENCE
  | UNSHARDED
```

| Pin | The table |
|---|---|
| `SHARD KEY (cols)` | Routes by exactly these columns. It is a member of a group keyed by them, or the root of a new group when no parent's group has that key |
| `COLOCATE WITH parent ON (cols)` | Joins `parent`'s table group; its `cols` hold the group's key, so its rows live on the same shard as the matching parent rows |
| `REFERENCE` | Marks the table as a reference table. Reference copies aren't built, so today it acts like `UNSHARDED`: the table stays unsharded, on one cluster |
| `UNSHARDED` | Stays in its database's default group, on one shard |

An unqualified table is in the current database; an unqualified `COLOCATE WITH` parent is in the pinned table's database. A new pin of a table replaces its earlier pin.

**Privileges:** `ALTER` on the table, and for `COLOCATE WITH` on the parent too.

**When pins apply.** The advisor honours pins in its next first design for the tenant. A table whose group already has several shards takes only a pin that matches its current place; anything else fails with 39411 and needs a re-key (`sprigctl shard rekey`).

**Conflict rules.** A pin fails with 39410 `ER_SPRIG_PIN_CONFLICT` when:

1. a named column appears twice, or has a type or collation a distribution key can't use (a missing column fails with 1072);
2. the table would be co-located with itself, with a table pinned `UNSHARDED` or `REFERENCE`, or around a cycle of `COLOCATE` pins;
3. a `COLOCATE` table's columns don't match the key of the group it joins, in number and type class;
4. tables are pinned `COLOCATE WITH` this one, and the new pin is `UNSHARDED`, `REFERENCE`, or a `SHARD KEY` their columns don't match.

**Examples:**

```sql test
CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(100));
CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NOT NULL, total DECIMAL(10,2));
CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT, customer_id INT NOT NULL, sku VARCHAR(40));
CREATE TABLE countries (code CHAR(2) PRIMARY KEY, name VARCHAR(100));
CREATE TABLE audit_log (id BIGINT PRIMARY KEY, note TEXT);

SPRIG PIN customers SHARD KEY (id);
SPRIG PIN orders COLOCATE WITH customers ON (customer_id);
SPRIG PIN order_items COLOCATE WITH orders ON (customer_id);
SPRIG PIN countries REFERENCE;
SPRIG PIN audit_log UNSHARDED;
-- each: Query OK, 1 row affected
```

```sql test-error
-- a table pinned UNSHARDED can't be a COLOCATE parent: 39410
SPRIG PIN order_items COLOCATE WITH audit_log ON (id);
```

## SPRIG UNPIN

```sql
SPRIG UNPIN [db_name.]table_name
```

Removes a table's pin. Without a pin, the statement answers OK with 0 rows affected. **Privileges:** `ALTER` on the table.

```sql test
SPRIG UNPIN audit_log;
```

## SPRIG SHOW PINS

```sql
SPRIG SHOW PINS
```

Lists the pins of every table you hold a privilege on.

| Column | Meaning |
|---|---|
| `TABLE_SCHEMA`, `TABLE_NAME` | The pinned table |
| `PIN` | `SHARD KEY`, `COLOCATE`, `REFERENCE` or `UNSHARDED` |
| `KEY_COLUMNS` | The columns of `SHARD KEY` or `COLOCATE … ON` |
| `PARENT_SCHEMA`, `PARENT_TABLE` | The `COLOCATE WITH` table |
| `PINNED_BY` | Who set the pin |
| `PINNED_AT` | When |

```sql test
SPRIG SHOW PINS;
```

After the examples above, it lists `customers` (`SHARD KEY`, `id`), `orders` and `order_items` (`COLOCATE`, `customer_id`, parent `customers` and `orders`) and `countries` (`REFERENCE`).

## SPRIG SHOW ADVICE

```sql
SPRIG SHOW ADVICE
```

Shows the sharding advisor's newest run for the tenant: the run's summary on every row, then one row per table in the design. Tables you hold no privilege on are left out. With no run yet, it returns no rows.

| Column | Meaning |
|---|---|
| `RUN_ID` | The run |
| `CAUSE` | What started it |
| `MODE` | `advisor.mode` at the time: `on`, `approve`, `recommend` or `off` |
| `STATUS` | The run's state |
| `VERDICT` | What the run concluded, such as applying the design or waiting for more history |
| `BEST_SCORE`, `RUNNER_UP_SCORE` | Scores of the best design and the next best |
| `CONFIDENCE` | How sure the advisor is; it applies a first design at `advisor.min_confidence` or above |
| `UNITS_SAMPLED` | Statements and transactions it learned from |
| `DECISION_ID` | The decision record, for `sprigctl explain` |
| `CREATED_AT` | When it ran |
| `GROUP_NAME` | The table group |
| `GROUP_KEY` | The group key's type classes and collations, such as `int` or `string(utf8mb4_0900_ai_ci)` |
| `TABLE_SCHEMA`, `TABLE_NAME` | The table |
| `ROUTING` | How rows find their shard: by the table's own key columns, through a parent, or `unsharded` |
| `KEY_COLUMNS` | The table's columns that hold the key |
| `PARENT_TABLE` | The table it follows, if any |
| `PIN` | The table's pin, if any |

```sql test
SPRIG SHOW ADVICE;
```

A tenant the advisor hasn't run for yet gets no rows. After a run, each table of the design has a row, after the run's summary columns.

The advisor's behaviour is set by the `advisor.*` settings (operator-only bounds, tenant values): `advisor.mode`, `advisor.min_confidence`, `advisor.min_observe_hours`, `advisor.window_days` and others. See the [settings reference](../reference/settings.md#advisor) and [Sharding advisor](../performance/sharding-advisor.md).

## Shard counts and min_shards

Every table group starts with one shard and splits when a shard passes `sharding.split_size_gb` (default 32 GB), or carries more than `sharding.split_write_share_pct` of a hot cluster's writes for `sharding.split_sustain_minutes`. Quiet neighbours below `sharding.merge_size_gb` merge. `sharding.min_shards` pre-splits each group into at least that many shards. These keys are operator-only: a tenant session can read them with `SHOW SPRIG SETTINGS LIKE 'sharding.%'` but can't set them; an operator sets them with `sprigctl settings`.

## How statements behave on sharded tables

Once a tenant has sharded tables, every statement goes through SprigDB's router. Statements on unsharded tables are passed to MariaDB unchanged.

| Statement | What the router does |
|---|---|
| Reads and writes with the distribution key in an equality or `IN` list | Sent to the one shard (or the few shards) that hold those keys |
| Reads without the key | Run on every shard in parallel (at most `router.max_parallel_shards` at once) and merged: `ORDER BY`, `LIMIT`, `GROUP BY`, `DISTINCT` and the common aggregates are finished in the gateway |
| Joins of tables in one group on the key | Run on each shard, where the rows live together |
| Other joins | Lookup joins in batches of `router.lookup_batch_size` keys, or gathered reads |
| `INSERT … SELECT`, `REPLACE … SELECT`, `LOAD DATA LOCAL INFILE` into a sharded table | The rows are read first, then inserted in batches, in one transaction (or a savepoint inside yours) |
| `INSERT` into a table with `AUTO_INCREMENT` | Values come from the tenant's ID blocks; see below |
| Writes on several shards | Run in a distributed transaction; see below |
| `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT` | Handled by SprigDB's transaction coordinator |

A statement that can't run with exact MariaDB semantics fails with 39420 `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE`. Its message names a reason code first:

```text
ERROR 39420 (HY000): Statement not distributable (window_function): a window function across shards
```

| Reason | Typical statement |
|---|---|
| `join_condition`, `join_types`, `join_depth` | A cross-shard join without a usable equality, comparing a string with a number, or with too many lookup joins |
| `correlated_subquery`, `subquery`, `derived_table`, `recursive_cte` | Subqueries, CTEs, derived tables or `WITH RECURSIVE` over shards that can't be split exactly |
| `window_function` | A window function across shards |
| `set_operation` | `UNION`, `INTERSECT` or `EXCEPT` across shards |
| `aggregate` | An aggregate the gateway can't combine exactly, `WITH ROLLUP`, `DISTINCT` with aggregates |
| `ordering` | Ordering the gateway can't reproduce: `GEOMETRY`, `ENUM`, an unknown collation, a non-deterministic expression, `NULLS FIRST` |
| `limited_dml`, `multi_table_dml` | Multi-shard `UPDATE` or `DELETE` with `ORDER BY` or `LIMIT`; multi-table writes not on one shard |
| `upsert_unique` | Upsert or `REPLACE` with a unique key that lacks the distribution key |
| `key_change` | An `UPDATE` that changes the distribution key, or `ON DUPLICATE KEY UPDATE` of it |
| `variables`, `found_rows` | Variable assignment or `SQL_CALC_FOUND_ROWS` across shards |
| `locking` | `LOCK TABLES` or `HANDLER` on a sharded table |
| `auto_increment`, `insert_key` | An `INSERT` row without its key value, a zero key, or a key value that isn't a literal of the key's type |
| `cross_cluster` | A write or join that needs one cluster but spans several |
| `lookup`, `foreign_key` | A write that would leave a unique lookup index or a gateway-kept foreign key unchecked |
| `routine` | A stored function or UDF in a statement across shards |
| `fulltext_relevance` | MariaDB `FULLTEXT` relevance compared across shards |
| `schema_skew` | Shards answered with different columns (a DDL change in flight) |
| `gather`, `parse`, `unsupported` | Other forms the router doesn't distribute |

Rewrite the statement to include the distribution key, split it, or pin the tables so they share a group.

### AUTO_INCREMENT

Inserts into a sharded table take `AUTO_INCREMENT` values from per-table ID blocks that every gateway shares. Values are unique across shards and follow the session's `auto_increment_increment` and `auto_increment_offset`. They are not gap-free and not strictly ordered across gateways. `LAST_INSERT_ID()` returns the first value of the statement. Explicit values are accepted, and the next blocks start above the highest one.

| Code | Name | When |
|---|---|---|
| 39460 | `ER_SPRIG_ID_BLOCK_UNAVAILABLE` | No ID block could be taken; retry |
| 39463 | `ER_SPRIG_ID_VALUE_RESERVED` | An explicit value collides with a reserved block |

### Foreign keys across table groups

A foreign key whose two tables live on one shard (one group, joined through the key) stays a MariaDB foreign key. When the two tables are in different groups, SprigDB keeps the key itself: `ALTER TABLE … ADD FOREIGN KEY` checks existing rows (an orphan fails with 1452 and nothing changes), and every later write is checked by the gateway. Actions it can't run across shards fail with 39462 `ER_SPRIG_FK_ACTION_NOT_DISTRIBUTED`.

### Transactions across shards

A transaction that writes on one cluster commits there, exactly as MariaDB. One that writes on several clusters commits with XA two-phase commit when the clusters support it. `txn.non_xa_policy` (`reject`, `best_effort` or `sprig_2pc`) decides what happens when a cluster lacks XA. A transaction may span at most `txn.max_branches` clusters (default 32). See [MySQL compatibility](overview.md#transactions-across-shards) for the isolation rules and errors.

### Reference tables

A table pinned `REFERENCE` stays unsharded, on one cluster, exactly like a table pinned `UNSHARDED`. There is no primary copy with read copies on other clusters.

### Not supported yet

- Reference tables copied to every cluster: `REFERENCE` pins are accepted and recorded, but the table isn't copied.

## Errors

| Code | Name | When |
|---|---|---|
| 1064 | (`42000`) | Syntax error in a pin statement |
| 1072 | (`42000`) | A pin names a column the table doesn't have |
| 1142 | (`42000`) | Missing `ALTER` on the table or the `COLOCATE WITH` parent |
| 1146 | (`42S02`) | The table doesn't exist |
| 39410 | `ER_SPRIG_PIN_CONFLICT` | The pin breaks a conflict rule |
| 39411 | `ER_SPRIG_PIN_NEEDS_REKEY` | The table's group already has several shards and the pin doesn't match its place |
| 39400 | `ER_SPRIG_UNSUPPORTED_KEY_TYPE` | A key column's type can't be a distribution key |
| 39401 | `ER_SPRIG_UNSUPPORTED_KEY_COLLATION` | A key column's collation can't be a distribution key |
| 39403 | `ER_SPRIG_SHARD_MAP_UNAVAILABLE` | The shard map couldn't be read; retry |
| 39405 | `ER_SPRIG_KEY_VALUE_UNROUTABLE` | A key value can't be routed |
| 39420 | `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` | The statement can't run exactly across shards |
| 39421 | `ER_SPRIG_PLAN_LIMIT_EXCEEDED` | The plan exceeds a router limit, such as `router.max_route_values` |
| 39422 | `ER_SPRIG_ROUTER_MEMORY_LIMIT` | A merge needs more than `router.max_statement_memory_mb` |
| 39424 | `ER_SPRIG_SHARD_MAP_CHANGED` | The shard map moved during the statement; retry |
| 39110 | `ER_SPRIG_SHARD_MOVED` | The shard moved; retry |
| 39470 | `ER_SPRIG_SHARD_BUSY` | A split, merge or move holds the shard |
| 39471 | `ER_SPRIG_SPLIT_NOT_ALLOWED` | A manual split isn't allowed for this group |
| 39460, 39463 | ID blocks | See [AUTO_INCREMENT](#auto_increment) |
| 39462 | `ER_SPRIG_FK_ACTION_NOT_DISTRIBUTED` | A foreign key action can't run across shards |

## Related

- [Shards and table groups](../concepts/shards-and-table-groups.md)
- [Sharding advisor](../performance/sharding-advisor.md)
- [MySQL compatibility](overview.md)
- [Sharding and moves troubleshooting](../troubleshooting/sharding-and-moves.md)
- [sprigctl](../tools/sprigctl.md)
