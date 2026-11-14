---
title: Shards and table groups
audience: [user, dba, operator]
sidebar:
  order: 4
---

# Shards and table groups

SprigDB spreads a database across MariaDB clusters by cutting it into shards, and it keeps related rows on the same shard by grouping tables that share a key. Read this page to understand table groups, distribution keys, how shards split, merge and move while you keep writing, and what changes for your SQL once data is sharded.

## The short version

- You write ordinary MySQL DDL. There is no `SHARD KEY` clause in `CREATE TABLE`.
- Every database starts unsharded: each table group has one shard, and everything behaves exactly as MariaDB.
- The **sharding advisor** watches your workload and designs **table groups**: sets of tables that share a **distribution key**, such as a customer id.
- The **balancer** splits a group's shard when it grows, merges quiet neighbours, and moves shards between clusters, online.
- A statement that touches one shard runs as one MariaDB statement. One that spans shards is split, run in parallel and merged, or fails with 39420 `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` if SprigDB can't give the exact MariaDB answer.

## Table groups and distribution keys

A **table group** is a set of objects whose rows are placed by the same key. In a shop:

```text
database shop
├── group "customers"   key: customer id (INT)
│     customers.id
│     orders.customer_id
│     order_items.customer_id
│     events (collection)       field customer_id
│     sessions (Redis)          the customer id in the key
├── group "products"    key: product id
│     products.id
│     product_embeddings.product_id
└── unsharded: countries, currencies   (one copy, on one cluster)
```

Every member of a group holds the group's key in its own columns: `customers.id`, `orders.customer_id`. The row for customer 42, their orders, their order items and their session keys all hash to the same value, so they always live on the same shard. That's **co-location**, and it is what lets SprigDB keep shards invisible:

- a join of `customers` and `orders` on the customer id runs inside one MariaDB server;
- a transaction that changes a customer and their orders commits on one server, with no distributed commit;
- foreign keys between the two stay ordinary MariaDB foreign keys.

Groups hold objects of every model: tables, document collections, key-value namespaces and Redis databases. A table group lives inside one database.

The key's columns must have the same type class and, for strings, the same collation in every member. A column whose type or collation can't be a key fails with 39400 `ER_SPRIG_UNSUPPORTED_KEY_TYPE` or 39401 `ER_SPRIG_UNSUPPORTED_KEY_COLLATION`.

### Who decides the groups

The **sharding advisor** reads your foreign keys, the joins your queries make and the tables your transactions write together. It weighs candidate designs and applies the best one when it is confident enough. A table it can't place well stays in its database's default group, on one shard.

Its main settings:

| Setting | Default | Meaning |
|---|---|---|
| `advisor.mode` | `on` | `on`, `approve`, `recommend` or `off`. See [Approvals](approvals.md) |
| `advisor.min_observe_hours` | 24 | History needed before a design |
| `advisor.min_confidence` | 0.9 | Confidence needed to apply a first design |
| `advisor.max_wait_hours` | 72 | Then apply the best design anyway |
| `advisor.window_days` | 7 | Statistics window |

You can steer it with **pins**, which fix one table's place in every design:

```sql test
CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(100));
CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NOT NULL, total DECIMAL(10,2));
CREATE TABLE countries (code CHAR(2) PRIMARY KEY, name VARCHAR(100));

SPRIG PIN customers SHARD KEY (id);
SPRIG PIN orders COLOCATE WITH customers ON (customer_id);
SPRIG PIN countries REFERENCE;
```

`SPRIG SHOW ADVICE` shows the advisor's latest design, and `SPRIG SHOW PINS` your pins. Operators can run the advisor and apply a design with `sprigctl advisor run | show | apply`. See [Sharding in SQL](../sql/sharding.md) and [Sharding advisor](../performance/sharding-advisor.md).

### Small tables read everywhere

A small table that everything joins, such as countries, currencies or tax rates, is best left **unsharded**: pin it with `REFERENCE` or `UNSHARDED` (see [Sharding in SQL](../sql/sharding.md)), and it stays whole on one cluster. A join between a sharded table and an unsharded one is planned by the router.

**Not supported yet:** reference tables with a read copy on every cluster, so that each shard can join against a local copy. Today a `REFERENCE` pin behaves exactly like `UNSHARDED`.

## Shards

A **shard** is one slice of one table group, stored as one MariaDB schema on one cluster:

```text
t<tenant>_d<database>_g<group>_s<shard>      for example  t42_d1_g3_s1
```

Each row has a **routing hash**: a 64-bit `xxh3` hash of its key value. Before hashing, SprigDB normalizes the value: numbers by value, strings by their collation's sort weights, so `'Acme'` and `'acme'` route alike under a case-insensitive collation, exactly as MariaDB would compare them. Each shard owns a contiguous range of the hash space:

```text
group 3, key: customer id
  [0x0000000000000000, 0x8000000000000000)  shard s0  cluster fra1-c1
  [0x8000000000000000, 0xFFFFFFFFFFFFFFFF]  shard s1  cluster fra1-c2
```

The map of ranges is the group's **shard map**, kept in the control plane of the tenant's home data centre. Gateways keep a watched copy, so routing a row costs a binary search and no round trip. Each range carries an epoch, raised by every split, merge and move.

Members whose rows find their shard through a parent, rather than their own key, carry the hash in an invisible column, `_sprig_rk`. You never see it: it is `INVISIBLE`, and names starting with `_sprig_` are reserved.

## Splits, merges and moves

The balancer and the sharding loop in each data centre's controller change shards while clients keep working.

| Change | When it happens | Settings |
|---|---|---|
| **Split** | A shard is larger than `sharding.split_size_gb` (32 GB), or carries more than `sharding.split_write_share_pct` (50 %) of a hot cluster's writes for `sharding.split_sustain_minutes` (30) | `sharding.max_split_ways` (4): most children from one split |
| **Merge** | Two neighbouring shards are both below `sharding.merge_size_gb` (4 GB) and older than `sharding.merge_cooldown_hours` (24) | |
| **Move** | A cluster is hotter than `balancer.high_pct` (80 %), or the fullest and emptiest clusters differ by more than `balancer.spread_pct` (25 points), for `balancer.sustain_minutes` (30) | `balancer.target_pct` (70 %): moves keep targets at or below it |
| **Pre-split** | A new group starts with `sharding.min_shards` shards (1) | |

These keys are operator-only. `sharding.mode` and `balancer.mode` choose whether the loops act, wait for approval, only recommend, or stay off (see [Approvals](approvals.md)). The balancer also limits itself: `balancer.max_moves_per_tenant_per_day` (2), `balancer.max_concurrent_moves_per_dc` (4), `balancer.shard_cooldown_hours` (24), and a move's benefit must be at least `balancer.benefit_cost_ratio` (2) times its cost.

Operators can do each change by hand:

```sh
sprigctl shard list acme                       # groups and shards
sprigctl shard split acme 3 0 --ways 2         # split shard 0 of group 3
sprigctl shard merge acme 3 1 2                # merge adjacent shards 1 and 2
sprigctl shard move acme 3 1 --to fra1-c3 --wait
sprigctl shard rekey acme 3 --key customer_id  # re-key the group, online
sprigctl job list --tenant acme                # mover jobs, newest first
```

### How a move stays online

Splits, merges, moves and re-keys are all **mover jobs**, run by workers:

1. **Copy.** The rows are copied in chunks (`mover.copy_chunk_rows`, 5,000), throttled when the source gets busy or its replicas lag.
2. **Catch up.** Changes made since the copy started are read from the source's binlog and applied in order. Every applied transaction also records its source position, so a restart resumes exactly.
3. **Verify.** Per-row digests of source and target are compared, and differing ranges are repaired. Every row is checked by default; `sprigctl shard move --verify` can check a sample of chunks instead, or skip the step.
4. **Cut over.** When the target has stayed within `mover.ready_max_lag_ms` (1 s) for `mover.ready_sustain_seconds` (60 s), the mover holds the shard's range in the map, fences the source, applies the last changes, raises the epoch, points the map at the target and releases the hold.
5. **Clean up.** The fenced source is kept for `mover.source_retain_hours` (24) and then dropped.

### The write pause

During the cut-over hold, a statement outside a transaction that touches the moving shard waits in the gateway until the hold ends, then runs against the new map; it isn't refused. The hold lasts at most `mover.cutover_max_hold_ms` (2,000 ms by default, settable per tenant from 200 ms to 60 s). Transactions already open get `mover.txn_grace_ms` (300 ms) to finish first.

If the cut-over can't finish inside the budget, the mover aborts it, releases the hold, keeps catching up, and tries again after `mover.cutover_retry_seconds` (30 s, doubling). After `mover.max_cutover_attempts` (5) it pauses the job and raises an alert.

A statement that was routed just before a cut-over can meet the new map. Then it fails with one of these, and is safe to retry:

| Code | Name | Meaning |
|---|---|---|
| 39110 | `ER_SPRIG_SHARD_MOVED` | The shard moved; retry |
| 39424 | `ER_SPRIG_SHARD_MAP_CHANGED` | The shard map changed during the statement; retry |
| 39470 | `ER_SPRIG_SHARD_BUSY` | A split, merge or move holds the shard |

A move between data centres pauses writes for the whole tenant instead, within `migration.write_pause_target_ms` (5 s). See [Clusters and data centres](clusters-and-data-centres.md).

## How statements run across shards

A tenant whose tables are all unsharded runs every statement on MariaDB unchanged. Once tables are sharded, the router plans each statement:

| Statement | What happens |
|---|---|
| Reads and writes with the key in an equality or `IN` list | Sent only to the shards that hold those keys |
| Reads without the key | Run on every shard in parallel and merged in the gateway: `ORDER BY`, `LIMIT`, `GROUP BY`, `DISTINCT` and the common aggregates |
| Joins of tables in one group, on the key | Run on each shard, where the rows live together |
| Other joins | Lookup joins in batches, or gathered reads |
| `INSERT` with `AUTO_INCREMENT` | Values come from ID blocks shared by every gateway: unique across shards, not gap-free |
| A foreign key between groups | Checked by the gateway on every write |

### Statements that can't be distributed

SprigDB never returns an approximate answer. A statement it can't run with exact MariaDB semantics fails with 39420 `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE`, and the message starts with a reason code:

```text
ERROR 39420 (HY000): Statement not distributable (window_function): a window function across shards
```

The common cases:

- a window function, `UNION`, `INTERSECT` or `EXCEPT` across shards;
- a correlated subquery, derived table or recursive CTE over shards;
- an aggregate the gateway can't combine exactly, or `WITH ROLLUP`;
- an `UPDATE` or `DELETE` with `ORDER BY` or `LIMIT` across shards;
- an `UPDATE` that changes the distribution key;
- an upsert or `REPLACE` on a unique key that doesn't include the distribution key;
- `LOCK TABLES` or `HANDLER` on a sharded table;
- variable assignment or `SQL_CALC_FOUND_ROWS` across shards;
- a stored function in a statement across shards.

To fix one, include the distribution key so the statement reaches one shard, split it into simpler statements, or pin the tables so they share a group. The full list of reason codes is in [Sharding in SQL](../sql/sharding.md#how-statements-behave-on-sharded-tables).

A schema change also has to reach every shard. One SprigDB can't distribute fails with 39481 `ER_SPRIG_DDL_UNSUPPORTED`.

## Distributed transactions

A transaction keeps one branch per cluster it touches. How it commits depends on where it wrote:

| Wrote on | Commit |
|---|---|
| One cluster (one shard, or several shards on one cluster) | An ordinary MariaDB commit |
| Several clusters, all with XA | XA two-phase commit: every other branch prepares, the last one commits together with a decision row (the commit point), then the prepared branches commit |
| Several clusters, some without XA | Decided by `txn.non_xa_policy` |

`txn.non_xa_policy` (tenant, DC or global scope):

| Value | Behaviour |
|---|---|
| `reject` (default) | A write that would add a second cluster without XA fails with 39440 `ER_SPRIG_CROSS_CLUSTER_WRITE_REJECTED`, before it runs |
| `best_effort` | XA where possible; the rest commit one at a time. If one fails after others committed, `COMMIT` fails with 39442 `ER_SPRIG_PARTIAL_COMMIT` |
| `sprig_2pc` | On a cluster without XA, SprigDB records the branch's writes and holds it open with its locks, so it can prepare. A statement that wouldn't replay to the same result rolls the transaction back with 39446 `ER_SPRIG_HELD_BRANCH_STATEMENT` |

Galera clusters don't support XA, so a transaction that writes to a Galera cluster and another cluster needs `best_effort` or `sprig_2pc`.

Other limits and failures:

| Code | Name | Meaning |
|---|---|---|
| 39444 | `ER_SPRIG_TOO_MANY_BRANCHES` | The transaction would touch more than `txn.max_branches` clusters (32) |
| 39443 | `ER_SPRIG_DTX_ROLLED_BACK` | The distributed transaction was rolled back; retry it |
| 39441 | `ER_SPRIG_COMMIT_OUTCOME_UNKNOWN` | The commit's outcome is unknown; the connection closes. The resolver finishes it from the decision row |
| 39449 | `ER_SPRIG_PHASE2_PENDING` | The previous distributed commit is still finishing on this cluster; retry |

If a gateway dies between prepare and commit, the controller's resolver finds the prepared branches and commits or rolls them back from the decision row. Operators can see and resolve in-doubt transactions with `sprigctl txn list | show | resolve`.

Each branch has MariaDB's isolation on its own cluster. A transaction that reads on several shards takes each shard's snapshot when it first touches that shard; there is no single snapshot across shards.

## Related

- [Sharding in SQL](../sql/sharding.md)
- [Sharding advisor](../performance/sharding-advisor.md)
- [Clusters and data centres](clusters-and-data-centres.md)
- [Backups and moves](../operate/backups-and-moves.md)
- [Sharding and moves troubleshooting](../troubleshooting/sharding-and-moves.md)
