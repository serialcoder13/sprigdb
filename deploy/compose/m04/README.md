# M0.4 harness: automatic indexing, end to end

This directory holds the harness for milestone M0.4's "done when" (chapter 48): chapter 13's scenario suite passes on all three reference topologies.

1. An index is proposed, validated and built within one trigger window for each worked example in chapter 13.
2. A regression candidate is rejected.
3. An unused index is dropped, and a migrated or user index never is.
4. Schema tools see no drift.

Everything goes through the real gateway (as `<tenant>.app`) and the real controller: chapter 12 records the statements in the tenant's catalog, and the controller's `auto_index` loop evaluates, validates, builds, observes and drops. The tests only generate load, change settings with `SPRIG SET`, and read the catalog's `auto_index*` tables and the gateway's `SHOW INDEX`.

## Run it

```sh
cd deploy/compose && docker compose --profile all up -d --wait && cd ../..
CARGO_TARGET_DIR=target/m04 deploy/compose/gateway-dev.sh up    # tenants acme and globex
CARGO_TARGET_DIR=target/m04 deploy/compose/m04/run.sh setup      # settings, tenants m04r and m04g, restart
CARGO_TARGET_DIR=target/m04 deploy/compose/m04/run.sh single     # or replication, galera, all
CARGO_TARGET_DIR=target/m04 deploy/compose/m04/run.sh drift
CARGO_TARGET_DIR=target/m04 deploy/compose/m04/run.sh teardown   # settings reset, m04 tenants deleted
```

| Topology | Tenant | Cluster | Validation |
|---|---|---|---|
| `single` | `acme` | `dc-a-c1`, 13301 | estimate and canary |
| `replication` | `m04r` | `dc-a-c2`, primary 13311, replicas 13312 and 13313 | replica build, binary log off |
| `galera` | `m04g` | `dc-a-c3`, 13321 to 13323 | Galera RSU on a node that isn't the writer |

Run one topology at a time: the dev MariaDB nodes are capped at 1 GB and share an 8 GB Docker VM. Check `docker stats --no-stream` between runs.

`setup` does three things:

- **Workload settings** at global scope, which the gateway's recorder reads at start: `workload.bucket_minutes = 1` (so "since the build" has minute resolution), `workload.watch_p95_ms = 0` (every fingerprint is probed and sampled, as a slow one would be), `workload.examined_sample_rate = 0.5`, `workload.sample_rate = 0.2`, `workload.samples_per_bucket = 10`. `teardown` resets them.
- **Tenants** `m04r` on `dc-a-c2` and `m04g` on `dc-a-c3`, each with the user `app` (password `sprig-dev-app`).
- **A restart** of the controller and the gateway (`gateway-dev.sh restart`), with `SPRIGDB_CONTROLLER_AUTOINDEX_TEST_CLOCK_FILE` pointing at `.gateway-dev/autoindex-clock`. The file holds whole seconds by which the `auto_index` loop's clock runs ahead (`[controller] autoindex_test_clock_file`, development only). Leases and elections never read it.

## The scenario test (`crates/sprig-testkit/tests/m04_scenarios.rs`)

Each run makes a fresh database `m04_<random>` and sets, with `SPRIG SET` at tenant scope: `auto_index.window_hours = 1`, `eval_interval_minutes = 1`, `p95_threshold_ms = 5`, `max_new_per_day = 50`, `rollout_pause_minutes = 0`, `cooldown_hours = 1` (the default 24 would hold a fingerprint decided in the first minute for the rest of the run; 0 lets an already-served fingerprint, whose window still holds its slow runs from before the build, propose redundant variants every minute). The tables are filled on the cluster directly (they are the tables' existing data; a bulk load inside the 1-hour window would count as an hour of writes in the cost model). Then the load runs through the gateway:

| Table | Rows | Statements | Expected |
|---|---|---|---|
| `orders` (worked example 1) | 100,000 | `customer_id = ? AND status IN ('open','hold') ORDER BY created_at DESC LIMIT 20` (prepared), inserts, status updates by id; user index `idx_created` | an automatic index starting with `customer_id`, built |
| `events` (worked example 2) | 100,000 | F1 `account_id = ? AND kind = ? AND created_at >= ? ORDER BY created_at LIMIT 20`, F2 `COUNT(*)` on the same equalities, F3 `account_id = ? ORDER BY id DESC LIMIT 20`, F4 `LOWER(source) = ?`, inserts | `(account_id, kind, created_at)` built; F4 skipped as `function:LOWER` |
| `t` (worked example 3, as chapter 13's `ORDER BY ... LIMIT` trap) | 100,000 | `c BETWEEN ? AND ?` (many runs); `d = 1 ORDER BY c LIMIT 10` (12 runs; `top_n = 0` on `t`, so it never qualifies itself) | `(c)` rejected for the regression; the covering retry `(c, d)` accepted and built |
| `v` | 30,000 | `a = ?` | `(a)` built; then the user creates `(a, b)` and ours is retired as `superseded` |
| `u` | 30,000 | `a = ?` until its index is built, then only primary-key reads and lookups by the user index | `(a)` built; on the test clock: active at day 8, retired as `unused` at day 40, dropped at day 42; the user index `idx_u_b` and the migrated `ix_u_mig` stay |

Worked example 4 is a MongoDB collection (R5) and isn't part of M0.4.

The migrated index is created by the test and given an `index_origins` row (`migrated`, source `mysqldump`), as a dump replay will record it once the import path exists (chapter 13 build step 17, R4).

The test asserts each item and writes `results/<topology>.json`: per example the index, the seconds from the first query to the built index, every validation (method, node, verdict, reason, p95 before and after, worst regression, elapsed), the skipped rows, the supersede time and the state timeline on the test clock. The full log is `results/<topology>.log`. Variables: `SPRIG_TEST_M04_ROWS` (100,000), `SPRIG_TEST_M04_KEEP=1` keeps the database.

## The drift suite (`drift.sh`)

As `acme.app`: Flyway migrates m03's `shop` and `crm` (V1 to V6), `shop.orders` grows to 30,000 rows and a lookup by `qty` (unindexed; `customer_id` has its foreign key's index) runs until an automatic index exists on it. With the index present: Flyway `info` and `validate`, Hibernate `ddl-auto=validate` (m03's image), `mysqldump` 8.4 (port 13306) and `mariadb-dump` 11.8 (port 13307) of `shop`, `SHOW CREATE TABLE`, `SHOW INDEX` and `information_schema.STATISTICS`. No output may contain `sprig_auto_`; the index must still exist (shown with `@sprig_show_auto_indexes = 1`) and `EXPLAIN` must show it. Results go to `results/drift/` (`drift.jsonl`, one line per check). It reuses m03's images; `SPRIG_M03_BUILD=1` builds them first.

## Results

`results/` is git-ignored. The milestone's verdict is in `sprigdb-plan/decisions/m0.4-done-when.md`.
