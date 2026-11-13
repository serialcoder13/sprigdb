---
title: Analytics
audience: [user, dba]
sidebar:
  order: 8
---

# Analytics

A dimensional model describes your existing tables in business terms: **facts** you measure (orders, sales lines), **measures** you add up (revenue, units, distinct buyers) and **dimensions** you slice by (customer region, month). You define a model once, then ask for "revenue by region and month, with subtotals" through the Analytics API, and SprigDB writes the SQL, runs it on every shard and merges the answer. Use it for reporting and dashboards over operational data, without copying the data into a warehouse.

## How it works

A model is a semantic layer, not a copy of your data:

- **Facts and dimensions are ordinary tables** in one database. You keep writing them with SQL or any other protocol; there is nothing to load or refresh.
- **The model is a JSON document** stored in that database, in a table named `__sprig_dim_models` (one row per model: name, definition, version and state).
- **A query compiles to one SQL statement** with partial aggregates (`SUM`, `COUNT`, `MIN`, `MAX`), which runs in your own session through the router, on one shard or on all of them. The gateway then merges the shards' partial results, adds subtotals and a grand total, sorts and limits.
- **Every query reads live data.** There are no pre-built aggregates, materialised rollups or result caches; each call runs against the tables as they are.

Because each statement runs in your session, you need `SELECT` on the tables a model reads, and row-level security policies apply as they do to your own SQL.

## Define a model

A model has a name, optional time settings, a list of dimensions and at least one fact:

```json
{
  "model": "sales",
  "time_zone": "Asia/Kolkata",
  "fiscal_start_month": 4,
  "dimensions": [
    {"name": "customer", "table": "customers", "key": "id", "scd": 2,
     "hierarchies": [{"name": "geo", "levels": ["country", "region", "city"]}]},
    {"name": "date", "builtin": "time"}
  ],
  "facts": [
    {"name": "orders", "table": "orders", "key": ["id"],
     "links": [
       {"dimension": "customer", "column": "customer_id",
        "attribution": "at_fact_time", "time": "placed_at"},
       {"dimension": "date", "column": "placed_at"}
     ],
     "measures": [
       {"name": "revenue", "expr": "total", "agg": "sum"},
       {"name": "units", "expr": "qty", "agg": "sum"},
       {"name": "avg_qty", "expr": "qty", "agg": "avg"},
       {"name": "buyers", "expr": "customer_id", "agg": "count_distinct"},
       {"name": "paid_revenue", "expr": "total", "agg": "sum", "filter": "status = 'paid'"}
     ]}
  ]
}
```

### Model fields

| Field | Type | Default | Meaning |
|---|---|---|---|
| `model` | text | required | The model's name: 1 to 64 letters, digits or underscores, unique in the database |
| `time_zone` | text | `UTC` | Zone of the time dimension: `UTC`, an offset such as `+05:30`, or a named zone such as `Asia/Kolkata` |
| `fiscal_start_month` | int | `1` | First month (1 to 12) of the fiscal year, for the `fiscal_year` level |
| `dimensions` | list | `[]` | The dimensions below |
| `facts` | list | required | At least one fact |

A named zone needs MariaDB's time zone tables loaded on the clusters; an offset always works.

### Dimensions

A **table dimension** names a table in the model's database, its key column, and its levels from coarsest to finest. Each level is a column of the table:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `name` | text | required | The dimension's name in queries |
| `table` | text | required | The dimension table |
| `key` | text | required | The column that facts reference |
| `hierarchies` | list | none | One hierarchy, `{"name": ..., "levels": [...]}`, coarsest level first |
| `levels` | list | `[key]` | The levels, if you don't use `hierarchies` |
| `scd` | int | `1` | `2` makes this a type 2 (history-keeping) dimension with system versioning |
| `versioning` | text | `none`, or `system` with `scd: 2` | `none`, `system`, or `application` |
| `valid_from`, `valid_to` | text | none | With `application` versioning: the two columns of the validity period |

The **time dimension** is built in: `{"name": "date", "builtin": "time"}`. It has no table; a fact links it to one of its date or datetime columns. Its levels, coarsest first:

| Level | Key value | Example |
|---|---|---|
| `year` | Calendar year | `2026` |
| `fiscal_year` | Year in which the fiscal year starts | `2026` |
| `quarter` | Year × 10 + quarter | `20262` |
| `month` | Year × 100 + month | `202605` |
| `week` | ISO year and week | `202619` |
| `day` | Date | `2026-05-14` |
| `hour` | Date and hour | `2026-05-14 09:00` |

The fact's time column is read as UTC and converted to the model's `time_zone` before it is cut into levels, so a day or month means the local day or month.

### Facts, links and measures

| Field | Type | Meaning |
|---|---|---|
| `name` | text | The fact's name |
| `table` | text | The fact table, in the model's database |
| `key` | list of text | The grain: the columns that identify one fact row |
| `links` | list | One link per dimension the fact uses |
| `measures` | list | The measures; names are unique across the whole model |

A **link** joins the fact to a dimension:

| Field | Meaning |
|---|---|
| `dimension` | The dimension's name |
| `column` | The fact's column holding the dimension's key, or, for the time dimension, the time column |
| `attribution` | For a type 2 dimension: `current` (default) uses the dimension row as it is now; `at_fact_time` uses the row that was valid when the fact happened |
| `time` | The fact's time column; required with `at_fact_time` |

A **measure** aggregates an expression over fact rows:

| Field | Meaning |
|---|---|
| `name` | The measure's name in queries |
| `expr` | An expression over the fact's columns |
| `agg` | `sum`, `count`, `count_distinct`, `min`, `max` or `avg` |
| `filter` | Optional condition; only fact rows where it holds count |

Expressions and filters use a small, safe grammar: column names of the fact table, numbers, quoted strings, parentheses, arithmetic (`+ - * /`, `DIV`, `MOD`) and comparison operators, and the words `AND`, `OR`, `NOT`, `IN`, `IS`, `NULL`, `BETWEEN`, `LIKE`, `CASE`, `WHEN`, `THEN`, `ELSE`, `END`, `TRUE` and `FALSE`. Functions, subqueries, comments and semicolons are refused, so a model can't inject SQL.

### Type 2 dimensions

A type 2 dimension keeps its history, so you can report a fact against the dimension as it was when the fact happened. With `"scd": 2` (or `"versioning": "system"`), SprigDB uses MariaDB system versioning: when you save the model, it runs `ALTER TABLE ... ADD SYSTEM VERSIONING` on the dimension table if the table isn't versioned yet. From then on, every `UPDATE` keeps the old row. Adding versioning to a table you have already filled starts the history at that moment.

If your table already holds validity periods, use `"versioning": "application"` with `valid_from` and `valid_to`; a fact matches the row where `valid_from <= time < valid_to`.

For example, if a customer moves from region `south` to region `west` on 1 July, `at_fact_time` attributes their June orders to `south` and their July orders to `west`, while `current` attributes all of them to `west`.

## Manage models

Models are managed through the Analytics API, over REST and gRPC (`sprig.v1.Analytics`), on the gateway's HTTP port. Authenticate as any other API call; see [REST and gRPC](../connect/rest-and-grpc.md).

| Method | Path | Call | Does |
|---|---|---|---|
| POST | `/v1/databases/{database}/models` | `Analytics.CreateModel` | Saves a new model: `{"definition": {...}}` |
| GET | `/v1/databases/{database}/models` | `Analytics.ListModels` | Lists the database's models |
| GET | `/v1/databases/{database}/models/{model}` | `Analytics.GetModel` | One model, its definition, version and state |
| PUT | `/v1/databases/{database}/models/{model}` | `Analytics.UpdateModel` | Replaces the definition and bumps the version |
| DELETE | `/v1/databases/{database}/models/{model}` | `Analytics.DropModel` | Drops the model; its tables stay |
| POST | `/v1/databases/{database}/models/{model}:query` | `Analytics.Query` | Runs a query |
| POST | `/v1/databases/{database}/models/{model}:explain` | `Analytics.Explain` | Shows the SQL a query compiles to |
| POST | `/v1/databases/{database}/models/{model}:drill` | `Analytics.Drill` | Returns the next query down or up a hierarchy |

```sh
curl -s -X POST https://db.example.com:8443/v1/databases/shop/models \
  -H 'Authorization: Bearer <api-key>' -H 'Content-Type: application/json' \
  -d '{"definition": {"model": "sales", "time_zone": "UTC",
        "dimensions": [{"name": "customer", "table": "customers", "key": "id", "levels": ["country", "region"]},
                       {"name": "date", "builtin": "time"}],
        "facts": [{"name": "orders", "table": "orders", "key": ["id"],
          "links": [{"dimension": "customer", "column": "customer_id"},
                    {"dimension": "date", "column": "placed_at"}],
          "measures": [{"name": "revenue", "expr": "total", "agg": "sum"}]}]}}'
```

```json
{"database": "shop", "model": "sales", "definition": {"model": "sales", "...": "..."},
 "version": "1", "state": "active"}
```

Saving a model checks every table and column it names against the database. Anything missing fails with `ER_SPRIG_DIM_INVALID_MODEL` (39752) and a list of what is missing. Creating a model whose name exists fails with 39752; updating one that doesn't exist fails with `ER_SPRIG_DIM_MODEL_NOT_FOUND` (39750).

If a table or column a model uses is later dropped or renamed, the next query marks the model `invalid` and fails with 39752. Fix the tables or update the model; the next query that finds everything in place sets the model back to `active`.

## Query a model

A query names measures, the dimension levels to group by, and optional filters, totals, order and limit:

```sh
curl -s -X POST https://db.example.com:8443/v1/databases/shop/models/sales:query \
  -H 'Authorization: Bearer <api-key>' -H 'Content-Type: application/json' \
  -d '{"measures": ["revenue", "avg_qty", "buyers"],
       "dimensions": [{"dimension": "customer", "level": "country"},
                      {"dimension": "date", "level": "month"}],
       "filters": [{"dimension": "date", "level": "month", "op": "between",
                    "values": ["2026-02", "2026-05"]}],
       "totals": true}'
```

### Query fields

| Field | Type | Default | Meaning |
|---|---|---|---|
| `measures` | list of text | required | Measures to compute; all must belong to one fact |
| `dimensions` | list | `[]` | `{"dimension", "level"}` pairs to group by, outermost first |
| `filters` | list | `[]` | Conditions on dimension levels, below |
| `totals` | bool | `false` | Add a subtotal row after each group and a grand total last |
| `order` | list | by keys | `{"measure": "revenue", "desc": true}` or `{"dimension": "date", "level": "month"}`; `desc` defaults to `false` |
| `limit` | int | none | Most detail rows to return; subtotals don't count |
| `attribution` | text | each link's | `current` or `at_fact_time`, overriding every link's attribution |

A filter is `{"dimension", "level", "op", "values"}` (or a single `value`). `op` is one of:

| `op` | Meaning |
|---|---|
| `eq`, `ne` | Equal, not equal |
| `lt`, `le`, `gt`, `ge` | Less than, at most, greater than, at least |
| `in`, `not_in` | In a list; an empty `in` matches nothing |
| `between` | Inclusive range of two values |

On the time dimension, filters on `day` (`"2026-05-14"`), `month` (`"2026-05"` or `202605`) and `year` (`"2026"`) become ranges on the raw time column in UTC, so an index on that column serves them. Filters on other time levels compare the level's key value, such as `20262` for a quarter.

### The answer

```json
{
  "plan": "per_level",
  "rows": [
    {"keys": ["in", "202602"], "values": ["15234.50", "3.9500", "3"], "grouping": 0},
    {"keys": ["in", "202603"], "values": ["9876.25", "4.1200", "3"], "grouping": 0},
    {"keys": ["in", null], "values": ["25110.75", "4.0300", "3"], "grouping": 1},
    {"keys": ["us", "202602"], "values": ["8120.00", "3.7000", "2"], "grouping": 0},
    {"keys": ["us", null], "values": ["8120.00", "3.7000", "2"], "grouping": 1},
    {"keys": [null, null], "values": ["33230.75", "3.9600", "5"], "grouping": 3}
  ]
}
```

- `keys` holds one value per grouped level, and `values` one per measure, in the query's order. Values are text, so decimal sums stay exact; `avg` keeps four more decimal places than its sum, as MariaDB's `AVG` does.
- `grouping` is a bitmask of the rolled-up levels, as MySQL's `GROUPING()` computes it: the last level is the lowest bit. Detail rows have 0 and the grand total has all bits set.
- With `totals`, each subtotal follows its group, and the grand total comes last. The rows are the same as MariaDB's `GROUP BY ... WITH ROLLUP` over the same data, in the same order.
- `plan` is `local` when one statement answers the query, or `per_level` when distinct counts need one statement per rollup level (distinct counts don't add up across groups).

To stream a large answer, send `Accept: application/x-ndjson`. The reply is a `{"header": {"plan": ...}}` line, `{"rows": [...]}` lines of at most 1,000 rows, and an `{"end": {"rows": n}}` line; an error is a single `{"error": ...}` line. Over gRPC, `Analytics.Query` is a server stream of the same parts. The merge still holds every group before the first row is sent, so streaming saves client memory, not gateway memory.

### Explain and drill

`:explain` takes the same body as `:query` and returns the plan and its SQL, without running it:

```json
{"plan": "local",
 "sql": ["SELECT d1.`country` AS k1, (YEAR(f.`placed_at`) * 100 + MONTH(f.`placed_at`)) AS k2, SUM(f.`total`) AS p1 FROM `shop`.`orders` AS f LEFT JOIN `shop`.`customers` AS d1 ON d1.`id` = f.`customer_id` GROUP BY k1, k2"]}
```

`:drill` returns the next query, ready to send to `:query`. Give it the current query, the dimension, and either a member to drill into or `"up": true`:

```json
{"query": {"measures": ["revenue"],
           "dimensions": [{"dimension": "customer", "level": "country"}]},
 "dimension": "customer", "member": "in"}
```

Drilling down moves the dimension one level finer (`country` to `region`) and adds a filter on the member (`country = 'in'`). Drilling up moves one level coarser and removes the filters below it. Drilling past the finest or coarsest level fails with `ER_SPRIG_DIM_UNKNOWN_MEMBER` (39751).

## Sharding

A query's SQL runs through the router like your own SQL, so the model works whether its fact table is on one shard or many, and keeps working through splits and moves. On several shards, each shard computes partial aggregates for its rows and the gateway combines them: sums and counts add, minimums and maximums compare, and an average is a sum over a count. Distinct counts with totals run one statement per rollup level, so they stay exact. See [Shards and table groups](../concepts/shards-and-table-groups.md).

The dimension joins follow the router's usual rules for joins across shards. See [Sharding](../sql/sharding.md).

## Analytic SQL across shards

Ordinary SQL aggregates also work across shards without a model. When a `SELECT` would have to copy many rows from the shards to the gateway, the router can answer it with an analytic read instead: it streams each shard's rows into an engine inside the gateway (DataFusion) that joins, groups and aggregates them with exact integer and decimal arithmetic.

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `router.analytics.mode` | text | `fallback` | `off`; `fallback` uses an analytic read only when a gathered read would pass `router.gather.max_rows`; `prefer` tries one first |
| `router.analytics.max_rows` | int | `50000000` | Rows an analytic read may read from the shards |
| `router.analytics.spill_mb` | int | `4096` | Disk an analytic read may spill to (0: none) |
| `analytics.datafusion_memory_mb` | int | `1024` | Memory per analytic read |

The hints `/*+ SPRIG_ANALYTICS */` and `/*+ SPRIG_GATHER */` choose per statement. Analytic reads accept a single `SELECT` over tables with inner or left joins on keys, `WHERE`, `GROUP BY` columns, `COUNT`, `COUNT(DISTINCT)`, `SUM`, `AVG`, `MIN`, `MAX`, `ROW_NUMBER`, `RANK` and `DENSE_RANK` windows, `ORDER BY` and `LIMIT`. Other statements, including `GROUP BY ... WITH ROLLUP`, are gathered as before. Past a limit, the statement fails with `ER_SPRIG_PLAN_LIMIT_EXCEEDED` (39421). A `WITH ROLLUP` that would need rows from several shards fails with `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` (39420); use a model with `totals` instead. See [Query plans](../performance/query-plans.md).

## Limits

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `analytics.max_groups` | int | `1000000` | database, tenant, dc, global | Groups one query's merge may hold; more fails with `ER_SPRIG_DIM_TOO_MANY_GROUPS` (39753) |
| `analytics.query_timeout_seconds` | int | `300` | database, tenant, dc, global | Time one query may run; then it fails with MySQL error 3024 |
| `analytics.max_concurrent_per_tenant` | int | `2` | tenant, dc, global | Queries one tenant may run at once on one gateway |
| `analytics.max_concurrent_per_gateway` | int | `8` | dc, global | Queries one gateway runs at once |

A tenant or database value can't exceed the data centre or global value. A query waits up to one second for a free slot, then fails with `ER_SPRIG_RATE_LIMITED` (39780), which is safe to retry. See [Settings and scopes](../concepts/settings-and-scopes.md).

### Errors

| Code | Name | HTTP | When |
|---|---|---|---|
| 39750 | `ER_SPRIG_DIM_MODEL_NOT_FOUND` | 404 | No model of that name in the database |
| 39751 | `ER_SPRIG_DIM_UNKNOWN_MEMBER` | 400 | An unknown measure, dimension, level or filter operator, a malformed query, or a drill past the end of a hierarchy |
| 39752 | `ER_SPRIG_DIM_INVALID_MODEL` | 400 | A bad definition, a missing table or column, or a duplicate model name |
| 39753 | `ER_SPRIG_DIM_TOO_MANY_GROUPS` | 429 | More groups than `analytics.max_groups` |
| 39754 | `ER_SPRIG_DIM_NOT_DECOMPOSABLE` | 409 | Measures from two facts in one query |
| 39780 | `ER_SPRIG_RATE_LIMITED` | 429 | No query slot within one second |

Over gRPC each error maps to the status in the [error reference](../reference/errors.md).

### Metrics

| Metric | Type | Meaning |
|---|---|---|
| `sprig_dim_queries_total` | counter, by `plan` | Queries answered |
| `sprig_dim_query_duration_seconds` | histogram | Query time |
| `sprig_dim_merge_groups` | histogram | Rows each merge produced |
| `sprig_dim_rejected_total` | counter, by `reason` | Queries refused |

## Not supported

- Defining or querying models in SQL; models are managed only through the Analytics API.
- Pre-computed aggregates, materialised rollups and result caching; every query reads the tables.
- A built-in geography dimension, more than one hierarchy per dimension, parent-child dimensions, and dimensions in another database.
- Collections as facts or dimensions, and inherited dimensions.
- Measures from two facts in one query (drill-across) fail with 39754.
- `as_of` attribution and bitemporal versioning.
- Functions in measure expressions and filters.
- Reading analytics queries from replicas.

## Related

- [Data models overview](overview.md)
- [Cross-model queries](cross-model-queries.md)
- [REST and gRPC](../connect/rest-and-grpc.md)
- [Query plans](../performance/query-plans.md)
- [Error codes](../reference/errors.md)
