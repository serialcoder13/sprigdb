---
title: Workload
audience: [dba, operator]
sidebar:
  order: 3
---

# Workload

Every gateway records the statements it runs, grouped by shape, with their counts and latencies. This record is the workload. You use it to find the statements that cost the most, and SprigDB uses it to build [automatic indexes](automatic-indexing.md) and to choose [table groups and keys](sharding-advisor.md). This page explains what is captured, how statements are grouped into fingerprints, where to see them, and the thresholds that decide which statements get a closer look.

## What is captured

For each tenant session, the gateway records every statement that reaches MariaDB:

- the statement's **fingerprint**, its normalized text and the database it ran in;
- counts: runs, errors, rows returned and rows affected;
- latency, measured from sending the statement to the end of its result, kept as a histogram, so p50, p95 and maximum are available;
- for **watched** fingerprints, rows examined, read from MariaDB's `Handler_read_*` counters before and after a sample of runs;
- for watched fingerprints, a few encrypted **parameter samples**, so automatic indexing can replay real statements on a replica;
- **transaction footprints**: which table groups each write transaction touched together, for the sharding advisor.

What is not captured:

- no plain statement text with values. Normalized text has every literal replaced by `?`, and parameter samples are encrypted;
- statements the gateway answers itself (DDL that goes through the catalog, settings statements, statements answered locally), which never reach MariaDB;
- sessions that log in through the interim accounts file rather than a tenant.

Statements of a client's multi-statement packet are counted, but never probed for rows examined.

### Fingerprint kinds

| Kind | Source |
|---|---|
| `sql` | SQL on the MySQL and MariaDB listeners, and SQL over HTTP |
| `mongo` | MongoDB query shapes |
| `txn` | Transaction footprints |

Automatic indexing reads `sql` and `mongo` fingerprints. Statements run from Studio are kept apart (caller `studio`) so you can tell them from your applications' statements.

## Fingerprints

A fingerprint is a 64-bit hash of the statement's kind, database, calling function and normalized text, written as 16 lowercase hex digits, such as `9c1e5f0a77b2d310`. Two statements with the same fingerprint differ only in their literal values.

Normalization:

- drops comments, but keeps optimizer hints (`/*+ ... */`) and the content of executable comments the persona runs (`/*!50001 ... */`, and `/*M!100100 ... */` on MariaDB);
- replaces literals and placeholders with `?`: numbers, strings, hex and bit values, typed literals such as `DATE '...'`, `TRUE` and `FALSE`;
- folds lists: `IN (?, ?, ?)` becomes `IN (...)`, and a multi-row `VALUES (...), (...)` becomes `VALUES (...)`;
- upper-cases keywords and function names, lower-cases column names, and qualifies tables with their logical database (`shop.orders`);
- joins tokens with single spaces and cuts the text at `workload.max_text_bytes` (4096).

```sql
SELECT * FROM orders WHERE customer_id = 42 AND status IN ('open', 'held') LIMIT 10;
SELECT * FROM orders WHERE customer_id=7 AND status IN ('open') LIMIT 50;
```

Both have the same fingerprint, with this text:

```text
SELECT * FROM shop.orders WHERE customer_id = ? AND status IN (...) LIMIT ?
```

The same statement in two databases has two fingerprints, because the database is part of the hash.

## See the workload

### In Studio

Open **Performance**, then **Automatic indexing**, then the **Fingerprints** tab. It lists the top fingerprints by total time, with:

| Column | Meaning |
|---|---|
| Statement | Normalized text (the full fingerprint id is in the tooltip) |
| Database | The database it ran in |
| Runs | Executions in the window |
| Total ms | Time spent in all of them |
| p50 ms, p95 ms | Median and 95th-percentile latency |
| Examined per returned | Rows examined per row returned, from probed runs; a high number means a scan |
| Unsargable | Predicates no index can serve, such as a function wrapped around a column |
| Source | `Application`, `Studio`, or `function:<id>` for statements run by a logic function |

Choose the window (1 hour, 24 hours or 7 days) and the source (all, applications or Studio). Select **Series** on a row to chart its p50 and p95 per bucket over the last seven days, with the same numbers as a table. That chart is how you see the effect of an index: the p95 drops at the bucket where it was built.

### With the REST API

| Method and path | Does |
|---|---|
| `GET /v1/workload/fingerprints?hours=&limit=&source=` | Top fingerprints by total time over the last `hours` |
| `GET /v1/workload/fingerprints/{id}/series?days=` | One fingerprint per bucket over the last `days`: runs, errors, p50, p95, total |

| Parameter | Default | Range | Meaning |
|---|---|---|---|
| `hours` | 24 | 1 to 2160 | Window, from the start of the hour |
| `limit` | 50 | 1 to 500 | Fingerprints returned |
| `source` | `all` | `all`, `client`, `studio` | Which callers |
| `days` | 7 | 1 to 90 | Series length |

```sh
curl -s 'https://gw.example.com:8443/v1/workload/fingerprints?hours=24&limit=20&source=client' \
  -H 'Authorization: Bearer <token>'
```

```json
{
  "hours": 24,
  "fingerprints": [
    {
      "id": "9c1e5f0a77b2d310",
      "kind": "sql",
      "stmt_type": "SELECT",
      "db": "shop",
      "text": "SELECT * FROM shop.orders WHERE customer_id = ? AND status IN (...) LIMIT ?",
      "caller": null,
      "runs": 18204,
      "errors": 0,
      "total_ms": 412880.5,
      "p50_ms": 12.4,
      "p95_ms": 61.9,
      "max_ms": 840.2,
      "rows_returned": 120511,
      "examined_per_returned": 3.1,
      "unsargable": [],
      "first_seen_ms": "1762128000000",
      "last_seen_ms": "1762988400000"
    }
  ]
}
```

Both calls read your own tenant only. You see a fingerprint when you hold any privilege on its database (or on `*.*`), or are a tenant administrator: normalized text has no values, but it names tables. A fingerprint you may not see answers MySQL error 1146 (`no such fingerprint in this tenant`), as does an unknown one.

### In SQL: performance_schema

On the MySQL listener (3306), `performance_schema.events_statements_summary_by_digest` and the `sys` views over it (`sys.statement_analysis`, `sys.statements_with_errors_or_warnings`, and their `x$` forms) are answered from the workload recorder, so tools that read them work:

```sql test
SELECT DIGEST, DIGEST_TEXT, COUNT_STAR, SUM_TIMER_WAIT / 1e12 AS total_s
FROM performance_schema.events_statements_summary_by_digest
ORDER BY SUM_TIMER_WAIT DESC
LIMIT 10;
```

Differences from MySQL:

- `DIGEST` is the 16-hex-digit fingerprint, not a 64-digit SHA-256.
- The rows are **this gateway's** counts since it first saw each statement, or since you last ran `TRUNCATE` on the table. With several gateways, each shows its own share; use the Studio view or the REST API for the whole tenant.
- Times are in picoseconds and include the network between the gateway and MariaDB.
- `SUM_ROWS_EXAMINED` adds up only probed runs. The `QUANTILE_*` columns are histogram bucket bounds.
- Counters MariaDB can't give per statement (lock time, temporary tables, sorts, index use, memory, warnings) are 0, and `QUERY_SAMPLE_TEXT` is `NULL`.
- You see digests of the databases you may see, plus statements run with no current database.

### For MongoDB clients

The `$queryStats` aggregation stage lists MongoDB query shapes, including the shapes kept in the workload store.

### For operators

`sprigctl explain query <fingerprint> --tenant acme` explains what automatic indexing decided for a fingerprint. See [Working with automatic indexes](automatic-indexing.md#find-out-why-an-index-wasnt-built).

## Watched fingerprints and slow statements

Measuring rows examined costs an extra round trip, so the gateway only probes fingerprints that look slow. A fingerprint is **watched** when its p95 is at least `workload.watch_p95_ms` (50 ms), or when automatic indexing found it worth an index. For a watched fingerprint:

- `workload.examined_sample_rate` (2%) of its runs are probed for rows examined;
- every run is probed once its mean latency reaches `workload.full_probe_ms` (100 ms);
- `workload.sample_rate` (1%) of its runs offer a parameter sample, of which each gateway keeps at most `workload.samples_per_bucket` (3) per bucket.

SprigDB has several thresholds that mean "slow". They do different things:

| Setting | Default | Effect |
|---|---|---|
| `workload.watch_p95_ms` | 50 | Fingerprints at or above this p95 are watched: probed and sampled |
| `workload.full_probe_ms` | 100 | Every run of a watched fingerprint with this mean latency is probed |
| `auto_index.p95_threshold_ms` | 100 | With `auto_index.min_runs` (50) runs, a p95 above this makes a statement a candidate for an index |
| `telemetry.trace_slow_ms` | 500 | Every statement slower than this, and every failed one, has its trace kept and exported |

There is no slow-query log file. To keep a record of individual slow statements, set `telemetry.otlp_endpoint` to an OpenTelemetry collector: every statement slower than `telemetry.trace_slow_ms` is exported as a trace, carrying its fingerprint and ids but never its text or values. The most recent kept traces are also served at `/debug/traces` on the telemetry port (9180). See [Monitoring](../operate/monitoring.md).

`telemetry.trace_slow_ms` is set by operators only, for a tenant, a data centre or everywhere:

```sh
sprigctl settings set --scope tenant:acme telemetry.trace_slow_ms 200 --reason "investigating checkout latency"
```

The `workload.*` thresholds can be set per table, but see the note under [Settings](#settings) on when gateways read them.

## Parameter samples

A parameter sample is one execution's literal values with the session context replay needs (database, `sql_mode`, time zone, character set and collation). Samples are encrypted with AES-256-GCM under a key derived for the tenant, and bound to the tenant and fingerprint, so a sample copied elsewhere can't be opened. Only automatic indexing's replay opens them, and it never logs a value.

Samples need a key file on every gateway and worker, named by `[gateway] workload_key_file` in the configuration file. Without one, statistics are still captured, but no samples, so automatic indexing validates by estimate and canary instead of by replay. The file holds one secret per line, `<version> <64 hex digits>`; blank lines and `#` comments are ignored:

```text
# workload sample keys
1 6f1c0b9e4d0a3c2b8e7f60514233aa98c1d2e3f405162738495a6b7c8d9e0f1a
```

Generate a secret with `openssl rand -hex 32`. To rotate, add a line with a higher version: new samples use it at once, and older samples still open under theirs. Remove the old version after `workload.sample_retention_days` (7), when every sample sealed under it has expired.

To stop capturing samples for a table or a whole tenant:

```sql test
CREATE TABLE customers (id BIGINT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(200));
ALTER TABLE customers SPRIG SET (workload.capture_samples = FALSE);
```

For the whole tenant, `SPRIG SET workload.capture_samples = FALSE` (this needs `SPRIG_SETTINGS_ADMIN`).

## How long it is kept

Each gateway aggregates in memory per bucket of `workload.bucket_minutes` (60) and writes to the tenant's catalog every `workload.flush_seconds` (900), off the statement path. `workload.rollup_delay_minutes` (10) after a bucket closes, every gateway's rows for it are merged into one row per fingerprint. Hourly rows older than seven days are merged into one row per day. Statistics older than `workload.retention_days` (30) are deleted, along with fingerprints not seen within that time.

## Settings

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `workload.bucket_minutes` | int | 60 | tenant, global | Statistics bucket size |
| `workload.retention_days` | int | 30 | tenant, global | How long statistics are kept |
| `workload.max_fingerprints_per_tenant` | int | 10000 | tenant, global | Fingerprints per tenant per gateway; new ones beyond it count under "other" |
| `workload.max_text_bytes` | int | 4096 | tenant, global | Longest normalized text |
| `workload.watch_p95_ms` | int | 50 | object to global | p95 that makes a fingerprint watched |
| `workload.examined_sample_rate` | float | 0.02 | object to global | Share of a watched fingerprint's runs probed for rows examined |
| `workload.full_probe_ms` | int | 100 | object to global | Mean latency from which every run is probed |
| `workload.capture_samples` | bool | true | object to global | Capture parameter samples |
| `workload.sample_rate` | float | 0.01 | object to global | Share of a watched fingerprint's runs that offer a sample |
| `workload.samples_per_bucket` | int | 3 | tenant, global | Samples kept per fingerprint per gateway per bucket |
| `workload.max_samples_per_fingerprint` | int | 50 | tenant, global | Stored samples kept per fingerprint |
| `workload.sample_retention_days` | int | 7 | tenant, global | Sample lifetime |
| `workload.max_in_list_values` | int | 1000 | tenant, global | `IN` list values kept in a sample |
| `workload.max_memory_mb` | int | 128 | dc, global | Recorder memory per gateway |
| `workload.flush_seconds` | int | 900 | dc, global | How often gateways write partial buckets |
| `workload.rollup_delay_minutes` | int | 10 | dc, global | Wait after a bucket closes before the rollup |

"Object to global" means the object, database, tenant and global scopes.

Each gateway reads the recorder's settings (`bucket_minutes`, `max_memory_mb`, `max_fingerprints_per_tenant`, `watch_p95_ms`, `examined_sample_rate`, `full_probe_ms`, `sample_rate`, `samples_per_bucket`, `max_text_bytes`, `max_in_list_values`, `sample_retention_days`) at the global and data-centre scopes, and applies a change live, without a restart. `workload.capture_samples` is resolved for the tenant before each sample, and the flush and rollup settings apply live.

## Memory caps

The recorder never grows without bound. Past `workload.max_memory_mb`, the least recently used fingerprints are written out early; one tenant may use only part of the memory, so a busy tenant can't push others out; and past `workload.max_fingerprints_per_tenant`, new shapes are counted under a reserved "other" fingerprint of their kind. These metrics show the pressure:

| Metric | Meaning |
|---|---|
| `sprig_workload_memory_bytes` | Recorder memory in use |
| `sprig_workload_fingerprints_active` | Fingerprints held in memory |
| `sprig_workload_other_total` | Runs counted under "other" |
| `sprig_workload_flush_late_dropped_total` | Closed rows dropped because the flusher fell behind |
| `sprig_workload_probes_total`, `sprig_workload_probes_run_total` | Probes planned and run |
| `sprig_workload_samples_kept_total`, `sprig_workload_samples_skipped_total` | Parameter samples kept and skipped |

A steady rise in `sprig_workload_other_total` usually means an application builds statements with literal values in places normalization can't fold, such as table names; fix the application or raise the cap.

## Related

- [Working with automatic indexes](automatic-indexing.md)
- [Query plans](query-plans.md)
- [EXPLAIN and workload in SQL](../sql/explain-and-workload.md)
- [Monitoring](../operate/monitoring.md)
- [Settings reference](../reference/settings.md)
