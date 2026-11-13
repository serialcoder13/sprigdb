---
title: Troubleshooting SprigDB
audience: [user, dba, operator]
sidebar:
  order: 1
---

# Troubleshooting SprigDB

This section is organised by symptom. Each page says what causes the symptom, how to confirm the cause with commands, statements and metrics, and how to fix it. Start here to learn where SprigDB keeps its evidence, then jump to the symptom in the [index](#symptom-index).

## Where to look

SprigDB gives you six sources of evidence. Use them in roughly this order: the error the client received, then the process that raised it, then the history behind it.

### 1. The error itself

Every SprigDB error has a number from 39000 to 39999, a name such as `ER_SPRIG_SHARD_MOVED`, a SQLSTATE and a retryable flag. The message names the setting, limit or object involved, so read it in full. Look the number up in the [error reference](../reference/errors.md), which says what each code means and what to do. [Reading SprigDB errors](reading-errors.md) shows how the same error looks in MySQL, MongoDB, REST and gRPC clients.

Errors that MariaDB raised (for example 1062, duplicate key, or 1146, table doesn't exist) pass through unchanged, except that physical schema names are translated back to your database names.

### 2. Process logs

Every `sprigdb` role (gateway, controller, worker, agent, operator) writes its log to standard output. Two keys in the `[telemetry]` section of `sprigdb.toml` control it:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `log_format` | text | `json` | `json` writes one JSON object per line; `pretty` writes multi-line text for a terminal |
| `bootstrap_log_level` | text | `info` | A `tracing` filter, such as `info` or `info,sprig_meta=debug` |

Internal detail that is never sent to clients, such as the MariaDB error behind `ER_SPRIG_BACKEND_UNAVAILABLE`, is logged with the error. When a client reports an error, search the gateway's log around that time for the tenant and the error name.

```sh
# Only the errors, from a gateway whose standard output is saved to gateway.log
jq -c 'select(.level == "ERROR")' gateway.log
```

Configuration changes in `sprigdb.toml` apply at the next restart. Settings (the `listener.*`, `limits.*` and other keys) are different: they live in the control plane and most apply live. See [Settings and scopes](../concepts/settings-and-scopes.md).

### 3. Health endpoints

Every process serves three endpoints on its telemetry port (9180 by default):

| Endpoint | Answers |
|---|---|
| `/healthz` | 200 `ok` while the process's watchdog ticks; 503 `watchdog stalled` when its runtime has been stuck for 5 seconds |
| `/readyz` | 200 `ready` while the process serves; 503 `starting` or `draining` otherwise. Load balancers use it |
| `/readyz?verbose` | The state, then one line per check: `[+]` passing, `[-]` failing |

```sh
curl -s http://gw1.example.internal:9180/readyz?verbose
```

```text
ready
[+]process pid=4127 started_unix_ms=1762934400123 version=0.0.1
[+]watchdog ticking
[+]config /etc/sprigdb/sprigdb.toml matches the running configuration
```

What the lines tell you:

- `process` gives the pid and start time. If the start time changed since you last looked, the process restarted.
- `watchdog stalled` means the process's async runtime stopped making progress. A gateway in that state fences itself: see [Failover problems](failover.md).
- `config drift` means `sprigdb.toml` on disk differs from what the process loaded. The new file applies only at the next restart.

### 4. Metrics

Every process exposes Prometheus metrics at `/metrics` on the telemetry port. The [metrics reference](../reference/metrics.md) lists all of them with their labels and units. The ones you reach for first:

| Question | Metric |
|---|---|
| Are statements failing, and whose fault is it? | `sprig_gateway_statements_total` by `result`: `ok`, `client`, `sprig` or `backend` |
| How slow are statements? | `sprig_gateway_query_duration_seconds`, and `sprig_gateway_overhead_seconds` for the gateway's own share |
| Are connections refused? | `sprig_gateway_connections_rejected_total` by `reason`, `sprig_auth_logins_total` by `outcome`, `sprig_auth_rate_limited_total` |
| Are statements queueing? | `sprig_gateway_admission_wait_seconds`, `sprig_backend_pool_wait_seconds` |
| Are replicas behind? | `sprig_backend_replica_lag_seconds`, `sprig_backend_gtid_wait_timeouts_total` |
| Are moves stuck? | `sprig_mover_catchup_lag_seconds`, `sprig_mover_cutovers_total`, `sprig_mover_errors_total` by `code` |
| Is a gateway fenced? | `sprig_gateway_fenced`, `sprig_gateway_permit_remaining_seconds` |

Statements with a tenant label appear only for tenants busier than `telemetry.tenant_series_min_rps`; quieter tenants are folded into `tenant=""`.

### 5. Traces

Every gateway keeps its most recent traces in memory and serves them as JSON at `/debug/traces` on the telemetry port. Failed statements are always kept, as are statements slower than `telemetry.trace_slow_ms` (500 ms by default); others are sampled at `telemetry.trace_sample_ratio`. Filter by an attribute of the root span:

```sh
curl -s 'http://gw1.example.internal:9180/debug/traces?attr=protocol=mysql'
```

Each trace shows the `gateway.request` span with the tenant id, protocol and result, and one `backend.query` child per round trip to MariaDB, so you can see where the time went. Traces never contain SQL text or values. A MySQL client on port 3306 can send a W3C `traceparent` query attribute to join its own trace.

### 6. sprigctl and Studio

`sprigctl` talks to the controllers' Admin API. The commands you need most when troubleshooting:

| Command | Shows |
|---|---|
| `sprigctl tenant show acme` | The tenant's state, home cluster and data centre |
| `sprigctl cluster list`, `sprigctl cluster show <id>` | Clusters, their nodes, health and capability profile |
| `sprigctl cluster probe <id> --full` | Probes a cluster now, including the XA and feature probes |
| `sprigctl gateway list` | The data centre's gateways, including draining ones |
| `sprigctl job list`, `sprigctl job show <id>` | Shard moves, splits and merges, and their state |
| `sprigctl shard list acme` | A tenant's table groups and shards |
| `sprigctl txn list` | In-doubt distributed transactions |
| `sprigctl index list`, `sprigctl index recommendations` | Automatic indexes and pending recommendations |
| `sprigctl explain query <fingerprint>` | Why an index was or wasn't built for a query shape |
| `sprigctl explain decision <id>` | Why an automatic decision (a move, an index, a failover) was made |
| `sprigctl explain placement acme` | Why a tenant lives where it does |
| `sprigctl settings get <key>` | The effective value of a setting and the scope it comes from |

Add `--output json` to any command to get machine-readable output. There is no single `status` command; start with `tenant show` and `cluster show`.

In Studio, operators sign in to **operator mode** (under `/ops/` of the Studio path) to see data centres, clusters, tenants, moves, jobs, failovers and in-doubt transactions on one screen each. Tenant users see their own automatic indexes on the **Indexing** page and their shards on the **Shards** page. See [Studio](../tools/studio.md).

From SQL, `EXPLAIN FORMAT=SPRIG <statement>` shows how the gateway routes a statement across shards, and `SHOW SPRIG SETTINGS` shows the settings in force and where each comes from.

## Symptom index

### Connecting and logging in

| Symptom | Page |
|---|---|
| TLS handshake fails, or "Connections using insecure transport are prohibited" (3159) | [Connection problems](connections.md#tls-errors) |
| "Access denied" on the first login with `caching_sha2_password` | [Connection problems](connections.md#caching_sha2_password-fails-on-the-first-login) |
| "Access denied" with a correct password | [Connection problems](connections.md#wrong-login-name-format) |
| `ER_SPRIG_TENANT_SUSPENDED` (39303) | [Connection problems](connections.md#tenant-suspended) |
| `ER_SPRIG_LOGIN_RATE_LIMITED` (39300), "Host is blocked" | [Connection problems](connections.md#address-blocked-after-failed-logins) |
| "Too many connections" (1040) or `ER_SPRIG_TENANT_CONNECTION_LIMIT` (39001) | [Connection problems](connections.md#connection-limits) |
| Connection refused on a port | [Connection problems](connections.md#listener-disabled-or-moved) |

### Slow or failing queries

| Symptom | Page |
|---|---|
| A query got slower, or is slow from the start | [Performance problems](performance.md#slow-queries) |
| A query reads every shard | [Performance problems](performance.md#scatter-queries) |
| No automatic index appears | [Performance problems](performance.md#automatic-indexing-doesnt-act) |
| Reads miss recent writes, or go to the writer only | [Performance problems](performance.md#replica-lag) |
| `ER_SPRIG_ADMISSION_TIMEOUT`, `ER_SPRIG_MEMORY_LIMIT`, `ER_SPRIG_PLAN_LIMIT_EXCEEDED` | [Performance problems](performance.md#timeouts-and-caps) |
| `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` (39420) | [Sharding and moves](sharding-and-moves.md#statements-that-cant-be-distributed) |

### Shards, moves and transactions

| Symptom | Page |
|---|---|
| A move or split never finishes | [Sharding and moves](sharding-and-moves.md#a-move-is-stuck-or-slow) |
| Writes pause for a second or two, or fail with 1205 or 1213 during a move | [Sharding and moves](sharding-and-moves.md#write-pauses-at-cutover) |
| `ER_SPRIG_CROSS_CLUSTER_WRITE_REJECTED` (39440), `ER_SPRIG_PARTIAL_COMMIT` (39442) | [Sharding and moves](sharding-and-moves.md#cross-shard-transaction-errors) |

### Failover

| Symptom | Page |
|---|---|
| A writer died and nothing took over | [Failover problems](failover.md#failover-didnt-happen) |
| A failover happened that you didn't expect | [Failover problems](failover.md#failover-happened-unexpectedly) |
| `ER_SPRIG_DC_FENCED` (39501), `ER_SPRIG_RECOVERY_FENCED` (39530), `ER_SPRIG_ADMISSION_BACKOFF` (39531) | [Failover problems](failover.md#fencing-errors) |
| A former writer was kept out of its cluster | [Failover problems](failover.md#replica-rebuilds) |

### Errors in general

| Symptom | Page |
|---|---|
| An error number you don't recognise, or a negative one | [Reading SprigDB errors](reading-errors.md) |
| Should the application retry? | [Reading SprigDB errors](reading-errors.md#retryable-errors) |

## Before you ask for help

Collect these, and you will have what anyone needs to find the cause:

1. The full error: number, name, SQLSTATE and message, and the time it happened.
2. The tenant, the protocol and port the client used, and the gateway it reached.
3. `/readyz?verbose` from that gateway.
4. The gateway log for a few minutes around the error.
5. For data-path problems, `sprigctl tenant show <tenant>` and `sprigctl cluster show <id>` for the tenant's home cluster.
6. For slow statements, the `EXPLAIN FORMAT=SPRIG` output and the trace from `/debug/traces`.

## Related

- [Reading SprigDB errors](reading-errors.md)
- [Error reference](../reference/errors.md)
- [Metrics reference](../reference/metrics.md)
- [Monitoring](../operate/monitoring.md)
- [sprigctl](../tools/sprigctl.md)
