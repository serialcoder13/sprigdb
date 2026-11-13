---
title: Limits and timeouts
audience: [user, dba, operator]
sidebar:
  order: 6
---

# Limits and timeouts

SprigDB is shared by many tenants, so every gateway limits what one tenant, one user and one statement may use. This page lists each limit, its setting and default, and the error your application sees when it is reached, so you can tell a limit from a fault and decide whether to retry.

Errors marked **retry: yes** are transient: back off and try again. The full list is in [Error codes](../reference/errors.md).

## Statement timeouts

### In SQL sessions

Set a timeout per session, or per statement, with the variables your client already knows. On the MySQL listener (3306), MySQL's names work and the gateway translates them to MariaDB's:

| You send (3306) | MariaDB runs |
|---|---|
| `SET SESSION max_execution_time = 5000` | `max_statement_time = 5` (seconds) |
| `SELECT /*+ MAX_EXECUTION_TIME(5000) */ ...` | `SET STATEMENT max_statement_time = 5 FOR SELECT ...` |
| `/*+ SET_VAR(v = x) */` after the first word | `SET STATEMENT v = x FOR ...` |

On the MariaDB listener (3307), use `max_statement_time` (in seconds) directly, as on MariaDB.

```sql test
SET SESSION max_execution_time = 5000;
SELECT /*+ MAX_EXECUTION_TIME(2000) */ COUNT(*) FROM information_schema.TABLES;
SET SESSION max_execution_time = 0;
```

A statement that runs out of time fails with error 3024 (`HY000`, "Query execution was interrupted, maximum statement execution time exceeded") on 3306, and MariaDB's 1969 on 3307.

One difference from MySQL: MySQL's `max_execution_time` limits only `SELECT`, but MariaDB's `max_statement_time` limits every statement, so a session that sets it also bounds its writes.

The session's timeout survives connection pooling: the gateway replays session variables on whichever backend connection runs the next statement.

### Over the REST API

`POST /v1/databases/{database}/sql:query` accepts a timeout and result caps in the request body:

| Field | Default | Meaning |
|---|---|---|
| `timeout_ms` | 0 (none) | Statement timeout; past it the statement is killed and the call answers error 3024 |
| `max_rows` | 0 (none) | Rows kept per result; at the cap the result ends with `truncated: true` and the statement is killed |
| `max_bytes` | 0 (none) | Cell bytes kept per result, the same way |

```sh
curl -s https://gw.example.com:8443/v1/databases/shop/sql:query \
  -H 'Authorization: Bearer <token>' -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM orders", "max_rows": 1000, "timeout_ms": 5000}'
```

A truncated result is not an error: check `truncated` on the result's end and narrow the query or page through it.

`sql:execute`, which returns the whole result in one response, buffers it in the gateway and refuses a result over `api.rest.max_buffered_bytes` (16 MiB) with `ER_SPRIG_RESULT_TOO_LARGE` (39785). Stream large results with `sql:query` and `Accept: application/x-ndjson` instead.

### In Studio

Studio sessions are clamped to the tenant's Studio limits, whatever the browser asks for:

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `studio.statement_timeout_ms` | int | 30000 | object, database, tenant, global | Statement timeout |
| `studio.max_result_rows` | int | 10000 | object, database, tenant, global | Rows held per result |
| `studio.max_result_mb` | int | 64 | dc, global | Browser memory per result |
| `studio.cursor_idle_seconds` | int | 60 | tenant, dc, global | Idle time before a paging cursor closes and its statement is killed |
| `studio.cursor_max_open` | int | 4 | tenant, global | Open cursors per user per gateway; opening another closes the least recently used |

Raise them for a database where people run long reports:

```sql test
ALTER DATABASE SPRIG SET (studio.statement_timeout_ms = 120000, studio.max_result_rows = 50000);
ALTER DATABASE SPRIG RESET (studio.statement_timeout_ms, studio.max_result_rows);
```

`ALTER DATABASE` without a name means the current database. A cursor fetched after it closed fails with `ER_SPRIG_STUDIO_CURSOR_EXPIRED` (39935).

## Connection limits

| Limit | Setting | Default | When reached |
|---|---|---|---|
| Client connections per gateway | `gateway.max_connections` | 20000 | MySQL clients get error 1040 (`Too many connections`) in place of the greeting; HTTP gets 503 |
| Sessions per tenant per gateway | `limits.tenant_connections` | 2000 | `ER_SPRIG_TENANT_CONNECTION_LIMIT` (39001, `08004`), retry: yes, after login |
| Connection phase (PROXY header, TLS handshake and login) | `gateway.handshake_timeout_ms` | 10000 | The connection is closed |

A tenant session's `@@max_connections` reports the tenant's limit, so drivers that check free connections see the right number.

With several gateways, each limit applies on each gateway. A tenant behind four gateways may hold up to four times `limits.tenant_connections` sessions.

### Backend connections

Gateways pool connections to MariaDB, so thousands of client sessions share far fewer backend connections. A statement that can't get a backend connection within `backend.acquire_timeout_ms` (2000) fails with `ER_SPRIG_BACKEND_POOL_TIMEOUT` (39031), retry: yes.

| Setting | Default | Meaning |
|---|---|---|
| `backend.max_conns_per_node` | 256 | Backend connections per gateway to one MariaDB node, across its pools |
| `backend.max_conns_per_account_node` | 32 | Backend connections per gateway for one backend account on one node |
| `backend.acquire_timeout_ms` | 2000 | Wait for a backend connection |
| `backend.pool_mode` | `transaction` | `transaction` (connections return to the pool between transactions) or `session` (each session keeps one) |

`backend.pool_mode = session` gives each client session its own backend connection for its whole life, which uses many more backend connections; use it only for applications that depend on connection state the gateway can't replay.

## Admission control

Each gateway limits how many statements run at once, per tenant and per user. A statement over the limit waits in its tenant's queue; statements of sessions inside a transaction go first, so open transactions finish and release their locks. `COMMIT`, `ROLLBACK`, pings and closes never wait.

| Limit | Setting | Default | Scopes |
|---|---|---|---|
| Statements a tenant runs at once per gateway | `limits.tenant_active_statements` | 256 | tenant, dc, global (operator only) |
| Statements one user runs at once per gateway | `limits.user_active_statements` | 64 | tenant, dc, global |
| Longest wait in the queue | `limits.queue_timeout_ms` | 5000 | tenant, dc, global |

A statement that waits longer than `limits.queue_timeout_ms` fails with `ER_SPRIG_ADMISSION_TIMEOUT` (39002), retry: yes (HTTP 429). It means the tenant is sending more concurrent work than its limit on that gateway: spread the load over more gateways, reduce concurrency in the application's pool, or ask your operator for a higher limit.

`sprig_gateway_admission_wait_seconds` shows how long statements wait to be admitted.

## Memory limits

What a gateway buffers (results it merges across shards, HTTP results it holds) is charged to three nested budgets. A charge succeeds only if all three have room; otherwise the statement fails with `ER_SPRIG_MEMORY_LIMIT` (39003), retry: yes.

| Budget | Setting | Default |
|---|---|---|
| The gateway | `gateway.memory_limit_mb` | 0: 75% of the container's memory limit (unlimited without one) |
| A tenant on one gateway | `limits.tenant_memory_mb` | 1024 |
| A user on one gateway | A quarter of the tenant's | 256 |

One statement's merging is also limited by `router.max_statement_memory_mb` (256), which fails with `ER_SPRIG_ROUTER_MEMORY_LIMIT` (39422). A gathered read that copies more than `router.gather.max_rows` (100,000) rows fails with `ER_SPRIG_PLAN_LIMIT_EXCEEDED` (39421) when no analytic read can take it over. See [Query plans](query-plans.md#limits-on-one-statement).

The MySQL wire relay is bounded by `gateway.relay_buffer_kb` (256 KB) per session: when a client reads slowly, the gateway stops reading from MariaDB rather than buffering the result.

### Packet and request sizes

| Limit | Setting | Default | When reached |
|---|---|---|---|
| Largest MySQL packet | `gateway.max_allowed_packet_bytes` | 64 MiB | MySQL's packet-too-large error |
| Long data per prepared statement | `gateway.long_data_max_bytes` | 64 MiB | `ER_SPRIG_LONG_DATA_LIMIT` (39011) at execute |
| Prepared statements per session | `gateway.max_prepared_per_session` | 1024 | The next prepare fails |
| HTTP request body | `api.max_request_bytes` | 16 MiB | `ER_SPRIG_REQUEST_TOO_LARGE` (39787) |
| Buffered REST result | `api.rest.max_buffered_bytes` | 16 MiB | `ER_SPRIG_RESULT_TOO_LARGE` (39785) |
| HTTP/2 streams per connection | `api.max_concurrent_streams` | 100 | The client waits for a stream |

## Rate limits

| What | Setting | Default | When reached |
|---|---|---|---|
| Open change streams per tenant per gateway | `api.stream.max_per_tenant` | 1000 | `ER_SPRIG_RATE_LIMITED` (39780) |
| Open REST transaction handles per tenant per gateway | `api.txn.max_open_per_tenant` | 256 | `ER_SPRIG_TENANT_CONNECTION_LIMIT` (39001) |
| Analytics API queries running at once | `analytics.max_concurrent_per_tenant`, `analytics.max_concurrent_per_gateway` | 2 per tenant, 8 per gateway | `ER_SPRIG_RATE_LIMITED` (39780), retry: yes |
| Failed logins | `auth.*` | see [Accounts and logins](../security/accounts-and-logins.md) | `ER_SPRIG_LOGIN_RATE_LIMITED` (39300) |

A change stream whose client can't keep up past `api.stream.max_buffered_events` (10,000) buffered events is closed with `ER_SPRIG_STREAM_SLOW_CONSUMER` (39790).

### REST transaction handles

A transaction opened with `POST /v1/databases/{database}/transactions` lives at most `api.txn.max_lifetime_ms` (60000) and may sit idle at most `api.txn.idle_timeout_ms` (10000) between calls. Past either, it is rolled back, and the next call with its handle fails with `ER_SPRIG_TXN_HANDLE_EXPIRED` (39782).

## Limits while a data centre recovers

When a data centre comes back after an outage, gateways admit new connections in steps so the returning clusters aren't flooded. A MySQL client over the current step's rate gets `ER_SPRIG_ADMISSION_BACKOFF` (39531) in place of the greeting, with a retry delay of 1 to 5 seconds; HTTP clients get 503 with `Retry-After`. The rate starts at `recovery.admission_connects_per_second` (200) per gateway. Connection pools that retry with backoff ride through this without errors reaching the application.

## How tenant limits are resolved

Each gateway resolves the `limits.*` settings for the session's tenant: the tenant's own value, then its ancestors', then the data centre's, then the global one. So one tenant can have its own limits, and a parent tenant's limits cover its descendants unless they set their own:

```sql
ALTER TENANT acme SPRIG SET (limits.user_active_statements = 128);
```

```sh
sprigctl settings set --scope dc:dc-a limits.tenant_connections 5000 --reason "bigger tenants in dc-a"
```

Changes apply live, without restarting gateways. The gateway-wide limits (`gateway.max_connections`, `gateway.memory_limit_mb`) are set per data centre or globally. `limits.tenant_active_statements` and `limits.tenant_connections` are operator-only.

## Errors at a glance

| Code | Name | Retry | Meaning |
|---|---|---|---|
| 1040 | `Too many connections` | yes | The gateway is at `gateway.max_connections` |
| 3024 | statement timeout (3306) | no | The statement ran past its timeout |
| 39001 | `ER_SPRIG_TENANT_CONNECTION_LIMIT` | yes | The tenant is at its session or transaction-handle limit on this gateway |
| 39002 | `ER_SPRIG_ADMISSION_TIMEOUT` | yes | The statement waited too long for admission |
| 39003 | `ER_SPRIG_MEMORY_LIMIT` | yes | A memory budget is full |
| 39011 | `ER_SPRIG_LONG_DATA_LIMIT` | no | Too much long data for one prepared statement |
| 39031 | `ER_SPRIG_BACKEND_POOL_TIMEOUT` | yes | No backend connection within `backend.acquire_timeout_ms` |
| 39421 | `ER_SPRIG_PLAN_LIMIT_EXCEEDED` | no | A gathered or analytic read passed its row limit |
| 39422 | `ER_SPRIG_ROUTER_MEMORY_LIMIT` | no | One statement's merge passed `router.max_statement_memory_mb` |
| 39531 | `ER_SPRIG_ADMISSION_BACKOFF` | yes | The data centre is recovering; reconnect after the delay |
| 39780 | `ER_SPRIG_RATE_LIMITED` | yes | A per-tenant rate or concurrency limit |
| 39782 | `ER_SPRIG_TXN_HANDLE_EXPIRED` | no | A REST transaction handle passed its lifetime or idle limit |
| 39785 | `ER_SPRIG_RESULT_TOO_LARGE` | no | A buffered REST result is too large; stream it |
| 39787 | `ER_SPRIG_REQUEST_TOO_LARGE` | no | The HTTP request body is too large |
| 39790 | `ER_SPRIG_STREAM_SLOW_CONSUMER` | yes | A change-stream client fell too far behind |

## Related

- [Query plans](query-plans.md)
- [Read routing](read-routing.md)
- [Reading errors](../troubleshooting/reading-errors.md)
- [Error codes](../reference/errors.md)
- [Settings reference](../reference/settings.md)
