---
title: REST and gRPC
audience: [user, dba]
sidebar:
  order: 8
---

# REST and gRPC

Every gateway serves an HTTP API: REST with JSON, and gRPC, on the same ports. Use it from languages or places where a database driver is awkward: serverless functions, scripts with `curl`, browsers through a backend, or services that already speak gRPC. Requests run with the caller's own rights, through the same engine as the MySQL protocol, so names, privileges, row-level security and errors are the same.

This page covers connecting, authentication and the SQL API. Every route is listed in the [REST API reference](../reference/api.md).

## Enable the listeners

Both listeners are on by default. Studio is served on them too.

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `listener.http.enabled` | bool | `true` | Serve HTTP on 8080 |
| `listener.http.bind` | text | `0.0.0.0:8080` | Address and port |
| `listener.http.tls` | text | `off` | `required`, `optional` or `off` |
| `listener.https.enabled` | bool | `true` | Serve HTTPS on 8443 |
| `listener.https.bind` | text | `0.0.0.0:8443` | Address and port |
| `listener.https.tls` | text | `required` | `required`, `optional` or `off` |
| `listener.http.cert`, `listener.https.cert` | text | `default` | Certificate bundles |

Use 8443 for anything that leaves the machine: Basic authentication and logins carry the password. A plaintext request to a listener that requires TLS gets `400 Bad Request`, "This listener needs TLS.".

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `api.max_request_bytes` | int | `16777216` | Largest request body; more fails with 413 (39787) |
| `api.max_concurrent_streams` | int | `100` | HTTP/2 streams per connection |
| `api.rest.max_buffered_bytes` | int | `16777216` | Largest result of a non-streamed call (39785 beyond) |
| `api.stream_batch_rows` | int | `500` | Rows per streamed batch |
| `api.txn.idle_timeout_ms` | int | `10000` | Idle time before a transaction handle expires |
| `api.txn.max_lifetime_ms` | int | `60000` | Longest life of a transaction handle |
| `api.txn.max_open_per_tenant` | int | `256` | Open transaction handles per tenant per gateway |

### HTTP versions

| | 8080 | 8443 |
|---|---|---|
| HTTP/1.1 | yes | yes |
| HTTP/2 | cleartext with prior knowledge (h2c) | ALPN `h2` |
| gRPC | over h2c | over TLS with ALPN `h2` |

A request whose `content-type` starts with `application/grpc` goes to gRPC; anything else goes to REST. gRPC over HTTP/1.1 fails with 415 (39791).

## Authenticate

Every data call carries an `Authorization` header (gRPC: `authorization` metadata). Three credentials work:

| Credential | Header | Notes |
|---|---|---|
| Password | `Authorization: Basic base64(acme.app:<password>)` | Checked like a MySQL login of the same user, on every request; after the first, a request costs two SHA-256 hashes |
| API key | `Authorization: Bearer sprig_acme_<key-id>_<secret>` | The key names its tenant and user |
| Session token | `Authorization: Bearer sprigat_...` | From `POST /v1/auth/login`, short-lived |

With Basic, the user name names the tenant: `acme.app` is user `app` of tenant `acme`. Alternatively send the plain user name with the header `x-sprig-tenant: acme`.

The checks are those of every other protocol: account locks, expired passwords (refused with 1862, since HTTP has no sandbox mode), `REQUIRE SSL`, `auth.require_tls`, failure delays and the per-address limit.

| Refusal | Status | Body `code` |
|---|---|---|
| Wrong or missing credentials, locked or expired account | 401, with `WWW-Authenticate: Basic realm="sprigdb", Bearer` | 1045, 3118, 3955 or 1862 |
| Tenant suspended | 403 | 39303 |
| Too many failed logins from your address | 429, with `Retry-After` | 39300 |
| Credentials can't be checked right now | 503, with `Retry-After` | 39301 |

### API keys

Create a key for yourself with any credential. The reply is the only time the key is shown:

```sh
curl -u 'acme.app:<password>' https://db.example.com:8443/v1/api-keys \
  -H 'content-type: application/json' \
  -d '{"name": "billing-service", "expires_days": 90, "allowed_cidrs": ["10.0.0.0/8"]}'
```

```json
{"key_id": "<key-id>", "key": "sprig_acme_<key-id>_<secret>", "name": "billing-service"}
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | empty | A label |
| `user` | string | yourself | Another user of the tenant; needs `CREATE USER` or tenant administration |
| `host` | string | `%` | The other user's host part |
| `expires_days` | int | never | Days until the key expires, up to 3,650 |
| `allowed_cidrs` | list of strings | any address | Addresses the key may be used from |

`GET /v1/api-keys` lists keys without their secrets, and `POST /v1/api-keys/{key_id}:revoke` revokes one. See [API keys and tokens](../security/api-keys-and-tokens.md).

### Session tokens

`POST /v1/auth/login` exchanges a password for an access token that lives `auth.token_ttl_seconds` (900 s by default), as Studio and `sprig` do:

```sh
curl https://db.example.com:8443/v1/auth/login -H 'content-type: application/json' \
  -d '{"login": "acme.app", "password": "<password>", "refresh_transport": "body"}'
```

```json
{"access_token": "sprigat_...", "token_type": "Bearer", "expires_in": 900, "session_id": "...", "refresh_token": "..."}
```

With `"refresh_transport": "body"` the refresh token comes in the body; otherwise it is set as an `HttpOnly` cookie, and later cookie calls need the `X-Sprig-CSRF` header. `POST /v1/auth/refresh` rotates it and `POST /v1/auth/logout` ends the session. When the tenant requires a second factor (`auth.mfa_required`), the login answers with a challenge that `POST /v1/auth/mfa` completes. A password login over plain HTTP from another host is refused with 403 (39921), unless `studio.allow_insecure_http` is on.

## Run SQL

`POST /v1/databases/{database}/sql:query` runs SQL and streams the results. `{database}` is the database `USE` would name; `-` means none.

```sh
curl -u 'acme.app:<password>' https://db.example.com:8443/v1/databases/shop/sql:query \
  -H 'content-type: application/json' -H 'accept: application/x-ndjson' \
  -d '{"sql": "SELECT status, COUNT(*) AS n FROM orders GROUP BY status"}'
```

With `Accept: application/x-ndjson` the answer is one JSON object per line: a `header` with the columns, `rows` in batches of up to `api.stream_batch_rows`, and an `end` with the counts. Column objects also carry `org_name`, `table`, `database`, `flags`, `charset`, `decimals` and `length`; they are trimmed here:

```text
{"header":{"index":0,"columns":[{"name":"status","type":"VARCHAR","mysql_type":253},{"name":"n","type":"BIGINT","mysql_type":8}]}}
{"rows":{"rows":[["open","1"],["paid","1"]]}}
{"end":{"index":0,"affected_rows":"0"}}
```

Without that header, the same objects come as one JSON array.

`POST /v1/databases/{database}/sql:execute` takes the same request and returns every result in one reply, up to `api.rest.max_buffered_bytes`:

```json
{"results": [{"columns": [...], "rows": [["open", "1"], ["paid", "1"]], "affected_rows": "0"}]}
```

### The request

| Field | Type | Default | Meaning |
|---|---|---|---|
| `sql` | string | - | The statement. Several statements separated by `;` need `multi_statements` |
| `multi_statements` | bool | `false` | Allow several statements, each with its own result |
| `transaction` | string | empty | A handle from `Transactions.Begin`; empty runs in a fresh autocommit session |
| `max_rows` | int | no limit | Rows kept per result; a result that reaches it ends with `truncated` and the statement is killed |
| `max_bytes` | int | no limit | Cell bytes kept per result, as `max_rows` |
| `timeout_ms` | int | none | Statement timeout; past it the statement is killed with 3024 |
| `language` | string | `sql` | `sql`, `cypher`, `gql` or `mongo` |
| `output` | string | `relaxed_ejson` | For `mongo`: `relaxed_ejson` or `canonical_ejson` |

There are no bound parameters: the statement is SQL text. Quote values yourself, or call a [logic function](../data-models/logic.md) with `POST /v1/databases/{database}/functions/{name}:call`.

### Values

- Each cell is a string in MySQL's text form (`"42"`, `"59.90"`, `"2026-11-07 10:15:00"`), or `null`. Numbers stay exact.
- A column with `"binary": true` holds the base64 of each value's bytes.
- 64-bit counts (`affected_rows`, `last_insert_id`, `length`) are strings, as in protobuf's JSON mapping.
- `end` carries `last_insert_id`, `warnings` and `info` when they are set, `more_results` when another result follows, and `truncated` when a cap cut the result.

### Other languages

`language` runs other statement types through the same call:

- `cypher` and `gql` run a graph query, with `{database}` naming the graph as Bolt's database does (`shop.social`). See [Graphs](../data-models/graphs.md).
- `mongo` runs one MongoDB command, given as Extended JSON in `sql`:

```sh
curl -u 'acme.app:<password>' https://db.example.com:8443/v1/databases/shop/sql:query \
  -H 'content-type: application/json' \
  -d '{"language": "mongo", "sql": "{\"find\": \"events\", \"filter\": {\"customer_id\": 42}}"}'
```

These languages take no `transaction` and no `multi_statements` (39794).

## Transactions

A transaction handle keeps one session open across calls:

```sh
curl -u 'acme.app:<password>' https://db.example.com:8443/v1/databases/shop/transactions \
  -H 'content-type: application/json' -d '{}'
```

```json
{"name": "transactions/<handle>", "idle_deadline_ms": "1794485710000", "deadline_ms": "1794485760000"}
```

Pass `name` as `transaction` in each statement, then commit or roll back:

```sh
curl -u 'acme.app:<password>' https://db.example.com:8443/v1/databases/shop/sql:execute \
  -H 'content-type: application/json' \
  -d '{"transaction": "transactions/<handle>", "sql": "UPDATE orders SET status = '\''paid'\'' WHERE id = 2"}'

curl -u 'acme.app:<password>' -X POST https://db.example.com:8443/v1/transactions/<handle>:commit \
  -H 'content-type: application/json' -d '{}'
```

- `{"read_only": true}` in `Begin` starts a read-only transaction.
- A handle lives on the gateway that opened it. A call that reaches another gateway is forwarded there with your credentials, so any gateway accepts it.
- A handle idle for `api.txn.idle_timeout_ms` (10 s), or older than `api.txn.max_lifetime_ms` (60 s), is rolled back; using it afterwards fails with 39782.
- Only the principal that opened a handle may use it (39783). An unknown handle is 39781.
- At most `api.txn.max_open_per_tenant` handles per tenant are open on a gateway.

## Other APIs

| Area | Routes | See |
|---|---|---|
| Catalog | `GET /v1/catalog`, `GET /v1/databases/{database}/catalog`, `GET /v1/databases/{database}/catalog/{kind}/{name}` | Databases, tables, views, routines and graphs you can see |
| Cursors | `POST /v1/databases/{database}/cursors`, `POST /v1/cursors/{cursor}:fetch`, `POST /v1/cursors/{cursor}:close` | Page through a large read: `{sql, skip, page_rows}`, then `fetch` until `done` |
| Exports | `POST /v1/exports:ticket`, then a form `POST /v1/exports` | CSV or NDJSON downloads for a browser session |
| Change streams | `GET /v1/databases/{database}/changes` | Server-Sent Events (`Accept: text/event-stream`), WebSocket, or NDJSON. Needs `SELECT` and `WATCH` on the tables; a named table without them fails with 1142. See [Change feed](../concepts/change-feed.md) |
| Key-value | `/v1/databases/{database}/namespaces/{namespace}:get`, `:put`, `:delete`, `:scan`, `:exec`, `:geoSearch` | [Key-value](../data-models/key-value.md) |
| Search, vectors | `/v1/databases/{database}/search/{index}:query`, `/v1/databases/{database}/vectorIndexes/...` | [Search](../data-models/search.md), [Vectors](../data-models/vectors.md) |
| Functions and logic | `POST /v1/databases/{database}/functions/{name}:call`, `/v1/databases/{database}/logic/...` | [Logic](../data-models/logic.md) |
| Queues, streams, sinks | `GET /v1/databases/{database}/mq`, `GET /v1/databases/{database}/streams/{stream}/groups`, `/v1/databases/{database}/sinks/...` | [Queues](../data-models/queues.md), [Event streams](../data-models/streams.md) |
| Analytics | `/v1/databases/{database}/models/...` | [Analytics](../data-models/analytics.md) |
| Accounts | `GET /v1/users`, `POST /v1/users:setPassword`, `/v1/api-keys` | [Accounts and logins](../security/accounts-and-logins.md) |
| Automatic indexing and approvals | `/v1/indexing/...`, `/v1/recommendations...` | [Automatic indexing](../performance/automatic-indexing.md), [Approvals](../concepts/approvals.md) |

### The OpenAPI document

`GET /v1/openapi.json` returns the OpenAPI 3 document of every route, without authentication. Generate a client from it, or browse it with any OpenAPI viewer:

```sh
curl -s http://db.example.com:8080/v1/openapi.json | jq '.paths | keys'
```

## gRPC

The same services are served over gRPC on the same ports, under the package `sprig.v1`: `Sql`, `Transactions`, `Functions`, `Logic`, `KeyValue`, `Vectors`, `Analytics`, `Search`, `Changes`, `Sinks` and `Connections`, plus the standard health service `grpc.health.v1.Health`. `Sql.Query` streams `QueryResponse` messages (header, row batches, end), exactly as the NDJSON lines above.

- Authenticate with the `authorization` metadata, as with REST: `Basic ...` or `Bearer ...`. `x-sprig-tenant` works as metadata too.
- Server reflection isn't enabled. Build clients from SprigDB's `.proto` files.
- Errors come as a gRPC status whose message is the MySQL error text, with `grpc-status-details-bin` holding a `google.rpc.Status` whose `ErrorInfo` has the `ER_SPRIG_*` name as `reason` (or `MYSQL_ERROR`), and `mysql_errno`, `sqlstate` and `sprig_code` as metadata. Retryable errors also carry a `RetryInfo`.

With `grpcurl` and the proto files:

```sh
grpcurl -import-path proto -proto sprig/v1/sql.proto \
  -H "authorization: Basic $(printf 'acme.app:%s' "$DB_PASSWORD" | base64)" \
  -d '{"database": "shop", "sql": "SELECT COUNT(*) FROM orders"}' \
  db.example.com:8443 sprig.v1.Sql/Query
```

The routes Studio added (`/v1/auth/...`, `/v1/api-keys`, `/v1/users`, cursors, exports, library, indexing) are REST only.

## Errors

A REST error is JSON with the HTTP status from the [error reference](../reference/errors.md):

```json
{"code": 1146, "name": "MYSQL_ERROR", "message": "Table 'shop.order' doesn't exist", "sqlstate": "42S02", "retryable": false}
```

| Field | Meaning |
|---|---|
| `code` | The MySQL error number; 39000 to 39999 for SprigDB's own |
| `name` | `ER_SPRIG_*`, or `MYSQL_ERROR` for a MariaDB or standard MySQL error |
| `message` | The error text a MySQL client would see |
| `sqlstate` | The SQLSTATE |
| `trace_id` | The request's trace id, when tracing knows one |
| `retryable` | Safe to retry as is |

Responses with 429 and 503 carry `Retry-After`. An error after a streamed answer has started ends the NDJSON stream with one last line, `{"error": {...}}`, in the same shape.

| Status | Code | When |
|---|---|---|
| 400 | 39785 | A `sql:execute` result over `api.rest.max_buffered_bytes`; use `sql:query` |
| 400 | 39794 | An unknown `language`, or a graph or MongoDB statement with a transaction handle or `multi_statements` |
| 401 | 1045 and others | See [Authenticate](#authenticate) |
| 403 | 39783 | Another principal's transaction handle |
| 409 | 39781, 39782 | An unknown or expired transaction handle |
| 413 | 39787 | A request over `api.max_request_bytes` |
| 415 | 39791 | gRPC over HTTP/1.1 |
| 429 | 39780, 39790 | Too many change streams for the tenant, or a change stream that fell too far behind |
| 503 | 39788 | A transaction handle's gateway can't be reached; retry |

## Related

- [REST API reference](../reference/api.md)
- [Connect to SprigDB](overview.md)
- [API keys and tokens](../security/api-keys-and-tokens.md)
- [Error codes](../reference/errors.md)
- [Change feed](../concepts/change-feed.md)
