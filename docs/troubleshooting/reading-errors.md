---
title: Reading SprigDB errors
audience: [user, dba, operator]
sidebar:
  order: 6
---

# Reading SprigDB errors

Every error SprigDB raises is one error with one number, rendered in each protocol's own form. This page shows how to read it in a MySQL or MariaDB client, a MongoDB driver, a REST call, a gRPC call and the other front ends, how to tell whether to retry, and why some drivers show the number as negative.

## The parts of an error

Every SprigDB error has:

| Part | Example | Notes |
|---|---|---|
| Number | `39420` | SprigDB's own errors use 39000 to 39999 |
| Name | `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` | Always starts `ER_SPRIG_` |
| MongoDB code name | `SprigStatementNotDistributable` | The name in CamelCase, starting `Sprig` |
| SQLSTATE | `HY000` | See below |
| Retryable | no | Whether retrying as it is can succeed |
| HTTP status, gRPC status | `400`, `FailedPrecondition` | Fixed per code |
| Message | `Statement not distributable (window_function): ...` | Says what, and usually which setting or object |

The [error reference](../reference/errors.md) lists every code with all of these, what it means and what to do.

Three kinds of errors reach clients:

- **SprigDB's own** (39000 to 39999).
- **MariaDB's**, passed through with the same number, SQLSTATE and message. SprigDB only translates physical schema names back to yours, so you see `Table 'shop.orders' doesn't exist`, never the shard's schema name.
- **Standard MySQL errors SprigDB raises itself**, with their usual numbers: 1045 (access denied), 1040 (too many connections), 3159 (TLS required), 1205 (lock wait timeout) and 1213 (deadlock) among them.

### SQLSTATE classes

| SQLSTATE | Meaning for SprigDB's codes |
|---|---|
| `HY000` | General error; most codes |
| `08S01` | The connection or backend was lost or is closing: reconnect |
| `08004` | The server refused the connection (`ER_SPRIG_TENANT_CONNECTION_LIMIT`) |
| `40001` | The transaction was rolled back and can be retried as a whole |
| `42000` | A reserved name (`ER_SPRIG_RESERVED_NAME`) |

## MySQL and MariaDB clients

On ports 3306 and 3307 an error is a standard ERR packet: the number, the SQLSTATE and the message.

```text
mysql> SELECT ROW_NUMBER() OVER (ORDER BY created_at) FROM orders;
ERROR 39420 (HY000): Statement not distributable (window_function): a window function across shards
```

In code, read the number and SQLSTATE from the driver's exception:

```python
import pymysql

try:
    cur.execute("SELECT ROW_NUMBER() OVER (ORDER BY created_at) FROM orders")
except pymysql.err.MySQLError as e:
    errno, message = e.args[0], e.args[1]
    if errno == 39420:
        ...  # rewrite the query: filter by the distribution key
```

```java
try {
    stmt.execute(sql);
} catch (SQLException e) {
    int errno = e.getErrorCode();        // 39420
    String state = e.getSQLState();      // "HY000"
}
```

The message is at most 512 bytes; longer messages are cut. The two personas send the same numbers, except where MariaDB itself differs: 3159 has SQLSTATE `08004` on 3307 (`HY000` on 3306), and MySQL's 3948 (local files disabled) is MariaDB's 4166 on 3307.

## MongoDB drivers

On port 27017 an error has a numeric `code`, a `codeName` and, for some errors, labels:

```json
{ "ok": 0, "code": 39420, "codeName": "SprigStatementNotDistributable",
  "errmsg": "Statement not distributable (aggregate): WITH ROLLUP across shards" }
```

SprigDB's codes keep their number and use the `Sprig...` code name, except where MongoDB has its own code with the same meaning, so drivers react the way they already do:

| SprigDB error | MongoDB `code`, `codeName` | Labels |
|---|---|---|
| `ER_SPRIG_GATEWAY_DRAINING` (39000) | 91, `ShutdownInProgress` | `RetryableWriteError` |
| `ER_SPRIG_DC_FENCED` (39501), `ER_SPRIG_RECOVERY_FENCED` (39530) | 10107, `NotWritablePrimary` | `RetryableWriteError` |
| `ER_SPRIG_LOGIC_VALIDATION_FAILED` | 121, `DocumentValidationFailure` | |

MariaDB errors map like this:

| MariaDB error | MongoDB `code`, `codeName` |
|---|---|
| 1062, duplicate key | 11000, `DuplicateKey` |
| 1205, 1213, lock wait timeout or deadlock | 112, `WriteConflict` |
| Any other | 8, `UnknownError`, with the MariaDB message |

Inside a transaction, the MongoDB front end adds `TransientTransactionError` to errors after which the whole transaction can be retried. Drivers' `withTransaction` helpers retry on that label.

```js
try {
  await db.collection("events").insertOne(doc);
} catch (e) {
  console.log(e.code, e.codeName, e.errorLabels);   // 39003 'SprigMemoryLimit' []
}
```

## REST

Over HTTP (8080, 8443) an error is an HTTP status with SprigDB's error body:

```http
HTTP/1.1 400 Bad Request
Content-Type: application/json

{
  "code": 39420,
  "name": "ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE",
  "message": "Statement not distributable (window_function): a window function across shards",
  "sqlstate": "HY000",
  "retryable": false
}
```

| Field | Meaning |
|---|---|
| `code` | The MySQL error number: 39000 to 39999 for SprigDB's own, MariaDB's number otherwise |
| `name` | `ER_SPRIG_...`, or `MYSQL_ERROR` for a MariaDB or standard error |
| `message` | The same text a MySQL client gets |
| `sqlstate` | The SQLSTATE |
| `trace_id` | The request's trace id, when the gateway knows one; left out otherwise |
| `retryable` | Whether retrying as it is can succeed |

The HTTP status of each SprigDB code is in the [error reference](../reference/errors.md). MariaDB errors map like this:

| MariaDB error | HTTP | gRPC |
|---|---|---|
| 1205, 1213, or any SQLSTATE `40001` | 409 | `ABORTED` |
| 1062 | 409 | `ALREADY_EXISTS` |
| 1049, 1146 (unknown database or table) | 404 | `NOT_FOUND` |
| 1044, 1142, 1143, 1227 (no privilege) | 403 | `PERMISSION_DENIED` |
| 1045 (access denied) | 401 | `UNAUTHENTICATED` |
| 1064 (syntax) | 400 | `INVALID_ARGUMENT` |
| SQLSTATE class `08` | 503 | `UNAVAILABLE` |
| Any other | 500 | `UNKNOWN` |

Some refusals carry headers: 401 adds `WWW-Authenticate`, and 429 (`ER_SPRIG_LOGIN_RATE_LIMITED`) and 503 while a data centre returns from an outage add `Retry-After`.

## gRPC

Over gRPC an error is a status with the code from the reference table and the MySQL message. The `grpc-status-details-bin` trailer holds a `google.rpc.Status` with:

- an `ErrorInfo` whose `domain` is `sprigdb.io`, whose `reason` is the `ER_SPRIG_...` name (or `MYSQL_ERROR`), and whose `metadata` has `mysql_errno`, `sqlstate` and, for SprigDB's codes, `sprig_code`;
- a `RetryInfo` asking for a 100 ms delay, only when the error is retryable.

```go
st := status.Convert(err)
for _, d := range st.Details() {
    if info, ok := d.(*errdetails.ErrorInfo); ok {
        fmt.Println(info.Reason, info.Metadata["mysql_errno"], info.Metadata["sqlstate"])
    }
    if _, ok := d.(*errdetails.RetryInfo); ok {
        // safe to retry
    }
}
```

## Other front ends

| Protocol | How errors look |
|---|---|
| Redis (6379) | `-ERR SprigDB backend: <number>: <message>` for errors from the data path; privilege errors as `-NOPERM ...`; some contention as `-TRYAGAIN ...`; Redis's own errors (`WRONGTYPE`, `ERR value is not an integer or out of range`) as Redis sends them |
| Neo4j Bolt (7687) | A Neo4j status code: Neo4j's own where one fits (`Neo.ClientError.Statement.SyntaxError`, `Neo.TransientError.Transaction.DeadlockDetected`), else `Neo.ClientError.Statement.Sprig<CamelCase>`. Draining gateways and moving shards are `Neo.TransientError.General.DatabaseUnavailable`, which drivers retry |
| Kafka (9092), AMQP (5672, 5671) | The protocol's own error codes and reply codes; see [Kafka](../connect/kafka.md) and [AMQP](../connect/amqp.md) |

## The trace id

A trace follows one request through the gateway and its round trips to MariaDB. To find the trace of a failed request:

- Over REST, use `trace_id` from the error body when it is present.
- Every gateway keeps failed requests' traces (they are always kept, whatever the sampling) and serves recent ones at `/debug/traces` on its telemetry port (9180). Filter by an attribute of the root span, such as the protocol:

  ```sh
  curl -s 'http://gw1.example.internal:9180/debug/traces?attr=protocol=mysql'
  ```

- A MySQL client on port 3306 can send its own W3C `traceparent` as a query attribute; the gateway joins that trace and passes it on to MariaDB as a comment, so your application's trace and SprigDB's line up.

Error messages over MySQL and MongoDB don't contain a trace id. Note the time, the tenant and the gateway, and look the request up in the gateway's log and traces.

`ER_SPRIG_INTERNAL` (39999) means an internal check failed: a bug. The gateway logs it at error level with the internal detail, which the client never sees. Report it with the time, the gateway and the statement.

## Retryable errors

Retry only when the error says it is safe. Each protocol says it in its own way:

| Protocol | Signal |
|---|---|
| MySQL, MariaDB | SQLSTATE `40001`, or number 1205 or 1213: retry the **whole transaction**. SQLSTATE `08S01` or `08004`: reconnect, then retry. Otherwise check the code's Retry column in the reference |
| MongoDB | The `RetryableWriteError` or `TransientTransactionError` label |
| REST | `"retryable": true` in the body; `Retry-After` when present |
| gRPC | A `RetryInfo` detail |
| Bolt | A `Neo.TransientError...` code |

Common retryable errors:

| Error | Retry how |
|---|---|
| 1213, deadlock; `ER_SPRIG_SHARD_MAP_CHANGED` (39424); `ER_SPRIG_DTX_ROLLED_BACK` (39443) | The whole transaction, from `BEGIN` |
| 1205, lock wait timeout | The statement or the transaction |
| `ER_SPRIG_GATEWAY_DRAINING` (39000), `ER_SPRIG_LISTENER_RETIRED` (39004) | Reconnect, then retry |
| `ER_SPRIG_ADMISSION_TIMEOUT` (39002), `ER_SPRIG_MEMORY_LIMIT` (39003), `ER_SPRIG_BACKEND_POOL_TIMEOUT` (39031) | With backoff; these mean the system is busy |
| `ER_SPRIG_BACKEND_UNAVAILABLE` (39030), `ER_SPRIG_CLUSTER_UNAVAILABLE` (39100), `ER_SPRIG_CATALOG_UNAVAILABLE` (39070) | With backoff, for up to a minute while a failover finishes |
| `ER_SPRIG_DC_FENCED` (39501), `ER_SPRIG_RECOVERY_FENCED` (39530) | With backoff, once the tenant is re-routed |
| `ER_SPRIG_ADMISSION_BACKOFF` (39531) | After the delay in the message |
| `ER_SPRIG_LOGIN_RATE_LIMITED` (39300) | After the block ends; fix the failing logins first |

Never retry these blindly; their outcome is unknown, and a second attempt may apply a change twice:

| Error | What to do |
|---|---|
| `ER_SPRIG_OUTCOME_UNKNOWN` (39032) | The connection was lost during a commit or an autocommit write. Read back whether the change happened, then repeat it only if it didn't |
| `ER_SPRIG_COMMIT_OUTCOME_UNKNOWN` (39441) | The same for a transaction across clusters; the connection is closed |
| `ER_SPRIG_PARTIAL_COMMIT` (39442) | Some clusters committed; repair the rest from the message |
| `ER_SPRIG_BACKEND_LOST` (39034) | Part of a result was sent; discard it and run the read again |

A retry loop for transactions, in Python:

```python
import time, random, pymysql

RETRY_ERRNOS = {1205, 1213, 39424, 39443, 39000, 39030, 39100, 39501, 39530}

def run_txn(conn, work, attempts=5):
    for i in range(attempts):
        try:
            with conn.cursor() as cur:
                conn.begin()
                work(cur)
            conn.commit()
            return
        except pymysql.err.MySQLError as e:
            conn.rollback()
            if e.args[0] not in RETRY_ERRNOS or i == attempts - 1:
                raise
            time.sleep(min(2 ** i * 0.05, 2) * random.random())
```

## Negative error numbers

Some drivers store the MySQL error number in a signed 16-bit integer. SprigDB's numbers are above 32767, so these drivers show them as negative: 39420 appears as -26116.

To get the real number, add 65536:

| Shown | Real | Name |
|---|---|---|
| -26536 | 39000 | `ER_SPRIG_GATEWAY_DRAINING` |
| -26236 | 39300 | `ER_SPRIG_LOGIN_RATE_LIMITED` |
| -26116 | 39420 | `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` |
| -26035 | 39501 | `ER_SPRIG_DC_FENCED` |
| -25537 | 39999 | `ER_SPRIG_INTERNAL` |

```python
errno = e.args[0]
if errno < 0:
    errno += 65536
```

The SQLSTATE and the message are unaffected, so matching on them works with any driver.

## Related

- [Error reference](../reference/errors.md)
- [Troubleshooting SprigDB](overview.md)
- [Connect overview](../connect/overview.md)
- [REST and gRPC](../connect/rest-and-grpc.md)
- [MongoDB](../connect/mongodb.md)
