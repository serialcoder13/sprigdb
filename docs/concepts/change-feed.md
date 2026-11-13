---
title: Change feed
audience: [user, dba, operator]
sidebar:
  order: 8
---

# Change feed

The change feed is SprigDB's ordered record of every committed change to a tenant's data, read from MariaDB's binary log. Read this page to understand what the feed is, which SprigDB features are built on it, and how your applications can subscribe to changes.

## What it is

Every MariaDB cluster writes its committed transactions to its binary log (binlog). SprigDB reads those binlogs as a replica would, and turns each row change into a **change event** that names what you see, not what MariaDB stores:

- the tenant, the logical database and the table, never physical schema names such as `t42_d1_g3_s0`;
- the operation: `insert`, `update`, `delete`, or a schema change (`ddl`, `truncate`);
- the row's key, and the row images: the row after an insert or update, the row before a delete.

SprigDB's internal tables and invisible internal columns (`_sprig_…`) never appear, and an update that changed nothing is dropped.

Each tenant has one feed. The feed reads only the tenant's own schemas, so one tenant's feed never contains another's changes.

### Order and transactions

Changes arrive in commit order, a whole transaction at a time. Each event has a stable id made of the cluster, the transaction's GTID and the row's position in it, so a consumer can recognise a change it has already seen.

### Resume tokens

Every event carries a **resume token**: an opaque hex string that names a point in the tenant's feed. A consumer that stores the last token it processed can reconnect and continue just after it, with nothing lost and nothing repeated. A token holds a position, never data.

Tokens keep working when SprigDB moves the tenant's data. When the mover cuts a shard over to another cluster, it records where the old cluster's history ends and the new one's begins, and the feed follows that hand-over: a consumer sees every change once, and never the mover's own copying.

### How far back it reaches

The feed can only read what the binlogs still hold. Each cluster keeps binlogs for `binlog_expire_logs_seconds` (a MariaDB setting); SprigDB warns when a cluster keeps less than `cluster.min_binlog_retention_hours` (24). A resume token older than the oldest binlog fails with 39800 `ER_SPRIG_FEED_HISTORY_LOST`, and the consumer must start again from now.

The feed can't start at a point in time: MariaDB has no index of binlog positions by time. Start from now, or after a token.

### What a cluster must have

The feed needs these MariaDB settings on every cluster, and the controller checks them when it probes (see [Clusters and data centres](clusters-and-data-centres.md)):

| MariaDB setting | Value |
|---|---|
| `log_bin` | `ON` |
| `binlog_format` | `ROW` |
| `binlog_row_image` | `FULL` |
| `gtid_strict_mode` | `ON` |
| `log_slave_updates` | `ON` on replicas |

Without them, the cluster's feed is unsupported, and features that need it fail with 39802 `ER_SPRIG_FEED_SOURCE_UNSUPPORTED`. `binlog_annotate_row_events` and `binlog_row_metadata = FULL` are recommended too: without them the feed still works, with coarser timestamps.

## What SprigDB builds on it

| Feature | How it uses the feed |
|---|---|
| [Search](../data-models/search.md) | Search indexes are kept up to date from the feed, on workers. They can always be rebuilt from MariaDB |
| [Logic](../data-models/logic.md) | Asynchronous triggers run on workers for each matching change |
| [Streams](../data-models/streams.md) | A table's change topic (`stream.change_topic = true`) publishes its changes as a Kafka-compatible stream |
| [Queues](../data-models/queues.md) | `queue.change_exchange` forwards a table's changes to an AMQP exchange |
| [Sinks](../data-models/sinks.md) | A sink reading `CHANGES OF` a table delivers its change topic to an external system |
| [Inheritance](../data-models/inheritance.md) | Fans changes of inherited definitions out to member tenants |
| [Graphs](../data-models/graphs.md) | Keeps graph mirrors in step with their tables |
| [Redis](../connect/redis.md) | Keyspace notifications (`__keyspace@<db>__:<key>` and `__keyevent@<db>__:<event>` channels, as set by `redis.notify_keyspace_events`) for keys changed by any client, SQL included |
| [Shard moves](shards-and-table-groups.md) | The mover reads the same binlogs to catch a copy up before cut-over, and writes the hand-over records the feed follows |

## Subscribing to changes

Applications can watch the feed in three ways. Each needs `SELECT` and `WATCH` on what it watches, and shows only the tables the caller may read and watch, with row-level security applied to each row image (see [Row-level security](../security/row-level-security.md)). Permissions are checked again with every batch, so a revoked grant applies from the next one.

```sql
GRANT SELECT, WATCH ON shop.* TO 'feed_reader'@'%';
```

`ALL PRIVILEGES` and MongoDB's read roles include `WATCH`, and grants that held `SELECT` before `WATCH` existed got it when the tenant's catalog was upgraded. See [Grants and roles](../security/grants-and-roles.md#change-streams-need-watch).

### MongoDB change streams

A MongoDB driver's `watch()` works on collections and databases:

```js
const stream = db.collection('events').watch([{ $match: { operationType: 'insert' } }]);
for await (const change of stream) {
  console.log(change.fullDocument);
  saveToken(change._id);   // resume later with { resumeAfter: token }
}
```

Supported:

- `insert`, `update` (with `updateDescription`), `replace` and `delete` events, and `drop`, `rename` and `invalidate` when a watched collection is dropped or renamed;
- `fullDocument: 'updateLookup'`, `'whenAvailable'` and `'required'`, and `fullDocumentBeforeChange` when the collection has `changeStreamPreAndPostImages` on;
- `resumeAfter` and `startAfter` with the event's `_id` (the feed's resume token);
- pipeline stages that work on one event at a time: `$match`, `$project`, `$addFields`, `$set`, `$unset`, `$replaceRoot`, `$replaceWith`;
- `showExpandedEvents` for `create`, `createIndexes`, `dropIndexes` and `modify`.

`startAtOperationTime` isn't supported. `watch()` on a collection without `WATCH` fails with 13 `Unauthorized`; a database stream leaves such collections out. Documents a TTL index deletes appear as `delete` events. See [MongoDB](../connect/mongodb.md).

### Server-Sent Events, WebSocket and NDJSON

Over HTTP, watch a database's tables at `GET /v1/databases/{database}/changes`. The `Accept` header picks the framing:

| Request | Framing |
|---|---|
| `Accept: text/event-stream` | Server-Sent Events: `id: <token>`, `event: <op>`, `data: <JSON>` |
| `Upgrade: websocket` (HTTP/1.1) | One JSON text frame per event |
| Anything else | NDJSON: one JSON object per line |

| Query parameter | Meaning |
|---|---|
| `objects` | Tables to watch, comma-separated or repeated. Default: every table of the database you may read and watch. A named table without `SELECT` or `WATCH` fails with 1142 |
| `start_after` | Resume just after this token. SSE clients can send `Last-Event-ID` instead |

```sh
curl -N https://db.example.com:8443/v1/databases/shop/changes?objects=orders \
  -H 'Accept: text/event-stream' \
  -H 'Authorization: Bearer <api-key>'
```

```text
id: 01000000000000002a...
event: insert
data: {"resume_token":"01000000000000002a...","object":"orders","model":"table","op":"insert","key":"{\"id\":1}","after":"{\"id\":1,\"status\":\"paid\",\"total\":42.50}","commit_hlc":"..."}
```

| Field | Meaning |
|---|---|
| `resume_token` | Resume after this event |
| `object` | The table |
| `op` | `insert`, `update`, `delete`, `ddl`, `truncate` or `heartbeat` |
| `key` | The row's key, as a JSON string |
| `after` | The row after the change, as a JSON string |
| `update` | For an update, the changed columns, as a JSON string |
| `ddl` | For `ddl`, the statement, with logical names |
| `commit_hlc` | The commit's timestamp in SprigDB's hybrid logical clock |

When nothing changes for `api.stream.heartbeat_seconds` (15), the stream sends a `heartbeat` event carrying the latest token, so a quiet stream still moves its resume point on. Changes to rows you may not see also move the token on, without showing you the row.

### gRPC

The same stream is the `Changes.Watch` server-streaming call of the `sprig.v1` API, one `ChangeEvent` message per event, on the gateway's HTTP ports. See [REST and gRPC](../connect/rest-and-grpc.md).

### Limits

| Setting | Default | Meaning |
|---|---|---|
| `api.stream.max_per_tenant` | 1000 | Open change streams per tenant per gateway; past it, 39780 `ER_SPRIG_RATE_LIMITED` |
| `api.stream.max_buffered_events` | 10000 | A client further behind gets 39790 `ER_SPRIG_STREAM_SLOW_CONSUMER`, and the stream ends; resume from the last token |
| `api.stream.heartbeat_seconds` | 15 | Heartbeat interval on a quiet stream |

## Errors

| Code | Name | When |
|---|---|---|
| 1142 | `ER_TABLEACCESS_DENIED_ERROR` | A named table you can't read (`SELECT command denied`) or watch (`WATCH command denied`) |
| 13 | `Unauthorized` (MongoDB) | `watch()` on a collection without `WATCH` |
| 39800 | `ER_SPRIG_FEED_HISTORY_LOST` | The start point is older than the history the binlogs keep |
| 39801 | `ER_SPRIG_FEED_BAD_RESUME_TOKEN` | The token is malformed or belongs to another tenant |
| 39802 | `ER_SPRIG_FEED_SOURCE_UNSUPPORTED` | A cluster's binlog settings don't meet the feed's needs, or a start by time was asked for |
| 39780 | `ER_SPRIG_RATE_LIMITED` | Too many open streams for the tenant on this gateway |
| 39790 | `ER_SPRIG_STREAM_SLOW_CONSUMER` | The client fell too far behind; resume from the last token |

## Not supported yet

- Starting a stream at a point in time (`start_at_hlc`, MongoDB's `startAtOperationTime`).
- A SQL statement for subscribing to changes. Use one of the transports above, a change topic or a sink.

## Related

- [Streams](../data-models/streams.md)
- [Sinks](../data-models/sinks.md)
- [Search](../data-models/search.md)
- [REST and gRPC](../connect/rest-and-grpc.md)
- [MongoDB](../connect/mongodb.md)
