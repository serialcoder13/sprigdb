---
title: Streams, queues and sinks in SQL
audience: [user, dba]
sidebar:
  order: 8
---

# Streams, queues and sinks in SQL

SprigDB keeps event streams (Kafka's model), queues (RabbitMQ's model) and sinks (delivery to systems outside SprigDB) as data in MariaDB, and lets you manage and use all three from SQL. Use streams for events many readers replay, queues for work one consumer takes, and sinks to push either, or a table's changes, to a webhook, Kafka, S3 or another database. This page is the SQL reference; for the models see [Streams](../data-models/streams.md), [Queues](../data-models/queues.md) and [Sinks](../data-models/sinks.md).

## Streams

A stream is an append-only log of records, split into partitions. Each record has a `key`, a `value`, `headers` and a timestamp, and gets an offset in its partition in commit order. Kafka clients read and write the same streams on port 9092; see [Kafka](../connect/kafka.md).

### CREATE STREAM

```sql
CREATE STREAM [IF NOT EXISTS] [db_name.]stream_name
    [KEY (column [, column] ...)]
    [ATTACHED TO GROUP OF [db_name.]table_name]
    [PARTITIONS n]
    [RETENTION 'duration']
    [CLEANUP {delete | compact}]
```

| Clause | Default | Meaning |
|---|---|---|
| `KEY (columns)` | none | The fields that make up the event key. Documents the key, and is checked against the attached group's distribution key |
| `ATTACHED TO GROUP OF t` | the database's default group | Put the stream's outbox on `t`'s shard, so a transaction that writes `t` and publishes commits on one shard. The group must have one shard, else 39945 |
| `PARTITIONS n` | `stream.partitions_default` (12) | Partitions. They can only grow |
| `RETENTION 'duration'` | `stream.retention_hours` (168) | How long records stay: a number and a unit, `h`/`hours`, `d`/`days`, `w`/`weeks`, `m`/`minutes` or `ms`; a bare number is hours. Rounded up to whole hours |
| `CLEANUP` | `delete` | `delete` removes records past retention; `compact` keeps the newest record per key (records need a key, else 39955) |

Names with dots or dashes need back-quotes: `` `orders.changes` ``. Size retention per partition is `stream.retention_bytes`; compaction waits `stream.compaction_lag_minutes`, and tombstones stay `stream.tombstone_hours`.

**Privileges:** `CREATE` on the database.

```sql test
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, total DECIMAL(10,2));
CREATE STREAM order_events KEY (customer_id) PARTITIONS 3 RETENTION '7 days';
SHOW STREAMS;
```

```text output
Stream	Kind	Partitions	Cleanup	Source	Key
order_events	events	3	delete	NULL	customer_id
```

### ALTER STREAM

```sql
ALTER STREAM [db_name.]stream_name
    {PARTITIONS n | RETENTION 'duration' | CLEANUP {delete | compact}
     | [SPRIG] SET (setting = value [, ...]) | [SPRIG] RESET (setting [, ...])} ...
```

Several changes may follow one another, optionally separated by commas. `SET` and `RESET` change the stream's object-scope settings, such as `stream.retention_bytes` or `stream.max_message_bytes`. Fewer partitions than now fails with 39943 `ER_SPRIG_STREAM_PARTITIONS_DECREASE`.

**Privileges:** `ALTER` on the stream.

```sql test
ALTER STREAM order_events PARTITIONS 6, SET (stream.retention_bytes = 1073741824);
```

### DROP STREAM

```sql
DROP STREAM [IF EXISTS] [db_name.]stream_name
```

Drops the stream, its log and its consumer groups' offsets. **Privileges:** `DROP` on the stream.

### SHOW STREAMS

```sql
SHOW STREAMS [{FROM | IN} db_name]
```

| Column | Meaning |
|---|---|
| `Stream` | The name |
| `Kind` | `events` (created with `CREATE STREAM`) or `changes` (a table's change topic) |
| `Partitions` | Partition count |
| `Cleanup` | `delete` or `compact` |
| `Source` | For a change topic: the table |
| `Key` | The `KEY` columns, comma-separated |

Streams you hold no `SELECT` on are left out.

### Publishing with INSERT

```sql
INSERT [IGNORE] [INTO] [db_name.]stream_name [(column [, column] ...)]
    {VALUES (...) [, (...)] ... | SELECT ...}

column: key | value | headers | partition_no | ts
```

A publish joins your transaction: the records exist only if it commits, and they get offsets in commit order. Without `partition_no`, the key picks the partition with Kafka's partitioner. `headers` is a JSON object.

**Privileges:** `INSERT` on the stream.

```sql test
INSERT INTO order_events (`key`, `value`, headers)
VALUES ('c42', '{"order_id": 1001, "total": 25.00}', JSON_OBJECT('source', 'web'));
```

A record above `stream.max_message_bytes` (1 MiB) or a key above 4096 bytes fails with 39944. `UPDATE`, `DELETE`, `REPLACE`, `TRUNCATE` and `INSERT … ON DUPLICATE KEY UPDATE` on a stream fail with 39941 `ER_SPRIG_STREAM_APPEND_ONLY`.

### Reading with SELECT

A stream reads like a table with the columns `partition_no`, `offset_no`, `ts`, `key`, `value` and `headers`. Ranges of partition and offset use the log's primary key.

```sql test
SELECT partition_no, offset_no, ts, `key`, `value`
FROM order_events
WHERE partition_no = 0 AND offset_no >= 0
ORDER BY offset_no
LIMIT 100;
```

Offsets are assigned just after commit, so a record you published a moment ago may take a few milliseconds to appear. **Privileges:** `SELECT` on the stream.

### Change topics

```sql
ALTER TABLE table_name SPRIG SET (stream.change_topic = true)
```

Keeps a stream named `<table>.changes` (kind `changes`) with one record per committed row change. Setting it to `false` drops it. Reading a change topic needs `SELECT` on the table; a change topic can't be published to (39941). A table whose rows a row-level security policy filters for you can't be read through its change topic (39366).

```sql test
ALTER TABLE orders SPRIG SET (stream.change_topic = true);
SELECT `key`, `value` FROM `orders.changes` LIMIT 10;
```

### Consumer groups and offsets

Consumer groups belong to the Kafka protocol: Kafka clients join groups, and SprigDB keeps each group's committed offset per partition. From SQL you commit an offset in the same transaction as your own writes, so a record's effects and its offset commit together (exactly-once consume and write):

```sql
CALL sprig.commit_offset(group_name, stream_name, partition_no, next_offset [, metadata])
```

| Argument | Meaning |
|---|---|
| `group_name` | The consumer group |
| `stream_name` | `[db_name.]stream_name` |
| `partition_no` | The partition |
| `next_offset` | The next offset to read (the last processed offset + 1) |
| `metadata` | Optional text kept with the offset |

The arguments may be any expressions. Outside a transaction, the offset is written at once. Inside one, it commits or rolls back with it. When the tenant's catalog lives on another cluster than the transaction's connection, the call fails with 39958 `ER_SPRIG_STREAM_OFFSET_TXN_UNSUPPORTED`.

**Privileges:** `SELECT` on the stream.

```sql
START TRANSACTION;
INSERT INTO order_totals (customer_id, total) VALUES (42, 25.00);
CALL sprig.commit_offset('billing', 'order_events', 0, 1201);
COMMIT;
```

Code that reads streams in SprigDB itself uses [triggers on streams](logic.md#create-trigger--on-stream-and-on-queue), which do this for you.

## Queues and exchanges

Exchanges, queues and bindings follow AMQP 0-9-1. Declare them over AMQP (or with Studio). In SQL you publish, take, acknowledge and inspect messages with `CALL sprig.mq_*`. Each database is an AMQP virtual host; `/` is the database named by `amqp.default_vhost_database` (`mq` by default). See [AMQP](../connect/amqp.md).

`CREATE EXCHANGE`, `CREATE QUEUE` and `BIND` exist in SQL only as [`FOR EACH TENANT`](#for-each-tenant) definitions.

| Call | Does | Returns |
|---|---|---|
| `sprig.mq_publish(exchange, routing_key, body, properties [, mandatory [, vhost]])` | Publishes one message | `routed`: queues reached |
| `sprig.mq_get(queue [, max [, lease_ms [, vhost [, lease_tag]]]])` | Takes up to `max` messages (default 1), each held for `lease_ms` | `delivery_token`, `body`, `properties`, `routing_key`, `exchange`, `redelivered`, `delivery_count` |
| `sprig.mq_ack(token)` | Acknowledges a delivery: the message is deleted | OK |
| `sprig.mq_nack(token, requeue)` | Refuses a delivery: back to the queue (`requeue` true) or dead-lettered | OK |
| `sprig.mq_info([queue [, vhost]])` | Counts for a queue, or one row per queue you may read | `ready`, `unacked`, `consumers` (with `queue` when listing) |
| `sprig.mq_browse(queue [, max [, vhost]])` | Reads messages without consuming them | `seq`, `body`, `properties`, `routing_key`, `exchange`, `delivery_count` |
| `sprig.mq_purge(queue [, vhost])` | Removes ready messages | `purged` |
| `sprig.mq_republish(queue, exchange [, routing_key [, max [, vhost]]])` | Moves up to `max` messages, such as a dead-letter queue's, to an exchange; a `NULL` routing key keeps each message's own | `moved` |
| `sprig.mq_release(queue, lease_tag [, vhost])` | Returns every message held under `lease_tag` to the queue at once | OK |

`properties` is a JSON object of AMQP properties: `content_type`, `content_encoding`, `headers`, `delivery_mode`, `priority`, `correlation_id`, `reply_to`, `expiration`, `message_id`, `timestamp`, `type`, `user_id`, `app_id`. Arguments may be any expressions, evaluated with your rights.

**Transactions.** `mq_publish` inside a transaction writes the message to an outbox in that transaction: it exists only if you commit, and is delivered after commit (in commit order with `queue.forwarder = feed`). `mq_ack` inside a transaction deletes the message in it: if the transaction rolls back, the message is delivered again. An acknowledgement of a delivery that is no longer held rolls the transaction back.

**Privileges:** the AMQP permissions of the virtual host's exchanges and queues: write to publish, read to get, browse and inspect.

```sql
-- take a job, do the work, and acknowledge it in one transaction
START TRANSACTION;
CALL sprig.mq_get('billing', 1, 30000);
-- delivery_token  body                 properties  routing_key  exchange  redelivered  delivery_count
-- billing.AAAB…   {"order_id": 1001}   {}          billing               0            1
UPDATE orders SET total = 25.00 WHERE id = 1001;
CALL sprig.mq_ack('billing.AAAB…');
COMMIT;

CALL sprig.mq_publish('orders', 'order.paid.eu',
                      JSON_OBJECT('id', 1001, 'total', 25.00),
                      JSON_OBJECT('message_id', 'm-1001', 'content_type', 'application/json'));
CALL sprig.mq_info('billing');
```

### Publishing a table's changes to an exchange

```sql
ALTER TABLE table_name SPRIG SET (queue.change_exchange = 'exchange_name')
```

Every committed row change of the table is published to the exchange; an empty value stops it.

## Connections

A connection holds where a sink delivers and the credentials it uses. Credentials are sealed with the tenant's sink key and never shown or logged: `SHOW CREATE CONNECTION` and the logs replace them.

### CREATE CONNECTION

```sql
CREATE CONNECTION [IF NOT EXISTS] [db_name.]connection_name
    TYPE {webhook | kafka | s3 | mysql | mariadb | postgresql | postgres}
    [WITH (option = value [, ...])]
    [CREDENTIALS (credential = 'value' [, ...])]
```

| Type | Required options | Credentials |
|---|---|---|
| `webhook` | `url` | `token`, `username`, `password`, `header_value`, `client_id`, `client_secret`, `signing_secret`, `tls_cert`, `tls_key` |
| `kafka` | `bootstrap` (`host:port[,host:port…]`, port 9092 by default) | `username`, `password`, `tls_cert`, `tls_key`, `client_secret` |
| `s3` | `endpoint`, `bucket` | `access_key_id`, `secret_access_key`, `session_token` |
| `mysql` (`mariadb`), `postgresql` (`postgres`) | `host` (`port` 3306 or 5432 by default) | `username`, `password`, `tls_cert`, `tls_key` |

Some optional connection options:

| Option | Type | Values |
|---|---|---|
| `auth` | webhook | `none` (default), `mtls`, `bearer` (`token`), `basic` (`username`, `password`), `header` (`auth_header` names it; `header_value`), `oauth2_client_credentials` (`token_url`, `scope`, `audience`; `client_id`, `client_secret`) |
| `signature` | webhook | `sprig` (default), `standard` (Standard Webhooks) or `none`; signed with `signing_secret` |
| `sasl_mechanism` | kafka | SASL PLAIN or SCRAM-SHA-256/512 |
| `region`, `path_style` | s3 | Region; path-style addressing (default on for endpoints other than AWS) |
| `tls` | mysql, postgresql | TLS mode for the destination |

Every host a connection names must be on `sink.destination_allowlist` (39213) and satisfy the tenant's residency rules (39214). A credential the type doesn't take fails with 39216.

**Privileges:** `SPRIG_SINK_ADMIN`.

```sql
CREATE CONNECTION hooks TYPE webhook
  WITH (url = 'https://hooks.example.com/sprig', auth = 'bearer')
  CREDENTIALS (token = '<api-key>');

CREATE CONNECTION events_kafka TYPE kafka
  WITH (bootstrap = 'kafka-1.example.com:9093,kafka-2.example.com:9093', sasl_mechanism = 'SCRAM-SHA-512')
  CREDENTIALS (username = 'sprig', password = 's3cret-Passw0rd');
```

### ALTER CONNECTION

```sql
ALTER CONNECTION [db_name.]connection_name SET (option = value [, ...])
ALTER CONNECTION [db_name.]connection_name RESET (option [, ...])
ALTER CONNECTION [db_name.]connection_name SET CREDENTIALS (credential = 'value' [, ...])
```

New credentials replace the old ones; the previous ones stay usable for `sink.credential_previous_hours` (24) so running deliveries finish. **Privileges:** `SPRIG_SINK_ADMIN`.

### DROP CONNECTION

```sql
DROP CONNECTION [IF EXISTS] [db_name.]connection_name
```

A connection that sinks use fails with 39215 `ER_SPRIG_SINK_CONNECTION_IN_USE`. **Privileges:** `SPRIG_SINK_ADMIN`.

### GRANT and REVOKE USAGE ON CONNECTION

```sql
GRANT USAGE ON [PLATFORM] CONNECTION connection_name TO account
REVOKE USAGE ON [PLATFORM] CONNECTION connection_name FROM account
```

Naming a connection in a sink needs `USAGE` on it, its ownership or `SPRIG_TENANT_ADMIN` (39212). **Platform connections** are defined by operators for every tenant; a tenant must be granted one (39233) before its sinks can name it with `PLATFORM CONNECTION`.

### SHOW CONNECTIONS

```sql
SHOW CONNECTIONS [{FROM | IN} db_name]
SHOW CREATE CONNECTION [db_name.]connection_name
SHOW PLATFORM CONNECTIONS
```

| Statement | Columns |
|---|---|
| `SHOW CONNECTIONS` | `Database`, `Name`, `Type`, `Hosts`, `Credentials` (which are set, never the values), `Residency` |
| `SHOW CREATE CONNECTION` | `Connection`, `Create Connection` (credentials redacted) |
| `SHOW PLATFORM CONNECTIONS` | `Name`, `Type`, `Description`, `Tags`, `Options`: the platform connections granted to the tenant |

```sql test
SHOW CONNECTIONS;
SHOW PLATFORM CONNECTIONS;
```

### TEST CONNECTION

```sql
TEST [PLATFORM] CONNECTION [db_name.]connection_name [WRITE [TO 'target']]
TEST CREATE CONNECTION ...        -- the checks of an unsaved definition
```

Runs the connection's checks step by step, from the destination rules (allowlist and residency) to reaching and authenticating with the destination, and, with `WRITE`, a test write. It answers one row per step: `Step`, `Result`, `Detail`, `Elapsed ms`. `TEST CREATE CONNECTION` takes a whole `CREATE CONNECTION` statement and saves nothing.

## Sinks

A sink reads a stream, a queue or tables' changes and delivers each record to a destination, with retries, ordering per key and dead letters. Workers run sinks in tasks; see [Sinks](../data-models/sinks.md).

### CREATE SINK

```sql
CREATE SINK [IF NOT EXISTS] [db_name.]sink_name
    FROM source
    [WHERE condition]
    [COLUMNS (column [, ...]) | EXCEPT COLUMNS (column [, ...])]
    [TRANSFORM function_name]
    TO target
    [GUARANTEE {AT LEAST ONCE | EXACTLY ONCE}]
    [DEAD LETTER TO {STREAM d | QUEUE q | EXCHANGE e}]
    [START AT {LATEST | EARLIEST | SNAPSHOT | TIMESTAMP 'iso-8601'}]
    [WITH (option = value [, ...])]

source:
    STREAM [db_name.]stream [FAN IN [UNDER tenant]]
  | QUEUE [db_name.]queue [FAN IN [UNDER tenant]]
  | CHANGES OF [db_name.]table [, [db_name.]table] ...

target:
    WEBHOOK [PLATFORM] CONNECTION c [PATH '/path']
  | KAFKA [PLATFORM] CONNECTION c TOPIC 'topic' [KEY 'expression']
  | S3 [PLATFORM] CONNECTION c PREFIX 'template'
  | DATABASE [PLATFORM] CONNECTION c TABLE 'name'
  | SCRIPT LANGUAGE {javascript | lua | kirun} [USING CONNECTIONS (c [, ...])] AS $$ body $$
  | SCRIPT FUNCTION f [USING CONNECTIONS (c [, ...])]
```

| Clause | Default | Meaning |
|---|---|---|
| `FROM STREAM` | | Every record of the stream, in partition order |
| `FROM QUEUE` | | Messages taken from the queue, each held for `sink.queue_lease_ms` until delivered |
| `FROM CHANGES OF` | | Committed row changes of one or more tables |
| `FAN IN [UNDER t]` | | The source is a [`FOR EACH TENANT`](#for-each-tenant) definition; read every member's instance (below `t`). Needs `SPRIG_FAN_IN` |
| `WHERE` | all | A condition on the record; rows a row-level security policy hides from the sink's owner are never delivered |
| `COLUMNS`, `EXCEPT COLUMNS` | all | For changes: which columns to send |
| `TRANSFORM f` | none | A logic function applied to each record within `sink.transform_budget_ms`. Under `EXACTLY ONCE` it must be deterministic and not write or call out (39219) |
| `TO WEBHOOK … PATH` | | `POST` to the connection's URL plus the path |
| `TO KAFKA … TOPIC … KEY` | | Produce to a topic; `KEY` is the record key expression |
| `TO S3 … PREFIX` | | Write objects under a key template |
| `TO DATABASE … TABLE` | | Write rows into a MySQL, MariaDB or PostgreSQL table |
| `TO SCRIPT` | | Your own delivery code, `deliver(batch, ctx)`, in JavaScript, Lua or KIRun; it may `fetch` through the named connections and read (only read) the database |
| `GUARANTEE` | `AT LEAST ONCE` | `EXACTLY ONCE` where the source and target can give it (Kafka transactions, database targets); else 39217 |
| `DEAD LETTER TO` | a default dead-letter stream | Where records that keep failing go, after `sink.record_max_attempts` (10) |
| `START AT` | `LATEST` | Where to begin: new records only, the oldest retained, a snapshot of the tables first (`CHANGES OF`), or a time |
| `WITH (…)` | | Delivery options, such as `format` (webhooks: `json`, `ndjson`, `cloudevents` or `cloudevents_binary`), `batch`, `partial_results`, `signature`; Kafka `value_format` and `compression`; S3 `compression`; database `mode` (`upsert` by default); and object-scope `sink.*` settings such as `sink.batch_max_records` |

Checks at create time: `sink.enabled` and `sink.allowed_kinds` (39230), `sink.max_per_tenant` (39226), `USAGE` on each connection (39212), the owner's `SELECT` on the source (39212), the destination allowlist and residency (39213, 39214), formats (39231) and options (39218).

**Privileges:** `SPRIG_SINK_ADMIN`; you become the sink's owner, and it reads with your rights.

```sql
CREATE SINK orders_to_hooks
  FROM CHANGES OF shop.orders
  WHERE total > 0
  EXCEPT COLUMNS (internal_note)
  TO WEBHOOK CONNECTION hooks PATH '/orders'
  START AT SNAPSHOT
  WITH (format = 'cloudevents');

CREATE SINK events_to_kafka
  FROM STREAM shop.order_events
  TO KAFKA CONNECTION events_kafka TOPIC 'shop.order-events'
  GUARANTEE EXACTLY ONCE;

DELIMITER //
CREATE SINK audit_script
  FROM STREAM shop.order_events
  TO SCRIPT LANGUAGE javascript USING CONNECTIONS (hooks) AS $$
export function deliver(batch, ctx) {
  for (const r of batch.records) {
    ctx.fetch('/audit', { connection: 'hooks', method: 'POST', body: r.value });
  }
  return { ok: true };
}
$$ //
DELIMITER ;
```

### ALTER SINK

```sql
ALTER SINK [db_name.]sink_name PAUSE
ALTER SINK [db_name.]sink_name RESUME
ALTER SINK [db_name.]sink_name SET (option = value [, ...])
ALTER SINK [db_name.]sink_name RESET (option [, ...])
ALTER SINK [db_name.]sink_name RESET TO {LATEST | EARLIEST | SNAPSHOT | TIMESTAMP 'iso-8601'}
ALTER SINK [db_name.]sink_name OWNER TO account
ALTER SINK [db_name.]sink_name SET SCRIPT AS $$ body $$
```

| Form | Effect |
|---|---|
| `PAUSE`, `RESUME` | Stop and restart delivery. A sink also pauses itself after `sink.pause_after_minutes` of destination-wide failure; with `sink.auto_resume` it probes every `sink.probe_interval_seconds` and resumes. It pauses with 39229 when its owner can no longer read the source, and resumes by itself when the owner can again (see below) |
| `SET`, `RESET` | Change or remove options |
| `RESET TO` | Move the read position |
| `OWNER TO` | Change the account whose rights the sink reads with |
| `SET SCRIPT` | Replace a script sink's code |

An operation the sink's state doesn't allow fails with 39232 `ER_SPRIG_SINK_STATE`. **Privileges:** `SPRIG_SINK_ADMIN`.

**When the owner loses access.** A sink reads with its owner's rights. If the owner loses `SELECT` on the source (a `REVOKE`, a revoked role), or its account is locked or dropped, the sink pauses: `SHOW SINKS` shows `paused` with a `Reason` that starts with `39229 ER_SPRIG_SINK_OWNER_PRIVILEGE_LOST` and names the cause. The gateway checks when an account or grant statement commits, and each task checks before its batches, at most once a second. When the owner can read the source again, the sink resumes by itself from where it stopped; or give it an owner who can, with `OWNER TO`, and `RESUME` it. Fan-in sinks aren't checked this way. See [Sinks](../data-models/sinks.md#retries-dead-letters-and-pausing).

```sql
REVOKE SELECT ON shop.* FROM etl;   -- etl's sinks on shop pause with 39229
GRANT SELECT ON shop.* TO etl;      -- and resume
```

```sql
ALTER SINK orders_to_hooks PAUSE;
ALTER SINK orders_to_hooks RESET TO TIMESTAMP '2026-11-01T00:00:00Z';
ALTER SINK orders_to_hooks RESUME;
```

### DROP SINK

```sql
DROP SINK [IF EXISTS] [db_name.]sink_name [WITH DEAD LETTERS]
```

`WITH DEAD LETTERS` also drops the sink's default dead-letter stream. **Privileges:** `SPRIG_SINK_ADMIN`.

### SHOW SINKS and SHOW SINK STATUS

```sql
SHOW SINKS [{FROM | IN} db_name]
SHOW CREATE SINK [db_name.]sink_name
SHOW SINK STATUS [db_name.]sink_name
```

| Statement | Columns |
|---|---|
| `SHOW SINKS` | `Database`, `Name`, `Source`, `Target`, `Connection`, `Guarantee`, `State`, `Reason` |
| `SHOW CREATE SINK` | `Sink`, `Create Sink` |
| `SHOW SINK STATUS` | One row per task: `Task`, `State`, `Epoch`, `Worker`, `Delivered`, `Reads`, `Last error` |

```sql test
SHOW SINKS;
```

### TEST SINK

```sql
TEST SINK [db_name.]sink_name [WITH SAMPLE n]
TEST CREATE SINK ... [WITH SAMPLE n]
```

A dry run: reads `n` records from the source, applies the filter, columns and transform, and shows what would be sent, without delivering. One row per record: `Position`, `Outcome`, `Key`, `Value`, `Headers`, `Detail`, `Payload`. `TEST CREATE SINK` takes a whole `CREATE SINK` statement and saves nothing.

## FOR EACH TENANT

A parent tenant can define messaging objects once for all its member (child) tenants. Each member gets its own **instance**, created on first use (or in advance with `inherit.instance_precreate = on`), holding only that member's data. See [Inheritance](inheritance.md) for the tenant tree.

### Definitions

```sql
CREATE STREAM name FOR EACH TENANT [scope] [stream clauses]
CREATE [OR REPLACE] TRIGGER name ON STREAM s FOR EACH TENANT [scope] [GRANTS (...)] LANGUAGE ... AS ...
CREATE EXCHANGE [IF NOT EXISTS] [db_name.]e FOR EACH TENANT [scope] [TYPE] type [INTERNAL] [WITH (...)]
CREATE QUEUE [IF NOT EXISTS] [db_name.]q FOR EACH TENANT [scope] [WITH (...)]
BIND {QUEUE | EXCHANGE} [db_name.]d FOR EACH TENANT [scope] TO EXCHANGE s [KEY 'key'] [WITH (...)]
UNBIND {QUEUE | EXCHANGE} [db_name.]d FROM EXCHANGE s [KEY 'key']
CREATE CONNECTION c FOR EACH TENANT [scope] TYPE ... [WITH (...)] [CREDENTIALS (k = REQUIRED, ...)]
CREATE SINK s FOR EACH TENANT [scope] [GRANTS (...)] FROM STREAM d ... TO ...

scope:
    [CHILDREN | DESCENDANTS] [UNDER tenant] [INCLUDING SELF]

GRANTS (privilege [, privilege] ON db_name.object [, ...])
```

| Clause | Default | Meaning |
|---|---|---|
| `DESCENDANTS` | yes | Every tenant below yours |
| `CHILDREN` | | Direct children only |
| `UNDER t` | | Only the subtree of `t` |
| `INCLUDING SELF` | | Your own tenant gets an instance too |
| `GRANTS (…)` | | Triggers and sinks: the privileges the code needs in each member, such as `INSERT, UPDATE ON app.users`. Each member's instance runs as that member's own principal; a missing privilege fails with 39242 |
| `CREDENTIALS (k = REQUIRED)` | | Connections: each member must set this credential itself (`ALTER INHERITED CONNECTION … SET CREDENTIALS`), else 39241 |

Queue `WITH` arguments use underscores for AMQP's dashes: `x_queue_type = 'quorum'` is `x-queue-type`.

```sql
CREATE STREAM security.user_events FOR EACH TENANT PARTITIONS 6;

DELIMITER //
CREATE TRIGGER security.sync_users ON STREAM security.user_events
  FOR EACH TENANT GRANTS (INSERT, UPDATE, SELECT ON app.users)
  LANGUAGE javascript AS $$
export default function (ctx) {
  const e = JSON.parse(ctx.record.value);
  db.exec('INSERT INTO app.users (id, email) VALUES (?, ?) ON DUPLICATE KEY UPDATE email = VALUES(email)',
          [e.user_id, e.email]);
}
$$ //
DELIMITER ;
```

`ALTER STREAM`, `ALTER CONNECTION … SET | RESET` and `ALTER SINK … SET | RESET` on a definition create a new version that every instance follows. `CREATE OR REPLACE TRIGGER` updates existing instances in the background.

### Dropping definitions

```sql
DROP STREAM [IF EXISTS] s [IMMEDIATE | CASCADE]
DROP TRIGGER g
DROP {EXCHANGE | QUEUE} ... [CASCADE]
DROP SINK s
DROP CONNECTION c
```

A stream definition that trigger definitions read is refused unless `CASCADE`. Without `IMMEDIATE`, instances drain for `inherit.drop_grace_hours` (168) before removal; with it, every instance goes now. Dropping a connection definition that a sink definition names fails with 39215.

### Publishing across tenants

```sql
INSERT [IGNORE] [INTO] s [(columns)] VALUES ...                         -- a member: into its own instance
INSERT [IGNORE] [INTO] s FOR TENANT t [(columns)] VALUES ...            -- an ancestor: into t's instance
INSERT [IGNORE] [INTO] s (tenant, key, value [, headers]) VALUES ...    -- an ancestor: one target per row
INSERT [IGNORE] [INTO] s FOR ALL TENANTS [WITH INSTANCE] [UNDER t] [(columns)] VALUES ...   -- broadcast
INSERT [IGNORE] [INTO] s IN TENANT t [(columns)] VALUES ...             -- a descendant: into an ancestor's stream
```

Every record carries the headers `x-sprig-tenant` (the instance's tenant) and `x-sprig-origin-tenant` (the publisher's), set by SprigDB over anything the client sent.

| Form | Rules |
|---|---|
| Own instance | Allowed unless `inherit.member_publish = off` |
| `FOR TENANT t` | From an ancestor of `t` |
| `FOR ALL TENANTS` | A **broadcast**: one record delivered to every member below you (inside `UNDER t`; `WITH INSTANCE`: only members that already have an instance), exactly once and in order per member. Limited by `inherit.broadcast_max_bytes`, `inherit.broadcasts_per_hour` and `inherit.broadcast_max_members` (39244). With `inherit.broadcast_replay = on`, a member's new instance also gets the broadcasts still kept (`inherit.broadcast_retention_hours`) |
| `IN TENANT t` | Into an ancestor's ordinary stream, when it allows `stream.descendant_publish = on`. Under `stream.descendant_key = origin` (the default), the record key must be your tenant name (39957) |

Inside a transaction, a cross-tenant publish becomes a row of your outbox and is forwarded after commit.

### Overrides

```sql
ALTER INHERITED {STREAM | TRIGGER | QUEUE | EXCHANGE | BINDING | CONNECTION | SINK} [db_name.]name
    [FOR TENANT t | FOR SUBTREE [t]]
    { SPRIG SET (k = v, ...) | SET (k = v, ...) [CREDENTIALS (...)] | SET CREDENTIALS (...)
    | RESET [(k, ...)] | DISABLE | ENABLE | DETACH | REPLACE LANGUAGE language AS body }
```

A member (or an ancestor, for one member with `FOR TENANT` or a subtree with `FOR SUBTREE`) overrides a definition: settings, a connection's options and credentials, disabling an instance, detaching it into an ordinary object, or replacing a trigger's code. The nearest level wins. A setting that can't be overridden fails with 39897.

### SHOW INHERITED

```sql
SHOW INHERITED DEFINITIONS [FOR kind [db_name.]name]
SHOW INHERITED INSTANCES [FOR kind [db_name.]name]
SHOW INHERITED OVERRIDES [FOR kind [db_name.]name]
SHOW INHERITED BROADCASTS FOR {STREAM | QUEUE | EXCHANGE} [db_name.]name
```

They list the definitions you declared or inherit, the instances (which member, created when, state), the overrides at each level, and a definition's broadcasts with their delivery progress.

```sql test
SHOW INHERITED DEFINITIONS;
```

## Errors

| Code | Name | When |
|---|---|---|
| 39940 | `ER_SPRIG_STREAM_NOT_FOUND` | Unknown stream |
| 39941 | `ER_SPRIG_STREAM_APPEND_ONLY` | `UPDATE`, `DELETE`, `REPLACE`, `TRUNCATE` or an upsert on a stream; a publish to a change topic |
| 39942 | `ER_SPRIG_STREAM_EXISTS` | The stream name is in use |
| 39943 | `ER_SPRIG_STREAM_PARTITIONS_DECREASE` | Fewer partitions |
| 39944 | `ER_SPRIG_STREAM_RECORD_TOO_LARGE` | Record above `stream.max_message_bytes`, or key above 4096 bytes |
| 39945 | `ER_SPRIG_STREAM_KEY_NOT_ATTACHED` | The key doesn't determine the attached group's key, or the group has several shards |
| 39946 | `ER_SPRIG_STREAM_SEQUENCING_TIMEOUT` | No offset within `stream.produce_wait_ms` |
| 39947 | `ER_SPRIG_STREAM_OFFSET_OUT_OF_RANGE` | An offset below the log start or above the high watermark |
| 39949 | `ER_SPRIG_STREAM_RESIDENCY` | A cross-tenant publish outside the allowed data centres |
| 39955 | `ER_SPRIG_STREAM_COMPACT_NEEDS_KEY` | A record without a key on a compacted stream |
| 39957 | `ER_SPRIG_STREAM_ORIGIN_KEY` | A descendant's publish with a key other than its tenant name |
| 39958 | `ER_SPRIG_STREAM_OFFSET_TXN_UNSUPPORTED` | `sprig.commit_offset` in a transaction on another cluster than the catalog |
| 39210 | `ER_SPRIG_SINK_NOT_FOUND` | Unknown sink or connection |
| 39211 | `ER_SPRIG_SINK_EXISTS` | The name is in use |
| 39212 | `ER_SPRIG_SINK_PRIVILEGE` | Missing `SPRIG_SINK_ADMIN`, `USAGE` on a connection, the owner's `SELECT`, or `SPRIG_FAN_IN` |
| 39213 | `ER_SPRIG_SINK_DESTINATION_NOT_ALLOWED` | A destination off `sink.destination_allowlist` |
| 39214 | `ER_SPRIG_SINK_RESIDENCY` | The destination breaks the tenant's residency policy |
| 39215 | `ER_SPRIG_SINK_CONNECTION_IN_USE` | Dropping a connection that sinks use |
| 39216 | `ER_SPRIG_SINK_CREDENTIALS_INVALID` | A credential the type doesn't take, or a required one missing |
| 39217 | `ER_SPRIG_SINK_EXACTLY_ONCE_UNSUPPORTED` | `EXACTLY ONCE` where the source and target can't give it |
| 39218 | `ER_SPRIG_SINK_OPTION_INVALID` | An unknown or invalid option, template or column list |
| 39219 | `ER_SPRIG_SINK_TRANSFORM_INVALID` | A transform that writes, calls out, or isn't deterministic under `EXACTLY ONCE` |
| 39220 | `ER_SPRIG_SINK_SCRIPT_FAILED` | A script raised an error or returned a malformed result |
| 39221 | `ER_SPRIG_SINK_DELIVERY_FAILED` | The destination refused or failed a delivery |
| 39222 | `ER_SPRIG_SINK_PAUSED` | The sink paused itself after failures |
| 39223 | `ER_SPRIG_SINK_SOURCE_LOST` | The sink fell behind its source's retention |
| 39224 | `ER_SPRIG_SINK_SCHEMA_MISMATCH` | The destination table or file schema doesn't fit |
| 39225 | `ER_SPRIG_SINK_RECORD_TOO_LARGE` | One record exceeds what the destination accepts |
| 39226 | `ER_SPRIG_SINK_QUOTA` | Over `sink.max_per_tenant` or a throughput quota |
| 39227 | `ER_SPRIG_SINK_FENCED` | A task's epoch moved on; the old worker stops |
| 39228 | `ER_SPRIG_SINK_PLATFORM_IDENTITY_DENIED` | The worker's cloud identity isn't allowed (`sink.platform_identity`) |
| 39229 | `ER_SPRIG_SINK_OWNER_PRIVILEGE_LOST` | A sink's pause reason: the owner lost `SELECT` on the source, or was locked or dropped; it resumes when access returns |
| 39230 | `ER_SPRIG_SINK_DISABLED` | `sink.enabled` is off, or the kind isn't in `sink.allowed_kinds` |
| 39231 | `ER_SPRIG_SINK_FORMAT_UNSUPPORTED` | A format or compression the target can't produce |
| 39232 | `ER_SPRIG_SINK_STATE` | An operation the sink's state doesn't allow |
| 39233 | `ER_SPRIG_SINK_PLATFORM_CONNECTION_NOT_GRANTED` | A platform connection the tenant isn't granted |
| 39234 | `ER_SPRIG_SINK_PLATFORM_SECRET_UNAVAILABLE` | A worker can't read a platform connection's secret |
| 39235 | `ER_SPRIG_SINK_SCRIPT_READ_ONLY` | A script's `db` handle was given anything but a read |
| 39240 | `ER_SPRIG_INHERIT_INSTANCE_QUOTA` | Over `inherit.max_definitions_per_tenant` or `inherit.max_instances_per_tenant` |
| 39241 | `ER_SPRIG_INHERIT_CONFIG_MISSING` | A `REQUIRED` option or credential isn't set for this member |
| 39242 | `ER_SPRIG_INHERIT_RUN_AS_PRIVILEGE` | A member lacks a privilege the definition's `GRANTS` asks for |
| 39243 | `ER_SPRIG_INHERIT_RESIDENCY` | A fan-in or cross-tenant publish would move data where residency forbids |
| 39244 | `ER_SPRIG_INHERIT_BROADCAST_LIMIT` | A broadcast over a broadcast limit |
| 39245 | `ER_SPRIG_INHERIT_OUTBOX_PENDING` | `DROP DATABASE` while the cross-tenant outbox still holds rows |
| 39896 | `ER_SPRIG_INHERIT_TARGET_TENANT` | A publish that names no target where one is needed, a tenant that isn't a member, or one you aren't a proper ancestor of |
| 39897 | `ER_SPRIG_INHERIT_NOT_OVERRIDABLE` | A member changes what only the definition decides, drops an instance, or creates a local object with an inherited name |
| 39898 | `ER_SPRIG_INHERIT_DEFINITION_DROPPING` | A publish into a definition or instance that is being dropped or has left its scope |
| 39899 | `ER_SPRIG_INHERIT_INSTANCE_UNAVAILABLE` | An instance wasn't ready within `inherit.instance_create_wait_ms` |
| 39366 | `ER_SPRIG_RLS_UNSUPPORTED` | Reading a change topic of a table whose rows a policy filters for you |

## Related

- [Streams](../data-models/streams.md)
- [Queues](../data-models/queues.md)
- [Sinks](../data-models/sinks.md)
- [Logic in SQL](logic.md)
- [Inheritance in SQL](inheritance.md)
