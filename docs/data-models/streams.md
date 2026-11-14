---
title: Event streams
audience: [user, dba]
sidebar:
  order: 9
---

# Event streams

An event stream is an append-only, partitioned log of records, compatible with Kafka. Use one when something happened ("order paid", "user added") and any number of readers should see every record, each at its own pace, and replay it later. If each message is work that exactly one worker should do, use a [queue](queues.md) instead.

You create and publish to streams with SQL, read them as tables, and produce and consume them with any Kafka client. A publish inside a SQL transaction is part of that transaction: the record exists only if the transaction commits.

## How streams are stored

A stream belongs to one database of one tenant, like a table. In MariaDB it is three internal tables in the database's shard schemas:

| Part | What it holds | Where it lives |
|---|---|---|
| Outbox | Records just published, not yet given an offset | The shard of the table the stream is attached to, or the database's default shard |
| Log | Sequenced records: partition, offset, timestamp, key, value, headers | The database's default shard |
| Checkpoints | Each partition's next offset and log start, and the stream's leader epoch | Beside the log |

A publish is a plain insert into the outbox, inside your transaction. A background **sequencer**, running in a `sprigdb worker`, reads committed outbox rows from the change feed in commit order, gives each record its partition offset and appends it to the log. It appends in batches, each written when its oldest record has waited 10 ms or it holds 1,000 records. Records become readable a few tens of milliseconds after the commit. Rolled-back records never appear, and offsets have no gaps except those that compaction leaves.

The log is partitioned by day, so time retention drops whole day partitions rather than deleting rows. Every stream has its own sequencer. When the tenant moves to another cluster, its streams move with it and keep their offsets.

## Create a stream

To create a stream, run `CREATE STREAM` in the database that should own it. Here the stream is attached to the `orders` table, so that publishes in a transaction that writes an order stay on the order's shard:

```sql test
CREATE TABLE orders (
  order_id BIGINT PRIMARY KEY,
  status   VARCHAR(16) NOT NULL,
  amount   DECIMAL(10,2) NOT NULL
);

CREATE STREAM order_events
  KEY (order_id) ATTACHED TO GROUP OF orders
  PARTITIONS 4 RETENTION '7 days';
```

The full grammar:

```sql
CREATE STREAM [IF NOT EXISTS] [db.]name
  [KEY (column, ...)] [ATTACHED TO GROUP OF table]
  [PARTITIONS n] [RETENTION 'text'] [CLEANUP delete | compact]
```

| Clause | Default | Meaning |
|---|---|---|
| `KEY (column, ...)` | none | Names the event key. Shown by `SHOW STREAMS`. It doesn't fill the key for you: you still give `key` on every publish |
| `ATTACHED TO GROUP OF table` | none | Puts the outbox in the same shard as `table`, so a publish in a transaction that writes `table` stays on one MariaDB server. The table must be in the same database and its table group must have one shard (`ER_SPRIG_STREAM_KEY_NOT_ATTACHED`, 39945, otherwise) |
| `PARTITIONS n` | `stream.partitions_default` (12) | Number of partitions, at least 1 |
| `RETENTION 'text'` | `stream.retention_hours` (168) | How long records are kept: `'72 hours'`, `'7 days'`, `'2 weeks'`, `'90 minutes'` (rounded up to whole hours), or a bare number of hours |
| `CLEANUP delete` or `compact` | `delete` | `delete` removes old records by time and size. `compact` also keeps only the latest record of each key; see [Compaction](#compaction) |

Stream names follow Kafka's topic rules: 1 to 249 characters of letters, digits, `.`, `_` and `-`, and not `.` or `..`. Quote a name that has dots or dashes in SQL: `` `orders.changes` ``. A stream and a table can't share a name in one database (`ER_SPRIG_STREAM_EXISTS`, 39942).

Creating a stream needs `CREATE` on the database.

### Change and drop a stream

```sql test
ALTER STREAM order_events PARTITIONS 24, RETENTION '3 days';

-- Per-stream settings, set or reset at the stream's own scope.
ALTER STREAM order_events
  SET (stream.retention_bytes = 1048576, stream.max_message_bytes = 2000000);
ALTER STREAM order_events RESET (stream.retention_bytes);

SHOW STREAMS;
```

Partitions never shrink:

```sql test-error
ALTER STREAM order_events PARTITIONS 2;  -- ER_SPRIG_STREAM_PARTITIONS_DECREASE (39943)
```

Switch the cleanup policy, qualify the name with its database, or drop the stream:

```sql
ALTER STREAM shop.order_events CLEANUP compact;
SHOW STREAMS FROM shop;
DROP STREAM IF EXISTS shop.order_events;
```

- Partitions can only grow (`ER_SPRIG_STREAM_PARTITIONS_DECREASE`, 39943). As in Kafka, adding partitions changes which partition a key maps to from then on; records already in the log stay where they are.
- `SET (...)` and `RESET (...)` (also written `SPRIG SET` and `SPRIG RESET`) accept `stream.retention_hours`, `stream.retention_bytes`, `stream.max_message_bytes`, `stream.compaction_lag_minutes` and `stream.tombstone_hours`. Any other key, or a value outside the key's bounds, is error 1231.
- `ALTER STREAM` needs `ALTER`, and `DROP STREAM` needs `DROP`, on the stream or its database.
- `DROP DATABASE` drops the database's streams first.
- `SHOW STREAMS [FROM | IN db]` lists the streams you may read, with the columns `Stream`, `Kind` (`events` or `changes`), `Partitions`, `Cleanup`, `Source`, `Key` and `Lag_ms` (how far the stream's sequencer is behind, in milliseconds; `NULL` while none is running).

## Records, keys and partitions

Every record has these columns:

| Column | Type | Meaning |
|---|---|---|
| `partition_no` | unsigned integer | The partition |
| `offset_no` | unsigned integer | Position in the partition, from 0 |
| `ts` | `DATETIME(3)`, UTC | The publish time, or the Kafka producer's timestamp |
| `key` | binary, up to 4,096 bytes | The key; may be `NULL` |
| `value` | binary | The value; `NULL` is a tombstone on a compacted stream |
| `headers` | JSON object | Record headers |

A record's partition is chosen as Kafka's default partitioner chooses it, bit for bit:

- **With a key:** `murmur2(key)` modulo the number of partitions. SQL, logic and Kafka producers therefore put the same key on the same partition, so records of one key stay in order.
- **Without a key:** spread by a hash of the record's internal event id.
- **Explicit:** a Kafka producer that names a partition, or a SQL insert that sets `partition_no`, is obeyed.

Headers are stored as one JSON object. A Kafka header whose value is UTF-8 becomes a string, a null value becomes `null`, and other bytes become `{"base64": "..."}`. A header key given twice keeps its last value.

## Publish with SQL

Insert into the stream as if it were a table with the columns `key`, `value`, `headers`, `partition_no` and `ts`:

```sql test
INSERT INTO orders (order_id, status, amount) VALUES (7001, 'open', 49.90);

START TRANSACTION;
UPDATE orders SET status = 'paid' WHERE order_id = 7001;
INSERT INTO order_events (key, value, headers)
  VALUES (7001, JSON_OBJECT('type', 'order.paid', 'order_id', 7001),
          JSON_OBJECT('source', 'checkout'));
COMMIT;
```

The update and the record commit together. If the transaction rolls back, nothing is published.

- Any insert shape works: several `VALUES` rows, or `INSERT INTO order_events (key, value) SELECT ...`.
- Without a column list, two values mean `(key, value)` and three mean `(key, value, headers)`.
- `ts` and `partition_no` are optional. Without `ts`, the record gets the time of the insert.
- Publishing needs `INSERT` on the stream or its database.

A stream is append-only:

```sql test-error
UPDATE order_events SET value = 'x';  -- ER_SPRIG_STREAM_APPEND_ONLY (39941)
```

Refused forms:

| Statement | Error |
|---|---|
| `UPDATE`, `DELETE`, `REPLACE` or `TRUNCATE` on a stream | `ER_SPRIG_STREAM_APPEND_ONLY` (39941) |
| `INSERT ... ON DUPLICATE KEY UPDATE` | 39941 |
| `INSERT ... SET col = ...` | 1235: use `VALUES` or `SELECT` |
| An insert into a change topic | 39941 |
| A literal larger than `stream.max_message_bytes` | `ER_SPRIG_STREAM_RECORD_TOO_LARGE` (39944) |
| A record without a key on a compacted stream | `ER_SPRIG_STREAM_COMPACT_NEEDS_KEY` (39955) |

Logic code publishes the same way. `db.publish(stream, key, value, headers?)` in JavaScript and Lua, and `Sprig.Db.Publish` in KIRun, run that `INSERT` through the caller's session, so the record is part of the caller's transaction. In logic, a `null` value is a tombstone. See [Logic](logic.md).

## Read with SQL

A stream reads like a table. Filter on `partition_no` and `offset_no` to read from a position; the log's primary key serves those ranges.

```sql test
SELECT partition_no, offset_no, ts, CAST(key AS CHAR) AS k, CAST(value AS CHAR) AS v
FROM order_events
WHERE partition_no = 3 AND offset_no >= 1200
ORDER BY offset_no
LIMIT 100;

SELECT partition_no, COUNT(*) FROM order_events GROUP BY partition_no;
```

`key` and `value` are binary; cast them to read them as text. You can write `key` and `value` unquoted in a statement that reads a stream, although `KEY` is a reserved word in MariaDB: SprigDB quotes them for you.

Reading needs `SELECT` on the stream or its database. A SQL read is a snapshot: it returns what has been sequenced so far, and doesn't wait for new records. To follow a stream as it grows, use a Kafka consumer, a [trigger on the stream](logic.md) or a [sink](sinks.md).

## Over REST

Streams have no REST publish or read call of their own. Run the same statements through the SQL endpoint, as the caller:

```sh
curl -s https://gateway.example.com:8443/v1/databases/shop/sql:execute \
  -H 'Authorization: Bearer <api-key>' \
  -H 'Content-Type: application/json' \
  --data @- <<'EOF'
{"sql": "INSERT INTO order_events (key, value) VALUES (7001, JSON_OBJECT('type', 'order.paid'))"}
EOF
```

To group a publish with other writes, open a transaction with `POST /v1/databases/{database}/transactions` and pass its handle on each statement. `GET /v1/databases/{database}/streams/{stream}/groups` returns the stream's partitions with their log start and high watermark, and its consumer groups with their offsets and lag. See the [REST API](../reference/api.md).

## Produce and consume with Kafka

The Kafka listener is off by default. Turn it on with `listener.kafka.enabled`; it listens on 9092 and requires TLS by default (`listener.kafka.tls`). See [Kafka](../connect/kafka.md) for connecting.

- **Login.** SASL with `SCRAM-SHA-512`, `SCRAM-SHA-256` or `PLAIN` (`kafka.sasl_mechanisms`). `PLAIN` is offered only over TLS. The user name names the tenant: `acme.app`.
- **Topics** are named `<database>.<stream>`, split at the first dot: stream `order_events` in `shop` is the topic `shop.order_events`, and the change topic of `shop.orders` is `shop.orders.changes`. A name without a database part is refused with `INVALID_TOPIC_EXCEPTION`.
- **Privileges.** Producing needs `INSERT` on the stream or its database, consuming needs `SELECT`. A topic you can neither read nor write is invisible to you: Metadata, DescribeConfigs, Produce and Fetch answer as if it didn't exist.

For example, with `confluent-kafka` for Python:

```python
from confluent_kafka import Consumer, Producer

conf = {
    "bootstrap.servers": "gateway.example.com:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": "acme.app",
    "sasl.password": "<password>",
}

p = Producer({**conf, "enable.idempotence": True, "acks": "all"})
p.produce("shop.order_events", key="7001", value='{"type":"order.paid"}',
          headers={"source": "checkout"})
p.flush()

c = Consumer({**conf, "group.id": "billing", "auto.offset.reset": "earliest"})
c.subscribe(["shop.order_events"])
msg = c.poll(5.0)
```

Or with `kcat`:

```sh
kcat -b gateway.example.com:9092 -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username=acme.app -X sasl.password='<password>' \
  -G billing shop.order_events
```

### Kafka APIs served

| API (key) | Versions | Notes |
|---|---|---|
| Produce (0) | 3–9 | `acks=1` and `acks=-1` both mean committed and sequenced; idempotent producers |
| Fetch (1) | 4–13 | Long poll; v13 finds topics by id |
| ListOffsets (2) | 1–7 | Earliest, latest, by timestamp, and largest timestamp (v7) |
| Metadata (3) | 0–12 | Gateways act as brokers; one replica per partition is shown |
| OffsetCommit (8), OffsetFetch (9) | 0–8, 0–8 | Offsets in the tenant's catalog |
| FindCoordinator (10) | 0–4 | Group coordinators only |
| JoinGroup (11), Heartbeat (12), LeaveGroup (13), SyncGroup (14) | 0–9, 0–4, 0–5, 0–5 | The classic group protocol |
| DescribeGroups (15), ListGroups (16), DeleteGroups (42) | 0–5, 0–5, 0–2 | |
| SaslHandshake (17), SaslAuthenticate (36) | 0–1, 0–2 | |
| ApiVersions (18) | 0–4 | Advertises only the APIs in this table |
| CreateTopics (19), DeleteTopics (20), CreatePartitions (37) | 0–7, 0–6, 0–3 | Run `CREATE STREAM`, `DROP STREAM` and `ALTER STREAM ... PARTITIONS` as you |
| InitProducerId (22) | 0–4 | Without `transactional.id` only |
| OffsetForLeaderEpoch (23) | 2–4 | Detects records lost in a failover |
| DescribeConfigs (32), IncrementalAlterConfigs (44) | 0–4, 0–1 | Topic configs below |

A request for any other API or version closes the connection, and the gateway logs `ER_SPRIG_KAFKA_UNSUPPORTED_API` (39952). The only exception is ApiVersions, which answers an unknown version with `UNSUPPORTED_VERSION` and the supported ranges.

### Producing

- **Batches** must be record batch format v2 (magic 2), one batch per partition, compressed with none, gzip, snappy, LZ4 or zstd. A batch may inflate to at most 64 MiB.
- **Idempotent producers** (`enable.idempotence`) follow Kafka's rules per producer id and partition: a retried batch matching one of the last five is answered with its original offsets and isn't appended again; a sequence gap is `OUT_OF_ORDER_SEQUENCE_NUMBER`; an older epoch is `INVALID_PRODUCER_EPOCH`. Producer state is kept next to the outbox and expires after seven days without use.
- **Acknowledgement.** The produce response carries the batch's base offset, so the gateway waits until the sequencer has given the records their offsets, for at most `stream.produce_wait_ms` (2,000 ms). After that the answer is `REQUEST_TIMED_OUT` with `ER_SPRIG_STREAM_SEQUENCING_TIMEOUT` (39946). The records are already committed, so a retry from an idempotent producer returns their offsets without writing them twice.
- **Pipelining.** While one produce waits for its offsets, the connection's next requests are read and served, and responses go out in request order, so a producer with several batches in flight isn't held to one wait at a time.
- **Timestamps.** Records keep the producer's timestamp (`CreateTime`). A batch with `LogAppendTime`, or no timestamp, gets the gateway's time.

Produce refusals:

| Case | Kafka error | SprigDB code in the message |
|---|---|---|
| `acks=0` | The connection is closed | `ER_SPRIG_KAFKA_ACKS_ZERO` (39950) |
| `acks` other than 0, 1 or -1 | `INVALID_REQUIRED_ACKS` | |
| `transactional.id` set, or a transactional batch | `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` | `ER_SPRIG_KAFKA_TRANSACTIONS_UNSUPPORTED` (39951) |
| A change topic | `INVALID_TOPIC_EXCEPTION` | 39941 |
| A record above `stream.max_message_bytes`, or a key above 4,096 bytes | `MESSAGE_TOO_LARGE` | 39944 |
| A keyless record on a compacted stream | `INVALID_RECORD` | 39955 |
| An unknown compression codec | `UNSUPPORTED_COMPRESSION_TYPE` | |

### Consuming

- **Fetch** reads from the requested offset. An offset outside `[log start, high watermark]` is `OFFSET_OUT_OF_RANGE`, and the response carries both bounds. The first record is returned whatever its size; after that the partition and request byte budgets apply.
- **Long poll.** While fewer than `min_bytes` were found, a fetch waits up to the client's `max_wait_ms`, capped by `kafka.fetch_max_wait_ms` (500 ms), and wakes as soon as new records are sequenced.
- **Isolation.** The log holds only committed records, so `read_committed` and `read_uncommitted` read the same records, and the last stable offset is the high watermark.
- **Fetch sessions** (KIP-227) aren't kept: every response says session 0 and clients send full fetches, which every client handles.
- **Leader epochs.** A partition's leader epoch rises with each new sequencer. A client with an older epoch gets `FENCED_LEADER_EPOCH` and refreshes its metadata. After a failover that lost records, OffsetForLeaderEpoch tells the client where its epoch's records end, so the client can detect the truncation, as with Kafka.

### Consumer groups and offsets

Consumer groups use Kafka's classic protocol, served by the gateways. Each group has one coordinator gateway in the data centre at a time, held by a lease; if that gateway stops, another takes the group over.

- Kafka's behaviour is kept: `MEMBER_ID_REQUIRED` from JoinGroup v4, static members (`group.instance.id`), the leader kept across rebalances.
- An empty group's first rebalance waits `kafka.group_initial_rebalance_delay_ms` (3,000 ms) for more members, like Kafka's `group.initial.rebalance.delay.ms`.
- A member may ask for a session timeout of up to `kafka.group_session_timeout_max_ms` (300,000 ms).
- Committed offsets are stored in the tenant's catalog, in MariaDB, so any gateway answers OffsetFetch. Offset metadata over 1,024 characters is `OFFSET_METADATA_TOO_LARGE`.
- ListGroups on a gateway lists the groups that gateway coordinates, so a client that asks every broker sees each group once.
- DeleteGroups refuses a group with members (`NON_EMPTY_GROUP`) and otherwise deletes the group and its offsets.

### Topic administration

CreateTopics, DeleteTopics, CreatePartitions and IncrementalAlterConfigs run the matching stream statement as you, so they need the same privileges as SQL. The call returns once the change is visible, within five seconds. `validate_only` checks without changing anything.

- **Partitions** default to `stream.partitions_default`. The replication factor must be -1 or at least 1, and is ignored: MariaDB replicates the log.
- **Topic configs:**

| Config | Stored as | Notes |
|---|---|---|
| `retention.ms` | `stream.retention_hours` | Rounded up to whole hours; `-1` (no time limit) is refused |
| `retention.bytes` | `stream.retention_bytes` | `-1` means no size limit; `0` is refused because it would keep nothing |
| `max.message.bytes` | `stream.max_message_bytes` | |
| `cleanup.policy` | `CLEANUP` | `delete` or `compact`, not both |

  Deleting a config resets it. Replication and segment configs that tools often send (`min.insync.replicas`, `segment.*`, `compression.type`, `delete.retention.ms` and others) are accepted and ignored; any other config is `INVALID_CONFIG`. DescribeConfigs names each config's SprigDB setting as its synonym. Broker configs read as an empty list and can't be altered.
- A change topic can't be deleted through Kafka (`POLICY_VIOLATION`). Turn its setting off instead.

## Exactly-once consume and write

A consumer that writes results to SprigDB can commit its group offset in the same transaction as those writes. If the transaction rolls back, the offset does too, and the record is read again; if it commits, it is never read again.

```sql test
CREATE TABLE invoices (order_id BIGINT PRIMARY KEY, amount DECIMAL(10,2) NOT NULL);

START TRANSACTION;
INSERT INTO invoices (order_id, amount) VALUES (7001, 49.90);
CALL sprig.commit_offset('billing', 'order_events', 5, 1206);
COMMIT;
```

`CALL sprig.commit_offset(group, stream, partition, offset [, metadata])`:

| Argument | Meaning |
|---|---|
| `group` | The consumer group, the same groups Kafka clients use |
| `stream` | `[db.]name`; a dotted name is read as a database and a stream, like a Kafka topic name |
| `partition` | The partition |
| `offset` | The next offset to read, as in Kafka: after processing offset 1205, commit 1206 |
| `metadata` | Optional text, as in Kafka's OffsetCommit |

Any SQL expression works for the arguments. Committing needs `SELECT` on the stream or its database (1142 otherwise). Outside a transaction the offset is written at once. Inside one, the transaction's connection must be on the same cluster as the tenant's catalog; otherwise the call fails with `ER_SPRIG_STREAM_OFFSET_TXN_UNSUPPORTED` (39958) rather than becoming a distributed transaction.

Logic code calls the same thing with `db.commitOffset(group, stream, partition, offset)`, or `Sprig.Db.CommitOffset` in KIRun.

## Streams fed from table changes

A **change topic** is a stream that SprigDB fills with every committed change of a table. Turn it on per table:

```sql test
ALTER TABLE orders SPRIG SET (stream.change_topic = on);
```

This creates the stream `orders.changes` (the Kafka topic `shop.orders.changes`). Turning the setting `off` drops it. Only a table's own setting creates a topic: setting `stream.change_topic` on a database or tenant doesn't create topics for every table.

Each record is one row change, in commit order. Its key is the JSON of the row's primary key, such as `{"order_id":7001}`, so all changes of one row land on one partition in order. Its value is a JSON envelope:

```json
{"op": "u",
 "before": {"order_id": 7001, "status": "open", "amount": 49.9},
 "after":  {"order_id": 7001, "status": "paid", "amount": 49.9},
 "source": {"tenant": "acme", "db": "shop", "object": "orders", "model": "table",
            "shard": "g3/s17", "gtid": "0-11-90412", "hlc": 7587861234,
            "txn": "0-11-90412"},
 "ts_ms": 1762508100000}
```

- `op` is `c` (insert), `u` (update) or `d` (delete). A schema change is `ddl`, with the statement in `ddl`, and a `TRUNCATE` is `t`; both go to partition 0 without a key.
- Internal columns never appear. `DECIMAL` values are JSON numbers, so trailing zeros are lost (`49.90` is `49.9`). Binary and spatial values are base64, and dates and times are MariaDB's text.
- Change topics are written only by SprigDB: publishing into one is 39941.
- Reading a change topic needs `SELECT` on its table. A reader bound by the table's [row-level security](../security/row-level-security.md) policies sees only the changes those policies show it; an update that hides or reveals a row reaches that reader as a delete or an insert. Offsets aren't renumbered, so filtered records leave gaps.

See [Change feed](../concepts/change-feed.md) for how changes are captured. To send a table's changes to an AMQP exchange instead, see `queue.change_exchange` in [Queues](queues.md).

## Retention

Each stream keeps records by time and, optionally, by size. A maintenance pass in the sequencer job runs every minute.

- **Time.** The log is partitioned by day. A day's records are dropped once the whole day is older than the retention, so a record can stay up to a day longer than `stream.retention_hours`.
- **Size.** When `stream.retention_bytes` is above 0, the oldest whole days before today are dropped while the log is larger than `retention_bytes` times the number of partitions. MariaDB can't size one stream partition inside a day, so the limit applies to the stream as a whole.
- After each pass, each partition's log start moves to its smallest remaining offset. Reading below it is `ER_SPRIG_STREAM_OFFSET_OUT_OF_RANGE` (39947) in SQL, and `OFFSET_OUT_OF_RANGE` in Kafka.

### Compaction

A stream with `CLEANUP compact` keeps the latest record of each key, as Kafka's compacted topics do:

- Every record needs a key (39955 otherwise).
- A record older than `stream.compaction_lag_minutes` (60) is deleted once a later record with the same key exists.
- A record with a `NULL` value is a tombstone. It is deleted after `stream.tombstone_hours` (24) when it is still its key's latest record.
- Offsets keep the gaps compaction leaves, as in Kafka.
- Time and size retention still apply.

## Tenants and fan-in

Every stream belongs to one tenant. A Kafka login names its tenant, and a client sees only that tenant's streams. Three features let records from many tenants or sources come together.

### Many publishers into one stream

Any number of producers can write one stream: SQL sessions, logic code, Kafka producers and `INSERT ... SELECT` from other tables. Order is kept per partition, so give records that must stay in order the same key.

### A platform stream that descendants publish into

A descendant tenant's session can publish into an **ancestor's** stream with `IN TENANT`. Here tenant `client_x` publishes into the stream `platform.client_events` of its ancestor `system`:

```sql
INSERT INTO platform.client_events IN TENANT system (key, value)
  VALUES ('client_x', JSON_OBJECT('type', 'user.added', 'user_id', 901));
```

- The stream's owner must consent by setting `stream.descendant_publish = on` on the stream (or its database or tenant). Without it, the publish is refused with 1142, as if the stream didn't take publishes.
- Under `stream.descendant_key = origin` (the default), the key must be the publishing tenant's name (`ER_SPRIG_STREAM_ORIGIN_KEY`, 39957, otherwise). With `any`, the descendant chooses the key.
- The server stamps the header `x-sprig-origin-tenant` with the publisher's tenant.
- Inside a transaction, the record goes through the publisher's own outbox and is forwarded after the commit, so the transaction stays on the publisher's cluster and the record lands exactly once.
- With the platform-wide `residency.cross_tenant_messaging_strict = on`, a publish from a pinned tenant into a stream outside its allowed data centres is refused with `ER_SPRIG_STREAM_RESIDENCY` (39949).

Every reader of the stream in the owning tenant sees every tenant's records. Route by key, and check `x-sprig-origin-tenant`, in your consumer.

### A stream for each tenant, read together

`CREATE STREAM s FOR EACH TENANT ...` declares a stream definition that every member tenant gets its own instance of, with its own storage, offsets and retention. An ancestor publishes into one member's instance with `INSERT INTO s FOR TENANT t ...`, or into every member's with `FOR ALL TENANTS`, and reads all instances together with `SELECT ... FROM s FAN IN`. Over Kafka, a holder of `SPRIG_FAN_IN` sees each member's instance as the topic `__t.<member>.<db>.<stream>`, and can subscribe to them all with a pattern. Records carry the header `x-sprig-tenant`. See [Inheritance](inheritance.md).

## Limits and sharding behaviour

| Limit | Value | When exceeded |
|---|---|---|
| Stream name | 1 to 249 characters of `[a-zA-Z0-9._-]` | 1103 |
| Partitions of a new stream | 1 to 10,000 (`stream.partitions_default` bounds) | |
| Record size | `stream.max_message_bytes`, 1 MiB by default, at most 1 GiB | 39944 |
| Key size | 4,096 bytes | 39944 |
| Time retention | 1 hour to 87,600 hours (10 years) | 1231 from `ALTER STREAM ... SET` |
| Kafka produce batch, uncompressed | 64 MiB | |
| Wait for offsets on produce | `stream.produce_wait_ms`, 2,000 ms | 39946, retryable |
| Fetch long-poll wait | `kafka.fetch_max_wait_ms`, 500 ms | |

Sharding:

- **One log shard per stream.** The log and its sequencer live in the database's default shard, so one stream is limited by one MariaDB primary and one sequencer, however many partitions it has. Spread heavy traffic over several streams.
- **Attached outboxes need a one-shard table group.** `ATTACHED TO GROUP OF t` fails with 39945 when `t`'s group has more than one shard.
- **Unattached streams** keep their outbox in the database's default shard, so a transaction that publishes and also writes rows on other shards spans more than one shard. Attach the stream to the table you write with it to keep such transactions on one server.
- **Moves.** When the tenant moves between clusters, the outbox, log and checkpoints move with their shard, and offsets continue.

## Settings

| Setting | Default | Scopes | Meaning |
|---|---|---|---|
| `stream.partitions_default` | 12 | database, tenant, global | Partitions of a new stream |
| `stream.retention_hours` | 168 | object, database, tenant, global | Time retention |
| `stream.retention_bytes` | 0 | object, database, tenant, global | Size retention per partition; 0 is none |
| `stream.max_message_bytes` | 1048576 | object, database, tenant, global | Largest record |
| `stream.compaction_lag_minutes` | 60 | object, database, tenant, global | Minimum age before compaction |
| `stream.tombstone_hours` | 24 | object, database, tenant, global | How long tombstones stay on compacted streams |
| `stream.change_topic` | false | object, database, tenant | Keep a change topic for the table |
| `stream.produce_wait_ms` | 2000 | tenant, global | How long a Kafka produce waits for offsets |
| `stream.descendant_publish` | off | object, database, tenant, global | Let descendants' own sessions publish with `IN TENANT` |
| `stream.descendant_key` | origin | object, database, tenant, global | `origin`: a descendant's key must be its tenant name; `any`: no check |
| `kafka.sasl_mechanisms` | `SCRAM-SHA-512,SCRAM-SHA-256,PLAIN` | dc, global | Offered SASL mechanisms |
| `kafka.fetch_max_wait_ms` | 500 | tenant, global | Cap on a fetch's wait |
| `kafka.group_initial_rebalance_delay_ms` | 3000 | dc, global | Delay of an empty group's first rebalance |
| `kafka.group_session_timeout_max_ms` | 300000 | dc, global | Largest session timeout a member may ask for |
| `kafka.advertised_host` | empty | dc, global | Host in Kafka Metadata; empty for the gateway's address |
| `listener.kafka.enabled` | false | dc, global | Serve the Kafka listener |
| `listener.kafka.bind` | `0.0.0.0:9092` | dc, global | Address and port |
| `listener.kafka.tls` | required | dc, global | `required`, `optional` or `off` |

See [Settings](../reference/settings.md) for bounds. The `sprig_stream_*` and `sprig_kafka_*` metrics, such as `sprig_stream_sequencing_lag_seconds` and `sprig_kafka_produce_wait_seconds`, are listed in [Metrics](../reference/metrics.md).

## Errors

| Code | Name | When |
|---|---|---|
| 39940 | `ER_SPRIG_STREAM_NOT_FOUND` | Unknown stream or topic |
| 39941 | `ER_SPRIG_STREAM_APPEND_ONLY` | `UPDATE`, `DELETE`, `REPLACE`, `TRUNCATE` or `ON DUPLICATE KEY UPDATE` on a stream; a publish into a change topic |
| 39942 | `ER_SPRIG_STREAM_EXISTS` | The name is in use by a stream or table |
| 39943 | `ER_SPRIG_STREAM_PARTITIONS_DECREASE` | Fewer partitions than the stream has |
| 39944 | `ER_SPRIG_STREAM_RECORD_TOO_LARGE` | Above `stream.max_message_bytes`, or a key above 4,096 bytes |
| 39945 | `ER_SPRIG_STREAM_KEY_NOT_ATTACHED` | The attached table is in another database, or its group has more than one shard |
| 39946 | `ER_SPRIG_STREAM_SEQUENCING_TIMEOUT` | A Kafka produce's offsets weren't assigned in time; retry |
| 39947 | `ER_SPRIG_STREAM_OFFSET_OUT_OF_RANGE` | An offset below the log start or above the high watermark |
| 39949 | `ER_SPRIG_STREAM_RESIDENCY` | A cross-tenant publish refused by residency |
| 39955 | `ER_SPRIG_STREAM_COMPACT_NEEDS_KEY` | A keyless record on a compacted stream |
| 39957 | `ER_SPRIG_STREAM_ORIGIN_KEY` | A descendant's key isn't its own tenant name |
| 39958 | `ER_SPRIG_STREAM_OFFSET_TXN_UNSUPPORTED` | `sprig.commit_offset` in a transaction on another cluster than the catalog |

Kafka clients receive Kafka's own error codes; where a response has a message field, it carries the SprigDB code and text. See [Errors](../reference/errors.md).

## Not supported

- **Kafka transactions.** The transaction APIs aren't advertised, and `transactional.id` is refused (39951). For atomic consume-and-write, use `sprig.commit_offset` inside a SQL transaction; for atomic publish-and-write, publish with `INSERT INTO` inside the transaction.
- **`acks=0`** (39950). SprigDB always acknowledges after the commit.
- **The KIP-848 consumer group protocol** (`group.protocol=consumer`). Use the classic protocol, the default in current clients.
- **Kafka quotas**, delegation tokens and SASL channel binding.
- **Unlimited time retention** (`retention.ms = -1`). The longest is 87,600 hours.
- **Change topics for other models.** Only SQL tables have change topics; collections, key-value namespaces, Redis structures and graphs don't yet. A new change topic starts from the moment it is turned on, without a snapshot of existing rows.
- **Streams over other protocols.** Streams can't be read or written over AMQP (stream queues), MongoDB or Redis. Use SQL, REST, logic or Kafka.
- **More than one log shard per stream**, so one stream doesn't spread its log across clusters.

## Related

- [Queues](queues.md): work that one consumer should do once
- [Sinks](sinks.md): deliver a stream to an external system
- [Logic](logic.md): triggers on streams, `db.publish` and `db.commitOffset`
- [Kafka](../connect/kafka.md): connecting Kafka clients
- [Streams, queues and sinks in SQL](../sql/streams-queues-sinks.md): statement reference
