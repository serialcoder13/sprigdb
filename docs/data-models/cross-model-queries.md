---
title: Cross-model queries
audience: [user, dba]
sidebar:
  order: 14
---

# Cross-model queries

Because every model in SprigDB is rows in MariaDB, one SQL statement can join tables with collections, key-value data, vectors, search results, graph patterns and stream records, and one transaction can change a table, publish an event and acknowledge a queue message together. This page shows how, how such statements run when data is spread across shards, and how values look when they cross from one model to another.

## What can meet in one SQL statement

| Model | How it appears in SQL | Example |
|---|---|---|
| [Tables](../sql/overview.md) | Itself | `orders` |
| [Documents](documents.md) | A table with `_id`, `doc` (JSON) and `_types`; read fields with `JSON_VALUE` | `JSON_VALUE(e.doc, '$.type')` |
| [Key-value](key-value.md) | A key table per namespace, and the views `<ns>__hash`, `<ns>__list`, `<ns>__set`, `<ns>__zset` | `redis.db0`, `redis.db0__hash` |
| [Vectors](vectors.md) | `VECTOR(n)` columns and `VEC_DISTANCE_*` functions | `ORDER BY VEC_DISTANCE_COSINE(embedding, ...)` |
| [Full-text search](search.md) | `MATCH ... AGAINST`, and the table function `sprig_search()` | `FROM sprig_search('product_text', 'running shoe') AS s` |
| [Graphs](graphs.md) | `GRAPH_TABLE (...)`, a derived table of a graph pattern's rows | `FROM GRAPH_TABLE (shop_graph MATCH ... COLUMNS (...)) AS g` |
| [Geospatial](geospatial.md) | Geometry columns and spatial functions | `ST_Distance_Sphere(location, ...)` |
| [Event streams](streams.md) | A stream reads like a table with `partition_no`, `offset_no`, `ts`, `key`, `value` and `headers` | `FROM order_events` |
| [Queues](queues.md) | Not a table: the `CALL sprig.mq_*` statements | `CALL sprig.mq_publish(...)` |

[Analytic models](analytics.md) are queried through the Analytics API rather than in SQL, and [sinks](sinks.md) and [logic](logic.md) are definitions rather than data, so they don't appear in this table.

## Tables with collections

A collection is a table, so SQL joins it to your tables like any other. Read a document's fields with `JSON_VALUE` and compare them with your columns:

```sql
SELECT c.name,
       JSON_VALUE(e.doc, '$.type') AS event,
       JSON_VALUE(e.doc, '$.at')   AS at
FROM customers c
JOIN events e ON JSON_VALUE(e.doc, '$.customer_id') = c.id
WHERE c.id = 42
ORDER BY at;
```

The `events` collection is created and written by a MongoDB client; SQL reads it. The rules:

- **SQL rules apply.** A SQL statement compares with MariaDB's semantics, not MongoDB's. `JSON_VALUE` returns text, and MariaDB converts it when you compare it with a number. `JSON_VALUE` on an array or an object returns `NULL`.
- **Types come from JSON.** A date written by a MongoDB driver is an ISO 8601 string in `doc`, and a Decimal128 is a JSON number with its exact digits. When the BSON type matters, read it from `_types`: `JSON_VALUE(e._types, '$.at')` is `'date'`. See [Types and the type map](documents.md#types-and-the-type-map).
- **Read, don't write.** Write documents through MongoDB or the REST API. SQL `INSERT`, `UPDATE` and `DELETE` on a collection's table skip `_id` encoding, the type map and validators.
- **Privileges and row-level security** are the table's: a [row policy](../security/row-level-security.md) on the collection filters SQL reads too.

### `$lookup` from MongoDB

In the other direction, MongoDB's `$lookup`, `$graphLookup` and `$unionWith` read collections only. `from` names a collection of the same database; a SQL table is not a collection, so a pipeline can't join one. To combine a collection with a table, write the join in SQL, as above, or call a [logic function](logic.md) from MongoDB with `sprigCall` and let it run the SQL.

`$lookup` with `localField` and `foreignField` reads the foreign collection in batches of 500 local documents with an `$in` list and joins them by hash. Its output counts against `doc.agg_stage_memory_bytes`; past it, the stage fails with MongoDB's error 292 `QueryExceededMemoryLimitNoDiskUseAllowed` unless the command allows disk use. See [Documents](documents.md#aggregate).

## Tables with key-value data

A key-value namespace is a key table plus element tables. Redis databases are namespaces in the tenant's database `redis`. Join a key's value, or a hash's fields, with your tables:

```sql
-- The customer named by each session, from Redis string keys holding JSON
SELECT s.k AS session_key, c.name
FROM redis.db0 s
JOIN shop.customers c ON c.id = JSON_VALUE(CAST(s.v AS CHAR), '$.customer_id')
WHERE s.k LIKE 'session:%'
  AND (s.expires_at IS NULL OR s.expires_at > UTC_TIMESTAMP(3));

-- Every field of one hash, beside the customer's orders
SELECT h.field, h.value, o.order_id, o.status
FROM redis.db0__hash h
JOIN shop.orders o ON o.customer_id = 7
WHERE h.k = 'customer:7';
```

- Keys and values are binary (`VARBINARY` and `LONGBLOB`). Use `CAST(v AS CHAR)` to read a value as text, and `JSON_VALUE(CAST(v AS CHAR), '$.path')` when it holds JSON.
- The key table shows expired keys until the expiry sweeper removes them, so filter on `expires_at` as above. The `__hash`, `__list`, `__set` and `__zset` views show live keys only.
- Write through Redis or the `KeyValue` API, so versions, element counts and expiry stay consistent.

See [Key-value](key-value.md#read-it-from-sql) for the columns, and for the automatic indexes SprigDB adds when SQL filters on hash fields or JSON values.

## Tables with vectors

A vector is a column, so similarity search and ordinary filters and joins go in one statement. The examples on the rest of this page run in order in one database.

```sql test
CREATE TABLE customers (
  id     INT PRIMARY KEY,
  name   VARCHAR(40) NOT NULL,
  region CHAR(2) NOT NULL
);

CREATE TABLE products (
  id        INT PRIMARY KEY,
  name      VARCHAR(60) NOT NULL,
  embedding VECTOR(3) NOT NULL,
  VECTOR INDEX (embedding) DISTANCE=cosine
);

CREATE TABLE orders (
  order_id    BIGINT PRIMARY KEY,
  customer_id INT NOT NULL,
  product_id  INT NOT NULL,
  status      VARCHAR(16) NOT NULL
);

INSERT INTO customers VALUES (1, 'Asha', 'IN'), (2, 'Ben', 'GB'), (3, 'Chen', 'SG');

INSERT INTO products VALUES
  (1, 'Trail running shoe', VEC_FromText('[1, 0, 0]')),
  (2, 'Road running shoe',  VEC_FromText('[0.6, 0.8, 0]')),
  (3, 'Rain jacket',        VEC_FromText('[0, 0, 1]'));

INSERT INTO orders VALUES
  (101, 1, 1, 'paid'),
  (102, 2, 3, 'paid'),
  (103, 3, 2, 'open');
```

The customers who ordered something close to the trail shoe, with their orders:

```sql test
SELECT c.name, p.name AS product, o.status
FROM orders o
JOIN customers c ON c.id = o.customer_id
JOIN products p  ON p.id = o.product_id
WHERE VEC_DISTANCE_COSINE(p.embedding, VEC_FromText('[1, 0, 0]')) < 0.5
ORDER BY o.order_id;
```

```text output
name	product	status
Asha	Trail running shoe	paid
Chen	Road running shoe	open
```

A distance used as a filter, or a sort with a second key, is computed exactly for every candidate row. Only `ORDER BY VEC_DISTANCE_<metric>(column, constant) LIMIT n` uses the HNSW index. See [Vectors](vectors.md#sql-tables).

From MongoDB, `$vectorSearch` searches a collection's vectors and returns documents, which later pipeline stages can filter, project and `$lookup` into other collections. The REST `Vectors` API searches standalone vector indexes. See [Vectors](vectors.md).

## Tables with search results

`MATCH ... AGAINST` works on tables with a search index, and `sprig_search()` returns a search's hits as rows to join with anything. It returns the object's key columns (the primary key for a table, `_id` for a collection, `k` for a namespace), `score`, `highlights` and `_shard`:

```sql
SELECT p.id, p.name, s.score, s.highlights
FROM sprig_search('product_text', 'red "running shoe" -kids',
                  JSON_OBJECT('highlight', JSON_ARRAY('name'), 'limit', 20)) AS s
JOIN products p ON p.id = s.id
ORDER BY s.score DESC;
```

Because a collection's hits come back with `_id`, you can join them with the collection's own table, or with your tables through a field of the document. SprigDB runs the search first and then the statement, so privileges, row-level security and sharding apply to the rest of the statement unchanged. Search indexes follow writes through the change feed, a little behind them; the `consistency` option chooses how fresh a search must be. See [Full-text search](search.md#freshness-and-lag).

## Tables with graphs

`GRAPH_TABLE` runs a graph pattern inside a SQL statement and returns its rows as a derived table. The graph can be a native graph written over Bolt, or a graph mapped over your own tables:

```sql
SELECT g.name, c.region
FROM GRAPH_TABLE (shop_graph
       MATCH (a IS Person)-[IS BOUGHT]->(p IS Product)
       WHERE p.name = 'Pen'
       COLUMNS (a.id AS id, a.name AS name)) AS g
JOIN customers c ON c.id = g.id;
```

The graph calls run first; then the statement, with their rows as derived tables, is planned like any other. Filters on the derived table are pushed into the Cypher where they can be. See [Graphs](graphs.md#query-a-graph-from-sql-with-graph_table).

## Tables with stream records

A stream reads like a table, so its records can be joined with the rows they describe. Create a stream next to the orders:

```sql test
CREATE STREAM order_events KEY (order_id) ATTACHED TO GROUP OF orders PARTITIONS 4;
```

Read the stream's records beside the orders they name:

```sql test
SELECT e.partition_no, e.offset_no, o.order_id, o.status
FROM order_events e
JOIN orders o ON o.order_id = JSON_VALUE(CAST(e.value AS CHAR), '$.order_id')
ORDER BY e.partition_no, e.offset_no
LIMIT 100;
```

A SQL read of a stream is a snapshot of the records sequenced so far. Records appear a few tens of milliseconds after the transaction that published them commits. `key` and `value` are binary; `CAST(value AS CHAR)` reads a value as text. See [Event streams](streams.md#read-with-sql).

## Transactions across models

A SQL transaction can change tables, publish to streams and queues, commit consumer offsets and settle queue messages, and all of it commits or rolls back together:

```sql test
START TRANSACTION;
UPDATE orders SET status = 'paid' WHERE order_id = 103;
INSERT INTO order_events (key, value)
  VALUES (103, JSON_OBJECT('type', 'order.paid', 'order_id', 103));
CALL sprig.mq_publish('', 'billing', JSON_OBJECT('order_id', 103), NULL, 0, DATABASE());
COMMIT;
```

```text output
routed
0
```

Here the queue call reaches no queue (`routed` is 0), because no queue `billing` has been declared; with one, the message would be delivered once the transaction commits. What each model contributes to a transaction:

| Model | In a SQL transaction | How |
|---|---|---|
| Tables | Yes | Ordinary writes |
| Event streams | Yes | `INSERT INTO stream ...` writes the stream's outbox in your transaction; a rollback publishes nothing |
| Consumer offsets | Yes | `CALL sprig.commit_offset(group, stream, partition, offset)` commits a group's offset with your writes |
| Queues | Yes | `CALL sprig.mq_publish(...)` writes the virtual host's outbox; `CALL sprig.mq_ack(token)` deletes the message in your transaction |
| Logic | Yes | A function or procedure called inside a transaction joins it; its `db` calls run in the same transaction |
| Documents | No | Write collections in MongoDB transactions |
| Key-value | No | Write keys through Redis or the `KeyValue` API |

The same applies to a REST transaction (`POST /v1/databases/{database}/transactions`, then each statement with its handle) and to [logic](logic.md) code, whose `db.exec`, `db.publish`, `db.commitOffset` and `db.mq.*` calls run in the caller's transaction.

Each other protocol has its own transactions over its own model:

- **MongoDB** multi-document transactions cover collections. See [Documents](documents.md#transactions).
- **Redis** `MULTI` and `EXEC` cover keys. Keep keys you change together under one hash tag so they share a shard. See [Key-value](key-value.md#transactions).
- **Bolt** transactions cover a graph's nodes and relationships. See [Graphs](graphs.md#transactions-and-isolation).
- **AMQP** channel transactions (`tx.select`) aren't one atomic MariaDB transaction across queues. Use SQL for atomic publish and acknowledge. See [Queues](queues.md#transactions-on-a-channel).

No single transaction spans two protocols: a MongoDB transaction can't include a SQL statement, and a SQL transaction can't include a MongoDB write.

### Transactions across shards

A transaction whose writes all land on one MariaDB cluster commits as one local MariaDB transaction, even when it touches several shard schemas there. One that writes on several clusters commits with two-phase commit, up to `txn.max_branches` (32) clusters. To keep a transaction on one server, keep what it writes in one table group: attach a stream to the table it's published with (`ATTACHED TO GROUP OF orders`), and keep the keys of one customer under one hash tag. If a shard moves while a transaction is open, the transaction rolls back with `ER_SPRIG_SHARD_MAP_CHANGED` (39424, SQLSTATE 40001); retry it as you would after a deadlock.

## How a cross-model statement runs

Every model's rows are MariaDB tables in [table groups](../concepts/shards-and-table-groups.md), so the router plans a statement over collections, namespaces and streams exactly as it plans one over tables:

| Situation | Plan | What happens |
|---|---|---|
| Every object the statement reads has one shard, or the statement's key filters narrow each to one shard, all on one cluster | Pass | MariaDB runs the whole statement; the gateway relays the answer |
| The objects are in one table group and the join compares their distribution keys | Read | Each shard runs the statement on its own rows; the gateway merges, sorts and finishes aggregates |
| Two objects of different groups, joined by exactly one equality between two columns, with no aggregates | Lookup join | The gateway reads one side, then the other with batched `IN` lists of `router.lookup_batch_size` (256) keys |
| Any other statement: joins on expressions such as `JSON_VALUE`, several groups, subqueries, window functions | Gathered read | The gateway copies the rows the statement needs into temporary tables on one cluster, and MariaDB runs the statement there |
| A gathered read that would copy more than `router.gather.max_rows` (100,000) rows | Analytic read, or an error | With `router.analytics.mode = fallback` (the default), the gateway's analytic engine runs it if it can; otherwise the statement fails with `ER_SPRIG_PLAN_LIMIT_EXCEEDED` (39421) |
| No exact plan | Reject | `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` (39420), with a reason code |

A new tenant has one shard per table group, so every cross-model statement starts as a single-shard statement that MariaDB runs on its own. The other plans matter once a group splits. `EXPLAIN FORMAT=SPRIG` shows the plan a statement gets. See [Query plans](../performance/query-plans.md).

To keep cross-model joins on one shard:

- **Share the distribution key.** A customer's orders, events and sessions on the same shard join inside MariaDB. The [sharding advisor](../performance/sharding-advisor.md) groups objects that are joined on their keys.
- **Compare like with like.** A join co-locates only when both sides compare as the same kind of value: an integer key with an integer, a string key with a string of the same collation. An integer column compared with a string, such as a Redis key `customer:7`, is a join across shards.
- **Filter on the key.** `WHERE c.id = 42` narrows every co-located object to one shard.

## Values across models

Each model keeps its values in MariaDB types. When a value crosses into another model or protocol, it changes form as follows.

### Documents

| BSON | In `doc` (what SQL reads) | `_types` entry |
|---|---|---|
| string, int32, boolean, null, object, array | Itself | none |
| int64 | An integer | `long` when it fits in 32 bits |
| double | A number with a fraction or exponent | `double` for NaN and ±Infinity only |
| Decimal128 | A number with its exact digits (`59.90`) | `decimal` |
| ObjectId | 24 lower-case hex digits | `objectId` |
| date | `"2026-11-07T10:15:00.000Z"` | `date` |
| UUID (binary subtype 4) | The UUID's text | `uuid` |
| other binary | Base64 | `binData:<subtype>` |

The full list, including timestamps, regular expressions and MinKey and MaxKey, is in [Documents](documents.md#types-and-the-type-map). Over REST, MongoDB commands return documents as relaxed Extended JSON, or canonical Extended JSON with `"output": "canonical_ejson"`, which keeps int32, int64 and double apart.

### Key-value

Redis values are bytes, stored as they are. SQL reads keys and values as binary; JSON values written through the `KeyValue` API are readable with `JSON_VALUE`. Sorted-set scores are `DOUBLE`, and the `__zset` view gives `-inf` and `+inf` as a `NULL` score with `score_class` `-1` or `1`.

### Vectors

A vector is an array of 32-bit floats. In SQL it is MariaDB's `VECTOR(n)`: `VEC_FromText('[...]')` writes one and `VEC_ToText(v)` reads it as text. On the MySQL persona (port 3306), which has no vector type, a `VECTOR` column travels as binary: `4 × n` bytes, one little-endian float per dimension. In a document, a vector is an array of numbers.

### Stream records

`key` and `value` are binary and `headers` is a JSON object. A SQL publish stores the value expression's bytes, so `JSON_OBJECT(...)` publishes JSON text. In change topics, each row is a JSON envelope in which:

- `DECIMAL` values are JSON numbers, so trailing zeros are lost (`49.90` becomes `49.9`);
- binary and spatial values are base64;
- dates and times are MariaDB's text.

See [Event streams](streams.md#streams-fed-from-table-changes).

### Logic

Inside logic code, `db` calls return `JSON` columns parsed, `DECIMAL` values as numbers and dates as text. See [Logic](logic.md#what-code-can-use).

## Not supported

- **`$lookup` into SQL tables.** MongoDB pipelines read collections only; join collections with tables in SQL.
- **Atomic writes to collections or keys from SQL.** SQL can read collections and namespaces, but documents are written through MongoDB or REST and keys through Redis or the `KeyValue` API, each in its own transactions.
- **Transactions spanning protocols.** A MongoDB, Redis or Bolt transaction can't include a SQL statement, and the reverse.
- **Analytic models in SQL.** Models are queried through the Analytics API. Plain SQL aggregates work across shards without a model.
- **Inheritable collections and namespaces.** Only SQL tables can be inheritable. See [Data inheritance](inheritance.md).

## Related

- [Data models](overview.md)
- [Documents](documents.md)
- [Key-value](key-value.md)
- [Shards and table groups](../concepts/shards-and-table-groups.md)
- [Query plans](../performance/query-plans.md)
