---
title: Data models
audience: [user, dba]
sidebar:
  order: 1
---

# Data models

SprigDB holds tables, documents, key-value data, vectors, search indexes, graphs, geospatial data, analytic models, event streams and queues in one store, for one tenant, under one set of logins and privileges. This page shows which protocol reaches which model, and helps you choose a model for a job.

## One store for every model

Every model is rows in MariaDB. SprigDB doesn't run a separate document store, cache or message broker beside it:

| Model | What MariaDB holds |
|---|---|
| [Tables](../sql/overview.md) | Your tables, as you created them |
| [Documents](documents.md) | One table per collection: `_id`, the document as `doc` (JSON), and `_types`, which keeps the BSON types JSON can't express |
| [Key-value](key-value.md) | One key table per namespace, plus element tables for hashes, lists, sets and sorted sets |
| [Vectors](vectors.md) | `VECTOR(n)` columns with MariaDB's HNSW `VECTOR INDEX`; standalone vector indexes are tables too |
| [Full-text search](search.md) | The source rows stay in MariaDB; the search index is kept outside it and can always be rebuilt from it |
| [Graphs](graphs.md) | Node and relationship tables, or a mapping over your existing tables |
| [Geospatial](geospatial.md) | MariaDB geometry columns and `SPATIAL` indexes, GeoJSON inside documents, or sorted sets for Redis GEO |
| [Analytics](analytics.md) | Nothing extra: a model describes your existing tables, and queries run on them |
| [Event streams](streams.md) | An outbox and a partitioned log per stream |
| [Queues](queues.md) | Messages, queue state, an outbox and dead letters |
| [Sinks](sinks.md) | Connections, sink definitions, progress and dead letters in the tenant's catalog |
| [Logic](logic.md) | Routines compiled into MariaDB, or source kept in the catalog for the gateway's engine |
| [Data inheritance](inheritance.md) | Each tenant's own rows, plus merged views of what it inherits |

What follows from this:

- **One copy, one backup, one failover.** Every model is replicated, backed up and failed over with the rest of the tenant's data. See [How SprigDB works](../concepts/overview.md).
- **Models share shards.** Collections, key-value namespaces and tables belong to [table groups](../concepts/shards-and-table-groups.md) like any table. Objects in one group with the same distribution key live on the same shard, so they join and commit together inside one MariaDB server.
- **Models share privileges.** MongoDB commands, Redis commands and REST calls run as SQL statements through the caller's own session, so the same grants decide what a user can do from every protocol. [Row-level security](../security/row-level-security.md) policies on a collection or namespace apply to its MongoDB, Redis and REST reads as well as to SQL.
- **Models meet in one statement.** SQL can join tables with collections, key-value data, search results, graph patterns and stream records. See [Cross-model queries](cross-model-queries.md).

## Which protocol reaches which model

Each protocol has its own listener. MySQL (3306), MariaDB (3307) and HTTP (8080, 8443) are on by default; turn on the others with their `listener.<name>.enabled` setting. See [Connect](../connect/overview.md).

| Model | SQL (3306, 3307) | MongoDB (27017) | Redis (6379) | Bolt (7687) | Kafka (9092) | AMQP (5672, 5671) | REST and gRPC (8080, 8443) |
|---|---|---|---|---|---|---|---|
| Tables | Read and write | No | No | Through a mapped graph | Change topics | Change exchanges | `sql:execute`, `sql:query` |
| Documents | Read (JSON functions) | Read and write | No | No | No | No | MongoDB commands with `"language": "mongo"` |
| Key-value | Read (key tables and views) | No | Read and write (Redis databases) | No | No | No | `KeyValue` API (namespaces) |
| Vectors | `VECTOR` columns, `VEC_DISTANCE_*` | `$vectorSearch` | No | No | No | No | `Vectors` API |
| Full-text search | `MATCH ... AGAINST`, `sprig_search()` | `$text`, `$search`, `$searchMeta` | No | No | No | No | `Search.Query` |
| Graphs | `CREATE GRAPH`, `GRAPH_TABLE` | No | No | Cypher | No | No | `"language": "cypher"` or `"gql"` |
| Geospatial | Spatial types and functions | `2dsphere`, `$near`, `$geoWithin`, `$geoIntersects`, `$geoNear` | `GEO*` commands | No | No | No | `KeyValue.GeoSearch` |
| Analytics | Analytic reads across shards | No | No | No | No | No | `Analytics` API |
| Event streams | `CREATE STREAM`, publish with `INSERT`, read with `SELECT` | No | No | No | Produce and consume | No | Through `sql:execute`; `Streams.ListGroups` |
| Queues | `sprig.mq_*` calls | No | No | No | No | Publish and consume | Through `sql:execute`; `Mq.GetTopology` |
| Sinks | `CREATE CONNECTION`, `CREATE SINK` | No | No | No | No | No | Tests and dead letters |
| Logic | `CREATE FUNCTION`, `CALL`, triggers, schedules | `sprigCall` | `FCALL` | No | No | No | `Functions.Call` |
| Data inheritance | `ALTER TABLE ... SPRIG INHERITABLE`, shares | No | No | No | No | No | Through `sql:execute` |

"No" means the protocol doesn't reach the model: for example, a MongoDB client can't read a SQL table as a collection, and a Redis client sees only the Redis databases, not REST namespaces. Studio, the browser workspace, is served by the HTTP listener. See [Studio](../tools/studio.md).

## Names across protocols

A database is the same object from every protocol. `shop` in a MySQL connection, in a MongoDB connection string and in the REST path `/v1/databases/shop/...` is one database, and its collections, namespaces, streams and graphs all belong to it.

Two models have a fixed home:

- **Redis databases** live in the tenant's database `redis` (the setting `redis.logical_database`): `SELECT 0` uses the namespace `redis.db0`.
- **AMQP virtual hosts** are databases: the virtual host `/` is the database named by `amqp.default_vhost_database` (`mq` by default), and every other virtual host is the database of the same name.

Names are per tenant. Two tenants can both have a database `shop` and a Redis key `session:42`, and never see each other's. See [Tenants and databases](../concepts/tenants-and-databases.md).

## Choose a model

Start with the protocol your application or library already speaks: SprigDB accepts it as it is. When you're designing something new, choose by what you need to do:

| You need to | Use | Why |
|---|---|---|
| Keep records with a fixed shape, with constraints and joins | [Tables](../sql/overview.md) | MariaDB's full SQL, foreign keys and transactions |
| Keep records whose shape varies, from MongoDB drivers | [Documents](documents.md) | Fields can differ per document; queries, aggregation and indexes on any field |
| Read and write values by key, fast, with expiry | [Key-value](key-value.md) | One primary-key lookup per command; Redis data types and TTLs |
| Find items similar to a vector (semantic search, recommendations) | [Vectors](vectors.md) | HNSW indexes, with filters and exact re-ranking |
| Find text by words, ranked by relevance, with facets and highlights | [Full-text search](search.md) | Analyzers, scoring and facets that SQL `LIKE` can't give |
| Follow relationships of any depth (friends of friends, supply chains) | [Graphs](graphs.md) | Cypher patterns and variable-length paths |
| Find things near a place, or inside an area | [Geospatial](geospatial.md) | Spatial indexes and distance on a sphere |
| Report totals by region and month across many shards | [Analytics](analytics.md) | Facts, dimensions and measures, with subtotals, computed exactly across shards |
| Record events that many readers replay at their own pace | [Event streams](streams.md) | A Kafka-compatible log with partitions, offsets and retention |
| Hand each piece of work to exactly one worker | [Queues](queues.md) | Exchanges, acknowledgements, retries and dead letters |
| Send every change or event to another system | [Sinks](sinks.md) | Webhooks, Kafka, S3 and databases, with retries and no code to run |
| Validate, compute or react to data inside the database | [Logic](logic.md) | Functions, validations, triggers and schedules in KIRun, JavaScript, Lua or SQL |
| Give child tenants a parent's data, with their own changes on top | [Data inheritance](inheritance.md) | Shared rows, overrides and merged reads |

Some guidance when two models could do the job:

- **Tables or documents.** If you query from SQL most of the time, use a table: a `JSON` column covers fields that vary. If your code is written for MongoDB drivers, or documents nest deeply and change shape often, use a collection. SQL can still read it.
- **Key-value or a table.** Use key-value for data you always fetch by key, such as sessions, carts and counters, especially with expiry. Use a table when you filter or join on the values.
- **Streams or queues.** A stream keeps every record for its retention period, and each consumer group reads all of them. A queue delivers each message to one consumer and deletes it once acknowledged. Use a stream for facts that happened, a queue for work to do.
- **Search or `LIKE`.** `LIKE '%shoe%'` can't use an index and doesn't rank. A search index finds words and their stems, ranks results and stays fast as the table grows, at the cost of a short lag behind writes.
- **Graphs or SQL joins.** A fixed number of joins is plain SQL. Use a graph when the depth varies, as in "everyone within four hops".

## Related

- [How SprigDB works](../concepts/overview.md)
- [Shards and table groups](../concepts/shards-and-table-groups.md)
- [Cross-model queries](cross-model-queries.md)
- [Connect](../connect/overview.md)
