---
title: Vectors
audience: [user, dba]
sidebar:
  order: 4
---

# Vectors

SprigDB stores embeddings in MariaDB `VECTOR` columns and finds the nearest ones with MariaDB's vector index, an HNSW graph. You reach vectors in three ways: as columns of SQL tables, as fields of MongoDB documents searched with `$vectorSearch`, and as standalone vector indexes managed through the Vectors API over REST and gRPC.

SprigDB doesn't compute embeddings. Your application creates them with the model of its choice and stores them as arrays of 32-bit floats.

## How vectors are stored

Every form of vector is MariaDB rows:

| Where the vector lives | What MariaDB holds | Index |
|---|---|---|
| A column of a SQL table | Your table, with a `VECTOR(n)` column | MariaDB's `VECTOR INDEX` on the column, if you create one |
| A field of a MongoDB document | The collection's table; the vector is a numeric array inside the document's JSON | A hidden side table with a `VECTOR INDEX`, once the collection holds 1,000 vectors |
| A standalone vector index (Vectors API) | One table per index, named after the index: `id`, `v VECTOR(n)`, and the item's metadata stored as a document | MariaDB's `VECTOR INDEX`, built at creation, automatically once the index is large enough, or never |

Vectors are 32-bit floats. MariaDB computes two distances:

| Distance | MariaDB function | Value |
|---|---|---|
| Euclidean | `VEC_DISTANCE_EUCLIDEAN` | The straight-line distance |
| Cosine | `VEC_DISTANCE_COSINE` | 1 − cosine similarity, from 0 (same direction) to 2 (opposite) |

The MongoDB and Vectors API forms also accept **`dotProduct`**, as MongoDB Atlas does. A dot product of unit vectors orders results the same way as cosine, so SprigDB searches a `dotProduct` index with the cosine distance. The Vectors API stores each vector scaled to length 1, and refuses a zero vector, which has no direction.

### Approximate and exact search

An **exact** search computes the distance to every candidate row and returns the true nearest neighbours. An **approximate** search walks the HNSW graph and reads only a bounded set of candidates, so it is much faster on large data but can miss a neighbour. The share of the true top k that an approximate search finds is its *recall*.

SprigDB never returns a wrong result from an approximate search: candidates read from a graph are re-ranked by their exact distance, and filters are rechecked on the stored data. What an approximate search can do is miss a row, which lowers recall.

The graph has two parameters:

| Parameter | Where you set it | Meaning |
|---|---|---|
| `M` | `VECTOR INDEX ... M=16`, or `m` in the Vectors API | Links per node in the graph, 3 to 200. Larger values give better recall and a larger graph. |
| `ef` | MariaDB's `mhnsw_ef_search` session variable, or `numCandidates` and `num_candidates` | Candidates read per search. Larger values give better recall and slower searches. |

## SQL tables

`VECTOR(n)`, `VECTOR INDEX`, the `VEC_*` functions and `IGNORE INDEX` pass through both SQL personas unchanged, on port 3306 and port 3307. You write MariaDB's vector SQL as you would against MariaDB.

```sql test
CREATE TABLE products (
  id        BIGINT PRIMARY KEY,
  title     VARCHAR(200) NOT NULL,
  category  VARCHAR(40)  NOT NULL,
  embedding VECTOR(4)    NOT NULL,
  VECTOR INDEX (embedding) M=16 DISTANCE=cosine
);

INSERT INTO products VALUES
  (1, 'Trail running shoe', 'shoes', VEC_FromText('[0.12, 0.80, 0.05, 0.31]')),
  (2, 'Road running shoe',  'shoes', VEC_FromText('[0.10, 0.75, 0.10, 0.40]')),
  (3, 'Rain jacket',        'coats', VEC_FromText('[0.90, 0.05, 0.40, 0.02]'));

SELECT id, title
FROM products
WHERE category = 'shoes'
ORDER BY VEC_DISTANCE_COSINE(embedding, VEC_FromText('[0.11, 0.78, 0.07, 0.35]'))
LIMIT 10;
```

```text output
id	title
1	Trail running shoe
2	Road running shoe
```

A second sort key turns the graph off, so this search is exact:

```sql test
SELECT id
FROM products
ORDER BY VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FromText('[0.90, 0.05, 0.40, 0.02]')), id
LIMIT 1;
```

```text output
id
3
```

MariaDB's own rules apply:

- A `VECTOR` column holds up to 16,383 dimensions.
- A table has at most one `VECTOR INDEX`, and its column must be `NOT NULL`.
- `DISTANCE` is `euclidean` (the default) or `cosine`.
- The index serves only `ORDER BY VEC_DISTANCE_<metric>(column, constant) LIMIT n`, ascending, with the index's own metric. A second sort key, or the other metric, makes MariaDB compute exact distances instead.
- `SET SESSION mhnsw_ef_search = 200` raises the candidates read per search for your session.
- To force an exact search on an indexed table, add `IGNORE INDEX (<index name>)` after the table name.

On the MySQL persona (port 3306), MySQL 8.4 has no vector type, so a `VECTOR(n)` column travels in result sets as a binary `VARBINARY` value without a type name: `4 × n` bytes, one little-endian 32-bit float per dimension. Use `VEC_ToText(embedding)` when you want the text form.

### Row-level security

A [row policy](../security/row-level-security.md) on a vector-indexed table is part of the search's `WHERE`, so it filters rows before the ranking. `LIMIT 5` returns the five nearest rows the user may see, not five rows of which some are then hidden.

### Automatic vector indexes

[Automatic indexing](../performance/automatic-indexing.md) watches for repeated `ORDER BY VEC_DISTANCE_*(column, ...) LIMIT n` queries on a `NOT NULL` vector column without an index. It then proposes a `VECTOR INDEX` with the queries' metric and `M=8` (or `M=16` on tables of a million rows or more). An HNSW index turns exact answers into approximate ones, so this is governed separately from B-tree indexes:

| Setting | Default | Meaning |
|---|---|---|
| `auto_index.vector_ann` | `recommend` | `on`, `approve`, `recommend` or `off` for vector indexes on SQL tables |
| `auto_index.vector_min_recall` | `0.95` | Recall at k a candidate index must reach, measured against exact answers, before it is accepted |

### Sharded tables

Each statement runs through your session and the router. When a table is split across shards, each shard keeps its own slice of rows and its own graph, answers `ORDER BY distance LIMIT n` on its own, and the gateway merges the results by distance. Every shard is asked for the full `n`, because the nearest rows may all sit on one shard. With exact search on every shard the merged answer is exact; with approximate search, recall across shards is about the recall of each shard's graph.

## MongoDB collections

MongoDB clients search vectors the way MongoDB Atlas does: create a search index of type `vectorSearch`, then use the `$vectorSearch` aggregation stage. See [MongoDB](../connect/mongodb.md) for connecting, and [Documents](documents.md) for how documents are stored.

### Create a vector index

```js
db.runCommand({
  createSearchIndexes: "events",
  indexes: [{
    name: "vx",
    type: "vectorSearch",
    definition: {
      fields: [
        { type: "vector", path: "embedding", numDimensions: 8, similarity: "cosine" },
        { type: "filter", path: "cat" }
      ]
    }
  }]
})
```

| Field | Meaning |
|---|---|
| `name` | The index name, unique per collection. Defaults to `default`. |
| `type` | `vectorSearch`. (`search` creates a [full-text index](search.md).) |
| `definition.fields` | Exactly one field of `type: "vector"`, and any number of `type: "filter"` fields |
| `path` | A dotted path in the document |
| `numDimensions` | A whole number from 1 to 8192 |
| `similarity` | `euclidean`, `cosine` or `dotProduct` |

A document is searchable when its `path` holds an array of numbers of exactly `numDimensions` elements. Other documents, including those without the field, are left out of vector searches.

The other index commands work as in Atlas:

| Command | Behaviour |
|---|---|
| `listSearchIndexes`, `$listSearchIndexes` | Lists vector and full-text indexes. A vector index reports `status: "READY"` and `queryable: true` at once. |
| `updateSearchIndex` | Replaces the definition. Fails with 27 `IndexNotFound` when the index doesn't exist. |
| `dropSearchIndex` | Drops the index. Dropping an index that doesn't exist succeeds. |

Creating an index whose name exists with the same definition succeeds and does nothing. With a different definition it fails with 68 `IndexKeySpecsConflict`. A malformed definition fails with 2 `BadValue`.

### Search with `$vectorSearch`

```js
db.events.aggregate([
  { $vectorSearch: {
      index: "vx",
      path: "embedding",
      queryVector: [0.12, -0.40, 0.88, 0.05, 0.31, -0.27, 0.66, 0.10],
      numCandidates: 100,
      limit: 10,
      filter: { cat: { $eq: 2 } }
  } },
  { $project: { _id: 1, score: { $meta: "vectorSearchScore" } } }
])
```

| Field | Required | Meaning |
|---|---|---|
| `index` | yes | The `vectorSearch` index. An index that doesn't exist returns no documents, as in Atlas. |
| `path` | yes | Must be the index's vector field. |
| `queryVector` | yes | An array of numbers, or a BSON vector (`BinData` subtype 9) of `float32` or `int8` values, with the index's dimensions. Packed-bit vectors are refused. |
| `limit` | yes | Documents to return; a positive whole number. |
| `numCandidates` | unless `exact` is true | Candidates read from the graph: at least `limit` and at most 10,000. |
| `exact` | no | `true` for an exact search. `numCandidates` isn't allowed with it. |
| `filter` | no | A MongoDB query on fields declared as `filter` fields in the index |

`$vectorSearch` must be the first stage of the pipeline. The rest of the pipeline runs as usual on the documents it returns.

`{ $meta: "vectorSearchScore" }` gives Atlas's score, from 0 to 1, higher is nearer:

| Similarity | Score from the distance `d` |
|---|---|
| `euclidean` | `1 / (1 + d)` |
| `cosine`, `dotProduct` | `1 − d / 2`, which equals `(1 + cosine similarity) / 2` |

### Exact and approximate search on collections

- **Exact** searches run inside MariaDB: the distance of every document whose path holds a valid vector, ordered and cut by `limit`. A collection with fewer than 1,000 vectors, or a search with `exact: true`, is always searched this way. When the filter translates exactly to SQL it is pushed into the statement; otherwise documents are read in pages and the filter is checked on each.
- **Approximate** searches start once the index covers 1,000 vectors. SprigDB keeps a hidden side table per vector index with MariaDB's HNSW index (`M=16`, cosine for `cosine` and `dotProduct`, euclidean for `euclidean`). The search reads `numCandidates` nearest ids from the graph, reads those documents, checks the filter, and ranks them by their exact distance. When the filter leaves fewer than `limit` documents, it reads four times as many candidates, up to 10,000.

The side table is brought up to date when it is searched, not on each write: before an approximate search, documents written since the last update are copied in, at most once a second per gateway, and once a minute rows of deleted documents are removed. Approximate results can therefore be about a second behind your writes, as Atlas Vector Search is eventually consistent. A stale row can only cost recall; it never returns a document that doesn't match, because each candidate is read again from the collection.

`updateSearchIndex` and `dropSearchIndex` remove the side table; the next search builds it again from the documents. Side tables move with their database when a tenant moves.

Documents whose encoded `_id` is longer than 254 bytes don't enter the graph (MariaDB limits a vector index's primary key), so only exact searches find them.

### Hybrid search with `$rankFusion` and `$scoreFusion`

To combine a vector search with a full-text search, use MongoDB 8's `$rankFusion` (reciprocal rank fusion) or `$scoreFusion`. Each input pipeline runs on its own, and the results are fused by `_id`:

```js
db.events.aggregate([
  { $rankFusion: {
      input: { pipelines: {
        semantic: [ { $vectorSearch: { index: "vx", path: "embedding",
                        queryVector: queryVec, numCandidates: 100, limit: 20 } } ],
        keywords: [ { $search: { index: "default",
                        text: { query: "late delivery", path: "notes" } } }, { $limit: 20 } ]
      } },
      combination: { weights: { semantic: 2, keywords: 1 } },
      scoreDetails: true
  } },
  { $project: { notes: 1, score: { $meta: "score" }, details: { $meta: "scoreDetails" } } }
])
```

`$rankFusion` scores each document as the sum of `weight / (60 + rank)` over the pipelines that return it. `$scoreFusion` normalises each pipeline's scores (`input.normalization`: `none`, `sigmoid` or `minMaxScaler`) and combines them with `combination.method` `avg` or an `expression`. See [Full-text search](search.md) for `$search`.

## Standalone vector indexes (Vectors API)

A standalone vector index holds items that aren't rows of one of your tables: each item has an id, a vector, optional metadata (a JSON document you can filter on) and, optionally, a location. You manage it over REST or gRPC with the `sprig.v1.Vectors` service. Every call runs in your own session, so your privileges apply. See [REST and gRPC](../connect/rest-and-grpc.md) for logins and tokens.

| Method | Path | Operation |
|---|---|---|
| POST | `/v1/databases/{database}/vectorIndexes` | `Vectors.CreateIndex` |
| GET | `/v1/databases/{database}/vectorIndexes/{index}` | `Vectors.GetIndex` |
| DELETE | `/v1/databases/{database}/vectorIndexes/{index}` | `Vectors.DropIndex` |
| POST | `/v1/databases/{database}/vectorIndexes/{index}:upsert` | `Vectors.Upsert` |
| POST | `/v1/databases/{database}/vectorIndexes/{index}:delete` | `Vectors.Delete` |
| POST | `/v1/databases/{database}/vectorIndexes/{index}:search` | `Vectors.Search` |

gRPC clients call the same operations on the same port; the messages have the same fields as the JSON bodies below.

### Create an index

```sh
curl -u 'acme.app:<password>' -H 'Content-Type: application/json' \
  -X POST http://localhost:8080/v1/databases/shop/vectorIndexes \
  -d '{"index": "product_embeddings", "dimensions": 4, "similarity": "cosine", "ann_mode": "auto", "geo": true}'
```

```json
{"database": "shop", "index": "product_embeddings", "dimensions": 4, "similarity": "cosine",
 "m": 16, "ann_mode": "auto", "ann_state": "none", "geo": true, "recall": 0.0, "rows": "0"}
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `index` | string | required | 1 to 64 letters, digits or underscores, not starting with `__sprig` |
| `dimensions` | integer | required | 1 to 16,383 |
| `similarity` | string | `euclidean` | `euclidean`, `cosine` or `dotProduct` |
| `m` | integer | `vector.default_m` (16) | The graph's `M`, kept between 3 and 200 |
| `ann_mode` | string | `auto` | When the graph exists: `auto`, `on` or `off` (see below) |
| `geo` | boolean | `false` | Items carry a location you can filter on |

The index is a table `<database>.<index>`. The database is created if it doesn't exist. A name already in use fails with MariaDB's error 1050. `GetIndex` returns the same fields, with `rows` counting the items and `recall` the recall measured when the graph was last validated.

### Add, replace and delete items

```sh
curl -u 'acme.app:<password>' -H 'Content-Type: application/json' \
  -X POST http://localhost:8080/v1/databases/shop/vectorIndexes/product_embeddings:upsert \
  -d '{"items": [
        {"id": "cDAwMQ==", "vector": [0.3, -0.2, 0.9, 0.1],
         "metadata": {"cat": 1, "name": "p1"},
         "location": {"latitude": 19.07, "longitude": 72.87}}
      ]}'
```

```json
{"upserted": "1"}
```

| Item field | Meaning |
|---|---|
| `id` | The item's id, base64-encoded bytes (1 to 254 bytes). An existing id is replaced. |
| `vector` | The vector, with the index's dimensions. NaN and infinities are refused. |
| `metadata` | A JSON object (MongoDB Extended JSON accepted), or the same as a JSON string. Stored as a document, so filters use MongoDB's rules. |
| `location` | `{"latitude": ..., "longitude": ...}` in degrees, for `geo` indexes |

Items are written 200 rows per statement. To delete items, send their ids:

```sh
curl -u 'acme.app:<password>' -H 'Content-Type: application/json' \
  -X POST http://localhost:8080/v1/databases/shop/vectorIndexes/product_embeddings:delete \
  -d '{"ids": ["cDAwMQ=="]}'
```

```json
{"deleted": "1"}
```

### Search

```sh
curl -u 'acme.app:<password>' -H 'Content-Type: application/json' \
  -X POST http://localhost:8080/v1/databases/shop/vectorIndexes/product_embeddings:search \
  -d '{"query": [0.3, -0.2, 0.9, 0.1], "k": 5, "filter": "{\"cat\": 1}",
       "geo": {"center": {"latitude": 19.07, "longitude": 72.87}, "radius_m": 10000}}'
```

```json
{"hits": [{"id": "cDAwMQ==", "distance": 0.0, "score": 1.0, "metadata": "{\"cat\":1,\"name\":\"p1\"}",
           "location": {"latitude": 19.07, "longitude": 72.87}}],
 "plan": "prefilter_exact"}
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `query` | array of numbers | required | The query vector |
| `k` | integer | required | Results wanted, 1 to `vector.max_k` (10,000) |
| `num_candidates` | integer | `vector.default_ef_search` (64) | The graph's `ef`: at least `k`, at most 10,000 |
| `exact` | boolean | `false` | Force an exact search |
| `filter` | string | none | A MongoDB query on the metadata, as a JSON string |
| `geo` | object | none | A circle (`center`, `radius_m`), a box (`south_west`, `north_east`) or a `polygon` (a list of points), each point `{"latitude", "longitude"}`. Only for `geo` indexes. |

Each hit has its `id` (base64), `distance`, `score` (the same formulas as `vectorSearchScore`), `metadata` as a JSON string, and `location` for `geo` indexes. `plan` says how the search ran.

### How a search is planned

| Plan | When | What runs |
|---|---|---|
| `exact` | No graph, or `exact: true`, and no filter | Exact distances over every item |
| `prefilter_exact` | No graph or `exact: true` with a filter; or a filter that leaves at most `vector.prefilter_max_rows` items | Exact distances over the items the filter selects |
| `ann` | A graph, no filter left to check | The graph with `ef = max(num_candidates, k)`, then the candidates re-ranked by exact distance |
| `ann_overfetch` | A graph and a filter that SQL can't check exactly | The graph, asking for more than `k` rows, then the filter checked on each |

The metadata filter is translated to SQL where it can be. Paths that might hold arrays make the SQL condition wider than the filter, so every row is checked against the filter again in the gateway. For `ann_overfetch`, SprigDB measures the share of rows that pass the filter on a sample of up to 1,000 rows and asks for `k × ceil(1.2 / share)` rows, at most `k × vector.max_overfetch`. If fewer than `k` rows survive, it doubles the limit and `ef` and asks again, up to the same cap, then returns what it has. Each retry counts in `sprig_vector_overfetch_retries_total`.

### When the graph is built

| `ann_mode` | Graph |
|---|---|
| `on` | Built when the index is created. |
| `off` | Never. Every search is exact. |
| `auto` | Built in the background once the index holds `vector.ann_min_rows` items, then validated: 20 of the index's own vectors are searched with and without the graph, and the graph is kept only if recall at 10 reaches `vector.min_recall`. Otherwise it is dropped and searches stay exact. |

Building or dropping a graph copies the table, because MariaDB can't add a vector index in place. While an `auto` graph is being built, searches stay exact. The check runs after upserts and searches on the gateway that served them.

`vector.auto_ann` controls the `auto` mode: `on` (the default) builds as described, `off` never builds, and `approve` or `recommend` send the build through [approvals](../concepts/approvals.md) first. While an approval is pending, the gateway asks again at most once a minute.

## Settings

The `vector.*` settings apply to the Vectors API. They are read on every call, so a change applies to the next search.

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `vector.default_m` | int | `16` | database, tenant, dc, global | `M` for new indexes that name none (3 to 200) |
| `vector.default_ef_search` | int | `64` | database, tenant, dc, global | `ef` when a search gives no `num_candidates` |
| `vector.max_k` | int | `10000` | tenant, dc, global | Largest `k` |
| `vector.prefilter_max_rows` | int | `10000` | database, tenant, dc, global | Items a filter may select and still be searched exactly |
| `vector.max_overfetch` | int | `16` | database, tenant, dc, global | Largest over-fetch multiple of `k` |
| `vector.ann_min_rows` | int | `20000` | database, tenant, dc, global | Items before an `auto` index gets its graph |
| `vector.min_recall` | float | `0.95` | database, tenant, dc, global | Recall at k to accept and keep an automatic graph |
| `vector.auto_ann` | text | `on` | object, database, tenant, cluster, dc, global | `on`, `approve`, `recommend` or `off` for automatic graphs |

A tenant or database value of `vector.max_k`, `vector.max_overfetch` or `vector.prefilter_max_rows` can't exceed the value at dc and global scope. For example, setting `vector.max_k` to 5 for tenant `acme` makes that tenant's next search with `k = 10` fail with 39738. See [Settings](../sql/settings.md) for how to set a value at each scope.

## Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `sprig_vector_searches_total` | counter | `plan` | Vectors API searches, by plan |
| `sprig_vector_search_duration_seconds` | histogram | `plan` | Their duration |
| `sprig_vector_overfetch_retries_total` | counter | none | Re-queries after a filter left fewer than `k` results |

## Errors

| Code | Name | When |
|---|---|---|
| 39735 | `ER_SPRIG_VECTOR_INDEX_NOT_FOUND` | No such standalone index (HTTP 404) |
| 39736 | `ER_SPRIG_VECTOR_DIMENSION_MISMATCH` | A vector or query vector of the wrong length, NaN or an infinity, or `dimensions` outside 1 to 16,383 |
| 39737 | `ER_SPRIG_VECTOR_METRIC_MISMATCH` | An unknown similarity, or a zero vector under `dotProduct` |
| 39738 | `ER_SPRIG_VECTOR_LIMIT_INVALID` | `k` or `limit` out of range, `numCandidates` below the limit or over 10,000, or `numCandidates` missing without `exact` |
| 39739 | `ER_SPRIG_VECTOR_FILTER_NOT_INDEXED` | A filter on a path not declared as a `filter` field (`$vectorSearch`), a filter that isn't a valid JSON object, a `geo` filter on an index without locations, or a request field that isn't offered (`type`, `text`) |
| 39765 | `ER_SPRIG_GEO_INVALID` | A location or region out of range or malformed |
| 39768 | `ER_SPRIG_GEO_TOO_COMPLEX` | A polygon over the vertex limit |
| 39792 | `ER_SPRIG_ADMIN_REQUEST_INVALID` | A bad name, id, metadata or `ann_mode` |

Over HTTP these return 400 unless stated, with the code in the body. MongoDB clients get the same numbers, and 40602 when `$vectorSearch` isn't the first stage. See [Errors](../reference/errors.md).

## Not supported

- Hybrid search through the Vectors API (`text` in a search request) and type filters (`type`): both fail with 39739. Use `$rankFusion` or `$scoreFusion` from MongoDB for hybrid search.
- More than one `vector` field in a MongoDB `vectorSearch` index.
- `vector` and `knnVector` fields inside a full-text (`search`) index: use a `vectorSearch` index.
- Packed-bit (`int1`) query vectors.
- A session switch that forces exact search for SQL; use `IGNORE INDEX` instead.
- Vector index types other than MariaDB's HNSW, and quantised storage.

## Related

- [Full-text search](search.md)
- [Documents](documents.md)
- [Cross-model queries](cross-model-queries.md)
- [Search and vectors in SQL](../sql/search-and-vectors.md)
- [Automatic indexing](../performance/automatic-indexing.md)
