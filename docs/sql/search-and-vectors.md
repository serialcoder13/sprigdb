---
title: Search and vectors in SQL
audience: [user, dba]
sidebar:
  order: 6
---

# Search and vectors in SQL

SprigDB adds full-text search indexes to tables, collections and key-value namespaces, and runs MySQL's `MATCH … AGAINST` and its own `sprig_search()` function against them. Vector columns, vector indexes and distance functions are MariaDB's, and SprigDB distributes nearest-neighbour queries across shards. This page is the SQL reference for both; for the models, see [Search](../data-models/search.md) and [Vectors](../data-models/vectors.md).

## Search indexes

A search index lives outside MariaDB, in partitions that SprigDB's workers keep up to date from the change feed. It can always be rebuilt from the table. Searches return keys, which SprigDB turns back into SQL over the table, so privileges, row-level security and sharding apply to the rows you get.

### CREATE SEARCH INDEX

```sql
CREATE SEARCH INDEX [IF NOT EXISTS] index_name ON [db_name.]object_name (
    field_spec [, field_spec] ...
) [WITH (index_option = value [, index_option = value] ...)]

field_spec:
    column_name kind [WITH (field_option = value [, ...])]       -- a table
  | '$.path' kind [WITH (field_option = value [, ...])]          -- a collection's documents
  | VALUE ['$.path'] kind [WITH (field_option = value [, ...])]  -- a key-value namespace's values

kind:
    TEXT | STRING | KEYWORD | TOKEN | NUMBER | NUMERIC | DATE
  | BOOL | BOOLEAN | AUTOCOMPLETE | SUBSTRING | FACET
```

| Kind | Indexes | Use for |
|---|---|---|
| `TEXT` (`STRING`) | Analysed text with positions, scored with BM25 | Words and phrases |
| `KEYWORD` (`TOKEN`) | The exact value | Filters, sorts, facets on codes and tags |
| `NUMBER` (`NUMERIC`) | A number | Ranges, sorts |
| `DATE` | A date | Ranges, sorts |
| `BOOL` (`BOOLEAN`) | True or false | Filters |
| `AUTOCOMPLETE` | Edge n-grams of 2 to 15 characters | Type-ahead |
| `SUBSTRING` | Trigrams | Substring matches |
| `FACET` | Hierarchical paths | Facet counts |

| Field option | Default | Meaning |
|---|---|---|
| `analyzer` | the index's | Index-time analyzer of a text field |
| `query_analyzer` (or `search_analyzer`) | `analyzer` | Analyzer applied to the query text |
| `boost` | `1.0` | Score multiplier |
| `filter` | `false` | Keep the value for fast filtering |
| `sort` | `false` | Keep the value for sorting |
| `facet` | `false` | Count the field in facets |

| Index option | Default | Meaning |
|---|---|---|
| `analyzer` | `standard` | Analyzer of text fields that name none |
| `partitions` | 1 | Key-hash partitions per copy; more partitions spread a large index over workers |
| `dynamic` | `false` | Collections: index every string in the document |

An object may have at most `search.max_indexes_per_object` search indexes (default 4). Each partition has `search.copies` copies (default 2) on different workers.

**Privileges:** `INDEX` on the object. The index is built with its creator's privileges.

```sql test
CREATE TABLE products (
  id INT PRIMARY KEY,
  name VARCHAR(200),
  description TEXT,
  category VARCHAR(40),
  price DECIMAL(10,2)
);
INSERT INTO products VALUES
  (1, 'Red running shoe', 'Light shoe for road running', 'shoes', 89.00),
  (2, 'Blue trail shoe', 'Grippy shoe for mountain trails', 'shoes', 120.00),
  (3, 'Wool socks', 'Warm socks for winter running', 'socks', 15.00);

CREATE SEARCH INDEX products_text ON products (
  name TEXT WITH (analyzer = 'english', boost = 2.0),
  description TEXT WITH (analyzer = 'english'),
  category KEYWORD WITH (filter = true, facet = true),
  price NUMBER WITH (filter = true, sort = true)
);
```

For a collection, name document paths: `CREATE SEARCH INDEX events_text ON events ('$.title' TEXT, '$.tags' KEYWORD)`. For a key-value namespace, name the value: `CREATE SEARCH INDEX notes_text ON notes (VALUE TEXT)` or `VALUE '$.body' TEXT` for JSON values.

A new index fills from the table's existing rows in the background, then follows changes, typically within about a second (see [Search](../data-models/search.md#freshness-and-lag)).

### ALTER SEARCH INDEX

```sql
ALTER SEARCH INDEX index_name [ON [db_name.]object_name] ADD FIELD field_spec
ALTER SEARCH INDEX index_name [ON [db_name.]object_name] DROP FIELD field_name
ALTER SEARCH INDEX index_name [ON [db_name.]object_name] REBUILD
```

`ADD FIELD` and `DROP FIELD` change the index's schema and raise its generation; `REBUILD` rebuilds every partition from the table. **Privileges:** `INDEX` on the object.

```sql test
ALTER SEARCH INDEX products_text DROP FIELD price;
ALTER SEARCH INDEX products_text ADD FIELD price NUMBER WITH (filter = true, sort = true);
```

### DROP SEARCH INDEX

```sql
DROP SEARCH INDEX [IF EXISTS] index_name [ON [db_name.]object_name]
```

**Privileges:** `INDEX` on the object. Dropping the table drops its search indexes.

### SHOW SEARCH INDEXES

```sql
SHOW SEARCH INDEXES [{FROM | IN} [db_name.]object_name]
```

One row per field of every search index in the current database (or on one object) that you hold `SELECT` on:

| Column | Meaning |
|---|---|
| `Index` | The index |
| `Object` | The table, collection or namespace |
| `Model` | `table`, `collection` or `kv` |
| `Flavor` | How it was created: `sql`, `fulltext_compat` (converted from `FULLTEXT`), `mongo_text` or `mongo_search` |
| `Field` | The field: a column or a dotted path |
| `Kind` | `text`, `keyword`, `number`, `date`, `bool`, `autocomplete`, `substring` or `facet` |
| `Analyzer` | The index-time analyzer |
| `Boost` | The score multiplier |
| `Filter`, `Sort`, `Facet` | `1` or `0` |
| `Generation` | Raised by each schema change |
| `State` | The index's state on this gateway, such as building or ready |

```sql test
SHOW SEARCH INDEXES FROM products;
```

### CREATE and DROP SEARCH ANALYZER

```sql
CREATE SEARCH ANALYZER [IF NOT EXISTS] analyzer_name WITH (
    tokenizer = 'name'
    [, min_gram = n, max_gram = n]
    [, filters = ('filter' [, 'filter'] ...)]
    [, synonyms = '[["word", "word", ...], ...]']
)
DROP SEARCH ANALYZER [IF EXISTS] analyzer_name
```

A custom analyzer belongs to the tenant and can be named in any of its indexes.

| Option | Values |
|---|---|
| `tokenizer` | `standard` (default), `whitespace`, `keyword` (or `raw`, the whole value), `ngram` |
| `min_gram`, `max_gram` | For `ngram`: gram lengths (default 3) |
| `filters` | Applied in order: `lowercase`, `ascii_folding`, `stop:<language>`, `stem:<language>`, `min_length:<n>`, `max_length:<n>`, `edge_ngram` |
| `synonyms` | A JSON array of synonym sets, as a string. Applied at query time, so changing them needs no rebuild |

Built-in analyzers:

| Analyzer | Chain |
|---|---|
| `standard` | Simple tokenizer, tokens over 255 bytes removed, lower case |
| `english`, `french`, … | Standard, the language's stop words and stemmer |
| `keyword` | The whole value |
| `whitespace` | Whitespace tokenizer |
| `autocomplete` | Standard, then edge n-grams of 2 to 15 characters |
| `trigram` | N-grams of 3, lower case, ASCII folding |
| `mysql_compat` | Standard, English stop words, tokens of 3 characters or more (used for converted `FULLTEXT` indexes) |

Languages: Arabic, Danish, Dutch, English, Finnish, French, German, Greek, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian, Spanish, Swedish, Tamil and Turkish, by name or two-letter code (`english` or `en`).

**Privileges:** `CREATE` (for `CREATE SEARCH ANALYZER`) or `DROP` at the tenant level (`ON *.*`); otherwise 1227.

```sql test
CREATE SEARCH ANALYZER IF NOT EXISTS product_names WITH (
  tokenizer = 'standard',
  filters = ('lowercase', 'ascii_folding', 'stem:english'),
  synonyms = '[["tv", "television"], ["sneaker", "trainer", "shoe"]]'
);
DROP SEARCH ANALYZER IF EXISTS product_names;
```

### MySQL FULLTEXT DDL

`CREATE FULLTEXT INDEX`, `ALTER TABLE … ADD FULLTEXT [INDEX | KEY]` and `FULLTEXT` keys inside `CREATE TABLE` (as in MySQL dumps) are handled by `search.fulltext_ddl`:

| Value | Effect |
|---|---|
| `convert` (default) | The table is created without the key, and each `FULLTEXT` key becomes a search index of `TEXT` fields with the `mysql_compat` analyzer (flavor `fulltext_compat`) |
| `mariadb` | MariaDB's own `FULLTEXT` index is created, and `MATCH` runs in MariaDB |
| `reject` | The statement fails with 39823 |

```sql test
CREATE TABLE articles (
  id INT PRIMARY KEY,
  title VARCHAR(200),
  body TEXT,
  FULLTEXT KEY ft_articles (title, body)
);
SHOW SEARCH INDEXES FROM articles;
```

## Full-text queries

### MATCH … AGAINST

```sql
MATCH (column [, column] ...) AGAINST ('text' [search_modifier])

search_modifier:
    IN NATURAL LANGUAGE MODE
  | IN BOOLEAN MODE
```

When the table has a search index whose fields cover the columns, SprigDB searches the index (the one with the fewest extra fields) and replaces the call:

- in `WHERE`, by a condition on the table's key (`id IN (…)`);
- in the select list or `ORDER BY`, by the score of each row.

The statement then runs as usual. A table without any search index keeps MariaDB's `FULLTEXT` behaviour.

| Mode | Query text |
|---|---|
| Natural language (default) | Words, any of which may match, ranked by BM25 |
| Boolean | MySQL's operators: `+word` (required), `-word` (excluded), `"phrase"`, `word*` (prefix), `( )`, and `>word`, `<word`, `~word`, which become boosts of 1.5, 0.67 and 0.5 |

`WITH QUERY EXPANSION` fails with 39823, and so does a placeholder (`?`) instead of a string literal in `AGAINST`. A table with search indexes none of which covers the columns fails with 39821 `ER_SPRIG_SEARCH_NO_INDEX_FOR_MATCH`.

```sql
SELECT id, name, MATCH (name, description) AGAINST ('running shoe') AS score
FROM products
WHERE MATCH (name, description) AGAINST ('running shoe')
ORDER BY score DESC;

SELECT id FROM products
WHERE MATCH (name, description) AGAINST ('+shoe -trail' IN BOOLEAN MODE)
  AND price < 100;
```

The scores are BM25 scores, not InnoDB's or MariaDB's relevance values, so compare them only with each other.

### sprig_search()

```sql
sprig_search('[db_name.]index_name', 'query' [, options_json])
```

A table function: use it in `FROM` with an alias. It returns one row per hit:

| Column | Meaning |
|---|---|
| The object's key columns | Such as `id`; `_id` for a collection, `k` for a namespace |
| `score` | The hit's score |
| `highlights` | Highlighted fragments of the requested fields, or `NULL` |
| `_shard` | Always `0` |

The query string syntax:

| Syntax | Meaning |
|---|---|
| `word word` | Any of the words (OR) |
| `"a phrase"`, `"a phrase"~2` | A phrase, with optional slop |
| `+word`, `-word` | Required, excluded |
| `field:word` | In one field |
| `word~1` | Fuzzy, within edit distance 1 |
| `pre*` | Prefix |
| `word^2` | Boost |
| `AND`, `OR`, `NOT`, `( )` | Boolean structure |

`options_json` is a JSON object, usually built with `JSON_OBJECT`:

| Option | Default | Meaning |
|---|---|---|
| `limit` | 10 | Hits returned, at most `search.max_window` (default 10,000) |
| `consistency` | `search.default_consistency` (`stale_ok`) | `stale_ok`: answer from the index as it is; `own_writes`: wait until the index has this session's writes; `latest`: wait until it has every commit so far. The wait is at most `search.wait_timeout_ms` (default 2000 ms), then 39826 |
| `highlight` | none | A JSON array of field names to highlight, fragments of `search.highlight_chars` characters |

**Privileges:** `SELECT` on the indexed object.

```sql
SELECT p.id, p.name, s.score
FROM sprig_search('products_text', 'name:shoe^2 running -kids',
                  JSON_OBJECT('limit', 20, 'consistency', 'own_writes',
                              'highlight', JSON_ARRAY('description'))) AS s
JOIN products p ON p.id = s.id
ORDER BY s.score DESC;
```

### EXPLAIN of searches

`EXPLAIN` of a statement with `MATCH` or `sprig_search()` returns MariaDB's plan of the rewritten statement, then one `SEARCH` row per search with the index, partitions, consistency, the parsed query, and the hit count and time. With `FORMAT=JSON` or `FORMAT=TREE`, the searches are in one `{"sprig_search_scans": [...]}` row. The hit count and time are left out when a row-level security policy binds you on the object, because they would count hidden rows. See [EXPLAIN and workload](explain-and-workload.md).

## Vectors

Vector columns, indexes and functions are MariaDB's, on both ports. SprigDB passes them to MariaDB and makes nearest-neighbour queries work across shards.

### Vector columns and indexes

```sql
CREATE TABLE table_name (
    ...,
    column_name VECTOR(dimensions) NOT NULL,
    VECTOR INDEX [index_name] (column_name) [M = m] [DISTANCE = {euclidean | cosine}]
)

CREATE VECTOR INDEX index_name ON table_name (column_name) [M = m] [DISTANCE = ...]
ALTER TABLE table_name ADD VECTOR INDEX [index_name] (column_name) [M = m] [DISTANCE = ...]
```

| Part | Meaning |
|---|---|
| `VECTOR(n)` | A vector of `n` 32-bit floats. A column in a vector index must be `NOT NULL` |
| `VECTOR INDEX` | MariaDB's HNSW index for approximate nearest neighbours |
| `M` | Graph connections per node; larger is more accurate and bigger |
| `DISTANCE` | `euclidean` or `cosine`: the distance the index serves |

Over the wire, a `VECTOR` column travels as a binary string (`VAR_STRING`, binary character set).

### Distance functions

| Function | Returns |
|---|---|
| `VEC_FromText('[x, y, …]')` | A vector from its JSON-array text |
| `VEC_ToText(v)` | The text of a vector |
| `VEC_DISTANCE_EUCLIDEAN(a, b)` | Euclidean distance |
| `VEC_DISTANCE_COSINE(a, b)` | 1 − cosine similarity |
| `VEC_DISTANCE(a, b)` | The distance of the index on the column, if any |

### Nearest-neighbour queries

```sql
SELECT ... FROM table_name
[WHERE condition]
ORDER BY VEC_DISTANCE_EUCLIDEAN(column_name, query_vector)   -- or _COSINE, or VEC_DISTANCE
LIMIT k
```

On a sharded table, the query runs on every shard and the gateway merges the rows by distance, so you get the `k` nearest overall. A vector index makes each shard's search approximate (HNSW); without one, the search is exact.

```sql test
CREATE TABLE product_embeddings (
  id INT PRIMARY KEY,
  embedding VECTOR(3) NOT NULL,
  VECTOR INDEX (embedding) M=8 DISTANCE=euclidean
);
INSERT INTO product_embeddings VALUES
  (1, VEC_FromText('[1, 0, 0]')),
  (2, VEC_FromText('[0, 1, 0]')),
  (3, VEC_FromText('[0.9, 0.1, 0]'));

SELECT id
FROM product_embeddings
ORDER BY VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FromText('[1, 0, 0]'))
LIMIT 2;
```

```text output
id
1
3
```

### Automatic vector indexes

SprigDB's automatic indexing also watches nearest-neighbour queries. `vector.auto_ann` (`on`, `approve`, `recommend` or `off`) decides whether it builds HNSW graphs for them, once a table has `vector.ann_min_rows` rows (default 20,000) and the graph reaches `vector.min_recall` (default 0.95) against exact results. Indexes SprigDB creates use `M = vector.default_m` (default 16). See [Automatic indexing](../performance/automatic-indexing.md).

Standalone vector indexes managed through the Vectors API, and MongoDB's `$vectorSearch`, are described in [Vectors](../data-models/vectors.md). Their errors (39735 to 39740) don't arise from SQL.

## Geospatial nearest neighbours

On the MySQL persona (port 3306), `SELECT … FROM t [WHERE …] ORDER BY ST_Distance(col, point) LIMIT k` over one table, with an SRID 4326 point, runs with a growing radius: starting at 1 km and multiplying by four, SprigDB counts rows within the radius using the column's `SPATIAL INDEX`, and runs the statement with the first radius that holds `k` rows. The result is the same as the full sort. See [Geospatial](../data-models/geospatial.md).

## Errors

| Code | Name | When |
|---|---|---|
| 1142 | (`42000`) | Missing `INDEX` (DDL) or `SELECT` (queries) on the object |
| 1227 | (`42000`) | `CREATE` or `DROP SEARCH ANALYZER` without the tenant-level privilege |
| 1046, 1049 | | No database selected; unknown database |
| 39820 | `ER_SPRIG_SEARCH_INDEX_NOT_FOUND` | The search index doesn't exist |
| 39821 | `ER_SPRIG_SEARCH_NO_INDEX_FOR_MATCH` | No search index on the table covers the `MATCH` columns |
| 39822 | `ER_SPRIG_SEARCH_QUERY_SYNTAX` | A query string, DDL option or analyzer definition can't be parsed |
| 39823 | `ER_SPRIG_SEARCH_UNSUPPORTED` | `WITH QUERY EXPANSION`, a placeholder in `AGAINST`, `FULLTEXT` DDL with `search.fulltext_ddl = reject`, or another unsupported form |
| 39824 | `ER_SPRIG_SEARCH_INDEX_NOT_READY` | The index is still building |
| 39825 | `ER_SPRIG_SEARCH_UNAVAILABLE` | No copy of a partition answered; retry |
| 39826 | `ER_SPRIG_SEARCH_WAIT_TIMEOUT` | The consistency wait passed `search.wait_timeout_ms` |
| 39827 | `ER_SPRIG_SEARCH_WINDOW_EXCEEDED` | More hits needed than `search.max_window` |
| 39828 | `ER_SPRIG_SEARCH_ANALYZER_UNKNOWN` | An analyzer, tokenizer or filter that doesn't exist |
| 39829 | `ER_SPRIG_SEARCH_FIELD_TYPE` | A field kind that doesn't fit the column or value |
| 39830 | `ER_SPRIG_SEARCH_INDEX_LIMIT` | The object already has `search.max_indexes_per_object` indexes |
| 39831 | `ER_SPRIG_SEARCH_NO_PLACEMENT` | No place may hold the index's copies |
| 39832 (warning) | `ER_SPRIG_SEARCH_STALE_IN_TXN` | A search inside a transaction that wrote the object reads committed data only, without the transaction's own writes |
| 39420 | `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` | MariaDB `FULLTEXT` relevance compared across shards (`fulltext_relevance`) |

## Related

- [Search](../data-models/search.md)
- [Vectors](../data-models/vectors.md)
- [Automatic indexing](../performance/automatic-indexing.md)
- [EXPLAIN and workload](explain-and-workload.md)
