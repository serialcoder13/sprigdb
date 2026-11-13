---
title: Full-text search
audience: [user, dba]
sidebar:
  order: 5
---

# Full-text search

A search index finds rows, documents or key-value entries by the words they contain, ranks them by relevance, and can highlight the matching words and count matches by category (facets). You create one on a SQL table, a MongoDB collection or a key-value namespace, and query it from SQL (`MATCH ... AGAINST` and `sprig_search()`), from MongoDB (`$text`, `$search` and `$searchMeta`) or over REST and gRPC.

## How search indexes are stored

Search indexes are the one structure SprigDB keeps outside MariaDB. Each index is a set of Tantivy files on local disk, built from your data and kept up to date from the [change feed](../concepts/change-feed.md). MariaDB stays the source of truth: an index can always be rebuilt from it, and every search reads the matching rows back from MariaDB through your session, so what you get back is current and passes your privileges and row policies.

| Part | Where it lives |
|---|---|
| The index definition and custom analyzers | The tenant's catalog in MariaDB |
| The index's files (a *copy*) | Local disk of workers with the search role, or of gateways when there are no such workers |
| Snapshots of a copy | A directory on the worker (`SPRIG_SEARCH_SNAPSHOTS`) |

By default each index has two copies, on different workers (`search.copies`). Each copy holds the whole index. If no worker in the data centre has the search role, or `search.copies` is `0`, each gateway that serves the index keeps its own copy instead. Queries go to a ready copy, and to the other copy when the first fails.

Text is never stored in the index. Highlights and returned rows always come from MariaDB.

## Create a search index in SQL

```sql test
CREATE TABLE products (
  id          BIGINT PRIMARY KEY,
  name        VARCHAR(200) NOT NULL,
  description TEXT,
  brand       VARCHAR(40),
  price       DECIMAL(10, 2)
);

INSERT INTO products VALUES
  (1, 'Trail running shoe', 'Grippy sole for muddy trails', 'acme', 89.00),
  (2, 'Road running shoe', 'Light and cushioned for long runs', 'zoom', 120.00),
  (3, 'Kids running shoe', 'Velcro straps, bright colours', 'acme', 45.00),
  (4, 'Rain jacket', 'Waterproof shell with a hood', 'tiny', 75.00);

CREATE SEARCH INDEX product_text ON products (
  name        TEXT    WITH (analyzer = 'english', boost = 2.0),
  description TEXT    WITH (analyzer = 'english'),
  brand       KEYWORD WITH (facet = true, filter = true),
  price       NUMBER  WITH (filter = true, sort = true)
);
```

The full grammar:

```sql
CREATE SEARCH INDEX [IF NOT EXISTS] name ON [db.]object (
  field_spec [, field_spec ...]
) [WITH (analyzer = 'standard', partitions = 2, dynamic = true)];

-- field_spec, by model:
column KIND [WITH (option = value, ...)]        -- a SQL table
'$.path' KIND [WITH (...)]                       -- a collection's documents
VALUE ['$.path'] KIND [WITH (...)]               -- a key-value namespace's values

ALTER SEARCH INDEX [db.]name [ON [db.]object] ADD FIELD field_spec;
ALTER SEARCH INDEX [db.]name [ON [db.]object] DROP FIELD field;
ALTER SEARCH INDEX [db.]name [ON [db.]object] REBUILD;
DROP SEARCH INDEX [IF EXISTS] [db.]name [ON [db.]object];
SHOW SEARCH INDEXES [FROM | IN [db.]object];
```

Index names are unique per database; a duplicate fails with 1061. An object has at most `search.max_indexes_per_object` (4) search indexes; one more fails with 39830. A table needs a primary key, because hits are identified by it. Creating, changing or dropping a search index needs the `INDEX` privilege on the object.

### Field kinds

| Kind | Indexes | SQL column types |
|---|---|---|
| `TEXT` | Analyzed words with positions; scored | `CHAR`, `VARCHAR`, `TEXT` types, `JSON`, `ENUM`, `SET` |
| `KEYWORD` | The exact value, for filters and facets | Text, numeric, date and time, binary and `UUID` types |
| `NUMBER` | A number | Integer, `DECIMAL`, `FLOAT`, `DOUBLE`, `BIT`, `YEAR` |
| `DATE` | A date | `DATE`, `DATETIME`, `TIMESTAMP`, `TIME` |
| `BOOL` | A boolean | Numeric types |
| `AUTOCOMPLETE` | Edge n-grams of 2 to 15 characters, for type-ahead | Text types |
| `SUBSTRING` | Trigrams (n-grams of 3) | Text types |
| `FACET` | Values counted by facets | Text types |

A column of the wrong type fails with 39829. The same name may appear once per kind, so a column can be both `TEXT` and `KEYWORD`.

### Field options

| Option | Type | Default | Meaning |
|---|---|---|---|
| `analyzer` | text | the index's analyzer | How text is split into terms when indexed |
| `query_analyzer` | text | the index-time analyzer | How query text is analyzed |
| `boost` | number | `1` | A positive multiplier for this field's score |
| `filter` | boolean | `false` | Keep the value for filter clauses (`range`, `equals`) |
| `sort` | boolean | `false` | Keep the value for sorting |
| `facet` | boolean | `false` (`true` for `FACET`) | Count the value in facets |

### Index options

| Option | Type | Default | Meaning |
|---|---|---|---|
| `analyzer` | text | `standard` | The analyzer of text fields that name none |
| `partitions` | integer | `2` | Hash buckets of keys inside each copy, 1 to 64 |
| `dynamic` | boolean | `false` | Index every string in the document (collections) |

### Collections and key-value namespaces

On a collection, fields are paths into the document. Arrays index one value per element.

```sql
CREATE SEARCH INDEX order_notes ON shop.events (
  '$.notes' TEXT,
  '$.customer.name' AUTOCOMPLETE,
  '$.tags' KEYWORD WITH (facet = true)
);
```

On a [key-value namespace](key-value.md), `VALUE '$.path'` indexes a path inside string values that hold JSON, and `VALUE` without a path indexes the whole value as text. Hashes and other non-string values aren't indexed.

```sql
CREATE SEARCH INDEX note_text ON shop.sessions (VALUE '$.note' TEXT);
```

### Inspect indexes

```sql test
SHOW SEARCH INDEXES FROM products;
```

`SHOW SEARCH INDEXES` returns one row per field of the indexes you may `SELECT` from:

| Column | Meaning |
|---|---|
| `Index`, `Object` | The index and the table, collection or namespace it covers |
| `Model` | `table`, `collection` or `kv` |
| `Flavor` | How it was created: `sql`, `mongo_text`, `mongo_search` or `fulltext_compat` |
| `Field`, `Kind`, `Analyzer`, `Boost` | The field |
| `Filter`, `Sort`, `Facet` | `1` when the option is on |
| `Generation` | Raised each time the index is rebuilt |
| `State` | Where the copies are and whether they are ready, for example `ready on wkr-1, wkr-2` or `ready in this gateway` |

### MySQL `FULLTEXT` indexes

Applications and dumps written for MySQL create `FULLTEXT` indexes. What SprigDB does with them depends on `search.fulltext_ddl`:

| Value | Behaviour |
|---|---|
| `convert` (default) | `CREATE FULLTEXT INDEX`, `ALTER TABLE ... ADD FULLTEXT` and `FULLTEXT KEY` inside `CREATE TABLE` become search indexes with the `mysql_compat` analyzer. The table is created without a MariaDB `FULLTEXT` index. |
| `mariadb` | The statement goes to MariaDB, which builds its own `FULLTEXT` index on each shard. |
| `reject` | The statement fails with 39823. |

`WITH PARSER` fails with 39823. A converted index answers `MATCH` with every committed row, as InnoDB does (see [Freshness](#freshness-and-lag)), so a query right after a write finds it:

```sql test
CREATE TABLE articles (
  id    INT PRIMARY KEY,
  title VARCHAR(200),
  body  TEXT,
  FULLTEXT KEY ft_title_body (title, body),
  KEY by_title (title)
) ENGINE=InnoDB;

INSERT INTO articles VALUES
  (1, 'MySQL tutorial', 'DBMS stands for DataBase'),
  (2, 'How to use MySQL well', 'After you went through a tutorial'),
  (3, 'Optimizing MySQL', 'In this tutorial we show'),
  (4, 'Comparing databases', 'Postgres versus MariaDB'),
  (5, 'Security', 'When configured properly, MySQL is safe');

SELECT id FROM articles
WHERE MATCH (title, body) AGAINST ('+tutorial -optimizing' IN BOOLEAN MODE)
ORDER BY id;
```

```text output
id
1
2
```

`SHOW SEARCH INDEXES FROM articles` now lists `ft_title_body` with flavor `fulltext_compat`, and MariaDB holds no `FULLTEXT` index on the table.

## Analyzers

An analyzer turns text into terms, both when a value is indexed and when a query is analyzed.

| Analyzer | What it does |
|---|---|
| `standard` | Splits on non-letters, drops tokens over 255 bytes, lowercases |
| `english`, `french`, ... | `standard`, then the language's stop words and Snowball stemmer |
| `keyword` | The whole value as one term |
| `whitespace` | Splits on whitespace only |
| `autocomplete` | `standard`, then edge n-grams of 2 to 15 characters at index time; plain `standard` for queries |
| `trigram` | N-grams of 3, lowercased, accents folded |
| `mysql_compat` | `standard`, English stop words, terms of 3 characters or more |
| `mongo_<language>`, `mongo_none` | For MongoDB `text` indexes: `standard`, the language's stop words and stemmer, then accents folded |

Languages: Arabic, Danish, Dutch, English, Finnish, French, German, Greek, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian, Spanish, Swedish, Tamil and Turkish, by English name or ISO 639-1 code (`en`, `fr`). Atlas names map onto these: `lucene.standard` and `lucene.simple` are `standard`, `lucene.english` is `english`, `lucene.keyword` is `keyword`. An unknown analyzer or language fails with 39828.

Stop words and stemmers are Tantivy's. `mysql_compat` and the MongoDB analyzers therefore use Tantivy's stop-word lists, not InnoDB's or MongoDB's, and a query made only of words that one list has and the other doesn't can match differently.

### Custom analyzers

```sql
CREATE SEARCH ANALYZER en_syn WITH (
  tokenizer = 'standard',
  filters = ('lowercase', 'ascii_folding', 'stop:english', 'stem:english'),
  synonyms = '[["tv","television"],["laptop","notebook"]]'
);

DROP SEARCH ANALYZER [IF EXISTS] en_syn;
```

| Option | Values |
|---|---|
| `tokenizer` | `standard`, `whitespace`, `keyword`, or `ngram` with `min_gram` and `max_gram` (default 3) |
| `filters` | In order: `lowercase`, `ascii_folding`, `stop:<language>`, `stem:<language>`, `min_length:<n>`, `max_length:<n>`, `edge_ngram` (2 to 15 characters, index time only) |
| `synonyms` | A JSON array of arrays of equivalent words |

Analyzers belong to the tenant, and their names can't be those of built-in analyzers. Synonyms are applied at query time: each query term becomes an OR of its set, so changing synonyms needs no rebuild. Creating or dropping an analyzer needs the `CREATE` or `DROP` privilege.

## Query from SQL

### `MATCH ... AGAINST`

MySQL's full-text syntax works on tables with a search index:

```sql test
SELECT id, name, MATCH(name, description) AGAINST ('red running shoe') AS score
FROM products
WHERE MATCH(name, description) AGAINST ('red running shoe') AND price < 100
ORDER BY score DESC
LIMIT 20;
```

SprigDB picks the table's search index whose text fields include every `MATCH` column, preferring the one with the fewest extra fields. It runs the search and replaces each `MATCH` call before the statement runs: in `WHERE` and `ON` by a condition on the primary key, elsewhere by each hit's score. The statement then runs as any other, so privileges, row-level security and sharding apply to it unchanged.

| MariaDB form | Search |
|---|---|
| Natural language mode (default) | Any of the words |
| `IN BOOLEAN MODE`: `+word`, `-word` | Required, excluded |
| `word*` | Prefix |
| `"two words"` | Phrase |
| `>word`, `<word`, `~word` | Boost 1.5, 0.67, 0.5 |
| `( ... )` | Grouping |
| `WITH QUERY EXPANSION` | Fails with 39823 |

```sql test-error
SELECT id FROM articles WHERE MATCH (title, body) AGAINST ('tutorial' WITH QUERY EXPANSION);
```

- A table without any search index keeps MariaDB's own `FULLTEXT` behaviour.
- A table with search indexes, none of which covers the `MATCH` columns, fails with 39821.
- `MATCH ... AGAINST (?)` works in server-side prepared statements. In a text statement, `AGAINST` needs a string literal (39823 otherwise).
- As a condition, `MATCH` needs every match, up to `search.max_window` (10,000); more fails with 39827.
- Scores are BM25 scores, so their values differ from MariaDB's relevance values. The order is usually similar.

### `sprig_search()`

`sprig_search()` is a table function that gives you the search's ranking, scores and highlights to join with anything:

```sql test
SELECT p.id, p.name, s.score, s.highlights
FROM sprig_search('product_text', 'red "running shoe" -kids name:trail~1',
                  JSON_OBJECT('highlight', JSON_ARRAY('name'), 'limit', 20,
                              'consistency', 'latest')) AS s
JOIN products p ON p.id = s.id
ORDER BY s.score DESC;
```

The arguments are the index (`name` or `db.name`), a query string, and optional options as `JSON_OBJECT(...)` or a JSON string:

| Option | Default | Meaning |
|---|---|---|
| `limit` | `10` | Best hits to return, at most `search.max_window` |
| `consistency` | `search.default_consistency` | `stale_ok`, `own_writes` or `latest` |
| `highlight` | none | Text fields to highlight (tables only) |

It returns the object's key columns (the primary key for a table, `_id` for a collection, `k` for a namespace), `score DOUBLE`, `highlights JSON` and `_shard`. Rows are read with your privileges. A highlight looks like this, with byte ranges of the matched words inside the fragment of about `search.highlight_chars` (150) characters:

```json
{"name": [{"text": "Trail running shoe, waterproof", "hits": [[6, 13], [14, 18]]}]}
```

### Query strings

`sprig_search()`, the REST `query_string` and MongoDB's `queryString` operator share one syntax:

| Form | Meaning |
|---|---|
| `red shoe` | Either word (OR by default) |
| `"running shoe"`, `"running shoe"~2` | A phrase; with up to 2 word moves |
| `+red`, `-kids` | Required, excluded |
| `name:trail` | A word in one field |
| `trail~1`, `trail~2` | Fuzzy, within 1 or 2 edits |
| `run*` | Prefix |
| `shoe^2` | Boost |
| `( ... )`, `AND`, `OR`, `NOT` | Grouping and operators |

Fuzzy and prefix terms expand to at most `search.max_expansions` (50) of the most frequent matching terms. A malformed query string fails with 39822.

### `EXPLAIN`

`EXPLAIN` of a statement with `MATCH` or `sprig_search()` runs the searches, returns MariaDB's plan of the rewritten statement, and adds one row per search with `select_type` `SEARCH`, `type` `fulltext`, `key` the index, `rows` the hits, and `Extra` describing the scan: `SearchScan(index shop.product_text, 2 partitions, consistency ..., scoring ..., copies: ..., ...)`. `FORMAT=JSON` and `FORMAT=TREE` get one more row, `{"sprig_search_scans": [...]}`. For a user bound by a row policy on the object, the hit count and time are left out. See [EXPLAIN and workload](../sql/explain-and-workload.md).

## Query from MongoDB

### Text indexes and `$text`

A MongoDB `text` index is a search index of flavor `mongo_text`. One is allowed per collection, as in MongoDB:

```js
db.events.createIndex(
  { notes: "text", title: "text" },
  { weights: { title: 10 }, default_language: "english" }
)

db.events.find(
  { $text: { $search: "late \"wrong address\" -refund" }, status: "open" },
  { score: { $meta: "textScore" } }
).sort({ score: { $meta: "textScore" } })
```

- Words are ORed, every quoted phrase is required, and `-word` or `-"phrase"` excludes.
- `weights` become field boosts. `{ "$**": "text" }` indexes every string.
- `default_language` picks the analyzer (`none` turns stemming and stop words off). A `language_override` other than `language` fails with 39823, and `$language` must match the index's language (39828 otherwise).
- `$caseSensitive` and `$diacriticSensitive` work: the search runs as usual, then only documents whose text contains each term and phrase with the same case or accents are kept.
- `$text` always waits until the index holds every committed write (`latest`), because MongoDB updates text indexes with the write.
- Without a text index, `$text` fails with 27 `IndexNotFound`.
- `textScore` is BM25, so its values differ from MongoDB's.

### Search indexes, `$search` and `$searchMeta`

Atlas-style search indexes use `createSearchIndexes` with `type: "search"` (the default):

```js
db.runCommand({
  createSearchIndexes: "events",
  indexes: [{
    name: "default",
    definition: {
      analyzer: "lucene.standard",
      mappings: {
        dynamic: false,
        fields: {
          notes: { type: "string", analyzer: "lucene.english" },
          tags:  [{ type: "token" }, { type: "stringFacet" }],
          customer: { type: "document", fields: { name: { type: "autocomplete" } } },
          amount: [{ type: "number" }, { type: "numberFacet" }]
        }
      }
    }
  }]
})
```

| Atlas field type | SprigDB kind |
|---|---|
| `string` | `TEXT` |
| `token`, `objectId`, `uuid`, `stringFacet` | `KEYWORD`, with filter and facet |
| `number`, `numberFacet` | `NUMBER`, with filter, sort and facet |
| `date`, `dateFacet` | `DATE`, with filter, sort and facet |
| `boolean` | `BOOL`, with filter and sort |
| `autocomplete` | `AUTOCOMPLETE` |
| `document` | Its sub-fields, by dotted path |

`dynamic: true` indexes every string in the document. `analyzer` and `searchAnalyzer` are accepted at index and field level. `listSearchIndexes` and `$listSearchIndexes` report `status: "BUILDING"` and `queryable: false` until a copy is ready. `updateSearchIndex` and `dropSearchIndex` work as in Atlas. Index names are unique per collection; the same name with a different definition fails with 68.

`$search` must be the first stage:

```js
db.events.aggregate([
  { $search: {
      index: "default",
      compound: {
        must:   [{ text: { query: "late delivery", path: "notes", fuzzy: { maxEdits: 1 } } }],
        filter: [{ range: { path: "amount", gte: 100 } }]
      },
      highlight: { path: "notes" }
  } },
  { $limit: 10 },
  { $project: { notes: 1, score: { $meta: "searchScore" }, hl: { $meta: "searchHighlights" } } }
])
```

| Operator | Supported options |
|---|---|
| `text` | `query`, `path`, `fuzzy.maxEdits` (1 or 2), `matchCriteria: "all"` |
| `phrase` | `query`, `path`, `slop` |
| `autocomplete` | `query`, `path`, `fuzzy.maxEdits`, `tokenOrder: "sequential"` |
| `compound` | `must`, `should`, `mustNot`, `filter`, `minimumShouldMatch` |
| `range` | `path`, `gt`, `gte`, `lt`, `lte` |
| `equals`, `in` | `path`, `value` |
| `exists` | `path` |
| `wildcard`, `regex` | `query`, `path` |
| `queryString` | `defaultPath`, `query` (the query string syntax above) |

Every operator takes `score: { boost: { value: n } }`. The stage also takes `index` (default `default`), `highlight.path` and `consistency` (`stale_ok`, `own_writes` or `latest`). A search on an index that doesn't exist returns no documents. `{ $meta: "searchScore" }` (or `"score"`) gives the score and `{ $meta: "searchHighlights" }` Atlas's highlight shape: `[{ path, score, texts: [{ value, type: "hit" | "text" }] }]`.

`$searchMeta` returns one document with a count and facets. For example, this stage:

```js
db.events.aggregate([
  { $searchMeta: {
      index: "default",
      facet: {
        operator: { text: { query: "late", path: "notes" } },
        facets: {
          tags:    { type: "string", path: "tags", numBuckets: 5 },
          amounts: { type: "number", path: "amount", boundaries: [0, 100, 500, 1000] }
        }
      },
      count: { type: "total" }
  } }
])
```

returns a document of this shape (the values here are illustrative, not from a real run):

```json
{ "count": { "total": 42 },
  "facet": { "tags":    { "buckets": [{ "_id": "courier", "count": 17 }, { "_id": "warehouse", "count": 9 }] },
             "amounts": { "buckets": [{ "_id": 0, "count": 30 }, { "_id": 100, "count": 10 }, { "_id": 500, "count": 2 }] } } }
```

A string facet returns its top `numBuckets` (default 10) values, most frequent first. A number facet has one bucket per pair of neighbouring `boundaries`, named by its lower boundary; a bucket includes its lower boundary and excludes its upper one, and values outside every bucket aren't counted. Without `count: { type: "total" }` the count is reported as `lowerBound`.

MongoDB's `explain` covers `find` with `$text` and pipelines that start with `$search`, `$searchMeta` or a `$match` with `$text`: the winning plan's `stage` is `TEXT_MATCH`, `SEARCH` or `SEARCH_META`, and `sprigSearch` describes the scan.

For hybrid ranking with vector search, use `$rankFusion` or `$scoreFusion`; see [Vectors](vectors.md#hybrid-search-with-rankfusion-and-scorefusion).

## Query over REST and gRPC

`Search.Query` searches any index by name:

```sh
curl -u 'acme.app:<password>' -H 'Content-Type: application/json' \
  -X POST http://localhost:8080/v1/databases/shop/search/product_text:query \
  -d '{"query": "{\"text\": {\"query\": \"running\", \"path\": \"name\"}}",
       "filter": "{\"range\": {\"path\": \"price\", \"lte\": 60}}",
       "facets": "{\"brands\": {\"type\": \"string\", \"path\": \"brand\", \"numBuckets\": 5}}",
       "highlight": ["name"], "include_row": true, "limit": 20, "consistency": "latest"}'
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `query_string` | string | none | A query string |
| `query` | string | none | One `$search` operator, as JSON. Give this or `query_string`, not both; neither matches everything. |
| `filter` | string | none | One operator or an array of them, as JSON, applied as unscored filter clauses |
| `facets` | string | none | Facets as `$searchMeta` takes them, as JSON |
| `highlight` | array | none | Text fields to highlight |
| `include_row` | boolean | `false` | Return each hit's row: a table's columns as JSON, a collection's document as Extended JSON, a namespace's value |
| `limit` | integer | `10` | Hits per page, at most `search.max_window` |
| `after` | string | none | The cursor from the previous page |
| `consistency` | string | `search.default_consistency` | `stale_ok`, `own_writes` or `latest` |
| `commit_token` | string | none | Any value waits as `latest` |
| `sort` | string | `score` | Only `score` (39823 otherwise) |

The response has `hits` (each with `key`, `score`, `highlights` and `row` as JSON strings), `facets`, `after` (empty on the last page) and `applied` (the change-feed positions the answering copy had indexed). The call runs in your session: you need `SELECT` on the indexed object, and each page's hits are read back through your session. Hits whose rows you can't read (privileges, row-level security, expired keys) are skipped and the page is filled from deeper hits, up to `search.max_window`. The cursor names the page's last hit by key, so writes that move hits around don't repeat or skip it.

gRPC clients call `sprig.v1.Search/Query` on the same port with the same fields. See [REST and gRPC](../connect/rest-and-grpc.md).

## Scoring

Scores are BM25 (k1 = 1.2, b = 0.75), multiplied by field and query boosts. Each copy holds all of an index's partitions, so by default (`search.scoring = global`) term statistics are summed over every partition and a document scores as it would in one unsharded index. `search.scoring = local` scores each partition with its own statistics, which is cheaper but lets the same document score differently in different partitions.

## Freshness and lag

A copy follows the tenant's change feed: inserts and updates replace the document by key, deletes remove it, `TRUNCATE` empties the index, and dropping the object stops the copy. It commits at transaction boundaries every `search.commit_interval_ms` (500), sooner while a query is waiting, or after 10,000 pending changes. A search therefore sees committed writes typically within about a second.

Searches never see a transaction's own uncommitted writes. Choose how fresh each search must be:

| Consistency | Waits for |
|---|---|
| `stale_ok` (default) | Nothing: the copy's last commit |
| `latest` | Every write committed on the tenant's clusters before the search started |
| `own_writes` | The same as `latest` |

A waiting search gives up after `search.wait_timeout_ms` (2000) with 39826, which is safe to retry. The gauge `sprig_search_index_lag_seconds`, per cluster, shows how far the slowest copy is behind the change feed; alert on it rather than on failed searches. `$text` and indexes converted from `FULLTEXT` always use `latest`. Set the default with `search.default_consistency`.

## Building and rebuilding

A new index, or a new copy, is filled in this order of preference: from a ready copy of the same generation on another worker, from the newest snapshot, or from MariaDB. A build from MariaDB reads the object in pages of 2,000 rows by key as the index's creator, then follows the change feed from where it started. A copy whose position is no longer in the binary log builds again from MariaDB.

`ALTER SEARCH INDEX ... ADD FIELD`, `DROP FIELD` and `REBUILD` raise the index's generation, and new copies are built for it. An operator can do the same with `sprigctl search rebuild acme shop.product_text`. While a copy is being built, searches wait for it up to `search.wait_timeout_ms`, then fail with 39824, which is safe to retry.

The lowest-numbered ready copy writes a snapshot every `search.snapshot_interval_minutes` (60), and the last `search.snapshot_keep` (3) are kept. Operators can list indexes and their copies with `sprigctl search list acme` and `sprigctl search show acme shop.product_text`. See [sprigctl](../tools/sprigctl.md).

## Sharding

An index's partitions are hash buckets of the keys, not shards, and each copy follows the change feed of every cluster that holds the object. When a table group is split, merged or moved, the search index needs no change. The rows that searches return are read back through the normal routing, from whichever shard holds them.

## Security

- A search needs `SELECT` on the indexed object.
- Copies are built without row-level security, and every hit is read back through your session, so you only get rows you may see. For a user bound by a row policy on the object, counts and facets cover only the rows that user may see; past `search.max_window` matches, such a count fails with 39827. Scores still use statistics over all rows.
- Search index files hold the indexed words. Keep `SPRIG_SEARCH_DIR` and `SPRIG_SEARCH_SNAPSHOTS` on encrypted disks.

## Settings

All `search.*` settings are set at dc or global scope and apply live.

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `search.copies` | int | `2` | Copies per index on workers; `0` keeps copies in gateways (0 to 8) |
| `search.commit_interval_ms` | int | `500` | Commit period |
| `search.default_consistency` | text | `stale_ok` | `stale_ok`, `own_writes` or `latest` |
| `search.wait_timeout_ms` | int | `2000` | Longest wait for consistency or a building copy |
| `search.scoring` | text | `global` | `global` or `local` statistics |
| `search.max_window` | int | `10000` | Hits read per query |
| `search.max_expansions` | int | `50` | Terms per fuzzy or prefix term |
| `search.overfetch_factor` | int | `3` | Page multiplier when hidden rows leave a page short |
| `search.highlight_chars` | int | `150` | Highlight fragment length |
| `search.query_timeout_ms` | int | `5000` | Per copy |
| `search.writer_memory_mb` | int | `32` | Indexing memory per partition |
| `search.fulltext_ddl` | text | `convert` | MySQL `FULLTEXT` DDL: `convert`, `mariadb` or `reject` |
| `search.max_indexes_per_object` | int | `4` | Search indexes per object (1 to 64) |
| `search.snapshot_interval_minutes` | int | `60` | Snapshot period |
| `search.snapshot_keep` | int | `3` | Snapshots kept |

Processes that hold copies read these environment variables:

| Variable | Meaning |
|---|---|
| `SPRIG_SEARCH_DIR` | Where copies live. Set it to a persistent volume; the default is a temporary directory, and a copy lost on restart rebuilds. |
| `SPRIG_SEARCH_SNAPSHOTS` | Where a worker writes snapshots |
| `SPRIG_SEARCH_LISTEN` | On a worker: the address it serves copies on, which gives it the search role |
| `SPRIG_SEARCH_ADVERTISE` | On a worker: the address others use, when it differs from `SPRIG_SEARCH_LISTEN` |

## Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `sprig_search_queries_total` | counter | `flavor`, `result` | Searches |
| `sprig_search_query_duration_seconds` | histogram | `phase` | Search time |
| `sprig_search_window_exceeded_total` | counter | none | Searches past `search.max_window` |
| `sprig_search_docs_indexed_total` | counter | `op` | Changes indexed from the feed |
| `sprig_search_commits_total` | counter | none | Copy commits |
| `sprig_search_seeds_total` | counter | `source` | Copies filled, from `mariadb`, `peer` or `snapshot` |
| `sprig_search_remote_queries_total` | counter | `result` | Searches sent to copies on workers |
| `sprig_search_gateway_copies_total` | counter | none | Searches answered from a gateway's own copy |
| `sprig_search_seed_page_seconds` | histogram | none | Time per page read while filling a copy |
| `sprig_search_snapshot_bytes_total` | counter | none | Snapshot bytes written |

See [Metrics](../reference/metrics.md).

## Errors

| Code | Name | When |
|---|---|---|
| 39820 | `ER_SPRIG_SEARCH_INDEX_NOT_FOUND` | No such index |
| 39821 | `ER_SPRIG_SEARCH_NO_INDEX_FOR_MATCH` | No search index on the table covers the `MATCH` columns |
| 39822 | `ER_SPRIG_SEARCH_QUERY_SYNTAX` | A malformed query, operator, definition or statement |
| 39823 | `ER_SPRIG_SEARCH_UNSUPPORTED` | A feature that isn't offered (see below) |
| 39824 | `ER_SPRIG_SEARCH_INDEX_NOT_READY` | The copy is still being built (retryable) |
| 39825 | `ER_SPRIG_SEARCH_UNAVAILABLE` | No copy can answer (retryable) |
| 39826 | `ER_SPRIG_SEARCH_WAIT_TIMEOUT` | The consistency wait timed out (retryable) |
| 39827 | `ER_SPRIG_SEARCH_WINDOW_EXCEEDED` | More matches than `search.max_window` where all are needed |
| 39828 | `ER_SPRIG_SEARCH_ANALYZER_UNKNOWN` | An unknown analyzer, tokenizer, filter or language |
| 39829 | `ER_SPRIG_SEARCH_FIELD_TYPE` | A column type that can't take the field kind, a duplicate field, or an unknown field |
| 39830 | `ER_SPRIG_SEARCH_INDEX_LIMIT` | Over `search.max_indexes_per_object` |

MongoDB clients get the same numbers. See [Errors](../reference/errors.md).

## Not supported

- `WITH QUERY EXPANSION`, `FULLTEXT ... WITH PARSER`.
- Atlas field types `embeddedDocuments`, `geo`, `knnVector` and `vector`; operators `embeddedDocument`, `geoShape`, `geoWithin`, `near`, `moreLikeThis`, `knnBeta` and `span`; `text.synonyms` (use a custom analyzer with synonyms); score options other than `score.boost.value`.
- Sorting search results by anything other than score, in REST and `$search`.
- Facets from SQL; use `$searchMeta` or the REST `facets` field.
- Per-query languages in `$text`, and `language_override`.
- Accelerating `LIKE '%...%'` with `SUBSTRING` fields.
- Indexing hash fields or other non-string values of key-value namespaces, and Redis `FT.*` commands.

## Related

- [Vectors](vectors.md)
- [Documents](documents.md)
- [Search and vectors in SQL](../sql/search-and-vectors.md)
- [Change feed](../concepts/change-feed.md)
- [Cross-model queries](cross-model-queries.md)
