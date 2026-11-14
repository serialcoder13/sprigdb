---
title: Searching the docs
audience: [user, dba, operator]
sidebar:
  order: 4
---

# Searching the docs

Every SprigDB release carries these pages as a search index, the **docs pack**. A running SprigDB loads it, so anyone signed in can search the docs of the exact release they use: from SQL, over REST, offline, with nothing sent anywhere.

The pack holds the pages cut into chunks of a few hundred words, each with its page, its heading path, its audience, the exact identifiers it mentions (error codes and names, SQLSTATEs, setting keys) and an embedding from a small model that ships with SprigDB (BAAI/bge-small-en-v1.5). Searches combine keyword search with similarity of meaning, so "why is replication slow" finds the page about replica lag even when it uses other words, and `39989` finds that error's entry exactly.

## How it gets there

At start, the controller checks the pack's SHA-256 and loads it into every cluster of the data centre, into a database no tenant can write to, and creates a read-only account for searching it. It loads each release once: a later start only checks that the rows still match the pack, and loads them again if they were changed. After an upgrade, the previous release's docs are kept beside the new one (`docs.keep_versions`, 2), so a rollback has its own docs at once.

Until a cluster has the release's docs, searches use another cluster's copy; if none has them, they fail with 39989 `ER_SPRIG_DOCS_UNAVAILABLE` (retryable). The controller logs the events `docs.pack.loaded`, `docs.pack.tampered` (rows changed since the load, reloaded) and `docs.pack.unavailable` (no valid pack found).

## From SQL: the sprig_docs database

Every session can read `sprig_docs.chunks`, on any port that speaks SQL, whatever its grants. Nobody can change it.

```sql test
DESCRIBE sprig_docs.chunks;
```

```text output
Field	Type	Null	Key	Default	Extra
version	varchar(32)	NO	PRI	NULL	
chunk_id	binary(16)	NO	PRI	NULL	
page	varchar(255)	NO		NULL	
title	varchar(255)	NO		NULL	
heading_path	varchar(1024)	NO		NULL	
audience	set('user','dba','operator')	NO		NULL	
text	mediumtext	NO	MUL	NULL	
tokens	smallint(5) unsigned	NO		NULL	
terms	text	NO		NULL	
embedding	vector(384)	NO	MUL	NULL	
```

| Column | What |
|---|---|
| `version` | The release. `sprig_docs.chunks` shows only the running release's rows |
| `chunk_id` | The chunk's id, the same in every copy of the pack |
| `page` | The page, as a path under the docs, such as `security/tenant-administration.md` |
| `title` | The page's title |
| `heading_path` | The page title and the headings above the chunk, joined by ` > ` |
| `audience` | Who the page is for: `user`, `dba`, `operator` |
| `text` | The chunk's Markdown |
| `tokens` | Its length in the model's tokens |
| `terms` | The exact identifiers the chunk mentions, for keyword search |
| `embedding` | The chunk's embedding, 384 numbers |

Keyword search uses the full-text index on `text`:

```sql test
SELECT page, heading_path
FROM sprig_docs.chunks
WHERE MATCH(text) AGAINST ('suspend tenant')
LIMIT 3;
```

Search by meaning with `sprig_docs.embed('<text>')`, which gives a question's embedding from the same model, and the vector index:

```sql
SELECT page, heading_path
FROM sprig_docs.chunks
ORDER BY VEC_DISTANCE_COSINE(embedding, sprig_docs.embed('why is replication slow'))
LIMIT 5;
```

Filter by audience as with any `SET` column:

```sql test
SELECT COUNT(*) > 0 AS has_operator_pages
FROM sprig_docs.chunks
WHERE FIND_IN_SET('operator', audience);
```

```text output
has_operator_pages
1
```

The rules:

- Reads work: `SELECT`, `WITH`, `EXPLAIN` of a `SELECT`, and `DESCRIBE`, `SHOW COLUMNS`, `SHOW INDEX` and `SHOW CREATE TABLE` of `sprig_docs.chunks`.
- Writes and DDL fail with 1044, as writes to `information_schema` do on MySQL.
- `USE sprig_docs` fails with 39051: name the table as `sprig_docs.chunks`.
- One statement can't join the docs with your own tables: it fails with 1142.
- `sprig_docs.embed` takes a string literal only. It needs the model on the gateway (`docs.embed_queries` on, and the model in `docs.model_dir`); without it, it fails with 39989.
- `sprig_docs` is a reserved name: no tenant can create a database with it, on any protocol.
- Prepared statements, MongoDB and Redis can't read `sprig_docs`; use SQL text or REST.

```sql test-error
DELETE FROM sprig_docs.chunks;   -- 1044: the docs are read-only
```

## Over REST: POST /v1/docs:search

`POST /v1/docs:search` runs the hybrid search: the 40 best chunks by keywords and the 40 nearest by meaning, merged by reciprocal-rank fusion, at most two chunks per page. A question that names an exact identifier (an error code or name, a SQLSTATE, a setting key) gives keyword matches three times the weight, since the embedding model doesn't know those names. Any signed-in user can call it: Basic authentication, a Studio session or an API key (see [REST and gRPC](../connect/rest-and-grpc.md)).

| Field | Required | What |
|---|---|---|
| `query` | yes | The question or words to search for |
| `audience` | no | `user`, `dba` or `operator`: only chunks for that audience |
| `limit` | no | How many hits, at most 50. Default `docs.search_limit` (8) |
| `version` | no | A release whose docs are loaded. Default: this gateway's own release, else the newest loaded |

```sh
curl -s -u acme.app:'<password>' -X POST https://sprig.example.com/v1/docs:search \
  -H 'content-type: application/json' \
  -d '{"query": "how do I suspend a tenant", "limit": 2}'
```

```json
{
  "version": "0.9.0",
  "mode": "hybrid",
  "hits": [
    {
      "chunk_id": "5c7bd6d31707d76ff6de015634a48081",
      "page": "security/tenant-administration.md",
      "title": "Tenant administration",
      "heading_path": "Tenant administration > Suspension",
      "audience": ["dba", "operator"],
      "text": "…",
      "score": 0.0328,
      "bm25_rank": 1,
      "vector_rank": 1
    }
  ]
}
```

- `mode` is `hybrid`, or `keyword` when the question wasn't embedded: `docs.embed_queries` is off, or the gateway couldn't load the model (counted in `sprig_docs_search_degraded_total`). Keyword search still answers.
- `bm25_rank` and `vector_rank` are the hit's place in each list, or `null` when it wasn't in that list. `score` is the fused score.
- An empty `query`, an unknown audience, or a `limit` that isn't a positive integer fails with 400. A `version` that isn't loaded fails with 503 and 39989.

## Settings

All are operator settings, live, at data-centre or global scope. See [Settings](../reference/settings.md).

| Setting | Default | What |
|---|---|---|
| `docs.enabled` | `true` | Load the docs and serve docs search |
| `docs.pack_path` | empty | The pack the controller loads; empty: `sprig-docs-<version>.pack` next to the `sprigdb` binary |
| `docs.keep_versions` | `2` | Releases kept on each cluster, the running one included |
| `docs.search_limit` | `8` | Hits returned when a request gives no `limit` |
| `docs.embed_queries` | `true` | Embed questions for hybrid search; off: keyword search only, and the model is never loaded |
| `docs.embed_threads` | `2` | CPU threads one query's embedding may use |
| `docs.model_dir` | empty | The model's directory (`config.json`, `tokenizer.json`, `model.safetensors`); empty: `models/bge-small-en-v1.5` next to the `sprigdb` binary |

The gateway loads the model on the first search that needs it.

## Monitoring

| Metric | What |
|---|---|
| `sprig_docs_pack_info{version,sha}` | The pack the controller verified and loads |
| `sprig_docs_load_seconds` | Time to load the pack into one cluster |
| `sprig_docs_verify_failures_total` | Changed or missing rows found and reloaded |
| `sprig_docs_search_total{result}` | Searches by result: `ok`, `empty`, `invalid`, `refused`, `unavailable`, `error` |
| `sprig_docs_search_seconds{mode}` | Search time: `hybrid`, `keyword` or `sql` |
| `sprig_docs_search_degraded_total` | Searches answered by keywords only because the model couldn't run |

See [Metrics](../reference/metrics.md).

## Troubleshooting

| Symptom | What to do |
|---|---|
| 39989 on every search | The docs aren't loaded on any cluster. Check `docs.enabled`, that the pack is next to the binary or at `docs.pack_path`, and the controller's `docs.pack.unavailable` event. After an upgrade, retry in a minute |
| 39989 from `sprig_docs.embed` only | The gateway has no model: check `docs.embed_queries` and `docs.model_dir` |
| `mode` is always `keyword` | Same cause; `sprig_docs_search_degraded_total` rises |
| Results from an older release | This gateway's release isn't loaded yet; pass `version` or wait for the controller |
