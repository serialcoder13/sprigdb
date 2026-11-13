---
title: Sinks
audience: [user, dba, operator]
sidebar:
  order: 11
---

# Sinks

A sink delivers records from SprigDB to a system outside it, continuously, with no code for you to run: a webhook, a Kafka cluster, an S3 bucket, a MySQL, MariaDB or PostgreSQL database, or a script you write. Use a sink when another system must see every event or change; SprigDB handles batching, ordering, retries, dead letters and pausing.

```sql
CREATE SINK orders_to_billing
  FROM STREAM order_events
  TO WEBHOOK CONNECTION billing_api PATH '/hooks/orders'
  WITH (format = 'cloudevents', batch_max_records = 100);
```

A sink has three parts:

- a **source**: an [event stream](streams.md), a [queue](queues.md), or the changes of tables;
- a **connection**: where the destination is and how to log in to it, with write-only credentials;
- a **target**: what to send and how (`WEBHOOK`, `KAFKA`, `S3`, `DATABASE` or `SCRIPT`).

To react to a record by writing into SprigDB itself, use a trigger instead (see [Logic](logic.md)): a trigger commits its writes and its position in one transaction. Sinks are for destinations outside SprigDB.

## How sinks are stored and run

Connections and sinks belong to one database of one tenant. Their definitions are rows in the tenant's catalog, in MariaDB: connections, sealed credentials, sinks, sink tasks, `USAGE` grants and dead letters. Creating a sink makes no tables in your databases.

A sink runs as one or more **tasks** in `sprigdb worker` processes of the tenant's home data centre. Each task reads its share of the source from the source's own positions:

| Source | Unit a task reads | Position kept in |
|---|---|---|
| Stream or change topic | Up to `sink.partitions_per_task` (4) partitions | The consumer group `__sprig_sink_<sink id>`, like any Kafka consumer group |
| Queue | The whole queue, one task | The queue itself: each message is taken under a lease and acknowledged once delivered |
| Fan-in | Member instances, up to `sink.fanin_instances_per_task` (256) instance partitions per task | One position per member instance |

Inside a task, records go through the pipeline:

1. Read while the task holds less than `sink.max_inflight_bytes` (64 MiB). The source is durable, so a slow destination makes the task wait, never drop records.
2. Apply `WHERE`, `COLUMNS` and `TRANSFORM`.
3. Put each record on a **lane** by a hash of its key. There are `sink.lanes` (8) lanes.
4. Cut each lane's records into batches at `sink.batch_max_records` (500), `sink.batch_max_bytes` (4 MiB) or `sink.batch_max_wait_ms` (1,000 ms), whichever comes first.
5. Deliver each lane's batches in order, one batch in flight per lane, lanes in parallel.
6. Retry, split or dead-letter what fails.
7. Move the source position up to the lowest record not yet done.

A task holds a lease; if its worker dies, another worker takes the task over and resumes from the last settled position.

## Connections

A connection names a destination, its options and its credentials. Create one per destination and reuse it in any number of sinks of the same database.

```sql
CREATE CONNECTION billing_api TYPE WEBHOOK
  WITH (url = 'https://billing.example.com', auth = 'bearer')
  CREDENTIALS (token = '<api-token>', signing_secret = '<signing-secret>');
```

The full grammar:

```sql
CREATE CONNECTION [IF NOT EXISTS] [db.]name
  TYPE WEBHOOK | KAFKA | S3 | MYSQL | MARIADB | POSTGRESQL | POSTGRES
  [WITH (option = value, ...)]
  [CREDENTIALS (name = 'value', ...)]

ALTER CONNECTION [db.]name SET (option = value, ...)
ALTER CONNECTION [db.]name RESET (option, ...)
ALTER CONNECTION [db.]name SET CREDENTIALS (name = 'value', ...)
DROP CONNECTION [IF EXISTS] [db.]name

SHOW CONNECTIONS [FROM | IN db]
SHOW CREATE CONNECTION [db.]name
SHOW PLATFORM CONNECTIONS

GRANT USAGE ON [PLATFORM] CONNECTION [db.]name TO user
REVOKE USAGE ON [PLATFORM] CONNECTION [db.]name FROM user
```

- `MARIADB` is the same type as `MYSQL`, and `POSTGRES` the same as `POSTGRESQL`.
- `WITH` holds options that aren't secret. `CREDENTIALS` holds secrets only, and each value must be a quoted string (39216 otherwise).
- Creating, altering and dropping connections needs `SPRIG_SINK_ADMIN`.
- `DROP CONNECTION` fails with `ER_SPRIG_SINK_CONNECTION_IN_USE` (39215) while a sink uses it.
- `SHOW CONNECTIONS` lists `Database`, `Name`, `Type`, `Hosts`, `Credentials` (each credential's name and version, never its value) and `Residency` (the tags its hosts matched).
- `GRANT USAGE` lets a user name the connection in a sink. Granting it needs `SPRIG_SINK_ADMIN` or `SPRIG_TENANT_ADMIN`.

### Credentials are write-only

You can set and replace a credential, but nothing ever returns its value: no statement, API, log or Studio view.

- The gateway seals each value with AES-256-GCM under a key derived for your tenant from the gateway's master key file (`[gateway] workload_key_file`), before the statement reaches logs, statement history or traces. A gateway without that file refuses credentials with `ER_SPRIG_SINK_CREDENTIALS_INVALID` (39216).
- Logs and the process list show the statement with every credential as `'***'`.
- `SHOW CREATE CONNECTION` prints each credential as `'***'` with its version, when it was set and by whom:

  ```text
  CREATE CONNECTION `billing_api` TYPE WEBHOOK WITH (auth = 'bearer', url = 'https://billing.example.com')
    CREDENTIALS (token = '***' /* version 2, set 2026-11-12 09:14:03.120 by admin */, ...)
  ```

- Only sink workers open credentials, in memory, when a task connects.

To rotate a credential, set a new value:

```sql
ALTER CONNECTION billing_api SET CREDENTIALS (token = '<new-api-token>');
```

The new value gets the next version. Running tasks switch at their next connection; the previous version stays usable for `sink.credential_previous_hours` (24), so work in flight can finish.

### Connection options by type

| Type | Option | Default | Meaning |
|---|---|---|---|
| Webhook | `url` | required | Base URL; a sink's `PATH` is appended |
| Webhook | `auth` | `none` | `none`, `bearer`, `basic`, `header`, `oauth2_client_credentials` or `mtls` |
| Webhook | `auth_header` | | Header name for `auth = 'header'` (an API key header) |
| Webhook | `token_url`, `scope`, `audience` | | For `oauth2_client_credentials` |
| Webhook | `signature` | `sprig` | `sprig`, `standard` or `none`; a sink's own `signature` option wins |
| Webhook | `http2` | `auto` | `auto` (ALPN), `off` or `prior_knowledge`; a sink's own `http2` option wins |
| Kafka | `bootstrap` | required | Comma-separated `host:port` list (port 9092 when left out) |
| Kafka | `security` | `plaintext` | `plaintext`, `ssl`, `sasl_plaintext` or `sasl_ssl` |
| Kafka | `sasl_mechanism` | `PLAIN` | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512` |
| Kafka | `request_timeout_ms` | 30000 | Per request |
| Kafka | `offsets_topic` | `__sprig_sink_offsets` | Positions topic of exactly-once sinks |
| S3 | `endpoint` | required | `https://s3.<region>.amazonaws.com`, a MinIO URL, Cloudflare R2, OCI or Google Cloud Storage's S3 API |
| S3 | `bucket` | required | The bucket |
| S3 | `region` | `us-east-1` | Signing region |
| S3 | `path_style` | off for `*.amazonaws.com`, on otherwise | Path-style addressing |
| S3 | `credentials` | `static` | Only `static` works; `assume_role`, `web_identity` and `instance` are refused with 39228 |
| MySQL, PostgreSQL | `host` | required | Server host |
| MySQL, PostgreSQL | `port` | 3306 or 5432 | Server port |
| MySQL, PostgreSQL | `database` | | Default database or schema |
| MySQL, PostgreSQL | `tls` | `verify_full` | `off`, `required` or `verify_full` |
| All with TLS | `ca_pem` | system roots | A CA certificate in PEM, for private CAs |

Credential names each type takes (any other is 39216):

| Type | Credentials |
|---|---|
| Webhook | `token` (bearer), `username` and `password` (basic), `header_value` (header), `client_id` and `client_secret` (OAuth2), `signing_secret`, `tls_cert` and `tls_key` (mTLS, PEM) |
| Kafka | `username`, `password` (SASL), `tls_cert`, `tls_key`, `client_secret` |
| S3 | `access_key_id`, `secret_access_key`, `session_token` |
| MySQL, PostgreSQL | `username`, `password`, `tls_cert`, `tls_key` |

### Where sinks may connect: the allowlist

A connection can reach only hosts on the tenant's `sink.destination_allowlist`. The default is empty, so **no sink can deliver anywhere until a tenant administrator lists the destinations**:

```sql
SPRIG SET (sink.destination_allowlist = '[
  {"host": "billing.example.com"},
  {"host": "*.s3.eu-central-1.amazonaws.com", "tags": {"jurisdiction": "EU"}},
  {"host": "10.20.0.15:5432", "tags": {"jurisdiction": "EU"}}
]');
```

- An entry is a host, `host:port`, `*.domain` or an address, as a JSON string or a `{"host": ..., "tags": {...}}` object. The list can also be comma-separated text.
- An entry without a port allows ports 80 and 443, plus the connection type's default port (3306, 5432, 9092 or 9093).
- Private, loopback, link-local, shared and unspecified addresses are refused unless an entry names the address itself.
- SprigDB resolves the name once, checks the address and connects to that address, so a second DNS answer can't swap it.
- Hosts are checked at `CREATE CONNECTION`, at `ALTER CONNECTION`, at every task start and at every connection a target or script opens. A host not on the list fails with `ER_SPRIG_SINK_DESTINATION_NOT_ALLOWED` (39213).

Until an administrator lists the host, creating a connection to it fails:

```sql test-error
CREATE CONNECTION crm_api TYPE WEBHOOK WITH (url = 'https://crm.example.com/v2');
```

**Residency.** When the tenant or an ancestor has a residency policy, the destination's tags (from its allowlist entry, or SprigDB's built-in table for cloud endpoints whose names state their region) must satisfy it. A destination without tags, or with tags the policy refuses, fails with `ER_SPRIG_SINK_RESIDENCY` (39214). See [Residency](../security/residency.md).

**Kinds.** `sink.enabled` (on) turns sinks off for a tenant, data centre or the platform, and `sink.allowed_kinds` (`webhook,kafka,s3,database,script`) limits the target kinds. Either refusal is `ER_SPRIG_SINK_DISABLED` (39230).

### Platform connections

A platform operator can register a shared destination whose secrets live in files on the workers, or in Kubernetes Secrets mounted as files, and grant it to tenants. Tenants use it without ever seeing the credentials.

```sh
sprigctl sink platform-connections register kafka_main --type kafka \
  --option bootstrap=k1.internal:9093,k2.internal:9093 --option security=sasl_ssl \
  --option sasl_mechanism=SCRAM-SHA-512 --tags jurisdiction=EU \
  --description 'Platform Kafka, EU' --hide-option bootstrap \
  --secret username=file:kafka_main/username --secret password=file:kafka_main/password
sprigctl sink platform-connections grant kafka_main --tenant acme
```

In the tenant, a tenant administrator grants `USAGE` to the users who may name it, and a sink names it with `PLATFORM CONNECTION`:

```sql
GRANT USAGE ON PLATFORM CONNECTION kafka_main TO app;

CREATE SINK orders_out FROM STREAM order_events
  TO KAFKA PLATFORM CONNECTION kafka_main TOPIC 'acme.orders';
```

- Secret files are read from `sink.platform_secret_dir` (`/etc/sprigdb/platform-secrets`); a path can't leave it. A worker that can't read a file stops the sink's tasks with `ER_SPRIG_SINK_PLATFORM_SECRET_UNAVAILABLE` (39234).
- The operator vetted the hosts by registering them, so platform connections don't need the tenant's allowlist. The tenant's residency policy still applies.
- `SHOW PLATFORM CONNECTIONS` shows `Name`, `Type`, `Description`, `Tags` and the options the operator didn't hide. Tenant and sink administrators see every connection granted to the tenant; other users see those they hold `USAGE` on. With `--visibility name_only`, only the name and type show.
- Naming a platform connection the tenant isn't granted fails with `ER_SPRIG_SINK_PLATFORM_CONNECTION_NOT_GRANTED` (39233). A revoked grant fails the sink's tasks with 39233 at their next start.

See [sprigctl](../tools/sprigctl.md) for `register`, `grant`, `revoke`, `list` and `show`.

## Create a sink

```sql
CREATE SINK [IF NOT EXISTS] [db.]name
  FROM STREAM s | QUEUE q | CHANGES OF t [, t2 ...]
     | STREAM s FAN IN [UNDER tenant] | QUEUE q FAN IN [UNDER tenant]
  [WHERE condition]
  [COLUMNS (c1, ...) | EXCEPT COLUMNS (c1, ...)]
  [TRANSFORM function]
  TO WEBHOOK [PLATFORM] CONNECTION c [PATH '/path']
   | TO KAFKA [PLATFORM] CONNECTION c TOPIC 'topic' [KEY 'json path']
   | TO S3 [PLATFORM] CONNECTION c PREFIX 'template'
   | TO DATABASE [PLATFORM] CONNECTION c TABLE 'name or template'
   | TO SCRIPT LANGUAGE javascript | kirun | lua
       [USING CONNECTIONS ([PLATFORM] c1, ...)] AS $$ ... $$
  [GUARANTEE AT LEAST ONCE | EXACTLY ONCE]
  [DEAD LETTER TO STREAM d]
  [START AT LATEST | EARLIEST | SNAPSHOT | TIMESTAMP 'iso time']
  [WITH (option = value, ...)]
```

Creating a sink needs:

- `SPRIG_SINK_ADMIN` (granted by name only; `GRANT ALL` doesn't include it);
- `SELECT` on the source (and `DELETE` too for a queue, because a sink consumes it);
- `USAGE` on each connection it names, or owning the connection, or `SPRIG_TENANT_ADMIN`.

A tenant may hold `sink.max_per_tenant` (50) sinks; one more fails with `ER_SPRIG_SINK_QUOTA` (39226).

**A sink runs as its owner**, the user who created it (or the user named by `ALTER SINK ... OWNER TO`), with the owner's privileges and row-level security, whoever later pauses or alters it.

### Sources

| Source | What it reads | Notes |
|---|---|---|
| `STREAM s` | An event stream, or a change topic such as `` `orders.changes` `` | Partitions are read in parallel, each in offset order |
| `QUEUE q` | A durable queue of the sink's database (its virtual host) | Needs the queue engine (AMQP) running in the data centre. The owner needs `SELECT` and `DELETE` on the queue |
| `CHANGES OF t, ...` | The committed changes of one or more tables of the database | Reads each table's change topic, which must be on (`ALTER TABLE t SPRIG SET (stream.change_topic = on)`); 39218 names the statement otherwise. See [Change feed](../concepts/change-feed.md) |
| `... FAN IN` | Every member tenant's instance of an inherited stream or queue | See [Fan-in sinks](#fan-in-sinks) |

A change record has `op` (`c`, `u`, `d`), `before` and `after`. Owners bound by row-level security get only the changes their policies show them, as for any reader of a change topic; see [Row-level security](../security/row-level-security.md).

A queue sink takes each message for `sink.queue_lease_ms` (300,000 ms); a message not settled by then goes back to the queue. When a task's worker has died, its successor gives the dead task's messages back at once. The record's value is the message body, its key the routing key, and its headers the application headers plus `amqp.routing_key`, `amqp.exchange`, `amqp.delivery_count`, `amqp.message_id`, `amqp.content_type`, `amqp.correlation_id` and `amqp.type`. If the queue is dropped and created again under the same name, the sink gives the messages back and fails with 39223 rather than read the new queue.

### Filter, project and transform

**`WHERE`** is a SQL condition evaluated on each record in the worker:

- stream and queue records see `key`, `value`, `headers` and `ts`, with JSON paths: `value->>'$.type' = 'order.paid'`, `headers->>'$.source' = 'checkout'`;
- change records see the row's columns (the after image, or the before image for deletes) and `_op` (`c`, `u`, `d`).

The condition may use literals, column names, `AND`, `OR`, `NOT`, comparisons, `+ - * /`, `->` and `->>`, `IS [NOT] NULL`, `[NOT] IN`, `[NOT] BETWEEN`, `[NOT] LIKE` and parentheses. Anything else, such as a function call or a subquery, is refused when you create the sink (39218). A condition that is unknown, because of a `NULL`, drops the record.

**`COLUMNS (...)`** keeps only the named columns of change records, and **`EXCEPT COLUMNS (...)`** removes them.

**`TRANSFORM f`** runs a JavaScript, KIRun or Lua function `f` of the sink's database on each record after `WHERE` and the projection. It receives the record (`key`, `value`, `headers`, `op`, `before`, `after`, `ts`) and returns:

| Return | Effect |
|---|---|
| `null` or `[]` | The record is dropped (settled, not delivered) |
| An object with `key`, `value` or `headers` | The record with those replaced |
| Any other object | The record with that object as its value |
| An array of those | Several records; their idempotency keys get `#0`, `#1`, ... |
| An error or a timeout | The record is dead-lettered with class `transform` |

A transform has no effects: its text may not use `db`, `fetch` or other calls that read or write data (`ER_SPRIG_SINK_TRANSFORM_INVALID`, 39219). Each record has `sink.transform_budget_ms` (50 ms). Without `transform_version`, the sink uses the function's active version at each task start; `WITH (transform_version = 3)` pins version 3. A function a sink uses can't be dropped (`ER_SPRIG_LOGIC_IN_USE`, 39875): alter or drop the sink first. See [Logic](logic.md) for writing functions.

### Where to start

| `START AT` | Streams and change sources |
|---|---|
| `LATEST` (default) | From the moment the sink was created, so records published while its first task was starting are still delivered |
| `EARLIEST` | From the oldest record still retained |
| `SNAPSHOT` | The same as `EARLIEST` for now: there is no initial copy of the table's existing rows |
| `TIMESTAMP '2026-11-09T10:00Z'` | From the first record at or after that UTC time (`YYYY-MM-DD[THH:MM[:SS[.fff]]][Z]`) |

A queue sink always starts at the queue's head.

## Targets

### Webhook

```sql
CREATE SINK order_hooks FROM STREAM order_events
  WHERE value->>'$.type' = 'order.paid'
  TO WEBHOOK CONNECTION billing_api PATH '/hooks/orders'
  WITH (format = 'ndjson', batch_max_records = 50);
```

Each batch is one `POST` to the connection's `url` plus `PATH`, over HTTP/1.1 or HTTP/2.

| Sink option | Default | Meaning |
|---|---|---|
| `format` | `json` | `json` (an array of records), `ndjson` (one record per line), `cloudevents` (a CloudEvents batch, `application/cloudevents-batch+json`), `cloudevents_binary` (one request per record, CloudEvents attributes in `ce-` headers) |
| `batch` | on | `off` sends one record per request |
| `signature` | `sprig` | `sprig`, `standard` or `none` |
| `partial_results` | off | Read per-record outcomes from the response body |
| `http2` | `auto` | `auto`, `off` or `prior_knowledge` |
| `http_connect_timeout_ms`, `http_timeout_ms` | 5000, 30000 | Connect and request timeouts |

In the `json` and `ndjson` formats each record is an object:

```json
{"key": "7001",
 "value": {"type": "order.paid", "order_id": 7001},
 "headers": {"source": "checkout"},
 "ts": 1762508100000,
 "idempotencyKey": "s/88/5/1205/3",
 "position": "5/1205/3"}
```

A change record also has `op`, `before` and `after`, and a fan-in record has `tenant`. A value that is JSON is sent as JSON, other UTF-8 as a string, and other bytes as `{"base64": "..."}`.

Every request carries `Content-Type`, `Sprig-Idempotency-Key` (the record's key, or the batch id for a batch), `Sprig-Sink`, `Sprig-Source` and a W3C `traceparent`. In CloudEvents, `id` is the idempotency key, `source` is `/sprig/<tenant id>/<database>/<source>`, `type` is `sprig.change.create`, `.update`, `.delete` for changes, else the record's `type` header, else `sprig.event`, and `subject` is the key.

**Signatures** use the connection's `signing_secret` credential. A sink with the default `signature = 'sprig'` and no `signing_secret` can't start, so set the secret or `signature = 'none'`.

- `sprig` adds `Sprig-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<body>">`.
- `standard` follows the Standard Webhooks specification: `webhook-id` (the idempotency key or batch id), `webhook-timestamp` and `webhook-signature: v1,<base64 HMAC-SHA256 of "<id>.<timestamp>.<body>">`. A secret starting `whsec_` is decoded from base64.

Receivers should reject timestamps older than a few minutes.

**Authentication** comes from the connection: a bearer token, HTTP Basic, a named header with a credential value, OAuth2 client credentials (the token is fetched from `token_url`, cached until 60 seconds before it expires, and refreshed once on a 401), or mutual TLS with `tls_cert` and `tls_key`.

**Responses:**

| Response | Outcome |
|---|---|
| 2xx | Delivered. With `partial_results = on`, a 200 or 207 body `{"results": [{"status": "ok" \| "retry" \| "fail", "reason": "..."}]}`, one entry per record in order, gives each record's outcome |
| 408, 429, 401, 403, 5xx, connection or TLS failure, timeout | Destination-wide failure: the batch is retried with backoff, honouring `Retry-After` in seconds or as an HTTP date |
| 413 | The batch is split in half and resent; a single record refused with 413 is dead-lettered |
| 3xx | Permanent failure: redirects are never followed, because they could lead off the allowlist |
| Other 4xx | Permanent failure of the batch: SprigDB halves it until the failing record is alone, then dead-letters that record |

### Kafka

```sql
CREATE SINK orders_to_kafka FROM STREAM order_events
  TO KAFKA CONNECTION kafka_eu TOPIC 'orders.v1' KEY '$.order_id'
  WITH (value_format = 'json', compression = 'zstd');
```

| Option | Default | Meaning |
|---|---|---|
| `KEY 'path'` | the source key | A JSON path into the value, such as `'$.order_id'` (the after image for change records), to use as the Kafka key |
| `value_format` | `json` | `json`, `raw` (a stream record's value bytes unchanged) or `cloudevents` (JSON value, CloudEvents attributes in `ce_` headers) |
| `partitioner` | `murmur2` | `murmur2` on the key (Kafka's default partitioner), `source` (keep the source partition) or `round_robin` |
| `compression` | `zstd` | `none`, `gzip`, `zstd`, `snappy` or `lz4` |

A change record's value is `{"op", "before", "after", "ts_ms"}`. Records keep their source headers and gain `sprig.idempotency.key` and `sprig.source`. The producer is idempotent (`acks = all`), with one request in flight per producer. Brokers are reached over TLS, SASL `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`, as the connection says.

### S3 and compatible stores

```sql
CREATE SINK orders_to_lake FROM CHANGES OF orders
  TO S3 CONNECTION lake PREFIX 'orders/{yyyy}/{MM}/{dd}/{HH}/'
  WITH (format = 'jsonl', compression = 'gzip');
```

`PREFIX` is a template. The time variables come from the record's own timestamp, never the clock, so a retried batch lands in the same place:

| Variable | Value |
|---|---|
| `{yyyy}`, `{MM}`, `{dd}`, `{HH}`, `{mm}` | Year, month, day, hour, minute of the record |
| `{db}`, `{source}`, `{table}` | Database, source name, table |
| `{op}` | `c`, `u`, `d` for change records |
| `{partition}` | Source partition |
| `{key_hash}` | A hash of the key modulo `key_buckets` (16) |
| `{tenant}` | The member tenant's id, for fan-in sinks |

Any other variable is refused (39218). Each batch writes one object per rendered prefix and partition, named `<prefix><sink id>-<task>-p<partition>-<first offset>-<last offset>.<ext>`, with 20-digit offsets.

| Option | Default | Meaning |
|---|---|---|
| `format` | `jsonl` | `jsonl`, `csv` or `parquet`; anything else is `ER_SPRIG_SINK_FORMAT_UNSUPPORTED` (39231) |
| `compression` | `gzip` | `none`, `gzip` or `zstd` for JSON Lines and CSV. Parquet compresses its pages with zstd |
| `columns` | from the change source | Required for CSV and Parquet from streams and queues (39231 otherwise): `name[:type][=$.path], ...`, types `string`, `int64`, `int32`, `double`, `bool`, `json`, `timestamp_ms` |
| `key_buckets` | 16 | Buckets for `{key_hash}` |

A JSON Lines line is the row (or the value) with an `_sprig` object holding `key` (the idempotency key), `op`, `ts` and `record_key`. Objects above `sink.s3.part_bytes` (16 MiB) are uploaded in parts; every upload uses `If-None-Match: *`, so a resent batch never makes a second object. Incomplete multipart uploads older than an hour are aborted hourly; a bucket lifecycle rule that aborts incomplete uploads is a good backstop.

### Databases: MySQL, MariaDB and PostgreSQL

```sql
CREATE CONNECTION reporting TYPE POSTGRESQL
  WITH (host = 'pg.internal.example.com', port = 5432, database = 'reporting', tls = 'verify_full')
  CREDENTIALS (username = 'sprig_sink', password = '<password>');

CREATE SINK orders_to_pg FROM CHANGES OF orders
  TO DATABASE CONNECTION reporting TABLE 'public.orders'
  GUARANTEE EXACTLY ONCE
  WITH (schema_policy = 'create', delete_mode = 'soft');
```

`TABLE` is a name or a template with `{table}`, `{db}` and `{source}`, for example `'analytics.{table}'` for a sink over several tables. Columns map by name.

| Option | Default | Meaning |
|---|---|---|
| `mode` | `upsert` | `upsert` by the destination's primary key, or `append` (every record a new row) |
| `delete_mode` | `delete` | `delete`, `soft` (sets `_sprig_deleted`) or `ignore` |
| `schema_policy` | `strict` | `strict`: the table and every column must exist; `add_columns`: missing columns are added; `create`: missing tables are created too |
| `column_map` | | A JSON object from source column to destination column |
| `primary_key` | the source's key | Key columns for tables that `create` makes |
| `db.batch_rows` | 500 | Rows per statement and per destination transaction |

- **Upsert** rows carry `_sprig_ver`, the record's version. An upsert replaces a row only when its version is newer, so retries and dead-letter replays never move a row backwards. A soft delete uses the same guard. A hard delete leaves no version, so `soft` is safer where replays are expected.
- **Append** rows carry `_sprig_op`, `_sprig_ts` and `_sprig_idem` (unique), so the destination drops duplicates.
- With `strict`, the table must already have these `_sprig_` columns. `add_columns` and `create` add them.
- A table that doesn't fit stops the batch with `ER_SPRIG_SINK_SCHEMA_MISMATCH` (39224), retried until you fix the table or change the policy.
- Lost connections, timeouts, lock waits and deadlocks are retried. A row the destination refuses, such as a constraint violation, fails the batch, which SprigDB halves until it can dead-letter the one bad row.

### Scripts

A script sink runs your JavaScript, KIRun or Lua code for each batch. SprigDB still provides positions, batching, lanes, retries, dead letters, pausing and idempotency keys; the script only turns a batch into calls.

```sql
CREATE SINK orders_to_crm
  FROM CHANGES OF orders WHERE _op IN ('c', 'u')
  TO SCRIPT LANGUAGE javascript USING CONNECTIONS (crm_api) AS $$
    export async function deliver(batch, ctx) {
      const body = JSON.stringify(batch.records.map(r => ({ id: r.after.order_id,
                                                            status: r.after.status })));
      const res = await fetch("https://crm.example.com/v2/orders:batchUpsert", {
        method: "POST", connection: "crm_api",
        headers: { "Idempotency-Key": batch.id }, body });
      if (res.status === 429 || res.status >= 500)
        return { retry: true, retryAfterMs: ctx.retryAfter(res) };
      if (res.status !== 200) return { fail: "crm answered " + res.status };
    }
  $$
  WITH (batch_max_records = 200);
```

`deliver(batch, ctx)` gets `batch.id` (stable across retries), `batch.attempt` and `batch.records` (each as in the webhook `json` format). It returns:

| Return | Meaning |
|---|---|
| Nothing, `true` or `{ok: true}` | Every record delivered |
| `{retry: true, retryAfterMs}` | Retry the whole batch |
| `{fail: reason}` | The batch failed; SprigDB halves it to find the bad record |
| `{results: [...]}` | One entry per record: `"ok"`, `"retry"`, `{retry: reason}` or `{fail: reason}` |

A thrown error retries the batch. A thrown `SinkPermanentError` (JavaScript), `error({permanent = true, reason = ...})` (Lua) or the `permanentError` event (KIRun) fails it permanently. A malformed result is `ER_SPRIG_SINK_SCRIPT_FAILED` (39220).

What a script can reach:

| Call | What it does |
|---|---|
| `fetch(url, {connection: 'c', ...})` (Lua `sprig.fetch`, KIRun `Sprig.Http.Fetch`) | An HTTP call under the sink's allowlist and residency rules; `connection` adds that connection's `bearer`, `basic` or `header` authentication |
| `ctx.sign(connection, name, data)` | Hex HMAC-SHA256 of `data` under the connection's credential `name`, which the script never sees |
| `ctx.connections` | Names and types of the `USING CONNECTIONS`, never credentials |
| `ctx.db.sql`, `ctx.db.query`, `ctx.db.get` (Lua `sprig.db.query`, `sprig.db.get`) | Read-only lookups as the sink's owner, in one read-only transaction per batch attempt; only a single `SELECT` or `WITH` is allowed (`ER_SPRIG_SINK_SCRIPT_READ_ONLY`, 39235) |
| `ctx.targets.webhook(c).post(records)` (Lua `sprig.sink.webhook_post`) | Delivery through the webhook target over connection `c` |

Budgets per batch: `sink.script_budget_ms` (30,000 ms of wall time), `sink.script_max_fetches` (1,000), `sink.script_db_max_queries` (100), `sink.script_db_max_rows` (10,000) and `sink.script_db_budget_ms` (5,000). A batch over budget is split in half; a single record over budget fails permanently. To deploy new code, run `ALTER SINK s SET SCRIPT AS $$ ... $$`.

## Delivery guarantees

Every sink delivers **at least once**. After a crash, a retry or a lost answer, a record can arrive again, always with the same **idempotency key**, so a receiver can drop duplicates:

| Source | Key form |
|---|---|
| Stream, change topic, `CHANGES OF` | `s/<stream id>/<partition>/<offset>/<leader epoch>` |
| Queue | `q/<queue id>/<message id>` |
| Fan-in | The member tenant's id, `:`, then one of the above |
| Transform output | The input's key with `#0`, `#1`, ... |

The key reaches the destination as the `Sprig-Idempotency-Key` header (webhooks), the Kafka header `sprig.idempotency.key`, `_sprig.key` in S3 files, `_sprig_idem` in append-mode tables, and `idempotencyKey` in scripts. A batch id is the first 16 bytes of a SHA-256 over its records' keys, in hex.

`GUARANTEE EXACTLY ONCE` makes each record's effect appear once in the destination, where the destination can store the position with the data:

| Source | Webhook, script | Kafka | S3 | Database |
|---|---|---|---|---|
| Stream, one table's `CHANGES OF` | No | Yes | Yes | Yes |
| Queue | No | No | No | Yes |
| Fan-in | No | No | No | Yes |

A combination marked No fails with `ER_SPRIG_SINK_EXACTLY_ONCE_UNSUPPORTED` (39217). `EXACTLY ONCE` over `CHANGES OF` more than one table fails with 39218.

- **Kafka**: a transactional producer (`transactional.id` `sprig-<tenant>-<sink>-<task>`) writes the records and the position to the offsets topic (`sink.kafka.offsets_topic`, created if missing) in one Kafka transaction. Consumers must read with `isolation.level=read_committed`.
- **Database**: each batch's rows and its position go in one destination transaction, in the table `__sprig_sink_offsets` (`__sprig_sink_seen` for queue and fan-in sources, purged after `sink.db.seen_retention_hours`, 168). The sink creates these tables.
- **S3**: object names depend only on the records, each write is conditional, and a manifest per partition under `_sprig/<sink>/<task>/` records what is complete. Readers that need exactly once read the manifests.
- Under `EXACTLY ONCE` each task reads one partition through one lane, and a `TRANSFORM` function must be declared `DETERMINISTIC` (39219 otherwise, checked again at every task start).

## Ordering

All records with the same key go to the same lane, and a lane sends one batch at a time and retries in place, so **records of one key arrive in source order**:

- streams: offset order within a partition;
- changes: commit order within a shard, which is commit order per row;
- queues: with `ordering = 'key'` (default) each routing key stays in order and lanes run in parallel; `WITH (ordering = 'queue')` uses one lane and waits for earlier unacknowledged messages, so the whole queue goes in order;
- fan-in: per member instance only.

A retrying batch holds up its lane, and so other keys hashed to that lane. `sink.lanes = 1` keeps strict source order at the cost of parallelism. A record that fails for good is dead-lettered and its lane moves on, so later records of that key are delivered after the gap.

## Retries, dead letters and pausing

Failures fall into three classes:

| Class | Examples | What happens |
|---|---|---|
| Destination-wide | Connection refused, TLS failure, timeout, HTTP 408, 429, 5xx, 401, 403, Kafka leader changes, database deadlocks | The batch is retried with backoff; this counts toward pausing |
| One record, retryable | A `"retry"` result for one record | That record is retried, up to `sink.record_max_attempts` (10), then dead-lettered with class `retries_exhausted` |
| One record, permanent | HTTP 4xx, a constraint violation, a transform error, a record too large | Dead-lettered at once with class `permanent`, `transform` or `too_large` |

**Backoff** uses full jitter: a random wait up to `min(sink.retry_max_ms, sink.retry_initial_ms × 2^(n−1))`, from 500 ms to 60 s by default, and at least the destination's `Retry-After` or the script's `retryAfterMs`, capped at `sink.retry_max_ms`.

**Dead letters.** A record that can't be delivered is kept in the tenant's catalog with its key, value, headers, reason, class and attempts. With `DEAD LETTER TO STREAM d`, it is also published into stream `d` of the tenant with the headers `sprig.dl.reason`, `sprig.dl.class`, `sprig.dl.attempts`, `sprig.dl.sink`, `sprig.dl.idempotency_key` and `sprig.dl.position`. A queue sink without `DEAD LETTER TO` rejects the message without requeue instead, so the queue's own dead-letter exchange applies, with `x-death`. A dead letter is written before its record counts as done, so a crash can duplicate a dead letter but never lose one.

List, replay or discard dead letters over REST or with `sprigctl sink dead-letters`. A replayed record goes back through the sink with its original idempotency key. Dead letters hold record data, so these calls need `SPRIG_SINK_ADMIN` or `SPRIG_TENANT_ADMIN`, or owning the sink (1227 otherwise).

**Pausing.** When a sink's batches have failed destination-wide for `sink.pause_after_minutes` (15), the sink moves from `failing` to `paused` with reason `ER_SPRIG_SINK_PAUSED` (39222) and stops reading. With `sink.auto_resume` on (default), one lane sends a probe every `sink.probe_interval_seconds` (60); the first success resumes the whole sink.

**Source lost.** If a sink's position falls below a stream's retained log start, the sink goes to `failed` with `ER_SPRIG_SINK_SOURCE_LOST` (39223). It never skips data by itself: run `ALTER SINK ... RESET TO` to choose where to continue.

## Manage a sink

```sql
ALTER SINK orders_to_billing PAUSE;
ALTER SINK orders_to_billing RESUME;
ALTER SINK orders_to_billing SET (batch_max_records = 200, lanes = 16);
ALTER SINK orders_to_billing RESET (batch_max_records);
ALTER SINK orders_to_billing RESET TO TIMESTAMP '2026-11-09T10:00Z';
ALTER SINK orders_to_billing OWNER TO reporting;
ALTER SINK orders_to_crm SET SCRIPT AS $$ export function deliver() {} $$;
DROP SINK IF EXISTS orders_to_billing;
```

- `PAUSE` holds new records in the source until `RESUME`. A sink a user paused never probes.
- `RESUME` of a `failed` sink fails with `ER_SPRIG_SINK_STATE` (39232): use `RESET TO LATEST | EARLIEST | SNAPSHOT | TIMESTAMP '...'`, which also sets it running.
- `SET` and `RESET` change `WITH` options live; the sink's tasks restart with the new definition from their positions. Numeric options must be whole numbers (39218).
- `ALTER` and `DROP` need `SPRIG_SINK_ADMIN`.

Every `sink.*` setting with an object scope can be given per sink in `WITH (...)`, with or without the `sink.` prefix. See [Settings reference](../reference/settings.md) for all of them.

### Test before you deliver

Test statements are dry runs: they never deliver a record and never move a position.

```sql
TEST CONNECTION billing_api;
TEST CONNECTION billing_api WRITE;
TEST CONNECTION kafka_eu WRITE TO 'sprig-test';
TEST PLATFORM CONNECTION kafka_main;
TEST SINK orders_to_billing WITH SAMPLE 20;

TEST CREATE CONNECTION crm_api TYPE WEBHOOK
  WITH (url = 'https://crm.example.com/v2', auth = 'bearer')
  CREDENTIALS (token = '<api-token>');
TEST CREATE SINK order_hooks FROM STREAM order_events
  TO WEBHOOK CONNECTION billing_api WITH SAMPLE 5;
```

- **`TEST CONNECTION`** opens the connection as a task would, through the allowlist and residency checks, then checks what it can: a webhook `OPTIONS` request, Kafka broker metadata, S3 `HeadBucket`, or a database login and `SELECT 1`. It returns one row per step (`Step`, `Result` `ok` or `failed`, `Detail`, `Elapsed ms`) and stops at the first failure. A failed step is a row, not an error.
- **`WRITE`** also writes one marked test record (header `x-sprig-test: 1`): one webhook request; one Kafka record into the topic `TO` names (required for Kafka); an S3 object that is deleted again; or, for a database, an `INSERT` checked inside a transaction that is rolled back, so no row is written.
- **`TEST SINK`** reads the newest records of a stream, or browses a queue's next messages without taking them, and runs them through `WHERE`, the projection and the transform. Each row has `Position`, `Outcome` (`deliver`, `filtered`, `dropped` by the transform, or `failed`), `Key`, `Value`, `Headers`, `Detail` and `Payload`, the exact body or record the target would send. `WITH SAMPLE n` takes 1 to 1,000 records (10 by default). It needs `SPRIG_SINK_ADMIN` or owning the sink, and `SELECT` on the stream.
- **`TEST CREATE CONNECTION`** and **`TEST CREATE SINK`** check a definition you haven't saved, with every check `CREATE` makes; credentials given to `TEST CREATE CONNECTION` are held in memory for the test only.

`TEST SINK` doesn't sample `CHANGES OF` or fan-in sources yet (39218).

### Status

```sql
SHOW SINKS FROM shop;
SHOW CREATE SINK orders_to_billing;
SHOW SINK STATUS orders_to_billing;
```

```sql test
SHOW SINKS;
SHOW CONNECTIONS;
```

`SHOW SINKS` lists `Database`, `Name`, `Source`, `Target`, `Connection`, `Guarantee`, `State` and `Reason`. A sink's state is `running`, `failing` (some batches retry for destination-wide reasons), `paused` (by a user, or by failures with 39222), `failed` (source lost) or `dropping`.

`SHOW SINK STATUS` has a first row for the sink (its state and reason), then one row per task: `Task`, `State` (`idle`, `running`, `retrying`, `paused`, `failed`), `Epoch`, `Worker`, `Delivered`, `Reads` and `Last error`.

Metrics, from workers, include:

| Metric | Type | Labels |
|---|---|---|
| `sprig_sink_records_delivered_total` | counter | `target` |
| `sprig_sink_bytes_delivered_total` | counter | `target` |
| `sprig_sink_batches_total` | counter | `target`, `result` (`ok`, `retry`, `fail`, `split`) |
| `sprig_sink_retries_total` | counter | `target`, `class` |
| `sprig_sink_dead_lettered_total` | counter | `target`, `reason` |
| `sprig_sink_delivery_seconds` | histogram | `target`: a batch's round trip |
| `sprig_sink_end_to_end_seconds` | histogram | `target`: source commit to delivery |
| `sprig_sink_paused` | gauge | `reason` |
| `sprig_sink_tasks` | gauge | `state` |
| `sprig_sink_destination_refused_total` | counter | `reason` (allowlist, residency, identity) |

See [Metrics](../reference/metrics.md) for every `sprig_sink_*` metric and [Monitoring](../operate/monitoring.md) for scraping.

## Over REST and sprigctl

Sink and connection statements run over the SQL endpoint like any other statement (`POST /v1/databases/{database}/sql:execute`). The REST API adds:

| Method and path | What it does |
|---|---|
| `POST /v1/databases/{database}/connections/{connection}:test` | `TEST CONNECTION`; body `{"platform": false, "write": true, "to": "sprig-test"}` |
| `POST /v1/databases/{database}/sinks/{sink}:test` | `TEST SINK`; body `{"sample": 20}` |
| `GET /v1/databases/{database}/sinks/{sink}/deadLetters` | Lists up to 1,000 dead letters: id, key, reason, class, attempts, state, time |
| `POST /v1/databases/{database}/sinks/{sink}/deadLetters:replay` | Marks dead letters for replay: `{"ids": [...]}` or `{"all": true}` |
| `POST /v1/databases/{database}/sinks/{sink}/deadLetters:discard` | Discards them, with the same body |

The test calls answer `{columns, rows}`. See the [REST API](../reference/api.md).

`sprigctl sink` has `list`, `show`, `pause`, `resume`, `reset --to`, `test`, `dead-letters list | replay | discard`, `connections list | show | test` and `platform-connections`. See [sprigctl](../tools/sprigctl.md).

## Sinks for every tenant and fan-in sinks

A parent tenant can declare a connection and a sink once with `FOR EACH TENANT`, and every member tenant runs its own instance, over its own instance of an inherited stream or queue, with its own destination. An ancestor can also run one **fan-in** sink over every member's instance. Both are described in [Data inheritance](inheritance.md); in short:

```sql
-- In the parent:
CREATE CONNECTION tenant_hooks FOR EACH TENANT TYPE WEBHOOK
  WITH (url = REQUIRED, auth = 'bearer', signature = 'none')
  CREDENTIALS (token = REQUIRED);
CREATE SINK user_hooks FOR EACH TENANT
  FROM STREAM user_events TO WEBHOOK CONNECTION tenant_hooks PATH '/users';

-- In a member, or from the parent FOR TENANT acme:
ALTER INHERITED CONNECTION tenant_hooks SET (url = 'https://hooks.acme.example')
  CREDENTIALS (token = '<api-token>');
ALTER INHERITED SINK user_hooks SET (batch_max_records = 50);
ALTER INHERITED SINK user_hooks DISABLE;
```

- An inherited sink's source must be a `FOR EACH TENANT` stream or queue, and its connections `FOR EACH TENANT` or platform connections, all in the sink's database.
- A definition's credentials can only be `REQUIRED`. Each member's are set with `ALTER INHERITED CONNECTION` and sealed under the member's own key. Options may also use `{tenant}` and `{tenant_id}`, such as `url = 'https://{tenant}.hooks.example.com'`.
- A member without a `REQUIRED` value has its instance in state `waiting_config` (`ER_SPRIG_INHERIT_CONFIG_MISSING`, 39241): nothing is read and nothing fails.
- Each member's instance is an ordinary sink of the member, owned by its `sprig_inherited` principal, checked against the member's allowlist and residency, with its own retries, dead letters and pauses.

### Fan-in sinks

```sql
CREATE SINK all_user_events FROM STREAM user_events FAN IN
  TO KAFKA PLATFORM CONNECTION kafka_main TOPIC 'platform.user-events'
  WITH (lanes = 16);
```

A fan-in sink lives in the tenant that declared the inherited stream or queue, or in a member above the instances it reads; `UNDER t` limits it to `t`'s subtree. Its owner needs `SPRIG_SINK_ADMIN`, `SPRIG_FAN_IN` and `SELECT` on the name (and `DELETE` for a queue).

- New member instances join within `sink.fanin_refresh_seconds` (30). An instance that existed when the sink was created starts at `START AT`; one made later starts at its beginning.
- Every record carries its member: the header `x-sprig-tenant`, a `tenant` field in the JSON formats, an idempotency key starting with the member's id, `{tenant}` in S3 prefixes, and a `_sprig_tenant` column (the member's id) in database targets.
- Members whose residency sets `residency.deny_remote_access` are left out and counted.
- Order is kept per member instance only. `EXACTLY ONCE` works into databases only.

## Limits and sharding behaviour

| Limit | Value | When exceeded |
|---|---|---|
| Sinks per tenant | `sink.max_per_tenant`, 50 | 39226 |
| Records per batch | `sink.batch_max_records`, 500 (1 to 1,000,000) | A new batch |
| Bytes per batch | `sink.batch_max_bytes`, 4 MiB | A new batch |
| Read-ahead per task | `sink.max_inflight_bytes`, 64 MiB | The task waits |
| Lanes per task | `sink.lanes`, 8 (1 to 256) | |
| Tasks per worker | `sink.worker_max_tasks`, 256 | |
| One tenant's share of a worker | `sink.tenant_worker_share_pct`, 25 % | |
| Delivery rate per tenant | `sink.tenant_bytes_per_sec`, 0 (no limit) | Delivery slows to the rate, shared across workers |
| Rows per database statement | `sink.db.batch_rows`, 500 | |
| S3 object roll | 128 MiB, 1,000,000 records or 300 s (`sink.s3.file_max_*`) | A new object |
| `TEST SINK` sample | 1 to 1,000 records | 1064 |

- **Where tasks run.** Sink tasks run only in the tenant's home data centre and move with the tenant. Credentials are opened only there.
- **Parallelism follows the source.** A stream with 12 partitions and the default `sink.partitions_per_task` (4) runs 3 tasks; a queue runs one. Add stream partitions, or lower `partitions_per_task`, for more parallel tasks.
- **Fan-in tasks** grow with the number of member instances: `ceil(instances × partitions / sink.fanin_instances_per_task)`.

## Errors

| Code | Name | When |
|---|---|---|
| 39210 | `ER_SPRIG_SINK_NOT_FOUND` | Unknown sink or connection |
| 39211 | `ER_SPRIG_SINK_EXISTS` | Name in use |
| 39212 | `ER_SPRIG_SINK_PRIVILEGE` | No `SPRIG_SINK_ADMIN`, no `USAGE` on a connection, no `SELECT` on the source, or a fan-in sink without `SPRIG_FAN_IN` |
| 39213 | `ER_SPRIG_SINK_DESTINATION_NOT_ALLOWED` | A host not on `sink.destination_allowlist`, or a private address not listed literally |
| 39214 | `ER_SPRIG_SINK_RESIDENCY` | The destination's tags don't satisfy the residency policy |
| 39215 | `ER_SPRIG_SINK_CONNECTION_IN_USE` | Dropping a connection a sink uses |
| 39216 | `ER_SPRIG_SINK_CREDENTIALS_INVALID` | An unknown or missing credential, or no credential key on the gateway |
| 39217 | `ER_SPRIG_SINK_EXACTLY_ONCE_UNSUPPORTED` | `EXACTLY ONCE` for a source and target that can't give it |
| 39218 | `ER_SPRIG_SINK_OPTION_INVALID` | An invalid option, template, column list or filter, or a form that isn't built |
| 39219 | `ER_SPRIG_SINK_TRANSFORM_INVALID` | A transform with effects, missing, or not `DETERMINISTIC` under `EXACTLY ONCE` |
| 39220 | `ER_SPRIG_SINK_SCRIPT_FAILED` | A script error or malformed result |
| 39221 | `ER_SPRIG_SINK_DELIVERY_FAILED` | The destination refused or failed; the message carries its status or code |
| 39222 | `ER_SPRIG_SINK_PAUSED` | Paused after `sink.pause_after_minutes` of failures |
| 39223 | `ER_SPRIG_SINK_SOURCE_LOST` | The position fell behind the source's retention, or the queue was replaced |
| 39224 | `ER_SPRIG_SINK_SCHEMA_MISMATCH` | The destination table doesn't fit under the schema policy |
| 39226 | `ER_SPRIG_SINK_QUOTA` | Over `sink.max_per_tenant` |
| 39228 | `ER_SPRIG_SINK_PLATFORM_IDENTITY_DENIED` | An S3 cloud identity, or a metadata address |
| 39230 | `ER_SPRIG_SINK_DISABLED` | `sink.enabled` off, or a kind not in `sink.allowed_kinds` |
| 39231 | `ER_SPRIG_SINK_FORMAT_UNSUPPORTED` | A format or compression the target can't produce |
| 39232 | `ER_SPRIG_SINK_STATE` | An operation the sink's state doesn't allow |
| 39233 | `ER_SPRIG_SINK_PLATFORM_CONNECTION_NOT_GRANTED` | A platform connection the tenant isn't granted |
| 39234 | `ER_SPRIG_SINK_PLATFORM_SECRET_UNAVAILABLE` | A worker can't read a platform secret file |
| 39235 | `ER_SPRIG_SINK_SCRIPT_READ_ONLY` | A script's `db` handle given anything but a read |
| 39241 | `ER_SPRIG_INHERIT_CONFIG_MISSING` | An inherited connection's `REQUIRED` value isn't set for this member |

Naming a stream that doesn't exist fails as for any stream statement:

```sql test-error
CREATE SINK ghost_hooks FROM STREAM no_such_stream
  TO WEBHOOK CONNECTION billing_api;
```

See [Errors](../reference/errors.md) for every code.

## Not supported

- **Sources outside SprigDB.** Sinks only send. To bring data in, produce into a stream with a Kafka client.
- **Other destinations**, such as MongoDB, Elasticsearch, ClickHouse, SQL Server or data warehouses. Use S3 files, a webhook or a script sink.
- **Schema registries** and Avro or Protobuf payloads.
- **Cloud identities for S3** (`assume_role`, `web_identity`, `instance`): only static keys work.
- **Kafka `OAUTHBEARER`** authentication.
- **`TO SCRIPT FUNCTION f`**: write the script inline with `AS $$ ... $$` (39218).
- **`DEAD LETTER TO QUEUE` and `DEAD LETTER TO EXCHANGE`** (39218). Use `DEAD LETTER TO STREAM`.
- **An initial copy for `START AT SNAPSHOT`**: it starts at the oldest retained change instead.
- **`EXACTLY ONCE` into webhooks and scripts**, from queues into Kafka or S3, or over more than one table.
- **`TEST SINK` samples of `CHANGES OF` and fan-in sources.**
- **Sinks in several data centres**: a sink runs only in its tenant's home data centre.
- **Reading another tenant's ordinary objects.** A sink reads only its own tenant's data; inherited sinks run inside each member, and fan-in sinks read only instances of inherited definitions.

## Related

- [Event streams](streams.md): the most common source
- [Queues](queues.md): queue sources and dead-letter exchanges
- [Data inheritance](inheritance.md): `FOR EACH TENANT` sinks and fan-in
- [Streams, queues and sinks in SQL](../sql/streams-queues-sinks.md): statement reference
- [Change feed](../concepts/change-feed.md): where `CHANGES OF` records come from
