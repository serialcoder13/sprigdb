---
title: Key-value
audience: [user, dba]
sidebar:
  order: 3
---

# Key-value

SprigDB stores keys and their values in MariaDB tables and serves them over the Redis protocol, a REST and gRPC key-value API, and SQL. Use it for sessions, caches, counters, leaderboards and queues of work that your application already writes with a Redis client, without running a separate Redis server.

## Namespaces and Redis databases

A **namespace** is a set of keys inside one of your databases. Each namespace is three MariaDB tables:

| Table | Holds | One row per |
|---|---|---|
| `<name>` (the key table) | each key's type, its string value, its element count, its version and its expiry | key |
| `__sprig_ke_<name>` | hash fields, list items and set members | element |
| `__sprig_kz_<name>` | sorted-set members with their scores | member |

The tables are created the first time you use the namespace. Every row also carries two hidden columns, the key's distribution key and its routing hash, which SprigDB uses to place and route it (see [Sharding](#sharding)).

**Redis databases are namespaces.** The Redis database that `SELECT n` chooses is the namespace `db<n>` in your tenant's database `redis`. So `SELECT 0` writes to the table `redis.db0`, and `SELECT 3` to `redis.db3`.

| Setting | Default | Meaning |
|---|---|---|
| `redis.logical_database` | `redis` | The database that holds the Redis databases' namespaces |
| `redis.databases` | `16` | The highest `SELECT` index plus one (1 to 1024) |

Each tenant has its own `redis` database, so two tenants writing the key `session:42` never see each other's value.

**API namespaces** are the ones you name in a REST or gRPC call: `POST /v1/databases/shop/namespaces/cache:put` creates and uses the namespace `cache` in the database `shop`. A namespace name is 1 to 40 letters, digits or underscores.

## Data types

| Type | Stored as | Redis commands |
|---|---|---|
| String | the key row's `v` column (`LONGBLOB`) | strings, bit operations, `BITFIELD` |
| Hash | one element row per field | `H*` |
| List | one element row per item, with a position | `L*`, `R*`, `B*POP` |
| Set | one element row per member | `S*` |
| Sorted set | one row per member, with a `DOUBLE` score | `Z*`, and the geo commands |

Values are binary-safe. `TYPE` answers `string`, `hash`, `list`, `set` or `zset`. A command on a key of the wrong type fails with Redis's `WRONGTYPE Operation against a key holding the wrong kind of value` (39730 `ER_SPRIG_KV_WRONG_TYPE` on the REST API).

Streams (`X*` commands) and HyperLogLog (`PF*` commands) aren't supported. See [What's not supported](#whats-not-supported).

## Use it from Redis

Turn on the Redis listener with `listener.redis.enabled` (port 6379, TLS required by default), then log in with your SprigDB account. A dotted user name names the tenant:

```sh
redis-cli -p 6379 --tls --user acme.app --pass '<password>'
```

The tenant can also come from `listener.redis.tenant`. One-argument `AUTH <password>` logs in as `redis.default_user`; while that setting is empty, it fails with `ERR AUTH <password> called without any password configured`. See [Redis](../connect/redis.md) for clients and TLS.

SprigDB answers as Redis 7.2 (`redis.server_version`), in RESP2 and RESP3 (`HELLO 3`). Commands keep Redis's arguments, options and reply texts:

```text
SET session:42 '{"user":7}' EX 1800
GET session:42
HSET customer:7 name Asha city Pune
HGETALL customer:7
ZADD leaderboard 1500 asha 1200 ravi
ZRANGE leaderboard 0 -1 REV WITHSCORES
LPUSH jobs:email job-1 job-2
BRPOP jobs:email 5
```

Every command runs as SQL statements through your own session, so your grants and row-level security apply to it. A command the account isn't allowed to run on that table answers `NOPERM this user has no permissions to run this command on this data: ...`.

### Commands by type

These commands are built. Each takes Redis 7.2's options.

| Group | Commands |
|---|---|
| Strings | `GET`, `SET` (`NX`, `XX`, `GET`, `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`), `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `MGET`, `MSET`, `MSETNX`, `APPEND`, `STRLEN`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `GETRANGE`, `SUBSTR`, `SETRANGE`, `LCS` |
| Bits | `SETBIT`, `GETBIT`, `BITCOUNT`, `BITPOS`, `BITOP`, `BITFIELD`, `BITFIELD_RO` |
| Keys | `DEL`, `UNLINK`, `EXISTS`, `TOUCH`, `TYPE`, `RENAME`, `RENAMENX`, `COPY` (with `DB` and `REPLACE`), `MOVE`, `KEYS`, `SCAN` (`MATCH`, `COUNT`, `TYPE`), `RANDOMKEY`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `OBJECT`, `SORT`, `SORT_RO` |
| Expiry | `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` (each with `NX`, `XX`, `GT`, `LT`), `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `PERSIST` |
| Hashes | `HSET`, `HMSET`, `HSETNX`, `HGET`, `HMGET`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HEXISTS`, `HDEL`, `HINCRBY`, `HINCRBYFLOAT`, `HSTRLEN`, `HRANDFIELD`, `HSCAN` |
| Lists | `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LINSERT`, `LREM`, `LTRIM`, `LPOS`, `LMOVE`, `RPOPLPUSH`, `LMPOP` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SPOP`, `SRANDMEMBER`, `SMOVE`, `SINTER`, `SUNION`, `SDIFF`, `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`, `SINTERCARD`, `SSCAN` |
| Sorted sets | `ZADD`, `ZINCRBY`, `ZREM`, `ZSCORE`, `ZMSCORE`, `ZCARD`, `ZCOUNT`, `ZLEXCOUNT`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZRANGESTORE`, `ZRANK`, `ZREVRANK`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYLEX`, `ZPOPMIN`, `ZPOPMAX`, `ZMPOP`, `ZRANDMEMBER`, `ZUNION`, `ZINTER`, `ZDIFF`, `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE`, `ZINTERCARD`, `ZSCAN` |
| Geo | `GEOADD`, `GEOPOS`, `GEODIST`, `GEOHASH`, `GEOSEARCH`, `GEOSEARCHSTORE`, `GEORADIUS`, `GEORADIUS_RO`, `GEORADIUSBYMEMBER`, `GEORADIUSBYMEMBER_RO`; see [Geospatial](geospatial.md) |
| Blocking | `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP` |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` |
| Scripting | `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `SCRIPT` (`LOAD`, `EXISTS`, `FLUSH`, `KILL`), `FUNCTION` (`LOAD`, `DELETE`, `FLUSH`, `LIST`, `STATS`, `KILL`), `FCALL`, `FCALL_RO` |
| Pub/sub | `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `SSUBSCRIBE`, `SUNSUBSCRIBE`, `PUBLISH`, `SPUBLISH`, `PUBSUB` (`CHANNELS`, `NUMSUB`, `NUMPAT`, `SHARDCHANNELS`, `SHARDNUMSUB`) |
| Connection | `PING`, `ECHO`, `QUIT`, `RESET`, `SELECT`, `HELLO`, `AUTH`, `CLIENT` (`ID`, `SETNAME`, `GETNAME`, `INFO`, `LIST`, `SETINFO`, `NO-EVICT`, `NO-TOUCH`, `REPLY ON`, `UNBLOCK`, `GETREDIR`, `TRACKINGINFO`) |
| Server | `INFO`, `TIME`, `COMMAND`, `CONFIG`, `ACL`, `ROLE`, `CLUSTER`, `SLOWLOG`, `LATENCY`, `MEMORY`, `LASTSAVE`, `LOLWUT`, `READONLY`, `READWRITE`, `ASKING`, `WAIT`, `WAITAOF`; `SAVE`, `BGSAVE` and `BGREWRITEAOF` succeed and do nothing |

A few behave differently from a single Redis server:

- `WAIT` answers `0` and `WAITAOF` answers `0 0`. Durability comes from MariaDB's commit.
- `CLIENT LIST` shows only your own connection.
- A `CLIENT SETNAME` name ending in `~<dc>`, such as `orders~fra1`, names the client's data centre, which counts its traffic for tenant placement; see [Redis](../connect/redis.md#naming-the-clients-data-centre).
- `FLUSHDB` and `FLUSHALL` empty the tables statement by statement. They aren't atomic across Redis databases or shards, as on a Redis Cluster.
- `SCAN`'s cursor is a position in routing-hash order, not a Redis slot cursor. Iterate until it returns `0`, as with Redis.

### How a command runs

A write command is one MariaDB transaction: it locks the key row, reads what it needs, writes the rows, and commits. Single commands run at `READ COMMITTED`.

The busiest writes run as one compound statement, in one round trip to MariaDB: `SET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `HSET`, `HMSET`, `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP` and `ZADD`. `redis.guarded_batches` (default `true`) turns this off. Commands that need an error reply, `ZADD INCR`, repeated `ZADD` members, and more than 200 items take the ordinary path.

When two writers conflict (a deadlock or a lock wait timeout), SprigDB reruns the command up to `redis.write_retries` times (default 10) after a short random wait. If it still conflicts, the client gets:

```text
TRYAGAIN SPRIG REDIS_SHARD_BUSY 39846 conflicting writes; retry
```

## Expiry

Set a time to live with `SET ... EX`, `EXPIRE`, `PEXPIREAT` and the rest, or with `ttl_ms` on the REST API. The expiry instant is stored in the key row's `expires_at` column (`DATETIME(3)`, UTC).

Expiry works in two steps:

1. **Reads hide expired keys at once.** Every read checks `expires_at` against MariaDB's own clock (`UTC_TIMESTAMP(3)`), so all gateways agree on when a key has expired, to the millisecond. A write to an expired key replaces it.
2. **A sweeper deletes the rows.** A background job in the workers finds expired keys in every namespace, deletes their key rows, and deletes their elements. It sleeps until the next expiry is due, but never longer than `kv.purge_interval_seconds`.

| Setting | Default | Meaning |
|---|---|---|
| `kv.purge_interval_seconds` | `10` | Longest sleep between sweeps |
| `kv.purge_batch_rows` | `500` | Keys per sweep transaction, and element rows per delete |
| `kv.inline_delete_max_elements` | `128` | Keys with more elements than this have their elements deleted after the key row's transaction |
| `kv.orphan_scan_hours` | `24` | Hours between scans for element rows whose key is gone; `0` scans on every pass |

The sweeper skips keys that a client has locked, and deletes the elements of large collections in batches, so a 100,000-item list expiring doesn't hold a long transaction. Watch `sprig_kv_purged_keys_total` and `sprig_kv_orphan_elements_deleted_total` in [Monitoring](../operate/monitoring.md).

Keys are never evicted to free memory: they stay until they are deleted or expire, and the `evicted` keyspace event never occurs.

## Transactions

`MULTI` queues commands and `EXEC` runs them in one MariaDB transaction at `REPEATABLE READ`. `WATCH` records each watched key's version; `EXEC` locks the watched keys and returns a null reply, running nothing, if any of them changed.

```text
WATCH balance:7
MULTI
DECRBY balance:7 100
INCRBY balance:9 100
EXEC
```

As in Redis:

- A command with a syntax or arity error while queuing makes `EXEC` fail with `EXECABORT Transaction discarded because of previous errors.`
- A command that fails at run time (for example `INCR` on a hash) returns its error in `EXEC`'s reply, and the others still apply.

If the transaction meets a conflict, SprigDB reruns the whole queue up to `redis.write_retries` times, then answers `TRYAGAIN` (39846). At most `redis.max_multi_commands` commands (default 10,000) may be queued; one more fails with `SPRIG REDIS_MULTI_TOO_LARGE 39844`.

Keys that share a hash tag, such as `{customer:7}:cart` and `{customer:7}:orders`, always live on the same shard, so a transaction over them commits inside one MariaDB server. See [Sharding](#sharding).

## Lua scripts and functions

`EVAL`, `EVALSHA`, `SCRIPT LOAD`, `FUNCTION LOAD` and `FCALL` run Lua 5.1 with Redis's `redis.*` library: `call`, `pcall`, `error_reply`, `status_reply`, `sha1hex`, `log`, `setresp`, `set_repl`, `replicate_commands`, `register_function`, `acl_check_cmd` and the `LOG_*` and `REPL_*` constants. `cjson` is available.

```text
EVAL "return redis.call('INCR', KEYS[1])" 1 counter:visits
```

A script runs in one MariaDB transaction at `REPEATABLE READ`. Before it starts, SprigDB locks the keys in `KEYS`, in order. If the script fails or runs out of budget, everything it wrote is rolled back.

| Setting | Default | Meaning |
|---|---|---|
| `redis.lua_time_limit_ms` | `5000` | Wall-time budget of one run |
| `redis.lua_memory_limit_mb` | `32` | Lua heap of one run |
| `redis.lua_kill_after_ms` | `1000` | `SCRIPT KILL` and `FUNCTION KILL` stop only runs older than this |
| `redis.lua_undeclared_keys` | `refuse` | Keys a script may touch outside `KEYS`: `refuse`, or `same_shard` |

A script over budget is rolled back with `SPRIG REDIS_LUA_BUDGET_EXCEEDED 39850`, naming the setting it exceeded. A script that touches a key not in `KEYS` fails with `SPRIG REDIS_LUA_UNDECLARED_KEY 39851`, unless `redis.lua_undeclared_keys` is `same_shard` and the key shares its hash tag's routing with a declared key.

Scripts loaded with `EVAL` or `SCRIPT LOAD`, and libraries loaded with `FUNCTION LOAD`, are stored in a table of your `redis` database. Every gateway sees them, and they survive restarts, so `EVALSHA` doesn't need a reload after a failover.

`FCALL` also calls SprigDB's own logic functions when no Lua library function has that name. `KEYS` bind the function's leading parameters and `ARGV` the rest, as text; `db.name` names a function in another database. See [Logic](logic.md).

## Pub/sub and keyspace notifications

`PUBLISH` reaches every subscriber of the channel on every gateway in the data centre, and its reply counts them. Channels belong to the tenant: another tenant's `PUBLISH` to a channel of the same name reaches none of your subscribers. Sharded channels (`SSUBSCRIBE`, `SPUBLISH`) are spread over `redis.sharded_pubsub_hubs` hubs (default 8). If no hub can be reached, `PUBLISH` fails with `SPRIG REDIS_PUBSUB_UNAVAILABLE 39848`.

Blocking commands such as `BLPOP` wake as soon as another client, on any gateway, pushes to the key. A safety poll rereads the key every `redis.block_poll_ms` (default 1000). At most `redis.max_blocked_per_tenant` clients (default 10,000) may block per tenant on each gateway; one more fails with `SPRIG REDIS_TOO_MANY_BLOCKED 39845`.

**Keyspace notifications** are off by default. Turn them on for every tenant with the setting `redis.notify_keyspace_events`, or for your tenant on one gateway with `CONFIG SET`:

```text
CONFIG SET notify-keyspace-events KEA
PSUBSCRIBE __keyspace@0__:session:*
```

The flags are Redis's (`K`, `E`, `g`, `$`, `l`, `s`, `h`, `z`, `x`, `t`, `d`, `n`, `m`, `A`). Notifications cover writes from Redis commands and also changes made by SQL, the REST API, scripts, and the expiry sweeper (`expired`). A subscriber receives a notification only for keys its own account may read, checked against its grants and row-level security.

## Use it from REST and gRPC

The `sprig.v1.KeyValue` service works on API namespaces. Every call is a `POST` with a JSON body, and the key goes in the body:

| Path | Operation | Body |
|---|---|---|
| `/v1/databases/{database}/namespaces/{namespace}:get` | `KeyValue.Get` | `key` |
| `/v1/databases/{database}/namespaces/{namespace}:put` | `KeyValue.Put` | `key`, `text` or `value`, preconditions, `ttl_ms`, `location` |
| `/v1/databases/{database}/namespaces/{namespace}:delete` | `KeyValue.Delete` | `key`, `if_version` |
| `/v1/databases/{database}/namespaces/{namespace}:scan` | `KeyValue.Scan` | `prefix`, `page_token`, `limit` |
| `/v1/databases/{database}/namespaces/{namespace}:exec` | `KeyValue.Exec` | `command`: one Redis command as an array |
| `/v1/databases/{database}/namespaces/{namespace}:geoSearch` | `KeyValue.GeoSearch` | see [Geospatial](geospatial.md) |

The `Put` fields:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `key` | string | - | 1 to 1024 bytes |
| `text` | string | - | The value as text |
| `value` | string | - | The value as base64; used instead of `text` when set |
| `if_version` | string | - | Write only if the key's version is this; `"0"` means the key must be absent |
| `if_absent` | bool | `false` | Write only if the key doesn't exist |
| `if_exists` | bool | `false` | Write only if the key exists |
| `ttl_ms` | int | none | Time to live in milliseconds |
| `location` | object | none | `{"latitude": ..., "longitude": ...}`, for geo search |

```sh
curl -u acme.app:'<password>' -X POST \
  https://sprig.example.com:8443/v1/databases/shop/namespaces/cache:put \
  -d '{"key": "user:7", "text": "Asha"}'
```

```json
{"version": "4093862571294310712"}
```

`Get` returns the entry with its value in base64 (`value`), as text when it is valid UTF-8 (`text`), its `version` and `expires_at_ms` (`"0"` when it has none):

```json
{"found": true, "entry": {"key": "user:7", "type": "string", "value": "QXNoYQ==", "text": "Asha", "version": "4093862571294310712", "expires_at_ms": "0"}}
```

**Compare-and-set.** Pass the version you read as `if_version`. If another writer changed the key first, the `Put` fails with 39721 `ER_SPRIG_KV_VERSION_MISMATCH` (HTTP 409), and the message carries the current version. `if_absent` on an existing key fails with 39722 `ER_SPRIG_KV_KEY_EXISTS` (HTTP 409). Versions are random 63-bit numbers that never repeat for a key; compare them for equality only.

**Scan** returns keys in key order, `limit` per page (default 100, at most 1000), with `next_page_token` empty on the last page:

```json
{"prefix": "/users/", "limit": 3}
```

**Exec** runs one Redis command on the namespace and returns the reply as JSON: a map for `HGETALL`, numbers for integers, `{"error": "..."}` for an error reply.

```json
{"command": ["HSET", "h1", "name", "Asha", "city", "Pune"]}
```

```json
{"reply": 2}
```

`Get` reads string keys only; a hash, list, set or sorted set fails with 39730. Use `Exec` for those. An invalid namespace name fails with 39720 `ER_SPRIG_KV_NAMESPACE_NOT_FOUND` (HTTP 404).

The same operations are on gRPC as `sprig.v1.KeyValue`. See [REST and gRPC](../connect/rest-and-grpc.md) and the [API reference](../reference/api.md).

## Read it from SQL

A namespace's key table is an ordinary table in its database. From the MySQL or MariaDB port you can read Redis keys directly:

```sql
SELECT v FROM redis.db0 WHERE k = 'session:42';
SELECT COUNT(*) FROM shop.cache WHERE k LIKE '/users/%';
```

| Column | Type | Meaning |
|---|---|---|
| `k` | `VARBINARY(1024)` | The key |
| `type` | `ENUM` | `string`, `hash`, `list`, `set` or `zset` |
| `v` | `LONGBLOB` | A string key's value |
| `n` | `BIGINT UNSIGNED` | Element count of a collection |
| `_v` | `BIGINT UNSIGNED` | The version |
| `expires_at` | `DATETIME(3)` | Expiry in UTC, or `NULL` |

The key table shows expired keys until the sweeper removes them. Add `AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(3))` to see only live keys.

For collections, each namespace has four read-only views that show live keys only:

| View | Columns |
|---|---|
| `<ns>__hash` | `k`, `field`, `value` |
| `<ns>__list` | `k`, `idx` (0 is the head), `item` |
| `<ns>__set` | `k`, `member` |
| `<ns>__zset` | `k`, `member`, `score`, `score_class` |

```sql
SELECT field, value FROM redis.db0__hash WHERE k = 'customer:7' ORDER BY field;
SELECT member, score, score_class FROM redis.db0__zset
WHERE k = 'leaderboard' ORDER BY score_class, score;
```

`score` is `NULL` for the scores `-inf` and `+inf`; `score_class` is `-1`, `0` or `1` for `-inf`, a finite score and `+inf`.

Because values are rows, you can join them with your tables: for example, a customer's session with `customers` and `orders`, or a JSON value with `JSON_VALUE(v, '$.user')`. See [Cross-model queries](cross-model-queries.md). Write through Redis or the API rather than with SQL `UPDATE`s, so versions, element counts and expiry stay consistent.

### Indexes

Keys and key prefixes are primary-key lookups, and sorted sets have a score index, so most key-value access needs no extra index. [Automatic indexing](../concepts/automatic-indexing.md) adds the two that SQL sometimes needs:

- SQL that finds a hash field's value or a set member across keys, such as `SELECT k FROM cache__hash WHERE field = 'email' AND value = 'asha@example.com'`, gets an index on the element table.
- SQL that filters string values by a JSON field, such as `WHERE JSON_VALUE(v, '$.email') = ...` with `=`, `IN`, `BETWEEN`, `<`, `>` or `IS NULL`, gets an invisible virtual column for that field with a prefix index. Your statement isn't rewritten: MariaDB uses the column on its own.

Each such index costs every write of the namespace.

## Limits

| Limit | Value | Error |
|---|---|---|
| Key length | 1024 bytes | 39723 `ER_SPRIG_KV_INVALID_KEY` (`SPRIG KV_INVALID_KEY 39723` on Redis) |
| Hash field, set member or sorted-set member | 2024 bytes | 39731 `ER_SPRIG_KV_ELEMENT_TOO_LONG` |
| String value or list item, Redis databases | 16 MiB | 39724 `ER_SPRIG_KV_VALUE_INVALID` |
| String value, API namespaces | 1 MiB | 39724 `ER_SPRIG_KV_VALUE_INVALID` |
| Bulk string on the wire | `redis.max_bulk_bytes`, 16 MiB | `ERR Protocol error: ...`, and the connection closes |
| One command | `redis.max_command_bytes`, 64 MiB | `ERR Protocol error: ...`, and the connection closes |
| Commands queued in `MULTI` | `redis.max_multi_commands`, 10,000 | 39844 |
| Blocked clients per tenant per gateway | `redis.max_blocked_per_tenant`, 10,000 | 39845 |

SprigDB's own Redis errors have the form `-SPRIG <NAME> <number> <message>`; Redis clients raise them like any other error reply.

## Sharding

Every key has a **distribution key** that decides its shard:

- **Redis databases** use Redis Cluster's hash tag: the bytes between the first `{` and the next `}`, when that isn't empty; otherwise the whole key. `{customer:7}:cart` and `{customer:7}:orders` share the distribution key `customer:7`.
- **API namespaces** use the first segment of a key that starts with `/`: `/users/42/profile` and `/users/9` share `users`. A key without a leading `/` is its own distribution key.

A key's elements always live on its key row's shard.

A namespace starts unsharded, in its database's default table group, like any table. The [sharding advisor](../performance/sharding-advisor.md) shards it, with its element tables, on the distribution key only when one of the usual rules holds:

- its database asks for shards (`sharding.min_shards` above 1);
- its tables hold at least half of `sharding.split_size_gb`;
- its shard is write-hot and the namespace takes that share of the writes.

The distribution key must also pass the advisor's key-quality checks (enough distinct values, no single value dominating). Once sharded, every statement a command runs names the key's distribution key, so a single-key command goes to one shard. Keep keys that you update together under one hash tag, so they stay on one shard and commit inside one MariaDB server. See [Shards and table groups](../concepts/shards-and-table-groups.md).

SprigDB presents itself as one standalone server (`HELLO` reports `mode` `standalone`), so connect without cluster mode: sharding happens behind the gateway. `CLUSTER KEYSLOT` answers Redis Cluster's slot for a key, for information only.

## What's not supported

| Feature | What you get |
|---|---|
| Streams: `XADD`, `XREAD`, `XRANGE` and the other `X*` commands | `SPRIG REDIS_UNSUPPORTED 39840`. Use [Streams](streams.md) |
| HyperLogLog: `PFADD`, `PFCOUNT`, `PFMERGE` | `SPRIG REDIS_UNSUPPORTED 39840` |
| `SWAPDB`, `MONITOR`, `CLIENT REPLY OFF` | `SPRIG REDIS_UNSUPPORTED 39840` |
| `SCRIPT DEBUG` | `SPRIG REDIS_LUA_UNSUPPORTED 39841` |
| `DUMP`, `RESTORE`, `MIGRATE`, `FUNCTION DUMP`, `FUNCTION RESTORE`, `DEBUG`, `SHUTDOWN`, `REPLICAOF`, `SLAVEOF`, `SYNC`, `PSYNC`, `FAILOVER`, `MODULE`, `CLIENT KILL`, `CLIENT PAUSE`, `CLIENT TRACKING` | `SPRIG REDIS_RESTRICTED 39842`: manage SprigDB through SQL, the API or `sprigctl` |
| Lua libraries `cmsgpack`, `bit`, `struct` | not loaded |
| Eviction of keys to free memory | not offered |
| Key-value API: transactions over several keys, value filters in `Scan` | use `Exec` with a script, or SQL |

## Related

- [Redis](../connect/redis.md)
- [Geospatial](geospatial.md)
- [Cross-model queries](cross-model-queries.md)
- [Shards and table groups](../concepts/shards-and-table-groups.md)
- [Row-level security](../security/row-level-security.md)
