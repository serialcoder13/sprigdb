---
title: Redis
audience: [user, dba]
sidebar:
  order: 4
---

# Redis

SprigDB answers the Redis protocol, so `redis-cli` and ordinary Redis clients can read and write keys that SprigDB stores in MariaDB. Use it when your application already speaks Redis and you want its keys durable, replicated, protected by your grants and readable from SQL.

This page covers connecting. For how keys are stored, expiry, transactions, scripts and pub/sub in depth, see [Key-value](../data-models/key-value.md).

## Enable the listener

The Redis listener is off by default.

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `listener.redis.enabled` | bool | `false` | Serve Redis |
| `listener.redis.bind` | text | `0.0.0.0:6379` | Address and port |
| `listener.redis.tls` | text | `required` | `required`, `optional` or `off` |
| `listener.redis.cert` | text | `default` | Certificate bundle |
| `listener.redis.client_cert` | text | `off` | Client certificates: `off`, `optional` or `required` (see [Client certificates](../security/tls.md#client-certificates)) |
| `listener.redis.tenant` | text | empty | Tenant of user names without a dot |

An operator turns it on with `sprigctl`; the listener starts without a restart:

```sh
sprigctl settings set listener.redis.enabled true --scope global --reason "Redis clients"
```

Keep TLS on. `AUTH` sends the password itself, so without TLS it crosses the network in clear text.

## Log in

Log in with your SprigDB account. The user name names the tenant: `acme.app` is user `app` of tenant `acme`.

```sh
redis-cli -h db.example.com -p 6379 --tls --cacert ca.pem --user acme.app --pass '<password>'
```

```text
db.example.com:6379> SET session:42 '{"user":7}' EX 1800
OK
db.example.com:6379> GET session:42
"{\"user\":7}"
```

The forms of login:

| Command | Logs in as |
|---|---|
| `AUTH acme.app <password>` | User `app` of tenant `acme` |
| `AUTH app <password>` | `<listener.redis.tenant>.app`, when that setting names a tenant; or user `app` of tenant `acme` when you connected over TLS to a tenant endpoint such as `acme.db.example.com` (see [Tenant endpoints by server name](../security/tls.md#tenant-endpoints-by-server-name)) |
| `AUTH <password>` | `redis.default_user`; while that setting is empty (the default) this fails with `ERR AUTH <password> called without any password configured for the default user. ...` |
| `HELLO 3 AUTH acme.app <password>` | Logs in and switches to RESP3 in one step |

Every command except `AUTH`, `HELLO`, `QUIT` and `RESET` needs a login first, and answers `NOAUTH Authentication required.` before it. A wrong user, password or tenant answers:

```text
WRONGPASS invalid username-password pair or user is disabled.
```

The login runs the same checks as on the other protocols: account locks, expired passwords, `REQUIRE SSL`, `auth.require_tls`, failure delays and the per-address limit. Accounts with `REQUIRE X509`, `REQUIRE SUBJECT` or `REQUIRE ISSUER` need a matching client certificate on the connection (`redis-cli --tls --cert client.pem --key client-key.pem`) and `listener.redis.client_cert` set. A Redis login gives no reason for a refusal; every refusal is `WRONGPASS`. `AUTH` again on the same connection switches to the new user, after ending the old user's subscriptions.

Redis `ACL` users don't exist separately: `ACL WHOAMI`, `ACL LIST`, `ACL USERS`, `ACL GETUSER` and `ACL CAT` answer, but `ACL SETUSER`, `ACL DELUSER` and the other changes fail with 39842. Manage users with SQL instead. See [Accounts and logins](../security/accounts-and-logins.md).

### Naming the client's data centre

If your application runs in several data centres, give each connection a name that ends with `~` and the data centre's id:

```text
CLIENT SETNAME orders~fra1
```

`HELLO 3 AUTH acme.app <password> SETNAME orders~fra1` does the same at login. The text after the last `~` names the data centre, whether you set the name before or after `AUTH`. When it is a registered data centre, the connection's commands count as traffic from it, and SprigDB moves a tenant towards the data centre most of its traffic comes from (see [Clusters and data centres](../concepts/clusters-and-data-centres.md#naming-the-clients-dc)). An unknown id is ignored. The hint never chooses which copy serves a read, and never grants access.

## Protocol

| Feature | Support |
|---|---|
| RESP2 | yes, the default |
| RESP3 (`HELLO 3`) | yes, with maps, sets, doubles, pushes and verbatim strings |
| `HELLO` answer | `server: redis`, `version: 7.2.0` (`redis.server_version`), `mode: standalone`, `role: master`, no modules |
| Inline commands (a plain line, as `telnet` sends) | yes |
| Pipelining | yes |
| Largest bulk string | `redis.max_bulk_bytes`, 16 MiB |
| Largest command | `redis.max_command_bytes`, 64 MiB |
| Cluster mode | no. Connect as a standalone client; `INFO` reports `cluster_enabled:0` |
| Replication, `MONITOR`, `DEBUG`, `SHUTDOWN`, `MODULE` | no |

SprigDB spreads keys over shards itself, so one connection to any gateway reaches every key of the tenant. Don't configure a Redis Cluster client.

## Databases and namespaces

`SELECT n` chooses Redis database `n`, from 0 to `redis.databases` minus one (16 databases by default). Each Redis database is a key-value namespace named `db<n>` in your tenant's database `redis`:

| Redis | SprigDB namespace | Key table in SQL |
|---|---|---|
| `SELECT 0` | `db0` in database `redis` | `redis.db0` |
| `SELECT 3` | `db3` in database `redis` | `redis.db3` |

The database `redis` and the namespace's tables are created the first time a database is used, so the account needs privileges to create and write tables in `redis`. Each tenant has its own `redis` database: two tenants' `session:42` are different keys. `redis.logical_database` changes the name of that database for every tenant.

Because a Redis database is an ordinary SprigDB namespace, you can read it from SQL on the MySQL port:

```sql
SELECT v FROM redis.db0 WHERE k = 'session:42';
```

`SELECT` beyond the limit answers `ERR DB index is out of range`. `MOVE` and `COPY ... DB` work between databases. `SWAPDB` isn't supported.

## What works

| Family | Support | Notes |
|---|---|---|
| Strings and bit operations | yes | Every Redis 7.2 command, including `SET` options, `LCS`, `BITFIELD` |
| Keys and expiry | yes | `SCAN` cursors are positions in SprigDB's routing order; iterate until `0`. `DUMP`, `RESTORE` and `MIGRATE` aren't supported |
| Hashes | yes | Hash fields can't expire individually (`HEXPIRE` and the other 7.4 commands aren't in 7.2) |
| Lists, including blocking pops | yes | `BLPOP` and the others wake when any gateway pushes to the key |
| Sets | yes | |
| Sorted sets | yes | Including `BZPOPMIN`, `BZMPOP`, `ZRANGESTORE` |
| Geo | yes | Stored as sorted sets; see [Geospatial](../data-models/geospatial.md) |
| `SORT`, `SORT_RO` | yes | |
| Transactions: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` | yes | One MariaDB transaction per `EXEC` |
| Lua: `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION`, `FCALL` and the `_RO` forms | yes | Lua 5.1 with `cjson`; every key must be in `KEYS`. `SCRIPT DEBUG` and `FUNCTION DUMP`/`RESTORE` aren't supported |
| Pub/sub, sharded pub/sub, `PUBSUB` | yes | Across every gateway of the data centre; channels belong to the tenant |
| Keyspace notifications | yes | Off by default; `CONFIG SET notify-keyspace-events` or `redis.notify_keyspace_events` |
| `CLIENT` | partly | `ID`, `SETNAME`, `GETNAME`, `INFO`, `LIST`, `SETINFO`, `NO-EVICT`, `NO-TOUCH`, `REPLY ON`, `UNBLOCK`, `GETREDIR`, `TRACKINGINFO`. `CLIENT LIST` shows only your own connection. A name given to `SETNAME` (or `HELLO ... SETNAME`) that ends in `~<dc>`, such as `orders~fra1`, also names the client's data centre; see [Naming the client's data centre](#naming-the-clients-data-centre). `CLIENT KILL`, `PAUSE`, `TRACKING` and `CACHING` fail with 39842 |
| `CONFIG` | partly | `CONFIG GET` answers fixed values; `CONFIG SET` accepts only `notify-keyspace-events` |
| `INFO`, `TIME`, `COMMAND`, `ROLE`, `SLOWLOG`, `LATENCY`, `MEMORY`, `LASTSAVE` | yes | |
| `WAIT`, `WAITAOF` | yes | Answer `0` and `0 0`: every write is already committed in MariaDB |
| `SAVE`, `BGSAVE`, `BGREWRITEAOF` | no-op | Succeed and do nothing |
| `CLUSTER` | partly | Answers as a single node, for clients that probe; don't use cluster mode |
| Streams (`XADD`, `XREAD`, `XREADGROUP` and the other `X*` commands) | no | 39840. Use SprigDB's [streams](../data-models/streams.md) over the Kafka protocol |
| HyperLogLog (`PFADD`, `PFCOUNT`, `PFMERGE`) | no | 39840 |
| `SWAPDB`, `MONITOR` | no | 39840 |
| `REPLICAOF`, `SLAVEOF`, `SYNC`, `PSYNC`, `FAILOVER`, `DEBUG`, `SHUTDOWN`, `MODULE` | no | 39842 |
| Modules (RedisJSON, RediSearch) | no | Unknown command |

The full command list, with every option, is in [Key-value](../data-models/key-value.md#commands-by-type).

## How it differs from a Redis server

- **Every write is durable.** A write command is a MariaDB transaction that has committed before you get the reply. There's no snapshot or append-only file to configure, and nothing is evicted to free memory.
- **Keys spread over shards by hash tag.** Keys that share a hash tag, such as `{customer:7}:cart` and `{customer:7}:orders`, always live together, so a `MULTI` or script over them commits inside one MariaDB server. Commands over keys without a common tag work too; they may span shards.
- **Your grants apply.** Each command runs as SQL in your own session, so your privileges and row-level security on `redis.db<n>` decide what you can read and write. A refused command answers `NOPERM this user has no permissions to run this command on this data: ...`.
- **Conflicts are retried.** When two writers deadlock, SprigDB reruns the command up to `redis.write_retries` (10) times before answering `TRYAGAIN`.
- **Limits per key.** A key is at most 1,024 bytes, a hash field or set member at most 2,024 bytes, and a value or list item at most 16 MiB.

### Settings

| Setting | Default | Meaning |
|---|---|---|
| `redis.databases` | `16` | Highest `SELECT` index plus one |
| `redis.logical_database` | `redis` | Database that holds the Redis databases |
| `redis.default_user` | empty | User for one-argument `AUTH`; empty refuses it |
| `redis.server_version` | `7.2.0` | Version in `HELLO` and `INFO` |
| `redis.max_bulk_bytes` | `16777216` | Largest bulk string |
| `redis.max_command_bytes` | `67108864` | Largest command |
| `redis.max_multi_commands` | `10000` | Commands queued in one `MULTI` |
| `redis.max_blocked_per_tenant` | `10000` | Blocked clients per tenant per gateway |
| `redis.block_poll_ms` | `1000` | Safety poll of blocked clients |
| `redis.write_retries` | `10` | Reruns on write conflicts |
| `redis.guarded_batches` | `true` | Run the busiest writes as one round trip |
| `redis.lua_time_limit_ms` | `5000` | Wall time of one script |
| `redis.lua_memory_limit_mb` | `32` | Lua heap of one script |
| `redis.lua_kill_after_ms` | `1000` | `SCRIPT KILL` stops only runs older than this |
| `redis.lua_undeclared_keys` | `refuse` | `refuse`, or `same_shard` to allow keys that route with a declared key |
| `redis.notify_keyspace_events` | empty | Keyspace notification classes |
| `redis.sharded_pubsub_hubs` | `8` | Hubs for sharded channels. Read when a gateway starts: a change applies after you restart the gateways, and every gateway in the data centre must use the same count |
| `redis.scan_small_key_elements` | `128` | `HSCAN`, `SSCAN` and `ZSCAN` return keys this small whole |

## Client examples

Python, with redis-py:

```python
import os, redis

r = redis.Redis(host="db.example.com", port=6379, ssl=True, ssl_ca_certs="ca.pem",
                username="acme.app", password=os.environ["REDIS_PASSWORD"], db=0)
r.set("session:42", '{"user":7}', ex=1800)
print(r.get("session:42"))
```

Node.js, with node-redis:

```js
import { createClient } from 'redis';

const client = createClient({
  url: 'rediss://db.example.com:6379/0',
  username: 'acme.app',
  password: process.env.REDIS_PASSWORD,
  socket: { ca: fs.readFileSync('ca.pem') },
});
await client.connect();
await client.hSet('customer:7', { name: 'Asha', city: 'Pune' });
```

Go, with go-redis:

```go
rdb := redis.NewClient(&redis.Options{
    Addr:      "db.example.com:6379",
    Username:  "acme.app",
    Password:  os.Getenv("REDIS_PASSWORD"),
    DB:        0,
    TLSConfig: &tls.Config{ServerName: "db.example.com", RootCAs: pool},
})
err := rdb.Set(ctx, "session:42", `{"user":7}`, 30*time.Minute).Err()
```

Use a client in standalone mode. A connection URL with a user name is `rediss://acme.app:<password>@db.example.com:6379/0`; URL-encode the password if it has special characters.

## Errors you may see

Redis's own errors keep Redis 7.2's texts: `WRONGTYPE`, `ERR syntax error`, `ERR wrong number of arguments for '...' command`, `ERR unknown command`, `EXECABORT`, `NOSCRIPT`, `NOTBUSY`. SprigDB's own errors start with `SPRIG`, then a name and the number:

```text
SPRIG REDIS_UNSUPPORTED 39840 XADD is ...
```

| Reply | When | What to do |
|---|---|---|
| `NOAUTH Authentication required.` | A command before `AUTH` | Log in first |
| `WRONGPASS invalid username-password pair or user is disabled.` | Wrong credentials, unknown tenant, locked account or any other refused login | Check the `tenant.user` name |
| `NOPERM this user has no permissions ...` | Your grants don't cover the table | Ask for privileges on the `redis` database |
| `TRYAGAIN SPRIG REDIS_SHARD_BUSY 39846 ...` | Conflicting writes after every retry | Retry |
| `SPRIG REDIS_UNSUPPORTED 39840` | A Redis 7.2 command SprigDB doesn't offer | See the table above |
| `SPRIG REDIS_LUA_UNSUPPORTED 39841` | `SCRIPT DEBUG` | |
| `SPRIG REDIS_RESTRICTED 39842` | An administrative command; manage SprigDB through SQL, the API or `sprigctl` | |
| `SPRIG REDIS_MULTI_TOO_LARGE 39844` | More than `redis.max_multi_commands` queued | Split the transaction |
| `SPRIG REDIS_TOO_MANY_BLOCKED 39845` | Too many blocked clients for the tenant on this gateway | |
| `SPRIG REDIS_PUBSUB_UNAVAILABLE 39848` | No pub/sub hub reachable | Retry |
| `SPRIG REDIS_LUA_BUDGET_EXCEEDED 39850` | A script ran over its time or memory budget; its writes are rolled back | Shorten the script or raise the limit |
| `SPRIG REDIS_LUA_UNDECLARED_KEY 39851` | A script used a key not in `KEYS` | Pass every key in `KEYS` |
| `SPRIG KV_INVALID_KEY 39723` | A key over 1,024 bytes | |
| `SPRIG KV_VALUE_INVALID 39724` | A value over 16 MiB | |
| `SPRIG KV_ELEMENT_TOO_LONG 39731` | A hash field or member over 2,024 bytes | |
| `NOPROTO unsupported protocol version` | `HELLO` with a version other than 2 or 3 | |

## Related

- [Key-value](../data-models/key-value.md)
- [Connect to SprigDB](overview.md)
- [Accounts and logins](../security/accounts-and-logins.md)
- [TLS](../security/tls.md)
- [Connection problems](../troubleshooting/connections.md)
