---
title: Connect to SprigDB
audience: [user, dba, operator]
sidebar:
  order: 1
---

# Connect to SprigDB

Applications connect to SprigDB with the clients they already use: MySQL and MariaDB drivers, MongoDB drivers, Redis clients, Neo4j drivers, Kafka and AMQP clients, or HTTP. Every gateway serves every protocol, and one SprigDB account logs in on all of them. This page compares the protocols and explains what they share; each protocol has its own page with the details.

## The protocols at a glance

| Protocol | Port | On by default | TLS by default | Logins | Reaches |
|---|---|---|---|---|---|
| [MySQL 8.4](mysql-and-mariadb.md) | 3306 | yes | required | `caching_sha2_password`; `mysql_native_password` when the tenant allows it; `mysql_clear_password` over TLS | Everything, through SQL |
| [MariaDB](mysql-and-mariadb.md) | 3307 | yes | required | As 3306 | Everything, through SQL |
| [MongoDB](mongodb.md) | 27017 | no | required | SCRAM-SHA-256 | Collections, with search and vector indexes |
| [Redis](redis.md) | 6379 | no | required | `AUTH` and `HELLO ... AUTH` with a password | Key-value namespaces |
| [Bolt](bolt-and-cypher.md) | 7687 | no | required | `basic` | Graphs, in Cypher |
| [Kafka](kafka.md) | 9092 | no | required | SASL SCRAM-SHA-512, SCRAM-SHA-256, PLAIN | Event streams, and change topics of tables |
| [AMQP 0-9-1 and 1.0](amqp.md) | 5672 | no | off | SASL PLAIN, AMQPLAIN | Exchanges and queues |
| [AMQP over TLS](amqp.md) | 5671 | no | required | As 5672 | Exchanges and queues |
| [HTTP: REST and gRPC](rest-and-grpc.md) | 8080 | yes | off | HTTP Basic, API keys, session tokens | SQL, Cypher, MongoDB commands, key-value, search, vectors, change streams, logic |
| [HTTPS: REST and gRPC](rest-and-grpc.md) | 8443 | yes | required | As 8080 | As 8080 |

Port 9180 serves `/metrics`, `/healthz` and `/readyz` for monitoring; it takes no client logins. See [Monitoring](../operate/monitoring.md).

The data is the same whichever way you reach it. A collection written from MongoDB is a table to SQL, a Redis database is a table, a stream written by a Kafka producer can be read with `SELECT`, and SQL can publish to queues. See [Cross-model queries](../data-models/cross-model-queries.md).

## Turn a listener on

Each listener has five settings, at the `dc` and `global` scopes:

| Setting | Meaning |
|---|---|
| `listener.<name>.enabled` | Serve the listener |
| `listener.<name>.bind` | Address and port, such as `0.0.0.0:27017` |
| `listener.<name>.tls` | `required`, `optional` or `off` |
| `listener.<name>.cert` | Certificate bundle; `default` unless you configure others |
| `listener.<name>.client_cert` | Ask TLS clients for a certificate: `off` (default), `optional` or `required`. See [Client certificates](../security/tls.md#client-certificates) |

The names are `mysql`, `mariadb`, `mongo`, `redis`, `bolt`, `kafka`, `amqp`, `amqps`, `http` and `https`. An operator changes them with `sprigctl`, for every gateway or for one data centre:

```sh
sprigctl settings set listener.mongo.enabled true --scope global --reason "MongoDB drivers"
sprigctl settings set listener.redis.bind 10.0.4.0:6379 --scope dc:mum1 --reason "private network only"
```

Changes apply without a restart. A new bind address is bound before the old socket closes, and if the bind fails the old listener keeps running. Connections keep the settings they were accepted with, except when a change makes the listener stricter: TLS turned on, the minimum TLS version raised, the listener moved or disabled. Then connections that no longer qualify get `listener.drain_seconds` (default 600) to finish, and are closed at their next safe point; MySQL clients receive 39004 `ER_SPRIG_LISTENER_RETIRED`.

## TLS

| `listener.<name>.tls` | Plaintext client | TLS client |
|---|---|---|
| `required` | Refused | Accepted |
| `optional` | Accepted | Accepted |
| `off` | Accepted | Refused |

How the gateway tells them apart depends on the protocol:

- **MySQL and MariaDB** negotiate TLS inside the protocol, after the server's greeting, as MySQL does. A plaintext client on a `required` listener gets error 3159.
- **Every other protocol** starts TLS with the first byte. With `optional`, the gateway looks at the first byte of the connection and accepts either TLS or plaintext on the same port. With `required`, a plaintext connection is closed; plaintext HTTP gets `400 Bad Request`, "This listener needs TLS.".

| Setting | Default | Meaning |
|---|---|---|
| `tls.min_version` | `1.2` | Lowest TLS version accepted: `1.2` or `1.3` |
| `tls.bundles.default.cert_path` | empty | PEM certificate chain of the default bundle, leaf first; reloaded when the file changes |
| `tls.bundles.default.key_path` | empty | PEM private key of the default bundle |

A tenant can also insist on TLS for its users: `auth.require_tls`, or `REQUIRE SSL` on an account, refuses a login that arrived without TLS, on every protocol. An account can also require a client certificate (`REQUIRE X509`) or a particular one (`REQUIRE SUBJECT`, `REQUIRE ISSUER`), on listeners that ask for client certificates. See [TLS](../security/tls.md).

## One user, every protocol

A SprigDB account is a user of a tenant, created with SQL (`CREATE USER`) or from the MongoDB user commands, and the same user and password log in on every protocol.

### The login name

The login names the tenant: `acme.app` is user `app` of tenant `acme`. The gateway splits the name at the first dot. Tenant names are lower case; the user part is case-sensitive.

There are other ways to name the tenant, for applications that can't change their user names:

| Protocol | Plain user name `app` works when |
|---|---|
| Every protocol, over TLS | the client connects to a tenant endpoint, `acme.<suffix>`, under `listener.tenant_sni_suffixes`. See [Tenant endpoints by server name](../security/tls.md#tenant-endpoints-by-server-name) |
| Redis | `listener.redis.tenant` names a tenant |
| AMQP | `listener.amqp.tenant` names a tenant |
| REST and gRPC | the request carries `x-sprig-tenant: acme` |
| API keys | always: the key itself names its tenant |

A tenant endpoint wins over everything else: there, the whole login name is the user name, so `acme.app` or `globex.app` at `acme.<suffix>` names a user of `acme` with a dot in its name, and usually fails with 1045. Elsewhere, a dotted name wins over the Redis and AMQP listener settings. See [Tenants and databases](../concepts/tenants-and-databases.md).

### How one password works everywhere

When a password is set, SprigDB stores several verifiers of it, one for each kind of login exchange:

| Verifier | Used by |
|---|---|
| `caching_sha2_password` | MySQL and MariaDB logins, and every protocol that sends the password itself: Redis, Bolt, AMQP PLAIN, Kafka PLAIN, HTTP Basic |
| SCRAM-SHA-256 | MongoDB, Kafka SCRAM-SHA-256 |
| SCRAM-SHA-512 | Kafka SCRAM-SHA-512 |
| `mysql_native_password` | Only when `auth.mysql_native_password` is on for the tenant |

The password never leaves the gateway, and SCRAM logins never send it at all. If an account's password was set before SprigDB stored SCRAM verifiers, its SCRAM logins fail until it logs in once with a protocol that sends the password, such as a MySQL login over TLS or HTTP Basic. That login stores the missing verifiers (`auth.backfill_verifiers`, on by default). Setting the password again does the same.

`ALTER USER ... RETAIN CURRENT PASSWORD` keeps the old password working beside the new one, on every protocol, while you rotate it in your applications.

### Shared login rules

Every protocol runs the same checks, in MySQL's order, and shares the counters:

- A wrong password, an unknown user and an unknown tenant all fail the same way, so a probe can't tell which.
- Failed logins count per account and address across protocols. After `auth.delay_after_failures` (3) failures in a row, further attempts are delayed; with `auth.failed_login_attempts` set, the account locks temporarily.
- An address with more than `auth.ip_failures_per_minute` (30) failures a minute on a gateway is refused for `auth.ip_block_seconds` (300).
- `ACCOUNT LOCK`, expired passwords, `REQUIRE SSL` and a suspended tenant are refused on every protocol. Client-certificate requirements are checked on MySQL, MariaDB, MongoDB and Redis, and `REQUIRE X509` on Kafka; accounts with them can't log in on the other protocols.
- Dropping or locking a user closes their open sessions at their next safe point (`auth.close_sessions_on_revoke`).

See [Accounts and logins](../security/accounts-and-logins.md).

### Privileges

A login on any protocol acts with the user's own grants, as SQL statements do: a MongoDB `find` needs `SELECT` on the collection, a Kafka produce needs `INSERT` on the stream, a Redis write needs write privileges on the `redis` database's tables. Row-level security policies apply to SQL, MongoDB, Redis and Kafka reads and to change streams. See [Grants and roles](../security/grants-and-roles.md).

## Behind a load balancer

Every gateway serves every request, so any TCP load balancer works in front of them. To keep the client's real address (for logs, address limits and accounts limited to hosts, such as `'app'@'10.%'`), have the load balancer send a PROXY protocol header:

| Setting | Default | Meaning |
|---|---|---|
| `gateway.proxy_protocol` | `off` | PROXY protocol v1 or v2: `off`, `optional` or `required` |
| `gateway.proxy_trusted_cidrs` | empty | Addresses allowed to send a PROXY header |
| `gateway.handshake_timeout_ms` | `10000` | Longest time for the PROXY header, TLS handshake and login |

Protocol-specific points: MongoDB drivers behind a plain TCP load balancer should use `loadBalanced=true` so cursors stay on their gateway; Neo4j drivers should use the load balancer's address in `bolt.advertise_address`; Kafka clients need `kafka.advertised_host` set to an address they can reach.

## Connection limits

| Setting | Default | What happens past it |
|---|---|---|
| `gateway.max_connections` | `20000` | New connections are refused (MySQL clients get 1040, HTTP 503) |
| `limits.tenant_connections` | `2000` per tenant per gateway | 39001 `ER_SPRIG_TENANT_CONNECTION_LIMIT` |

When a gateway shuts down or drains, it stops being ready, waits `gateway.drain_lb_delay_seconds` for load balancers to notice, and lets busy sessions finish for up to `gateway.drain_timeout_seconds`. Sessions then close at a safe point; MySQL clients see 39000 `ER_SPRIG_GATEWAY_DRAINING`. Reconnect and another gateway takes over.

## Choosing a protocol

- **You have an application already.** Point it at the matching port. A MySQL or MariaDB application uses 3306 or 3307, a MongoDB one 27017, and so on. Only the host, port and user name change.
- **New application, relational data.** Use the MySQL protocol: it reaches every model through SQL, supports transactions across tables, collections, key-value data, streams and queues, and has the most mature drivers. Pick 3306 for MySQL 8.4 syntax or 3307 for MariaDB's.
- **Documents with a flexible shape.** Use MongoDB drivers. SQL can still read and join the same collections.
- **Caches, sessions, counters, leaderboards.** Use Redis clients. The keys are durable, so they survive restarts and failovers.
- **Relationships and paths.** Use Cypher over Bolt, or `GRAPH_TABLE` from SQL.
- **Events many readers replay.** Use Kafka clients on streams.
- **Work that one worker should do once.** Use AMQP clients on queues, or `sprig.mq_*` calls from SQL when the work must commit with your data.
- **Serverless functions, scripts, browsers through a backend.** Use REST, with an API key. Each request is independent, so there are no connections to pool.

## Related

- [How SprigDB works](../concepts/overview.md)
- [Tenants and databases](../concepts/tenants-and-databases.md)
- [Accounts and logins](../security/accounts-and-logins.md)
- [TLS](../security/tls.md)
- [Connection problems](../troubleshooting/connections.md)
