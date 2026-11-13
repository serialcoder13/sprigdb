---
title: MongoDB
audience: [user, dba]
sidebar:
  order: 3
---

# MongoDB

SprigDB speaks the MongoDB wire protocol, so `mongosh`, MongoDB drivers and tools built on them work against SprigDB's document collections. Use it when your application already uses MongoDB, or when documents fit your data better than tables.

This page covers connecting and what the protocol supports. For how documents are stored, indexes, validation, transactions and change streams in depth, see [Documents](../data-models/documents.md).

## Enable the listener

The MongoDB listener is off by default.

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `listener.mongo.enabled` | bool | `false` | Serve MongoDB |
| `listener.mongo.bind` | text | `0.0.0.0:27017` | Address and port |
| `listener.mongo.tls` | text | `required` | `required`, `optional` or `off` |
| `listener.mongo.cert` | text | `default` | Certificate bundle |
| `listener.mongo.client_cert` | text | `off` | Client certificates: `off`, `optional` or `required` (see [Client certificates](../security/tls.md#client-certificates)) |

```sh
sprigctl settings set listener.mongo.enabled true --scope global --reason "MongoDB drivers"
```

Protocol settings:

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `mongo.max_wire_version` | int | `25` | `maxWireVersion` in `hello` (MongoDB 8.0) |
| `mongo.server_version` | text | `8.0.0` | Version in `buildInfo` |
| `mongo.compressors` | text | `zstd,snappy,zlib` | Compressors, in the server's order of preference |
| `mongo.session_timeout_minutes` | int | `30` | `logicalSessionTimeoutMinutes` |
| `mongo.cursor_timeout_ms` | int | `600000` | Idle cursor lifetime |
| `mongo.max_cursors_per_tenant` | int | `10000` | Open cursors per tenant per gateway |
| `mongo.txn_lifetime_seconds` | int | `60` | How long a transaction may stay open |
| `mongo.max_open_transactions` | int | `10000` | Open transactions per gateway |
| `mongo.max_operation_memory_bytes` | int | `1073741824` | Memory one command may hold in the gateway |
| `mongo.write_conflict_retries` | int | `5` | Internal retries of a write conflict outside transactions |
| `mongo.session_record_retention_minutes` | int | `35` | How long retryable-write results are kept |
| `mongo.server_js` | bool | `true` | Run `$where`, `$function` and `$accumulator` |

## Connect

Log in with your SprigDB account and SCRAM-SHA-256. The user name names the tenant: `acme.app` is user `app` of tenant `acme`. Authenticate against `admin`, and name your SprigDB database as the default database. On a tenant endpoint (`mongodb://app:<password>@acme.db.example.com:27017/...`, with `listener.tenant_sni_suffixes` set by your operator), the host name names the tenant and you log in as plain `app`; see [Tenant endpoints by server name](../security/tls.md#tenant-endpoints-by-server-name).

```sh
mongosh "mongodb://acme.app:<password>@db.example.com:27017/shop?authSource=admin&tls=true&tlsCAFile=ca.pem"
```

```text
shop> db.events.insertOne({ customer_id: 42, type: "checkout", total: NumberDecimal("59.90") })
shop> db.events.find({ customer_id: 42 })
```

A MongoDB database is a SprigDB database of your tenant, and a collection is a table in it. `shop.events` from MongoDB is `shop.events` from SQL too. Databases and collections are created on first use, as in MongoDB.

Connection-string options:

| Option | Use |
|---|---|
| `authSource=admin` | Users are reported in `admin`, the database drivers authenticate against |
| `authMechanism=SCRAM-SHA-256` | Optional: it is the only mechanism, and drivers pick it from `saslSupportedMechs` |
| `tls=true` | Needed while `listener.mongo.tls` is `required` |
| `directConnection=true` | Optional; a single gateway address works either way |
| `compressors=zstd,snappy,zlib` | Any of the three |
| `retryWrites=true` | The default in drivers; supported |
| `appName=...` | Shown in `$currentOp` and `$queryStats`. A name ending in `~<dc>`, such as `orders~fra1`, also tells SprigDB which data centre the client runs in; see [Naming the client's data centre](#naming-the-clients-data-centre) |

### Naming the client's data centre

If your application runs in several data centres, end its `appName` with `~` and the data centre's id:

```text
mongodb://acme.app:<password>@db.example.com:27017/shop?authSource=admin&tls=true&appName=orders~fra1
```

The text after the last `~` is taken from the first `hello`. When it is a registered data centre, the connection's commands count as traffic from it, and SprigDB moves a tenant towards the data centre most of its traffic comes from (see [Clusters and data centres](../concepts/clusters-and-data-centres.md#naming-the-clients-dc)). An unknown id is ignored. The hint never chooses which copy serves a read, and never grants access.

To spread load over several gateways, list them in the connection string, or put them behind a load balancer and add `loadBalanced=true`. The gateway presents itself as a `mongos` (`msg: "isdbgrid"`), so drivers use the sharded-cluster rules: any gateway serves any request.

A cursor lives on the gateway that opened it. Drivers send `getMore` on the same connection when they know the topology, but behind a plain TCP load balancer a `getMore` can reach another gateway and fail with 43 `CursorNotFound` ("it was opened on another gateway"). Use `loadBalanced=true` there, which pins each cursor to its connection.

### Driver examples

Python, with PyMongo:

```python
from pymongo import MongoClient

client = MongoClient("mongodb://db.example.com:27017/?authSource=admin&tls=true",
                     username="acme.app", password=os.environ["DB_PASSWORD"],
                     tlsCAFile="ca.pem")
events = client.shop.events
events.insert_one({"customer_id": 42, "type": "view"})
print(events.count_documents({"customer_id": 42}))
```

Node.js:

```js
import { MongoClient } from 'mongodb';

const client = new MongoClient('mongodb://db.example.com:27017/?authSource=admin&tls=true', {
  auth: { username: 'acme.app', password: process.env.DB_PASSWORD },
  tlsCAFile: 'ca.pem',
});
const events = client.db('shop').collection('events');
await events.updateOne({ _id: 1 }, { $set: { type: 'checkout' } }, { upsert: true });
```

Java:

```java
MongoClient client = MongoClients.create(
    "mongodb://acme.app:" + URLEncoder.encode(password, UTF_8)
    + "@db.example.com:27017/?authSource=admin&tls=true");
MongoCollection<Document> events = client.getDatabase("shop").getCollection("events");
events.find(Filters.eq("customer_id", 42)).forEach(d -> System.out.println(d.toJson()));
```

Go:

```go
opts := options.Client().
    ApplyURI("mongodb://db.example.com:27017/?authSource=admin&tls=true").
    SetAuth(options.Credential{Username: "acme.app", Password: os.Getenv("DB_PASSWORD")})
client, err := mongo.Connect(opts)
coll := client.Database("shop").Collection("events")
```

URL-encode the password if it contains `@`, `:`, `/` or `%`.

## Authentication

| Mechanism | Supported | Notes |
|---|---|---|
| SCRAM-SHA-256 | yes | Including speculative authentication in `hello` and `saslSupportedMechs` |
| SCRAM-SHA-1 | no | It needs a verifier format SprigDB doesn't store |
| MONGODB-X509 | no | A client certificate can be required in addition to SCRAM; see below |
| PLAIN (LDAP), GSSAPI, MONGODB-AWS, MONGODB-OIDC | no | |

A login that fails for any reason answers 18 `AuthenticationFailed`, "Authentication failed.". A second `saslStart` on a connection that is already logged in answers 20 `IllegalOperation`. Commands other than the handshake and `saslStart` before a login answer 13 `Unauthorized`, "Command ... requires authentication".

The login runs the same checks as every other protocol: account locks, expired passwords, `REQUIRE SSL`, `auth.require_tls`, failure delays and the per-address limit. Accounts with `REQUIRE X509`, `REQUIRE SUBJECT` or `REQUIRE ISSUER` log in with SCRAM over a connection that presented a matching client certificate, so set `listener.mongo.client_cert` and pass the certificate with `tlsCertificateKeyFile`. SCRAM needs a SCRAM-SHA-256 verifier for the account. Every password set in SprigDB stores one; see [Connect to SprigDB](overview.md#one-user-every-protocol) for accounts whose password predates that.

### Users and roles

The user and role commands manage the same accounts as SQL's `CREATE USER` and `GRANT`, so a user created from `mongosh` logs in on every protocol:

| Command | Support |
|---|---|
| `createUser`, `updateUser`, `dropUser`, `usersInfo` | yes; `customData`, `authenticationRestrictions` and mechanisms other than SCRAM-SHA-256 aren't supported |
| `grantRolesToUser`, `revokeRolesFromUser` | yes |
| `createRole`, `dropRole`, `rolesInfo`, `grantRolesToRole`, `revokeRolesFromRole` | yes |
| `updateRole`, `grantPrivilegesToRole`, `revokePrivilegesFromRole`, `dropAllUsersFromDatabase` | no |

A MongoDB role `(role, db)` becomes the SQL role `mongo.<db>.<role>`. Built-in roles are created on first use with matching privileges: `read` is `SELECT` and `WATCH` on the database, and `readWrite` adds `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALTER` and `INDEX`. A custom role's `changeStream` action becomes `WATCH`. See [Grants and roles](../security/grants-and-roles.md).

```js
db.getSiblingDB("admin").createUser({
  user: "reporting", pwd: "s3cret-Passw0rd",
  roles: [{ role: "read", db: "shop" }]
})
```

## Protocol

| Feature | Support |
|---|---|
| `OP_MSG`, including checksums and document sequences | yes |
| `OP_QUERY` | For the legacy `isMaster` handshake only |
| `OP_COMPRESSED` with zstd, snappy and zlib | yes |
| Wire versions | 0 to 25 (MongoDB 8.0) |
| Topology | A `mongos` of a sharded cluster; load-balanced mode with `loadBalanced=true` |
| Largest document | 16 MiB (`maxBsonObjectSize`) |
| Largest message | 48,000,000 bytes (`maxMessageSizeBytes`) |
| Largest write batch | 100,000 operations (`maxWriteBatchSize`) |
| Stable API version 1 | accepted; another version fails with 322 |
| Sessions, causal consistency, `$clusterTime` | yes; cluster time is unsigned |
| Retryable writes and reads | yes |
| Read preference | accepted; SprigDB decides which copy serves a read, and you always read your own writes. See [Read routing](../performance/read-routing.md) |
| Write concern | accepted; every write has committed in MariaDB when it is acknowledged |

## Commands

| Area | Commands | Support |
|---|---|---|
| Handshake and diagnostics | `hello`, `isMaster`, `ping`, `buildInfo`, `connectionStatus`, `serverStatus`, `hostInfo`, `getParameter`, `whatsmyuri`, `getLog`, `getCmdLineOpts`, `logout` | yes; `getLog` and `getCmdLineOpts` answer empty |
| Sessions | `startSession`, `endSessions`, `killSessions`, `killAllSessions`, `refreshSessions` | yes |
| CRUD | `insert`, `find`, `update`, `delete`, `findAndModify`, `count`, `distinct`, `aggregate`, `bulkWrite` (MongoDB 8.0's, across collections) | yes |
| Cursors | `getMore`, `killCursors`; tailable and `awaitData` cursors on capped collections and change streams | yes |
| Transactions | `commitTransaction`, `abortTransaction`, with recovery tokens | yes |
| Collections and databases | `create`, `drop`, `dropDatabase`, `listDatabases`, `listCollections`, `renameCollection`, `collMod`, `convertToCapped`, `cloneCollectionAsCapped`, `dbStats`, `collStats` | yes; `renameCollection` within one database only |
| Indexes | `createIndexes`, `listIndexes`, `dropIndexes` | partly; see [Indexes](#indexes). `collMod` changes a TTL index's `expireAfterSeconds` |
| Search and vector indexes | `createSearchIndexes`, `updateSearchIndex`, `dropSearchIndex` | yes; see [Search](../data-models/search.md) and [Vectors](../data-models/vectors.md) |
| `explain` | for `find`, `aggregate`, `count`, `distinct`, `update`, `delete` | yes; other commands answer 39662 |
| Users and roles | see [above](#users-and-roles) | partly |
| Query settings | `setQuerySettings`, `removeQuerySettings`, `$querySettings`, `$queryStats` | yes |
| SprigDB extensions | `sprigCall` (logic functions), `sprigCreatePolicy`, `sprigDropPolicy`, `sprigRowSecurity` | yes |
| Sharding | `shardCollection`, `enableSharding` | accepted as a hint only; SprigDB shards collections itself |
| `mapReduce`, `eval`, `geoNear` | | no: 115 `CommandNotSupported` |
| Replication and server administration, such as `replSetGetStatus`, `fsync` and `shutdown` | | no: 59 `CommandNotFound` |

### Queries, updates and aggregation

MongoDB 8.0's query operators, update operators and aggregation expressions work, with MongoDB's type ordering, numeric promotion and decimal arithmetic. The exceptions, such as the `$slice` aggregation expression, are listed in [Documents](../data-models/documents.md#not-supported). Aggregation stages:

| Stages | Support |
|---|---|
| `$match`, `$project`, `$addFields`, `$set`, `$unset`, `$replaceRoot`, `$replaceWith`, `$sort`, `$skip`, `$limit`, `$sample`, `$count`, `$group`, `$sortByCount`, `$unwind`, `$facet`, `$bucket`, `$bucketAuto`, `$setWindowFields`, `$densify`, `$fill`, `$redact`, `$documents` | yes |
| `$lookup`, `$graphLookup`, `$unionWith` | yes; `$lookup` with `let` isn't supported, use `pipeline` |
| `$out`, `$merge` | yes; `$merge` matches on `_id` only, without a `whenMatched` pipeline |
| `$geoNear` | yes, with a `2dsphere` index |
| `$search`, `$searchMeta`, `$vectorSearch` | yes, over SprigDB's search and vector indexes |
| `$changeStream` | yes |
| `$collStats`, `$indexStats`, `$planCacheStats`, `$currentOp`, `$listSessions`, `$listLocalSessions` | yes |

Blocking stages and sorts may use `doc.agg_stage_memory_bytes` and `doc.max_sort_bytes` (100 MiB each). Past that they spill to local disk when `allowDiskUse` is true, the default; with it false, the command fails with 292 `QueryExceededMemoryLimitNoDiskUseAllowed`.

`$where`, `$function` and `$accumulator` run JavaScript in a sandbox while `mongo.server_js` is on (the default).

### Indexes

| Index | Support |
|---|---|
| Single-field, compound, ascending, descending | yes; one MariaDB index over generated columns |
| `unique` | yes |
| `hashed` | accepted; built as an ascending index |
| `text` | yes, one per collection, as a search index |
| `2dsphere` | yes, on one path |
| Vector search indexes | yes, with `createSearchIndexes` |
| Multikey (indexing array elements) | no; queries on array fields are checked in the gateway |
| Wildcard (`$**`), `2d`, `geoHaystack` | no |
| `sparse`, `partialFilterExpression`, `collation`, `hidden` | kept and listed, but the index holds every document |
| TTL (`expireAfterSeconds`) | yes: expired documents are deleted in the background; see [Documents](../data-models/documents.md#ttl-indexes) |

SprigDB also creates indexes on its own from your queries; `listIndexes` hides those unless you pass `showAutoIndexes: true`. See [Automatic indexing](../performance/automatic-indexing.md).

### Transactions and sessions

Multi-document transactions work through the drivers' session API, across collections and databases. A transaction is one MariaDB transaction; a driver that retries `commitTransaction` on another gateway gets the real outcome. `readConcern: "snapshot"` works inside transactions and for reads outside them. See [Documents](../data-models/documents.md#transactions).

### Change streams

`watch()` on a collection, a database or the whole deployment works, with `fullDocument`, `fullDocumentBeforeChange`, `resumeAfter`, `startAfter` and `showExpandedEvents`. `startAtOperationTime` isn't supported (39662). A stream needs `find` and `changeStream` on what it watches: in SprigDB's terms `SELECT` and `WATCH`. The built-in `read` and `readWrite` roles include both; see [Grants and roles](../security/grants-and-roles.md#change-streams-need-watch). See [Documents](../data-models/documents.md#change-streams).

## Not supported

| Feature | What happens |
|---|---|
| SCRAM-SHA-1, MONGODB-X509, LDAP, Kerberos, AWS and OIDC logins | Authentication fails |
| Views, time-series collections, clustered collections, Queryable Encryption | 39662 `SprigDocFeatureUnsupported` |
| `mapReduce`, `eval`, the `geoNear` command | 115 `CommandNotSupported` |
| Multikey, wildcard and `2d` indexes; sparse and partial semantics | See [Indexes](#indexes) |
| A collection's default collation | Collation per command works |
| `startAtOperationTime` in change streams | 39662 |
| Replica-set commands and topology | SprigDB presents a `mongos` |

## Tested clients

SprigDB's MongoDB tests run:

- the MongoDB driver specification tests in the unified format (CRUD, transactions, retryable writes, change streams and sessions), through the Rust driver, against SprigDB and a MongoDB 8.0 reference;
- a differential suite that runs the same commands against MongoDB 8.0 and SprigDB and compares the results, including thousands of aggregation-expression and validation cases;
- Spring Data MongoDB 5.1 over the Java sync driver 5.8;
- Mongoose 8's own test suite;
- `mongosh` in the deployment smoke test.

## Errors you may see

MongoDB clients receive MongoDB's codes and `codeName`s, and SprigDB's own errors with their 39xxx number and a `codeName` that starts with `Sprig`.

| Code | `codeName` | When |
|---|---|---|
| 13 | `Unauthorized` | A command before logging in, or your grants don't cover the collection; a change stream on a collection without `WATCH` |
| 18 | `AuthenticationFailed` | Wrong credentials, unknown tenant, a missing or wrong client certificate for the account, or any other refused login |
| 43 | `CursorNotFound` | The cursor idled past `mongo.cursor_timeout_ms`, was killed, or was opened on another gateway |
| 59 | `CommandNotFound` | A command SprigDB doesn't have |
| 115 | `CommandNotSupported` | `mapReduce`, `eval`, `geoNear` |
| 146 | `ExceededMemoryLimit` | A command held more than `mongo.max_operation_memory_bytes` |
| 251 | `NoSuchTransaction` | The transaction ended, timed out after `mongo.txn_lifetime_seconds`, or never committed |
| 263 | `OperationNotSupportedInTransaction` | A command that can't run in a transaction |
| 292 | `QueryExceededMemoryLimitNoDiskUseAllowed` | A blocking stage ran out of memory with `allowDiskUse: false` |
| 509 | `TooManyOpenTransactions` | More than `mongo.max_open_transactions` on the gateway |
| 10334 | `BSONObjectTooLarge` | A document over 16 MiB |
| 11000 | `DuplicateKey` | A unique index refused the write |
| 39630 | `SprigCursorLimit` | More than `mongo.max_cursors_per_tenant` cursors open |
| 39660 | `SprigDocIdTooLong` | An encoded `_id` over 1,024 bytes |
| 39662 | `SprigDocFeatureUnsupported` | A feature in the "Not supported" table |
| 39664 | `SprigPathPublishTimeout` | A new array path wasn't known to every gateway in time; retry |
| 39665 | `SprigDocTooDeep` | A document nested more than 32 levels |

Transient errors carry MongoDB's error labels (`RetryableWriteError`, `TransientTransactionError`), so drivers retry them as they would against MongoDB. See [Error codes](../reference/errors.md).

## Related

- [Documents](../data-models/documents.md)
- [Connect to SprigDB](overview.md)
- [Accounts and logins](../security/accounts-and-logins.md)
- [Search](../data-models/search.md)
- [Connection problems](../troubleshooting/connections.md)
