---
title: Documents
audience: [user, dba]
sidebar:
  order: 2
---

# Documents

SprigDB stores MongoDB collections as MariaDB tables and serves them to MongoDB drivers, `mongosh` and Compass on port 27017. Use documents when your data has a flexible shape, or when you already have code written against MongoDB. The same collections are readable from SQL and over HTTP.

## How documents are stored

A MongoDB database is a SprigDB database: `shop` in a MongoDB connection string is the same `shop` you see from SQL. A collection is a table in that database, with one row per document:

| Column | Type | Holds |
|---|---|---|
| `_id` | `VARBINARY(1024)`, primary key | The document's `_id`, encoded so that byte order is MongoDB's sort order |
| `_t` | `VARCHAR(255)` | Reserved; `NULL` |
| `doc` | `JSON` | The whole document as JSON text, `_id` first, fields in the order you wrote them |
| `_types` | `JSON` | The type map: the BSON type of each value that JSON can't express; `NULL` when there are none |
| `_v` | `BIGINT UNSIGNED` | A version stamp of the last write, used for optimistic concurrency and the change feed |

When you index a field, SprigDB adds hidden, generated columns for it (see [Indexes](#indexes)). They are `INVISIBLE`, so `SELECT *` doesn't show them.

The table takes the collection's name when the name is at most 40 characters of `A-Z`, `a-z`, `0-9` and `_` and doesn't start with `_sprig`. Other names get a table called `c` followed by 16 hex digits. MariaDB sees only the table; drivers always see the collection name.

A collection belongs to a table group, like any table, so it shares shards with the tables and keys that have the same distribution key. Its distribution key is `_id` by default. See [Shards and table groups](../concepts/shards-and-table-groups.md).

### Types and the type map

The document is stored as JSON, and the type map records what JSON loses, so MongoDB clients read back exactly the BSON types they wrote:

| BSON type | In `doc` | Entry in `_types` |
|---|---|---|
| string, object, array, boolean, null, int32 | itself | none |
| int64 | integer | `long` when the value fits in int32 |
| double | number with a fraction or exponent (`5.0`); NaN and ±Infinity as strings | `double` for NaN and ±Infinity only |
| Decimal128 | number with its exact digits (`59.90`); non-finite or out-of-range values as strings | `decimal` |
| ObjectId | 24 lower-case hex digits | `objectId` |
| date | `"2026-11-07T10:15:00.000Z"` for years 1000 to 9999, otherwise integer milliseconds | `date` |
| binary | canonical UUID text for subtype 4, otherwise base64 | `uuid`, or `binData:<subtype>` |
| Timestamp | `"<seconds>.<increment>"`, each zero-padded to 10 digits | `timestamp` |
| regular expression | `"/<pattern>/<flags>"` | `regex` |
| DBPointer | `"<namespace>:<24 hex>"` | `dbPointer` |
| symbol, JavaScript | the string | `symbol`, `javascript` |
| JavaScript with scope | `{"$code": ..., "$scope": ...}` | `javascriptWithScope` |
| undefined, MinKey, MaxKey | `null`, `"MinKey"`, `"MaxKey"` | `undefined`, `minKey`, `maxKey` |

Without an entry, an integer that fits in 32 bits reads back as int32, a larger one as int64, and a number with a fraction or exponent as a double. Type map keys are dotted paths with array positions, such as `{"_id": "objectId", "at": "date", "items.0.price": "decimal"}`. A `.` or `\` inside a field name is escaped with `\`.

For example, this document:

```js
db.events.insertOne({
  _id: ObjectId("6730b2f0c1a4e5d6f7a8b9c0"),
  customer_id: 42,
  type: "checkout",
  at: ISODate("2026-11-07T10:15:00Z"),
  total: NumberDecimal("59.90")
})
```

is stored as:

```text
doc     {"_id": "6730b2f0c1a4e5d6f7a8b9c0", "customer_id": 42, "type": "checkout",
         "at": "2026-11-07T10:15:00.000Z", "total": 59.90}
_types  {"_id": "objectId", "at": "date", "total": "decimal"}
```

Numbers are read back with their exact digits, never through a binary double, so `NumberDecimal("59.90")` stays `59.90`.

## Use documents from MongoDB clients

Turn on the MongoDB listener first. It is off by default and requires TLS by default:

| Setting | Default | Meaning |
|---|---|---|
| `listener.mongo.enabled` | `false` | Serve the MongoDB listener |
| `listener.mongo.bind` | `0.0.0.0:27017` | Address and port |
| `listener.mongo.tls` | `required` | `required`, `optional` or `off` |

Then connect as a tenant user. The login names the tenant: `acme.app` is user `app` of tenant `acme`.

```sh
mongosh "mongodb://acme.app:<password>@db.example.com:27017/shop?authSource=admin&tls=true"
```

The gateway answers as MongoDB 8.0 (`maxWireVersion` 25) and as a `mongos`. Drivers, retryable writes, sessions, compression (`zstd`, `snappy`, `zlib`) and SCRAM logins work as they do against MongoDB. See [MongoDB](../connect/mongodb.md) for connection options and driver examples.

### Create collections

Collections and databases are created on the first insert, as in MongoDB. Create one explicitly when you want options such as a validator or a cap:

```js
db.createCollection("events")
db.events.insertMany([
  { _id: 1, customer_id: 42, type: "view",     tags: ["web"] },
  { _id: 2, customer_id: 42, type: "checkout", tags: ["web", "promo"] },
  { _id: 3, customer_id: 7,  type: "view",     tags: [] }
])
```

A document without an `_id` gets a new ObjectId. `_id` can be any type except an array, a regular expression or undefined (53 `InvalidIdField`), and an `_id` that is a document can't contain `$`-prefixed fields (53). Once written, `_id` can't change: an update that changes it fails with 66 `ImmutableField`.

The collection commands that work: `create`, `drop`, `dropDatabase`, `renameCollection` (within one database), `listCollections`, `listDatabases`, `collMod`, `convertToCapped`, `cloneCollectionAsCapped`, `dbStats` and `collStats`.

### Read

`find`, `count`, `distinct`, `aggregate` and `getMore` work with sort, skip, limit, projection (including `$slice` and `$elemMatch`), `hint`, `collation`, `batchSize` and `maxTimeMS`:

```js
db.events.find({ type: "checkout", customer_id: { $gte: 40 } })
         .sort({ customer_id: -1 }).skip(0).limit(5)
         .projection({ _id: 0, customer_id: 1 })

db.events.countDocuments({ tags: "web" })     // 2
db.events.distinct("type")                    // [ "checkout", "view" ]
```

Query operators:

| Kind | Operators |
|---|---|
| Comparison | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin` |
| Logical | `$and`, `$or`, `$nor`, `$not` |
| Element | `$exists`, `$type` |
| Evaluation | `$expr`, `$jsonSchema`, `$mod`, `$regex` (with `$options`), `$text`, `$where` |
| Array | `$all`, `$elemMatch`, `$size` |
| Bitwise | `$bitsAllClear`, `$bitsAllSet`, `$bitsAnyClear`, `$bitsAnySet` |
| Geospatial | `$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere`, with `$geometry`, `$box`, `$polygon`, `$center`, `$centerSphere`, `$maxDistance`, `$minDistance` |
| Other | `$comment` |

Filters follow MongoDB's rules: a condition on a field that holds an array matches any element or the array as a whole, comparisons stay within a type (a number never matches a string), and `1`, `NumberLong(1)`, `1.0` and `NumberDecimal("1.00")` are equal.

SprigDB turns the filter into SQL where it can, and then checks every candidate document with MongoDB's own rules in the gateway, so results are exact whatever the SQL selects. Regular expressions use an engine without look-around or back-references; a pattern that needs them fails with 2 `BadValue`.

`$text` is covered in [Search](search.md), and the geospatial operators in [Geospatial](geospatial.md).

### Write

`insert`, `update`, `delete`, `findAndModify` and MongoDB 8.0's `bulkWrite` work, ordered and unordered, with upserts:

```js
db.events.updateOne({ _id: 1 }, { $inc: { views: 1 }, $set: { seen: true } })
db.events.updateMany({ type: "view" }, { $set: { checked: 1 } })
db.events.updateOne({ _id: 1000 }, { $set: { type: "view" } }, { upsert: true })
db.events.replaceOne({ _id: 1000 }, { replaced: true })
db.events.findOneAndUpdate({ _id: 2 }, { $set: { total: -2 } }, { returnDocument: "after" })
db.events.deleteMany({ customer_id: { $lt: 10 } })
```

Update operators:

| Kind | Operators |
|---|---|
| Fields | `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`, `$currentDate`, `$setOnInsert` |
| Arrays | `$push` (with `$each`, `$slice`, `$sort`, `$position`), `$addToSet` (with `$each`), `$pop`, `$pull`, `$pullAll` |
| Bitwise | `$bit` |
| Positional | `$`, `$[]`, `$[<identifier>]` with `arrayFilters` |

An update can also be a pipeline of `$addFields`, `$set`, `$project`, `$unset`, `$replaceRoot` and `$replaceWith`; any other stage fails with 2 `BadValue`. Operations apply in MongoDB 5.0's order, by field path, so new fields appear in the same order as in MongoDB.

```js
db.events.updateOne(
  { _id: 2 },
  { $set: { "items.$[e].qty": 0 } },
  { arrayFilters: [ { "e.sku": "S0" } ] }
)
```

Each document write is one MariaDB transaction. Updates read the document, apply the operators in the gateway and write it back only if its `_v` hasn't changed. If another writer got there first, the gateway retries; outside transactions a command that hits a deadlock or write conflict is run again up to `mongo.write_conflict_retries` times (default 5). After that the client gets 112 `WriteConflict`.

A duplicate `_id` or unique-index value fails with 11000 `DuplicateKey`, with `keyPattern` and `keyValue` as MongoDB gives them.

### Aggregate

`aggregate` runs these stages. The leading `$match`, `$sort`, `$skip` and `$limit` become SQL on the collection; every other stage runs in the gateway.

| Kind | Stages |
|---|---|
| Filter and shape | `$match`, `$project`, `$addFields`, `$set`, `$unset`, `$replaceRoot`, `$replaceWith`, `$redact` |
| Order and page | `$sort`, `$skip`, `$limit`, `$sample` |
| Group and count | `$group`, `$count`, `$sortByCount`, `$bucket`, `$bucketAuto`, `$facet` |
| Arrays | `$unwind` |
| Windows and gaps | `$setWindowFields`, `$densify`, `$fill` |
| Joins | `$lookup`, `$graphLookup`, `$unionWith` |
| Output | `$out`, `$merge` |

These must be the first stage of the pipeline (40602 otherwise):

| Stage | Purpose |
|---|---|
| `$documents` | Literal documents, in `db.aggregate([...])` without a collection |
| `$changeStream` | A change stream (what `watch()` sends); see [Change streams](#change-streams) |
| `$geoNear` | Nearest documents; see [Geospatial](geospatial.md) |
| `$search`, `$searchMeta`, `$rankFusion`, `$scoreFusion`, `$listSearchIndexes` | Full-text and hybrid search; see [Search](search.md) |
| `$vectorSearch` | Nearest vectors; see [Vectors](vectors.md) |
| `$collStats`, `$indexStats`, `$planCacheStats`, `$listClusterCatalog` | Collection and index statistics |
| `$currentOp`, `$listSessions`, `$listLocalSessions` | Running operations and sessions |
| `$querySettings`, `$queryStats` | Query settings and query statistics |

```js
db.events.aggregate([
  { $match: { customer_id: { $lt: 200 } } },
  { $group: { _id: "$type", count: { $sum: 1 } } },
  { $sort: { _id: 1 } }
])
```

Points to know:

- `$sample` returns the first `size` documents of its input, not a random choice.
- `$lookup` takes `from`, `localField`, `foreignField`, `as` and `pipeline`, and reads collections of the same database. It works in batches of 500 input documents, each one query with `$in` on the foreign field.
- `$graphLookup` searches breadth first, one read of the `from` collection per level, within `doc.graph_lookup_max_bytes` per input document.
- `$merge` matches on `_id` only. `whenMatched` takes `merge` (the default), `replace`, `keepExisting` or `fail`; `whenNotMatched` takes `insert` (the default), `discard` or `fail`.
- `$out` and `$merge` write outside transactions. `$out` re-creates its target with the target's old options, validator included.

**Accumulators** (`$group`, `$bucket`, `$bucketAuto` and windows): `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`, `$count`, `$mergeObjects`, `$concatArrays`, `$setUnion`, `$top`, `$topN`, `$bottom`, `$bottomN`, `$firstN`, `$lastN`, `$maxN`, `$minN`, `$median`, `$percentile`, `$stdDevPop`, `$stdDevSamp` and `$accumulator`.

**Window functions** (`$setWindowFields`, document and range windows, with time units): the accumulators above plus `$rank`, `$denseRank`, `$documentNumber`, `$shift`, `$derivative`, `$integral`, `$expMovingAvg`, `$covariancePop`, `$covarianceSamp`, `$linearFill` and `$locf`.

**Expressions** run in the gateway with MongoDB's null handling, numeric promotion and error codes:

| Kind | Expressions |
|---|---|
| Arithmetic | `$abs`, `$add`, `$ceil`, `$divide`, `$exp`, `$floor`, `$ln`, `$log`, `$log10`, `$mod`, `$multiply`, `$pow`, `$round`, `$sqrt`, `$subtract`, `$trunc` |
| Trigonometry | `$sin`, `$cos`, `$tan`, `$asin`, `$acos`, `$atan`, `$atan2`, `$sinh`, `$cosh`, `$tanh`, `$asinh`, `$acosh`, `$atanh`, `$degreesToRadians`, `$radiansToDegrees` |
| Bitwise | `$bitAnd`, `$bitOr`, `$bitXor`, `$bitNot` |
| Comparison and logic | `$cmp`, `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$and`, `$or`, `$not` |
| Conditional | `$cond`, `$ifNull`, `$switch` |
| Arrays | `$arrayElemAt`, `$arrayToObject`, `$concatArrays`, `$filter`, `$first`, `$last`, `$firstN`, `$lastN`, `$maxN`, `$minN`, `$in`, `$indexOfArray`, `$isArray`, `$map`, `$objectToArray`, `$range`, `$reduce`, `$reverseArray`, `$size`, `$sortArray`, `$zip` |
| Sets | `$allElementsTrue`, `$anyElementTrue`, `$setDifference`, `$setEquals`, `$setIntersection`, `$setIsSubset`, `$setUnion` |
| Strings | `$concat`, `$indexOfBytes`, `$indexOfCP`, `$ltrim`, `$rtrim`, `$trim`, `$regexFind`, `$regexFindAll`, `$regexMatch`, `$replaceAll`, `$replaceOne`, `$split`, `$strcasecmp`, `$strLenBytes`, `$strLenCP`, `$substr`, `$substrBytes`, `$substrCP`, `$toLower`, `$toUpper` |
| Dates | `$dateAdd`, `$dateDiff`, `$dateFromParts`, `$dateFromString`, `$dateSubtract`, `$dateToParts`, `$dateToString`, `$dateTrunc`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`, `$hour`, `$isoDayOfWeek`, `$isoWeek`, `$isoWeekYear`, `$millisecond`, `$minute`, `$month`, `$second`, `$week`, `$year` |
| Types | `$convert`, `$isNumber`, `$type`, `$toBool`, `$toDate`, `$toDecimal`, `$toDouble`, `$toInt`, `$toLong`, `$toObjectId`, `$toString`, `$toUUID`, `$toArray`, `$toObject`, `$bsonSize`, `$binarySize`, `$subtype`, `$tsIncrement`, `$tsSecond` |
| Objects | `$getField`, `$setField`, `$unsetField`, `$mergeObjects` |
| Group values | `$sum`, `$avg`, `$min`, `$max` over an array |
| Other | `$literal`, `$let`, `$rand`, `$meta`, `$function`, `$hash`, `$hexHash`, `$serializeEJSON`, `$deserializeEJSON`, `$createObjectId`, `$currentDate` |

Time zones are Olson names or UTC offsets. Variables `$$ROOT`, `$$CURRENT` and `$$NOW` work, and `$map` takes `arrayIndexAs` (`$$IDX`). Some MongoDB 8.1 to 8.3 forms are on as well, such as `$toString` of any type and `$convert` with `base` and `byteOrder`.

### Server-side JavaScript

`$where`, `$function` and `$accumulator` run in SprigDB's embedded JavaScript engine while `mongo.server_js` is on (the default; scope tenant, data centre or global). All the code of one command shares `logic.function_budget_ms` of JavaScript time and a heap of `logic.memory_limit_mb`. Past the time, `$where` fails with 50 `MaxTimeMSExpired` and the others with 11601 `Interrupted`. With `mongo.server_js` off, they fail with MongoDB's errors for disabled JavaScript.

### Collation

A command's `collation` option applies to its filters, sorts, `distinct`, `$in` and ranges, and to `getMore` on its cursor. Strength 1 to 5, `caseLevel`, `caseFirst` and `numericOrdering` work; every locale compares as the root locale. While a collation is active, string conditions aren't pushed into SQL, so the query reads more rows.

## Indexes

Create, list and drop indexes as in MongoDB:

```js
db.events.createIndex({ customer_id: 1, at: -1 })
db.events.createIndex({ type: 1 }, { unique: true, name: "type_unique" })
db.events.getIndexes()
db.events.dropIndex("customer_id_1_at_-1")
```

For each indexed path SprigDB adds three hidden generated columns over `doc`: the value's type class, its numeric value and its string value (the first 160 characters). It then creates one MariaDB index over those columns, in key order and direction, named `sprig_dix_<n>`. MariaDB keeps the columns and the index up to date on every write, and MariaDB's optimiser chooses among them. `listIndexes` shows the MongoDB name and options.

| Key | What you get |
|---|---|
| `1`, `-1`, compound | A MariaDB index over the field columns |
| `"hashed"` | Accepted; built as an ascending index, which serves equality |
| `"text"` | A full-text index, one per collection; see [Search](search.md) |
| `"2dsphere"` | A geospatial index on one path; see [Geospatial](geospatial.md) |
| `createSearchIndexes` with `vectorSearch` | A vector index; see [Vectors](vectors.md) |

Rules and limits:

- `unique: true` makes a unique MariaDB index. If existing documents break it, the build fails with 11000 `DuplicateKey`. Strings are compared by their first 160 characters, so two strings that share those characters count as duplicates.
- `sparse`, `partialFilterExpression`, `hidden` and `collation` are kept and shown by `listIndexes`, but don't change what the index holds or which documents it serves.
- `expireAfterSeconds` makes a TTL index: see [TTL indexes](#ttl-indexes).
- Index paths can't start with `$` or contain a numeric part such as `items.0` (67 `CannotCreateIndex`). Other index types, such as `"2d"`, fail with 39662 `SprigDocFeatureUnsupported`.
- A second index with the same name and a different key gives 86 `IndexKeySpecsConflict`; the same key under another name gives 85 `IndexOptionsConflict`.
- `hint` on `find`, `count`, `distinct` and `aggregate` becomes MariaDB's `FORCE INDEX` on the collection's reads.

### TTL indexes

A TTL index deletes documents a set time after the date in one field, as in MongoDB. Use it for sessions, events and other data that should age out:

```js
db.events.createIndex({ at: 1 }, { expireAfterSeconds: 86400 })   // delete a day after `at`
db.runCommand({ collMod: "events", index: { keyPattern: { at: 1 }, expireAfterSeconds: 3600 } })
```

```text
{ expireAfterSeconds_old: 86400, expireAfterSeconds_new: 3600, ok: 1 }
```

Which documents expire:

- A document expires once the indexed field holds a date earlier than now minus `expireAfterSeconds`. If the field is an array, the earliest date in it counts.
- Other values never expire: strings, numbers, timestamps, `null`, a missing field and nested arrays.
- `partialFilterExpression` doesn't narrow which documents expire, because partial semantics aren't built: every document whose field is old enough is deleted.
- Capped collections take TTL indexes too, and expire their documents the same way.

The rules for the index:

- One field, not `_id`. A compound key fails with 67 `CannotCreateIndex` ("TTL indexes are single-field indexes"), and `_id` with 197 `InvalidIndexSpecificationOption`.
- `expireAfterSeconds` is a number from 0 to 2,147,483,647, truncated to whole seconds. In `createIndexes`, a negative, `NaN`, out-of-range or non-numeric value fails with 67; in `collMod`, with 72 `InvalidOptions` or 14 `TypeMismatch`.
- `collMod` with `index: {name | keyPattern, expireAfterSeconds}` changes the seconds of an existing index, or turns a single-field index into a TTL index. The reply carries `expireAfterSeconds_old` (when there was one) and `expireAfterSeconds_new`. An unknown index fails with 27 `IndexNotFound`; `hidden`, `unique` and `prepareUnique` in `collMod`'s `index` fail with 39662.

How expiry runs:

- One background job per data centre deletes the expired documents of the tenants homed there, every `doc.ttl_interval_seconds` (60). A document can therefore outlive its expiry by up to the interval, plus the time the pass takes, as in MongoDB.
- Each pass works through every collection with a TTL index and every shard of it, round robin, in batches of `doc.multi_write_batch` (100) documents per transaction.
- Each tenant's deletes are paced to at most `doc.ttl_tenant_deletes_per_second` (5,000), so one tenant's backlog doesn't swamp a cluster. A pass that runs long stops after a minute (or the interval, if longer) and the next pass carries on.
- A document whose field was changed since the pass read it isn't deleted, so refreshing the date keeps it.
- Deletes are ordinary deletes: change streams show them as `delete` events. They run while a tenant is suspended, and they follow shard moves safely: a shard is expired only where it is currently placed.

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `doc.ttl_interval_seconds` | int, 1 to 86400 | 60 | dc, global | How often a pass runs (MongoDB's `ttlMonitorSleepSecs`) |
| `doc.ttl_tenant_deletes_per_second` | int, 0 to 10000000 | 5000 | tenant, dc, global | Most TTL deletes per second for one tenant; 0 for no limit |
| `doc.multi_write_batch` | int, 1 to 100000 | 100 | tenant, global | Documents per delete transaction |

`sprig_doc_ttl_deleted_total` (by `cluster`) counts the documents deleted, and `sprig_doc_ttl_passes_total` the passes.

### Arrays and multikey queries

The field columns index scalar values. They don't index the elements of arrays, so there are no multikey indexes: a query on a field that has held an array anywhere in the collection reads candidates with `JSON_CONTAINS` and checks them in the gateway. Range conditions on such a field aren't pushed into SQL at all.

SprigDB keeps a list of the paths that have held arrays. The first write that puts an array at a new path waits until every gateway in the data centre knows about it, so no gateway plans a query on that path as a plain scalar. The wait is at most `doc.path_publish_timeout_ms` (default 10,000 ms); past it the write fails with 39664 `SprigPathPublishTimeout` and nothing of it commits. Retry it. After 1,000 distinct array paths in one collection, SprigDB treats every path as one that may hold arrays.

### Automatic indexes

[Automatic indexing](../concepts/automatic-indexing.md) also builds indexes for collections from the queries it sees, named `sprig_auto_<hex>` in MariaDB. `listIndexes` hides them unless you pass `showAutoIndexes: true`, and `dropIndexes: "*"` leaves them alone. A user index on the same key is created beside an automatic one without a conflict.

## Validation

Collections take MongoDB's validators: query operators and `$jsonSchema` (MongoDB's full dialect, except the encryption keywords, which give 39662).

```js
db.createCollection("orders_log", {
  validator: { $jsonSchema: {
    required: ["x"],
    properties: { x: { bsonType: "int", minimum: 0 } }
  } }
})
db.runCommand({ collMod: "orders_log", validationLevel: "moderate", validationAction: "warn" })
```

| Option | Values |
|---|---|
| `validationLevel` | `strict` (default), `moderate` (only updates of documents that were valid), `off`, and `constraint` |
| `validationAction` | `error` (default), `warn` (logs and writes), `errorAndLog` |

The gateway checks each document before an insert and after an update is applied, so inserts, updates, replacements, upserts, `findAndModify`, `bulkWrite`, `$merge` and `$out` are all covered. A refused document fails with 121 `DocumentValidationFailure` and MongoDB's `errInfo`: `failingDocumentId`, and `details` clause by clause. In an unordered insert, the valid documents are kept.

`bypassDocumentValidation: true` on a command skips the rules. The level `constraint` needs `collMod` with `prepareConstraintValidationLevel: true` first; raising the level scans for an invalid document and refuses with its 121. At `constraint`, the validator can't change and bypassing is ignored.

A malformed `$jsonSchema` fails at `create` with 9 `FailedToParse`, and `$where` in a validator with 2 `BadValue`. `$jsonSchema` also works as a query operator in `find` and `$match`.

## Transactions

Multi-document transactions work through the drivers' session API:

```js
const s = db.getMongo().startSession()
s.startTransaction()
s.getDatabase("shop").events.insertOne({ _id: "t2" })
s.getDatabase("shop").events.updateOne({ _id: 20 }, { $set: { t: 1 } })
s.commitTransaction()
```

A transaction is one MariaDB transaction on the gateway's session for it. Its outcome is written with its last writes, so a driver that retries `commitTransaction` on another gateway, after a network failure or a gateway restart, gets the true outcome: `ok` if it committed, 251 `NoSuchTransaction` with `TransientTransactionError` if it didn't.

| Setting | Default | Meaning |
|---|---|---|
| `mongo.txn_lifetime_seconds` | `60` | How long a transaction may stay open |
| `mongo.max_open_transactions` | `10000` | Open transactions per gateway; past it, 509 `TooManyOpenTransactions` |
| `mongo.session_timeout_minutes` | `30` | `logicalSessionTimeoutMinutes` |

Inside a transaction, `count`, `listCollections`, `listIndexes`, `drop`, `dropDatabase`, `createIndexes`, `dropIndexes`, `renameCollection`, `listDatabases`, `convertToCapped` and `cloneCollectionAsCapped` fail with 263 `OperationNotSupportedInTransaction`, as do writes to capped collections. `create` is allowed.

`readConcern: {level: "snapshot"}` works inside transactions, and outside them for `find`, `aggregate` (without `$out` or `$merge`), `distinct` and `count`. Other commands with it fail with 72 `InvalidOptions`. A tenant spread over several clusters gets a consistent snapshot per cluster, not one across them.

Retryable writes are on, as in MongoDB: the gateway records each retryable write's result for `mongo.session_record_retention_minutes` (default 35), so a retry returns the first result instead of writing twice.

## Change streams

`watch()` on a collection, a database or the whole deployment (`allChangesForCluster`) opens a change stream over SprigDB's [change feed](../concepts/change-feed.md):

```js
const cs = db.events.watch([ { $match: { operationType: "insert" } } ])
```

- Events: `insert`, `update` (with `updateDescription`), `replace`, `delete`, `drop`, `rename` and `invalidate`. With `showExpandedEvents: true`, also `create`, `createIndexes`, `dropIndexes` and `modify`.
- Documents in events carry the BSON types the client wrote.
- `fullDocument: "updateLookup"`, `"whenAvailable"` or `"required"` give updates the document as that update left it, taken from the change itself; MongoDB gives the current document instead.
- `fullDocumentBeforeChange` needs pre- and post-images on the collection: `db.runCommand({collMod: "events", changeStreamPreAndPostImages: {enabled: true}})`.
- `resumeAfter` and `startAfter` work, also across a move of the tenant to another cluster. `startAtOperationTime` fails with 39662.
- Stages after `$changeStream` may be `$match`, `$project`, `$addFields`, `$set`, `$unset`, `$replaceRoot` and `$replaceWith`.

Row-level security applies to streams: a user sees only events for documents a policy lets them read.

A stream needs `SELECT` and `WATCH` (MongoDB's `find` and `changeStream`) on what it watches. On a single collection without `WATCH`, `watch()` fails with 13 `Unauthorized`; a database or deployment stream leaves such collections out. See [Grants and roles](../security/grants-and-roles.md#change-streams-need-watch).

Documents deleted by a [TTL index](#ttl-indexes) appear as `delete` events.

## Capped collections

```js
db.createCollection("log", { capped: true, size: 1048576, max: 1000 })
```

A capped collection keeps insertion order and deletes its oldest documents when it passes `size` bytes or `max` documents, in the same transaction as the insert. Reads without a sort, or with `{$natural: 1}` or `{$natural: -1}`, follow insertion order, and tailable cursors work. A tailable cursor whose position was deleted fails with 136 `CappedPositionLost`. `collMod` changes `cappedSize` and `cappedMax`, and `convertToCapped` and `cloneCollectionAsCapped` work.

## Use documents from SQL

From a MySQL or MariaDB client, a collection is a table with the columns in [How documents are stored](#how-documents-are-stored). Read it with MariaDB's JSON functions:

```sql
USE shop;

SELECT JSON_VALUE(doc, '$.type') AS type, COUNT(*) AS n
FROM events
GROUP BY 1;

SELECT JSON_VALUE(doc, '$._id')  AS id,
       JSON_VALUE(doc, '$.at')   AS at,
       JSON_VALUE(_types, '$.at') AS at_type     -- 'date'
FROM events
WHERE JSON_VALUE(doc, '$.customer_id') = 42;
```

SQL reads run with MariaDB's semantics, not MongoDB's: `JSON_VALUE` on an array returns `NULL`, and types come from JSON, so read `_types` when the BSON type matters. Row-level security policies on a collection apply to SQL reads as well. To join collections with tables, see [Cross-model queries](cross-model-queries.md).

Write documents through the MongoDB protocol or the REST call below, not with SQL `INSERT`, `UPDATE` or `DELETE`. SQL writes reach the table as they are: they don't encode `_id`, keep `_types` or the array paths up to date, or run validators. A procedure, function, trigger or event whose body writes a collection is refused when you create it, with 39602 `ER_SPRIG_NEEDS_GATEWAY`.

## Use documents over REST

The SQL endpoint takes MongoDB commands with `"language": "mongo"`. The `sql` field holds one command document as Extended JSON:

```sh
curl -s https://db.example.com:8443/v1/databases/shop/sql:execute \
  -H 'Authorization: Bearer <api-key>' \
  -H 'Content-Type: application/json' \
  -d '{"language": "mongo",
       "sql": "{\"find\": \"events\", \"filter\": {\"customer_id\": {\"$gte\": 10}}, \"sort\": {\"_id\": 1}}"}'
```

The command runs as the caller, with the same privileges as on the MongoDB port. A cursor is drained for you: each document is one row in a single `document` column, in relaxed Extended JSON, or canonical Extended JSON (which keeps int32, int64 and double apart) with `"output": "canonical_ejson"`. Any other reply, such as an insert's, is one row. `sql:query` streams the same result as NDJSON, and `max_rows`, `max_bytes` and `timeout_ms` cap it as for SQL.

Over REST:

- `$where` is refused, and so are authentication and fail-point commands (39794 `ER_SPRIG_QUERY_LANGUAGE_INVALID`). Shell syntax such as `db.events.find()` gets 39794 too: send the command document.
- There are no transactions (39794).
- Errors keep SprigDB's and MySQL's numbers where they exist: a duplicate key is 1062 with HTTP 409. Others are 39795 `ER_SPRIG_LANGUAGE_STATEMENT_FAILED`, with MongoDB's code in the message.

See [REST and gRPC](../connect/rest-and-grpc.md) and the [REST API reference](../reference/api.md).

## Other commands

| Command | What it does |
|---|---|
| `explain` | For `find`, `aggregate`, `count`, `distinct`, `update` and `delete`: MongoDB's `queryPlanner` and `executionStats`, built from MariaDB's plan, plus the SQL statements and their MariaDB plans under `sprig`. Other commands give 39662 |
| `sprigCreatePolicy`, `sprigDropPolicy`, `sprigRowSecurity` | Row-level security policies on a collection, with MongoDB filters; see [Row-level security](../security/row-level-security.md) |
| `sprigCall` | Calls a logic function exposed to MongoDB: `db.runCommand({sprigCall: "quote", args: [7, 3]})`. Inside a transaction the call joins it; a failed validation answers 121. See [Logic](logic.md) |
| `setQuerySettings`, `removeQuerySettings` | MongoDB's query settings, such as `reject` and `indexHints` |
| `shardCollection`, `enableSharding` | Accepted and recorded as a hint only; SprigDB shards collections itself |

## Limits

| Limit | Value | Error |
|---|---|---|
| Document size | 16 MiB of BSON | 10334 `BSONObjectTooLarge` |
| Nesting depth | 32 levels | 39665 `SprigDocTooDeep` |
| Encoded `_id` | 1,024 bytes | 39660 `SprigDocIdTooLong` |
| Duplicate field names, invalid UTF-8 | refused | 2 `BadValue` |
| Collection name | 1 to 255 characters; no `$` or NUL; not `system.*` | 73 `InvalidNamespace` |
| Database name | 1 to 64 characters; none of `/ \ . " $`, space or NUL | 73 `InvalidNamespace` |
| Memory per blocking stage or sort | `doc.agg_stage_memory_bytes`, `doc.max_sort_bytes` (100 MiB each) | spills to disk, or 292 without `allowDiskUse` |
| Memory per command | `mongo.max_operation_memory_bytes` (1 GiB) | 146 `ExceededMemoryLimit` |
| Open cursors | `mongo.max_cursors_per_tenant` (10,000 per tenant per gateway) | 39630 `SprigCursorLimit` |
| Idle cursor lifetime | `mongo.cursor_timeout_ms` (10 minutes) | 43 `CursorNotFound` |

The nesting limit is lower than MongoDB's 100 because MariaDB's JSON functions have their own. `allowDiskUse` is true by default (`doc.allow_disk_use_by_default`); spill files go to `doc.spill_dir`, within `doc.spill_max_bytes` per gateway and `doc.spill_tenant_max_bytes` per tenant. See [Settings](../reference/settings.md) and [Error codes](../reference/errors.md).

## Not supported

| Feature | What happens |
|---|---|
| Views (`create` with `viewOn` or `pipeline`) | 39662 `SprigDocFeatureUnsupported` |
| Time-series collections, clustered collections, Queryable Encryption (`timeseries`, `clusteredIndex`, `encryptedFields`) | 39662 |
| `mapReduce`, `eval`, the `geoNear` command | 115 `CommandNotSupported` |
| `renameCollection` across databases | 39662 |
| Multikey and wildcard (`$**`) indexes | No multikey indexes; `{"$**": 1}` gives 67 |
| `sparse` and partial index semantics | Recorded only |
| A collection's default collation, locale tailoring | Per-command root collation only |
| `$lookup` with `let`, `$lookup` into SQL tables | Use `pipeline`, or SQL for tables |
| The `$slice` aggregation expression | `$slice` works in projections and `$push` only |
| `$merge` on fields other than `_id`, or with a pipeline in `whenMatched` | 39662 |
| `startAtOperationTime` in change streams | 39662 |
| Typed collections and type inheritance | Inheritance works on SQL tables; see [Inheritance](inheritance.md) |

## Related

- [MongoDB](../connect/mongodb.md)
- [Search](search.md)
- [Vectors](vectors.md)
- [Cross-model queries](cross-model-queries.md)
- [Change feed](../concepts/change-feed.md)
