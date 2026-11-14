---
title: Logic
audience: [user, dba]
sidebar:
  order: 12
---

# Logic

Logic is code that SprigDB runs next to your data: functions and procedures that clients call, validations that check rows before they are written, computed columns, triggers that run after a commit or for each stream record or queue message, and schedules. You write it in KIRun, JavaScript or Lua, declare it with SQL, and call it from SQL, REST, gRPC, MongoDB or Redis. Plain MariaDB stored routines and triggers keep working too.

This page explains the model: what each kind of logic does, where it runs, what your code can reach, and how it fails. The statement grammar is in [Logic in SQL](../sql/logic.md).

## Kinds of logic

| Kind | Runs when | Languages | Where it runs |
|---|---|---|---|
| Function or procedure | A client calls it | KIRun, JavaScript, Lua | Compiled into MariaDB when it can be, otherwise in the gateway's engine |
| Validation | Before each `INSERT` or `UPDATE` of a table | KIRun, JavaScript | Inside MariaDB, as a `CHECK` constraint or `BEFORE` triggers |
| Computed column | Whenever MariaDB computes the column | KIRun, JavaScript | Inside MariaDB, as a generated column |
| Trigger `AFTER COMMIT` | After a table's rows change and commit | KIRun, JavaScript | Compiled into MariaDB, run by a worker from the change feed |
| Trigger `ON STREAM` | For each record of a stream | KIRun, JavaScript, Lua | In a gateway's engine, driven by a worker |
| Trigger `ON QUEUE` | For each message of a queue | KIRun, JavaScript, Lua | In a gateway's engine, driven by a worker |
| Schedule | On a cron schedule | Calls a procedure | Compiled procedure, run by a worker |
| Stored routine, trigger or event in SQL | As in MariaDB | SQL | Inside MariaDB, as ordinary DDL |

Functions, procedures, validations, triggers and schedules are **modules** in the database's logic catalog. Each `CREATE OR REPLACE` adds a **version**; versions are numbered from 1 and are never reused.

## Languages

### KIRun

KIRun is an open-source function format: a JSON definition with parameters, events and a graph of steps, each step calling a function such as `System.If`, `System.Math.Add` or `Sprig.Db.QueryOne`. Its expressions read `Arguments.<name>` and the outputs of earlier steps (`Steps.item.output.row.price`). Studio's logic editor shows a KIRun definition as a visual step graph and as text, and checks it against the functions SprigDB provides. `sprigctl logic compile` compiles a definition offline and prints where it would run.

A KIRun definition takes its parameters from the JSON, so a `CREATE FUNCTION` for KIRun has no parameter list. The result is the first event the steps raise: `output` with its fields, or another event such as `error` or `reject`. A definition that raises no event returns `output` with no fields.

### JavaScript

JavaScript runs on QuickJS, embedded in the gateway. A body is either a module whose default export is the function (`import { db, sprig } from 'sprig:db'` is the only import), or plain statements with the parameters in scope by name:

```js
// Statements: the declared parameters are in scope.
return price * qty * 0.9;
```

```js
// A module: the default export is the function.
import { db, sprig } from 'sprig:db';
export default function (productId, qty) {
  const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [productId]);
  if (p === null || p.stock < qty) return sprig.event('error', { message: 'not enough stock' });
  return { total: p.price * qty + 40 };
}
```

The parameter list and `RETURNS` of the declaration are the function's signature. SQL types map to integers (the integer types and `YEAR`), doubles (`DECIMAL`, `FLOAT`, `DOUBLE`), booleans (`BOOLEAN`, `TINYINT(1)`), JSON, and text (everything else). A body-form function whose parameter name is a JavaScript reserved word fails at deploy with 39861; write it as a module instead.

The code has no file system, network, timers, workers or `std` and `os` modules. `fetch` is the only way out, through the outbound allowlist.

### Lua

Lua is Lua 5.1 in Redis's dialect, in a sandbox: the base library, `string`, `table`, `math`, `coroutine` and `cjson` are loaded; `io`, `os`, `debug`, `package`, `require`, `module`, `dofile`, `loadfile`, `load` and `string.dump` are removed, and `loadstring` refuses binary chunks. `print` writes to the run's log.

A Lua body is a chunk run with the arguments: the declared parameters are locals by name, `...` holds them all, and the first return value is the result. Values cross as JSON: `nil` and `sprig.null` are `null`, tables with keys 1 to n are arrays, other tables are objects, and whole numbers come back as integers.

Lua code never compiles into MariaDB; it always runs in the engine.

### SQL

`CREATE FUNCTION` or `CREATE PROCEDURE` with `LANGUAGE sql`, or with no `LANGUAGE`, is an ordinary MariaDB stored routine. So are `CREATE TRIGGER … FOR EACH ROW` and `CREATE EVENT`. They follow MariaDB's rules, with your account as their definer. A stored body that writes a MongoDB collection is refused with 39602; see [Documents](documents.md).

## Two tiers: MariaDB or the engine

Each version of a function or procedure runs in one of two tiers:

- **The MariaDB tier.** SprigDB compiles the code into MariaDB routines (PL/SQL under `sql_mode=ORACLE`), deploys them into every shard schema of the database, and the code runs inside MariaDB, in the caller's transaction. This is the fastest path: a call is one round trip.
- **The engine tier.** The code runs in the gateway: the native KIRun runtime, QuickJS or Lua. Each `db` call it makes runs through the caller's own session, with the caller's names, privileges, row-level security and transaction, exactly as if the client had sent the statement.

SprigDB places each version (`SPRIG_TIER = 'auto'`, the default). The version compiles when every part of it is in the compilable subset:

| Language | Compiles into MariaDB |
|---|---|
| KIRun | `System.If`, `System.GenerateEvent`, `System.Context.Create`, `Get` and `Set`, `System.Loop.RangeLoop`, `CountLoop`, `ForEachLoop` and `Break`, `System.Math.Add`, `Maximum`, `Minimum`, `Absolute`, `Floor`, `Ceiling`, `Round`, `SquareRoot` and `Power`, `System.String.Concatenate`, `Length`, `UpperCase`, `LowerCase`, `Contains`, `StartsWith`, `EndsWith` and `IndexOf`, and `Sprig.Db.QueryOne`, `Query` and `Exec` |
| JavaScript | `const`, `let`, `var`; `if`, `while`, `for`, `for … of` over rows, `break`, `continue`, `switch` without fall-through, `return`, `throw`; literals, templates, arithmetic, comparisons, `&&`, `\|\|`, `??`, `?:`; `db.sql`, `db.sqlOne` and `db.exec` with literal SQL and an array literal of arguments; `Math.abs`, `floor`, `ceil`, `round`, `sqrt`, `pow`, `max`, `min`; `length`, `toUpperCase`, `toLowerCase`, `includes`, `startsWith`, `endsWith`, `indexOf`, `String(x)`; `return {…}`, `sprig.event`, `sprig.reject` |
| Lua | Nothing |

Anything else sends the version to the engine tier, among them: outbound `fetch`, `db.call`, `try`/`catch`, helper functions, `db.publish` and `db.mq.*`, KIRun functions outside the list, and code that reads or writes tables in a database that has more than one shard. Compiled JavaScript keeps JavaScript's meaning where MariaDB's differs: a possibly-`null` number reads as 0, `''` is false, and reading a column of a `sqlOne` row that may be `null` fails with 39865, as the `TypeError` would.

After a declaration, `SHOW WARNINGS` lists one note per line of the placement: the module, the version, the tier, and each reason the code went to the engine tier, with where in the source it is. `POST /v1/databases/{database}/logic:compile` does the same placement as a dry run, without recording or deploying anything.

Choose the tier yourself with `WITH (SPRIG_TIER = …)`:

| Value | Meaning |
|---|---|
| `'auto'` | SprigDB decides (the default) |
| `'mariadb'` | Compile or refuse: code that can't compile fails with 39868 `ER_SPRIG_LOGIC_TIER_UNAVAILABLE`, listing the reasons |
| `'engine'` | Always the engine tier |

The engine tier has three limits that the MariaDB tier doesn't:

- It can't run `SQL SECURITY DEFINER` code: such a version is refused at deploy with 39862. Deploy it `SQL SECURITY INVOKER`, or make it compile.
- `SELECT f(…)` works only in a `SELECT` without `FROM`, with constant arguments. Other calls fail with 39876 `ER_SPRIG_LOGIC_ENGINE_UNAVAILABLE`; use `CALL`.
- A prepared `CALL` (the binary protocol) fails with 39876. Send `CALL` as text.

## Functions and procedures

### Declare them

To create a function, declare it with its language and body. The examples on this page build on each other in one database:

```sql test
CREATE TABLE products (id BIGINT PRIMARY KEY, name VARCHAR(40), price DECIMAL(10,2) NOT NULL, stock INT NOT NULL);
CREATE TABLE orders (id BIGINT AUTO_INCREMENT PRIMARY KEY, product_id BIGINT NOT NULL, qty INT NOT NULL, total DOUBLE);
CREATE TABLE order_audit (id BIGINT AUTO_INCREMENT PRIMARY KEY, order_id BIGINT, qty INT);
INSERT INTO products VALUES (7, 'pen', 400.00, 10), (8, 'ink', 19.99, 3);
```

```sql test
CREATE FUNCTION net(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE javascript
  AS $$ return price * qty * 0.9 $$;

DELIMITER //
CREATE FUNCTION order_quote(productId BIGINT, qty INT) LANGUAGE javascript
  EXPOSE (sql, http, grpc, mongo, redis) AS $$
  const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [productId]);
  if (p === null || p.stock < qty) return sprig.event('error', { message: 'not enough stock' });
  return { total: p.price * qty + 40 };
$$ //
DELIMITER ;

CREATE FUNCTION stock_left(productId BIGINT) RETURNS INT LANGUAGE lua AS $$
local p = db.sqlOne('SELECT stock FROM products WHERE id = ?', productId)
if p == sprig.null then return 0 end
return p.stock
$$;
```

Bodies that contain semicolons need `DELIMITER` in the `mysql` client; clients that send one statement at a time don't.

A function and a procedure differ in how you call them: a function can be called with `SELECT f(…)` as well as `CALL`. Both take `IN` parameters only; `OUT` and `INOUT` fail with 39861, so return values instead. Creating either needs `CREATE ROUTINE` on the database (1044 otherwise). An existing name without `OR REPLACE` fails with 1304.

### Results and events

Every call ends in one **event**: `output` for a normal return, or a named event such as `error`. What the code returns becomes the event:

| The code returns | Event |
|---|---|
| `sprig.event(name, fields)` | That event with those fields |
| A value, in a function declared `RETURNS type` | `output` with one field, `value` |
| An object (a Lua table with keys) | `output` with its members as fields |
| Nothing | `output` with no fields |

In JavaScript, `NaN`, an infinity or a `BigInt` in the result fails with 39866.

### Call from SQL

`SELECT f(…)` returns the `output` event's value: its only field, or its fields as JSON. Any other event fails with 39865 `ER_SPRIG_LOGIC_USER_ERROR` and the text `<event>: <message>`.

```sql test
SELECT net(10, 3) AS net, order_quote(7, 3) AS total, stock_left(7) AS stock_left;
```

```text output
net	total	stock_left
27	1240	10
```

```sql test-error
-- 39865 ER_SPRIG_LOGIC_USER_ERROR: error: not enough stock
SELECT order_quote(7, 1000);
```

`CALL` returns one row: the `event` column, then one column for each field of each event the version can raise. Calling needs `EXECUTE` on the routine or its database.

```sql test
CALL order_quote(7, 3);
```

A compiled function that writes nothing also gets a MariaDB stored function, so `SELECT f(…)` works anywhere a function call does, including in queries with `FROM`. A function that writes data has no stored function: `CALL` it.

### Call over REST and gRPC

A function exposed to `http` is a REST endpoint. Pass arguments as a list by position, an object by parameter name, or one value:

```sh
curl -u 'acme.app:<password>' \
  https://db.example.com:8443/v1/databases/shop/functions/order_quote:call \
  -H 'content-type: application/json' \
  -d '{"args": {"productId": 7, "qty": 1000}}'
```

```json
{"event": "error", "result": {"message": "not enough stock"}}
```

The answer is the event and its fields. Add `"transaction": "<handle>"` to run the call inside a REST transaction. gRPC clients call `sprig.v1.Functions/Call` with the same fields. A call is one statement through the same engine as `Sql.Execute`, so privileges, budgets and errors are a SQL `CALL`'s. An argument name that matches no parameter fails with 39874. See [REST and gRPC](../connect/rest-and-grpc.md).

### Call from MongoDB and Redis

MongoDB drivers call a function exposed to `mongo` with the `sprigCall` command, in the command's database:

```js
db.runCommand({ sprigCall: "order_quote", args: [7, 3] })
// { event: "output", result: { total: 1240 }, ok: 1 }
```

`args` is a list, a document by parameter name, or one value. Inside `startTransaction` the call joins the transaction. A validation failure (39863) reaches MongoDB clients as 121 `DocumentValidationFailure`; other errors keep their SprigDB codes.

Redis clients call a function exposed to `redis` with `FCALL`, when no Redis Lua library function has that name (Redis's own functions are found first):

```text
FCALL shop.order_quote 0 7 3
```

A plain name means a function in the tenant's `redis.logical_database`; `db.name` names another database. `KEYS` bind the leading parameters, then `ARGV`, all as text. The reply is the `output` event's value: its one field, or its fields as a map (a flat array in RESP2). Another event is `-SPRIG LOGIC_USER_ERROR 39865 <event>: <message>`. `FCALL_RO` runs the call in a read-only transaction, so a write fails whatever the tier; it is refused inside `MULTI`. See [Key-value](key-value.md).

### Exposure, rights and transactions

- **Exposure.** `EXPOSE (…)` lists the protocols that may call the function: `sql`, `http`, `grpc`, `mongo`, `redis`. The default is `(sql)`, and SQL callers are never restricted by it. A call over a protocol not in the list fails with 39789 `ER_SPRIG_FUNCTION_NOT_EXPOSED`.
- **Rights.** Every protocol needs `EXECUTE`. With `SQL SECURITY INVOKER` (the default) the code's statements run with the caller's privileges, so a caller without `SELECT` on a table gets 1142 from inside the function. `SQL SECURITY DEFINER` runs a compiled version with its creator's privileges.
- **Transactions.** A call in autocommit runs in its own transaction, rolled back if the call fails. A call inside a transaction joins it, and its writes commit or roll back with the transaction.
- **Nesting.** `db.call` runs `CALL` through the same session, so `EXECUTE` is checked again and an engine-tier callee runs nested. Nesting deeper than 8 calls fails with 39872 `ER_SPRIG_LOGIC_CALL_DEPTH`.

## What code can use

All three languages reach the same host interface. Every `db` call is one statement through the caller's session (the definer's, for triggers), inside the caller's transaction.

| JavaScript and Lua | KIRun | Does |
|---|---|---|
| `db.sql(text, args)` | `Sprig.Db.Query` | Runs a query and returns its rows; in JavaScript, an `INSERT`, `REPLACE`, `UPDATE` or `DELETE` returns `{affected}` |
| `db.sqlOne(text, args)` | `Sprig.Db.QueryOne` | Returns one row, or `null` (KIRun: `row` and `found`); more than one row fails with 39866 |
| `db.exec(text, args)` | `Sprig.Db.Exec` | Runs an `INSERT`, `REPLACE`, `UPDATE` or `DELETE`; returns `{affected}` |
| `db.call(name, ...args)` | | Calls another logic function; returns `{event, fields}` |
| `db.publish(stream, key, value, headers)` | `Sprig.Db.Publish` | Publishes a record to a [stream](streams.md), in the caller's transaction |
| `db.commitOffset(group, stream, partition, offset)` | `Sprig.Db.CommitOffset` | Commits a consumer group's offset, in the caller's transaction |
| `db.mq.publish(exchange, routingKey, body, props)` | `Sprig.Mq.Publish` | Publishes to a [queue](queues.md) exchange, in the caller's transaction; returns the number of queues reached |
| `db.mq.get(queue, {max, leaseMs})` | `Sprig.Mq.Get` | Takes messages: `deliveryToken`, `body`, `properties`, `routingKey`, `exchange`, `redelivered`, `deliveryCount` |
| `db.mq.ack(token)`, `db.mq.nack(token, requeue)` | `Sprig.Mq.Ack`, `Sprig.Mq.Nack` | Settles a delivery, in the caller's transaction |
| `fetch(url, init)` | `Sprig.Http.Fetch` | An outbound HTTP call to an allowed host. `init` takes `method`, `headers` and `body`. JavaScript gets `{status, ok, headers, body, text(), json()}`; KIRun gets `status`, `headers` and `body` |
| `sprig.event(name, fields)` | `System.GenerateEvent` | Ends the call with that event |
| `sprig.reject(message)` | | Fails the call with 39863 and the message; can't be caught |
| `ctx` | | `user`, `tenant` (the tenant's id), `database`, `function`, `version`, and `log` |
| `console.log`, Lua `log` and `print` | | Writes a line to the run's log |

Rules for `db` statements:

- The text is one statement. Queries start with `SELECT`, `WITH` or `VALUES`; writes are `INSERT`, `REPLACE`, `UPDATE` or `DELETE`. Anything else, such as DDL, `SET` or `COMMIT`, fails with 39866.
- Each `?` takes the next argument, as a literal; a count that doesn't match fails with 39874.
- Unqualified table names mean the code's own database, whatever the caller's current database. Qualify a name to reach another database.
- `JSON` columns come back parsed, `DECIMAL` as a number, dates as text.
- In `db.publish`, a stream name with a dot is `<database>.<stream>`. Values that aren't text are sent as JSON; a `null` value is a tombstone. The queue calls use virtual host `/`.

An error from a `db` or `fetch` call that the code doesn't catch fails the call with the original error, so a missing privilege is still 1142. Caught in JavaScript, it is an `Error` with `code` and `sqlState`; in Lua, an error value whose text is the message. An exception the code throws and doesn't catch fails the call with 39865 and its text. KIRun's `catchErrors` steps can catch `db` and outbound errors, but never a budget error.

Lines the code logs go to the gateway's own log at debug level, and into the report of a test run (up to 100 lines per run). Compiled JavaScript drops them.

## Validations

A validation checks each row before MariaDB writes it. It is enforced inside MariaDB, so it applies to every write to the table: SQL on either port, REST, logic code, and functions called from MongoDB or Redis.

To refuse orders with a quantity of zero or less:

```sql test
CREATE VALIDATION qty_positive ON orders FOR INSERT, UPDATE LANGUAGE kirun AS '{"namespace": "Shop", "name": "QtyPositive",
 "parameters": {"qty": {"parameterName": "qty", "schema": {"type": ["INTEGER"]}}},
 "events": {"output": {"name": "output", "parameters": {}},
            "reject": {"name": "reject", "parameters": {"message": {"type": ["STRING"]}}}},
 "steps": {
  "bad": {"statementName": "bad", "namespace": "System", "name": "If",
    "parameterMap": {"condition": {"c": {"type": "EXPRESSION", "expression": "Arguments.qty <= 0"}}}},
  "no": {"statementName": "no", "namespace": "System", "name": "GenerateEvent",
    "executeIftrue": {"Steps.bad.true": true},
    "parameterMap": {"eventName": {"e": {"type": "VALUE", "value": "reject"}},
      "results": {"r": {"type": "VALUE", "value": {"name": "message", "value": {"isExpression": false, "value": "qty must be positive"}}}}}}}}';
INSERT INTO orders (product_id, qty) VALUES (7, 2);
```

```sql test-error
-- 39863 ER_SPRIG_LOGIC_VALIDATION_FAILED: qty must be positive
INSERT INTO orders (product_id, qty) VALUES (7, 0);
```

How a validation works:

- **Reading the row.** KIRun parameters bind to the row by name: a parameter named after a column gets the value after the write; `old_<column>` gets the value before it (`NULL` for an insert); `op` gets `insert` or `update`. Any other name fails at deploy with 39874. JavaScript code takes `(row, old)`, or `(ctx)` with `ctx.trigger.after`, `ctx.trigger.before` and `ctx.trigger.op`, or the same names as KIRun's parameters, or is a body alone with `row`, `old`, `op` and `ctx` in scope. `old` is `null` for an insert, so test it before reading its columns.
- **Rejecting.** The code rejects the row by raising a `reject` (or `error`) event with a `message`. The statement fails with 39863 `ER_SPRIG_LOGIC_VALIDATION_FAILED` and that message.
- **A `CHECK` constraint when it can be.** A validation `FOR INSERT, UPDATE` that reads only the new row and rejects with one constant message becomes a `CHECK` constraint named `sprig_lc<binding>_v<version>`, visible in `SHOW CREATE TABLE`. Rows already in the table that break it refuse the deploy. Otherwise SprigDB installs `BEFORE INSERT` and `BEFORE UPDATE` triggers that call the compiled code; the deploy notes say which and why.
- **Compiled only.** Validations are KIRun or JavaScript and must compile into MariaDB; one that can't, and every Lua validation, is refused with 39862. A new version replaces the old one at once.
- **Rights.** Validations run with their definer's rights (`SQL SECURITY INVOKER` fails with 39861). Creating one needs `TRIGGER` on the table.

The same check in JavaScript, with a second rule that reads the row before the write:

```sql test
DELIMITER //
CREATE OR REPLACE VALIDATION order_rules ON orders FOR INSERT, UPDATE LANGUAGE javascript AS $$
export default function (row, old) {
  if (row.qty <= 0) sprig.reject(`qty ${row.qty} must be positive`);
  if (old !== null && row.qty < old.qty)
    sprig.reject(`qty can only go up (was ${old.qty})`);
}
$$ //
DELIMITER ;
```

Test a validation without writing anything with `TEST VALIDATION order_rules WITH '{"row": {…}, "old": {…}, "op": "update"}'`; see [Logic in SQL](../sql/logic.md#test).

## Computed columns

A computed column is a MariaDB generated column whose expression comes from a logic function. Declare the function, then add the column with a call to it:

```sql test
CREATE FUNCTION stock_value_of LANGUAGE kirun AS '{"namespace": "Shop", "name": "StockValue",
 "parameters": {"price": {"parameterName": "price", "schema": {"type": ["DOUBLE"]}},
                "stock": {"parameterName": "stock", "schema": {"type": ["INTEGER"]}}},
 "events": {"output": {"name": "output", "parameters": {"value": {"type": ["DOUBLE"]}}}},
 "steps": {"out": {"statementName": "out", "namespace": "System", "name": "GenerateEvent",
   "parameterMap": {"results": {"r": {"type": "VALUE", "value": {"name": "value",
     "value": {"isExpression": true, "value": "Arguments.price * Arguments.stock"}}}}}}}}';
ALTER TABLE products ADD COLUMN stock_value DOUBLE AS (stock_value_of(price, stock)) VIRTUAL;
SELECT id, stock_value FROM products WHERE id = 7;
```

```text output
id	stock_value
7	4000
```

SprigDB writes the function's body as one SQL expression, with the arguments in place of the parameters, and runs the `ALTER TABLE` as ordinary DDL. `STORED` and `VIRTUAL` work as in MariaDB. The function must be KIRun or JavaScript, and its body must have an expression form: no data access, no loops, no JSON values, no division by a value that isn't constant, and at most 64 paths. A KIRun function's `output` event must have exactly one field; a JavaScript function returns the value. Otherwise the statement fails with 39862, as does a Lua function, and a JavaScript function that calls `Math.random()`, `Date.now()`, `Date()`, `performance.now()` or `crypto.*` (reason `js.nondeterministic`). A wrong argument count fails with 39874.

```sql test
DELIMITER //
CREATE OR REPLACE FUNCTION line_net(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE javascript AS $$
export default function (price, qty) {
  if (qty >= 10) return price * qty * 0.9;
  return price * qty;
}
$$ //
DELIMITER ;
ALTER TABLE products ADD COLUMN stock_net DOUBLE AS (line_net(price, stock)) STORED;
``` Where KIRun would fail on a `NULL` operand, the column is `NULL`.

The column keeps its expression: a later version of the function with the same parameters deploys and doesn't change it. A version with different parameters fails with 39875 and names the columns. `DROP FUNCTION` of a function a column was made from fails with 39875 unless you add `CASCADE`, which keeps the column.

## Triggers after commit

A trigger `AFTER COMMIT` runs once for each committed change to a table's rows, outside the writing transaction. It reads the tenant's [change feed](../concepts/change-feed.md), so it never slows the write and never sees a change that rolled back.

```sql test
CREATE TRIGGER audit_orders AFTER COMMIT ON orders FOR INSERT LANGUAGE kirun AS '{"namespace": "Shop", "name": "AuditOrder",
 "parameters": {"id": {"parameterName": "id", "schema": {"type": ["LONG"]}},
                "qty": {"parameterName": "qty", "schema": {"type": ["INTEGER"]}}},
 "steps": {"ins": {"statementName": "ins", "namespace": "Sprig.Db", "name": "Exec",
   "parameterMap": {"sql": {"s": {"key": "s", "type": "VALUE", "value": "INSERT INTO order_audit (order_id, qty) VALUES (?, ?)"}},
                    "args": {"a": {"key": "a", "type": "EXPRESSION", "order": 1, "expression": "Arguments.id"},
                             "b": {"key": "b", "type": "EXPRESSION", "order": 2, "expression": "Arguments.qty"}}}}}}';
INSERT INTO orders (product_id, qty) VALUES (8, 1);
```

A few moments after the insert commits, `order_audit` has a row for it.

- **Events.** `FOR INSERT`, `UPDATE` and `DELETE`, in any combination. `UPDATE OF (col, …)` fires only when one of those columns changed.
- **Reading the row** works as for validations, in KIRun or JavaScript: the row after the change (for a `DELETE`, the deleted row), the row before it, and `op`, the operation.
- **Exactly once.** Each change runs in its own transaction on the table's shard, together with a marker for that change. A change delivered again, after a worker restart or a shard move, finds its marker and does nothing.
- **Order.** Changes to the same row run in commit order. Different rows run in parallel, in `logic.trigger_lanes` lanes (default 8) by row key.
- **Failures.** A failed run is retried with doubling waits and jitter, from `logic.trigger_retry_initial_ms` (1 s) up to `logic.trigger_retry_max_ms` (5 minutes). Failures that are transient (a lost connection, a lock wait, a server that is read-only or fenced during a failover) are retried for as long as they last and don't count. After `logic.trigger_max_attempts` (10) counted failures the change goes to the dead letters, and the lane moves on. An `error` or `reject` event counts as a failure (39865).
- **Time.** One run may take `logic.trigger_budget_ms` (30 s).
- **Compiled only.** After-commit triggers are KIRun or JavaScript and must compile into MariaDB (39862 otherwise, and for Lua). Code that reads or writes tables compiles only when the table's group has one shard.
- **Rights.** The trigger runs with its definer's rights. Creating one needs `TRIGGER` and `SELECT` on the table, because the code receives the changed rows. When [row-level security](../security/row-level-security.md) policies bind the definer, the trigger runs only for rows the definer may see: an update that shows or hides a row to the definer arrives as an insert or a delete.

A `CREATE TRIGGER` without `AFTER COMMIT`, `ON STREAM` or `ON QUEUE` is an ordinary MariaDB trigger that runs inside the writing statement.

## Triggers on streams and queues

A trigger `ON STREAM` runs for every record of a [stream](streams.md); a trigger `ON QUEUE` runs for every message of a durable [queue](queues.md). Both are written in KIRun, JavaScript or Lua, always run in the engine tier as their definer, and commit their writes together with their position, so each record's or message's effects happen exactly once.

```sql test
CREATE STREAM order_events PARTITIONS 4;

DELIMITER //
CREATE TRIGGER audit_order_events ON STREAM order_events LANGUAGE javascript AS $$
export default function (ctx) {
  const e = JSON.parse(ctx.record.value);
  db.exec('INSERT INTO order_audit (order_id, qty) VALUES (?, ?)', [e.order_id, e.qty]);
}
$$ //
DELIMITER ;

INSERT INTO order_events (key, value) VALUES (501, JSON_OBJECT('order_id', 501, 'qty', 2));
```

A queue trigger reads a queue that AMQP clients declared. Name the queue's virtual host as its database: `ON QUEUE shop.billing` reads queue `billing` of virtual host `shop`.

```sql
CREATE TRIGGER bill ON QUEUE shop.billing LANGUAGE lua AS $$
local m = cjson.decode(ctx.message.body)
db.exec('UPDATE orders SET total = ? WHERE id = ?', m.total, m.order_id)
$$;
```

### What the code receives

JavaScript and Lua code is called with one argument, `ctx`, which is also a global:

| Field | Contents |
|---|---|
| `ctx.record` (streams) | `stream`, `partition`, `offset`, `key`, `value`, `headers`, `timestamp`. Key, value and header values are text; bytes that aren't UTF-8 are base64 |
| `ctx.message` (queues) | `queue`, `vhost`, `exchange`, `routingKey`, `body`, `properties`, `redelivered`, `deliveryCount` |
| `ctx.trigger` | `binding` and `kind` (`stream` or `queue`) |
| `ctx.user`, `ctx.tenant`, `ctx.database` | The definer, the tenant and the trigger's database |

A KIRun definition's parameters bind by name to the fields of the record or message, or take the whole object as `record`, `message` or `ctx`. Any other parameter name fails at deploy with 39874.

### How they run

- **Streams.** A new trigger starts at each partition's end: it runs for records published after it was created. It has its own consumer group, `__sprig_logic_<binding>`. Each record runs in one transaction that commits the group's offset and the code's writes together; a second run of the same record finds the offset moved (39877) and does nothing. Partitions run in parallel, each in offset order. `CREATE OR REPLACE` keeps the position; a replacement that reads another stream starts a new one. Records that retention removed before they ran are skipped.
- **Queues.** The trigger is one more competing consumer. It takes the next message (held for `logic.trigger_budget_ms` plus 5 s), runs the code, and acknowledges the message in the same transaction as the code's writes. `logic.trigger_lanes` loops take messages at once; an empty queue is asked again after 100 ms, doubling to 1 s.
- **Failures.** A failure is retried as for after-commit triggers; transient failures never count. A stream record that fails `logic.trigger_max_attempts` times goes to the dead letters, and the offset moves past it. A queue message's attempts are counted by its delivery count; at the limit it goes to the dead letters and is then rejected, so a queue with a dead-letter exchange gets it there as well. A queue's own `x-delivery-limit` still applies.
- **Changes.** Each running trigger picks up a new active version within about 5 seconds, and stops when the trigger, its stream or queue, or its database is dropped.

Creating a trigger on a stream needs `CREATE ROUTINE` on the trigger's database and `SELECT` on the stream; on a queue, `SELECT` and `DELETE` on the queue. The stream must exist (39940) and the queue must be a durable queue of that virtual host (39860). `SQL SECURITY INVOKER` fails with 39861.

### Triggers across tenants

- `CREATE TRIGGER g ON STREAM s FOR EACH TENANT GRANTS (…)` declares a trigger on an inherited stream that runs separately for each member tenant, with the privileges it lists.
- `CREATE TRIGGER g ON STREAM s FAN IN` reads every member tenant's instance of an inherited stream below your tenant, and needs `SPRIG_FAN_IN`. `FAN IN` isn't allowed on queues.

See [Inheritance](inheritance.md).

## Schedules

A schedule calls a procedure on a cron schedule:

```sql test
DELIMITER //
CREATE PROCEDURE restock() LANGUAGE javascript AS $$
  db.exec('UPDATE products SET stock = 10 WHERE stock < ?', [2]);
  return {};
$$ //
DELIMITER ;
CREATE SCHEDULE nightly_restock CRON '0 3 * * *' TIME ZONE 'Europe/London' DO CALL restock();
```

- **Cron text.** Five fields, `minute hour day-of-month month day-of-week`; each is `*`, a number, a range `a-b`, a list `a,b` or a step `*/n` or `a-b/n`. Months and days may be named (`JAN`, `MON`), and day 7 is Sunday, like 0. When both day fields are restricted, a day matching either fires. `@hourly`, `@daily`, `@weekly`, `@monthly` and `@yearly` are short forms. A bad text or zone fails with 39873 `ER_SPRIG_LOGIC_SCHEDULE_INVALID`.
- **Time zone.** An IANA name; `UTC` by default. A time the clocks skip fires once, at the next valid minute; a time they repeat fires once, the first time.
- **One run per fire.** Each fire runs once on a worker, even across a controller failover or a worker restart, within `logic.job_budget_ms` (5 minutes). A fire more than 5 minutes late is recorded as misfired and skipped.
- **Overlap.** With `OVERLAP SKIP` (the default), a fire while the previous run is still going is skipped. With `OVERLAP QUEUE`, one fire waits and runs when the previous run ends.
- **The procedure** must be in the same database (39860 otherwise) and take constant arguments: text, numbers, `TRUE`, `FALSE` or `NULL`. It runs with the schedule creator's rights, and must compile into MariaDB: a procedure in the engine tier fails each run with 39876.

Creating a schedule needs `CREATE ROUTINE` on the database.

## Budgets and limits

Code in the engine tier runs under budgets. Each is a setting; a run that exceeds one fails with 39864 `ER_SPRIG_LOGIC_BUDGET_EXCEEDED`, naming the setting, and the code can't catch it.

| Setting | Default | Scopes | Limits |
|---|---|---|---|
| `logic.function_budget_ms` | `5000` | object, database, tenant, global | Wall time of one synchronous call |
| `logic.trigger_budget_ms` | `30000` | object, database, tenant, global | Wall time of one trigger run |
| `logic.job_budget_ms` | `300000` | object, database, tenant, global | Wall time of one scheduled run |
| `logic.kirun_max_steps` | `1000000` | object, database, tenant, global | Function calls in one KIRun run, loop bodies included |
| `logic.max_loop_iterations` | `100000` | object, database, tenant, global | Iterations of one loop |
| `logic.max_data_ops` | `1000` | object, database, tenant, global | `db` and outbound calls in one run |
| `logic.max_rows_read` | `100000` | object, database, tenant, global | Rows read in one run |
| `logic.memory_limit_mb` | `64` | tenant, global | Heap of one run |

A call in a gateway reads the value for the code's own database, so set these on the database, the tenant or globally. Workers read the tenant's and global values of the trigger settings (`logic.trigger_*`, `logic.job_budget_ms`). All of them apply live. See the [settings reference](../reference/settings.md#logic).

Some limits are fixed:

- **Compiled code** is deployed with the defaults: each call runs under MariaDB's `max_statement_time` of 5,000 ms, and loops without a static bound are stopped at 100,000 iterations (39864). A `SELECT f(…)` of a compiled function is bounded by the statement's own `max_statement_time` instead.
- **Engine threads.** JavaScript and Lua calls run on as many threads as the gateway has cores; up to 16 calls per thread may wait for one, and beyond that a call fails at once with 39871 `ER_SPRIG_LOGIC_ENGINE_BUSY`, which you can retry.
- **Memory.** QuickJS and Lua count their real heap. The KIRun runtime estimates the size of the values each step holds.
- **Cancellation.** When the client goes away or runs `KILL QUERY`, the run stops.

To give one database a tighter time budget:

```sql test
ALTER DATABASE SPRIG SET (logic.function_budget_ms = 500);
CREATE PROCEDURE spin() LANGUAGE lua AS $$ while true do end $$;
```

```sql test-error
-- 39864 ER_SPRIG_LOGIC_BUDGET_EXCEEDED, naming logic.function_budget_ms
CALL spin();
```

```sql test
ALTER DATABASE SPRIG RESET (logic.function_budget_ms);
DROP PROCEDURE spin;
```

A Lua `pcall` around the loop wouldn't help: once a budget trips, `pcall`, `xpcall` and `coroutine.resume` pass the error on.

## Outbound HTTP

`fetch` (KIRun: `Sprig.Http.Fetch`) reaches only hosts on `logic.outbound_allowlist`, which is empty by default:

| Setting | Default | Scopes | Meaning |
|---|---|---|---|
| `logic.outbound_allowlist` | `[]` | tenant, global | Hosts code may call: a JSON list or comma-separated |
| `logic.outbound_timeout_ms` | `2000` | tenant, global | Time for the whole call |
| `logic.outbound_max_bytes` | `1048576` | tenant, global | Largest response body |

An entry is `host`, `host:port`, `*.domain` (any subdomain, not the domain itself) or an IP address, with an optional port. Without a port it allows 80 and 443 only. For example:

```sql
ALTER TENANT SPRIG SET (logic.outbound_allowlist = '["api.example.com", "*.svc.example.org:8443"]');
```

The call must be `http` or `https`; `https` checks the server's certificate against the public web roots. A name that resolves to a private, loopback, link-local, shared (CGNAT), unspecified or multicast address is refused unless the allowlist names that address literally, and the call connects to the address it checked, so a second DNS answer can't redirect it. A refused call fails with 39867 `ER_SPRIG_LOGIC_OUTBOUND_DENIED`; a slow or large one with 39864. Each call counts against `logic.max_data_ops`. A function that calls `fetch` always runs in the engine tier.

```sql test
CREATE FUNCTION ping() RETURNS INT LANGUAGE javascript
  AS $$ return fetch('https://api.example.com/health').status $$;
```

```sql test-error
-- 39867 ER_SPRIG_LOGIC_OUTBOUND_DENIED: api.example.com isn't on the allowlist
CALL ping();
```

## Test runs

A test run runs a function, procedure, trigger or validation against inputs you give, in a sandbox that never writes, and reports everything it did:

```sql test
TEST FUNCTION order_quote (7, 3);
TEST FUNCTION order_quote WITH '{"args": {"productId": 7, "qty": 2}, "rows": {"products": [{"id": 7, "price": 10, "stock": 5}]}}';
TEST FUNCTION draft WITH '{"source": "CREATE FUNCTION draft(a INT) RETURNS INT LANGUAGE lua AS $$ return a * 2 $$", "args": [21]}';
```

The answer is one row: `event`, `result` and `report`. The report is JSON: the event and fields or the error (`code`, `name`, `message`); every statement in order with its arguments bound, its kind (`read`, `write`, `call` or `refused`), rows or affected count, time and error; the writes that ran; the log lines; the `fetch` calls; the snapshot tables as the run left them (up to 1,000 rows each); the budgets used (wall time, steps, data operations, rows read, peak memory); and the timing.

How a test run works:

- It runs inside a transaction, or a savepoint of yours, and rolls everything back.
- **Inputs.** `args` for functions and procedures (a list, an object by name, or one value); `row`, `old` and `op` for validations and after-commit triggers; `record` or `message` for triggers on streams and queues.
- **Snapshots.** `rows` gives each named table's rows: the code sees exactly those rows, in a temporary copy of the table (up to 10,000 rows per table).
- **Drafts.** `source` runs a whole `CREATE … AS …` declaration without recording or deploying it; the name in the statement is then only a label.
- **`fetch`** is never sent. It is checked against the allowlist and answered from the mocks in `fetch` (`method`, `url` or a prefix ending in `*`, `status`, `headers`, `body`), or fails.
- **Refused statements.** Anything that could end the transaction or reach outside it is refused with 39862 and shows as `refused`: `COMMIT`, DDL, `SET`, `LOCK`, plain stored procedures, and `db.mq.get`, `ack` and `nack`, which settle real messages. Reads, writes, `db.publish`, `db.mq.publish`, `db.commitOffset` and calls of logic functions run and are rolled back.
- **Tier and rights.** A test always runs on the engine tier, even for a version compiled into MariaDB, as you: triggers and validations run with your rights, not their definer's, and the report says so.
- **Privileges.** Testing a deployed version needs `EXECUTE`, `CREATE ROUTINE` or `ALTER ROUTINE` on the database; a draft needs `CREATE ROUTINE` or `ALTER ROUTINE` (1044 otherwise).

The same run is `POST /v1/databases/{database}/logic:testRun` with the options as a JSON body (plus `name`, `kind` and `version`), and gRPC `sprig.v1.Logic/TestRun`. Studio's logic editor uses it for its test panel.

## Versions, rollout and rollback

`CREATE OR REPLACE` of a function or procedure deploys version n+1 into every shard of the database, all or nothing, and makes it active. A deploy that fails anywhere leaves the previous version active; two deploys of one module at once fail with 39869, which you can retry. Validations, triggers and schedules switch to a new version at once too.

### Staged rollout

To try a new version on part of your callers first, deploy it with `SPRIG_ROLLOUT`:

```sql test
CREATE OR REPLACE FUNCTION net(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE javascript
  WITH (SPRIG_ROLLOUT = 10) AS $$ return price * qty * 0.85 $$;
ALTER FUNCTION net ROLLOUT TO 50 PERCENT;
ALTER FUNCTION net ROLLOUT PROMOTE;
SELECT net(10, 3) AS net;
```

```text output
net
25.5
```

- The new version is the **candidate** for n percent of callers (1 to 99; 100 is an ordinary deploy), and the active version answers the rest.
- **Who gets it.** `SPRIG_ROLLOUT_BY = 'user'` (the default) picks by a hash of the caller's account and the module, so a caller keeps its version while a stage lasts, and a larger stage keeps everyone the smaller one had. `'tenant'` decides once for the whole tenant.
- **Every path follows the pick:** `CALL`, REST, gRPC, MongoDB, Redis and `db.call`. `SELECT f(…)` keeps the active version until the candidate is promoted.
- `ALTER FUNCTION f ROLLOUT TO n [PERCENT] [BY USER | TENANT]` sets another stage; `ROLLOUT PROMOTE` (or `TO 100`) makes the candidate active; `ROLLOUT ABORT` (or `TO 0`) ends the rollout. A deploy without `SPRIG_ROLLOUT`, or a rollback, ends it too. Stages don't advance on their own.
- **The candidate must keep** the active version's parameters (names and types), language and `SQL SECURITY`; otherwise the deploy fails with 39870 `ER_SPRIG_LOGIC_ROLLOUT_BLOCKED`. The first version of a module deploys at once.
- **Automatic rollback.** Gateways count each version's calls, errors and latency, and add them to the catalog every 5 seconds. With at least 20 candidate calls, a candidate whose error rate is above 1% and above twice the active version's, or (both with 20 calls) whose p95 latency is above twice the active version's, is rolled back by SprigDB, and the module's log says why.

Studio shows each version's calls, errors, error rate, mean, p95 and latency histogram, from `GET /v1/databases/{database}/logic/{kind}/{name}/rollout`. Only functions and procedures can be staged; `SPRIG_ROLLOUT` on other kinds fails with 39861.

### Rollback

`ALTER FUNCTION | PROCEDURE f ROLLBACK [TO VERSION n]` makes an earlier version active again at once. Without `TO VERSION`, it is the newest earlier version that deployed. SprigDB compiles that version's source again against today's tables and redeploys it under the same number; no version number is used up.

```sql test
ALTER FUNCTION net ROLLBACK;
SELECT net(10, 3) AS net;
```

```text output
net
27
```

Rolling back or changing a rollout needs `ALTER ROUTINE` on the database. `sprigctl logic rollback` sends the same statement through a gateway.

## Dropping logic

`DROP {FUNCTION | PROCEDURE | VALIDATION | TRIGGER | SCHEDULE} [IF EXISTS] name [RESTRICT | CASCADE]` removes a module, every version's routines and MariaDB triggers on every shard, and a validation's `CHECK` constraint. A trigger or schedule stops at its next check. The name can be used again, starting at version 1.

```sql test
DROP FUNCTION ping;
```

A function or procedure that others depend on isn't dropped:

| Dependent | `RESTRICT` (default) | `CASCADE` |
|---|---|---|
| A schedule that calls it | 39875 | The schedule is dropped too |
| A sink's `TRANSFORM` | 39875 | 39875: change or drop the sink first |
| A computed column made from it | 39875 | The column stays; the link is removed |
| Other logic that calls it | 39875 | Dropped; those calls then fail with 39860 |

`SHOW DEPENDENTS OF FUNCTION f` lists the dependents before you drop. Dropping needs `ALTER ROUTINE` or `CREATE ROUTINE` on the database, or `TRIGGER` for validations and triggers. Dropping the database removes all of its logic.

## Logs and dead letters

Each module keeps a **log**: a line for each deploy, each trigger job's start, the first failure of each change, record or message, each dead letter and replay, each scheduled run, and each automatic rollback. Lines your code writes aren't kept in it.

A **dead letter** is a change, record or message that failed `logic.trigger_max_attempts` times. It holds the event, the attempts, the last error code and message, and a state: `dead`, `replaying`, `replayed` or `discarded`. Replaying runs dead letters again with the active version, oldest first, in the trigger's own job; each runs once even if you mark it again. If a replay fails, it and every later dead letter of the trigger go back to `dead`, so nothing runs out of order.

| Task | Command or route |
|---|---|
| Read the log | `sprigctl logic logs [name] [--follow]`, or `GET /v1/databases/{database}/logic/{kind}/{name}/logs` |
| List dead letters | `sprigctl logic dead-letters list [--state s]`, or `GET …/{kind}/{name}/deadLetters` |
| Replay | `sprigctl logic dead-letters replay <id>… \| --all`, or `POST …/{kind}/{name}/deadLetters:replay` with `{"ids": […]}` or `{"all": true}` |
| Discard | `sprigctl logic dead-letters discard <id>… \| --all`, or `POST …/{kind}/{name}/deadLetters:discard` |

The `sprigctl` commands also take `--catalog`, the tenant's catalog. Logs and dead letters hold row data, so reading them needs `ALTER ROUTINE`, `CREATE ROUTINE` or `ALL` on the database or on `*.*`, or `SPRIG_TENANT_ADMIN`. Studio's logic editor shows both. See [sprigctl](../tools/sprigctl.md).

## Logic elsewhere

- **Sinks** can transform each record with a logic function (`TRANSFORM f`). Under exactly-once delivery the function must be declared `DETERMINISTIC`. See [Sinks](sinks.md).
- **MongoDB server-side JavaScript** (`$where`, `$function`, `$accumulator`) runs on the same QuickJS engine, under `logic.function_budget_ms` and `logic.memory_limit_mb`. See [Documents](documents.md).
- **Redis scripts** (`EVAL`, `FUNCTION LOAD`) run in the same Lua sandbox with Redis's `redis.*` library and their own budgets. See [Key-value](key-value.md).

## Errors

| Code | Name | When |
|---|---|---|
| 39789 | `ER_SPRIG_FUNCTION_NOT_EXPOSED` | A call over a protocol the function's `EXPOSE` doesn't list |
| 39860 | `ER_SPRIG_LOGIC_NOT_FOUND` | Unknown function, version, queue or schedule target |
| 39861 | `ER_SPRIG_LOGIC_SYNTAX` | The declaration or body doesn't parse; `OUT` parameters; `INVOKER` on a kind that always runs as its definer |
| 39862 | `ER_SPRIG_LOGIC_UNSUPPORTED` | No tier can run it: a validation, after-commit trigger or computed column that doesn't compile, `SQL SECURITY DEFINER` in the engine tier, `System.Wait` or `System.Print`; a statement refused in a test run |
| 39863 | `ER_SPRIG_LOGIC_VALIDATION_FAILED` | A validation or `sprig.reject` refused it (MongoDB 121) |
| 39864 | `ER_SPRIG_LOGIC_BUDGET_EXCEEDED` | A budget ran out; the message names the setting |
| 39865 | `ER_SPRIG_LOGIC_USER_ERROR` | The code threw, or a call ended in an event other than `output` where a value was needed |
| 39866 | `ER_SPRIG_LOGIC_RUNTIME_ERROR` | A type error, overflow, division by zero, `NaN` or infinity, `sqlOne` with many rows, a statement of the wrong kind |
| 39867 | `ER_SPRIG_LOGIC_OUTBOUND_DENIED` | `fetch` to a host not on the allowlist, or to an internal address |
| 39868 | `ER_SPRIG_LOGIC_TIER_UNAVAILABLE` | `SPRIG_TIER = 'mariadb'` for code that can't compile |
| 39869 | `ER_SPRIG_LOGIC_VERSION_CONFLICT` | Two deploys of one module at once; retry |
| 39870 | `ER_SPRIG_LOGIC_ROLLOUT_BLOCKED` | A candidate that can't be staged; an automatic rollback |
| 39871 | `ER_SPRIG_LOGIC_ENGINE_BUSY` | No engine thread free and the queue full; retry |
| 39872 | `ER_SPRIG_LOGIC_CALL_DEPTH` | `db.call` nested more than 8 deep |
| 39873 | `ER_SPRIG_LOGIC_SCHEDULE_INVALID` | A bad cron text or time zone |
| 39874 | `ER_SPRIG_LOGIC_SIGNATURE_MISMATCH` | Arguments that don't fit; a parameter that binds to nothing; placeholders and arguments that don't match |
| 39875 | `ER_SPRIG_LOGIC_IN_USE` | `DROP` of code that others depend on |
| 39876 | `ER_SPRIG_LOGIC_ENGINE_UNAVAILABLE` | An engine-tier version reached by a path that can't run it: `SELECT f(…)` with non-constant arguments, a prepared `CALL`, a schedule |
| 39877 | `ER_SPRIG_LOGIC_TRIGGER_POSITION_MOVED` | A stream trigger's record was already done by another run; the run is skipped |

Missing privileges are MySQL's 1044 (database level), 1142 (table level) and 1370 (routine level). See [Errors](../reference/errors.md).

## Not supported

- **Lua for validations, after-commit triggers and computed columns.** They take KIRun or JavaScript.
- **Validations, after-commit triggers and schedules in the engine tier.** Their code must compile into MariaDB.
- **Compiled data access in sharded databases.** A function or procedure that reads or writes tables in a database with more than one shard runs in the engine tier, and an after-commit trigger whose code reads or writes tables can't be deployed on a table whose group has more than one shard.
- **Triggers on collections, key-value namespaces, Redis structures or graphs.** Triggers bind to SQL tables, streams and queues. To react to document or key changes, publish to a stream from your application or from logic.
- **Collections, key-value and search from code.** The `db` interface runs SQL, stream and queue calls only.
- **`SQL SECURITY DEFINER` in the engine tier**, `SELECT f(…)` of engine-tier functions with non-constant arguments or in queries with `FROM`, and prepared `CALL` of engine-tier versions.
- **Rollout of validations, triggers, schedules and computed columns**, rollout by session or by call, and automatic stages: they switch at once, and stages move by hand.
- **Pausing a trigger.** Only triggers declared `FOR EACH TENANT` can be disabled and enabled (see [Inheritance](inheritance.md)); drop and re-create any other trigger instead. A re-created stream trigger starts at the end of the stream.
- **Keeping your code's log lines** in the module's log.
- **`System.Wait` and `System.Print`** in KIRun code.

## Related

- [Logic in SQL](../sql/logic.md): the statement reference
- [Event streams](streams.md) and [Queues](queues.md): what triggers read and code publishes to
- [Sinks](sinks.md): transforming records with a logic function
- [Change feed](../concepts/change-feed.md): what drives after-commit triggers
- [REST and gRPC](../connect/rest-and-grpc.md): calling functions over HTTP
