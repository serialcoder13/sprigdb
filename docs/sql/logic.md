---
title: Logic in SQL
audience: [user, dba]
sidebar:
  order: 7
---

# Logic in SQL

SprigDB runs your code next to your data: functions and procedures written in KIRun, JavaScript, Lua or SQL, validations that check rows before they are written, triggers that run after a commit or for each stream record or queue message, schedules, and computed columns. You declare all of it with SQL. This page is the SQL reference; for the model, the runtimes and the `db` interface your code calls, see [Logic](../data-models/logic.md).

## How code runs

When you declare a function or procedure, SprigDB compiles it, records a new **version** in the database's logic catalog, and deploys it into every shard of the database. Callers always reach it through one wrapper routine under its name, whichever protocol they use.

Each version runs in one of two **tiers**:

- **MariaDB tier**: the code compiles to SQL routines and runs inside MariaDB, in your transaction.
- **Engine tier**: the code runs in the gateway's runtime (the native KIRun runtime, QuickJS for JavaScript, Lua 5.1), and each `db` call it makes runs through the caller's session, with the caller's names, privileges, row-level security and transaction.

SprigDB picks the tier (`SPRIG_TIER = 'auto'`): code that can't compile to SQL, such as code with outbound `fetch` calls or loops it can't bound, runs in the engine tier. After a declaration, `SHOW WARNINGS` lists one note per line of the placement: the module, the version, the tier and each reason for the engine tier.

Budgets are settings, per object, database or tenant: `logic.function_budget_ms` (5000 ms per synchronous call), `logic.trigger_budget_ms`, `logic.job_budget_ms`, `logic.kirun_max_steps`, `logic.max_data_ops` (1000 `db` calls per invocation), `logic.max_rows_read`, `logic.max_loop_iterations` and `logic.memory_limit_mb`. Outbound `fetch` reaches only the hosts in `logic.outbound_allowlist`. See the [settings reference](../reference/settings.md#logic).

## Statements

| Statement | Purpose |
|---|---|
| [`CREATE [OR REPLACE] FUNCTION … LANGUAGE …`](#create-function-and-create-procedure) | Declare or replace a function |
| [`CREATE [OR REPLACE] PROCEDURE … LANGUAGE …`](#create-function-and-create-procedure) | Declare or replace a procedure |
| [`CREATE [OR REPLACE] VALIDATION`](#create-validation) | Check rows before `INSERT` and `UPDATE` |
| [`CREATE [OR REPLACE] TRIGGER … AFTER COMMIT ON`](#create-trigger--after-commit) | Run code after rows commit |
| [`CREATE [OR REPLACE] TRIGGER … ON STREAM \| ON QUEUE`](#create-trigger--on-stream-and-on-queue) | Run code for each stream record or queue message |
| [`CREATE [OR REPLACE] SCHEDULE`](#create-schedule) | Call a procedure on a cron schedule |
| [`ALTER TABLE … ADD c type AS (f(…))`](#computed-columns) | A computed column from a function |
| [`CALL`, `SELECT f(…)`](#calling-functions-and-procedures) | Call code |
| [`TEST FUNCTION \| PROCEDURE \| TRIGGER \| VALIDATION`](#test) | Run code in a sandbox that never writes |
| [`ALTER FUNCTION \| PROCEDURE … ROLLOUT`](#staged-rollouts) | Move a staged rollout |
| [`ALTER FUNCTION \| PROCEDURE … ROLLBACK`](#alter--rollback) | Make an earlier version active |
| [`DROP FUNCTION \| PROCEDURE \| VALIDATION \| TRIGGER \| SCHEDULE`](#drop) | Remove code |
| [`SHOW DEPENDENTS OF FUNCTION \| PROCEDURE`](#show-dependents) | What a `DROP` would affect |

The examples below build on each other. They use `DELIMITER` because bodies contain semicolons; drop it in clients that send one statement at a time.

```sql test
CREATE TABLE products (id BIGINT PRIMARY KEY, price DECIMAL(10,2) NOT NULL, stock INT NOT NULL);
CREATE TABLE orders (id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL, qty INT NOT NULL, note VARCHAR(64));
INSERT INTO products VALUES (7, 400.00, 10), (8, 19.99, 3), (9, 0.10, 0);
```

## CREATE FUNCTION and CREATE PROCEDURE

```sql
CREATE [OR REPLACE] {FUNCTION | PROCEDURE} [db_name.]name
    [([IN] param_name type [, [IN] param_name type] ...)]
    [RETURNS type]
    LANGUAGE {kirun | javascript | js | lua | sql}
    [SQL SECURITY {INVOKER | DEFINER}]
    [[NOT] DETERMINISTIC]
    [EXPOSE (protocol [, protocol] ...)]
    [WITH (option = value [, option = value] ...)]
    [COMMENT 'text']
    AS {'body' | "body" | $$ body $$}

protocol: sql | http | grpc | mongo | redis
option:   SPRIG_TIER = {'auto' | 'mariadb' | 'engine'}
        | SPRIG_ROLLOUT = n            -- 1 to 100
        | SPRIG_ROLLOUT_BY = {'user' | 'tenant'}
```

The clauses may come in any order before `AS`.

| Clause | Default | Meaning |
|---|---|---|
| `OR REPLACE` | | Deploy a new version of an existing module. Without it, an existing name fails |
| `name` | | 1 to 64 characters. Unqualified: the current database |
| `(params)` | none | `IN` parameters only; `OUT` and `INOUT` fail with 39861 (return values instead). A KIRun definition's own parameters take precedence; JavaScript and Lua code takes its parameters from here |
| `RETURNS type` | | A function's SQL return type, such as `DOUBLE` or `DECIMAL(12,2)` |
| `LANGUAGE` | | `kirun` (a KIRun JSON definition), `javascript` or `js`, `lua`. `LANGUAGE sql`, or no `LANGUAGE`, is an ordinary MariaDB stored routine and runs as MariaDB DDL |
| `SQL SECURITY` | `INVOKER` | `DEFINER` runs with the definer's privileges |
| `DETERMINISTIC` | not | Marks the function deterministic |
| `EXPOSE (…)` | `(sql)` | The protocols that may call it: SQL, REST (`http`), gRPC, MongoDB, Redis |
| `SPRIG_TIER` | `'auto'` | `'mariadb'` requires the MariaDB tier and fails with 39868 if the code can't compile to SQL; `'engine'` always uses the engine tier |
| `SPRIG_ROLLOUT` | 100 | Stage the new version for this percentage of callers; see [Staged rollouts](#staged-rollouts) |
| `SPRIG_ROLLOUT_BY` | `'user'` | Pick callers by account or by tenant; only with `SPRIG_ROLLOUT` |
| `COMMENT` | | Accepted and not stored |
| `AS` | | The body: a quoted string or text between `$$` markers |

`DEFINER = …` isn't accepted here (39861): code always gets your account as its definer.

**Privileges:** `CREATE ROUTINE` on the database. The routines are created with your account as definer, and MariaDB checks your right to create them.

### JavaScript

The body is a module whose default export is the function, or plain statements with the parameters in scope. `db` runs SQL through the caller's session; `sprig.event(name, fields)` returns a named event instead of output.

```sql test
DELIMITER //
CREATE OR REPLACE FUNCTION quote_js(productId BIGINT, qty INT) LANGUAGE javascript
  EXPOSE (sql, http, grpc) AS $$
import { db, sprig } from 'sprig:db';
export default function (productId, qty) {
  const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [productId]);
  if (p === null || !(p.stock >= qty)) return sprig.event('error', { message: 'not enough stock' });
  return { total: p.price * qty + 40 };
}
$$ //
CREATE OR REPLACE FUNCTION net(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE js
  AS $$ return price * qty * 0.9; $$ //
DELIMITER ;
```

### Lua

```sql test
CREATE OR REPLACE FUNCTION quote_lua(productId BIGINT, qty INT) LANGUAGE lua
  EXPOSE (sql, http) AS $$
local p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', productId)
if p == sprig.null or not (p.stock >= qty) then
  return sprig.event('error', { message = 'not enough stock' })
end
return { total = p.price * qty + 40 }
$$;
```

### KIRun

The body is a KIRun function definition in JSON: its parameters, events and steps. `Sprig.Db.*` steps run SQL. Build definitions in Studio's visual editor, or compile them with `sprigctl logic compile`.

```sql
CREATE OR REPLACE FUNCTION quote_k LANGUAGE kirun EXPOSE (sql, http, grpc)
  AS '{"namespace": "Shop", "name": "Quote", "parameters": {...}, "steps": {...}}';
```

### SQL

`LANGUAGE sql` (or no `LANGUAGE`) creates an ordinary MariaDB stored function or procedure, under MariaDB's rules. A stored body that writes a MongoDB-style collection fails with 39602.

## Calling functions and procedures

```sql
CALL [db_name.]name([arg [, arg] ...])
SELECT [db_name.]name([arg [, arg] ...])
```

`CALL` returns one row: the `event` column (`output` for a normal return, or the name given to `sprig.event`), then one column for each field of every event the function can raise. Fields of the events that weren't raised are `NULL`, so `quote_js` below has both `total` (from `output`) and `message` (from `error`). `SELECT f(…)` returns the function's value; an `error` or `reject` event makes it fail with 39865 `ER_SPRIG_LOGIC_USER_ERROR` and the event's message.

**Privileges:** `EXECUTE` on the routine (or on its database). Over REST, gRPC, MongoDB and Redis, the protocol must be in `EXPOSE`, else 39789 `ER_SPRIG_FUNCTION_NOT_EXPOSED`; see [REST and gRPC](../connect/rest-and-grpc.md).

```sql test
CALL quote_js(7, 3);
```

```text output
event	total	message
output	1240	NULL
```

```sql test
SELECT quote_js(7, 3) AS js, quote_lua(7, 3) AS lua, net(10, 3) AS n;
```

```text output
js	lua	n
1240	1240	27
```

```sql test-error
-- the function returns an error event: 39865 ER_SPRIG_LOGIC_USER_ERROR, "not enough stock"
SELECT quote_js(7, 1000);
```

## CREATE VALIDATION

```sql
CREATE [OR REPLACE] VALIDATION [db_name.]name
    ON [db_name.]table_name
    FOR {INSERT | UPDATE} [, {INSERT | UPDATE}]
    LANGUAGE {kirun | javascript}
    [other clauses as for CREATE FUNCTION]
    AS body
```

A validation runs before each insert or update of the table and refuses the write when the code returns a `reject` (or `error`) event: the statement fails with 39863 `ER_SPRIG_LOGIC_VALIDATION_FAILED` and the event's message.

KIRun code reads the row through its parameters, by name: a parameter named after a column gets the new value, `old_<column>` the value before the write (`NULL` for an insert), and `op` gets `insert` or `update`. Any other parameter name fails with 39874.

JavaScript code can read the row in any of these forms:

| Form | Reads |
|---|---|
| `export default function (row, old) { … }` | `row.qty`, `old.qty` |
| `export default function (ctx) { … }` | `ctx.trigger.after.qty`, `ctx.trigger.before.qty`, `ctx.trigger.op` |
| `export default function (qty, old_qty, op) { … }` | Columns, `old_<column>` and `op` by name, as in KIRun |
| The body alone | `row`, `old`, `op` and `ctx` are in scope |

`row` is the row after the write. `old` is the row before it, and `null` for an insert, so test it before reading a column of it (`old !== null && old.status === 'shipped'`); reading a column of `null` throws, as in JavaScript. `sprig.reject(message)` or `throw new Error(message)` refuses the write.

When the body is one expression whose only failure is a `reject` with a constant message, and it uses no `old_` or `op` parameter, SprigDB enforces it as a MariaDB `CHECK` constraint. Otherwise it installs `BEFORE INSERT` and `BEFORE UPDATE` triggers. Validations always run with the definer's rights (`SQL SECURITY INVOKER` fails with 39861), and must compile to the MariaDB tier: one that can't is refused with 39862. KIRun and JavaScript validations compile; a Lua validation is refused with 39862.

**Privileges:** `TRIGGER` on the table.

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
INSERT INTO orders VALUES (100, 7, 1, 'first');
```

```sql test-error
-- 39863 ER_SPRIG_LOGIC_VALIDATION_FAILED: qty must be positive
INSERT INTO orders VALUES (101, 7, 0, 'none');
```

The same rules in JavaScript, with a message that names the value, and a second rule that reads the row before the write:

```sql
DELIMITER //
CREATE OR REPLACE VALIDATION order_rules ON orders FOR INSERT, UPDATE LANGUAGE javascript AS $$
export default function (row, old) {
  if (row.qty <= 0) sprig.reject(`qty ${row.qty} must be positive`);
  if (old !== null && old.note === 'shipped' && row.qty !== old.qty)
    sprig.reject("the quantity of a shipped order can't change");
}
$$ //
DELIMITER ;
```

Because it reads `old`, this validation becomes `BEFORE INSERT` and `BEFORE UPDATE` triggers rather than a `CHECK` constraint; the deploy notes say which.

## CREATE TRIGGER … AFTER COMMIT

```sql
CREATE [OR REPLACE] TRIGGER [db_name.]name
    AFTER COMMIT ON [db_name.]table_name
    FOR event [, event] ...
    LANGUAGE {kirun | javascript}
    [other clauses as for CREATE FUNCTION]
    AS body

event: INSERT | UPDATE [OF (column [, column] ...)] | DELETE
```

An after-commit trigger runs once for each committed row change, outside the writing transaction. It reads the tenant's change feed, so it never slows the write and never sees rolled-back rows. `UPDATE OF (columns)` fires only when one of those columns changed. The code reads the row as a validation does, in KIRun or JavaScript; for a `DELETE`, `row` is the deleted row.

Delivery:

- Changes to the same row run in order; different rows run in parallel, in `logic.trigger_lanes` lanes per shard (default 8).
- Each change runs exactly once: a marker commits with the trigger's own writes, so a retried change does nothing twice.
- A failure is retried with backoff from `logic.trigger_retry_initial_ms` (1 s) to `logic.trigger_retry_max_ms` (5 min). After `logic.trigger_max_attempts` failures (default 10), the change goes to the dead letters and the lane moves on.

Triggers always run with the definer's rights. An after-commit trigger must compile to the MariaDB tier: KIRun and JavaScript triggers compile, and a Lua after-commit trigger is refused with 39862. A `CREATE TRIGGER` without `AFTER COMMIT`, `ON STREAM` or `ON QUEUE` is an ordinary MariaDB trigger.

**Privileges:** `TRIGGER` and `SELECT` on the table. The code receives the changed rows as its definer, so it needs the right to read them.

```sql test
CREATE TABLE audit (id BIGINT AUTO_INCREMENT PRIMARY KEY, order_id BIGINT, qty INT);
CREATE TRIGGER audit_orders AFTER COMMIT ON orders FOR INSERT LANGUAGE kirun AS '{"namespace": "Shop", "name": "AuditOrder",
 "parameters": {"id": {"parameterName": "id", "schema": {"type": ["INTEGER"]}},
                "qty": {"parameterName": "qty", "schema": {"type": ["INTEGER"]}}},
 "steps": {"ins": {"statementName": "ins", "namespace": "Sprig.Db", "name": "Exec",
   "parameterMap": {"sql": {"s": {"key": "s", "type": "VALUE", "value": "INSERT INTO audit (order_id, qty) VALUES (?, ?)"}},
                    "args": {"a": {"key": "a", "type": "EXPRESSION", "order": 1, "expression": "Arguments.id"},
                             "b": {"key": "b", "type": "EXPRESSION", "order": 2, "expression": "Arguments.qty"}}}}}}';
```

In JavaScript, the same audit for every kind of change (for a `DELETE`, `row` is the deleted row):

```sql
DELIMITER //
CREATE TRIGGER audit_orders_js AFTER COMMIT ON orders FOR INSERT, UPDATE, DELETE LANGUAGE javascript AS $$
export default function (row, old, op) {
  db.exec('INSERT INTO audit (order_id, qty) VALUES (?, ?)', [row.id, row.qty]);
}
$$ //
DELIMITER ;
```

## CREATE TRIGGER … ON STREAM and ON QUEUE

```sql
CREATE [OR REPLACE] TRIGGER [db_name.]name
    ON {STREAM [db_name.]stream_name [FAN IN] | QUEUE [db_name.]queue_name}
    LANGUAGE {kirun | javascript | lua}
    [other clauses as for CREATE FUNCTION]
    AS body
```

| Source | Runs |
|---|---|
| `ON STREAM s` | For every record, in each partition's offset order, with the trigger's own consumer group. The offset commits in the same transaction as the code's writes, so each record's writes happen exactly once |
| `ON STREAM s FAN IN` | `s` is an inherited stream definition; the trigger reads every member tenant's instance below your tenant. Needs `SPRIG_FAN_IN` |
| `ON QUEUE q` | As one more competing consumer of the queue: each message is acknowledged in the same transaction as the code's writes |

Stream and queue triggers always run in the engine tier, as the definer. JavaScript and Lua code gets one argument, `ctx` (also a global):

- `ctx.record`: `stream`, `partition`, `offset`, `key`, `value`, `headers`, `timestamp`;
- `ctx.message`: `queue`, `exchange`, `routingKey`, `body`, `properties`, `redelivered`, `deliveryCount`.

KIRun parameters bind by name to those fields, or to `record`, `message` or `ctx` for the whole object.

After `logic.trigger_max_attempts` failures, a stream record goes to the dead letters and the offset moves past it. A queue message is written to the trigger's dead letters and then rejected, so the queue also sends it to its dead-letter exchange when it has one. Until then, each failure returns the message to the queue with its delivery count raised. `FAN IN` isn't allowed on queues.

A trigger on an inherited stream for every member tenant takes `FOR EACH TENANT GRANTS (…)`; see [FOR EACH TENANT](streams-queues-sinks.md#for-each-tenant). The streams and queues themselves are in [Streams, queues and sinks](streams-queues-sinks.md).

**Privileges:** `CREATE ROUTINE` on the trigger's database, plus `SELECT` on the stream, or `SELECT` and `DELETE` on the queue. `FAN IN` also needs `SPRIG_FAN_IN`.

```sql
DELIMITER //
CREATE TRIGGER audit_events ON STREAM order_events LANGUAGE javascript AS $$
export default function (ctx) {
  const e = JSON.parse(ctx.record.value);
  db.exec('INSERT INTO audit (order_id, qty) VALUES (?, ?)', [e.order_id, e.qty]);
}
$$ //
DELIMITER ;

CREATE TRIGGER bill ON QUEUE billing LANGUAGE lua AS $$
local m = cjson.decode(ctx.message.body)
db.exec('UPDATE orders SET note = ? WHERE id = ?', 'billed', m.order_id)
$$;
```

## CREATE SCHEDULE

```sql
CREATE [OR REPLACE] SCHEDULE [db_name.]name
    CRON 'minute hour day-of-month month day-of-week'
    [TIME ZONE 'zone']
    [OVERLAP {SKIP | QUEUE}]
    DO CALL [db_name.]procedure([constant [, constant] ...])
```

| Clause | Default | Meaning |
|---|---|---|
| `CRON` | | A five-field cron expression |
| `TIME ZONE` | `'UTC'` | An IANA time zone name, such as `'Europe/London'` |
| `OVERLAP SKIP` | yes | A fire while the previous run is still going is skipped |
| `OVERLAP QUEUE` | | One fire waits and runs when the previous run ends |
| `DO CALL` | | The procedure and its arguments: string, number, `TRUE`, `FALSE` or `NULL` literals |

Each fire runs once, on a worker, within `logic.job_budget_ms` (5 minutes by default), with the definer's rights. A fire late by more than the misfire window is recorded as misfired and skipped. A malformed cron text or zone fails with 39873 `ER_SPRIG_LOGIC_SCHEDULE_INVALID`. `SPRIG_ROLLOUT` isn't allowed on schedules, validations or triggers: they switch versions at once.

**Privileges:** `CREATE ROUTINE` on the database.

```sql test
DELIMITER //
CREATE OR REPLACE PROCEDURE restock() LANGUAGE javascript AS $$
db.exec('UPDATE products SET stock = 10 WHERE stock < 2');
return {};
$$ //
DELIMITER ;
CREATE SCHEDULE nightly_restock CRON '0 3 * * *' TIME ZONE 'Europe/London' DO CALL restock();
```

## Computed columns

```sql
ALTER TABLE [db_name.]table_name ADD [COLUMN] column_name type [GENERATED ALWAYS] AS (function_name(arg [, arg] ...)) [column_options]
```

When `function_name` is a KIRun or JavaScript function that reduces to one expression, SprigDB puts the expression, with the arguments in place of the parameters, into the column definition, and the statement runs as ordinary DDL: the column is a MariaDB generated column. A KIRun function's `output` event must have exactly one field, the value, and every path through its steps must end in that event or in an argument check (which gives `NULL`). A JavaScript function returns the value. `STORED` and `VIRTUAL` columns both work.

- A wrong argument count fails with 39874 `ER_SPRIG_LOGIC_SIGNATURE_MISMATCH`.
- A function with no expression form fails with 39862 `ER_SPRIG_LOGIC_UNSUPPORTED`: data access, loops, or a Lua function.
- A JavaScript function that isn't deterministic fails with 39862 and the reason `js.nondeterministic`: `Math.random()`, `Date.now()`, `Date()`, `performance.now()` and `crypto.*` give a different value on each call.
- A new version of the function with the same parameters deploys, and the column keeps the expression of the version it was made from (a note says so). A new version with different parameters fails with 39875 and names the columns.

```sql test
CREATE OR REPLACE FUNCTION net_value LANGUAGE kirun AS '{"namespace": "Shop", "name": "NetValue",
 "parameters": {
   "price": {"parameterName": "price", "schema": {"type": ["DOUBLE"]}},
   "qty": {"parameterName": "qty", "schema": {"type": ["INTEGER"]}}},
 "events": {"output": {"name": "output", "parameters": {"value": {"type": ["DOUBLE"]}}}},
 "steps": {"out": {"statementName": "out", "namespace": "System", "name": "GenerateEvent",
   "parameterMap": {"results": {"r1": {"type": "VALUE", "value": {"name": "value",
     "value": {"isExpression": true, "value": "Arguments.price * Arguments.qty * 0.9"}}}}}}}}';
ALTER TABLE products ADD COLUMN stock_value DOUBLE AS (net_value(price, stock));
SELECT id, stock_value FROM products WHERE id = 7;
```

```text output
id	stock_value
7	3600
```

In JavaScript, with a discount for ten or more:

```sql
DELIMITER //
CREATE OR REPLACE FUNCTION line_net(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE javascript AS $$
export default function (price, qty) {
  if (qty >= 10) return price * qty * 0.9;
  return price * qty;
}
$$ //
DELIMITER ;
ALTER TABLE products ADD COLUMN stock_net DOUBLE AS (line_net(price, stock)) STORED;
```

## EXPLAIN FUNCTION

```sql
EXPLAIN {FUNCTION | PROCEDURE} [db_name.]name [FORMAT = SQL]
```

Shows where each version of the code runs and why. It returns one row per version that isn't retired or superseded, newest first:

| Column | Meaning |
|---|---|
| `Version` | The version number |
| `State` | The version's state; `(live)` marks the active one |
| `Tier` | `mariadb` (compiled) or `engine` |
| `Placement` | Why the version runs in that tier (each reason with its place in the source), the route, the tables read and written, and the cost |
| `PL/SQL` | With `FORMAT = SQL` only: the generated MariaDB code |

A name with no function or procedure fails with 39860. **Privileges:** `EXECUTE`, `ALTER ROUTINE` or `CREATE ROUTINE` on the database; `FORMAT = SQL` needs `ALTER ROUTINE`. `sprigctl logic compile` prints the same for code you haven't deployed.

```sql
EXPLAIN FUNCTION quote_js;
EXPLAIN FUNCTION line_net FORMAT = SQL;
```

## TEST

```sql
TEST {FUNCTION | PROCEDURE | TRIGGER | VALIDATION} [db_name.]name [VERSION n]
    [(arg [, arg] ...)]
    [WITH 'json_options']
```

Runs a version (the active one by default) in a sandbox and rolls everything back. The run is always in the engine tier, as you (code that normally runs as its definer runs with your rights here), inside a transaction (or a savepoint in yours). `fetch` calls are never sent: they are answered from the mocks you give or refused.

The answer is one row with three columns: `event`, `result` and `report`. The report is JSON: the event and fields or the error, each `db` statement with its timing, the writes, logs, fetches, the snapshot tables after the run, and the budgets used.

| JSON option | Meaning |
|---|---|
| `source` | A draft: a whole `CREATE … AS …` declaration, run without being recorded or deployed. Needs `CREATE ROUTINE` |
| `args` | Arguments: a list (by position), an object (by parameter name) or one value |
| `row`, `old`, `op` | For validations and after-commit triggers: the row after the write, before it, and `insert`, `update` or `delete` |
| `record` | For stream triggers: `ctx.record` |
| `message` | For queue triggers: `ctx.message` |
| `rows` | `{"table": [{…}, …]}`: the code sees exactly these rows of each named table (temporary copies) |
| `fetch` | Mock answers for outbound calls |

Statements that could end the transaction or reach outside it (DDL, `COMMIT`, writes outside the snapshot) are refused inside a test run with 39862, and the code sees that error. A request that names nothing, or too many rows, fails with 39874.

The same run is available as `POST /v1/databases/{db}/logic:testRun` and gRPC `sprig.v1.Logic/TestRun`.

```sql test
TEST FUNCTION quote_js (7, 3);
TEST FUNCTION quote_js WITH '{"args": {"productId": 8, "qty": 1}, "rows": {"products": [{"id": 8, "price": 5, "stock": 1}]}}';
TEST FUNCTION draft WITH '{"source": "CREATE FUNCTION draft(a INT) RETURNS INT LANGUAGE lua AS $$ return a * 2 $$", "args": [21]}';
```

A validation takes the row as it would see it. This run reports 39863 with "the quantity of a shipped order can't change":

```sql
TEST VALIDATION order_rules WITH '{"row": {"id": 100, "qty": 3, "note": "shipped"}, "old": {"id": 100, "qty": 1, "note": "shipped"}, "op": "update"}';
```

## Versions

Every `CREATE OR REPLACE` of a function or procedure records a new version and makes it active (or the candidate of a staged rollout). Versions are numbered from 1 and never reused. A deploy that fails on any shard leaves the previous version active. List versions with `sprigctl logic show`, or in Studio's logic editor.

### Staged rollouts

```sql
CREATE OR REPLACE FUNCTION ... WITH (SPRIG_ROLLOUT = n [, SPRIG_ROLLOUT_BY = {'user' | 'tenant'}]) AS ...

ALTER {FUNCTION | PROCEDURE} [db_name.]name ROLLOUT TO n [PERCENT] [BY {USER | TENANT}]
ALTER {FUNCTION | PROCEDURE} [db_name.]name ROLLOUT PROMOTE      -- same as TO 100
ALTER {FUNCTION | PROCEDURE} [db_name.]name ROLLOUT ABORT        -- same as TO 0
```

With `SPRIG_ROLLOUT = n` (1 to 99), the new version is the **candidate** for `n` percent of callers, and the active version answers the rest. Callers are picked by a hash of their account (`BY USER`, the default) or of the tenant (`BY TENANT`), so a caller keeps its version during a stage, and a larger stage keeps everyone the smaller one had. `ROLLOUT TO` moves the stage; `PROMOTE` makes the candidate active; `ABORT` returns everyone to the active version.

Gateways count calls, errors and latency per version. When the candidate has enough calls and its error rate is above twice the active version's and above 1%, or its p95 latency is above twice the active version's, SprigDB aborts the rollout on its own and records why in the logic log. Stages don't advance on their own.

**Privileges:** `ALTER ROUTINE` on the database for `ALTER … ROLLOUT`; `CREATE ROUTINE` on the database for the `CREATE OR REPLACE` that starts a rollout.

```sql test
DELIMITER //
CREATE OR REPLACE FUNCTION net(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE js
  WITH (SPRIG_ROLLOUT = 10) AS $$ return price * qty * 0.85; $$ //
DELIMITER ;
ALTER FUNCTION net ROLLOUT TO 50 PERCENT;
ALTER FUNCTION net ROLLOUT ABORT;
```

### ALTER … ROLLBACK

```sql
ALTER {FUNCTION | PROCEDURE} [db_name.]name ROLLBACK [TO VERSION n]
```

Makes an earlier version active again, at once. The version's own source is compiled again against today's tables and redeployed under the same version number; the version it replaces becomes `superseded`. Without `TO VERSION`, the target is the newest earlier version that deployed. `sprigctl logic rollback` sends the same statement.

```sql
ALTER FUNCTION net ROLLBACK;
ALTER FUNCTION shop.quote_js ROLLBACK TO VERSION 1;
```

## DROP

```sql
DROP {FUNCTION | PROCEDURE | VALIDATION | TRIGGER | SCHEDULE} [IF EXISTS] [db_name.]name
    [RESTRICT | CASCADE]
```

Removes the module and every version's routines and MariaDB triggers on every shard. A validation's `CHECK` constraint or triggers go with it; a trigger or schedule stops. A `FUNCTION`, `PROCEDURE` or `TRIGGER` that isn't a logic module is dropped as ordinary MariaDB DDL; a missing `VALIDATION` or `SCHEDULE` fails with 39860 (a note with `IF EXISTS`).

A function or procedure may have dependents:

| Dependent | `RESTRICT` (default) | `CASCADE` |
|---|---|---|
| A schedule that calls it | 39875 | The schedule is dropped too |
| A sink's `TRANSFORM` | 39875 | 39875: alter or drop the sink first |
| A computed column made from it | 39875 | The column stays (its expression is its own); the link is removed |
| Other code that calls it | 39875 | Dropped anyway; those calls then fail with 39860 |

**Privileges:** `ALTER ROUTINE` (functions, procedures, schedules) or `TRIGGER` (validations, triggers) on the database, or `CREATE ROUTINE`.

```sql test-error
-- products.stock_value is computed from net_value: 39875 ER_SPRIG_LOGIC_IN_USE
DROP FUNCTION net_value;
```

## SHOW DEPENDENTS

```sql
SHOW DEPENDENTS OF {FUNCTION | PROCEDURE} [db_name.]name
```

Lists what a `DROP` of the routine would find, so you don't have to read 39875's message.

| Column | Meaning |
|---|---|
| `Kind` | `schedule`, `sink`, `column` or `caller` |
| `Name` | The dependent, such as `products.stock_value` |
| `Cascade` | What `CASCADE` does to it: `dropped`, `refused` or `kept` |
| `Detail` | Why |

```sql test
SHOW DEPENDENTS OF FUNCTION net_value;
SHOW DEPENDENTS OF PROCEDURE restock;
DROP SCHEDULE nightly_restock;
DROP FUNCTION net_value CASCADE;
```

## Logs and dead letters

What triggers and schedules did, and the events that kept failing, aren't SQL tables. Read and settle them with:

- `sprigctl logic logs [name] [--follow]`;
- `sprigctl logic dead-letters list | replay | discard` (replay sends them back to the trigger in order, with the active version);
- Studio's logic editor, or `GET /v1/databases/{db}/logic/{kind}/{name}/logs` and `…/deadLetters`, `…/deadLetters:replay`, `…/deadLetters:discard`.

Reading logs and dead letters needs `ALTER ROUTINE`, `CREATE ROUTINE` or `ALL` on the database or `*.*`, or `SPRIG_TENANT_ADMIN`, because they hold row data. See [sprigctl](../tools/sprigctl.md).

## Errors

| Code | Name | When |
|---|---|---|
| 1142, 1044, 1370 | | Missing `CREATE ROUTINE`, `TRIGGER`, `ALTER ROUTINE` or `EXECUTE` |
| 39602 | | A MariaDB stored body that writes a collection |
| 39789 | `ER_SPRIG_FUNCTION_NOT_EXPOSED` | A call over a protocol the function's `EXPOSE` doesn't name |
| 39860 | `ER_SPRIG_LOGIC_NOT_FOUND` | Unknown function, version or binding; a call of dropped code |
| 39861 | `ER_SPRIG_LOGIC_SYNTAX` | The declaration or the body doesn't parse, or a clause isn't allowed for the kind |
| 39862 | `ER_SPRIG_LOGIC_UNSUPPORTED` | The code can run in no tier, such as a validation that needs the engine tier; a refused statement in a test run |
| 39863 | `ER_SPRIG_LOGIC_VALIDATION_FAILED` | A validation rejected the row |
| 39864 | `ER_SPRIG_LOGIC_BUDGET_EXCEEDED` | A budget (time, steps, rows, data operations, memory) ran out |
| 39865 | `ER_SPRIG_LOGIC_USER_ERROR` | The code threw, or a `SELECT f()` got an `error` event |
| 39866 | `ER_SPRIG_LOGIC_RUNTIME_ERROR` | A type error, overflow, division by zero, or `sqlOne` that found several rows |
| 39867 | `ER_SPRIG_LOGIC_OUTBOUND_DENIED` | `fetch` to a host outside `logic.outbound_allowlist`, or to an internal address |
| 39868 | `ER_SPRIG_LOGIC_TIER_UNAVAILABLE` | `SPRIG_TIER = 'mariadb'` for code that can't compile to SQL |
| 39869 | `ER_SPRIG_LOGIC_VERSION_CONFLICT` | Another deploy of the module ran at the same time |
| 39870 | `ER_SPRIG_LOGIC_ROLLOUT_BLOCKED` | A version that can't be staged (other parameters, language or rights), or a candidate already rolled back automatically |
| 39871 | `ER_SPRIG_LOGIC_ENGINE_BUSY` | The engine's queue is full; retry |
| 39872 | `ER_SPRIG_LOGIC_CALL_DEPTH` | Nested `db.call` too deep |
| 39873 | `ER_SPRIG_LOGIC_SCHEDULE_INVALID` | A bad cron text or time zone |
| 39874 | `ER_SPRIG_LOGIC_SIGNATURE_MISMATCH` | Wrong arguments, or a parameter that binds to nothing |
| 39875 | `ER_SPRIG_LOGIC_IN_USE` | `DROP` of code that others depend on |
| 39876 | `ER_SPRIG_LOGIC_ENGINE_UNAVAILABLE` | An engine-tier version was called through a path without the runtime |
| 39877 | `ER_SPRIG_LOGIC_TRIGGER_POSITION_MOVED` | A stream trigger's offset moved under it (a second run of a record); the run is rolled back |

## Related

- [Logic](../data-models/logic.md)
- [Streams, queues and sinks](streams-queues-sinks.md)
- [REST and gRPC](../connect/rest-and-grpc.md)
- [Studio](../tools/studio.md)
- [sprigctl](../tools/sprigctl.md)
