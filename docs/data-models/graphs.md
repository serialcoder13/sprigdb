---
title: Graphs
audience: [user, dba]
sidebar:
  order: 6
---

# Graphs

SprigDB stores property graphs in Neo4j's model and answers Cypher over the Bolt protocol, so Neo4j drivers and tools work unchanged. You can also query a graph from SQL with `GRAPH_TABLE`, and declare a read-only graph over tables you already have. Use a graph when your questions follow relationships: friends of friends, shortest routes, recommendations, dependency chains.

## The model

A graph holds:

- **Nodes**, each with zero or more **labels** (`Person`, `Product`) and a map of **properties**.
- **Relationships**, each directed from one node to another, with exactly one **type** (`KNOWS`, `BOUGHT`) and a map of properties.

Property values are Neo4j's types: integers, floats, strings, booleans, byte arrays, lists of one type, the temporal types (`Date`, `LocalTime`, `Time`, `LocalDateTime`, `DateTime`, `Duration`) and `Point`. A map can't be stored as a property value; storing one fails with `ER_SPRIG_GRAPH_PROPERTY_TYPE` (39909), as in Neo4j.

There are two kinds of graph:

| Kind | Created with | Data lives in | Writes |
|---|---|---|---|
| Native | `CREATE GRAPH`, or the first write to the Bolt default graph | Node and relationship tables that SprigDB manages | Yes |
| Mapped | `CREATE PROPERTY GRAPH` | Your existing tables, read live | No (39904) |

A graph belongs to a database, like a table. Its full name is `<database>.<graph>`, for example `shop.social`.

## Create a native graph

Create and drop graphs with SQL on the MySQL (3306) or MariaDB (3307) port. The `IF NOT EXISTS` and `IF EXISTS` forms don't fail when the graph is already there, or already gone:

```sql test
CREATE GRAPH social;
CREATE GRAPH IF NOT EXISTS social;
DROP GRAPH social;
DROP GRAPH IF EXISTS social;
```

Name a graph in another database as `<database>.<graph>`, for example `CREATE GRAPH shop.social`. Creating a graph that exists fails with `ER_SPRIG_GRAPH_DEFINITION_INVALID` (39908):

```sql test-error
CREATE GRAPH social;
CREATE GRAPH social;
```

`CREATE GRAPH` needs the `CREATE` privilege on the database, and `DROP GRAPH` needs `DROP` on the graph or its database. Without a database name, the session's current database is used; with none selected, the statement fails with MySQL error 1046.

Labels, relationship types and property keys aren't declared. A label or type is registered the first time a write uses it.

Over Bolt, a write to the default graph creates it if it doesn't exist yet (see [Graph names over Bolt](#graph-names-over-bolt)). Any other graph must exist first, or the statement fails with `ER_SPRIG_GRAPH_NOT_FOUND` (39902).

### How a native graph is stored

Each native graph is its own sharded table group, keyed by a numeric partition column, and starts with `sharding.min_shards` shards (1 by default). Each shard schema holds these tables, named after the graph's internal id:

| Table | Holds | Primary key |
|---|---|---|
| `__sprig_gn_<id>` | Nodes: `id`, `part` (the distribution key, equal to `id`), `labels` (JSON list of label ids), `props` (JSON), `_types` | `id` |
| `__sprig_ge_<id>` | Relationships, on their **source** node's shard: `src`, `type`, `dst`, `id`, `props` | `(src, type, dst, id)`, plus a key on `(dst, type, src)` |
| `__sprig_gl_<id>` | One row per node and label, for label scans | `(label, id)` |
| `__sprig_gm_<id>` | Mirrors of incoming relationships, on the **destination** node's shard | `(dst, type, src, id)` |
| `__sprig_gu_<id>` | Uniqueness claims for constraints across shards | `(idx, k)` |

Properties are JSON, with a `_types` entry for values JSON can't tell apart (a float `1.0`, a date, a byte array), so every value reads back with its original type. The graph's definition, labels, types and indexes are rows in the tenant's catalog schema.

You never query these tables directly; they are listed so that you can recognise them in MariaDB and in plans.

## Create a graph over existing tables

`CREATE PROPERTY GRAPH` (SQL/PGQ) declares a **mapped graph** over tables you already have, without copying anything. Each table becomes one label or relationship type, and each query reads the tables live:

```sql test
CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(40), email VARCHAR(80), joined DATE);
CREATE TABLE products  (sku INT PRIMARY KEY, name VARCHAR(40), price DECIMAL(8,2));
CREATE TABLE orders    (id INT PRIMARY KEY, customer_id INT, sku INT, qty INT, placed_at DATETIME);

INSERT INTO customers VALUES
  (1, 'Alice', 'alice@example.com', '2020-01-02'),
  (2, 'Bob', 'bob@example.com', '2021-03-04'),
  (3, 'Carol', NULL, NULL);
INSERT INTO products VALUES (10, 'Lamp', 10.50), (11, 'Desk', 100.00), (12, 'Pen', 1.25);
INSERT INTO orders VALUES
  (100, 1, 10, 2, '2024-01-01 10:00:00'),
  (101, 1, 12, 10, '2024-01-02 11:00:00'),
  (102, 2, 11, 1, '2024-02-01 09:30:00'),
  (103, 2, 12, 3, '2024-02-03 08:00:00'),
  (104, 3, 10, 1, '2024-03-01 12:00:00');

CREATE PROPERTY GRAPH shop_graph
  VERTEX TABLES (
    customers KEY (id) LABEL Person PROPERTIES (id, name, email, joined),
    products  KEY (sku) LABEL Product PROPERTIES (sku, name, price))
  EDGE TABLES (
    orders KEY (id)
      SOURCE KEY (customer_id) REFERENCES customers (id)
      DESTINATION KEY (sku) REFERENCES products (sku)
      LABEL BOUGHT PROPERTIES (qty, placed_at));
```

The full grammar:

```text
CREATE [OR REPLACE] PROPERTY GRAPH [IF NOT EXISTS] [db.]name
  VERTEX TABLES ( vertex [, vertex ...] )
  [ EDGE TABLES ( edge [, edge ...] ) ]

vertex: table [AS alias] KEY (column) [LABEL name | DEFAULT LABEL] [properties]
edge:   table [AS alias] KEY (column)
          SOURCE KEY (column) REFERENCES vertex_table (key_column)
          DESTINATION KEY (column) REFERENCES vertex_table (key_column)
          [LABEL name | DEFAULT LABEL] [properties]

properties: PROPERTIES (expr [AS name], ...) | PROPERTIES ARE ALL COLUMNS | NO PROPERTIES
```

| Clause | Default | Meaning |
|---|---|---|
| `KEY (column)` | required | The element's identity: one integer column |
| `LABEL name` | the table name | The node label or relationship type |
| `PROPERTIES (...)` | all columns | Columns or SQL expressions; an expression needs `AS name`, for example `JSON_VALUE(doc, '$.stars') AS stars` |
| `SOURCE KEY` / `DESTINATION KEY` | required on edges | The edge column and the vertex table (or its alias) it references; the reference must be that vertex table's `KEY` |
| `OR REPLACE` | off | Replace an existing mapped graph of that name |

Mapped graphs have these rules:

- One integer key column per table, with values below 2^48. Composite keys fail with `ER_SPRIG_GRAPH_DEFINITION_INVALID` (39908).
- One table per label and one table per type; using a label twice fails with 39908.
- The tables must be unsharded. A sharded table fails with 39908.
- A missing table or column fails when you create the graph.
- You need `CREATE` on the database and `SELECT` on every table the graph maps.
- The graph is read-only. `CREATE`, `MERGE`, `SET`, `REMOVE` and `DELETE` fail with `ER_SPRIG_GRAPH_READ_ONLY_SOURCE` (39904). Change the tables with SQL instead.
- `id()` of a mapped element is `source × 2^48 + key`, so it is stable and usable in `WHERE id(n) = ...`.
- Per-label grants don't apply to mapped graphs; the `SELECT` check on each table does.

For example, a table that doesn't exist and a composite key both fail:

```sql test-error
CREATE PROPERTY GRAPH bad VERTEX TABLES (no_such_table KEY (id));
```

```sql test-error
CREATE PROPERTY GRAPH bad VERTEX TABLES (customers KEY (id, email));
```

Drop a mapped graph with `DROP GRAPH` or `DROP PROPERTY GRAPH`. Dropping it never touches the tables.

## Query with Cypher over Bolt

Turn on the Bolt listener (`listener.bolt.enabled`, port 7687, TLS required by default) and connect with any Neo4j driver, `cypher-shell`, or `sprig --protocol bolt`. Log in with the `basic` scheme as `acme.app`, or with the tenant taken from the TLS server name. See [Bolt and Cypher](../connect/bolt-and-cypher.md) for drivers, logins, routing, bookmarks and versions.

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("neo4j+s://db.example.com:7687",
                              auth=("acme.app", "s3cret-Passw0rd"))
with driver.session(database="shop.social") as s:
    s.run("CREATE (a:Person {name: 'Alice', email: 'alice@example.com'})"
          "-[:KNOWS {since: 2010}]->(b:Person {name: 'Bob'})")
    for r in s.run("MATCH (a:Person {name: $n})-[:KNOWS]->(b) RETURN b.name AS name", n="Alice"):
        print(r["name"])
```

### Graph names over Bolt

The driver's `database` names the graph:

| `database` given | Graph used |
|---|---|
| `shop.social` | Graph `social` in database `shop` |
| `social` | Graph `social` in the database named by `bolt.logical_database` (default `graph`) |
| none | The graph named by `bolt.default_graph` (default `neo4j`) in `bolt.logical_database` |

The default graph is created on its first write, if its database exists. Reading it before then returns no rows. `USE` inside a statement is refused (39901); name the graph as the session's database instead.

### Transactions and isolation

- Each auto-commit statement is one transaction. An explicit transaction (`BEGIN` … `COMMIT`) holds its connections until it ends.
- Bolt transactions run at `READ COMMITTED` by default, Neo4j's level. Set `bolt.isolation` to `repeatable_read` to change it.
- A transaction that writes on one cluster commits locally. One that writes on several clusters commits with two-phase commit, so a graph split across clusters still commits atomically.
- Deleting a node that still has relationships is allowed inside a transaction, but the commit fails with `ER_SPRIG_GRAPH_NODE_HAS_RELATIONSHIPS` (39906) if any remain. Use `DETACH DELETE` to remove them together.
- A transaction's `tx_timeout` bounds its lock waits and its run time, and `tx_metadata` is returned by `CALL tx.getMetaData()`.
- If a shard moves while a statement runs, an auto-commit statement runs again on the new layout; an explicit transaction fails with `ER_SPRIG_SHARD_MAP_CHANGED` (39424), which drivers treat as transient and retry.

## Query with Cypher over REST

The REST statement endpoints take a `language` field. Put the graph's full name in the path where the database usually goes:

```sh
curl -s https://db.example.com:8443/v1/databases/shop.social/sql:execute \
  -H 'Authorization: Bearer <api-key>' \
  -H 'Content-Type: application/json' \
  -d '{"language": "cypher",
       "sql": "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b"}'
```

`language` is `cypher` or `gql`. Each call is one auto-commit statement, with the same graphs, privileges and limits as Bolt. Writes report their counters in the result's `info` (`nodes-created: 2, relationships-created: 1`). Strings, numbers and booleans come back as text; nodes, relationships and paths come back as JSON objects `{"$node": ...}`, `{"$rel": ...}` and `{"$path": [...]}`. `POST .../sql:query` streams the same result as NDJSON, and `max_rows`, `max_bytes` and `timeout_ms` apply. These languages can't use a REST transaction handle.

From the shell, `sprig -l cypher` sends Cypher over REST, and `sprig --protocol bolt` speaks Bolt directly.

## Query a graph from SQL with GRAPH_TABLE

`GRAPH_TABLE` runs a graph pattern inside any SQL statement and returns its rows as a derived table, so graph results join with ordinary tables:

```sql test
SELECT g.name, SUM(g.total) AS spent
FROM GRAPH_TABLE (shop_graph
       MATCH (c IS Person)-[o IS BOUGHT]->(p IS Product)
       WHERE c.email = 'alice@example.com'
       COLUMNS (p.name AS name, o.qty * p.price AS total)) AS g
GROUP BY g.name;
```

Who bought a pen, and how many people the graph holds:

```sql test
SELECT g.name
FROM GRAPH_TABLE (shop_graph
       MATCH (c IS Person)-[IS BOUGHT]->(p IS Product)
       WHERE p.name = 'Pen'
       COLUMNS (c.name AS name)) AS g
ORDER BY g.name;
```

```text output
name
Alice
Bob
```

```sql test
SELECT COUNT(*) AS people
FROM GRAPH_TABLE (shop_graph MATCH (c IS Person) COLUMNS (c.id AS id)) AS g;
```

```text output
people
3
```

A native graph joins with tables the same way. Here `social` holds `P` nodes with a `cid` property, written over Bolt:

```sql test
SELECT a.name, b.name
FROM GRAPH_TABLE (social MATCH (x IS P)-[IS FOLLOWS]->(y IS P)
                  COLUMNS (x.cid AS f, y.cid AS t)) AS g
JOIN customers a ON a.id = g.f
JOIN customers b ON b.id = g.t;
```

The form is `GRAPH_TABLE ([db.]graph MATCH pattern [WHERE condition] COLUMNS (expr AS name, ...))`. The pattern and expressions are Cypher (with GQL's `IS` labels). Behaviour to know:

- Integers, floats, strings and booleans come back as SQL values. Nodes, relationships, paths, lists, maps and temporal values come back as JSON or text; mapped graph properties keep their column values.
- Conditions in the outer `WHERE` or join on a graph column (`=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN` with integers, `IS NOT NULL`) are pushed into the graph pattern. String comparisons are not pushed.
- A join between a graph column and a sharded table's distribution key reads only the shards that hold those keys.
- Prepared statements may use `?` placeholders inside and outside the call.
- One statement's graph rows are limited to 16 MB of SQL text. Past that it fails with `ER_SPRIG_GRAPH_BUDGET_EXCEEDED` (39903), unless the statement reads no distributed table, in which case the rows go through session temporary tables instead.
- `EXPLAIN FORMAT=SPRIG` shows each call's Cypher, its pushed filters, the graph's shards and the key joins. See [Query plans](../performance/query-plans.md).

Dropping the graphs leaves the mapped graph's tables in place:

```sql test
DROP GRAPH social;
DROP PROPERTY GRAPH shop_graph;
SELECT COUNT(*) AS customers FROM customers;
```

```text output
customers
3
```

Over MongoDB, `$graphLookup` does recursive lookups within collections; it doesn't read graphs. See [Documents](documents.md).

## Cypher support

SprigDB speaks Cypher 5 as Neo4j 5.26 does. A leading `CYPHER 5` (with options) is accepted; any other version fails with 39901. Syntax errors are `ER_SPRIG_GRAPH_SYNTAX` (39900) with Neo4j's `SyntaxError` status; unsupported features are `ER_SPRIG_GRAPH_UNSUPPORTED` (39901) and name the feature.

### Clauses

| Area | Supported |
|---|---|
| Reading | `MATCH`, `OPTIONAL MATCH`, `WHERE`, `WITH`, `RETURN`, `UNWIND`, `ORDER BY`, `SKIP`, `LIMIT`, `DISTINCT`, `UNION`, `UNION ALL` |
| Writing | `CREATE`, `MERGE` with `ON CREATE SET` and `ON MATCH SET`, `SET` (`n.p = v`, `n = map`, `n += map`, labels), `REMOVE` (properties, labels), `DELETE`, `DETACH DELETE`, `FOREACH` |
| Subqueries | `CALL { ... }`, `CALL (a, b) { ... }`, `CALL { ... } IN TRANSACTIONS`, `EXISTS { ... }`, `COUNT { ... }`, `COLLECT { ... }` |
| Procedures | `CALL db.labels()`, `CALL db.relationshipTypes()`, `CALL tx.getMetaData()`, `CALL dbms.components()`, with `YIELD` |
| Schema | `CREATE INDEX`, `CREATE CONSTRAINT`, `DROP INDEX`, `DROP CONSTRAINT`, `SHOW INDEXES`, `SHOW CONSTRAINTS` |
| Plans | `EXPLAIN`, `PROFILE` |

`CALL { ... } IN TRANSACTIONS` supports `OF n ROWS` (default 1,000), `ON ERROR CONTINUE | BREAK | FAIL` and `REPORT STATUS AS s`. It runs only in auto-commit statements, and not after a write in the same statement. Batches run one after another, even with `CONCURRENT`. With `FAIL`, the error message ends with `(Transactions committed: n)` and the earlier batches stay committed.

```text
UNWIND $rows AS row
CALL (row) { CREATE (:Person {name: row.name}) } IN TRANSACTIONS OF 500 ROWS
```

### Patterns

- Label expressions: `:A:B`, `:A&B`, `:A|B`, `:!A`, `:%`, and the GQL form `IS A`.
- Variable-length relationships: `*`, `*n`, `*m..n`, and the quantifiers `{m,n}`, `+` and `*` after a relationship, such as `-[:KNOWS]->{1,3}`.
- `shortestPath(...)`, `allShortestPaths(...)`, `ANY SHORTEST` and `ALL SHORTEST`.
- Pattern predicates (`WHERE (a)-[:KNOWS]->(b)`), pattern comprehensions, map projections (`n {.name, .*, k: expr}`), `CASE`, list comprehensions, `reduce()`, and `all`, `any`, `none`, `single`.
- Patterns match trails: one relationship is never used twice in a match, as in Neo4j.

### Functions

| Kind | Functions |
|---|---|
| Elements and paths | `id`, `elementId`, `labels`, `type`, `properties`, `keys`, `nodes`, `relationships`, `startNode`, `endNode`, `length`, `size` |
| Lists | `head`, `last`, `tail`, `reverse`, `range`, `isEmpty`, `coalesce` |
| Conversion | `toString`, `toInteger`, `toFloat`, `toBoolean`, `valueType` |
| Maths | `abs`, `sign`, `ceil`, `floor`, `round`, `sqrt`, `exp`, `log`, `log10`, `sin`, `cos`, `tan`, `pi`, `e`, `rand` |
| Strings | `toUpper`, `toLower`, `trim`, `ltrim`, `rtrim`, `replace`, `split`, `substring`, `left`, `right` |
| Temporal | `date`, `localtime`, `time`, `localdatetime`, `datetime`, `duration`, each with `.realtime`, `.statement`, `.transaction` and `.truncate`; `datetime.fromEpoch`, `datetime.fromEpochMillis`; `duration.between`, `duration.inMonths`, `duration.inDays`, `duration.inSeconds`; `timestamp` |
| Spatial | `point`, `point.distance`, `point.withinBBox` |
| Aggregates | `count`, `sum`, `avg`, `min`, `max`, `collect`, `stDev`, `stDevP`, `percentileDisc`, `percentileCont` |

Temporal values take ISO text, maps or other temporal values, and support accessors (`d.year`, `dt.epochMillis`), arithmetic and comparison. Points use SRIDs 7203, 9157, 4326 and 4979. `point.distance` is Euclidean, or haversine for geographic points.

### Indexes and constraints

Create indexes and uniqueness constraints in Cypher, on node labels or relationship types:

```text
CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name)
CREATE TEXT INDEX FOR (p:Product) ON (p.description)
CREATE INDEX FOR ()-[k:KNOWS]-() ON (k.since)
CREATE CONSTRAINT person_email FOR (p:Person) REQUIRE p.email IS UNIQUE
SHOW INDEXES YIELD name, type, labelsOrTypes, properties
DROP CONSTRAINT person_email
```

- Index kinds are range (the default) and text. Each indexed property becomes a virtual column of the node or relationship table with a MariaDB index, so equality and `IN` on an indexed property read the index instead of scanning a label.
- Uniqueness holds across all of the graph's shards. A duplicate fails with MySQL error 1062, which Bolt reports as `Neo.ClientError.Schema.ConstraintValidationFailed`. A `MERGE` that races another on a constrained key matches the winner's node rather than creating a duplicate.
- `CREATE CONSTRAINT` fails if existing data already has duplicates.
- A name clash, an equivalent index, or an index and a constraint on the same label and properties fail with 39908 and Neo4j's status code.
- Index commands need the `INDEX` privilege. In an explicit transaction, an index command must be the first statement.

Neo4j's two built-in lookup indexes appear in `SHOW INDEXES`; SprigDB's label and relationship tables serve the same scans.

### GQL

GQL's forms are accepted in every statement, over Bolt and REST: `IS` labels, `FILTER`, `LET`, `FOR ... IN`, `INSERT`, `NEXT`, `OFFSET` and `MATCH TRAIL`. They run on the same engine as Cypher:

```text
MATCH (c IS Person)-[o IS BOUGHT]->(p IS Product)
FILTER o.qty >= 2
LET t = o.qty * p.price
RETURN c.name AS n, t ORDER BY n, t
```

## Variable-length paths and shortest paths

When a variable-length pattern only needs the nodes it reaches (no path or relationship variable, a minimum of 0 or 1, a direction, and a `DISTINCT` result or `count(DISTINCT ...)`, `min` or `max`), SprigDB runs it inside MariaDB as a recursive CTE, a few hops at a time (`graph.local_burst_hops`, 4), and continues across shards from the gateway. Otherwise the gateway enumerates trails level by level. Both give the same answer:

```text
// Endpoints only: runs as recursive SQL on each shard
MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(x) RETURN DISTINCT x.name

// Whole paths: enumerated by the gateway
MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*1..4]->(x) RETURN x.name, length(p)
```

Shortest paths between two bound nodes use a breadth-first search from both ends:

```text
MATCH (a:Person {name: 'Alice'}), (d:Person {name: 'Dave'})
MATCH p = shortestPath((a)-[:KNOWS*]-(d))
RETURN length(p), [n IN nodes(p) | n.name]
```

An unbounded `*` is exact or an error, never truncated: it ends when no new node is reachable, and fails with 39903 if there is still one to reach at `graph.max_hops` (15). An explicit upper bound above `graph.max_hops` also fails. Raise the setting when your data needs deeper searches.

A typical recommendation query, friends of friends who bought something you haven't:

```text
MATCH (a:Person {email: $email})-[:KNOWS*1..2]->(f:Person)-[:BOUGHT]->(p:Product)
WHERE NOT (a)-[:BOUGHT]->(p) AND f <> a
RETURN p.name AS product, count(DISTINCT f) AS buyers
ORDER BY buyers DESC, product LIMIT 5
```

## Sharding

A native graph is a sharded table group, so it splits, merges and moves between clusters like any other group, online. See [Shards and table groups](../concepts/shards-and-table-groups.md).

- Each node's shard is chosen by a hash of its id. A relationship is stored on its source node's shard, so outgoing hops read one shard per node.
- Incoming hops read the **mirror** of each relationship on its destination's shard. By default (`graph.mirror = async`) a background applier keeps mirrors from the change feed. A read waits up to `graph.mirror_max_wait_ms` (1,000 ms) for the mirrors to include everything the session has committed; if they don't, the read scans every shard instead. A lagging mirror makes a read slower, never wrong. With `graph.mirror = sync`, writers maintain mirrors in their own transaction.
- Writes, the commit check and `DETACH DELETE` always use exact reads.
- Every read step is planned by the same router as SQL. `EXPLAIN` and `PROFILE` list the graph's shards, clusters and the statements each step ran.
- `elementId()` (`n:<graph>:<hash>:<id>`) carries the node's routing hash, so lookups by element id go straight to one shard.

Graphs aren't partitioned by property or community yet; placement is by id hash. `ALTER GRAPH` fails with 39901. `sprigctl graph partition <tenant> <db.graph>` reports the share of relationships whose ends are on different shards.

## Privileges

A graph has the table privileges:

| Statement | Needs, on the graph or its database |
|---|---|
| Reads | `SELECT` |
| `CREATE`, `MERGE` | `INSERT` |
| `SET`, `REMOVE` | `UPDATE` |
| `DELETE`, `DETACH DELETE` | `DELETE` |
| Index and constraint commands | `INDEX` |

You can also grant reads and writes on single labels and relationship types, in Neo4j's form:

```sql
GRANT READ ON GRAPH shop.social NODES Person TO analyst;
GRANT READ ON GRAPH shop.social RELATIONSHIPS KNOWS TO analyst;
GRANT CREATE, DELETE, SET PROPERTY {*} ON GRAPH shop.social NODES Person TO writer;
REVOKE READ ON GRAPH shop.social NODES Person FROM analyst;
```

Read privileges are `READ`, `MATCH`, `TRAVERSE`, `SELECT` and `ALL`; write privileges are `CREATE`, `DELETE`, `SET PROPERTY {*}`, `MERGE` and `WRITE`. Granting them needs the graph-wide privilege `WITH GRANT OPTION`. A user with only element grants sees only the nodes with a readable label and the relationships with a readable type whose ends are both visible; `db.labels()` lists only readable labels. A refused write fails with MySQL error 1142 (`Neo.ClientError.Security.Forbidden`) and rolls the transaction back. See [Grants and roles](../security/grants-and-roles.md).

## Limits

Each graph statement runs within budgets. Exceeding one fails the statement with `ER_SPRIG_GRAPH_BUDGET_EXCEEDED` (39903), with the budget's name in the message (`graph budget exceeded (hops): ...`).

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `graph.max_hops` | int | `15` | Most hops of any expansion, unbounded patterns included |
| `graph.max_frontier` | int | `100000` | Nodes or paths in one level of an expansion |
| `graph.max_rows` | int | `1000000` | Rows or paths one pattern may produce |
| `graph.max_time_ms` | int | `30000` | Wall time of one statement, capped by the transaction's `tx_timeout` |
| `graph.max_memory_mb` | int | `256` | Gateway memory of one statement |
| `graph.max_detach_edges` | int | `100000` | Relationships one `DETACH DELETE` may remove; more fails with `ER_SPRIG_GRAPH_DETACH_TOO_LARGE` (39907) |
| `graph.batch_nodes` | int | `1000` | Frontier ids per shard statement |
| `graph.local_burst_hops` | int | `4` | Hops one recursive CTE runs on a shard |
| `graph.mirror` | text | `async` | Mirrors of incoming relationships: `async` or `sync` |
| `graph.mirror_max_wait_ms` | int | `1000` | Wait for mirrors before scanning every shard |
| `bolt.max_buffered_result_mb` | int | `64` | Size of a Bolt result that can't stream; more fails with 39918 |

Most of these can be set per graph, database or tenant. See [Settings and scopes](../concepts/settings-and-scopes.md) and the [settings reference](../reference/settings.md).

### Errors

| Code | Name | When |
|---|---|---|
| 39900 | `ER_SPRIG_GRAPH_SYNTAX` | A syntax or semantic error in Cypher or graph DDL |
| 39901 | `ER_SPRIG_GRAPH_UNSUPPORTED` | A feature SprigDB doesn't support; the message names it |
| 39902 | `ER_SPRIG_GRAPH_NOT_FOUND` | The graph doesn't exist |
| 39903 | `ER_SPRIG_GRAPH_BUDGET_EXCEEDED` | A budget above, or `GRAPH_TABLE` rows past 16 MB |
| 39904 | `ER_SPRIG_GRAPH_READ_ONLY_SOURCE` | A write to a mapped graph |
| 39906 | `ER_SPRIG_GRAPH_NODE_HAS_RELATIONSHIPS` | A deleted node still had relationships at commit |
| 39907 | `ER_SPRIG_GRAPH_DETACH_TOO_LARGE` | `DETACH DELETE` past `graph.max_detach_edges` |
| 39908 | `ER_SPRIG_GRAPH_DEFINITION_INVALID` | A bad mapped graph definition, or an index or constraint clash |
| 39909 | `ER_SPRIG_GRAPH_PROPERTY_TYPE` | A type error, such as a map as a property value or division by zero |

Over Bolt, each error carries Neo4j's status code where one fits, so drivers classify and retry as they would with Neo4j. See [Error codes](../reference/errors.md).

## Watch and manage graphs

- `sprigctl graph list <tenant>` lists a tenant's graphs with their kind, group and shard count.
- `sprigctl graph show <tenant> <db.graph>` shows labels, types, indexes, mirror state, and node, relationship, mirror and claim counts per shard.
- Bolt sessions appear in `SHOW PROCESSLIST` with their statement and graph; `KILL QUERY` stops a statement and `KILL` closes the connection.
- Metrics are under `sprig_graph_*`, for example `sprig_graph_statements_total`, `sprig_graph_budget_exceeded_total` and `sprig_graph_mirror_waits_total`. See the [metrics reference](../reference/metrics.md).

## Not supported

- `USE` inside a statement, composite databases, `LOAD CSV`, APOC and Graph Data Science procedures, and procedures other than the four listed.
- `SHORTEST k` path searches, and `WHERE` inside a node or relationship pattern.
- Quantified path patterns with a parenthesised group; quantifiers apply to one relationship.
- GQL's `WALK`, `ACYCLIC` and `SIMPLE` path modes (patterns match trails), and closed graph types.
- `CALL { } IN TRANSACTIONS ... ON ERROR RETRY` and Cypher 25 features.
- `POINT`, `FULLTEXT`, `VECTOR` and `LOOKUP` indexes, and existence, key and type constraints.
- Partitioning a graph by property or community (`ALTER GRAPH`), and `graph.mirror = off`, which behaves as `sync`.
- Writes to mapped graphs, mapped graphs over sharded tables or with composite keys, and mapped graphs over collections.
- Bolt `bearer` logins (39915) and impersonation (39916).

## Related

- [Bolt and Cypher](../connect/bolt-and-cypher.md)
- [Data models overview](overview.md)
- [Cross-model queries](cross-model-queries.md)
- [Shards and table groups](../concepts/shards-and-table-groups.md)
- [Grants and roles](../security/grants-and-roles.md)
