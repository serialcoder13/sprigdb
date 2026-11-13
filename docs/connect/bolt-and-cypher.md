---
title: Bolt and Cypher
audience: [user, dba]
sidebar:
  order: 5
---

# Bolt and Cypher

SprigDB speaks Neo4j's Bolt protocol, so Neo4j drivers and tools can run Cypher against SprigDB's graphs. Use it when your application already uses a Neo4j driver, or when a graph query is easier to write in Cypher than in SQL.

This page covers connecting. For the graph model, creating graphs, and the complete Cypher reference, see [Graphs](../data-models/graphs.md).

## Enable the listener

The Bolt listener is off by default.

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `listener.bolt.enabled` | bool | `false` | Serve Bolt |
| `listener.bolt.bind` | text | `0.0.0.0:7687` | Address and port |
| `listener.bolt.tls` | text | `required` | `required`, `optional` or `off` |
| `listener.bolt.cert` | text | `default` | Certificate bundle |

```sh
sprigctl settings set listener.bolt.enabled true --scope global --reason "Neo4j drivers"
```

Bolt-specific settings:

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `bolt.max_version` | text | `5.8` | Highest Bolt version offered, `5.0` to `5.8` |
| `bolt.server_agent` | text | `Neo4j/5.26.0` | `server` in the `HELLO` answer; drivers check the `Neo4j/` prefix |
| `bolt.advertise_address` | text | empty | Address in `ROUTE` answers; empty means the address the driver used |
| `bolt.routing_ttl_seconds` | int | `300` | Lifetime of a routing table |
| `bolt.recv_timeout_seconds` | int | `120` | Receive-timeout hint sent to drivers; idle connections close after twice this |
| `bolt.default_graph` | text | `neo4j` | Graph of requests that name no database |
| `bolt.logical_database` | text | `graph` | Database of graphs named without a database |
| `bolt.isolation` | text | `read_committed` | `read_committed` or `repeatable_read` |
| `bolt.max_buffered_result_mb` | int | `64` | Size of a result that can't stream; more fails with 39918 |

## Connect

Use the `neo4j+s://` scheme with TLS (the default), or `neo4j://` on a listener with TLS `off` or `optional`. `bolt+s://` and `bolt://` also work. Log in with the `basic` scheme. The user name names the tenant: `acme.app` is user `app` of tenant `acme`.

```sh
cypher-shell -a neo4j+s://db.example.com:7687 -u acme.app -p '<password>' -d shop.social
```

```text
neo4j@shop.social> MATCH (p:Person) RETURN p.name LIMIT 3;
```

Python:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("neo4j+s://db.example.com:7687",
                              auth=("acme.app", os.environ["DB_PASSWORD"]))
with driver.session(database="shop.social") as s:
    s.run("CREATE (:Person {name: $name})", name="Asha")
    for r in s.run("MATCH (a:Person {name: $n})-[:KNOWS]->(b) RETURN b.name AS name", n="Asha"):
        print(r["name"])
driver.close()
```

JavaScript:

```js
import neo4j from 'neo4j-driver';

const driver = neo4j.driver('neo4j+s://db.example.com:7687',
  neo4j.auth.basic('acme.app', process.env.DB_PASSWORD));
const { records } = await driver.executeQuery(
  'MATCH (p:Person) RETURN p.name AS name LIMIT 10', {}, { database: 'shop.social' });
```

Go:

```go
driver, err := neo4j.NewDriverWithContext("neo4j+s://db.example.com:7687",
    neo4j.BasicAuth("acme.app", os.Getenv("DB_PASSWORD"), ""))
res, err := neo4j.ExecuteQuery(ctx, driver,
    "MATCH (p:Person) RETURN p.name AS name", nil,
    neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase("shop.social"))
```

SprigDB's shell connects too: `sprig --protocol bolt`.

### Which graph a session uses

The driver's database name chooses the graph:

| `database` | Graph |
|---|---|
| `shop.social` | Graph `social` in database `shop` |
| `social` | Graph `social` in the database `bolt.logical_database` names (default `graph`) |
| none | Graph `bolt.default_graph` (default `neo4j`) in `bolt.logical_database` |

The default graph is created on its first write, if its database exists; reading it before then returns no rows. `USE` inside a statement isn't supported (39901): name the graph as the session's database.

## Authentication

| Scheme | Supported | Notes |
|---|---|---|
| `basic` | yes | The same accounts and passwords as on every other protocol |
| `bearer` | no | 39915 |
| `none`, `kerberos`, custom | no | 39915 |
| Impersonation (`imp_user`) | no | 39916 |

The user name must contain the tenant (`acme.app`), unless you connect over TLS to a tenant endpoint: a host name such as `acme.db.example.com` under `listener.tenant_sni_suffixes`, where you log in as plain `app` (see [Tenant endpoints by server name](../security/tls.md#tenant-endpoints-by-server-name)). Otherwise a plain user name fails with 39915 and the message `name the tenant: log in as <tenant>.<user>, or connect with the tenant's TLS server name`.

Accounts with a client-certificate requirement (`REQUIRE X509`, `SUBJECT` or `ISSUER`) can't log in over Bolt.

A wrong password, an unknown user or tenant, a locked account or any other refused login fails with `Neo.ClientError.Security.Unauthorized`:

```text
The client is unauthorized due to authentication failure (bad_credentials: Access denied for user ...)
```

The login runs the same checks as on the other protocols: account locks, expired passwords, `REQUIRE SSL`, `auth.require_tls`, failure delays and the per-address limit. See [Accounts and logins](../security/accounts-and-logins.md).

## Protocol support

| Feature | Support | Notes |
|---|---|---|
| Bolt 5.0 to 5.8 | yes | Up to `bolt.max_version`. The 5.8 drivers' manifest proposal is skipped and a listed version is chosen |
| Bolt 4.x and earlier | no | The handshake answers `00 00 00 00`; use a 5.x or 6.x driver |
| `HELLO`, `LOGON`, `LOGOFF` | yes | Re-authentication on a connection with `LOGOFF` and `LOGON` (5.1 and later) |
| `RUN`, `PULL`, `DISCARD` | yes | `PULL {n}` streams with back-pressure; several open results in one transaction (`qid`) |
| `BEGIN`, `COMMIT`, `ROLLBACK` | yes | With `tx_timeout` and `tx_metadata` |
| `RESET`, `GOODBYE` | yes | |
| `ROUTE` | yes | One address for routing, reads and writes; see below |
| `TELEMETRY` | accepted | Ignored |
| Bookmarks | yes | Causal consistency across sessions and gateways |
| Access mode `READ` | yes | Reads may go to a replica that has caught up with your bookmarks |
| Notifications and GQL statuses in summaries | yes | |
| Message size | 16 MiB | Largest message accepted |

### Routing

The `neo4j://` and `neo4j+s://` schemes ask for a routing table. Every SprigDB gateway can serve every request, so the answer lists one address for routers, readers and writers: `bolt.advertise_address`, or the address the driver connected to. Put the gateways behind a load balancer and give drivers its address. The table lives for `bolt.routing_ttl_seconds`.

### Bookmarks and replica reads

After a transaction that wrote, SprigDB returns a signed bookmark (starting `SB1:`) that records the commit's position in replication. Pass it to a later session, on any gateway, and that session sees the write. Drivers do this for you within a driver instance.

A read in `READ` access mode goes to a replica only after the replica has applied the session's bookmarks, waiting up to `reads.gtid_wait_timeout_ms`; otherwise it runs on the writer. Writes, and reads in `WRITE` mode, always use the writer. A bookmark that is malformed or belongs to another tenant fails with 39917 `Neo.ClientError.Transaction.InvalidBookmark`.

### Transactions

- An auto-commit `RUN` is one MariaDB transaction. An explicit transaction holds its connections until `COMMIT` or `ROLLBACK`.
- Transactions run at `READ COMMITTED` by default, Neo4j's level; `bolt.isolation` can make it `REPEATABLE READ`.
- A transaction that writes to several clusters commits with two-phase commit.
- Deleting a node that still has relationships fails at commit with 39906. Use `DETACH DELETE`.
- If a shard moves during an explicit transaction, it fails with a transient error (`Neo.TransientError.General.DatabaseUnavailable`), which drivers retry in managed transactions.

### Results

Reads whose rows don't depend on each other stream to the driver as it pulls. Other results (sorted, aggregated, or writes) are computed whole first, up to `bolt.max_buffered_result_mb` (64 MiB); a larger one fails with 39918. Add `LIMIT`, or page with `SKIP` and `LIMIT`.

## Cypher support

SprigDB speaks Cypher 5 as Neo4j 5.26 does. A summary:

| Area | Support | Details |
|---|---|---|
| `MATCH`, `OPTIONAL MATCH`, `WHERE`, `WITH`, `RETURN`, `UNWIND`, `ORDER BY`, `SKIP`, `LIMIT`, `DISTINCT`, `UNION` | yes | |
| `CREATE`, `MERGE` (`ON CREATE`, `ON MATCH`), `SET`, `REMOVE`, `DELETE`, `DETACH DELETE`, `FOREACH` | yes | Writes to graphs mapped over tables aren't supported (39904) |
| Subqueries: `CALL { }`, `EXISTS { }`, `COUNT { }`, `COLLECT { }` | yes | |
| `CALL { } IN TRANSACTIONS` | yes | Auto-commit only; `ON ERROR RETRY` isn't supported |
| Variable-length paths, quantifiers on one relationship, `shortestPath`, `allShortestPaths`, `ANY SHORTEST`, `ALL SHORTEST` | yes | Bounded by `graph.max_hops` (15) |
| Quantified path patterns over a parenthesised group, `SHORTEST k` | no | 39901 |
| Pattern and list comprehensions, map projections, `CASE`, `reduce` | yes | |
| Functions: scalar, list, string, maths, temporal, spatial, aggregates | yes | |
| Procedures `db.labels`, `db.relationshipTypes`, `dbms.components`, `tx.getMetaData` | yes | No other procedures, APOC or Graph Data Science |
| `CREATE` and `DROP` `INDEX` and `CONSTRAINT`, `SHOW INDEXES`, `SHOW CONSTRAINTS` | partly | Property and text indexes, uniqueness constraints. No point, full-text, vector or lookup indexes; no existence, key or type constraints |
| `EXPLAIN`, `PROFILE` | yes | |
| Parameters | yes | All Bolt value types, including temporal and spatial values and byte arrays |
| `LOAD CSV`, `USE`, composite databases, Cypher 25 | no | 39901 |

See [Graphs](../data-models/graphs.md#cypher-support) for every clause, pattern form and function.

Every graph statement runs within budgets: `graph.max_hops`, `graph.max_frontier`, `graph.max_rows`, `graph.max_time_ms` and `graph.max_memory_mb`. Exceeding one fails with 39903 and names the budget.

## Drivers known to work

SprigDB runs the Neo4j drivers' own integration test suite (testkit) against both Neo4j 5.26 and SprigDB and compares the results:

| Driver | Result |
|---|---|
| Python driver (6.x) | Same results as Neo4j 5.26, including through `neo4j://` routing and over TLS |
| Go driver v6.4 | Same results as Neo4j 5.26 |

Other Neo4j 5.x and 6.x drivers (Java, JavaScript, .NET) and `cypher-shell` use the same Bolt 5 messages.

## Errors you may see

Each error carries a Neo4j status code, so drivers classify and retry as they would with Neo4j. SprigDB errors without a Neo4j equivalent use `Neo.ClientError.Statement.Sprig<Name>`, for example `Neo.ClientError.Statement.SprigGraphBudgetExceeded`.

| Neo4j status | SprigDB code | When |
|---|---|---|
| `Neo.ClientError.Security.Unauthorized` | 39915, or a refused login | A scheme other than `basic`, no tenant in the user name, or wrong credentials |
| `Neo.ClientError.Security.Forbidden` | 39916, or a missing privilege | Impersonation, or your grants don't cover the graph's tables |
| `Neo.ClientError.Statement.SyntaxError` | 39900 | A Cypher syntax error |
| `Neo.ClientError.Statement.SprigGraphUnsupported` | 39901 | A feature SprigDB doesn't support; the message names it |
| `Neo.ClientError.Database.DatabaseNotFound` | 39902 | The graph doesn't exist |
| `Neo.ClientError.Statement.SprigGraphBudgetExceeded` | 39903 | A graph budget ran out |
| `Neo.ClientError.Schema.ConstraintValidationFailed` | 39906, or a duplicate key | A deleted node kept relationships, or a uniqueness constraint failed |
| `Neo.ClientError.Statement.TypeError` | 39909 | A type error, such as a map as a property value |
| `Neo.ClientError.Transaction.InvalidBookmark` | 39917 | A malformed or foreign bookmark |
| `Neo.ClientError.Statement.SprigBoltResultBufferLimit` | 39918 | A result larger than `bolt.max_buffered_result_mb` that can't stream |
| `Neo.ClientError.Request.Invalid` | 39914 | A Bolt protocol error |
| `Neo.TransientError.Transaction.DeadlockDetected` | MariaDB 1213 or 1205 | A deadlock or lock wait timeout; retry |
| `Neo.TransientError.Transaction.Terminated` | MariaDB 1317 | The statement was killed |
| `Neo.TransientError.General.DatabaseUnavailable` | 39000, 39424 and others | The gateway is draining or a shard moved; retry |

A driver that offers only Bolt 4 or earlier gets no common version: SprigDB answers `00 00 00 00` and closes the connection, and the driver reports that the server doesn't support its protocol versions.

## Related

- [Graphs](../data-models/graphs.md)
- [Connect to SprigDB](overview.md)
- [Read routing](../performance/read-routing.md)
- [Accounts and logins](../security/accounts-and-logins.md)
- [Connection problems](../troubleshooting/connections.md)
