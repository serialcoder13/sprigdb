---
title: Data inheritance
audience: [user, dba]
sidebar:
  order: 13
---

# Data inheritance

Data inheritance lets a child in a tree, such as a customer's tenant under a platform or a branch under a head office, see a subset of its parent's data and keep receiving the parent's changes, while its own changes and additions stay its own. Use it for shared catalogues, price lists, templates and reference data that every child needs and some children adjust.

SprigDB inherits two things down the tenant tree:

- **Data.** A parent shares rows of a table. Each child reads its parent's shared rows merged with its own **overrides**, **additions** and **hidden** rows; the nearest level wins.
- **Definitions.** A parent declares a stream, exchange, queue, binding, trigger, connection or sink once `FOR EACH TENANT`, and every descendant gets its own **instance**, with its own data. See [Inherited definitions](#inherited-definitions-for-each-tenant).

## Spaces and the tenant tree

Inheritance follows a tree of **spaces**. Every tenant is a space, named after the tenant, and its place in the [tenant tree](../concepts/tenants-and-databases.md) is its place in the space tree: a child tenant's root space hangs below its parent tenant's root space. Inside one tenant you can add more spaces below its root, for branches or regions that share the tenant's databases:

```text
platform                 tenant (root space "platform")
├── acme                 child tenant
│   ├── retail           space inside acme
│   │   └── pune         space inside acme, below retail
│   └── acme-eu          child tenant of acme
└── globex               child tenant
```

```sql
CREATE SPACE retail;                 -- below the session's space
CREATE SPACE pune UNDER retail;
SHOW SPACES;
USE SPACE pune;                      -- or SET @sprig_space = 'pune'
SET @sprig_space = DEFAULT;          -- back to your home space
ALTER SPACE pune SET HOME FOR clerk_pune;
DROP SPACE pune;
DROP SPACE retail CASCADE;
```

- A session works in its user's **home space**, by default the tenant's root space. `USE SPACE` moves it to that space or a space below it; a space above or beside the home space fails with `ER_SPRIG_SPACE_ACCESS_DENIED` (39884). `ALTER SPACE s SET HOME FOR user, ...` sets users' home spaces, so a branch's users never see more than their branch.
- `CREATE SPACE s [UNDER p]` creates `s` below `p`, or below the session's space; `p` must be at or below the session's space. Space names are unique in the tenant and can't be the tenant's name (`ER_SPRIG_SPACE_EXISTS`, 39881).
- `DROP SPACE [IF EXISTS] s [CASCADE]` drops a space below the session's space. Without `CASCADE`, a space with child spaces or with rows of its own fails with `ER_SPRIG_SPACE_HAS_CHILDREN` (39882); `CASCADE` drops the child spaces and deletes their rows. The root space can't be dropped.
- `SHOW SPACES` lists `Space`, `Parent`, `Depth` and `Current`.
- A space's depth counts its tenant's ancestors too, and can't exceed `tenant.max_depth` (64; 39883).
- Space statements need `CREATE USER` or `SPRIG_TENANT_ADMIN` (1227 otherwise).

Tenants themselves are created with `sprigctl` or the Admin API, not SQL; see [Tenant administration](../security/tenant-administration.md).

## Make a table inheritable

A table becomes inheritable with an identity key, which identifies one record across every level. The key must be the table's primary key:

```sql test
CREATE TABLE products (
  id     INT PRIMARY KEY,
  name   VARCHAR(40) NOT NULL,
  colour VARCHAR(20) NULL,
  price  DECIMAL(10,2) NULL,
  region CHAR(2) NULL
);
ALTER TABLE products SPRIG INHERITABLE KEY (id);
INSERT INTO products VALUES
  (1, 'P1', 'red',   10, 'IN'),
  (2, 'P2', 'blue',  20, 'IN'),
  (3, 'P3', 'green', 30, 'US');
```

```sql test
SELECT id, name, colour FROM products ORDER BY id;
```

```text output
id	name	colour
1	P1	red
2	P2	blue
3	P3	green
```

- A key other than the primary key fails with `ER_SPRIG_INHERIT_STATEMENT_UNSUPPORTED` (39891), as does making a table inheritable twice.
- Every `UNIQUE` key of the table becomes unique per level, so a child can hold its own row with a value its parent also uses. Uniqueness across the merged view isn't enforced.
- `ALTER TABLE t DROP SPRIG INHERITABLE` makes it an ordinary table again. It fails with 39891 while other spaces hold rows in it (`SPRIG RESET` them or `DROP SPACE ... CASCADE` first), and with `ER_SPRIG_INHERITED_OBJECT_IN_USE` (39895) while a share reaches it.
- Making a table inheritable needs `ALTER` on it. MariaDB copies the table once to add the layer columns.

### How it is stored

An inheritable table is one MariaDB table holding every level's rows. Three invisible columns tell the levels apart:

| Column | Meaning |
|---|---|
| `_sprig_space` | The space the row belongs to |
| `_sprig_state` | `own` (an addition or the owner's record), `ovr` (an override of an inherited record), `hide` (an inherited record this space deleted) or `orphan` (an override whose parent record is gone) |
| `_sprig_ovr` | For an override, which fields it sets; the others come from above |

The primary key becomes `(key, _sprig_space)`. Clients never see these columns: `SELECT *`, `SHOW CREATE TABLE`, `SHOW COLUMNS`, `information_schema.COLUMNS` and dump tools show the table as you created it, and a dump of a space restores anywhere as a plain table. Schema tools such as Flyway and Hibernate work unchanged.

## Share rows with children

A table is private until a space shares it. Run `CREATE SHARE` in the parent space:

```sql
CREATE SHARE in_products ON products TO SPACE retail
  WHERE region = 'IN' WITH RESHARE;

USE SPACE retail;
CREATE SHARE pune_products ON products TO SPACE pune;
```

The full grammar:

```sql
CREATE SHARE name ON [db.]table
  TO SPACE child | TO ALL CHILDREN
  [WHERE condition]
  [COLUMNS (c1, c2, ...)]
  [WITH RESHARE]

ALTER SHARE name WHERE condition        -- in the parent: widen or narrow
ALTER SHARE name TAKE WHERE condition   -- in the child: narrow what it takes
ALTER SHARE name TAKE ALL               -- in the child: take everything again
DROP SHARE [IF EXISTS] name
```

| Clause | Meaning |
|---|---|
| `TO SPACE child` | A child space of the session's space. From a tenant's root space, `child` may also name a **child tenant** |
| `TO ALL CHILDREN` | Every child, now and in future. From a root space, this includes child tenants |
| `WHERE condition` | Only rows matching it. The condition must be deterministic (no `RAND()`, `NOW()`, `USER()`, variables and the like), have no subquery, and name only shared columns (`ER_SPRIG_SHARE_FILTER_INVALID`, 39888) |
| `COLUMNS (...)` | Only these columns; the key is always included. Others come down as `NULL` |
| `WITH RESHARE` | The child may share what it receives with its own children. Without it, a child's `CREATE SHARE` of the table fails with `ER_SPRIG_SHARE_NOT_RESHARABLE` (39890) |

- Shares are created from the session's space. Share names are unique in the tenant.
- A share chain longer than `inherit.max_depth` (16) fails with 39883.
- `TAKE` narrows a share that was made to the child's space by name.
- Creating, altering and dropping shares needs `ALTER` on the table (1142 otherwise).
- A share on a table that isn't inheritable fails with `ER_SPRIG_NOT_INHERITABLE` (39885).

**Columns cut by `COLUMNS` and `NOT NULL`.** A field the share leaves out comes down as `NULL`. Overriding such a record copies that `NULL` into the child's row, which fails with 1048 when the column is `NOT NULL`. Share every column of such tables, or make the cut columns nullable.

## What a child sees

A child's view of the table is its parent's view, filtered by the share and the child's `TAKE`, with the child's own rows on top. For each record, every field comes from the nearest level that sets it:

| The child holds for a key | The child sees |
|---|---|
| Nothing | The parent's record, if the share lets it through |
| An addition (`own`) | Its own record |
| An override (`ovr`) | Its own value for the fields it set, the parent's for the rest; later parent changes to other fields still show |
| A hide marker | Nothing |
| An orphan | Its own record, as it was when the parent's record went |

A child never sees a row its share filters out, even through `WHERE`, joins or aggregates: the merged view is applied to every reference to the table, in every statement.

To see where each field came from, select `SPRIG_ORIGIN()`:

```sql
USE SPACE pune;
SELECT id, colour, SPRIG_ORIGIN() FROM products WHERE id = 2;
```

After `retail` overrode the colour of record 2, this returns `navy` and the origin `{"name": "acme", "colour": "retail", "price": "acme", "region": "acme"}`.

`SPRIG_ORIGIN()` returns a JSON object naming, for each column other than the key, the space its value came from, `'<space> (orphaned)'` for an orphan's fields, and the parent tenant's name for fields from a parent tenant. It needs exactly one inheritable table in the statement (39891 otherwise).

## Writes from children

Every write changes only the session's own space; a parent's rows are never touched. SprigDB turns each statement into writes of the space's layer:

| Statement in a child space | What happens |
|---|---|
| `INSERT` a new key | An addition (`own` row) |
| `INSERT` a key the space already sees | 1062, as for any duplicate. The message shows the key, never the space |
| `UPDATE` an inherited record | An override of the fields the `SET` names (copy on write) |
| `UPDATE` the space's own record | Updated in place |
| `DELETE` an inherited record | A hide marker; the parent's record is unchanged |
| `DELETE` the space's own record or orphan | Deleted |
| `SPRIG RESET t WHERE ...` | Drops the space's overrides, hide markers and orphans for those keys, so the inherited record shows again |
| `SPRIG RESET t (c1, ...) WHERE ...` | Drops the override of those fields only |

```sql
USE SPACE retail;
UPDATE products SET colour = 'navy' WHERE id = 2;     -- override one field
INSERT INTO products VALUES (9, 'P9', 'black', 15, 'IN');  -- retail's own record
USE SPACE pune;
DELETE FROM products WHERE id = 1;                     -- hidden in pune only
SPRIG RESET products WHERE id = 1;                     -- shown again
```

- **Field or whole-record overrides.** With `inherit.override = 'field'` (default), an override holds only the fields it sets, and the parent's later changes to other fields still show. With `'record'`, an update copies the whole record, so the parent's later changes no longer show in it.
- **The key can't change.** An `UPDATE` that changes an inherited record's key fails with `ER_SPRIG_INHERIT_KEY_UPDATE` (39886).
- `INSERT IGNORE` skips keys the space already sees, with a warning each. `INSERT ... ON DUPLICATE KEY UPDATE` works on the space's own records; for an inherited one, use `UPDATE` (39891).
- A `SET` that reads a column the same statement sets (`SET a = b, b = 1`) fails with 39891; split the statement.
- `SPRIG RESET ... WHERE` is evaluated on the space's own stored rows, so hidden records can be reset too.
- Affected-row counts and info lines follow MySQL. A multi-statement plan runs in one transaction, or under a savepoint inside yours.
- Writes need the privileges of the statements they become: an `UPDATE` of an inherited record needs `SELECT`, `INSERT` and `UPDATE` on the table.

### When a parent deletes a record

| `inherit.parent_delete` | A child's override of the deleted record |
|---|---|
| `keep` (default) | Becomes an **orphan**: the child keeps it as its own record, and `SPRIG_ORIGIN()` marks it `(orphaned)` |
| `follow` | Is deleted with the parent's record |

Inside one tenant this happens in the deleting transaction. Across tenants it happens a moment later, in a background worker, and always keeps. A record that merely leaves the share's filter isn't orphaned; it just stops showing.

## Child tenants

A share from a tenant's root space to a child tenant (`TO SPACE acme-eu`, or `TO ALL CHILDREN`) works like a share between spaces, with these additions.

**Children receive the tables.** Before a child tenant's next statement, SprigDB creates in the child any database and table its parents share with it and it lacks, with the parent's definition and key, and adds columns the parent added. You don't run DDL in the child. A share made while a client is connected reaches it at its next statement, including a `USE`, the connection's default database and a prepared statement.

**Children may add, not remove.** A child can add its own columns and tables, which reach its own children the same way. It can't drop, rename or destructively alter what it inherits (`ER_SPRIG_INHERIT_NOT_OVERRIDABLE`, 39897). A parent's new column or table whose name a descendant already uses fails with 39897.

**Schema changes while shared.** While a share reaches a table, a parent's destructive change (`DROP TABLE`, `RENAME TABLE`, `TRUNCATE`, `DROP DATABASE`, an `ALTER TABLE` that drops or narrows a column) fails with `ER_SPRIG_INHERITED_OBJECT_IN_USE` (39895), naming the share. Additive changes pass, and widened types (a longer `VARCHAR`, `INT` to `BIGINT`, new `ENUM` members at the end, a column made nullable) reach children at their next statement. To drop a shared database, drop its shares first.

**Dropping a share.** When a parent drops a share to a child tenant, the child keeps its overrides as its own records (orphans), its hide markers go, and the parent's records leave its view. The child's database stays, with its own rows.

### Foreign keys and groups

Tables linked by foreign keys inherit together. A table's **group** is every inheritable table linked to it by foreign keys, in either direction, in the same database, and a share of any table of a group shares the whole group, with no filter on the other tables.

- `ALTER TABLE t SPRIG INHERITABLE` turns the table's foreign keys into links that SprigDB checks itself, and makes the tables at their other ends inheritable with their primary keys.
- A `CREATE TABLE` with a foreign key onto an inheritable table creates an inheritable table.
- A link must reference the other table's whole key in the same database (39891). `ON DELETE CASCADE`, `SET NULL` and `SET DEFAULT` fail with `ER_SPRIG_FK_ACTION_NOT_DISTRIBUTED` (39462).
- Adding a foreign key checks existing rows in every space (1452). Adding or dropping one while either table is shared fails with 39895.
- A row that references a key not in the writer's view fails with 1452. Deleting or hiding a record that any descendant still references, at any level, fails with 1451; the message names the tenant to a tenant administrator, and says "a descendant tenant" otherwise.

`SPRIG CHECK REFERENCES t [WHERE e]` lists, for up to 1,000 rows of your view, what would refuse their delete: columns `Key`, `Tenant`, `Space`, `Table`, `Row`, `Link` and `Kind` (`reference`, or `orphaning in progress` while a background worker is still turning a parent's deletion into orphans).

### One id sequence per table

In an inheritable table with an `AUTO_INCREMENT` column, every tenant of the tree takes generated values from one sequence, kept by the tree's top tenant, so a child's new row never gets an id its parent already uses. An explicit value moves the sequence past it; one inside a block another gateway still holds fails with `ER_SPRIG_ID_VALUE_RESERVED` (39463). `SPRIG CHECK IDS t` lists (`Id`, `Tenants`) ids that more than one tenant holds as its own, which can exist from before the tables were linked; SprigDB never renumbers them.

### Typed tables

A [typed table](../sql/inheritance.md) (`INHERITS`) and its subtypes inherit as one group. Children receive the types with the tables and may declare subtypes of their own. A row's type is computed with the viewing tenant's types, so a value only a child declares as a subtype shows as that subtype in the child and as the parent type above it. `ADD | DROP DISCRIMINATOR` runs only in the tenant at the top of the tree (39897 below), and children follow at their next statement.

## Read-through and materialized

SprigDB builds each child's merged view in one of two ways, chosen per share when it is created:

| Mode | How | When |
|---|---|---|
| Read-through | MariaDB computes the view at query time from the levels' rows | Levels in one tenant, or tenants on the same cluster and unsharded |
| Materialized | A background worker keeps a copy of the parent's shared rows in the child's schema, from the [change feed](../concepts/change-feed.md); the child reads the copy | Long chains, and child tenants on other clusters |

`inherit.merge` decides: `auto` (default) keeps a share read-through unless it would make more than `inherit.read_through_max_depth` (4) read-through levels since the nearest materialized one, and then materializes it; `materialized` always copies; `read_through` refuses the deep case (39883); `federated` refuses new shares (`ER_SPRIG_SHARE_RESIDENCY_CONFLICT`, 39889). A share keeps the mode it was created with.

A materialized copy changes a few tens of milliseconds after its source, typically. While a copy is still being filled, or after a schema or filter change that needs a refill, the share is read through instead when the tenants are on one cluster. Across clusters, read-through isn't possible (39889), so the child waits for its copy. `SHOW SHARES` shows each copy's state (`seeding` or `serving`) and lag.

## Inspect

| Statement | Shows |
|---|---|
| `SHOW SPACES` | `Space`, `Parent`, `Depth`, `Current` |
| `SHOW SHARES` | `Share`, `Role`, `Object`, `From`, `To`, `Filter`, `Take`, `Columns`, `Reshare`, `Mode`, `Copy`, `Lag_ms` |
| `SHOW CREATE SHARE name` | `Share`, `Create Share`, `From`, `Mode`, `Group` (39887 for an unknown share) |
| `SHOW INHERITABLE TABLES` | `Table`, `Key`, `Origin` (the tenant at the top of its tree), `Levels_up`, `Own_columns` (columns this tenant added), `Group`, `Shares` |
| `SPRIG CHECK IDS t` | Ids held by more than one tenant of the tree |
| `SPRIG CHECK REFERENCES t [WHERE e]` | What refuses a delete of those rows |
| `EXPLAIN SELECT ...` | The merged view's plan; in a tenant that reads its parent through a share, MariaDB refuses it (1345) |

```sql test
SHOW INHERITABLE TABLES;
SELECT id, SPRIG_ORIGIN() FROM products WHERE id = 1;
```

`sprigctl space` (`list`, `show`, `create`, `share`, `unshare`) and `sprigctl inherit` (`check`, `tables`, `share`, `blockers`) do the same from the command line; see [sprigctl](../tools/sprigctl.md).

## Statements refused on inheritable tables

These fail with `ER_SPRIG_INHERIT_STATEMENT_UNSUPPORTED` (39891) when they name an inheritable table:

- `INSERT ... SELECT` into it, and `INSERT` without `VALUES`;
- `REPLACE` from a space that inherits it;
- `LOAD DATA` into it (use `INSERT`);
- multi-table `UPDATE` and `DELETE` of it;
- views, routines, triggers, `CREATE TABLE ... LIKE | SELECT` and `CREATE UNIQUE INDEX` naming it;
- prepared statements naming it: send the statement as text;
- `TRUNCATE` from a space that inherits it, or of a table other tables reference (use `DELETE`, which checks references);
- a table with a record hierarchy can't be made inheritable.

```sql test-error
CREATE VIEW cheap_products AS SELECT id, name FROM products WHERE price < 50;
```

## Inherited definitions (`FOR EACH TENANT`)

Some objects should exist once per tenant without anyone creating them: an event stream every customer tenant publishes into, a queue of jobs per tenant, a webhook per tenant. Declare the object once in a parent tenant `FOR EACH TENANT`, and every descendant, now and in future, gets its own **instance**: its own records or messages, positions and consumers, stored in that tenant's shards in its home data centre.

```sql
-- In the platform tenant, database security:
CREATE STREAM user_events FOR EACH TENANT
  KEY (user_id) PARTITIONS 6 RETENTION '7 days';
CREATE EXCHANGE jobs TYPE topic FOR EACH TENANT;
CREATE QUEUE welcome_mail FOR EACH TENANT WITH (x_queue_type = 'quorum');
BIND QUEUE welcome_mail TO EXCHANGE jobs KEY 'mail.*' FOR EACH TENANT;
CREATE TRIGGER sync_users ON STREAM user_events
  FOR EACH TENANT GRANTS (INSERT, UPDATE, SELECT ON app_a.users)
  LANGUAGE javascript AS $$
export default function (ctx) {
  const e = JSON.parse(ctx.record.value);
  db.exec('INSERT INTO app_a.users (id, email) VALUES (?, ?) ' +
          'ON DUPLICATE KEY UPDATE email = VALUES(email)', [e.user_id, e.email]);
}
$$;

-- Publish an event into one customer's instance:
INSERT INTO user_events FOR TENANT acme (key, value)
  VALUES (901, JSON_OBJECT('type', 'user.added', 'user_id', 901, 'email', 'a@acme.example'));
```

### The clause

```sql
FOR EACH TENANT [CHILDREN | DESCENDANTS] [UNDER tenant] [INCLUDING SELF]
                [GRANTS (privilege [, ...] ON db.object [, ...])]
```

| Part | Meaning |
|---|---|
| `DESCENDANTS` (default) | Every descendant at any depth is a **member** |
| `CHILDREN` | Direct children only |
| `UNDER t` | Only tenants in `t`'s subtree |
| `INCLUDING SELF` | The declaring tenant gets an instance too. Without it, the name in the declarer is the definition, which takes publishes only with a target tenant and reads only as fan-in |
| `GRANTS (...)` | Triggers and sinks: what the instance may do in each member. Needs `SPRIG_TENANT_ADMIN` (1227 otherwise) |

| Kind | Statement |
|---|---|
| Stream | `CREATE STREAM s FOR EACH TENANT ...`; see [Event streams](streams.md) |
| Exchange, queue | `CREATE EXCHANGE e TYPE t FOR EACH TENANT`, `CREATE QUEUE q FOR EACH TENANT ...`; see [Queues](queues.md) |
| Binding | `BIND QUEUE \| EXCHANGE d TO EXCHANGE e [KEY 'k'] FOR EACH TENANT`, both ends definitions of the same database |
| Trigger | `CREATE [OR REPLACE] TRIGGER g ON STREAM \| QUEUE s FOR EACH TENANT [GRANTS (...)] ...`; see [Logic](logic.md) |
| Connection, sink | `CREATE CONNECTION c FOR EACH TENANT ...`, `CREATE SINK k FOR EACH TENANT ...`; see [Sinks](sinks.md) |

A trigger or sink definition must read a stream or queue that is itself `FOR EACH TENANT`. Creating a definition needs `CREATE` on the name in the declaring database; a declaring tenant may hold `inherit.max_definitions_per_tenant` (200) definitions (`ER_SPRIG_INHERIT_INSTANCE_QUOTA`, 39240).

### Instances are created on first use

A definition reaches every member at once, because members read their ancestors' definitions; nothing is written in a member when you declare it. A member's instance is created the first time it is used:

- a publish into it, by the member or by an ancestor for the member;
- for exchanges and queues, the member's first AMQP connection to the database's virtual host, or a publish aimed at it;
- for triggers and sinks, their source instance becoming active.

Reads never create an instance: a stream instance that doesn't exist yet reads as empty. Instances aren't created in advance. A publish waits up to `inherit.instance_create_wait_ms` (5,000 ms) for an instance being created, then fails with the retryable `ER_SPRIG_INHERIT_INSTANCE_UNAVAILABLE` (39899). A member may hold `inherit.max_instances_per_tenant` (1,000) instances (39240).

### Publishing by target tenant

Publishing goes down the tree only. A tenant may publish into its own instance and into its descendants' instances, never up and never sideways.

| Publisher | Without a target | With `FOR TENANT t` |
|---|---|---|
| The member itself | Its own instance, unless `inherit.member_publish = 'off'` | `t` must be itself |
| An ancestor that is also a member | Its own instance | `t`'s instance, if `t` is a member below it |
| The declaring tenant | Its own instance with `INCLUDING SELF`; otherwise `ER_SPRIG_INHERIT_TARGET_TENANT` (39896) | `t`'s instance, if `t` is a member |
| A sibling, a descendant or an unrelated tenant | Not visible | 39896 |

```sql
INSERT INTO user_events FOR TENANT acme (key, value) VALUES (901, '{"type": "user.added"}');
INSERT INTO user_events (tenant, key, value)
  SELECT tenant_name, user_id, payload FROM pending_events;     -- one target per row
```

- The target can also be named by the header `x-sprig-tenant` (Kafka, AMQP, `sprig.mq_publish`), the AMQP routing-key prefix `@acme:mail.welcome`, and `{tenant: 'acme'}` in logic's `db.publish`.
- Every record or message in an instance carries `x-sprig-tenant` (the instance's tenant) and `x-sprig-origin-tenant` (the publisher), set by the server over anything the client sent.
- Publishing needs `INSERT` on the name. A sibling, a non-member, an unknown name and a tenant outside the publisher's subtree all give 39896, so the error doesn't reveal whether a tenant exists.
- **Inside a transaction**, a publish into another tenant's instance is written to an outbox in the publisher's own shard and forwarded after commit, exactly once. A rolled-back transaction publishes nothing. `DROP DATABASE` waits up to `inherit.outbox_drain_seconds` (30) for its outbox to empty, then fails with the retryable `ER_SPRIG_INHERIT_OUTBOX_PENDING` (39245).
- **Residency.** A record an ancestor publishes is the ancestor's data until it lands. With the platform setting `residency.cross_tenant_messaging_strict = on`, a transactional publish into a member whose residency policy doesn't allow the publisher's data centre fails with `ER_SPRIG_INHERIT_RESIDENCY` (39243).

### Broadcast

An ancestor can publish once for every member:

```sql
INSERT INTO user_events FOR ALL TENANTS (key, value)
  VALUES ('maintenance', JSON_OBJECT('type', 'notice', 'at', '2026-11-20T02:00Z'));
INSERT INTO user_events FOR ALL TENANTS WITH INSTANCE UNDER acme (key, value)
  VALUES ('notice', '{}');
SHOW INHERITED BROADCASTS FOR STREAM user_events;
```

- `FOR ALL TENANTS` reaches every member that exists when the broadcast commits, creating missing instances. `WITH INSTANCE` reaches only members whose instance already exists. `UNDER t` limits it to `t`'s subtree. Over AMQP and Kafka, the target `*` does the same.
- Each member gets the record once, carrying `x-sprig-broadcast`, and receives a definition's broadcasts in order. Delivery fans out at `inherit.broadcast_rate` (2,000 members per second per data centre); a member whose delivery fails is retried with backoff without holding up the others.
- A tenant created after the broadcast gets nothing, unless the definition has `inherit.broadcast_replay = 'on'`: then a new instance gets the broadcasts still in the log (`inherit.broadcast_retention_hours`, 168).
- Limits: `inherit.broadcast_max_bytes` (65,536), `inherit.broadcasts_per_hour` (60 per definition) and `inherit.broadcast_max_members` (100,000). Over any of them: `ER_SPRIG_INHERIT_BROADCAST_LIMIT` (39244).
- `SHOW INHERITED BROADCASTS` lists `Broadcast`, `Publisher`, `Committed_ms`, `Form`, `Under`, `State`, `Delivered`, `Skipped`, `Error` and `Pending`.

### Overrides: the nearest level wins

A member, or an ancestor for a member or a subtree, can change some values of its instance. Each value comes from the nearest level that sets it: the member, then each tenant between it and the declarer, then the definition.

```sql
-- In the member:
ALTER INHERITED STREAM security.user_events SPRIG SET (stream.retention_hours = 72);
ALTER INHERITED QUEUE welcome_mail SET (x_message_ttl = 60000);
ALTER INHERITED TRIGGER security.sync_users DISABLE;
ALTER INHERITED STREAM security.user_events RESET;

-- From an ancestor:
ALTER INHERITED TRIGGER security.sync_users FOR TENANT acme ENABLE;
ALTER INHERITED SINK user_hooks FOR SUBTREE acme SET (batch_max_records = 50);

SHOW INHERITED OVERRIDES FOR STREAM security.user_events;
```

```sql
ALTER INHERITED kind [db.]name [FOR TENANT t | FOR SUBTREE [t]]
  SPRIG SET (...) | SET (...) | RESET [(keys)]
  | DISABLE | ENABLE | DETACH | REPLACE LANGUAGE lang AS body
```

| Kind | What a member can change |
|---|---|
| Stream | `stream.retention_hours`, `stream.retention_bytes`, `stream.max_message_bytes`, `stream.compaction_lag_minutes`, `stream.tombstone_hours` |
| Queue | `x-message-ttl`, `x-expires`, `x-max-length`, `x-max-length-bytes`, `x-overflow`, `x-delivery-limit`, `x-dead-letter-exchange`, `x-dead-letter-routing-key`, `x-consumer-timeout` (written with `_` or `-`) |
| Trigger | Its body (`REPLACE LANGUAGE ... AS ...`); `DISABLE` and `ENABLE` |
| Binding | `DISABLE` and `ENABLE`; an AMQP `queue.unbind` in the member does the same |
| Sink | `sink.*` settings and format options; `DISABLE` and `ENABLE` |
| Connection | Options and credentials; see [Sinks](sinks.md) |

- Everything else belongs to the definition, and changing it fails with 39897: a stream's key, partitions and cleanup policy; a queue's type; an exchange's type; a binding's ends and key; a trigger's or sink's source. A member's `DROP` of an instance, and a local object with an inherited name, also fail with 39897. A member that needs something different declares its own object under another name.
- `DISABLE` stops that member's trigger or sink and keeps its position, so `ENABLE` resumes where it stopped.
- An override in the member needs `ALTER` on the name (`SPRIG_SINK_ADMIN` or `SPRIG_TENANT_ADMIN` for sinks and connections). `FOR TENANT` and `FOR SUBTREE` need `SPRIG_TENANT_ADMIN`.
- `SHOW INHERITED OVERRIDES` lists `Kind`, `Name`, `What`, `Value`, `Set_in`, `For` and `Applies_to`.

### Changing and dropping a definition

- `ALTER` of a definition in the declaring tenant reaches every instance that hasn't overridden the value: settings at once, a new trigger body as a new version, a sink's options with a restart of its tasks. A change that isn't additive (a stream's key, fewer partitions, a queue's type) fails with 39895 while instances exist.
- `DROP` of a definition starts a grace period of `inherit.drop_grace_hours` (168): publishes into it fail with `ER_SPRIG_INHERIT_DEFINITION_DROPPING` (39898), consumers, triggers and sinks drain what the instances hold, and afterwards every instance is dropped. `DROP ... IMMEDIATE` skips the grace and needs `SPRIG_TENANT_ADMIN`.
- During the grace, a member can keep its data: `ALTER INHERITED STREAM s DETACH` makes its instance an ordinary object of the member. Only the member itself can detach.
- `DROP DATABASE` of a database holding active definitions fails with 39895, naming them.

### Run-as: the member's `sprig_inherited` principal

Inherited triggers and sinks run in each member with the **member's** rights, never the declarer's. Every tenant has a built-in principal `sprig_inherited`, with no login. When a trigger or sink instance is deployed in a member, `sprig_inherited` is granted the definition's `GRANTS`.

- The member's row-level security applies to it like any user.
- A member's administrator can `REVOKE` those privileges. The instance then stops with `ER_SPRIG_INHERIT_RUN_AS_PRIVILEGE` (39242): its records wait, nothing is dead-lettered, `SHOW INHERITED INSTANCES` shows `blocked`, and the waiting records run once each when the grant returns.

### Fan-in: reading every member's instance

An ancestor can read all its members' instances together, with `SELECT` on the name and the privilege `SPRIG_FAN_IN` (granted by name; `GRANT ALL` doesn't include it; 1227 otherwise):

| How | Reads |
|---|---|
| `SELECT ... FROM s FAN IN [WHERE tenant IN (...)]` | Each member instance's newest 1,000 records as one table with a `tenant` column, for at most `inherit.fan_in_sql_max_tenants` (100) members (1104 above it) |
| Kafka | Each member instance is also the read-only topic `__t.<member>.<db>.<stream>`; subscribe with a pattern |
| AMQP | A consumer on the definition's queue with the argument `x-sprig-fan-in = true` |
| Triggers | `CREATE TRIGGER g ON STREAM s FAN IN ...` runs once per record of every member |
| Sinks | `CREATE SINK k FROM STREAM s FAN IN ...`; see [Sinks](sinks.md) |

```sql
SELECT tenant, COUNT(*) FROM security.user_events FAN IN GROUP BY tenant;
```

Every record read by fan-in carries `x-sprig-tenant`. Order is kept per member instance only. Members whose residency sets `residency.deny_remote_access` are left out and counted in `sprig_inherit_fanin_skipped_total`. Fan-in reads only instances of definitions; a tenant can never read a descendant's ordinary tables, streams or queues this way.

### Listing definitions and instances

| Statement | Shows |
|---|---|
| `SHOW INHERITED DEFINITIONS` | The tenant's definitions: `Kind`, `Database`, `Name`, `Scope`, `Version`, `State`, `Instances` |
| `SHOW INHERITED INSTANCES` | Every definition that reaches this tenant: `Kind`, `Database`, `Name`, `Inherited_from`, `Version`, `Instance`, `Instance_id`, `Binding` |
| `SHOW INHERITED OVERRIDES [FOR kind name]` | Overrides with the level that set them |
| `SHOW INHERITED BROADCASTS FOR kind name` | A definition's broadcast log |

`Instance` is `none` (not created yet), `active`, `dropping`, `detached`, `blocked`, `disabled`, `inherited` (connections and bindings, which have no instance of their own), `waiting_config` (a sink whose connection still needs a `REQUIRED` value, 39241) or `error` (a sink that couldn't be deployed, for example because the member's allowlist refuses its destination; it is retried every few seconds, up to every 5 minutes).

## Settings

| Key | Default | Meaning |
|---|---|---|
| `inherit.override` | `field` | `field` or `record`: what an update of an inherited record overrides |
| `inherit.parent_delete` | `keep` | `keep` or `follow`: a child's override when the parent deletes the record |
| `inherit.merge` | `auto` | `auto`, `read_through`, `materialized` or `federated` (refused) |
| `inherit.read_through_max_depth` | 4 | Read-through levels before a share must be materialized |
| `inherit.max_depth` | 16 | Longest share chain of one table |
| `inherit.member_publish` | `on` | Whether a member may publish into its own instance |
| `inherit.instance_create_wait_ms` | 5000 | How long a publish waits for an instance being created |
| `inherit.max_definitions_per_tenant` | 200 | `FOR EACH TENANT` definitions per declaring tenant |
| `inherit.max_instances_per_tenant` | 1000 | Instances per member |
| `inherit.drop_grace_hours` | 168 | How long a dropped definition's instances drain |
| `inherit.fan_in_sql_max_tenants` | 100 | Members one fan-in `SELECT` may read |
| `inherit.outbox_drain_seconds` | 30 | How long `DROP DATABASE` waits for its cross-tenant outbox |
| `inherit.broadcast_*`, `inherit.broadcasts_per_hour` | | Broadcast limits, rate, replay and retention, above |

Set them with `SPRIG SET` at the scopes each allows; see [Settings](../sql/settings.md) and the [settings reference](../reference/settings.md).

## Limits and sharding behaviour

- **Tables only.** Inheritable objects are SQL tables. Collections and key-value namespaces can't be inheritable yet.
- **Unsharded tables for read-through across tenants.** Read-through between tenants needs both databases on the same cluster and unsharded; other placements need materialized shares (39889 otherwise).
- **Reads cost more per level.** Each read-through level adds a little to every query on the table, so a point read several levels down takes longer than on a plain table. Materialized shares stop the chain at the copy. A single-table `SELECT` with `key = value` or `key IN (...)` pushes the key into every level.
- **Instances live in members.** A member's instances use the member's shards, quotas and residency, and move with the member.

## Errors

| Code | Name | When |
|---|---|---|
| 39880 | `ER_SPRIG_SPACE_NOT_FOUND` | Unknown space, or a share target that is neither a child space nor a child tenant |
| 39881 | `ER_SPRIG_SPACE_EXISTS` | A space or share name in use |
| 39882 | `ER_SPRIG_SPACE_HAS_CHILDREN` | Dropping a space with children or rows without `CASCADE`, or the root space |
| 39883 | `ER_SPRIG_SPACE_DEPTH_EXCEEDED` | A space too deep, or a share chain too long |
| 39884 | `ER_SPRIG_SPACE_ACCESS_DENIED` | A space outside the session's reach |
| 39885 | `ER_SPRIG_NOT_INHERITABLE` | Sharing or resetting an ordinary table |
| 39886 | `ER_SPRIG_INHERIT_KEY_UPDATE` | Changing an inherited record's key |
| 39887 | `ER_SPRIG_SHARE_NOT_FOUND` | Unknown share |
| 39888 | `ER_SPRIG_SHARE_FILTER_INVALID` | A non-deterministic filter, a subquery, or an unshared column |
| 39889 | `ER_SPRIG_SHARE_RESIDENCY_CONFLICT` | No merge mode works for the placement, or `inherit.merge = federated` |
| 39890 | `ER_SPRIG_SHARE_NOT_RESHARABLE` | Re-sharing without `WITH RESHARE` |
| 39891 | `ER_SPRIG_INHERIT_STATEMENT_UNSUPPORTED` | A statement form refused on inheritable tables |
| 39895 | `ER_SPRIG_INHERITED_OBJECT_IN_USE` | A destructive change to a shared table, or to a definition with instances |
| 39896 | `ER_SPRIG_INHERIT_TARGET_TENANT` | A publish with no target, or a target that isn't a member below the publisher |
| 39897 | `ER_SPRIG_INHERIT_NOT_OVERRIDABLE` | Changing what only the definition or the parent decides |
| 39898 | `ER_SPRIG_INHERIT_DEFINITION_DROPPING` | A publish into a definition being dropped |
| 39899 | `ER_SPRIG_INHERIT_INSTANCE_UNAVAILABLE` | An instance not created in time (retryable, SQLSTATE `40001`) |
| 39240 | `ER_SPRIG_INHERIT_INSTANCE_QUOTA` | Too many definitions or instances |
| 39241 | `ER_SPRIG_INHERIT_CONFIG_MISSING` | An inherited connection's `REQUIRED` value isn't set |
| 39242 | `ER_SPRIG_INHERIT_RUN_AS_PRIVILEGE` | A member revoked what an instance needs |
| 39243 | `ER_SPRIG_INHERIT_RESIDENCY` | A cross-tenant publish or fan-in read the residency policy forbids |
| 39244 | `ER_SPRIG_INHERIT_BROADCAST_LIMIT` | Over a broadcast limit |
| 39245 | `ER_SPRIG_INHERIT_OUTBOX_PENDING` | `DROP DATABASE` while the cross-tenant outbox isn't empty |
| 39462 | `ER_SPRIG_FK_ACTION_NOT_DISTRIBUTED` | A cascading foreign-key action on an inheritable table |
| 39463 | `ER_SPRIG_ID_VALUE_RESERVED` | An explicit id inside another gateway's block |
| 1451, 1452 | MySQL's foreign-key errors | A delete of a referenced record, or a reference to a key not in view |

See [Errors](../reference/errors.md).

## Not supported

- **Collections and key-value namespaces** as inheritable objects, and inheritance over MongoDB, Redis or the key-value API. Inheritable tables are read and written through SQL and the REST SQL endpoint.
- **Federated edges** (`inherit.merge = 'federated'`), which would read a parent on another cluster live.
- **Uniqueness across levels** for columns other than the key.
- **Removing or changing inherited columns in a child.** A child can only add.
- **Record hierarchies** on inheritable tables.
- **Prepared statements, views, routines and `INSERT ... SELECT`** naming inheritable tables (39891).
- **Moving a tenant to another parent**, with or without shares.
- **Creating instances in advance.** A member's instance of a `FOR EACH TENANT` definition is created on first use only.

## Related

- [Tenants and databases](../concepts/tenants-and-databases.md): the tenant tree
- [Inheritance in SQL](../sql/inheritance.md): statement reference
- [Event streams](streams.md) and [Queues](queues.md): inherited streams, exchanges and queues
- [Sinks](sinks.md): inherited and fan-in sinks
- [Change feed](../concepts/change-feed.md): how materialized copies stay current
