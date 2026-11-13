---
title: Inheritance in SQL
audience: [user, dba]
sidebar:
  order: 9
---

# Inheritance in SQL

SprigDB has two kinds of inheritance, and this page is the SQL reference for both:

- **Data inheritance** lets a tenant, or a space inside a tenant, share rows with the spaces and child tenants below it. Each level sees its parent's rows merged with its own changes: it can override fields, hide rows and add its own, without copying the parent's data. Use it for a catalogue that a head office maintains and branches adjust.
- **Typed tables and record hierarchies** let a table inherit another table's columns (`INHERITS`), so you can query a whole type family at once, and let a table hold a tree of records (`ADD HIERARCHY`) that you query by subtree.

For the model and when to use each, see [Inheritance](../data-models/inheritance.md). Messaging objects defined `FOR EACH TENANT` (streams, queues, triggers, connections and sinks) are in [Streams, queues and sinks](streams-queues-sinks.md#for-each-tenant).

## Data inheritance

### Spaces and layers

A **space** is a level of data inside a tenant. Every tenant has a root space, named after the tenant. Spaces form a tree below it, and the tenant's child tenants continue the same tree. A session works in one space at a time: its home space at login, or the one it chooses with `USE SPACE`.

An **inheritable table** holds every space's rows in one table, in layers. In a space, a statement reads the space's **merged view**: the rows shared down to it, with its own overrides applied, its hidden rows removed and its own rows added. The nearest level wins.

Writes in a space change only that space's layer:

| Statement in a space | Effect |
|---|---|
| `INSERT` | Adds the space's own row. A key already visible in the space fails with 1062 |
| `UPDATE` of an inherited row | Records an override of the changed fields (or the whole record, with `inherit.override = record`) |
| `UPDATE` of an own row | Updates it in place |
| `DELETE` of an inherited row | Hides it in this space and below |
| `DELETE` of an own row | Deletes it. Spaces below that override it keep their copy as an orphan (`inherit.parent_delete = keep`) or lose it (`follow`) |
| `SPRIG RESET` | Removes overrides, hides and orphans, so the parent's row shows again |

`SELECT *` and `mysqldump` see the merged view, and `SHOW CREATE TABLE` shows the table as you created it, without SprigDB's layer columns.

### CREATE SPACE

```sql
CREATE SPACE space_name [UNDER parent_space]
```

Creates a space below `parent_space`, or below the session's space. Space names are unique in the tenant and can't be the tenant's name. Spaces count toward the tenant tree's depth limit, `tenant.max_depth` (39883).

**Privileges:** `CREATE USER` or `SPRIG_TENANT_ADMIN`, and the new space must be at or below the session's space (39884).

```sql test
DROP SPACE IF EXISTS docs_eu CASCADE;
CREATE SPACE docs_eu;
SHOW SPACES;
```

### DROP SPACE

```sql
DROP SPACE [IF EXISTS] space_name [CASCADE]
```

A space with child spaces or rows in any inheritable table fails with 39882 `ER_SPRIG_SPACE_HAS_CHILDREN` unless `CASCADE`, which deletes the child spaces and their rows. The root space and the session's own space can't be dropped. **Privileges:** as for `CREATE SPACE`; the space must be below the session's space.

### ALTER SPACE … SET HOME

```sql
ALTER SPACE space_name SET HOME FOR account [, account] ...
```

Sets the users' home space: the space their sessions start in, and the highest space they may use. An unknown account fails with 1396. **Privileges:** as for `CREATE SPACE`.

### USE SPACE

```sql
USE SPACE space_name
SET @sprig_space = 'space_name'
SET @sprig_space = DEFAULT              -- back to the home space
```

Switches the session's space. Only the home space and spaces below it may be used (39884). `SPRIG_SESSION_SPACE()` in a [row-level security policy](row-level-security.md#policy-expressions) returns the space's id.

### SHOW SPACES

```sql
SHOW SPACES
```

| Column | Meaning |
|---|---|
| `Space` | The space |
| `Parent` | Its parent space |
| `Depth` | Its depth below the root |
| `Current` | Whether it is the session's space |

### ALTER TABLE … SPRIG INHERITABLE

```sql
ALTER TABLE [db_name.]table_name SPRIG INHERITABLE KEY (column [, column] ...) [TAG FIELD column]
ALTER TABLE [db_name.]table_name DROP SPRIG INHERITABLE
```

Makes a table inheritable. The key must be the table's primary key (39891). The table gains invisible layer columns, its primary and unique keys are widened with the space, and existing rows become the root space's own rows (MariaDB copies the table). `TAG FIELD` names a column that is recorded with the table's definition. A typed table can't be made inheritable (39891).

**Foreign keys.** A foreign key to or from an inheritable table is kept by SprigDB, never by MariaDB, because the referenced row may live in an ancestor's layer or another tenant. The table's existing foreign keys become SprigDB's, and the tables at their other ends become inheritable too: tables linked by foreign keys form one **group** and are shared together. A write is checked against its space's merged view of the referenced table (1452); a delete is refused while a row of the space, or of any space or tenant below it, still references the key (1451). Adding a foreign key to a table that is already shared fails with 39895.

**AUTO_INCREMENT.** Inserts take generated values from one sequence for the table's whole tree, so an id names the same row at every level. An explicit value that another gateway's block holds fails with 39463.

`DROP SPRIG INHERITABLE` makes the table ordinary again.

**Privileges:** `ALTER` on the table.

```sql test
CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2));
INSERT INTO products VALUES (1, 'Shoe', 89.00), (2, 'Sock', 15.00);
ALTER TABLE products SPRIG INHERITABLE KEY (id);
SHOW INHERITABLE TABLES;
```

### CREATE SHARE

```sql
CREATE SHARE share_name ON [db_name.]table_name
    {TO SPACE space_name | TO ALL CHILDREN}
    [WHERE condition]
    [COLUMNS (column [, column] ...)]
    [WITH RESHARE]
```

Shares a table from the session's space to one space, or to every child tenant.

| Clause | Meaning |
|---|---|
| `TO SPACE s` | A space below the session's space |
| `TO ALL CHILDREN` | Every direct child tenant. Each child gets the table (and the tables of its foreign-key group) created in its own schema on first use, with the parent's definition. Columns the parent adds later are added to the child's table too; the child's own added columns stay its own |
| `WHERE condition` | Only the rows that match, evaluated on the parent's merged values. An invalid condition fails with 39888 |
| `COLUMNS (…)` | Only these columns; the others come down as `NULL` |
| `WITH RESHARE` | The receiver may share the table further down; without it, a re-share fails with 39890 |

A table that isn't inheritable fails with 39885 `ER_SPRIG_NOT_INHERITABLE`. A share that can't be merged for the receiver, for example because of residency rules, fails with 39889. **Privileges:** `ALTER` on the table.

```sql test
CREATE SHARE products_eu ON products TO SPACE docs_eu;
SHOW SHARES;
```

Now work in the space: override a price and add a product.

```sql test
USE SPACE docs_eu;
UPDATE products SET price = 79.00 WHERE id = 1;
INSERT INTO products VALUES (3, 'Scarf', 25.00);
SELECT id, name, price FROM products ORDER BY id;
```

```text output
id	name	price
1	Shoe	79.00
2	Sock	15.00
3	Scarf	25.00
```

The root space still sees its own rows:

```sql test
SELECT id, name, price FROM products ORDER BY id;
```

```text output
id	name	price
1	Shoe	89.00
2	Sock	15.00
```

### SPRIG_ORIGIN()

In a statement that reads an inheritable table, `SPRIG_ORIGIN()` returns, for each row, a JSON object naming the level each field came from: a space or tenant name, with ` (orphaned)` for an orphaned row.

```sql test
USE SPACE docs_eu;
SELECT id, price, SPRIG_ORIGIN() FROM products WHERE id = 1;
```

### ALTER SHARE

```sql
ALTER SHARE share_name WHERE condition          -- the sharer: change the filter
ALTER SHARE share_name TAKE WHERE condition     -- the receiver: take only some of it
ALTER SHARE share_name TAKE ALL                 -- the receiver: take all of it again
```

An unknown share fails with 39887. **Privileges:** `ALTER` on the table.

### DROP SHARE

```sql
DROP SHARE [IF EXISTS] share_name
```

The receivers stop seeing the shared rows; their own rows and overrides stay.

### SPRIG RESET

```sql
SPRIG RESET [db_name.]table_name [(column [, column] ...)] [WHERE condition]
```

Removes the session space's overrides, hides and orphans of the matching rows, so the parent's values show again. With columns, only those fields' overrides go.

```sql test
USE SPACE docs_eu;
SPRIG RESET products (price) WHERE id = 1;
SELECT id, price FROM products WHERE id = 1;
```

```text output
id	price
1	89.00
```

### SHOW SHARES, SHOW CREATE SHARE and SHOW INHERITABLE TABLES

```sql
SHOW SHARES
SHOW CREATE SHARE share_name
SHOW INHERITABLE TABLES
```

| Statement | Columns |
|---|---|
| `SHOW SHARES` | `Share`, `Role` (granted or received), `Object`, `From`, `To`, `Filter`, `Take`, `Columns`, `Reshare`, `Mode` (how the receiver reads it: read-through or a materialized copy), `Copy`, `Lag_ms` |
| `SHOW CREATE SHARE` | `Share`, `Create Share`, `From`, `Mode`, `Group` (the foreign-key group shared with it) |
| `SHOW INHERITABLE TABLES` | `Table`, `Key`, `Origin` (the level that declared it), `Levels_up`, `Own_columns`, `Group`, `Shares` |

How a child tenant reads a parent's share is set by `inherit.merge` (`auto`, `read_through` or `materialized`) and `inherit.read_through_max_depth`.

### SPRIG CHECK

```sql
SPRIG CHECK IDS [db_name.]table_name
SPRIG CHECK REFERENCES [db_name.]table_name [WHERE condition]
```

`CHECK IDS` lists ids held by more than one tenant of the tree (for example from before the shared sequence). `CHECK REFERENCES` lists the rows and tenants whose references would refuse a delete of the matching rows.

```sql test
SPRIG CHECK IDS products;
SPRIG CHECK REFERENCES products WHERE id = 2;
DROP SHARE products_eu;
DROP SPACE docs_eu CASCADE;
```

### What isn't supported

Statements that can't run on merged views fail with 39891 `ER_SPRIG_INHERIT_STATEMENT_UNSUPPORTED`. Changing the key of an inherited row fails with 39886. Collections and key-value namespaces can't be inheritable.

## Typed tables

A typed table inherits another table's columns, as a subtype of it. Reading the parent returns every subtype's rows too (polymorphic reads); `ONLY` reads the parent's own rows.

### CREATE TABLE … INHERITS

```sql
CREATE TABLE table_name (subtype_columns ...)
    INHERITS (parent_table)
    [WHERE discriminator_column = 'value']
    [WITH (option = value [, ...])]

option:
    SPRIG_ABSTRACT = 1
  | SPRIG_STORAGE = {'joined' | 'single_table'}
  | SPRIG_DISCRIMINATOR = 'column'
```

| Clause | Meaning |
|---|---|
| `INHERITS (p)` | The table is a subtype of `p`: it has `p`'s columns and key, then its own. One parent only (39701). A column that clashes with an inherited one fails with 39702 |
| `WHERE c = 'v'` | With a discriminator: rows of this subtype have `c = 'v'` |
| `SPRIG_ABSTRACT = 1` | No rows may be inserted into this type itself, only into subtypes (39703) |
| `SPRIG_STORAGE` | `joined` (default): one physical table per type, joined on the key. `single_table`: every type's rows in the root's table |
| `SPRIG_DISCRIMINATOR = 'c'` | On the root: the column that says each row's type |

The parent becomes a root when its first subtype is created. Type hierarchies are at most `types.max_depth` levels deep (8; 39708). Collections, composite types, arrays, upserts and `INSERT … SELECT` into subtypes aren't supported (39710).

**Privileges:** `CREATE` on the database, as for any table.

```sql test
CREATE TABLE asset (id BIGINT PRIMARY KEY, name VARCHAR(200) NOT NULL, site_id INT);
CREATE TABLE vehicle (vin CHAR(17) NOT NULL UNIQUE) INHERITS (asset);
CREATE TABLE truck (payload_kg DECIMAL(10,2)) INHERITS (vehicle);

INSERT INTO asset (id, name, site_id) VALUES (1, 'Head office', 1);
INSERT INTO vehicle (id, name, site_id, vin) VALUES (2, 'Van', 1, 'V0002');
INSERT INTO truck (id, name, site_id, vin, payload_kg) VALUES (3, 'Lorry', 2, 'V0003', 12000);

SELECT id, name FROM asset ORDER BY id;
```

```text output
id	name
1	Head office
2	Van
3	Lorry
```

```sql test
SELECT id FROM ONLY asset;
SELECT id, vin, payload_kg FROM truck;
```

```text output
id
1
id	vin	payload_kg
3	V0003	12000.00
```

```sql test-error
-- one parent only: 39701 ER_SPRIG_TYPE_MULTIPLE_INHERITANCE
CREATE TABLE bad (x INT) INHERITS (vehicle, asset);
```

### ONLY and @sprig_inheritance

```sql
SELECT ... FROM ONLY table_name ...
UPDATE ONLY table_name ...
DELETE FROM ONLY table_name ...

SET @sprig_inheritance = {'only' | 'polymorphic' | DEFAULT}
```

`ONLY t` acts on `t`'s own rows, without its subtypes'. `@sprig_inheritance = 'only'` makes that the session's default; `'polymorphic'` (the default) includes subtypes. `ONLY` is the extension only when `t` is a typed table; elsewhere it is an ordinary name.

### Discriminators

```sql
ALTER TABLE table_name ADD DISCRIMINATOR (column) [VALUES (subtype = 'value' [, ...])]
ALTER TABLE table_name DROP DISCRIMINATOR
```

A discriminator column on the root says each row's type. Rows whose value disagrees with their type fail with 39717 `ER_SPRIG_TYPE_DISCRIMINATOR_MISMATCH`.

### Other DDL on typed tables

`ALTER TABLE` adds or drops a type's own columns. Updating a row's key fails with 39706. `DROP TABLE` of a type that has subtypes fails with 39707 unless you add `CASCADE`. Writes to SprigDB's type and path columns fail with 39705. `CREATE TYPE` isn't supported and fails with 39710.

What schema tools see: `SHOW CREATE TABLE` shows a subtype as created, with `INHERITS (parent)`; `SHOW COLUMNS` lists inherited columns first; `SHOW TABLES` lists parents before subtypes, so dumps restore in order. `information_schema.COLUMNS` shows the physical tables.

## Record hierarchies

A hierarchy turns a table with a parent column into a tree that SprigDB maintains: each row's path, depth and top ancestor.

### ADD HIERARCHY and DROP HIERARCHY

```sql
ALTER TABLE table_name ADD HIERARCHY (parent_column)
    [ON DELETE {RESTRICT | CASCADE}]
    [MAX DEPTH n]

ALTER TABLE table_name DROP HIERARCHY
```

| Clause | Default | Meaning |
|---|---|---|
| `parent_column` | | The column holding the parent row's key; `NULL` for a top row |
| `ON DELETE RESTRICT` | yes | Deleting a row that has children fails with 39714 |
| `ON DELETE CASCADE` | | Deleting a row deletes its subtree, deepest first |
| `MAX DEPTH n` | | The deepest a row may be (39713) |

Existing rows get their paths when you add the hierarchy; a cycle in them fails with 39716 and nothing changes. After that:

- an insert computes each new row's path, in the same transaction; a missing parent fails with 39712, too deep or a cycle with 39713;
- changing a row's parent moves its whole subtree, after checking for cycles (39711), size (39715) and depth (39713);
- changing a row's key fails with 39706.

### Hierarchy functions

| Function | Use | Meaning |
|---|---|---|
| `SPRIG_SUBTREE(t, k)` | `WHERE` | `t`'s row is `k` or below it |
| `SPRIG_DESCENDANTS(t, k)` | `WHERE` | `t`'s row is below `k` |
| `SPRIG_ANCESTORS(t, k)` | `WHERE` | `t`'s row is above `k` |
| `SPRIG_DEPTH(t)` | select list | The row's depth, 0 at the top |
| `SPRIG_PATH(t)` | select list | The row's path of keys, such as `/1/7/42/` |

`t` is the table's name or alias in the statement; `k` is a key, a literal or an expression such as another table's column.

```sql test
CREATE TABLE org (id INT PRIMARY KEY, parent_id INT, name VARCHAR(40));
INSERT INTO org VALUES (1, NULL, 'Company'), (2, 1, 'Sales'), (3, 2, 'EMEA'), (4, 1, 'Support');
ALTER TABLE org ADD HIERARCHY (parent_id) ON DELETE RESTRICT MAX DEPTH 40;
SELECT id FROM org WHERE SPRIG_SUBTREE(org, 2) ORDER BY id;
```

```text output
id
2
3
```

```sql test
SELECT o.id, SPRIG_DEPTH(o), SPRIG_PATH(o) FROM org o WHERE SPRIG_ANCESTORS(o, 3) ORDER BY o.id;
```

```sql test-error
-- Sales has a child: 39714 ER_SPRIG_HIERARCHY_HAS_CHILDREN
DELETE FROM org WHERE id = 2;
```

## Errors

| Code | Name | When |
|---|---|---|
| 1062 | (`23000`) | An insert in a space of a key already visible there |
| 1142, 1227 | (`42000`) | Missing `ALTER` on the table; missing `CREATE USER` or `SPRIG_TENANT_ADMIN` for spaces |
| 1451, 1452 | (`23000`) | A delete or write that breaks a foreign key on inheritable tables |
| 39361 | `ER_SPRIG_TENANT_MOVE_BLOCKED` | A tenant move that its shares prevent |
| 39463 | `ER_SPRIG_ID_VALUE_RESERVED` | An explicit id another gateway's block holds |
| 39880 | `ER_SPRIG_SPACE_NOT_FOUND` | Unknown space |
| 39881 | `ER_SPRIG_SPACE_EXISTS` | The space name is in use |
| 39882 | `ER_SPRIG_SPACE_HAS_CHILDREN` | Dropping a space with children or rows without `CASCADE`, or the root |
| 39883 | `ER_SPRIG_SPACE_DEPTH_EXCEEDED` | Past `tenant.max_depth` |
| 39884 | `ER_SPRIG_SPACE_ACCESS_DENIED` | A space outside your home space's subtree |
| 39885 | `ER_SPRIG_NOT_INHERITABLE` | Sharing a table that isn't inheritable |
| 39886 | `ER_SPRIG_INHERIT_KEY_UPDATE` | Changing the key of an inherited row |
| 39887 | `ER_SPRIG_SHARE_NOT_FOUND` | Unknown share |
| 39888 | `ER_SPRIG_SHARE_FILTER_INVALID` | A share or take filter that isn't valid |
| 39889 | `ER_SPRIG_SHARE_RESIDENCY_CONFLICT` | No way to merge the share for the receiver, such as when residency rules forbid both reading through and copying |
| 39890 | `ER_SPRIG_SHARE_NOT_RESHARABLE` | Re-sharing a share received without `WITH RESHARE` |
| 39891 | `ER_SPRIG_INHERIT_STATEMENT_UNSUPPORTED` | A statement or key form inheritance doesn't support |
| 39892 | `ER_SPRIG_INHERIT_SCHEMA_MISMATCH` | A child tenant's table diverges from its source |
| 39893 (note) | `ER_SPRIG_INHERIT_COPY_STALE` | A materialized copy lags behind its source |
| 39895 | `ER_SPRIG_INHERITED_OBJECT_IN_USE` | A destructive schema change on a table that a share still reaches |
| 39701 | `ER_SPRIG_TYPE_MULTIPLE_INHERITANCE` | More than one parent |
| 39702 | `ER_SPRIG_TYPE_FIELD_CONFLICT` | A column that clashes with an inherited one |
| 39703 | `ER_SPRIG_TYPE_ABSTRACT` | An insert into an abstract type |
| 39705 | `ER_SPRIG_TYPE_COLUMN_READ_ONLY` | A write to SprigDB's type or path columns |
| 39706 | `ER_SPRIG_TYPE_KEY_UPDATE` | Changing a typed or hierarchical row's key |
| 39707 | `ER_SPRIG_TYPE_HAS_DEPENDENTS` | Dropping a type that has subtypes, without `CASCADE` |
| 39708 | `ER_SPRIG_TYPE_PATH_TOO_LONG` | Past `types.max_depth` |
| 39710 | `ER_SPRIG_TYPE_UNSUPPORTED` | `CREATE TYPE` and other unsupported type forms |
| 39711 | `ER_SPRIG_HIERARCHY_CYCLE` | A parent change that would make a cycle |
| 39712 | `ER_SPRIG_HIERARCHY_PARENT_NOT_FOUND` | A parent that doesn't exist |
| 39713 | `ER_SPRIG_HIERARCHY_TOO_DEEP` | Past `MAX DEPTH` |
| 39714 | `ER_SPRIG_HIERARCHY_HAS_CHILDREN` | Deleting a row with children under `RESTRICT` |
| 39715 | `ER_SPRIG_SUBTREE_TOO_LARGE` | A subtree move or cascade over `hierarchy.max_move_rows` |
| 39716 | `ER_SPRIG_HIERARCHY_BACKFILL_FAILED` | `ADD HIERARCHY` over rows with a cycle or a missing parent |
| 39717 | `ER_SPRIG_TYPE_DISCRIMINATOR_MISMATCH` | A discriminator value that disagrees with the row's type |

## Related

- [Inheritance](../data-models/inheritance.md)
- [Streams, queues and sinks: FOR EACH TENANT](streams-queues-sinks.md#for-each-tenant)
- [Tenants and databases](../concepts/tenants-and-databases.md)
- [Row-level security in SQL](row-level-security.md)
