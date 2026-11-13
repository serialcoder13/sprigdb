---
title: Settings in SQL
audience: [dba]
sidebar:
  order: 3
---

# Settings in SQL

SprigDB's behaviour is controlled by settings, such as `auto_index.mode` or `auth.password_policy`. This page is the SQL reference for reading and changing them at the tenant, database and table scopes, and for the bounds that limit what narrower scopes may choose. Every setting, with its type, default, scopes and hard bounds, is listed in the [settings reference](../reference/settings.md).

## How scopes resolve

A setting's value comes from the most specific scope that sets it:

```text
object (table, collection, namespace, search index)
  → database
    → tenant → each ancestor tenant, nearest first
      → cluster → data centre → global      (set by operators with sprigctl)
        → built-in default
```

SQL writes the three tenant scopes: the tenant, a database and a table (or any other object in a database's table namespace). Operators set cluster, data-centre and global values with `sprigctl settings`; those can't be changed from SQL. Each setting accepts only the scopes its reference row lists. Keys marked "operator only" can't be set from a tenant session at all. See [Settings and scopes](../concepts/settings-and-scopes.md).

**Bounds** limit the values narrower scopes may set: a minimum, a maximum, a list of allowed values, or a lock. A tenant administrator can, for example, cap `auto_index.max_per_table` at 6 for every database. Every key also has hard bounds built into SprigDB.

## Grammar

```sql
ALTER target SPRIG SET (key = value [, key = value] ...)
ALTER target SPRIG RESET (key [, key] ...)
ALTER target SPRIG SET BOUNDS (key bound [bound] ... [, key bound [bound] ...] ...)
ALTER target SPRIG RESET BOUNDS (key [, key] ...)

SPRIG SET {key = value | (key = value [, key = value] ...)}
SPRIG RESET {key | (key [, key] ...)}
SPRIG SET BOUNDS {key bound ... | (key bound ... [, ...])}
SPRIG RESET BOUNDS {key | (key [, key] ...)}

SHOW SPRIG SETTINGS [LIKE 'pattern'] [FOR target]

target:
    TENANT [tenant_name]
  | {DATABASE | SCHEMA} [db_name]
  | TABLE [db_name.]table_name

bound:
    MIN number
  | MAX number
  | ALLOWED (value [, value] ...)
  | LOCKED
  | UNLOCKED

key:    dotted lower-case name, such as auto_index.mode; may be quoted
value:  'string' | number | word        -- TRUE, ON, recommend, ...
```

Keywords are case-insensitive. The short forms without `ALTER` (`SPRIG SET …`) act on the session's tenant, exactly as `ALTER TENANT SPRIG SET`.

| Target | Meaning |
|---|---|
| `TENANT` | The session's tenant |
| `TENANT name` | The session's own tenant by name, or one of its descendants |
| `DATABASE` or `SCHEMA` without a name | The current database (`USE`); 1046 if there is none |
| `DATABASE name` | A database of the tenant |
| `TABLE name`, `TABLE db.name` | A table, collection, key-value namespace or search index; an unqualified name is in the current database |

Each value is parsed with its key's type: `int`, `float`, `bool` (`true`, `false`, `on`, `off`, `1`, `0`), `text` or a duration. A statement that names several keys writes them one at a time, in order, and stops at the first refusal; the keys before it stay changed.

## ALTER TABLE … SPRIG SET

Sets values for one table. The table overrides its database, tenant and everything wider.

```sql
ALTER TABLE [db_name.]table_name SPRIG SET (key = value [, key = value] ...)
ALTER TABLE [db_name.]table_name SPRIG RESET (key [, key] ...)
```

**Privileges:** `ALTER` on the table. `SHOW SPRIG SETTINGS … FOR TABLE` needs any privilege on it.

```sql test
CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, status VARCHAR(20));
ALTER TABLE orders SPRIG SET (auto_index.mode = 'recommend', auto_index.max_per_table = 8);
SELECT SETTING_NAME, VALUE, SOURCE_SCOPE
FROM information_schema.SPRIG_SETTINGS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders'
  AND SETTING_NAME IN ('auto_index.mode', 'auto_index.max_per_table')
ORDER BY SETTING_NAME;
```

```text output
SETTING_NAME	VALUE	SOURCE_SCOPE
auto_index.max_per_table	8	object
auto_index.mode	recommend	object
```

```sql test
ALTER TABLE orders SPRIG RESET (auto_index.mode);
```

A qualified name works too: `ALTER TABLE shop.orders SPRIG SET (…)`.

`ALTER TABLE … SPRIG SET` changes only settings: it never runs DDL and never reaches MariaDB. `RESET` removes the table's value, so the next scope's value applies again.

## ALTER DATABASE … SPRIG SET

```sql
ALTER {DATABASE | SCHEMA} [db_name] SPRIG SET (key = value [, ...])
ALTER {DATABASE | SCHEMA} [db_name] SPRIG RESET (key [, ...])
```

**Privileges:** `ALTER` on the database.

```sql test
ALTER DATABASE SPRIG SET (auto_index.mode = 'approve');   -- the current database
SELECT SETTING_NAME, VALUE, SOURCE_SCOPE
FROM information_schema.SPRIG_SETTINGS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders'
  AND SETTING_NAME = 'auto_index.mode';
```

```text output
SETTING_NAME	VALUE	SOURCE_SCOPE
auto_index.mode	approve	database
```

The table's own value was reset above, so it now takes the database's. With a name: `ALTER DATABASE shop SPRIG SET (…)`.

An ordinary `ALTER DATABASE` (character set, collation) is still DDL and works as in MySQL.

## ALTER TENANT … SPRIG SET

```sql
ALTER TENANT [tenant_name] SPRIG SET (key = value [, ...])
ALTER TENANT [tenant_name] SPRIG RESET (key [, ...])
SPRIG SET key = value
SPRIG RESET key
```

Without a name, or with the session's own tenant name, the statement sets the tenant scope. With a descendant's name, it writes that descendant's tenant scope.

**Privileges:**

| Statement | Needs |
|---|---|
| Tenant scope of your own tenant | `SPRIG_SETTINGS_ADMIN` or `SPRIG_TENANT_ADMIN` |
| A descendant tenant | `SPRIG_TENANT_ADMIN` |

A name that isn't your tenant or a descendant fails with 1227, worded as a missing `SPRIG_TENANT_ADMIN`. The answer is the same whether the tenant exists or not.

```sql
ALTER TENANT SPRIG SET (auth.password_policy = 'medium', auth.password_history = 5);
SPRIG SET auto_index.mode = 'recommend';

-- a child tenant of acme
ALTER TENANT pune SPRIG SET (auth.password_lock_minutes = 30);
```

Tenant-scope values are copied into every descendant's catalog, where they act as the "ancestor" scope.

## Bounds

```sql
ALTER target SPRIG SET BOUNDS (key bound [bound] ... [, ...])
ALTER target SPRIG RESET BOUNDS (key [, ...])
```

| Bound | Meaning |
|---|---|
| `MIN number` | Narrower scopes may not set a smaller number |
| `MAX number` | Narrower scopes may not set a larger number |
| `ALLOWED (v, …)` | Narrower scopes may set only these values |
| `LOCKED` | Narrower scopes may not set the key at all |
| `UNLOCKED` | Removes the lock, keeping the other bounds |

`SET BOUNDS` changes only the parts it names, so `SET BOUNDS (k MAX 6)` keeps an existing `MIN`. `RESET BOUNDS` removes all the key's bounds at that scope. A value and its bounds live in one row: `RESET` clears the value only, `RESET BOUNDS` the bounds only.

Bounds apply to scopes narrower than the one that sets them. Tenant bounds bind the tenant's databases and tables and every descendant tenant; database bounds bind the database's tables.

**Privileges:** `SPRIG_SETTINGS_ADMIN` (or `SPRIG_TENANT_ADMIN`) at every scope, including database and table bounds.

```sql
ALTER TENANT SPRIG SET BOUNDS (auto_index.max_per_table MAX 6,
                              auto_index.mode ALLOWED ('on', 'recommend'));
ALTER TENANT SPRIG RESET BOUNDS (auto_index.max_per_table);
```

Database bounds bind the database's tables:

```sql test
ALTER DATABASE SPRIG SET BOUNDS (auto_index.mode LOCKED);
```

```sql test-error
-- auto_index.mode is locked for every table of the database: 39083
ALTER TABLE orders SPRIG SET (auto_index.mode = 'off');
```

```sql test
ALTER DATABASE SPRIG RESET BOUNDS (auto_index.mode);
```

**Writes against bounds.** A new value outside the bounds of any wider scope, or outside the key's hard bounds, is refused with 39080 `ER_SPRIG_SETTING_OUT_OF_BOUNDS`, and the old value stays. A value for a key locked at a wider scope is refused with 39083 `ER_SPRIG_SETTING_LOCKED`.

**Tightening bounds.** Values already stored at narrower scopes aren't rejected when you tighten a bound. They are clamped when read, and the statement answers OK with one warning 39080 per value that will be clamped or ignored:

```sql test
-- orders holds auto_index.max_per_table = 8 from the example above
ALTER DATABASE SPRIG SET BOUNDS (auto_index.max_per_table MAX 6);
-- Query OK, 0 rows affected, 1 warning: code 39080, naming the key and the
-- scope whose value will be clamped (or ignored, for a lock)
SELECT SETTING_NAME, VALUE, CLAMPED
FROM information_schema.SPRIG_SETTINGS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders'
  AND SETTING_NAME = 'auto_index.max_per_table';
```

```text output
SETTING_NAME	VALUE	CLAMPED
auto_index.max_per_table	6	YES
```

A new value above the bound is refused:

```sql test-error
-- 39080 ER_SPRIG_SETTING_OUT_OF_BOUNDS
ALTER TABLE orders SPRIG SET (auto_index.max_per_table = 7);
```

## SHOW SPRIG SETTINGS

```sql
SHOW SPRIG SETTINGS [LIKE 'pattern'] [FOR target]
```

Lists every key accepted at the target's scope, with its effective value there and where it came from. Without `FOR`, the target is the tenant. `LIKE` filters keys with SQL `LIKE` wildcards (`%` and `_`).

| Column | Meaning |
|---|---|
| `SETTING_SCOPE` | The scope shown: `tenant`, `database` or `object` |
| `TABLE_SCHEMA` | The database, for database and object scopes |
| `TABLE_NAME` | The table, for the object scope |
| `SETTING_NAME` | The key |
| `VALUE` | The effective value; strings without quotes |
| `SOURCE_SCOPE` | Where the value came from: `object`, `database`, `tenant`, `ancestor <name>`, `cluster`, `dc`, `global` or `default` |
| `BOUNDS` | The bounds in force, such as `max 6`, `allowed (on, recommend)`, `locked`, or `none` |
| `CLAMPED` | `YES` when a bound changed the stored value |

**Privileges:** any privilege on the target table or database for those scopes; none for the tenant scope.

```sql test
SHOW SPRIG SETTINGS LIKE 'auto_index.%' FOR TABLE orders;
SHOW SPRIG SETTINGS LIKE 'auth.password%';
```

For `shop.orders` with its own `auto_index.max_per_table` and `auto_index.mode`, the rows for those two keys look like this:

```text
SETTING_SCOPE	TABLE_SCHEMA	TABLE_NAME	SETTING_NAME	VALUE	SOURCE_SCOPE	BOUNDS	CLAMPED
…
object	shop	orders	auto_index.max_per_table	8	object	…	NO
object	shop	orders	auto_index.mode	recommend	object	…	NO
```

`SHOW SPRIG SETTINGS` can also be prepared: its result columns are known at prepare time.

## information_schema.SPRIG_SETTINGS

The same rows, for every scope at once, as a view you can query with SQL:

```sql test
SELECT SETTING_SCOPE, SETTING_NAME, VALUE, SOURCE_SCOPE
FROM information_schema.SPRIG_SETTINGS
WHERE TABLE_SCHEMA = DATABASE()
  AND SETTING_NAME LIKE 'auto_index.%';
```

The view has the columns of `SHOW SPRIG SETTINGS`. It holds the tenant's rows, then one set per database and per table you hold any privilege on; internal objects are left out. It is built per query, and more than 10,000 rows fail with 39052 `ER_SPRIG_METADATA_TOO_LARGE`; filter with `SHOW SPRIG SETTINGS … FOR` instead. See [information_schema and SHOW](information-schema.md).

## Settings with side effects

Some object-scope settings start work when you set them:

| Key | Effect |
|---|---|
| `stream.change_topic` | `true` creates the table's change topic; `false` drops it. See [Streams, queues and sinks](streams-queues-sinks.md) |
| `queue.change_exchange` | An exchange name starts forwarding the table's changes to it; empty stops. See [Streams, queues and sinks](streams-queues-sinks.md) |

## When changes apply

A change commits in the tenant's catalog with a new version. Every gateway follows the version, and the session that made the change sees it at once. Most keys apply live; the reference's "Applies" column says when a key needs new sessions or a restart.

## Not supported in SQL

- Cluster, data-centre and global scopes: use `sprigctl settings`. See [Configuration](../operate/configuration.md).
- `ALTER FUNCTION … SPRIG SET` fails with 39051 `ER_SPRIG_NAME_REWRITE_UNSUPPORTED`.
- Keys starting with `sprig.` are internal and fail with 39081.

## Errors

| Code | Name | When |
|---|---|---|
| 1064 | (`42000`) | Syntax error |
| 1046 | (`3D000`) | `ALTER DATABASE SPRIG …` or an unqualified `TABLE` with no current database |
| 1049 | (`42000`) | The database doesn't exist |
| 1146 | (`42S02`) | The table doesn't exist |
| 1227 | (`42000`) | Missing `SPRIG_SETTINGS_ADMIN` or `SPRIG_TENANT_ADMIN`; an "operator only" key; a tenant that isn't yours or a descendant |
| 1142, 1044 | (`42000`) | Missing `ALTER` (or any privilege, for `SHOW`) on the table or database |
| 39080 | `ER_SPRIG_SETTING_OUT_OF_BOUNDS` | The value is outside a wider scope's bounds or the hard bounds. As a warning: a tightened bound will clamp stored values |
| 39081 | `ER_SPRIG_SETTING_UNKNOWN` | No such key |
| 39082 | `ER_SPRIG_SETTING_SCOPE_NOT_ALLOWED` | The key can't be set at this scope |
| 39083 | `ER_SPRIG_SETTING_LOCKED` | A wider scope locked the key |
| 39084 | `ER_SPRIG_SETTING_BAD_VALUE` | The value doesn't parse as the key's type |
| 39051 | `ER_SPRIG_NAME_REWRITE_UNSUPPORTED` | A target SQL settings don't support, such as `ALTER FUNCTION` |
| 39052 | `ER_SPRIG_METADATA_TOO_LARGE` | `information_schema.SPRIG_SETTINGS` would exceed 10,000 rows |
| 39070, 39071, 39072 | `ER_SPRIG_CATALOG_UNAVAILABLE`, `ER_SPRIG_CATALOG_CONFLICT`, `ER_SPRIG_TENANT_NOT_READY` | The catalog couldn't take the change; retry |

## Related

- [Settings reference](../reference/settings.md)
- [Settings and scopes](../concepts/settings-and-scopes.md)
- [Tenants and accounts](tenants-and-accounts.md)
- [sprigctl](../tools/sprigctl.md)
