---
title: Settings and scopes
audience: [user, dba, operator]
sidebar:
  order: 7
---

# Settings and scopes

SprigDB's behaviour is set by named settings, such as `auto_index.mode` or `reads.max_replica_lag_ms`, and each setting can take a different value for the whole platform, a data centre, a cluster, a tenant, a database or a single table. Read this page to understand how a value is chosen, how a wider scope limits narrower ones, and where to read and change settings.

## Settings

A setting has a dotted lower-case name: a group, then the key, such as `auto_index.max_per_table`. The [settings reference](../reference/settings.md) lists every setting with:

| Column | Meaning |
|---|---|
| Type | `int`, `float`, `bool` or `text`. Durations and lists, such as `approvals.escalate_after = '24h'`, are text in a documented form |
| Default | The built-in value, used when no scope sets one |
| Scopes | The scopes that may set it. A setting accepts only these |
| Bounds | Hard bounds built into SprigDB: a range for numbers, or a list of allowed values |
| Applies | When a change takes effect (see [When a change applies](#when-a-change-applies)) |

`bool` values accept `true`, `false`, `on`, `off`, `1` and `0`.

## Scopes and resolution

A value can be set at six scopes. When SprigDB needs a setting, it takes the value from the most specific scope that sets one:

```text
object        a table, collection, key-value namespace or search index
  database
    tenant
      each ancestor tenant, nearest first
        cluster
          data centre
            global
              built-in default
```

For example, with these values set:

| Scope | `auto_index.mode` |
|---|---|
| global | `on` |
| tenant `acme` | `recommend` |
| table `shop.orders` | `approve` |

`shop.orders` resolves to `approve`, every other table of `acme` to `recommend`, and every other tenant's tables to `on`.

Who writes each scope:

| Scope | Stored in | Set by |
|---|---|---|
| object, database, tenant | The tenant's catalog | The tenant's users, with SQL or Studio; operators, with `sprigctl` |
| ancestor | Read-only copies of each ancestor's tenant-scope values in the descendant's catalog | The ancestor tenant's administrators |
| cluster, data centre, global | The control plane (etcd) | Operators, with `sprigctl` |

When an ancestor tenant changes a tenant-scope value, the controller copies it into every descendant's catalog, so a child of `acme` inherits `acme`'s values without a lookup at query time.

## Bounds and locks

A scope can also set **bounds**: limits on what narrower scopes may choose.

| Bound | Meaning |
|---|---|
| `MIN n` | Narrower scopes may not set a smaller number |
| `MAX n` | Narrower scopes may not set a larger number |
| `ALLOWED (v, …)` | Narrower scopes may set only these values |
| `LOCKED` | Narrower scopes may not set the key at all |

Bounds bind every narrower scope: a tenant's bounds bind its databases, its tables and every descendant tenant; an operator's global bounds bind every tenant. A value must be inside the bounds of every wider scope, and inside the setting's hard bounds.

So an operator can let tenants tune a setting within limits, and a tenant administrator can do the same for the people who own its databases:

```sql test
CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR(20));
ALTER DATABASE SPRIG SET BOUNDS (auto_index.max_per_table MAX 6);
```

```sql test-error
-- 39080 ER_SPRIG_SETTING_OUT_OF_BOUNDS: above the database's MAX 6
ALTER TABLE orders SPRIG SET (auto_index.max_per_table = 8);
```

```sql test
ALTER DATABASE SPRIG RESET BOUNDS (auto_index.max_per_table);
```

A write that breaks a rule is refused, and the old value stays:

| Code | Name | When |
|---|---|---|
| 39080 | `ER_SPRIG_SETTING_OUT_OF_BOUNDS` | The value is outside a wider scope's bounds or the hard bounds |
| 39081 | `ER_SPRIG_SETTING_UNKNOWN` | No such setting |
| 39082 | `ER_SPRIG_SETTING_SCOPE_NOT_ALLOWED` | The setting can't be set at this scope |
| 39083 | `ER_SPRIG_SETTING_LOCKED` | A wider scope locked the setting |
| 39084 | `ER_SPRIG_SETTING_BAD_VALUE` | The value doesn't parse as the setting's type |

**Tightening a bound doesn't fail** when narrower scopes already hold values outside it. Those values are **clamped** when they are read: a stored 8 under a new `MAX 6` reads as 6, and a value under a new lock is ignored. The statement that tightened the bound answers OK with one warning 39080 for each value it clamps or ignores, and `SHOW SPRIG SETTINGS` marks clamped values (`CLAMPED = YES`). Nothing is rewritten, so loosening the bound again restores the stored value.

## Reading settings

### SQL

`SHOW SPRIG SETTINGS` lists every setting accepted at a scope, with its effective value, where it came from, and the bounds in force:

```sql test
SHOW SPRIG SETTINGS LIKE 'reads.%';
SHOW SPRIG SETTINGS LIKE 'auto_index.%' FOR TABLE orders;
```

The `SOURCE_SCOPE` column says where the value came from: `object`, `database`, `tenant`, `ancestor <name>`, `cluster`, `dc`, `global` or `default`. The same rows, for every scope at once, are in `information_schema.SPRIG_SETTINGS`:

```sql test
ALTER TABLE orders SPRIG SET (auto_index.max_per_table = 4);
SELECT SETTING_NAME, VALUE, SOURCE_SCOPE
FROM information_schema.SPRIG_SETTINGS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders'
  AND SETTING_NAME = 'auto_index.max_per_table';
```

```text output
SETTING_NAME	VALUE	SOURCE_SCOPE
auto_index.max_per_table	4	object
```

### sprigctl

Operators see the whole chain, one line per scope, with the bounds at each:

```sh
sprigctl settings get auto_index.max_per_table --tenant acme --db shop --object orders
sprigctl settings get reads.max_replica_lag_ms --scope cluster:fra1-c2
sprigctl settings describe auto_index.mode       # type, default, scopes, hard bounds
sprigctl settings list --prefix auto_index.
sprigctl settings list --operator-only
sprigctl tenant settings acme --all              # the tenant's own rows, and its ancestors' copies
```

### Studio

**Settings** (the gear at the bottom of the activity bar) shows every key at a chosen **Scope**: the tenant, a database or a table. Each row has the key, its effective value, its source and its bounds, and the scope's own values are in bold. A tenant administrator viewing a descendant tenant sees its tenant scope.

## Changing settings

### SQL

Tenant users change the object, database and tenant scopes in SQL:

```sql
ALTER TABLE orders SPRIG SET (auto_index.mode = 'recommend');
ALTER DATABASE shop SPRIG SET (reads.routing = 'writer');
ALTER TENANT SPRIG SET (auth.password_history = 5);
SPRIG SET auto_index.mode = 'recommend';           -- the tenant scope, short form
ALTER TENANT `acme-eu` SPRIG SET (auth.password_lock_minutes = 30); -- a descendant tenant

ALTER TABLE orders SPRIG RESET (auto_index.mode);  -- inherit again
ALTER TENANT SPRIG SET BOUNDS (auto_index.mode ALLOWED ('on', 'recommend'));
ALTER TENANT SPRIG SET BOUNDS (doc.wildcard_index LOCKED);
```

| Change | Needs |
|---|---|
| A table's value | `ALTER` on the table |
| A database's value | `ALTER` on the database |
| The tenant's value | `SPRIG_SETTINGS_ADMIN` or `SPRIG_TENANT_ADMIN` |
| A descendant tenant's value | `SPRIG_TENANT_ADMIN` |
| Bounds at any scope | `SPRIG_SETTINGS_ADMIN` or `SPRIG_TENANT_ADMIN` |

A statement that names several keys writes them in order and stops at the first refusal. The full grammar is in [Settings in SQL](../sql/settings.md).

### sprigctl

Operators set every scope, with a reason that is audited:

```sh
sprigctl settings set reads.max_replica_lag_ms 500 --scope cluster:fra1-c2 --reason "slow replica"
sprigctl settings set auto_index.mode recommend --scope tenant:acme
sprigctl settings set auto_index.max_per_table 8 --scope object:acme.shop.orders
sprigctl settings reset auto_index.mode --scope tenant:acme
sprigctl settings bounds auto_index.max_per_table --scope global --max 10
sprigctl settings lock failover.mode --scope dc:fra1
sprigctl settings unlock failover.mode --scope dc:fra1
```

`--scope` takes `global`, `dc:<id>`, `cluster:<id>`, `tenant:<name>`, `db:<tenant>.<db>` or `object:<tenant>.<db>.<object>`. `reset --bounds` removes only the bounds, and `reset --all` the value and the bounds. Add `--dry-run` to see what would change.

### Studio

In **Settings**, choose the scope, select **Set** on a row, enter the value, and Studio shows the SQL statement it will run; **Run it** applies it. **Reset** appears on rows set at the chosen scope. Studio checks the value against the bounds in force before it offers to run the statement.

## Operator-only settings

Settings marked "operator only" in the reference control shared resources: the clusters, the data centre, failover, the mover and the balancer. Examples: `cluster.probe_interval_seconds`, `sharding.split_size_gb`, `failover.mode`, `limits.tenant_connections`.

- A tenant session can read them with `SHOW SPRIG SETTINGS`, but setting one fails with 1227, even at the tenant scope.
- Operators set them with `sprigctl settings`, at any scope the setting accepts, including a single tenant.
- `sprigctl settings list --operator-only` and `--tenant-settable` list each kind.

## When a change applies

| Applies | Meaning |
|---|---|
| `live` | At once, on every gateway, controller and worker. Most settings |
| `new sessions` | Read at login: sessions opened after the change use it, and a session never sees it change mid-transaction |
| `rebind` | The listener re-binds with the new value, without a restart. When the change moves, disables or tightens a listener, its open sessions close at their next safe point with 39004 `ER_SPRIG_LISTENER_RETIRED`, and clients reconnect |

A change at the tenant, database or object scope commits in the tenant's catalog with a new version, and every gateway follows that version, so the next statement on any gateway sees it. The session that made the change sees it at once. Changes at the cluster, data centre and global scopes go through the control plane and reach every process through its watches.

## Related

- [Settings in SQL](../sql/settings.md)
- [Settings reference](../reference/settings.md)
- [Configuration](../operate/configuration.md)
- [Tenants and databases](tenants-and-databases.md)
- [sprigctl](../tools/sprigctl.md)
