---
title: Grants and roles
audience: [dba]
sidebar:
  order: 2
---

# Grants and roles

SprigDB uses MySQL's grant model: privileges granted to users and roles at the tenant, database, table, column and routine level, with `GRANT`, `REVOKE` and `SHOW GRANTS` behaving as they do in MySQL 8.4. A few SprigDB privileges are added for things MySQL doesn't have. Every grant is enforced twice: by the gateway, and by MariaDB itself through a backend account per user.

## Levels

| You write | Level | Covers |
|---|---|---|
| `*.*` | Tenant | Every database of the tenant, now and later. Never another tenant, and never the server |
| `shop.*` | Database | Every object in database `shop`, now and later |
| `shop.orders` | Object | One table or view |
| `shop.orders (status, total)` | Column | Named columns of one table |
| `FUNCTION shop.f`, `PROCEDURE shop.p` | Routine | One stored function or procedure |
| `GRAPH shop.g NODES Person` | Graph element | One node label or relationship type of a graph |

`*.*` in a tenant means the tenant, not the server. A tenant administrator who runs `GRANT ALL ON *.*` gives access to every database of their own tenant and nothing else.

## Privileges by level

Each level accepts the privileges MySQL accepts there:

| Level | Privileges |
|---|---|
| Tenant (`*.*`) | Every privilege below, plus `CREATE USER`, `CREATE ROLE`, `DROP ROLE`, `SHOW DATABASES`, `PROCESS`, and the dynamic privileges |
| Database (`db.*`) | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALTER`, `INDEX`, `REFERENCES`, `CREATE VIEW`, `SHOW VIEW`, `CREATE TEMPORARY TABLES`, `LOCK TABLES`, `CREATE ROUTINE`, `ALTER ROUTINE`, `EXECUTE`, `EVENT`, `TRIGGER`, `WATCH`, `GRANT OPTION` |
| Table or view | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALTER`, `INDEX`, `REFERENCES`, `CREATE VIEW`, `SHOW VIEW`, `TRIGGER`, `WATCH`, `GRANT OPTION` |
| Column | `SELECT`, `INSERT`, `UPDATE`, `REFERENCES` |
| Routine | `EXECUTE`, `ALTER ROUTINE`, `GRANT OPTION` |
| Graph element | `READ`, `CREATE`, `DELETE`, `SET PROPERTY {*}` |

`ALL [PRIVILEGES]` means every privilege the level accepts except `GRANT OPTION`. At `*.*`, `ALL` also leaves out `SPRIG_TENANT_ADMIN`, `BYPASS RLS`, `SPRIG_SINK_ADMIN` and `SPRIG_FAN_IN`, which must always be granted by name.

### Server-wide privileges

Privileges that only make sense for a whole server (`SUPER`, `FILE`, `RELOAD`, `SHUTDOWN`, replication and binary-log privileges, `CREATE TABLESPACE` and the like) mean nothing inside a tenant. By default a `GRANT` that names one drops it with warning 39350 and grants the rest, so scripts written for MySQL still run. With `auth.unsafe_privilege_action = error`, such a `GRANT` fails with error 1227 instead. A privilege neither server knows fails with 3929 on 3306 and 1064 on 3307.

## SprigDB and tenant-scoped dynamic privileges

These are granted at `*.*` only, except `WATCH`, and appear on the dynamic-privilege line of `SHOW GRANTS` on the MySQL listener.

| Privilege | Allows |
|---|---|
| `SPRIG_TENANT_ADMIN` | Administering the tenant and its descendant tenants: every account and grant statement, every setting, descendants' settings, impersonation where allowed. Data access still comes from ordinary privileges. See [Tenant administration](tenant-administration.md) |
| `SPRIG_SETTINGS_ADMIN` | Setting and resetting tenant-scope settings and their bounds (`SPRIG SET`, `ALTER TENANT SPRIG SET`). Showing settings needs no privilege; database and table settings need `ALTER` there |
| `SPRIG_SINK_ADMIN` | Creating, altering and dropping sinks and connections |
| `SPRIG_FAN_IN` | Reading every member's instance of an inherited definition, as fan-in readers and sinks do |
| `BYPASS RLS` | Reading and writing past row-level security policies. See [Row-level security](row-level-security.md) |
| `WATCH` | Opening change streams, together with `SELECT`. Unlike the others it can also be granted on a database or a table: `GRANT WATCH ON shop.orders TO 'auditor'`. See [Change streams need WATCH](#change-streams-need-watch) |
| `ROLE_ADMIN` | Granting and revoking any role |
| `CONNECTION_ADMIN` (MariaDB: `CONNECTION ADMIN`) | `KILL` of other users' sessions in the tenant |
| `SYSTEM_VARIABLES_ADMIN` | Required by `SET GLOBAL`, `SET PERSIST` and `@@global.` assignments, which are still refused; see below |
| `SET_ANY_DEFINER` (MariaDB: `SET USER`) | Naming another account in a `DEFINER` clause |

Notes on the MySQL dynamic privileges:

- **`KILL`.** Without `CONNECTION_ADMIN`, a session can kill only sessions of its own user; another user's session fails with 1095 (`You are not owner of thread ...`). With it, a session can kill any session of its tenant, on any gateway, but never another tenant's. The same rule applies to `KILL` sent over REST and gRPC.
- **`SET GLOBAL`.** A `SET GLOBAL`, `SET PERSIST` or `@@global.` assignment without `SYSTEM_VARIABLES_ADMIN` fails with 1227 (`you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s)`). With the privilege it still fails with 1227, because such an assignment would change the MariaDB server for every tenant on it, and tenant defaults for global variables aren't supported. It never reaches MariaDB.
- **`DEFINER`.** A `CREATE` statement whose `DEFINER` clause names an account other than your own fails with 1227 (`you need (at least one of) the SUPER or SET_ANY_DEFINER privilege(s)`) unless you hold `SET_ANY_DEFINER`. Even with it, stored programs (procedures, functions, triggers, events) are created with the creating user's own backend account as their definer, so they run with that user's rights and never with more. If MariaDB records a different definer, the program is dropped and the statement fails with `ER_SPRIG_DEFINER_REFUSED` (39367).

`SPRIG_TENANT_ADMIN`, `BYPASS RLS`, `SPRIG_SINK_ADMIN` and `SPRIG_FAN_IN` are never included in `ALL`; grant them by name:

```sql
GRANT SPRIG_SINK_ADMIN ON *.* TO 'etl'@'%';
GRANT BYPASS RLS ON *.* TO 'auditor'@'%';
```

### Change streams need WATCH

A change stream needs `SELECT` and `WATCH` on what it watches, as MongoDB needs `find` and `changeStream`:

- **MongoDB.** `watch()` on a collection without `WATCH` on it fails with 13 `Unauthorized` (`WATCH (changeStream) is required`). A database or deployment stream leaves out the collections you can't watch.
- **REST and gRPC.** A watch that names a table without `WATCH` fails with 1142 (`WATCH command denied to user ...`), and events of other tables you can't watch are left out.

```sql
GRANT SELECT, WATCH ON shop.* TO 'feed_reader'@'%';
```

`ALL PRIVILEGES` includes `WATCH`. MongoDB's built-in read roles (`read`, `readWrite`, `readAnyDatabase`, `readWriteAnyDatabase` and `backup`) include it, and a MongoDB role with the `changeStream` action gets it. When a tenant's catalog is upgraded to a version with `WATCH`, every grant that held `SELECT` gets `WATCH` at the same level, so streams that worked before keep working. Grants made after that need `WATCH` named.

## Grant and revoke

```text
GRANT priv [(cols)] {, priv [(cols)]} ON [TABLE | FUNCTION | PROCEDURE] target
    TO acct {, acct} [WITH GRANT OPTION]
REVOKE priv [(cols)] {, ...} ON target FROM acct {, acct}
REVOKE ALL [PRIVILEGES], GRANT OPTION FROM acct {, acct}
target := *.* | * | db.* | db.obj | obj
```

To grant, you need the privileges you grant and `GRANT OPTION` at that level or above (or `SPRIG_TENANT_ADMIN`). Refusals use MySQL's errors: 1045 at `*.*`, 1044 at `db.*`, 1142 at `db.t`. A grant to an account that doesn't exist fails with 1410 on 3306 and 1133 on 3307.

A `GRANT` or `REVOKE` takes effect from the next statement of every open session, on every gateway, within moments: each session recompiles its privileges when the tenant's credentials change.

The examples below create a role, give it read access to one table and write access to two of its columns, and show the result:

```sql test
CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  status VARCHAR(20) NOT NULL,
  total DECIMAL(10,2) NOT NULL
);
CREATE ROLE IF NOT EXISTS 'docs_order_clerk';
GRANT SELECT ON orders TO 'docs_order_clerk';
GRANT UPDATE (status) ON orders TO 'docs_order_clerk';
SHOW GRANTS FOR 'docs_order_clerk';
```

```sql test-error
-- Not a privilege either server knows.
GRANT FROBNICATE ON orders TO 'docs_order_clerk';
```

### Partial revokes

By default, revoking at the database level a privilege the account holds at `*.*` fails with error 1141, as in MySQL 8.4. With `auth.partial_revokes = true` (MySQL's `partial_revokes`), it stores a restriction instead, shown by `SHOW GRANTS` as `REVOKE ... ON db.*`:

```sql
GRANT SELECT ON *.* TO 'analyst'@'%';
REVOKE SELECT ON payroll.* FROM 'analyst'@'%';   -- with auth.partial_revokes = true
```

### Grants on dropped tables

With `auth.grants_on_drop = keep` (the default), grants on a table survive `DROP TABLE` and `DROP DATABASE` and apply again to a table re-created with the same name, as in MySQL. With `drop`, they are deleted with the table. The setting accepts the database, tenant and global scopes.

## Roles

Roles are named collections of privileges, as in MySQL 8.4:

```sql
CREATE ROLE 'order_clerk', 'reporting';
GRANT SELECT, INSERT, UPDATE ON shop.orders TO 'order_clerk';
GRANT 'order_clerk' TO 'app'@'%';
GRANT 'reporting' TO 'alice'@'%' WITH ADMIN OPTION;

SET DEFAULT ROLE 'order_clerk' TO 'app'@'%';     -- MySQL form
SET DEFAULT ROLE 'order_clerk' FOR 'app'@'%';    -- MariaDB form
```

- `CREATE ROLE` and `DROP ROLE` need `CREATE ROLE` or `DROP ROLE`, or `CREATE USER`.
- Granting or revoking a role needs `ROLE_ADMIN`, or the role `WITH ADMIN OPTION`; else error 1227.
- Roles can be granted to roles; a session's privileges are its own plus those of every active role and the roles they contain.

### Active roles

A session's privileges include only its **active** roles. At login, the active roles are the account's default roles, or every granted role when `auth.activate_all_roles_on_login` is on (MySQL's `activate_all_roles_on_login`). A session changes them with:

```sql
SET ROLE 'reporting';
SET ROLE ALL EXCEPT 'reporting';
SET ROLE DEFAULT;
SET ROLE NONE;
```

A role revoked from the account while a session has it active stops counting from the session's next statement.

## SHOW GRANTS

```sql test
SHOW GRANTS;
DROP ROLE IF EXISTS 'docs_order_clerk';
```

`SHOW GRANTS` shows your own grants; `SHOW GRANTS FOR acct [USING roles]` shows another account's and needs `CREATE USER` (else 1142 on 3306, 1044 on 3307). The output follows each persona:

```text
GRANT SELECT, INSERT ON *.* TO `app`@`%`
GRANT SPRIG_SETTINGS_ADMIN ON *.* TO `app`@`%`
GRANT SELECT, UPDATE (`status`) ON `shop`.`orders` TO `app`@`%`
GRANT `order_clerk`@`%` TO `app`@`%`
```

- On 3306, static privileges come first, then a separate line of dynamic privileges at `*.*`, then roles on one line.
- On 3307, roles are one line each, followed by `SET DEFAULT ROLE ... FOR ...`.
- Names are always your logical names (`shop`.`orders`), never the shard schemas behind them.
- Grants kept on a dropped table (`auth.grants_on_drop = keep`) are shown under its name.

In Studio, **Access control** has two pages for this: **Users and access** lists accounts and their grants and builds `GRANT` statements (with `WITH GRANT OPTION` if you want), and **Roles and policies** lists roles and their grants. Both generate the statements and show them to you before running them.

## How grants are enforced twice

**In the gateway.** For each statement, the gateway checks every object it touches against the session's compiled privileges, and refuses with MySQL's error before anything reaches MariaDB. This covers every protocol: a MongoDB `find`, a Redis `GET`, a REST call or a Cypher query is checked against the same grants on the tables behind it. Privileges that MariaDB can't express (`SPRIG_*`, `BYPASS RLS`, `GRANT OPTION`, `CREATE USER`, settings) are checked only here.

**In MariaDB.** Every user of a tenant also has its own MariaDB account, named `t<tenant id>_u<user id>`, used for that user's statements. Its MariaDB grants are derived from the user's grants, translated to the tenant's shard schemas:

| SprigDB grant | MariaDB grant |
|---|---|
| `*.*` | Every shard schema of the tenant (`t42_d%`), now and later; never the tenant's catalog schema |
| `shop.*` | Every schema of database `shop` (`t42_d1_%`), now and later |
| `shop.orders` | `orders` in each schema that holds it on that cluster |
| `shop.orders (status)` | The column, in each schema that holds the table |
| `FUNCTION shop.f` | The function in each schema that holds it |

An account gets its own privileges plus those of every role granted to it; which roles are active is enforced by the gateway. Only data and object privileges reach MariaDB. Backend accounts' passwords are derived from a secret only gateways hold and are never stored; the accounts require TLS.

So even a statement the gateway let through by mistake can only touch the tenant's own schemas, with the user's own rights. MariaDB also applies exact column-level rules, which the gateway checks only per table.

Gateways keep the backend accounts in step with the grants: every account change updates them, and a sweep every `auth.reconcile_interval_seconds` (300) per tenant and cluster corrects any drift, for example a grant changed by hand on MariaDB. `sprig_auth_drift_corrections_total` counts corrections.

## Pluggable authorization

`auth.authorizer` names the authorization engine a tenant's sessions use. The only engine today is `mysql_grants`, MySQL's grant model as described on this page. A change of `auth.authorizer` is refused with `ER_SPRIG_AUTHORIZER_UNKNOWN_TO_GATEWAYS` (39368) unless every running gateway knows the engine.

## Settings

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `auth.activate_all_roles_on_login` | bool | false | tenant, dc, global | Activate every granted role at login |
| `auth.partial_revokes` | bool | false | tenant, dc, global | Database-level revokes of `*.*` privileges become restrictions |
| `auth.grants_on_drop` | text | `keep` | database, tenant, global | `keep` or `drop` grants when a table or database is dropped |
| `auth.unsafe_privilege_action` | text | `warn` | tenant, dc, global | `warn` (drop server-wide privileges with 39350) or `error` (refuse with 1227) |
| `auth.authorizer` | text | `mysql_grants` | tenant, dc, global | The authorization engine; applies to new sessions |
| `auth.reconcile_interval_seconds` | int | 300 | dc, global (operator only) | Drift sweep of backend grants |

## Not supported yet

- `GRANT PROXY`.
- MySQL's mandatory roles.
- Account resource limits (`WITH MAX_QUERIES_PER_HOUR` and the like).

## Related

- [Accounts and logins](accounts-and-logins.md)
- [Tenant administration](tenant-administration.md)
- [Row-level security](row-level-security.md)
- [Tenants and accounts in SQL](../sql/tenants-and-accounts.md)
- [Graphs](../data-models/graphs.md)
