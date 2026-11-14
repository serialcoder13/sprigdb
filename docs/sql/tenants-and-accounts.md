---
title: Tenants and accounts
audience: [dba]
sidebar:
  order: 2
---

# Tenants and accounts

This page is the SQL reference for users, roles and privileges inside a tenant, and for the few statements that act on tenants. Use it when you manage who can log in and what they can do. The statements follow MySQL 8.4, with SprigDB's own privileges added and a few MySQL clauses refused.

## Tenants in SQL

A session always belongs to one tenant: the login names it. `acme.app` is user `app` of tenant `acme`. Inside the session, every account statement acts on that tenant only. Account names in SQL never carry the tenant prefix, so you write `CREATE USER app`, not `CREATE USER acme.app`.

There are no SQL statements that create, drop, list or switch tenants. `CREATE TENANT`, `DROP TENANT`, `SHOW TENANTS` and `USE TENANT` don't exist in SQL. Create and manage tenants with [`sprigctl`](../tools/sprigctl.md), the Admin API or Studio; see [Tenant administration](../security/tenant-administration.md). Over HTTP, an administrator of a parent tenant picks the tenant to act in with the `x-sprig-tenant` header (`sprig --tenant` in the shell).

The one tenant statement in SQL is `ALTER TENANT … SPRIG SET`, which changes settings:

```sql
-- the session's own tenant
ALTER TENANT SPRIG SET (auth.password_history = 5);

-- a descendant tenant, by name
ALTER TENANT pune SPRIG SET (auth.password_lock_minutes = 30);
```

Changing a descendant's settings needs `SPRIG_TENANT_ADMIN`. A name that isn't a descendant of your tenant fails with 1227, the same answer as a missing privilege, so the statement doesn't reveal which tenants exist. The full grammar, including bounds, is in [Settings in SQL](settings.md#alter-tenant--sprig-set).

## Accounts and hosts

An account is `name` or `name@host`, as in MySQL:

| Form | Meaning |
|---|---|
| `app` | `app@'%'`: any host |
| `'app'@'10.0.%'` | `app` connecting from `10.0.*` addresses |
| `CURRENT_USER` or `CURRENT_USER()` | The session's own account |

Names and hosts are at most 255 characters. Accounts live in SprigDB's catalog, not in MariaDB's `mysql.user`; SprigDB derives the MariaDB grants it needs from them. Every change applies on every gateway from the next statement after it commits.

Account changes are audited. While the audit spool is full, every statement on this page except `SHOW GRANTS`, `SHOW CREATE USER`, `SET ROLE` and `FLUSH PRIVILEGES` fails with 39970 `ER_SPRIG_AUDIT_UNAVAILABLE`.

## CREATE USER

Creates one or more users in the session's tenant.

```sql
CREATE USER [IF NOT EXISTS]
    account [auth_option] [, account [auth_option]] ...
    [DEFAULT ROLE role [, role] ...]
    [REQUIRE {NONE | SSL | X509 | tls_name [[AND] tls_name]}]
    [password_option | lock_option] ...
    [COMMENT 'text' | ATTRIBUTE 'json']

auth_option:
    IDENTIFIED BY 'password'
  | IDENTIFIED WITH plugin [BY 'password']

password_option:
    PASSWORD EXPIRE [DEFAULT | NEVER | INTERVAL n DAY]
  | PASSWORD HISTORY {DEFAULT | n}
  | PASSWORD REUSE INTERVAL {DEFAULT | n DAY}
  | PASSWORD REQUIRE CURRENT [DEFAULT | OPTIONAL]
  | FAILED_LOGIN_ATTEMPTS n
  | PASSWORD_LOCK_TIME {n | UNBOUNDED}

lock_option:
    ACCOUNT LOCK
  | ACCOUNT UNLOCK

tls_name:
    SUBJECT 'name'
  | ISSUER 'name'
```

| Clause | Meaning |
|---|---|
| `IF NOT EXISTS` | An existing account isn't an error: the statement adds note 3163 (`Authorization ID … already exists.`) and goes on |
| `IDENTIFIED BY 'password'` | The password. SprigDB stores a `caching_sha2_password` verifier, SCRAM-SHA-256 and SCRAM-SHA-512 verifiers (for MongoDB, AMQP and Kafka logins), and a `mysql_native_password` verifier when `auth.mysql_native_password` is on |
| `IDENTIFIED WITH plugin` | Accepted for compatibility. SprigDB stores the verifiers above whatever plugin you name |
| `DEFAULT ROLE` | Roles active at login |
| `REQUIRE NONE` / `SSL` / `X509` | Whether the account must connect over TLS (`SSL`) or with a verified client certificate (`X509`) |
| `REQUIRE SUBJECT 'name'`, `ISSUER 'name'` | A verified client certificate whose subject, or issuer, is exactly `name`, in the form `/C=GB/O=Acme/CN=billing`. Give either or both, in any order, with an optional `AND` between them. Each implies `X509`. See [Client certificates](../security/tls.md#client-certificates) |
| `COMMENT 'text'` | Stores `{"comment": "text"}` as the account's attributes |
| `ATTRIBUTE 'json'` | Stores a JSON object as the account's attributes, which `SPRIG_USER_ATTR('key')` reads in [row-level security](row-level-security.md) policies. A value that isn't an object fails with 3934. Give `COMMENT` or `ATTRIBUTE`, not both |
| `PASSWORD EXPIRE` | Without a value: the password is expired now, and the user must change it at the next login. `DEFAULT` follows `auth.default_password_lifetime_days`; `NEVER` never expires; `INTERVAL n DAY` expires after `n` days |
| `PASSWORD HISTORY` | Earlier passwords a new one may not repeat; `DEFAULT` follows `auth.password_history` |
| `PASSWORD REUSE INTERVAL` | Days within which a password may not be reused; `DEFAULT` follows `auth.password_reuse_interval_days` |
| `PASSWORD REQUIRE CURRENT` | The user must give the current password (`REPLACE`) to change it. `OPTIONAL` turns this off for the account; `DEFAULT` follows `auth.password_require_current` |
| `FAILED_LOGIN_ATTEMPTS n` | Failed logins in a row before a temporary lock |
| `PASSWORD_LOCK_TIME` | Lock length in minutes (SprigDB stores the number as the account's lock minutes, unlike MySQL's days), or `UNBOUNDED` until an administrator unlocks it |
| `ACCOUNT LOCK` / `UNLOCK` | A locked account can't log in |

New passwords are checked against the tenant's password rules: `auth.password_policy` (strength), `auth.password_history` and `auth.password_reuse_interval_days`.

**Privileges:** `CREATE USER`, or `SPRIG_TENANT_ADMIN`.

**Example:**

```sql test
DROP USER IF EXISTS reporter;
CREATE USER reporter IDENTIFIED BY 's3cret-Passw0rd';
CREATE USER IF NOT EXISTS reporter IDENTIFIED BY 's3cret-Passw0rd';
SHOW WARNINGS;
```

```text output
Level	Code	Message
Note	3163	Authorization ID 'reporter'@'%' already exists.
```

The new user logs in as `acme.reporter`.

**Example:** an account that must log in with a particular client certificate, and carries an attribute for row-level security. The listener must ask for client certificates (`listener.<name>.client_cert`); the development stack doesn't.

```sql test
DROP USER IF EXISTS 'billing'@'%';
CREATE USER 'billing'@'%' IDENTIFIED BY 's3cret-Passw0rd'
    REQUIRE SUBJECT '/C=GB/O=Acme/CN=billing' AND ISSUER '/C=GB/O=Acme/CN=Acme client CA'
    ATTRIBUTE '{"region": "eu"}';
```

### Clauses SprigDB refuses

These fail with 1235 (`This version of SprigDB doesn't yet support '…'`):

| Clause | Why |
|---|---|
| `IDENTIFIED BY PASSWORD 'hash'`, `IDENTIFIED WITH … AS 'hash'` | SprigDB never takes a precomputed hash |
| `REQUIRE CIPHER` | Only `NONE`, `SSL`, `X509`, `SUBJECT` and `ISSUER` |
| `WITH MAX_QUERIES_PER_HOUR …` and other resource options | Use the tenant's limits settings instead |
| `RENAME USER` | Drop and create the account instead |

## ALTER USER

Changes accounts: password, roles, TLS, expiry and locking.

```sql
ALTER USER [IF EXISTS]
    account [auth_option [RETAIN CURRENT PASSWORD]]
    [, account [auth_option [RETAIN CURRENT PASSWORD]]] ...
    [DEFAULT ROLE role [, role] ...]
    [REQUIRE {NONE | SSL | X509 | tls_name [[AND] tls_name]}]
    [password_option | lock_option] ...
    [DISCARD OLD PASSWORD]
    [COMMENT 'text' | ATTRIBUTE 'json']
```

`auth_option`, `password_option`, `lock_option` and `tls_name` are as for [`CREATE USER`](#create-user). `REQUIRE NONE`, `SSL` or `X509` clears any subject and issuer the account had. `ATTRIBUTE` merges the object into the account's attributes: keys you name are set, a key set to `null` is removed, and other keys stay. `COMMENT` sets the `comment` key the same way.

| Clause | Meaning |
|---|---|
| `IF EXISTS` | A missing account adds note 3162 (`Authorization ID … does not exist.`) instead of failing |
| `RETAIN CURRENT PASSWORD` | Keeps the old password as a secondary password, so clients using either can log in while you rotate it (dual passwords) |
| `DISCARD OLD PASSWORD` | Removes the secondary password |

**Privileges:** `CREATE USER` (or `SPRIG_TENANT_ADMIN`) to change another account. Any user may change their own password; see [Changing your own password](#changing-your-own-password).

**Example:** rotate a password without downtime.

```sql test
ALTER USER reporter IDENTIFIED BY 'n3w-s3cret-Passw0rd' RETAIN CURRENT PASSWORD;
-- deploy the new password to every client, then:
ALTER USER reporter DISCARD OLD PASSWORD;
```

## SET PASSWORD

```sql
SET PASSWORD [FOR account] = 'password'
```

`SET PASSWORD FOR account = 'password'` is `ALTER USER account IDENTIFIED BY 'password'`, with the same privilege rule. Without `FOR`, it changes your own password. The old form `= PASSWORD('…')` fails with 1235.

## Changing your own password

Any user may change their own password with one of these forms:

```sql
SET PASSWORD [FOR own_account] = 'password'

ALTER USER [IF EXISTS] {USER() | CURRENT_USER[()] | own_account}
    IDENTIFIED [WITH plugin] BY 'password'
    [REPLACE 'current_password']
    [RETAIN CURRENT PASSWORD]
```

`REPLACE` gives the current password. It is required when the account (or `auth.password_require_current`) says `PASSWORD REQUIRE CURRENT`.

When a password has expired, the session starts in a restricted mode: only these statements and `SET` run. Every other statement fails with 1820 (`You must reset your password using ALTER USER statement before executing this statement.`). After a successful change, the session works normally.

**Errors of a password change:**

| Code | When |
|---|---|
| 1819 | The password doesn't satisfy `auth.password_policy`; on the MariaDB persona, also a reused password |
| 3638 | MySQL persona: the password repeats one the history or reuse interval forbids |
| 3892 | `REPLACE` is required and missing |
| 3891 | The `REPLACE` password is wrong |

## DROP USER

```sql
DROP USER [IF EXISTS] account [, account] ...
```

Drops users and every privilege granted to them. Their open sessions close at their next safe point when `auth.close_sessions_on_revoke` is on (the default).

**Privileges:** `CREATE USER`, or `SPRIG_TENANT_ADMIN`.

```sql test
CREATE USER temp_user IDENTIFIED BY 's3cret-Passw0rd';
DROP USER temp_user;
DROP USER IF EXISTS temp_user;
SHOW WARNINGS;
```

```text output
Level	Code	Message
Note	3162	Authorization ID 'temp_user'@'%' does not exist.
```

## CREATE ROLE and DROP ROLE

```sql
CREATE ROLE [IF NOT EXISTS] role [, role] ...
DROP ROLE [IF EXISTS] role [, role] ...
```

A role is a named set of privileges you grant to users. `CREATE ROLE … WITH ADMIN` fails with 1235.

**Privileges:** `CREATE ROLE` (or `DROP ROLE`), or `CREATE USER`, or `SPRIG_TENANT_ADMIN`.

```sql test
DROP ROLE IF EXISTS reporting;
CREATE ROLE reporting;
GRANT SELECT ON * TO reporting;     -- every table of the current database
GRANT reporting TO reporter;
```

## GRANT

Grants privileges on a level, or grants roles.

```sql
GRANT priv_type [(column [, column] ...)] [, priv_type [(column [, column] ...)]] ...
    ON [TABLE | FUNCTION | PROCEDURE] priv_level
    TO account [, account] ...
    [WITH GRANT OPTION]

GRANT priv_type [, priv_type] ...
    ON GRAPH [db.]graph {NODES | RELATIONSHIPS} name
    TO account [, account] ...

GRANT role [, role] ...
    TO account [, account] ...
    [WITH ADMIN OPTION]

priv_level:
    *.*            -- the whole tenant: every database, now and later
  | *              -- the current database
  | db.*           -- one database
  | db.object      -- one table, view, function or procedure
  | object         -- an object in the current database
```

| Clause | Meaning |
|---|---|
| `(column, …)` | A column privilege: `SELECT`, `INSERT`, `UPDATE` or `REFERENCES` on these columns only |
| `ON TABLE` / `FUNCTION` / `PROCEDURE` | What kind of object `db.object` is; `TABLE` is the default |
| `*.*` | The tenant. It never reaches other tenants or the server |
| `ON GRAPH … NODES label` / `RELATIONSHIPS type` | A privilege on one node label or relationship type of a graph |
| `WITH GRANT OPTION` | The grantee may grant the same privileges on |
| `WITH ADMIN OPTION` | The grantee may grant and revoke the role |

`GRANT PROXY`, `GRANT … IDENTIFIED BY`, `GRANT … REQUIRE`, `GRANT … WITH` resource options and `GRANT … AS` fail with 1235.

**Privileges:** to grant privileges, you must hold them and `GRANT OPTION` at that level or above. To grant a role, you need `ROLE_ADMIN` or the role `WITH ADMIN OPTION`. `SPRIG_TENANT_ADMIN` may grant anything.

**Example:**

```sql test
CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(200));
CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total DECIMAL(10,2));
GRANT SELECT, INSERT, UPDATE ON orders TO reporter;
GRANT SELECT (id, name) ON customers TO reporter;
GRANT SPRIG_SETTINGS_ADMIN ON *.* TO reporter;
```

Qualified names work the same way: `GRANT SELECT ON shop.orders TO app`, `GRANT EXECUTE ON FUNCTION shop.order_total TO app`.

### Privileges

Privileges that mean nothing inside a tenant are dropped with warning 39350 (`ER_SPRIG_PRIVILEGE_DROPPED`): `SUPER`, `FILE`, `RELOAD`, `SHUTDOWN`, replication and binary-log privileges, `CREATE TABLESPACE`, `BACKUP_ADMIN`, `SYSTEM_USER` and the other server-wide names. Set `auth.unsafe_privilege_action = 'error'` to refuse them with 1227 instead. A name neither server knows fails with 3929 (MySQL persona) or 1064 (MariaDB persona).

These are the privileges SprigDB recognises, and the levels each one is accepted at:

| Privilege | Tenant `*.*` | Database `db.*` | Table `db.t` | Routine | Column |
|---|---|---|---|---|---|
| `SELECT`, `INSERT`, `UPDATE` | yes | yes | yes | | yes |
| `DELETE`, `CREATE`, `DROP`, `INDEX`, `ALTER` | yes | yes | yes | | |
| `REFERENCES` | yes | yes | yes | | yes |
| `CREATE VIEW`, `SHOW VIEW`, `TRIGGER` | yes | yes | yes | | |
| `CREATE TEMPORARY TABLES`, `LOCK TABLES`, `EVENT`, `CREATE ROUTINE` | yes | yes | | | |
| `EXECUTE`, `ALTER ROUTINE` | yes | yes | | yes | |
| `GRANT OPTION` | yes | yes | yes | yes | |
| `CREATE USER`, `CREATE ROLE`, `DROP ROLE`, `ROLE_ADMIN` | yes | | | | |
| `SHOW DATABASES`, `PROCESS` | yes | | | | |
| `CONNECTION_ADMIN` (MariaDB: `CONNECTION ADMIN`) | yes | | | | |
| `SYSTEM_VARIABLES_ADMIN` | yes | | | | |
| `SET_ANY_DEFINER` (MariaDB: `SET USER`) | yes | | | | |
| `WATCH` | yes | yes | yes | | |
| `BYPASS RLS` | yes | | | | |
| `SPRIG_SETTINGS_ADMIN` | yes | | | | |
| `SPRIG_TENANT_ADMIN` | yes | | | | |
| `SPRIG_SINK_ADMIN` | yes | | | | |
| `SPRIG_FAN_IN` | yes | | | | |

`PROCESS`, `SHOW DATABASES` and `CONNECTION_ADMIN` are scoped to the tenant: `PROCESS` shows the tenant's own sessions, and `CONNECTION_ADMIN` lets `KILL` stop other users' sessions of the tenant (without it, another user's session fails with 1095). `SYSTEM_VARIABLES_ADMIN` lets `SET GLOBAL`, `SET PERSIST` and `@@global.` assignments set the tenant's defaults for new sessions of `sql_mode`, `time_zone`, `transaction_isolation`, `max_execution_time` and `wait_timeout`; without it they fail with 1227, and any other variable fails with 1227 even with it, because it would change the MariaDB server for every tenant. See [SET GLOBAL sets the tenant's defaults](overview.md#set-global-sets-the-tenants-defaults). `SET_ANY_DEFINER` is needed to name another account in a `DEFINER` clause; see [Definers](#definers). `WATCH`, with `SELECT`, is needed to open change streams; see [Grants and roles](../security/grants-and-roles.md#change-streams-need-watch).

### SprigDB privileges

| Privilege | What it allows |
|---|---|
| `SPRIG_TENANT_ADMIN` | Everything in the tenant and its descendants: every account statement, every setting and bound, settings of descendant tenants. The tenant's first administrator holds it |
| `SPRIG_SETTINGS_ADMIN` | Tenant-scope settings (`ALTER TENANT SPRIG SET`, `SPRIG SET`) and bounds at any scope. See [Settings in SQL](settings.md) |
| `SPRIG_SINK_ADMIN` | Create, alter and drop sinks and connections. See [Streams, queues and sinks](streams-queues-sinks.md) |
| `SPRIG_FAN_IN` | Read every member tenant's instance of an inherited definition, as fan-in readers and sinks do. See [Inheritance](inheritance.md) |
| `BYPASS RLS` | Row-level security policies don't apply to the holder. See [Row-level security](row-level-security.md) |
| `WATCH` | Subscribe to change streams |

`ALL [PRIVILEGES] ON *.*` gives every tenant-level privilege except `GRANT OPTION`, `SPRIG_TENANT_ADMIN`, `BYPASS RLS`, `SPRIG_SINK_ADMIN` and `SPRIG_FAN_IN`. Grant those by name.

### Graph privileges

On a graph's labels and relationship types, `GRANT` takes graph privilege names:

| Name | Gives |
|---|---|
| `READ`, `MATCH`, `TRAVERSE`, `SELECT`, `ALL` | Read the label or type |
| `CREATE`, `INSERT` | Create elements |
| `DELETE` | Delete elements |
| `SET PROPERTY`, `SET PROPERTY {*}`, `UPDATE` | Change properties |
| `MERGE`, `MERGE {*}` | Read, create and change |
| `WRITE` | Create, change and delete |
| `USAGE` | Nothing |

Column lists aren't allowed here (1144).

```sql
GRANT READ ON GRAPH shop.social NODES Person TO app;
```

## REVOKE

```sql
REVOKE [IF EXISTS] priv_type [(column, ...)] [, priv_type [(column, ...)]] ...
    ON [TABLE | FUNCTION | PROCEDURE] priv_level
    FROM account [, account] ...

REVOKE ALL [PRIVILEGES], GRANT OPTION
    FROM account [, account] ...

REVOKE role [, role] ...
    FROM account [, account] ...
```

Revoking a privilege the account doesn't hold at that level fails with 1141 (or 1147 for a table). With `auth.partial_revokes` on, revoking on `db.*` what `*.*` gives stores a restriction instead, as MySQL's `partial_revokes` does. `REVOKE PROXY` fails with 1235.

**Privileges:** as for [`GRANT`](#grant).

```sql test
REVOKE INSERT ON orders FROM reporter;
REVOKE SPRIG_SETTINGS_ADMIN ON *.* FROM reporter;
```

```sql test-error
-- reporter no longer holds INSERT on orders: 1141 (or 1147)
REVOKE INSERT ON orders FROM reporter;
```

To remove everything at once: `REVOKE ALL PRIVILEGES, GRANT OPTION FROM reporter`.

What happens to grants when you drop or rename a table depends on `auth.grants_on_drop`: `keep` (the default, as MySQL) keeps them under the old name; `drop` deletes them with the object.

## SET ROLE

```sql
SET ROLE {DEFAULT | NONE | ALL [EXCEPT role [, role] ...] | role [, role] ...}
```

Chooses the roles active in the session. A role that isn't granted to you fails with 3530 (MySQL persona) or 1959 (MariaDB persona). With `auth.activate_all_roles_on_login` on, every granted role is active at login.

```sql test
SET ROLE NONE;
SET ROLE DEFAULT;
```

```sql test-error
-- reporting is granted to reporter, not to the session's user: 3530
SET ROLE reporting;
```

## SET DEFAULT ROLE

```sql
-- MySQL form
SET DEFAULT ROLE {NONE | ALL | role [, role] ...} TO account [, account] ...

-- MariaDB form
SET DEFAULT ROLE {role | NONE} [FOR account]
```

Both forms work on both ports. Setting another account's default roles needs `CREATE USER`.

## SHOW GRANTS

```sql
SHOW GRANTS [FOR account [USING role [, role] ...]]
```

Shows grants with logical names: databases and tables as you named them, never MariaDB's physical schemas.

| Clause | Meaning |
|---|---|
| (none) | Your own grants |
| `FOR account` | Another account's grants; needs `CREATE USER` (else 1142 on the MySQL persona, 1044 on the MariaDB persona) |
| `USING role` | Also the privileges of these roles |

```sql test
SHOW GRANTS FOR reporter;
SHOW GRANTS FOR reporter USING reporting;
```

For a user `app` holding privileges on tables of database `shop` and the role `reporting`, the result looks like this:

```text
Grants for app@%
GRANT USAGE ON *.* TO `app`@`%`
GRANT SELECT, UPDATE ON `shop`.`orders` TO `app`@`%`
GRANT SELECT (`id`, `name`) ON `shop`.`customers` TO `app`@`%`
GRANT `reporting`@`%` TO `app`@`%`
```

On the MySQL persona, dynamic privileges (`SPRIG_*`, `ROLE_ADMIN`, `WATCH`, `BYPASS RLS` and the other names not in MySQL's static list) appear on their own `ON *.*` line. On the MariaDB persona, each role is on its own line, followed by `SET DEFAULT ROLE … FOR …`.

## SHOW CREATE USER

```sql
SHOW CREATE USER account
```

Shows the `CREATE USER` statement for an account, as MySQL 8.4 prints it on 3306 and as MariaDB prints it on 3307. It never shows a password hash or verifier: on 3306 the authentication clause is `IDENTIFIED WITH '<plugin>'` alone, and on 3307 there is no `IDENTIFIED` clause. The column is named `CREATE USER for name@host`.

Your own account (`SHOW CREATE USER CURRENT_USER()`) needs no privilege. Another account needs `CREATE USER` (else 1142 on the MySQL persona, 1044 on the MariaDB persona), and an account that doesn't exist fails with 1396.

```sql
CREATE USER 'ledger'@'%' IDENTIFIED BY 's3cret-Passw0rd'
    REQUIRE SUBJECT '/O=Acme/CN=ledger' AND ISSUER '/O=Acme/CN=Acme client CA';
SHOW CREATE USER ledger;
```

On 3306:

```text
CREATE USER `ledger`@`%` IDENTIFIED WITH 'caching_sha2_password' REQUIRE SUBJECT '/O=Acme/CN=ledger' ISSUER '/O=Acme/CN=Acme client CA' PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT PASSWORD REQUIRE CURRENT DEFAULT
```

On 3307:

```text
CREATE USER `ledger`@`%` REQUIRE ISSUER '/O=Acme/CN=Acme client CA' SUBJECT '/O=Acme/CN=ledger'
```

As in MySQL and MariaDB, the `REQUIRE` clause lists `SUBJECT` and `ISSUER` without `AND`: `SUBJECT` first on 3306, `ISSUER` first on 3307.

## FLUSH PRIVILEGES

```sql
FLUSH PRIVILEGES
```

Accepted and does nothing: privilege changes apply at once. `FLUSH TABLES` is accepted the same way.

```sql test
FLUSH PRIVILEGES;
FLUSH TABLES;
-- remove the account and role the examples on this page created
DROP USER reporter;
DROP ROLE reporting;
```

## Definers

MariaDB runs triggers, events, and `SQL SECURITY DEFINER` routines and views with their definer's rights. Without a `DEFINER` clause, or with `DEFINER = CURRENT_USER`, every view and stored program you create gets your own account as its definer.

A `DEFINER = …` clause on `CREATE` or `ALTER` of a view, procedure, function, trigger or event that names another account, such as one a dump file carries, works as on MySQL 8.4:

| Case | Result |
|---|---|
| You don't hold `SET_ANY_DEFINER` | 1227 (`you need (at least one of) the SUPER or SET_ANY_DEFINER privilege(s)`) |
| The account named isn't an account of your tenant | 1227 (`you need (at least one of) the SUPER or ALLOW_NONEXISTENT_DEFINER privilege(s)`) |
| Otherwise | The named account becomes the definer |

The object then runs as its definer: `SQL SECURITY DEFINER` routines and views, triggers and events use the definer's privileges, and under row-level security the definer's policies. `information_schema` (`VIEWS`, `ROUTINES`, `TRIGGERS`, `EVENTS`) and `SHOW CREATE` name the definer you gave.

```sql
-- needs SET_ANY_DEFINER; reporter must be an account of the tenant
CREATE DEFINER = 'reporter'@'%' PROCEDURE shop.order_count()
    SQL SECURITY DEFINER
    SELECT COUNT(*) FROM shop.orders;
```

A caller with only `EXECUTE` on the procedure counts the orders with `reporter`'s `SELECT`. If `reporter` later loses that privilege, the call fails. `SET_ANY_DEFINER` therefore lets its holder run code with any account's rights in the tenant: grant it only to administrators and to the accounts that restore dumps.

If MariaDB records a definer other than the one SprigDB asked for, SprigDB drops the object and the statement fails with 39367 `ER_SPRIG_DEFINER_REFUSED`. Code in `LANGUAGE kirun`, `javascript` or `lua` doesn't take a `DEFINER` clause; see [Logic in SQL](logic.md). See also [Grants and roles](../security/grants-and-roles.md).

## Errors

| Code | Name or SQLSTATE | When |
|---|---|---|
| 1064 | `42000` | Syntax error; on the MariaDB persona also an unknown privilege |
| 1235 | `42000` | A clause SprigDB doesn't support (see the lists above) |
| 1227 | `42000` | Missing a tenant-level privilege, such as `CREATE USER`, `ROLE_ADMIN`, `SET_ANY_DEFINER`, `SYSTEM_VARIABLES_ADMIN` or `SPRIG_TENANT_ADMIN`; a `DEFINER` that names no account of the tenant; a server-wide privilege with `auth.unsafe_privilege_action = 'error'`; `SET GLOBAL` of a variable a tenant can't set |
| 1095 | `HY000` | `KILL` of another user's session without `CONNECTION_ADMIN` |
| 3934 | `HY000` | `ATTRIBUTE` with a value that isn't a JSON object |
| 1045 | `28000` | `GRANT … ON *.*` of something you can't grant |
| 1044 | `42000` | `GRANT … ON db.*` you can't give; another's `SHOW GRANTS` or `SHOW CREATE USER` on the MariaDB persona |
| 1142 | `42000` | `GRANT … ON db.t` you can't give; another's `SHOW GRANTS` or `SHOW CREATE USER` on the MySQL persona |
| 1144 | `42000` | A privilege not allowed at that level |
| 1221 | `HY000` | Database and global privileges mixed wrongly |
| 1141, 1147 | `42000` | `REVOKE` of a grant that doesn't exist |
| 1396 | `HY000` | `CREATE USER` of an existing account, `DROP USER`, `ALTER USER` or `SHOW CREATE USER` of a missing one, `SET DEFAULT ROLE` for a missing account |
| 1410 / 1133 | `42000` / `28000` | MySQL / MariaDB persona: `GRANT` to an account that doesn't exist |
| 3929 | `HY000` | MySQL persona: unknown privilege name |
| 3530 / 1959 | `HY000` / `OP000` | MySQL / MariaDB persona: `SET ROLE` of a role not granted to you |
| 1819, 3638, 3891, 3892 | `HY000` | Password rules; see [Changing your own password](#changing-your-own-password) |
| 1820 | `HY000` | The password has expired and must be changed first |
| 39350 (warning) | `ER_SPRIG_PRIVILEGE_DROPPED` | A server-wide privilege was dropped from the statement |
| 39367 | `ER_SPRIG_DEFINER_REFUSED` | A view or stored program was recorded with another definer than the one asked for; it was dropped |
| 39970 | `ER_SPRIG_AUDIT_UNAVAILABLE` | The audit spool is full; account changes are refused until it drains |
| 39070, 39071, 39072 | `ER_SPRIG_CATALOG_UNAVAILABLE`, `ER_SPRIG_CATALOG_CONFLICT`, `ER_SPRIG_TENANT_NOT_READY` | The tenant's catalog couldn't take the change; retry |

## Related

- [Accounts and logins](../security/accounts-and-logins.md)
- [Grants and roles](../security/grants-and-roles.md)
- [Tenant administration](../security/tenant-administration.md)
- [Settings in SQL](settings.md)
- [Tenants and databases](../concepts/tenants-and-databases.md)
