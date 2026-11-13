---
title: Row-level security in SQL
audience: [dba, user]
sidebar:
  order: 5
---

# Row-level security in SQL

Row-level security limits which rows of a table each user can read and write. You attach policies to a table, switch its row-level security on, and SprigDB adds the policies' conditions to every statement that touches the table, on every protocol. Use it when several users or customers share one table and must only see their own rows. This page is the SQL reference; for the model and how to plan policies, see [Row-level security](../security/row-level-security.md).

The model follows PostgreSQL's: policies are permissive or restrictive, apply to commands and to users or roles, and have a `USING` condition for rows read and a `WITH CHECK` condition for rows written.

## Statements

```sql
CREATE POLICY policy_name ON [db_name.]table_name
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR command [, command] ...]
    [TO grantee [, grantee] ...]
    [USING (expression)]
    [WITH CHECK (expression)]

ALTER POLICY policy_name ON [db_name.]table_name RENAME TO new_name
ALTER POLICY policy_name ON [db_name.]table_name
    [TO grantee [, grantee] ...]
    [USING (expression)]
    [WITH CHECK (expression)]

DROP POLICY [IF EXISTS] policy_name ON [db_name.]table_name

ALTER TABLE [db_name.]table_name {ENABLE | DISABLE | FORCE | NO FORCE} ROW LEVEL SECURITY
ALTER TABLE [db_name.]table_name SPRIG ROW LEVEL SECURITY {ON | OFF}

SHOW POLICIES [{FROM | IN} db_name | ON [db_name.]table_name]
SHOW CREATE POLICY policy_name ON [db_name.]table_name

command:  ALL | SELECT | INSERT | UPDATE | DELETE
grantee:  PUBLIC | CURRENT_USER [()] | user_or_role ['@' host]
```

## How policies apply

A table **binds** a session when:

1. its row-level security is enabled;
2. the session's user doesn't hold `BYPASS RLS`; and
3. the table is `FORCE`d, or the user doesn't hold `ALTER` on the table.

Users who can `ALTER` the table manage it and see every row, as a table's owner does in PostgreSQL, unless you `FORCE` row-level security.

For a bound table, the policies that apply are those for the statement's command whose `TO` list is `PUBLIC`, names the user, or names one of the user's active roles:

```text
rows visible     = (p1.USING OR p2.USING OR ...)  AND (r1.USING AND r2.USING AND ...)
                    permissive policies               restrictive policies
rows writable    = the same over WITH CHECK; a policy without WITH CHECK uses its USING
```

- With no applicable permissive policy, no row is visible or writable.
- `SELECT`, `UPDATE` and `DELETE` see only rows that pass `USING`.
- `INSERT` and the new row of an `UPDATE` must pass `WITH CHECK`, else the statement fails with 39363 `ER_SPRIG_RLS_CHECK_FAILED`.
- A table with security enabled and no policies shows no rows to bound users.

SprigDB applies policies twice. The gateway rewrites each statement so every reference to a protected table carries its condition, before the statement is planned and routed to shards. Inside MariaDB, `rls.backend_enforcement` adds a second layer: `views` (the default) gives bound users access to the table only through a filtering view and checks writes with triggers; `triggers` keeps only the write checks; `off` relies on the gateway alone.

## CREATE POLICY

| Clause | Default | Meaning |
|---|---|---|
| `policy_name` | | Unique per table |
| `AS PERMISSIVE` | yes | Combined with other permissive policies by `OR` |
| `AS RESTRICTIVE` | | Combined by `AND`; narrows what permissive policies allow |
| `FOR command, …` | `ALL` | The commands the policy applies to |
| `TO grantee, …` | `PUBLIC` | Who it applies to: everyone the table binds, the account running the statement (`CURRENT_USER`), or named users and roles |
| `USING (expr)` | | Which existing rows are visible to `SELECT`, `UPDATE` and `DELETE` |
| `WITH CHECK (expr)` | `USING` | Which new or changed rows `INSERT` and `UPDATE` may write |

Rules, as in PostgreSQL:

- a policy needs `USING`, `WITH CHECK` or both;
- a `FOR INSERT` policy can't have `USING`;
- a `FOR SELECT` or `FOR DELETE` policy can't have `WITH CHECK`.

Breaking one fails with 1064. A policy with the same name on the table fails with 1304; a `TO` account that doesn't exist fails with 3523.

**Privileges:** `ALTER` on the table, or `SPRIG_TENANT_ADMIN`.

### Policy expressions

An expression is one SQL condition over the table's columns. It may use:

- the table's columns, unqualified or as `table.column`;
- deterministic built-in functions (`COALESCE`, `IF`, `LOWER`, `JSON_VALUE`, comparison and string functions and the like);
- subqueries over other tables of the tenant, such as a membership table. Inside a subquery, name the protected table's columns as `table.column`;
- the session functions below.

It may not use `?` parameters, user or system variables (`@v`, `@@v`), non-deterministic functions (`NOW()`, `RAND()`, `UUID()`, `USER()`), stored functions, `INTO`, `FOR UPDATE`, names starting with `__sprig_` or `_sprig_`, or system schemas. A subquery that leads back to the same table through other policies is a cycle. An invalid expression fails with 39364 `ER_SPRIG_RLS_POLICY_INVALID`; a cycle fails with 39365 `ER_SPRIG_RLS_POLICY_CYCLE`.

| Session function | Value |
|---|---|
| `CURRENT_USER`, `CURRENT_USER()` | The session's account as `name@host`, such as `'app@%'` |
| `SPRIG_USER_ID()` | The user's numeric id |
| `SPRIG_TENANT_ID()` | The id of the tenant the session acts in |
| `SPRIG_HOME_TENANT_ID()` | The id of the user's own tenant (differs from `SPRIG_TENANT_ID()` when an administrator of a parent tenant acts in a child) |
| `SPRIG_TENANT_IN_SUBTREE(expr)` | True when `expr` is the id of the home tenant or one of its descendants |
| `SPRIG_TENANT_MANAGED(expr)` | True when `expr` is the id of a tenant the user administers |
| `SPRIG_USER_ATTR('key')` | The user's attribute `key`, set with `CREATE USER` or `ALTER USER ... ATTRIBUTE '{json}'` (see [Tenants and accounts](tenants-and-accounts.md#create-user)), or `NULL` |
| `SPRIG_CLAIM('key')` | Claim `key` of the SprigDB session token that authenticated the statement (REST, gRPC and Studio): `sub`, `ten`, `sid`, `amr`, `cli`, `iat`, `exp`, and `act` while impersonating. `NULL` in sessions without a session token, such as MySQL logins, HTTP Basic and API keys |
| `SPRIG_SESSION_SPACE()` | The session's space id (see [Inheritance](inheritance.md)), or `NULL` |

`SPRIG_USER_ATTR` and `SPRIG_CLAIM` take a string literal. Up to `rls.inline_subtree_max` tenant ids (default 1000) are written into the statement for `SPRIG_TENANT_IN_SUBTREE` and `SPRIG_TENANT_MANAGED`; beyond that, SprigDB reads them from a table.

### Example

```sql test
CREATE TABLE orders (
  id INT PRIMARY KEY,
  customer_id INT NOT NULL,
  owner VARCHAR(100) NOT NULL,
  total DECIMAL(10,2)
);
INSERT INTO orders VALUES (1, 10, 'app@%', 25.00), (2, 11, 'clerk@%', 40.00);

CREATE POLICY own_orders ON orders
  FOR SELECT
  USING (owner = CURRENT_USER());

ALTER TABLE orders ENABLE ROW LEVEL SECURITY;

SHOW POLICIES ON orders;
```

```text output
Database	Table	Policy	Mode	Commands	Roles	Using	With_check	Enabled	Forced
docs_row_level_security	orders	own_orders	PERMISSIVE	SELECT	PUBLIC	owner = CURRENT_USER()	NULL	YES	NO
```

A restrictive policy narrows what the permissive ones allow:

```sql test
CREATE POLICY no_large_orders ON orders
  AS RESTRICTIVE
  FOR INSERT, UPDATE
  WITH CHECK (total <= 10000);
```

A user without `ALTER` on `orders` who logs in as `acme.app` now sees only order 1. More examples:

```sql
-- rows of the customer stored on the account:
--   ALTER USER app ATTRIBUTE '{"customer_id": 42}';
CREATE POLICY by_attr ON shop.orders
  USING (customer_id = SPRIG_USER_ATTR('customer_id'));

-- a team's rows, through a membership table
CREATE POLICY team_rows ON shop.orders
  FOR SELECT TO reporting
  USING (customer_id IN (SELECT m.customer_id FROM shop.members m
                         WHERE m.user_id = SPRIG_USER_ID()));

-- rows of tenants in the user's subtree
CREATE POLICY subtree ON shop.orders
  USING (SPRIG_TENANT_IN_SUBTREE(org_tenant_id));
```

```sql test-error
-- NOW() isn't deterministic: 39364 ER_SPRIG_RLS_POLICY_INVALID
CREATE POLICY recent ON orders FOR SELECT USING (id > 0 AND NOW() > '2026-01-01');
```

```sql test-error
-- a FOR SELECT policy can't have WITH CHECK: 1064
CREATE POLICY bad ON orders FOR SELECT WITH CHECK (total > 0);
```

## ALTER POLICY

```sql
ALTER POLICY policy_name ON [db_name.]table_name RENAME TO new_name
ALTER POLICY policy_name ON [db_name.]table_name
    [TO grantee [, grantee] ...] [USING (expr)] [WITH CHECK (expr)]
```

Changes a policy's grantees or expressions; clauses you leave out keep their values. At least one clause is required. The mode (`PERMISSIVE` or `RESTRICTIVE`) and commands can't be changed: drop and create the policy instead. A missing policy fails with 1305.

**Privileges:** `ALTER` on the table.

```sql test
ALTER POLICY no_large_orders ON orders WITH CHECK (total <= 50000);
ALTER POLICY own_orders ON orders RENAME TO my_orders;
SHOW CREATE POLICY my_orders ON orders;
```

```text output
Policy	Create Policy
my_orders	CREATE POLICY `my_orders` ON `docs_row_level_security`.`orders` AS PERMISSIVE FOR SELECT TO PUBLIC USING (owner = CURRENT_USER())
```

## DROP POLICY

```sql
DROP POLICY [IF EXISTS] policy_name ON [db_name.]table_name
```

A missing policy fails with 1305, or with `IF EXISTS` does nothing. Dropping the table drops its policies.

**Privileges:** `ALTER` on the table.

```sql test
DROP POLICY IF EXISTS no_large_orders ON orders;
```

## ENABLE, DISABLE and FORCE ROW LEVEL SECURITY

```sql
ALTER TABLE [db_name.]table_name ENABLE ROW LEVEL SECURITY
ALTER TABLE [db_name.]table_name DISABLE ROW LEVEL SECURITY
ALTER TABLE [db_name.]table_name FORCE ROW LEVEL SECURITY
ALTER TABLE [db_name.]table_name NO FORCE ROW LEVEL SECURITY
ALTER TABLE [db_name.]table_name SPRIG ROW LEVEL SECURITY {ON | OFF}
```

| Form | Effect |
|---|---|
| `ENABLE` (or `SPRIG … ON`) | Policies apply to users the table binds |
| `DISABLE` (or `SPRIG … OFF`) | Policies are kept but don't apply |
| `FORCE` | Policies apply to users with `ALTER` on the table too |
| `NO FORCE` | Users with `ALTER` see every row again |

`BYPASS RLS` exempts its holder even from a `FORCE`d table. These forms change only security; they don't run DDL in MariaDB other than the checks SprigDB installs.

**Privileges:** `ALTER` on the table.

```sql test
ALTER TABLE orders FORCE ROW LEVEL SECURITY;
SELECT id FROM orders;           -- only rows whose owner is the session's account
ALTER TABLE orders NO FORCE ROW LEVEL SECURITY;
```

## SHOW POLICIES

```sql
SHOW POLICIES [{FROM | IN} db_name | ON [db_name.]table_name]
```

Without a clause, lists the current database's tables that have policies or a security setting. Tables you hold neither `SELECT` nor `ALTER` on are left out. A table with security switched but no policies has one row with `NULL` policy columns.

| Column | Meaning |
|---|---|
| `Database`, `Table` | The table |
| `Policy` | The policy name |
| `Mode` | `PERMISSIVE` or `RESTRICTIVE` |
| `Commands` | `ALL`, or a list such as `SELECT, UPDATE` |
| `Roles` | `PUBLIC`, or the accounts and roles, such as `` `app`@`%` `` |
| `Using`, `With_check` | The expressions as written, or `NULL` |
| `Enabled`, `Forced` | `YES` or `NO` |

## SHOW CREATE POLICY

```sql
SHOW CREATE POLICY policy_name ON [db_name.]table_name
```

Returns `Policy` and `Create Policy`: a `CREATE POLICY` statement that recreates it, with a qualified table name. Needs `SELECT` or `ALTER` on the table; otherwise, and for a missing policy, it fails with 1305.

## information_schema.SPRIG_ROW_POLICIES

The rows of `SHOW POLICIES` for every database you can see, as a view:

| Column | Same as |
|---|---|
| `TABLE_SCHEMA`, `TABLE_NAME` | `Database`, `Table` |
| `POLICY_NAME` | `Policy` |
| `POLICY_MODE` | `Mode` |
| `COMMANDS` | `Commands` |
| `ROLES` | `Roles` |
| `USING_EXPR`, `CHECK_EXPR` | `Using`, `With_check` |
| `RLS_ENABLED`, `RLS_FORCED` | `Enabled`, `Forced` |

```sql test
SELECT TABLE_NAME, POLICY_NAME, POLICY_MODE, COMMANDS
FROM information_schema.SPRIG_ROW_POLICIES
WHERE TABLE_SCHEMA = DATABASE()
ORDER BY POLICY_NAME;
```

```text output
TABLE_NAME	POLICY_NAME	POLICY_MODE	COMMANDS
orders	my_orders	PERMISSIVE	SELECT
```

## Testing as another user

There is no SQL statement to run as another user. To see what a user sees, use Studio's row-level security tester ("view as this user"), backed by `POST /v1/policies:testAs`:

- you need `SPRIG_TENANT_ADMIN` in the user's tenant or an ancestor, and `studio.view_as` must be `on` (the default) for that tenant;
- the user may not be an administrator of your own tenant;
- only one read-only statement runs (`SELECT`, `WITH`, `TABLE`, `VALUES` or `SHOW`), with the user's privileges, roles and policies, capped by `studio.max_result_rows` and `studio.statement_timeout_ms`;
- you may supply claims and attributes that stand in for `SPRIG_CLAIM` and `SPRIG_USER_ATTR`; the answer lists each policy applied, with its condition as bound;
- every run is audited.

A refused request fails with 39930 `ER_SPRIG_STUDIO_VIEW_AS_DENIED`; a statement that isn't read-only fails with 39931 `ER_SPRIG_STUDIO_VIEW_AS_NOT_READ_ONLY`. See [Studio](../tools/studio.md).

## Other protocols and statements

Policies apply wherever the table's rows are read or written: MongoDB commands on a collection, REST and gRPC, change streams and Redis keyspace notifications (each change is checked against the subscriber's privileges and policies). For collections, `CREATE POLICY` also takes a `SPRIG MONGO USING 'name' CHECK 'name'` clause, which MongoDB's `sprigCreatePolicy` command writes; you don't need it in SQL.

Some statements can't carry the rewrite. On a table that binds the session, these fail with 39366 `ER_SPRIG_RLS_UNSUPPORTED`:

- `CALL`, `PREPARE` and `EXECUTE` of text naming a protected table, `HANDLER`, `CHECKSUM TABLE`, `LOAD DATA … REPLACE`;
- `CREATE VIEW`, `PROCEDURE`, `FUNCTION`, `TRIGGER`, `EVENT` or `CREATE TABLE … SELECT` that read a protected table, or stored code with dynamic SQL.

`LOAD DATA` without `REPLACE` is checked by MariaDB's triggers. DDL, `SHOW`, `DESCRIBE` and transaction statements are unaffected. A statement that names `@sprig_rls` (SprigDB's internal session context) fails with 39050 `ER_SPRIG_RESERVED_NAME`.

## Errors

| Code | Name | When |
|---|---|---|
| 1064 | (`42000`) | Syntax error, or a clause the command doesn't allow |
| 1046, 1049, 1146 | | No database selected; unknown database; unknown table |
| 1142 | (`42000`) | Missing `ALTER` on the table |
| 1304 | (`42000`) | `CREATE POLICY` of a name the table already has |
| 1305 | (`42000`) | The policy doesn't exist, or you can't see the table |
| 3523 | (`HY000`) | A `TO` account or role that doesn't exist |
| 39050 | `ER_SPRIG_RESERVED_NAME` | The statement names `@sprig_rls` |
| 39363 | `ER_SPRIG_RLS_CHECK_FAILED` | A written row fails `WITH CHECK` |
| 39364 | `ER_SPRIG_RLS_POLICY_INVALID` | The expression uses something a policy can't |
| 39365 | `ER_SPRIG_RLS_POLICY_CYCLE` | Policies refer to each other's tables in a cycle |
| 39366 | `ER_SPRIG_RLS_UNSUPPORTED` | A statement form that can't be filtered, on a protected table |

## Related

- [Row-level security](../security/row-level-security.md)
- [Grants and roles](../security/grants-and-roles.md)
- [Tenants and accounts](tenants-and-accounts.md)
- [Studio](../tools/studio.md)
