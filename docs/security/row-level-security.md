---
title: Row-level security
audience: [dba]
sidebar:
  order: 4
---

# Row-level security

Row-level security limits which rows of a table each user can read and write. You attach policies to a table; the gateway adds them to every statement that touches the table, and MariaDB checks them again on every write. Use it when users of one tenant share tables but must see only their own rows: each customer's orders, each region's accounts, each sub-tenant's data.

This page explains how policies work and gives recipes. For the full statement grammar, see [Row-level security in SQL](../sql/row-level-security.md).

## How it works

A table is **protected** once row-level security is enabled on it. Then, for every user it binds:

- a read sees only rows that pass the `USING` expressions of the policies that apply;
- an `UPDATE` or `DELETE` can only touch rows that pass;
- an `INSERT`, or the new version of an updated row, must pass the `WITH CHECK` expressions (or `USING` where a policy has no `WITH CHECK`), else the statement fails with `ER_SPRIG_RLS_CHECK_FAILED` (39363).

Policies combine as in PostgreSQL:

```text
visible = (any permissive policy passes) AND (every restrictive policy passes)
```

With no permissive policy for a command, nothing is visible. A `NULL` result fails, as in PostgreSQL.

### Who a policy binds

A protected table binds a session unless:

- the user holds `BYPASS RLS` on the table, its database or `*.*`; or
- the user holds `ALTER` on the table (a **manager**, like PostgreSQL's table owner), and the table isn't `FORCE`d.

`FORCE ROW LEVEL SECURITY` binds managers too. A policy applies to the users and roles it names in `TO`, or to everyone (`PUBLIC`) when it names none. Roles count only while active in the session.

### Enforced twice

1. **In the gateway.** Every reference to a protected table in a `SELECT`, `WITH`, `UPDATE`, `DELETE`, `INSERT ... SELECT`, `SET` or `DO` gets the policies' predicate, before any join, so the statement can't see past it. The predicate is placed first and your own condition stays in parentheses, so nothing in your statement can change what it means. Because the predicate is part of the statement, the router can use it to skip shards.
2. **In MariaDB.** Each shard of a protected table gets write-check triggers, which raise 39363 when a row version fails the policies, and policy views that keep only visible rows. The gateway sets the session's policy context on every backend connection; clients can never set it themselves (a statement naming it fails with 39050).

`rls.backend_enforcement` chooses MariaDB's part:

| Value | MariaDB enforces |
|---|---|
| `views` (default) | Write checks, and policy views for stored code |
| `triggers` | Write checks only |
| `off` | Nothing; the gateway's rewrite alone |

## A first policy

This example keeps a `FORCE`d table to rows of one region, so it binds even an administrator:

```sql test
CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  region VARCHAR(10) NOT NULL,
  total DECIMAL(10,2) NOT NULL
);
INSERT INTO orders VALUES (1, 42, 'eu', 25.00), (2, 43, 'us', 40.00), (3, 44, 'eu', 12.50);

CREATE POLICY eu_only ON orders FOR ALL TO PUBLIC USING (region = 'eu');
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders FORCE ROW LEVEL SECURITY;

SELECT id, region FROM orders ORDER BY id;
```

```text output
id	region
1	eu
3	eu
```

A row outside the policy can't be written:

```sql test-error
INSERT INTO orders VALUES (4, 45, 'us', 9.99);
```

List the table's policies:

```sql test
SHOW POLICIES ON orders;
```

Release managers again, and the administrator sees every row:

```sql test
ALTER TABLE orders NO FORCE ROW LEVEL SECURITY;
SELECT COUNT(*) FROM orders;
```

```text output
COUNT(*)
3
```

The statements:

| Statement | Does |
|---|---|
| `CREATE POLICY name ON t [AS PERMISSIVE \| RESTRICTIVE] [FOR cmd, ...] [TO grantee, ...] [USING (expr)] [WITH CHECK (expr)]` | Adds a policy; `cmd` is `ALL`, `SELECT`, `INSERT`, `UPDATE` or `DELETE`; `grantee` is `PUBLIC`, `CURRENT_USER`, a user or a role |
| `ALTER POLICY name ON t [TO ...] [USING (...)] [WITH CHECK (...)]`, `ALTER POLICY name ON t RENAME TO new` | Changes a policy |
| `DROP POLICY [IF EXISTS] name ON t` | Removes one |
| `ALTER TABLE t ENABLE \| DISABLE ROW LEVEL SECURITY` | Turns protection on or off (`SPRIG ROW LEVEL SECURITY ON \| OFF` is the same) |
| `ALTER TABLE t FORCE \| NO FORCE ROW LEVEL SECURITY` | Binds managers too, or not |
| `SHOW POLICIES [FROM db \| ON t]`, `SHOW CREATE POLICY name ON t` | Lists policies |

Policy statements need `ALTER` on the table. A policy change takes effect for every session on every gateway from its next statement, like a `REVOKE`.

## What a policy may use

A policy expression is one SQL expression over the table's columns. It may use:

- deterministic built-in functions (`COALESCE`, `LOWER`, `JSON_VALUE`, and the like);
- subqueries over other tables of the tenant (refer to the protected table's own columns inside them as `table.column`);
- the session functions below.

It may not use placeholders (`?`), user or system variables, stored functions, non-deterministic functions (`NOW()`, `RAND()`, `UUID()`, `USER()`, `CURRENT_TIMESTAMP`, `CURRENT_ROLE`), `INTO` or `FOR UPDATE`, SprigDB's internal names, or system schemas. An invalid policy fails with `ER_SPRIG_RLS_POLICY_INVALID` (39364). Policies whose subqueries read each other's tables in a cycle fail with `ER_SPRIG_RLS_POLICY_CYCLE` (39365).

### Session functions

| Function | Value |
|---|---|
| `CURRENT_USER` | The account, as `'user@host'` |
| `SPRIG_USER_ID()` | The user's numeric id |
| `SPRIG_TENANT_ID()` | The session's tenant id |
| `SPRIG_HOME_TENANT_ID()` | The user's home tenant id |
| `SPRIG_TENANT_IN_SUBTREE(expr)` | True when `expr` is the session's tenant or one of its descendants |
| `SPRIG_TENANT_MANAGED(expr)` | True when `expr` is a tenant the user administers |
| `SPRIG_USER_ATTR('key')` | An attribute stored on the user account (`CREATE USER ... ATTRIBUTE '{json}'`) |
| `SPRIG_CLAIM('key')` | A claim of the session token that authenticated the statement (REST, gRPC, Studio) |
| `SPRIG_SESSION_SPACE()` | The session's space, for [data inheritance](../data-models/inheritance.md) |

The values are bound into the statement as literals by the gateway, so they cost nothing at run time. `SPRIG_TENANT_IN_SUBTREE` and `SPRIG_TENANT_MANAGED` become `IN` lists of tenant ids up to `rls.inline_subtree_max` (1000) tenants; above that, they read a table of the subtree that gateways keep on each cluster.

## Recipes

### Each user sees their own rows

```sql
CREATE POLICY own_rows ON shop.notes FOR ALL TO PUBLIC
  USING (owner = CURRENT_USER);
ALTER TABLE shop.notes ENABLE ROW LEVEL SECURITY;
```

`owner` holds values such as `'app@%'`, written by your application; the policy's check makes sure nobody writes a row as someone else. Don't fill it with a column default of `CURRENT_USER()`: a default is evaluated inside MariaDB, which sees SprigDB's backend account rather than your login.

Or key rows by user id: `USING (user_id = SPRIG_USER_ID())`.

### A role sees a subset

Give the `support` role read access to open tickets only, while other users see everything:

```sql
CREATE POLICY everyone ON shop.tickets FOR SELECT TO PUBLIC USING (TRUE);
CREATE POLICY support_open ON shop.tickets AS RESTRICTIVE FOR SELECT TO 'support'
  USING (status = 'open');
ALTER TABLE shop.tickets ENABLE ROW LEVEL SECURITY;
```

The restrictive policy narrows what the permissive one allows, for sessions with `support` active.

### By a column value

Limit a sales team to its region, read from a mapping table:

```sql
CREATE POLICY by_region ON shop.accounts FOR ALL TO 'sales'
  USING (region IN (SELECT r.region FROM shop.rep_regions r WHERE r.rep = CURRENT_USER));
```

### Writes may only create your own rows

Let anyone read published posts, but write only their own:

```sql
CREATE POLICY read_published ON shop.posts FOR SELECT USING (published = 1 OR author = CURRENT_USER);
CREATE POLICY write_own ON shop.posts FOR INSERT WITH CHECK (author = CURRENT_USER);
CREATE POLICY change_own ON shop.posts FOR UPDATE USING (author = CURRENT_USER) WITH CHECK (author = CURRENT_USER);
CREATE POLICY delete_own ON shop.posts FOR DELETE USING (author = CURRENT_USER);
ALTER TABLE shop.posts ENABLE ROW LEVEL SECURITY;
```

### Per tenant, in shared tables

A platform tenant keeps rows for many customer tenants in one table, with a `tenant_id` column. Each user sees their own tenant's and its descendants' rows, and administrators see every tenant they manage:

```sql
CREATE POLICY tenant_rows ON shop.invoices FOR ALL
  USING (SPRIG_TENANT_IN_SUBTREE(tenant_id) OR SPRIG_TENANT_MANAGED(tenant_id));
ALTER TABLE shop.invoices ENABLE ROW LEVEL SECURITY;
```

## Across protocols

Policies are table policies, so they apply wherever the table's rows go:

| Path | How policies apply |
|---|---|
| MySQL and MariaDB listeners, REST `sql:query` and `sql:execute`, Studio | The gateway's rewrite, then MariaDB's write checks |
| MongoDB drivers | Collections are tables: reads and writes go through the same rewrite and checks. MongoDB commands create policies in filter form (below) |
| Change streams, Kafka change topics, stream triggers, sinks | Each change record is filtered by the reader's `SELECT` policies, over its before and after images: an update whose row becomes visible arrives as an insert, one that becomes hidden as a delete |
| Redis keyspace notifications | Filtered the same way, per key |
| Search | Hits are re-read through your session; counts and facets count only the hits you can see |
| `LOAD DATA` into a protected table | MariaDB's write checks |

### Policies from MongoDB drivers

```js
db.runCommand({
  sprigCreatePolicy: "orders", name: "own",
  mode: "permissive", commands: ["find", "update", "remove"],
  using: { owner: "$$USER" }
});
db.runCommand({ sprigRowSecurity: "orders", enable: true, force: false });
db.runCommand({ sprigDropPolicy: "orders", name: "own", ifExists: true });
```

A filter may use equality, `$in`, `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$nin`, `$exists: true`, `$and` and `$or`, and the session values `$$USER`, `$$USER_ID`, `$$TENANT_ID`, `$$HOME_TENANT_ID`, `$$SESSION_SPACE`, `$$CLAIM.<key>` and `$$ATTR.<key>`. Other operators are refused with 39364. Where MongoDB's matching needs more than the translation can express, the document stays hidden: a policy filter is never wider than MongoDB's own match.

## Statements a protected table refuses

Some statements can't be rewritten safely. Naming a protected table that binds the session in them fails with `ER_SPRIG_RLS_UNSUPPORTED` (39366):

- `CALL`, `PREPARE`, `EXECUTE`, `HANDLER`, `CHECKSUM`, `LOAD DATA ... REPLACE`;
- `CREATE VIEW`, `PROCEDURE`, `FUNCTION`, `TRIGGER`, `EVENT`, or `CREATE TABLE ... SELECT` over a protected table, and stored code with dynamic SQL;
- a statement the gateway's parser can't read;
- a change topic of a protected table read whole by a principal the policies bind (it is read record by record instead where possible).

DDL, `SHOW`, `DESCRIBE` and transaction statements are not affected.

## Test as a user

Tenant administrators can run one read-only statement as another user to see exactly what that user's policies allow. In Studio, open **Access control**, **Roles and policies**, then **View as a user**. Over HTTP:

```sh
curl -s https://gw.example.com:8443/v1/policies:testAs \
  -H 'Authorization: Bearer <admin-session-token>' -H 'Content-Type: application/json' \
  -d '{"user": "app", "host": "%", "database": "shop",
       "sql": "SELECT id, region FROM orders",
       "claims": {"team": "emea"}, "attributes": {"region": "eu"}}'
```

The rules:

- The caller holds `SPRIG_TENANT_ADMIN` over the user's tenant (their own or a descendant), the user doesn't administer the caller's tenant, `studio.view_as` is `on` at the user's tenant, and the caller isn't impersonating. Else `ER_SPRIG_STUDIO_VIEW_AS_DENIED` (39930).
- The statement is one read-only statement: `SELECT`, `WITH`, `TABLE`, `VALUES` or `SHOW`, without `INTO` or locking clauses. Else `ER_SPRIG_STUDIO_VIEW_AS_NOT_READ_ONLY` (39931).
- It runs as the user, with their privileges, policies and session values, capped by `studio.max_result_rows` and `studio.statement_timeout_ms`.
- Optional `claims` and `attributes` stand in for `SPRIG_CLAIM` and `SPRIG_USER_ATTR` values, so you can test policies that use them.
- The reply lists every policy the rewrite applied, with its predicate as bound for this run.
- Every test is audited as `studio.view_as`, with the caller, the user and a hash of the statement.

## Security caveats

- **Managers are exempt by default.** Anyone with `ALTER` on a table bypasses its policies until you `FORCE` it. Grant `ALTER` sparingly on protected tables, or force them.
- **`BYPASS RLS` and `SPRIG_TENANT_ADMIN`.** `BYPASS RLS` skips every policy it covers. A tenant's first administrator holds `ALL` on `*.*`, which includes `ALTER`, so they are a manager of every table and see all rows of unforced tables.
- **Claims and attributes.** `SPRIG_USER_ATTR('key')` reads the account's attributes, which an administrator sets with `CREATE USER` or `ALTER USER ... ATTRIBUTE '{json}'` (or `COMMENT 'text'`, read as `SPRIG_USER_ATTR('comment')`); see [Accounts and logins](accounts-and-logins.md#users-and-passwords). `SPRIG_CLAIM('key')` reads the claims of the SprigDB session token that authenticated the statement, over REST, gRPC or Studio: `sub`, `ten`, `sid`, `amr`, `cli`, `iat` and `exp`, with `act` while impersonating. Sessions without a session token (MySQL and the other wire protocols, HTTP Basic, API keys) have no claims, so `SPRIG_CLAIM` returns `NULL` there, and a policy that relies on a claim hides rows from them. Tokens from external identity providers aren't accepted, so there are no custom claims.
- **Constraint errors.** A plain `INSERT` whose key collides with a hidden row fails with the duplicate-key error, which tells the user that a row with that key exists. Don't rely on row-level security to keep the existence of keys secret; use keys that reveal nothing.
- **Stored code.** `SQL SECURITY DEFINER` routines over protected tables run under their definer's policies while they run, and the caller's again afterwards. Views and routines can't be created over protected tables (39366).
- **Search indexes and analytics copies** are filled without row-level security; queries re-check results through your session, but treat index storage as containing every row.
- **`rls.backend_enforcement = off`** leaves the gateway as the only enforcement. Keep the default unless you have measured the cost of the triggers.

## Settings

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `rls.backend_enforcement` | text | `views` | database, tenant, global | `views`, `triggers` or `off` |
| `rls.inline_subtree_max` | int | 1000 | tenant, dc, global | Tenant ids inlined for `SPRIG_TENANT_IN_SUBTREE` and `SPRIG_TENANT_MANAGED` |
| `studio.view_as` | text | `on` | tenant, dc, global | Allow the row-level security tester |

## Related

- [Row-level security in SQL](../sql/row-level-security.md)
- [Grants and roles](grants-and-roles.md)
- [Tenant administration](tenant-administration.md)
- [Change feed](../concepts/change-feed.md)
- [Audit](audit.md)
