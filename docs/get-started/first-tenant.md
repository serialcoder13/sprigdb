---
title: Your first tenant
audience: [user, dba, operator]
sidebar:
  order: 2
---

# Your first tenant

This walk-through creates a tenant with an administrator, a database with tables, an application user with only the rights it needs, and a tenant-wide setting. It takes about ten minutes on the stack from the [Quick start](quick-start.md), and every step works the same on a production deployment.

On the quick-start stack, the tenants `acme` and `globex` already exist. To follow along there, pick a new name, such as `initech`, and use it wherever this page says `acme`.

## What you need

- A running SprigDB: the [Quick start](quick-start.md) stack, or a deployment you operate.
- `sprigctl`, pointed at the controller's Admin API. On the quick-start stack, `gateway-dev.sh up` builds it at `target/debug/sprigctl`, and its default endpoint, `http://127.0.0.1:7480`, is the local controller. Elsewhere, pass `--endpoint` and `--token-file`, or set `SPRIGCTL_ENDPOINT` and `SPRIGCTL_TOKEN_FILE`.
- A MySQL or MariaDB client.

The local ports below are the quick-start's: the gateway's MySQL listener on 13306. On a production deployment it's 3306.

## 1. Create the tenant

Creating tenants is an operator's job, done with `sprigctl`:

```sh
sprigctl tenant create acme
sprigctl tenant show acme
```

The name must be a lowercase DNS label: 2 to 63 letters, digits and hyphens, starting with a letter. `tenant create` returns once the tenant is `active`. SprigDB has picked a cluster for the tenant's catalog, the active cluster with the fewest tenants, unless you named one with `--cluster`.

The new tenant has no databases and no users yet.

## 2. Add its administrator

```sh
sprigctl tenant user add acme admin --password 's3cret-Passw0rd'
```

This creates the user `admin` in tenant `acme`, with `ALL PRIVILEGES` and `SPRIG_TENANT_ADMIN` on the whole tenant. Its login name is `acme.admin`. From here on, the tenant administrator does everything else in SQL; no more operator steps are needed.

Use a strong password and keep it in a secret store. Passwords set later in SQL are checked against the tenant's password policy (`auth.password_policy`, `off` by default).

## 3. Connect as the administrator

```sh
mysql -h 127.0.0.1 -P 13306 -u acme.admin -p \
      --ssl-mode=VERIFY_IDENTITY --ssl-ca=deploy/compose/gateway-certs/ca.pem
```

The login name is always `tenant.user`. Connect with TLS: the first login of a user uses `caching_sha2_password`'s full exchange, which SprigDB accepts only over TLS (see [Connect from code](connect-from-code.md#the-caching_sha2_password-note)).

## 4. Create a database and tables

```sql
CREATE DATABASE shop;
USE shop;
```

Then create tables, as on MySQL:

```sql test
CREATE TABLE customers (
  id   BIGINT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  country CHAR(2) NOT NULL
);

CREATE TABLE orders (
  id          BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  status      VARCHAR(20) NOT NULL,
  total       DECIMAL(10,2) NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES customers (id)
);

INSERT INTO customers VALUES (1, 'Ana', 'PT'), (2, 'Ben', 'DE');
INSERT INTO orders VALUES (1, 1, 'paid', 42.50), (2, 1, 'open', 18.00), (3, 2, 'paid', 7.25);

SELECT c.name, COUNT(*) AS orders, SUM(o.total) AS total
FROM customers c JOIN orders o ON o.customer_id = c.id
GROUP BY c.name
ORDER BY c.name;
```

```text output
name	orders	total
Ana	2	60.50
Ben	1	7.25
```

This is a plain MariaDB schema underneath. SprigDB stores `shop` as schemas it names itself, such as `t42_d1_g1_s0`, but you only ever see `shop`. When the tables grow, SprigDB shards them without any change to this SQL; see [Shards and table groups](../concepts/shards-and-table-groups.md).

## 5. Create an application user

Applications shouldn't log in as the administrator. Create a user with only the rights the application needs:

```sql
CREATE USER app IDENTIFIED BY 'an0ther-s3cret-Passw0rd';
GRANT SELECT, INSERT, UPDATE, DELETE ON shop.* TO app;
SHOW GRANTS FOR app;
```

```text
+-----------------------------------------------------------------------+
| Grants for app@%                                                      |
+-----------------------------------------------------------------------+
| GRANT USAGE ON *.* TO `app`@`%`                                       |
| GRANT SELECT, INSERT, UPDATE, DELETE ON `shop`.* TO `app`@`%`         |
+-----------------------------------------------------------------------+
```

In SQL, user names never carry the tenant: you write `app`, and the user logs in as `acme.app`. `*.*` in a grant means every database of this tenant, never the whole server.

For people who only read, use a role:

```sql
CREATE ROLE reporting;
GRANT SELECT ON shop.* TO reporting;

CREATE USER analyst IDENTIFIED BY 'th1rd-s3cret-Passw0rd';
GRANT reporting TO analyst;
SET DEFAULT ROLE reporting TO analyst;
```

See [Tenants and accounts](../sql/tenants-and-accounts.md) for every account statement.

## 6. Connect as the application

```sh
mysql -h 127.0.0.1 -P 13306 -u acme.app -p \
      --ssl-mode=VERIFY_IDENTITY --ssl-ca=deploy/compose/gateway-certs/ca.pem shop
```

```sql
SELECT id, status, total FROM orders WHERE customer_id = 1;
UPDATE orders SET status = 'paid' WHERE id = 2;
DROP TABLE orders;   -- fails with 1142
```

The reads and the update work. The `DROP` fails with error 1142 (`DROP command denied`), because `app` holds no `DROP` privilege. MariaDB itself enforces that: `app`'s statements run as its own MariaDB account, granted only on `acme`'s schemas.

From code, use the same login name and password with your driver; see [Connect from code](connect-from-code.md).

## 7. Set a setting for the tenant

Settings control SprigDB's behaviour, and the tenant administrator sets them for the whole tenant, a database or one table. As `acme.admin`:

```sql
-- Remember the last five passwords of every user in the tenant
ALTER TENANT SPRIG SET (auth.password_history = 5);

-- Let automatic indexing only recommend indexes, tenant-wide
SPRIG SET auto_index.mode = 'recommend';

SHOW SPRIG SETTINGS LIKE 'auth.password_history';
```

A table can override the tenant. Here, `orders` waits for approval before an index is built on it:

```sql test
ALTER TABLE orders SPRIG SET (auto_index.mode = 'approve');
SELECT SETTING_NAME, VALUE, SOURCE_SCOPE
FROM information_schema.SPRIG_SETTINGS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders'
  AND SETTING_NAME = 'auto_index.mode';
```

```text output
SETTING_NAME	VALUE	SOURCE_SCOPE
auto_index.mode	approve	object
```

An operator can set the same tenant-scope value with `sprigctl`, and also settings marked operator-only:

```sh
sprigctl settings set auth.password_history 5 --scope tenant:acme --reason "security baseline"
sprigctl settings get auth.password_history --tenant acme
```

See [Settings and scopes](../concepts/settings-and-scopes.md).

## 8. Optional: a child tenant

A tenant can have child tenants, for example one per customer of a platform. An administrator of the parent administers every child:

```sh
sprigctl tenant create acme-eu --under acme
sprigctl tenant user add acme-eu admin --password 'f0urth-s3cret-Passw0rd'
```

`acme-eu` inherits `acme`'s tenant settings, such as `auth.password_history`, unless it sets its own. As `acme.admin`, you can change the child's settings:

```sql
ALTER TENANT `acme-eu` SPRIG SET (auth.password_lock_minutes = 30);
```

A tenant name with a hyphen is quoted with backticks in SQL, like any identifier.

See [Tenants and databases](../concepts/tenants-and-databases.md#the-tenant-tree).

## 9. Clean up

To remove what you made, drop the database as the administrator, then delete the tenants, children first:

```sql
DROP DATABASE shop;
```

```sh
sprigctl tenant delete acme-eu --yes
sprigctl tenant delete acme --yes
```

Deleting a tenant can't be undone: its databases, users and settings go with it. `sprigctl tenant delete` drops any databases that are left, and refuses a tenant that still has children (39362 `ER_SPRIG_TENANT_HAS_CHILDREN`).

## Related

- [Tenants and databases](../concepts/tenants-and-databases.md)
- [Tenants and accounts](../sql/tenants-and-accounts.md)
- [Connect from code](connect-from-code.md)
- [Settings and scopes](../concepts/settings-and-scopes.md)
- [Tenant administration](../security/tenant-administration.md)
