---
title: Tenants and databases
audience: [user, dba, operator]
sidebar:
  order: 2
---

# Tenants and databases

A tenant is the unit of ownership and isolation in SprigDB: it has its own users, its own databases and its own settings, and no other tenant can see them. Read this page to understand how tenants nest, how a login names its tenant, and what a tenant's databases are underneath.

## Tenants

A **tenant** is one customer, team or application: whoever owns a set of databases and the users that reach them. Every session belongs to exactly one tenant, and every statement in it acts inside that tenant.

Each tenant has:

- a **name**, such as `acme`, which users type in their login;
- a numeric **id**, such as `42`, which appears in the physical MariaDB names (`t42_…`) and is never reused, even after the tenant is deleted;
- a **catalog**, the MariaDB schema `t<id>_sprig`, which holds its databases, objects, settings, users and grants;
- a **home data centre**, where its catalog lives (see [Clusters and data centres](clusters-and-data-centres.md));
- a **state**, described in [Tenant states](#tenant-states).

### Tenant names

A tenant name is a lowercase DNS label:

- 2 to 63 characters;
- lowercase letters `a`–`z`, digits and hyphens;
- starts with a letter and doesn't end with a hyphen.

`acme`, `globex-eu` and `team7` are valid. `Acme`, `7team`, `acme_eu` and `acme-` are not. A name that breaks these rules fails with 39792 `ER_SPRIG_ADMIN_REQUEST_INVALID`. The same rule applies everywhere a tenant name appears: creating a tenant, login names, tenant endpoints (TLS server names) and API keys.

A tenant's name is reserved until its deletion has finished cleaning up. After that the name can be used again, but the new tenant gets a new id.

### The tenant tree

Tenants form a tree. A tenant created without a parent is a **root tenant**; one created `--under` another is its **child**. A reseller, for example, can be a root tenant with one child tenant per customer:

```text
platform                 root tenant, depth 0
├── acme                 depth 1
│   ├── acme-eu          depth 2
│   └── acme-us          depth 2
└── globex               depth 1
```

The tree does three things:

- **Administration from above.** A user with `SPRIG_TENANT_ADMIN` in a tenant administers that tenant and every tenant below it. For example, an administrator of `acme` may change the settings of `acme-eu`. An administrator of a child has no rights in its parent or its siblings.
- **Settings flow down.** A child inherits every tenant-scope setting of its ancestors unless it sets its own, and an ancestor can bound or lock a setting for its whole subtree. See [Settings and scopes](settings-and-scopes.md).
- **Suspension flows down.** If a tenant is suspended, so is every tenant below it.

The tree is at most `tenant.max_depth` levels deep (default 64, with the root at depth 0). Creating a tenant deeper than that fails with 39508 `ER_SPRIG_TENANT_DEPTH_EXCEEDED`. A parent must be `active` and not suspended to take a new child.

### Creating and listing tenants

Tenants are created by an operator with [`sprigctl`](../tools/sprigctl.md), through the Admin API. There is no SQL statement that creates, lists, switches or drops a tenant.

```sh
# A root tenant; the controller picks the least busy active cluster
sprigctl tenant create acme

# A child tenant, with its catalog on a chosen cluster
sprigctl tenant create acme-eu --under acme --cluster dc-a-c1

sprigctl tenant list
sprigctl tenant show acme
```

`sprigctl tenant create` returns once the tenant is `active`. Behind it, the controller of the home data centre:

1. allocates the id and records the tenant in the directory, in state `creating`;
2. picks a cluster for the catalog: the one you named with `--cluster`, or else the active cluster in the data centre that holds the fewest catalogs;
3. creates the catalog schema `t<id>_sprig` and its tables;
4. copies the ancestors' settings into the new catalog;
5. marks the tenant `active`.

A tenant starts with no databases and no users. Add its first administrator with `sprigctl tenant user add`:

```sh
sprigctl tenant user add acme admin --password 's3cret-Passw0rd'
```

A user created this way holds `ALL PRIVILEGES` and `SPRIG_TENANT_ADMIN` on `*.*` in the tenant. From then on, the administrator manages everything else in SQL. See [Your first tenant](../get-started/first-tenant.md) for a full walk-through.

If creation hasn't finished after `catalog.bootstrap_timeout_minutes` (default 30), the tenant becomes `failed`, and an operator can delete it.

Operators can limit the ids a deployment hands out with the global settings `tenant.id_min` and `tenant.id_max`, so that several deployments sharing MariaDB servers never use the same id. When the range is used up, creation fails with 39080.

### Tenant states

| State | Meaning | Logins |
|---|---|---|
| `creating` | The catalog is being set up | Refused |
| `active` | Serving | Allowed |
| `migrating` | Moving to another home data centre | Allowed |
| `failing_over` | Its home data centre is failing over | Allowed |
| `fenced` | Writes are fenced during a failover | Refused |
| `deleting` | Deleted; cleanup is running | Refused |
| `failed` | Creation gave up after `catalog.bootstrap_timeout_minutes` | Refused |

`sprigctl tenant show` prints the state, the tenant's ancestry, its catalog cluster and its catalog versions.

## Logins

A login names the tenant and the user, separated by the first dot:

```text
acme.app        user "app" of tenant "acme"
acme.etl.job    user "etl.job" of tenant "acme"
```

Only the first dot splits the name, so the user part may contain dots. Tenant names are matched exactly as typed: `ACME.app` names no tenant, because MySQL user names are case-sensitive.

The same rule applies on every protocol: MySQL and MariaDB, MongoDB, Redis, Bolt, Kafka, AMQP, and HTTP sign-in. Limits:

| Part | Limit |
|---|---|
| Whole login name | 255 characters |
| User part, MySQL persona (port 3306) | 32 characters |
| User part, MariaDB persona (port 3307) | 80 characters |

In SQL, inside a session, account names never carry the tenant. You write `CREATE USER app`, and the user logs in as `acme.app`. See [Tenants and accounts](../sql/tenants-and-accounts.md).

### Tenant endpoints

An operator can also let the host name name the tenant. With `listener.tenant_sni_suffixes` set to `db.example.com`, a client that connects over TLS to `acme.db.example.com` reaches tenant `acme` and logs in with the plain user name:

```text
host acme.db.example.com, login app        user "app" of tenant "acme"
host acme.db.example.com, login acme.app   user "acme.app" of tenant "acme" (usually no such user: 1045)
host db.example.com,      login acme.app   user "app" of tenant "acme"
```

On a tenant endpoint the whole login name is the user name, so a dotted name there never reaches another tenant. See [Tenant endpoints by server name](../security/tls.md#tenant-endpoints-by-server-name).

### What a failed login tells you

A login that names an unknown tenant, a tenant that can't take logins, or a wrong password all fail the same way, with MySQL's 1045 `Access denied`. A client can't use the error to find out which tenants exist.

Two failures are reported only after the password has been checked:

| Code | Name | When |
|---|---|---|
| 39303 | `ER_SPRIG_TENANT_SUSPENDED` | The tenant, or a tenant above it, is suspended |
| 39302 | `ER_SPRIG_REMOTE_ACCESS_DENIED` | The tenant's data residency forbids logins from this data centre. See [Data residency](../security/residency.md) |

## Databases

A tenant owns any number of **logical databases**. To you they behave like MySQL databases: `CREATE DATABASE`, `USE`, `SHOW DATABASES`, `DROP DATABASE`, qualified names such as `shop.orders`, and grants on `shop.*`.

```sql
CREATE DATABASE shop;
USE shop;
CREATE TABLE customers (id BIGINT PRIMARY KEY, name VARCHAR(100));
SHOW DATABASES;
```

```text
+--------------------+
| Database           |
+--------------------+
| information_schema |
| shop               |
+--------------------+
```

`SHOW DATABASES` and `information_schema` list only your tenant's databases, by the names you gave them.

### What a database is underneath

A logical database is not one MariaDB schema. Its objects are grouped into table groups, each table group is cut into shards, and each shard is one MariaDB schema on one cluster:

```text
tenant acme (id 42)
├── t42_sprig              the catalog
└── database shop (id 1)
    ├── t42_d1_g3_s0       table group 3, shard 0
    ├── t42_d1_g3_s1       table group 3, shard 1
    └── t42_d1             tables that aren't sharded
```

The physical names follow one pattern:

| Physical name | What it is |
|---|---|
| `t<tenant>_sprig` | The tenant's catalog |
| `t<tenant>_d<db>_g<group>_s<shard>` | One shard of one table group of a database |
| `t<tenant>_d<db>` | The database's tables that aren't sharded (every table, until the first split) |
| `t<tenant>_u<user>` | The MariaDB account of one SprigDB user |
| `t<tenant>_r<role>` | The MariaDB role of one SprigDB role |

SprigDB rewrites schema names in both directions, so you never see a physical name: errors, `SHOW CREATE TABLE`, `information_schema` and `SHOW GRANTS` all use your logical names. Table, column and index names are never changed. A new database starts with one shard per table group; see [Shards and table groups](shards-and-table-groups.md) for how it grows.

### Reserved names

These names fail with 39050 `ER_SPRIG_RESERVED_NAME`:

| Kind | Reserved |
|---|---|
| Databases | `information_schema`, `performance_schema`, `sys`, `mysql`, and any name of the form `t<digits>_…` |
| Tables, views, routines, triggers, events | Names starting with `__sprig_` |
| Columns | Names starting with `_sprig_` |
| Indexes | Names starting with `sprig_auto_`, kept for [automatic indexes](automatic-indexing.md) |

Reserved prefixes match in any letter case.

### The catalog

Each tenant's catalog `t<id>_sprig` records:

- its databases and every object in them: tables, views, collections, key-value namespaces, search indexes and the rest;
- its settings at tenant, database and object scope, plus read-only copies of its ancestors' tenant-scope settings;
- its users, password verifiers, roles and grants;
- its workload statistics, which feed automatic indexing and the sharding advisor.

Every gateway keeps a cached snapshot of each active tenant's catalog and reloads it when the catalog's version changes. A DDL statement, setting change or account change commits to the catalog first and then reaches every gateway, so the next statement on any gateway sees it.

If the catalog's cluster can't be reached, statements that change the catalog fail with 39070 `ER_SPRIG_CATALOG_UNAVAILABLE`, while queries keep running from the cached snapshot.

## Isolation

Isolation between tenants is enforced by MariaDB, not only by the gateway:

- Each SprigDB user runs its statements as its own MariaDB account, `t<tenant>_u<user>`.
- The gateway derives that account's MariaDB grants from the user's SprigDB grants, on the tenant's own schemas only.
- Database DDL (`CREATE DATABASE`, `DROP DATABASE`) runs as a separate provisioning account, so no tenant account ever holds a server-wide privilege.

So even a statement that slipped past the gateway's checks couldn't read another tenant's schema: MariaDB would refuse it.

Per-user backend accounts are set by the `[per_user]` table of the gateway's backends file, which the Compose and VM deployments configure. A gateway configured instead with one backend account per tenant, or one shared by every tenant, isolates only as far as that account's MariaDB grants go; see [Configuration](../operate/configuration.md).

Inside the tenant, SQL works as in MySQL. `GRANT … ON *.*` means "every database of this tenant", never the whole server. `PROCESS` shows only the tenant's own sessions, and `CONNECTION_ADMIN` lets you kill only them. Privileges that make sense only for a whole server, such as `SUPER`, `FILE` or `RELOAD`, are dropped from a `GRANT` with warning 39350 `ER_SPRIG_PRIVILEGE_DROPPED`.

## What a tenant administrator can do

`SPRIG_TENANT_ADMIN` is the tenant administrator's privilege. Its holder can, in the tenant and every tenant below it:

- create, change and drop users and roles, and grant any privilege;
- create and drop databases, and change any object;
- set and reset settings at tenant, database and object scope, and set bounds and locks for the subtree;
- see and act on recommendations waiting for approval (see [Approvals](approvals.md));
- see the sharding advisor's runs, which cover every database.

`ALL PRIVILEGES ON *.*` doesn't include `SPRIG_TENANT_ADMIN`; grant it by name:

```sql
GRANT SPRIG_TENANT_ADMIN ON *.* TO ops;
```

A tenant administrator can't create or delete tenants, register clusters, move tenants between data centres, or change settings an operator marked operator-only. Those are operator tasks in `sprigctl`. See [Tenant administration](../security/tenant-administration.md).

## Suspension

A platform operator suspends a tenant, for example for an unpaid bill or an investigation, and resumes it later:

```sh
sprigctl tenant suspend acme --reason "invoice 2026-118 overdue"
sprigctl tenant resume acme
```

A suspended tenant keeps its data, but nobody can use it:

- Logins to the tenant, and to every tenant below it, fail with 39303 `ER_SPRIG_TENANT_SUSPENDED` on every protocol (HTTP answers 403). The error comes only after a correct password, so it doesn't reveal the tenant to someone without credentials.
- Open sessions of the tenant and its descendants get `ER_SPRIG_SESSION_REVOKED` (39306) at their next statement, while `auth.close_sessions_on_revoke` is on (the default).
- Its triggers, sinks and scheduled runs wait until it is resumed. Housekeeping goes on, so the data stays as the tenant left it: shard moves, index builds, search copies, key-value expiry and document TTL expiry keep running.
- A suspended tenant can't take new child tenants: creating one fails with 39303.

Gateways read the tenant directory with a one-second cache, so a suspension reaches every login within about a second.

## Deletion

To delete a tenant, run in its home data centre:

```sh
sprigctl tenant delete acme-eu --yes
```

Only a leaf tenant can be deleted. A tenant that still has children fails with 39362 `ER_SPRIG_TENANT_HAS_CHILDREN`; delete the children first. The tenant must be `active` or `failed`; any other state fails with 39072 `ER_SPRIG_TENANT_NOT_READY`.

Deletion takes effect at once and can't be undone. There is no grace period or recycle bin. If you might need the data, take an export first:

```sh
sprigctl tenant export acme-eu --to ./acme-eu-export \
    --gateway db.example.com:3306 --user acme-eu.admin --password 's3cret-Passw0rd'
```

The tenant becomes `deleting`, and no one can log in. Cleanup then runs in the background:

1. The tenant's shard moves and other mover jobs end.
2. Its databases are dropped.
3. Its schemas and MariaDB accounts are removed from every cluster in the data centre.
4. The catalog schema is dropped.
5. The directory entry and the name are removed. The name can then be used again.

`sprigctl tenant list` shows the tenant until cleanup has finished.

## Not supported yet

- **SQL statements for tenants.** `CREATE TENANT`, `DROP TENANT`, `SHOW TENANTS` and `USE TENANT` don't exist; use `sprigctl` or the Admin API. `ALTER TENANT … SPRIG SET` exists, for settings only.
- **Moving a tenant to another parent.**

## Related

- [Your first tenant](../get-started/first-tenant.md)
- [Tenants and accounts](../sql/tenants-and-accounts.md)
- [Tenant administration](../security/tenant-administration.md)
- [Settings and scopes](settings-and-scopes.md)
- [Shards and table groups](shards-and-table-groups.md)
