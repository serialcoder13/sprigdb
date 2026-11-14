# Tenant harness: two tenants, schema tools, leaks and settings

This directory holds the tenant harness. It checks that:

1. Two tenants on one cluster each run the same schema-tool suite (Flyway, Hibernate schema validation, `mysqldump`) and see only their own objects.
2. No physical name appears in any result, error or `SHOW` output in the leak suite.
3. A setting changed at object scope takes effect on every gateway within seconds, and a value outside its bounds is refused.

The harness reuses the connector matrix's patterns and images (`../connectors/README.md`).

Until the gateway serves these tenants, the harness runs in **baseline** mode against direct MariaDB. There the two tenants are two plain MariaDB users, each with grants on its own database prefix. That proves the scripts. The leak verdicts are n/a in baseline, because no physical names exist.

## Run it

```sh
cd deploy/compose
docker compose --profile single up -d --wait     # etcd + MariaDB 11.8 on 13301

# Baseline: direct MariaDB. Creates the users m03_acme and m03_globex first (as root)
SPRIG_M03_MODE=baseline m03/run.sh 127.0.0.1 13301 mariadb m03_acme m03_globex sprig-dev-app
SPRIG_M03_MODE=baseline m03/run.sh 127.0.0.1 13301 mysql   m03_acme m03_globex sprig-dev-app

# The gateway (gateway-dev.sh up; tenants acme and globex on dc-a-single)
m03/run.sh 127.0.0.1 13307 mariadb acme.app globex.app sprig-dev-app
m03/run.sh 127.0.0.1 13306 mysql   acme.app globex.app sprig-dev-app

# The same, with the leak suite probing the cluster's real physical schema names
SPRIG_M03_BACKEND=mysql://root:sprig-dev-root@127.0.0.1:13301 \
  m03/run.sh 127.0.0.1 13307 mariadb acme.app globex.app sprig-dev-app
```

`run.sh <host> <port> <persona> <tenantA-user> <tenantB-user> <password>` takes these steps, for both tenants each time:

1. **login**: a TLS login (it fills `caching_sha2_password`'s cache).
2. **reset**: drop the tenant's suite databases.
3. **flyway**: `migrate`, `info` and `validate`.
4. **seed**: the tenant's own rows, a private table and a private database, then the Flyway history compared across tenants.
5. **hibernate**: Spring Data JPA with `ddl-auto=validate`.
6. **dump**: `mysqldump` on persona `mysql`, `mariadb-dump` on `mariadb`.
7. **isolation**: the isolation checks.
8. The Rust tests `m03_leaks` and `m03_settings`, through `cargo test`.

It runs one container at a time, each with the `mem_limit` of the `m03` Compose profile. Then it prints the matrix and exits non-zero if any check differs from `expected.txt`.

A baseline run takes about 15 to 20 seconds once the images exist (`cargo test` builds the two test binaries the first time). Results go to `results/<mode>-<persona>-<host>-<port>/` (git-ignored): one `<stage>.jsonl` per stage with its stderr in `<stage>.log`, Flyway's JSON per tenant and operation, and the Rust tests' full output in `m03_*.out`.

### Variables

| Variable | Default | Meaning |
|---|---|---|
| `SPRIG_M03_MODE` | `gateway` | `gateway`, or `baseline` |
| `SPRIG_M03_SETUP` | `1` | In baseline mode, create the two users and their grants first |
| `SPRIG_M03_ROOT_USER`, `SPRIG_M03_ROOT_PASSWORD` | `root`, `sprig-dev-root` | The account baseline setup uses |
| `SPRIG_M03_TLS` | `disabled` | `disabled` or `required`, for the tools. The leak suite reads raw bytes, so it always uses plaintext |
| `SPRIG_M03_SUITES` | all | A subset of `flyway hibernate dump isolation leaks settings` |
| `SPRIG_M03_BUILD` | `1` | `0` skips building the images |
| `SPRIG_M03_RESULTS` | as above | The results directory |
| `SPRIG_M03_EXPECTED_OVERLAY` | none | More lines in `expected.txt`'s format, for one target's own outcomes |
| `SPRIG_M03_OBSERVERS` | none | More observer URLs for the settings check (for example a second gateway) |
| `SPRIG_M03_BACKEND` | none | A direct root URL of the cluster: the leak suite probes its real `t<id>_...` schema names |
| `SPRIG_M03_BACKEND_USERS` | `root` | Backend accounts that must never appear as a `DEFINER` or as Flyway's `installed_by` |
| `CARGO_TARGET_DIR` | cargo's | Passed through to `cargo test` |

The containers reach a loopback target as `connectors/run.sh`'s do: through `host.docker.internal` on Docker Desktop, and through the host network on Linux.

### Names

A tenant's **tag** is its user name with every character other than `[A-Za-z0-9]` replaced by `_` (`acme.app` becomes `acme_app`). Through the gateway, both tenants use the same logical names: `shop`, `crm`, `shop_restored`, `m03_leaks` and `m03_settings`. Each tenant also has databases only it owns, `vault_<tag>` and `m03_leaks_vault_<tag>`.

In baseline mode, every database name gets the prefix `<tag>_` (for example `m03_acme_shop`). The two users have `GRANT ALL ON <tag>\_%.*` and nothing else. Flyway's placeholders `${shop}` and `${crm}` carry the names, so the migrations' text, and therefore their checksums, are the same in both modes.

## What each part checks

### Flyway (`flyway/sql`, image `flyway/flyway:13.8.1`)

There are six migrations, run by both tenants into the same logical names:

| Migration | Creates |
|---|---|
| V1 | `crm.customers` |
| V2 | `shop.products` and `shop.orders`, with an FK inside `shop` |
| V3 | An FK from `shop.orders` to `crm.customers`: across two databases of the same tenant |
| V4 | An index, and the view `shop.order_totals`, which joins both databases |
| V5 | The procedure `shop.place_order`, with `DELIMITER` |
| V6 | Seed rows, inserted partly through the procedure |

Flyway creates both databases itself (`-schemas=shop,crm`) and keeps its history in `shop.flyway_schema_history`. The checks are:

- **migrate:** 6 migrations executed.
- **info:** 6 versioned migrations, all `Success`, at schema version 6.
- **validate:** passes.
- **history**, run from the CLI image: both tenants' history tables hold the same versions with the same checksums. `installed_by` (Flyway's `CURRENT_USER()`) must never be a backend account.

Flyway 13 ships MariaDB Connector/J 2.7 and uses it for `jdbc:mysql:` URLs too.

### Hibernate (`hibernate/`, image `sprigdb-m03-hibernate:dev`)

This is Spring Data JPA (Spring Boot 3.5.7, Hibernate 6.6.33) with `ddl-auto=validate` against the schema Flyway created. The entities are `crm.customers`, `shop.products`, `shop.orders` (two FKs, one into `crm`) and the view `shop.order_totals`. The Dockerfile builds with the connector matrix's own `pom.xml` (the `conn` build context), so Docker's layer cache shares the dependency download with `conn-hibernate`.

| Check | What it does |
|---|---|
| `connect` | JDBC login; `DATABASE()` must be the logical `shop` |
| `schema_validate` | `ddl-auto=validate` for the 3 tables and the view. If it fails, Spring restarts without validation so the rest still run |
| `dialect` | The dialect Hibernate uses; MariaDB Connector/J must get `MariaDBDialect` (set explicitly) |
| `read` | All customers: the seeds and the tenant's own marker, never the other tenant's |
| `write` | A JPA insert, `CALL shop.place_order` through `StoredProcedureQuery`, a JPQL join, and the cross-database view |
| `native_query` | `SELECT DATABASE()` and an unqualified `SELECT` |

Persona `mariadb` runs `hibernate-mariadb` (MariaDB Connector/J 3.5.11). Persona `mysql` runs it too, plus `hibernate` (MySQL Connector/J 9.7.0), whose `schema_validate` is an expected failure in baseline mode only (below).

### Dumps (`sql/suite.sh dump`, the connector matrix's CLI images)

Persona `mysql` uses `mysqldump` 8.4 and persona `mariadb` uses `mariadb-dump` 11.8. The options are `--single-transaction --routines --triggers --hex-blob --no-tablespaces`. For `mysqldump` they add `--column-statistics=0 --set-gtid-purged=OFF`: `COLUMN_STATISTICS` is the translator's job, and GTIDs and tablespaces need privileges a tenant doesn't have.

| Check | What it asserts |
|---|---|
| `dump_databases` | `--databases shop crm vault_<tag>`: `CREATE DATABASE` names exactly those three, under logical names. The tables are exactly the tenant's six. The view, the procedure and `` REFERENCES `crm`.`customers` `` are there. The tenant's own rows are there, and none of the other tenant's rows, table or database |
| `dump_leaks` | No physical name anywhere in that dump |
| `dump_definers` | No `DEFINER` names a backend account |
| `dump_all` | `--all-databases`: the tenant's own databases, nothing of the other tenant's, and no physical name |
| `dump_listed` | The workaround for `mysqldump`'s `--all-databases` (below): `--databases` with every database `SHOW DATABASES` lists, the system schemas left out. The same assertions as `dump_all` |
| `restore` | A dump of `shop` alone (no `CREATE DATABASE` or `USE`), restored into the new logical database `shop_restored` of the same tenant. Row counts and `SHOW CREATE TABLE` are equal for 4 tables, and the procedure and the view are there |

### Isolation (`sql/suite.sh isolation`)

Each tenant runs these checks in turn. Every answer is also scanned for physical names.

| Check | What it asserts |
|---|---|
| `show_databases` | Lists its own databases, not the other tenant's vault |
| `show_tables` | `SHOW TABLES FROM shop` gives exactly its five tables (its `only_<tag>`, not the other tenant's) |
| `information_schema` | `TABLES`, `COLUMNS` (`secret_<tag>`), `SCHEMATA` and `KEY_COLUMN_USAGE` hold only its own objects. The cross-database FK reads `shop.orders -> crm.customers` |
| `select_rows` | Its own customers and secrets, none of the other tenant's |
| `other_tenant` | Six statements naming the other tenant's database, table or column all fail |
| `isolation_leaks` | No physical name in any of the above |

### The leak suite (`crates/sprig-testkit/tests/m03_leaks.rs`)

This is a Rust test (ignored by default, gated on environment variables like the repository's other live tests). Each tenant's connections go through a TCP proxy inside the test, and every byte the server sends is scanned with the physical-name detector: the handshake, OK and error packets, column definitions, rows and session-state changes. The detector is `sprig_sql::names::physical_name_leaks`, ported byte for byte, with a unit test against its examples. Column definitions (`schema`, `table`, `org_table`, `name`, `org_name`) are also checked field by field. Each tenant runs:

- **Setup:** a database with FK-linked tables, a unique key, a trigger, a view, a procedure, a function, a view whose table is dropped, and a private vault holding a secret marker.
- **Results:** `USE`, `DATABASE()`, unqualified and joined `SELECT`, the view, the function, `CALL` with an OUT parameter, and `CURRENT_USER()`.
- **`SHOW`:** `DATABASES`, `[FULL] TABLES`, `TABLE STATUS`, `[FULL] COLUMNS` and `INDEX`; `CREATE TABLE`, `VIEW`, `PROCEDURE`, `FUNCTION`, `TRIGGER` and `DATABASE`; `PROCEDURE` and `FUNCTION STATUS`, `TRIGGERS`, `OPEN TABLES` and `GRANTS`.
- **`information_schema`:** `SCHEMATA`, `TABLES`, `COLUMNS`, `STATISTICS`, `KEY_COLUMN_USAGE`, `REFERENTIAL_CONSTRAINTS`, `TABLE_CONSTRAINTS`, `VIEWS`, `ROUTINES`, `PARAMETERS`, `TRIGGERS`, `PARTITIONS` and `SCHEMA_PRIVILEGES`.
- **`EXPLAIN`:** plain, `EXTENDED` and `FORMAT=JSON`. With warnings on, the rewritten statement comes back as note 1003.
- **Warnings**, each followed by `SHOW WARNINGS`: `INSERT IGNORE` of a duplicate, and `DROP ... IF EXISTS` and `CREATE ... IF NOT EXISTS` notes.
- **Errors caused on purpose:**
  - unknown table, column, qualified column (`db.t.c`), procedure, function and database;
  - duplicate primary and unique keys;
  - FK violations on insert, delete and update;
  - table exists, drop unknown, `SHOW CREATE` of an unknown table;
  - the broken view (1356);
  - syntax errors that echo the statement;
  - an ambiguous column;
  - `information_schema.SPRIG_SETTINGS` inside `CREATE VIEW` and `CREATE TABLE ... SELECT` (39051 through the gateway; reading it is fine).
- **The other tenant's names:** its vault (use, select, `SHOW TABLES FROM`) and its database.
- **Prepared statements:** the prepare response carries column definitions, and a prepare fails on an unknown table.
- **Physical names typed by hand:** `USE`, `SELECT`, `SHOW TABLES FROM` and a backquoted form. The names are the cluster's real `t<id>_...` schemas with `SPRIG_M03_BACKEND`, or made-up ones otherwise. The reply must not echo them.
- **`PROCESSLIST`:** while both tenants have a statement running, `SHOW [FULL] PROCESSLIST` and `information_schema.PROCESSLIST`.
- **`KILL`:** each tenant tries to kill the other's session.

It reports these checks per tenant:

| Check | What it asserts |
|---|---|
| `leaks` | No physical name in any byte |
| `outcomes` | Each statement succeeded or failed as intended |
| `other_secret` | The other tenant's secret marker is in no reply |
| `own_rows` | `SELECT` shows the tenant's own rows only |
| `processlist` | The other tenant's running statement isn't listed. The detail says whether the tenant's own statement was listed, which shows that the `Info` column was scanned |
| `kill_other` | The `KILL` is refused, and the victim's session still works |

In baseline mode, `leaks` is n/a. The test asserts instead that the detector caught the probe names MariaDB echoes in its errors (16 replies per tenant), which proves the byte capture end to end. The function and the trigger are left out in baseline: with binary logging on, MariaDB refuses them (1419) to a user without SUPER, and turning on `log_bin_trust_function_creators` would change a shared server.

```sh
SPRIG_TEST_M03_A=mysql://acme.app:sprig-dev-app@127.0.0.1:13307 \
SPRIG_TEST_M03_B=mysql://globex.app:sprig-dev-app@127.0.0.1:13307 \
  cargo test -p sprig-testkit --test m03_leaks -- --ignored --nocapture
```

Its variables are `SPRIG_TEST_M03_BASELINE=1`, `SPRIG_TEST_M03_BACKEND` (the real probe names) and `SPRIG_TEST_M03_REPORT` (a file for its JSON lines).

### Settings (`crates/sprig-testkit/tests/m03_settings.rs`)

This test uses the `SPRIG SET` settings statements and the key `reads.max_replica_lag_ms`, whose scopes are all six and whose hard bounds are 0 to 86,400,000. Tenant A creates `m03_settings.hot` and `.cold`, then:

| Check | What it asserts |
|---|---|
| `object_scope` | After `ALTER TABLE m03_settings.hot SPRIG SET (reads.max_replica_lag_ms = <v>)` (`<v>` from 1001 to 51001, never the default 1000 or the value read before), every observer reads `<v>` with an object-scope source within 5 s. The observers are a second session on the same endpoint, plus each URL in `SPRIG_TEST_M03_OBSERVERS`. They read `information_schema.SPRIG_SETTINGS` (`VALUE`, `SOURCE_SCOPE`), or `SHOW SPRIG SETTINGS LIKE ... FOR TABLE` if the view isn't there. The detail gives the latency per observer |
| `object_only` | `m03_settings.cold` doesn't read `<v>` |
| `out_of_bounds` | `SET (... = 86400001)` is refused with 39080 (`ER_SPRIG_SETTING_OUT_OF_BOUNDS`), and the value stays `<v>` |
| `scope_bounds` | `ALTER DATABASE m03_settings SPRIG SET BOUNDS (... MAX <v+1000>)`, then a table value of `<v+2000>`, is refused with 39080. Then `RESET BOUNDS` |

`SPRIG_TEST_M03_SETTING_KEY` and `SPRIG_TEST_M03_SETTING_MAX` choose another key, `SPRIG_TEST_M03_WITHIN_S` the deadline, and `SPRIG_TEST_M03_SCOPE_BOUNDS=0` leaves out `scope_bounds`. In baseline mode, the statement must fail as a syntax error (MariaDB answers 1064), and every check reports n/a.

**A second gateway.** "On every gateway" needs a second gateway process, which is optional here. When one runs on other ports, pass its URL:

```sh
SPRIG_M03_OBSERVERS=mysql://acme.app:sprig-dev-app@127.0.0.1:13317 \
  m03/run.sh 127.0.0.1 13307 mariadb acme.app globex.app sprig-dev-app
```

Today, `gateway-dev.sh` can't start one next to the first on the same etcd. The listener addresses are dc-scope settings, so a second gateway in `dc-a` would try to bind the same ports. See "What the gateway must provide" below.

## Expected outcomes

`expected.txt` lists every outcome other than `pass`, by mode, persona, suite, connector and check, with the reason. It works like `../connectors/expected.toml`: an unexpected pass counts as a difference too. Today's entries are:

- **Hibernate with MySQL Connector/J, `schema_validate`: fail (baseline only).** `DatabaseMetaData.getSQLKeywords()` reads `information_schema.KEYWORDS WHERE RESERVED = 1`, which MariaDB lacks, so Hibernate can't resolve a dialect. The rest of the check passes after a restart without validation. Through the gateway's 3306, the translator answers `KEYWORDS` with MySQL 8.4's list and the check passes.
- **`mysqldump`'s `dump_all` on persona `mysql`: fail (both modes).** `mysqldump` 8.4 dumps the `mysql` database first. It holds back every database `SHOW DATABASES` lists until it reaches `mysql`, and if `mysql` never comes it writes nothing and still exits 0. Neither a tenant (through either port) nor a plain MariaDB user sees `mysql`, and MySQL 8.4 does the same for a user who can't see it. `dump_listed` checks the workaround, `--databases` with the `SHOW DATABASES` list.
- **Baseline only:**
  - The leak checks are n/a: no physical names exist.
  - The settings checks are n/a: MariaDB has no `SPRIG SET`.

## Results (8 November 2026, MariaDB 11.8.9 on 13301)

| Mode | Persona | Checks | Pass | Expected fail | n/a | Differences |
|---|---|---|---|---|---|---|
| baseline | `mariadb` | 66 | 56 | 0 | 10 | 0 |
| baseline | `mysql` | 78 | 64 | 4 | 10 | 0 |
| gateway (13307) | `mariadb` | 66 | 66 | 0 | 0 | 0 |
| gateway (13306) | `mysql` | 78 | 74 | 4 | 0 | 0 |

On 8 November 2026 (a gateway on 16306 and 16307 in catalog mode, tenants `translator` and `translatorb`): persona `mysql` 78 checks, 76 pass, 2 expected failures (`dump_all`), 0 differences; persona `mariadb` 66 checks, 66 pass. Hibernate with MySQL Connector/J now validates on 3306.

In baseline the n/a checks are `dump_leaks`, `isolation_leaks` and the leak suite's `leaks` for each tenant, plus the four settings checks. The expected failures on persona `mysql` are Connector/J's `schema_validate` (baseline only) and `mysqldump`'s `dump_all`, once per tenant. The gateway runs used `SPRIG_M03_BACKEND`. A full run takes 15 to 25 s.

## Memory and containers

The Docker VM is small and shared. Each tool runs in its own `docker compose run --rm` container, one at a time:

| Service | Image | `mem_limit` | JVM heap |
|---|---|---|---|
| `m03-flyway` | `flyway/flyway:13.8.1` | 384 MiB | `-Xmx160m` |
| `m03-hibernate` | `sprigdb-m03-hibernate:dev` | 512 MiB | `-Xmx256m` |
| `m03-mysql-cli`, `m03-mariadb-cli` | the connector matrix's CLI images | 256 MiB | none |

The Rust tests run on the host.

## What the gateway must provide

These come on top of the contract (tenants `acme` and `globex` on the single node; users `acme.app` and `globex.app` with password `sprig-dev-app`; ports 13306 and 13307):

- **Plaintext logins with `caching_sha2_password`.** Today the gateway doesn't answer the client's request for the RSA public key, and `mariadb --skip-ssl` fails with 2061. `run.sh` therefore logs in once over TLS first, which fills the cache, as `connectors/run.sh` does. The leak suite needs plaintext to read the bytes, so the cache must hold until it runs. The fix in the gateway is to answer the public-key request, or to accept `mysql_native_password` for these users.
- **TLS `optional`** on both listeners (`gateway-dev.sh`'s default).
- **Tenant users can run DDL.** `CREATE` and `DROP DATABASE` (Flyway creates `shop` and `crm`; the suites create and drop their own databases), tables, views, procedures, functions and triggers.
- **Two gateways for "every gateway"** (optional). For example, a per-process listener override (`SPRIGDB_*` or config), so that `gateway-dev.sh` can start `gw-2` in `dc-a` on 13316 and 13317.
- **`information_schema.SPRIG_SETTINGS`** with `TABLE_SCHEMA`, `TABLE_NAME`, `SETTING_NAME`, `VALUE` and `SOURCE_SCOPE`, or `SHOW SPRIG SETTINGS ... FOR TABLE`. `VALUE` must compare equal to the number as text, with surrounding quotes allowed.

## CI

The job `m03` in `.github/workflows/ci.yml` runs the baseline, starts the gateway, creates the tenants, and runs both ports. It stays disabled (`if: false`) until the gateway serves these tenants.
