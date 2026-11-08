# Connector matrix, sysbench and the latency gate

This directory holds the harness for milestone M0.2's "done when" (chapter 48): the tier 1 SQL connectors and sysbench must pass through the gateway on both ports, and the gateway must add no more than 0.5 ms at p50. The design comes from chapter 46, step 5, and chapter 05, step 13. Today it runs against MariaDB 11.8 and MySQL 8.4 directly, and every check passes there or has an expected outcome in `expected.toml`. Later the same commands point at the gateway's 3306 and 3307.

## Run it

```sh
cd deploy/compose
docker compose --profile single up -d --wait            # MariaDB 11.8 on 13301

connectors/run.sh 127.0.0.1 13301 mariadb               # every connector, ORM and tool
connectors/run.sh --quick 127.0.0.1 13301 mariadb       # drivers + SQLAlchemy, fast checks (CI per PR)
connectors/run.sh 127.0.0.1 13301 mariadb mysql2 pymysql  # a subset

# Or a throwaway MariaDB with a memory cap of its own: the 16 MiB round trips
# grow the server's buffers, and on a full Docker VM the kernel OOM killer
# picks the biggest container (it took the shared single node once)
docker run -d --rm --name sprigdb-mariadb118-baseline --memory 1536m -p 127.0.0.1:13318:3306 \
  -e MARIADB_ROOT_PASSWORD=sprig-dev-root mariadb:11.8 --innodb-buffer-pool-size=64M
connectors/run.sh 127.0.0.1 13318 mariadb
docker stop sprigdb-mariadb118-baseline

# MySQL 8.4 as the 3306 reference (give it 2 GiB: the 16 MiB round trips need ~900 MiB)
docker run -d --rm --name sprigdb-mysql84-baseline --memory 2g -p 127.0.0.1:13384:3306 \
  -e MYSQL_ROOT_PASSWORD=sprig-dev-root mysql:8.4 --innodb-buffer-pool-size=64M
connectors/run.sh 127.0.0.1 13384 mysql
docker stop sprigdb-mysql84-baseline

# The gateway, once it serves the MySQL protocol
connectors/run.sh 127.0.0.1 3306 mysql
connectors/run.sh 127.0.0.1 3307 mariadb
```

`run.sh <host> <port> <persona> [connector...]` builds the images of the `connectors` Compose profile and sets up the target. It then runs each connector in `required` and then `disabled` TLS mode, one container at a time with the `mem_limit` from `compose.yaml`. Finally it prints the matrix and exits non-zero if any check differs from `expected.toml`. The persona (`mysql` or `mariadb`) selects which reference the target stands for. A full run takes about 3 minutes 10 seconds (per connector below), plus the image builds the first time; `--quick` runs the drivers and SQLAlchemy without `large_row`, `multi_packet` and `lock_wait` in about 25 seconds. Per connector and TLS mode on a laptop: each driver 2 to 3 s (the CLIs 8 s: their lock checks hold a lock from a background session), SQLAlchemy 1 s, Hibernate 3 s, Prisma 3 to 4 s, each dump round trip 1 s, sysbench 41 s and sysbench-tpcc 55 s (34 s of it loading one warehouse). The `required` runs of sysbench and sysbench-tpcc are n/a and take no time.

Results go to `results/<persona>-<host>-<port>/` (git-ignored). That directory holds one `<connector>.<tls>.jsonl` file per run with its stderr in a `.log`, plus `summary.json` and `junit.xml`.

Variables: `SPRIG_CONN_USER`, `SPRIG_CONN_PASSWORD` (default `root` / `sprig-dev-root`), `SPRIG_CONN_DATABASE` (`sprig_conn`), `SPRIG_CONN_TLS_MODES`, `SPRIG_CONN_BUILD=0`, `SPRIG_CONN_SETUP=0`, `SPRIG_CONN_RESULTS` and `SPRIG_CONN_EXPECTED_OVERLAY`. The overlay is a TOML file merged over `expected.toml`, for outcomes that belong to one target rather than a persona. `SPRIG_SYSBENCH_*` variables are described in `sysbench/sysbench.sh`.

**Setup.** Unless `SPRIG_CONN_SETUP=0`, `run.sh` creates the database. If the server's `max_allowed_packet` is below 64 MiB, it also raises it with `SET GLOBAL` (on the Compose single node that lasts until the server restarts). The `multi_packet` check needs this. Setup tries TLS first, which also fills `caching_sha2_password`'s cache on MySQL.

**Networking.** The containers reach a loopback target through `host.docker.internal` on Docker Desktop and through the host network on Linux (`SPRIG_CONN_NETWORK_MODE=host`, which `run.sh` sets). A non-loopback host is used as given.

## The connectors

| Connector | Folder | Version | Notes |
|---|---|---|---|
| `mysql-connector-j` | `java/` | MySQL Connector/J 9.7.0 (SHA-256 pinned), Temurin 21 | `useServerPrepStmts=true` |
| `mariadb-connector-j` | `java/` | MariaDB Connector/J 3.5.11 | `useServerPrepStmts=true` |
| `go-sql-driver` | `go/` | go-sql-driver/mysql v1.9.3 (go.sum), Go 1.25.3 | |
| `mysql2` | `node/` | mysql2 3.24.6 (package-lock), Node 22.21.1 | `execute()` for prepared statements |
| `pymysql` | `python/` | PyMySQL 1.2.3 (+ cryptography 46.0.3), Python 3.12 | |
| `mysqlclient` | `python/` | mysqlclient 2.3.0 on Debian's libmariadb 3.4 | |
| `pdo_mysql` | `php/` | PHP 8.4.14 (mysqlnd) | emulated prepares (PDO's default) except in `prepared` |
| `mysqli` | `php/` | PHP 8.4.14 (mysqlnd) | blobs via `send_long_data` |
| `mysql-cli` | `cli/` | `mysql` 8.4.11 | |
| `mariadb-cli` | `cli/` | `mariadb` 11.8.9 | |
| `sysbench` | `sysbench/` | sysbench 1.0.20 (Debian 13.7, libmariadb) | plaintext only |
| `sysbench-tpcc` | `sysbench/` | Percona-Lab sysbench-tpcc at f110afa (SHA-256 pinned), on the same sysbench | plaintext only; 1 warehouse |
| `sqlalchemy-pymysql` | `python/` | SQLAlchemy 2.0.44 over PyMySQL | `mysql+pymysql://` on persona mysql, `mariadb+pymysql://` on mariadb |
| `sqlalchemy-mysqlclient` | `python/` | SQLAlchemy 2.0.44 over mysqlclient | likewise with `mysqldb` |
| `hibernate` | `hibernate/` | Hibernate 6.6.33 via Spring Data JPA (Spring Boot 3.5.7), MySQL Connector/J 9.7.0, Maven 3.9.11 (SHA-512 pinned) | dialect detected, not configured |
| `hibernate-mariadb` | `hibernate/` | the same with MariaDB Connector/J 3.5.11 | |
| `prisma` | `prisma/` | Prisma ORM 6.19.0 (package-lock), Node 22.21.1 | its schema and query engines are Rust MySQL clients of their own |
| `mysqldump` | `cli/` | `mysqldump` 8.4.11, restored with `mysql` 8.4.11 | |
| `mariadb-dump` | `cli/` | `mariadb-dump` 11.8.9, restored with `mariadb` 11.8.9 | |

## The script

Every connector runs the same checks and prints one JSON line per check:

```json
{"connector":"mysql2","tls":"required","check":"types","status":"pass","detail":"17 columns, 2 rows","ms":12.3}
```

The status is `pass`, `fail`, `n/a` (the connector can't do it) or `skip` (the server can't show it).

| Check | What it does |
|---|---|
| `connect` | Connects with the TLS mode and asserts that `Ssl_cipher` is set (`required`) or empty (`disabled`). |
| `plain_query` | `SELECT 1 + 1, 'sprig', VERSION()` |
| `types` | Text protocol: a row with every common type plus an all-NULL row, decoded and compared. The types are TINYINT to BIGINT, BIGINT UNSIGNED max, DECIMAL(20,6), FLOAT, DOUBLE, utf8mb4 VARCHAR and TEXT with emoji and quotes, VARBINARY and BLOB with every byte value, DATE, DATETIME(6), TIMESTAMP, TIME `838:59:59` and JSON. |
| `prepared` | Binary protocol: a prepared INSERT with a parameter of each type, executed again with all NULLs. Then a prepared SELECT, executed twice, compares both rows. The CLIs use SQL `PREPARE`/`EXECUTE`. |
| `transaction` | Insert and roll back (the row is gone), insert and commit (another session sees it). |
| `multi_statement` | `SELECT 1; SELECT 2, 'two'` in one query, both resultsets read. The CLIs switch the delimiter to `$$` so both statements go in one `COM_QUERY`. |
| `duplicate_key` | Asserts errno 1062 and SQLSTATE 23000. |
| `last_insert_id` | The OK packet's insert id after a single-row and a three-row insert (100, then 101), and `LAST_INSERT_ID()`. The CLIs check `LAST_INSERT_ID()` only. |
| `large_row` | A 4 MiB random blob: upload, server-side SHA-256, download, byte comparison. |
| `multi_packet` | The same with 16 MiB + 1 KiB, so both directions span two protocol packets (the 4 MiB row fits in one). |
| `compression` | A compressed session (`Compression` = ON), a 1 MB `REPEAT`, a 1 MiB round trip. |
| `conn_attrs` | The client's connection attributes appear in `performance_schema.session_connect_attrs`. Connectors that can add attributes send `sprig_check`; the others' own `_client_name` or `program_name` is checked. |
| `prepared_biginteger` | Java only: BIGINT UNSIGNED max bound as a `BigInteger`. |
| `local_infile` | `LOAD DATA LOCAL INFILE` of a 3-row CSV (UTF-8, a quoted comma, `\N`) with the driver's local-infile switch on; the rows must match. Skip if the server has `local_infile` off (`run.sh` turns it on: MySQL 8.4 ships it off). |
| `local_infile_off` | The same with the switch off: the load must fail and load nothing. When the server refuses it, the errno must be the persona's: 3948 (MySQL) or 4166 (MariaDB). Some drivers refuse on their own side instead (detail says which). |
| `call` | `CALL` of a procedure that returns two resultsets, then sets an OUT and an INOUT parameter. Java uses `CallableStatement`; go, mysqli and mysql2 a prepared `CALL` (mysql2 also the text one); the rest a text `CALL` and `SELECT @b, @c`. |
| `change_user` | `COM_CHANGE_USER` to the same account: the connection id stays, and a user variable, a session variable and a temporary table are gone. |
| `reset_connection` | `COM_RESET_CONNECTION`, with the same assertions. |
| `pool_reprepare` | A one-connection pool runs a server-side prepared statement; another session `KILL`s the pooled connection; the pool must hand out a new connection and the statement is prepared on it again. HikariCP 6.3.0 for Java, `database/sql` for go, mysql2's pool, PHP persistent connections. |
| `lock_wait` | `SELECT ... FOR UPDATE` in a transaction, a second session with `innodb_lock_wait_timeout = 1` waits on the row: errno 1205 after about a second, and only the statement is rolled back (its earlier lock and a later UPDATE commit). |
| `lock_nowait` | `FOR UPDATE NOWAIT` on the locked row fails with the persona's errno (3572 on MySQL, 1205 on MariaDB); `FOR UPDATE SKIP LOCKED` returns only the free row. |

sysbench reports `prepare`, `point_select_ps`, `point_select_text`, `read_write_ps`, `read_write_text` and `cleanup`. That is oltp_point_select and oltp_read_write, each with `--db-ps-mode=auto` (server-side prepared statements) and `disable`, on 2 tables of 10,000 rows with 4 threads for 10 seconds each. The summary shows their tps and p95.

## The ORMs and tools

Each reports its own checks (`[check_sets]` in `expected.toml`).

| Connector | Checks |
|---|---|
| `sqlalchemy-*` | `connect` (TLS, and the dialect's `is_mariadb` must match the persona), `core` (15 types through a `Table`, executemany, update, delete), `orm` (a relationship graph, `selectinload`, delete-orphan, cascade), `transaction` (rollback, `SAVEPOINT` rollback, commit), `reflection` (columns, types, PK, unique, index, FK with `ON DELETE`, comments), `pool_reconnect` (`pool_pre_ping` after a `KILL`) |
| `hibernate*` | `connect` (also creates the schema with SQL), `schema_validate` (`ddl-auto=validate` against it: identity, DECIMAL, DATETIME(6), DATE, BIT, ENUM, LONGBLOB, JSON, FK), `dialect` (detected dialect must match the persona), `crud`, `query` (derived, JPQL with join and GROUP BY, paging, native), `transaction`, `optimistic_lock` (`@Version`). If validation fails, Spring is restarted without it so the rest still run. |
| `prisma` | `db_pull` (tables created with `prisma db execute`, then introspected: types, enum, index, relation with `onDelete`), `generate`, `connect`, `query` (nested create, include, relation filter, update, aggregate, `$queryRaw`, cascade), `transaction` (interactive rollback, batch commit) |
| `mysqldump`, `mariadb-dump` | `source` (3 tables, 1,003 rows of 30 column types incl. BIT, ENUM, SET, YEAR, POINT, JSON, a generated column; a trigger, a view, a procedure, a function), `dump` (`--single-transaction --routines --triggers --events --hex-blob`), `restore` (into a new schema), `checksum` (`CHECKSUM TABLE` and counts equal), `objects` (`SHOW CREATE` equal; view, procedure, function and trigger work) |
| `sysbench-tpcc` | `prepare`, `run` (20 s, 2 threads, server-side prepares; the detail counts errors sysbench ignores: 1213, 1205, 1020), `check` (tpcc_check.lua), `cleanup` |

## Expected outcomes

`expected.toml` lists every outcome other than `pass`, by persona and connector, with the reason. A check passes the matrix only when its status equals the expected one, so an unexpected pass fails too and the file stays accurate. An expected `fail` is a known incompatibility of the reference server itself, and the gateway must reproduce it. Today's entries:

- PyMySQL: `prepared` and `compression` are n/a (no binary protocol, no compressed protocol). mysqlclient: `prepared` is n/a.
- `change_user`, `reset_connection` and `pool_reprepare` are n/a where the connector has no API for them; `expected.toml` lists each. Notably go-sql-driver's `ResetSession` (database/sql's hook) only checks liveness and never sends `COM_RESET_CONNECTION`.
- sysbench and sysbench-tpcc in `required` mode are n/a. sysbench 1.0's `--mysql-ssl=on` loads `client-key.pem`, `client-cert.pem` and `cacert.pem` from its working directory and verifies the server with that CA, so it can't encrypt without verifying.
- Persona `mariadb`: `conn_attrs` is a skip, because MariaDB ships with `performance_schema` off.
- SQLAlchemy over mysqlclient, `required`: `pool_reconnect` fails on both servers. After the `KILL`, libmariadb reports 2026 ("TLS/SSL error: unexpected eof"), which SQLAlchemy 2.0.44 doesn't treat as a disconnect, so `pool_pre_ping` raises instead of reconnecting. A gateway that sends TLS close_notify on `KILL` may turn this into a pass (2013), which the matrix would show.
- Persona `mysql`: MariaDB Connector/J's `reset_connection` is n/a. Its `reset()` sends `COM_RESET_CONNECTION` only when the server's version says MariaDB, so it never does on 3306.
- Persona `mysql`: `mariadb-dump`'s `restore`, `checksum` and `objects` fail. mariadb-dump 11.8 writes values for MySQL's virtual generated columns, and MySQL rejects them (3105).
- Persona `mariadb`: `mysqldump`'s `dump` fails with the usual options. mysqldump 8.4 reads `information_schema.COLUMN_STATISTICS` and can't set `terminology_use_previous` for events. With `--column-statistics=0` it works, and the rest of the round trip passes.
- Persona `mariadb`: Hibernate's `schema_validate` fails with both drivers, for two reasons. With Connector/J 9.7, `DatabaseMetaData.getSQLKeywords()` reads `information_schema.KEYWORDS WHERE RESERVED = 1`, which MariaDB lacks, so Hibernate can't even resolve a dialect. With MariaDB Connector/J, MariaDBDialect expects a `json` column but MariaDB's JSON is LONGTEXT. The other Hibernate checks pass after a restart without validation.
- Persona `mariadb`: MySQL Connector/J's `prepared_biginteger` fails. With server-side prepares, Connector/J 9.7 binds a `BigInteger` above `Long.MAX_VALUE` in a form MariaDB 11.8 rejects (1264, out of range). MySQL 8.4 accepts it, and client-side prepares work on both. The 3306 persona, which runs on MariaDB, must still pass it.

## The latency gate

`crates/sprig-testkit/tests/latency_gate.rs` (ignored by default) measures three workloads on one connection per endpoint: a prepared point select, the same point select as a plain query, and a prepared single-row insert. It interleaves target and baseline in batches of 100 and reports p50, p99 and max. It fails when a p50 delta exceeds `SPRIG_TEST_LATENCY_MAX_P50_DELTA_MS` (0.5), or a p99 delta exceeds `SPRIG_TEST_LATENCY_MAX_P99_DELTA_MS` when that is set.

```sh
SPRIG_TEST_LATENCY_TARGET=mysql://root:sprig-dev-root@127.0.0.1:3307 \
SPRIG_TEST_LATENCY_BASELINE=mysql://root:sprig-dev-root@127.0.0.1:13301 \
  cargo test -p sprig-testkit --test latency_gate -- --ignored --nocapture
```

`crates/sprig-testkit/tests/connectors_matrix.rs` (ignored by default) runs `run.sh` once per endpoint of `SPRIG_TEST_CONNECTORS`. `SPRIG_TEST_CONNECTORS_ONLY` limits the run to a subset of connectors.

```sh
SPRIG_TEST_DOCKER=1 SPRIG_TEST_CONNECTORS=127.0.0.1:3306:mysql,127.0.0.1:3307:mariadb \
  cargo test -p sprig-testkit --test connectors_matrix -- --ignored --nocapture
```

CI has the job `connectors` (M0.2) in `.github/workflows/ci.yml`. It runs both tests against the gateway and stays disabled until the gateway exists.

## Differences between the references worth knowing

Checks that pass on both references but assert persona-specific answers, because the gateway's 3306 runs on MariaDB backends and must give the MySQL one:

- `local_infile_off`: a server refusal is 3948 on MySQL and 4166 on MariaDB. MySQL 8.4 also ships with `local_infile` off.
- `lock_nowait`: `NOWAIT` on a locked row is 3572 on MySQL and 1205 on MariaDB.
- `sqlalchemy-*` `connect`: SQLAlchemy decides MariaDB-or-not from the version string, and `mariadb://` URLs refuse a non-MariaDB server.
- `sqlalchemy-*` `reflection` and `prisma` `db_pull`: MySQL's JSON reflects as JSON (`Json` in Prisma); MariaDB's as LONGTEXT (`String @db.LongText`). An FK without `ON UPDATE` reads back as NO ACTION on MySQL and RESTRICT on MariaDB.
- `hibernate-mariadb` `dialect`: Hibernate picks MariaDBDialect for MariaDB Connector/J even against MySQL 8.4 (as "MariaDB 8.4.11").
- MySQL Connector/J reports SQLSTATE 40001 for 1205; the others report the server's HY000.
- sysbench `read_write` and sysbench-tpcc: MariaDB 11.8 has a few hundred ignored errors per run (`innodb_snapshot_isolation` makes conflicting REPEATABLE READ writes fail with 1020, alongside deadlocks); MySQL 8.4 has none for `read_write` and a handful (deadlocks) for tpcc.

## Not covered yet

Golden captures and fuzz targets (the rest of chapter 05's step 13), and the `optional` TLS mode of the listeners: both wait for the gateway. Prisma 7 (driver adapters over the `mariadb` npm package) is not in the matrix; Prisma 6 covers the Rust engines.
