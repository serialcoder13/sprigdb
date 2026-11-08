# Connector matrix, sysbench and the latency gate

This directory holds the harness for milestone M0.2's "done when" (chapter 48): the tier 1 SQL connectors and sysbench must pass through the gateway on both ports, and the gateway must add no more than 0.5 ms at p50. The design comes from chapter 46, step 5, and chapter 05, step 13. Today it runs against MariaDB 11.8 and MySQL 8.4 directly, and every check passes there or has an expected outcome in `expected.toml`. Later the same commands point at the gateway's 3306 and 3307.

## Run it

```sh
cd deploy/compose
docker compose --profile single up -d --wait            # MariaDB 11.8 on 13301

connectors/run.sh 127.0.0.1 13301 mariadb               # every connector, then sysbench
connectors/run.sh 127.0.0.1 13301 mariadb mysql2 pymysql  # a subset

# MySQL 8.4 as the 3306 reference (give it 2 GiB: the 16 MiB round trips need ~900 MiB)
docker run -d --rm --name sprigdb-mysql84-baseline --memory 2g -p 127.0.0.1:13384:3306 \
  -e MYSQL_ROOT_PASSWORD=sprig-dev-root mysql:8.4
connectors/run.sh 127.0.0.1 13384 mysql
docker stop sprigdb-mysql84-baseline

# The gateway, once it serves the MySQL protocol
connectors/run.sh 127.0.0.1 3306 mysql
connectors/run.sh 127.0.0.1 3307 mariadb
```

`run.sh <host> <port> <persona> [connector...]` builds the images of the `connectors` Compose profile and sets up the target. It then runs each connector in `required` and then `disabled` TLS mode, one container at a time with the `mem_limit` from `compose.yaml`. Finally it prints the matrix and exits non-zero if any check differs from `expected.toml`. The persona (`mysql` or `mariadb`) selects which reference the target stands for. A full run takes about 80 seconds, of which sysbench takes 40.

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

sysbench reports `prepare`, `point_select_ps`, `point_select_text`, `read_write_ps`, `read_write_text` and `cleanup`. That is oltp_point_select and oltp_read_write, each with `--db-ps-mode=auto` (server-side prepared statements) and `disable`, on 2 tables of 10,000 rows with 4 threads for 10 seconds each. The summary shows their tps and p95.

## Expected outcomes

`expected.toml` lists every outcome other than `pass`, by persona and connector, with the reason. A check passes the matrix only when its status equals the expected one, so an unexpected pass fails too and the file stays accurate. An expected `fail` is a known incompatibility of the reference server itself, and the gateway must reproduce it. Today's entries:

- PyMySQL: `prepared` and `compression` are n/a (no binary protocol, no compressed protocol). mysqlclient: `prepared` is n/a.
- sysbench in `required` mode is n/a. sysbench 1.0's `--mysql-ssl=on` loads `client-key.pem`, `client-cert.pem` and `cacert.pem` from its working directory and verifies the server with that CA, so it can't encrypt without verifying.
- Persona `mariadb`: `conn_attrs` is a skip, because MariaDB ships with `performance_schema` off.
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

## Not covered yet

The matrix doesn't cover the other chapter 05 matrix items: `LOCAL INFILE`, `CALL`, change user and reset, and prepared statements re-bound across pooled connections. Nor does it include the rest of chapter 46's tier 1 SQL list: Hibernate, SQLAlchemy, `mysqldump`, `mariadb-dump` and sysbench-tpcc. Each would be one more check function per program, or one more folder.
