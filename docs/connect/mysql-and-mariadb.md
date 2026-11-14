---
title: MySQL and MariaDB
audience: [user, dba]
sidebar:
  order: 2
---

# MySQL and MariaDB

SprigDB speaks the MySQL client protocol on two ports. Port 3306 behaves as MySQL 8.4 and port 3307 behaves as MariaDB. Use whichever matches the driver and SQL dialect your application already has: the data is the same, only the persona differs.

## The two personas

| | 3306 (`mysql` listener) | 3307 (`mariadb` listener) |
|---|---|---|
| Greeting version | `gateway.mysql_server_version`, default `8.4.8-SprigDB` | `gateway.mariadb_server_version`, default `11.8.3-MariaDB-SprigDB` |
| Default collation in the greeting | `utf8mb4_0900_ai_ci` (255) | `utf8mb4_general_ci` (45) |
| Longest user name (the part after the tenant) | 32 characters | 80 characters |
| Compression | zlib and zstd | zlib |
| Query attributes (`CLIENT_QUERY_ATTRIBUTES`) | yes | no |
| `COM_STMT_BULK_EXECUTE` and extended metadata | no | yes |
| SQL dialect and error numbers | MySQL 8.4's | MariaDB's |

Both personas store data in MariaDB. On 3306 SprigDB translates MySQL 8.4 syntax and answers in MySQL's words, for example `ONLY_FULL_GROUP_BY` semantics, `information_schema` columns that MySQL tools read, and MySQL error numbers such as 3118 where MariaDB would send 4151. See [SQL overview](../sql/overview.md) for the statement-level differences.

## Enable the listeners

Both listeners are on by default and require TLS.

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `listener.mysql.enabled` | bool | `true` | Serve the 3306 persona |
| `listener.mysql.bind` | text | `0.0.0.0:3306` | Address and port |
| `listener.mysql.tls` | text | `required` | `required`, `optional` or `off` |
| `listener.mysql.cert` | text | `default` | Certificate bundle |
| `listener.mariadb.enabled` | bool | `true` | Serve the 3307 persona |
| `listener.mariadb.bind` | text | `0.0.0.0:3307` | Address and port |
| `listener.mariadb.tls` | text | `required` | `required`, `optional` or `off` |
| `listener.mariadb.cert` | text | `default` | Certificate bundle |
| `listener.mysql.client_cert`, `listener.mariadb.client_cert` | text | `off` | Client certificates: `off`, `optional` or `required`, checked against `tls.bundles.default.client_ca_path`. Needed for accounts with `REQUIRE X509`, `SUBJECT` or `ISSUER`; see [Client certificates](../security/tls.md#client-certificates) |

Listener settings take the `dc` and `global` scopes. An operator changes them with `sprigctl`; the listener rebinds without a restart:

```sh
sprigctl settings set listener.mariadb.tls optional --scope dc:mum1 --reason "legacy reporting client"
```

When a listener becomes stricter (TLS turned on, or the listener disabled), sessions that no longer qualify get `listener.drain_seconds` (default 600) to finish, then receive error 39004 `ER_SPRIG_LISTENER_RETIRED` at their next command.

Other connection-phase settings:

| Setting | Default | Meaning |
|---|---|---|
| `gateway.handshake_timeout_ms` | `10000` | Longest time for the PROXY header, TLS handshake and login together |
| `gateway.compression` | `zlib,zstd` | Compression algorithms offered |
| `gateway.max_allowed_packet_bytes` | `67108864` (64 MiB) | Largest logical packet, the value of `@@max_allowed_packet` |
| `gateway.max_connect_attrs_bytes` | `65536` | Connection attributes kept per session |
| `gateway.max_connections` | `20000` | Client connections per gateway |
| `limits.tenant_connections` | `2000` | Client connections per tenant per gateway |
| `tls.min_version` | `1.2` | Lowest TLS version accepted |

## Log in

The user name names the tenant: `acme.app` is user `app` of tenant `acme`. The gateway splits the name at the first dot. Tenant names are lower case, so `ACME.app` names no tenant, while the user part is case-sensitive, as in MySQL. A whole login name can be up to 255 characters.

```sh
mysql -h db.example.com -P 3306 -u acme.app -p --ssl-mode=VERIFY_IDENTITY --ssl-ca=ca.pem shop
```

```sh
mariadb -h db.example.com -P 3307 -u acme.app -p --ssl --ssl-ca=ca.pem shop
```

If the operator has set up tenant endpoints (`listener.tenant_sni_suffixes`), you can instead connect over TLS to `acme.<suffix>` and log in as plain `app`; your client must send the host name as the TLS server name (SNI). See [Tenant endpoints by server name](../security/tls.md#tenant-endpoints-by-server-name).

The database in the connection (`shop` above, or `CONNECT_WITH_DB` from a driver) runs as `USE shop` after the login. Database names are the tenant's own: two tenants can each have a `shop`, and neither can see the other's.

The same user and password work on every SprigDB protocol. See [Connect to SprigDB](overview.md#one-user-every-protocol).

### Authentication plugins

| Plugin | Supported | Notes |
|---|---|---|
| `caching_sha2_password` | yes | The default on both personas, announced in the greeting |
| `mysql_native_password` | when the tenant allows it | Off unless `auth.mysql_native_password` is `true` |
| `mysql_clear_password` | over TLS only | Refused with 3159 without TLS |
| `sha256_password` | no | |
| `client_ed25519`, `parsec` | no | |
| `auth_gssapi_client`, `authentication_ldap_*` | no | |

When a client starts with another plugin, the gateway sends an authentication switch request to the plugin the account uses. MariaDB clients that start with `mysql_native_password` switch to `caching_sha2_password` this way; MariaDB Connector/J, Connector/C and the `mariadb` command-line client all handle it.

### `caching_sha2_password` needs TLS for the first login

`caching_sha2_password` has two paths. The fast path checks a scramble against a cached hash and works with or without TLS. The full path sends the password itself, which is only safe over TLS. MySQL offers an RSA key exchange for the full path without TLS; **SprigDB doesn't support the RSA key exchange.**

The full path runs at the first login of a user on a gateway, after a password change, and whenever the gateway's cache doesn't hold the user. On a plaintext connection that login fails with 1045 and this hint:

```text
ERROR 1045 (28000): Access denied for user 'acme.app'@'203.0.113.7' (using password: YES); caching_sha2_password needs TLS for a full authentication (RSA key exchange is not supported): connect with TLS
```

What to do:

- Connect with TLS. This is the default, because both listeners require it.
- If a client can't use TLS, turn on `mysql_native_password` for its tenant and create the user with that plugin. Native logins don't need the full path.

```sh
sprigctl settings set auth.mysql_native_password true --scope tenant:acme --reason "plaintext reporting tool"
```

```sql
CREATE USER 'report' IDENTIFIED WITH mysql_native_password BY 's3cret-Passw0rd';
```

Options such as `--get-server-public-key` and `allowPublicKeyRetrieval=true` don't help: the gateway refuses the key request in the same way.

### What a login checks

The checks run in MySQL's order, after the password exchange has finished:

| Situation | Error (3306) | Error (3307) |
|---|---|---|
| Account temporarily locked after failed logins (`auth.failed_login_attempts`, `auth.password_lock_minutes`) | 3955 | 4150 |
| Wrong password, unknown user or unknown tenant (one message for all three) | 1045 | 1045 |
| Tenant or a parent tenant suspended | 39303 `ER_SPRIG_TENANT_SUSPENDED` | 39303 |
| `ACCOUNT LOCK` | 3118 | 4151 |
| No TLS, but `auth.require_tls` is on or the account has `REQUIRE SSL` | 1045 | 1045 |
| Password expired and the client can't handle expired passwords | 1862 | 1862 |
| Too many failed logins from one address (`auth.ip_failures_per_minute`, default 30) | 39300 `ER_SPRIG_LOGIN_RATE_LIMITED`, before the greeting | 39300 |
| Account directory or catalog unreachable, or login hashing busy for over 2 s | 39301 `ER_SPRIG_AUTH_UNAVAILABLE`; retry | 39301 |

A client that sets `CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS` (the `mysql` client does) logs in with an expired password in sandbox mode, and every statement other than a password change fails until the user runs `ALTER USER ... IDENTIFIED BY` or `SET PASSWORD`.

After `auth.delay_after_failures` (default 3) failures in a row for one account and address, further failures are delayed, starting at `auth.failure_delay_ms` (1 s) and doubling up to `auth.failure_delay_max_ms` (10 s).

## TLS

| `listener.<name>.tls` | Client without TLS | Client with TLS |
|---|---|---|
| `required` (default) | 3159, "Connections using insecure transport are prohibited while --require_secure_transport=ON." (SQLSTATE `HY000` on 3306, `08004` on 3307) | accepted |
| `optional` | accepted | accepted |
| `off` | accepted | TLS isn't offered; a client that sends a TLS request anyway gets 1043 |

Certificates come from the bundle the listener's `cert` setting names; the `default` bundle is configured with `tls.bundles.default.cert_path` and `tls.bundles.default.key_path`, and the files are reloaded when they change. See [TLS](../security/tls.md).

Clients that don't support MySQL 4.1 authentication get 1251.

## What works

### Protocol commands

| Command | Support | Notes |
|---|---|---|
| `COM_QUERY` | yes | Text protocol, multi-statements when the client asks for them |
| `COM_STMT_PREPARE`, `EXECUTE`, `CLOSE`, `RESET` | yes | Server-side prepared statements |
| `COM_STMT_SEND_LONG_DATA` | yes | Buffered up to `gateway.long_data_max_bytes` (64 MiB) per statement; more fails with 39011 |
| `COM_STMT_FETCH` | yes | Server-side cursors; an open cursor holds its backend connection |
| `COM_STMT_BULK_EXECUTE` | 3307 only | MariaDB's array binding |
| `COM_INIT_DB` | yes | As `USE` |
| `COM_PING` | yes | Answered by the gateway |
| `COM_CHANGE_USER` | yes | Logs in again; the session starts fresh |
| `COM_RESET_CONNECTION` | yes | Resets session state and prepared statements |
| `COM_SET_OPTION` | yes | Turns multi-statements on or off |
| `COM_FIELD_LIST` | yes | |
| `COM_STATISTICS`, `COM_PROCESS_INFO`, `COM_PROCESS_KILL` | yes | From the gateway's own session list |
| `COM_CREATE_DB`, `COM_DROP_DB` | yes | As `CREATE DATABASE` and `DROP DATABASE` |
| `COM_REFRESH`, `COM_SHUTDOWN`, `COM_DEBUG` | no | 1227 |
| `COM_BINLOG_DUMP`, `COM_BINLOG_DUMP_GTID`, `COM_REGISTER_SLAVE` | no | 1227: SprigDB isn't a replication source. Use the [change feed](../concepts/change-feed.md) |
| Anything else | no | 1047 |

### Session features

| Feature | Support | Notes |
|---|---|---|
| Prepared statements | yes | Up to `gateway.max_prepared_per_session` (1024) per session; they survive moves to other backend connections and are prepared again where needed |
| `PREPARE`, `EXECUTE`, `EXECUTE IMMEDIATE`, `DEALLOCATE PREPARE` | yes | SQL-level prepared statements. Once the tenant has a sharded table, the gateway holds them and plans each execution afresh; errors come at `PREPARE`, and an unknown name fails with 1243. Before that they go to MariaDB, and the session keeps one backend connection. See [SQL-level prepared statements](../sql/overview.md#sql-level-prepared-statements) |
| Multi-statements | yes | Needs `CLIENT_MULTI_STATEMENTS` or `COM_SET_OPTION`. Without it, two statements in one query fail with 1064. An error stops the remaining statements |
| Multiple result sets, stored procedures with `CALL` | yes | |
| `LOAD DATA LOCAL INFILE` | yes | Needs the client's local-files option and `sql.local_infile` (default `true`). Otherwise 3948 on 3306, 4166 on 3307. Into a sharded table the rows become routed inserts |
| Transactions: `BEGIN`, `START TRANSACTION`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `SET TRANSACTION` | yes | A transaction that touches several clusters commits with two-phase commit. See [SQL overview](../sql/overview.md) |
| `XA` from the client | yes | The session keeps its backend connection until the branch ends |
| Temporary tables, `LOCK TABLES`, `GET_LOCK()`, `HANDLER` | yes | The session keeps its backend connection while they exist |
| `SET NAMES`, character sets and collations | yes | |
| `USE` | yes | |
| `KILL [CONNECTION \| QUERY] id` | yes | Ids are the gateway's connection ids. You can kill your own user's sessions, and with `CONNECTION_ADMIN` any session of your tenant; another user's session otherwise fails with 1095. A killed statement returns 1317 |
| `SHOW [FULL] PROCESSLIST` | yes | Your own user's sessions on this gateway |
| Compression | yes | zlib on both; zstd on 3306 |
| Query attributes | 3306 | |
| Session state tracking, `CLIENT_DEPRECATE_EOF` | yes | |
| Connection attributes | yes | Kept up to `gateway.max_connect_attrs_bytes`; never used to pick a tenant |
| Binary log replication, `SHOW BINARY LOGS`, `SHOW MASTER STATUS`, `SHOW REPLICA STATUS`, `SHOW ENGINE` | no | 1227 |

### Session variables

These session variables are tracked and applied to whichever backend connection runs your next statement, so they behave as on a single server:

```text
autocommit, sql_mode, time_zone, character_set_client, character_set_connection,
character_set_results, collation_connection, transaction_isolation, tx_isolation,
transaction_read_only, tx_read_only, foreign_key_checks, unique_checks,
max_statement_time, optimizer_switch, sql_select_limit, group_concat_max_len,
div_precision_increment, lc_time_names, sql_auto_is_null, sql_safe_updates,
sql_big_selects, sql_quote_show_create, sql_notes, sql_warnings,
auto_increment_increment, auto_increment_offset, big_tables,
default_storage_engine, default_tmp_storage_engine, character_set_collations,
innodb_snapshot_isolation
```

User variables (`SET @v = ...`) and `LAST_INSERT_ID()` follow the session too.

The gateway owns `wait_timeout`, `interactive_timeout`, `net_read_timeout`, `net_write_timeout` and the `session_track_*` variables. You may set them, and `SELECT` shows your value, but they don't change the backend. `@@version`, `@@version_comment`, `@@protocol_version`, `@@max_allowed_packet` and `@@hostname` come from the persona.

`SET GLOBAL` doesn't change MariaDB. With `SYSTEM_VARIABLES_ADMIN`, `SET GLOBAL` of `sql_mode`, `time_zone`, `transaction_isolation`, `max_execution_time` (3306) or `wait_timeout` sets the tenant's default for new sessions; any other variable fails with 1227. See [SET GLOBAL sets the tenant's defaults](../sql/overview.md#set-global-sets-the-tenants-defaults).

Setting any other session variable works, but the session then keeps one backend connection until it ends. That is correct, but it uses more backend connections. On 3306, `innodb_snapshot_isolation` starts `OFF`, so a `REPEATABLE READ` update never fails with 1020, as in MySQL.

## Drivers known to work

SprigDB's connector test matrix runs these clients against both ports, with and without TLS. Each client runs connect, data types, prepared statements, transactions, multi-statements, duplicate keys, `LAST_INSERT_ID`, large rows, packets over 16 MiB, compression, `LOAD DATA LOCAL INFILE`, `CALL`, change user, reset connection, re-preparing in a pool and lock waits.

| Client | Version tested |
|---|---|
| MySQL Connector/J | 9.7 (`useServerPrepStmts=true`) |
| MariaDB Connector/J | 3.5 |
| go-sql-driver/mysql | 1.9 |
| mysql2 (Node.js) | 3.x |
| PyMySQL | 1.2 |
| mysqlclient (Python, libmariadb) | 2.3 |
| PHP mysqlnd: PDO_MySQL and mysqli | PHP 8.4 |
| `mysql` and `mysqldump` | 8.4 |
| `mariadb` and `mariadb-dump` | 11.8 |
| SQLAlchemy over PyMySQL and mysqlclient | 2.0 |
| Hibernate with Spring Data JPA | 6.6 |
| Prisma ORM | 6.19 |
| sysbench and sysbench-tpcc | 1.0.20 |

The checks a client can't do (PyMySQL has no server-side prepared statements, go-sql-driver has no change-user call) are skipped, not failed.

### Examples

Java, with MySQL Connector/J:

```java
String url = "jdbc:mysql://db.example.com:3306/shop?sslMode=VERIFY_IDENTITY&useServerPrepStmts=true";
try (Connection c = DriverManager.getConnection(url, "acme.app", System.getenv("DB_PASSWORD"));
     PreparedStatement ps = c.prepareStatement("SELECT id, status FROM orders WHERE customer_id = ?")) {
    ps.setLong(1, 42);
    try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) System.out.println(rs.getLong(1) + " " + rs.getString(2));
    }
}
```

Go:

```go
dsn := "acme.app:" + os.Getenv("DB_PASSWORD") + "@tcp(db.example.com:3306)/shop?tls=true&parseTime=true"
db, err := sql.Open("mysql", dsn)
```

Python, with PyMySQL:

```python
import pymysql

conn = pymysql.connect(host="db.example.com", port=3306, user="acme.app",
                       password=os.environ["DB_PASSWORD"], database="shop",
                       ssl={"ca": "ca.pem"})
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM orders WHERE status = %s", ("paid",))
    print(cur.fetchone())
```

Node.js, with mysql2:

```js
const mysql = require('mysql2/promise');
const conn = await mysql.createConnection({
  host: 'db.example.com', port: 3306, user: 'acme.app',
  password: process.env.DB_PASSWORD, database: 'shop',
  ssl: { ca: fs.readFileSync('ca.pem') },
});
const [rows] = await conn.execute('SELECT * FROM orders WHERE id = ?', [1]);
```

## Behaviour to know about

- **Connection ids are the gateway's.** `CONNECTION_ID()`, `SHOW PROCESSLIST` and `KILL` use them. A session's statements may run on different backend connections between transactions.
- **Reads may go to replicas** that have caught up with your session's writes. You always read your own writes. See [Read routing](../performance/read-routing.md).
- **Statements that can't run exactly across shards fail** with 39420 `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` instead of returning a partial answer.
- **Tell SprigDB where the client runs.** Set the connection attribute `sprig_dc` to a data-centre id, or run `SET @sprig_dc = 'fra1'`. SprigDB counts the tenant's traffic per client data centre and uses it to move tenants toward their users (see [Data centres](../operate/data-centres.md)). The variable is the gateway's and never reaches MariaDB; an id that isn't registered answers OK with warning 39503 `ER_SPRIG_UNKNOWN_DC` and is ignored.
- **Large statements** of `gateway.large_statement_bytes` (1 MiB) or more run on a separate thread, so they don't hold up other sessions.
- **A gateway that drains** (for an upgrade or a shutdown) answers the next command with 39000 and closes the connection. Reconnect; another gateway takes the session.

## Errors you may see

| Code | Name | When | What to do |
|---|---|---|---|
| 1040 | `ER_CON_COUNT_ERROR` | The gateway has `gateway.max_connections` connections | Retry, or add gateways |
| 1043 | `ER_HANDSHAKE_ERROR` | TLS requested on a listener with TLS `off` | Connect without TLS, or change the listener |
| 1045 | `ER_ACCESS_DENIED_ERROR` | Wrong credentials, or a full `caching_sha2_password` login without TLS | Check the login name `tenant.user`; use TLS |
| 1047 | `ER_UNKNOWN_COM_ERROR` | An unsupported protocol command | |
| 1227 | `ER_SPECIFIC_ACCESS_DENIED_ERROR` | Replication or server-administration commands; `SET GLOBAL` without `SYSTEM_VARIABLES_ADMIN`, or of a variable a tenant can't set | |
| 1243 | `ER_UNKNOWN_STMT_HANDLER` | `EXECUTE` or `DEALLOCATE PREPARE` of a name the session hasn't prepared | `PREPARE` it first |
| 1251 | `ER_NOT_SUPPORTED_AUTH_MODE` | A client without 4.1 authentication | Upgrade the client |
| 1317 | `ER_QUERY_INTERRUPTED` | `KILL QUERY` stopped the statement | |
| 3159 | `ER_SECURE_TRANSPORT_REQUIRED` | No TLS on a listener that requires it | Enable TLS in the client |
| 3948 / 4166 | Local files disabled | `LOAD DATA LOCAL` without the client option, or with `sql.local_infile` off | Enable local files in the client |
| 39000 | `ER_SPRIG_GATEWAY_DRAINING` | The gateway is shutting down | Reconnect |
| 39001 | `ER_SPRIG_TENANT_CONNECTION_LIMIT` | The tenant has `limits.tenant_connections` connections on this gateway | Close idle connections or raise the limit |
| 39004 | `ER_SPRIG_LISTENER_RETIRED` | The listener was disabled or made stricter | Reconnect with the new requirements |
| 39010 | `ER_SPRIG_LOCAL_INFILE_REJECTED` | The backend asked for a file other than the one the statement named | |
| 39011 | `ER_SPRIG_LONG_DATA_LIMIT` | More than `gateway.long_data_max_bytes` of long data for one statement | Send smaller values |
| 39030 | `ER_SPRIG_BACKEND_UNAVAILABLE` | No MariaDB server is reachable for the data | Retry |
| 39032 | `ER_SPRIG_OUTCOME_UNKNOWN` | The connection to MariaDB broke during `COMMIT` | Check whether the transaction applied before retrying |
| 39033 | `ER_SPRIG_SESSION_STATE_LOST` | The MariaDB connection holding state SprigDB can't replay (temporary tables, `GET_LOCK` locks, an open transaction) was lost | Reconnect and rebuild the state |
| 39034 | `ER_SPRIG_BACKEND_LOST` | The MariaDB connection broke after part of a result was sent | Discard the partial result and run the statement again |
| 39300 | `ER_SPRIG_LOGIN_RATE_LIMITED` | Too many failed logins from your address | Wait `auth.ip_block_seconds` (300 s) |
| 39301 | `ER_SPRIG_AUTH_UNAVAILABLE` | Logins can't be checked right now | Retry |
| 39303 | `ER_SPRIG_TENANT_SUSPENDED` | The tenant is suspended | Ask the tenant's administrator |
| 39306 | `ER_SPRIG_SESSION_REVOKED` | Your account was dropped or locked while connected | |
| 39420 | `ER_SPRIG_STATEMENT_NOT_DISTRIBUTABLE` | A statement can't run with exact semantics across shards | Rewrite it, or keep the data in one table group |

The full list is in [Error codes](../reference/errors.md). Some drivers show 39xxx numbers as negative; add 65536.

## Related

- [Connect to SprigDB](overview.md)
- [SQL overview](../sql/overview.md)
- [Accounts and logins](../security/accounts-and-logins.md)
- [TLS](../security/tls.md)
- [Connection problems](../troubleshooting/connections.md)
