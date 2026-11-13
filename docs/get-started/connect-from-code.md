---
title: Connect from code
audience: [user]
sidebar:
  order: 3
---

# Connect from code

Your application connects to SprigDB with the driver it already uses for MySQL or MariaDB. This page gives a working connection for each common language, with TLS, and explains the two things that differ from a plain MySQL server: the login name and the `caching_sha2_password` rule.

## The three things to get right

1. **The login name is `tenant.user`.** User `app` of tenant `acme` logs in as `acme.app`. Put it wherever the driver takes a user name. See [Tenants and databases](../concepts/tenants-and-databases.md#logins).
2. **Use TLS.** Both MySQL listeners require it by default, and the first login of a user needs it (see [below](#the-caching_sha2_password-note)).
3. **Pick a port.** 3306 behaves as MySQL 8.4 and 3307 as MariaDB 11.8, with the same data behind both. Use 3306 with MySQL drivers and 3307 with MariaDB drivers, or whichever dialect your SQL is written in. See [MySQL and MariaDB](../connect/mysql-and-mariadb.md).

The examples use the host `db.example.com`, the database `shop`, a CA certificate in `ca.pem`, and read the password from the environment. On the [Quick start](quick-start.md) stack, use `127.0.0.1`, ports 13306 and 13307, and `deploy/compose/gateway-certs/ca.pem`.

## Tested drivers

SprigDB's connector tests run each of these against both ports, with TLS required and with TLS off: connecting, data types, prepared statements, transactions, multi-statements, duplicate keys, `LAST_INSERT_ID()`, large rows and packets, compression, `LOAD DATA LOCAL INFILE`, `CALL`, change user, reset connection and lock waits. Each driver's own upstream test suite also runs against SprigDB, at the version shown.

| Language | Driver | Version tested |
|---|---|---|
| Java | MySQL Connector/J | 9.7.0 |
| Java | MariaDB Connector/J | 3.5.11 |
| Java | Hibernate ORM (with Spring Data JPA) | 6.6.33 |
| Go | go-sql-driver/mysql | 1.10.1 |
| Node.js | mysql2 | 3.24.6 |
| Node.js | Prisma ORM | 6.19 |
| Python | PyMySQL | 1.2.3 |
| Python | mysqlclient (`MySQLdb`) | 2.3.0 |
| Python | SQLAlchemy over either | 2.0.36 |
| PHP | PDO_MySQL and mysqli (mysqlnd) | PHP 8.4.14 |
| Command line | `mysql`, `mysqldump` / `mariadb`, `mariadb-dump` | 8.4 / 11.8 |

A check a driver can't do, such as server-side prepared statements in PyMySQL, is skipped rather than failed.

## Java

### MySQL Connector/J

```java
import java.sql.*;

String url = "jdbc:mysql://db.example.com:3306/shop"
        + "?sslMode=VERIFY_IDENTITY"
        + "&useServerPrepStmts=true";
try (Connection c = DriverManager.getConnection(url, "acme.app", System.getenv("DB_PASSWORD"));
     PreparedStatement ps = c.prepareStatement(
             "SELECT id, status, total FROM orders WHERE customer_id = ?")) {
    ps.setLong(1, 42);
    try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
            System.out.println(rs.getLong("id") + " " + rs.getString("status"));
        }
    }
}
```

| Option | Meaning |
|---|---|
| `sslMode=VERIFY_IDENTITY` | Encrypt, and check the certificate and host name. With a certificate from a private CA, add it to a Java truststore and pass `trustCertificateKeyStoreUrl=file:/path/truststore.jks` and `trustCertificateKeyStorePassword` |
| `sslMode=REQUIRED` | Encrypt without checking the certificate. The connector tests use it against a self-signed certificate; don't use it in production |
| `useServerPrepStmts=true` | Server-side prepared statements, which SprigDB supports and caches per session |
| `allowMultiQueries=true` | Several statements in one `execute`, if your code needs it |

### MariaDB Connector/J

```java
String url = "jdbc:mariadb://db.example.com:3307/shop"
        + "?sslMode=verify-full"
        + "&serverSslCert=/etc/myapp/ca.pem";
try (Connection c = DriverManager.getConnection(url, "acme.app", System.getenv("DB_PASSWORD"))) {
    // ...
}
```

| Option | Meaning |
|---|---|
| `sslMode=verify-full` | Encrypt, and check the certificate and host name |
| `serverSslCert` | The CA certificate (or the server's certificate), as a path or PEM text |
| `sslMode=trust` | Encrypt without checking; the connector tests use it, don't use it in production |

MariaDB Connector/J first offers `mysql_native_password`; the gateway switches it to `caching_sha2_password`, which the driver handles.

### Connection pools

HikariCP and other pools work as with MySQL. Each pooled connection is a SprigDB session. Pool size counts against `limits.tenant_connections` (2000 per tenant per gateway by default): a tenant past it gets 39001 `ER_SPRIG_TENANT_CONNECTION_LIMIT`.

## Go

```go
import (
    "crypto/tls"
    "crypto/x509"
    "database/sql"
    "os"

    "github.com/go-sql-driver/mysql"
)

func open() (*sql.DB, error) {
    pem, err := os.ReadFile("ca.pem")
    if err != nil {
        return nil, err
    }
    roots := x509.NewCertPool()
    roots.AppendCertsFromPEM(pem)
    if err := mysql.RegisterTLSConfig("sprig", &tls.Config{RootCAs: roots, ServerName: "db.example.com"}); err != nil {
        return nil, err
    }
    dsn := "acme.app:" + os.Getenv("DB_PASSWORD") +
        "@tcp(db.example.com:3306)/shop?tls=sprig&parseTime=true&loc=UTC"
    return sql.Open("mysql", dsn)
}
```

| DSN option | Meaning |
|---|---|
| `tls=sprig` | The TLS configuration registered under that name: your CA, with host-name checking |
| `tls=true` | Encrypt and verify against the system's CAs; enough when the gateway's certificate is from a public CA |
| `tls=skip-verify` | Encrypt without checking; the connector tests use it against a self-signed certificate |
| `parseTime=true` | Return `DATETIME` columns as `time.Time` |
| `multiStatements=true` | Several statements in one call, if your code needs it |

## Node.js

### mysql2

```js
const fs = require('fs');
const mysql = require('mysql2/promise');

const pool = mysql.createPool({
  host: 'db.example.com',
  port: 3306,
  user: 'acme.app',
  password: process.env.DB_PASSWORD,
  database: 'shop',
  ssl: { ca: fs.readFileSync('ca.pem') },
  supportBigNumbers: true,
  bigNumberStrings: true,
});

const [rows] = await pool.execute('SELECT id, status FROM orders WHERE customer_id = ?', [42]);
```

`ssl: { ca }` encrypts and checks the certificate. `{ rejectUnauthorized: false }` encrypts without checking; the connector tests use it, but don't use it in production.

### Prisma

```text
DATABASE_URL="mysql://acme.app:<password>@db.example.com:3306/shop?sslmode=require&sslaccept=strict&sslcert=ca.pem"
```

`sslaccept=strict` with `sslcert` (the CA file, relative to the Prisma schema) checks the certificate. The connector tests use `sslmode=require&sslaccept=accept_invalid_certs` against a self-signed certificate; don't use that in production. URL-encode the password if it has special characters; the tenant prefix's dot needs no encoding.

## Python

### PyMySQL

```python
import os
import pymysql

conn = pymysql.connect(
    host="db.example.com", port=3306,
    user="acme.app", password=os.environ["DB_PASSWORD"],
    database="shop",
    ssl={"ca": "ca.pem"},
)
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM orders WHERE status = %s", ("paid",))
    print(cur.fetchone())
conn.close()
```

`ssl={"ca": "ca.pem"}` encrypts and checks the certificate against that CA.

### mysqlclient

```python
import os
import MySQLdb

conn = MySQLdb.connect(
    host="db.example.com", port=3306,
    user="acme.app", password=os.environ["DB_PASSWORD"],
    database="shop",
    ssl={"ca": "ca.pem"},
)
```

### SQLAlchemy

```python
from sqlalchemy import create_engine

engine = create_engine(
    "mysql+pymysql://acme.app:<password>@db.example.com:3306/shop",
    connect_args={"ssl": {"ca": "ca.pem"}},
)
```

Use `mysql+mysqldb://` for mysqlclient.

## PHP

### PDO

```php
$pdo = new PDO(
    'mysql:host=db.example.com;port=3306;dbname=shop;charset=utf8mb4',
    'acme.app',
    getenv('DB_PASSWORD'),
    [
        Pdo\Mysql::ATTR_SSL_CA => '/etc/myapp/ca.pem',
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    ]
);
$stmt = $pdo->prepare('SELECT id, status FROM orders WHERE customer_id = ?');
$stmt->execute([42]);
```

On PHP before 8.4, the constant is `PDO::MYSQL_ATTR_SSL_CA`.

### mysqli

```php
mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
$db = mysqli_init();
$db->ssl_set(null, null, '/etc/myapp/ca.pem', null, null);
$db->real_connect('db.example.com', 'acme.app', getenv('DB_PASSWORD'), 'shop', 3306, null, MYSQLI_CLIENT_SSL);
```

## .NET and other languages

.NET MySQL drivers aren't in SprigDB's test matrix yet. Any driver that speaks the MySQL protocol with `caching_sha2_password` over TLS is expected to work, with the same rules: the `tenant.user` login and TLS.

## The caching_sha2_password note

`caching_sha2_password` is the default authentication plugin on both ports. It has two paths:

- **Fast:** the client proves it knows the password against a hash the gateway has cached. This works with or without TLS.
- **Full:** the client sends the password itself. This happens at a user's first login on a gateway, after a password change, and whenever the gateway's cache doesn't hold the user. SprigDB accepts the full path **only over TLS**.

MySQL offers an RSA key exchange for the full path without TLS. SprigDB doesn't support it, so `allowPublicKeyRetrieval=true`, `--get-server-public-key` and similar options don't help. A plaintext connection that needs the full path fails with:

```text
ERROR 1045 (28000): Access denied for user 'acme.app'@'203.0.113.7' (using password: YES); caching_sha2_password needs TLS for a full authentication (RSA key exchange is not supported): connect with TLS
```

The fix is to connect with TLS. If a client truly can't, a tenant administrator can ask an operator to turn on `mysql_native_password` for the tenant (`auth.mysql_native_password = true`); passwords set afterwards also store a native verifier, which doesn't need the full path. See [Accounts and logins](../security/accounts-and-logins.md).

Other plugins: `mysql_clear_password` works over TLS only, and `sha256_password`, `client_ed25519` and the Kerberos and LDAP plugins aren't supported.

## Telling SprigDB where the client runs

If your application runs in several data centres, tell the gateway which one each connection comes from, with the connection attribute `sprig_dc` or a statement right after connecting:

```sql
SET @sprig_dc = 'fra1';
```

MongoDB clients name it at the end of `appName` (`appName=orders~fra1`), and Redis clients at the end of the client name (`CLIENT SETNAME orders~fra1`).

SprigDB uses it to move a tenant's data toward its traffic. It never affects access, or which copy serves a read. See [Clusters and data centres](../concepts/clusters-and-data-centres.md#naming-the-clients-dc).

## Errors to handle

Most errors are MySQL's or MariaDB's own. These SprigDB errors are worth handling in code, because a retry fixes them:

| Code | Name | What to do |
|---|---|---|
| 39000 | `ER_SPRIG_GATEWAY_DRAINING` | The gateway is shutting down; reconnect, and another gateway takes the session |
| 39030 | `ER_SPRIG_BACKEND_UNAVAILABLE` | No MariaDB server was reachable; retry |
| 39110 | `ER_SPRIG_SHARD_MOVED` | A shard moved during the statement; retry |
| 39424 | `ER_SPRIG_SHARD_MAP_CHANGED` | The shard map changed during the statement; retry |
| 39443 | `ER_SPRIG_DTX_ROLLED_BACK` | A distributed transaction was rolled back; retry the transaction |
| 39441 | `ER_SPRIG_COMMIT_OUTCOME_UNKNOWN` | The commit's outcome is unknown and the connection closes; check before retrying |

Some drivers show 39xxx codes as negative numbers: add 65536. The full list is in [Error codes](../reference/errors.md).

## MongoDB clients

Any MongoDB driver connects to the MongoDB listener (27017, off by default) with SCRAM-SHA-256, the same `tenant.user` login and password, and `authSource=admin`. The database in the URI is a SprigDB database:

```js
const { MongoClient } = require('mongodb');
const client = new MongoClient(
  'mongodb://acme.app:<password>@db.example.com:27017/shop?authSource=admin&tls=true&tlsCAFile=ca.pem'
);
const events = client.db('shop').collection('events');
await events.insertOne({ customer_id: 42, kind: 'login' });
```

```python
from pymongo import MongoClient

client = MongoClient("mongodb://db.example.com:27017/?authSource=admin&tls=true&tlsCAFile=ca.pem",
                     username="acme.app", password=os.environ["DB_PASSWORD"])
```

See [MongoDB](../connect/mongodb.md).

## Redis clients

Redis clients connect to the Redis listener (6379, off by default) in standalone mode, and log in with `AUTH acme.app <password>` or a URL with the user name:

```python
import os
import redis

r = redis.Redis(host="db.example.com", port=6379, ssl=True, ssl_ca_certs="ca.pem",
                username="acme.app", password=os.environ["DB_PASSWORD"], db=0)
r.set("session:42", "active", ex=3600)
```

```text
rediss://acme.app:<password>@db.example.com:6379/0
```

See [Redis](../connect/redis.md).

## Related

- [MySQL and MariaDB](../connect/mysql-and-mariadb.md)
- [Connect to SprigDB](../connect/overview.md)
- [TLS](../security/tls.md)
- [Accounts and logins](../security/accounts-and-logins.md)
- [Connection problems](../troubleshooting/connections.md)
