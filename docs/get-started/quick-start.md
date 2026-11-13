---
title: Quick start
audience: [user, dba, operator]
sidebar:
  order: 1
---

# Quick start

This guide runs SprigDB on your own machine with Docker Compose, then connects to it with an ordinary MySQL client. It takes about ten minutes, most of it the first build.

## What you need

- **Docker** with Compose v2.
- **Rust.** `rust-toolchain.toml` pins the version, and rustup installs it on first use.
- A **MySQL or MariaDB client**: `mysql`, `mariadb`, or any GUI that speaks the MySQL protocol.
- `curl`, `jq` and `openssl`. The setup script uses them.

## 1. Start MariaDB and etcd

SprigDB stores data in MariaDB and keeps its control plane in etcd. The Compose file under `deploy/compose/` runs all three MariaDB topologies SprigDB supports, plus etcd:

```sh
cd deploy/compose
docker compose --profile all up -d --wait
cd -
```

| Service | Host port |
|---|---|
| etcd | 2379 |
| single node | 13301 |
| primary, replica-1, replica-2 | 13311, 13312, 13313 |
| galera-1, galera-2, galera-3 | 13321, 13322, 13323 |

## 2. Start a controller and a gateway

```sh
deploy/compose/gateway-dev.sh up
```

This builds `sprigdb` and `sprigctl` and starts a controller. The controller registers the three MariaDB clusters and starts a gateway. It also creates two sample tenants, `acme` and `globex`, each with a user called `app`. `gateway-dev.sh status` shows what is running, and `gateway-dev.sh down` stops it all and removes what `up` created.

## 3. Connect

The gateway listens as MySQL 8.4 on port 13306 and as MariaDB on 13307. A login names its tenant: `acme.app` is user `app` of tenant `acme`.

```sh
mysql -h 127.0.0.1 -P 13306 -u acme.app -psprig-dev-app \
      --ssl-mode=VERIFY_IDENTITY --ssl-ca=deploy/compose/gateway-certs/ca.pem
```

```sql
CREATE DATABASE shop;
USE shop;
CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR(20), total DECIMAL(10,2));
INSERT INTO orders VALUES (1, 'paid', 42.50), (2, 'open', 18.00);
SELECT status, COUNT(*), SUM(total) FROM orders GROUP BY status;
```

From here on, it's a database. Tenants are isolated from each other: `globex.app` sees none of `acme`'s databases.

The passwords in `deploy/compose/.env` and these examples are for local development only. Never reuse them.

## 4. Look around in Studio and `sprig`

**Studio** is SprigDB's browser workspace. Its test setup turns on the gateway's HTTP listener and adds sample data:

```sh
deploy/compose/studio/e2e.sh setup
```

Open <http://127.0.0.1:18080/studio> and sign in as `acme.app` with the same password.

**`sprig`** is the terminal shell: a full-screen interface, or a scriptable command line.

```sh
cargo run -p sprig-shell --bin sprig -- --url http://127.0.0.1:18080 -u acme.app login
cargo run -p sprig-shell --bin sprig
```

Press `Ctrl-K` in either for the command palette.

## Where next

- [Settings](../reference/settings.md): what you can tune, at which scope.
- [Error codes](../reference/errors.md): what an error means and what to do.
- [Developing SprigDB](../operate/development.md): the other environments (three data centres), running a single role, and the tests.
