# SprigDB on VMs with Docker Compose: runbook

This bundle runs one SprigDB data center on one VM: etcd, one MariaDB 11.8 node, the controller, the worker and one gateway. Every client port is TLS, etcd uses mutual TLS, and passwords, tokens and keys are read from files. Chapter 27 ("Compose on VMs") describes the design; this file is the procedure.

It was tested on the laptop (Docker Desktop, arm64) on 11 November 2026: install, first operator, tenant, a smoke test on all nine client protocols, backup and restore, upgrade and rollback between two builds, and certificate rotation. "Tested" notes below give the numbers. It has **not** run on a real VM yet.

Contents: [Files](#files) · [Install](#install) · [First operator](#first-operator) · [Tenants](#tenants) · [Ports](#ports) · [Adding a cluster](#adding-a-cluster) · [Backups](#backups) · [Restore](#restore) · [Upgrade and rollback](#upgrade-and-rollback) · [Rotating certificates](#rotating-certificates) · [Monitoring](#monitoring) · [Logs](#logs) · [Common failures](#common-failures) · [Galera: a node that can't rejoin](#galera-a-node-that-cant-rejoin) · [Not done yet](#not-done-yet)

## Files

| File | What it is |
|---|---|
| `compose.yaml` | the services, limits, healthchecks, volumes and ports |
| `.env.example` | every value you set per VM; copy to `.env` |
| `sprig-vm.sh` | init, up, configure, tenant, backup, restore, upgrade, rollback, rotate-certs |
| `config/controller.toml`, `config/gateway.toml` | the process files (the worker reads the controller's) |
| `config/mariadb/10-sprig.cnf` | chapter 27's server settings, TLS required |
| `config/mariadb/initdb/` | creates `sprig_ctl` and `sprig_repl` on first start |
| `config/mariadb/health.sh` | MariaDB's healthcheck (root's password file; survives a restore) |
| `smoke/smoke.sh`, `smoke/smoke.mjs` | the nine-protocol smoke test (mysql client, mongosh and Node.js 18+) |
| `config/prometheus.yml` | scrape configuration (profile `monitoring`) |
| `Dockerfile`, `Dockerfile.runtime`, `build-image.sh` | the `sprigdb` image |
| `secrets/` | made by `init`; never committed (`.gitignore`) |
| `backups/` | made by `backup`; never committed |

## Install

On the VM (Ubuntu 24.04 or Debian 12 with Docker Engine 27+ and the compose plugin; `openssl` and `jq` installed):

1. **Get the image** onto the VM. On a build machine: `deploy/compose-vm/build-image.sh sprigdb:<version>`, then `docker save sprigdb:<version> | gzip > sprigdb-<version>.tar.gz`, copy it over, `docker load < sprigdb-<version>.tar.gz`. (A registry works too: tag and push, then set `SPRIGDB_IMAGE` to the registry name.) The build runs in a container capped at 6 GB with 2 jobs and 16 codegen units (the workspace's one-unit release profile needs more than 6 GB for `sprig-gateway`; on a 12 GB builder, `SPRIG_BUILD_CGU=1` gives the exact profile). It took about 25 minutes on the laptop from a cold cache.
2. **Copy this directory** to the VM, for example `/opt/sprigdb`.
3. **Settings:** `cp .env.example .env`, then set at least `SPRIGDB_IMAGE`, `SPRIG_PUBLIC_HOST` (the name clients use; it goes into the certificate and into Kafka's advertised address), `SPRIG_TLS_SANS` for any other names, the memory limits for the VM's size and `SPRIG_DC`.
4. **Secrets:** `./sprig-vm.sh init`. It makes `secrets/`:
   - `ca/ca-key.pem`: the deployment's CA key. **Move it off the VM** after init and bring it back only to rotate certificates.
   - `tls/`: the CA certificate and the gateway, etcd, MariaDB and client certificates.
   - `mariadb/`: the root, `sprig_ctl` and `sprig_repl` passwords.
   - `sprig/`: the admin (bootstrap) token, the gateways' token, the workload master key, cluster credentials and the gateway's backend-account secret.
   `secrets/` itself is mode 0700; the files in it are 0644, because each container reads them as its own user (sprigdb 10001, mysql 999, etcd root). **Back up `secrets/` now**, somewhere other than the data backups: without it, a restored backup can't be opened (the workload key and the backend secret are in it).
5. **Start:** `./sprig-vm.sh up`. It starts the containers, waits for every healthcheck, registers the MariaDB node as cluster `<SPRIG_DC>-main` and writes the listener settings (TLS required on every listener; plain HTTP and plain AMQP off). The listeners come up within a few seconds.
6. **Check:** `./sprig-vm.sh status`, then create a tenant ([Tenants](#tenants)) and run the smoke test from a machine that reaches the ports: `SMOKE_PASSWORD=... smoke/smoke.sh acme.app <public host>`. It logs in over TLS (verifying the certificate against `secrets/tls/ca.pem`) on MySQL, MariaDB, MongoDB, REST, gRPC, Redis, Bolt, Kafka and AMQP, does one operation on each, and checks that a plaintext MySQL login is refused.

**Tested (laptop, project `sprigvm`, ports 3xxxx):** `up` took 19 s from nothing to every healthcheck green, including the cluster registration (TLS verified against the deployment CA). The smoke test passed on all nine protocols plus the plaintext refusal; each check took 4–107 ms after login (Redis 107 ms, the rest under 40 ms).

To use your own certificates instead of the generated ones, put them at the same paths in `secrets/tls/` (`gateway.pem` with the full chain and `gateway-key.pem` for clients; the CA that signed `etcd.pem`, `mariadb.pem` and `client.pem` as `ca.pem`).

## First operator

People who run SprigDB log in as **named operators** (chapter 14, "Operators"), with a password and TOTP. The bootstrap token in `secrets/sprig/admin-token` works only until the first operator logs in.

```sh
docker compose exec controller sprigctl --endpoint http://127.0.0.1:7480 --token-file /run/sprig/sprig/admin-token \
    operator create alice --role platform --display-name "Alice" --email alice@example.com
#   asks for alice's password (12 characters or more) on the terminal
```

(`sprig-vm.sh ctl` runs without a terminal, so it can't ask for a password. From a machine with `sprigctl` and the SSH tunnel below, `--password-file` works too.)

Then alice logs in from the VM, through an SSH tunnel to the Admin API (`ssh -L 7480:127.0.0.1:7480 vm`), with `sprigctl` on her machine:

```sh
sprigctl --endpoint http://127.0.0.1:7480 login --name alice
#   first login: shows an otpauth:// URI; add it to an authenticator app, type a code;
#   prints ten recovery codes: keep them
```

From that login on, the bootstrap token is refused (39310). So that `sprig-vm.sh` keeps working, alice issues it a service token and saves it as `secrets/sprig/ops-token`:

```sh
sprigctl --endpoint http://127.0.0.1:7480 token create --service sprig-vm --kind ci --scope '*:write' --never-expires --label "sprig-vm.sh on $(hostname)"
#   "The token is shown once": copy the sprigsvc_... value into secrets/sprig/ops-token (mode 0644 inside secrets/)
```

**Tested (laptop):** `operator create alice` with the bootstrap token; login gave an enrolment ticket and an `otpauth://` URI; enrolment with a TOTP code returned ten recovery codes and a session. From then on, the bootstrap token got 39310 on `cluster list`. A `sprigsvc_` token issued from alice's session (`--kind ci --scope '*:write' --never-expires`) in `secrets/sprig/ops-token` made `./sprig-vm.sh ctl cluster list` work again.

Add more operators with `sprigctl operator create` (a `dc` operator: `--role dc --dcs dc1`). If every operator is locked out, set `admin_token_break_glass = true` under `[controller]` in `config/controller.toml`, `docker compose up -d controller`, fix the accounts with the bootstrap token, then remove the setting. Every call made that way is audited as `breakglass.admin_token`.

## Tenants

```sh
echo 'a-long-password' | ./sprig-vm.sh tenant acme app       # tenant acme, user app: logs in as acme.app
```

Clients log in over TLS. **A tenant that needs plaintext MySQL logins without TLS** (old clients) must turn on `auth.mysql_native_password`, because the gateway does not do `caching_sha2_password`'s RSA key exchange (decision D4; bugfix-23): `./sprig-vm.sh ctl settings set auth.mysql_native_password true --scope tenant:acme --yes`, and add each such user with `--native`. This bundle's MySQL ports require TLS anyway (`listener.mysql.tls = required`); setting them to `optional` is the operator's choice per DC.

## Ports

| Port (default) | Protocol | TLS | Published on |
|---|---|---|---|
| 3306 | MySQL 8.4 persona | required | `SPRIG_BIND_IP` (all interfaces) |
| 3307 | MariaDB persona | required | `SPRIG_BIND_IP` |
| 8443 | REST, gRPC (ALPN h2), Studio | required | `SPRIG_BIND_IP` |
| 27017 | MongoDB | required | `SPRIG_BIND_IP` |
| 6379 | Redis | required | `SPRIG_BIND_IP` |
| 7687 | Bolt | required | `SPRIG_BIND_IP` |
| 9092 | Kafka | required | `SPRIG_BIND_IP` |
| 5671 | AMQP 0-9-1 and 1.0 | required | `SPRIG_BIND_IP` |
| 7480 | controller Admin API (bearer token, plain HTTP) | **no** | 127.0.0.1 only: use SSH |
| 9090 | Prometheus (profile `monitoring`) | no | 127.0.0.1 only |
| 2379, 3306, 9180, 7481 | etcd (mTLS), MariaDB (TLS), telemetry, controller internal | etcd and MariaDB yes | inside the Compose network only |

Every port number is a variable in `.env`. The gateway listens inside its container on the same number it is published on, so Kafka advertises the address clients really use (`SPRIG_PUBLIC_HOST:SPRIG_KAFKA_PORT`). If a VM also runs RabbitMQ, MongoDB or Redis, move SprigDB's ports in `.env` (the laptop test used 3xxxx ports).

## Adding a cluster

The default backend is the `mariadb-1` container. To add capacity:

- **Another MariaDB container on this VM:** `compose.mariadb-2.example.yaml` adds `mariadb-2` (it extends `mariadb-1` with `--server-id=201 --gtid-domain-id=2` and its own volume). Copy it to `compose.mariadb-2.yaml`, add `-f compose.mariadb-2.yaml` wherever `compose.yaml` is used (or set `COMPOSE_FILE=compose.yaml:compose.mariadb-2.yaml` in `.env`), start it, then register it:
  `./sprig-vm.sh ctl cluster register dc1-c2 --dc dc1 --topology single --node mariadb-2:3306 --credentials file:/run/sprig/sprig/cluster-credentials.toml --memory-gb 4 --storage-gb 100 --yes`
  (the certificate in `secrets/tls/mariadb.pem` already covers `mariadb-1` to `mariadb-3`). **Tested (laptop):** prechecks passed and the cluster was active and healthy in 1 s. A tenant created with `--cluster vm1-c2` wrote and read through the gateway, and its schemas were on mariadb-2.
- **MariaDB on another VM, or a managed server:** create `sprig_ctl` and `sprig_repl` there as chapter 27 ("External registration") says, with TLS required. Put the server's CA into a credentials file under `secrets/sprig/`, then register it with `--topology single|primary_replica|galera|external` and one `--node host:port` per node, the writer first. The controller prechecks binlog format, GTID mode, privileges and TLS, and refuses a server that doesn't meet chapter 27's table.

Tenants placed on the new cluster afterwards, or moved there with `sprigctl shard move`, use it. Existing tenants stay where they are.

## Backups

`./sprig-vm.sh backup` writes `backups/<UTC timestamp>/`:

- `mariadb-1/`: a `mariadb-backup` copy of the whole server, already prepared (consistent and ready to copy back);
- `etcd.db`: an etcd snapshot (shard maps, settings, operators, cluster records);
- `image.env`: the image that was running.

It covers `mariadb-1` only. A second MariaDB container ("Adding a cluster") needs the same `mariadb-backup` step for it, which the script doesn't do yet.

Run it from cron or a systemd timer (for example hourly), and copy `backups/` off the VM (object storage, another host). Keep `secrets/` backed up separately. Backups are not encrypted by this script; encrypt them when they leave the VM.

**Consistency.** The MariaDB copy and the etcd snapshot are taken a few seconds apart. Take backups when no move, split or merge is running (`./sprig-vm.sh ctl mover list`), or pause the balancer first (`ctl settings set balancer.mode off --scope dc:dc1`). A job that ran between the two copies can leave a shard map that points at a schema state from before or after it.

**Tested (laptop):** backup of a tenant with 3 tables (2,000 rows) plus the operator directory: 2 s, 59 MB. After that, 500 rows were deleted, a column updated, a table dropped, a table and a second tenant created. `restore` brought back exactly the backed-up state: `CHECKSUM TABLE` equal on all three tables, the dropped table back, the new table and the new tenant gone, logins and operator tokens working. A full restore took 29 s from `restore` to every healthcheck green.

## Restore

```sh
./sprig-vm.sh restore <timestamp>     # a directory name under backups/
```

It stops the gateway, worker, controller, MariaDB and etcd (the MariaDB healthcheck uses root's password file, because the image's own healthcheck account lives in the datadir the restore replaces); empties MariaDB's datadir and copies the backup back; restores etcd from the snapshot into a fresh data directory; starts everything and waits for the healthchecks. Everything written after the backup is gone. Clients reconnect.

To restore onto a new VM: copy the bundle, the same `secrets/` and the backup directory, `docker load` the image named in `image.env`, `./sprig-vm.sh up` once (it creates the volumes), then `./sprig-vm.sh restore <timestamp>`.

## Upgrade and rollback

```sh
./sprig-vm.sh upgrade sprigdb:<new version>    # takes a backup, then controller → worker → gateway
./sprig-vm.sh rollback                         # back to the image before that upgrade
```

`upgrade` records the running image in `.state/previous-image`, takes a backup (`backups/pre-upgrade-<ts>/`), changes `SPRIGDB_IMAGE` in `.env` and recreates the controller, then the worker, then the gateway, each after the previous one is healthy. The gateway drains on stop: open sessions get up to `gateway.drain_timeout_seconds` (30 s) to finish, and new connections are refused for the few seconds the container is replaced. With one gateway per VM that is a short outage on this VM's ports. Put two VMs' gateways behind a load balancer and upgrade one VM at a time to avoid it.

`rollback` swaps the image back in the same order. It is safe when the new build hasn't changed a stored format. If a release note says a version migrates the catalog or etcd records, rolling back means restoring the `pre-upgrade` backup instead: `./sprig-vm.sh restore pre-upgrade-<ts>` with the old image.

**With one controller per DC, its restart fences writes for a few seconds.** The gateway holds writes to a valid DC lease, which the controller renews (chapter 25's self-fencing). While the only controller restarts, writes get **39501** ("data center ... is fenced") for about 3–5 s (reads weren't measured). Clients that retry ride through it. To avoid it, run a second controller (another VM), or accept it for a single-VM deployment.

**Tested (laptop):** build A (tree of 16:24 IST, 11 Nov) → build B (tree of 17:38) → back to A, with one client inserting a row every 0.2 s on a new TLS connection each time:

| Step | Took | Client errors | Acknowledged rows lost |
|---|---|---|---|
| `upgrade sprigdb:harden1-b` (backup 2 s, then three roles) | 33 s | 30 failed inserts in 7 s: 26 × 39501 (controller restart), 4 × 2003/2013 (gateway restart) | 0 |
| `rollback` | 33 s | 15 failed inserts: 8 × 39501 over 3 s, 7 × 2003/2013 over 5 s | 0 |

All 299 acknowledged inserts were in the table afterwards, and no row appeared that wasn't acknowledged. The nine-protocol smoke test passed after each step.

## Rotating certificates

```sh
./sprig-vm.sh rotate-certs     # needs secrets/ca/ca-key.pem back on the VM
```

It makes new gateway, etcd and MariaDB certificates from the same CA. **Tested (laptop):** the gateway served the new certificate 1 s after the script finished, a MySQL session in the middle of a 15 s statement finished normally, and etcd and MariaDB took new connections with their new certificates. Each file is replaced by rename, so a reader never sees half a file. The gateway re-reads its bundle within two polls (a few seconds; log event `gateway.cert_rotated`) without dropping connections. MariaDB gets `FLUSH SSL`, and etcd picks up new certificates on new connections. To use certificates from your own CA, write them to `secrets/tls/gateway.pem` and `gateway-key.pem` (key first, then the certificate, each by `mv`). The generated certificates last `SPRIG_CERT_DAYS` (397) days: put a reminder or a timer on it. `sprig_gateway_cert_expiry_seconds` (and the `SprigCertExpiringSoon` alert) warns 14 days ahead.

Rotating the CA itself: make a new CA, put both CA certificates into `ca.pem` (old and new), restart everything once (`docker compose up -d --force-recreate`), then issue new certificates from the new CA and remove the old CA from `ca.pem`.

## Monitoring

- **Tested (laptop):** Prometheus scraped all three SprigDB targets (`up` = 3) and loaded the 35 alert rules; none fired on an idle deployment.
- `docker compose --profile monitoring up -d prometheus` starts Prometheus on 127.0.0.1:9090. It scrapes the gateway, controller and worker (`/metrics` on 9180 inside the network) and loads the alert rules from `deploy/alerts/` (`sprig-ops`, `sprig-slo`, `sprig-approvals`). To use your own Prometheus instead, scrape the three containers' port 9180 from inside the Compose network, or publish it.
- **Dashboards:** import `deploy/dashboards/*.json` (fleet, DC, cluster, tenant, data movement, approvals) into Grafana with the Prometheus data source.
- **Alertmanager** is not in the bundle: point Prometheus's `alerting:` section at yours.
- MariaDB itself has no exporter here; SprigDB's own metrics cover connections, latency, replication lag and probe results per cluster. Add `prom/mysqld-exporter` if you want server internals.

## Logs

- Every container logs to Docker's `json-file` driver, rotated at `SPRIG_LOG_MAX_SIZE` (50 MB) × `SPRIG_LOG_MAX_FILES` (5) per container.
- SprigDB's processes log JSON lines (`SPRIGDB_TELEMETRY_LOG_FORMAT=json`): `docker compose logs -f gateway | jq .`
- MariaDB's error log is its container log: `docker compose logs mariadb-1`. The SST and backup logs are in the datadir (`mariadb-backup.*.log`) and in `backups/<ts>/`.
- Audit events (operator changes, break glass, tenant admin) are log lines with `"audit":true`. Ship the container logs (journald, Loki, a SIEM) if you need to keep them longer than the rotation.

## Common failures

| Symptom | Cause | Fix |
|---|---|---|
| `up` waits, `controller` unhealthy, log says `etcd ... certificate` | wrong or missing `secrets/tls/client*.pem`, or etcd's certificate lacks the name `etcd` | re-run `init` only for a new deployment; otherwise re-issue with `rotate-certs` |
| Every login fails with 2061 "requires secure connection" | a plaintext client against a TLS-required port, or `--ssl-mode=DISABLED` | use TLS; for old clients see "Tenants" (`auth.mysql_native_password`) |
| 39310 from `sprig-vm.sh ctl` | named operators took over; the bootstrap token is refused | issue `secrets/sprig/ops-token` (First operator) |
| A listener doesn't come up | another process holds the port on the VM | change the port in `.env`, `./sprig-vm.sh configure` |
| MariaDB container restarts, exit 137 | out of memory | raise `MARIADB_MEM_LIMIT` or lower `MARIADB_BUFFER_POOL` and `MARIADB_MAX_CONNECTIONS` |
| `cluster register` precheck fails on TLS | the server's certificate doesn't cover the name you registered | register the name in the certificate (`mariadb-1`), or re-issue |
| Kafka clients connect and then time out | `SPRIG_PUBLIC_HOST` isn't the name clients reach | fix it in `.env`, `./sprig-vm.sh configure` |
| Backups fail with "No space left" | `backups/` on the root disk | mount a data disk at `backups/`; prune old backups |

## Galera: a node that can't rejoin

Seen on the dev stack on 11 November 2026 (decision HD1). A node takes a full copy (SST) and then stops with `Failed to parse uuid:seqno pair`, `IST received: 00000000-…:-1` and `Sending JOIN failed: -103`. Check `SHOW STATUS LIKE 'wsrep_cluster_state_uuid'` on a running node: if it is all zeros, the cluster has lost its identity (seen after every node crashed at once), and no node can join it. Fix (needs a short outage of the cluster):

1. Stop the other nodes cleanly, one at a time (`docker stop -t 300`), the most up-to-date node (highest `wsrep_last_committed`) last.
2. In that node's datadir, keep a copy of `grastate.dat` and write a new one with a fresh UUID (`uuidgen`), `seqno:` = its last committed position and `safe_to_bootstrap: 1`.
3. Start it (it bootstraps a new primary component with that UUID), then start the others one by one: each takes an SST or IST and joins.
4. Check: every node `Synced`, `wsrep_cluster_size` = the node count, the UUID not zero, and the schema and table counts as before.

A node that joins under heavy load can also fail because the donor loses contact with the cluster mid-copy (`BACKUP STAGE START: WSREP has not yet prepared node`). Chapter 27's Galera options with this bundle's timeouts (`evs.suspect_timeout=PT30S`, `evs.inactive_timeout=PT60S`) and a gcache large enough for IST avoid it.

## Not done yet

- **Run on a real VM** (Kiran decides when; nothing has touched the dev or stage VMs).
- **One gateway per VM:** two gateways on one VM would need different ports. HA means a second VM running only the gateway (a `gateway-only` compose file pointing at the first VM's etcd), which isn't written yet.
- **The Admin API is plain HTTP** (loopback only, bearer token): the controller has no TLS listener yet (chapter 37's 8443 relay through a gateway is TLS: `sprigctl --endpoint https://<gateway>:8443`).
- **One etcd member:** etcd is a single point of failure for control (not for data). Three members across VMs are chapter 23's production layout.
- **Backups aren't encrypted or shipped** by the script.
