# SprigDB on VMs with Docker Compose: runbook

This bundle runs one SprigDB data center on one VM (etcd, one MariaDB 11.8 node, the controller, the worker and one gateway), or on three VMs for high availability (an etcd member, a controller and a gateway on each; MariaDB and the worker on the first). Every client port is TLS, the controller's Admin API is HTTPS, etcd uses mutual TLS, backups are encrypted, and passwords, tokens and keys are read from files that are checked at start. Chapter 27 ("Compose on VMs") describes the design; this file is the procedure.

It was tested on the laptop (Docker Desktop, arm64) on 11 November 2026: install, first operator, tenant, a smoke test on all nine client protocols, backup and restore, upgrade and rollback between two builds, and certificate rotation (hardening round 1); then three "VMs" as three Compose projects on separate networks, reaching each other only through published ports, with three etcd members, two controllers and two gateways, an upgrade and rollback with a client writing throughout, losing an etcd member, encrypted backups copied off the VM and restored, and the secrets checks in a Linux container (round 2). "Tested" notes below give the numbers. It has **not** run on a real VM yet.

Contents: [Files](#files) · [Install](#install) · [High availability](#high-availability-three-vms) · [First operator](#first-operator) · [Tenants](#tenants) · [Ports](#ports) · [Adding a cluster](#adding-a-cluster) · [Backups](#backups) · [Restore](#restore) · [Upgrade and rollback](#upgrade-and-rollback) · [etcd members](#etcd-members) · [Rotating certificates](#rotating-certificates) · [Monitoring](#monitoring) · [Logs](#logs) · [Common failures](#common-failures) · [Galera: a node that can't rejoin](#galera-a-node-that-cant-rejoin) · [Not done yet](#not-done-yet)

## Files

| File | What it is |
|---|---|
| `compose.yaml` | the services, limits, healthchecks, volumes and ports (one VM, or the first of three with the next file) |
| `compose.ha.yaml` | the first VM of three: publishes etcd, MariaDB, the controller's internal port and the gateway's peer port on the private network |
| `compose.gateway-vm.yaml` | the second and third VMs: an etcd member, a controller and a gateway (a worker under the profile `worker`), no MariaDB |
| `compose.mariadb-2.example.yaml` | a second MariaDB container as its own cluster ([Adding a cluster](#adding-a-cluster)) |
| `.env.example` | every value you set per VM; copy to `.env` |
| `sprig-vm.sh` | init, check-secrets, up-etcd, up, configure, status, tenant, backup, verify-backup, restore, etcd-rejoin, upgrade, rollback, rotate-certs, stop, down |
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
   - `tls/`: the CA certificate and the gateway, etcd, MariaDB, controller (the HTTPS Admin API) and process certificates. The process certificate (`client.pem`) is every SprigDB process's identity: etcd's client certificate and both ends of the gateways' peer service.
   - `mariadb/`: the root, `sprig_ctl` and `sprig_repl` passwords.
   - `sprig/`: the admin (bootstrap) token, the gateways' token, the workload master key, cluster credentials and the gateway's backend-account secret.
   - `backup/`: the backups' key pair. `backup-pub.pem` stays (each backup is encrypted to it); **move `backup-key.pem` off the VM** with the CA key: it is needed only to restore.
   `secrets/` itself is mode 0700; the files in it are 0644, because each container reads them as its own user (sprigdb 10001, mysql 999, etcd root); the two offline keys are 0600. **Back up `secrets/` now**, somewhere other than the data backups: without it, a restored backup can't be opened (the workload key and the backend secret are in it).
5. **Start:** `./sprig-vm.sh up`. It checks `secrets/` first ([Secrets checks](#secrets-checks)), then starts the containers, waits for every healthcheck, registers the MariaDB node as cluster `<SPRIG_DC>-main` and writes the listener settings (TLS required on every listener; plain HTTP and plain AMQP off). The listeners come up within a few seconds.
6. **Check:** `./sprig-vm.sh status`, then create a tenant ([Tenants](#tenants)) and run the smoke test from a machine that reaches the ports: `SMOKE_PASSWORD=... smoke/smoke.sh acme.app <public host>`. It logs in over TLS (verifying the certificate against `secrets/tls/ca.pem`) on MySQL, MariaDB, MongoDB, REST, gRPC, Redis, Bolt, Kafka and AMQP, does one operation on each, and checks that a plaintext MySQL login is refused.

**Tested (laptop, project `sprigvm`, ports 3xxxx):** `up` took 19 s from nothing to every healthcheck green, including the cluster registration (TLS verified against the deployment CA). The smoke test passed on all nine protocols plus the plaintext refusal; each check took 4–107 ms after login (Redis 107 ms, the rest under 40 ms).

To use your own certificates instead of the generated ones, put them at the same paths in `secrets/tls/` (`gateway.pem` with the full chain and `gateway-key.pem` for clients; the CA that signed `etcd.pem`, `mariadb.pem`, `controller.pem` and `client.pem` as `ca.pem`).

**A bundle installed before round 2** lacks the controller's certificate and the backups' key pair, and its `client.pem` can't serve the peer service. `up` makes what is missing; for the two certificates it needs `secrets/ca` back on the VM for that one run.

### Secrets checks

`up`, `up-etcd` and `./sprig-vm.sh check-secrets` refuse to go on, listing each problem with the command that fixes it, unless:

- `secrets/` is owned by the user running the script and closed to everyone else (0700);
- everything inside is owned by that user (or root), is not a symbolic link, and isn't writable by group or others;
- the files the containers read are readable by others (0644: each container reads them as its own uid, and `secrets/` being 0700 keeps other users of the VM out);
- the two offline keys, `secrets/ca/*` and `secrets/backup/backup-key.pem`, are 0600 when they are present (the script also reminds you to move them off).

The SprigDB processes check again when they start: a token, key or backends file that isn't a regular file or is writable by everyone stops the process with 39998 naming the file (chapter 47, "Secret files are checked at start").

**Tested (Linux, `debian:bookworm-slim`, as uid 1000):** 14 of 14 cases gave the expected answer: the layout `init` makes passes, and so do a file owned by root and a 0600 backup key; `secrets/` at 0755 or owned by root, a group-writable token, a world-writable key, a file owned by another uid, the CA key at 0644, the backup key at 0640, a certificate at 0600 (the containers couldn't read it), a symbolic link, a group-writable `tls/` and a missing `secrets/` are refused. Docker Desktop on the Mac maps ownership differently, which is why this ran in a Linux container.

## High availability (three VMs)

One VM is a single point of failure, and its upgrades refuse writes while its gateway restarts (and, with one controller, while the controller restarts). Three VMs on one private network remove both:

| VM | Compose files | Runs |
|---|---|---|
| 1 | `compose.yaml:compose.ha.yaml` | etcd member `etcd-vm1`, MariaDB, controller `ctl-1` (bootstraps the deployment), worker, gateway `gw-1` |
| 2 | `compose.gateway-vm.yaml` | etcd member `etcd-vm2`, controller `ctl-2`, gateway `gw-2` |
| 3 | `compose.gateway-vm.yaml` | etcd member `etcd-vm3`, controller `ctl-3`, gateway `gw-3` (or only the etcd member: `up-etcd`) |

- **etcd** has three members, one per VM, with mutual TLS between members and to every client. It keeps working with any two.
- **Every controller serves permits.** A gateway renews its write permit from any controller of its DC, so restarting one controller no longer fences writes; the DC-leader loops (balancer, mover dispatch, reconcilers) move to another controller within the leader lease.
- **Gateways behind a load balancer.** Put the VMs' client ports behind a TCP load balancer with a health check on each gateway's port (or give clients several hosts). A gateway that stops drains: it stops accepting, and the balancer sends new connections to the others.
- **MariaDB stays on VM 1** (or on managed or separate database servers): the other VMs reach it on the private network, and the cluster is registered at VM 1's private address. MariaDB itself isn't made highly available by this layout: use a primary/replica or Galera cluster, or a managed server, for that ([Adding a cluster](#adding-a-cluster)).

**Settings** (`.env.example`, "Several VMs"). The same on every VM: `SPRIG_DC`, `SPRIG_DEPLOYMENT_ID`, the client ports, `SPRIG_ETCD_INITIAL_CLUSTER`, `SPRIG_ETCD_SEEDS` and `SPRIG_INTERNAL_SANS` (every VM's private address). Per VM: `COMPOSE_FILE`, `SPRIG_VM_ID` (1, 2, 3), `SPRIG_CONTROLLER_BOOTSTRAP` (`true` on VM 1 only), `SPRIG_ETCD_NAME`, `SPRIG_PRIVATE_HOST` and `SPRIG_PRIVATE_IP` (this VM's private address) and `SPRIG_ADMIN_BIND_IP` (the private address too, so the other VMs' gateways can relay admin calls to whichever controller leads). Each internal port binds inside its container on the number it is published on, so the advertised address is the one the others reach.

**Install:**

1. On VM 1: `./sprig-vm.sh init` with `SPRIG_INTERNAL_SANS` set, so the etcd, MariaDB, controller and process certificates name every VM.
2. Copy `secrets/` to VM 2 and VM 3 **without** `secrets/ca/` and `secrets/backup/backup-key.pem` (for example `rsync -a --exclude ca/ --exclude backup/backup-key.pem secrets/ vm2:/opt/sprigdb/secrets/`, then `chmod 700 secrets` there).
3. On every VM: `./sprig-vm.sh up-etcd`. Each member waits for a majority, so start all three within a few minutes of each other.
4. On VM 1: `./sprig-vm.sh up` (it registers MariaDB at `SPRIG_PRIVATE_HOST:SPRIG_BACKEND_PORT`), then on VM 2 and VM 3: `./sprig-vm.sh up` (they don't configure anything; the DC's settings are VM 1's).
5. `./sprig-vm.sh status` on any VM lists the three etcd members, and `./sprig-vm.sh ctl dc show <dc>` names the DC leader.

Open between the VMs only: etcd's client and peer ports, MariaDB's port, the controllers' Admin and internal ports, and the gateways' peer port. The client ports face the load balancer.

**If etcd refuses its peers** with `rejected connection on peer endpoint ... does not match any of DNSNames`, a member's connections arrive from an address its certificate doesn't name (NAT between the VMs, or Docker's userland proxy). Add that address to `SPRIG_INTERNAL_SANS` and re-issue the certificates, or set `SPRIG_ETCD_PEER_SKIP_SAN=true`: members still need a certificate from the deployment CA, but etcd stops matching it against the source address. The laptop test needed it (every "VM" was behind Docker Desktop's NAT).

@@HA-TESTED@@

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

`./sprig-vm.sh backup` runs on the VM with MariaDB (VM 1) and writes `backups/<UTC timestamp>/`:

- `<service>.xbstream.zst.enc` for **every MariaDB container on the VM** (`mariadb-1`, `mariadb-2`, ...): `mariadb-backup --stream=xbstream`, compressed with zstd and encrypted, as one stream;
- `etcd.db.zst.enc`: a snapshot of the whole etcd cluster (shard maps, settings, operators, cluster records), compressed and encrypted;
- `data-key.enc`: this backup's data key, encrypted to `secrets/backup/backup-pub.pem`;
- `MANIFEST`, `image.env` (the image that was running), `SHA256SUMS` (every file), and the tools' logs.

**Encryption.** Each backup gets a fresh 256-bit data key, made on the host. The data files are encrypted with it (AES-256-CTR, `openssl enc` with PBKDF2 and a salt), and the data key is encrypted with the backups' RSA-4096 public key (RSA-OAEP with SHA-256) into `data-key.enc`. The streams go straight from `mariadb-backup` through zstd and `openssl` inside the MariaDB container, which gets the key on stdin and keeps it in memory, so no unencrypted copy reaches the disk. The one exception is etcd's snapshot (etcdctl only writes files): it is encrypted the moment it is written and then shredded. The VM holds only the public key: someone who copies `backups/` or the off-VM copies, or the VM itself, can't read them. Restoring needs `backup-key.pem`, which you keep off the VM with the CA key. The tools are OpenSSL 3 and zstd, both already in the MariaDB image (Apache-2.0 and BSD licences); nothing is downloaded.

**Off the VM.** Set `SPRIG_BACKUP_COPY_TO` in `.env` to a directory (a separate disk, an NFS or SMB share, a mounted bucket) or to `host:path` (rsync over SSH). After each backup the encrypted files and `SHA256SUMS` are copied there; a directory copy is written as `<ts>.part`, checked against `SHA256SUMS` and only then renamed to `<ts>`. Old copies there are pruned by that storage's own retention. In `backups/` on the VM, the newest `SPRIG_BACKUP_KEEP` (7) backups are kept.

`./sprig-vm.sh verify-backup <ts> [dir]` checks a backup's files against `SHA256SUMS` without the key; elsewhere (the backup host, from cron), `sha256sum -c SHA256SUMS` in the backup's directory does the same.

Run `backup` from cron or a systemd timer (for example hourly). Keep `secrets/` backed up separately: a restored backup needs the same workload key and backend secret.

**Consistency.** The MariaDB copies and the etcd snapshot are taken a few seconds apart. Take backups when no move, split or merge is running (`./sprig-vm.sh ctl mover list`), or pause the balancer first (`ctl settings set balancer.mode off --scope dc:dc1`). A job that ran between the copies can leave a shard map that points at a schema state from before or after it.

@@BACKUP-TESTED@@

## Restore

```sh
./sprig-vm.sh restore <ts>              # from backups/<ts>
./sprig-vm.sh restore <ts> /mnt/backups # from the off-VM copy: fetched into backups/<ts> first
```

Put `backup-key.pem` back first (`secrets/backup/backup-key.pem`, mode 0600, or `SPRIG_BACKUP_KEY=<path>`). `restore` checks `SHA256SUMS`, opens the data key on the host (only the 64-character data key is decrypted there; it reaches the containers on stdin), stops this VM's SprigDB, MariaDB and etcd, then, for every MariaDB container: empties its datadir, decrypts and unpacks the stream into it and prepares it (`mariadb-backup --prepare`). etcd is restored from the snapshot as a one-member cluster (this VM's member). Then it starts everything and waits for the healthchecks. Everything written after the backup is gone; clients reconnect. Move the private key off the VM again afterwards.

**With three VMs**, the other two must not run while VM 1 restores:

1. On VM 2 and VM 3: `./sprig-vm.sh stop`.
2. On VM 1: `./sprig-vm.sh restore <ts>`. etcd comes back with one member (VM 1's), which is a majority of one.
3. On VM 2, then VM 3: `./sprig-vm.sh etcd-rejoin` (it adds the member back and starts it empty; it copies the data from VM 1's member), then `./sprig-vm.sh up`.

To restore onto a new VM: copy the bundle, the same `secrets/` and the backup directory, `docker load` the image named in `image.env`, `./sprig-vm.sh up` once (it creates the volumes), then `./sprig-vm.sh restore <ts>`.

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
