# SprigDB on VMs with Docker Compose: runbook

This bundle runs one SprigDB data center on one VM (etcd, one MariaDB 11.8 node, the controller, the worker and one gateway), or on three VMs for high availability (an etcd member, a controller and a gateway on each; MariaDB and the worker on the first). Every client port is TLS, the controller's Admin API is HTTPS, etcd uses mutual TLS, backups are encrypted, and passwords, tokens and keys are read from files that are checked at start. Chapter 27 ("Compose on VMs") describes the design; this file is the procedure.

It was tested on the laptop (Docker Desktop, arm64) on 11 November 2026: install, first operator, tenant, a smoke test on all nine client protocols, backup and restore, upgrade and rollback between two builds, and certificate rotation (hardening round 1); then three "VMs" as three Compose projects on separate networks, reaching each other only through published ports, with three etcd members, two controllers and two gateways, an upgrade and rollback with a client writing throughout, losing an etcd member, encrypted backups copied off the VM and restored, and the secrets checks in a Linux container (round 2). "Tested" notes below give the numbers. Round 3 added mutual TLS on the controllers' internal port, MariaDB high availability across the three VMs (a primary and two semi-sync replicas failed over by SprigDB), a load balancer, certificate rotation without restarts, Prometheus across VMs, and growing a one-VM bundle to three. It has **not** run on a real VM yet.

Before the first run on real VMs, go through [GO-LIVE.md](GO-LIVE.md): what to prepare (VMs, network, names, certificates, secrets, backups, monitoring).

Contents: [Files](#files) · [Install](#install) · [High availability](#high-availability-three-vms) · [MariaDB high availability](#mariadb-high-availability) · [Load balancer](#load-balancer) · [From one VM to three](#from-one-vm-to-three) · [First operator](#first-operator) · [Tenants](#tenants) · [Ports](#ports) · [Adding a cluster](#adding-a-cluster) · [Backups](#backups) · [Restore](#restore) · [Upgrade and rollback](#upgrade-and-rollback) · [etcd members](#etcd-members) · [Rotating certificates](#rotating-certificates) · [Monitoring](#monitoring) · [Logs](#logs) · [Common failures](#common-failures) · [Galera: a node that can't rejoin](#galera-a-node-that-cant-rejoin) · [Not done yet](#not-done-yet)

## Files

| File | What it is |
|---|---|
| `compose.yaml` | the services, limits, healthchecks, volumes and ports (one VM, or the first of three with the next file) |
| `compose.ha.yaml` | the first VM of three: publishes etcd, MariaDB, the controller's internal port and the gateway's peer port on the private network |
| `compose.gateway-vm.yaml` | the second and third VMs: an etcd member, a controller and a gateway (a worker under the profile `worker`), no MariaDB |
| `compose.mariadb-2.example.yaml` | a second MariaDB container as its own cluster ([Adding a cluster](#adding-a-cluster)) |
| `compose.db-ha.yaml` | MariaDB high availability: a node and its agent on every VM, a primary and two semi-sync replicas as one cluster ([MariaDB high availability](#mariadb-high-availability)) |
| `compose.lb.yaml` | the load balancer (Envoy) in front of the gateways ([Load balancer](#load-balancer)) |
| `config/agent.toml` | the node agent's process file (`compose.db-ha.yaml`) |
| `config/prometheus.ha.yml` | Prometheus scraping every VM ([Monitoring](#monitoring)) |
| `GO-LIVE.md` | what to prepare before the first run on real VMs |
| `.env.example` | every value you set per VM; copy to `.env` |
| `sprig-vm.sh` | init, check-secrets, up-etcd, up, configure, status, ctl, tenant, backup, verify-backup, restore, etcd-rejoin, upgrade, rollback, rotate-certs, reload-certs, db-status, lb-config, lb-up, monitoring-config, stop, down |
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

1. **Get the image** onto the VM. On a build machine: `deploy/compose-vm/build-image.sh sprigdb:<version>`, then `docker save sprigdb:<version> | gzip > sprigdb-<version>.tar.gz`, copy it over, `docker load < sprigdb-<version>.tar.gz`. (A registry works too: tag and push, then set `SPRIGDB_IMAGE` to the registry name.) The build runs in a container capped at 6 GB with 2 jobs and 16 codegen units (the workspace's one-unit release profile needs more than 6 GB for `sprig-gateway`; on a 12 GB builder, `SPRIG_BUILD_CGU=1` gives the exact profile). It took about 25 minutes on the laptop from a cold cache in round 1, 45 minutes at a load of 40–68 in round 2, and 27 minutes with the cache volume warm.
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

Two layouts. **With MariaDB high availability** (the default for production, hardening round 3), every VM runs the same files and the same services:

| VM | Compose files | Runs |
|---|---|---|
| 1, 2, 3 | `compose.yaml:compose.ha.yaml:compose.db-ha.yaml` | etcd member `etcd-vm<n>`, MariaDB node `db<n>` and its agent `agt-<n>`, controller `ctl-<n>` (VM 1's bootstraps the deployment), worker `wkr-<n>`, gateway `gw-<n>` |

**With MariaDB on VM 1 only** (or on managed or separate database servers; round 2's layout):

| VM | Compose files | Runs |
|---|---|---|
| 1 | `compose.yaml:compose.ha.yaml` | etcd member `etcd-vm1`, MariaDB, controller `ctl-1` (bootstraps the deployment), worker, gateway `gw-1` |
| 2 | `compose.gateway-vm.yaml` | etcd member `etcd-vm2`, controller `ctl-2`, gateway `gw-2` |
| 3 | `compose.gateway-vm.yaml` | etcd member `etcd-vm3`, controller `ctl-3`, gateway `gw-3` (or only the etcd member: `up-etcd`) |

- **etcd** has three members, one per VM, with mutual TLS between members and to every client. It keeps working with any two.
- **Every controller serves permits.** A gateway renews its write permit from any controller of its DC, so restarting one controller no longer fences writes; the DC-leader loops (balancer, mover dispatch, reconcilers) move to another controller within the leader lease.
- **The controllers' internal port is mutual TLS** (`SPRIG_INTERNAL_TLS`, true by default in `compose.ha.yaml`): each controller serves permits, peer pings, observations and the relay with its process certificate and requires one from every caller; gateways, workers, agents and the other controllers call it with theirs (decision HD18).
- **Gateways behind a load balancer** ([Load balancer](#load-balancer)): Envoy (`compose.lb.yaml`) or the cloud's, with a health check on each gateway's `/readyz`. A gateway that stops drains: its readiness turns off, the balancer stops sending it new connections, then it closes its listeners.
- **MariaDB:** a primary and two replicas, one per VM, failed over by SprigDB ([MariaDB high availability](#mariadb-high-availability)); or, in round 2's layout, one node on VM 1 that the other VMs reach on the private network (not highly available by itself).

**Settings** (`.env.example`, "Several VMs"). The same on every VM: `SPRIG_DC`, `SPRIG_DEPLOYMENT_ID`, the client ports, `SPRIG_ETCD_INITIAL_CLUSTER`, `SPRIG_ETCD_SEEDS` and `SPRIG_INTERNAL_SANS` (every VM's private address). Per VM: `COMPOSE_FILE`, `SPRIG_VM_ID` (1, 2, 3), `SPRIG_CONTROLLER_BOOTSTRAP` (`true` on VM 1 only), `SPRIG_ETCD_NAME`, `SPRIG_PRIVATE_HOST` and `SPRIG_PRIVATE_IP` (this VM's private address) and `SPRIG_ADMIN_BIND_IP` (the private address too, so the other VMs' gateways can relay admin calls to whichever controller leads). Each internal port binds inside its container on the number it is published on, so the advertised address is the one the others reach.

**Install:**

1. On VM 1: `./sprig-vm.sh init` with `SPRIG_INTERNAL_SANS` set, so the etcd, MariaDB, controller and process certificates name every VM.
2. Copy `secrets/` to VM 2 and VM 3 **without** `secrets/ca/` and `secrets/backup/backup-key.pem` (for example `rsync -a --exclude ca/ --exclude backup/backup-key.pem secrets/ vm2:/opt/sprigdb/secrets/`, then `chmod 700 secrets` there).
3. On every VM: `./sprig-vm.sh up-etcd`. Each member waits for a majority, so start all three within a few minutes of each other.
4. On VM 1: `./sprig-vm.sh up` (it registers MariaDB at `SPRIG_PRIVATE_HOST:SPRIG_BACKEND_PORT`), then on VM 2 and VM 3: `./sprig-vm.sh up` (they don't configure anything; the DC's settings are VM 1's).
5. `./sprig-vm.sh status` on any VM lists the three etcd members, and `./sprig-vm.sh ctl dc show <dc>` names the DC leader.

**Admin calls that only the DC leader serves** (tenant creation and deletion, for example) get 39504 "send the request to the leader, ctl-N" from the other controllers. `./sprig-vm.sh ctl` then sends the call again to the leader's controller, at the Admin address the leader registered (its VM's private address), and says so. **Tested (round 3):** `ctl tenant create` and `ctl tenant delete` on VM 2 while `ctl-1` led: both went to `ctl-1` and succeeded.

**Kafka clients** connect to each gateway directly: Kafka's Metadata lists every gateway as a broker, each under its own `SPRIG_KAFKA_HOST` (`[gateway] kafka_advertise_host`, published in its registration; hardening round 3), or its registered address (`SPRIG_PRIVATE_HOST`) when unset. Give each VM a public name clients resolve (`db1.example.com`, ...) and open each VM's Kafka port to them; the load balancer serves only their first connection. **Tested (round 3):** through the balancer, Metadata named the three gateways `127.0.0.1`, `localhost` and `kafka-vm3.invalid`, each VM's own setting (the laptop can't publish three Kafka ports under one number, so producing through each broker wasn't tried).

Open between the VMs only: etcd's client and peer ports, MariaDB's port, the controllers' Admin and internal ports, the gateways' peer port, and the telemetry ports 9180–9183 (also to the load balancer and Prometheus). The client ports face the load balancer (and each VM's Kafka port, the Kafka clients).

**If etcd refuses its peers** with `rejected connection on peer endpoint ... does not match any of DNSNames`, a member's connections arrive from an address its certificate doesn't name (NAT between the VMs, or Docker's userland proxy). Add that address to `SPRIG_INTERNAL_SANS` and re-issue the certificates, or set `SPRIG_ETCD_PEER_SKIP_SAN=true`: members still need a certificate from the deployment CA, but etcd stops matching it against the source address. The laptop test needed it (every "VM" was behind Docker Desktop's NAT).

**Tested (laptop, 11 November 2026):** three Compose projects on separate Docker networks, reaching each other only through published ports on `host.docker.internal` (`SPRIG_ETCD_PEER_SKIP_SAN=true`, see above); VM 3 ran only its etcd member. `up` on VM 1 took 14 s and on VM 2 12 s once the three members were up; the cluster registered at VM 1's private address with TLS verified. The nine-protocol smoke test passed on VM 1's gateway, and on VM 2's except for Kafka, which named the DC's Kafka port where the laptop had remapped VM 2's (a laptop-only effect). `KILL QUERY` sent to one gateway ended a statement on the other through the peer service within 3 s. Upgrades, etcd failures and backups: their sections below.

## MariaDB high availability

`compose.db-ha.yaml` puts one MariaDB node on each of the three VMs: a primary and two replicas with semi-synchronous replication that waits `AFTER_SYNC` (a commit is acknowledged only once a replica has it), registered as one SprigDB cluster of topology `primary_replica`. A SprigDB **agent** runs next to each node. When the primary is lost, the DC leader promotes the replica that holds every acknowledged commit and points the other replica at it; when the old primary comes back, it rejoins as a replica (chapter 25, "Primary + replicas, driven by SprigDB").

Why this and not Galera (decision HD19): cross-cluster transactions (XA) and online index builds need a non-Galera cluster (chapter 09's capability rules), and this layout was already failed over by SprigDB with no acknowledged write lost (chapter 25's live tests). Galera remains possible: register a Galera cluster as in [Adding a cluster](#adding-a-cluster).

| VM | Node | At first | Agent |
|---|---|---|---|
| 1 | `db1` (server id 101) | primary | `agt-1` |
| 2 | `db2` (server id 102) | replica of `db1` | `agt-2` |
| 3 | `db3` (server id 103) | replica of `db1` | `agt-3` |

- **Every node boots `read_only`, with semi-sync's primary side off.** Its agent clears `read_only` only on the node that holds the cluster's writer lease while its DC permit is valid, and switches semi-sync's primary side on once a replica is attached (also after the primary's mysqld restarts). Any other node is fenced: `read_only`, tenant sessions killed.
- **Replication is TLS**, verified against the deployment CA (`MASTER_SSL_CA`), with `sprig_repl`. The MariaDB certificate names every VM's private address (`SPRIG_INTERNAL_SANS`).
- **Each node creates its own accounts** on first start without writing them to the binlog, so a new replica's binlog is empty and it replicates the primary from its first GTID.

**Settings** (`.env.example`, "MariaDB high availability"), on every VM: `COMPOSE_FILE=compose.yaml:compose.ha.yaml:compose.db-ha.yaml` (all three VMs run the full `compose.yaml`: a worker on each too), `SPRIG_DB_NODES=db1=<VM 1 private>:<backend port>,db2=...,db3=...` and `SPRIG_DB_PRIMARY=db1`. `SPRIG_CONTROLLER_BOOTSTRAP=true` on VM 1 only.

**Install** (after `init` and copying `secrets/`, as in [High availability](#high-availability-three-vms)):

1. On every VM: `./sprig-vm.sh up-etcd`.
2. On VM 1: `./sprig-vm.sh up`. It writes the DC's settings; its MariaDB is the primary. It says it waits for the replicas before registering the cluster.
3. On VM 2, then VM 3: `./sprig-vm.sh up`. Each MariaDB starts replicating `db1` over TLS.
4. On VM 1: `./sprig-vm.sh configure`. With both replicas attached, it opens `db1` once, turns on semi-sync's primary side and registers `<dc>-main` with the three nodes, the writer first. Then run `./sprig-vm.sh configure` on VM 2 and VM 3: each writes the cluster's id into `.env` (`SPRIG_DB_CLUSTER_ID`) and restarts its agent if it changed.
5. `./sprig-vm.sh status` (or `db-status`) on each VM: `db1` has `read_only=0` and 2 replicas acknowledging; `db2` and `db3` replicate with IO and SQL running.

**When the primary is lost** nothing needs doing: the agent of a stopped mysqld hands the writer lease back (or its lease expires with its VM), the DC leader waits `failover.cluster_detect_seconds` (5 s) and `failover.clock_drift_margin_ms`, drains the replicas' relay logs, promotes the one that received the most, and repoints the other. Writes fail for those seconds (clients retry), then go to the new primary; the record (`./sprig-vm.sh ctl cluster show <dc>-main`) lists the failover with its detection time, its recovery time and the loss estimate. **Bring the old primary back** with `docker compose start mariadb-1` (or by restarting its VM): it boots `read_only`, and the controller rejoins it as a replica of the new primary (or, if it had commits nobody else received, keeps it out with a report: chapter 25, "Recovery after an outage").

**A replica that lost its data**, or a VM rebuilt from scratch: an empty node replicates from the current primary's first GTID only while the primary still has every binlog (`binlog_expire_logs_seconds`, 7 days). Otherwise seed it from a backup of the primary first (`mariadb-backup --stream` into its empty datadir), set `gtid_slave_pos` from the backup's `mariadb_backup_binlog_info`, then `./sprig-vm.sh configure`.

**Backups** run on any VM (`backup` streams the local node); take them on the primary's VM, which has the newest data, or on a replica to keep the load off the primary.

**Tested (laptop, round 3; three Compose projects as three VMs, ports 42xxx–44xxx; one client inserting a row every 0.05 s through the load balancer, each on a new TLS connection):**

| What was lost | SprigDB's record | Writes refused (client) | Acknowledged writes lost | Afterwards |
|---|---|---|---|---|
| `db1`'s mysqld killed (`docker kill`), the rest of VM 1 running | detected 5.1 s, writer `db2` 5.4 s after the kill, loss estimate 1 s (semi-sync on) | 5 of 845, for 8.6 s | **0** (840 acknowledged, 840 in the table) | `db3` repointed at `db2`; `db1` started again rejoined as a replica in 5 s, over verified TLS |
| all of VM 1 killed (its etcd member, controller, worker, agent, gateway and `db1`, the primary) | detected 8.7 s, writer `db2` 8.8 s after the kill | 5 of 810, for 10.6 s | **0** (805 of 805) | the balancer dropped VM 1's gateway; VM 1 started again: `db1` replicating `db2` 17 s later |
| the same with `sprigdb:harden3-a` (before decision HD20's fix) | detected 9.6 s, promoted 161 s after the kill, only once the lost VM came back | every write for 161 s | 0 | every control-plane etcd call had a one-in-three chance of failing on the dead member; see HD20 |

The cluster record stayed `zero_loss = yes` and `xa = yes` throughout; `./sprig-vm.sh ctl cluster list` showed it `degraded` while a node was missing and `healthy` once it was back.

## Load balancer

Clients reach the gateways through one address. `compose.lb.yaml` runs **Envoy** (Apache-2.0; image `envoyproxy/envoy:v1.34.1`) as a TCP load balancer: one listener per client port, each forwarding a connection to the ready gateway with the fewest open connections. TLS passes through untouched; clients check the gateways' certificate, so it must name the balancer's address (`SPRIG_PUBLIC_HOST`, `SPRIG_TLS_SANS`).

- **Health checks on readiness.** Every second Envoy asks each gateway's telemetry port (`GET /readyz`, `SPRIG_TELEMETRY_PORT_GATEWAY`, published on the private address by `compose.ha.yaml`); two failures take a gateway out, two successes bring it back. With no ready gateway it refuses connections rather than sending them to an unready one.
- **Drain-aware upgrades.** A stopping gateway turns `/readyz` off at once and keeps serving for `gateway.drain_lb_delay_seconds` (10 s), so the balancer stops sending it new connections within about 2 s, before it closes its listeners; open sessions continue until `gateway.drain_timeout_seconds` (30 s). A connection refused anyway (a gateway that died) is tried on another gateway (`max_connect_attempts: 3`).
- **Kafka** clients use the balancer only for their first connection: Kafka's Metadata then names every gateway by its own `SPRIG_KAFKA_HOST` (one public name per VM, set on each VM; `[gateway] kafka_advertise_host`), and clients connect to each directly. Those names must resolve, for the clients, to the VMs, and each VM's Kafka port must be open to them.

**Where it runs:** on a small VM of its own (1 vCPU, 1 GB is plenty), or on every VM behind a floating address (keepalived, VRRP) if the balancer must not be a single point of failure. A cloud's TCP load balancer does the same job: TCP to each client port, HTTP health check `GET /readyz` on the telemetry port, 1–2 s interval, 2 failures.

```sh
# on the load balancer's VM: this directory, an .env with the client ports and
SPRIG_LB_GATEWAYS=10.0.0.11,10.0.0.12,10.0.0.13
./sprig-vm.sh lb-up        # writes lb/envoy.yaml, starts compose.lb.yaml
curl -s http://127.0.0.1:9901/clusters | grep health_flags   # each gateway's state
```

It needs no secrets. Envoy's admin page (9901) is published on the VM's loopback only.

**Tested (laptop, round 3; Envoy as a fourth Compose project in front of the three VMs' gateways):** all eight listeners up, the three gateways healthy; the nine-protocol smoke test through the balancer passed MySQL, MariaDB, MongoDB, REST, gRPC, Redis, Bolt and AMQP and refused plaintext MySQL (Kafka's Metadata named each VM's own broker host, which the laptop can't route; see "Kafka clients" above). Upgrading the three VMs one after the other, with a client writing every 0.05 s: 0 of 1,019 writes refused while VM 2's and VM 3's gateways were replaced (VM 1's 5 refusals came from its MariaDB agent, see [Upgrade and rollback](#upgrade-and-rollback)). Over every test, Envoy recorded 0 failed upstream connects: it had taken each stopping or lost gateway out by its health check before sending it a connection.

## From one VM to three

A bundle installed on one VM (round 1 or 2) grows into the three-VM layout without losing its data. Its MariaDB stays on VM 1 (round 2's layout). Moving its tenants onto a highly available cluster afterwards is a second step: register a `compose.db-ha.yaml`-style cluster and drain the old one (`ctl cluster drain`; the mover copies each tenant online), then `ctl cluster retire`.

1. **New files, same data.** Copy this directory's new files over the old ones on VM 1 (keep `.env`, `secrets/`, `backups/`), `docker load` the new image. In `.env`, add the "Several VMs" values for VM 1 (`COMPOSE_FILE=compose.yaml:compose.ha.yaml`, the private address, `SPRIG_INTERNAL_SANS`, the three members in `SPRIG_ETCD_INITIAL_CLUSTER` and `SPRIG_ETCD_SEEDS`, the internal ports), keep `SPRIG_ETCD_NAME` as it was (`etcd-0`: the member's name is in its data), and set `SPRIGDB_IMAGE`.
2. **Bring `secrets/ca` back for one run**, then `./sprig-vm.sh up`. It makes what the old bundle lacks (the controller's certificate, the backups' key pair, a process certificate that also serves), re-issues the internal certificates for `SPRIG_INTERNAL_SANS`, recreates the containers with the new image, and moves the etcd member's peer URL from the Compose network to the private address (`member update`). Move `secrets/ca` and `secrets/backup/backup-key.pem` off again.
3. **Copy `secrets/`** to VM 2 and VM 3 (without the two offline keys).
4. **etcd from one member to three:** on VM 2, then VM 3, `./sprig-vm.sh etcd-rejoin` (it adds the member and starts it empty; it copies the data). While the second member joins, etcd has no majority for a few seconds and writes may be fenced (39501) briefly.
5. **The old registration's address.** A round-1 bundle registered its MariaDB as `mariadb-1:3306`, a name only VM 1's Compose network knows. On VM 1, point the record at the private address: `./sprig-vm.sh ctl cluster set-nodes <dc>-main --node n1=<VM 1's private address>:<SPRIG_BACKEND_PORT>` (the node id as `ctl cluster show <dc>-main` lists it; `--dry-run` prints the new list first). Every controller, worker and gateway connects to the new address from then on, and the next probe pins the server there (bugfix-40). The alias still works for a bundle that can't run the command yet: on VM 2 and VM 3 set `SPRIG_DB_ALIAS_HOST=mariadb-1` and `SPRIG_DB_ALIAS_IP=<VM 1's private address>` (`compose.gateway-vm.yaml` adds the host entry), and on VM 1 keep `SPRIG_BACKEND_PORT=3306`. (A round-2 bundle registered the private address already.)
6. On VM 2 and VM 3: `./sprig-vm.sh up` with `COMPOSE_FILE=compose.gateway-vm.yaml`. Then the load balancer ([Load balancer](#load-balancer)).

**Tested (laptop, round 3):** a round-1 bundle (the round-1 files from git, `init` and `up` of that round, one tenant with 500 rows; the round-1 image had been deleted, so the round-2 image `harden2-c` ran it) was moved to the round-3 files and `sprigdb:harden3-c` with `up` on VM 1: 36 s, everything above made, the member's peer URL moved, the 500 rows and their checksum unchanged. `etcd-rejoin` on VM 2 and VM 3 gave three members; `up` there started their controllers (internal port over mutual TLS, like VM 1's) and gateways. A row inserted through each VM's gateway was read back by the others (503 rows); a tenant created and deleted from VM 3 was forwarded to the leader on VM 1. The laptop can't give a container's `mariadb-1` the laptop's port 3306 (Kiran's MySQL is there), so VM 2 and VM 3 joined VM 1's Compose network instead of using `SPRIG_DB_ALIAS_*`.

## First operator

People who run SprigDB log in as **named operators** (chapter 14, "Operators"), with a password and TOTP. The bootstrap token in `secrets/sprig/admin-token` works only until the first operator logs in.

```sh
docker compose exec controller sprigctl --endpoint https://127.0.0.1:7480 --ca-file /run/sprig/tls/ca.pem \
    --token-file /run/sprig/sprig/admin-token operator create alice --role platform --display-name "Alice" --email alice@example.com
#   asks for alice's password (12 characters or more) on the terminal
```

(`sprig-vm.sh ctl` runs without a terminal, so it can't ask for a password. From a machine with `sprigctl` and the SSH tunnel below, `--password-file` works too.)

Then alice logs in from the VM, through an SSH tunnel to the Admin API (`ssh -L 7480:127.0.0.1:7480 vm`), with `sprigctl` and a copy of `secrets/tls/ca.pem` on her machine. The Admin API is HTTPS with a certificate from the deployment CA, valid for `127.0.0.1`, `localhost` and `controller` (and the private addresses in `SPRIG_INTERNAL_SANS`):

```sh
export SPRIGCTL_CA_FILE=~/sprigdb-ca.pem
sprigctl --endpoint https://127.0.0.1:7480 login --name alice
#   first login: shows an otpauth:// URI; add it to an authenticator app, type a code;
#   prints ten recovery codes: keep them
```

From that login on, the bootstrap token is refused (39310). So that `sprig-vm.sh` keeps working, alice issues it a service token and saves it as `secrets/sprig/ops-token`:

```sh
sprigctl --endpoint https://127.0.0.1:7480 token create --service sprig-vm --kind ci --scope '*:write' --never-expires --label "sprig-vm.sh on $(hostname)"
#   "The token is shown once": copy the sprigsvc_... value into secrets/sprig/ops-token (mode 0644 inside secrets/)
```

**Tested (laptop, round 1, plain HTTP then):** `operator create alice` with the bootstrap token; login gave an enrolment ticket and an `otpauth://` URI; enrolment with a TOTP code returned ten recovery codes and a session. From then on, the bootstrap token got 39310 on `cluster list`. A `sprigsvc_` token issued from alice's session (`--kind ci --scope '*:write' --never-expires`) in `secrets/sprig/ops-token` made `./sprig-vm.sh ctl cluster list` work again.

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
| 7480 | controller Admin API (bearer token) | **HTTPS** (deployment CA) | 127.0.0.1 only (use SSH); several VMs: the private address (`SPRIG_ADMIN_BIND_IP`) |
| 9090 | Prometheus (profile `monitoring`) | no | 127.0.0.1 only |
| 2379, 2380 | etcd client and peer | mutual TLS | one VM: inside the Compose network; several VMs: `SPRIG_PRIVATE_IP` |
| 3306 (MariaDB) | the backend | required | one VM: inside the network; several VMs: `SPRIG_PRIVATE_IP:SPRIG_BACKEND_PORT` |
| 7481 | controller internal (permits, peer pings, the relay) | one VM: no (inside the network); several VMs: **mutual TLS** (`SPRIG_INTERNAL_TLS`, process certificates) | one VM: inside the network; several VMs: `SPRIG_PRIVATE_IP` |
| 7403 | gateway peer service (KILL, drains, relays) | mutual TLS | one VM: inside the network; several VMs: `SPRIG_PRIVATE_IP` |
| 9180–9183 | telemetry: `/readyz`, `/metrics` (gateway, controller, worker, agent) | no | one VM: inside the Compose network; several VMs: `SPRIG_PRIVATE_IP` (`SPRIG_TELEMETRY_PORT_*`), for the load balancer's health checks and Prometheus |

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

**Tested (laptop, three "VMs", two MariaDB containers on VM 1 as two clusters):** a tenant on each cluster (783 rows; 3,000 rows), then `backup`: 4.5 s for both MariaDBs and etcd, 4.3 MB, copied to the stand-in off-VM directory and checked there. The data files start with OpenSSL's `Salted__` header and nothing in them is readable; decrypting with a wrong key fails at the zstd frame check. After the backup: 300 rows deleted and the rest updated on one tenant, a table dropped and another created on the other, and a third tenant created. Then the local copy was deleted and `restore <ts> <off-VM dir>` run on VM 1 with VM 2 stopped:
- without the private key it refused before stopping anything ("restoring needs the backups' private key");
- with it: fetched and checked the copy, restored both MariaDBs and etcd in 28 s; VM 2 and VM 3 then ran `etcd-rejoin` (18 s for both), and VM 2 `up`.
- Afterwards every table of both tenants had the same `CHECKSUM TABLE` and row count as before the backup (68 tables, including the tenants' catalogs), the dropped table was back and the new one gone, the third tenant was gone from etcd and MariaDB, and both tenants logged in and read through both gateways.
- Two problems found and fixed: the first `etcd-rejoin` on VM 2 stopped silently when its `etcdctl` call reached a member that was down (it now retries each call), and VM 1's controller restarted for about three minutes after the restore, because at start it refused to run when its first etcd read reached a member that wasn't back yet. The processes now try the other members at start: with one of three members stopped, a restarted controller was healthy in 6–8 s (`harden2-c`), where the build before the fix didn't start within 300 s.

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

`upgrade` records the running image in `.state/previous-image`, takes a backup on the VM with MariaDB (`backups/pre-upgrade-<ts>/`), changes `SPRIGDB_IMAGE` in `.env` and recreates this VM's controller, then its worker, then its gateway, each after the previous one is healthy. `rollback` swaps the image back in the same order. It is safe when the new build hasn't changed a stored format. If a release note says a version migrates the catalog or etcd records, rolling back means restoring the `pre-upgrade` backup instead: `./sprig-vm.sh restore pre-upgrade-<ts>` with the old image.

**The gateway drains on stop.** Its readiness turns off at once (load balancers stop sending it new connections), it keeps serving for `gateway.drain_lb_delay_seconds` (10 s), then closes its listeners and gives open sessions up to `gateway.drain_timeout_seconds` (30 s) to finish; `stop_grace_period` is 45 s. Its write permit stays valid throughout (before hardening round 2, a stopping gateway refused writes with 39501 for 5–6 s while it still accepted connections).

**Three VMs: upgrade one VM at a time** (VM 1, then VM 2, then VM 3; roll back in the same order). While a VM's controller restarts, the other controllers keep serving permits, and while its gateway restarts the load balancer sends clients to the others, so no write is refused. A restarted controller answers permit requests only once it has read the DC lease, so it can't fence anyone while it starts.

**One VM:** the gateway's restart is a short outage of this VM's ports, and the controller's restart fences writes for a few seconds: the gateway holds writes to a valid DC lease, which only that controller renews (chapter 25's self-fencing), so writes get **39501** ("data center ... is fenced") for about 3–5 s. Clients that retry ride through it.

**With MariaDB high availability** the upgrade also replaces the VM's node agent (after the worker, before the gateway). On the primary's VM, the new agent fences its node until it has its DC permit and the writer lease again: about half a second of writes refused with 1290 ("read-only").

**Tested (laptop, three VMs, MariaDB HA, Envoy, round 3).** One client inserting every 0.05 s through the load balancer; `upgrade` on VM 1, VM 2, VM 3 in turn (each 37–42 s, including the backup of the VM's MariaDB node):

| Run | Writes | Refused | Acknowledged lost |
|---|---|---|---|
| `harden3-c` → `harden3-c2` (the same binaries under another tag) | 1,739 | 5, all 1290 within 0.4 s, when VM 1's agent (the writer's) restarted; none while any gateway or controller restarted | **0** |
| `harden3-a` → `harden3-c` | 1,375 | VM 1: 7 from the stale writer lease HD20 fixes, until VM 1's agent was replaced, and 102 duplicate keys from a test table the run before had left; VM 2 and VM 3: 0 | **0** |

**Tested (laptop, three "VMs", hardening round 2).** One client inserting a row every 0.2 s, each on a new TLS connection, alternating between the two gateways, and trying the other gateway at once when a connection is refused or dropped (what a load balancer with health checks does); upgrade VM 1, then VM 2, then roll back VM 1, then VM 2:

| Run | Images | Writes acknowledged | Refused with 39501 | Connections refused or dropped (then sent to the other gateway) | Acknowledged rows lost |
|---|---|---|---|---|---|
| upgrade and rollback | `harden2-b` → `harden2-c` → `harden2-b` (two builds, 20:48 and 21:28 trees) | **729 of 729** | **0** | 6, at most 0.5 s on one gateway per step | **0** |
| upgrade and rollback | `harden2-b` → `harden2-b2` → `harden2-b` | 690 of 690 | 0 | 7, at most 0.6 s | 0 |
| upgrade only | `harden2-a` → `harden2-b` (19:20 and 20:48 trees) | 387 of 412 | 25 (13 on VM 1, 12 on VM 2, about 5 s each: the old build's gateway drain) | 4 | 0 |

Each step took 22–33 s (VM 1 includes the 2–5 s backup). `harden2-b2` is `harden2-b`'s binaries under another tag: the containers are recreated and the processes restart exactly as in an upgrade, but no code changed in that run. The first run's 39501s come from stopping `harden2-a` gateways, which still had the drain problem fixed in `harden2-b`; the first `harden2-a` rehearsal (before the fix) also showed 2 refused writes on VM 2's gateway while VM 1's controller restarted, from the restarted controller answering "fenced" before it had read the lease (also fixed in `harden2-b`).

**Tested (laptop, one VM, hardening round 1):** build A → build B → back to A, the same client against one gateway: 33 s each way; 30 and 15 failed inserts (39501 while the only controller restarted and while the gateway drained, then 2003/2013 while it was replaced); 0 of 299 acknowledged rows lost.

## etcd members

With three members, etcd keeps working while any two run; SprigDB's processes list all three (`SPRIG_ETCD_SEEDS`) and use whichever answer.

- **A member down for a while** (its VM rebooting, say): nothing to do; it catches up when it starts again. `./sprig-vm.sh status` shows each member's state.
- **A member that lost its data, or a VM rebuilt from scratch:** on that VM, `./sprig-vm.sh etcd-rejoin`. It removes the member from the cluster, adds it again with the same name and address, empties its data directory and starts it; it copies the data from the others.
- **Two members down:** etcd has no majority and refuses writes. SprigDB's data path keeps going for a while (writes need a DC permit, which the controllers keep renewing only while etcd answers: about `failover.dc_lease_seconds`), then writes get 39501. Bring a member back; if two are lost for good, restore etcd from a backup ([Restore](#restore)).

**Tested (laptop, three "VMs"; a client writing every 0.2 s through both gateways, each insert on a new TLS connection):**

| Step | Took | Writes | Failed |
|---|---|---|---|
| etcd-vm2 (a follower) stopped for 60 s, then started | healthy again 5.4 s after start | 300 | 0 |
| etcd-vm1 (the leader, on the VM with MariaDB and the leading controller) stopped for 45 s, then started | a new leader at once | 228 | 0 |
| `etcd-rejoin` on VM 3 (member removed, re-added, started empty) | 19.6 s | 56 | 0 |

783 acknowledged inserts in all, 783 in the table. Every member reported the same data size afterwards, VM 3's under a new member id. A client without a certificate gets no answer from any member (mutual TLS).

## Rotating certificates

```sh
./sprig-vm.sh rotate-certs     # needs secrets/ca/ca-key.pem back on the VM
```

It makes new gateway, etcd, MariaDB, controller and process (`client.pem`) certificates from the same CA, **with no restart**: each file is replaced by rename (key first, then certificate), so a reader never sees half a file; the gateway re-reads its client bundle within two polls (log event `gateway.cert_rotated`); the controller re-reads its Admin API and internal-port certificates within a few seconds of the files changing (hardening round 3; a pair that doesn't load keeps the old one served, with a warning); MariaDB gets `FLUSH SSL`; etcd uses its new certificate on new connections. The process certificate is also every SprigDB process's client identity (to etcd and the controllers' internal port): each process presents the new one from its next start, and the old one stays valid until it expires.

**Several VMs:** run `rotate-certs` on the VM that has the CA key, copy `secrets/tls/` to the other VMs (`rsync -a secrets/tls/ vm2:/opt/sprigdb/secrets/tls/`), then `./sprig-vm.sh reload-certs` on each (MariaDB's `FLUSH SSL`; everything else picks the files up by itself).

**Tested (laptop, round 3, three VMs, a client writing through the load balancer every 0.05 s):** `rotate-certs` on VM 1, the copy and `reload-certs` on VM 2 and VM 3 took 1.1 s. 8 s later all 15 TLS ports served new certificates (on each VM: the gateway's client port, the controller's Admin API and internal port, etcd and MariaDB); no container restarted; 238 writes, none failed, none lost. **Round 1 (one VM):** a MySQL session in the middle of a 15 s statement finished normally.

To use certificates from your own CA for clients, write them to `secrets/tls/gateway.pem` and `gateway-key.pem` (key first, then the certificate, each by `mv`). The generated certificates last `SPRIG_CERT_DAYS` (397) days: put a reminder or a timer on it. `sprig_gateway_cert_expiry_seconds` (and the `SprigCertExpiringSoon` alert) warns 14 days ahead.

Rotating the CA itself: make a new CA, put both CA certificates into `ca.pem` (old and new), restart everything once (`docker compose up -d --force-recreate`), then issue new certificates from the new CA and remove the old CA from `ca.pem`.

## Monitoring

- `docker compose --profile monitoring up -d prometheus` starts Prometheus on 127.0.0.1:9090. It loads the alert rules from `deploy/alerts/` (`sprig-ops`, `sprig-slo`, `sprig-approvals`). **One VM:** it scrapes the gateway, controller and worker (`/metrics` on 9180 inside the network). **Several VMs:** run it on one VM with `SPRIG_PROMETHEUS_CONFIG=prometheus.ha.yml` and `SPRIG_VM_HOSTS` (every VM's private address) in `.env`, after `./sprig-vm.sh monitoring-config`, which writes `config/prometheus-targets.json`: every VM's gateway, controller, worker and agent at the telemetry ports `compose.ha.yaml` and `compose.db-ha.yaml` publish on the private address (`SPRIG_TELEMETRY_PORT_*`, 9180 to 9183), labelled `role` and `vm`. Prometheus re-reads the file when it changes. To use your own Prometheus instead, scrape those ports.
- **Tested (laptop, round 3, three VMs):** 12 of 12 targets up (gateway, controller, worker and agent on each VM), the 35 alert rules loaded, none fired. **Round 1, one VM:** 3 of 3.
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
| Backups fail with "No space left" | `backups/` on the root disk | mount a data disk at `backups/`; lower `SPRIG_BACKUP_KEEP` |
| `up` stops with "secrets/ failed its checks" | an owner or mode in `secrets/` is wrong | run each `chmod`/`chown` it prints ([Secrets checks](#secrets-checks)) |
| A process exits with 39998 naming a token or key file | the file is a symbolic link to nothing, not a regular file, or writable by everyone | `chmod 644` it (0600 for the offline keys) |
| `sprig-vm.sh ctl` says `unexpected argument '--ca-file'` | the image's `sprigctl` predates HTTPS on the Admin API | `SPRIGCTL_BIN=<a newer sprigctl> ./sprig-vm.sh ...`, or upgrade the image |
| etcd logs `rejected connection on peer endpoint ... does not match` | a member connects from an address its certificate doesn't name (NAT) | add the address to `SPRIG_INTERNAL_SANS` and re-issue, or `SPRIG_ETCD_PEER_SKIP_SAN=true` |
| `up-etcd` member stays `starting` | fewer than two members run | start the other VMs' members (`up-etcd` on each) |
| Gateways refuse writes with 39030 "the writer lease (epoch 0) is older than the last failover" | an agent from a build before hardening round 3 restarted on the primary after a failover (HD20) | upgrade the agents; restarting the primary's agent with a round-3 build fixes the lease |
| A replica's SQL thread stops with "out-of-order sequence number ... gtid strict mode" | a statement written to that replica's own binlog (round 3 found `FLUSH SSL`; `rotate-certs` now uses `FLUSH NO_WRITE_TO_BINLOG SSL`) | on that replica: `STOP SLAVE; RESET MASTER; START SLAVE` (its binlog holds nothing the cluster needs: it replicates everything) |
| `configure` on VM 1 says "0 of 2 replicas attached" | VM 2 and VM 3 aren't up yet, or their MariaDB can't reach VM 1's (TLS name, firewall) | `up` on VM 2 and VM 3, `db-status` there, then `configure` on VM 1 again |
| The balancer shows a gateway unhealthy | its `/readyz` isn't reachable on the telemetry port (not published, or firewalled), or it is draining | `curl http://<vm>:9180/readyz`; publish `SPRIG_TELEMETRY_PORT_GATEWAY` (`compose.ha.yaml`) |
| `restore` says it needs the private key | `backup-key.pem` is off the VM, as it should be | put it back at `secrets/backup/backup-key.pem` (0600) or set `SPRIG_BACKUP_KEY` for that run |

## Galera: a node that can't rejoin

Seen on the dev stack on 11 November 2026 (decision HD1). A node takes a full copy (SST) and then stops with `Failed to parse uuid:seqno pair`, `IST received: 00000000-…:-1` and `Sending JOIN failed: -103`. Check `SHOW STATUS LIKE 'wsrep_cluster_state_uuid'` on a running node: if it is all zeros, the cluster has lost its identity (seen after every node crashed at once), and no node can join it. Fix (needs a short outage of the cluster):

1. Stop the other nodes cleanly, one at a time (`docker stop -t 300`), the most up-to-date node (highest `wsrep_last_committed`) last.
2. In that node's datadir, keep a copy of `grastate.dat` and write a new one with a fresh UUID (`uuidgen`), `seqno:` = its last committed position and `safe_to_bootstrap: 1`.
3. Start it (it bootstraps a new primary component with that UUID), then start the others one by one: each takes an SST or IST and joins.
4. Check: every node `Synced`, `wsrep_cluster_size` = the node count, the UUID not zero, and the schema and table counts as before.

A node that joins under heavy load can also fail because the donor loses contact with the cluster mid-copy (`BACKUP STAGE START: WSREP has not yet prepared node`). Chapter 27's Galera options with this bundle's timeouts (`evs.suspect_timeout=PT30S`, `evs.inactive_timeout=PT60S`) and a gcache large enough for IST avoid it.

## Not done yet

- **Run on a real VM** (Kiran decides when; nothing has touched the dev or stage VMs). Everything here ran on one laptop, three Compose projects on separate networks standing in for three VMs. [GO-LIVE.md](GO-LIVE.md) lists what to prepare.
- **The controllers' internal port is JSON over HTTP/1.1 with mutual TLS**, not chapter 23's gRPC port 7400.
- **Restore with MariaDB high availability** wasn't rehearsed: `restore` puts each VM's own node back; with three nodes, restore the primary's VM and rebuild the replicas from it (empty datadir, `configure`).
- **A replica rebuilt after the primary's binlogs expired** (7 days) needs seeding from a backup by hand (see [MariaDB high availability](#mariadb-high-availability)); the agent's `node_rebuild` service isn't wired into the bundle.
- **The load balancer is one container**: run two behind keepalived, or use the cloud's.
- **`ctl cluster set-nodes` on a round-1 bundle** (step 5 of "From one VM to three") was tested on a laptop deployment, not on the bundle itself.
- **Two gateways on one VM** would need different ports (the listener ports are DC-wide settings). Use one gateway per VM.
- **Backups** are a few seconds apart between MariaDB and etcd (back up when no mover job runs), and there is no point-in-time restore yet (chapter 47's binlog archiving).
