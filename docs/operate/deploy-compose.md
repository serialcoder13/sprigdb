---
title: Deploy with Docker Compose
audience: [operator]
sidebar:
  order: 1
---

# Deploy with Docker Compose

SprigDB ships two Compose setups. The **development stack** in `deploy/compose/` runs etcd and MariaDB on your machine, with the SprigDB processes on the host; use it to try SprigDB, develop against it and run the tests. The **VM bundle** in `deploy/compose-vm/` runs a complete, TLS-everywhere data centre in containers on one VM or three; use it when you want Compose in production.

| | Development stack | VM bundle |
|---|---|---|
| Directory | `deploy/compose/` | `deploy/compose-vm/` |
| Runs in containers | etcd, MariaDB (one topology per profile) | etcd, MariaDB, controller, worker, gateway, agent, Prometheus, Envoy |
| SprigDB processes | on the host, started by `gateway-dev.sh` | in the `sprigdb` image |
| TLS | optional; self-signed development certificates | required on every client port, etcd and MariaDB |
| Passwords | fixed development values | generated per deployment, read from files |
| Ports | loopback only, 13xxx | the product defaults (3306, 3307, 8443, ...) |
| Fit for | laptops, CI, tests | one data centre on one or three VMs |

## Development stack

### Start MariaDB and etcd

`deploy/compose/compose.yaml` runs one simulated data centre: a single-member etcd that serves as both the DC tier and the global tier, and MariaDB in three topologies. Pick the topologies with a profile:

```sh
cd deploy/compose
docker compose --profile single up -d --wait        # etcd + one node
docker compose --profile replication up -d --wait   # etcd + primary + 2 replicas, semi-sync AFTER_SYNC
docker compose --profile galera up -d --wait        # etcd + 3-node Galera
docker compose --profile all up -d --wait           # everything above
docker compose --profile all down -v                # stop and delete the data
```

| Profile | Services | Host ports (127.0.0.1) |
|---|---|---|
| (always) | `etcd` | 2379 |
| `single` | `single` | 13301 |
| `replication` | `primary`, `replica-1`, `replica-2` | 13311, 13312, 13313 |
| `galera` | `galera-1`, `galera-2`, `galera-3` | 13321, 13322, 13323 |
| `all` | all of the above | |
| `faults` | `toxiproxy` (API on 18474, proxies on 13400–13419) and `faultd` | test tooling |
| `elle` | `elle`, a history checker (build only) | test tooling |
| `connectors` | one client image per driver (Java, Go, Node.js, Python, PHP, the MySQL and MariaDB command-line clients, Hibernate, Prisma, sysbench) | test tooling |

Every port is bound to 127.0.0.1 and kept away from 3306 and 3307, which a gateway would use.

Each node starts with the binlog and durability settings SprigDB requires (`deploy/compose/mariadb/binlog.cnf`: row-based binlog with full row images and metadata, GTID strict mode, `sync_binlog = 1`, seven days of binlog). The replication profile adds semi-synchronous replication that waits `AFTER_SYNC`, so a failover never loses an acknowledged commit. The development nodes leave `read_only` and `require_secure_transport` off, which production nodes don't.

You can change the stack with environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `MARIADB_IMAGE` | `mariadb:11.8` | MariaDB image; `mariadb:12.3` is the other supported version |
| `ETCD_IMAGE` | `quay.io/coreos/etcd:v3.6.5` | etcd image |
| `MARIADB_MEM_LIMIT` | `1536m` | Memory cap of each MariaDB container |
| `MARIADB_SINGLE_MEM_LIMIT` | `2560m` | Memory cap of the `single` node, which holds the many-tenant tests |
| `COMPOSE_PROJECT_NAME` | `sprigdb-dev` | Compose project name (set in `.env`) |

The root password is in `deploy/compose/.env` (`sprig-dev-root`). It is for local development only; never reuse it.

etcd keeps one hour of revisions (`--auto-compaction-mode=periodic`, `--auto-compaction-retention=1h`); without that its database grows with every write.

### Run a controller, a worker and a gateway

`deploy/compose/gateway-dev.sh` builds `sprigdb` and `sprigctl` and runs a controller, a worker and a gateway on the host against the stack. It needs the `all` profile running, plus `curl`, `jq` and `openssl`.

```sh
deploy/compose/gateway-dev.sh up            # build, start, register clusters, configure listeners
deploy/compose/gateway-dev.sh status
deploy/compose/gateway-dev.sh restart       # restart the processes, keeping the tenants
deploy/compose/gateway-dev.sh tls required  # or optional, or off; applies live
deploy/compose/gateway-dev.sh down          # stop, and undo what up wrote to etcd
```

`up` does the following, in order:

1. Starts a controller with `deploy/compose/sprigdb.dev.toml`. On an empty etcd the controller bootstraps the deployment (`deployment_id = "sprigdb-dev"`, data centre `dc-a`).
2. Waits for a DC leader, then registers the three Compose clusters: `dc-a-single` as `dc-a-c1`, `dc-a-repl` as `dc-a-c2` and `dc-a-galera` as `dc-a-c3`.
3. Starts a worker (node id `wkr-1`), which runs moves and other background jobs.
4. Makes a development CA and server certificate in `deploy/compose/gateway-certs/` (once, with `openssl`).
5. Writes listener and test settings at DC scope (below).
6. Starts the gateway with `deploy/compose/sprigdb.gateway.dev.toml` and waits for `/readyz`.
7. In the default login mode, creates the tenants `acme` and `globex`, each with a user `app`.

| Endpoint | Address |
|---|---|
| MySQL persona | 127.0.0.1:13306 |
| MariaDB persona | 127.0.0.1:13307 |
| Admin API (controller) | http://127.0.0.1:7480 |
| Controller telemetry | 127.0.0.1:9180 |
| Worker telemetry | 127.0.0.1:9185 |
| Gateway telemetry | 127.0.0.1:9190 |

Log in as `acme.app` or `globex.app` with the development-only password `sprig-dev-app`:

```sh
mysql -h 127.0.0.1 -P 13306 -u acme.app -psprig-dev-app \
      --ssl-mode=VERIFY_IDENTITY --ssl-ca=deploy/compose/gateway-certs/ca.pem
```

The settings `up` writes, and `down` removes:

| Setting | Value | Why |
|---|---|---|
| `listener.mysql.bind`, `listener.mariadb.bind` | `127.0.0.1:13306`, `127.0.0.1:13307` | Development ports |
| `listener.mysql.tls`, `listener.mariadb.tls` | `optional` (`SPRIG_GATEWAY_TLS`) | Plaintext clients such as sysbench must log in too |
| `tls.bundles.default.cert_path`, `key_path` | the development certificate | |
| `listener.http.enabled`, `listener.https.enabled` | `false` | Port 8080 is often taken on development machines |
| `workload.flush_seconds` | `10` | Workload statistics reach the catalog within seconds |
| `failover.self_fence` | `false` | `restart` stops the controller; with self-fencing on, the gateway would refuse writes with 39501 while it is away. `SPRIG_GATEWAY_SELF_FENCE=keep` leaves the default (`true`) |

Variables `up` reads:

| Variable | Default | Meaning |
|---|---|---|
| `SPRIG_GATEWAY_LOGIN_MODE` | `catalog` | `catalog`: tenants with their own databases (`acme.app`). `accounts_file`: the fixed logins `app`, `app_repl` and `app_galera` of `gateway-accounts.dev.toml`, straight onto the three clusters |
| `SPRIG_GATEWAY_TLS` | `optional` | TLS mode of both listeners: `required`, `optional` or `off` |
| `SPRIG_GATEWAY_PROFILE` | `debug` | `release` builds and runs release binaries, for latency and benchmark numbers |
| `CARGO_TARGET_DIR` | `target` | Where the binaries are built |
| `SPRIG_GATEWAY_SKIP_CLUSTERS` | none | Comma-separated cluster names not to register, or `down` to skip every cluster with a node that refuses connections |
| `SPRIG_TENANT_ID_BASE` | none on the shared etcd | Sets `tenant.id_min` and `tenant.id_max` (base + 99,999), so instances sharing MariaDB servers never hand out the same tenant id; `none` sets no range |
| `SPRIG_GATEWAY_DELETE_WAIT` | `300` | Seconds `down` waits for each tenant's deletion |

To run a second instance beside the first (for example against your own etcd), set `SPRIG_GATEWAY_DEV_STATE`, `SPRIG_ETCD`, `SPRIG_GATEWAY_MYSQL_PORT`, `SPRIG_GATEWAY_MARIADB_PORT`, `SPRIG_ADMIN_PORT`, `SPRIG_CTL_TELEMETRY_PORT`, `SPRIG_GW_TELEMETRY_PORT` and `SPRIG_WK_TELEMETRY_PORT` for `up`. `up` records them in the state directory, so `status` and `down` need only `SPRIG_GATEWAY_DEV_STATE`.

Processes, logs (`controller.log`, `worker.log`, `gateway.log`) and what `up` created are in `deploy/compose/.gateway-dev/`. Re-running `up` leaves running processes alone, so after a code change run `down`, then `up`.

### Turn on Studio and the other listeners

`gateway-dev.sh` leaves the HTTP listener off. `deploy/compose/studio/e2e.sh setup` turns it on at 127.0.0.1:18080 (TLS off; browsers treat loopback as a secure context), turns on AMQP at 127.0.0.1:18682, and adds sample data and named operators for Studio's end-to-end tests. Then open <http://127.0.0.1:18080/studio> and sign in as `acme.app`. See [Studio](../tools/studio.md).

To turn on other listeners yourself, set their settings at DC scope, binding before enabling:

```sh
sprigctl settings set listener.mongo.bind 127.0.0.1:17017 --scope dc:dc-a
sprigctl settings set listener.mongo.enabled true --scope dc:dc-a
```

### Three data centres on one machine

`deploy/compose/compose.3dc.yaml` simulates three data centres. Each has its own etcd (its DC tier) and one MariaDB cluster in a different topology, and one global etcd member per DC forms the global tier. A `netem` service delays traffic between the DCs' subnets. Always run it through `3dc.sh`, which uses its own project name (`sprigdb-3dc`) so it never touches the single-DC project:

```sh
cd deploy/compose
./3dc.sh up -d --build --wait     # the first run builds the netem image
./3dc.sh latency                  # ping matrix between the DCs
./3dc.sh ps                       # any other arguments go to docker compose
./3dc.sh down -v                  # stop and delete the data
```

| DC | Subnet | Cluster | Host ports: DC etcd, global etcd, MariaDB |
|---|---|---|---|
| `mum1` | 10.10.1.0/24 | `mum1-c1`: primary + 2 replicas, semi-sync `AFTER_SYNC` | 34179, 34180, 34106–34108 |
| `fra1` | 10.10.2.0/24 | `fra1-c1`: 3-node Galera | 34279, 34280, 34206–34208 |
| `iad1` | 10.10.3.0/24 | `iad1-c1`: single node | 34379, 34380, 34306 |

One-way delays are 30 ms between mum1 and fra1, 80 ms between mum1 and iad1 and 60 ms between fra1 and iad1, so a ping shows 60, 160 and 120 ms. Change them with `SPRIG3DC_DELAY_MUM1_FRA1_MS`, `SPRIG3DC_DELAY_MUM1_IAD1_MS` and `SPRIG3DC_DELAY_FRA1_IAD1_MS`, and add jitter with `SPRIG3DC_JITTER_MS`. Traffic from the host through published ports isn't delayed. The bridge name and subnet are fixed, so only one copy runs per Docker host; it can run beside the single-DC stack.

The layout holds etcd and MariaDB only. Run controllers and gateways on the host, one set per DC, each with its DC's etcd in `[meta] dc_seeds` and the three global members in `[meta] global_seeds` (see [Configuration](configuration.md)). `deploy/compose/env.3dc.toml` describes the layout for the integration tests.

For building from source, running single roles and the tests, see [Developing SprigDB](development.md).

## The VM bundle

`deploy/compose-vm/` runs one data centre in containers, with TLS on every client port, an HTTPS Admin API, mutual TLS to etcd, encrypted backups, and every password, token and key read from files that each process checks at start. It runs on one VM, or on three for high availability.

### Requirements

- Ubuntu 24.04 or Debian 12, Docker Engine 27 or later with the Compose plugin, `openssl`, `jq` and (for three VMs) `rsync`.
- NTP on every VM. Leases tolerate clock drift up to `failover.clock_drift_margin_ms` (500 ms), not minutes.
- The `sprigdb` image. Build it on a machine with at least 6 GB of memory for the compiler:

```sh
deploy/compose-vm/build-image.sh sprigdb:1.0.0
docker save sprigdb:1.0.0 | gzip > sprigdb-1.0.0.tar.gz     # copy to the VM, then:
docker load < sprigdb-1.0.0.tar.gz
```

`build-image.sh` compiles in a capped container (`SPRIG_BUILD_MEM`, default `6g`; `SPRIG_BUILD_JOBS`, default 2) and copies the two binaries into a `debian:bookworm-slim` image. On a builder with enough memory, `deploy/compose-vm/Dockerfile` does both steps in one `docker build`. A registry works too: push the image and set `SPRIGDB_IMAGE` to its name.

### Services

| Service | What it runs | Healthcheck |
|---|---|---|
| `etcd` | the DC tier (and, in single-etcd mode, the global tier), mutual TLS | `etcdctl endpoint health` |
| `mariadb-1` | MariaDB 11.8, TLS required, accounts `sprig_ctl` and `sprig_repl` created on first start | connects with root's password file |
| `controller` | `sprigdb controller`, Admin API on HTTPS 7480 | `/readyz` |
| `worker` | `sprigdb worker` | `/readyz` |
| `gateway` | `sprigdb gateway`, every client listener | `/readyz` |
| `prometheus` | Prometheus on 127.0.0.1:9090 (profile `monitoring`) | |

Extra Compose files add the other layouts:

| File | Adds |
|---|---|
| `compose.ha.yaml` | The first of three VMs: publishes etcd, MariaDB, the controller's internal port and the gateway's peer port on the private network |
| `compose.gateway-vm.yaml` | The second and third VMs: an etcd member, a controller and a gateway (a worker under the profile `worker`), no MariaDB |
| `compose.db-ha.yaml` | MariaDB high availability: one node and one agent per VM, a primary and two semi-sync replicas as one cluster |
| `compose.mariadb-2.example.yaml` | A second MariaDB container on the same VM, as its own cluster |
| `compose.lb.yaml` | Envoy as a TCP load balancer in front of the gateways |

### Install on one VM

1. Copy `deploy/compose-vm/` to the VM, for example to `/opt/sprigdb`, and load the image.
2. `cp .env.example .env`, then set at least `SPRIGDB_IMAGE`, `SPRIG_DC`, `SPRIG_PUBLIC_HOST` (the name clients use; it goes into the gateway's certificate and into the address Kafka advertises), `SPRIG_TLS_SANS` for any other names, and the memory limits.
3. `./sprig-vm.sh init` makes `secrets/`: the deployment's CA, every certificate, MariaDB's passwords, the bootstrap Admin token, the gateways' token, the workload key, cluster credentials, the backend-account secret and the backups' key pair. Move `secrets/ca/ca-key.pem` and `secrets/backup/backup-key.pem` off the VM; they come back only to rotate certificates or to restore. Back up the rest of `secrets/` somewhere other than the data backups: a restored backup can't be opened without it.
4. `./sprig-vm.sh up` checks `secrets/` (owner, modes, no symbolic links), starts the containers, waits for every healthcheck, registers the MariaDB node as cluster `<SPRIG_DC>-main` and writes the listener settings: TLS required on every listener, plain HTTP and plain AMQP off.
5. `./sprig-vm.sh status`, then create the first operator and a tenant (below), and run the smoke test from a machine that reaches the ports: `SMOKE_PASSWORD=<password> smoke/smoke.sh acme.app <public host>`. It logs in over verified TLS on MySQL, MariaDB, MongoDB, REST, gRPC, Redis, Bolt, Kafka and AMQP, does one operation on each, and checks that a plaintext MySQL login is refused.

A process that finds a token, key or backends file that isn't a regular file, or is writable by everyone, stops with 39998 naming the file.

### First operator and tenants

The bootstrap token in `secrets/sprig/admin-token` works only until the first named operator logs in. Create one, log in through an SSH tunnel to the Admin API, then give `sprig-vm.sh` a service token of its own:

```sh
docker compose exec controller sprigctl --endpoint https://127.0.0.1:7480 --ca-file /run/sprig/tls/ca.pem \
    --token-file /run/sprig/sprig/admin-token operator create alice --role platform --display-name "Alice"
#   asks for alice's password (12 characters or more)

ssh -L 7480:127.0.0.1:7480 vm1                 # on alice's machine, in another terminal
export SPRIGCTL_CA_FILE=~/sprigdb-ca.pem       # a copy of secrets/tls/ca.pem
sprigctl --endpoint https://127.0.0.1:7480 login --name alice
#   first login: enrol a TOTP authenticator; keep the ten recovery codes

sprigctl --endpoint https://127.0.0.1:7480 token create --service sprig-vm --kind ci \
    --scope '*:write' --never-expires --label "sprig-vm.sh on vm1"
#   save the sprigsvc_... value as secrets/sprig/ops-token
```

After the first operator logs in, the bootstrap token is refused with 39310. If every operator is locked out, set `admin_token_break_glass = true` under `[controller]` in `config/controller.toml`, restart the controller, repair the accounts with the bootstrap token, and remove the setting; every call made that way is audited as `breakglass.admin_token`.

Create a tenant with its first user (the password comes from standard input):

```sh
echo 's3cret-Passw0rd' | ./sprig-vm.sh tenant acme app       # logs in as acme.app
```

### Ports

Every port number is a variable in `.env`. The gateway listens inside its container on the number it is published on, so Kafka advertises the address clients really use.

| Default | What | TLS | Published on |
|---|---|---|---|
| 3306 | MySQL 8.4 persona | required | `SPRIG_BIND_IP` (all interfaces) |
| 3307 | MariaDB persona | required | `SPRIG_BIND_IP` |
| 8443 | REST, gRPC and Studio | required | `SPRIG_BIND_IP` |
| 27017 | MongoDB | required | `SPRIG_BIND_IP` |
| 6379 | Redis | required | `SPRIG_BIND_IP` |
| 7687 | Bolt | required | `SPRIG_BIND_IP` |
| 9092 | Kafka | required | `SPRIG_BIND_IP` |
| 5671 | AMQP 0-9-1 and 1.0 | required | `SPRIG_BIND_IP` |
| 7480 | Admin API | HTTPS | 127.0.0.1 (reach it over SSH); with three VMs, the private address |
| 9090 | Prometheus | no | 127.0.0.1 |
| 2379, 2380 | etcd client and peer | mutual TLS | inside the Compose network; with three VMs, the private address |
| 7481 | controller internal port (permits, peer pings) | with three VMs, mutual TLS | inside the network; with three VMs, the private address |
| 7403 | gateway peer service (`KILL`, drains) | mutual TLS | inside the network; with three VMs, the private address |
| 9180–9183 | telemetry of the gateway, controller, worker and agent | no | inside the network; with three VMs, the private address |

### Three VMs

One VM is a single point of failure, and restarting its controller fences writes for a few seconds (39501), because only that controller renews the DC lease. Three VMs on one private network remove both. Two layouts are supported:

| Layout | Compose files on each VM | Runs |
|---|---|---|
| MariaDB high availability (recommended) | `compose.yaml:compose.ha.yaml:compose.db-ha.yaml` on all three | On every VM: an etcd member, a MariaDB node and its agent, a controller, a worker and a gateway |
| MariaDB on VM 1 only | VM 1: `compose.yaml:compose.ha.yaml`; VMs 2 and 3: `compose.gateway-vm.yaml` | etcd members and controllers on all three; MariaDB and the worker on VM 1 |

What changes with three VMs:

- etcd has three members with mutual TLS, and keeps working with any two.
- A gateway renews its write permit from any controller in its DC, so restarting one controller fences nothing. The DC-leader loops move to another controller within the leader lease.
- Calls that only the DC leader serves (creating a tenant, for example) get 39504 from the others; `./sprig-vm.sh ctl` resends them to the leader.
- Gateways sit behind a load balancer that checks each gateway's `/readyz`.

Settings that are the same on every VM: `SPRIG_DC`, `SPRIG_DEPLOYMENT_ID`, the client ports, `SPRIG_ETCD_INITIAL_CLUSTER`, `SPRIG_ETCD_SEEDS` and `SPRIG_INTERNAL_SANS` (every VM's private address). Per VM: `COMPOSE_FILE`, `SPRIG_VM_ID` (1, 2, 3), `SPRIG_CONTROLLER_BOOTSTRAP` (`true` on VM 1 only), `SPRIG_ETCD_NAME`, `SPRIG_PRIVATE_HOST`, `SPRIG_PRIVATE_IP`, `SPRIG_ADMIN_BIND_IP` and `SPRIG_KAFKA_HOST`. With MariaDB high availability also `SPRIG_DB_NODES=db1=<VM 1>:3306,db2=<VM 2>:3306,db3=<VM 3>:3306` and `SPRIG_DB_PRIMARY=db1`.

Install:

1. On VM 1: `./sprig-vm.sh init` with `SPRIG_INTERNAL_SANS` set.
2. Copy `secrets/` to VM 2 and VM 3 without the two offline keys: `rsync -a --exclude ca/ --exclude backup/backup-key.pem secrets/ vm2:/opt/sprigdb/secrets/`, then `chmod 700 secrets` there.
3. On every VM, within a few minutes of each other: `./sprig-vm.sh up-etcd`.
4. `./sprig-vm.sh up` on VM 1, then VM 2, then VM 3.
5. With MariaDB high availability: `./sprig-vm.sh configure` on VM 1 once both replicas replicate; it registers the cluster with the writer first. Then `configure` on VM 2 and VM 3, which records the cluster id.
6. The load balancer (below), then `./sprig-vm.sh ctl dc show <dc>` to see the DC leader.

Open between the VMs only: etcd 2379 and 2380, MariaDB 3306, the controller's 7480 and 7481, the gateway's 7403 and telemetry 9180–9183. If etcd refuses its peers with `does not match any of DNSNames`, a member connects from an address its certificate doesn't name (NAT); add that address to `SPRIG_INTERNAL_SANS` and re-issue the certificates, or set `SPRIG_ETCD_PEER_SKIP_SAN=true`.

How the agents fence MariaDB nodes and how failover works in this layout is in [Deploy on VMs](deploy-vms.md) and [Failover](failover.md).

### Load balancer

`compose.lb.yaml` runs Envoy as a TCP load balancer: one listener per client port, each sending a new connection to the ready gateway with the fewest open connections. TLS passes through untouched, so the gateways' certificate must name the balancer's address (`SPRIG_PUBLIC_HOST`, `SPRIG_TLS_SANS`).

```sh
# on the balancer's VM: this directory and an .env with the client ports
SPRIG_LB_GATEWAYS=10.0.0.11,10.0.0.12,10.0.0.13 ./sprig-vm.sh lb-up
curl -s http://127.0.0.1:9901/clusters | grep health_flags     # each gateway's state
```

Envoy checks `GET /readyz` on each gateway's telemetry port every second: two failures take a gateway out, two successes bring it back. A cloud TCP load balancer does the same job with this health check: HTTP `GET /readyz` on port 9180, expecting 200, every 1 to 2 seconds, two failures to mark a gateway down.

Kafka clients use the balancer only for their first connection. Kafka's metadata then names every gateway by its own `SPRIG_KAFKA_HOST` (`[gateway] kafka_advertise_host`), so each VM needs a public name clients resolve, and its Kafka port open to them.

### Day-two commands

| Command | What it does |
|---|---|
| `status`, `db-status` | Containers, etcd members, MariaDB replication state |
| `ctl <args>` | Runs `sprigctl` against the local controller, forwarding leader-only calls |
| `tenant <tenant> <user>` | Creates a tenant and its first user |
| `backup`, `verify-backup <ts> [dir]`, `restore <ts> [dir]` | Encrypted backups of every MariaDB container and etcd; see [Backups and moves](backups-and-moves.md) |
| `upgrade <image>`, `rollback` | Takes a backup, then replaces the controller, worker, agent and gateway in turn; see [Upgrades](upgrades.md) |
| `rotate-certs`, `reload-certs` | New certificates from the same CA, with no restart |
| `etcd-rejoin` | Re-adds a member that lost its data |
| `etcd-defrag` | Defragments etcd; run it daily from cron or a systemd timer |
| `monitoring-config` | Writes Prometheus targets for every VM; see [Monitoring](monitoring.md) |
| `lb-config`, `lb-up` | Writes the Envoy configuration and starts the balancer |
| `stop`, `down` | Stops, or removes, this VM's containers |

### Sizing the bundle

The memory limits in `.env` default to a small VM. For three VMs of 4 vCPU and 16 GB each, set MariaDB to 8 GB with a 5 GB buffer pool, the gateway to 3 GB, the controller and worker to 1 GB each, etcd to 512 MB and the agent to 256 MB.

| Variable | Default | Meaning |
|---|---|---|
| `MARIADB_MEM_LIMIT` | `4g` | MariaDB container limit |
| `MARIADB_BUFFER_POOL` | `2G` | `innodb_buffer_pool_size` |
| `MARIADB_REDO_SIZE` | `1G` | `innodb_log_file_size` |
| `MARIADB_MAX_CONNECTIONS` | `1000` | `max_connections` |
| `GATEWAY_MEM_LIMIT` | `2g` | Gateway container limit |
| `CONTROLLER_MEM_LIMIT`, `WORKER_MEM_LIMIT` | `1g` | |
| `ETCD_MEM_LIMIT` | `512m` | |
| `AGENT_MEM_LIMIT` | `256m` | With `compose.db-ha.yaml` |
| `SPRIG_CLUSTER_MEMORY_GB`, `SPRIG_CLUSTER_STORAGE_GB` | `4`, `100` | The capacity the cluster is registered with, which placement uses |

A MariaDB container that restarts with exit code 137 ran out of memory: raise `MARIADB_MEM_LIMIT`, or lower `MARIADB_BUFFER_POOL` and `MARIADB_MAX_CONNECTIONS`. See [Sizing](sizing.md).

### Logs

Every container logs to Docker's `json-file` driver, rotated at `SPRIG_LOG_MAX_SIZE` (50 MB) times `SPRIG_LOG_MAX_FILES` (5). SprigDB's processes log JSON lines: `docker compose logs -f gateway | jq .`. Ship the container logs elsewhere if you need them longer than the rotation keeps them.

## Development or production

Use the development stack only for development and tests:

- its passwords are fixed and public;
- its MariaDB nodes accept plaintext connections and don't boot `read_only`;
- `gateway-dev.sh` turns self-fencing off;
- etcd is one plaintext member;
- the SprigDB processes run from your build directory, with no supervisor.

For production on Compose, use the VM bundle on three VMs with MariaDB high availability and a load balancer, follow the checklist in `deploy/compose-vm/GO-LIVE.md` (VMs, network, DNS names, certificates, secrets, backups, monitoring), and rehearse a failover before real data arrives: stop the primary (`docker compose stop mariadb-1`) and watch the writer move. Then start it again; it rejoins as a replica.

Not supported in the VM bundle:

- Two gateways on one VM: listener ports are DC-wide settings, so run one gateway per VM.
- Point-in-time restore: backups are full copies, taken a few seconds apart for MariaDB and etcd. Take them when no move, split or merge is running.
- Restore of a three-node MariaDB cluster in one step: restore the primary's VM, then rebuild the replicas from it.
- An Alertmanager: Prometheus loads the alert rules but sends nothing until you point its `alerting:` section at yours.

## Related

- [Deploy on VMs](deploy-vms.md)
- [Deploy on Kubernetes](deploy-kubernetes.md)
- [Configuration](configuration.md)
- [Developing SprigDB](development.md)
- [Monitoring](monitoring.md)
