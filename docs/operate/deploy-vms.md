---
title: Deploy on VMs
audience: [operator]
sidebar:
  order: 3
---

# Deploy on VMs

On virtual or physical machines, SprigDB runs as ordinary processes: the `sprigdb` binary under systemd, one unit per role, next to etcd and MariaDB servers you install. A SprigDB **agent** beside each MariaDB node keeps the node read-only unless it is the cluster's writer, so a node cut off from SprigDB can't accept writes. This page covers the native layout. For SprigDB in containers on VMs, see [Deploy with Docker Compose](deploy-compose.md#the-vm-bundle), which is the most complete VM deployment SprigDB ships.

## What's provided

| Piece | Where | What it is |
|---|---|---|
| systemd units | `deploy/vm/systemd/` | One unit per role, both etcd tiers, and a drop-in for `mariadb.service` |
| Package definition | `deploy/packaging/nfpm.yaml` | `.deb` and `.rpm` for amd64 and arm64, built with nfpm |
| Ansible role | `deploy/ansible/` | Installs the package, etcd, certificates and `sprigdb.toml`, and starts each host's units |
| Example process file | `deploy/packaging/sprigdb.example.toml` | Installed as `/etc/sprigdb/sprigdb.toml` |
| MariaDB settings | `deploy/compose-vm/config/mariadb/10-sprig.cnf` | The server settings a SprigDB node needs |

The packages and the Ansible role are starting points for your own automation: adapt them, and rehearse on test machines before production.

## Plan the hosts

A data centre needs:

| Hosts | Run | Notes |
|---|---|---|
| 3 (or 5) | etcd, DC tier | A majority must be up for writes to continue |
| 1 per DC | etcd, global tier | Three DCs give a three-member global tier; with one DC, the DC tier serves both |
| 2 or more | `sprigdb controller` | One leads the DC; the others take over within the leader lease and serve write permits |
| 2 or more | `sprigdb gateway` | Behind a TCP load balancer that checks `/readyz` |
| 1 or more | `sprigdb worker` | Moves, index builds, search indexing, triggers |
| per cluster | MariaDB and `sprigdb agent` | One agent per MariaDB node |

Roles can share hosts. Run NTP on every host: leases tolerate clock drift up to `failover.clock_drift_margin_ms` (500 ms). See [Sizing](sizing.md).

## Install the binaries

Build the package from a release build of the workspace:

```sh
ARCH=amd64 VERSION=1.0.0 nfpm package -p deb -f deploy/packaging/nfpm.yaml
ARCH=arm64 VERSION=1.0.0 nfpm package -p rpm -f deploy/packaging/nfpm.yaml
```

The package installs:

| Path | What |
|---|---|
| `/usr/bin/sprigdb`, `/usr/bin/sprigctl` | The binaries |
| `/usr/lib/systemd/system/sprigdb-{gateway,controller,worker,agent}.service` | Role units |
| `/usr/lib/systemd/system/sprigdb-etcd-{dc,global}.service` | etcd units |
| `/usr/lib/systemd/system/mariadb.service.d/10-sprigdb-limits.conf` | The MariaDB drop-in |
| `/etc/sprigdb/sprigdb.toml` | The process file (mode 0640, group `sprigdb`; kept on upgrade) |
| `/etc/sprigdb/creds/` | Cluster credentials files (mode 0750, group `sprigdb`) |

Installing creates the system user and group `sprigdb` (home `/var/lib/sprigdb`, no login shell) and reloads systemd. It enables no unit: enable the roles each host runs yourself, or let the Ansible role do it.

## The systemd units

| Unit | Runs as | Command | Type | Watchdog | Stop timeout |
|---|---|---|---|---|---|
| `sprigdb-gateway.service` | `sprigdb` | `sprigdb gateway --config /etc/sprigdb/sprigdb.toml` | `notify` | 10 s | 90 s |
| `sprigdb-controller.service` | `sprigdb` | `sprigdb controller --config ...` | `notify` | 10 s | 30 s |
| `sprigdb-worker.service` | `sprigdb` | `sprigdb worker --config ...` | `notify` | 10 s | 120 s |
| `sprigdb-agent.service` | root | `sprigdb agent --config ...` | `notify` | 3 s | |
| `sprigdb-etcd-dc.service` | `etcd` | `/usr/local/bin/etcd --config-file /etc/sprigdb/etcd-dc.yaml` | `notify` | | |
| `sprigdb-etcd-global.service` | `etcd` | `/usr/local/bin/etcd --config-file /etc/sprigdb/etcd-global.yaml` | `notify` | | |

Every role unit restarts always (`RestartSec=1s`) and, except the agent, runs with `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome` and `PrivateTmp`, a state directory `/var/lib/sprigdb` and a logs directory `/var/log/sprigdb`. The gateway unit also raises the open-file limit to 1,048,576, grants `CAP_NET_BIND_SERVICE` so it can listen on ports below 1024, and sets `MALLOC_ARENA_MAX=2`. The agent keeps its state in `/var/lib/sprigdb-agent`.

`Type=notify` means systemd waits for the process to report itself ready: `sprigdb` sends `READY=1` when the role is ready (the same moment `/readyz` turns 200), and `WATCHDOG=1` every half watchdog interval while its internal watchdog ticks. A stalled process stops the pings and systemd restarts it. On stop, each role drains: a gateway turns `/readyz` off, waits `gateway.drain_lb_delay_seconds` (10 seconds) for the load balancer to notice, closes its listeners and gives sessions up to `gateway.drain_timeout_seconds` (30 seconds), within the unit's 90-second stop timeout.

The MariaDB drop-in orders `mariadb.service` after the agent, raises its open-file limit to 524,288, and sets `Restart=no`: MariaDB must not come back on its own behind the agent's back.

```sh
sudo systemctl enable --now sprigdb-controller.service
sudo systemctl status sprigdb-controller.service
journalctl -u sprigdb-gateway.service -f -o cat | jq .
```

## The process file

Each host needs a small `/etc/sprigdb/sprigdb.toml`. Everything else is a live setting in etcd. This is what the Ansible role writes:

```toml
[node]
dc = "fra1"
id = "fra1-gw-1"            # unique per process; defaults to the host name

[meta]
dc_seeds = ["https://fra1-etcd-1.example.net:2379", "https://fra1-etcd-2.example.net:2379", "https://fra1-etcd-3.example.net:2379"]
global_seeds = []           # empty: this DC's etcd also serves the global tier
deployment_id = "6f1c0d52-3b1e-4c47-9a55-0d6f8f6b2a10"

[tls]
ca = "/etc/sprigdb/tls/ca.pem"
cert = "/etc/sprigdb/tls/cert.pem"
key = "/etc/sprigdb/tls/key.pem"

[telemetry]
bootstrap_addr = "0.0.0.0:9180"
```

Controllers also need a `[controller]` section (Admin API address, token file, and `bootstrap = true` on the first DC's controllers), gateways a `[gateway]` section (backends file, workload key), and agents an `[agent]` section (below). Any field can be overridden with `SPRIGDB_<SECTION>_<FIELD>`. Every section and field is in [Configuration](configuration.md).

## The Ansible role

`deploy/ansible/site.yml` applies the role `sprigdb` to every host, one host at a time. A host's inventory groups decide what it runs: `<dc>_etcd`, `<dc>_controller`, `<dc>_gateway`, `<dc>_worker` and `<dc>_db` (MariaDB hosts, which run the agent).

```ini
[fra1_etcd]
fra1-etcd-[1:3].example.net

[fra1_controller]
fra1-ctl-[1:3].example.net

[fra1_gateway]
fra1-gw-[1:3].example.net

[fra1_worker]
fra1-wk-[1:2].example.net

[fra1_db]
fra1-db-[1:6].example.net

[fra1:children]
fra1_etcd
fra1_controller
fra1_gateway
fra1_worker
fra1_db

[fra1:vars]
sprigdb_deployment_id=6f1c0d52-3b1e-4c47-9a55-0d6f8f6b2a10
sprigdb_dc_seeds=["https://fra1-etcd-1.example.net:2379","https://fra1-etcd-2.example.net:2379","https://fra1-etcd-3.example.net:2379"]
```

| Variable | Default | Meaning |
|---|---|---|
| `sprigdb_version` | `0.0.1` | Package version to install |
| `sprigdb_package_dir` | `target/packages` beside the playbook | Where the `.deb` or `.rpm` files are |
| `sprigdb_dc` | from the host's groups | Data centre id |
| `sprigdb_roles` | from the host's groups | Roles this host runs |
| `sprigdb_deployment_id` | `""` | The deployment's id |
| `sprigdb_dc_seeds`, `sprigdb_global_seeds` | `[]` | etcd client URLs |
| `sprigdb_ca_pem`, `sprigdb_cert_pem`, `sprigdb_key_pem` | `""` | The deployment's CA and this host's certificate and key, PEM |
| `etcd_version` | `3.6.5` | etcd release downloaded on etcd hosts |
| `etcd_compaction` | periodic, `1h` | Written into each etcd configuration file |
| `etcd_defrag_on_calendar` | `*-*-* 03:17:00` | When the daily defrag timer runs |
| `etcd_defrag_cert`, `etcd_defrag_key` | the host's certificate | With etcd authentication on, a certificate whose CN is `root` |

The role installs the package (apt on Debian and Ubuntu, dnf on RHEL-compatible systems), unpacks etcd to `/usr/local/bin` on etcd hosts, sets auto-compaction in `/etc/sprigdb/etcd-dc.yaml` and `etcd-global.yaml`, installs a `sprigdb-etcd-defrag.timer`, writes the certificates to `/etc/sprigdb/tls/` (the key mode 0640, group `sprigdb`), writes `sprigdb.toml`, and enables and starts the host's units. Write the etcd configuration files yourself (member name, URLs, TLS); the role only adds the compaction keys. On upgrades, the handlers restart the units one host at a time.

## Install MariaDB

SprigDB doesn't install MariaDB on VMs: install MariaDB 11.8 or 12.3 from MariaDB's repository, with `mariadb-backup`, and configure it as below. Then register the cluster (see [Clusters](clusters.md)).

### Server settings

Put these in `/etc/mysql/mariadb.conf.d/60-sprigdb.cnf` (Debian and Ubuntu) or `/etc/my.cnf.d/60-sprigdb.cnf` (RHEL-compatible):

```text
[mariadbd]
server_id                       = 701        # cluster number x 100 + node index
gtid_domain_id                  = 7          # the cluster number
log_bin                         = mariadb-bin
binlog_format                   = ROW
binlog_row_image                = FULL
binlog_row_metadata             = FULL
binlog_annotate_row_events      = ON
replicate_annotate_row_events   = ON
binlog_expire_logs_seconds      = 604800     # 7 days
sync_binlog                     = 1
innodb_flush_log_at_trx_commit  = 1
gtid_strict_mode                = ON
log_slave_updates               = ON
max_allowed_packet              = 68157440   # the gateway's 64 MiB limit + 1 MiB
skip_name_resolve               = ON
open_files_limit                = 524288
table_open_cache_instances      = 8
innodb_buffer_pool_size         = 9G         # about 60% of the host's memory
innodb_log_file_size            = 2304M      # a quarter of the buffer pool, at least 1G
read_only                       = ON         # boot fenced; the agent unfences the writer
require_secure_transport        = ON
ssl_ca                          = /etc/mysql/tls/ca.pem
ssl_cert                        = /etc/mysql/tls/server.pem
ssl_key                         = /etc/mysql/tls/server-key.pem
```

For a primary with replicas, add semi-synchronous replication that waits `AFTER_SYNC`, so an acknowledged commit is always on a replica:

```text
rpl_semi_sync_master_enabled    = ON         # OFF on replicas; switched on at promotion
rpl_semi_sync_slave_enabled     = ON
rpl_semi_sync_master_wait_point = AFTER_SYNC
rpl_semi_sync_master_timeout    = 10000
report_host                     = 10.8.1.11  # the address replicas reach this node at
report_port                     = 3306
```

For Galera, use `wsrep_sst_method = mariabackup`, `wsrep_gtid_mode = ON`, `wsrep_gtid_domain_id` set to the cluster number, `binlog_format = ROW` and `innodb_autoinc_lock_mode = 2`.

Give every node of a cluster the same `gtid_domain_id`, and give each cluster its own. Seven days of binlog is what the change feed and the mover read from; size the disk for it.

### System accounts

Create two accounts on every node, without writing them to the binlog so that a new replica's binlog stays empty:

```sql
SET SESSION sql_log_bin = 0;
CREATE USER 'sprig_ctl'@'%' IDENTIFIED BY 's3cret-Passw0rd' REQUIRE SSL;
GRANT ALL PRIVILEGES ON *.* TO 'sprig_ctl'@'%' WITH GRANT OPTION;
CREATE USER 'sprig_repl'@'%' IDENTIFIED BY 's3cret-Passw0rd-2' REQUIRE SSL;
GRANT REPLICATION SLAVE, REPLICATION SLAVE ADMIN, BINLOG MONITOR, SLAVE MONITOR, SELECT, RELOAD
  ON *.* TO 'sprig_repl'@'%';
```

`sprig_ctl` manages tenant schemas and accounts, sets server variables and fences nodes. `sprig_repl` reads the binlog for the change feed and the mover. Store both in a credentials file that `sprigctl cluster register --credentials file:<path>` names; the passwords never go into etcd.

## The agent and self-fencing

SprigDB never trusts a MariaDB node to be the writer just because it is up. Two mechanisms keep a cut-off part of the system from accepting writes:

- **Gateways self-fence.** A gateway writes only while it holds a valid permit from a controller of its DC, and the controllers renew permits only while they can reach etcd and hold the DC lease (`failover.dc_lease_seconds`, 10 seconds). A gateway that loses its controllers stops writes `failover.fence_lead_ms` (3 seconds) before its permit could expire, and clients get 39501. With `failover.self_fence = false` gateways don't fence; that also disables automatic DC failover.
- **The agent fences its MariaDB node.** Every 200 ms, the agent decides whether its node may serve writes: it must hold a valid DC permit, hold the cluster's writer lease in etcd (`failover.writer_lease_seconds`, 10 seconds), and not be replicating from another node. If so, it sets `read_only = OFF`. Otherwise it sets `read_only = ON` and kills every tenant session. MariaDB boots `read_only`, so a node is writable only while its agent says so.

The agent also:

- runs a watchdog on its own operating-system thread. If the agent's runtime stalls for `failover.watchdog_ms` (1 second), the watchdog fences the node directly over its own connection; at twice that, it fences and aborts the process. A process that was paused fences on resume.
- registers itself in etcd under a lease, so the DC leader knows the node fences itself.
- reports a node whose connections are refused for 2 seconds as `down` and hands back the writer lease, so the DC leader can promote a replica at once.
- switches the primary side of semi-synchronous replication off while no replica is attached and back on once one is, so a newly promoted primary's first commit doesn't wait out `rpl_semi_sync_master_timeout`.

When the agent restarts on the writer, its node is fenced until it has its permit and writer lease again, usually well under a second, during which writes fail with MariaDB's 1290 (read-only). Applications that retry ride through it.

### Configure the agent

Install the agent on every MariaDB host, give it `[node]`, `[meta]` and `[tls]` like the other roles, and name its node:

```toml
[node]
dc = "fra1"
id = "fra1-db-1"

[agent]
cluster = "fra1-c7"                   # the cluster's id, as sprigctl cluster list shows it
node = "n1"                           # the node's id in the cluster record
internal_addr = "0.0.0.0:7470"        # optional: the rebuild service
internal_advertise = "https://10.8.1.11:7470"
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `cluster` | string | unset | The cluster the node belongs to. Unset: the agent fences nothing and only reports ready |
| `node` | string | the node whose address is `mysql_addr` | The node's id in the cluster record |
| `mysql_addr` | `host:port` | the node's address in the record | How the agent reaches its local MariaDB |
| `credentials` | `file:<path>#<table>` | the cluster's `sprig_ctl` reference | The account the agent fences with; it needs `READ_ONLY ADMIN` and `CONNECTION ADMIN` |
| `internal_addr` | socket address | unset | Where the rebuild service listens. Unset: the node can't be rebuilt or be a rebuild's source |
| `internal_advertise` | URL | the scheme and bound address | How workers and other agents reach the service |
| `worker_threads` | int | `0` (one per CPU) | Runtime threads |

The agent can start before its cluster is registered: it retries every second until the record names its node. With `[tls]`, the rebuild service uses mutual TLS with the process certificate; without it, plain HTTP, for development only.

`[agent.rebuild]` says how the agent stops, starts and restores its node when `sprigctl cluster rebuild <cluster> <node>` rebuilds it from a replica's backup:

| Field | Default | Meaning |
|---|---|---|
| `exec_prefix` | `[]` | Put before every command, such as `["docker", "exec", "-i", "mariadb-1"]` for a node in a container |
| `stop_command` | `["systemctl", "stop", "mariadb"]` | Stops mysqld and returns once it has stopped |
| `start_command` | `["systemctl", "start", "mariadb"]` | Starts mysqld (it boots `read_only`) |
| `datadir` | `/var/lib/mysql` | MariaDB's data directory |
| `staging_dir` | `/var/lib/sprigdb/rebuild` | Where a backup is received and prepared; needs room for one copy of the data |
| `owner` | `mysql:mysql` | Owner of the restored files |
| `backup_bin` | `mariadb-backup` | |
| `mbstream_bin` | `mbstream` | |

Watch `sprig_agent_fenced` (1 while the node is fenced) and `sprig_agent_writer_lease_remaining_seconds` on each agent's telemetry port. See [Failover](failover.md) for what the controller does when a writer is lost.

## Not supported yet

- Creating MariaDB clusters on VM hosts from SprigDB (installing packages, writing settings, seeding replicas): install and register clusters yourself.
- Supervising mysqld from the agent: systemd starts MariaDB.

## Related

- [Deploy with Docker Compose](deploy-compose.md)
- [Configuration](configuration.md)
- [Clusters](clusters.md)
- [Failover](failover.md)
- [Sizing](sizing.md)
