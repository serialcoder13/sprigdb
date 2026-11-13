---
title: Clusters
audience: [operator]
sidebar:
  order: 5
---

# Clusters

A cluster is one MariaDB server, or a set of servers that replicate one data set, registered with SprigDB in one data centre. SprigDB places tenants and shards on clusters, probes them, fails them over and moves data off them. This page covers registering an existing cluster, what SprigDB checks, how it watches health, and how to drain and remove one.

Clusters that SprigDB creates itself, on Kubernetes or on VMs, register themselves; see [Deploying on Kubernetes](deploy-kubernetes.md) and [Deploying on VMs](deploy-vms.md).

## Topologies

| Topology | `--topology` | Writes go to | Failover |
|---|---|---|---|
| Single node | `single` (or `single_node`) | The one node | None inside the cluster: a restart isn't a failover. Tenants are promoted from a standby in another data centre. |
| Primary and replicas | `primary_replica` (or `replication`) | The primary | SprigDB promotes the most advanced replica, within the recovery point objective. Semi-synchronous replication (`AFTER_SYNC`) makes it lossless. |
| Galera | `galera` | Any synced node; SprigDB picks one per shard | Writers move to another synced node at once; SprigDB bootstraps a cluster that lost quorum only from the most advanced node. |
| External | `external` | The writer endpoint you declare | The provider (a managed service). SprigDB follows the endpoint. |

See [Failover](failover.md) for each in detail.

## Before you register

MariaDB 11.8 or later. Every node needs:

| Requirement | Why | Severity |
|---|---|---|
| TLS on every node | SprigDB connects to MariaDB only over TLS | blocking |
| `default_storage_engine = InnoDB` | Transactions, online moves | blocking |
| `lower_case_table_names` equal to the fleet's `cluster.lower_case_table_names` (default `0`) | Names must resolve the same everywhere | blocking |
| `log_bin` on | The mover and the change feed read the binlog | blocking |
| `binlog_format = ROW` | Row events | blocking |
| `binlog_row_image = FULL` | Whole before and after images | blocking |
| `gtid_strict_mode = ON` | Positions are GTIDs | blocking |
| `log_slave_updates = ON` on replicas and Galera nodes | Every node logs what it applies | blocking |
| `binlog_annotate_row_events = ON`, `replicate_annotate_row_events = ON` | Statement text alongside row events | degrades the feed |
| `binlog_row_metadata = FULL` | Column names in the binlog | advisory |
| `binlog_expire_logs_seconds` at least the feed's retention, and at least `cluster.min_binlog_retention_hours` (24 h) | Moves and standbys resume from the binlog | advisory |
| `require_secure_transport = ON` | Refuse unencrypted logins from anything else | advisory |

### System accounts

SprigDB uses two accounts on every external cluster. Create them yourself:

```sql
CREATE USER 'sprig_ctl'@'%' IDENTIFIED BY 's3cret-Passw0rd' REQUIRE SSL;
GRANT CREATE USER ON *.* TO 'sprig_ctl'@'%';
GRANT ALL ON `t%`.* TO 'sprig_ctl'@'%' WITH GRANT OPTION;
GRANT ALL ON `\_\_sprig\_sys`.* TO 'sprig_ctl'@'%' WITH GRANT OPTION;

CREATE USER 'sprig_repl'@'%' IDENTIFIED BY 's3cret-Passw0rd' REQUIRE SSL;
GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO 'sprig_repl'@'%';
```

- `sprig_ctl` creates tenant schemas (`t<id>_...`) and accounts, probes, and fences. Missing privileges beyond these disable only what needs them, and the precheck lists each as an advisory with the `GRANT` that fixes it. For example, without `SUPER` or `log_bin_trust_function_creators = ON`, compiled logic is off.
- `sprig_repl` reads the binlog for the mover and the change feed.

On external clusters SprigDB never changes server settings, restarts servers, adds or removes nodes, or touches schemas other than `t%` and `__sprig_sys`.

### Credentials by reference

Passwords never go into etcd. A cluster record names its credentials by reference, and only controllers and workers resolve them:

| Reference | Reads |
|---|---|
| `file:/etc/sprigdb/creds/fra1-c1.toml` | A TOML file (mode 0600, or a mounted Kubernetes Secret) |
| `file:/etc/sprigdb/creds/fra1-c1.toml#sprig_ctl` | One table of that file |
| `env:SPRIG_CTL` | A TOML document from an environment variable |

One file can hold both accounts:

```toml
[sprig_ctl]
user = "sprig_ctl"
password = "s3cret-Passw0rd"
tls = "verify"                              # the default; or "skip_verify" (development only)
ca_file = "/etc/sprigdb/tls/mariadb-ca.pem" # optional; else the built-in roots

[sprig_repl]
password = "s3cret-Passw0rd"                # user defaults to the table's name
```

`verify` checks the server's certificate; `skip_verify` encrypts without checking it, for development servers with self-signed certificates. The file must exist at the same path on every controller and worker of the data centre.

## Register a cluster

```sh
sprigctl cluster register fra1-main --dc fra1 --topology primary_replica \
    --node db1.fra1.internal:3306 --node db2.fra1.internal:3306 --node db3.fra1.internal:3306 \
    --credentials file:/etc/sprigdb/creds/fra1-main.toml --memory-gb 256 --storage-gb 4000
```

The positional argument is the cluster's name. SprigDB assigns its id, `<dc>-c<n>` (here `fra1-c1`), in registration order; every other command takes the id.

| Flag | Meaning |
|---|---|
| `--topology` | `single`, `primary_replica`, `galera` or `external` |
| `--node [id=]host:port` | A node; repeat for each, the writer first. Without `id=`, nodes are `n1`, `n2`, ... |
| `--credentials <ref>` | One reference to a file with `[sprig_ctl]` and `[sprig_repl]` tables |
| `--ctl-secret <ref>`, `--repl-secret <ref>` | Separate references per account, instead of `--credentials` |
| `--memory-gb` | Server memory. It is recorded with the cluster, but nothing uses it yet |
| `--storage-gb` | Storage, for placement |
| `--declare <file>` | A TOML file of `[declared]` facts (external clusters) |
| `-f <file>` | Every field from a file; flags override it |
| `--dc` | The data centre the cluster is in |
| `--dry-run` | Run the prechecks only; register nothing |

Point `sprigctl` at a controller of the cluster's data centre. Registration writes the cluster record, so it runs on the data centre's leader; another controller answers 39504 `ER_SPRIG_NOT_LEADER` and names the leader.

### The registration file

```toml
dc = "fra1"
name = "fra1-ext1"
topology = "external"
credentials_ref = "file:/etc/sprigdb/creds/fra1-ext1.toml"

[endpoints]                  # or [[nodes]] with node, addr and zone
writer = "db-primary.fra1.internal:3306"
readers = ["db-r1.fra1.internal:3306", "db-r2.fra1.internal:3306"]

[capacity]
memory_gb = 256
storage_gb = 4000
connections = 2000

[declared]
failover = "provider"        # provider or none
rpo_seconds = 0
trust = "asserted"           # asserted or unverified
replica_dcs = ["fra1"]       # for residency
```

```sh
sprigctl cluster register -f fra1-ext1.toml
```

`[capacity]` also accepts `write_rows_per_sec` and `busy_ms_per_sec`. `[declared]` also accepts `writer_endpoint`, `reader_endpoints`, `storage_gb` and `memory_gb`. Use it for what probes can't see on a managed service: who fails it over, its RPO, and which data centres hold its replicas.

### The prechecks

Registration runs these steps, in order:

1. Connect to every node as `sprig_ctl` over TLS. A node without TLS is refused.
2. Compare `SHOW GRANTS` with the required list.
3. Read the required server variables (the table above).
4. Create and drop a probe user and a probe schema (`t0_sprig_probe_<hex>`).
5. As `sprig_repl`, open a binlog dump at the current position and read one event.
6. Probe the cluster and build its capability profile.
7. Make sure the gateways' monitoring account `sprig_gw` exists, or say what to run.

If anything blocks, nothing is registered and the command fails with 39511 `ER_SPRIG_CLUSTER_PRECHECK_FAILED`, listing every missing item with its fix:

```text
prechecks:    failed (2 blocking)
  NODE  ADDRESS          ROLE    VERSION                     TLS
  n1    127.0.0.1:13311  writer  11.8.9-MariaDB-ubu2404-log  TLS_AES_256_GCM_SHA384
  n2    127.0.0.1:13312  -       -                           -
blocking:
  - node n2: binlog_row_image is MINIMAL, needs FULL: SET GLOBAL binlog_row_image = 'FULL'
  - sprig_repl can't read binlogs: GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO sprig_repl
advisories:
  - node n1: require_secure_transport is OFF: SET GLOBAL require_secure_transport = ON
```

A binlog that `sprig_repl` can't read for a reason the cluster can fix later (missing `REPLICATION SLAVE`, or a binlog checksum or format the reader doesn't know) is an advisory: the cluster registers with the change feed and the mover `unsupported`. The binlog reader authenticates only with `mysql_native_password` (MariaDB's default for `IDENTIFIED BY`), so don't give `sprig_repl` another authentication plugin. A credential that doesn't resolve or log in, or a dump that times out, blocks.

## Look at a cluster

```sh
sprigctl cluster list
```

```text
ID       NAME              TOPOLOGY         STATE   HEALTH   NODES  PROFILE  ZERO_LOSS  XA
dc-a-c1  dc-a-single       single_node      active  healthy  1      v3       no         yes
dc-a-c2  dc-a-replication  primary_replica  active  healthy  3      v3       yes        yes
dc-a-c3  dc-a-galera       galera           active  healthy  3      v3       yes        no
```

```sh
sprigctl cluster show dc-a-c2
```

```text
cluster:      dc-a-c2 (dc-a-replication)
dc:           dc-a
topology:     primary_replica
provisioner:  external
state:        active
health:       healthy since 2026-11-07T10:00:00Z
nodes:        n1 127.0.0.1:13311 [ok, writer]
              n2 127.0.0.1:13312 [ok, replica, lag 9201 ms]
              n3 127.0.0.1:13313 [ok, replica, lag 9201 ms]
profile:      v3
  server                11.8.9-MariaDB-ubu2404-log (lower_case_table_names 0, InnoDB)
  semi_sync             enabled, active, AFTER_SYNC, 2 client(s)
  binlog                ROW, row image FULL, metadata FULL, gtid_strict_mode yes, expire 604800 s
  xa                    yes
  zero_loss_failover    yes (expected loss zero)
  read_your_writes      gtid_wait
  mover                 source yes, target yes
  change_feed           full, retention 604800 s
  fence_methods         revoke_kill, rename
  validation            replica_build
  online_index_build    yes
  enforce_json_schema   yes
  compiled_logic        yes
gaps:         none
```

Add `-o json` for the full record.

### The capability profile

The profile is what SprigDB has measured the cluster can do, and every feature checks it before relying on it:

| Field | Used for |
|---|---|
| `semi_sync`, `zero_loss_failover` | Whether failover can promote without loss. Placement prefers clusters that match a tenant's RPO. |
| `binlog`, `change_feed`, `mover` | Whether shards can move on or off the cluster, and whether search, triggers and streams can follow it |
| `xa` | Transactions across shards on this cluster. Galera reports `no`. |
| `read_your_writes` | How a read on a replica waits for the session's last write |
| `fence_methods` | How writes to a shard are stopped for a cutover or failover |
| `validation`, `online_index_build` | How automatic indexes are tested and built |
| `compiled_logic`, `enforce_json_schema` | Whether logic can run inside MariaDB, and JSON schema checks |

A feature that needs a missing capability fails with 39102 `ER_SPRIG_CLUSTER_CAPABILITY_REQUIRED`.

## Probes and health

The data centre's leader probes every registered cluster:

| Probe | Period | Setting |
|---|---|---|
| Light (connect, role, lag, heartbeat) | 5 s | `cluster.health_interval_seconds` |
| Full (variables, grants, binlog, profile) | 300 s | `cluster.probe_interval_seconds` |
| Writer heartbeat, read on replicas to measure lag | 1000 ms | `cluster.heartbeat_interval_ms` |

To probe now, for example after changing a server variable:

```sh
sprigctl cluster probe dc-a-c2 --full
```

```text
probe:        full
changes:
  - derived.zero_loss_failover: true -> false (downgrade)
  - semi_sync.enabled: true -> false (downgrade)
held:         derived.xa (upgrades wait cluster.capability_upgrade_sustain_seconds)
```

`--full` also runs the XA, feature and routine probes. A downgrade is published at once. An upgrade is held until it has lasted `cluster.capability_upgrade_sustain_seconds` (60 s), so a flapping variable can't make features switch on and off.

### Node and cluster health

| Node state | Meaning |
|---|---|
| `ok` | Reachable, in the expected role, lag in bounds |
| `lagging` | Lag above `cluster.max_replica_lag_seconds` (10 s) |
| `desynced` | A Galera donor, or desynced for an index build |
| `joining` | A Galera state transfer |
| `broken` | Replication stopped, unexpected `read_only`, failed login, or a Galera node outside the primary component |
| `unreachable` | `cluster.unhealthy_after_failures` (3) probes failed in a row |

| Cluster health | Meaning | Effect |
|---|---|---|
| `healthy` | Every node `ok` | Normal |
| `degraded` | A writer exists, but something is wrong | No new placements |
| `no_writer` | Positively no writer | Writes fail with 39100 `ER_SPRIG_CLUSTER_UNAVAILABLE` until failover restores one |
| `unknown` | No probe of the writer succeeds from this controller | Nothing automatic acts |

A controller that can't reach a writer reports `unknown`, never `no_writer`, so a controller cut off from the network never triggers a failover by itself.

Probe timing is tunable per cluster: `cluster.probe_timeout_ms` (2000), `cluster.clock_skew_warn_ms` (500), `cluster.galera_writer_rejoin_seconds` (120: how long a Galera node must be healthy before it rejoins the writer set).

## Change node addresses

When a node's address changes, or you add or remove a node:

```sh
sprigctl cluster set-nodes fra1-c1 --node n2=db2-new.fra1.internal:3306
sprigctl cluster set-nodes fra1-c1 --node n4=db4.fra1.internal:3306       # a new id is added
sprigctl cluster set-nodes fra1-c1 --remove n3
sprigctl cluster set-nodes fra1-c1 --only --node n1=db1:3306 --node n2=db2:3306   # the whole list, writer first
sprigctl --dry-run cluster set-nodes fra1-c1 --node n2=db2-new:3306
```

A node named in `--node` keeps its zone and gets the new address. `--only` makes the list exact and ordered. A cluster needs at least one node.

## Drain and remove a cluster

```text
active ─drain─► draining ─no shard left─► drained ─retire─► retired
```

```sh
sprigctl cluster drain fra1-c2            # asks you to type the id, unless --yes
sprigctl job list                         # the moves the drain started
sprigctl cluster show fra1-c2             # state: drained once empty
sprigctl cluster retire fra1-c2
```

- `drain` stops new placements on the cluster, and the balancer moves its shards and tenants to other active clusters of the data centre, online. An empty cluster becomes `drained` at once.
- A tenant pinned to the cluster (`sprigctl tenant pin`) blocks the drain and is reported as blocked. Unpin it, or move it yourself with `sprigctl shard move`.
- `retire` works only on a `drained` cluster; otherwise it fails with 39106 `ER_SPRIG_CLUSTER_STATE_INVALID`. Probes stop and the record stays as `retired`.
- `--dry-run` says what would happen.

After `retire` you can shut the servers down. Nothing in SprigDB refers to them any more.

## Recover a node

Two commands deal with a former writer that diverged after a failover. See [Failover](failover.md#a-former-writer-that-diverged).

```sh
sprigctl cluster extract fra1-c1 n1          # save its diverged transactions, sealed
sprigctl cluster rebuild fra1-c1 n1          # rebuild it from a replica's backup
```

## Not supported yet

- Automatic tuning of MariaDB server variables (caches and memory budgets). The `tuning.*` settings are registered, but nothing reads them yet; size the server variables yourself, as in [Sizing](sizing.md#mariadb).

## Related

- [Clusters and data centres](../concepts/clusters-and-data-centres.md)
- [Failover](failover.md)
- [Data centres](data-centres.md)
- [Backups and moves](backups-and-moves.md)
- [sprigctl](../tools/sprigctl.md)
