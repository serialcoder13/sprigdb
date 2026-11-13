---
title: Clusters and data centres
audience: [dba, operator]
sidebar:
  order: 3
---

# Clusters and data centres

A cluster is a set of MariaDB servers that SprigDB stores shards on, and a data centre is a site that holds clusters, gateways and a control plane. Read this page to understand which MariaDB topologies SprigDB uses, what it learns about each cluster, and how requests and reads find the right server across data centres.

## Clusters

A **cluster** is one replicated MariaDB unit: a single server, a primary with replicas, or a Galera cluster. Every shard lives on exactly one cluster, and one cluster holds shards of many tenants.

Each cluster has an id made of its data centre and a number, such as `mum1-c7`, and a name that is unique in its data centre.

### Topologies

| Topology | `--topology` | Writes | Reads | Expected loss on failover |
|---|---|---|---|---|
| Single node | `single` (or `single_node`) | The node | The node | Everything since the last backup |
| Primary with replicas | `primary_replica` (or `replication`) | The primary | Replicas that have caught up, else the primary | None with semi-synchronous replication (`AFTER_SYNC`, at least one replica connected); otherwise recent commits may be lost |
| Galera | `galera` | One node, chosen by SprigDB | Any synced node | None while every node is synced |
| External (managed) | `external` | The declared writer endpoint | The declared reader endpoints | What you declare |

An **external** cluster is a managed service, such as a cloud database, where SprigDB can't see or control everything. You tell SprigDB what the provider guarantees in a `[declared]` TOML table:

```toml
[declared]
failover = "provider"        # provider or none: who fails it over
rpo_seconds = 0              # the provider's recovery point objective
trust = "asserted"           # asserted (you vouch for it) or unverified
replica_dcs = ["fra1"]       # data centres that hold replicas, for residency
```

SprigDB treats an external cluster as zero-loss only if it declares `rpo_seconds = 0` with `trust = "asserted"`, or if probing shows semi-synchronous replication.

Every cluster must run MariaDB 11.8 or later, with InnoDB as the default storage engine and the same `lower_case_table_names` as the rest of the fleet (the global setting `cluster.lower_case_table_names`, default 0).

### Registration

An operator registers a cluster with `sprigctl`, in the data centre it belongs to:

```sh
sprigctl cluster register fra1-main \
    --topology primary_replica \
    --node 10.0.1.10:3306 --node 10.0.1.11:3306 --node 10.0.1.12:3306 \
    --credentials file:/etc/sprigdb/creds/fra1-main.toml
```

| Flag | Meaning |
|---|---|
| `[name]` | A name unique in the data centre |
| `--topology` | `single`, `primary_replica`, `galera` or `external` |
| `--node [id=]host:port` | One node; repeat for each, the writer first |
| `--credentials` | A file (`file:<path>`) or environment variable (`env:<name>`) with `[sprig_ctl]` and `[sprig_repl]` tables: the control account and the replication account SprigDB uses |
| `--ctl-secret`, `--repl-secret` | The two credentials separately |
| `--declare <file>` | A TOML file with the `[declared]` table, for an external cluster |
| `--storage-gb`, `--memory-gb` | Server storage, which the balancer uses to place shards, and memory, which is only recorded |

Registration runs a precheck, and the cluster then moves through these states:

```text
register ─► probing ─gates pass─► active ─drain─► draining ─empty─► drained ─retire─► retired
               │                    ▲
               └─gate fails─► rejected ─a later probe passes
```

| State | Meaning |
|---|---|
| `probing` | Registered; the first probe hasn't decided yet |
| `active` | In service: new shards and tenants may be placed on it |
| `rejected` | A registration gate failed. A later probe that passes makes it `active` |
| `draining` | Takes no new placements; its shards are moving away |
| `drained` | Empty |
| `retired` | Gone for good; probes stop |

A cluster is **rejected** when a node won't accept the control account's login, MariaDB is older than 11.8, the default engine isn't InnoDB, or `lower_case_table_names` doesn't match the fleet. Missing privileges don't reject it. They're listed as advisories, each with the `GRANT` that fixes it, and they turn off only the features that need them.

Other cluster commands:

```sh
sprigctl cluster list
sprigctl cluster show fra1-c2          # nodes, health, capability profile, gaps
sprigctl cluster probe fra1-c2 --full  # probe now, including XA and feature probes
sprigctl cluster drain fra1-c2         # stop placing here and move the shards away
sprigctl cluster retire fra1-c2        # after the drain has emptied it
sprigctl cluster set-nodes fra1-c2 --node n2=10.0.1.21:3306
```

See [Clusters](../operate/clusters.md) for the full procedures.

### Probing and the capability profile

The controller probes every node of every cluster. It doesn't trust the topology name: it asks the servers what they can do, and the result is the cluster's **capability profile**. Everything that depends on the backend, such as read routing, the mover, failover or index builds, reads the profile, never the version or the topology.

| Probe | Period | Setting |
|---|---|---|
| Light probe (health) | 5 s | `cluster.health_interval_seconds` |
| Heartbeat write | 1 s | `cluster.heartbeat_interval_ms` |
| Full probe | 300 s | `cluster.probe_interval_seconds` |
| Statement timeout of a probe | 2 s | `cluster.probe_timeout_ms` |

A node is marked unreachable after `cluster.unhealthy_after_failures` failed probes in a row (default 3). A replica more than `cluster.max_replica_lag_seconds` behind (default 10) is marked lagging.

The profile records:

- **Server:** the lowest MariaDB version across the nodes, `lower_case_table_names`, the default engine and the largest clock skew seen.
- **Replication:** semi-synchronous state, binlog format and row image, GTID strict mode, `log_slave_updates`, binlog expiry; on Galera, cluster size, synced nodes and GTID mode.
- **Privileges** of the control account.
- **Features** such as JSON and generated-column support, and routine limits.

From these facts SprigDB derives what automation may rely on:

| Derived capability | True when |
|---|---|
| XA (distributed transactions) | The XA probe passes, the cluster isn't Galera (MariaDB refuses `XA START` with Galera on), and a prepared transaction would survive a failover |
| Zero-loss failover | Galera with every node synced; or a primary with semi-synchronous `AFTER_SYNC` replication, active, with a replica connected; or an external cluster that declares it |
| Read-your-writes method | Galera: a sync-wait check. Primary with replicas: wait for the GTID on a replica. Single node: read from the writer |
| Mover source | The binlog is on, in `ROW` format with a `FULL` row image, GTID strict mode is on, and the binlog can be read |
| Mover target | The control account may create and grant on tenant schemas |
| Online index builds | Not Galera |
| Index validation | Galera with three or more nodes, or a primary with replicas and the needed privileges |

A capability that disappears is published at once. One that appears must hold for `cluster.capability_upgrade_sustain_seconds` (default 60) first, so a flapping capability is always seen at its worse value. A node that timed out isn't counted as lacking a capability.

## Data centres

A **data centre** (DC) is a site: a region, a zone or a building. Each has an id such as `mum1`, `fra1` or `iad1`, and holds:

- its own clusters;
- its gateways, controllers and workers;
- its own etcd tier of the control plane.

Register a DC, then its clusters:

```sh
sprigctl dc register fra1 --tags country=de,jurisdiction=eu
sprigctl dc list
sprigctl dc show fra1        # the DC and its current leader controller
sprigctl dc latency          # measured p50, p90 and p99 latency between DCs
```

A new DC starts as `joining`; the global leader makes it `active`. `sprigctl dc drain fra1` stops new tenants from being placed there, and the DC is retired once its tenants have moved out. `sprigctl dc undrain` reverses a drain. A DC can also be registered with `--kind witness`: a site that holds no data and only helps decide failovers.

Tags such as `country` and `jurisdiction` are what [data residency](../security/residency.md) rules match.

### The home DC

Every tenant has a **home DC**: the DC whose control plane holds its shard maps and whose clusters hold its catalog. A tenant is created in the DC of the controller that `sprigctl` talks to (`--dc` chooses another).

Gateways in every DC count each tenant's reads and writes by the client's DC and report them to the home DC. When most of a tenant's traffic comes from elsewhere, the migration loop moves the tenant toward it, online. The loop's behaviour is set by the `migration.*` settings:

| Setting | Default | Meaning |
|---|---|---|
| `migration.mode` | `auto` | `auto`, `approve`, `recommend` or `off`. See [Approvals](approvals.md) |
| `migration.window_minutes` | 30 | Traffic window the decision looks at |
| `migration.sustain_minutes` | 15 | How long a better DC must stay better |
| `migration.min_improvement_pct` | 30 | Relative latency gain a move needs |
| `migration.min_improvement_ms` | 5 | Absolute latency gain a move needs |
| `migration.cooldown_hours` | 6 | Wait after a move; doubles with each move back |
| `migration.auto_size_limit_gb` | 200 | Larger tenants need approval to move |
| `migration.write_pause_target_ms` | 5000 | Budget for the tenant-wide write pause at the end of a move |

Operators can also move, pin and inspect tenants by hand:

```sh
sprigctl tenant traffic acme       # traffic by client DC, and the move it suggests
sprigctl tenant move acme --to fra1 --wait   # move to another DC, online
sprigctl tenant pin acme           # keep it on its cluster and DC
sprigctl explain placement acme    # why the tenant is where it is
```

See [Data centres](../operate/data-centres.md).

### Naming the client's DC

A gateway learns which DC a client is in from a hint:

| Protocol | Hint |
|---|---|
| MySQL and MariaDB | The connection attribute `sprig_dc`, or `SET @sprig_dc = 'fra1'` |
| REST | The header `Sprig-DC: fra1` |
| gRPC | The metadata `sprig-dc` |
| MongoDB | An `appName` ending in `~<dc>`, such as `orders~fra1` |
| Redis | A client name ending in `~<dc>`: `CLIENT SETNAME orders~fra1`, or `HELLO ... SETNAME orders~fra1` |

Each statement or command of a session with a known hint counts as a read or a write from that DC, in the traffic that `sprigctl tenant traffic` shows and the migration loop weighs. `SET @sprig_dc` to a DC that isn't registered answers OK with warning 39503 `ER_SPRIG_UNKNOWN_DC`, and the hint is ignored; on MongoDB and Redis an unknown DC is ignored silently. `sprig_gateway_dc_hints_total` counts hints by `protocol` and `result` (`known`, `unknown` or `missing`). Hints only feed placement: they never choose which copy serves a read, and never grant access.

## The control plane

SprigDB keeps its control state in etcd, split into two tiers:

| Tier | Keys | Holds |
|---|---|---|
| Global | `/sprig/g/` | The DC registry, the tenant directory (names, ids, state, home DC, the tree), id counters, global-scope settings |
| One per DC | `/sprig/dc/<dc>/` | The DC's clusters and their profiles, shard maps of the tenants homed there, leader elections, leases, gateway, controller and worker registrations, jobs, mover state, DC-scope settings |

Each DC runs its own etcd for its tier, so a DC keeps working on its own tenants when it loses contact with the others. The global tier is small and changes rarely: tenant creation, DC changes and global settings. A deployment with one DC can serve both tiers from one etcd.

Each DC elects one **leader controller**, which runs that DC's probes, placement, balancing, failover decisions and job scheduling. Gateways, workers and agents hold permits under the DC's lease, renewed every few seconds. A DC that can't renew its lease fences itself, so two DCs never both accept writes for the same tenant. See [Failover](../operate/failover.md).

## How a gateway routes requests

Any gateway in any DC can serve any tenant. For each statement, the gateway:

1. finds the tenant's directory entry and its home DC;
2. reads the shard map from the home DC's tier, and keeps following it, so shard moves and write pauses reach it wherever the tenant lives;
3. picks the shards the statement touches and, for each, the cluster's writer or a reader;
4. connects to that MariaDB node directly.

A client far from the home DC still works, but every statement crosses the distance between the DCs. That's what the migration loop fixes over time, or what you avoid by connecting to a gateway in the tenant's home DC.

## Reads and read-your-writes

A **plain read**, such as a `SELECT` with no locking clause, no `INTO` and no function with side effects, may run on a replica or another Galera node. Everything else, including any statement SprigDB can't classify, runs on the shard's writer. A session that holds its connection, such as one in an open transaction, keeps all its reads on that connection.

After every commit the gateway records the session's GTID position. A later plain read must see at least that position (and, by default, every commit the same tenant made through this gateway). The gateway then picks:

- **Primary with replicas:** a healthy replica within `reads.max_replica_lag_ms` whose position already covers the target; or one expected to catch up soon, with a `MASTER_GTID_WAIT` prefix bounded by `reads.gtid_wait_timeout_ms`; otherwise the primary. If the wait times out, the read reruns on the primary.
- **Galera:** a node whose position covers the target, with no wait. If no node but the writer is known to have it, `reads.galera_unconfirmed` decides: `check` (the default) runs a quick sync-wait check on another node first, and `writer` keeps the read on the writer.
- **Single node and external clusters:** the writer.

So you read your own writes on every topology, without hints in your SQL.

| Setting | Default | Meaning |
|---|---|---|
| `reads.routing` | `auto` | `auto` lets plain reads leave the writer; `writer` keeps every read on the writer |
| `reads.causal_scope` | `tenant_gateway` | `session`: reads see the session's own commits. `tenant_gateway`: also every commit of the tenant through this gateway |
| `reads.max_replica_lag_ms` | 1000 | Never read from a replica further behind |
| `reads.gtid_wait_timeout_ms` | 100 | Longest wait for a replica to catch up before reading from the writer |
| `reads.max_galera_lag_ms` | 1000 | A Galera node further behind gets no reads |
| `reads.galera_unconfirmed` | `check` | `check` or `writer` |
| `reads.galera_sync_wait_timeout_ms` | 300 | The Galera sync-wait check's limit |
| `reads.replica_position_poll_ms` | 250 | How often gateways poll replica positions |

To keep one table's reads on the writer, for example a table read right after other sessions write it:

```sql test
CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR(20));
ALTER TABLE orders SPRIG SET (reads.routing = 'writer');
SELECT SETTING_NAME, VALUE, SOURCE_SCOPE
FROM information_schema.SPRIG_SETTINGS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders'
  AND SETTING_NAME = 'reads.routing';
```

```text output
SETTING_NAME	VALUE	SOURCE_SCOPE
reads.routing	writer	object
```

`ALTER TENANT SPRIG SET (reads.routing = 'writer')` does the same for the whole tenant.

See [Read routing](../performance/read-routing.md).

## Not supported yet

- Changing a cluster's topology in place. Register the new cluster, then drain the old one.
- Moving a tenant's home DC by changing a setting. Use `sprigctl tenant move`, or let the migration loop do it.

## Related

- [How SprigDB works](overview.md)
- [Clusters](../operate/clusters.md)
- [Data centres](../operate/data-centres.md)
- [Failover](../operate/failover.md)
- [Read routing](../performance/read-routing.md)
