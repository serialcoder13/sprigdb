---
title: Sizing
audience: [operator]
sidebar:
  order: 11
---

# Sizing

How many gateways, controllers and workers to run, how to size MariaDB, and how many tenants fit on a cluster. SprigDB doesn't publish fixed capacity numbers: they depend on your statements, your data and your hardware. This page gives the limits SprigDB enforces, the defaults its deployments ship with, and the rules for working out the rest. Measure with your own workload, using the metrics in [Monitoring](monitoring.md), before you commit to a size.

## A starting point

SprigDB's three-VM layout is sized for VMs of 4 vCPU and 16 GB each, with every role on every VM:

| Process | Memory |
|---|---|
| MariaDB | 8 GB, with a 5 GB buffer pool |
| Gateway | 3 GB |
| Controller | 1 GB |
| Worker | 1 GB |
| etcd | 512 MB |
| Agent | 256 MB |

The Helm chart's defaults for one data centre are 3 etcd members, 3 controllers, 3 to 30 gateways (autoscaled at 60% CPU), 2 workers and 2 operator replicas, with MariaDB clusters of 3 nodes and 500 GB per node.

## Gateways

Gateways are stateless and hold no data, so you scale them by adding more. Each one runs every client listener, logs users in, translates, routes and merges results; their work grows with statements per second, not with data size.

- **Run at least two per data centre**, behind a load balancer that checks `/readyz`, so one can restart or drain without an outage. The Helm chart starts with three.
- **Scale on CPU.** Statement handling is CPU-bound. The chart's autoscaler targets 60% CPU, leaving room for a gateway's share when another drains.
- **Watch the overhead.** `sprig_gateway_overhead_seconds` is the time a statement spends in the gateway, not counting MariaDB. If its p99 climbs while MariaDB's time doesn't, add gateways.

Per-gateway limits:

| Setting | Default | What it limits |
|---|---|---|
| `gateway.max_connections` | `20000` | Client connections per gateway |
| `gateway.memory_limit_mb` | `0` (75% of the container's memory limit) | The gateway's memory budget |
| `catalog.cache_max_mb` | `512` | Memory for cached tenant catalogs |
| `catalog.cache_max_tenants` | `20000` | Tenants whose catalogs are cached |
| `limits.tenant_connections` | `2000` | Connections per tenant |
| `limits.tenant_active_statements` | `256` | Statements one tenant runs at once |
| `limits.tenant_memory_mb` | `1024` | Session memory per tenant |
| `limits.user_active_statements` | `64` | Statements one user runs at once |

A statement that waits longer than `limits.queue_timeout_ms` (5 seconds) for admission is refused. When many tenants are active on each gateway, raise `catalog.cache_max_tenants` and `catalog.cache_max_mb` together, and give the gateway the memory for them. See [Limits and timeouts](../performance/limits-and-timeouts.md).

### Connections to MariaDB

Each gateway keeps its own pools of connections to each MariaDB node: at most `backend.max_conns_per_account_node` (32) per backend account and node, and at most `backend.max_conns_per_node` (256) per node across all its pools. In the default `transaction` pool mode, a client connection holds a backend connection only for the duration of a transaction, so thousands of client connections share a few hundred backend ones.

Size each MariaDB node's `max_connections` for the worst case:

```text
max_connections >= gateways x backend.max_conns_per_node + controllers + workers + headroom
```

For example, 6 gateways at the default 256 need at least 1,536, plus a few dozen for controllers, workers, replication and your own sessions. If that is more than the node can hold, lower `backend.max_conns_per_node` at cluster scope rather than leaving the pools to fail with backend errors.

## Controllers

Each data centre has one leading controller. It runs the DC's loops: probing clusters, placement, the balancer, failover and the job scheduler. The other controllers stand by, take over within the leader lease, and serve write permits to gateways and agents.

- **Run three per data centre.** Two survive one failure; three survive one failure during an upgrade. Adding more doesn't add throughput, because only the leader does the work.
- **Give each 1 GB** to start. Memory grows with the number of tenants and clusters in the DC.
- **Keep them close to etcd.** Every lease and permit renewal is an etcd round trip.

With a single controller, restarting it fences writes for a few seconds (39501), because nothing else renews the gateways' permits.

## Workers

Workers run background jobs: moving, splitting and merging shards, building and validating indexes, search indexing, asynchronous triggers and schedules, sinks and the change feed's consumers.

- **Run at least two per data centre**, so jobs continue while one restarts.
- **Add workers** when jobs queue (`sprig_controller_jobs` with `state="queued"` stays high) or when search indexing and triggers fall behind the change feed.
- Data movement is bounded by settings, not by worker count: `balancer.max_concurrent_moves_per_dc` (4 moves at once per DC), `mover.max_copies_per_cluster` (2 copies reading or writing one cluster) and `mover.copy_max_rows_per_sec` (50,000 rows per second per job). The mover also pauses copying when a cluster is busy (`mover.throttle_threads_running`, 64), its p99 rises 20% over its baseline, or a replica lags 2 seconds.

## etcd

etcd holds SprigDB's metadata: cluster records, shard maps, leases, settings and jobs. It stays small, but it is on the path of every lease renewal.

- **Three members per data centre** (five for more failure tolerance), on SSDs, in different zones. Writes continue while a majority is up.
- **512 MB of memory** per member is enough for the layouts SprigDB ships.
- **Compact and defragment.** Keep one hour of revisions (`--auto-compaction-mode=periodic --auto-compaction-retention=1h`) and defragment daily; every deployment SprigDB ships does both. Without them the database grows with every write. The `EtcdDbLarge` alert fires above 512 MiB.

## MariaDB

### Memory

On clusters SprigDB creates, a node starts with:

| Setting | Value |
|---|---|
| `innodb_buffer_pool_size` | 60% of the node's memory |
| `innodb_log_file_size` | a quarter of the buffer pool, at least 1 GB |
| `max_allowed_packet` | 65 MiB (the gateway's 64 MiB limit plus 1 MiB for its rewrites) |

The remaining memory goes to connections (each busy connection can use a few megabytes for sort buffers and temporary tables), the table caches below, and the operating system. If MariaDB is killed for running out of memory, lower `max_connections` or the buffer pool before you raise anything else. Turning down caches you don't use helps: SprigDB stores everything in InnoDB, so `key_buffer_size` can be 16 MB and `aria_pagecache_buffer_size` 32 to 64 MB.

### Disk

Plan for the data, its indexes, and seven days of binlog (`binlog_expire_logs_seconds = 604800`), which the change feed and the mover read from. Leave room for a full copy of the data when a replica is rebuilt or a backup is staged. The balancer treats a cluster above `balancer.storage_hard_pct` (90%) of its registered storage as full, and moves shards to keep clusters at or below `balancer.target_pct` (70%).

Register each cluster with its real capacity (`--memory-gb` and `--storage-gb` on `sprigctl cluster register`), because placement and the balancer compare clusters by it. See [Clusters](clusters.md).

### Table caches

Every shard is a MariaDB schema, and every tenant also has a catalog schema of its own, so a cluster with many tenants holds many tables. MariaDB's defaults for its table caches are far too small for that, and a cache that is too small makes queries reopen tables, which shows as a rising `Opened_tables` and `table_open_cache_misses`.

Size them from what the cluster holds:

| Variable | Rule |
|---|---|
| `table_definition_cache` | 1.1 x all tables on the server, + 400 (at least 400, at most 2,097,152) |
| `table_open_cache` | 1.2 x the tables used in the last day, + 8 x peak `Threads_running` (at least 2,000, at most 1,048,576) |
| `innodb_open_files` | the same as `table_open_cache`, at least 400 |
| `open_files_limit` | `table_open_cache` + `innodb_open_files` + 2 x `max_connections` + 1,000 |
| `stored_program_cache` | twice the most routines in one schema, rounded up to a power of two (256 to 4,096) |

Budget about 12 KiB per table definition and 8 KiB per open table: 20 KiB per table that is both defined and open. 100,000 tables therefore need about 2 GB for the caches alone. Keep the caches within about a tenth of the server's memory; if they don't fit, the cluster holds too many tables, and new tenants belong on another cluster.

`table_definition_cache`, `table_open_cache` and `stored_program_cache` change live with `SET GLOBAL`. Setting `table_open_cache` closes every open table, so change it rarely and only by a worthwhile amount (20% or more). `innodb_open_files` and `open_files_limit` are read-only on 11.8 and 12.3: set them in the server's configuration file and restart. SprigDB's own nodes start with `open_files_limit = 524288` and `table_open_cache_instances = 8`.

## Tenants per cluster

There is no fixed number of tenants per cluster. What fills a cluster first is usually its table count, then its storage, then its load. Each tenant brings:

- a catalog schema, with a few dozen tables of its own;
- one schema per shard of each table group, holding its tables, collections, key-value namespaces and the side tables of its queues, streams and sinks.

So a tenant with one database and a handful of tables starts with well under a hundred MariaDB tables, and a large tenant split across many shards has many more.

Placement enforces these caps when it puts new shards on a cluster:

| Setting | Default | Scopes | Meaning |
|---|---|---|---|
| `placement.max_tables_per_cluster` | `75000` | cluster, dc, global | Tables per cluster, side tables included |
| `placement.max_shard_schemas_per_cluster` | `15000` | cluster, dc, global | Shard schemas per cluster |
| `placement.max_schema_creates_per_minute` | `300` | cluster, dc, global | New shard schemas created on one cluster per minute |
| `balancer.storage_hard_pct` | `90` | cluster, dc, global | Storage ceiling |
| `balancer.high_pct` | `80` | cluster, dc, global | Above this, a cluster is hot and sheds load |
| `balancer.target_pct` | `70` | cluster, dc, global | Moves keep their target at or below this |
| `balancer.new_tenant_load` | `1` | dc, global | The storage, in GB, placement assumes for a new tenant |

A cluster at a cap gets no new shards; the balancer moves load away when it is hot.

To fit more tenants on a cluster, raise `placement.max_tables_per_cluster` for it and give MariaDB the memory the table caches need at that count. To fit fewer, lower it. Either way, count the tables MariaDB really holds (`SELECT COUNT(*) FROM information_schema.TABLES` on a node) against the caps and the cache budget above.

## Data centres

Each data centre runs its own etcd, controllers, gateways and workers, sized as above for the traffic it serves. The global etcd tier needs one member per data centre, three or more. Inter-DC latency adds to anything that crosses DCs: a client reading a tenant whose home is in another DC, a DC move, and the global tier's writes. See [Data centres](data-centres.md).

## Related

- [Monitoring](monitoring.md)
- [Clusters](clusters.md)
- [Limits and timeouts](../performance/limits-and-timeouts.md)
- [Deploy with Docker Compose](deploy-compose.md)
- [Deploy on Kubernetes](deploy-kubernetes.md)
