---
title: Failover
audience: [operator]
sidebar:
  order: 7
---

# Failover

SprigDB fails over at two levels. **Cluster failover** replaces a lost writer inside one MariaDB cluster, run by the DC leader. **DC failover** moves the tenants of a lost data centre to their standbys in another one, decided by the global leader. Both promote only within each tenant's recovery point objective (RPO), and both fence the old writer first, so two writers never accept the same tenant's writes.

This page explains leases and fencing, what happens for each topology, how DC failover is decided, how to fail over by hand with `sprigctl tenant failover`, standbys, recovery, and what clients see.

## Leases and fencing

Write authority comes from leases in etcd. A process that can't renew its lease stops writing on its own; nothing has to reach it.

| Lease | Held by | TTL setting | Losing it means |
|---|---|---|---|
| **DC lease** (`dc-leases/<dc>`) | The DC's controllers | `failover.dc_lease_seconds` (10) | The DC stops granting permits. The global leader notices and starts the DC failover decision. |
| **Permit** (a sub-lease of the DC lease) | Every gateway, agent and worker, renewed every second from the DC's controllers | Granted for up to half the DC lease's TTL | A gateway fails writes for tenants homed in its DC with 39501 `ER_SPRIG_DC_FENCED` (reads continue). An agent on a writer sets `read_only = ON` and kills tenant sessions. A worker stops cutovers and promotions. |
| **Writer lease** (`leases/writer/<cluster>`) | The node agent on the cluster's writer | `failover.writer_lease_seconds` (10) | The agent sets its node `read_only`. The controller may promote another node only once this lease is gone. |

Permits end `failover.fence_lead_ms` (3000) **before** etcd could expire the DC lease, measured on the holder's own clock read before it asked, so delays only end a permit earlier. A watchdog thread in every holder fences directly when the process stalls for `failover.watchdog_ms` (1000), and aborts the process if it is still stuck after twice that. A process that was paused (by a debugger, a VM freeze) fences when it resumes.

**Self-fencing on VMs.** The node agent (`sprigdb agent` with `[agent] cluster` set) checks every 200 ms that it holds a valid DC permit and the writer lease and that its node doesn't replicate. Only then is `read_only = OFF`. MariaDB boots with `read_only = ON`, so a node is writable only while its agent says so. See [Deploying on VMs](deploy-vms.md).

`failover.self_fence = false` turns self-fencing off for a DC, and with it automatic DC failover, because the global leader can no longer prove the lost DC stopped writing.

## Cluster failover

The DC leader's `cluster_failover` loop checks every primary-and-replicas and Galera cluster every 500 ms.

### Primary and replicas

1. **Detect.** The writer fails probes for `failover.cluster_detect_seconds` (5).
2. **Damp.** At least `failover.cluster_detect_sources` (2) independent sources must agree the writer is down: the leader's probes (refused connections, not timeouts) and the writer's agent (lease handed back, or mysqld down). A probe timeout alone is "slow", never "down", so a paused process isn't failed over. Within `failover.min_interval_minutes` (30) of the last automatic failover, only a hard failure proceeds: writer lease gone **and** the host unreachable.
3. **Fence.** The writer lease must be gone (its agent fenced and released it, or its session ended), then the leader waits `failover.clock_drift_margin_ms` (500).
4. **Mode.** With `failover.mode = manual`, stop here: the failover is blocked with 39512 `ER_SPRIG_FAILOVER_BLOCKED` and an alert.
5. **Choose.** Every replica whose agent answers applies the relay log it already received. The best replica is the one whose received position contains every other's. If none does, two diverge, or the best can't finish applying, stop with 39512.
6. **Estimate loss.** At most 1 s when semi-synchronous replication (`AFTER_SYNC`) was active in the last per-second sample; otherwise the best replica's last measured lag. Over `failover.rpo_seconds` (5): stop with 39512.
7. **Promote.** The writer epoch rises; the leader claims the writer lease for the chosen node, resets its replication and turns semi-sync on; its agent takes the lease over and clears `read_only`. The other replicas are repointed to it with GTID positions.
8. **Publish.** Gateways see the new writer and epoch and route writes to it.

A cluster without node agents can't hand its writer lease back, so the controller waits for the lease's TTL to run out before it promotes.

### Galera

Every node can write. The DC leader probes each node every second and keeps the set of nodes that are `Primary`, `Synced` and ready. Each shard's writer is picked from that set; when a writer leaves it, the shard moves to another node at once. No fence is needed: nodes outside the primary component refuse queries.

If no node is in a primary component for `failover.galera_quorum_wait_seconds` (30), quorum is lost. In `auto` mode, the leader bootstraps the reachable node with the highest `wsrep_last_committed`, but only if that is at least the highest value seen in the last primary component and every other node is confirmed down (no agent registration and unreachable). Otherwise it alerts and waits. A node that was out comes back into the writer set only after `cluster.galera_writer_rejoin_seconds` (120) of good health.

### Single node

A restart isn't a failover: writes fail with 39100 `ER_SPRIG_CLUSTER_UNAVAILABLE` until the node is back. To survive the loss of the node, give the tenants a standby in another DC and let DC failover promote them.

### External and Kubernetes clusters

When someone else fails the cluster over (an external cluster with `[declared] failover = "provider"`, or mariadb-operator on Kubernetes), SprigDB follows. The DC leader publishes a new writer, with a new epoch, only when exactly one node is writable (`read_only = OFF`, replicating from nobody), the old writer is unreachable or now replicates from the new one, and the observation has held for `failover.observe_settle_ms` (2000). Two writable nodes publish nothing: the cluster is marked `diverged` and an alert fires. The declared `rpo_seconds` stands in for SprigDB's own loss estimate.

### A former writer that comes back

When the old primary answers again:

- if nothing diverged, it rejoins as a replica, still `read_only`;
- if it logged transactions after the position the new primary had applied, those transactions are **reported, never merged**. The node stays out and an alert fires. In `auto` mode, an extract job saves the diverged transactions, and once the extract is verified a rebuild job reseeds the node from a healthy replica.

### A former writer that diverged

Run the same steps by hand when a job failed, or in `manual` mode:

```sh
sprigctl cluster extract fra1-c1 n1        # the diverged transactions' row images, sealed per tenant
sprigctl job show <job-id>
sprigctl cluster extract-open --key-file /etc/sprigdb/keys/extract.keys n1-t42.extract   # read one as JSON lines
sprigctl cluster rebuild fra1-c1 n1 [--from n3]
```

- `extract` reads the node's binlog for the diverged GTIDs and writes, per tenant, the schema, table, kind, before and after images and equivalent SQL of every row change. Each tenant's file is sealed (AES-256-GCM) under a key derived for that tenant from the workers' `[recovery] extract_key_file`, and stored under `[recovery] extract_dir`. Extracts aren't deleted automatically; remove them from that directory when you no longer need them.
- `extract-open` decrypts one file with the key file and prints it.
- `rebuild` streams a `mariadb-backup` of a healthy replica (never the writer; the healthiest by default, or `--from`) to the node's agent, restores it, and attaches the node to the writer as a replica. The rate is capped by `recovery.rebuild_max_mb_per_second` (200), one rebuild per cluster at a time (`recovery.max_rebuilds_per_cluster`). Both agents need `[agent] internal_addr`. A diverged node is rebuilt only after a verified extract; `--no-extract` rebuilds anyway and keeps its old data directory.

`sprigctl tenant failover <tenant> --errant` shows the diverged-transaction reports of a tenant's DC failovers.

## DC failover

### The automatic decision

The global leader runs it when a DC's lease vanishes:

1. The DC becomes `suspect` and a failover record is created.
2. Wait `failover.clock_drift_margin_ms`. If the DC took its lease back, close the record.
3. Count the sites: the other active DCs and witnesses that hold valid DC leases. If the global voters span fewer than `failover.min_sites` (3) sites, or fewer than `min_sites − 1` are available, the record becomes `manual_only` and an alert fires.
4. Every available site observes the lost DC `failover.observer_probes` (3) times, `failover.observer_probe_interval_ms` (1000) apart: its controllers, and a TCP connect and MySQL handshake to each of its writers. The verdict is **alive** (a controller reports valid permits), **down** (every reachable writer self-fences, or the DC's controller reported it fenced), or **unsafe** (a writer is reachable and not proven fenced).
5. At least `min_sites − 1` "down" verdicts and no "alive" are needed. Otherwise retry until `failover.decision_timeout_seconds` (120), then `manual_only`.
6. The DC becomes `failed`. Each tenant homed there is promoted to its standby, at most `failover.max_parallel_promotions` (16) at a time, unless:
   - its `failover.mode` is `manual`;
   - no standby DC holds all of it;
   - the standby's reported lag is over its `failover.rpo_seconds`;
   - a cluster holding it is listed unsafe.

   Skipped tenants stay fenced in the lost DC and wait for it to return, or for an operator.

With `failover.mode = approve` at global scope, each promotion waits in the approvals inbox. An approved one runs only if the DC is still down, the sites still agree, and the standby is still within the RPO. See [Approvals](../concepts/approvals.md).

**Two sites.** Automatic DC failover needs three sites, so two DCs need a third: a `witness` DC (`sprigctl dc register <id> --kind witness --global-voter`) that runs only a global etcd voter and a controller. Without one, DC failover is manual.

### Promotion

Promoting tenant T from DC X to DC Y:

1. One global transaction moves T's home to Y and raises its epoch. This is the commit point.
2. T's standby stops, and the applied source position is recorded.
3. Y's shard maps for T become `home`, with a higher epoch.
4. Write grants are restored on the promoted schemas.
5. T is `active` in Y.

Every step is idempotent, and a promotion interrupted after step 1 is finished by the next leader. Gateways see the new epoch and route to Y; one still routing to X meets a fenced writer.

### Manual failover

```sh
sprigctl --endpoint https://ctl.ams1.internal:7480 --dry-run tenant failover acme --to ams1
sprigctl --endpoint https://ctl.ams1.internal:7480 tenant failover acme --to ams1
sprigctl tenant failover --all-from fra1 --to ams1            # every tenant homed in fra1
sprigctl tenant failover acme --to ams1 --accept-data-loss
sprigctl tenant failover acme --errant                         # past failovers and diverged transactions
```

| Flag | Meaning |
|---|---|
| `--to <dc>` | The new home: a DC that holds the tenant's standby |
| `--all-from <dc>` | Every tenant homed in that DC, instead of one |
| `--accept-data-loss` | Accept losing what the standby hadn't applied. Required whenever loss is possible. |
| `--old-home-fenced` | Vouch that the old home is fenced although it still holds its DC lease. Audited. |
| `--errant` | Show the tenant's failovers and their diverged-transaction reports |
| `--dry-run` | The checks and the standby's lag, without acting |

Point `--endpoint` at a controller of the new home: the old one may be gone. The output:

```text
TENANT  MOVE         STANDBY LAG  OLD HOME FENCED BY  OUTCOME   EPOCH  REASON
acme    fra1 -> ams1 120 ms       dc lease            promoted  8
```

The failover is refused with 39505 `ER_SPRIG_FAILOVER_REFUSED` when:

- the target is the home, isn't active, or the tenant is pinned to its home;
- the target doesn't hold a standby of the whole tenant;
- the old home still holds its DC lease (it is healthy: use `sprigctl tenant move` instead), unless `--old-home-fenced`;
- loss is possible and `--accept-data-loss` wasn't given.

With two sites this is the only way to fail a DC over.

## Standbys

A standby is a copy of a tenant in another DC, kept current from the home cluster's binlog. DC failover can only promote a tenant that has one.

**Automatic job standbys.** In single-etcd mode, the home DC's leader keeps `failover.standby_dcs` (1) standby DCs per tenant, checked every 10 s. Each is a continuous mover job that copies the tenant to a cluster in the standby DC. The standby DC is an active, non-witness DC the tenant's residency allows, preferring another `region` tag, then the lowest latency from home. `failover.standby_mode` controls the loop: `on`, `approve`, `recommend` or `off`. `failover.standby_dcs = 0` disables DC failover for the tenant. Every standby holds a worker slot while it streams. Tenants with table groups get no job standby yet.

**Replica-cluster standbys.** Set up a cluster in the standby DC that replicates the home cluster with MariaDB replication, register it, and record it as the tenant's standby:

```sh
sprigctl tenant standby acme --dc ams1 --cluster ams1-c1
```

Every tenant on such a cluster moves with it: promoting one stops the replication for all of them, and the others' standby lag then grows until they are promoted too or set up again. Use this form in deployments with separate etcd tiers.

## Recovery

A DC that comes back stays fenced for every tenant that moved away:

```text
fenced ──► checking ──┬─► catching_up ──┐
                      │                 ├──► warming ──► stable
                      └─► rebuilding ───┘   (traffic in steps)
```

- Its copies of moved tenants are fenced (write privileges revoked, sessions killed) before it grants permits again. A write that reaches one gets 39530 `ER_SPRIG_RECOVERY_FENCED`.
- Diverged transactions are reported in the failover record (`--errant`), never merged.
- A node isn't rebuilt for being far behind; there is no size threshold. Only a node holding diverged transactions is rebuilt, after its extract; see [A former writer that diverged](#a-former-writer-that-diverged).
- Traffic returns in steps of `recovery.traffic_steps_pct` (`10,25,50,100`), each lasting `recovery.step_minutes` (5). The steps advance on time alone; replica lag and latency don't pause them. New connections are admitted at `recovery.admission_connects_per_second` (200) per gateway at the first step, a rate raised in proportion at each later step and lifted at the last; refused ones get 39531 `ER_SPRIG_ADMISSION_BACKOFF` with a 1–5 s delay.
- A recovered cluster receives shards from the balancer again only after `recovery.stability_minutes` (60).

Tenants don't move back on their own because their old home returned. Automatic DC moves may later bring them back if their traffic says so; see [Data centres](data-centres.md).

## What clients see

| Event | Error | Retry | What to do |
|---|---|---|---|
| A cluster has no writer, during failover | 39100 `ER_SPRIG_CLUSTER_UNAVAILABLE` | yes | Retry with backoff; a primary-and-replicas failover usually completes in seconds |
| The home DC lost its lease | 39501 `ER_SPRIG_DC_FENCED` | yes | Reads continue. Writes resume when the DC renews its lease, or after promotion elsewhere |
| A node fenced by its agent | MariaDB 1290 (read-only) | yes | Retry; the agent clears it once it holds its permit and the writer lease |
| A write reached a fenced copy after recovery | 39530 `ER_SPRIG_RECOVERY_FENCED` | yes | Retry; the gateway re-routes |
| A returning DC admits connections in steps | 39531 `ER_SPRIG_ADMISSION_BACKOFF` | yes | Retry after the delay in the message |
| A tenant moved DC under a session with temporary tables, locks or XA | 39502 `ER_SPRIG_SESSION_MOVED` | yes | Reconnect |

Sessions connected to the old writer are killed when it is fenced, so drivers see a dropped connection. Use a pool that reconnects, and retry transactions.

## Watching failovers

```sh
sprigctl cluster show fra1-c1      # health, nodes and the current writer
sprigctl dc list                   # suspect, failed and returning DCs
sprigctl tenant failover acme --errant
```

The metrics and alerts for leases, failovers and fencing are listed in [Monitoring](monitoring.md) and the [metrics reference](../reference/metrics.md).

## Settings

| Setting | Default | Scopes | Meaning |
|---|---|---|---|
| `failover.mode` | `auto` | tenant, cluster, dc, global | `auto`; `approve` (global scope only: DC promotions wait for approval); `manual` (decide and alert, but never promote) |
| `failover.rpo_seconds` | `5` | tenant, dc, global | Most data loss accepted automatically |
| `failover.cluster_detect_seconds` | `5` | cluster, dc, global | Probe failures before a primary is lost |
| `failover.cluster_detect_sources` | `2` | cluster, dc, global | Sources that must agree a writer is down |
| `failover.min_interval_minutes` | `30` | cluster, dc, global | Within it, only hard failures fail over again |
| `failover.writer_lease_seconds` | `10` | cluster, dc, global | Writer lease TTL |
| `failover.dc_lease_seconds` | `10` | dc, global | DC lease TTL |
| `failover.fence_lead_ms` | `3000` | dc, global | Permits end this long before the lease could expire |
| `failover.watchdog_ms` | `1000` | dc, global | A stall that triggers a direct fence |
| `failover.clock_drift_margin_ms` | `500` | global | Wait after a lease expires |
| `failover.self_fence` | `true` | dc, global | `false` also disables automatic DC failover |
| `failover.min_sites` | `3` | global | Sites needed for automatic DC failover |
| `failover.decision_timeout_seconds` | `120` | global | Then the DC failover becomes manual only |
| `failover.observer_probes`, `failover.observer_probe_interval_ms` | `3`, `1000` | global | How each site observes a lost DC |
| `failover.max_parallel_promotions` | `16` | global | Concurrent tenant promotions |
| `failover.galera_quorum_wait_seconds` | `30` | cluster, dc, global | Quorum loss before a bootstrap |
| `failover.observe_settle_ms` | `2000` | cluster, dc, global | How long an observed writer change must hold |
| `failover.standby_dcs` | `1` | tenant, dc, global | Standby DCs per tenant; `0` disables DC failover |
| `failover.standby_mode` | `on` | tenant, dc, global | Automatic standbys: `on`, `approve`, `recommend` or `off` |

All are operator-only and apply live. The full list, with bounds, is in the [settings reference](../reference/settings.md).

## Related

- [Clusters](clusters.md)
- [Data centres](data-centres.md)
- [Troubleshooting failover](../troubleshooting/failover.md)
- [Monitoring](monitoring.md)
- [Error codes](../reference/errors.md)
