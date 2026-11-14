---
title: Failover problems
audience: [operator]
sidebar:
  order: 5
---

# Failover problems

This page covers failovers that didn't happen, failovers you didn't expect, the fencing errors clients see around a failover, and former writers that need rebuilding. For how failover works, see [Failover](../operate/failover.md).

## How failover decides

SprigDB fails over at two levels, and both are built on leases, so that two writers never accept writes at the same time.

**Inside a cluster.** The data centre's leading controller probes each cluster's writer every 500 ms. The writer's node holds a **writer lease** (`failover.writer_lease_seconds`, 10 seconds); on VMs, the node's agent renews it and fences the node itself (sets `read_only` and kills sessions) if it can't. The controller fails over when:

1. the writer fails its probes for `failover.cluster_detect_seconds` (5), confirmed by `failover.cluster_detect_sources` (2) sources, such as the probes and the agent;
2. the writer lease is gone, plus `failover.clock_drift_margin_ms` (500 ms);
3. `failover.mode` isn't `manual`;
4. the expected loss of the most advanced replica is within `failover.rpo_seconds` (5), and no two replicas diverge.

It then raises the writer epoch, promotes that replica, repoints the others and records the failover.

**Between data centres.** Each data centre holds a **DC lease** (`failover.dc_lease_seconds`, 10 seconds) in the global control plane, and every gateway, agent and worker holds a short **permit** from its data centre's controllers, renewed every second. When a data centre's lease disappears, the global leader asks observers in the other sites whether it is down. It needs at least `failover.min_sites` (3) sites, and `failover.min_sites - 1` "down" verdicts with no "alive". It then promotes each tenant homed there to its standby data centre, `failover.max_parallel_promotions` (16) at a time.

## Failover didn't happen

**Symptom.** A writer or a data centre is down, clients get `ER_SPRIG_CLUSTER_UNAVAILABLE` (39100) or `ER_SPRIG_DC_FENCED` (39501), and nothing is promoted.

### Confirm

```sh
sprigctl cluster show <cluster-id>      # nodes, the writer, health, the last failover
sprigctl tenant failover acme --errant  # the tenant's failovers and their reports
```

In Studio's operator mode, the **Failovers** page lists failover records with their state and reason. The decision record says why a failover stopped:

```sh
sprigctl explain decision <id> --tenant acme
```

| Metric | Shows |
|---|---|
| `sprig_controller_cluster_failovers_total` by `topology` and `result` | Cluster failovers, and those that ended blocked |
| `sprig_controller_failovers_suppressed_total` by `level` | Failovers held back by damping |
| `sprig_controller_dc_failovers_total` by `result` | Data-centre failovers |
| `sprig_controller_dc_lease_remaining_seconds` | Time left on this data centre's lease |
| `sprig_cluster_probe_failures_total` | Failed probes by cluster, node and probe |

### Causes and fixes

| Cause | How you see it | Fix |
|---|---|---|
| `failover.mode` is `manual` | The record is blocked with `ER_SPRIG_FAILOVER_BLOCKED` (39512): "manual" | Fail over by hand (below), or set `failover.mode = auto` |
| The loss estimate is over the RPO | Blocked (39512): the most advanced replica lags more than `failover.rpo_seconds` | Decide whether to accept the loss; promote by hand if so |
| Replicas diverge | Blocked (39512): two replicas have transactions the other lacks | Compare the replicas' GTID positions and choose one; rebuild the others |
| The writer isn't confirmed down | Probes fail but the agent or the lease says it is alive | Check the network between the controller and the node. The writer may be alive but unreachable from the controller only |
| The writer lease still exists | The old writer's agent still renews it | The node is still a writer as far as SprigDB knows; check the agent on that node |
| Damping | `sprig_controller_failovers_suppressed_total` rises: a failover happened within `failover.min_interval_minutes` (30) and only hard failures fail over again | Wait, or fail over by hand |
| Too few sites for a data-centre failover | The record ends `manual_only`: fewer than `failover.min_sites` sites, or no decision within `failover.decision_timeout_seconds` (120) | Fail over by hand. With two sites, data-centre failover is always manual |
| No standby | The tenant is skipped: no standby data centre holds every unit, or `failover.standby_dcs` is 0 | Set up standbys (`failover.standby_mode`, or `sprigctl tenant standby`) |
| The standby lags | The tenant is skipped: the standby's lag is over its `failover.rpo_seconds` | Accept the loss by hand, or wait for the old home |
| Self-fencing is off | `failover.self_fence = false` for the data centre | Such a data centre never fails over automatically; fail over by hand |

### Failing a tenant over by hand

Point `sprigctl` at a controller of the **new** home; the old one may be gone. Always run the dry run first, which shows the checks and the expected lag:

```sh
sprigctl --endpoint http://ctl.ams1.example.internal:7480 --dry-run tenant failover acme --to ams1
sprigctl --endpoint http://ctl.ams1.example.internal:7480 tenant failover acme --to ams1
```

| Flag | Meaning |
|---|---|
| `--to <dc>` | The new home: a data centre that holds the tenant's standby |
| `--all-from <dc>` | Every tenant homed in that data centre |
| `--accept-data-loss` | Required when the standby hadn't applied everything |
| `--old-home-fenced` | Vouch that the old home is fenced although it still holds its lease. Audited; use only when you have stopped it yourself |

The command fails with `ER_SPRIG_FAILOVER_REFUSED` (39505) when the old home still holds its DC lease (it is healthy: move the tenant with `sprigctl tenant move` instead), the target isn't allowed or holds no standby, or possible loss wasn't accepted. The message says which.

## Failover happened unexpectedly

**Symptom.** The writer changed, or a tenant moved to its standby data centre, although you saw no outage.

**Find out why.** Every failover is recorded with its evidence. Start with Studio's Failovers page or `sprigctl tenant failover acme --errant`, then `sprigctl explain decision <id>` for the record.

| Cause | Evidence | Fix |
|---|---|---|
| A network partition between the controller and the writer's node | Probe failures from the controller only; the node's own MariaDB log shows nothing | Fix the network path. Raise `failover.cluster_detect_seconds` if short blips are normal there |
| The writer node fenced itself | `sprig_agent_fenced` was 1 for the cluster; `sprig_agent_writer_lease_remaining_seconds` reached 0 | The agent couldn't renew its lease: check the agent's connectivity to the controllers and etcd, and whether the host was paused |
| A long pause of the host | A VM freeze or a stopped process outlived the lease | Find the cause of the pause; leases can't outlast it safely |
| A gateway's runtime stalled | `sprig_gateway_runtime_stalls_total` rose; the gateway fenced itself after `failover.watchdog_ms` (1,000 ms) | Check the gateway's CPU and memory; this fences one gateway, not the tenant |
| Someone promoted a replica outside SprigDB | `sprig_controller_observed_writer_changes_total` rose | SprigDB followed the change after `failover.observe_settle_ms`. Make changes through SprigDB |
| A data centre lost its DC lease | `sprig_controller_dc_failovers_total` rose; the data centre's controllers couldn't reach the global tier | Check the links between data centres. A short loss usually ends with the lease taken back and no failover |

After an unexpected failover, check `sprig_controller_failover_loss_seconds` for how much, if anything, was lost, and see [Replica rebuilds](#replica-rebuilds) for the old writer.

## Fencing errors

Fencing stops a copy that may no longer be the writer from accepting writes. Clients see these errors:

| Error | Meaning | Retry? | What to do |
|---|---|---|---|
| `ER_SPRIG_DC_FENCED` (39501) | The tenant's home data centre holds no valid lease or permit; writes are refused, reads continue | Yes, once the tenant is re-routed | Retry with backoff. If the message says the permit lapsed *while the statement ran*, the outcome is unknown: check before repeating a non-idempotent write |
| `ER_SPRIG_RECOVERY_FENCED` (39530) | The write reached a copy fenced after a failover, while the gateway finds the tenant's new home | Yes | Retry; the next attempt goes to the new home |
| `ER_SPRIG_ADMISSION_BACKOFF` (39531) | The data centre is returning from an outage and admits new connections in steps | Yes, after the delay in the message (1 to 5 seconds) | Retry with the delay. Over HTTP it is a 503 with `Retry-After` |
| `ER_SPRIG_CLUSTER_FENCE_FAILED` (39103) | A shard fence for a move or failover couldn't complete in time and was rolled back | Yes | Nothing; the job retries. Long transactions on the shard delay fences |
| `ER_SPRIG_CLUSTER_UNAVAILABLE` (39100) | The cluster has no writer for the shard at the moment | Yes | Retry with backoff; failover usually restores a writer within seconds |
| 1205, lock wait timeout | A statement waited for a promotion's hold longer than allowed | Yes | Retry |

MongoDB clients see `ER_SPRIG_DC_FENCED` and `ER_SPRIG_RECOVERY_FENCED` as MongoDB's own `NotWritablePrimary` (10107) with the `RetryableWriteError` label, so drivers with retryable writes retry on their own.

### Confirm

| Metric | Shows |
|---|---|
| `sprig_gateway_fenced` | 1 while the gateway refuses writes |
| `sprig_gateway_permit_remaining_seconds` | Time left on the gateway's permit |
| `sprig_gateway_fenced_writes_total`, `sprig_gateway_recovery_fenced_writes_total` | Writes refused for each reason |
| `sprig_gateway_fence_errors_total` by `outcome` | Fencing errors and whether the statement was retried or lost |
| `sprig_gateway_admission_backoffs_total`, `sprig_gateway_admission_rate` | Connections turned away while the data centre returns, and the current rate |

### A gateway refuses writes but the cluster is fine

The gateway's permit lapsed: it couldn't renew it from its data centre's controllers. Check that the controllers run and that the gateway reaches their internal address. A gateway that never saw a controller serving permits doesn't enforce them, so this only happens to gateways that had a permit and lost it.

### A returning data centre is slow to take traffic

After an outage, a returning data centre stays fenced for tenants that failed over, reports their diverged transactions, and then takes traffic back in steps: `recovery.traffic_steps_pct` (`10,25,50,100`), each for `recovery.step_minutes` (5), admitting `recovery.admission_connects_per_second` (200) new connections per gateway at the first step and proportionally more at each later step. The steps advance on time alone, so a full return takes about three steps' time (15 minutes with the defaults). This is deliberate; clients get 39531 and retry. To return faster, lower `recovery.step_minutes` or use fewer steps.

## Split-brain protection

SprigDB prevents two writers with these rules. When you troubleshoot, don't work around them:

- **A writer needs a lease.** A node accepts writes only while it holds the writer lease, and its agent fences it `failover.fence_lead_ms` (3,000 ms) before the lease could expire. A new writer is promoted only after the old lease is gone plus the clock-drift margin.
- **Epochs.** Every promotion raises the writer epoch; work carrying an older epoch is refused.
- **Gateways need a permit.** A gateway refuses writes of tenants homed in its data centre without a valid permit, and doesn't acknowledge a write that finished after its permit lapsed.
- **Observers must agree.** A data centre is failed over only when enough other sites see it down.
- **A returning copy stays fenced.** A former writer or data centre that comes back doesn't take writes again on its own. Its write privileges on the moved tenant's schemas stay revoked until it is rebuilt or the tenant moves back.

`--old-home-fenced` in a manual failover overrides the last two checks on your word. Use it only after you have stopped the old home yourself, for example by powering it off.

## Replica rebuilds

**Symptom.** After a failover, the former writer doesn't rejoin its cluster as a replica.

**Cause.** When a former writer returns, SprigDB compares its binlog with the new writer:

- If nothing diverged, it rejoins as a replica, with `read_only` on.
- If it has transactions the new writer never received (it accepted writes that weren't replicated before it failed), it is kept out of the cluster, the transactions are reported in the failover record, and an alert is raised. With `failover.mode = auto`, SprigDB then extracts the diverged transactions and rebuilds the node from a healthy replica.

A node is never rebuilt just because it is far behind; one with nothing diverged replays the binlog however long that takes.

**Confirm.** `sprigctl cluster show <id>` shows the node's state. `sprig_controller_diverged_writers_total` and `sprig_controller_diverged_transactions_total` count what was found.

**Fix by hand.**

1. Extract the diverged transactions, as sealed row images, to the workers' extract store:

   ```sh
   sprigctl cluster extract fra1-c3 node-2
   ```

2. Read them to decide what to replay into the new writer. Reading needs the workers' extract key file (`[recovery] extract_key_file` in their `sprigdb.toml`):

   ```sh
   sprigctl cluster extract-open <file>.extract --key-file <extract-key-file>
   ```

   Extracts aren't deleted automatically. Remove them from the workers' `[recovery] extract_dir` when you no longer need them.

3. Rebuild the node from a replica's backup and attach it to the writer:

   ```sh
   sprigctl cluster rebuild fra1-c3 node-2 --from node-3
   ```

   `--no-extract` rebuilds without a verified extract; the node's old data directory is kept.

Rebuilds stream at up to `recovery.rebuild_max_mb_per_second` (200), with at most `recovery.max_rebuilds_per_cluster` (1) at a time. A recovered cluster receives shards from the balancer again only after it has been healthy for `recovery.stability_minutes` (60).

On Kubernetes, the operator repairs replicas itself; `sprig_operator_replica_repairs_total` by `result` shows repairs and rebuilds.

## Related

- [Failover](../operate/failover.md)
- [Data centres](../operate/data-centres.md)
- [Clusters](../operate/clusters.md)
- [Monitoring](../operate/monitoring.md)
- [Reading SprigDB errors](reading-errors.md)
