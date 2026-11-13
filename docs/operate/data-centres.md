---
title: Data centres
audience: [operator]
sidebar:
  order: 6
---

# Data centres

A data centre (DC) is a site with its own gateways, controllers, workers, MariaDB clusters and etcd. Every tenant has one **home DC**, where its data is written. SprigDB measures the latency between DCs, places new tenants in the least busy DC their residency allows, moves tenants towards their traffic, and fails tenants over to another DC when theirs is lost.

This page covers the two etcd tiers, registering and draining DCs, tenant homes, moving tenants between DCs, and latency.

## The two control-plane tiers

| Tier | Holds | Where it runs |
|---|---|---|
| **DC tier**, one etcd per DC | That DC's clusters, shard maps, leases, jobs and DC-scope settings | Three or five members inside the DC |
| **Global tier**, one etcd for the deployment | The tenant directory (each tenant's home), DC records, global settings, operators, the latency matrix, DC failover records | One voting member in each of three or more sites |

A process finds both through `[meta] dc_seeds` and `[meta] global_seeds` in `sprigdb.toml` (see [Configuration](configuration.md)). Inside a DC, everything keeps working while the global tier is unreachable, except what changes the directory: creating tenants, moving them, and DC failover.

**Single-etcd mode.** With `global_seeds` empty (or equal to `dc_seeds`), one etcd serves both tiers. Use it for one DC and for development. Automatic standbys need single-etcd mode; deployments with separate tiers use replica-cluster standbys instead (see [Failover](failover.md#standbys)).

Each DC has one **DC leader**, a controller elected through its DC tier, which runs that DC's loops: probes, the balancer, failover, moves. The deployment has one **global leader**, elected through the global tier among controllers with `[controller] global_candidate = true`, which activates DCs and decides DC failovers.

etcd access can be restricted per role with `deploy/etcd/roles.sh`, which creates one etcd user per SprigDB role, named after the role's certificate common name and limited to its key prefixes:

```sh
deploy/etcd/roles.sh dc fra1                  # a DC tier's etcd
deploy/etcd/roles.sh global fra1 ams1 mum1    # the global tier's etcd, every DC named
```

Run it as etcd's root user before `etcdctl auth enable`.

## The first DC

The first DC's controllers start with `[controller] bootstrap = true`. The first one to reach an empty global tier writes the deployment id (`system.deployment_id`, which must match `[meta] deployment_id`), the DC's record as `active`, and the tenant id counter. Later starts only verify the id.

```toml
[node]
dc = "fra1"

[meta]
dc_seeds = ["https://etcd-fra1-0:2379", "https://etcd-fra1-1:2379", "https://etcd-fra1-2:2379"]
global_seeds = ["https://etcd-g-fra1:2379", "https://etcd-g-ams1:2379", "https://etcd-g-mum1:2379"]
deployment_id = "prod-eu"

[controller]
bootstrap = true
global_candidate = true
```

## Add a DC

1. Register it. Registration writes the DC's record as `joining`:

   ```sh
   sprigctl dc register ams1 --tags country=NL,jurisdiction=EU --global-voter
   ```

   | Flag | Meaning |
   |---|---|
   | `--tags key=value` | Tags, comma-separated or repeated. Residency policies match them (`jurisdiction`, `country`), and standbys prefer a different `region`. |
   | `--kind full` or `witness` | `full` (the default) runs tenants. A `witness` runs no tenants: it holds a global etcd voter and observes during DC failover, so two DCs can have a third site. |
   | `--global-voter` | A global etcd voter runs in this DC. |

2. Start the new DC's etcd, and its controllers with `bootstrap = false`, `[node] dc = "ams1"`, its own `dc_seeds` and the shared `global_seeds`. Then start its workers and gateways.
3. Register its clusters, from a controller of the new DC. See [Clusters](clusters.md).

The global leader marks the DC `active` once its controllers hold the DC lease and latency has been measured both ways between it and every other active DC. Check with:

```sh
sprigctl dc list
```

```text
DC    KIND     STATE    EPOCH  VOTER  TAGS
dc-a  full     active   0      yes    -
ams1  witness  joining  0      no     country=NL,jurisdiction=EU
```

```sh
sprigctl dc show dc-a
```

```text
dc:            dc-a
kind:          full
state:         active
dc_epoch:      0
global_voter:  true
tags:          -
leader:        ctl-a (token 12, build 0.0.1)
```

`dc_epoch` rises each time the DC takes a new DC lease. `leader` names the current DC leader.

## DC states

| State | Meaning |
|---|---|
| `joining` | Registered; waiting for its lease and latency rows |
| `active` | Serving tenants |
| `suspect` | Its DC lease vanished; the global leader is checking whether it is down |
| `failed` | Declared down; its tenants were promoted elsewhere |
| `returning` | Back after a failure; it stays fenced for moved tenants while it catches up |
| `draining` | Takes no new tenants; still serves the ones homed there |
| `retired` | Empty and out of service |

Only `active`, `joining` and `draining` DCs grant write permits to their gateways, agents and workers. See [Failover](failover.md).

## Drain a DC

```sh
sprigctl dc drain ams1        # no new tenants
sprigctl tenant list          # find the tenants homed there
sprigctl tenant move acme --to fra1 --wait
sprigctl dc undrain ams1      # changed your mind: active again
```

A draining DC keeps serving its tenants. Automatic placement no longer chooses it, so move its tenants out. The global leader retires the DC once no tenant is homed in it.

## Tenant homes

When you create a tenant without `--cluster`, SprigDB homes it in the **least busy DC** its residency allows. Each DC's leader publishes the DC's spare storage, CPU and connections after every balancer run, and the score is

```text
score(dc) = 0.4 × storage_spare + 0.4 × cpu_spare + 0.2 × connections_spare
```

with the weights from `placement.least_busy_weights`. Only DCs that have been `active` for `recovery.stability_minutes` (60) count, unless none has, as in a new deployment. If no allowed DC has a placeable cluster, creation fails with 39525 `ER_SPRIG_NO_DC_CAPACITY`. With `--cluster`, the tenant is homed in the DC of the controller you called, on that cluster.

A child tenant's residency lies within its parent's, so its home must be one of the parent's allowed DCs. See [Residency](../security/residency.md).

## Move a tenant to another DC

```sh
sprigctl --dry-run tenant move acme --to ams1     # check and show the plan
sprigctl tenant move acme --to ams1 --wait
sprigctl tenant move acme --status                # the move in progress, or the last one
```

| Flag | Default | Meaning |
|---|---|---|
| `--to <dc>` | none | The target DC |
| `--cluster <id>` | the target DC's first active cluster | The target cluster |
| `--max-pause-ms <ms>` | `migration.write_pause_target_ms` (5000) | The longest write pause the cutover may take |
| `--wait` | off | Return once the move committed or rolled back, with the pause and the duration |
| `--status` | off | Show the current or last move instead of starting one |

Point `--endpoint` at a controller of the tenant's **home** DC: its leader runs the move.

What happens:

1. The tenant's directory entry becomes `migrating`.
2. A mover job in the target DC copies the whole tenant (its catalog and every database) from its cluster to the target cluster, while clients keep writing.
3. It catches up from the binlog, verifies, and waits until lag stays under `mover.ready_max_lag_ms` for `mover.ready_sustain_seconds`.
4. It holds writes, fences the source, applies to the fence position, and commits the new home in one transaction. Every gateway in every DC follows the new home.
5. An attempt that would hold writes longer than the budget is aborted; after three, the move rolls back and the tenant stays where it was.

What clients see: writes pause for at most the budget. Sessions that held temporary tables, locks or an XA transaction are ended with 39502 `ER_SPRIG_SESSION_MOVED`; reconnect. Other sessions continue.

A move is refused with 39550 `ER_SPRIG_TENANT_MOVE_REFUSED` when the tenant is pinned, not `active`, already in the target, has another move or mover job open, the target has no active cluster, or you asked outside the home DC. An unknown DC gives 39503 `ER_SPRIG_UNKNOWN_DC`. A tenant with table groups can't move whole yet; it gets 39472 `ER_SPRIG_SHARDED_TENANT_MOVE`.

### Pin a tenant

```sh
sprigctl tenant pin acme --reason "contract requires fra1"
sprigctl tenant unpin acme
```

A pinned tenant stays on its cluster and in its DC. The balancer and automatic DC moves leave it alone, and drains and residency changes report it as blocked. `--cluster` pins it to another cluster of the same DC.

## Automatic moves towards traffic

Gateways report every tenant's reads and writes per client DC to the tenant's home DC every `routing.traffic_report_seconds` (10). A gateway knows a client's DC only when the client names it: the connection attribute or `SET @sprig_dc` on MySQL and MariaDB, the `Sprig-DC` header on REST and `sprig-dc` metadata on gRPC, an `appName` ending in `~fra1` on MongoDB, or a `CLIENT SETNAME` name ending in `~fra1` on Redis. Traffic without a hint, or with an unregistered DC, is reported without a client DC. See [Naming the client's DC](../concepts/clusters-and-data-centres.md#naming-the-clients-dc). The home DC's leader then decides, every `migration.evaluate_seconds` (60):

```text
w[d] = reads[d] + migration.write_weight × writes[d]      for each client DC d
L(c) = Σ w[d] × rtt(d, c) / Σ w[d]                         the weighted latency if c were home
```

A candidate DC qualifies when it improves `L` by at least `migration.min_improvement_pct` (30%) **and** `migration.min_improvement_ms` (5 ms), and keeps qualifying for `migration.sustain_minutes` (15). Then, in `auto` mode, the tenant moves exactly as with `sprigctl tenant move`.

Guards:

- `migration.min_signal_requests` (1000) weighted requests in `migration.window_minutes` (30) before traffic counts at all;
- a cooldown after each move of `migration.cooldown_hours` (6), doubling for each move back, capped at `migration.max_cooldown_days` (7);
- at most `migration.max_concurrent_per_dc` (2) moves at once at either end;
- tenants larger than `migration.auto_size_limit_gb` (200) wait for an operator;
- only DCs the tenant's residency allows. A better DC outside it raises an alert, never a move.

When nothing qualifies on latency, a home DC with less than `placement.dc_relief_spare_pct` (15%) spare capacity sheds tenants to a DC that is less busy by `placement.least_busy_margin`.

`migration.mode` sets the behaviour: `auto`, `approve` (each move waits in the approvals inbox), `recommend` (decide and log only) or `off`. See [Approvals](../concepts/approvals.md).

To see a tenant's traffic and what the loop would decide:

```sh
sprigctl tenant traffic acme
```

## Latency

Every DC leader pings a controller of every other DC every `control.latency_probe_seconds` (5), keeps p50, p90 and p99 over five minutes, and publishes a row when p50 moves by more than `control.latency_publish_change_pct` (10%), reachability changes, or ten minutes pass. Rows older than 30 minutes are ignored.

```sh
sprigctl dc latency
```

```text
FROM  TO    P50       P90       P99       REACHABLE
fra1  ams1  8.10 ms   9.40 ms   12.30 ms  true
fra1  mum1  61.20 ms  63.00 ms  70.10 ms  true
```

The matrix drives automatic moves, standby placement (nearest first) and the DC failover decision.

## Related

- [Clusters and data centres](../concepts/clusters-and-data-centres.md)
- [Failover](failover.md)
- [Residency](../security/residency.md)
- [Clusters](clusters.md)
- [Backups and moves](backups-and-moves.md)
