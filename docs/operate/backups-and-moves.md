---
title: Backups and moves
audience: [operator]
sidebar:
  order: 8
---

# Backups and moves

This page covers copying a tenant out and back in with `sprigctl tenant export` and `import`, moving tenants and shards between clusters online, and backing up the two things SprigDB's state lives in: MariaDB and etcd.

| You want to | Use |
|---|---|
| A portable copy of one tenant, or to clone it into another | `sprigctl tenant export` and `import` |
| Move a tenant or a shard to another cluster, online | `sprigctl shard move` |
| Move a tenant to another data centre, online | `sprigctl tenant move` ([Data centres](data-centres.md)) |
| Recover from losing a cluster's data | A MariaDB backup with the matching etcd snapshot |
| Recover the control plane | An etcd snapshot |

## Export a tenant

```sh
sprigctl tenant export acme --to /backups/acme-2026-11-13 \
    --gateway gw.fra1.internal:3306 --user acme.admin --password 's3cret-Passw0rd' \
    --tls-ca /etc/sprigdb/tls/ca.pem
```

| Flag | Environment | Meaning |
|---|---|---|
| `--to <dir>` | | A new or empty directory |
| `--gateway <host:port>` | `SPRIGCTL_GATEWAY` | A gateway's MySQL or MariaDB listener |
| `--user <tenant>.<user>` | | A tenant administrator's login |
| `--password` | `SPRIGCTL_GATEWAY_PASSWORD` | Its password. Prefer the variable, so the password isn't in your shell history. |
| `--tls-ca <file>` | | Verify the gateway's certificate against this CA |
| `--tls-insecure` | | Accept any certificate (development only) |

Schemas, rows and grants travel through the gateway's normal SQL path, under logical names, so an export loads into any tenant on any cluster, whatever its topology. The Admin API carries what SQL can't: password verifiers and setting rows with their bounds. `sprigctl` also needs to reach the Admin API (`--endpoint`).

All rows are read in one `START TRANSACTION WITH CONSISTENT SNAPSHOT`, so the export is consistent as of one moment.

### What's in the directory

```text
manifest.json       format, tenant and time; per database its CREATE DATABASE;
                    per table its CREATE statement, row count and checksum
accounts.json       users with their password verifiers and SQL roles, and each
                    account's SHOW GRANTS lines
settings.json       the tenant's own setting rows (tenant, database and object scope)
data/<db>/<t>.sql   one INSERT per line, at most 512 KB each
```

The export holds password verifiers and all the tenant's data. Store it as you would a database backup: encrypted, with restricted access.

### What isn't carried

- Triggers, stored routines and events: listed in the manifest's `skipped`, not exported.
- Automatic indexes: left out on purpose; the target's own automatic indexing decides.
- Only what the MySQL protocol shows as tables and views is exported. Other data models aren't carried yet.

## Import a tenant

```sh
sprigctl tenant import /backups/acme-2026-11-13 --into acme-restore --create \
    --gateway gw.fra1.internal:3306 --user acme-restore.admin --password 's3cret-Passw0rd'
```

| Flag | Meaning |
|---|---|
| `<from>` | The export directory |
| `--into <tenant>` | The target tenant |
| `--create` | Create the target tenant first |
| `--cluster <id>` | With `--create`: the target's cluster |
| `--verify-only` | Only compare the target with the export; change nothing |
| `--gateway`, `--user`, `--password`, `--tls-ca`, `--tls-insecure` | As for export; `--user` is a login of the **target** tenant |

The import runs in this order:

1. Create the tenant (`--create`).
2. Create the accounts with their verifiers, so the source's passwords log in to the target. Accounts that held `SPRIG_TENANT_ADMIN` become tenant administrators. Accounts that already exist are left alone and noted.
3. Logged in through the gateway: databases, tables, rows, views, roles and grants.
4. The setting rows.
5. Verify.

Because the accounts come first, `--user` can be one of the source's administrators under the target's name: `acme-restore.admin` with `acme.admin`'s password.

### Verification

Each table's checksum is independent of row order: the sum, modulo 2^128, of the first 16 bytes of each row's SHA-256 over its values as the text protocol returns them (NULL marked apart from the empty string), together with the row count. `import` recomputes every table's checksum on the target and compares it, and every account's grants, with the manifest. Any difference fails with 39971 `ER_SPRIG_IMPORT_VERIFY_FAILED` and lists what differs.

To check a tenant against an export later, without changing anything:

```sh
sprigctl tenant import /backups/acme-2026-11-13 --into acme --verify-only \
    --gateway gw.fra1.internal:3306 --user acme.admin
```

## Move a tenant or a shard to another cluster

The mover copies data between clusters while clients keep writing, then switches ownership in a short write hold.

```sh
sprigctl shard move acme --to fra1-c2 --wait          # a tenant without table groups, whole
sprigctl shard move acme 3 4 --to fra1-c2 --wait      # shard 4 of table group 3
sprigctl shard list acme                              # groups, shards and their clusters
```

| Flag | Default | Meaning |
|---|---|---|
| `--to <cluster>` | none | The target cluster, in the same data centre |
| `--verify` | `full` | `full`, `off`, or `sample:<pct>` |
| `--manual` | off | Wait in `ready` until you run `sprigctl job cutover <id>` |
| `--wait` | off | Return once the cutover committed or the job ended |
| `--pin` | off | Then pin the tenant to the target, so the balancer leaves it there |

### How a move runs

| State | What happens |
|---|---|
| `planned` | Waiting for a worker |
| `preparing` | Creating target schemas and tables |
| `copying` | Chunked copy, with binlog catch-up running alongside |
| `catching_up` | Applying the binlog until lag is low |
| `verifying` | Comparing source and target by per-row digests, repairing ranges that differ |
| `ready` | Lag below `mover.ready_max_lag_ms` (1000) for `mover.ready_sustain_seconds` (60); waiting to cut over |
| `cutting_over` | Holding writes, fencing the source, applying up to the fence, raising the shard map's epoch |
| `post_cutover`, `retaining`, `cleaning` | The fenced source is kept for `mover.source_retain_hours` (24), then dropped |
| `done` | Finished |
| `rolling_back`, `rolled_back` | Cancelled or failed before the commit; the source stayed authoritative throughout |
| `paused` | Stopped by an operator or a guardrail; resumes into the same state |

```sh
sprigctl job list --tenant acme
sprigctl job show <id>
sprigctl job cutover <id>        # a --manual move waiting in ready
sprigctl job cancel <id>         # before the cutover commits
```

### What clients see

Writes to the moving shard pause for at most `mover.cutover_max_hold_ms` (2000). A transaction that was using the shard when it cut over is rolled back with 39110 `ER_SPRIG_SHARD_MOVED`, which MySQL clients receive as a deadlock (1213, SQLSTATE 40001): retry it, as after any deadlock. A cutover that would hold longer is aborted (39135) and retried after `mover.cutover_retry_seconds` (30, doubling), up to `mover.max_cutover_attempts` (5).

### Throttles

The copy slows down or pauses so it doesn't hurt live traffic:

| Setting | Default | Effect |
|---|---|---|
| `mover.copy_max_rows_per_sec` | 50000 | Copy rate cap per job |
| `mover.copy_max_mb_per_sec` | 64 | Copy bandwidth cap per job |
| `mover.max_copies_per_cluster` | 2 | Copies reading or writing one cluster at once |
| `mover.throttle_replica_lag_ms` | 2000 | Replica lag that slows the copy |
| `mover.throttle_threads_running` | 64 | Source `Threads_running` that slows it |
| `mover.throttle_flow_control_pct` | 5 | Galera flow-control threshold |
| `mover.throttle_apply_lag_ms` | 5000 | Stop copying when catch-up lags this much |

### Failures

| Error | Meaning | What to do |
|---|---|---|
| 39134 `ER_SPRIG_MOVE_SOURCE_DIVERGED` | The source's binlog no longer has the position the move needs | Nothing: the job restarts from a fresh snapshot. Keep binlogs for at least `cluster.min_binlog_retention_hours`. |
| 39136 `ER_SPRIG_MOVE_VERIFY_FAILED` | Verification still differs after `mover.verify_max_repairs` (3) repairs | The job rolls back by itself. Look for writes reaching the source outside SprigDB, stop them, and start the move again |
| 39137 `ER_SPRIG_MOVE_UNSUPPORTED` | Statement-based binlog events, a table without a primary key, or DDL during a cutover | Fix the cause and start the move again |
| 39111 `ER_SPRIG_MOVE_TARGET_INCOMPATIBLE` | The target already has a schema with the same name, or lacks a capability the shard uses | Drop a leftover schema from an old failed move, or choose another target |

### Split, merge and re-key shards

For tenants with table groups:

```sh
sprigctl shard split acme 3 4 --ways 3                 # split shard 4 of group 3 at the size quantiles
sprigctl shard split acme 3 4 --at 0x9000000000000000  # at a hash
sprigctl shard merge acme 3 6 7                        # two adjacent shards
sprigctl shard rekey acme 3 --key customer_id --member events=cust_id --shards 4
```

Each runs as a mover job, online, and shows in `sprigctl job list`. The balancer does the same on its own; see [Shards and table groups](../concepts/shards-and-table-groups.md) and [Sharding](../sql/sharding.md).

## Back up MariaDB

SprigDB keeps every tenant's data in ordinary MariaDB schemas, so back up each cluster with MariaDB's own tools. `mariadb-backup` takes a consistent physical copy of a running server:

```sh
mariadb-backup --backup --stream=xbstream --user=backup --password='s3cret-Passw0rd' \
    | zstd | openssl enc -aes-256-ctr -pbkdf2 -salt -pass file:/etc/sprigdb/backup.pass \
    > /backups/fra1-c1-$(date -u +%Y%m%dT%H%M%SZ).xbstream.zst.enc
```

Take it on a replica to keep the load off the primary. Keep binlogs long enough to bridge the gap between backups (`binlog_expire_logs_seconds`, at least `cluster.min_binlog_retention_hours`).

Back up **etcd at the same time** as MariaDB. Shard maps in etcd say which cluster holds which schema, so a restored MariaDB needs the shard maps from the same moment.

### Consistency

A MariaDB backup and an etcd snapshot taken seconds apart can disagree if a move, split or merge cut over between them. Take backups when no mover job is running (`sprigctl job list`), or pause the balancer around the backup:

```sh
sprigctl settings set balancer.mode off --scope dc --dc fra1 --reason "nightly backup"
# ... back up MariaDB and etcd ...
sprigctl settings reset balancer.mode --scope dc --dc fra1 --reason "backup done"
```

Point-in-time restore from archived binlogs isn't supported yet.

## Back up etcd

etcd holds the control plane: tenants and their homes, shard maps, cluster records, settings at global, dc and cluster scope, operators, and jobs. It holds no secrets: credentials are referenced, never stored.

```sh
etcdctl --endpoints https://etcd-fra1-0:2379 \
    --cacert /etc/sprigdb/tls/ca.pem --cert /etc/sprigdb/tls/cert.pem --key /etc/sprigdb/tls/key.pem \
    snapshot save /backups/etcd-fra1-$(date -u +%Y%m%dT%H%M%SZ).db
```

Snapshot every DC tier and the global tier. In single-etcd mode, one snapshot covers both.

Keep etcd small: the Ansible role and the Helm chart set up periodic auto-compaction and a daily defragmentation of each member. See [Deploying on VMs](deploy-vms.md) and [Deploying on Kubernetes](deploy-kubernetes.md).

## Back up the secrets

Restored data is useless without the files that open it. Keep a copy of these, separately from the data backups and off the hosts:

- the credential files the cluster records reference (`sprig_ctl`, `sprig_repl`);
- the gateways' `backends_file` and its secret;
- `workload_key_file`, which sealed query samples are encrypted under;
- `[recovery] extract_key_file`, which diverged-transaction extracts are sealed under;
- Admin API and service tokens, and the TLS CA and keys.

## Restore

A restore puts back the data and the control plane as of one moment. Everything written after it is gone.

1. Stop SprigDB's processes in the affected data centre.
2. Restore each MariaDB node (`mariadb-backup --prepare`, then `--copy-back` or `--move-back` into an empty data directory).
3. Restore etcd from the snapshot taken with that MariaDB backup (`etcdutl snapshot restore`), as a new cluster, then add the other members back.
4. Start etcd, then the controllers, workers and gateways.
5. Check: `sprigctl cluster list`, `sprigctl tenant list`, and a `CHECKSUM TABLE` on a few tenants' tables.

The VM bundle in `deploy/compose-vm` automates the whole cycle: `sprig-vm.sh backup` streams every local MariaDB node through `mariadb-backup`, zstd and encryption, snapshots etcd into the same encrypted backup, copies it off the VM and prunes old ones; `sprig-vm.sh restore <ts>` checks and restores it. See [Deploying on VMs](deploy-vms.md) and [Deploying with Docker Compose](deploy-compose.md).

## Related

- [Data centres](data-centres.md)
- [Clusters](clusters.md)
- [Shards and table groups](../concepts/shards-and-table-groups.md)
- [Troubleshooting sharding and moves](../troubleshooting/sharding-and-moves.md)
- [sprigctl](../tools/sprigctl.md)
