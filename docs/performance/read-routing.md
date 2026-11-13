---
title: Read routing
audience: [user, dba, operator]
sidebar:
  order: 5
---

# Read routing

When a shard's cluster has replicas, or is a Galera cluster, the gateway sends plain reads to copies other than the writer, so reads scale with the cluster and the writer keeps its capacity for writes. It does this without giving up read-your-writes: a read that follows your own write goes only to a copy that already has that write. This page explains which reads leave the writer, how read-your-writes works with GTIDs, the staleness limits, and how to keep reads on the writer.

## What can leave the writer

A statement is a **plain read** when it starts with `SELECT`, `WITH`, `VALUES`, `TABLE`, `DESCRIBE` or `EXPLAIN`, and has:

- no locking clause (`FOR UPDATE`, `FOR SHARE`, `LOCK IN SHARE MODE`);
- no `INTO`;
- no user-variable assignment;
- no call of a function with side effects or session effects, such as `GET_LOCK()`, `NEXTVAL()`, `SETVAL()`, `SLEEP()`, `BENCHMARK()`, `FOUND_ROWS()` or `ROW_COUNT()`.

Anything else, and anything the gateway doesn't recognize, is treated as a write and runs on the writer.

A plain read still stays on the writer while the session is pinned to its connection, that is, while any of these hold:

| Session state | Why it stays on the writer |
|---|---|
| A transaction is open (including after `SET TRANSACTION`, waiting for the next one) | The rest of the transaction runs there |
| `autocommit = 0` | Every statement opens a transaction |
| Temporary tables exist | They live on that connection |
| `LOCK TABLES` is in force, or `GET_LOCK()` locks are held | Locks belong to the connection |
| Server-side cursors or `HANDLER ... OPEN` are open | They belong to the connection |
| A client `XA` transaction | It belongs to the connection |
| `backend.pool_mode = session` | Every session keeps one connection |
| The session set `wsrep_sync_wait` to a non-zero value | It asked for causal reads across gateways, which the writer gives |

The topology decides the rest:

| Cluster | Plain reads go to |
|---|---|
| Single node, or an external cluster | The writer |
| Primary with replicas | A healthy replica that is within the lag limit and has the session's writes; otherwise the writer |
| Galera | The least-loaded synced node within the lag limit that has the session's writes; reads spread over equally loaded nodes |

A statement across several shards applies the same rule on each shard's cluster. Reads always go to the tenant's home data centre, wherever the gateway is: a gateway in another data centre sends them to the home data centre's clusters.

## Read-your-writes

After each commit, the gateway learns the commit's GTID from MariaDB's session tracking. That position becomes the **causal target** of later plain reads:

- with `reads.causal_scope = tenant_gateway` (the default), the target is every commit by the same tenant through this gateway, so another session of your application on the same gateway also sees the write;
- with `reads.causal_scope = session`, the target is the session's own last commit only.

A read with a causal target goes to a copy whose position covers it:

1. **A replica that already has it.** The gateway polls every replica's `@@gtid_slave_pos`, and every writer's and Galera node's `@@gtid_binlog_pos`, every `reads.replica_position_poll_ms` (250 ms). A replica whose last polled position covers the target serves the read with no wait.
2. **A replica that should have it soon.** On a primary-and-replica cluster that supports it, a replica less than `reads.gtid_wait_timeout_ms` (100 ms) behind serves the read after waiting for the position with `MASTER_GTID_WAIT`, bounded by the same timeout. If the wait times out, the statement reruns on the writer. You never see the timeout.
3. **The writer**, when no replica qualifies.

On Galera, a node that the polls show has the write serves the read at once. When only the writer is known to have it, `reads.galera_unconfirmed` decides: with `check` (the default), the read goes to the least-loaded node within the lag limit after a sync check bounded by `reads.galera_sync_wait_timeout_ms` (300 ms); a check that fails or times out reruns the statement on the writer. With `writer`, such reads stay on the writer.

A read with no causal target (nothing written yet) goes to any healthy copy within the lag limit.

### Across gateways

Read-your-writes covers commits made through the same gateway. If your application writes through one gateway and reads through another, for example behind a load balancer without session affinity, the second gateway doesn't know about the first one's commit. Your options:

- keep a client's connections on one gateway (connections are long-lived, so this is the usual case with connection pools);
- run the read inside a transaction, or with `reads.routing = writer` for the tables that need it;
- on Galera, set `wsrep_sync_wait` in the session, which keeps its plain reads on the writer.

## Staleness limits

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `reads.max_replica_lag_ms` | int | 1000 | object to global | A replica further behind gets no reads |
| `reads.max_galera_lag_ms` | int | 1000 | object to global | A Galera node further behind the most advanced node gets no reads until it catches up |
| `reads.gtid_wait_timeout_ms` | int | 100 | object to global | How long a read may wait for a replica to catch up before it reruns on the writer (0: never wait) |
| `reads.galera_sync_wait_timeout_ms` | int | 300 | object to global | Sync check before a Galera read whose write isn't confirmed on the node |
| `reads.galera_unconfirmed` | text | `check` | object to global | `check` or `writer`, for a Galera read whose write no polled position shows yet |
| `reads.replica_position_poll_ms` | int | 250 | cluster, dc, global | How often positions are polled |
| `reads.causal_scope` | text | `tenant_gateway` | tenant, global | `session` or `tenant_gateway` |
| `reads.routing` | text | `auto` | object, database, tenant, global | `auto`, or `writer` to keep every read on the writer |

"Object to global" means object, database, tenant, cluster, data centre and global. Lowering `reads.max_replica_lag_ms` sends more reads to the writer; raising it lets reads without a causal target see older data. A read with a causal target is never served stale, whatever the lag limit.

## Keep reads on the writer

Some reads must see the latest committed data from every gateway, such as a balance check before a payment. Keep them on the writer for one table, one database or the whole tenant:

```sql test
CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance DECIMAL(12,2) NOT NULL);
ALTER TABLE accounts SPRIG SET (reads.routing = 'writer');
SHOW SPRIG SETTINGS LIKE 'reads.routing' FOR TABLE accounts;
```

```sql
-- Every read in database shop.
ALTER DATABASE shop SPRIG SET (reads.routing = 'writer');

-- Every read of the tenant (needs SPRIG_SETTINGS_ADMIN).
SPRIG SET reads.routing = 'writer';
```

To keep one statement on the writer without changing settings, run it inside a transaction:

```sql test
START TRANSACTION READ ONLY;
SELECT balance FROM accounts WHERE id = 1;
COMMIT;
```

Remove the override with `ALTER TABLE accounts SPRIG RESET (reads.routing);`.

## Reads during moves and failover

While a shard is being cut over to another cluster, or a tenant moves to another data centre, statements wait for the cutover (up to its hold) rather than run against a copy that isn't caught up; a statement still waiting when the hold's deadline passes fails with error 1205 and can be retried. During a data-centre failover, every statement of the tenant, reads included, waits until the new home has caught up. A replica that falls behind or stops is skipped by the next read, and a read that fails on a replica before sending anything is retried elsewhere. See [Failover](../operate/failover.md) and [Backups and moves](../operate/backups-and-moves.md).

## Watch read routing

| Metric | Labels | Meaning |
|---|---|---|
| `sprig_backend_reads_total` | `cluster`, `target`, `causal` | Plain reads by where they went (`writer`, `replica`, `galera`, `galera_unconfirmed`) and whether they had a causal target |
| `sprig_backend_gtid_wait_seconds` | `cluster` | Time reads waited for a replica to catch up |
| `sprig_backend_gtid_wait_timeouts_total` | `cluster` | Waits that timed out and reran on the writer |
| `sprig_backend_replica_lag_seconds` | `cluster`, `node` | Each replica's lag |

Many `writer` reads with `causal="yes"` and many wait timeouts mean replicas lag behind your write rate: add replica capacity, or raise `reads.gtid_wait_timeout_ms` slightly. See [Monitoring](../operate/monitoring.md).

## Related

- [Clusters and data centres](../concepts/clusters-and-data-centres.md)
- [Limits and timeouts](limits-and-timeouts.md)
- [Failover](../operate/failover.md)
- [Settings reference](../reference/settings.md)
