---
title: Upgrades
audience: [operator]
sidebar:
  order: 9
---

# Upgrades

SprigDB is one binary, `sprigdb`, in five roles. You upgrade it by replacing the binary (or image) and restarting processes one at a time. Gateways drain before they stop, controllers hand leadership over, and catalog changes are additive, so a careful rolling upgrade loses no acknowledged write and refuses few or none.

This page covers the order, draining gateways, what each role does on restart, catalog format migrations, checking that live changes need no restart, and rolling back.

## Before you start

- Read the release notes for any change to a stored format (tenant catalogs or etcd records). It decides how you can roll back.
- Take a backup of MariaDB and etcd. See [Backups and moves](backups-and-moves.md).
- Check that nothing is mid-flight that you'd rather not restart: `sprigctl job list` for mover jobs, `sprigctl txn list` for in-doubt distributed transactions.
- Run at least two controllers and two gateways per data centre. With one of each, a restart is a short outage (see below).

Check versions with `sprigdb --version` and `sprigctl --version`. `sprigctl dc show <dc>` shows the build of the current DC leader.

## The order

Within each data centre, one process at a time, each only after the previous one is ready again (`/readyz` answers 200):

1. **Controllers**, followers first and the DC leader last, so leadership moves once.
2. **Workers**.
3. **Node agents**, on VMs where SprigDB fences MariaDB.
4. **Gateways**, each drained first.

Then the next data centre. The Ansible role (`deploy/ansible`) upgrades one host at a time and restarts its roles one by one; the Helm chart's pod disruption budgets allow one pod of each component down at a time. See [Deploying on VMs](deploy-vms.md) and [Deploying on Kubernetes](deploy-kubernetes.md).

## Draining a gateway

A gateway drains on SIGTERM, on `sprigctl gateway drain`, or when its provisioner removes it:

1. Its state becomes `draining` and `/readyz` answers 503, so load balancers stop sending it new connections. It keeps serving for `gateway.drain_lb_delay_seconds` (10).
2. It closes its listeners.
3. It closes idle sessions, and marks the rest to close at the next safe point (outside a transaction).
4. It waits until no session is left, or `gateway.drain_timeout_seconds` (30), then closes what remains.
5. It flushes workload statistics, ends change-feed subscriptions, gives up its registration and exits 0.

Its write permit stays valid throughout, so writes in progress complete. A draining gateway refuses to begin new REST or gRPC transactions with 39000 `ER_SPRIG_GATEWAY_DRAINING` (retryable); clients should reconnect, and the load balancer sends them to another gateway.

```sh
sprigctl gateway list            # GATEWAY, SLOT, STATE, ADDR, CONNS/ACTIVE and LISTENERS per gateway
sprigctl gateway show gw-2
sprigctl gateway drain gw-2      # any gateway answers; the drain reaches gw-2 through the peer service
```

`gateway drain` returns at once, saying the gateway is draining (or already was). The gateway then exits on its own; your process manager decides whether to start it again.

Give the process manager more time than the drain needs: drain delay plus drain timeout, plus a margin. The shipped systemd units use `TimeoutStopSec=90s` and the Helm chart `terminationGracePeriodSeconds: 90`. If you raise `gateway.drain_timeout_seconds`, raise those too.

## What each role does on restart

| Role | On stop | On start | Effect on clients |
|---|---|---|---|
| Gateway | Drains (above) | Registers, reads settings, opens listeners, then turns ready | None with a load balancer and another gateway |
| Controller | A DC leader gives up leadership; another controller takes over | Answers permit requests only after it has read the DC lease, so it can't fence anyone while it starts | None while another controller of the DC renews permits |
| Worker | Stops claiming jobs; its jobs' leases lapse | Claims runnable jobs | A running move pauses for up to `mover.job_lease_seconds` (10) and resumes on another worker from its recorded state |
| Agent | — | Keeps its node `read_only` until it holds a DC permit and the writer lease again | On the writer, about half a second of writes refused with MariaDB error 1290 |

**One controller.** Gateways may write only while a DC permit is valid, and only controllers renew permits. With a single controller, its restart fences writes for a few seconds: clients get 39501 `ER_SPRIG_DC_FENCED`, which is retryable. Clients that retry ride through it; two controllers avoid it.

**One gateway.** Its restart closes the client ports until it is back.

## Configuration changes

`sprigdb.toml` is read only at start. Change the file on every host, then restart processes in the order above. Until a process restarts, `/readyz?verbose` reports the difference:

```text
[-]config drift: /etc/sprigdb/sprigdb.toml differs from the running configuration; it applies at the next rolling restart
```

Live settings need no restart at all; see [Configuration](configuration.md).

## Check that live changes need no restart

`sprigctl check no-restart` proves that settings, listener addresses and certificates change under load with no restart and no failed statement:

```sh
export SPRIGCTL_GATEWAY_PASSWORD='s3cret-Passw0rd'
sprigctl check no-restart --telemetry http://gw-1.fra1.internal:9180 \
    --gateway gw-1.fra1.internal:3306 --user acme.app \
    --set gateway.max_connections=30000@dc:fra1 \
    --listener mysql=0.0.0.0:3316 \
    --rotate-cert /etc/sprigdb/tls/gw.pem=/tmp/new-gw.pem --rotate-key /etc/sprigdb/tls/gw-key.pem=/tmp/new-gw-key.pem
```

It opens `--connections` (4) connections running `SELECT 1` through the gateway, then applies each change, waits `--settle` seconds (5), checks it took effect, restores it, and checks again:

| Change | Applied through | Took effect when |
|---|---|---|
| `--set key=value@scope` | `sprigctl settings set`, restored afterwards | The Admin API resolves the new value, and the gateway counted no settings-apply failures |
| `--listener name=host:port` | `listener.<name>.bind` at `--listener-scope` (`dc`) | The new address accepts connections and the listener generation rose |
| `--rotate-cert live.pem=new.pem` (with `--rotate-key`) | The certificate file is replaced; the key first | The gateway's certificate expiry metric changed |

Throughout, the gateway's `/readyz?verbose` must keep the same process id and start time (no restart) and stay ready, and no statement may fail. A connection the gateway closes at a safe point, to move a session off a retired listener, counts as a reconnect, not a failure. The command exits 0 when every check passed, and 1 otherwise. It also reports `sprigdb.toml` drift. `--dry-run` lists the changes without making them; `--tls-insecure` runs the load over TLS accepting any certificate.

## Catalog format migrations

Each tenant has a catalog schema, `t<id>_sprig`, whose tables belong to modules (the core, authentication, approvals, the advisor, and so on). Each module's migrations are numbered, and the number is the module's **format**. Migrations come in two phases:

- **Expand** is additive and idempotent: `CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, copies. A binary that reads the previous format still works afterwards.
- **Contract** removes what older binaries need: drops and renames. After a contract to format F, only binaries that read F can use the catalog.

A new binary applies the expand migrations it knows to tenant catalogs: every module's at tenant creation, and a module's own when the component that owns it next needs its tables. Runs are serialized per catalog with a MariaDB named lock, recorded per migration, and safe to repeat after a crash. A catalog that a mover cutover has fenced is skipped and retried later (39070, retryable).

Each catalog records the oldest format a reader must know. A binary older than that gets 39073 `ER_SPRIG_CATALOG_FORMAT_TOO_NEW`: upgrade it rather than run old binaries against upgraded tenants.

Contract migrations don't run automatically yet, so an upgraded catalog stays readable by the previous binary.

## Rolling back

Roll back by installing the previous binary and restarting in the same order.

- When the release changed no stored format, that is all.
- When the release notes say a version migrates catalogs or etcd records in a way older binaries can't read, restore the backup you took before the upgrade, with the old binary, instead. Everything written since is lost.

## Kubernetes

Change the image tag in your values and run `helm upgrade`. The chart's readiness probes use `/readyz`, so a rollout waits for each pod, and gateways get 90 seconds to drain. See [Deploying on Kubernetes](deploy-kubernetes.md).

### Controllers move from a Deployment to a StatefulSet

Earlier versions of the chart ran the controllers as a Deployment. The current chart runs them as a StatefulSet with the same name, `sprigdb-controller`, behind a new headless Service, `sprigdb-controller-pods`. The first `helm upgrade` to it replaces the Deployment with the StatefulSet: Helm creates the new objects and then deletes the old Deployment and its pods. Plan for this:

- The new pods are `sprigdb-controller-0`, `-1`, and so on. They start in parallel and register `<pod>.sprigdb-controller-pods.<namespace>.svc:7480` as their Admin address, where gateways forward Admin calls to the leader.
- The DC's leader moves to one of the new pods once the old ones are gone. Admin calls (`sprigctl`, Studio's operator pages) made while it moves may need a retry.
- If you set `controller.adminApi.tlsSecret`, its certificate must also name `*.sprigdb-controller-pods.<namespace>.svc` before you upgrade. The chart's own controller certificate already covers it.
- The upgrade also adds telemetry Services for workers and operators, and lets the operator list `batch` Jobs to export backup times; see [Monitoring](deploy-kubernetes.md#monitoring).

Upgrade one data centre at a time, as for any release.

## Not supported yet

- Automatic upgrade orchestration: SprigDB doesn't track which builds are running before it migrates catalogs, and doesn't run contract migrations or a delay before them.
- Point-in-time restore for a rollback after a stored-format change.

## Related

- [Deploying on VMs](deploy-vms.md)
- [Deploying on Kubernetes](deploy-kubernetes.md)
- [Backups and moves](backups-and-moves.md)
- [Monitoring](monitoring.md)
- [Error codes](../reference/errors.md)
