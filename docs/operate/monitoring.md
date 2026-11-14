---
title: Monitoring
audience: [operator]
sidebar:
  order: 10
---

# Monitoring

Every SprigDB process (gateway, controller, worker, agent and operator) serves health checks and Prometheus metrics on its telemetry port, logs JSON lines to standard output, and can export traces over OTLP. SprigDB ships alert rules and Grafana dashboards built on those metrics. This page covers what to scrape, what to alert on and where to look when something is wrong.

## The telemetry port

Each process serves its telemetry endpoints on `[telemetry] bootstrap_addr`, by default `0.0.0.0:9180`. It is a process setting: change it in `sprigdb.toml` or with `SPRIGDB_TELEMETRY_BOOTSTRAP_ADDR`, and give each process on one host its own port. The port is plain HTTP; keep it on a private network.

| Endpoint | Returns |
|---|---|
| `GET /healthz` | Liveness: `200 ok`, or `503 watchdog stalled` when the process's internal watchdog hasn't ticked for 5 seconds |
| `GET /readyz` | Readiness: `200 ready`, or `503` with `starting` or `draining` |
| `GET /readyz?verbose` | The same status code, with the process's identity and every check |
| `GET /metrics` | Every metric in Prometheus text format |
| `GET /debug/traces` | The most recent kept traces as JSON (up to 64); `?attr=<key>=<value>` keeps those whose root span has that attribute |

The port is bound before the role starts, so `/healthz` answers at once and `/readyz` reports `starting` until the role is ready. On SIGTERM a process turns `/readyz` to `draining` first, so load balancers stop sending it work before it closes anything. A draining process never becomes ready again.

Use `/healthz` for liveness probes (restart a process that fails it) and `/readyz` for load-balancer health checks and readiness probes. Every deployment SprigDB ships does this: the Helm chart's probes, the Compose healthchecks, and Envoy's health check in the VM bundle.

### Verbose readiness

```sh
curl -s http://10.0.0.11:9180/readyz?verbose
```

```text
ready
[+]process pid=4242 started_unix_ms=1762930021512 version=1.0.0
[+]watchdog ticking
[+]config /etc/sprigdb/sprigdb.toml matches the running configuration
```

- `process` gives the pid and start time, so you can prove that a process didn't restart across a change (as `sprigctl check no-restart` does).
- `watchdog` is `ticking`, or `[-]watchdog stalled`.
- `config` compares the configuration file on disk with the one the process started from. `[-]config drift: ... differs from the running configuration; it applies at the next rolling restart` means someone edited `sprigdb.toml` and the process hasn't picked it up. Live settings never cause drift; only the process file does.

## Metrics

`/metrics` exposes every metric SprigDB records, about 430 in 50 groups, all prefixed `sprig_`. [Metrics](../reference/metrics.md) lists each one with its type, labels and the component that records it.

- Counters end in `_total`. Durations are in seconds.
- The latency metrics that service-level objectives read are Prometheus histograms with buckets at 0.5, 1, 2, 5, 10, 25, 50, 100, 250 and 500 ms and 1, 2.5, 5, 10 and 30 s: `sprig_gateway_query_duration_seconds`, `sprig_gateway_overhead_seconds`, `sprig_gateway_admission_wait_seconds`, `sprig_backend_pool_wait_seconds` and `sprig_mover_cutover_hold_seconds`. Every other duration is a summary.
- Labels carry ids and kinds, never SQL text or values.

### Statements and their result

Every request on every protocol is counted once in `sprig_gateway_statements_total`, with labels `protocol`, `result` and `tenant`:

| `result` | Meaning |
|---|---|
| `ok` | Succeeded |
| `client` | The application caused the error: a syntax error, a duplicate key, a permission refusal |
| `sprig` | SprigDB failed |
| `backend` | MariaDB was unreachable or failed mid-statement (for example errors 1040, 1053, 2006, 2013) |

Availability is the share of statements that aren't `sprig` or `backend`. Requests cancelled by `KILL` or a client hang-up aren't counted.

To keep the number of series bounded, a tenant gets its own `tenant` label value only once its statements reach `telemetry.tenant_series_min_rps` (10 per second) over its last full minute. Every other statement has `tenant=""`, which PromQL treats as no label.

`sprig_gateway_overhead_seconds` is the time a request spent in the gateway itself, excluding the time waiting on MariaDB. Watching it separately from `sprig_gateway_query_duration_seconds` tells you whether slowness is SprigDB's or the database's.

### What to watch

| Area | Metrics | Look for |
|---|---|---|
| Traffic and errors | `sprig_gateway_statements_total` | A rising share of `sprig` or `backend` results |
| Latency | `sprig_gateway_query_duration_seconds`, `sprig_gateway_overhead_seconds` | p99 over 1 s; overhead above 2 ms for more than 1% of statements |
| Admission | `sprig_gateway_admission_wait_seconds`, `sprig_gateway_connections_rejected_total` | Requests queueing for admission; refused connections by `reason` |
| Connections | `sprig_gateway_connections` (`listener`, `state`) | Approaching `gateway.max_connections` |
| Backend pools | `sprig_backend_pool_connections`, `sprig_backend_pool_wait_seconds`, `sprig_backend_failures_total` | Pools exhausted, rising failures by `kind` |
| Replication | `sprig_backend_replica_lag_seconds`, `sprig_backend_node_up`, `sprig_backend_writer_changes_total` | Lag above 30 s, nodes down, unexpected writer changes |
| Clusters | `sprig_cluster_health_state`, `sprig_cluster_probe_failures_total`, `sprig_cluster_semi_sync_status` | A cluster with no writer (state 2), failing probes, semi-sync off |
| Fencing | `sprig_gateway_fenced`, `sprig_gateway_fenced_writes_total`, `sprig_gateway_permit_remaining_seconds`, `sprig_agent_fenced` | A gateway or MariaDB node fenced outside a failover |
| Failover | `sprig_controller_cluster_failovers_total`, `sprig_controller_failover_duration_seconds`, `sprig_controller_failover_loss_seconds` | Failovers, how long they took, the loss estimate |
| Control plane | `sprig_controller_leader_changes_total`, `sprig_controller_loop_blocked`, `sprig_controller_jobs`, `sprig_meta_cache_staleness_seconds` | Leader churn, blocked loops, stuck jobs, metadata caches more than 2 s stale |
| Data movement | `sprig_mover_catchup_lag_seconds`, `sprig_mover_copy_rows_total`, `sprig_mover_apply_rows_total` | Moves that stop making progress |
| Certificates | `sprig_gateway_cert_expiry_seconds` (`bundle`) | Under 14 days |
| Audit | `sprig_audit_spool_bytes`, `sprig_audit_spool_max_bytes`, `sprig_audit_dropped_total` | A spool filling up |
| Tracing | `sprig_telemetry_spans_dropped_total` | A collector that is down or too slow |

### Scrape configuration

Scrape every process's telemetry port and add a `dc` label to each data centre's targets; the alert rules and dashboards group by it. Without a `dc` label everything is treated as one data centre.

```yaml
scrape_configs:
  - job_name: sprigdb
    scrape_interval: 15s
    static_configs:
      - targets: ["10.0.0.11:9180", "10.0.0.12:9180", "10.0.0.13:9180"]
        labels: {dc: fra1, role: gateway}
      - targets: ["10.0.0.11:9181", "10.0.0.12:9181", "10.0.0.13:9181"]
        labels: {dc: fra1, role: controller}
```

On Kubernetes, set `monitoring.serviceMonitor: true` in the Helm chart: its ServiceMonitor scrapes the gateways, controllers, workers and operators (not etcd); see [Deploy on Kubernetes](deploy-kubernetes.md#monitoring). In the Compose VM bundle, `docker compose --profile monitoring up -d prometheus` starts Prometheus on 127.0.0.1:9090 with the rules loaded; with three VMs, set `SPRIG_PROMETHEUS_CONFIG=prometheus.ha.yml` and `SPRIG_VM_HOSTS`, and run `./sprig-vm.sh monitoring-config` to write targets for every VM's gateway, controller, worker and agent. MariaDB itself has no exporter in the bundle; add `prom/mysqld-exporter` if you want server internals.

Scrape etcd's own `/metrics` too: two of the alert rules read etcd's metrics.

## Alert rules

`deploy/alerts/` holds Prometheus rule files, each tested with `promtool test rules`:

```sh
deploy/alerts/test.sh      # promtool check rules and test rules (uses the prom/prometheus image without promtool)
```

Load all three files into Prometheus (`rule_files:`) and route the alerts with your Alertmanager. Each alert has a `severity` label, `page` or `ticket`.

### Service-level objectives (`sprig-slo.rules.yaml`)

Two objectives per data centre, with recording rules (`sprig:availability_errors:ratio_rate5m` and so on, over 5m, 30m, 1h, 6h and 3d) and multi-window burn-rate alerts:

| Objective | Target | Measured from |
|---|---|---|
| Availability | 99.95% over 30 days | Statements whose result isn't `sprig` or `backend` |
| Gateway overhead | 99% of statements under 2 ms in the gateway | `sprig_gateway_overhead_seconds` |

| Alert | Severity | Fires when |
|---|---|---|
| `SprigAvailabilityFastBurn` | page | The error budget burns at 14.4 times the sustainable rate over 1 h and 5 m, or 6 times over 6 h and 30 m |
| `SprigAvailabilitySlowBurn` | ticket | It burns faster than sustainable over 3 days and 6 hours |
| `SprigOverheadFastBurn` | page | As the availability fast burn, for overhead above 2 ms |
| `SprigOverheadSlowBurn` | ticket | As the slow burn, for overhead |
| `SprigQueryLatencyHigh` | ticket | Statement p99 above 1 s for 10 minutes |

### Operations (`sprig-ops.rules.yaml`)

| Alert | Severity | Fires when |
|---|---|---|
| `SprigReplicaLagHigh` | ticket | A replica is more than 30 s behind for 5 minutes; reads stop going to it |
| `SprigReplicaLagCritical` | page | A replica is more than 5 minutes behind for 5 minutes |
| `SprigClusterNoWriter` | page | A cluster has had no writer for 1 minute |
| `SprigMoverStuck` | page | A move, split, merge, rekey or tenant migration has catch-up lag over 10 minutes, or copied and applied no rows for an hour, for 15 minutes |
| `SprigMetaPropagationSlow` | ticket | Metadata caches are more than 2 s stale for 5 minutes |
| `SprigSinkPaused` | ticket | A worker has had a paused sink for 5 minutes. The `reason` label is `failures` (39222: the destination kept failing) or `owner_privilege` (39229: the sink's owner lost `SELECT` on its source, or `SPRIG_FAN_IN` for a fan-in sink, or was locked or dropped). `SHOW SINKS` shows which sink and why; see [Sinks](../data-models/sinks.md#retries-dead-letters-and-pausing) |
| `AuditSpoolFilling` | ticket | An audit spool is over half full for 10 minutes |
| `AuditUnavailable` | page | An audit spool is full: admin and account changes fail with 39970 |
| `AuditEventsDropped` | page | Audit events were dropped |
| `CertificateExpiring` | ticket | A listener certificate expires within 14 days |
| `CertificateExpiringSoon` | page | A listener certificate expires within 3 days |
| `EtcdNoLeader` | page | An etcd member has had no leader for 1 minute |
| `EtcdDbNearQuota` | ticket | An etcd database is above 80% of its quota |
| `EtcdDbLarge` | ticket | An etcd database is above 512 MiB for 30 minutes |
| `EtcdDbFragmented` | ticket | An etcd database above 128 MiB is more than half free space for 2 hours (defrag isn't running) |
| `EtcdDbGrowing` | ticket | Live etcd data grew more than 16 MiB in 6 hours (compaction off, or keys leak) |
| `BackupMissed` | page | A cluster has no full backup younger than 26 hours |
| `SprigTracesDropped` | ticket | More than 1,000 spans dropped in 15 minutes |
| `SprigStreamSequencersLagging` | ticket | A worker's stream sequencers have stayed further behind than `stream.sequencer_lag_alert_seconds` (60) for 10 minutes |
| `SprigThrashing` | ticket | The balancer reverted one of its own moves in the last 6 hours |

`BackupMissed` reads `sprig_backup_last_success_timestamp_seconds{cluster, kind}`, the time of each cluster's newest successful backup. SprigDB exports it in two places:

- **Compose VM bundle.** `sprig-vm.sh backup` records each completed backup in etcd, and every controller reads the records once a minute and exports the metric.
- **Kubernetes.** The leading SprigDB operator reads mariadb-operator's backup Jobs once a minute and exports it, with `kind="full"`, for each SprigCluster with `spec.backup`.

With other backup tooling, export the metric yourself with a `cluster` label, or the alert stays silent.

### Approvals (`sprig-approvals.rules.yaml`)

| Alert | Severity | Fires when |
|---|---|---|
| `SprigApprovalsWaitingLong` | ticket | A recommendation has waited more than 3 days for a decision |
| `SprigApprovalsNotifyFailing` | ticket | Notifications on a channel fail and none get through for 30 minutes |
| `SprigApprovalsApplyFailing` | ticket | An approved change failed to apply |

See [Approvals](../concepts/approvals.md).

## Dashboards

`deploy/dashboards/` has six Grafana dashboards. Import the JSON files (Dashboards, then Import, or file provisioning) and choose your Prometheus data source in the `datasource` variable. The SLO panels use the recording rules, so load `sprig-slo.rules.yaml` first.

| Dashboard | File | Variables | Shows |
|---|---|---|---|
| SprigDB fleet | `fleet.json` | `dc` | SLOs and burn rates, traffic by result and protocol, latency and overhead, the busiest tenants, gateway connections, refusals, admission waits, draining gateways, certificate expiry, metadata staleness, audit and traces |
| SprigDB data center | `dc.json` | `dc` | Traffic, error ratio, p99 by protocol, requests by API, leader changes, reconcile time, blocked loops, jobs, catalog cache, etcd health, size and fsync |
| SprigDB cluster | `cluster.json` | `cluster` | Health state, nodes up, semi-sync, replica lag, writer changes, GTID waits, backend pools, failures, probes, shard schemas |
| SprigDB tenant | `tenant.json` | `tenant` | One tenant's statements, errors and latency (for tenants above the series threshold), automatic indexes, jobs per hour |
| SprigDB data movement | `data-movement.json` | `dc` | Movement jobs, balancer moves, rows copied and applied, catch-up lag, cutover pauses, verify mismatches, DC migrations |
| SprigDB approvals | `approvals.json` | `dc` | Waiting recommendations, decisions, time to decision, notifications, applied changes |

The files are generated by `deploy/dashboards/gen.py`; `gen.py --check` fails if a file is stale or names a metric no component records.

## Logs

Every process logs to standard output, one event per line:

| Setting (`[telemetry]`) | Environment | Default | Meaning |
|---|---|---|---|
| `log_format` | `SPRIGDB_TELEMETRY_LOG_FORMAT` | `json` | `json`: one JSON object per line, for production. `pretty`: multi-line, human-readable, for development |
| `bootstrap_log_level` | `SPRIGDB_TELEMETRY_BOOTSTRAP_LOG_LEVEL` | `info` | A filter in `tracing` directive syntax: a level, or a level per component, such as `info,sprig_meta=debug` |

Both are process settings, read at start. A JSON line carries the timestamp, level, target (the component), the message and the event's fields, flattened, plus the current span:

```sh
journalctl -u sprigdb-gateway -o cat | jq 'select(.level == "WARN" or .level == "ERROR")'
docker compose logs -f gateway | jq .
```

[Error codes](../reference/errors.md) explains each SprigDB error number you find in a log line or a client error.

### Audit events

Audit events (logins, account and grant changes, operator actions, settings changes as `settings.changed`) are log lines with target `sprig_audit`:

```sh
journalctl -u sprigdb-controller -o cat | jq 'select(.target == "sprig_audit")'
```

To keep them durably, give each process an audit spool with `[audit] spool_dir` and a sink with `[audit] sink_dir`. Events are written to the spool first and shipped every second to the sink directory, under `audit/<stream>/<date>/`. If the spool passes `audit.spool_max_mb` (512), admin and account changes fail with 39970 rather than go unaudited. See [Audit](../security/audit.md).

## Tracing

SprigDB traces each request across the layers it passes through, inside one process: the gateway's request span and its MariaDB round trips.

- **Head sampling.** A request is traced with probability `telemetry.trace_sample_ratio` (1%), or when an incoming W3C `traceparent` says it was sampled upstream.
- **Tail sampling.** Every failed request, and every request slower than `telemetry.trace_slow_ms` (500 ms), is kept as well.
- **Propagation.** A statement on the MySQL listener (3306) takes its trace context from a query attribute named `traceparent`, which MySQL 8 clients can send. On a sampled statement, the gateway sends the context to MariaDB as a `/* traceparent='...' */` comment, so you can match slow-query-log entries to traces.
- **Where traces go.** The last 64 kept traces are served at `/debug/traces`. With `telemetry.otlp_endpoint` set, kept traces are sent once a second to `<endpoint>/v1/traces` as OTLP over HTTP with JSON encoding, which any OpenTelemetry collector accepts on port 4318. Up to 1,024 traces queue in memory; when the collector is down, a batch is retried for 30 seconds, then dropped and counted in `sprig_telemetry_spans_dropped_total`. The export is plain HTTP, so run the collector inside the data centre.

Spans carry ids and query fingerprints, never SQL text or values.

```sh
sprigctl settings set telemetry.otlp_endpoint http://otel-collector.monitoring:4318 --scope dc:fra1
sprigctl settings set telemetry.trace_slow_ms 200 --scope dc:fra1
```

## Telemetry settings

These are live settings: a change applies within seconds, without a restart.

| Setting | Type | Default | Scopes | Meaning |
|---|---|---|---|---|
| `telemetry.otlp_endpoint` | text | empty | dc, global (operator only) | OTLP/HTTP collector for traces, such as `http://host:4318`; empty sends nothing |
| `telemetry.trace_sample_ratio` | float, 0 to 1 | `0.01` | dc, global (operator only) | Head sampling of requests |
| `telemetry.trace_slow_ms` | int, 0 to 3,600,000 | `500` | dc, global (operator only) | Keep every slower request |
| `telemetry.tenant_series_min_rps` | int, 0 to 1,000,000 | `10` | dc, global (operator only) | Statements per second a tenant needs for its own metric series |

The telemetry address, log format and log filter are process settings in `[telemetry]`; see [Configuration](configuration.md).

## Related

- [Metrics](../reference/metrics.md)
- [Configuration](configuration.md)
- [Failover](failover.md)
- [Audit](../security/audit.md)
- [Troubleshooting](../troubleshooting/overview.md)
