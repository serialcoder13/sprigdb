#!/usr/bin/env python3
"""Generates SprigDB's Grafana dashboards and checks them.

    deploy/dashboards/gen.py          # write fleet.json, dc.json, cluster.json,
                                      # tenant.json, data-movement.json
    deploy/dashboards/gen.py --check  # fail if a file is stale, or if a
                                      # dashboard or alert rule names a
                                      # sprig_* metric no crate emits

The JSON files are what Grafana imports (Dashboards > Import, or file
provisioning); this script is their source. Every panel reads one
Prometheus data source, chosen with the `datasource` variable, and the
recording rules of deploy/alerts/sprig-slo.rules.yaml. The `dc` label is
the one each DC's Prometheus adds to its targets.
"""

import json
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent.parent

# Metrics that dashboards or alerts may name although no crate emits them
# yet. Keep it empty: a panel or alert on a metric nobody exports shows
# nothing and never fires (deploy/alerts/check-metrics.py checks the alerts
# the same way, including metrics of etcd).
LATER: dict[str, str] = {}

DS = {"type": "prometheus", "uid": "${datasource}"}


class Board:
    def __init__(self, uid, title, description, variables=()):
        self.uid, self.title, self.description = uid, title, description
        self.variables = list(variables)
        self.panels = []
        self.annotations = []
        self.x = self.y = 0
        self.row_h = 0

    def annotation(self, name, expr, title, text, color="#73BF69"):
        """A Prometheus annotation query: a marker wherever `expr` has a value."""
        self.annotations.append({
            "name": name, "enable": True, "iconColor": color, "datasource": DS,
            "expr": expr, "step": "60s", "titleFormat": title, "textFormat": text,
            "useValueForTime": False,
        })

    def row(self, title):
        if self.x:
            self.y += self.row_h
            self.x = self.row_h = 0
        self.panels.append({
            "type": "row", "title": title, "collapsed": False,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": self.y},
            "id": len(self.panels) + 1, "panels": [],
        })
        self.y += 1

    def panel(self, title, targets, unit="short", kind="timeseries", w=8, h=8,
              description="", thresholds=None, max_=None):
        if self.x + w > 24:
            self.y += self.row_h
            self.x = self.row_h = 0
        defaults = {"unit": unit}
        if thresholds:
            defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
        if max_ is not None:
            defaults["max"] = max_
        p = {
            "type": kind,
            "title": title,
            "description": description,
            "datasource": DS,
            "gridPos": {"h": h, "w": w, "x": self.x, "y": self.y},
            "id": len(self.panels) + 1,
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}}
            if kind == "timeseries" else {"reduceOptions": {"calcs": ["lastNotNull"]}},
            "targets": [
                {"refId": chr(ord("A") + i), "expr": e, "legendFormat": l, "datasource": DS}
                for i, (e, l) in enumerate(targets)
            ],
        }
        self.panels.append(p)
        self.x += w
        self.row_h = max(self.row_h, h)

    def json(self):
        templating = [{
            "name": "datasource", "label": "Prometheus", "type": "datasource",
            "query": "prometheus", "current": {}, "hide": 0,
        }]
        for name, query, multi in self.variables:
            templating.append({
                "name": name, "label": name, "type": "query", "datasource": DS,
                "query": {"query": query, "refId": name}, "definition": query,
                "refresh": 2, "includeAll": multi, "multi": multi,
                "allValue": ".*" if multi else None, "current": {}, "hide": 0,
                "sort": 1,
            })
        return {
            "uid": self.uid,
            "title": self.title,
            "description": self.description,
            "tags": ["sprigdb"],
            "editable": True,
            "schemaVersion": 39,
            "version": 1,
            "refresh": "30s",
            "time": {"from": "now-6h", "to": "now"},
            "timezone": "utc",
            "templating": {"list": templating},
            "annotations": {"list": self.annotations},
            "links": [{"type": "dashboards", "tags": ["sprigdb"], "asDropdown": True,
                       "title": "SprigDB"}],
            "panels": self.panels,
        }


DC_VAR = ("dc", "label_values(sprig_gateway_statements_total, dc)", True)
GREEN_RED = lambda red: [{"color": "green", "value": None}, {"color": "red", "value": red}]


def fleet():
    b = Board("sprig-fleet", "SprigDB fleet",
              "Every DC: availability and overhead SLOs, traffic, gateways, audit and traces.",
              [DC_VAR])
    sel = 'dc=~"$dc"'
    b.row("SLOs")
    b.panel("Availability (window)", [(
        f'1 - sum(increase(sprig_gateway_statements_total{{{sel},result=~"sprig|backend"}}[$__range])) '
        f'/ sum(increase(sprig_gateway_statements_total{{{sel}}}[$__range]))', "availability")],
        unit="percentunit", kind="stat", w=6, h=5,
        description="Target 99.95% over 30 days: 1 - (sprig + backend) / all statements.")
    b.panel("Overhead under 2 ms (window)", [(
        f'sum(increase(sprig_gateway_overhead_seconds_bucket{{{sel},le="0.002"}}[$__range])) '
        f'/ sum(increase(sprig_gateway_overhead_seconds_count{{{sel}}}[$__range]))', "under 2 ms")],
        unit="percentunit", kind="stat", w=6, h=5, description="Target 99%.")
    b.panel("Availability burn rate (1h)", [(
        f'sprig:availability_errors:ratio_rate1h{{{sel}}} / 0.0005', "{{dc}}")],
        unit="x", kind="stat", w=6, h=5, thresholds=GREEN_RED(6),
        description="Pages at 14.4 (with the 5-minute window) and 6 (6h with 30m).")
    b.panel("Overhead burn rate (1h)", [(
        f'sprig:overhead_slow:ratio_rate1h{{{sel}}} / 0.01', "{{dc}}")],
        unit="x", kind="stat", w=6, h=5, thresholds=GREEN_RED(6))
    b.row("Traffic")
    b.panel("Statements by result", [(
        f'sum by (result) (rate(sprig_gateway_statements_total{{{sel}}}[5m]))', "{{result}}")],
        unit="ops", w=12)
    b.panel("Statements by protocol", [(
        f'sum by (protocol) (rate(sprig_gateway_statements_total{{{sel}}}[5m]))', "{{protocol}}")],
        unit="ops", w=12)
    b.panel("Statement latency", [
        (f'histogram_quantile(0.5, sum by (le) (rate(sprig_gateway_query_duration_seconds_bucket{{{sel}}}[5m])))', "p50"),
        (f'histogram_quantile(0.99, sum by (le) (rate(sprig_gateway_query_duration_seconds_bucket{{{sel}}}[5m])))', "p99"),
    ], unit="s")
    b.panel("Gateway overhead", [
        (f'histogram_quantile(0.5, sum by (le) (rate(sprig_gateway_overhead_seconds_bucket{{{sel}}}[5m])))', "p50"),
        (f'histogram_quantile(0.99, sum by (le) (rate(sprig_gateway_overhead_seconds_bucket{{{sel}}}[5m])))', "p99"),
    ], unit="s", description="Time in the gateway itself: admission, parsing, routing; not MariaDB.")
    b.panel("Busiest tenants", [(
        f'topk(10, sum by (tenant) (rate(sprig_gateway_statements_total{{{sel},tenant!=""}}[5m])))',
        "tenant {{tenant}}")], unit="ops",
        description="Tenants above telemetry.tenant_series_min_rps have their own series.")
    b.row("Gateways")
    b.panel("Open connections", [(
        f'sum by (instance) (sprig_gateway_connections{{{sel},state="open"}})', "{{instance}}")])
    b.panel("Connections refused", [(
        f'sum by (reason) (rate(sprig_gateway_connections_rejected_total{{{sel}}}[5m]))', "{{reason}}")],
        unit="ops")
    b.panel("Admission wait p99", [(
        f'histogram_quantile(0.99, sum by (le) (rate(sprig_gateway_admission_wait_seconds_bucket{{{sel}}}[5m])))', "p99")],
        unit="s")
    b.panel("Draining gateways", [(f'sum(sprig_gateway_draining{{{sel}}})', "draining")], kind="stat", w=6, h=5)
    b.panel("Certificate expiry", [(
        f'min by (instance, bundle) (sprig_gateway_cert_expiry_seconds{{{sel}}})', "{{instance}} {{bundle}}")],
        unit="s", kind="stat", w=6, h=5, thresholds=[{"color": "red", "value": None},
                                                    {"color": "green", "value": 14 * 86400}])
    b.panel("Metadata staleness", [(
        f'max(sprig_meta_cache_staleness_seconds{{{sel}}})', "max")], unit="s", w=12, h=5,
        thresholds=GREEN_RED(2))
    b.row("Audit and traces")
    b.panel("Audit spool", [(
        f'max by (instance) (sprig_audit_spool_bytes{{{sel}}} / sprig_audit_spool_max_bytes{{{sel}}})',
        "{{instance}}")], unit="percentunit", max_=1, thresholds=GREEN_RED(0.5))
    b.panel("Audit events", [
        (f'sum by (stream) (rate(sprig_audit_events_total{{{sel}}}[5m]))', "{{stream}}"),
        (f'sum(rate(sprig_audit_dropped_total{{{sel}}}[5m]))', "dropped"),
    ], unit="ops")
    b.panel("Traces kept and spans dropped", [
        (f'sum by (reason) (rate(sprig_telemetry_traces_kept_total{{{sel}}}[5m]))', "kept: {{reason}}"),
        (f'sum(rate(sprig_telemetry_spans_dropped_total{{{sel}}}[5m]))', "spans dropped"),
    ], unit="ops")
    return b


def dc():
    b = Board("sprig-dc", "SprigDB data center", "One DC: traffic, control plane, etcd, jobs.",
              [("dc", "label_values(sprig_gateway_statements_total, dc)", False)])
    sel = 'dc="$dc"'
    b.row("Traffic")
    b.panel("Statements by result", [(
        f'sum by (result) (rate(sprig_gateway_statements_total{{{sel}}}[5m]))', "{{result}}")], unit="ops", w=12)
    b.panel("Error ratio (sprig + backend)", [
        (f'sprig:availability_errors:ratio_rate5m{{{sel}}}', "5m"),
        (f'sprig:availability_errors:ratio_rate1h{{{sel}}}', "1h"),
    ], unit="percentunit", w=12)
    b.panel("Statement latency p99 by protocol", [(
        f'histogram_quantile(0.99, sum by (le, protocol) (rate(sprig_gateway_query_duration_seconds_bucket{{{sel}}}[5m])))',
        "{{protocol}}")], unit="s", w=12)
    b.panel("Requests by API", [(
        f'sum by (service, method) (rate(sprig_api_requests_total{{{sel}}}[5m]))', "{{service}} {{method}}")],
        unit="ops", w=12)
    b.row("Control plane")
    b.panel("Leader changes", [(
        f'sum by (tier) (increase(sprig_controller_leader_changes_total{{{sel}}}[1h]))', "{{tier}}")])
    b.panel("Reconcile duration p99", [(
        f'max by (loop) (sprig_controller_reconcile_duration_seconds{{{sel},quantile="0.99"}})',
        "{{loop}}")], unit="s", description="A summary: the 0.99 quantile each controller reports.")
    b.panel("Loops blocked", [(f'sum by (loop) (sprig_controller_loop_blocked{{{sel}}})', "{{loop}}")])
    b.panel("Jobs by state", [(f'sum by (kind, state) (sprig_controller_jobs{{{sel}}})', "{{kind}} {{state}}")], w=12)
    b.panel("Catalog cache", [
        (f'sum(sprig_catalog_cache_tenants{{{sel}}})', "tenants"),
        (f'max(sprig_catalog_version_lag_seconds{{{sel}}})', "version lag (s)"),
    ], w=12)
    b.row("etcd")
    b.panel("Has leader", [('min by (instance) (etcd_server_has_leader)', "{{instance}}")], kind="stat", w=8, h=5)
    b.panel("DB size vs quota", [(
        'max by (instance) (etcd_mvcc_db_total_size_in_bytes / etcd_server_quota_backend_bytes)', "{{instance}}")],
        unit="percentunit", w=8, h=5, thresholds=GREEN_RED(0.8))
    b.panel("WAL fsync p99", [(
        'histogram_quantile(0.99, sum by (le, instance) (rate(etcd_disk_wal_fsync_duration_seconds_bucket[5m])))',
        "{{instance}}")], unit="s", w=8, h=5, thresholds=GREEN_RED(0.01))
    return b


def cluster():
    b = Board("sprig-cluster", "SprigDB cluster", "One MariaDB cluster: health, nodes, lag, pools.",
              [("cluster", "label_values(sprig_backend_node_up, cluster)", False)])
    sel = 'cluster="$cluster"'
    b.row("Health")
    b.panel("Health state", [(f'max(sprig_cluster_health_state{{{sel}}})', "state")], kind="stat", w=6, h=5,
            description="0 healthy, 1 degraded, 2 no writer, 3 unknown.",
            thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1},
                        {"color": "red", "value": 2}])
    b.panel("Nodes up", [(f'min by (node) (sprig_backend_node_up{{{sel}}})', "{{node}}")], kind="stat", w=6, h=5)
    b.panel("Semi-sync", [(f'max(sprig_cluster_semi_sync_status{{{sel}}})', "active")], kind="stat", w=6, h=5)
    b.panel("Disk full (placement)", [(f'max(sprig_balancer_cluster_full_ratio{{{sel}}})', "full")],
            unit="percentunit", kind="stat", w=6, h=5, thresholds=GREEN_RED(0.8))
    b.row("Replication and routing")
    b.panel("Replica lag", [(f'max by (node) (sprig_backend_replica_lag_seconds{{{sel}}})', "{{node}}")],
            unit="s", w=12, thresholds=GREEN_RED(30))
    b.panel("Writer changes and GTID wait timeouts", [
        (f'sum(increase(sprig_backend_writer_changes_total{{{sel}}}[1h]))', "writer changes"),
        (f'sum(rate(sprig_backend_gtid_wait_timeouts_total{{{sel}}}[5m]))', "GTID wait timeouts/s"),
    ], w=12)
    b.row("Pools")
    b.panel("Pool connections", [(f'sum by (state) (sprig_backend_pool_connections{{{sel}}})', "{{state}}")])
    b.panel("Pool wait p99", [(
        f'histogram_quantile(0.99, sum by (le) (rate(sprig_backend_pool_wait_seconds_bucket{{{sel}}}[5m])))', "p99")],
        unit="s")
    b.panel("Backend failures", [(f'sum by (kind) (rate(sprig_backend_failures_total{{{sel}}}[5m]))', "{{kind}}")],
            unit="ops")
    b.panel("State syncs", [(f'sum by (kind) (rate(sprig_backend_state_syncs_total{{{sel}}}[5m]))', "{{kind}}")],
            unit="ops")
    b.panel("Probe duration and failures", [
        (f'max(sprig_cluster_probe_duration_seconds{{{sel},quantile="0.99"}})', "probe p99 (s)"),
        (f'sum(rate(sprig_cluster_probe_failures_total{{{sel}}}[5m]))', "failures/s"),
    ])
    b.panel("Shard schemas", [(f'max(sprig_balancer_shard_schemas{{{sel}}})', "schemas")])
    return b


def tenant():
    b = Board("sprig-tenant", "SprigDB tenant",
              "One busy tenant (above telemetry.tenant_series_min_rps): its statements, latency, indexes.",
              [("tenant", 'label_values(sprig_gateway_statements_total{tenant!=""}, tenant)', False)])
    sel = 'tenant="$tenant"'
    b.row("Statements")
    b.panel("Statements by result", [(
        f'sum by (result) (rate(sprig_gateway_statements_total{{{sel}}}[5m]))', "{{result}}")], unit="ops", w=12)
    b.panel("Error ratio", [(
        f'sum(rate(sprig_gateway_statements_total{{{sel},result=~"sprig|backend"}}[5m])) '
        f'/ sum(rate(sprig_gateway_statements_total{{{sel}}}[5m]))', "sprig + backend")],
        unit="percentunit", w=12)
    b.panel("Latency", [
        (f'histogram_quantile(0.5, sum by (le) (rate(sprig_gateway_query_duration_seconds_bucket{{{sel}}}[5m])))', "p50"),
        (f'histogram_quantile(0.99, sum by (le) (rate(sprig_gateway_query_duration_seconds_bucket{{{sel}}}[5m])))', "p99"),
    ], unit="s", w=12)
    b.panel("Client errors", [(
        f'sum by (protocol) (rate(sprig_gateway_statements_total{{{sel},result="client"}}[5m]))', "{{protocol}}")],
        unit="ops", w=12)
    b.row("Automation (all tenants)")
    b.panel("Automatic indexes", [
        ('sum by (status) (rate(sprig_advisor_autoindex_candidates_total[1h]))', "candidates {{status}}"),
        ('sum by (reason) (increase(sprig_advisor_autoindex_drops_total[1d]))', "drops {{reason}}"),
    ], w=12)
    b.panel("Jobs per tenant per hour (max)", [('max(sprig_controller_tenant_jobs_per_hour_max)', "max")], w=12)
    return b


def movement():
    b = Board("sprig-data-movement", "SprigDB data movement",
              "Shard moves, splits and DC migrations: progress, catch-up, cutover pauses, throttling.",
              [DC_VAR])
    sel = 'dc=~"$dc"'
    b.row("Jobs")
    b.panel("Movement jobs by state", [(
        f'sum by (kind, state) (sprig_controller_jobs{{{sel},kind=~"move_shard|split_shard|merge_shards|rekey|tenant_migration"}})',
        "{{kind}} {{state}}")], w=12)
    b.panel("Balancer moves", [
        (f'sum(increase(sprig_balancer_moves_total{{{sel}}}[1h]))', "moves/h"),
        (f'sum(increase(sprig_balancer_moves_reverted_total{{{sel}}}[6h]))', "reverted/6h"),
    ], w=12)
    b.row("Copy and catch-up")
    b.panel("Rows copied and applied", [
        (f'sum(rate(sprig_mover_copy_rows_total{{{sel}}}[5m]))', "copied/s"),
        (f'sum(rate(sprig_mover_apply_rows_total{{{sel}}}[5m]))', "applied/s"),
    ], unit="ops")
    b.panel("Catch-up lag", [(f'max(sprig_mover_catchup_lag_seconds{{{sel}}})', "lag")], unit="s",
            thresholds=GREEN_RED(600))
    b.panel("Throttled", [(f'sum(rate(sprig_mover_throttled_milliseconds_total{{{sel}}}[5m])) / 1000', "throttled s/s")],
            unit="percentunit")
    b.row("Cutover")
    b.panel("Cutover pause p99", [(
        f'histogram_quantile(0.99, sum by (le) (rate(sprig_mover_cutover_hold_seconds_bucket{{{sel}}}[1h])))', "p99")],
        unit="s", thresholds=GREEN_RED(5))
    b.panel("Cutovers", [(f'sum by (outcome) (increase(sprig_mover_cutovers_total{{{sel}}}[1h]))', "{{outcome}}")])
    b.panel("Errors, retries, verify mismatches", [
        (f'sum by (code) (increase(sprig_mover_errors_total{{{sel}}}[1h]))', "error {{code}}"),
        (f'sum(increase(sprig_mover_chunk_retries_total{{{sel}}}[1h]))', "chunk retries"),
        (f'sum(increase(sprig_mover_verify_mismatches_total{{{sel}}}[1h]))', "verify mismatches"),
    ])
    b.row("DC migrations")
    b.panel("Migration write pause", [(
        f'max(sprig_controller_migration_write_pause_seconds{{{sel}}})', "pause")], unit="s", w=12,
        thresholds=GREEN_RED(5))
    b.panel("Migrations", [(
        f'sum by (result) (increase(sprig_controller_migrations_total{{{sel}}}[1d]))', "{{result}}")], w=12)
    return b


def approvals():
    b = Board("sprig-approvals", "SprigDB approvals",
              "Recommendations waiting for approval, decisions, escalations, "
              "auto-applies, applied changes and notifications.", [DC_VAR])
    sel = 'dc=~"$dc"'
    b.annotation("Approved changes applied",
                 f'sum by (kind, result) (increase(sprig_approvals_applied_total{{{sel}}}[2m])) > 0',
                 "{{kind}} applied", "result {{result}}")
    b.annotation("Emergencies",
                 f'sum by (kind) (increase(sprig_approvals_actions_total{{{sel},action="emergency"}}[2m])) > 0',
                 "{{kind}} emergency", "applied without waiting", color="#F2495C")
    b.row("Waiting")
    b.panel("Waiting recommendations", [(
        f'sum by (kind, level) (sprig_approvals_open{{{sel}}})', "{{kind}} {{level}}")], w=12)
    b.panel("Oldest waiting", [(
        f'max by (kind) (sprig_approvals_oldest_open_seconds{{{sel}}})', "{{kind}}")], unit="s", w=12,
        thresholds=GREEN_RED(3 * 86400))
    b.row("Decisions")
    b.panel("Raised", [(f'sum by (kind) (increase(sprig_approvals_raised_total{{{sel}}}[1h]))', "{{kind}}")])
    b.panel("Actions", [(
        f'sum by (action) (increase(sprig_approvals_actions_total{{{sel}}}[1h]))', "{{action}}")])
    b.panel("Time to decision p90", [(
        f'histogram_quantile(0.9, sum by (le, kind) (rate(sprig_approvals_decision_seconds_bucket{{{sel}}}[1d])))',
        "{{kind}}")], unit="s")
    b.panel("Suppressed by an ignore", [(
        f'sum by (kind) (increase(sprig_approvals_suppressed_total{{{sel}}}[1h]))', "{{kind}}")])
    b.panel("Applied", [(
        f'sum by (kind, result) (increase(sprig_approvals_applied_total{{{sel}}}[1h]))', "{{kind}} {{result}}")])
    b.panel("Notifications", [(
        f'sum by (channel, result) (increase(sprig_approvals_notifications_total{{{sel}}}[1h]))',
        "{{channel}} {{result}}")])
    return b


BOARDS = {
    "approvals.json": approvals,
    "fleet.json": fleet,
    "dc.json": dc,
    "cluster.json": cluster,
    "tenant.json": tenant,
    "data-movement.json": movement,
}


def emitted_metrics():
    """Every sprig_* name a crate emits (counter!, gauge!, histogram!)."""
    names = set()
    pat = re.compile(r'(?:counter|gauge|histogram)!\(\s*"(sprig_[a-z0-9_]+)"')
    for f in (ROOT / "crates").rglob("*.rs"):
        names.update(pat.findall(f.read_text(errors="replace")))
    return names


def referenced(text):
    names = set(re.findall(r"\bsprig_[a-z0-9_]+", text))
    # Histogram series and recording-rule names aren't metrics of their own.
    out = set()
    for n in names:
        for suf in ("_bucket", "_count", "_sum"):
            if n.endswith(suf) and n[: -len(suf)].endswith("_seconds"):
                n = n[: -len(suf)]
        out.add(n)
    return out


def main():
    check = "--check" in sys.argv[1:]
    failed = False
    for name, build in BOARDS.items():
        text = json.dumps(build().json(), indent=2, sort_keys=False) + "\n"
        path = HERE / name
        if check:
            if not path.exists() or path.read_text() != text:
                print(f"stale: {path.relative_to(ROOT)} (run deploy/dashboards/gen.py)")
                failed = True
        else:
            path.write_text(text)
            print(f"wrote {path.relative_to(ROOT)}")
    if check:
        emitted = emitted_metrics()
        sources = [HERE / n for n in BOARDS] + sorted((ROOT / "deploy" / "alerts").glob("*.rules.yaml"))
        for src in sources:
            if not src.exists():
                continue
            for m in sorted(referenced(src.read_text())):
                if m not in emitted and m not in LATER:
                    print(f"{src.relative_to(ROOT)}: {m} is emitted by no crate")
                    failed = True
        if not failed:
            print(f"dashboards up to date; every sprig_* metric they and the alert rules use is emitted "
                  f"({len(emitted)} metrics in the code)")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
