//! Text output of `sprigctl` (`-o table`, the default). `-o json` prints
//! the API's JSON instead. Every renderer returns a `String` so the golden
//! tests can compare it.

use std::fmt::Write as _;

use serde::Serialize;
use serde_json::Value;
use sprig_cluster::health::HealthReport;
use sprig_cluster::profile::{CapabilityProfile, Direction};
use sprig_controller::admin::types::{ClusterView, ProbeClusterResponse, TenantView};
use sprig_controller::dc::DcView;
use sprig_meta::bootstrap::DcRecord;
use sprig_provision::PrecheckReport;

/// A serde enum's name, such as `primary_replica` or `AFTER_SYNC`.
fn name<T: Serialize>(v: &T) -> String {
    match serde_json::to_value(v) {
        Ok(Value::String(s)) => s,
        Ok(Value::Null) => "-".to_owned(),
        Ok(other) => other.to_string(),
        Err(_) => "?".to_owned(),
    }
}

fn yes(b: bool) -> &'static str {
    if b { "yes" } else { "no" }
}

/// Columns padded to their widest cell; trailing spaces trimmed.
pub(crate) fn table<const N: usize>(header: [&str; N], rows: &[[String; N]]) -> String {
    let mut widths = header.map(str::len);
    for row in rows {
        for (w, cell) in widths.iter_mut().zip(row) {
            *w = (*w).max(cell.len());
        }
    }
    let mut s = String::new();
    let header = header.map(str::to_owned);
    for row in std::iter::once(&header).chain(rows) {
        let cells: Vec<String> = row
            .iter()
            .zip(widths)
            .map(|(c, w)| format!("{c:<w$}"))
            .collect();
        s.push_str(cells.join("  ").trim_end());
        s.push('\n');
    }
    s
}

fn tags(t: &serde_json::Map<String, Value>) -> String {
    if t.is_empty() {
        return "-".to_owned();
    }
    t.iter()
        .map(|(k, v)| {
            format!(
                "{k}={}",
                v.as_str().map_or_else(|| v.to_string(), str::to_owned)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// `sprigctl dc list`.
#[must_use]
pub(crate) fn dc_list(records: &[DcRecord]) -> String {
    let rows: Vec<[String; 6]> = records
        .iter()
        .map(|r| {
            [
                r.dc_id.to_string(),
                r.kind.clone(),
                r.state.clone(),
                r.dc_epoch.to_string(),
                yes(r.global_voter).to_owned(),
                tags(&r.tags),
            ]
        })
        .collect();
    table(["DC", "KIND", "STATE", "EPOCH", "VOTER", "TAGS"], &rows)
}

/// `sprigctl dc show`.
#[must_use]
pub(crate) fn dc_show(v: &DcView) -> String {
    let r = &v.record;
    let mut s = String::new();
    let _ = writeln!(s, "dc:            {}", r.dc_id);
    let _ = writeln!(s, "kind:          {}", r.kind);
    let _ = writeln!(s, "state:         {}", r.state);
    let _ = writeln!(s, "dc_epoch:      {}", r.dc_epoch);
    let _ = writeln!(s, "global_voter:  {}", r.global_voter);
    let _ = writeln!(s, "tags:          {}", tags(&r.tags));
    match &v.leader {
        Some(l) => {
            let _ = writeln!(
                s,
                "leader:        {} (token {}, build {})",
                l.holder, l.token, l.build
            );
        }
        None => s.push_str("leader:        none\n"),
    }
    s
}

/// `sprigctl cluster list`.
#[must_use]
pub(crate) fn cluster_list(clusters: &[ClusterView]) -> String {
    let rows: Vec<[String; 9]> = clusters
        .iter()
        .map(|c| {
            let r = &c.record;
            let d = r.profile.as_ref().map(|p| &p.derived);
            [
                r.cluster_id.to_string(),
                r.name.clone(),
                r.topology.to_string(),
                r.admin_state.to_string(),
                r.health
                    .as_ref()
                    .map_or("-".to_owned(), |h| h.state.to_string()),
                r.nodes.len().to_string(),
                if r.profile.is_some() {
                    format!("v{}", r.profile_version)
                } else {
                    "-".to_owned()
                },
                d.map_or("-", |d| yes(d.zero_loss_failover)).to_owned(),
                d.map_or("-", |d| yes(d.xa)).to_owned(),
            ]
        })
        .collect();
    table(
        [
            "ID",
            "NAME",
            "TOPOLOGY",
            "STATE",
            "HEALTH",
            "NODES",
            "PROFILE",
            "ZERO_LOSS",
            "XA",
        ],
        &rows,
    )
}

fn node_lines(c: &ClusterView, report: Option<&HealthReport>) -> Vec<String> {
    c.record
        .nodes
        .iter()
        .map(|n| {
            let a = report.and_then(|r| r.nodes.iter().find(|a| a.node == n.node));
            let mut line = format!("{} {}", n.node, n.addr);
            if let Some(z) = &n.zone {
                let _ = write!(line, " zone {z}");
            }
            if let Some(a) = a {
                let _ = write!(line, " [{}, {}", name(&a.state), name(&a.role));
                if let Some(l) = a.lag_ms {
                    let _ = write!(line, ", lag {l} ms");
                }
                if let Some(r) = &a.reason {
                    let _ = write!(line, ": {r}");
                }
                line.push(']');
            }
            line
        })
        .collect()
}

#[allow(clippy::too_many_lines)] // one line per profile field
fn profile_lines(p: &CapabilityProfile) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut add = |k: &str, v: String| out.push((k.to_owned(), v));
    let srv = &p.server;
    add(
        "server",
        format!(
            "{} (lower_case_table_names {}, {})",
            if srv.version_text.is_empty() {
                "-"
            } else {
                &srv.version_text
            },
            srv.lower_case_table_names,
            if srv.default_storage_engine.is_empty() {
                "-"
            } else {
                &srv.default_storage_engine
            }
        ),
    );
    if let Some(g) = &p.galera {
        add(
            "galera",
            format!(
                "{} nodes, {} synced, wsrep_gtid_mode {}, shared domain {}",
                g.size,
                g.synced,
                yes(g.gtid_mode),
                yes(g.shared_gtid_domain)
            ),
        );
    }
    let ss = &p.semi_sync;
    add(
        "semi_sync",
        if ss.enabled {
            format!(
                "enabled, {}, {}, {} client(s)",
                if ss.active {
                    "active"
                } else {
                    "fallen back to async"
                },
                ss.wait_point.as_ref().map_or("-".to_owned(), name),
                ss.clients
            )
        } else {
            "disabled".to_owned()
        },
    );
    let b = &p.binlog;
    add(
        "binlog",
        if b.log_bin {
            format!(
                "{}, row image {}, metadata {}, gtid_strict_mode {}, expire {} s{}",
                b.format.as_ref().map_or("-".to_owned(), name),
                b.row_image.as_ref().map_or("-".to_owned(), name),
                b.row_metadata.as_ref().map_or("-".to_owned(), name),
                yes(b.gtid_strict_mode),
                b.expire_logs_seconds,
                if b.in_files { "" } else { ", InnoDB-stored" }
            )
        } else {
            "off".to_owned()
        },
    );
    let d = &p.derived;
    add("xa", yes(d.xa).to_owned());
    add(
        "zero_loss_failover",
        format!(
            "{} (expected loss {})",
            yes(d.zero_loss_failover),
            name(&d.expected_loss)
        ),
    );
    add("read_your_writes", name(&d.ryw));
    add(
        "mover",
        format!(
            "source {}, target {}",
            yes(d.mover_source),
            yes(d.mover_target)
        ),
    );
    add(
        "change_feed",
        format!(
            "{}, retention {} s",
            name(&d.feed_level),
            d.feed_retention_secs
        ),
    );
    add(
        "fence_methods",
        if d.fence_methods.is_empty() {
            "-".to_owned()
        } else {
            d.fence_methods
                .iter()
                .map(name)
                .collect::<Vec<_>>()
                .join(", ")
        },
    );
    add(
        "validation",
        d.validation.as_ref().map_or("-".to_owned(), name),
    );
    add("online_index_build", yes(d.online_index_build).to_owned());
    add("enforce_json_schema", yes(d.enforce_json_schema).to_owned());
    add("compiled_logic", yes(d.compiled_logic).to_owned());
    out
}

/// `sprigctl cluster show`.
#[must_use]
pub(crate) fn cluster_show(c: &ClusterView) -> String {
    let r = &c.record;
    let mut s = String::new();
    let _ = writeln!(s, "cluster:      {} ({})", r.cluster_id, r.name);
    let _ = writeln!(s, "dc:           {}", r.dc_id);
    let _ = writeln!(s, "topology:     {}", r.topology);
    let _ = writeln!(s, "provisioner:  {}", name(&r.provisioner));
    let _ = writeln!(s, "state:        {}", r.admin_state);
    match &r.health {
        Some(h) => {
            let _ = writeln!(s, "health:       {} since {}", h.state, h.since);
        }
        None => s.push_str("health:       -\n"),
    }
    for (i, line) in node_lines(c, c.health_report.as_ref()).iter().enumerate() {
        let label = if i == 0 { "nodes:" } else { "" };
        let _ = writeln!(s, "{label:<14}{line}");
    }
    if let Some(g) = &r.galera {
        let writers: Vec<String> = g
            .writers
            .iter()
            .map(|w| format!("{} ({})", w.node, w.weight))
            .collect();
        let _ = writeln!(
            s,
            "writers:      epoch {}: {}",
            g.writer_epoch,
            writers.join(", ")
        );
    }
    match &r.profile {
        Some(p) => {
            let _ = writeln!(s, "profile:      v{}", r.profile_version);
            for (k, v) in profile_lines(p) {
                let _ = writeln!(s, "  {k:<22}{v}");
            }
        }
        None => s.push_str("profile:      none yet\n"),
    }
    if r.gaps.is_empty() {
        s.push_str("gaps:         none\n");
    } else {
        s.push_str("gaps:\n");
        for g in &r.gaps {
            let _ = writeln!(s, "  - {g}");
        }
    }
    if c.tuning_plan.is_empty() {
        s.push_str("tuning:       no plan yet (server tuning is chapter 09 step 10)\n");
    } else {
        s.push_str("tuning:\n");
        for t in &c.tuning_plan {
            let _ = writeln!(s, "  {t};");
        }
    }
    s
}

/// The precheck report of `register` (also its `--dry-run`).
#[must_use]
pub(crate) fn precheck(r: &PrecheckReport) -> String {
    let mut s = String::new();
    if r.passed() {
        s.push_str("prechecks:    passed\n");
    } else {
        let _ = writeln!(s, "prechecks:    failed ({} blocking)", r.blocking.len());
    }
    let rows: Vec<[String; 5]> = r
        .nodes
        .iter()
        .map(|n| {
            [
                n.node.to_string(),
                n.addr.clone(),
                n.role.clone().unwrap_or_else(|| "-".to_owned()),
                n.version.clone().unwrap_or_else(|| "-".to_owned()),
                n.tls_cipher
                    .clone()
                    .filter(|c| !c.is_empty())
                    .unwrap_or_else(|| "-".to_owned()),
            ]
        })
        .collect();
    for line in table(["NODE", "ADDRESS", "ROLE", "VERSION", "TLS"], &rows).lines() {
        let _ = writeln!(s, "  {line}");
    }
    for (title, items) in [("blocking", &r.blocking), ("advisories", &r.advisories)] {
        if items.is_empty() {
            continue;
        }
        let _ = writeln!(s, "{title}:");
        for i in items {
            let _ = writeln!(s, "  - {i}");
        }
    }
    s
}

/// `sprigctl cluster probe`.
#[must_use]
pub(crate) fn probe(r: &ProbeClusterResponse) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "probe:        {}", name(&r.depth));
    if r.changes.is_empty() {
        s.push_str("changes:      none\n");
    } else {
        s.push_str("changes:\n");
        for c in &r.changes {
            let dir = match c.direction {
                Direction::Upgrade => "upgrade",
                Direction::Downgrade => "downgrade",
                Direction::Changed => "changed",
            };
            let _ = writeln!(s, "  - {}: {} -> {} ({dir})", c.field, c.old, c.new);
        }
    }
    if !r.held.is_empty() {
        let _ = writeln!(
            s,
            "held:         {} (upgrades wait cluster.capability_upgrade_sustain_seconds)",
            r.held.join(", ")
        );
    }
    s.push_str(&cluster_show(&r.cluster));
    s
}

/// `sprigctl tenant list`.
#[must_use]
pub(crate) fn tenant_list(tenants: &[TenantView]) -> String {
    let rows: Vec<[String; 8]> = tenants
        .iter()
        .map(|t| {
            [
                t.tenant_id.to_string(),
                t.name.clone(),
                t.state.clone(),
                t.parent.clone().unwrap_or_else(|| "-".to_owned()),
                t.home_dc.to_string(),
                t.depth.to_string(),
                t.child_count.to_string(),
                format!("{}/{}", t.catalog_version, t.settings_version),
            ]
        })
        .collect();
    table(
        [
            "ID", "NAME", "STATE", "PARENT", "HOME", "DEPTH", "CHILDREN", "VERSIONS",
        ],
        &rows,
    )
}

/// `sprigctl tenant show` and `create`.
#[must_use]
pub(crate) fn tenant_show(t: &TenantView) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "tenant:            {} ({})", t.name, t.tenant_id);
    let _ = writeln!(s, "state:             {}", t.state);
    let _ = writeln!(s, "home dc:           {}", t.home_dc);
    let _ = writeln!(
        s,
        "parent:            {}",
        t.parent.as_deref().unwrap_or("- (a root tenant)")
    );
    if !t.ancestry.is_empty() {
        let _ = writeln!(s, "ancestry:          {}", t.ancestry.join(" > "));
    }
    let _ = writeln!(s, "path:              {} (depth {})", t.path, t.depth);
    let _ = writeln!(s, "children:          {}", t.child_count);
    let _ = writeln!(
        s,
        "catalog cluster:   {}",
        t.cluster.as_ref().map_or("-".to_owned(), ToString::to_string)
    );
    let _ = writeln!(s, "catalog version:   {}", t.catalog_version);
    let _ = writeln!(s, "auth version:      {}", t.auth_version);
    let _ = writeln!(s, "settings version:  {}", t.settings_version);
    if !t.created_at.is_empty() {
        let _ = writeln!(s, "created:           {}", t.created_at);
    }
    s
}
