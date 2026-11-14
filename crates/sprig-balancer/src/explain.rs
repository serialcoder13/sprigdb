//! `sprigctl explain placement` and `sprigctl explain decision`:
//! the tenant's shards with cluster, size and
//! load, each cluster's `full` with its bottleneck, and the last run's
//! reasoning, for example "cluster 7 above `balancer.high_pct` for 41 min:
//! hot; best move s3 to cluster 9, gain 0.28; blocked by shard cooldown
//! until 09:40".

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::time::{Duration, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, TenantId};

use crate::decisions::DecisionRow;
use crate::model::{ClusterState, FleetView, Timestamp};
use crate::plan::{PlanReport, Trigger};
use crate::settings::GB;

/// One of the tenant's shards.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardLine {
    /// `t42/g1/s3`.
    pub shard: String,
    /// Its cluster.
    pub cluster: String,
    /// Bytes.
    pub size_bytes: f64,
    /// Busy time, ms per second.
    pub busy_ms_per_sec: f64,
    /// Rows written per second.
    pub write_rows_per_sec: f64,
    /// Statements per second, as the gateways recorded them (0 when
    /// unknown).
    #[serde(default)]
    pub statements_per_sec: f64,
    /// Pinned.
    pub pinned: bool,
    /// Locked by a job (moving).
    pub locked: bool,
    /// In cooldown until, if it is.
    pub cooldown_until: Option<Timestamp>,
}

/// One cluster holding (or considered for) the tenant's shards.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterLine {
    /// The cluster.
    pub cluster: String,
    /// Its `full`.
    pub full: f64,
    /// What bounds it.
    pub bottleneck: String,
    /// `placeable`, `degraded`, `draining`, `unavailable`.
    pub state: String,
    /// Holds a shard of the tenant.
    pub holds_tenant: bool,
    /// Shard schemas and tables on it.
    pub schemas: f64,
    /// Tables on it.
    pub tables: f64,
}

/// `sprigctl explain placement <tenant>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlacementExplanation {
    /// The tenant.
    pub tenant: TenantId,
    /// When the view was taken.
    pub at: Timestamp,
    /// The tenant's `balancer.mode`.
    pub mode: String,
    /// Its shards.
    pub shards: Vec<ShardLine>,
    /// The DC's clusters, the tenant's first.
    pub clusters: Vec<ClusterLine>,
    /// The last run's reasoning that concerns the tenant.
    pub reasoning: Vec<String>,
    /// The weights the last run judged moves with.
    #[serde(default)]
    pub weights: Option<crate::judge::Weights>,
    /// The operator's pin, if the tenant is pinned.
    #[serde(default)]
    pub pin: Option<crate::pins::TenantPin>,
}

/// `HH:MM` UTC of `t`.
fn hhmm(t: Timestamp) -> String {
    let s = sprig_meta::bootstrap::rfc3339(UNIX_EPOCH + Duration::from_secs(t));
    s.get(11..16).map_or(s.clone(), str::to_owned)
}

/// Builds the explanation from the view and the last plan.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn explain_placement(
    view: &FleetView,
    last: Option<&PlanReport>,
    tenant: TenantId,
    mode: &str,
    now: Timestamp,
) -> PlacementExplanation {
    let mine: Vec<_> = view
        .shards
        .values()
        .filter(|s| s.key.tenant == tenant)
        .collect();
    let holding: BTreeSet<ClusterId> = mine.iter().map(|s| s.cluster.clone()).collect();
    let shards = mine
        .iter()
        .map(|s| ShardLine {
            shard: s.key.to_string(),
            cluster: s.cluster.to_string(),
            size_bytes: s.load.storage_bytes,
            busy_ms_per_sec: s.load.busy_ms_per_sec,
            write_rows_per_sec: s.load.write_rows_per_sec,
            statements_per_sec: 0.0,
            pinned: s.pinned,
            locked: s.locked,
            cooldown_until: (s.cooldown_until > now).then_some(s.cooldown_until),
        })
        .collect();
    let loads = view.loads();
    let mut clusters: Vec<ClusterLine> = view
        .clusters
        .values()
        .map(|c| {
            let l = loads.get(&c.id).copied().unwrap_or_default();
            let f = c.full_with(&l);
            ClusterLine {
                cluster: c.id.to_string(),
                full: f.value,
                bottleneck: f.bottleneck.to_string(),
                state: match c.state {
                    ClusterState::Placeable => "placeable",
                    ClusterState::Degraded => "degraded",
                    ClusterState::Draining => "draining",
                    ClusterState::Unavailable => "unavailable",
                }
                .to_owned(),
                holds_tenant: holding.contains(&c.id),
                schemas: l.schemas,
                tables: l.tables,
            }
        })
        .collect();
    clusters.sort_by(|a, b| {
        b.holds_tenant
            .cmp(&a.holds_tenant)
            .then(a.cluster.cmp(&b.cluster))
    });
    let mut reasoning = Vec::new();
    if let Some(r) = last {
        reasoning.push(format!(
            "last run at {}: spread {:.2}, trigger {}",
            hhmm(r.at),
            r.spread,
            match r.trigger {
                Trigger::None => "none",
                Trigger::Hot => "hot clusters",
                Trigger::Spread => "spread",
            }
        ));
        for h in r.hot.iter().chain(r.warming.iter()) {
            if !holding.contains(&h.cluster) {
                continue;
            }
            let minutes_hot = r.at.saturating_sub(h.since) / 60;
            let sustained = r.hot.iter().any(|x| x.cluster == h.cluster);
            reasoning.push(format!(
                "cluster {} at {:.2} above balancer.high_pct (or the fullest under spread) for {minutes_hot} min: {}",
                h.cluster,
                h.full,
                if sustained { "hot" } else { "waiting for balancer.sustain_minutes" }
            ));
        }
        for m in &r.moves {
            if m.tenant == tenant {
                reasoning.push(if m.why.is_empty() {
                    format!(
                        "move {} from {} to {}: {}, gain {:.2}, {:.1} GB",
                        m.shards[0],
                        m.from,
                        m.to,
                        m.reason.code(),
                        m.gain,
                        m.size_bytes / GB
                    )
                } else {
                    m.why.clone()
                });
                for c in m.considered.iter().filter(|c| c.tenant == tenant) {
                    reasoning.push(c.why.clone());
                }
            } else if holding.contains(&m.from) {
                reasoning.push(format!(
                    "best move off {} was another tenant's shard, gain {:.2}",
                    m.from, m.gain
                ));
            }
        }
        for n in &r.notes {
            let about_me = n.shard.is_some_and(|s| s.tenant == tenant)
                || n.cluster.as_ref().is_some_and(|c| holding.contains(c));
            if about_me {
                reasoning.push(n.text.clone());
            }
        }
        for p in r.pinned.iter().filter(|p| p.tenant == tenant) {
            reasoning.push(p.why.clone());
        }
        for c in r.rejected.iter().filter(|c| c.tenant == tenant) {
            reasoning.push(c.why.clone());
        }
        for a in &r.approvals {
            if a.shard.tenant == tenant {
                reasoning.push(format!(
                    "move {} to {} needs approval: {:.1} GB is above migration.auto_size_limit_gb",
                    a.shard,
                    a.to,
                    a.size_bytes / GB
                ));
            }
        }
    } else {
        reasoning.push("no planning run yet".to_owned());
    }
    for s in &mine {
        if s.cooldown_until > now {
            reasoning.push(format!(
                "{} is in shard cooldown until {} UTC",
                s.key,
                hhmm(s.cooldown_until)
            ));
        }
    }
    PlacementExplanation {
        tenant,
        at: now,
        mode: mode.to_owned(),
        shards,
        clusters,
        reasoning,
        weights: last.map(|r| r.weights),
        pin: view.tenants.get(&tenant).and_then(|t| t.pin.clone()),
    }
}

/// The text form of `explain placement`.
#[must_use]
pub fn placement_text(e: &PlacementExplanation) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "tenant {}  balancer.mode {}", e.tenant, e.mode);
    if let Some(p) = &e.pin {
        let _ = writeln!(s, "PINNED   {} since {}", p.describe(), hhmm(p.pinned_at));
    }
    let _ = writeln!(
        s,
        "\nSHARD                CLUSTER        SIZE_GB  STMTS/S  BUSY_MS/S  WRITES/S  STATE"
    );
    for l in &e.shards {
        let state = if l.locked {
            "moving".to_owned()
        } else if l.pinned {
            "pinned".to_owned()
        } else if let Some(t) = l.cooldown_until {
            format!("cooldown to {}", hhmm(t))
        } else {
            "-".to_owned()
        };
        let _ = writeln!(
            s,
            "{:<20} {:<14} {:>7.3} {:>8.1} {:>10.1} {:>9.1}  {state}",
            l.shard,
            l.cluster,
            l.size_bytes / GB,
            l.statements_per_sec,
            l.busy_ms_per_sec,
            l.write_rows_per_sec
        );
    }
    let _ = writeln!(
        s,
        "\nCLUSTER        FULL  BOTTLENECK   STATE        SCHEMAS   TABLES"
    );
    for c in &e.clusters {
        let _ = writeln!(
            s,
            "{:<14} {:>4.2}  {:<12} {:<12} {:>7.0} {:>8.0}{}",
            c.cluster,
            c.full,
            c.bottleneck,
            c.state,
            c.schemas,
            c.tables,
            if c.holds_tenant { "  *" } else { "" }
        );
    }
    if let Some(w) = &e.weights {
        let _ = writeln!(s, "\nWEIGHTS  {}", w.line());
    }
    let _ = writeln!(s, "\nREASONING");
    for r in &e.reasoning {
        let _ = writeln!(s, "- {r}");
    }
    s
}

/// The text form of `explain decision`.
#[must_use]
pub fn decision_text(r: &DecisionRow) -> String {
    let at = |us: i64| {
        let secs = u64::try_from(us / 1_000_000).unwrap_or(0);
        sprig_meta::bootstrap::rfc3339(UNIX_EPOCH + Duration::from_secs(secs))
    };
    let mut s = String::new();
    let _ = writeln!(
        s,
        "decision {}  {} {}",
        r.decision_id, r.d.component, r.d.kind
    );
    let _ = writeln!(s, "subject   {} {}", r.d.subject_type, r.d.subject_id);
    let _ = writeln!(
        s,
        "decided   {} by {} in {}",
        at(r.d.decided_at_us),
        r.d.actor,
        r.d.dc_id
    );
    let _ = writeln!(s, "mode      {}", r.d.mode);
    let _ = writeln!(
        s,
        "outcome   {} (updated {})",
        r.d.outcome,
        at(r.updated_at_us)
    );
    let _ = writeln!(s, "reason    {}", r.d.reason_code);
    let _ = writeln!(s, "summary   {}", r.d.summary);
    if let Some(p) = r.d.parent_id {
        let _ = writeln!(s, "parent    {p}");
    }
    let _ = writeln!(
        s,
        "evidence  {}",
        serde_json::to_string_pretty(&r.d.evidence).unwrap_or_default()
    );
    if let Some(a) = &r.d.alternatives {
        let _ = writeln!(
            s,
            "alternatives {}",
            serde_json::to_string_pretty(a).unwrap_or_default()
        );
    }
    s
}
