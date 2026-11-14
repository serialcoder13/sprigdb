//! Hard constraints and `feasible()`, and the co-write rule that binds groups into units.
//!
//! Every cluster that fails a constraint is returned with the first
//! [`Blocker`] found, so placement errors and `sprigctl explain placement`
//! can name it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, GroupId, TenantId};

use crate::model::{ClusterState, ClusterView, FleetView, LoadVector, ShardKey, Timestamp};

/// Why a placement or move is being checked; some constraints exempt
/// residency and operator moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    /// A new shard.
    Place,
    /// A rebalancing move.
    Rebalance,
    /// A residency rule tightened: skips cooldowns and stability.
    Residency,
    /// A cluster is draining: its shards must go.
    Drain,
    /// `sprigctl shard move --to`: the operator decides; only residency,
    /// state, capabilities and storage apply.
    Operator,
}

/// Shards that move together, with their summed load.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Unit {
    /// The tenant.
    pub tenant: TenantId,
    /// The shards.
    pub shards: Vec<ShardKey>,
    /// Their load (predicted, for new shards).
    pub load: LoadVector,
    /// Where they are now; `None` for new shards.
    pub from: Option<ClusterId>,
}

/// The first hard constraint a cluster fails.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Blocker {
    /// The cluster's DC or a replica's DC is not allowed for the tenant.
    Residency,
    /// Not `active` and `healthy` (or draining, degraded).
    State {
        /// The state.
        state: String,
    },
    /// Probe data is stale.
    StaleProbe,
    /// A feature the tenant needs, or `mover_target` / `mover_source`.
    Capability {
        /// Which.
        missing: String,
    },
    /// Storage after the move above `balancer.storage_hard_pct`.
    Storage,
    /// Tables after the move above `placement.max_tables_per_cluster`.
    TableCap,
    /// Shard schemas after the move above
    /// `placement.max_shard_schemas_per_cluster`.
    SchemaCap,
    /// The cluster's tuning reports it over its table budget.
    TableBudget,
    /// Table-cache misses keep rising after tuning.
    CacheThrashing,
    /// Returned from an outage or failover less than
    /// `recovery.stability_minutes` ago.
    Stability {
        /// When it becomes eligible.
        until: Timestamp,
    },
    /// A move there failed; excluded for one cooldown.
    Excluded {
        /// Until when.
        until: Timestamp,
    },
    /// The cluster the unit is on.
    Source,
}

impl Blocker {
    /// A short code for metrics, decisions and recommendations.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Blocker::Residency => "residency",
            Blocker::State { .. } => "state",
            Blocker::StaleProbe => "stale_probe",
            Blocker::Capability { .. } => "capability",
            Blocker::Storage => "storage",
            Blocker::TableCap => "table_cap",
            Blocker::SchemaCap => "schema_cap",
            Blocker::TableBudget => "table_budget",
            Blocker::CacheThrashing => "cache_thrashing",
            Blocker::Stability { .. } => "stability",
            Blocker::Excluded { .. } => "excluded",
            Blocker::Source => "source",
        }
    }

    /// Whether the blocker is about the schema cap (an `add_cluster`
    /// recommendation with reason `schema_cap`).
    #[must_use]
    pub fn is_schema_cap(&self) -> bool {
        matches!(
            self,
            Blocker::TableCap | Blocker::SchemaCap | Blocker::TableBudget | Blocker::CacheThrashing
        )
    }
}

impl fmt::Display for Blocker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Blocker::Residency => f.write_str("residency does not allow its DCs"),
            Blocker::State { state } => write!(f, "cluster is {state}"),
            Blocker::StaleProbe => f.write_str("probe data is stale"),
            Blocker::Capability { missing } => write!(f, "lacks {missing}"),
            Blocker::Storage => f.write_str("storage would pass balancer.storage_hard_pct"),
            Blocker::TableCap => f.write_str("tables would pass placement.max_tables_per_cluster"),
            Blocker::SchemaCap => {
                f.write_str("shard schemas would pass placement.max_shard_schemas_per_cluster")
            }
            Blocker::TableBudget => f.write_str("over its table budget"),
            Blocker::CacheThrashing => f.write_str("table-cache misses keep rising"),
            Blocker::Stability { until } => {
                write!(f, "recovering: stable from {until}")
            }
            Blocker::Excluded { until } => write!(f, "a move there failed; excluded until {until}"),
            Blocker::Source => f.write_str("already holds it"),
        }
    }
}

/// A feasible cluster: its `full` and utilization after taking the unit.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Fit {
    /// `full` after.
    pub full_after: f64,
    /// Storage utilization after.
    pub storage_after: f64,
    /// Busy utilization after.
    pub busy_after: f64,
    /// Connection utilization after.
    pub connections_after: f64,
    /// Pressure (unchanged by the unit).
    pub pressure: f64,
}

/// The result of `feasible()`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Feasibility {
    /// Clusters that pass, in cluster-id order.
    pub ok: Vec<(ClusterId, Fit)>,
    /// Clusters that fail, with the first blocker.
    pub blocked: Vec<(ClusterId, Blocker)>,
}

impl Feasibility {
    /// The blocker most clusters failed on (for 39120's message).
    #[must_use]
    pub fn main_blocker(&self) -> Option<&Blocker> {
        let mut counts: Vec<(&Blocker, usize)> = Vec::new();
        for (_, b) in &self.blocked {
            if matches!(b, Blocker::Source) {
                continue;
            }
            match counts.iter_mut().find(|(x, _)| x.code() == b.code()) {
                Some((_, n)) => *n += 1,
                None => counts.push((b, 1)),
            }
        }
        counts.into_iter().max_by_key(|(_, n)| *n).map(|(b, _)| b)
    }
}

/// Checks every cluster of `view` for `unit` against the hard
/// constraints; the co-write rule is in [`unit_of`].
#[must_use]
pub fn feasible(view: &FleetView, unit: &Unit, purpose: Purpose, now: Timestamp) -> Feasibility {
    feasible_in(view, &view.loads(), unit, purpose, now)
}

/// [`feasible`] with every cluster's load already summed (the planner
/// keeps them up to date as it plans moves).
#[must_use]
pub fn feasible_in(
    view: &FleetView,
    loads: &BTreeMap<ClusterId, LoadVector>,
    unit: &Unit,
    purpose: Purpose,
    now: Timestamp,
) -> Feasibility {
    let tenant = view.tenants.get(&unit.tenant);
    let mut out = Feasibility::default();
    for (id, c) in &view.clusters {
        let base = loads.get(id).copied().unwrap_or_default();
        match check(c, base, unit, tenant, purpose, now, view) {
            Ok(fit) => out.ok.push((id.clone(), fit)),
            Err(b) => out.blocked.push((id.clone(), b)),
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn check(
    c: &ClusterView,
    base: LoadVector,
    unit: &Unit,
    tenant: Option<&crate::model::TenantView>,
    purpose: Purpose,
    now: Timestamp,
    view: &FleetView,
) -> Result<Fit, Blocker> {
    if unit.from.as_ref() == Some(&c.id) {
        return Err(Blocker::Source);
    }
    // 1. Residency: the cluster's DC and every replica DC.
    if let Some(allowed) = tenant.and_then(|t| t.allowed_dcs.as_ref())
        && (!allowed.contains(&c.dc) || c.replica_dcs.iter().any(|d| !allowed.contains(d)))
    {
        return Err(Blocker::Residency);
    }
    // 2. State.
    if c.state != ClusterState::Placeable {
        return Err(Blocker::State {
            state: match c.state {
                ClusterState::Degraded => "degraded",
                ClusterState::Draining => "draining",
                _ => "unavailable",
            }
            .to_owned(),
        });
    }
    if c.probe_stale {
        return Err(Blocker::StaleProbe);
    }
    // 3. Capabilities: features, and the mover's flags for moves.
    if let Some(t) = tenant
        && let Some(f) = t.needs.iter().find(|f| !c.caps.features.contains(*f))
    {
        return Err(Blocker::Capability { missing: f.clone() });
    }
    if purpose != Purpose::Place {
        if !c.caps.mover_target {
            return Err(Blocker::Capability {
                missing: "mover_target".to_owned(),
            });
        }
        if let Some(src) = unit.from.as_ref().and_then(|f| view.clusters.get(f))
            && !src.caps.mover_source
        {
            return Err(Blocker::Capability {
                missing: "mover_source on the source".to_owned(),
            });
        }
    }
    let after = base + unit.load;
    // 4. Storage.
    let storage_after = c.util_of(&after, crate::model::Dim::Storage);
    if c.cap.storage_bytes > 0.0 && storage_after > c.limits.storage_hard {
        return Err(Blocker::Storage);
    }
    // 6. Schema cap and table budget (operators may override).
    if purpose != Purpose::Operator {
        #[allow(clippy::cast_precision_loss)]
        if after.tables > c.limits.max_tables as f64 && unit.load.tables > 0.0 {
            return Err(Blocker::TableCap);
        }
        #[allow(clippy::cast_precision_loss)]
        if after.schemas > c.limits.max_schemas as f64 && unit.load.schemas > 0.0 {
            return Err(Blocker::SchemaCap);
        }
        if c.over_table_budget {
            return Err(Blocker::TableBudget);
        }
        if c.cache_misses_rising {
            return Err(Blocker::CacheThrashing);
        }
    }
    // 7. Stability after recovery: rebalancing only.
    if purpose == Purpose::Rebalance
        && let Some(at) = c.recovered_at
        && now < at + c.limits.stability_secs
    {
        return Err(Blocker::Stability {
            until: at + c.limits.stability_secs,
        });
    }
    // A failed move excludes the target for one cooldown.
    if matches!(purpose, Purpose::Rebalance | Purpose::Drain) {
        for k in &unit.shards {
            if let Some(until) = view.shards.get(k).and_then(|s| s.excluded_from(&c.id, now)) {
                return Err(Blocker::Excluded { until });
            }
        }
    }
    let full = c.full_with(&after);
    Ok(Fit {
        full_after: full.value,
        storage_after,
        busy_after: c.util_of(&after, crate::model::Dim::Busy),
        connections_after: c.util_of(&after, crate::model::Dim::Connections),
        pressure: c.pressure.value(c.cap.threads),
    })
}

/// Groups of `tenant` bound to `group` by the co-write rule (constraint
/// 8): under `txn.non_xa_policy = reject`, groups with at least
/// `balancer.cowrite_min_per_hour` co-writing transactions share a
/// cluster. Transitive.
#[must_use]
pub fn bound_groups(view: &FleetView, tenant: TenantId, group: GroupId) -> BTreeSet<GroupId> {
    let mut out = BTreeSet::from([group]);
    let Some(t) = view.tenants.get(&tenant) else {
        return out;
    };
    if !t.non_xa_reject {
        return out;
    }
    #[allow(clippy::cast_precision_loss)]
    let min = t.knobs.cowrite_min_per_hour as f64;
    loop {
        let before = out.len();
        for cw in view.cowrites.iter().filter(|c| c.tenant == tenant) {
            if cw.per_hour < min || cw.per_hour <= 0.0 {
                continue;
            }
            if out.contains(&cw.a) {
                out.insert(cw.b);
            }
            if out.contains(&cw.b) {
                out.insert(cw.a);
            }
        }
        if out.len() == before {
            return out;
        }
    }
}

/// The unit a shard moves in: the shard plus every shard on the same
/// cluster of a group the co-write rule binds to it.
#[must_use]
pub fn unit_of(view: &FleetView, key: ShardKey) -> Option<Unit> {
    let s = view.shards.get(&key)?;
    let groups = bound_groups(view, key.tenant, key.group);
    let mut shards = vec![key];
    let mut load = s.load;
    if groups.len() > 1 {
        for o in view.shards.values() {
            if o.key != key
                && o.key.tenant == key.tenant
                && groups.contains(&o.key.group)
                && o.cluster == s.cluster
            {
                shards.push(o.key);
                load += o.load;
            }
        }
    }
    Some(Unit {
        tenant: key.tenant,
        shards,
        load,
        from: Some(s.cluster.clone()),
    })
}

/// Co-writes per hour between `unit`'s groups and the tenant's groups
/// that would end up on another cluster than `target`.
#[must_use]
pub fn cowrite_split_rate(view: &FleetView, unit: &Unit, target: &ClusterId) -> f64 {
    let groups: BTreeSet<GroupId> = unit.shards.iter().map(|k| k.group).collect();
    let mut rate = 0.0;
    for cw in view.cowrites.iter().filter(|c| c.tenant == unit.tenant) {
        let (mine, other) = if groups.contains(&cw.a) && !groups.contains(&cw.b) {
            (cw.a, cw.b)
        } else if groups.contains(&cw.b) && !groups.contains(&cw.a) {
            (cw.b, cw.a)
        } else {
            continue;
        };
        let _ = mine;
        let elsewhere = view
            .shards
            .values()
            .any(|s| s.key.tenant == unit.tenant && s.key.group == other && &s.cluster != target);
        if elsewhere {
            rate += cw.per_hour;
        }
    }
    rate
}

#[cfg(test)]
mod tests;
