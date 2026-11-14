//! The DC failover record, `/sprig/g/failovers/<failover_id>`, and the verdicts observers send.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_common::ids::DcId;
use sprig_meta::{Key, Tier, Versioned};

/// `failovers/<id>` in the global tier.
///
/// # Errors
/// Internal for an id that isn't a key segment.
pub fn failover_key(id: &str) -> Result<Key> {
    Tier::Global.key(&format!("failovers/{id}"))
}

/// The failover record's states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailoverState {
    /// The DC lease vanished; waiting out the drift margin.
    Suspect,
    /// Observers are probing the DC.
    Observing,
    /// Tenants are being promoted.
    Promoting,
    /// Every tenant was promoted or skipped.
    Done,
    /// Too few sites, no agreement, or the decision timed out.
    ManualOnly,
    /// The DC took its lease back within the margin.
    Closed,
}

/// What one observer concluded about a DC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerdictKind {
    /// No gateway or controller of the DC reports valid permits, and
    /// every reachable writer self-fences.
    Down,
    /// A gateway or controller of the DC reports valid permits.
    Alive,
    /// Down, but some reachable writers can't self-fence.
    Unsafe,
}

/// One tenant's standby lag, as the observer in its standby DC reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StandbyLag {
    /// The tenant.
    pub tenant_id: u64,
    /// `source_seen_at - applied_through`, or 0 when caught up.
    pub lag_ms: u32,
}

/// `Observer.ObserveDc`'s reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verdict {
    /// The observing site.
    pub observer_dc: DcId,
    /// The observer itself holds a valid DC lease; a verdict from one that
    /// doesn't is ignored.
    pub observer_lease_valid: bool,
    /// The verdict.
    pub verdict: VerdictKind,
    /// Reachable writers that cannot self-fence (`cluster/addr`).
    #[serde(default)]
    pub unsafe_writers: Vec<String>,
    /// Standby lags of tenants whose standby this site holds.
    #[serde(default)]
    pub lags: Vec<StandbyLag>,
}

/// One vote in the record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vote {
    /// The observing site.
    pub dc: DcId,
    /// Its verdict.
    pub verdict: VerdictKind,
    /// Writers it found unsafe.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unsafe_writers: Vec<String>,
}

/// The diverged-transaction report of a returning copy (diverged
/// transactions are extracted and reported, never merged).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ErrantReport {
    /// How many transactions only the returning copy committed.
    pub count: u64,
    /// Their GTIDs, as ranges per domain (`1-101-118..120`).
    #[serde(default)]
    pub gtids: Vec<String>,
    /// Tables they changed.
    #[serde(default)]
    pub tables: Vec<String>,
    /// Where the extract is kept (object storage), if written.
    #[serde(default)]
    pub path: Option<String>,
    /// The statements, where the binlog annotates them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statements: Vec<String>,
    /// The `errant_extract` job's sealed row images, once written and
    /// verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extract: Option<sprig_cluster::failover::extract::ExtractSummary>,
}

/// One tenant's line in the record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantOutcome {
    /// The tenant.
    pub tenant_id: u64,
    /// `promoted` or `skipped`.
    pub outcome: String,
    /// The new home.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<DcId>,
    /// The standby lag at the decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag_ms: Option<u32>,
    /// Why it was skipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The epoch the promotion committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    /// `applied_source_gtid` per source cluster when the standby stopped.
    #[serde(default)]
    pub applied: BTreeMap<String, String>,
    /// The diverged-transaction report, after the old home returned.
    #[serde(default)]
    pub errant: Option<ErrantReport>,
}

/// `/sprig/g/failovers/<failover_id>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverRecord {
    /// `fo-<unix ms>-<dc>`.
    pub failover_id: String,
    /// The lost DC.
    pub dc_id: DcId,
    /// The state.
    pub state: FailoverState,
    /// When the lease was seen gone (RFC 3339, for people).
    pub lease_lost_at: String,
    /// Votes of the last observation round.
    #[serde(default)]
    pub votes: Vec<Vote>,
    /// Per tenant.
    #[serde(default)]
    pub tenants: Vec<TenantOutcome>,
    /// Why the record ended `manual_only` or `closed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl Versioned for FailoverRecord {
    const V: u32 = 1;
}
