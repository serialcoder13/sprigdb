//! The JSON views of [`super`] and how they are built from the store's
//! rows: pure functions, shared with `sprigctl`'s renderers and tests.
//!
//! Evidence never carries parameter values: the store keeps
//! selectivities, timings, fingerprints and sample ids only, and the views
//! leave out even the sample ids (`auto_index_validations.details`).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sprig_advisor::autoindex::cost::BUILD_LOAD_SHARE;
use sprig_advisor::autoindex::model::{IndexDef, KeyPart, PartSource, Role};
use sprig_advisor::autoindex::policy::{Mode, ObjectPolicy};
use sprig_advisor::autoindex::store::usage::{UsageRow, UserIndexState};
use sprig_advisor::autoindex::store::{
    AutoIndexRow, CandidateRow, CandidateStatus, IndexState, ValidationRow,
};
use sprig_common::ids::{Fingerprint, ObjectId};
use sprig_common::names::quote;
use sprig_meta::bootstrap::rfc3339;

/// Logical names of the tenant's objects: id to (database, object).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectNames(pub BTreeMap<ObjectId, (String, String)>);

impl ObjectNames {
    /// The database and object of `o`; `?` and `#<id>` when the snapshot
    /// doesn't know it (a dropped table's history).
    #[must_use]
    pub fn of(&self, o: ObjectId) -> (String, String) {
        self.0
            .get(&o)
            .cloned()
            .unwrap_or_else(|| ("?".to_owned(), format!("#{o}")))
    }
}

/// One fingerprint an index or candidate serves.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FingerprintView {
    /// 16 hex digits.
    pub id: String,
    /// `primary` (generated for it) or `merged`; absent when unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Baseline runs in the trigger window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runs: Option<u64>,
    /// Baseline p95, ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p95_ms: Option<f64>,
    /// Baseline mean, ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean_ms: Option<f64>,
}

/// An automatic index or recommendation (`auto_indexes`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexView {
    /// `auto_indexes.index_id`.
    pub index_id: u64,
    /// Logical database.
    pub db: String,
    /// Table (or collection, namespace).
    pub table: String,
    /// Catalog object id.
    pub object_id: u64,
    /// `sprig_auto_<8 hex>`.
    pub name: String,
    /// `sql`, `document`, ...
    pub model: String,
    /// `btree`, ..., or `advice_*` for a recommendation never applied.
    pub kind: String,
    /// Key parts: `customer_id`, `sku(12)`, `created_at desc`.
    pub columns: Vec<String>,
    /// The DDL a user would run to create it themselves, with logical
    /// names (`None` for advice).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ddl: Option<String>,
    /// Chapter 13's state.
    pub state: String,
    /// Restored when unpinned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_state: Option<String>,
    /// Why it is being retired or was dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_reason: Option<String>,
    /// Predicted benefit B, ms saved per hour.
    pub est_benefit_ms_h: f64,
    /// Predicted write cost Wc, ms added per hour.
    pub est_write_ms_h: f64,
    /// B - Wc.
    pub est_net_ms_h: f64,
    /// Predicted size, bytes.
    pub est_bytes: u64,
    /// Measured size, bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_bytes: Option<u64>,
    /// Build state per shard.
    pub shards: BTreeMap<String, String>,
    /// Target fingerprints with their baselines.
    pub fingerprints: Vec<FingerprintView>,
    /// What observation measured, as the observer stored it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed: Option<serde_json::Value>,
    /// The candidate it came from.
    pub candidate_id: u64,
    /// The validation that accepted it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_id: Option<u64>,
    /// Last time it was seen used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<String>,
    /// Created (RFC 3339, UTC).
    pub created_at: String,
    /// Last state change.
    pub state_changed_at: String,
    /// `user` or `migrated` for an index the user created or migrated whose
    /// removal automatic indexing manages (decision O1); absent for
    /// automatic indexes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
}

/// A validation (`auto_index_validations`), without per-statement
/// details.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationView {
    /// Its id.
    pub validation_id: u64,
    /// `replica`, `galera_rsu`, `estimate_canary`.
    pub method: String,
    /// The cluster.
    pub cluster: String,
    /// The node it ran on.
    pub node: String,
    /// The shard.
    pub shard_id: u64,
    /// `running`, `accepted`, `rejected`, `inconclusive`, `aborted`.
    pub verdict: String,
    /// Why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Statements replayed, and how many were noisy.
    pub statements: u64,
    /// Noisy statements.
    pub noisy: u64,
    /// Target p95 before, ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_p95_before_ms: Option<f64>,
    /// Target p95 after, ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_p95_after_ms: Option<f64>,
    /// Measured improvement, percent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub improvement_pct: Option<f64>,
    /// Worst slowdown of another statement, percent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worst_regression_pct: Option<f64>,
    /// Its fingerprint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worst_fingerprint: Option<String>,
    /// Vector candidates' recall@k.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recall_at_k: Option<f64>,
    /// Build time on the node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_seconds: Option<u32>,
    /// Started.
    pub started_at: String,
    /// Finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

/// A candidate (`auto_index_candidates`) with its evidence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandidateView {
    /// Its id.
    pub candidate_id: u64,
    /// The evaluation run.
    pub run_id: u64,
    /// Logical database.
    pub db: String,
    /// Table.
    pub table: String,
    /// Key parts (empty: the table got no candidate, see `reason`).
    pub columns: Vec<String>,
    /// Where each part came from: `eq`, `range`, `order`, `group`,
    /// `cover`.
    pub part_sources: Vec<String>,
    /// Covering.
    pub covering: bool,
    /// Fingerprints served.
    pub fingerprints: Vec<FingerprintView>,
    /// `proposed`, `covered`, `analyze`, `costed`, `selected`,
    /// `deferred`, `rejected`, `recommended`.
    pub status: String,
    /// The reason code (`covered_by:idx_x`, `below_min_net`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The reason in words.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_text: Option<String>,
    /// B, ms per hour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub benefit_ms_h: Option<f64>,
    /// Wc, ms per hour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_ms_h: Option<f64>,
    /// B - Wc.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub net_ms_h: Option<f64>,
    /// S, bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub est_bytes: Option<u64>,
    /// Tb, seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_seconds: Option<u32>,
    /// Selectivity per predicate column.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub selectivity: BTreeMap<String, f64>,
    /// Where each selectivity came from.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub selectivity_source: BTreeMap<String, String>,
    /// Entries one run reads through the index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entries_per_run: Option<f64>,
    /// Notes: variants, plans, the EXPLAIN check, skipped columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
    /// Created.
    pub created_at: String,
}

/// One entry of an index's history, from the store's rows (chapter 11's
/// decision log will carry the full history).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventView {
    /// When (RFC 3339, UTC).
    pub at: String,
    /// `candidate`, `validation`, `index`, `state`.
    pub event: String,
    /// What happened.
    pub detail: String,
}

/// `GET /admin/v1/tenants/{name}/indexes/{index}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDetail {
    /// The tenant.
    pub tenant: String,
    /// The index.
    pub index: IndexView,
    /// The candidate it came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<CandidateView>,
    /// The candidate's validations, newest first.
    pub validations: Vec<ValidationView>,
    /// What happened, oldest first.
    pub history: Vec<EventView>,
}

/// `GET /admin/v1/tenants/{name}/indexes` and `.../recommendations`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListIndexesResponse {
    /// The tenant.
    pub tenant: String,
    /// The indexes, by database, table and id.
    pub indexes: Vec<IndexView>,
}

/// One of the cost model's gates for a candidate (chapter 13, "Cost
/// model"), checked against today's settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateView {
    /// `min_net`, `benefit_ratio`, `build_time`, `payback`,
    /// `improvement`.
    pub gate: String,
    /// Passed, failed, or `None` when the candidate wasn't costed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passed: Option<bool>,
    /// The numbers.
    pub detail: String,
}

/// The policy of one object (`auto_index.*` resolved for it now).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyView {
    /// Logical database.
    pub db: String,
    /// Table.
    pub table: String,
    /// `on`, `recommend` or `off`.
    pub mode: String,
    /// Where a selected candidate goes.
    pub route: String,
    /// Key settings: `value (source scope)`.
    pub settings: BTreeMap<String, String>,
}

/// One candidate in an explanation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandidateExplain {
    /// The candidate.
    pub candidate: CandidateView,
    /// How it serves the fingerprint: `primary` or `merged`.
    pub role: String,
    /// The cost model's gates.
    pub gates: Vec<GateView>,
    /// Its validations, newest first.
    pub validations: Vec<ValidationView>,
    /// The index it became, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<IndexView>,
}

/// `GET /admin/v1/tenants/{name}/queries/{fingerprint}:explain`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryExplanation {
    /// The tenant.
    pub tenant: String,
    /// The fingerprint.
    pub fingerprint: String,
    /// Candidates since this time were read (`workload.retention_days`).
    pub since: String,
    /// The answer in a few lines.
    pub summary: Vec<String>,
    /// The policy of each object a candidate is on.
    pub policies: Vec<PolicyView>,
    /// Candidates, the newest evaluation first, then by id.
    pub candidates: Vec<CandidateExplain>,
}

/// `POST .../indexes/{index}:approve|:reject|:drop|:pin|:unpin`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexActionRequest {
    /// Why (logged with the decision).
    #[serde(default)]
    pub reason: String,
    /// Who, recorded as `op:<actor>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

/// What an action did.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexActionResponse {
    /// The tenant.
    pub tenant: String,
    /// `approve`, `reject`, `drop`, `pin`, `unpin`.
    pub action: String,
    /// Whether the state changed (`false`: it already was so).
    pub changed: bool,
    /// The state before.
    pub from: String,
    /// The state after.
    pub to: String,
    /// What happens next.
    pub message: String,
    /// The index after the action.
    pub index: IndexView,
}

fn time(t: SystemTime) -> String {
    rfc3339(t)
}

/// A key part as `col`, `col(12)`, `col desc`.
#[must_use]
pub fn part_text(p: &KeyPart) -> String {
    let mut s = p.column.clone();
    if let Some(n) = p.prefix {
        let _ = write!(s, "({n})");
    }
    if p.desc {
        s.push_str(" desc");
    }
    s
}

fn role_text(r: Role) -> &'static str {
    match r {
        Role::Primary => "primary",
        Role::Merged => "merged",
    }
}

fn source_text(s: PartSource) -> &'static str {
    match s {
        PartSource::Eq => "eq",
        PartSource::Range => "range",
        PartSource::Order => "order",
        PartSource::Group => "group",
        PartSource::Cover => "cover",
    }
}

/// The view of an index row; roles come from its candidate when given.
#[must_use]
pub fn index_view(r: &AutoIndexRow, names: &ObjectNames, cand: Option<&CandidateRow>) -> IndexView {
    let (db, table) = names.of(r.object);
    let ddl = r.def().map(|d: IndexDef| {
        format!(
            "ALTER TABLE {}.{} ADD INDEX {} ({})",
            quote(&db),
            quote(&table),
            quote(&r.name),
            d.ddl_columns()
        )
    });
    let role_of = |f: &str| {
        cand.and_then(|c| {
            c.fingerprints
                .iter()
                .find(|(g, _)| g.to_string() == f)
                .map(|(_, role)| role_text(*role).to_owned())
        })
    };
    let mut fingerprints: Vec<FingerprintView> = r
        .baseline
        .iter()
        .map(|(f, b)| FingerprintView {
            id: f.clone(),
            role: role_of(f),
            runs: Some(b.runs),
            p95_ms: Some(b.p95_ms),
            mean_ms: Some(b.mean_ms),
        })
        .collect();
    if let Some(c) = cand {
        for (f, role) in &c.fingerprints {
            let id = f.to_string();
            if !fingerprints.iter().any(|v| v.id == id) {
                fingerprints.push(FingerprintView {
                    id,
                    role: Some(role_text(*role).to_owned()),
                    runs: None,
                    p95_ms: None,
                    mean_ms: None,
                });
            }
        }
    }
    IndexView {
        index_id: r.index_id.0,
        db,
        table,
        object_id: r.object.0,
        name: r.name.clone(),
        model: r.model.to_string(),
        kind: r.kind.to_string(),
        columns: r.definition.parts.iter().map(part_text).collect(),
        ddl,
        state: r.state.to_string(),
        prior_state: r.prior_state.map(|s| s.to_string()),
        drop_reason: r.drop_reason.map(|s| s.to_string()),
        est_benefit_ms_h: r.est_benefit_ms_h,
        est_write_ms_h: r.est_write_ms_h,
        est_net_ms_h: r.est_benefit_ms_h - r.est_write_ms_h,
        est_bytes: r.est_bytes,
        actual_bytes: r.actual_bytes,
        shards: r
            .shards
            .iter()
            .map(|(k, v)| {
                let s = serde_json::to_value(v)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_default();
                (k.clone(), s)
            })
            .collect(),
        fingerprints,
        observed: r.observed.clone(),
        candidate_id: r.candidate_id,
        validation_id: r.validation_id,
        last_used_at: r.last_used_at.map(time),
        created_at: time(r.created_at),
        state_changed_at: time(r.state_changed_at),
        origin: None,
    }
}

/// The view of a user or migrated index's `index_usage` row (decision O1):
/// no id (it is named by `[<db>.][<table>.]<name>`), no estimates.
#[must_use]
pub fn user_index_view(r: &UsageRow, names: &ObjectNames) -> IndexView {
    let (db, table) = names.of(r.object);
    let columns = r
        .definition
        .find('(')
        .zip(r.definition.rfind(')'))
        .filter(|(a, b)| a < b)
        .map(|(a, b)| {
            r.definition[a + 1..b]
                .split(',')
                .map(|c| c.trim().replace('`', ""))
                .collect()
        })
        .unwrap_or_default();
    IndexView {
        index_id: 0,
        db: db.clone(),
        table: table.clone(),
        object_id: r.object.0,
        name: r.index_name.clone(),
        model: "sql".into(),
        kind: "btree".into(),
        columns,
        ddl: (!r.definition.is_empty()).then(|| {
            format!(
                "ALTER TABLE {}.{} ADD {}",
                quote(&db),
                quote(&table),
                r.definition
            )
        }),
        state: r.state.to_string(),
        prior_state: None,
        drop_reason: r.drop_reason.clone(),
        est_benefit_ms_h: 0.0,
        est_write_ms_h: 0.0,
        est_net_ms_h: 0.0,
        est_bytes: 0,
        actual_bytes: None,
        shards: BTreeMap::new(),
        fingerprints: Vec::new(),
        observed: Some(r.observed.clone()),
        candidate_id: 0,
        validation_id: None,
        last_used_at: None,
        created_at: time(r.created_at),
        state_changed_at: time(r.state_changed_at),
        origin: Some(r.origin.as_str().to_owned()),
    }
}

/// What `restore`, `pin` or `unpin` does to a user or migrated index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserPlan {
    /// Move to `to`, running `ddl` on every shard first (`NOT IGNORED`,
    /// or adding it back).
    Move {
        /// The state the row must still be in.
        from: UserIndexState,
        /// The new state.
        to: UserIndexState,
        /// What to run on each shard.
        ddl: UserDdl,
        /// What happens next, for the operator.
        message: String,
    },
    /// Nothing to do.
    Noop(String),
    /// Refused (39202): why.
    Refuse(String),
}

/// The shard statement of a [`UserPlan::Move`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserDdl {
    /// None.
    None,
    /// `ALTER INDEX x NOT IGNORED`.
    Visible,
    /// `ADD <saved definition>`.
    Recreate,
}

/// What `a` does to a user or migrated index in state `r.state`.
#[must_use]
pub fn plan_user_action(r: &UsageRow, a: Action) -> UserPlan {
    use UserIndexState as U;
    let s = r.state;
    let refuse = |why: &str| UserPlan::Refuse(format!("{} is {s}: {why}", r.index_name));
    let pinned =
        "pinned: visible, and never removed by automatic indexing until sprigctl index unpin";
    match a {
        Action::Restore | Action::Pin => match s {
            U::Pinned => UserPlan::Noop(format!("{} is already pinned", r.index_name)),
            U::Retiring => UserPlan::Move {
                from: s,
                to: U::Pinned,
                ddl: UserDdl::Visible,
                message: format!("made visible again (NOT IGNORED) and {pinned}"),
            },
            U::Dropped if a == Action::Restore => UserPlan::Move {
                from: s,
                to: U::Pinned,
                ddl: UserDdl::Recreate,
                message: format!("recreated from its saved definition and {pinned}"),
            },
            U::Dropped => refuse("it was dropped; restore it instead"),
            U::Serving | U::Recommended => UserPlan::Move {
                from: s,
                to: U::Pinned,
                ddl: UserDdl::None,
                message: pinned.into(),
            },
        },
        Action::Unpin => match s {
            U::Pinned => UserPlan::Move {
                from: s,
                to: U::Serving,
                ddl: UserDdl::None,
                message: "unpinned: judged by its usage again from now".into(),
            },
            _ => UserPlan::Noop(format!("{} isn't pinned", r.index_name)),
        },
        Action::Approve | Action::Reject | Action::Drop => refuse(
            "a user or migrated index is only restored, pinned or unpinned here;              drop it with DROP INDEX",
        ),
    }
}

/// The view of a validation row (per-statement details left out).
#[must_use]
pub fn validation_view(v: &ValidationRow) -> ValidationView {
    let improvement = match (v.target_p95_before, v.target_p95_after) {
        (Some(b), Some(a)) if b > 0.0 => Some(round1((b - a) / b * 100.0)),
        _ => None,
    };
    ValidationView {
        validation_id: v.validation_id,
        method: v.method.to_string(),
        cluster: v.cluster_id.clone(),
        node: v.node.clone(),
        shard_id: v.shard_id,
        verdict: v.verdict.to_string(),
        reason: v.reason.clone(),
        statements: v.statements,
        noisy: v.noisy,
        target_p95_before_ms: v.target_p95_before,
        target_p95_after_ms: v.target_p95_after,
        improvement_pct: improvement,
        worst_regression_pct: v.worst_regression,
        worst_fingerprint: v.worst_fp.map(|f| f.to_string()),
        recall_at_k: v.recall_at_k,
        build_seconds: v.build_seconds,
        started_at: time(v.started_at),
        finished_at: v.finished_at.map(time),
    }
}

fn round1(x: f64) -> f64 {
    (x * 10.0).round() / 10.0
}

/// The view of a candidate row.
#[must_use]
pub fn candidate_view(c: &CandidateRow, names: &ObjectNames) -> CandidateView {
    let (db, table) = names.of(c.object);
    CandidateView {
        candidate_id: c.candidate_id,
        run_id: c.run_id,
        db,
        table,
        columns: c.definition.parts.iter().map(part_text).collect(),
        part_sources: c
            .evidence
            .parts
            .iter()
            .map(|s| source_text(*s).to_owned())
            .collect(),
        covering: c.covering,
        fingerprints: c
            .fingerprints
            .iter()
            .map(|(f, r)| FingerprintView {
                id: f.to_string(),
                role: Some(role_text(*r).to_owned()),
                runs: None,
                p95_ms: None,
                mean_ms: None,
            })
            .collect(),
        status: c.status.to_string(),
        reason: c.reason.clone(),
        reason_text: c.reason.as_deref().map(reason_text),
        benefit_ms_h: c.benefit_ms_h,
        write_ms_h: c.write_ms_h,
        net_ms_h: c.benefit_ms_h.zip(c.write_ms_h).map(|(b, w)| b - w),
        est_bytes: c.est_bytes,
        build_seconds: c.build_seconds,
        selectivity: c.evidence.selectivity.clone(),
        selectivity_source: c.evidence.sel_source.clone(),
        entries_per_run: c.evidence.entries_per_run,
        notes: c.evidence.notes.clone(),
        created_at: time(c.created_at),
    }
}

/// A reason code in words.
#[must_use]
pub fn reason_text(code: &str) -> String {
    let (head, arg) = code.split_once(':').unwrap_or((code, ""));
    match head {
        "covered_by" => format!("an existing index, {arg}, already serves it"),
        "optimizer_avoids" => format!(
            "{arg} serves it on paper, but the optimizer doesn't use it even with fresh \
             statistics; no duplicate is built"
        ),
        "analyze_for" => format!(
            "{arg} should serve it but the optimizer doesn't use it; statistics are refreshed \
             before the next check"
        ),
        "below_min_net" => "its net benefit is below auto_index.min_net_ms_per_hour".to_owned(),
        "below_benefit_ratio" => {
            "its benefit is less than auto_index.min_benefit_ratio times its write cost".to_owned()
        }
        "build_too_long" => {
            "building it would take longer than auto_index.max_build_hours".to_owned()
        }
        "no_payback" => "its build wouldn't pay back within auto_index.payback_hours".to_owned(),
        "no_predicted_gain" => format!(
            "fingerprint {arg} isn't predicted to improve by auto_index.min_improvement_pct"
        ),
        "recreate_blocked" => format!(
            "the same index was dropped ({arg}) recently; auto_index.recreate_block_days holds \
             it back"
        ),
        "cooldown" => "its fingerprints were decided within auto_index.cooldown_hours".to_owned(),
        "budget" => "the table's budget (auto_index.max_per_table, auto_index.max_storage_pct) \
                     or the day's cap (auto_index.max_new_per_day) is used up"
            .to_owned(),
        "merged" => format!("merged into a longer candidate ({arg})"),
        "variant" => format!("a variant: {arg}"),
        "function" => format!("a predicate wraps its column in {arg}(), so no index can serve it"),
        "no_sargable_predicate" => "no predicate an index can serve".to_owned(),
        "no_statistics" => "the table has no statistics yet".to_owned(),
        "no_probes" => "no rows-examined probes yet; it is watched from now on".to_owned(),
        "exists" => format!("an automatic index with this definition already exists ({arg})"),
        "leading_wildcard" => "its only filter is LIKE '%...', which a B-tree can't serve (a \
                               search index can: chapter 39)"
            .to_owned(),
        _ => code.to_owned(),
    }
}

/// What a state means for the user.
fn state_text(r: &AutoIndexRow, tenant: &str) -> String {
    let reason = r.drop_reason.map(|d| format!(" ({d})")).unwrap_or_default();
    match r.state {
        IndexState::Validating => "being validated".to_owned(),
        IndexState::Recommended if r.kind.is_advice() => format!(
            "recommended ({}); applied by hand, never automatically",
            r.kind
        ),
        IndexState::Recommended => format!(
            "recommended: sprigctl index approve {} --tenant {tenant} rolls it out, \
             sprigctl index reject turns it down",
            r.index_id
        ),
        IndexState::RollingOut => "being built shard by shard".to_owned(),
        IndexState::Observing => {
            "built; watched for auto_index.observe_days before it is kept".to_owned()
        }
        IndexState::Active => "active".to_owned(),
        IndexState::Retiring => format!("being retired{reason}"),
        IndexState::Dropping => format!("being dropped{reason}"),
        IndexState::Dropped => format!("dropped{reason}"),
        IndexState::Rejected => "refused by validation".to_owned(),
        IndexState::Failed => "failed to build".to_owned(),
        IndexState::Frozen => "pinned: nothing changes it until unpinned".to_owned(),
    }
}

fn status_text(c: &CandidateRow) -> String {
    let why = c.reason.as_deref().map(reason_text);
    let with = |s: &str| match &why {
        Some(w) => format!("{s}: {w}"),
        None => s.to_owned(),
    };
    match c.status {
        CandidateStatus::Proposed => "proposed, not costed yet".to_owned(),
        CandidateStatus::Covered | CandidateStatus::Analyze => {
            why.unwrap_or_else(|| "an existing index serves it".to_owned())
        }
        CandidateStatus::Costed => "passed the cost model's gates".to_owned(),
        CandidateStatus::Selected => "selected; validation pending".to_owned(),
        CandidateStatus::Deferred => with("deferred"),
        CandidateStatus::Rejected => with("rejected"),
        CandidateStatus::Recommended => "recommended".to_owned(),
    }
}

/// The cost model's gates for `c` against `p`'s parameters. Gate 5
/// (predicted improvement per fingerprint) is known only from the reason.
#[must_use]
pub fn gates(c: &CandidateRow, p: Option<&ObjectPolicy>) -> Vec<GateView> {
    let costed = c.benefit_ms_h.zip(c.write_ms_h);
    let mut out = Vec::new();
    let Some(p) = p else {
        return out;
    };
    let cp = &p.cost;
    let unknown = |gate: &str| GateView {
        gate: gate.to_owned(),
        passed: None,
        detail: if costed.is_some() {
            "not recorded".to_owned()
        } else {
            format!("not costed ({})", c.status)
        },
    };
    if costed.is_none() {
        for g in ["min_net", "benefit_ratio", "build_time", "payback"] {
            out.push(unknown(g));
        }
    }
    if let Some((b, w)) = costed {
        let net = b - w;
        out.push(GateView {
            gate: "min_net".into(),
            passed: Some(net >= cp.min_net_ms_per_hour),
            detail: format!(
                "net {net:.0} ms/h (B {b:.0} - Wc {w:.0}) vs auto_index.min_net_ms_per_hour {}",
                cp.min_net_ms_per_hour
            ),
        });
        out.push(GateView {
            gate: "benefit_ratio".into(),
            passed: Some(b >= cp.min_benefit_ratio * w),
            detail: format!(
                "B {b:.0} vs {} x Wc {w:.0} (auto_index.min_benefit_ratio)",
                cp.min_benefit_ratio
            ),
        });
        if let Some(tb) = c.build_seconds {
            {
                let tb = f64::from(tb);
                out.push(GateView {
                    gate: "build_time".into(),
                    passed: Some(tb <= cp.max_build_hours * 3600.0),
                    detail: format!(
                        "build {tb:.0} s vs auto_index.max_build_hours {}",
                        cp.max_build_hours
                    ),
                });
                out.push(GateView {
                    gate: "payback".into(),
                    passed: Some(BUILD_LOAD_SHARE * tb * 1000.0 <= net * cp.payback_hours),
                    detail: format!(
                        "build load {:.0} ms vs net x auto_index.payback_hours {:.0} ms",
                        BUILD_LOAD_SHARE * tb * 1000.0,
                        net * cp.payback_hours
                    ),
                });
            }
        } else {
            out.push(unknown("build_time"));
            out.push(unknown("payback"));
        }
    }
    let failed_improvement = c
        .reason
        .as_deref()
        .and_then(|r| r.strip_prefix("no_predicted_gain:"));
    let improvement = match (failed_improvement, c.status) {
        (Some(fp), _) => GateView {
            gate: "improvement".into(),
            passed: Some(false),
            detail: format!(
                "fingerprint {fp} isn't predicted to improve by {}%",
                cp.min_improvement_pct
            ),
        },
        (
            None,
            CandidateStatus::Costed | CandidateStatus::Selected | CandidateStatus::Recommended,
        ) => GateView {
            gate: "improvement".into(),
            passed: Some(true),
            detail: format!(
                "every primary fingerprint predicted to improve by {}% or more",
                cp.min_improvement_pct
            ),
        },
        _ => unknown("improvement"),
    };
    out.push(improvement);
    out
}

/// The keys an explanation shows, in this order.
const POLICY_KEYS: &[&str] = &[
    "auto_index.mode",
    "auto_index.recommend_validate",
    "auto_index.min_runs",
    "auto_index.p95_threshold_ms",
    "auto_index.top_n",
    "auto_index.min_examined_per_returned",
    "auto_index.cooldown_hours",
    "auto_index.min_net_ms_per_hour",
    "auto_index.min_benefit_ratio",
    "auto_index.max_build_hours",
    "auto_index.payback_hours",
    "auto_index.min_improvement_pct",
    "auto_index.max_per_table",
    "auto_index.max_storage_pct",
    "auto_index.recreate_block_days",
];

/// Where a selected candidate goes under `p` (chapter 13's `route`).
#[must_use]
pub fn route_text(p: &ObjectPolicy) -> String {
    match p.mode {
        Mode::Off => "off: no evaluation; existing automatic indexes are kept as they are".into(),
        Mode::On => "on: validate, then roll out (a large-scan candidate on a table above \
                     auto_index.scan_recommend_above_gb stops at recommended)"
            .into(),
        Mode::Recommend if p.recommend_validate => "recommend: validate when a validation node \
                                                    exists, then stop at recommended"
            .into(),
        Mode::Recommend => "recommend: stop at recommended without validating".into(),
        Mode::Approve => "approve: stop at recommended and wait in the approvals inbox \
                          (chapter 52); an approval rolls it out"
            .into(),
    }
}

/// The policy view of an object.
#[must_use]
pub fn policy_view(db: &str, table: &str, p: &ObjectPolicy) -> PolicyView {
    let settings = POLICY_KEYS
        .iter()
        .filter_map(|k| {
            p.used.get(k).map(|u| {
                (
                    (*k).to_owned(),
                    format!(
                        "{} ({}{})",
                        sprig_settings::show_value(&u.value),
                        u.source,
                        if u.clamped { ", clamped" } else { "" }
                    ),
                )
            })
        })
        .collect();
    PolicyView {
        db: db.to_owned(),
        table: table.to_owned(),
        mode: match p.mode {
            Mode::On => "on",
            Mode::Recommend => "recommend",
            Mode::Approve => "approve",
            Mode::Off => "off",
        }
        .to_owned(),
        route: route_text(p),
        settings,
    }
}

/// Everything the store holds about one fingerprint.
#[derive(Debug, Default)]
pub struct Evidence<'a> {
    /// Candidates serving it, newest first.
    pub candidates: &'a [CandidateRow],
    /// Validations by candidate id, newest first.
    pub validations: BTreeMap<u64, Vec<ValidationRow>>,
    /// Index rows by candidate id.
    pub indexes: BTreeMap<u64, Vec<AutoIndexRow>>,
    /// Policies by object.
    pub policies: BTreeMap<ObjectId, ObjectPolicy>,
}

/// The policy of each object a candidate is on; an object with
/// `mode = off` also gets a summary line.
fn policies_of(
    ev: &Evidence<'_>,
    names: &ObjectNames,
    summary: &mut Vec<String>,
) -> Vec<PolicyView> {
    let objects: BTreeSet<ObjectId> = ev.candidates.iter().map(|c| c.object).collect();
    let mut policies = Vec::new();
    for o in objects {
        if let Some(p) = ev.policies.get(&o) {
            let (db, table) = names.of(o);
            if p.mode == Mode::Off {
                summary.push(format!(
                    "auto_index.mode is off for {db}.{table}: nothing new is evaluated"
                ));
            }
            policies.push(policy_view(&db, &table, p));
        }
    }
    policies
}

/// `sprigctl explain query <fingerprint>`: why an index was or wasn't
/// built for that query shape.
#[must_use]
pub fn explain(
    tenant: &str,
    fp: Fingerprint,
    since: SystemTime,
    ev: &Evidence<'_>,
    names: &ObjectNames,
) -> QueryExplanation {
    let mut summary = Vec::new();
    let newest_run = ev.candidates.iter().map(|c| c.run_id).max();
    if ev.candidates.is_empty() {
        summary.push(format!(
            "no candidate recorded for {fp} since {}",
            time(since)
        ));
        summary.push(
            "it didn't qualify (not auto_index.min_runs runs with a p95 above \
             auto_index.p95_threshold_ms, not among the auto_index.top_n by total time, no \
             large scans), or no evaluation has run since it did"
                .to_owned(),
        );
    }
    let mut ordered: Vec<&CandidateRow> = ev.candidates.iter().collect();
    ordered.sort_by_key(|c| (std::cmp::Reverse(c.run_id), c.candidate_id));
    let mut out = Vec::new();
    for c in ordered {
        let (db, table) = names.of(c.object);
        let index = ev.indexes.get(&c.candidate_id).and_then(|v| v.last());
        let latest = Some(c.run_id) == newest_run;
        if latest {
            let line = if c.is_skip() {
                format!(
                    "no candidate on {db}.{table}: {}",
                    c.reason
                        .as_deref()
                        .map_or_else(|| "no reason recorded".into(), reason_text)
                )
            } else {
                let cols = c
                    .definition
                    .parts
                    .iter()
                    .map(part_text)
                    .collect::<Vec<_>>()
                    .join(", ");
                match index {
                    Some(r) => format!(
                        "{db}.{table}: index {} ({cols}) is {}",
                        r.name,
                        state_text(r, tenant)
                    ),
                    None => format!("{db}.{table}: candidate ({cols}) {}", status_text(c)),
                }
            };
            summary.push(line);
        }
        let role = c
            .fingerprints
            .iter()
            .find(|(f, _)| *f == fp)
            .map_or("primary", |(_, r)| role_text(*r));
        out.push(CandidateExplain {
            candidate: candidate_view(c, names),
            role: role.to_owned(),
            gates: if c.is_skip() {
                Vec::new()
            } else {
                gates(c, ev.policies.get(&c.object))
            },
            validations: ev
                .validations
                .get(&c.candidate_id)
                .map(|v| v.iter().map(validation_view).collect())
                .unwrap_or_default(),
            index: index.map(|r| index_view(r, names, Some(c))),
        });
    }
    let earlier: BTreeSet<u64> = ev
        .candidates
        .iter()
        .map(|c| c.run_id)
        .filter(|r| Some(*r) != newest_run)
        .collect();
    if !earlier.is_empty() {
        summary.push(format!(
            "{} earlier evaluation{} also recorded candidates for it",
            earlier.len(),
            if earlier.len() == 1 { "" } else { "s" }
        ));
    }
    let policies = policies_of(ev, names, &mut summary);
    QueryExplanation {
        tenant: tenant.to_owned(),
        fingerprint: fp.to_string(),
        since: time(since),
        summary,
        policies,
        candidates: out,
    }
}

/// An operator's action on an automatic index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Roll a recommendation out (chapter 13's `apply`).
    Approve,
    /// Turn a recommendation down.
    Reject,
    /// Force-drop an index.
    Drop,
    /// Freeze it in its state (chapter 13's `freeze`).
    Pin,
    /// Restore the state it was pinned in (`freeze --off`).
    Unpin,
    /// Make a retiring (`IGNORED`) index visible again, or recreate a user
    /// index dropped for disuse, and pin it (decision O1).
    Restore,
}

impl Action {
    /// The name in routes and output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Action::Approve => "approve",
            Action::Reject => "reject",
            Action::Drop => "drop",
            Action::Pin => "pin",
            Action::Unpin => "unpin",
            Action::Restore => "restore",
        }
    }

    /// Parses a route's custom method.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "approve" | "apply" => Action::Approve,
            "reject" | "dismiss" => Action::Reject,
            "drop" => Action::Drop,
            "pin" | "freeze" => Action::Pin,
            "unpin" | "unfreeze" => Action::Unpin,
            "restore" => Action::Restore,
            _ => return None,
        })
    }
}

/// What an action does to a row in its current state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Move `from` → `to`, with a drop reason for `dropped` and
    /// `dropping`.
    Move {
        /// The state the row must still be in.
        from: IndexState,
        /// The new state.
        to: IndexState,
        /// Set with the move.
        drop_reason: Option<sprig_advisor::autoindex::store::DropReason>,
        /// What happens next, for the operator.
        message: String,
    },
    /// Restore the pinned state.
    Unfreeze,
    /// `retiring → active`, `NOT IGNORED` on every shard, then
    /// `active → frozen` (decision O1's restore).
    Restore,
    /// Nothing to do: it already is so.
    Noop(String),
    /// Refused (39202): why.
    Refuse(String),
}

/// What `a` does to `r` (chapter 13's state machine, operator edges).
#[must_use]
pub fn plan_action(r: &AutoIndexRow, a: Action) -> Plan {
    use IndexState as S;
    use sprig_advisor::autoindex::store::DropReason;
    let s = r.state;
    let refuse = |why: &str| Plan::Refuse(format!("{} is {s}: {why}", r.name));
    match a {
        Action::Approve => match s {
            S::Recommended if r.kind.is_advice() => refuse(&format!(
                "{} advice is applied by hand (chapter 13); approving it isn't supported in R0",
                r.kind
            )),
            S::Recommended => Plan::Move {
                from: s,
                to: S::RollingOut,
                drop_reason: None,
                message: "handed to rollout: it is built shard by shard in the build window, \
                          then observed"
                    .into(),
            },
            S::RollingOut | S::Observing | S::Active => {
                Plan::Noop(format!("{} is already {s}", r.name))
            }
            _ => refuse("only a recommendation can be approved"),
        },
        Action::Reject => match s {
            S::Recommended => Plan::Move {
                from: s,
                to: S::Dropped,
                drop_reason: Some(DropReason::Dismissed),
                message: "dismissed; not recommended again for auto_index.recreate_block_days"
                    .into(),
            },
            S::Dropped if r.drop_reason == Some(DropReason::Dismissed) => {
                Plan::Noop(format!("{} was already dismissed", r.name))
            }
            _ => refuse("only a recommendation can be rejected; drop an index instead"),
        },
        Action::Drop => match s {
            S::RollingOut | S::Observing | S::Active | S::Retiring | S::Failed => Plan::Move {
                from: s,
                to: S::Dropping,
                drop_reason: Some(DropReason::Operator),
                message: "being dropped on every shard; not proposed again for \
                          auto_index.recreate_block_days"
                    .into(),
            },
            S::Dropping | S::Dropped => Plan::Noop(format!("{} is already {s}", r.name)),
            S::Recommended => refuse("it isn't built; reject the recommendation instead"),
            S::Frozen => refuse("it is pinned; unpin it first"),
            S::Validating => refuse("it is being validated; drop it once validation finishes"),
            S::Rejected => Plan::Noop(format!("{} was never built", r.name)),
        },
        Action::Pin => match s {
            S::Frozen => Plan::Noop(format!("{} is already pinned", r.name)),
            _ if s.is_terminal() => refuse("there is nothing left to pin"),
            _ => Plan::Move {
                from: s,
                to: S::Frozen,
                drop_reason: None,
                message: format!("pinned in {s}: nothing changes it until sprigctl index unpin"),
            },
        },
        Action::Unpin => match s {
            S::Frozen => Plan::Unfreeze,
            _ => Plan::Noop(format!("{} isn't pinned", r.name)),
        },
        Action::Restore => match s {
            S::Retiring => Plan::Restore,
            S::Observing | S::Active => Plan::Move {
                from: s,
                to: S::Frozen,
                drop_reason: None,
                message: format!(
                    "visible already; pinned in {s}: never removed until sprigctl index unpin"
                ),
            },
            S::Frozen => Plan::Noop(format!("{} is already pinned", r.name)),
            S::Dropped | S::Dropping => refuse(
                "an automatic index isn't recreated by hand; evaluation proposes it again \
                 when a query needs it",
            ),
            _ => refuse("only a retiring (IGNORED) or serving index can be restored"),
        },
    }
}

/// The history of an index from its rows, oldest first.
#[must_use]
pub fn history(
    r: &AutoIndexRow,
    cand: Option<&CandidateRow>,
    validations: &[ValidationRow],
) -> Vec<EventView> {
    let mut ev: Vec<(SystemTime, EventView)> = Vec::new();
    if let Some(c) = cand {
        ev.push((
            c.created_at,
            EventView {
                at: time(c.created_at),
                event: "candidate".into(),
                detail: format!(
                    "candidate {} in evaluation run {}: {}",
                    c.candidate_id,
                    c.run_id,
                    status_text(c)
                ),
            },
        ));
    }
    for v in validations {
        ev.push((
            v.started_at,
            EventView {
                at: time(v.started_at),
                event: "validation".into(),
                detail: format!("{} validation on {} started", v.method, v.node),
            },
        ));
        if let Some(t) = v.finished_at {
            ev.push((
                t,
                EventView {
                    at: time(t),
                    event: "validation".into(),
                    detail: format!(
                        "{}{}",
                        v.verdict,
                        v.reason
                            .as_deref()
                            .map(|r| format!(": {r}"))
                            .unwrap_or_default()
                    ),
                },
            ));
        }
    }
    ev.push((
        r.created_at,
        EventView {
            at: time(r.created_at),
            event: "index".into(),
            detail: format!("{} recorded", r.name),
        },
    ));
    if r.state_changed_at > r.created_at {
        ev.push((
            r.state_changed_at,
            EventView {
                at: time(r.state_changed_at),
                event: "state".into(),
                detail: format!(
                    "now {}{}",
                    r.state,
                    r.drop_reason.map(|d| format!(" ({d})")).unwrap_or_default()
                ),
            },
        ));
    }
    ev.sort_by_key(|(t, _)| *t);
    ev.into_iter().map(|(_, e)| e).collect()
}

/// Whether `r` is a recommendation (`recommended`, including advice).
#[must_use]
pub fn is_recommendation(r: &AutoIndexRow) -> bool {
    r.state == IndexState::Recommended
}
