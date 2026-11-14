//! The recommendation and its parts.

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sprig_common::ids::{ClusterId, DbId, DcId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};

/// Microseconds since the Unix epoch.
pub type Micros = i64;

/// Comments kept per item; older ones are dropped with a history note.
pub const MAX_COMMENTS: usize = 100;
/// Transitions kept per item.
pub const MAX_HISTORY: usize = 50;
/// Longest text kept in a summary, reason or result.
pub const MAX_TEXT: usize = 512;
/// Longest comment.
pub const MAX_COMMENT: usize = 2000;

/// Cuts `s` to at most `max` bytes on a character boundary.
#[must_use]
pub fn clip(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_owned()
}

macro_rules! text_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident = $text:literal,)* }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        pub enum $name { $($(#[$vdoc])* #[serde(rename = $text)] $variant,)* }

        impl $name {
            /// Every value, in declaration order.
            pub const ALL: &'static [$name] = &[$($name::$variant),*];

            /// The text form.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $($name::$variant => $text,)* }
            }

            /// Parses the text form.
            #[must_use]
            pub fn parse(s: &str) -> Option<Self> {
                match s { $($text => Some($name::$variant),)* _ => None }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

text_enum! {
    /// Who a recommendation belongs to, narrowest first.
    Level {
        /// One table (object).
        Table = "table",
        /// One database.
        Database = "database",
        /// The tenant.
        Tenant = "tenant",
        /// One data center's operators.
        Dc = "dc",
        /// Platform operators.
        Platform = "platform",
    }
}

text_enum! {
    /// Where a recommendation stands.
    State {
        /// Waiting for a person (or a timer).
        Open = "open",
        /// Approved; applied on the deciding component's next cycle.
        Approved = "approved",
        /// The action runs.
        Applying = "applying",
        /// Done.
        Applied = "applied",
        /// The action failed.
        Failed = "failed",
        /// A person said no.
        Ignored = "ignored",
        /// Out of the inbox until `snooze_until`.
        Snoozed = "snoozed",
        /// Newer advice for the same target replaced it.
        Superseded = "superseded",
        /// The condition went away, or re-validation failed.
        Expired = "expired",
    }
}

impl State {
    /// One live item per fingerprint: these states.
    #[must_use]
    pub const fn is_live(self) -> bool {
        matches!(
            self,
            State::Open | State::Approved | State::Applying | State::Snoozed
        )
    }

    /// Nothing more happens to the item (reopen aside).
    #[must_use]
    pub const fn is_closed(self) -> bool {
        !self.is_live()
    }
}

text_enum! {
    /// A mode, as the gate sees it.
    Mode {
        /// Act automatically.
        On = "on",
        /// Hold for an approval.
        Approve = "approve",
        /// Record advice; a person may still approve it.
        Recommend = "recommend",
        /// Nothing.
        Off = "off",
    }
}

impl Mode {
    /// The gate's reading of a mode key's text. Each key keeps its own
    /// spelling: `auto` is `on`; `dry_run` and `manual` keep their
    /// component's own behaviour and never reach the gate (`off` here).
    #[must_use]
    pub fn from_setting(text: &str) -> Mode {
        match text {
            "approve" => Mode::Approve,
            "recommend" => Mode::Recommend,
            "off" | "dry_run" | "manual" => Mode::Off,
            _ => Mode::On,
        }
    }
}

/// Where an item is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Home {
    /// The tenant catalog.
    Tenant,
    /// The DC tier of etcd.
    Dc,
    /// The global tier of etcd.
    Global,
}

text_enum! {
    /// What kind of decision is held. `<family>.<action>`.
    Kind {
        /// Create an index (any model: B-tree, vector, spatial, field column).
        IndexCreate = "index.create",
        /// Drop or retire an index.
        IndexDrop = "index.drop",
        /// Build an ANN (HNSW) graph for a vector index.
        VectorAnn = "vector.ann",
        /// Split a shard.
        ShardSplit = "shard.split",
        /// Merge shards.
        ShardMerge = "shard.merge",
        /// Move a shard to another cluster in the DC.
        ShardMove = "shard.move",
        /// Choose keys and table groups.
        AdvisorDesign = "advisor.design",
        /// Re-key a table group.
        AdvisorRekey = "advisor.rekey",
        /// Make a table a reference table.
        AdvisorReference = "advisor.reference",
        /// Create a global lookup index.
        AdvisorLookup = "advisor.lookup",
        /// Move a tenant to another cluster.
        BalancerMove = "balancer.move",
        /// Move a tenant to another DC.
        DcMove = "dc.move",
        /// Create a standby in another DC.
        StandbyCreate = "standby.create",
        /// Drop a standby.
        StandbyDrop = "standby.drop",
        /// Fail a cluster's writer over.
        FailoverCluster = "failover.cluster",
        /// Fail a tenant over to another DC.
        FailoverDc = "failover.dc",
        /// Partition a graph.
        GraphPartition = "graph.partition",
    }
}

impl Kind {
    /// The family: the part before the dot.
    #[must_use]
    pub fn family(self) -> &'static str {
        let s = self.as_str();
        s.split_once('.').map_or(s, |(f, _)| f)
    }

    /// The mode key that governs the kind.
    #[must_use]
    pub const fn mode_key(self) -> &'static str {
        match self {
            Kind::IndexCreate | Kind::IndexDrop => "auto_index.mode",
            Kind::VectorAnn => "vector.auto_ann",
            Kind::ShardSplit | Kind::ShardMerge | Kind::ShardMove => "sharding.mode",
            Kind::AdvisorDesign
            | Kind::AdvisorRekey
            | Kind::AdvisorReference
            | Kind::AdvisorLookup => "advisor.mode",
            Kind::BalancerMove => "balancer.mode",
            Kind::DcMove => "migration.mode",
            Kind::StandbyCreate | Kind::StandbyDrop => "failover.standby_mode",
            Kind::FailoverCluster | Kind::FailoverDc => "failover.mode",
            Kind::GraphPartition => "graph.auto_partition",
        }
    }

    /// The narrowest level an item of this kind can sit at.
    #[must_use]
    pub const fn min_level(self) -> Level {
        match self {
            Kind::IndexCreate | Kind::IndexDrop | Kind::VectorAnn | Kind::GraphPartition => {
                Level::Table
            }
            Kind::ShardSplit
            | Kind::ShardMerge
            | Kind::ShardMove
            | Kind::AdvisorDesign
            | Kind::AdvisorRekey
            | Kind::AdvisorReference
            | Kind::AdvisorLookup => Level::Database,
            Kind::BalancerMove | Kind::DcMove | Kind::StandbyCreate | Kind::StandbyDrop => {
                Level::Tenant
            }
            Kind::FailoverCluster | Kind::FailoverDc => Level::Dc,
        }
    }

    /// Where items of the kind live.
    #[must_use]
    pub const fn home(self) -> Home {
        match self {
            Kind::FailoverCluster => Home::Dc,
            Kind::FailoverDc => Home::Global,
            _ => Home::Tenant,
        }
    }

    /// An item not refreshed for this long is `expired`: three times the
    /// deciding loop's period, at least an hour.
    #[must_use]
    pub const fn expire_after(self) -> Duration {
        const H: u64 = 3600;
        Duration::from_secs(match self {
            Kind::IndexCreate | Kind::IndexDrop | Kind::VectorAnn => 3 * 24 * H,
            Kind::AdvisorDesign
            | Kind::AdvisorRekey
            | Kind::AdvisorReference
            | Kind::AdvisorLookup
            | Kind::GraphPartition => 24 * H,
            Kind::ShardSplit
            | Kind::ShardMerge
            | Kind::ShardMove
            | Kind::BalancerMove
            | Kind::DcMove
            | Kind::StandbyCreate
            | Kind::StandbyDrop => 3 * H,
            Kind::FailoverCluster | Kind::FailoverDc => H,
        })
    }

    /// Whether the kind changes an index (`INDEX` holders may act on it).
    #[must_use]
    pub const fn is_index(self) -> bool {
        matches!(self, Kind::IndexCreate | Kind::IndexDrop | Kind::VectorAnn)
    }
}

/// What a recommendation is about.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Subject {
    /// The tenant, for every kind but cluster failover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<TenantId>,
    /// The tenant's name, for people.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_name: Option<String>,
    /// The database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<DbId>,
    /// The database's name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_name: Option<String>,
    /// The object (table, collection, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<ObjectId>,
    /// The object's name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_name: Option<String>,
    /// The DC the subject is in (the tenant's home DC, or the cluster's).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dc: Option<DcId>,
    /// The cluster, for cluster kinds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<ClusterId>,
}

impl Subject {
    /// A tenant-wide subject.
    #[must_use]
    pub fn tenant(tenant: TenantId, dc: DcId) -> Self {
        Self {
            tenant: Some(tenant),
            dc: Some(dc),
            ..Self::default()
        }
    }

    /// `db.object`, `db` or the tenant name, for people.
    #[must_use]
    pub fn display(&self) -> String {
        match (&self.db_name, &self.object_name) {
            (Some(d), Some(o)) => format!("{d}.{o}"),
            (Some(d), None) => d.clone(),
            _ => match (&self.tenant_name, &self.cluster) {
                (Some(t), _) => t.clone(),
                (None, Some(c)) => c.to_string(),
                (None, None) => self.tenant.map(|t| t.to_string()).unwrap_or_default(),
            },
        }
    }
}

/// The proposed action.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Action {
    /// The exact statement or command, for people.
    pub text: String,
    /// What the component needs to repeat it; part of the fingerprint.
    #[serde(default)]
    pub machine: serde_json::Value,
}

/// An approval window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Window {
    /// Opens.
    pub start_us: Micros,
    /// Closes.
    pub end_us: Micros,
}

text_enum! {
    /// How long an ignore holds.
    IgnoreFor {
        /// `approvals.ignore_once_days`.
        Once = "once",
        /// A number of days.
        Days = "days",
        /// Until reopened.
        Forever = "forever",
    }
}

/// An ignore's terms.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Ignore {
    /// How it was given.
    pub how: IgnoreFor,
    /// Suppression ends; `None` is forever.
    pub until_us: Option<Micros>,
    /// The score when ignored; a new proposal must beat it by the factor.
    pub score: f64,
    /// `approvals.reraise_factor` when ignored.
    pub factor: f64,
}

/// One transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// When.
    pub at_us: Micros,
    /// From state.
    pub from: State,
    /// To state.
    pub to: State,
    /// Who: a caller name, `auto-apply`, `approvals`, or the component.
    pub by: String,
    /// Why.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,
}

/// One comment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Comment {
    /// When.
    pub at_us: Micros,
    /// Who.
    pub author: String,
    /// Text.
    pub body: String,
}

/// Who gets notified now: the escalation chain's current step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub enum NotifyLevel {
    /// Table owners.
    Table,
    /// Database owners.
    Database,
    /// Tenant administrators.
    Tenant,
    /// An ancestor tenant's administrators.
    Ancestor(TenantId),
    /// The DC's operators.
    Dc,
    /// Platform operators.
    Platform,
}

impl NotifyLevel {
    /// The level a notify level starts from.
    #[must_use]
    pub const fn of(level: Level) -> Self {
        match level {
            Level::Table => NotifyLevel::Table,
            Level::Database => NotifyLevel::Database,
            Level::Tenant => NotifyLevel::Tenant,
            Level::Dc => NotifyLevel::Dc,
            Level::Platform => NotifyLevel::Platform,
        }
    }

    /// The next step up: table, database, tenant, the parent, ..., the
    /// root, platform; dc, platform. `ancestry` is root first.
    #[must_use]
    pub fn next(self, ancestry: &[TenantId]) -> Option<NotifyLevel> {
        match self {
            NotifyLevel::Table => Some(NotifyLevel::Database),
            NotifyLevel::Database => Some(NotifyLevel::Tenant),
            NotifyLevel::Tenant => Some(
                ancestry
                    .last()
                    .map_or(NotifyLevel::Platform, |&p| NotifyLevel::Ancestor(p)),
            ),
            NotifyLevel::Ancestor(a) => {
                let up = ancestry
                    .iter()
                    .position(|&x| x == a)
                    .and_then(|i| i.checked_sub(1))
                    .map(|i| ancestry[i]);
                Some(up.map_or(NotifyLevel::Platform, NotifyLevel::Ancestor))
            }
            NotifyLevel::Dc => Some(NotifyLevel::Platform),
            NotifyLevel::Platform => None,
        }
    }
}

impl fmt::Display for NotifyLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotifyLevel::Table => f.write_str("table"),
            NotifyLevel::Database => f.write_str("database"),
            NotifyLevel::Tenant => f.write_str("tenant"),
            NotifyLevel::Ancestor(t) => write!(f, "ancestor:{t}"),
            NotifyLevel::Dc => f.write_str("dc"),
            NotifyLevel::Platform => f.write_str("platform"),
        }
    }
}

impl From<NotifyLevel> for String {
    fn from(n: NotifyLevel) -> String {
        n.to_string()
    }
}

impl TryFrom<String> for NotifyLevel {
    type Error = String;
    fn try_from(s: String) -> std::result::Result<Self, String> {
        Ok(match s.as_str() {
            "table" => NotifyLevel::Table,
            "database" => NotifyLevel::Database,
            "tenant" => NotifyLevel::Tenant,
            "dc" => NotifyLevel::Dc,
            "platform" => NotifyLevel::Platform,
            other => match other.strip_prefix("ancestor:").map(str::parse::<u64>) {
                Some(Ok(t)) => NotifyLevel::Ancestor(TenantId(t)),
                _ => return Err(format!("bad notify level {other:?}")),
            },
        })
    }
}

/// A recommendation's id, with its store in it: `t42.17`, `dc.mum1.3`,
/// `g.5`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub enum RecId {
    /// Row `n` of a tenant's catalog.
    Tenant(TenantId, u64),
    /// Item `n` of a DC tier.
    Dc(DcId, u64),
    /// Item `n` of the global tier.
    Global(u64),
}

impl RecId {
    /// The number within its store.
    #[must_use]
    pub const fn number(&self) -> u64 {
        match self {
            RecId::Tenant(_, n) | RecId::Dc(_, n) | RecId::Global(n) => *n,
        }
    }

    /// Parses the text form.
    ///
    /// # Errors
    /// 39972 for anything else.
    pub fn parse(s: &str) -> Result<RecId> {
        let bad = || {
            SprigError::new(
                SprigCode::RecommendationNotFound,
                format!("{s:?} is not a recommendation id (t<tenant>.<n>, dc.<dc>.<n> or g.<n>)"),
            )
        };
        let num = |x: &str| x.parse::<u64>().map_err(|_| bad());
        if let Some(rest) = s.strip_prefix("g.") {
            return Ok(RecId::Global(num(rest)?));
        }
        if let Some(rest) = s.strip_prefix("dc.") {
            let (dc, n) = rest.rsplit_once('.').ok_or_else(bad)?;
            let dc = DcId::new(dc).map_err(|_| bad())?;
            return Ok(RecId::Dc(dc, num(n)?));
        }
        if let Some(rest) = s.strip_prefix('t') {
            let (t, n) = rest.split_once('.').ok_or_else(bad)?;
            return Ok(RecId::Tenant(TenantId(num(t)?), num(n)?));
        }
        Err(bad())
    }
}

impl fmt::Display for RecId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecId::Tenant(t, n) => write!(f, "t{t}.{n}"),
            RecId::Dc(d, n) => write!(f, "dc.{d}.{n}"),
            RecId::Global(n) => write!(f, "g.{n}"),
        }
    }
}

impl From<RecId> for String {
    fn from(r: RecId) -> String {
        r.to_string()
    }
}

impl TryFrom<String> for RecId {
    type Error = String;
    fn try_from(s: String) -> std::result::Result<Self, String> {
        RecId::parse(&s).map_err(|e| e.message)
    }
}

/// What a deciding component proposes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Proposal {
    /// The kind.
    pub kind: Kind,
    /// What it is about.
    pub subject: Subject,
    /// Stable text naming what changes: `t42/o7/idx:3fa91c0e`, `t42/dc`.
    pub target: String,
    /// The statement or command.
    pub action: Action,
    /// Identifiers and numbers only.
    pub evidence: serde_json::Value,
    /// One number that grows with the evidence (re-raise factor).
    pub score: f64,
    /// One line for people.
    pub summary: String,
    /// Skip the wait when `approvals.allow_emergency` is on.
    #[serde(default)]
    pub emergency: bool,
}

impl Proposal {
    /// `hex(first 16 bytes of SHA-256(kind 0 target 0 machine))`, the
    /// machine form serialized with sorted keys.
    #[must_use]
    pub fn fingerprint(&self) -> String {
        let mut h = Sha256::new();
        h.update(self.kind.as_str().as_bytes());
        h.update([0]);
        h.update(self.target.as_bytes());
        h.update([0]);
        h.update(canonical(&self.action.machine).as_bytes());
        let d = h.finalize();
        d[..16].iter().fold(String::with_capacity(32), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        })
    }
}

/// JSON with object keys sorted, at every depth.
#[must_use]
pub fn canonical(v: &serde_json::Value) -> String {
    fn sort(v: &serde_json::Value) -> serde_json::Value {
        match v {
            serde_json::Value::Object(m) => {
                let mut keys: Vec<_> = m.keys().collect();
                keys.sort();
                let mut out = serde_json::Map::new();
                for k in keys {
                    out.insert(k.clone(), sort(&m[k]));
                }
                serde_json::Value::Object(out)
            }
            serde_json::Value::Array(a) => serde_json::Value::Array(a.iter().map(sort).collect()),
            other => other.clone(),
        }
    }
    // serde_json's map keeps insertion order only with `preserve_order`;
    // either way the sorted copy serializes in sorted order.
    sort(v).to_string()
}

/// One stored recommendation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Recommendation {
    /// Its id.
    pub id: RecId,
    /// The kind.
    pub kind: Kind,
    /// Who it belongs to.
    pub level: Level,
    /// Raised under `recommend`: no timers.
    pub advisory: bool,
    /// What it is about.
    pub subject: Subject,
    /// Stable text naming what changes.
    pub target: String,
    /// Dedupe key.
    pub fingerprint: String,
    /// Where it stands.
    pub state: State,
    /// One line for people.
    pub summary: String,
    /// The statement or command.
    pub action: Action,
    /// The latest evidence.
    pub evidence: serde_json::Value,
    /// The latest score.
    pub score: f64,
    /// The scope the mode came from, as text (`database`, `ancestor 7`).
    pub mode_source: String,
    /// Who is notified now.
    pub notify_level: NotifyLevel,
    /// The tenant's ancestors when raised, root first.
    #[serde(default)]
    pub ancestry: Vec<TenantId>,
    /// Escalation steps taken.
    pub escalations: u32,
    /// The escalation timer, seconds.
    pub escalate_after_s: Option<u64>,
    /// The auto-apply timer, seconds.
    pub auto_apply_after_s: Option<u64>,
    /// Next escalation.
    pub escalate_at_us: Option<Micros>,
    /// Auto-apply time.
    pub auto_apply_at_us: Option<Micros>,
    /// An approval window.
    pub window: Option<Window>,
    /// Snoozed until.
    pub snooze_until_us: Option<Micros>,
    /// The ignore's terms, while ignored (and kept after, for reopen).
    pub ignore: Option<Ignore>,
    /// Applied as an emergency.
    pub emergency: bool,
    /// Who decided (approve or ignore).
    pub decided_by: Option<String>,
    /// When.
    pub decided_at_us: Option<Micros>,
    /// Their reason.
    pub reason: Option<String>,
    /// The outcome of applying, or why it expired.
    pub result: Option<String>,
    /// Times the component proposed it again.
    pub refreshes: u32,
    /// Raised.
    pub created_at_us: Micros,
    /// Last proposed.
    pub refreshed_at_us: Micros,
    /// Last changed.
    pub updated_at_us: Micros,
    /// Optimistic concurrency: raised by every write.
    pub version: u64,
    /// The last [`MAX_HISTORY`] transitions.
    #[serde(default)]
    pub history: Vec<HistoryEntry>,
    /// The last [`MAX_COMMENTS`] comments.
    #[serde(default)]
    pub comments: Vec<Comment>,
}

impl Recommendation {
    /// Moves to `to`, recording who and why.
    pub fn transition(&mut self, to: State, by: &str, note: &str, now: Micros) {
        self.history.push(HistoryEntry {
            at_us: now,
            from: self.state,
            to,
            by: clip(by, 128),
            note: clip(note, MAX_TEXT),
        });
        if self.history.len() > MAX_HISTORY {
            let cut = self.history.len() - MAX_HISTORY;
            self.history.drain(..cut);
        }
        self.state = to;
        self.updated_at_us = now;
    }

    /// The tenant, if any.
    #[must_use]
    pub fn tenant(&self) -> Option<TenantId> {
        self.subject.tenant
    }

    /// Hours since it was raised.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn age_hours(&self, now: Micros) -> f64 {
        (now - self.created_at_us).max(0) as f64 / 3.6e9
    }
}

/// Converts a duration to microseconds, saturating.
#[must_use]
pub fn micros(d: Duration) -> Micros {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

/// Converts a wall time to microseconds since the epoch.
#[must_use]
pub fn wall_micros(t: std::time::SystemTime) -> Micros {
    t.duration_since(std::time::UNIX_EPOCH).map_or(0, micros)
}
