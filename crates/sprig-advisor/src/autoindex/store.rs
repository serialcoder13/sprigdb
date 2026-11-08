//! The automatic-indexing tables of the tenant catalog (chapter 13, "Data
//! structures and storage"; Appendix C; build step 1), as the `autoindex`
//! migration module, with typed rows and access.
//!
//! The module registers with `sprig-catalog`'s migrator ([`AUTOINDEX_MODULE`]),
//! so every tenant catalog gets these tables with its own: `sprig-advisor`
//! depends on `sprig-catalog`, never the reverse (chapter 08, "Tables owned
//! by other chapters"). The tables are not versioned: chapter 08 lists
//! "indexing" among the modules read on demand, never bumping a catalog
//! version, so writes here run in plain transactions, not through
//! `Versioner::change`.
//!
//! Format 1 creates:
//!
//! - `auto_indexes`, `auto_index_candidates` and `auto_index_validations`
//!   as Appendix C defines them, except `auto_index_validations.cluster_id`,
//!   which is `VARCHAR(48)` because cluster ids are text (`mum1-c7`,
//!   chapter 03) where the appendix has `INT UNSIGNED`;
//! - `index_origins`, which chapter 13 requires ("SQL index origins are
//!   kept in the catalog") but no chapter defines: one row per index whose
//!   origin can't be derived from its name (`user` and `migrated`); the
//!   primary key is `system` and `sprig_auto_*` names are `auto`
//!   ([`origin_of`]);
//! - the counter `sprig.next_autoindex_run_id` for evaluation runs.
//!
//! Decisions belong in chapter 11's `decisions` table, which doesn't exist
//! yet; [`DecisionLog`] is the hook, and [`LogDecisions`] logs them.
//!
//! **Times.** `DATETIME` columns hold UTC and travel as microseconds since
//! the epoch, as in the other catalog modules.

use std::collections::BTreeMap;
use std::fmt;
use std::time::SystemTime;

use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};
use serde::{Deserialize, Serialize};
use sprig_catalog::{Counter, Migration, Module, Phase, Store};
use sprig_common::ids::{Fingerprint, ObjectId, TenantId};
use sprig_common::names::{SchemaName, parse_auto_index, quote};
use sprig_common::{Result, SprigError};

use super::model::{
    AutoIndexId, Evidence, IndexDef, IndexKind, IndexOptions, KeyPart, Origin, Role,
    fingerprints_json, parse_fingerprints,
};
use crate::sql::{
    AT, col, json_col, json_req, last_id, map_err, micros, opt_time_col, row_count, time_col,
    to_json,
};

/// The module name in the catalog's format records.
pub const MODULE: &str = "autoindex";

/// `auto_indexes` (chapter 13, Appendix C).
pub const DDL_AUTO_INDEXES: &str = "CREATE TABLE IF NOT EXISTS auto_indexes (
  index_id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  object_id        BIGINT UNSIGNED NOT NULL,
  name             VARCHAR(64)     NOT NULL,
  model            ENUM('sql','document','kv','vector','geo','lookup') NOT NULL,
  kind             ENUM('btree','side_table','field_column','vector','spatial','s2',
                        'lookup','advice_wildcard','advice_search',
                        'advice_drop_migrated') NOT NULL,
  definition       JSON            NOT NULL,
  state            ENUM('validating','recommended','rolling_out','observing','active',
                        'retiring','dropping','dropped','rejected','failed',
                        'frozen') NOT NULL,
  prior_state      VARCHAR(16)     NULL,
  candidate_id     BIGINT UNSIGNED NOT NULL,
  validation_id    BIGINT UNSIGNED NULL,
  decision_id      BIGINT UNSIGNED NULL,
  est_benefit_ms_h DOUBLE          NOT NULL,
  est_write_ms_h   DOUBLE          NOT NULL,
  est_bytes        BIGINT UNSIGNED NOT NULL,
  actual_bytes     BIGINT UNSIGNED NULL,
  shards           JSON            NOT NULL,
  baseline         JSON            NOT NULL,
  observed         JSON            NULL,
  last_used_at     DATETIME        NULL,
  drop_reason      VARCHAR(48)     NULL,
  created_at       DATETIME(6)     NOT NULL,
  state_changed_at DATETIME(6)     NOT NULL,
  PRIMARY KEY (index_id),
  UNIQUE KEY by_name (object_id, name),
  KEY by_state (state, state_changed_at)
) ENGINE=InnoDB";

/// `auto_index_candidates` (chapter 13, Appendix C).
pub const DDL_CANDIDATES: &str = "CREATE TABLE IF NOT EXISTS auto_index_candidates (
  candidate_id   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  run_id         BIGINT UNSIGNED NOT NULL,
  object_id      BIGINT UNSIGNED NOT NULL,
  def_hash       BIGINT UNSIGNED NOT NULL,
  kind           VARCHAR(16)     NOT NULL,
  definition     JSON            NOT NULL,
  covering       BOOLEAN         NOT NULL,
  fingerprints   JSON            NOT NULL,
  status         ENUM('proposed','covered','analyze','costed','selected','deferred',
                      'rejected','recommended') NOT NULL,
  reason         VARCHAR(96)     NULL,
  benefit_ms_h   DOUBLE          NULL,
  write_ms_h     DOUBLE          NULL,
  est_bytes      BIGINT UNSIGNED NULL,
  build_seconds  INT UNSIGNED    NULL,
  evidence       JSON            NOT NULL,
  created_at     DATETIME(6)     NOT NULL,
  PRIMARY KEY (candidate_id),
  KEY by_object (object_id, def_hash),
  KEY by_run (run_id)
) ENGINE=InnoDB";

/// `auto_index_validations` (chapter 13, Appendix C), with `cluster_id`
/// as text (module docs).
pub const DDL_VALIDATIONS: &str = "CREATE TABLE IF NOT EXISTS auto_index_validations (
  validation_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  candidate_id      BIGINT UNSIGNED NOT NULL,
  method            ENUM('replica','galera_rsu','estimate_canary') NOT NULL,
  cluster_id        VARCHAR(48)     NOT NULL,
  node              VARCHAR(64)     NOT NULL,
  shard_id          BIGINT UNSIGNED NOT NULL,
  started_at        DATETIME(6)     NOT NULL,
  finished_at       DATETIME(6)     NULL,
  build_seconds     INT UNSIGNED    NULL,
  statements        BIGINT UNSIGNED NOT NULL DEFAULT 0,
  noisy             BIGINT UNSIGNED NOT NULL DEFAULT 0,
  target_p95_before DOUBLE          NULL,
  target_p95_after  DOUBLE          NULL,
  worst_regression  DOUBLE          NULL,
  worst_fp          BIGINT UNSIGNED NULL,
  recall_at_k       DOUBLE          NULL,
  verdict           ENUM('running','accepted','rejected','inconclusive',
                         'aborted') NOT NULL,
  reason            VARCHAR(128)    NULL,
  details           JSON            NOT NULL,
  PRIMARY KEY (validation_id),
  KEY by_candidate (candidate_id)
) ENGINE=InnoDB";

/// `index_origins`: the origin of indexes whose name doesn't tell it
/// (module docs). Not in Appendix C yet.
pub const DDL_INDEX_ORIGINS: &str = "CREATE TABLE IF NOT EXISTS index_origins (
  object_id   BIGINT UNSIGNED NOT NULL,
  index_name  VARCHAR(64)     NOT NULL,
  origin      ENUM('system','auto','user','migrated') NOT NULL,
  source      VARCHAR(48)     NOT NULL,
  recorded_at DATETIME(6)     NOT NULL,
  PRIMARY KEY (object_id, index_name)
) ENGINE=InnoDB";

/// `index_usage` (decision O1): user and migrated indexes whose removal
/// automatic indexing manages, with the state of their usage judgement and
/// their exact definition, so a removed one can be restored. Automatic
/// indexes keep the same state in `auto_indexes.observed`.
pub const DDL_INDEX_USAGE: &str = "CREATE TABLE IF NOT EXISTS index_usage (
  object_id        BIGINT UNSIGNED NOT NULL,
  index_name       VARCHAR(64)     NOT NULL,
  origin           ENUM('user','migrated') NOT NULL,
  state            ENUM('serving','recommended','retiring','dropped','pinned') NOT NULL,
  definition       TEXT            NOT NULL,
  observed         JSON            NOT NULL,
  drop_reason      VARCHAR(48)     NULL,
  created_at       DATETIME(6)     NOT NULL,
  state_changed_at DATETIME(6)     NOT NULL,
  PRIMARY KEY (object_id, index_name),
  KEY by_state (state, state_changed_at)
) ENGINE=InnoDB";

/// The run id counter, `sprig.next_autoindex_run_id`, starting at 1.
pub const INSERT_RUN_COUNTER: &str = "INSERT INTO settings
  (scope_kind, scope_id, setting_key, value, bounds, set_by, set_at, version) VALUES
  ('tenant', 0, 'sprig.next_autoindex_run_id', '1', NULL, 'system', UTC_TIMESTAMP(6), 0)
  ON DUPLICATE KEY UPDATE setting_key = setting_key";

/// The module's migrations.
pub static AUTOINDEX_MIGRATIONS: &[Migration] = &[
    Migration {
        format: 1,
        phase: Phase::Expand,
        name: "auto_indexes, auto_index_candidates, auto_index_validations, index_origins \
               and the run counter",
        statements: &[
            DDL_AUTO_INDEXES,
            DDL_CANDIDATES,
            DDL_VALIDATIONS,
            DDL_INDEX_ORIGINS,
            INSERT_RUN_COUNTER,
        ],
    },
    Migration {
        format: 2,
        phase: Phase::Expand,
        name: "index_usage (decision O1)",
        statements: &[DDL_INDEX_USAGE],
    },
];

/// The module, for [`Migrator::new`](sprig_catalog::Migrator::new) and
/// chapter 08's `BootstrapHooks.modules`.
pub const AUTOINDEX_MODULE: Module = Module {
    name: MODULE,
    migrations: AUTOINDEX_MIGRATIONS,
};

/// The evaluation run counter (`sprig.next_autoindex_run_id`).
pub const RUN_COUNTER: Counter = Counter::Other("autoindex_run");

macro_rules! sql_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident = $text:literal,)* }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $($(#[$vdoc])* $variant,)* }

        impl $name {
            /// Every value, in declaration order.
            pub const ALL: &'static [$name] = &[$($name::$variant),*];

            /// The SQL text.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $($name::$variant => $text,)* }
            }

            /// Parses the SQL text.
            ///
            /// # Errors
            /// `Internal` for any other text.
            pub fn parse(s: &str) -> Result<Self> {
                match s {
                    $($text => Ok($name::$variant),)*
                    _ => Err(SprigError::internal(format!(
                        concat!("autoindex catalog: bad ", stringify!($name), " {:?}"), s
                    ))),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

sql_enum! {
    /// `auto_indexes.model`.
    Model {
        /// SQL tables.
        Sql = "sql",
        /// Collections (R5).
        Document = "document",
        /// Key-value namespaces, Redis included (R6).
        Kv = "kv",
        /// Vector indexes (R6).
        Vector = "vector",
        /// Geospatial (R6).
        Geo = "geo",
        /// Global lookup indexes (R1).
        Lookup = "lookup",
    }
}

sql_enum! {
    /// `auto_indexes.kind` and `auto_index_candidates.kind`: an
    /// [`IndexKind`] or a recommendation that is never applied.
    StoredKind {
        /// A B-tree.
        BTree = "btree",
        /// A side table.
        SideTable = "side_table",
        /// A field column.
        FieldColumn = "field_column",
        /// A vector index.
        Vector = "vector",
        /// A spatial index.
        Spatial = "spatial",
        /// An S2 side table.
        S2 = "s2",
        /// A lookup index.
        Lookup = "lookup",
        /// Recommend a wildcard index.
        AdviceWildcard = "advice_wildcard",
        /// Recommend a search index.
        AdviceSearch = "advice_search",
        /// Recommend dropping a migrated index.
        AdviceDropMigrated = "advice_drop_migrated",
    }
}

impl StoredKind {
    /// The index kind, `None` for advice.
    #[must_use]
    pub const fn index_kind(self) -> Option<IndexKind> {
        Some(match self {
            StoredKind::BTree => IndexKind::BTree,
            StoredKind::SideTable => IndexKind::SideTable,
            StoredKind::FieldColumn => IndexKind::FieldColumn,
            StoredKind::Vector => IndexKind::Vector,
            StoredKind::Spatial => IndexKind::Spatial,
            StoredKind::S2 => IndexKind::S2,
            StoredKind::Lookup => IndexKind::Lookup,
            StoredKind::AdviceWildcard
            | StoredKind::AdviceSearch
            | StoredKind::AdviceDropMigrated => return None,
        })
    }

    /// Advice only ever reaches `recommended`.
    #[must_use]
    pub const fn is_advice(self) -> bool {
        self.index_kind().is_none()
    }
}

impl From<IndexKind> for StoredKind {
    fn from(k: IndexKind) -> Self {
        match k {
            IndexKind::BTree => StoredKind::BTree,
            IndexKind::SideTable => StoredKind::SideTable,
            IndexKind::FieldColumn => StoredKind::FieldColumn,
            IndexKind::Vector => StoredKind::Vector,
            IndexKind::Spatial => StoredKind::Spatial,
            IndexKind::S2 => StoredKind::S2,
            IndexKind::Lookup => StoredKind::Lookup,
        }
    }
}

sql_enum! {
    /// `auto_indexes.state` (chapter 13, "State machines").
    IndexState {
        /// Built on a validation node and replayed.
        Validating = "validating",
        /// Stopped with its DDL and evidence (`recommend` mode, advice).
        Recommended = "recommended",
        /// Being built shard by shard.
        RollingOut = "rolling_out",
        /// Built everywhere; watched for `observe_days`.
        Observing = "observing",
        /// Kept.
        Active = "active",
        /// `IGNORED`, waiting to be dropped.
        Retiring = "retiring",
        /// Being dropped on each shard.
        Dropping = "dropping",
        /// Gone; the row is history.
        Dropped = "dropped",
        /// Validation refused it.
        Rejected = "rejected",
        /// Rollout failed 3 times.
        Failed = "failed",
        /// `sprigctl index freeze`: nothing changes it.
        Frozen = "frozen",
    }
}

impl IndexState {
    /// No further transitions (except history purging).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            IndexState::Dropped | IndexState::Rejected | IndexState::Failed
        )
    }

    /// The index exists (or may exist) on some shard and counts against
    /// the table's budget: from rollout until it is dropped.
    #[must_use]
    pub const fn counts_for_budget(self) -> bool {
        matches!(
            self,
            IndexState::RollingOut
                | IndexState::Observing
                | IndexState::Active
                | IndexState::Retiring
                | IndexState::Dropping
                | IndexState::Frozen
        )
    }

    /// Whether `self → to` is an edge of chapter 13's state machine.
    /// `frozen` is reachable from any state and returns to the prior one
    /// (checked by [`AutoIndexStore::unfreeze`]); `dropped` is reachable from
    /// any live state when the table is dropped or the reconciler finds the
    /// index gone.
    #[must_use]
    pub fn can_move_to(self, to: IndexState) -> bool {
        use IndexState as S;
        // `failed` is terminal except for the clean-up of what a failed
        // build left on the shards: the observer moves it to `dropping`.
        if self == to
            || self.is_terminal()
                && !matches!((self, to), (_, S::Frozen) | (S::Failed, S::Dropping))
        {
            return false;
        }
        if to == S::Frozen {
            return self != S::Frozen;
        }
        if to == S::Dropped {
            // DROP TABLE, reconciler drift, an expired recommendation, or
            // a finished drop.
            return self != S::Frozen;
        }
        matches!(
            (self, to),
            (S::Validating, S::Rejected | S::Recommended | S::RollingOut)
                | (S::Recommended, S::RollingOut)
                | (S::RollingOut, S::Observing | S::Failed | S::Dropping)
                | (S::Observing, S::Active | S::Retiring | S::Dropping)
                | (S::Active, S::Retiring | S::Dropping)
                | (S::Retiring, S::Dropping | S::Active)
                | (S::Failed, S::Dropping)
        )
    }
}

sql_enum! {
    /// `auto_index_candidates.status`.
    CandidateStatus {
        /// Generated.
        Proposed = "proposed",
        /// An existing index serves it (`covered_by:<name>`).
        Covered = "covered",
        /// An existing index should serve it but the optimizer doesn't use
        /// it: statistics are refreshed first.
        Analyze = "analyze",
        /// Passed the cost gates.
        Costed = "costed",
        /// Chosen for validation.
        Selected = "selected",
        /// Over budget or out of the day's cap; re-evaluated later.
        Deferred = "deferred",
        /// Failed a gate (`below_min_net`, ...).
        Rejected = "rejected",
        /// Stopped at a recommendation.
        Recommended = "recommended",
    }
}

sql_enum! {
    /// `auto_index_validations.method`.
    ValidationMethod {
        /// Build on a replica with binary logging off.
        Replica = "replica",
        /// Build on a desynced Galera node (RSU).
        GaleraRsu = "galera_rsu",
        /// Estimate plus a one-shard canary.
        EstimateCanary = "estimate_canary",
    }
}

sql_enum! {
    /// `auto_index_validations.verdict`.
    Verdict {
        /// In progress.
        Running = "running",
        /// Accepted.
        Accepted = "accepted",
        /// Rejected.
        Rejected = "rejected",
        /// Too noisy to tell; retried in another window.
        Inconclusive = "inconclusive",
        /// Aborted (node promoted, lost, lagging; user DDL).
        Aborted = "aborted",
    }
}

sql_enum! {
    /// `auto_indexes.drop_reason`.
    DropReason {
        /// Observation: the target fingerprints didn't improve enough.
        NoGain = "no_gain",
        /// Observation: a fingerprint using it got slower.
        Regression = "regression",
        /// Observation: writes got slower and the benefit didn't cover it.
        WriteCost = "write_cost",
        /// Rarely used and costing more than it saves, over
        /// `usage_sample_reads` reads (decision O1).
        Unused = "unused",
        /// A user index equals or extends it.
        Superseded = "superseded",
        /// A better automatic index replaced it under the budget.
        Replaced = "replaced",
        /// Over a tightened budget.
        Budget = "budget",
        /// The table was dropped.
        TableDropped = "table_dropped",
        /// A recommendation expired.
        Expired = "expired",
        /// The reconciler found it changed or gone.
        Drift = "drift",
        /// `sprigctl index drop`.
        Operator = "operator",
        /// Room for a user index under MariaDB's 64-index limit.
        IndexLimit = "index_limit",
        /// `sprigctl index reject`: an operator turned a recommendation
        /// down.
        Dismissed = "dismissed",
    }
}

impl DropReason {
    /// A drop that says the definition is harmful or useless, so it may
    /// not be proposed again for `auto_index.recreate_block_days`: the
    /// observer's verdicts, and an operator's drop or rejection (chapter
    /// 07: an index a person dropped isn't rebuilt behind their back).
    #[must_use]
    pub const fn blocks_recreate(self) -> bool {
        matches!(
            self,
            DropReason::NoGain
                | DropReason::Regression
                | DropReason::WriteCost
                | DropReason::Operator
                | DropReason::Dismissed
        )
    }
}

/// `auto_indexes.definition` and `auto_index_candidates.definition`: the
/// parts and options of an [`IndexDef`] without its object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Definition {
    /// Key parts.
    pub parts: Vec<KeyPart>,
    /// Kind-specific options.
    #[serde(default, skip_serializing_if = "is_empty")]
    pub options: IndexOptions,
}

fn is_empty(o: &IndexOptions) -> bool {
    o.0.is_empty()
}

impl Definition {
    /// The stored form of `def`.
    #[must_use]
    pub fn of(def: &IndexDef) -> Self {
        Self {
            parts: def.parts.clone(),
            options: def.options.clone(),
        }
    }

    /// The definition on `object`.
    #[must_use]
    pub fn on(&self, object: ObjectId, kind: IndexKind) -> IndexDef {
        IndexDef {
            object,
            kind,
            parts: self.parts.clone(),
            options: self.options.clone(),
        }
    }
}

/// Build state of one shard in `auto_indexes.shards`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardBuild {
    /// Not built yet.
    Pending,
    /// Building now.
    Building,
    /// Built.
    Built,
    /// `IGNORED`.
    Ignored,
    /// Dropped.
    Dropped,
}

/// One fingerprint's baseline in `auto_indexes.baseline`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Baseline {
    /// Runs in the window.
    pub runs: u64,
    /// p95 in ms.
    pub p95_ms: f64,
    /// Mean in ms.
    pub mean_ms: f64,
}

/// An `auto_indexes` row.
#[derive(Debug, Clone, PartialEq)]
pub struct AutoIndexRow {
    /// The id (ignored on insert).
    pub index_id: AutoIndexId,
    /// The catalog object.
    pub object: ObjectId,
    /// `sprig_auto_<8 hex>`, a side table or a column.
    pub name: String,
    /// The data model.
    pub model: Model,
    /// The kind.
    pub kind: StoredKind,
    /// Parts and options.
    pub definition: Definition,
    /// The state.
    pub state: IndexState,
    /// Restored when unfrozen.
    pub prior_state: Option<IndexState>,
    /// The candidate it came from.
    pub candidate_id: u64,
    /// The validation that accepted it.
    pub validation_id: Option<u64>,
    /// The decision that created it.
    pub decision_id: Option<u64>,
    /// B, ms saved per hour.
    pub est_benefit_ms_h: f64,
    /// Wc, ms added per hour.
    pub est_write_ms_h: f64,
    /// S, bytes.
    pub est_bytes: u64,
    /// Measured size.
    pub actual_bytes: Option<u64>,
    /// Per shard: build state.
    pub shards: BTreeMap<String, ShardBuild>,
    /// Per fingerprint (16 hex digits): runs, p95, mean.
    pub baseline: BTreeMap<String, Baseline>,
    /// What observation measured.
    pub observed: Option<serde_json::Value>,
    /// Last time it was seen used.
    pub last_used_at: Option<SystemTime>,
    /// Why it was dropped or is being retired.
    pub drop_reason: Option<DropReason>,
    /// Created.
    pub created_at: SystemTime,
    /// Last state change.
    pub state_changed_at: SystemTime,
}

impl AutoIndexRow {
    /// The definition as an [`IndexDef`], `None` for advice.
    #[must_use]
    pub fn def(&self) -> Option<IndexDef> {
        self.kind
            .index_kind()
            .map(|k| self.definition.on(self.object, k))
    }
}

/// An `auto_index_candidates` row.
#[derive(Debug, Clone, PartialEq)]
pub struct CandidateRow {
    /// The id (ignored on insert).
    pub candidate_id: u64,
    /// The evaluation run.
    pub run_id: u64,
    /// The catalog object.
    pub object: ObjectId,
    /// `xxh3_64` of the canonical definition.
    pub def_hash: u64,
    /// The kind.
    pub kind: StoredKind,
    /// Parts and options.
    pub definition: Definition,
    /// Covering.
    pub covering: bool,
    /// Fingerprints served.
    pub fingerprints: Vec<(Fingerprint, Role)>,
    /// Status.
    pub status: CandidateStatus,
    /// `covered_by:<index>`, `below_min_net`, ...
    pub reason: Option<String>,
    /// B, ms per hour.
    pub benefit_ms_h: Option<f64>,
    /// Wc, ms per hour.
    pub write_ms_h: Option<f64>,
    /// S, bytes.
    pub est_bytes: Option<u64>,
    /// Tb, seconds.
    pub build_seconds: Option<u32>,
    /// Selectivities, buckets, plans.
    pub evidence: Evidence,
    /// Created.
    pub created_at: SystemTime,
}

impl CandidateRow {
    /// A table of fingerprint `fp` that got no candidate, recorded so
    /// `sprigctl explain query` can say why: no key parts, status
    /// `rejected`, `reason` the generator's (`function:LOWER`,
    /// `no_sargable_predicate`, ...), with `notes` (column names, never
    /// values) as evidence.
    #[must_use]
    pub fn skipped(
        run_id: u64,
        object: ObjectId,
        fp: Fingerprint,
        reason: &str,
        notes: Vec<String>,
        at: SystemTime,
    ) -> Self {
        Self {
            candidate_id: 0,
            run_id,
            object,
            def_hash: 0,
            kind: StoredKind::BTree,
            definition: Definition::default(),
            covering: false,
            fingerprints: vec![(fp, Role::Primary)],
            status: CandidateStatus::Rejected,
            reason: Some(reason.to_owned()),
            benefit_ms_h: None,
            write_ms_h: None,
            est_bytes: None,
            build_seconds: None,
            evidence: Evidence {
                notes,
                ..Evidence::default()
            },
            created_at: at,
        }
    }

    /// Whether this row records a table that got no candidate
    /// ([`skipped`](Self::skipped)).
    #[must_use]
    pub fn is_skip(&self) -> bool {
        self.definition.parts.is_empty()
    }
}

/// An `auto_index_validations` row.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidationRow {
    /// The id (ignored on insert).
    pub validation_id: u64,
    /// The candidate.
    pub candidate_id: u64,
    /// How.
    pub method: ValidationMethod,
    /// The cluster (text id).
    pub cluster_id: String,
    /// The node.
    pub node: String,
    /// The shard.
    pub shard_id: u64,
    /// Started.
    pub started_at: SystemTime,
    /// Finished.
    pub finished_at: Option<SystemTime>,
    /// Build time on the node.
    pub build_seconds: Option<u32>,
    /// Statements replayed.
    pub statements: u64,
    /// Of those, noisy.
    pub noisy: u64,
    /// Target p95 before, ms.
    pub target_p95_before: Option<f64>,
    /// Target p95 after, ms.
    pub target_p95_after: Option<f64>,
    /// Worst slowdown, percent.
    pub worst_regression: Option<f64>,
    /// The fingerprint of the worst slowdown.
    pub worst_fp: Option<Fingerprint>,
    /// Vector candidates.
    pub recall_at_k: Option<f64>,
    /// The verdict.
    pub verdict: Verdict,
    /// Why.
    pub reason: Option<String>,
    /// Per statement: sample ids and timings, never values.
    pub details: serde_json::Value,
}

/// An `index_origins` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginRow {
    /// The object.
    pub object: ObjectId,
    /// The index name in MariaDB.
    pub index_name: String,
    /// Its origin.
    pub origin: Origin,
    /// What recorded it: `ddl`, `import`, `mysqldump`, `mongorestore`, ...
    pub source: String,
    /// When.
    pub recorded_at: SystemTime,
}

/// The origin of index `name` on an object, given the recorded origins:
/// `PRIMARY` is `system`; a name matching `^sprig_auto_[0-9a-f]{8}$` is
/// `auto`; otherwise the
/// recorded origin, else `user`.
#[must_use]
pub fn origin_of(name: &str, recorded: Option<Origin>) -> Origin {
    if name.eq_ignore_ascii_case("PRIMARY") {
        Origin::System
    } else if parse_auto_index(name).is_some() {
        Origin::Auto
    } else {
        recorded.unwrap_or(Origin::User)
    }
}

const INDEX_COLS: &str = "index_id, object_id, name, model, kind, definition, state, \
    prior_state, candidate_id, validation_id, decision_id, est_benefit_ms_h, est_write_ms_h, \
    est_bytes, actual_bytes, shards, baseline, observed, \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', last_used_at), drop_reason, \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', created_at), \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', state_changed_at)";

const CANDIDATE_COLS: &str = "candidate_id, run_id, object_id, def_hash, kind, definition, \
    covering, fingerprints, status, reason, benefit_ms_h, write_ms_h, est_bytes, \
    build_seconds, evidence, TIMESTAMPDIFF(MICROSECOND, '1970-01-01', created_at)";

const VALIDATION_COLS: &str = "validation_id, candidate_id, method, cluster_id, node, shard_id, \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', started_at), \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', finished_at), build_seconds, statements, noisy, \
    target_p95_before, target_p95_after, worst_regression, worst_fp, recall_at_k, verdict, \
    reason, details";

/// A state change applied by [`AutoIndexStore::set_state`].
#[derive(Debug, Clone, PartialEq)]
pub struct StateChange {
    /// The state the row must be in.
    pub from: IndexState,
    /// The new state.
    pub to: IndexState,
    /// When.
    pub at: SystemTime,
    /// Set with `retiring`, `dropping` and `dropped`.
    pub drop_reason: Option<DropReason>,
}

/// Typed access to one catalog's automatic-indexing tables. Every method
/// takes the connection or transaction to run on.
#[derive(Debug, Clone)]
pub struct AutoIndexStore {
    schema: String,
    indexes: String,
    candidates: String,
    validations: String,
    origins: String,
    usage: String,
}

impl AutoIndexStore {
    /// The tables of catalog schema `schema`.
    #[must_use]
    pub fn new(schema: impl Into<String>) -> Self {
        let schema = schema.into();
        let q = quote(&schema);
        Self {
            indexes: format!("{q}.auto_indexes"),
            candidates: format!("{q}.auto_index_candidates"),
            validations: format!("{q}.auto_index_validations"),
            origins: format!("{q}.index_origins"),
            usage: format!("{q}.index_usage"),
            schema,
        }
    }

    /// The tables of `tenant`'s catalog, `t<tenant>_sprig`.
    #[must_use]
    pub fn for_tenant(tenant: TenantId) -> Self {
        Self::new(SchemaName::catalog(tenant).to_string())
    }

    /// The tables next to `store`'s core tables.
    #[must_use]
    pub fn of(store: &Store) -> Self {
        Self::new(store.schema())
    }

    /// The catalog schema.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Reserves the next evaluation run id.
    ///
    /// # Errors
    /// 39072 if the counter row is missing (the module isn't migrated);
    /// 39070 or a backend error.
    pub async fn next_run_id<Q: Queryable + Send>(&self, core: &Store, q: &mut Q) -> Result<u64> {
        core.next_ids(q, RUN_COUNTER, 1).await
    }

    // ---- auto_indexes ----

    /// Inserts an index row and returns its id. An existing
    /// `(object_id, name)` is MariaDB's 1062: the same definition already
    /// has a row (see [`index_by_name`](Self::index_by_name)).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn insert_index<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        r: &AutoIndexRow,
    ) -> Result<AutoIndexId> {
        let sql = format!(
            "INSERT INTO {} (object_id, name, model, kind, definition, state, prior_state, \
             candidate_id, validation_id, decision_id, est_benefit_ms_h, est_write_ms_h, \
             est_bytes, actual_bytes, shards, baseline, observed, last_used_at, drop_reason, \
             created_at, state_changed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
             ?, ?, {}, ?, {AT}, {AT})",
            self.indexes,
            opt_at(r.last_used_at.is_some()),
        );
        let mut p: Vec<Value> = vec![
            r.object.0.into(),
            r.name.as_str().into(),
            r.model.as_str().into(),
            r.kind.as_str().into(),
            to_json(&r.definition)?.into(),
            r.state.as_str().into(),
            r.prior_state.map(IndexState::as_str).into(),
            r.candidate_id.into(),
            r.validation_id.into(),
            r.decision_id.into(),
            r.est_benefit_ms_h.into(),
            r.est_write_ms_h.into(),
            r.est_bytes.into(),
            r.actual_bytes.into(),
            to_json(&r.shards)?.into(),
            to_json(&r.baseline)?.into(),
            r.observed.as_ref().map(to_json).transpose()?.into(),
        ];
        if let Some(t) = r.last_used_at {
            p.push(micros(t).into());
        }
        p.push(r.drop_reason.map(DropReason::as_str).into());
        p.push(micros(r.created_at).into());
        p.push(micros(r.state_changed_at).into());
        q.exec_drop(sql, p).await.map_err(map_err)?;
        Ok(AutoIndexId(last_id(q).await?))
    }

    /// The index `id`.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a malformed row.
    pub async fn index<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: AutoIndexId,
    ) -> Result<Option<AutoIndexRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {INDEX_COLS} FROM {} WHERE index_id = ?",
                    self.indexes
                ),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().next().map(index_row).transpose()
    }

    /// The index called `name` on `object`.
    ///
    /// # Errors
    /// As [`index`](Self::index).
    pub async fn index_by_name<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        name: &str,
    ) -> Result<Option<AutoIndexRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {INDEX_COLS} FROM {} WHERE object_id = ? AND name = ?",
                    self.indexes
                ),
                (object.0, name),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().next().map(index_row).transpose()
    }

    /// Every index row of `object`, dropped ones included, by id.
    ///
    /// # Errors
    /// As [`index`](Self::index).
    pub async fn indexes_on<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
    ) -> Result<Vec<AutoIndexRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {INDEX_COLS} FROM {} WHERE object_id = ? ORDER BY index_id",
                    self.indexes
                ),
                (object.0,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(index_row).collect()
    }

    /// Every index row in one of `states`, oldest state change first.
    ///
    /// # Errors
    /// As [`index`](Self::index).
    pub async fn indexes_in<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        states: &[IndexState],
    ) -> Result<Vec<AutoIndexRow>> {
        if states.is_empty() {
            return Ok(Vec::new());
        }
        let marks = vec!["?"; states.len()].join(", ");
        let p: Vec<Value> = states.iter().map(|s| s.as_str().into()).collect();
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {INDEX_COLS} FROM {} WHERE state IN ({marks}) \
                     ORDER BY state_changed_at, index_id",
                    self.indexes
                ),
                p,
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(index_row).collect()
    }

    /// Every index row, by object and id; dropped and rejected ones (kept
    /// a year as history) only with `history` (`sprigctl index list`).
    ///
    /// # Errors
    /// As [`index`](Self::index).
    pub async fn list_indexes<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        history: bool,
    ) -> Result<Vec<AutoIndexRow>> {
        let cond = if history {
            ""
        } else {
            " WHERE state NOT IN ('dropped','rejected')"
        };
        let rows: Vec<Row> = q
            .query(format!(
                "SELECT {INDEX_COLS} FROM {}{cond} ORDER BY object_id, index_id",
                self.indexes
            ))
            .await
            .map_err(map_err)?;
        rows.into_iter().map(index_row).collect()
    }

    /// The index rows that came from candidate `candidate_id`.
    ///
    /// # Errors
    /// As [`index`](Self::index).
    pub async fn indexes_of_candidate<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        candidate_id: u64,
    ) -> Result<Vec<AutoIndexRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {INDEX_COLS} FROM {} WHERE candidate_id = ? ORDER BY index_id",
                    self.indexes
                ),
                (candidate_id,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(index_row).collect()
    }

    /// Index rows created at or after `since` (for the per-day caps).
    ///
    /// # Errors
    /// As [`index`](Self::index).
    pub async fn created_since<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        since: SystemTime,
    ) -> Result<Vec<AutoIndexRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {INDEX_COLS} FROM {} WHERE created_at >= {AT} ORDER BY index_id",
                    self.indexes
                ),
                (micros(since),),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(index_row).collect()
    }

    /// Dropped rows of `object` whose state changed at or after `since`
    /// (churn dampers: `recreate_block_days`, the churn metric).
    ///
    /// # Errors
    /// As [`index`](Self::index).
    pub async fn dropped_since<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        since: SystemTime,
    ) -> Result<Vec<AutoIndexRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {INDEX_COLS} FROM {} WHERE object_id = ? AND state = 'dropped' \
                     AND state_changed_at >= {AT} ORDER BY index_id",
                    self.indexes
                ),
                (object.0, micros(since)),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(index_row).collect()
    }

    /// Moves index `id` from `c.from` to `c.to`, if it is still in
    /// `c.from` and the edge exists ([`IndexState::can_move_to`]). Moving to
    /// `frozen` keeps the previous state in `prior_state`. Returns whether
    /// the row changed; `false` means another writer moved it first.
    ///
    /// # Errors
    /// `Internal` for an edge the state machine doesn't have; 39070 or a
    /// backend error.
    pub async fn set_state<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: AutoIndexId,
        c: &StateChange,
    ) -> Result<bool> {
        if !c.from.can_move_to(c.to) {
            return Err(SprigError::internal(format!(
                "auto index {id}: no transition {} -> {}",
                c.from, c.to
            )));
        }
        let prior = (c.to == IndexState::Frozen).then_some(c.from.as_str());
        q.exec_drop(
            format!(
                "UPDATE {} SET state = ?, prior_state = ?, state_changed_at = {AT}, \
                 drop_reason = COALESCE(?, drop_reason) WHERE index_id = ? AND state = ?",
                self.indexes
            ),
            (
                c.to.as_str(),
                prior,
                micros(c.at),
                c.drop_reason.map(DropReason::as_str),
                id.0,
                c.from.as_str(),
            ),
        )
        .await
        .map_err(map_err)?;
        Ok(row_count(q).await? == 1)
    }

    /// `sprigctl index freeze --off`: restores the state saved in
    /// `prior_state`. Returns the restored state, or `None` if the row
    /// isn't frozen.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a bad `prior_state`.
    pub async fn unfreeze<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: AutoIndexId,
        at: SystemTime,
    ) -> Result<Option<IndexState>> {
        let prior: Option<Option<String>> = q
            .exec_first(
                format!(
                    "SELECT prior_state FROM {} WHERE index_id = ? AND state = 'frozen' \
                     FOR UPDATE",
                    self.indexes
                ),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
        let Some(prior) = prior else {
            return Ok(None);
        };
        let to = IndexState::parse(prior.as_deref().unwrap_or("active"))?;
        q.exec_drop(
            format!(
                "UPDATE {} SET state = ?, prior_state = NULL, state_changed_at = {AT} \
                 WHERE index_id = ? AND state = 'frozen'",
                self.indexes
            ),
            (to.as_str(), micros(at), id.0),
        )
        .await
        .map_err(map_err)?;
        Ok((row_count(q).await? == 1).then_some(to))
    }

    /// Sets one shard's build state.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn set_shard<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: AutoIndexId,
        shard: &str,
        s: ShardBuild,
    ) -> Result<bool> {
        let state = to_json(&s)?;
        q.exec_drop(
            format!(
                "UPDATE {} SET shards = JSON_SET(shards, CONCAT('$.\"', ?, '\"'), \
                 JSON_EXTRACT(?, '$')) WHERE index_id = ?",
                self.indexes
            ),
            (shard, state, id.0),
        )
        .await
        .map_err(map_err)?;
        Ok(row_count(q).await? == 1)
    }

    /// Records that the index was used at `at`.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn touch_used<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: AutoIndexId,
        at: SystemTime,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET last_used_at = {AT} WHERE index_id = ?",
                self.indexes
            ),
            (micros(at), id.0),
        )
        .await
        .map_err(map_err)
    }

    /// Replaces `auto_indexes.observed`: the rollout's progress (attempts,
    /// failures, the next attempt) and the observer's measurements live
    /// there under `rollout`, `watch` and `usage`. Returns whether the row
    /// exists.
    ///
    /// Added by the rollout and observer (chapter 13 steps 10 and 11);
    /// additive.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn set_observed<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: AutoIndexId,
        observed: &serde_json::Value,
    ) -> Result<bool> {
        q.exec_drop(
            format!(
                "UPDATE {} SET observed = ? WHERE index_id = ?",
                self.indexes
            ),
            (to_json(observed)?, id.0),
        )
        .await
        .map_err(map_err)?;
        // ROW_COUNT() is 0 for an unchanged value too; check existence.
        let n: Option<u64> = q
            .exec_first(
                format!("SELECT COUNT(*) FROM {} WHERE index_id = ?", self.indexes),
                (id.0,),
            )
            .await
            .map_err(map_err)?;
        Ok(n == Some(1))
    }

    /// Records the validation that accepted index `id`
    /// (`auto_indexes.validation_id`).
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn set_validation<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: AutoIndexId,
        validation_id: u64,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET validation_id = ? WHERE index_id = ?",
                self.indexes
            ),
            (validation_id, id.0),
        )
        .await
        .map_err(map_err)
    }

    // ---- auto_index_candidates ----

    /// Inserts a candidate and returns its id.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn insert_candidate<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        r: &CandidateRow,
    ) -> Result<u64> {
        let p: Vec<Value> = vec![
            r.run_id.into(),
            r.object.0.into(),
            r.def_hash.into(),
            r.kind.as_str().into(),
            to_json(&r.definition)?.into(),
            r.covering.into(),
            to_json(&fingerprints_json(&r.fingerprints))?.into(),
            r.status.as_str().into(),
            r.reason.as_deref().map(|s| truncate(s, 96)).into(),
            r.benefit_ms_h.into(),
            r.write_ms_h.into(),
            r.est_bytes.into(),
            r.build_seconds.into(),
            to_json(&r.evidence)?.into(),
            micros(r.created_at).into(),
        ];
        q.exec_drop(
            format!(
                "INSERT INTO {} (run_id, object_id, def_hash, kind, definition, covering, \
                 fingerprints, status, reason, benefit_ms_h, write_ms_h, est_bytes, \
                 build_seconds, evidence, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                 ?, ?, ?, {AT})",
                self.candidates
            ),
            p,
        )
        .await
        .map_err(map_err)?;
        last_id(q).await
    }

    /// Records the cost model's numbers and a status.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn set_candidate<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        r: &CandidateRow,
    ) -> Result<bool> {
        q.exec_drop(
            format!(
                "UPDATE {} SET status = ?, reason = ?, benefit_ms_h = ?, write_ms_h = ?, \
                 est_bytes = ?, build_seconds = ?, fingerprints = ?, evidence = ? \
                 WHERE candidate_id = ?",
                self.candidates
            ),
            vec![
                Value::from(r.status.as_str()),
                r.reason.as_deref().map(|s| truncate(s, 96)).into(),
                r.benefit_ms_h.into(),
                r.write_ms_h.into(),
                r.est_bytes.into(),
                r.build_seconds.into(),
                to_json(&fingerprints_json(&r.fingerprints))?.into(),
                to_json(&r.evidence)?.into(),
                r.candidate_id.into(),
            ],
        )
        .await
        .map_err(map_err)?;
        Ok(row_count(q).await? == 1)
    }

    /// The candidate `id`.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a malformed row.
    pub async fn candidate<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: u64,
    ) -> Result<Option<CandidateRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {CANDIDATE_COLS} FROM {} WHERE candidate_id = ?",
                    self.candidates
                ),
                (id,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().next().map(candidate_row).transpose()
    }

    /// The candidates of one evaluation run.
    ///
    /// # Errors
    /// As [`candidate`](Self::candidate).
    pub async fn candidates_of_run<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        run_id: u64,
    ) -> Result<Vec<CandidateRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {CANDIDATE_COLS} FROM {} WHERE run_id = ? ORDER BY candidate_id",
                    self.candidates
                ),
                (run_id,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(candidate_row).collect()
    }

    /// Candidates of `object` with definition hash `def_hash` created at or
    /// after `since`, newest first (cooldowns, history for `explain`).
    ///
    /// # Errors
    /// As [`candidate`](Self::candidate).
    pub async fn candidates_for<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        def_hash: u64,
        since: SystemTime,
    ) -> Result<Vec<CandidateRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {CANDIDATE_COLS} FROM {} WHERE object_id = ? AND def_hash = ? \
                     AND created_at >= {AT} ORDER BY candidate_id DESC",
                    self.candidates
                ),
                (object.0, def_hash, micros(since)),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(candidate_row).collect()
    }

    /// Candidates decided (not just proposed) at or after `since`: the
    /// fingerprints they serve are in their cooldown. Rows recording a
    /// table that got no candidate ([`CandidateRow::skipped`], `def_hash`
    /// 0) decide nothing and are left out: a fingerprint waiting for its
    /// probes is evaluated again next round.
    ///
    /// # Errors
    /// As [`candidate`](Self::candidate).
    pub async fn decided_since<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        since: SystemTime,
    ) -> Result<Vec<CandidateRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {CANDIDATE_COLS} FROM {} WHERE created_at >= {AT} \
                     AND status IN ('selected','rejected','recommended','deferred') \
                     AND def_hash <> 0 ORDER BY candidate_id",
                    self.candidates
                ),
                (micros(since),),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(candidate_row).collect()
    }

    /// Candidates serving fingerprint `fp` (as primary or merged) created
    /// at or after `since`, newest first (`sprigctl explain query`).
    ///
    /// # Errors
    /// As [`candidate`](Self::candidate).
    pub async fn candidates_serving<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        fp: Fingerprint,
        since: SystemTime,
    ) -> Result<Vec<CandidateRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {CANDIDATE_COLS} FROM {} WHERE created_at >= {AT} \
                     AND JSON_CONTAINS(fingerprints, JSON_OBJECT('id', ?)) \
                     ORDER BY candidate_id DESC",
                    self.candidates
                ),
                (micros(since), fp.to_string()),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(candidate_row).collect()
    }

    // ---- auto_index_validations ----

    /// The validations of candidate `candidate_id`, newest first.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a malformed row.
    pub async fn validations_of<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        candidate_id: u64,
    ) -> Result<Vec<ValidationRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {VALIDATION_COLS} FROM {} WHERE candidate_id = ? \
                     ORDER BY validation_id DESC",
                    self.validations
                ),
                (candidate_id,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(validation_row).collect()
    }

    /// Inserts a validation and returns its id.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn insert_validation<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        r: &ValidationRow,
    ) -> Result<u64> {
        let mut p: Vec<Value> = vec![
            r.candidate_id.into(),
            r.method.as_str().into(),
            r.cluster_id.as_str().into(),
            r.node.as_str().into(),
            r.shard_id.into(),
            micros(r.started_at).into(),
        ];
        if let Some(t) = r.finished_at {
            p.push(micros(t).into());
        }
        p.extend([
            r.build_seconds.into(),
            r.statements.into(),
            r.noisy.into(),
            r.target_p95_before.into(),
            r.target_p95_after.into(),
            r.worst_regression.into(),
            r.worst_fp.map(|f| f.0).into(),
            r.recall_at_k.into(),
            r.verdict.as_str().into(),
            r.reason.as_deref().map(|s| truncate(s, 128)).into(),
            to_json(&r.details)?.into(),
        ]);
        q.exec_drop(
            format!(
                "INSERT INTO {} (candidate_id, method, cluster_id, node, shard_id, started_at, \
                 finished_at, build_seconds, statements, noisy, target_p95_before, \
                 target_p95_after, worst_regression, worst_fp, recall_at_k, verdict, reason, \
                 details) VALUES (?, ?, ?, ?, ?, {AT}, {}, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                self.validations,
                opt_at(r.finished_at.is_some()),
            ),
            p,
        )
        .await
        .map_err(map_err)?;
        last_id(q).await
    }

    /// Finishes a running validation with its results. Returns whether a
    /// running row changed.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn finish_validation<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        r: &ValidationRow,
    ) -> Result<bool> {
        let at = r.finished_at.unwrap_or_else(SystemTime::now);
        q.exec_drop(
            format!(
                "UPDATE {} SET finished_at = {AT}, build_seconds = ?, statements = ?, noisy = ?, \
                 target_p95_before = ?, target_p95_after = ?, worst_regression = ?, \
                 worst_fp = ?, recall_at_k = ?, verdict = ?, reason = ?, details = ? \
                 WHERE validation_id = ? AND verdict = 'running'",
                self.validations
            ),
            vec![
                Value::from(micros(at)),
                r.build_seconds.into(),
                r.statements.into(),
                r.noisy.into(),
                r.target_p95_before.into(),
                r.target_p95_after.into(),
                r.worst_regression.into(),
                r.worst_fp.map(|f| f.0).into(),
                r.recall_at_k.into(),
                r.verdict.as_str().into(),
                r.reason.as_deref().map(|s| truncate(s, 128)).into(),
                to_json(&r.details)?.into(),
                r.validation_id.into(),
            ],
        )
        .await
        .map_err(map_err)?;
        Ok(row_count(q).await? == 1)
    }

    /// The validation `id`.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a malformed row.
    pub async fn validation<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        id: u64,
    ) -> Result<Option<ValidationRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {VALIDATION_COLS} FROM {} WHERE validation_id = ?",
                    self.validations
                ),
                (id,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().next().map(validation_row).transpose()
    }

    // ---- index_origins ----

    /// Records (or replaces) the origin of `r.index_name` on `r.object`.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn put_origin<Q: Queryable + Send>(&self, q: &mut Q, r: &OriginRow) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (object_id, index_name, origin, source, recorded_at) \
                 VALUES (?, ?, ?, ?, {AT}) ON DUPLICATE KEY UPDATE origin = VALUES(origin), \
                 source = VALUES(source), recorded_at = VALUES(recorded_at)",
                self.origins
            ),
            (
                r.object.0,
                &r.index_name,
                r.origin.as_str(),
                &r.source,
                micros(r.recorded_at),
            ),
        )
        .await
        .map_err(map_err)
    }

    /// Forgets the origin of a dropped index. Returns whether a row went.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn delete_origin<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        index_name: &str,
    ) -> Result<bool> {
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE object_id = ? AND index_name = ?",
                self.origins
            ),
            (object.0, index_name),
        )
        .await
        .map_err(map_err)?;
        Ok(row_count(q).await? == 1)
    }

    /// The recorded origins on `object`, by index name.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a malformed row.
    pub async fn origins<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
    ) -> Result<BTreeMap<String, Origin>> {
        let rows: Vec<(String, String)> = q
            .exec(
                format!(
                    "SELECT index_name, origin FROM {} WHERE object_id = ?",
                    self.origins
                ),
                (object.0,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter()
            .map(|(n, o)| Ok((n, parse_origin(&o)?)))
            .collect()
    }

    // ---- retention ----

    /// Deletes candidates and validations created before `before`
    /// (`workload.retention_days`), and dropped or rejected index rows
    /// whose state changed before `history_before` (a year). Returns the
    /// rows deleted.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn purge<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        before: SystemTime,
        history_before: SystemTime,
    ) -> Result<u64> {
        let mut n = 0;
        for (table, cond) in [
            (&self.candidates, format!("created_at < {AT}")),
            (&self.validations, format!("started_at < {AT}")),
        ] {
            q.exec_drop(
                format!("DELETE FROM {table} WHERE {cond}"),
                (micros(before),),
            )
            .await
            .map_err(map_err)?;
            n += row_count(q).await?;
        }
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE state IN ('dropped','rejected') \
                 AND state_changed_at < {AT}",
                self.indexes
            ),
            (micros(history_before),),
        )
        .await
        .map_err(map_err)?;
        Ok(n + row_count(q).await?)
    }
}

fn opt_at(set: bool) -> &'static str {
    if set { AT } else { "NULL" }
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn parse_origin(s: &str) -> Result<Origin> {
    Ok(match s {
        "system" => Origin::System,
        "auto" => Origin::Auto,
        "user" => Origin::User,
        "migrated" => Origin::Migrated,
        other => {
            return Err(SprigError::internal(format!(
                "autoindex catalog: bad origin {other:?}"
            )));
        }
    })
}

fn index_row(mut row: Row) -> Result<AutoIndexRow> {
    let r = &mut row;
    let prior: Option<String> = col(r, 7, "prior_state")?;
    let reason: Option<String> = col(r, 19, "drop_reason")?;
    Ok(AutoIndexRow {
        index_id: AutoIndexId(col(r, 0, "index_id")?),
        object: ObjectId(col(r, 1, "object_id")?),
        name: col(r, 2, "name")?,
        model: Model::parse(&col::<String>(r, 3, "model")?)?,
        kind: StoredKind::parse(&col::<String>(r, 4, "kind")?)?,
        definition: json_req(r, 5, "definition")?,
        state: IndexState::parse(&col::<String>(r, 6, "state")?)?,
        prior_state: prior.as_deref().map(IndexState::parse).transpose()?,
        candidate_id: col(r, 8, "candidate_id")?,
        validation_id: col(r, 9, "validation_id")?,
        decision_id: col(r, 10, "decision_id")?,
        est_benefit_ms_h: col(r, 11, "est_benefit_ms_h")?,
        est_write_ms_h: col(r, 12, "est_write_ms_h")?,
        est_bytes: col(r, 13, "est_bytes")?,
        actual_bytes: col(r, 14, "actual_bytes")?,
        shards: json_req(r, 15, "shards")?,
        baseline: json_req(r, 16, "baseline")?,
        observed: json_col(r, 17, "observed")?,
        last_used_at: opt_time_col(r, 18, "last_used_at")?,
        drop_reason: reason.as_deref().map(DropReason::parse).transpose()?,
        created_at: time_col(r, 20, "created_at")?,
        state_changed_at: time_col(r, 21, "state_changed_at")?,
    })
}

fn candidate_row(mut row: Row) -> Result<CandidateRow> {
    let r = &mut row;
    let fps: serde_json::Value = json_req(r, 7, "fingerprints")?;
    Ok(CandidateRow {
        candidate_id: col(r, 0, "candidate_id")?,
        run_id: col(r, 1, "run_id")?,
        object: ObjectId(col(r, 2, "object_id")?),
        def_hash: col(r, 3, "def_hash")?,
        kind: StoredKind::parse(&col::<String>(r, 4, "kind")?)?,
        definition: json_req(r, 5, "definition")?,
        covering: col(r, 6, "covering")?,
        fingerprints: parse_fingerprints(&fps)
            .ok_or_else(|| SprigError::internal("autoindex catalog: bad fingerprints"))?,
        status: CandidateStatus::parse(&col::<String>(r, 8, "status")?)?,
        reason: col(r, 9, "reason")?,
        benefit_ms_h: col(r, 10, "benefit_ms_h")?,
        write_ms_h: col(r, 11, "write_ms_h")?,
        est_bytes: col(r, 12, "est_bytes")?,
        build_seconds: col(r, 13, "build_seconds")?,
        evidence: json_req(r, 14, "evidence")?,
        created_at: time_col(r, 15, "created_at")?,
    })
}

fn validation_row(mut row: Row) -> Result<ValidationRow> {
    let r = &mut row;
    let worst: Option<u64> = col(r, 14, "worst_fp")?;
    Ok(ValidationRow {
        validation_id: col(r, 0, "validation_id")?,
        candidate_id: col(r, 1, "candidate_id")?,
        method: ValidationMethod::parse(&col::<String>(r, 2, "method")?)?,
        cluster_id: col(r, 3, "cluster_id")?,
        node: col(r, 4, "node")?,
        shard_id: col(r, 5, "shard_id")?,
        started_at: time_col(r, 6, "started_at")?,
        finished_at: opt_time_col(r, 7, "finished_at")?,
        build_seconds: col(r, 8, "build_seconds")?,
        statements: col(r, 9, "statements")?,
        noisy: col(r, 10, "noisy")?,
        target_p95_before: col(r, 11, "target_p95_before")?,
        target_p95_after: col(r, 12, "target_p95_after")?,
        worst_regression: col(r, 13, "worst_regression")?,
        worst_fp: worst.map(Fingerprint),
        recall_at_k: col(r, 15, "recall_at_k")?,
        verdict: Verdict::parse(&col::<String>(r, 16, "verdict")?)?,
        reason: col(r, 17, "reason")?,
        details: json_req(r, 18, "details")?,
    })
}

/// A decision of automatic indexing, as a row of chapter 11's
/// `decisions` table would hold it (`component = 'advisor'`).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexDecision {
    /// The tenant.
    pub tenant: TenantId,
    /// `index.create`, `index.reject`, `index.retire`, `index.drop`,
    /// `index.recommend`, `index.defer`.
    pub kind: &'static str,
    /// `index` (an automatic index name on an object) or `fingerprint`.
    pub subject_type: &'static str,
    /// `<object id>/<index name>` or the fingerprint's hex.
    pub subject_id: String,
    /// `applied` (mode `on`) or `recommended`.
    pub mode: &'static str,
    /// `proposed`, `scheduled`, `rejected`, `succeeded`, ...
    pub outcome: &'static str,
    /// `slow_p95`, `budget`, `below_min_net`, `no_gain`, ...
    pub reason_code: String,
    /// One line for people.
    pub summary: String,
    /// Inputs, scores, thresholds, the resolved settings used.
    pub evidence: serde_json::Value,
    /// Options considered.
    pub alternatives: Option<serde_json::Value>,
    /// When.
    pub at: SystemTime,
}

/// Where decisions go: chapter 11's `decisions` table once it exists.
#[async_trait::async_trait]
pub trait DecisionLog: Send + Sync {
    /// Records `d`; returns its `decision_id` when the log assigns one.
    ///
    /// # Errors
    /// The log's.
    async fn record(&self, d: &IndexDecision) -> Result<Option<u64>>;
}

/// Logs decisions as `decision` events until the decision log exists.
#[derive(Debug, Clone, Copy, Default)]
pub struct LogDecisions;

#[async_trait::async_trait]
impl DecisionLog for LogDecisions {
    async fn record(&self, d: &IndexDecision) -> Result<Option<u64>> {
        tracing::info!(
            event = "decision",
            component = "advisor",
            kind = d.kind,
            tenant = %d.tenant,
            subject_type = d.subject_type,
            subject = %d.subject_id,
            mode = d.mode,
            outcome = d.outcome,
            reason_code = %d.reason_code,
            evidence = %d.evidence,
            "{}",
            d.summary
        );
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
pub mod usage;
