//! The single-shard DDL write path and the repairs the reconciler makes
//! with it.
//!
//! ```text
//! CREATE TABLE billing.orders (...):
//!   1. commit without a version: objects row (creating, pending_job_id)
//!   2. rewritten DDL on the group's shard schemas
//!   3. SHOW CREATE TABLE, normalized to logical names
//!   4. change(Catalog): schema_versions row; state = active
//! ```
//!
//! [`DdlEngine::execute`] runs every statement in three phases:
//!
//! 1. **Prepare** (the version row locked, no version allocated): resolve
//!    names, check existence and `IF [NOT] EXISTS`, refuse to start while
//!    another DDL holds a target (waiting `catalog.ddl_wait_ms`, then
//!    39071), allocate ids, and mark the targets: a new database or object
//!    row in state `creating`, `pending_job_id` on existing objects, and an
//!    **intent row** `sprig.ddl.<job>` at tenant scope recording what the
//!    job does (its operation, the targets, the final names of a rename,
//!    the statement text). These commit without a version
//!    ([`Locked::commit_unversioned`](crate::version::Locked::commit_unversioned)),
//!    so caches don't reload for them.
//! 2. **Apply** the physical statements through a [`DdlExecutor`] on the
//!    tenant's cluster. A definite failure rolls phase 1 back (unversioned:
//!    the visible catalog never changed). A lost connection leaves the
//!    outcome unknown: the marks stay for the reconciler.
//! 3. **Finish**: read the result (`SHOW CREATE`, the schema's defaults)
//!    outside the lock, then one versioned change publishes it, clears the
//!    marks and deletes the intent row. A failed read is tried again a few
//!    times over a few seconds ([`DdlOpts::finish_read_attempts`]) before
//!    the job is left to the reconciler. The session reads its own DDL at
//!    the returned version.
//!
//! What readers see: only phase 3 is versioned. A full load during phase 1
//! or 2 sees `creating` rows (readers treat them as absent) and
//! `pending_job_id` on existing objects (readers ignore it). This path
//! never writes `altering` or `dropping`: an existing object stays `active`
//! until its versioned change, so no cache ever holds a transient state it
//! can't see leave (multi-shard jobs, where the visible state
//! really passes through them, will use them).
//!
//! For the gateway: build a [`DdlRequest`] from the statement (its kind,
//! names and `IF [NOT] EXISTS` flags as a [`DdlStatement`], the rewritten
//! text, the original text, the session's database and user, and the
//! tenant's `names.lower_case_table_names`), pass a [`DdlExecutor`] that
//! runs on the tenant's cluster, and answer the client with the
//! [`DdlOutcome`] (OK with affected rows and notes) or the error (ERR,
//! names already logical). Readers treat `creating` rows as absent; the
//! session's next statements read at `min_version = outcome.version`.
//!
//! Crash windows and their recovery ([`DdlEngine::resolve`], called by the
//! reconciler once an intent is older than `catalog.stuck_state_minutes`
//! and no DDL runs in its schema):
//!
//! | Crash | Left behind | Recovery |
//! |---|---|---|
//! | During phase 1 | Nothing (rolled back) | None needed |
//! | After phase 1, before the statement ran | Marks and intent; physical unchanged | Create, alter, rename: roll back. Drop: roll forward (run the drop) |
//! | After the statement ran, before phase 3 commits | Marks and intent; physical changed | Finish: publish what `SHOW CREATE` shows |
//! | After phase 3 commits | Nothing | None needed |
//!
//! Drops roll forward, as every removing step does: the client asked for the drop, and a half-dropped database is
//! worse than a dropped one.

pub mod backend;
pub mod exec;
pub mod mem;
mod multi;
mod owners;
mod repair;
pub mod shards;

#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod shard_live_tests;
#[cfg(test)]
mod shard_sim;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sprig_common::ids::{DbId, GroupId, JobId, ObjectId, ShardId, TenantId, UserId};
use sprig_common::names::{SchemaName, is_internal_object, is_reserved_database, quote};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::{Apply, HardBounds, Scopes, Setting};

pub use backend::{DdlBackend, DdlTx};
pub use exec::{
    DdlExecutor, Effect, MariaExecutor, PhysicalDdl, PhysicalObject, RoutedExecutor, SchemaInfo,
    ShardTarget, drop_sql, kind_keyword, outcome_unknown,
};
pub use multi::{ShardRef, ShardSet, jobs_on_shards};
pub use owners::{
    GATEWAY_PROCESS_PREFIX, OWNER_MIN_AGE, ProcessOwners, gateway_label, is_gateway_owner,
};
pub use repair::{Resolution, drop_schema, job_schemas};

use crate::store::{
    Counter, DatabaseRow, DbState, Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState,
    SchemaVersionRow, SettingRow, SettingScope,
};
use crate::version::{ChangeOpts, Locked, VersionKind, Versioner};

// ---- keys ----

/// `catalog.ddl_wait_ms`: how long a DDL waits for another DDL holding
/// one of its targets before failing with 39071.
pub static DDL_WAIT_MS: Setting<u64> = Setting {
    key: "catalog.ddl_wait_ms",
    default: 10_000,
    scopes: Scopes::TREE,
    hard: HardBounds::range(0, 3_600_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 8,
    doc: "Wait for a pending DDL job",
};
sprig_settings::register!(DDL_WAIT_MS);

/// `ddl.mover_wait_minutes`: how long a schema change on a table group's
/// shards waits for a split, merge or shard move holding one of them
/// before failing with 39470.
pub static MOVER_WAIT_MINUTES: Setting<u64> = Setting {
    key: "ddl.mover_wait_minutes",
    default: 120,
    scopes: Scopes(0b11_0100),
    hard: HardBounds::range(0, 1440),
    apply: Apply::Live,
    operator_only: true,
    owner: 21,
    doc: "How long DDL waits for mover jobs",
};
sprig_settings::register!(MOVER_WAIT_MINUTES);

/// `catalog.reconcile_interval_seconds`.
pub static RECONCILE_INTERVAL_SECONDS: Setting<u64> = Setting {
    key: "catalog.reconcile_interval_seconds",
    default: 300,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 86_400),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "Reconciler period per tenant",
};
sprig_settings::register!(RECONCILE_INTERVAL_SECONDS);

/// `catalog.stuck_state_minutes`.
pub static STUCK_STATE_MINUTES: Setting<u64> = Setting {
    key: "catalog.stuck_state_minutes",
    default: 15,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(1, 10_080),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "Age at which a transient state is stuck",
};
sprig_settings::register!(STUCK_STATE_MINUTES);

/// `catalog.orphan_retention_days`.
pub static ORPHAN_RETENTION_DAYS: Setting<u64> = Setting {
    key: "catalog.orphan_retention_days",
    default: 7,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 3650),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "Quarantine before dropping an empty orphan",
};
sprig_settings::register!(ORPHAN_RETENTION_DAYS);

/// `catalog.tombstone_retention_hours`.
pub static TOMBSTONE_RETENTION_HOURS: Setting<u64> = Setting {
    key: "catalog.tombstone_retention_hours",
    default: 24,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 87_600),
    apply: Apply::Live,
    operator_only: true,
    owner: 8,
    doc: "Keep dropped rows this long",
};
sprig_settings::register!(TOMBSTONE_RETENTION_HOURS);

/// `names.lower_case_table_names`: 1 compares database and
/// relation names in lower case. Fixed at tenant creation.
pub static LOWER_CASE_TABLE_NAMES: Setting<u32> = Setting {
    key: "names.lower_case_table_names",
    default: 0,
    scopes: Scopes(0b10_0100),
    hard: HardBounds::range(0, 1),
    apply: Apply::NewSessions,
    operator_only: true,
    owner: 7,
    doc: "1 lowercases database and table names in the rewrite",
};
sprig_settings::register!(LOWER_CASE_TABLE_NAMES);

// ---- names ----

/// The lookup form of a database name.
#[must_use]
pub fn database_key(name: &str, lower_case_table_names: bool) -> String {
    if lower_case_table_names {
        name.to_lowercase()
    } else {
        name.to_owned()
    }
}

/// The lookup form of an object name: relations follow
/// `lower_case_table_names`; routines, events and search indexes compare in
/// lower case; triggers are case-sensitive.
#[must_use]
pub fn object_key(kind: ObjectKind, name: &str, lower_case_table_names: bool) -> String {
    match kind.namespace() {
        Namespace::Relation | Namespace::Type | Namespace::Graph => {
            database_key(name, lower_case_table_names)
        }
        Namespace::Trigger => name.to_owned(),
        _ => name.to_lowercase(),
    }
}

/// The distributed-transaction decision table, in every schema a distributed
/// transaction's decision can be anchored in (each shard schema, the
/// database's own schema).
pub const DTX_TABLE: &str = "__sprig_dtx";

/// `CREATE TABLE IF NOT EXISTS` of [`DTX_TABLE`] in `schema`
/// (`sprig_txn::decision::create_table_sql` is this).
#[must_use]
pub fn dtx_table_sql(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS `{}`.`{DTX_TABLE}` (\
         dtid BINARY(16) NOT NULL, \
         outcome ENUM('commit','abort','pending') NOT NULL, \
         decider BINARY(16) NULL, \
         coordinator VARCHAR(64) NOT NULL, \
         participants VARBINARY(4096) NULL, \
         created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
         PRIMARY KEY (dtid), KEY (created_at)) ENGINE=InnoDB",
        schema.replace('`', "``")
    )
}

/// The statement creating [`DTX_TABLE`] in `schema`, run with the schema
/// itself (`CREATE DATABASE`, its repair).
#[must_use]
pub fn dtx_table_ddl(schema: SchemaName) -> PhysicalDdl {
    PhysicalDdl {
        schema,
        sql: dtx_table_sql(&schema.to_string()),
        effect: Effect::Create {
            kind: ObjectKind::Table,
            name: DTX_TABLE.to_owned(),
        },
    }
}

/// The `databases.options` key listing a database's shard ids in its
/// default group.
pub const SHARDS_OPTION: &str = "shards";

/// The physical schemas of a database: one per shard of its default group
/// (`t<tenant>_d<db>_g<group>_s<shard>`), from `options.shards`. Empty for
/// a database created before the DDL path recorded placement.
#[must_use]
pub fn shard_schemas(tenant: TenantId, db: &DatabaseRow) -> Vec<SchemaName> {
    db.options
        .get(SHARDS_OPTION)
        .and_then(serde_json::Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(serde_json::Value::as_u64)
                .filter_map(|s| u32::try_from(s).ok())
                .map(|s| SchemaName::shard(tenant, db.db_id, db.default_group_id, ShardId(s)))
                .collect()
        })
        .unwrap_or_default()
}

/// The template schema of a database: the first shard of its default
/// group, the schema the statement rewrite names. A statement that changes
/// several shards is written for it and rewritten per shard.
fn single_schema(tenant: TenantId, db: &DatabaseRow) -> Result<SchemaName> {
    shard_schemas(tenant, db).first().copied().ok_or_else(|| {
        SprigError::internal(format!("database {} has no physical placement", db.name))
    })
}

/// Replaces backquoted physical schema names of `tenant` in `text` with
/// the logical names `name_of` gives (others are left alone).
#[must_use]
pub fn logical_text(
    text: &str,
    tenant: TenantId,
    name_of: &dyn Fn(DbId) -> Option<String>,
) -> String {
    let prefix = format!("t{tenant}_");
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find('`') {
        out.push_str(&rest[..i]);
        let after = &rest[i + 1..];
        let Some(j) = after.find('`') else {
            out.push_str(&rest[i..]);
            return out;
        };
        let ident = &after[..j];
        let mapped = ident
            .starts_with(&prefix)
            .then(|| ident.parse::<SchemaName>().ok())
            .flatten()
            .filter(|s| s.tenant() == tenant)
            .and_then(|s| s.db())
            .and_then(name_of);
        if let Some(name) = mapped {
            out.push_str(&quote(&name));
        } else {
            out.push('`');
            out.push_str(ident);
            out.push('`');
        }
        rest = &after[j + 1..];
    }
    out.push_str(rest);
    // Bare physical names (in messages) too.
    if out.contains(&prefix) {
        let mut mapped = String::with_capacity(out.len());
        let mut s = out.as_str();
        while let Some(i) = s.find(&prefix) {
            let boundary = i == 0 || !s.as_bytes()[i - 1].is_ascii_alphanumeric();
            mapped.push_str(&s[..i]);
            let tail = &s[i..];
            let end = tail
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(tail.len());
            let word = &tail[..end];
            let name = boundary
                .then(|| word.parse::<SchemaName>().ok())
                .flatten()
                .and_then(|n| n.db())
                .and_then(name_of);
            match name {
                Some(n) => mapped.push_str(&n),
                None => mapped.push_str(word),
            }
            s = &tail[end..];
        }
        mapped.push_str(s);
        return mapped;
    }
    out
}

// ---- requests ----

/// A name as the statement wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectName {
    /// The logical database, if qualified; else the session's current one.
    pub db: Option<String>,
    /// The object's name.
    pub name: String,
}

impl ObjectName {
    /// An unqualified name.
    #[must_use]
    pub fn bare(name: &str) -> Self {
        Self {
            db: None,
            name: name.to_owned(),
        }
    }

    /// A qualified name.
    #[must_use]
    pub fn qualified(db: &str, name: &str) -> Self {
        Self {
            db: Some(db.to_owned()),
            name: name.to_owned(),
        }
    }
}

/// What a DDL statement does, as the gateway parsed it. Index statements
/// (`CREATE INDEX`, `DROP INDEX`) are [`DdlStatement::AlterObject`] of
/// their table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlStatement {
    /// `CREATE DATABASE`.
    CreateDatabase {
        /// The logical name.
        name: String,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// `CHARACTER SET`.
        charset: Option<String>,
        /// `COLLATE`.
        collation: Option<String>,
    },
    /// `ALTER DATABASE` (not `SPRIG SET`, which is a setting write).
    AlterDatabase {
        /// The logical name.
        name: String,
        /// `CHARACTER SET`.
        charset: Option<String>,
        /// `COLLATE`.
        collation: Option<String>,
    },
    /// `DROP DATABASE`.
    DropDatabase {
        /// The logical name.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `CREATE TABLE`, `VIEW`, `SEQUENCE`, `PROCEDURE`, `FUNCTION`,
    /// `TRIGGER` or `EVENT`.
    CreateObject {
        /// The kind.
        kind: ObjectKind,
        /// The name.
        name: ObjectName,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// `OR REPLACE`.
        or_replace: bool,
        /// A trigger's table.
        parent: Option<ObjectName>,
    },
    /// `ALTER TABLE`, `ALTER VIEW`, `ALTER EVENT`, ..., `CREATE INDEX`,
    /// `DROP INDEX`.
    AlterObject {
        /// The kind.
        kind: ObjectKind,
        /// The name.
        name: ObjectName,
        /// `ALTER TABLE IF EXISTS` (MariaDB): a missing object is a note,
        /// not an error.
        if_exists: bool,
        /// The statement also renames the object (`ALTER TABLE t ADD c
        /// INT, RENAME TO u`, `ALTER EVENT e RENAME TO f`): its name
        /// afterwards, possibly in another database.
        rename_to: Option<ObjectName>,
    },
    /// `DROP TABLE a, b`, `DROP VIEW`, `DROP PROCEDURE`, ...
    DropObjects {
        /// The kind.
        kind: ObjectKind,
        /// The names.
        names: Vec<ObjectName>,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `RENAME TABLE a TO b, c TO d` (also `ALTER TABLE a RENAME TO b`).
    RenameTables {
        /// The moves, in order.
        pairs: Vec<(ObjectName, ObjectName)>,
        /// `RENAME TABLE IF EXISTS` (MariaDB): a move whose source doesn't
        /// exist is skipped with a note.
        if_exists: bool,
    },
    /// `TRUNCATE TABLE`.
    Truncate {
        /// The table.
        name: ObjectName,
    },
}

/// One DDL statement to run for a tenant.
#[derive(Debug, Clone)]
pub struct DdlRequest<'a> {
    /// The tenant.
    pub tenant: TenantId,
    /// What the statement does.
    pub statement: DdlStatement,
    /// The rewritten text for MariaDB (physical schema names). Unused for
    /// database statements, whose SQL the engine writes.
    pub sql: &'a str,
    /// The statement as accepted, with logical names
    /// (`schema_versions.ddl`).
    pub logical_sql: &'a str,
    /// The session's current logical database.
    pub current_db: Option<&'a str>,
    /// The user running it.
    pub user: Option<UserId>,
    /// The tenant's `names.lower_case_table_names`.
    pub lower_case_table_names: bool,
}

/// A note for the client (level `Note`), as MariaDB raises for
/// `IF [NOT] EXISTS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlNote {
    /// The MySQL error number.
    pub code: u16,
    /// The message, with logical names.
    pub message: String,
}

/// What the client gets back on success: an OK packet.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DdlOutcome {
    /// The catalog version the change committed at; `None` when nothing
    /// in the catalog changed. The session reads its own DDL with
    /// `min_version` at least this.
    pub version: Option<u64>,
    /// The OK packet's affected rows (1 for `CREATE DATABASE`, the number
    /// of tables for `DROP DATABASE`, else 0).
    pub affected_rows: u64,
    /// Notes.
    pub notes: Vec<DdlNote>,
    /// The job, when one ran.
    pub job: Option<JobId>,
    /// The committed catalog version the statement read, when it changed
    /// nothing (an `IF [NOT] EXISTS` that found its answer). The session
    /// reads at least this version next, as it does `version`, so a
    /// statement that found another session's new database or table sees
    /// it in its next statement too.
    pub seen: Option<u64>,
}

// ---- intents ----

/// The prefix of intent rows: `sprig.ddl.<job>` at tenant scope.
pub const INTENT_PREFIX: &str = "sprig.ddl.";

/// The intent row's key of a job.
#[must_use]
pub fn intent_key(job: JobId) -> String {
    format!("{INTENT_PREFIX}{job}")
}

/// One move of a rename: where the object ends up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Move {
    /// The object.
    pub object: ObjectId,
    /// Its database afterwards.
    pub db: DbId,
    /// Its name afterwards.
    pub name: String,
    /// Its name key afterwards.
    pub name_key: String,
}

/// What a job does to the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum IntentOp {
    /// A new database (row in `creating`).
    CreateDatabase {
        /// The database.
        db: DbId,
    },
    /// Dropping a database and everything in it.
    DropDatabase {
        /// The database.
        db: DbId,
    },
    /// A new object (row in `creating`), or `OR REPLACE` of an existing
    /// one (`replace`).
    Create {
        /// The object.
        object: ObjectId,
        /// It existed: the statement replaces it.
        replace: bool,
    },
    /// A definition change.
    Alter {
        /// The object.
        object: ObjectId,
    },
    /// Drops.
    Drop {
        /// The objects.
        objects: Vec<ObjectId>,
    },
    /// Renames.
    Rename {
        /// The moves.
        moves: Vec<Move>,
    },
}

/// The intent row of a job: what the reconciler needs to finish or roll
/// back a DDL whose process died.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DdlIntent {
    /// The job.
    pub job: JobId,
    /// When phase 1 committed, milliseconds since the epoch.
    pub started_ms: u64,
    /// The statement as accepted (logical names).
    pub ddl: String,
    /// The user who ran it.
    #[serde(default)]
    pub user: Option<UserId>,
    /// The shards a multi-shard job changes; `None` for a single-schema
    /// job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shards: Option<ShardSet>,
    /// The engine running the job ([`DdlEngine::owner_id`]): another DDL
    /// that finds the job abandoned by it resolves it at once instead of
    /// waiting for the reconciler. `None` in rows written
    /// before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// What it does.
    #[serde(flatten)]
    pub op: IntentOp,
}

impl DdlIntent {
    /// Parses an intent row.
    ///
    /// # Errors
    /// `Internal` for a row that isn't an intent.
    pub fn from_row(row: &SettingRow) -> Result<Self> {
        let v = row
            .value
            .clone()
            .ok_or_else(|| SprigError::internal(format!("{} has no value", row.key)))?;
        serde_json::from_value(v)
            .map_err(|e| SprigError::internal(format!("{}: bad intent: {e}", row.key)))
    }

    /// Every intent among `rows` (other rows and bad ones skipped).
    #[must_use]
    pub fn all(rows: &[SettingRow]) -> Vec<Self> {
        rows.iter()
            .filter(|r| r.scope == SettingScope::Tenant && r.key.starts_with(INTENT_PREFIX))
            .filter_map(|r| match Self::from_row(r) {
                Ok(i) => Some(i),
                Err(e) => {
                    tracing::warn!(key = %r.key, error = %e, "ignoring a bad DDL intent row");
                    None
                }
            })
            .collect()
    }

    /// When phase 1 committed.
    #[must_use]
    pub fn started(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.started_ms)
    }

    /// The databases the job holds as a whole.
    #[must_use]
    pub fn database(&self) -> Option<DbId> {
        match self.op {
            IntentOp::CreateDatabase { db } | IntentOp::DropDatabase { db } => Some(db),
            _ => None,
        }
    }

    /// The objects the job marked.
    #[must_use]
    pub fn objects(&self) -> Vec<ObjectId> {
        match &self.op {
            IntentOp::Create { object, .. } | IntentOp::Alter { object } => vec![*object],
            IntentOp::Drop { objects } => objects.clone(),
            IntentOp::Rename { moves } => moves.iter().map(|m| m.object).collect(),
            IntentOp::CreateDatabase { .. } | IntentOp::DropDatabase { .. } => Vec::new(),
        }
    }

    pub(crate) fn row(&self, version: u64, at: SystemTime) -> Result<SettingRow> {
        Ok(SettingRow {
            scope: SettingScope::Tenant,
            key: intent_key(self.job),
            value: Some(
                serde_json::to_value(self)
                    .map_err(|e| SprigError::internal(format!("encoding an intent: {e}")))?,
            ),
            bounds: None,
            set_by: "system".to_owned(),
            set_at: at,
            version,
        })
    }
}

// ---- the engine ----

/// A point in [`DdlEngine::execute`] where a test can inject a crash: an
/// `Err` abandons the statement there, as a dying process would.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DdlStep {
    /// Phase 1's rows are written; about to commit them.
    Planned,
    /// Phase 1 committed; the physical statement hasn't run.
    Prepared,
    /// The physical statement ran; phase 3 hasn't started.
    Applied,
    /// A multi-shard job is about to run its statement on shard `i` (in
    /// the job's order).
    Shard(usize),
    /// A multi-shard job is about to undo shard `i`.
    Undo(usize),
}

/// A test hook called at every [`DdlStep`].
pub type DdlFailpoint = Arc<dyn Fn(TenantId, DdlStep) -> Result<()> + Send + Sync>;

/// Timing of the DDL path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DdlOpts {
    /// `catalog.ddl_wait_ms`: the wait for a target another DDL holds.
    pub ddl_wait: Duration,
    /// How often a waiting DDL looks again.
    pub poll: Duration,
    /// How many times phase 3 reads the result (`SHOW CREATE`, the
    /// schema's defaults) before leaving the job to the reconciler.
    pub finish_read_attempts: u32,
    /// The wait before the second read; it doubles for each later one.
    pub finish_read_backoff: Duration,
}

impl Default for DdlOpts {
    fn default() -> Self {
        Self {
            // `catalog.ddl_wait_ms`'s default: the key is registered ahead
            // of a per-tenant read (marked unbuilt).
            ddl_wait: Duration::from_millis(10_000),
            poll: Duration::from_millis(50),
            // Four reads over 3.5 s (waits of 0.5, 1 and 2 s): a dropped
            // connection or a writer switch heals in that time, and the job
            // isn't held for `catalog.stuck_state_minutes`.
            finish_read_attempts: 4,
            finish_read_backoff: Duration::from_millis(500),
        }
    }
}

/// A physical target whose result phase 3 reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FactTarget {
    pub object: ObjectId,
    pub schema: SchemaName,
    pub kind: ObjectKind,
    pub name: String,
}

/// What phase 3 publishes, read from the cluster outside the lock.
#[derive(Debug, Clone, Default)]
pub(crate) struct Facts {
    pub schema: Option<SchemaInfo>,
    pub create_sql: BTreeMap<ObjectId, Option<String>>,
}

/// A prepared job.
struct Plan {
    fanout: Option<multi::Fanout>,
    intent: DdlIntent,
    physical: Vec<PhysicalDdl>,
    targets: Vec<FactTarget>,
    schema: Option<SchemaName>,
    names: Vec<(DbId, String)>,
    affected: u64,
    notes: Vec<DdlNote>,
}

enum Planned {
    /// A target is held by another job.
    Busy(String),
    /// Nothing to do (`IF [NOT] EXISTS`).
    Done(DdlOutcome),
    /// `TRUNCATE`: run it, no catalog change.
    Run(PhysicalDdl, Vec<(DbId, String)>, Option<multi::Fanout>),
    /// `ALTER DATABASE`: run it, then one versioned change.
    AlterDb(DatabaseRow, Vec<PhysicalDdl>),
    /// A job: marks written in the transaction.
    Go(Box<Plan>),
}

/// Runs DDL for tenants through their catalogs (see the module docs).
pub struct DdlEngine<B: DdlBackend>
where
    B::Tx: DdlTx,
{
    versioner: Versioner<B>,
    clock: Arc<dyn Clock>,
    opts: DdlOpts,
    failpoint: Option<DdlFailpoint>,
    checks: Vec<Arc<dyn PrepareCheck>>,
    owner: Arc<Owner>,
}

// ---- abandoned jobs ----

/// How often a DDL waiting for a held target looks for abandoned jobs.
const ABANDONED_SWEEP_EVERY: Duration = Duration::from_secs(1);

/// This process's token: a random id fixed for the process's life, so a
/// restarted process can be told from the one that started a job.
#[must_use]
pub fn process_token() -> &'static str {
    static TOKEN: std::sync::LazyLock<String> =
        std::sync::LazyLock::new(|| uuid::Uuid::now_v7().simple().to_string());
    &TOKEN
}

/// Splits an owner id (`<label>@<process token>.<engine>`) into its label
/// and process token.
#[must_use]
pub fn owner_parts(owner: &str) -> Option<(&str, &str)> {
    let (label, rest) = owner.rsplit_once('@')?;
    let (process, _) = rest.split_once('.')?;
    Some((label, process))
}

/// Tells whether another engine's jobs are abandoned: its process ended.
/// Asked by a DDL that waits for a target such a job holds.
#[async_trait::async_trait]
pub trait OwnerCheck: Send + Sync {
    /// Whether the engine `owner` (an intent's [`DdlIntent::owner`]) is
    /// certainly gone, for a job started `age` ago. `false` when in doubt:
    /// the reconciler resolves the job after `catalog.stuck_state_minutes`.
    async fn gone(&self, owner: &str, age: Duration) -> bool;
}

/// Where an engine's janitor reaches a tenant's cluster to resolve a job
/// the engine abandoned ([`DdlEngine::with_janitor`]).
#[async_trait::async_trait]
pub trait JobExecutors: Send + Sync {
    /// An executor on `tenant`'s cluster.
    ///
    /// # Errors
    /// The cluster is unknown or unreachable.
    async fn executor(&self, tenant: TenantId) -> Result<Arc<dyn DdlExecutor>>;
}

/// When the janitor looks at an abandoned job, after it was abandoned:
/// the first look leaves the session's `KILL QUERY` time to land; the
/// later ones wait for a statement MariaDB still runs (or rolls back).
/// After the last, the reconciler resolves the job.
const JANITOR_LOOKS: [Duration; 7] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(15),
    Duration::from_secs(30),
    Duration::from_secs(60),
];

/// Who runs an engine's jobs: the id written into its intent rows and the
/// jobs running now. A job of this engine that isn't running is abandoned:
/// the statement's future was dropped (the client hung up, `KILL QUERY`)
/// or it failed with its outcome unknown.
struct Owner {
    id: String,
    running: std::sync::Mutex<std::collections::HashSet<JobId>>,
    check: Option<Arc<dyn OwnerCheck>>,
    janitor: std::sync::OnceLock<tokio::sync::mpsc::UnboundedSender<(TenantId, JobId)>>,
}

impl Owner {
    fn new(label: &str, check: Option<Arc<dyn OwnerCheck>>) -> Self {
        static ENGINES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let n = ENGINES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            id: format!("{label}@{}.{n}", process_token()),
            running: std::sync::Mutex::default(),
            check,
            janitor: std::sync::OnceLock::new(),
        }
    }

    fn jobs(&self) -> std::sync::MutexGuard<'_, std::collections::HashSet<JobId>> {
        self.running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn start(self: &Arc<Self>, tenant: TenantId, job: JobId) -> Running {
        self.jobs().insert(job);
        Running {
            owner: Arc::clone(self),
            tenant,
            job,
            committed: false,
            finished: false,
        }
    }
}

/// A job running in its engine, until dropped (also when the statement's
/// future is). A job whose phase 1 committed and that didn't finish goes
/// to the engine's janitor, if it has one.
struct Running {
    owner: Arc<Owner>,
    tenant: TenantId,
    job: JobId,
    committed: bool,
    finished: bool,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.owner.jobs().remove(&self.job);
        if self.committed
            && !self.finished
            && let Some(tx) = self.owner.janitor.get()
        {
            let _ = tx.send((self.tenant, self.job));
        }
    }
}

/// A check another module runs in the prepare phase, under the catalog
/// lock, before anything reaches MariaDB (the check on schema changes to
/// an object shared with descendants is one). An error refuses
/// the statement.
#[async_trait::async_trait]
pub trait PrepareCheck: Send + Sync {
    /// Checks `req` against the catalog as `tx` reads it.
    ///
    /// # Errors
    /// The refusal; the catalog's errors.
    async fn check(&self, req: &DdlRequest<'_>, tx: &mut dyn DdlTx) -> Result<()>;
}

impl<B: DdlBackend> Clone for DdlEngine<B>
where
    B::Tx: DdlTx,
{
    fn clone(&self) -> Self {
        Self {
            versioner: self.versioner.clone(),
            clock: Arc::clone(&self.clock),
            opts: self.opts,
            failpoint: self.failpoint.clone(),
            checks: self.checks.clone(),
            owner: Arc::clone(&self.owner),
        }
    }
}

impl<B: DdlBackend> std::fmt::Debug for DdlEngine<B>
where
    B::Tx: DdlTx,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DdlEngine")
            .field("opts", &self.opts)
            .finish_non_exhaustive()
    }
}

fn std_err(code: u16, state: [u8; 5], msg: impl Into<String>) -> SprigError {
    SprigError::standard(code, state, msg)
}

fn no_database() -> SprigError {
    std_err(1046, *b"3D000", "No database selected")
}

fn unknown_database(name: &str) -> SprigError {
    std_err(1049, *b"42000", format!("Unknown database '{name}'"))
}

/// The error (and note) for an object that doesn't exist.
fn missing(kind: ObjectKind, db: &str, name: &str, dropping: bool) -> (u16, [u8; 5], String) {
    match kind {
        ObjectKind::Procedure | ObjectKind::Function => (
            1305,
            *b"42000",
            format!("{} {db}.{name} does not exist", kind_keyword(kind)),
        ),
        ObjectKind::Trigger => (1360, *b"HY000", "Trigger does not exist".to_owned()),
        ObjectKind::Event if dropping => (1305, *b"42000", format!("Event {name} does not exist")),
        ObjectKind::Event => (1539, *b"HY000", format!("Unknown event '{name}'")),
        ObjectKind::View if dropping => (4092, *b"42S02", format!("Unknown VIEW: '{db}.{name}'")),
        ObjectKind::Sequence if dropping => {
            (4091, *b"42S02", format!("Unknown SEQUENCE: '{db}.{name}'"))
        }
        _ if dropping => (1051, *b"42S02", format!("Unknown table '{db}.{name}'")),
        _ => (
            1146,
            *b"42S02",
            format!("Table '{db}.{name}' doesn't exist"),
        ),
    }
}

/// A missing object's note as the error a statement without `IF EXISTS`
/// gets: `DROP EVENT` notes 1305 (`Event e does not exist`) but fails with
/// 1539 (`Unknown event 'e'`), in MySQL 8.4 and MariaDB 11.8 alike
/// (measured).
fn missing_as_error(e: SprigError) -> SprigError {
    if errno(&e) == Some(1305)
        && let Some(name) = e
            .message
            .strip_prefix("Event ")
            .and_then(|r| r.strip_suffix(" does not exist"))
    {
        return std_err(1539, *b"HY000", format!("Unknown event '{name}'"));
    }
    e
}

fn missing_err(kind: ObjectKind, db: &str, name: &str, dropping: bool) -> SprigError {
    let (code, state, msg) = missing(kind, db, name, dropping);
    std_err(code, state, msg)
}

/// The error (and note) for an object that already exists.
fn exists(kind: ObjectKind, name: &str) -> (u16, [u8; 5], String) {
    match kind {
        ObjectKind::Procedure | ObjectKind::Function => (
            1304,
            *b"42000",
            format!("{} {name} already exists", kind_keyword(kind)),
        ),
        ObjectKind::Trigger => (1359, *b"HY000", "Trigger already exists".to_owned()),
        ObjectKind::Event => (1537, *b"HY000", format!("Event '{name}' already exists")),
        _ => (1050, *b"42S01", format!("Table '{name}' already exists")),
    }
}

fn sql_kind(kind: ObjectKind) -> Result<()> {
    match kind {
        ObjectKind::Table
        | ObjectKind::View
        | ObjectKind::Sequence
        | ObjectKind::Procedure
        | ObjectKind::Function
        | ObjectKind::Trigger
        | ObjectKind::Event => Ok(()),
        other => Err(SprigError::internal(format!(
            "{other} objects are created by their owners, not by SQL DDL"
        ))),
    }
}

/// `CHARACTER SET x COLLATE y`, with the names checked (they become SQL).
fn charset_clause(charset: Option<&str>, collation: Option<&str>) -> Result<String> {
    let ok = |s: &str| {
        !s.is_empty() && s.len() <= 64 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    };
    let mut out = String::new();
    if let Some(c) = charset {
        if !ok(c) {
            return Err(std_err(
                1115,
                *b"42000",
                format!("Unknown character set: '{c}'"),
            ));
        }
        let _ = write!(out, " CHARACTER SET {c}");
    }
    if let Some(c) = collation {
        if !ok(c) {
            return Err(std_err(
                1273,
                *b"HY000",
                format!("Unknown collation: '{c}'"),
            ));
        }
        let _ = write!(out, " COLLATE {c}");
    }
    Ok(out)
}

fn millis(t: SystemTime) -> u64 {
    u64::try_from(t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()).unwrap_or(u64::MAX)
}

fn sha256(text: &str) -> [u8; 32] {
    Sha256::digest(text.as_bytes()).into()
}

impl<B: DdlBackend> DdlEngine<B>
where
    B::Tx: DdlTx,
{
    /// DDL through `versioner`'s catalogs, timestamps from `clock`.
    pub fn new(versioner: Versioner<B>, clock: Arc<dyn Clock>) -> Self {
        Self {
            versioner,
            clock,
            opts: DdlOpts::default(),
            failpoint: None,
            checks: Vec::new(),
            owner: Arc::new(Owner::new("engine", None)),
        }
    }

    /// The same engine under a new owner id `<label>@<process>.<n>`, with
    /// `check` telling whether other owners' processes are gone. A gateway passes `gateway:<dc>:<gateway id>`.
    #[must_use]
    pub fn with_owner(mut self, label: &str, check: Option<Arc<dyn OwnerCheck>>) -> Self {
        self.owner = Arc::new(Owner::new(label, check));
        self
    }

    /// The same engine with a janitor: a job this engine
    /// abandons (the statement's future dropped, or failed with its outcome
    /// unknown) is resolved within seconds through `executors`, as the
    /// reconciler would, once no DDL runs in its schemas; not only when
    /// another DDL waits for it. Needs a Tokio runtime; the janitor lives
    /// as long as the process.
    #[must_use]
    pub fn with_janitor(self, executors: Arc<dyn JobExecutors>) -> Self
    where
        B: 'static,
    {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(TenantId, JobId)>();
        if self.owner.janitor.set(tx).is_err() {
            return self;
        }
        // The task holds a clone, so it lives as long as the process (a
        // gateway's engine does anyway).
        let engine = self.clone();
        tokio::spawn(async move {
            while let Some((tenant, job)) = rx.recv().await {
                let engine = engine.clone();
                let executors = Arc::clone(&executors);
                tokio::spawn(async move {
                    engine.tidy(tenant, job, &*executors).await;
                });
            }
        });
        self
    }

    /// The id this engine writes into its jobs' intent rows.
    #[must_use]
    pub fn owner_id(&self) -> &str {
        &self.owner.id
    }

    /// The same engine with another module's prepare-phase check.
    #[must_use]
    pub fn with_check(mut self, check: Arc<dyn PrepareCheck>) -> Self {
        self.checks.push(check);
        self
    }

    /// The same engine with other timing.
    #[must_use]
    pub fn with_opts(mut self, opts: DdlOpts) -> Self {
        self.opts = opts;
        self
    }

    /// The same engine with a failpoint, for crash tests.
    #[must_use]
    #[doc(hidden)]
    pub fn with_failpoint(mut self, f: DdlFailpoint) -> Self {
        self.failpoint = Some(f);
        self
    }

    /// The versioner.
    #[must_use]
    pub fn versioner(&self) -> &Versioner<B> {
        &self.versioner
    }

    fn fail(&self, tenant: TenantId, step: DdlStep) -> Result<()> {
        self.failpoint.as_ref().map_or(Ok(()), |f| f(tenant, step))
    }

    fn new_job(&self) -> JobId {
        let d = self
            .clock
            .wall()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        JobId(uuid::Uuid::new_v7(uuid::Timestamp::from_unix(
            uuid::NoContext,
            d.as_secs(),
            d.subsec_nanos(),
        )))
    }

    /// Runs one DDL statement: prepare, apply, finish (module docs).
    ///
    /// # Errors
    /// - standard errors in MariaDB's words: 1046 no database selected,
    ///   1049 unknown database, 1007 and 1008 for databases, 1050, 1051,
    ///   1146, 1304, 1305, 1359, 1360, 1537, 1539 for objects;
    /// - 39050 for a reserved name;
    /// - 39071 when another DDL holds a target past `catalog.ddl_wait_ms`
    ///   (and for catalog lock timeouts), 39070 when the catalog or global
    ///   tier is unreachable, 39504 when leadership is lost;
    /// - the backend's error for a refused statement (names mapped to
    ///   logical ones); its error when the outcome is unknown, with the
    ///   job left for the reconciler.
    pub async fn execute(
        &self,
        req: &DdlRequest<'_>,
        exec: &dyn DdlExecutor,
    ) -> Result<DdlOutcome> {
        let tenant = req.tenant;
        let job = self.new_job();
        // Until this returns or its future is dropped.
        let mut running = self.owner.start(tenant, job);
        let plan = match self.prepare(req, job, exec).await? {
            Planned::Busy(what) => return Err(conflict(&what)),
            Planned::Done(o) => return Ok(o),
            Planned::Run(stmt, names, fan) => {
                for s in multi::fan_out(tenant, &stmt, fan.as_ref(), exec).await? {
                    exec.execute(&s)
                        .await
                        .map_err(|e| map_names(e, tenant, &names))?;
                }
                return Ok(DdlOutcome::default());
            }
            Planned::AlterDb(db, stmts) => {
                return self.alter_database(tenant, db, &stmts, exec).await;
            }
            Planned::Go(p) => *p,
        };
        running.committed = true;
        self.fail(tenant, DdlStep::Prepared)?;
        if let Some(set) = plan.intent.shards.clone() {
            self.apply_shards(tenant, &plan, &set, exec).await?;
        }
        let mut created: Vec<SchemaName> = Vec::new();
        for stmt in plan
            .physical
            .iter()
            .filter(|_| plan.intent.shards.is_none())
        {
            if let Err(e) = exec.execute(stmt).await {
                if outcome_unknown(&e) {
                    tracing::warn!(%tenant, job = %job, error = %e,
                        "DDL outcome unknown; the reconciler will finish or roll it back");
                    return Err(map_names(e, tenant, &plan.names));
                }
                // A schema this statement's plan created a moment ago goes
                // too (otherwise a `CREATE DATABASE` refused at its second
                // statement leaves its backend schema behind with no catalog
                // entry).
                for schema in &created {
                    drop_if_ours(exec, tenant, job, schema).await;
                }
                match self.rollback(tenant, &plan.intent).await {
                    Ok(_) => running.finished = true,
                    Err(r) => tracing::warn!(%tenant, job = %job, error = %r,
                        "rolling back a refused DDL failed; the reconciler will"),
                }
                return Err(map_names(e, tenant, &plan.names));
            }
            if matches!(stmt.effect, Effect::CreateSchema) {
                created.push(stmt.schema);
            }
        }
        self.fail(tenant, DdlStep::Applied)?;
        let facts = self.gather_retrying(tenant, job, exec, &plan).await?;
        if let Some(set) = &plan.intent.shards {
            multi::check_skew(tenant, job, set, &plan.targets, exec).await;
        }
        let version = self.finish(tenant, &plan.intent, &facts).await?;
        running.finished = true;
        if version.is_none() {
            tracing::warn!(%tenant, job = %job, "the reconciler resolved this DDL before it finished");
        }
        Ok(DdlOutcome {
            version,
            affected_rows: plan.affected,
            notes: plan.notes,
            job: Some(job),
            seen: None,
        })
    }

    /// Phase 3's read, tried `finish_read_attempts` times with a doubling
    /// wait: the statement already ran, so a failed read would otherwise
    /// hold the job until the reconciler finds it, after
    /// `catalog.stuck_state_minutes`. The last error is returned with the
    /// job left for the reconciler.
    async fn gather_retrying(
        &self,
        tenant: TenantId,
        job: JobId,
        exec: &dyn DdlExecutor,
        plan: &Plan,
    ) -> Result<Facts> {
        let attempts = self.opts.finish_read_attempts.max(1);
        let mut wait = self.opts.finish_read_backoff;
        let mut attempt = 1;
        loop {
            match gather(exec, plan.schema.as_ref(), &plan.targets).await {
                Ok(f) => return Ok(f),
                Err(e) if attempt < attempts => {
                    tracing::warn!(%tenant, job = %job, attempt, error = %e,
                        "reading a DDL's result failed; trying again");
                    tokio::time::sleep(wait).await;
                    wait = wait.saturating_mul(2);
                    attempt += 1;
                }
                Err(e) => {
                    tracing::warn!(%tenant, job = %job, attempts, error = %e,
                        "reading a DDL's result failed; the reconciler will finish it");
                    return Err(map_names(e, tenant, &plan.names));
                }
            }
        }
    }

    /// Phase 1, waiting while a target is held.
    async fn prepare(
        &self,
        req: &DdlRequest<'_>,
        job: JobId,
        exec: &dyn DdlExecutor,
    ) -> Result<Planned> {
        let tenant = req.tenant;
        let deadline = self.clock.now() + self.opts.ddl_wait;
        let mut next_sweep = self.clock.now();
        loop {
            let mut locked = self.versioner.lock(tenant, VersionKind::Catalog).await?;
            let planned = match self.plan(&mut locked, req, job).await {
                Ok(Planned::Go(p)) => self.place(&mut locked, req.tenant, *p, exec).await,
                other => other,
            };
            match planned {
                Ok(Planned::Go(p)) => {
                    if let Err(e) = self.fail(tenant, DdlStep::Planned) {
                        drop(locked);
                        return Err(e);
                    }
                    locked.commit_unversioned().await?;
                    return Ok(Planned::Go(p));
                }
                Ok(Planned::Busy(what)) => {
                    locked.release().await?;
                    // A job its engine abandoned is resolved
                    // here, not after `catalog.stuck_state_minutes`.
                    if self.clock.now() >= next_sweep {
                        next_sweep = self.clock.now() + ABANDONED_SWEEP_EVERY;
                        match self.resolve_abandoned(tenant, job, exec).await {
                            Ok(0) => {}
                            Ok(_) => continue,
                            Err(e) => tracing::warn!(%tenant, job = %job, error = %e,
                                "looking for abandoned DDL jobs failed"),
                        }
                    }
                    if self.clock.now() >= deadline {
                        return Err(conflict(&what));
                    }
                    tokio::time::sleep(self.opts.poll).await;
                }
                Ok(other) => {
                    let seen = locked.previous();
                    locked.release().await?;
                    return Ok(match other {
                        Planned::Done(mut o) => {
                            o.seen = Some(seen);
                            Planned::Done(o)
                        }
                        other => other,
                    });
                }
                Err(e) => {
                    let _ = locked.release().await;
                    return Err(e);
                }
            }
        }
    }

    /// Reads and checks the targets, and writes the marks for a job.
    #[allow(clippy::too_many_lines)] // one arm per statement kind
    async fn plan(
        &self,
        locked: &mut Locked<B>,
        req: &DdlRequest<'_>,
        job: JobId,
    ) -> Result<Planned> {
        for c in &self.checks {
            c.check(req, locked.tx()).await?;
        }
        let tenant = req.tenant;
        let lctn = req.lower_case_table_names;
        let previous = locked.previous();
        let now = self.clock.wall();
        let intents = DdlIntent::all(&locked.tx().internal_rows(INTENT_PREFIX).await?);
        let held_db = |db: DbId| intents.iter().any(|i| i.database() == Some(db));
        let intent = |op: IntentOp| DdlIntent {
            job,
            started_ms: millis(now),
            ddl: req.logical_sql.to_owned(),
            user: req.user,
            shards: None,
            owner: Some(self.owner.id.clone()),
            op,
        };
        let tx = locked.tx();
        match &req.statement {
            DdlStatement::CreateDatabase {
                name,
                if_not_exists,
                charset,
                collation,
            } => {
                if name.is_empty() || name.chars().count() > 64 || name.ends_with(' ') {
                    return Err(std_err(
                        1102,
                        *b"42000",
                        format!("Incorrect database name '{name}'"),
                    ));
                }
                if is_reserved_database(name) {
                    return Err(SprigError::new(
                        SprigCode::ReservedName,
                        format!("'{name}' is a reserved name"),
                    ));
                }
                let clause = charset_clause(charset.as_deref(), collation.as_deref())?;
                let key = database_key(name, lctn);
                if let Some(d) = tx.database_by_name(&key).await? {
                    if d.state == DbState::Creating || held_db(d.db_id) {
                        return Ok(Planned::Busy(format!("database '{name}'")));
                    }
                    let msg = format!("Can't create database '{name}'; database exists");
                    if *if_not_exists {
                        return Ok(Planned::Done(DdlOutcome {
                            notes: vec![DdlNote {
                                code: 1007,
                                message: msg,
                            }],
                            ..DdlOutcome::default()
                        }));
                    }
                    return Err(std_err(1007, *b"HY000", msg));
                }
                let db = DbId(
                    u32::try_from(tx.next_id(Counter::Database).await?)
                        .map_err(|_| SprigError::internal("database ids exhausted"))?,
                );
                let group = GroupId(
                    u32::try_from(tx.next_id(Counter::Other("group")).await?)
                        .map_err(|_| SprigError::internal("group ids exhausted"))?,
                );
                let shard = u32::try_from(tx.next_id(Counter::Other("shard")).await?)
                    .map_err(|_| SprigError::internal("shard ids exhausted"))?;
                let row = DatabaseRow {
                    db_id: db,
                    name: name.clone(),
                    name_key: key,
                    default_charset: charset.clone().unwrap_or_default(),
                    default_collation: collation.clone().unwrap_or_default(),
                    default_group_id: group,
                    state: DbState::Creating,
                    options: serde_json::json!({ SHARDS_OPTION: [shard] }),
                    created_at: now,
                    created_by: req.user,
                    changed_version: previous,
                };
                tx.insert_database(&row)
                    .await
                    .map_err(|e| busy_on_duplicate(e, name))?;
                let schema = SchemaName::shard(tenant, db, group, ShardId(shard));
                let it = intent(IntentOp::CreateDatabase { db });
                tx.put_internal(&it.row(previous, now)?).await?;
                Ok(Planned::Go(Box::new(Plan {
                    fanout: None,
                    intent: it,
                    physical: vec![
                        PhysicalDdl {
                            schema,
                            sql: format!(
                                "CREATE DATABASE IF NOT EXISTS {}{clause}",
                                quote(&schema.to_string())
                            ),
                            effect: Effect::CreateSchema,
                        },
                        // The database's schema anchors the
                        // decisions of transactions that start on its
                        // unsharded tables.
                        dtx_table_ddl(schema),
                    ],
                    targets: Vec::new(),
                    schema: Some(schema),
                    names: vec![(db, name.clone())],
                    affected: 1,
                    notes: Vec::new(),
                })))
            }
            DdlStatement::AlterDatabase {
                name,
                charset,
                collation,
            } => {
                let clause = charset_clause(charset.as_deref(), collation.as_deref())?;
                let d = tx
                    .database_by_name(&database_key(name, lctn))
                    .await?
                    .ok_or_else(|| unknown_database(name))?;
                if d.state == DbState::Creating || held_db(d.db_id) {
                    return Ok(Planned::Busy(format!("database '{name}'")));
                }
                let stmts = shard_schemas(tenant, &d)
                    .into_iter()
                    .map(|s| PhysicalDdl {
                        schema: s,
                        sql: format!("ALTER DATABASE {}{clause}", quote(&s.to_string())),
                        effect: Effect::AlterSchema,
                    })
                    .collect();
                Ok(Planned::AlterDb(d, stmts))
            }
            DdlStatement::DropDatabase { name, if_exists } => {
                let Some(d) = tx.database_by_name(&database_key(name, lctn)).await? else {
                    let msg = format!("Can't drop database '{name}'; database doesn't exist");
                    if *if_exists {
                        return Ok(Planned::Done(DdlOutcome {
                            notes: vec![DdlNote {
                                code: 1008,
                                message: msg,
                            }],
                            ..DdlOutcome::default()
                        }));
                    }
                    return Err(std_err(1008, *b"HY000", msg));
                };
                if d.state == DbState::Creating || held_db(d.db_id) {
                    return Ok(Planned::Busy(format!("database '{name}'")));
                }
                let objs = tx.objects_in(d.db_id).await?;
                let live: Vec<&ObjectRow> = objs
                    .iter()
                    .filter(|o| o.state != ObjectState::Dropped)
                    .collect();
                if let Some(o) = live.iter().find(|o| o.pending_job_id.is_some()) {
                    return Ok(Planned::Busy(format!("'{name}.{}'", o.name)));
                }
                let tables = live.iter().filter(|o| o.kind == ObjectKind::Table).count() as u64;
                let it = intent(IntentOp::DropDatabase { db: d.db_id });
                tx.put_internal(&it.row(previous, now)?).await?;
                let physical = shard_schemas(tenant, &d)
                    .into_iter()
                    .map(|s| PhysicalDdl {
                        schema: s,
                        sql: format!("DROP DATABASE IF EXISTS {}", quote(&s.to_string())),
                        effect: Effect::DropSchema,
                    })
                    .collect();
                Ok(Planned::Go(Box::new(Plan {
                    fanout: None,
                    intent: it,
                    physical,
                    targets: Vec::new(),
                    schema: None,
                    names: vec![(d.db_id, d.name.clone())],
                    affected: tables,
                    notes: Vec::new(),
                })))
            }
            DdlStatement::CreateObject {
                kind,
                name,
                if_not_exists,
                or_replace,
                parent,
            } => {
                sql_kind(*kind)?;
                // The document engine keeps its own hidden tables
                // (`__sprig_doc_*`) in each logical database. Clients can't
                // name `__sprig_` objects at all (the statement rewrite
                // refuses them), so these arrive only from the engine's
                // internal-mode statements.
                let doc_engine = name.name.len() > 12
                    && name.name.as_bytes()[..12].eq_ignore_ascii_case(b"__sprig_doc_");
                // The key-value element tables (`__sprig_ke_`, `__sprig_kz_`)
                // and the Redis scripts table, from the key-value engine's
                // internal-mode statements likewise; the model table
                // (`__sprig_dim_models`) too.
                let kv_engine = [
                    "__sprig_ke_",
                    "__sprig_kz_",
                    "__sprig_redis_",
                    "__sprig_dim_",
                ]
                .iter()
                .any(|p| {
                    name.name.len() > p.len()
                        && name.name.as_bytes()[..p.len()].eq_ignore_ascii_case(p.as_bytes())
                });
                if is_internal_object(&name.name) && !doc_engine && !kv_engine {
                    return Err(SprigError::new(
                        SprigCode::ReservedName,
                        format!("'{}' is a reserved name", name.name),
                    ));
                }
                let db = match resolve_db(tx, name, req.current_db, lctn).await? {
                    Resolved::Db(d) => d,
                    Resolved::Busy(w) => return Ok(Planned::Busy(w)),
                };
                if held_db(db.db_id) {
                    return Ok(Planned::Busy(format!("database '{}'", db.name)));
                }
                let schema = single_schema(tenant, &db)?;
                let fan = multi::Fanout::of_create(
                    &db,
                    *kind,
                    &name.name,
                    parent.as_ref().map(|p| p.name.as_str()),
                );
                let key = object_key(*kind, &name.name, lctn);
                let names = vec![(db.db_id, db.name.clone())];
                let physical = vec![PhysicalDdl {
                    schema,
                    sql: req.sql.to_owned(),
                    effect: Effect::Create {
                        kind: *kind,
                        name: name.name.clone(),
                    },
                }];
                if let Some(mut o) = tx.object_by_name(db.db_id, kind.namespace(), &key).await? {
                    if o.pending_job_id.is_some() || o.state == ObjectState::Creating {
                        return Ok(Planned::Busy(format!("'{}.{}'", db.name, o.name)));
                    }
                    if !*or_replace || o.kind != *kind {
                        let (code, state, msg) = exists(*kind, &name.name);
                        if *if_not_exists {
                            return Ok(Planned::Done(DdlOutcome {
                                notes: vec![DdlNote { code, message: msg }],
                                ..DdlOutcome::default()
                            }));
                        }
                        return Err(std_err(code, state, msg));
                    }
                    o.pending_job_id = Some(job);
                    tx.update_object(&o).await?;
                    let it = intent(IntentOp::Create {
                        object: o.object_id,
                        replace: true,
                    });
                    tx.put_internal(&it.row(previous, now)?).await?;
                    return Ok(Planned::Go(Box::new(Plan {
                        fanout: Some(fan.clone()),
                        intent: it,
                        physical,
                        targets: vec![FactTarget {
                            object: o.object_id,
                            schema,
                            kind: *kind,
                            name: name.name.clone(),
                        }],
                        schema: None,
                        names,
                        affected: 0,
                        notes: Vec::new(),
                    })));
                }
                let parent_id = match (kind, parent) {
                    (ObjectKind::Trigger, Some(p)) => {
                        let pdb = match resolve_db(tx, p, Some(db.name.as_str()), lctn).await? {
                            Resolved::Db(d) => d,
                            Resolved::Busy(w) => return Ok(Planned::Busy(w)),
                        };
                        let pkey = object_key(ObjectKind::Table, &p.name, lctn);
                        let po = tx
                            .object_by_name(pdb.db_id, Namespace::Relation, &pkey)
                            .await?
                            .filter(|o| o.state == ObjectState::Active)
                            .ok_or_else(|| {
                                missing_err(ObjectKind::Table, &pdb.name, &p.name, false)
                            })?;
                        if po.pending_job_id.is_some() {
                            return Ok(Planned::Busy(format!("'{}.{}'", pdb.name, po.name)));
                        }
                        Some(po.object_id)
                    }
                    _ => None,
                };
                let id = ObjectId(tx.next_id(Counter::Object).await?);
                let row = ObjectRow {
                    object_id: id,
                    db_id: db.db_id,
                    namespace: kind.namespace(),
                    name: name.name.clone(),
                    name_key: key,
                    kind: *kind,
                    parent_id,
                    state: ObjectState::Creating,
                    schema_version: 0,
                    flags: ObjectFlags::empty(),
                    pending_job_id: Some(job),
                    created_at: now,
                    changed_version: previous,
                };
                tx.insert_object(&row)
                    .await
                    .map_err(|e| busy_on_duplicate(e, &name.name))?;
                let it = intent(IntentOp::Create {
                    object: id,
                    replace: false,
                });
                tx.put_internal(&it.row(previous, now)?).await?;
                Ok(Planned::Go(Box::new(Plan {
                    fanout: Some(fan),
                    intent: it,
                    physical,
                    targets: vec![FactTarget {
                        object: id,
                        schema,
                        kind: *kind,
                        name: name.name.clone(),
                    }],
                    schema: None,
                    names,
                    affected: 0,
                    notes: Vec::new(),
                })))
            }
            DdlStatement::AlterObject {
                kind,
                name,
                if_exists,
                rename_to,
            } => {
                sql_kind(*kind)?;
                let (db, mut o) = match find(tx, *kind, name, req.current_db, lctn, false).await? {
                    Found::Object(b) => *b,
                    Found::Busy(w) => return Ok(Planned::Busy(w)),
                    Found::Missing(e) if *if_exists => {
                        return Ok(Planned::Done(DdlOutcome {
                            notes: vec![DdlNote {
                                code: errno(&e).unwrap_or(1146),
                                message: e.message,
                            }],
                            ..DdlOutcome::default()
                        }));
                    }
                    Found::Missing(e) => return Err(missing_as_error(e)),
                };
                if held_db(db.db_id) {
                    return Ok(Planned::Busy(format!("database '{}'", db.name)));
                }
                let schema = single_schema(tenant, &db)?;
                if let Some(to) = rename_to {
                    return self
                        .plan_alter_rename(tx, req, (db, o), to, &held_db, previous, now, job)
                        .await;
                }
                o.pending_job_id = Some(job);
                tx.update_object(&o).await?;
                let it = intent(IntentOp::Alter {
                    object: o.object_id,
                });
                tx.put_internal(&it.row(previous, now)?).await?;
                Ok(Planned::Go(Box::new(Plan {
                    fanout: Some(multi::Fanout::of(&db, o.kind, &o.name)),
                    intent: it,
                    physical: vec![PhysicalDdl {
                        schema,
                        sql: req.sql.to_owned(),
                        effect: Effect::Alter {
                            kind: o.kind,
                            name: o.name.clone(),
                        },
                    }],
                    targets: vec![FactTarget {
                        object: o.object_id,
                        schema,
                        kind: o.kind,
                        name: o.name.clone(),
                    }],
                    schema: None,
                    names: vec![(db.db_id, db.name.clone())],
                    affected: 0,
                    notes: Vec::new(),
                })))
            }
            DdlStatement::DropObjects {
                kind,
                names,
                if_exists,
            } => {
                sql_kind(*kind)?;
                let mut found: Vec<(DatabaseRow, ObjectRow)> = Vec::new();
                let mut notes = Vec::new();
                for n in names {
                    match find(tx, *kind, n, req.current_db, lctn, true).await? {
                        Found::Object(b) => {
                            let (d, o) = *b;
                            if held_db(d.db_id) {
                                return Ok(Planned::Busy(format!("database '{}'", d.name)));
                            }
                            if found.iter().all(|(_, f)| f.object_id != o.object_id) {
                                found.push((d, o));
                            }
                        }
                        Found::Busy(w) => return Ok(Planned::Busy(w)),
                        Found::Missing(e) => {
                            if *if_exists {
                                notes.push(DdlNote {
                                    code: errno(&e).unwrap_or(1051),
                                    message: e.message,
                                });
                            } else {
                                return Err(missing_as_error(e));
                            }
                        }
                    }
                }
                let Some((first_db, _)) = found.first() else {
                    return Ok(Planned::Done(DdlOutcome {
                        notes,
                        ..DdlOutcome::default()
                    }));
                };
                let schema = single_schema(tenant, first_db)?;
                let first_id = first_db.db_id;
                let fan = multi::Fanout::of_drop(first_db, *kind, &found);
                let mut db_names: Vec<(DbId, String)> = Vec::new();
                let mut in_schema = Vec::new();
                for (d, o) in &mut found {
                    if !db_names.iter().any(|(id, _)| *id == d.db_id) {
                        db_names.push((d.db_id, d.name.clone()));
                    }
                    if d.db_id == first_id {
                        in_schema.push(o.name.clone());
                    }
                    o.pending_job_id = Some(job);
                    tx.update_object(o).await?;
                }
                let it = intent(IntentOp::Drop {
                    objects: found.iter().map(|(_, o)| o.object_id).collect(),
                });
                tx.put_internal(&it.row(previous, now)?).await?;
                Ok(Planned::Go(Box::new(Plan {
                    fanout: Some(fan),
                    intent: it,
                    physical: vec![PhysicalDdl {
                        schema,
                        sql: req.sql.to_owned(),
                        effect: Effect::Drop {
                            kind: *kind,
                            names: in_schema,
                        },
                    }],
                    targets: Vec::new(),
                    schema: None,
                    names: db_names,
                    affected: 0,
                    notes,
                })))
            }
            DdlStatement::RenameTables { pairs, if_exists } => {
                self.plan_rename(tx, req, pairs, *if_exists, &held_db, previous, now, job)
                    .await
            }
            DdlStatement::Truncate { name } => {
                let (db, _) =
                    match find(tx, ObjectKind::Table, name, req.current_db, lctn, false).await? {
                        Found::Object(b) => *b,
                        Found::Busy(w) => return Ok(Planned::Busy(w)),
                        Found::Missing(e) => return Err(missing_as_error(e)),
                    };
                if held_db(db.db_id) {
                    return Ok(Planned::Busy(format!("database '{}'", db.name)));
                }
                let schema = single_schema(tenant, &db)?;
                let fan = multi::Fanout::of(&db, ObjectKind::Table, &name.name);
                Ok(Planned::Run(
                    PhysicalDdl {
                        schema,
                        sql: req.sql.to_owned(),
                        effect: Effect::Truncate {
                            name: name.name.clone(),
                        },
                    },
                    vec![(db.db_id, db.name.clone())],
                    Some(fan),
                ))
            }
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // one statement's planning
    async fn plan_rename(
        &self,
        tx: &mut B::Tx,
        req: &DdlRequest<'_>,
        pairs: &[(ObjectName, ObjectName)],
        if_exists: bool,
        held_db: &(dyn Fn(DbId) -> bool + Sync),
        previous: u64,
        now: SystemTime,
        job: JobId,
    ) -> Result<Planned> {
        let tenant = req.tenant;
        let lctn = req.lower_case_table_names;
        // Simulate the moves in order, as MariaDB applies them: `overlay`
        // holds names the earlier moves took (`Some`) or freed (`None`).
        let mut overlay: BTreeMap<(DbId, String), Option<ObjectId>> = BTreeMap::new();
        let mut rows: BTreeMap<ObjectId, ObjectRow> = BTreeMap::new();
        let mut dbs: BTreeMap<DbId, DatabaseRow> = BTreeMap::new();
        let mut physical_moves = Vec::new();
        let mut notes = Vec::new();
        for (from, to) in pairs {
            let fdb = match resolve_db(tx, from, req.current_db, lctn).await? {
                Resolved::Db(d) => d,
                Resolved::Busy(w) => return Ok(Planned::Busy(w)),
            };
            let tdb = match resolve_db(tx, to, req.current_db, lctn).await? {
                Resolved::Db(d) => d,
                Resolved::Busy(w) => return Ok(Planned::Busy(w)),
            };
            for d in [&fdb, &tdb] {
                if held_db(d.db_id) {
                    return Ok(Planned::Busy(format!("database '{}'", d.name)));
                }
            }
            let fkey = object_key(ObjectKind::Table, &from.name, lctn);
            let tkey = object_key(ObjectKind::Table, &to.name, lctn);
            let src = match overlay.get(&(fdb.db_id, fkey.clone())) {
                Some(id) => *id,
                None => tx
                    .object_by_name(fdb.db_id, Namespace::Relation, &fkey)
                    .await?
                    .map(|o| {
                        let id = o.object_id;
                        rows.entry(id).or_insert(o);
                        id
                    }),
            };
            let Some(id) = src else {
                let msg = format!("Table '{}.{}' doesn't exist", fdb.name, from.name);
                if if_exists {
                    // MariaDB skips the move with a note.
                    notes.push(DdlNote {
                        code: 1146,
                        message: msg,
                    });
                    continue;
                }
                return Err(std_err(1146, *b"42S02", msg));
            };
            let Some(row) = rows.get(&id).cloned() else {
                return Err(SprigError::internal("rename source row missing"));
            };
            if row.pending_job_id.is_some() || row.state != ObjectState::Active {
                return Ok(Planned::Busy(format!("'{}.{}'", fdb.name, row.name)));
            }
            let taken = match overlay.get(&(tdb.db_id, tkey.clone())) {
                Some(id) => id.is_some(),
                None => tx
                    .object_by_name(tdb.db_id, Namespace::Relation, &tkey)
                    .await?
                    .is_some(),
            };
            if taken {
                return Err(std_err(
                    1050,
                    *b"42S01",
                    format!("Table '{}' already exists", to.name),
                ));
            }
            overlay.insert((fdb.db_id, fkey), None);
            overlay.insert((tdb.db_id, tkey.clone()), Some(id));
            physical_moves.push((
                single_schema(tenant, &fdb)?,
                row.name.clone(),
                single_schema(tenant, &tdb)?,
                to.name.clone(),
            ));
            if let Some(r) = rows.get_mut(&id) {
                r.db_id = tdb.db_id;
                r.name.clone_from(&to.name);
                r.name_key = tkey;
            }
            dbs.insert(fdb.db_id, fdb);
            dbs.insert(tdb.db_id, tdb);
        }
        if physical_moves.is_empty() {
            return Ok(Planned::Done(DdlOutcome {
                notes,
                ..DdlOutcome::default()
            }));
        }
        let mut moves = Vec::new();
        let mut targets = Vec::new();
        for (id, final_row) in &rows {
            let mut o = tx
                .object(*id)
                .await?
                .ok_or_else(|| SprigError::internal("renamed object vanished"))?;
            o.pending_job_id = Some(job);
            tx.update_object(&o).await?;
            let db = dbs
                .get(&final_row.db_id)
                .ok_or_else(|| SprigError::internal("rename target database"))?;
            moves.push(Move {
                object: *id,
                db: final_row.db_id,
                name: final_row.name.clone(),
                name_key: final_row.name_key.clone(),
            });
            targets.push(FactTarget {
                object: *id,
                schema: single_schema(tenant, db)?,
                kind: final_row.kind,
                name: final_row.name.clone(),
            });
        }
        let it = DdlIntent {
            job,
            started_ms: millis(now),
            ddl: req.logical_sql.to_owned(),
            user: req.user,
            shards: None,
            owner: Some(self.owner.id.clone()),
            op: IntentOp::Rename { moves },
        };
        tx.put_internal(&it.row(previous, now)?).await?;
        let schema = physical_moves
            .first()
            .map(|m| m.0)
            .ok_or_else(|| SprigError::internal("RENAME without moves"))?;
        let fan = dbs
            .values()
            .find(|d| single_schema(tenant, d).ok() == Some(schema))
            .map(|d| multi::Fanout::of_rename(d, &physical_moves));
        Ok(Planned::Go(Box::new(Plan {
            fanout: fan,
            intent: it,
            physical: vec![PhysicalDdl {
                schema,
                sql: req.sql.to_owned(),
                effect: Effect::Rename {
                    moves: physical_moves,
                },
            }],
            targets,
            schema: None,
            names: dbs.values().map(|d| (d.db_id, d.name.clone())).collect(),
            affected: 0,
            notes,
        })))
    }

    /// `ALTER ... RENAME TO` with other changes in the same statement: one
    /// move, published as a rename (the reconciler judges it the same way:
    /// finished when the new name exists and the old one doesn't).
    #[allow(clippy::too_many_arguments)] // one statement's planning
    async fn plan_alter_rename(
        &self,
        tx: &mut B::Tx,
        req: &DdlRequest<'_>,
        (db, o): (DatabaseRow, ObjectRow),
        to: &ObjectName,
        held_db: &(dyn Fn(DbId) -> bool + Sync),
        previous: u64,
        now: SystemTime,
        job: JobId,
    ) -> Result<Planned> {
        let tenant = req.tenant;
        let lctn = req.lower_case_table_names;
        let tdb = match resolve_db(tx, to, Some(db.name.as_str()), lctn).await? {
            Resolved::Db(d) => d,
            Resolved::Busy(w) => return Ok(Planned::Busy(w)),
        };
        if held_db(tdb.db_id) {
            return Ok(Planned::Busy(format!("database '{}'", tdb.name)));
        }
        if is_internal_object(&to.name) {
            return Err(SprigError::new(
                SprigCode::ReservedName,
                format!("'{}' is a reserved name", to.name),
            ));
        }
        let tkey = object_key(o.kind, &to.name, lctn);
        let same = tdb.db_id == db.db_id && tkey == o.name_key;
        if !same
            && tx
                .object_by_name(tdb.db_id, o.kind.namespace(), &tkey)
                .await?
                .is_some()
        {
            let (code, state, msg) = exists(o.kind, &to.name);
            return Err(std_err(code, state, msg));
        }
        let from_schema = single_schema(tenant, &db)?;
        let to_schema = single_schema(tenant, &tdb)?;
        let mut marked = o.clone();
        marked.pending_job_id = Some(job);
        tx.update_object(&marked).await?;
        let it = DdlIntent {
            job,
            started_ms: millis(now),
            ddl: req.logical_sql.to_owned(),
            user: req.user,
            shards: None,
            owner: Some(self.owner.id.clone()),
            op: IntentOp::Rename {
                moves: vec![Move {
                    object: o.object_id,
                    db: tdb.db_id,
                    name: to.name.clone(),
                    name_key: tkey,
                }],
            },
        };
        tx.put_internal(&it.row(previous, now)?).await?;
        let mut names = vec![(db.db_id, db.name.clone())];
        if tdb.db_id != db.db_id {
            names.push((tdb.db_id, tdb.name.clone()));
        }
        Ok(Planned::Go(Box::new(Plan {
            fanout: Some(multi::Fanout::of(&db, o.kind, &o.name)),
            intent: it,
            physical: vec![PhysicalDdl {
                schema: from_schema,
                sql: req.sql.to_owned(),
                effect: Effect::Rename {
                    moves: vec![(from_schema, o.name.clone(), to_schema, to.name.clone())],
                },
            }],
            targets: vec![FactTarget {
                object: o.object_id,
                schema: to_schema,
                kind: o.kind,
                name: to.name.clone(),
            }],
            schema: None,
            names,
            affected: 0,
            notes: Vec::new(),
        })))
    }

    /// `ALTER DATABASE`: the statements, then one versioned change with
    /// the schema's new defaults. Not marked: a crash between leaves the
    /// catalog's defaults stale, which the reconciler reports as drift.
    async fn alter_database(
        &self,
        tenant: TenantId,
        db: DatabaseRow,
        stmts: &[PhysicalDdl],
        exec: &dyn DdlExecutor,
    ) -> Result<DdlOutcome> {
        let names = vec![(db.db_id, db.name.clone())];
        for s in stmts {
            exec.execute(s)
                .await
                .map_err(|e| map_names(e, tenant, &names))?;
        }
        let Some(first) = stmts.first() else {
            return Ok(DdlOutcome::default());
        };
        let Some(info) = exec.schema(&first.schema).await? else {
            return Ok(DdlOutcome::default());
        };
        let mut locked = self.versioner.lock(tenant, VersionKind::Catalog).await?;
        let Some(mut row) = locked.tx().database(db.db_id).await? else {
            locked.release().await?;
            return Ok(DdlOutcome::default());
        };
        if row.default_charset == info.charset && row.default_collation == info.collation {
            locked.release().await?;
            return Ok(DdlOutcome::default());
        }
        let mut c = locked.allocate(ChangeOpts::default()).await?;
        let v = c.version();
        row.default_charset = info.charset;
        row.default_collation = info.collation;
        row.changed_version = v;
        if let Err(e) = c.tx().update_database(&row).await {
            let _ = c.rollback().await;
            return Err(e);
        }
        c.commit().await?;
        Ok(DdlOutcome {
            version: Some(v),
            affected_rows: 1,
            ..DdlOutcome::default()
        })
    }

    /// Phase 3: publishes a job's result in one versioned change, if the
    /// job still holds its targets. `None` if its intent row is gone (the
    /// reconciler resolved it).
    pub(crate) async fn finish(
        &self,
        tenant: TenantId,
        intent: &DdlIntent,
        facts: &Facts,
    ) -> Result<Option<u64>> {
        let key = intent_key(intent.job);
        let mut locked = self.versioner.lock(tenant, VersionKind::Catalog).await?;
        if locked.tx().internal_rows(&key).await?.is_empty() {
            locked.release().await?;
            return Ok(None);
        }
        let mut c = locked.allocate(ChangeOpts::default()).await?;
        let v = c.version();
        let now = self.clock.wall();
        let r = apply_finish(c.tx(), tenant, intent, facts, v, now).await;
        match r {
            Ok(()) => {
                c.commit().await?;
                tracing::info!(%tenant, job = %intent.job, version = v, "DDL published");
                Ok(Some(v))
            }
            Err(e) => {
                let _ = c.rollback().await;
                Err(e)
            }
        }
    }

    /// Undoes phase 1 (the physical statement didn't run): deletes
    /// `creating` rows, clears marks, deletes the intent row. Unversioned:
    /// nothing visible changed. No-op if the intent row is gone.
    pub(crate) async fn rollback(&self, tenant: TenantId, intent: &DdlIntent) -> Result<bool> {
        let key = intent_key(intent.job);
        let mut locked = self.versioner.lock(tenant, VersionKind::Catalog).await?;
        if locked.tx().internal_rows(&key).await?.is_empty() {
            locked.release().await?;
            return Ok(false);
        }
        let r = apply_rollback(locked.tx(), intent).await;
        match r {
            Ok(()) => {
                locked.commit_unversioned().await?;
                tracing::info!(%tenant, job = %intent.job, "DDL rolled back");
                Ok(true)
            }
            Err(e) => {
                let _ = locked.release().await;
                Err(e)
            }
        }
    }
}

/// Drops `schema`, which the refused statement's own plan created, if it
/// holds nothing but SprigDB's internal objects (the `__sprig_dtx` table
/// the plan's next statement may have made); anything else in it means it
/// isn't only ours, and it stays.
async fn drop_if_ours(exec: &dyn DdlExecutor, tenant: TenantId, job: JobId, schema: &SchemaName) {
    match exec.objects(schema).await {
        Ok(objs) if objs.iter().all(|o| is_internal_object(&o.name)) => {
            match exec.execute(&repair::drop_schema(*schema)).await {
                Ok(()) => tracing::warn!(%tenant, job = %job, %schema,
                    "a refused DDL's new schema dropped"),
                Err(e) => tracing::warn!(%tenant, job = %job, %schema, error = %e,
                    "a refused DDL's new schema couldn't be dropped"),
            }
        }
        Ok(_) => tracing::warn!(%tenant, job = %job, %schema,
            "a refused DDL's schema holds other objects; kept"),
        Err(e) => tracing::warn!(%tenant, job = %job, %schema, error = %e,
            "a refused DDL's schema couldn't be read; kept"),
    }
}

fn conflict(what: &str) -> SprigError {
    SprigError::new(
        SprigCode::CatalogConflict,
        format!("another DDL statement is running on {what}; retry"),
    )
}

/// The MySQL error number of a backend or standard error.
#[must_use]
pub fn errno(e: &SprigError) -> Option<u16> {
    match e.kind {
        sprig_common::ErrorKind::Backend { errno, .. }
        | sprig_common::ErrorKind::Standard { errno, .. } => Some(errno),
        sprig_common::ErrorKind::Sprig(c) => Some(c.number()),
    }
}

/// A 1062 on a live name inside phase 1: another job got there first.
fn busy_on_duplicate(e: SprigError, name: &str) -> SprigError {
    if errno(&e) == Some(1062) {
        SprigError::new(
            SprigCode::CatalogConflict,
            format!("another DDL statement is creating '{name}'; retry"),
        )
    } else {
        e
    }
}

fn map_names(mut e: SprigError, tenant: TenantId, names: &[(DbId, String)]) -> SprigError {
    e.message = logical_text(&e.message, tenant, &|id| {
        names.iter().find(|(d, _)| *d == id).map(|(_, n)| n.clone())
    });
    e
}

enum Resolved {
    Db(DatabaseRow),
    Busy(String),
}

async fn resolve_db<T: DdlTx + ?Sized>(
    tx: &mut T,
    name: &ObjectName,
    current: Option<&str>,
    lctn: bool,
) -> Result<Resolved> {
    let db = name.db.as_deref().or(current).ok_or_else(no_database)?;
    let d = tx
        .database_by_name(&database_key(db, lctn))
        .await?
        .ok_or_else(|| unknown_database(db))?;
    if d.state == DbState::Creating {
        return Ok(Resolved::Busy(format!("database '{db}'")));
    }
    Ok(Resolved::Db(d))
}

enum Found {
    Object(Box<(DatabaseRow, ObjectRow)>),
    Busy(String),
    Missing(SprigError),
}

async fn find<T: DdlTx + ?Sized>(
    tx: &mut T,
    kind: ObjectKind,
    name: &ObjectName,
    current: Option<&str>,
    lctn: bool,
    dropping: bool,
) -> Result<Found> {
    let db_name = name.db.as_deref().or(current).ok_or_else(no_database)?;
    let Some(d) = tx.database_by_name(&database_key(db_name, lctn)).await? else {
        if dropping {
            // DROP ... IF EXISTS on an unknown database is a note too.
            return Ok(Found::Missing(missing_err(kind, db_name, &name.name, true)));
        }
        return Err(unknown_database(db_name));
    };
    if d.state == DbState::Creating {
        return Ok(Found::Busy(format!("database '{db_name}'")));
    }
    let key = object_key(kind, &name.name, lctn);
    let o = tx.object_by_name(d.db_id, kind.namespace(), &key).await?;
    match o {
        Some(o) if o.pending_job_id.is_some() || o.state != ObjectState::Active => {
            Ok(Found::Busy(format!("'{}.{}'", d.name, o.name)))
        }
        // A view named in DROP TABLE, a table in ALTER VIEW: MariaDB's
        // own errors.
        Some(o) if o.kind == kind || kind.namespace() == Namespace::Relation => {
            Ok(Found::Object(Box::new((d, o))))
        }
        _ => Ok(Found::Missing(missing_err(
            kind, &d.name, &name.name, dropping,
        ))),
    }
}

/// Reads what phase 3 publishes.
pub(crate) async fn gather(
    exec: &dyn DdlExecutor,
    schema: Option<&SchemaName>,
    targets: &[FactTarget],
) -> Result<Facts> {
    let mut f = Facts::default();
    if let Some(s) = schema {
        f.schema = exec.schema(s).await?;
    }
    for t in targets {
        let text = exec.show_create(&t.schema, t.kind, &t.name).await?;
        f.create_sql.insert(t.object, text);
    }
    Ok(f)
}

/// Phase 3's writes, inside the versioned change.
async fn apply_finish<T: DdlTx + ?Sized>(
    tx: &mut T,
    tenant: TenantId,
    intent: &DdlIntent,
    facts: &Facts,
    v: u64,
    now: SystemTime,
) -> Result<()> {
    // Logical names for create_sql.
    let mut dbs: BTreeMap<DbId, String> = BTreeMap::new();
    for d in tx.databases().await? {
        if d.state != DbState::Dropped {
            dbs.insert(d.db_id, d.name);
        }
    }
    let logical = |text: &Option<String>| -> Option<String> {
        text.as_ref()
            .map(|t| logical_text(t, tenant, &|id| dbs.get(&id).cloned()))
    };
    match &intent.op {
        IntentOp::CreateDatabase { db } => {
            let mut d = tx
                .database(*db)
                .await?
                .ok_or_else(|| SprigError::internal(format!("database {db} vanished")))?;
            d.state = DbState::Active;
            if let Some(info) = &facts.schema {
                d.default_charset.clone_from(&info.charset);
                d.default_collation.clone_from(&info.collation);
            }
            d.changed_version = v;
            tx.update_database(&d).await?;
        }
        IntentOp::DropDatabase { db } => {
            if let Some(mut d) = tx.database(*db).await? {
                d.state = DbState::Dropped;
                d.changed_version = v;
                tx.update_database(&d).await?;
            }
            for mut o in tx.objects_in(*db).await? {
                if o.state != ObjectState::Dropped {
                    o.state = ObjectState::Dropped;
                    o.pending_job_id = None;
                    o.changed_version = v;
                    tx.update_object(&o).await?;
                }
            }
        }
        IntentOp::Create { object, .. } | IntentOp::Alter { object } => {
            let mut o = tx
                .object(*object)
                .await?
                .ok_or_else(|| SprigError::internal(format!("object {object} vanished")))?;
            let create_sql = facts.create_sql.get(object).and_then(&logical);
            publish_version(tx, &mut o, intent, create_sql, v, now).await?;
        }
        IntentOp::Drop { objects } => {
            let set: BTreeSet<ObjectId> = objects.iter().copied().collect();
            let mut dbs_touched = BTreeSet::new();
            for id in objects {
                if let Some(mut o) = tx.object(*id).await? {
                    dbs_touched.insert(o.db_id);
                    o.state = ObjectState::Dropped;
                    o.pending_job_id = None;
                    o.changed_version = v;
                    tx.update_object(&o).await?;
                }
            }
            // A table's triggers go with it.
            for db in dbs_touched {
                for mut o in tx.objects_in(db).await? {
                    if o.state != ObjectState::Dropped
                        && o.parent_id.is_some_and(|p| set.contains(&p))
                    {
                        o.state = ObjectState::Dropped;
                        o.changed_version = v;
                        tx.update_object(&o).await?;
                    }
                }
            }
        }
        IntentOp::Rename { moves } => {
            // Two passes, so swaps never collide on the live-name key.
            for m in moves {
                if let Some(mut o) = tx.object(m.object).await? {
                    o.name_key = format!("#sprig_rename_{}", m.object);
                    o.changed_version = v;
                    tx.update_object(&o).await?;
                }
            }
            for m in moves {
                let Some(mut o) = tx.object(m.object).await? else {
                    continue;
                };
                o.db_id = m.db;
                o.name.clone_from(&m.name);
                o.name_key.clone_from(&m.name_key);
                let create_sql = facts.create_sql.get(&m.object).and_then(&logical);
                publish_version(tx, &mut o, intent, create_sql, v, now).await?;
            }
        }
    }
    tx.delete_internal(&intent_key(intent.job)).await
}

/// Makes `o` active at a new schema version with its history row.
async fn publish_version<T: DdlTx + ?Sized>(
    tx: &mut T,
    o: &mut ObjectRow,
    intent: &DdlIntent,
    create_sql: Option<String>,
    v: u64,
    now: SystemTime,
) -> Result<()> {
    let prev = tx
        .latest_schema_version(o.object_id)
        .await?
        .map_or(0, |r| r.version)
        .max(o.schema_version);
    o.state = ObjectState::Active;
    o.pending_job_id = None;
    o.schema_version = prev + 1;
    o.changed_version = v;
    tx.update_object(o).await?;
    tx.insert_schema_version(&SchemaVersionRow {
        object_id: o.object_id,
        version: prev + 1,
        ddl: intent.ddl.clone(),
        create_sql_hash: create_sql.as_deref().map(sha256),
        create_sql,
        ddl_job_id: Some(intent.job),
        applied_by: intent.user,
        applied_at: now,
        changed_version: v,
    })
    .await
}

/// Phase 1's undo, inside an unversioned commit.
async fn apply_rollback<T: DdlTx + ?Sized>(tx: &mut T, intent: &DdlIntent) -> Result<()> {
    match &intent.op {
        IntentOp::CreateDatabase { db } => {
            if tx
                .database(*db)
                .await?
                .is_some_and(|d| d.state == DbState::Creating)
            {
                tx.delete_database(*db).await?;
            }
        }
        IntentOp::Create {
            object,
            replace: false,
        } => {
            if tx
                .object(*object)
                .await?
                .is_some_and(|o| o.state == ObjectState::Creating)
            {
                tx.delete_object(*object).await?;
            }
        }
        IntentOp::DropDatabase { .. } => {}
        _ => {
            for id in intent.objects() {
                if let Some(mut o) = tx.object(id).await?
                    && o.pending_job_id == Some(intent.job)
                {
                    o.pending_job_id = None;
                    tx.update_object(&o).await?;
                }
            }
        }
    }
    tx.delete_internal(&intent_key(intent.job)).await
}
