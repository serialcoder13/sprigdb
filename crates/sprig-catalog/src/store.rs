//! Typed access to the catalog's four tables (chapter 08, build step 2).
//!
//! [`Store`] is bound to one catalog schema and runs on any `mysql_async`
//! connection or transaction ([`Queryable`]), so the same calls serve
//! versioned changes ([`crate::version`]), unversioned commits of
//! transient states ("Intermediate states (`creating`) commit without a
//! version") and cache loads.
//!
//! Conventions the store keeps:
//! - **Ids are never reused.** [`Store::next_ids`] hands out ids from the
//!   internal counters `sprig.next_*_id` in one atomic statement.
//! - **Tombstones.** A drop sets `state = 'dropped'`, which frees the name
//!   (`live_name` becomes NULL). A setting reset that leaves a row empty
//!   keeps it as an empty row with the change's version, so incremental
//!   loads see the reset; [`Store::purge`] removes tombstones, their
//!   settings rows and empty rows, and raises `sprig.purged_version` so
//!   caches older than it reload in full.
//! - **Versions.** Every write takes the version it records
//!   (`changed_version`, or `version` for settings): the change's version
//!   inside [`change`](crate::version::Versioner::change), or the current
//!   committed version for an unversioned commit ([`Store::versions`]).
//! - Nothing here checks privileges, bounds or `sprig.` keys: those are
//!   the callers' (chapter 08 step 7, chapter 29).

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use mysql_async::prelude::{Queryable, ToValue};
use mysql_async::{Params, Pool, Row, Transaction, TxOpts, Value};
use serde_json::Value as Json;
use sprig_common::ids::{DbId, GroupId, JobId, ObjectId, TenantId, UserId};
use sprig_common::names::{SchemaName, quote};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::{Bounds, Entry};

use crate::sql::{bytes_col, col, json_col, map_err, time_col, to_datetime};
use crate::version::VersionKind;

macro_rules! sql_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident = $text:literal,)* }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name { $($(#[$vdoc])* $variant,)* }

        impl $name {
            /// Every value, in the column's order.
            pub const ALL: &'static [$name] = &[$($name::$variant),*];

            /// The column's text.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $($name::$variant => $text,)* }
            }

            /// Parses the column's text.
            ///
            /// # Errors
            /// `Internal` for a value this build doesn't know.
            pub fn parse(s: &str) -> Result<Self> {
                match s {
                    $($text => Ok($name::$variant),)*
                    other => Err(SprigError::internal(format!(
                        concat!("unknown ", stringify!($name), " {:?}"), other
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
    /// `databases.state`.
    DbState {
        /// Inserted, not yet created on the shards.
        Creating = "creating",
        /// In use.
        Active = "active",
        /// Being dropped.
        Dropping = "dropping",
        /// A tombstone: the name is free, the id is not.
        Dropped = "dropped",
    }
}

sql_enum! {
    /// `objects.state`.
    ObjectState {
        /// Inserted, DDL not yet applied.
        Creating = "creating",
        /// In use.
        Active = "active",
        /// An `ALTER` is being applied.
        Altering = "altering",
        /// Being dropped.
        Dropping = "dropping",
        /// A tombstone.
        Dropped = "dropped",
    }
}

sql_enum! {
    /// `objects.namespace`: names are unique per database and namespace.
    Namespace {
        /// Tables, views, sequences, collections, key-value namespaces.
        Relation = "relation",
        /// Stored procedures.
        Procedure = "procedure",
        /// SQL and logic functions (`SELECT f(x)` binds either).
        Function = "function",
        /// Triggers.
        Trigger = "trigger",
        /// Events.
        Event = "event",
        /// Types (chapter 32).
        Type = "type",
        /// Search indexes (chapter 39).
        SearchIndex = "search_index",
        /// Logic validations, triggers after commit and schedules (41).
        Logic = "logic",
        /// Property graphs (chapter 43).
        Graph = "graph",
    }
}

sql_enum! {
    /// `objects.kind`.
    ObjectKind {
        /// A table.
        Table = "table",
        /// A view.
        View = "view",
        /// A sequence.
        Sequence = "sequence",
        /// A document collection (chapter 31).
        Collection = "collection",
        /// A key-value namespace, Redis databases included (33, 40).
        KvNamespace = "kv_namespace",
        /// A stored procedure.
        Procedure = "procedure",
        /// A SQL function.
        Function = "function",
        /// A trigger.
        Trigger = "trigger",
        /// An event.
        Event = "event",
        /// A type.
        Type = "type",
        /// A search index.
        SearchIndex = "search_index",
        /// A logic function.
        LogicFunction = "logic_function",
        /// A logic validation.
        LogicValidation = "logic_validation",
        /// A logic trigger after commit.
        LogicTrigger = "logic_trigger",
        /// A logic schedule.
        LogicSchedule = "logic_schedule",
        /// A property graph.
        Graph = "graph",
    }
}

impl ObjectKind {
    /// The namespace objects of this kind live in (chapter 08,
    /// "Namespaces").
    #[must_use]
    pub const fn namespace(self) -> Namespace {
        match self {
            ObjectKind::Table
            | ObjectKind::View
            | ObjectKind::Sequence
            | ObjectKind::Collection
            | ObjectKind::KvNamespace => Namespace::Relation,
            ObjectKind::Procedure => Namespace::Procedure,
            ObjectKind::Function | ObjectKind::LogicFunction => Namespace::Function,
            ObjectKind::Trigger => Namespace::Trigger,
            ObjectKind::Event => Namespace::Event,
            ObjectKind::Type => Namespace::Type,
            ObjectKind::SearchIndex => Namespace::SearchIndex,
            ObjectKind::LogicValidation | ObjectKind::LogicTrigger | ObjectKind::LogicSchedule => {
                Namespace::Logic
            }
            ObjectKind::Graph => Namespace::Graph,
        }
    }
}

/// `objects.flags`, a `SET`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ObjectFlags(u8);

impl ObjectFlags {
    /// The invisible primary key was added (chapter 07).
    pub const GIPK_ADDED: ObjectFlags = ObjectFlags(1);
    /// A system-versioned table.
    pub const SYSTEM_VERSIONED: ObjectFlags = ObjectFlags(2);
    /// Inheritable by descendants (chapter 42).
    pub const INHERITABLE: ObjectFlags = ObjectFlags(4);
    /// A Redis database (`kv_namespace`, chapter 40).
    pub const REDIS: ObjectFlags = ObjectFlags(8);
    /// Row-level security on (chapter 29).
    pub const RLS: ObjectFlags = ObjectFlags(16);

    const NAMES: [(ObjectFlags, &'static str); 5] = [
        (Self::GIPK_ADDED, "gipk_added"),
        (Self::SYSTEM_VERSIONED, "system_versioned"),
        (Self::INHERITABLE, "inheritable"),
        (Self::REDIS, "redis"),
        (Self::RLS, "rls"),
    ];

    /// No flags.
    #[must_use]
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Whether every flag of `other` is set.
    #[must_use]
    pub const fn contains(self, other: ObjectFlags) -> bool {
        self.0 & other.0 == other.0
    }

    /// The column's text: names joined by commas.
    #[must_use]
    pub fn to_sql(self) -> String {
        Self::NAMES
            .iter()
            .filter(|(f, _)| self.contains(*f))
            .map(|(_, n)| *n)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Parses the column's text.
    ///
    /// # Errors
    /// `Internal` for a flag this build doesn't know.
    pub fn parse(s: &str) -> Result<Self> {
        let mut out = Self(0);
        for part in s.split(',').filter(|p| !p.is_empty()) {
            let (f, _) = Self::NAMES
                .iter()
                .find(|(_, n)| *n == part)
                .ok_or_else(|| SprigError::internal(format!("unknown object flag {part:?}")))?;
            out = out | *f;
        }
        Ok(out)
    }
}

impl std::ops::BitOr for ObjectFlags {
    type Output = ObjectFlags;
    fn bitor(self, rhs: ObjectFlags) -> ObjectFlags {
        ObjectFlags(self.0 | rhs.0)
    }
}

/// The scope of a `settings` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SettingScope {
    /// The tenant (`scope_id` 0); also holds the internal `sprig.` keys.
    Tenant,
    /// A database.
    Database(DbId),
    /// An object.
    Object(ObjectId),
    /// A read-only copy of an ancestor's tenant-scope row.
    Ancestor(TenantId),
}

impl SettingScope {
    /// `scope_kind` and `scope_id`.
    #[must_use]
    pub fn to_sql(self) -> (&'static str, u64) {
        match self {
            SettingScope::Tenant => ("tenant", 0),
            SettingScope::Database(d) => ("database", u64::from(d.0)),
            SettingScope::Object(o) => ("object", o.0),
            SettingScope::Ancestor(t) => ("ancestor", t.0),
        }
    }

    /// Parses `scope_kind` and `scope_id`.
    ///
    /// # Errors
    /// `Internal` for an unknown kind or an id out of range.
    pub fn parse(kind: &str, id: u64) -> Result<Self> {
        Ok(match kind {
            "tenant" => SettingScope::Tenant,
            "database" => SettingScope::Database(DbId(u32::try_from(id).map_err(|_| {
                SprigError::internal(format!("database scope id {id} out of range"))
            })?)),
            "object" => SettingScope::Object(ObjectId(id)),
            "ancestor" => SettingScope::Ancestor(TenantId(id)),
            other => {
                return Err(SprigError::internal(format!(
                    "unknown setting scope {other:?}"
                )));
            }
        })
    }
}

/// An id counter in the internal keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Counter {
    /// `sprig.next_db_id`.
    Database,
    /// `sprig.next_object_id`.
    Object,
    /// `sprig.next_<name>_id`, for other modules (`user`, `role`, ...).
    /// The module inserts its row in its first migration.
    Other(&'static str),
}

impl Counter {
    /// The internal key.
    #[must_use]
    pub fn key(self) -> String {
        match self {
            Counter::Database => "sprig.next_db_id".to_owned(),
            Counter::Object => "sprig.next_object_id".to_owned(),
            Counter::Other(name) => format!("sprig.next_{name}_id"),
        }
    }
}

/// A `databases` row.
#[derive(Debug, Clone, PartialEq)]
pub struct DatabaseRow {
    /// The id; never reused.
    pub db_id: DbId,
    /// The name as the client spelled it.
    pub name: String,
    /// The lookup form (lower case if `lower_case_table_names = 1`).
    pub name_key: String,
    /// Default character set.
    pub default_charset: String,
    /// Default collation.
    pub default_collation: String,
    /// Table group new tables join by default.
    pub default_group_id: GroupId,
    /// Lifecycle state.
    pub state: DbState,
    /// Options, a JSON object.
    pub options: Json,
    /// Creation time (UTC).
    pub created_at: SystemTime,
    /// The creating user.
    pub created_by: Option<UserId>,
    /// Catalog version of the last change.
    pub changed_version: u64,
}

/// An `objects` row.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectRow {
    /// The id; never reused.
    pub object_id: ObjectId,
    /// The owning database.
    pub db_id: DbId,
    /// The namespace; must equal `kind.namespace()`.
    pub namespace: Namespace,
    /// The name as the client spelled it.
    pub name: String,
    /// The lookup form.
    pub name_key: String,
    /// What the object is.
    pub kind: ObjectKind,
    /// For a trigger or search index, the object it belongs to.
    pub parent_id: Option<ObjectId>,
    /// Lifecycle state.
    pub state: ObjectState,
    /// The current `schema_versions.version`.
    pub schema_version: u64,
    /// Flags.
    pub flags: ObjectFlags,
    /// The DDL job working on the object (chapter 21).
    pub pending_job_id: Option<JobId>,
    /// Creation time (UTC).
    pub created_at: SystemTime,
    /// Catalog version of the last change.
    pub changed_version: u64,
}

/// A `settings` row.
#[derive(Debug, Clone, PartialEq)]
pub struct SettingRow {
    /// The scope.
    pub scope: SettingScope,
    /// Dotted `snake_case` key.
    pub key: String,
    /// The value; `None` when the row only sets bounds. Internal keys hold
    /// any JSON; registry keys hold a scalar.
    pub value: Option<Json>,
    /// Bounds on narrower scopes.
    pub bounds: Option<Bounds>,
    /// `user:7`, `op:<subject>` or `system`; at most 288 characters.
    pub set_by: String,
    /// When (UTC).
    pub set_at: SystemTime,
    /// Catalog version of the last change.
    pub version: u64,
}

impl SettingRow {
    /// An internal (`sprig.`) key.
    #[must_use]
    pub fn is_internal(&self) -> bool {
        self.key.starts_with("sprig.")
    }

    /// A tombstone left by a reset: no value and no bounds.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.value.is_none() && self.bounds.is_none()
    }

    /// The row as resolution reads it.
    ///
    /// # Errors
    /// `Internal` if the value is not a setting scalar.
    pub fn entry(&self) -> Result<Entry> {
        let value = self
            .value
            .clone()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| {
                SprigError::internal(format!("setting {}: not a scalar value: {e}", self.key))
            })?;
        Ok(Entry {
            value,
            bounds: self.bounds.clone(),
        })
    }
}

/// A `schema_versions` row.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaVersionRow {
    /// The object.
    pub object_id: ObjectId,
    /// Its schema version.
    pub version: u64,
    /// The statement as accepted, with logical names.
    pub ddl: String,
    /// `SHOW CREATE` after apply, without automatic indexes.
    pub create_sql: Option<String>,
    /// SHA-256 of `create_sql`.
    pub create_sql_hash: Option<[u8; 32]>,
    /// The DDL job (chapter 21).
    pub ddl_job_id: Option<JobId>,
    /// The user who ran it.
    pub applied_by: Option<UserId>,
    /// When (UTC).
    pub applied_at: SystemTime,
    /// Catalog version of the change.
    pub changed_version: u64,
}

/// The internal versions of a catalog.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Versions {
    /// `sprig.catalog_version`.
    pub catalog: u64,
    /// `sprig.auth_version`.
    pub auth: u64,
    /// `sprig.purged_version`: incremental loads from older versions must
    /// reload in full.
    pub purged: u64,
}

impl Versions {
    /// The version of `kind`.
    #[must_use]
    pub fn get(&self, kind: VersionKind) -> u64 {
        match kind {
            VersionKind::Catalog => self.catalog,
            VersionKind::Auth => self.auth,
        }
    }
}

/// Rows read in one consistent snapshot, for the cache loader.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    /// The versions in the snapshot.
    pub versions: Versions,
    /// `Some(v)`: only rows changed after `v` (an incremental load).
    /// `None`: every row (a full load), also when the caller asked for an
    /// incremental load from before `sprig.purged_version`.
    pub since: Option<u64>,
    /// `databases` rows.
    pub databases: Vec<DatabaseRow>,
    /// `objects` rows.
    pub objects: Vec<ObjectRow>,
    /// `settings` rows, internal keys included.
    pub settings: Vec<SettingRow>,
}

/// What [`Store::purge`] removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PurgeReport {
    /// Dropped databases removed.
    pub databases: u64,
    /// Dropped objects removed.
    pub objects: u64,
    /// Settings rows removed: empty rows and rows of removed scopes.
    pub settings: u64,
    /// `schema_versions` rows of removed objects.
    pub schema_versions: u64,
}

const DB_COLS: &str = "db_id, name, name_key, default_charset, default_collation, \
                       default_group_id, state, options, created_at, created_by, changed_version";
const OBJ_COLS: &str = "object_id, db_id, namespace, name, name_key, kind, parent_id, state, \
                        schema_version, flags, pending_job_id, created_at, changed_version";
const SET_COLS: &str = "scope_kind, scope_id, setting_key, value, bounds, set_by, set_at, version";
const SV_COLS: &str = "object_id, version, ddl, create_sql, create_sql_hash, ddl_job_id, \
                       applied_by, applied_at, changed_version";

/// Typed access to one catalog schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Store {
    schema: String,
    databases: String,
    objects: String,
    settings: String,
    schema_versions: String,
}

impl Store {
    /// A store over `schema` (normally `t<tenant>_sprig`; tests use their
    /// own names).
    #[must_use]
    pub fn new(schema: impl Into<String>) -> Self {
        let schema = schema.into();
        let q = quote(&schema);
        Self {
            databases: format!("{q}.`databases`"),
            objects: format!("{q}.`objects`"),
            settings: format!("{q}.`settings`"),
            schema_versions: format!("{q}.`schema_versions`"),
            schema,
        }
    }

    /// The store of `tenant`'s catalog, `t<tenant>_sprig`.
    #[must_use]
    pub fn for_tenant(tenant: TenantId) -> Self {
        Self::new(SchemaName::catalog(tenant).to_string())
    }

    /// The schema name.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    // ---- internal keys and versions ----

    /// Reads the catalog, auth and purged versions.
    ///
    /// # Errors
    /// 39070 or a backend error; 39072 if the catalog has no version rows
    /// (not migrated).
    pub async fn versions<Q: Queryable + Send>(&self, q: &mut Q) -> Result<Versions> {
        let rows: Vec<(String, Vec<u8>)> = q
            .exec(
                format!(
                    "SELECT setting_key, value FROM {} WHERE scope_kind = 'tenant' AND scope_id = 0 \
                     AND setting_key IN ('sprig.catalog_version', 'sprig.auth_version', \
                     'sprig.purged_version')",
                    self.settings
                ),
                (),
            )
            .await
            .map_err(map_err)?;
        let mut v = Versions::default();
        let mut found = 0;
        for (key, raw) in rows {
            let n = parse_u64(&key, &raw)?;
            match key.as_str() {
                "sprig.catalog_version" => v.catalog = n,
                "sprig.auth_version" => v.auth = n,
                _ => v.purged = n,
            }
            found += 1;
        }
        if found < 3 {
            return Err(not_ready());
        }
        Ok(v)
    }

    /// Locks the version row of `kind` (`SELECT ... FOR UPDATE`) and
    /// returns its value. Waits at most `lock_wait_secs` for the lock.
    ///
    /// # Errors
    /// 39071 if the lock wait times out; 39072 if the row is missing.
    pub async fn lock_version<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        kind: VersionKind,
        lock_wait_secs: u64,
    ) -> Result<u64> {
        // SET STATEMENT limits the lock wait to this statement.
        let raw: Option<Vec<u8>> = q
            .exec_first(
                format!(
                    "SET STATEMENT innodb_lock_wait_timeout = {} FOR SELECT value FROM {} \
                     WHERE scope_kind = 'tenant' AND scope_id = 0 AND setting_key = ? FOR UPDATE",
                    lock_wait_secs.max(1),
                    self.settings
                ),
                (kind.key(),),
            )
            .await
            .map_err(map_err)?;
        let raw = raw.ok_or_else(not_ready)?;
        parse_u64(kind.key(), &raw)
    }

    /// Sets the version row of `kind` to `v`. Call only with the row
    /// locked, inside the change's transaction.
    ///
    /// # Errors
    /// 39070 or a backend error; 39072 if the row is missing.
    pub async fn set_version<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        kind: VersionKind,
        v: u64,
        at: SystemTime,
    ) -> Result<()> {
        let n = exec_count(
            q,
            format!(
                "UPDATE {} SET value = ?, version = ?, set_at = ? WHERE scope_kind = 'tenant' \
                 AND scope_id = 0 AND setting_key = ?",
                self.settings
            ),
            (v.to_string(), v, to_datetime(at), kind.key()),
        )
        .await?;
        // Affected rows skip matched-but-unchanged rows, so check that a
        // zero means a missing row.
        if n == 0 && self.internal(q, kind.key()).await?.is_none() {
            return Err(not_ready());
        }
        Ok(())
    }

    /// Reads an internal (`sprig.`) key at tenant scope.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for bad JSON.
    pub async fn internal<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        key: &str,
    ) -> Result<Option<Json>> {
        Ok(self
            .setting(q, SettingScope::Tenant, key)
            .await?
            .and_then(|r| r.value))
    }

    /// Writes an internal (`sprig.`) key at tenant scope.
    ///
    /// # Errors
    /// `Internal` unless `key` starts with `sprig.`; 39070 or a backend
    /// error.
    pub async fn put_internal<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        key: &str,
        value: &Json,
        version: u64,
        at: SystemTime,
    ) -> Result<()> {
        if !key.starts_with("sprig.") {
            return Err(SprigError::internal(format!(
                "{key} is not an internal key"
            )));
        }
        self.put_setting(
            q,
            &SettingRow {
                scope: SettingScope::Tenant,
                key: key.to_owned(),
                value: Some(value.clone()),
                bounds: None,
                set_by: "system".to_owned(),
                set_at: at,
                version,
            },
        )
        .await
    }

    /// Reserves `n` ids from `counter` and returns the first; the ids are
    /// `first..first + n`. One atomic statement, so it is safe in
    /// autocommit mode and never hands out an id twice. The counter row is
    /// not a versioned change: its `version` is left alone.
    ///
    /// # Errors
    /// 39072 if the counter row is missing; 39070 or a backend error.
    pub async fn next_ids<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        counter: Counter,
        n: u64,
    ) -> Result<u64> {
        if n == 0 {
            return Err(SprigError::internal("reserving zero ids"));
        }
        let key = counter.key();
        let changed = exec_count(
            q,
            format!(
                "UPDATE {} SET value = LAST_INSERT_ID(CAST(value AS UNSIGNED) + ?), \
                 set_at = UTC_TIMESTAMP(6) WHERE scope_kind = 'tenant' AND scope_id = 0 \
                 AND setting_key = ?",
                self.settings
            ),
            (n, &key),
        )
        .await?;
        if changed == 0 {
            return Err(not_ready());
        }
        let next: Option<u64> = q
            .exec_first("SELECT LAST_INSERT_ID()", ())
            .await
            .map_err(map_err)?;
        let next = next.ok_or_else(|| SprigError::internal("LAST_INSERT_ID() returned nothing"))?;
        next.checked_sub(n)
            .ok_or_else(|| SprigError::internal(format!("counter {key} below {n}")))
    }

    /// Creates `counter`'s row at 1 unless it exists: for counters added
    /// after a catalog was migrated (the DDL path's `sprig.next_group_id`
    /// and `sprig.next_shard_id`), so older catalogs need no migration.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn ensure_counter<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        counter: Counter,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} ({SET_COLS}) VALUES ('tenant', 0, ?, '1', NULL, 'system', \
                 UTC_TIMESTAMP(6), 0) ON DUPLICATE KEY UPDATE setting_key = setting_key",
                self.settings
            ),
            (counter.key(),),
        )
        .await
        .map_err(map_err)
    }

    /// The next database id.
    ///
    /// # Errors
    /// As [`next_ids`](Self::next_ids); `Internal` past `u32::MAX`.
    pub async fn next_db_id<Q: Queryable + Send>(&self, q: &mut Q) -> Result<DbId> {
        let id = self.next_ids(q, Counter::Database, 1).await?;
        u32::try_from(id)
            .map(DbId)
            .map_err(|_| SprigError::internal("database ids exhausted"))
    }

    /// The next object id.
    ///
    /// # Errors
    /// As [`next_ids`](Self::next_ids).
    pub async fn next_object_id<Q: Queryable + Send>(&self, q: &mut Q) -> Result<ObjectId> {
        Ok(ObjectId(self.next_ids(q, Counter::Object, 1).await?))
    }

    // ---- databases ----

    /// Inserts a database. A live name already in use is MariaDB's 1062.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn insert_database<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        r: &DatabaseRow,
    ) -> Result<()> {
        let params: Vec<Value> = vec![
            r.db_id.0.into(),
            r.name.as_str().into(),
            r.name_key.as_str().into(),
            r.default_charset.as_str().into(),
            r.default_collation.as_str().into(),
            r.default_group_id.0.into(),
            r.state.as_str().into(),
            json_text(&r.options)?.into(),
            to_datetime(r.created_at),
            r.created_by.map(|u| u.0).to_value(),
            r.changed_version.into(),
        ];
        q.exec_drop(
            format!(
                "INSERT INTO {} ({DB_COLS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                self.databases
            ),
            Params::Positional(params),
        )
        .await
        .map_err(map_err)
    }

    /// Updates every column of a database but its id and creation fields.
    /// Returns whether the row exists.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn update_database<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        r: &DatabaseRow,
    ) -> Result<bool> {
        let params: Vec<Value> = vec![
            r.name.as_str().into(),
            r.name_key.as_str().into(),
            r.default_charset.as_str().into(),
            r.default_collation.as_str().into(),
            r.default_group_id.0.into(),
            r.state.as_str().into(),
            json_text(&r.options)?.into(),
            r.changed_version.into(),
            r.db_id.0.into(),
        ];
        self.exists_after(
            q,
            format!(
                "UPDATE {} SET name = ?, name_key = ?, default_charset = ?, default_collation = ?, \
                 default_group_id = ?, state = ?, options = ?, changed_version = ? WHERE db_id = ?",
                self.databases
            ),
            params,
            format!("SELECT 1 FROM {} WHERE db_id = ?", self.databases),
            vec![r.db_id.0.into()],
        )
        .await
    }

    /// Moves a database to `state` (a drop: `Dropped`, which frees the
    /// name). Returns whether the row exists.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn set_database_state<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        db: DbId,
        state: DbState,
        version: u64,
    ) -> Result<bool> {
        self.exists_after(
            q,
            format!(
                "UPDATE {} SET state = ?, changed_version = ? WHERE db_id = ?",
                self.databases
            ),
            vec![state.as_str().into(), version.into(), db.0.into()],
            format!("SELECT 1 FROM {} WHERE db_id = ?", self.databases),
            vec![db.0.into()],
        )
        .await
    }

    /// One database by id, tombstones included.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a bad row.
    pub async fn database<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        db: DbId,
    ) -> Result<Option<DatabaseRow>> {
        let row: Option<Row> = q
            .exec_first(
                format!("SELECT {DB_COLS} FROM {} WHERE db_id = ?", self.databases),
                (db.0,),
            )
            .await
            .map_err(map_err)?;
        row.map(database_row).transpose()
    }

    /// The live (not dropped) database with this name key.
    ///
    /// # Errors
    /// As [`database`](Self::database).
    pub async fn database_by_name<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        name_key: &str,
    ) -> Result<Option<DatabaseRow>> {
        let row: Option<Row> = q
            .exec_first(
                format!(
                    "SELECT {DB_COLS} FROM {} WHERE live_name = ?",
                    self.databases
                ),
                (name_key,),
            )
            .await
            .map_err(map_err)?;
        row.map(database_row).transpose()
    }

    /// Databases changed after `since` (every database for `None`), by id.
    ///
    /// # Errors
    /// As [`database`](Self::database).
    pub async fn databases_since<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        since: Option<u64>,
    ) -> Result<Vec<DatabaseRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {DB_COLS} FROM {} WHERE changed_version > ? ORDER BY db_id",
                    self.databases
                ),
                (since_param(since),),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(database_row).collect()
    }

    // ---- objects ----

    /// Inserts an object. A live name already in use in its database and
    /// namespace is MariaDB's 1062.
    ///
    /// # Errors
    /// `Internal` if `namespace` doesn't match `kind`; 39070 or a backend
    /// error.
    pub async fn insert_object<Q: Queryable + Send>(&self, q: &mut Q, r: &ObjectRow) -> Result<()> {
        check_namespace(r)?;
        let params: Vec<Value> = vec![
            r.object_id.0.into(),
            r.db_id.0.into(),
            r.namespace.as_str().into(),
            r.name.as_str().into(),
            r.name_key.as_str().into(),
            r.kind.as_str().into(),
            r.parent_id.map(|p| p.0).to_value(),
            r.state.as_str().into(),
            r.schema_version.into(),
            r.flags.to_sql().into(),
            r.pending_job_id.map(|j| j.to_bytes().to_vec()).to_value(),
            to_datetime(r.created_at),
            r.changed_version.into(),
        ];
        q.exec_drop(
            format!(
                "INSERT INTO {} ({OBJ_COLS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                self.objects
            ),
            Params::Positional(params),
        )
        .await
        .map_err(map_err)
    }

    /// Updates every column of an object but its id and creation time
    /// (renames, state, schema version, flags, pending job). Returns
    /// whether the row exists.
    ///
    /// # Errors
    /// As [`insert_object`](Self::insert_object).
    pub async fn update_object<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        r: &ObjectRow,
    ) -> Result<bool> {
        check_namespace(r)?;
        let params: Vec<Value> = vec![
            r.db_id.0.into(),
            r.namespace.as_str().into(),
            r.name.as_str().into(),
            r.name_key.as_str().into(),
            r.kind.as_str().into(),
            r.parent_id.map(|p| p.0).to_value(),
            r.state.as_str().into(),
            r.schema_version.into(),
            r.flags.to_sql().into(),
            r.pending_job_id.map(|j| j.to_bytes().to_vec()).to_value(),
            r.changed_version.into(),
            r.object_id.0.into(),
        ];
        self.exists_after(
            q,
            format!(
                "UPDATE {} SET db_id = ?, namespace = ?, name = ?, name_key = ?, kind = ?, \
                 parent_id = ?, state = ?, schema_version = ?, flags = ?, pending_job_id = ?, \
                 changed_version = ? WHERE object_id = ?",
                self.objects
            ),
            params,
            format!("SELECT 1 FROM {} WHERE object_id = ?", self.objects),
            vec![r.object_id.0.into()],
        )
        .await
    }

    /// Moves an object to `state` (a drop: `Dropped`). Returns whether the
    /// row exists.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn set_object_state<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        state: ObjectState,
        version: u64,
    ) -> Result<bool> {
        self.exists_after(
            q,
            format!(
                "UPDATE {} SET state = ?, changed_version = ? WHERE object_id = ?",
                self.objects
            ),
            vec![state.as_str().into(), version.into(), object.0.into()],
            format!("SELECT 1 FROM {} WHERE object_id = ?", self.objects),
            vec![object.0.into()],
        )
        .await
    }

    /// One object by id, tombstones included.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a bad row.
    pub async fn object<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
    ) -> Result<Option<ObjectRow>> {
        let row: Option<Row> = q
            .exec_first(
                format!(
                    "SELECT {OBJ_COLS} FROM {} WHERE object_id = ?",
                    self.objects
                ),
                (object.0,),
            )
            .await
            .map_err(map_err)?;
        row.map(object_row).transpose()
    }

    /// The live object with this name key in `db` and `namespace`.
    ///
    /// # Errors
    /// As [`object`](Self::object).
    pub async fn object_by_name<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        db: DbId,
        namespace: Namespace,
        name_key: &str,
    ) -> Result<Option<ObjectRow>> {
        let row: Option<Row> = q
            .exec_first(
                format!(
                    "SELECT {OBJ_COLS} FROM {} WHERE db_id = ? AND namespace = ? AND live_name = ?",
                    self.objects
                ),
                (db.0, namespace.as_str(), name_key),
            )
            .await
            .map_err(map_err)?;
        row.map(object_row).transpose()
    }

    /// Objects changed after `since` (every object for `None`), by id.
    ///
    /// # Errors
    /// As [`object`](Self::object).
    pub async fn objects_since<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        since: Option<u64>,
    ) -> Result<Vec<ObjectRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {OBJ_COLS} FROM {} WHERE changed_version > ? ORDER BY object_id",
                    self.objects
                ),
                (since_param(since),),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(object_row).collect()
    }

    /// Every object of `db`, tombstones included, by id.
    ///
    /// # Errors
    /// As [`object`](Self::object).
    pub async fn objects_in<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        db: DbId,
    ) -> Result<Vec<ObjectRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {OBJ_COLS} FROM {} WHERE db_id = ? ORDER BY object_id",
                    self.objects
                ),
                (db.0,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(object_row).collect()
    }

    /// Deletes an object row that was never visible (a `creating` row
    /// whose DDL is rolled back), with its settings rows and schema
    /// versions. Returns whether it existed.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn delete_object<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
    ) -> Result<bool> {
        exec_count(
            q,
            format!(
                "DELETE FROM {} WHERE scope_kind = 'object' AND scope_id = ?",
                self.settings
            ),
            (object.0,),
        )
        .await?;
        exec_count(
            q,
            format!("DELETE FROM {} WHERE object_id = ?", self.schema_versions),
            (object.0,),
        )
        .await?;
        let n = exec_count(
            q,
            format!("DELETE FROM {} WHERE object_id = ?", self.objects),
            (object.0,),
        )
        .await?;
        Ok(n > 0)
    }

    /// Deletes a database row that was never visible (a `creating` row
    /// whose DDL is rolled back), with its settings rows. Returns whether
    /// it existed.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn delete_database<Q: Queryable + Send>(&self, q: &mut Q, db: DbId) -> Result<bool> {
        exec_count(
            q,
            format!(
                "DELETE FROM {} WHERE scope_kind = 'database' AND scope_id = ?",
                self.settings
            ),
            (db.0,),
        )
        .await?;
        let n = exec_count(
            q,
            format!("DELETE FROM {} WHERE db_id = ?", self.databases),
            (db.0,),
        )
        .await?;
        Ok(n > 0)
    }

    // ---- settings ----

    /// Deletes an internal (`sprig.`) key at tenant scope (transient
    /// records such as a DDL job's intent). Returns whether it existed.
    ///
    /// # Errors
    /// `Internal` unless `key` starts with `sprig.`; 39070 or a backend
    /// error.
    pub async fn delete_internal<Q: Queryable + Send>(&self, q: &mut Q, key: &str) -> Result<bool> {
        if !key.starts_with("sprig.") {
            return Err(SprigError::internal(format!(
                "{key} is not an internal key"
            )));
        }
        let n = exec_count(
            q,
            format!(
                "DELETE FROM {} WHERE scope_kind = 'tenant' AND scope_id = 0 AND setting_key = ?",
                self.settings
            ),
            (key,),
        )
        .await?;
        Ok(n > 0)
    }

    /// One settings row.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a bad row.
    pub async fn setting<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        scope: SettingScope,
        key: &str,
    ) -> Result<Option<SettingRow>> {
        let (kind, id) = scope.to_sql();
        let row: Option<Row> = q
            .exec_first(
                format!(
                    "SELECT {SET_COLS} FROM {} WHERE scope_kind = ? AND scope_id = ? AND setting_key = ?",
                    self.settings
                ),
                (kind, id, key),
            )
            .await
            .map_err(map_err)?;
        row.map(setting_row).transpose()
    }

    /// Inserts or replaces a settings row.
    ///
    /// # Errors
    /// `Internal` if `set_by` is longer than 288 characters; 39070 or a
    /// backend error.
    pub async fn put_setting<Q: Queryable + Send>(&self, q: &mut Q, r: &SettingRow) -> Result<()> {
        if r.set_by.chars().count() > 288 {
            return Err(SprigError::internal("set_by is longer than 288 characters"));
        }
        let (kind, id) = r.scope.to_sql();
        let bounds = r
            .bounds
            .as_ref()
            .map(|b| {
                serde_json::to_string(b)
                    .map_err(|e| SprigError::internal(format!("encoding bounds: {e}")))
            })
            .transpose()?;
        let value = r.value.as_ref().map(json_text).transpose()?;
        let params: Vec<Value> = vec![
            kind.into(),
            id.into(),
            r.key.as_str().into(),
            value.to_value(),
            bounds.to_value(),
            r.set_by.as_str().into(),
            to_datetime(r.set_at),
            r.version.into(),
        ];
        q.exec_drop(
            format!(
                "INSERT INTO {} ({SET_COLS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE \
                 value = VALUES(value), bounds = VALUES(bounds), set_by = VALUES(set_by), \
                 set_at = VALUES(set_at), version = VALUES(version)",
                self.settings
            ),
            Params::Positional(params),
        )
        .await
        .map_err(map_err)
    }

    /// Resets a row: clears its value (`value`), its bounds (`bounds`), or
    /// both. A row left empty stays as a tombstone with `version`, so
    /// incremental loads see the reset; [`purge`](Self::purge) removes it.
    /// Returns whether the row existed.
    ///
    /// # Errors
    /// 39070 or a backend error.
    #[allow(clippy::too_many_arguments)] // one row's address and its change
    pub async fn reset_setting<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        scope: SettingScope,
        key: &str,
        value: bool,
        bounds: bool,
        set_by: &str,
        at: SystemTime,
        version: u64,
    ) -> Result<bool> {
        let (kind, id) = scope.to_sql();
        let mut sets = Vec::new();
        if value {
            sets.push("value = NULL");
        }
        if bounds {
            sets.push("bounds = NULL");
        }
        sets.push("set_by = ?, set_at = ?, version = ?");
        let params: Vec<Value> = vec![
            set_by.into(),
            to_datetime(at),
            version.into(),
            kind.into(),
            id.into(),
            key.into(),
        ];
        let where_params: Vec<Value> = vec![kind.into(), id.into(), key.into()];
        self.exists_after(
            q,
            format!(
                "UPDATE {} SET {} WHERE scope_kind = ? AND scope_id = ? AND setting_key = ?",
                self.settings,
                sets.join(", ")
            ),
            params,
            format!(
                "SELECT 1 FROM {} WHERE scope_kind = ? AND scope_id = ? AND setting_key = ?",
                self.settings
            ),
            where_params,
        )
        .await
    }

    /// Every row of one scope, by key.
    ///
    /// # Errors
    /// As [`setting`](Self::setting).
    pub async fn settings_of<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        scope: SettingScope,
    ) -> Result<Vec<SettingRow>> {
        let (kind, id) = scope.to_sql();
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {SET_COLS} FROM {} WHERE scope_kind = ? AND scope_id = ? ORDER BY setting_key",
                    self.settings
                ),
                (kind, id),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(setting_row).collect()
    }

    /// Settings rows changed after `since` (every row for `None`).
    ///
    /// # Errors
    /// As [`setting`](Self::setting).
    pub async fn settings_since<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        since: Option<u64>,
    ) -> Result<Vec<SettingRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {SET_COLS} FROM {} WHERE version > ? \
                     ORDER BY scope_kind, scope_id, setting_key",
                    self.settings
                ),
                (since_param(since),),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(setting_row).collect()
    }

    /// Every row of `key`, at any scope (empty rows included), ordered by
    /// scope. The key isn't indexed alone: this scans the tenant's
    /// `settings` table, which `catalog.max_setting_rows` keeps small.
    ///
    /// # Errors
    /// As [`setting`](Self::setting).
    pub async fn settings_for_key<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        key: &str,
    ) -> Result<Vec<SettingRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {SET_COLS} FROM {} WHERE setting_key = ? ORDER BY scope_kind, scope_id",
                    self.settings
                ),
                (key,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(setting_row).collect()
    }

    // ---- schema_versions ----

    /// Inserts a schema version.
    ///
    /// # Errors
    /// 39070 or a backend error (1062 if the version exists).
    pub async fn insert_schema_version<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        r: &SchemaVersionRow,
    ) -> Result<()> {
        let params: Vec<Value> = vec![
            r.object_id.0.into(),
            r.version.into(),
            r.ddl.as_str().into(),
            r.create_sql.as_deref().to_value(),
            r.create_sql_hash.map(|h| h.to_vec()).to_value(),
            r.ddl_job_id.map(|j| j.to_bytes().to_vec()).to_value(),
            r.applied_by.map(|u| u.0).to_value(),
            to_datetime(r.applied_at),
            r.changed_version.into(),
        ];
        q.exec_drop(
            format!(
                "INSERT INTO {} ({SV_COLS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                self.schema_versions
            ),
            Params::Positional(params),
        )
        .await
        .map_err(map_err)
    }

    /// Every schema version of an object, oldest first. Read on demand,
    /// never by the cache: `create_sql` can be large.
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a bad row.
    pub async fn schema_versions<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
    ) -> Result<Vec<SchemaVersionRow>> {
        let rows: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {SV_COLS} FROM {} WHERE object_id = ? ORDER BY version",
                    self.schema_versions
                ),
                (object.0,),
            )
            .await
            .map_err(map_err)?;
        rows.into_iter().map(schema_version_row).collect()
    }

    /// The newest schema version of an object.
    ///
    /// # Errors
    /// As [`schema_versions`](Self::schema_versions).
    pub async fn latest_schema_version<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
    ) -> Result<Option<SchemaVersionRow>> {
        let row: Option<Row> = q
            .exec_first(
                format!(
                    "SELECT {SV_COLS} FROM {} WHERE object_id = ? ORDER BY version DESC LIMIT 1",
                    self.schema_versions
                ),
                (object.0,),
            )
            .await
            .map_err(map_err)?;
        row.map(schema_version_row).transpose()
    }

    // ---- tombstones ----

    /// Removes tombstones changed at or before `up_to`: dropped databases
    /// and objects with their settings rows and schema versions, and empty
    /// settings rows. Then raises `sprig.purged_version` to `up_to`, so a
    /// cache older than that reloads in full instead of missing the
    /// deletions. Run it inside a transaction holding the catalog version
    /// row ([`Versioner::purge`](crate::version::Versioner::purge)), with
    /// `up_to` no newer than the committed catalog version.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn purge<Q: Queryable + Send>(&self, q: &mut Q, up_to: u64) -> Result<PurgeReport> {
        let mut r = PurgeReport::default();
        r.settings += exec_count(
            q,
            format!(
                "DELETE FROM {} WHERE scope_kind = 'object' AND scope_id IN \
                 (SELECT object_id FROM {} WHERE state = 'dropped' AND changed_version <= ?)",
                self.settings, self.objects
            ),
            (up_to,),
        )
        .await?;
        r.schema_versions = exec_count(
            q,
            format!(
                "DELETE FROM {} WHERE object_id IN \
                 (SELECT object_id FROM {} WHERE state = 'dropped' AND changed_version <= ?)",
                self.schema_versions, self.objects
            ),
            (up_to,),
        )
        .await?;
        r.objects = exec_count(
            q,
            format!(
                "DELETE FROM {} WHERE state = 'dropped' AND changed_version <= ?",
                self.objects
            ),
            (up_to,),
        )
        .await?;
        r.settings += exec_count(
            q,
            format!(
                "DELETE FROM {} WHERE scope_kind = 'database' AND scope_id IN \
                 (SELECT db_id FROM {} WHERE state = 'dropped' AND changed_version <= ?)",
                self.settings, self.databases
            ),
            (up_to,),
        )
        .await?;
        r.databases = exec_count(
            q,
            format!(
                "DELETE FROM {} WHERE state = 'dropped' AND changed_version <= ?",
                self.databases
            ),
            (up_to,),
        )
        .await?;
        r.settings += exec_count(
            q,
            format!(
                "DELETE FROM {} WHERE value IS NULL AND bounds IS NULL AND version <= ? \
                 AND setting_key NOT LIKE 'sprig.%'",
                self.settings
            ),
            (up_to,),
        )
        .await?;
        exec_count(
            q,
            format!(
                "UPDATE {} SET value = GREATEST(CAST(value AS UNSIGNED), ?), set_at = UTC_TIMESTAMP(6) \
                 WHERE scope_kind = 'tenant' AND scope_id = 0 AND setting_key = 'sprig.purged_version'",
                self.settings
            ),
            (up_to,),
        )
        .await?;
        Ok(r)
    }

    // ---- snapshots ----

    /// Reads the versions and every row changed after `since` (every row
    /// for `None`) in one consistent, read-only snapshot, so a reader never
    /// sees part of a change. An incremental request from before
    /// `sprig.purged_version` becomes a full read ([`Snapshot::since`]).
    /// Schema versions are not included.
    ///
    /// # Errors
    /// 39070 or a backend error; 39072 if the catalog isn't migrated.
    pub async fn snapshot(&self, pool: &Pool, since: Option<u64>) -> Result<Snapshot> {
        let mut tx = self.begin_snapshot(pool).await?;
        let snap = self.snapshot_in(&mut tx, since).await?;
        tx.commit().await.map_err(map_err)?;
        Ok(snap)
    }

    /// Starts the consistent, read-only transaction a snapshot reads in
    /// (`START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY` at
    /// `REPEATABLE READ`), so the cache loader can read extension modules'
    /// tables in the same snapshot as the core rows.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn begin_snapshot(&self, pool: &Pool) -> Result<Transaction<'static>> {
        let mut opts = TxOpts::default();
        opts.with_consistent_snapshot(true)
            .with_isolation_level(mysql_async::IsolationLevel::RepeatableRead)
            .with_readonly(true);
        pool.start_transaction(opts).await.map_err(map_err)
    }

    /// [`snapshot`](Self::snapshot)'s reads, inside a transaction from
    /// [`begin_snapshot`](Self::begin_snapshot).
    ///
    /// # Errors
    /// As [`snapshot`](Self::snapshot).
    pub async fn snapshot_in<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        since: Option<u64>,
    ) -> Result<Snapshot> {
        let versions = self.versions(q).await?;
        let since = since.filter(|&s| s >= versions.purged);
        let databases = self.databases_since(q, since).await?;
        let objects = self.objects_since(q, since).await?;
        let settings = self.settings_since(q, since).await?;
        Ok(Snapshot {
            versions,
            since,
            databases,
            objects,
            settings,
        })
    }

    /// Runs `update`, and if it changed nothing, `probe` to tell an
    /// unchanged row from a missing one (affected rows don't count
    /// matched-but-unchanged rows).
    async fn exists_after<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        update: String,
        params: Vec<Value>,
        probe: String,
        probe_params: Vec<Value>,
    ) -> Result<bool> {
        if exec_count(q, update, Params::Positional(params)).await? > 0 {
            return Ok(true);
        }
        let found: Option<i64> = q
            .exec_first(probe, Params::Positional(probe_params))
            .await
            .map_err(map_err)?;
        Ok(found.is_some())
    }
}

/// Where a tenant's catalog lives: a pool to its cluster and the store.
#[derive(Clone)]
pub struct CatalogEndpoint {
    /// Connections to the catalog's cluster (writer or replica).
    pub pool: Pool,
    /// The catalog schema.
    pub store: Store,
}

impl fmt::Debug for CatalogEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogEndpoint")
            .field("schema", &self.store.schema())
            .finish_non_exhaustive()
    }
}

/// Finds a tenant's catalog. In production the catalog is shard 0 of
/// table group 0 in the tenant's home DC (chapter 08, "Where the catalog
/// lives"), resolved through the shard map; tests and single-cluster
/// development use [`StaticLocator`].
#[async_trait::async_trait]
pub trait CatalogLocator: Send + Sync + 'static {
    /// The catalog's writer.
    async fn writer(&self, tenant: TenantId) -> Result<CatalogEndpoint>;
    /// A node to load caches from: a healthy local replica, else the
    /// writer.
    async fn reader(&self, tenant: TenantId) -> Result<CatalogEndpoint> {
        self.writer(tenant).await
    }
    /// The mover job holding the catalog's shard map for a cutover, if one
    /// does (chapter 10, "Cutover"): catalog migrations wait for it, as
    /// the cutover may rename the catalog's tables away. The default knows
    /// of no holds.
    async fn held_by(&self, _tenant: TenantId) -> Result<Option<String>> {
        Ok(None)
    }
}

/// A fixed map from tenants to catalogs.
#[derive(Debug, Clone, Default)]
pub struct StaticLocator {
    map: HashMap<TenantId, CatalogEndpoint>,
}

impl StaticLocator {
    /// An empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `tenant`'s catalog.
    #[must_use]
    pub fn with(mut self, tenant: TenantId, endpoint: CatalogEndpoint) -> Self {
        self.map.insert(tenant, endpoint);
        self
    }

    /// As a shared locator.
    #[must_use]
    pub fn shared(self) -> Arc<dyn CatalogLocator> {
        Arc::new(self)
    }
}

#[async_trait::async_trait]
impl CatalogLocator for StaticLocator {
    async fn writer(&self, tenant: TenantId) -> Result<CatalogEndpoint> {
        self.map.get(&tenant).cloned().ok_or_else(|| {
            SprigError::new(
                SprigCode::TenantNotReady,
                format!("no catalog for tenant {tenant}"),
            )
        })
    }
}

// ---- row decoding ----

fn database_row(mut row: Row) -> Result<DatabaseRow> {
    let r = &mut row;
    Ok(DatabaseRow {
        db_id: DbId(col(r, 0, "db_id")?),
        name: col(r, 1, "name")?,
        name_key: col(r, 2, "name_key")?,
        default_charset: col(r, 3, "default_charset")?,
        default_collation: col(r, 4, "default_collation")?,
        default_group_id: GroupId(col(r, 5, "default_group_id")?),
        state: DbState::parse(&col::<String>(r, 6, "state")?)?,
        options: json_col(r, 7, "options")?.unwrap_or(Json::Null),
        created_at: time_col(r, 8, "created_at")?,
        created_by: col::<Option<u32>>(r, 9, "created_by")?.map(UserId),
        changed_version: col(r, 10, "changed_version")?,
    })
}

fn object_row(mut row: Row) -> Result<ObjectRow> {
    let r = &mut row;
    Ok(ObjectRow {
        object_id: ObjectId(col(r, 0, "object_id")?),
        db_id: DbId(col(r, 1, "db_id")?),
        namespace: Namespace::parse(&col::<String>(r, 2, "namespace")?)?,
        name: col(r, 3, "name")?,
        name_key: col(r, 4, "name_key")?,
        kind: ObjectKind::parse(&col::<String>(r, 5, "kind")?)?,
        parent_id: col::<Option<u64>>(r, 6, "parent_id")?.map(ObjectId),
        state: ObjectState::parse(&col::<String>(r, 7, "state")?)?,
        schema_version: col(r, 8, "schema_version")?,
        flags: ObjectFlags::parse(&col::<String>(r, 9, "flags")?)?,
        pending_job_id: bytes_col::<16>(r, 10, "pending_job_id")?
            .map(|b| JobId(uuid::Uuid::from_bytes(b))),
        created_at: time_col(r, 11, "created_at")?,
        changed_version: col(r, 12, "changed_version")?,
    })
}

fn setting_row(mut row: Row) -> Result<SettingRow> {
    let r = &mut row;
    let kind: String = col(r, 0, "scope_kind")?;
    let id: u64 = col(r, 1, "scope_id")?;
    let key: String = col(r, 2, "setting_key")?;
    let value = json_col(r, 3, "value")?;
    let bounds = json_col(r, 4, "bounds")?
        .map(serde_json::from_value::<Bounds>)
        .transpose()
        .map_err(|e| SprigError::internal(format!("setting {key}: bad bounds: {e}")))?;
    Ok(SettingRow {
        scope: SettingScope::parse(&kind, id)?,
        value,
        bounds,
        set_by: col(r, 5, "set_by")?,
        set_at: time_col(r, 6, "set_at")?,
        version: col(r, 7, "version")?,
        key,
    })
}

fn schema_version_row(mut row: Row) -> Result<SchemaVersionRow> {
    let r = &mut row;
    Ok(SchemaVersionRow {
        object_id: ObjectId(col(r, 0, "object_id")?),
        version: col(r, 1, "version")?,
        ddl: col(r, 2, "ddl")?,
        create_sql: col(r, 3, "create_sql")?,
        create_sql_hash: bytes_col::<32>(r, 4, "create_sql_hash")?,
        ddl_job_id: bytes_col::<16>(r, 5, "ddl_job_id")?.map(|b| JobId(uuid::Uuid::from_bytes(b))),
        applied_by: col::<Option<u32>>(r, 6, "applied_by")?.map(UserId),
        applied_at: time_col(r, 7, "applied_at")?,
        changed_version: col(r, 8, "changed_version")?,
    })
}

// ---- helpers ----

fn check_namespace(r: &ObjectRow) -> Result<()> {
    if r.kind.namespace() == r.namespace {
        Ok(())
    } else {
        Err(SprigError::internal(format!(
            "object kind {} lives in namespace {}, not {}",
            r.kind,
            r.kind.namespace(),
            r.namespace
        )))
    }
}

fn json_text(v: &Json) -> Result<String> {
    serde_json::to_string(v).map_err(|e| SprigError::internal(format!("encoding JSON: {e}")))
}

fn parse_u64(key: &str, raw: &[u8]) -> Result<u64> {
    serde_json::from_slice(raw)
        .map_err(|e| SprigError::internal(format!("{key} is not a version number: {e}")))
}

/// `since` as a `> ?` parameter: everything for `None`. Versions start at
/// 1, so `> 0` matches every row written by a change; unversioned rows of a
/// fresh catalog (version 0) are included by `-1`.
fn since_param(since: Option<u64>) -> Value {
    match since {
        Some(s) => Value::UInt(s),
        None => Value::Int(-1),
    }
}

fn not_ready() -> SprigError {
    SprigError::new(
        SprigCode::TenantNotReady,
        "the tenant's catalog is not bootstrapped",
    )
}

async fn exec_count<Q: Queryable + Send, P: Into<Params> + Send>(
    q: &mut Q,
    stmt: String,
    params: P,
) -> Result<u64> {
    let result = q.exec_iter(stmt, params).await.map_err(map_err)?;
    let n = result.affected_rows();
    result.drop_result().await.map_err(map_err)?;
    Ok(n)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn enums_round_trip() {
        for s in DbState::ALL {
            assert_eq!(DbState::parse(s.as_str()).unwrap(), *s);
        }
        for s in ObjectState::ALL {
            assert_eq!(ObjectState::parse(s.as_str()).unwrap(), *s);
        }
        for n in Namespace::ALL {
            assert_eq!(Namespace::parse(n.as_str()).unwrap(), *n);
        }
        for k in ObjectKind::ALL {
            assert_eq!(ObjectKind::parse(k.as_str()).unwrap(), *k);
        }
        assert!(ObjectKind::parse("index").is_err());
        assert_eq!(ObjectKind::LogicFunction.namespace(), Namespace::Function);
        assert_eq!(ObjectKind::LogicSchedule.namespace(), Namespace::Logic);
        assert_eq!(ObjectKind::KvNamespace.namespace(), Namespace::Relation);
    }

    #[test]
    fn flags_round_trip() {
        let f = ObjectFlags::REDIS | ObjectFlags::GIPK_ADDED;
        assert_eq!(f.to_sql(), "gipk_added,redis");
        assert_eq!(ObjectFlags::parse("gipk_added,redis").unwrap(), f);
        assert_eq!(ObjectFlags::parse("").unwrap(), ObjectFlags::empty());
        assert!(f.contains(ObjectFlags::REDIS));
        assert!(!f.contains(ObjectFlags::RLS));
        assert!(ObjectFlags::parse("sharded").is_err());
    }

    #[test]
    fn scopes_round_trip() {
        for s in [
            SettingScope::Tenant,
            SettingScope::Database(DbId(3)),
            SettingScope::Object(ObjectId(u64::MAX)),
            SettingScope::Ancestor(TenantId(7)),
        ] {
            let (k, id) = s.to_sql();
            assert_eq!(SettingScope::parse(k, id).unwrap(), s);
        }
        assert!(SettingScope::parse("database", u64::MAX).is_err());
        assert!(SettingScope::parse("cluster", 1).is_err());
        assert_eq!(Counter::Other("user").key(), "sprig.next_user_id");
    }

    #[test]
    fn setting_entries_need_scalars() {
        let mut r = SettingRow {
            scope: SettingScope::Tenant,
            key: "auto_index.max_per_table".to_owned(),
            value: Some(serde_json::json!(6)),
            bounds: Some(Bounds::max(5.0)),
            set_by: "user:7".to_owned(),
            set_at: SystemTime::UNIX_EPOCH,
            version: 3,
        };
        let e = r.entry().unwrap();
        assert_eq!(e.value, Some(sprig_settings::Value::Int(6)));
        assert_eq!(e.bounds.unwrap().max, Some(5.0));
        r.value = Some(serde_json::json!({"a": 1}));
        assert!(r.entry().is_err());
        r.value = None;
        r.bounds = None;
        assert!(r.is_empty());
        assert!(!r.is_internal());
    }

    #[test]
    fn stores_quote_their_schema() {
        let s = Store::for_tenant(TenantId(42));
        assert_eq!(s.schema(), "t42_sprig");
        assert_eq!(s.databases, "`t42_sprig`.`databases`");
    }
}
