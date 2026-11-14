//! Search indexes in the tenant catalog: the `search` catalog module.
//!
//! - **Tables.** `search_indexes`, `search_fields` and `search_analyzers`
//!   in `t<tenant>_sprig`, created by the module's migration the first
//!   time a gateway needs them for a tenant.
//! - **Objects.** A search index is an object of kind `search_index`
//!   whose parent is the indexed table or collection, so its
//!   id is an `objects.object_id`.
//! - **Definitions.** `search_indexes.definition` holds the normalized
//!   definition ([`IndexDef`] as JSON, with what was given); one
//!   `search_fields` row per field mirrors it for `SHOW SEARCH INDEXES`
//!   and `information_schema` readers.
//! - **Changes.** Creating or dropping an index or analyzer is one
//!   versioned catalog change, so every gateway's snapshot reloads it.
//!   Indexes whose object was dropped are left out of the snapshot.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::SystemTime;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::ddl::{LOWER_CASE_TABLE_NAMES, database_key, object_key};
use sprig_catalog::migrate::{MigrateOpts, Migration, Migrator, Module, Phase};
use sprig_catalog::store::{Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, CatalogLocator as _, Freshness, TenantCatalog};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_search::analyze::CustomAnalyzer;
use sprig_search::def::{Flavor, IndexDef};

use crate::tenant::TenantServices;

/// The module name.
pub const MODULE: &str = "search";

const DDL_INDEXES: &str = "CREATE TABLE IF NOT EXISTS search_indexes (
  index_id         BIGINT UNSIGNED NOT NULL,
  db_id            INT UNSIGNED NOT NULL,
  name             VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
  object_id        BIGINT UNSIGNED NOT NULL,
  model            ENUM('table','collection','kv') NOT NULL,
  flavor           ENUM('sql','mongo_text','mongo_search','fulltext_compat') NOT NULL,
  default_analyzer VARCHAR(64) NOT NULL,
  dynamic          TINYINT(1) NOT NULL DEFAULT 0,
  definition       JSON NOT NULL,
  generation       INT UNSIGNED NOT NULL,
  origin           ENUM('user','migrated','auto') NOT NULL,
  auto_index_id    BIGINT UNSIGNED NULL,
  state            ENUM('building','ready','rebuilding','failed','dropping') NOT NULL,
  created_at       DATETIME(3) NOT NULL,
  updated_at       DATETIME(3) NOT NULL,
  PRIMARY KEY (index_id),
  UNIQUE KEY uk_search_name (db_id, object_id, name),
  KEY by_object (object_id)
) ENGINE=InnoDB";

const DDL_FIELDS: &str = "CREATE TABLE IF NOT EXISTS search_fields (
  index_id         BIGINT UNSIGNED NOT NULL,
  field_no         SMALLINT UNSIGNED NOT NULL,
  name             VARCHAR(255) NOT NULL,
  source           ENUM('column','path','kv_value','dynamic') NOT NULL,
  source_ref       VARCHAR(512) NOT NULL,
  kind             ENUM('text','keyword','number','date','bool','autocomplete',
                        'substring','facet') NOT NULL,
  analyzer         VARCHAR(64) NULL,
  query_analyzer   VARCHAR(64) NULL,
  boost            DOUBLE NOT NULL DEFAULT 1,
  is_filter        TINYINT(1) NOT NULL DEFAULT 0,
  is_sort          TINYINT(1) NOT NULL DEFAULT 0,
  is_facet         TINYINT(1) NOT NULL DEFAULT 0,
  added_generation INT UNSIGNED NOT NULL,
  PRIMARY KEY (index_id, field_no),
  UNIQUE KEY uk_field (index_id, name, kind)
) ENGINE=InnoDB";

const DDL_ANALYZERS: &str = "CREATE TABLE IF NOT EXISTS search_analyzers (
  analyzer_id      INT UNSIGNED NOT NULL,
  name             VARCHAR(64) NOT NULL,
  tokenizer        VARCHAR(32) NOT NULL,
  tokenizer_opts   JSON NOT NULL,
  filters          JSON NOT NULL,
  synonyms         JSON NULL,
  version          INT UNSIGNED NOT NULL,
  created_at       DATETIME(3) NOT NULL,
  updated_at       DATETIME(3) NOT NULL,
  PRIMARY KEY (analyzer_id),
  UNIQUE KEY uk_analyzer_name (name)
) ENGINE=InnoDB";

/// The module's migrations.
pub static SEARCH_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "search_indexes, search_fields, search_analyzers",
    statements: &[DDL_INDEXES, DDL_FIELDS, DDL_ANALYZERS],
}];

fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let mut state = *b"HY000";
            if s.state.len() == 5 {
                state.copy_from_slice(s.state.as_bytes());
            }
            SprigError::backend(
                s.code,
                state,
                format!(
                    "search catalog statement failed ({}): {}",
                    s.code, s.message
                ),
            )
        }
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable"),
    }
    .with_source(e)
}

fn t(schema: &str, table: &str) -> String {
    format!("{}.{}", quote(schema), quote(table))
}

/// One tenant's search indexes and analyzers in a snapshot.
#[derive(Debug, Default)]
pub struct SearchState {
    /// Indexes by database id.
    pub by_db: HashMap<u32, Vec<Arc<IndexDef>>>,
    /// Custom analyzers.
    pub analyzers: Vec<CustomAnalyzer>,
}

impl ExtState for SearchState {
    fn bytes(&self) -> usize {
        self.by_db
            .values()
            .flatten()
            .map(|d| 512 + d.fields.len() * 128)
            .sum::<usize>()
            + self.analyzers.len() * 256
    }
}

impl SearchState {
    /// The indexes of database `db`.
    #[must_use]
    pub fn of_db(&self, db: u32) -> &[Arc<IndexDef>] {
        self.by_db.get(&db).map_or(&[], Vec::as_slice)
    }

    /// The indexes on `object` (by name) of database `db`.
    #[must_use]
    pub fn on_object(&self, db: u32, object: &str, lctn: bool) -> Vec<Arc<IndexDef>> {
        self.of_db(db)
            .iter()
            .filter(|d| {
                if lctn {
                    d.object.eq_ignore_ascii_case(object)
                } else {
                    d.object == object
                }
            })
            .cloned()
            .collect()
    }

    /// The index with id `index` (any database).
    #[must_use]
    pub fn by_id(&self, index: u64) -> Option<Arc<IndexDef>> {
        self.by_db
            .values()
            .flatten()
            .find(|d| d.index_id == index)
            .cloned()
    }

    /// Index `name` of database `db`.
    #[must_use]
    pub fn by_name(&self, db: u32, name: &str) -> Option<Arc<IndexDef>> {
        self.of_db(db)
            .iter()
            .find(|d| d.name.eq_ignore_ascii_case(name))
            .cloned()
    }
}

/// The module's rows in catalog schema `schema`.
#[derive(Debug, Clone)]
pub struct SearchTables {
    schema: String,
}

impl SearchTables {
    /// Over `t<tenant>_sprig`.
    #[must_use]
    pub fn new(schema: &str) -> Self {
        Self {
            schema: schema.to_owned(),
        }
    }

    /// Whether the module's tables exist.
    ///
    /// # Errors
    /// A backend error.
    pub async fn migrated<Q: Queryable + Send>(&self, q: &mut Q) -> Result<bool> {
        let n: Option<u64> = q
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'search_analyzers'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// Creates the module's tables (idempotent).
    ///
    /// # Errors
    /// A backend error.
    pub async fn migrate(&self, q: &mut mysql_async::Conn) -> Result<()> {
        Migrator::new(&[Module {
            name: MODULE,
            migrations: SEARCH_MIGRATIONS,
        }])?
        .run(q, &self.schema, &MigrateOpts::default())
        .await?;
        Ok(())
    }

    /// Every live index (its object not dropped) and every analyzer.
    ///
    /// # Errors
    /// A backend error.
    pub async fn load<Q: Queryable + Send>(&self, q: &mut Q) -> Result<SearchState> {
        let rows: Vec<(u64, u32, String)> = q
            .query(format!(
                "SELECT s.index_id, s.db_id, s.definition FROM {} s JOIN {} o \
                 ON o.object_id = s.object_id \
                 WHERE s.state <> 'dropping' AND o.state NOT IN ('dropping', 'dropped') \
                 ORDER BY s.index_id",
                t(&self.schema, "search_indexes"),
                t(&self.schema, "objects")
            ))
            .await
            .map_err(map_err)?;
        let mut st = SearchState::default();
        for (id, db, json) in rows {
            match serde_json::from_str::<IndexDef>(&json) {
                Ok(mut d) => {
                    d.index_id = id;
                    st.by_db.entry(db).or_default().push(Arc::new(d));
                }
                Err(e) => {
                    tracing::warn!(index = id, error = %e, "unreadable search index definition; skipped");
                }
            }
        }
        let an: Vec<(String, String, String, String, Option<String>, u32)> = q
            .query(format!(
                "SELECT name, tokenizer, tokenizer_opts, filters, synonyms, version FROM {} ORDER BY analyzer_id",
                t(&self.schema, "search_analyzers")
            ))
            .await
            .map_err(map_err)?;
        for (name, tokenizer, opts, filters, syn, version) in an {
            st.analyzers.push(CustomAnalyzer {
                name,
                tokenizer,
                tokenizer_opts: serde_json::from_str(&opts).unwrap_or_default(),
                filters: serde_json::from_str(&filters).unwrap_or_default(),
                synonyms: syn
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default(),
                version,
            });
        }
        Ok(st)
    }

    async fn insert_index<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        db: u32,
        d: &IndexDef,
        origin: &str,
    ) -> Result<()> {
        let json = serde_json::to_string(d).map_err(|e| SprigError::internal(e.to_string()))?;
        q.exec_drop(
            format!(
                "INSERT INTO {} (index_id, db_id, name, object_id, model, flavor, default_analyzer, \
                 dynamic, definition, generation, origin, state, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
                t(&self.schema, "search_indexes")
            ),
            (
                d.index_id,
                db,
                d.name.as_str(),
                d.object_id,
                d.model.as_str(),
                d.flavor.as_str(),
                d.default_analyzer.as_str(),
                u8::from(d.dynamic),
                json,
                d.generation,
                origin,
            ),
        )
        .await
        .map_err(map_err)?;
        for f in &d.fields {
            q.exec_drop(
                format!(
                    "INSERT INTO {} (index_id, field_no, name, source, source_ref, kind, analyzer, \
                     query_analyzer, boost, is_filter, is_sort, is_facet, added_generation) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    t(&self.schema, "search_fields")
                ),
                mysql_async::Params::Positional(vec![
                    d.index_id.into(),
                    f.field_no.into(),
                    f.name.as_str().into(),
                    serde_json::to_value(f.source)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_owned))
                        .unwrap_or_default()
                        .into(),
                    f.source_ref.as_str().into(),
                    f.kind.as_str().into(),
                    f.analyzer.clone().into(),
                    f.query_analyzer.clone().into(),
                    f.boost.into(),
                    u8::from(f.filter).into(),
                    u8::from(f.sort).into(),
                    u8::from(f.facet).into(),
                    f.added_generation.max(1).into(),
                ]),
            )
            .await
            .map_err(map_err)?;
        }
        Ok(())
    }

    async fn delete_index<Q: Queryable + Send>(&self, q: &mut Q, id: u64) -> Result<()> {
        for tb in ["search_fields", "search_indexes"] {
            q.exec_drop(
                format!("DELETE FROM {} WHERE index_id = ?", t(&self.schema, tb)),
                (id,),
            )
            .await
            .map_err(map_err)?;
        }
        Ok(())
    }
}

/// The catalog extension (loads [`SearchState`] into every snapshot).
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchExtension;

#[async_trait]
impl CatalogExtension for SearchExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        SEARCH_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        let d = SearchTables::new(tx.schema());
        let conn = tx.conn();
        if !d.migrated(conn).await? {
            return Ok(Arc::new(SearchState::default()));
        }
        Ok(Arc::new(d.load(conn).await?))
    }
}

/// Reads and changes the module through the tenant services.
pub struct SearchCatalog {
    svc: Arc<TenantServices>,
    migrated: Mutex<std::collections::HashSet<u64>>,
}

impl std::fmt::Debug for SearchCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchCatalog").finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An empty state for snapshots loaded before the module existed.
static EMPTY: std::sync::LazyLock<SearchState> = std::sync::LazyLock::new(SearchState::default);

/// The module's state in a snapshot (empty when the tenant has none).
#[must_use]
pub fn state(cat: &TenantCatalog) -> &SearchState {
    cat.ext.get::<SearchState>(MODULE).unwrap_or(&EMPTY)
}

impl SearchCatalog {
    /// Over the tenant services.
    #[must_use]
    pub fn new(svc: Arc<TenantServices>) -> Self {
        Self {
            svc,
            migrated: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// The tenant services.
    #[must_use]
    pub fn services(&self) -> &Arc<TenantServices> {
        &self.svc
    }

    async fn prepare(&self, tenant: TenantId) -> Result<()> {
        if lock(&self.migrated).contains(&tenant.0) {
            return Ok(());
        }
        let ep = self.svc.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        SearchTables::new(ep.store.schema())
            .migrate(&mut conn)
            .await?;
        lock(&self.migrated).insert(tenant.0);
        Ok(())
    }

    /// Whether the tenant's names are case-insensitive.
    #[must_use]
    pub fn lctn(&self, cat: &TenantCatalog) -> bool {
        self.svc.settings.get(
            &LOWER_CASE_TABLE_NAMES,
            &cat.setting_ctx(&self.svc.dc),
            Some(&cat.settings),
        ) == 1
    }

    /// The newest snapshot.
    ///
    /// # Errors
    /// The cache's.
    pub async fn snapshot(&self, tenant: TenantId, min_version: u64) -> Result<Arc<TenantCatalog>> {
        let cat = self.svc.cache.get(tenant, Freshness::Latest).await?;
        if cat.version >= min_version {
            return Ok(cat);
        }
        self.svc
            .cache
            .get(tenant, Freshness::Cached { min_version })
            .await
    }

    /// Creates index `d` on the object `object_id` of database `db_id`;
    /// fills in its id. Returns the committed version, or `None` when an
    /// index by that name exists (and `if_not_exists`).
    ///
    /// # Errors
    /// 1061 (`ER_DUP_KEYNAME`) for an existing name; 39830 over
    /// `max_per_object`; catalog errors.
    pub async fn create(
        &self,
        tenant: TenantId,
        db_id: DbId,
        d: &mut IndexDef,
        if_not_exists: bool,
        max_per_object: usize,
        origin: &str,
    ) -> Result<Option<u64>> {
        self.prepare(tenant).await?;
        let cat = self.snapshot(tenant, 0).await?;
        let lctn = self.lctn(&cat);
        let ep = self.svc.locator.writer(tenant).await?;
        let object_id = {
            let mut c = ep.pool.get_conn().await.map_err(map_err)?;
            ep.store.next_object_id(&mut c).await?
        };
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let version = ch.version();
        let r: Result<bool> = async {
            let (store, tx) = ch.tx().parts();
            let tb = SearchTables::new(store.schema());
            // Indexes whose object was dropped go first (their names can be
            // used again).
            let orphans: Vec<u64> = tx
                .exec(
                    format!(
                        "SELECT s.index_id FROM {} s LEFT JOIN {} o ON o.object_id = s.object_id \
                         WHERE s.db_id = ? AND (o.object_id IS NULL OR o.state IN ('dropping', 'dropped'))",
                        t(store.schema(), "search_indexes"),
                        t(store.schema(), "objects")
                    ),
                    (db_id.0,),
                )
                .await
                .map_err(map_err)?;
            for id in orphans {
                tb.delete_index(tx, id).await?;
                store.delete_object(tx, ObjectId(id)).await?;
            }
            let existing: Vec<(u64, String, u64)> = tx
                .exec(
                    format!(
                        "SELECT index_id, name, object_id FROM {} WHERE db_id = ? FOR UPDATE",
                        t(store.schema(), "search_indexes")
                    ),
                    (db_id.0,),
                )
                .await
                .map_err(map_err)?;
            // SQL names are unique per database; MongoDB's per collection.
            let per_object = matches!(d.flavor, Flavor::MongoText | Flavor::MongoSearch);
            if existing.iter().any(|(_, n, o)| {
                n.eq_ignore_ascii_case(&d.name) && (!per_object || *o == d.object_id)
            }) {
                if if_not_exists {
                    return Ok(false);
                }
                return Err(SprigError::standard(
                    1061,
                    *b"42000",
                    format!("Duplicate key name '{}'", d.name),
                ));
            }
            if existing.iter().filter(|(_, _, o)| *o == d.object_id).count() >= max_per_object {
                return Err(SprigError::new(
                    SprigCode::SearchIndexLimit,
                    format!(
                        "'{}' already has {max_per_object} search indexes (search.max_indexes_per_object)",
                        d.object
                    ),
                ));
            }
            d.index_id = object_id.0;
            let obj = ObjectRow {
                object_id,
                db_id,
                namespace: Namespace::SearchIndex,
                name: d.name.clone(),
                name_key: if matches!(d.flavor, Flavor::MongoText | Flavor::MongoSearch) {
                    object_key(
                        ObjectKind::SearchIndex,
                        &format!("{}${}", d.object_id, d.name),
                        lctn,
                    )
                } else {
                    object_key(ObjectKind::SearchIndex, &d.name, lctn)
                },
                kind: ObjectKind::SearchIndex,
                parent_id: Some(ObjectId(d.object_id)),
                state: ObjectState::Active,
                schema_version: 1,
                flags: ObjectFlags::default(),
                pending_job_id: None,
                created_at: SystemTime::now(),
                changed_version: version,
            };
            store.insert_object(tx, &obj).await?;
            tb.insert_index(tx, db_id.0, d, origin).await?;
            Ok(true)
        }
        .await;
        match r {
            Ok(true) => Ok(Some(ch.commit().await?)),
            Ok(false) => {
                let _ = ch.rollback().await;
                Ok(None)
            }
            Err(e) => {
                let _ = ch.rollback().await;
                Err(e)
            }
        }
    }

    /// Replaces index `d`'s definition (a new generation, or a renamed
    /// object). Returns the committed version.
    ///
    /// # Errors
    /// Catalog errors.
    pub async fn replace(&self, tenant: TenantId, db_id: DbId, d: &IndexDef) -> Result<u64> {
        self.prepare(tenant).await?;
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let r: Result<()> = async {
            let (store, tx) = ch.tx().parts();
            let tb = SearchTables::new(store.schema());
            tb.delete_index(tx, d.index_id).await?;
            tb.insert_index(tx, db_id.0, d, "user").await
        }
        .await;
        match r {
            Ok(()) => ch.commit().await,
            Err(e) => {
                let _ = ch.rollback().await;
                Err(e)
            }
        }
    }

    /// Drops index `id`. Returns the committed version.
    ///
    /// # Errors
    /// Catalog errors.
    pub async fn drop_index(&self, tenant: TenantId, id: u64) -> Result<u64> {
        self.prepare(tenant).await?;
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let r: Result<()> = async {
            let (store, tx) = ch.tx().parts();
            SearchTables::new(store.schema())
                .delete_index(tx, id)
                .await?;
            store.delete_object(tx, ObjectId(id)).await?;
            Ok(())
        }
        .await;
        match r {
            Ok(()) => ch.commit().await,
            Err(e) => {
                let _ = ch.rollback().await;
                Err(e)
            }
        }
    }

    /// Creates or drops a custom analyzer. Returns the committed version,
    /// or `None` when nothing changed (`IF [NOT] EXISTS`).
    ///
    /// # Errors
    /// 39828 / 1061 for a missing or duplicate name; catalog errors.
    pub async fn analyzer(
        &self,
        tenant: TenantId,
        create: Option<&CustomAnalyzer>,
        drop_name: Option<&str>,
        lenient: bool,
    ) -> Result<Option<u64>> {
        self.prepare(tenant).await?;
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let r: Result<bool> = async {
            let (store, tx) = ch.tx().parts();
            let tbl = t(store.schema(), "search_analyzers");
            if let Some(a) = create {
                let exists: Option<u32> = tx
                    .exec_first(
                        format!("SELECT analyzer_id FROM {tbl} WHERE name = ? FOR UPDATE"),
                        (a.name.as_str(),),
                    )
                    .await
                    .map_err(map_err)?;
                if exists.is_some() {
                    if lenient {
                        return Ok(false);
                    }
                    return Err(SprigError::standard(
                        1061,
                        *b"42000",
                        format!("search analyzer '{}' already exists", a.name),
                    ));
                }
                let next: Option<u32> = tx
                    .query_first(format!("SELECT COALESCE(MAX(analyzer_id), 0) + 1 FROM {tbl}"))
                    .await
                    .map_err(map_err)?;
                tx.exec_drop(
                    format!(
                        "INSERT INTO {tbl} (analyzer_id, name, tokenizer, tokenizer_opts, filters, synonyms, \
                         version, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))"
                    ),
                    (
                        next.unwrap_or(1),
                        a.name.as_str(),
                        a.tokenizer.as_str(),
                        a.tokenizer_opts.to_string(),
                        serde_json::to_string(&a.filters).unwrap_or_default(),
                        serde_json::to_string(&a.synonyms).unwrap_or_default(),
                        a.version,
                    ),
                )
                .await
                .map_err(map_err)?;
                return Ok(true);
            }
            if let Some(n) = drop_name {
                let used: Option<String> = tx
                    .exec_first(
                        format!(
                            "SELECT i.name FROM {} f JOIN {} i ON i.index_id = f.index_id \
                             WHERE f.analyzer = ? OR f.query_analyzer = ? OR i.default_analyzer = ? LIMIT 1",
                            t(store.schema(), "search_fields"),
                            t(store.schema(), "search_indexes")
                        ),
                        (n, n, n),
                    )
                    .await
                    .map_err(map_err)?;
                if let Some(i) = used {
                    return Err(SprigError::new(
                        SprigCode::SearchFieldType,
                        format!("search analyzer '{n}' is used by search index '{i}'"),
                    ));
                }
                tx.exec_drop(format!("DELETE FROM {tbl} WHERE name = ?"), (n,))
                    .await
                    .map_err(map_err)?;
                let gone: Option<u64> = tx.query_first("SELECT ROW_COUNT()").await.map_err(map_err)?;
                if gone.unwrap_or(0) == 0 {
                    if lenient {
                        return Ok(false);
                    }
                    return Err(SprigError::new(
                        SprigCode::SearchAnalyzerUnknown,
                        format!("search analyzer '{n}' doesn't exist"),
                    ));
                }
                return Ok(true);
            }
            Ok(false)
        }
        .await;
        match r {
            Ok(true) => Ok(Some(ch.commit().await?)),
            Ok(false) => {
                let _ = ch.rollback().await;
                Ok(None)
            }
            Err(e) => {
                let _ = ch.rollback().await;
                Err(e)
            }
        }
    }

    /// Database `db`'s row id, by name.
    #[must_use]
    pub fn db_id(&self, cat: &TenantCatalog, db: &str) -> Option<DbId> {
        cat.database(&database_key(db, self.lctn(cat)))
            .map(|r| r.db_id)
    }

    /// The table `name` of database `db_id`: its object id.
    #[must_use]
    pub fn table_id(&self, cat: &TenantCatalog, db_id: DbId, name: &str) -> Option<u64> {
        cat.object(
            db_id,
            Namespace::Relation,
            &object_key(ObjectKind::Table, name, self.lctn(cat)),
        )
        .map(|o| o.object_id.0)
    }
}
