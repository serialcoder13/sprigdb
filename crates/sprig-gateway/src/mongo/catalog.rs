//! Collections in the tenant catalog: the `doc` catalog module and the
//! document engine's [`DocCatalog`] over it.
//!
//! - **Tables.** `collections`, `doc_indexes` and `promoted_fields` in
//!   `t<tenant>_sprig`, created by the module's migration the first time a
//!   gateway needs them for a tenant. A collection's id is the
//!   `objects.object_id` of its table (an ordinary `table` row, created by
//!   the engine's `CREATE TABLE` through the user's session).
//! - **Changes.** Registering or removing a collection, its indexes, and a
//!   new array path are each one versioned catalog change, so every
//!   gateway's cache reloads them.
//! - **Reads.** Commands read collections from the gateway's cached
//!   snapshot ([`DocState`]): no metadata read per command. The snapshot
//!   is the newest the directory watch has delivered, and never older
//!   than this gateway's own last change.
//! - **Old data.** A database written before the catalog tables existed
//!   keeps its collections in `__sprig_doc_meta`. The first command on
//!   that database in each gateway process imports its rows into the
//!   catalog and drops the table.
//!
//! The publication wait of `check_flags` (every gateway acknowledging a
//! new array path before the write commits) is in [`super::publish`].

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bson::Document;
use mysql_async::Row;
use mysql_async::prelude::Queryable;
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::ddl::{LOWER_CASE_TABLE_NAMES, database_key, object_key};
use sprig_catalog::migrate::{MigrateOpts, Migration, Migrator, Module, Phase};
use sprig_catalog::store::{Namespace, ObjectKind};
use sprig_catalog::version::{ChangeOpts, VersionKind};
use sprig_catalog::{CatalogExtension, CatalogLocator, Freshness, TenantCatalog};
use sprig_common::ids::TenantId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_doc::catalog::{DocCatalog, TableCatalog};
use sprig_doc::engine::{Ns, SqlRunner};
use sprig_doc::meta::{self, CollMeta, IndexDef, META_TABLE};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::tenant::TenantServices;

/// The module name.
pub const MODULE: &str = "doc";

const DDL_COLLECTIONS: &str = "CREATE TABLE IF NOT EXISTS collections (
  collection_id     BIGINT UNSIGNED NOT NULL,
  db_id             INT UNSIGNED NOT NULL,
  name              VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
  table_name        VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
  group_id          INT UNSIGNED NOT NULL DEFAULT 0,
  dist_key          JSON NOT NULL,
  coll_uuid         BINARY(16) NOT NULL,
  type_id           BIGINT UNSIGNED NULL,
  validator         LONGBLOB NULL,
  validation_level  ENUM('off','strict','moderate') NOT NULL DEFAULT 'strict',
  validation_action ENUM('error','warn') NOT NULL DEFAULT 'error',
  validator_where   ENUM('none','check','gateway') NOT NULL DEFAULT 'none',
  collation         JSON NULL,
  pre_post_images   TINYINT(1) NOT NULL DEFAULT 0,
  array_paths       JSON NOT NULL,
  array_paths_full  TINYINT(1) NOT NULL DEFAULT 0,
  view_on           VARCHAR(255) NULL,
  view_pipeline     LONGBLOB NULL,
  options           LONGBLOB NULL,
  next_index        INT UNSIGNED NOT NULL DEFAULT 1,
  state             ENUM('creating','ready','dropping') NOT NULL,
  created_at        DATETIME(3) NOT NULL,
  PRIMARY KEY (collection_id),
  UNIQUE KEY uk_coll_name (db_id, name),
  KEY k_coll_table (db_id, table_name)
) ENGINE=InnoDB";

const DDL_DOC_INDEXES: &str = "CREATE TABLE IF NOT EXISTS doc_indexes (
  collection_id   BIGINT UNSIGNED NOT NULL,
  index_id        INT UNSIGNED NOT NULL,
  name            VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
  key_pattern     LONGBLOB NOT NULL,
  impl            ENUM('primary','column','hybrid','side','wildcard','s2') NOT NULL,
  mariadb_name    VARCHAR(64) NULL,
  field_columns   JSON NOT NULL,
  is_unique       TINYINT(1) NOT NULL DEFAULT 0,
  is_sparse       TINYINT(1) NOT NULL DEFAULT 0,
  partial_filter  LONGBLOB NULL,
  collation       JSON NULL,
  expire_seconds  BIGINT NULL,
  hidden          TINYINT(1) NOT NULL DEFAULT 0,
  multikey_paths  JSON NOT NULL,
  global_lookup   BIGINT UNSIGNED NULL,
  origin          ENUM('system','user','auto','migrated') NOT NULL,
  auto_index_id   BIGINT UNSIGNED NULL,
  options         LONGBLOB NULL,
  state           ENUM('building','ready','rebuilding','failed','dropping') NOT NULL,
  created_at      DATETIME(3) NOT NULL,
  PRIMARY KEY (collection_id, index_id),
  UNIQUE KEY uk_index_name (collection_id, name)
) ENGINE=InnoDB";

const DDL_PROMOTED_FIELDS: &str = "CREATE TABLE IF NOT EXISTS promoted_fields (
  collection_id   BIGINT UNSIGNED NOT NULL,
  path_hash       BIGINT UNSIGNED NOT NULL,
  class           ENUM('integer','number','string','objectid','date','bool',
                       'uuid','pair') NOT NULL,
  path            VARCHAR(1024) NOT NULL,
  slug            VARCHAR(40) NOT NULL,
  sql_type        VARCHAR(64) NOT NULL,
  collation       VARCHAR(64) NULL,
  mixed_seen      TINYINT(1) NOT NULL DEFAULT 0,
  off_column_seen TINYINT(1) NOT NULL DEFAULT 0,
  origin          ENUM('user','auto','migrated') NOT NULL,
  auto_index_id   BIGINT UNSIGNED NULL,
  state           ENUM('adding','ready','dropping') NOT NULL,
  created_at      DATETIME(3) NOT NULL,
  PRIMARY KEY (collection_id, path_hash, class),
  UNIQUE KEY uk_field_slug (collection_id, slug)
) ENGINE=InnoDB";

/// Format 2: array paths registered but not yet confirmed by every
/// gateway (the publication wait).
const DDL_PENDING: &str =
    "ALTER TABLE collections ADD COLUMN IF NOT EXISTS array_paths_pending JSON NULL";

/// The module's migrations.
pub static DOC_MIGRATIONS: &[Migration] = &[
    Migration {
        format: 1,
        phase: Phase::Expand,
        name: "collections, doc_indexes, promoted_fields",
        statements: &[DDL_COLLECTIONS, DDL_DOC_INDEXES, DDL_PROMOTED_FIELDS],
    },
    Migration {
        format: 2,
        phase: Phase::Expand,
        name: "collections.array_paths_pending",
        statements: &[DDL_PENDING],
    },
];

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
                    "collection catalog statement failed ({}): {}",
                    s.code, s.message
                ),
            )
        }
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable"),
    }
    .with_source(e)
}

fn mongo(e: &SprigError) -> MongoError {
    let c = match e.kind {
        sprig_common::ErrorKind::Sprig(c) => i32::from(c.number()),
        _ => code::INTERNAL_ERROR,
    };
    MongoError::new(c, e.message.clone())
}

/// One tenant's collections in a snapshot: by database id, then name.
#[derive(Debug, Default)]
pub struct DocState {
    by_db: HashMap<u32, BTreeMap<String, Arc<CollMeta>>>,
}

impl ExtState for DocState {
    fn bytes(&self) -> usize {
        self.by_db
            .values()
            .flat_map(BTreeMap::values)
            .map(|m| 256 + m.array_paths.len() * 32 + m.indexes.len() * 128)
            .sum()
    }
}

impl DocState {
    /// Collection `name` of database `db`.
    #[must_use]
    pub fn get(&self, db: u32, name: &str) -> Option<&Arc<CollMeta>> {
        self.by_db.get(&db)?.get(name)
    }

    /// The collections of `db`, by name.
    pub fn of(&self, db: u32) -> impl Iterator<Item = &Arc<CollMeta>> {
        self.by_db.get(&db).into_iter().flat_map(BTreeMap::values)
    }
}

fn t(schema: &str, table: &str) -> String {
    format!("{}.{table}", quote(schema))
}

fn bson_doc(b: Option<Vec<u8>>) -> Document {
    b.filter(|b| !b.is_empty())
        .and_then(|b| bson::from_slice::<Document>(&b).ok())
        .unwrap_or_default()
}

fn bson_bytes(d: &Document) -> Vec<u8> {
    bson::to_vec(d).unwrap_or_default()
}

fn paths_of(json: &str) -> BTreeSet<String> {
    serde_json::from_str::<Vec<String>>(json)
        .map(|v| v.into_iter().collect())
        .unwrap_or_default()
}

fn paths_json(p: &BTreeSet<String>) -> String {
    serde_json::Value::from(p.iter().cloned().collect::<Vec<_>>()).to_string()
}

/// A `doc_indexes` row as read.
type IndexRow = (
    u64,
    u32,
    String,
    Vec<u8>,
    i8,
    i8,
    Option<i64>,
    String,
    Option<Vec<u8>>,
    Option<String>,
    Option<u64>,
);

/// The `doc_indexes` columns [`index_of`] reads, in order.
const INDEX_COLS: &str = "collection_id, index_id, name, key_pattern, is_unique, is_sparse, \
     expire_seconds, origin, options, mariadb_name, auto_index_id";

fn index_of(r: Row) -> Option<(u64, IndexDef)> {
    let (cid, id, name, key, uniq, sparse, expire, origin, opts, mariadb, auto_id): IndexRow =
        mysql_async::from_row_opt(r).ok()?;
    // `sprig_dix_<id>` is implied; only another name is kept.
    let mariadb = mariadb.filter(|m| *m != format!("sprig_dix_{id}"));
    let k = bson::from_slice::<Document>(&key).ok()?;
    let key = k
        .iter()
        .map(|(p, v)| {
            let d = sprig_doc::filter::as_i64(v).unwrap_or(1);
            (p.clone(), if d < 0 { -1 } else { 1 })
        })
        .collect();
    Some((
        cid,
        IndexDef {
            id,
            name,
            key,
            unique: uniq != 0,
            sparse: sparse != 0,
            expire_after: expire,
            origin,
            extra: bson_doc(opts),
            mariadb,
            auto_index_id: auto_id,
        },
    ))
}

const COLL_COLS: &str = "c.collection_id, c.db_id, c.name, c.table_name, c.coll_uuid, \
     CAST(c.array_paths AS CHAR), c.array_paths_full, c.options, c.next_index, \
     CAST(c.array_paths_pending AS CHAR)";

/// [`COLL_COLS`] of a catalog still at format 1 (no pending paths).
const COLL_COLS_V1: &str = "c.collection_id, c.db_id, c.name, c.table_name, c.coll_uuid, \
     CAST(c.array_paths AS CHAR), c.array_paths_full, c.options, c.next_index, NULL";

type CollRow = (
    u64,
    u32,
    String,
    String,
    Vec<u8>,
    String,
    i8,
    Option<Vec<u8>>,
    u32,
    Option<String>,
);

fn coll_of(r: CollRow) -> (u64, u32, CollMeta) {
    let (cid, db, name, table, uuid_b, paths, full, opts, next, pending) = r;
    let mut uuid = [0u8; 16];
    if uuid_b.len() == 16 {
        uuid.copy_from_slice(&uuid_b);
    }
    (
        cid,
        db,
        CollMeta {
            name,
            table,
            uuid,
            array_paths: paths_of(&paths),
            arrays_full: full != 0,
            pending_paths: pending.as_deref().map(paths_of).unwrap_or_default(),
            indexes: Vec::new(),
            options: bson_doc(opts),
            next_index: next.max(1),
        },
    )
}

/// The module's rows, read and written in catalog schema `schema`.
#[derive(Debug, Clone)]
pub struct DocTables {
    schema: String,
}

impl DocTables {
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
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'promoted_fields'",
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
            migrations: DOC_MIGRATIONS,
        }])?
        .run(q, &self.schema, &MigrateOpts::default())
        .await?;
        Ok(())
    }

    /// Every live collection: a collection whose table was dropped (also
    /// by SQL) is left out.
    ///
    /// # Errors
    /// A backend error.
    pub async fn load<Q: Queryable + Send>(&self, q: &mut Q) -> Result<DocState> {
        let select = |cols: &str| {
            format!(
                "SELECT {cols} FROM {} c JOIN {} o ON o.object_id = c.collection_id \
                 WHERE c.state = 'ready' AND o.state NOT IN ('dropping', 'dropped')",
                t(&self.schema, "collections"),
                t(&self.schema, "objects")
            )
        };
        let rows: Vec<CollRow> = match q.query(select(COLL_COLS)).await {
            Ok(r) => r,
            // A catalog no gateway of this build has migrated yet.
            Err(mysql_async::Error::Server(e)) if e.code == 1054 => {
                q.query(select(COLL_COLS_V1)).await.map_err(map_err)?
            }
            Err(e) => return Err(map_err(e)),
        };
        let mut by_id: HashMap<u64, (u32, CollMeta)> = rows
            .into_iter()
            .map(|r| {
                let (cid, db, m) = coll_of(r);
                (cid, (db, m))
            })
            .collect();
        let idx: Vec<Row> = q
            .query(format!(
                "SELECT {INDEX_COLS} FROM {} WHERE state = 'ready' \
                 ORDER BY collection_id, index_id",
                t(&self.schema, "doc_indexes")
            ))
            .await
            .map_err(map_err)?;
        for r in idx {
            if let Some((cid, i)) = index_of(r)
                && let Some((_, m)) = by_id.get_mut(&cid)
            {
                m.indexes.push(i);
            }
        }
        let mut st = DocState::default();
        for (_, (db, m)) in by_id {
            st.by_db
                .entry(db)
                .or_default()
                .insert(m.name.clone(), Arc::new(m));
        }
        Ok(st)
    }

    /// The collection `(db, name)` and its id, locked.
    async fn find<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        db: u32,
        name: &str,
    ) -> Result<Option<(u64, CollMeta, bool)>> {
        let row: Option<Row> = q
            .exec_first(
                format!(
                    "SELECT {COLL_COLS}, o.state FROM {} c LEFT JOIN {} o ON o.object_id = c.collection_id \
                     WHERE c.db_id = ? AND c.name = ? FOR UPDATE",
                    t(&self.schema, "collections"),
                    t(&self.schema, "objects")
                ),
                (db, name),
            )
            .await
            .map_err(map_err)?;
        let Some(mut row) = row else { return Ok(None) };
        let state: Option<String> = row.take(10).flatten();
        let live = matches!(state.as_deref(), Some("active" | "altering" | "creating"));
        let r: CollRow = (
            row.take(0).unwrap_or(0),
            row.take(1).unwrap_or(0),
            row.take(2).unwrap_or_default(),
            row.take(3).unwrap_or_default(),
            row.take(4).unwrap_or_default(),
            row.take(5).unwrap_or_default(),
            row.take(6).unwrap_or(0),
            row.take(7).flatten(),
            row.take(8).unwrap_or(1),
            row.take(9).flatten(),
        );
        let (cid, _, mut m) = coll_of(r);
        let idx: Vec<Row> = q
            .exec(
                format!(
                    "SELECT {INDEX_COLS} FROM {} WHERE collection_id = ? \
                     ORDER BY index_id",
                    t(&self.schema, "doc_indexes")
                ),
                (cid,),
            )
            .await
            .map_err(map_err)?;
        m.indexes = idx.into_iter().filter_map(index_of).map(|x| x.1).collect();
        Ok(Some((cid, m, live)))
    }

    /// Deletes collection `cid`'s rows.
    async fn delete<Q: Queryable + Send>(&self, q: &mut Q, cid: u64) -> Result<()> {
        for tb in ["promoted_fields", "doc_indexes", "collections"] {
            q.exec_drop(
                format!(
                    "DELETE FROM {} WHERE collection_id = ?",
                    t(&self.schema, tb)
                ),
                (cid,),
            )
            .await
            .map_err(map_err)?;
        }
        Ok(())
    }

    /// Rewrites collection `cid`'s indexes and field-column rows.
    async fn write_indexes<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        cid: u64,
        m: &CollMeta,
    ) -> Result<()> {
        for tb in ["promoted_fields", "doc_indexes"] {
            q.exec_drop(
                format!(
                    "DELETE FROM {} WHERE collection_id = ?",
                    t(&self.schema, tb)
                ),
                (cid,),
            )
            .await
            .map_err(map_err)?;
        }
        let mut paths: BTreeMap<String, String> = BTreeMap::new();
        for i in &m.indexes {
            let slugs: Vec<String> = i.key.iter().map(|(p, _)| meta::slug(p)).collect();
            for (p, _) in &i.key {
                paths.entry(p.clone()).or_insert_with(|| i.origin.clone());
            }
            let origin = match i.origin.as_str() {
                "migrated" | "auto" | "system" => i.origin.as_str(),
                _ => "user",
            };
            q.exec_drop(
                format!(
                    "INSERT INTO {} (collection_id, index_id, name, key_pattern, impl, mariadb_name, \
                     field_columns, is_unique, is_sparse, expire_seconds, multikey_paths, origin, \
                     options, auto_index_id, state, created_at) VALUES (?, ?, ?, ?, 'column', ?, ?, ?, ?, ?, '[]', ?, ?, \
                     ?, 'ready', UTC_TIMESTAMP(3))",
                    t(&self.schema, "doc_indexes")
                ),
                mysql_async::Params::Positional(vec![
                    cid.into(),
                    i.id.into(),
                    i.name.clone().into(),
                    bson_bytes(&i.key_doc()).into(),
                    i.mariadb_name().into(),
                    serde_json::Value::from(slugs).to_string().into(),
                    i8::from(i.unique).into(),
                    i8::from(i.sparse).into(),
                    i.expire_after.into(),
                    origin.into(),
                    bson_bytes(&i.extra).into(),
                    i.auto_index_id.into(),
                ]),
            )
            .await
            .map_err(map_err)?;
        }
        for (p, origin) in paths {
            let origin = if matches!(origin.as_str(), "migrated" | "auto") {
                origin
            } else {
                "user".to_owned()
            };
            q.exec_drop(
                format!(
                    "INSERT INTO {} (collection_id, path_hash, class, path, slug, sql_type, origin, \
                     state, created_at) VALUES (?, ?, 'pair', ?, ?, ?, ?, 'ready', UTC_TIMESTAMP(3))",
                    t(&self.schema, "promoted_fields")
                ),
                (
                    cid,
                    meta::fnv64(p.as_bytes()),
                    p.as_str(),
                    meta::slug(&p),
                    format!("TINYINT,DOUBLE,VARCHAR({})", meta::S_CHARS),
                    origin,
                ),
            )
            .await
            .map_err(map_err)?;
        }
        q.exec_drop(
            format!(
                "UPDATE {} SET next_index = ?, options = ? WHERE collection_id = ?",
                t(&self.schema, "collections")
            ),
            (m.next_index, bson_bytes(&m.options), cid),
        )
        .await
        .map_err(map_err)
    }

    /// Inserts collection `cid` with all of `m`.
    async fn insert<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        cid: u64,
        db: u32,
        m: &CollMeta,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (collection_id, db_id, name, table_name, dist_key, coll_uuid, \
                 array_paths, array_paths_full, options, next_index, state, created_at) \
                 VALUES (?, ?, ?, ?, '{{\"fields\":[\"_id\"],\"source\":\"default\"}}', ?, ?, ?, ?, ?, \
                 'ready', UTC_TIMESTAMP(3))",
                t(&self.schema, "collections")
            ),
            mysql_async::Params::Positional(vec![
                cid.into(),
                db.into(),
                m.name.clone().into(),
                m.table.clone().into(),
                m.uuid.to_vec().into(),
                paths_json(&m.array_paths).into(),
                i8::from(m.arrays_full).into(),
                bson_bytes(&m.options).into(),
                m.next_index.into(),
            ]),
        )
        .await
        .map_err(map_err)?;
        self.write_indexes(q, cid, m).await
    }
}

/// The module in the catalog cache: [`DocState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct DocExtension;

#[async_trait]
impl CatalogExtension for DocExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        DOC_MIGRATIONS
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
        let d = DocTables::new(tx.schema());
        let conn = tx.conn();
        if !d.migrated(conn).await? {
            return Ok(Arc::new(DocState::default()));
        }
        Ok(Arc::new(d.load(conn).await?))
    }
}

/// The document engine's metadata in the tenant catalog.
pub struct CatalogDocs {
    svc: Arc<TenantServices>,
    /// Tenants whose module tables exist.
    migrated: Mutex<HashSet<u64>>,
    /// The newest catalog version this gateway committed, per tenant.
    seen: Mutex<HashMap<u64, u64>>,
    /// Databases checked for an old `__sprig_doc_meta`, by tenant.
    legacy: Mutex<HashMap<u64, HashSet<String>>>,
    /// The publication wait; `None` without a DC tier.
    publisher: Option<Arc<super::publish::Publisher>>,
}

impl std::fmt::Debug for CatalogDocs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogDocs").finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A [`CatalogDocs::stored_shapes`] row as read.
type StoredRow = (
    String,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<i64>,
    Option<i64>,
);

/// One MongoDB shape's totals in the workload store: every
/// gateway's flushed counts, kept for the store's retention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredShape {
    /// The fingerprint's text: `<command> <db>.<collection> <shape JSON>`.
    pub text: String,
    /// Runs.
    pub runs: u64,
    /// Total time, microseconds.
    pub total_us: u64,
    /// Longest run, microseconds.
    pub max_us: u64,
    /// Documents returned.
    pub returned: u64,
    /// Documents changed (writes).
    pub affected: u64,
    /// First and last seen, ms since the epoch.
    pub first_ms: i64,
    /// Last seen.
    pub last_ms: i64,
}

impl CatalogDocs {
    /// The tenant's MongoDB shapes in the workload store, with
    /// their totals over every row the store keeps; a tenant whose
    /// workload tables don't exist yet has none.
    ///
    /// # Errors
    /// The catalog's.
    pub async fn stored_shapes(&self, tenant: TenantId) -> Result<Vec<StoredShape>> {
        let ep = self.svc.locator.reader(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        let schema = ep.store.schema();
        let rows: std::result::Result<Vec<Row>, _> = conn
            .query(format!(
                "SELECT f.normalized_text, CAST(SUM(s.exec_count) AS UNSIGNED), \
                 CAST(SUM(s.total_us) AS UNSIGNED), MAX(s.max_us), \
                 CAST(SUM(s.rows_returned) AS UNSIGNED), CAST(SUM(s.rows_affected) AS UNSIGNED), \
                 CAST(UNIX_TIMESTAMP(MIN(f.first_seen)) * 1000 AS SIGNED), \
                 CAST(UNIX_TIMESTAMP(MAX(f.last_seen)) * 1000 AS SIGNED) \
                 FROM {} f JOIN {} s ON s.fingerprint_id = f.fingerprint_id \
                 WHERE f.kind = 'mongo' AND NOT FIND_IN_SET('truncated', f.flags) \
                 AND NOT FIND_IN_SET('other', f.flags) \
                 GROUP BY f.fingerprint_id, f.normalized_text",
                t(schema, "workload_fingerprints"),
                t(schema, "workload_stats")
            ))
            .await;
        let rows = match rows {
            Ok(r) => r,
            // No workload tables yet.
            Err(mysql_async::Error::Server(e)) if e.code == 1146 => return Ok(Vec::new()),
            Err(e) => return Err(map_err(e)),
        };
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let (text, runs, total_us, max_us, returned, affected, first_ms, last_ms): StoredRow =
                    mysql_async::from_row_opt(r).ok()?;
                Some(StoredShape {
                    text,
                    runs: runs.unwrap_or(0),
                    total_us: total_us.unwrap_or(0),
                    max_us: max_us.unwrap_or(0),
                    returned: returned.unwrap_or(0),
                    affected: affected.unwrap_or(0),
                    first_ms: first_ms.unwrap_or(0),
                    last_ms: last_ms.unwrap_or(0),
                })
            })
            .collect())
    }

    /// Over the tenant services.
    #[must_use]
    pub fn new(svc: Arc<TenantServices>) -> Self {
        Self {
            svc,
            migrated: Mutex::new(HashSet::new()),
            seen: Mutex::new(HashMap::new()),
            legacy: Mutex::new(HashMap::new()),
            publisher: None,
        }
    }

    /// With the publication wait of `check_flags`.
    #[must_use]
    pub fn with_publisher(mut self, p: Arc<super::publish::Publisher>) -> Self {
        self.publisher = Some(p);
        self
    }

    async fn prepare(&self, tenant: TenantId) -> Result<()> {
        if lock(&self.migrated).contains(&tenant.0) {
            return Ok(());
        }
        let ep = self.svc.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        DocTables::new(ep.store.schema()).migrate(&mut conn).await?;
        lock(&self.migrated).insert(tenant.0);
        Ok(())
    }

    /// The snapshot: the newest the directory watch delivered, and never
    /// older than this gateway's own last change.
    async fn snapshot(&self, tenant: TenantId) -> Result<Arc<TenantCatalog>> {
        let min = lock(&self.seen).get(&tenant.0).copied().unwrap_or(0);
        let cat = self.svc.cache.get(tenant, Freshness::Latest).await?;
        if cat.version >= min {
            return Ok(cat);
        }
        self.svc
            .cache
            .get(tenant, Freshness::Cached { min_version: min })
            .await
    }

    fn saw(&self, tenant: TenantId, v: u64) {
        let mut s = lock(&self.seen);
        let e = s.entry(tenant.0).or_insert(0);
        *e = (*e).max(v);
    }

    fn lctn(&self, cat: &TenantCatalog) -> bool {
        self.svc.settings.get(
            &LOWER_CASE_TABLE_NAMES,
            &cat.setting_ctx(&self.svc.dc),
            Some(&cat.settings),
        ) == 1
    }

    /// Imports `db`'s old `__sprig_doc_meta` rows once per process.
    async fn legacy(
        &self,
        sql: &mut dyn SqlRunner,
        tenant: TenantId,
        db: &str,
        cat: &TenantCatalog,
        db_id: sprig_common::ids::DbId,
    ) -> Result<bool> {
        if self.legacy_checked(tenant, db) {
            return Ok(false);
        }
        // Only a database the round-1 engine wrote has the table (an
        // object of the catalog): nothing to read for any other, and
        // users with grants on single collections never probe it.
        let lctn = self.lctn(cat);
        if cat
            .object(
                db_id,
                Namespace::Relation,
                &object_key(ObjectKind::Table, META_TABLE, lctn),
            )
            .is_none()
        {
            self.legacy_done(tenant, db);
            return Ok(false);
        }
        let rows = match TableCatalog::rows(sql, db).await {
            Ok(r) => r,
            // This user can't read it: a user who can imports it.
            Err(e) if e.code == code::UNAUTHORIZED => return Ok(false),
            Err(e) => return Err(SprigError::new(SprigCode::CatalogUnavailable, e.message)),
        };
        let mut imported = false;
        if !rows.is_empty() {
            for m in &rows {
                let ns = Ns {
                    db: db.to_owned(),
                    coll: m.name.clone(),
                };
                match self.register_in(tenant, &ns, m).await {
                    Ok(_) => imported = true,
                    // The table is gone: nothing to import.
                    Err(e) if e.kind == sprig_common::ErrorKind::Sprig(SprigCode::Internal) => {
                        tracing::warn!(%tenant, db, coll = m.name, error = %e, "old collection metadata names no table; skipped");
                    }
                    Err(e) => return Err(e),
                }
            }
            let drop = format!(
                "DROP TABLE IF EXISTS {}.{}",
                sprig_doc::sql::ident(db),
                sprig_doc::sql::ident(META_TABLE)
            );
            if let Err(e) = sql.exec(&drop).await {
                tracing::warn!(%tenant, db, error = %e.message, "dropping __sprig_doc_meta after its import failed");
            }
            tracing::info!(%tenant, db, collections = rows.len(), "collection metadata moved to the catalog");
        }
        self.legacy_done(tenant, db);
        Ok(imported)
    }

    /// `db` of `tenant` was checked for old metadata in this process.
    fn legacy_checked(&self, tenant: TenantId, db: &str) -> bool {
        lock(&self.legacy)
            .get(&tenant.0)
            .is_some_and(|s| s.contains(db))
    }

    fn legacy_done(&self, tenant: TenantId, db: &str) {
        lock(&self.legacy)
            .entry(tenant.0)
            .or_default()
            .insert(db.to_owned());
    }

    /// Registers `m` as `ns` in one change; the existing live collection
    /// if there is one.
    async fn register_in(
        &self,
        tenant: TenantId,
        ns: &Ns,
        m: &CollMeta,
    ) -> Result<Option<CollMeta>> {
        self.prepare(tenant).await?;
        let cat = self.snapshot(tenant).await?;
        let lctn = self.lctn(&cat);
        // The version row is locked first and a version is
        // allocated only when something is written, so finding the
        // collection already registered leaves no gap.
        let mut ch = self.svc.catalog.lock(tenant, VersionKind::Catalog).await?;
        let r: Result<Option<CollMeta>> = async {
            let (store, tx) = ch.tx().parts();
            let d = DocTables::new(store.schema());
            let db = store
                .database_by_name(tx, &database_key(&ns.db, lctn))
                .await?
                .ok_or_else(|| {
                    SprigError::new(SprigCode::Internal, format!("no database {}", ns.db))
                })?;
            let obj = store
                .object_by_name(
                    tx,
                    db.db_id,
                    Namespace::Relation,
                    &object_key(ObjectKind::Table, &m.table, lctn),
                )
                .await?
                .ok_or_else(|| {
                    SprigError::new(
                        SprigCode::Internal,
                        format!("collection {ns} has no table {}", m.table),
                    )
                })?;
            if let Some((cid, old, live)) = d.find(tx, db.db_id.0, &ns.coll).await? {
                if live && cid == obj.object_id.0 {
                    return Ok(Some(old));
                }
                // Left by a table dropped outside the engine.
                d.delete(tx, cid).await?;
            }
            d.insert(tx, obj.object_id.0, db.db_id.0, m).await?;
            Ok(None)
        }
        .await;
        match r {
            Ok(Some(x)) => {
                let _ = ch.release().await;
                Ok(Some(x))
            }
            Ok(None) => {
                let v = ch.allocate(ChangeOpts::default()).await?.commit().await?;
                self.saw(tenant, v);
                Ok(None)
            }
            Err(e) => {
                let _ = ch.release().await;
                Err(e)
            }
        }
    }

    /// Runs `f` on the locked collection `ns` in one change; the version
    /// committed, when `f` changed something.
    async fn update<F>(&self, tenant: TenantId, ns: &Ns, f: F) -> Result<Option<u64>>
    where
        F: for<'a> FnOnce(
                &'a DocTables,
                &'a mut mysql_async::Transaction<'static>,
                u64,
                CollMeta,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<bool>> + Send + 'a>,
            > + Send,
    {
        self.prepare(tenant).await?;
        let cat = self.snapshot(tenant).await?;
        let lctn = self.lctn(&cat);
        // Allocate only when `f` changed something.
        let mut ch = self.svc.catalog.lock(tenant, VersionKind::Catalog).await?;
        let r: Result<bool> = async {
            let (store, tx) = ch.tx().parts();
            let d = DocTables::new(store.schema());
            let Some(db) = store
                .database_by_name(tx, &database_key(&ns.db, lctn))
                .await?
            else {
                return Ok(false);
            };
            let Some((cid, m, _)) = d.find(tx, db.db_id.0, &ns.coll).await? else {
                return Ok(false);
            };
            f(&d, tx, cid, m).await
        }
        .await;
        match r {
            Ok(true) => {
                let v = ch.allocate(ChangeOpts::default()).await?.commit().await?;
                self.saw(tenant, v);
                Ok(Some(v))
            }
            Ok(false) => {
                let _ = ch.release().await;
                Ok(None)
            }
            Err(e) => {
                let _ = ch.release().await;
                Err(e)
            }
        }
    }

    /// Deletes the rows of collections whose table is gone (a dropped
    /// database, or a table dropped by SQL), in one change.
    async fn purge_orphans(&self, tenant: TenantId) -> Result<()> {
        self.prepare(tenant).await?;
        // After nearly every `dropDatabase` there is nothing to purge; the
        // version row is locked, and a version allocated only when rows go
        // (before, each call left a gap that held every `Freshness::Latest` load
        // of the tenant for `catalog.version_wait_ms`).
        let mut ch = self.svc.catalog.lock(tenant, VersionKind::Catalog).await?;
        let r: Result<u64> = async {
            let (store, tx) = ch.tx().parts();
            let s = store.schema().to_owned();
            let ids: Vec<u64> = tx
                .query(format!(
                    "SELECT c.collection_id FROM {} c LEFT JOIN {} o ON o.object_id = c.collection_id \
                     WHERE o.object_id IS NULL OR o.state = 'dropped'",
                    t(&s, "collections"),
                    t(&s, "objects")
                ))
                .await
                .map_err(map_err)?;
            let d = DocTables::new(&s);
            for id in &ids {
                d.delete(tx, *id).await?;
            }
            Ok(ids.len() as u64)
        }
        .await;
        match r {
            Ok(0) => {
                let _ = ch.release().await;
                Ok(())
            }
            Ok(_) => {
                let v = ch.allocate(ChangeOpts::default()).await?.commit().await?;
                self.saw(tenant, v);
                Ok(())
            }
            Err(e) => {
                let _ = ch.release().await;
                Err(e)
            }
        }
    }

    async fn state(
        &self,
        sql: &mut dyn SqlRunner,
        tenant: TenantId,
        db: &str,
    ) -> Result<Option<(u32, Arc<TenantCatalog>)>> {
        let mut cat = self.snapshot(tenant).await?;
        let lctn = self.lctn(&cat);
        let Some(row) = cat.database(&database_key(db, lctn)).cloned() else {
            return Ok(None);
        };
        if self.legacy(sql, tenant, db, &cat, row.db_id).await? {
            cat = self.snapshot(tenant).await?;
        }
        Ok(Some((row.db_id.0, cat)))
    }
}

fn tenant_of(sql: &dyn SqlRunner) -> Option<TenantId> {
    Some(sql.tenant()).filter(|t| *t != 0).map(TenantId)
}

#[async_trait]
impl DocCatalog for CatalogDocs {
    async fn get(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> std::result::Result<Option<CollMeta>, MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.get(sql, ns).await;
        };
        let Some((db, cat)) = self
            .state(sql, tenant, &ns.db)
            .await
            .map_err(|e| mongo(&e))?
        else {
            return Ok(None);
        };
        Ok(cat
            .ext
            .get::<DocState>(MODULE)
            .and_then(|s| s.get(db, &ns.coll))
            .map(|m| (**m).clone()))
    }

    async fn list(
        &self,
        sql: &mut dyn SqlRunner,
        db: &str,
    ) -> std::result::Result<Vec<CollMeta>, MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.list(sql, db).await;
        };
        let Some((id, cat)) = self.state(sql, tenant, db).await.map_err(|e| mongo(&e))? else {
            return Ok(Vec::new());
        };
        Ok(cat
            .ext
            .get::<DocState>(MODULE)
            .map(|s| s.of(id).map(|m| (**m).clone()).collect())
            .unwrap_or_default())
    }

    async fn dropped_db(
        &self,
        sql: &mut dyn SqlRunner,
        _db: &str,
    ) -> std::result::Result<(), MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return Ok(());
        };
        self.purge_orphans(tenant).await.map_err(|e| mongo(&e))
    }

    async fn prepare_db(
        &self,
        sql: &mut dyn SqlRunner,
        db: &str,
    ) -> std::result::Result<(), MongoError> {
        if let Some(tenant) = tenant_of(sql) {
            // Once a database was checked, the one-time work is done: no
            // catalog snapshot per command.
            if self.legacy_checked(tenant, db) {
                return Ok(());
            }
            self.state(sql, tenant, db).await.map_err(|e| mongo(&e))?;
        }
        Ok(())
    }

    async fn ensure_db(
        &self,
        sql: &mut dyn SqlRunner,
        db: &str,
    ) -> std::result::Result<(), MongoError> {
        if tenant_of(sql).is_none() {
            return TableCatalog.ensure_db(sql, db).await;
        }
        Ok(())
    }

    async fn register(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
    ) -> std::result::Result<Option<CollMeta>, MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.register(sql, ns, m).await;
        };
        self.register_in(tenant, ns, m).await.map_err(|e| mongo(&e))
    }

    async fn unregister(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> std::result::Result<(), MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.unregister(sql, ns).await;
        };
        self.update(tenant, ns, |d, tx, cid, _| {
            Box::pin(async move {
                d.delete(tx, cid).await?;
                Ok(true)
            })
        })
        .await
        .map(|_| ())
        .map_err(|e| mongo(&e))
    }

    async fn save_indexes(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
    ) -> std::result::Result<(), MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.save_indexes(sql, ns, m).await;
        };
        let m = m.clone();
        self.update(tenant, ns, move |d, tx, cid, _| {
            Box::pin(async move {
                d.write_indexes(tx, cid, &m).await?;
                Ok(true)
            })
        })
        .await
        .map(|_| ())
        .map_err(|e| mongo(&e))
    }

    async fn add_array_paths(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        paths: &BTreeSet<String>,
        full: bool,
    ) -> std::result::Result<(), MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.add_array_paths(sql, ns, paths, full).await;
        };
        // 1. Register what is new, marked pending, in one change.
        let req = paths.clone();
        let added = self
            .update(tenant, ns, move |d, tx, cid, old| {
                Box::pin(async move {
                    let fresh: BTreeSet<String> = if old.arrays_full {
                        BTreeSet::new()
                    } else {
                        req.difference(&old.array_paths).cloned().collect()
                    };
                    let becomes_full = full && !old.arrays_full;
                    if fresh.is_empty() && !becomes_full {
                        return Ok(false);
                    }
                    let mut all = old.array_paths;
                    all.extend(fresh.iter().cloned());
                    let mut pending = old.pending_paths;
                    if becomes_full {
                        pending.insert("*".to_owned());
                    } else {
                        pending.extend(fresh);
                    }
                    tx.exec_drop(
                        format!(
                            "UPDATE {} SET array_paths = ?, array_paths_full = ?, \
                             array_paths_pending = ? WHERE collection_id = ?",
                            t(&d.schema, "collections")
                        ),
                        (
                            if becomes_full || old.arrays_full {
                                "[]".to_owned()
                            } else {
                                paths_json(&all)
                            },
                            i8::from(becomes_full || old.arrays_full),
                            paths_json(&pending),
                            cid,
                        ),
                    )
                    .await
                    .map_err(map_err)?;
                    metrics::counter!("sprig_doc_path_publications_total", "kind" => "array")
                        .increment(1);
                    Ok(true)
                })
            })
            .await
            .map_err(|e| mongo(&e))?;
        // 2. Every gateway loads a version holding them before the write
        // commits (check_flags). Paths an earlier write registered without
        // a confirmation are confirmed now.
        if let Some(p) = &self.publisher {
            let cat = self.snapshot(tenant).await.map_err(|e| mongo(&e))?;
            let version = added.unwrap_or(cat.version).max(cat.version);
            let timeout = std::time::Duration::from_millis(self.svc.settings.get(
                &super::publish::PATH_PUBLISH_TIMEOUT_MS,
                &cat.setting_ctx(&self.svc.dc),
                Some(&cat.settings),
            ));
            p.publish(tenant, version, timeout).await?;
        }
        // 3. Confirmed: no longer pending.
        let done = paths.clone();
        self.update(tenant, ns, move |d, tx, cid, old| {
            Box::pin(async move {
                let mut pending = old.pending_paths;
                let before = pending.len();
                pending.retain(|p| !done.contains(p) && !(p == "*" && (full || old.arrays_full)));
                if pending.len() == before {
                    return Ok(false);
                }
                tx.exec_drop(
                    format!(
                        "UPDATE {} SET array_paths_pending = ? WHERE collection_id = ?",
                        t(&d.schema, "collections")
                    ),
                    (
                        if pending.is_empty() {
                            None
                        } else {
                            Some(paths_json(&pending))
                        },
                        cid,
                    ),
                )
                .await
                .map_err(map_err)?;
                Ok(true)
            })
        })
        .await
        .map(|_| ())
        .map_err(|e| mongo(&e))
    }

    async fn rename(
        &self,
        sql: &mut dyn SqlRunner,
        from: &Ns,
        to: &Ns,
        m: &CollMeta,
    ) -> std::result::Result<(), MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.rename(sql, from, to, m).await;
        };
        self.unregister(sql, from).await?;
        let mut n = m.clone();
        n.name.clone_from(&to.coll);
        match self
            .register_in(tenant, to, &n)
            .await
            .map_err(|e| mongo(&e))?
        {
            None => Ok(()),
            Some(_) => Err(MongoError::new(
                code::NAMESPACE_EXISTS,
                "target namespace exists",
            )),
        }
    }
}

#[async_trait]
impl super::changes::TableNames for CatalogDocs {
    async fn collection(&self, tenant: TenantId, db: &str, table: &str) -> Option<String> {
        let cat = self.snapshot(tenant).await.ok()?;
        let lctn = self.lctn(&cat);
        let row = cat.database(&database_key(db, lctn))?;
        cat.ext
            .get::<DocState>(MODULE)?
            .of(row.db_id.0)
            .find(|m| m.table == table)
            .map(|m| m.name.clone())
    }

    async fn uuid(&self, tenant: TenantId, db: &str, table: &str) -> Option<[u8; 16]> {
        let cat = self.snapshot(tenant).await.ok()?;
        let lctn = self.lctn(&cat);
        let row = cat.database(&database_key(db, lctn))?;
        cat.ext
            .get::<DocState>(MODULE)?
            .of(row.db_id.0)
            .find(|m| m.table == table)
            .map(|m| m.uuid)
    }

    async fn images(&self, tenant: TenantId, db: &str, table: &str) -> bool {
        let Ok(cat) = self.snapshot(tenant).await else {
            return false;
        };
        let lctn = self.lctn(&cat);
        let Some(row) = cat.database(&database_key(db, lctn)) else {
            return false;
        };
        cat.ext
            .get::<DocState>(MODULE)
            .and_then(|s| s.of(row.db_id.0).find(|m| m.table == table).cloned())
            .is_some_and(|m| sprig_doc::meta::keeps_images(&m.options))
    }
}
