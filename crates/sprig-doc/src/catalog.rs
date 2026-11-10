//! Where collection metadata lives (chapter 31, "Catalog tables").
//!
//! The engine reads and changes a collection's metadata ([`CollMeta`]:
//! table, uuid, path registry, indexes, options) only through a
//! [`DocCatalog`]:
//!
//! - the gateway's implementation keeps it in the tenant catalog's
//!   `collections`, `doc_indexes` and `promoted_fields`, changed through
//!   chapter 08's versioned changes and read from the gateway's cached
//!   snapshot, so a command costs no metadata read
//!   (`sprig_gateway::mongo::catalog`);
//! - [`TableCatalog`] keeps it in a hidden table `__sprig_doc_meta` in
//!   each logical database, read at the start of every command. It was
//!   R5's interim storage (`decisions/r5-interim.md`, D1); it remains for
//!   the engine's tests straight on MariaDB, and the gateway reads it once
//!   per database to migrate old collections into the catalog.

use std::collections::BTreeSet;
use std::fmt::Debug;

use async_trait::async_trait;
use sprig_proto_mongo::errors::{MongoError, code};

use crate::engine::{Ns, SqlRunner, mongo_error};
use crate::json;
use crate::meta::{self, CollMeta, META_COLUMNS, META_TABLE};
use crate::sql::{ident, str_lit};

/// Collection metadata storage.
#[async_trait]
pub trait DocCatalog: Send + Sync + Debug {
    /// The collection's metadata; `None` when it doesn't exist.
    async fn get(&self, sql: &mut dyn SqlRunner, ns: &Ns) -> Result<Option<CollMeta>, MongoError>;

    /// Every collection of `db`, by name.
    async fn list(&self, sql: &mut dyn SqlRunner, db: &str) -> Result<Vec<CollMeta>, MongoError>;

    /// Whatever a database needs before its first collection.
    async fn ensure_db(&self, sql: &mut dyn SqlRunner, db: &str) -> Result<(), MongoError>;

    /// After `DROP DATABASE db`: forget its collections.
    async fn dropped_db(&self, _sql: &mut dyn SqlRunner, _db: &str) -> Result<(), MongoError> {
        Ok(())
    }

    /// Called outside transactions before a command on `db`: one-time
    /// work that may run DDL (moving old metadata into the catalog).
    async fn prepare_db(&self, _sql: &mut dyn SqlRunner, _db: &str) -> Result<(), MongoError> {
        Ok(())
    }

    /// Registers a collection whose table exists now. Returns the
    /// collection another command registered first, if any.
    async fn register(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
    ) -> Result<Option<CollMeta>, MongoError>;

    /// Removes the collection (its table is already dropped).
    async fn unregister(&self, sql: &mut dyn SqlRunner, ns: &Ns) -> Result<(), MongoError>;

    /// Records `m`'s indexes, index counter and options.
    async fn save_indexes(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
    ) -> Result<(), MongoError>;

    /// Adds `paths` to the path registry, or marks it full, before a
    /// write that holds them commits (chapter 31's `check_flags`).
    async fn add_array_paths(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        paths: &BTreeSet<String>,
        full: bool,
    ) -> Result<(), MongoError>;

    /// `from`'s table was renamed to `m.table`: records it as `to`.
    async fn rename(
        &self,
        sql: &mut dyn SqlRunner,
        from: &Ns,
        to: &Ns,
        m: &CollMeta,
    ) -> Result<(), MongoError>;
}

fn ns_missing(e: &crate::engine::SqlError) -> bool {
    matches!(e.errno, Some(1146 | 1049))
}

/// The hidden-table storage (`__sprig_doc_meta` per database).
#[derive(Debug, Clone, Copy, Default)]
pub struct TableCatalog;

impl TableCatalog {
    /// Every row of `db`'s table; empty when it doesn't exist.
    ///
    /// # Errors
    /// SQL errors other than a missing table; malformed rows.
    pub async fn rows(sql: &mut dyn SqlRunner, db: &str) -> Result<Vec<CollMeta>, MongoError> {
        let q = format!(
            "SELECT {META_COLUMNS} FROM {}.{} ORDER BY coll",
            ident(db),
            ident(META_TABLE)
        );
        match sql.query(&q).await {
            Ok(rows) => rows.iter().map(|r| CollMeta::from_row(r)).collect(),
            Err(e) if ns_missing(&e) => Ok(Vec::new()),
            Err(e) => Err(mongo_error(&e)),
        }
    }
}

#[async_trait]
impl DocCatalog for TableCatalog {
    async fn get(&self, sql: &mut dyn SqlRunner, ns: &Ns) -> Result<Option<CollMeta>, MongoError> {
        let q = format!(
            "SELECT {META_COLUMNS} FROM {}.{} WHERE coll = {}",
            ident(&ns.db),
            ident(META_TABLE),
            str_lit(&ns.coll)
        );
        match sql.query(&q).await {
            Ok(rows) => rows.first().map(|r| CollMeta::from_row(r)).transpose(),
            Err(e) if ns_missing(&e) => Ok(None),
            Err(e) => Err(mongo_error(&e)),
        }
    }

    async fn list(&self, sql: &mut dyn SqlRunner, db: &str) -> Result<Vec<CollMeta>, MongoError> {
        Self::rows(sql, db).await
    }

    async fn ensure_db(&self, sql: &mut dyn SqlRunner, db: &str) -> Result<(), MongoError> {
        sql.exec(&meta::meta_table_sql(db))
            .await
            .map(|_| ())
            .map_err(|e| mongo_error(&e))
    }

    async fn register(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
    ) -> Result<Option<CollMeta>, MongoError> {
        match sql.exec(&m.insert_sql(&ns.db)).await {
            Ok(_) => Ok(None),
            Err(e) if e.errno == Some(1062) => match self.get(sql, ns).await? {
                Some(x) => Ok(Some(x)),
                None => Err(mongo_error(&e)),
            },
            Err(e) => Err(mongo_error(&e)),
        }
    }

    async fn unregister(&self, sql: &mut dyn SqlRunner, ns: &Ns) -> Result<(), MongoError> {
        sql.exec(&format!(
            "DELETE FROM {}.{} WHERE coll = {}",
            ident(&ns.db),
            ident(META_TABLE),
            str_lit(&ns.coll)
        ))
        .await
        .map(|_| ())
        .map_err(|e| mongo_error(&e))
    }

    async fn save_indexes(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
    ) -> Result<(), MongoError> {
        let stmt = format!(
            "UPDATE {}.{} SET indexes = {}, next_index = {} WHERE coll = {}",
            ident(&ns.db),
            ident(META_TABLE),
            str_lit(&m.indexes_json()),
            m.next_index,
            str_lit(&ns.coll)
        );
        sql.exec(&stmt)
            .await
            .map(|_| ())
            .map_err(|e| mongo_error(&e))
    }

    async fn add_array_paths(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        paths: &BTreeSet<String>,
        full: bool,
    ) -> Result<(), MongoError> {
        let stmt = if full {
            format!(
                "UPDATE {}.{} SET arrays_full = 1 WHERE coll = {}",
                ident(&ns.db),
                ident(META_TABLE),
                str_lit(&ns.coll)
            )
        } else {
            let mut set = String::from("array_paths");
            let mut cond = Vec::new();
            for p in paths {
                let mut j = String::new();
                json::write_json_string(&mut j, p);
                set = format!(
                    "IF(JSON_CONTAINS(array_paths, {0}), {set}, JSON_ARRAY_APPEND({set}, '$', {1}))",
                    str_lit(&j),
                    str_lit(p)
                );
                cond.push(format!("NOT JSON_CONTAINS(array_paths, {})", str_lit(&j)));
            }
            if cond.is_empty() {
                return Ok(());
            }
            format!(
                "UPDATE {}.{} SET array_paths = {set} WHERE coll = {} AND ({})",
                ident(&ns.db),
                ident(META_TABLE),
                str_lit(&ns.coll),
                cond.join(" OR ")
            )
        };
        sql.exec(&stmt)
            .await
            .map(|_| ())
            .map_err(|e| mongo_error(&e))
    }

    async fn rename(
        &self,
        sql: &mut dyn SqlRunner,
        from: &Ns,
        to: &Ns,
        m: &CollMeta,
    ) -> Result<(), MongoError> {
        self.unregister(sql, from).await?;
        let mut n = m.clone();
        n.name.clone_from(&to.coll);
        sql.exec(&n.insert_sql(&to.db))
            .await
            .map(|_| ())
            .map_err(|e| mongo_error(&e))
    }
}

/// 26 for a collection that vanished between two steps of a command.
#[must_use]
pub fn vanished(ns: &Ns) -> MongoError {
    MongoError::new(
        code::NAMESPACE_NOT_FOUND,
        format!("ns does not exist: {ns}"),
    )
}
