//! Automatic document indexes in the catalog.
//!
//! An automatic index on a collection table (`sprig_advisor::autoindex::doc`)
//! is built like any other: field columns, then the MariaDB index, on
//! every shard. The document engine plans with an index only once it is a
//! `doc_indexes` row, so [`DocRegistry::reconcile`] keeps those rows in
//! step with `auto_indexes`, at the start of each `auto_index` pass:
//!
//! ```text
//! wanted     := document indexes in observing or active (built everywhere)
//! registered := doc_indexes rows with origin 'auto'
//! wanted − registered → a doc_indexes row (origin auto, auto_index_id,
//!                        mariadb_name sprig_auto_<hex>, the next index id),
//!                        promoted_fields pair rows for its paths
//! registered − wanted → its doc_indexes row deleted (retiring, dropping,
//!                        failed or gone): the engine stops planning with it
//!                        before the index is dropped
//! all of it in one versioned catalog change, so every gateway reloads it
//! ```
//!
//! The field columns stay when an index is unregistered (retiring drops
//! only the index); the engine reads them only for paths
//! some registered index holds.

use std::collections::{BTreeMap, BTreeSet};

use bson::Document;
use mysql_async::prelude::Queryable;
use sprig_advisor::autoindex::doc;
use sprig_advisor::autoindex::model::IndexKind;
use sprig_advisor::autoindex::store::{AutoIndexRow, IndexState};
use sprig_catalog::{MariaBackend, VersionKind, Versioner};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};

fn err(e: impl std::fmt::Display) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, e.to_string())
}

fn t(schema: &str, table: &str) -> String {
    format!("`{}`.`{table}`", schema.replace('`', "``"))
}

/// The paths of a document index row, `None` for any other row.
#[must_use]
pub fn paths(row: &AutoIndexRow) -> Option<Vec<(String, i32)>> {
    let def = row.definition.on(row.object, IndexKind::FieldColumn);
    doc::is_doc(&def).then(|| doc::paths_of(&def))
}

/// A document index to register: its row and its paths.
type Wanted<'a> = (&'a AutoIndexRow, Vec<(String, i32)>);

/// Keeps `doc_indexes` in step with automatic document indexes.
#[derive(Clone)]
pub struct DocRegistry {
    versioner: Versioner<MariaBackend>,
}

impl std::fmt::Debug for DocRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DocRegistry")
    }
}

impl DocRegistry {
    /// A registry changing tenant catalogs through `versioner`.
    #[must_use]
    pub fn new(versioner: Versioner<MariaBackend>) -> Self {
        Self { versioner }
    }

    /// Registers and unregisters (module docs); returns how many rows it
    /// changed. `rows` are the tenant's live `auto_indexes` rows.
    ///
    /// # Errors
    /// The catalog's.
    #[allow(clippy::too_many_lines)]
    pub async fn reconcile(&self, tenant: TenantId, rows: &[AutoIndexRow]) -> Result<u32> {
        let wanted: BTreeMap<u64, Wanted<'_>> = rows
            .iter()
            .filter(|r| matches!(r.state, IndexState::Observing | IndexState::Active))
            .filter_map(|r| Some((r.index_id.0, (r, paths(r)?))))
            .collect();
        let mut ch = self.versioner.lock(tenant, VersionKind::Catalog).await?;
        let r: Result<u32> = async {
            let (store, tx) = ch.tx().parts();
            let s = store.schema().to_owned();
            let registered: Vec<(u64, u32, Option<u64>)> = tx
                .query(format!(
                    "SELECT collection_id, index_id, auto_index_id FROM {} WHERE origin = 'auto'",
                    t(&s, "doc_indexes")
                ))
                .await
                .map_err(err)?;
            let have: BTreeSet<u64> = registered.iter().filter_map(|(_, _, a)| *a).collect();
            let mut changed = 0u32;
            for (cid, iid, auto) in &registered {
                if auto.is_some_and(|a| wanted.contains_key(&a)) {
                    continue;
                }
                tx.exec_drop(
                    format!(
                        "DELETE FROM {} WHERE collection_id = ? AND index_id = ?",
                        t(&s, "doc_indexes")
                    ),
                    (cid, iid),
                )
                .await
                .map_err(err)?;
                tracing::info!(%tenant, collection = cid, index = iid,
                    "auto_index: document index unregistered");
                changed += 1;
            }
            for (id, (row, ps)) in &wanted {
                if have.contains(id) {
                    continue;
                }
                let cid = row.object.0;
                let next: Option<u32> = tx
                    .exec_first(
                        format!(
                            "SELECT next_index FROM {} WHERE collection_id = ? FOR UPDATE",
                            t(&s, "collections")
                        ),
                        (cid,),
                    )
                    .await
                    .map_err(err)?;
                let Some(next) = next else {
                    // Not a collection (any more): nothing to register.
                    continue;
                };
                let mut key = Document::new();
                for (p, d) in ps {
                    key.insert(p.clone(), *d);
                }
                let key_bytes = bson::to_vec(&key).map_err(err)?;
                let opts = bson::to_vec(&Document::new()).map_err(err)?;
                let slugs: Vec<String> = ps.iter().map(|(p, _)| sprig_doc::meta::slug(p)).collect();
                tx.exec_drop(
                    format!(
                        "INSERT INTO {} (collection_id, index_id, name, key_pattern, impl, \
                         mariadb_name, field_columns, is_unique, is_sparse, expire_seconds, \
                         multikey_paths, origin, options, auto_index_id, state, created_at) \
                         VALUES (?, ?, ?, ?, 'column', ?, ?, 0, 0, NULL, '[]', 'auto', ?, ?, \
                         'ready', UTC_TIMESTAMP(3))",
                        t(&s, "doc_indexes")
                    ),
                    mysql_async::Params::Positional(vec![
                        cid.into(),
                        next.into(),
                        row.name.clone().into(),
                        key_bytes.into(),
                        row.name.clone().into(),
                        serde_json::Value::from(slugs).to_string().into(),
                        opts.into(),
                        id.into(),
                    ]),
                )
                .await
                .map_err(err)?;
                for (p, _) in ps {
                    tx.exec_drop(
                        format!(
                            "INSERT IGNORE INTO {} (collection_id, path_hash, class, path, slug, \
                             sql_type, origin, auto_index_id, state, created_at) \
                             VALUES (?, ?, 'pair', ?, ?, ?, 'auto', ?, 'ready', UTC_TIMESTAMP(3))",
                            t(&s, "promoted_fields")
                        ),
                        (
                            cid,
                            sprig_doc::meta::fnv64(p.as_bytes()),
                            p.as_str(),
                            sprig_doc::meta::slug(p),
                            format!("TINYINT,DOUBLE,VARCHAR({})", sprig_doc::meta::S_CHARS),
                            id,
                        ),
                    )
                    .await
                    .map_err(err)?;
                }
                tx.exec_drop(
                    format!(
                        "UPDATE {} SET next_index = ? WHERE collection_id = ?",
                        t(&s, "collections")
                    ),
                    (next + 1, cid),
                )
                .await
                .map_err(err)?;
                tracing::info!(%tenant, collection = cid, index = next, name = %row.name,
                    "auto_index: document index registered");
                changed += 1;
            }
            Ok(changed)
        }
        .await;
        match r {
            Ok(0) => {
                let _ = ch.release().await;
                Ok(0)
            }
            Ok(n) => {
                let v = ch
                    .allocate(sprig_catalog::ChangeOpts::default())
                    .await?
                    .commit()
                    .await?;
                tracing::info!(%tenant, version = v, changed = n, "auto_index: document indexes published");
                Ok(n)
            }
            Err(e) => {
                let _ = ch.release().await;
                Err(e)
            }
        }
    }
}
