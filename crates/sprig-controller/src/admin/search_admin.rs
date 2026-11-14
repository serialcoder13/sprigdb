//! `sprigctl search list|show`:
//! the DC tier's search records, `search/indexes/` and `search/copies/`
//! (see `sprig_search::placement`; the controller reads them as JSON).
//!
//! `sprigctl search rebuild` does what `ALTER SEARCH INDEX
//! ... REBUILD` does through a gateway: the index's generation goes up by
//! one in the tenant catalog (its `search_indexes` row and the definition
//! in it), and the index's records leave the DC tier, so the copies of
//! the old generation stop. Every gateway then loads the new definition:
//! its next query of the index writes the record again, and copies of the
//! new generation are built from MariaDB (the old one serves nothing
//! meanwhile: queries wait for a copy, as after `ALTER ... REBUILD`).

use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{ListOpts, Op, Txn};

use super::AdminService;

/// Strips the record envelope's `v`.
fn body(bytes: &[u8]) -> Option<Value> {
    let mut v: Value = serde_json::from_slice(bytes).ok()?;
    if let Some(m) = v.as_object_mut() {
        m.remove("v");
    }
    Some(v)
}

impl AdminService {
    async fn search_records(&self, rel: &str) -> Result<Vec<Value>> {
        let store = &self.stores.dc;
        let prefix = store.tier().key(rel)?;
        let page = store
            .list(
                &prefix,
                ListOpts {
                    limit: 100_000,
                    ..ListOpts::default()
                },
            )
            .await?;
        Ok(page.kvs.iter().filter_map(|kv| body(&kv.value)).collect())
    }

    /// `sprigctl search list <tenant>`: each index placed on workers, with
    /// its copies' states.
    ///
    /// # Errors
    /// 39902 for an unknown tenant; the store's.
    pub async fn list_search(&self, tenant: &str) -> Result<Value> {
        let t = self.get_tenant(tenant).await?.tenant_id.0;
        let mut out = Vec::new();
        for r in self.search_records(&format!("search/indexes/{t}/")).await? {
            let index = r["index"].as_u64().unwrap_or(0);
            let copies = self
                .search_records(&format!("search/copies/{t}/{index}/"))
                .await?;
            let def = &r["def"];
            out.push(json!({
                "index": index,
                "name": def["name"], "database": def["db"], "object": def["object"],
                "model": def["model"], "flavor": def["flavor"], "generation": def["generation"],
                "copies_wanted": r["copies"],
                "copies": copies.iter().map(|c| json!({
                    "copy": c["copy"], "worker": c["worker_id"], "state": c["state"],
                    "generation": c["generation"], "seeded_from": c["seeded_from"],
                    "updated_ms": c["updated_ms"],
                })).collect::<Vec<_>>(),
            }));
        }
        Ok(Value::Array(out))
    }

    /// `sprigctl search rebuild <tenant> <db.index>`: a new generation of
    /// the index (module docs).
    ///
    /// # Errors
    /// 39902 for an unknown tenant; 39820 for an unknown index; the
    /// catalog's and the store's.
    pub async fn rebuild_search(&self, tenant: &str, index: &str) -> Result<Value> {
        let t = self.get_tenant(tenant).await?.tenant_id;
        let Some(versioner) = &self.search_catalog else {
            return Err(super::invalid(
                "this controller doesn't change tenant catalogs",
            ));
        };
        let (db, name) = index.split_once('.').ok_or_else(|| {
            super::invalid(format!(
                "search rebuild: name the index as database.index, not {index}"
            ))
        })?;
        let not_found = || {
            SprigError::new(
                SprigCode::SearchIndexNotFound,
                format!("Unknown search index '{name}' in database '{db}' of tenant {tenant}"),
            )
        };
        let mut ch = versioner
            .change(t, sprig_catalog::VersionKind::Catalog)
            .await?;
        let r: Result<(u64, u32)> = async {
            let (store, tx) = ch.tx().parts();
            let row = match store.database_by_name(tx, db).await? {
                Some(r) => Some(r),
                None => store.database_by_name(tx, &db.to_lowercase()).await?,
            };
            let db_id = row.ok_or_else(not_found)?.db_id.0;
            let table = format!("`{}`.`search_indexes`", store.schema().replace('`', "``"));
            let found: Option<(u64, u32)> = tx
                .exec_first(
                    format!(
                        "SELECT index_id, generation FROM {table} WHERE db_id = ? AND name = ?"
                    ),
                    (db_id, name),
                )
                .await
                .map_err(|e| SprigError::new(SprigCode::CatalogUnavailable, e.to_string()))?;
            let (id, generation) = found.ok_or_else(not_found)?;
            // MariaDB assigns left to right: the definition gets the new
            // generation.
            tx.exec_drop(
                format!(
                    "UPDATE {table} SET generation = generation + 1, \
                     definition = JSON_SET(definition, '$.generation', generation), \
                     updated_at = UTC_TIMESTAMP(3) WHERE index_id = ?"
                ),
                (id,),
            )
            .await
            .map_err(|e| SprigError::new(SprigCode::CatalogUnavailable, e.to_string()))?;
            // The old generation's records leave the DC tier before the
            // new definition is published, so no gateway's new record is
            // removed.
            self.drop_search_records(t, id).await?;
            Ok((id, generation + 1))
        }
        .await;
        let (id, generation) = match r {
            Ok(x) => x,
            Err(e) => {
                let _ = ch.rollback().await;
                return Err(e);
            }
        };
        let version = ch.commit().await?;
        tracing::info!(
            tenant,
            index,
            generation,
            version,
            "search index rebuild started"
        );
        Ok(json!({
            "tenant": tenant, "index": id, "database": db, "name": name,
            "generation": generation, "catalog_version": version,
        }))
    }

    /// Removes `search/indexes/<t>/<id>` and its copies' records.
    async fn drop_search_records(&self, t: TenantId, id: u64) -> Result<()> {
        let store = &self.stores.dc;
        let mut ops = vec![Op::Delete(
            store.tier().key(&format!("search/indexes/{}/{id}", t.0))?,
        )];
        let prefix = store.tier().key(&format!("search/copies/{}/{id}/", t.0))?;
        let page = store
            .list(
                &prefix,
                ListOpts {
                    limit: 100_000,
                    ..ListOpts::default()
                },
            )
            .await?;
        ops.extend(page.kvs.into_iter().map(|kv| Op::Delete(kv.key)));
        store
            .txn(Txn {
                when: Vec::new(),
                then: ops,
                otherwise: Vec::new(),
            })
            .await?;
        Ok(())
    }

    /// `sprigctl search show <tenant> <db.index>`: the record and its
    /// copies in full.
    ///
    /// # Errors
    /// 39902 for an unknown tenant; 39820 for an unknown index.
    pub async fn show_search(&self, tenant: &str, index: &str) -> Result<Value> {
        let t = self.get_tenant(tenant).await?.tenant_id.0;
        let (db, name) = index
            .split_once('.')
            .map_or((None, index), |(d, n)| (Some(d), n));
        for r in self.search_records(&format!("search/indexes/{t}/")).await? {
            let def = &r["def"];
            let same_db = db.is_none_or(|d| def["db"].as_str() == Some(d));
            if same_db && def["name"].as_str() == Some(name) {
                let id = r["index"].as_u64().unwrap_or(0);
                let copies = self
                    .search_records(&format!("search/copies/{t}/{id}/"))
                    .await?;
                return Ok(json!({"record": r, "copies": copies}));
            }
        }
        Err(SprigError::new(
            SprigCode::SearchIndexNotFound,
            format!("no search index {index} of tenant {tenant} is placed on workers"),
        ))
    }
}
