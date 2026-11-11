//! `sprigctl search list|show` (chapter 39 build step 14; R5 round 6):
//! the DC tier's search records, `search/indexes/` and `search/copies/`
//! (see `sprig_search::placement`; the controller reads them as JSON).

use serde_json::{Value, json};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::ListOpts;

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
