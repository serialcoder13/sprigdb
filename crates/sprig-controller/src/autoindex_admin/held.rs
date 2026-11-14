//! The `recommended` rows of a tenant with the settings the
//! approvals gate needs, resolved for each row's object exactly as
//! automatic indexing resolves its own policy.

use std::collections::BTreeMap;

use sprig_advisor::autoindex::policy::{Layered, SettingSource};
use sprig_common::Result;
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_settings::Resolved;

use super::view::IndexView;
use super::{AutoIndexAdmin, AutoIndexService, ListIndexesQuery};

/// One `recommended` row.
#[derive(Debug, Clone)]
pub struct HeldIndex {
    /// The tenant's name.
    pub tenant_name: String,
    /// The row as the Admin API shows it.
    pub view: IndexView,
    /// The object's database.
    pub db_id: Option<DbId>,
    /// `keys` resolved for the object.
    pub settings: BTreeMap<String, Resolved>,
}

impl AutoIndexService {
    /// The tenant's `recommended` rows, with `keys` resolved for each
    /// row's object. An inactive tenant, or one homed elsewhere, has none.
    ///
    /// # Errors
    /// Catalog errors.
    pub async fn held(&self, tenant: TenantId, keys: &[&str]) -> Result<Vec<HeldIndex>> {
        let Some(e) = self.directory.get(tenant).await? else {
            return Ok(Vec::new());
        };
        let name = e.body().name.clone();
        if e.body().home_dc != *self.dc() {
            return Ok(Vec::new());
        }
        let q = ListIndexesQuery {
            state: Some("recommended".into()),
            ..ListIndexesQuery::default()
        };
        let rows = self.list(&name, &q).await?.indexes;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let t = self.tenant(&name).await?;
        let etcd = self.live.snapshot();
        let dc = self.dc().clone();
        let src = Layered {
            etcd: &etcd,
            tenant: Some(&t.cat.settings),
        };
        let mut out = Vec::new();
        for v in rows {
            let obj = ObjectId(v.object_id);
            let db_id = t.cat.objects.get(obj).map(|o| o.db_id);
            let mut settings = BTreeMap::new();
            if let Some(db) = db_id {
                let ctx = t.cat.setting_ctx(&dc).with_db(db).with_object(obj);
                for k in keys {
                    if let Ok(r) = src.resolve(k, &ctx) {
                        settings.insert((*k).to_owned(), r);
                    }
                }
            }
            out.push(HeldIndex {
                tenant_name: name.clone(),
                view: v,
                db_id,
                settings,
            });
        }
        Ok(out)
    }
}
