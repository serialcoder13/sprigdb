//! What goes with a dropped database besides its schemas: its sinks and
//! connections and its logic modules, so none stays `running` or `active`
//! in the catalog and no job keeps running for it; and a member's
//! inherited instances in it, created again at their next use. Its streams
//! went first (in [`super::streams`]).
//!
//! [`DbDrop::before`] notes the database's id and sinks before the
//! statement runs; [`DbDrop::after`] runs once `DROP DATABASE` has
//! published its catalog version. A failure there is logged and the
//! client still gets the drop's OK: the database is gone either way, and
//! the jobs also end on their own checks (a trigger whose module or
//! database is gone).

use std::sync::Arc;

use sprig_catalog::CatalogLocator;
use sprig_catalog::ddl::database_key;
use sprig_catalog::version::VersionKind;
use sprig_common::Result;
use sprig_common::ids::{DbId, TenantId};
use sprig_sinks::store::SinksState;

use crate::tenant::TenantServices;
use crate::tenant::session::TenantSession;

/// A `DROP DATABASE` about to run (see the module docs).
pub(super) struct DbDrop {
    tenant: TenantId,
    db: DbId,
    name: String,
    lctn: bool,
    sinks: Option<SinksState>,
}

impl DbDrop {
    /// The database `name` of the session's tenant, if it exists.
    pub(super) fn before(ts: &TenantSession, name: &str, lctn: bool) -> Option<Self> {
        let cat = ts.catalog()?;
        let db = cat.database(&database_key(name, lctn))?.db_id;
        Some(Self {
            tenant: cat.tenant,
            db,
            name: name.to_owned(),
            lctn,
            sinks: cat
                .ext
                .get::<SinksState>(sprig_sinks::store::MODULE)
                .filter(|s| s.migrated)
                .cloned(),
        })
    }

    /// Drops the database's sinks, connections and logic modules after
    /// the drop published `version`. Returns the catalog version the sink
    /// changes published, if any.
    pub(super) async fn after(self, svc: &Arc<TenantServices>, version: u64) -> Option<u64> {
        let sinks = match self.drop_sinks(svc).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(tenant = %self.tenant, db = %self.name, error = %e,
                    "DROP DATABASE: the database's sinks were not all dropped");
                None
            }
        };
        if let Err(e) = self.drop_logic(svc, version).await {
            tracing::warn!(tenant = %self.tenant, db = %self.name, error = %e,
                "DROP DATABASE: the database's logic modules were not all dropped");
        }
        // A member's inherited instances in the database.
        if let Err(e) = Box::pin(crate::inherited::lifecycle::database_dropped(
            svc,
            self.tenant,
            &self.name,
            self.lctn,
        ))
        .await
        {
            tracing::warn!(tenant = %self.tenant, db = %self.name, error = %e,
                "DROP DATABASE: the database's inherited instances were not all dropped");
        }
        sinks
    }

    async fn drop_sinks(&self, svc: &TenantServices) -> Result<Option<u64>> {
        let Some(st) = &self.sinks else {
            return Ok(None);
        };
        let (sinks, connections) = sprig_sinks::dropdb::database_plan(st, self.db.0);
        if sinks.is_empty() && connections.is_empty() {
            return Ok(None);
        }
        // Tasks end when their index entry and catalog row are gone, as
        // after `DROP SINK`.
        let dc = svc.locator.dc_store();
        for s in &sinks {
            sprig_sinks::meta::index_delete(&dc, self.tenant.0, s.id.0).await?;
        }
        let names: Vec<String> = sinks
            .iter()
            .map(|s| s.name.clone())
            .chain(connections.iter().map(|c| format!("connection {}", c.name)))
            .collect();
        let changes = sprig_sinks::dropdb::database_changes(sinks, connections);
        let mut c = svc
            .catalog
            .change(self.tenant, VersionKind::Catalog)
            .await?;
        let applied = {
            let (store, tx) = c.tx().parts();
            let mut r = Ok(());
            for ch in &changes {
                r = sprig_sinks::sql::apply(tx, store.schema(), ch, None, self.tenant, 0).await;
                if r.is_err() {
                    break;
                }
            }
            r
        };
        if let Err(e) = applied {
            let _ = c.rollback().await;
            return Err(e);
        }
        let v = c.commit().await?;
        tracing::info!(tenant = %self.tenant, db = %self.name, dropped = ?names,
            "DROP DATABASE: the database's sinks and connections dropped");
        Ok(Some(v))
    }

    async fn drop_logic(&self, svc: &TenantServices, version: u64) -> Result<()> {
        let ep = svc.locator.writer(self.tenant).await?;
        let mut c = ep.pool.get_conn().await.map_err(|e| {
            sprig_common::SprigError::new(
                sprig_common::SprigCode::CatalogUnavailable,
                "catalog unreachable",
            )
            .with_source(e)
        })?;
        sprig_logic::dropdb::database_dropped(
            &mut c,
            ep.store.schema(),
            &svc.locator.dc_store(),
            self.tenant,
            self.db.0,
            version,
        )
        .await?;
        Ok(())
    }
}
