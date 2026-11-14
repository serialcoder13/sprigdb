//! Settings rows inside a change, over any catalog backend.
//!
//! The ancestry copy ([`crate::ancestry`]) and setting writes
//! ([`crate::settings`]) read and write `settings` rows inside the
//! transaction that holds the catalog version row, and read other tenants'
//! tenant-scope rows outside any change. These traits name what they need,
//! so the same code runs over MariaDB ([`MariaTx`], [`MariaBackend`]) and
//! the in-memory catalog of unit tests ([`MemSourceTx`], [`MemSource`]).

use std::time::SystemTime;

use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};

use crate::cache::mem::{MemSource, MemSourceTx, MemTables};
use crate::sql::{map_err, timed};
use crate::store::{DbState, ObjectState, SettingRow, SettingScope};
use crate::version::{MariaBackend, MariaTx, VersionBackend};

/// Reads and writes `settings` rows inside an open change (or a locked
/// version row). Reads see the transaction's own writes.
#[async_trait::async_trait]
pub trait SettingsTx: Send {
    /// One row.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn setting(&mut self, scope: SettingScope, key: &str) -> Result<Option<SettingRow>>;

    /// Every row of one scope, empty rows included.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn scope_rows(&mut self, scope: SettingScope) -> Result<Vec<SettingRow>>;

    /// Every row of one key at any scope, empty rows included.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn key_rows(&mut self, key: &str) -> Result<Vec<SettingRow>>;

    /// Inserts or replaces a row.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn put_setting(&mut self, row: &SettingRow) -> Result<()>;

    /// Clears a row's value, bounds or both, leaving an empty row with
    /// `version` (a tombstone incremental loads see). Returns whether the
    /// row existed.
    ///
    /// # Errors
    /// 39070 or a backend error.
    #[allow(clippy::too_many_arguments)] // one row's address and its change
    async fn reset_setting(
        &mut self,
        scope: SettingScope,
        key: &str,
        value: bool,
        bounds: bool,
        set_by: &str,
        at: SystemTime,
        version: u64,
    ) -> Result<bool>;

    /// Whether database `db` exists and isn't dropped or being dropped.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn database_live(&mut self, db: DbId) -> Result<bool>;

    /// The database of object `object` if it exists and isn't dropped or
    /// being dropped.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn object_db(&mut self, object: ObjectId) -> Result<Option<DbId>>;
}

/// One tenant's tenant-scope rows (internal `sprig.` rows included) at its
/// committed catalog version.
#[derive(Debug, Clone, PartialEq)]
pub struct TenantScopeRows {
    /// The committed catalog version the rows were read at.
    pub version: u64,
    /// The rows, by key.
    pub rows: Vec<SettingRow>,
}

impl TenantScopeRows {
    /// The value of an internal key.
    #[must_use]
    pub fn internal(&self, key: &str) -> Option<&serde_json::Value> {
        self.rows
            .iter()
            .find(|r| r.key == key)
            .and_then(|r| r.value.as_ref())
    }

    /// The rows descendants copy: settings rows with a value or bounds
    /// (internal keys and empty rows left out).
    #[must_use]
    pub fn copyable(&self) -> Vec<SettingRow> {
        self.rows
            .iter()
            .filter(|r| !r.is_internal() && !r.is_empty())
            .cloned()
            .collect()
    }
}

/// A catalog backend whose changes write settings rows, and which can read
/// any tenant's tenant-scope rows outside a change.
#[async_trait::async_trait]
pub trait SettingsBackend: VersionBackend
where
    Self::Tx: SettingsTx,
{
    /// `tenant`'s tenant-scope rows, read in one consistent snapshot (from
    /// a replica where the backend has one).
    ///
    /// # Errors
    /// 39070 if the catalog is unreachable; 39072 if the tenant has no
    /// catalog.
    async fn tenant_scope(&self, tenant: TenantId) -> Result<TenantScopeRows>;
}

// ---- MariaDB ----

#[async_trait::async_trait]
impl SettingsTx for MariaTx {
    async fn setting(&mut self, scope: SettingScope, key: &str) -> Result<Option<SettingRow>> {
        let (s, q) = self.parts();
        s.setting(q, scope, key).await
    }

    async fn scope_rows(&mut self, scope: SettingScope) -> Result<Vec<SettingRow>> {
        let (s, q) = self.parts();
        s.settings_of(q, scope).await
    }

    async fn key_rows(&mut self, key: &str) -> Result<Vec<SettingRow>> {
        let (s, q) = self.parts();
        s.settings_for_key(q, key).await
    }

    async fn put_setting(&mut self, row: &SettingRow) -> Result<()> {
        let (s, q) = self.parts();
        s.put_setting(q, row).await
    }

    async fn reset_setting(
        &mut self,
        scope: SettingScope,
        key: &str,
        value: bool,
        bounds: bool,
        set_by: &str,
        at: SystemTime,
        version: u64,
    ) -> Result<bool> {
        let (s, q) = self.parts();
        s.reset_setting(q, scope, key, value, bounds, set_by, at, version)
            .await
    }

    async fn database_live(&mut self, db: DbId) -> Result<bool> {
        let (s, q) = self.parts();
        Ok(s.database(q, db)
            .await?
            .is_some_and(|d| matches!(d.state, DbState::Creating | DbState::Active)))
    }

    async fn object_db(&mut self, object: ObjectId) -> Result<Option<DbId>> {
        let (s, q) = self.parts();
        Ok(s.object(q, object)
            .await?
            .filter(|o| !matches!(o.state, ObjectState::Dropping | ObjectState::Dropped))
            .map(|o| o.db_id))
    }
}

#[async_trait::async_trait]
impl SettingsBackend for MariaBackend {
    async fn tenant_scope(&self, tenant: TenantId) -> Result<TenantScopeRows> {
        let ep = self.locator().reader(tenant).await?;
        timed(
            self.opts().statement_timeout * 2,
            "tenant settings",
            async {
                let mut tx = ep.store.begin_snapshot(&ep.pool).await?;
                let versions = ep.store.versions(&mut tx).await?;
                let rows = ep.store.settings_of(&mut tx, SettingScope::Tenant).await?;
                tx.commit().await.map_err(map_err)?;
                Ok(TenantScopeRows {
                    version: versions.catalog,
                    rows,
                })
            },
        )
        .await
    }
}

// ---- in memory ----

impl MemSourceTx {
    /// The tables with this transaction's staged writes applied.
    pub(crate) fn current(&self) -> MemTables {
        let mut t = self.committed();
        self.apply_staged(&mut t);
        t
    }
}

#[async_trait::async_trait]
impl SettingsTx for MemSourceTx {
    async fn setting(&mut self, scope: SettingScope, key: &str) -> Result<Option<SettingRow>> {
        Ok(self
            .current()
            .settings
            .get(&(scope, key.to_owned()))
            .cloned())
    }

    async fn scope_rows(&mut self, scope: SettingScope) -> Result<Vec<SettingRow>> {
        Ok(self
            .current()
            .settings
            .into_iter()
            .filter(|((s, _), _)| *s == scope)
            .map(|(_, r)| r)
            .collect())
    }

    async fn key_rows(&mut self, key: &str) -> Result<Vec<SettingRow>> {
        Ok(self
            .current()
            .settings
            .into_iter()
            .filter(|((_, k), _)| k == key)
            .map(|(_, r)| r)
            .collect())
    }

    async fn put_setting(&mut self, row: &SettingRow) -> Result<()> {
        if row.set_by.chars().count() > 288 {
            return Err(SprigError::internal("set_by is longer than 288 characters"));
        }
        MemSourceTx::put_setting(self, row.clone());
        Ok(())
    }

    async fn reset_setting(
        &mut self,
        scope: SettingScope,
        key: &str,
        value: bool,
        bounds: bool,
        set_by: &str,
        at: SystemTime,
        version: u64,
    ) -> Result<bool> {
        let Some(mut row) = self.current().settings.remove(&(scope, key.to_owned())) else {
            return Ok(false);
        };
        if value {
            row.value = None;
        }
        if bounds {
            row.bounds = None;
        }
        set_by.clone_into(&mut row.set_by);
        row.set_at = at;
        row.version = version;
        MemSourceTx::put_setting(self, row);
        Ok(true)
    }

    async fn database_live(&mut self, db: DbId) -> Result<bool> {
        Ok(self
            .current()
            .databases
            .get(&db)
            .is_some_and(|d| matches!(d.state, DbState::Creating | DbState::Active)))
    }

    async fn object_db(&mut self, object: ObjectId) -> Result<Option<DbId>> {
        Ok(self
            .current()
            .objects
            .get(&object)
            .filter(|o| !matches!(o.state, ObjectState::Dropping | ObjectState::Dropped))
            .map(|o| o.db_id))
    }
}

#[async_trait::async_trait]
impl SettingsBackend for MemSource {
    async fn tenant_scope(&self, tenant: TenantId) -> Result<TenantScopeRows> {
        let t = self.tables(tenant).ok_or_else(|| {
            SprigError::new(
                SprigCode::TenantNotReady,
                format!("no catalog for tenant {tenant}"),
            )
        })?;
        Ok(TenantScopeRows {
            version: t.versions.catalog,
            rows: t
                .settings
                .into_iter()
                .filter(|((s, _), _)| *s == SettingScope::Tenant)
                .map(|(_, r)| r)
                .collect(),
        })
    }
}
