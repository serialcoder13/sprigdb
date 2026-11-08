//! Resolution of physical names to logical ones (chapter 38, "Resolution").
//!
//! A schema name parses into tenant, database, group and shard; the
//! database's logical name and the table's object id come from the
//! tenant's catalog (`t<tenant>_sprig.databases` and `objects`, chapter
//! 08). Inside a shard schema a table keeps its logical name.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use sprig_common::ids::{DbId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};

use crate::binlog::schemas_in;

/// Looks up logical names in a tenant's catalog.
#[async_trait::async_trait]
pub trait Resolver: Send + Sync {
    /// The logical name of database `db`, if the catalog has it.
    ///
    /// # Errors
    /// The catalog's errors (retryable while a move holds it).
    async fn database(&self, tenant: TenantId, db: DbId) -> Result<Option<String>>;

    /// The object id of table `table` in database `db`, if the catalog
    /// has it.
    ///
    /// # Errors
    /// The catalog's errors.
    async fn object(&self, tenant: TenantId, db: DbId, table: &str) -> Result<Option<u64>>;
}

/// Names from a fixed table (tests and the simulation).
#[derive(Debug, Clone, Default)]
pub struct StaticResolver {
    dbs: HashMap<(TenantId, DbId), String>,
    objects: HashMap<(TenantId, DbId, String), u64>,
}

impl StaticResolver {
    /// No names yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Database `db` of `tenant` is called `name`.
    #[must_use]
    pub fn database(mut self, tenant: TenantId, db: DbId, name: &str) -> Self {
        self.dbs.insert((tenant, db), name.to_owned());
        self
    }

    /// Table `table` of `db` has object id `id`.
    #[must_use]
    pub fn object(mut self, tenant: TenantId, db: DbId, table: &str, id: u64) -> Self {
        self.objects.insert((tenant, db, table.to_owned()), id);
        self
    }
}

#[async_trait::async_trait]
impl Resolver for StaticResolver {
    async fn database(&self, tenant: TenantId, db: DbId) -> Result<Option<String>> {
        Ok(self.dbs.get(&(tenant, db)).cloned())
    }

    async fn object(&self, tenant: TenantId, db: DbId, table: &str) -> Result<Option<u64>> {
        Ok(self.objects.get(&(tenant, db, table.to_owned())).copied())
    }
}

/// Where a tenant's catalog can be queried: a `sprig_ctl` pool on the
/// tenant's cluster.
#[async_trait::async_trait]
pub trait CatalogPools: Send + Sync {
    /// A pool reaching `tenant`'s catalog schema.
    ///
    /// # Errors
    /// When the tenant's cluster can't be found.
    async fn pool(&self, tenant: TenantId) -> Result<Pool>;
}

type ObjectKey = (TenantId, DbId, String);

/// Names from the tenant catalog, cached. Names found are kept for the
/// process's life: a renamed database shows its old name until restart
/// (R1; chapter 08's catalog cache replaces this).
pub struct CatalogResolver {
    pools: Arc<dyn CatalogPools>,
    dbs: Mutex<HashMap<(TenantId, DbId), String>>,
    objects: Mutex<HashMap<ObjectKey, u64>>,
}

impl std::fmt::Debug for CatalogResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogResolver").finish_non_exhaustive()
    }
}

impl CatalogResolver {
    /// Reads catalogs through `pools`.
    #[must_use]
    pub fn new(pools: Arc<dyn CatalogPools>) -> Self {
        Self {
            pools,
            dbs: Mutex::default(),
            objects: Mutex::default(),
        }
    }
}

fn catalog_err(e: &mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!("read the tenant catalog: {e}"),
    )
}

#[async_trait::async_trait]
impl Resolver for CatalogResolver {
    async fn database(&self, tenant: TenantId, db: DbId) -> Result<Option<String>> {
        if let Some(n) = self
            .dbs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(tenant, db))
        {
            return Ok(Some(n.clone()));
        }
        let mut c = self
            .pools
            .pool(tenant)
            .await?
            .get_conn()
            .await
            .map_err(|e| catalog_err(&e))?;
        let q = format!(
            "SELECT name FROM `{}`.`databases` WHERE db_id = ? AND state <> 'dropped'",
            SchemaName::catalog(tenant)
        );
        let name: Option<String> = c
            .exec_first(q, (db.0,))
            .await
            .map_err(|e| catalog_err(&e))?;
        if let Some(n) = &name {
            self.dbs
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert((tenant, db), n.clone());
        }
        Ok(name)
    }

    async fn object(&self, tenant: TenantId, db: DbId, table: &str) -> Result<Option<u64>> {
        let key = (tenant, db, table.to_owned());
        if let Some(id) = self
            .objects
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
        {
            return Ok(Some(*id));
        }
        let mut c = self
            .pools
            .pool(tenant)
            .await?
            .get_conn()
            .await
            .map_err(|e| catalog_err(&e))?;
        let q = format!(
            "SELECT object_id FROM `{}`.objects WHERE db_id = ? AND namespace = 'relation' \
             AND name_key = ? AND state <> 'dropped'",
            SchemaName::catalog(tenant)
        );
        let id: Option<u64> = c
            .exec_first(q, (db.0, table.to_lowercase()))
            .await
            .map_err(|e| catalog_err(&e))?;
        if let Some(id) = id {
            self.objects
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(key, id);
        }
        Ok(id)
    }
}

/// Replaces every one of `tenant`'s schema names in `sql` by the logical
/// database name `names` gives it. `None` when a name can't be resolved,
/// or the text still holds a physical or internal name afterwards: DDL
/// text never leaves the feed with physical names in it.
#[must_use]
pub fn logical_sql<S: std::hash::BuildHasher>(
    sql: &str,
    tenant: TenantId,
    names: &HashMap<String, String, S>,
) -> Option<String> {
    let mut out = String::with_capacity(sql.len());
    let mut word = String::new();
    let flush = |word: &mut String, out: &mut String| -> Option<()> {
        if word.is_empty() {
            return Some(());
        }
        match word.parse::<SchemaName>() {
            Ok(s) if s.tenant() == tenant => out.push_str(names.get(word.as_str())?),
            _ => out.push_str(word),
        }
        word.clear();
        Some(())
    };
    for ch in sql.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            word.push(ch);
        } else {
            flush(&mut word, &mut out)?;
            out.push(ch);
        }
    }
    flush(&mut word, &mut out)?;
    let leaks = !schemas_in(&out, tenant).is_empty()
        || out.to_ascii_lowercase().contains("__sprig_")
        || out.contains(&format!("t{}_", tenant.0));
    (!leaks).then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_text_gets_logical_names_or_nothing() {
        let t = TenantId(42);
        let names: HashMap<String, String> = [("t42_d1_g1_s1".to_owned(), "shop".to_owned())]
            .into_iter()
            .collect();
        assert_eq!(
            logical_sql("ALTER TABLE `t42_d1_g1_s1`.`orders` ADD c INT", t, &names).as_deref(),
            Some("ALTER TABLE `shop`.`orders` ADD c INT")
        );
        assert_eq!(
            logical_sql("ALTER TABLE t42_d2_g1_s1.x ADD c INT", t, &names),
            None
        );
        assert_eq!(
            logical_sql(
                "RENAME TABLE t42_d1_g1_s1.a TO t42_d1_g1_s1.__sprig_f_0",
                t,
                &names
            ),
            None
        );
        assert_eq!(
            logical_sql("CREATE TABLE a (b INT) COMMENT 't42_x'", t, &names),
            None
        );
    }
}
