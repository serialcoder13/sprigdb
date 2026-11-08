//! Logical databases to physical schemas and back (chapter 07, "Physical
//! names" and build step 1).
//!
//! The grammar of physical names lives in [`sprig_common::names`], the one
//! module that knows the patterns (chapter 03, "Identifiers"); this module
//! re-exports it and adds the per-tenant map. [`NameMap`] is the interface
//! the rewriter uses. [`MemoryNameMap`] is an in-memory implementation for
//! tests and for the gateway until chapter 08 step 4 builds the map from
//! catalog snapshots and shard maps.

use std::collections::HashMap;

use sprig_common::ids::{DbId, GroupId, ShardId, TenantId};
pub use sprig_common::names::{AccountName, NameParseError, SchemaName, quote};

use super::NameError;

/// What a statement does with a database, which picks its schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Purpose {
    /// The session's current database (`USE`, `COM_INIT_DB`).
    Session,
    /// A read.
    Read,
    /// A write.
    Write,
    /// DDL: every shard of the database plus its reference schema.
    Ddl,
    /// Metadata: one representative shard per table group.
    Metadata,
}

/// The physical schemas a database name stands for in one statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaSet(pub Vec<SchemaName>);

impl SchemaSet {
    /// The only schema, when there is exactly one (always so in R0, where
    /// each database has one shard and no reference schema).
    #[must_use]
    pub fn single(&self) -> Option<SchemaName> {
        match self.0.as_slice() {
            [one] => Some(*one),
            _ => None,
        }
    }
}

/// One logical database of the tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalDb {
    /// Its id within the tenant.
    pub id: DbId,
    /// The name clients use.
    pub name: String,
    /// Its shard schemas, one per shard of each table group, in the order
    /// the catalog lists them.
    pub shards: Vec<SchemaName>,
    /// Its reference-table schema, from R1.
    pub reference: Option<SchemaName>,
}

/// A tenant's logical-to-physical name map, built from a catalog snapshot.
///
/// Implementations are immutable snapshots: a catalog change builds a new
/// map, and [`NameMap::version`] tells them apart.
pub trait NameMap: Send + Sync {
    /// The tenant whose names these are.
    fn tenant(&self) -> TenantId;

    /// The catalog version the map was built from.
    fn version(&self) -> u64;

    /// The database named `db`, matched exactly as the client wrote it (the
    /// caller lowercases first under `names.lower_case_table_names = 1`).
    fn database(&self, db: &str) -> Option<&LogicalDb>;

    /// The database with id `id`.
    fn database_by_id(&self, id: DbId) -> Option<&LogicalDb>;

    /// The schemas `db` stands for, for `purpose`.
    ///
    /// # Errors
    ///
    /// [`NameError::UnknownDatabase`] (1049) when the tenant has no such
    /// database. The caller forces one catalog refresh and retries before
    /// sending it to the client (chapter 07, "Failure handling").
    fn physical(&self, db: &str, purpose: Purpose) -> Result<SchemaSet, NameError> {
        let ldb = self
            .database(db)
            .ok_or_else(|| NameError::UnknownDatabase(db.to_owned()))?;
        let mut set = match purpose {
            // R0 has one shard per database; from R1 the router picks the
            // shard and asks per shard.
            Purpose::Session | Purpose::Read | Purpose::Write => {
                ldb.shards.first().copied().into_iter().collect()
            }
            Purpose::Ddl => ldb.shards.clone(),
            Purpose::Metadata => representatives(&ldb.shards),
        };
        if purpose == Purpose::Ddl {
            set.extend(ldb.reference);
        }
        Ok(SchemaSet(set))
    }

    /// The logical database a physical schema belongs to, found by parsing
    /// the name, checking the tenant and looking up the database id. `None`
    /// for another tenant's schema, the tenant catalog, an unknown id or
    /// anything that is not a physical name.
    fn logical(&self, schema: &str) -> Option<&LogicalDb> {
        let parsed: SchemaName = schema.parse().ok()?;
        if parsed.tenant() != self.tenant() {
            return None;
        }
        self.database_by_id(parsed.db()?)
    }
}

/// The lowest shard of each table group.
fn representatives(shards: &[SchemaName]) -> Vec<SchemaName> {
    let mut best: Vec<SchemaName> = Vec::new();
    for s in shards {
        let SchemaName::Shard { group, shard, .. } = *s else {
            continue;
        };
        match best
            .iter_mut()
            .find(|b| matches!(b, SchemaName::Shard { group: g, .. } if *g == group))
        {
            Some(b) => {
                if let SchemaName::Shard { shard: cur, .. } = b
                    && shard < *cur
                {
                    *b = *s;
                }
            }
            None => best.push(*s),
        }
    }
    best
}

/// An in-memory [`NameMap`] for tests and for the gateway until the
/// catalog-backed map exists (chapter 08 step 4).
#[derive(Debug, Clone)]
pub struct MemoryNameMap {
    tenant: TenantId,
    version: u64,
    dbs: Vec<LogicalDb>,
    by_name: HashMap<String, usize>,
    by_id: HashMap<DbId, usize>,
}

impl MemoryNameMap {
    /// An empty map for `tenant`.
    #[must_use]
    pub fn new(tenant: TenantId) -> Self {
        Self {
            tenant,
            version: 0,
            dbs: Vec::new(),
            by_name: HashMap::new(),
            by_id: HashMap::new(),
        }
    }

    /// Adds database `name` with id `db` and one shard schema
    /// `t<tenant>_d<db>_g<group>_s<shard>`, replacing any database with the
    /// same name or id.
    #[must_use]
    pub fn with_db(mut self, db: u32, name: &str, group: u32, shard: u32) -> Self {
        let schema = SchemaName::shard(self.tenant, DbId(db), GroupId(group), ShardId(shard));
        self.insert(LogicalDb {
            id: DbId(db),
            name: name.to_owned(),
            shards: vec![schema],
            reference: None,
        });
        self
    }

    /// Sets the catalog version.
    #[must_use]
    pub fn with_version(mut self, version: u64) -> Self {
        self.version = version;
        self
    }

    /// Adds or replaces a database.
    pub fn insert(&mut self, db: LogicalDb) {
        self.remove_name(&db.name);
        if let Some(&i) = self.by_id.get(&db.id) {
            let old = self.dbs[i].name.clone();
            self.remove_name(&old);
        }
        let i = self.dbs.len();
        self.by_name.insert(db.name.clone(), i);
        self.by_id.insert(db.id, i);
        self.dbs.push(db);
    }

    fn remove_name(&mut self, name: &str) {
        if let Some(i) = self.by_name.remove(name) {
            let id = self.dbs[i].id;
            self.by_id.remove(&id);
            self.dbs.remove(i);
            self.reindex();
        }
    }

    fn reindex(&mut self) {
        self.by_name.clear();
        self.by_id.clear();
        for (i, d) in self.dbs.iter().enumerate() {
            self.by_name.insert(d.name.clone(), i);
            self.by_id.insert(d.id, i);
        }
    }

    /// Every database, in insertion order.
    #[must_use]
    pub fn databases(&self) -> &[LogicalDb] {
        &self.dbs
    }
}

impl NameMap for MemoryNameMap {
    fn tenant(&self) -> TenantId {
        self.tenant
    }

    fn version(&self) -> u64 {
        self.version
    }

    fn database(&self, db: &str) -> Option<&LogicalDb> {
        self.by_name.get(db).map(|&i| &self.dbs[i])
    }

    fn database_by_id(&self, id: DbId) -> Option<&LogicalDb> {
        self.by_id.get(&id).map(|&i| &self.dbs[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acme() -> MemoryNameMap {
        MemoryNameMap::new(TenantId(42))
            .with_db(1, "billing", 1, 1)
            .with_db(2, "crm", 2, 2)
    }

    #[test]
    fn forward_and_reverse() {
        let m = acme();
        let s = m.physical("billing", Purpose::Read).unwrap();
        assert_eq!(s.single().unwrap().to_string(), "t42_d1_g1_s1");
        assert_eq!(m.logical("t42_d2_g2_s2").unwrap().name, "crm");
        assert!(m.logical("t43_d2_g2_s2").is_none(), "another tenant");
        assert!(
            m.logical("t42_sprig").is_none(),
            "the catalog is never visible"
        );
        assert!(m.logical("t42_d9_g1_s1").is_none(), "unknown id");
        assert!(m.logical("billing").is_none());
        assert!(matches!(
            m.physical("Billing", Purpose::Read),
            Err(NameError::UnknownDatabase(n)) if n == "Billing"
        ));
    }

    #[test]
    fn purposes_pick_schemas() {
        let mut m = acme();
        let t = TenantId(42);
        m.insert(LogicalDb {
            id: DbId(3),
            name: "sharded".into(),
            shards: vec![
                SchemaName::shard(t, DbId(3), GroupId(1), ShardId(4)),
                SchemaName::shard(t, DbId(3), GroupId(1), ShardId(2)),
                SchemaName::shard(t, DbId(3), GroupId(5), ShardId(9)),
            ],
            reference: Some(SchemaName::reference(t, DbId(3))),
        });
        let ddl = m.physical("sharded", Purpose::Ddl).unwrap();
        assert_eq!(ddl.0.len(), 4);
        assert_eq!(ddl.0[3].to_string(), "t42_d3_ref");
        assert!(ddl.single().is_none());
        let meta = m.physical("sharded", Purpose::Metadata).unwrap();
        let names: Vec<String> = meta.0.iter().map(ToString::to_string).collect();
        assert_eq!(names, ["t42_d3_g1_s2", "t42_d3_g5_s9"]);
        assert_eq!(m.physical("sharded", Purpose::Write).unwrap().0.len(), 1);
    }

    #[test]
    fn replacing_by_name_or_id() {
        let mut m = acme().with_version(7);
        assert_eq!(m.version(), 7);
        m.insert(LogicalDb {
            id: DbId(1),
            name: "ledger".into(),
            shards: vec![SchemaName::shard(
                TenantId(42),
                DbId(1),
                GroupId(1),
                ShardId(1),
            )],
            reference: None,
        });
        assert!(m.database("billing").is_none());
        assert_eq!(m.database_by_id(DbId(1)).unwrap().name, "ledger");
        assert_eq!(m.database("crm").unwrap().id, DbId(2));
        assert_eq!(m.databases().len(), 2);
    }
}
