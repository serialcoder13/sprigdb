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

    /// Every database of the tenant, in the catalog's order.
    fn list(&self) -> Vec<&LogicalDb>;

    /// The representative schemas of `db`'s other table groups (and its
    /// reference schema), for metadata: the advisor moves a grouped table
    /// out of the database's first shard into its group's schemas (chapter
    /// 16), so `Purpose::Metadata` lists one schema per group. Empty when
    /// nothing of `db` is grouped.
    fn grouped_schemas(&self, _db: DbId) -> &[SchemaName] {
        &[]
    }

    /// The representative schema of table `table` (matched ignoring case,
    /// as the router does) of `db` when it is a member of a table group or
    /// a reference table; `None` when it is in the database's own schema.
    fn table_schema(&self, _db: DbId, _table: &str) -> Option<SchemaName> {
        None
    }

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
            Purpose::Metadata => {
                let mut v = representatives(&ldb.shards);
                for s in self.grouped_schemas(ldb.id) {
                    if !v.contains(s) {
                        v.push(*s);
                    }
                }
                v
            }
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

/// Where a tenant's grouped tables are, for metadata (chapter 07,
/// "Physical names"; R1). Built by the gateway from the router's layout
/// for the cluster a session's metadata statements run on: per group, its
/// lowest shard on that cluster, else its lowest shard.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupedTables {
    /// Per database: one representative schema per group, each once,
    /// sorted by name.
    schemas: HashMap<DbId, Vec<SchemaName>>,
    /// Per database and lowercase table name: its group's representative.
    tables: HashMap<(DbId, String), SchemaName>,
}

impl GroupedTables {
    /// Records that table `table` of `db` is read, for metadata, in
    /// `schema`.
    pub fn add(&mut self, db: DbId, table: &str, schema: SchemaName) {
        // Sorted by name, so the order never depends on how they came.
        let v = self.schemas.entry(db).or_default();
        let key = schema.to_string();
        if let Err(i) = v.binary_search_by(|s| s.to_string().cmp(&key)) {
            v.insert(i, schema);
        }
        self.tables.insert((db, table.to_lowercase()), schema);
    }

    /// Whether no table is grouped.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

/// A [`NameMap`] that adds a tenant's [`GroupedTables`] to `base`.
pub struct WithGroups<'a> {
    /// The catalog's map.
    pub base: &'a dyn NameMap,
    /// The grouped tables.
    pub groups: &'a GroupedTables,
}

impl NameMap for WithGroups<'_> {
    fn tenant(&self) -> TenantId {
        self.base.tenant()
    }

    fn version(&self) -> u64 {
        self.base.version()
    }

    fn database(&self, db: &str) -> Option<&LogicalDb> {
        self.base.database(db)
    }

    fn database_by_id(&self, id: DbId) -> Option<&LogicalDb> {
        self.base.database_by_id(id)
    }

    fn list(&self) -> Vec<&LogicalDb> {
        self.base.list()
    }

    fn grouped_schemas(&self, db: DbId) -> &[SchemaName] {
        self.groups.schemas.get(&db).map_or(&[], Vec::as_slice)
    }

    fn table_schema(&self, db: DbId, table: &str) -> Option<SchemaName> {
        self.groups
            .tables
            .get(&(db, table.to_lowercase()))
            .copied()
    }
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

    fn list(&self) -> Vec<&LogicalDb> {
        self.dbs.iter().collect()
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
    fn grouped_tables_add_metadata_schemas() {
        let m = acme();
        let t = TenantId(42);
        let mut g = GroupedTables::default();
        let g3 = SchemaName::shard(t, DbId(1), GroupId(3), ShardId(2));
        g.add(DbId(1), "Orders", g3);
        g.add(DbId(1), "order_items", g3);
        g.add(DbId(1), "countries", SchemaName::reference(t, DbId(1)));
        let w = WithGroups {
            base: &m,
            groups: &g,
        };
        let names = |p| -> Vec<String> {
            w.physical("billing", p)
                .unwrap()
                .0
                .iter()
                .map(ToString::to_string)
                .collect()
        };
        assert_eq!(
            names(Purpose::Metadata),
            ["t42_d1_g1_s1", "t42_d1_g3_s2", "t42_d1_ref"]
        );
        // Statements keep the database's own schema.
        assert_eq!(names(Purpose::Read), ["t42_d1_g1_s1"]);
        assert_eq!(names(Purpose::Ddl), ["t42_d1_g1_s1"]);
        assert_eq!(w.table_schema(DbId(1), "orders"), Some(g3));
        assert_eq!(w.table_schema(DbId(1), "invoices"), None);
        assert_eq!(w.table_schema(DbId(2), "orders"), None);
        // Another database is unchanged.
        assert_eq!(
            w.physical("crm", Purpose::Metadata).unwrap().0.len(),
            1,
            "crm has no grouped tables"
        );
        assert!(m.table_schema(DbId(1), "orders").is_none());
        assert_eq!(m.physical("billing", Purpose::Metadata).unwrap().0.len(), 1);
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
