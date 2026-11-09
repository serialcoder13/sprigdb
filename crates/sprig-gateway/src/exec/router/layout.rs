//! Tenants' shard layouts from the catalog and the shard maps (chapter 15,
//! "Interfaces"): what the router plans against.
//!
//! - [`ShardingExtension`] loads chapter 15's catalog module (`sharding`:
//!   `table_groups`, `group_members`, `reference_tables`) into every
//!   catalog snapshot, with each member's `SHOW CREATE TABLE` text from
//!   `schema_versions` (the key's position and unique keys). A change to
//!   membership bumps the catalog version like any other catalog change.
//! - [`Layouts`] keeps one `sprig_shard::ShardMapCache` per DC tier (the
//!   tenant's home), and per tenant the router's [`StaticTopology`] built
//!   from the snapshot and the current maps, rebuilt when the snapshot's
//!   version or the maps change. A split, merge or move reaches the next
//!   statement through the cache's watch.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use mysql_async::prelude::Queryable;
use sprig_catalog::{
    CatalogExtension, ExtState, Migration, SnapshotTx, TenantCatalog, VersionKind,
};
use sprig_common::ids::{ClusterId, DbId, DcId, GroupId, ObjectId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_meta::{MetaStore, Tier, TierView};
use sprig_router::StaticTopology;
use sprig_router::topology::catalog::{CatalogGroup, CatalogLookup, CatalogTable, Membership};
use sprig_shard::{KeySpec, ShardMapCache};

/// One `table_groups` row the router reads.
#[derive(Debug, Clone)]
pub(crate) struct GroupRow {
    /// Group id.
    pub id: u32,
    /// `sharded`, `reference`, ...
    pub kind: String,
    /// The key spec (sharded groups).
    pub spec: Option<KeySpec>,
}

/// One `group_members` row, with the table's definition.
#[derive(Debug, Clone)]
pub(crate) struct MemberRow {
    /// The table.
    pub object: u64,
    /// Its group.
    pub group: u32,
    /// Its database.
    pub db: u32,
    /// `columns`, `parent`, `lookup`, `none`.
    pub routing: String,
    /// Key columns (column members).
    pub key_columns: Option<Vec<String>>,
    /// `SHOW CREATE TABLE` of its current schema version.
    pub create_sql: Option<String>,
    /// `rk_state` is `filling`, `backfilling` or `ready`: writes carry
    /// `_sprig_rk`.
    pub rk_fill: bool,
}

/// One `reference_tables` row.
#[derive(Debug, Clone)]
pub(crate) struct CopyRow {
    /// The table.
    pub object: u64,
    /// The cluster holding the copy.
    pub cluster: String,
    /// `primary` or `copy`.
    pub role: String,
    /// `serving`, `stale`, ...
    pub state: String,
}

/// A `lookup_indexes` row as read.
type LookupTuple = (u64, u64, String, Option<String>, bool, String, u32, String);

/// One `lookup_indexes` row (chapter 19).
#[derive(Debug, Clone)]
pub(crate) struct LookupRow {
    pub id: u64,
    pub object: u64,
    pub index_name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub origin: sprig_shard::lookup::LookupOrigin,
    pub group: u32,
    pub state: sprig_shard::lookup::LookupState,
}

fn lookup_state(s: &str) -> Option<sprig_shard::lookup::LookupState> {
    use sprig_shard::lookup::LookupState as S;
    [
        S::Building,
        S::WriteOnly,
        S::Backfilling,
        S::Public,
        S::Dropping,
    ]
    .into_iter()
    .find(|x| x.as_str() == s)
}

fn lookup_origin(s: &str) -> Option<sprig_shard::lookup::LookupOrigin> {
    use sprig_shard::lookup::LookupOrigin as O;
    [O::UniqueKey, O::PrimaryKey, O::Auto, O::FkChild]
        .into_iter()
        .find(|x| x.as_str() == s)
}

/// The `sharding` module's rows in one catalog snapshot.
#[derive(Debug, Clone, Default)]
pub(crate) struct ShardingState {
    /// The module's tables exist in this catalog.
    pub migrated: bool,
    /// Groups.
    pub groups: Vec<GroupRow>,
    /// Members.
    pub members: Vec<MemberRow>,
    /// Reference copies.
    pub copies: Vec<CopyRow>,
    /// Lookup indexes (chapter 19's `lookup_indexes`).
    pub lookups: Vec<LookupRow>,
}

impl ExtState for ShardingState {
    fn bytes(&self) -> usize {
        self.members
            .iter()
            .map(|m| 64 + m.create_sql.as_ref().map_or(0, String::len))
            .sum::<usize>()
            + 64 * (self.groups.len() + self.copies.len())
            + 128 * self.lookups.len()
    }
}

/// Chapter 15's catalog module, loaded into snapshots for the router.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ShardingExtension;

fn db_err(e: mysql_async::Error) -> SprigError {
    match e {
        mysql_async::Error::Server(s) => {
            let mut st = *b"HY000";
            for (i, b) in s.state.bytes().take(5).enumerate() {
                st[i] = b;
            }
            SprigError::backend(s.code, st, s.message)
        }
        other => SprigError::new(
            sprig_common::SprigCode::BackendUnavailable,
            format!("MariaDB: {other}"),
        ),
    }
}

#[async_trait::async_trait]
impl CatalogExtension for ShardingExtension {
    fn module(&self) -> &'static str {
        sprig_shard::groups::MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        sprig_shard::groups::SHARDING_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }

    #[allow(clippy::too_many_lines)] // one query per catalog table, in order
    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        let schema = tx.schema().replace('`', "``");
        let conn = tx.conn();
        let n: Option<u64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'group_members'",
                (schema.as_str(),),
            )
            .await
            .map_err(db_err)?;
        if n.unwrap_or(0) == 0 {
            return Ok(Arc::new(ShardingState::default()));
        }
        let groups: Vec<(u32, String, Option<String>)> = conn
            .query(format!(
                "SELECT group_id, kind, CAST(key_spec AS CHAR) FROM `{schema}`.table_groups \
                 WHERE state <> 'retired' ORDER BY group_id"
            ))
            .await
            .map_err(db_err)?;
        let members: Vec<(u64, u32, u32, String, Option<String>, Option<String>, String)> = conn
            .query(format!(
                "SELECT m.object_id, m.group_id, m.db_id, m.routing, CAST(m.key_columns AS CHAR), \
                        v.create_sql, CAST(m.rk_state AS CHAR) \
                 FROM `{schema}`.group_members m \
                 LEFT JOIN `{schema}`.objects o ON o.object_id = m.object_id \
                 LEFT JOIN `{schema}`.schema_versions v \
                   ON v.object_id = m.object_id AND v.version = o.schema_version \
                 ORDER BY m.object_id"
            ))
            .await
            .map_err(db_err)?;
        let copies: Vec<(u64, String, String, String)> = conn
            .query(format!(
                "SELECT object_id, cluster_id, role, state FROM `{schema}`.reference_tables \
                 ORDER BY object_id, role DESC, cluster_id"
            ))
            .await
            .map_err(db_err)?;
        // Chapter 19's lookup indexes, once that table exists.
        let has_lookups: Option<u64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'lookup_indexes'",
                (schema.as_str(),),
            )
            .await
            .map_err(db_err)?;
        let lookups: Vec<LookupTuple> = if has_lookups.unwrap_or(0) > 0 {
            conn.query(format!(
                "SELECT lookup_id, object_id, index_name, CAST(columns AS CHAR), is_unique, \
                        origin, group_id, state \
                 FROM `{schema}`.lookup_indexes ORDER BY lookup_id"
            ))
            .await
            .map_err(db_err)?
        } else {
            Vec::new()
        };
        let mut state = ShardingState {
            migrated: true,
            ..ShardingState::default()
        };
        for (id, kind, spec) in groups {
            let spec = spec
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .and_then(|v| KeySpec::from_json(&v).ok());
            state.groups.push(GroupRow { id, kind, spec });
        }
        for (object, group, db, routing, cols, create_sql, rk) in members {
            state.members.push(MemberRow {
                object,
                group,
                db,
                routing,
                key_columns: cols.and_then(|c| serde_json::from_str(&c).ok()),
                create_sql,
                rk_fill: rk != "absent",
            });
        }
        for (id, object, index_name, cols, unique, origin, group, st) in lookups {
            let (Some(origin), Some(lstate)) = (lookup_origin(&origin), lookup_state(&st)) else {
                continue;
            };
            state.lookups.push(LookupRow {
                id,
                object,
                index_name,
                columns: cols
                    .and_then(|c| serde_json::from_str(&c).ok())
                    .unwrap_or_default(),
                unique,
                origin,
                group,
                state: lstate,
            });
        }
        for (object, cluster, role, st) in copies {
            state.copies.push(CopyRow {
                object,
                cluster,
                role,
                state: st,
            });
        }
        Ok(Arc::new(state))
    }
}

/// One tenant's layout, as the router reads it.
#[derive(Debug)]
pub(crate) struct TenantLayout {
    /// The router's view.
    pub topo: StaticTopology,
    /// The maps' versions it was built from, per sharded group.
    pub versions: HashMap<u32, u64>,
    /// Tables whose group has a range a mover cutover holds, with the
    /// latest hold deadline (ms since the epoch).
    pub held: HashMap<String, u64>,
    /// Every cluster holding the tenant's shards or reference copies.
    pub clusters: BTreeSet<String>,
    /// Groups the catalog names whose map isn't cached yet.
    pub missing: Vec<u32>,
    /// Each shard schema's range epoch (raised by every split, merge and
    /// move): a transaction that used a shard whose epoch moved is
    /// rolled back with 39424.
    pub epochs: HashMap<String, u64>,
}

impl TenantLayout {
    /// Whether nothing is sharded or referenced.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.topo.is_empty()
    }

    /// The latest hold deadline among the tables `sql` names, if any.
    #[must_use]
    pub(crate) fn hold_for(&self, sql: &str) -> Option<u64> {
        if self.held.is_empty() {
            return None;
        }
        let mut best: Option<u64> = None;
        for w in sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '$')) {
            if let Some(u) = self.held.get(&w.to_ascii_lowercase()) {
                best = Some(best.map_or(*u, |b| b.max(*u)));
            }
        }
        best
    }
}

type Cached = (u64, u64, Arc<TenantLayout>);

/// The gateway's layouts (see the module docs).
pub(crate) struct Layouts {
    dc: Arc<dyn MetaStore>,
    my_dc: DcId,
    caches: Mutex<HashMap<DcId, ShardMapCache>>,
    tenants: Mutex<HashMap<TenantId, Cached>>,
}

impl std::fmt::Debug for Layouts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Layouts")
            .field("dc", &self.my_dc)
            .finish_non_exhaustive()
    }
}

impl Layouts {
    /// Layouts over this DC's tier `dc`.
    #[must_use]
    pub(crate) fn new(dc: Arc<dyn MetaStore>, my_dc: DcId) -> Self {
        Self {
            dc,
            my_dc,
            caches: Mutex::new(HashMap::new()),
            tenants: Mutex::new(HashMap::new()),
        }
    }

    /// The map cache of DC `home` (started at first use).
    ///
    /// # Errors
    /// `Internal` never in practice.
    pub(crate) fn cache(&self, home: &DcId) -> Result<ShardMapCache> {
        let mut c = self.caches.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(x) = c.get(home) {
            return Ok(x.clone());
        }
        let store = if *home == self.my_dc {
            Arc::clone(&self.dc)
        } else {
            TierView::over(&self.dc, Tier::Dc(home.clone()))
        };
        let cache = ShardMapCache::spawn(store)?;
        c.insert(home.clone(), cache.clone());
        Ok(cache)
    }

    /// A linearizable map store over DC `home`'s tier (stale-map checks).
    #[must_use]
    pub(crate) fn store(&self, home: &DcId) -> sprig_shard::ShardMapStore {
        if *home == self.my_dc {
            sprig_shard::ShardMapStore::new(Arc::clone(&self.dc))
        } else {
            sprig_shard::ShardMapStore::new(TierView::over(&self.dc, Tier::Dc(home.clone())))
        }
    }

    /// `cat`'s layout with the maps of `cache` now; `None` for a tenant
    /// without sharded or reference tables.
    pub(crate) fn get(
        &self,
        cat: &TenantCatalog,
        cache: &ShardMapCache,
    ) -> Option<Arc<TenantLayout>> {
        let state = cat.ext.get::<ShardingState>(sprig_shard::groups::MODULE)?;
        if !state.migrated || state.members.is_empty() {
            return None;
        }
        let generation = *cache.changes().borrow();
        if let Some((v, g, l)) = self
            .tenants
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&cat.tenant)
            && *v == cat.version
            && *g == generation
        {
            return (!l.is_empty()).then(|| Arc::clone(l));
        }
        let built = Arc::new(build(cat, state, cache));
        self.tenants
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(cat.tenant, (cat.version, generation, Arc::clone(&built)));
        (!built.is_empty()).then_some(built)
    }
}

#[allow(clippy::too_many_lines)] // groups, members, lookups, then epochs
fn build(cat: &TenantCatalog, state: &ShardingState, cache: &ShardMapCache) -> TenantLayout {
    let tenant = cat.tenant;
    let mut groups: Vec<CatalogGroup> = Vec::new();
    let mut versions = HashMap::new();
    let mut missing = Vec::new();
    let mut held_groups: HashMap<u32, u64> = HashMap::new();
    let mut clusters = BTreeSet::new();
    for g in &state.groups {
        if g.kind != "sharded" {
            continue;
        }
        let Some(spec) = g.spec.clone() else { continue };
        match cache.get(tenant, GroupId(g.id)) {
            Ok(map) => {
                versions.insert(g.id, map.version);
                for r in &map.ranges {
                    clusters.insert(r.cluster.to_string());
                    if r.state == sprig_shard::shardmap::RangeState::Held {
                        let u = r.hold_until_ms.unwrap_or(0);
                        let e = held_groups.entry(g.id).or_insert(u);
                        *e = (*e).max(u);
                    }
                }
                groups.push(CatalogGroup {
                    id: g.id,
                    spec,
                    map,
                });
            }
            Err(_) => missing.push(g.id),
        }
    }
    let kinds: HashMap<u32, &str> = state
        .groups
        .iter()
        .map(|g| (g.id, g.kind.as_str()))
        .collect();
    let mut tables = Vec::new();
    let mut held = HashMap::new();
    let mut seen = HashSet::new();
    for m in &state.members {
        let Some(obj) = cat.objects.get(ObjectId(m.object)) else {
            continue;
        };
        let Some(db) = cat.databases.get(DbId(m.db)) else {
            continue;
        };
        let Some(default_schema) = sprig_catalog::ddl::shard_schemas(tenant, db)
            .first()
            .map(ToString::to_string)
        else {
            continue;
        };
        if !seen.insert(m.object) {
            continue;
        }
        let member = match kinds.get(&m.group).copied() {
            Some("sharded") => Membership::Sharded {
                group: m.group,
                key_columns: (m.routing == "columns")
                    .then(|| m.key_columns.clone())
                    .flatten(),
                create_sql: m.create_sql.clone(),
                fill_rk: m.rk_fill,
                lookups: state
                    .lookups
                    .iter()
                    .filter(|l| l.object == m.object)
                    .map(|l| {
                        let map = cache.get(tenant, GroupId(l.group)).ok();
                        if let Some(map) = &map {
                            versions.insert(l.group, map.version);
                            clusters.extend(map.ranges.iter().map(|r| r.cluster.to_string()));
                        } else if l.state.maintained() {
                            missing.push(l.group);
                        }
                        CatalogLookup {
                            id: l.id,
                            owner: l.object,
                            index_name: l.index_name.clone(),
                            columns: l.columns.clone(),
                            unique: l.unique,
                            origin: l.origin,
                            state: l.state,
                            map,
                        }
                    })
                    .collect(),
            },
            Some("reference") => {
                let cl: Vec<String> = state
                    .copies
                    .iter()
                    .filter(|c| {
                        c.object == m.object && (c.role == "primary" || c.state == "serving")
                    })
                    .map(|c| c.cluster.clone())
                    .collect();
                clusters.extend(cl.iter().cloned());
                Membership::Reference { clusters: cl }
            }
            _ => continue,
        };
        if let Some(u) = held_groups.get(&m.group) {
            held.insert(obj.name.to_ascii_lowercase(), *u);
        }
        tables.push(CatalogTable {
            db: m.db,
            default_schema,
            name: obj.name.clone(),
            member,
        });
    }
    let topo = sprig_router::topology::catalog::build(tenant.0, &groups, &tables);
    let mut epochs = HashMap::new();
    for (_, e) in topo.entries() {
        if e.is_reference() {
            continue;
        }
        for sh in &e.group.shards {
            epochs.insert(sh.schema.clone(), sh.epoch);
        }
    }
    TenantLayout {
        topo,
        versions,
        held,
        clusters,
        missing,
        epochs,
    }
}

/// The cluster ids of `layout`, typed.
#[must_use]
pub(crate) fn cluster_ids(layout: &TenantLayout) -> Vec<ClusterId> {
    layout
        .clusters
        .iter()
        .filter_map(|c| ClusterId::new(c).ok())
        .collect()
}
