//! Tenants' shard layouts from the catalog and the shard maps: what the
//! router plans against.
//!
//! - [`ShardingExtension`] loads the sharding catalog module (`sharding`:
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
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigError};
use sprig_meta::{MetaStore, Tier, TierView};
use sprig_router::StaticTopology;
use sprig_router::topology::catalog::{CatalogGroup, CatalogLookup, CatalogTable, Membership};
use sprig_shard::{KeySpec, ShardMapCache};
use sprig_sql::names::GroupedTables;

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
    /// A parent member's `parent_fk` (the constraint to its parent).
    pub parent_fk: Option<String>,
    /// During an online re-key: the new key, this member's
    /// columns for it, and the re-key's first new shard id
    /// (`next_routing`). The router uses them once every range of the
    /// group's map is a new shard: the cutover replaced all of them in one
    /// compare-and-put, before the catalog's key is rewritten.
    pub next: Option<(KeySpec, Vec<String>, u32)>,
}

/// `group_members.next_routing` as a re-key writes it:
/// `{"key_spec": <KeySpec>, "key_columns": [...], "shard_from": n, ...}`.
fn next_routing(json: Option<&str>) -> Option<(KeySpec, Vec<String>, u32)> {
    let v: serde_json::Value = serde_json::from_str(json?).ok()?;
    let spec = KeySpec::from_json(v.get("key_spec")?).ok()?;
    let cols: Vec<String> = serde_json::from_value(v.get("key_columns")?.clone()).ok()?;
    let from = u32::try_from(v.get("shard_from")?.as_u64()?).ok()?;
    (from > 0).then_some((spec, cols, from))
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

/// One `lookup_indexes` row.
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
    /// Lookup indexes (`lookup_indexes`).
    pub lookups: Vec<LookupRow>,
    /// Foreign keys (`foreign_keys`).
    pub fks: Vec<sprig_shard::fk::ForeignKey>,
}

impl ExtState for ShardingState {
    fn bytes(&self) -> usize {
        self.members
            .iter()
            .map(|m| 64 + m.create_sql.as_ref().map_or(0, String::len))
            .sum::<usize>()
            + 64 * (self.groups.len() + self.copies.len())
            + 128 * self.lookups.len()
            + 160 * self.fks.len()
    }
}

/// The sharding catalog module, loaded into snapshots for the router.
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
        let members: Vec<(
            u64,
            u32,
            u32,
            String,
            Option<String>,
            Option<String>,
            String,
            Option<String>,
            Option<String>,
        )> = conn
            .query(format!(
                "SELECT m.object_id, m.group_id, m.db_id, m.routing, CAST(m.key_columns AS CHAR), \
                        v.create_sql, CAST(m.rk_state AS CHAR), m.parent_fk, \
                        CAST(m.next_routing AS CHAR) \
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
        // Lookup indexes, once that table exists.
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
        // Foreign keys: the rows the gateway keeps.
        let has_fks: Option<u64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'foreign_keys'",
                (schema.as_str(),),
            )
            .await
            .map_err(db_err)?;
        let fks: Vec<sprig_shard::fk::FkTuple> = if has_fks.unwrap_or(0) > 0 {
            conn.query(format!(
                "SELECT fk_id, name, child_object_id, CAST(child_columns AS CHAR), \
                        parent_object_id, CAST(parent_columns AS CHAR), on_delete, on_update, \
                        enforcement, child_lookup_id, state \
                 FROM `{schema}`.foreign_keys ORDER BY fk_id"
            ))
            .await
            .map_err(db_err)?
        } else {
            Vec::new()
        };
        let mut state = ShardingState {
            migrated: true,
            fks: fks
                .into_iter()
                .filter_map(sprig_shard::fk::from_tuple)
                .collect(),
            ..ShardingState::default()
        };
        for (id, kind, spec) in groups {
            let spec = spec
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .and_then(|v| KeySpec::from_json(&v).ok());
            state.groups.push(GroupRow { id, kind, spec });
        }
        for (object, group, db, routing, cols, create_sql, rk, parent_fk, next) in members {
            state.members.push(MemberRow {
                next: next_routing(next.as_deref()),
                object,
                group,
                db,
                key_columns: cols.and_then(|c| serde_json::from_str(&c).ok()),
                create_sql,
                rk_fill: rk != "absent",
                parent_fk: (routing == "parent").then_some(parent_fk).flatten(),
                routing,
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
    /// The catalog object of each table the router knows, by its default
    /// schema and name, lowercase (for the foreign-key DDL path).
    pub objects: HashMap<(String, String), u64>,
    /// The metadata placement of the grouped tables, per cluster
    /// a session runs metadata statements on (made at first use).
    pub grouped: Mutex<HashMap<String, Arc<GroupedTables>>>,
}

impl TenantLayout {
    /// Where metadata statements of a session on `cluster` read each
    /// grouped table: its group's lowest
    /// shard on `cluster`, else its group's lowest shard; a reference
    /// table's schema. Statements keep naming the database's own schema;
    /// only `SHOW`, `DESCRIBE` and `information_schema` read these.
    #[must_use]
    pub(crate) fn grouped_tables(&self, cluster: &str) -> Arc<GroupedTables> {
        let mut m = self.grouped.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(g) = m.get(cluster) {
            return Arc::clone(g);
        }
        let mut g = GroupedTables::default();
        for (_, e) in self.topo.entries() {
            let pick = |local: bool| {
                e.group
                    .shards
                    .iter()
                    .filter(|s| !local || s.cluster == cluster)
                    .min_by_key(|s| s.shard)
            };
            let local = pick(true);
            let Some(shard) = local.or_else(|| pick(false)) else {
                continue;
            };
            let Ok(schema) = shard.schema.parse::<SchemaName>() else {
                continue;
            };
            g.add(DbId(e.group.db), &e.name, schema);
            // Its `information_schema` rows are read there.
            if local.is_none() && !e.is_reference() {
                g.set_remote(schema, &shard.cluster);
            }
        }
        let g = Arc::new(g);
        m.insert(cluster.to_owned(), Arc::clone(&g));
        g
    }

    /// The cluster a `SHOW` or `DESCRIBE` of one grouped
    /// table runs on when the table's schema (as [`Self::grouped_tables`]
    /// picked it) has no copy on the session's cluster `main`; `None` when
    /// it may run on `main`.
    #[must_use]
    pub(crate) fn metadata_cluster(&self, sql: &str, main: &str) -> Option<String> {
        let first = sql.split_ascii_whitespace().next()?.to_ascii_uppercase();
        // 3306 answers `SHOW [FULL] COLUMNS`, `DESCRIBE` and `SHOW INDEX`
        // with a `SELECT` over `information_schema` that probes the table
        // (`(SELECT 1 FROM schema.t LIMIT 0) IS NULL`): it runs there too.
        let probe = first == "SELECT" && sql.contains("LIMIT 0) IS NULL");
        if !probe && !matches!(first.as_str(), "SHOW" | "DESCRIBE" | "DESC") {
            return None;
        }
        let mut found: Option<&str> = None;
        for (_, e) in self.topo.entries() {
            for s in &e.group.shards {
                if !sql.contains(&format!("`{}`", s.schema)) {
                    continue;
                }
                if s.cluster == main {
                    return None;
                }
                match found {
                    Some(c) if c != s.cluster => return None,
                    _ => found = Some(&s.cluster),
                }
            }
        }
        found.map(str::to_owned)
    }

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

    /// Whether `cat` has sharded or reference tables (its groups' maps
    /// aside): a layout without any map loaded is still to come.
    pub(crate) fn has_groups(cat: &TenantCatalog) -> bool {
        cat.ext
            .get::<ShardingState>(sprig_shard::groups::MODULE)
            .is_some_and(|s| s.migrated && !s.members.is_empty())
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
    let mut rekeyed: HashSet<u32> = HashSet::new();
    let mut clusters = BTreeSet::new();
    for g in &state.groups {
        if g.kind != "sharded" {
            continue;
        }
        let Some(mut spec) = g.spec.clone() else {
            continue;
        };
        match cache.get(tenant, GroupId(g.id)) {
            Ok(map) => {
                // An online re-key's cutover committed: every
                // range is a new shard, hashed by the new key, before the
                // catalog's key is rewritten; the members' next routing
                // has it.
                if let Some((next, _, _)) = state.members.iter().find_map(|m| {
                    m.next.as_ref().filter(|(_, _, from)| {
                        m.group == g.id && map.ranges.iter().all(|r| r.shard.0 >= *from)
                    })
                }) {
                    spec = next.clone();
                    rekeyed.insert(g.id);
                }
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
    let mut objects = HashMap::new();
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
        let next = m
            .next
            .as_ref()
            .filter(|_| rekeyed.contains(&m.group))
            .map(|(_, cols, _)| cols.clone());
        let member = match kinds.get(&m.group).copied() {
            Some("sharded") => Membership::Sharded {
                group: m.group,
                key_columns: match &next {
                    Some(cols) => Some(cols.clone()),
                    None => (m.routing == "columns")
                        .then(|| m.key_columns.clone())
                        .flatten(),
                },
                create_sql: m.create_sql.clone(),
                fill_rk: m.rk_fill || next.is_some(),
                parent_fk: if next.is_some() {
                    None
                } else {
                    m.parent_fk.clone()
                },
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
        objects.insert(
            (
                default_schema.to_ascii_lowercase(),
                obj.name.to_ascii_lowercase(),
            ),
            m.object,
        );
        tables.push(CatalogTable {
            object: m.object,
            db: m.db,
            default_schema,
            name: obj.name.clone(),
            member,
        });
    }
    let mut topo =
        sprig_router::topology::catalog::build_with_fks(tenant.0, &groups, &tables, &state.fks);
    // Groups without a table the router plans (a graph's
    // group), by id, in their database's schemas (`Plan::Graph`'s shards).
    for g in &groups {
        let used = tables
            .iter()
            .any(|t| matches!(&t.member, Membership::Sharded { group, .. } if *group == g.id));
        if let (false, Some(m)) = (used, state.members.iter().find(|m| m.group == g.id)) {
            topo.insert_group(Arc::new(sprig_router::topology::catalog::numeric_group(
                tenant.0, m.db, g,
            )));
        }
    }
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
        objects,
        grouped: Mutex::new(HashMap::new()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_router::topology::catalog::{CatalogGroup, CatalogTable, Membership, build};
    use sprig_shard::{KeyClass, ShardMap};
    use sprig_sql::names::{NameMap, Purpose};

    fn layout(topo: StaticTopology) -> TenantLayout {
        TenantLayout {
            topo,
            versions: HashMap::new(),
            held: HashMap::new(),
            clusters: BTreeSet::new(),
            missing: Vec::new(),
            epochs: HashMap::new(),
            objects: HashMap::new(),
            grouped: Mutex::new(HashMap::new()),
        }
    }

    /// Group 3 has shards 1 and 3 on c1 and shard 2 on c2: a session on
    /// c2 reads shard 2, one on c1 shard 1, one on a cluster without a
    /// shard the lowest; the reference table reads its schema.
    #[test]
    fn grouped_tables_prefer_the_sessions_cluster() {
        let spec = KeySpec::new(vec![KeyClass::Numeric]).unwrap_or_else(|e| panic!("{e}"));
        let c = |n: &str| ClusterId::new(n).unwrap_or_else(|e| panic!("{e}"));
        let map = ShardMap::presplit(TenantId(7), GroupId(3), spec.digest, 3, &[c("c1"), c("c2")])
            .unwrap_or_else(|e| panic!("{e}"));
        let g = CatalogGroup {
            id: 3,
            spec,
            map: Arc::new(map),
        };
        let sharded = |name: &str| CatalogTable {
            object: 0,
            db: 1,
            default_schema: "t7_d1_g1_s1".into(),
            name: name.into(),
            member: Membership::Sharded {
                group: 3,
                key_columns: None,
                create_sql: None,
                lookups: Vec::new(),
                fill_rk: false,
                parent_fk: None,
            },
        };
        let tables = vec![
            sharded("Orders"),
            sharded("items"),
            CatalogTable {
                object: 0,
                db: 1,
                default_schema: "t7_d1_g1_s1".into(),
                name: "countries".into(),
                member: Membership::Reference {
                    clusters: vec!["c1".into(), "c2".into()],
                },
            },
        ];
        let l = layout(build(7, &[g], &tables));
        let names = |cluster: &str| {
            let g = l.grouped_tables(cluster);
            let w = sprig_sql::names::WithGroups {
                base: &sprig_sql::names::MemoryNameMap::new(TenantId(7)).with_db(1, "shop", 1, 1),
                groups: &g,
            };
            let meta: Vec<String> = w
                .physical("shop", Purpose::Metadata)
                .unwrap_or_else(|e| panic!("{e:?}"))
                .0
                .iter()
                .map(ToString::to_string)
                .collect();
            let orders = w.table_schema(DbId(1), "orders").map(|s| s.to_string());
            (meta, orders)
        };
        let (meta, orders) = names("c2");
        assert_eq!(meta, ["t7_d1_g1_s1", "t7_d1_g3_s2", "t7_d1_ref"]);
        assert_eq!(orders.as_deref(), Some("t7_d1_g3_s2"));
        assert_eq!(names("c1").1.as_deref(), Some("t7_d1_g3_s1"));
        assert_eq!(names("c9").1.as_deref(), Some("t7_d1_g3_s1"));
        assert!(Arc::ptr_eq(
            &l.grouped_tables("c2"),
            &l.grouped_tables("c2")
        ));
        // A SHOW of a table whose schema is only elsewhere runs there.
        let show = "SHOW CREATE TABLE `t7_d1_g3_s2`.orders";
        assert_eq!(l.metadata_cluster(show, "c1").as_deref(), Some("c2"));
        assert_eq!(l.metadata_cluster(show, "c2"), None);
        assert_eq!(
            l.metadata_cluster("DESCRIBE `t7_d1_g3_s2`.orders", "c9")
                .as_deref(),
            Some("c2")
        );
        assert_eq!(
            l.metadata_cluster("SHOW CREATE TABLE `t7_d1_ref`.countries", "c1"),
            None
        );
        assert_eq!(
            l.metadata_cluster("SELECT 1 FROM `t7_d1_g3_s2`.orders", "c1"),
            None
        );
        // 3306's SHOW COLUMNS, DESCRIBE and SHOW INDEX (a probing SELECT).
        let probe = "SELECT * FROM (SELECT `COLUMN_NAME` AS `Field` FROM information_schema.COLUMNS \
                     WHERE `TABLE_SCHEMA` = 't7_d1_g3_s2' AND `TABLE_NAME` = 'orders' \
                     AND (SELECT 1 FROM `t7_d1_g3_s2`.`orders` LIMIT 0) IS NULL) AS `c`";
        assert_eq!(l.metadata_cluster(probe, "c1").as_deref(), Some("c2"));
        assert_eq!(l.metadata_cluster(probe, "c2"), None);
    }
}
