//! What the inheritance layer needs beyond the session (Kiran's decisions
//! of 11 November 2026, chapter 42): the tree-wide id sequences, the
//! reference index, reads as the provisioning account, the tenants of a
//! tree, and bringing a child tenant's tables up to its parent's
//! ([`adopt`]).
//!
//! ```text
//! tree sequence  t<root>_sprig.sequences, seq_id = root object | 1<<62,
//!                chapter 19's ID blocks (one allocator per root tenant
//!                in this gateway); created at first use above every
//!                tenant's MAX(id) and AUTO_INCREMENT in the root's subtree
//! inherit_refs   t<root>_sprig.inherit_refs (created at first use)
//! adopt          before a tenant statement: for each tenant on the
//!                session's line, top first, what sprig_inherit::adopt::
//!                wanted says is missing is created or added, as the
//!                provisioning account through the DDL engine, and the
//!                child's inherit module records it
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use mysql_async::prelude::Queryable;
use sprig_catalog::ddl::{DdlRequest, DdlStatement, MariaExecutor, ObjectName, database_key};
use sprig_catalog::store::ObjectKind;
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, Freshness, TenantCatalog};
use sprig_common::ids::{ObjectId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use sprig_inherit::ids::{TreeSeq, TreeTable};
use sprig_inherit::model::InheritableObject;
use sprig_inherit::{InheritChange, InheritStore, RawRows};
use sprig_meta::{ListOpts, Record};
use sprig_shard::ids::{
    AutoIncSession, BLOCK_LEASE, BlockAllocator, BlockConfig, CatalogSequences, Explicit,
    IdAllocator, Ordering, SeqId, SequenceSource,
};

use crate::inherited::DirEntry;
use crate::tenant::TenantServices;

fn q(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

fn server_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let mut state = *b"HY000";
            if s.state.len() == 5 {
                state.copy_from_slice(s.state.as_bytes());
            }
            SprigError::backend(s.code, state, format!("{} ({})", s.message, s.code))
        }
        other => SprigError::new(SprigCode::BackendUnavailable, other.to_string()),
    }
    .with_source(e)
}

fn bytes_of(v: mysql_async::Value) -> Option<Vec<u8>> {
    match v {
        mysql_async::Value::NULL => None,
        mysql_async::Value::Bytes(b) => Some(b),
        mysql_async::Value::Int(i) => Some(i.to_string().into_bytes()),
        mysql_async::Value::UInt(u) => Some(u.to_string().into_bytes()),
        mysql_async::Value::Float(f) => Some(f.to_string().into_bytes()),
        mysql_async::Value::Double(f) => Some(f.to_string().into_bytes()),
        other => Some(other.as_sql(true).into_bytes()),
    }
}

/// Runs `sql` as the provisioning account on tenant `t`'s cluster.
///
/// # Errors
/// The backend's (server errors keep their number).
pub(crate) async fn ctl_query(svc: &TenantServices, t: TenantId, sql: &str) -> Result<RawRows> {
    let cluster = svc.locator.cluster(t).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let mut conn = pool.get_conn().await.map_err(server_err)?;
    let rows: Vec<mysql_async::Row> = conn.query(sql).await.map_err(server_err)?;
    Ok(rows
        .into_iter()
        .map(|r| r.unwrap().into_iter().map(bytes_of).collect())
        .collect())
}

fn refs_ready() -> &'static Mutex<HashSet<u64>> {
    static R: OnceLock<Mutex<HashSet<u64>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Runs `sql` on `inherit_refs` in tenant `root`'s catalog, creating the
/// table when this gateway hasn't seen it yet.
///
/// # Errors
/// The catalog's.
pub(crate) async fn refs(svc: &TenantServices, root: TenantId, sql: &str) -> Result<RawRows> {
    let table = format!(
        "{}.`inherit_refs`",
        q(&SchemaName::catalog(root).to_string())
    );
    let ready = refs_ready()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&root.0);
    if !ready {
        ctl_query(
            svc,
            root,
            &sprig_inherit::links::DDL_REFS.replace("{refs}", &table),
        )
        .await?;
        refs_ready()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(root.0);
    }
    ctl_query(svc, root, &sql.replace("{refs}", &table)).await
}

/// Every active tenant of the directory, by id.
async fn directory(svc: &TenantServices) -> Result<BTreeMap<u64, DirEntry>> {
    let prefix = svc.global.tier().key("tenants/")?;
    let page = svc
        .global
        .list(
            &prefix,
            ListOpts {
                limit: 10_000_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| Record::<DirEntry>::decode(&kv.value).ok().map(|r| r.body))
        .filter(|e| e.state == "active" && e.tenant_id != 0)
        .map(|e| (e.tenant_id, e))
        .collect())
}

/// Every tenant in `root`'s subtree holding `db.table`.
///
/// # Errors
/// The directory's and the cache's.
pub(crate) async fn tree_tables(
    svc: &TenantServices,
    root: TenantId,
    db: &str,
    table: &str,
    lctn: bool,
) -> Result<Vec<TreeTable>> {
    let mut out = Vec::new();
    for (id, e) in directory(svc).await? {
        let path = e.path_ids();
        if !path.contains(&root.0) {
            continue;
        }
        let t = TenantId(id);
        let Ok(cat) = svc.cache.get(t, Freshness::Latest).await else {
            continue;
        };
        let Some(d) = cat.database(&database_key(db, lctn)) else {
            continue;
        };
        let key = sprig_catalog::ddl::object_key(ObjectKind::Table, table, lctn);
        if cat
            .object(d.db_id, sprig_catalog::store::Namespace::Relation, &key)
            .is_none()
        {
            continue;
        }
        let Ok(schema) = sprig_inherit::plan::physical_schema(&cat, db, lctn) else {
            continue;
        };
        out.push(TreeTable {
            tenant: t,
            name: e.name.clone(),
            ancestors: path
                .iter()
                .filter(|x| **x != id)
                .map(|x| TenantId(*x))
                .collect(),
            schema,
        });
    }
    Ok(out)
}

// ------------------------------------------------------- tree sequences

/// The sequences of one root tenant's catalog.
struct TreeSource {
    svc: Arc<TenantServices>,
    root: TenantId,
    holder: String,
}

impl TreeSource {
    async fn catalog(&self) -> Result<CatalogSequences> {
        let cluster = self.svc.locator.cluster(self.root).await?;
        let pool = self.svc.locator.ctl_pool(&cluster).await?;
        Ok(
            CatalogSequences::new(pool, SchemaName::catalog(self.root).to_string())
                .held_by(self.holder.clone(), BLOCK_LEASE),
        )
    }
}

#[async_trait::async_trait]
impl SequenceSource for TreeSource {
    async fn fetch(&self, seq: SeqId, size: u128) -> Result<(i128, i128)> {
        let r = self.catalog().await?.fetch(seq, size).await;
        if r.is_ok() {
            metrics::counter!("sprig_ids_blocks_fetched_total", "kind" => "inherit_tree")
                .increment(1);
        }
        r
    }

    async fn raise(&self, seq: SeqId, v: i128) -> Result<()> {
        self.catalog().await?.raise(seq, v).await
    }

    async fn ordering(&self, seq: SeqId) -> Result<Ordering> {
        self.catalog().await?.ordering(seq).await
    }

    fn lease(&self) -> Option<std::time::Duration> {
        Some(BLOCK_LEASE)
    }

    async fn explicit(&self, seq: SeqId, v: i128) -> Result<Explicit> {
        self.catalog().await?.explicit(seq, v).await
    }
}

struct RootIds {
    alloc: BlockAllocator<TreeSource>,
    ready: Mutex<HashSet<u64>>,
    creating: tokio::sync::Mutex<()>,
}

fn roots() -> &'static Mutex<HashMap<u64, Arc<RootIds>>> {
    static R: OnceLock<Mutex<HashMap<u64, Arc<RootIds>>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashMap::new()))
}

fn root_ids(svc: &Arc<TenantServices>, root: TenantId) -> Arc<RootIds> {
    let mut m = roots().lock().unwrap_or_else(PoisonError::into_inner);
    Arc::clone(m.entry(root.0).or_insert_with(|| {
        Arc::new(RootIds {
            alloc: BlockAllocator::new(
                TreeSource {
                    svc: Arc::clone(svc),
                    root,
                    holder: format!("gw-inherit-{}", std::process::id()),
                },
                BlockConfig::default(),
            ),
            ready: Mutex::new(HashSet::new()),
            creating: tokio::sync::Mutex::new(()),
        })
    }))
}

/// The first value of a new tree sequence: above every value and
/// `AUTO_INCREMENT` counter of the table in the root's subtree. Ids that
/// more than one tenant holds as its own are counted and logged (`SPRIG
/// CHECK IDS` lists them); none is renumbered.
async fn first_value(svc: &TenantServices, seq: &TreeSeq, lctn: bool) -> Result<i128> {
    let mut next: i128 = 1;
    let mut holders: HashMap<String, usize> = HashMap::new();
    for tt in tree_tables(svc, seq.root, &seq.db, &seq.table, lctn).await? {
        let rows = ctl_query(
            svc,
            tt.tenant,
            &format!(
                "SELECT CAST(GREATEST(COALESCE((SELECT MAX({col}) FROM {s}.{t}), 0) + 1, \
                 COALESCE((SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE \
                 TABLE_SCHEMA = '{sn}' AND TABLE_NAME = '{tn}'), 1)) AS CHAR)",
                col = q(&seq.column),
                s = q(&tt.schema),
                t = q(&seq.table),
                sn = tt.schema.replace('\'', "''"),
                tn = seq.table.replace('\'', "''"),
            ),
        )
        .await?;
        let v: i128 = rows
            .first()
            .and_then(|r| r.first().cloned().flatten())
            .and_then(|b| String::from_utf8_lossy(&b).parse().ok())
            .unwrap_or(1);
        next = next.max(v);
        let own = ctl_query(
            svc,
            tt.tenant,
            &format!(
                "SELECT DISTINCT CAST({col} AS CHAR) FROM {s}.{t} WHERE `_sprig_state` IN \
                 ('own', 'orphan')",
                col = q(&seq.column),
                s = q(&tt.schema),
                t = q(&seq.table),
            ),
        )
        .await
        .unwrap_or_default();
        for r in own {
            if let Some(Some(b)) = r.first() {
                *holders
                    .entry(String::from_utf8_lossy(b).into_owned())
                    .or_default() += 1;
            }
        }
    }
    let collisions = holders.values().filter(|n| **n > 1).count();
    if collisions > 0 {
        metrics::counter!("sprig_inherit_id_collisions_total").increment(collisions as u64);
        tracing::warn!(
            root = %seq.root,
            table = %seq.table,
            collisions,
            "ids held by more than one tenant of the tree (SPRIG CHECK IDS lists them; none \
             renumbered)"
        );
    }
    Ok(next)
}

/// `n` values of `seq`, after moving it past `explicit`.
///
/// # Errors
/// 39460, 39463, 1467.
pub(crate) async fn tree_ids(
    svc: &Arc<TenantServices>,
    seq: &TreeSeq,
    n: u64,
    explicit: Option<i128>,
    s: &AutoIncSession,
    lctn: bool,
) -> Result<Option<(i128, i128)>> {
    let ids = root_ids(svc, seq.root);
    let known = |x: &RootIds| {
        x.ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&seq.seq)
    };
    if !known(&ids) {
        let _one = ids.creating.lock().await;
        if !known(&ids) {
            let cat = ids.alloc.source().catalog().await?;
            cat.ensure_tables().await?;
            if !cat.exists(SeqId(seq.seq)).await? {
                let next = first_value(svc, seq, lctn).await?;
                match cat
                    .create(
                        SeqId(seq.seq),
                        seq.root_object.0,
                        Some(&seq.column),
                        next,
                        Ordering::Blocks,
                    )
                    .await
                {
                    Ok(()) => tracing::info!(
                        root = %seq.root,
                        table = %seq.table,
                        next,
                        "tree id sequence created"
                    ),
                    Err(e) if matches!(e.kind, ErrorKind::Backend { errno: 1062, .. }) => {}
                    Err(e) => return Err(e),
                }
            }
            ids.ready
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(seq.seq);
        }
    }
    if let Some(v) = explicit {
        ids.alloc.explicit(SeqId(seq.seq), v).await?;
    }
    if n == 0 {
        return Ok(Some((0, 1)));
    }
    let run = ids.alloc.take(SeqId(seq.seq), n, s).await?;
    Ok(Some((run.first, run.step)))
}

// ------------------------------------------------------------- adoption

/// What this gateway last found up to date: tenant → (its version, its
/// parent's version).
fn synced() -> &'static Mutex<HashMap<u64, (u64, u64)>> {
    static S: OnceLock<Mutex<HashMap<u64, (u64, u64)>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn ddl(
    svc: &TenantServices,
    t: TenantId,
    statement: DdlStatement,
    sql: &str,
    logical: &str,
    lctn: bool,
) -> Result<Option<u64>> {
    let cluster = svc.locator.cluster(t).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let req = DdlRequest {
        tenant: t,
        statement,
        sql,
        logical_sql: logical,
        current_db: None,
        user: None,
        lower_case_table_names: lctn,
    };
    let out = svc.ddl.execute(&req, &MariaExecutor::new(pool)).await?;
    Ok(out.version)
}

async fn show_create(
    svc: &TenantServices,
    t: TenantId,
    schema: &str,
    table: &str,
) -> Result<String> {
    let rows = ctl_query(
        svc,
        t,
        &format!("SHOW CREATE TABLE {}.{}", q(schema), q(table)),
    )
    .await?;
    rows.first()
        .and_then(|r| r.get(1).cloned().flatten())
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .ok_or_else(|| SprigError::internal(format!("no SHOW CREATE TABLE for {table}")))
}

/// Commits `change` to tenant `t`'s inherit module.
async fn commit(svc: &TenantServices, t: TenantId, change: &InheritChange) -> Result<u64> {
    let ep = svc.locator.writer(t).await?;
    {
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        sprig_inherit::store::migrate(&mut conn, ep.store.schema()).await?;
    }
    let mut c = svc.catalog.change(t, VersionKind::Catalog).await?;
    let version = c.version();
    let applied = {
        let (store, tx) = c.tx().parts();
        InheritStore::new(store.schema())
            .apply(tx, change, version)
            .await
    };
    if let Err(e) = applied {
        let _ = c.rollback().await;
        return Err(e);
    }
    c.commit().await
}

/// Brings `child` up to its parent: the catalog version when anything
/// changed.
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the steps in order
async fn sync_one(
    svc: &Arc<TenantServices>,
    parent: &TenantCatalog,
    child: &Arc<TenantCatalog>,
    lctn: bool,
) -> Result<Option<u64>> {
    let wants = sprig_inherit::adopt::wanted(parent, child, lctn);
    if wants.iter().all(sprig_inherit::adopt::Want::done) {
        return Ok(None);
    }
    let t = child.tenant;
    let mut ver = child.version;
    // Physical changes first.
    for w in wants.iter().filter(|w| !w.done()) {
        let pschema = sprig_inherit::plan::physical_schema(parent, &w.db, lctn)?;
        let text = show_create(svc, parent.tenant, &pschema, &w.table).await?;
        if w.child.is_none() {
            let cat = crate::inherited::streams::ensure_database(svc, t, &w.db).await?;
            let cschema = sprig_inherit::plan::physical_schema(&cat, &w.db, lctn)?;
            let target = format!("{}.{}", q(&cschema), q(&w.table));
            let sql = sprig_inherit::adopt::child_create(&text, &target);
            let logical =
                sprig_inherit::adopt::child_create(&text, &format!("{}.{}", q(&w.db), q(&w.table)));
            let v = ddl(
                svc,
                t,
                DdlStatement::CreateObject {
                    kind: ObjectKind::Table,
                    name: ObjectName {
                        db: Some(w.db.clone()),
                        name: w.table.clone(),
                    },
                    if_not_exists: true,
                    or_replace: false,
                    parent: None,
                },
                &sql,
                &logical,
                lctn,
            )
            .await?;
            ver = ver.max(v.unwrap_or(0));
            metrics::counter!("sprig_inherit_adopted_tables_total").increment(1);
            tracing::info!(tenant = %t, db = %w.db, table = %w.table, "inherited table created");
        } else if !w.missing.is_empty() {
            let cschema = sprig_inherit::plan::physical_schema(child, &w.db, lctn)?;
            let adds: Vec<String> = w
                .missing
                .iter()
                .filter_map(|c| sprig_inherit::adopt::column_def(&text, c))
                .map(|d| format!("ADD COLUMN IF NOT EXISTS {d}"))
                .collect();
            if !adds.is_empty() {
                let sql = format!(
                    "ALTER TABLE {}.{} {}",
                    q(&cschema),
                    q(&w.table),
                    adds.join(", ")
                );
                let logical = format!(
                    "ALTER TABLE {}.{} {}",
                    q(&w.db),
                    q(&w.table),
                    adds.join(", ")
                );
                let v = ddl(
                    svc,
                    t,
                    DdlStatement::AlterObject {
                        kind: ObjectKind::Table,
                        name: ObjectName {
                            db: Some(w.db.clone()),
                            name: w.table.clone(),
                        },
                        if_exists: false,
                        rename_to: None,
                    },
                    &sql,
                    &logical,
                    lctn,
                )
                .await?;
                ver = ver.max(v.unwrap_or(0));
                tracing::info!(tenant = %t, table = %w.table, columns = ?w.missing, "inherited columns added");
            }
        }
    }
    // Then the inherit module, from a fresh snapshot.
    let fresh = svc
        .cache
        .get(t, Freshness::Cached { min_version: ver })
        .await?;
    let mut change = InheritChange {
        root_name: None,
        ..InheritChange::default()
    };
    for w in wants.iter().filter(|w| !w.done()) {
        let Some(d) = fresh.database(&database_key(&w.db, lctn)) else {
            continue;
        };
        let key = sprig_catalog::ddl::object_key(ObjectKind::Table, &w.table, lctn);
        let Some(row) = fresh.object(d.db_id, sprig_catalog::store::Namespace::Relation, &key)
        else {
            continue;
        };
        let p = &w.parent;
        #[allow(clippy::single_match_else)] // two ways to the same object
        let o = match &w.child {
            Some(c) => {
                let mut o = (**c).clone();
                let mut cols = o.columns.clone();
                for m in &w.missing {
                    if let Some(pc) = p.column(m) {
                        cols.push(pc.to_owned());
                    }
                }
                if let Some(n) = o.with_slots_for(&cols) {
                    o = n;
                }
                o.links = sprig_inherit::adopt::merged_links(&p.links, &o.links);
                if o.auto.is_none() {
                    o.auto.clone_from(&p.auto);
                }
                o
            }
            None => {
                let empty = InheritableObject {
                    object_id: ObjectId(row.object_id.0),
                    db: d.db_id,
                    table: w.table.clone(),
                    key: p.key.clone(),
                    columns: Vec::new(),
                    slots: std::collections::BTreeMap::new(),
                    uniques: p.uniques.clone(),
                    tag_field: p.tag_field.clone(),
                    version: 1,
                    auto: p.auto.clone(),
                    links: p.links.clone(),
                    adopted: true,
                };
                empty.with_slots_for(&p.columns).unwrap_or(empty)
            }
        };
        change.upsert_objects.push(o);
    }
    if change.upsert_objects.is_empty() {
        return Ok((ver > child.version).then_some(ver));
    }
    if sprig_inherit::plan::state_of(child).is_none_or(|s| !s.migrated) {
        let name = crate::inherited::tenant_name(svc, t)
            .await
            .unwrap_or_else(|_| t.to_string());
        change.root_name = Some(name);
    }
    let v = commit(svc, t, &change).await?;
    Ok(Some(v.max(ver)))
}

/// Brings the session tenant (`cat`) and each tenant above it up to its
/// parent, top first; the session tenant's fresh snapshot when anything
/// changed.
///
/// # Errors
/// The steps'.
pub(crate) async fn adopt(
    svc: &Arc<TenantServices>,
    cat: &Arc<TenantCatalog>,
    lctn: bool,
) -> Result<Option<Arc<TenantCatalog>>> {
    if cat.ancestry.is_empty() {
        return Ok(None);
    }
    let mut line: Vec<Arc<TenantCatalog>> = Vec::with_capacity(cat.ancestry.len() + 1);
    for a in cat.ancestry.iter() {
        line.push(svc.cache.get(a.id, Freshness::Latest).await?);
    }
    line.push(Arc::clone(cat));
    // Nothing shared to anyone on the line: nothing to do.
    if line[..line.len() - 1]
        .iter()
        .all(|c| sprig_inherit::plan::state_of(c).is_none_or(|s| s.shares().next().is_none()))
    {
        return Ok(None);
    }
    let mut changed = false;
    for j in 1..line.len() {
        let child = Arc::clone(&line[j]);
        let key = (child.version, line[j - 1].version);
        let known = synced()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&child.tenant.0)
            .copied();
        if known == Some(key) {
            continue;
        }
        let did = sync_one(svc, &line[j - 1], &child, lctn).await?;
        let now = if let Some(v) = did {
            changed = true;
            let fresh = svc
                .cache
                .get(child.tenant, Freshness::Cached { min_version: v })
                .await?;
            line[j] = Arc::clone(&fresh);
            fresh
        } else {
            child
        };
        synced()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(now.tenant.0, (now.version, line[j - 1].version));
    }
    Ok(changed.then(|| Arc::clone(&line[line.len() - 1])))
}
