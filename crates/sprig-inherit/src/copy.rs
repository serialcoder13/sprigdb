//! Materialized copies and asynchronous orphaning (chapter 42,
//! "Materialized copies" and "Parent deletes and orphans"; build step 8
//! and the asynchronous half of step 9).
//!
//! A materialized edge keeps the rows its parent shares in a **copy**, a
//! table `__sprig_ih_<object id>` in the child's physical schema, next to
//! the child's object:
//!
//! ```text
//! <the child's columns; the key NOT NULL, the rest NULL>,
//! _sprig_share  share id of an edge inside the tenant; 0 = the edge from the parent tenant
//! _sprig_live   FALSE: a tombstone
//! _sprig_src_v  epoch << 32 | unix seconds of the write (the guard)
//! PRIMARY KEY (<key>, _sprig_share)
//! ```
//!
//! and its state in `__sprig_ih_state` in the same schema (one row per
//! copy: `seeding` or `serving`, the object and share versions it was
//! seeded for, and when the worker last had nothing left to apply). Reads
//! use a copy only while it is `serving` at the current versions
//! ([`CopyStates::ready`]); otherwise they read through, so a copy being
//! seeded is never read half-filled.
//!
//! One [`Maintainer`] keeps every copy of a **family** (a tenant tree,
//! named by its root tenant), serially, from the change feed of every
//! tenant in it (the worker's job, `sprig-worker::inherit`):
//!
//! ```text
//! on_keys(tenant t, object db.table, keys K):        # rows of K changed in t
//!   orphan pass, parents first: for each tenant x at or below t that inherits
//!     db.table from its parent tenant, and each k in K with an `ovr` row in
//!     x's root space: when no level above x still has the record (ignoring
//!     share filters), the row becomes `orphan` with every bit set
//!   copy pass, parents first: for each serving copy whose source is at or
//!     below t: read the source's merged view of K now (writer), filtered by
//!     the share and cut to its columns; upsert what is there, tombstone
//!     what isn't, each write guarded by _sprig_src_v
//! seed(copy): create the tables; read the source's whole view; upsert it;
//!   tombstone the keys the copy holds that the source doesn't; serving
//! ```
//!
//! **Why it converges.** Every write recomputes the source's *current*
//! state, read after the event's commit, and one maintainer writes a
//! family's copies one at a time, so the last write of a key is the newest
//! read. A worker that lost its job (a zombie) may still write once; the
//! guard keeps it from overwriting what the new owner wrote, because its
//! epoch (the job's attempt) is lower, and tombstones keep it from
//! resurrecting a removed row. Events after the last acknowledged position
//! are applied again after a restart, which is harmless. A seed runs in
//! the same serial loop, after the feeds were opened, so no event between
//! the seed's read and the next event is lost.
//!
//! Orphaning across tenants doesn't know why a record left the parent's
//! view. It orphans an override when the record no longer exists above
//! (deleted, hidden, or an orphan reset), and only hides it when the
//! record is merely filtered out, as the chapter wants for filter changes.
//! The override keeps the values stored when it was written; only the
//! `keep` policy is applied.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row, Value};
use sprig_catalog::TenantCatalog;
use sprig_common::ids::{ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_sql::lex::LexOptions;

use crate::model::{ALL_BITS, InheritState, InheritableObject, MergeMode, ROOT_SPACE, SpaceId};
use crate::plan::{AllowAll, Ctx, Limits, physical_schema, state_of, tenant_edge};
use crate::upstream;
use crate::view::{ViewSql, q};

/// The copy state table in each physical schema holding copies.
pub const STATE_TABLE: &str = "__sprig_ih_state";

/// The share key of the edge from the parent tenant.
pub const UPSTREAM: u64 = 0;

/// The copy table of an object.
#[must_use]
pub fn copy_table(object: ObjectId) -> String {
    format!("__sprig_ih_{}", object.0)
}

/// Which copy: the child tenant, the child's object and the share key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CopyKey {
    /// The child tenant (whose schema holds the copy).
    pub tenant: TenantId,
    /// The child's object.
    pub object: ObjectId,
    /// The share id inside the tenant, or [`UPSTREAM`].
    pub share: u64,
}

/// A copy's state row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyInfo {
    /// `serving` (else `seeding`).
    pub serving: bool,
    /// The child object's version it was seeded for.
    pub object_version: u64,
    /// The share's version it was seeded for.
    pub share_version: u64,
    /// When the worker last had nothing left to apply (ms since the epoch).
    pub caught_up_ms: u64,
    /// When it was seeded (ms).
    pub seeded_ms: u64,
}

/// The copies a statement may read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CopyStates(pub HashMap<CopyKey, CopyInfo>);

impl CopyStates {
    /// The copy when it is serving at these versions.
    #[must_use]
    pub fn ready(&self, k: CopyKey, object_version: u64, share_version: u64) -> Option<&CopyInfo> {
        self.0.get(&k).filter(|i| {
            i.serving && i.object_version == object_version && i.share_version == share_version
        })
    }

    /// Whether nothing is known.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// `CREATE TABLE` of the state table in `schema`.
#[must_use]
pub fn state_ddl(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
  object_id       BIGINT UNSIGNED NOT NULL,
  share_key       BIGINT UNSIGNED NOT NULL,
  source_tenant   BIGINT UNSIGNED NOT NULL,
  state           ENUM('seeding','serving') NOT NULL,
  object_version  BIGINT UNSIGNED NOT NULL,
  share_version   BIGINT UNSIGNED NOT NULL,
  epoch           BIGINT UNSIGNED NOT NULL,
  seeded_ms       BIGINT UNSIGNED NOT NULL,
  caught_up_ms    BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (object_id, share_key)
) ENGINE=InnoDB",
        q(schema),
        q(STATE_TABLE)
    )
}

/// `CREATE TABLE` of a copy in `schema`: `cols` are the child's columns
/// (name, `COLUMN_TYPE`, collation), `key` its identity key.
#[must_use]
pub fn copy_ddl(
    schema: &str,
    object: ObjectId,
    key: &[String],
    cols: &[(String, String, Option<String>)],
) -> String {
    let mut out = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (",
        q(schema),
        q(&copy_table(object))
    );
    for (name, ty, coll) in cols {
        let is_key = key.iter().any(|k| k.eq_ignore_ascii_case(name));
        let _ = write!(
            out,
            "{} {ty}{} {}, ",
            q(name),
            coll.as_deref()
                .map(|c| format!(" COLLATE {c}"))
                .unwrap_or_default(),
            if is_key { "NOT NULL" } else { "NULL" }
        );
    }
    let _ = write!(
        out,
        "`_sprig_share` BIGINT UNSIGNED NOT NULL, `_sprig_live` BOOLEAN NOT NULL, \
         `_sprig_src_v` BIGINT UNSIGNED NOT NULL, PRIMARY KEY ({}, `_sprig_share`)) ENGINE=InnoDB",
        key.iter().map(|k| q(k)).collect::<Vec<_>>().join(", ")
    );
    out
}

/// The rows of a copy a merged view reads: the table, and the condition
/// selecting live rows of `share` passing the share's current filter
/// (narrowing applies before the worker cleans up).
#[must_use]
pub fn read_sql(
    prefix: &str,
    object: ObjectId,
    share: u64,
    filter: Option<&str>,
) -> (String, String) {
    (
        format!("{prefix}{}", q(&copy_table(object))),
        format!(
            "`_sprig_share` = {share} AND `_sprig_live`{}",
            filter.map(|f| format!(" AND ({f})")).unwrap_or_default()
        ),
    )
}

/// The state rows of `schema` (a missing table reads as none).
///
/// # Errors
/// The backend's, but 1146.
pub async fn load_states<Q: Queryable + Send>(
    c: &mut Q,
    tenant: TenantId,
    schema: &str,
) -> Result<Vec<(CopyKey, CopyInfo)>> {
    let r: std::result::Result<Vec<Row>, _> = c
        .query(format!(
            "SELECT object_id, share_key, CAST(state AS CHAR), object_version, share_version, \
             caught_up_ms, seeded_ms FROM {}.{}",
            q(schema),
            q(STATE_TABLE)
        ))
        .await;
    let rows = match r {
        Ok(r) => r,
        Err(mysql_async::Error::Server(s)) if s.code == 1146 => return Ok(Vec::new()),
        Err(e) => return Err(backend(&e)),
    };
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let (o, k, st, ov, sv, cu, sd): (u64, u64, String, u64, u64, u64, u64) =
            mysql_async::from_row_opt(r).map_err(|_| SprigError::internal("bad copy state row"))?;
        out.push((
            CopyKey {
                tenant,
                object: ObjectId(o),
                share: k,
            },
            CopyInfo {
                serving: st == "serving",
                object_version: ov,
                share_version: sv,
                caught_up_ms: cu,
                seeded_ms: sd,
            },
        ));
    }
    Ok(out)
}

/// The physical schemas of `cat` whose objects have a materialized edge
/// into them (from this tenant's shares or the parent tenant's).
#[must_use]
pub fn copy_schemas(
    cat: &TenantCatalog,
    parent: Option<&TenantCatalog>,
    lctn: bool,
) -> Vec<String> {
    let Some(state) = state_of(cat) else {
        return Vec::new();
    };
    let mut out = BTreeSet::new();
    for o in state.objects() {
        let Some(db) = cat.databases.get(o.db).map(|d| d.name.clone()) else {
            continue;
        };
        let own = state
            .shares_of(o.object_id)
            .any(|s| s.mode == MergeMode::Materialized);
        let up = parent.is_some_and(|p| {
            tenant_edge(p, cat, &db, &o.table, lctn)
                .ok()
                .flatten()
                .is_some_and(|u| u.share.mode == MergeMode::Materialized)
        });
        if (own || up)
            && let Ok(s) = physical_schema(cat, &db, lctn)
        {
            out.insert(s);
        }
    }
    out.into_iter().collect()
}

fn backend(e: &mysql_async::Error) -> SprigError {
    let mut x = SprigError::new(SprigCode::BackendUnavailable, format!("copy worker: {e}"));
    x.retryable = true;
    x
}

/// What the maintainer needs from its process.
#[async_trait::async_trait]
pub trait CopyEnv: Send + Sync {
    /// The tenants of the family under `root` (root included), parents
    /// before children.
    ///
    /// # Errors
    /// The directory's.
    async fn family(&self, root: TenantId) -> Result<Vec<TenantId>>;

    /// `tenant`'s catalog, read from its writer now.
    ///
    /// # Errors
    /// The catalog's.
    async fn catalog(&self, tenant: TenantId) -> Result<Arc<TenantCatalog>>;

    /// A `sprig_ctl` connection on the cluster of `tenant`'s shards.
    ///
    /// # Errors
    /// The cluster's.
    async fn conn(&self, tenant: TenantId) -> Result<Conn>;

    /// Milliseconds since the Unix epoch.
    fn now_ms(&self) -> u64;
}

/// One copy the family keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyEdge {
    /// Which copy.
    pub key: CopyKey,
    /// The object's logical database.
    pub db_name: String,
    /// The object's name.
    pub table: String,
    /// The child's physical schema (where the copy lives).
    pub child_schema: String,
    /// The child object's identity key.
    pub child_key: Vec<String>,
    /// The tenant whose view is copied.
    pub source_tenant: TenantId,
    /// The space whose view is copied.
    pub source_space: SpaceId,
    /// The share's filter.
    pub filter: Option<String>,
    /// The child's columns, and whether the edge shares each.
    pub columns: Vec<(String, bool)>,
    /// The child object's version.
    pub object_version: u64,
    /// The share's version.
    pub share_version: u64,
    /// Sort order: tenant depth, then the child space's depth.
    pub order: (usize, u16),
}

/// A family's catalogs and copies at one refresh.
#[derive(Debug, Clone, Default)]
pub struct Family {
    /// Tenants, parents first.
    pub tenants: Vec<TenantId>,
    /// Their catalogs.
    pub cats: BTreeMap<TenantId, Arc<TenantCatalog>>,
    /// Each tenant's ancestors in the family, root first.
    pub ancestors: BTreeMap<TenantId, Vec<Arc<TenantCatalog>>>,
    /// The copies, in order.
    pub edges: Vec<CopyEdge>,
}

impl Family {
    /// The depth of `t` in the family.
    #[must_use]
    pub fn depth(&self, t: TenantId) -> usize {
        self.ancestors.get(&t).map_or(0, Vec::len)
    }

    /// Whether `a` is `b` or one of its ancestors.
    #[must_use]
    pub fn at_or_above(&self, a: TenantId, b: TenantId) -> bool {
        a == b
            || self
                .ancestors
                .get(&b)
                .is_some_and(|v| v.iter().any(|c| c.tenant == a))
    }

    /// Whether any tenant of the family inherits across tenants or keeps a
    /// copy (the worker has something to do).
    #[must_use]
    pub fn active(&self) -> bool {
        !self.edges.is_empty() || !self.inheriting_roots().is_empty()
    }

    /// Tenants whose root space receives an object from the parent tenant:
    /// (tenant, db, table), parents first.
    #[must_use]
    pub fn inheriting_roots(&self) -> Vec<(TenantId, String, String)> {
        let mut out = Vec::new();
        for t in &self.tenants {
            let (Some(cat), Some(anc)) = (self.cats.get(t), self.ancestors.get(t)) else {
                continue;
            };
            let (Some(parent), Some(st)) = (anc.last(), state_of(cat)) else {
                continue;
            };
            for o in st.objects() {
                let Some(db) = cat.databases.get(o.db).map(|d| d.name.clone()) else {
                    continue;
                };
                if matches!(tenant_edge(parent, cat, &db, &o.table, false), Ok(Some(_))) {
                    out.push((*t, db, o.table.clone()));
                }
            }
        }
        out
    }

    /// A statement context for `t` (the worker acts as an unrestricted
    /// principal in the root space).
    #[must_use]
    pub fn ctx<'a>(&'a self, t: TenantId, copies: &'a CopyStates) -> Option<Ctx<'a>> {
        static EMPTY: std::sync::OnceLock<InheritState> = std::sync::OnceLock::new();
        let cat = self.cats.get(&t)?;
        Some(Ctx {
            cat,
            state: state_of(cat).unwrap_or_else(|| EMPTY.get_or_init(InheritState::unmigrated)),
            tenant_name: "",
            ancestors: self.ancestors.get(&t).map_or(&[], Vec::as_slice),
            lex: LexOptions::mariadb(),
            lctn: false,
            current_db: None,
            space: ROOT_SPACE,
            user: None,
            limits: Limits::default(),
            privileges: &AllowAll,
            typed: &[],
            from_types: false,
            copies,
        })
    }
}

/// Loads `root`'s family and finds its copies.
///
/// # Errors
/// The directory's and catalogs'.
pub async fn load_family(env: &dyn CopyEnv, root: TenantId) -> Result<Family> {
    let tenants = env.family(root).await?;
    let mut f = Family {
        tenants: tenants.clone(),
        ..Family::default()
    };
    for t in &tenants {
        f.cats.insert(*t, env.catalog(*t).await?);
    }
    for t in &tenants {
        let cat = &f.cats[t];
        let anc: Vec<Arc<TenantCatalog>> = cat
            .ancestry
            .iter()
            .filter_map(|a| f.cats.get(&a.id).cloned())
            .collect();
        f.ancestors.insert(*t, anc);
    }
    let mut edges = Vec::new();
    for t in &tenants {
        let cat = Arc::clone(&f.cats[t]);
        let Some(st) = state_of(&cat) else { continue };
        let depth = f.depth(*t);
        for o in st.objects() {
            let Some(db) = cat.databases.get(o.db).map(|d| d.name.clone()) else {
                continue;
            };
            let Ok(schema) = physical_schema(&cat, &db, false) else {
                continue;
            };
            let mk = |share: u64,
                      source_tenant: TenantId,
                      source_space: SpaceId,
                      filter: Option<String>,
                      shared: &dyn Fn(&str) -> bool,
                      share_version: u64,
                      space_depth: u16| CopyEdge {
                key: CopyKey {
                    tenant: *t,
                    object: o.object_id,
                    share,
                },
                db_name: db.clone(),
                table: o.table.clone(),
                child_schema: schema.clone(),
                child_key: o.key.clone(),
                source_tenant,
                source_space,
                filter,
                columns: o
                    .columns
                    .iter()
                    .map(|c| (c.clone(), o.is_key(c) || shared(c)))
                    .collect(),
                object_version: o.version,
                share_version,
                order: (depth, space_depth),
            };
            // Edges inside the tenant: a share to a space, or to all
            // children of a space that has child spaces (a share to a
            // child tenant is that tenant's edge from its parent, below).
            for sh in st.shares_of(o.object_id).filter(|s| {
                s.mode == MergeMode::Materialized
                    && match s.to {
                        crate::model::ShareTarget::Space(_) => true,
                        crate::model::ShareTarget::AllChildren => !st.children(s.from).is_empty(),
                        crate::model::ShareTarget::Tenant(_) => false,
                    }
            }) {
                let sd = st.space(sh.from, "").map_or(0, |s| s.depth) + 1;
                edges.push(mk(
                    sh.id,
                    *t,
                    sh.from,
                    sh.filter.clone(),
                    &|c| sh.shares_column(o, c),
                    sh.version,
                    sd,
                ));
            }
            if let Some(parent) = f.ancestors[t].last()
                && let Ok(Some(up)) = tenant_edge(parent, &cat, &db, &o.table, false)
                && up.share.mode == MergeMode::Materialized
            {
                let cols = up.columns.clone();
                edges.push(mk(
                    UPSTREAM,
                    parent.tenant,
                    ROOT_SPACE,
                    up.share.filter.clone(),
                    &|c| cols.iter().any(|x| x.eq_ignore_ascii_case(c)),
                    up.share.version,
                    0,
                ));
            }
        }
    }
    edges.sort_by_key(|e| (e.order, e.key));
    f.edges = edges;
    Ok(f)
}

/// A key as SQL literals, one per key column.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyLit(pub Vec<String>);

impl KeyLit {
    /// As a tuple for `IN (...)`: `5` or `(1, 'a')`.
    #[must_use]
    pub fn tuple(&self) -> String {
        if self.0.len() == 1 {
            self.0[0].clone()
        } else {
            format!("({})", self.0.join(", "))
        }
    }
}

/// A value as an SQL literal.
#[must_use]
pub fn literal(v: &Value) -> String {
    v.as_sql(false)
}

/// A key value as a string literal of its text, so a key read from a
/// result set (text) and one from a binlog event (typed) compare equal;
/// MariaDB converts it back for the key's column.
#[must_use]
pub fn key_text(text: Option<&str>) -> String {
    text.map_or_else(|| "NULL".to_owned(), crate::view::lit)
}

/// [`key_text`] of a result-set value.
#[must_use]
pub fn key_value(v: &Value) -> String {
    match v {
        Value::NULL => "NULL".to_owned(),
        Value::Bytes(b) => key_text(Some(&String::from_utf8_lossy(b))),
        Value::Int(i) => key_text(Some(&i.to_string())),
        Value::UInt(u) => key_text(Some(&u.to_string())),
        other => key_text(Some(other.as_sql(true).trim_matches('\''))),
    }
}

fn key_pred(key: &[String], keys: &[KeyLit], alias: &str) -> String {
    let lhs = if key.len() == 1 {
        format!("{alias}{}", q(&key[0]))
    } else {
        format!(
            "({})",
            key.iter()
                .map(|k| format!("{alias}{}", q(k)))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    if keys.is_empty() {
        return "FALSE".into();
    }
    format!(
        "{lhs} IN ({})",
        keys.iter()
            .map(KeyLit::tuple)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// What one maintainer did, for progress and tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// Copy rows written live.
    pub upserts: u64,
    /// Tombstones written.
    pub tombstones: u64,
    /// Overrides orphaned.
    pub orphans: u64,
    /// Copies seeded.
    pub seeds: u64,
    /// Rows read by seeds.
    pub seed_rows: u64,
}

/// Keeps one family's copies (see the module docs).
pub struct Maintainer {
    env: Arc<dyn CopyEnv>,
    root: TenantId,
    epoch: u64,
    family: Family,
    copies: CopyStates,
    views: BTreeSet<(u64, String, String)>,
    /// Root tenants whose `inherit_refs` this maintainer made sure of.
    refs_ready: BTreeSet<u64>,
    /// What it did so far.
    pub counts: Counts,
}

impl std::fmt::Debug for Maintainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Maintainer")
            .field("root", &self.root)
            .field("epoch", &self.epoch)
            .field("edges", &self.family.edges.len())
            .finish_non_exhaustive()
    }
}

#[allow(clippy::needless_pass_by_value)] // `map_err(exec_err)`
fn exec_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let mut x = SprigError::new(
                SprigCode::BackendUnavailable,
                format!("copy worker: {} ({})", s.message, s.code),
            );
            x.retryable = matches!(s.code, 1205 | 1213 | 1146 | 1054);
            x
        }
        _ => backend(&e),
    }
}

impl Maintainer {
    /// A maintainer of `root`'s family writing with `epoch` (the job's
    /// attempt; higher wins).
    #[must_use]
    pub fn new(env: Arc<dyn CopyEnv>, root: TenantId, epoch: u64) -> Self {
        Self {
            env,
            root,
            epoch,
            family: Family::default(),
            copies: CopyStates::default(),
            views: BTreeSet::new(),
            refs_ready: BTreeSet::new(),
            counts: Counts::default(),
        }
    }

    /// The family as last refreshed.
    #[must_use]
    pub fn family(&self) -> &Family {
        &self.family
    }

    /// The copies' states as last read or written.
    #[must_use]
    pub fn copies(&self) -> &CopyStates {
        &self.copies
    }

    fn stamp(&self) -> u64 {
        (self.epoch << 32) | ((self.env.now_ms() / 1000) & 0xFFFF_FFFF)
    }

    /// Reloads the family and the copies' states, drops copies of shares
    /// that are gone, and seeds the copies that aren't serving at the
    /// current versions. Returns how many were seeded.
    ///
    /// # Errors
    /// The catalogs' and backends'.
    pub async fn refresh(&mut self) -> Result<usize> {
        self.family = load_family(self.env.as_ref(), self.root).await?;
        let mut states = CopyStates::default();
        let mut schemas: BTreeSet<(TenantId, String)> = BTreeSet::new();
        for e in &self.family.edges {
            schemas.insert((e.key.tenant, e.child_schema.clone()));
        }
        // Also schemas that held copies of shares dropped since.
        for t in &self.family.tenants {
            let cat = &self.family.cats[t];
            if let Some(st) = state_of(cat) {
                for o in st.objects() {
                    if let Some(db) = cat.databases.get(o.db).map(|d| d.name.clone())
                        && let Ok(s) = physical_schema(cat, &db, false)
                    {
                        schemas.insert((*t, s));
                    }
                }
            }
        }
        for (t, s) in &schemas {
            let mut c = self.env.conn(*t).await?;
            let rows = load_states(&mut c, *t, s).await?;
            for (k, info) in rows {
                let live = self.family.edges.iter().any(|e| e.key == k);
                if live {
                    states.0.insert(k, info);
                } else {
                    // A share or object that is gone: its rows go too.
                    let _ = c
                        .query_drop(format!(
                            "DELETE FROM {}.{} WHERE `_sprig_share` = {}",
                            q(s),
                            q(&copy_table(k.object)),
                            k.share
                        ))
                        .await;
                    c.query_drop(format!(
                        "DELETE FROM {}.{} WHERE object_id = {} AND share_key = {}",
                        q(s),
                        q(STATE_TABLE),
                        k.object.0,
                        k.share
                    ))
                    .await
                    .map_err(exec_err)?;
                }
            }
        }
        self.copies = states;
        let mut seeded = 0;
        let edges = self.family.edges.clone();
        for e in &edges {
            if self
                .copies
                .ready(e.key, e.object_version, e.share_version)
                .is_none()
            {
                self.seed(e).await?;
                seeded += 1;
            }
        }
        Ok(seeded)
    }

    /// Makes sure the share views a source tenant's chain reads exist.
    async fn ensure_views(&mut self, t: TenantId, db: &str, table: &str) -> Result<()> {
        let k = (t.0, db.to_owned(), table.to_owned());
        if self.views.contains(&k) {
            return Ok(());
        }
        let Some(anc) = self.family.ancestors.get(&t) else {
            return Ok(());
        };
        let mut lineage: Vec<&TenantCatalog> = anc.iter().map(AsRef::as_ref).collect();
        lineage.push(&self.family.cats[&t]);
        let mut j = lineage.len() - 1;
        let mut sqls = Vec::new();
        while j >= 1 {
            match tenant_edge(lineage[j - 1], lineage[j], db, table, false)? {
                Some(up) => {
                    sqls.push((
                        lineage[j].tenant,
                        upstream::create_sql(&lineage, j, db, table, &up, false)?,
                    ));
                }
                None => break,
            }
            j -= 1;
        }
        for (tenant, sql) in sqls.into_iter().rev() {
            let mut c = self.env.conn(tenant).await?;
            c.query_drop(sql).await.map_err(exec_err)?;
        }
        self.views.insert(k);
        Ok(())
    }

    /// The source rows of `e` for `keys` (all rows when `None`), as the
    /// child's columns.
    async fn source(&mut self, e: &CopyEdge, keys: Option<&[KeyLit]>) -> Result<Vec<Vec<Value>>> {
        self.ensure_views(e.source_tenant, &e.db_name, &e.table)
            .await?;
        let sql = {
            let ctx = self
                .family
                .ctx(e.source_tenant, &self.copies)
                .ok_or_else(|| SprigError::internal("a source outside the family"))?;
            let cat = ctx.cat;
            let src = ctx
                .state
                .objects()
                .find(|o| {
                    o.table.eq_ignore_ascii_case(&e.table)
                        && cat
                            .databases
                            .get(o.db)
                            .is_some_and(|d| d.name.eq_ignore_ascii_case(&e.db_name))
                })
                .cloned()
                .ok_or_else(|| SprigError::internal("the source object is gone"))?;
            let schema = physical_schema(cat, &e.db_name, false)?;
            let prefix = format!("{}.", q(&schema));
            let chain = ctx.chain(&src, e.source_space, &prefix)?;
            let view = ViewSql {
                o: &src,
                layer: format!("{prefix}{}", q(&src.table)),
                keys: keys.map(|ks| ks.iter().map(KeyLit::tuple).collect()),
                origin: false,
                derived: Vec::new(),
            }
            .view(&chain);
            let cols: Vec<String> = e
                .columns
                .iter()
                .map(|(c, shared)| {
                    if *shared && src.column(c).is_some() {
                        format!("`__sprig_v`.{}", q(c))
                    } else {
                        format!("NULL AS {}", q(c))
                    }
                })
                .collect();
            format!(
                "SELECT {} FROM ({view}) AS `__sprig_v` WHERE {}",
                cols.join(", "),
                e.filter
                    .as_deref()
                    .map_or_else(|| "TRUE".to_owned(), |f| format!("({f})"))
            )
        };
        let mut c = self.env.conn(e.source_tenant).await?;
        let rows: Vec<Row> = c.query(sql).await.map_err(exec_err)?;
        Ok(rows.into_iter().map(Row::unwrap).collect())
    }

    fn key_of(e: &CopyEdge, row: &[Value]) -> KeyLit {
        KeyLit(
            e.child_key
                .iter()
                .filter_map(|k| {
                    e.columns
                        .iter()
                        .position(|(c, _)| c.eq_ignore_ascii_case(k))
                })
                .map(|i| key_value(&row[i]))
                .collect(),
        )
    }

    /// Writes `rows` live and `gone` as tombstones into `e`'s copy.
    async fn write(&mut self, e: &CopyEdge, rows: &[Vec<Value>], gone: &[KeyLit]) -> Result<()> {
        let v = self.stamp();
        let mut c = self.env.conn(e.key.tenant).await?;
        let table = format!("{}.{}", q(&e.child_schema), q(&copy_table(e.key.object)));
        let names: Vec<String> = e.columns.iter().map(|(n, _)| q(n)).collect();
        let sets: Vec<String> = e
            .columns
            .iter()
            .filter(|(n, _)| !e.child_key.iter().any(|k| k.eq_ignore_ascii_case(n)))
            .map(|(n, _)| {
                format!(
                    "{c} = IF(VALUES(`_sprig_src_v`) >= `_sprig_src_v`, VALUES({c}), {c})",
                    c = q(n)
                )
            })
            .collect();
        for chunk in rows.chunks(500) {
            let mut sql = format!(
                "INSERT INTO {table} ({}, `_sprig_share`, `_sprig_live`, `_sprig_src_v`) VALUES ",
                names.join(", ")
            );
            for (i, r) in chunk.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                let _ = write!(
                    sql,
                    "({}, {}, TRUE, {v})",
                    r.iter().map(literal).collect::<Vec<_>>().join(", "),
                    e.key.share
                );
            }
            let _ = write!(
                sql,
                " ON DUPLICATE KEY UPDATE {}{}`_sprig_live` = IF(VALUES(`_sprig_src_v`) >= \
                 `_sprig_src_v`, TRUE, `_sprig_live`), `_sprig_src_v` = \
                 GREATEST(`_sprig_src_v`, VALUES(`_sprig_src_v`))",
                sets.join(", "),
                if sets.is_empty() { "" } else { ", " }
            );
            c.query_drop(sql).await.map_err(exec_err)?;
            self.counts.upserts += chunk.len() as u64;
        }
        let keys: Vec<String> = e.child_key.iter().map(|k| q(k)).collect();
        for chunk in gone.chunks(500) {
            let mut sql = format!(
                "INSERT INTO {table} ({}, `_sprig_share`, `_sprig_live`, `_sprig_src_v`) VALUES ",
                keys.join(", ")
            );
            for (i, k) in chunk.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                let _ = write!(sql, "({}, {}, FALSE, {v})", k.0.join(", "), e.key.share);
            }
            sql.push_str(
                " ON DUPLICATE KEY UPDATE `_sprig_live` = IF(VALUES(`_sprig_src_v`) >= \
                 `_sprig_src_v`, FALSE, `_sprig_live`), `_sprig_src_v` = \
                 GREATEST(`_sprig_src_v`, VALUES(`_sprig_src_v`))",
            );
            c.query_drop(sql).await.map_err(exec_err)?;
            self.counts.tombstones += chunk.len() as u64;
        }
        Ok(())
    }

    async fn set_state(&mut self, e: &CopyEdge, serving: bool) -> Result<()> {
        let now = self.env.now_ms();
        let mut c = self.env.conn(e.key.tenant).await?;
        c.query_drop(format!(
            "INSERT INTO {}.{} (object_id, share_key, source_tenant, state, object_version, \
             share_version, epoch, seeded_ms, caught_up_ms) VALUES ({}, {}, {}, '{}', {}, {}, {}, \
             {now}, {now}) ON DUPLICATE KEY UPDATE source_tenant = VALUES(source_tenant), \
             state = VALUES(state), object_version = VALUES(object_version), \
             share_version = VALUES(share_version), epoch = VALUES(epoch), \
             seeded_ms = VALUES(seeded_ms), caught_up_ms = VALUES(caught_up_ms)",
            q(&e.child_schema),
            q(STATE_TABLE),
            e.key.object.0,
            e.key.share,
            e.source_tenant.0,
            if serving { "serving" } else { "seeding" },
            e.object_version,
            e.share_version,
            self.epoch
        ))
        .await
        .map_err(exec_err)?;
        self.copies.0.insert(
            e.key,
            CopyInfo {
                serving,
                object_version: e.object_version,
                share_version: e.share_version,
                caught_up_ms: now,
                seeded_ms: now,
            },
        );
        Ok(())
    }

    /// Creates `e`'s tables (again, when the child's columns changed) and
    /// fills its copy from the source's whole view.
    ///
    /// # Errors
    /// The backends'.
    pub async fn seed(&mut self, e: &CopyEdge) -> Result<()> {
        let mut c = self.env.conn(e.key.tenant).await?;
        c.query_drop(state_ddl(&e.child_schema))
            .await
            .map_err(exec_err)?;
        let cols: Vec<(String, String, Option<String>)> = c
            .exec(
                "SELECT COLUMN_NAME, COLUMN_TYPE, COLLATION_NAME FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                (e.child_schema.as_str(), e.table.as_str()),
            )
            .await
            .map_err(exec_err)?;
        let cols: Vec<_> = cols
            .into_iter()
            .filter(|(n, _, _)| e.columns.iter().any(|(x, _)| x.eq_ignore_ascii_case(n)))
            .collect();
        let have: Vec<(String, String)> = c
            .exec(
                "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS WHERE \
                 TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME NOT LIKE '\\_sprig\\_%' \
                 ORDER BY ORDINAL_POSITION",
                (e.child_schema.as_str(), copy_table(e.key.object)),
            )
            .await
            .map_err(exec_err)?;
        let want: Vec<(String, String)> = cols
            .iter()
            .map(|(n, t, _)| (n.clone(), t.clone()))
            .collect();
        if !have.is_empty() && have != want {
            // The child's columns changed (names, or types its parent
            // widened): start the object's copies over.
            c.query_drop(format!(
                "DROP TABLE IF EXISTS {}.{}",
                q(&e.child_schema),
                q(&copy_table(e.key.object))
            ))
            .await
            .map_err(exec_err)?;
            c.query_drop(format!(
                "DELETE FROM {}.{} WHERE object_id = {}",
                q(&e.child_schema),
                q(STATE_TABLE),
                e.key.object.0
            ))
            .await
            .map_err(exec_err)?;
            self.copies.0.retain(|k, _| {
                !(k.tenant == e.key.tenant && k.object == e.key.object && k.share != e.key.share)
            });
        }
        c.query_drop(copy_ddl(&e.child_schema, e.key.object, &e.child_key, &cols))
            .await
            .map_err(exec_err)?;
        drop(c);
        self.set_state(e, false).await?;
        let rows = self.source(e, None).await?;
        let present: BTreeSet<KeyLit> = rows.iter().map(|r| Self::key_of(e, r)).collect();
        let mut c = self.env.conn(e.key.tenant).await?;
        let held: Vec<Row> = c
            .query(format!(
                "SELECT {} FROM {}.{} WHERE `_sprig_share` = {} AND `_sprig_live`",
                e.child_key
                    .iter()
                    .map(|k| q(k))
                    .collect::<Vec<_>>()
                    .join(", "),
                q(&e.child_schema),
                q(&copy_table(e.key.object)),
                e.key.share
            ))
            .await
            .map_err(exec_err)?;
        drop(c);
        let gone: Vec<KeyLit> = held
            .into_iter()
            .map(|r| KeyLit(r.unwrap().iter().map(key_value).collect()))
            .filter(|k| !present.contains(k))
            .collect();
        self.counts.seed_rows += rows.len() as u64;
        self.write(e, &rows, &gone).await?;
        self.set_state(e, true).await?;
        self.counts.seeds += 1;
        metrics::counter!("sprig_inherit_copy_seeds_total").increment(1);
        tracing::info!(tenant = %e.key.tenant, object = %e.key.object, share = e.key.share, rows = rows.len(), "copy seeded");
        Ok(())
    }

    /// Whether `db.table` of `tenant` is an object the family inherits or
    /// copies (else events for it are ignored).
    #[must_use]
    pub fn concerns(&self, tenant: TenantId, db: &str, table: &str) -> bool {
        let Some(cat) = self.family.cats.get(&tenant) else {
            return false;
        };
        let Some(st) = state_of(cat) else {
            return false;
        };
        st.objects().any(|o| {
            o.table.eq_ignore_ascii_case(table)
                && cat
                    .databases
                    .get(o.db)
                    .is_some_and(|d| d.name.eq_ignore_ascii_case(db))
        })
    }

    /// Applies a change of `keys` of `db.table` in `tenant` (see the module
    /// docs).
    ///
    /// # Errors
    /// The backends'.
    pub async fn on_keys(
        &mut self,
        tenant: TenantId,
        db: &str,
        table: &str,
        keys: &[KeyLit],
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        self.orphan(tenant, db, table, keys).await?;
        let edges: Vec<CopyEdge> = self
            .family
            .edges
            .iter()
            .filter(|e| {
                e.db_name.eq_ignore_ascii_case(db)
                    && e.table.eq_ignore_ascii_case(table)
                    && self.family.at_or_above(tenant, e.source_tenant)
                    && self
                        .copies
                        .ready(e.key, e.object_version, e.share_version)
                        .is_some()
            })
            .cloned()
            .collect();
        for e in &edges {
            let rows = self.source(e, Some(keys)).await?;
            let present: BTreeSet<KeyLit> = rows.iter().map(|r| Self::key_of(e, r)).collect();
            let gone: Vec<KeyLit> = keys
                .iter()
                .filter(|k| !present.contains(*k))
                .cloned()
                .collect();
            self.write(e, &rows, &gone).await?;
        }
        Ok(())
    }

    /// The orphan pass of [`Self::on_keys`].
    async fn orphan(
        &mut self,
        tenant: TenantId,
        db: &str,
        table: &str,
        keys: &[KeyLit],
    ) -> Result<()> {
        let roots: Vec<(TenantId, String, String)> = self
            .family
            .inheriting_roots()
            .into_iter()
            .filter(|(x, d, t)| {
                d.eq_ignore_ascii_case(db)
                    && t.eq_ignore_ascii_case(table)
                    && self.family.at_or_above(tenant, *x)
            })
            .collect();
        for (x, d, t) in roots {
            let lineage = self.lineage(x, &d, &t)?;
            let Some((_, xo, xschema)) = lineage.last().cloned() else {
                continue;
            };
            // x's overrides of the keys.
            let mut c = self.env.conn(x).await?;
            let rows: Vec<Row> = c
                .query(format!(
                    "SELECT {} FROM {}.{} WHERE `_sprig_space` = {} AND `_sprig_state` = 'ovr' \
                     AND {}",
                    xo.key.iter().map(|k| q(k)).collect::<Vec<_>>().join(", "),
                    q(&xschema),
                    q(&xo.table),
                    ROOT_SPACE.0,
                    key_pred(&xo.key, keys, "")
                ))
                .await
                .map_err(exec_err)?;
            drop(c);
            let ovr: Vec<KeyLit> = rows
                .into_iter()
                .map(|r| KeyLit(r.unwrap().iter().map(key_value).collect()))
                .collect();
            if ovr.is_empty() {
                continue;
            }
            // Whether each still exists above x, ignoring filters.
            let mut exists: BTreeMap<KeyLit, bool> =
                ovr.iter().map(|k| (k.clone(), false)).collect();
            for (y, yo, yschema) in &lineage[..lineage.len() - 1] {
                let mut c = self.env.conn(*y).await?;
                let rows: Vec<Row> = c
                    .query(format!(
                        "SELECT {}, CAST(`_sprig_state` AS CHAR) FROM {}.{} WHERE `_sprig_space` = \
                         {} AND {}",
                        yo.key.iter().map(|k| q(k)).collect::<Vec<_>>().join(", "),
                        q(yschema),
                        q(&yo.table),
                        ROOT_SPACE.0,
                        key_pred(&yo.key, &ovr, "")
                    ))
                    .await
                    .map_err(exec_err)?;
                let mut states: HashMap<KeyLit, String> = HashMap::new();
                for r in rows {
                    let mut v = r.unwrap();
                    let st = v.pop().map(|s| key_value(&s)).unwrap_or_default();
                    states.insert(KeyLit(v.iter().map(key_value).collect()), st);
                }
                for (k, e) in &mut exists {
                    match states.get(k).map(|s| s.trim_matches('\'')) {
                        Some("hide") => *e = false,
                        Some("own" | "orphan") => *e = true,
                        _ => {}
                    }
                }
            }
            let lost: Vec<KeyLit> = exists
                .into_iter()
                .filter(|(_, e)| !e)
                .map(|(k, _)| k)
                .collect();
            if lost.is_empty() {
                continue;
            }
            let fam = if xo.links.is_empty() {
                None
            } else {
                let fam = self.fresh_line(x).await?;
                self.fill_references(&fam, x, &d, &lineage, &lost).await?;
                Some(fam)
            };
            let mut c = self.env.conn(x).await?;
            c.query_drop(format!(
                "UPDATE {}.{} SET `_sprig_state` = 'orphan', `_sprig_ovr` = {ALL_BITS} WHERE \
                 `_sprig_space` = {} AND `_sprig_state` = 'ovr' AND {}",
                q(&xschema),
                q(&xo.table),
                ROOT_SPACE.0,
                key_pred(&xo.key, &lost, "")
            ))
            .await
            .map_err(exec_err)?;
            let n = c.affected_rows();
            drop(c);
            self.counts.orphans += n;
            metrics::counter!("sprig_inherit_orphans_total", "policy" => "keep", "path" => "async")
                .increment(n);
            if n > 0
                && let Some(fam) = &fam
            {
                self.claim_orphans(fam, x, &d, &xschema, &xo.table, &lost)
                    .await?;
            }
        }
        Ok(())
    }

    /// Runs `sql` on `inherit_refs` in tenant `root`'s catalog (`{refs}`
    /// stands for the table, created when missing), as the gateway does.
    async fn refs(&mut self, root: TenantId, sql: &str) -> Result<Vec<Row>> {
        let table = format!(
            "{}.`inherit_refs`",
            q(&sprig_common::names::SchemaName::catalog(root).to_string())
        );
        let mut c = self.env.conn(root).await?;
        if self.refs_ready.insert(root.0) {
            let made = async {
                c.query_drop(crate::links::DDL_REFS.replace("{refs}", &table))
                    .await?;
                c.query_drop(crate::links::DDL_REFS_BY_FROM.replace("{refs}", &table))
                    .await
            }
            .await;
            if let Err(e) = made {
                self.refs_ready.remove(&root.0);
                return Err(exec_err(e));
            }
        }
        c.query(sql.replace("{refs}", &table))
            .await
            .map_err(exec_err)
    }

    /// `t` and its ancestors with their catalogs read now. The family is
    /// refreshed every few seconds, and a table dropped and created again
    /// under the same name has new object ids, which the reference index
    /// is keyed by: the link work of an orphan pass (rare: records that
    /// left) uses these instead.
    async fn fresh_line(&self, t: TenantId) -> Result<Family> {
        let cat = self.env.catalog(t).await?;
        let mut ids: Vec<TenantId> = cat.ancestry.iter().map(|a| a.id).collect();
        ids.push(t);
        let mut f = Family {
            tenants: ids.clone(),
            ..Family::default()
        };
        for id in &ids {
            let c = if *id == t {
                Arc::clone(&cat)
            } else {
                self.env.catalog(*id).await?
            };
            f.cats.insert(*id, c);
        }
        for id in &ids {
            let anc: Vec<Arc<TenantCatalog>> = f.cats[id]
                .ancestry
                .iter()
                .filter_map(|a| f.cats.get(&a.id).cloned())
                .collect();
            f.ancestors.insert(*id, anc);
        }
        Ok(f)
    }

    /// The object `db.table` in `t`'s catalog in `fam`.
    fn object_in(
        fam: &Family,
        t: TenantId,
        db: &str,
        table: &str,
    ) -> Option<Arc<InheritableObject>> {
        let cat = fam.cats.get(&t)?;
        let st = state_of(cat)?;
        let d = cat
            .databases
            .iter()
            .find(|d| d.name.eq_ignore_ascii_case(db))?;
        st.object_named_ci(d.db_id, table).cloned()
    }

    /// The tree root (tenant, object) of the table that link `l` of `o`
    /// (in tenant `t`, database `db`) references: where its claims live.
    fn ref_root(
        &self,
        fam: &Family,
        t: TenantId,
        db: &str,
        l: &crate::model::Link,
    ) -> Option<(TenantId, ObjectId)> {
        let ctx = fam.ctx(t, &self.copies)?;
        let d = ctx
            .cat
            .databases
            .iter()
            .find(|d| d.name.eq_ignore_ascii_case(db))?;
        let rr = ctx.state.object_named_ci(d.db_id, &l.ref_table)?;
        crate::ids::tree_root(&ctx, rr).ok()
    }

    /// Before `x`'s overrides of `lost` become orphans: each foreign key
    /// they inherit (its override bits unset) takes the value `x`'s view
    /// showed when the record left it, not the value stored when the
    /// override was written (the parent may have changed it since). Walking
    /// up from `x`'s parent, that is the first level whose row holds the
    /// record as its own (orphaned first in this pass), else the first
    /// level whose handover claim covers it ([`crate::links::HANDOVER`]).
    /// Any tenant's event can make the orphans (the deleter's, or an
    /// earlier change applied late), so every level above is looked at.
    async fn fill_references(
        &mut self,
        fam: &Family,
        x: TenantId,
        db: &str,
        lineage: &[(TenantId, Arc<InheritableObject>, String)],
        lost: &[KeyLit],
    ) -> Result<()> {
        let Some((_, xo, xschema)) = lineage.last().cloned() else {
            return Ok(());
        };
        let xo = Self::object_in(fam, x, db, &xo.table).unwrap_or(xo);
        let above: Vec<(TenantId, Arc<InheritableObject>, String)> =
            lineage[..lineage.len() - 1].to_vec();
        // Each level's object as its catalog says now (handovers are keyed
        // by it).
        let pairs: Vec<String> = above
            .iter()
            .filter_map(|(y, yo, _)| {
                Self::object_in(fam, *y, db, &yo.table)
                    .map(|o| format!("({}, {})", y.0, o.object_id.0))
            })
            .collect();
        let key_hex = crate::links::canon(&xo.key, None);
        for l in &xo.links {
            let b = crate::links::bits(&xo, &l.columns);
            let mut c = self.env.conn(x).await?;
            let rows: Vec<Row> = c
                .query(format!(
                    "SELECT HEX({key_hex}) FROM {}.{} WHERE `_sprig_space` = {} AND \
                     `_sprig_state` = 'ovr' AND `_sprig_ovr` & {b} <> {b} AND {}",
                    q(&xschema),
                    q(&xo.table),
                    ROOT_SPACE.0,
                    key_pred(&xo.key, lost, "")
                ))
                .await
                .map_err(exec_err)?;
            drop(c);
            let mut need: BTreeSet<String> = rows
                .into_iter()
                .filter_map(|r| r.get::<Option<String>, _>(0).flatten())
                .collect();
            if need.is_empty() {
                continue;
            }
            let in_list = |need: &BTreeSet<String>| {
                need.iter()
                    .map(|h| format!("X'{h}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            // The handovers of every level above, newest last.
            let mut handed: BTreeMap<(u64, String), String> = BTreeMap::new();
            if !pairs.is_empty()
                && let Some(root) = self.ref_root(fam, x, db, l)
            {
                let rows = self
                    .refs(
                        root.0,
                        &format!(
                            "SELECT tenant, HEX(from_key), HEX(ref_key) FROM {{refs}} WHERE \
                             ref_obj = {} AND kind = 'ref' AND (tenant, from_obj) IN ({}) AND \
                             link = {} AND from_key IN ({}) ORDER BY at",
                            root.1.0,
                            pairs.join(", "),
                            crate::view::lit(&format!("{}{}", crate::links::HANDOVER, l.name)),
                            in_list(&need)
                        ),
                    )
                    .await?;
                for r in rows {
                    if let (Some(t), Some(k), Some(v)) = (
                        r.get::<Option<u64>, _>(0).flatten(),
                        r.get::<Option<String>, _>(1).flatten(),
                        r.get::<Option<String>, _>(2).flatten(),
                    ) {
                        handed.insert((t, k), v);
                    }
                }
            }
            let mut values: BTreeMap<String, String> = BTreeMap::new();
            for (y, yo, yschema) in above.iter().rev() {
                if need.is_empty() {
                    break;
                }
                let mut c = self.env.conn(*y).await?;
                let rows: Vec<Row> = c
                    .query(format!(
                        "SELECT HEX({}), HEX({}) FROM {}.{} WHERE `_sprig_space` = {} AND \
                         `_sprig_state` IN ('own', 'orphan') AND {} IN ({})",
                        crate::links::canon(&yo.key, None),
                        crate::links::canon(&l.columns, None),
                        q(yschema),
                        q(&yo.table),
                        ROOT_SPACE.0,
                        crate::links::canon(&yo.key, None),
                        in_list(&need)
                    ))
                    .await
                    .map_err(exec_err)?;
                drop(c);
                for r in rows {
                    let (Some(k), v) = (
                        r.get::<Option<String>, _>(0).flatten(),
                        r.get::<Option<String>, _>(1).flatten(),
                    ) else {
                        continue;
                    };
                    need.remove(&k);
                    if let Some(v) = v {
                        values.insert(k, v);
                    }
                }
                for k in need.clone() {
                    if let Some(v) = handed.get(&(y.0, k.clone())) {
                        values.insert(k.clone(), v.clone());
                        need.remove(&k);
                    }
                }
            }
            tracing::debug!(
                tenant = %x,
                table = %xo.table,
                link = %l.name,
                left = need.len(),
                filled = values.len(),
                "orphans' inherited references filled"
            );
            if values.is_empty() {
                continue;
            }
            let mut c = self.env.conn(x).await?;
            for (k, v) in &values {
                let sets: Vec<String> = l
                    .columns
                    .iter()
                    .zip(crate::links::hex_values(&format!("X'{v}'")))
                    .map(|(col, val)| format!("{} = {val}", q(col)))
                    .collect();
                c.query_drop(format!(
                    "UPDATE {}.{} SET {} WHERE `_sprig_space` = {} AND `_sprig_state` = 'ovr' \
                     AND {key_hex} = X'{k}'",
                    q(&xschema),
                    q(&xo.table),
                    sets.join(", "),
                    ROOT_SPACE.0
                ))
                .await
                .map_err(exec_err)?;
            }
        }
        Ok(())
    }

    /// Claims for the references `x`'s new orphans `lost` now hold as
    /// their own (decision IG7's index), so a delete of a referenced key
    /// anywhere above sees them.
    async fn claim_orphans(
        &mut self,
        fam: &Family,
        x: TenantId,
        db: &str,
        xschema: &str,
        table: &str,
        lost: &[KeyLit],
    ) -> Result<()> {
        let Some(xo) = Self::object_in(fam, x, db, table) else {
            return Ok(());
        };
        for l in &xo.links {
            let Some(root) = self.ref_root(fam, x, db, l) else {
                continue;
            };
            let mut c = self.env.conn(x).await?;
            let rows: Vec<Row> = c
                .query(format!(
                    "SELECT HEX({}), HEX({}) FROM {}.{} WHERE `_sprig_space` = {} AND \
                     `_sprig_state` = 'orphan' AND {} AND {}",
                    crate::links::canon(&xo.key, None),
                    crate::links::canon(&l.columns, None),
                    q(xschema),
                    q(&xo.table),
                    ROOT_SPACE.0,
                    key_pred(&xo.key, lost, ""),
                    l.columns
                        .iter()
                        .map(|c| format!("{} IS NOT NULL", q(c)))
                        .collect::<Vec<_>>()
                        .join(" AND ")
                ))
                .await
                .map_err(exec_err)?;
            drop(c);
            let found: Vec<crate::links::Ref> = rows
                .into_iter()
                .filter_map(|r| {
                    let k = r.get::<Option<String>, _>(0).flatten()?;
                    let v = r.get::<Option<String>, _>(1).flatten()?;
                    Some(crate::links::Ref {
                        from_hex: format!("X'{k}'"),
                        ref_hex: format!("X'{v}'"),
                        ref_sql: String::new(),
                    })
                })
                .collect();
            let at = crate::links::At {
                tenant: x,
                space: ROOT_SPACE,
            };
            for chunk in found.chunks(500) {
                self.refs(
                    root.0,
                    &crate::links::claims_sql(root.1, at, xo.object_id, &l.name, chunk),
                )
                .await?;
            }
            metrics::counter!("sprig_inherit_orphan_claims_total").increment(found.len() as u64);
        }
        Ok(())
    }

    /// The tenants from the top of `x`'s chain of tenant edges for
    /// `db.table` down to `x`: (tenant, its object, its physical schema).
    fn lineage(
        &self,
        x: TenantId,
        db: &str,
        table: &str,
    ) -> Result<Vec<(TenantId, Arc<InheritableObject>, String)>> {
        let anc = self.family.ancestors.get(&x).cloned().unwrap_or_default();
        let mut cats: Vec<Arc<TenantCatalog>> = anc;
        cats.push(Arc::clone(&self.family.cats[&x]));
        let mut j = cats.len() - 1;
        while j >= 1 && tenant_edge(&cats[j - 1], &cats[j], db, table, false)?.is_some() {
            j -= 1;
        }
        let mut out = Vec::new();
        for cat in &cats[j..] {
            let st = state_of(cat).ok_or_else(|| SprigError::internal("no inherit state"))?;
            let d = cat
                .databases
                .iter()
                .find(|d| d.name.eq_ignore_ascii_case(db))
                .ok_or_else(|| SprigError::internal("no database"))?;
            let o = st
                .object_named(d.db_id, table, true)
                .ok_or_else(|| SprigError::internal("no object"))?;
            out.push((cat.tenant, Arc::clone(o), physical_schema(cat, db, false)?));
        }
        Ok(out)
    }

    /// Records that the worker had nothing left to apply at `at_ms`.
    ///
    /// # Errors
    /// The backends'.
    pub async fn caught_up(&mut self, at_ms: u64) -> Result<()> {
        let mut by_schema: BTreeSet<(TenantId, String)> = BTreeSet::new();
        for e in &self.family.edges {
            by_schema.insert((e.key.tenant, e.child_schema.clone()));
        }
        for (t, s) in by_schema {
            let mut c = self.env.conn(t).await?;
            let _ = c
                .query_drop(format!(
                    "UPDATE {}.{} SET caught_up_ms = GREATEST(caught_up_ms, {at_ms}) WHERE state \
                     = 'serving' AND epoch <= {}",
                    q(&s),
                    q(STATE_TABLE),
                    self.epoch
                ))
                .await;
        }
        for i in self.copies.0.values_mut() {
            i.caught_up_ms = i.caught_up_ms.max(at_ms);
        }
        Ok(())
    }

    /// Removes tombstones older than `hours`.
    ///
    /// # Errors
    /// The backends'.
    pub async fn purge(&mut self, hours: u64) -> Result<u64> {
        let cutoff = (self.env.now_ms() / 1000).saturating_sub(hours * 3600);
        let mut n = 0;
        let mut seen = BTreeSet::new();
        for e in self.family.edges.clone() {
            if !seen.insert((e.key.tenant, e.child_schema.clone(), e.key.object)) {
                continue;
            }
            let mut c = self.env.conn(e.key.tenant).await?;
            c.query_drop(format!(
                "DELETE FROM {}.{} WHERE NOT `_sprig_live` AND (`_sprig_src_v` & 4294967295) < \
                 {cutoff}",
                q(&e.child_schema),
                q(&copy_table(e.key.object))
            ))
            .await
            .map_err(exec_err)?;
            n += c.affected_rows();
        }
        Ok(n)
    }
}

/// A child's own columns added to an object it receives through a
/// serving materialized copy (decision T20): the copy gets the same
/// columns, `NULL` (the parent has no values for them), and its state
/// moves to the object's new version, so reads keep using it instead of
/// failing with 39889 until the worker reseeds (across clusters, read
/// through can't stand in). `added`: the columns the statement added;
/// `before`: the object's version before them. Only the edge from the
/// parent tenant: copies of the tenant's own spaces need the new values
/// and are reseeded as before.
///
/// # Errors
/// The backend's.
pub(crate) async fn keep_serving(
    ctx: &Ctx<'_>,
    r: &mut dyn crate::Runner,
    o: &InheritableObject,
    db_name: &str,
    added: &[String],
    before: u64,
) -> Result<()> {
    if added.is_empty() {
        return Ok(());
    }
    let Some(up) = ctx.up_edge(o)? else {
        return Ok(());
    };
    if up.share.mode != MergeMode::Materialized
        || added.iter().any(|c| up.parent_obj.column(c).is_some())
    {
        return Ok(());
    }
    let key = CopyKey {
        tenant: ctx.cat.tenant,
        object: o.object_id,
        share: UPSTREAM,
    };
    if ctx.copies.ready(key, before, up.share.version).is_none() {
        return Ok(());
    }
    let t = ctx.cat.tenant;
    let schema = physical_schema(ctx.cat, db_name, ctx.lctn)?;
    let text = |v: &Option<Vec<u8>>| {
        v.as_ref()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default()
    };
    let lit = |s: &str| format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"));
    let cols = r
        .ctl_query(
            t,
            &format!(
                "SELECT COLUMN_NAME, COLUMN_TYPE, COLLATION_NAME FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
                lit(&schema),
                lit(&o.table)
            ),
        )
        .await?;
    let have: Vec<String> = r
        .ctl_query(
            t,
            &format!(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {} AND \
                 TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
                lit(&schema),
                lit(&copy_table(o.object_id))
            ),
        )
        .await?
        .iter()
        .map(|row| text(&row[0]))
        .collect();
    if have.is_empty() {
        return Ok(());
    }
    let mut adds = Vec::new();
    let mut prev: Option<String> = None;
    for row in &cols {
        let name = text(&row[0]);
        if added.iter().any(|a| a.eq_ignore_ascii_case(&name))
            && !have.iter().any(|h| h.eq_ignore_ascii_case(&name))
        {
            let collate = row
                .get(2)
                .and_then(Option::as_ref)
                .map(|c| format!(" COLLATE {}", String::from_utf8_lossy(c)))
                .unwrap_or_default();
            adds.push(format!(
                "ADD COLUMN {} {}{collate} NULL{}",
                q(&name),
                text(&row[1]),
                prev.as_ref()
                    .map_or_else(|| " FIRST".to_owned(), |p| format!(" AFTER {}", q(p)))
            ));
        }
        if have.iter().any(|h| h.eq_ignore_ascii_case(&name))
            || added.iter().any(|a| a.eq_ignore_ascii_case(&name))
        {
            prev = Some(name);
        }
    }
    if !adds.is_empty() {
        r.ctl(&format!(
            "ALTER TABLE {}.{} {}",
            q(&schema),
            q(&copy_table(o.object_id)),
            adds.join(", ")
        ))
        .await?;
    }
    r.ctl(&format!(
        "UPDATE {}.{} SET object_version = {} WHERE object_id = {} AND share_key = {UPSTREAM} \
         AND object_version = {before} AND state = 'serving'",
        q(&schema),
        q(STATE_TABLE),
        before + 1,
        o.object_id.0
    ))
    .await?;
    metrics::counter!("sprig_inherit_copy_kept_serving_total").increment(1);
    tracing::info!(tenant = %t, table = %o.table, columns = ?added, "copy kept serving after own columns");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_tables_and_reads() {
        let ddl = copy_ddl(
            "t1_d1_g1_s1",
            ObjectId(7),
            &["id".into()],
            &[
                ("id".into(), "int(11)".into(), None),
                (
                    "name".into(),
                    "varchar(40)".into(),
                    Some("utf8mb4_uca1400_ai_ci".into()),
                ),
            ],
        );
        assert_eq!(
            ddl,
            "CREATE TABLE IF NOT EXISTS `t1_d1_g1_s1`.`__sprig_ih_7` (`id` int(11) NOT NULL, \
             `name` varchar(40) COLLATE utf8mb4_uca1400_ai_ci NULL, `_sprig_share` BIGINT \
             UNSIGNED NOT NULL, `_sprig_live` BOOLEAN NOT NULL, `_sprig_src_v` BIGINT UNSIGNED \
             NOT NULL, PRIMARY KEY (`id`, `_sprig_share`)) ENGINE=InnoDB"
        );
        let (from, cond) = read_sql("`s`.", ObjectId(7), 0, Some("price < 50"));
        assert_eq!(from, "`s`.`__sprig_ih_7`");
        assert_eq!(
            cond,
            "`_sprig_share` = 0 AND `_sprig_live` AND (price < 50)"
        );
        assert!(state_ddl("s").contains("PRIMARY KEY (object_id, share_key)"));
    }

    #[test]
    fn readiness_needs_matching_versions() {
        let k = CopyKey {
            tenant: TenantId(1),
            object: ObjectId(2),
            share: 0,
        };
        let mut s = CopyStates::default();
        s.0.insert(
            k,
            CopyInfo {
                serving: true,
                object_version: 3,
                share_version: 1,
                caught_up_ms: 0,
                seeded_ms: 0,
            },
        );
        assert!(s.ready(k, 3, 1).is_some());
        assert!(s.ready(k, 4, 1).is_none());
        assert!(s.ready(k, 3, 2).is_none());
        s.0.get_mut(&k).unwrap().serving = false;
        assert!(s.ready(k, 3, 1).is_none());
    }

    #[test]
    fn key_literals() {
        assert_eq!(KeyLit(vec!["5".into()]).tuple(), "5");
        assert_eq!(KeyLit(vec!["1".into(), "'a'".into()]).tuple(), "(1, 'a')");
        assert_eq!(
            key_pred(&["id".into()], &[KeyLit(vec!["1".into()])], "c."),
            "c.`id` IN (1)"
        );
        assert_eq!(key_pred(&["id".into()], &[], ""), "FALSE");
        assert_eq!(literal(&Value::Int(-3)), "-3");
        assert_eq!(key_value(&Value::Bytes(b"5".to_vec())), "'5'");
        assert_eq!(key_value(&Value::Int(5)), "'5'");
        assert_eq!(key_value(&Value::Bytes(b"it's".to_vec())), "'it''s'");
    }
}
