//! The router in the tenant path (chapter 17, R1): a statement chapter 07
//! rewrote goes to [`sprig_router::plan`] when the session's tenant has
//! sharded or reference tables, and is relayed as before otherwise.
//!
//! ```text
//! no sharded or reference table in the catalog  → forward (one lookup)
//! BEGIN, COMMIT, ROLLBACK, SAVEPOINT,           → chapter 18's coordinator, never the
//!   ROLLBACK TO, RELEASE                          backend (39441 closes the connection)
//! a range a mover cutover holds is named        → wait for the hold (outside transactions)
//! NotRouted                                     → forward (in a coordinator transaction:
//!                                                 on the session cluster's branch)
//! Pass                                          → relayed undecoded from the shard's
//!                                                 cluster: the session's connection, its
//!                                                 connection to that cluster, or the branch
//! Reject                                        → ERR 39420 with the reason
//! Read, LookupJoin, Write, Explain              → sprig_router::execute_to over the
//!                                                 session's connections ([`backend`]);
//!                                                 scatters stream from parallel shards
//! ERR 1142, 1044 or 1146 before any row         → stale map: wait for the map to move,
//!                                                 plan again (router.stale_map_retries)
//! ```
//!
//! Layouts come from the catalog (`table_groups`, `group_members`,
//! `reference_tables`, loaded by [`layout::ShardingExtension`]) and the
//! shard maps (`sprig_shard::ShardMapCache`), see [`layout`]. Prepared
//! statements on sharded tables are planned per execution from a parsed
//! template cached per text (the plan cache, `router.plan_cache_entries`):
//! a single-shard plan runs as the backend's own prepared statement on
//! the shard, others as text, their rows encoded in the binary protocol.

mod backend;
mod binary;
mod ids;
pub(crate) mod layout;
mod txn;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::command::BoundParam;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, Persona, StatusFlags};
use sprig_router::{Budget, Output, Plan, PlanCtx, RowSink, Template};
use sprig_shard::ShardMapCache;
use sprig_sql::lex::LexOptions;
use sprig_sql::names::{NameMap, OutputCtx, ResultMapper};
use sprig_txn::lookup::{AnchorOf, DistTxn};
use sprig_txn::{BeginOptions, DbError, DtidGen, Intent, ShardRef, TxnConfig, TxnCoordinator};

use self::backend::{GwBackend, db_error, shard_ref, sync_vars};
use self::layout::{Layouts, TenantLayout};
use self::txn::{GwAdmin, GwTxn, ensure_accounts, session_on};
use super::PassSession;
use super::relay::{map_error, rename_all};
use super::split::Renamed;
use super::tenant::Step;
use crate::backend::{BackendSession, StmtClass, StmtInfo};
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues};
use crate::tenant::TenantServices;
use crate::tenant::names::Accounts;

pub(crate) use self::layout::ShardingExtension;

/// The longest a statement waits for a mover's hold.
const MAX_HOLD_WAIT: Duration = Duration::from_secs(10);
/// The longest a stale-map retry waits for the cache to catch up.
const STALE_WAIT: Duration = Duration::from_secs(3);

/// Process-wide router state.
struct Services {
    layouts: Layouts,
    admin: Arc<GwAdmin>,
    dtids: Arc<DtidGen>,
    coordinator_id: String,
    /// Parsed prepared-statement templates, by text.
    templates: Mutex<HashMap<String, Arc<Template>>>,
    /// Bumped when a `router.*` or `txn.*` row changes at any scope:
    /// sessions resolve their [`Configs`] again.
    settings_gen: tokio::sync::watch::Receiver<u64>,
    /// Chapter 19's ID blocks, per tenant.
    ids: Mutex<HashMap<sprig_common::ids::TenantId, Arc<ids::TenantIds>>>,
}

impl Services {
    /// Tenant `tenant`'s ID blocks in this gateway.
    fn ids_of(
        &self,
        tenant: sprig_common::ids::TenantId,
        locator: &Arc<crate::tenant::locator::GatewayLocator>,
    ) -> Arc<ids::TenantIds> {
        let mut m = self.ids.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(
            m.entry(tenant)
                .or_insert_with(|| Arc::new(ids::TenantIds::new(Arc::clone(locator), tenant))),
        )
    }
}

/// One statement's values from chapter 19's ID blocks.
#[derive(Debug, Clone, Copy)]
pub(crate) struct IdsTaken {
    /// The first generated value (`LAST_INSERT_ID()` after it).
    first: i128,
    /// `auto_increment_increment`.
    step: i128,
    /// Some row took a generated value (else only explicit values).
    generated: bool,
}

/// Chapters 17's and 18's settings as one session resolved them, with
/// what they were resolved at.
#[derive(Debug, Clone)]
pub(crate) struct Configs {
    /// The catalog version (tenant rows) they were read at.
    version: u64,
    /// [`Services::settings_gen`] when they were read.
    generation: u64,
    pub(crate) router: sprig_router::RouterConfig,
    txn: TxnConfig,
}

static SERVICES: OnceLock<Services> = OnceLock::new();

fn services(svc: &TenantServices) -> &'static Services {
    SERVICES.get_or_init(|| {
        let incarnation = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(0))
            ^ (u64::from(std::process::id()) << 40);
        Services {
            layouts: Layouts::new(svc.locator.dc_store(), svc.dc.clone()),
            admin: Arc::new(GwAdmin::new(Arc::clone(&svc.locator))),
            dtids: Arc::new(DtidGen::new(incarnation)),
            coordinator_id: format!("gw-{incarnation:016x}"),
            templates: Mutex::new(HashMap::new()),
            settings_gen: svc.settings.subscribe(&["router.*", "txn.*"]),
            ids: Mutex::new(HashMap::new()),
        }
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// What the relay's first-packet probe looks for (see `forward`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Probe {
    /// Relay everything.
    #[default]
    Off,
    /// Hold back an ERR in `STALE_MAP_CODES`.
    Stale,
    /// Hold back any ERR (a coordinator branch: `statement_failed` first).
    Any,
}

/// A session's router state.
#[derive(Default)]
pub(crate) struct RouterState {
    /// The session's connections to clusters other than its own.
    others: HashMap<ClusterId, BackendSession>,
    /// Chapter 18's coordinator, once the session met a sharded table.
    /// Chapter 19's lookup sessions (Pre, Main, Post) over the
    /// coordinator, which it derefs to.
    coord: Option<DistTxn<GwTxn>>,
    /// The layout the coordinator's anchors were taken from.
    anchors_from: usize,
    factory: Option<Arc<GwTxn>>,
    /// The forward path's first-packet probe and what it held back.
    pub(crate) probe: Probe,
    pub(crate) hit: Option<SprigError>,
    /// The probe also holds back 1054 (a column a schema change is
    /// dropping, chapter 21): a routed statement outside a transaction.
    pub(crate) probe_column: bool,
    /// The settings this session last resolved.
    cfg: Option<Configs>,
    /// Shard schemas the open coordinator transaction used, with their
    /// range epochs then (chapter 17: a shard that moves under a
    /// transaction rolls it back with 39424).
    touched: HashMap<String, u64>,
    /// The ID-block values the statement being run took (chapter 19).
    ids: Option<IdsTaken>,
}

impl std::fmt::Debug for RouterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterState")
            .field("others", &self.others.len())
            .field("coord", &self.coord.is_some())
            .finish_non_exhaustive()
    }
}

impl RouterState {
    /// A coordinator transaction is open.
    pub(crate) fn in_txn(&self) -> bool {
        self.coord.as_ref().is_some_and(|c| c.in_txn())
    }

    /// Ends the session's router state: an open transaction rolls back.
    pub(crate) async fn close(&mut self) {
        if let Some(c) = self.coord.as_mut() {
            let _ = c.rollback().await;
        }
        for (_, mut bs) in self.others.drain() {
            bs.close().await;
        }
    }

    /// `COM_RESET_CONNECTION`: rolls back and forgets the other clusters'
    /// session state.
    pub(crate) async fn reset(&mut self) {
        self.close().await;
    }

    /// Checks the first packet of the statement `bs` just sent: an ERR the
    /// probe looks for is held back (drained, kept in `hit`), and `true`
    /// tells the forward path to stop.
    pub(crate) async fn probe_first(&mut self, bs: &mut BackendSession) -> Result<bool> {
        let code = match (self.probe, bs.first_error()) {
            (Probe::Off, _) | (_, None) => return Ok(false),
            (Probe::Stale, Some(e))
                if !(sprig_router::STALE_MAP_CODES.contains(&e.code)
                    || self.probe_column && e.code == COLUMN_GONE) =>
            {
                return Ok(false);
            }
            (_, Some(e)) => super::relay::backend_error(e),
        };
        bs.finish().await?;
        self.hit = Some(code);
        Ok(true)
    }
}

/// Where a relayed statement runs.
#[derive(Debug, Clone)]
enum Slot {
    /// The session's own connection.
    Main,
    /// The session's connection to another cluster.
    Other(ClusterId),
    /// The coordinator's branch on a cluster.
    Branch(ClusterId, ShardRef, Intent),
}

/// What running a plan came to.
enum Ran {
    Done(Step),
    /// A stale-map error before anything reached the client.
    Stale(SprigError),
    /// Outside a transaction, before anything reached the client: an
    /// unknown column (1054) or shards that disagree on columns
    /// (`schema_skew`), as a schema change dropping a column leaves them
    /// for a moment (chapter 21, decision D8).
    ColumnGone(SprigError),
}

/// `lookup.lock_wait_seconds`' default (chapter 19): Pre sessions' lock
/// wait. The key isn't registered yet.
const LOOKUP_LOCK_WAIT_SECONDS: u64 = 5;

/// Where a lookup statement's shard anchors a transaction decision: the
/// shard's place in its map (hash point), from `layout`'s groups and
/// lookup groups; other schemas by their name.
fn anchors(layout: &TenantLayout) -> AnchorOf {
    let mut m: HashMap<String, ShardRef> = HashMap::new();
    for (_, e) in layout.topo.entries() {
        if e.is_reference() {
            continue;
        }
        for (i, sh) in e.group.shards.iter().enumerate() {
            let at = e.group.at(i);
            m.entry(sh.schema.clone()).or_insert_with(|| ShardRef {
                db: DbId(at.db),
                group: GroupId(at.group),
                shard: ShardId(sh.shard),
                hash_point: sh.lo,
                schema: sh.schema.clone(),
            });
        }
        for l in &e.lookups {
            let p = &l.placement;
            for r in &p.lookup_map.ranges {
                let schema = sprig_shard::shard_schema(p.tenant, p.db, p.lookup_map.group, r.shard);
                m.entry(schema.clone()).or_insert_with(|| ShardRef {
                    db: p.db,
                    group: p.lookup_map.group,
                    shard: r.shard,
                    hash_point: r.lo,
                    schema,
                });
            }
        }
    }
    Arc::new(move |t: &sprig_shard::lookup::Target| {
        m.get(&t.schema).cloned().unwrap_or_else(|| {
            let (db, group, shard) = match t.schema.parse::<SchemaName>() {
                Ok(SchemaName::Shard {
                    db, group, shard, ..
                }) => (db, group, shard),
                _ => (DbId(0), GroupId(0), ShardId(0)),
            };
            ShardRef {
                db,
                group,
                shard,
                hash_point: 0,
                schema: t.schema.clone(),
            }
        })
    })
}

/// 39424 `ER_SPRIG_SHARD_MAP_CHANGED`.
fn shard_map_changed() -> SprigError {
    SprigError::new(
        SprigCode::ShardMapChanged,
        "a shard this transaction used moved; the transaction was rolled back, retry it",
    )
}

/// `ER_BAD_FIELD_ERROR`.
const COLUMN_GONE: u16 = 1054;

/// The longest a statement that met a dropped column waits for the
/// schema change to publish before its one retry.
const DDL_PUBLISH_WAIT: Duration = Duration::from_secs(5);

/// Whether `e` is what a shard that already dropped a column answers.
fn column_gone(e: &SprigError) -> bool {
    match &e.kind {
        sprig_common::ErrorKind::Backend { errno, .. } => *errno == COLUMN_GONE,
        sprig_common::ErrorKind::Sprig(SprigCode::StatementNotDistributable) => {
            e.message.contains("(schema_skew)")
        }
        _ => false,
    }
}

/// Rows the router combined, to the client (text or binary protocol).
struct GwSink<'s, 'o> {
    out: &'s mut dyn ResultSink,
    mapper: &'s mut ResultMapper,
    ctx: &'s OutputCtx<'o>,
    renames: &'s [Renamed],
    mysql: bool,
    format: RowFormat,
    cols: Vec<ColumnDef>,
    started: bool,
}

#[async_trait::async_trait]
impl RowSink for GwSink<'_, '_> {
    async fn columns(&mut self, columns: &[ColumnDef]) -> Result<()> {
        let mut c = columns.to_vec();
        self.mapper.column_defs(&mut c, self.ctx);
        rename_all(&mut c, self.renames);
        if self.mysql {
            for d in &mut c {
                sprig_sql::compat::meta::mysql_column(d);
            }
        }
        self.out.begin_rows(&c, self.format).await?;
        self.cols = c;
        self.started = true;
        Ok(())
    }

    async fn row(&mut self, row: &[Option<Bytes>]) -> Result<()> {
        match self.format {
            RowFormat::Text => {
                let vals: Vec<Option<&[u8]>> = row.iter().map(|v| v.as_deref()).collect();
                self.out.row(RowValues::Text(&vals)).await
            }
            RowFormat::Binary => {
                let vals: Vec<sprig_proto_mysql::Value> = row
                    .iter()
                    .zip(&self.cols)
                    .map(|(v, d)| binary::text_value(d, v.as_deref()))
                    .collect();
                self.out.row(RowValues::Binary(&vals)).await
            }
        }
    }
}

/// `SAVEPOINT x`, `ROLLBACK [WORK] TO [SAVEPOINT] x`, `RELEASE SAVEPOINT x`.
#[derive(Debug, PartialEq, Eq)]
enum SpStmt {
    Set(String),
    RollbackTo(String),
    Release(String),
}

fn savepoint_stmt(text: &str) -> Option<SpStmt> {
    let words: Vec<String> = text
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .map(|w| w.trim_matches('`').to_owned())
        .collect();
    let up: Vec<String> = words.iter().map(|w| w.to_ascii_uppercase()).collect();
    let up: Vec<&str> = up.iter().map(String::as_str).collect();
    match up.as_slice() {
        ["SAVEPOINT", _] => Some(SpStmt::Set(words[1].clone())),
        ["RELEASE", "SAVEPOINT", _] => Some(SpStmt::Release(words[2].clone())),
        ["ROLLBACK", rest @ ..] => {
            let rest: Vec<&str> = rest.iter().copied().filter(|w| *w != "WORK").collect();
            match rest.as_slice() {
                ["TO", "SAVEPOINT", _] | ["TO", _] => {
                    Some(SpStmt::RollbackTo(words.last()?.clone()))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

impl PassSession {
    /// The session's tenant layout and the map cache it came from; `None`
    /// for a tenant without sharded or reference tables.
    async fn router_layout(&self) -> Option<(Arc<TenantLayout>, ShardMapCache)> {
        let ts = self.tenant.as_deref()?;
        let cat = ts.catalog()?;
        let svc = ts.services();
        let rs = services(svc);
        let home = svc.locator.home_of(ts.tenant);
        let cache = rs.layouts.cache(&home).ok()?;
        let mut layout = rs.layouts.get(cat, &cache)?;
        if !layout.missing.is_empty() {
            // The cache may still be loading.
            let _ = tokio::time::timeout(Duration::from_secs(5), cache.ready()).await;
            layout = rs.layouts.get(cat, &cache)?;
        }
        Some((layout, cache))
    }

    /// The coordinator, made for the session's first sharded statement,
    /// with the clusters' capabilities and the session's state current.
    async fn ensure_coord(&mut self, cx: &ExecCtx<'_>, layout: &TenantLayout) {
        let _ = self.bs(cx);
        let (Some(ts), Some(bs)) = (self.tenant.as_deref(), self.bs.as_ref()) else {
            return;
        };
        let svc = Arc::clone(ts.services());
        let rs = services(&svc);
        if self.router.factory.is_none() {
            let main = bs.target().cluster.clone();
            let f = Arc::new(GwTxn {
                backends: Arc::clone(&self.backends),
                target: bs.target().clone(),
                opts: crate::backend::SessionOpts {
                    persona: self.persona,
                    found_rows: cx.proto.has(sprig_proto_mysql::Caps::FOUND_ROWS),
                    mariadb_caps: super::mariadb_caps(self.persona, cx.proto.caps),
                    collation: cx.proto.collation,
                    schema: bs.vars().schema.clone(),
                    ..crate::backend::SessionOpts::default()
                },
                main,
                vars: Mutex::new(bs.vars().clone()),
                caps: Mutex::new(HashMap::new()),
                admin: Arc::clone(&rs.admin),
                svc: Arc::clone(&svc),
                cat: Mutex::new(None),
            });
            let txn = TxnCoordinator::new(
                Arc::clone(&f),
                Arc::clone(&rs.dtids),
                ts.tenant,
                TxnConfig {
                    coordinator_id: rs.coordinator_id.clone(),
                    ..TxnConfig::default()
                },
            );
            self.router.coord = Some(DistTxn::new(
                txn,
                Arc::clone(&f),
                anchors(layout),
                LOOKUP_LOCK_WAIT_SECONDS,
            ));
            self.router.anchors_from = std::ptr::from_ref(layout) as usize;
            self.router.factory = Some(f);
        }
        let at = std::ptr::from_ref(layout) as usize;
        if self.router.anchors_from != at
            && let Some(c) = self.router.coord.as_mut()
        {
            c.set_anchor_of(anchors(layout));
            self.router.anchors_from = at;
        }
        let Some(f) = self.router.factory.as_ref() else {
            return;
        };
        *f.vars.lock().unwrap_or_else(PoisonError::into_inner) = bs.vars().clone();
        *f.cat.lock().unwrap_or_else(PoisonError::into_inner) = ts.catalog().cloned();
        let missing: Vec<ClusterId> = {
            let caps = f.caps.lock().unwrap_or_else(PoisonError::into_inner);
            layout::cluster_ids(layout)
                .into_iter()
                .filter(|c| !caps.contains_key(c))
                .collect()
        };
        for c in missing {
            // The capability profile decides (chapter 09's `derived.xa`).
            // `SPRIGDB_ROUTER_XA_CLUSTERS` stays as an operator override
            // for clusters whose profile is wrong (decision RR11); nothing
            // sets it in tests any more.
            let forced = std::env::var("SPRIGDB_ROUTER_XA_CLUSTERS")
                .is_ok_and(|v| v.split(',').any(|x| x.trim() == c.as_str()));
            let xa = forced || svc.locator.cluster_xa(&c).await.unwrap_or(false);
            f.caps
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(c, sprig_txn::ClusterCaps { xa });
        }
        if let Some(coord) = self.router.coord.as_mut()
            && !coord.in_txn()
        {
            let all_xa = {
                let caps = f.caps.lock().unwrap_or_else(PoisonError::into_inner);
                !caps.is_empty() && caps.values().all(|c| c.xa)
            };
            let mut cfg = self
                .router
                .cfg
                .as_ref()
                .map_or_else(|| coord.config().clone(), |c| c.txn.clone());
            cfg.tenant_all_xa = all_xa;
            cfg.coordinator_id.clone_from(&rs.coordinator_id);
            if *coord.config() != cfg {
                coord.set_config(cfg);
            }
        }
    }

    /// The session's `router.*` and `txn.*` settings, resolved again when
    /// the catalog version (tenant rows) or a settings row changed.
    pub(crate) fn router_cfg(&mut self) -> sprig_router::RouterConfig {
        let Some(ts) = self.tenant.as_deref() else {
            return sprig_router::RouterConfig::default();
        };
        let Some(cat) = ts.catalog() else {
            return sprig_router::RouterConfig::default();
        };
        let svc = ts.services();
        let rs = services(svc);
        let generation = *rs.settings_gen.borrow();
        if let Some(c) = &self.router.cfg
            && c.version == cat.version
            && c.generation == generation
        {
            return c.router;
        }
        let ctx = cat.setting_ctx(&svc.dc);
        let get = |k: &'static sprig_settings::Setting<u64>,
                   cx: &sprig_settings::SettingCtx<'_>| {
            svc.settings.get(k, cx, Some(&cat.settings))
        };
        let router = sprig_router::RouterConfig::resolve(&ctx, get);
        let txn = TxnConfig::resolve_with(
            &ctx,
            |k, cx| svc.settings.get(k, cx, Some(&cat.settings)),
            |k, cx| svc.settings.get(k, cx, Some(&cat.settings)).into_owned(),
            &rs.coordinator_id,
        );
        self.router.cfg = Some(Configs {
            version: cat.version,
            generation,
            router,
            txn,
        });
        router
    }

    /// The shard reference of the session's default schema (unsharded
    /// statements inside a coordinator transaction).
    fn home_ref(&self) -> ShardRef {
        let schema = self
            .bs
            .as_ref()
            .and_then(|b| b.vars().schema.clone())
            .unwrap_or_default();
        let (db, group, shard) = match schema.parse::<SchemaName>() {
            Ok(SchemaName::Shard {
                db, group, shard, ..
            }) => (db, group, shard),
            _ => (DbId(0), GroupId(0), ShardId(0)),
        };
        ShardRef {
            db,
            group,
            shard,
            hash_point: 0,
            schema,
        }
    }

    fn main_cluster(&self) -> ClusterId {
        self.bs.as_ref().map_or_else(
            || self.target.cluster.clone(),
            |b| b.target().cluster.clone(),
        )
    }

    /// The main connection is in a transaction the coordinator doesn't own
    /// (`autocommit = 0`, client XA).
    fn legacy_txn(&self) -> bool {
        self.bs.as_ref().is_some_and(|b| b.pins().in_transaction())
    }

    /// Puts `slot`'s connection where the forward path looks
    /// (`self.bs`).
    async fn slot_in(&mut self, slot: &Slot) -> Result<()> {
        let Some(main) = self.bs.as_mut() else {
            return Err(SprigError::internal("no backend session"));
        };
        match slot {
            Slot::Main => Ok(()),
            Slot::Other(c) => {
                if !self.router.others.contains_key(c) {
                    let f = self
                        .router
                        .factory
                        .clone()
                        .ok_or_else(|| SprigError::internal("no router factory"))?;
                    let cat = f.cat.lock().unwrap_or_else(PoisonError::into_inner).clone();
                    ensure_accounts(&f.svc, cat.as_ref(), c).await?;
                    let bs = session_on(&f.backends, &f.target, &f.opts, &f.main, c, main.vars());
                    self.router.others.insert(c.clone(), bs);
                }
                let o = self
                    .router
                    .others
                    .get_mut(c)
                    .ok_or_else(|| SprigError::internal("no session for the cluster"))?;
                sync_vars(o, main.vars());
                std::mem::swap(main, o);
                Ok(())
            }
            Slot::Branch(c, r, intent) => {
                let coord = self
                    .router
                    .coord
                    .as_mut()
                    .ok_or_else(|| SprigError::internal("no coordinator"))?;
                let b = coord.branch(c, r, *intent).await?;
                sync_vars(b.conn_mut(), main.vars());
                std::mem::swap(main, b.conn_mut());
                Ok(())
            }
        }
    }

    /// Takes `slot`'s connection back; the session's state it changed
    /// (`SET`, `USE` aside) carries to the main connection.
    fn slot_out(&mut self, slot: &Slot) {
        let Some(main) = self.bs.as_mut() else { return };
        let other = match slot {
            Slot::Main => return,
            Slot::Other(c) => self.router.others.get_mut(c),
            Slot::Branch(c, ..) => self
                .router
                .coord
                .as_mut()
                .and_then(|co| co.conns_mut(std::slice::from_ref(c)).into_iter().next())
                .map(|(_, conn)| conn),
        };
        if let Some(o) = other {
            std::mem::swap(main, o);
            sync_vars(main, &o.vars().clone());
        }
    }

    /// Relays `sql` from `slot`'s connection, holding back a stale-map ERR
    /// (and, on a branch, any ERR the coordinator must see first).
    #[allow(clippy::too_many_arguments)]
    async fn forward_on(
        &mut self,
        cx: &mut ExecCtx<'_>,
        slot: Slot,
        routed_plan: bool,
        sql: Bytes,
        original: &[u8],
        more: bool,
        format: RowFormat,
        mapper: ResultMapper,
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Ran> {
        let _ = self.bs(cx);
        self.slot_in(&slot).await?;
        self.router.probe = match slot {
            Slot::Branch(..) => Probe::Any,
            _ => Probe::Stale,
        };
        let outside = !self.router.in_txn() && !self.legacy_txn();
        self.router.probe_column = outside && routed_plan;
        self.router.hit = None;
        let r = self
            .forward(cx, sql, Some(original), more, format, mapper, map, out)
            .await;
        self.router.probe = Probe::Off;
        self.router.probe_column = false;
        self.slot_out(&slot);
        let hit = self.router.hit.take();
        let step = match r {
            Ok(s) => s,
            Err(e) => {
                if let Slot::Branch(c, ..) = &slot
                    && let Some(db) = db_error(&e)
                    && let Some(coord) = self.router.coord.as_mut()
                    && let Err(e2) = coord.statement_failed(c, &db).await
                {
                    return Err(e2);
                }
                return Err(e);
            }
        };
        let Some(err) = hit else {
            return Ok(Ran::Done(step));
        };
        if let Slot::Branch(c, ..) = &slot
            && let Some(db) = db_error(&err)
            && let Some(coord) = self.router.coord.as_mut()
            && let Err(e2) = coord.statement_failed(c, &db).await
        {
            return Ok(Ran::Done(self.fail_mapped(out, e2, map).await?));
        }
        let stale = matches!(err.kind, sprig_common::ErrorKind::Backend { errno, .. }
            if sprig_router::STALE_MAP_CODES.contains(&errno));
        // Inside a coordinator transaction a stale map becomes 39424 when
        // the maps moved (`routed`); a transaction the gateway doesn't
        // coordinate gets the shard's error.
        if stale && !self.legacy_txn() {
            return Ok(Ran::Stale(err));
        }
        if outside && routed_plan && column_gone(&err) {
            return Ok(Ran::ColumnGone(err));
        }
        Ok(Ran::Done(self.fail_mapped(out, err, map).await?))
    }

    async fn fail_mapped(
        &mut self,
        out: &mut dyn ResultSink,
        e: SprigError,
        map: &dyn NameMap,
    ) -> Result<Step> {
        let accounts = Accounts(self.tenant.as_deref().and_then(|t| t.auth()));
        let ctx = OutputCtx {
            map,
            accounts: Some(&accounts),
            lex: LexOptions::mariadb(),
        };
        let e = map_error(e, &ctx);
        self.fail(out, &e).await
    }

    /// Waits while a mover cutover holds a range of a table `text` names
    /// (outside transactions, which run on).
    async fn wait_hold(&self, layout: &TenantLayout, cache: &ShardMapCache, text: &str) {
        if self.router.in_txn() || self.legacy_txn() {
            return;
        }
        let Some(until) = layout.hold_for(text) else {
            return;
        };
        let now = now_ms();
        if now >= until {
            return;
        }
        let wait = Duration::from_millis(until - now).min(MAX_HOLD_WAIT);
        metrics::counter!("sprig_router_hold_waits_total").increment(1);
        if let Some(ts) = self.tenant.as_deref() {
            let _ = maps_moved(cache, ts.tenant, layout, wait).await;
        }
    }

    /// After a stale-map error: whether the tenant's maps moved past the
    /// ones the plan used (read linearizably, then waited for in the
    /// cache), so planning again can help.
    async fn map_moved(&self, layout: &TenantLayout, cache: &ShardMapCache) -> bool {
        let Some(ts) = self.tenant.as_deref() else {
            return false;
        };
        let svc = ts.services();
        let store = services(svc).layouts.store(&svc.locator.home_of(ts.tenant));
        let mut moved = false;
        for (g, v) in &layout.versions {
            if let Ok((m, _)) = store.load(ts.tenant, GroupId(*g)).await
                && m.version > *v
            {
                let _ = tokio::time::timeout(
                    STALE_WAIT,
                    cache.wait_version(ts.tenant, GroupId(*g), m.version),
                )
                .await;
                moved = true;
            }
        }
        if !moved && !layout.held.is_empty() {
            // A held range: its cutover commits or releases soon.
            let until = layout.held.values().copied().max().unwrap_or(0);
            let wait = Duration::from_millis(until.saturating_sub(now_ms())).min(MAX_HOLD_WAIT);
            moved = maps_moved(
                cache,
                ts.tenant,
                layout,
                // A hold past its deadline: the cutover is committing or
                // its worker died; give it the stale-map wait.
                wait.max(STALE_WAIT),
            )
            .await;
        }
        moved
    }

    /// A transaction-control statement of a coordinator session, answered
    /// here; `None` for any other statement.
    async fn txn_control(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &str,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if self.legacy_txn() || self.router.coord.is_none() {
            return Ok(None);
        }
        let info = StmtInfo::classify(text.as_bytes());
        let effect = info.effects.iter().find(|e| {
            matches!(
                e,
                crate::backend::Effect::Begin
                    | crate::backend::Effect::Commit
                    | crate::backend::Effect::Rollback
            )
        });
        let sp = if effect.is_none() {
            savepoint_stmt(text)
        } else {
            None
        };
        if effect.is_none() && sp.is_none() {
            return Ok(None);
        }
        let Some(coord) = self.router.coord.as_mut() else {
            return Ok(None);
        };
        let r: Result<()> = match (effect, sp) {
            (Some(crate::backend::Effect::Begin), _) => {
                let up = text.to_ascii_uppercase();
                let mut res = Ok(());
                if coord.in_txn() {
                    // MySQL commits an open transaction first.
                    res = coord.commit().await.map(|_| ());
                }
                if res.is_ok() {
                    res = coord.begin(BeginOptions {
                        read_only: up.contains("READ ONLY"),
                        snapshot: up.contains("CONSISTENT SNAPSHOT"),
                    });
                }
                res
            }
            (Some(crate::backend::Effect::Commit), _) => {
                let r = coord.commit().await.map(|_| ());
                if let Err(e) = &r
                    && e.code() == Some(SprigCode::CommitOutcomeUnknown)
                {
                    cx.session
                        .flags()
                        .close_at_safe_point(crate::session::CloseReason::OutcomeUnknown);
                }
                r
            }
            (Some(_), _) => coord.rollback().await,
            (None, Some(SpStmt::Set(n))) => coord.savepoint(&n),
            (None, Some(SpStmt::RollbackTo(n))) => coord.rollback_to(&n).await,
            (None, Some(SpStmt::Release(n))) => coord.release(&n),
            (None, None) => return Ok(None),
        };
        if effect.is_some() {
            // The transaction ended (or a new one began).
            self.router.touched.clear();
        }
        metrics::counter!("sprig_router_statements_total", "plan" => "txn_control").increment(1);
        match r {
            Ok(()) => {
                let mut status = self.status();
                if more {
                    status |= StatusFlags::MORE_RESULTS_EXISTS;
                }
                out.ok(OkPacket::new(status)).await?;
                Ok(Some(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
            Err(e) => Ok(Some(self.fail(out, &e).await?)),
        }
    }

    /// A forwarded statement of a tenant session: through the router when
    /// the tenant has sharded or reference tables, else as before.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn routed(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: Vec<u8>,
        original: &[u8],
        more: bool,
        format: RowFormat,
        mapper: ResultMapper,
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let layout = self.router_layout().await;
        let (Some((mut layout, cache)), Ok(text)) = (layout, std::str::from_utf8(&sql)) else {
            return self
                .forward(
                    cx,
                    Bytes::from(sql),
                    Some(original),
                    more,
                    format,
                    mapper,
                    map,
                    out,
                )
                .await;
        };
        let text = text.to_owned();
        let cfg = self.router_cfg();
        self.ensure_coord(cx, &layout).await;
        if let Some(e) = self.moved_under(&layout, &text).await {
            return self.fail(out, &e).await;
        }
        if let Some(step) = self.txn_control(cx, &text, more, out).await? {
            return Ok(step);
        }
        // Statements that end a coordinator transaction first: `SET
        // autocommit = 1` after 0, and implicit-commit statements (MySQL
        // commits before them). Neither may reach a branch, where MariaDB
        // would commit (or refuse) the branch alone.
        if self.router.in_txn()
            && let Some(commit) = ends_txn(&text, self.autocommit_off())
        {
            if commit && let Err(e) = Box::pin(self.implicit_commit(cx)).await {
                return self.fail(out, &e).await;
            }
            return match self
                .forward_on(
                    cx,
                    Slot::Main,
                    false,
                    Bytes::from(sql),
                    original,
                    more,
                    format,
                    mapper,
                    map,
                    out,
                )
                .await?
            {
                Ran::Done(step) => Ok(step),
                Ran::Stale(e) | Ran::ColumnGone(e) => self.fail_mapped(out, e, map).await,
            };
        }
        // `autocommit = 0`: a statement that reads or writes opens a
        // coordinator transaction, as MariaDB opens one (decision RR13).
        self.implicit_begin(&text);
        let mut retries = 0usize;
        let mut column_retried = false;
        loop {
            if layout.hold_for(&text).is_some() {
                self.wait_hold(&layout, &cache, &text).await;
                if let Some((l, _)) = self.router_layout().await {
                    layout = l;
                }
            }
            let plan = match Box::pin(self.plan_routed(&layout, &text, &cfg)).await {
                Ok(p) => p,
                Err(e) => return self.fail(out, &e).await,
            };
            self.note_targets(&plan, &layout);
            let ran = self
                .run_plan(
                    cx,
                    plan,
                    Bytes::from(sql.clone()),
                    original,
                    more,
                    format,
                    mapper.clone(),
                    map,
                    out,
                )
                .await?;
            self.router.ids = None;
            match ran {
                Ran::Done(step) => return Ok(step),
                Ran::Stale(err) => {
                    if self.router.in_txn() {
                        return Box::pin(self.moved_under_txn(&layout, &cache, err, map, out))
                            .await;
                    }
                    if retries >= cfg.stale_map_retries || !self.map_moved(&layout, &cache).await {
                        return self.fail_mapped(out, err, map).await;
                    }
                    retries += 1;
                    metrics::counter!("sprig_router_stale_map_retries_total").increment(1);
                    match self.router_layout().await {
                        Some((l, _)) => layout = l,
                        None => return self.fail_mapped(out, err, map).await,
                    }
                }
                Ran::ColumnGone(err) => {
                    let skew = !matches!(err.kind, sprig_common::ErrorKind::Backend { .. });
                    if column_retried || !Box::pin(self.schema_change_pending(&text, skew)).await {
                        return self.fail_mapped(out, err, map).await;
                    }
                    column_retried = true;
                    metrics::counter!("sprig_router_schema_change_retries_total").increment(1);
                    match self.router_layout().await {
                        Some((l, _)) => layout = l,
                        None => return self.fail_mapped(out, err, map).await,
                    }
                }
            }
        }
    }

    /// Before a statement of an open coordinator transaction: if a shard
    /// it used moved since (its range's epoch changed or the range is
    /// gone), the transaction is rolled back and the statement gets 39424
    /// (`ROLLBACK` itself just rolls back). Forgets the shards of a
    /// transaction that ended.
    async fn moved_under(&mut self, layout: &TenantLayout, text: &str) -> Option<SprigError> {
        if !self.router.in_txn() {
            self.router.touched.clear();
            return None;
        }
        if self.router.touched.is_empty() || first_word(text) == "ROLLBACK" {
            return None;
        }
        let moved = self
            .router
            .touched
            .iter()
            .any(|(s, e)| layout.epochs.get(s) != Some(e));
        if !moved {
            return None;
        }
        self.router.touched.clear();
        if let Some(c) = self.router.coord.as_mut() {
            let _ = c.rollback().await;
        }
        metrics::counter!("sprig_router_txn_map_changed_total").increment(1);
        Some(shard_map_changed())
    }

    /// Inside a coordinator transaction: remembers the shards `plan` runs
    /// on, with their epochs now.
    fn note_targets(&mut self, plan: &Plan, layout: &TenantLayout) {
        if !self.router.in_txn() {
            return;
        }
        let mut note = |schema: &str| {
            if let Some(e) = layout.epochs.get(schema)
                && !self.router.touched.contains_key(schema)
            {
                self.router.touched.insert(schema.to_owned(), *e);
            }
        };
        let mut stack = vec![plan];
        while let Some(p) = stack.pop() {
            match p {
                Plan::Pass(t) => note(&t.at.schema),
                Plan::Read(r) => r.targets.iter().for_each(|t| note(&t.at.schema)),
                Plan::Write(w) => w.targets.iter().for_each(|t| note(&t.at.schema)),
                Plan::LookupJoin(j) => {
                    j.outer.targets.iter().for_each(|t| note(&t.at.schema));
                    j.inner_group.shards.iter().for_each(|s| note(&s.schema));
                }
                Plan::ViaLookup(v) => stack.push(&v.plan),
                Plan::ViaParent(v) => v.group.shards.iter().for_each(|s| note(&s.schema)),
                Plan::NeedIds(f) => f.group.shards.iter().for_each(|s| note(&s.schema)),
                Plan::Explain(_) | Plan::NotRouted | Plan::Reject(_) => {}
            }
        }
    }

    /// Plans a text statement of the router path. An INSERT that takes
    /// chapter 19's ID blocks gets its values first; they are kept in
    /// `self.router.ids` for its OK packet.
    ///
    /// # Errors
    /// The ID blocks' errors (39460, 1467).
    async fn plan_routed(
        &mut self,
        layout: &TenantLayout,
        text: &str,
        cfg: &sprig_router::RouterConfig,
    ) -> Result<Plan> {
        let default_schema = self
            .bs
            .as_ref()
            .and_then(|bs| bs.vars().schema.clone())
            .or_else(|| {
                self.tenant
                    .as_deref()
                    .and_then(|ts| ts.schema_of(ts.current.as_deref()))
            });
        let cluster = self.main_cluster().to_string();
        let pcx = PlanCtx {
            topo: &layout.topo,
            default_schema: default_schema.as_deref(),
            session_cluster: &cluster,
            max_route_values: cfg.max_route_values,
            lookup_batch: cfg.lookup_batch,
            ids: self.id_mode(),
        };
        let tenant = self.tenant.as_deref().map(|t| t.tenant);
        // Chapter 20: writes count into the gateway's sketches.
        let plan = crate::write_load::observe(
            tenant,
            text,
            &cluster,
            || sprig_router::plan(text, &pcx),
            |p| p,
        );
        self.router.ids = None;
        let Plan::NeedIds(f) = &plan else {
            return Ok(plan);
        };
        let taken = self.take_ids(f).await?;
        let next = crate::write_load::observe(
            tenant,
            text,
            &cluster,
            || f.finish(taken.first, taken.step, &pcx),
            |p| p,
        );
        self.router.ids = Some(taken);
        Ok(next)
    }

    /// [`Self::plan_routed`] for one execution of a prepared statement.
    ///
    /// # Errors
    /// The ID blocks' errors (39460, 1467).
    async fn plan_prepared_routed(
        &mut self,
        layout: &TenantLayout,
        text: &str,
        template: &Template,
        params: &[sprig_router::ParamLit],
        cfg: &sprig_router::RouterConfig,
    ) -> Result<sprig_router::PreparedPlan> {
        let default_schema = self.bs.as_ref().and_then(|bs| bs.vars().schema.clone());
        let cluster = self.main_cluster().to_string();
        let pcx = PlanCtx {
            topo: &layout.topo,
            default_schema: default_schema.as_deref(),
            session_cluster: &cluster,
            max_route_values: cfg.max_route_values,
            lookup_batch: cfg.lookup_batch,
            ids: self.id_mode(),
        };
        let tenant = self.tenant.as_deref().map(|t| t.tenant);
        // Chapter 20: writes count into the gateway's sketches.
        let mut pp = crate::write_load::observe(
            tenant,
            text,
            &cluster,
            || sprig_router::plan_prepared(template, params, &pcx),
            |p| &p.plan,
        );
        self.router.ids = None;
        let Plan::NeedIds(f) = &pp.plan else {
            return Ok(pp);
        };
        let taken = self.take_ids(f).await?;
        let next = crate::write_load::observe(
            tenant,
            text,
            &cluster,
            || f.finish(taken.first, taken.step, &pcx),
            |p| p,
        );
        pp.plan = next;
        pp.placeholders = false;
        self.router.ids = Some(taken);
        Ok(pp)
    }

    /// The session's `NO_AUTO_VALUE_ON_ZERO` (chapter 19).
    fn id_mode(&self) -> sprig_router::planner::IdMode {
        let zero = self
            .bs
            .as_ref()
            .and_then(|b| b.vars().vars.get("sql_mode").cloned())
            .is_some_and(|m| m.to_ascii_uppercase().contains("NO_AUTO_VALUE_ON_ZERO"));
        sprig_router::planner::IdMode {
            no_auto_value_on_zero: zero,
            filled: false,
        }
    }

    /// Takes an INSERT's `AUTO_INCREMENT` values from the tenant's ID
    /// blocks, in the session's `auto_increment_increment` and
    /// `auto_increment_offset` series, and raises the sequence past its
    /// explicit values (chapter 19).
    async fn take_ids(&self, f: &sprig_router::plan::IdFill) -> Result<IdsTaken> {
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let tids = services(ts.services()).ids_of(ts.tenant, &ts.services().locator);
        let var = |k: &str| {
            self.bs
                .as_ref()
                .and_then(|b| b.vars().vars.get(k).and_then(|v| v.parse::<u64>().ok()))
                .unwrap_or(1)
                .clamp(1, 65_535)
        };
        let s = sprig_shard::ids::AutoIncSession {
            increment: var("auto_increment_increment"),
            offset: var("auto_increment_offset"),
        };
        let run = tids.take(f, &s).await?;
        Ok(match run {
            Some(r) => IdsTaken {
                first: r.first,
                step: r.step,
                generated: true,
            },
            None => IdsTaken {
                first: 0,
                step: 1,
                generated: false,
            },
        })
    }

    /// Whether the session runs with `autocommit = 0`.
    fn autocommit_off(&self) -> bool {
        self.bs.as_ref().is_some_and(|b| {
            b.vars()
                .vars
                .get("autocommit")
                .is_some_and(|v| v.eq_ignore_ascii_case("off") || v == "0")
        })
    }

    /// With `autocommit = 0`, outside a transaction: a statement that
    /// reads or writes tables opens a coordinator transaction.
    fn implicit_begin(&mut self, text: &str) {
        if self.router.in_txn() || self.legacy_txn() || !self.autocommit_off() || !opens_txn(text) {
            return;
        }
        if let Some(c) = self.router.coord.as_mut() {
            // `begin` only records the options; branches open on use.
            let _ = c.begin(BeginOptions::default());
            metrics::counter!("sprig_router_implicit_txns_total").increment(1);
        }
    }

    /// Commits the coordinator transaction before a statement that commits
    /// implicitly (DDL, `SET autocommit = 1` after 0, `LOCK TABLES`, ...),
    /// as MySQL does. A failed commit is the statement's error.
    ///
    /// # Errors
    /// The commit's (39443, 39441, ...).
    pub(super) async fn implicit_commit(&mut self, cx: &mut ExecCtx<'_>) -> Result<()> {
        let Some(coord) = self.router.coord.as_mut() else {
            return Ok(());
        };
        if !coord.in_txn() {
            return Ok(());
        }
        metrics::counter!("sprig_router_implicit_commits_total").increment(1);
        let r = coord.commit().await.map(|_| ());
        self.router.touched.clear();
        if let Err(e) = &r
            && e.code() == Some(SprigCode::CommitOutcomeUnknown)
        {
            cx.session
                .flags()
                .close_at_safe_point(crate::session::CloseReason::OutcomeUnknown);
        }
        r
    }

    /// A shard a transaction's statement reached was fenced: if the maps
    /// moved, the transaction is rolled back and the client gets 39424
    /// (retry it); otherwise the shard's own error.
    async fn moved_under_txn(
        &mut self,
        layout: &TenantLayout,
        cache: &ShardMapCache,
        err: SprigError,
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        if !self.map_moved(layout, cache).await {
            return self.fail_mapped(out, err, map).await;
        }
        if let Some(c) = self.router.coord.as_mut() {
            let _ = c.rollback().await;
        }
        self.router.touched.clear();
        metrics::counter!("sprig_router_txn_map_changed_total").increment(1);
        self.fail(out, &shard_map_changed()).await
    }

    /// After a dropped-column error outside a transaction: whether a
    /// schema change on a table the statement names is publishing, so one
    /// retry after it publishes can help. Reloads the catalog from its
    /// writer (at most once a second per tenant); when a job holds a table
    /// the statement names, or shards disagree on columns (`skew`: only a
    /// schema change in flight does that), waits up to
    /// [`DDL_PUBLISH_WAIT`] for a newer catalog version.
    async fn schema_change_pending(&mut self, text: &str, skew: bool) -> bool {
        let Some(ts) = self.tenant.as_deref_mut() else {
            return false;
        };
        let before = ts.catalog().map_or(0, |c| c.version);
        if ts.reload_after_miss().await.unwrap_or(false) {
            return true;
        }
        let svc = Arc::clone(ts.services());
        let words = words_of(text);
        let held = svc.cache.cached(ts.tenant).is_some_and(|cat| {
            cat.objects
                .iter()
                .any(|o| o.pending_job_id.is_some() && words.contains(&o.name.to_ascii_lowercase()))
        });
        if !held && !skew {
            return false;
        }
        let deadline = tokio::time::Instant::now() + DDL_PUBLISH_WAIT;
        loop {
            if svc
                .cache
                .cached(ts.tenant)
                .is_some_and(|c| c.version > before)
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let _ = ts.refresh().await;
        true
    }

    /// Runs one plan of a text statement.
    #[allow(clippy::too_many_arguments)]
    async fn run_plan(
        &mut self,
        cx: &mut ExecCtx<'_>,
        plan: Plan,
        sql: Bytes,
        original: &[u8],
        more: bool,
        format: RowFormat,
        mut mapper: ResultMapper,
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Ran> {
        let main = self.main_cluster();
        let in_txn = self.router.in_txn();
        match plan {
            Plan::NotRouted => {
                let slot = if in_txn {
                    let intent = intent_of(&sql);
                    Slot::Branch(main, self.home_ref(), intent)
                } else {
                    Slot::Main
                };
                self.forward_on(
                    cx, slot, false, sql, original, more, format, mapper, map, out,
                )
                .await
            }
            // An INSERT with ID-block values runs through the executor, so
            // its OK packet carries the first value (chapter 19).
            Plan::Pass(t) if self.router.ids.is_none() => {
                metrics::counter!("sprig_router_statements_total", "plan" => "pass").increment(1);
                let cluster = ClusterId::new(&t.cluster)
                    .map_err(|e| SprigError::internal(format!("cluster id: {e}")))?;
                let slot = if in_txn {
                    Slot::Branch(cluster, shard_ref(&t), intent_of(t.sql.as_bytes()))
                } else if cluster == main {
                    Slot::Main
                } else if self.legacy_txn() {
                    let e = sprig_router::Rejection::new(
                        sprig_router::Reason::CrossCluster,
                        "a shard on another cluster inside a transaction the gateway doesn't coordinate",
                    );
                    return Ok(Ran::Done(self.fail(out, &e.error()).await?));
                } else {
                    Slot::Other(cluster)
                };
                self.forward_on(
                    cx,
                    slot,
                    true,
                    Bytes::from(t.sql),
                    original,
                    more,
                    format,
                    mapper,
                    map,
                    out,
                )
                .await
            }
            Plan::Reject(r) => Ok(Ran::Done(self.fail(out, &r.error()).await?)),
            plan => {
                let renames = std::mem::take(&mut self.subst);
                self.execute_plan(cx, &plan, more, format, &mut mapper, &renames, map, out)
                    .await
            }
        }
    }

    /// Runs a plan the router executes (reads over shards, lookup joins,
    /// writes, EXPLAIN), its rows to the client in `format`.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn execute_plan(
        &mut self,
        cx: &mut ExecCtx<'_>,
        plan: &Plan,
        more: bool,
        format: RowFormat,
        mapper: &mut ResultMapper,
        renames: &[Renamed],
        map: &dyn NameMap,
        out: &mut dyn ResultSink,
    ) -> Result<Ran> {
        let _ = self.bs(cx);
        let cfg = self.router_cfg();
        let main_cluster = self.main_cluster();
        let legacy_txn = self.legacy_txn();
        let ids = self.router.ids.take();
        let accounts = Accounts(self.tenant.as_deref().and_then(|t| t.auth()));
        let ctx = OutputCtx {
            map,
            accounts: Some(&accounts),
            lex: LexOptions::mariadb(),
        };
        let mut sink = GwSink {
            out,
            mapper,
            ctx: &ctx,
            renames,
            mysql: self.persona == Persona::MySql84,
            format,
            cols: Vec::new(),
            started: false,
        };
        let result = {
            let (Some(bs), Some(coord), Some(factory)) = (
                self.bs.as_mut(),
                self.router.coord.as_mut(),
                self.router.factory.clone(),
            ) else {
                return Err(SprigError::internal("no backend session or coordinator"));
            };
            *factory.vars.lock().unwrap_or_else(PoisonError::into_inner) = bs.vars().clone();
            let mut be = GwBackend {
                main: bs,
                main_cluster,
                others: &mut self.router.others,
                coord,
                factory,
                legacy_txn,
                parallel: cfg.max_parallel_shards,
                implicit: false,
                mark: None,
                explicit_ids: ids.is_some(),
            };
            let mut budget = Budget::mb(cfg.max_statement_memory_mb);
            sprig_router::execute_to(plan, &mut be, &mut budget, &mut sink).await
        };
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let started = sink.started;
        let out = sink.out;
        let fail = |e: SprigError| map_error(e, &ctx);
        match result {
            Err(e) => {
                let in_txn = self.router.in_txn();
                let stale_code = matches!(e.kind, sprig_common::ErrorKind::Backend { errno, .. }
                        if sprig_router::STALE_MAP_CODES.contains(&errno));
                // Outside a transaction only reads retry (a write may have
                // run on some shards); inside one, any statement becomes
                // 39424 when the maps moved (`routed`).
                // An ID-block INSERT on one shard failed before writing.
                let stale = !started
                    && stale_code
                    && !legacy_txn
                    && (in_txn
                        || matches!(plan, Plan::Read(_) | Plan::LookupJoin(_))
                        || (ids.is_some() && matches!(plan, Plan::Pass(_))));
                if stale {
                    return Ok(Ran::Stale(e));
                }
                if !started
                    && !in_txn
                    && !legacy_txn
                    && matches!(plan, Plan::Read(_) | Plan::LookupJoin(_))
                    && column_gone(&e)
                {
                    return Ok(Ran::ColumnGone(e));
                }
                let e = super::tenant::scrub_typed(fail(e));
                out.error(&e).await?;
                Ok(Ran::Done(Step {
                    errored: true,
                    status: None,
                }))
            }
            Ok(Output::Ok(ok)) => {
                let mut p = OkPacket::new(status);
                p.affected_rows = ok.affected;
                p.last_insert_id = ok.last_insert_id;
                // Chapter 19: the first generated value, as MariaDB reports
                // a multi-row INSERT; it becomes the session's
                // LAST_INSERT_ID(), which reaches a connection before a
                // statement reads it.
                if let Some(t) = ids
                    && t.generated
                {
                    let first = u64::try_from(t.first).unwrap_or(0);
                    p.last_insert_id = if ok.affected > 0 { first } else { 0 };
                    if ok.affected > 0
                        && let Some(bs) = self.bs.as_mut()
                    {
                        bs.vars_mut().last_insert_id = first;
                    }
                }
                p.warnings = ok.warnings;
                p.info = Bytes::from(ok.info);
                out.ok(p).await?;
                Ok(Ran::Done(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
            Ok(Output::Streamed { warnings }) => {
                out.end_rows(EndOfRows { status, warnings }).await?;
                Ok(Ran::Done(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
            Ok(Output::Rows {
                columns,
                rows,
                warnings,
            }) => {
                let mut sink = GwSink {
                    out,
                    mapper: sink.mapper,
                    ctx: &ctx,
                    renames,
                    mysql: self.persona == Persona::MySql84,
                    format,
                    cols: Vec::new(),
                    started: false,
                };
                sink.columns(&columns).await?;
                for r in &rows {
                    sink.row(r).await?;
                }
                sink.out.end_rows(EndOfRows { status, warnings }).await?;
                Ok(Ran::Done(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
        }
    }

    /// `COM_STMT_EXECUTE` of a tenant session whose tenant has sharded or
    /// reference tables: `None` to run it as before (on `slot`'s
    /// connection, with `sql` instead of the statement's own text when
    /// given), or the answer the router sent.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn routed_execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: &[u8],
        values: &[BoundParam],
        cursor: bool,
        out: &mut dyn ResultSink,
    ) -> Result<ExecRoute> {
        let Some((layout, cache)) = self.router_layout().await else {
            return Ok(ExecRoute::Normal(None, ExecSlot::Main));
        };
        let Ok(text) = std::str::from_utf8(sql) else {
            return Ok(ExecRoute::Normal(None, ExecSlot::Main));
        };
        let cfg = self.router_cfg();
        self.ensure_coord(cx, &layout).await;
        if let Some(e) = self.moved_under(&layout, text).await {
            out.error(&e).await?;
            return Ok(ExecRoute::Answered);
        }
        self.implicit_begin(text);
        let rs = self
            .tenant
            .as_deref()
            .map(|ts| services(ts.services()))
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let cached = rs
            .templates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(text)
            .cloned();
        let template = match cached {
            Some(t) => t,
            None => match sprig_router::parse_template(text, &layout.topo) {
                Ok(None) => {
                    return Ok(self.unrouted_slot(sql));
                }
                Ok(Some(t)) => {
                    let t = Arc::new(t);
                    let mut m = rs.templates.lock().unwrap_or_else(PoisonError::into_inner);
                    if m.len() >= cfg.plan_cache_entries {
                        m.clear();
                    }
                    m.insert(text.to_owned(), Arc::clone(&t));
                    metrics::counter!("sprig_router_plan_cache_misses_total").increment(1);
                    t
                }
                Err(r) => {
                    let e = r.error();
                    out.error(&e).await?;
                    return Ok(ExecRoute::Answered);
                }
            },
        };
        metrics::counter!("sprig_router_plan_cache_hits_total").increment(1);
        let charset = self
            .bs
            .as_ref()
            .and_then(|b| b.vars().vars.get("character_set_client").cloned())
            .unwrap_or_else(|| "utf8mb4".to_owned());
        let params: Vec<sprig_router::ParamLit> = values
            .iter()
            .filter(|p| p.name.is_empty())
            .map(|p| binary::param_lit(p, &charset))
            .collect();
        let pp = match Box::pin(self.plan_prepared_routed(&layout, text, &template, &params, &cfg))
            .await
        {
            Ok(p) => p,
            Err(e) => {
                out.error(&e).await?;
                return Ok(ExecRoute::Answered);
            }
        };
        self.note_targets(&pp.plan, &layout);
        let main = self.main_cluster();
        let in_txn = self.router.in_txn();
        match pp.plan {
            Plan::NotRouted => Ok(self.unrouted_slot(sql)),
            Plan::Pass(t) if pp.placeholders => {
                metrics::counter!("sprig_router_statements_total", "plan" => "pass").increment(1);
                let cluster = ClusterId::new(&t.cluster)
                    .map_err(|e| SprigError::internal(format!("cluster id: {e}")))?;
                let slot = if in_txn {
                    ExecSlot::Branch(cluster, shard_ref(&t), intent_of(t.sql.as_bytes()))
                } else if cluster == main {
                    ExecSlot::Main
                } else if self.legacy_txn() || cursor {
                    let e = sprig_router::Rejection::new(
                        sprig_router::Reason::CrossCluster,
                        "a prepared statement on another cluster in a transaction the gateway \
                         doesn't coordinate, or with a cursor",
                    );
                    out.error(&e.error()).await?;
                    return Ok(ExecRoute::Answered);
                } else {
                    ExecSlot::Other(cluster)
                };
                Ok(ExecRoute::Normal(Some(t.sql), slot))
            }
            Plan::Reject(r) => {
                out.error(&r.error()).await?;
                Ok(ExecRoute::Answered)
            }
            plan => {
                let map = match self.tenant.as_deref() {
                    Some(ts) => ts.map(),
                    None => return Err(SprigError::internal("no tenant")),
                };
                let mut mapper = ResultMapper::scrub();
                let ran = self
                    .execute_plan(
                        cx,
                        &plan,
                        false,
                        RowFormat::Binary,
                        &mut mapper,
                        &[],
                        &*map,
                        out,
                    )
                    .await?;
                match ran {
                    Ran::Done(step) => Ok(ExecRoute::Done(step.status)),
                    Ran::Stale(e) if self.router.in_txn() => {
                        self.moved_under_txn(&layout, &cache, e, &*map, out).await?;
                        Ok(ExecRoute::Answered)
                    }
                    Ran::Stale(e) | Ran::ColumnGone(e) => {
                        let e = self.map_error(e);
                        out.error(&e).await?;
                        Ok(ExecRoute::Answered)
                    }
                }
            }
        }
    }

    /// An unsharded prepared statement: the session's connection, or the
    /// session cluster's branch inside a coordinator transaction.
    fn unrouted_slot(&self, sql: &[u8]) -> ExecRoute {
        if self.router.in_txn() {
            ExecRoute::Normal(
                None,
                ExecSlot::Branch(self.main_cluster(), self.home_ref(), intent_of(sql)),
            )
        } else {
            ExecRoute::Normal(None, ExecSlot::Main)
        }
    }

    /// Puts an execute's slot in place (see [`Self::routed_execute`]).
    pub(super) async fn exec_slot_in(&mut self, slot: &ExecSlot) -> Result<()> {
        self.slot_in(&slot.as_slot()).await
    }

    /// Takes it back.
    pub(super) fn exec_slot_out(&mut self, slot: &ExecSlot) {
        self.slot_out(&slot.as_slot());
    }

    /// After an execute on a branch failed: the coordinator's verdict.
    pub(super) async fn exec_failed(&mut self, slot: &ExecSlot, e: SprigError) -> SprigError {
        if let ExecSlot::Branch(c, ..) = slot
            && let Some(db) = db_error(&e)
            && let Some(coord) = self.router.coord.as_mut()
            && let Err(e2) = coord.statement_failed(c, &db).await
        {
            return e2;
        }
        e
    }
}

/// Waits up to `wait` for a map of `layout` to move past the version the
/// layout was built from; whether one did.
async fn maps_moved(
    cache: &ShardMapCache,
    tenant: sprig_common::ids::TenantId,
    layout: &TenantLayout,
    wait: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + wait;
    let mut rx = cache.changes();
    loop {
        // Changes before this point are judged by the versions below.
        rx.borrow_and_update();
        let moved = layout
            .versions
            .iter()
            .any(|(g, v)| cache.get(tenant, GroupId(*g)).is_ok_and(|m| m.version > *v));
        if moved {
            return true;
        }
        if tokio::time::timeout_at(deadline, rx.changed())
            .await
            .is_err()
        {
            return false;
        }
    }
}

/// Where a routed `COM_STMT_EXECUTE` runs.
#[derive(Debug, Clone)]
pub(super) enum ExecSlot {
    /// The session's connection.
    Main,
    /// The session's connection to another cluster.
    Other(ClusterId),
    /// A coordinator branch.
    Branch(ClusterId, ShardRef, Intent),
}

impl ExecSlot {
    fn as_slot(&self) -> Slot {
        match self {
            Self::Main => Slot::Main,
            Self::Other(c) => Slot::Other(c.clone()),
            Self::Branch(c, r, i) => Slot::Branch(c.clone(), r.clone(), *i),
        }
    }
}

/// What [`PassSession::routed_execute`] decided.
pub(super) enum ExecRoute {
    /// Run the execute as before, on the slot, with this text if given.
    Normal(Option<String>, ExecSlot),
    /// The router answered (rows or OK); the status to report.
    Done(Option<StatusFlags>),
    /// An error went to the client.
    Answered,
}

/// Chapter 21's view of a tenant's shards (`tenant::ddl::ShardedDdl`):
/// for a template schema and a table, the shard schemas holding it and
/// their clusters (a sharded table: its group's shards; a reference table:
/// each copy); for database-level objects (`None`), every shard schema of
/// the database's sharded groups. `None` for a tenant without sharded or
/// reference tables, or a table that isn't one.
pub(super) fn ddl_layout(
    ts: &crate::tenant::session::TenantSession,
) -> Option<crate::tenant::ddl::ShardLayout> {
    let cat = ts.catalog()?;
    let svc = ts.services();
    let rs = services(svc);
    let cache = rs.layouts.cache(&svc.locator.home_of(ts.tenant)).ok()?;
    let layout = rs.layouts.get(cat, &cache)?;
    Some(Arc::new(move |template: &str, table: Option<&str>| {
        let shards = |e: &sprig_router::topology::TableEntry| -> Vec<(String, ClusterId)> {
            e.group
                .shards
                .iter()
                .filter_map(|s| Some((s.schema.clone(), ClusterId::new(&s.cluster).ok()?)))
                .collect()
        };
        if let Some(t) = table {
            return sprig_router::Topology::table(&layout.topo, template, t).map(shards);
        }
        let out: Vec<(String, ClusterId)> = layout
            .topo
            .shards_under(template)
            .into_iter()
            .filter_map(|s| Some((s.schema, ClusterId::new(&s.cluster).ok()?)))
            .collect();
        (!out.is_empty()).then_some(out)
    }))
}

/// `text` without leading spaces and comments.
fn skip_comments(mut t: &str) -> &str {
    loop {
        t = t.trim_start();
        if let Some(r) = t.strip_prefix("/*") {
            // `/*!` and `/*M!` executable comments hold the statement.
            if r.starts_with('!') || r.starts_with("M!") {
                return t;
            }
            t = r.split_once("*/").map_or("", |(_, rest)| rest);
        } else if t.starts_with("-- ") || t.starts_with('#') {
            t = t.split_once('\n').map_or("", |(_, rest)| rest);
        } else {
            return t;
        }
    }
}

/// The statement's first word, uppercase.
fn first_word(text: &str) -> String {
    let t = skip_comments(text).trim_start_matches('(');
    t.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase()
}

/// With `autocommit = 0`, whether the statement opens a transaction:
/// reads and writes of tables (MariaDB opens one at the first table
/// access; `SET`, `SHOW`, `USE` and the like don't).
fn opens_txn(text: &str) -> bool {
    matches!(
        first_word(text).as_str(),
        "SELECT"
            | "INSERT"
            | "UPDATE"
            | "DELETE"
            | "REPLACE"
            | "WITH"
            | "TABLE"
            | "VALUES"
            | "LOAD"
    )
}

/// Inside a coordinator transaction: `Some(commit first)` for a statement
/// that must run on the session's own connection instead of a branch:
/// `SET ... autocommit = ...` (committing when it turns autocommit on
/// from off) and statements that commit implicitly (`LOCK TABLES`,
/// `ANALYZE TABLE`, ...); `None` for any other.
fn ends_txn(text: &str, autocommit_off: bool) -> Option<bool> {
    if first_word(text) == "SET" {
        let up = text.to_ascii_uppercase();
        let i = up.find("AUTOCOMMIT")?;
        let rest = up[i + "AUTOCOMMIT".len()..]
            .trim_start()
            .trim_start_matches(":=")
            .trim_start_matches('=')
            .trim_start()
            .trim_start_matches(['\'', '"']);
        let on = rest.starts_with('1') || rest.starts_with("ON") || rest.starts_with("TRUE");
        return Some(on && autocommit_off);
    }
    StmtInfo::classify(text.as_bytes())
        .effects
        .iter()
        .any(|e| {
            matches!(
                e,
                crate::backend::Effect::ImplicitCommit | crate::backend::Effect::LockTables
            )
        })
        .then_some(true)
}

/// The identifiers of `text`, lowercase.
fn words_of(text: &str) -> std::collections::HashSet<String> {
    text.split(|c: char| !c.is_alphanumeric() && c != '_' && c != '$')
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect()
}

/// Write intent unless the statement is a plain read.
fn intent_of(sql: &[u8]) -> Intent {
    if StmtInfo::classify(sql).class == StmtClass::Read {
        Intent::Read
    } else {
        Intent::Write
    }
}

/// A coordinator error as the backend error it stands for (tests).
#[allow(dead_code)]
fn as_db(e: &SprigError) -> Option<DbError> {
    db_error(e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn savepoint_statements_parse() {
        assert_eq!(savepoint_stmt("SAVEPOINT a"), Some(SpStmt::Set("a".into())));
        assert_eq!(
            savepoint_stmt("rollback work to savepoint `b`;"),
            Some(SpStmt::RollbackTo("b".into()))
        );
        assert_eq!(
            savepoint_stmt("ROLLBACK TO c"),
            Some(SpStmt::RollbackTo("c".into()))
        );
        assert_eq!(
            savepoint_stmt("release savepoint d"),
            Some(SpStmt::Release("d".into()))
        );
        assert_eq!(savepoint_stmt("ROLLBACK"), None);
        assert_eq!(savepoint_stmt("SELECT 1"), None);
    }

    #[test]
    fn statements_that_open_a_transaction_under_autocommit_0() {
        for t in [
            "SELECT * FROM t",
            "  /* hint */ select 1",
            "(SELECT a FROM t) UNION (SELECT b FROM u)",
            "insert into t values (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "REPLACE INTO t VALUES (1)",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "-- c\nSELECT 1",
        ] {
            assert!(opens_txn(t), "{t}");
        }
        for t in ["SET @a = 1", "SHOW TABLES", "USE db", "COMMIT", "DO 1", ""] {
            assert!(!opens_txn(t), "{t}");
        }
    }

    #[test]
    fn statements_that_leave_a_coordinator_transaction() {
        assert_eq!(ends_txn("SET autocommit = 1", true), Some(true));
        assert_eq!(ends_txn("set @@session.autocommit=ON", true), Some(true));
        assert_eq!(ends_txn("SET autocommit = 1", false), Some(false));
        assert_eq!(ends_txn("SET autocommit = 0", true), Some(false));
        assert_eq!(
            ends_txn("SET SESSION autocommit = 'OFF'", false),
            Some(false)
        );
        assert_eq!(ends_txn("SET @x = 1", true), None);
        assert_eq!(ends_txn("SELECT 1", true), None);
        assert_eq!(ends_txn("LOCK TABLES t WRITE", false), Some(true));
    }

    #[test]
    fn column_gone_errors() {
        assert!(column_gone(&SprigError::backend(
            1054,
            *b"42S22",
            "Unknown column 'c' in 'field list'"
        )));
        assert!(column_gone(
            &sprig_router::Rejection::new(sprig_router::Reason::SchemaSkew, "x").error()
        ));
        assert!(!column_gone(&SprigError::backend(1146, *b"42S02", "x")));
        assert!(words_of("SELECT `Email` FROM users").contains("users"));
    }
}
