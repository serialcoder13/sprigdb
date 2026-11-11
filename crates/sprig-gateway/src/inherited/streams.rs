//! Stream instances (chapter 42, "Instances: created on first use";
//! chapter 45, "Inherited streams"):
//!
//! ```text
//! ensure(T, D):
//!  1. T's snapshot has an active instance of D whose stream exists: done
//!  2. D active (39898), T a member (39896), under inherit.max_instances_per_tenant
//!     (39240)
//!  3. inherited_instances: read the row; none: INSERT IGNORE (D, id := T's next
//!     object id, 'creating'); the loser of a race reads the winner's row
//!  4. the winner: the database D names, in T, if T has none (CREATE DATABASE as
//!     the provisioning account); then CREATE STREAM IF NOT EXISTS through the
//!     stream layer (chapter 45) with id := the claimed id, whose catalog change
//!     also sets the row 'active'; the stream's sequencer starts as for any
//!     stream
//!  5. others wait for 'active' up to inherit.instance_create_wait_ms (39899
//!     after); a 'creating' row older than 30 s is taken over (the creator died)
//!  6. T's inherited triggers on D (triggers::ensure_for_stream), the settings
//!     the instance should have (adopt), the instance index entry
//! ```

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_catalog::ddl::{DdlRequest, DdlStatement, database_key, object_key, shard_schemas};
use sprig_catalog::store::{Namespace, ObjectKind, SettingRow, SettingScope};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, TenantCatalog};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::store::MsgStore;
use sprig_inherit::msg::{
    Cause, Chain, DefKey, DefKind, DefState, Definition, Instance, InstanceState, StreamSpec, What,
};
use sprig_streams::sql::{Ctx, Host, PhysSlot, Privilege, StmtSettings, StreamsChange, TableInfo};
use sprig_streams::{ShardSlot, StreamDef, StreamsState};

use super::{InstanceIndex, catalog, dropping, target_err, unavailable};
use crate::tenant::TenantServices;

/// The stream settings a member may override (chapter 42, "What can be
/// overridden").
pub const OVERRIDABLE: &[&str] = &[
    "stream.retention_hours",
    "stream.retention_bytes",
    "stream.max_message_bytes",
    "stream.compaction_lag_minutes",
    "stream.tombstone_hours",
];

/// A member's stream instance, ready to publish into.
#[derive(Debug, Clone)]
pub struct StreamInstance {
    /// The member.
    pub tenant: TenantId,
    /// Its snapshot holding the stream.
    pub cat: Arc<TenantCatalog>,
    /// The stream.
    pub stream: Arc<StreamDef>,
    /// The database's name.
    pub db_name: String,
    /// Created by this call.
    pub created: bool,
}

impl StreamInstance {
    /// The outbox's physical schema.
    ///
    /// # Errors
    /// `Internal` when the stream has no outbox slot.
    pub fn outbox_schema(&self) -> Result<String> {
        let slot = self
            .stream
            .outbox
            .first()
            .ok_or_else(|| SprigError::internal("an instance stream without an outbox"))?;
        Ok(slot.schema(self.tenant, self.stream.db).to_string())
    }
}

fn streams_of(cat: &TenantCatalog) -> StreamsState {
    cat.ext
        .get::<StreamsState>(sprig_streams::MODULE)
        .cloned()
        .unwrap_or_default()
}

pub(crate) fn lctn(svc: &TenantServices, cat: &TenantCatalog) -> bool {
    svc.settings.get(
        &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
        &cat.setting_ctx(&svc.dc),
        Some(&cat.settings),
    ) == 1
}

/// The stream of an active instance row in `cat`, if the snapshot has
/// both.
fn found(cat: &Arc<TenantCatalog>, def: &Definition) -> Option<(Arc<StreamDef>, String)> {
    let st = super::state_of(cat);
    let i = st.instances.get(&def.key())?;
    if i.state != InstanceState::Active {
        return None;
    }
    let s = streams_of(cat)
        .all()
        .find(|s| s.id.0 == i.instance_id)
        .cloned()?;
    let db = cat.databases.get(s.db)?.name.clone();
    Some((s, db))
}

/// The member's active instance of `def` in snapshot `cat`, if it has
/// one (never creates it).
#[must_use]
pub fn existing(cat: &Arc<TenantCatalog>, def: &Definition) -> Option<StreamInstance> {
    let (stream, db_name) = found(cat, def)?;
    Some(StreamInstance {
        tenant: cat.tenant,
        cat: Arc::clone(cat),
        stream,
        db_name,
        created: false,
    })
}

/// The member's instance of stream definition `def`, created if needed
/// (see the module docs). `chain` is the member's.
///
/// # Errors
/// 39896, 39898, 39899, 39240, and the catalog's.
#[allow(clippy::too_many_lines)]
pub async fn ensure(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    def: &Arc<Definition>,
    cause: Cause,
) -> Result<StreamInstance> {
    let t = chain.member();
    // A dropping definition takes no more publishes (39898); its instances
    // still drain.
    if def.state != DefState::Active && cause == Cause::Publish {
        return Err(dropping(format!(
            "stream {}.{} is being dropped",
            def.db_name, def.name
        )));
    }
    let mut cat = catalog(svc, t).await?;
    if let Some((stream, db_name)) = found(&cat, def) {
        let si = StreamInstance {
            tenant: t,
            cat,
            stream,
            db_name,
            created: false,
        };
        after_use(svc, chain, def, &si).await?;
        return Ok(si);
    }
    if def.state != DefState::Active {
        return Err(dropping(format!(
            "stream {}.{} is being dropped",
            def.db_name, def.name
        )));
    }
    if !chain.reaches_level(def, 0) {
        return Err(target_err(format!(
            "tenant {t} isn't a member of stream {}.{}",
            def.db_name, def.name
        )));
    }
    let ctx = cat.setting_ctx(&svc.dc);
    let max = svc.settings.get(
        &sprig_inherit::msg::settings::MAX_INSTANCES_PER_TENANT,
        &ctx,
        Some(&cat.settings),
    );
    let wait_ms = svc.settings.get(
        &sprig_inherit::msg::settings::INSTANCE_CREATE_WAIT_MS,
        &ctx,
        Some(&cat.settings),
    );
    super::migrate(svc, t).await?;
    let ep = svc.locator.writer(t).await?;
    let schema = ep.store.schema().to_owned();
    let store = MsgStore::new(&schema);
    let mut conn = ep.pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
    })?;
    let deadline = Instant::now() + Duration::from_millis(wait_ms);
    let key = def.key();
    let mut created = false;
    loop {
        let row = store.instance(&mut conn, key).await?;
        match row {
            Some(i) if i.state == InstanceState::Active => {
                // Committed; wait for this gateway's snapshot to have it.
                loop {
                    cat = catalog(svc, t).await?;
                    if let Some((stream, db_name)) = found(&cat, def) {
                        let si = StreamInstance {
                            tenant: t,
                            cat,
                            stream,
                            db_name,
                            created,
                        };
                        after_use(svc, chain, def, &si).await?;
                        return Ok(si);
                    }
                    if Instant::now() > deadline {
                        return Err(unavailable(format!(
                            "tenant {t}'s instance of {}.{} isn't loaded yet",
                            def.db_name, def.name
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            Some(i) if i.state == InstanceState::Creating => {
                if store.take_over(&mut conn, key, 30_000).await? {
                    create(svc, t, def, i.instance_id).await?;
                    created = true;
                    continue;
                }
                if Instant::now() > deadline {
                    return Err(unavailable(format!(
                        "tenant {t}'s instance of {}.{} is still being created",
                        def.db_name, def.name
                    )));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Some(i) => {
                return Err(dropping(format!(
                    "tenant {t}'s instance of {}.{} is {}",
                    def.db_name,
                    def.name,
                    i.state.as_str()
                )));
            }
            None => {
                let have = super::state_of(&cat).instances.len() as u64;
                if have >= max {
                    return Err(SprigError::new(
                        SprigCode::InheritInstanceQuota,
                        format!(
                            "ER_SPRIG_INHERIT_INSTANCE_QUOTA: tenant {t} holds {have} inherited \
                             instances (inherit.max_instances_per_tenant = {max})"
                        ),
                    ));
                }
                let id = ep.store.next_object_id(&mut conn).await?.0;
                let row = Instance {
                    def: key,
                    instance_id: id,
                    kind: DefKind::Stream,
                    def_version: def.def_version,
                    definition: def.definition.clone(),
                    state: InstanceState::Creating,
                    cause,
                    physical: Some(json!({"db": def.db_name, "name": def.name})),
                    created_at_ms: 0,
                };
                if store.claim(&mut conn, &row).await? {
                    let started = Instant::now();
                    if let Err(e) = create(svc, t, def, id).await {
                        tracing::warn!(tenant = %t, def = %key, error = %e, "inherited stream instance not created; the next use retries");
                        let mut u = unavailable(format!(
                            "tenant {t}'s instance of {}.{}: {}",
                            def.db_name, def.name, e.message
                        ));
                        u.retryable = true;
                        return Err(u);
                    }
                    metrics::histogram!("sprig_inherit_instance_create_seconds", "kind" => "stream")
                        .record(started.elapsed().as_secs_f64());
                    metrics::counter!("sprig_inherit_instance_creations_total", "kind" => "stream", "cause" => cause.as_str())
                        .increment(1);
                    tracing::info!(tenant = %t, def = %key, stream = id, ms = started.elapsed().as_millis(), "inherited stream instance created");
                    created = true;
                    // Chapter 42: inherit.broadcast_replay.
                    tokio::spawn(super::broadcast::replay(
                        Arc::clone(svc),
                        Arc::clone(def),
                        t,
                    ));
                }
            }
        }
    }
}

/// After a use of an instance: its triggers, its settings, its index
/// entry (each once per change, see [`applied`]).
async fn after_use(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    def: &Arc<Definition>,
    si: &StreamInstance,
) -> Result<()> {
    let sig = signature(chain, def);
    if applied(si.tenant, def.key()) == Some(sig.clone()) {
        return Ok(());
    }
    adopt_settings(svc, chain, def, si).await?;
    super::triggers::ensure_for_stream(svc, chain, def, si).await?;
    // R9 (r9more): inherited sinks (and their connections) on the instance.
    super::sinks::ensure_for_stream(svc, chain, def, si).await;
    // R9 (r9next): fan-in triggers above read the instance.
    super::fanin_triggers::on_instance(svc, chain, def, si).await;
    super::put_index(
        svc,
        &InstanceIndex {
            tenant: si.tenant,
            def: def.key(),
            kind: DefKind::Stream,
            instance_id: si.stream.id.0,
            db: si.db_name.clone(),
            name: si.stream.name.clone(),
        },
    )
    .await?;
    set_applied(si.tenant, def.key(), sig);
    Ok(())
}

/// What an instance's derived state depends on: the definition's version,
/// every override of it or of the triggers on it the member can see, and
/// the trigger definitions reaching the member.
fn signature(chain: &Chain, def: &Definition) -> String {
    use std::fmt::Write as _;
    let mut s = format!("{}:{}", def.key(), def.def_version);
    for l in &chain.levels {
        let Some(st) = &l.state else { continue };
        for o in &st.overrides {
            let _ = write!(s, "|o{}:{}:{}", o.def, o.set_at_ms, o.value);
        }
        for d in st.defs.values().filter(|d| {
            matches!(
                d.kind,
                DefKind::Trigger | DefKind::Sink | DefKind::Connection
            )
        }) {
            let _ = write!(s, "|t{}:{}:{}", d.key(), d.def_version, d.state.as_str());
        }
    }
    s
}

type Applied = HashMap<(TenantId, DefKey), String>;

fn applied_map() -> &'static Mutex<Applied> {
    static M: OnceLock<Mutex<Applied>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

fn applied(t: TenantId, d: DefKey) -> Option<String> {
    applied_map()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&(t, d))
        .cloned()
}

fn set_applied(t: TenantId, d: DefKey, sig: String) {
    applied_map()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert((t, d), sig);
}

/// Forgets what this process applied for `(t, d)` (after an override, so
/// the next use applies it at once).
pub fn forget_applied(t: TenantId, d: DefKey) {
    applied_map()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&(t, d));
}

/// The effective stream settings of the member's instance: each
/// overridable key's nearest override, else the definition's value, else
/// none (the member's ordinary chain decides).
#[must_use]
pub fn effective_settings(chain: &Chain, def: &Definition) -> BTreeMap<&'static str, Option<u64>> {
    let spec: StreamSpec = serde_json::from_value(def.definition.clone()).unwrap_or_default();
    OVERRIDABLE
        .iter()
        .map(|k| {
            let v = chain
                .effective(def.key(), What::Settings, Some(k))
                .and_then(|(v, _)| v.as_u64().or_else(|| v.as_str()?.parse().ok()))
                .or_else(|| spec.settings.get(*k).and_then(Value::as_u64))
                .or_else(|| {
                    (*k == "stream.retention_hours")
                        .then_some(spec.retention_hours)
                        .flatten()
                });
            (*k, v)
        })
        .collect()
}

/// Writes the instance's effective settings as settings of its stream in
/// the member (object scope), where they differ: the stream layer reads
/// them as for any stream.
async fn adopt_settings(
    svc: &TenantServices,
    chain: &Chain,
    def: &Definition,
    si: &StreamInstance,
) -> Result<()> {
    let want = effective_settings(chain, def);
    let scope = SettingScope::Object(ObjectId(si.stream.id.0));
    let tscope = sprig_settings::TenantScope::Object(ObjectId(si.stream.id.0));
    let have = |k: &str| -> Option<u64> {
        match si.cat.settings.entry(tscope, k)?.value.as_ref()? {
            sprig_settings::Value::Int(i) => u64::try_from(*i).ok(),
            sprig_settings::Value::Float(f) => format!("{f:.0}").parse().ok(),
            sprig_settings::Value::Str(s) => s.parse().ok(),
            sprig_settings::Value::Bool(_) => None,
        }
    };
    let changes: Vec<(&str, Option<u64>)> = want
        .iter()
        .filter(|(k, v)| have(k) != **v)
        .map(|(k, v)| (*k, *v))
        .collect();
    if changes.is_empty() {
        return Ok(());
    }
    let mut c = svc.catalog.change(si.tenant, VersionKind::Catalog).await?;
    let version = c.version();
    let r = {
        let (store, tx) = c.tx().parts();
        async {
            for (k, v) in &changes {
                match v {
                    Some(v) => {
                        store
                            .put_setting(
                                tx,
                                &SettingRow {
                                    scope,
                                    key: (*k).to_owned(),
                                    value: Some(json!(v)),
                                    bounds: None,
                                    set_by: "system".to_owned(),
                                    set_at: SystemTime::now(),
                                    version,
                                },
                            )
                            .await?;
                    }
                    None => {
                        store
                            .reset_setting(
                                tx,
                                scope,
                                k,
                                true,
                                false,
                                "system",
                                SystemTime::now(),
                                version,
                            )
                            .await?;
                    }
                }
            }
            Ok::<_, SprigError>(())
        }
        .await
    };
    if let Err(e) = r {
        let _ = c.rollback().await;
        return Err(e);
    }
    c.commit().await?;
    Ok(())
}

/// `t`'s snapshot holding database `db`, created (empty, as the
/// provisioning account) when `t` has none: a member without the
/// definition's database gets it (r9fet decision 2).
///
/// # Errors
/// The catalog's and the DDL's.
pub async fn ensure_database(
    svc: &Arc<TenantServices>,
    t: TenantId,
    db: &str,
) -> Result<Arc<TenantCatalog>> {
    let cat = catalog(svc, t).await?;
    let lc = lctn(svc, &cat);
    if cat.database(&database_key(db, lc)).is_some() {
        return Ok(cat);
    }
    let cluster = svc.locator.cluster(t).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let logical = format!(
        "CREATE DATABASE IF NOT EXISTS {}",
        sprig_common::names::quote(db)
    );
    let req = DdlRequest {
        tenant: t,
        statement: DdlStatement::CreateDatabase {
            name: db.to_owned(),
            if_not_exists: true,
            charset: None,
            collation: None,
        },
        sql: &logical,
        logical_sql: &logical,
        current_db: None,
        user: None,
        lower_case_table_names: lc,
    };
    let out = svc
        .ddl
        .execute(&req, &sprig_catalog::ddl::MariaExecutor::new(pool))
        .await?;
    let cat = match out.version {
        Some(v) => {
            svc.cache
                .get(
                    t,
                    sprig_catalog::cache::Freshness::Cached { min_version: v },
                )
                .await?
        }
        None => catalog(svc, t).await?,
    };
    tracing::info!(tenant = %t, %db, "database created for an inherited instance");
    Ok(cat)
}

/// Step 4: the database and the stream, with id `id`.
#[allow(clippy::too_many_lines)]
async fn create(svc: &Arc<TenantServices>, t: TenantId, def: &Definition, id: u64) -> Result<()> {
    let spec: StreamSpec = serde_json::from_value(def.definition.clone())
        .map_err(|e| SprigError::internal(format!("stream definition: {e}")))?;
    let cat = ensure_database(svc, t, &def.db_name).await?;
    let lc = lctn(svc, &cat);
    let mut text = format!(
        "CREATE STREAM IF NOT EXISTS {}.{}",
        sprig_common::names::quote(&def.db_name),
        sprig_common::names::quote(&def.name)
    );
    if !spec.key.is_empty() {
        text.push_str(" KEY (");
        text.push_str(
            &spec
                .key
                .iter()
                .map(|k| sprig_common::names::quote(k))
                .collect::<Vec<_>>()
                .join(", "),
        );
        text.push(')');
    }
    {
        use std::fmt::Write as _;
        let _ = write!(text, " PARTITIONS {}", spec.partitions.max(1));
    }
    if spec.cleanup == "compact" {
        text.push_str(" CLEANUP compact");
    }
    let state = streams_of(&cat);
    let ctx = Ctx {
        state: &state,
        lex: sprig_sql::lex::LexOptions::mariadb(),
        lctn: lc,
        current: None,
        mysql: false,
    };
    let mut host = InstanceHost {
        svc: Arc::clone(svc),
        cat,
        cluster: svc.locator.cluster(t).await?,
        lctn: lc,
        id: Some(id),
        def: def.key(),
        def_version: def.def_version,
        definition: def.definition.clone(),
    };
    match sprig_streams::sql::handle(&text, &ctx, &mut host).await? {
        sprig_streams::sql::Handled::Ok { .. } => {}
        other => {
            return Err(SprigError::internal(format!(
                "creating an instance stream answered {other:?}"
            )));
        }
    }
    if host.id.is_some() {
        // IF NOT EXISTS found the stream (a retry after a crash): the row
        // goes active now.
        let ep = svc.locator.writer(t).await?;
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        let existing = streams_of(&host.cat)
            .get(
                host.cat
                    .database(&database_key(&def.db_name, lc))
                    .map_or(DbId(0), |d| d.db_id),
                &def.name,
                lc,
            )
            .cloned();
        match existing {
            Some(s) if s.id.0 == id => {
                MsgStore::new(ep.store.schema())
                    .set_instance(
                        &mut conn,
                        def.key(),
                        InstanceState::Active,
                        def.def_version,
                        &def.definition,
                    )
                    .await?;
                // Every gateway reloads the member at its next version.
                let c = svc.catalog.change(t, VersionKind::Catalog).await?;
                c.commit().await?;
            }
            _ => {
                return Err(super::not_overridable(format!(
                    "tenant {t} has its own object named {}.{}; it shadows the inherited stream",
                    def.db_name, def.name
                )));
            }
        }
    }
    Ok(())
}

/// The stream layer's view of a member, for creating an instance.
struct InstanceHost {
    svc: Arc<TenantServices>,
    cat: Arc<TenantCatalog>,
    cluster: sprig_common::ids::ClusterId,
    lctn: bool,
    /// The claimed id, taken by `next_id`.
    id: Option<u64>,
    def: DefKey,
    def_version: u64,
    definition: Value,
}

impl InstanceHost {
    async fn ctl(&self, cluster: &sprig_common::ids::ClusterId) -> Result<mysql_async::Conn> {
        let pool = self.svc.locator.ctl_pool(cluster).await?;
        pool.get_conn().await.map_err(sprig_streams::maria::map_err)
    }
}

#[async_trait]
impl Host for InstanceHost {
    fn tenant(&self) -> TenantId {
        self.cat.tenant
    }

    fn db_id(&self, name: &str) -> Option<DbId> {
        self.cat
            .database(&database_key(name, self.lctn))
            .map(|d| d.db_id)
    }

    fn table(&self, db: DbId, name: &str) -> Option<TableInfo> {
        let o = self.cat.object(
            db,
            Namespace::Relation,
            &object_key(ObjectKind::Table, name, self.lctn),
        )?;
        Some(TableInfo {
            object: o.object_id,
            name: o.name.clone(),
            slots: Vec::new(),
        })
    }

    fn template(&self, db: DbId) -> Option<PhysSlot> {
        let row = self.cat.databases.get(db)?;
        let first = shard_schemas(self.cat.tenant, row).first().copied()?;
        let SchemaName::Shard { group, shard, .. } = first else {
            return None;
        };
        Some(PhysSlot {
            slot: ShardSlot { group, shard },
            schema: first.to_string(),
            cluster: self.cluster.clone(),
        })
    }

    fn allowed(&self, _p: Privilege, _db: &str, _o: Option<ObjectId>, _n: &str) -> bool {
        true
    }

    fn row_filtered(&self, _db: &str, _o: ObjectId) -> bool {
        false
    }

    fn settings(&self, db: DbId) -> StmtSettings {
        use sprig_streams::settings as s;
        let ctx = self.cat.setting_ctx(&self.svc.dc).with_db(db);
        let get = |k: &'static sprig_settings::Setting<u64>| {
            self.svc.settings.get(k, &ctx, Some(&self.cat.settings))
        };
        StmtSettings {
            partitions_default: u32::try_from(get(&s::PARTITIONS_DEFAULT)).unwrap_or(12),
            max_message_bytes: get(&s::MAX_MESSAGE_BYTES),
        }
    }

    fn change_topic_on(&self, _db: DbId, _o: ObjectId) -> bool {
        false
    }

    fn user(&self) -> (String, String) {
        (super::RUN_AS_USER.to_owned(), "localhost".to_owned())
    }

    async fn ddl(&mut self, slot: &PhysSlot, statements: &[String]) -> Result<()> {
        let mut c = self.ctl(&slot.cluster).await?;
        for s in statements {
            c.query_drop(s.as_str())
                .await
                .map_err(sprig_streams::maria::map_err)?;
        }
        Ok(())
    }

    async fn position(&mut self, slot: &PhysSlot) -> Result<String> {
        let mut c = self.ctl(&slot.cluster).await?;
        let p: Option<String> = c
            .query_first("SELECT @@gtid_binlog_pos")
            .await
            .map_err(sprig_streams::maria::map_err)?;
        Ok(p.unwrap_or_default())
    }

    async fn next_id(&mut self) -> Result<u64> {
        self.id
            .take()
            .ok_or_else(|| SprigError::internal("an instance stream asked for a second id"))
    }

    async fn commit(&mut self, change: &StreamsChange) -> Result<()> {
        let tenant = self.cat.tenant;
        let ep = self.svc.locator.writer(tenant).await?;
        {
            let mut conn = ep.pool.get_conn().await.map_err(|e| {
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
            })?;
            sprig_streams::store::migrate(&mut conn, ep.store.schema()).await?;
        }
        let mut c = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let version = c.version();
        let applied = {
            let (store, tx) = c.tx().parts();
            let streams = sprig_streams::StreamsStore::new(store.schema());
            let msg = MsgStore::new(store.schema());
            async {
                match change {
                    StreamsChange::Insert(def, h) => {
                        streams.insert(tx, def).await?;
                        if let Some(h) = h {
                            store
                                .put_setting(
                                    tx,
                                    &SettingRow {
                                        scope: SettingScope::Object(ObjectId(def.id.0)),
                                        key: sprig_streams::settings::RETENTION_HOURS
                                            .key
                                            .to_owned(),
                                        value: Some(json!(h)),
                                        bounds: None,
                                        set_by: "system".to_owned(),
                                        set_at: SystemTime::now(),
                                        version,
                                    },
                                )
                                .await?;
                        }
                        msg.set_instance(
                            tx,
                            self.def,
                            InstanceState::Active,
                            self.def_version,
                            &self.definition,
                        )
                        .await?;
                    }
                    StreamsChange::Update(def, _) => streams.update(tx, def).await?,
                    StreamsChange::Delete(def) => streams.delete(tx, def.id).await?,
                }
                Ok::<_, SprigError>(())
            }
            .await
        };
        if let Err(e) = applied {
            let _ = c.rollback().await;
            return Err(e);
        }
        let v = c.commit().await?;
        self.cat = self
            .svc
            .cache
            .get(
                tenant,
                sprig_catalog::cache::Freshness::Cached { min_version: v },
            )
            .await?;
        Ok(())
    }

    async fn subscribe(&mut self, def: &StreamDef) -> Result<()> {
        sprig_streams::runtime::subscribe(self.svc.locator.dc_store(), self.cat.tenant, def).await
    }

    async fn unsubscribe(&mut self, def: &StreamDef) -> Result<()> {
        sprig_streams::runtime::unsubscribe(self.svc.locator.dc_store(), self.cat.tenant, def.id)
            .await
    }
}

/// Removes a member's stream instance now (`DROP ... IMMEDIATE`, or the end
/// of a drop grace): its stream (catalog row, subscription, tables), its
/// row, its index entry.
///
/// # Errors
/// The catalog's.
pub async fn remove(svc: &Arc<TenantServices>, t: TenantId, def: DefKey) -> Result<()> {
    let cat = catalog(svc, t).await?;
    let st = super::state_of(&cat);
    if let Some(i) = st.instances.get(&def)
        && let Some(s) = streams_of(&cat)
            .all()
            .find(|s| s.id.0 == i.instance_id)
            .cloned()
    {
        let lc = lctn(svc, &cat);
        let db = cat
            .databases
            .get(s.db)
            .map(|d| d.name.clone())
            .unwrap_or_default();
        let state = streams_of(&cat);
        let ctx = Ctx {
            state: &state,
            lex: sprig_sql::lex::LexOptions::mariadb(),
            lctn: lc,
            current: None,
            mysql: false,
        };
        let mut host = InstanceHost {
            svc: Arc::clone(svc),
            cat: Arc::clone(&cat),
            cluster: svc.locator.cluster(t).await?,
            lctn: lc,
            id: None,
            def,
            def_version: 0,
            definition: Value::Null,
        };
        let text = format!(
            "DROP STREAM IF EXISTS {}.{}",
            sprig_common::names::quote(&db),
            sprig_common::names::quote(&s.name)
        );
        sprig_streams::sql::handle(&text, &ctx, &mut host).await?;
    }
    let ep = svc.locator.writer(t).await?;
    let mut conn = ep.pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
    })?;
    MsgStore::new(ep.store.schema())
        .delete_instance(&mut conn, def)
        .await?;
    super::delete_index(svc, t, def).await?;
    forget_applied(t, def);
    Ok(())
}
