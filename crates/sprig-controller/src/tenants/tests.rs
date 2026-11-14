//! Tenant bootstrap and deletion.
//!
//! The unit case runs without a server: placement is recorded before the
//! catalog is touched. The live case needs `SPRIG_TEST_MARIADB_SINGLE`
//! (for example `mysql://root:<password>@127.0.0.1:13301`) and is
//! skipped without it; with `SPRIG_TEST_ETCD` the directory and the DC
//! tier are private tiers of that etcd (deleted afterwards), else
//! `MemStore`s. Tenants are named `test-<tag>-*`; their catalogs are the
//! schemas `__sprig_test_tenant_<pid>_<tag>_<id>`, dropped afterwards, also
//! when the case fails.
//!
//! The live case covers create (root, child, grandchild), placement
//! (requested and fewest tenants), re-runs, a bootstrap left half done,
//! the `tree_settings` copy over MariaDB, settings taking effect (an
//! object-scope change takes effect within seconds; a value outside
//! its bounds is refused) and deletion with its guard.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use sprig_catalog::cache::{CacheConfig, DirectoryWatch, Freshness, MariaSource, TenantCache};
use sprig_catalog::placement::ShardMap;
use sprig_catalog::store::{
    DatabaseRow, DbState, Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState,
};
use sprig_catalog::{
    AllowAll, DirectoryAllocator, LogDecisions, MariaBackend, Principal, SetRequest, Target,
    TenantSettingsWriter, VersionKind, Versioner,
};
use sprig_common::ids::{DbId, GroupId, ObjectId};
use sprig_common::time::SystemClock;
use sprig_meta::conformance::{ETCD_ENV, delete_prefix, unique_tier};
use sprig_meta::{EtcdConfig, EtcdStore, MemStore, Tier};
use sprig_settings::{
    Apply, Bounds, HardBounds, LiveSettings, OwnedCtx, Registry, ScopeKind, Scopes, Setting,
    SettingChange, Settings, SettingsStores, Value,
};

use super::*;
use crate::catalogs::{CatalogNaming, StaticPools};
use crate::tree::{IdRange, NewTenant};
use crate::tree_settings::TreeSync;

const MARIADB_ENV: &str = "SPRIG_TEST_MARIADB_SINGLE";

static LOCK_MINUTES: Setting<i64> = Setting {
    key: "test_ctl.lock_minutes",
    default: 0,
    scopes: Scopes::TREE,
    hard: HardBounds::range(-1, 1_000_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "A test key at tenant (and ancestor), dc and global scope",
};
sprig_settings::register!(LOCK_MINUTES);

static OBJECT_LIMIT: Setting<u32> = Setting {
    key: "test_ctl.object_limit",
    default: 10,
    scopes: Scopes::ALL,
    hard: HardBounds::range(1, 1000),
    apply: Apply::Live,
    operator_only: false,
    owner: 8,
    doc: "A test key at every scope",
};
sprig_settings::register!(OBJECT_LIMIT);

fn tag() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    format!(
        "{:x}{:x}",
        (nanos ^ std::process::id()) & 0xff_ffff,
        N.fetch_add(1, Ordering::Relaxed)
    )
}

fn dc_of(store: &dyn MetaStore) -> DcId {
    match store.tier() {
        Tier::Dc(d) => d.clone(),
        Tier::Global => unreachable!("a DC tier"),
    }
}

async fn stores() -> (Arc<dyn MetaStore>, Arc<dyn MetaStore>) {
    match std::env::var(ETCD_ENV) {
        Ok(url) if !url.trim().is_empty() => {
            let cfg = EtcdConfig::new(vec![url]);
            (
                Arc::new(EtcdStore::connect(unique_tier(), &cfg).await.unwrap()),
                Arc::new(EtcdStore::connect(unique_tier(), &cfg).await.unwrap()),
            )
        }
        _ => (
            Arc::new(MemStore::new(unique_tier())),
            Arc::new(MemStore::new(Tier::Global)),
        ),
    }
}

struct Env {
    dc_store: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    service: Arc<TenantService<MariaBackend>>,
    tree: Arc<TreeSync<MariaBackend>>,
    locator: ShardMapLocator,
    tag: String,
}

impl Env {
    async fn new(pools: StaticPools) -> Self {
        let (dc_store, global) = stores().await;
        let tag = tag();
        let prefix = format!("__sprig_test_tenant_{}_{tag}_", std::process::id());
        let naming: CatalogNaming = Arc::new(move |t| format!("{prefix}{t}"));
        let locator = ShardMapLocator::new(Arc::clone(&dc_store), Arc::new(pools), naming);
        let v = Versioner::new(
            MariaBackend::new(Arc::new(locator.clone())),
            Arc::new(DirectoryAllocator::new(Arc::clone(&global))),
        );
        let tree = Arc::new(TreeSync::new(
            v,
            Directory::new(Arc::clone(&global)),
            Arc::new(SystemClock),
        ));
        let service = Arc::new(
            TenantService::new(
                Arc::clone(&dc_store),
                locator.clone(),
                Arc::clone(&tree),
                Arc::new(NoHooks),
                Arc::new(SystemClock),
                None,
            )
            .unwrap(),
        );
        Self {
            dc_store,
            global,
            service,
            tree,
            locator,
            tag,
        }
    }

    fn name(&self, n: &str) -> String {
        format!("test-{}-{n}", self.tag)
    }

    async fn create(
        &self,
        n: &str,
        parent: Option<&str>,
        cluster: Option<&ClusterId>,
    ) -> Result<CreateTenantResponse> {
        self.service
            .create(
                &CreateTenantRequest {
                    name: self.name(n),
                    parent: parent.map(|p| self.name(p)),
                    cluster: cluster.cloned(),
                },
                &[],
            )
            .await
    }

    async fn cleanup(&self, url: Option<&str>) {
        if let Some(url) = url {
            let mut c = mysql_async::Conn::new(mysql_async::Opts::from_url(url).unwrap())
                .await
                .unwrap();
            let like = format!(
                "__sprig\\_test\\_tenant\\_{}\\_{}\\_%",
                std::process::id(),
                self.tag
            );
            let schemas: Vec<String> = c
                .exec(
                    "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE ?",
                    (like,),
                )
                .await
                .unwrap();
            for s in schemas {
                c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
                    .await
                    .unwrap();
            }
            let _ = c.disconnect().await;
        }
        delete_prefix(&*self.dc_store, &self.dc_store.tier().root()).await;
        delete_prefix(&*self.global, &self.global.tier().root()).await;
    }
}

/// Runs `body`, then cleans up, also when it fails.
async fn with_env<F, Fut>(pools: StaticPools, url: Option<&str>, body: F)
where
    F: FnOnce(Arc<Env>) -> Fut,
    Fut: Future<Output = ()>,
{
    let env = Arc::new(Env::new(pools).await);
    let r = AssertUnwindSafe(body(Arc::clone(&env)))
        .catch_unwind()
        .await;
    env.cleanup(url).await;
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

fn cid(s: &str) -> ClusterId {
    ClusterId::new(s).unwrap()
}

fn code(e: &SprigError) -> u16 {
    e.protocol().mysql_errno
}

/// Without a server: placement is chosen and recorded before the catalog
/// is created, so a failed bootstrap leaves the tenant `creating` with its
/// shard map, and a re-run keeps the choice.
#[tokio::test]
async fn placement_is_recorded_before_the_catalog() {
    let pools = StaticPools::new()
        .with_url(cid("lt-c1"), "mysql://u:p@127.0.0.1:1")
        .unwrap()
        .with_url(cid("lt-c2"), "mysql://u:p@127.0.0.1:1")
        .unwrap();
    with_env(pools, None, |env| async move {
        // An unknown --cluster is refused before anything is written.
        let e = env
            .create("x", None, Some(&cid("lt-c9")))
            .await
            .unwrap_err();
        assert_eq!(code(&e), 39120);
        assert!(
            env.service
                .directory()
                .by_name(&env.name("x"))
                .await
                .unwrap()
                .is_none()
        );
        // The server is unreachable: bootstrap fails after placing.
        let e = env.create("a", None, None).await.unwrap_err();
        assert_ne!(code(&e), 39120, "{e}");
        let a = env
            .service
            .directory()
            .require(&env.name("a"))
            .await
            .unwrap();
        assert_eq!(a.body().state, TenantState::Creating);
        let id = a.body().tenant_id;
        assert_eq!(
            env.locator.cluster_of(id).await.unwrap(),
            Some(cid("lt-c1"))
        );
        let e2 = env
            .create("b", None, Some(&cid("lt-c1")))
            .await
            .unwrap_err();
        assert_ne!(code(&e2), 39120);
        let b = env
            .service
            .directory()
            .require(&env.name("b"))
            .await
            .unwrap();
        assert_eq!(b.body().placement_hint, Some(cid("lt-c1")));
        assert_eq!(
            env.locator.cluster_of(b.body().tenant_id).await.unwrap(),
            Some(cid("lt-c1"))
        );
        // Fewest tenants: c goes to lt-c2.
        let _ = env.create("c", None, None).await.unwrap_err();
        let c = env
            .service
            .directory()
            .require(&env.name("c"))
            .await
            .unwrap();
        assert_eq!(
            env.locator.cluster_of(c.body().tenant_id).await.unwrap(),
            Some(cid("lt-c2"))
        );
        // A re-run keeps the recorded choice; a child of a creating tenant
        // is refused.
        let _ = env.create("a", None, None).await.unwrap_err();
        assert_eq!(
            env.locator.cluster_of(id).await.unwrap(),
            Some(cid("lt-c1"))
        );
        assert_eq!(
            code(&env.create("k", Some("a"), None).await.unwrap_err()),
            39072
        );
        // Not deletable while creating; failed after the deadline, then
        // deletable (cleanup can't reach the server: the entry stays
        // deleting for the loop).
        assert_eq!(
            code(&env.service.delete(&env.name("a"), &[]).await.unwrap_err()),
            39072
        );
        env.service.fail(id, "test").await.unwrap();
        let v = env.service.delete(&env.name("a"), &[]).await.unwrap();
        assert_eq!(v.state, "deleting");
        // The loop's keys: creating and deleting tenants homed here.
        let lp = TenantLifecycle::new(Arc::clone(&env.service));
        let (cx, _stop) = crate::runtime::tests::cx_for(
            &crate::runtime::Stores {
                dc: Arc::clone(&env.dc_store),
                global: Arc::clone(&env.global),
            },
            &(Arc::new(SystemClock) as Arc<dyn Clock>),
            None,
            crate::settings::ControlSettings::default(),
        );
        let mut keys = lp.all_keys(&cx).await.unwrap();
        keys.sort();
        assert_eq!(keys, vec![id, b.body().tenant_id, c.body().tenant_id]);
        assert_eq!(dc_of(&*env.dc_store), *env.service.dc());
    })
    .await;
}

/// A move's retaining job of tenant `tenant`, as the mover writes it.
fn retaining_job(id: &str, tenant: TenantId) -> sprig_mover::JobRecord {
    let v = serde_json::json!({
        "job_id": id, "kind": "move_shard", "tenant_id": tenant.0,
        "spec": {"kind": "move_shard", "tenant": tenant.0,
            "sources": [{"cluster": "lt-c1", "shard": 0, "schemas": []}],
            "targets": [{"cluster": "lt-c2", "shard": 0, "schemas": []}],
            "routing": {"kind": "whole"}, "cutover": {"kind": "auto"},
            "verify": {"kind": "full"}, "requested_by": "test"},
        "state": "retaining", "retain_until_ms": u64::MAX / 2,
        "created_ms": 1, "updated_ms": 1, "v": 1
    });
    serde_json::from_value(v).unwrap()
}

/// Regression: a tenant moved within the last 24 hours
/// waits for its move job to drop the old copy. The tenant loop looks
/// again within seconds ([`CLEANUP_JOBS_RECHECK`]), not after the 30 s of
/// a blocked key, which outlasted `gateway-dev.sh down`'s wait: the
/// controller was stopped with the new home's catalog, accounts and name
/// rows still there. Once the job is done, cleanup moves on to the
/// clusters (here unreachable, so it fails instead of waiting).
#[tokio::test]
async fn a_deletion_waiting_for_its_move_is_looked_at_again_within_seconds() {
    use sprig_meta::elect::LeaderRecord;
    use sprig_meta::elect::{Election, Elector};
    use sprig_meta::session::SessionOpts;

    let pools = StaticPools::new()
        .with_url(cid("lt-c1"), "mysql://u:p@127.0.0.1:1")
        .unwrap();
    with_env(pools, None, |env| async move {
        let _ = env.create("m", None, None).await.unwrap_err();
        let id = env
            .service
            .directory()
            .require(&env.name("m"))
            .await
            .unwrap()
            .body()
            .tenant_id;
        env.service.fail(id, "test").await.unwrap();
        let jobs = sprig_mover::JobStore::new(Arc::clone(&env.dc_store), Arc::new(SystemClock));
        let rec = retaining_job("01a1c0ba-c97a-73f2-9f66-ea0d92ef9001", id);
        let r = env
            .dc_store
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Put {
                    key: env
                        .dc_store
                        .tier()
                        .key(&format!("mover/jobs/{}", rec.job_id.0))
                        .unwrap(),
                    value: Record::new(rec.clone()).encode().unwrap(),
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await
            .unwrap();
        assert!(r.succeeded);
        // Delete: the job's retention ends now, and cleanup waits for it.
        let v = env.service.delete(&env.name("m"), &[]).await.unwrap();
        assert_eq!(v.state, "deleting");
        let (after, rev) = jobs.get(&rec.job_id).await.unwrap();
        assert!(after.purge_tenant);
        assert!(after.retain_until_ms.unwrap() <= jobs.now_ms());
        assert_eq!(
            env.service.cleanup(id, &[]).await.unwrap(),
            Cleanup::WaitingForJobs(1)
        );
        // The loop looks again soon, not as a blocked key.
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let key = env.dc_store.tier().key("leader").unwrap();
        let leadership = Election::new(
            Arc::clone(&env.dc_store),
            Arc::clone(&clock),
            SessionOpts::new(Duration::from_secs(5)),
        )
        .campaign(
            &key,
            &LeaderRecord {
                holder: "ctl".to_owned(),
                dc: None,
                build: "test".to_owned(),
            },
        )
        .await
        .unwrap();
        let (cx, _stop) = crate::runtime::tests::cx_for(
            &crate::runtime::Stores {
                dc: Arc::clone(&env.dc_store),
                global: Arc::clone(&env.global),
            },
            &clock,
            Some(leadership),
            crate::settings::ControlSettings::default(),
        );
        let lp = TenantLifecycle::new(Arc::clone(&env.service));
        let out = lp.reconcile(&cx, &id).await.unwrap();
        assert_eq!(out, Outcome::RequeueAfter(CLEANUP_JOBS_RECHECK));
        assert!(CLEANUP_JOBS_RECHECK <= Duration::from_secs(5));
        // The worker finishes the job: cleanup goes on to the catalog's
        // cluster (unreachable here) instead of waiting.
        let mut done = after.clone();
        done.state = sprig_mover::JobState::Done;
        assert!(jobs.put_if(&done, rev).await.unwrap().is_some());
        let e = env.service.cleanup(id, &[]).await.unwrap_err();
        assert_ne!(code(&e), 39072, "{e}");
        assert!(env.service.directory().get(id).await.unwrap().is_some());
    })
    .await;
}

fn database(id: DbId, v: u64) -> DatabaseRow {
    DatabaseRow {
        db_id: id,
        name: "shop".into(),
        name_key: "shop".into(),
        default_charset: "utf8mb4".into(),
        default_collation: "utf8mb4_bin".into(),
        default_group_id: GroupId(0),
        state: DbState::Active,
        options: serde_json::json!({}),
        created_at: SystemTime::now(),
        created_by: None,
        changed_version: v,
    }
}

fn table(id: ObjectId, db: DbId, v: u64) -> ObjectRow {
    ObjectRow {
        object_id: id,
        db_id: db,
        namespace: Namespace::Relation,
        name: "orders".into(),
        name_key: "orders".into(),
        kind: ObjectKind::Table,
        parent_id: None,
        state: ObjectState::Active,
        schema_version: 1,
        flags: ObjectFlags::empty(),
        pending_job_id: None,
        created_at: SystemTime::now(),
        changed_version: v,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_bootstrap_tree_settings_and_delete() {
    let Some(url) = std::env::var(MARIADB_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipped live_bootstrap_tree_settings_and_delete: set {MARIADB_ENV} to run it");
        return;
    };
    let pools = StaticPools::new()
        .with_url(cid("lt-c1"), &url)
        .unwrap()
        .with_url(cid("lt-c2"), &url)
        .unwrap();
    with_env(pools, Some(&url), |env| async move {
        let registry = Registry::global().unwrap();
        // Create a → b → c.
        let a = env.create("a", None, None).await.unwrap();
        assert!(a.created);
        assert_eq!(a.tenant.state, "active");
        assert_eq!(a.tenant.cluster, Some(cid("lt-c1")));
        assert_eq!(a.placement.as_deref(), Some("fewest_tenants"));
        let b = env
            .create("b", Some("a"), Some(&cid("lt-c1")))
            .await
            .unwrap();
        assert_eq!(b.placement.as_deref(), Some("requested"));
        assert_eq!(b.tenant.depth, 1);
        let c = env.create("c", Some("b"), None).await.unwrap();
        assert_eq!(
            c.tenant.cluster,
            Some(cid("lt-c2")),
            "the cluster with fewer catalogs"
        );
        assert_eq!(c.tenant.ancestry, vec![env.name("a"), env.name("b")]);
        assert_eq!(c.tenant.parent.as_deref(), Some(env.name("b").as_str()));
        let (ida, idb, idc) = (a.tenant.tenant_id, b.tenant.tenant_id, c.tenant.tenant_id);
        // A re-run returns the tenant as it is.
        let again = env.create("b", Some("a"), None).await.unwrap();
        assert!(!again.created);
        assert_eq!(again.tenant.tenant_id, idb);
        assert_eq!(code(&env.create("b", None, None).await.unwrap_err()), 39360);
        // The catalogs: migrated, ancestry and progress recorded.
        let pool = env.locator.pools().writer(&cid("lt-c2")).await.unwrap();
        let store = Store::new(env.locator.schema(idc));
        let progress = read_progress(&pool, &store).await.unwrap().unwrap();
        assert_eq!(progress.step, BootstrapStep::Done);
        assert_eq!(progress.cluster, cid("lt-c2"));
        let mut conn = pool.get_conn().await.unwrap();
        let anc = store
            .internal(&mut conn, sprig_catalog::cache::snapshot::ANCESTRY_KEY)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(anc[0]["id"], ida.0);
        assert_eq!(anc[1]["name"], env.name("b"));
        drop(conn);
        let kv = env
            .dc_store
            .get(
                &ShardMap::catalog_key(env.dc_store.tier(), idc).unwrap(),
                Consistency::Linearizable,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            Record::<ShardMap>::decode(&kv.value).unwrap().body.group_id,
            0
        );
        let listed = TenantAdmin::list(&*env.service).await.unwrap();
        assert_eq!(listed.tenants.len(), 3);
        assert_eq!(
            listed.tenants[2].parent.as_deref(),
            Some(env.name("b").as_str())
        );

        // A bootstrap left half done (a crash after the directory write)
        // is finished by a re-run, as the loop does.
        let d = env
            .service
            .directory()
            .create(
                &NewTenant {
                    name: env.name("d"),
                    parent: Some(ida),
                    home_dc: env.service.dc().clone(),
                    placement_hint: None,
                },
                64,
                SystemTime::now(),
            )
            .await
            .unwrap();
        let idd = d.body().tenant_id;
        let done = env.service.bootstrap(idd, &[]).await.unwrap();
        assert_eq!(done.entry.state, TenantState::Active);
        assert_eq!(done.placement, "fewest_tenants");

        // Settings down the tree, over MariaDB.
        let writer = TenantSettingsWriter::new(
            env.tree.versioner().clone(),
            registry,
            Arc::new(sprig_catalog::FixedEtcd {
                rows: Arc::new(sprig_settings::MemSettings::new(registry)),
                dc: env.service.dc().clone(),
            }),
            Arc::new(AllowAll),
            Arc::new(LogDecisions),
            Arc::new(SystemClock),
        );
        let set = |t: TenantId, target: Target, key: &'static str, change: SettingChange| {
            let w = &writer;
            async move {
                w.set(&SetRequest {
                    tenant: t,
                    target,
                    key: key.to_owned(),
                    change,
                    principal: Principal::Operator("test".into()),
                    reason: "live test".into(),
                })
                .await
            }
        };
        let lm = LOCK_MINUTES.key;
        set(ida, Target::Tenant, lm, SettingChange::Set(Value::Int(30)))
            .await
            .unwrap();
        set(idb, Target::Tenant, lm, SettingChange::Set(Value::Int(10)))
            .await
            .unwrap();
        for t in [idb, idc] {
            assert!(matches!(
                env.tree.sync(t).await.unwrap().unwrap(),
                crate::tree_settings::SyncOutcome::Copied { .. }
            ));
        }

        // The reader: a catalog cache over MariaDB fed by the directory.
        let mem = MemStore::new(Tier::Dc(env.service.dc().clone()));
        let live = LiveSettings::start(
            registry,
            SettingsStores::new(Arc::new(mem.with_tier(Tier::Global)), Arc::new(mem)).unwrap(),
        )
        .unwrap();
        live.ready().await;
        let settings = Settings::new(live);
        let feed = DirectoryWatch::start(Arc::clone(&env.global)).unwrap();
        feed.ready().await;
        let cache = TenantCache::start(
            registry,
            Arc::new(MariaSource::new(Arc::new(env.locator.clone()), Vec::new()).unwrap()),
            Arc::new(feed),
            Some(Arc::new(settings.clone())),
            CacheConfig::default(),
        );
        let _pin = cache.pin(idc);
        let dc = env.service.dc().clone();
        let cat = cache.get(idc, Freshness::Latest).await.unwrap();
        let r = settings
            .resolve(lm, &cat.setting_ctx(&dc), Some(&cat.settings))
            .unwrap();
        assert_eq!(
            (r.value, r.source),
            (Value::Int(10), ScopeKind::Ancestor(idb))
        );
        // b changes later; c didn't override: c gets it.
        set(idb, Target::Tenant, lm, SettingChange::Set(Value::Int(20)))
            .await
            .unwrap();
        env.tree.sync(idc).await.unwrap();
        let cat = cache.get(idc, Freshness::Latest).await.unwrap();
        assert_eq!(
            settings
                .resolve(lm, &cat.setting_ctx(&dc), Some(&cat.settings))
                .unwrap()
                .value,
            Value::Int(20)
        );

        // Settings take effect: an object-scope change takes effect within
        // seconds; a value outside its bounds is refused.
        let (shop, orders) = (DbId(1), ObjectId(1));
        let mut ch = env
            .tree
            .versioner()
            .change(idc, VersionKind::Catalog)
            .await
            .unwrap();
        let v = ch.version();
        {
            let (s, q) = ch.tx().parts();
            s.insert_database(q, &database(shop, v)).await.unwrap();
            s.insert_object(q, &table(orders, shop, v)).await.unwrap();
        }
        ch.commit().await.unwrap();
        let ol = OBJECT_LIMIT.key;
        set(
            idc,
            Target::Tenant,
            ol,
            SettingChange::SetBounds(Bounds::max(50.0)),
        )
        .await
        .unwrap();
        let cat = cache.get(idc, Freshness::Latest).await.unwrap();
        let ctx = OwnedCtx {
            db: Some(shop),
            object: Some(orders),
            ..OwnedCtx::process(dc.clone())
        };
        let mut w = settings
            .watch_resolved(ol, ctx, Some(Arc::clone(&cat.settings)))
            .unwrap();
        assert_eq!(w.current().value, Value::Int(10));
        let t0 = std::time::Instant::now();
        set(
            idc,
            Target::Object(orders),
            ol,
            SettingChange::Set(Value::Int(25)),
        )
        .await
        .unwrap();
        let r = tokio::time::timeout(Duration::from_secs(5), w.changed())
            .await
            .expect("the object-scope change arrives within seconds")
            .unwrap();
        assert_eq!((r.value, r.source), (Value::Int(25), ScopeKind::Object));
        println!("live: object-scope change applied in {:?}", t0.elapsed());
        let e = set(
            idc,
            Target::Object(orders),
            ol,
            SettingChange::Set(Value::Int(51)),
        )
        .await
        .unwrap_err();
        assert_eq!(code(&e), 39080);
        drop(w);
        drop(cache);

        // Deletion: leaves only, children first.
        assert_eq!(
            code(&env.service.delete(&env.name("a"), &[]).await.unwrap_err()),
            39362
        );
        let e = env.service.delete(&env.name("c"), &[]).await;
        // c's catalog lists a live database: cleanup waits.
        assert_eq!(e.unwrap().state, "deleting");
        assert!(env.service.directory().get(idc).await.unwrap().is_some());
        let e = env.service.cleanup(idc, &[]).await.unwrap_err();
        assert_eq!(code(&e), 39072);
        let mut conn = pool.get_conn().await.unwrap();
        conn.query_drop(format!(
            "UPDATE `{}`.`databases` SET state = 'dropped'",
            store.schema()
        ))
        .await
        .unwrap();
        drop(conn);
        env.service.cleanup(idc, &[]).await.unwrap();
        for n in ["d", "b", "a"] {
            env.service.delete(&env.name(n), &[]).await.unwrap();
        }
        assert!(
            TenantAdmin::list(&*env.service)
                .await
                .unwrap()
                .tenants
                .is_empty()
        );
        let mut conn = pool.get_conn().await.unwrap();
        let left: Vec<String> = conn
            .exec(
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME IN (?, ?)",
                (env.locator.schema(ida), env.locator.schema(idc)),
            )
            .await
            .unwrap();
        assert!(left.is_empty(), "{left:?}");
        assert!(env.locator.cluster_of(ida).await.unwrap().is_none());
    })
    .await;
}

/// Ids are never reused, and bootstrap never adopts a catalog schema an
/// earlier tenant with the same id left behind: a new id is
/// raised past the catalogs on the clusters; a schema found anyway (the
/// counter behind, a cluster unread) is refused unless it carries this
/// tenant's mark, and deleting the refused tenant leaves it alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_catalog_schemas_are_never_reused() {
    let Some(url) = std::env::var(MARIADB_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipped live_catalog_schemas_are_never_reused: set {MARIADB_ENV} to run it");
        return;
    };
    let pools = StaticPools::new().with_url(cid("lt-c1"), &url).unwrap();
    with_env(pools, Some(&url), |env| {
        let url = url.clone();
        async move {
            let mut conn = mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
                .await
                .unwrap();
            let comment = |id: u64| {
                let schema = env.locator.schema(TenantId(id));
                format!(
                    "CREATE DATABASE `{schema}` COMMENT 'sprig tenant {id} an-earlier-incarnation'"
                )
            };

            // 1. The directory's counter is fresh (next id 1), but tenant 1's
            //    catalog is still on the cluster: the new tenant gets id 2.
            conn.query_drop(comment(1)).await.unwrap();
            let a = env.create("a", None, None).await.unwrap();
            assert_eq!(a.tenant.state, "active");
            assert_eq!(a.tenant.tenant_id, TenantId(2));
            let mark_a = mark_of(&mut conn, env.locator.schema(TenantId(2)))
                .await
                .unwrap();
            assert!(mark_a.starts_with("sprig tenant 2 "), "{mark_a}");

            // 2. The counter behind anyway (the entry is made directly, as
            //    if the cluster couldn't be read): bootstrap refuses tenant
            //    3's existing catalog, marked or not, and the tenant fails
            //    without a catalog shard map.
            for (n, earlier) in [("b", Some(comment(3))), ("c", None)] {
                let entry = env
                    .service
                    .directory()
                    .create(
                        &NewTenant {
                            name: env.name(n),
                            parent: None,
                            home_dc: env.service.dc().clone(),
                            placement_hint: None,
                        },
                        4,
                        SystemTime::now(),
                    )
                    .await
                    .unwrap();
                let id = entry.body().tenant_id;
                let schema = env.locator.schema(id);
                let stmt = earlier.unwrap_or_else(|| format!("CREATE DATABASE `{schema}`"));
                conn.query_drop(stmt).await.unwrap();
                let before = mark_of(&mut conn, schema.clone()).await.unwrap();
                let e = env.service.bootstrap(id, &[]).await.unwrap_err();
                assert_eq!(code(&e), 39072, "{e}");
                assert!(e.message.contains("isn't this tenant's"), "{e}");
                let got = env.service.directory().get(id).await.unwrap().unwrap();
                assert_eq!(got.body().state, TenantState::Failed);
                assert!(env.locator.cluster_of(id).await.unwrap().is_none());
                // Deleting it leaves the earlier tenant's schema as it was.
                env.service.delete(&env.name(n), &[]).await.unwrap();
                assert!(env.service.directory().get(id).await.unwrap().is_none());
                assert_eq!(mark_of(&mut conn, schema).await, Some(before));
            }

            // 3. A re-run adopts a schema with its own mark (a crash right
            //    after the CREATE DATABASE).
            let entry = env
                .service
                .directory()
                .create(
                    &NewTenant {
                        name: env.name("d"),
                        parent: None,
                        home_dc: env.service.dc().clone(),
                        placement_hint: None,
                    },
                    4,
                    SystemTime::now(),
                )
                .await
                .unwrap();
            let id = entry.body().tenant_id;
            let mark = owner_mark(entry.body());
            conn.query_drop(format!(
                "CREATE DATABASE `{}` COMMENT '{mark}'",
                env.locator.schema(id)
            ))
            .await
            .unwrap();
            let done = env.service.bootstrap(id, &[]).await.unwrap();
            assert_eq!(done.entry.state, TenantState::Active);

            // Deleting the tenants drops their own catalogs only.
            for n in ["a", "d"] {
                env.service.delete(&env.name(n), &[]).await.unwrap();
            }
            for (id, left) in [(1, true), (2, false), (3, true), (4, true), (5, false)] {
                assert_eq!(
                    mark_of(&mut conn, env.locator.schema(TenantId(id)))
                        .await
                        .is_some(),
                    left,
                    "tenant {id}'s catalog"
                );
            }
            let _ = conn.disconnect().await;
        }
    })
    .await;
}

/// `tenant.id_min..=tenant.id_max` (ctlfix, 9 Nov): only tenant-shaped
/// schemas inside the range raise the id counter, so another deployment's
/// schemas on the same servers no longer pull this one's ids into its
/// range, and a used-up range refuses with 39080.
#[tokio::test]
async fn live_tenant_ids_stay_in_the_deployments_range() {
    let Some(url) = std::env::var(MARIADB_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipped live_tenant_ids_stay_in_the_deployments_range: set {MARIADB_ENV}");
        return;
    };
    let pools = StaticPools::new().with_url(cid("lt-c1"), &url).unwrap();
    with_env(pools, Some(&url), |env| {
        let url = url.clone();
        async move {
            // Live settings with the range 10..=12 at global scope.
            let g = MemStore::new(Tier::Global);
            let d = g.with_tier(Tier::Dc(env.service.dc().clone()));
            for (k, v) in [("tenant.id_min", 10), ("tenant.id_max", 12)] {
                g.txn(sprig_meta::Txn {
                    when: vec![],
                    then: vec![sprig_meta::Op::Put {
                        key: g.tier().key(&format!("settings/{k}")).unwrap(),
                        value: format!(r#"{{"v":1,"value":{v},"set_by":"test"}}"#).into_bytes(),
                        lease: None,
                    }],
                    otherwise: vec![],
                })
                .await
                .unwrap();
            }
            let live = LiveSettings::start(
                Registry::global().unwrap(),
                SettingsStores::new(Arc::new(g.clone()), Arc::new(d)).unwrap(),
            )
            .unwrap();
            live.ready().await;
            let service = TenantService::new(
                Arc::clone(&env.dc_store),
                env.locator.clone(),
                Arc::clone(&env.tree),
                Arc::new(NoHooks),
                Arc::new(SystemClock),
                Some(live.clone()),
            )
            .unwrap();
            assert_eq!(service.id_range(), IdRange { min: 10, max: 12 });
            let mut conn = mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
                .await
                .unwrap();
            // Another deployment's catalog (id 100, outside the range) and
            // an earlier one of this deployment's (id 10, inside).
            for id in [100, 10] {
                conn.query_drop(format!(
                    "CREATE DATABASE `{}` COMMENT 'sprig tenant {id} elsewhere'",
                    env.locator.schema(TenantId(id))
                ))
                .await
                .unwrap();
            }
            let req = |n: &str| CreateTenantRequest {
                name: env.name(n),
                parent: None,
                cluster: None,
            };
            let a = service.create(&req("a"), &[]).await.unwrap();
            assert_eq!(a.tenant.tenant_id, TenantId(11), "past 10, not past 100");
            let b = service.create(&req("b"), &[]).await.unwrap();
            assert_eq!(b.tenant.tenant_id, TenantId(12));
            let e = service.create(&req("c"), &[]).await.unwrap_err();
            assert_eq!(code(&e), 39080, "{e}");
            assert!(e.message.contains("10..=12"), "{e}");
            for n in ["a", "b"] {
                service.delete(&env.name(n), &[]).await.unwrap();
            }
            let _ = conn.disconnect().await;
            drop(live);
        }
    })
    .await;
}

/// A schema's comment (its owner mark), `None` if it doesn't exist.
async fn mark_of(conn: &mut mysql_async::Conn, schema: String) -> Option<String> {
    conn.exec_first(
        "SELECT SCHEMA_COMMENT FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
        (schema,),
    )
    .await
    .unwrap()
}
