//! Unit and property tests of the version protocol on `MemStore` (the
//! global tier) and `MemBackend` (the catalog). The same protocol runs on
//! etcd and MariaDB in `live_tests`.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::many_single_char_names
)]

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use proptest::prelude::*;
use serde_json::{Value as Json, json};
use sprig_common::SprigCode;
use sprig_common::ids::TenantId;
use sprig_meta::elect::{Election, Elector, LeaderRecord};
use sprig_meta::session::{SessionOpts, TokioClock};
use sprig_meta::{Consistency, MemStore, MetaStore, Op, Tier, Txn};

use super::mem::MemBackend;
use super::*;

const T: TenantId = TenantId(42);

fn global() -> Arc<MemStore> {
    Arc::new(MemStore::new(Tier::Global))
}

async fn seed(store: &dyn MetaStore, tenant: TenantId, body: Json) {
    let key = Tier::Global.key(&format!("tenants/{tenant}")).unwrap();
    let r = store
        .txn(Txn {
            then: vec![Op::Put {
                key,
                value: serde_json::to_vec(&body).unwrap(),
                lease: None,
            }],
            ..Txn::default()
        })
        .await
        .unwrap();
    assert!(r.succeeded);
}

async fn directory(store: &dyn MetaStore, tenant: TenantId) -> Json {
    let key = Tier::Global.key(&format!("tenants/{tenant}")).unwrap();
    let kv = store
        .get(&key, Consistency::Linearizable)
        .await
        .unwrap()
        .unwrap();
    serde_json::from_slice(&kv.value).unwrap()
}

fn entry() -> Json {
    json!({"v": 1, "tenant_id": 42, "name": "acme", "state": "active", "home_dc": "mum1",
           "catalog_version": 0, "auth_version": 0})
}

async fn setup() -> (Arc<MemStore>, Versioner<MemBackend>) {
    let g = global();
    seed(&*g, T, entry()).await;
    let backend = MemBackend::new();
    backend.bootstrap(T);
    let alloc = DirectoryAllocator::new(g.clone()).with_timeout(Duration::from_millis(300));
    (g, Versioner::new(backend, Arc::new(alloc)))
}

/// Fails once at `step`, as a crash there would.
fn crash_at(step: Step) -> Failpoint {
    Arc::new(move |_, _, s| {
        if s == step {
            Err(SprigError::internal(format!("injected crash at {s:?}")))
        } else {
            Ok(())
        }
    })
}

async fn commit_one(v: &Versioner<MemBackend>, kind: VersionKind) -> Result<u64> {
    let mut c = v.change(T, kind).await?;
    let n = c.version();
    c.tx().write(format!("row@{n}"));
    c.commit().await
}

#[tokio::test(start_paused = true)]
async fn changes_allocate_globally_and_commit_in_order() {
    let (g, v) = setup().await;
    for want in 1..=3 {
        assert_eq!(commit_one(&v, VersionKind::Catalog).await.unwrap(), want);
    }
    assert_eq!(commit_one(&v, VersionKind::Auth).await.unwrap(), 1);
    let cat = v.backend().read(T).unwrap();
    assert_eq!((cat.catalog, cat.auth), (3, 1));
    let versions: Vec<u64> = cat.commits.iter().map(|c| c.version).collect();
    assert_eq!(versions, [1, 2, 3, 1]);
    assert_eq!(cat.commits[1].rows, ["row@2"]);
    let d = directory(&*g, T).await;
    assert_eq!(d["catalog_version"], 3);
    assert_eq!(d["auth_version"], 1);
    // Fields this crate doesn't own are kept.
    assert_eq!(d["name"], "acme");
    assert_eq!(d["home_dc"], "mum1");
    assert_eq!(d["settings_version"], 0);
}

#[tokio::test(start_paused = true)]
async fn tenant_settings_changes_move_settings_version() {
    let (g, v) = setup().await;
    commit_one(&v, VersionKind::Catalog).await.unwrap();
    let c = v
        .change_with(
            T,
            VersionKind::Catalog,
            ChangeOpts {
                tenant_settings: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(c.commit().await.unwrap(), 2);
    commit_one(&v, VersionKind::Catalog).await.unwrap();
    // Auth changes never move it.
    v.change_with(
        T,
        VersionKind::Auth,
        ChangeOpts {
            tenant_settings: true,
        },
    )
    .await
    .unwrap()
    .commit()
    .await
    .unwrap();
    let d = directory(&*g, T).await;
    assert_eq!(d["settings_version"], 2);
    assert_eq!(d["catalog_version"], 3);
}

#[tokio::test(start_paused = true)]
async fn missing_tenants_are_not_ready() {
    let (_g, v) = setup().await;
    // No directory entry.
    v.backend().bootstrap(TenantId(7));
    let e = v
        .change(TenantId(7), VersionKind::Catalog)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::TenantNotReady));
    // No catalog.
    let e = v
        .change(TenantId(8), VersionKind::Catalog)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::TenantNotReady));
    // The failed change released its lock.
    let c = tokio::time::timeout(
        Duration::from_secs(1),
        v.change(TenantId(7), VersionKind::Catalog),
    )
    .await
    .unwrap();
    assert!(c.is_err());
}

#[tokio::test(start_paused = true)]
async fn an_unreachable_global_tier_fails_39070_with_no_gap() {
    let (g, v) = setup().await;
    commit_one(&v, VersionKind::Catalog).await.unwrap();
    g.set_unavailable(true);
    let e = v.change(T, VersionKind::Catalog).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::CatalogUnavailable));
    assert!(e.retryable);
    g.set_unavailable(false);
    // Rolled back and unlocked; nothing allocated.
    assert_eq!(directory(&*g, T).await["catalog_version"], 1);
    assert_eq!(commit_one(&v, VersionKind::Catalog).await.unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn concurrent_changes_serialize_per_tenant() {
    let (g, v) = setup().await;
    let mut tasks = Vec::new();
    for i in 0..20 {
        let v = v.clone();
        let kind = if i % 4 == 0 {
            VersionKind::Auth
        } else {
            VersionKind::Catalog
        };
        tasks.push(tokio::spawn(async move {
            let mut c = v.change(T, kind).await.unwrap();
            let n = c.version();
            // Yield while holding the lock: others must wait, not overtake.
            tokio::time::sleep(Duration::from_millis(5)).await;
            c.tx().write(format!("{n}"));
            c.commit().await.unwrap()
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let cat = v.backend().read(T).unwrap();
    for kind in VersionKind::ALL {
        let seen: Vec<u64> = cat
            .commits
            .iter()
            .filter(|c| c.kind == kind)
            .map(|c| c.version)
            .collect();
        let want: Vec<u64> = (1..=seen.len() as u64).collect();
        assert_eq!(
            seen, want,
            "{kind:?}: no gaps, commit order = version order"
        );
    }
    let d = directory(&*g, T).await;
    assert_eq!(d["catalog_version"], 15);
    assert_eq!(d["auth_version"], 5);
}

#[tokio::test(start_paused = true)]
async fn crashes_at_each_step_leave_no_half_change() {
    for (step, gap, committed) in [
        (Step::Locked, false, false),
        (Step::Allocated, true, false),
        (Step::BeforeCommit, true, false),
        (Step::Committed, false, true),
    ] {
        let (g, v) = setup().await;
        let crashing = v.clone().with_failpoint(crash_at(step));
        let r = async {
            let mut c = crashing.change(T, VersionKind::Catalog).await?;
            c.tx().write("half");
            c.commit().await
        }
        .await;
        assert!(r.is_err(), "{step:?}");
        let cat = v.backend().read(T).unwrap();
        let dir = directory(&*g, T).await["catalog_version"].as_u64().unwrap();
        assert_eq!(cat.catalog, u64::from(committed), "{step:?}");
        assert_eq!(
            cat.commits.iter().any(|c| c.rows == ["half"]),
            committed,
            "{step:?}: rows are visible only with the whole change"
        );
        assert_eq!(dir, u64::from(gap || committed), "{step:?}");
        // The lock was released by the "crash": the next change proceeds
        // above any gap.
        let next =
            tokio::time::timeout(Duration::from_secs(1), commit_one(&v, VersionKind::Catalog))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(next, dir + 1, "{step:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn settle_closes_a_gap_and_is_a_no_op_otherwise() {
    let (g, v) = setup().await;
    commit_one(&v, VersionKind::Catalog).await.unwrap();
    assert_eq!(v.settle(T, VersionKind::Catalog).await.unwrap(), 1);
    let crashing = v.clone().with_failpoint(crash_at(Step::BeforeCommit));
    for _ in 0..2 {
        let c = crashing.change(T, VersionKind::Catalog).await.unwrap();
        assert!(c.commit().await.is_err());
    }
    assert_eq!(v.backend().read(T).unwrap().catalog, 1);
    assert_eq!(directory(&*g, T).await["catalog_version"], 3);
    assert_eq!(v.settle(T, VersionKind::Catalog).await.unwrap(), 3);
    let cat = v.backend().read(T).unwrap();
    assert_eq!(cat.catalog, 3);
    assert!(
        cat.commits.last().unwrap().rows.is_empty(),
        "an empty change"
    );
    let c = v.change(T, VersionKind::Catalog).await.unwrap();
    assert_eq!((c.previous(), c.version()), (3, 4));
    c.rollback().await.unwrap();
}

/// A change rolled back on purpose (nothing to write) leaves
/// no gap, so `Freshness::Latest` loaders don't wait for it; an abandoned
/// one (a crash) still does, until the next change or `settle`.
#[tokio::test(start_paused = true)]
async fn rollback_settles_its_version_and_abandon_leaves_a_gap() {
    let (g, v) = setup().await;
    commit_one(&v, VersionKind::Catalog).await.unwrap();
    let c = v.change(T, VersionKind::Catalog).await.unwrap();
    assert_eq!(c.version(), 2);
    c.rollback().await.unwrap();
    assert_eq!(directory(&*g, T).await["catalog_version"], 2);
    let cat = v.backend().read(T).unwrap();
    assert_eq!(cat.catalog, 2, "the rolled-back version is committed empty");
    assert!(cat.commits.last().unwrap().rows.is_empty());
    let c = v.change(T, VersionKind::Catalog).await.unwrap();
    assert_eq!((c.previous(), c.version()), (2, 3));
    c.abandon().await.unwrap();
    assert_eq!(v.backend().read(T).unwrap().catalog, 2, "a gap");
    assert_eq!(directory(&*g, T).await["catalog_version"], 3);
    assert_eq!(commit_one(&v, VersionKind::Catalog).await.unwrap(), 4);
    // Auth versions settle the same way, on their own row.
    let a = v.change(T, VersionKind::Auth).await.unwrap();
    a.rollback().await.unwrap();
    assert_eq!(v.backend().read(T).unwrap().auth, 1);
    assert_eq!(directory(&*g, T).await["auth_version"], 1);
}

#[tokio::test(start_paused = true)]
async fn a_directory_behind_the_catalog_never_reissues_versions() {
    let (g, v) = setup().await;
    for _ in 0..5 {
        commit_one(&v, VersionKind::Catalog).await.unwrap();
    }
    // The directory is restored from an old backup.
    let mut old = entry();
    old["catalog_version"] = json!(2);
    seed(&*g, T, old).await;
    let e = v.settle(T, VersionKind::Catalog).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::Internal));
    assert_eq!(commit_one(&v, VersionKind::Catalog).await.unwrap(), 6);
    assert_eq!(directory(&*g, T).await["catalog_version"], 6);
}

#[tokio::test(start_paused = true)]
async fn concurrent_allocations_are_unique_and_contiguous() {
    let g = global();
    seed(&*g, T, entry()).await;
    let a = Arc::new(DirectoryAllocator::new(g.clone()));
    let mut tasks = Vec::new();
    for _ in 0..25 {
        let a = Arc::clone(&a);
        tasks.push(tokio::spawn(async move {
            a.allocate(T, VersionKind::Catalog, 0, false).await.unwrap()
        }));
    }
    let mut got = BTreeSet::new();
    for t in tasks {
        assert!(got.insert(t.await.unwrap()));
    }
    assert_eq!(got, (1..=25).collect());
    assert_eq!(a.current(T, VersionKind::Catalog).await.unwrap(), 25);
}

#[tokio::test(start_paused = true)]
async fn records_from_newer_builds_are_not_rewritten() {
    let g = global();
    let mut e = entry();
    e["v"] = json!(2);
    seed(&*g, T, e).await;
    let a = DirectoryAllocator::new(g.clone());
    let err = a
        .allocate(T, VersionKind::Catalog, 0, false)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(SprigCode::Internal));
    assert_eq!(directory(&*g, T).await["catalog_version"], 0);
}

#[tokio::test(start_paused = true)]
async fn lost_leadership_refuses_to_allocate_or_commit() {
    let mem = MemStore::with_clock(Tier::Global, Arc::new(TokioClock::new()));
    let g: Arc<dyn MetaStore> = Arc::new(mem.clone());
    seed(&*g, T, entry()).await;
    let election = Election::new(
        Arc::clone(&g),
        Arc::new(TokioClock::new()),
        SessionOpts::new(Duration::from_secs(5)),
    );
    let key = Tier::Global.key("leader").unwrap();
    let me = LeaderRecord {
        holder: "ctl-1".to_owned(),
        dc: Some("mum1".to_owned()),
        build: "test".to_owned(),
    };
    let leadership = election.campaign(&key, &me).await.unwrap();
    let backend = MemBackend::new();
    backend.bootstrap(T);
    let v = Versioner::new(
        backend,
        Arc::new(DirectoryAllocator::leader(leadership.clone())),
    );
    assert_eq!(commit_one(&v, VersionKind::Catalog).await.unwrap(), 1);

    // Leadership ends after allocation: the commit is refused (a gap).
    let c = v.change(T, VersionKind::Catalog).await.unwrap();
    assert_eq!(c.version(), 2);
    // Another controller takes over: the key is deleted (lease expiry)
    // and re-created by the new leader.
    g.txn(Txn {
        then: vec![Op::Delete(key.clone())],
        ..Txn::default()
    })
    .await
    .unwrap();
    let other = Election::new(
        Arc::clone(&g),
        Arc::new(TokioClock::new()),
        SessionOpts::new(Duration::from_secs(5)),
    );
    let _new_leader = other
        .campaign(
            &key,
            &LeaderRecord {
                holder: "ctl-2".to_owned(),
                ..me.clone()
            },
        )
        .await
        .unwrap();
    // The deposed leader's next allocation fails its token compare without
    // writing (an auth change: the catalog row is still locked by `c`), and
    // that tells it it no longer leads...
    let e = v.change(T, VersionKind::Auth).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::NotLeader));
    assert!(!leadership.is_leading());
    assert_eq!(directory(&*g, T).await["auth_version"], 0);
    // ...so the change opened before the takeover can't commit: a gap.
    let e = c.commit().await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::NotLeader));
    assert_eq!(v.backend().read(T).unwrap().catalog, 1);
    assert_eq!(directory(&*g, T).await["catalog_version"], 2);
}

#[tokio::test(start_paused = true)]
async fn a_lost_session_stops_the_commit() {
    let mem = MemStore::with_clock(Tier::Global, Arc::new(TokioClock::new()));
    let g: Arc<dyn MetaStore> = Arc::new(mem);
    seed(&*g, T, entry()).await;
    let election = Election::new(
        Arc::clone(&g),
        Arc::new(TokioClock::new()),
        SessionOpts::new(Duration::from_secs(5)),
    );
    let key = Tier::Global.key("leader").unwrap();
    let me = LeaderRecord {
        holder: "ctl-1".to_owned(),
        dc: None,
        build: "test".to_owned(),
    };
    let leadership = election.campaign(&key, &me).await.unwrap();
    let backend = MemBackend::new();
    backend.bootstrap(T);
    let v = Versioner::new(
        backend,
        Arc::new(DirectoryAllocator::leader(leadership.clone())),
    );
    let c = v.change(T, VersionKind::Catalog).await.unwrap();
    // The process hangs past its lease: the session counts as lost.
    leadership.session.halt_keep_alive();
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(!leadership.is_leading());
    let e = c.commit().await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::NotLeader));
    assert_eq!(v.backend().read(T).unwrap().catalog, 0);
    let e = v.change(T, VersionKind::Catalog).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::NotLeader));
}

// ---- property test ----

#[derive(Debug, Clone)]
struct OpSpec {
    auth: bool,
    tenant_settings: bool,
    crash: Option<Step>,
    hold_ms: u64,
}

fn op_spec() -> impl Strategy<Value = OpSpec> {
    (
        any::<bool>(),
        any::<bool>(),
        prop_oneof![
            6 => Just(None),
            1 => Just(Some(Step::Locked)),
            1 => Just(Some(Step::Allocated)),
            1 => Just(Some(Step::BeforeCommit)),
            1 => Just(Some(Step::Committed)),
        ],
        0_u64..4,
    )
        .prop_map(|(auth, tenant_settings, crash, hold_ms)| OpSpec {
            auth,
            tenant_settings,
            crash,
            hold_ms,
        })
}

/// What one operation did, as the test observed it.
#[derive(Debug, Clone, Copy)]
struct Outcome {
    kind: VersionKind,
    allocated: Option<u64>,
    committed: bool,
    tenant_settings: bool,
}

fn run_ops(ops: Vec<OpSpec>) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    rt.block_on(async move {
        let (g, v) = setup().await;
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for op in ops {
            let kind = if op.auth {
                VersionKind::Auth
            } else {
                VersionKind::Catalog
            };
            let v = match op.crash {
                Some(step) => v.clone().with_failpoint(crash_at(step)),
                None => v.clone(),
            };
            let outcomes = Arc::clone(&outcomes);
            tasks.push(tokio::spawn(async move {
                let mut out = Outcome {
                    kind,
                    allocated: None,
                    committed: false,
                    tenant_settings: op.tenant_settings && kind == VersionKind::Catalog,
                };
                let opts = ChangeOpts {
                    tenant_settings: op.tenant_settings,
                };
                if let Ok(mut c) = v.change_with(T, kind, opts).await {
                    let n = c.version();
                    out.allocated = Some(n);
                    tokio::time::sleep(Duration::from_millis(op.hold_ms)).await;
                    c.tx().write(format!("{}:{n}:a", kind.label()));
                    c.tx().write(format!("{}:{n}:b", kind.label()));
                    // A crash at Committed still commits.
                    out.committed = c.commit().await.is_ok() || op.crash == Some(Step::Committed);
                } else if op.crash == Some(Step::Allocated) {
                    // Allocated, then crashed: we never saw the number.
                    out.allocated = Some(0);
                }
                outcomes.lock().unwrap().push(out);
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let outcomes = outcomes.lock().unwrap().clone();
        let cat = v.backend().read(T).unwrap();
        let d = directory(&*g, T).await;
        let mut dir_versions = HashMap::new();
        for kind in VersionKind::ALL {
            let field = match kind {
                VersionKind::Catalog => "catalog_version",
                VersionKind::Auth => "auth_version",
            };
            let dir = d[field].as_u64().unwrap();
            dir_versions.insert(kind, dir);
            let commits: Vec<_> = cat.commits.iter().filter(|c| c.kind == kind).collect();
            // Monotonic in commit order, and every row carries its version.
            for w in commits.windows(2) {
                assert!(w[0].version < w[1].version, "{kind:?}: {commits:?}");
            }
            for c in &commits {
                let n = c.version;
                assert_eq!(
                    c.rows,
                    [
                        format!("{}:{n}:a", kind.label()),
                        format!("{}:{n}:b", kind.label())
                    ]
                );
            }
            let mine: Vec<_> = outcomes.iter().filter(|o| o.kind == kind).collect();
            let allocations = mine.iter().filter(|o| o.allocated.is_some()).count() as u64;
            let committed: BTreeSet<u64> = commits.iter().map(|c| c.version).collect();
            // Every allocation took exactly one new version.
            assert_eq!(dir, allocations, "{kind:?}");
            // What committed is what the callers saw commit.
            let seen: BTreeSet<u64> = mine
                .iter()
                .filter(|o| o.committed)
                .filter_map(|o| o.allocated)
                .collect();
            assert_eq!(committed, seen, "{kind:?}");
            // Gaps are exactly the allocations that never committed.
            let gaps = (1..=dir).filter(|n| !committed.contains(n)).count();
            let lost = mine
                .iter()
                .filter(|o| o.allocated.is_some() && !o.committed)
                .count();
            assert_eq!(gaps, lost, "{kind:?}");
            assert_eq!(cat.version(kind), committed.last().copied().unwrap_or(0));
            assert!(cat.version(kind) <= dir);
        }
        // settings_version is the newest catalog allocation that touched
        // tenant settings (committed or not)
        let settings = outcomes
            .iter()
            .filter(|o| o.tenant_settings)
            .filter_map(|o| o.allocated)
            .max()
            .unwrap_or(0);
        // (A crash right after allocation hides its number: at least the
        // newest seen, at most the newest catalog version.)
        let sv = d["settings_version"].as_u64().unwrap_or(0);
        assert!(sv >= settings && sv <= dir_versions[&VersionKind::Catalog]);
        // Settling closes every gap.
        for kind in VersionKind::ALL {
            assert_eq!(v.settle(T, kind).await.unwrap(), dir_versions[&kind]);
            assert_eq!(
                v.backend().read(T).unwrap().version(kind),
                dir_versions[&kind]
            );
        }
    });
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn versions_stay_monotonic_and_gaps_are_explained(ops in prop::collection::vec(op_spec(), 1..24)) {
        run_ops(ops);
    }
}
