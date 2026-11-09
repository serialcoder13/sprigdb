//! The `registry` loop over `MemStore`, with nodes that refuse connections:
//! scheduling, settings per cluster, health publishing, the probing grace,
//! leader guards. Profiles from real servers are in sprigctl's Compose
//! test.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use sprig_cluster::health::ClusterHealth;
use sprig_cluster::probe::ProbeDepth;
use sprig_cluster::record::{
    AdminState, Capacity, NodeSpec, ProvisionerKind, SecretRefs, TopologyKind,
};
use sprig_cluster::registry::{ClusterRegistry, Registration};
use sprig_common::SprigCode;
use sprig_common::ids::{ClusterId, DcId, NodeId};
use sprig_common::time::SystemClock;
use sprig_meta::{Cmp, MemStore, MetaStore, Op, Tier, Txn};
use sprig_settings::{LiveSettings, Registry, SettingsStores};

use super::*;
use crate::runtime::Source;

pub(crate) struct Fixture {
    pub(crate) dc: MemStore,
    pub(crate) global: MemStore,
    pub(crate) live: LiveSettings,
    pub(crate) monitor: Arc<ClusterMonitor>,
}

pub(crate) async fn fixture(dc: &str) -> Fixture {
    let dcs = MemStore::new(Tier::Dc(DcId::new(dc).unwrap()));
    let global = dcs.with_tier(Tier::Global);
    let live = LiveSettings::start(
        Registry::global().unwrap(),
        SettingsStores::new(Arc::new(global.clone()), Arc::new(dcs.clone())).unwrap(),
    )
    .unwrap();
    live.ready().await;
    let registry = ClusterRegistry::new(Arc::new(global.clone()), Arc::new(dcs.clone())).unwrap();
    let monitor = Arc::new(ClusterMonitor::new(
        registry,
        live.clone(),
        Arc::new(SystemClock),
    ));
    Fixture {
        dc: dcs,
        global,
        live,
        monitor,
    }
}

/// A registration whose nodes refuse connections (port 1).
pub(crate) fn unreachable(name: &str, topology: TopologyKind) -> Registration {
    Registration {
        name: name.into(),
        topology,
        provisioner: ProvisionerKind::External,
        nodes: vec![NodeSpec {
            node: NodeId::new("n1").unwrap(),
            addr: "127.0.0.1:1".into(),
            zone: None,
        }],
        secrets: SecretRefs {
            ctl: "env:SPRIG_TEST_MONITOR_CTL".into(),
            repl: "env:SPRIG_TEST_MONITOR_REPL".into(),
        },
        capacity: Capacity::default(),
        declared: None,
    }
}

/// Writes a setting record straight into the store, as the DC leader's
/// settings writer would.
pub(crate) async fn put_setting(store: &MemStore, rel: &str, json: &str) {
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: store.tier().key(rel).unwrap(),
                value: json.as_bytes().to_vec(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
}

/// Becomes the "leader": a leader key and the guard on it.
pub(crate) async fn lead(store: &MemStore) -> Vec<Cmp> {
    let key = store.tier().key("leader").unwrap();
    let r = store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: key.clone(),
                value: b"{}".to_vec(),
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
    vec![Cmp::CreateRev(key, r.revision)]
}

async fn wait_for(what: &str, f: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[tokio::test]
async fn an_unreachable_cluster_is_unknown_and_follows_live_settings() {
    let f = fixture("mon-a").await;
    let guard = lead(&f.dc).await;
    let reg = f.monitor.registry().clone();
    let s = reg
        .register(unreachable("u1", TopologyKind::SingleNode))
        .await
        .unwrap();
    let id = s.body().cluster_id.clone();
    reg.set_admin_state(&id, AdminState::Active, vec![])
        .await
        .unwrap();

    let r = f
        .monitor
        .pass(&id, None, guard.clone())
        .await
        .unwrap()
        .unwrap();
    // No profile yet: the first pass goes deep.
    assert_eq!(r.depth, Some(ProbeDepth::Deep));
    assert_eq!(r.next, Duration::from_secs(5));
    let rec = r.cluster.unwrap();
    assert_eq!(
        rec.body().health.as_ref().unwrap().state,
        ClusterHealth::Unknown
    );
    assert!(
        rec.body().profile.is_some(),
        "an empty profile is published"
    );
    assert!(f.monitor.knows(&id));
    assert!(f.monitor.last_report(&id).await.is_some());

    // A dc value, then a cluster value that wins for this cluster only.
    put_setting(
        &f.dc,
        "settings/cluster.health_interval_seconds",
        r#"{"v":1,"value":7,"set_by":"test"}"#,
    )
    .await;
    wait_for("the dc value", || {
        f.monitor.settings_for(Some(&id)).health_interval == Duration::from_secs(7)
    })
    .await;
    put_setting(
        &f.dc,
        &format!("cluster-settings/{id}/cluster.health_interval_seconds"),
        r#"{"v":1,"value":2,"set_by":"test"}"#,
    )
    .await;
    wait_for("the cluster value", || {
        f.monitor.settings_for(Some(&id)).health_interval == Duration::from_secs(2)
    })
    .await;
    assert_eq!(
        f.monitor.settings_for(None).health_interval,
        Duration::from_secs(7)
    );
    // Within the interval a scheduled pass only waits, now for at most 2 s.
    let r = f
        .monitor
        .pass(&id, None, guard.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.depth, None);
    assert!(r.next <= Duration::from_secs(2), "{:?}", r.next);
    let r = f
        .monitor
        .pass(&id, Some(ProbeDepth::Light), guard)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.next, Duration::from_secs(2));
    drop(f.live);
}

#[tokio::test]
async fn probing_records_get_a_grace_period_and_guards_hold() {
    let f = fixture("mon-b").await;
    let guard = lead(&f.dc).await;
    let reg = f.monitor.registry().clone();
    let s = reg
        .register(unreachable("p1", TopologyKind::PrimaryReplica))
        .await
        .unwrap();
    let id = s.body().cluster_id.clone();
    let r = f
        .monitor
        .pass(&id, None, guard.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.depth, None, "a registration in flight owns the record");
    assert!(r.next > Duration::from_secs(25));
    // Forced (sprigctl cluster probe): gates run and reject it.
    let r = f
        .monitor
        .pass(&id, Some(ProbeDepth::Full), guard.clone())
        .await
        .unwrap()
        .unwrap();
    let rec = r.cluster.unwrap();
    assert_eq!(rec.body().admin_state, AdminState::Rejected);
    assert!(
        rec.body().gaps[0].starts_with("node n1: no sprig_ctl login"),
        "{:?}",
        rec.body().gaps
    );

    // A stale guard: 39504, nothing written.
    let stale = vec![Cmp::CreateRev(f.dc.tier().key("leader").unwrap(), 1)];
    let e = f.monitor.pass(&id, Some(ProbeDepth::Light), stale).await;
    let before = reg.get(&id).await.unwrap().unwrap().mod_revision;
    if let Err(e) = e {
        assert_eq!(e.code(), Some(SprigCode::NotLeader));
    }
    assert_eq!(reg.get(&id).await.unwrap().unwrap().mod_revision, before);

    // Gone: forgotten.
    let missing = ClusterId::new("mon-b-c99").unwrap();
    assert!(
        f.monitor
            .pass(&missing, None, guard)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!f.monitor.knows(&missing));
    let _ = &f.global;
}

/// Chapter 09, "Publishing": a pass in which no node answered (a loaded
/// host's timeouts) publishes nothing; the record keeps XA and the binlog
/// facts, a new term schedules a deep probe one sustain period after it
/// takes the profile over, and a deep probe whose active probes didn't run
/// is retried a sustain period later rather than the next day.
#[tokio::test]
async fn a_silent_cluster_keeps_its_published_capabilities() {
    use sprig_cluster::probe::ServerVersion;
    use sprig_cluster::profile::{CapabilityProfile, Publication};

    let f = fixture("mon-e").await;
    let guard = lead(&f.dc).await;
    let reg = f.monitor.registry().clone();
    let s = reg
        .register(unreachable("q1", TopologyKind::SingleNode))
        .await
        .unwrap();
    let id = s.body().cluster_id.clone();
    reg.set_admin_state(&id, AdminState::Active, vec![])
        .await
        .unwrap();
    let mut good = CapabilityProfile::default();
    good.server.version = Some(ServerVersion::new(11, 8, 9));
    good.server.default_storage_engine = "InnoDB".into();
    good.binlog.log_bin = true;
    good.binlog.gtid_strict_mode = true;
    good.derived.xa = true;
    good.derived.mover_source = true;
    good.derived.feed_source = true;
    let published = reg
        .publish_profile(
            &id,
            &Publication {
                profile: good,
                changes: Vec::new(),
            },
        )
        .await
        .unwrap();
    let version = published.body().profile_version;

    let r = f
        .monitor
        .pass(&id, None, guard.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.depth, Some(ProbeDepth::Full), "facts carried: not deep");
    assert_eq!(r.coverage, Some(Coverage::Blind));
    assert!(r.changes.is_empty(), "{:?}", r.changes);
    let rec = r.cluster.unwrap();
    assert_eq!(rec.body().profile_version, version);
    let p = rec.body().profile.as_ref().unwrap();
    assert!(p.derived.xa && p.binlog.log_bin && p.binlog.gtid_strict_mode);
    assert!(p.derived.mover_source && p.derived.feed_source);
    assert_eq!(
        rec.body().health.as_ref().unwrap().state,
        ClusterHealth::Unknown,
        "the silence shows in health instead"
    );
    let sustain = f.monitor.settings_for(Some(&id)).capability_upgrade_sustain;
    let due = r.deep_due_in.expect("a deep probe follows the takeover");
    assert!(
        due <= sustain && due + Duration::from_secs(5) > sustain,
        "{due:?}"
    );

    // A deep probe that couldn't run its active probes: retried later.
    let r = f
        .monitor
        .pass(&id, Some(ProbeDepth::Deep), guard)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.depth, Some(ProbeDepth::Deep));
    assert!(r.changes.is_empty(), "{:?}", r.changes);
    let due = r.deep_due_in.expect("the failed deep probe is retried");
    assert!(due > Duration::from_secs(5) && due <= sustain, "{due:?}");
    let rec = r.cluster.unwrap();
    assert_eq!(rec.body().profile_version, version);
    assert!(rec.body().profile.as_ref().unwrap().derived.xa);
    drop(f.live);
}

#[tokio::test]
async fn record_events_schedule_and_passes_debounce() {
    let f = fixture("mon-c").await;
    let l = RegistryLoop::new(Arc::clone(&f.monitor));
    let id = ClusterId::new("mon-c-c1").unwrap();
    let ev = |deleted: bool| MetaEvent {
        source: Source::Dc,
        key: f.dc.tier().key("clusters/mon-c-c1").unwrap(),
        kv: (!deleted).then(|| sprig_meta::KeyValue {
            key: f.dc.tier().key("clusters/mon-c-c1").unwrap(),
            value: Vec::new(),
            create_revision: 1,
            mod_revision: 1,
            version: 1,
            lease: None,
        }),
        mod_revision: 1,
    };
    assert_eq!(l.keys_for(&ev(false)), vec![id.clone()]);
    assert_eq!(l.keys_for(&ev(true)), vec![id]);
    assert_eq!(
        (l.name(), l.tier(), l.workers()),
        ("registry", LoopTier::DcLeader, 8)
    );

    let guard = lead(&f.dc).await;
    let reg = f.monitor.registry().clone();
    let s = reg
        .register(unreachable("d1", TopologyKind::SingleNode))
        .await
        .unwrap();
    let id = s.body().cluster_id.clone();
    reg.set_admin_state(&id, AdminState::Active, vec![])
        .await
        .unwrap();
    let first = f
        .monitor
        .pass(&id, None, guard.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(first.depth.is_some());
    // Woken again at once (its own write): no probe, just the wait.
    let again = f
        .monitor
        .pass(&id, None, guard.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.depth, None);
    assert!(again.next <= Duration::from_secs(5) && again.next > Duration::from_secs(4));
    // A forced pass always probes.
    let forced = f
        .monitor
        .pass(&id, Some(ProbeDepth::Light), guard)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(forced.depth, Some(ProbeDepth::Light));
}

/// A credentials file under the system temp directory with a `[sprig_ctl]`
/// and a `[sprig_repl]` table (development TLS), removed on drop.
struct CredsFile(std::path::PathBuf);

impl CredsFile {
    fn new(ctl: (&str, &str), repl: (&str, &str)) -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static N: AtomicU32 = AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "sprigdb-monitor-creds-{}-{}.toml",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let f = Self(path);
        f.write(ctl, repl);
        f
    }

    fn write(&self, ctl: (&str, &str), repl: (&str, &str)) {
        std::fs::write(
            &self.0,
            format!(
                "[sprig_ctl]\nuser = {:?}\npassword = {:?}\ntls = \"skip_verify\"\n\n\
                 [sprig_repl]\nuser = {:?}\npassword = {:?}\ntls = \"skip_verify\"\n",
                ctl.0, ctl.1, repl.0, repl.1
            ),
        )
        .unwrap();
    }

    fn secrets(&self) -> SecretRefs {
        let p = self.0.display();
        SecretRefs {
            ctl: format!("file:{p}#sprig_ctl"),
            repl: format!("file:{p}#sprig_repl"),
        }
    }
}

impl Drop for CredsFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn node(n: &str, addr: &str) -> NodeSpec {
    NodeSpec {
        node: NodeId::new(n).unwrap(),
        addr: addr.into(),
        zone: None,
    }
}

#[test]
fn deep_probes_get_sprig_repl_endpoints_in_node_order() {
    let creds = CredsFile::new(("root", "a"), ("repl_user", "b"));
    let id = ClusterId::new("c1").unwrap();
    let nodes = [node("n1", "10.0.0.1:3306"), node("n2", "10.0.0.2:3307")];
    let s = creds.secrets();
    for depth in [ProbeDepth::Light, ProbeDepth::Full] {
        assert!(
            repl_endpoints(&id, &s, &nodes, depth).is_none(),
            "{depth:?}"
        );
    }
    let eps = repl_endpoints(&id, &s, &nodes, ProbeDepth::Deep).unwrap();
    let got: Vec<(String, String, Option<String>)> = eps
        .iter()
        .map(|e| {
            (
                e.node.to_string(),
                e.addr(),
                e.opts.user().map(str::to_owned),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (
                "n1".into(),
                "10.0.0.1:3306".into(),
                Some("repl_user".into())
            ),
            (
                "n2".into(),
                "10.0.0.2:3307".into(),
                Some("repl_user".into())
            ),
        ]
    );
    // A credential that doesn't resolve skips the dump probe.
    let unset = SecretRefs {
        ctl: s.ctl.clone(),
        repl: "env:SPRIG_TEST_MONITOR_REPL_UNSET".into(),
    };
    assert!(repl_endpoints(&id, &unset, &nodes, ProbeDepth::Deep).is_none());
}

/// The single Compose node, as `(user, password, host:port)`.
fn single_node() -> Option<(String, String, String)> {
    let Some(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE")
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        let strict = std::env::var("SPRIG_TEST_STRICT")
            .is_ok_and(|s| matches!(s.trim(), "1" | "true" | "yes" | "on"));
        assert!(
            !strict,
            "SPRIG_TEST_MARIADB_SINGLE is not set and SPRIG_TEST_STRICT is on"
        );
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return None;
    };
    let url = url.split(',').next().unwrap().trim();
    let rest = url.strip_prefix("mysql://").unwrap();
    let (auth, host) = rest.split_once('@').unwrap();
    let (user, pass) = auth.split_once(':').unwrap();
    let host = host.split('/').next().unwrap();
    Some((user.into(), pass.into(), host.into()))
}

/// Daily and `--full` deep passes run the binlog dump probe as `sprig_repl`,
/// as registration does: it passes with a working credential, and a
/// failing one turns the feed off on every deep pass, rather than only at
/// registration (the published profile would otherwise lapse to the feed
/// settings' verdict after the upgrade sustain period).
#[tokio::test]
async fn deep_passes_run_the_binlog_dump_probe() {
    let Some((user, pass, addr)) = single_node() else {
        return;
    };
    let creds = CredsFile::new((&user, &pass), (&user, &pass));
    let f = fixture("mon-dump").await;
    let guard = lead(&f.dc).await;
    let reg = f.monitor.registry().clone();
    let mut r = unreachable("dump1", TopologyKind::SingleNode);
    r.nodes = vec![node("n1", &addr)];
    r.secrets = creds.secrets();
    let id = reg.register(r).await.unwrap().body().cluster_id.clone();
    reg.set_admin_state(&id, AdminState::Active, vec![])
        .await
        .unwrap();

    let dump = |m: &ClusterMonitor| {
        let cell = m.cell(&id);
        let st = cell.try_lock().unwrap();
        st.last_probes[0]
            .active
            .as_ref()
            .expect("the deep probe's active probes")
            .binlog_dump
            .clone()
    };
    let pass1 = f
        .monitor
        .pass(&id, Some(ProbeDepth::Deep), guard.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pass1.depth, Some(ProbeDepth::Deep));
    let d = dump(&f.monitor).expect("the dump probe ran");
    assert!(d.passed(), "{d:?}");
    assert!(d.events.iter().any(|e| e == "format_description"), "{d:?}");

    // sprig_repl's password stops working: the next deep pass says so.
    creds.write((&user, &pass), (&user, "not-the-password"));
    let pass2 = f
        .monitor
        .pass(&id, Some(ProbeDepth::Deep), guard.clone())
        .await
        .unwrap()
        .unwrap();
    let d = dump(&f.monitor).expect("the dump probe ran");
    let failure = d.failure.as_ref().expect("a login failure");
    assert_eq!(
        failure.kind,
        sprig_cluster::probe::dump::DumpFailureKind::Login,
        "{d:?}"
    );
    let profile = pass2.cluster.unwrap().body().profile.clone().unwrap();
    assert_eq!(
        profile.derived.feed_level,
        sprig_cluster::probe::feed::FeedLevel::Unsupported
    );
    assert!(!profile.derived.feed_source);

    // Light passes don't run it.
    f.monitor
        .pass(&id, Some(ProbeDepth::Light), guard)
        .await
        .unwrap()
        .unwrap();
    let cell = f.monitor.cell(&id);
    assert!(cell.try_lock().unwrap().last_probes[0].active.is_none());
    drop(f.live);
}
