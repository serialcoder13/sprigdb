#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use sprig_cluster::record::{Capacity, NodeSpec, ProvisionerKind, SecretRefs, TopologyKind};
use sprig_cluster::registry::Registration;
use sprig_common::ids::{DbId, GroupId, NodeId, ShardId, TenantId};
use sprig_common::time::SystemClock;
use sprig_meta::{MemStore, MetaStore, Tier, TierView};
use sprig_txn::xid::bqual;
use sprig_txn::{AnchorRef, Decision, DecisionRow, Dtid, DtidGen, Gtrid, StaticLocator, Xid};

use super::*;

const T: TenantId = TenantId(3_930_650_002);

fn dc() -> DcId {
    DcId::new("mum1").unwrap()
}

fn registration(name: &str) -> Registration {
    Registration {
        name: name.into(),
        topology: TopologyKind::SingleNode,
        provisioner: ProvisionerKind::External,
        nodes: vec![NodeSpec {
            node: NodeId::new("n1").unwrap(),
            addr: "10.0.0.1:3306".into(),
            zone: None,
        }],
        secrets: SecretRefs {
            ctl: "file:/x".into(),
            repl: "file:/x".into(),
        },
        capacity: Capacity::default(),
        declared: None,
    }
}

/// A registry with one cluster in each state, by name.
async fn registry() -> (ClusterRegistry, BTreeMap<&'static str, ClusterId>) {
    use AdminState::{Active, Drained, Draining, Rejected, Retired};
    let store: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc())));
    let reg =
        ClusterRegistry::new(TierView::over(&store, Tier::Global), Arc::clone(&store)).unwrap();
    let paths: [(&str, &[AdminState]); 6] = [
        ("probing", &[]),
        ("active", &[Active]),
        ("rejected", &[Rejected]),
        ("draining", &[Active, Draining]),
        ("drained", &[Active, Draining, Drained]),
        ("retired", &[Active, Draining, Drained, Retired]),
    ];
    let mut ids = BTreeMap::new();
    for (name, path) in paths {
        let id = reg
            .register(registration(name))
            .await
            .unwrap()
            .body()
            .cluster_id
            .clone();
        for s in path {
            reg.set_admin_state(&id, *s, Vec::new()).await.unwrap();
        }
        ids.insert(name, id);
    }
    (reg, ids)
}

fn cx() -> LoopCx {
    let store = MemStore::new(Tier::Dc(dc()));
    LoopCx::new(
        Arc::new(store.clone()),
        Arc::new(store.with_tier(Tier::Global)),
        None,
        Arc::new(SystemClock),
        sprig_common::CancellationToken::new(),
        tokio::sync::watch::channel(Arc::new(crate::settings::ControlSettings::default())).1,
    )
}

/// An admin with prepared branches per cluster; nothing recorded yet, so
/// every race inserts `abort`.
#[derive(Default)]
struct FakeAdmin {
    prepared: Mutex<BTreeMap<ClusterId, Vec<Xid>>>,
    unreachable: Mutex<Vec<ClusterId>>,
    finished: Mutex<Vec<(ClusterId, bool)>>,
    rows: Mutex<Vec<(ClusterId, String, DecisionRow)>>,
}

#[async_trait::async_trait]
impl ClusterAdmin for FakeAdmin {
    async fn xa_recover(&self, c: &ClusterId) -> DbResult<Vec<Xid>> {
        if self.unreachable.lock().unwrap().contains(c) {
            return Err(DbError::lost("down"));
        }
        Ok(self
            .prepared
            .lock()
            .unwrap()
            .get(c)
            .cloned()
            .unwrap_or_default())
    }
    async fn xa_finish(&self, c: &ClusterId, xid: &Xid, commit: bool) -> DbResult<()> {
        if let Some(l) = self.prepared.lock().unwrap().get_mut(c) {
            l.retain(|x| x != xid);
        }
        self.finished.lock().unwrap().push((c.clone(), commit));
        Ok(())
    }
    async fn kill(&self, _: &ClusterId, _: u64) -> DbResult<()> {
        Ok(())
    }
    async fn insert_decision(&self, c: &ClusterId, s: &str, row: &DecisionRow) -> DbResult<()> {
        let mut rows = self.rows.lock().unwrap();
        if rows.iter().any(|(_, _, r)| r.dtid == row.dtid) {
            return Err(DbError::new(sprig_txn::backend::errno::DUP_ENTRY, "dup"));
        }
        rows.push((c.clone(), s.to_owned(), row.clone()));
        Ok(())
    }
    async fn read_decision(
        &self,
        _: &ClusterId,
        _: &str,
        d: Dtid,
    ) -> DbResult<Option<DecisionRow>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .find(|(_, _, r)| r.dtid == d)
            .map(|(_, _, r)| r.clone()))
    }
    async fn settle_pending(&self, _: &ClusterId, _: &str, _: Dtid, _: Decision) -> DbResult<()> {
        Ok(())
    }
    async fn decision_rows(&self, _: &ClusterId) -> DbResult<Vec<(String, DecisionRow)>> {
        Ok(Vec::new())
    }
    async fn delete_decision(&self, _: &ClusterId, _: &str, _: Dtid) -> DbResult<()> {
        Ok(())
    }
}

fn home() -> AnchorRef {
    AnchorRef {
        db: DbId(1),
        group: GroupId(1),
        hash_point: 0,
    }
}

fn xid(cluster: &ClusterId) -> Xid {
    Xid {
        gtrid: Gtrid {
            tenant: T,
            dtid: DtidGen::new(7).next(),
            start_ms: 0,
            home: home(),
        },
        bqual: bqual(cluster, ShardId(1)),
    }
}

#[tokio::test]
async fn the_loop_scans_clusters_that_may_hold_data() {
    let (reg, ids) = registry().await;
    let l = ResolverLoop::new(
        dc(),
        reg,
        Arc::new(FakeAdmin::default()),
        Arc::new(StaticLocator::new()),
        None,
    );
    let mut keys = l.all_keys(&cx()).await.unwrap();
    keys.sort();
    let mut want = vec![
        ids["active"].clone(),
        ids["draining"].clone(),
        ids["drained"].clone(),
    ];
    want.sort();
    assert_eq!(keys, want);
    assert_eq!(l.name(), "txn_resolver");
    assert_eq!(l.tier(), LoopTier::DcLeader);
}

#[tokio::test(start_paused = true)]
async fn an_old_prepared_branch_is_decided_and_finished() {
    let (reg, ids) = registry().await;
    let c = ids["active"].clone();
    let admin = Arc::new(FakeAdmin::default());
    admin
        .prepared
        .lock()
        .unwrap()
        .insert(c.clone(), vec![xid(&c)]);
    let locator = Arc::new(StaticLocator::new());
    locator.insert(T, home(), c.clone(), "t3930650002_d1_g1_s1");
    let l = ResolverLoop::new(dc(), reg, admin.clone(), locator, None);
    let cx = cx();
    let every = ResolverConfig::default().interval;
    // Young: the coordinator's job still.
    assert_eq!(
        l.reconcile(&cx, &c).await.unwrap(),
        Outcome::RequeueAfter(every)
    );
    assert!(admin.finished.lock().unwrap().is_empty());
    // Past txn.resolver_min_age_seconds: raced to abort, rolled back.
    tokio::time::advance(ResolverConfig::default().min_age + Duration::from_secs(1)).await;
    assert_eq!(
        l.reconcile(&cx, &c).await.unwrap(),
        Outcome::RequeueAfter(every)
    );
    assert_eq!(*admin.finished.lock().unwrap(), vec![(c.clone(), false)]);
    let rows = admin.rows.lock().unwrap().clone();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, "t3930650002_d1_g1_s1");
    assert_eq!(rows[0].2.outcome, Decision::Abort);
    assert_eq!(rows[0].2.coordinator, "resolver");
}

#[tokio::test]
async fn unreachable_and_unscanned_clusters() {
    let (reg, ids) = registry().await;
    let admin = Arc::new(FakeAdmin::default());
    admin
        .unreachable
        .lock()
        .unwrap()
        .push(ids["active"].clone());
    let l = ResolverLoop::new(
        dc(),
        reg,
        admin.clone(),
        Arc::new(StaticLocator::new()),
        None,
    );
    let cx = cx();
    assert!(matches!(
        l.reconcile(&cx, &ids["active"]).await.unwrap(),
        Outcome::Blocked { .. }
    ));
    for gone in ["retired", "probing", "rejected"] {
        assert_eq!(
            l.reconcile(&cx, &ids[gone]).await.unwrap(),
            Outcome::Done,
            "{gone}"
        );
    }
    assert_eq!(
        l.reconcile(&cx, &ClusterId::new("mum1-c99").unwrap())
            .await
            .unwrap(),
        Outcome::Done
    );
}
