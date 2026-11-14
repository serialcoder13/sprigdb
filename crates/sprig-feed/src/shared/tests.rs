#![allow(clippy::unwrap_used, clippy::many_single_char_names)]

use std::sync::atomic::AtomicUsize;

use sprig_binlog::RowImage;

use super::*;
use crate::source::{RawDdl, RawRow, RowKind, TableShape};

fn g(seq: u64) -> Gtid {
    Gtid {
        domain: 0,
        server: 1,
        seq,
    }
}

fn at(seq: u64) -> GtidPos {
    GtidPos::from_gtids([g(seq)])
}

fn row_in(schema: &str, table: &str) -> RawRow {
    RawRow {
        table: Arc::new(TableShape {
            schema: schema.into(),
            table: table.into(),
            columns: vec!["out_id".into()],
            key: vec![0],
        }),
        kind: RowKind::Insert,
        before: None,
        after: Some(RowImage(vec![Some(Value::Int(1))])),
        hint: None,
    }
}

fn row(schema: &str) -> RawRow {
    row_in(schema, "__sprig_qo")
}

fn txn(seq: u64, schemas: &[&str]) -> RawTxn {
    let mut t = RawTxn::new(g(seq), at(seq - 1), 0);
    t.rows = schemas.iter().map(|s| row(s)).collect();
    t
}

/// A cluster whose binlog grows while tests run; records every dump's
/// scope.
#[derive(Clone, Default)]
struct Fake {
    groups: Arc<Mutex<Vec<RawTxn>>>,
    grew: Arc<Notify>,
    opens: Arc<Mutex<Vec<DumpScope>>>,
    live: Arc<AtomicUsize>,
}

impl Fake {
    fn with(groups: Vec<RawTxn>) -> Self {
        let f = Self::default();
        *f.groups.lock().unwrap() = groups;
        f
    }

    fn push(&self, t: RawTxn) {
        self.groups.lock().unwrap().push(t);
        self.grew.notify_waiters();
    }

    fn last(&self) -> u64 {
        self.groups.lock().unwrap().last().map_or(0, |t| t.gtid.seq)
    }

    fn opens(&self) -> Vec<DumpScope> {
        self.opens.lock().unwrap().clone()
    }

    fn shared_opens(&self) -> usize {
        self.opens().iter().filter(|s| s.only.is_none()).count()
    }

    fn catchup_opens(&self) -> usize {
        self.opens().iter().filter(|s| s.only.is_some()).count()
    }
}

struct FakeSrc {
    fake: Fake,
    scope: Arc<DumpScope>,
    pos: GtidPos,
    _live: LiveGuard,
}

struct LiveGuard(Arc<AtomicUsize>);

impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// What a dump with `scope` keeps of `t` (as `BinlogTxns` does).
fn cut(t: &RawTxn, scope: &DumpScope) -> Option<RawTxn> {
    let mut out = t.clone();
    out.rows
        .retain(|r| scope.keeps(&r.table.schema, &r.table.table));
    out.ddl = t.ddl.clone().filter(|d| scope.names_whole(&d.db, &d.sql));
    let empty = out.rows.is_empty() && out.ddl.is_none() && out.xa.is_none();
    (!empty).then_some(out)
}

#[async_trait::async_trait]
impl TxnSource for FakeSrc {
    async fn next(&mut self) -> Result<SourceItem> {
        loop {
            let grew = self.fake.grew.notified();
            let next = {
                let groups = self.fake.groups.lock().unwrap();
                groups.iter().find(|t| after(&self.pos, &t.gtid)).cloned()
            };
            match next {
                Some(t) => {
                    self.pos.update(t.gtid);
                    if let Some(c) = cut(&t, &self.scope) {
                        return Ok(SourceItem::Txn(c));
                    }
                }
                None => {
                    tokio::select! {
                        () = grew => {}
                        () = tokio::time::sleep(Duration::from_millis(200)) => {
                            return Ok(SourceItem::Idle { pos: self.pos.clone() });
                        }
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Clusters for Fake {
    async fn open(&self, _: &ClusterId, _: TenantId, _: &GtidPos) -> Result<Box<dyn TxnSource>> {
        unreachable!("the shared reader opens cluster-wide sources only")
    }

    async fn open_scope(
        &self,
        _: &ClusterId,
        scope: Arc<DumpScope>,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>> {
        self.opens.lock().unwrap().push((*scope).clone());
        self.live.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FakeSrc {
            fake: self.clone(),
            scope,
            pos: from.clone(),
            _live: LiveGuard(Arc::clone(&self.live)),
        }))
    }

    async fn position(&self, _: &ClusterId) -> Result<GtidPos> {
        Ok(at(self.last()))
    }
}

fn cluster() -> ClusterId {
    ClusterId::new("dc-a-c1").unwrap()
}

async fn take(src: &mut Box<dyn TxnSource>, n: usize) -> Vec<(u64, usize)> {
    let mut out = Vec::new();
    while out.len() < n {
        match tokio::time::timeout(Duration::from_secs(5), src.next())
            .await
            .expect("no item within 5 s")
            .unwrap()
        {
            SourceItem::Txn(t) => out.push((t.gtid.seq, t.rows.len())),
            SourceItem::Idle { .. } => {}
        }
    }
    out
}

async fn quiet(src: &mut Box<dyn TxnSource>, ms: u64) -> bool {
    tokio::time::timeout(Duration::from_millis(ms), async {
        loop {
            if let SourceItem::Txn(t) = src.next().await.unwrap() {
                return t.gtid.seq;
            }
        }
    })
    .await
    .is_err()
}

#[test]
fn groups_split_by_tenant_and_xa_follows_its_prepare() {
    let mut xa = HashMap::new();
    let t = txn(
        5,
        &["t7_d1_g1_s1", "t8_d2_g1_s1", "t7_d1_g1_s2", "t9_sprig"],
    );
    let parts = split(&t, &mut xa);
    let shape: Vec<(u64, usize)> = parts.iter().map(|(t, x)| (t.0, x.rows.len())).collect();
    // A catalog's rows go to its tenant; a Kept::Tables view drops them.
    assert_eq!(shape, vec![(7, 2), (8, 1), (9, 1)]);
    let tables = Kept::Tables(vec!["__sprig_qo".to_owned()].into());
    assert!(view(&parts[2].1, &tables).is_none());
    assert_eq!(view(&parts[2].1, &Kept::All).unwrap().rows.len(), 1);
    let mut p = txn(6, &["t7_d1_g1_s1"]);
    p.xa = Some(XaMark::Prepare("x1".into()));
    assert_eq!(split(&p, &mut xa).len(), 1);
    let mut c = txn(7, &[]);
    c.xa = Some(XaMark::Commit("x1".into()));
    let parts = split(&c, &mut xa);
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].0, TenantId(7));
    assert!(xa.is_empty());
    // DDL goes to every tenant it names; statement DML to its database's.
    let mut d = txn(8, &[]);
    d.ddl = Some(RawDdl {
        db: "t3_d1_g1_s1".into(),
        sql: "RENAME TABLE a TO t4_d1_g1_s1.b".into(),
    });
    d.statement_dml = Some("t3_d1_g1_s1".into());
    let parts = split(&d, &mut xa);
    let who: Vec<(u64, bool, bool)> = parts
        .iter()
        .map(|(t, x)| (t.0, x.ddl.is_some(), x.statement_dml.is_some()))
        .collect();
    assert_eq!(who, vec![(3, true, true), (4, true, false)]);
    assert!(view(&parts[0].1, &tables).is_none());
    let a = at(5);
    let b = at(3);
    assert_eq!(older(&a, &b), b);
    assert_eq!(newer(&a, &b), a);
}

#[tokio::test]
async fn many_tenants_share_one_dump_and_late_starters_read_the_ring() {
    let fake = Fake::with(vec![
        txn(1, &["t1_d1_g1_s1"]),
        txn(2, &["t2_d1_g1_s1", "t1_d1_g1_s1"]),
        txn(3, &["t2_d1_g1_s1"]),
        txn(4, &["t1_d1_g1_s1"]),
    ]);
    let feed = SharedFeed::new(Arc::new(fake.clone()), SharedOpts::default());
    let shared = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let c = cluster();
    // Tenant 1 from group 1 on; tenant 2 joins later from group 2 on.
    let mut s1 = shared.open(&c, TenantId(1), &at(0)).await.unwrap();
    assert_eq!(take(&mut s1, 3).await, vec![(1, 1), (2, 1), (4, 1)]);
    let mut s2 = shared.open(&c, TenantId(2), &at(1)).await.unwrap();
    // Tenant 2 started behind the hub: the ring serves it, and tenant 1
    // sees nothing twice.
    assert_eq!(take(&mut s2, 2).await, vec![(2, 1), (3, 1)]);
    assert!(quiet(&mut s1, 300).await, "tenant 1 got a group twice");
    assert_eq!(fake.shared_opens(), 1, "one dump, no rewind");
    assert_eq!(fake.catchup_opens(), 0);
    let st = feed.stats(&c).unwrap();
    assert_eq!((st.subscriptions, st.tenants, st.running), (2, 2, true));
    // 1,000 more tenants add subscriptions, not dumps.
    let mut subs = Vec::new();
    for t in 10..1_010 {
        subs.push(shared.open(&c, TenantId(t), &at(4)).await.unwrap());
    }
    assert_eq!(feed.stats(&c).unwrap().subscriptions, 1_002);
    assert_eq!(fake.shared_opens(), 1);
    // A new group for one of them reaches it alone.
    fake.push(txn(5, &["t500_d1_g1_s1"]));
    assert_eq!(take(&mut subs[490], 1).await, vec![(5, 1)]);
    assert!(quiet(&mut subs[0], 200).await);
    drop(subs);
}

#[tokio::test]
async fn a_slow_subscription_is_detached_and_comes_back_without_stalling_the_others() {
    let fake = Fake::with(vec![txn(1, &["t1_d1_g1_s1"])]);
    let opts = SharedOpts {
        sub_channel: 4,
        ..SharedOpts::default()
    };
    let feed = SharedFeed::new(Arc::new(fake.clone()), opts);
    let shared = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let c = cluster();
    let mut fast = shared.open(&c, TenantId(1), &at(0)).await.unwrap();
    let mut slow = shared.open(&c, TenantId(2), &at(0)).await.unwrap();
    assert_eq!(take(&mut fast, 1).await, vec![(1, 1)]);
    // 100 groups for both: the slow one reads nothing meanwhile.
    for seq in 2..102 {
        fake.push(txn(seq, &["t1_d1_g1_s1", "t2_d1_g1_s1"]));
    }
    let got = take(&mut fast, 100).await;
    assert_eq!(got.first(), Some(&(2, 1)));
    assert_eq!(got.last(), Some(&(101, 1)));
    // The slow one now reads everything, in order, once.
    let got = take(&mut slow, 100).await;
    let seqs: Vec<u64> = got.iter().map(|x| x.0).collect();
    assert_eq!(seqs, (2..102).collect::<Vec<_>>());
    assert!(quiet(&mut slow, 300).await);
    assert_eq!(fake.shared_opens(), 1);
    assert_eq!(fake.catchup_opens(), 0, "the ring served the slow one");
}

#[tokio::test]
async fn behind_the_ring_a_catch_up_dump_reads_then_hands_over() {
    let fake = Fake::with((1..=50).map(|s| txn(s, &["t1_d1_g1_s1"])).collect());
    let opts = SharedOpts {
        ring_groups: 5,
        gather: Duration::ZERO,
        ..SharedOpts::default()
    };
    let feed = SharedFeed::new(Arc::new(fake.clone()), opts);
    let shared = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let c = cluster();
    let mut a = shared.open(&c, TenantId(1), &at(0)).await.unwrap();
    assert_eq!(take(&mut a, 50).await.len(), 50);
    // A second subscription of the same tenant from group 10: the ring
    // holds 46..50 only.
    let mut b = shared.open(&c, TenantId(1), &at(10)).await.unwrap();
    let seqs: Vec<u64> = take(&mut b, 40).await.iter().map(|x| x.0).collect();
    assert_eq!(seqs, (11..=50).collect::<Vec<_>>());
    assert_eq!(fake.catchup_opens(), 1);
    assert_eq!(fake.opens()[1].only, Some(TenantId(1)));
    // New groups reach both through the hub; the catch-up dump closed.
    for s in 51..=60 {
        fake.push(txn(s, &["t1_d1_g1_s1"]));
    }
    let seqs: Vec<u64> = take(&mut b, 10).await.iter().map(|x| x.0).collect();
    assert_eq!(seqs, (51..=60).collect::<Vec<_>>());
    assert_eq!(take(&mut a, 10).await.len(), 10);
    // A consumer keeps reading: at the next check the hub takes over.
    assert!(quiet(&mut b, 500).await);
    assert_eq!(
        fake.live.load(Ordering::SeqCst),
        1,
        "only the hub's dump is open"
    );
    assert_eq!(feed.stats(&c).unwrap().catchups, 0);
}

#[tokio::test]
async fn subscriptions_arriving_together_move_a_gathering_hub_back() {
    let fake = Fake::with(
        (1..=20)
            .map(|s| txn(s, &["t1_d1_g1_s1", "t2_d1_g1_s1"]))
            .collect(),
    );
    let feed = SharedFeed::new(Arc::new(fake.clone()), SharedOpts::default());
    let shared = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let c = cluster();
    // A worker restarts: each sequencer comes back from its own checkpoint.
    let mut a = shared.open(&c, TenantId(1), &at(15)).await.unwrap();
    let mut b = shared.open(&c, TenantId(2), &at(5)).await.unwrap();
    let sa: Vec<u64> = take(&mut a, 5).await.iter().map(|x| x.0).collect();
    let sb: Vec<u64> = take(&mut b, 15).await.iter().map(|x| x.0).collect();
    assert_eq!(sa, (16..=20).collect::<Vec<_>>());
    assert_eq!(sb, (6..=20).collect::<Vec<_>>());
    assert!(quiet(&mut a, 300).await);
    assert_eq!(fake.catchup_opens(), 0);
    assert!(fake.shared_opens() <= 2);
}

#[tokio::test]
async fn a_whole_tenant_view_gets_its_tables_and_ddl_and_widens_the_dump() {
    let mut g1 = txn(1, &["t1_d1_g1_s1"]);
    g1.rows.push(row_in("t1_d1_g1_s1", "orders"));
    g1.rows.push(row_in("t2_d1_g1_s1", "orders"));
    let mut g2 = RawTxn::new(g(2), at(1), 0);
    g2.ddl = Some(RawDdl {
        db: "t1_d1_g1_s1".into(),
        sql: "ALTER TABLE orders ADD c INT".into(),
    });
    let fake = Fake::with(vec![g1, g2]);
    let feed = SharedFeed::new(Arc::new(fake.clone()), SharedOpts::default());
    let outbox = feed.view(Kept::Tables(vec!["__sprig_qo".to_owned()].into()));
    let all = feed.view(Kept::All);
    let c = cluster();
    let mut q = outbox.open(&c, TenantId(1), &at(0)).await.unwrap();
    assert_eq!(take(&mut q, 1).await, vec![(1, 1)]);
    assert!(fake.opens()[0].tenants.is_empty());
    let mut w = all.open(&c, TenantId(1), &at(0)).await.unwrap();
    // Rows of every table of tenant 1 (not tenant 2's), then its DDL.
    let mut seen = Vec::new();
    while seen.len() < 2 {
        if let SourceItem::Txn(t) = tokio::time::timeout(Duration::from_secs(5), w.next())
            .await
            .unwrap()
            .unwrap()
        {
            seen.push((t.gtid.seq, t.rows.len(), t.ddl.is_some()));
        }
    }
    assert_eq!(seen, vec![(1, 2, false), (2, 0, true)]);
    // The hub reopens with tenant 1 kept whole; the catch-up dump hands
    // over to it.
    assert!(quiet(&mut w, 500).await);
    let scopes = fake.opens();
    assert!(
        scopes
            .iter()
            .any(|s| s.only.is_none() && s.tenants.contains(&TenantId(1))),
        "the hub's dump keeps tenant 1 whole: {scopes:?}"
    );
    assert_eq!(
        fake.live.load(Ordering::SeqCst),
        1,
        "only the hub's dump is open"
    );
    // The outbox view still sees its row once, and no DDL.
    assert!(quiet(&mut q, 300).await);
    // A new group with rows of both kinds reaches each view as it keeps.
    let mut g3 = txn(3, &["t1_d1_g1_s1"]);
    g3.rows.push(row_in("t1_d1_g1_s1", "orders"));
    fake.push(g3);
    assert_eq!(take(&mut w, 1).await, vec![(3, 2)]);
    assert_eq!(take(&mut q, 1).await, vec![(3, 1)]);
}

/// Regression (r9feed's live run 2): a subscription registered behind a
/// gathering hub, waiting for it to move back, gets no idle position ahead
/// of the groups it hasn't had (its tail would skip them).
#[test]
fn no_idle_position_while_a_move_back_is_pending() {
    let mut st = HubState::default();
    let (tx_a, mut rx_a) = mpsc::channel(4);
    let (tx_b, mut rx_b) = mpsc::channel(4);
    st.register(TenantId(1), Kept::All, at(5), tx_a, Arc::default());
    // B started at 0 behind the hub at 5: registered at 0, move back asked.
    st.register(TenantId(2), Kept::All, at(0), tx_b, Arc::default());
    st.rewind = Some(at(0));
    st.idle_fanout(&at(5));
    assert!(rx_a.try_recv().is_err() && rx_b.try_recv().is_err());
    // Once the hub has moved back, it reads 1..5 again and sends positions.
    st.rewind = None;
    st.idle_fanout(&at(0));
    assert!(rx_b.try_recv().is_ok(), "B is at 0: position 0 is fine");
    assert!(rx_a.try_recv().is_err(), "A is at 5: nothing before it");
}
