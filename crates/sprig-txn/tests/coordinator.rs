//! The coordinator against a scripted backend: mode choice, the write
//! rule, statement order on each branch, savepoints, and error mapping.
//! The deterministic simulation (`sim_txn.rs`) covers crashes; the live
//! tests real MariaDB.

#![allow(
    clippy::many_single_char_names,
    clippy::unwrap_used,
    clippy::missing_panics_doc
)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_txn::{
    BeginOptions, BranchConn, ClusterAdmin, ClusterCaps, Ctl, DbError, DbResult, Decision,
    DecisionRow, Dtid, DtidGen, Intent, NonXaPolicy, ShardRef, TxnBackend, TxnConfig,
    TxnCoordinator, TxnMode, Xid,
};

type Log = Arc<Mutex<Vec<(String, String)>>>;

/// Fails the first control statement whose SQL starts with the prefix.
#[derive(Default)]
struct Script {
    fail: Mutex<Vec<(String, String, u16)>>,
}

struct FakeConn {
    cluster: String,
    log: Log,
    script: Arc<Script>,
}

#[async_trait::async_trait]
impl BranchConn for FakeConn {
    fn thread_id(&self) -> u64 {
        7
    }

    async fn control(&mut self, ctl: &[Ctl]) -> DbResult<()> {
        for c in ctl {
            let sql = c.sql();
            let mut f = self.script.fail.lock().unwrap();
            if let Some(i) = f
                .iter()
                .position(|(cl, p, _)| *cl == self.cluster && sql.starts_with(p.as_str()))
            {
                let (_, _, e) = f.remove(i);
                return Err(DbError::new(e, "scripted"));
            }
            drop(f);
            let short = sql.split(" X'").next().unwrap().to_owned();
            self.log.lock().unwrap().push((self.cluster.clone(), short));
        }
        Ok(())
    }
}

#[derive(Default)]
struct FakeAdmin {
    rows: Mutex<BTreeMap<(String, Dtid), Decision>>,
}

#[async_trait::async_trait]
impl ClusterAdmin for FakeAdmin {
    async fn xa_recover(&self, _: &ClusterId) -> DbResult<Vec<Xid>> {
        Ok(Vec::new())
    }
    async fn xa_finish(&self, _: &ClusterId, _: &Xid, _: bool) -> DbResult<()> {
        Ok(())
    }
    async fn kill(&self, _: &ClusterId, _: u64) -> DbResult<()> {
        Ok(())
    }
    async fn insert_decision(&self, c: &ClusterId, _: &str, row: &DecisionRow) -> DbResult<()> {
        let mut r = self.rows.lock().unwrap();
        let k = (c.to_string(), row.dtid);
        if r.contains_key(&k) {
            return Err(DbError::new(1062, "dup"));
        }
        r.insert(k, row.outcome);
        Ok(())
    }
    async fn read_decision(
        &self,
        c: &ClusterId,
        _: &str,
        d: Dtid,
    ) -> DbResult<Option<DecisionRow>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .get(&(c.to_string(), d))
            .map(|o| DecisionRow::new(d, *o, "x")))
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

struct Fake {
    log: Log,
    script: Arc<Script>,
    admin: Arc<FakeAdmin>,
}

#[async_trait::async_trait]
impl TxnBackend for Fake {
    type Conn = FakeConn;
    fn caps(&self, c: &ClusterId) -> ClusterCaps {
        ClusterCaps {
            xa: !c.as_str().starts_with('g'),
        }
    }
    async fn checkout(&self, c: &ClusterId) -> DbResult<FakeConn> {
        Ok(FakeConn {
            cluster: c.to_string(),
            log: self.log.clone(),
            script: self.script.clone(),
        })
    }
    fn admin(&self) -> Arc<dyn ClusterAdmin> {
        self.admin.clone()
    }
}

fn shard(i: u32) -> ShardRef {
    ShardRef {
        db: DbId(1),
        group: GroupId(1),
        shard: ShardId(i),
        hash_point: u64::from(i),
        schema: format!("t1_d1_g1_s{i}"),
    }
}

fn c(s: &str) -> ClusterId {
    ClusterId::new(s).unwrap()
}

fn setup(
    policy: NonXaPolicy,
    all_xa: bool,
) -> (TxnCoordinator<Fake>, Log, Arc<Script>, Arc<FakeAdmin>) {
    let log: Log = Arc::default();
    let script = Arc::new(Script::default());
    let admin = Arc::new(FakeAdmin::default());
    let b = Arc::new(Fake {
        log: log.clone(),
        script: script.clone(),
        admin: admin.clone(),
    });
    let cfg = TxnConfig {
        policy,
        tenant_all_xa: all_xa,
        ..TxnConfig::default()
    };
    (
        TxnCoordinator::new(b, Arc::new(DtidGen::new(1)), TenantId(1), cfg),
        log,
        script,
        admin,
    )
}

fn on(log: &Log, cluster: &str) -> Vec<String> {
    log.lock()
        .unwrap()
        .iter()
        .filter(|(c, _)| c == cluster)
        .map(|(_, s)| s.clone())
        .collect()
}

async fn write(t: &mut TxnCoordinator<Fake>, cl: &str, s: u32) -> sprig_common::Result<()> {
    t.branch(&c(cl), &shard(s), Intent::Write).await.map(|_| ())
}

#[tokio::test]
async fn one_writer_commits_locally() {
    let (mut t, log, _, _) = setup(NonXaPolicy::Reject, true);
    t.begin(BeginOptions::default()).unwrap();
    write(&mut t, "a", 1).await.unwrap();
    t.branch(&c("b"), &shard(2), Intent::Read).await.unwrap();
    let o = t.commit().await.unwrap();
    assert_eq!(o.mode, TxnMode::SingleWriter);
    assert_eq!(on(&log, "a")[0], "START TRANSACTION");
    assert!(on(&log, "a").contains(&"COMMIT".to_owned()));
    // The reader was an XA branch, ended in one phase.
    assert_eq!(on(&log, "b")[1], "XA START");
    assert!(on(&log, "b").contains(&"XA COMMIT".to_owned()));
}

#[tokio::test]
async fn all_xa_tenant_commits_the_home_last_without_preparing_it() {
    let (mut t, log, _, admin) = setup(NonXaPolicy::Reject, true);
    t.begin(BeginOptions::default()).unwrap();
    write(&mut t, "a", 1).await.unwrap();
    write(&mut t, "b", 2).await.unwrap();
    let o = t.commit().await.unwrap();
    assert_eq!(o.mode, TxnMode::Xa2pc);
    let a = on(&log, "a");
    assert_eq!(a[0], "START TRANSACTION");
    assert!(
        a.iter()
            .any(|s| s.starts_with("SET SESSION innodb_lock_wait_timeout = 10"))
    );
    assert!(
        a.iter()
            .any(|s| s.starts_with("INSERT INTO `t1_d1_g1_s1`.`__sprig_dtx`"))
    );
    assert!(!a.iter().any(|s| s == "XA PREPARE"));
    let b = on(&log, "b");
    let p = b.iter().position(|s| s == "XA PREPARE").unwrap();
    assert_eq!(b[p + 1], "XA COMMIT", "phase 2 after prepare");
    // No pending row: the home is the last resource.
    assert!(admin.rows.lock().unwrap().is_empty());
}

#[tokio::test]
async fn galera_is_the_last_resource_and_the_home_gets_a_pending_row() {
    let (mut t, log, _, admin) = setup(NonXaPolicy::Reject, false);
    t.begin(BeginOptions::default()).unwrap();
    write(&mut t, "a", 1).await.unwrap();
    write(&mut t, "g1", 2).await.unwrap();
    write(&mut t, "b", 3).await.unwrap();
    let dtid = t.dtid().unwrap();
    let o = t.commit().await.unwrap();
    assert_eq!(o.mode, TxnMode::Xa2pc);
    assert_eq!(
        admin.rows.lock().unwrap().get(&("a".to_owned(), dtid)),
        Some(&Decision::Pending)
    );
    let g = on(&log, "g1");
    assert_eq!(g[0], "SET SESSION innodb_lock_wait_timeout = 10");
    assert_eq!(g[1], "START TRANSACTION");
    assert!(g.iter().any(|s| s.starts_with("INSERT INTO `t1_d1_g1_s2`")));
    assert!(on(&log, "a").contains(&"XA PREPARE".to_owned()));
    assert!(on(&log, "b").contains(&"XA PREPARE".to_owned()));
}

#[tokio::test]
async fn reject_refuses_a_second_cluster_without_xa_before_running_anything() {
    let (mut t, log, _, _) = setup(NonXaPolicy::Reject, false);
    t.begin(BeginOptions::default()).unwrap();
    write(&mut t, "g1", 1).await.unwrap();
    let e = write(&mut t, "g2", 2).await.unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39440);
    assert!(on(&log, "g2").is_empty(), "nothing sent to g2");
    // Reading g2 is fine, and so is a write on an XA cluster.
    t.branch(&c("g2"), &shard(2), Intent::Read).await.unwrap();
    write(&mut t, "a", 3).await.unwrap();
    // Writing g2 through the open read branch is refused too.
    let e = t.note_write(&c("g2")).unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39440);
    let e = t.check_writes(&[c("g1"), c("g2")]).unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39440);
    t.rollback().await.unwrap();
    assert!(on(&log, "g1").contains(&"ROLLBACK".to_owned()));
}

#[tokio::test]
async fn best_effort_commits_writers_without_xa_one_at_a_time() {
    let (mut t, log, script, _) = setup(NonXaPolicy::BestEffort, false);
    t.begin(BeginOptions::default()).unwrap();
    write(&mut t, "g1", 1).await.unwrap();
    write(&mut t, "g2", 2).await.unwrap();
    assert!(t.is_best_effort());
    assert_eq!(t.commit().await.unwrap().mode, TxnMode::BestEffort);
    // The second commit fails after the first: 39442.
    t.begin(BeginOptions::default()).unwrap();
    write(&mut t, "g1", 1).await.unwrap();
    write(&mut t, "g2", 2).await.unwrap();
    script
        .fail
        .lock()
        .unwrap()
        .push(("g2".into(), "COMMIT".into(), 1213));
    let e = t.commit().await.unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39442, "{e}");
    assert!(
        e.message.contains("only clusters g1 (error 1213 on g2)"),
        "{e}"
    );
    assert_eq!(on(&log, "g1").iter().filter(|s| *s == "COMMIT").count(), 2);
}

#[tokio::test]
async fn a_failed_prepare_rolls_everything_back_with_39443() {
    let (mut t, log, script, _) = setup(NonXaPolicy::Reject, true);
    t.begin(BeginOptions::default()).unwrap();
    write(&mut t, "a", 1).await.unwrap();
    write(&mut t, "b", 2).await.unwrap();
    script
        .fail
        .lock()
        .unwrap()
        .push(("b".into(), "XA PREPARE".into(), 1399));
    let e = t.commit().await.unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39443, "{e}");
    assert!(on(&log, "a").contains(&"ROLLBACK".to_owned()));
    assert!(!on(&log, "a").iter().any(|s| s.starts_with("INSERT")));
}

#[tokio::test]
async fn the_resolver_winning_the_race_aborts_the_commit() {
    let (mut t, log, script, _) = setup(NonXaPolicy::Reject, true);
    t.begin(BeginOptions::default()).unwrap();
    write(&mut t, "a", 1).await.unwrap();
    write(&mut t, "b", 2).await.unwrap();
    script
        .fail
        .lock()
        .unwrap()
        .push(("a".into(), "INSERT INTO".into(), 1062));
    let e = t.commit().await.unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39443);
    assert!(on(&log, "b").contains(&"XA ROLLBACK".to_owned()));
}

#[tokio::test]
async fn deadlocks_and_lock_waits_roll_back_the_whole_transaction() {
    let (mut t, log, _, _) = setup(NonXaPolicy::Reject, true);
    t.begin(BeginOptions::default()).unwrap();
    write(&mut t, "a", 1).await.unwrap();
    // One branch: a lock-wait timeout keeps the transaction.
    t.statement_failed(&c("a"), &DbError::new(1205, "x"))
        .await
        .unwrap();
    assert!(t.in_txn());
    write(&mut t, "b", 2).await.unwrap();
    let e = t
        .statement_failed(&c("b"), &DbError::new(1205, "x"))
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 1205);
    assert!(!t.in_txn());
    assert!(on(&log, "a").contains(&"ROLLBACK".to_owned()));
    // A duplicate key leaves it open.
    t.begin(BeginOptions::default()).unwrap();
    write(&mut t, "a", 1).await.unwrap();
    write(&mut t, "b", 2).await.unwrap();
    t.statement_failed(&c("b"), &DbError::new(1062, "x"))
        .await
        .unwrap();
    assert!(t.in_txn());
    let e = t
        .statement_failed(&c("a"), &DbError::new(1213, "x"))
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 1213);
}

#[tokio::test]
async fn savepoints_are_set_lazily_and_undo_branches_opened_after_them() {
    let (mut t, log, _, _) = setup(NonXaPolicy::Reject, false);
    t.begin(BeginOptions::default()).unwrap();
    write(&mut t, "g1", 1).await.unwrap();
    t.savepoint("s1").unwrap();
    // Nothing sent until g1's next statement.
    assert!(!on(&log, "g1").iter().any(|s| s.starts_with("SAVEPOINT")));
    write(&mut t, "g1", 1).await.unwrap();
    assert!(on(&log, "g1").contains(&"SAVEPOINT `s1`".to_owned()));
    // A second Galera write would be refused; after rolling back to s1
    // (which drops the branch opened after it) it is still refused, since
    // g1 wrote before s1.
    write(&mut t, "a", 2).await.unwrap();
    t.rollback_to("s1").await.unwrap();
    assert_eq!(t.branches().len(), 1, "branch a dropped");
    assert!(on(&log, "a").contains(&"XA ROLLBACK".to_owned()));
    assert!(on(&log, "g1").contains(&"ROLLBACK TO SAVEPOINT `s1`".to_owned()));
    assert_eq!(
        write(&mut t, "g2", 3)
            .await
            .unwrap_err()
            .protocol()
            .mysql_errno,
        39440
    );
    let e = t.rollback_to("nope").await.unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 1305);
    t.release("s1").unwrap();
    assert_eq!(t.commit().await.unwrap().mode, TxnMode::SingleWriter);
}

#[tokio::test]
async fn statement_savepoints_undo_one_statement_and_its_new_branches() {
    let (mut t, log, _, _) = setup(NonXaPolicy::Reject, false);
    t.begin(BeginOptions::default()).unwrap();
    t.branch(&c("g1"), &shard(1), Intent::Read).await.unwrap();
    let m = t.statement_savepoint(&[c("g1"), c("g2")]).await.unwrap();
    write(&mut t, "g1", 1).await.unwrap();
    write(&mut t, "a", 2).await.unwrap();
    t.rollback_statement(m).await.unwrap();
    assert!(on(&log, "g1").contains(&"ROLLBACK TO SAVEPOINT `__sprig_stmt`".to_owned()));
    assert_eq!(t.branches().len(), 1);
    // g1's write was undone, so g2 may now be the one writer without XA.
    write(&mut t, "g2", 3).await.unwrap();
    assert_eq!(t.commit().await.unwrap().mode, TxnMode::SingleWriter);
}

#[tokio::test]
async fn max_branches_is_enforced() {
    let (mut t, _, _, _) = setup(NonXaPolicy::Reject, true);
    let mut cfg = t.config().clone();
    cfg.max_branches = 2;
    t.set_config(cfg);
    t.begin(BeginOptions::default()).unwrap();
    write(&mut t, "a", 1).await.unwrap();
    write(&mut t, "b", 2).await.unwrap();
    let e = write(&mut t, "c", 3).await.unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39444);
    t.rollback().await.unwrap();
}
