#![allow(clippy::many_single_char_names)]

use std::sync::{Arc, Mutex};

use super::*;
use crate::sim::mariadb::Outcome;

const S: &str = "s";

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn t(name: &str) -> String {
    name.to_owned()
}

fn ins(k: i64, v: &str) -> Stmt {
    Stmt::Insert {
        t: t("t"),
        k,
        v: v.to_owned(),
    }
}

fn upd(k: i64, v: &str) -> Stmt {
    Stmt::Update {
        t: t("t"),
        k,
        v: v.to_owned(),
    }
}

fn get(k: i64) -> Stmt {
    Stmt::Get { t: t("t"), k }
}

fn done(s: Step) -> Outcome {
    match s {
        Step::Done(o) => o,
        Step::Blocked => panic!("blocked"),
    }
}

fn val(o: &Outcome) -> Option<String> {
    match o {
        Outcome::Ok { rows, .. } => rows.first().and_then(|r| r[1].clone()),
        Outcome::Err(e) => panic!("error {e}"),
    }
}

/// One server with table `s.t`, and a root connection.
fn single() -> (Model, ConnId) {
    let mut m = Model::new(1);
    m.add_server(ServerConfig::new("db", 101, 1));
    let c = m.connect("db", None, S, Duration::ZERO).unwrap();
    assert_eq!(
        done(m.exec(c, &Stmt::CreateTable(t("t")), Duration::ZERO)),
        Outcome::ok()
    );
    (m, c)
}

#[test]
fn autocommit_writes_get_gtids() {
    let (mut m, c) = single();
    let z = Duration::ZERO;
    assert_eq!(done(m.exec(c, &ins(1, "a"), z)), Outcome::affected(1));
    assert_eq!(
        done(m.exec(c, &ins(1, "b"), z)),
        Outcome::Err(errno::DUP_ENTRY)
    );
    assert_eq!(done(m.exec(c, &upd(1, "a"), z)), Outcome::affected(0));
    assert_eq!(done(m.exec(c, &upd(1, "c"), z)), Outcome::affected(1));
    assert_eq!(val(&done(m.exec(c, &get(1), z))), Some("c".into()));
    // CREATE TABLE was 1-101-1; the insert 1-101-2; the update 1-101-3.
    assert_eq!(
        done(m.exec(c, &Stmt::LastGtid, z)).scalar(),
        Some("1-101-3")
    );
    assert_eq!(
        done(m.exec(c, &Stmt::GtidBinlogPos, z)).scalar(),
        Some("1-101-3")
    );
    let other = m.connect("db", None, "x", z).unwrap();
    assert_eq!(
        done(m.exec(other, &get(1), z)),
        Outcome::Err(errno::NO_SUCH_TABLE)
    );
}

#[test]
fn lock_waits_time_out_and_keep_the_transaction() {
    let (mut m, a) = single();
    let b = m.connect("db", None, S, Duration::ZERO).unwrap();
    let z = Duration::ZERO;
    done(m.exec(a, &ins(1, "x"), z));
    done(m.exec(a, &ins(2, "x"), z));
    done(m.exec(b, &Stmt::SetLockWaitTimeout(1), z));
    done(m.exec(a, &Stmt::Begin, z));
    done(m.exec(a, &upd(1, "a"), z));
    done(m.exec(b, &Stmt::Begin, z));
    done(m.exec(b, &upd(2, "b"), z));
    assert_eq!(m.exec(b, &upd(1, "b"), z), Step::Blocked);
    m.advance(ms(999));
    assert_eq!(m.take_done(b), None);
    m.advance(secs(1));
    assert_eq!(m.take_done(b), Some(Outcome::Err(errno::LOCK_WAIT_TIMEOUT)));
    // b still holds row 2.
    done(m.exec(a, &Stmt::SetLockWaitTimeout(1), secs(1)));
    assert_eq!(m.exec(a, &upd(2, "a"), secs(1)), Step::Blocked);
    done(m.exec(b, &Stmt::Commit, secs(1)));
    assert_eq!(m.take_done(a), Some(Outcome::affected(1)));
}

#[test]
fn deadlocks_roll_back_the_lighter_transaction() {
    let (mut m, a) = single();
    let b = m.connect("db", None, S, Duration::ZERO).unwrap();
    let z = Duration::ZERO;
    for k in 1..=3 {
        done(m.exec(a, &ins(k, "x"), z));
    }
    done(m.exec(a, &Stmt::Begin, z));
    done(m.exec(a, &upd(1, "a"), z));
    done(m.exec(a, &upd(3, "a"), z)); // a is heavier
    done(m.exec(b, &Stmt::Begin, z));
    done(m.exec(b, &upd(2, "b"), z));
    assert_eq!(m.exec(a, &upd(2, "a"), z), Step::Blocked);
    // b closes the cycle and is lighter: b is the victim, a proceeds.
    assert_eq!(
        done(m.exec(b, &upd(1, "b"), z)),
        Outcome::Err(errno::DEADLOCK)
    );
    assert_eq!(m.take_done(a), Some(Outcome::affected(1)));
    done(m.exec(a, &Stmt::Commit, z));
    assert_eq!(
        m.rows("db", S, "t"),
        vec![(1, "a".into()), (2, "a".into()), (3, "a".into())]
    );
}

#[test]
fn snapshots_and_snapshot_isolation() {
    let (mut m, a) = single();
    let b = m.connect("db", None, S, Duration::ZERO).unwrap();
    let z = Duration::ZERO;
    done(m.exec(a, &ins(1, "old"), z));
    done(m.exec(a, &Stmt::Begin, z));
    assert_eq!(val(&done(m.exec(a, &get(1), z))), Some("old".into()));
    done(m.exec(b, &upd(1, "new"), z));
    // The snapshot holds; a locking read sees the change and fails.
    assert_eq!(val(&done(m.exec(a, &get(1), z))), Some("old".into()));
    assert_eq!(
        done(m.exec(a, &Stmt::GetForUpdate { t: t("t"), k: 1 }, z)),
        Outcome::Err(errno::CHECKREAD)
    );
    // READ COMMITTED sees it at once.
    done(m.exec(a, &Stmt::SetIsolation(Isolation::ReadCommitted), z));
    done(m.exec(a, &Stmt::Begin, z));
    assert_eq!(val(&done(m.exec(a, &get(1), z))), Some("new".into()));
    done(m.exec(b, &upd(1, "newer"), z));
    assert_eq!(val(&done(m.exec(a, &get(1), z))), Some("newer".into()));
    assert_eq!(done(m.exec(a, &upd(1, "mine"), z)), Outcome::affected(1));
}

#[test]
fn prepared_xa_survives_disconnect_and_crash() {
    let (mut m, a) = single();
    let z = Duration::ZERO;
    done(m.exec(a, &ins(1, "x"), z));
    done(m.exec(a, &Stmt::XaStart(t("x1")), z));
    done(m.exec(a, &upd(1, "xa"), z));
    assert_eq!(
        done(m.exec(a, &Stmt::Commit, z)),
        Outcome::Err(errno::XAER_RMFAIL)
    );
    done(m.exec(a, &Stmt::XaEnd(t("x1")), z));
    done(m.exec(a, &Stmt::XaPrepare(t("x1")), z));
    assert_eq!(
        done(m.exec(a, &get(1), z)),
        Outcome::Err(errno::XAER_RMFAIL)
    );
    let b = m.connect("db", None, S, z).unwrap();
    // Attached to a live session: invisible to others.
    assert_eq!(
        done(m.exec(b, &Stmt::XaCommit(t("x1")), z)),
        Outcome::Err(errno::XAER_NOTA)
    );
    // An open transaction with work, then a crash.
    let c = m.connect("db", None, S, z).unwrap();
    done(m.exec(c, &Stmt::Begin, z));
    done(m.exec(c, &ins(2, "lost"), z));
    m.crash("db", ms(10));
    m.restart("db", ms(20));
    let d = m.connect("db", None, S, ms(20)).unwrap();
    assert_eq!(
        done(m.exec(a, &get(1), ms(20))),
        Outcome::Err(errno::SERVER_LOST)
    );
    assert_eq!(
        done(m.exec(d, &Stmt::XaRecover, ms(20))),
        Outcome::rows(vec![vec![Some("x1".into())]])
    );
    assert_eq!(m.rows("db", S, "t"), vec![(1, "x".into())]);
    // The prepared branch still holds its lock.
    done(m.exec(d, &Stmt::SetLockWaitTimeout(1), ms(20)));
    assert_eq!(m.exec(d, &upd(1, "d"), ms(20)), Step::Blocked);
    m.advance(secs(2));
    assert_eq!(m.take_done(d), Some(Outcome::Err(errno::LOCK_WAIT_TIMEOUT)));
    done(m.exec(d, &Stmt::XaCommit(t("x1")), secs(2)));
    assert_eq!(m.rows("db", S, "t"), vec![(1, "xa".into())]);
    assert_eq!(
        done(m.exec(d, &Stmt::XaRecover, secs(2))),
        Outcome::rows(vec![])
    );
}

#[test]
fn kill_query_and_connection() {
    let (mut m, a) = single();
    let b = m.connect("db", None, S, Duration::ZERO).unwrap();
    let z = Duration::ZERO;
    done(m.exec(a, &ins(1, "x"), z));
    done(m.exec(a, &Stmt::Begin, z));
    done(m.exec(a, &upd(1, "a"), z));
    assert_eq!(m.exec(b, &upd(1, "b"), z), Step::Blocked);
    done(m.exec(a, &Stmt::KillQuery(b), z));
    assert_eq!(m.take_done(b), Some(Outcome::Err(errno::QUERY_INTERRUPTED)));
    assert_eq!(m.exec(b, &upd(1, "b"), z), Step::Blocked);
    let k = m.connect("db", None, S, z).unwrap();
    done(m.exec(k, &Stmt::KillConnection(a), z));
    // a's transaction rolled back: b gets the row.
    assert_eq!(m.take_done(b), Some(Outcome::affected(1)));
    assert_eq!(
        done(m.exec(a, &get(1), z)),
        Outcome::Err(errno::SERVER_LOST)
    );
}

#[test]
fn revoke_misses_sessions_already_in_the_schema() {
    let (mut m, root) = single();
    let z = Duration::ZERO;
    done(m.exec(root, &Stmt::CreateUser(t("app")), z));
    done(m.exec(root, &Stmt::GrantAll(t("app")), z));
    let used = m.connect("db", Some("app"), S, z).unwrap();
    let other = m.connect("db", Some("app"), S, z).unwrap();
    done(m.exec(used, &Stmt::Use, z));
    done(m.exec(root, &Stmt::RevokeAll(t("app")), z));
    assert_eq!(
        done(m.exec(used, &ins(1, "still"), z)),
        Outcome::affected(1)
    );
    assert_eq!(
        done(m.exec(other, &ins(2, "no"), z)),
        Outcome::Err(errno::TABLE_ACCESS_DENIED)
    );
    // read_only stops the user but not root.
    done(m.exec(root, &Stmt::GrantAll(t("app")), z));
    done(m.exec(root, &Stmt::SetReadOnly(true), z));
    assert_eq!(
        done(m.exec(other, &ins(3, "ro"), z)),
        Outcome::Err(errno::READ_ONLY)
    );
    assert_eq!(done(m.exec(root, &ins(3, "root"), z)), Outcome::affected(1));
}

/// A primary with two semi-sync replicas, lag 1 s, reachability by a set
/// of cut pairs.
/// Pairs of servers that can't reach each other.
type Cuts = Arc<Mutex<Vec<(String, String)>>>;

fn replication() -> (Model, ConnId, Cuts) {
    let cuts: Cuts = Arc::default();
    let mut m = Model::new(1);
    let c2 = Arc::clone(&cuts);
    m.set_reachability(Box::new(move |a, b| {
        !c2.lock()
            .unwrap()
            .iter()
            .any(|(x, y)| (x == a && y == b) || (x == b && y == a))
    }));
    m.set_delay(Box::new(|_, _| ms(1)));
    let mut p = ServerConfig::new("p", 201, 2);
    p.semisync_master = true;
    m.add_server(p);
    m.add_replica(ServerConfig::new("r1", 202, 2), "p", secs(1));
    m.add_replica(ServerConfig::new("r2", 203, 2), "p", secs(1));
    let c = m.connect("p", None, S, Duration::ZERO).unwrap();
    // CREATE TABLE waits for no ack (DDL) in the model.
    done(m.exec(c, &Stmt::CreateTable(t("t")), Duration::ZERO));
    (m, c, cuts)
}

/// Advances until `conn`'s statement finishes or `limit` passes.
fn wait(m: &mut Model, conn: ConnId, from: Duration, limit: Duration) -> (Outcome, Duration) {
    let mut now = from;
    loop {
        if let Some(o) = m.take_done(conn) {
            return (o, now);
        }
        let next = m.next_event_time().expect("an event");
        assert!(next <= limit, "still waiting at {next:?}");
        now = next;
        m.advance(now);
    }
}

#[test]
fn semi_sync_waits_for_an_ack_then_falls_back() {
    let (mut m, c, cuts) = replication();
    assert_eq!(m.exec(c, &ins(1, "a"), ms(10)), Step::Blocked);
    let (o, at) = wait(&mut m, c, ms(10), secs(1));
    assert_eq!(o, Outcome::affected(1));
    assert_eq!(at, ms(12), "one round trip");
    // Applied after the lag.
    let r = m.connect("r1", None, S, at).unwrap();
    let g: Gtid = "2-201-2".parse().unwrap();
    assert_eq!(
        m.exec(
            r,
            &Stmt::MasterGtidWait {
                gtid: g,
                timeout: 5
            },
            at
        ),
        Step::Blocked
    );
    let (o, applied) = wait(&mut m, r, at, secs(3));
    assert_eq!(o.scalar(), Some("0"));
    assert!(applied >= at + secs(1), "{applied:?}");
    // Cut both replicas: the commit waits the full timeout, then semi-sync
    // switches off and commits return at once.
    cuts.lock()
        .unwrap()
        .extend([(t("p"), t("r1")), (t("p"), t("r2"))]);
    assert_eq!(m.exec(c, &ins(2, "b"), secs(5)), Step::Blocked);
    let (o, at) = wait(&mut m, c, secs(5), secs(20));
    assert_eq!(o, Outcome::affected(1));
    assert_eq!(at, secs(15));
    assert!(!m.semisync_status("p"));
    assert_eq!(done(m.exec(c, &ins(3, "c"), at)), Outcome::affected(1));
    // Heal: a replica catches up, acks, semi-sync is back on.
    cuts.lock().unwrap().clear();
    m.advance(at + secs(2));
    assert!(m.semisync_status("p"));
}

#[test]
fn failover_loses_exactly_the_unreplicated_commits() {
    let (mut m, c, cuts) = replication();
    assert_eq!(m.exec(c, &ins(1, "a"), ms(10)), Step::Blocked);
    let (_, at) = wait(&mut m, c, ms(10), secs(1));
    // Partition the primary from its replicas; semi-sync times out; two
    // more commits happen only on the primary.
    cuts.lock()
        .unwrap()
        .extend([(t("p"), t("r1")), (t("p"), t("r2"))]);
    m.exec(c, &ins(2, "b"), at);
    let (_, at) = wait(&mut m, c, at, secs(20));
    done(m.exec(c, &ins(3, "c"), at));
    // A commit waiting for an ack when the primary crashes stays committed
    // there (it is in the binlog) but was never acknowledged.
    m.crash("p", at + secs(1));
    m.restart("p", at + secs(2));
    assert_eq!(m.rows("p", S, "t").len(), 3);
    // Fail over to r1 after it applied its relay log.
    let dropped = m.promote("r1", true, at + secs(3));
    assert!(dropped.is_empty());
    let lost = m.lost_gtids("p", "r1");
    assert_eq!(
        lost.iter().map(ToString::to_string).collect::<Vec<_>>(),
        vec!["2-201-3", "2-201-4"]
    );
    assert_eq!(m.rows("r1", S, "t"), vec![(1, "a".into())]);
    // The old primary can't rejoin as a replica of r1: it has GTIDs r1
    // lacks (gtid_strict_mode).
    cuts.lock().unwrap().clear();
    m.change_source("p", "r1", secs(1), at + secs(4));
    m.advance(at + secs(5));
    assert!(m.replica_error("p").unwrap().contains("1236"));
    // r2 follows the new primary.
    m.change_source("r2", "r1", secs(1), at + secs(4));
    let w = m.connect("r1", None, S, at + secs(5)).unwrap();
    // The new primary has semi-sync on: the commit waits for r2's ack.
    assert_eq!(m.exec(w, &ins(9, "new"), at + secs(5)), Step::Blocked);
    let (o, acked) = wait(&mut m, w, at + secs(5), at + secs(6));
    assert_eq!(o, Outcome::affected(1));
    m.advance(acked + secs(5));
    assert!(m.replica_error("r2").is_none());
    assert_eq!(
        m.rows("r2", S, "t"),
        vec![(1, "a".into()), (9, "new".into())]
    );
}

fn galera() -> (Model, [ConnId; 3], Arc<Mutex<Vec<String>>>) {
    let isolated: Arc<Mutex<Vec<String>>> = Arc::default();
    let mut m = Model::new(1);
    let i2 = Arc::clone(&isolated);
    m.set_reachability(Box::new(move |a, b| {
        let i = i2.lock().unwrap();
        !(i.iter().any(|x| x == a) || i.iter().any(|x| x == b))
    }));
    m.set_delay(Box::new(|_, _| ms(1)));
    m.add_galera(
        "g",
        vec![
            ServerConfig::new("g1", 301, 301),
            ServerConfig::new("g2", 302, 302),
            ServerConfig::new("g3", 303, 303),
        ],
        3,
    );
    let z = Duration::ZERO;
    let c = [
        m.connect("g1", None, S, z).unwrap(),
        m.connect("g2", None, S, z).unwrap(),
        m.connect("g3", None, S, z).unwrap(),
    ];
    done(m.exec(c[0], &Stmt::CreateTable(t("t")), z));
    m.advance(ms(5));
    (m, c, isolated)
}

#[test]
fn galera_certifies_first_committer_wins() {
    let (mut m, [a, b, _], _) = galera();
    let z = ms(5);
    done(m.exec(a, &ins(1, "x"), z));
    m.advance(ms(10));
    done(m.exec(a, &Stmt::Begin, ms(10)));
    done(m.exec(b, &Stmt::Begin, ms(10)));
    done(m.exec(a, &upd(1, "a"), ms(10)));
    done(m.exec(b, &upd(1, "b"), ms(10)));
    done(m.exec(a, &Stmt::Commit, ms(10)));
    // b's local lock is brute-force aborted when a's writeset applies on g2.
    m.advance(ms(20));
    assert_eq!(
        done(m.exec(b, &Stmt::Commit, ms(20))),
        Outcome::Err(errno::DEADLOCK)
    );
    assert_eq!(m.rows("g2", S, "t"), vec![(1, "a".into())]);
    assert_eq!(
        done(m.exec(a, &Stmt::LastGtid, ms(20))).scalar(),
        Some("3-301-3")
    );
}

#[test]
fn galera_minority_refuses_queries() {
    let (mut m, [_, _, c], iso) = galera();
    iso.lock().unwrap().push(t("g3"));
    assert_eq!(
        done(m.exec(c, &get(1), ms(10))),
        Outcome::Err(errno::WSREP_NOT_READY)
    );
    iso.lock().unwrap().clear();
    assert_eq!(done(m.exec(c, &get(1), ms(10))), Outcome::rows(vec![]));
}

/// MariaDB 12.3 with `binlog_storage_engine = innodb`: `XA PREPARE` takes no
/// GTID and replicas never hold the prepared branch; `XA COMMIT` ships the
/// whole transaction as one group; `XA ROLLBACK` logs nothing. With the
/// classic binlog the prepare is a group of its own and replicas hold the
/// branch prepared (measured on 12.3.3 and 11.8).
#[test]
fn xa_prepare_is_not_binlogged_when_the_binlog_is_in_innodb() {
    for innodb in [false, true] {
        let mut m = Model::new(1);
        let mut p = ServerConfig::new("p", 201, 2);
        p.binlog_in_innodb = innodb;
        m.add_server(p);
        m.add_replica(ServerConfig::new("r1", 202, 2), "p", ms(10));
        let z = Duration::ZERO;
        let a = m.connect("p", None, S, z).unwrap();
        done(m.exec(a, &Stmt::CreateTable(t("t")), z));
        let pos = |m: &mut Model, c, now| -> String {
            done(m.exec(c, &Stmt::GtidBinlogPos, now))
                .scalar()
                .unwrap_or_default()
                .to_owned()
        };
        let before = pos(&mut m, a, z);
        done(m.exec(a, &Stmt::XaStart(t("x1")), z));
        done(m.exec(a, &ins(1, "xa"), z));
        done(m.exec(a, &Stmt::XaEnd(t("x1")), z));
        done(m.exec(a, &Stmt::XaPrepare(t("x1")), z));
        let after_prepare = pos(&mut m, a, z);
        m.advance(ms(100));
        let r = m.connect("r1", None, S, ms(100)).unwrap();
        let on_replica = done(m.exec(r, &Stmt::XaRecover, ms(100)));
        m.disconnect(a, ms(100));
        let b = m.connect("p", None, S, ms(100)).unwrap();
        done(m.exec(b, &Stmt::XaCommit(t("x1")), ms(100)));
        let after_commit = pos(&mut m, b, ms(100));
        m.advance(ms(200));
        assert_eq!(
            m.rows("r1", S, "t"),
            vec![(1, "xa".into())],
            "innodb={innodb}"
        );
        if innodb {
            assert_eq!(after_prepare, before);
            assert_eq!(on_replica, Outcome::rows(vec![]));
            assert_eq!(after_commit, "2-201-2");
        } else {
            assert_eq!(after_prepare, "2-201-2");
            assert_eq!(on_replica, Outcome::rows(vec![vec![Some("x1".into())]]));
            assert_eq!(after_commit, "2-201-3");
        }

        // A prepared branch rolled back: logged only with the file binlog.
        done(m.exec(b, &Stmt::XaStart(t("x2")), ms(200)));
        done(m.exec(b, &ins(2, "gone"), ms(200)));
        done(m.exec(b, &Stmt::XaEnd(t("x2")), ms(200)));
        done(m.exec(b, &Stmt::XaPrepare(t("x2")), ms(200)));
        m.disconnect(b, ms(200));
        let c = m.connect("p", None, S, ms(200)).unwrap();
        let mid = pos(&mut m, c, ms(200));
        done(m.exec(c, &Stmt::XaRollback(t("x2")), ms(200)));
        let end = pos(&mut m, c, ms(200));
        assert_eq!(mid == end, innodb, "innodb={innodb}: {mid} -> {end}");
    }
}
