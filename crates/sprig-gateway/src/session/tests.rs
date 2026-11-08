use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sprig_common::time::{ManualClock, SystemClock};
use sprig_common::{CancellationToken, SprigCode};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use super::*;
use crate::frontend::{ClientConn, InProtocolTls, MaybeTls};
use crate::listener::ListenerSpec;

fn peer() -> SocketAddr {
    "10.0.0.7:50000".parse().unwrap()
}

fn table() -> Arc<SessionTable> {
    Arc::new(SessionTable::new(Arc::new(SystemClock)))
}

fn open(t: &Arc<SessionTable>, listener: &'static str, generation: u64) -> Session {
    t.open_parts(Protocol::MySql84, listener, generation, peer(), None)
        .unwrap()
}

/// A connected client socket and the server side wrapped as a
/// [`ClientConn`] of the `mysql` listener.
async fn client_conn() -> (TcpStream, ClientConn) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(l.local_addr().unwrap()).await.unwrap();
    let (server, addr) = l.accept().await.unwrap();
    let spec = ListenerSpec::new("mysql", Protocol::MySql84, l.local_addr().unwrap());
    let conn = ClientConn {
        stream: MaybeTls::Plain(server),
        peer: addr,
        listener: Arc::new(spec),
        generation: 3,
        in_protocol_tls: InProtocolTls::NotOffered,
        retired: CancellationToken::new(),
        close_soon: CancellationToken::new(),
        acceptor: None,
    };
    (client, conn)
}

#[test]
fn conn_ids_pack_slot_and_counter() {
    // Chapter 03's example id.
    let id = ConnId(1_179_649);
    assert_eq!((slot_of(id), counter_of(id)), (1, 131_073));
    assert_eq!(conn_id(1, 131_073), id);
    let max = conn_id(MAX_SLOT, MAX_COUNTER);
    assert_eq!(max, ConnId(u32::MAX));
    assert_eq!((slot_of(max), counter_of(max)), (4095, 0xF_FFFF));
    assert_eq!(conn_id(17, 1), ConnId((17 << 20) | 1));
}

#[test]
fn ids_count_up_on_slot_zero_then_the_registered_slot() {
    let t = table();
    let a = open(&t, "mysql", 1);
    let b = open(&t, "mysql", 1);
    assert_eq!((a.id, b.id), (ConnId(1), ConnId(2)));
    assert_eq!(slot_of(a.id), 0, "slot 0 until registration");
    t.set_slot(17).unwrap();
    let c = open(&t, "mysql", 1);
    assert_eq!(c.id, conn_id(17, 3));
    assert!(t.set_slot(4096).is_err());
    assert_eq!(t.len(), 3);
}

#[test]
fn counter_wraps_and_skips_ids_in_use() {
    let t = table();
    let one = open(&t, "mysql", 1); // id 1, kept open
    t.set_counter(MAX_COUNTER - 1);
    let last = open(&t, "mysql", 1);
    assert_eq!(counter_of(last.id), MAX_COUNTER);
    let wrapped = open(&t, "mysql", 1);
    assert_eq!(wrapped.id, ConnId(2), "0 is never used and 1 is in use");
    drop(one);
    t.set_counter(MAX_COUNTER);
    assert_eq!(open(&t, "mysql", 1).id, ConnId(1), "freed ids are reused");
}

#[test]
fn dropping_a_session_removes_it() {
    let t = table();
    let s = open(&t, "mysql", 1);
    let id = s.id;
    assert!(t.get(id).is_some());
    drop(s);
    assert!(t.get(id).is_none());
    assert!(t.is_empty());
}

#[test]
fn processlist_rows() {
    let clock = Arc::new(ManualClock::new());
    let t = Arc::new(SessionTable::new(clock.clone()));
    let mut a = open(&t, "mysql", 1);
    let b = t
        .open_parts(Protocol::MariaDb, "mariadb", 4, peer(), None)
        .unwrap();
    a.set_state(SessionState::Authenticating);
    a.set_principal(Principal {
        tenant: None,
        tenant_name: None,
        user_id: None,
        user: "app".into(),
        host: "%".into(),
        client_ip: peer().ip(),
        must_change_password: false,
        limits: sprig_auth::store::AccountLimits::default(),
        auth_version: 0,
        backend: sprig_auth::BackendTarget {
            cluster: sprig_common::ids::ClusterId::new("dc-a-c1").unwrap(),
            user: "root".into(),
            credentials: "env:SPRIG_TEST_BACKEND".parse().unwrap(),
        },
    });
    a.set_state(SessionState::Ready);
    a.set_db(Some("shop".into()));
    clock.advance(Duration::from_secs(5));
    a.begin_command("Query", Some("SELECT SLEEP(10)".into()));
    clock.advance(Duration::from_secs(2));

    let rows = t.list();
    assert_eq!(rows.len(), 2);
    let ra = &rows[0];
    assert_eq!(ra.id, a.id);
    assert_eq!(ra.user.as_deref(), Some("app"));
    assert_eq!(ra.db.as_deref(), Some("shop"));
    assert_eq!(ra.command, "Query");
    assert_eq!(ra.info.as_deref(), Some("SELECT SLEEP(10)"));
    assert_eq!(ra.time, Duration::from_secs(2));
    assert_eq!(ra.state, SessionState::Ready);
    assert_eq!((ra.listener, ra.listener_gen), ("mysql", 1));
    let rb = &rows[1];
    assert_eq!(rb.id, b.id);
    assert_eq!((rb.command, rb.user.as_deref()), ("Connect", None));
    assert_eq!(rb.state, SessionState::Handshake);
    assert_eq!(rb.time, Duration::from_secs(7));
    assert_eq!(rb.protocol, Protocol::MariaDb);

    a.end_command();
    let ra = &t.list()[0];
    assert_eq!((ra.command, ra.info.as_deref()), ("Sleep", None));
    assert_eq!(ra.time, Duration::ZERO);
}

#[test]
fn kill_flags_and_safe_points() {
    let t = table();
    let mut s = open(&t, "mysql", 1);
    s.set_state(SessionState::Ready);
    assert!(!s.should_close());

    assert!(t.kill_query(s.id));
    assert!(s.flags().kill_query_requested());
    assert!(!s.should_close(), "KILL QUERY keeps the session");
    s.begin_command("Query", None);
    assert!(
        !s.flags().kill_query_requested(),
        "a new statement clears an old KILL QUERY"
    );

    s.set_state(SessionState::InTransaction);
    assert_eq!(t.close_all_at_safe_point(), 1);
    assert!(!s.should_close(), "not inside a transaction");
    s.set_state(SessionState::Ready);
    assert!(s.should_close());
    assert_eq!(s.flags().close_reason(), Some(CloseReason::Drain));
    assert_eq!(
        CloseReason::Drain.error().code(),
        Some(SprigCode::GatewayDraining)
    );

    let k = open(&t, "mysql", 1);
    assert!(t.kill_conn(k.id));
    assert!(k.flags().conn_killed() && k.flags().kill_query_requested());
    assert!(k.should_close());
    assert!(!t.kill_conn(ConnId(999_999)));
    assert!(!t.kill_query(ConnId(999_999)));
}

#[test]
fn counts_and_closes_by_listener_generation() {
    let t = table();
    let old = [open(&t, "mysql", 1), open(&t, "mysql", 1)];
    let new = open(&t, "mysql", 2);
    let other = open(&t, "mariadb", 1);
    let counts = t.count_by_generation("mysql");
    assert_eq!(counts.into_iter().collect::<Vec<_>>(), vec![(1, 2), (2, 1)]);
    assert_eq!(t.count_by_generation("redis").len(), 0);

    assert_eq!(t.close_older_than("mysql", 2), 2);
    assert!(old.iter().all(|s| s.flags().close_requested()));
    assert_eq!(
        old[0].flags().close_reason(),
        Some(CloseReason::ListenerRetired)
    );
    let e = CloseReason::ListenerRetired.error();
    assert_eq!(e.code(), Some(SprigCode::ListenerRetired));
    assert_eq!(&e.protocol().sqlstate, b"08S01");
    assert!(!new.flags().close_requested());
    assert!(!other.flags().close_requested());
    // The first reason is kept.
    assert_eq!(t.close_all_at_safe_point(), 4);
    assert_eq!(
        old[0].flags().close_reason(),
        Some(CloseReason::ListenerRetired)
    );
    assert_eq!(new.flags().close_reason(), Some(CloseReason::Drain));
}

#[tokio::test]
async fn query_killed_wakes_a_waiter() {
    let t = table();
    let s = open(&t, "mysql", 1);
    let h = Arc::clone(s.handle());
    let waiter = tokio::spawn(async move { h.flags().query_killed().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiter.is_finished());
    t.kill_query(s.id);
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_panicking_session_is_isolated() {
    let t = table();
    let survivor_done = Arc::new(AtomicBool::new(false));
    let done = Arc::clone(&survivor_done);
    let survivor = open(&t, "mysql", 1);
    let survivor_id = survivor.id;
    let survivor_task = tokio::spawn(async move {
        SessionTable::run_session(survivor, (), |s, ()| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(s.state(), SessionState::Handshake);
            done.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await
    });

    let victim = open(&t, "mysql", 1);
    let victim_id = victim.id;
    let r = SessionTable::run_session(victim, (), |_s, ()| async move {
        tokio::task::yield_now().await;
        panic!("boom in a session");
    })
    .await;
    let e = r.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::Internal));
    assert!(e.message.contains(&victim_id.to_string()));
    assert!(
        t.get(victim_id).is_none(),
        "the panicked session left the table"
    );
    assert!(
        t.get(survivor_id).is_some(),
        "the other session is untouched"
    );

    survivor_task.await.unwrap().unwrap();
    assert!(survivor_done.load(Ordering::SeqCst));
    assert!(t.is_empty());

    // A panic while building the future is caught too.
    let early = open(&t, "mysql", 1);
    let r = SessionTable::run_session(
        early,
        (),
        |_s, ()| -> std::future::Ready<sprig_common::Result<()>> {
            panic!("boom before the first await")
        },
    )
    .await;
    assert_eq!(r.unwrap_err().code(), Some(SprigCode::Internal));
    assert!(t.is_empty());
}

/// Sets its flag when dropped.
struct Flag(Arc<AtomicBool>);

impl Drop for Flag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn kill_drops_the_session_task_and_closes_the_connection() {
    let t = table();
    let (mut client, conn) = client_conn().await;
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Flag(Arc::clone(&dropped));
    let t2 = Arc::clone(&t);
    let task = tokio::spawn(async move {
        t2.run(conn, Protocol::MySql84, move |s, conn| async move {
            let _keep = (flag, conn);
            // Only a field of `s` is captured, so the Session itself is
            // dropped here; the entry must stay until the task ends.
            assert_eq!(s.listener_gen, 3);
            std::future::pending::<()>().await;
            Ok(())
        })
        .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let id = loop {
        if let Some(row) = t.list().first() {
            break row.id;
        }
        assert!(!task.is_finished(), "{:?}", task.await);
        assert!(
            tokio::time::Instant::now() < deadline,
            "the session never opened"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!(t.list()[0].listener, "mysql");
    assert!(t.kill_conn(id));
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert!(t.is_empty());
    // The server side is closed: the client reads end of file.
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::io::AsyncReadExt::read(&mut client, &mut buf),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn peer_closed_waits_for_the_client_to_go() {
    let (client, conn) = client_conn().await;
    let watch = tokio::spawn(async move {
        client_gone(&conn.stream).await;
        conn
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!watch.is_finished(), "still connected");
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), watch)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn pipelined_bytes_are_not_a_disconnect_and_stay_unread() {
    let (mut client, conn) = client_conn().await;
    client.write_all(b"next command").await.unwrap();
    let watch = tokio::spawn(async move {
        client_gone(&conn.stream).await;
        conn
    });
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!watch.is_finished(), "unread bytes are not a hang-up");
    // A FIN after unread bytes is still seen.
    client.shutdown().await.unwrap();
    drop(client);
    let conn = tokio::time::timeout(Duration::from_secs(5), watch)
        .await
        .unwrap()
        .unwrap();
    let mut buf = [0u8; 12];
    let MaybeTls::Plain(mut s) = conn.stream else {
        unreachable!()
    };
    tokio::io::AsyncReadExt::read_exact(&mut s, &mut buf)
        .await
        .unwrap();
    assert_eq!(&buf, b"next command");
}

#[tokio::test]
async fn watch_statement_outcomes() {
    let t = table();
    let s = open(&t, "mysql", 1);
    let (client, conn) = client_conn().await;

    let done = watch_statement(s.handle(), &conn.stream, async { 42 }).await;
    assert!(matches!(done, StatementEnd::Done(42)));

    let h = Arc::clone(s.handle());
    let kill_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        h.flags().kill_query();
    });
    let killed = watch_statement(s.handle(), &conn.stream, std::future::pending::<()>()).await;
    assert!(matches!(killed, StatementEnd::Killed));
    kill_task.await.unwrap();
    assert!(s.flags().take_kill_query());

    let hang_up = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(client);
    });
    let gone = tokio::time::timeout(
        Duration::from_secs(5),
        watch_statement(s.handle(), &conn.stream, std::future::pending::<()>()),
    )
    .await
    .unwrap();
    assert!(matches!(gone, StatementEnd::ClientGone));
    hang_up.await.unwrap();
}

#[tokio::test]
async fn context_has_a_session_table() {
    let ctx = crate::frontend::GatewayContext::new(Arc::new(SystemClock));
    let (_client, conn) = client_conn().await;
    let s = ctx.sessions.open(&conn, Protocol::MySql84).unwrap();
    assert_eq!(s.id, ConnId(1));
    assert_eq!(ctx.sessions.len(), 1);
}
