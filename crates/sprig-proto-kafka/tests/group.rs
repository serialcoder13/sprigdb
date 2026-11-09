//! The group coordinator's state machine, with tokio's clock paused:
//! rebalance timeouts, static members, protocol checks.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use sprig_proto_kafka::group::{
    Coordinator, GroupConfig, GroupKey, GroupState, JoinArgs, SyncArgs,
};
use sprig_proto_kafka::mem::{MemGroupStore, MemLeases};
use sprig_proto_kafka::protocol::err;

fn coord() -> Arc<Coordinator> {
    Arc::new(Coordinator::new(
        Arc::new(MemGroupStore::default()),
        Arc::new(MemLeases::single(1)),
        GroupConfig {
            min_session: Duration::from_secs(6),
            max_session: Duration::from_secs(300),
            initial_rebalance_delay: Duration::from_secs(3),
            tick: Duration::from_millis(100),
        },
    ))
}

fn key() -> GroupKey {
    GroupKey {
        tenant: 9,
        group: "g".into(),
    }
}

fn args(member: &str, instance: Option<&str>, protocols: &[&str]) -> JoinArgs {
    JoinArgs {
        member_id: member.into(),
        group_instance_id: instance.map(Into::into),
        client_id: "c".into(),
        client_host: "/127.0.0.1".into(),
        session_timeout_ms: 10_000,
        rebalance_timeout_ms: 30_000,
        protocol_type: "consumer".into(),
        protocols: protocols
            .iter()
            .map(|p| ((*p).to_owned(), vec![1]))
            .collect(),
        require_known_member_id: false,
    }
}

#[tokio::test(start_paused = true)]
async fn validation() {
    let c = coord();
    let mut a = args("", None, &["range"]);
    a.session_timeout_ms = 1000;
    assert_eq!(c.join(&key(), a).await.error, err::INVALID_SESSION_TIMEOUT);
    assert_eq!(
        c.join(&key(), args("", None, &[])).await.error,
        err::INCONSISTENT_GROUP_PROTOCOL
    );
    assert_eq!(
        c.join(&key(), args("ghost", None, &["range"])).await.error,
        err::UNKNOWN_MEMBER_ID
    );
    let mut a = args("", None, &["range"]);
    a.require_known_member_id = true;
    let r = c.join(&key(), a).await;
    assert_eq!(r.error, err::MEMBER_ID_REQUIRED);
    assert!(r.member_id.starts_with("c-"));
}

#[tokio::test(start_paused = true)]
async fn initial_delay_then_rebalance_timeout_drops_silent_members() {
    let c = coord();
    let t0 = tokio::time::Instant::now();
    let ticker = tokio::spawn(Arc::clone(&c).run(sprig_common::CancellationToken::new()));
    // The first join waits the initial delay (3 s) for more members.
    let j1 = c
        .join(&key(), args("", None, &["range", "roundrobin"]))
        .await;
    assert_eq!(j1.error, err::NONE);
    assert!(t0.elapsed() >= Duration::from_secs(3));
    assert_eq!(j1.generation, 1);
    // A second member with a different first choice: range is common to
    // both; the leader's order breaks the tie.
    let c2 = Arc::clone(&c);
    let t = tokio::spawn(async move {
        c2.join(&key(), args("", None, &["roundrobin", "range"]))
            .await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Member 1 never syncs or rejoins: its session (10 s from the join's
    // completion) expires before the rebalance timeout (30 s), so it is
    // dropped then, and member 2 completes alone, as leader.
    let j2 = t.await.unwrap();
    assert_eq!(j2.error, err::NONE);
    assert_eq!(j2.leader, j2.member_id);
    assert_eq!(j2.members.len(), 1);
    assert_eq!(j2.generation, 2);
    assert_eq!(j2.protocol_name.as_deref(), Some("roundrobin"));
    let waited = t0.elapsed();
    assert!(
        waited >= Duration::from_secs(13) && waited < Duration::from_secs(14),
        "{waited:?}"
    );
    assert_eq!(
        c.heartbeat(&key(), &j1.member_id, j1.generation, None)
            .await,
        err::UNKNOWN_MEMBER_ID
    );
    ticker.abort();
}

#[tokio::test(start_paused = true)]
async fn static_member_replaces_its_old_incarnation() {
    let c = coord();
    let ticker = tokio::spawn(Arc::clone(&c).run(sprig_common::CancellationToken::new()));
    let j1 = c.join(&key(), args("", Some("pod-1"), &["range"])).await;
    let s = c
        .sync(
            &key(),
            SyncArgs {
                member_id: j1.member_id.clone(),
                generation: j1.generation,
                assignments: vec![(j1.member_id.clone(), vec![7])],
                ..Default::default()
            },
        )
        .await;
    assert_eq!(s.assignment, vec![7]);
    // The pod restarts: same instance id, no member id.
    let j2 = c.join(&key(), args("", Some("pod-1"), &["range"])).await;
    assert_eq!(j2.error, err::NONE);
    assert_ne!(j2.member_id, j1.member_id);
    assert_eq!(
        c.heartbeat(&key(), &j1.member_id, j1.generation, Some("pod-1"))
            .await,
        err::UNKNOWN_MEMBER_ID
    );
    // A protocol nobody else supports is refused while members exist.
    let r = c.join(&key(), args("", None, &["sticky"])).await;
    assert_eq!(r.error, err::INCONSISTENT_GROUP_PROTOCOL);
    // Leaving by instance id empties the group.
    let (e, each) = c
        .leave(&key(), &[(String::new(), Some("pod-1".into()))])
        .await;
    assert_eq!((e, each), (err::NONE, vec![err::NONE]));
    assert_eq!(c.describe(&key()).await.state, GroupState::Empty);
    ticker.abort();
}
