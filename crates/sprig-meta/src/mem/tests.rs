use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use sprig_common::SprigCode;
use sprig_common::ids::DcId;
use sprig_common::time::ManualClock;

use super::*;

fn store() -> (MemStore, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new());
    let tier = Tier::Dc(DcId::new("mum1").unwrap());
    (MemStore::with_clock(tier, clock.clone()), clock)
}

fn key(s: &MemStore, rel: &str) -> Key {
    s.tier().key(rel).unwrap()
}

fn put(k: &Key, v: &str) -> Op {
    Op::Put {
        key: k.clone(),
        value: v.as_bytes().to_vec(),
        lease: None,
    }
}

async fn get(s: &MemStore, k: &Key) -> Option<String> {
    let kv = s.get(k, Consistency::Linearizable).await.unwrap()?;
    Some(String::from_utf8(kv.value).unwrap())
}

async fn next(w: &mut WatchStream) -> WatchEvent {
    tokio::time::timeout(Duration::from_secs(1), w.next())
        .await
        .expect("watch event within 1 s")
        .expect("stream open")
        .unwrap()
}

#[tokio::test]
#[allow(clippy::many_single_char_names)]
async fn create_if_missing_then_guarded_update() {
    let (s, _) = store();
    let k = key(&s, "clusters/mum1-c7");
    let create = Txn {
        when: vec![Cmp::Missing(k.clone())],
        then: vec![put(&k, r#"{"v":1}"#)],
        otherwise: vec![Op::Get(k.clone())],
    };
    let r = s.txn(create.clone()).await.unwrap();
    assert!(r.succeeded);
    assert_eq!(r.revision, 2);
    let kv = s.get(&k, Consistency::Linearizable).await.unwrap().unwrap();
    assert_eq!((kv.create_revision, kv.mod_revision, kv.version), (2, 2, 1));

    // A second create fails and returns the current value instead.
    let r = s.txn(create).await.unwrap();
    assert!(!r.succeeded);
    assert_eq!(r.revision, 2);
    assert!(matches!(&r.results[0], OpResult::Get(Some(kv)) if kv.mod_revision == 2));

    // Updates guarded by ModRev: a stale revision loses.
    let stale = Txn {
        when: vec![Cmp::ModRev(k.clone(), 1)],
        then: vec![put(&k, "stale")],
        otherwise: vec![],
    };
    assert!(!s.txn(stale).await.unwrap().succeeded);
    let fresh = Txn {
        when: vec![Cmp::ModRev(k.clone(), 2), Cmp::CreateRev(k.clone(), 2)],
        then: vec![put(&k, "fresh")],
        otherwise: vec![],
    };
    assert!(s.txn(fresh).await.unwrap().succeeded);
    let kv = s.get(&k, Consistency::Linearizable).await.unwrap().unwrap();
    assert_eq!((kv.create_revision, kv.mod_revision, kv.version), (2, 3, 2));

    // One revision per transaction, however many writes it has.
    let (a, b) = (key(&s, "jobs/a"), key(&s, "jobs/b"));
    let both = Txn {
        then: vec![put(&a, "1"), put(&b, "2"), Op::Delete(k.clone())],
        ..Txn::default()
    };
    let r = s.txn(both).await.unwrap();
    assert_eq!(r.revision, 4);
    assert_eq!(r.results[2], OpResult::Delete(true));
    assert_eq!(get(&s, &k).await, None);
}

#[tokio::test]
async fn listing_pages_at_one_revision() {
    let (s, _) = store();
    let prefix = key(&s, "clusters/");
    for name in ["a", "b", "c"] {
        let k = key(&s, &format!("clusters/{name}"));
        s.txn(Txn {
            then: vec![put(&k, name)],
            ..Txn::default()
        })
        .await
        .unwrap();
    }
    s.txn(Txn {
        then: vec![put(&key(&s, "dcs/x"), "x")],
        ..Txn::default()
    })
    .await
    .unwrap();

    let first = s
        .list(
            &prefix,
            ListOpts {
                limit: 2,
                ..ListOpts::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(first.kvs.len(), 2);
    assert!(first.more);

    // Writes between pages don't show in a listing pinned to its revision.
    let c = key(&s, "clusters/c");
    s.txn(Txn {
        then: vec![put(&c, "c2")],
        ..Txn::default()
    })
    .await
    .unwrap();
    s.txn(Txn {
        then: vec![Op::Delete(key(&s, "clusters/b"))],
        ..Txn::default()
    })
    .await
    .unwrap();

    let rest = s
        .list(
            &prefix,
            ListOpts {
                at: Some(first.revision),
                after: first.kvs.last().map(|kv| kv.key.clone()),
                limit: 2,
            },
        )
        .await
        .unwrap();
    assert!(!rest.more);
    assert_eq!(rest.kvs.len(), 1);
    assert_eq!(rest.kvs[0].value, b"c");

    let now = s.list(&prefix, ListOpts::default()).await.unwrap();
    let values: Vec<_> = now.kvs.iter().map(|kv| kv.value.clone()).collect();
    assert_eq!(values, vec![b"a".to_vec(), b"c2".to_vec()]);
}

#[tokio::test]
async fn watch_replays_history_then_follows_live_writes() {
    let (s, _) = store();
    let prefix = key(&s, "settings/");
    let k = key(&s, "settings/auto_index.min_runs");
    let other = key(&s, "clusters/x");
    let r1 = s
        .txn(Txn {
            then: vec![put(&k, "50")],
            ..Txn::default()
        })
        .await
        .unwrap()
        .revision;
    s.txn(Txn {
        then: vec![put(&other, "x")],
        ..Txn::default()
    })
    .await
    .unwrap();

    let mut w = s.watch(&prefix, r1).await.unwrap();
    assert!(matches!(next(&mut w).await, WatchEvent::Put(kv) if kv.value == b"50"));

    s.txn(Txn {
        then: vec![put(&other, "y")],
        ..Txn::default()
    })
    .await
    .unwrap();
    s.txn(Txn {
        then: vec![put(&k, "60")],
        ..Txn::default()
    })
    .await
    .unwrap();
    s.txn(Txn {
        then: vec![Op::Delete(k.clone())],
        ..Txn::default()
    })
    .await
    .unwrap();
    assert!(matches!(next(&mut w).await, WatchEvent::Put(kv) if kv.value == b"60"));
    assert!(matches!(next(&mut w).await, WatchEvent::Delete { key, mod_revision: 6 } if key == k));

    s.request_progress();
    assert_eq!(next(&mut w).await, WatchEvent::Progress(6));

    // Watching from 0 starts with the next write.
    let mut live = s.watch(&prefix, 0).await.unwrap();
    s.txn(Txn {
        then: vec![put(&k, "70")],
        ..Txn::default()
    })
    .await
    .unwrap();
    assert!(matches!(next(&mut live).await, WatchEvent::Put(kv) if kv.mod_revision == 7));
}

#[tokio::test]
async fn compaction_invalidates_old_revisions() {
    let (s, _) = store();
    let prefix = key(&s, "jobs/");
    let k = key(&s, "jobs/a");
    let r1 = s
        .txn(Txn {
            then: vec![put(&k, "1")],
            ..Txn::default()
        })
        .await
        .unwrap()
        .revision;
    let r2 = s
        .txn(Txn {
            then: vec![put(&k, "2")],
            ..Txn::default()
        })
        .await
        .unwrap()
        .revision;
    s.compact(r2);

    let old = ListOpts {
        at: Some(r1),
        ..ListOpts::default()
    };
    assert_eq!(
        s.list(&prefix, old).await.unwrap_err().code(),
        Some(SprigCode::MetaUnavailable)
    );
    let mut w = s.watch(&prefix, r1).await.unwrap();
    assert_eq!(
        next(&mut w).await,
        WatchEvent::Compacted {
            compact_revision: r2
        }
    );
    assert!(w.next().await.is_none());

    // The latest value survives compaction.
    assert_eq!(get(&s, &k).await.as_deref(), Some("2"));
}

#[tokio::test]
async fn leases_expire_on_the_clock() {
    let (s, clock) = store();
    let k = key(&s, "gateways/gw-7");
    let lease = s.grant_lease(Duration::from_secs(10)).await.unwrap();
    let bound = Op::Put {
        key: k.clone(),
        value: b"{}".to_vec(),
        lease: Some(lease),
    };
    s.txn(Txn {
        then: vec![bound],
        ..Txn::default()
    })
    .await
    .unwrap();
    let mut w = s.watch(&key(&s, "gateways/"), 0).await.unwrap();

    clock.advance(Duration::from_secs(8));
    assert_eq!(
        s.keep_alive(lease).await.unwrap(),
        Some(Duration::from_secs(10))
    );
    clock.advance(Duration::from_secs(8));
    assert!(get(&s, &k).await.is_some(), "kept alive");

    clock.advance(Duration::from_secs(3));
    assert_eq!(get(&s, &k).await, None, "expired");
    assert!(matches!(next(&mut w).await, WatchEvent::Delete { key, .. } if key == k));
    assert_eq!(s.keep_alive(lease).await.unwrap(), None);

    // Revoking deletes the lease's keys at once.
    let lease = s.grant_lease(Duration::from_secs(10)).await.unwrap();
    let bound = Op::Put {
        key: k.clone(),
        value: b"{}".to_vec(),
        lease: Some(lease),
    };
    s.txn(Txn {
        then: vec![bound],
        ..Txn::default()
    })
    .await
    .unwrap();
    s.revoke_lease(lease).await.unwrap();
    assert_eq!(get(&s, &k).await, None);

    // A put bound to a missing lease fails and writes nothing.
    let orphan = Op::Put {
        key: k.clone(),
        value: b"{}".to_vec(),
        lease: Some(lease),
    };
    assert!(
        s.txn(Txn {
            then: vec![orphan],
            ..Txn::default()
        })
        .await
        .is_err()
    );
    assert_eq!(get(&s, &k).await, None);
}

#[tokio::test]
async fn injected_outage_fails_operations_and_ends_watches() {
    let (s, _) = store();
    let prefix = key(&s, "dcs/");
    let mut w = s.watch(&prefix, 0).await.unwrap();
    s.set_unavailable(true);
    let err = s.get(&prefix, Consistency::Serializable).await.unwrap_err();
    assert_eq!(err.code(), Some(SprigCode::MetaUnavailable));
    assert!(err.retryable);
    assert!(w.next().await.is_none());
    s.set_unavailable(false);
    assert!(
        s.get(&prefix, Consistency::Serializable)
            .await
            .unwrap()
            .is_none()
    );
}
