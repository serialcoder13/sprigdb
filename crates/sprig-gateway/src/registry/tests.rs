use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::DcId;
use sprig_common::time::{Clock, SystemClock};
use sprig_meta::{MemStore, MetaStore, Tier};

use super::*;

fn record(id: &str) -> GatewayRecord {
    GatewayRecord {
        gateway_id: id.to_owned(),
        slot: 0,
        version: "test".into(),
        addr: "127.0.0.1".into(),
        listeners: BTreeMap::from([("mysql".to_owned(), 3306)]),
        state: GatewayState::Starting,
        catalog_formats: vec![],
        authorizers: vec![],
        load: Load::default(),
        kafka_host: None,
    }
}

fn store() -> Arc<dyn MetaStore> {
    Arc::new(MemStore::new(Tier::Dc(DcId::new("dc-t").unwrap())))
}

#[tokio::test]
async fn gateways_get_distinct_slots_and_records() {
    let store = store();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let stop = CancellationToken::new();
    let mut tasks = Vec::new();
    let mut tables = Vec::new();
    for id in ["gw-a", "gw-b", "gw-c"] {
        let r = Registrar::new(record(id));
        let sessions = Arc::new(SessionTable::new(clock.clone()));
        tasks.push(
            r.start(
                store.clone(),
                clock.clone(),
                sessions.clone(),
                || Duration::from_secs(5),
                stop.clone(),
            )
            .await,
        );
        tables.push(sessions);
    }
    let slots: std::collections::BTreeSet<u16> = tables.iter().map(|t| t.slot()).collect();
    assert_eq!(slots.len(), 3, "distinct slots: {slots:?}");
    assert!(!slots.contains(&0));
    let all = list(store.as_ref()).await.unwrap();
    assert_eq!(all.len(), 3);
    let b = get(store.as_ref(), "gw-b").await.unwrap().unwrap();
    assert_eq!(b.slot, tables[1].slot());
    let holder = slot_holder(store.as_ref(), tables[2].slot())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(holder.gateway_id, "gw-c");
    stop.cancel();
    for t in tasks {
        t.await.unwrap();
    }
    // Stopping gives the records and slots back.
    assert!(list(store.as_ref()).await.unwrap().is_empty());
}

#[tokio::test]
async fn state_changes_are_written_at_once() {
    let store = store();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let stop = CancellationToken::new();
    let r = Registrar::new(record("gw-a"));
    let sessions = Arc::new(SessionTable::new(clock.clone()));
    let task = r
        .start(
            store.clone(),
            clock,
            sessions,
            || Duration::from_secs(5),
            stop.clone(),
        )
        .await;
    r.set_state(GatewayState::Draining);
    let mut seen = None;
    for _ in 0..50 {
        seen = get(store.as_ref(), "gw-a").await.unwrap().map(|g| g.state);
        if seen == Some(GatewayState::Draining) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(seen, Some(GatewayState::Draining));
    stop.cancel();
    task.await.unwrap();
}
