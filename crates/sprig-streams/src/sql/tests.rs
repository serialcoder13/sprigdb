#![allow(clippy::unwrap_used)]

use std::collections::HashMap;

use sprig_common::ids::{ShardId, TenantId};

use super::*;

#[derive(Default)]
struct Mock {
    ddl: Vec<String>,
    committed: Vec<StreamsChange>,
    subscribed: Vec<StreamId>,
    unsubscribed: Vec<StreamId>,
    next: u64,
    deny: Vec<Privilege>,
    filtered: bool,
    change_topic: bool,
    tables: HashMap<String, TableInfo>,
}

fn slot(shard: u32) -> PhysSlot {
    PhysSlot {
        slot: ShardSlot {
            group: GroupId(1),
            shard: ShardId(shard),
        },
        schema: format!("t7_d1_g1_s{shard}"),
        cluster: ClusterId::new("dc-a-c1").unwrap(),
    }
}

#[async_trait::async_trait]
impl Host for Mock {
    fn tenant(&self) -> TenantId {
        TenantId(7)
    }
    fn db_id(&self, name: &str) -> Option<DbId> {
        (name == "shop").then_some(DbId(1))
    }
    fn table(&self, _db: DbId, name: &str) -> Option<TableInfo> {
        self.tables.get(name).cloned()
    }
    fn template(&self, _db: DbId) -> Option<PhysSlot> {
        Some(slot(1))
    }
    fn allowed(&self, p: Privilege, _db: &str, _o: Option<ObjectId>, _n: &str) -> bool {
        !self.deny.contains(&p)
    }
    fn row_filtered(&self, _db: &str, _o: ObjectId) -> bool {
        self.filtered
    }
    fn settings(&self, _db: DbId) -> StmtSettings {
        StmtSettings {
            partitions_default: 12,
            max_message_bytes: 100,
        }
    }
    fn change_topic_on(&self, _db: DbId, _o: ObjectId) -> bool {
        self.change_topic
    }
    fn user(&self) -> (String, String) {
        ("app".into(), "%".into())
    }
    async fn ddl(&mut self, _slot: &PhysSlot, statements: &[String]) -> Result<()> {
        self.ddl.extend(statements.iter().cloned());
        Ok(())
    }
    async fn position(&mut self, _slot: &PhysSlot) -> Result<String> {
        Ok("0-1-100".into())
    }
    async fn next_id(&mut self) -> Result<u64> {
        self.next += 1;
        Ok(40 + self.next)
    }
    async fn commit(&mut self, change: &StreamsChange) -> Result<()> {
        self.committed.push(change.clone());
        Ok(())
    }
    async fn subscribe(&mut self, def: &StreamDef) -> Result<()> {
        self.subscribed.push(def.id);
        Ok(())
    }
    async fn unsubscribe(&mut self, def: &StreamDef) -> Result<()> {
        self.unsubscribed.push(def.id);
        Ok(())
    }
}

fn cx(state: &StreamsState) -> Ctx<'_> {
    Ctx {
        state,
        lex: LexOptions::mariadb(),
        lctn: false,
        current: Some("shop"),
        mysql: false,
    }
}

fn created(m: &Mock) -> StreamDef {
    match &m.committed[0] {
        StreamsChange::Insert(d, _) => d.clone(),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn create_publish_read_drop() {
    let empty = StreamsState::default();
    let mut m = Mock::default();
    m.tables.insert(
        "orders".into(),
        TableInfo {
            object: ObjectId(9),
            name: "orders".into(),
            slots: vec![slot(1)],
        },
    );
    let r = handle(
        "CREATE STREAM order_events KEY (order_id) ATTACHED TO GROUP OF orders PARTITIONS 4 RETENTION '3 days'",
        &cx(&empty),
        &mut m,
    )
    .await
    .unwrap();
    assert_eq!(r, Handled::Ok { affected: 0 });
    let def = created(&m);
    assert_eq!(def.partitions, 4);
    assert_eq!(def.attached_group, Some(GroupId(1)));
    assert!(matches!(&m.committed[0], StreamsChange::Insert(_, Some(72))));
    assert_eq!(m.subscribed, vec![def.id]);
    assert!(m.ddl.iter().any(|s| s.contains("__sprig_eo_41")));
    assert!(m.ddl.iter().any(|s| s.contains("__sprig_sl_41")));
    assert!(m.ddl.iter().any(|s| s.contains("X'4064632D612D63317C302D312D313030'")), "start position stored");

    let state = StreamsState::new(vec![def.clone()]);
    // Publishing.
    let r = handle(
        "INSERT INTO order_events (key, value, headers) VALUES (7001, JSON_OBJECT('a', 1), NULL)",
        &cx(&state),
        &mut m,
    )
    .await
    .unwrap();
    assert_eq!(
        r,
        Handled::Forward {
            sql: "INSERT INTO `__sprig_eo_41` (k, v, headers) VALUES (7001, JSON_OBJECT('a', 1), NULL)".into(),
            write: true,
            compact: false
        }
    );
    let Handled::Forward { sql, .. } = handle("insert shop.order_events values ('k', 'v')", &cx(&state), &mut m)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(sql, "insert `shop`.`__sprig_eo_41` (k, v) values ('k', 'v')");
    let e = handle(
        &format!("INSERT INTO order_events (key, value) VALUES (1, '{}')", "x".repeat(101)),
        &cx(&state),
        &mut m,
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::StreamRecordTooLarge));
    let e = handle("INSERT INTO order_events (key, nope) VALUES (1, 2)", &cx(&state), &mut m)
        .await
        .unwrap_err();
    assert!(e.message.contains("Unknown column 'nope'"));
    for w in [
        "UPDATE order_events SET v = 1",
        "DELETE FROM order_events",
        "TRUNCATE order_events",
        "INSERT INTO order_events (key, value) VALUES (1, 2) ON DUPLICATE KEY UPDATE value = 3",
    ] {
        let e = handle(w, &cx(&state), &mut m).await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::StreamAppendOnly), "{w}");
    }
    let e = handle("CREATE TABLE order_events (a INT)", &cx(&state), &mut m)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::StreamExists));
    // Reading.
    let Handled::Forward { sql, write, .. } = handle(
        "SELECT offset_no, key FROM order_events WHERE partition_no = 1",
        &cx(&state),
        &mut m,
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert!(!write);
    assert!(sql.contains("`__sprig_sl_41`"), "{sql}");
    // Not streams.
    assert_eq!(handle("SELECT 1", &cx(&state), &mut m).await.unwrap(), Handled::NotStream);
    assert_eq!(
        handle("INSERT INTO orders VALUES (1)", &cx(&state), &mut m).await.unwrap(),
        Handled::NotStream
    );
    // Privileges.
    m.deny = vec![Privilege::Insert, Privilege::Select];
    let e = handle("INSERT INTO order_events (key, value) VALUES (1, 2)", &cx(&state), &mut m)
        .await
        .unwrap_err();
    assert!(e.message.contains("INSERT command denied"));
    let e = handle("SELECT * FROM order_events", &cx(&state), &mut m).await.unwrap_err();
    assert!(e.message.contains("SELECT command denied"));
    m.deny.clear();
    // ALTER and DROP.
    let e = handle("ALTER STREAM order_events PARTITIONS 2", &cx(&state), &mut m)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::StreamPartitionsDecrease));
    handle("ALTER STREAM order_events PARTITIONS 6 CLEANUP compact", &cx(&state), &mut m)
        .await
        .unwrap();
    assert!(matches!(m.committed.last(), Some(StreamsChange::Update(d, None)) if d.partitions == 6 && d.cleanup == Cleanup::Compact));
    assert!(m.ddl.iter().any(|s| s.contains("(41, 5, 0, 0, 0, '')")));
    assert!(m.ddl.iter().any(|s| s.contains("ADD CONSTRAINT sprig_sc_needs_key")));
    handle("DROP STREAM order_events", &cx(&state), &mut m).await.unwrap();
    assert_eq!(m.unsubscribed, vec![def.id]);
    assert!(m.ddl.iter().any(|s| s.starts_with("DROP TABLE IF EXISTS `t7_d1_g1_s1`.`__sprig_eo_41`")));
    let e = handle("DROP STREAM nope", &cx(&state), &mut m).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::StreamNotFound));
    handle("DROP STREAM IF EXISTS nope", &cx(&state), &mut m).await.unwrap();
    let Handled::Rows { rows, .. } = handle("SHOW STREAMS", &cx(&state), &mut m).await.unwrap() else {
        panic!()
    };
    assert_eq!(rows[0][0].as_deref(), Some("order_events"));
}

#[tokio::test]
async fn change_topics() {
    let empty = StreamsState::default();
    let mut m = Mock {
        change_topic: true,
        ..Mock::default()
    };
    m.tables.insert(
        "orders".into(),
        TableInfo {
            object: ObjectId(9),
            name: "orders".into(),
            slots: vec![slot(1)],
        },
    );
    let q = change_topic_target(
        "ALTER TABLE orders SPRIG SET (stream.change_topic = on, stream.retention_hours = 72)",
        &LexOptions::mariadb(),
    )
    .unwrap();
    assert_eq!(q.name, "orders");
    assert!(change_topic_target("ALTER TABLE orders SPRIG SET (x.y = 1)", &LexOptions::mariadb()).is_none());
    assert!(reconcile_change_topic(&q, &cx(&empty), &mut m).await.unwrap());
    let def = created(&m);
    assert_eq!(def.name, "orders.changes");
    assert_eq!(def.kind, StreamKind::Changes);
    assert_eq!(def.source_object, Some(ObjectId(9)));
    let state = StreamsState::new(vec![def.clone()]);
    assert!(!reconcile_change_topic(&q, &cx(&state), &mut m).await.unwrap());
    // Reading a change topic checks the source table, and refuses a row
    // filter it can't apply.
    m.filtered = true;
    let e = handle("SELECT * FROM `orders.changes`", &cx(&state), &mut m)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::RlsUnsupported));
    m.filtered = false;
    let e = handle("INSERT INTO `orders.changes` (key, value) VALUES (1, 2)", &cx(&state), &mut m)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::StreamAppendOnly));
    m.change_topic = false;
    assert!(reconcile_change_topic(&q, &cx(&state), &mut m).await.unwrap());
    assert!(matches!(m.committed.last(), Some(StreamsChange::Delete(_))));
}

#[tokio::test]
async fn multi_shard_groups_are_refused_for_now() {
    let empty = StreamsState::default();
    let mut m = Mock::default();
    m.tables.insert(
        "orders".into(),
        TableInfo {
            object: ObjectId(9),
            name: "orders".into(),
            slots: vec![slot(1), slot(2)],
        },
    );
    let e = handle(
        "CREATE STREAM e ATTACHED TO GROUP OF orders",
        &cx(&empty),
        &mut m,
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::StreamKeyNotAttached));
}
