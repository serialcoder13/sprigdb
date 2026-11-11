#![allow(clippy::similar_names, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use super::plan::{self, Col, Dialect, Op, OpKind, TableInfo};
use super::{DbOpts, DeleteMode, Mode, Param, SchemaPolicy};
use crate::model::{ChangeOp, Position, SinkRecord};

fn opts(mode: Mode, del: DeleteMode) -> DbOpts {
    DbOpts {
        mode,
        delete_mode: del,
        schema_policy: SchemaPolicy::Strict,
        batch_rows: 500,
        column_map: BTreeMap::new(),
        table: "public.orders".into(),
        primary_key: Vec::new(),
        seen_retention: Duration::from_secs(3600),
        db_name: "shop".into(),
        source_name: "orders".into(),
    }
}

fn col(n: &str, cast: &str) -> Col {
    Col {
        name: n.into(),
        cast: cast.into(),
        json: cast.starts_with("json"),
        binary: cast == "bytea",
    }
}

fn table() -> HashMap<String, Arc<TableInfo>> {
    let t = TableInfo {
        name: "public.orders".into(),
        cols: vec![
            col("id", "integer"),
            col("status", "text"),
            col("doc", "jsonb"),
            col("_sprig_ver", "bytea"),
            col("_sprig_deleted", "boolean"),
            col("_sprig_op", "character varying"),
            col("_sprig_ts", "bigint"),
            col("_sprig_idem", "character varying"),
        ],
        pk: vec!["id".into()],
    };
    HashMap::from([("public.orders".to_owned(), Arc::new(t))])
}

fn change(
    op: ChangeOp,
    seq: u64,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
) -> SinkRecord {
    SinkRecord {
        key: Some(br#"{"id":1}"#.to_vec()),
        value: None,
        headers: Vec::new(),
        op: Some(op),
        before,
        after,
        ts_ms: 1000,
        idem_key: format!("c/7/0-1-{seq}/0"),
        pos: Position::Change {
            event: format!("0-1-{seq}"),
            token: String::new(),
            shard: 0,
            seq,
        },
    }
}

#[test]
fn versions_order_as_bytes() {
    let a = change(ChangeOp::Update, 5, None, Some(json!({"id": 1})));
    let b = change(ChangeOp::Update, 6, None, Some(json!({"id": 1})));
    assert!(plan::version_of(&a) < plan::version_of(&b));
    let s = |o| SinkRecord {
        pos: Position::Stream {
            stream: 1,
            partition: 0,
            offset: o,
            epoch: 0,
        },
        ..a.clone()
    };
    assert!(plan::version_of(&s(255)) < plan::version_of(&s(256)));
}

#[test]
fn upserts_guard_versions_in_both_dialects() {
    let o = opts(Mode::Upsert, DeleteMode::Delete);
    let recs = vec![
        change(
            ChangeOp::Create,
            1,
            None,
            Some(json!({"id": 1, "status": "new", "doc": {"a": 1}})),
        ),
        change(
            ChangeOp::Update,
            2,
            None,
            Some(json!({"id": 1, "status": "paid", "doc": "x"})),
        ),
        change(
            ChangeOp::Create,
            3,
            None,
            Some(json!({"id": 2, "status": "new", "doc": null})),
        ),
    ];
    let ops = plan::plan(&recs, &o, Dialect::Postgres).unwrap();
    let st = plan::statements(Dialect::Postgres, &ops, &table(), &o).unwrap();
    assert_eq!(st.len(), 1);
    assert_eq!(
        st[0].sql,
        "INSERT INTO \"public\".\"orders\" AS d (\"doc\", \"id\", \"status\", \"_sprig_ver\") VALUES \
         (CAST($1 AS jsonb), CAST($2 AS integer), CAST($3 AS text), $4), \
         (CAST($5 AS jsonb), CAST($6 AS integer), CAST($7 AS text), $8) \
         ON CONFLICT (\"id\") DO UPDATE SET \"doc\" = EXCLUDED.\"doc\", \"status\" = EXCLUDED.\"status\", \
         \"_sprig_ver\" = EXCLUDED.\"_sprig_ver\" WHERE d.\"_sprig_ver\" IS NULL OR d.\"_sprig_ver\" < EXCLUDED.\"_sprig_ver\""
    );
    // Key 1 twice in one statement: only the later (newer) row stays.
    assert_eq!(st[0].params[2], Param::Text("paid".into()));
    assert_eq!(st[0].params[0], Param::Text("\"x\"".into()));
    assert_eq!(st[0].params[4], Param::Null);
    let st = plan::statements(Dialect::Mysql, &ops, &table(), &o).unwrap();
    assert_eq!(
        st[0].sql,
        "INSERT INTO `public`.`orders` (`doc`, `id`, `status`, `_sprig_ver`) VALUES (?, ?, ?, ?), (?, ?, ?, ?) \
         ON DUPLICATE KEY UPDATE \
         `doc` = IF((`_sprig_ver` IS NULL OR VALUES(`_sprig_ver`) > `_sprig_ver`), VALUES(`doc`), `doc`), \
         `status` = IF((`_sprig_ver` IS NULL OR VALUES(`_sprig_ver`) > `_sprig_ver`), VALUES(`status`), `status`), \
         `_sprig_ver` = IF((`_sprig_ver` IS NULL OR VALUES(`_sprig_ver`) > `_sprig_ver`), VALUES(`_sprig_ver`), `_sprig_ver`)"
    );
}

#[test]
fn deletes_key_changes_and_soft_deletes() {
    let o = opts(Mode::Upsert, DeleteMode::Delete);
    let recs = vec![
        change(
            ChangeOp::Delete,
            4,
            Some(json!({"id": 1, "status": "x"})),
            None,
        ),
        change(
            ChangeOp::Update,
            5,
            Some(json!({"id": 2, "status": "a"})),
            Some(json!({"id": 3, "status": "a"})),
        ),
    ];
    let ops = plan::plan(&recs, &o, Dialect::Mysql).unwrap();
    let st = plan::statements(Dialect::Mysql, &ops, &table(), &o).unwrap();
    // Delete 1 and the old key 2 share a statement; then the upsert of 3.
    assert_eq!(st.len(), 2);
    assert_eq!(
        st[0].sql,
        "DELETE FROM `public`.`orders` WHERE (`id` = ? AND (`_sprig_ver` IS NULL OR `_sprig_ver` <= ?)) \
         OR (`id` = ? AND (`_sprig_ver` IS NULL OR `_sprig_ver` <= ?))"
    );
    assert_eq!(st[0].params[2], Param::Text("2".into()));
    assert!(
        st[1]
            .sql
            .starts_with("INSERT INTO `public`.`orders` (`id`, `status`, `_sprig_ver`)")
    );
    let o = opts(Mode::Upsert, DeleteMode::Soft);
    let ops = plan::plan(&recs[..1], &o, Dialect::Postgres).unwrap();
    assert_eq!(ops[0].kind, OpKind::Upsert);
    let st = plan::statements(Dialect::Postgres, &ops, &table(), &o).unwrap();
    assert!(
        st[0]
            .sql
            .contains("\"_sprig_deleted\" = EXCLUDED.\"_sprig_deleted\""),
        "{}",
        st[0].sql
    );
    assert!(st[0].params.contains(&Param::Text("true".into())));
    let o = opts(Mode::Upsert, DeleteMode::Ignore);
    assert!(
        plan::plan(&recs[..1], &o, Dialect::Postgres)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn appends_drop_duplicates() {
    let o = opts(Mode::Append, DeleteMode::Delete);
    let recs = vec![change(
        ChangeOp::Create,
        1,
        None,
        Some(json!({"id": 1, "status": "new"})),
    )];
    let ops = plan::plan(&recs, &o, Dialect::Postgres).unwrap();
    let st = plan::statements(Dialect::Postgres, &ops, &table(), &o).unwrap();
    assert_eq!(
        st[0].sql,
        "INSERT INTO \"public\".\"orders\" (\"id\", \"status\", \"_sprig_op\", \"_sprig_ts\", \"_sprig_idem\") VALUES \
         (CAST($1 AS integer), CAST($2 AS text), CAST($3 AS character varying), CAST($4 AS bigint), \
         CAST($5 AS character varying)) ON CONFLICT DO NOTHING"
    );
    let st = plan::statements(Dialect::Mysql, &ops, &table(), &o).unwrap();
    assert!(
        st[0]
            .sql
            .ends_with("ON DUPLICATE KEY UPDATE `_sprig_idem` = `_sprig_idem`")
    );
    assert_eq!(st[0].params[4], Param::Text("c/7/0-1-1/0".into()));
}

#[test]
fn stream_values_tombstones_and_column_maps() {
    let mut o = opts(Mode::Upsert, DeleteMode::Delete);
    o.column_map.insert("order_id".into(), "id".into());
    let r = SinkRecord {
        key: Some(br#"{"order_id":9}"#.to_vec()),
        value: Some(br#"{"order_id":9,"status":"x"}"#.to_vec()),
        headers: Vec::new(),
        op: None,
        before: None,
        after: None,
        ts_ms: 0,
        idem_key: "s/1/0/4/0".into(),
        pos: Position::Stream {
            stream: 1,
            partition: 0,
            offset: 4,
            epoch: 0,
        },
    };
    let ops = plan::plan(std::slice::from_ref(&r), &o, Dialect::Mysql).unwrap();
    assert_eq!(ops[0].cols[0].0, "id");
    assert_eq!(ops[0].key_cols, vec!["id".to_owned()]);
    let tomb = SinkRecord {
        value: None,
        ..r.clone()
    };
    let ops = plan::plan(&[tomb], &o, Dialect::Mysql).unwrap();
    assert_eq!(ops[0].kind, OpKind::Delete);
    // A plain value is a key and a value column.
    let plain = SinkRecord {
        key: Some(b"k1".to_vec()),
        value: Some(b"hello".to_vec()),
        ..r
    };
    let ops = plan::plan(&[plain], &o, Dialect::Mysql).unwrap();
    assert_eq!(
        ops[0].cols,
        vec![
            ("key".into(), json!("k1")),
            ("value".into(), json!("hello"))
        ]
    );
}

#[test]
fn creates_and_eo_tables() {
    let o = opts(Mode::Upsert, DeleteMode::Soft);
    let recs = vec![change(
        ChangeOp::Create,
        1,
        None,
        Some(json!({"id": 1, "n": 2.5, "doc": {}, "ok": true})),
    )];
    let ops = plan::plan(&recs, &o, Dialect::Postgres).unwrap();
    let refs: Vec<&Op> = ops.iter().collect();
    let cols = plan::needed_columns(&refs, &o);
    let pk = plan::create_key(&refs, &o);
    assert_eq!(pk, vec!["id".to_owned()]);
    assert_eq!(
        plan::create_table_sql(Dialect::Postgres, "public.t", &cols, &pk, &o).unwrap(),
        "CREATE TABLE IF NOT EXISTS \"public\".\"t\" (\"doc\" JSONB, \"id\" BIGINT NOT NULL, \
         \"n\" DOUBLE PRECISION, \"ok\" BOOLEAN, \"_sprig_deleted\" BOOLEAN NOT NULL DEFAULT FALSE, \
         \"_sprig_ver\" BYTEA, PRIMARY KEY (\"id\"))"
    );
    assert!(plan::create_table_sql(Dialect::Mysql, "t", &cols, &[], &o).is_err());
    assert!(plan::offsets_ddl(Dialect::Mysql).contains("VARBINARY(2048)"));
    let (sql, p) = plan::offsets_update_sql(
        Dialect::Postgres,
        "1/2/0/5",
        &Position::Stream {
            stream: 1,
            partition: 5,
            offset: 9,
            epoch: 0,
        },
        3,
    );
    assert_eq!(
        sql,
        "UPDATE __sprig_sink_offsets SET position = $1, epoch = $2, updated_at = now() WHERE sink_key = $3 AND epoch <= $4"
    );
    assert_eq!(p.len(), 4);
    let (sql, _) = plan::seen_lock_sql(Dialect::Mysql, "1/2", &["a".into(), "b".into()]);
    assert_eq!(
        sql,
        "SELECT idem_key FROM __sprig_sink_seen WHERE sink_key = ? AND idem_key IN (?, ?) FOR UPDATE"
    );
    assert!(
        plan::purge_seen_sql(Dialect::Mysql, "1/2'; DROP", Duration::from_secs(60))
            .contains("'1/2'")
    );
}

/// R9 (r9more): a fan-in sink's rows carry their member in
/// `_sprig_tenant`, which leads the key of a table the sink creates.
#[test]
fn fan_in_rows_carry_their_member() {
    let member = 252_530_600_004u64;
    let packed = crate::source::FANIN_SLOT_FLAG | 3;
    let mut r = SinkRecord {
        key: Some(br#"{"id": 7}"#.to_vec()),
        value: Some(br#"{"id": 7, "name": "a"}"#.to_vec()),
        headers: Vec::new(),
        op: None,
        before: None,
        after: None,
        ts_ms: 1000,
        idem_key: format!("{member}:s/3/0/5/1"),
        pos: Position::Stream {
            stream: packed,
            partition: 0,
            offset: 5,
            epoch: 1,
        },
    };
    let o = opts(Mode::Upsert, DeleteMode::Delete);
    let ops = plan::plan(std::slice::from_ref(&r), &o, Dialect::Mysql).unwrap();
    assert_eq!(ops[0].cols[0], ("_sprig_tenant".to_owned(), json!(member)));
    assert_eq!(ops[0].key_cols[0], "_sprig_tenant");
    // The sink's own tenant's records don't get the column.
    r.idem_key = "s/3/0/5/1".into();
    r.pos = Position::Stream {
        stream: 3,
        partition: 0,
        offset: 5,
        epoch: 1,
    };
    let ops = plan::plan(&[r], &o, Dialect::Mysql).unwrap();
    assert!(ops[0].cols.iter().all(|(c, _)| c != "_sprig_tenant"));
}
