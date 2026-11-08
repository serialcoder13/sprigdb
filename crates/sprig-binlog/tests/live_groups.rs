//! Transaction-group shapes: XA, two-phase `ALTER`, DDL, annotations, and
//! (on scratch servers only) compressed and fragmented events.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names
)]

mod common;

use common::{drop_schema, gtid_pos, opts, read_typed, schema_name, source, sql, urls};
use mysql_async::prelude::Queryable;
use sprig_binlog::consts::event_type as et;
use sprig_binlog::{BinlogStream, Event, RowsKind, Value};

/// A compact trace of a group sequence for assertions.
fn trace(events: &[(u8, Event)]) -> Vec<String> {
    events
        .iter()
        .filter_map(|(_, e)| {
            Some(match e {
                Event::Gtid {
                    flags,
                    xa,
                    alter_seq,
                    ..
                } => {
                    let mut s = "GTID".to_owned();
                    if flags.prepared_xa() {
                        s.push_str(" prepared_xa");
                    }
                    if flags.completed_xa() {
                        s.push_str(" completed_xa");
                    }
                    if flags.ddl() {
                        s.push_str(" ddl");
                    }
                    if flags.start_alter() {
                        s.push_str(" start_alter");
                    }
                    if flags.commit_alter() {
                        s.push_str(" commit_alter");
                    }
                    if alter_seq.is_some() {
                        s.push_str(" alter_seq");
                    }
                    if let Some(x) = xa {
                        s.push(' ');
                        s.push_str(&x.to_string());
                    }
                    s
                }
                Event::Rows { kind, rows, .. } => format!("{kind:?}x{}", rows.len()),
                Event::Query { sql, .. } => {
                    let s = sql.split_whitespace().take(2).collect::<Vec<_>>().join(" ");
                    format!("Q {s}")
                }
                Event::Xid(_) => "XID".to_owned(),
                Event::XaPrepare { xid, one_phase } => format!("XA_PREPARE {xid} {one_phase}"),
                Event::Annotate(_) => "ANNOTATE".to_owned(),
                _ => return None,
            })
        })
        .collect()
}

/// The trace of only the test schema's groups: drops other groups (other
/// agents share the servers) by keeping GTID..end spans that mention the
/// schema's table or are XA/ALTER statements on it.
fn own_groups(events: &[(u8, Event)], schema: &str) -> Vec<(u8, Event)> {
    let mut out = Vec::new();
    let mut group: Vec<(u8, Event)> = Vec::new();
    let mut mine = false;
    for (t, e) in events {
        if matches!(e, Event::Gtid { .. }) {
            if mine {
                out.append(&mut group);
            }
            group.clear();
            mine = false;
        }
        match e {
            Event::TableMap(m) if m.schema == schema => mine = true,
            Event::Rows { table, .. } if table.schema == schema => mine = true,
            Event::Query { sql, .. } if sql.contains(schema) || sql.starts_with("XA ") => {
                mine = true;
            }
            _ => {}
        }
        group.push((*t, e.clone()));
    }
    if mine {
        out.append(&mut group);
    }
    out
}

async fn xa_and_alter(url: &str) {
    let schema = schema_name();
    let mut c = sql(url).await;
    c.query_drop(format!("CREATE DATABASE {schema}"))
        .await
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {schema}.t (id INT PRIMARY KEY, v INT)"
    ))
    .await
    .unwrap();
    let from = gtid_pos(&mut c).await;
    let x = &schema[schema.len() - 8..];
    for q in [
        format!("XA START 'p{x}','b',7"),
        format!("INSERT INTO {schema}.t VALUES (1, 1)"),
        format!("XA END 'p{x}','b',7"),
        format!("XA PREPARE 'p{x}','b',7"),
        format!("XA COMMIT 'p{x}','b',7"),
        format!("XA START 'r{x}'"),
        format!("INSERT INTO {schema}.t VALUES (2, 2)"),
        format!("XA END 'r{x}'"),
        format!("XA PREPARE 'r{x}'"),
        format!("XA ROLLBACK 'r{x}'"),
        format!("XA START 'o{x}'"),
        format!("INSERT INTO {schema}.t VALUES (3, 3)"),
        format!("XA END 'o{x}'"),
        format!("XA COMMIT 'o{x}' ONE PHASE"),
        "SET SESSION binlog_alter_two_phase = 1".to_owned(),
        format!("ALTER TABLE {schema}.t ADD COLUMN w INT"),
        "SET SESSION binlog_alter_two_phase = 0".to_owned(),
        format!("UPDATE {schema}.t SET w = 9"),
    ] {
        c.query_drop(q).await.unwrap();
    }
    let to = gtid_pos(&mut c).await;
    let mut o = opts(&schema);
    o.annotate_rows = true;
    let mut s = BinlogStream::connect(&source(url), &from, &o)
        .await
        .unwrap();
    let all = read_typed(&mut s, &to).await;
    let mine = own_groups(&all, &schema);
    let hex = |s: &str| common::hex(s.as_bytes());
    let (p, r, one) = (
        hex(&format!("p{x}")),
        hex(&format!("r{x}")),
        hex(&format!("o{x}")),
    );
    let engine: Option<String> = c
        .query_first(
            "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_VARIABLES \
             WHERE VARIABLE_NAME = 'BINLOG_STORAGE_ENGINE'",
        )
        .await
        .unwrap();
    let innodb_binlog = engine.is_some_and(|e| e.eq_ignore_ascii_case("innodb"));
    let xa_part: Vec<String> = if innodb_binlog {
        // MariaDB 12.3 with the binlog in InnoDB logs a prepared XA
        // transaction only when it commits, as an ordinary group; a rolled
        // back one leaves nothing.
        vec![
            "GTID".to_owned(),
            "ANNOTATE".to_owned(),
            "Insertx1".to_owned(),
            "Q COMMIT".to_owned(),
        ]
    } else {
        vec![
            format!("GTID prepared_xa X'{p}',X'62',7"),
            "ANNOTATE".to_owned(),
            "Insertx1".to_owned(),
            "Q XA END".to_owned(),
            format!("XA_PREPARE X'{p}',X'62',7 false"),
            format!("GTID completed_xa X'{p}',X'62',7"),
            "Q XA COMMIT".to_owned(),
            format!("GTID prepared_xa X'{r}',X'',1"),
            "ANNOTATE".to_owned(),
            "Insertx1".to_owned(),
            "Q XA END".to_owned(),
            format!("XA_PREPARE X'{r}',X'',1 false"),
            format!("GTID completed_xa X'{r}',X'',1"),
            "Q XA ROLLBACK".to_owned(),
        ]
    };
    let mut want = xa_part;
    want.extend(vec![
        // XA COMMIT ... ONE PHASE logs as an ordinary transaction.
        "GTID".to_owned(),
        "ANNOTATE".to_owned(),
        "Insertx1".to_owned(),
        "XID".to_owned(),
        "GTID ddl start_alter".to_owned(),
        "Q ALTER TABLE".to_owned(),
        "GTID ddl commit_alter alter_seq".to_owned(),
        "Q ALTER TABLE".to_owned(),
        "GTID".to_owned(),
        "ANNOTATE".to_owned(),
        "Updatex2".to_owned(),
        "XID".to_owned(),
    ]);
    assert_eq!(trace(&mine), want, "on {url}; one-phase id {one}");
    assert!(s.position().reached(&to));
    drop_schema(url, &schema).await;
}

#[tokio::test]
async fn xa_groups_and_two_phase_alter_on_the_single_node() {
    let Some(u) = urls(common::SINGLE, module_path!()) else {
        return;
    };
    xa_and_alter(&u[0]).await;
}

#[tokio::test]
async fn xa_groups_and_two_phase_alter_on_scratch_servers() {
    let Some(us) = urls(common::SCRATCH, module_path!()) else {
        return;
    };
    for u in us {
        xa_and_alter(&u).await;
    }
}

/// `log_bin_compress` (global) and, on 12.3+, a tiny
/// `binlog_row_event_fragment_threshold`: every compressed rows-event type,
/// a compressed query, and fragments, decoded to the written values.
#[tokio::test]
async fn compressed_and_fragmented_events_on_scratch_servers() {
    let Some(us) = urls(common::SCRATCH, module_path!()) else {
        return;
    };
    for url in us {
        let schema = schema_name();
        let mut c = sql(&url).await;
        let has_fragments: Option<String> = c
            .query_first(
                "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_VARIABLES \
                 WHERE VARIABLE_NAME = 'BINLOG_ROW_EVENT_FRAGMENT_THRESHOLD'",
            )
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE {schema}"))
            .await
            .unwrap();
        c.query_drop(format!(
            "CREATE TABLE {schema}.t (id INT PRIMARY KEY, v LONGTEXT)"
        ))
        .await
        .unwrap();
        let from = gtid_pos(&mut c).await;
        c.query_drop("SET GLOBAL log_bin_compress = ON, GLOBAL log_bin_compress_min_len = 10")
            .await
            .unwrap();
        // Globals apply to new sessions.
        let mut w = sql(&url).await;
        w.query_drop(format!(
            "INSERT INTO {schema}.t VALUES (1, REPEAT('a', 300)), (2, REPEAT('b', 3000))"
        ))
        .await
        .unwrap();
        w.query_drop(format!(
            "UPDATE {schema}.t SET v = REPEAT('c', 500) WHERE id = 1"
        ))
        .await
        .unwrap();
        w.query_drop(format!("DELETE FROM {schema}.t WHERE id = 2"))
            .await
            .unwrap();
        w.query_drop(format!(
            "CREATE TABLE {schema}.long_ddl_statement_to_compress (a INT, b INT, c INT)"
        ))
        .await
        .unwrap();
        drop(w);
        c.query_drop("SET GLOBAL log_bin_compress = OFF")
            .await
            .unwrap();
        let mut fragments = false;
        if has_fragments.is_some() {
            c.query_drop("SET GLOBAL binlog_row_event_fragment_threshold = 1024")
                .await
                .unwrap();
            let mut w = sql(&url).await;
            w.query_drop(format!(
                "INSERT INTO {schema}.t VALUES (3, REPEAT('f', 100000))"
            ))
            .await
            .unwrap();
            drop(w);
            c.query_drop("SET GLOBAL binlog_row_event_fragment_threshold = DEFAULT")
                .await
                .unwrap();
            fragments = true;
        }
        let to = gtid_pos(&mut c).await;
        let mut s = BinlogStream::connect(&source(&url), &from, &opts(&schema))
            .await
            .unwrap();
        let events = read_typed(&mut s, &to).await;
        let types: std::collections::BTreeSet<u8> = events.iter().map(|(t, _)| *t).collect();
        for t in [
            et::WRITE_ROWS_COMPRESSED_V1,
            et::UPDATE_ROWS_COMPRESSED_V1,
            et::DELETE_ROWS_COMPRESSED_V1,
            et::QUERY_COMPRESSED,
        ] {
            assert!(
                types.contains(&t),
                "{url}: no {} event in {types:?}",
                et::name(t)
            );
        }
        let mut seen = Vec::new();
        for (_, e) in &events {
            if let Event::Rows { kind, rows, .. } = e {
                for r in rows {
                    let img = r.after.as_ref().or(r.before.as_ref()).unwrap();
                    let Some(Value::Int(id)) = img.get(0) else {
                        panic!()
                    };
                    let v = img.get(1).unwrap().as_bytes().unwrap();
                    seen.push((*kind, *id, v.len(), v[0]));
                }
            }
        }
        let mut want = vec![
            (RowsKind::Insert, 1, 300, b'a'),
            (RowsKind::Insert, 2, 3000, b'b'),
            (RowsKind::Update, 1, 500, b'c'),
            (RowsKind::Delete, 2, 3000, b'b'),
        ];
        if fragments {
            want.push((RowsKind::Insert, 3, 100_000, b'f'));
        }
        assert_eq!(seen, want, "{url}");
        assert!(events.iter().any(|(_, e)| matches!(e, Event::Query { sql, .. } if sql.contains("long_ddl_statement_to_compress"))));
        eprintln!("{url}: event types {types:?}, fragments {fragments}");
        drop_schema(&url, &schema).await;
    }
}
