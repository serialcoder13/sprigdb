//! The captured binlog corpus (`tests/fixtures`, made by `capture.sh` from
//! `corpus.sql`) read in file mode: every event decodes, every column type
//! decodes to the value written, and compressed, fragmented, XA and
//! two-phase `ALTER` groups have the expected shape. No server needed.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names
)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use sprig_binlog::consts::event_type as et;
use sprig_binlog::value::{inet4_text, inet6_text, uuid_text, vector_values};
use sprig_binlog::{BinlogFile, Event, RowsKind, TableMap, Value};
use sprig_common::SprigCode;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// Every event of a file, with its type code.
async fn read_all(name: &str) -> Vec<(u8, Event)> {
    let mut f = BinlogFile::open(fixture(name)).await.unwrap();
    let mut out = Vec::new();
    while let Some(ev) = f.next().await.unwrap() {
        out.push((f.last_header().type_code, ev));
    }
    out
}

fn get<'a>(t: &TableMap, img: &'a sprig_binlog::RowImage, col: &str) -> &'a Value {
    img.get(t.column_index(col).unwrap()).unwrap()
}

fn text(v: &Value) -> &str {
    v.as_str().unwrap()
}

async fn check(name: &str, fragments: bool) {
    let events = read_all(name).await;
    let mut counts: BTreeMap<u8, usize> = BTreeMap::new();
    for (t, _) in &events {
        *counts.entry(*t).or_default() += 1;
    }
    let mut want = vec![
        et::FORMAT_DESCRIPTION,
        et::GTID_LIST,
        et::GTID,
        et::QUERY,
        et::ANNOTATE_ROWS,
        et::TABLE_MAP,
        et::WRITE_ROWS_V1,
        et::UPDATE_ROWS_V1,
        et::DELETE_ROWS_V1,
        et::XID,
        et::XA_PREPARE_LOG,
        et::QUERY_COMPRESSED,
        et::WRITE_ROWS_COMPRESSED_V1,
        et::UPDATE_ROWS_COMPRESSED_V1,
        et::DELETE_ROWS_COMPRESSED_V1,
        et::ROTATE,
    ];
    if fragments {
        want.push(et::PARTIAL_ROW_DATA);
    }
    for t in want {
        assert!(counts.contains_key(&t), "{name}: no {} event", et::name(t));
    }

    // The every-type row, its update and the all-NULL row's delete.
    let rows: Vec<_> = events
        .iter()
        .filter_map(|(_, e)| match e {
            Event::Rows {
                table, kind, rows, ..
            } if table.table == "t" => Some((table.clone(), *kind, rows.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 4, "{name}");
    let (t, kind, r) = &rows[0];
    assert_eq!(*kind, RowsKind::Insert);
    let a = r[0].after.as_ref().unwrap();
    assert_eq!(get(t, a, "ti"), &Value::Int(-128));
    assert_eq!(get(t, a, "biu"), &Value::UInt(u64::MAX));
    assert_eq!(get(t, a, "mi"), &Value::Int(-8_388_608));
    assert_eq!(get(t, a, "f"), &Value::Float(1.5));
    assert_eq!(get(t, a, "d"), &Value::Double(-2.25));
    assert_eq!(
        get(t, a, "dec65"),
        &Value::Decimal(
            "-12345678901234567890123456789012345.123456789012345678901234567890".into()
        )
    );
    assert_eq!(get(t, a, "dec10"), &Value::Decimal("-0.05".into()));
    assert_eq!(get(t, a, "b13"), &Value::Bit(0b1_0101_0101_0101));
    assert_eq!(get(t, a, "y"), &Value::Year(2155));
    let Value::Date(d) = get(t, a, "dt") else {
        panic!()
    };
    assert_eq!(d.to_string(), "2024-02-29");
    let Value::Time(tm) = get(t, a, "tm3") else {
        panic!()
    };
    assert_eq!(tm.to_string(), "-838:59:58.999");
    let Value::DateTime(dtm) = get(t, a, "dtm6") else {
        panic!()
    };
    assert_eq!(dtm.to_string(), "2024-02-29 01:02:03.123456");
    let Value::Timestamp(ts) = get(t, a, "ts") else {
        panic!()
    };
    assert_eq!(
        (ts.secs, ts.to_string().as_str()),
        (2_147_483_647, "2038-01-19 03:14:07.99")
    );
    assert_eq!(text(get(t, a, "c")), "héllo");
    assert_eq!(get(t, a, "bn"), &Value::Bytes(vec![0, 0xff, 0, 0]));
    assert_eq!(text(get(t, a, "vc")), "vär");
    assert_eq!(text(get(t, a, "tx")), "text");
    assert_eq!(get(t, a, "bl"), &Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
    assert_eq!(text(get(t, a, "j")), r#"{"a": [1, 2.5]}"#);
    assert_eq!(get(t, a, "e"), &Value::Enum(3));
    assert_eq!(get(t, a, "s"), &Value::Set(0b1001));
    let Value::Geometry(g) = get(t, a, "g") else {
        panic!()
    };
    assert_eq!(&g[..4], &4326u32.to_le_bytes());
    assert_eq!(g.len(), 4 + 21, "SRID + WKB point");
    let u = get(t, a, "u").as_bytes().unwrap();
    assert_eq!(
        uuid_text(u).unwrap(),
        "12345678-9abc-1def-8123-456789abcdef"
    );
    let i4 = get(t, a, "i4").as_bytes().unwrap();
    assert_eq!(
        inet4_text(i4).unwrap(),
        "10.0.0.0",
        "trailing zero bytes restored"
    );
    assert_eq!(
        inet6_text(get(t, a, "i6").as_bytes().unwrap()).unwrap(),
        "2001:db8::1"
    );
    assert_eq!(
        vector_values(get(t, a, "vec").as_bytes().unwrap()).unwrap(),
        vec![1.5, -2.0]
    );
    assert_eq!(text(get(t, a, "cvc")), "ab".repeat(250));
    assert_eq!(text(get(t, a, "ctx")), "cd".repeat(2000));
    let col = &t.columns[t.column_index("cvc").unwrap()];
    assert_eq!(
        col.type_code,
        sprig_binlog::consts::column_type::VARCHAR_COMPRESSED
    );
    assert_eq!(t.primary_key, vec![(0, 0)]);

    let (_, kind, r) = &rows[1];
    assert_eq!(*kind, RowsKind::Insert);
    let a = r[0].after.as_ref().unwrap();
    assert!(a.0[1..].iter().all(|v| v == &Some(Value::Null)));

    let (t, kind, r) = &rows[2];
    assert_eq!(*kind, RowsKind::Update);
    let (b, a) = (r[0].before.as_ref().unwrap(), r[0].after.as_ref().unwrap());
    assert_eq!(get(t, b, "ti"), &Value::Int(-128));
    assert_eq!(get(t, a, "ti"), &Value::Int(127));
    assert_eq!(get(t, a, "vc"), &Value::Null);
    assert_eq!(rows[3].1, RowsKind::Delete);

    // XA: prepare + commit, prepare + rollback, one phase.
    let shape: Vec<&str> = events
        .iter()
        .filter_map(|(_, e)| match e {
            Event::Gtid { flags, .. } if flags.prepared_xa() => Some("prepared"),
            Event::Gtid { flags, .. } if flags.completed_xa() => Some("completed"),
            Event::XaPrepare {
                one_phase: false, ..
            } => Some("xa_prepare"),
            Event::Query { sql, .. } if sql.starts_with("XA ") => {
                Some(sql.split(' ').nth(1).unwrap_or(""))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        shape,
        [
            "prepared",
            "END",
            "xa_prepare",
            "completed",
            "COMMIT",
            "prepared",
            "END",
            "xa_prepare",
            "completed",
            "ROLLBACK"
        ],
        "{name}"
    );

    // Two-phase ALTER: START ALTER, then COMMIT ALTER naming it.
    let alters: Vec<_> = events
        .iter()
        .filter_map(|(_, e)| match e {
            Event::Gtid {
                gtid,
                flags,
                alter_seq,
                ..
            } if flags.start_alter() || flags.commit_alter() => {
                Some((gtid.seq, flags.start_alter(), *alter_seq))
            }
            _ => None,
        })
        .collect();
    assert_eq!(alters.len(), 2, "{name}");
    assert!(alters[0].1);
    assert_eq!(alters[1].2, Some(alters[0].0));

    // Compressed rows and query events, and (12.3) fragments.
    let z: Vec<(RowsKind, i64, usize)> = events
        .iter()
        .filter_map(|(_, e)| match e {
            Event::Rows {
                table, kind, rows, ..
            } if table.table == "z" => Some(
                rows.iter()
                    .map(|r| {
                        let img = r.after.as_ref().or(r.before.as_ref()).unwrap();
                        let Some(Value::Int(id)) = img.get(0) else {
                            panic!()
                        };
                        (*kind, *id, img.get(1).unwrap().as_bytes().unwrap().len())
                    })
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .flatten()
        .collect();
    // Updates report the after image here.
    let mut want = vec![
        (RowsKind::Insert, 1, 300),
        (RowsKind::Insert, 2, 3000),
        (RowsKind::Update, 1, 500),
        (RowsKind::Delete, 2, 3000),
    ];
    if fragments {
        want.push((RowsKind::Insert, 3, 5000));
    }
    assert_eq!(z, want, "{name}");
    assert!(events.iter().any(|(t, e)| *t == et::QUERY_COMPRESSED
        && matches!(e, Event::Query { sql, .. } if sql.contains("compressed_ddl_with_a_long_statement"))));
}

#[tokio::test]
async fn corpus_mariadb_11_8() {
    check("mariadb-11.8.binlog", false).await;
}

#[tokio::test]
async fn corpus_mariadb_12_3() {
    check("mariadb-12.3.binlog", true).await;
}

#[tokio::test]
async fn positions_follow_complete_groups() {
    let mut f = BinlogFile::open(fixture("mariadb-12.3.binlog"))
        .await
        .unwrap();
    let mut last_gtid = None;
    while let Some(ev) = f.next().await.unwrap() {
        if let Event::Gtid { gtid, .. } = ev {
            last_gtid = Some(gtid);
        }
    }
    assert_eq!(f.position().get(9).copied(), last_gtid);
}

#[tokio::test]
async fn damaged_files_fail_cleanly() {
    let data = std::fs::read(fixture("mariadb-11.8.binlog")).unwrap();
    // A flipped byte inside an event fails its checksum.
    let mut bad = data.clone();
    let mid = bad.len() / 2;
    bad[mid] ^= 0x40;
    let mut f = BinlogFile::new(&bad[..]).await.unwrap();
    let err = loop {
        match f.next().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("damage not detected"),
            Err(e) => break e,
        }
    };
    assert_eq!(err.code(), Some(SprigCode::BinlogCorrupt), "{err}");
    // Every truncation either ends cleanly at an event boundary or errors.
    for cut in (5..data.len()).step_by(97) {
        let mut f = BinlogFile::new(&data[..cut]).await.unwrap();
        while let Ok(Some(_)) = f.next().await {}
    }
    assert!(BinlogFile::new(&b"\xfebix"[..]).await.is_err());
}
