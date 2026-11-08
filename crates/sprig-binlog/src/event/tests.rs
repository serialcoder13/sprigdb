//! Decoder unit tests on hand-built events whose layouts were checked
//! against MariaDB 11.8 and 12.3 with `mariadb-binlog --hexdump`. Whole
//! captured binlog files are tested in `tests/corpus.rs`.

use super::*;
use crate::compress::tests::compress_event_payload;
use crate::consts::event_type as et;
use crate::value::Value;

/// Builds an event with a CRC32 trailer.
fn event(t: u8, body: &[u8]) -> Vec<u8> {
    let size = u32::try_from(HEADER_LEN + body.len() + CHECKSUM_LEN).unwrap();
    let mut v = Vec::new();
    v.extend_from_slice(&1_700_000_000u32.to_le_bytes());
    v.push(t);
    v.extend_from_slice(&901u32.to_le_bytes());
    v.extend_from_slice(&size.to_le_bytes());
    v.extend_from_slice(&0u32.to_le_bytes());
    v.extend_from_slice(&0u16.to_le_bytes());
    v.extend_from_slice(body);
    let crc = crc32fast::hash(&v);
    v.extend_from_slice(&crc.to_le_bytes());
    v
}

/// A GTID body: optional fields right after flags2, zero-padded to 19 bytes.
fn gtid_body(seq: u64, domain: u32, flags: u8, tail: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&seq.to_le_bytes());
    b.extend_from_slice(&domain.to_le_bytes());
    b.push(flags);
    b.extend_from_slice(tail);
    if b.len() < 19 {
        b.resize(19, 0);
    }
    b
}

/// A GTID event of an XA PREPARE group as MariaDB 12.3 logged it
/// (`xa prepare 'g1','b1',7`, thread 17): flags 0x4c, the XID, then
/// `flags_extra` 0x11 (extra engines 0xff, thread id).
const XA_GTID_12_3: &[u8] = &[
    0x1c, 0, 0, 0, 0, 0, 0, 0, 0x09, 0, 0, 0, 0x4c, 0x07, 0, 0, 0, 0x02, 0x02, b'g', b'1', b'b',
    b'1', 0x11, 0xff, 0x11, 0, 0, 0,
];

#[test]
fn captured_xa_gtid_event() {
    let mut d = Decoder::new(true);
    let Some(Event::Gtid {
        gtid,
        flags,
        xa: Some(xa),
        thread_id,
        ..
    }) = d.decode(&event(et::GTID, XA_GTID_12_3)).unwrap()
    else {
        panic!()
    };
    assert_eq!(gtid.to_string(), "9-901-28");
    assert!(flags.prepared_xa() && flags.transactional());
    assert_eq!(
        (xa.gtrid.as_slice(), xa.bqual.as_slice(), xa.format_id),
        (&b"g1"[..], &b"b1"[..], 7)
    );
    assert_eq!(thread_id, Some(17));
}

/// The table map of `frag.t (id INT PRIMARY KEY, b LONGBLOB)` (12.3).
const FRAG_T: &[u8] = &[
    0x12, 0, 0, 0, 0, 0, 0x01, 0x00, 0x04, b'f', b'r', b'a', b'g', 0, 0x01, b't', 0, 0x02, 0x03,
    0xfc, 0x01, 0x04, 0x02, 0x01, 0x01, 0x00, 0x02, 0x01, 0x3f, 0x04, 0x05, 0x02, b'i', b'd', 0x01,
    b'b', 0x08, 0x01, 0x00,
];

/// Rows-event body for `frag.t`: `(id, b)` with `b` of `n` bytes of `x`.
fn write_rows_body(id: u32, n: usize) -> (Vec<u8>, Vec<u8>) {
    let head = vec![0x12, 0, 0, 0, 0, 0, 0x01, 0x00, 0x02, 0x03];
    let mut rows = vec![0xfc];
    rows.extend_from_slice(&id.to_le_bytes());
    rows.extend_from_slice(&u32::try_from(n).unwrap().to_le_bytes());
    rows.extend(std::iter::repeat_n(b'x', n));
    (head, rows)
}

fn decoder() -> Decoder {
    let mut d = Decoder::new(true);
    d.decode(&event(et::GTID, &gtid_body(3, 9, 0, &[])))
        .unwrap();
    d.decode(&event(et::TABLE_MAP, FRAG_T)).unwrap();
    d
}

fn expect_frag_row(ev: Option<Event>, id: i64, n: usize) {
    let Some(Event::Rows {
        kind, rows, table, ..
    }) = ev
    else {
        panic!("not a rows event: {ev:?}")
    };
    assert_eq!(kind, RowsKind::Insert);
    assert_eq!(table.table, "t");
    let after = rows[0].after.as_ref().unwrap();
    assert_eq!(after.get(0), Some(&Value::Int(id)));
    assert_eq!(after.get(1).unwrap().as_bytes().unwrap().len(), n);
}

#[test]
fn checksum_mismatch_is_corrupt() {
    let mut raw = event(et::XID, &7u64.to_le_bytes());
    let mut d = Decoder::new(true);
    assert_eq!(d.decode(&raw).unwrap(), Some(Event::Xid(7)));
    raw[20] ^= 1;
    let err = d.decode(&raw).unwrap_err();
    assert_eq!(err.code(), Some(sprig_common::SprigCode::BinlogCorrupt));
    d.set_verify_checksums(false);
    assert!(d.decode(&raw).is_ok());
}

#[test]
fn start_encryption_is_skipped_on_a_dump_and_refused_in_a_file() {
    // Scheme 1, key version 1, a 12-byte nonce.
    let mut body = vec![1, 1, 0, 0, 0];
    body.extend_from_slice(&[0xab; 12]);
    let raw = event(et::START_ENCRYPTION, &body);
    let mut file = Decoder::new(true);
    let err = file.decode(&raw).unwrap_err();
    assert_eq!(err.code(), Some(sprig_common::SprigCode::BinlogUnsupported));
    assert!(err.message.contains("encrypted"), "{}", err.message);
    let mut dump = Decoder::new(true);
    dump.set_server_decrypts(true);
    assert_eq!(dump.decode(&raw).unwrap(), None);
    assert_eq!(dump.last_header().type_code, et::START_ENCRYPTION);
    // The decrypted events after it decode as usual.
    let xid = event(et::XID, &7u64.to_le_bytes());
    assert_eq!(dump.decode(&xid).unwrap(), Some(Event::Xid(7)));
}

#[test]
fn an_open_files_format_description_passes_its_checksum() {
    let mut body = 4u16.to_le_bytes().to_vec();
    let mut ver = b"12.3.0-MariaDB-log".to_vec();
    ver.resize(50, 0);
    body.extend_from_slice(&ver);
    body.extend_from_slice(&0u32.to_le_bytes());
    body.push(19);
    body.extend_from_slice(&[0; 8]); // post-header lengths
    body.push(1); // CRC32
    // The checksum covers the event with LOG_EVENT_BINLOG_IN_USE_F clear;
    // the server sets the flag afterwards on the file it has open.
    let mut raw = event(et::FORMAT_DESCRIPTION, &body);
    raw[17] |= 1;
    let mut d = Decoder::new(false);
    assert!(matches!(
        d.decode(&raw).unwrap(),
        Some(Event::FormatDescription {
            checksums: true,
            ..
        })
    ));
    raw[30] ^= 1;
    let err = Decoder::new(false).decode(&raw).unwrap_err();
    assert_eq!(err.code(), Some(sprig_common::SprigCode::BinlogCorrupt));
}

#[test]
fn gtid_groups_advance_the_position_when_they_end() {
    let mut d = Decoder::new(true);
    let ev = d
        .decode(&event(
            et::GTID,
            &gtid_body(5, 2, gtid_flags::TRANSACTIONAL, &[]),
        ))
        .unwrap()
        .unwrap();
    let Event::Gtid { gtid, flags, .. } = ev else {
        panic!()
    };
    assert_eq!(gtid.to_string(), "2-901-5");
    assert!(flags.transactional() && !flags.standalone());
    assert!(d.position().is_empty());
    assert_eq!(d.open_group(), Some(gtid));
    d.decode(&event(et::XID, &1u64.to_le_bytes())).unwrap();
    assert_eq!(d.position().to_string(), "2-901-5");
    assert_eq!(d.open_group(), None);
}

#[test]
fn xa_gtid_and_prepare_events() {
    // GTID of an XA PREPARE group: formatID, gtrid and bqual lengths, data.
    let mut tail = Vec::new();
    tail.extend_from_slice(&1i32.to_le_bytes());
    tail.extend_from_slice(&[3, 2]);
    tail.extend_from_slice(b"abcxy");
    let body = gtid_body(
        6,
        2,
        gtid_flags::PREPARED_XA | gtid_flags::TRANSACTIONAL,
        &tail,
    );
    let mut d = Decoder::new(true);
    let Some(Event::Gtid {
        xa: Some(xa),
        flags,
        ..
    }) = d.decode(&event(et::GTID, &body)).unwrap()
    else {
        panic!()
    };
    assert!(flags.prepared_xa());
    assert_eq!(xa.to_string(), "X'616263',X'7879',1");
    // XA_PREPARE: one_phase, formatID, gtrid_length u32, bqual_length u32, data.
    let mut p = vec![0];
    p.extend_from_slice(&1i32.to_le_bytes());
    p.extend_from_slice(&3u32.to_le_bytes());
    p.extend_from_slice(&2u32.to_le_bytes());
    p.extend_from_slice(b"abcxy");
    let ev = d.decode(&event(et::XA_PREPARE_LOG, &p)).unwrap().unwrap();
    assert_eq!(
        ev,
        Event::XaPrepare {
            xid: xa,
            one_phase: false
        }
    );
    assert_eq!(d.position().to_string(), "2-901-6");
}

#[test]
fn gtid_extra_flags_carry_the_start_alter_sequence() {
    let mut tail = vec![gtid_flags_extra::COMMIT_ALTER];
    tail.extend_from_slice(&41u64.to_le_bytes());
    let body = gtid_body(42, 0, gtid_flags::STANDALONE | gtid_flags::DDL, &tail);
    let mut d = Decoder::new(true);
    let Some(Event::Gtid {
        flags, alter_seq, ..
    }) = d.decode(&event(et::GTID, &body)).unwrap()
    else {
        panic!()
    };
    assert!(flags.commit_alter() && flags.ddl());
    assert_eq!(alter_seq, Some(41));
}

fn query_body(db: &str, sql: &[u8], status: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&77u32.to_le_bytes());
    b.extend_from_slice(&0u32.to_le_bytes());
    b.push(u8::try_from(db.len()).unwrap());
    b.extend_from_slice(&0u16.to_le_bytes());
    b.extend_from_slice(&u16::try_from(status.len()).unwrap().to_le_bytes());
    b.extend_from_slice(status);
    b.extend_from_slice(db.as_bytes());
    b.push(0);
    b.extend_from_slice(sql);
    b
}

#[test]
fn query_events_plain_and_compressed() {
    let mut status = vec![0, 0, 0, 0, 0, 1];
    status.extend_from_slice(&0x1234u64.to_le_bytes());
    status.push(4);
    status.extend_from_slice(&[45, 0, 45, 0, 8, 0]);
    status.extend_from_slice(&[5, 6]);
    status.extend_from_slice(b"+00:00");
    let mut d = Decoder::new(true);
    d.decode(&event(
        et::GTID,
        &gtid_body(9, 0, gtid_flags::STANDALONE, &[]),
    ))
    .unwrap();
    let ev = d
        .decode(&event(
            et::QUERY,
            &query_body("s1", b"CREATE TABLE t (a INT)", &status),
        ))
        .unwrap()
        .unwrap();
    let Event::Query {
        db,
        sql,
        status,
        thread_id,
        ..
    } = ev
    else {
        panic!()
    };
    assert_eq!(
        (db.as_str(), sql.as_str(), thread_id),
        ("s1", "CREATE TABLE t (a INT)", 77)
    );
    assert_eq!(status.sql_mode, Some(0x1234));
    assert_eq!(status.collation_server, Some(8));
    assert_eq!(status.time_zone.as_deref(), Some("+00:00"));
    // A standalone group ends with its one statement.
    assert_eq!(d.position().to_string(), "0-901-9");

    let long = "INSERT INTO t VALUES ".to_owned() + &"(1),".repeat(100) + "(2)";
    let body = query_body("s1", &compress_event_payload(long.as_bytes()), &[]);
    let Some(Event::Query { sql, .. }) = d.decode(&event(et::QUERY_COMPRESSED, &body)).unwrap()
    else {
        panic!()
    };
    assert_eq!(sql, long);
}

#[test]
fn rows_events_v1_v2_and_compressed() {
    let mut d = decoder();
    let (head, rows) = write_rows_body(1, 5);
    let mut v1 = head.clone();
    v1.extend_from_slice(&rows);
    expect_frag_row(d.decode(&event(et::WRITE_ROWS_V1, &v1)).unwrap(), 1, 5);
    // v2 adds the extra-data length (2: no extra data) after the flags.
    let mut v2 = head[..8].to_vec();
    v2.extend_from_slice(&2u16.to_le_bytes());
    v2.extend_from_slice(&head[8..]);
    v2.extend_from_slice(&rows);
    expect_frag_row(d.decode(&event(et::WRITE_ROWS_V2, &v2)).unwrap(), 1, 5);
    // Compressed v1: only the row data after the bitmaps is compressed.
    let (head, rows) = write_rows_body(2, 600);
    let mut c = head.clone();
    c.extend_from_slice(&compress_event_payload(&rows));
    expect_frag_row(
        d.decode(&event(et::WRITE_ROWS_COMPRESSED_V1, &c)).unwrap(),
        2,
        600,
    );
}

#[test]
fn rows_for_an_unknown_table_or_a_short_row_are_errors() {
    let mut d = Decoder::new(true);
    let (mut head, rows) = write_rows_body(1, 5);
    head.extend_from_slice(&rows);
    assert!(d.decode(&event(et::WRITE_ROWS_V1, &head)).is_err());
    let mut d = decoder();
    head.truncate(head.len() - 2);
    assert!(d.decode(&event(et::WRITE_ROWS_V1, &head)).is_err());
}

#[test]
fn filtered_tables_are_skipped_without_decoding() {
    let mut d = Decoder::new(true);
    d.set_filter(Some(Arc::new(|schema: &str, _: &str| schema != "frag")));
    assert_eq!(d.decode(&event(et::TABLE_MAP, FRAG_T)).unwrap(), None);
    let (mut head, rows) = write_rows_body(1, 5);
    head.extend_from_slice(&rows);
    assert_eq!(d.decode(&event(et::WRITE_ROWS_V1, &head)).unwrap(), None);
}

/// Splits a rows event the way MariaDB 12.3 does with a small
/// `binlog_row_event_fragment_threshold`.
fn fragments(inner_body: &[u8], chunk: usize) -> Vec<Vec<u8>> {
    let mut inner = event(et::WRITE_ROWS_V1, inner_body);
    inner.truncate(inner.len() - CHECKSUM_LEN); // assembled without its CRC
    let parts: Vec<&[u8]> = inner.chunks(chunk).collect();
    let total = u32::try_from(parts.len()).unwrap();
    parts
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let seq = u32::try_from(i + 1).unwrap();
            let mut b = Vec::new();
            b.extend_from_slice(&total.to_le_bytes());
            b.extend_from_slice(&seq.to_le_bytes());
            if i == 0 {
                b.push(1);
                b.extend_from_slice(&(inner.len() as u64).to_le_bytes());
            } else {
                b.push(0);
            }
            b.extend_from_slice(p);
            event(et::PARTIAL_ROW_DATA, &b)
        })
        .collect()
}

#[test]
fn partial_row_fragments_reassemble() {
    let (mut body, rows) = write_rows_body(1, 5000);
    body.extend_from_slice(&rows);
    let frags = fragments(&body, 992);
    assert_eq!(frags.len(), 6);
    let mut d = decoder();
    for f in &frags[..5] {
        assert_eq!(d.decode(f).unwrap(), None);
    }
    expect_frag_row(d.decode(&frags[5]).unwrap(), 1, 5000);
    // Out of order: a later fragment without the first.
    let mut d = decoder();
    assert!(d.decode(&frags[1]).is_err());
}

#[test]
fn gtid_list_and_rotate() {
    let mut b = Vec::new();
    b.extend_from_slice(&2u32.to_le_bytes());
    for (dom, srv, seq) in [(1u32, 101u32, 7u64), (2, 201, 9)] {
        b.extend_from_slice(&dom.to_le_bytes());
        b.extend_from_slice(&srv.to_le_bytes());
        b.extend_from_slice(&seq.to_le_bytes());
    }
    let mut d = Decoder::new(true);
    let Some(Event::GtidList(p)) = d.decode(&event(et::GTID_LIST, &b)).unwrap() else {
        panic!()
    };
    assert_eq!(p.to_string(), "1-101-7,2-201-9");
    let mut r = 4u64.to_le_bytes().to_vec();
    r.extend_from_slice(b"mariadb-bin.000002");
    assert_eq!(
        d.decode(&event(et::ROTATE, &r)).unwrap(),
        Some(Event::Rotate {
            file: "mariadb-bin.000002".into(),
            pos: 4
        })
    );
}

#[test]
fn group_end_statements() {
    for s in [
        "COMMIT",
        "commit",
        "ROLLBACK",
        "XA COMMIT X'01',X'',1",
        "xa rollback 'a'",
    ] {
        assert!(is_group_end(s), "{s}");
    }
    for s in ["BEGIN", "COMMITTED_TABLE", "CREATE TABLE t (a INT)"] {
        assert!(!is_group_end(s), "{s}");
    }
}
