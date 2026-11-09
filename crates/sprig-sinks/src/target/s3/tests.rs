//! Unit tests of the S3 target's pieces.

#![allow(
    clippy::similar_names,
    clippy::case_sensitive_file_extension_comparisons
)]

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use serde_json::json;

use super::format::{ColType, Compress, Format, encode, infer_columns, jsonl_line, parse_columns};
use super::*;
use crate::model::{ChangeOp, Position, SinkRecord, stream_key};

fn rec(p: u32, o: u64, value: &str, ts: i64) -> SinkRecord {
    SinkRecord {
        key: Some(format!("k{o}").into_bytes()),
        value: Some(value.as_bytes().to_vec()),
        headers: Vec::new(),
        op: None,
        before: None,
        after: None,
        ts_ms: ts,
        idem_key: stream_key(9, p, o, 1),
        pos: Position::Stream {
            stream: 9,
            partition: p,
            offset: o,
            epoch: 1,
        },
    }
}

fn change(o: u64, op: ChangeOp, after: serde_json::Value) -> SinkRecord {
    let mut r = rec(0, o, "", 0);
    r.value = None;
    r.op = Some(op);
    if op == ChangeOp::Delete {
        r.before = Some(after);
    } else {
        r.after = Some(after);
    }
    r
}

#[test]
fn templates_render_from_event_time() {
    let segs =
        parse_template("orders/{db}/{yyyy}/{MM}/{dd}/{HH}{mm}/p{partition}/{op}/h{key_hash}/")
            .unwrap();
    assert_eq!(template_root(&segs), "orders/");
    let cx = RenderCtx {
        db: "shop".into(),
        source: "order_events".into(),
        tenant: 7,
        key_buckets: 4,
    };
    // 2025-11-09T10:05:00Z
    let r = rec(3, 12, "{}", 1_762_682_700_000);
    let s = render(&segs, &r, &cx);
    assert!(
        s.starts_with("orders/shop/2025/11/09/1005/p3/event/h"),
        "{s}"
    );
    assert_eq!(render(&segs, &r, &cx), s, "stable");
    assert_eq!(template_root(&parse_template("{tenant}/x/").unwrap()), "");
    assert_eq!(template_root(&parse_template("a/b-{yyyy}").unwrap()), "a/");
    assert!(parse_template("x/{nope}/").is_err());
    assert!(parse_template("x/{yyyy").is_err());
    let t = parse_template("t={table}/{tenant}/{source}/").unwrap();
    let mut r2 = r.clone();
    r2.headers.push(("sprig.table".into(), b"orders".to_vec()));
    assert_eq!(render(&t, &r2, &cx), "t=orders/7/order_events/");
    assert_eq!(render(&t, &r, &cx), "t=order_events/7/order_events/");
}

#[test]
fn names_are_deterministic_and_sort() {
    let a = rec(5, 1205, "{}", 0);
    let b = rec(5, 1730, "{}", 0);
    let n = object_name("orders/", 88, 5, &[&a, &b], "jsonl.gz");
    assert_eq!(
        n,
        "orders/88-5-p5-00000000000000001205-00000000000000001730.jsonl.gz"
    );
    let mut q = a.clone();
    q.pos = Position::Queue {
        queue: 1,
        msg_id: "m1".into(),
        delivery: "d".into(),
    };
    let n1 = object_name("", 1, 0, &[&q], "csv");
    assert!(n1.starts_with("1-0-q") && n1.ends_with(".csv"));
    assert_eq!(object_name("", 1, 0, &[&q], "csv"), n1);
    assert_eq!(
        manifest_key("orders/", 88, 5, 2, 17),
        "orders/_sprig/88/5/u2/00000000000000000017.json"
    );
    assert_eq!(part_ranges(10, 4), vec![0..4, 4..8, 8..10]);
    assert_eq!(part_ranges(8, 4), vec![0..4, 4..8]);
    assert!(part_ranges(0, 4).is_empty());
}

#[test]
fn jsonl_and_csv() {
    let a = rec(0, 1, r#"{"id":1,"s":"a,b \"q\""}"#, 5);
    let line = jsonl_line(&a);
    assert_eq!(line["id"], 1);
    assert_eq!(line["_sprig"]["key"], "s/9/0/1/1");
    assert_eq!(line["_sprig"]["ts"], 5);
    let plain = jsonl_line(&rec(0, 2, "hello", 0));
    assert_eq!(plain["value"], "hello");
    let e = encode(Format::Jsonl, Compress::None, None, &[&a, &a]).unwrap();
    assert_eq!(String::from_utf8(e.bytes).unwrap().lines().count(), 2);
    let cols = parse_columns("id:int64, s, deep:int32=$.x.y").unwrap();
    assert_eq!(cols[2].path, vec!["x", "y"]);
    let bad = rec(0, 3, r#"{"id":"nope"}"#, 0);
    let e = encode(Format::Csv, Compress::None, Some(&cols), &[&a, &bad]).unwrap();
    let text = String::from_utf8(e.bytes).unwrap();
    assert_eq!(
        text,
        "_sprig_key,_sprig_op,_sprig_ts,id,s,deep\r\ns/9/0/1/1,,5,1,\"a,b \"\"q\"\"\",\r\n"
    );
    assert_eq!(e.failed.len(), 1);
    assert_eq!(e.failed[0].0, 1);
    assert!(parse_columns("").is_err());
    assert!(parse_columns("a:weird").is_err());
    assert!(parse_columns("a,a").is_err());
    assert!(encode(Format::Parquet, Compress::None, None, &[&a]).is_err());
    // Compressed bodies are deterministic (a resend makes the same bytes).
    let g1 = encode(Format::Jsonl, Compress::Gzip, None, &[&a])
        .unwrap()
        .bytes;
    let g2 = encode(Format::Jsonl, Compress::Gzip, None, &[&a])
        .unwrap()
        .bytes;
    assert_eq!(g1, g2);
    let z = encode(Format::Jsonl, Compress::Zstd, None, &[&a])
        .unwrap()
        .bytes;
    assert_eq!(
        zstd::decode_all(&z[..]).unwrap(),
        encode(Format::Jsonl, Compress::None, None, &[&a])
            .unwrap()
            .bytes
    );
}

#[test]
fn parquet_round_trips() {
    let recs = [
        change(
            1,
            ChangeOp::Create,
            json!({"id": 1, "name": "a", "amount": 2.5, "ok": true, "tags": [1]}),
        ),
        change(
            2,
            ChangeOp::Update,
            json!({"id": 2, "name": null, "amount": 3, "ok": false}),
        ),
        change(3, ChangeOp::Delete, json!({"id": 3, "name": "c"})),
    ];
    let refs: Vec<&SinkRecord> = recs.iter().collect();
    let cols = infer_columns(&refs);
    let names: Vec<(&str, ColType)> = cols.iter().map(|c| (c.name.as_str(), c.ty)).collect();
    assert!(names.contains(&("id", ColType::Int64)));
    assert!(names.contains(&("amount", ColType::Double)));
    assert!(names.contains(&("tags", ColType::Json)));
    let e = encode(Format::Parquet, Compress::None, Some(&cols), &refs).unwrap();
    assert!(e.failed.is_empty());
    let rd = SerializedFileReader::new(bytes::Bytes::from(e.bytes)).unwrap();
    assert_eq!(rd.metadata().num_row_groups(), 1);
    let rows: Vec<_> = rd.get_row_iter(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(rows.len(), 3);
    let first: Vec<(String, Field)> = rows[0]
        .get_column_iter()
        .map(|(n, f)| (n.clone(), f.clone()))
        .collect();
    assert!(
        first
            .iter()
            .any(|(n, f)| n == "_sprig_op" && *f == Field::Str("c".into()))
    );
    assert!(first.iter().any(|(n, f)| n == "id" && *f == Field::Long(1)));
    assert!(
        first
            .iter()
            .any(|(n, f)| n == "amount" && *f == Field::Double(2.5))
    );
    let second: Vec<(String, Field)> = rows[1]
        .get_column_iter()
        .map(|(n, f)| (n.clone(), f.clone()))
        .collect();
    assert!(second.iter().any(|(n, f)| n == "name" && *f == Field::Null));
    let third: Vec<(String, Field)> = rows[2]
        .get_column_iter()
        .map(|(n, f)| (n.clone(), f.clone()))
        .collect();
    assert!(
        third
            .iter()
            .any(|(n, f)| n == "name" && *f == Field::Str("c".into()))
    );
    // A value that doesn't fit its column is left out, the rest written.
    let cols = parse_columns("id:int64").unwrap();
    let a = rec(0, 1, r#"{"id": 5}"#, 0);
    let b = rec(0, 2, r#"{"id": "x"}"#, 0);
    let e = encode(Format::Parquet, Compress::None, Some(&cols), &[&a, &b]).unwrap();
    assert_eq!((e.records, e.failed.len()), (1, 1));
}

#[test]
fn settings_follow_file_limits() {
    let mut s = RunSettings::default();
    let mut o = Opts::default();
    o.0.insert("s3.file_max_records".into(), "1000".into());
    o.0.insert("s3.file_max_age_seconds".into(), "60".into());
    run_settings_override(&o, &mut s).unwrap();
    assert_eq!(s.batch_max_records, 1000);
    assert_eq!(s.batch_max_bytes, 128 << 20);
    assert_eq!(s.batch_max_wait, Duration::from_secs(60));
    assert!(s.max_inflight_bytes >= 256 << 20);
}
