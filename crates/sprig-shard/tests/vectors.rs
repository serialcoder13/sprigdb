//! Golden vectors: `tests/vectors.txt`
//! pins the normalized bytes and `xxh3_64` of fixed values. A changed line
//! fails: routing hashes of stored rows must never change.
//!
//! `SPRIG_REGEN_VECTORS=1 cargo test -p sprig-shard --test vectors` rewrites
//! the file (only when a change is intended and reviewed).

#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use sprig_shard::collation::collation_by_name;
use sprig_shard::key::{DateTimeParts, DecimalRef, KeyClass, KeySpec, KeyValue};
use sprig_shard::normalize::{Normalizer, StdNormalizer};
use sprig_shard::routing_hash;

const CASES: &[&str] = &[
    "numeric\tint:42",
    "numeric\tint:7",
    "numeric\tint:1001",
    "numeric\tint:1002",
    "numeric\tnull",
    "numeric\tdec:0.5",
    "numeric\tdec:42.000",
    "numeric\tdouble:42",
    "numeric\tdouble:0.1",
    "numeric\tint:-1",
    "numeric\tint:18446744073709551615",
    "numeric\tdec:-123456789012345678901234567890123456789012345.6789",
    "string:utf8mb4_uca1400_ai_ci\tstr:Straße",
    "string:utf8mb4_uca1400_ai_ci\tstr:STRASSE   ",
    "string:utf8mb4_uca1400_as_cs\tstr:Ab ",
    "string:utf8mb4_general_ci\tstr:ann@example.com",
    "string:utf8mb4_unicode_ci\tstr:Ångström",
    "string:utf8mb4_unicode_520_ci\tstr:😀",
    "string:utf8mb4_bin\tstr:a b ",
    "string:latin1_swedish_ci\tstr:Åsa",
    "string:utf8mb4_0900_ai_ci\tstr:a ",
    "binary\tbytes:00ff10",
    "uuid\tuuid:123e4567-e89b-12d3-a456-426614174000",
    "datetime\tdate:2026-01-01 00:00:00",
    "timestamp\tts:1700000000.123456",
];

fn class(s: &str) -> KeyClass {
    match s.split_once(':') {
        Some(("string", c)) => KeyClass::String {
            collation: collation_by_name(c).unwrap().id,
        },
        _ => match s {
            "numeric" => KeyClass::Numeric,
            "binary" => KeyClass::Binary,
            "uuid" => KeyClass::Uuid,
            "datetime" => KeyClass::DateTime,
            _ => KeyClass::Timestamp,
        },
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn line(case: &str) -> String {
    let (c, v) = case.split_once('\t').unwrap();
    let spec = KeySpec::new(vec![class(c)]).unwrap();
    let bytes;
    let value = match v.split_once(':') {
        None => KeyValue::Null,
        Some(("int", x)) => KeyValue::Int(x.parse().unwrap()),
        Some(("dec", x)) => KeyValue::Decimal(DecimalRef(x)),
        Some(("double", x)) => KeyValue::Double(x.parse().unwrap()),
        Some(("str", x)) => KeyValue::str(x),
        Some(("bytes", x)) => {
            bytes = (0..x.len() / 2)
                .map(|i| u8::from_str_radix(&x[2 * i..2 * i + 2], 16).unwrap())
                .collect::<Vec<u8>>();
            KeyValue::Bytes(&bytes)
        }
        Some(("uuid", x)) => KeyValue::Uuid(sprig_shard::key::parse_uuid(x).unwrap()),
        Some(("date", x)) => KeyValue::DateTime(DateTimeParts::parse(x).unwrap()),
        Some(("ts", x)) => {
            let (s, m) = x.split_once('.').unwrap();
            KeyValue::Timestamp {
                secs: s.parse().unwrap(),
                micros: m.parse().unwrap(),
            }
        }
        Some(_) => panic!("bad case {case}"),
    };
    let mut out = Vec::new();
    StdNormalizer.normalize(&spec, &[value], &mut out).unwrap();
    format!("{case}\t{}\t{:#018x}", hex(&out), routing_hash(&out).0)
}

#[test]
fn golden_vectors() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors.txt");
    let got: Vec<String> = CASES.iter().map(|c| line(c)).collect();
    if std::env::var("SPRIG_REGEN_VECTORS").as_deref() == Ok("1") {
        let mut text =
            String::from("# class\tvalue\tnormalized bytes\txxh3_64; never edit by hand\n");
        for l in &got {
            text.push_str(l);
            text.push('\n');
        }
        std::fs::write(path, text).unwrap();
    }
    let want: Vec<String> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|l| !l.starts_with('#'))
        .map(str::to_owned)
        .collect();
    assert_eq!(got, want);
}
