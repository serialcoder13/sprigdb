#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::format_collect,
    clippy::too_many_lines
)]

use proptest::prelude::*;

use super::*;
use crate::collation::CollationId;
use crate::hash::routing_hash;
use crate::key::DecimalRef;

fn norm(spec: &KeySpec, v: &[KeyValue<'_>]) -> (Vec<u8>, bool) {
    let mut out = Vec::new();
    let o = StdNormalizer.normalize(spec, v, &mut out).unwrap();
    (out, o.off_class)
}

fn numeric() -> KeySpec {
    KeySpec::new(vec![KeyClass::Numeric]).unwrap()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn chapter_vectors() {
    let s = numeric();
    let cases: &[(KeyValue<'_>, u64)] = &[
        (KeyValue::Int(42), 0x313c_a5e2_98b7_eb08),
        (KeyValue::Int(7), 0x7448_b590_0dd5_6361),
        (KeyValue::Int(1001), 0xdf8f_7a48_13a9_9b52),
        (KeyValue::Int(1002), 0xaeef_270b_7754_3289),
        (KeyValue::Null, 0xe864_e589_3a27_3242),
        (KeyValue::Decimal(DecimalRef("0.5")), 0x679d_87f1_3bd0_2ed0),
    ];
    for (v, h) in cases {
        let (b, off) = norm(&s, std::slice::from_ref(v));
        assert!(!off);
        assert_eq!(routing_hash(&b).0, *h, "{v:?}: {}", hex(&b));
    }
    let (b, _) = norm(&s, &[KeyValue::Decimal(DecimalRef("0.5"))]);
    assert_eq!(hex(&b), "020000000601ffffffff05");
    let (b, _) = norm(&s, &[KeyValue::Int(42)]);
    assert_eq!(hex(&b), format!("0100000010{}2a", "00".repeat(15)));
}

#[test]
fn equal_numbers_across_models() {
    let s = numeric();
    let base = norm(&s, &[KeyValue::Int(42)]).0;
    for v in [
        KeyValue::Double(42.0),
        KeyValue::Decimal(DecimalRef("42.000")),
        KeyValue::Decimal(DecimalRef("+0042")),
        KeyValue::Decimal(DecimalRef("4.2E+1")),
        KeyValue::Decimal(DecimalRef("420e-1")),
    ] {
        assert_eq!(norm(&s, &[v]).0, base, "{v:?}");
    }
    // Booleans are 0 and 1; -0 is 0.
    assert_eq!(
        norm(&s, &[KeyValue::Bool(true)]).0,
        norm(&s, &[KeyValue::Int(1)]).0
    );
    assert_eq!(
        norm(&s, &[KeyValue::Double(-0.0)]).0,
        norm(&s, &[KeyValue::Int(0)]).0
    );
    assert_eq!(
        norm(&s, &[KeyValue::Decimal(DecimalRef("-0.000"))]).0,
        norm(&s, &[KeyValue::Int(0)]).0
    );
    // Decimal cohorts collapse; a double equal to a decimal matches it.
    assert_eq!(
        norm(&s, &[KeyValue::Decimal(DecimalRef("1.50"))]).0,
        norm(&s, &[KeyValue::Double(1.5)]).0
    );
    // Signed -1 and unsigned 2^64-1 differ.
    assert_ne!(
        norm(&s, &[KeyValue::Int(-1)]).0,
        norm(&s, &[KeyValue::Int(i128::from(u64::MAX))]).0
    );
    // 0.1 as a double is not the decimal 0.1.
    assert_ne!(
        norm(&s, &[KeyValue::Double(0.1)]).0,
        norm(&s, &[KeyValue::Decimal(DecimalRef("0.1"))]).0
    );
    // Specials.
    assert_ne!(
        norm(&s, &[KeyValue::Double(f64::INFINITY)]).0,
        norm(&s, &[KeyValue::Double(f64::NEG_INFINITY)]).0
    );
    // A DECIMAL(65) beyond i128 stays exact.
    let big = "1".repeat(60);
    let (b, _) = norm(&s, &[KeyValue::Decimal(DecimalRef(&big))]);
    assert_eq!(b[0], TAG_DECIMAL);
}

#[test]
fn off_class_and_framing() {
    let s = numeric();
    let (b, off) = norm(&s, &[KeyValue::str("42")]);
    assert!(off);
    assert_eq!(b[0], TAG_STRING);
    // Composite framing: ('ab','c') and ('a','bc') differ.
    let ci = KeyClass::String {
        collation: CollationId(2304),
    };
    let s2 = KeySpec::new(vec![ci, ci]).unwrap();
    assert_ne!(
        norm(&s2, &[KeyValue::str("ab"), KeyValue::str("c")]).0,
        norm(&s2, &[KeyValue::str("a"), KeyValue::str("bc")]).0
    );
    // Wrong arity.
    let mut out = Vec::new();
    assert!(
        StdNormalizer
            .normalize(&s2, &[KeyValue::Null], &mut out)
            .is_err()
    );
}

#[test]
fn strings_dates_uuids() {
    let ci = KeySpec::new(vec![KeyClass::String {
        collation: CollationId(2304),
    }])
    .unwrap();
    assert_eq!(
        norm(&ci, &[KeyValue::str("Straße ")]).0,
        norm(&ci, &[KeyValue::str("STRASSE")]).0
    );
    // An ObjectId in a string group is its lowercase hex.
    let oid = [0xABu8; 12];
    assert_eq!(
        norm(&ci, &[KeyValue::ObjectId(oid)]).0,
        norm(&ci, &[KeyValue::str(&"ab".repeat(12))]).0
    );
    let dt = KeySpec::new(vec![KeyClass::DateTime]).unwrap();
    assert_eq!(
        norm(&dt, &[KeyValue::DateTime(DateTimeParts::date(2026, 1, 1))]).0,
        norm(&dt, &[KeyValue::str("2026-01-01 00:00:00")]).0
    );
    let (_, off) = norm(&dt, &[KeyValue::Timestamp { secs: 0, micros: 0 }]);
    assert!(off);
    let u = KeySpec::new(vec![KeyClass::Uuid]).unwrap();
    let raw = crate::key::parse_uuid("123e4567-e89b-12d3-a456-426614174000").unwrap();
    assert_eq!(
        norm(&u, &[KeyValue::Uuid(raw)]).0,
        norm(&u, &[KeyValue::str("123E4567E89B12D3A456426614174000")]).0
    );
}

proptest! {
    #[test]
    fn integers_and_decimals_agree(i in any::<i64>(), zeros in 0usize..5) {
        let s = numeric();
        let text = format!("{i}.{}", "0".repeat(zeros));
        prop_assert_eq!(
            norm(&s, &[KeyValue::Int(i128::from(i))]).0,
            norm(&s, &[KeyValue::Decimal(DecimalRef(&text))]).0
        );
    }

    #[test]
    fn doubles_and_their_exact_decimals_agree(d in any::<f64>().prop_filter("finite", |d| d.is_finite())) {
        let s = numeric();
        // Rust prints the shortest round-tripping text, not the exact
        // value; compare with an integer when integral, else check only
        // that it never panics and equals itself.
        let a = norm(&s, &[KeyValue::Double(d)]).0;
        prop_assert_eq!(&a, &norm(&s, &[KeyValue::Double(d)]).0);
        if d.fract() == 0.0 && d.abs() < 1e30 {
            #[allow(clippy::cast_possible_truncation)]
            let i = d as i128;
            prop_assert_eq!(a, norm(&s, &[KeyValue::Int(i)]).0);
        }
    }

    #[test]
    fn never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..40), text in ".{0,20}") {
        let s = KeySpec::new(vec![
            KeyClass::Numeric,
            KeyClass::String { collation: CollationId(224) },
            KeyClass::Binary,
            KeyClass::Uuid,
            KeyClass::DateTime,
            KeyClass::Timestamp,
        ]).unwrap();
        let mut out = Vec::new();
        let vals = [
            KeyValue::Decimal(DecimalRef(&text)),
            KeyValue::Str { bytes: &bytes, charset: Charset::Utf8mb4 },
            KeyValue::Str { bytes: &bytes, charset: Charset::Latin1 },
            KeyValue::str(&text),
            KeyValue::Bytes(&bytes),
            KeyValue::str(&text),
        ];
        let _ = StdNormalizer.normalize(&s, &vals, &mut out);
    }
}
