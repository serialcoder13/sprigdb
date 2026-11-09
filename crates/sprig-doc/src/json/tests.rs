use std::str::FromStr;

use bson::spec::BinarySubtype;
use bson::{
    Binary, Bson, DateTime, Decimal128, JavaScriptCodeWithScope, Regex, Timestamp, doc,
    oid::ObjectId,
};

use super::*;

fn round_trip(d: &Document) -> (String, Option<String>) {
    let s = to_stored(d, &RenderOpts::default()).unwrap();
    let back = to_bson(&s.doc, s.types.as_deref()).unwrap();
    assert_eq!(
        bson::to_vec(&back).unwrap(),
        bson::to_vec(d).unwrap(),
        "\n{d:?}\n{back:?}\n{}\n{:?}",
        s.doc,
        s.types
    );
    (s.doc, s.types)
}

#[test]
fn chapter_worked_example() {
    let d = doc! {
        "_id": ObjectId::parse_str("6721c5a4e1b2c3d4e5f60718").unwrap(),
        "status": "paid",
        "qty": 3i64,
        "total": Decimal128::from_str("59.90").unwrap(),
        "created": DateTime::from_millis(ms_from_iso("2026-11-07T10:15:00Z").unwrap()),
        "customer": {"id": 42},
        "items": [{"sku": "A-1", "price": Decimal128::from_str("19.95").unwrap()},
                  {"sku": "B-7", "price": 39.95}],
    };
    let (j, t) = round_trip(&d);
    assert_eq!(
        j,
        r#"{"_id":"6721c5a4e1b2c3d4e5f60718","status":"paid","qty":3,"total":59.90,"created":"2026-11-07T10:15:00.000Z","customer":{"id":42},"items":[{"sku":"A-1","price":19.95},{"sku":"B-7","price":39.95}]}"#
    );
    assert_eq!(
        t.unwrap(),
        r#"{"_id":"objectId","qty":"long","total":"decimal","created":"date","items.0.price":"decimal"}"#
    );
}

#[test]
fn every_bson_type_round_trips() {
    let d = doc! {
        "_id": 7,
        "d": 1.5, "d0": 5.0, "dneg0": -0.0, "dbig": 1e300, "dsmall": 1e-7,
        "nan": f64::NAN, "inf": f64::INFINITY, "ninf": f64::NEG_INFINITY,
        "s": "héllo \"q\" \\ \n\t\u{1}", "o": {"a": {"b": [1, [2, 3]]}},
        "a": [], "e": {}, "bin": Binary{subtype: BinarySubtype::Generic, bytes: vec![0, 255, 7]},
        "bin9": Binary{subtype: BinarySubtype::UserDefined(0x80), bytes: vec![1]},
        "uuid": Binary{subtype: BinarySubtype::Uuid, bytes: (0..16).collect()},
        "oid": ObjectId::new(), "t": true, "f": false,
        "date": DateTime::from_millis(1_762_510_500_123),
        "old": DateTime::from_millis(-62_198_755_200_000), // year 0
        "far": DateTime::from_millis(253_402_300_800_000), // year 10000
        "n": Bson::Null, "re": Regex{pattern: "a/b.*".into(), options: "im".into()},
        "dbp": Bson::try_from(serde_json::json!({"$dbPointer": {"$ref": "db.c", "$id": {"$oid": "6721c5a4e1b2c3d4e5f60718"}}})).unwrap(),
        "js": Bson::JavaScriptCode("function(){}".into()),
        "jsw": JavaScriptCodeWithScope{code: "x".into(), scope: doc!{"y": 1}},
        "i32": i32::MIN, "i64small": 5i64, "i64": i64::MAX, "i64min": i64::MIN,
        "ts": Timestamp{time: 1_700_000_000, increment: 42},
        "sym": Bson::Symbol("sym".into()), "u": Bson::Undefined,
        "min": Bson::MinKey, "max": Bson::MaxKey,
        "dec": Decimal128::from_str("-0.000000000000000000000000000000001").unwrap(),
        "decbig": Decimal128::from_str("1.2345E+6000").unwrap(),
        "decnan": Decimal128::from_str("NaN").unwrap(),
        "decinf": Decimal128::from_str("-Infinity").unwrap(),
        "dec0": Decimal128::from_str("0E-10").unwrap(),
        "dot.ted": {"back\\slash": 1i64},
    };
    let (_, t) = round_trip(&d);
    let t = t.unwrap();
    assert!(t.contains(r#""dot\\.ted.back\\\\slash":"long""#), "{t}");
    assert!(t.contains(r#""bin9":"binData:128""#));
}

#[test]
fn plain_json_has_no_type_map() {
    let (j, t) = round_trip(&doc! {"_id": "k", "a": [1, 2.5, "x", null, true], "b": {"c": 2_147_483_648i64}});
    assert_eq!(t, None);
    assert_eq!(j, r#"{"_id":"k","a":[1,2.5,"x",null,true],"b":{"c":2147483648}}"#);
}

#[test]
fn id_first_and_mariadb_spelling_parses() {
    let s = to_stored(&doc! {"a": 1, "_id": 2}, &RenderOpts::default()).unwrap();
    assert_eq!(s.doc, r#"{"_id":2,"a":1}"#);
    // JSON_SET re-serializes with spaces; numbers keep their digits.
    let back = to_bson(r#"{"_id": 2, "a": 1, "price": 59.90, "q": 3}"#,
        Some(r#"{"price": "decimal", "q": "long"}"#)).unwrap();
    assert_eq!(back.get("price"), Some(&Bson::Decimal128(Decimal128::from_str("59.90").unwrap())));
    assert_eq!(back.get("q"), Some(&Bson::Int64(3)));
    // an entry that no longer fits the value (SQL changed it) is ignored
    let back = to_bson(r#"{"_id": "x", "q": "text"}"#, Some(r#"{"q": "long"}"#)).unwrap();
    assert_eq!(back.get_str("q").unwrap(), "text");
    // \u escapes and surrogate pairs
    let back = to_bson(r#"{"s":"é😀"}"#, None).unwrap();
    assert_eq!(back.get_str("s").unwrap(), "é😀");
}

#[test]
fn limits() {
    let mut d = doc! {"x": 1};
    for _ in 0..40 {
        d = doc! {"x": d};
    }
    d.insert("_id", 1);
    let e = to_stored(&d, &RenderOpts::default()).unwrap_err();
    assert_eq!(e.code, code::SPRIG_DOC_TOO_DEEP);
    assert_eq!(id_key(&Bson::Array(vec![])).unwrap_err().code, code::INVALID_ID_FIELD);
    assert_eq!(id_key(&Bson::String("x".repeat(2000))).unwrap_err().code, code::SPRIG_DOC_ID_TOO_LONG);
    // duplicate names in raw BSON
    let mut raw = bson::RawDocumentBuf::new();
    raw.append("a", 1);
    raw.append("a", 2);
    assert_eq!(check_raw(&raw).unwrap_err().code, code::BAD_VALUE);
    let off = render(&doc! {"_id": ObjectId::new()}, &RenderOpts { type_map: false }).unwrap();
    assert_eq!(off.1, None);
}

#[test]
fn dates() {
    for ms in [0i64, 1_762_510_500_123, -1, 253_402_300_799_999, -30_610_224_000_000] {
        let t = iso_from_ms(ms).unwrap();
        assert_eq!(ms_from_iso(&t), Some(ms), "{t}");
    }
    assert_eq!(ms_from_iso("2026-11-01"), Some(1_793_491_200_000));
    assert_eq!(iso_from_ms(-62_198_755_200_000), None);
}
