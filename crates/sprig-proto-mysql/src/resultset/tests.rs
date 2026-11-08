use bytes::Bytes;
use proptest::prelude::*;

use super::column_type as ct;
use super::*;

fn col(name: &'static [u8], col_type: u8, flags: ColumnFlags) -> ColumnDef {
    ColumnDef {
        schema: Bytes::from_static(b"t42_d1_g1_s1"),
        table: Bytes::from_static(b"o"),
        org_table: Bytes::from_static(b"orders"),
        name: Bytes::from_static(name),
        org_name: Bytes::from_static(name),
        charset: 45,
        length: 11,
        col_type,
        flags: flags.bits(),
        ..ColumnDef::default()
    }
}

#[test]
fn column_def_round_trips() {
    let c = col(
        b"id",
        ct::LONG,
        ColumnFlags::NOT_NULL | ColumnFlags::PRI_KEY,
    );
    let b = c.encode(Caps::PROTOCOL_41);
    assert_eq!(&b[..4], b"\x03def");
    assert_eq!(
        ColumnDef::decode(&b, Caps::PROTOCOL_41).ok(),
        Some(c.clone())
    );

    let mut j = col(b"doc", ct::BLOB, ColumnFlags::BLOB);
    j.ext_type = ColumnDef::ext_metadata(Some(b"json"), None);
    let caps = Caps::MARIADB_CLIENT_EXTENDED_METADATA;
    let d = ColumnDef::decode(&j.encode(caps), caps).expect("decode");
    assert_eq!(d.ext_type_name(), Some(&b"json"[..]));
    assert_eq!(d.ext_format_name(), None);
    assert_eq!(d, j);
    // A client without the capability gets no block.
    let d = ColumnDef::decode(&j.encode(Caps::empty()), Caps::empty()).expect("decode");
    assert_eq!(d.ext_type, None);
    // A plain column for a client with the capability: empty block.
    let d = ColumnDef::decode(&c.encode(caps), caps).expect("decode");
    assert_eq!(d, c);
}

#[test]
fn text_rows() {
    let row = encode_text_row([Some(&b"1"[..]), None, Some(&b""[..])]);
    assert_eq!(&row[..], &[1, b'1', 0xFB, 0]);
    let v = decode_text_row(&row, 3).expect("decode");
    assert_eq!(v, vec![Some(&b"1"[..]), None, Some(&b""[..])]);
    assert!(decode_text_row(&row, 2).is_err());
    assert!(decode_text_row(&row, 4).is_err());
}

#[test]
fn binary_row_null_bitmap_has_offset_2() {
    let types = [ct::LONG, ct::VAR_STRING];
    let b = encode_binary_row_types(
        &[Value::Null, Value::Bytes(Bytes::from_static(b"x"))],
        &types,
    )
    .expect("encode");
    // Header, bitmap with bit 2 set (column 0), then the one value.
    assert_eq!(&b[..], &[0x00, 0b0000_0100, 1, b'x']);
}

#[test]
fn every_type_round_trips_in_a_binary_row() {
    let cols = vec![
        col(b"t", ct::TINY, ColumnFlags::empty()),
        col(b"tu", ct::TINY, ColumnFlags::UNSIGNED),
        col(b"s", ct::SHORT, ColumnFlags::empty()),
        col(b"y", ct::YEAR, ColumnFlags::UNSIGNED),
        col(b"i", ct::LONG, ColumnFlags::empty()),
        col(b"m", ct::INT24, ColumnFlags::UNSIGNED),
        col(b"b", ct::LONGLONG, ColumnFlags::empty()),
        col(b"bu", ct::LONGLONG, ColumnFlags::UNSIGNED),
        col(b"f", ct::FLOAT, ColumnFlags::empty()),
        col(b"d", ct::DOUBLE, ColumnFlags::empty()),
        col(b"dec", ct::NEWDECIMAL, ColumnFlags::empty()),
        col(b"dt", ct::DATETIME, ColumnFlags::empty()),
        col(b"date", ct::DATE, ColumnFlags::empty()),
        col(b"ts", ct::TIMESTAMP, ColumnFlags::empty()),
        col(b"zero", ct::DATETIME, ColumnFlags::empty()),
        col(b"tm", ct::TIME, ColumnFlags::empty()),
        col(b"tm0", ct::TIME, ColumnFlags::empty()),
        col(b"j", ct::JSON, ColumnFlags::empty()),
        col(b"g", ct::GEOMETRY, ColumnFlags::empty()),
        col(b"n", ct::VAR_STRING, ColumnFlags::empty()),
        col(b"bit", ct::BIT, ColumnFlags::UNSIGNED),
    ];
    let values = vec![
        Value::Int(-5),
        Value::UInt(250),
        Value::Int(-30000),
        Value::UInt(2024),
        Value::Int(-2_000_000_000),
        Value::UInt(16_000_000),
        Value::Int(i64::MIN),
        Value::UInt(u64::MAX),
        Value::Float(1.5),
        Value::Double(-2.25),
        Value::Bytes(Bytes::from_static(b"12.50")),
        Value::DateTime(DateTime {
            year: 2026,
            month: 11,
            day: 8,
            hour: 13,
            minute: 5,
            second: 9,
            micro: 123_456,
        }),
        Value::DateTime(DateTime {
            year: 2026,
            month: 1,
            day: 2,
            ..DateTime::default()
        }),
        Value::DateTime(DateTime {
            year: 1999,
            month: 12,
            day: 31,
            hour: 23,
            minute: 59,
            second: 59,
            micro: 0,
        }),
        Value::DateTime(DateTime::default()),
        Value::Time(Time {
            negative: true,
            days: 3,
            hour: 4,
            minute: 5,
            second: 6,
            micro: 7,
        }),
        Value::Time(Time::default()),
        Value::Bytes(Bytes::from_static(br#"{"a":1}"#)),
        Value::Bytes(Bytes::from_static(&[0, 0, 0, 0, 1, 1, 0, 0])),
        Value::Null,
        Value::Bytes(Bytes::from_static(&[0x01, 0x02])),
    ];
    let b = encode_binary_row(&values, &cols).expect("encode");
    assert_eq!(decode_binary_row(&b, &cols).expect("decode"), values);
}

#[test]
fn values_that_dont_fit_their_type_are_errors() {
    let mut w = crate::buf::Writer::new();
    assert!(encode_binary_value(&mut w, &Value::Bytes(Bytes::new()), ct::LONG).is_err());
    assert!(encode_binary_value(&mut w, &Value::Int(1), ct::DATE).is_err());
    // Numbers given for a string type go as text.
    encode_binary_value(&mut w, &Value::Int(-12), ct::VAR_STRING).expect("encode");
    assert_eq!(&w.into_bytes()[..], b"\x03-12");
}

#[test]
fn row_stream_classification() {
    let deprecate = Caps::PROTOCOL_41 | Caps::DEPRECATE_EOF;
    let eof = EofPacket {
        warnings: 0,
        status: StatusFlags::AUTOCOMMIT,
    };
    let b = eof.encode(Caps::PROTOCOL_41);
    assert_eq!(
        classify_row(&b, Caps::PROTOCOL_41).expect("classify"),
        RowPacket::End(Terminator::Eof(eof))
    );
    let term = Terminator::Eof(eof).encode(deprecate);
    assert_eq!(term[0], 0xFE);
    match classify_row(&term, deprecate).expect("classify") {
        RowPacket::End(Terminator::Ok(ok)) => assert_eq!(ok.status, StatusFlags::AUTOCOMMIT),
        other => panic!("{other:?}"),
    }
    let err = crate::error::ErrPacket::new(1317, *b"70100", "Query execution was interrupted");
    assert!(matches!(
        classify_row(&err.encode(deprecate), deprecate),
        Ok(RowPacket::Err(e)) if e.code == 1317
    ));
    assert_eq!(
        classify_row(&Bytes::from_static(b"\x01x"), deprecate).ok(),
        Some(RowPacket::Row)
    );
    // An OK terminator re-encoded for a client without DEPRECATE_EOF.
    let back = Terminator::decode(&term, deprecate)
        .expect("decode")
        .encode(Caps::PROTOCOL_41);
    assert_eq!(back, b);
}

#[test]
fn response_classification() {
    let caps = Caps::PROTOCOL_41;
    let ok = OkPacket::new(StatusFlags::AUTOCOMMIT).encode(caps);
    assert!(matches!(classify_response(&ok, caps), Ok(Response::Ok(_))));
    assert_eq!(
        classify_response(&encode_column_count(3), caps).ok(),
        Some(Response::ResultSet { columns: 3 })
    );
    assert_eq!(
        classify_response(&Bytes::from_static(b"\xfbf.csv"), caps).ok(),
        Some(Response::LocalInfile(Bytes::from_static(b"f.csv")))
    );
    assert!(classify_response(&Bytes::new(), caps).is_err());
}

#[test]
fn prepare_ok_round_trips() {
    let p = PrepareOk {
        stmt_id: 9,
        columns: 2,
        params: 3,
        warnings: 1,
        metadata_follows: None,
    };
    let b = p.encode();
    assert_eq!(b.len(), 12);
    assert_eq!(PrepareOk::decode(&b).ok(), Some(p));
}

proptest! {
    #[test]
    fn prop_ints_round_trip_at_their_width(v in any::<i64>(), u in any::<u64>()) {
        for (t, bits) in [(ct::TINY, 8), (ct::SHORT, 16), (ct::LONG, 32), (ct::LONGLONG, 64)] {
            let types = [(t, false), (t, true)];
            let b = encode_binary_row_types(&[Value::Int(v), Value::UInt(u)], &[t, t]).expect("encode");
            let got = decode_binary_row_types(&b, &types).expect("decode");
            let shift = 64 - bits;
            let want_i = (v << shift) >> shift;
            let want_u = if bits == 64 { u } else { u & ((1u64 << bits) - 1) };
            prop_assert_eq!(&got, &vec![Value::Int(want_i), Value::UInt(want_u)]);
        }
    }

    #[test]
    fn prop_text_rows_round_trip(vals in prop::collection::vec(prop::option::of(prop::collection::vec(any::<u8>(), 0..300)), 0..20)) {
        let row = encode_text_row(vals.iter().map(|v| v.as_deref()));
        let got = decode_text_row(&row, vals.len()).expect("decode");
        let want: Vec<Option<&[u8]>> = vals.iter().map(|v| v.as_deref()).collect();
        prop_assert_eq!(got, want);
    }
}
