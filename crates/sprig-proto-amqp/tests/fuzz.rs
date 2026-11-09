//! Decoder fuzz-style tests: arbitrary and mutated bytes never panic the
//! decoders, and random values round-trip.

#![allow(clippy::unwrap_used, missing_docs)]

use bytes::{Bytes, BytesMut};
use proptest::prelude::*;
use sprig_proto_amqp::v10::message::{Address, Sections};
use sprig_proto_amqp::v10::performative::{Frame as Frame10, Performative};
use sprig_proto_amqp::v10::types::Value;
use sprig_proto_amqp::v091::codec::{Dec, FieldTable, FieldValue};
use sprig_proto_amqp::v091::frame::{ContentHeader, Frame};
use sprig_proto_amqp::v091::method::{
    BasicConsume, BasicPublish, ConnectionStartOk, Method, QueueDeclare,
};
use sprig_proto_amqp::v091::props::BasicProperties;

fn field_value() -> impl Strategy<Value = FieldValue> {
    let leaf = prop_oneof![
        any::<bool>().prop_map(FieldValue::Bool),
        any::<i8>().prop_map(FieldValue::I8),
        any::<u8>().prop_map(FieldValue::U8),
        any::<i16>().prop_map(FieldValue::I16),
        any::<u16>().prop_map(FieldValue::U16),
        any::<i32>().prop_map(FieldValue::I32),
        any::<u32>().prop_map(FieldValue::U32),
        any::<i64>().prop_map(FieldValue::I64),
        (any::<u8>(), any::<u32>()).prop_map(|(a, b)| FieldValue::Decimal(a, b)),
        ".{0,20}".prop_map(|s| FieldValue::str(&s)),
        any::<u64>().prop_map(FieldValue::Timestamp),
        Just(FieldValue::Void),
        proptest::collection::vec(any::<u8>(), 0..20)
            .prop_map(|v| FieldValue::Bytes(Bytes::from(v))),
    ];
    leaf.prop_recursive(4, 32, 6, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..5).prop_map(FieldValue::Array),
            proptest::collection::vec(("[a-z]{1,8}", inner), 0..5)
                .prop_map(|v| FieldValue::Table(FieldTable(v))),
        ]
    })
}

fn value10() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<u8>().prop_map(Value::Ubyte),
        any::<u16>().prop_map(Value::Ushort),
        any::<u32>().prop_map(Value::Uint),
        any::<u64>().prop_map(Value::Ulong),
        any::<i8>().prop_map(Value::Byte),
        any::<i16>().prop_map(Value::Short),
        any::<i32>().prop_map(Value::Int),
        any::<i64>().prop_map(Value::Long),
        any::<i64>().prop_map(Value::Timestamp),
        any::<[u8; 16]>().prop_map(Value::Uuid),
        any::<char>().prop_map(Value::Char),
        ".{0,300}".prop_map(Value::String),
        "[a-z:-]{0,20}".prop_map(Value::Symbol),
        proptest::collection::vec(any::<u8>(), 0..300).prop_map(|v| Value::Binary(Bytes::from(v))),
    ];
    leaf.prop_recursive(4, 64, 8, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..8).prop_map(Value::List),
            proptest::collection::vec((inner.clone(), inner.clone()), 0..4).prop_map(Value::Map),
            (any::<u64>(), inner)
                .prop_map(|(d, v)| Value::Described(Box::new(Value::Ulong(d)), Box::new(v))),
            // Arrays of one type.
            proptest::collection::vec(any::<u32>(), 0..10)
                .prop_map(|v| Value::Array(v.into_iter().map(Value::Uint).collect())),
            proptest::collection::vec("[a-z]{0,5}", 0..10)
                .prop_map(|v| Value::Array(v.into_iter().map(Value::Symbol).collect())),
        ]
    })
}

fn mutate(mut b: Vec<u8>, at: usize, byte: u8, cut: usize) -> Vec<u8> {
    if !b.is_empty() {
        let i = at % b.len();
        b[i] = byte;
        let keep = b.len() - cut % b.len();
        b.truncate(keep);
    }
    b
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2000, .. ProptestConfig::default() })]

    #[test]
    fn tables_round_trip(t in proptest::collection::vec(("[a-z-]{1,10}", field_value()), 0..6)) {
        let t = FieldTable(t);
        let mut b = BytesMut::new();
        t.encode(&mut b);
        prop_assert_eq!(Dec::new(&b).table().unwrap(), t);
    }

    #[test]
    fn values_round_trip(v in value10()) {
        let b = v.to_bytes();
        prop_assert_eq!(Value::decode_all(&b).unwrap(), v);
    }

    #[test]
    fn decoders_never_panic_on_noise(b in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = Dec::new(&b).table();
        let _ = Method::decode(&b);
        let _ = BasicProperties::decode(&b);
        let _ = ContentHeader::decode(&Bytes::from(b.clone()));
        let _ = Frame::parse(&mut BytesMut::from(&b[..]), 131_072);
        let _ = Value::decode(&b);
        let _ = Sections::parse(&Bytes::from(b.clone()));
        let _ = Frame10::parse(&mut BytesMut::from(&b[..]), 131_072);
        if let Ok((v, _)) = Value::decode(&b) {
            let _ = Performative::from_value(&v);
        }
        let _ = Address::parse(&String::from_utf8_lossy(&b), true);
    }

    #[test]
    fn mutated_methods_never_panic(at in any::<usize>(), byte in any::<u8>(), cut in any::<usize>(), which in 0u8..4) {
        let m: Method = match which {
            0 => QueueDeclare { queue: "q".into(), durable: true, arguments: FieldTable::from([("x-max-length", FieldValue::I32(5))]), ..Default::default() }.into(),
            1 => BasicPublish { exchange: "x".into(), routing_key: "k".into(), mandatory: true, ..Default::default() }.into(),
            2 => BasicConsume { queue: "q".into(), consumer_tag: "t".into(), arguments: FieldTable::from([("x-priority", FieldValue::I32(1))]), ..Default::default() }.into(),
            _ => ConnectionStartOk { client_properties: FieldTable::from([("product", FieldValue::str("p"))]), mechanism: "PLAIN".into(), response: Bytes::from_static(b"\0u\0p"), locale: "en_US".into() }.into(),
        };
        let mut b = BytesMut::new();
        m.encode(&mut b);
        let b = mutate(b.to_vec(), at, byte, cut);
        let _ = Method::decode(&b);
    }

    #[test]
    fn mutated_performatives_never_panic(v in value10(), at in any::<usize>(), byte in any::<u8>(), cut in any::<usize>()) {
        let p = Value::composite(0x12, vec![Value::str("l"), Value::Uint(0), Value::Bool(true), v]);
        let b = mutate(p.to_bytes().to_vec(), at, byte, cut);
        if let Ok((v, _)) = Value::decode(&b) {
            let _ = Performative::from_value(&v);
        }
        let _ = Sections::parse(&Bytes::from(b));
    }
}
