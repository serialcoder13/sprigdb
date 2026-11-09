//! Golden bytes, worked out by hand from the AMQP 0-9-1 spec (with
//! `RabbitMQ`'s errata for field-table tags) and the OASIS AMQP 1.0 spec.

#![allow(clippy::unwrap_used, missing_docs)]

use bytes::{Bytes, BytesMut};
use sprig_proto_amqp::v10::message::{Properties, data_section, encode_sections};
use sprig_proto_amqp::v10::performative::{
    DeliveryState, FRAME_AMQP, Frame as Frame10, Performative, put_frame,
};
use sprig_proto_amqp::v10::types::Value;
use sprig_proto_amqp::v091::codec::{FieldTable, FieldValue};
use sprig_proto_amqp::v091::frame::{Frame, put_content, put_method};
use sprig_proto_amqp::v091::method::*;

fn frame(m: impl Into<Method>, ch: u16) -> Vec<u8> {
    let mut b = BytesMut::new();
    put_method(&mut b, ch, &m.into());
    b.to_vec()
}

#[test]
fn connection_close_frame() {
    // connection.close(320, "x", 0, 0) on channel 0.
    let got = frame(
        ConnectionClose {
            reply_code: 320,
            reply_text: "x".into(),
            class_id: 0,
            method_id: 0,
        },
        0,
    );
    let want = [
        1, 0, 0, 0, 0, 0, 12, 0, 10, 0, 50, 0x01, 0x40, 1, b'x', 0, 0, 0, 0, 0xCE,
    ];
    assert_eq!(got, want);
}

#[test]
fn queue_declare_bits() {
    // queue.declare: ticket 0, "q", passive=0 durable=1 exclusive=0
    // auto-delete=1 no-wait=0 -> bits 0b01010, empty table.
    let got = frame(
        QueueDeclare {
            queue: "q".into(),
            durable: true,
            auto_delete: true,
            ..Default::default()
        },
        1,
    );
    let want = [
        1,
        0,
        1,
        0,
        0,
        0,
        13,
        0,
        50,
        0,
        10,
        0,
        0,
        1,
        b'q',
        0b0000_1010,
        0,
        0,
        0,
        0,
        0xCE,
    ];
    assert_eq!(got, want);
}

#[test]
fn basic_ack_and_nack() {
    let got = frame(
        BasicAck {
            delivery_tag: 5,
            multiple: true,
        },
        1,
    );
    assert_eq!(
        got,
        [
            1, 0, 1, 0, 0, 0, 13, 0, 60, 0, 80, 0, 0, 0, 0, 0, 0, 0, 5, 1, 0xCE
        ]
    );
    let got = frame(
        BasicNack {
            delivery_tag: 5,
            multiple: false,
            requeue: true,
        },
        1,
    );
    assert_eq!(
        got,
        [
            1, 0, 1, 0, 0, 0, 13, 0, 60, 0, 120, 0, 0, 0, 0, 0, 0, 0, 5, 0b10, 0xCE
        ]
    );
}

#[test]
fn deliver_with_content() {
    // basic.deliver + header (content_type "t") + body "hi" on channel 1.
    let mut b = BytesMut::new();
    let props = [0x80, 0x00, 1, b't'];
    put_content(
        &mut b,
        1,
        &BasicDeliver {
            consumer_tag: "c".into(),
            delivery_tag: 1,
            redelivered: true,
            exchange: String::new(),
            routing_key: "q".into(),
        }
        .into(),
        &props,
        b"hi",
        4096,
    );
    let want: Vec<u8> = [
        &[
            1, 0, 1, 0, 0, 0, 18, 0, 60, 0, 60, 1, b'c', 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, b'q',
            0xCE,
        ][..],
        &[
            2, 0, 1, 0, 0, 0, 16, 0, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0x80, 0, 1, b't', 0xCE,
        ],
        &[3, 0, 1, 0, 0, 0, 2, b'h', b'i', 0xCE],
    ]
    .concat();
    assert_eq!(&b[..], &want[..]);
}

#[test]
fn rabbitmq_field_tags() {
    // RabbitMQ's tags: 's' is a signed 16-bit integer, 'l' signed 64, 'x' bytes.
    let t = FieldTable::from([
        ("s", FieldValue::I16(-2)),
        ("l", FieldValue::I64(1)),
        ("x", FieldValue::Bytes(Bytes::from_static(&[9]))),
    ]);
    let mut b = BytesMut::new();
    t.encode(&mut b);
    let want = [
        0, 0, 0, 24, 1, b's', b's', 0xff, 0xfe, 1, b'l', b'l', 0, 0, 0, 0, 0, 0, 0, 1, 1, b'x',
        b'x', 0, 0, 0, 1, 9,
    ];
    assert_eq!(&b[..], &want[..]);
}

#[test]
fn amqp10_attach_and_transfer_frames() {
    // attach(name "l", handle 0, role receiver) on channel 0:
    // 00 53 12 | c0 size count | a1 01 6c | 43 | 41 ...
    let a = Performative::Attach {
        name: "l".into(),
        handle: 0,
        role: true,
        snd_settle_mode: 2,
        rcv_settle_mode: 0,
        source: None,
        target: None,
        coordinator: false,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: None,
        max_message_size: None,
        offered_capabilities: vec![],
        desired_capabilities: vec![],
        properties: None,
    };
    let v = a.to_value().to_bytes();
    assert_eq!(&v[..3], &[0x00, 0x53, 0x12]);
    assert_eq!(
        &v[5..],
        &[
            9, 0xa1, 1, b'l', 0x43, 0x41, 0x50, 2, 0x50, 0, 0x40, 0x40, 0x40, 0x42
        ]
    );
    // A transfer frame carrying a data section "hi": the payload follows
    // the performative in the same frame.
    let t = Performative::Transfer {
        handle: 0,
        delivery_id: Some(0),
        delivery_tag: Some(Bytes::from_static(&[0])),
        message_format: Some(0),
        settled: Some(true),
        more: false,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    };
    let payload = encode_sections(&[data_section(Bytes::from_static(b"hi"))]);
    assert_eq!(&payload[..], &[0x00, 0x53, 0x75, 0xa0, 2, b'h', b'i']);
    let mut b = BytesMut::new();
    put_frame(&mut b, FRAME_AMQP, 3, &t.to_value(), &payload);
    let f = Frame10::parse(&mut b, 0).unwrap().unwrap();
    assert_eq!((f.channel, &f.payload[..]), (3, &payload[..]));
    assert_eq!(
        Performative::from_value(f.body.as_ref().unwrap()).unwrap(),
        t
    );
}

#[test]
fn amqp10_outcomes() {
    assert_eq!(
        &DeliveryState::Accepted.to_value().to_bytes()[..],
        &[0x00, 0x53, 0x24, 0x45]
    );
    assert_eq!(
        &DeliveryState::Released.to_value().to_bytes()[..],
        &[0x00, 0x53, 0x26, 0x45]
    );
    // properties with message-id ulong 1 and to "/queues/q".
    let p = Properties {
        message_id: Some(Value::Ulong(1)),
        to: Some("/queues/q".into()),
        ..Default::default()
    };
    let b = p.to_value().to_bytes();
    assert_eq!(&b[..3], &[0x00, 0x53, 0x73]);
    assert_eq!(
        &b[6..],
        &[
            0x53, 1, 0x40, 0xa1, 9, b'/', b'q', b'u', b'e', b'u', b'e', b's', b'/', b'q'
        ]
    );
}

#[test]
fn heartbeats() {
    let mut b = BytesMut::from(&[8u8, 0, 0, 0, 0, 0, 0, 0xCE][..]);
    let f = Frame::parse(&mut b, 0).unwrap().unwrap();
    assert_eq!((f.kind, f.channel, f.payload.len()), (8, 0, 0));
    let mut b = BytesMut::from(&[0u8, 0, 0, 8, 2, 0, 0, 0][..]);
    assert!(Frame10::parse(&mut b, 0).unwrap().unwrap().body.is_none());
}
