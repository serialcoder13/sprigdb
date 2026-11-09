//! Graph values as PackStream structures, Bolt 5.x layouts (chapter 43,
//! "Bolt front end"; build step 19).
//!
//! | Tag | Structure | Fields (5.x) |
//! |---|---|---|
//! | `4E` | Node | id, labels, properties, `element_id` |
//! | `52` | Relationship | id, start id, end id, type, properties, `element_id`, start `element_id`, end `element_id` |
//! | `72` | `UnboundRelationship` | id, type, properties, `element_id` |
//! | `50` | Path | nodes, unbound relationships, indices |
//!
//! Path indices alternate a relationship index (1-based; negative when the
//! relationship is walked against its direction) and a node index.

use crate::packstream::PackValue;

/// Node tag.
pub const NODE: u8 = 0x4E;
/// Relationship tag.
pub const RELATIONSHIP: u8 = 0x52;
/// `UnboundRelationship` tag.
pub const UNBOUND: u8 = 0x72;
/// Path tag.
pub const PATH: u8 = 0x50;

/// A Node structure.
#[must_use]
pub fn node(id: i64, labels: &[String], props: PackValue, element_id: &str) -> PackValue {
    PackValue::Struct {
        tag: NODE,
        fields: vec![
            PackValue::Int(id),
            PackValue::List(labels.iter().cloned().map(PackValue::String).collect()),
            props,
            PackValue::str(element_id),
        ],
    }
}

/// The ends of a relationship: ids and element ids.
#[derive(Debug, Clone)]
pub struct Ends<'a> {
    /// Start node id.
    pub start: i64,
    /// End node id.
    pub end: i64,
    /// Start element id.
    pub start_eid: &'a str,
    /// End element id.
    pub end_eid: &'a str,
}

/// A Relationship structure.
#[must_use]
pub fn relationship(
    id: i64,
    ends: &Ends<'_>,
    rtype: &str,
    props: PackValue,
    eid: &str,
) -> PackValue {
    PackValue::Struct {
        tag: RELATIONSHIP,
        fields: vec![
            PackValue::Int(id),
            PackValue::Int(ends.start),
            PackValue::Int(ends.end),
            PackValue::str(rtype),
            props,
            PackValue::str(eid),
            PackValue::str(ends.start_eid),
            PackValue::str(ends.end_eid),
        ],
    }
}

/// An `UnboundRelationship` structure.
#[must_use]
pub fn unbound(id: i64, rtype: &str, props: PackValue, eid: &str) -> PackValue {
    PackValue::Struct {
        tag: UNBOUND,
        fields: vec![
            PackValue::Int(id),
            PackValue::str(rtype),
            props,
            PackValue::str(eid),
        ],
    }
}

/// A Path structure: `nodes` (distinct), `rels` (unbound, distinct) and
/// the walk as indices.
#[must_use]
pub fn path(nodes: Vec<PackValue>, rels: Vec<PackValue>, indices: Vec<i64>) -> PackValue {
    PackValue::Struct {
        tag: PATH,
        fields: vec![
            PackValue::List(nodes),
            PackValue::List(rels),
            PackValue::List(indices.into_iter().map(PackValue::Int).collect()),
        ],
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;
    use crate::packstream::encode;

    fn hex(s: &str) -> Vec<u8> {
        s.split_whitespace()
            .map(|h| u8::from_str_radix(h, 16).expect("hex"))
            .collect()
    }

    #[test]
    fn node_golden_bytes() {
        // (:Person {name: "A"}) with id 1, element id "n:1".
        let n = node(
            1,
            &["Person".to_owned()],
            PackValue::map([("name", PackValue::str("A"))]),
            "n:1",
        );
        let mut b = BytesMut::new();
        encode(&n, &mut b);
        assert_eq!(
            b.to_vec(),
            hex("B4 4E 01 91 86 50 65 72 73 6F 6E A1 84 6E 61 6D 65 81 41 83 6E 3A 31")
        );
    }

    #[test]
    fn relationship_and_path_golden_bytes() {
        let r = relationship(
            7,
            &Ends {
                start: 1,
                end: 2,
                start_eid: "a",
                end_eid: "b",
            },
            "KNOWS",
            PackValue::Map(vec![]),
            "r",
        );
        let mut b = BytesMut::new();
        encode(&r, &mut b);
        assert_eq!(
            b.to_vec(),
            hex("B8 52 07 01 02 85 4B 4E 4F 57 53 A0 81 72 81 61 81 62")
        );
        let p = path(
            vec![PackValue::Null, PackValue::Null],
            vec![PackValue::Null],
            vec![1, 1],
        );
        let mut b = BytesMut::new();
        encode(&p, &mut b);
        assert_eq!(b.to_vec(), hex("B3 50 92 C0 C0 91 C0 92 01 01"));
    }
}
