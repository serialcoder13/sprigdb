//! MongoDB shapes (chapter 12, "MongoDB shapes and API patterns"; build
//! step 8): `Fingerprinter::mongo`.
//!
//! The gateway (which has the BSON and `sprig_doc::qshape`) reads the
//! command and hands over a [`MongoInput`]: the command, the namespace,
//! the collection's table, the shape text `$queryStats` and query settings
//! key by (`sprig_doc::qshape`, S96), and the filter's top-level
//! conditions with their values. [`fingerprint`] turns it into a
//! [`Fingerprint`] of kind `mongo` whose:
//!
//! - **text** is `<command> <db>.<collection> <shape>`, the shape being
//!   MongoDB 8.0's `debugQueryShape` form as compact JSON, so the
//!   fingerprint and the `$queryStats` entry of one query shape are the
//!   same grouping;
//! - **shape** names the collection's table (its catalog object), one
//!   `json_path` predicate per condition on the `doc` column with `func`
//!   `mongo` (a path no document has held an array on) or `mongo_multikey`
//!   (a path crossing an array) and `cmp` the condition (`eq`, `in`,
//!   `range`, `is_null`, `ne`), and the sort paths as `order_by`. Chapter
//!   13's document candidates read these (`sprig_advisor::autoindex::doc`);
//!   the SQL candidate generator ignores them (they aren't SQL columns);
//! - **literals** fill one slot per condition, in order: one value, an
//!   `$in` list, or nothing (`null`). A sample keeps the values as
//!   [`SampleValue`]s whose kind is the BSON class (number, string, bool,
//!   date as `Temporal`, ObjectId as `Hex`) and whose text is a SQL
//!   literal, so the validation replay can bind them.

use std::sync::Arc;

use sprig_common::ids::{DbId, ObjectId};

use super::{
    DEFAULT_MAX_TEXT_BYTES, Fingerprint, FpFlags, FpKind, FrontEnd, Literal, LiteralKind, digest,
};
use crate::shape::{
    ColumnRef, OrderItem, PredOp, Predicate, Shape, SortDir, StmtType, TableRef, WriteKind, Writes,
};
use crate::window::{SampleValue, SampledLiterals, SampledSlot};

/// The `func` of a condition on a path no document holds an array on.
pub const FUNC_SCALAR: &str = "mongo";
/// The `func` of a condition on a path crossing an array (multikey).
pub const FUNC_MULTIKEY: &str = "mongo_multikey";
/// The collection table's document column.
pub const DOC_COLUMN: &str = "doc";

/// One top-level condition of a filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MongoPred {
    /// The dotted field path.
    pub path: String,
    /// The condition: `Eq`, `In`, `Range`, `IsNull`, `Ne`.
    pub cmp: PredOp,
    /// Some document has held an array at the path or a prefix of it.
    pub multikey: bool,
    /// Its values (one; several for `$in`; none for `null`).
    pub values: Vec<SampleValue>,
}

/// What [`fingerprint`] reads from one command.
#[derive(Debug, Clone)]
pub struct MongoInput<'a> {
    /// `find`, `aggregate`, `count`, `distinct`, `update`, `delete`,
    /// `findAndModify`.
    pub cmd: &'a str,
    /// The logical database.
    pub db: &'a str,
    /// Its id (part of the fingerprint id), `DbId(0)` if unknown.
    pub db_id: DbId,
    /// The collection.
    pub coll: &'a str,
    /// The collection's MariaDB table.
    pub table: &'a str,
    /// The table's catalog object (`collections.collection_id`).
    pub object_id: Option<ObjectId>,
    /// The query shape as compact JSON (`sprig_doc::qshape`).
    pub shape_text: &'a str,
    /// The filter's top-level conditions, in filter order.
    pub preds: Vec<MongoPred>,
    /// The sort, in order: `(path, 1 | -1)`.
    pub sort: Vec<(String, i32)>,
    /// A `limit` was given.
    pub limit: bool,
    /// `workload.max_text_bytes`.
    pub max_text_bytes: usize,
}

impl Default for MongoInput<'_> {
    fn default() -> Self {
        Self {
            cmd: "find",
            db: "",
            db_id: DbId(0),
            coll: "",
            table: "",
            object_id: None,
            shape_text: "{}",
            preds: Vec::new(),
            sort: Vec::new(),
            limit: false,
            max_text_bytes: DEFAULT_MAX_TEXT_BYTES,
        }
    }
}

/// The statement type of a MongoDB command.
#[must_use]
pub fn stmt_type(cmd: &str) -> StmtType {
    match cmd {
        "update" | "findAndModify" => StmtType::Update,
        "delete" => StmtType::Delete,
        "insert" => StmtType::Insert,
        _ => StmtType::Select,
    }
}

/// `Fingerprinter::mongo` (module docs): the fingerprint of one command,
/// and this run's values by slot (for a sample).
#[must_use]
pub fn fingerprint(input: &MongoInput<'_>) -> (Fingerprint, SampledLiterals) {
    let full = format!(
        "{} {}.{} {}",
        input.cmd, input.db, input.coll, input.shape_text
    );
    let id = digest(FpKind::Mongo, input.db_id, None, &full);
    let mut flags = FpFlags::default();
    let text = if full.len() > input.max_text_bytes {
        flags.truncated = true;
        let mut end = input.max_text_bytes;
        while end > 0 && !full.is_char_boundary(end) {
            end -= 1;
        }
        full[..end].to_owned()
    } else {
        full
    };
    let alias = input.table.to_owned();
    let st = stmt_type(input.cmd);
    let mut shape = Shape::empty(st);
    shape.tables.push(TableRef {
        alias: alias.clone(),
        object_id: input.object_id,
        db: Some(input.db.to_owned()),
        table: input.table.to_owned(),
        db_id: (input.db_id.0 != 0).then_some(input.db_id),
        derived: false,
    });
    let mut literals = Vec::new();
    let mut slots = Vec::new();
    let mut list_slots = Vec::new();
    let mut in_list_max = 0u32;
    for (i, p) in input.preds.iter().enumerate() {
        let slot = u32::try_from(i).unwrap_or(u32::MAX);
        let list = matches!(p.cmp, PredOp::In { .. });
        shape.predicates.push(Predicate {
            column: ColumnRef::new(alias.clone(), DOC_COLUMN),
            op: PredOp::JsonPath {
                path: p.path.clone(),
                func: Some(
                    if p.multikey {
                        FUNC_MULTIKEY
                    } else {
                        FUNC_SCALAR
                    }
                    .to_owned(),
                ),
                cmp: Some(Box::new(p.cmp.clone())),
            },
            args: vec![slot],
        });
        for v in &p.values {
            literals.push(Literal {
                kind: v.kind,
                span: 0..0,
                slot,
                param_index: 0,
            });
        }
        if list {
            list_slots.push(slot);
            in_list_max = in_list_max.max(u32::try_from(p.values.len()).unwrap_or(u32::MAX));
            slots.push(SampledSlot::List(p.values.clone()));
        } else if let Some(v) = p.values.first() {
            slots.push(SampledSlot::One(v.clone()));
        } else {
            slots.push(SampledSlot::List(Vec::new()));
        }
    }
    for (path, dir) in &input.sort {
        shape.order_by.push(OrderItem {
            column: ColumnRef::new(alias.clone(), path.clone()),
            dir: if *dir < 0 { SortDir::Desc } else { SortDir::Asc },
        });
    }
    shape.limit = input.limit;
    if matches!(st, StmtType::Update | StmtType::Delete) {
        shape.writes = Some(Writes {
            kind: if st == StmtType::Delete {
                WriteKind::Delete
            } else {
                WriteKind::Update
            },
            targets: vec![alias.clone()],
            set: vec![ColumnRef::new(alias, DOC_COLUMN)],
            all_columns: st == StmtType::Delete,
        });
    }
    let fp = Fingerprint {
        id,
        kind: FpKind::Mongo,
        text: Arc::from(text),
        shape: Arc::new(shape),
        literals,
        front_end: FrontEnd::Mongo,
        caller: None,
        db_id: input.db_id,
        flags,
        in_list_max,
        sensitive: false,
        list_slots,
    };
    (fp, SampledLiterals { slots })
}

/// A sampled number (its decimal text).
#[must_use]
pub fn number(text: &str) -> SampleValue {
    SampleValue {
        kind: LiteralKind::Number,
        text: text.as_bytes().to_vec(),
    }
}

/// A sampled string, as a quoted SQL literal of `kind` (`String`, `Hex`
/// for an ObjectId's hex, `Temporal` for a date's ISO text).
#[must_use]
pub fn quoted(kind: LiteralKind, s: &str) -> SampleValue {
    let mut t = Vec::with_capacity(s.len() + 2);
    t.push(b'\'');
    for b in s.bytes() {
        match b {
            b'\'' => t.extend_from_slice(b"''"),
            b'\\' => t.extend_from_slice(b"\\\\"),
            0 => t.extend_from_slice(b"\\0"),
            _ => t.push(b),
        }
    }
    t.push(b'\'');
    SampleValue { kind, text: t }
}

/// A sampled boolean (`TRUE`, `FALSE`).
#[must_use]
pub fn boolean(b: bool) -> SampleValue {
    SampleValue {
        kind: LiteralKind::Bool,
        text: if b { b"TRUE".to_vec() } else { b"FALSE".to_vec() },
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn input<'a>(shape: &'a str, v: i64) -> MongoInput<'a> {
        MongoInput {
            cmd: "find",
            db: "shop",
            db_id: DbId(3),
            coll: "orders",
            table: "orders",
            object_id: Some(ObjectId(1201)),
            shape_text: shape,
            preds: vec![
                MongoPred {
                    path: "status".into(),
                    cmp: PredOp::Eq,
                    multikey: false,
                    values: vec![quoted(LiteralKind::String, "open")],
                },
                MongoPred {
                    path: "qty".into(),
                    cmp: PredOp::Range { lo: true, hi: false },
                    multikey: false,
                    values: vec![number(&v.to_string())],
                },
                MongoPred {
                    path: "tags".into(),
                    cmp: PredOp::In { list: true },
                    multikey: true,
                    values: vec![
                        quoted(LiteralKind::String, "a"),
                        quoted(LiteralKind::String, "b'c"),
                    ],
                },
            ],
            sort: vec![("createdAt".into(), -1)],
            limit: true,
            max_text_bytes: 4096,
        }
    }

    #[test]
    fn same_shape_same_id_values_by_slot() {
        let s = r#"{"filter":{"status":{"$eq":"?string"}}}"#;
        let (a, la) = fingerprint(&input(s, 5));
        let (b, lb) = fingerprint(&input(s, 9));
        assert_eq!(a.id, b.id);
        assert_eq!(a.kind, FpKind::Mongo);
        assert_eq!(&*a.text, &format!("find shop.orders {s}"));
        assert_ne!(la, lb);
        assert_eq!(la.slots.len(), 3);
        assert_eq!(a.list_slots, vec![2]);
        assert_eq!(a.in_list_max, 2);
        let SampledSlot::List(l) = &la.slots[2] else {
            panic!()
        };
        assert_eq!(l[1].text, b"'b''c'".to_vec());
        // Another database id: another fingerprint.
        let mut i = input(s, 5);
        i.db_id = DbId(4);
        assert_ne!(fingerprint(&i).0.id, a.id);
    }

    #[test]
    fn shape_names_doc_paths() {
        let (f, _) = fingerprint(&input("{}", 1));
        let j = f.shape.to_json();
        let back = Shape::from_json(&j).unwrap();
        assert_eq!(back, *f.shape);
        assert_eq!(f.shape.main_table().unwrap().object_id, Some(ObjectId(1201)));
        let PredOp::JsonPath { path, func, cmp } = &f.shape.predicates[0].op else {
            panic!()
        };
        assert_eq!(path, "status");
        assert_eq!(func.as_deref(), Some(FUNC_SCALAR));
        assert_eq!(**cmp.as_ref().unwrap(), PredOp::Eq);
        assert!(matches!(
            &f.shape.predicates[2].op,
            PredOp::JsonPath { func: Some(x), .. } if x == FUNC_MULTIKEY
        ));
        assert_eq!(f.shape.order_by[0].dir, SortDir::Desc);
        assert!(f.shape.limit);
        assert!(!f.shape.predicates[0].op.is_sargable());
        assert!(!f.shape.predicates[0].op.is_field_sargable());
    }

    #[test]
    fn writes_and_truncation() {
        let mut i = input("{}", 1);
        i.cmd = "update";
        i.max_text_bytes = 10;
        let (f, _) = fingerprint(&i);
        assert_eq!(f.shape.stmt_type, StmtType::Update);
        assert_eq!(f.shape.writes.as_ref().unwrap().set[0].col, "doc");
        assert!(f.flags.truncated);
        assert_eq!(f.text.len(), 10);
        assert_eq!(stmt_type("delete"), StmtType::Delete);
    }
}
