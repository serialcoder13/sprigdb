//! Unit tests of the shape extractor.

use sprig_common::ids::{DbId, ObjectId};
use sprig_sql::lex::LexOptions;

use super::*;
use crate::fingerprint::normalize;

/// Knows `shop.orders` (1201, `email` and `status` are strings) and
/// `shop.customers` (1188).
pub(crate) struct TestResolver;

impl TableResolver for TestResolver {
    fn resolve(&self, db: Option<&str>, table: &str) -> Option<ResolvedTable> {
        let id = match (db?, table) {
            ("shop", "orders") => 1201,
            ("shop", "customers") => 1188,
            ("shop", "events") => 1300,
            ("shop", "invoices") => 1400,
            _ => return None,
        };
        Some(ResolvedTable {
            object_id: ObjectId(id),
            db_id: DbId(1),
            physical: None,
        })
    }

    fn has_column(&self, t: &ResolvedTable, column: &str) -> Option<bool> {
        Some(match t.object_id.0 {
            1201 => [
                "id",
                "customer_id",
                "status",
                "created_at",
                "total",
                "email",
            ]
            .contains(&column),
            1188 => ["id", "name", "email_verified"].contains(&column),
            _ => return None,
        })
    }

    fn column_class(&self, t: &ResolvedTable, column: &str) -> Option<ColumnClass> {
        match (t.object_id.0, column) {
            (1201, "email" | "status") => Some(ColumnClass::String),
            (1201, _) => Some(ColumnClass::Numeric),
            _ => None,
        }
    }
}

pub(crate) fn shape_of(sql: &str) -> Shape {
    let lex = LexOptions::mariadb();
    let norm = normalize(sql.as_bytes(), &lex, Some("shop"), false);
    let ctx = ExtractCtx {
        lex,
        current_db: Some("shop"),
        lower_case_table_names: false,
    };
    extract(sql.as_bytes(), &norm, &ctx, &TestResolver)
}

#[test]
fn chapter_example_shape() {
    let s = shape_of(
        "SELECT o.id, o.total, c.name FROM orders o JOIN customers c ON c.id = o.customer_id \
         WHERE o.customer_id = 42 AND o.status IN ('open','hold') AND o.created_at >= '2026-01-01' \
         AND LOWER(o.email) = 'x' ORDER BY o.created_at DESC LIMIT 20",
    );
    let json = s.to_json();
    assert_eq!(
        json,
        r#"{"v":1,"type":"select","tables":[{"alias":"o","object_id":1201,"db":"shop","table":"orders","db_id":1},{"alias":"c","object_id":1188,"db":"shop","table":"customers","db_id":1}],"predicates":[{"t":"o","col":"customer_id","op":"eq","args":[0]},{"t":"o","col":"status","op":"in","list":true,"args":[1]},{"t":"o","col":"created_at","op":"range","lo":true,"hi":false,"args":[2]},{"t":"o","col":"email","op":"unsargable","why":"function:LOWER"}],"joins":[{"l":["c","id"],"r":["o","customer_id"],"op":"eq","kind":"inner"}],"order_by":[{"t":"o","col":"created_at","dir":"desc"}],"group_by":[],"select":{"c":["name"],"o":["id","total"]},"limit":true,"writes":null,"or_groups":0,"limit_arg":4}"#
    );
    assert_eq!(Shape::from_json(&json).unwrap(), s);
}

#[test]
fn scratch_shapes() {
    for sql in [
        "SELECT * FROM orders WHERE email LIKE 'abc%' AND total + 1 > 5 AND status = 5",
        "SELECT * FROM orders WHERE email LIKE '%abc' OR status = 'x'",
        "SELECT * FROM orders WHERE status = 'a' OR status = 'b' OR status IN ('c', 'd')",
        "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE name = 'x')",
        "SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.email_verified = 1)",
        "UPDATE orders SET status = 'x', total = total + 1 WHERE id = 5 ORDER BY created_at LIMIT 1",
        "DELETE FROM orders WHERE created_at < NOW() - INTERVAL 30 DAY",
        "INSERT INTO orders (id, status) VALUES (1, 'a'), (2, 'b') ON DUPLICATE KEY UPDATE status = VALUES(status)",
        "SELECT status, COUNT(*) FROM orders WHERE created_at BETWEEN ? AND ? GROUP BY status ORDER BY 2 DESC",
        "SELECT * FROM orders WHERE JSON_VALUE(doc, '$.x') = 1 AND name COLLATE utf8mb4_bin = 'a' AND id IS NOT NULL AND customer_id IS NULL",
        "SELECT * FROM orders WHERE MATCH (email) AGAINST ('x') AND ST_Contains(g, POINT(1,2))",
        "SELECT id FROM events ORDER BY VEC_DISTANCE_COSINE(v, VEC_FromText('[1,2]')) LIMIT 5",
        "SELECT * FROM (SELECT * FROM orders WHERE total > 5) d WHERE d.id = 3",
        "SELECT * FROM orders o1, orders o2 WHERE o1.id = o2.customer_id",
        "WITH big AS (SELECT customer_id FROM orders WHERE total > 100) SELECT * FROM customers c JOIN big b ON b.customer_id = c.id",
        "SELECT * FROM orders WHERE id = -5 AND total > -1.5",
        "SELECT /*+ NO_INDEX(orders) */ * FROM orders WHERE id = 1",
        "SELECT * FROM unknown_table WHERE a = 1",
        "SELECT * FROM orders WHERE status <> 'x' AND NOT (id = 3)",
        "SELECT * FROM orders WHERE (customer_id, status) IN ((1, 'a'), (2, 'b'))",
        "SELECT * FROM orders WHERE customer_id = 1 AND (status = 'a' OR total > 3)",
    ] {
        println!("{sql}\n  {}\n", shape_of(sql).to_json());
    }
}

/// R6 round 6 (decision K3): a JSON path predicate records its function
/// and the condition as it would be on a column, also in an `IN` list, so
/// the advisor can propose a field column for `JSON_VALUE`.
#[test]
fn json_paths_keep_their_function_and_condition() {
    let s = shape_of(
        "SELECT id FROM orders WHERE JSON_VALUE(email, '$.a') = ? AND JSON_VALUE(email, '$.b') \
         IN ('x', 'y') AND JSON_VALUE(email, '$.c') > 3 AND JSON_EXTRACT(email, '$.d') IS NULL \
         AND JSON_VALUE(email, '$.e') LIKE 'a%'",
    );
    let ops: Vec<String> = s
        .predicates
        .iter()
        .map(|p| serde_json::to_string(&p.op).unwrap())
        .collect();
    assert_eq!(
        ops,
        [
            r#"{"op":"json_path","path":"$.a","func":"JSON_VALUE","cmp":{"op":"eq"}}"#,
            r#"{"op":"json_path","path":"$.b","func":"JSON_VALUE","cmp":{"op":"in","list":true}}"#,
            r#"{"op":"json_path","path":"$.c","func":"JSON_VALUE","cmp":{"op":"range","lo":true,"hi":false}}"#,
            r#"{"op":"json_path","path":"$.d","func":"JSON_EXTRACT","cmp":{"op":"is_null"}}"#,
            r#"{"op":"json_path","path":"$.e","func":"JSON_VALUE","cmp":{"op":"like"}}"#,
        ]
    );
    // JSON_VALUE in =, IN, a range or IS NULL can use a field column; the
    // others can't, and a shape of only those stays all unsargable.
    let field: Vec<bool> = s
        .predicates
        .iter()
        .map(|p| p.op.is_field_sargable())
        .collect();
    assert_eq!(field, [true, true, true, false, false]);
    assert!(!s.all_unsargable());
    assert!(
        shape_of("SELECT id FROM orders WHERE JSON_VALUE(email, '$.e') LIKE 'a%'").all_unsargable()
    );
    // Shapes recorded before still read.
    let old: PredOp = serde_json::from_str(r#"{"op":"json_path","path":"$.a"}"#).unwrap();
    assert_eq!(
        old,
        PredOp::JsonPath {
            path: "$.a".into(),
            func: None,
            cmp: None
        }
    );
}
