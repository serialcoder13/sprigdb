//! The tables and fingerprints of four example workloads, shared by the
//! candidate, cost and policy tests. Shapes are written in the workload
//! store's JSON form, the contract with `sprig-workload`.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::sync::Arc;

use sprig_common::ids::{Fingerprint, ObjectId};
use sprig_workload::shape::Shape;

use super::candidates::{FpInput, Literal};
use super::model::{IndexInfo, KeyPart, Origin};
use super::stats::histogram::{Histogram, KeyClass};
use super::stats::reader::classify;
use super::stats::{ColumnInfo, ColumnStats, Param, TableStats};

pub(crate) const ORDERS: ObjectId = ObjectId(1201);
pub(crate) const EVENTS: ObjectId = ObjectId(2201);
pub(crate) const INVOICES: ObjectId = ObjectId(3301);
pub(crate) const DOC_ORDERS: ObjectId = ObjectId(4401);

pub(crate) const GB: f64 = 1e9;

pub(crate) fn shape(json: &serde_json::Value) -> Arc<Shape> {
    Arc::new(Shape::from_json(&json.to_string()).unwrap())
}

/// A column: name, type, varchar octets.
fn col(name: &str, ty: &str, octets: Option<u64>) -> ColumnInfo {
    let (class, lob, fixed) = classify(ty);
    ColumnInfo {
        name: name.into(),
        data_type: ty.into(),
        class,
        charset: octets.map(|_| "utf8mb4".into()),
        collation: octets.map(|_| "utf8mb4_uca1400_ai_ci".into()),
        nullable: name != "id",
        max_bytes: fixed.or(octets),
        lob,
        generated: name.starts_with("_sprig_p_"),
    }
}

fn stat(avg_length: f64, avg_frequency: f64, histogram: Option<Histogram>) -> ColumnStats {
    ColumnStats {
        nulls_ratio: 0.0,
        avg_length,
        avg_frequency,
        histogram,
    }
}

fn hist(class: KeyClass, buckets: &[(&str, f64, u64)], end: &str) -> Histogram {
    let mut v: Vec<serde_json::Value> = buckets
        .iter()
        .map(|(s, size, ndv)| serde_json::json!({"start": s, "size": size, "ndv": ndv}))
        .collect();
    if let Some(l) = v.last_mut() {
        l["end"] = serde_json::json!(end);
    }
    let doc = serde_json::json!({"histogram_hb": v});
    Histogram::parse(class, doc.to_string().as_bytes()).unwrap()
}

fn index(name: &str, cols: &[&str], origin: Origin) -> IndexInfo {
    IndexInfo {
        name: name.into(),
        parts: cols.iter().map(|c| KeyPart::col(c)).collect(),
        unique: name == "PRIMARY",
        origin,
        ignored: false,
        usable: true,
    }
}

fn table(schema: &str, name: &str, rows: u64, bytes: f64) -> TableStats {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let total = bytes as u64;
    TableStats {
        schema: schema.into(),
        table: name.into(),
        cardinality: Some(rows),
        n_rows: Some(rows),
        data_bytes: total * 3 / 4,
        index_bytes: total - total * 3 / 4,
        primary_key: vec!["id".into()],
        eits: true,
        ..TableStats::default()
    }
}

/// Example 1: `shop.orders`, 48 million rows, 31.5 GB with
/// indexes, a user index `idx_created (created_at)`.
pub(crate) fn orders() -> TableStats {
    let mut t = table("t42_d1_g1_s1", "orders", 48_000_000, 31.5 * GB);
    for c in [
        col("id", "bigint", None),
        col("customer_id", "bigint", None),
        col("status", "varchar", Some(64)),
        col("created_at", "datetime", None),
        col("total", "decimal", None),
    ] {
        t.columns.insert(c.name.clone(), c);
    }
    t.column_stats.insert("id".into(), stat(8.0, 1.0, None));
    t.column_stats
        .insert("customer_id".into(), stat(8.0, 400.0, None));
    t.column_stats.insert(
        "status".into(),
        stat(
            4.0,
            12_000_000.0,
            Some(hist(
                KeyClass::Text,
                &[
                    ("cancelled", 0.03, 1),
                    ("hold", 0.01, 1),
                    ("open", 0.06, 1),
                    ("paid", 0.9, 1),
                ],
                "paid",
            )),
        ),
    );
    t.column_stats
        .insert("created_at".into(), stat(5.0, 1.0, None));
    t.column_stats.insert("total".into(), stat(8.0, 3.0, None));
    t.indexes = vec![
        index("PRIMARY", &["id"], Origin::System),
        index("idx_created", &["created_at"], Origin::User),
    ];
    t
}

/// Example 1's fingerprint `9c41d2e07b3a5f18`: 18,240 runs in 24 h, p95
/// 412 ms, mean 260 ms, 32,600 examined, 18 returned; `LIMIT 20` and
/// `status IN ('open','hold')` in its samples.
pub(crate) fn fp_example1() -> FpInput {
    FpInput {
        id: Fingerprint(0x9c41_d2e0_7b3a_5f18),
        shape: shape(&serde_json::json!({
            "v": 1, "type": "select",
            "tables": [{"alias": "o", "object_id": ORDERS.0, "table": "orders"}],
            "predicates": [
                {"t": "o", "col": "customer_id", "op": "eq", "args": [0]},
                {"t": "o", "col": "status", "op": "in", "list": true, "args": [1]}],
            "joins": [],
            "order_by": [{"t": "o", "col": "created_at", "dir": "desc"}],
            "group_by": [], "select": {"o": ["created_at", "id", "total"]},
            "limit": true, "writes": null, "or_groups": 0})),
        runs: 18_240,
        mean_ms: 260.0,
        p95_ms: 412.0,
        examined_per_run: 32_600.0,
        returned_per_run: 18.0,
        affected_per_run: 0.0,
        limit: Some(20),
        in_list_max: Some(2),
        samples: (0..20)
            .map(|i| {
                vec![
                    Literal::Value(Param::Num(f64::from(i))),
                    Literal::List(vec![Param::Text("open".into()), Param::Text("hold".into())]),
                    Literal::Value(Param::Num(20.0)),
                ]
            })
            .collect(),
        fast_only: false,
    }
}

fn write_fp(id: u64, object: ObjectId, kind: &str, set: &[&str], runs: u64) -> FpInput {
    let set: Vec<serde_json::Value> = set
        .iter()
        .map(|c| serde_json::json!({"t": "t", "col": c}))
        .collect();
    let (ty, preds) = match kind {
        "insert" => ("insert", serde_json::json!([])),
        _ => (
            kind,
            serde_json::json!([{"t": "t", "col": "id", "op": "eq", "args": [1]}]),
        ),
    };
    FpInput {
        id: Fingerprint(id),
        shape: shape(&serde_json::json!({
            "v": 1, "type": ty,
            "tables": [{"alias": "t", "object_id": object.0, "table": "t"}],
            "predicates": preds, "joins": [], "order_by": [], "group_by": [], "select": {},
            "limit": false,
            "writes": {"kind": kind, "targets": ["t"], "set": set,
                       "all_columns": kind == "insert" && set.is_empty()},
            "or_groups": 0})),
        runs,
        mean_ms: 1.0,
        p95_ms: 2.0,
        examined_per_run: 1.0,
        returned_per_run: 0.0,
        affected_per_run: 1.0,
        limit: None,
        in_list_max: None,
        samples: Vec::new(),
        fast_only: false,
    }
}

/// Example 1's writes over 24 h: 144,000 inserts and 216,000 status
/// updates an hour.
pub(crate) fn orders_writes() -> Vec<FpInput> {
    vec![
        write_fp(0x1001, ORDERS, "insert", &[], 144_000 * 24),
        write_fp(0x1002, ORDERS, "update", &["status"], 216_000 * 24),
    ]
}

/// Two other `SELECT` fingerprints on `orders`, so the median ms per row
/// read (`u_T`) is example 1's 0.0079.
pub(crate) fn orders_other_reads() -> Vec<FpInput> {
    let mk = |id: u64, mean: f64, examined: f64| FpInput {
        id: Fingerprint(id),
        shape: shape(&serde_json::json!({
            "v": 1, "type": "select",
            "tables": [{"alias": "o", "object_id": ORDERS.0, "table": "orders"}],
            "predicates": [{"t": "o", "col": "id", "op": "eq", "args": [0]}],
            "joins": [], "order_by": [], "group_by": [], "select": {"o": ["*"]},
            "limit": false, "writes": null, "or_groups": 0})),
        runs: 100,
        mean_ms: mean,
        p95_ms: mean * 2.0,
        examined_per_run: examined,
        returned_per_run: 1.0,
        affected_per_run: 0.0,
        limit: None,
        in_list_max: None,
        samples: Vec::new(),
        fast_only: false,
    };
    vec![mk(0x2001, 0.79, 100.0), mk(0x2002, 0.5, 100.0)]
}

/// Example 2: `shop.events`, 220 million rows on Galera, 800,000
/// inserts an hour.
pub(crate) fn events() -> TableStats {
    let mut t = table("t42_d1_g1_s1", "events", 220_000_000, 120.0 * GB);
    for c in [
        col("id", "bigint", None),
        col("account_id", "bigint", None),
        col("kind", "varchar", Some(128)),
        col("created_at", "datetime", None),
        col("source", "varchar", Some(256)),
        col("payload", "json", None),
    ] {
        t.columns.insert(c.name.clone(), c);
    }
    t.column_stats.insert("id".into(), stat(8.0, 1.0, None));
    // 2,000 rows per account.
    t.column_stats
        .insert("account_id".into(), stat(8.0, 2000.0, None));
    // Ten kinds.
    t.column_stats
        .insert("kind".into(), stat(8.0, 22_000_000.0, None));
    t.column_stats
        .insert("created_at".into(), stat(5.0, 1.0, None));
    t.indexes = vec![index("PRIMARY", &["id"], Origin::System)];
    t
}

fn events_fp(id: u64, runs: u64, p95: f64, mean: f64, body: &serde_json::Value) -> FpInput {
    let mut doc = serde_json::json!({
        "v": 1, "type": "select",
        "tables": [{"alias": "e", "object_id": EVENTS.0, "table": "events"}],
        "joins": [], "order_by": [], "group_by": [], "select": {},
        "limit": false, "writes": null, "or_groups": 0});
    for (k, v) in body.as_object().unwrap() {
        doc[k] = v.clone();
    }
    FpInput {
        id: Fingerprint(id),
        shape: shape(&doc),
        runs,
        mean_ms: mean,
        p95_ms: p95,
        examined_per_run: 50_000.0,
        returned_per_run: 20.0,
        affected_per_run: 0.0,
        limit: Some(20),
        in_list_max: None,
        samples: Vec::new(),
        fast_only: false,
    }
}

/// Example 2's F1 to F4. The run counts are read as per hour
/// (×24 over the 24-hour window): per day, the merged index's benefit
/// (about 17,000 ms/h) would fail its own gate against 2 × 40,000 ms/h of
/// write cost.
pub(crate) fn events_fps() -> [FpInput; 4] {
    let f1 = events_fp(
        0xF1,
        1900 * 24,
        180.0,
        150.0,
        &serde_json::json!({
            "predicates": [
                {"t": "e", "col": "account_id", "op": "eq", "args": [0]},
                {"t": "e", "col": "kind", "op": "eq", "args": [1]},
                {"t": "e", "col": "created_at", "op": "range", "lo": true, "hi": false,
                 "args": [2]}],
            "order_by": [{"t": "e", "col": "created_at", "dir": "asc"}],
            "select": {"e": ["created_at", "id", "payload"]}, "limit": true}),
    );
    let f2 = events_fp(
        0xF2,
        600 * 24,
        140.0,
        120.0,
        &serde_json::json!({
            "predicates": [
                {"t": "e", "col": "account_id", "op": "eq", "args": [0]},
                {"t": "e", "col": "kind", "op": "eq", "args": [1]}]}),
    );
    // F3's mean is chosen: 60 ms (p95 160) is low enough that its own
    // index is worth less per byte than the merged one, the intended
    // outcome; at a 120 ms mean the own index wins first.
    let f3 = events_fp(
        0xF3,
        2400 * 24,
        160.0,
        60.0,
        &serde_json::json!({
            "predicates": [{"t": "e", "col": "account_id", "op": "eq", "args": [0]}],
            "order_by": [{"t": "e", "col": "id", "dir": "desc"}],
            "select": {"e": ["*"]}, "limit": true}),
    );
    let f4 = events_fp(
        0xF4,
        80 * 24,
        900.0,
        700.0,
        &serde_json::json!({
            "predicates": [{"t": "e", "col": "source", "op": "unsargable",
                            "why": "function:LOWER"}],
            "select": {"e": ["*"]}}),
    );
    [f1, f2, f3, f4]
}

/// Example 2's inserts: 800,000 an hour.
pub(crate) fn events_writes() -> Vec<FpInput> {
    vec![write_fp(0x3001, EVENTS, "insert", &[], 800_000 * 24)]
}

/// Example 3: `billing.invoices`, 9 million rows, 4.1 GB; the
/// histogram puts `overdue` at 0.5% of rows.
pub(crate) fn invoices() -> TableStats {
    let mut t = table("t42_d2_g1_s1", "invoices", 9_000_000, 4.1 * GB);
    for c in [
        col("id", "bigint", None),
        col("state", "varchar", Some(64)),
        col("due_date", "date", None),
        col("amount", "decimal", None),
    ] {
        t.columns.insert(c.name.clone(), c);
    }
    t.column_stats.insert("id".into(), stat(8.0, 1.0, None));
    t.column_stats.insert(
        "state".into(),
        stat(
            7.0,
            2_250_000.0,
            Some(hist(
                KeyClass::Text,
                &[
                    ("draft", 0.1, 1),
                    ("open", 0.3, 1),
                    ("overdue", 0.005, 1),
                    ("paid", 0.595, 1),
                ],
                "paid",
            )),
        ),
    );
    t.column_stats.insert(
        "due_date".into(),
        stat(
            3.0,
            2500.0,
            Some(hist(
                KeyClass::Temporal,
                &[("2020-01-01", 0.5, 1800), ("2025-01-01", 0.5, 1800)],
                "2030-01-01",
            )),
        ),
    );
    t.column_stats
        .insert("amount".into(), stat(6.0, 10.0, None));
    t.indexes = vec![index("PRIMARY", &["id"], Origin::System)];
    t
}

/// Example 3's F5 (`state = ? AND due_date < ? ORDER BY due_date LIMIT ?`)
/// and F6 (`due_date BETWEEN ? AND ? GROUP BY state`).
pub(crate) fn invoices_fps() -> [FpInput; 2] {
    let base = |id: u64, runs: u64, p95: f64, mean: f64| FpInput {
        id: Fingerprint(id),
        shape: shape(&serde_json::json!({"v": 1, "type": "select", "tables": [],
            "predicates": [], "joins": [], "order_by": [], "group_by": [], "select": {},
            "limit": false, "writes": null, "or_groups": 0})),
        runs,
        mean_ms: mean,
        p95_ms: p95,
        examined_per_run: 9_000_000.0,
        returned_per_run: 20.0,
        affected_per_run: 0.0,
        limit: Some(20),
        in_list_max: None,
        samples: Vec::new(),
        fast_only: false,
    };
    let mut f5 = base(0xF5, 3100, 220.0, 180.0);
    f5.shape = shape(&serde_json::json!({
        "v": 1, "type": "select",
        "tables": [{"alias": "billing.invoices", "object_id": INVOICES.0, "table": "invoices"}],
        "predicates": [
            {"t": "billing.invoices", "col": "state", "op": "eq", "args": [0]},
            {"t": "billing.invoices", "col": "due_date", "op": "range", "lo": false,
             "hi": true, "args": [1]}],
        "joins": [],
        "order_by": [{"t": "billing.invoices", "col": "due_date", "dir": "asc"}],
        "group_by": [], "select": {"billing.invoices": ["amount", "due_date", "id"]},
        "limit": true, "writes": null, "or_groups": 0}));
    f5.samples = (0..20)
        .map(|_| {
            vec![
                Literal::Value(Param::Text("overdue".into())),
                Literal::Value(Param::Text("2026-11-01".into())),
            ]
        })
        .collect();
    let mut f6 = base(0xF6, 40, 1800.0, 1800.0);
    f6.limit = None;
    f6.shape = shape(&serde_json::json!({
        "v": 1, "type": "select",
        "tables": [{"alias": "billing.invoices", "object_id": INVOICES.0, "table": "invoices"}],
        "predicates": [
            {"t": "billing.invoices", "col": "due_date", "op": "range", "lo": true,
             "hi": true, "args": [0, 1]}],
        "joins": [], "order_by": [],
        "group_by": [{"t": "billing.invoices", "col": "state"}],
        "select": {"billing.invoices": ["amount", "state"]},
        "limit": false, "writes": null, "or_groups": 0}));
    [f5, f6]
}

/// Example 4: the collection `shop.orders`, 12 million documents,
/// with field columns for `customer.id` and `createdAt`.
pub(crate) fn doc_orders() -> TableStats {
    let mut t = table("t42_d1_g1_s1", "orders_doc", 12_000_000, 9.0 * GB);
    for c in [
        col("id", "bigint", None),
        col("doc", "json", None),
        col("_sprig_p_customer_id", "bigint", None),
        col("_sprig_p_createdat", "bigint", None),
    ] {
        t.columns.insert(c.name.clone(), c);
    }
    t.column_stats.insert("id".into(), stat(8.0, 1.0, None));
    t.column_stats
        .insert("_sprig_p_customer_id".into(), stat(8.0, 100.0, None));
    t.column_stats
        .insert("_sprig_p_createdat".into(), stat(8.0, 1.0, None));
    t.indexes = vec![index("PRIMARY", &["id"], Origin::System)];
    t
}

/// Example 4's shape, translated to field columns: `customer.id` and
/// `createdAt` are field columns, `status` stays a JSON filter.
pub(crate) fn fp_example4() -> FpInput {
    FpInput {
        id: Fingerprint(0x4444),
        shape: shape(&serde_json::json!({
            "v": 1, "type": "select",
            "tables": [{"alias": "o", "object_id": DOC_ORDERS.0, "table": "orders"}],
            "predicates": [
                {"t": "o", "col": "_sprig_p_customer_id", "op": "eq", "args": [0]},
                {"t": "o", "col": "doc", "op": "json_path", "path": "$.status", "args": [1]}],
            "joins": [],
            "order_by": [{"t": "o", "col": "_sprig_p_createdat", "dir": "desc"}],
            "group_by": [], "select": {"o": ["*"]},
            "limit": true, "writes": null, "or_groups": 0})),
        runs: 9600,
        mean_ms: 200.0,
        p95_ms: 300.0,
        examined_per_run: 100_000.0,
        returned_per_run: 20.0,
        affected_per_run: 0.0,
        limit: Some(20),
        in_list_max: None,
        samples: Vec::new(),
        fast_only: false,
    }
}
