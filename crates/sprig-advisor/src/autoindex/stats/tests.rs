#![allow(clippy::unwrap_used, clippy::float_cmp, clippy::many_single_char_names)]

use std::sync::Arc;
use std::time::Duration;

use sprig_common::time::{Clock, ManualClock};

use super::analyze::AnalyzeReason;
use super::*;
use crate::autoindex::model::{KeyPart, Origin};

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-9_f64.max(b.abs() * 1e-6)
}

/// A `JSON_HB` document from `(start, size, ndv)` buckets and an end.
fn hb(buckets: &[(&str, f64, u64)], end: &str) -> Vec<u8> {
    let mut v: Vec<serde_json::Value> = buckets
        .iter()
        .map(|(s, size, ndv)| serde_json::json!({"start": s, "size": size, "ndv": ndv}))
        .collect();
    if let Some(last) = v.last_mut() {
        last["end"] = serde_json::json!(end);
    }
    serde_json::to_vec(&serde_json::json!({
        "target_histogram_size": 254,
        "collected_at": "2026-11-08 10:15:50",
        "collected_by": "11.8.9-MariaDB",
        "histogram_hb": v,
    }))
    .unwrap()
}

fn column(name: &str, data_type: &str) -> ColumnInfo {
    let (class, lob, fixed) = reader::classify(data_type);
    ColumnInfo {
        name: name.into(),
        data_type: data_type.into(),
        class,
        charset: None,
        collation: None,
        nullable: true,
        max_bytes: fixed,
        lob,
        generated: false,
    }
}

/// `shop.orders` of worked example 1: 48 million rows; `customer_id`
/// with `avg_frequency` 400; `status` with a histogram where `open` is 6%
/// and `hold` 1%.
fn orders() -> TableStats {
    let mut t = TableStats {
        schema: "t42_d1_g1_s1".into(),
        table: "orders".into(),
        cardinality: Some(48_000_000),
        n_rows: Some(48_000_000),
        eits: true,
        primary_key: vec!["id".into()],
        ..TableStats::default()
    };
    for (c, ty) in [
        ("id", "bigint"),
        ("customer_id", "bigint"),
        ("status", "varchar"),
        ("created_at", "datetime"),
        ("total", "decimal"),
        ("note", "text"),
    ] {
        t.columns.insert(c.into(), column(c, ty));
    }
    let status = Histogram::parse(
        KeyClass::Text,
        &hb(
            &[
                ("cancelled", 0.03, 1),
                ("hold", 0.01, 1),
                ("open", 0.06, 1),
                ("paid", 0.9, 1),
            ],
            "paid",
        ),
    )
    .unwrap();
    t.column_stats.insert(
        "status".into(),
        ColumnStats {
            nulls_ratio: 0.0,
            avg_length: 4.0,
            avg_frequency: 12_000_000.0,
            histogram: Some(status),
        },
    );
    t.column_stats.insert(
        "customer_id".into(),
        ColumnStats {
            nulls_ratio: 0.0,
            avg_length: 8.0,
            avg_frequency: 400.0,
            histogram: None,
        },
    );
    t
}

fn text(s: &str) -> Key {
    Key::Bytes(s.as_bytes().to_vec())
}

#[test]
fn worked_example_1_selectivities() {
    let t = orders();
    // customer_id avg_frequency 400 → sel 8.3e-6.
    let c = t.sel_eq("customer_id", &[]);
    assert_eq!(c.source, SelSource::AvgFrequency);
    assert!(close(c.value, 400.0 / 48e6));
    assert!((c.value - 8.3e-6).abs() < 0.05e-6);
    // status JSON_HB: open 0.06, hold 0.01 → sel_in 0.07.
    let s = t.sel_in("status", &[text("open"), text("hold")], 2);
    assert_eq!(s.source, SelSource::Histogram);
    assert!(close(s.value, 0.07));
}

#[test]
fn point_estimates_on_a_measured_shape() {
    // The integer histogram measured on 11.8: a value can end one bucket
    // and start the next.
    let h = Histogram::parse(
        KeyClass::Numeric,
        &hb(&[("0", 0.004, 2), ("1", 0.004, 3), ("3", 0.004, 3)], "4"),
    )
    .unwrap();
    // 0: only the first bucket; 1: half of the first plus a third of the
    // second; 2: a third of the second.
    assert!(close(h.point(&Key::Num(0.0)), 0.002));
    assert!(close(h.point(&Key::Num(1.0)), 0.002 + 0.004 / 3.0));
    assert!(close(h.point(&Key::Num(2.0)), 0.004 / 3.0));
    // Outside the histogram.
    assert_eq!(h.point(&Key::Num(-1.0)), 0.0);
    assert_eq!(h.point(&Key::Num(5.0)), 0.0);
    // A popular value: its whole bucket; a value between two popular
    // values doesn't exist.
    let s = orders();
    let st = s.column_stats["status"].histogram.as_ref().unwrap();
    assert!(close(st.point(&text("paid")), 0.9));
    assert_eq!(st.point(&text("new")), 0.0);
}

#[test]
fn ranges_interpolate_inside_numeric_buckets() {
    let h = Histogram::parse(
        KeyClass::Numeric,
        &hb(&[("0", 0.25, 10), ("10", 0.25, 10), ("20", 0.5, 20)], "40"),
    )
    .unwrap();
    assert!(close(h.range(None, None), 1.0));
    assert!(close(
        h.range(Some(&Key::Num(10.0)), Some(&Key::Num(20.0))),
        0.25
    ));
    // Half of the first bucket and all of the second.
    assert!(close(
        h.range(Some(&Key::Num(5.0)), Some(&Key::Num(20.0))),
        0.375
    ));
    // Open below: [.., 30] = 0.25 + 0.25 + half of 0.5.
    assert!(close(h.range(None, Some(&Key::Num(30.0))), 0.75));
    assert!(close(h.range(Some(&Key::Num(50.0)), None), 0.0));
}

#[test]
fn temporal_values_compare_as_seconds() {
    assert_eq!(temporal_seconds("1970-01-01").unwrap(), 0.0);
    assert_eq!(temporal_seconds("1970-01-02 00:00:01").unwrap(), 86_401.0);
    assert_eq!(temporal_seconds("-01:00:00").unwrap(), -3600.0);
    assert!(temporal_seconds("2026-13-01").is_none());
    let h = Histogram::parse(
        KeyClass::Temporal,
        &hb(
            &[
                ("2026-01-01 00:00:00", 0.5, 100),
                ("2026-01-02 00:00:00", 0.5, 100),
            ],
            "2026-01-03 00:00:00",
        ),
    )
    .unwrap();
    let lo = Key::parse(KeyClass::Temporal, "2026-01-01 12:00:00").unwrap();
    assert!(close(h.range(Some(&lo), None), 0.75));
}

#[test]
fn nulls_scale_histograms_but_not_avg_frequency() {
    // Measured: half NULL, `a` in 10% of all rows → size 0.2 of non-NULL
    // rows; avg_frequency 250 over 1000 rows.
    let mut t = TableStats {
        cardinality: Some(1000),
        n_rows: Some(1000),
        eits: true,
        ..TableStats::default()
    };
    t.columns.insert("v".into(), column("v", "varchar"));
    t.column_stats.insert(
        "v".into(),
        ColumnStats {
            nulls_ratio: 0.5,
            avg_length: 1.0,
            avg_frequency: 250.0,
            histogram: Some(
                Histogram::parse(KeyClass::Text, &hb(&[("a", 0.2, 1), ("b", 0.8, 1)], "b"))
                    .unwrap(),
            ),
        },
    );
    assert!(close(t.sel_eq("v", &[text("a")]).value, 0.1));
    assert!(close(t.sel_eq("v", &[]).value, 0.25));
    assert!(close(t.sel_null("v").value, 0.5));
    assert!(close(t.sel_not_null("v").value, 0.5));
}

#[test]
fn defaults_and_fallbacks() {
    let mut t = orders();
    assert_eq!(t.sel_range("created_at", None, None).value, DEFAULT_SEL);
    assert_eq!(t.sel_eq("created_at", &[]).source, SelSource::Default);
    // With SHOW INDEX cardinality of an index starting with the column.
    t.indexes.push(crate::autoindex::model::IndexInfo {
        name: "idx_created".into(),
        parts: vec![KeyPart::col("created_at")],
        unique: false,
        origin: Origin::User,
        ignored: false,
        usable: true,
    });
    t.show_index
        .insert("idx_created".into(), vec![Some(1_000_000)]);
    let s = t.sel_eq("created_at", &[]);
    assert_eq!(s.source, SelSource::ShowIndex);
    assert!(close(s.value, 1e-6));
    // Clamped to [1/N, 1].
    assert!(t.sel_in("status", &[], 1000).value <= 1.0);
    assert!(t.sel_eq("status", &[text("nothing")]).value >= 1.0 / 48e6);
}

#[test]
fn prefix_selectivity_prefers_index_stats() {
    let mut t = orders();
    t.indexes.push(crate::autoindex::model::IndexInfo {
        name: "k_cust".into(),
        parts: vec![KeyPart::col("status"), KeyPart::col("customer_id")],
        unique: false,
        origin: Origin::User,
        ignored: false,
        usable: true,
    });
    t.index_stats
        .insert("k_cust".into(), vec![12_000_000.0, 96.0]);
    // Same set of columns in another order.
    let s = t.sel_prefix(&["customer_id", "status"], &[1e-5, 0.07]);
    assert_eq!(s.source, SelSource::IndexStats);
    assert!(close(s.value, 2e-6));
    let s = t.sel_prefix(&["customer_id", "created_at"], &[1e-5, 0.1]);
    assert!(close(s.value, 1e-6));
}

#[test]
fn staleness_and_analyze_plans() {
    let mut t = orders();
    let limits = AnalyzeLimits::default();
    assert!(!t.is_stale(0.2));
    // created_at has no statistics; note is a TEXT column, skipped.
    let p = plan_analyze(&t, &["customer_id", "created_at", "note"], &[], &limits).unwrap();
    assert_eq!(p.columns, ["created_at"]);
    assert_eq!(p.reason, AnalyzeReason::Missing);
    assert_eq!(
        p.statement(),
        "ANALYZE LOCAL TABLE `t42_d1_g1_s1`.`orders` PERSISTENT FOR COLUMNS (`created_at`) \
         INDEXES ()"
    );
    // A column with statistics but no histogram is missing only where a
    // histogram is wanted.
    assert!(plan_analyze(&t, &["customer_id"], &[], &limits).is_none());
    let p = plan_analyze(&t, &[], &["customer_id"], &limits).unwrap();
    assert_eq!(p.columns, ["customer_id"]);
    assert!(plan_analyze(&t, &["status"], &["status"], &limits).is_none());
    // 25% more rows: everything wanted is re-collected.
    t.n_rows = Some(60_000_000);
    assert!(t.is_stale(0.2));
    let p = plan_analyze(&t, &["status", "customer_id"], &[], &limits).unwrap();
    assert_eq!(p.reason, AnalyzeReason::Stale);
    assert_eq!(p.columns, ["customer_id", "status"]);
    // No access to the statistics tables: nothing to plan.
    t.eits = false;
    assert!(plan_analyze(&t, &["status"], &[], &limits).is_none());
}

#[derive(Clone)]
struct Shared(Arc<ManualClock>);

impl Clock for Shared {
    fn now(&self) -> std::time::Instant {
        self.0.now()
    }
    fn wall(&self) -> std::time::SystemTime {
        self.0.wall()
    }
}

#[test]
fn analyze_is_throttled_per_table_and_per_tenant() {
    let clock = Arc::new(ManualClock::new());
    let th = AnalyzeThrottle::new(
        Shared(clock.clone()),
        AnalyzeLimits {
            per_table: Duration::from_secs(600),
            max_per_window: 2,
            window: Duration::from_secs(3600),
            ..AnalyzeLimits::default()
        },
    );
    let plan = |table: &str| AnalyzePlan {
        schema: "s".into(),
        table: table.into(),
        columns: vec!["a".into()],
        indexes: vec![],
        reason: AnalyzeReason::Missing,
    };
    assert!(th.admit(1, &plan("a")));
    assert!(!th.admit(1, &plan("a")), "same table too soon");
    assert!(th.admit(1, &plan("b")));
    assert!(!th.admit(1, &plan("c")), "tenant cap");
    assert!(th.admit(2, &plan("a")), "other tenant");
    clock.advance(Duration::from_secs(3601));
    assert!(th.admit(1, &plan("a")));
}

#[test]
fn bad_histograms_are_errors() {
    assert!(Histogram::parse(KeyClass::Numeric, b"{}").is_err());
    assert!(Histogram::parse(KeyClass::Numeric, br#"{"histogram_hb": []}"#).is_err());
    assert!(
        Histogram::parse(
            KeyClass::Numeric,
            br#"{"histogram_hb": [{"start": "x", "size": 1, "ndv": 1}]}"#
        )
        .is_err()
    );
}

proptest::proptest! {
    /// Point and range estimates stay within [0, 1], and a range over the
    /// whole histogram is its total.
    #[test]
    fn estimates_stay_in_bounds(
        sizes in proptest::collection::vec((1u32..1000, 1u64..50), 1..40),
        probe in -10.0f64..5000.0,
        width in 0.0f64..3000.0,
    ) {
        let total: f64 = sizes.iter().map(|(s, _)| f64::from(*s)).sum();
        let mut start = 0.0;
        let mut bs = Vec::new();
        for (s, ndv) in &sizes {
            bs.push((format!("{start}"), f64::from(*s) / total, *ndv));
            start += f64::from(*s);
        }
        let refs: Vec<(&str, f64, u64)> = bs.iter().map(|(a, b, c)| (a.as_str(), *b, *c)).collect();
        let h = Histogram::parse(KeyClass::Numeric, &hb(&refs, &format!("{start}"))).unwrap();
        let p = h.point(&Key::Num(probe));
        proptest::prop_assert!((0.0..=1.0 + 1e-9).contains(&p));
        let r = h.range(Some(&Key::Num(probe)), Some(&Key::Num(probe + width)));
        proptest::prop_assert!((0.0..=1.0).contains(&r));
        proptest::prop_assert!((h.range(None, None) - 1.0).abs() < 1e-6);
        // Widening a range never lowers it.
        let wider = h.range(Some(&Key::Num(probe - 1.0)), Some(&Key::Num(probe + width + 1.0)));
        proptest::prop_assert!(wider + 1e-12 >= r);
    }
}
