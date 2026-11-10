#![allow(clippy::unwrap_used, clippy::float_cmp, clippy::many_single_char_names)]

use proptest::prelude::*;

use super::*;
use crate::autoindex::fixtures::{
    DOC_ORDERS, EVENTS, INVOICES, ORDERS, doc_orders, events, events_fps, fp_example1, fp_example4,
    invoices, invoices_fps, orders,
};
use crate::autoindex::model::Origin;

fn cols(c: &Candidate) -> Vec<&str> {
    c.def.columns().collect()
}

fn find<'a>(cands: &'a [Candidate], want: &[&str]) -> &'a Candidate {
    cands.iter().find(|c| cols(c) == want).unwrap_or_else(|| {
        panic!(
            "no candidate {want:?} in {:?}",
            cands.iter().map(cols).collect::<Vec<_>>()
        )
    })
}

#[test]
fn example_1_generates_both_variants() {
    let t = orders();
    let f = fp_example1();
    let g = Generator::default();
    let acc = access(&f, "o", ORDERS, &t);
    // status IN (2 values) is an equality that breaks the order after it.
    let status = acc.pred("status").unwrap();
    assert_eq!(status.kind, PredKind::InList);
    assert!((status.sel - 0.07).abs() < 1e-9);
    assert_eq!(acc.order, [("created_at".to_owned(), SortDir::Desc)]);
    assert_eq!(acc.limit, Some(20.0));

    let cands = g.for_table(&acc, &t);
    // A: equality columns, most selective first, then the order column.
    let a = find(&cands, &["customer_id", "status", "created_at"]);
    assert!(!a.covering);
    assert_eq!(
        a.evidence.parts,
        [PartSource::Eq, PartSource::Eq, PartSource::Order]
    );
    // e = 48M × 8.3e-6 × 0.07 ≈ 28: A doesn't deliver the order (IN), so
    // no early stop.
    let ua = usage(&a.def.parts, &acc, &t).unwrap();
    assert!((ua.entries - 28.0).abs() < 0.1, "{}", ua.entries);
    assert!(!ua.delivers_order);
    // B: equality without the IN list, then the order: LIMIT 20 / 0.07.
    let b = find(&cands, &["customer_id", "created_at"]);
    let ub = usage(&b.def.parts, &acc, &t).unwrap();
    assert!(ub.delivers_order);
    assert!((ub.entries - 20.0 / 0.07).abs() < 0.5, "{}", ub.entries);
    // A reads 28 rows, under the 50 of add_covering, so the chapter's A'
    // isn't generated here (see cost tests, which build it).
    assert!(
        !cands
            .iter()
            .any(|c| cols(c) == ["customer_id", "status", "created_at", "total"])
    );
    // Every candidate's name is stable.
    assert!(b.def.name().starts_with("sprig_auto_"));
}

#[test]
fn example_2_merges_kept_order_and_explains_the_unsargable() {
    let t = events();
    let [f1, f2, f3, f4] = events_fps();
    let g = Generator::default();
    let gen_of = |f: &FpInput| g.generate_all(f, &|o| (o == EVENTS).then_some(&t));
    let c1 = gen_of(&f1).candidates;
    let c2 = gen_of(&f2).candidates;
    let c3 = gen_of(&f3).candidates;
    let g4 = gen_of(&f4);
    // F1: range = order column.
    let a1 = find(&c1, &["account_id", "kind", "created_at"]);
    assert_eq!(
        a1.evidence.parts,
        [PartSource::Eq, PartSource::Eq, PartSource::Range]
    );
    // payload is JSON: no covering variant.
    assert_eq!(c1.len(), 1);
    // F2: an index-only count.
    let a2 = find(&c2, &["account_id", "kind"]);
    assert!(a2.covering);
    // F3: primary-key order inside one account.
    let a3 = find(&c3, &["account_id"]);
    assert!(a3.evidence.pk_order);
    // F4: no candidate, with the reason.
    assert!(g4.candidates.is_empty());
    assert_eq!(g4.skipped[0].reason, "function:LOWER");

    let merged = merge([c1, c2, c3].concat());
    assert_eq!(merged.len(), 2, "{merged:?}");
    let m = find(&merged, &["account_id", "kind", "created_at"]);
    assert_eq!(
        m.serves,
        [
            (Fingerprint(0xF1), Role::Primary),
            (Fingerprint(0xF2), Role::Merged)
        ]
    );
    let alone = find(&merged, &["account_id"]);
    assert_eq!(alone.serves, [(Fingerprint(0xF3), Role::Primary)]);

    // With the merged index F3 reads an account's 2,000 rows (no order);
    // with its own, 20.
    let acc3 = access(&f3, "e", EVENTS, &t);
    let um = usage(&m.def.parts, &acc3, &t).unwrap();
    assert!(!um.delivers_order);
    assert!((um.entries - 2000.0).abs() < 1.0);
    let uo = usage(&alone.def.parts, &acc3, &t).unwrap();
    assert!(uo.delivers_order);
    assert_eq!(uo.entries, 20.0);
}

#[test]
fn example_3_covering_retry() {
    let t = invoices();
    let [f5, f6] = invoices_fps();
    let g = Generator::default();
    let alias = "billing.invoices";
    let acc5 = access(&f5, alias, INVOICES, &t);
    let c5 = g.for_table(&acc5, &t);
    // C1 (state, due_date): eq, then the range that is also the order.
    let c1 = find(&c5, &["state", "due_date"]);
    assert_eq!(c1.evidence.parts, [PartSource::Eq, PartSource::Range]);
    let s = acc5.pred("state").unwrap();
    assert!((s.sel - 0.005).abs() < 1e-9, "overdue at 0.5%");
    // F5 stops after 20 rows, so no covering variant up front.
    assert_eq!(c5.len(), 1);

    // F6 regressed with C1 through primary-key lookups: the covering
    // retry adds F6's other column.
    let acc6 = access(&f6, alias, INVOICES, &t);
    let u6 = usage(&c1.def.parts, &acc6, &t);
    // C1 can serve F6 only as a full scan in state order: grouping.
    assert!(u6.is_some_and(|u| u.delivers_group && !u.covering));
    let c2 = g.extend_covering(c1, &acc6, &t).unwrap();
    assert_eq!(cols(&c2), ["state", "due_date", "amount"]);
    assert!(c2.covering);
    assert!(usage(&c2.def.parts, &acc6, &t).unwrap().covering);
    // 72 MB more: 9M rows × avg_length(amount) 6 / 0.75.
    let extra = 9e6 * t.avg_length("amount") / 0.75;
    assert!((extra - 72e6).abs() < 1.0);

    // F6's own candidates: the range alone, and ordered grouping.
    let c6 = g.for_table(&acc6, &t);
    find(&c6, &["state"]);
}

#[test]
fn example_4_field_columns() {
    let t = doc_orders();
    let f = fp_example4();
    let g = Generator::default();
    let out = g.generate_all(&f, &|o| (o == DOC_ORDERS).then_some(&t));
    // status stays a JSON filter; the order is a field column.
    assert_eq!(out.candidates.len(), 1, "{:?}", out.candidates);
    assert_eq!(
        cols(&out.candidates[0]),
        ["_sprig_p_customer_id", "_sprig_p_createdat"]
    );
}

fn existing(name: &str, cols: &[&str]) -> IndexInfo {
    IndexInfo {
        name: name.into(),
        parts: cols.iter().map(|c| KeyPart::col(c)).collect(),
        unique: false,
        origin: Origin::User,
        ignored: false,
        usable: true,
    }
}

#[test]
fn coverage_by_existing_indexes() {
    let t = events();
    let [f1, _, f3, _] = events_fps();
    let g = Generator::default();
    let c1 = g.for_table(&access(&f1, "e", EVENTS, &t), &t).remove(0);
    // Equalities in any order, then the same columns.
    let e = [existing(
        "idx_k_a_c_x",
        &["kind", "account_id", "created_at", "x"],
    )];
    assert_eq!(covered_by(&c1, &e).unwrap().name, "idx_k_a_c_x");
    // The range column must follow the equalities.
    let e = [existing("idx_a_c_k", &["account_id", "created_at", "kind"])];
    assert!(covered_by(&c1, &e).is_none());
    // An IGNORED or still-validating automatic index doesn't count.
    let mut ign = existing("idx_k_a_c", &["kind", "account_id", "created_at"]);
    ign.ignored = true;
    assert!(covered_by(&c1, &[ign.clone()]).is_none());
    ign.ignored = false;
    ign.origin = Origin::Auto;
    ign.usable = false;
    assert!(covered_by(&c1, &[ign]).is_none());
    // A primary-key-order candidate needs exactly its parts.
    let c3 = g.for_table(&access(&f3, "e", EVENTS, &t), &t).remove(0);
    assert!(covered_by(&c3, &[existing("idx_a_k", &["account_id", "kind"])]).is_none());
    assert!(covered_by(&c3, &[existing("idx_a", &["account_id"])]).is_some());
    // A prefix index doesn't cover a whole column.
    let mut p = existing("idx_a_p", &["kind", "account_id", "created_at"]);
    p.parts[0].prefix = Some(4);
    assert!(covered_by(&c1, &[p]).is_none());
}

#[test]
fn example_1_existing_user_index_is_not_enough() {
    // idx_created (created_at) serves the order only: it doesn't cover A
    // or B.
    let t = orders();
    let f = fp_example1();
    let cands = Generator::default().for_table(&access(&f, "o", ORDERS, &t), &t);
    for c in &cands {
        assert!(covered_by(c, &t.indexes).is_none(), "{:?}", cols(c));
    }
}

#[test]
fn joins_and_writes() {
    let t = orders();
    let f = FpInput {
        shape: crate::autoindex::fixtures::shape(&serde_json::json!({
            "v": 1, "type": "update",
            "tables": [{"alias": "o", "object_id": ORDERS.0, "table": "orders"},
                       {"alias": "c", "object_id": 99, "table": "customers"}],
            "predicates": [{"t": "c", "col": "region", "op": "eq", "args": [0]}],
            "joins": [{"l": ["o", "customer_id"], "r": ["c", "id"], "op": "eq",
                       "kind": "inner"}],
            "order_by": [], "group_by": [], "select": {"o": ["total"]},
            "limit": false,
            "writes": {"kind": "update", "targets": ["o"],
                       "set": [{"t": "o", "col": "total"}]},
            "or_groups": 0})),
        ..fp_example1()
    };
    let acc = access(&f, "o", ORDERS, &t);
    // The join column is an equality for lookups into orders.
    assert_eq!(acc.pred("customer_id").unwrap().kind, PredKind::Eq);
    // An UPDATE reads the whole row: never covered.
    assert!(acc.whole_row);
    let cands = Generator::default().for_table(&acc, &t);
    assert_eq!(cands.len(), 1);
    assert_eq!(cols(&cands[0]), ["customer_id"]);
    assert!(!cands[0].covering);
}

#[test]
fn long_in_lists_are_ranges_and_scans_are_dropped() {
    let t = orders();
    let mut f = fp_example1();
    f.samples.clear();
    f.in_list_max = Some(40);
    let acc = access(&f, "o", ORDERS, &t);
    assert_eq!(acc.pred("status").unwrap().kind, PredKind::Range);
    // A lone status = 'paid' reads 90% of the table and gives no order:
    // dropped.
    let g = FpInput {
        shape: crate::autoindex::fixtures::shape(&serde_json::json!({
            "v": 1, "type": "select",
            "tables": [{"alias": "o", "object_id": ORDERS.0, "table": "orders"}],
            "predicates": [{"t": "o", "col": "status", "op": "eq", "args": [0]}],
            "joins": [], "order_by": [], "group_by": [], "select": {"o": ["*"]},
            "limit": false, "writes": null, "or_groups": 0})),
        samples: vec![vec![Literal::Value(Param::Text("paid".into()))]],
        ..fp_example1()
    };
    let acc = access(&g, "o", ORDERS, &t);
    assert!(acc.pred("status").unwrap().sel > 0.8);
    assert!(Generator::default().for_table(&acc, &t).is_empty());
}

#[test]
fn mixed_directions_get_descending_parts() {
    let t = orders();
    let f = FpInput {
        shape: crate::autoindex::fixtures::shape(&serde_json::json!({
            "v": 1, "type": "select",
            "tables": [{"alias": "o", "object_id": ORDERS.0, "table": "orders"}],
            "predicates": [{"t": "o", "col": "customer_id", "op": "eq", "args": [0]}],
            "joins": [],
            "order_by": [{"t": "o", "col": "created_at", "dir": "desc"},
                         {"t": "o", "col": "total", "dir": "asc"}],
            "group_by": [], "select": {"o": ["*"]},
            "limit": true, "writes": null, "or_groups": 0})),
        ..fp_example1()
    };
    let acc = access(&f, "o", ORDERS, &t);
    let c = Generator::default().for_table(&acc, &t).remove(0);
    assert_eq!(
        c.def.canonical(),
        "btree(customer_id,created_at desc,total)"
    );
    assert!(usage(&c.def.parts, &acc, &t).unwrap().delivers_order);
}

fn arb_candidate() -> impl Strategy<Value = Candidate> {
    let col = prop::sample::select(vec!["a", "b", "c", "d", "e"]);
    (
        prop::collection::vec(col, 1..5),
        any::<bool>(),
        any::<bool>(),
        0u64..4,
    )
        .prop_map(|(cs, covering, pk, fp)| {
            let mut seen = BTreeSet::new();
            let cs: Vec<&str> = cs.into_iter().filter(|c| seen.insert(*c)).collect();
            Candidate {
                def: IndexDef::btree(ObjectId(1), &cs),
                covering,
                serves: vec![(Fingerprint(fp), Role::Primary)],
                evidence: Evidence {
                    parts: vec![PartSource::Eq; cs.len()],
                    pk_order: pk,
                    ..Evidence::default()
                },
            }
        })
}

proptest! {
    /// Merging is idempotent, keeps every fingerprint served, and leaves
    /// no mergeable pair behind.
    #[test]
    fn merge_is_idempotent(cands in prop::collection::vec(arb_candidate(), 0..12)) {
        let once = merge(cands.clone());
        let twice = merge(once.clone());
        prop_assert_eq!(&once, &twice);
        let before: BTreeSet<u64> = cands.iter().flat_map(|c| c.serves.iter().map(|s| s.0.0)).collect();
        let after: BTreeSet<u64> = once.iter().flat_map(|c| c.serves.iter().map(|s| s.0.0)).collect();
        prop_assert_eq!(before, after);
        for c in &once {
            for d in &once {
                if c.def.parts.len() < d.def.parts.len() {
                    prop_assert!(!c.def.is_prefix_of(&d.def) || c.evidence.pk_order);
                }
            }
        }
    }

    /// Generation never emits a key over max_key_bytes or with more than
    /// max_cover_columns added columns.
    #[test]
    fn covering_respects_limits(max_cover in 0usize..4, max_bytes in 16u64..600) {
        let t = orders();
        let f = fp_example1();
        let g = Generator::new(GenSettings { max_cover_columns: max_cover, max_key_bytes: max_bytes });
        let acc = access(&f, "o", ORDERS, &t);
        for c in g.for_table(&acc, &t) {
            let cover = c.evidence.parts.iter().filter(|p| **p == PartSource::Cover).count();
            prop_assert!(cover <= max_cover);
            if cover > 0 {
                prop_assert!(key_bytes(&c.def.parts, &t) <= max_bytes);
            }
        }
    }
}

/// R6 round 3: spatial and vector candidates (chapter 13, "Other models").
#[test]
#[allow(clippy::too_many_lines)]
fn spatial_and_vector_candidates() {
    use crate::autoindex::stats::ColumnInfo;
    let mut t = TableStats {
        schema: "t1_d1_g1_s1".into(),
        table: "place".into(),
        n_rows: Some(50_000),
        cardinality: Some(50_000),
        ..TableStats::default()
    };
    for (name, ty, nullable) in [
        ("loc", "point", false),
        ("area", "polygon", true),
        ("emb", "vector", false),
    ] {
        t.columns.insert(
            name.into(),
            ColumnInfo {
                name: name.into(),
                data_type: ty.into(),
                class: None,
                charset: None,
                collation: None,
                nullable,
                max_bytes: None,
                lob: true,
                generated: false,
            },
        );
    }
    let object = ObjectId(77);
    let f = |preds: serde_json::Value, limit: bool| FpInput {
        id: Fingerprint(7),
        shape: crate::autoindex::fixtures::shape(&serde_json::json!({
            "v": 1, "type": "select",
            "tables": [{"alias": "p", "object_id": 77, "table": "place"}],
            "predicates": preds, "joins": [], "order_by": [], "group_by": [],
            "select": {"p": ["id"]}, "limit": limit, "writes": null, "or_groups": 0})),
        runs: 1000,
        mean_ms: 400.0,
        p95_ms: 600.0,
        examined_per_run: 50_000.0,
        returned_per_run: 20.0,
        affected_per_run: 0.0,
        limit: Some(10),
        in_list_max: None,
        samples: Vec::new(),
        fast_only: false,
    };
    // A geodesic distance on a NOT NULL point: a spatial index.
    let c = model_candidates(
        &f(
            serde_json::json!([{"t": "p", "col": "loc", "op": "unsargable", "why": "function:ST_DISTANCE"}]),
            false,
        ),
        "p",
        object,
        &t,
    );
    assert_eq!(c.len(), 1);
    assert_eq!(c[0].def.kind, IndexKind::Spatial);
    assert_eq!(
        c[0].def.add_clause(),
        format!("SPATIAL INDEX IF NOT EXISTS `{}` (`loc`)", c[0].def.name())
    );
    // A nullable polygon: none (it would need the companion box).
    assert!(
        model_candidates(
            &f(
                serde_json::json!([{"t": "p", "col": "area", "op": "spatial"}]),
                false
            ),
            "p",
            object,
            &t
        )
        .is_empty()
    );
    // Ordering by a cosine distance with a LIMIT: a cosine vector index.
    let v = model_candidates(
        &f(
            serde_json::json!([
                {"t": "p", "col": "emb", "op": "vector"},
                {"t": "p", "col": "emb", "op": "unsargable", "why": "function:VEC_DISTANCE_COSINE"}]),
            true,
        ),
        "p",
        object,
        &t,
    );
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].def.kind, IndexKind::Vector);
    assert!(
        v[0].def
            .add_clause()
            .ends_with("(`emb`) M=8 DISTANCE=cosine"),
        "{}",
        v[0].def.add_clause()
    );
    // The cost model predicts the run reads about the rows it returns.
    let ctx_inputs = vec![f(
        serde_json::json!([{"t": "p", "col": "loc", "op": "spatial"}]),
        false,
    )];
    let ctx = crate::autoindex::cost::TableContext::new(
        object,
        &t,
        &ctx_inputs,
        crate::autoindex::cost::CostParams::default(),
    );
    let e =
        crate::autoindex::cost::CostModel::estimate(&crate::autoindex::cost::Model, &c[0], &ctx);
    assert!(e.benefit_ms_h > 0.0, "{e:?}");
}
