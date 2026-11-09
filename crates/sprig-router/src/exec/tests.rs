#![allow(clippy::unwrap_used, clippy::panic, clippy::unnecessary_wraps)]

use super::*;

/// Columns: an INT, a VARCHAR under `utf8mb4_general_ci`, three weight
/// columns.
fn int_col(name: &str) -> ColumnDef {
    ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        charset: 63,
        col_type: 3,
        ..ColumnDef::default()
    }
}

fn str_col(name: &str) -> ColumnDef {
    ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        charset: 45,
        col_type: 253,
        ..ColumnDef::default()
    }
}

fn bin_col(name: &str) -> ColumnDef {
    ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        charset: 63,
        col_type: 253,
        ..ColumnDef::default()
    }
}

fn dec_col(name: &str, decimals: u8) -> ColumnDef {
    ColumnDef {
        name: Bytes::copy_from_slice(name.as_bytes()),
        charset: 63,
        col_type: 246,
        decimals,
        ..ColumnDef::default()
    }
}

/// `utf8mb4_general_ci` weights: uppercase ASCII, two bytes per char.
fn w(s: &str) -> Bytes {
    let mut out = Vec::new();
    for c in s.chars() {
        out.extend_from_slice(&[0, c.to_ascii_uppercase() as u8]);
    }
    Bytes::from(out)
}

fn v(s: &str) -> Option<Bytes> {
    Some(Bytes::copy_from_slice(s.as_bytes()))
}

struct Fake {
    answers: HashMap<String, ShardOutcome>,
    log: Vec<String>,
    in_txn: bool,
}

#[async_trait::async_trait]
impl ShardBackend for Fake {
    async fn run(
        &mut self,
        _target: &Target,
        sql: &str,
        budget: &mut Budget,
    ) -> Result<ShardOutcome> {
        self.log.push(sql.to_owned());
        budget.charge(1)?;
        self.answers
            .get(sql)
            .cloned()
            .ok_or_else(|| SprigError::internal(format!("no answer for {sql}")))
    }
    fn in_transaction(&self) -> bool {
        self.in_txn
    }
}

fn target(sql: &str) -> Target {
    Target {
        cluster: "c1".into(),
        shard: 1,
        sql: sql.into(),
        at: crate::plan::ShardAt::default(),
    }
}

fn rows(cols: Vec<ColumnDef>, rows: Vec<Row>) -> ShardOutcome {
    ShardOutcome::Rows(ShardRows {
        columns: cols,
        rows,
        warnings: 0,
    })
}

fn texts(o: &Output) -> Vec<Vec<String>> {
    match o {
        Output::Rows { rows, .. } => rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|c| {
                        c.as_ref()
                            .map_or("NULL".into(), |b| String::from_utf8_lossy(b).into_owned())
                    })
                    .collect()
            })
            .collect(),
        Output::Ok(_) | Output::Streamed { .. } => panic!("rows expected"),
    }
}

fn wcols() -> Vec<ColumnDef> {
    vec![
        int_col("id"),
        str_col("name"),
        bin_col("h1"),
        bin_col("h2"),
        bin_col("h3"),
        str_col("h4"),
    ]
}

fn wrow(id: &str, name: Option<&str>) -> Row {
    vec![
        v(id),
        name.map(|n| Bytes::copy_from_slice(n.as_bytes())),
        name.map(w),
        name.map(w),
        name.map(w),
        name.map(|_| Bytes::from_static(b"utf8mb4_general_ci")),
    ]
}

#[tokio::test]
async fn merge_orders_by_weights_with_nulls_first_and_pad_space() {
    let mut f = Fake {
        answers: HashMap::new(),
        log: Vec::new(),
        in_txn: false,
    };
    f.answers.insert(
        "s1".into(),
        rows(
            wcols(),
            vec![wrow("1", None), wrow("2", Some("b")), wrow("3", Some("D"))],
        ),
    );
    f.answers.insert(
        "s2".into(),
        rows(wcols(), vec![wrow("4", Some("a ")), wrow("5", Some("C"))]),
    );
    let key = |value, desc| SortKey {
        cols: KeyCols {
            value,
            weights: [
                ColRef::Hidden(0),
                ColRef::Hidden(1),
                ColRef::Hidden(2),
                ColRef::Hidden(3),
            ],
        },
        desc,
    };
    let plan = Plan::Read(ReadPlan {
        kind: ReadKind::Merge,
        targets: vec![target("s1"), target("s2")],
        hidden: 4,
        order: vec![key(ColRef::Visible(1), false)],
        agg: None,
        offset: 1,
        limit: Some(3),
        pushed_limit: Some(4),
        group: 9,
    });
    let out = execute(&plan, &mut f, &mut Budget::mb(1)).await.unwrap();
    // NULL, 'a ', 'b', 'C', 'D' → skip 1, take 3.
    assert_eq!(
        texts(&out),
        vec![vec!["4", "a "], vec!["2", "b"], vec!["5", "C"]]
    );
    if let Output::Rows { columns, .. } = &out {
        assert_eq!(columns.len(), 2);
    }
}

#[tokio::test]
async fn aggregates_combine_partials_exactly() {
    // SELECT g, COUNT(*), SUM(d), AVG(d), MIN(name) … GROUP BY g, with
    // hidden: g weights (3), SUM(d), COUNT(d), MIN weights (3).
    let cols = vec![
        str_col("g"),
        int_col("count"),
        dec_col("sum", 2),
        dec_col("avg", 6),
        str_col("min"),
        bin_col("gw1"),
        bin_col("gw2"),
        bin_col("gw3"),
        str_col("gcoll"),
        dec_col("hsum", 2),
        int_col("hcount"),
        bin_col("mw1"),
        bin_col("mw2"),
        bin_col("mw3"),
        str_col("mcoll"),
    ];
    let row = |g: &str, c: &str, s: &str, a: &str, m: &str, hc: &str| -> Row {
        vec![
            v(g),
            v(c),
            v(s),
            v(a),
            v(m),
            Some(w(g)),
            Some(w(g)),
            Some(w(g)),
            v("utf8mb4_general_ci"),
            v(s),
            v(hc),
            Some(w(m)),
            Some(w(m)),
            Some(w(m)),
            v("utf8mb4_general_ci"),
        ]
    };
    let mut f = Fake {
        answers: HashMap::new(),
        log: Vec::new(),
        in_txn: false,
    };
    f.answers.insert(
        "s1".into(),
        rows(
            cols.clone(),
            vec![
                row("x", "2", "3.50", "1.750000", "b", "2"),
                row("Y", "1", "1.00", "1.000000", "q", "1"),
            ],
        ),
    );
    f.answers.insert(
        "s2".into(),
        rows(cols, vec![row("X", "1", "0.25", "0.250000", "A", "1")]),
    );
    let wts = |a, b, c, d| {
        [
            ColRef::Hidden(a),
            ColRef::Hidden(b),
            ColRef::Hidden(c),
            ColRef::Hidden(d),
        ]
    };
    let agg = AggPlan {
        groups: vec![KeyCols {
            value: ColRef::Visible(0),
            weights: wts(0, 1, 2, 3),
        }],
        aggs: vec![
            AggCall {
                func: AggFunc::Count,
                value: ColRef::Visible(1),
                count: None,
                weights: None,
                scale_from: None,
            },
            AggCall {
                func: AggFunc::Sum,
                value: ColRef::Visible(2),
                count: None,
                weights: None,
                scale_from: None,
            },
            AggCall {
                func: AggFunc::Avg,
                value: ColRef::Hidden(4),
                count: Some(ColRef::Hidden(5)),
                weights: None,
                scale_from: Some(ColRef::Visible(3)),
            },
            AggCall {
                func: AggFunc::Min,
                value: ColRef::Visible(4),
                count: None,
                weights: Some(wts(6, 7, 8, 9)),
                scale_from: None,
            },
        ],
        outputs: vec![
            OutCol::First,
            OutCol::Agg(0),
            OutCol::Agg(1),
            OutCol::Agg(2),
            OutCol::Agg(3),
        ],
        having: Some(Pred::Cmp(
            CmpOp::Ge,
            Operand::Agg(0),
            Operand::Num("1".into()),
        )),
        order: Vec::new(),
    };
    let plan = Plan::Read(ReadPlan {
        kind: ReadKind::Aggregate,
        targets: vec![target("s1"), target("s2")],
        hidden: 10,
        order: Vec::new(),
        agg: Some(agg),
        offset: 0,
        limit: None,
        pushed_limit: None,
        group: 9,
    });
    let out = execute(&plan, &mut f, &mut Budget::mb(1)).await.unwrap();
    // x and X are one group under general_ci: 3 rows, 3.75, 1.250000, 'A'.
    assert_eq!(
        texts(&out),
        vec![
            vec!["x", "3", "3.75", "1.250000", "A"],
            vec!["Y", "1", "1.00", "1.000000", "q"],
        ]
    );
}

#[tokio::test]
async fn writes_sum_and_roll_back_on_error() {
    let mut f = Fake {
        answers: HashMap::new(),
        log: Vec::new(),
        in_txn: false,
    };
    for s in ["BEGIN", "COMMIT", "ROLLBACK"] {
        f.answers
            .insert(s.into(), ShardOutcome::Ok(ShardOk::default()));
    }
    let ok = |a: u64, info: &str| {
        ShardOutcome::Ok(ShardOk {
            affected: a,
            last_insert_id: 0,
            warnings: 0,
            info: info.into(),
        })
    };
    f.answers.insert(
        "u1".into(),
        ok(2, "Rows matched: 3  Changed: 2  Warnings: 0"),
    );
    f.answers.insert(
        "u2".into(),
        ok(1, "Rows matched: 1  Changed: 1  Warnings: 0"),
    );
    let plan = Plan::Write(WritePlan {
        kind: WriteKind::Update,
        targets: vec![target("u1"), target("u2")],
        rows: 0,
        claims: Vec::new(),
        releases: Vec::new(),
        fk: None,
    });
    let out = execute(&plan, &mut f, &mut Budget::mb(1)).await.unwrap();
    match out {
        Output::Ok(o) => {
            assert_eq!(o.affected, 3);
            assert_eq!(o.info, "Rows matched: 4  Changed: 3  Warnings: 0");
        }
        Output::Rows { .. } | Output::Streamed { .. } => panic!(),
    }
    assert_eq!(f.log, ["BEGIN", "u1", "u2", "COMMIT"]);
    // A failing shard rolls everything back.
    f.log.clear();
    let plan = Plan::Write(WritePlan {
        kind: WriteKind::Delete,
        targets: vec![target("u1"), target("missing")],
        rows: 0,
        claims: Vec::new(),
        releases: Vec::new(),
        fk: None,
    });
    assert!(execute(&plan, &mut f, &mut Budget::mb(1)).await.is_err());
    assert_eq!(f.log, ["BEGIN", "u1", "missing", "ROLLBACK"]);
}

#[test]
fn doubles_print_like_mariadb_for_common_values() {
    assert_eq!(format_f64(2.5), "2.5");
    assert_eq!(format_f64(3.0), "3");
    assert_eq!(format_f64(0.0), "0");
    assert_eq!(format_f64(1e20), "1e20");
}

/// A backend whose reads stream: every shard answers from `answers`.
struct Streams {
    answers: HashMap<String, ShardOutcome>,
}

struct OneStream {
    answers: HashMap<String, ShardOutcome>,
    rows: std::collections::VecDeque<Row>,
}

#[async_trait::async_trait]
impl ShardStream for OneStream {
    async fn start(&mut self, sql: &str) -> Result<Option<Vec<ColumnDef>>> {
        match self.answers.get(sql).cloned() {
            Some(ShardOutcome::Rows(r)) => {
                self.rows = r.rows.into();
                Ok(Some(r.columns))
            }
            Some(ShardOutcome::Ok(_)) => Ok(None),
            None => Err(SprigError::backend(1146, *b"42S02", "Table doesn't exist")),
        }
    }
    async fn next_row(&mut self) -> Result<Option<Row>> {
        // Interleave the streams.
        tokio::task::yield_now().await;
        Ok(self.rows.pop_front())
    }
    fn warnings(&self) -> u16 {
        1
    }
}

#[async_trait::async_trait]
impl ShardBackend for Streams {
    async fn run(&mut self, _t: &Target, sql: &str, _b: &mut Budget) -> Result<ShardOutcome> {
        self.answers
            .get(sql)
            .cloned()
            .ok_or_else(|| SprigError::internal("no answer"))
    }
    fn in_transaction(&self) -> bool {
        false
    }
    async fn open_stream(&mut self, _c: &str) -> Result<Option<Box<dyn ShardStream>>> {
        Ok(Some(Box::new(OneStream {
            answers: self.answers.clone(),
            rows: std::collections::VecDeque::new(),
        })))
    }
    fn max_parallel(&self) -> usize {
        2
    }
}

#[derive(Default)]
struct Collect {
    cols: usize,
    rows: Vec<Row>,
}

#[async_trait::async_trait]
impl RowSink for Collect {
    async fn columns(&mut self, c: &[ColumnDef]) -> Result<()> {
        self.cols = c.len();
        Ok(())
    }
    async fn row(&mut self, r: &[Option<Bytes>]) -> Result<()> {
        self.rows.push(r.to_vec());
        Ok(())
    }
}

fn scatter(n: usize, offset: u64, limit: Option<u64>) -> Plan {
    Plan::Read(ReadPlan {
        kind: ReadKind::Scatter,
        targets: (0..n).map(|i| target(&format!("s{i}"))).collect(),
        hidden: 0,
        order: Vec::new(),
        agg: None,
        offset,
        limit,
        pushed_limit: None,
        group: 1,
    })
}

#[tokio::test]
async fn scatters_stream_from_parallel_shards_with_offset_and_limit() {
    let mut answers = HashMap::new();
    for i in 0..5 {
        answers.insert(
            format!("s{i}"),
            rows(
                vec![int_col("id")],
                (0..40)
                    .map(|j| vec![v(&format!("{}", i * 100 + j))])
                    .collect(),
            ),
        );
    }
    let mut be = Streams { answers };
    let mut sink = Collect::default();
    let out = execute_to(&scatter(5, 0, None), &mut be, &mut Budget::mb(1), &mut sink)
        .await
        .unwrap();
    assert_eq!(out, Output::Streamed { warnings: 5 });
    assert_eq!(sink.cols, 1);
    let mut got: Vec<String> = sink
        .rows
        .iter()
        .map(|r| String::from_utf8_lossy(r[0].as_ref().unwrap()).into_owned())
        .collect();
    got.sort();
    let mut want: Vec<String> = (0..5)
        .flat_map(|i| (0..40).map(move |j| format!("{}", i * 100 + j)))
        .collect();
    want.sort();
    assert_eq!(got, want);
    // OFFSET and LIMIT apply in the gateway; the rest is drained.
    let mut sink = Collect::default();
    execute_to(
        &scatter(5, 10, Some(25)),
        &mut be,
        &mut Budget::mb(1),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(sink.rows.len(), 25);
    // A gathered merge over parallel streams runs out of budget: 39422.
    let mut big = HashMap::new();
    for i in 0..3 {
        big.insert(
            format!("s{i}"),
            rows(
                vec![int_col("id")],
                (0..20_000).map(|_| vec![v(&"x".repeat(40))]).collect(),
            ),
        );
    }
    let mut be = Streams { answers: big };
    let plan = Plan::Read(ReadPlan {
        kind: ReadKind::Aggregate,
        ..match scatter(3, 0, None) {
            Plan::Read(r) => r,
            _ => unreachable!(),
        }
    });
    let e = execute(&plan, &mut be, &mut Budget::mb(1))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::RouterMemoryLimit));
    // A shard error ends the stream with that error.
    let mut be = Streams {
        answers: HashMap::new(),
    };
    let mut sink = Collect::default();
    assert!(
        execute_to(&scatter(3, 0, None), &mut be, &mut Budget::mb(1), &mut sink)
            .await
            .is_err()
    );
}
