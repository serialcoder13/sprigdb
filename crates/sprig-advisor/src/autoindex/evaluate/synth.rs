//! Synthetic chapter 12 windows for the trigger and evaluation tests: a
//! fingerprint's runs, latency, probes, scans and samples, built directly
//! into [`FpWindowStats`].

#![allow(
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]

use std::collections::BTreeMap;
use std::sync::Arc;

use sprig_common::ids::{DbId, Fingerprint, ObjectId, TenantId};
use sprig_workload::fingerprint::{FpKind, LiteralKind};
use sprig_workload::histogram::bucket_of;
use sprig_workload::recorder::Stats;
use sprig_workload::shape::Shape;
use sprig_workload::window::{FpWindowStats, SampleValue, SampledLiterals, SampledSlot};

use super::ObjectMeta;
use crate::autoindex::candidates::{FpInput, Literal};
use crate::autoindex::stats::Param;

pub(crate) const TENANT: TenantId = TenantId(42);

/// A registry of this crate's `auto_index.*` keys only, so these tests
/// don't depend on which other crates declare keys in the test binary.
pub(crate) fn registry() -> &'static sprig_settings::Registry {
    static REG: std::sync::OnceLock<sprig_settings::Registry> = std::sync::OnceLock::new();
    REG.get_or_init(|| {
        sprig_settings::Registry::new(crate::autoindex::settings::ALL.iter().copied()).unwrap()
    })
}
pub(crate) const DB: DbId = DbId(1);

/// A window of `runs` runs of `id`, mean `mean_ms`, every run in the
/// histogram bucket of `p95_ms` (so the p95 is that bucket's upper bound,
/// at most 19% above), no probes.
pub(crate) fn win(
    id: u64,
    shape: Arc<Shape>,
    runs: u64,
    mean_ms: f64,
    p95_ms: f64,
) -> FpWindowStats {
    let mut stats = Stats {
        exec_count: runs,
        total_us: (mean_ms * 1000.0 * runs as f64).round() as u64,
        max_us: (p95_ms * 1000.0) as u64,
        ..Stats::default()
    };
    if runs > 0 {
        stats
            .latency_hist
            .add_bucket(bucket_of((p95_ms * 1000.0) as u64), runs);
    }
    FpWindowStats {
        tenant: TENANT,
        id: Fingerprint(id),
        kind: FpKind::Sql,
        text: Arc::from(format!("fp {id:016x}")),
        shape,
        caller: None,
        stats,
        samples: Vec::new(),
    }
}

/// Adds `samples` probed runs, each examining `examined` rows and
/// returning `returned`; every run returns `returned`.
pub(crate) fn probed(
    mut w: FpWindowStats,
    samples: u64,
    examined: f64,
    returned: f64,
) -> FpWindowStats {
    w.stats.examined_samples = samples;
    w.stats.rows_examined = (examined * samples as f64).round() as u64;
    w.stats.returned_sampled = (returned * samples as f64).round() as u64;
    w.stats.rows_returned = (returned * w.stats.exec_count as f64).round() as u64;
    w
}

/// Adds `runs` large-scan runs from `sessions` distinct sessions.
pub(crate) fn scans(mut w: FpWindowStats, runs: u64, sessions: u64) -> FpWindowStats {
    w.stats.scan_runs = runs;
    for s in 0..sessions {
        w.stats.scan_sessions.insert(0x5e55_0000 + s);
    }
    w
}

fn value(p: &Param) -> SampleValue {
    match p {
        Param::Num(x) => SampleValue {
            kind: LiteralKind::Number,
            text: format!("{x}").into_bytes(),
        },
        Param::Text(t) => SampleValue {
            kind: LiteralKind::String,
            text: format!("'{}'", t.replace('\'', "''")).into_bytes(),
        },
        Param::Null | Param::Weight(_) => SampleValue {
            kind: LiteralKind::Param,
            text: b"?".to_vec(),
        },
    }
}

/// A sample in chapter 12's form from the generator's literal list.
pub(crate) fn sampled(lits: &[Literal]) -> SampledLiterals {
    SampledLiterals {
        slots: lits
            .iter()
            .map(|l| match l {
                Literal::Value(p) => SampledSlot::One(value(p)),
                Literal::List(ps) => SampledSlot::List(ps.iter().map(value).collect()),
            })
            .collect(),
    }
}

/// The window an [`FpInput`] fixture summarizes: its runs, mean and
/// p95, `max(5, runs / 50)` probes at its rows examined and returned, its
/// rows affected, its longest `IN` list and its samples. `limit_slot` is
/// the slot of the sampled `LIMIT` (the fixtures' shapes don't name it).
pub(crate) fn from_input(f: &FpInput, limit_slot: Option<u32>) -> FpWindowStats {
    let mut shape = (*f.shape).clone();
    if limit_slot.is_some() {
        shape.limit_arg = limit_slot;
    }
    let mut w = win(f.id.0, Arc::new(shape), f.runs, f.mean_ms, f.p95_ms);
    w = probed(
        w,
        (f.runs / 50).max(5),
        f.examined_per_run,
        f.returned_per_run,
    );
    w.stats.rows_affected = (f.affected_per_run * f.runs as f64).round() as u64;
    w.stats.in_list_max = f.in_list_max.map_or(0, |n| n as u32);
    w.samples = f.samples.iter().map(|s| sampled(s)).collect();
    w
}

/// Catalog metadata: every object in database 1, with a validation node.
pub(crate) fn directory(objects: &[(ObjectId, Option<u64>)]) -> BTreeMap<ObjectId, ObjectMeta> {
    objects
        .iter()
        .map(|(o, bytes)| {
            (
                *o,
                ObjectMeta {
                    db: DB,
                    data_bytes: *bytes,
                    validation_node: true,
                    build_rate_bytes_per_sec: None,
                },
            )
        })
        .collect()
}
