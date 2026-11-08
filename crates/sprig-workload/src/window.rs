//! Window statistics: one fingerprint's counters merged over a time window,
//! as the workload store returns them (chapter 12, `WorkloadStore::window`)
//! and automatic indexing reads them (chapter 13, "Cost model").
//!
//! [`FpWindowStats`] carries the merged [`Stats`] plus decrypted parameter
//! samples ([`SampledLiterals`]); its accessors give what `sprig-advisor`'s
//! `FpInput` needs: runs, mean and p95 latency, rows examined, returned and
//! affected per run, the longest `IN` list, the samples' literal lists (an
//! `IN` list as one list) and the sampled `LIMIT` value.
//!
//! The store ([`crate::store::WorkloadStore::window`]) builds it from the
//! catalog's rows over a window; [`crate::recorder::Recorder::window`]
//! builds the same view from one gateway's open buckets.

use std::sync::Arc;

use sprig_common::ids::TenantId;

use crate::fingerprint::{Caller, Fingerprint, FingerprintId, FpKind, Literal, LiteralKind};
use crate::recorder::{FlushRow, FpMeta, Stats};
use crate::shape::Shape;

/// One sampled value: its type and its SQL text (a literal as written,
/// with its sign, quotes and introducer; a bound parameter rendered as a
/// literal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampleValue {
    /// The literal's type. [`LiteralKind::Param`] only when the bound
    /// value was not supplied.
    pub kind: LiteralKind,
    /// Its SQL text.
    pub text: Vec<u8>,
}

impl SampleValue {
    /// The value as an unsigned integer, when it is one (`LIMIT 20`).
    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        let t = std::str::from_utf8(&self.text).ok()?.trim();
        let t = t.strip_prefix('+').unwrap_or(t);
        t.parse().ok()
    }

    /// The value as a number, when it is one.
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        if self.kind != LiteralKind::Number {
            return None;
        }
        let t = std::str::from_utf8(&self.text).ok()?;
        t.split_whitespace().collect::<String>().parse().ok()
    }

    /// The value of a string literal (quotes and escapes undone; a charset
    /// introducer or `DATE` keyword dropped). `None` for other kinds.
    #[must_use]
    pub fn string_value(&self, no_backslash_escapes: bool) -> Option<Vec<u8>> {
        if !matches!(self.kind, LiteralKind::String | LiteralKind::Temporal) {
            return None;
        }
        let q = self.text.iter().position(|b| matches!(b, b'\'' | b'"'))?;
        sprig_sql::lex::string_value(&self.text[q..], no_backslash_escapes)
    }
}

/// One slot's sampled value: one literal, or a collapsed list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SampledSlot {
    /// A `?` slot.
    One(SampleValue),
    /// An `IN (...)` or `VALUES (...)` slot: every value, in order.
    List(Vec<SampleValue>),
}

/// One execution's literals by slot: what a parameter sample holds once
/// decrypted. `slots[k]` fills slot `k` of the normalized text, so a
/// predicate's `args` index it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SampledLiterals {
    /// The slots, in text order.
    pub slots: Vec<SampledSlot>,
}

impl SampledLiterals {
    /// The literals of one execution, from the statement text and its
    /// fingerprint. `params` are the bound values of its `?` placeholders,
    /// in order, rendered as literals (empty for text statements); a
    /// placeholder without one stays [`LiteralKind::Param`].
    #[must_use]
    pub fn from_execution(stmt: &[u8], fp: &Fingerprint, params: &[SampleValue]) -> Self {
        let mut slots: Vec<SampledSlot> = Vec::new();
        let mut last_slot: Option<u32> = None;
        for l in &fp.literals {
            let v = value_of(stmt, l, params);
            let list = fp.list_slots.binary_search(&l.slot).is_ok();
            if last_slot == Some(l.slot) {
                if let Some(SampledSlot::List(vs)) = slots.last_mut() {
                    vs.push(v);
                }
                continue;
            }
            // Slots with no literal (a collapsed list of NULLs) stay empty.
            while u32::try_from(slots.len()).unwrap_or(u32::MAX) < l.slot {
                slots.push(SampledSlot::List(Vec::new()));
            }
            slots.push(if list {
                SampledSlot::List(vec![v])
            } else {
                SampledSlot::One(v)
            });
            last_slot = Some(l.slot);
        }
        Self { slots }
    }

    /// The value of slot `k`.
    #[must_use]
    pub fn slot(&self, k: u32) -> Option<&SampledSlot> {
        self.slots.get(usize::try_from(k).ok()?)
    }
}

fn value_of(stmt: &[u8], l: &Literal, params: &[SampleValue]) -> SampleValue {
    if l.kind == LiteralKind::Param
        && let Some(p) = params.get(l.param_index as usize)
    {
        return p.clone();
    }
    SampleValue {
        kind: l.kind,
        text: l.text(stmt).to_vec(),
    }
}

/// One fingerprint over a window (chapter 12, `FpWindowStats`): merged
/// buckets with their percentiles, and its decrypted samples.
#[derive(Debug, Clone)]
pub struct FpWindowStats {
    /// The tenant.
    pub tenant: TenantId,
    /// The fingerprint.
    pub id: FingerprintId,
    /// Its kind.
    pub kind: FpKind,
    /// The normalized text.
    pub text: Arc<str>,
    /// The shape.
    pub shape: Arc<Shape>,
    /// The logic function or Studio caller, if any.
    pub caller: Option<Caller>,
    /// The counters, summed over the window (maxima kept, histograms and
    /// session sets merged).
    pub stats: Stats,
    /// Parameter samples (empty without `workload.capture_samples`).
    pub samples: Vec<SampledLiterals>,
}

#[allow(clippy::cast_precision_loss)] // counters far below 2^52
impl FpWindowStats {
    /// Merges a fingerprint's rows (any buckets, gateways and parts).
    #[must_use]
    pub fn from_rows(tenant: TenantId, meta: &FpMeta, rows: &[FlushRow]) -> Self {
        let mut stats = Stats::default();
        for r in rows
            .iter()
            .filter(|r| r.fingerprint == meta.id && r.tenant == tenant)
        {
            stats.merge(&r.stats);
        }
        Self {
            tenant,
            id: meta.id,
            kind: meta.kind,
            text: Arc::clone(&meta.text),
            shape: Arc::clone(&meta.shape),
            caller: meta.caller,
            stats,
            samples: Vec::new(),
        }
    }

    /// Executions in the window (`n_f`).
    #[must_use]
    pub fn runs(&self) -> u64 {
        self.stats.exec_count
    }

    /// Mean latency, ms (`t_f`).
    #[must_use]
    pub fn mean_ms(&self) -> f64 {
        if self.stats.exec_count == 0 {
            return 0.0;
        }
        self.stats.total_us as f64 / self.stats.exec_count as f64 / 1000.0
    }

    /// p95 latency, ms: the upper bound of its histogram bucket (at most
    /// 19% high, never low). 0 without runs.
    #[must_use]
    pub fn p95_ms(&self) -> f64 {
        self.quantile_ms(0.95)
    }

    /// Any quantile, ms.
    #[must_use]
    pub fn quantile_ms(&self, q: f64) -> f64 {
        self.stats
            .latency_hist
            .quantile_us(q)
            .map_or(0.0, |us| us as f64 / 1000.0)
    }

    /// Rows examined per run (`x_f`), over the probed runs. `None` without
    /// probes.
    #[must_use]
    pub fn examined_per_run(&self) -> Option<f64> {
        (self.stats.examined_samples > 0)
            .then(|| self.stats.rows_examined as f64 / self.stats.examined_samples as f64)
    }

    /// Rows returned per run (`r_f`), over all runs.
    #[must_use]
    pub fn returned_per_run(&self) -> f64 {
        per_run(self.stats.rows_returned, self.stats.exec_count)
    }

    /// Rows affected per run (writes), over all runs.
    #[must_use]
    pub fn affected_per_run(&self) -> f64 {
        per_run(self.stats.rows_affected, self.stats.exec_count)
    }

    /// Rows examined per row returned over the probed runs (chapter 13's
    /// `top_total_time` trigger): `rows_examined / max(1, returned_sampled)`.
    #[must_use]
    pub fn examined_per_returned(&self) -> Option<f64> {
        (self.stats.examined_samples > 0)
            .then(|| self.stats.rows_examined as f64 / self.stats.returned_sampled.max(1) as f64)
    }

    /// The longest `IN` list seen, in elements. `None` when the shape has
    /// no list predicate and none was seen.
    #[must_use]
    pub fn in_list_max(&self) -> Option<u64> {
        (self.stats.in_list_max > 0).then_some(u64::from(self.stats.in_list_max))
    }

    /// The `LIMIT` row count from the samples: the largest sampled value
    /// (larger is the conservative choice for the cost model). `None`
    /// without a `LIMIT` slot or samples.
    #[must_use]
    pub fn sampled_limit(&self) -> Option<u64> {
        let slot = self.shape.limit_arg?;
        self.samples
            .iter()
            .filter_map(|s| match s.slot(slot)? {
                SampledSlot::One(v) => v.as_u64(),
                SampledSlot::List(_) => None,
            })
            .max()
    }

    /// Total time in the window, ms (for the top-N by total time).
    #[must_use]
    pub fn total_ms(&self) -> f64 {
        self.stats.total_us as f64 / 1000.0
    }
}

#[allow(clippy::cast_precision_loss)]
fn per_run(n: u64, runs: u64) -> f64 {
    if runs == 0 {
        0.0
    } else {
        n as f64 / runs as f64
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sprig_common::ids::{DbId, TenantId};
    use sprig_common::time::ManualClock;
    use sprig_proto_mysql::packets::Persona;

    use super::*;
    use crate::fingerprint::{SqlCtx, SqlFingerprinter};
    use crate::recorder::{ExecSummary, Recorder, RecorderConfig};
    use crate::shape::NoResolver;

    #[test]
    #[allow(clippy::many_single_char_names)]
    fn samples_keep_in_lists_as_lists_and_give_the_limit() {
        let f = SqlFingerprinter::default();
        let ctx = SqlCtx::new(Persona::MySql84).current_db("shop", DbId(1));
        let sql = b"SELECT * FROM orders WHERE customer_id = -42 AND status IN ('open', ?) ORDER BY id LIMIT 20";
        let fp = f.sql(TenantId(1), sql, &ctx, &NoResolver);
        assert_eq!(fp.shape.limit_arg, Some(2));
        let bound = SampleValue {
            kind: LiteralKind::String,
            text: b"'hold'".to_vec(),
        };
        let s = SampledLiterals::from_execution(sql, &fp, std::slice::from_ref(&bound));
        assert_eq!(s.slots.len(), 3);
        let SampledSlot::One(id) = &s.slots[0] else {
            panic!("{s:?}")
        };
        assert_eq!(id.as_f64(), Some(-42.0));
        let SampledSlot::List(l) = &s.slots[1] else {
            panic!("{s:?}")
        };
        assert_eq!(l.len(), 2);
        assert_eq!(l[0].string_value(false), Some(b"open".to_vec()));
        assert_eq!(l[1], bound);

        let clock = Arc::new(ManualClock::new());
        let r = Recorder::new(RecorderConfig::default(), clock);
        for (us, examined) in [(2_000u64, Some(5_000u64)), (4_000, None)] {
            let ex = ExecSummary {
                latency_us: us,
                rows_returned: 20,
                in_list_max: 0,
                examined,
                ..ExecSummary::default()
            };
            r.record(TenantId(1), &fp, &ex);
        }
        let mut w = r.window(TenantId(1)).pop().unwrap();
        w.samples.push(s);
        assert_eq!(w.runs(), 2);
        assert!((w.mean_ms() - 3.0).abs() < 1e-9);
        assert!(w.p95_ms() >= 4.0 && w.p95_ms() <= 4.0 * 1.19);
        assert_eq!(w.examined_per_run(), Some(5_000.0));
        assert!((w.returned_per_run() - 20.0).abs() < 1e-9);
        assert_eq!(w.examined_per_returned(), Some(250.0));
        assert_eq!(w.in_list_max(), Some(2));
        assert_eq!(w.sampled_limit(), Some(20));
    }
}
