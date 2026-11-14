//! The cost model, its gates, selection under budgets and replacement.
//!
//! ```text
//! u_T     := median over T's SELECT fingerprints of t_f / max(x_f, 1)   # ms per row
//! fixed_f := max(0, t_f - x_f * u_T)
//! e_f     := N * sel(the parts of c that f's predicates use)
//!            (min(e_f, k / sel_residual(f)) when c delivers f's order and f has LIMIT k)
//! w_f     := e_f * (c covers f ? 1 : 1 + lookup_cost_factor)
//! t̂_f     := fixed_f + w_f * u_T;   Δ_f := max(0, t_f - t̂_f)
//! B(c)    := Σ_f (n_f / W) * Δ_f                        # ms saved per hour
//! Wc(c)   := (inserts + deletes + updates touching c's columns) per hour
//!            * maintenance_cost_ms
//! S(c)    := N * (Σ avg_length(parts) + pk_length + 14) / 0.75
//! Tb(c)   := S(c) / build rate
//! gates:  net >= min_net_ms_per_hour; B >= min_benefit_ratio * Wc;
//!         Tb <= max_build_hours; 0.05 * Tb * 1000 <= net * payback_hours;
//!         t̂_f <= (1 - min_improvement_pct/100 [* estimate_margin]) * t_f for
//!         every primary fingerprint
//! ```
//!
//! [`select`] then picks, per table, by marginal net value per byte, within
//! `max_per_table` automatic indexes and `max_storage_pct` extra storage,
//! replacing an active automatic index only when the newcomer is worth more
//! than twice as much per byte.
//!
//! **Choices:**
//!
//! - Usage per fingerprint comes from [`usage`]: the same reading of the
//!   shape the generator uses, so an index serving a fingerprint it wasn't
//!   generated for (an existing or already chosen one) is priced the same
//!   way in the marginal value.
//! - A table over its budget after a bound was tightened retires its
//!   lowest-value automatic index, and that evaluation adds nothing to the
//!   table: at most one retirement (for budget or replacement) per table
//!   per evaluation, so a settings change never causes a burst of churn.
//! - Pool members left when selection stops are `deferred` with reason
//!   `marginal_below_min_net`.

use std::collections::{BTreeMap, BTreeSet};

use sprig_common::ids::{Fingerprint, ObjectId};
use sprig_workload::shape::{StmtType, WriteKind};

use super::candidates::{FpInput, TableAccess, access, covered_by, usage};
use super::model::{Candidate, IndexInfo, Origin, Role};
use super::stats::TableStats;
use super::store::DropReason;

/// `InnoDB`'s overhead per secondary index entry, bytes.
pub const ENTRY_OVERHEAD_BYTES: f64 = 14.0;
/// Fill factor of a B-tree after random inserts.
pub const FILL_FACTOR: f64 = 0.75;
/// Share of the cluster's time charged during a build (the payback gate).
pub const BUILD_LOAD_SHARE: f64 = 0.05;
/// A replacement must be worth this many times the replaced index per byte.
pub const REPLACE_FACTOR: f64 = 2.0;

/// The cost model's parameters, from the resolved settings.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CostParams {
    /// `auto_index.window_hours` (`W`).
    pub window_hours: f64,
    /// `auto_index.lookup_cost_factor`.
    pub lookup_cost_factor: f64,
    /// `auto_index.maintenance_cost_ms`.
    pub maintenance_cost_ms: f64,
    /// `auto_index.min_net_ms_per_hour`.
    pub min_net_ms_per_hour: f64,
    /// `auto_index.min_benefit_ratio`.
    pub min_benefit_ratio: f64,
    /// `auto_index.max_build_hours`.
    pub max_build_hours: f64,
    /// `auto_index.payback_hours`.
    pub payback_hours: f64,
    /// `auto_index.min_improvement_pct`.
    pub min_improvement_pct: f64,
    /// Bytes per second a build writes: the cluster's measured rate, else
    /// `auto_index.build_rate_mb_per_sec`.
    pub build_rate_bytes_per_sec: f64,
    /// `auto_index.estimate_margin` when the cluster has no replica to
    /// validate on (estimate and canary), else `None`.
    pub estimate_margin: Option<f64>,
}

impl Default for CostParams {
    /// The defaults, with a replica to validate on.
    fn default() -> Self {
        Self {
            window_hours: 24.0,
            lookup_cost_factor: 2.0,
            maintenance_cost_ms: 0.05,
            min_net_ms_per_hour: 100.0,
            min_benefit_ratio: 2.0,
            max_build_hours: 6.0,
            payback_hours: 24.0,
            min_improvement_pct: 30.0,
            build_rate_bytes_per_sec: 20e6,
            estimate_margin: None,
        }
    }
}

#[allow(clippy::cast_precision_loss)]
fn f(n: u64) -> f64 {
    n as f64
}

/// Row changes per hour on one table, from its write fingerprints.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WriteLoad {
    /// Rows inserted per hour (`REPLACE` and upserts count here too).
    pub inserts_h: f64,
    /// Rows deleted per hour.
    pub deletes_h: f64,
    /// Rows updated per hour, by set of updated columns.
    pub updates_h: BTreeMap<BTreeSet<String>, f64>,
    /// Rows updated per hour with an unknown column set (counted against
    /// every index).
    pub unknown_updates_h: f64,
}

impl WriteLoad {
    /// The load on `alias`-tables of `object` from `fps` over `window_hours`.
    #[must_use]
    pub fn from_fps(object: ObjectId, fps: &[FpInput], window_hours: f64) -> Self {
        let mut w = WriteLoad::default();
        let per_h =
            |fp: &FpInput| f(fp.runs) / window_hours.max(1e-9) * fp.affected_per_run.max(1.0);
        for fp in fps {
            let Some(writes) = &fp.shape.writes else {
                continue;
            };
            let targets: Vec<&str> = writes
                .targets
                .iter()
                .filter(|a| fp.shape.table(a).and_then(|t| t.object_id) == Some(object))
                .map(String::as_str)
                .collect();
            if targets.is_empty() {
                continue;
            }
            let rows = per_h(fp);
            match writes.kind {
                WriteKind::Insert | WriteKind::Replace => w.inserts_h += rows,
                WriteKind::Delete => w.deletes_h += rows,
                WriteKind::Update | WriteKind::Upsert => {
                    let cols: BTreeSet<String> = writes
                        .set
                        .iter()
                        .filter(|c| c.table.as_deref().is_some_and(|t| targets.contains(&t)))
                        .map(|c| c.col.to_ascii_lowercase())
                        .collect();
                    if writes.kind == WriteKind::Upsert {
                        // The insert half; the update half below.
                        w.inserts_h += rows / 2.0;
                    }
                    let rows = if writes.kind == WriteKind::Upsert {
                        rows / 2.0
                    } else {
                        rows
                    };
                    if cols.is_empty() || writes.all_columns {
                        w.unknown_updates_h += rows;
                    } else {
                        *w.updates_h.entry(cols).or_default() += rows;
                    }
                }
            }
        }
        w
    }

    /// Index entry changes per hour for an index over `columns`: inserts,
    /// deletes, and updates of a column set that touches it.
    #[must_use]
    pub fn touching<'a>(&self, columns: impl IntoIterator<Item = &'a str>) -> f64 {
        let cols: BTreeSet<&str> = columns.into_iter().collect();
        let updates: f64 = self
            .updates_h
            .iter()
            .filter(|(set, _)| set.iter().any(|c| cols.contains(c.as_str())))
            .map(|(_, n)| n)
            .sum();
        self.inserts_h + self.deletes_h + updates + self.unknown_updates_h
    }
}

/// The median of `v` (0 for none).
#[must_use]
pub fn median(mut v: Vec<f64>) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        f64::midpoint(v[n / 2 - 1], v[n / 2])
    }
}

/// Everything the cost model knows about one table.
#[derive(Debug, Clone)]
pub struct TableContext<'a> {
    /// The object.
    pub object: ObjectId,
    /// Its statistics.
    pub stats: &'a TableStats,
    /// Every fingerprint reading the table, with its access to it.
    pub reads: BTreeMap<Fingerprint, (&'a FpInput, TableAccess)>,
    /// Row changes per hour.
    pub writes: WriteLoad,
    /// ms per row read (`u_T`).
    pub u_t: f64,
    /// Parameters.
    pub params: CostParams,
}

impl<'a> TableContext<'a> {
    /// The context of `object` from the tenant's window `fps`.
    #[must_use]
    pub fn new(
        object: ObjectId,
        stats: &'a TableStats,
        fps: &'a [FpInput],
        params: CostParams,
    ) -> Self {
        let mut reads = BTreeMap::new();
        let mut per_row = Vec::new();
        for fp in fps {
            let Some(t) = fp
                .shape
                .tables
                .iter()
                .find(|t| t.object_id == Some(object) && !t.derived)
            else {
                continue;
            };
            if fp.shape.stmt_type == StmtType::Select {
                per_row.push(fp.mean_ms / fp.examined_per_run.max(1.0));
            }
            if fp.shape.stmt_type.is_indexable() && fp.shape.stmt_type != StmtType::Insert {
                reads.insert(fp.id, (fp, access(fp, &t.alias, object, stats)));
            }
        }
        Self {
            object,
            stats,
            reads,
            writes: WriteLoad::from_fps(object, fps, params.window_hours),
            u_t: median(per_row),
            params,
        }
    }

    /// `t̂_f` for fingerprint `fp` with an index over `c`'s parts, and the
    /// entries read; `None` when `fp` doesn't read the table or the index
    /// can't serve it.
    #[must_use]
    pub fn predict(&self, parts: &[super::model::KeyPart], fp: Fingerprint) -> Option<(f64, f64)> {
        let (input, acc) = self.reads.get(&fp)?;
        let u = usage(parts, acc, self.stats)?;
        let fixed = (input.mean_ms - input.examined_per_run * self.u_t).max(0.0);
        let w = u.entries
            * if u.covering {
                1.0
            } else {
                1.0 + self.params.lookup_cost_factor
            };
        Some((fixed + w * self.u_t, u.entries))
    }

    /// Runs per hour of `fp`.
    #[must_use]
    pub fn per_hour(&self, fp: Fingerprint) -> f64 {
        self.reads
            .get(&fp)
            .map_or(0.0, |(i, _)| f(i.runs) / self.params.window_hours.max(1e-9))
    }

    /// `S`: bytes on disk of an index over `parts`.
    #[must_use]
    pub fn bytes(&self, parts: &[super::model::KeyPart]) -> f64 {
        let key: f64 = parts
            .iter()
            .map(|p| {
                let a = self.stats.avg_length(&p.column);
                p.prefix.map_or(a, |n| a.min(f64::from(n) * 4.0))
            })
            .sum();
        self.stats.rows() * (key + self.stats.pk_length() + ENTRY_OVERHEAD_BYTES) / FILL_FACTOR
    }

    /// `S` of a field-column index: the columns don't exist
    /// yet, so each part counts the value's average length, at most its
    /// prefix.
    #[must_use]
    pub fn field_bytes(&self, parts: &[super::model::KeyPart]) -> f64 {
        let v = self.stats.avg_length(super::kv::VALUE_COLUMN);
        let key: f64 = parts
            .iter()
            .map(|p| p.prefix.map_or(v, |n| v.min(f64::from(n))))
            .sum();
        self.stats.rows() * (key + self.stats.pk_length() + ENTRY_OVERHEAD_BYTES) / FILL_FACTOR
    }

    /// `Wc`: ms per hour of index maintenance for an index over `parts`.
    #[must_use]
    pub fn write_cost(&self, parts: &[super::model::KeyPart]) -> f64 {
        self.writes
            .touching(parts.iter().map(|p| p.column.as_str()))
            * self.params.maintenance_cost_ms
    }
}

/// One fingerprint's prediction.
#[derive(Debug, Clone, PartialEq)]
pub struct FpEstimate {
    /// The fingerprint.
    pub fp: Fingerprint,
    /// How the candidate serves it.
    pub role: Role,
    /// Runs per hour.
    pub per_hour: f64,
    /// Mean time now, ms (`t_f`).
    pub t_ms: f64,
    /// Predicted time, ms (`t̂_f`); `t_f` when the index can't serve it.
    pub t_hat_ms: f64,
    /// Entries read per run.
    pub entries: f64,
}

/// A gate a candidate failed.
#[derive(Debug, Clone, PartialEq)]
pub enum Gate {
    /// `net < min_net_ms_per_hour`.
    MinNet,
    /// `B < min_benefit_ratio * Wc`.
    BenefitRatio,
    /// `Tb > max_build_hours`.
    BuildTime,
    /// The build doesn't pay back within `payback_hours`.
    Payback,
    /// A primary fingerprint isn't predicted to improve enough.
    Improvement(Fingerprint),
}

impl Gate {
    /// The candidate's `reason`.
    #[must_use]
    pub fn reason(&self) -> String {
        match self {
            Gate::MinNet => "below_min_net".into(),
            Gate::BenefitRatio => "below_benefit_ratio".into(),
            Gate::BuildTime => "build_too_long".into(),
            Gate::Payback => "no_payback".into(),
            Gate::Improvement(fp) => format!("no_predicted_gain:{fp}"),
        }
    }
}

/// The cost model's estimate of one candidate: `B`, `Wc`, `S` and `Tb`
/// (module docs), and the gates it fails.
#[derive(Debug, Clone, PartialEq)]
pub struct Estimate {
    /// `B`: ms saved per hour.
    pub benefit_ms_h: f64,
    /// `Wc`: ms added per hour.
    pub write_ms_h: f64,
    /// `S`: bytes.
    pub bytes: f64,
    /// `Tb`: seconds.
    pub build_seconds: f64,
    /// Per fingerprint served.
    pub per_fp: Vec<FpEstimate>,
    /// Gates failed, in the order of the module docs; empty: it may go on.
    pub failed: Vec<Gate>,
}

impl Estimate {
    /// `net = B - Wc`.
    #[must_use]
    pub fn net(&self) -> f64 {
        self.benefit_ms_h - self.write_ms_h
    }

    /// Passed every gate.
    #[must_use]
    pub fn passes(&self) -> bool {
        self.failed.is_empty()
    }

    /// Net value per byte.
    #[must_use]
    pub fn net_per_byte(&self) -> f64 {
        self.net() / self.bytes.max(1.0)
    }
}

/// The cost model interface.
pub trait CostModel: Send + Sync {
    /// `B`, `Wc`, `S`, `Tb` and the gates of `c` on `t`.
    fn estimate(&self, c: &Candidate, t: &TableContext<'_>) -> Estimate;
}

/// The model above.
#[derive(Debug, Clone, Copy, Default)]
pub struct Model;

impl CostModel for Model {
    fn estimate(&self, c: &Candidate, t: &TableContext<'_>) -> Estimate {
        let p = &t.params;
        let mut per_fp = Vec::new();
        let mut benefit = 0.0;
        for (fp, role) in &c.serves {
            let Some((input, _)) = t.reads.get(fp) else {
                continue;
            };
            let (t_hat, entries) =
                if c.def.kind == super::model::IndexKind::BTree && !super::kv::is_kv(c) {
                    t.predict(&c.def.parts, *fp)
                        .unwrap_or((input.mean_ms, input.examined_per_run))
                } else {
                    // Spatial and vector indexes and key-value
                    // element tables: the run reads
                    // about the entries the generator expects instead of the
                    // rows it examines now; time scales with that share.
                    let entries = c.evidence.entries_per_run.unwrap_or(input.examined_per_run);
                    let share = (entries / input.examined_per_run.max(1.0)).min(1.0);
                    (input.mean_ms * share, entries)
                };
            let per_hour = t.per_hour(*fp);
            benefit += per_hour * (input.mean_ms - t_hat).max(0.0);
            per_fp.push(FpEstimate {
                fp: *fp,
                role: *role,
                per_hour,
                t_ms: input.mean_ms,
                t_hat_ms: t_hat,
                entries,
            });
        }
        // A field column's index changes with `v`.
        let write = t.writes.touching(
            super::kv::maintained_columns(&c.def)
                .iter()
                .map(String::as_str),
        ) * p.maintenance_cost_ms;
        let bytes = if super::doc::is_doc(&c.def) {
            super::doc::bytes(&c.def, t.stats)
        } else if c.def.kind == super::model::IndexKind::FieldColumn {
            t.field_bytes(&c.def.parts)
        } else {
            t.bytes(&c.def.parts)
        };
        let build_seconds = bytes / p.build_rate_bytes_per_sec.max(1.0);
        let mut e = Estimate {
            benefit_ms_h: benefit,
            write_ms_h: write,
            bytes,
            build_seconds,
            per_fp,
            failed: Vec::new(),
        };
        let net = e.net();
        if net < p.min_net_ms_per_hour {
            e.failed.push(Gate::MinNet);
        }
        if benefit < p.min_benefit_ratio * write {
            e.failed.push(Gate::BenefitRatio);
        }
        if build_seconds > p.max_build_hours * 3600.0 {
            e.failed.push(Gate::BuildTime);
        }
        if BUILD_LOAD_SHARE * build_seconds * 1000.0 > net * p.payback_hours {
            e.failed.push(Gate::Payback);
        }
        let need = p.min_improvement_pct / 100.0 * p.estimate_margin.unwrap_or(1.0);
        for fe in e.per_fp.iter().filter(|x| x.role == Role::Primary) {
            if fe.t_hat_ms > (1.0 - need.min(1.0)) * fe.t_ms {
                e.failed.push(Gate::Improvement(fe.fp));
            }
        }
        e
    }
}

/// A candidate in the selection pool.
#[derive(Debug, Clone, PartialEq)]
pub struct Pooled {
    /// The candidate.
    pub candidate: Candidate,
    /// Its estimate (it passed the gates).
    pub estimate: Estimate,
    /// Only the fast trigger qualified its fingerprints: it never
    /// displaces an active automatic index.
    pub fast_only: bool,
}

/// An automatic index already on the table (any state that counts for the
/// budget).
#[derive(Debug, Clone, PartialEq)]
pub struct AutoOnTable {
    /// Its name.
    pub name: String,
    /// Its key, for coverage of new candidates.
    pub info: IndexInfo,
    /// Bytes (measured, else estimated).
    pub bytes: f64,
    /// Measured value, ms per hour (else the estimated net).
    pub value_ms_h: f64,
    /// It is active (only active ones can be replaced).
    pub active: bool,
}

/// One table's budget, resolved at the table (see
/// [`policy`](super::policy)).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Budget {
    /// `auto_index.max_per_table`.
    pub max_per_table: u32,
    /// `auto_index.max_storage_pct`.
    pub max_storage_pct: f64,
    /// `auto_index.min_storage_budget_mb` (64), in bytes: the storage
    /// budget is never below this, so a small table can get the indexes it
    /// needs (the default may change).
    pub min_storage_bytes: f64,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            max_per_table: 5,
            max_storage_pct: 20.0,
            min_storage_bytes: 64.0 * 1024.0 * 1024.0,
        }
    }
}

impl Budget {
    /// The table's storage budget for automatic indexes, bytes:
    /// `max(max_storage_pct% of size, min_storage_bytes)`.
    #[must_use]
    pub fn cap_bytes(&self, size: f64) -> f64 {
        (size * self.max_storage_pct / 100.0).max(self.min_storage_bytes)
    }
}

/// A chosen candidate.
#[derive(Debug, Clone, PartialEq)]
pub struct Chosen {
    /// The candidate.
    pub candidate: Candidate,
    /// Its estimate.
    pub estimate: Estimate,
    /// Marginal net value when chosen, ms per hour.
    pub marginal_ms_h: f64,
    /// The active automatic index it replaces (retired as `replaced` once
    /// this one is active).
    pub replaces: Option<String>,
}

/// What [`select`] decided for one table.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Selection {
    /// To validate, in order of choice.
    pub chosen: Vec<Chosen>,
    /// Left for later, with the reason (`budget`, `fast_trigger_no_displace`,
    /// `marginal_below_min_net`, `covered_by:<name>`).
    pub deferred: Vec<(Candidate, String)>,
    /// An automatic index to retire now because the table is over a
    /// tightened budget.
    pub retire: Option<(String, DropReason)>,
}

fn as_info(c: &Candidate) -> IndexInfo {
    IndexInfo {
        name: c.def.name(),
        parts: c.def.parts.clone(),
        unique: false,
        origin: Origin::Auto,
        ignored: false,
        usable: true,
    }
}

/// `select(T)`: selection under the table's budget (module docs).
#[must_use]
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the whole loop, in one place
pub fn select(
    mut pool: Vec<Pooled>,
    t: &TableContext<'_>,
    auto: &[AutoOnTable],
    budget: Budget,
) -> Selection {
    let mut out = Selection::default();
    let size = f(t.stats.size_bytes());
    let cap_bytes = budget.cap_bytes(size);
    let mut live: Vec<AutoOnTable> = auto.to_vec();
    let vpb = |a: &AutoOnTable| a.value_ms_h / a.bytes.max(1.0);
    let count = |live: &[AutoOnTable]| live.len();
    let bytes = |live: &[AutoOnTable]| live.iter().map(|a| a.bytes).sum::<f64>();

    // Bound tightening: over budget already → retire the lowest value per
    // byte, one per evaluation, and add nothing this time.
    if count(&live) > budget.max_per_table as usize || bytes(&live) > cap_bytes {
        if let Some(i) = live
            .iter()
            .enumerate()
            .min_by(|a, b| vpb(a.1).total_cmp(&vpb(b.1)))
            .map(|(i, _)| i)
        {
            out.retire = Some((live[i].name.clone(), DropReason::Budget));
        }
        out.deferred = pool
            .into_iter()
            .map(|p| (p.candidate, "budget".to_owned()))
            .collect();
        return out;
    }

    let mut chosen_parts: Vec<Vec<super::model::KeyPart>> = Vec::new();
    let mut retired_one = false;
    loop {
        if pool.is_empty() {
            break;
        }
        // Marginal net value of each candidate given what's chosen.
        let marginal: Vec<f64> = pool
            .iter()
            .map(|p| marginal(&p.candidate, &p.estimate, t, &chosen_parts))
            .collect();
        let Some(best) = (0..pool.len()).max_by(|&a, &b| {
            let va = marginal[a] / pool[a].estimate.bytes.max(1.0);
            let vb = marginal[b] / pool[b].estimate.bytes.max(1.0);
            va.total_cmp(&vb).then_with(|| {
                pool[b]
                    .candidate
                    .def
                    .canonical()
                    .cmp(&pool[a].candidate.def.canonical())
            })
        }) else {
            break;
        };
        let m = marginal[best];
        if m < t.params.min_net_ms_per_hour {
            for p in pool.drain(..) {
                out.deferred
                    .push((p.candidate, "marginal_below_min_net".to_owned()));
            }
            break;
        }
        let c = pool.swap_remove(best);
        let s = c.estimate.bytes;
        let mut replaces = None;
        let over = count(&live) + 1 > budget.max_per_table as usize || bytes(&live) + s > cap_bytes;
        if over {
            let victim = live
                .iter()
                .enumerate()
                .filter(|(_, a)| a.active)
                .min_by(|a, b| vpb(a.1).total_cmp(&vpb(b.1)))
                .map(|(i, a)| (i, vpb(a)));
            let fits_after = |i: usize| {
                let mut l = live.clone();
                l.remove(i);
                count(&l) < budget.max_per_table as usize && bytes(&l) + s <= cap_bytes
            };
            match victim {
                Some((i, v))
                    if !c.fast_only
                        && !retired_one
                        && m / s.max(1.0) > REPLACE_FACTOR * v
                        && fits_after(i) =>
                {
                    replaces = Some(live.remove(i).name);
                    retired_one = true;
                }
                _ => {
                    let reason = if c.fast_only {
                        "fast_trigger_no_displace"
                    } else {
                        "budget"
                    };
                    out.deferred.push((c.candidate, reason.to_owned()));
                    continue;
                }
            }
        }
        live.push(AutoOnTable {
            name: c.candidate.def.name(),
            info: as_info(&c.candidate),
            bytes: s,
            value_ms_h: m,
            active: false,
        });
        chosen_parts.push(c.candidate.def.parts.clone());
        let info = as_info(&c.candidate);
        let mut still = Vec::with_capacity(pool.len());
        for p in pool.drain(..) {
            if covered_by(&p.candidate, std::slice::from_ref(&info)).is_some() {
                out.deferred
                    .push((p.candidate, format!("covered_by:{}", info.name)));
            } else {
                still.push(p);
            }
        }
        pool = still;
        out.chosen.push(Chosen {
            candidate: c.candidate,
            estimate: c.estimate,
            marginal_ms_h: m,
            replaces,
        });
    }
    out
}

/// `m(c)`: the improvement over the best each served fingerprint already
/// has (its measured time, or a chosen index's prediction), minus `Wc`.
fn marginal(
    c: &Candidate,
    e: &Estimate,
    t: &TableContext<'_>,
    chosen: &[Vec<super::model::KeyPart>],
) -> f64 {
    let mut gain = 0.0;
    for fe in &e.per_fp {
        let best = chosen
            .iter()
            .filter_map(|parts| t.predict(parts, fe.fp).map(|(th, _)| th))
            .fold(fe.t_ms, f64::min);
        gain += fe.per_hour * (best - fe.t_hat_ms).max(0.0);
    }
    let _ = c;
    gain - e.write_ms_h
}

#[cfg(test)]
mod tests;
