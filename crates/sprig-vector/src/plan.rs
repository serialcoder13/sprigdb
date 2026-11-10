//! The planner (chapter 34, "Planning").
//!
//! ```text
//! plan(spec, index, stats):
//!  1. spec.exact, or the graph isn't ready: Exact
//!  2. pushed = the filter as SQL (exact or a superset); residual = the rest
//!  3. rows passing pushed <= vector.prefilter_max_rows: PrefilterExact
//!  4. ef = max(num_candidates or vector.default_ef_search, k)
//!     residual present: AnnOverfetch, limit = k x clamp(ceil(1.2 / sel), 1, max_overfetch)
//!     else:             Ann, limit = k
//! ```
//!
//! As built in R6 the rows passing `pushed` are counted (up to the limit,
//! one bounded query) instead of estimated from histograms, and the
//! residual's selectivity is measured on a sample of up to 1,000 rows.

/// The search settings (chapter 34, "Configuration").
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Settings {
    /// `vector.default_ef_search`.
    pub default_ef: u32,
    /// `vector.prefilter_max_rows`.
    pub prefilter_max_rows: u64,
    /// `vector.max_overfetch`.
    pub max_overfetch: u32,
    /// `vector.max_k`.
    pub max_k: u32,
    /// `vector.ann_min_rows`.
    pub ann_min_rows: u64,
    /// `vector.min_recall`.
    pub min_recall: f64,
    /// `vector.default_m`.
    pub default_m: u8,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            default_ef: 64,
            prefilter_max_rows: 10_000,
            max_overfetch: 16,
            max_k: 10_000,
            ann_min_rows: 20_000,
            min_recall: 0.95,
            default_m: 16,
        }
    }
}

/// How a search runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plan {
    /// The graph, `LIMIT k`.
    Ann {
        /// `mhnsw_ef_search`.
        ef: u32,
        /// Rows asked of each shard.
        limit: u32,
    },
    /// The graph, more rows than k, for a residual filter.
    AnnOverfetch {
        /// `mhnsw_ef_search`.
        ef: u32,
        /// Rows asked of each shard.
        limit: u32,
    },
    /// Exact distances over the rows the pushed filter selects.
    PrefilterExact,
    /// Exact distances over every row.
    Exact,
}

impl Plan {
    /// The name in metrics and replies.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ann { .. } => "ann",
            Self::AnnOverfetch { .. } => "ann_overfetch",
            Self::PrefilterExact => "prefilter_exact",
            Self::Exact => "exact",
        }
    }

    /// Whether the plan reads the graph.
    #[must_use]
    pub const fn approximate(self) -> bool {
        matches!(self, Self::Ann { .. } | Self::AnnOverfetch { .. })
    }
}

/// What the planner knows about the data.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Stats {
    /// The graph exists and was accepted.
    pub ann_ready: bool,
    /// The request has a filter.
    pub filtered: bool,
    /// Rows passing the pushed filter, counted up to
    /// `prefilter_max_rows + 1` (`None`: not counted, no filter).
    pub pushed_rows: Option<u64>,
    /// A residual filter remains (the pushed one is a superset).
    pub residual: bool,
    /// The residual's measured selectivity (0 to 1).
    pub residual_sel: f64,
}

/// Picks the plan.
#[must_use]
pub fn plan(exact: bool, k: u32, num_candidates: Option<u32>, st: Stats, s: &Settings) -> Plan {
    if exact || !st.ann_ready {
        return if st.filtered {
            Plan::PrefilterExact
        } else {
            Plan::Exact
        };
    }
    if st.filtered
        && let Some(n) = st.pushed_rows
        && n <= s.prefilter_max_rows
    {
        return Plan::PrefilterExact;
    }
    let ef = num_candidates.unwrap_or(s.default_ef).max(k);
    if st.residual {
        let sel = st.residual_sel.clamp(1e-6, 1.0);
        let mult = (1.2 / sel)
            .ceil()
            .clamp(1.0, f64::from(s.max_overfetch.max(1))) as u32;
        Plan::AnnOverfetch {
            ef: ef.max(k.saturating_mul(mult)),
            limit: k.saturating_mul(mult),
        }
    } else {
        Plan::Ann { ef, limit: k }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(ready: bool, filtered: bool, rows: Option<u64>, residual: bool, sel: f64) -> Stats {
        Stats {
            ann_ready: ready,
            filtered,
            pushed_rows: rows,
            residual,
            residual_sel: sel,
        }
    }

    #[test]
    fn plans() {
        let s = Settings::default();
        assert_eq!(
            plan(false, 10, None, st(false, false, None, false, 1.0), &s),
            Plan::Exact
        );
        assert_eq!(
            plan(true, 10, None, st(true, false, None, false, 1.0), &s),
            Plan::Exact
        );
        assert_eq!(
            plan(false, 10, None, st(true, false, None, false, 1.0), &s),
            Plan::Ann { ef: 64, limit: 10 }
        );
        assert_eq!(
            plan(false, 100, Some(50), st(true, false, None, false, 1.0), &s),
            Plan::Ann {
                ef: 100,
                limit: 100
            }
        );
        assert_eq!(
            plan(false, 10, None, st(true, true, Some(500), true, 0.1), &s),
            Plan::PrefilterExact
        );
        // 1.2 / 0.1 = 12 times k.
        assert_eq!(
            plan(false, 10, None, st(true, true, Some(50_000), true, 0.1), &s),
            Plan::AnnOverfetch {
                ef: 120,
                limit: 120
            }
        );
        // Capped at max_overfetch.
        assert_eq!(
            plan(
                false,
                10,
                None,
                st(true, true, Some(50_000), true, 0.001),
                &s
            ),
            Plan::AnnOverfetch {
                ef: 160,
                limit: 160
            }
        );
    }
}
