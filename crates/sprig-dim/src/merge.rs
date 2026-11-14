//! Merging, subtotals, order and limit.
//!
//! ```text
//! merge(rows, spec):
//!  1. hash-aggregate rows by the group keys with the final functions;
//!     over analytics.max_groups groups: 39753
//!  2. totals: for j = n − 1 down to 0, fold the groups by keys 1..j into
//!     subtotal rows, setting the grouping bits of keys j+1..n (per_level
//!     plans take level j from its own statement)
//!  3. order within each level; each subtotal follows its group; LIMIT
//!     counts detail rows
//! ```
//!
//! Sums add exactly as decimals (MariaDB's `SUM` of integers and
//! `DECIMAL`s is exact); `avg` is the sum over the count rounded to the
//! sum's scale plus 4 digits, as MariaDB's `AVG` (`div_precision_increment`).

use std::cmp::Ordering;
use std::collections::BTreeMap;

use sprig_common::{Result, SprigCode, SprigError};

use crate::model::Agg;
use crate::query::{Compiled, OrderBy};

/// A number from a partial aggregate: an exact decimal when it has one,
/// else a double.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Num {
    /// `mantissa × 10^-scale`.
    Dec(i128, u32),
    /// A double.
    F(f64),
}

impl Num {
    /// Parses MariaDB's text form.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let t = s.trim();
        if t.is_empty() {
            return None;
        }
        if !t.contains(['e', 'E']) && t.len() <= 36 {
            let neg = t.starts_with('-');
            let body = t.trim_start_matches(['-', '+']);
            let (i, f) = body.split_once('.').unwrap_or((body, ""));
            if !i.is_empty()
                && i.bytes().all(|b| b.is_ascii_digit())
                && f.bytes().all(|b| b.is_ascii_digit())
            {
                let m: i128 = format!("{i}{f}").parse().ok()?;
                return Some(Self::Dec(
                    if neg { -m } else { m },
                    u32::try_from(f.len()).ok()?,
                ));
            }
        }
        t.parse().ok().map(Self::F)
    }

    fn f(self) -> f64 {
        match self {
            Self::Dec(m, s) => m as f64 / 10f64.powi(i32::try_from(s).unwrap_or(0)),
            Self::F(x) => x,
        }
    }

    fn scaled(m: i128, from: u32, to: u32) -> i128 {
        m * 10i128.pow(to - from)
    }

    /// The sum.
    #[must_use]
    pub fn plus(self, o: Self) -> Self {
        match (self, o) {
            (Self::Dec(a, sa), Self::Dec(b, sb)) => {
                let s = sa.max(sb);
                Self::Dec(Self::scaled(a, sa, s) + Self::scaled(b, sb, s), s)
            }
            (a, b) => Self::F(a.f() + b.f()),
        }
    }

    /// Orders two numbers.
    #[must_use]
    pub fn compare(self, o: Self) -> Ordering {
        match (self, o) {
            (Self::Dec(a, sa), Self::Dec(b, sb)) => {
                let s = sa.max(sb);
                Self::scaled(a, sa, s).cmp(&Self::scaled(b, sb, s))
            }
            (a, b) => a.f().total_cmp(&b.f()),
        }
    }

    /// `self / n`, rounded half away from zero to the scale plus 4 (decimals).
    #[must_use]
    pub fn div_by(self, n: i128) -> Self {
        match self {
            Self::Dec(m, s) if n != 0 => {
                let num = m * 10_000;
                let q = num / n;
                let r = num % n;
                let q = if 2 * r.abs() >= n.abs() {
                    q + num.signum() * n.signum()
                } else {
                    q
                };
                Self::Dec(q, s + 4)
            }
            other => Self::F(other.f() / n as f64),
        }
    }

    /// MariaDB's text form.
    #[must_use]
    pub fn text(self) -> String {
        match self {
            Self::Dec(m, 0) => m.to_string(),
            Self::Dec(m, s) => {
                let neg = m < 0;
                let a = m.unsigned_abs().to_string();
                let s = s as usize;
                let a = if a.len() <= s {
                    format!("{}{a}", "0".repeat(s - a.len() + 1))
                } else {
                    a
                };
                let (i, f) = a.split_at(a.len() - s);
                format!("{}{i}.{f}", if neg { "-" } else { "" })
            }
            Self::F(x) => x.to_string(),
        }
    }
}

/// Partial values of one group.
type Partials = Vec<Option<Num>>;

/// One output row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutRow {
    /// The level values (`None`: rolled up, or an unknown member).
    pub keys: Vec<Option<String>>,
    /// One value per measure.
    pub values: Vec<Option<String>>,
    /// Bit `n − 1 − i` set when level `i` is rolled up (MySQL's
    /// `GROUPING(k1, ..., kn)`).
    pub grouping: u32,
}

fn fold(c: &Compiled, acc: &mut Partials, row: &Partials) {
    for m in &c.measures {
        let n = if m.agg == Agg::Avg { 2 } else { 1 };
        for i in m.col..m.col + n {
            let (a, b) = (acc[i], row[i]);
            acc[i] = match (a, b) {
                (None, x) | (x, None) => x,
                (Some(x), Some(y)) => Some(match m.agg {
                    Agg::Min => {
                        if y.compare(x) == Ordering::Less {
                            y
                        } else {
                            x
                        }
                    }
                    Agg::Max => {
                        if y.compare(x) == Ordering::Greater {
                            y
                        } else {
                            x
                        }
                    }
                    _ => x.plus(y),
                }),
            };
        }
    }
}

fn finals(c: &Compiled, p: &Partials) -> Vec<Option<Num>> {
    c.measures
        .iter()
        .map(|m| match m.agg {
            Agg::Avg => match (p[m.col], p[m.col + 1]) {
                (Some(s), Some(Num::Dec(n, 0))) if n != 0 => Some(s.div_by(n)),
                _ => None,
            },
            // COUNT of nothing is 0, not NULL.
            Agg::Count | Agg::CountDistinct => Some(p[m.col].unwrap_or(Num::Dec(0, 0))),
            _ => p[m.col],
        })
        .collect()
}

/// A statement's text rows as keys and partials.
#[must_use]
pub fn rows_of(
    c: &Compiled,
    nkeys: usize,
    rows: &[Vec<Option<Vec<u8>>>],
) -> Vec<(Vec<Option<String>>, Partials)> {
    rows.iter()
        .map(|r| {
            let text = |i: usize| {
                r.get(i)
                    .and_then(Clone::clone)
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
            };
            let keys = (0..nkeys).map(text).collect();
            let np = c
                .measures
                .iter()
                .map(|m| if m.agg == Agg::Avg { 2 } else { 1 })
                .sum::<usize>();
            let parts = (0..np)
                .map(|i| text(nkeys + i).and_then(|t| Num::parse(&t)))
                .collect();
            (keys, parts)
        })
        .collect()
}

/// Compares two key texts: numbers as numbers, NULL first (MariaDB's
/// order for GROUP BY).
#[allow(clippy::ref_option)]
fn cmp_key(a: &Option<String>, b: &Option<String>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, _) => Ordering::Less,
        (_, None) => Ordering::Greater,
        (Some(x), Some(y)) => match (Num::parse(x), Num::parse(y)) {
            (Some(p), Some(q)) => p.compare(q),
            _ => x.cmp(y),
        },
    }
}

/// What [`merge`] needs to know about the query.
#[derive(Debug, Clone)]
pub struct Spec<'a> {
    /// The compiled query.
    pub compiled: &'a Compiled,
    /// The query's measures, by name, in order.
    pub measures: &'a [String],
    /// Sort keys.
    pub order: &'a [(OrderBy, bool)],
    /// Subtotals.
    pub totals: bool,
    /// Detail rows wanted.
    pub limit: Option<u64>,
    /// `analytics.max_groups`.
    pub max_groups: usize,
}

/// Merges the detail statement's rows (and the per-level statements'
/// when `per_level`) into the answer.
///
/// # Errors
/// 39753 past `max_groups`.
#[allow(clippy::too_many_lines)]
pub fn merge(
    s: &Spec<'_>,
    detail: Vec<(Vec<Option<String>>, Partials)>,
    levels: Vec<Vec<(Vec<Option<String>>, Partials)>>,
) -> Result<Vec<OutRow>> {
    let c = s.compiled;
    let n = c.keys;
    let np = c
        .measures
        .iter()
        .map(|m| if m.agg == Agg::Avg { 2 } else { 1 })
        .sum::<usize>();
    // 1. Groups (the statement grouped already; rows from several shards
    // of one group fold here).
    let mut groups: BTreeMap<Vec<KeyOrd>, (Vec<Option<String>>, Partials)> = BTreeMap::new();
    for (k, p) in detail {
        let e = groups
            .entry(k.iter().cloned().map(KeyOrd).collect())
            .or_insert_with(|| (k.clone(), vec![None; np]));
        fold(c, &mut e.1, &p);
        if groups.len() > s.max_groups {
            return Err(SprigError::new(
                SprigCode::DimTooManyGroups,
                format!("more than {} groups (analytics.max_groups)", s.max_groups),
            ));
        }
    }
    // 2. Subtotals per prefix length j < n.
    let mut subs: Vec<BTreeMap<Vec<KeyOrd>, Partials>> = vec![BTreeMap::new(); n];
    if s.totals {
        let distinct = !levels.is_empty();
        if distinct {
            for (j, rows) in levels.into_iter().enumerate() {
                for (k, p) in rows {
                    let e = subs[j]
                        .entry(k.into_iter().take(j).map(KeyOrd).collect())
                        .or_insert_with(|| vec![None; np]);
                    fold(c, e, &p);
                }
            }
        } else {
            for (k, p) in groups.values() {
                for (j, sub) in subs.iter_mut().enumerate() {
                    let e = sub
                        .entry(k[..j].iter().cloned().map(KeyOrd).collect())
                        .or_insert_with(|| vec![None; np]);
                    fold(c, e, p);
                }
            }
        }
    }
    let value_of = |p: &Partials, name: &str| -> Option<Num> {
        let i = s.measures.iter().position(|m| m == name)?;
        finals(c, p)[i]
    };
    let cmp_rows = |ka: &[Option<String>],
                    pa: &Partials,
                    kb: &[Option<String>],
                    pb: &Partials,
                    depth: usize|
     -> Ordering {
        for (by, desc) in s.order {
            let o = match by {
                OrderBy::Measure(m) => match (value_of(pa, m), value_of(pb, m)) {
                    (Some(x), Some(y)) => x.compare(y),
                    (None, Some(_)) => Ordering::Less,
                    (Some(_), None) => Ordering::Greater,
                    (None, None) => Ordering::Equal,
                },
                OrderBy::Level(i) if *i < depth => cmp_key(&ka[*i], &kb[*i]),
                OrderBy::Level(_) => Ordering::Equal,
            };
            let o = if *desc { o.reverse() } else { o };
            if o != Ordering::Equal {
                return o;
            }
        }
        for i in 0..depth {
            let o = cmp_key(&ka[i], &kb[i]);
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    };
    let out_row = |keys: Vec<Option<String>>, p: &Partials, grouping: u32| OutRow {
        keys,
        values: finals(c, p).into_iter().map(|v| v.map(Num::text)).collect(),
        grouping,
    };
    let mut out = Vec::new();
    let limit = s
        .limit
        .map_or(usize::MAX, |l| usize::try_from(l).unwrap_or(usize::MAX));
    if !s.totals || n == 0 {
        let mut all: Vec<&(Vec<Option<String>>, Partials)> = groups.values().collect();
        all.sort_by(|a, b| cmp_rows(&a.0, &a.1, &b.0, &b.1, n));
        for (k, p) in all.into_iter().take(limit) {
            out.push(out_row(k.clone(), p, 0));
        }
        if s.totals && n == 0 && out.is_empty() {
            out.push(out_row(Vec::new(), &vec![None; np], 0));
        }
        return Ok(out);
    }
    // 3. Hierarchical order: children of each prefix sorted, a subtotal
    // after each group, the grand total last.
    let mut detail_left = limit;
    emit(
        &[],
        0,
        n,
        &groups,
        &subs,
        &cmp_rows,
        &out_row,
        &mut out,
        &mut detail_left,
        np,
    );
    let grand = subs[0]
        .get(&Vec::new())
        .cloned()
        .unwrap_or_else(|| vec![None; np]);
    out.push(out_row(vec![None; n], &grand, (1u32 << n) - 1));
    Ok(out)
}

/// A key ordered as [`cmp_key`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyOrd(Option<String>);

impl PartialOrd for KeyOrd {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

impl Ord for KeyOrd {
    fn cmp(&self, o: &Self) -> Ordering {
        cmp_key(&self.0, &o.0).then_with(|| self.0.cmp(&o.0))
    }
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn emit(
    prefix: &[KeyOrd],
    depth: usize,
    n: usize,
    groups: &BTreeMap<Vec<KeyOrd>, (Vec<Option<String>>, Partials)>,
    subs: &[BTreeMap<Vec<KeyOrd>, Partials>],
    cmp_rows: &dyn Fn(
        &[Option<String>],
        &Partials,
        &[Option<String>],
        &Partials,
        usize,
    ) -> Ordering,
    out_row: &dyn Fn(Vec<Option<String>>, &Partials, u32) -> OutRow,
    out: &mut Vec<OutRow>,
    left: &mut usize,
    np: usize,
) {
    // The children of `prefix` at depth + 1, with their aggregates.
    let mut children: Vec<(Vec<KeyOrd>, Vec<Option<String>>, Partials)> = Vec::new();
    if depth + 1 == n {
        for (k, (keys, p)) in groups.range(prefix.to_vec()..) {
            if !k.starts_with(prefix) {
                break;
            }
            children.push((k.clone(), keys.clone(), p.clone()));
        }
    } else {
        for (k, p) in subs[depth + 1].range(prefix.to_vec()..) {
            if !k.starts_with(prefix) {
                break;
            }
            let mut keys: Vec<Option<String>> = k.iter().map(|x| x.0.clone()).collect();
            keys.resize(n, None);
            children.push((k.clone(), keys, p.clone()));
        }
    }
    children.sort_by(|a, b| cmp_rows(&a.1, &a.2, &b.1, &b.2, depth + 1));
    for (k, keys, p) in children {
        if *left == 0 {
            return;
        }
        if depth + 1 == n {
            out.push(out_row(keys, &p, 0));
            *left -= 1;
        } else {
            emit(
                &k,
                depth + 1,
                n,
                groups,
                subs,
                cmp_rows,
                out_row,
                out,
                left,
                np,
            );
            let rolled = (1u32 << (n - depth - 1)) - 1;
            out.push(out_row(keys, &p, rolled));
        }
    }
    let _ = np;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals() {
        let a = Num::parse("10.25").unwrap();
        let b = Num::parse("-0.5").unwrap();
        assert_eq!(a.plus(b).text(), "9.75");
        assert_eq!(Num::parse("7").unwrap().div_by(2).text(), "3.5000");
        assert_eq!(Num::parse("10.00").unwrap().div_by(3).text(), "3.333333");
        assert_eq!(Num::parse("-1").unwrap().div_by(3).text(), "-0.3333");
        assert_eq!(Num::parse("0.05").unwrap().text(), "0.05");
        assert_eq!(Num::parse("1e3"), Some(Num::F(1000.0)));
        assert_eq!(Num::parse("abc"), None);
    }
}
