//! MongoDB's numeric accumulation (M2, 12 Nov 2026): sums with MongoDB's
//! type promotion (int, then long, then double), compensated double sums
//! (a double-double, as MongoDB keeps them), and exact Decimal128
//! arithmetic rounded to 34 digits, half to even, as IEEE 754-2008 asks.

use std::cmp::Ordering;
use std::str::FromStr;

use bson::{Bson, Decimal128};
use num_bigint::BigUint;
use num_traits::{ToPrimitive, Zero};

const PREC: usize = 34;
const EMAX: i32 = 6111;
const EMIN: i32 = -6176;

/// A Decimal128 value: exact while an operation runs, rounded to 34
/// digits after each one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Dec {
    Nan,
    Inf(bool),
    Fin { neg: bool, coef: BigUint, exp: i32 },
}

fn pow10(n: u32) -> BigUint {
    BigUint::from(10u32).pow(n)
}

impl Dec {
    pub(crate) fn zero() -> Self {
        Self::Fin {
            neg: false,
            coef: BigUint::zero(),
            exp: 0,
        }
    }

    pub(crate) fn from_i128(i: i128) -> Self {
        Self::Fin {
            neg: i < 0,
            coef: BigUint::from(i.unsigned_abs()),
            exp: 0,
        }
    }

    /// The exact value of a double, with no trailing zeros after the
    /// point, rounded to 34 digits (what MongoDB adds to a decimal total).
    pub(crate) fn from_f64(f: f64) -> Self {
        if f.is_nan() {
            return Self::Nan;
        }
        if f.is_infinite() {
            return Self::Inf(f < 0.0);
        }
        let bits = f.to_bits();
        let neg = bits >> 63 == 1;
        let e = i32::try_from((bits >> 52) & 0x7ff).unwrap_or(0);
        let frac = bits & ((1u64 << 52) - 1);
        let (m, e2) = if e == 0 {
            (frac, -1074)
        } else {
            (frac | (1u64 << 52), e - 1075)
        };
        if m == 0 {
            return Self::Fin {
                neg,
                coef: BigUint::zero(),
                exp: 0,
            };
        }
        let (mut coef, mut exp) = if e2 >= 0 {
            (BigUint::from(m) << usize::try_from(e2).unwrap_or(0), 0)
        } else {
            (
                BigUint::from(m) * BigUint::from(5u32).pow(e2.unsigned_abs()),
                e2,
            )
        };
        let ten = BigUint::from(10u32);
        while exp < 0 && (&coef % &ten).is_zero() {
            coef /= &ten;
            exp += 1;
        }
        round(neg, coef, exp, false)
    }

    /// A number as MongoDB coerces it to a decimal: doubles rounded to 15
    /// significant digits.
    pub(crate) fn from_bson15(v: &Bson) -> Option<Self> {
        match v {
            Bson::Double(f) if f.is_finite() => Self::parse(&format!("{f:.14e}")),
            o => Self::from_bson(o),
        }
    }

    /// A numeric BSON value as a decimal.
    pub(crate) fn from_bson(v: &Bson) -> Option<Self> {
        match v {
            Bson::Int32(i) => Some(Self::from_i128(i128::from(*i))),
            Bson::Int64(i) => Some(Self::from_i128(i128::from(*i))),
            Bson::Double(f) => Some(Self::from_f64(*f)),
            Bson::Decimal128(d) => Self::parse(&d.to_string()),
            _ => None,
        }
    }

    /// Parses Decimal128's string form.
    pub(crate) fn parse(s: &str) -> Option<Self> {
        let (neg, rest) = match s.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, s.strip_prefix('+').unwrap_or(s)),
        };
        match rest {
            "NaN" => return Some(Self::Nan),
            "Infinity" | "Inf" => return Some(Self::Inf(neg)),
            _ => {}
        }
        let (mant, e) = match rest.find(['e', 'E']) {
            Some(i) => (&rest[..i], rest[i + 1..].parse::<i32>().ok()?),
            None => (rest, 0),
        };
        let (ip, fp) = mant.split_once('.').unwrap_or((mant, ""));
        let digits = format!("{ip}{fp}");
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        Some(Self::Fin {
            neg,
            coef: BigUint::from_str(&digits).ok()?,
            exp: e - i32::try_from(fp.len()).ok()?,
        })
    }

    pub(crate) fn to_bson(&self) -> Bson {
        let s = match self {
            Self::Nan => "NaN".to_owned(),
            Self::Inf(n) => if *n { "-Infinity" } else { "Infinity" }.to_owned(),
            Self::Fin { neg, coef, exp } => {
                format!("{}{coef}E{exp}", if *neg { "-" } else { "" })
            }
        };
        Decimal128::from_str(&s).map_or(Bson::Null, Bson::Decimal128)
    }

    pub(crate) fn to_f64(&self) -> f64 {
        match self {
            Self::Nan => f64::NAN,
            Self::Inf(n) => {
                if *n {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                }
            }
            Self::Fin { neg, coef, exp } => {
                let v: f64 = format!("{}{coef}e{exp}", if *neg { "-" } else { "" })
                    .parse()
                    .unwrap_or(f64::NAN);
                v
            }
        }
    }

    fn is_zero(&self) -> bool {
        matches!(self, Self::Fin { coef, .. } if coef.is_zero())
    }

    pub(crate) fn neg(&self) -> Self {
        match self {
            Self::Nan => Self::Nan,
            Self::Inf(n) => Self::Inf(!n),
            Self::Fin { neg, coef, exp } => Self::Fin {
                neg: !neg,
                coef: coef.clone(),
                exp: *exp,
            },
        }
    }

    pub(crate) fn add(&self, o: &Self) -> Self {
        match (self, o) {
            (Self::Nan, _) | (_, Self::Nan) => Self::Nan,
            (Self::Inf(a), Self::Inf(b)) if a != b => Self::Nan,
            (Self::Inf(a), _) | (_, Self::Inf(a)) => Self::Inf(*a),
            (
                Self::Fin {
                    neg: na,
                    coef: ca,
                    exp: ea,
                },
                Self::Fin {
                    neg: nb,
                    coef: cb,
                    exp: eb,
                },
            ) => {
                let e = (*ea).min(*eb);
                let a = ca * pow10(u32::try_from(ea - e).unwrap_or(0));
                let b = cb * pow10(u32::try_from(eb - e).unwrap_or(0));
                let (neg, coef) = if na == nb {
                    (*na, a + b)
                } else {
                    match a.cmp(&b) {
                        Ordering::Greater => (*na, a - b),
                        Ordering::Less => (*nb, b - a),
                        Ordering::Equal => (*na && *nb, BigUint::zero()),
                    }
                };
                round(neg, coef, e, false)
            }
        }
    }

    pub(crate) fn sub(&self, o: &Self) -> Self {
        self.add(&o.neg())
    }

    pub(crate) fn mul(&self, o: &Self) -> Self {
        match (self, o) {
            (Self::Nan, _) | (_, Self::Nan) => Self::Nan,
            (Self::Inf(a), b) | (b, Self::Inf(a)) => {
                if b.is_zero() {
                    Self::Nan
                } else {
                    Self::Inf(*a != b.is_neg())
                }
            }
            (
                Self::Fin {
                    neg: na,
                    coef: ca,
                    exp: ea,
                },
                Self::Fin {
                    neg: nb,
                    coef: cb,
                    exp: eb,
                },
            ) => round(na != nb, ca * cb, ea + eb, false),
        }
    }

    fn is_neg(&self) -> bool {
        match self {
            Self::Nan => false,
            Self::Inf(n) | Self::Fin { neg: n, .. } => *n,
        }
    }

    /// `self / o`, with IEEE's preferred exponent for exact quotients.
    pub(crate) fn div(&self, o: &Self) -> Self {
        match (self, o) {
            (Self::Nan, _) | (_, Self::Nan) | (Self::Inf(_), Self::Inf(_)) => Self::Nan,
            (Self::Inf(a), b) => Self::Inf(*a != b.is_neg()),
            (a, Self::Inf(b)) => Self::Fin {
                neg: a.is_neg() != *b,
                coef: BigUint::zero(),
                exp: 0,
            },
            (
                Self::Fin {
                    neg: na,
                    coef: ca,
                    exp: ea,
                },
                Self::Fin {
                    neg: nb,
                    coef: cb,
                    exp: eb,
                },
            ) => {
                if cb.is_zero() {
                    return if ca.is_zero() {
                        Self::Nan
                    } else {
                        Self::Inf(na != nb)
                    };
                }
                let pe = ea - eb;
                if ca.is_zero() {
                    return round(na != nb, BigUint::zero(), pe, false);
                }
                let da = ca.to_string().len();
                let db = cb.to_string().len();
                let s = (PREC + 2 + db).saturating_sub(da);
                let num = ca * pow10(u32::try_from(s).unwrap_or(0));
                let mut q = &num / cb;
                let r = &num % cb;
                let mut exp = pe - i32::try_from(s).unwrap_or(0);
                if r.is_zero() {
                    let ten = BigUint::from(10u32);
                    while exp < pe && !q.is_zero() && (&q % &ten).is_zero() {
                        q /= &ten;
                        exp += 1;
                    }
                    round(na != nb, q, exp, false)
                } else {
                    round(na != nb, q, exp, true)
                }
            }
        }
    }
}

/// Rounds an exact value to 34 digits, half to even; `sticky` says the
/// true value is a little above `coef` (a nonzero remainder was dropped).
fn round(neg: bool, coef: BigUint, exp: i32, sticky: bool) -> Dec {
    let digits = coef.to_string();
    let mut drop = digits.len().saturating_sub(PREC);
    // Below the smallest exponent the value loses digits too.
    if exp + i32::try_from(drop).unwrap_or(0) < EMIN {
        drop = usize::try_from(EMIN - exp).unwrap_or(0);
    }
    if drop == 0 {
        let mut coef = coef;
        let mut exp = exp;
        // Too large an exponent: fold it into the coefficient (clamped).
        while exp > EMAX && coef.to_string().len() < PREC && !coef.is_zero() {
            coef *= BigUint::from(10u32);
            exp -= 1;
        }
        if exp > EMAX && !coef.is_zero() {
            return Dec::Inf(neg);
        }
        return Dec::Fin {
            neg,
            coef,
            exp: exp.min(EMAX),
        };
    }
    let (keep, rest) = if drop >= digits.len() {
        (String::new(), format!("{}{digits}", "0".repeat(drop - digits.len())))
    } else {
        (
            digits[..digits.len() - drop].to_owned(),
            digits[digits.len() - drop..].to_owned(),
        )
    };
    let mut k = if keep.is_empty() {
        BigUint::zero()
    } else {
        BigUint::from_str(&keep).unwrap_or_default()
    };
    let first = rest.as_bytes().first().copied().unwrap_or(b'0');
    let tail_nonzero = sticky || rest.bytes().skip(1).any(|b| b != b'0');
    let up = match first.cmp(&b'5') {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => tail_nonzero || keep.as_bytes().last().is_some_and(|d| (d - b'0') % 2 == 1),
    };
    let mut exp = exp + i32::try_from(drop).unwrap_or(0);
    if up {
        k += 1u32;
        if k.to_string().len() > PREC {
            k /= BigUint::from(10u32);
            exp += 1;
        }
    }
    if exp > EMAX {
        return Dec::Inf(neg);
    }
    Dec::Fin { neg, coef: k, exp }
}

// --- double-double sums ----------------------------------------------------------

/// A compensated sum of doubles, exact to about 106 bits, as MongoDB
/// keeps `$sum` and `$avg` totals.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DoubleDouble {
    sum: f64,
    addend: f64,
    special: f64,
}

fn two_sum(a: f64, b: f64) -> (f64, f64) {
    let s = a + b;
    let bp = s - a;
    (s, (a - (s - bp)) + (b - bp))
}

impl DoubleDouble {
    pub(crate) fn add_double(&mut self, x: f64) {
        if !x.is_finite() {
            self.special += x;
            return;
        }
        let (hi, lo) = two_sum(self.sum, x);
        self.addend += lo;
        let s = hi + self.addend;
        self.addend -= s - hi;
        self.sum = s;
    }

    pub(crate) fn add_long(&mut self, x: i64) {
        let high = x / (1i64 << 32) * (1i64 << 32);
        let low = x - high;
        #[allow(clippy::cast_precision_loss)]
        {
            self.add_double(low as f64);
            self.add_double(high as f64);
        }
    }


    pub(crate) fn to_dec(self) -> Dec {
        if self.special != 0.0 || self.special.is_nan() {
            return Dec::from_f64(self.special);
        }
        Dec::from_f64(self.sum).add(&Dec::from_f64(self.addend))
    }
}

/// Kahan's compensated sum.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Kahan {
    sum: f64,
    c: f64,
    special: f64,
}

impl Kahan {
    pub(crate) fn add(&mut self, x: f64) {
        if !x.is_finite() {
            self.special += x;
            return;
        }
        let y = x - self.c;
        let t = self.sum + y;
        self.c = (t - self.sum) - y;
        self.sum = t;
    }

    pub(crate) fn get(&self) -> f64 {
        if self.special != 0.0 || self.special.is_nan() {
            return self.special;
        }
        self.sum
    }
}

// --- sums with type promotion -------------------------------------------------------

/// `$sum` and `$avg` over numeric values: other values are skipped.
#[derive(Debug, Clone, Default)]
pub(crate) struct Summer {
    /// 0 int, 1 long, 2 double, 3 decimal: the widest type seen.
    width: u8,
    ints: i128,
    dd: DoubleDouble,
    /// The double result: a Kahan sum in input order (what MongoDB 8.0
    /// returns for double totals); `dd` keeps the exact total for decimals.
    kahan: Kahan,
    dec: Option<Dec>,
    pub(crate) count: i64,
}

impl Summer {
    /// Adds `v` when it is a number; says whether it was.
    pub(crate) fn add(&mut self, v: &Bson) -> bool {
        match v {
            Bson::Int32(i) => {
                self.ints += i128::from(*i);
                self.dd.add_long(i64::from(*i));
                self.kahan.add(f64::from(*i));
            }
            Bson::Int64(i) => {
                self.width = self.width.max(1);
                self.ints += i128::from(*i);
                self.dd.add_long(*i);
                #[allow(clippy::cast_precision_loss)]
                self.kahan.add(*i as f64);
            }
            Bson::Double(f) => {
                self.width = self.width.max(2);
                self.dd.add_double(*f);
                self.kahan.add(*f);
            }
            Bson::Decimal128(_) => {
                self.width = 3;
                let d = Dec::from_bson(v).unwrap_or(Dec::Nan);
                self.dec = Some(match self.dec.take() {
                    None => d,
                    Some(t) => t.add(&d),
                });
            }
            _ => return false,
        }
        self.count += 1;
        true
    }

    fn decimal_total(&self) -> Dec {
        self.dec
            .clone()
            .unwrap_or_else(Dec::zero)
            .add(&self.dd.to_dec())
    }

    /// The sum. `narrow` (window functions over removable windows) gives
    /// the narrowest integer type that holds an integer total.
    pub(crate) fn sum(&self, narrow: bool) -> Bson {
        match self.width {
            3 => self.decimal_total().to_bson(),
            2 => Bson::Double(self.kahan.get()),
            w => {
                if (narrow || w == 0)
                    && let Ok(i) = i32::try_from(self.ints)
                {
                    return Bson::Int32(i);
                }
                i64::try_from(self.ints).map_or_else(|_| Bson::Double(self.kahan.get()), Bson::Int64)
            }
        }
    }

    /// The average: null without numbers, decimal when one was decimal.
    pub(crate) fn avg(&self) -> Bson {
        if self.count == 0 {
            return Bson::Null;
        }
        if self.width == 3 {
            return self
                .decimal_total()
                .div(&Dec::from_i128(i128::from(self.count)))
                .to_bson();
        }
        #[allow(clippy::cast_precision_loss)]
        Bson::Double(self.kahan.get() / self.count as f64)
    }
}

/// A numeric value as a double (decimals through their string).
pub(crate) fn to_f64(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(i) => Some(f64::from(*i)),
        #[allow(clippy::cast_precision_loss)]
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        Bson::Decimal128(d) => Some(Dec::parse(&d.to_string()).map_or(f64::NAN, |x| x.to_f64())),
        _ => None,
    }
}

/// Whether `v` is a BSON number.
pub(crate) fn is_num(v: &Bson) -> bool {
    matches!(
        v,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)
    )
}

/// An integral value of a number (`2.0` and decimal `2` included).
pub(crate) fn integral(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(i) => Some(i64::from(*i)),
        Bson::Int64(i) => Some(*i),
        #[allow(clippy::cast_possible_truncation, clippy::float_cmp)]
        Bson::Double(f) if f.fract() == 0.0 && f.abs() < 9.2e18 => Some(*f as i64),
        Bson::Decimal128(_) => match Dec::from_bson(v)? {
            Dec::Fin { neg, coef, exp } if exp >= 0 => {
                let c = (coef * pow10(exp.unsigned_abs())).to_i64()?;
                Some(if neg { -c } else { c })
            }
            Dec::Fin { neg, coef, exp } => {
                let p = pow10(exp.unsigned_abs());
                if !(&coef % &p).is_zero() {
                    return None;
                }
                let c = (coef / p).to_i64()?;
                Some(if neg { -c } else { c })
            }
            _ => None,
        },
        _ => None,
    }
}

/// `a op b` for numbers with MongoDB's promotion (`+ - *`: int, long,
/// then double on overflow; double; decimal; `/`: double or decimal).
/// `None` when either isn't a number.
pub(crate) fn varith(a: &Bson, b: &Bson, op: char) -> Option<Bson> {
    if !is_num(a) || !is_num(b) {
        return None;
    }
    if matches!(a, Bson::Decimal128(_)) || matches!(b, Bson::Decimal128(_)) {
        let (x, y) = (Dec::from_bson(a)?, Dec::from_bson(b)?);
        let r = match op {
            '+' => x.add(&y),
            '-' => x.sub(&y),
            '*' => x.mul(&y),
            _ => x.div(&y),
        };
        return Some(r.to_bson());
    }
    let (fa, fb) = (to_f64(a)?, to_f64(b)?);
    let f = |x: f64, y: f64| match op {
        '+' => x + y,
        '-' => x - y,
        '*' => x * y,
        _ => x / y,
    };
    if op == '/' || matches!(a, Bson::Double(_)) || matches!(b, Bson::Double(_)) {
        return Some(Bson::Double(f(fa, fb)));
    }
    let (ia, ib) = (integral(a)?, integral(b)?);
    let r = match op {
        '+' => ia.checked_add(ib),
        '-' => ia.checked_sub(ib),
        _ => ia.checked_mul(ib),
    };
    let both_int = matches!(a, Bson::Int32(_)) && matches!(b, Bson::Int32(_));
    Some(match r {
        Some(r) if both_int => i32::try_from(r).map_or(Bson::Int64(r), Bson::Int32),
        Some(r) => Bson::Int64(r),
        None => Bson::Double(f(fa, fb)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Dec {
        Dec::parse(s).unwrap()
    }

    #[test]
    fn decimal_arithmetic() {
        assert_eq!(d("20.0").div(&d("5")).to_bson().to_string(), "4.0");
        assert_eq!(
            d("8.5").div(&d("3")).to_bson().to_string(),
            "2.833333333333333333333333333333333"
        );
        assert_eq!(d("1").div(&d("4")).to_bson().to_string(), "0.25");
        assert_eq!(
            d("6.0").add(&d("1E+40")).to_bson().to_string(),
            "1.000000000000000000000000000000000E+40"
        );
        assert_eq!(Dec::from_f64(17.5).to_bson().to_string(), "17.5");
        assert_eq!(
            Dec::from_f64(0.2).add(&d("3.1")).to_bson().to_string(),
            "3.300000000000000011102230246251565"
        );
        assert_eq!(Dec::from_f64(3.0).to_bson().to_string(), "3");
    }

    #[test]
    fn sums() {
        let mut s = Summer::default();
        for x in [0.1, 0.2, 0.3] {
            s.add(&Bson::Double(x));
        }
        assert_eq!(s.sum(false), Bson::Double(0.6));
        let mut s = Summer::default();
        s.add(&Bson::Int64(5));
        s.add(&Bson::Int64(7));
        assert_eq!(s.sum(false), Bson::Int64(12));
        assert_eq!(s.sum(true), Bson::Int32(12));
        let mut s = Summer::default();
        s.add(&Bson::Int64(i64::MAX));
        s.add(&Bson::Int32(1));
        assert_eq!(s.sum(false), Bson::Double(9_223_372_036_854_775_808.0));
    }
}
