//! Decimal128 arithmetic for the transcendental expressions (M1): `$exp`,
//! `$ln`, `$log`, `$log10`, `$pow`, `$sqrt`, the trigonometric family and
//! the degree/radian conversions on decimals, to MongoDB's 34 significant
//! digits (decision S36).
//!
//! Values are worked in fixed point with 90 decimal places on big
//! integers, then rounded half-even to 34 digits as the IEEE 754 decimal
//! library MongoDB uses does. Exact results follow that library's rules:
//! exact quotients and square roots take the ideal exponent (`log2 8` is
//! `3`, `sqrt 4` is `2`), products are exact up to 34 digits.

use std::sync::OnceLock;

use bson::Decimal128;
use num_bigint::BigInt;
use num_traits::{One, Signed, Zero};

/// Working precision: values are fixed point scaled by 2^bits, with
/// about 90 decimal places for decimals and 40 for the doubles of
/// [`f64_trig`] (enough to round a double correctly). A binary scale makes
/// the rescaling after each product a shift.
const P: u32 = 90;
const P_LOW: u32 = 40;
const BITS: u32 = 300;
const BITS_LOW: u32 = 136;

thread_local! {
    static LOW: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn low() -> bool {
    LOW.with(std::cell::Cell::get)
}

/// Runs `f` at the lower precision.
fn at_low_precision<T>(f: impl FnOnce() -> T) -> T {
    LOW.with(|l| l.set(true));
    let r = f();
    LOW.with(|l| l.set(false));
    r
}

/// Decimal places worth of the scale.
fn prec() -> u32 {
    if low() { P_LOW } else { P }
}

fn bits() -> u32 {
    if low() { BITS_LOW } else { BITS }
}

fn ten_pow(n: u32) -> BigInt {
    BigInt::from(10u8).pow(n)
}

/// A constant at each precision.
struct Both(OnceLock<BigInt>, OnceLock<BigInt>);

impl Both {
    const fn new() -> Self {
        Self(OnceLock::new(), OnceLock::new())
    }
    fn get(&'static self, f: impl FnOnce() -> BigInt) -> &'static BigInt {
        if low() {
            self.1.get_or_init(f)
        } else {
            self.0.get_or_init(f)
        }
    }
}

fn s() -> &'static BigInt {
    static S: Both = Both::new();
    S.get(|| BigInt::one() << usize::try_from(bits()).unwrap_or(0))
}

/// A finite decimal: `coef × 10^exp`.
#[derive(Clone, Debug)]
pub(super) struct Dec {
    pub coef: BigInt,
    pub exp: i64,
}

/// A decimal's exact value; `None` for NaN and the infinities.
pub(super) fn parse(d: &Decimal128) -> Option<Dec> {
    let t = d.to_string();
    if t.contains("NaN") || t.contains("Inf") {
        return None;
    }
    let (neg, body) = t
        .strip_prefix('-')
        .map_or((false, t.as_str()), |r| (true, r));
    let (mant, e) = match body.find(['E', 'e']) {
        Some(i) => (&body[..i], body[i + 1..].parse::<i64>().ok()?),
        None => (body, 0),
    };
    let (ip, fp) = mant.split_once('.').unwrap_or((mant, ""));
    let coef: BigInt = format!("{ip}{fp}").parse().ok()?;
    let exp = e - i64::try_from(fp.len()).ok()?;
    Some(Dec {
        coef: if neg { -coef } else { coef },
        exp,
    })
}

fn to_decimal(neg: bool, coef: &BigInt, exp: i64) -> Decimal128 {
    let t = format!("{}{}E{exp}", if neg { "-" } else { "" }, coef.abs());
    t.parse()
        .unwrap_or_else(|_| super::expr_common::dec_parse("NaN"))
}

/// The exact decimal back (coefficients of up to 34 digits).
pub(super) fn exact(d: &Dec) -> Decimal128 {
    to_decimal(d.coef.is_negative(), &d.coef, d.exp)
}

/// Fixed point (scaled by 2^bits) of a decimal.
pub(super) fn fixed(d: &Dec) -> BigInt {
    if d.exp >= 0 {
        (&d.coef * ten_pow(u32::try_from(d.exp).unwrap_or(0)))
            << usize::try_from(bits()).unwrap_or(0)
    } else {
        let p = u32::try_from(-d.exp).unwrap_or(u32::MAX);
        if p > 7000 {
            BigInt::zero()
        } else {
            (&d.coef << usize::try_from(bits()).unwrap_or(0)) / ten_pow(p)
        }
    }
}

/// A fixed-point value as an integer scaled by 10^prec.
fn to_decimal_scale(x: &BigInt) -> BigInt {
    (x * ten_pow(prec())) >> usize::try_from(bits()).unwrap_or(0)
}

/// Rounds `coef × 10^exp` half-even to at most 34 digits.
fn round_coef(coef: &BigInt, exp: i64, keep_zeros: bool) -> Dec {
    let neg = coef.is_negative();
    let digits = coef.abs().to_string();
    let n = digits.len();
    if n <= 34 {
        if keep_zeros && n < 34 && !coef.is_zero() {
            let pad = u32::try_from(34 - n).unwrap_or(0);
            return Dec {
                coef: coef * ten_pow(pad),
                exp: exp - i64::from(pad),
            };
        }
        return Dec {
            coef: coef.clone(),
            exp,
        };
    }
    let mut head: BigInt = digits[..34].parse().unwrap_or_default();
    let rest = &digits[34..];
    let first = rest.as_bytes()[0];
    let tail_nonzero = rest[1..].bytes().any(|b| b != b'0');
    let up = first > b'5' || (first == b'5' && (tail_nonzero || head.is_odd_big()));
    if up {
        head += 1;
    }
    let mut e = exp + i64::try_from(n - 34).unwrap_or(0);
    if head.to_string().len() > 34 {
        head /= 10;
        e += 1;
    }
    Dec {
        coef: if neg { -head } else { head },
        exp: e,
    }
}

trait OddBig {
    fn is_odd_big(&self) -> bool;
}

impl OddBig for BigInt {
    fn is_odd_big(&self) -> bool {
        (self % 2u8) != BigInt::zero()
    }
}

/// A fixed-point value rounded to 34 significant digits.
pub(super) fn round34(x: &BigInt) -> Dec {
    if x.is_zero() {
        return Dec {
            coef: BigInt::zero(),
            exp: 0,
        };
    }
    round_coef(&to_decimal_scale(x), -i64::from(prec()), true)
}

fn mul(a: &BigInt, b: &BigInt) -> BigInt {
    (a * b) >> usize::try_from(bits()).unwrap_or(0)
}

fn div(a: &BigInt, b: &BigInt) -> BigInt {
    (a << usize::try_from(bits()).unwrap_or(0)) / b
}

fn ln2() -> &'static BigInt {
    static L: Both = Both::new();
    L.get(|| {
        let two = s() * 2u8;
        ln_reduced(&two)
    })
}

fn ln10() -> &'static BigInt {
    static L: Both = Both::new();
    L.get(|| ln(&(s() * 10u8)))
}

/// `2 atanh((m − 1)/(m + 1))` = `ln m`, for m near 1.
fn ln_reduced(m: &BigInt) -> BigInt {
    let z = div(&(m - s()), &(m + s()));
    let z2 = mul(&z, &z);
    let mut term = z;
    let mut sum = BigInt::zero();
    let mut n = 1u32;
    while !term.is_zero() {
        sum += &term / n;
        term = mul(&term, &z2);
        n += 2;
    }
    sum * 2u8
}

/// Natural logarithm of a positive fixed-point value.
pub(super) fn ln(x: &BigInt) -> BigInt {
    let k = i64::try_from(x.bits()).unwrap_or(0) - i64::try_from(s().bits()).unwrap_or(0);
    let m = if k >= 0 {
        x >> usize::try_from(k).unwrap_or(0)
    } else {
        x << usize::try_from(-k).unwrap_or(0)
    };
    ln_reduced(&m) + ln2() * BigInt::from(k)
}

/// `e^x` of a fixed-point value.
pub(super) fn exp(x: &BigInt) -> BigInt {
    let k = (x / ln2()).clone();
    let r = x - ln2() * &k;
    // e^r with r in (−ln 2, ln 2): halve 12 times, Taylor, square back.
    let m = 12u32;
    let r = r >> usize::try_from(m).unwrap_or(0);
    let mut sum = s().clone();
    let mut term = s().clone();
    let mut n = 1u32;
    loop {
        term = mul(&term, &r) / n;
        if term.is_zero() {
            break;
        }
        sum += &term;
        n += 1;
    }
    for _ in 0..m {
        sum = mul(&sum, &sum);
    }
    let k: i64 = i64::try_from(&k).unwrap_or(0);
    if k >= 0 {
        sum << usize::try_from(k).unwrap_or(0)
    } else {
        sum >> usize::try_from(-k).unwrap_or(0)
    }
}

pub(super) fn sqrt(x: &BigInt) -> BigInt {
    (x * s()).sqrt()
}

/// Taylor series of `atan x`, for small |x|.
fn atan_series(x: &BigInt) -> BigInt {
    let x2 = mul(x, x);
    let mut term = x.clone();
    let mut sum = BigInt::zero();
    let mut n = 1u32;
    let mut neg = false;
    while !term.is_zero() {
        let t = &term / n;
        if neg {
            sum -= t;
        } else {
            sum += t;
        }
        term = mul(&term, &x2);
        n += 2;
        neg = !neg;
    }
    sum
}

pub(super) fn pi() -> &'static BigInt {
    static PI: Both = Both::new();
    PI.get(|| {
        // Machin: π = 16 atan(1/5) − 4 atan(1/239).
        let a = atan_series(&(s() / 5u8));
        let b = atan_series(&(s() / 239u8));
        a * 16u8 - b * 4u8
    })
}

pub(super) fn atan(x: &BigInt) -> BigInt {
    if x.is_negative() {
        return -atan(&-x);
    }
    if x > s() {
        return pi() / 2u8 - atan(&div(s(), x));
    }
    // Halve the angle three times: atan x = 2 atan(x / (1 + √(1 + x²))).
    let mut y = x.clone();
    for _ in 0..3 {
        let r = sqrt(&(s() + mul(&y, &y)));
        y = div(&y, &(s() + r));
    }
    atan_series(&y) * 8u8
}

fn reduce_angle(x: &BigInt) -> BigInt {
    let two_pi = pi() * 2u8;
    let mut r = x % &two_pi;
    if r > *pi() {
        r -= &two_pi;
    } else if r < -pi().clone() {
        r += &two_pi;
    }
    r
}

pub(super) fn sin(x: &BigInt) -> BigInt {
    let r = reduce_angle(x);
    let r2 = mul(&r, &r);
    let mut term = r.clone();
    let mut sum = BigInt::zero();
    let mut n = 1u32;
    while !term.is_zero() {
        sum += &term;
        term = -mul(&term, &r2) / ((n + 1) * (n + 2));
        n += 2;
    }
    sum
}

pub(super) fn cos(x: &BigInt) -> BigInt {
    let r = reduce_angle(x);
    let r2 = mul(&r, &r);
    let mut term = s().clone();
    let mut sum = BigInt::zero();
    let mut n = 0u32;
    while !term.is_zero() {
        sum += &term;
        term = -mul(&term, &r2) / ((n + 1) * (n + 2));
        n += 2;
    }
    sum
}

/// The decimal functions of one argument, already checked for domain.
pub(super) fn unary(op: &str, d: &Dec) -> Decimal128 {
    let x = fixed(d);
    let one = s();
    let r = match op {
        "$exp" => exp(&x),
        "$ln" => {
            if x == *one {
                return exact(&Dec {
                    coef: BigInt::zero(),
                    exp: 0,
                });
            }
            ln(&x)
        }
        "$log10" => return log_base(d, 10),
        "$sqrt" => return sqrt_dec(d),
        "$sin" => sin(&x),
        "$cos" => cos(&x),
        "$tan" => div(&sin(&x), &cos(&x)),
        "$asin" | "$acos" => {
            let a = if x.abs() == *one {
                if x.is_negative() {
                    -(pi() / 2u8)
                } else {
                    pi() / 2u8
                }
            } else {
                atan(&div(&x, &sqrt(&(one - mul(&x, &x)))))
            };
            if op == "$asin" { a } else { pi() / 2u8 - a }
        }
        "$atan" => atan(&x),
        "$sinh" | "$cosh" | "$tanh" => {
            let (e, ei) = (exp(&x), exp(&-&x));
            match op {
                "$sinh" => (&e - &ei) / 2u8,
                "$cosh" => (&e + &ei) / 2u8,
                _ => div(&(&e - &ei), &(&e + &ei)),
            }
        }
        "$asinh" => {
            let a = x.abs();
            let v = ln(&(&a + sqrt(&(mul(&a, &a) + one))));
            if x.is_negative() { -v } else { v }
        }
        "$acosh" => ln(&(&x + sqrt(&(mul(&x, &x) - one)))),
        "$atanh" => ln(&div(&(one + &x), &(one - &x))) / 2u8,
        "$degreesToRadians" => return mul_exact(d, &pi_over_180()),
        _ => return mul_exact(d, &d180_over_pi()),
    };
    exact(&round34(&r))
}

fn pi_over_180() -> Dec {
    round34(&(pi() / 180u8))
}

fn d180_over_pi() -> Dec {
    round34(&div(&(s() * 180u8), pi()))
}

/// IEEE decimal multiplication: the exact product, rounded to 34 digits
/// when longer.
fn mul_exact(a: &Dec, b: &Dec) -> Decimal128 {
    exact(&round_coef(&(&a.coef * &b.coef), a.exp + b.exp, false))
}

/// IEEE decimal division: exact quotients take the exponent closest to
/// the ideal one (`a.exp − b.exp`); others are rounded to 34 digits.
pub(super) fn div_exact(a: &Dec, b: &Dec) -> Decimal128 {
    if a.coef.is_zero() {
        return exact(&Dec {
            coef: BigInt::zero(),
            exp: a.exp - b.exp,
        });
    }
    for k in 0..=40u32 {
        let num = &a.coef * ten_pow(k);
        if (&num % &b.coef).is_zero() {
            let q = &num / &b.coef;
            if q.abs().to_string().len() <= 34 {
                return exact(&Dec {
                    coef: q,
                    exp: a.exp - b.exp - i64::from(k),
                });
            }
            break;
        }
    }
    exact(&round34(&div(&fixed(a), &fixed(b))))
}

/// `log_base(x)` for base 2 or 10: exact integers when `x` is a power of
/// the base.
pub(super) fn log_base(d: &Dec, base: u8) -> Decimal128 {
    let x = fixed(d);
    let lb = if base == 10 {
        ln10().clone()
    } else {
        ln2().clone()
    };
    let v = div(&ln(&x), &lb);
    // An integer result (within the working precision) is exact.
    let half: BigInt = s() / 2u8;
    let rounded: BigInt = if v.is_negative() {
        (&v - &half) / s()
    } else {
        (&v + &half) / s()
    };
    let back: BigInt = &rounded * s();
    let gap: BigInt = &v - &back;
    if gap.abs() < (s() >> 200usize) {
        return exact(&Dec {
            coef: rounded,
            exp: 0,
        });
    }
    exact(&round34(&v))
}

/// `$log` with a decimal operand: base 2 and 10 directly, others as
/// `ln x / ln b` of 34-digit values.
pub(super) fn log(x: &Dec, b: &Dec) -> Decimal128 {
    let bf = fixed(b);
    if bf == s() * 2u8 {
        return log_base(x, 2);
    }
    if bf == s() * 10u8 {
        return log_base(x, 10);
    }
    let lx = round34(&ln(&fixed(x)));
    let lb = round34(&ln(&bf));
    div_exact(&lx, &lb)
}

/// IEEE square root: exact roots take the ideal exponent.
pub(super) fn sqrt_dec(d: &Dec) -> Decimal128 {
    let (mut c, mut e) = (d.coef.clone(), d.exp);
    if e % 2 != 0 {
        c *= 10u8;
        e -= 1;
    }
    let r = c.sqrt();
    if &r * &r == c {
        return exact(&round_coef(&r, e / 2, false));
    }
    exact(&round34(&sqrt(&fixed(d))))
}

/// `x^y` with a decimal operand, to 34 digits.
pub(super) fn pow(x: &Dec, y: &Dec) -> Option<Decimal128> {
    let (xf, yf) = (fixed(x), fixed(y));
    if yf.is_zero() {
        return Some(exact(&Dec {
            coef: BigInt::one(),
            exp: 0,
        }));
    }
    if xf.is_zero() {
        return Some(exact(&Dec {
            coef: BigInt::zero(),
            exp: 0,
        }));
    }
    let y_int = (&yf % s()).is_zero();
    if xf.is_negative() && !y_int {
        return None;
    }
    let mag = exp(&mul(&yf, &ln(&xf.abs())));
    let odd = y_int && ((&yf / s()) % 2u8) != BigInt::zero();
    let v = if xf.is_negative() && odd { -mag } else { mag };
    Some(exact(&round34(&v)))
}

/// `atan2(y, x)` to 34 digits.
pub(super) fn atan2(y: &Dec, x: &Dec) -> Decimal128 {
    let (yf, xf) = (fixed(y), fixed(x));
    let r = if xf.is_zero() {
        if yf.is_negative() {
            -(pi() / 2u8)
        } else if yf.is_zero() {
            BigInt::zero()
        } else {
            pi() / 2u8
        }
    } else {
        let a = atan(&div(&yf, &xf));
        if xf.is_positive() {
            a
        } else if yf.is_negative() {
            a - pi()
        } else {
            a + pi()
        }
    };
    exact(&round34(&r))
}

/// A double's exact value in fixed point.
fn fixed_from_f64(x: f64) -> BigInt {
    let bits = x.to_bits();
    let neg = bits >> 63 == 1;
    let exp_bits = i64::try_from((bits >> 52) & 0x7ff).unwrap_or(0);
    let frac = bits & ((1u64 << 52) - 1);
    let (m, e) = if exp_bits == 0 {
        (frac, -1074)
    } else {
        (frac | (1u64 << 52), exp_bits - 1075)
    };
    let mut v = BigInt::from(m) * s();
    if e >= 0 {
        v <<= usize::try_from(e).unwrap_or(0);
    } else {
        v >>= usize::try_from(-e).unwrap_or(0);
    }
    if neg { -v } else { v }
}

/// The double nearest a fixed-point value.
fn f64_from_fixed(v: &BigInt) -> f64 {
    format!("{}e-{}", to_decimal_scale(v), prec())
        .parse()
        .unwrap_or(f64::NAN)
}

/// Correctly rounded `sin`, `cos`, `tan`, `asin`, `acos` and `atan` of a
/// finite double in the function's domain, as glibc (MongoDB's libm on
/// Linux) gives them; the platform's libm can be a unit off.
pub(super) fn f64_trig(op: &str, x: f64) -> f64 {
    // Large arguments need π to more places to reduce exactly.
    if x.abs() < 1e15 {
        at_low_precision(|| f64_trig_at(op, x))
    } else {
        f64_trig_at(op, x)
    }
}

fn f64_trig_at(op: &str, x: f64) -> f64 {
    // Tiny arguments: the result rounds to x (to 1 for cos).
    if x.abs() < 1e-9 && op != "$acos" {
        return if op == "$cos" { 1.0 } else { x };
    }
    let fx = fixed_from_f64(x);
    let one = s();
    let r = match op {
        "$sin" => sin(&fx),
        "$cos" => cos(&fx),
        "$tan" => div(&sin(&fx), &cos(&fx)),
        "$asin" | "$acos" => {
            let a = if fx.abs() == *one {
                if fx.is_negative() {
                    -(pi() / 2u8)
                } else {
                    pi() / 2u8
                }
            } else {
                atan(&div(&fx, &sqrt(&(one - mul(&fx, &fx)))))
            };
            if op == "$asin" { a } else { pi() / 2u8 - a }
        }
        _ => atan(&fx),
    };
    f64_from_fixed(&r)
}

/// Correctly rounded `atan2` of finite doubles.
pub(super) fn f64_atan2(y: f64, x: f64) -> f64 {
    at_low_precision(|| f64_atan2_at(y, x))
}

fn f64_atan2_at(y: f64, x: f64) -> f64 {
    if !(y.is_finite() && x.is_finite()) || x == 0.0 || y == 0.0 {
        return y.atan2(x);
    }
    let (yf, xf) = (fixed_from_f64(y), fixed_from_f64(x));
    let a = atan(&div(&yf, &xf));
    let r = if xf.is_positive() {
        a
    } else if yf.is_negative() {
        a - pi()
    } else {
        a + pi()
    };
    f64_from_fixed(&r)
}
