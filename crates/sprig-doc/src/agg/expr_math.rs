//! Arithmetic, trigonometric and bitwise expressions (M1): `$exp`,
//! `$ln`, `$log`, `$log10`, `$pow`, `$sqrt`, the trigonometric family,
//! `$degreesToRadians`, `$radiansToDegrees` and the `$bit*` operators.
//!
//! Integers give doubles and decimals give decimals, as in MongoDB;
//! decimal results are computed to 34 digits in [`super::expr_decimal`].

use bson::{Bson, Decimal128};
use sprig_proto_mongo::errors::MongoError;

use super::Vars;
use super::expr_common::{
    dec_from_f64, err, eval_all, exact_args, fmt_num6, is_nullish, is_num, one, tname, to_f64,
};

fn num_result(input: &Bson, f: f64) -> Bson {
    if matches!(input, Bson::Decimal128(_)) {
        Bson::Decimal128(dec_from_f64(f))
    } else {
        Bson::Double(f)
    }
}

/// A number as an exact decimal value (doubles as MongoDB widens them).
fn as_dec(v: &Bson) -> Option<super::expr_decimal::Dec> {
    let d = match v {
        Bson::Decimal128(d) => *d,
        Bson::Int32(i) => super::expr_common::dec_from_i64(i64::from(*i)),
        Bson::Int64(i) => super::expr_common::dec_from_i64(*i),
        other => dec_from_f64(to_f64(other)),
    };
    super::expr_decimal::parse(&d)
}

fn dec_exact(s: &str) -> Bson {
    s.parse::<Decimal128>().map_or(Bson::Null, Bson::Decimal128)
}

/// One-argument numeric operators.
fn unary(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let v = one(op, arg, vars)?;
    if is_nullish(&v) {
        return Ok(Bson::Null);
    }
    if !is_num(&v) {
        return Err(err(
            28765,
            format!("{op} only supports numeric types, not {}", tname(&v)),
        ));
    }
    let x = to_f64(&v);
    let domain = |lo: &str, ok: bool| -> Result<(), MongoError> {
        if ok || x.is_nan() {
            Ok(())
        } else {
            Err(err(
                50989,
                format!(
                    "cannot apply {op} to {}, value must be in {lo}",
                    fmt_num6(&v)
                ),
            ))
        }
    };
    // Finite decimals: to 34 digits (after the domain checks below).
    let dec = match &v {
        Bson::Decimal128(_) => as_dec(&v),
        _ => None,
    };
    let r = match op {
        "$exp" => x.exp(),
        "$ln" | "$log10" => {
            if x <= 0.0 {
                return Err(err(
                    if op == "$ln" { 28766 } else { 28761 },
                    format!(
                        "{op}'s argument must be a positive number, but is {}",
                        fmt_num6(&v)
                    ),
                ));
            }
            if op == "$ln" { x.ln() } else { x.log10() }
        }
        "$sqrt" => {
            if x < 0.0 {
                return Err(err(
                    28714,
                    "$sqrt's argument must be greater than or equal to 0",
                ));
            }
            x.sqrt()
        }
        "$sin" | "$cos" | "$tan" => {
            domain("(-inf,inf)", x.is_finite())?;
            if x.is_nan() {
                x
            } else {
                super::expr_decimal::f64_trig(op, x)
            }
        }
        "$asin" | "$acos" | "$atanh" => {
            domain("[-1,1]", (-1.0..=1.0).contains(&x))?;
            match op {
                _ if x.is_nan() => x,
                "$atanh" => x.atanh(),
                _ => super::expr_decimal::f64_trig(op, x),
            }
        }
        "$acosh" => {
            domain("[1,inf]", x >= 1.0)?;
            x.acosh()
        }
        "$atan" if x.is_finite() => super::expr_decimal::f64_trig(op, x),
        "$atan" => x.atan(),
        "$sinh" => x.sinh(),
        "$cosh" => x.cosh(),
        "$tanh" => x.tanh(),
        "$asinh" => x.asinh(),
        "$degreesToRadians" => x * (std::f64::consts::PI / 180.0),
        _ => x * (180.0 / std::f64::consts::PI),
    };
    if let Some(d) = dec {
        return Ok(Bson::Decimal128(super::expr_decimal::unary(op, &d)));
    }
    Ok(num_result(&v, r))
}

fn log(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let a = eval_all(&exact_args("$log", arg, 2)?, vars)?;
    let (x, b) = (&a[0], &a[1]);
    if is_nullish(x) || is_nullish(b) {
        return Ok(Bson::Null);
    }
    if !is_num(x) {
        return Err(err(
            28756,
            format!("$log's argument must be numeric, not {}", tname(x)),
        ));
    }
    if !is_num(b) {
        return Err(err(
            28757,
            format!("$log's base must be numeric, not {}", tname(b)),
        ));
    }
    let (xf, bf) = (to_f64(x), to_f64(b));
    if xf <= 0.0 {
        return Err(err(
            28758,
            format!(
                "$log's argument must be a positive number, but is {}",
                fmt_num6(x)
            ),
        ));
    }
    if bf <= 0.0 || (bf - 1.0).abs() < f64::EPSILON / 4.0 {
        return Err(err(
            28759,
            format!(
                "$log's base must be a positive number not equal to 1, but is {}",
                fmt_num6(b)
            ),
        ));
    }
    let r = xf.ln() / bf.ln();
    if matches!(x, Bson::Decimal128(_)) || matches!(b, Bson::Decimal128(_)) {
        if let (Some(dx), Some(db)) = (as_dec(x), as_dec(b)) {
            return Ok(Bson::Decimal128(super::expr_decimal::log(&dx, &db)));
        }
        return Ok(Bson::Decimal128(dec_from_f64(r)));
    }
    // Exact powers of the base give the exact integer, as `log(x)/log(b)`
    // in long double does in MongoDB.
    if (r - r.round()).abs() < 1e-12 && (bf.powf(r.round()) - xf).abs() <= 0.0 {
        return Ok(Bson::Double(r.round()));
    }
    Ok(Bson::Double(r))
}

fn ipow(base: i64, exp: i64) -> Option<i64> {
    let e = u32::try_from(exp).ok()?;
    base.checked_pow(e)
}

fn pow(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let a = eval_all(&exact_args("$pow", arg, 2)?, vars)?;
    let (b, e) = (&a[0], &a[1]);
    if is_nullish(b) || is_nullish(e) {
        return Ok(Bson::Null);
    }
    if !is_num(b) {
        return Err(err(
            28762,
            format!("$pow's base must be numeric, not {}", tname(b)),
        ));
    }
    if !is_num(e) {
        return Err(err(
            28763,
            format!("$pow's exponent must be numeric, not {}", tname(e)),
        ));
    }
    let (bf, ef) = (to_f64(b), to_f64(e));
    if bf == 0.0 && ef < 0.0 {
        return Err(err(
            28764,
            "$pow cannot take a base of 0 and a negative exponent",
        ));
    }
    if matches!(b, Bson::Decimal128(_)) || matches!(e, Bson::Decimal128(_)) {
        if let (Some(db), Some(de)) = (as_dec(b), as_dec(e)) {
            return Ok(Bson::Decimal128(
                super::expr_decimal::pow(&db, &de).unwrap_or_else(|| dec_from_f64(f64::NAN)),
            ));
        }
        let r = bf.powf(ef);
        if r.fract() == 0.0 && r.abs() < 1e15 {
            // MongoDB's decimal power keeps 34 digits: 8.000…0.
            #[allow(clippy::cast_possible_truncation)]
            let i = r as i64;
            let digits = i.unsigned_abs().to_string().len();
            let frac = 34usize.saturating_sub(digits);
            return Ok(dec_exact(&if frac == 0 {
                i.to_string()
            } else {
                format!("{i}.{}", "0".repeat(frac))
            }));
        }
        return Ok(Bson::Decimal128(dec_from_f64(r)));
    }
    if matches!(b, Bson::Double(_)) || matches!(e, Bson::Double(_)) {
        return Ok(Bson::Double(bf.powf(ef)));
    }
    // Both integers.
    let (bi, ei) = (
        super::expr_common::integral_i64(b),
        super::expr_common::integral_i64(e),
    );
    let (Some(bi), Some(ei)) = (bi, ei) else {
        return Ok(Bson::Double(bf.powf(ef)));
    };
    let both_int = matches!(b, Bson::Int32(_)) && matches!(e, Bson::Int32(_));
    let int_out = |r: i64| -> Bson {
        if both_int {
            i32::try_from(r).map_or(Bson::Int64(r), Bson::Int32)
        } else {
            Bson::Int64(r)
        }
    };
    if ei < 0 {
        return Ok(match bi {
            1 => int_out(1),
            -1 => int_out(if ei % 2 == 0 { 1 } else { -1 }),
            _ => Bson::Double(bf.powf(ef)),
        });
    }
    Ok(match ipow(bi, ei) {
        Some(r) => int_out(r),
        None => Bson::Double(bf.powf(ef)),
    })
}

fn atan2(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let a = eval_all(&exact_args("$atan2", arg, 2)?, vars)?;
    let (y, x) = (&a[0], &a[1]);
    if is_nullish(y) || is_nullish(x) {
        return Ok(Bson::Null);
    }
    for v in [y, x] {
        if !is_num(v) {
            return Err(err(
                51044,
                format!("$atan2 only supports numeric types, not {}", tname(v)),
            ));
        }
    }
    let r = super::expr_decimal::f64_atan2(to_f64(y), to_f64(x));
    if matches!(y, Bson::Decimal128(_)) || matches!(x, Bson::Decimal128(_)) {
        if let (Some(dy), Some(dx)) = (as_dec(y), as_dec(x)) {
            return Ok(Bson::Decimal128(super::expr_decimal::atan2(&dy, &dx)));
        }
        return Ok(Bson::Decimal128(dec_from_f64(r)));
    }
    Ok(Bson::Double(r))
}

fn bitwise(op: &str, arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let list: Vec<Bson> = super::expr_common::arg_list(arg)
        .into_iter()
        .cloned()
        .collect();
    let operands = eval_all(&list, vars)?;
    let mut acc: i64 = if op == "$bitAnd" { -1 } else { 0 };
    let mut long = false;
    for v in &operands {
        let x = match v {
            Bson::Int32(i) => i64::from(*i),
            Bson::Int64(i) => {
                long = true;
                *i
            }
            v if is_nullish(v) => return Ok(Bson::Null),
            _ => {
                return Err(err(
                    14,
                    format!("{op} only supports int and long operands."),
                ));
            }
        };
        acc = match op {
            "$bitAnd" => acc & x,
            "$bitOr" => acc | x,
            _ => acc ^ x,
        };
    }
    Ok(if long {
        Bson::Int64(acc)
    } else {
        #[allow(clippy::cast_possible_truncation)]
        Bson::Int32(acc as i32)
    })
}

fn bit_not(arg: &Bson, vars: &Vars) -> Result<Bson, MongoError> {
    let v = one("$bitNot", arg, vars)?;
    Ok(match v {
        Bson::Int32(i) => Bson::Int32(!i),
        Bson::Int64(i) => Bson::Int64(!i),
        v if is_nullish(&v) => Bson::Null,
        v if is_num(&v) => {
            return Err(err(
                14,
                format!("$bitNot only supports int and long, not: {}.", tname(&v)),
            ));
        }
        v => {
            return Err(err(
                28765,
                format!("$bitNot only supports numeric types, not {}", tname(&v)),
            ));
        }
    })
}

/// This module's operators.
pub(super) fn eval_op(op: &str, arg: &Bson, vars: &Vars) -> Option<Result<Bson, MongoError>> {
    Some(match op {
        "$exp" | "$ln" | "$log10" | "$sqrt" | "$sin" | "$cos" | "$tan" | "$asin" | "$acos"
        | "$atan" | "$sinh" | "$cosh" | "$tanh" | "$asinh" | "$acosh" | "$atanh"
        | "$degreesToRadians" | "$radiansToDegrees" => unary(op, arg, vars),
        "$log" => log(arg, vars),
        "$pow" => pow(arg, vars),
        "$atan2" => atan2(arg, vars),
        "$bitAnd" | "$bitOr" | "$bitXor" => bitwise(op, arg, vars),
        "$bitNot" => bit_not(arg, vars),
        _ => return None,
    })
}
