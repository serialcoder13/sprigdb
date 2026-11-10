//! Redis's number rules, so replies and refusals match Redis 7.2 byte for
//! byte.

/// Redis's `string2ll`: an optional `-`, then digits without leading zeros
/// (except `0` itself), no spaces, no `+`, within `i64`.
#[must_use]
pub fn parse_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() > 20 {
        return None;
    }
    if s == b"0" {
        return Some(0);
    }
    let (neg, digits) = match s[0] {
        b'-' => (true, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() || digits[0] == b'0' || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut v: u64 = 0;
    for d in digits {
        v = v.checked_mul(10)?.checked_add(u64::from(d - b'0'))?;
    }
    if neg {
        if v > (i64::MAX as u64) + 1 {
            return None;
        }
        Some((v as i64).wrapping_neg())
    } else {
        i64::try_from(v).ok()
    }
}

/// Redis's `string2ld` / `strtod` check for scores and floats: the whole
/// text must parse, no surrounding spaces, `inf`, `+inf`, `-inf` and
/// `infinity` accepted (any case), NaN refused.
#[must_use]
pub fn parse_f64(s: &[u8]) -> Option<f64> {
    if s.is_empty() || s.len() > 5000 {
        return None;
    }
    if s[0].is_ascii_whitespace() || s[s.len() - 1].is_ascii_whitespace() {
        return None;
    }
    let t = std::str::from_utf8(s).ok()?;
    let lower = t.to_ascii_lowercase();
    let body = lower.trim_start_matches(['+', '-']);
    if body == "inf" || body == "infinity" {
        if lower.len() - body.len() > 1 {
            return None;
        }
        return Some(if lower.starts_with('-') {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    }
    if body.starts_with("nan") {
        return None;
    }
    // strtod also takes hex floats; Rust doesn't. Redis users never send them.
    let v: f64 = t.parse().ok()?;
    if v.is_nan() { None } else { Some(v) }
}

/// Redis's `double2ll`: the integer value of `d` when it is integral and
/// within the range doubles hold exactly.
fn double_as_i64(d: f64) -> Option<i64> {
    // Redis: d < -LLONG_MAX/2 || d > LLONG_MAX/2 -> no.
    const HALF: f64 = (i64::MAX / 2) as f64;
    if !(-HALF..=HALF).contains(&d) {
        return None;
    }
    let l = d as i64;
    if (l as f64) == d { Some(l) } else { None }
}

/// The text Redis 7.2 gives a double (`d2string`): `nan`, `inf`, `-inf`,
/// `0` / `-0`, an integer when the value is one, else the shortest
/// round-trip digits laid out as `fpconv_dtoa` does.
#[must_use]
pub fn d2string(d: f64) -> String {
    if d.is_nan() {
        return "nan".into();
    }
    if d.is_infinite() {
        return if d < 0.0 { "-inf".into() } else { "inf".into() };
    }
    if d == 0.0 {
        return if d.is_sign_negative() {
            "-0".into()
        } else {
            "0".into()
        };
    }
    if let Some(l) = double_as_i64(d) {
        return l.to_string();
    }
    fpconv(d)
}

/// `fpconv_dtoa`'s layout over Rust's shortest digits.
fn fpconv(d: f64) -> String {
    let neg = d < 0.0;
    // "{:e}" is the shortest round-trip representation: "1.2345e-7".
    let sci = format!("{:e}", d.abs());
    let (mant, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp10: i32 = exp.parse().unwrap_or(0);
    let digits: Vec<u8> = mant.bytes().filter(u8::is_ascii_digit).collect();
    let ndigits = i32::try_from(digits.len()).unwrap_or(17);
    // value = digits * 10^k
    let k = exp10 - (ndigits - 1);
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    let push = |out: &mut String, b: &[u8]| out.push_str(std::str::from_utf8(b).unwrap_or(""));
    let e = (k + ndigits - 1).abs();
    if k >= 0 && e < ndigits + 7 {
        push(&mut out, &digits);
        for _ in 0..k {
            out.push('0');
        }
        return out;
    }
    if k < 0 && (k > -7 || e < 4) {
        let offset = ndigits - k.abs();
        if offset <= 0 {
            out.push_str("0.");
            for _ in 0..(-offset) {
                out.push('0');
            }
            push(&mut out, &digits);
        } else {
            let o = usize::try_from(offset).unwrap_or(0);
            push(&mut out, &digits[..o]);
            out.push('.');
            push(&mut out, &digits[o..]);
        }
        return out;
    }
    let nd = usize::try_from(ndigits.min(18 - i32::from(neg))).unwrap_or(1);
    out.push(char::from(digits[0]));
    if nd > 1 {
        out.push('.');
        push(&mut out, &digits[1..nd]);
    }
    out.push('e');
    out.push(if k + ndigits - 1 < 0 { '-' } else { '+' });
    out.push_str(&e.to_string());
    out
}

/// Redis's `%.17Lg`-free text for `INCRBYFLOAT` and `HINCRBYFLOAT`
/// (`ld2string` with `LD_STR_HUMAN`): no exponent, trailing zeros dropped.
/// SprigDB computes in `f64`, so it prints the shortest digits rather than
/// a long double's.
#[must_use]
pub fn ld2string_human(d: f64) -> String {
    if d.is_infinite() {
        return if d < 0.0 { "-inf".into() } else { "inf".into() };
    }
    if d == 0.0 {
        return "0".into();
    }
    // Shortest digits, then laid out without an exponent.
    let sci = format!("{:e}", d.abs());
    let (mant, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp10: i32 = exp.parse().unwrap_or(0);
    let digits: String = mant.chars().filter(char::is_ascii_digit).collect();
    let nd = i32::try_from(digits.len()).unwrap_or(17);
    let mut s = String::new();
    if d < 0.0 {
        s.push('-');
    }
    if exp10 >= nd - 1 {
        s.push_str(&digits);
        for _ in 0..(exp10 - (nd - 1)) {
            s.push('0');
        }
    } else if exp10 >= 0 {
        let p = usize::try_from(exp10 + 1).unwrap_or(1);
        s.push_str(&digits[..p]);
        s.push('.');
        s.push_str(&digits[p..]);
    } else {
        s.push_str("0.");
        for _ in 0..(-exp10 - 1) {
            s.push('0');
        }
        s.push_str(&digits);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_follow_string2ll() {
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"-1"), Some(-1));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
        for bad in [
            &b""[..],
            b"01",
            b"+1",
            b" 1",
            b"1 ",
            b"-0",
            b"-",
            b"9223372036854775808",
            b"1.0",
        ] {
            assert_eq!(parse_i64(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn doubles_print_as_redis_does() {
        for (d, s) in [
            (1.5, "1.5"),
            (3.0, "3"),
            (-0.0, "-0"),
            (0.1, "0.1"),
            (1e20, "1e+20"),
            (1e30, "1e+30"),
            (1.23e-5, "1.23e-5"),
            (0.001, "0.001"),
            (123_456.789, "123456.789"),
            (f64::INFINITY, "inf"),
            (-2.5e-10, "-2.5e-10"),
            (3.14e100, "3.14e+100"),
        ] {
            assert_eq!(d2string(d), s, "{d}");
        }
    }

    #[test]
    fn floats_parse_as_strtod() {
        assert_eq!(parse_f64(b"1.5"), Some(1.5));
        assert_eq!(parse_f64(b"+inf"), Some(f64::INFINITY));
        assert_eq!(parse_f64(b"-Inf"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_f64(b"1e3"), Some(1000.0));
        assert_eq!(parse_f64(b"nan"), None);
        assert_eq!(parse_f64(b" 1"), None);
        assert_eq!(parse_f64(b"abc"), None);
    }

    #[test]
    fn human_long_doubles() {
        assert_eq!(ld2string_human(10.5), "10.5");
        assert_eq!(ld2string_human(3.0), "3");
        assert_eq!(ld2string_human(5e20), "500000000000000000000");
        assert_eq!(ld2string_human(1.5e-5), "0.000015");
        assert_eq!(ld2string_human(-2.25), "-2.25");
    }
}
