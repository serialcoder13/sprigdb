//! Just enough DER to read a certificate's `notAfter`, for the expiry
//! metric and validation, without an X.509 parsing dependency.
//!
//! ```text
//! Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, signature }
//! TBSCertificate ::= SEQUENCE {
//!     version [0] EXPLICIT OPTIONAL, serialNumber INTEGER,
//!     signature AlgorithmIdentifier, issuer Name,
//!     validity SEQUENCE { notBefore Time, notAfter Time }, ... }
//! Time ::= UTCTime (YYMMDDHHMMSSZ) | GeneralizedTime (YYYYMMDDHHMMSSZ)
//! ```

use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SEQUENCE: u8 = 0x30;
const INTEGER: u8 = 0x02;
const VERSION: u8 = 0xa0;
const UTC_TIME: u8 = 0x17;
const GENERALIZED_TIME: u8 = 0x18;

/// The `notAfter` of a DER certificate, or `None` if it doesn't parse.
#[must_use]
pub fn not_after(cert: &[u8]) -> Option<SystemTime> {
    let (cert, _) = tlv(cert, SEQUENCE)?;
    let (mut tbs, _) = tlv(cert, SEQUENCE)?;
    if tbs.first() == Some(&VERSION) {
        tbs = any(tbs)?.1;
    }
    let (_, rest) = tlv(tbs, INTEGER)?; // serialNumber
    let (_, rest) = tlv(rest, SEQUENCE)?; // signature
    let (_, rest) = tlv(rest, SEQUENCE)?; // issuer
    let (validity, _) = tlv(rest, SEQUENCE)?;
    let (_, validity) = any(validity)?; // notBefore
    let (&tag, _) = validity.split_first()?;
    let (time, _) = any(validity)?;
    match tag {
        UTC_TIME => {
            let yy = digits(time.get(..2)?)?;
            let year = if yy >= 50 { 1900 + yy } else { 2000 + yy };
            time_of(year, time.get(2..)?)
        }
        GENERALIZED_TIME => time_of(digits(time.get(..4)?)?, time.get(4..)?),
        _ => None,
    }
}

/// `(contents, rest)` of the element at the start of `input`, which must
/// have tag `tag`.
fn tlv(input: &[u8], tag: u8) -> Option<(&[u8], &[u8])> {
    (input.first() == Some(&tag)).then(|| any(input))?
}

/// `(contents, rest)` of the element at the start of `input`, any tag.
fn any(input: &[u8]) -> Option<(&[u8], &[u8])> {
    let (&first, rest) = input.get(1..)?.split_first()?;
    let (len, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let n = usize::from(first & 0x7f);
        if n == 0 || n > 4 {
            return None; // indefinite length is not DER; > 4 GiB is nonsense
        }
        let len = rest
            .get(..n)?
            .iter()
            .fold(0usize, |acc, &b| (acc << 8) | usize::from(b));
        (len, rest.get(n..)?)
    };
    Some((rest.get(..len)?, rest.get(len..)?))
}

/// `MMDDHHMMSSZ` after the year.
fn time_of(year: u64, rest: &[u8]) -> Option<SystemTime> {
    if rest.len() != 11 || rest[10] != b'Z' {
        return None;
    }
    let field = |i: usize| digits(&rest[i..i + 2]);
    let (month, day, hour, min, sec) = (field(0)?, field(2)?, field(4)?, field(6)?, field(8)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    let days = days_from_civil(year, month, day)?;
    let secs = days * 86_400 + hour * 3_600 + min * 60 + sec;
    UNIX_EPOCH.checked_add(Duration::from_secs(secs))
}

fn digits(b: &[u8]) -> Option<u64> {
    b.iter().try_fold(0u64, |acc, &c| {
        c.is_ascii_digit().then(|| acc * 10 + u64::from(c - b'0'))
    })
}

/// Days from 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's
/// algorithm), for dates from 1970 on.
fn days_from_civil(year: u64, month: u64, day: u64) -> Option<u64> {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe).checked_sub(719_468)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_days() {
        assert_eq!(days_from_civil(1970, 1, 1), Some(0));
        assert_eq!(days_from_civil(2000, 3, 1), Some(11_017));
        assert_eq!(days_from_civil(2038, 1, 19), Some(24_855));
        assert_eq!(days_from_civil(1969, 12, 31), None);
    }

    #[test]
    fn times() {
        let t =
            |y, s: &[u8]| time_of(y, s).map(|t| t.duration_since(UNIX_EPOCH).unwrap().as_secs());
        assert_eq!(t(2038, b"0119031407Z"), Some(2_147_483_647));
        assert_eq!(t(2038, b"0119031407"), None);
        assert_eq!(t(2038, b"1319031407Z"), None);
    }

    #[test]
    fn garbage_does_not_parse() {
        assert_eq!(not_after(&[]), None);
        assert_eq!(not_after(&[0x30, 0x84, 0xff, 0xff, 0xff, 0xff]), None);
        assert_eq!(not_after(&[0x30, 0x80]), None);
        assert_eq!(not_after(&[0x30, 0x03, 0x30, 0x01, 0x02]), None);
    }
}
