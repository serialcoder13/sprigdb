//! Just enough DER to read a certificate's `notAfter`, for the expiry
//! metric and validation, and its subject and issuer names, for `REQUIRE
//! SUBJECT` and `ISSUER`, without an X.509 parsing dependency.
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

/// The subject and issuer of a DER certificate as OpenSSL's one-line form
/// writes them, which MySQL's `REQUIRE SUBJECT` and `ISSUER` compare
/// (`/C=US/O=Acme/CN=alice`): each attribute in certificate order, short
/// names for the common types, the dotted OID for others.
#[must_use]
pub(super) fn names(cert: &[u8]) -> Option<(String, String)> {
    let (cert, _) = tlv(cert, SEQUENCE)?;
    let (mut tbs, _) = tlv(cert, SEQUENCE)?;
    if tbs.first() == Some(&VERSION) {
        tbs = any(tbs)?.1;
    }
    let (_, rest) = tlv(tbs, INTEGER)?; // serialNumber
    let (_, rest) = tlv(rest, SEQUENCE)?; // signature
    let (issuer, rest) = tlv(rest, SEQUENCE)?;
    let (_, rest) = tlv(rest, SEQUENCE)?; // validity
    let (subject, _) = tlv(rest, SEQUENCE)?;
    Some((name(subject)?, name(issuer)?))
}

const SET: u8 = 0x31;
const OID: u8 = 0x06;

/// `Name ::= SEQUENCE OF SET OF SEQUENCE { type OID, value ANY }`.
fn name(mut rdns: &[u8]) -> Option<String> {
    let mut out = String::new();
    while !rdns.is_empty() {
        let (mut set, rest) = tlv(rdns, SET)?;
        rdns = rest;
        while !set.is_empty() {
            let (atv, rest) = tlv(set, SEQUENCE)?;
            set = rest;
            let (oid, value) = tlv(atv, OID)?;
            let (&tag, _) = value.split_first()?;
            let (v, _) = any(value)?;
            out.push('/');
            out.push_str(&attr_name(oid));
            out.push('=');
            out.push_str(&text(tag, v));
        }
    }
    Some(out)
}

/// OpenSSL's short names of the common attribute types.
fn attr_name(oid: &[u8]) -> String {
    let short = match oid {
        [0x55, 0x04, 0x03] => "CN",
        [0x55, 0x04, 0x04] => "SN",
        [0x55, 0x04, 0x05] => "serialNumber",
        [0x55, 0x04, 0x06] => "C",
        [0x55, 0x04, 0x07] => "L",
        [0x55, 0x04, 0x08] => "ST",
        [0x55, 0x04, 0x09] => "street",
        [0x55, 0x04, 0x0a] => "O",
        [0x55, 0x04, 0x0b] => "OU",
        [0x55, 0x04, 0x0c] => "title",
        [0x55, 0x04, 0x2a] => "GN",
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x09, 0x01] => "emailAddress",
        [0x09, 0x92, 0x26, 0x89, 0x93, 0xf2, 0x2c, 0x64, 0x01, 0x19] => "DC",
        [0x09, 0x92, 0x26, 0x89, 0x93, 0xf2, 0x2c, 0x64, 0x01, 0x01] => "UID",
        _ => return dotted(oid),
    };
    short.to_owned()
}

fn dotted(oid: &[u8]) -> String {
    let Some((&first, rest)) = oid.split_first() else {
        return String::new();
    };
    let mut parts = vec![u64::from(first / 40), u64::from(first % 40)];
    let mut acc = 0u64;
    for &b in rest {
        acc = (acc << 7) | u64::from(b & 0x7f);
        if b & 0x80 == 0 {
            parts.push(acc);
            acc = 0;
        }
    }
    parts
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(".")
}

/// A directory string's text: UTF-8, Printable, IA5, T61 and Universal
/// strings as bytes (Latin-1 for T61), BMP as UTF-16.
fn text(tag: u8, v: &[u8]) -> String {
    match tag {
        0x1e => {
            let units: Vec<u16> = v
                .chunks_exact(2)
                .map(|c| u16::from_be_bytes([c[0], c[1]]))
                .collect();
            String::from_utf16_lossy(&units)
        }
        0x14 => v.iter().map(|&b| char::from(b)).collect(),
        _ => String::from_utf8_lossy(v).into_owned(),
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
    fn names_in_mysql_form() {
        let mut p = rcgen::CertificateParams::new(vec!["x.test".to_owned()]).unwrap();
        p.distinguished_name = rcgen::DistinguishedName::new();
        p.distinguished_name.push(rcgen::DnType::CountryName, "SE");
        p.distinguished_name
            .push(rcgen::DnType::OrganizationName, "Acme Oy");
        p.distinguished_name
            .push(rcgen::DnType::CommonName, "alice");
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = p.self_signed(&key).unwrap();
        let (s, i) = names(cert.der()).unwrap();
        assert_eq!(s, "/C=SE/O=Acme Oy/CN=alice");
        assert_eq!(i, s);
        assert_eq!(
            dotted(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d]),
            "1.2.840.113549"
        );
        assert_eq!(names(&[0x30, 0x00]), None);
    }

    #[test]
    fn garbage_does_not_parse() {
        assert_eq!(not_after(&[]), None);
        assert_eq!(not_after(&[0x30, 0x84, 0xff, 0xff, 0xff, 0xff]), None);
        assert_eq!(not_after(&[0x30, 0x80]), None);
        assert_eq!(not_after(&[0x30, 0x03, 0x30, 0x01, 0x02]), None);
    }
}
