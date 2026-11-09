//! AWS Signature Version 4 for S3 requests, written in-house on `ring`
//! (HMAC-SHA256 and SHA-256): the `aws-sigv4` crate would bring the
//! aws-smithy stack and a second `http` version for about 150 lines.
//! Checked against AWS's signing test suite (`get-vanilla`,
//! `post-vanilla`, `get-vanilla-query-order-key-case`,
//! `post-x-www-form-urlencoded`) and live against `MinIO`, which checks
//! signatures strictly.
//!
//! ```text
//! canonical request = method \n uri \n sorted query \n
//!                     lower-case headers "name:value\n"... \n signed names \n payload hash
//! string to sign    = "AWS4-HMAC-SHA256" \n date-time \n date/region/service/aws4_request \n
//!                     hex(SHA-256(canonical request))
//! key               = HMAC(HMAC(HMAC(HMAC("AWS4" ‖ secret, date), region), service), "aws4_request")
//! ```

use ring::{digest, hmac};
use zeroize::Zeroizing;

use crate::model::hex;

/// Static credentials (zeroized; never printed).
#[derive(Clone)]
pub struct Creds {
    /// The access key id.
    pub access_key_id: String,
    /// The secret access key.
    pub secret: Zeroizing<String>,
    /// A session token (temporary credentials).
    pub session_token: Option<Zeroizing<String>>,
}

impl std::fmt::Debug for Creds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Creds")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

/// Hex SHA-256.
#[must_use]
pub fn sha256_hex(b: &[u8]) -> String {
    hex(digest::digest(&digest::SHA256, b).as_ref())
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&k, data).as_ref().to_vec()
}

/// RFC 3986 encoding as `SigV4` wants it: every byte but `A-Za-z0-9-_.~`
/// is `%XX`; `/` too unless `keep_slash`.
#[must_use]
pub fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(b));
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The canonical query string of `(name, value)` pairs (not yet encoded).
#[must_use]
pub fn canonical_query(q: &[(String, String)]) -> String {
    let mut v: Vec<(String, String)> = q
        .iter()
        .map(|(k, v)| (uri_encode(k, false), uri_encode(v, false)))
        .collect();
    v.sort();
    v.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// One request to sign.
#[derive(Debug, Clone)]
pub struct Signing<'a> {
    /// `GET`, `PUT`, ...
    pub method: &'a str,
    /// The path, already URI-encoded (`/bucket/key`).
    pub path: &'a str,
    /// Query pairs, not encoded.
    pub query: &'a [(String, String)],
    /// Headers sent (all are signed), names in any case.
    pub headers: &'a [(String, String)],
    /// Hex SHA-256 of the body (or `UNSIGNED-PAYLOAD`).
    pub payload_hash: &'a str,
    /// The region.
    pub region: &'a str,
    /// The service (`s3`).
    pub service: &'a str,
    /// `YYYYMMDDTHHMMSSZ`.
    pub amz_date: &'a str,
}

/// The `Authorization` header value.
#[must_use]
pub fn authorization(s: &Signing<'_>, creds: &Creds) -> String {
    let mut hs: Vec<(String, String)> = s
        .headers
        .iter()
        .map(|(k, v)| {
            (
                k.to_ascii_lowercase(),
                v.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .collect();
    hs.sort();
    let mut canon_headers = String::new();
    for (k, v) in &hs {
        canon_headers.push_str(&format!("{k}:{v}\n"));
    }
    let signed: Vec<&str> = hs.iter().map(|(k, _)| k.as_str()).collect();
    let signed = signed.join(";");
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        s.method,
        s.path,
        canonical_query(s.query),
        canon_headers,
        signed,
        s.payload_hash
    );
    let date = &s.amz_date[..8];
    let scope = format!("{date}/{}/{}/aws4_request", s.region, s.service);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        s.amz_date,
        sha256_hex(canonical.as_bytes())
    );
    let k_secret = Zeroizing::new(format!("AWS4{}", creds.secret.as_str()));
    let k = hmac(k_secret.as_bytes(), date.as_bytes());
    let k = hmac(&k, s.region.as_bytes());
    let k = hmac(&k, s.service.as_bytes());
    let k = hmac(&k, b"aws4_request");
    let sig = hex(&hmac(&k, to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={sig}",
        creds.access_key_id
    )
}

/// `YYYYMMDDTHHMMSSZ` for ms since the epoch.
#[must_use]
pub fn amz_date(ms: i64) -> String {
    // 2015-08-30T12:36:00.000Z → 20150830T123600Z
    let t = crate::target::kafka::rfc3339(ms);
    format!(
        "{}{}{}T{}{}{}Z",
        &t[0..4],
        &t[5..7],
        &t[8..10],
        &t[11..13],
        &t[14..16],
        &t[17..19]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn creds() -> Creds {
        Creds {
            access_key_id: "AKIDEXAMPLE".into(),
            secret: Zeroizing::new("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into()),
            session_token: None,
        }
    }

    fn sign(method: &str, query: &[(&str, &str)], extra: &[(&str, &str)], body: &[u8]) -> String {
        let q: Vec<(String, String)> = query
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let mut h: Vec<(String, String)> = vec![
            ("Host".into(), "example.amazonaws.com".into()),
            ("X-Amz-Date".into(), "20150830T123600Z".into()),
        ];
        h.extend(
            extra
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
        );
        let ph = sha256_hex(body);
        authorization(
            &Signing {
                method,
                path: "/",
                query: &q,
                headers: &h,
                payload_hash: &ph,
                region: "us-east-1",
                service: "service",
                amz_date: "20150830T123600Z",
            },
            &creds(),
        )
    }

    /// AWS's signing test suite (aws-c-auth's `aws-signing-test-suite/v4`).
    #[test]
    fn aws_test_suite_vectors() {
        assert!(sign("GET", &[], &[], b"").ends_with(
            "SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        ));
        assert!(sign("POST", &[], &[], b"").ends_with(
            "Signature=5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b"
        ));
        assert!(
            sign(
                "GET",
                &[("Param2", "value2"), ("Param1", "value1")],
                &[],
                b""
            )
            .ends_with(
                "Signature=b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500"
            )
        );
        let body = b"Param1=value1";
        let a = sign(
            "POST",
            &[],
            &[
                ("Content-Type", "application/x-www-form-urlencoded"),
                ("Content-Length", "13"),
                ("x-amz-content-sha256", &sha256_hex(body)),
            ],
            body,
        );
        assert_eq!(
            sha256_hex(body),
            "9095672bbd1f56dfc5b65f3e153adc8731a4a654192329106275f4c7b24d0b6e"
        );
        assert_eq!(
            a,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=content-length;content-type;host;x-amz-content-sha256;x-amz-date, \
             Signature=d3875051da38690788ef43de4db0d8f280229d82040bfac253562e56c3f20e0b"
        );
        assert!(!format!("{:?}", creds()).contains("wJal"));
    }

    #[test]
    fn encoding_and_dates() {
        assert_eq!(uri_encode("a b/c~d+é", true), "a%20b/c~d%2B%C3%A9");
        assert_eq!(uri_encode("a/b", false), "a%2Fb");
        assert_eq!(
            canonical_query(&[
                ("uploads".into(), String::new()),
                ("a".into(), "x y".into())
            ]),
            "a=x%20y&uploads="
        );
        assert_eq!(amz_date(1_440_938_160_000), "20150830T123600Z");
    }
}
