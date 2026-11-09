use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;

use super::*;

fn b64(s: &str) -> Vec<u8> {
    B64.decode(s).unwrap()
}

/// One RFC exchange: password, salt, iterations, the three messages that
/// make up `AuthMessage`, the proof and the server signature.
struct Vector {
    password: &'static [u8],
    salt: &'static str,
    iterations: u32,
    client_first_bare: &'static str,
    server_first: &'static str,
    client_final_without_proof: &'static str,
    proof: &'static str,
    server_signature: &'static str,
}

impl Vector {
    fn auth_message(&self) -> Vec<u8> {
        format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, self.client_final_without_proof
        )
        .into_bytes()
    }
}

/// RFC 7677 section 3: user "user", password "pencil".
const RFC7677: Vector = Vector {
    password: b"pencil",
    salt: "W22ZaJ0SNY7soEsUEjb6gQ==",
    iterations: 4096,
    client_first_bare: "n=user,r=rOprNGfwEbeRWgbNEkqO",
    server_first: "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
    client_final_without_proof: "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0",
    proof: "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
    server_signature: "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=",
};

/// RFC 5802 section 5 (SCRAM-SHA-1): the same construction with SHA-1.
const RFC5802: Vector = Vector {
    password: b"pencil",
    salt: "QSXCR+Q6sek8bf92",
    iterations: 4096,
    client_first_bare: "n=user,r=fyko+d2lbbFgONRv9qkxdawL",
    server_first: "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
    client_final_without_proof: "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j",
    proof: "v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=",
    server_signature: "rmF9pqV8S7suAoZWja4dJRkFsKQ=",
};

/// RFC 7677's exchange with SHA-512 (no RFC has a SHA-512 vector): the
/// values Python's `hashlib.pbkdf2_hmac` and `hmac` give.
const SHA512: Vector = Vector {
    proof: "gMGXRcevScNtxZ6/8lQYpGtnsNAc3mGcmNomv+xnoOMw+3R2xNJdMNnzMlTN8PPC6wdp6dybEmDYXYTxwnYPJQ==",
    server_signature: "ZQnYEgWQMFmmsM8aQMF0nDDCy/AgCzkwk8CmMZYcMg0vSVlKDanekLtifDSeVGT4+5ZxXnJq199RVG2rR7N7Zw==",
    ..RFC7677
};

const SHA512_STORED_KEY: &str = "e8002e6f7d3ae446119b216933644dc2a2be7869eb918b8459b5e7d7d2ec12606aceef106825cd735170a675fd3611f684affad1dce3f43a0ee43bd590e1dbbe";
const SHA512_SERVER_KEY: &str = "8d91db6230b5687874fe129bc7206e1858c3ae08e02934f57ac03b6b05a229c459d28ff46f5c9611e6c179256490215ec1ff759cb0df285db89af0f99e613aac";

fn hex(b: &[u8]) -> String {
    b.iter().fold(String::new(), |mut s, x| {
        use std::fmt::Write as _;
        let _ = write!(s, "{x:02x}");
        s
    })
}

fn check_vector(alg: Alg, v: &Vector) {
    let salted = alg.salted(v.password, &b64(v.salt), v.iterations);
    let (stored, server) = alg.keys(&salted);
    let am = v.auth_message();
    // The client's side gives the RFC's proof...
    assert_eq!(B64.encode(alg.proof(&salted, &am)), v.proof);
    // ...and the server's side, from StoredKey and ServerKey alone, accepts
    // it and signs as the RFC does.
    let sig = alg.check(&stored, &server, &am, &b64(v.proof)).unwrap();
    assert_eq!(B64.encode(sig), v.server_signature);
    // A flipped bit fails.
    let mut wrong = b64(v.proof);
    wrong[0] ^= 1;
    assert!(alg.check(&stored, &server, &am, &wrong).is_none());
    assert!(alg.check(&stored, &server, &am, &wrong[1..]).is_none());
}

#[test]
fn rfc_5802_sha1_vector() {
    check_vector(Alg::Sha1, &RFC5802);
}

#[test]
fn rfc_7677_sha256_vector() {
    check_vector(Alg::Sha256, &RFC7677);
    let v = ScramVerifier::derive(ScramKind::Sha256, b"pencil", &b64(RFC7677.salt), 4096);
    let sig = v
        .check_proof(&RFC7677.auth_message(), &b64(RFC7677.proof))
        .unwrap();
    assert_eq!(B64.encode(sig), RFC7677.server_signature);
    assert_eq!(
        client_proof(
            ScramKind::Sha256,
            b"pencil",
            &b64(RFC7677.salt),
            4096,
            &RFC7677.auth_message()
        )
        .map(|p| B64.encode(p))
        .as_deref(),
        Some(RFC7677.proof)
    );
}

#[test]
fn sha512_vector() {
    check_vector(Alg::Sha512, &SHA512);
    let v = ScramVerifier::derive(ScramKind::Sha512, b"pencil", &b64(SHA512.salt), 4096);
    assert_eq!(hex(v.stored_key()), SHA512_STORED_KEY);
    assert_eq!(hex(v.server_key()), SHA512_SERVER_KEY);
    assert_eq!(v.stored_key().len(), 64);
}

#[test]
fn create_verify_and_rows() {
    for kind in ScramKind::ALL {
        let v = ScramVerifier::create(kind, b"Tr0ub4dor&3", None, MIN_SCRAM_ITERATIONS)
            .unwrap()
            .unwrap();
        assert_eq!(v.kind(), kind);
        assert_eq!(v.salt().len(), SCRAM_SALT_LEN);
        assert_eq!(v.stored_key().len(), kind.key_len());
        assert!(v.verify_password(b"Tr0ub4dor&3"));
        assert!(!v.verify_password(b"Tr0ub4dor&4"));
        // Fresh salts differ.
        let w = ScramVerifier::create(kind, b"Tr0ub4dor&3", None, MIN_SCRAM_ITERATIONS)
            .unwrap()
            .unwrap();
        assert_ne!(v.salt(), w.salt());
        // A given salt reproduces the verifier (dual passwords share one).
        let same =
            ScramVerifier::create(kind, b"Tr0ub4dor&3", Some(v.salt()), MIN_SCRAM_ITERATIONS)
                .unwrap()
                .unwrap();
        assert_eq!(same, v);
        // Row round trip.
        let row = v.to_row(UserId(7), Slot::Primary, 3, SystemTime::UNIX_EPOCH);
        assert_eq!(row.kind, kind.verifier_kind());
        assert!(row.auth_string.is_none());
        assert_eq!(ScramVerifier::from_row(&row).unwrap(), v);
        let mut short = row.clone();
        short.stored_key = Some(vec![0; 3]);
        assert!(ScramVerifier::from_row(&short).is_err());
        let mut null = row;
        null.salt = None;
        assert!(ScramVerifier::from_row(&null).is_err());
    }
}

#[test]
fn iterations_are_checked() {
    for bad in [0, 4096, MIN_SCRAM_ITERATIONS - 1, MAX_SCRAM_ITERATIONS + 1] {
        assert!(ScramVerifier::create(ScramKind::Sha256, b"pw", None, bad).is_err());
    }
    assert!(check_iterations(DEFAULT_SCRAM_ITERATIONS).is_ok());
    assert!(ScramVerifier::create(ScramKind::Sha256, b"pw", Some(&[0; 65]), 5000).is_err());
    assert!(ScramVerifier::create(ScramKind::Sha256, b"pw", Some(&[]), 5000).is_err());
}

/// RFC 4013 section 3's examples, and a non-ASCII space.
#[test]
fn saslprep_rfc_4013_examples() {
    let ok = |input: &str, out: &str| {
        assert_eq!(
            saslprep(input.as_bytes()).as_deref().map(Vec::as_slice),
            Some(out.as_bytes()),
            "{input:?}"
        );
    };
    ok("I\u{00AD}X", "IX"); // soft hyphen maps to nothing
    ok("user", "user");
    ok("USER", "USER");
    ok("\u{00AA}", "a"); // NFKC
    ok("\u{2168}", "IX"); // NFKC
    ok("a\u{00A0}b", "a b"); // non-ASCII space
    ok("pässwörd-日本", "pässwörd-日本");
    assert!(saslprep("\u{0007}".as_bytes()).is_none()); // prohibited
    assert!(saslprep("\u{0627}\u{0031}".as_bytes()).is_none()); // bidi
    assert!(saslprep(b"\xff\xfe").is_none()); // not UTF-8
}

#[test]
fn sha256_prepares_and_sha512_does_not() {
    // SCRAM-SHA-256 hashes SASLprep's output, so the soft hyphen doesn't
    // matter; SCRAM-SHA-512 hashes the bytes, as Kafka's clients do.
    let salt = [9u8; SCRAM_SALT_LEN];
    let a = ScramVerifier::create(
        ScramKind::Sha256,
        "I\u{00AD}X".as_bytes(),
        Some(&salt),
        5000,
    )
    .unwrap()
    .unwrap();
    assert!(a.verify_password(b"IX"));
    let b = ScramVerifier::create(
        ScramKind::Sha512,
        "I\u{00AD}X".as_bytes(),
        Some(&salt),
        5000,
    )
    .unwrap()
    .unwrap();
    assert!(!b.verify_password(b"IX"));
    assert!(b.verify_password("I\u{00AD}X".as_bytes()));
    // A password SASLprep refuses gets no SCRAM-SHA-256 verifier, but a
    // SCRAM-SHA-512 one.
    assert!(
        ScramVerifier::create(ScramKind::Sha256, b"bell\x07", None, 5000)
            .unwrap()
            .is_none()
    );
    assert!(
        ScramVerifier::create(ScramKind::Sha512, b"bell\x07", None, 5000)
            .unwrap()
            .is_some()
    );
}

#[test]
fn debug_hides_the_keys() {
    let v = ScramVerifier::derive(ScramKind::Sha256, b"pencil", &b64(RFC7677.salt), 4096);
    let shown = format!("{v:?}");
    assert_eq!(
        shown,
        "ScramVerifier { kind: Sha256, iterations: 4096, .. }"
    );
    let row = v.to_row(UserId(1), Slot::Primary, 1, SystemTime::UNIX_EPOCH);
    let shown = format!("{row:?}");
    assert!(!shown.contains("salt") && !shown.contains("key"), "{shown}");
}
