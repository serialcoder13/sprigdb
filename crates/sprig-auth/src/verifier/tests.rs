use super::*;

/// `HEX(authentication_string)` from `mysql.user` on a MySQL 8.4.11 server
/// (`mysql:8.4` image), for users created with `IDENTIFIED WITH
/// caching_sha2_password BY '<password>'` at the default 5000 rounds. The
/// salts hold control characters, as MySQL's do.
const MYSQL_84_SHA2: &[(&str, &str)] = &[
    (
        "secret",
        "2441243030352461054B524B4E691D0B2578570D1B79530D08384A714E5655304852656B74683135346F2E52555868477930305A4B5A4A494D455555626546566B7254436C2E",
    ),
    (
        "sprig-dev-app",
        "24412430303524233E393D3D731C6F151D2C0A3B2A6A1D6F3E22772E484D74654F45343036327A63382E78793466736450776D47483570386A716C524C487A712F416A454B2F",
    ),
    // Exactly 32 bytes: one full SHA-256 block of B in digest A.
    (
        "0123456789abcdef0123456789abcdef",
        "2441243030352459594C5D016B26162B3E454344772E746E364551412E6D4D2E4D467556773665565842316E75756A565272616B48756931535A4C666C524B682F612E427944",
    ),
    // Longer than 64 bytes.
    (
        "The quick brown fox jumps over the lazy dog, then naps for 70+ bytes!",
        "244124303035242B4218101C2C7A48102D11514B4D7205501C493754347878532F336F324A70456F4B624A334E314746764541487054335A574F3978415575484A752E616E2E",
    ),
    // UTF-8, sent by a utf8mb4 client.
    (
        "pässwörd-日本",
        "244124303035246D4B2D516C6144532C113B027A3C2A35356D407372612E48355058646345584C544779684344645853697369616A724C5A496E42736573376C706C564F7133",
    ),
];

/// A dual password: `ALTER USER ... IDENTIFIED BY 'second-pw' RETAIN CURRENT
/// PASSWORD` after creating the user with `first-pw`. The primary is in
/// `authentication_string`, the secondary in
/// `User_attributes->'$.additional_password'`.
const MYSQL_84_DUAL: [(&str, &str); 2] = [
    (
        "second-pw",
        "24412430303524655D68277903291418275B5033042B6E7630344452796479673378756F49755971632E454E32336A32366C6D44506653414A53462F48416C79766D4A387A33",
    ),
    (
        "first-pw",
        "24412430303524574F604D303E1B2939292C1621797F69605F77606E44314E334D4972544836486476566D5932474C412E6D4D553438695645794E6D7A734A6755496B796433",
    ),
];

/// `secret` on a MySQL 8.4.11 server started with
/// `--caching-sha2-password-digest-rounds=10000`: the field is "00A",
/// upper-case hex.
const MYSQL_84_SHA2_10K: &str = "244124303041247D6C3E701A7A03097B327F7C197502285B482A3A4A7A78632F422F506F66756C776C703778516E3166332E696E4257546E6535732F356E61784450794C6C38";

/// `HEX(authentication_string)` for `IDENTIFIED WITH mysql_native_password`
/// on the same server (started with `--mysql-native-password=ON`).
const MYSQL_84_NATIVE: &[(&str, &str)] = &[
    (
        "secret",
        "2A31344536353536374142444235313335443043464439413730423330333243313739413439454537",
    ),
    (
        "sprig-dev-app",
        "2A36374438394545313734324135454530413438383841383833313532374641374132394141314635",
    ),
];

fn unhex(h: &str) -> Vec<u8> {
    let mut out = vec![0; h.len() / 2];
    decode_hex(h, &mut out).unwrap();
    out
}

#[test]
fn mysql_84_caching_sha2_vectors_verify_and_round_trip() {
    for (pw, hex) in MYSQL_84_SHA2.iter().chain(&MYSQL_84_DUAL) {
        let stored = unhex(hex);
        let v = CachingSha2Verifier::parse(&stored).unwrap();
        assert_eq!(v.rounds(), 5000);
        assert!(v.verify(pw.as_bytes()), "{pw}");
        assert!(!v.verify(b"wrong"), "{pw}");
        assert!(!v.verify(b""), "{pw}");
        assert_eq!(v.to_bytes(), stored, "{pw}");
        // Recomputing with MySQL's salt gives MySQL's string: all 20 salt
        // bytes are used.
        let salt: [u8; SALT_LEN] = stored[7..27].try_into().unwrap();
        let again = CachingSha2Verifier::with_salt(pw.as_bytes(), salt, 5000).unwrap();
        assert_eq!(again.to_bytes(), stored, "{pw}");
    }
}

#[test]
fn mysql_84_dual_passwords_are_distinct() {
    let primary = CachingSha2Verifier::parse(&unhex(MYSQL_84_DUAL[0].1)).unwrap();
    let secondary = CachingSha2Verifier::parse(&unhex(MYSQL_84_DUAL[1].1)).unwrap();
    assert!(primary.verify(b"second-pw") && !primary.verify(b"first-pw"));
    assert!(secondary.verify(b"first-pw") && !secondary.verify(b"second-pw"));
}

#[test]
fn mysql_84_rounds_field_is_three_hex_digits() {
    let stored = unhex(MYSQL_84_SHA2_10K);
    assert_eq!(&stored[..7], b"$A$00A$");
    let v = CachingSha2Verifier::parse(&stored).unwrap();
    assert_eq!(v.rounds(), 10_000);
    assert!(v.verify(b"secret"));
    assert_eq!(v.to_bytes(), stored);
    // Lower-case hex parses too.
    let mut lower = stored.clone();
    lower[5] = b'a';
    assert_eq!(CachingSha2Verifier::parse(&lower).unwrap().rounds(), 10_000);
}

#[test]
fn mysql_84_native_vectors() {
    for (pw, hex) in MYSQL_84_NATIVE {
        let stored = String::from_utf8(unhex(hex)).unwrap();
        let v: NativeVerifier = stored.parse().unwrap();
        assert!(v.verify(pw.as_bytes()));
        assert!(!v.verify(b"wrong") && !v.verify(b""));
        assert_eq!(v.to_string(), stored);
        assert_eq!(NativeVerifier::create(pw.as_bytes()).to_string(), stored);
        assert_eq!(
            NativeVerifier::parse(&stored.to_ascii_lowercase()).unwrap(),
            v
        );
    }
}

#[test]
fn empty_passwords_have_empty_verifiers() {
    // MySQL stores '' for IDENTIFIED BY '' with either plugin.
    let s = CachingSha2Verifier::parse(b"").unwrap();
    assert!(s.is_empty() && s.verify(b"") && !s.verify(b"x"));
    assert_eq!(s.to_string(), "");
    assert_eq!(CachingSha2Verifier::create(b"", 5000).unwrap(), s);
    let n = NativeVerifier::parse("").unwrap();
    assert!(n.is_empty() && n.verify(b"") && !n.verify(b"x"));
    assert_eq!(NativeVerifier::create(b""), n);
    assert!(verify_native_scramble(&[7; 20], b"", &n));
    assert!(!verify_native_scramble(&[7; 20], &[0; 20], &n));
}

#[test]
fn created_verifiers_are_printable_and_salted() {
    let a = CachingSha2Verifier::create(b"hunter2", DEFAULT_CACHING_SHA2_ROUNDS).unwrap();
    let b = CachingSha2Verifier::create(b"hunter2", DEFAULT_CACHING_SHA2_ROUNDS).unwrap();
    assert_ne!(a, b, "fresh salts");
    let text = a.to_string();
    assert_eq!(text.len(), CACHING_SHA2_LEN);
    assert!(text.starts_with("$A$005$"));
    assert!(text.bytes().all(|c| c.is_ascii_graphic()));
    let parsed: CachingSha2Verifier = text.parse().unwrap();
    assert_eq!(parsed, a);
    assert!(parsed.verify(b"hunter2") && b.verify(b"hunter2"));
    assert!(!parsed.verify(b"hunter3"));
    assert!(!format!("{a:?}").contains(&text[7..]));
}

#[test]
fn malformed_verifiers_are_refused() {
    let good = CachingSha2Verifier::create(b"pw", 5000).unwrap().to_bytes();
    let mut cases: Vec<Vec<u8>> = vec![
        good[..69].to_vec(),
        [good.as_slice(), b"x"].concat(),
        b"$B$005$".iter().chain(&good[7..]).copied().collect(),
        b"$A$0G5$".iter().chain(&good[7..]).copied().collect(),
        b"$A$000$".iter().chain(&good[7..]).copied().collect(), // 0 rounds
        b"$A$005#".iter().chain(&good[7..]).copied().collect(),
    ];
    let mut dollar_salt = good.clone();
    dollar_salt[10] = b'$';
    let mut nul_salt = good.clone();
    nul_salt[10] = 0;
    let mut bad_digest = good.clone();
    bad_digest[69] = b'!';
    cases.extend([dollar_salt, nul_salt, bad_digest]);
    for c in cases {
        assert!(
            CachingSha2Verifier::parse(&c).is_err(),
            "{}",
            String::from_utf8_lossy(&c)
        );
    }
    for bad in [
        "14E65567ABDB5135D0CFD9A70B3032C179A49EE7",
        "*14E6",
        "*ZZE65567ABDB5135D0CFD9A70B3032C179A49EE7",
    ] {
        assert!(NativeVerifier::parse(bad).is_err(), "{bad}");
    }
    for rounds in [0, 999, 1500, 4_096_000] {
        assert!(
            CachingSha2Verifier::create(b"pw", rounds).is_err(),
            "{rounds}"
        );
    }
}

#[test]
fn fast_path_scramble_round_trip() {
    let nonce: [u8; NONCE_LEN] = *b"0123456789abcdefghij";
    let s2 = FastCacheValue::from_password(b"secret");
    let response = caching_sha2_scramble(b"secret", &nonce);
    assert!(verify_scramble(&nonce, &response, &s2));
    assert!(!verify_scramble(
        &nonce,
        &caching_sha2_scramble(b"Secret", &nonce),
        &s2
    ));
    let other_nonce = *b"0123456789abcdefghiX";
    assert!(!verify_scramble(&other_nonce, &response, &s2));
    assert!(!verify_scramble(&nonce, &response[..31], &s2));
    assert!(!verify_scramble(&nonce, b"", &s2));
    assert!(!format!("{s2:?}").contains("0x"));
}

/// The fast-path value is SHA256(SHA256(password)): a fixed vector.
#[test]
fn fast_cache_value_vector() {
    // SHA256("secret") = 2bb80d53...; SHA256 of those 32 bytes:
    let s2 = FastCacheValue::from_password(b"secret");
    let stage1: [u8; 32] = Sha256::digest(b"secret").into();
    assert_eq!(
        stage1[..4],
        [0x2b, 0xb8, 0x0d, 0x53],
        "SHA256(\"secret\") starts 2bb80d53"
    );
    let expect: [u8; 32] = Sha256::digest(stage1).into();
    assert_eq!(s2.as_bytes(), &expect);
}

#[test]
fn native_scramble_round_trip() {
    let nonce = *b"abcdefghij0123456789";
    let v = NativeVerifier::create(b"secret");
    let response = native_scramble(b"secret", &nonce);
    assert_eq!(response.len(), 20);
    assert!(verify_native_scramble(&nonce, &response, &v));
    assert!(!verify_native_scramble(
        &nonce,
        &native_scramble(b"secreT", &nonce),
        &v
    ));
    assert!(!verify_native_scramble(
        b"abcdefghij012345678X",
        &response,
        &v
    ));
    assert!(!verify_native_scramble(&nonce, b"", &v));
    assert!(!verify_native_scramble(&nonce, &response[..19], &v));
}

/// Pins the order of the SHA-1 inputs in the native scramble, as MySQL's
/// `scramble_411` computes it: the nonce first, then stage 2.
#[test]
fn native_scramble_matches_mysql_formula() {
    let nonce = [0x11u8; 20];
    let stage1: [u8; 20] = Sha1::digest(b"pw").into();
    let stage2: [u8; 20] = Sha1::digest(stage1).into();
    let mask: [u8; 20] = Sha1::digest([nonce.as_slice(), stage2.as_slice()].concat()).into();
    let expect: Vec<u8> = stage1.iter().zip(mask).map(|(a, b)| a ^ b).collect();
    assert_eq!(native_scramble(b"pw", &nonce), expect);
}

#[test]
fn decoy_never_matches() {
    assert!(!decoy_verify(b"anything"));
    assert!(!decoy_verify(b""));
}

#[test]
fn helper_strings() {
    let s = caching_sha2_string("sprig-dev-app").unwrap();
    assert!(
        s.parse::<CachingSha2Verifier>()
            .unwrap()
            .verify(b"sprig-dev-app")
    );
    let both = PasswordVerifiers::from_password(b"sprig-dev-app", 5000).unwrap();
    assert!(both.caching_sha2.verify(b"sprig-dev-app"));
    assert_eq!(
        both.native.to_string(),
        String::from_utf8(unhex(MYSQL_84_NATIVE[1].1)).unwrap()
    );
}
