#![allow(clippy::unwrap_used)]

use sprig_catalog::Migrator;

use super::*;

#[test]
fn enums_round_trip_their_sql_text() {
    for k in UserKind::ALL {
        assert_eq!(UserKind::parse(k.as_str()).unwrap(), *k);
    }
    for k in VerifierKind::ALL {
        assert_eq!(VerifierKind::parse(k.as_str()).unwrap(), *k);
    }
    for s in Slot::ALL {
        assert_eq!(Slot::parse(s.as_str()).unwrap(), *s);
    }
    for s in UserSource::ALL {
        assert_eq!(UserSource::parse(s.as_str()).unwrap(), *s);
    }
    assert!(RequireTls::parse("X509").is_err());
    // Slots order as the cache tries them: primary, secondary, history.
    assert!(Slot::Primary < Slot::Secondary && Slot::Secondary < Slot::History);
}

#[test]
fn auth_methods_are_a_set() {
    let m = AuthMethods::parse("password,api_key").unwrap();
    assert!(m.contains(AuthMethods::PASSWORD) && m.contains(AuthMethods::API_KEY));
    assert!(!m.contains(AuthMethods::TOKEN));
    assert_eq!(m.to_sql(), "password,api_key");
    assert_eq!(AuthMethods::parse("").unwrap(), AuthMethods::empty());
    assert!(AuthMethods::parse("password,bogus").is_err());
    assert_eq!(AuthMethods::default(), AuthMethods::PASSWORD);
}

#[test]
fn times_round_trip_as_micros() {
    let t = UNIX_EPOCH + Duration::from_micros(1_762_560_000_123_456);
    assert_eq!(from_micros(micros(t)), t);
    assert_eq!(micros(UNIX_EPOCH - Duration::from_secs(5)), 0);
    // 9999-12-31 00:00:00 UTC.
    assert_eq!(UNBOUNDED_LOCK_MICROS / 1_000_000, 253_402_214_400);
}

#[test]
fn the_module_registers_with_the_migrator() {
    let m = Migrator::new(&[AUTH_MODULE]).unwrap();
    assert_eq!(m.supports().get(MODULE), Some(&2));
    assert_eq!(AUTH_MODULE.newest(), 2);
}

#[test]
fn verifier_debug_hides_secrets() {
    let v = VerifierRow::mysql(
        UserId(1),
        VerifierKind::Native,
        Slot::Primary,
        b"*0123456789ABCDEF0123456789ABCDEF01234567".to_vec(),
        1,
        UNIX_EPOCH,
    );
    let d = format!("{v:?}");
    assert!(!d.contains("0123456789"), "{d}");
}

#[test]
fn qualified_table_names() {
    let s = AuthStore::for_tenant(TenantId(42));
    assert_eq!(s.schema(), "t42_sprig");
    assert_eq!(s.users, "`t42_sprig`.users");
}
