use std::time::{Duration, UNIX_EPOCH};

use sprig_common::ids::{DbId, TenantId};
use sprig_proto_mysql::packets::Persona;

use super::*;
use crate::fingerprint::{SqlCtx, SqlFingerprinter};
use crate::keyring::{MasterKeyring, NoKeys};
use crate::shape::NoResolver;

fn payload() -> (FingerprintId, SamplePayload) {
    let f = SqlFingerprinter::default();
    let ctx = SqlCtx::new(Persona::MariaDb).current_db("shop", DbId(3));
    let sql = b"SELECT * FROM orders WHERE customer_id = -42 AND note = 'caf\xc3\xa9' AND status IN ('open', 'hold') LIMIT 20";
    let fp = f.sql(TenantId(1), sql, &ctx, &NoResolver);
    let literals = SampledLiterals::from_execution(sql, &fp, &[]);
    (
        fp.id,
        SamplePayload {
            literals,
            context: SampleContext {
                database: Some("shop".into()),
                db_id: DbId(3),
                sql_mode: Some("STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES".into()),
                time_zone: Some("+05:30".into()),
                charset: Some("utf8mb4".into()),
                collation: Some("utf8mb4_uca1400_ai_ci".into()),
            },
        },
    )
}

fn meta() -> CaptureMeta {
    CaptureMeta {
        captured_at: UNIX_EPOCH + Duration::from_secs(1_800_000_000),
        latency_us: 120_000,
        rows_examined: Some(5000),
        rows_returned: 1,
        expires_at: UNIX_EPOCH + Duration::from_secs(1_800_604_800),
    }
}

#[test]
fn payloads_round_trip() {
    let (_, p) = payload();
    assert_eq!(p.literals.slots.len(), 4);
    let b = encode(&p);
    assert_eq!(decode(&b).as_ref(), Some(&p));
    // Truncated or extended plaintexts are refused.
    assert!(decode(&b[..b.len() - 1]).is_none());
    let mut longer = b.clone();
    longer.push(0);
    assert!(decode(&longer).is_none());
    assert!(decode(&[2]).is_none());
    assert_eq!(
        decode(&encode(&SamplePayload::default())),
        Some(SamplePayload::default())
    );
}

#[test]
fn seal_open_round_trip_and_tampering() {
    let keys = MasterKeyring::new([(1, [5u8; 32])]).unwrap();
    let rng = SystemRandom::new();
    let (fp, p) = payload();
    let s = seal(&keys, &rng, TenantId(1), fp, &p, meta()).unwrap();
    assert_eq!(s.key_version, 1);
    // The ciphertext holds no plaintext value.
    let hay = String::from_utf8_lossy(&s.payload);
    assert!(!hay.contains("open") && !hay.contains("shop"));
    assert!(!format!("{s:?}").contains("open"));
    let o = open(&keys, TenantId(1), &s).unwrap();
    assert_eq!(o.payload, p);
    assert_eq!(o.rows_examined, Some(5000));
    assert!(!format!("{o:?}").contains("open"), "{o:?}");

    // Another tenant, another fingerprint, a flipped bit: no.
    assert_eq!(open(&keys, TenantId(2), &s), Err(SampleError::Tampered));
    let mut other = s.clone();
    other.fingerprint = sprig_common::ids::Fingerprint(fp.0 ^ 1);
    assert_eq!(open(&keys, TenantId(1), &other), Err(SampleError::Tampered));
    let mut flipped = s.clone();
    flipped.payload[0] ^= 1;
    assert_eq!(
        open(&keys, TenantId(1), &flipped),
        Err(SampleError::Tampered)
    );
    // No key: nothing seals, nothing opens.
    assert_eq!(
        seal(&NoKeys, &rng, TenantId(1), fp, &p, meta()),
        Err(SampleError::KeyUnavailable)
    );
    assert_eq!(
        open(&NoKeys, TenantId(1), &s),
        Err(SampleError::KeyUnavailable)
    );
}

#[test]
fn key_rotation_keeps_old_samples_open_until_their_version_goes() {
    let rng = SystemRandom::new();
    let (fp, p) = payload();
    let v1 = MasterKeyring::new([(1, [5u8; 32])]).unwrap();
    let old = seal(&v1, &rng, TenantId(1), fp, &p, meta()).unwrap();
    let both = MasterKeyring::new([(1, [5u8; 32]), (2, [6u8; 32])]).unwrap();
    let new = seal(&both, &rng, TenantId(1), fp, &p, meta()).unwrap();
    assert_eq!(new.key_version, 2);
    assert_eq!(open(&both, TenantId(1), &old).unwrap().payload, p);
    assert_eq!(open(&both, TenantId(1), &new).unwrap().payload, p);
    let v2 = MasterKeyring::new([(2, [6u8; 32])]).unwrap();
    assert_eq!(
        open(&v2, TenantId(1), &old),
        Err(SampleError::KeyUnavailable)
    );
    assert_eq!(open(&v2, TenantId(1), &new).unwrap().payload, p);
}

#[test]
fn lists_are_capped() {
    let (_, mut p) = payload();
    cap_lists(&mut p.literals, 1);
    let lens: Vec<usize> = p
        .literals
        .slots
        .iter()
        .map(|s| match s {
            SampledSlot::One(_) => 1,
            SampledSlot::List(v) => v.len(),
        })
        .collect();
    assert_eq!(lens, vec![1, 1, 1, 1]);
}

#[test]
fn the_outbox_keeps_a_reservoir_per_bucket_and_writes_slots_once() {
    let keys = MasterKeyring::new([(1, [5u8; 32])]).unwrap();
    let rng = SystemRandom::new();
    let (fp, p) = payload();
    let ob = SampleOutbox::new(3, 77);
    let mk = || seal(&keys, &rng, TenantId(1), fp, &p, meta()).unwrap();
    let kept = (0..200)
        .filter(|_| ob.offer(TenantId(1), 3600, mk()))
        .count();
    assert!((3..200).contains(&kept), "{kept}");
    let first = ob.drain(3600);
    assert_eq!(first.len(), 3);
    let mut ids: Vec<u64> = first.iter().map(|(_, s)| s.sample_id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 3, "one id per slot");
    assert!(ob.drain(3600).is_empty(), "written once");
    // Later offers can only replace a slot: same ids.
    for _ in 0..500 {
        ob.offer(TenantId(1), 3600, mk());
    }
    for (_, s) in ob.drain(3600) {
        assert!(ids.contains(&s.sample_id));
    }
    // A new bucket: new slots and ids; the old bucket's unwritten ones
    // still come out once.
    ob.offer(TenantId(1), 3600, mk());
    assert!(ob.offer(TenantId(1), 7200, mk()));
    let next = ob.drain(7200);
    assert!(!next.is_empty());
    assert!(next.iter().any(|(_, s)| !ids.contains(&s.sample_id)));
    ob.forget_tenant(TenantId(1));
    assert!(ob.drain(7200).is_empty());
    assert!(!SampleOutbox::new(0, 1).offer(TenantId(1), 0, mk()));
}
