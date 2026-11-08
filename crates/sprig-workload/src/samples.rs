//! Encrypted parameter samples (chapter 12, "Parameter samples for
//! replay"; build step 6).
//!
//! For watched fingerprints the gateway captures one execution's literals
//! by slot ([`SampledLiterals`], full `IN` lists up to
//! `workload.max_in_list_values`) with the session context replay needs
//! ([`SampleContext`]). [`seal`] encrypts both with AES-256-GCM under the
//! tenant's workload data key ([`crate::keyring`]), with the tenant and
//! fingerprint ids as associated data, so a sample copied to another
//! fingerprint or tenant doesn't open. [`SampleOutbox`] keeps at most
//! `workload.samples_per_bucket` (3) per fingerprint per gateway per bucket
//! by reservoir sampling, and the flusher writes them to
//! `workload_samples` with `expires_at = now +
//! workload.sample_retention_days`.
//!
//! **Opening.** Only the validation worker's replay opens samples
//! ([`open`], or [`crate::store::WorkloadStore::open_samples`]). An
//! [`OpenedSample`] holds values: its `Debug` prints none, and nothing in
//! this crate logs one.
//!
//! **Sample ids** *(choice)*: chapter 12 says "random"; here a sample's id
//! is `xxh3_64(process salt ‖ bucket ‖ reservoir slot)`, random per
//! gateway process, so each flush can write a reservoir slot with
//! `REPLACE` and a replaced slot overwrites its own row: a flush is
//! idempotent and the reservoir never stores more than its size.
//!
//! **Payload format** (version 1, before encryption): `u8` 1, the context
//! (`u32` database id, then database, `sql_mode`, `time_zone`, character
//! set and collation, each `u8` present and `u32`-length bytes), `u32`
//! slot count, then per slot `u8` 0 (one value) or 1 (a list, `u32`
//! count), each value `u8` kind and `u32`-length text. Integers are little
//! endian.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use sprig_common::ids::{DbId, TenantId};

use crate::fingerprint::{FingerprintId, LiteralKind};
use crate::keyring::{DataKey, TenantKeyring};
use crate::window::{SampleValue, SampledLiterals, SampledSlot};

/// The session context a sample was captured with, as replay sets it
/// (chapter 13, "Replay and acceptance"). Field names match
/// `sprig-advisor`'s replay `SessionContext`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampleContext {
    /// The session's current logical database (replay maps it to the
    /// shard's physical schema), `None` without one.
    pub database: Option<String>,
    /// Its id, `DbId(0)` without one.
    pub db_id: DbId,
    /// `sql_mode`, when the session set one.
    pub sql_mode: Option<String>,
    /// `time_zone`, when the session set one.
    pub time_zone: Option<String>,
    /// The connection character set.
    pub charset: Option<String>,
    /// The connection collation.
    pub collation: Option<String>,
}

impl Default for SampleContext {
    fn default() -> Self {
        Self {
            database: None,
            db_id: DbId(0),
            sql_mode: None,
            time_zone: None,
            charset: None,
            collation: None,
        }
    }
}

/// What a sample holds once opened.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct SamplePayload {
    /// The literals by slot.
    pub literals: SampledLiterals,
    /// The session context.
    pub context: SampleContext,
}

impl std::fmt::Debug for SamplePayload {
    // Values: never printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SamplePayload")
            .field("slots", &self.literals.slots.len())
            .finish_non_exhaustive()
    }
}

/// A sealed sample: one `workload_samples` row.
#[derive(Clone, PartialEq, Eq)]
pub struct SealedSample {
    /// The fingerprint.
    pub fingerprint: FingerprintId,
    /// The sample's id (module docs).
    pub sample_id: u64,
    /// When it was captured.
    pub captured_at: SystemTime,
    /// The execution's latency.
    pub latency_us: u64,
    /// Rows examined, when the same execution was probed.
    pub rows_examined: Option<u64>,
    /// Rows returned (or affected, for a write).
    pub rows_returned: u64,
    /// The tenant data key's version.
    pub key_version: u32,
    /// The AES-GCM nonce.
    pub nonce: [u8; NONCE_LEN],
    /// Ciphertext and tag.
    pub payload: Vec<u8>,
    /// When it expires.
    pub expires_at: SystemTime,
}

impl std::fmt::Debug for SealedSample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedSample")
            .field("fingerprint", &self.fingerprint)
            .field("sample_id", &self.sample_id)
            .field("key_version", &self.key_version)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

/// An opened sample: values in memory for one replay.
#[derive(Clone, PartialEq, Eq)]
pub struct OpenedSample {
    /// The fingerprint.
    pub fingerprint: FingerprintId,
    /// `workload_samples.sample_id`.
    pub sample_id: u64,
    /// When it was captured.
    pub captured_at: SystemTime,
    /// The execution's latency.
    pub latency_us: u64,
    /// Rows examined, when probed.
    pub rows_examined: Option<u64>,
    /// Rows returned.
    pub rows_returned: u64,
    /// Literals and context.
    pub payload: SamplePayload,
}

impl std::fmt::Debug for OpenedSample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenedSample")
            .field("fingerprint", &self.fingerprint)
            .field("sample_id", &self.sample_id)
            .field("payload", &self.payload)
            .finish_non_exhaustive()
    }
}

/// Why a sample didn't seal or open. Never carries values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleError {
    /// No key for the tenant (or that version is gone).
    KeyUnavailable,
    /// Authentication failed: another tenant's or fingerprint's sample, a
    /// wrong key, or a damaged row.
    Tampered,
    /// The plaintext isn't a payload this version knows.
    Malformed,
    /// The system random generator failed.
    Random,
}

impl std::fmt::Display for SampleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SampleError::KeyUnavailable => "no workload data key for this sample",
            SampleError::Tampered => "the sample doesn't authenticate",
            SampleError::Malformed => "the sample's payload is malformed",
            SampleError::Random => "the random generator failed",
        })
    }
}

impl std::error::Error for SampleError {}

/// What one capture adds besides the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaptureMeta {
    /// When it was captured.
    pub captured_at: SystemTime,
    /// The execution's latency.
    pub latency_us: u64,
    /// Rows examined, when probed.
    pub rows_examined: Option<u64>,
    /// Rows returned or affected.
    pub rows_returned: u64,
    /// When it expires.
    pub expires_at: SystemTime,
}

fn aad(tenant: TenantId, fp: FingerprintId) -> [u8; 24] {
    let mut a = [0u8; 24];
    a[..8].copy_from_slice(b"sprigws1");
    a[8..16].copy_from_slice(&tenant.0.to_le_bytes());
    a[16..].copy_from_slice(&fp.0.to_le_bytes());
    a
}

fn aead_key(k: &DataKey) -> Result<LessSafeKey, SampleError> {
    UnboundKey::new(&AES_256_GCM, k.bytes())
        .map(LessSafeKey::new)
        .map_err(|_| SampleError::KeyUnavailable)
}

/// Seals `payload` for `tenant`'s fingerprint `fp` under the tenant's
/// current key. The sample id is set by the outbox.
///
/// # Errors
/// [`SampleError::KeyUnavailable`] without a key; [`SampleError::Random`].
pub fn seal(
    keyring: &dyn TenantKeyring,
    rng: &SystemRandom,
    tenant: TenantId,
    fp: FingerprintId,
    payload: &SamplePayload,
    meta: CaptureMeta,
) -> Result<SealedSample, SampleError> {
    let (version, key) = keyring.current(tenant).ok_or(SampleError::KeyUnavailable)?;
    let key = aead_key(&key)?;
    let mut nonce = [0u8; NONCE_LEN];
    rng.fill(&mut nonce).map_err(|_| SampleError::Random)?;
    let mut buf = encode(payload);
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce),
        Aad::from(aad(tenant, fp)),
        &mut buf,
    )
    .map_err(|_| SampleError::KeyUnavailable)?;
    Ok(SealedSample {
        fingerprint: fp,
        sample_id: 0,
        captured_at: meta.captured_at,
        latency_us: meta.latency_us,
        rows_examined: meta.rows_examined,
        rows_returned: meta.rows_returned,
        key_version: version,
        nonce,
        payload: buf,
        expires_at: meta.expires_at,
    })
}

/// Opens a sample of `tenant` (chapter 12: only the validation worker's
/// replay does this, in memory).
///
/// # Errors
/// [`SampleError::KeyUnavailable`] when its key version is gone;
/// [`SampleError::Tampered`] when it doesn't authenticate (another
/// tenant's or fingerprint's sample included); [`SampleError::Malformed`].
pub fn open(
    keyring: &dyn TenantKeyring,
    tenant: TenantId,
    s: &SealedSample,
) -> Result<OpenedSample, SampleError> {
    let key = keyring
        .key(tenant, s.key_version)
        .ok_or(SampleError::KeyUnavailable)?;
    let key = aead_key(&key)?;
    let mut buf = zeroize::Zeroizing::new(s.payload.clone());
    let plain = key
        .open_in_place(
            Nonce::assume_unique_for_key(s.nonce),
            Aad::from(aad(tenant, s.fingerprint)),
            &mut buf,
        )
        .map_err(|_| SampleError::Tampered)?;
    let payload = decode(plain).ok_or(SampleError::Malformed)?;
    Ok(OpenedSample {
        fingerprint: s.fingerprint,
        sample_id: s.sample_id,
        captured_at: s.captured_at,
        latency_us: s.latency_us,
        rows_examined: s.rows_examined,
        rows_returned: s.rows_returned,
        payload,
    })
}

/// Cuts every list slot to at most `max` values
/// (`workload.max_in_list_values`).
pub fn cap_lists(l: &mut SampledLiterals, max: usize) {
    for s in &mut l.slots {
        if let SampledSlot::List(v) = s {
            v.truncate(max);
        }
    }
}

// --- payload encoding ---------------------------------------------------------

const VERSION: u8 = 1;

const fn kind_byte(k: LiteralKind) -> u8 {
    match k {
        LiteralKind::Number => 1,
        LiteralKind::String => 2,
        LiteralKind::Hex => 3,
        LiteralKind::Bit => 4,
        LiteralKind::Bool => 5,
        LiteralKind::Temporal => 6,
        LiteralKind::Param => 7,
    }
}

const fn kind_of(b: u8) -> Option<LiteralKind> {
    Some(match b {
        1 => LiteralKind::Number,
        2 => LiteralKind::String,
        3 => LiteralKind::Hex,
        4 => LiteralKind::Bit,
        5 => LiteralKind::Bool,
        6 => LiteralKind::Temporal,
        7 => LiteralKind::Param,
        _ => return None,
    })
}

fn put_len(out: &mut Vec<u8>, n: usize) {
    out.extend_from_slice(&u32::try_from(n).unwrap_or(u32::MAX).to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_len(out, b.len());
    out.extend_from_slice(b);
}

fn put_opt(out: &mut Vec<u8>, s: Option<&str>) {
    match s {
        None => out.push(0),
        Some(s) => {
            out.push(1);
            put_bytes(out, s.as_bytes());
        }
    }
}

fn put_value(out: &mut Vec<u8>, v: &SampleValue) {
    out.push(kind_byte(v.kind));
    put_bytes(out, &v.text);
}

/// The payload's plaintext (module docs).
#[must_use]
pub fn encode(p: &SamplePayload) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(VERSION);
    let c = &p.context;
    out.extend_from_slice(&c.db_id.0.to_le_bytes());
    for s in [
        c.database.as_deref(),
        c.sql_mode.as_deref(),
        c.time_zone.as_deref(),
        c.charset.as_deref(),
        c.collation.as_deref(),
    ] {
        put_opt(&mut out, s);
    }
    put_len(&mut out, p.literals.slots.len());
    for s in &p.literals.slots {
        match s {
            SampledSlot::One(v) => {
                out.push(0);
                put_value(&mut out, v);
            }
            SampledSlot::List(vs) => {
                out.push(1);
                put_len(&mut out, vs.len());
                for v in vs {
                    put_value(&mut out, v);
                }
            }
        }
    }
    out
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn u8(&mut self) -> Option<u8> {
        let (b, rest) = self.0.split_first()?;
        self.0 = rest;
        Some(*b)
    }

    fn u32(&mut self) -> Option<u32> {
        if self.0.len() < 4 {
            return None;
        }
        let (b, rest) = self.0.split_at(4);
        self.0 = rest;
        Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn bytes(&mut self) -> Option<&'a [u8]> {
        let n = usize::try_from(self.u32()?).ok()?;
        if self.0.len() < n {
            return None;
        }
        let (b, rest) = self.0.split_at(n);
        self.0 = rest;
        Some(b)
    }

    /// An optional string: `Err` when malformed.
    fn opt(&mut self) -> Result<Option<String>, ()> {
        match self.u8().ok_or(())? {
            0 => Ok(None),
            1 => Ok(Some(
                String::from_utf8(self.bytes().ok_or(())?.to_vec()).map_err(|_| ())?,
            )),
            _ => Err(()),
        }
    }

    fn value(&mut self) -> Option<SampleValue> {
        let kind = kind_of(self.u8()?)?;
        Some(SampleValue {
            kind,
            text: self.bytes()?.to_vec(),
        })
    }
}

/// Reads a payload's plaintext; `None` if malformed.
#[must_use]
pub fn decode(b: &[u8]) -> Option<SamplePayload> {
    let mut r = Reader(b);
    if r.u8()? != VERSION {
        return None;
    }
    let db_id = DbId(r.u32()?);
    let context = SampleContext {
        db_id,
        database: r.opt().ok()?,
        sql_mode: r.opt().ok()?,
        time_zone: r.opt().ok()?,
        charset: r.opt().ok()?,
        collation: r.opt().ok()?,
    };
    let n = r.u32()?;
    let mut slots = Vec::with_capacity(usize::try_from(n.min(4096)).unwrap_or(0));
    for _ in 0..n {
        slots.push(match r.u8()? {
            0 => SampledSlot::One(r.value()?),
            1 => {
                let k = r.u32()?;
                let mut vs = Vec::with_capacity(usize::try_from(k.min(4096)).unwrap_or(0));
                for _ in 0..k {
                    vs.push(r.value()?);
                }
                SampledSlot::List(vs)
            }
            _ => return None,
        });
    }
    if !r.0.is_empty() {
        return None;
    }
    Some(SamplePayload {
        literals: SampledLiterals { slots },
        context,
    })
}

// --- the reservoir ------------------------------------------------------------

struct Slot {
    sample: SealedSample,
    written: bool,
}

struct Reservoir {
    bucket: u64,
    seen: u64,
    slots: Vec<Slot>,
}

/// Per-gateway reservoirs of sealed samples, `workload.samples_per_bucket`
/// per (tenant, fingerprint) per bucket (module docs).
pub struct SampleOutbox {
    per_bucket: usize,
    salt: u64,
    map: Mutex<HashMap<(TenantId, u64), Reservoir>>,
    /// Slots of finished buckets not written yet.
    leftover: Mutex<Vec<(TenantId, SealedSample)>>,
    counter: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for SampleOutbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SampleOutbox")
            .field("per_bucket", &self.per_bucket)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn sample_id(salt: u64, bucket: u64, slot: usize) -> u64 {
    use std::hash::Hasher;
    let mut h = twox_hash::XxHash3_64::with_seed(salt);
    h.write(&bucket.to_le_bytes());
    h.write(&(slot as u64).to_le_bytes());
    h.finish()
}

impl SampleOutbox {
    /// An outbox keeping `per_bucket` samples per fingerprint and bucket.
    /// `salt` makes sample ids unique to this process (pass a random
    /// value).
    #[must_use]
    pub fn new(per_bucket: u32, salt: u64) -> Self {
        Self {
            per_bucket: usize::try_from(per_bucket).unwrap_or(3),
            salt,
            map: Mutex::new(HashMap::new()),
            leftover: Mutex::new(Vec::new()),
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Offers a sealed sample of `bucket` (start, in seconds since the
    /// epoch): kept by reservoir sampling. Returns whether it was kept.
    pub fn offer(&self, tenant: TenantId, bucket: u64, mut sample: SealedSample) -> bool {
        if self.per_bucket == 0 {
            return false;
        }
        let mut map = lock(&self.map);
        let r = map
            .entry((tenant, sample.fingerprint.0))
            .or_insert(Reservoir {
                bucket,
                seen: 0,
                slots: Vec::new(),
            });
        if r.bucket != bucket {
            let old = std::mem::take(&mut r.slots);
            lock(&self.leftover).extend(
                old.into_iter()
                    .filter(|s| !s.written)
                    .map(|s| (tenant, s.sample)),
            );
            r.bucket = bucket;
            r.seen = 0;
        }
        r.seen += 1;
        let idx = if r.slots.len() < self.per_bucket {
            r.slots.len()
        } else {
            let roll = crate::recorder::mix(
                self.counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    ^ self.salt,
            );
            let j = usize::try_from(roll % r.seen).unwrap_or(usize::MAX);
            if j >= self.per_bucket {
                return false;
            }
            j
        };
        sample.sample_id = sample_id(self.salt, bucket, idx);
        let slot = Slot {
            sample,
            written: false,
        };
        if idx == r.slots.len() {
            r.slots.push(slot);
        } else {
            r.slots[idx] = slot;
        }
        true
    }

    /// The samples not written yet, marked written. Reservoirs of buckets
    /// before `current_bucket` are dropped afterwards.
    pub fn drain(&self, current_bucket: u64) -> Vec<(TenantId, SealedSample)> {
        let mut out = std::mem::take(&mut *lock(&self.leftover));
        let mut m = lock(&self.map);
        for ((tenant, _), r) in m.iter_mut() {
            for s in r.slots.iter_mut().filter(|s| !s.written) {
                s.written = true;
                out.push((*tenant, s.sample.clone()));
            }
        }
        m.retain(|_, r| r.bucket >= current_bucket);
        out
    }

    /// Puts samples back after a failed write.
    pub fn requeue(&self, rows: Vec<(TenantId, SealedSample)>) {
        let mut l = lock(&self.leftover);
        // Bounded: a catalog that stays down loses the oldest samples.
        l.extend(rows);
        let over = l.len().saturating_sub(10_000);
        if over > 0 {
            l.drain(..over);
        }
    }

    /// Forgets a tenant's reservoirs.
    pub fn forget_tenant(&self, tenant: TenantId) {
        lock(&self.map).retain(|k, _| k.0 != tenant);
        lock(&self.leftover).retain(|(t, _)| *t != tenant);
    }
}

#[cfg(test)]
mod tests;
