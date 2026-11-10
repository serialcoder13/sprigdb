//! Token signing keys (chapter 14, "SprigDB tokens and sessions": keys).
//!
//! **Where the book differs (decision ST18).** The chapter gives each DC
//! one Ed25519 key pair, made by the DC leader and kept in the DC's secret
//! store. SprigDB has no secret store yet, so each *gateway* makes its own
//! key pair in memory at start and publishes only the public key, at
//! `/sprig/g/token-keys/<dc_id>/<id>` in the global tier. Every gateway in
//! every DC verifies with public keys read from there, as the chapter has
//! it; no private key ever leaves the process that made it, and none is
//! shared. A gateway restart makes a new pair; tokens its old key signed
//! keep verifying, because the old public key stays published until
//! `verify_until`.
//!
//! - **kid** `<dc_id>/<gateway_id>-<12 hex>`, so the key's etcd path is
//!   `token-keys/<kid>`.
//! - **Rotation:** a new pair every `auth.token_key_rotation_days`
//!   ([`KeyRing::rotate_if_due`]), published before it signs.
//! - **`verify_until`** = creation + rotation period + 2 days (an access
//!   token lives at most a day, `auth.token_ttl_seconds`' hard bound). Any
//!   gateway deletes records past it ([`KeyRing::sweep`]).
//! - **Unknown kid:** one read of `token-keys/<kid>`; a key not found is
//!   remembered for 5 s, so forged kids can't make every request read etcd.
//!   Deleting a key's record makes every token it signed fail within the
//!   cache's 10 minutes (chapter 14's "a DC's signing key leaked").

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::signature::{Ed25519KeyPair, KeyPair as _};
use serde::{Deserialize, Serialize};
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigError};
use sprig_meta::{Consistency, ListOpts, MetaStore, Op, Record, Txn, Versioned};

/// Margin after a key's rotation period during which its tokens still
/// verify: longer than the longest access token (one day).
pub const VERIFY_MARGIN: Duration = Duration::from_secs(2 * 86_400);
/// How long a public key read from etcd is trusted before it is read again
/// (so a deleted key stops working).
pub const KNOWN_TTL: Duration = Duration::from_secs(600);
/// How long a kid not found is remembered.
pub const MISSING_TTL: Duration = Duration::from_secs(5);

/// The etcd record of one public key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenKeyRecord {
    /// Always `EdDSA`.
    pub alg: String,
    /// The raw 32-byte public key, base64url.
    pub public_key: String,
    /// The gateway that holds the private key.
    pub gateway: String,
    /// When the pair was made, Unix milliseconds.
    pub created_ms: u64,
    /// Verify tokens signed with it until then, Unix milliseconds.
    pub verify_until_ms: u64,
}

impl Versioned for TokenKeyRecord {
    const V: u32 = 1;
}

/// A private key with its id.
pub struct SigningKey {
    /// `<dc>/<id>`.
    pub kid: String,
    pair: Ed25519KeyPair,
    /// When it was made.
    pub created: SystemTime,
    /// Until when its tokens verify.
    pub verify_until: SystemTime,
}

impl std::fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigningKey")
            .field("kid", &self.kid)
            .finish_non_exhaustive()
    }
}

impl SigningKey {
    /// A new pair for `gateway` in `dc`.
    ///
    /// # Errors
    /// `Internal` if the OS random source fails.
    pub fn generate(
        dc: &DcId,
        gateway: &str,
        now: SystemTime,
        rotation: Duration,
    ) -> Result<Self> {
        let rng = ring::rand::SystemRandom::new();
        let doc = Ed25519KeyPair::generate_pkcs8(&rng)
            .map_err(|_| SprigError::internal("can't make a token signing key"))?;
        let pair = Ed25519KeyPair::from_pkcs8(doc.as_ref())
            .map_err(|_| SprigError::internal("can't load a token signing key"))?;
        let mut tag = [0u8; 6];
        getrandom::fill(&mut tag)
            .map_err(|e| SprigError::internal(format!("no randomness for a key id: {e}")))?;
        let hex: String = tag.iter().fold(String::new(), |mut acc, b| {
            use std::fmt::Write as _;
            let _ = write!(acc, "{b:02x}");
            acc
        });
        let gw: String = gateway
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                    c
                } else {
                    '_'
                }
            })
            .take(48)
            .collect();
        Ok(Self {
            kid: format!("{dc}/{gw}-{hex}"),
            pair,
            created: now,
            verify_until: now + rotation + VERIFY_MARGIN,
        })
    }

    /// The raw public key.
    #[must_use]
    pub fn public(&self) -> &[u8] {
        self.pair.public_key().as_ref()
    }

    /// Signs `msg`.
    #[must_use]
    pub fn sign(&self, msg: &[u8]) -> Vec<u8> {
        self.pair.sign(msg).as_ref().to_vec()
    }

    fn record(&self, gateway: &str) -> TokenKeyRecord {
        TokenKeyRecord {
            alg: super::jws::ALG.to_owned(),
            public_key: URL_SAFE_NO_PAD.encode(self.public()),
            gateway: gateway.to_owned(),
            created_ms: ms(self.created),
            verify_until_ms: ms(self.verify_until),
        }
    }
}

fn ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[derive(Clone)]
enum Known {
    Key {
        public: Vec<u8>,
        verify_until_ms: u64,
        read: Instant,
    },
    Missing(Instant),
}

/// This process's signing key and the public keys it has seen.
pub struct KeyRing {
    dc: DcId,
    gateway: String,
    current: Mutex<Arc<SigningKey>>,
    /// Keys this process made (verified without etcd).
    own: Mutex<HashMap<String, (Vec<u8>, u64)>>,
    known: Mutex<HashMap<String, Known>>,
    store: Option<Arc<dyn MetaStore>>,
}

impl std::fmt::Debug for KeyRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyRing")
            .field("kid", &self.signer().kid)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl KeyRing {
    /// A ring with a new key for `gateway` in `dc`; `store` is the global
    /// tier (`None`: tokens verify only on this process).
    ///
    /// # Errors
    /// `Internal` if no key can be made.
    pub fn new(
        dc: DcId,
        gateway: impl Into<String>,
        store: Option<Arc<dyn MetaStore>>,
        rotation: Duration,
    ) -> Result<Self> {
        let gateway = gateway.into();
        let k = SigningKey::generate(&dc, &gateway, SystemTime::now(), rotation)?;
        let own = HashMap::from([(k.kid.clone(), (k.public().to_vec(), ms(k.verify_until)))]);
        Ok(Self {
            dc,
            gateway,
            current: Mutex::new(Arc::new(k)),
            own: Mutex::new(own),
            known: Mutex::new(HashMap::new()),
            store,
        })
    }

    /// The key new tokens are signed with.
    #[must_use]
    pub fn signer(&self) -> Arc<SigningKey> {
        Arc::clone(&lock(&self.current))
    }

    /// Writes the current key's public half to the global tier.
    ///
    /// # Errors
    /// The store's.
    pub async fn publish(&self) -> Result<()> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let k = self.signer();
        let key = store.tier().key(&format!("token-keys/{}", k.kid))?;
        store
            .txn(Txn {
                when: vec![],
                then: vec![Op::Put {
                    key,
                    value: Record::new(k.record(&self.gateway)).encode()?,
                    lease: None,
                }],
                otherwise: vec![],
            })
            .await?;
        tracing::info!(kid = %k.kid, "token signing key published");
        Ok(())
    }

    /// Makes and publishes a new key when the current one is older than
    /// `rotation`. Returns whether it rotated.
    ///
    /// # Errors
    /// The store's (the old key keeps signing).
    pub async fn rotate_if_due(&self, rotation: Duration, now: SystemTime) -> Result<bool> {
        let age = now
            .duration_since(self.signer().created)
            .unwrap_or_default();
        if age < rotation {
            return Ok(false);
        }
        let k = Arc::new(SigningKey::generate(
            &self.dc,
            &self.gateway,
            now,
            rotation,
        )?);
        // Published before it signs.
        if let Some(store) = &self.store {
            let key = store.tier().key(&format!("token-keys/{}", k.kid))?;
            store
                .txn(Txn {
                    when: vec![],
                    then: vec![Op::Put {
                        key,
                        value: Record::new(k.record(&self.gateway)).encode()?,
                        lease: None,
                    }],
                    otherwise: vec![],
                })
                .await?;
        }
        lock(&self.own).insert(k.kid.clone(), (k.public().to_vec(), ms(k.verify_until)));
        *lock(&self.current) = k;
        tracing::info!(kid = %self.signer().kid, "token signing key rotated");
        Ok(true)
    }

    /// The public key `kid` names, if it is known and still verifies.
    ///
    /// # Errors
    /// The store's, when the key must be read.
    pub async fn public_key(&self, kid: &str, now: SystemTime) -> Result<Option<Vec<u8>>> {
        let now_ms = ms(now);
        if let Some((p, until)) = lock(&self.own).get(kid) {
            return Ok((*until > now_ms).then(|| p.clone()));
        }
        let cached = lock(&self.known).get(kid).cloned();
        match cached {
            Some(Known::Key {
                public,
                verify_until_ms,
                read,
            }) if read.elapsed() < KNOWN_TTL => {
                return Ok((verify_until_ms > now_ms).then_some(public));
            }
            Some(Known::Missing(at)) if at.elapsed() < MISSING_TTL => return Ok(None),
            _ => {}
        }
        let Some(store) = &self.store else {
            return Ok(None);
        };
        let key = store.tier().key(&format!("token-keys/{kid}"))?;
        let found = store
            .get(&key, Consistency::Serializable)
            .await?
            .and_then(|kv| Record::<TokenKeyRecord>::decode(&kv.value).ok())
            .and_then(|r| {
                URL_SAFE_NO_PAD
                    .decode(&r.body.public_key)
                    .ok()
                    .filter(|p| p.len() == 32 && r.body.alg == super::jws::ALG)
                    .map(|p| (p, r.body.verify_until_ms))
            });
        let mut known = lock(&self.known);
        if known.len() > 10_000 {
            known.clear();
        }
        if let Some((public, until)) = found {
            known.insert(
                kid.to_owned(),
                Known::Key {
                    public: public.clone(),
                    verify_until_ms: until,
                    read: Instant::now(),
                },
            );
            Ok((until > now_ms).then_some(public))
        } else {
            known.insert(kid.to_owned(), Known::Missing(Instant::now()));
            Ok(None)
        }
    }

    /// Deletes key records past `verify_until`, in every DC (any gateway
    /// may). Returns how many.
    ///
    /// # Errors
    /// The store's.
    pub async fn sweep(&self, now: SystemTime) -> Result<usize> {
        let Some(store) = &self.store else {
            return Ok(0);
        };
        let prefix = store.tier().key("token-keys/")?;
        let page = store
            .list(
                &prefix,
                ListOpts {
                    at: None,
                    after: None,
                    limit: 1000,
                },
            )
            .await?;
        let now_ms = ms(now);
        let dead: Vec<_> = page
            .kvs
            .iter()
            .filter(|kv| {
                Record::<TokenKeyRecord>::decode(&kv.value)
                    .is_ok_and(|r| r.body.verify_until_ms <= now_ms)
            })
            .map(|kv| Op::Delete(kv.key.clone()))
            .collect();
        let n = dead.len();
        if n > 0 {
            store
                .txn(Txn {
                    when: vec![],
                    then: dead,
                    otherwise: vec![],
                })
                .await?;
        }
        Ok(n)
    }
}
