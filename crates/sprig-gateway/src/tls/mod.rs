//! Certificate bundles: watched PEM files behind a `rustls` resolver.
//!
//! A [`CertBundle`] holds the current certificate chain and key of one
//! bundle (`tls.bundles.<bundle>.cert_path`, `.key_path`) and is the
//! `rustls` resolver of every listener that uses it. [`CertStore::poll`]
//! re-reads the files; a change seen on two polls in a row (the debounce) is
//! loaded and validated, then swapped in atomically. New handshakes use the
//! new pair; open connections keep the one they negotiated. A bad pair is
//! rejected and the old one kept.
//!
//! Files are polled rather than watched with `notify`, whose licence (CC0)
//! is not on the allow-list. Comparing contents, not modification times,
//! also catches Kubernetes Secret updates, which swap a symlink.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use rustls::DigitallySignedStruct;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::UnixTime;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};

use crate::listener::TlsVersion;

mod der;

pub use der::not_after;

/// How often [`CertStore::spawn_watcher`] polls by default. A change is
/// loaded on the second poll that sees it, so rotation takes one to two
/// intervals (debounced 1 s).
pub const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The crypto provider: `ring`, as the workspace's `rustls` features select.
pub(crate) fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// What one poll of a bundle found.
#[derive(Debug)]
pub enum PollOutcome {
    /// The files are as last loaded.
    Unchanged,
    /// The files changed; they are loaded once a second poll sees the same
    /// contents.
    Pending,
    /// A new pair was validated and swapped in.
    Rotated,
    /// The new pair failed validation; the old one is kept until the files
    /// change again.
    Rejected(SprigError),
}

/// The raw contents of a bundle's two files.
#[derive(Clone, PartialEq, Eq)]
struct Files {
    cert: Vec<u8>,
    key: Vec<u8>,
}

/// A validated pair.
struct Loaded {
    key: Arc<CertifiedKey>,
    not_after: SystemTime,
    files: Files,
}

#[derive(Default)]
struct Watch {
    /// Contents seen on the previous poll and not loaded yet.
    pending: Option<Files>,
    /// Contents that failed validation; not retried until they change.
    rejected: Option<Files>,
}

/// One certificate bundle: a chain and key from PEM files, the resolver of
/// every listener that names it.
pub struct CertBundle {
    name: String,
    cert_path: PathBuf,
    key_path: PathBuf,
    current: ArcSwap<Loaded>,
    watch: Mutex<Watch>,
    clock: Arc<dyn Clock>,
}

impl fmt::Debug for CertBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CertBundle")
            .field("name", &self.name)
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .finish_non_exhaustive()
    }
}

impl CertBundle {
    /// Loads and validates the bundle `name` from `cert_path` (the chain,
    /// leaf first) and `key_path` (PKCS#8, PKCS#1 or SEC1).
    ///
    /// # Errors
    /// `ConfigInvalid` if a file can't be read, has no certificate or key,
    /// the key doesn't match the leaf, or the leaf has expired.
    pub fn load(
        name: impl Into<String>,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
        clock: Arc<dyn Clock>,
    ) -> Result<Arc<Self>> {
        let (name, cert_path, key_path) = (name.into(), cert_path.into(), key_path.into());
        let files = read_files(&cert_path, &key_path).map_err(|e| bundle_error(&name, &e))?;
        let loaded = validate(files, clock.wall()).map_err(|e| bundle_error(&name, &e))?;
        let bundle = Arc::new(Self {
            name,
            cert_path,
            key_path,
            current: ArcSwap::from_pointee(loaded),
            watch: Mutex::new(Watch::default()),
            clock,
        });
        bundle.export_expiry();
        Ok(bundle)
    }

    /// The bundle name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The certificate file.
    #[must_use]
    pub fn cert_path(&self) -> &Path {
        &self.cert_path
    }

    /// The key file.
    #[must_use]
    pub fn key_path(&self) -> &Path {
        &self.key_path
    }

    /// The pair new handshakes use.
    #[must_use]
    pub fn certified_key(&self) -> Arc<CertifiedKey> {
        self.current.load().key.clone()
    }

    /// When the current leaf certificate expires.
    #[must_use]
    pub fn not_after(&self) -> SystemTime {
        self.current.load().not_after
    }

    /// Seconds until the current leaf expires; negative once it has.
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // seconds to expiry fit an f64 gauge
    pub fn seconds_to_expiry(&self) -> f64 {
        let now = self.clock.wall();
        let not_after = self.not_after();
        match not_after.duration_since(now) {
            Ok(d) => d.as_secs_f64(),
            Err(e) => -e.duration().as_secs_f64(),
        }
    }

    /// Re-reads the files and rotates if they changed and the change has
    /// settled (seen on two polls in a row).
    pub fn poll(&self) -> PollOutcome {
        let outcome = self.poll_inner();
        match &outcome {
            PollOutcome::Rotated => {
                tracing::info!(event = "gateway.cert_rotated", bundle = %self.name, "certificate rotated");
            }
            PollOutcome::Rejected(e) => {
                tracing::error!(event = "gateway.cert_rejected", bundle = %self.name, error = %e, source = ?std::error::Error::source(e), "new certificate rejected; keeping the old one");
            }
            PollOutcome::Unchanged | PollOutcome::Pending => {}
        }
        self.export_expiry();
        outcome
    }

    fn poll_inner(&self) -> PollOutcome {
        // Mid-rotation a file can be missing for a moment; the next poll sees
        // the finished state.
        let Ok(files) = read_files(&self.cert_path, &self.key_path) else {
            return PollOutcome::Pending;
        };
        let mut w = self.watch.lock().unwrap_or_else(PoisonError::into_inner);
        if files == self.current.load().files {
            w.pending = None;
            return PollOutcome::Unchanged;
        }
        if w.rejected.as_ref() == Some(&files) {
            return PollOutcome::Unchanged;
        }
        if w.pending.as_ref() != Some(&files) {
            w.pending = Some(files);
            return PollOutcome::Pending;
        }
        w.pending = None;
        match validate(files.clone(), self.clock.wall()) {
            Ok(loaded) => {
                w.rejected = None;
                self.current.store(Arc::new(loaded));
                PollOutcome::Rotated
            }
            Err(e) => {
                w.rejected = Some(files);
                PollOutcome::Rejected(bundle_error(&self.name, &e))
            }
        }
    }

    fn export_expiry(&self) {
        metrics::gauge!("sprig_gateway_cert_expiry_seconds", "bundle" => self.name.clone())
            .set(self.seconds_to_expiry());
    }
}

impl ResolvesServerCert for CertBundle {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.certified_key())
    }
}

/// A `rustls` server configuration that takes its certificate from
/// `bundle` at every handshake, so rotation needs no new configuration.
///
/// # Errors
/// `Internal` if the provider rejects the protocol versions (a bug).
pub fn server_config(
    bundle: Arc<CertBundle>,
    min: TlsVersion,
) -> Result<Arc<rustls::ServerConfig>> {
    server_config_with(bundle, min, None)
}

/// [`server_config`] that also asks for a client certificate signed by
/// `client.0`'s CAs: verified when presented, and required when `client.1`
/// (`listener.<name>.client_cert = required`), else optional. The
/// verifier reads the CAs at each handshake, so a changed CA file applies
/// without a new configuration ([`ClientCa::poll`]).
///
/// # Errors
/// `Internal` if the provider rejects the protocol versions (a bug).
pub fn server_config_with(
    bundle: Arc<CertBundle>,
    min: TlsVersion,
    client: Option<(&Arc<ClientCa>, bool)>,
) -> Result<Arc<rustls::ServerConfig>> {
    let versions: &[&'static rustls::SupportedProtocolVersion] = match min {
        TlsVersion::V1_2 => &[&rustls::version::TLS13, &rustls::version::TLS12],
        TlsVersion::V1_3 => &[&rustls::version::TLS13],
    };
    let builder = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(versions)
        .map_err(|e| SprigError::internal("TLS protocol versions").with_source(e))?;
    let cfg = match client {
        None => builder.with_no_client_auth().with_cert_resolver(bundle),
        Some((ca, required)) => builder
            .with_client_cert_verifier(Arc::new(LiveClientVerifier {
                ca: Arc::clone(ca),
                required,
            }))
            .with_cert_resolver(bundle),
    };
    Ok(Arc::new(cfg))
}

/// A client CA file's contents, checked, with the two verifiers built
/// over them.
struct CaLoaded {
    pem: Vec<u8>,
    roots: usize,
    optional: Arc<dyn ClientCertVerifier>,
    required: Arc<dyn ClientCertVerifier>,
}

/// The CAs whose client certificates listeners accept
/// (`tls.bundles.default.client_ca_path`): PEM, one or more certificates.
/// Re-read as the server certificate is ([`CertStore::poll`]):
/// a change seen on two polls in a row is loaded and swapped in for new
/// handshakes; a file that doesn't load keeps the old CAs, with an error
/// logged. Connections already open keep running.
pub struct ClientCa {
    /// The file.
    pub path: PathBuf,
    current: ArcSwap<CaLoaded>,
    watch: Mutex<CaWatch>,
}

#[derive(Default)]
struct CaWatch {
    pending: Option<Vec<u8>>,
    rejected: Option<Vec<u8>>,
}

impl fmt::Debug for ClientCa {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientCa")
            .field("path", &self.path)
            .field("roots", &self.current.load().roots)
            .finish_non_exhaustive()
    }
}

/// Two handles on one file are the same CA: its contents change in place.
impl PartialEq for ClientCa {
    fn eq(&self, o: &Self) -> bool {
        self.path == o.path
    }
}

fn ca_loaded(path: &Path, pem: Vec<u8>) -> std::result::Result<CaLoaded, String> {
    let mut roots = rustls::RootCertStore::empty();
    for c in CertificateDer::pem_slice_iter(&pem) {
        let c = c.map_err(|e| format!("{}: {e}", path.display()))?;
        roots
            .add(c)
            .map_err(|e| format!("{}: {e}", path.display()))?;
    }
    if roots.is_empty() {
        return Err(format!("{}: no certificate", path.display()));
    }
    let n = roots.len();
    let roots = Arc::new(roots);
    let build = |required: bool| {
        let mut v = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::clone(&roots),
            provider(),
        );
        if !required {
            v = v.allow_unauthenticated();
        }
        v.build()
            .map_err(|e| format!("client CA {}: {e}", path.display()))
    };
    Ok(CaLoaded {
        pem,
        roots: n,
        optional: build(false)?,
        required: build(true)?,
    })
}

impl ClientCa {
    /// Reads and checks `path`.
    ///
    /// # Errors
    /// Why the file isn't a usable CA bundle.
    pub fn load(path: &Path) -> std::result::Result<Self, String> {
        let pem = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Self::from_pem(path, pem)
    }

    /// A CA bundle from PEM text (`path` names it in messages and is read
    /// again by [`Self::poll`]).
    ///
    /// # Errors
    /// Why the text isn't a usable CA bundle.
    pub fn from_pem(path: &Path, pem: Vec<u8>) -> std::result::Result<Self, String> {
        Ok(Self {
            path: path.to_owned(),
            current: ArcSwap::from_pointee(ca_loaded(path, pem)?),
            watch: Mutex::new(CaWatch::default()),
        })
    }

    fn verifier(&self, required: bool) -> Arc<dyn ClientCertVerifier> {
        let c = self.current.load();
        if required {
            Arc::clone(&c.required)
        } else {
            Arc::clone(&c.optional)
        }
    }

    /// Re-reads the file; a change seen on two polls in a row is loaded
    /// (as [`CertBundle::poll`]).
    pub fn poll(&self) -> PollOutcome {
        let outcome = self.poll_inner();
        let path = self.path.display().to_string();
        match &outcome {
            PollOutcome::Rotated => {
                tracing::info!(event = "gateway.client_ca_reloaded", path = %path, "client CA file reloaded");
            }
            PollOutcome::Rejected(e) => {
                tracing::error!(event = "gateway.client_ca_rejected", path = %path, error = %e, "changed client CA file rejected; keeping the old CAs");
            }
            PollOutcome::Unchanged | PollOutcome::Pending => {}
        }
        outcome
    }

    fn poll_inner(&self) -> PollOutcome {
        let Ok(pem) = std::fs::read(&self.path) else {
            return PollOutcome::Pending;
        };
        let mut w = self.watch.lock().unwrap_or_else(PoisonError::into_inner);
        if pem == self.current.load().pem {
            w.pending = None;
            return PollOutcome::Unchanged;
        }
        if w.rejected.as_ref() == Some(&pem) {
            return PollOutcome::Unchanged;
        }
        if w.pending.as_ref() != Some(&pem) {
            w.pending = Some(pem);
            return PollOutcome::Pending;
        }
        w.pending = None;
        match ca_loaded(&self.path, pem.clone()) {
            Ok(l) => {
                w.rejected = None;
                self.current.store(Arc::new(l));
                PollOutcome::Rotated
            }
            Err(e) => {
                w.rejected = Some(pem);
                PollOutcome::Rejected(SprigError::new(SprigCode::ConfigInvalid, e))
            }
        }
    }
}

/// A listener's client verifier over a [`ClientCa`]: each handshake uses
/// the CAs loaded now. Root hints aren't sent (they would be the old
/// CAs' after a reload); clients send their certificate anyway.
#[derive(Debug)]
struct LiveClientVerifier {
    ca: Arc<ClientCa>,
    required: bool,
}

impl ClientCertVerifier for LiveClientVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        self.required
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> std::result::Result<ClientCertVerified, rustls::Error> {
        self.ca
            .verifier(self.required)
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.ca
            .verifier(self.required)
            .verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.ca
            .verifier(self.required)
            .verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.ca.verifier(self.required).supported_verify_schemes()
    }
}

/// The subject and issuer of a DER certificate in MySQL's form
/// (`/C=US/O=Acme/CN=alice`), for `REQUIRE SUBJECT` and `ISSUER`.
#[must_use]
pub fn client_dn(cert: &[u8]) -> Option<sprig_auth::mysql::ClientDn> {
    let (subject, issuer) = der::names(cert)?;
    Some(sprig_auth::mysql::ClientDn { subject, issuer })
}

/// Every bundle in use, by name, and the poller that rotates them.
pub struct CertStore {
    bundles: Mutex<HashMap<String, Arc<CertBundle>>>,
    /// Client CA files in use, by path: polled with the
    /// bundles.
    client_cas: Mutex<HashMap<PathBuf, Arc<ClientCa>>>,
    clock: Arc<dyn Clock>,
}

impl fmt::Debug for CertStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<String> = self.lock().keys().cloned().collect();
        f.debug_struct("CertStore")
            .field("bundles", &names)
            .finish()
    }
}

impl CertStore {
    /// An empty store.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            bundles: Mutex::new(HashMap::new()),
            client_cas: Mutex::new(HashMap::new()),
            clock,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<CertBundle>>> {
        self.bundles.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The bundle `name` over these files: the one already loaded if its
    /// paths are the same, otherwise a newly loaded one, which replaces it
    /// for future lookups (listeners holding the old one keep it until they
    /// are re-applied).
    ///
    /// # Errors
    /// As [`CertBundle::load`].
    pub fn bundle(&self, name: &str, cert_path: &Path, key_path: &Path) -> Result<Arc<CertBundle>> {
        if let Some(b) = self.lock().get(name)
            && b.cert_path == cert_path
            && b.key_path == key_path
        {
            return Ok(b.clone());
        }
        let b = CertBundle::load(name, cert_path, key_path, self.clock.clone())?;
        self.lock().insert(name.to_owned(), b.clone());
        Ok(b)
    }

    /// The client CA file at `path`: the one already loaded, whose
    /// contents [`Self::poll`] keeps current, otherwise a newly loaded one.
    ///
    /// # Errors
    /// Why the file isn't a usable CA bundle.
    pub fn client_ca(&self, path: &Path) -> std::result::Result<Arc<ClientCa>, String> {
        let mut g = self
            .client_cas
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(ca) = g.get(path) {
            return Ok(Arc::clone(ca));
        }
        let ca = Arc::new(ClientCa::load(path)?);
        g.insert(path.to_owned(), Arc::clone(&ca));
        Ok(ca)
    }

    /// Polls every bundle once (see [`CertBundle::poll`]), then every
    /// client CA file (named `client_ca:<path>`, [`ClientCa::poll`]).
    pub fn poll(&self) -> Vec<(String, PollOutcome)> {
        let bundles: Vec<Arc<CertBundle>> = self.lock().values().cloned().collect();
        let cas: Vec<Arc<ClientCa>> = self
            .client_cas
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        bundles
            .into_iter()
            .map(|b| (b.name.clone(), b.poll()))
            .chain(
                cas.into_iter()
                    .map(|c| (format!("client_ca:{}", c.path.display()), c.poll())),
            )
            .collect()
    }

    /// Polls every `interval` on a blocking thread until `stop` is
    /// cancelled.
    pub fn spawn_watcher(
        self: &Arc<Self>,
        interval: Duration,
        stop: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let store = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    _ = tick.tick() => {}
                }
                let s = store.clone();
                if let Err(e) = tokio::task::spawn_blocking(move || s.poll()).await {
                    tracing::error!(error = %e, "certificate poll failed");
                }
            }
        })
    }
}

fn read_files(cert_path: &Path, key_path: &Path) -> std::result::Result<Files, String> {
    let read = |p: &Path| std::fs::read(p).map_err(|e| format!("can't read {}: {e}", p.display()));
    Ok(Files {
        cert: read(cert_path)?,
        key: read(key_path)?,
    })
}

/// Parses and checks a pair: a chain with at least one certificate, a key
/// the provider can load that matches the leaf, and a leaf not yet expired.
fn validate(files: Files, now: SystemTime) -> std::result::Result<Loaded, String> {
    let chain = CertificateDer::pem_slice_iter(&files.cert)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| format!("bad certificate PEM: {e}"))?;
    let leaf = chain
        .first()
        .ok_or("no certificate in the certificate file")?;
    let not_after = der::not_after(leaf).ok_or("can't read the leaf certificate's validity")?;
    if not_after <= now {
        let secs = not_after
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        return Err(format!(
            "the leaf certificate expired (notAfter {secs} s after the epoch)"
        ));
    }
    let key = PrivateKeyDer::from_pem_slice(&files.key).map_err(|e| format!("bad key PEM: {e}"))?;
    let key = CertifiedKey::from_der(chain, key, &provider())
        .map_err(|e| format!("certificate and key don't match: {e}"))?;
    Ok(Loaded {
        key: Arc::new(key),
        not_after,
        files,
    })
}

fn bundle_error(name: &str, why: &str) -> SprigError {
    SprigError::new(
        SprigCode::ConfigInvalid,
        format!("certificate bundle {name}: {why}"),
    )
}

#[cfg(test)]
mod tests;
