//! Approval webhooks over the sinks' webhook target: notifications of
//! approval events.
//!
//! - **The platform's** `approvals.webhook_url` (`http://` or `https://`,
//!   DC and global scope) gets every event. The operator chose the host, so
//!   it is the connection's only allowed destination (internal addresses
//!   included); TLS checks the server against the Mozilla roots. Events
//!   are signed with `[controller] approvals_webhook_secret_file`.
//! - **A tenant's** `approvals.webhook_connection` names one of its own
//!   webhook connections (`<db>.<connection>`) or a platform connection
//!   granted to it (`platform:<name>`). The tenant's events, and those of
//!   descendants escalated to it, go there under every rule a sink's
//!   delivery has: `sink.enabled` and `sink.allowed_kinds`,
//!   `sink.destination_allowlist` and the address checks, the tenant's
//!   residency, the connection's CA (`ca_pem`), client certificate and
//!   authentication, and its sealed `signing_secret` (opened here with the
//!   master key file, `[controller] workload_key_file`).
//!
//! Every POST carries `Sprig-Event`, `Sprig-Idempotency-Key` and the
//! connection's signature; delivery retries with the sinks' backoff
//! ([`super::notify::WEBHOOK_ATTEMPTS`] attempts) and never blocks a
//! decision.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hyper::body::Bytes;
use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use serde_json::{Map, json};
use sprig_catalog::CatalogLocator;
use sprig_common::ids::{DcId, TenantId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::MetaStore;
use sprig_settings::LiveSettings;
use sprig_sinks::job::SinkEnv;
use sprig_sinks::model::{ConnKind, ConnRef, Outcome, TargetKind};
use sprig_sinks::policy::{Destinations, Residency, Unpinned};
use sprig_sinks::secrets::SinkKeyring;
use sprig_sinks::store::SinksStore;
use sprig_sinks::target::webhook::WebhookTarget;
use sprig_sinks::target::{Conn, MemSecrets, Opts};

use super::notify::WEBHOOK_ATTEMPTS;

/// How long an opened target (its pooled connections, its opened
/// credentials) is reused before the connection is read again.
pub const REUSE: Duration = Duration::from_secs(60);

/// Where an event goes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Dest {
    /// `approvals.webhook_url`.
    Platform(String),
    /// `approvals.webhook_connection` of a tenant.
    Tenant(TenantId, String),
}

/// A parsed `approvals.webhook_connection`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnName {
    /// `<db>.<connection>`.
    Own {
        /// The database.
        db: String,
        /// The connection.
        name: String,
    },
    /// `platform:<name>`.
    Platform(String),
}

impl ConnName {
    /// Parses the setting's value.
    ///
    /// # Errors
    /// 39977 for anything else.
    pub fn parse(v: &str) -> Result<Self> {
        let v = v.trim();
        let bad = || {
            SprigError::new(
                SprigCode::ApprovalPolicyInvalid,
                format!(
                    "approvals.webhook_connection {v:?}: expected <database>.<connection> or \
                     platform:<name>"
                ),
            )
        };
        if let Some(p) = v.strip_prefix("platform:") {
            let p = p.trim();
            if p.is_empty() || p.contains(['.', ' ']) {
                return Err(bad());
            }
            return Ok(ConnName::Platform(p.to_owned()));
        }
        let (db, name) = v.split_once('.').ok_or_else(bad)?;
        let unq = |s: &str| s.trim().trim_matches('`').to_owned();
        let (db, name) = (unq(db), unq(name));
        if db.is_empty() || name.is_empty() || name.contains('.') {
            return Err(bad());
        }
        Ok(ConnName::Own { db, name })
    }
}

/// Opens tenants' webhook connections for approval events: the sinks'
/// [`SinkEnv`] as the controller sees it.
pub struct Webhooks {
    locator: Arc<dyn CatalogLocator>,
    dc_store: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    keyring: Option<Arc<dyn SinkKeyring>>,
    live: Option<LiveSettings>,
    dc: DcId,
    /// `SPRIG_SINK_SECRET_DIR`, which overrides `sink.platform_secret_dir`.
    secret_dir: Option<PathBuf>,
    webhook_secret: Option<String>,
    targets: Mutex<BTreeMap<Dest, (Instant, Arc<WebhookTarget>)>>,
}

impl std::fmt::Debug for Webhooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Webhooks")
            .field("dc", &self.dc)
            .field("keyring", &self.keyring.is_some())
            .finish_non_exhaustive()
    }
}

impl Webhooks {
    /// Over the controller's catalogs and tiers; `key_file` is the master
    /// key file sink credentials are sealed under; `webhook_secret` signs
    /// `approvals.webhook_url` events.
    #[must_use]
    pub fn new(
        locator: Arc<dyn CatalogLocator>,
        dc_store: Arc<dyn MetaStore>,
        global: Arc<dyn MetaStore>,
        key_file: Option<&std::path::Path>,
        live: Option<LiveSettings>,
        dc: DcId,
        webhook_secret: Option<String>,
    ) -> Self {
        let keyring = key_file.and_then(
            |p| match sprig_workload::keyring::MasterKeyring::from_file(p) {
                Ok(k) => Some(Arc::new(k) as Arc<dyn SinkKeyring>),
                Err(e) => {
                    tracing::warn!(error = %e, "approvals: sink credential keys not loaded");
                    None
                }
            },
        );
        Self {
            locator,
            dc_store,
            global,
            keyring,
            live,
            dc,
            secret_dir: std::env::var_os("SPRIG_SINK_SECRET_DIR").map(PathBuf::from),
            webhook_secret,
            targets: Mutex::new(BTreeMap::new()),
        }
    }

    fn cached(&self, d: &Dest) -> Option<Arc<WebhookTarget>> {
        let g = self.targets.lock().unwrap_or_else(PoisonError::into_inner);
        g.get(d)
            .filter(|(at, _)| at.elapsed() < REUSE)
            .map(|(_, t)| Arc::clone(t))
    }

    fn keep(&self, d: Dest, t: &Arc<WebhookTarget>) {
        self.targets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(d, (Instant::now(), Arc::clone(t)));
    }

    /// The target of `d`, opened (or reused).
    ///
    /// # Errors
    /// A bad URL or setting; the connection is gone, its credentials
    /// don't open, or the tenant may not use it.
    pub async fn target(&self, d: &Dest) -> Result<Arc<WebhookTarget>> {
        if let Some(t) = self.cached(d) {
            return Ok(t);
        }
        let t = Arc::new(match d {
            Dest::Platform(url) => self.platform_target(url)?,
            Dest::Tenant(t, v) => self.tenant_target(*t, &ConnName::parse(v)?).await?,
        });
        self.keep(d.clone(), &t);
        Ok(t)
    }

    /// `approvals.webhook_url`: the URL's host is the only destination.
    fn platform_target(&self, url: &str) -> Result<WebhookTarget> {
        let (host, port, _) = sprig_sinks::policy::url_host_port(url)?;
        let mut options = Map::new();
        options.insert("url".into(), json!(url));
        let secrets = if let Some(s) = &self.webhook_secret {
            MemSecrets::of(&[("signing_secret", s.as_str())])
        } else {
            options.insert("signature".into(), json!("none"));
            MemSecrets::default()
        };
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        let conn = Conn {
            name: "approvals.webhook_url".into(),
            kind: ConnKind::Webhook,
            options,
            secrets: Arc::new(secrets),
            dest: Arc::new(Destinations {
                allow: Vec::new(),
                residency: Box::new(Unpinned),
                platform_hosts: Some(vec![format!("{bare}:{port}")]),
                metadata_allowed: false,
            }),
        };
        WebhookTarget::for_events(conn, Opts::default(), "sprig_approvals")
    }

    /// A tenant's own connection or a platform connection granted to it.
    async fn tenant_target(&self, tenant: TenantId, name: &ConnName) -> Result<WebhookTarget> {
        let ep = self.locator.writer(tenant).await?;
        let schema = ep.store.schema().to_owned();
        let mut c = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
        })?;
        let store = SinksStore::new(schema.clone());
        let (db_id, r) = match name {
            ConnName::Own { db, name } => {
                let id: Option<u32> = c
                    .exec_first(
                        format!(
                            "SELECT db_id FROM {}.`databases` WHERE name_key = ? AND state = 'active'",
                            quote(&schema)
                        ),
                        (db.to_lowercase(),),
                    )
                    .await
                    .map_err(|e| SprigError::internal("approvals webhook: databases").with_source(e))?;
                let db_id = id.ok_or_else(|| not_found(&format!("database {db}")))?;
                let st = store.load(&mut c).await?;
                let def = st
                    .connection(db_id, name)
                    .or_else(|| st.connection(db_id, &name.to_lowercase()))
                    .ok_or_else(|| not_found(&format!("connection {db}.{name}")))?;
                if def.kind != ConnKind::Webhook {
                    return Err(not_found(&format!("webhook connection {db}.{name}")));
                }
                (
                    db_id,
                    ConnRef::Tenant {
                        id: def.id,
                        name: def.name.clone(),
                    },
                )
            }
            ConnName::Platform(p) => (0, ConnRef::Platform { name: p.clone() }),
        };
        let probe = sprig_sinks::model::SinkDef {
            id: sprig_sinks::model::SinkId(0),
            db_id,
            name: "sprig_approvals".into(),
            source: sprig_sinks::model::SourceSpec::Stream {
                id: 0,
                name: "sprig_recommendations".into(),
                partitions: 1,
            },
            filter_sql: None,
            projection: None,
            transform: None,
            target: sprig_sinks::model::TargetSpec::Webhook { path: None },
            connections: vec![r.clone()],
            guarantee: sprig_sinks::model::Guarantee::AtLeastOnce,
            dead_letter: None,
            start_at: sprig_sinks::model::StartAt::Latest,
            owner_user_id: 0,
            state: sprig_sinks::model::SinkState::Running,
            state_reason: None,
            definition_ver: 0,
            options: BTreeMap::new(),
        };
        let opts = sprig_sinks::job::resolve_opts(&mut c, &schema, &probe).await?;
        let opts = match &self.live {
            Some(l) => sprig_sinks::job::over_global(opts, &l.snapshot(), &self.dc),
            None => opts,
        };
        sprig_sinks::policy::check_enabled(
            opts.flag("enabled", true),
            opts.get("allowed_kinds")
                .unwrap_or("webhook,kafka,s3,database,script"),
            TargetKind::Webhook,
        )?;
        let conn = sprig_sinks::job::open_conn(self, tenant, &mut c, &store, &r, &opts).await?;
        drop(c);
        if conn.kind != ConnKind::Webhook {
            return Err(not_found(&format!("webhook connection {}", r.name())));
        }
        WebhookTarget::for_events(conn, opts, "sprig_approvals")
    }

    /// Delivers `body` to `d` with retries; counts the result.
    pub async fn deliver(&self, d: &Dest, body: Bytes, idem: &str, event: &str) -> bool {
        let rng = sprig_sinks::runtime::Rng::from_os();
        for n in 1..=WEBHOOK_ATTEMPTS {
            let (hint, why) = match self.target(d).await {
                Ok(t) => match t.post_event(body.clone(), idem, event).await {
                    Ok(Outcome::Ok) => return true,
                    Ok(Outcome::Fail { reason }) => {
                        tracing::warn!(dest = ?d, event, reason, "approvals: webhook refused the event");
                        return false;
                    }
                    Ok(Outcome::Retry { after, reason }) => (after, reason),
                    Err(e) if e.retryable || e.code() == Some(SprigCode::SinkDeliveryFailed) => {
                        (None, e.message)
                    }
                    Err(e) => {
                        tracing::warn!(dest = ?d, event, error = %e, "approvals: webhook not deliverable");
                        return false;
                    }
                },
                Err(e) => {
                    // A setting, a missing connection or a refused
                    // destination: retrying doesn't help.
                    tracing::warn!(dest = ?d, event, error = %e, "approvals: webhook connection not opened");
                    return false;
                }
            };
            if n == WEBHOOK_ATTEMPTS {
                tracing::warn!(dest = ?d, event, why, "approvals: webhook not delivered");
                break;
            }
            // A failed connection is opened again on the next attempt.
            self.targets
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(d);
            tokio::time::sleep(sprig_sinks::runtime::backoff(
                n,
                hint,
                Duration::from_millis(500),
                Duration::from_secs(60),
                &rng,
            ))
            .await;
        }
        false
    }
}

fn not_found(what: &str) -> SprigError {
    SprigError::new(
        SprigCode::SinkNotFound,
        format!("approvals.webhook_connection: no {what}"),
    )
}

#[async_trait]
impl SinkEnv for Webhooks {
    async fn catalog(&self, tenant: TenantId) -> Result<(Pool, String)> {
        let ep = self.locator.writer(tenant).await?;
        Ok((ep.pool, ep.store.schema().to_owned()))
    }

    async fn shard_pool(&self, _tenant: TenantId) -> Result<Pool> {
        Err(SprigError::internal(
            "approval webhooks never open shard pools",
        ))
    }

    fn dc(&self) -> Arc<dyn MetaStore> {
        Arc::clone(&self.dc_store)
    }

    fn global(&self) -> Option<Arc<dyn MetaStore>> {
        Some(Arc::clone(&self.global))
    }

    fn keyring(&self) -> Option<Arc<dyn SinkKeyring>> {
        self.keyring.clone()
    }

    async fn residency_of(&self, tenant: TenantId) -> Result<Box<dyn Residency>> {
        // The sink workers' rule, from `sprig-residency`.
        sprig_sinks::residency::residency_of(&*self.global, tenant).await
    }

    fn secret_dir(&self) -> PathBuf {
        if let Some(d) = &self.secret_dir {
            return d.clone();
        }
        let key = &sprig_sinks::settings::PLATFORM_SECRET_DIR;
        PathBuf::from(
            match &self.live {
                Some(l) => l.get(key, &sprig_settings::SettingCtx::process(&self.dc)),
                None => key.default.clone(),
            }
            .as_ref(),
        )
    }

    fn worker_id(&self) -> String {
        format!("controller-{}", self.dc)
    }

    fn settings(&self) -> Option<(sprig_settings::Snapshot, DcId)> {
        self.live.as_ref().map(|l| (l.snapshot(), self.dc.clone()))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::many_single_char_names)]

    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Reads one HTTP/1.1 request (headers and a content-length body),
    /// answers `status`; returns the head and the body.
    async fn one_request<S>(mut s: S, status: u16) -> (String, Vec<u8>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let mut buf = Vec::new();
        let mut b = [0u8; 4096];
        let head_end = loop {
            let n = s.read(&mut b).await.unwrap();
            assert!(n > 0, "closed before the request ended");
            buf.extend_from_slice(&b[..n]);
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
        let len: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .map_or(0, |v| v.trim().parse().unwrap());
        while buf.len() < head_end + len {
            let n = s.read(&mut b).await.unwrap();
            buf.extend_from_slice(&b[..n]);
        }
        let reply = format!("HTTP/1.1 {status} X\r\ncontent-length: 0\r\n\r\n");
        s.write_all(reply.as_bytes()).await.unwrap();
        s.flush().await.unwrap();
        (head, buf[head_end..head_end + len].to_vec())
    }

    fn header(head: &str, name: &str) -> String {
        head.lines()
            .find_map(|l| l.strip_prefix(&format!("{name}:")))
            .unwrap_or_default()
            .trim()
            .to_owned()
    }

    fn hub(secret: Option<&str>) -> Webhooks {
        let dc = DcId::new("dc-a").unwrap();
        Webhooks::new(
            Arc::new(sprig_catalog::StaticLocator::new()),
            Arc::new(sprig_meta::MemStore::new(sprig_meta::Tier::Dc(dc.clone()))),
            Arc::new(sprig_meta::MemStore::new(sprig_meta::Tier::Global)),
            None,
            None,
            dc,
            secret.map(str::to_owned),
        )
    }

    fn check_signature(head: &str, body: &[u8], secret: &str) {
        let sig = header(head, "sprig-signature");
        let t: u64 = sig
            .strip_prefix("t=")
            .and_then(|r| r.split(',').next())
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            sig,
            sprig_sinks::target::webhook::sprig_signature(secret.as_bytes(), t, body)
        );
    }

    #[tokio::test]
    async fn the_platform_url_gets_signed_events() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            one_request(s, 200).await
        });
        let w = hub(Some("s3cret"));
        let body = Bytes::from_static(br#"{"event":"recommendation.new","id":"t1.1"}"#);
        let d = Dest::Platform(format!("http://127.0.0.1:{port}/hooks/sprig?x=1"));
        assert!(
            w.deliver(&d, body.clone(), "t1.1:new:7", "recommendation.new")
                .await
        );
        let (head, got) = server.await.unwrap();
        assert!(head.starts_with("post /hooks/sprig?x=1 http/1.1"), "{head}");
        assert_eq!(got, body.to_vec());
        assert_eq!(header(&head, "sprig-event"), "recommendation.new");
        assert_eq!(header(&head, "sprig-idempotency-key"), "t1.1:new:7");
        check_signature(&head, &got, "s3cret");
    }

    #[tokio::test]
    async fn a_refusal_is_not_retried_and_a_bad_url_never_connects() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            one_request(s, 400).await
        });
        let w = hub(None);
        let d = Dest::Platform(format!("http://127.0.0.1:{port}/"));
        assert!(!w.deliver(&d, Bytes::from_static(b"{}"), "k", "e").await);
        let (head, _) = server.await.unwrap();
        assert!(
            header(&head, "sprig-signature").is_empty(),
            "no secret, no signature"
        );
        assert!(w.target(&Dest::Platform("ftp://x/".into())).await.is_err());
    }

    fn tls_server() -> (tokio_rustls::TlsAcceptor, String) {
        let ck = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()])
            .unwrap();
        let cert = ck.cert.der().clone();
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(ck.signing_key.serialize_der()),
        );
        let cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .unwrap();
        (
            tokio_rustls::TlsAcceptor::from(Arc::new(cfg)),
            ck.cert.pem(),
        )
    }

    #[tokio::test]
    async fn https_checks_the_certificate() {
        let (acceptor, ca) = tls_server();
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let a2 = acceptor.clone();
        let server = tokio::spawn(async move {
            // 1. The platform URL: the Mozilla roots don't know this CA.
            let (s, _) = l.accept().await.unwrap();
            assert!(a2.accept(s).await.is_err());
            // 2. A connection carrying the CA (`ca_pem`), as a tenant's
            //    webhook connection does: delivered, signed with its
            //    signing_secret.
            let (s, _) = l.accept().await.unwrap();
            let t = acceptor.accept(s).await.unwrap();
            one_request(t, 204).await
        });
        let w = hub(Some("platform-secret"));
        let url = format!("https://127.0.0.1:{port}/in");
        let e = w
            .target(&Dest::Platform(url.clone()))
            .await
            .unwrap()
            .post_event(Bytes::from_static(b"{}"), "k", "e")
            .await
            .unwrap_err();
        assert!(e.message.contains("TLS failed"), "{e}");
        let mut options = Map::new();
        options.insert("url".into(), json!(url));
        options.insert("ca_pem".into(), json!(ca));
        let conn = Conn {
            name: "hooks".into(),
            kind: ConnKind::Webhook,
            options,
            secrets: Arc::new(MemSecrets::of(&[("signing_secret", "tenant-secret")])),
            dest: Arc::new(Destinations::allow_list(&[&format!("127.0.0.1:{port}")])),
        };
        let t = WebhookTarget::for_events(conn, Opts::default(), "sprig_approvals").unwrap();
        let body = Bytes::from_static(br#"{"event":"recommendation.escalated"}"#);
        let o = t
            .post_event(body.clone(), "t9.3:escalated:1", "recommendation.escalated")
            .await
            .unwrap();
        assert_eq!(o, Outcome::Ok);
        let (head, got) = server.await.unwrap();
        assert_eq!(got, body.to_vec());
        assert_eq!(header(&head, "sprig-event"), "recommendation.escalated");
        check_signature(&head, &got, "tenant-secret");
        // Off the allowlist: refused before connecting.
        let mut options = Map::new();
        options.insert("url".into(), json!("https://10.0.0.1/x"));
        let conn = Conn {
            name: "hooks".into(),
            kind: ConnKind::Webhook,
            options,
            secrets: Arc::new(MemSecrets::of(&[("signing_secret", "s")])),
            dest: Arc::new(Destinations::allow_list(&["hooks.example"])),
        };
        let t = WebhookTarget::for_events(conn, Opts::default(), "sprig_approvals").unwrap();
        let e = t
            .post_event(Bytes::from_static(b"{}"), "k", "e")
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::SinkDestinationNotAllowed));
    }

    #[test]
    fn connection_names() {
        assert_eq!(
            ConnName::parse("shop.hooks").unwrap(),
            ConnName::Own {
                db: "shop".into(),
                name: "hooks".into()
            }
        );
        assert_eq!(
            ConnName::parse(" `shop`.`hooks` ").unwrap(),
            ConnName::Own {
                db: "shop".into(),
                name: "hooks".into()
            }
        );
        assert_eq!(
            ConnName::parse("platform:ops").unwrap(),
            ConnName::Platform("ops".into())
        );
        for bad in [
            "",
            "hooks",
            "shop.",
            ".hooks",
            "a.b.c",
            "platform:",
            "platform:a.b",
        ] {
            assert!(ConnName::parse(bad).is_err(), "{bad}");
        }
    }
}
