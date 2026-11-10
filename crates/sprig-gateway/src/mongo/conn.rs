//! One MongoDB connection (chapter 30, "Framing and compression",
//! "Handshake as a `mongos`", "SCRAM-SHA-256", "Dispatch"; build steps 1-4).

#![allow(clippy::too_many_lines)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use base64::Engine as _;
use bson::spec::BinarySubtype;
use bson::{Binary, Bson, DateTime, Document, RawDocumentBuf, doc};
use sprig_auth::ScramKind;
use sprig_auth::mysql::{LoginRequest, MysqlLogin};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mongo::Compressor;
use sprig_proto_mongo::codec::{self, FLAG_MORE_TO_COME, Frame, OpMsg, OpReply};
use sprig_proto_mongo::errors::MongoError;
use sprig_proto_mongo::scram::{Credential, ScramServer};
use sprig_proto_mysql::Persona;
use sprig_proto_mysql::auth::{AuthPath, AuthVerdict, LoginFlow, Outcome};
use tokio::io::AsyncWriteExt;

use super::Shared;
use super::sql::SqlSession;
use crate::frontend::{ClientConn, TlsInfo};
use crate::session::Principal;

/// Commands that run before authentication.
const PRE_AUTH: [&str; 10] = [
    "hello",
    "isMaster",
    "ismaster",
    "ping",
    "buildInfo",
    "buildinfo",
    "saslStart",
    "saslContinue",
    "logout",
    "endSessions",
];

/// Commands whose replies are never compressed.
const NEVER_COMPRESSED: [&str; 7] = [
    "hello",
    "isMaster",
    "ismaster",
    "saslStart",
    "saslContinue",
    "createUser",
    "updateUser",
];

/// A SCRAM conversation in progress.
pub(crate) struct PendingScram {
    server: ScramServer,
    login: Option<MysqlLogin>,
    real: bool,
    admitted: bool,
    skip_empty: bool,
    /// The proof matched; waiting for the client's empty round.
    finished: Option<Principal>,
}

/// One connection's state.
pub struct Conn {
    pub(crate) shared: Arc<Shared>,
    pub(crate) id: i32,
    pub(crate) peer: SocketAddr,
    pub(crate) listener: &'static str,
    pub(crate) generation: u64,
    pub(crate) tls: Option<TlsInfo>,
    pub(crate) principal: Option<Principal>,
    pub(crate) scram: Option<PendingScram>,
    pub(crate) client_meta: Option<Document>,
    pub(crate) compressors: Vec<Compressor>,
    pub(crate) sql: Option<SqlSession>,
    /// The data center named by `appName` (`orders~mum1`).
    pub(crate) dc: Option<String>,
    /// A restore connection (`mongorestore`): indexes are `migrated`.
    pub(crate) restore: bool,
    pub(crate) hello_done: bool,
    /// A fail point closes the connection without a reply.
    pub(crate) close_now: bool,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Conn")
            .field("id", &self.id)
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

fn io(e: &std::io::Error) -> SprigError {
    SprigError::new(SprigCode::BackendLost, format!("mongo client i/o: {e}"))
}

fn codec_err(e: &codec::CodecError) -> SprigError {
    SprigError::new(SprigCode::BackendProtocol, format!("mongo framing: {e}"))
}

/// Serves `conn` until it closes.
///
/// # Errors
/// Framing and I/O errors, which close the connection.
pub async fn serve(shared: Arc<Shared>, mut conn: ClientConn) -> Result<()> {
    let id = shared.conn_ids.fetch_add(1, Ordering::SeqCst);
    metrics::gauge!("sprig_mongo_connections", "dc" => "").increment(1.0);
    let mut st = Conn {
        shared,
        id,
        peer: conn.peer,
        listener: conn.listener.name,
        generation: conn.generation,
        tls: conn.tls_info(),
        principal: None,
        scram: None,
        client_meta: None,
        compressors: Vec::new(),
        sql: None,
        dc: None,
        restore: false,
        hello_done: false,
        close_now: false,
    };
    let close = conn.close_soon.clone();
    let mut next_id = 1i32;
    let r: Result<()> = async {
        loop {
            let msg = tokio::select! {
                m = codec::read_message(&mut conn.stream) => m.map_err(|e| codec_err(&e))?,
                () = close.cancelled() => return Ok(()),
            };
            let Some(msg) = msg else { return Ok(()) };
            let (h, frame, comp) = codec::decode(&msg).map_err(|e| codec_err(&e))?;
            let (reply, used) = match frame {
                Frame::Query(q) => {
                    let cmd = q
                        .query
                        .to_document()
                        .map_err(|e| codec_err(&codec::CodecError::Malformed(e.to_string())))?;
                    let name = cmd.keys().next().cloned().unwrap_or_default();
                    if !q.full_collection_name.ends_with(".$cmd")
                        || !matches!(name.as_str(), "hello" | "isMaster" | "ismaster")
                    {
                        return Err(SprigError::new(
                            SprigCode::BackendProtocol,
                            "OP_QUERY is accepted only for the handshake",
                        ));
                    }
                    let body = match st.hello(&cmd, true).await {
                        Ok(d) => d,
                        Err(e) => e.to_doc(),
                    };
                    let raw = RawDocumentBuf::from_document(&body)
                        .map_err(|e| SprigError::internal(e.to_string()))?;
                    (
                        Some(Frame::Reply(OpReply {
                            flags: 8,
                            cursor_id: 0,
                            starting_from: 0,
                            docs: vec![raw],
                        })),
                        None,
                    )
                }
                Frame::Msg(m) => {
                    let more = m.flags & FLAG_MORE_TO_COME != 0;
                    let (name, body) = st.dispatch(m).await;
                    if st.close_now {
                        return Ok(());
                    }
                    let comp = comp.filter(|_| !NEVER_COMPRESSED.contains(&name.as_str()));
                    if more {
                        (None, None)
                    } else {
                        let raw = RawDocumentBuf::from_document(&body).unwrap_or_else(|e| {
                            RawDocumentBuf::from_document(
                                &MongoError::new(code::INTERNAL_ERROR, e.to_string()).to_doc(),
                            )
                            .unwrap_or_default()
                        });
                        (
                            Some(Frame::Msg(OpMsg {
                                flags: 0,
                                body: raw,
                                sequences: Vec::new(),
                            })),
                            comp,
                        )
                    }
                }
                Frame::Reply(_) => {
                    return Err(SprigError::new(
                        SprigCode::BackendProtocol,
                        "OP_REPLY from a client",
                    ));
                }
            };
            if let Some(f) = reply {
                let bytes =
                    codec::encode(next_id, h.request_id, &f, used).map_err(|e| codec_err(&e))?;
                next_id = next_id.wrapping_add(1);
                conn.stream.write_all(&bytes).await.map_err(|e| io(&e))?;
                conn.stream.flush().await.map_err(|e| io(&e))?;
            }
        }
    }
    .await;
    if let Some(s) = st.sql.take() {
        s.close().await;
    }
    metrics::gauge!("sprig_mongo_connections", "dc" => "").decrement(1.0);
    r
}

impl Conn {
    /// `hello` / `isMaster` (legacy when `legacy`).
    pub(crate) async fn hello(
        &mut self,
        cmd: &Document,
        legacy: bool,
    ) -> std::result::Result<Document, MongoError> {
        let cfg = self.shared.settings();
        if self.shared.d.draining.load(Ordering::SeqCst) {
            return Err(MongoError::new(
                code::SHUTDOWN_IN_PROGRESS,
                "The gateway is draining; connect to another gateway",
            )
            .label("RetryableWriteError"));
        }
        // Awaitable hello: wait until the topology changes (a drain) or the
        // time passes.
        if cmd.contains_key("topologyVersion")
            && let Some(ms) = cmd
                .get("maxAwaitTimeMS")
                .and_then(sprig_doc::filter::as_i64)
        {
            let wait = Duration::from_millis(u64::try_from(ms.clamp(0, 10_000)).unwrap_or(0));
            let deadline = tokio::time::Instant::now() + wait;
            while tokio::time::Instant::now() < deadline {
                if self.shared.d.draining.load(Ordering::SeqCst) {
                    return Err(MongoError::new(
                        code::SHUTDOWN_IN_PROGRESS,
                        "The gateway is draining",
                    )
                    .label("RetryableWriteError"));
                }
                tokio::time::sleep(
                    Duration::from_millis(250).min(deadline - tokio::time::Instant::now()),
                )
                .await;
            }
        }
        if let Ok(client) = cmd.get_document("client") {
            if self.client_meta.is_some() && self.hello_done {
                return Err(MongoError::new(
                    code::CLIENT_METADATA_CANNOT_BE_MUTATED,
                    "The client metadata document may only be sent in the first hello",
                ));
            }
            let app = client
                .get_document("application")
                .ok()
                .and_then(|a| a.get_str("name").ok())
                .unwrap_or("");
            if let Some((_, dc)) = app.rsplit_once('~') {
                self.dc = Some(dc.to_owned());
            }
            if app.eq_ignore_ascii_case("mongorestore") {
                self.restore = true;
            }
            self.client_meta = Some(client.clone());
        }
        if !self.hello_done
            && let Ok(offered) = cmd.get_array("compression")
        {
            {
                let names: Vec<&str> = offered.iter().filter_map(Bson::as_str).collect();
                self.compressors = cfg
                    .compressors
                    .iter()
                    .copied()
                    .filter(|c| names.contains(&c.name()))
                    .collect();
            }
        }
        self.hello_done = true;
        let mut d = Document::new();
        if legacy || !cmd.contains_key("hello") {
            d.insert("ismaster", true);
            if cmd.get_bool("helloOk").unwrap_or(false) {
                d.insert("helloOk", true);
            }
        } else {
            d.insert("isWritablePrimary", true);
        }
        d.insert("msg", "isdbgrid");
        d.insert(
            "topologyVersion",
            doc! {"processId": self.shared.process_id, "counter": 0i64},
        );
        d.insert("maxBsonObjectSize", 16_777_216);
        d.insert("maxMessageSizeBytes", 48_000_000);
        d.insert("maxWriteBatchSize", 100_000);
        d.insert("localTime", DateTime::now());
        d.insert("logicalSessionTimeoutMinutes", cfg.session_timeout_minutes);
        d.insert("connectionId", self.id);
        d.insert("minWireVersion", 0);
        d.insert("maxWireVersion", cfg.max_wire_version);
        d.insert("readOnly", false);
        if !self.compressors.is_empty() {
            d.insert(
                "compression",
                self.compressors
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
            );
        }
        if cmd.get_bool("loadBalanced").unwrap_or(false) {
            d.insert("serviceId", self.shared.process_id);
        }
        if cmd.contains_key("saslSupportedMechs") {
            d.insert("saslSupportedMechs", vec!["SCRAM-SHA-256"]);
        }
        if let Ok(spec) = cmd.get_document("speculativeAuthenticate")
            && spec.get_str("mechanism") == Ok("SCRAM-SHA-256")
            && let Ok(r) = self.sasl_start(spec).await
        {
            d.insert("speculativeAuthenticate", r);
        }
        d.insert("ok", 1.0);
        Ok(d)
    }

    fn payload(cmd: &Document) -> std::result::Result<Vec<u8>, MongoError> {
        match cmd.get("payload") {
            Some(Bson::Binary(b)) => Ok(b.bytes.clone()),
            Some(Bson::String(s)) => base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_err(|_| MongoError::new(code::BAD_VALUE, "payload is not base64")),
            _ => Err(MongoError::new(
                code::BAD_VALUE,
                "saslStart needs a payload",
            )),
        }
    }

    fn bin(b: Vec<u8>) -> Bson {
        Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: b,
        })
    }

    fn auth_failed() -> MongoError {
        MongoError::new(code::AUTHENTICATION_FAILED, "Authentication failed.")
    }

    /// `saslStart`: steps 1-4 of the exchange.
    pub(crate) async fn sasl_start(
        &mut self,
        cmd: &Document,
    ) -> std::result::Result<Document, MongoError> {
        if self.principal.is_some() {
            return Err(MongoError::new(
                code::ILLEGAL_OPERATION,
                "Each connection can be authenticated as one user only; a second user on the connection is refused",
            ));
        }
        let mech = cmd.get_str("mechanism").unwrap_or("");
        if mech != "SCRAM-SHA-256" {
            return Err(MongoError::new(
                334,
                format!("Received authentication for mechanism {mech} which is not enabled"),
            ));
        }
        let skip_empty = cmd
            .get_document("options")
            .ok()
            .and_then(|o| o.get_bool("skipEmptyExchange").ok())
            .unwrap_or(false);
        let mut server = ScramServer::new();
        let user = server
            .client_first(&Self::payload(cmd)?)
            .map_err(|e| MongoError::new(code::AUTHENTICATION_FAILED, e.to_string()))?;
        let svc = &self.shared.d.login;
        let admitted = svc.admit(self.peer.ip()).is_ok();
        let login = svc
            .start_mongo(LoginRequest {
                persona: Persona::MySql84,
                login_name: user.as_bytes().to_vec(),
                sni: self.tls.as_ref().and_then(|t| t.sni.clone()),
                client_ip: self.peer.ip(),
                tls: self.tls.is_some(),
                client_cert: self.tls.as_ref().is_some_and(|t| t.client_cert),
                can_handle_expired_password: false,
            })
            .await;
        let real = login
            .scram_credential(ScramKind::Sha256)
            .filter(|_| admitted)
            .map(|(v, sec)| Credential {
                salt: v.salt().to_vec(),
                iterations: v.iterations(),
                stored_key: v.stored_key().to_vec(),
                server_key: v.server_key().to_vec(),
                secondary: sec.map(|s| (s.stored_key().to_vec(), s.server_key().to_vec())),
            });
        let seed = format!(
            "{}/{user}",
            self.tls
                .as_ref()
                .and_then(|t| t.sni.as_deref())
                .unwrap_or("")
        );
        let cred = real.clone().unwrap_or_else(|| {
            Credential::decoy(
                &self.shared.decoy_secret,
                &seed,
                sprig_auth::scram::DEFAULT_SCRAM_ITERATIONS,
            )
        });
        let first = server.server_first(cred);
        self.scram = Some(PendingScram {
            server,
            login: Some(login),
            real: real.is_some(),
            admitted,
            skip_empty,
            finished: None,
        });
        Ok(doc! {"conversationId": 1, "done": false, "payload": Self::bin(first), "ok": 1.0})
    }

    /// `saslContinue`: steps 5-9.
    pub(crate) async fn sasl_continue(
        &mut self,
        cmd: &Document,
    ) -> std::result::Result<Document, MongoError> {
        let Some(mut p) = self.scram.take() else {
            return Err(MongoError::new(
                code::PROTOCOL_ERROR_OR_AUTH,
                "No SASL session state found",
            ));
        };
        if let Some(principal) = p.finished.take() {
            // The empty round of an exchange without skipEmptyExchange.
            self.principal = Some(principal);
            metrics::counter!("sprig_mongo_auth_total", "mechanism" => "SCRAM-SHA-256", "result" => "ok").increment(1);
            return Ok(
                doc! {"conversationId": 1, "done": true, "payload": Self::bin(Vec::new()), "ok": 1.0},
            );
        }
        let verified = p.server.client_final(&Self::payload(cmd)?);
        let mut login = p.login.take().ok_or_else(Self::auth_failed)?;
        let outcome = if verified.is_ok() && p.real && p.admitted {
            Outcome::Verified
        } else {
            Outcome::WrongCredentials
        };
        login.proof_presented();
        let verdict = login.decide(outcome, AuthPath::Full).await;
        let principal = match (&verdict, login.principal()) {
            (AuthVerdict::Accept, Some(pr)) if pr.tenant.is_some() => pr.clone(),
            _ => {
                metrics::counter!("sprig_mongo_auth_total", "mechanism" => "SCRAM-SHA-256", "result" => "failed").increment(1);
                return Err(Self::auth_failed());
            }
        };
        let v = verified.map_err(|_| Self::auth_failed())?;
        if p.skip_empty {
            self.principal = Some(principal);
            metrics::counter!("sprig_mongo_auth_total", "mechanism" => "SCRAM-SHA-256", "result" => "ok").increment(1);
            Ok(doc! {"conversationId": 1, "done": true, "payload": Self::bin(v), "ok": 1.0})
        } else {
            p.finished = Some(principal);
            self.scram = Some(p);
            Ok(doc! {"conversationId": 1, "done": false, "payload": Self::bin(v), "ok": 1.0})
        }
    }

    /// The connection's SQL session, opened on first use.
    pub(crate) fn sql(&mut self) -> std::result::Result<&mut SqlSession, MongoError> {
        if self.sql.is_none() {
            let p = self
                .principal
                .as_ref()
                .ok_or_else(|| MongoError::new(code::UNAUTHORIZED, "not authenticated"))?;
            let s = SqlSession::open(
                &self.shared.d.sessions,
                &self.shared.d.executor,
                p,
                self.listener,
                self.generation,
                self.peer,
                self.tls.clone(),
                64 * 1024 * 1024,
            )
            .map_err(|e| MongoError::new(code::INTERNAL_ERROR, e.message))?;
            self.sql = Some(s);
        }
        self.sql
            .as_mut()
            .ok_or_else(|| MongoError::new(code::INTERNAL_ERROR, "no session"))
    }

    /// Dispatches one `OP_MSG`: returns the command name and the reply.
    pub(crate) async fn dispatch(&mut self, m: OpMsg) -> (String, Document) {
        let started = std::time::Instant::now();
        let mut body = match m.body.to_document() {
            Ok(d) => d,
            Err(e) => {
                return (
                    String::new(),
                    MongoError::new(code::BAD_VALUE, format!("invalid BSON: {e}")).to_doc(),
                );
            }
        };
        let name = body.keys().next().cloned().unwrap_or_default();
        // Documents in kind 1 sequences: check the raw BSON, then merge.
        for s in &m.sequences {
            for d in &s.docs {
                if let Err(e) = sprig_doc::json::check_raw(d) {
                    return (name, e.to_doc());
                }
            }
            let docs: Vec<Bson> = s
                .docs
                .iter()
                .filter_map(|d| d.to_document().ok())
                .map(Bson::Document)
                .collect();
            match body.get_mut(&s.identifier) {
                Some(Bson::Array(a)) => a.extend(docs),
                _ => {
                    body.insert(s.identifier.clone(), docs);
                }
            }
        }
        if name == "insert"
            && let Ok(rawdocs) = m.body.get_array("documents")
        {
            for d in rawdocs.into_iter().flatten() {
                if let bson::RawBsonRef::Document(raw) = d
                    && let Err(e) = sprig_doc::json::check_raw(raw)
                {
                    return (name, e.to_doc());
                }
            }
        }
        if self.principal.is_none() && !PRE_AUTH.contains(&name.as_str()) {
            return (
                name.clone(),
                MongoError::new(
                    code::UNAUTHORIZED,
                    format!("Command {name} requires authentication"),
                )
                .to_doc(),
            );
        }
        // Driver test suites' fail point (D18), when the operator allows it.
        let tenant = self
            .principal
            .as_ref()
            .and_then(|p| p.tenant)
            .map_or(0, |t| t.0);
        if name == "configureFailPoint" {
            let r = if self.shared.test_commands() && self.principal.is_some() {
                self.shared.failpoints.configure(tenant, &body)
            } else {
                Err(MongoError::new(
                    code::COMMAND_NOT_FOUND,
                    "no such command: 'configureFailPoint'",
                ))
            };
            return (name, r.unwrap_or_else(|e| e.to_doc()));
        }
        let injected = if self.principal.is_some() && self.shared.test_commands() {
            let app = self
                .client_meta
                .as_ref()
                .and_then(|m| m.get_document("application").ok())
                .and_then(|a| a.get_str("name").ok())
                .map(str::to_owned);
            self.shared.failpoints.check(tenant, &name, app.as_deref())
        } else {
            None
        };
        if let Some(i) = &injected {
            if let Some(b) = i.block {
                tokio::time::sleep(b).await;
            }
            if i.close {
                self.close_now = true;
                return (name, Document::new());
            }
            if let Some(c) = i.error_code {
                let mut e = MongoError::new(c, "Failing command via 'failCommand' failpoint");
                match &i.labels {
                    Some(l) => {
                        for x in l {
                            let known = [
                                "RetryableWriteError",
                                "TransientTransactionError",
                                "UnknownTransactionCommitResult",
                                "NoWritesPerformed",
                                "ResumableChangeStreamError",
                                "NonResumableChangeStreamError",
                                "SystemOverloadedError",
                                "RetryableError",
                            ];
                            if let Some(k) = known.iter().find(|k| **k == x.as_str()) {
                                e = e.label(k);
                            }
                        }
                    }
                    None => {
                        let retryable = super::retry::applies(&name, &body)
                            || matches!(name.as_str(), "commitTransaction" | "abortTransaction");
                        if retryable && super::retry::retryable_code(c) {
                            e = e.label("RetryableWriteError");
                        }
                    }
                }
                return (name, e.to_doc());
            }
        }
        // The other two fail points of the drivers' tests.
        let mut close_after = false;
        if self.principal.is_some() && self.shared.test_commands() {
            if name == "getMore"
                && let Some(c) = self
                    .shared
                    .failpoints
                    .named(tenant, "failGetMoreAfterCursorCheckout")
                    .and_then(|i| i.error_code)
            {
                return (
                    name,
                    MongoError::new(c, "Failing getMore via failGetMoreAfterCursorCheckout")
                        .to_doc(),
                );
            }
            if super::retry::applies(&name, &body)
                && let Some(i) = self
                    .shared
                    .failpoints
                    .named(tenant, "onPrimaryTransactionalWrite")
            {
                if let Some(c) = i.error_code {
                    let mut e =
                        MongoError::new(c, "Failing write via onPrimaryTransactionalWrite");
                    if super::retry::retryable_code(c) {
                        e = e.label("RetryableWriteError");
                    }
                    return (name, e.to_doc());
                }
                close_after = i.close;
            }
        }
        let r = match name.as_str() {
            "hello" | "isMaster" | "ismaster" => self.hello(&body, false).await,
            "saslStart" => self.sasl_start(&body).await,
            "saslContinue" => self.sasl_continue(&body).await,
            "logout" => {
                self.principal = None;
                if let Some(s) = self.sql.take() {
                    s.close().await;
                }
                Ok(doc! {"ok": 1.0})
            }
            _ => super::commands::run(self, &name, &body).await,
        };
        if close_after {
            // The write committed; the client never hears.
            self.close_now = true;
            return (name, Document::new());
        }
        let result = if r.is_ok() { "ok" } else { "error" };
        metrics::counter!("sprig_mongo_commands_total", "command" => name.clone(), "result" => result).increment(1);
        metrics::histogram!("sprig_mongo_command_duration_seconds", "command" => name.clone())
            .record(started.elapsed().as_secs_f64());
        let doc = match r {
            Ok(mut d) => {
                if !d.contains_key("ok") {
                    d.insert("ok", 1.0);
                }
                if let Some(w) = injected.and_then(|i| i.write_concern_error) {
                    d.insert("writeConcernError", w);
                }
                d
            }
            Err(e) => e.to_doc(),
        };
        (name, doc)
    }
}

/// Codes: the shared table plus `ProtocolError` (17).
mod code {
    pub(crate) use sprig_proto_mongo::errors::code::*;
    pub(crate) const PROTOCOL_ERROR_OR_AUTH: i32 = 17;
}
