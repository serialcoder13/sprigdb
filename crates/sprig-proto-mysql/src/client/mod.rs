//! The client side, for backend connections (chapter 05, build step 5):
//! [`MysqlClient`] connects to MariaDB (or MySQL), upgrades to TLS
//! (rustls, optionally with a client certificate), authenticates with
//! `caching_sha2_password` (fast path, and the full path over TLS) or
//! `mysql_native_password`, and negotiates `SESSION_TRACK`,
//! `DEPRECATE_EOF`, `MULTI_RESULTS`, `PS_MULTI_RESULTS`,
//! `MULTI_STATEMENTS`, and `LOCAL_FILES` only when asked.
//!
//! **Raw packets first.** The relay engine forwards backend packets to the
//! client without re-encoding them, so the core API streams the response
//! to a command as raw payloads, each classified by the client's protocol
//! state machine:
//!
//! ```text
//! client.send_command(&Command::Query(QueryAttrs::default(), sql)).await?;
//! while let Some(p) = client.next_packet().await? {
//!     match p.kind {
//!         PacketKind::ColumnDef => relay(rewrite_column_def(&p.payload)),
//!         PacketKind::Row => relay_raw(&p.payload),          // zero copy
//!         PacketKind::End(ref t) => relay(terminator_for_client(t)),
//!         PacketKind::Ok(ref ok) => relay(ok_for_client(ok)),
//!         PacketKind::Err(ref e) => relay(err_for_client(e)),
//!         PacketKind::LocalInfile => { /* send_infile_data / finish_infile */ }
//!         _ => relay_raw(&p.payload),
//!     }
//! }
//! ```
//!
//! [`next_packet`](MysqlClient::next_packet) returns `None` once the
//! response is complete, following `SERVER_MORE_RESULTS_EXISTS` across the
//! results of a multi-statement or `CALL`. OK packets and OK-form
//! terminators update the status flags and the [`SessionTracker`] (schema,
//! system variables, the last GTID, transaction state) as they pass; each
//! [`RespPacket`] carries the changes its packet reported.
//!
//! Typed helpers sit on top: [`query`](MysqlClient::query),
//! [`query_drop`](MysqlClient::query_drop),
//! [`query_first`](MysqlClient::query_first),
//! [`prepare`](MysqlClient::prepare), [`ping`](MysqlClient::ping),
//! [`reset_connection`](MysqlClient::reset_connection) and
//! [`quit`](MysqlClient::quit) (`COM_QUIT` on close).
//!
//! **Cancellation.** Dropping a future of this client mid-response leaves
//! it not [`is_idle`](MysqlClient::is_idle); such a connection must be
//! discarded, as must one that [`is_broken`](MysqlClient::is_broken).
//!
//! **No RSA.** Without TLS, the `caching_sha2_password` full path needs the
//! server's RSA key; this client has no RSA (decision D4), so it fails
//! with [`ClientError::Auth`] asking for TLS. Backends always use TLS
//! (chapter 06).

pub mod auth;
mod config;
mod error;
mod stream;
mod tracker;

#[cfg(test)]
mod tests;

pub use config::{ClientCert, ClientConfig, TlsMode, TlsOptions};
pub use error::{ClientError, ClientResult};
pub use stream::{ClientStream, ClientTlsInfo, Io};
pub use tracker::{SessionTracker, TrackedChanges, decode_session_state};

use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _};
use rustls::pki_types::ServerName;
use tokio::io::AsyncWriteExt as _;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, FramedParts};
use zeroize::Zeroizing;

use crate::codec::{DEFAULT_ZLIB_LEVEL, Framing, Packet, PacketCodec};
use crate::command::{Command, com};
use crate::error::ErrPacket;
use crate::packets::{
    AuthMoreData, AuthSwitchRequest, Caps, EOF_HEADER, ERR_HEADER, EofPacket, HandshakeResponse,
    HandshakeV10, OK_HEADER, OkPacket, SslRequest, StatusFlags,
};
use crate::resultset::{
    ColumnDef, PrepareOk, Response, RowPacket, Terminator, classify_response, classify_row,
    decode_text_row,
};

/// What the server said about itself in its greeting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInfo {
    /// Version string, such as `11.8.3-MariaDB-log`.
    pub version: String,
    /// The 32-bit connection id from the greeting. MariaDB's thread ids
    /// are 64-bit: read `CONNECTION_ID()` for the full value.
    pub connection_id: u32,
    /// Capabilities the server offered.
    pub caps: Caps,
    /// The server's default collation id.
    pub collation: u8,
    /// The auth plugin the greeting named.
    pub auth_plugin: String,
}

impl ServerInfo {
    /// Whether the server is MariaDB.
    #[must_use]
    pub fn is_mariadb(&self) -> bool {
        self.version.contains("MariaDB")
    }
}

/// How a packet of a response was classified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PacketKind {
    /// An OK packet (header 0x00) ending a statement.
    Ok(OkPacket),
    /// An ERR packet; the response ends.
    Err(ErrPacket),
    /// A `LOCAL INFILE` request; the file name is `payload[1..]`. Answer
    /// with [`MysqlClient::send_infile_data`] and
    /// [`MysqlClient::finish_infile`].
    LocalInfile,
    /// The column count that starts a resultset.
    ColumnCount(u64),
    /// A column definition (of a resultset, a `COM_FIELD_LIST` or a
    /// prepare).
    ColumnDef,
    /// The EOF after column definitions (without `DEPRECATE_EOF`).
    ColumnsEof,
    /// A text or binary row.
    Row,
    /// The terminator of rows, of a `COM_FIELD_LIST`, or of
    /// `COM_SET_OPTION`.
    End(Terminator),
    /// `COM_STMT_PREPARE` succeeded.
    PrepareOk(PrepareOk),
    /// A parameter definition of a prepare.
    ParamDef,
    /// The EOF after parameter definitions (without `DEPRECATE_EOF`).
    ParamsEof,
    /// The single packet answering `COM_STATISTICS`.
    Raw,
}

/// One packet of a response: its raw payload and its classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespPacket {
    /// What the packet is.
    pub kind: PacketKind,
    /// The payload as received, to relay without re-encoding.
    pub payload: Bytes,
    /// Session-state changes this packet reported (OK packets and OK-form
    /// terminators); empty otherwise.
    pub tracked: TrackedChanges,
}

impl RespPacket {
    /// Status flags of an OK, EOF or terminator packet.
    #[must_use]
    pub fn status(&self) -> Option<StatusFlags> {
        match &self.kind {
            PacketKind::Ok(ok) => Some(ok.status),
            PacketKind::End(t) => Some(t.status()),
            _ => None,
        }
    }

    /// Whether another result follows this statement-ending packet.
    #[must_use]
    pub fn more_results(&self) -> bool {
        self.status()
            .is_some_and(|s| s.contains(StatusFlags::MORE_RESULTS_EXISTS))
    }
}

/// One result of a statement, collected by the typed helpers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResultSet {
    /// Column definitions; empty for a statement that returned OK.
    pub columns: Vec<ColumnDef>,
    /// Row payloads, raw (text rows for `COM_QUERY`).
    pub rows: Vec<Bytes>,
    /// The OK packet, for a statement without a resultset, or the OK-form
    /// terminator under `DEPRECATE_EOF`.
    pub ok: Option<OkPacket>,
    /// Status flags of the final packet.
    pub status: StatusFlags,
    /// Warning count of the final packet.
    pub warnings: u16,
}

impl ResultSet {
    /// The rows decoded as text rows; `None` is SQL NULL.
    pub fn text_rows(&self) -> crate::ProtoResult<Vec<Vec<Option<Bytes>>>> {
        self.rows
            .iter()
            .map(|r| {
                decode_text_row(r, self.columns.len()).map(|vals| {
                    vals.into_iter()
                        .map(|v| v.map(|b| r.slice_ref(b)))
                        .collect()
                })
            })
            .collect()
    }
}

/// The result of [`MysqlClient::prepare`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Prepared {
    /// Statement id, column and parameter counts.
    pub ok: PrepareOk,
    /// Parameter definitions.
    pub params: Vec<ColumnDef>,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
}

/// Where the client is in the response to the current command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    Idle,
    /// OK, ERR, `LOCAL INFILE` or a column count.
    First,
    Columns(u64),
    ColumnsEof,
    Rows,
    InfileData,
    PrepareFirst,
    PrepareParams {
        left: u16,
        cols: u16,
    },
    PrepareParamsEof {
        cols: u16,
    },
    PrepareCols(u16),
    PrepareColsEof,
    FieldList,
    OkOrErr,
    Raw,
}

/// How the response to a command starts.
fn expect_for(code: u8) -> Expect {
    match code {
        com::QUERY | com::STMT_EXECUTE | com::PROCESS_INFO | com::STMT_BULK_EXECUTE => {
            Expect::First
        }
        com::STMT_FETCH => Expect::Rows,
        com::STMT_PREPARE => Expect::PrepareFirst,
        com::FIELD_LIST => Expect::FieldList,
        com::STATISTICS => Expect::Raw,
        com::QUIT | com::STMT_CLOSE | com::STMT_SEND_LONG_DATA => Expect::Idle,
        _ => Expect::OkOrErr,
    }
}

/// What follows a statement-ending packet with these status flags.
fn after_end(status: StatusFlags) -> Expect {
    if status.contains(StatusFlags::MORE_RESULTS_EXISTS) {
        Expect::First
    } else {
        Expect::Idle
    }
}

/// Largest `LOCAL INFILE` data packet sent; never 0xFFFFFF, whose empty
/// continuation the server would read as the end of the file.
const INFILE_CHUNK: usize = 64 * 1024;

/// A connection to a MariaDB or MySQL server. See the module docs.
pub struct MysqlClient {
    io: Framed<ClientStream, PacketCodec>,
    caps: Caps,
    server: ServerInfo,
    status: StatusFlags,
    warnings: u16,
    tracker: SessionTracker,
    pending: TrackedChanges,
    expect: Expect,
    broken: bool,
    tls: Option<ClientTlsInfo>,
}

impl std::fmt::Debug for MysqlClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlClient")
            .field("server", &self.server.version)
            .field("connection_id", &self.server.connection_id)
            .field("caps", &self.caps)
            .field("status", &self.status)
            .field("expect", &self.expect)
            .field("broken", &self.broken)
            .field("tls", &self.tls)
            .finish_non_exhaustive()
    }
}

impl MysqlClient {
    /// Connects to `addr` (`host:port`) and logs in, all within
    /// `cfg.connect_timeout`.
    ///
    /// # Errors
    /// [`ClientError::Connect`], [`ClientError::Timeout`], a TLS or
    /// authentication failure, or the server's ERR (1045 and the like).
    pub async fn connect(addr: &str, cfg: &ClientConfig) -> ClientResult<Self> {
        tokio::time::timeout(cfg.connect_timeout, async {
            // Development: a simulated DC's process dials through its proxy.
            let tcp = TcpStream::connect(&*sprig_common::net::dial(addr))
                .await
                .map_err(|source| ClientError::Connect {
                    addr: addr.to_owned(),
                    source,
                })?;
            let _ = tcp.set_nodelay(true);
            let host = host_of(addr);
            Self::handshake(ClientStream::Plain(tcp), host, cfg).await
        })
        .await
        .map_err(|_| ClientError::Timeout("connect, TLS and login"))?
    }

    /// Runs the connection phase over an open stream: greeting, optional
    /// TLS upgrade (only on [`ClientStream::Plain`]), handshake response
    /// and the auth exchange. `host` is the TLS server name unless
    /// `cfg.server_name` is set. No deadline: [`MysqlClient::connect`] adds
    /// one.
    ///
    /// # Errors
    /// As [`MysqlClient::connect`].
    #[allow(clippy::too_many_lines)] // one linear exchange, easier to follow whole
    pub async fn handshake(
        stream: ClientStream,
        host: &str,
        cfg: &ClientConfig,
    ) -> ClientResult<Self> {
        let mut io = Framed::new(stream, PacketCodec::new(cfg.max_packet));
        let greeting = read_packet(&mut io).await?;
        if greeting.payload.first() == Some(&ERR_HEADER) {
            return Err(ClientError::Server(ErrPacket::decode(&greeting.payload)?));
        }
        let hs = HandshakeV10::decode(&greeting.payload)?;
        if !hs.caps.contains(Caps::REQUIRED) {
            return Err(ClientError::Unexpected(
                "the server lacks 4.1 authentication".to_owned(),
            ));
        }
        let use_tls = match cfg.tls {
            TlsMode::Disabled => false,
            TlsMode::Preferred => hs.caps.contains(Caps::SSL),
            TlsMode::Required if hs.caps.contains(Caps::SSL) => true,
            TlsMode::Required => {
                return Err(ClientError::Tls("the server doesn't offer TLS".to_owned()));
            }
        };
        let mut want = Caps::LONG_PASSWORD
            | Caps::LONG_FLAG
            | Caps::PROTOCOL_41
            | Caps::TRANSACTIONS
            | Caps::SECURE_CONNECTION
            | Caps::PLUGIN_AUTH
            | Caps::PLUGIN_AUTH_LENENC_CLIENT_DATA
            | Caps::MULTI_RESULTS
            | Caps::PS_MULTI_RESULTS
            | Caps::SESSION_TRACK
            | Caps::DEPRECATE_EOF
            | Caps::CONNECT_ATTRS
            | cfg.extra_caps;
        want.set(Caps::MULTI_STATEMENTS, cfg.multi_statements);
        want.set(Caps::FOUND_ROWS, cfg.found_rows);
        want.set(Caps::LOCAL_FILES, cfg.local_files);
        want.set(Caps::CONNECT_WITH_DB, cfg.database.is_some());
        want.set(Caps::COMPRESS, cfg.compress);
        want.set(Caps::SSL, use_tls);
        // A MariaDB greeting clears CLIENT_MYSQL; so does the answer, and the
        // extended bits asked for travel in the filler.
        let caps = want & hs.caps;
        let max_packet = u32::try_from(cfg.max_packet).unwrap_or(u32::MAX);

        let mut tls_info = None;
        if use_tls {
            send(
                &mut io,
                SslRequest {
                    caps,
                    max_packet,
                    collation: cfg.collation,
                }
                .encode(),
            )
            .await?;
            io = upgrade_tls(io, cfg, host).await?;
            tls_info = io.get_ref().tls_info();
        }

        let password = cfg.password.as_bytes();
        let mut plugin = if hs.auth_plugin.is_empty() {
            auth::NATIVE.to_owned()
        } else {
            hs.auth_plugin.clone()
        };
        let mut nonce = auth::nonce_of(&hs.auth_data).to_vec();
        let first = if let Some(r) = auth::first_response(&plugin, password, &nonce, use_tls) {
            r
        } else {
            // Answer as native; the server switches us to the user's plugin.
            auth::NATIVE.clone_into(&mut plugin);
            auth::native_scramble(password, &nonce)
        };
        let response = HandshakeResponse {
            caps,
            max_packet,
            collation: cfg.collation,
            user: cfg.user.as_bytes().to_vec(),
            auth_response: Bytes::from(first),
            database: cfg.database.as_ref().map(|d| d.as_bytes().to_vec()),
            plugin: Some(plugin.clone()),
            attrs: if caps.contains(Caps::CONNECT_ATTRS) {
                cfg.attrs.clone()
            } else {
                Vec::new()
            },
            zstd_level: None,
        };
        send(&mut io, response.encode()).await?;

        let ok = loop {
            let p = read_packet(&mut io).await?;
            match p.payload.first() {
                Some(&OK_HEADER) => break OkPacket::decode(&p.payload, caps)?,
                Some(&ERR_HEADER) => {
                    return Err(ClientError::Server(ErrPacket::decode(&p.payload)?));
                }
                Some(&EOF_HEADER) => {
                    let sw = AuthSwitchRequest::decode(&p.payload)?;
                    if sw.plugin.is_empty() {
                        return Err(ClientError::Auth(
                            "the server asked for the pre-4.1 password scheme".to_owned(),
                        ));
                    }
                    plugin = sw.plugin;
                    nonce = auth::nonce_of(&sw.data).to_vec();
                    let r = auth::first_response(&plugin, password, &nonce, use_tls).ok_or_else(
                        || {
                            ClientError::Auth(format!(
                                "plugin {plugin} is not supported{}",
                                if plugin == auth::CLEAR {
                                    " without TLS"
                                } else {
                                    ""
                                }
                            ))
                        },
                    )?;
                    send(&mut io, Bytes::from(r)).await?;
                }
                Some(&crate::packets::AUTH_MORE_DATA_HEADER) if plugin == auth::CACHING_SHA2 => {
                    let more = AuthMoreData::decode(&p.payload)?;
                    match more.data.first() {
                        Some(&auth::FAST_AUTH_SUCCESS) => {}
                        Some(&auth::PERFORM_FULL_AUTH) if use_tls => {
                            let mut clear = Zeroizing::new(password.to_vec());
                            clear.push(0);
                            send(&mut io, Bytes::copy_from_slice(&clear)).await?;
                        }
                        Some(&auth::PERFORM_FULL_AUTH) => {
                            return Err(ClientError::Auth(
                                "caching_sha2_password full authentication needs TLS \
                                 (this client has no RSA key exchange)"
                                    .to_owned(),
                            ));
                        }
                        _ => {
                            return Err(ClientError::Auth(
                                "unexpected caching_sha2_password data from the server".to_owned(),
                            ));
                        }
                    }
                }
                other => {
                    return Err(ClientError::Unexpected(format!(
                        "packet {other:02x?} during authentication with {plugin}"
                    )));
                }
            }
        };

        if caps.contains(Caps::COMPRESS) {
            io.codec_mut().set_framing(Framing::Zlib {
                level: DEFAULT_ZLIB_LEVEL,
            });
        }
        let mut client = Self {
            io,
            caps,
            server: ServerInfo {
                version: hs.server_version,
                connection_id: hs.connection_id,
                caps: hs.caps,
                collation: hs.collation,
                auth_plugin: hs.auth_plugin,
            },
            status: ok.status,
            warnings: ok.warnings,
            tracker: SessionTracker::new(),
            pending: TrackedChanges::default(),
            expect: Expect::Idle,
            broken: false,
            tls: tls_info,
        };
        client.on_ok(&ok)?;
        Ok(client)
    }

    /// Effective capabilities: what the client asked for and the server
    /// offered.
    #[must_use]
    pub fn caps(&self) -> Caps {
        self.caps
    }

    /// The greeting's facts.
    #[must_use]
    pub fn server(&self) -> &ServerInfo {
        &self.server
    }

    /// The 32-bit connection id from the greeting.
    #[must_use]
    pub fn connection_id(&self) -> u32 {
        self.server.connection_id
    }

    /// Status flags of the last OK, EOF or terminator.
    #[must_use]
    pub fn status(&self) -> StatusFlags {
        self.status
    }

    /// Whether the server reported an open transaction.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.status.contains(StatusFlags::IN_TRANS)
    }

    /// Warning count of the last OK, EOF or terminator.
    #[must_use]
    pub fn warnings(&self) -> u16 {
        self.warnings
    }

    /// Everything session tracking has reported on this connection.
    #[must_use]
    pub fn tracker(&self) -> &SessionTracker {
        &self.tracker
    }

    /// The changes reported since the current command was sent.
    #[must_use]
    pub fn changes(&self) -> &TrackedChanges {
        &self.pending
    }

    /// Takes the changes reported since the current command was sent (or
    /// the last take).
    pub fn take_changes(&mut self) -> TrackedChanges {
        std::mem::take(&mut self.pending)
    }

    /// Negotiated TLS parameters, or `None` for plaintext.
    #[must_use]
    pub fn tls_info(&self) -> Option<&ClientTlsInfo> {
        self.tls.as_ref()
    }

    /// Whether no response is pending, so a command may be sent.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.expect == Expect::Idle && !self.broken
    }

    /// Whether the connection failed and must be discarded.
    #[must_use]
    pub fn is_broken(&self) -> bool {
        self.broken
    }

    /// Whether the server has closed this idle connection, or sent
    /// something nobody asked for (a TLS alert, MariaDB's ERR 1927 before
    /// it closes a killed connection): a poll of the stream that never
    /// waits. Such a connection is marked broken, so a command sent on it
    /// could never have reached the server. A connection with a response
    /// pending isn't checked (`false`).
    pub fn closed_while_idle(&mut self) -> bool {
        if self.broken {
            return true;
        }
        if self.expect != Expect::Idle {
            return false;
        }
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match self.io.poll_next_unpin(&mut cx) {
            std::task::Poll::Pending => false,
            std::task::Poll::Ready(_) => {
                self.broken = true;
                true
            }
        }
    }

    /// Sends a command. The response, if the command has one, is read with
    /// [`next_packet`](Self::next_packet).
    ///
    /// # Errors
    /// [`ClientError::Busy`] while a response is pending; I/O errors.
    pub async fn send_command(&mut self, cmd: &Command) -> ClientResult<()> {
        let payload = cmd.encode(self.caps)?;
        self.send_command_payload(payload).await
    }

    /// Sends an encoded command payload (its first byte is the command
    /// code), as received from a client or built by the caller.
    ///
    /// # Errors
    /// As [`send_command`](Self::send_command).
    pub async fn send_command_payload(&mut self, payload: Bytes) -> ClientResult<()> {
        if self.broken {
            return Err(ClientError::Closed);
        }
        if self.expect != Expect::Idle {
            return Err(ClientError::Busy("a response is still pending"));
        }
        let code = *payload
            .first()
            .ok_or(ClientError::Busy("empty command payload"))?;
        self.io.codec_mut().reset_seq();
        self.pending = TrackedChanges::default();
        self.expect = expect_for(code);
        self.write(payload).await
    }

    /// Sends `sql` as `COM_QUERY` (without query attributes).
    ///
    /// # Errors
    /// As [`send_command`](Self::send_command).
    pub async fn send_query(&mut self, sql: &[u8]) -> ClientResult<()> {
        let mut payload = Vec::with_capacity(1 + sql.len());
        payload.push(com::QUERY);
        payload.extend_from_slice(sql);
        self.send_command_payload(Bytes::from(payload)).await
    }

    /// The next packet of the response, or `None` once it is complete.
    ///
    /// # Errors
    /// I/O and framing errors, and packets that don't fit the protocol
    /// state ([`ClientError::Unexpected`]); each breaks the connection.
    pub async fn next_packet(&mut self) -> ClientResult<Option<RespPacket>> {
        if self.expect == Expect::Idle {
            return Ok(None);
        }
        if self.expect == Expect::InfileData {
            return Err(ClientError::Busy("send the LOCAL INFILE data first"));
        }
        if self.broken {
            return Err(ClientError::Closed);
        }
        let r = self.next_inner().await;
        if let Err(e) = &r
            && e.is_connection_lost()
        {
            self.broken = true;
        }
        r.map(Some)
    }

    #[allow(clippy::too_many_lines)] // one arm per protocol state
    async fn next_inner(&mut self) -> ClientResult<RespPacket> {
        let payload = self.read().await?.payload;
        let caps = self.caps;
        let deprecate_eof = caps.contains(Caps::DEPRECATE_EOF);
        let mut tracked = TrackedChanges::default();
        let first = payload.first().copied();
        let kind = match self.expect {
            Expect::First => match classify_response(&payload, caps)? {
                Response::Ok(ok) => {
                    tracked = self.on_ok(&ok)?;
                    self.expect = after_end(ok.status);
                    PacketKind::Ok(ok)
                }
                Response::Err(e) => self.on_err(e),
                Response::LocalInfile(_) => {
                    self.expect = Expect::InfileData;
                    PacketKind::LocalInfile
                }
                Response::ResultSet { columns } => {
                    self.expect = Expect::Columns(columns);
                    PacketKind::ColumnCount(columns)
                }
            },
            Expect::Columns(_) | Expect::PrepareCols(_) if first == Some(ERR_HEADER) => {
                self.on_err(ErrPacket::decode(&payload)?)
            }
            Expect::Columns(left) => {
                self.expect = match left {
                    0 | 1 if deprecate_eof => Expect::Rows,
                    0 | 1 => Expect::ColumnsEof,
                    n => Expect::Columns(n - 1),
                };
                PacketKind::ColumnDef
            }
            Expect::ColumnsEof => {
                let eof = self.expect_eof(&payload)?;
                self.expect = if eof.status.contains(StatusFlags::CURSOR_EXISTS) {
                    after_end(eof.status)
                } else {
                    Expect::Rows
                };
                PacketKind::ColumnsEof
            }
            Expect::Rows => match classify_row(&payload, caps)? {
                RowPacket::Row => PacketKind::Row,
                RowPacket::End(t) => {
                    tracked = self.on_terminator(&t)?;
                    self.expect = after_end(t.status());
                    PacketKind::End(t)
                }
                RowPacket::Err(e) => self.on_err(e),
            },
            Expect::PrepareFirst => match first {
                Some(ERR_HEADER) => self.on_err(ErrPacket::decode(&payload)?),
                Some(OK_HEADER) => {
                    let ok = PrepareOk::decode(&payload)?;
                    self.warnings = ok.warnings;
                    self.expect = match (ok.params, ok.columns) {
                        (0, 0) => Expect::Idle,
                        (0, c) => Expect::PrepareCols(c),
                        (p, c) => Expect::PrepareParams { left: p, cols: c },
                    };
                    PacketKind::PrepareOk(ok)
                }
                _ => return Err(self.unexpected(&payload, "a prepare response")),
            },
            Expect::PrepareParams { left, cols } => {
                self.expect = match (left, cols) {
                    (0 | 1, 0) if deprecate_eof => Expect::Idle,
                    (0 | 1, c) if deprecate_eof => Expect::PrepareCols(c),
                    (0 | 1, c) => Expect::PrepareParamsEof { cols: c },
                    (n, c) => Expect::PrepareParams {
                        left: n - 1,
                        cols: c,
                    },
                };
                PacketKind::ParamDef
            }
            Expect::PrepareParamsEof { cols } => {
                self.expect_eof(&payload)?;
                self.expect = if cols == 0 {
                    Expect::Idle
                } else {
                    Expect::PrepareCols(cols)
                };
                PacketKind::ParamsEof
            }
            Expect::PrepareCols(left) => {
                self.expect = match left {
                    0 | 1 if deprecate_eof => Expect::Idle,
                    0 | 1 => Expect::PrepareColsEof,
                    n => Expect::PrepareCols(n - 1),
                };
                PacketKind::ColumnDef
            }
            Expect::PrepareColsEof => {
                self.expect_eof(&payload)?;
                self.expect = Expect::Idle;
                PacketKind::ColumnsEof
            }
            Expect::FieldList => match classify_row(&payload, caps)? {
                RowPacket::Row => PacketKind::ColumnDef,
                RowPacket::End(t) => {
                    tracked = self.on_terminator(&t)?;
                    self.expect = Expect::Idle;
                    PacketKind::End(t)
                }
                RowPacket::Err(e) => self.on_err(e),
            },
            Expect::OkOrErr => match first {
                Some(OK_HEADER) => {
                    let ok = OkPacket::decode(&payload, caps)?;
                    tracked = self.on_ok(&ok)?;
                    self.expect = Expect::Idle;
                    PacketKind::Ok(ok)
                }
                Some(ERR_HEADER) => self.on_err(ErrPacket::decode(&payload)?),
                Some(EOF_HEADER) => {
                    let t = Terminator::decode(&payload, caps)?;
                    tracked = self.on_terminator(&t)?;
                    self.expect = Expect::Idle;
                    PacketKind::End(t)
                }
                _ => return Err(self.unexpected(&payload, "OK or ERR")),
            },
            Expect::Raw => {
                if first == Some(ERR_HEADER) {
                    self.on_err(ErrPacket::decode(&payload)?)
                } else {
                    self.expect = Expect::Idle;
                    PacketKind::Raw
                }
            }
            Expect::Idle | Expect::InfileData => {
                return Err(ClientError::Busy("no response is expected"));
            }
        };
        Ok(RespPacket {
            kind,
            payload,
            tracked,
        })
    }

    fn unexpected(&self, payload: &[u8], wanted: &str) -> ClientError {
        let head: Vec<u8> = payload.iter().take(32).copied().collect();
        ClientError::Unexpected(format!(
            "wanted {wanted} in state {:?}, got {head:02x?}",
            self.expect
        ))
    }

    fn expect_eof(&mut self, payload: &[u8]) -> ClientResult<EofPacket> {
        if payload.first() != Some(&EOF_HEADER) || payload.len() >= crate::codec::MAX_CHUNK {
            return Err(self.unexpected(payload, "EOF"));
        }
        let eof = EofPacket::decode(payload, self.caps)?;
        self.status = eof.status;
        self.warnings = eof.warnings;
        Ok(eof)
    }

    fn on_err(&mut self, e: ErrPacket) -> PacketKind {
        self.expect = Expect::Idle;
        PacketKind::Err(e)
    }

    fn on_ok(&mut self, ok: &OkPacket) -> ClientResult<TrackedChanges> {
        self.status = ok.status;
        self.warnings = ok.warnings;
        if ok.session_state.is_empty() {
            return Ok(TrackedChanges::default());
        }
        let changes = self
            .tracker
            .apply(&tracker::decode_session_state(&ok.session_state)?);
        self.pending.merge(changes.clone());
        Ok(changes)
    }

    fn on_terminator(&mut self, t: &Terminator) -> ClientResult<TrackedChanges> {
        match t {
            Terminator::Ok(ok) => self.on_ok(ok),
            Terminator::Eof(e) => {
                self.status = e.status;
                self.warnings = e.warnings;
                Ok(TrackedChanges::default())
            }
        }
    }

    /// Sends a chunk of the file asked for by a `LOCAL INFILE` request.
    ///
    /// # Errors
    /// [`ClientError::Busy`] if no request is pending; I/O errors.
    pub async fn send_infile_data(&mut self, data: &[u8]) -> ClientResult<()> {
        if self.expect != Expect::InfileData {
            return Err(ClientError::Busy("no LOCAL INFILE request is pending"));
        }
        for chunk in data.chunks(INFILE_CHUNK) {
            self.write(Bytes::copy_from_slice(chunk)).await?;
        }
        Ok(())
    }

    /// Ends the file (or declines the request when no data was sent); the
    /// server's OK or ERR follows through [`next_packet`](Self::next_packet).
    ///
    /// # Errors
    /// As [`send_infile_data`](Self::send_infile_data).
    pub async fn finish_infile(&mut self) -> ClientResult<()> {
        if self.expect != Expect::InfileData {
            return Err(ClientError::Busy("no LOCAL INFILE request is pending"));
        }
        self.expect = Expect::First;
        self.write(Bytes::new()).await
    }

    /// Reads and discards the rest of the response.
    ///
    /// # Errors
    /// As [`next_packet`](Self::next_packet).
    pub async fn drain(&mut self) -> ClientResult<()> {
        loop {
            if self.expect == Expect::InfileData {
                self.finish_infile().await?;
            }
            if self.next_packet().await?.is_none() {
                return Ok(());
            }
        }
    }

    /// Runs `sql` (one or more statements) and collects every result.
    ///
    /// # Errors
    /// [`ClientError::Server`] for the first ERR (earlier results are
    /// lost); connection errors.
    pub async fn query(&mut self, sql: &str) -> ClientResult<Vec<ResultSet>> {
        self.send_query(sql.as_bytes()).await?;
        self.collect().await
    }

    /// Runs `sql` and discards its results.
    ///
    /// # Errors
    /// As [`query`](Self::query).
    pub async fn query_drop(&mut self, sql: &str) -> ClientResult<()> {
        self.query(sql).await.map(drop)
    }

    /// The first row of the first resultset of `sql`, decoded as text.
    ///
    /// # Errors
    /// As [`query`](Self::query).
    pub async fn query_first(&mut self, sql: &str) -> ClientResult<Option<Vec<Option<Bytes>>>> {
        let results = self.query(sql).await?;
        let Some(rs) = results.into_iter().find(|r| !r.columns.is_empty()) else {
            return Ok(None);
        };
        Ok(rs.text_rows()?.into_iter().next())
    }

    /// Collects the response to the current command.
    ///
    /// # Errors
    /// As [`query`](Self::query).
    pub async fn collect(&mut self) -> ClientResult<Vec<ResultSet>> {
        let mut out = Vec::new();
        let mut cur: Option<ResultSet> = None;
        let mut err = None;
        while let Some(p) = self.next_packet().await? {
            match p.kind {
                PacketKind::Ok(ok) => out.push(ResultSet {
                    status: ok.status,
                    warnings: ok.warnings,
                    ok: Some(ok),
                    ..ResultSet::default()
                }),
                PacketKind::Err(e) => err = Some(e),
                PacketKind::LocalInfile => self.finish_infile().await?,
                PacketKind::ColumnCount(_) => cur = Some(ResultSet::default()),
                PacketKind::ColumnDef => {
                    if let Some(rs) = cur.as_mut() {
                        rs.columns.push(ColumnDef::decode(&p.payload, self.caps)?);
                    }
                }
                PacketKind::Row => {
                    if let Some(rs) = cur.as_mut() {
                        rs.rows.push(p.payload);
                    }
                }
                PacketKind::End(t) => {
                    let mut rs = cur.take().unwrap_or_default();
                    rs.status = t.status();
                    rs.warnings = t.warnings();
                    if let Terminator::Ok(ok) = t {
                        rs.ok = Some(ok);
                    }
                    out.push(rs);
                }
                PacketKind::ColumnsEof
                | PacketKind::PrepareOk(_)
                | PacketKind::ParamDef
                | PacketKind::ParamsEof
                | PacketKind::Raw => {}
            }
        }
        match err {
            Some(e) => Err(ClientError::Server(e)),
            None => Ok(out),
        }
    }

    /// Prepares `sql` on the server.
    ///
    /// # Errors
    /// [`ClientError::Server`] if the server refuses it; connection errors.
    pub async fn prepare(&mut self, sql: &[u8]) -> ClientResult<Prepared> {
        self.send_command(&Command::StmtPrepare(Bytes::copy_from_slice(sql)))
            .await?;
        let mut out = Prepared::default();
        let mut err = None;
        let mut in_params = true;
        while let Some(p) = self.next_packet().await? {
            match p.kind {
                PacketKind::PrepareOk(ok) => {
                    in_params = ok.params > 0;
                    out.ok = ok;
                }
                PacketKind::ParamDef => {
                    out.params.push(ColumnDef::decode(&p.payload, self.caps)?);
                    if out.params.len() == usize::from(out.ok.params) {
                        in_params = false;
                    }
                }
                PacketKind::ColumnDef if !in_params => {
                    out.columns.push(ColumnDef::decode(&p.payload, self.caps)?);
                }
                PacketKind::Err(e) => err = Some(e),
                _ => {}
            }
        }
        match err {
            Some(e) => Err(ClientError::Server(e)),
            None => Ok(out),
        }
    }

    /// Closes a prepared statement (`COM_STMT_CLOSE`, no response).
    ///
    /// # Errors
    /// As [`send_command`](Self::send_command).
    pub async fn close_statement(&mut self, stmt_id: u32) -> ClientResult<()> {
        self.send_command(&Command::StmtClose(stmt_id)).await
    }

    /// Runs a command whose answer is OK or ERR.
    async fn simple(&mut self, cmd: &Command) -> ClientResult<OkPacket> {
        self.send_command(cmd).await?;
        let mut result = Err(ClientError::Unexpected("no answer".to_owned()));
        while let Some(p) = self.next_packet().await? {
            result = match p.kind {
                PacketKind::Ok(ok) => Ok(ok),
                PacketKind::End(t) => Ok(OkPacket {
                    status: t.status(),
                    warnings: t.warnings(),
                    ..OkPacket::default()
                }),
                PacketKind::Err(e) => Err(ClientError::Server(e)),
                _ => Err(ClientError::Unexpected(format!(
                    "{:?} to {}",
                    p.kind,
                    cmd.name()
                ))),
            };
        }
        result
    }

    /// `COM_PING`.
    ///
    /// # Errors
    /// The server's ERR; connection errors.
    pub async fn ping(&mut self) -> ClientResult<()> {
        self.simple(&Command::Ping).await.map(drop)
    }

    /// `COM_INIT_DB`.
    ///
    /// # Errors
    /// The server's ERR (1049 unknown database); connection errors.
    pub async fn init_db(&mut self, schema: &str) -> ClientResult<()> {
        self.simple(&Command::InitDb(Bytes::copy_from_slice(schema.as_bytes())))
            .await
            .map(drop)
    }

    /// `COM_RESET_CONNECTION`: the server drops session state, user
    /// variables, temporary tables and prepared statements; the tracker is
    /// cleared.
    ///
    /// # Errors
    /// The server's ERR; connection errors.
    pub async fn reset_connection(&mut self) -> ClientResult<()> {
        self.simple(&Command::ResetConnection).await?;
        self.tracker.clear();
        Ok(())
    }

    /// Sends `COM_QUIT` (when the connection is usable) and closes the
    /// transport, with a TLS `close_notify`. Errors are ignored: the
    /// connection is gone either way, and broken afterwards.
    pub async fn quit(&mut self) {
        if self.is_idle() {
            let _ = self.send_command(&Command::Quit).await;
        }
        let _ = self.io.get_mut().shutdown().await;
        self.broken = true;
    }

    async fn read(&mut self) -> ClientResult<Packet> {
        read_packet(&mut self.io).await
    }

    async fn write(&mut self, payload: Bytes) -> ClientResult<()> {
        if let Err(e) = send(&mut self.io, payload).await {
            self.broken = true;
            return Err(e);
        }
        Ok(())
    }
}

async fn read_packet(io: &mut Framed<ClientStream, PacketCodec>) -> ClientResult<Packet> {
    match io.next().await {
        Some(Ok(p)) => Ok(p),
        Some(Err(e)) => Err(e.into()),
        None => Err(ClientError::Closed),
    }
}

async fn send(io: &mut Framed<ClientStream, PacketCodec>, payload: Bytes) -> ClientResult<()> {
    io.send(payload).await.map_err(ClientError::from)
}

/// The host part of `host:port` or `[v6]:port`.
fn host_of(addr: &str) -> &str {
    let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h);
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

async fn upgrade_tls(
    io: Framed<ClientStream, PacketCodec>,
    cfg: &ClientConfig,
    host: &str,
) -> ClientResult<Framed<ClientStream, PacketCodec>> {
    let parts = io.into_parts();
    let ClientStream::Plain(tcp) = parts.io else {
        return Err(ClientError::Tls(
            "TLS needs a plaintext TCP stream".to_owned(),
        ));
    };
    let config = match &cfg.tls_config {
        Some(c) => std::sync::Arc::clone(c),
        None => TlsOptions::default().build()?,
    };
    let name = cfg.server_name.as_deref().unwrap_or(host);
    let server_name = ServerName::try_from(name.to_owned())
        .map_err(|e| ClientError::Tls(format!("server name {name:?}: {e}")))?;
    let tls = tokio_rustls::TlsConnector::from(config)
        .connect(server_name, tcp)
        .await
        .map_err(|e| ClientError::Tls(e.to_string()))?;
    let mut next = FramedParts::new::<Bytes>(ClientStream::Tls(Box::new(tls)), parts.codec);
    next.read_buf = parts.read_buf;
    next.write_buf = parts.write_buf;
    Ok(Framed::from_parts(next))
}
