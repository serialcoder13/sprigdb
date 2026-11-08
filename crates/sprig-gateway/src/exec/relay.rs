//! The relay engine (chapter 05, "Resultsets: relay and rewrite"; build
//! step 7): streams one backend response into the client's sink.
//!
//! - **Rows** pass through as the backend's bytes ([`ResultSink::raw_row`]):
//!   text and binary rows don't depend on capabilities.
//! - **Column definitions** pass through unchanged when the client and the
//!   backend connection encode them alike (MariaDB extended metadata on
//!   both or neither, which the pool key guarantees in M0.2); otherwise
//!   they are decoded with the backend's capabilities and re-encoded for
//!   the client's (extended metadata stripped on 3306).
//! - **Terminators** become EOF or the OK form, whichever the client's
//!   `DEPRECATE_EOF` asks for.
//! - **OK packets** are re-encoded for the client: status flags pass
//!   through, the session-state block keeps the schema and the client's
//!   own system variables but loses what the gateway tracks for itself
//!   (`last_gtid`, GTIDs, transaction state, the state-change flag, the
//!   gateway-owned variables), and goes entirely without `SESSION_TRACK`.
//! - **ERR packets** keep the backend's code, SQLSTATE and message.
//! - **`LOCAL INFILE` requests** are answered only for the file the client
//!   named in the statement, and only when allowed; the file streams from
//!   the client to the backend packet by packet.
//!
//! `MORE_RESULTS_EXISTS` from the backend (a `CALL`, or a compound text
//! the backend split) passes through; [`Relay::more`] adds it to the final
//! packet when the gateway has more statements of the client's text to run.

use bytes::Bytes;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::client::PacketKind;
use sprig_proto_mysql::codec::Framing;
use sprig_proto_mysql::packets::{OkPacket, SessionChange};
use sprig_proto_mysql::resultset::{
    decode_binary_row, decode_text_row, encode_binary_row, encode_text_row,
};
use sprig_proto_mysql::state::ProtoState;
use sprig_proto_mysql::{Caps, ColumnDef, ErrPacket, StatusFlags, Value};

use super::split::Renamed;
use crate::backend::BackendSession;
use crate::backend::state::gateway_var;
use crate::pipeline::{EndOfRows, ResultSink, RowFormat};

/// How to relay one response.
#[allow(clippy::struct_excessive_bools)] // one flag per relay option
#[derive(Debug, Clone, Copy)]
pub(crate) struct Relay<'a> {
    /// Another result of the client's text follows this response.
    pub more: bool,
    /// Text or binary rows.
    pub format: RowFormat,
    /// A cursor was asked for (`COM_STMT_EXECUTE` cursor flags).
    pub cursor: bool,
    /// The response is `COM_STMT_FETCH` rows: no column count or
    /// definitions.
    pub fetch: bool,
    /// The backend connection decodes column definitions with these
    /// capabilities (only `MARIADB_CLIENT_EXTENDED_METADATA` matters).
    pub backend_caps: Caps,
    /// What a `LOCAL INFILE` request may ask for.
    pub infile: Infile<'a>,
    /// `SHOW [SESSION] STATUS`: the rows describing the client's own
    /// connection to rewrite.
    pub conn_status: Option<&'a ConnStatus>,
    /// Column names to give back (the gateway rewrote `CONNECTION_ID()`).
    pub renames: &'a [Renamed],
    /// The 3306 persona ran a `NOWAIT` statement: MariaDB's 1205 for a
    /// lock not granted at once is MySQL's 3572.
    pub mysql_nowait: bool,
}

/// The session status variables that describe the client's connection
/// (TLS, compression). The backend reports its own link to the gateway,
/// always TLS and uncompressed, so `SHOW STATUS` rows for these are
/// rewritten with the client's values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ConnStatus {
    vars: Vec<(&'static str, String)>,
}

impl ConnStatus {
    /// The client connection's values.
    pub(crate) fn of(proto: &ProtoState) -> Self {
        let (cipher, version) = proto
            .tls
            .as_ref()
            .map_or((String::new(), String::new()), |t| {
                (t.cipher.to_owned(), format!("TLSv{}", t.version))
            });
        let (on, algorithm, level) = match proto.framing {
            Framing::Plain => ("OFF", String::new(), "0".to_owned()),
            Framing::Zlib { level } => ("ON", "zlib".to_owned(), level.to_string()),
            Framing::Zstd { level } => ("ON", "zstd".to_owned(), level.to_string()),
        };
        Self {
            vars: vec![
                ("Ssl_cipher", cipher),
                ("Ssl_version", version),
                ("Compression", on.to_owned()),
                ("Compression_algorithm", algorithm),
                ("Compression_level", level),
            ],
        }
    }

    /// The same for a binary row (`SHOW STATUS` prepared, as Prisma's raw
    /// queries do), whose column definitions are `defs`.
    fn rewrite_binary(&self, row: &Bytes, defs: &[Bytes], caps: Caps) -> Option<Bytes> {
        let cols = decode_defs(defs, caps, &[]).ok()?;
        let mut vals = decode_binary_row(row, &cols).ok()?;
        let Some(Value::Bytes(name)) = vals.first() else {
            return None;
        };
        let (_, v) = self
            .vars
            .iter()
            .find(|(n, _)| n.as_bytes().eq_ignore_ascii_case(name))?;
        *vals.get_mut(1)? = Value::Bytes(Bytes::from(v.clone()));
        encode_binary_row(&vals, &cols).ok()
    }

    /// The row for a status variable, rewritten if it is one of these.
    fn rewrite(&self, row: &[u8]) -> Option<Bytes> {
        let vals = decode_text_row(row, 2).ok()?;
        let name = vals.first().copied().flatten()?;
        let (_, v) = self
            .vars
            .iter()
            .find(|(n, _)| n.as_bytes().eq_ignore_ascii_case(name))?;
        Some(encode_text_row([Some(name), Some(v.as_bytes())]))
    }
}

/// The `LOCAL INFILE` request a statement may cause.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Infile<'a> {
    /// The file the client named in the statement.
    pub name: Option<&'a [u8]>,
    /// The client has `LOCAL_FILES` and `sql.local_infile` is on.
    pub allowed: bool,
}

/// How a relayed response ended.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Relayed {
    /// An ERR went to the client: the client's text stops here.
    pub errored: bool,
    /// A cursor opened (its rows wait on the backend).
    pub cursor_opened: bool,
    /// The status of the final packet sent.
    pub status: Option<StatusFlags>,
}

/// 39012: the backend sent something the protocol doesn't allow here.
pub(crate) fn protocol_error(what: &str) -> SprigError {
    SprigError::new(
        SprigCode::BackendProtocol,
        format!("unexpected backend packet: {what}"),
    )
}

/// A backend ERR as the client sees it: code, SQLSTATE and message kept.
pub(crate) fn backend_error(e: &ErrPacket) -> SprigError {
    SprigError::backend(e.code, e.sqlstate, e.message.clone())
}

impl Relay<'_> {
    fn status(&self, s: StatusFlags) -> StatusFlags {
        let mut s = s;
        if self.more {
            s |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        s
    }

    /// Column definitions can pass unchanged.
    fn raw_defs(&self, client: Caps) -> bool {
        let ext = Caps::MARIADB_CLIENT_EXTENDED_METADATA;
        client.contains(ext) == self.backend_caps.contains(ext)
    }
}

fn decode_defs(defs: &[Bytes], caps: Caps, renames: &[Renamed]) -> Result<Vec<ColumnDef>> {
    let mut out = Vec::with_capacity(defs.len());
    for d in defs {
        let mut c = ColumnDef::decode(d, caps)?;
        rename(&mut c, renames);
        out.push(c);
    }
    Ok(out)
}

/// Gives a column the name the client's text gave it, if the gateway
/// rewrote that expression.
pub(crate) fn rename(c: &mut ColumnDef, renames: &[Renamed]) {
    if let Some(r) = renames.iter().find(|r| r.from == c.name) {
        c.name = r.to.clone();
    }
}

/// The client's OK packet for a backend OK (see the module docs).
pub(crate) fn client_ok(mut ok: OkPacket, status: StatusFlags) -> OkPacket {
    ok.status = status;
    if ok.session_state.is_empty() {
        ok.status.remove(StatusFlags::SESSION_STATE_CHANGED);
        return ok;
    }
    match ok.session_changes() {
        Ok(changes) => {
            let n = changes.len();
            let kept: Vec<SessionChange> = changes.into_iter().filter(client_visible).collect();
            if kept.len() != n {
                ok.set_session_changes(&kept);
            }
        }
        Err(e) => {
            tracing::debug!(error = %e, "undecodable session-state block; dropped");
            ok.set_session_changes(&[]);
        }
    }
    ok
}

/// Whether a tracked change is the client's to see.
fn client_visible(c: &SessionChange) -> bool {
    match c {
        SessionChange::Schema(_) => true,
        SessionChange::SystemVariable { name, .. } => {
            let name = String::from_utf8_lossy(name).to_ascii_lowercase();
            !gateway_var(&name)
        }
        _ => false,
    }
}

/// Relays the response of the statement `bs` just started into `out`.
///
/// # Errors
/// The backend's lost-connection errors; 39012 for a packet out of place;
/// the client's transport errors (the client is gone).
#[allow(clippy::too_many_lines)] // one arm per packet kind
pub(crate) async fn relay(
    bs: &mut BackendSession,
    out: &mut dyn ResultSink,
    r: &Relay<'_>,
) -> Result<Relayed> {
    let client_caps = out.wire().caps;
    let mut res = Relayed::default();
    let mut defs: Vec<Bytes> = Vec::new();
    let mut want: u64 = 0;
    let mut begun = r.fetch;
    while let Some(p) = bs.next().await? {
        match p.kind {
            PacketKind::ColumnCount(n) => {
                defs.clear();
                want = n;
                begun = false;
            }
            PacketKind::ColumnDef => {
                defs.push(p.payload);
                // With a cursor, the packet after the definitions decides
                // what the client gets.
                if defs.len() as u64 == want && !r.cursor {
                    begin(out, &defs, r, client_caps).await?;
                    begun = true;
                }
            }
            PacketKind::ColumnsEof => {
                let status = p.status().unwrap_or_default();
                if !begun && r.cursor && status.contains(StatusFlags::CURSOR_EXISTS) {
                    cursor_opened(out, &defs, r, status, &mut res).await?;
                    begun = true;
                } else if !begun {
                    begin(out, &defs, r, client_caps).await?;
                    begun = true;
                }
            }
            PacketKind::Row => {
                if !begun {
                    begin(out, &defs, r, client_caps).await?;
                    begun = true;
                }
                let rewritten = r
                    .conn_status
                    .filter(|_| want == 2)
                    .and_then(|cs| match r.format {
                        RowFormat::Text => cs.rewrite(&p.payload),
                        RowFormat::Binary => cs.rewrite_binary(&p.payload, &defs, r.backend_caps),
                    });
                match rewritten {
                    Some(row) => out.raw_row(row).await?,
                    None => out.raw_row(p.payload).await?,
                }
            }
            PacketKind::End(t) => {
                let mut status = t.status();
                if res.cursor_opened {
                    // The EOF of a cursor's definitions already went out.
                    continue;
                }
                if !begun && r.cursor && status.contains(StatusFlags::CURSOR_EXISTS) {
                    cursor_opened(out, &defs, r, status, &mut res).await?;
                    continue;
                }
                if !begun {
                    begin(out, &defs, r, client_caps).await?;
                }
                // EndOfRows carries no session-state block.
                status.remove(StatusFlags::SESSION_STATE_CHANGED);
                let status = r.status(status);
                out.end_rows(EndOfRows {
                    status,
                    warnings: t.warnings(),
                })
                .await?;
                res.status = Some(status);
                begun = r.fetch;
            }
            PacketKind::Ok(ok) => {
                let status = r.status(ok.status);
                out.ok(client_ok(ok, status)).await?;
                res.status = Some(status);
            }
            PacketKind::Err(e) => {
                let err = if r.mysql_nowait && e.code == 1205 {
                    SprigError::standard(
                        3572,
                        *b"HY000",
                        "Statement aborted because lock(s) could not be acquired immediately and NOWAIT is set.",
                    )
                } else {
                    backend_error(&e)
                };
                out.error(&err).await?;
                res.errored = true;
            }
            PacketKind::LocalInfile => {
                if !local_infile(bs, out, r, &p.payload[1..]).await? {
                    // Declined: the backend answers the empty file; the
                    // client gets 39010 instead.
                    while bs.next().await?.is_some() {}
                    out.error(&SprigError::new(
                        SprigCode::LocalInfileRejected,
                        "LOAD DATA LOCAL INFILE asked for a file the statement didn't name",
                    ))
                    .await?;
                    res.errored = true;
                    return Ok(res);
                }
            }
            PacketKind::PrepareOk(_)
            | PacketKind::ParamDef
            | PacketKind::ParamsEof
            | PacketKind::Raw => {
                return Err(protocol_error("prepare or raw packet in a resultset"));
            }
        }
    }
    Ok(res)
}

async fn begin(
    out: &mut dyn ResultSink,
    defs: &[Bytes],
    r: &Relay<'_>,
    client: Caps,
) -> Result<()> {
    if r.raw_defs(client) && r.renames.is_empty() {
        out.begin_rows_raw(defs, r.format).await
    } else {
        metrics::counter!("sprig_mysql_rewritten_packets_total", "kind" => "column_def")
            .increment(defs.len() as u64);
        let cols = decode_defs(defs, r.backend_caps, r.renames)?;
        out.begin_rows(&cols, r.format).await
    }
}

async fn cursor_opened(
    out: &mut dyn ResultSink,
    defs: &[Bytes],
    r: &Relay<'_>,
    status: StatusFlags,
    res: &mut Relayed,
) -> Result<()> {
    let cols = decode_defs(defs, r.backend_caps, r.renames)?;
    let mut status = r.status(status);
    status.remove(StatusFlags::SESSION_STATE_CHANGED);
    out.cursor_opened(
        &cols,
        EndOfRows {
            status,
            warnings: 0,
        },
    )
    .await?;
    res.cursor_opened = true;
    res.status = Some(status);
    Ok(())
}

/// Answers a `LOCAL INFILE` request for `name`: streams the client's file
/// when the statement named exactly this file and loading is allowed
/// (true), else declines it to the backend with an empty file (false).
async fn local_infile(
    bs: &mut BackendSession,
    out: &mut dyn ResultSink,
    r: &Relay<'_>,
    name: &[u8],
) -> Result<bool> {
    let ok = r.infile.allowed && r.infile.name == Some(name);
    if !ok {
        tracing::warn!(
            requested = %String::from_utf8_lossy(name),
            named = ?r.infile.name.map(String::from_utf8_lossy),
            "backend asked for a LOCAL INFILE file the statement didn't name; declined"
        );
        bs.finish_infile().await?;
        return Ok(false);
    }
    out.local_infile_request(name).await?;
    // Stream until the client's empty packet. If the backend fails
    // mid-file, keep reading the client's packets so the connection stays
    // in step, then report the failure.
    let mut failed: Option<SprigError> = None;
    loop {
        let chunk = out.local_infile_read().await?;
        if chunk.is_empty() {
            break;
        }
        if failed.is_none()
            && let Err(e) = bs.send_infile_data(&chunk).await
        {
            failed = Some(e);
        }
    }
    if let Some(e) = failed {
        return Err(e);
    }
    bs.finish_infile().await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_status_rows_describe_the_client() {
        let cs = ConnStatus {
            vars: vec![
                ("Ssl_cipher", "TLS_AES_256_GCM_SHA384".into()),
                ("Compression", "OFF".into()),
            ],
        };
        let row = encode_text_row([Some(&b"Ssl_cipher"[..]), Some(&b"ECDHE"[..])]);
        let out = cs.rewrite(&row).unwrap();
        assert_eq!(
            decode_text_row(&out, 2).unwrap()[1],
            Some(&b"TLS_AES_256_GCM_SHA384"[..])
        );
        let other = encode_text_row([Some(&b"Uptime"[..]), Some(&b"5"[..])]);
        assert!(cs.rewrite(&other).is_none());
        // Binary rows (a prepared SHOW STATUS).
        let col = |n: &str| ColumnDef {
            name: Bytes::copy_from_slice(n.as_bytes()),
            col_type: sprig_proto_mysql::resultset::column_type::VAR_STRING,
            charset: 45,
            ..ColumnDef::default()
        };
        let cols = [col("Variable_name"), col("Value")];
        let defs: Vec<Bytes> = cols.iter().map(|c| c.encode(Caps::PROTOCOL_41)).collect();
        let row = encode_binary_row(
            &[
                Value::Bytes(Bytes::from_static(b"Compression")),
                Value::Bytes(Bytes::from_static(b"ON")),
            ],
            &cols,
        )
        .unwrap();
        let out = cs.rewrite_binary(&row, &defs, Caps::PROTOCOL_41).unwrap();
        let vals = decode_binary_row(&out, &cols).unwrap();
        assert_eq!(vals[1], Value::Bytes(Bytes::from_static(b"OFF")));
    }

    #[test]
    fn ok_session_state_keeps_the_clients_changes_only() {
        let mut ok = OkPacket::new(StatusFlags::AUTOCOMMIT);
        ok.set_session_changes(&[
            SessionChange::Schema(Bytes::from_static(b"shop")),
            SessionChange::SystemVariable {
                name: Bytes::from_static(b"autocommit"),
                value: Bytes::from_static(b"OFF"),
            },
            SessionChange::SystemVariable {
                name: Bytes::from_static(b"last_gtid"),
                value: Bytes::from_static(b"0-1-5"),
            },
            SessionChange::StateChange(true),
            SessionChange::TransactionState(Bytes::from_static(b"T_______")),
        ]);
        let out = client_ok(
            ok,
            StatusFlags::AUTOCOMMIT | StatusFlags::SESSION_STATE_CHANGED,
        );
        let kept = out.session_changes().unwrap();
        assert_eq!(kept.len(), 2);
        assert!(matches!(&kept[0], SessionChange::Schema(s) if &s[..] == b"shop"));
        assert!(out.status.contains(StatusFlags::SESSION_STATE_CHANGED));

        let mut only_gateway = OkPacket::new(StatusFlags::AUTOCOMMIT);
        only_gateway.set_session_changes(&[SessionChange::SystemVariable {
            name: Bytes::from_static(b"last_gtid"),
            value: Bytes::from_static(b"0-1-6"),
        }]);
        let out = client_ok(
            only_gateway,
            StatusFlags::AUTOCOMMIT | StatusFlags::SESSION_STATE_CHANGED,
        );
        assert!(out.session_state.is_empty());
        assert!(!out.status.contains(StatusFlags::SESSION_STATE_CHANGED));
    }
}
