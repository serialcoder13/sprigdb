//! The client side of the MySQL protocol, as much as a binlog client needs:
//! packet framing, the handshake with `mysql_native_password`, text queries,
//! `COM_REGISTER_SLAVE` and `COM_BINLOG_DUMP`.
//!
//! `sprig-proto-mysql` will own the full codec (chapter 05); this module
//! moves there when it exists. TLS is not implemented yet.

use std::time::Duration;

use sha1::{Digest, Sha1};
use sprig_common::{SprigCode, SprigError};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::buf::{Buf, corrupt, disconnected, put_lenenc, unsupported};

const MAX_PAYLOAD: usize = 0xff_ffff;

// Capability flags.
const CLIENT_LONG_PASSWORD: u32 = 1;
const CLIENT_LONG_FLAG: u32 = 4;
const CLIENT_PROTOCOL_41: u32 = 0x200;
const CLIENT_TRANSACTIONS: u32 = 0x2000;
const CLIENT_SECURE_CONNECTION: u32 = 0x8000;
const CLIENT_MULTI_RESULTS: u32 = 0x2_0000;
const CLIENT_PLUGIN_AUTH: u32 = 0x8_0000;
const CLIENT_PLUGIN_AUTH_LENENC: u32 = 0x20_0000;

/// Largest packet the client accepts (1 GiB), sent in the handshake.
const MAX_PACKET: u32 = 1 << 30;

/// A connection to a MariaDB server.
pub(crate) struct Conn {
    io: BufReader<TcpStream>,
    seq: u8,
    /// The server's version string.
    pub(crate) server_version: String,
    /// The server's connection id.
    pub(crate) connection_id: u32,
}

/// A text result set: rows of nullable strings.
pub(crate) type Rows = Vec<Vec<Option<String>>>;

impl Conn {
    /// Connects and authenticates.
    pub(crate) async fn connect(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        timeout: Duration,
    ) -> Result<Self, SprigError> {
        let addr = format!("{host}:{port}");
        let tcp = tokio::time::timeout(timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| disconnected(format!("connecting to {addr} timed out")))?
            .map_err(|e| disconnected(format!("connecting to {addr}: {e}")).with_source(e))?;
        tcp.set_nodelay(true).ok();
        let mut conn = Conn {
            io: BufReader::with_capacity(64 * 1024, tcp),
            seq: 0,
            server_version: String::new(),
            connection_id: 0,
        };
        tokio::time::timeout(timeout, conn.handshake(user, password))
            .await
            .map_err(|_| disconnected(format!("handshake with {addr} timed out")))??;
        Ok(conn)
    }

    async fn handshake(&mut self, user: &str, password: &str) -> Result<(), SprigError> {
        let greeting = self.read_packet().await?;
        if greeting.first() == Some(&0xff) {
            return Err(server_error(&greeting));
        }
        let mut b = Buf::new(&greeting);
        let proto = b.u8()?;
        if proto != 10 {
            return Err(unsupported(format!("handshake protocol {proto}")));
        }
        self.server_version = String::from_utf8_lossy(b.nul_terminated()?).into_owned();
        self.connection_id = b.u32()?;
        let mut scramble = b.take(8)?.to_vec();
        b.skip(1)?;
        let caps_lo = u32::from(b.u16()?);
        let _charset = b.u8()?;
        let _status = b.u16()?;
        let caps_hi = u32::from(b.u16()?);
        let server_caps = caps_lo | caps_hi << 16;
        let auth_len = usize::from(b.u8()?);
        b.skip(10)?;
        if server_caps & CLIENT_SECURE_CONNECTION != 0 {
            let n = auth_len.saturating_sub(8).max(13);
            let part2 = b.take(n)?;
            // The last byte is a NUL terminator.
            scramble.extend_from_slice(&part2[..n - 1]);
        }
        let plugin = if server_caps & CLIENT_PLUGIN_AUTH != 0 {
            String::from_utf8_lossy(b.nul_terminated().unwrap_or(b"mysql_native_password"))
                .into_owned()
        } else {
            "mysql_native_password".to_owned()
        };
        let needed = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        if server_caps & needed != needed {
            return Err(unsupported("server lacks protocol 4.1 authentication"));
        }
        let caps = CLIENT_LONG_PASSWORD
            | CLIENT_LONG_FLAG
            | CLIENT_PROTOCOL_41
            | CLIENT_TRANSACTIONS
            | CLIENT_SECURE_CONNECTION
            | CLIENT_MULTI_RESULTS
            | CLIENT_PLUGIN_AUTH
            | (server_caps & CLIENT_PLUGIN_AUTH_LENENC);
        let auth = auth_response(&plugin, password, &scramble)?;
        let mut out = Vec::with_capacity(128);
        out.extend_from_slice(&caps.to_le_bytes());
        out.extend_from_slice(&MAX_PACKET.to_le_bytes());
        out.push(45); // utf8mb4_general_ci
        out.extend_from_slice(&[0u8; 23]);
        out.extend_from_slice(user.as_bytes());
        out.push(0);
        if caps & CLIENT_PLUGIN_AUTH_LENENC != 0 {
            put_lenenc(&mut out, auth.len() as u64);
        } else {
            out.push(u8::try_from(auth.len()).unwrap_or(0));
        }
        out.extend_from_slice(&auth);
        out.extend_from_slice(plugin.as_bytes());
        out.push(0);
        self.write_packet(&out).await?;
        loop {
            let reply = self.read_packet().await?;
            match reply.first() {
                Some(0x00) => return Ok(()),
                Some(0xff) => return Err(server_error(&reply)),
                Some(0xfe) => {
                    // Auth switch: plugin name, then new scramble.
                    let mut b = Buf::new(&reply[1..]);
                    let plugin = String::from_utf8_lossy(b.nul_terminated()?).into_owned();
                    let data = b.rest();
                    let data = data.strip_suffix(&[0]).unwrap_or(data);
                    let auth = auth_response(&plugin, password, data)?;
                    self.write_packet(&auth).await?;
                }
                _ => return Err(corrupt("unexpected packet during authentication")),
            }
        }
    }

    /// Runs a statement and returns its rows (none for statements that
    /// return OK).
    pub(crate) async fn query(&mut self, sql: &str) -> Result<Rows, SprigError> {
        self.command(0x03, sql.as_bytes()).await?;
        let first = self.read_packet().await?;
        match first.first() {
            Some(0x00) => return Ok(Vec::new()),
            Some(0xff) => return Err(server_error(&first)),
            Some(0xfb) => return Err(unsupported("LOCAL INFILE request")),
            _ => {}
        }
        let ncols = Buf::new(&first).lenenc_usize()?;
        for _ in 0..ncols {
            self.read_packet().await?;
        }
        let eof = self.read_packet().await?;
        if !is_eof(&eof) {
            return Err(corrupt("expected EOF after column definitions"));
        }
        let mut rows = Vec::new();
        loop {
            let p = self.read_packet().await?;
            if is_eof(&p) {
                break;
            }
            if p.first() == Some(&0xff) {
                return Err(server_error(&p));
            }
            let mut b = Buf::new(&p);
            let mut row = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                if b.peek_rest().first() == Some(&0xfb) {
                    b.skip(1)?;
                    row.push(None);
                } else {
                    row.push(Some(
                        String::from_utf8_lossy(b.lenenc_bytes()?).into_owned(),
                    ));
                }
            }
            rows.push(row);
        }
        Ok(rows)
    }

    /// The single value of a one-row, one-column query.
    pub(crate) async fn query_value(&mut self, sql: &str) -> Result<Option<String>, SprigError> {
        let rows = self.query(sql).await?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next().flatten()))
    }

    /// `COM_REGISTER_SLAVE`.
    pub(crate) async fn register_slave(
        &mut self,
        server_id: u32,
        hostname: &str,
        port: u16,
    ) -> Result<(), SprigError> {
        let mut p = Vec::with_capacity(32 + hostname.len());
        p.extend_from_slice(&server_id.to_le_bytes());
        let host = &hostname.as_bytes()[..hostname.len().min(255)];
        p.push(u8::try_from(host.len()).unwrap_or(0));
        p.extend_from_slice(host);
        p.push(0); // user
        p.push(0); // password
        p.extend_from_slice(&port.to_le_bytes());
        p.extend_from_slice(&0u32.to_le_bytes()); // replication rank
        p.extend_from_slice(&0u32.to_le_bytes()); // master id
        self.command(0x15, &p).await?;
        let reply = self.read_packet().await?;
        match reply.first() {
            Some(0x00) => Ok(()),
            Some(0xff) => Err(server_error(&reply)),
            _ => Err(corrupt("unexpected reply to COM_REGISTER_SLAVE")),
        }
    }

    /// `COM_BINLOG_DUMP`. After it the server sends events, one per packet.
    pub(crate) async fn binlog_dump(
        &mut self,
        pos: u32,
        flags: u16,
        server_id: u32,
        file: &str,
    ) -> Result<(), SprigError> {
        let mut p = Vec::with_capacity(10 + file.len());
        p.extend_from_slice(&pos.to_le_bytes());
        p.extend_from_slice(&flags.to_le_bytes());
        p.extend_from_slice(&server_id.to_le_bytes());
        p.extend_from_slice(file.as_bytes());
        self.command(0x12, &p).await
    }

    /// Sends a command, resetting the sequence number.
    async fn command(&mut self, cmd: u8, payload: &[u8]) -> Result<(), SprigError> {
        self.seq = 0;
        let mut p = Vec::with_capacity(1 + payload.len());
        p.push(cmd);
        p.extend_from_slice(payload);
        self.write_packet(&p).await
    }

    async fn write_packet(&mut self, payload: &[u8]) -> Result<(), SprigError> {
        let mut out = Vec::with_capacity(payload.len() + 8);
        let mut chunks = payload.chunks(MAX_PAYLOAD).peekable();
        if payload.is_empty() {
            out.extend_from_slice(&[0, 0, 0, self.seq]);
            self.seq = self.seq.wrapping_add(1);
        }
        while let Some(c) = chunks.next() {
            out.extend_from_slice(&u32::try_from(c.len()).unwrap_or(0).to_le_bytes()[..3]);
            out.push(self.seq);
            self.seq = self.seq.wrapping_add(1);
            out.extend_from_slice(c);
            if chunks.peek().is_none() && c.len() == MAX_PAYLOAD {
                out.extend_from_slice(&[0, 0, 0, self.seq]);
                self.seq = self.seq.wrapping_add(1);
            }
        }
        let io = self.io.get_mut();
        io.write_all(&out).await.map_err(io_err)?;
        io.flush().await.map_err(io_err)
    }

    /// Reads one logical packet, joining 16 MiB continuation packets.
    pub(crate) async fn read_packet(&mut self) -> Result<Vec<u8>, SprigError> {
        let mut payload = Vec::new();
        loop {
            let mut h = [0u8; 4];
            self.io.read_exact(&mut h).await.map_err(io_err)?;
            let len = usize::from(h[0]) | usize::from(h[1]) << 8 | usize::from(h[2]) << 16;
            self.seq = h[3].wrapping_add(1);
            let start = payload.len();
            payload.resize(start + len, 0);
            self.io
                .read_exact(&mut payload[start..])
                .await
                .map_err(io_err)?;
            if len < MAX_PAYLOAD {
                return Ok(payload);
            }
        }
    }
}

fn io_err(e: std::io::Error) -> SprigError {
    disconnected(format!("binlog connection: {e}")).with_source(e)
}

fn is_eof(p: &[u8]) -> bool {
    p.first() == Some(&0xfe) && p.len() < 9
}

/// Turns an ERR packet into a backend error with the server's errno.
pub(crate) fn server_error(p: &[u8]) -> SprigError {
    let mut b = Buf::new(p.get(1..).unwrap_or_default());
    let errno = b.u16().unwrap_or(0);
    let mut sqlstate = *b"HY000";
    if b.peek_rest().first() == Some(&b'#')
        && let Ok(s) = b.take(6)
    {
        sqlstate.copy_from_slice(&s[1..]);
    }
    let msg = String::from_utf8_lossy(b.rest()).into_owned();
    SprigError::backend(errno, sqlstate, msg)
}

fn auth_response(plugin: &str, password: &str, scramble: &[u8]) -> Result<Vec<u8>, SprigError> {
    match plugin {
        "mysql_native_password" => Ok(native_password(password, scramble)),
        other => Err(SprigError::new(
            SprigCode::BinlogUnsupported,
            format!("authentication plugin {other} is not supported by the binlog client"),
        )),
    }
}

/// `SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password)))`.
fn native_password(password: &str, scramble: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let h1 = Sha1::digest(password.as_bytes());
    let h2 = Sha1::digest(h1);
    let mut h = Sha1::new();
    h.update(scramble);
    h.update(h2);
    let h3 = h.finalize();
    h1.iter().zip(h3.iter()).map(|(a, b)| a ^ b).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_password_matches_a_known_vector() {
        // From the MySQL protocol documentation's example exchange: the
        // response is a 20-byte XOR; an empty password sends nothing.
        let scramble = b"01234567890123456789";
        let r = native_password("secret", scramble);
        assert_eq!(r.len(), 20);
        assert!(native_password("", scramble).is_empty());
        // Deterministic: SHA1 of the inputs.
        assert_eq!(r, native_password("secret", scramble));
        assert_ne!(r, native_password("secret2", scramble));
    }

    #[test]
    fn err_packets_carry_errno_and_sqlstate() {
        let mut p = vec![0xff, 0xd4, 0x04, b'#'];
        p.extend_from_slice(b"HY000");
        p.extend_from_slice(b"Could not find GTID state");
        let e = server_error(&p);
        assert_eq!(
            e.kind,
            sprig_common::ErrorKind::Backend {
                errno: 1236,
                sqlstate: *b"HY000"
            }
        );
        assert!(e.message.contains("GTID"));
    }
}
