//! [`BinlogStream`]: a GTID-positioned binlog dump from one MariaDB node
//! (chapter 10, "The binlog client").

use std::fmt;
use std::time::Duration;

use sprig_common::{Result, SprigCode, SprigError};

use crate::buf::{corrupt, disconnected};
use crate::consts::{dump_flags, event_type};
use crate::event::{Decoder, Event, EventHeader, TableFilter};
use crate::gtid::GtidPos;
use crate::protocol::{Conn, server_error};

/// Where to read a binlog from: one MariaDB node and an account with
/// `REPLICATION SLAVE` (and `BINLOG MONITOR` for `@@gtid_binlog_pos`).
#[derive(Clone, PartialEq, Eq)]
pub struct BinlogSource {
    /// Host name or address.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Account name, `sprig_repl` in managed clusters.
    pub user: String,
    /// Password.
    pub password: String,
}

impl fmt::Debug for BinlogSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinlogSource")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

impl BinlogSource {
    /// A source from its parts.
    pub fn new(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            user: user.into(),
            password: password.into(),
        }
    }

    /// Parses `mysql://user:password@host:port`, the form `sprig-testkit`
    /// uses; a path or query after the address is ignored. Percent-escapes
    /// are not decoded.
    ///
    /// # Errors
    /// `ER_SPRIG_CONFIG_INVALID` if the URL lacks a user or host.
    pub fn from_url(url: &str) -> Result<Self> {
        let bad = |why: &str| {
            SprigError::new(
                SprigCode::ConfigInvalid,
                format!("binlog source URL: {why}"),
            )
        };
        let rest = url
            .strip_prefix("mysql://")
            .or_else(|| url.strip_prefix("mariadb://"))
            .ok_or_else(|| bad("want mysql://user:password@host:port"))?;
        let rest = rest.split(['/', '?']).next().unwrap_or(rest);
        let (cred, addr) = rest.rsplit_once('@').ok_or_else(|| bad("no user"))?;
        let (user, password) = cred.split_once(':').unwrap_or((cred, ""));
        let (host, port) = match addr.rsplit_once(':') {
            Some((h, p)) => (h, p.parse().map_err(|_| bad("bad port"))?),
            None => (addr, 3306),
        };
        if user.is_empty() || host.is_empty() {
            return Err(bad("empty user or host"));
        }
        Ok(Self::new(host, port, user, password))
    }
}

/// How to open a stream.
#[derive(Clone)]
pub struct StreamOptions {
    /// The replica `server_id` the stream presents. It must differ from every
    /// real server's and from other streams' on the same node: MariaDB ends
    /// an older dump with the same id. Chapter 10 allocates these from
    /// `mover.server_id_base` (4000000000).
    pub server_id: u32,
    /// `@master_heartbeat_period`: the server sends a heartbeat after this
    /// much idle time.
    pub heartbeat: Duration,
    /// Silence of this many heartbeat periods ends the stream with
    /// `ER_SPRIG_BINLOG_DISCONNECTED`, so the caller reconnects.
    pub silence_periods: u32,
    /// Ask for `ANNOTATE_ROWS` events (the change feed wants them; the
    /// mover doesn't).
    pub annotate_rows: bool,
    /// Send `COM_REGISTER_SLAVE` first, so the stream shows in `SHOW SLAVE
    /// HOSTS`. Not needed by MariaDB.
    pub register_slave: bool,
    /// `@mariadb_slave_capability`; 4 (`MARIA_SLAVE_CAPABILITY_GTID`)
    /// is what a GTID dump needs.
    pub capability: u8,
    /// Connect and handshake timeout.
    pub connect_timeout: Duration,
    /// Decode rows only for tables this accepts, given (schema, table).
    pub filter: Option<TableFilter>,
    /// Verify CRC32 checksums.
    pub verify_checksums: bool,
}

impl fmt::Debug for StreamOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamOptions")
            .field("server_id", &self.server_id)
            .field("heartbeat", &self.heartbeat)
            .field("annotate_rows", &self.annotate_rows)
            .field("register_slave", &self.register_slave)
            .field("filter", &self.filter.is_some())
            .finish_non_exhaustive()
    }
}

impl StreamOptions {
    /// Defaults for a stream presenting `server_id`: 1 s heartbeats, three
    /// periods of silence, no annotations, no registration, capability 4.
    #[must_use]
    pub fn new(server_id: u32) -> Self {
        Self {
            server_id,
            heartbeat: Duration::from_secs(1),
            silence_periods: 3,
            annotate_rows: false,
            register_slave: false,
            capability: 4,
            connect_timeout: Duration::from_secs(10),
            filter: None,
            verify_checksums: true,
        }
    }
}

/// A binlog dump from one node, positioned by GTID.
///
/// ```no_run
/// # async fn example() -> sprig_common::Result<()> {
/// use sprig_binlog::{BinlogSource, BinlogStream, Event, GtidPos, StreamOptions};
///
/// let src = BinlogSource::from_url("mysql://sprig_repl:pw@127.0.0.1:13311")?;
/// let from: GtidPos = "2-201-41".parse()?;
/// let mut stream = BinlogStream::connect(&src, &from, &StreamOptions::new(4_000_000_001)).await?;
/// loop {
///     match stream.next().await? {
///         Event::Rows { table, rows, .. } => println!("{}.{}: {}", table.schema, table.table, rows.len()),
///         Event::Xid(_) => println!("committed; resume from {}", stream.position()),
///         _ => {}
///     }
/// }
/// # }
/// ```
pub struct BinlogStream {
    conn: Conn,
    decoder: Decoder,
    pending: Option<Vec<u8>>,
    silence: Duration,
    server_version: String,
}

impl fmt::Debug for BinlogStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinlogStream")
            .field("server_version", &self.server_version)
            .field("decoder", &self.decoder)
            .finish_non_exhaustive()
    }
}

impl BinlogStream {
    /// Connects to `src` and starts a dump after `from`: the first group
    /// returned is the one following `from` in every domain. An empty `from`
    /// starts at the oldest binlog the server has.
    ///
    /// # Errors
    /// `ER_SPRIG_BINLOG_DISCONNECTED` if the node can't be reached; the
    /// server's own error otherwise, notably 1236 when `from` isn't in its
    /// binlogs (purged, or transactions the node never had).
    pub async fn connect(src: &BinlogSource, from: &GtidPos, opts: &StreamOptions) -> Result<Self> {
        let mut conn = Conn::connect(
            &src.host,
            src.port,
            &src.user,
            &src.password,
            opts.connect_timeout,
        )
        .await?;
        let checksum = conn
            .query_value("SELECT @@global.binlog_checksum")
            .await?
            .unwrap_or_default();
        let checksums = match checksum.to_ascii_uppercase().as_str() {
            "CRC32" => true,
            "NONE" => false,
            other => {
                return Err(SprigError::new(
                    SprigCode::BinlogUnsupported,
                    format!("binlog_checksum = {other}"),
                ));
            }
        };
        let heartbeat_ns = u64::try_from(opts.heartbeat.as_nanos()).unwrap_or(u64::MAX);
        for sql in [
            "SET @master_binlog_checksum = @@global.binlog_checksum".to_owned(),
            format!("SET @mariadb_slave_capability = {}", opts.capability),
            format!("SET @slave_connect_state = '{from}'"),
            "SET @slave_gtid_strict_mode = 1".to_owned(),
            "SET @slave_gtid_ignore_duplicates = 0".to_owned(),
            format!("SET @master_heartbeat_period = {heartbeat_ns}"),
        ] {
            conn.query(&sql).await?;
        }
        if opts.register_slave {
            conn.register_slave(opts.server_id, "sprig-binlog", 0)
                .await?;
        }
        let flags = if opts.annotate_rows {
            dump_flags::SEND_ANNOTATE_ROWS
        } else {
            0
        };
        conn.binlog_dump(4, flags, opts.server_id, "").await?;
        // The first reply shows whether the server accepted the position.
        let first = tokio::time::timeout(opts.connect_timeout, conn.read_packet())
            .await
            .map_err(|_| disconnected("no reply to COM_BINLOG_DUMP"))??;
        check_packet(&first)?;
        let mut decoder = Decoder::new(checksums);
        decoder.set_filter(opts.filter.clone());
        decoder.set_verify_checksums(opts.verify_checksums);
        decoder.set_position(from.clone());
        let silence = opts.heartbeat * opts.silence_periods.max(1);
        let server_version = conn.server_version.clone();
        tracing::debug!(
            host = %src.host,
            port = src.port,
            from = %from,
            server_id = opts.server_id,
            "binlog stream started"
        );
        Ok(Self {
            conn,
            decoder,
            pending: Some(first),
            silence,
            server_version,
        })
    }

    /// The next event.
    ///
    /// # Errors
    /// `ER_SPRIG_BINLOG_DISCONNECTED` (retryable) when the connection breaks
    /// or stays silent past the configured heartbeat periods;
    /// `ER_SPRIG_BINLOG_CORRUPT` on a bad event; the server's error if it
    /// ends the dump with one. After an error, reconnect from
    /// [`BinlogStream::position`] or a position committed elsewhere.
    pub async fn next(&mut self) -> Result<Event> {
        loop {
            let packet = match self.pending.take() {
                Some(p) => p,
                None => tokio::time::timeout(self.silence, self.conn.read_packet())
                    .await
                    .map_err(|_| {
                        disconnected(format!(
                            "no binlog event or heartbeat for {:?}",
                            self.silence
                        ))
                    })??,
            };
            check_packet(&packet)?;
            let raw = &packet[1..];
            let decoded = self.decoder.decode(raw)?;
            let t = self.decoder.last_header().type_code;
            metrics::counter!("sprig_binlog_events_total", "type" => event_type::name(t))
                .increment(1);
            if let Some(ev) = decoded {
                return Ok(ev);
            }
        }
    }

    /// The position after the last complete transaction group returned.
    #[must_use]
    pub fn position(&self) -> &GtidPos {
        self.decoder.position()
    }

    /// The header of the last event read (timestamps for lag, server id).
    #[must_use]
    pub fn last_header(&self) -> &EventHeader {
        self.decoder.last_header()
    }

    /// The server's version string.
    #[must_use]
    pub fn server_version(&self) -> &str {
        &self.server_version
    }
}

fn check_packet(p: &[u8]) -> Result<()> {
    match p.first() {
        Some(0x00) => Ok(()),
        Some(0xff) => Err(server_error(p)),
        Some(0xfe) if p.len() < 9 => Err(disconnected("the server ended the binlog dump")),
        _ => Err(corrupt("binlog packet without an OK marker")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_source_urls() {
        let s = BinlogSource::from_url("mysql://root:sprig-dev-root@127.0.0.1:13301").unwrap();
        assert_eq!(
            s,
            BinlogSource::new("127.0.0.1", 13301, "root", "sprig-dev-root")
        );
        let s = BinlogSource::from_url("mariadb://u@db/x?y=1").unwrap();
        assert_eq!(
            (s.host.as_str(), s.port, s.password.as_str()),
            ("db", 3306, "")
        );
        assert!(BinlogSource::from_url("http://x").is_err());
        assert!(BinlogSource::from_url("mysql://host:1").is_err());
        assert!(!format!("{s:?}").contains("password"));
    }
}
