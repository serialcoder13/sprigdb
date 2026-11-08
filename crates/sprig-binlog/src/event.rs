//! Binlog events and the [`Decoder`] that turns raw event bytes into them.
//!
//! The decoder is the same for the network stream and for binlog files: it
//! checks CRC32 checksums, keeps the format description, table maps and
//! fragments of split rows events, and tracks the GTID position of the last
//! complete transaction group.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use sprig_common::SprigError;

use crate::buf::{Buf, corrupt, unsupported};
use crate::compress::uncompress_event_payload;
use crate::consts::{CHECKSUM_LEN, HEADER_LEN, event_type as et, gtid_flags, gtid_flags_extra};
use crate::gtid::{Gtid, GtidPos};
use crate::table_map::TableMap;
use crate::value::{RowImage, decode_row};

/// The 19-byte header every event starts with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EventHeader {
    /// Seconds since the epoch when the statement started.
    pub timestamp: u32,
    /// Event type code ([`crate::consts::event_type`]).
    pub type_code: u8,
    /// `server_id` of the server that wrote the event.
    pub server_id: u32,
    /// Event size including header and checksum.
    pub event_size: u32,
    /// Position of the next event in the binlog file; 0 inside transaction
    /// groups when `binlog_legacy_event_pos = OFF`, and for artificial events.
    pub log_pos: u32,
    /// Header flags.
    pub flags: u16,
}

impl EventHeader {
    fn parse(raw: &[u8]) -> Result<Self, SprigError> {
        let mut b = Buf::new(raw);
        Ok(Self {
            timestamp: b.u32()?,
            type_code: b.u8()?,
            server_id: b.u32()?,
            event_size: b.u32()?,
            log_pos: b.u32()?,
            flags: b.u16()?,
        })
    }

    /// Whether the dump thread made this event up (the first `ROTATE` and
    /// `GTID_LIST` of a stream).
    #[must_use]
    pub fn is_artificial(&self) -> bool {
        self.flags & crate::consts::FLAG_ARTIFICIAL != 0
    }
}

/// `GTID` event flags: `flags2` and, from MariaDB 10.8, `flags_extra`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GtidFlags {
    /// `flags2` ([`crate::consts::gtid_flags`]).
    pub bits: u8,
    /// `flags_extra` ([`crate::consts::gtid_flags_extra`]).
    pub extra: u8,
}

impl GtidFlags {
    /// One statement without `BEGIN`/`COMMIT`, such as DDL.
    #[must_use]
    pub fn standalone(self) -> bool {
        self.bits & gtid_flags::STANDALONE != 0
    }
    /// The group is DDL.
    #[must_use]
    pub fn ddl(self) -> bool {
        self.bits & gtid_flags::DDL != 0
    }
    /// Transactional engines only.
    #[must_use]
    pub fn transactional(self) -> bool {
        self.bits & gtid_flags::TRANSACTIONAL != 0
    }
    /// An `XA PREPARE`d transaction; the group ends with [`Event::XaPrepare`].
    #[must_use]
    pub fn prepared_xa(self) -> bool {
        self.bits & gtid_flags::PREPARED_XA != 0
    }
    /// `XA COMMIT` or `XA ROLLBACK` of a prepared transaction.
    #[must_use]
    pub fn completed_xa(self) -> bool {
        self.bits & gtid_flags::COMPLETED_XA != 0
    }
    /// `START ALTER` of a two-phase `ALTER` (`binlog_alter_two_phase`).
    #[must_use]
    pub fn start_alter(self) -> bool {
        self.extra & gtid_flags_extra::START_ALTER != 0
    }
    /// `COMMIT ALTER` of a two-phase `ALTER`.
    #[must_use]
    pub fn commit_alter(self) -> bool {
        self.extra & gtid_flags_extra::COMMIT_ALTER != 0
    }
    /// `ROLLBACK ALTER` of a two-phase `ALTER`.
    #[must_use]
    pub fn rollback_alter(self) -> bool {
        self.extra & gtid_flags_extra::ROLLBACK_ALTER != 0
    }
}

/// An XA transaction id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct XaId {
    /// `formatID`.
    pub format_id: i32,
    /// Global transaction id.
    pub gtrid: Vec<u8>,
    /// Branch qualifier.
    pub bqual: Vec<u8>,
}

impl fmt::Display for XaId {
    /// The SQL form: `X'..',X'..',format_id`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hex = |s: &[u8]| crate::buf::hex(s, true);
        write!(
            f,
            "X'{}',X'{}',{}",
            hex(&self.gtrid),
            hex(&self.bqual),
            self.format_id
        )
    }
}

/// The kind of a rows event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RowsKind {
    /// `WRITE_ROWS`: after images only.
    Insert,
    /// `UPDATE_ROWS`: before and after images.
    Update,
    /// `DELETE_ROWS`: before images only.
    Delete,
}

/// One row change: `before` for updates and deletes, `after` for inserts and
/// updates.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RowChange {
    /// The row before the change.
    pub before: Option<RowImage>,
    /// The row after the change.
    pub after: Option<RowImage>,
}

/// Session state a `QUERY` event carries in its status variables, as far as
/// an applier needs it. Unknown variables end parsing; fields after them
/// stay `None`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueryStatus {
    /// `sql_mode` bits.
    pub sql_mode: Option<u64>,
    /// `character_set_client` collation id.
    pub charset_client: Option<u16>,
    /// `collation_connection`.
    pub collation_connection: Option<u16>,
    /// `collation_server`.
    pub collation_server: Option<u16>,
    /// `time_zone`, when not the system zone.
    pub time_zone: Option<String>,
    /// `collation_database`.
    pub collation_database: Option<u16>,
    /// The XID of a DDL logged with one (MariaDB `Q_XID`).
    pub xid: Option<u64>,
}

/// A decoded binlog event.
///
/// Statement-format helpers (`INTVAR`, `RAND`, `USER_VAR`),
/// `BINLOG_CHECKPOINT`, `START_ENCRYPTION` (skipped on a dump, refused in a
/// file: see [`Decoder::set_server_decrypts`]) and MySQL-only events are not
/// returned; partial-row fragments are returned once, reassembled, as one
/// [`Event::Rows`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Event {
    /// The binlog's format: first event of every file, and of a stream after
    /// each `ROTATE`.
    FormatDescription {
        /// Binlog format version (4).
        binlog_version: u16,
        /// The server's version string.
        server_version: String,
        /// Whether events carry CRC32 checksums.
        checksums: bool,
    },
    /// The binlog moves to `file` at `pos`.
    Rotate {
        /// The next binlog file.
        file: String,
        /// Position in it.
        pos: u64,
    },
    /// Opens a transaction group.
    Gtid {
        /// The group's GTID.
        gtid: Gtid,
        /// Its flags.
        flags: GtidFlags,
        /// Group-commit id, for parallel apply.
        commit_id: Option<u64>,
        /// The XA transaction of a prepared or completed XA group.
        xa: Option<XaId>,
        /// The `START ALTER` sequence number a `COMMIT ALTER` or
        /// `ROLLBACK ALTER` refers to.
        alter_seq: Option<u64>,
        /// The originating thread id (MariaDB 11.5+).
        thread_id: Option<u32>,
    },
    /// The binlog state where a file starts, or where a GTID stream starts.
    GtidList(GtidPos),
    /// The statement text behind the following row events
    /// (`BINLOG_SEND_ANNOTATE_ROWS_EVENT`).
    Annotate(String),
    /// A table map, kept by the decoder for the following rows events.
    TableMap(Arc<TableMap>),
    /// Row changes to one table.
    Rows {
        /// The table id from the table map.
        table_id: u64,
        /// The table map.
        table: Arc<TableMap>,
        /// Insert, update or delete.
        kind: RowsKind,
        /// Rows-event flags ([`crate::consts::rows_flags`]).
        flags: u16,
        /// The changes, in order.
        rows: Vec<RowChange>,
    },
    /// A statement: `BEGIN`, `COMMIT`, DDL, `XA COMMIT`/`ROLLBACK`, or
    /// statement-format DML.
    Query {
        /// The default database.
        db: String,
        /// The statement.
        sql: String,
        /// Session state it ran under.
        status: QueryStatus,
        /// The thread that ran it.
        thread_id: u32,
        /// The error it raised on the source, if any.
        error_code: u16,
    },
    /// Commits the group.
    Xid(u64),
    /// Ends a prepared XA group (`XA PREPARE`), or commits it in one phase.
    XaPrepare {
        /// The XA transaction.
        xid: XaId,
        /// `XA COMMIT ... ONE PHASE`.
        one_phase: bool,
    },
    /// The server is alive and has nothing to send.
    Heartbeat,
    /// The server lost events; consumers must restart from a snapshot.
    Incident(String),
    /// The server stopped (end of a file it closed at shutdown).
    Stop,
    /// The server reached the end of its binlog and ended a non-blocking
    /// dump ([`crate::StreamOptions::non_blocking`]). Never decoded from a
    /// file or an event; only [`crate::BinlogStream`] returns it.
    DumpEnd,
}

/// Which tables' rows to decode. Rows events and table maps of other tables
/// are dropped without decoding.
pub type TableFilter = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

#[derive(Debug)]
struct Fragments {
    total: u32,
    next: u32,
    len: usize,
    data: Vec<u8>,
}

/// Turns raw events, header included, into [`Event`]s.
pub struct Decoder {
    checksums: bool,
    verify: bool,
    post_header: Vec<u8>,
    tables: HashMap<u64, Arc<TableMap>>,
    skipped_tables: HashSet<u64>,
    filter: Option<TableFilter>,
    fragments: Option<Fragments>,
    header: EventHeader,
    position: GtidPos,
    open: Option<(Gtid, GtidFlags)>,
    server_decrypts: bool,
}

impl fmt::Debug for Decoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Decoder")
            .field("checksums", &self.checksums)
            .field("tables", &self.tables.len())
            .field("position", &self.position)
            .finish_non_exhaustive()
    }
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new(false)
    }
}

impl Decoder {
    /// A decoder for events that carry CRC32 checksums (`checksums`) or not,
    /// until a format description says otherwise.
    #[must_use]
    pub fn new(checksums: bool) -> Self {
        Self {
            checksums,
            verify: true,
            post_header: Vec::new(),
            tables: HashMap::new(),
            skipped_tables: HashSet::new(),
            filter: None,
            fragments: None,
            header: EventHeader::default(),
            position: GtidPos::new(),
            open: None,
            server_decrypts: false,
        }
    }

    /// Whether the events come from a server's dump, which decrypts an
    /// encrypted binlog's events before sending them but still sends the
    /// file's `START_ENCRYPTION` event: the decoder then skips that event.
    /// Off by default, for binlog files, where it means the events after it
    /// are encrypted and the decoder refuses it. [`crate::BinlogStream`]
    /// turns it on.
    pub fn set_server_decrypts(&mut self, on: bool) {
        self.server_decrypts = on;
    }

    /// Decodes rows only for tables `filter` accepts.
    pub fn set_filter(&mut self, filter: Option<TableFilter>) {
        self.filter = filter;
    }

    /// Turns checksum verification off (events still have their trailer
    /// stripped). On by default.
    pub fn set_verify_checksums(&mut self, verify: bool) {
        self.verify = verify;
    }

    /// Starts position tracking from `pos`.
    pub fn set_position(&mut self, pos: GtidPos) {
        self.position = pos;
    }

    /// The position after the last complete transaction group: what to
    /// reconnect from to see the next group whole.
    #[must_use]
    pub fn position(&self) -> &GtidPos {
        &self.position
    }

    /// The GTID of the group being read, between its `GTID` event and its end.
    #[must_use]
    pub fn open_group(&self) -> Option<Gtid> {
        self.open.map(|(g, _)| g)
    }

    /// The header of the last event decoded.
    #[must_use]
    pub fn last_header(&self) -> &EventHeader {
        &self.header
    }

    /// Decodes one event. Returns `None` for events that are skipped or
    /// only update decoder state (fragments before the last, filtered
    /// tables, statement-format helpers).
    ///
    /// # Errors
    /// `ER_SPRIG_BINLOG_CORRUPT` on a checksum mismatch or malformed event;
    /// `ER_SPRIG_BINLOG_UNSUPPORTED` for events this decoder can't read.
    pub fn decode(&mut self, raw: &[u8]) -> Result<Option<Event>, SprigError> {
        if raw.len() < HEADER_LEN {
            return Err(corrupt(format!("event of {} bytes", raw.len())));
        }
        let header = EventHeader::parse(raw)?;
        if header.event_size as usize != raw.len() {
            return Err(corrupt(format!(
                "event header says {} bytes, got {}",
                header.event_size,
                raw.len()
            )));
        }
        self.header = header;
        if header.type_code == et::FORMAT_DESCRIPTION {
            return self.format_description(raw).map(Some);
        }
        let body = if self.checksums {
            if raw.len() < HEADER_LEN + CHECKSUM_LEN {
                return Err(corrupt("event too short for its checksum"));
            }
            let (data, crc) = raw.split_at(raw.len() - CHECKSUM_LEN);
            if self.verify {
                check_crc(data, crc, header.type_code)?;
            }
            &data[HEADER_LEN..]
        } else {
            &raw[HEADER_LEN..]
        };
        self.decode_body(header, body)
    }

    fn post_header_len(&self, t: u8, default: u8) -> u8 {
        self.post_header
            .get(usize::from(t).wrapping_sub(1))
            .copied()
            .unwrap_or(default)
    }

    fn format_description(&mut self, raw: &[u8]) -> Result<Event, SprigError> {
        let body = &raw[HEADER_LEN..];
        let mut b = Buf::new(body);
        let binlog_version = b.u16()?;
        if binlog_version != 4 {
            return Err(unsupported(format!(
                "binlog format version {binlog_version}"
            )));
        }
        let ver = b.take(50)?;
        let server_version =
            String::from_utf8_lossy(ver.split(|&c| c == 0).next().unwrap_or(ver)).into_owned();
        b.skip(4)?;
        let header_len = b.u8()?;
        if usize::from(header_len) != HEADER_LEN {
            return Err(unsupported(format!("event header length {header_len}")));
        }
        // Checksum-aware servers (all MariaDB we support) end the event with
        // the algorithm byte and a 4-byte checksum, whatever the algorithm.
        let rest = b.rest();
        if rest.len() < 1 + CHECKSUM_LEN {
            return Err(corrupt("format description too short"));
        }
        let alg = rest[rest.len() - 1 - CHECKSUM_LEN];
        let checksums = match alg {
            0 => false,
            1 => true,
            a => {
                return Err(unsupported(format!(
                    "binlog checksum algorithm {a}; only NONE and CRC32 exist"
                )));
            }
        };
        if checksums && self.verify {
            let (data, crc) = raw.split_at(raw.len() - CHECKSUM_LEN);
            // The server sets LOG_EVENT_BINLOG_IN_USE_F (bit 0 of the header
            // flags, byte 17) on the open file's format description without
            // recomputing the checksum, which covers the event with the bit
            // clear (as `mariadb-binlog` checks it).
            if data[17] & 1 != 0 {
                let mut clear = data.to_vec();
                clear[17] &= !1;
                check_crc(&clear, crc, et::FORMAT_DESCRIPTION)?;
            } else {
                check_crc(data, crc, et::FORMAT_DESCRIPTION)?;
            }
        }
        self.post_header = rest[..rest.len() - 1 - CHECKSUM_LEN].to_vec();
        self.checksums = checksums;
        Ok(Event::FormatDescription {
            binlog_version,
            server_version,
            checksums,
        })
    }

    fn decode_body(
        &mut self,
        header: EventHeader,
        body: &[u8],
    ) -> Result<Option<Event>, SprigError> {
        let t = header.type_code;
        let ev = match t {
            et::ROTATE => {
                let mut b = Buf::new(body);
                let pos = b.u64()?;
                let file = String::from_utf8_lossy(b.rest()).into_owned();
                Event::Rotate { file, pos }
            }
            et::GTID => self.gtid(body)?,
            et::GTID_LIST => Event::GtidList(parse_gtid_list(body)?),
            et::ANNOTATE_ROWS => Event::Annotate(String::from_utf8_lossy(body).into_owned()),
            et::TABLE_MAP => {
                let map = TableMap::parse(body, self.post_header_len(t, 8))?;
                let keep = self
                    .filter
                    .as_ref()
                    .is_none_or(|f| f(&map.schema, &map.table));
                if !keep {
                    self.tables.remove(&map.table_id);
                    self.skipped_tables.insert(map.table_id);
                    return Ok(None);
                }
                self.skipped_tables.remove(&map.table_id);
                let map = Arc::new(map);
                self.tables.insert(map.table_id, Arc::clone(&map));
                Event::TableMap(map)
            }
            et::WRITE_ROWS_V1
            | et::UPDATE_ROWS_V1
            | et::DELETE_ROWS_V1
            | et::WRITE_ROWS_V2
            | et::UPDATE_ROWS_V2
            | et::DELETE_ROWS_V2
            | et::WRITE_ROWS_COMPRESSED_V1
            | et::UPDATE_ROWS_COMPRESSED_V1
            | et::DELETE_ROWS_COMPRESSED_V1
            | et::WRITE_ROWS_COMPRESSED_V2
            | et::UPDATE_ROWS_COMPRESSED_V2
            | et::DELETE_ROWS_COMPRESSED_V2 => match self.rows(t, body)? {
                Some(ev) => ev,
                None => return Ok(None),
            },
            et::PARTIAL_ROW_DATA => return self.fragment(body),
            et::QUERY | et::QUERY_COMPRESSED => self.query(t, body)?,
            et::XID => Event::Xid(Buf::new(body).u64()?),
            et::XA_PREPARE_LOG => {
                let mut b = Buf::new(body);
                let one_phase = b.u8()? != 0;
                let format_id = i32::from_le_bytes(b.u32()?.to_le_bytes());
                let gl = b.u32()? as usize;
                let bl = b.u32()? as usize;
                Event::XaPrepare {
                    xid: XaId {
                        format_id,
                        gtrid: b.take(gl)?.to_vec(),
                        bqual: b.take(bl)?.to_vec(),
                    },
                    one_phase,
                }
            }
            et::HEARTBEAT => Event::Heartbeat,
            et::INCIDENT => {
                let mut b = Buf::new(body);
                let kind = b.u16()?;
                let msg = if b.is_empty() {
                    String::new()
                } else {
                    let n = usize::from(b.u8()?);
                    String::from_utf8_lossy(b.take(n)?).into_owned()
                };
                Event::Incident(format!("incident {kind}: {msg}"))
            }
            et::STOP => Event::Stop,
            et::START_ENCRYPTION if self.server_decrypts => {
                tracing::debug!(
                    "skipping START_ENCRYPTION: the server decrypts the binlog's events"
                );
                return Ok(None);
            }
            et::START_ENCRYPTION => {
                return Err(unsupported(
                    "encrypted binlog file; read it through a server, which decrypts",
                ));
            }
            _ => {
                tracing::trace!(type_code = t, "skipping binlog event");
                return Ok(None);
            }
        };
        self.track(&ev);
        Ok(Some(ev))
    }

    /// Advances the position when a group ends.
    fn track(&mut self, ev: &Event) {
        let ends = match ev {
            Event::Gtid { gtid, flags, .. } => {
                self.open = Some((*gtid, *flags));
                false
            }
            Event::Xid(_) | Event::XaPrepare { .. } => true,
            Event::Query { sql, .. } => self
                .open
                .is_some_and(|(_, f)| f.standalone() || is_group_end(sql) || f.completed_xa()),
            _ => false,
        };
        if ends && let Some((gtid, _)) = self.open.take() {
            self.position.update(gtid);
            // Table maps are re-sent in every group that uses them.
            self.tables.clear();
            self.skipped_tables.clear();
        }
    }

    fn gtid(&self, body: &[u8]) -> Result<Event, SprigError> {
        let mut b = Buf::new(body);
        let seq = b.u64()?;
        let domain = b.u32()?;
        let bits = b.u8()?;
        // Optional fields follow flags2 directly; the event is zero-padded to
        // the 19-byte post-header, so padding reads as `flags_extra = 0`.
        let commit_id = if bits & gtid_flags::GROUP_COMMIT_ID != 0 {
            Some(b.u64()?)
        } else {
            None
        };
        let xa = if bits & (gtid_flags::PREPARED_XA | gtid_flags::COMPLETED_XA) != 0 {
            let format_id = i32::from_le_bytes(b.u32()?.to_le_bytes());
            let gl = usize::from(b.u8()?);
            let bl = usize::from(b.u8()?);
            Some(XaId {
                format_id,
                gtrid: b.take(gl)?.to_vec(),
                bqual: b.take(bl)?.to_vec(),
            })
        } else {
            None
        };
        let mut extra = 0;
        let mut alter_seq = None;
        let mut thread_id = None;
        if !b.is_empty() {
            extra = b.u8()?;
            if extra & gtid_flags_extra::MULTI_ENGINE != 0 {
                b.skip(1)?;
            }
            if extra & (gtid_flags_extra::COMMIT_ALTER | gtid_flags_extra::ROLLBACK_ALTER) != 0 {
                alter_seq = Some(b.u64()?);
            }
            if extra & gtid_flags_extra::THREAD_ID != 0 && b.remaining() >= 4 {
                thread_id = Some(b.u32()?);
            }
        }
        Ok(Event::Gtid {
            gtid: Gtid {
                domain,
                server: self.header.server_id,
                seq,
            },
            flags: GtidFlags { bits, extra },
            commit_id,
            xa,
            alter_seq,
            thread_id,
        })
    }

    fn query(&self, t: u8, body: &[u8]) -> Result<Event, SprigError> {
        let mut b = Buf::new(body);
        let thread_id = b.u32()?;
        b.skip(4)?; // exec_time
        let db_len = usize::from(b.u8()?);
        let error_code = b.u16()?;
        let status_len = usize::from(b.u16()?);
        let phl = usize::from(self.post_header_len(t, 13));
        if phl > 13 {
            b.skip(phl - 13)?;
        }
        let status = parse_status_vars(b.take(status_len)?);
        let db = String::from_utf8_lossy(b.take(db_len)?).into_owned();
        b.skip(1)?;
        let sql_bytes = b.rest();
        let sql = if t == et::QUERY_COMPRESSED {
            String::from_utf8_lossy(&uncompress_event_payload(sql_bytes)?).into_owned()
        } else {
            String::from_utf8_lossy(sql_bytes).into_owned()
        };
        Ok(Event::Query {
            db,
            sql,
            status,
            thread_id,
            error_code,
        })
    }

    fn rows(&mut self, t: u8, body: &[u8]) -> Result<Option<Event>, SprigError> {
        let (kind, v2, compressed) = match t {
            et::WRITE_ROWS_V1 => (RowsKind::Insert, false, false),
            et::UPDATE_ROWS_V1 => (RowsKind::Update, false, false),
            et::DELETE_ROWS_V1 => (RowsKind::Delete, false, false),
            et::WRITE_ROWS_V2 => (RowsKind::Insert, true, false),
            et::UPDATE_ROWS_V2 => (RowsKind::Update, true, false),
            et::DELETE_ROWS_V2 => (RowsKind::Delete, true, false),
            et::WRITE_ROWS_COMPRESSED_V1 => (RowsKind::Insert, false, true),
            et::UPDATE_ROWS_COMPRESSED_V1 => (RowsKind::Update, false, true),
            et::DELETE_ROWS_COMPRESSED_V1 => (RowsKind::Delete, false, true),
            et::WRITE_ROWS_COMPRESSED_V2 => (RowsKind::Insert, true, true),
            et::UPDATE_ROWS_COMPRESSED_V2 => (RowsKind::Update, true, true),
            _ => (RowsKind::Delete, true, true),
        };
        let mut b = Buf::new(body);
        let phl = self.post_header_len(t, if v2 { 10 } else { 8 });
        let table_id = if phl == 6 {
            u64::from(b.u32()?)
        } else {
            b.u48()?
        };
        let flags = b.u16()?;
        if v2 && phl >= 10 {
            let extra = usize::from(b.u16()?);
            b.skip(extra.saturating_sub(2))?;
        }
        let Some(table) = self.tables.get(&table_id).cloned() else {
            if self.skipped_tables.contains(&table_id) {
                return Ok(None);
            }
            return Err(corrupt(format!(
                "rows event for table id {table_id} without a table map"
            )));
        };
        let width = b.lenenc_usize()?;
        if width != table.columns.len() {
            return Err(corrupt(format!(
                "rows event has {width} columns, table map of {}.{} has {}",
                table.schema,
                table.table,
                table.columns.len()
            )));
        }
        let present = b.take(width.div_ceil(8))?;
        let present_after = if kind == RowsKind::Update {
            b.take(width.div_ceil(8))?
        } else {
            present
        };
        let inflated;
        let data = if compressed {
            inflated = uncompress_event_payload(b.rest())?;
            &inflated[..]
        } else {
            b.rest()
        };
        let mut rb = Buf::new(data);
        let mut rows = Vec::new();
        let cols = &table.columns;
        while !rb.is_empty() {
            let row = match kind {
                RowsKind::Insert => RowChange {
                    before: None,
                    after: Some(decode_row(cols, present, &mut rb)?),
                },
                RowsKind::Delete => RowChange {
                    before: Some(decode_row(cols, present, &mut rb)?),
                    after: None,
                },
                RowsKind::Update => RowChange {
                    before: Some(decode_row(cols, present, &mut rb)?),
                    after: Some(decode_row(cols, present_after, &mut rb)?),
                },
            };
            rows.push(row);
        }
        Ok(Some(Event::Rows {
            table_id,
            table,
            kind,
            flags,
            rows,
        }))
    }

    /// `PARTIAL_ROW_DATA` (MariaDB 12.3+): post-header `total u32, seq u32,
    /// flags u8`; the first fragment (flag bit 0) then holds the assembled
    /// length as `u64`. The assembled bytes are one rows event, header
    /// included, without its checksum.
    fn fragment(&mut self, body: &[u8]) -> Result<Option<Event>, SprigError> {
        let mut b = Buf::new(body);
        let total = b.u32()?;
        let seq = b.u32()?;
        let flags = b.u8()?;
        let phl = usize::from(self.post_header_len(et::PARTIAL_ROW_DATA, 9));
        if phl > 9 {
            b.skip(phl - 9)?;
        }
        if flags & 1 != 0 {
            let len = usize::try_from(b.u64()?)
                .map_err(|_| corrupt("fragmented event length overflows"))?;
            if len > (1 << 32) {
                return Err(corrupt(format!("fragmented event of {len} bytes")));
            }
            self.fragments = Some(Fragments {
                total,
                next: 1,
                len,
                data: Vec::with_capacity(len),
            });
        }
        let Some(fr) = self.fragments.as_mut() else {
            return Err(corrupt(format!(
                "fragment {seq} of {total} without the first fragment"
            )));
        };
        if seq != fr.next || total != fr.total {
            return Err(corrupt(format!(
                "fragment {seq} of {total}, expected {} of {}",
                fr.next, fr.total
            )));
        }
        fr.data.extend_from_slice(b.rest());
        fr.next += 1;
        if seq < total {
            return Ok(None);
        }
        let Some(fr) = self.fragments.take() else {
            return Ok(None);
        };
        if fr.data.len() != fr.len {
            return Err(corrupt(format!(
                "fragments assembled {} bytes, first fragment said {}",
                fr.data.len(),
                fr.len
            )));
        }
        if fr.data.len() < HEADER_LEN {
            return Err(corrupt("assembled fragment shorter than a header"));
        }
        let inner = EventHeader::parse(&fr.data)?;
        let ev = self.decode_body(inner, &fr.data[HEADER_LEN..])?;
        Ok(ev)
    }
}

fn is_group_end(sql: &str) -> bool {
    let s = sql.trim_start();
    let head: String = s.chars().take(11).collect::<String>().to_ascii_uppercase();
    head == "COMMIT"
        || head == "ROLLBACK"
        || head.starts_with("XA COMMIT")
        || head.starts_with("XA ROLLBACK")
        || head.starts_with("COMMIT ")
}

fn check_crc(data: &[u8], crc: &[u8], t: u8) -> Result<(), SprigError> {
    let want = u32::from_le_bytes([crc[0], crc[1], crc[2], crc[3]]);
    let got = crc32fast::hash(data);
    if want == got {
        Ok(())
    } else {
        Err(corrupt(format!(
            "{} event fails its checksum: {got:#010x}, logged {want:#010x}",
            et::name(t)
        )))
    }
}

fn parse_gtid_list(body: &[u8]) -> Result<GtidPos, SprigError> {
    let mut b = Buf::new(body);
    let count = b.u32()? & 0x0fff_ffff;
    let mut pos = GtidPos::new();
    for _ in 0..count {
        let domain = b.u32()?;
        let server = b.u32()?;
        let seq = b.u64()?;
        pos.update(Gtid {
            domain,
            server,
            seq,
        });
    }
    Ok(pos)
}

/// Status variables of a `QUERY` event, as far as their sizes are known.
fn parse_status_vars(raw: &[u8]) -> QueryStatus {
    let mut s = QueryStatus::default();
    let mut b = Buf::new(raw);
    let _ = parse_status_inner(&mut b, &mut s);
    s
}

fn parse_status_inner(b: &mut Buf<'_>, s: &mut QueryStatus) -> Result<(), SprigError> {
    while !b.is_empty() {
        match b.u8()? {
            // flags2, auto_increment, master_data_written
            0 | 3 | 10 => b.skip(4)?,
            1 => s.sql_mode = Some(b.u64()?),
            2 => {
                let n = usize::from(b.u8()?);
                b.skip(n + 1)?;
            }
            4 => {
                s.charset_client = Some(b.u16()?);
                s.collation_connection = Some(b.u16()?);
                s.collation_server = Some(b.u16()?);
            }
            5 => {
                let n = usize::from(b.u8()?);
                s.time_zone = Some(String::from_utf8_lossy(b.take(n)?).into_owned());
            }
            6 => {
                let n = usize::from(b.u8()?);
                b.skip(n)?;
            }
            7 => b.skip(2)?, // lc_time_names
            8 => s.collation_database = Some(b.u16()?),
            9 => b.skip(8)?, // table_map_for_update
            11 => {
                let n = usize::from(b.u8()?);
                b.skip(n)?;
                let n = usize::from(b.u8()?);
                b.skip(n)?;
            }
            12 => {
                let n = b.u8()?;
                if n != 254 {
                    for _ in 0..n {
                        b.nul_terminated()?;
                    }
                }
            }
            13 | 128 => b.skip(3)?, // microseconds / hrnow
            129 => s.xid = Some(b.u64()?),
            _ => return Ok(()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
