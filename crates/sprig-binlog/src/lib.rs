//! `sprig-binlog`: the MariaDB binlog client and event parser.
//!
//! Layer: 1 foundations. Design: [chapter 10](https://github.com/serialcoder13/sprigdb-plan/blob/main/lld/chapters/10-mover.md) (The mover) of the SprigDB low-level design,
//! with the consumer side in chapter 38 (The change feed).
//!
//! This is build step 1 of chapter 10, with step 2's parser additions
//! (compressed and fragmented events, XA groups, two-phase `ALTER` flags):
//!
//! - [`BinlogStream`]: connects to a node as a GTID-aware replica
//!   (`@mariadb_slave_capability = 4`, `@slave_connect_state`,
//!   `@master_binlog_checksum`, then `COM_BINLOG_DUMP`) and yields
//!   [`Event`]s, resuming after any [`GtidPos`]; the server decrypts an
//!   encrypted binlog (`encrypt_binlog = ON`), and the stream skips the
//!   `START_ENCRYPTION` event it sends ahead of the events; a non-blocking dump
//!   ([`StreamOptions::non_blocking`]) ends with [`Event::DumpEnd`] at the
//!   end of the binlog (chapter 09's dump probe reads this way);
//! - [`BinlogFile`]: the same events from binlog files;
//! - [`Decoder`]: the shared parser: framing, CRC32 checksums, format
//!   description, `GTID` and `GTID_LIST`, `TABLE_MAP`, rows events v1 and
//!   v2 and their zlib-compressed forms, 12.3's `PARTIAL_ROW_DATA`
//!   fragments, `QUERY` (and compressed), `XID`, `XA_PREPARE`,
//!   `ANNOTATE_ROWS`, heartbeats and incidents;
//! - [`Value`]: every MariaDB column type decoded from a row image,
//!   including compressed columns;
//! - [`annotate`]: the SprigDB statement comment the change feed reads
//!   from `ANNOTATE_ROWS` events (chapter 38, build step 1).
//!
//! - [`BinlogTls`]: TLS to the source (verify against the cluster's CA by
//!   default, or encrypt without verifying for development servers).
//!
//! Not here: authentication plugins other than
//! `mysql_native_password`, MariaDB's pre-10.1 temporal format with
//! fractional seconds, whose width the binlog doesn't record, and
//! encrypted binlog files read directly ([`BinlogFile`] refuses them: only
//! the server holds the key).

pub mod annotate;
pub mod consts;
pub mod gtid;
pub mod table_map;
pub mod value;

mod buf;
mod compress;
mod event;
mod file;
mod protocol;
mod stream;
mod tls;

pub use annotate::{OpHint, SprigComment, parse_comment};
pub use event::{
    Decoder, Event, EventHeader, GtidFlags, QueryStatus, RowChange, RowsKind, TableFilter, XaId,
};
pub use file::BinlogFile;
pub use gtid::{Gtid, GtidPos};
pub use stream::{BinlogSource, BinlogStream, StreamOptions};
pub use table_map::{Column, ColumnType, TableMap};
pub use tls::BinlogTls;
pub use value::{Date, DateTime, RowImage, Time, Timestamp, Value};
