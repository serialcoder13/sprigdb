//! Event type codes, column type codes and flags, as MariaDB numbers them
//! (`sql/log_event.h`, `include/mysql_com.h`).

/// Binlog event type codes.
#[allow(missing_docs)]
pub mod event_type {
    pub const UNKNOWN: u8 = 0;
    pub const START_V3: u8 = 1;
    pub const QUERY: u8 = 2;
    pub const STOP: u8 = 3;
    pub const ROTATE: u8 = 4;
    pub const INTVAR: u8 = 5;
    pub const RAND: u8 = 13;
    pub const USER_VAR: u8 = 14;
    pub const FORMAT_DESCRIPTION: u8 = 15;
    pub const XID: u8 = 16;
    pub const BEGIN_LOAD_QUERY: u8 = 17;
    pub const EXECUTE_LOAD_QUERY: u8 = 18;
    pub const TABLE_MAP: u8 = 19;
    pub const WRITE_ROWS_V1: u8 = 23;
    pub const UPDATE_ROWS_V1: u8 = 24;
    pub const DELETE_ROWS_V1: u8 = 25;
    pub const INCIDENT: u8 = 26;
    pub const HEARTBEAT: u8 = 27;
    pub const IGNORABLE: u8 = 28;
    pub const ROWS_QUERY: u8 = 29;
    pub const WRITE_ROWS_V2: u8 = 30;
    pub const UPDATE_ROWS_V2: u8 = 31;
    pub const DELETE_ROWS_V2: u8 = 32;
    pub const GTID_MYSQL: u8 = 33;
    pub const ANONYMOUS_GTID_MYSQL: u8 = 34;
    pub const PREVIOUS_GTIDS_MYSQL: u8 = 35;
    pub const XA_PREPARE_LOG: u8 = 38;
    // MariaDB's own events start at 160.
    pub const ANNOTATE_ROWS: u8 = 160;
    pub const BINLOG_CHECKPOINT: u8 = 161;
    pub const GTID: u8 = 162;
    pub const GTID_LIST: u8 = 163;
    pub const START_ENCRYPTION: u8 = 164;
    pub const QUERY_COMPRESSED: u8 = 165;
    pub const WRITE_ROWS_COMPRESSED_V1: u8 = 166;
    pub const UPDATE_ROWS_COMPRESSED_V1: u8 = 167;
    pub const DELETE_ROWS_COMPRESSED_V1: u8 = 168;
    pub const WRITE_ROWS_COMPRESSED_V2: u8 = 169;
    pub const UPDATE_ROWS_COMPRESSED_V2: u8 = 170;
    pub const DELETE_ROWS_COMPRESSED_V2: u8 = 171;
    /// One fragment of a rows event larger than
    /// `binlog_row_event_fragment_threshold` (MariaDB 12.3+).
    pub const PARTIAL_ROW_DATA: u8 = 172;

    /// A short name for metrics and logs (`sprig_binlog_events_total{type}`).
    #[must_use]
    pub fn name(t: u8) -> &'static str {
        match t {
            QUERY => "query",
            STOP => "stop",
            ROTATE => "rotate",
            INTVAR => "intvar",
            RAND => "rand",
            USER_VAR => "user_var",
            FORMAT_DESCRIPTION => "format_description",
            XID => "xid",
            TABLE_MAP => "table_map",
            WRITE_ROWS_V1 | WRITE_ROWS_V2 => "write_rows",
            UPDATE_ROWS_V1 | UPDATE_ROWS_V2 => "update_rows",
            DELETE_ROWS_V1 | DELETE_ROWS_V2 => "delete_rows",
            INCIDENT => "incident",
            HEARTBEAT => "heartbeat",
            XA_PREPARE_LOG => "xa_prepare",
            ANNOTATE_ROWS => "annotate_rows",
            BINLOG_CHECKPOINT => "binlog_checkpoint",
            GTID => "gtid",
            GTID_LIST => "gtid_list",
            START_ENCRYPTION => "start_encryption",
            QUERY_COMPRESSED => "query_compressed",
            WRITE_ROWS_COMPRESSED_V1 | WRITE_ROWS_COMPRESSED_V2 => "write_rows_compressed",
            UPDATE_ROWS_COMPRESSED_V1 | UPDATE_ROWS_COMPRESSED_V2 => "update_rows_compressed",
            DELETE_ROWS_COMPRESSED_V1 | DELETE_ROWS_COMPRESSED_V2 => "delete_rows_compressed",
            PARTIAL_ROW_DATA => "partial_row_data",
            _ => "other",
        }
    }
}

/// Column type codes in `TABLE_MAP` events (`enum_field_types`).
#[allow(missing_docs)]
pub mod column_type {
    pub const DECIMAL: u8 = 0;
    pub const TINY: u8 = 1;
    pub const SHORT: u8 = 2;
    pub const LONG: u8 = 3;
    pub const FLOAT: u8 = 4;
    pub const DOUBLE: u8 = 5;
    pub const NULL: u8 = 6;
    pub const TIMESTAMP: u8 = 7;
    pub const LONGLONG: u8 = 8;
    pub const INT24: u8 = 9;
    pub const DATE: u8 = 10;
    pub const TIME: u8 = 11;
    pub const DATETIME: u8 = 12;
    pub const YEAR: u8 = 13;
    pub const NEWDATE: u8 = 14;
    pub const VARCHAR: u8 = 15;
    pub const BIT: u8 = 16;
    pub const TIMESTAMP2: u8 = 17;
    pub const DATETIME2: u8 = 18;
    pub const TIME2: u8 = 19;
    /// MariaDB column compression (`BLOB`/`TEXT ... COMPRESSED`).
    pub const BLOB_COMPRESSED: u8 = 140;
    /// MariaDB column compression (`VARCHAR ... COMPRESSED`).
    pub const VARCHAR_COMPRESSED: u8 = 141;
    /// MySQL's binary JSON; MariaDB never logs it (its JSON is `LONGTEXT`).
    pub const JSON: u8 = 245;
    pub const NEWDECIMAL: u8 = 246;
    pub const ENUM: u8 = 247;
    pub const SET: u8 = 248;
    pub const TINY_BLOB: u8 = 249;
    pub const MEDIUM_BLOB: u8 = 250;
    pub const LONG_BLOB: u8 = 251;
    pub const BLOB: u8 = 252;
    pub const VAR_STRING: u8 = 253;
    pub const STRING: u8 = 254;
    pub const GEOMETRY: u8 = 255;
}

/// `GTID` event flags (`flags2`).
pub mod gtid_flags {
    /// The group has no `BEGIN`/`COMMIT`: one statement, such as DDL.
    pub const STANDALONE: u8 = 1;
    /// A commit id for parallel replication follows the domain id.
    pub const GROUP_COMMIT_ID: u8 = 2;
    /// Transactional engine only.
    pub const TRANSACTIONAL: u8 = 4;
    /// Safe to apply in parallel.
    pub const ALLOW_PARALLEL: u8 = 8;
    /// The transaction waited for a lock held by another one.
    pub const WAITED: u8 = 16;
    /// The group is DDL.
    pub const DDL: u8 = 32;
    /// The group is an `XA PREPARE`d transaction (ends with `XA_PREPARE`).
    pub const PREPARED_XA: u8 = 64;
    /// The group is `XA COMMIT` or `XA ROLLBACK` of a prepared transaction.
    pub const COMPLETED_XA: u8 = 128;
}

/// `GTID` event extra flags (`flags_extra`, MariaDB 10.8+).
pub mod gtid_flags_extra {
    /// An extra-engines count byte follows.
    pub const MULTI_ENGINE: u8 = 1;
    /// `START ALTER` of a two-phase `ALTER` (`binlog_alter_two_phase`).
    pub const START_ALTER: u8 = 2;
    /// `COMMIT ALTER`; the start-alter sequence number follows.
    pub const COMMIT_ALTER: u8 = 4;
    /// `ROLLBACK ALTER`; the start-alter sequence number follows.
    pub const ROLLBACK_ALTER: u8 = 8;
    /// The originating thread id follows (MariaDB 11.5+).
    pub const THREAD_ID: u8 = 16;
}

/// Rows-event flags.
pub mod rows_flags {
    /// The last rows event of a statement.
    pub const STMT_END: u16 = 1;
    /// `foreign_key_checks = 0` was in effect.
    pub const NO_FOREIGN_KEY_CHECKS: u16 = 2;
    /// `unique_checks = 0` was in effect.
    pub const RELAXED_UNIQUE_CHECKS: u16 = 4;
    /// Every column is present in the after image.
    pub const COMPLETE_ROWS: u16 = 8;
}

/// `COM_BINLOG_DUMP` flags.
pub(crate) mod dump_flags {
    /// `BINLOG_DUMP_NON_BLOCK`: end the dump with an EOF packet at the end
    /// of the binlog instead of waiting for new events.
    pub(crate) const NON_BLOCK: u16 = 1;
    /// Send `ANNOTATE_ROWS` events (the statement text before row events).
    pub(crate) const SEND_ANNOTATE_ROWS: u16 = 2;
}

/// Event header length (binlog format v4).
pub(crate) const HEADER_LEN: usize = 19;
/// CRC32 trailer length.
pub(crate) const CHECKSUM_LEN: usize = 4;
/// `LOG_EVENT_ARTIFICIAL_F`: the event was made up by the dump thread.
pub(crate) const FLAG_ARTIFICIAL: u16 = 0x20;
/// The magic number that starts every binlog file.
pub const BINLOG_MAGIC: [u8; 4] = [0xfe, b'b', b'i', b'n'];
