//! The executor's [`ResultSink`] for HTTP requests: results become
//! `sprig.v1` messages ([`QueryResponse`] parts) on a bounded channel.
//!
//! The executor sees a MySQL 8.4 client with `DEPRECATE_EOF`,
//! `MULTI_RESULTS` and `SESSION_TRACK`, so it runs every statement exactly
//! as it would for a wire client: the same rewrite, the same relay of row
//! packets (decoded here from the text protocol), the same errors. Errors
//! go through the MySQL persona's error map ([`ErrPacket::from_error`]),
//! so their number, SQLSTATE and text are the wire's.
//!
//! The channel holds four messages: a slow HTTP client stops the backend
//! read, as TCP backpressure does on the wire. Rows are batched by
//! `api.stream_batch_rows` or 1 MiB, whichever comes first.

use async_trait::async_trait;
use bytes::Bytes;
use prost_types::ListValue;
use sprig_api::proto::v1::{
    Column, QueryResponse, ResultEnd, ResultHeader, RowBatch, query_response,
};
use sprig_api::results::{cell, is_binary, type_name};
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::resultset::decode_text_row;
use sprig_proto_mysql::{Caps, ColumnDef, ErrPacket, Persona, StatusFlags, Value};
use tokio::sync::mpsc;

use crate::pipeline::{EndOfRows, ResultSink, RowFormat, RowValues, Wire};

/// Largest row batch, in bytes of cell text.
pub const BATCH_BYTES: usize = 1 << 20;

/// The capabilities the executor sees: a MySQL 8.4 client without TLS,
/// compression or `LOCAL INFILE`.
#[must_use]
pub fn http_caps() -> Caps {
    let mut c = Caps::server_default(Persona::MySql84);
    c.remove(Caps::SSL | Caps::COMPRESS | Caps::ZSTD_COMPRESSION_ALGORITHM | Caps::LOCAL_FILES);
    c
}

/// One item on the channel.
#[derive(Debug)]
pub enum Item {
    /// A part of the response.
    Part(QueryResponse),
    /// The response ended with this error (the persona's mapping applied).
    Error(SprigError),
}

/// The error the wire would show for `e`: the MySQL persona's number,
/// SQLSTATE and text.
#[must_use]
pub fn as_wire(e: &SprigError) -> SprigError {
    let p = ErrPacket::from_error(Persona::MySql84, e);
    sprig_api::status::from_parts(p.code, &p.sqlstate, &p.message)
}

/// The client went away (the channel's reader dropped).
fn gone() -> SprigError {
    SprigError::new(
        sprig_common::SprigCode::BackendLost,
        "the HTTP client went away",
    )
}

/// A [`ResultSink`] writing [`Item`]s.
#[derive(Debug)]
pub struct ChannelSink {
    tx: mpsc::Sender<Item>,
    batch_rows: usize,
    /// Which result is next, from 0.
    index: u32,
    /// Per column of the open resultset: its cells are binary.
    binary: Vec<bool>,
    in_rows: bool,
    batch: Vec<ListValue>,
    batch_bytes: usize,
    finished: bool,
    /// The status of the last OK or terminator.
    pub last_status: Option<StatusFlags>,
}

impl ChannelSink {
    /// A sink sending on `tx`, batching `batch_rows` rows.
    #[must_use]
    pub fn new(tx: mpsc::Sender<Item>, batch_rows: usize) -> Self {
        Self {
            tx,
            batch_rows: batch_rows.max(1),
            index: 0,
            binary: Vec::new(),
            in_rows: false,
            batch: Vec::new(),
            batch_bytes: 0,
            finished: false,
            last_status: None,
        }
    }

    /// Whether the reader is gone.
    #[must_use]
    pub fn closed(&self) -> bool {
        self.tx.is_closed()
    }

    async fn send(&self, item: Item) -> Result<()> {
        self.tx.send(item).await.map_err(|_| gone())
    }

    async fn part(&self, part: query_response::Part) -> Result<()> {
        self.send(Item::Part(QueryResponse { part: Some(part) }))
            .await
    }

    async fn flush_batch(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.batch);
        self.batch_bytes = 0;
        self.part(query_response::Part::Rows(RowBatch { rows }))
            .await
    }

    async fn push_row(&mut self, cells: Vec<prost_types::Value>, bytes: usize) -> Result<()> {
        self.batch.push(ListValue { values: cells });
        self.batch_bytes += bytes;
        if self.batch.len() >= self.batch_rows || self.batch_bytes >= BATCH_BYTES {
            self.flush_batch().await?;
        }
        Ok(())
    }

    async fn end(&mut self, end: ResultEnd) -> Result<()> {
        let more = end.more_results;
        self.part(query_response::Part::End(end)).await?;
        self.index += 1;
        self.in_rows = false;
        self.binary.clear();
        if !more {
            self.finished = true;
        }
        Ok(())
    }
}

fn column(c: &ColumnDef) -> Column {
    let s = |b: &Bytes| String::from_utf8_lossy(b).into_owned();
    let mysql_type = u32::from(c.col_type);
    let charset = u32::from(c.charset);
    Column {
        name: s(&c.name),
        org_name: s(&c.org_name),
        table: s(&c.table),
        org_table: s(&c.org_table),
        database: s(&c.schema),
        r#type: type_name(mysql_type).to_owned(),
        mysql_type,
        flags: u32::from(c.flags),
        charset,
        decimals: u32::from(c.decimals),
        length: u64::from(c.length),
        binary: is_binary(mysql_type, charset),
    }
}

/// A binary-protocol value in its text form (only for executors that
/// answer a text query with binary rows, which none do today).
fn binary_text(v: &Value) -> Option<Vec<u8>> {
    let t = match v {
        Value::Null => return None,
        Value::Bytes(b) => return Some(b.to_vec()),
        other => format!("{other:?}"),
    };
    Some(t.into_bytes())
}

#[async_trait]
impl ResultSink for ChannelSink {
    fn wire(&self) -> Wire {
        Wire {
            persona: Persona::MySql84,
            caps: http_caps(),
        }
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        let cols: Vec<Column> = columns.iter().map(column).collect();
        self.binary = cols.iter().map(|c| c.binary).collect();
        self.in_rows = true;
        self.part(query_response::Part::Header(ResultHeader {
            index: self.index,
            columns: cols,
        }))
        .await
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        let mut bytes = 0;
        let cells: Vec<prost_types::Value> = match values {
            RowValues::Text(vals) => vals
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    bytes += v.map_or(0, <[u8]>::len);
                    cell(*v, self.binary.get(i).copied().unwrap_or(false))
                })
                .collect(),
            RowValues::Binary(vals) => vals
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let t = binary_text(v);
                    bytes += t.as_ref().map_or(0, Vec::len);
                    cell(t.as_deref(), self.binary.get(i).copied().unwrap_or(false))
                })
                .collect(),
        };
        self.push_row(cells, bytes).await
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        let vals = decode_text_row(&payload, self.binary.len())
            .map_err(|e| SprigError::internal(format!("a backend row didn't decode: {e}")))?;
        let bytes = payload.len();
        let cells = vals
            .iter()
            .enumerate()
            .map(|(i, v)| cell(*v, self.binary.get(i).copied().unwrap_or(false)))
            .collect();
        self.push_row(cells, bytes).await
    }

    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.flush_batch().await?;
        self.last_status = Some(end.status);
        self.end(ResultEnd {
            index: self.index,
            warnings: u32::from(end.warnings),
            more_results: end.status.contains(StatusFlags::MORE_RESULTS_EXISTS),
            ..ResultEnd::default()
        })
        .await
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.flush_batch().await?;
        self.last_status = Some(ok.status);
        self.end(ResultEnd {
            index: self.index,
            affected_rows: ok.affected_rows,
            last_insert_id: ok.last_insert_id,
            warnings: u32::from(ok.warnings),
            info: String::from_utf8_lossy(&ok.info).into_owned(),
            more_results: ok.status.contains(StatusFlags::MORE_RESULTS_EXISTS),
        })
        .await
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.flush_batch().await?;
        self.finished = true;
        self.in_rows = false;
        self.send(Item::Error(as_wire(err))).await
    }

    async fn column_list(&mut self, _columns: &[ColumnDef], _end: EndOfRows) -> Result<()> {
        Err(SprigError::internal("COM_FIELD_LIST has no HTTP form"))
    }

    async fn local_infile_request(&mut self, _file_name: &[u8]) -> Result<()> {
        Err(SprigError::standard(
            3948,
            *b"42000",
            "Loading local data is disabled; this must be enabled on both the client and server sides",
        ))
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn finished(&self) -> bool {
        self.finished
    }
}
