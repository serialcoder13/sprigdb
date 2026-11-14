//! [`WireSink`]: the [`ResultSink`] that encodes a response for the
//! client's capabilities onto its `Framed` connection.
//!
//! Packets are fed into the codec's write buffer; the buffer is flushed to
//! the socket whenever it passes the backpressure boundary (the connection
//! is built with `gateway.relay_buffer_kb`), and every feed waits until it
//! has, so at most that much waits for a slow client.

use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt as _};
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::packets::{EofPacket, OkPacket};
use sprig_proto_mysql::resultset::{
    Terminator, encode_binary_row, encode_column_count, encode_text_row,
};
use sprig_proto_mysql::{Caps, ColumnDef, ErrPacket, PacketCodec, Persona, StatusFlags};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use crate::pipeline::{EndOfRows, ResultSink, RowFormat, RowValues, Wire};

/// Flushes a client connection's write buffer.
pub(crate) async fn flush<S>(framed: &mut Framed<S, PacketCodec>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    <Framed<S, PacketCodec> as SinkExt<Bytes>>::flush(framed)
        .await
        .map_err(SprigError::from)
}

/// Where a response is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Nothing sent, or between results.
    Idle,
    /// Rows of a resultset.
    Rows(RowFormat),
    /// Waiting for the client's `LOCAL INFILE` packets.
    Infile,
    /// The final packet went out.
    Finished,
}

/// The sink of one request.
pub(crate) struct WireSink<'a, S> {
    framed: &'a mut Framed<S, PacketCodec>,
    persona: Persona,
    caps: Caps,
    columns: Vec<ColumnDef>,
    state: State,
    /// The status of the last OK, EOF or terminator sent.
    pub(crate) last_status: Option<StatusFlags>,
}

impl<'a, S> WireSink<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub(crate) fn new(
        framed: &'a mut Framed<S, PacketCodec>,
        persona: Persona,
        caps: Caps,
    ) -> Self {
        Self {
            framed,
            persona,
            caps,
            columns: Vec::new(),
            state: State::Idle,
            last_status: None,
        }
    }

    async fn feed(&mut self, payload: Bytes) -> Result<()> {
        self.framed.feed(payload).await.map_err(SprigError::from)
    }

    fn finish_with(&mut self, status: StatusFlags) {
        self.last_status = Some(status);
        self.state = if status.contains(StatusFlags::MORE_RESULTS_EXISTS) {
            State::Idle
        } else {
            State::Finished
        };
    }
}

#[async_trait]
impl<S> ResultSink for WireSink<'_, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn wire(&self) -> Wire {
        Wire {
            persona: self.persona,
            caps: self.caps,
        }
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.feed(encode_column_count(columns.len() as u64)).await?;
        for c in columns {
            self.feed(c.encode(self.caps)).await?;
        }
        if !self.caps.contains(Caps::DEPRECATE_EOF) {
            let eof = EofPacket {
                warnings: 0,
                status: StatusFlags::AUTOCOMMIT,
            };
            self.feed(eof.encode(self.caps)).await?;
        }
        self.columns = columns.to_vec();
        self.state = State::Rows(format);
        Ok(())
    }

    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        self.feed(encode_column_count(defs.len() as u64)).await?;
        for d in defs {
            self.feed(d.clone()).await?;
        }
        if !self.caps.contains(Caps::DEPRECATE_EOF) {
            let eof = EofPacket {
                warnings: 0,
                status: StatusFlags::AUTOCOMMIT,
            };
            self.feed(eof.encode(self.caps)).await?;
        }
        // Rows are relayed raw: nothing to encode them with.
        self.columns.clear();
        self.state = State::Rows(format);
        Ok(())
    }

    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.feed(encode_column_count(columns.len() as u64)).await?;
        for c in columns {
            self.feed(c.encode(self.caps)).await?;
        }
        // The one packet after the definitions carries CURSOR_EXISTS: an
        // EOF, or under DEPRECATE_EOF the OK-form terminator.
        let t = if self.caps.contains(Caps::DEPRECATE_EOF) {
            Terminator::Ok(OkPacket {
                header: 0xFE,
                status: end.status,
                warnings: end.warnings,
                ..OkPacket::default()
            })
        } else {
            Terminator::Eof(EofPacket {
                warnings: end.warnings,
                status: end.status,
            })
        };
        self.feed(t.encode(self.caps)).await?;
        self.finish_with(end.status);
        Ok(())
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        let payload = match values {
            RowValues::Text(v) => encode_text_row(v.iter().copied()),
            RowValues::Binary(v) => encode_binary_row(v, &self.columns)?,
        };
        self.feed(payload).await
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        self.feed(payload).await
    }

    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        let t = Terminator::Ok(OkPacket {
            header: 0xFE,
            status: end.status,
            warnings: end.warnings,
            ..OkPacket::default()
        });
        self.feed(t.encode(self.caps)).await?;
        self.finish_with(end.status);
        Ok(())
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        let status = ok.status;
        let ok = OkPacket { header: 0, ..ok };
        self.feed(ok.encode(self.caps)).await?;
        self.finish_with(status);
        Ok(())
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        // 3306: MariaDB-only errors as MySQL 8.4's.
        let mapped = sprig_sql::compat::errmap::for_persona(self.persona, err);
        let p = ErrPacket::from_error(self.persona, mapped.as_ref().unwrap_or(err));
        self.feed(p.encode(self.caps)).await?;
        self.state = State::Finished;
        Ok(())
    }

    async fn column_list(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        for c in columns {
            self.feed(c.encode(self.caps)).await?;
        }
        self.state = State::Rows(RowFormat::Text);
        self.end_rows(end).await
    }

    async fn local_infile_request(&mut self, file_name: &[u8]) -> Result<()> {
        let mut p = Vec::with_capacity(1 + file_name.len());
        p.push(0xFB);
        p.extend_from_slice(file_name);
        self.feed(p.into()).await?;
        flush(self.framed).await?;
        self.state = State::Infile;
        Ok(())
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        if self.state != State::Infile {
            return Err(SprigError::internal("LOCAL INFILE read without a request"));
        }
        match self.framed.next().await {
            Some(Ok(p)) => {
                if p.payload.is_empty() {
                    self.state = State::Idle;
                }
                Ok(p.payload)
            }
            Some(Err(e)) => Err(e.into()),
            None => Err(SprigError::standard(
                1158,
                *b"08S01",
                "Got an error reading communication packets",
            )),
        }
    }

    async fn flush(&mut self) -> Result<()> {
        flush(self.framed).await
    }

    fn finished(&self) -> bool {
        self.state == State::Finished
    }
}
