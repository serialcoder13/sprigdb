//! Streamed Bolt results: the statement runs in its own task on its
//! connection and hands rows through a bounded channel; `PULL` takes them
//! as the client asks, so a slow client pauses the statement rather than
//! the gateway buffering its answer. The channel holds at most
//! [`CHANNEL_ROWS`] rows.
//!
//! Dropping the [`GwStream`] (`DISCARD`, `RESET`, a new auto-commit `RUN`,
//! the connection closing) closes the channel: the producer stops at its
//! next row and ends its transaction, or, inside an explicit transaction,
//! hands the connection back to the session.

use std::collections::BTreeMap;

use async_trait::async_trait;
use sprig_common::Result;
use sprig_graph::RowSink;
use sprig_graph::value::Value;
use sprig_proto_bolt::PackValue;
use sprig_proto_bolt::server::{BoltFailure, RecordStream, StreamEnd};
use tokio::sync::mpsc;

use super::to_pack;

/// Rows the channel holds before the statement waits for the client.
pub(super) const CHANNEL_ROWS: usize = 512;

/// What the producer sends.
pub(super) enum Msg {
    /// The columns, first.
    Fields(Vec<String>),
    /// A record.
    Row(Vec<PackValue>),
    /// The end: the summary or the failure.
    End(std::result::Result<StreamEnd, BoltFailure>),
}

/// The producer's sink: rows into the channel.
pub(super) struct ChanSink {
    pub(super) tx: mpsc::Sender<Msg>,
}

#[async_trait]
impl RowSink for ChanSink {
    async fn columns(&mut self, cols: &[String]) -> Result<()> {
        let _ = self.tx.send(Msg::Fields(cols.to_vec())).await;
        Ok(())
    }

    async fn row(&mut self, row: Vec<Value>) -> Result<bool> {
        let rec: Vec<PackValue> = row.iter().map(to_pack).collect();
        Ok(self.tx.send(Msg::Row(rec)).await.is_ok())
    }
}

/// The consumer side, held by the Bolt connection's result.
pub(super) struct GwStream {
    pub(super) rx: mpsc::Receiver<Msg>,
    pub(super) end: Option<StreamEnd>,
}

#[async_trait]
impl RecordStream for GwStream {
    async fn next(&mut self) -> std::result::Result<Option<Vec<PackValue>>, BoltFailure> {
        loop {
            return match self.rx.recv().await {
                Some(Msg::Row(r)) => Ok(Some(r)),
                Some(Msg::Fields(_)) => continue,
                Some(Msg::End(Ok(e))) => {
                    self.end = Some(e);
                    Ok(None)
                }
                Some(Msg::End(Err(f))) => Err(f),
                None => Err(BoltFailure::new(
                    "Neo.DatabaseError.General.UnknownError",
                    "the statement ended without a summary",
                )),
            };
        }
    }

    fn end(&mut self) -> StreamEnd {
        self.end.take().unwrap_or_default()
    }
}

/// The first messages of a stream: its columns, then either the first
/// record (the rest stream) or the whole answer's end.
pub(super) enum Start {
    /// Columns, the first record, and the stream for the rest.
    Rows(Vec<String>, Vec<PackValue>, GwStream),
    /// Columns and the summary: no rows.
    Done(Vec<String>, StreamEnd),
}

/// Waits for a stream's start, so a statement that fails before its first
/// row fails its `RUN`.
///
/// # Errors
/// The statement's failure.
pub(super) async fn start(mut rx: mpsc::Receiver<Msg>) -> std::result::Result<Start, BoltFailure> {
    let lost = || {
        BoltFailure::new(
            "Neo.DatabaseError.General.UnknownError",
            "the statement ended without a summary",
        )
    };
    let fields = match rx.recv().await {
        Some(Msg::Fields(f)) => f,
        Some(Msg::End(Err(f))) => return Err(f),
        Some(Msg::End(Ok(e))) => return Ok(Start::Done(Vec::new(), e)),
        Some(Msg::Row(_)) | None => return Err(lost()),
    };
    match rx.recv().await {
        Some(Msg::Row(r)) => Ok(Start::Rows(fields, r, GwStream { rx, end: None })),
        Some(Msg::End(Ok(e))) => Ok(Start::Done(fields, e)),
        Some(Msg::End(Err(f))) => Err(f),
        Some(Msg::Fields(_)) | None => Err(lost()),
    }
}

/// The summary of a finished statement.
pub(super) fn summary(stats: BTreeMap<String, i64>, bookmark: Option<String>) -> StreamEnd {
    StreamEnd { stats, bookmark }
}

/// The rest of a result whose top `RETURN` failed on a row:
/// after the records before that row, `PULL` gets the failure, as with
/// Neo4j's lazy results.
pub(super) struct LateError(pub(super) Option<BoltFailure>);

#[async_trait]
impl RecordStream for LateError {
    async fn next(&mut self) -> std::result::Result<Option<Vec<PackValue>>, BoltFailure> {
        match self.0.take() {
            Some(f) => Err(f),
            None => Ok(None),
        }
    }

    fn end(&mut self) -> StreamEnd {
        StreamEnd {
            stats: BTreeMap::new(),
            bookmark: None,
        }
    }
}
