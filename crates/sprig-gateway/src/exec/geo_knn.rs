//! Nearest neighbours on the MySQL persona (chapter 36, "Nearest
//! neighbours", `nearest()`; R6 round 3).
//!
//! `SELECT ... FROM t [WHERE w] ORDER BY ST_Distance(col, <SRID 4326
//! point>) LIMIT k` on one table ([`sprig_sql::compat::geo::knn`]) runs
//! with a growing radius: from `geo.knn_initial_radius_m` (1 km), times
//! four, a probe counts the rows within `r` (the box pass turns
//! `ST_Distance(...) <= r` into an R-tree read of the column's
//! `SPATIAL INDEX` plus the exact Andoyer test); at the first `r` holding
//! `k` rows, or past the earth's half circumference, the statement runs
//! with that radius and its rows go to the client. Every point within `r`
//! is a candidate, so the `k` nearest are among them. Probes and the final
//! statement go through the whole tenant path (names, privileges, router),
//! so a failing probe falls back to the statement as sent.

use async_trait::async_trait;
use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::ColumnDef;
use sprig_sql::lex::LexOptions;

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};

/// `geo.knn_initial_radius_m`.
const INITIAL_RADIUS_M: f64 = 1000.0;
/// Past this, a circle covers the earth: the statement runs as sent.
const EARTH_M: f64 = 20_100_000.0;

/// Reads a probe's `COUNT(*)`.
struct Count {
    wire: Wire,
    columns: usize,
    n: Option<u64>,
    failed: bool,
    done: bool,
}

#[async_trait]
impl ResultSink for Count {
    fn wire(&self) -> Wire {
        self.wire
    }
    async fn begin_rows(&mut self, columns: &[ColumnDef], _format: RowFormat) -> Result<()> {
        self.columns = columns.len();
        Ok(())
    }
    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        if let RowValues::Text(v) = values {
            self.n = v
                .first()
                .copied()
                .flatten()
                .and_then(|c| std::str::from_utf8(c).ok())
                .and_then(|s| s.parse().ok());
        }
        Ok(())
    }
    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        let r = sprig_proto_mysql::resultset::decode_text_row(&payload, self.columns.max(1))?;
        self.n = r
            .first()
            .copied()
            .flatten()
            .and_then(|c| std::str::from_utf8(c).ok())
            .and_then(|s| s.parse().ok());
        Ok(())
    }
    async fn end_rows(&mut self, _end: EndOfRows) -> Result<()> {
        self.done = true;
        Ok(())
    }
    async fn ok(&mut self, _ok: OkPacket) -> Result<()> {
        self.done = true;
        Ok(())
    }
    async fn error(&mut self, _err: &SprigError) -> Result<()> {
        self.failed = true;
        self.done = true;
        Ok(())
    }
    async fn column_list(&mut self, _c: &[ColumnDef], _e: EndOfRows) -> Result<()> {
        Ok(())
    }
    async fn local_infile_request(&mut self, _f: &[u8]) -> Result<()> {
        Err(SprigError::internal("LOAD DATA in a nearest-neighbour probe"))
    }
    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn finished(&self) -> bool {
        self.done
    }
}

impl PassSession {
    /// Runs a nearest-neighbour statement with a growing radius; `None`
    /// when `text` isn't one.
    pub(super) async fn tenant_geo_knn(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        lex: &LexOptions,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some(k) = sprig_sql::compat::geo::knn(text, lex) else {
            return Ok(None);
        };
        let mut r = INITIAL_RADIUS_M;
        let mut rounds = 0u32;
        let chosen = loop {
            rounds += 1;
            if r >= EARTH_M {
                break None;
            }
            let probe = Bytes::from(k.probe(text, r));
            let mut c = Count {
                wire: out.wire(),
                columns: 0,
                n: None,
                failed: false,
                done: false,
            };
            Box::pin(self.tenant_text(cx, &probe, attrs, false, RowFormat::Text, &mut c)).await?;
            match c.n {
                Some(n) if !c.failed && n >= k.k => break Some(r),
                Some(_) if !c.failed => r *= 4.0,
                _ => {
                    tracing::debug!("nearest-neighbour probe failed; running the statement as sent");
                    break None;
                }
            }
        };
        metrics::histogram!("sprig_geo_knn_rounds").record(f64::from(rounds));
        let text = match chosen {
            Some(r) => Bytes::from(k.with_radius(text, r)),
            None => {
                let mut t = sprig_sql::compat::geo::KNN_MARK.as_bytes().to_vec();
                t.extend_from_slice(text);
                Bytes::from(t)
            }
        };
        Box::pin(self.tenant_text(cx, &text, attrs, more, format, out))
            .await
            .map(Some)
    }
}
