//! The job's [`ChangeStream`] on `sprig-binlog`: groups events into
//! committed transactions, keeps the job's tables and watermarks, sorts
//! `QUERY` events, and converts binlog values to the applier's (chapter 10,
//! "The binlog client", "Filtering and routing").

use std::collections::HashMap;
use std::sync::Arc;

use sprig_binlog::{
    BinlogSource, BinlogStream, Event, Gtid, GtidFlags, GtidPos, RowImage, StreamOptions,
    TableMap, Value as BValue,
};
use sprig_common::ids::JobId;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};

use super::sql::{QueryKind, classify, ddl_touches};
use crate::io::ChangeStream;
use crate::model::{
    RowChange, SourceDdl, SourceTxn, StreamItem, TableMeta, Value, Watermark, WmKind, XaPhase,
};

/// The internal schema and the watermark table.
const SYS: &str = "__sprig_sys";
const WATERMARK: &str = "__sprig_watermark";

/// Converts a binlog value to the applier's (the binary protocol's) form.
#[must_use]
pub fn value(v: &BValue) -> Value {
    match v {
        BValue::Null => Value::NULL,
        BValue::Int(i) => Value::Int(*i),
        BValue::UInt(u) | BValue::Bit(u) | BValue::Set(u) => Value::UInt(*u),
        BValue::Float(f) => Value::Float(*f),
        BValue::Double(d) => Value::Double(*d),
        BValue::Decimal(s) => Value::Bytes(s.clone().into_bytes()),
        BValue::Year(y) => Value::Int(i64::from(*y)),
        BValue::Date(d) => Value::Date(d.year, d.month, d.day, 0, 0, 0, 0),
        BValue::Time(t) => Value::Time(
            t.negative,
            u32::from(t.hours / 24),
            u8::try_from(t.hours % 24).unwrap_or(0),
            t.minutes,
            t.seconds,
            t.micros,
        ),
        BValue::DateTime(d) => Value::Date(
            d.date.year,
            d.date.month,
            d.date.day,
            d.hour,
            d.minute,
            d.second,
            d.micros,
        ),
        BValue::Timestamp(t) => {
            let d = t.to_utc();
            Value::Date(
                d.date.year,
                d.date.month,
                d.date.day,
                d.hour,
                d.minute,
                d.second,
                d.micros,
            )
        }
        BValue::Enum(i) => Value::UInt(u64::from(*i)),
        BValue::String(b) | BValue::Bytes(b) | BValue::Geometry(b) => Value::Bytes(b.clone()),
    }
}

/// Maps a table map's columns onto the job's table, by name when the
/// binlog has names (`binlog_row_metadata = FULL`), else by position.
fn column_map(tm: &TableMap, t: &TableMeta) -> Vec<Option<usize>> {
    let named = tm.columns.iter().all(|c| c.name.is_some());
    t.columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            if named {
                tm.columns
                    .iter()
                    .position(|bc| bc.name.as_deref().is_some_and(|n| n.eq_ignore_ascii_case(&c.name)))
            } else {
                (i < tm.columns.len()).then_some(i)
            }
        })
        .collect()
}

fn row(img: &RowImage, map: &[Option<usize>]) -> Vec<Value> {
    map.iter()
        .map(|m| m.and_then(|i| img.get(i)).map_or(Value::NULL, value))
        .collect()
}

/// A watermark from the after image of a `__sprig_watermark` row.
fn watermark(tm: &TableMap, img: &RowImage) -> Option<Watermark> {
    let col = |name: &str| {
        tm.columns
            .iter()
            .position(|c| c.name.as_deref() == Some(name))
            .and_then(|i| img.get(i))
    };
    // Without names: the table's declared order.
    let at = |name: &str, i: usize| col(name).or_else(|| img.get(i));
    let job = match at("job_id", 0)? {
        BValue::Bytes(b) | BValue::String(b) => {
            let mut a = [0u8; 16];
            let n = b.len().min(16);
            a[..n].copy_from_slice(&b[..n]);
            JobId(uuid::Uuid::from_bytes(a))
        }
        _ => return None,
    };
    let stream = match at("stream", 1)? {
        BValue::UInt(u) => u16::try_from(*u).ok()?,
        BValue::Int(i) => u16::try_from(*i).ok()?,
        _ => return None,
    };
    let kind = match at("kind", 2)? {
        BValue::Enum(1) => WmKind::Low,
        BValue::Enum(2) => WmKind::High,
        BValue::Enum(3) => WmKind::Tick,
        BValue::String(b) => WmKind::parse(b)?,
        _ => return None,
    };
    let seq = match at("seq", 3)? {
        BValue::UInt(u) => *u,
        BValue::Int(i) => u64::try_from(*i).ok()?,
        _ => return None,
    };
    Some(Watermark {
        job,
        stream,
        kind,
        seq,
    })
}

/// The group being read.
#[derive(Debug, Default)]
struct Group {
    gtid: Option<Gtid>,
    flags: GtidFlags,
    xa: Option<String>,
    txn: SourceTxn,
}

/// The stream.
pub struct MysqlStream {
    stream: BinlogStream,
    job: JobId,
    schemas: Vec<String>,
    tables: HashMap<(String, String), Arc<TableMeta>>,
    maps: HashMap<u64, Vec<Option<usize>>>,
    /// Only this GTID domain counts (Galera: the cluster's wsrep domain).
    domain: Option<u32>,
    group: Option<Group>,
}

impl std::fmt::Debug for MysqlStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlStream")
            .field("job", &self.job)
            .finish_non_exhaustive()
    }
}

/// Error 1236 at connect: the source doesn't have the position.
fn diverged(e: SprigError) -> SprigError {
    let text = e.to_string();
    if text.contains("1236") {
        return SprigError::new(SprigCode::MoveSourceDiverged, text);
    }
    match e.kind {
        ErrorKind::Backend { errno: 1236, .. } => {
            SprigError::new(SprigCode::MoveSourceDiverged, text)
        }
        _ => e,
    }
}

impl MysqlStream {
    /// Connects to `src` (as `sprig_repl`) after `from`.
    ///
    /// # Errors
    /// 39134 when the source lacks `from` (1236); the reader's errors.
    pub async fn connect(
        src: &BinlogSource,
        from: &GtidPos,
        server_id: u32,
        job: JobId,
        schemas: Vec<String>,
        tables: &[Arc<TableMeta>],
        domain: Option<u32>,
    ) -> Result<Self> {
        let mut opts = StreamOptions::new(server_id);
        let keep: std::collections::HashSet<String> = schemas.iter().cloned().collect();
        opts.filter = Some(Arc::new(move |schema: &str, table: &str| {
            keep.contains(schema) || (schema == SYS && table == WATERMARK)
        }));
        let stream = BinlogStream::connect(src, from, &opts)
            .await
            .map_err(diverged)?;
        Ok(Self {
            stream,
            job,
            schemas,
            tables: tables
                .iter()
                .map(|t| ((t.schema.clone(), t.name.clone()), t.clone()))
                .collect(),
            maps: HashMap::new(),
            domain,
            group: None,
        })
    }

    fn table_map(&mut self, tm: &TableMap) -> Option<(Arc<TableMeta>, Vec<Option<usize>>)> {
        let t = self.tables.get(&(tm.schema.clone(), tm.table.clone()))?.clone();
        let map = self
            .maps
            .entry(tm.table_id)
            .or_insert_with(|| column_map(tm, &t))
            .clone();
        Some((t, map))
    }

    fn finish(&mut self) -> Option<SourceTxn> {
        let g = self.group.take()?;
        let mut txn = g.txn;
        txn.gtid = g.gtid.filter(|gt| self.domain.is_none_or(|d| d == gt.domain));
        if g.flags.prepared_xa() {
            if let Some(x) = g.xa {
                txn.xa = Some(XaPhase::Prepare(x));
            }
        }
        Some(txn)
    }
}

#[async_trait::async_trait]
impl ChangeStream for MysqlStream {
    async fn next(&mut self) -> Result<StreamItem> {
        loop {
            let ev = self.stream.next().await.map_err(diverged)?;
            match ev {
                Event::Gtid { gtid, flags, xa, .. } => {
                    self.group = Some(Group {
                        gtid: Some(gtid),
                        flags,
                        xa: xa.map(|x| x.to_string()),
                        txn: SourceTxn::default(),
                    });
                }
                Event::TableMap(tm) => {
                    // A new definition gets a new id; drop a stale mapping.
                    self.maps.remove(&tm.table_id);
                }
                Event::Rows { table, rows, .. } => {
                    if table.schema == SYS && table.table == WATERMARK {
                        let job = self.job;
                        let wms: Vec<Watermark> = rows
                            .iter()
                            .filter_map(|r| r.after.as_ref())
                            .filter_map(|img| watermark(&table, img))
                            .filter(|w| w.job == job)
                            .collect();
                        if let Some(g) = self.group.as_mut() {
                            g.txn.watermarks.extend(wms);
                        }
                        continue;
                    }
                    let Some((t, map)) = self.table_map(&table) else {
                        continue;
                    };
                    let Some(g) = self.group.as_mut() else { continue };
                    for r in rows {
                        g.txn.changes.push(RowChange {
                            table: t.clone(),
                            before: r.before.as_ref().map(|i| row(i, &map)),
                            after: r.after.as_ref().map(|i| row(i, &map)),
                        });
                    }
                }
                Event::Query { db, sql, .. } => {
                    let kind = classify(&sql);
                    let standalone = self
                        .group
                        .as_ref()
                        .is_some_and(|g| g.flags.standalone() || g.flags.ddl());
                    match kind {
                        QueryKind::Begin | QueryKind::XaInner => {}
                        QueryKind::Commit => {
                            if let Some(t) = self.finish() {
                                return Ok(StreamItem::Txn(t));
                            }
                        }
                        QueryKind::Rollback => {
                            // A rolled-back group changed nothing.
                            self.group = None;
                        }
                        QueryKind::XaCommit | QueryKind::XaRollback => {
                            let xid = self.group.as_ref().and_then(|g| g.xa.clone());
                            if let (Some(g), Some(x)) = (self.group.as_mut(), xid) {
                                g.txn.xa = Some(if kind == QueryKind::XaCommit {
                                    XaPhase::Commit(x)
                                } else {
                                    XaPhase::Rollback(x)
                                });
                            }
                            if let Some(t) = self.finish() {
                                return Ok(StreamItem::Txn(t));
                            }
                        }
                        QueryKind::Ddl => {
                            if ddl_touches(&db, &sql, &self.schemas)
                                && let Some(g) = self.group.as_mut()
                            {
                                g.txn.ddl = Some(SourceDdl { db, sql });
                            }
                            if standalone && let Some(t) = self.finish() {
                                return Ok(StreamItem::Txn(t));
                            }
                        }
                        QueryKind::Dml => {
                            if self.schemas.iter().any(|s| *s == db) {
                                return Err(SprigError::new(
                                    SprigCode::MoveUnsupported,
                                    format!(
                                        "statement-based DML in moved schema {db}: the source \
                                         must use binlog_format = ROW"
                                    ),
                                ));
                            }
                        }
                        QueryKind::Other => {
                            if standalone && let Some(t) = self.finish() {
                                return Ok(StreamItem::Txn(t));
                            }
                        }
                    }
                }
                Event::Xid(_) => {
                    if let Some(t) = self.finish() {
                        return Ok(StreamItem::Txn(t));
                    }
                }
                Event::XaPrepare { one_phase, .. } => {
                    if one_phase && let Some(g) = self.group.as_mut() {
                        g.flags = GtidFlags::default();
                    }
                    if let Some(t) = self.finish() {
                        return Ok(StreamItem::Txn(t));
                    }
                }
                Event::Incident(m) => {
                    return Err(SprigError::new(
                        SprigCode::MoveSourceDiverged,
                        format!("the source lost binlog events: {m}"),
                    ));
                }
                Event::Heartbeat | Event::DumpEnd => {
                    if self.group.is_none() {
                        return Ok(StreamItem::Idle);
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use sprig_binlog::{Date, DateTime, Time, Timestamp};

    use super::*;

    #[test]
    fn values_convert_like_the_binary_protocol() {
        assert_eq!(value(&BValue::Enum(3)), Value::UInt(3));
        assert_eq!(value(&BValue::Decimal("-1.50".into())), Value::Bytes(b"-1.50".to_vec()));
        let d = Date { year: 2026, month: 11, day: 8 };
        assert_eq!(value(&BValue::Date(d)), Value::Date(2026, 11, 8, 0, 0, 0, 0));
        let dt = DateTime { date: d, hour: 1, minute: 2, second: 3, micros: 4, fsp: 6 };
        assert_eq!(value(&BValue::DateTime(dt)), Value::Date(2026, 11, 8, 1, 2, 3, 4));
        let t = Time { negative: true, hours: 49, minutes: 1, seconds: 2, micros: 3, fsp: 6 };
        assert_eq!(value(&BValue::Time(t)), Value::Time(true, 2, 1, 1, 2, 3));
        let ts = Timestamp { secs: 86_400, micros: 0, fsp: 0 };
        assert_eq!(value(&BValue::Timestamp(ts)), Value::Date(1970, 1, 2, 0, 0, 0, 0));
    }
}
