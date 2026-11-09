//! Reading a stream (chapter 45, "SQL": "A stream reads like a table with
//! the columns `partition_no`, `offset_no`, `ts`, `key`, `value` and
//! `headers`"), and the log reads the Kafka front end will use.
//!
//! **SQL.** Each stream named in a `FROM` becomes a derived table over its
//! log, keeping the name as its alias:
//!
//! ```sql
//! SELECT partition_no, offset_no, ts, `key`, `value` FROM order_events
//!  WHERE partition_no = 3 AND offset_no >= 1200 ORDER BY offset_no LIMIT 100
//! -- becomes
//! SELECT partition_no, offset_no, ts, `key`, `value` FROM (SELECT partition_no,
//!   offset_no, ts, k AS `key`, v AS `value`, headers FROM `__sprig_sl_41`)
//!   AS order_events WHERE ...
//! ```
//!
//! MariaDB merges the derived table, so the log's primary key
//! `(partition_no, offset_no, day)` serves partition and offset ranges
//! (`decisions/spike-stream-log.md`). `key` is a reserved word in MariaDB:
//! bare `key` and `value` words of a statement that reads a stream are
//! quoted before it is parsed.

use std::ops::ControlFlow;

use mysql_async::prelude::Queryable;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_sql::lex::{LexOptions, TokenKind, lex};
use sqlparser::ast::{ObjectNamePart, Statement, TableFactor, VisitMut, VisitorMut};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserOptions};

use crate::model::{
    CKPT_TABLE, LeaderEpoch, Offset, PartitionNo, Record, SHARD_ROW, StreamId, log_table,
};

/// Quotes bare `key` and `value` words (not index hints such as `USE KEY`
/// or `PRIMARY KEY`).
#[must_use]
pub fn quote_columns(sql: &str, lex_opts: &LexOptions) -> String {
    let Ok(toks) = lex(sql.as_bytes(), lex_opts) else {
        return sql.to_owned();
    };
    let mut out = String::with_capacity(sql.len() + 8);
    let mut last = 0;
    let mut prev_word: Option<String> = None;
    for t in &toks {
        if t.kind.is_trivia() {
            continue;
        }
        let text = &sql[t.range()];
        if t.kind == TokenKind::Word
            && (text.eq_ignore_ascii_case("key") || text.eq_ignore_ascii_case("value"))
            && !prev_word.as_deref().is_some_and(|p| {
                matches!(
                    p,
                    "USE" | "FORCE" | "IGNORE" | "PRIMARY" | "UNIQUE" | "FOREIGN" | "DUPLICATE"
                )
            })
        {
            out.push_str(&sql[last..t.start as usize]);
            out.push('`');
            out.push_str(&text.to_ascii_lowercase());
            out.push('`');
            last = t.end as usize;
        }
        prev_word = (t.kind == TokenKind::Word).then(|| text.to_ascii_uppercase());
    }
    out.push_str(&sql[last..]);
    out
}

/// The derived table reading stream `id`'s log (`db` qualifies it).
#[must_use]
pub fn derived_sql(db: Option<&str>, id: StreamId) -> String {
    let table = match db {
        Some(d) => format!("{}.{}", quote(d), quote(&log_table(id))),
        None => quote(&log_table(id)),
    };
    format!("(SELECT partition_no, offset_no, ts, k AS `key`, v AS `value`, headers FROM {table})")
}

fn parse(sql: &str) -> Option<Vec<Statement>> {
    Parser::new(&MySqlDialect {})
        .with_options(ParserOptions::new().with_unescape(false))
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_statements())
        .ok()
}

/// A stream a read names: `(db as written, name)`.
pub type Lookup<'a> = dyn Fn(Option<&str>, &str) -> Option<StreamId> + 'a;

struct Swap<'a> {
    lookup: &'a Lookup<'a>,
    hits: Vec<(Option<String>, String, StreamId)>,
}

fn part(p: &ObjectNamePart) -> Option<String> {
    p.as_ident().map(|i| i.value.clone())
}

impl VisitorMut for Swap<'_> {
    type Break = ();

    fn post_visit_table_factor(&mut self, tf: &mut TableFactor) -> ControlFlow<()> {
        let TableFactor::Table {
            name, alias, args, ..
        } = tf
        else {
            return ControlFlow::Continue(());
        };
        if args.is_some() {
            return ControlFlow::Continue(());
        }
        let parts: Vec<String> = name.0.iter().filter_map(part).collect();
        let (db, n) = match parts.as_slice() {
            [n] => (None, n.clone()),
            [d, n] => (Some(d.clone()), n.clone()),
            _ => return ControlFlow::Continue(()),
        };
        let Some(id) = (self.lookup)(db.as_deref(), &n) else {
            return ControlFlow::Continue(());
        };
        let alias_text = alias.as_ref().map_or_else(
            || quote(&n),
            |a| {
                let t = a.to_string();
                t.strip_prefix("AS ").map_or(t.clone(), str::to_owned)
            },
        );
        let snippet = format!(
            "SELECT * FROM {} AS {alias_text}",
            derived_sql(db.as_deref(), id)
        );
        let Some(mut stmts) = parse(&snippet) else {
            return ControlFlow::Break(());
        };
        let Some(Statement::Query(q)) = stmts.pop() else {
            return ControlFlow::Break(());
        };
        let sqlparser::ast::SetExpr::Select(sel) = *q.body else {
            return ControlFlow::Break(());
        };
        let Some(from) = sel.from.into_iter().next() else {
            return ControlFlow::Break(());
        };
        *tf = from.relation;
        self.hits.push((db, n, id));
        ControlFlow::Continue(())
    }
}

/// A rewritten read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rewritten {
    /// The statement with derived tables over the logs.
    pub sql: String,
    /// The streams read: `(db as written, name, id)`.
    pub streams: Vec<(Option<String>, String, StreamId)>,
}

/// Rewrites a statement whose `FROM`s name streams; `None` when it names
/// none (or doesn't parse: MariaDB then answers it).
///
/// # Errors
/// 1235 for a statement that names a stream but can't be rewritten.
pub fn rewrite_select(
    sql: &str,
    lex_opts: &LexOptions,
    lookup: &Lookup<'_>,
) -> Result<Option<Rewritten>> {
    let quoted = quote_columns(sql, lex_opts);
    let Some(mut stmts) = parse(&quoted) else {
        return Ok(None);
    };
    if stmts.len() != 1 {
        return Ok(None);
    }
    let mut v = Swap {
        lookup,
        hits: Vec::new(),
    };
    if stmts[0].visit(&mut v).is_break() {
        return Err(SprigError::standard(
            1235,
            *b"42000",
            "This version of SprigDB doesn't yet support this statement on a stream",
        ));
    }
    if v.hits.is_empty() {
        return Ok(None);
    }
    Ok(Some(Rewritten {
        sql: stmts[0].to_string(),
        streams: v.hits,
    }))
}

/// Limits of one read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadLimits {
    /// Records at most.
    pub max_records: u32,
    /// Bytes at most (the first record is returned whatever its size).
    pub max_bytes: u64,
}

/// A partition's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    /// The first offset still retained.
    pub log_start: Offset,
    /// The high watermark: the next offset.
    pub high_watermark: Offset,
    /// The epoch of the last append.
    pub leader_epoch: LeaderEpoch,
}

/// A batch of records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadBatch {
    /// The records, in offset order (with gaps after compaction).
    pub records: Vec<Record>,
    /// The partition's bounds at the read.
    pub bounds: Bounds,
}

/// Reads of one log shard schema, for chapter 45's `StreamLog::read` and
/// `high_watermark` (the Kafka front end and HTTP consumers build on it).
#[derive(Debug, Clone)]
pub struct LogReader {
    /// The log shard schema.
    pub schema: String,
}

impl LogReader {
    /// A partition's bounds.
    ///
    /// # Errors
    /// 39940 for an unknown partition; the backend's.
    pub async fn bounds<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        s: StreamId,
        p: PartitionNo,
    ) -> Result<Bounds> {
        let row: Option<(u64, u64, u32)> = q
            .exec_first(
                format!(
                    "SELECT log_start, next_offset, leader_epoch FROM {}.{} \
                     WHERE stream_id = ? AND partition_no = ?",
                    quote(&self.schema),
                    quote(CKPT_TABLE)
                ),
                (s.0, p.0),
            )
            .await
            .map_err(crate::maria::map_err)?;
        let (start, hw, epoch) = row.ok_or_else(|| {
            SprigError::new(
                SprigCode::StreamNotFound,
                format!("stream {} has no partition {}", s.0, p.0),
            )
        })?;
        let _ = SHARD_ROW;
        Ok(Bounds {
            log_start: Offset(start),
            high_watermark: Offset(hw),
            leader_epoch: LeaderEpoch(epoch),
        })
    }

    /// Records of partition `p` from offset `from` (chapter 45's fetch,
    /// steps 2 and 3).
    ///
    /// # Errors
    /// 39947 when `from` is below the log start or above the high
    /// watermark; the backend's.
    pub async fn read<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        s: StreamId,
        p: PartitionNo,
        from: Offset,
        lim: ReadLimits,
    ) -> Result<ReadBatch> {
        let bounds = self.bounds(q, s, p).await?;
        if from < bounds.log_start || from > bounds.high_watermark {
            return Err(SprigError::new(
                SprigCode::StreamOffsetOutOfRange,
                format!(
                    "offset {} is outside [{}, {}] of partition {}",
                    from.0, bounds.log_start.0, bounds.high_watermark.0, p.0
                ),
            ));
        }
        let rows: Vec<(
            u64,
            i64,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<String>,
            u32,
        )> = q
            .exec(
                format!(
                    "SELECT offset_no, CAST(UNIX_TIMESTAMP(ts) * 1000 AS SIGNED), k, v, \
                     CAST(headers AS CHAR), leader_epoch FROM {}.{} \
                     WHERE partition_no = ? AND offset_no >= ? AND offset_no < ? \
                     ORDER BY offset_no LIMIT ?",
                    quote(&self.schema),
                    quote(&log_table(s))
                ),
                (p.0, from.0, bounds.high_watermark.0, lim.max_records.max(1)),
            )
            .await
            .map_err(crate::maria::map_err)?;
        let mut records = Vec::with_capacity(rows.len());
        let mut bytes = 0u64;
        for (o, ts, k, v, h, e) in rows {
            let size = (k.as_ref().map_or(0, Vec::len) + v.as_ref().map_or(0, Vec::len)) as u64;
            if !records.is_empty() && bytes + size > lim.max_bytes {
                break;
            }
            bytes += size;
            let headers = h
                .and_then(|h| {
                    serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&h).ok()
                })
                .map(|m| {
                    m.into_iter()
                        .map(|(k, v)| {
                            let b = match v {
                                serde_json::Value::String(s) => s.into_bytes(),
                                other => other.to_string().into_bytes(),
                            };
                            (k, b)
                        })
                        .collect()
                })
                .unwrap_or_default();
            records.push(Record {
                partition: p,
                offset: Offset(o),
                ts_ms: ts,
                key: k,
                value: v,
                headers,
                leader_epoch: LeaderEpoch(e),
            });
        }
        Ok(ReadBatch { records, bounds })
    }

    /// Waits until partition `p`'s high watermark passes `o` or `deadline`
    /// comes (chapter 45's `wait_beyond`, by polling the checkpoint every
    /// `every`; the watermark signal from the sequencer will replace the
    /// polling for the Kafka front end).
    ///
    /// # Errors
    /// The backend's.
    pub async fn wait_beyond<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        s: StreamId,
        p: PartitionNo,
        o: Offset,
        deadline: tokio::time::Instant,
        every: std::time::Duration,
    ) -> Result<Bounds> {
        loop {
            let b = self.bounds(q, s, p).await?;
            if b.high_watermark > o || tokio::time::Instant::now() >= deadline {
                return Ok(b);
            }
            tokio::time::sleep(every.min(deadline - tokio::time::Instant::now())).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn look(db: Option<&str>, n: &str) -> Option<StreamId> {
        (db.is_none_or(|d| d == "shop") && n == "order_events").then_some(StreamId(41))
    }

    #[test]
    fn keys_are_quoted() {
        let o = LexOptions::mariadb();
        assert_eq!(
            quote_columns("SELECT key, value FROM s USE KEY (x)", &o),
            "SELECT `key`, `value` FROM s USE KEY (x)"
        );
        assert_eq!(
            quote_columns("SELECT 'key' FROM s", &o),
            "SELECT 'key' FROM s"
        );
    }

    #[test]
    fn reads_become_derived_tables() {
        let o = LexOptions::mariadb();
        let r = rewrite_select(
            "SELECT partition_no, offset_no, ts, key, value FROM order_events \
             WHERE partition_no = 3 AND offset_no >= 1200 ORDER BY offset_no LIMIT 100",
            &o,
            &look,
        )
        .unwrap()
        .unwrap();
        assert!(r.sql.contains("FROM (SELECT partition_no, offset_no, ts, k AS `key`, v AS `value`, headers FROM `__sprig_sl_41`) AS `order_events`"), "{}", r.sql);
        assert!(
            r.sql
                .contains("SELECT partition_no, offset_no, ts, `key`, `value` FROM")
        );
        assert_eq!(r.streams, vec![(None, "order_events".into(), StreamId(41))]);
        let r = rewrite_select(
            "SELECT e.offset_no, o.status FROM shop.order_events AS e JOIN orders o ON o.id = e.offset_no",
            &o,
            &look,
        )
        .unwrap()
        .unwrap();
        assert!(
            r.sql
                .contains("FROM `shop`.`__sprig_sl_41`) AS e JOIN orders"),
            "{}",
            r.sql
        );
        assert_eq!(
            rewrite_select("SELECT order_events FROM t", &o, &look).unwrap(),
            None
        );
        assert_eq!(rewrite_select("SELECT 1 FROM", &o, &look).unwrap(), None);
    }
}
