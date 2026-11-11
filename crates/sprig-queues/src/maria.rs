//! The MariaDB message store (chapter 50, "Shard tables"; build step 4):
//! `__sprig_qm` (messages), `__sprig_qs` (owner state per lane),
//! `__sprig_qd` (delayed messages), `__sprig_qo` (the queue outbox) and
//! `__sprig_qf` (the forward ledger, R9 r9fwd) in one shard schema.
//!
//! **As built, against the book:**
//! - The tables live in the vhost database's default shard (its first
//!   shard), as a stream's log does: no separate queue group is created.
//! - Times are epoch milliseconds in `BIGINT` columns (`expires_at_ms`,
//!   `due_ms`), not `DATETIME(3)`.
//! - `deaths` is the AMQP-encoded `x-death` array (`BLOB`), so the
//!   headers round-trip exactly.
//! - `__sprig_qs.writes` counts batches: every batch changes the row, so
//!   a zero row count means only "fenced", never "nothing changed".
//! - Bodies are not chunked into `__sprig_qb`: a body is one `LONGBLOB`
//!   (messages are capped by `amqp.max_message_bytes`, 16 MiB); a batch
//!   splits its inserts so one statement stays under 4 MiB.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use mysql_async::prelude::Queryable;
use mysql_async::{Params, Pool, TxOpts, Value};
use sprig_common::names::quote;

use crate::store::{
    Claim, DelayedRow, LaneState, MsgId, MsgStore, OutDest, OutKind, OutboxRow, StoreError,
    StoredMsg, WriteBatch,
};

/// The shard tables, created on first use.
pub const DDL: [&str; 5] = [
    "CREATE TABLE IF NOT EXISTS {s}.`__sprig_qm` (
  queue_id       BIGINT UNSIGNED NOT NULL,
  lane           SMALLINT UNSIGNED NOT NULL,
  pclass         TINYINT UNSIGNED NOT NULL,
  seq            BIGINT UNSIGNED NOT NULL,
  msg_id         BINARY(16) NOT NULL,
  fmt            TINYINT UNSIGNED NOT NULL,
  exchange       VARBINARY(255) NOT NULL,
  routing_key    VARBINARY(255) NOT NULL,
  props          BLOB NOT NULL,
  body           LONGBLOB NOT NULL,
  body_bytes     BIGINT UNSIGNED NOT NULL,
  expires_at_ms  BIGINT NULL,
  delivery_count INT UNSIGNED NOT NULL DEFAULT 0,
  deaths         BLOB NULL,
  enq_at         DATETIME(3) NOT NULL,
  PRIMARY KEY (queue_id, lane, pclass, seq),
  UNIQUE KEY uk_msg (queue_id, msg_id)
) ENGINE=InnoDB",
    "CREATE TABLE IF NOT EXISTS {s}.`__sprig_qs` (
  queue_id       BIGINT UNSIGNED NOT NULL,
  lane           SMALLINT UNSIGNED NOT NULL,
  owner_epoch    BIGINT UNSIGNED NOT NULL,
  next_seq       BIGINT UNSIGNED NOT NULL,
  delivered_hwm  BIGINT UNSIGNED NOT NULL,
  ready_count    BIGINT UNSIGNED NOT NULL,
  ready_bytes    BIGINT UNSIGNED NOT NULL,
  writes         BIGINT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (queue_id, lane)
) ENGINE=InnoDB",
    "CREATE TABLE IF NOT EXISTS {s}.`__sprig_qd` (
  exchange_id    BIGINT UNSIGNED NOT NULL,
  exchange       VARBINARY(255) NOT NULL,
  due_ms         BIGINT NOT NULL,
  msg_id         BINARY(16) NOT NULL,
  routing_key    VARBINARY(255) NOT NULL,
  fmt            TINYINT UNSIGNED NOT NULL,
  props          BLOB NOT NULL,
  body           LONGBLOB NOT NULL,
  PRIMARY KEY (due_ms, msg_id),
  KEY k_exchange (exchange_id)
) ENGINE=InnoDB",
    "CREATE TABLE IF NOT EXISTS {s}.`__sprig_qo` (
  out_id         BINARY(16) NOT NULL,
  kind           ENUM('publish','dead_letter','delayed','inherited') NOT NULL,
  dest           JSON NOT NULL,
  msg_id         BINARY(16) NOT NULL,
  fmt            TINYINT UNSIGNED NOT NULL,
  exchange       VARBINARY(255) NOT NULL,
  routing_key    VARBINARY(255) NOT NULL,
  props          BLOB NOT NULL,
  body           LONGBLOB NOT NULL,
  deaths         BLOB NULL,
  PRIMARY KEY (out_id)
) ENGINE=InnoDB",
    // The forward ledger (R9, r9fwd): one row per message an outbox
    // forward enqueued, written in the owner's batch with the message, so
    // a forward replayed after the message was consumed stays a no-op.
    // Rows go once their outbox row is gone and they are old enough
    // (`MsgStore::purge_ledger`).
    "CREATE TABLE IF NOT EXISTS {s}.`__sprig_qf` (
  queue_id       BIGINT UNSIGNED NOT NULL,
  out_id         BINARY(16) NOT NULL,
  at_ms          BIGINT NOT NULL,
  PRIMARY KEY (queue_id, out_id),
  KEY k_at (at_ms)
) ENGINE=InnoDB",
];

/// The statements that create the tables in `schema`.
#[must_use]
pub fn ddl(schema: &str) -> Vec<String> {
    DDL.iter()
        .map(|d| d.replace("{s}", &quote(schema)))
        .collect()
}

/// The insert of one outbox row into `schema` (a transactional publish
/// runs it in the client's own transaction).
#[must_use]
pub fn outbox_insert(schema: &str, r: &OutboxRow) -> (String, Vec<Value>) {
    (
        format!(
            "INSERT INTO {}.`__sprig_qo` (out_id, kind, dest, msg_id, fmt, exchange, routing_key, \
             props, body, deaths) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            quote(schema)
        ),
        outbox_params(r),
    )
}

fn outbox_params(r: &OutboxRow) -> Vec<Value> {
    vec![
        r.out_id.to_vec().into(),
        r.kind.as_str().into(),
        serde_json::to_string(&r.dest).unwrap_or_default().into(),
        r.msg_id.to_vec().into(),
        r.fmt.into(),
        r.exchange.as_bytes().into(),
        r.routing_key.as_bytes().into(),
        r.props.to_vec().into(),
        r.body.to_vec().into(),
        r.deaths.as_ref().map(|d| d.to_vec()).into(),
    ]
}

fn id16(v: &[u8]) -> MsgId {
    let mut a = [0u8; 16];
    let n = v.len().min(16);
    a[..n].copy_from_slice(&v[..n]);
    a
}

/// The store of one shard schema.
#[derive(Clone)]
pub struct MariaStore {
    pool: Pool,
    /// `sprig_ctl` pools of the cluster's replicas (`queue.confirm_mode =
    /// replica`).
    replicas: Vec<Pool>,
    /// Confirm only after a replica applied the commit.
    confirm_replica: Arc<std::sync::atomic::AtomicBool>,
    schema: String,
    q: String,
    /// The schema moved away (a mover renamed its tables, chapter 10's
    /// fence) or is fenced: the gateway opens the store again where the
    /// tenant's map points now.
    stale: Arc<std::sync::atomic::AtomicBool>,
}

impl std::fmt::Debug for MariaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MariaStore")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

const MAX_STMT_BYTES: usize = 4 << 20;
const CHUNK: usize = 64;

impl MariaStore {
    /// The store of `schema`, over `pool` (the cluster's `sprig_ctl`
    /// pool).
    #[must_use]
    pub fn new(pool: Pool, schema: impl Into<String>) -> Self {
        let schema = schema.into();
        let q = quote(&schema);
        Self {
            pool,
            replicas: Vec::new(),
            confirm_replica: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            schema,
            q,
            stale: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// The cluster's replicas, for `queue.confirm_mode = replica`.
    #[must_use]
    pub fn with_replicas(mut self, replicas: Vec<Pool>) -> Self {
        self.replicas = replicas;
        self
    }

    /// Turns `queue.confirm_mode = replica` on or off.
    pub fn set_confirm_replica(&self, on: bool) {
        self.confirm_replica
            .store(on, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether confirms wait for a replica (on, and the cluster has
    /// replicas).
    #[must_use]
    pub fn confirms_on_replica(&self) -> bool {
        !self.replicas.is_empty()
            && self
                .confirm_replica
                .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// `queue.confirm_mode = replica` (chapter 50, "Confirms"): after a
    /// commit on `c`, waits until one replica has applied it
    /// (`MASTER_GTID_WAIT` on the session's `@@last_gtid`). A replica that
    /// doesn't within 10 s fails the write: the owner stops and the
    /// publishers get nacks (they republish), since the confirm's promise
    /// can't be kept.
    async fn wait_replica(&self, c: &mut mysql_async::Conn) -> Result<(), StoreError> {
        if !self.confirms_on_replica() {
            return Ok(());
        }
        let gtid: Option<String> = c
            .query_first("SELECT @@last_gtid")
            .await
            .map_err(|e| self.fail(&e))?;
        let Some(gtid) = gtid.filter(|g| !g.is_empty()) else {
            return Ok(());
        };
        let started = std::time::Instant::now();
        for p in &self.replicas {
            let Ok(mut rc) = p.get_conn().await else {
                continue;
            };
            let r: Result<Option<i64>, _> = rc
                .exec_first("SELECT MASTER_GTID_WAIT(?, 10)", (gtid.as_str(),))
                .await;
            if matches!(r, Ok(Some(0))) {
                metrics::histogram!("sprig_queue_replica_wait_seconds")
                    .record(started.elapsed().as_secs_f64());
                return Ok(());
            }
        }
        Err(StoreError::Failed(format!(
            "no replica applied {gtid} within 10 s (queue.confirm_mode = replica)"
        )))
    }

    /// A backend error; one that says the tables are gone (renamed by a
    /// mover's fence, or the schema dropped after a move) or not ours to
    /// write marks the store stale.
    fn fail(&self, e: &mysql_async::Error) -> StoreError {
        if let mysql_async::Error::Server(se) = e
            && matches!(se.code, 1049 | 1146 | 1142 | 1044)
        {
            self.stale.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        StoreError::Failed(e.to_string())
    }

    /// The schema.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Creates the tables if missing.
    ///
    /// # Errors
    /// A backend error.
    pub async fn ensure(&self) -> Result<(), StoreError> {
        let mut c = self.pool.get_conn().await.map_err(|e| self.fail(&e))?;
        // A schema a mover has fenced (its tables renamed to
        // `__sprig_f_*`) must not get fresh, empty queue tables: writes
        // there would be lost when the move commits.
        let fenced: Option<u64> = c
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?                  AND TABLE_NAME LIKE '\\_\\_sprig\\_f\\_%'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        if fenced.unwrap_or(0) > 0 {
            self.stale.store(true, std::sync::atomic::Ordering::Relaxed);
            return Err(StoreError::Failed(format!(
                "schema {} is fenced by a shard move",
                self.schema
            )));
        }
        for s in ddl(&self.schema) {
            c.query_drop(s).await.map_err(|e| self.fail(&e))?;
        }
        Ok(())
    }

    fn t(&self, table: &str) -> String {
        format!("{}.`{table}`", self.q)
    }

    async fn conn(&self) -> Result<mysql_async::Conn, StoreError> {
        self.pool.get_conn().await.map_err(|e| self.fail(&e))
    }
}

fn msg_of(r: mysql_async::Row) -> Option<StoredMsg> {
    let (pclass, seq, msg_id, fmt, exchange, routing_key, props, body, exp, count, deaths): (
        u8,
        u64,
        Vec<u8>,
        u8,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Option<i64>,
        u32,
        Option<Vec<u8>>,
    ) = mysql_async::from_row_opt(r).ok()?;
    Some(StoredMsg {
        lane: 0,
        pclass,
        seq,
        msg_id: id16(&msg_id),
        fmt,
        exchange: String::from_utf8_lossy(&exchange).into_owned(),
        routing_key: String::from_utf8_lossy(&routing_key).into_owned(),
        props: Bytes::from(props),
        body: Bytes::from(body),
        expires_at_ms: exp,
        delivery_count: count,
        deaths: deaths.map(Bytes::from),
    })
}

fn outbox_of(r: mysql_async::Row) -> Option<OutboxRow> {
    let (out_id, kind, dest, msg_id, fmt, exchange, routing_key, props, body, deaths): (
        Vec<u8>,
        String,
        String,
        Vec<u8>,
        u8,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Option<Vec<u8>>,
    ) = mysql_async::from_row_opt(r).ok()?;
    Some(OutboxRow {
        out_id: id16(&out_id),
        kind: OutKind::parse(&kind)?,
        dest: serde_json::from_str(&dest).ok()?,
        msg_id: id16(&msg_id),
        fmt,
        exchange: String::from_utf8_lossy(&exchange).into_owned(),
        routing_key: String::from_utf8_lossy(&routing_key).into_owned(),
        props: Bytes::from(props),
        body: Bytes::from(body),
        deaths: deaths.map(Bytes::from),
    })
}

const MSG_COLS: &str = "pclass, seq, msg_id, fmt, exchange, routing_key, props, body, \
                        expires_at_ms, delivery_count, deaths";
const OUT_COLS: &str =
    "out_id, kind, CAST(dest AS CHAR), msg_id, fmt, exchange, routing_key, props, body, deaths";

#[async_trait]
impl MsgStore for MariaStore {
    fn stale(&self) -> bool {
        self.stale.load(std::sync::atomic::Ordering::Relaxed)
    }

    async fn claim(&self, queue: u64, lane: u16) -> Result<Claim, StoreError> {
        let mut c = self.conn().await?;
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(|e| self.fail(&e))?;
        tx.exec_drop(
            format!(
                "INSERT IGNORE INTO {} (queue_id, lane, owner_epoch, next_seq, delivered_hwm, \
                 ready_count, ready_bytes, writes) VALUES (?, ?, 0, 1, 0, 0, 0, 0)",
                self.t("__sprig_qs")
            ),
            (queue, lane),
        )
        .await
        .map_err(|e| self.fail(&e))?;
        let row: Option<(u64, u64, u64)> = tx
            .exec_first(
                format!(
                    "SELECT owner_epoch, next_seq, delivered_hwm FROM {} \
                     WHERE queue_id = ? AND lane = ? FOR UPDATE",
                    self.t("__sprig_qs")
                ),
                (queue, lane),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        let (epoch, next_seq, hwm) = row.ok_or_else(|| StoreError::Failed("no lane row".into()))?;
        tx.exec_drop(
            format!(
                "UPDATE {} SET owner_epoch = owner_epoch + 1, writes = writes + 1 \
                 WHERE queue_id = ? AND lane = ?",
                self.t("__sprig_qs")
            ),
            (queue, lane),
        )
        .await
        .map_err(|e| self.fail(&e))?;
        let counts: Option<(u64, Option<u64>)> = tx
            .exec_first(
                format!(
                    "SELECT COUNT(*), CAST(SUM(body_bytes + LENGTH(props)) AS UNSIGNED) FROM {} \
                     WHERE queue_id = ? AND lane = ?",
                    self.t("__sprig_qm")
                ),
                (queue, lane),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        tx.commit().await.map_err(|e| self.fail(&e))?;
        let (count, bytes) = counts.unwrap_or((0, None));
        Ok(Claim {
            epoch: epoch + 1,
            state: LaneState {
                next_seq,
                delivered_hwm: hwm,
                ready_count: count,
                ready_bytes: bytes.unwrap_or(0),
            },
            count,
            bytes: bytes.unwrap_or(0),
        })
    }

    async fn load(
        &self,
        queue: u64,
        lane: u16,
        after: Option<(u8, u64)>,
        limit: usize,
    ) -> Result<Vec<StoredMsg>, StoreError> {
        let mut c = self.conn().await?;
        let rows: Vec<mysql_async::Row> = match after {
            None => c
                .exec(
                    format!(
                        "SELECT {MSG_COLS} FROM {} WHERE queue_id = ? AND lane = ? \
                         ORDER BY pclass, seq LIMIT ?",
                        self.t("__sprig_qm")
                    ),
                    (queue, lane, limit as u64),
                )
                .await
                .map_err(|e| self.fail(&e))?,
            Some((p, s)) => c
                .exec(
                    format!(
                        "SELECT {MSG_COLS} FROM {} WHERE queue_id = ? AND lane = ? \
                         AND (pclass > ? OR (pclass = ? AND seq > ?)) ORDER BY pclass, seq LIMIT ?",
                        self.t("__sprig_qm")
                    ),
                    (queue, lane, p, p, s, limit as u64),
                )
                .await
                .map_err(|e| self.fail(&e))?,
        };
        Ok(rows.into_iter().filter_map(msg_of).collect())
    }

    async fn existing(&self, queue: u64, ids: &[MsgId]) -> Result<Vec<MsgId>, StoreError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut c = self.conn().await?;
        let mut out = Vec::new();
        for chunk in ids.chunks(CHUNK) {
            let marks = vec!["?"; chunk.len()].join(", ");
            // A locking read: waits for a transaction that holds the row
            // (an acknowledgement inside a transaction) to end. With the
            // forward ledger: forwarded before, maybe consumed already.
            let mut params: Vec<Value> = vec![queue.into()];
            params.extend(chunk.iter().map(|i| Value::from(i.to_vec())));
            params.push(queue.into());
            params.extend(chunk.iter().map(|i| Value::from(i.to_vec())));
            let rows: Vec<Vec<u8>> = c
                .exec(
                    format!(
                        "(SELECT msg_id FROM {} WHERE queue_id = ? AND msg_id IN ({marks}) LOCK IN SHARE MODE) \
                         UNION ALL (SELECT out_id FROM {} WHERE queue_id = ? AND out_id IN ({marks}))",
                        self.t("__sprig_qm"),
                        self.t("__sprig_qf")
                    ),
                    Params::Positional(params),
                )
                .await
                .map_err(|e| self.fail(&e))?;
            out.extend(rows.iter().map(|r| id16(r)));
        }
        Ok(out)
    }

    async fn write(&self, b: &WriteBatch) -> Result<(), StoreError> {
        let mut c = self.conn().await?;
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(|e| self.fail(&e))?;
        // The fence first: it locks the lane row for the batch.
        tx.exec_drop(
            format!(
                "UPDATE {} SET next_seq = ?, delivered_hwm = ?, ready_count = ?, ready_bytes = ?, \
                 writes = writes + 1 WHERE queue_id = ? AND lane = ? AND owner_epoch = ?",
                self.t("__sprig_qs")
            ),
            (
                b.state.next_seq,
                b.state.delivered_hwm,
                b.state.ready_count,
                b.state.ready_bytes,
                b.queue,
                b.lane,
                b.epoch,
            ),
        )
        .await
        .map_err(|e| self.fail(&e))?;
        if tx.affected_rows() == 0 {
            let _ = tx.rollback().await;
            return Err(StoreError::Fenced);
        }
        // Inserts, in statements under MAX_STMT_BYTES.
        let mut i = 0;
        while i < b.inserts.len() {
            let mut n = 0;
            let mut bytes = 0;
            while i + n < b.inserts.len() && n < CHUNK {
                let m = &b.inserts[i + n];
                if n > 0 && bytes + m.body.len() + m.props.len() > MAX_STMT_BYTES {
                    break;
                }
                bytes += m.body.len() + m.props.len();
                n += 1;
            }
            let rows = &b.inserts[i..i + n];
            let marks =
                vec!["(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3))"; n].join(", ");
            let mut params: Vec<Value> = Vec::with_capacity(n * 14);
            for m in rows {
                params.extend([
                    b.queue.into(),
                    b.lane.into(),
                    m.pclass.into(),
                    m.seq.into(),
                    m.msg_id.to_vec().into(),
                    m.fmt.into(),
                    m.exchange.as_bytes().into(),
                    m.routing_key.as_bytes().into(),
                    m.props.to_vec().into(),
                    m.body.to_vec().into(),
                    (m.body.len() as u64).into(),
                    m.expires_at_ms.into(),
                    m.delivery_count.into(),
                    m.deaths.as_ref().map(|d| d.to_vec()).into(),
                ]);
            }
            tx.exec_drop(
                format!(
                    "INSERT INTO {} (queue_id, lane, pclass, seq, msg_id, fmt, exchange, routing_key, \
                     props, body, body_bytes, expires_at_ms, delivery_count, deaths, enq_at) VALUES {marks}",
                    self.t("__sprig_qm")
                ),
                Params::Positional(params),
            )
            .await
            .map_err(|e| self.fail(&e))?;
            i += n;
        }
        for chunk in b.deletes.chunks(CHUNK) {
            let cond = vec!["(pclass = ? AND seq = ? AND msg_id = ?)"; chunk.len()].join(" OR ");
            let mut params: Vec<Value> = vec![b.queue.into(), b.lane.into()];
            for d in chunk {
                params.extend([d.pclass.into(), d.seq.into(), d.msg_id.to_vec().into()]);
            }
            tx.exec_drop(
                format!(
                    "DELETE FROM {} WHERE queue_id = ? AND lane = ? AND ({cond})",
                    self.t("__sprig_qm")
                ),
                Params::Positional(params),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        }
        for (r, n) in &b.counts {
            tx.exec_drop(
                format!(
                    "UPDATE {} SET delivery_count = ? WHERE queue_id = ? AND lane = ? AND pclass = ? \
                     AND seq = ? AND msg_id = ?",
                    self.t("__sprig_qm")
                ),
                (*n, b.queue, b.lane, r.pclass, r.seq, r.msg_id.to_vec()),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        }
        for o in &b.outbox {
            let (sql, params) = outbox_insert(&self.schema, o);
            tx.exec_drop(sql, Params::Positional(params))
                .await
                .map_err(|e| self.fail(&e))?;
        }
        for chunk in b.forwarded.chunks(CHUNK) {
            let marks = vec!["(?, ?, ?)"; chunk.len()].join(", ");
            // When it was forwarded (the purge's margin counts from here).
            let at = crate::store::msg_id_ms(&crate::store::new_msg_id());
            let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 3);
            for id in chunk {
                params.extend([b.queue.into(), id.to_vec().into(), at.into()]);
            }
            tx.exec_drop(
                format!(
                    "INSERT IGNORE INTO {} (queue_id, out_id, at_ms) VALUES {marks}",
                    self.t("__sprig_qf")
                ),
                Params::Positional(params),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        }
        tx.commit().await.map_err(|e| self.fail(&e))?;
        if b.inserts.is_empty() {
            return Ok(());
        }
        self.wait_replica(&mut c).await
    }

    async fn purge(
        &self,
        queue: u64,
        lane: u16,
        epoch: u64,
        keep: &[u64],
    ) -> Result<u64, StoreError> {
        let mut c = self.conn().await?;
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(|e| self.fail(&e))?;
        tx.exec_drop(
            format!(
                "UPDATE {} SET ready_count = 0, ready_bytes = 0, writes = writes + 1 \
                 WHERE queue_id = ? AND lane = ? AND owner_epoch = ?",
                self.t("__sprig_qs")
            ),
            (queue, lane, epoch),
        )
        .await
        .map_err(|e| self.fail(&e))?;
        if tx.affected_rows() == 0 {
            let _ = tx.rollback().await;
            return Err(StoreError::Fenced);
        }
        let mut params: Vec<Value> = vec![queue.into(), lane.into()];
        let not_in = if keep.is_empty() {
            String::new()
        } else {
            params.extend(keep.iter().map(|s| Value::from(*s)));
            format!(" AND seq NOT IN ({})", vec!["?"; keep.len()].join(", "))
        };
        tx.exec_drop(
            format!(
                "DELETE FROM {} WHERE queue_id = ? AND lane = ?{not_in}",
                self.t("__sprig_qm")
            ),
            Params::Positional(params),
        )
        .await
        .map_err(|e| self.fail(&e))?;
        let n = tx.affected_rows();
        tx.commit().await.map_err(|e| self.fail(&e))?;
        Ok(n)
    }

    async fn drop_queue(&self, queue: u64) -> Result<(), StoreError> {
        let mut c = self.conn().await?;
        for t in ["__sprig_qm", "__sprig_qs", "__sprig_qf"] {
            c.exec_drop(
                format!("DELETE FROM {} WHERE queue_id = ?", self.t(t)),
                (queue,),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        }
        Ok(())
    }

    async fn outbox(&self, limit: usize) -> Result<Vec<OutboxRow>, StoreError> {
        let mut c = self.conn().await?;
        let rows: Vec<mysql_async::Row> = c
            .exec(
                format!(
                    "SELECT {OUT_COLS} FROM {} ORDER BY out_id LIMIT ?",
                    self.t("__sprig_qo")
                ),
                (limit as u64,),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        Ok(rows.into_iter().filter_map(outbox_of).collect())
    }

    async fn outbox_head(&self) -> Result<Option<MsgId>, StoreError> {
        let mut c = self.conn().await?;
        let id: Option<Vec<u8>> = c
            .query_first(format!(
                "SELECT out_id FROM {} ORDER BY out_id LIMIT 1",
                self.t("__sprig_qo")
            ))
            .await
            .map_err(|e| self.fail(&e))?;
        Ok(id.and_then(|v| <[u8; 16]>::try_from(v.as_slice()).ok()))
    }

    async fn outbox_done(&self, ids: &[MsgId]) -> Result<(), StoreError> {
        let mut c = self.conn().await?;
        for chunk in ids.chunks(CHUNK) {
            let marks = vec!["?"; chunk.len()].join(", ");
            c.exec_drop(
                format!(
                    "DELETE FROM {} WHERE out_id IN ({marks})",
                    self.t("__sprig_qo")
                ),
                Params::Positional(chunk.iter().map(|i| Value::from(i.to_vec())).collect()),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        }
        Ok(())
    }

    async fn outbox_rows(&self, ids: &[MsgId]) -> Result<Vec<OutboxRow>, StoreError> {
        let mut c = self.conn().await?;
        let mut found: Vec<OutboxRow> = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(CHUNK * 8) {
            let marks = vec!["?"; chunk.len()].join(", ");
            let rows: Vec<mysql_async::Row> = c
                .exec(
                    format!(
                        "SELECT {OUT_COLS} FROM {} WHERE out_id IN ({marks})",
                        self.t("__sprig_qo")
                    ),
                    Params::Positional(chunk.iter().map(|i| Value::from(i.to_vec())).collect()),
                )
                .await
                .map_err(|e| self.fail(&e))?;
            found.extend(rows.into_iter().filter_map(outbox_of));
        }
        // In the order asked (commit order), not the table's.
        let mut by_id: std::collections::HashMap<MsgId, OutboxRow> =
            found.into_iter().map(|r| (r.out_id, r)).collect();
        Ok(ids.iter().filter_map(|i| by_id.remove(i)).collect())
    }

    async fn outbox_due(
        &self,
        limit: usize,
        publish_before: MsgId,
    ) -> Result<Vec<OutboxRow>, StoreError> {
        let mut c = self.conn().await?;
        let rows: Vec<mysql_async::Row> = c
            .exec(
                format!(
                    "SELECT {OUT_COLS} FROM {} WHERE kind <> 'publish' OR out_id < ? \
                     ORDER BY out_id LIMIT ?",
                    self.t("__sprig_qo")
                ),
                (publish_before.to_vec(), limit as u64),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        Ok(rows.into_iter().filter_map(outbox_of).collect())
    }

    async fn purge_ledger(&self, before_ms: i64) -> Result<u64, StoreError> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "DELETE FROM {} WHERE at_ms < ? AND out_id NOT IN (SELECT out_id FROM {}) LIMIT 5000",
                self.t("__sprig_qf"),
                self.t("__sprig_qo")
            ),
            (before_ms,),
        )
        .await
        .map_err(|e| self.fail(&e))?;
        Ok(c.affected_rows())
    }

    async fn delay(&self, row: &DelayedRow) -> Result<(), StoreError> {
        let mut c = self.conn().await?;
        c.exec_drop(
            format!(
                "INSERT INTO {} (exchange_id, exchange, due_ms, msg_id, routing_key, fmt, props, body) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                self.t("__sprig_qd")
            ),
            Params::Positional(vec![
                row.exchange_id.into(),
                row.exchange.as_bytes().into(),
                row.due_ms.into(),
                row.msg_id.to_vec().into(),
                row.routing_key.as_bytes().into(),
                row.fmt.into(),
                row.props.to_vec().into(),
                row.body.to_vec().into(),
            ]),
        )
        .await
        .map_err(|e| self.fail(&e))?;
        self.wait_replica(&mut c).await
    }

    async fn release_due(&self, now_ms: i64, limit: usize) -> Result<usize, StoreError> {
        let mut c = self.conn().await?;
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(|e| self.fail(&e))?;
        let rows: Vec<(Vec<u8>, i64, Vec<u8>, Vec<u8>, u8, Vec<u8>, Vec<u8>)> = tx
            .exec(
                format!(
                    "SELECT exchange, due_ms, msg_id, routing_key, fmt, props, body FROM {} \
                     WHERE due_ms <= ? ORDER BY due_ms, msg_id LIMIT ? FOR UPDATE",
                    self.t("__sprig_qd")
                ),
                (now_ms, limit as u64),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        let n = rows.len();
        for (exchange, due, msg_id, rk, fmt, props, body) in rows {
            let exchange = String::from_utf8_lossy(&exchange).into_owned();
            let rk = String::from_utf8_lossy(&rk).into_owned();
            let row = OutboxRow {
                out_id: crate::store::new_msg_id(),
                kind: OutKind::Delayed,
                dest: OutDest::Exchange {
                    name: exchange.clone(),
                    key: rk.clone(),
                },
                msg_id: id16(&msg_id),
                fmt,
                exchange,
                routing_key: rk,
                props: Bytes::from(props),
                body: Bytes::from(body),
                deaths: None,
            };
            let (sql, params) = outbox_insert(&self.schema, &row);
            tx.exec_drop(sql, Params::Positional(params))
                .await
                .map_err(|e| self.fail(&e))?;
            tx.exec_drop(
                format!(
                    "DELETE FROM {} WHERE due_ms = ? AND msg_id = ?",
                    self.t("__sprig_qd")
                ),
                (due, msg_id),
            )
            .await
            .map_err(|e| self.fail(&e))?;
        }
        tx.commit().await.map_err(|e| self.fail(&e))?;
        Ok(n)
    }

    async fn next_due(&self) -> Result<Option<i64>, StoreError> {
        let mut c = self.conn().await?;
        let v: Option<Option<i64>> = c
            .query_first(format!("SELECT MIN(due_ms) FROM {}", self.t("__sprig_qd")))
            .await
            .map_err(|e| self.fail(&e))?;
        Ok(v.flatten())
    }
}
