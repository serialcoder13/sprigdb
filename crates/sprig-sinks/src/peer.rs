//! Sink tasks reaching a gateway (chapter 51, R9, r9queue): the
//! `LogicPeer` request `SinkSql`, which runs a sink's statements in a
//! session of the sink's **owner** on a gateway of the DC, so the owner's
//! privileges, column privileges and row-level security apply as for any
//! session of the owner.
//!
//! ```text
//! script db handle (target::script::ReadOnlyDb):
//!   Begin                → a read-only transaction handle (one per batch attempt)
//!   Read {handle, sql}   → one checked SELECT or WITH in it (39235 otherwise)
//!   End {handle}         → ROLLBACK
//! queue source (source::QueueSource):
//!   Queue {statements}   → CALL sprig.mq_get(...) / mq_ack(...) / mq_nack(...),
//!                          nothing else, in one session, in order
//! ```
//!
//! A handle lives on the gateway that began it: a session's requests all
//! go to the same gateway (the same rendezvous seed); a gateway that
//! stops answering mid-batch fails the batch, which the runtime retries.

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigCode, SprigError};

/// What a request does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum SqlOp {
    /// Begins a read-only transaction: the reply's `handle`.
    Begin,
    /// One read in `handle`'s transaction.
    Read {
        /// The transaction.
        handle: String,
        /// One `SELECT` or `WITH` statement.
        sql: String,
    },
    /// Ends `handle`'s transaction (`ROLLBACK`).
    End {
        /// The transaction.
        handle: String,
    },
    /// Queue calls, in one session, in order.
    Queue {
        /// Each a `CALL sprig.mq_get | mq_ack | mq_nack (...)`.
        statements: Vec<String>,
    },
    /// A fan-in queue sink's get from member `member`'s instance of the
    /// queue definition `(def_tenant, def_object)`: the result has
    /// `mq_get`'s columns.
    FanInGet {
        /// The definition's declaring tenant.
        def_tenant: u64,
        /// The definition's object id there.
        def_object: u64,
        /// The member.
        member: u64,
        /// The instance's database (vhost).
        db: String,
        /// The queue's name.
        queue: String,
        /// Messages at most.
        max: u32,
        /// How long each is held unless settled.
        lease_ms: u64,
        /// The task's lease tag (R9, r9last), so a later task can give
        /// the messages back with [`SqlOp::FanInRelease`]; absent from
        /// older workers.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tag: Option<String>,
    },
    /// Gives back what a dead fan-in queue sink task held in member
    /// `member`'s instance under lease tag `tag` (R9, r9last; as
    /// `mq_release` for a sink's own queue): an empty result once the
    /// queue's owner has them.
    FanInRelease {
        /// The definition's declaring tenant.
        def_tenant: u64,
        /// The definition's object id there.
        def_object: u64,
        /// The member.
        member: u64,
        /// The instance's database (vhost).
        db: String,
        /// The queue's name.
        queue: String,
        /// The dead task's lease tag.
        tag: String,
    },
    /// Settles a fan-in queue sink's deliveries by their tokens:
    /// acknowledged (`ack`) or rejected without requeue (dead-lettered).
    FanInSettle {
        /// The delivery tokens.
        tokens: Vec<String>,
        /// Acknowledge (else reject).
        ack: bool,
    },
    /// R9 (r9fwd): a step of the tenant's feed-driven outbox forwarder
    /// (chapter 50, "Publishing inside a database transaction"): forward
    /// the rows `ids` (out ids in hex, in commit order) of outbox `table`
    /// (`__sprig_qo` or `__sprig_xo`) in shard schema `schema` of the
    /// request's tenant, in that order, and delete them. Internal: no
    /// session is opened (`user_id` and `database` are ignored). The
    /// result is one row: `forwarded`, `gone` (already forwarded), `left`
    /// (a destination that can't take them yet; the fallback retries).
    OutboxForward {
        /// The shard schema.
        schema: String,
        /// `__sprig_qo` or `__sprig_xo`.
        table: String,
        /// Out ids, hex, in commit order.
        ids: Vec<String>,
    },
    /// Chapter 29 (RLS round 5, RL50): which row images the session's
    /// user may see, each checked as the gateway checks change events for
    /// a subscriber (`SELECT` on the table, and its row policies with the
    /// user's session values, evaluated by MariaDB over the image): one
    /// result, rows `(t, i, v)`: table `t`, image `i`, visible `v` (0/1).
    Images {
        /// The tables and their images.
        tables: Vec<ImageTable>,
    },
}

/// One table's images for [`SqlOp::Images`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageTable {
    /// The logical database.
    pub db: String,
    /// The table.
    pub table: String,
    /// Each image as a JSON object of column values (binary values in
    /// base64, as change records carry them), or `null` for the privilege
    /// alone.
    pub images: Vec<Option<serde_json::Map<String, serde_json::Value>>>,
}

/// [`SqlOp::Images`]'s answer as `[table][image] → visible`.
#[must_use]
pub fn images_answer(tables: &[ImageTable], reply: &SqlReply) -> Vec<Vec<bool>> {
    let mut out: Vec<Vec<bool>> = tables.iter().map(|t| vec![false; t.images.len()]).collect();
    for r in reply.results.iter().flat_map(|r| &r.rows) {
        let n = |i: usize| SqlResult::text(r, i).and_then(|v| v.parse::<usize>().ok());
        if let (Some(t), Some(i), Some(v)) = (n(0), n(1), n(2))
            && let Some(slot) = out.get_mut(t).and_then(|x| x.get_mut(i))
        {
            *slot = v == 1;
        }
    }
    out
}

/// `SinkSql`'s request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlRequest {
    /// The sink's tenant.
    pub tenant: u64,
    /// Its name (sessions are opened by name; the id must match).
    pub tenant_name: String,
    /// The sink's owner.
    pub user_id: u32,
    /// The session's default database (the sink's).
    pub database: String,
    /// What to run.
    #[serde(flatten)]
    pub op: SqlOp,
    /// Rows kept per result; 0 for no cap.
    #[serde(default)]
    pub max_rows: u64,
    /// The statement timeout; 0 for none.
    #[serde(default)]
    pub timeout_ms: u64,
}

/// One result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlResult {
    /// Column names.
    pub columns: Vec<String>,
    /// MySQL type codes of the columns.
    #[serde(default)]
    pub types: Vec<u32>,
    /// Rows: each cell's bytes in base64, `None` for SQL NULL.
    pub rows: Vec<Vec<Option<String>>>,
    /// Rows changed (statements without rows).
    #[serde(default)]
    pub affected: u64,
    /// This statement's error (queue calls run on after one fails).
    #[serde(default)]
    pub error: Option<PeerError>,
}

impl SqlResult {
    /// Cell `i` of `row` as bytes.
    #[must_use]
    pub fn bytes(row: &[Option<String>], i: usize) -> Option<Vec<u8>> {
        row.get(i)?.as_deref().and_then(|s| B64.decode(s).ok())
    }

    /// Cell `i` of `row` as text (lossy).
    #[must_use]
    pub fn text(row: &[Option<String>], i: usize) -> Option<String> {
        Self::bytes(row, i).map(|b| String::from_utf8_lossy(&b).into_owned())
    }

    /// The index of column `name`.
    #[must_use]
    pub fn col(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(name))
    }

    /// The encoding of a cell.
    #[must_use]
    pub fn cell(b: Option<&[u8]>) -> Option<String> {
        b.map(|b| B64.encode(b))
    }
}

/// An error a gateway answered with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerError {
    /// The error number (a MySQL or SprigDB code).
    pub code: u32,
    /// The SQLSTATE.
    #[serde(default)]
    pub state: String,
    /// The message.
    pub message: String,
    /// Retrying may help.
    #[serde(default)]
    pub retryable: bool,
}

impl PeerError {
    /// From an error.
    #[must_use]
    pub fn of(e: &SprigError) -> Self {
        let p = e.protocol();
        Self {
            code: u32::from(p.mysql_errno),
            state: String::from_utf8_lossy(&p.sqlstate).into_owned(),
            message: e.message.clone(),
            retryable: e.retryable,
        }
    }

    /// Back to an error.
    #[must_use]
    pub fn error(&self) -> SprigError {
        let mut state = *b"HY000";
        if self.state.len() == 5 {
            state.copy_from_slice(self.state.as_bytes());
        }
        let code = SprigCode::ALL
            .iter()
            .copied()
            .find(|c| u32::from(c.number()) == self.code);
        let mut e = match code {
            Some(c) => SprigError::new(c, self.message.clone()),
            None => SprigError::backend(
                u16::try_from(self.code).unwrap_or(u16::MAX),
                state,
                self.message.clone(),
            ),
        };
        e.retryable = self.retryable;
        e
    }
}

/// `SinkSql`'s reply.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlReply {
    /// `Begin`'s transaction handle.
    #[serde(default)]
    pub handle: Option<String>,
    /// One per statement that returned.
    #[serde(default)]
    pub results: Vec<SqlResult>,
    /// The error that stopped the request.
    #[serde(default)]
    pub error: Option<PeerError>,
}

impl SqlReply {
    /// The reply, or its error.
    ///
    /// # Errors
    /// The gateway's error.
    pub fn ok(self) -> Result<Self> {
        match &self.error {
            Some(e) => Err(e.error()),
            None => Ok(self),
        }
    }
}

/// Where a sink's statements run: a gateway of the DC (the worker's
/// `LogicPeer` client), or a fake.
#[async_trait]
pub trait SinkPeer: Send + Sync {
    /// Runs `req` on the gateway `seed` picks (the same seed, the same
    /// gateway while it answers).
    ///
    /// # Errors
    /// No gateway answered (retryable).
    async fn sql(&self, seed: u64, req: &SqlRequest) -> Result<SqlReply>;
}

/// Who a sink's statements run as: its tenant (id and name) and owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunAs {
    /// The tenant.
    pub tenant: u64,
    /// Its name.
    pub tenant_name: String,
    /// The owner.
    pub user_id: u32,
    /// The sink's database.
    pub database: String,
}

impl RunAs {
    /// A request of `op` as this owner.
    #[must_use]
    pub fn request(&self, op: SqlOp) -> SqlRequest {
        SqlRequest {
            tenant: self.tenant,
            tenant_name: self.tenant_name.clone(),
            user_id: self.user_id,
            database: self.database.clone(),
            op,
            max_rows: 0,
            timeout_ms: 0,
        }
    }
}

/// The name of tenant `id` from its directory entry (`tenants/<id>` in
/// the global tier).
///
/// # Errors
/// The store's; 39070-style `TenantNotReady` for a tenant without one.
pub async fn tenant_name(global: &dyn sprig_meta::MetaStore, id: u64) -> Result<String> {
    let key = global.tier().key(&format!("tenants/{id}"))?;
    let kv = global
        .get(&key, sprig_meta::Consistency::Serializable)
        .await?
        .ok_or_else(|| {
            SprigError::new(
                SprigCode::TenantNotReady,
                format!("tenant {id} has no directory entry"),
            )
        })?;
    let v: serde_json::Value = serde_json::from_slice(&kv.value)
        .map_err(|e| SprigError::internal(format!("tenant {id}'s directory entry: {e}")))?;
    v.get("name")
        .or_else(|| v.get("body").and_then(|b| b.get("name")))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| SprigError::internal(format!("tenant {id}'s directory entry has no name")))
}

// ---- the script sink's db handle in workers ----------------------------------

/// The script sink's read-only `db` handle in a worker: each batch
/// attempt's reads in one read-only transaction of a session of the
/// sink's owner on a gateway ([`SqlOp::Begin`], [`SqlOp::Read`],
/// [`SqlOp::End`]), so the owner's privileges, column privileges and
/// row-level security apply, and nothing can be written.
pub struct PeerReadOnly {
    peer: std::sync::Arc<dyn SinkPeer>,
    run_as: RunAs,
    sessions: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for PeerReadOnly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerReadOnly")
            .field("tenant", &self.run_as.tenant)
            .field("owner", &self.run_as.user_id)
            .finish_non_exhaustive()
    }
}

impl PeerReadOnly {
    /// Reads as `run_as` through `peer`.
    #[must_use]
    pub fn new(peer: std::sync::Arc<dyn SinkPeer>, run_as: RunAs) -> Self {
        Self {
            peer,
            run_as,
            sessions: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

/// The script runtime's kind of a MySQL type code.
#[must_use]
pub fn col_kind(t: u32) -> sprig_logic_js::bridge::ColKind {
    use sprig_logic_js::bridge::ColKind;
    match t {
        1 | 2 | 3 | 8 | 9 | 13 => ColKind::Int,
        0 | 4 | 5 | 246 => ColKind::Float,
        245 => ColKind::Json,
        _ => ColKind::Str,
    }
}

struct PeerSession {
    peer: std::sync::Arc<dyn SinkPeer>,
    run_as: RunAs,
    seed: u64,
    handle: String,
}

#[async_trait]
impl crate::target::script::ReadSession for PeerSession {
    async fn query(
        &mut self,
        sql: &str,
    ) -> Result<(
        Vec<sprig_logic_js::bridge::Column>,
        Vec<Vec<Option<String>>>,
    )> {
        crate::target::script::check_read_only(sql)?;
        let r = self
            .peer
            .sql(
                self.seed,
                &self.run_as.request(SqlOp::Read {
                    handle: self.handle.clone(),
                    sql: sql.to_owned(),
                }),
            )
            .await?
            .ok()?;
        let res = r.results.into_iter().next().unwrap_or_default();
        if let Some(e) = &res.error {
            return Err(e.error());
        }
        let columns = res
            .columns
            .iter()
            .enumerate()
            .map(|(i, n)| sprig_logic_js::bridge::Column {
                name: n.clone(),
                kind: col_kind(res.types.get(i).copied().unwrap_or(253)),
            })
            .collect();
        let rows = res
            .rows
            .iter()
            .map(|row| (0..row.len()).map(|i| SqlResult::text(row, i)).collect())
            .collect();
        Ok((columns, rows))
    }

    async fn end(self: Box<Self>) {
        let _ = self
            .peer
            .sql(
                self.seed,
                &self.run_as.request(SqlOp::End {
                    handle: self.handle.clone(),
                }),
            )
            .await;
    }
}

#[async_trait]
impl crate::target::script::ReadOnlyDb for PeerReadOnly {
    async fn begin(&self) -> Result<Box<dyn crate::target::script::ReadSession>> {
        // One gateway per session (the handle lives there); sessions
        // spread over the gateways.
        let n = self
            .sessions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let seed = self.run_as.tenant ^ (u64::from(self.run_as.user_id) << 32) ^ n;
        let r = self
            .peer
            .sql(seed, &self.run_as.request(SqlOp::Begin))
            .await?
            .ok()?;
        let handle = r
            .handle
            .ok_or_else(|| SprigError::internal("a read session began without a handle"))?;
        Ok(Box::new(PeerSession {
            peer: std::sync::Arc::clone(&self.peer),
            run_as: self.run_as.clone(),
            seed,
            handle,
        }))
    }
}

/// 39235 unless every statement is a `CALL sprig.mq_get`, `mq_ack` or
/// `mq_nack` (a queue source's only statements).
///
/// # Errors
/// 39235.
pub fn check_queue_calls(statements: &[String]) -> Result<()> {
    for s in statements {
        let t = s.trim().trim_end_matches(';').trim();
        let low = t.to_ascii_lowercase();
        let ok = [
            "call sprig.mq_get(",
            "call sprig.mq_ack(",
            "call sprig.mq_nack(",
            // `ordering = queue` waits out earlier leases (r9fanin).
            "call sprig.mq_info(",
            // A dead task's leased messages back (r9tests).
            "call sprig.mq_release(",
        ]
        .iter()
        .any(|p| low.starts_with(p))
            && t.ends_with(')')
            && !inner_semicolon(t);
        if !ok {
            return Err(SprigError::new(
                SprigCode::SinkScriptReadOnly,
                "a sink's queue session runs sprig.mq_get, mq_ack, mq_nack, mq_info and mq_release only",
            ));
        }
    }
    Ok(())
}

/// Whether `s` has a `;` outside quotes.
fn inner_semicolon(s: &str) -> bool {
    let mut q: Option<char> = None;
    let mut esc = false;
    for c in s.chars() {
        if esc {
            esc = false;
            continue;
        }
        match (q, c) {
            (Some(_), '\\') => esc = true,
            (Some(x), c) if c == x => q = None,
            (None, '\'' | '"' | '`') => q = Some(c),
            (None, ';') => return true,
            _ => {}
        }
    }
    false
}

/// A SQL string literal.
#[must_use]
pub fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            '\0' => out.push_str("\\0"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_round_trip_as_json() {
        let r = RunAs {
            tenant: (1 << 40) + 1,
            tenant_name: "acme".into(),
            user_id: 3,
            database: "shop".into(),
        };
        for op in [
            SqlOp::Begin,
            SqlOp::Read {
                handle: "transactions/x".into(),
                sql: "SELECT 1".into(),
            },
            SqlOp::End {
                handle: "transactions/x".into(),
            },
            SqlOp::Queue {
                statements: vec!["CALL sprig.mq_get('q', 10, 60000, 'shop')".into()],
            },
            SqlOp::FanInGet {
                def_tenant: 1,
                def_object: 2,
                member: 3,
                db: "shop".into(),
                queue: "jobs".into(),
                max: 10,
                lease_ms: 60_000,
                tag: Some("sink/7/0/3".into()),
            },
            SqlOp::FanInRelease {
                def_tenant: 1,
                def_object: 2,
                member: 3,
                db: "shop".into(),
                queue: "jobs".into(),
                tag: "sink/7/0/3".into(),
            },
        ] {
            let req = r.request(op);
            let j = serde_json::to_string(&req).unwrap();
            assert_eq!(serde_json::from_str::<SqlRequest>(&j).unwrap(), req);
        }
        // R9 (r9last): a FanInGet from a worker before lease tags reads
        // as untagged.
        let old = r#"{"tenant":1,"tenant_name":"acme","user_id":3,"database":"shop","op":"fan_in_get","def_tenant":1,"def_object":2,"member":3,"db":"shop","queue":"jobs","max":10,"lease_ms":60000}"#;
        let req = serde_json::from_str::<SqlRequest>(old).unwrap();
        assert!(
            matches!(req.op, SqlOp::FanInGet { tag: None, .. }),
            "{req:?}"
        );
        let reply = SqlReply {
            handle: None,
            results: vec![SqlResult {
                columns: vec!["body".into()],
                types: vec![252],
                rows: vec![vec![SqlResult::cell(Some(b"\x00\xff")), None]],
                affected: 0,
                error: None,
            }],
            error: None,
        };
        let back: SqlReply = serde_json::from_str(&serde_json::to_string(&reply).unwrap()).unwrap();
        assert_eq!(
            SqlResult::bytes(&back.results[0].rows[0], 0).unwrap(),
            b"\x00\xff"
        );
        assert_eq!(SqlResult::bytes(&back.results[0].rows[0], 1), None);
        assert_eq!(back.results[0].col("BODY"), Some(0));
    }

    #[test]
    fn errors_keep_their_codes() {
        let e = SprigError::new(SprigCode::SinkScriptReadOnly, "no");
        let back = PeerError::of(&e).error();
        assert_eq!(back.code(), Some(SprigCode::SinkScriptReadOnly));
        let b = SprigError::backend(1142, *b"42000", "SELECT command denied");
        let back = PeerError::of(&b).error();
        assert_eq!(back.protocol().mysql_errno, 1142);
    }

    #[test]
    fn only_queue_calls_pass() {
        let ok = |s: &str| check_queue_calls(&[s.to_owned()]).is_ok();
        assert!(ok("CALL sprig.mq_get('jobs', 100, 60000, 'shop')"));
        assert!(ok("call sprig.mq_ack('q1.AAA')"));
        assert!(ok("CALL sprig.mq_nack('q1.AAA', 0);"));
        assert!(!ok("CALL sprig.mq_ack('x'); DELETE FROM t"));
        assert!(ok("CALL sprig.mq_ack('x;y')"), "a ; inside quotes");
        assert!(!ok("DELETE FROM t"));
        assert!(!ok("CALL sprig.mq_publish('x', 'k', 'b', '{}')"));
        assert_eq!(quote("a'b\\c"), "'a''b\\\\c'");
    }
}
