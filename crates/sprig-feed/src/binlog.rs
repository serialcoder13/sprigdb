//! [`TxnSource`] and [`Clusters`] on `sprig-binlog` (chapter 38, "The
//! cluster reader").
//!
//! A [`BinlogTxns`] is one dump connection (`sprig_repl`) asking for
//! `ANNOTATE_ROWS` events, decoding rows only for one tenant's schemas and
//! for the applier position tables, and assembling events into committed
//! groups. Groups with nothing for the tenant only move the position; they
//! surface as [`SourceItem::Idle`] at most every [`IDLE_EVERY`].

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_binlog::{
    BinlogSource, BinlogStream, Event, GtidFlags, GtidPos, RowsKind, StreamOptions, TableMap,
    parse_comment,
};
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use tokio::time::Instant;

use crate::source::{
    Clusters, RawDdl, RawRow, RawTxn, RowKind, SourceItem, TableShape, TxnSource, XaMark,
};

/// The internal schema.
pub const SYS_SCHEMA: &str = "__sprig_sys";
/// The applier position tables (filter rule 2).
pub const APPLY_TABLES: [&str; 2] = ["__sprig_mover_pos", "__sprig_ref_pos"];
/// How often groups without the tenant's changes surface as progress.
pub const IDLE_EVERY: Duration = Duration::from_millis(200);

/// What a `QUERY` event's statement is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    /// `BEGIN` / `START TRANSACTION`.
    Begin,
    /// `COMMIT`.
    Commit,
    /// `ROLLBACK`.
    Rollback,
    /// `XA COMMIT`.
    XaCommit,
    /// `XA ROLLBACK`.
    XaRollback,
    /// `XA START`, `XA END`, `XA PREPARE`.
    XaOther,
    /// `CREATE`, `ALTER`, `DROP`, `RENAME`, `TRUNCATE`.
    Ddl,
    /// `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, `LOAD`: statement-format DML.
    Dml,
    /// Anything else (`SET`, `GRANT`, `FLUSH`, `ANALYZE`, ...).
    Other,
}

/// The statement after its leading spaces and comments.
#[must_use]
pub fn strip_comments(mut s: &str) -> &str {
    loop {
        s = s.trim_start();
        if let Some(r) = s.strip_prefix("/*") {
            match r.find("*/") {
                Some(i) => s = &r[i + 2..],
                None => return "",
            }
        } else if s.starts_with("-- ") || s.starts_with('#') {
            match s.find('\n') {
                Some(i) => s = &s[i + 1..],
                None => return "",
            }
        } else {
            return s;
        }
    }
}

/// The statement's first `n` words, upper-cased.
fn words(sql: &str, n: usize) -> Vec<String> {
    strip_comments(sql)
        .split(|c: char| c.is_ascii_whitespace() || c == ';' || c == '(')
        .filter(|w| !w.is_empty())
        .take(n)
        .map(str::to_ascii_uppercase)
        .collect()
}

/// Classifies a statement by its first words.
#[must_use]
pub fn classify(sql: &str) -> StatementKind {
    let w = words(sql, 2);
    match (
        w.first().map_or("", String::as_str),
        w.get(1).map_or("", String::as_str),
    ) {
        ("BEGIN", _) | ("START", "TRANSACTION") => StatementKind::Begin,
        ("COMMIT", _) => StatementKind::Commit,
        ("ROLLBACK", _) => StatementKind::Rollback,
        ("XA", "COMMIT") => StatementKind::XaCommit,
        ("XA", "ROLLBACK") => StatementKind::XaRollback,
        ("XA", _) => StatementKind::XaOther,
        ("CREATE" | "ALTER" | "DROP" | "RENAME" | "TRUNCATE", _) => StatementKind::Ddl,
        ("INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "LOAD", _) => StatementKind::Dml,
        _ => StatementKind::Other,
    }
}

/// Every word of `sql` that parses as one of `tenant`'s schema names.
#[must_use]
pub fn schemas_in(sql: &str, tenant: TenantId) -> Vec<String> {
    let mut out: Vec<String> = sql
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|w| w.starts_with('t'))
        .filter(|w| w.parse::<SchemaName>().is_ok_and(|s| s.tenant() == tenant))
        .map(str::to_owned)
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Whether `schema` is one of `tenant`'s.
#[must_use]
pub fn is_tenant_schema(schema: &str, tenant: TenantId) -> bool {
    schema
        .parse::<SchemaName>()
        .is_ok_and(|s| s.tenant() == tenant)
}

fn shape_of(tm: &TableMap) -> TableShape {
    TableShape {
        schema: tm.schema.clone(),
        table: tm.table.clone(),
        columns: tm
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| c.name.clone().unwrap_or_else(|| format!("@{}", i + 1)))
            .collect(),
        key: tm.primary_key.iter().map(|(i, _)| *i).collect(),
    }
}

/// Error 1236 (the server lacks the position) as 39800.
fn history_lost(e: SprigError) -> SprigError {
    let text = e.to_string();
    let lost = matches!(e.kind, ErrorKind::Backend { errno: 1236, .. }) || text.contains("1236");
    if lost {
        SprigError::new(SprigCode::FeedHistoryLost, text)
    } else {
        e
    }
}

#[derive(Debug)]
struct Group {
    txn: RawTxn,
    flags: GtidFlags,
    xa: Option<String>,
}

/// One cluster's committed groups for one tenant, from a binlog dump.
pub struct BinlogTxns {
    stream: BinlogStream,
    tenant: TenantId,
    pos: GtidPos,
    group: Option<Group>,
    shapes: HashMap<u64, Arc<TableShape>>,
    quiet_since: Option<Instant>,
}

impl std::fmt::Debug for BinlogTxns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinlogTxns")
            .field("tenant", &self.tenant)
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

impl BinlogTxns {
    /// Starts a dump on `src` after `from` for `tenant`, presenting
    /// `server_id`.
    ///
    /// # Errors
    /// 39800 when the node lacks `from`; the reader's errors.
    pub async fn connect(
        src: &BinlogSource,
        tenant: TenantId,
        from: &GtidPos,
        server_id: u32,
    ) -> Result<Self> {
        let mut opts = StreamOptions::new(server_id);
        opts.annotate_rows = true;
        opts.filter = Some(Arc::new(move |schema: &str, table: &str| {
            is_tenant_schema(schema, tenant)
                || (schema == SYS_SCHEMA && APPLY_TABLES.contains(&table))
        }));
        let stream = BinlogStream::connect(src, from, &opts)
            .await
            .map_err(history_lost)?;
        Ok(Self {
            stream,
            tenant,
            pos: from.clone(),
            group: None,
            shapes: HashMap::new(),
            quiet_since: None,
        })
    }

    /// Closes the open group. Returns it if it holds anything for the
    /// tenant.
    fn close(&mut self, xa: Option<XaMark>) -> Option<RawTxn> {
        let g = self.group.take()?;
        let mut txn = g.txn;
        self.pos.update(txn.gtid);
        txn.xa = xa;
        let empty = txn.rows.is_empty()
            && txn.ddl.is_none()
            && txn.statement_dml.is_none()
            && txn.xa.is_none();
        if empty {
            self.quiet_since.get_or_insert_with(Instant::now);
            return None;
        }
        self.quiet_since = None;
        Some(txn)
    }

    fn quiet_item(&mut self) -> Option<SourceItem> {
        let since = self.quiet_since?;
        if since.elapsed() >= IDLE_EVERY {
            self.quiet_since = None;
            return Some(SourceItem::Idle {
                pos: self.pos.clone(),
            });
        }
        None
    }
}

#[async_trait::async_trait]
impl TxnSource for BinlogTxns {
    #[allow(clippy::too_many_lines)] // one arm per event type
    async fn next(&mut self) -> Result<SourceItem> {
        loop {
            if self.group.is_none()
                && let Some(i) = self.quiet_item()
            {
                return Ok(i);
            }
            let ev = self.stream.next().await.map_err(history_lost)?;
            match ev {
                Event::Gtid {
                    gtid, flags, xa, ..
                } => {
                    self.group = Some(Group {
                        txn: RawTxn::new(
                            gtid,
                            self.pos.clone(),
                            self.stream.last_header().timestamp,
                        ),
                        flags,
                        xa: xa.map(|x| x.to_string()),
                    });
                }
                Event::Annotate(text) => {
                    // Only the comment is kept; the statement is dropped here.
                    if let (Some(c), Some(g)) = (parse_comment(&text), self.group.as_mut()) {
                        g.txn.stamps.push(c.hlc);
                    }
                }
                Event::TableMap(tm) => {
                    self.shapes.remove(&tm.table_id);
                }
                Event::Rows {
                    table_id,
                    table,
                    kind,
                    rows,
                    ..
                } => {
                    let Some(g) = self.group.as_mut() else {
                        continue;
                    };
                    if table.schema == SYS_SCHEMA {
                        if APPLY_TABLES.contains(&table.table.as_str()) {
                            g.txn.apply = true;
                        }
                        continue;
                    }
                    let shape = self
                        .shapes
                        .entry(table_id)
                        .or_insert_with(|| Arc::new(shape_of(&table)))
                        .clone();
                    let kind = match kind {
                        RowsKind::Insert => RowKind::Insert,
                        RowsKind::Update => RowKind::Update,
                        RowsKind::Delete => RowKind::Delete,
                    };
                    for r in rows {
                        g.txn.rows.push(RawRow {
                            table: shape.clone(),
                            kind,
                            before: r.before,
                            after: r.after,
                        });
                    }
                }
                Event::Query { db, sql, .. } => {
                    let standalone = self
                        .group
                        .as_ref()
                        .is_some_and(|g| g.flags.standalone() || g.flags.ddl());
                    match classify(&sql) {
                        StatementKind::Begin | StatementKind::XaOther => {}
                        StatementKind::Commit => {
                            if let Some(t) = self.close(None) {
                                return Ok(SourceItem::Txn(t));
                            }
                        }
                        StatementKind::Rollback => {
                            // Nothing changed, but the group took a GTID.
                            if let Some(g) = self.group.as_mut() {
                                g.txn.rows.clear();
                            }
                            let _ = self.close(None);
                        }
                        k @ (StatementKind::XaCommit | StatementKind::XaRollback) => {
                            let x = self.group.as_ref().and_then(|g| g.xa.clone());
                            let mark = x.map(|x| {
                                if k == StatementKind::XaCommit {
                                    XaMark::Commit(x)
                                } else {
                                    XaMark::Rollback(x)
                                }
                            });
                            if let Some(t) = self.close(mark) {
                                return Ok(SourceItem::Txn(t));
                            }
                        }
                        StatementKind::Ddl => {
                            let tenant = self.tenant;
                            let mine = is_tenant_schema(&db, tenant)
                                || !schemas_in(&sql, tenant).is_empty();
                            if mine && let Some(g) = self.group.as_mut() {
                                g.txn.ddl = Some(RawDdl { db, sql });
                            }
                            if standalone && let Some(t) = self.close(None) {
                                return Ok(SourceItem::Txn(t));
                            }
                        }
                        StatementKind::Dml => {
                            if is_tenant_schema(&db, self.tenant)
                                && let Some(g) = self.group.as_mut()
                            {
                                g.txn.statement_dml = Some(db);
                            }
                            if standalone && let Some(t) = self.close(None) {
                                return Ok(SourceItem::Txn(t));
                            }
                        }
                        StatementKind::Other => {
                            if standalone && let Some(t) = self.close(None) {
                                return Ok(SourceItem::Txn(t));
                            }
                        }
                    }
                }
                Event::Xid(_) => {
                    if let Some(t) = self.close(None) {
                        return Ok(SourceItem::Txn(t));
                    }
                }
                Event::XaPrepare { xid, one_phase } => {
                    let mark = (!one_phase).then(|| XaMark::Prepare(xid.to_string()));
                    if let Some(t) = self.close(mark) {
                        return Ok(SourceItem::Txn(t));
                    }
                }
                Event::Incident(m) => {
                    return Err(SprigError::new(
                        SprigCode::FeedSnapshotRequired,
                        format!("the server lost binlog events: {m}"),
                    ));
                }
                Event::Heartbeat | Event::DumpEnd => {
                    if self.group.is_none() {
                        self.quiet_since = None;
                        return Ok(SourceItem::Idle {
                            pos: self.pos.clone(),
                        });
                    }
                }
                _ => {}
            }
        }
    }
}

/// Where each cluster's binlog is read: one node and a `sprig_repl`
/// account per cluster.
#[derive(Debug, Clone, Default)]
pub struct StaticClusters {
    sources: HashMap<ClusterId, BinlogSource>,
    pools: Arc<std::sync::Mutex<HashMap<ClusterId, Pool>>>,
    next_server_id: Arc<AtomicU32>,
}

/// The first binlog client server id the feed presents (chapter 10's
/// `mover.server_id_base` is 4000000000; the feed takes the range above
/// 4100000000).
pub const SERVER_ID_BASE: u32 = 4_100_000_000;

impl StaticClusters {
    /// No clusters yet.
    #[must_use]
    pub fn new() -> Self {
        // A per-process offset keeps two processes' dumps on one node from
        // ending each other.
        let offset = std::process::id() % 10_000 * 1_000;
        Self {
            sources: HashMap::new(),
            pools: Arc::default(),
            next_server_id: Arc::new(AtomicU32::new(SERVER_ID_BASE + offset)),
        }
    }

    /// Reads `cluster` from `src`.
    #[must_use]
    pub fn with(mut self, cluster: ClusterId, src: BinlogSource) -> Self {
        self.sources.insert(cluster, src);
        self
    }

    fn source(&self, cluster: &ClusterId) -> Result<&BinlogSource> {
        self.sources.get(cluster).ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no binlog source for cluster {cluster}"),
            )
        })
    }

    fn pool(&self, cluster: &ClusterId) -> Result<Pool> {
        let src = self.source(cluster)?;
        let mut pools = self
            .pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(pools
            .entry(cluster.clone())
            .or_insert_with(|| {
                let opts = OptsBuilder::default()
                    .ip_or_hostname(src.host.clone())
                    .tcp_port(src.port)
                    .user(Some(src.user.clone()))
                    .pass(Some(src.password.clone()))
                    .pool_opts(
                        PoolOpts::new()
                            .with_constraints(PoolConstraints::new(0, 2).unwrap_or_default())
                            .with_inactive_connection_ttl(Duration::from_secs(60)),
                    );
                Pool::new(opts)
            })
            .clone())
    }
}

#[async_trait::async_trait]
impl Clusters for StaticClusters {
    async fn open(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>> {
        let src = self.source(cluster)?;
        let id = self.next_server_id.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(BinlogTxns::connect(src, tenant, from, id).await?))
    }

    async fn position(&self, cluster: &ClusterId) -> Result<GtidPos> {
        let pool = self.pool(cluster)?;
        let unavailable = |e: mysql_async::Error| {
            SprigError::new(
                SprigCode::BackendUnavailable,
                format!("read @@gtid_binlog_pos of {cluster}: {e}"),
            )
        };
        let mut c = pool.get_conn().await.map_err(unavailable)?;
        let p: Option<String> = c
            .query_first("SELECT @@gtid_binlog_pos")
            .await
            .map_err(unavailable)?;
        p.unwrap_or_default().parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_classify_by_their_first_words() {
        assert_eq!(classify("BEGIN"), StatementKind::Begin);
        assert_eq!(classify("  /* x */ commit"), StatementKind::Commit);
        assert_eq!(classify("XA COMMIT X'01',X'',1"), StatementKind::XaCommit);
        assert_eq!(classify("XA END X'01'"), StatementKind::XaOther);
        assert_eq!(
            classify("/*sprig h=0000000000000001 o=i*/ ALTER TABLE a ADD b INT"),
            StatementKind::Ddl
        );
        assert_eq!(classify("truncate table x"), StatementKind::Ddl);
        assert_eq!(classify("INSERT INTO t VALUES (1)"), StatementKind::Dml);
        assert_eq!(classify("SET @a = 1"), StatementKind::Other);
        assert_eq!(classify("-- c\nDROP TABLE t"), StatementKind::Ddl);
    }

    #[test]
    fn finds_a_tenants_schemas_in_statements() {
        let t = TenantId(42);
        assert_eq!(
            schemas_in(
                "RENAME TABLE `t42_d1_g1_s1`.`a` TO `t42_d1_g1_s1`.`__sprig_f_0`, t7_d1_g1_s1.b TO x",
                t
            ),
            vec!["t42_d1_g1_s1".to_owned()]
        );
        assert!(schemas_in("CREATE TABLE t420_x (a INT)", t).is_empty());
        assert!(is_tenant_schema("t42_sprig", t));
        assert!(!is_tenant_schema("t4_sprig", t));
    }
}
