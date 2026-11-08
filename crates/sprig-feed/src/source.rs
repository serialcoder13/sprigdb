//! The seam between a cluster's binlog and the feed's logic.
//!
//! [`TxnSource`] yields one cluster's committed transactions, already
//! assembled from binlog events and cut down to one tenant's schemas plus
//! the internal tables the filters need. Production builds it on
//! `sprig-binlog` ([`crate::binlog`]); the simulation
//! (`sprig-testkit::sim::mover`) builds it on its binlog model, so both run
//! the same tail ([`crate::tail`]).

use std::sync::Arc;

use sprig_binlog::{Gtid, GtidPos, RowImage};
use sprig_common::Result;
use sprig_common::ids::{ClusterId, TenantId};

/// A table as its rows events describe it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableShape {
    /// The physical schema.
    pub schema: String,
    /// The table.
    pub table: String,
    /// Column names, in table order.
    pub columns: Vec<String>,
    /// Primary-key column indexes (empty if the binlog didn't say).
    pub key: Vec<usize>,
}

/// The kind of a row change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    /// After image only.
    Insert,
    /// Before and after images.
    Update,
    /// Before image only.
    Delete,
}

/// One row change.
#[derive(Debug, Clone, PartialEq)]
pub struct RawRow {
    /// Its table.
    pub table: Arc<TableShape>,
    /// What happened.
    pub kind: RowKind,
    /// The row before.
    pub before: Option<RowImage>,
    /// The row after.
    pub after: Option<RowImage>,
}

/// A statement event (`QUERY`) that the feed may turn into an object event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawDdl {
    /// The default database it ran in.
    pub db: String,
    /// The statement.
    pub sql: String,
}

/// What an XA group is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XaMark {
    /// `XA PREPARE` on a file binlog: hold the rows until the commit group.
    Prepare(String),
    /// `XA COMMIT` of a prepared branch: publish the held rows.
    Commit(String),
    /// `XA ROLLBACK` of a prepared branch: discard them.
    Rollback(String),
}

/// One committed binlog group, cut down to what the feed reads.
#[derive(Debug, Clone, PartialEq)]
pub struct RawTxn {
    /// The group's GTID.
    pub gtid: Gtid,
    /// The stream's position before the group.
    pub before: GtidPos,
    /// The event header's timestamp (seconds) of the group's first event.
    pub ts: u32,
    /// HLC stamps from the statements' comments.
    pub stamps: Vec<u64>,
    /// Row changes to the tenant's schemas, in binlog order.
    pub rows: Vec<RawRow>,
    /// A DDL or `TRUNCATE` statement naming one of the tenant's schemas.
    pub ddl: Option<RawDdl>,
    /// Statement-format DML ran in this tenant schema.
    pub statement_dml: Option<String>,
    /// The group writes `__sprig_mover_pos` or `__sprig_ref_pos`: it is an
    /// applier's (filter rule 2).
    pub apply: bool,
    /// The XA role of the group, if any.
    pub xa: Option<XaMark>,
}

impl RawTxn {
    /// An empty group.
    #[must_use]
    pub fn new(gtid: Gtid, before: GtidPos, ts: u32) -> Self {
        Self {
            gtid,
            before,
            ts,
            stamps: Vec::new(),
            rows: Vec::new(),
            ddl: None,
            statement_dml: None,
            apply: false,
            xa: None,
        }
    }

    /// The position after the group.
    #[must_use]
    pub fn after(&self) -> GtidPos {
        let mut p = self.before.clone();
        p.update(self.gtid);
        p
    }
}

/// What a [`TxnSource`] yields.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceItem {
    /// A committed group that may hold the tenant's changes.
    Txn(RawTxn),
    /// The stream reached `pos` with nothing for the tenant since the last
    /// item (idle, or other tenants' groups).
    Idle {
        /// The stream's position.
        pos: GtidPos,
    },
}

/// One cluster's transactions for one tenant.
#[async_trait::async_trait]
pub trait TxnSource: Send {
    /// The next item.
    ///
    /// # Errors
    /// 39113 when the stream broke (reopen from the last position); 39800
    /// when the cluster no longer has the position; 39112 on a corrupt
    /// event.
    async fn next(&mut self) -> Result<SourceItem>;
}

/// Opens [`TxnSource`]s on clusters.
#[async_trait::async_trait]
pub trait Clusters: Send + Sync {
    /// A source on `cluster` for `tenant`'s schemas, starting after `from`
    /// (the empty position: the oldest binlog).
    ///
    /// # Errors
    /// 39800 when the cluster no longer has `from`; connection errors
    /// (retryable).
    async fn open(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>>;

    /// The cluster's binlog position now (`@@gtid_binlog_pos`).
    ///
    /// # Errors
    /// Connection errors (retryable).
    async fn position(&self, cluster: &ClusterId) -> Result<GtidPos>;
}
