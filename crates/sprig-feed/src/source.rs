//! The seam between a cluster's binlog and the feed's logic.
//!
//! [`TxnSource`] yields one cluster's committed transactions, already
//! assembled from binlog events and cut down to one tenant's schemas plus
//! the internal tables the filters need. Production builds it on
//! `sprig-binlog` ([`crate::binlog`]); the simulation
//! (`sprig-testkit::sim::mover`) builds it on its binlog model, so both run
//! the same tail ([`crate::tail`]).

use std::collections::BTreeSet;
use std::sync::Arc;

use sprig_binlog::{Gtid, GtidPos, OpHint, RowImage};
use sprig_common::Result;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::names::SchemaName;

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
    /// The operation hint (`o=`) of the statement's SprigDB comment, when
    /// it had one (chapter 38, "Statement comments"): a replacement shows
    /// as `r` on an update.
    pub hint: Option<OpHint>,
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

    /// One source on `cluster` for **every** tenant in `scope`, starting
    /// after `from`: the shared per-cluster reader ([`crate::shared`])
    /// splits it by tenant.
    ///
    /// # Errors
    /// 39800 when the cluster no longer has `from`; connection errors.
    /// The default: not supported (`ER_SPRIG_INTERNAL`).
    async fn open_scope(
        &self,
        cluster: &ClusterId,
        scope: Arc<DumpScope>,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>> {
        let _ = (scope, from);
        Err(sprig_common::SprigError::new(
            sprig_common::SprigCode::Internal,
            format!("no cluster-wide binlog source for {cluster}"),
        ))
    }

    /// The cluster's binlog position now (`@@gtid_binlog_pos`).
    ///
    /// # Errors
    /// Connection errors (retryable).
    async fn position(&self, cluster: &ClusterId) -> Result<GtidPos>;
}

/// What a cluster-wide dump keeps ([`Clusters::open_scope`]; R9, r9feed).
///
/// A row of `schema`.`table` is kept when the schema is a tenant's and:
/// the tenant is kept whole (`tenants`), or the table is one of `tables`
/// and the schema isn't a catalog. A whole tenant also gets the DDL and
/// statement-format DML naming its schemas, so its rows and statements are
/// what [`Clusters::open`] would give it. `only` cuts the dump down to one
/// tenant: a catch-up reader's.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DumpScope {
    /// Internal tables kept in every tenant's shard and reference schemas:
    /// exact names, or name prefixes ending in `*` (`__sprig_eo_*`).
    pub tables: Vec<String>,
    /// Tenants kept whole.
    pub tenants: BTreeSet<TenantId>,
    /// Only this tenant's schemas.
    pub only: Option<TenantId>,
}

impl DumpScope {
    /// Whether `table` matches `pattern` (an exact name, or a prefix ending
    /// in `*`).
    #[must_use]
    pub fn table_matches(pattern: &str, table: &str) -> bool {
        match pattern.strip_suffix('*') {
            Some(prefix) => table.starts_with(prefix),
            None => pattern == table,
        }
    }

    /// Whether `table` matches one of `tables`.
    #[must_use]
    pub fn keeps_table(&self, table: &str) -> bool {
        self.tables.iter().any(|p| Self::table_matches(p, table))
    }

    /// Whether tenant `t` is kept whole.
    #[must_use]
    pub fn whole(&self, t: TenantId) -> bool {
        self.tenants.contains(&t) && self.only.is_none_or(|o| o == t)
    }

    /// Whether rows of `schema`.`table` are kept (the applier marks of
    /// `__sprig_sys` aside: every dump keeps those).
    #[must_use]
    pub fn keeps(&self, schema: &str, table: &str) -> bool {
        let Ok(s) = schema.parse::<SchemaName>() else {
            return false;
        };
        let t = s.tenant();
        if self.only.is_some_and(|o| o != t) {
            return false;
        }
        if self.tenants.contains(&t) {
            return true;
        }
        !matches!(s, SchemaName::Catalog { .. }) && self.keeps_table(table)
    }

    /// The tenant of a schema name (any of its schemas, the catalog too).
    #[must_use]
    pub fn tenant_of(schema: &str) -> Option<TenantId> {
        schema.parse::<SchemaName>().ok().map(|s| s.tenant())
    }

    /// Every tenant a statement names: the tenant of its default database
    /// and of each word that parses as a schema name, in that order, once.
    #[must_use]
    pub fn tenants_named(db: &str, sql: &str) -> Vec<TenantId> {
        let mut out: Vec<TenantId> = Vec::new();
        let words = std::iter::once(db).chain(
            sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .filter(|w| w.starts_with('t')),
        );
        for w in words {
            if let Some(t) = Self::tenant_of(w)
                && !out.contains(&t)
            {
                out.push(t);
            }
        }
        out
    }

    /// Whether a statement names a tenant kept whole.
    #[must_use]
    pub fn names_whole(&self, db: &str, sql: &str) -> bool {
        !self.tenants.is_empty()
            && Self::tenants_named(db, sql)
                .into_iter()
                .any(|t| self.whole(t))
    }
}

#[cfg(test)]
mod scope_tests {
    use super::*;

    #[test]
    fn scope_keeps_tables_and_whole_tenants() {
        let s = DumpScope {
            tables: vec!["__sprig_qo".into(), "__sprig_eo_*".into()],
            tenants: [TenantId(7)].into_iter().collect(),
            only: None,
        };
        assert!(s.keeps("t5_d1_g1_s1", "__sprig_qo"));
        assert!(!s.keeps("t5_d1_g1_s1", "__sprig_qox"));
        assert!(s.keeps("t5_d1_g1_s1", "__sprig_eo_12"));
        assert!(!s.keeps("t5_d1_g1_s1", "orders"));
        assert!(!s.keeps("t5_sprig", "__sprig_qo"));
        assert!(s.keeps("t7_d1_g1_s1", "orders"));
        assert!(s.keeps("t7_sprig", "objects"));
        assert!(!s.keeps("mysql", "user"));
        let one = DumpScope {
            only: Some(TenantId(5)),
            ..s.clone()
        };
        assert!(one.keeps("t5_d1_g1_s1", "__sprig_qo"));
        assert!(!one.keeps("t7_d1_g1_s1", "orders"));
        assert!(!one.whole(TenantId(7)));
        assert_eq!(
            DumpScope::tenants_named("t5_d1_g1_s1", "RENAME TABLE t7_d1_g1_s1.a TO t9_d1_g1_s1.b, t70.x"),
            vec![TenantId(5), TenantId(7), TenantId(9)]
        );
        assert!(s.names_whole("mysql", "ALTER TABLE `t7_d1_g1_s1`.`a` ADD c INT"));
        assert!(!s.names_whole("t5_d1_g1_s1", "ALTER TABLE a ADD c INT"));
    }
}
