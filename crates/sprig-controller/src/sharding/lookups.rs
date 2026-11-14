//! Global lookup indexes in the sharding loop (chapters 13, 16 and 19):
//! the advisor's proposals and the recovery of stopped builds.
//!
//! ```text
//! lookup_tick(tenant), after the groups' ticks:
//!   1. resume: every lookup job no one touched for BUILD_LEASE (its
//!      gateway or controller died) is taken over and finished
//!   2. at most every LOOKUP_EVERY, the advisor: fingerprints of the last
//!      hour that ran at least LOOKUP_MIN_RUNS times, at least
//!      auto_index.lookup_scatter_pct (80%) of them on every shard of a
//!      sharded group, with `=` or `IN` on one column of a single table;
//!      a column that carries a declared single-column UNIQUE key without
//!      the distribution key and has no lookup yet is a candidate
//!   3. each candidate goes through the approvals gate as `advisor.lookup`
//!      (advisor.mode: on builds at once; approve and recommend raise an
//!      item and build once it is approved); the build is
//!      sprig_shard::lookup::admin's, as for ADD UNIQUE
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use mysql_async::prelude::Queryable as _;
use mysql_async::{Conn, Pool};
use sprig_common::ids::{ClusterId, DbId, GroupId, ObjectId, TenantId};
use sprig_common::names::{SchemaName, quote};
use sprig_common::{Result, SprigError};
use sprig_shard::lookup::admin::{Admin, LookupHost, NewLookup, lookup_keys, owner_shape};
use sprig_shard::lookup::direct::Pools;
use sprig_shard::lookup::{LookupCatalog, LookupOrigin};
use sprig_shard::shard_schema;
use sprig_workload::shape::PredOp;
use sprig_workload::store::WorkloadTables;

use super::{ShardingLoop, TenantSettings, lock};

/// How often a tenant's workload is read for lookup proposals.
pub const LOOKUP_EVERY: Duration = Duration::from_secs(300);
/// The window the advisor reads.
pub const LOOKUP_WINDOW: Duration = Duration::from_secs(3600);
/// Fewest runs of a column's scatter reads in the window to propose a
/// lookup.
pub const LOOKUP_MIN_RUNS: u64 = 100;
/// `auto_index.lookup_scatter_pct`'s default.
pub const LOOKUP_SCATTER_PCT: u64 = 80;
/// How long a publish waits for gateways to load the new catalog version.
pub const PUBLISH_SETTLE: Duration = Duration::from_millis(2500);

/// A column the workload reads by equality across every shard.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scatter {
    /// Runs in the window.
    pub runs: u64,
    /// Of them, on every shard.
    pub scattered: u64,
    /// Their time, µs.
    pub total_us: u64,
    /// Fingerprints.
    pub fingerprints: Vec<u64>,
}

/// Folds one fingerprint into the per-column tally: single-table shapes
/// with `=` or `IN` on a column, by `(object, column)`.
pub fn tally(
    out: &mut BTreeMap<(ObjectId, String), Scatter>,
    fp: u64,
    shape: &sprig_workload::shape::Shape,
    runs: u64,
    scattered: u64,
    total_us: u64,
) {
    let [t] = shape.tables.as_slice() else {
        return;
    };
    let Some(object) = t.object_id else {
        return;
    };
    let mut cols: Vec<String> = shape
        .predicates
        .iter()
        .filter(|p| matches!(p.op, PredOp::Eq | PredOp::In { .. }))
        .map(|p| p.column.col.to_ascii_lowercase())
        .collect();
    cols.sort();
    cols.dedup();
    for c in cols {
        let e = out.entry((object, c)).or_default();
        e.runs += runs;
        e.scattered += scattered;
        e.total_us += total_us;
        e.fingerprints.push(fp);
    }
}

/// Whether a tally qualifies.
#[must_use]
pub fn qualifies(s: &Scatter) -> bool {
    s.runs >= LOOKUP_MIN_RUNS && s.scattered * 100 >= s.runs * LOOKUP_SCATTER_PCT
}

/// [`LookupHost`] for the controller: the catalog on the tenant's home
/// cluster, the loop's writer pools, and the loop's publish.
struct CtlHost<'a> {
    l: &'a ShardingLoop,
    tenant: TenantId,
    home: ClusterId,
}

#[async_trait::async_trait]
impl LookupHost for CtlHost<'_> {
    async fn catalog(&self) -> Result<Conn> {
        let mut c = self.l.conn(&self.home).await?;
        c.query_drop(format!(
            "USE {}",
            quote(&SchemaName::catalog(self.tenant).to_string())
        ))
        .await
        .map_err(|e| SprigError::internal(format!("catalog: {e}")))?;
        Ok(c)
    }

    async fn pools(&self, clusters: &[ClusterId]) -> Result<Pools> {
        let mut m: std::collections::HashMap<ClusterId, Pool> = std::collections::HashMap::new();
        for c in clusters {
            m.insert(c.clone(), self.l.pools.pool(c).await?);
        }
        Ok(Arc::new(m))
    }

    async fn publish(&self) -> Result<()> {
        if let Some(p) = &self.l.publish {
            p.catalog_changed(self.tenant).await?;
            tokio::time::sleep(PUBLISH_SETTLE).await;
        }
        Ok(())
    }
}

/// One proposal.
#[derive(Debug, Clone)]
struct Candidate {
    object: ObjectId,
    db: DbId,
    db_name: String,
    table: String,
    group: GroupId,
    key: String,
    column: String,
    scatter: Scatter,
}

impl ShardingLoop {
    /// Step 4 of a tick: lookup jobs to resume, then the advisor's
    /// lookup proposals (module docs).
    ///
    /// # Errors
    /// Catalog errors; a failed build is logged and reported to its
    /// approval item.
    pub(super) async fn lookup_tick(
        &self,
        tenant: TenantId,
        home: &ClusterId,
        ts: &TenantSettings,
    ) -> Result<()> {
        let host = CtlHost {
            l: self,
            tenant,
            home: home.clone(),
        };
        let admin = Admin {
            tenant,
            maps: &self.maps,
            host: &host,
            chunk: ts.chunk_rows,
        };
        for (id, what) in admin.resume().await? {
            tracing::info!(tenant = %tenant, lookup = id.0, outcome = %what, "lookup job resumed");
        }
        if ts.advisor.mode == sprig_advisor::sharding::AdvisorMode::Off {
            return Ok(());
        }
        {
            let mut last = lock(&self.lookup_runs);
            if last
                .get(&tenant)
                .is_some_and(|t| t.elapsed() < LOOKUP_EVERY)
            {
                return Ok(());
            }
            last.insert(tenant, Instant::now());
        }
        for c in self.lookup_candidates(tenant, home).await? {
            self.propose_lookup(tenant, ts, &admin, &c).await;
        }
        Ok(())
    }

    /// The columns worth a unique lookup now (module docs).
    async fn lookup_candidates(
        &self,
        tenant: TenantId,
        home: &ClusterId,
    ) -> Result<Vec<Candidate>> {
        let mut c = self.conn(home).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        let has: Option<u64> = c
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME IN ('workload_stats', 'lookup_indexes')",
                (cat.as_str(),),
            )
            .await
            .map_err(|e| SprigError::internal(format!("catalog: {e}")))?;
        if has.unwrap_or(0) < 2 {
            return Ok(Vec::new());
        }
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let wl = WorkloadTables::for_tenant(tenant);
        let stats = wl
            .merged_stats(
                &mut c,
                now.saturating_sub(LOOKUP_WINDOW.as_secs()),
                now + 60,
            )
            .await?;
        let busy: Vec<(u64, u64, u64, u64)> = stats
            .iter()
            .filter(|(_, s)| s.exec_count >= 1 && s.scatter_count > 0)
            .map(|(id, s)| (*id, s.exec_count, s.scatter_count, s.total_us))
            .collect();
        if busy.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<sprig_common::ids::Fingerprint> = busy
            .iter()
            .map(|(id, ..)| sprig_common::ids::Fingerprint(*id))
            .collect();
        let fps = wl.fingerprints(&mut c, &ids).await?;
        let mut by_col: BTreeMap<(ObjectId, String), Scatter> = BTreeMap::new();
        for f in &fps {
            let Some(&(id, runs, scattered, us)) = busy.iter().find(|b| b.0 == f.id.0) else {
                continue;
            };
            tally(&mut by_col, id, &f.shape, runs, scattered, us);
        }
        c.query_drop(format!("USE {}", quote(&cat)))
            .await
            .map_err(|e| SprigError::internal(format!("catalog: {e}")))?;
        let existing = LookupCatalog.rows(&mut c).await?;
        let mut out = Vec::new();
        for ((object, column), s) in by_col {
            if !qualifies(&s) {
                continue;
            }
            if existing
                .iter()
                .any(|l| l.owner == object && l.columns.as_slice() == [column.clone()])
            {
                continue;
            }
            let member: Option<(u32, u32, String, Option<String>, String, String, String)> = c
                .exec_first(
                    "SELECT m.group_id, m.db_id, CAST(m.routing AS CHAR), \
                     CAST(m.key_columns AS CHAR), CAST(m.rk_state AS CHAR), o.name, d.name \
                     FROM group_members m JOIN table_groups g ON g.group_id = m.group_id \
                     JOIN objects o ON o.object_id = m.object_id \
                     JOIN `databases` d ON d.db_id = m.db_id \
                     WHERE m.object_id = ? AND g.kind = 'sharded' AND g.state = 'active'",
                    (object.0,),
                )
                .await
                .map_err(|e| SprigError::internal(format!("catalog: {e}")))?;
            let Some((group, db, routing, key, rk, table, db_name)) = member else {
                continue;
            };
            if routing != "columns" || rk != "ready" {
                continue;
            }
            let key: Vec<String> = key
                .and_then(|k| serde_json::from_str::<Vec<String>>(&k).ok())
                .unwrap_or_default()
                .into_iter()
                .map(|k| k.to_ascii_lowercase())
                .collect();
            let Ok((map, _)) = self.maps.load(tenant, GroupId(group)).await else {
                continue;
            };
            if map.ranges.len() < 2 {
                continue; // one shard: nothing scatters
            }
            let Some(r) = map.ranges.first() else {
                continue;
            };
            let schema = shard_schema(tenant, DbId(db), GroupId(group), r.shard);
            let Ok(mut sc) = self.conn(&r.cluster).await else {
                continue;
            };
            let Ok(shape) = owner_shape(&mut sc, &schema, &table).await else {
                continue;
            };
            if let Some((k, _)) = lookup_keys(&shape, &key)
                .into_iter()
                .find(|(_, col)| *col == column)
            {
                out.push(Candidate {
                    object,
                    db: DbId(db),
                    db_name,
                    table,
                    group: GroupId(group),
                    key: k,
                    column,
                    scatter: s,
                });
            }
        }
        Ok(out)
    }

    /// The approvals gate for one candidate, and the build once it may go.
    async fn propose_lookup(
        &self,
        tenant: TenantId,
        ts: &TenantSettings,
        admin: &Admin<'_>,
        c: &Candidate,
    ) {
        use sprig_approvals::{Action, Kind, Proposal, Verdict};
        let ticket = match self.approvals.get() {
            Some(a) => {
                let policy = a
                    .policy(
                        Kind::AdvisorLookup,
                        Some(tenant),
                        Some(c.db),
                        Some(c.object),
                        None,
                    )
                    .await;
                let mut subject = a.subject(tenant).await;
                subject.db = Some(c.db);
                subject.db_name = Some(c.db_name.clone());
                subject.object = Some(c.object);
                subject.object_name = Some(c.table.clone());
                let ms = c.scatter.total_us as f64 / 1000.0;
                let p = Proposal {
                    kind: Kind::AdvisorLookup,
                    subject,
                    target: format!("t{tenant}/o{}/lookup/{}", c.object.0, c.key),
                    action: Action {
                        text: format!(
                            "SPRIG REBUILD LOOKUP INDEX `{}` ON `{}`.`{}`",
                            c.key, c.db_name, c.table
                        ),
                        machine: serde_json::json!({
                            "object": c.object.0,
                            "group": c.group.0,
                            "key": c.key,
                            "column": c.column,
                        }),
                    },
                    evidence: serde_json::json!({
                        "runs": c.scatter.runs,
                        "scattered": c.scatter.scattered,
                        "total_ms": ms,
                        "fingerprints": c.scatter.fingerprints,
                        "window_hours": 1,
                    }),
                    score: ms,
                    summary: format!(
                        "global lookup index on {}.{} ({}, unique key {}): {} of {} reads in \
                         the last hour went to every shard ({:.0} ms)",
                        c.db_name,
                        c.table,
                        c.column,
                        c.key,
                        c.scatter.scattered,
                        c.scatter.runs,
                        ms
                    ),
                    emergency: false,
                };
                match a.gate(&p, &policy).await {
                    Verdict::Proceed(t) => Some(t),
                    other => {
                        tracing::debug!(tenant = %tenant, table = %c.table, verdict = ?other,
                            "lookup proposal held");
                        return;
                    }
                }
            }
            None if ts.advisor.mode == sprig_advisor::sharding::AdvisorMode::On => None,
            None => return,
        };
        let r = async {
            let (map, _) = self.maps.load(tenant, c.group).await?;
            admin
                .create(&NewLookup {
                    db: c.db,
                    owner: c.object,
                    owner_table: c.table.clone(),
                    owner_map: Arc::new(map),
                    columns: vec![c.column.clone()],
                    key: Some(c.key.clone()),
                    origin: LookupOrigin::UniqueKey,
                })
                .await
        }
        .await;
        let (ok, detail) = match &r {
            Ok((l, rep)) => (
                true,
                format!(
                    "lookup index {} built: {} rows, {} repaired",
                    l.index_name, rep.rows, rep.repaired
                ),
            ),
            Err(e) => (false, format!("lookup index {} failed: {e}", c.key)),
        };
        tracing::info!(tenant = %tenant, table = %c.table, ok, detail = %detail, "advisor lookup");
        self.approval_done(ticket.as_ref().filter(|t| t.id.is_some()), ok, &detail)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_workload::shape::{Shape, StmtType};

    #[test]
    fn scatter_reads_by_column_are_tallied() {
        let mut out = BTreeMap::new();
        let s = Shape {
            tables: vec![sprig_workload::shape::TableRef {
                alias: "customers".into(),
                object_id: Some(ObjectId(7)),
                db: None,
                table: "customers".into(),
                db_id: None,
                derived: false,
            }],
            predicates: vec![
                sprig_workload::shape::Predicate {
                    column: sprig_workload::shape::ColumnRef {
                        table: None,
                        col: "Email".into(),
                    },
                    op: PredOp::Eq,
                    args: vec![0],
                },
                sprig_workload::shape::Predicate {
                    column: sprig_workload::shape::ColumnRef {
                        table: None,
                        col: "age".into(),
                    },
                    op: PredOp::Range {
                        lo: true,
                        hi: false,
                    },
                    args: vec![1],
                },
            ],
            ..Shape::empty(StmtType::Select)
        };
        tally(&mut out, 1, &s, 90, 90, 9000);
        tally(&mut out, 2, &s, 30, 10, 1000);
        let e = &out[&(ObjectId(7), "email".to_owned())];
        assert_eq!((e.runs, e.scattered, e.total_us), (120, 100, 10_000));
        assert_eq!(out.len(), 1, "ranges don't count");
        assert!(qualifies(e));
        assert!(!qualifies(&Scatter {
            runs: 120,
            scattered: 90,
            ..Scatter::default()
        }));
        assert!(!qualifies(&Scatter {
            runs: 99,
            scattered: 99,
            ..Scatter::default()
        }));
        // Two tables (a join): not a lookup's read.
        let mut j = s.clone();
        j.tables.push(j.tables[0].clone());
        let mut none = BTreeMap::new();
        tally(&mut none, 3, &j, 500, 500, 1);
        assert!(none.is_empty());
    }
}
