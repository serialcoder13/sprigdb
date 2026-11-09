//! Chapter 18's coordinator in the gateway ("The gateway's side"): a
//! [`TxnBackend`] whose branches are [`BackendSession`]s, and the admin
//! the coordinator finishes prepared branches with.
//!
//! - `caps(cluster)`: the cluster's capability profile (`derived.xa`),
//!   read when the session's layout names the cluster.
//! - `checkout(cluster)`: a new `BackendSession` for that cluster with the
//!   session's backend account (reconciled there first under per-user
//!   accounts) and the session's state, replayed on its connection; a
//!   branch pins its connection, so its statements go to the writer.
//! - `admin`: `sprig-txn::mysql::MyAdmin` on `sprig_ctl` connections, one
//!   small pool per cluster, per gateway.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use sprig_auth::BackendTarget;
use sprig_catalog::TenantCatalog;
use sprig_common::ids::ClusterId;
use sprig_txn::mysql::{MyAdmin, MyCluster};
use sprig_txn::{
    ClusterAdmin, ClusterCaps, DbError, DbResult, Decision, DecisionRow, Dtid, TxnBackend, Xid,
};

use crate::backend::state::SessionVars;
use crate::backend::{BackendSession, Backends, SessionOpts};
use crate::tenant::TenantServices;
use crate::tenant::locator::GatewayLocator;

/// One session's branch factory.
pub(crate) struct GwTxn {
    pub(super) backends: Arc<Backends>,
    pub(super) target: BackendTarget,
    pub(super) opts: SessionOpts,
    pub(super) main: ClusterId,
    /// The session's state, copied before each statement.
    pub(super) vars: Mutex<SessionVars>,
    pub(super) caps: Mutex<HashMap<ClusterId, ClusterCaps>>,
    pub(super) admin: Arc<GwAdmin>,
    pub(super) svc: Arc<TenantServices>,
    pub(super) cat: Mutex<Option<Arc<TenantCatalog>>>,
}

impl std::fmt::Debug for GwTxn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GwTxn")
            .field("main", &self.main)
            .finish_non_exhaustive()
    }
}

/// A new backend session on `cluster` with `vars` (the session's state;
/// the default schema only on the session's own cluster, where it lives).
pub(super) fn session_on(
    backends: &Arc<Backends>,
    target: &BackendTarget,
    opts: &SessionOpts,
    main: &ClusterId,
    cluster: &ClusterId,
    vars: &SessionVars,
) -> BackendSession {
    let mut t = target.clone();
    t.cluster = cluster.clone();
    let mut o = opts.clone();
    if cluster != main {
        o.schema = None;
    }
    let mut bs = backends.session(t, o);
    let mut v = vars.clone();
    if cluster != main {
        v.schema = None;
    }
    *bs.vars_mut() = v;
    bs
}

/// Per-user backend accounts on `cluster` (chapter 29), before a session
/// first uses it.
pub(super) async fn ensure_accounts(
    svc: &TenantServices,
    cat: Option<&Arc<TenantCatalog>>,
    cluster: &ClusterId,
) -> sprig_common::Result<()> {
    match cat {
        Some(c) if svc.authz.per_user() => svc.authz.ensure(svc, c, cluster).await,
        _ => Ok(()),
    }
}

#[async_trait::async_trait]
impl TxnBackend for GwTxn {
    type Conn = BackendSession;

    fn caps(&self, cluster: &ClusterId) -> ClusterCaps {
        self.caps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            .copied()
            .unwrap_or_default()
    }

    async fn checkout(&self, cluster: &ClusterId) -> DbResult<BackendSession> {
        if cluster != &self.main {
            let cat = self
                .cat
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            ensure_accounts(&self.svc, cat.as_ref(), cluster)
                .await
                .map_err(|e| DbError::new(0, e.to_string()))?;
        }
        let vars = self
            .vars
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        Ok(session_on(
            &self.backends,
            &self.target,
            &self.opts,
            &self.main,
            cluster,
            &vars,
        ))
    }

    fn admin(&self) -> Arc<dyn ClusterAdmin> {
        self.admin.clone()
    }
}

/// The coordinator's admin on `sprig_ctl`, per gateway: a `MyAdmin` per
/// cluster, made at first use.
pub(crate) struct GwAdmin {
    locator: Arc<GatewayLocator>,
    admins: tokio::sync::Mutex<HashMap<ClusterId, Arc<MyAdmin>>>,
}

impl std::fmt::Debug for GwAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GwAdmin").finish_non_exhaustive()
    }
}

impl GwAdmin {
    /// An admin finding clusters through `locator`.
    #[must_use]
    pub(crate) fn new(locator: Arc<GatewayLocator>) -> Self {
        Self {
            locator,
            admins: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn of(&self, cluster: &ClusterId) -> DbResult<Arc<MyAdmin>> {
        let mut m = self.admins.lock().await;
        if let Some(a) = m.get(cluster) {
            return Ok(Arc::clone(a));
        }
        let opts = self
            .locator
            .ctl_opts(cluster)
            .await
            .map_err(|e| DbError::lost(e.to_string()))?;
        let mut clusters = std::collections::BTreeMap::new();
        clusters.insert(
            cluster.clone(),
            MyCluster {
                opts: opts.clone(),
                admin_opts: opts,
                caps: ClusterCaps::default(),
            },
        );
        let a = Arc::new(MyAdmin::new(&clusters));
        m.insert(cluster.clone(), Arc::clone(&a));
        Ok(a)
    }
}

#[async_trait::async_trait]
impl ClusterAdmin for GwAdmin {
    async fn xa_recover(&self, c: &ClusterId) -> DbResult<Vec<Xid>> {
        self.of(c).await?.xa_recover(c).await
    }
    async fn xa_finish(&self, c: &ClusterId, xid: &Xid, commit: bool) -> DbResult<()> {
        self.of(c).await?.xa_finish(c, xid, commit).await
    }
    async fn kill(&self, c: &ClusterId, thread: u64) -> DbResult<()> {
        self.of(c).await?.kill(c, thread).await
    }
    async fn insert_decision(
        &self,
        c: &ClusterId,
        schema: &str,
        row: &DecisionRow,
    ) -> DbResult<()> {
        self.of(c).await?.insert_decision(c, schema, row).await
    }
    async fn read_decision(
        &self,
        c: &ClusterId,
        schema: &str,
        dtid: Dtid,
    ) -> DbResult<Option<DecisionRow>> {
        self.of(c).await?.read_decision(c, schema, dtid).await
    }
    async fn settle_pending(
        &self,
        c: &ClusterId,
        schema: &str,
        dtid: Dtid,
        o: Decision,
    ) -> DbResult<()> {
        self.of(c).await?.settle_pending(c, schema, dtid, o).await
    }
    async fn decision_rows(&self, c: &ClusterId) -> DbResult<Vec<(String, DecisionRow)>> {
        self.of(c).await?.decision_rows(c).await
    }
    async fn delete_decision(&self, c: &ClusterId, schema: &str, dtid: Dtid) -> DbResult<()> {
        self.of(c).await?.delete_decision(c, schema, dtid).await
    }
}

/// Chapter 19's lookup statements on the gateway's own connections
/// (`sprig_txn::lookup::DistTxn`'s Pre, Main and Post): parameters are
/// written into the text as literals (strings as `_utf8mb4 X'…'`, so the
/// column's collation compares them), rows come back as `mysql_async`
/// rows of text values.
#[async_trait::async_trait]
impl sprig_txn::lookup::DistConn for BackendSession {
    async fn run(&mut self, sql: &str) -> DbResult<()> {
        let mut budget = sprig_router::Budget::mb(16);
        super::backend::run_on(self, sql, &mut budget)
            .await
            .map(|_| ())
            .map_err(|e| as_db(&e))
    }

    async fn query(
        &mut self,
        sql: &str,
        params: mysql_async::Params,
    ) -> DbResult<Vec<mysql_async::Row>> {
        let text = inline_params(sql, params).map_err(|m| DbError::new(1210, m))?;
        let mut budget = sprig_router::Budget::mb(64);
        match super::backend::run_on(self, &text, &mut budget).await {
            Ok(sprig_router::ShardOutcome::Rows(r)) => Ok(to_rows(&r)),
            Ok(sprig_router::ShardOutcome::Ok(_)) => Ok(Vec::new()),
            Err(e) => Err(as_db(&e)),
        }
    }
}

fn as_db(e: &sprig_common::SprigError) -> DbError {
    super::backend::db_error(e).unwrap_or_else(|| DbError::new(0, e.to_string()))
}

fn to_rows(r: &sprig_router::ShardRows) -> Vec<mysql_async::Row> {
    use mysql_common::constants::ColumnType;
    use mysql_common::packets::Column;
    let cols: Arc<[Column]> = r
        .columns
        .iter()
        .map(|c| Column::new(ColumnType::MYSQL_TYPE_VAR_STRING).with_name(&c.name))
        .collect();
    r.rows
        .iter()
        .map(|row| {
            let vals = row
                .iter()
                .map(|v| {
                    v.as_ref().map_or(mysql_async::Value::NULL, |b| {
                        mysql_async::Value::Bytes(b.to_vec())
                    })
                })
                .collect();
            mysql_common::row::new_row(vals, Arc::clone(&cols))
        })
        .collect()
}

/// `sql` with each `?` outside quotes replaced by its positional value as
/// a literal.
fn inline_params(sql: &str, params: mysql_async::Params) -> Result<String, String> {
    let vals = match params {
        mysql_async::Params::Empty => Vec::new(),
        mysql_async::Params::Positional(v) => v,
        mysql_async::Params::Named(_) => return Err("named parameters".to_owned()),
    };
    let mut out = String::with_capacity(sql.len() + 32 * vals.len());
    let mut it = vals.iter();
    let mut quote: Option<char> = None;
    for c in sql.chars() {
        match quote {
            Some(q) => {
                out.push(c);
                if c == q {
                    quote = None;
                }
            }
            None if matches!(c, '`' | '\'' | '"') => {
                quote = Some(c);
                out.push(c);
            }
            None if c == '?' => {
                let v = it.next().ok_or("too few parameters")?;
                out.push_str(&literal(v));
            }
            None => out.push(c),
        }
    }
    if it.next().is_some() {
        return Err("too many parameters".to_owned());
    }
    Ok(out)
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter()
        .fold(String::with_capacity(2 * b.len()), |mut s, x| {
            let _ = write!(s, "{x:02X}");
            s
        })
}

fn literal(v: &mysql_async::Value) -> String {
    use mysql_async::Value as V;
    match v {
        V::NULL => "NULL".to_owned(),
        V::Bytes(b) if std::str::from_utf8(b).is_ok() => format!("_utf8mb4 X'{}'", hex(b)),
        V::Bytes(b) => format!("_binary X'{}'", hex(b)),
        V::Int(i) => i.to_string(),
        V::UInt(u) => u.to_string(),
        V::Float(f) => format!("{f:e}"),
        V::Double(d) => format!("{d:e}"),
        V::Date(year, month, day, hour, minute, second, micros) => {
            format!("'{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}'")
        }
        V::Time(neg, days, hour, minute, second, micros) => {
            let hours = u64::from(*days) * 24 + u64::from(*hour);
            let sign = if *neg { "-" } else { "" };
            format!("'{sign}{hours}:{minute:02}:{second:02}.{micros:06}'")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parameters_become_literals() {
        let p = mysql_async::Params::Positional(vec![
            mysql_async::Value::Bytes(b"a@x".to_vec()),
            mysql_async::Value::Int(-4),
            mysql_async::Value::NULL,
            mysql_async::Value::Bytes(vec![0xff]),
        ]);
        assert_eq!(
            inline_params(
                "SELECT `q?` FROM t WHERE a = ? AND b = ? AND c <=> ? AND d = ?",
                p
            )
            .as_deref(),
            Ok(
                "SELECT `q?` FROM t WHERE a = _utf8mb4 X'614078' AND b = -4 AND c <=> NULL \
                 AND d = _binary X'FF'"
            )
        );
        assert!(inline_params("? ?", mysql_async::Params::Positional(vec![1.into()])).is_err());
    }
}
