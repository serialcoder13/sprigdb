//! The cross-tenant outbox (chapter 42, "Publishing by target tenant",
//! "Inside a database transaction"; chapter 45, "One platform stream for
//! every client"): a publish into another tenant's stream inside a
//! transaction.
//!
//! ```text
//! publish(P's transaction, record r, destination d):
//!   INSERT INTO <db>.__sprig_xo (out_id := UUIDv7, kind 'cross_tenant', dest d, r)
//!     through P's session, in its transaction, in a shard of P's (the
//!     database the statement names, else the session's current one)
//!   -- the table is made on first use as the provisioning account, and its
//!   -- schema is listed under inherit-xo/<P>/<schema> in the DC tier
//! after the request, when the transaction has ended (the commit hook):
//!   forward(those out_ids) in the background; rows a ROLLBACK removed
//!   aren't there
//! sweeper (every gateway, every 5 s): forward(rows older than 5 s) of each
//!   listed schema: rows a crashed gateway or a lost hook left
//! forward(rows):
//!   SELECT ... FOR UPDATE SKIP LOCKED   -- one forwarder per row at a time
//!   d an instance of a definition: create it if needed (chapter 42); d a
//!     stream: that stream
//!   INSERT IGNORE INTO the destination's outbox (event_id := out_id, ...)
//!   -- a second forward is a no-op: the outbox's primary key, or, once
//!   -- sequenced, the log's duplicate check on the event id
//!   DELETE the rows; COMMIT
//! ```
//!
//! The record exists only if P's transaction committed, and lands once.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::{Cause, DefKey};
use sprig_meta::{ListOpts, Record, Versioned};
use sprig_streams::StreamsState;
use sprig_streams::publish::Publish;

use super::catalog;
use crate::tenant::TenantServices;

/// The table, in a shard schema of the publisher.
pub const TABLE: &str = "__sprig_xo";

/// The table's DDL in `schema`.
#[must_use]
pub fn ddl(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
  out_id         BINARY(16) NOT NULL,
  kind           ENUM('cross_tenant','broadcast') NOT NULL,
  dest           JSON NOT NULL,
  k              VARBINARY(4096) NULL,
  v              LONGBLOB NULL,
  headers        JSON NULL,
  partition_no   INT UNSIGNED NULL,
  created_at     DATETIME(3) NOT NULL DEFAULT (UTC_TIMESTAMP(3)),
  PRIMARY KEY (out_id)
) ENGINE=InnoDB",
        quote(schema),
        quote(TABLE)
    )
}

/// Where an outbox row goes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "to", rename_all = "snake_case")]
pub enum Dest {
    /// A member's instance of a definition (`FOR TENANT`, the `tenant`
    /// column).
    Instance {
        /// The member.
        tenant: u64,
        /// The definition's tenant.
        def_tenant: u64,
        /// Its object id.
        def_object: u64,
    },
    /// An ancestor's ordinary stream (`IN TENANT`).
    Stream {
        /// The owner.
        tenant: u64,
        /// The stream.
        stream: u64,
    },
    /// A broadcast (`FOR ALL TENANTS`, `x-sprig-tenant: *`): forwarded
    /// into the definition's broadcast log, whose fan-out delivers it.
    Broadcast {
        /// The definition's tenant.
        def_tenant: u64,
        /// Its object id.
        def_object: u64,
        /// The publisher.
        publisher: u64,
        /// The form.
        form: super::broadcast::Form,
    },
    /// A member's instance of an exchange definition (or, through the
    /// default exchange, of a queue definition): the row's key is the
    /// routing key, its value the body, its headers the stored
    /// properties ([`super::mq::xo_headers`]).
    Exchange {
        /// The member.
        tenant: u64,
        /// The vhost's database.
        db: String,
        /// The exchange (empty: the default one).
        exchange: String,
    },
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(3 + b.len() * 2);
    s.push_str("X'");
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s.push('\'');
    s
}

fn lit(b: Option<&[u8]>) -> String {
    b.map_or_else(|| "NULL".to_owned(), hex)
}

/// A fresh out id (a UUIDv7: it is also the event id in the destination).
#[must_use]
pub fn new_id() -> [u8; 16] {
    *uuid::Uuid::now_v7().as_bytes()
}

/// The insert of `rows` into logical database `db`'s outbox, run by the
/// publisher's session (internal mode, logical names).
#[must_use]
pub fn insert_sql(db: &str, rows: &[([u8; 16], Dest, Publish)]) -> String {
    let values: Vec<String> = rows
        .iter()
        .map(|(id, dest, r)| {
            format!(
                "({}, '{}', {}, {}, {}, {}, {})",
                hex(id),
                if matches!(dest, Dest::Broadcast { .. }) {
                    "broadcast"
                } else {
                    "cross_tenant"
                },
                hex(serde_json::to_string(dest).unwrap_or_default().as_bytes()),
                lit(r.key.as_deref()),
                lit(r.value.as_deref()),
                r.headers.as_deref().map_or_else(
                    || "NULL".to_owned(),
                    |h| format!("CONVERT({} USING utf8mb4)", hex(h.as_bytes()))
                ),
                r.partition
                    .map_or_else(|| "NULL".to_owned(), |p| p.to_string()),
            )
        })
        .collect();
    format!(
        "INSERT INTO {}.{} (out_id, kind, dest, k, v, headers, partition_no) VALUES {}",
        quote(db),
        quote(TABLE),
        values.join(", ")
    )
}

/// `inherit-xo/<tenant>/<schema>`: a schema holding a cross-tenant outbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XoIndex {
    /// The publisher.
    pub tenant: TenantId,
    /// The shard schema.
    pub schema: String,
}

impl Versioned for XoIndex {
    const V: u32 = 1;
}

fn made() -> &'static Mutex<HashSet<(TenantId, String)>> {
    static M: OnceLock<Mutex<HashSet<(TenantId, String)>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Makes `tenant`'s outbox table in `schema` (on `cluster`) and lists it,
/// once per process.
///
/// # Errors
/// The cluster's and the DC tier's.
pub async fn ensure_table(
    svc: &TenantServices,
    tenant: TenantId,
    schema: &str,
    cluster: &ClusterId,
) -> Result<()> {
    let key = (tenant, schema.to_owned());
    if made()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&key)
    {
        return Ok(());
    }
    let pool = svc.locator.ctl_pool(cluster).await?;
    let mut c = pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    c.query_drop(ddl(schema))
        .await
        .map_err(sprig_streams::maria::map_err)?;
    let store = svc.locator.dc_store();
    let k = store.tier().key(&format!("inherit-xo/{tenant}/{schema}"))?;
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![sprig_meta::Op::Put {
                key: k,
                value: Record::new(XoIndex {
                    tenant,
                    schema: schema.to_owned(),
                })
                .encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    made()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(key);
    Ok(())
}

/// Rows a session wrote in its open transaction.
struct Pending {
    svc: Weak<TenantServices>,
    tenant: TenantId,
    schema: String,
    ids: Vec<[u8; 16]>,
    at: Instant,
}

fn pending() -> &'static Mutex<HashMap<u64, Vec<Pending>>> {
    static M: OnceLock<Mutex<HashMap<u64, Vec<Pending>>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Notes rows session `session` wrote in its transaction.
pub fn note(
    session: u64,
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    schema: &str,
    ids: Vec<[u8; 16]>,
) {
    let mut m = pending().lock().unwrap_or_else(PoisonError::into_inner);
    // Sessions that went away inside a transaction: their rows are gone,
    // their entries too after 10 minutes.
    m.retain(|_, v| v.iter().any(|p| p.at.elapsed() < Duration::from_secs(600)));
    m.entry(session).or_default().push(Pending {
        svc: Arc::downgrade(svc),
        tenant,
        schema: schema.to_owned(),
        ids,
        at: Instant::now(),
    });
}

/// The commit hook: after a request of session `session`, when its
/// transaction has ended, forwards what it wrote (a rolled-back
/// transaction's rows aren't there, so nothing goes).
pub fn after_request(session: u64, in_txn: bool) {
    if in_txn {
        return;
    }
    let taken = {
        let mut m = pending().lock().unwrap_or_else(PoisonError::into_inner);
        if m.is_empty() {
            return;
        }
        m.remove(&session)
    };
    for p in taken.into_iter().flatten() {
        let Some(svc) = p.svc.upgrade() else { continue };
        tokio::spawn(async move {
            if let Err(e) = forward(&svc, p.tenant, &p.schema, Some(&p.ids), 0).await {
                tracing::debug!(tenant = %p.tenant, error = %e, "cross-tenant outbox: forward deferred to the sweeper");
            }
        });
    }
}

/// One row read for forwarding.
struct Row {
    id: Vec<u8>,
    rec: Publish,
    /// `created_at`, ms since the epoch.
    at_ms: u64,
}

/// Forwards rows of `tenant`'s outbox in `schema`: those of `only`, or
/// every row older than `older_ms`. The number forwarded.
///
/// # Errors
/// The clusters'; a destination that can't take a row leaves it for the
/// next pass.
pub async fn forward(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    schema: &str,
    only: Option<&[[u8; 16]]>,
    older_ms: u64,
) -> Result<usize> {
    forward_with(svc, tenant, schema, only, older_ms, false).await
}

/// [`forward`], also trying destinations that are backing off when
/// `force` (a `DROP DATABASE` draining its outbox).
#[allow(clippy::too_many_lines)]
async fn forward_with(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    schema: &str,
    only: Option<&[[u8; 16]]>,
    older_ms: u64,
    force: bool,
) -> Result<usize> {
    let cluster = svc.locator.cluster(tenant).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let mut c = pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    let mut tx = c
        .start_transaction(mysql_async::TxOpts::default())
        .await
        .map_err(sprig_streams::maria::map_err)?;
    let filter = match only {
        Some(ids) if !ids.is_empty() => format!(
            "out_id IN ({})",
            ids.iter().map(|i| hex(i)).collect::<Vec<_>>().join(", ")
        ),
        Some(_) => return Ok(0),
        None => format!(
            "created_at < UTC_TIMESTAMP(3) - INTERVAL {} MICROSECOND",
            older_ms * 1000
        ),
    };
    let rows: std::result::Result<Vec<mysql_async::Row>, _> = tx
        .query(format!(
            "SELECT out_id, CAST(dest AS CHAR), k, v, CAST(headers AS CHAR), partition_no, \
             TIMESTAMPDIFF(MICROSECOND, '1970-01-01', created_at) DIV 1000 \
             FROM {}.{} WHERE {filter} ORDER BY out_id LIMIT 500 FOR UPDATE SKIP LOCKED",
            quote(schema),
            quote(TABLE)
        ))
        .await;
    let rows = match rows {
        Ok(r) => r,
        // No table (yet, or dropped with its database): nothing to do.
        Err(mysql_async::Error::Server(s)) if matches!(s.code, 1146 | 1049) => return Ok(0),
        Err(e) => return Err(sprig_streams::maria::map_err(e)),
    };
    let mut by_dest: BTreeMap<String, (Dest, Vec<Row>)> = BTreeMap::new();
    for r in rows {
        let id: Vec<u8> = r.get(0).unwrap_or_default();
        let dest_s: String = r.get(1).unwrap_or_default();
        let Ok(dest) = serde_json::from_str::<Dest>(&dest_s) else {
            tracing::warn!(%tenant, dest = %dest_s, "cross-tenant outbox: unreadable destination; row kept");
            continue;
        };
        let rec = Publish {
            key: r.get::<Option<Vec<u8>>, _>(2).flatten(),
            value: r.get::<Option<Vec<u8>>, _>(3).flatten(),
            headers: r.get::<Option<String>, _>(4).flatten(),
            partition: r.get::<Option<u32>, _>(5).flatten(),
            ts_ms: None,
        };
        by_dest
            .entry(dest_s)
            .or_insert_with(|| (dest, Vec::new()))
            .1
            .push(Row {
                id,
                rec,
                at_ms: r.get::<Option<u64>, _>(6).flatten().unwrap_or(0),
            });
    }
    let mut done: Vec<Vec<u8>> = Vec::new();
    for (key, (dest, rows)) in by_dest {
        // A destination that keeps failing (a member's catalog busy or
        // unreachable) is retried with growing waits, so retries don't
        // add to its load.
        if !force && backing_off(&key) {
            continue;
        }
        match deliver(svc, &dest, &rows).await {
            Ok(()) => {
                failed(&key, false);
                done.extend(rows.into_iter().map(|r| r.id));
            }
            Err(e) => {
                let wait = failed(&key, true);
                tracing::warn!(%tenant, ?dest, error = %e, retry_in_s = wait.as_secs(), "cross-tenant outbox: destination not reached; retried later");
            }
        }
    }
    let n = done.len();
    if !done.is_empty() {
        tx.query_drop(format!(
            "DELETE FROM {}.{} WHERE out_id IN ({})",
            quote(schema),
            quote(TABLE),
            done.iter().map(|i| hex(i)).collect::<Vec<_>>().join(", ")
        ))
        .await
        .map_err(sprig_streams::maria::map_err)?;
    }
    tx.commit().await.map_err(sprig_streams::maria::map_err)?;
    if n > 0 {
        metrics::counter!("sprig_inherit_publishes_total", "kind" => "stream", "path" => "outbox")
            .increment(n as u64);
    }
    Ok(n)
}

type Backoff = HashMap<String, (u32, Instant)>;

fn backoff() -> &'static Mutex<Backoff> {
    static M: OnceLock<Mutex<Backoff>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Whether destination `key` failed recently and waits still.
fn backing_off(key: &str) -> bool {
    backoff()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(key)
        .is_some_and(|(_, next)| Instant::now() < *next)
}

/// Records a delivery to `key`: a failure doubles its wait (5 s up to
/// 2 min), a success clears it. The wait set.
fn failed(key: &str, yes: bool) -> Duration {
    let mut m = backoff().lock().unwrap_or_else(PoisonError::into_inner);
    if !yes {
        m.remove(key);
        return Duration::ZERO;
    }
    let n = m.get(key).map_or(0, |(n, _)| *n).saturating_add(1);
    let wait = Duration::from_secs(5u64.saturating_mul(1 << n.min(5)).min(120));
    m.insert(key.to_owned(), (n, Instant::now() + wait));
    wait
}

/// Writes `rows` into `dest`'s outbox (event id = out id).
#[allow(clippy::too_many_lines)]
async fn deliver(svc: &Arc<TenantServices>, dest: &Dest, rows: &[Row]) -> Result<()> {
    let (tenant, schema, stream) = match dest {
        Dest::Broadcast {
            def_tenant,
            def_object,
            publisher,
            form,
        } => {
            let a = TenantId(*def_tenant);
            let cat = catalog(svc, a).await?;
            let def = super::state_of(&cat)
                .def_by_id(*def_object)
                .cloned()
                .ok_or_else(|| {
                    super::dropping(format!("the definition {a}.{def_object} is gone"))
                })?;
            for r in rows {
                // The outbox row's time is the transaction's: membership at
                // commit is read from it.
                super::broadcast::append(
                    svc,
                    &def,
                    &r.id,
                    TenantId(*publisher),
                    r.at_ms,
                    *form,
                    &r.rec,
                )
                .await?;
            }
            return Ok(());
        }
        Dest::Exchange {
            tenant,
            db,
            exchange,
        } => {
            let t = TenantId(*tenant);
            let cat = catalog(svc, t).await?;
            let chain = super::chain(svc, &cat).await?;
            let rows: Vec<_> = rows
                .iter()
                .map(|r| {
                    (
                        r.id.clone(),
                        r.rec.key.clone(),
                        r.rec.value.clone(),
                        r.rec.headers.clone(),
                    )
                })
                .collect();
            super::mq::deliver_outbox(svc, &chain, db, exchange, &rows).await?;
            metrics::counter!("sprig_inherit_publishes_total", "kind" => "exchange", "path" => "outbox")
                .increment(rows.len() as u64);
            return Ok(());
        }
        Dest::Instance {
            tenant,
            def_tenant,
            def_object,
        } => {
            let t = TenantId(*tenant);
            let cat = catalog(svc, t).await?;
            let chain = super::chain(svc, &cat).await?;
            let key = DefKey {
                tenant: TenantId(*def_tenant),
                object: *def_object,
            };
            let d = chain
                .def(key)
                .ok_or_else(|| super::dropping(format!("the definition {key} is gone")))?;
            let si = super::streams::ensure(svc, &chain, &d, Cause::Publish).await?;
            (t, si.outbox_schema()?, si.stream)
        }
        Dest::Stream { tenant, stream } => {
            let t = TenantId(*tenant);
            let cat = catalog(svc, t).await?;
            let s = cat
                .ext
                .get::<StreamsState>(sprig_streams::MODULE)
                .and_then(|st| st.all().find(|s| s.id.0 == *stream).cloned())
                .ok_or_else(|| {
                    SprigError::new(
                        SprigCode::StreamNotFound,
                        format!("ER_SPRIG_STREAM_NOT_FOUND: tenant {t}'s stream {stream} is gone"),
                    )
                })?;
            let slot = s
                .outbox
                .first()
                .ok_or_else(|| SprigError::internal("a stream without an outbox"))?;
            (t, slot.schema(t, s.db).to_string(), s)
        }
    };
    let values: Vec<String> = rows
        .iter()
        .map(|r| {
            format!(
                "({}, {}, {}, {}, {})",
                hex(&r.id),
                lit(r.rec.key.as_deref()),
                lit(r.rec.value.as_deref()),
                r.rec.headers.as_deref().map_or_else(
                    || "NULL".to_owned(),
                    |h| format!("CONVERT({} USING utf8mb4)", hex(h.as_bytes()))
                ),
                r.rec.partition.map_or_else(
                    || "NULL".to_owned(),
                    |p| (p % stream.partitions.max(1)).to_string()
                ),
            )
        })
        .collect();
    let cluster = svc.locator.cluster(tenant).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let mut c = pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    c.query_drop(format!(
        "INSERT IGNORE INTO {}.{} (event_id, k, v, headers, partition_no) VALUES {}",
        quote(&schema),
        quote(&stream.outbox_table()),
        values.join(", ")
    ))
    .await
    .map_err(sprig_streams::maria::map_err)?;
    Ok(())
}

/// Rows waiting in `tenant`'s outbox in `schema` (0 without the table).
///
/// # Errors
/// The cluster's.
pub async fn pending_rows(svc: &TenantServices, tenant: TenantId, schema: &str) -> Result<u64> {
    let cluster = svc.locator.cluster(tenant).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let mut c = pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    let r: std::result::Result<Option<u64>, _> = c
        .query_first(format!(
            "SELECT COUNT(*) FROM {}.{}",
            quote(schema),
            quote(TABLE)
        ))
        .await;
    match r {
        Ok(n) => Ok(n.unwrap_or(0)),
        Err(mysql_async::Error::Server(s)) if matches!(s.code, 1146 | 1049) => Ok(0),
        Err(e) => Err(sprig_streams::maria::map_err(e)),
    }
}

/// Before `DROP DATABASE` of one of `tenant`'s databases (lead's choice on
/// r9fet item 23): forwards the committed rows of the outboxes in its
/// shard schemas `schemas`, retrying every second (destinations backing
/// off included) for up to `wait`. Rows of transactions still open aren't
/// seen (they aren't committed; the drop waits for their locks anyway).
///
/// # Errors
/// 39245 naming the rows still there after `wait`; the clusters'.
pub async fn drain_for_drop(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    db: &str,
    schemas: &[String],
    wait: Duration,
) -> Result<()> {
    let deadline = Instant::now() + wait;
    loop {
        let mut left = 0u64;
        for s in schemas {
            if pending_rows(svc, tenant, s).await? == 0 {
                continue;
            }
            if let Err(e) = forward_with(svc, tenant, s, None, 0, true).await {
                tracing::debug!(%tenant, schema = %s, error = %e, "DROP DATABASE: outbox forward failed; retried");
            }
            left += pending_rows(svc, tenant, s).await?;
        }
        if left == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(SprigError::new(
                SprigCode::InheritOutboxPending,
                format!(
                    "ER_SPRIG_INHERIT_OUTBOX_PENDING: database '{db}' holds {left} committed \
                     cross-tenant message{} not forwarded yet after {} s \
                     (inherit.outbox_drain_seconds); DROP DATABASE refused so they aren't lost, \
                     try again later",
                    if left == 1 { "" } else { "s" },
                    wait.as_secs()
                ),
            ));
        }
        tokio::time::sleep(Duration::from_secs(1).min(deadline - Instant::now())).await;
    }
}

/// 39895 naming the `FOR EACH TENANT` definitions still active in database
/// `db` of `st`'s tenant (at most 10 named, then a count).
///
/// # Errors
/// 39895 when there is any.
pub fn refuse_live_definitions(st: &sprig_inherit::msg::MsgState, db: &str) -> Result<()> {
    let live = st.live_defs_in_db(db);
    if live.is_empty() {
        return Ok(());
    }
    let mut names: Vec<String> = live
        .iter()
        .take(10)
        .map(|d| format!("{} {}.{}", d.kind.as_str(), d.db_name, d.name))
        .collect();
    if live.len() > 10 {
        names.push(format!("and {} more", live.len() - 10));
    }
    Err(SprigError::new(
        SprigCode::InheritedObjectInUse,
        format!(
            "ER_SPRIG_INHERITED_OBJECT_IN_USE: database '{db}' holds {} FOR EACH TENANT \
             definition{} ({}); drop {} first (every member's instance goes with {})",
            live.len(),
            if live.len() == 1 { "" } else { "s" },
            names.join(", "),
            if live.len() == 1 { "it" } else { "them" },
            if live.len() == 1 { "it" } else { "them" },
        ),
    ))
}

/// The `DROP DATABASE` hook: [`drain_for_drop`] over database `db`'s shard
/// schemas, waiting `inherit.outbox_drain_seconds`; then refused (39895)
/// while `FOR EACH TENANT` definitions are active in `db`. Nothing to do
/// for a database the tenant doesn't have.
///
/// # Errors
/// 39895; 39245; the catalog's and the clusters'.
pub async fn before_drop_database(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    db: &str,
    lctn: bool,
) -> Result<()> {
    let cat = catalog(svc, tenant).await?;
    let Some(row) = cat.database(&sprig_catalog::ddl::database_key(db, lctn)) else {
        return Ok(());
    };
    let schemas: Vec<String> = sprig_catalog::ddl::shard_schemas(tenant, row)
        .iter()
        .map(ToString::to_string)
        .collect();
    // Only schemas that ever held an outbox (listed when it was made).
    let listed: HashSet<String> = listed(svc)
        .await?
        .into_iter()
        .filter(|e| e.tenant == tenant)
        .map(|e| e.schema)
        .collect();
    let schemas: Vec<String> = schemas.into_iter().filter(|s| listed.contains(s)).collect();
    if !schemas.is_empty() {
        let wait = svc.settings.get(
            &sprig_inherit::msg::settings::OUTBOX_DRAIN_SECONDS,
            &cat.setting_ctx(&svc.dc).with_db(row.db_id),
            Some(&cat.settings),
        );
        drain_for_drop(svc, tenant, &row.name, &schemas, Duration::from_secs(wait)).await?;
    }
    // The lead's choice on r9fet3's question 1: a declarer's database
    // isn't dropped from under its live definitions (every member's
    // instances would go with them, too easily by accident). After the
    // outbox: its rows may be bound for those definitions' instances,
    // which take no more publishes once the definitions are dropped.
    refuse_live_definitions(&super::state_of(&cat), &row.name)
}

/// Every outbox schema this DC lists.
async fn listed(svc: &TenantServices) -> Result<Vec<XoIndex>> {
    let store = svc.locator.dc_store();
    let prefix = store.tier().key("inherit-xo/")?;
    let page = store
        .list(
            &prefix,
            ListOpts {
                limit: 100_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| Record::<XoIndex>::decode(&kv.value).ok().map(|r| r.body))
        .collect())
}

/// Starts this gateway's sweeper: every 5 s, rows older than 5 s in every
/// listed outbox (see the module docs). It ends with the services.
pub fn start_sweeper(svc: &Arc<TenantServices>) {
    let weak = Arc::downgrade(svc);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let Some(svc) = weak.upgrade() else { return };
            let Ok(list) = listed(&svc).await else {
                continue;
            };
            for e in list {
                if let Err(err) = forward(&svc, e.tenant, &e.schema, None, 5000).await {
                    tracing::debug!(tenant = %e.tenant, schema = %e.schema, error = %err, "cross-tenant outbox sweep");
                }
            }
        }
    });
}

/// A destination as stored (`dest`).
#[must_use]
pub fn dest_json(d: &Dest) -> Value {
    serde_json::to_value(d).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destinations_and_inserts() {
        let d = Dest::Instance {
            tenant: 9,
            def_tenant: 2,
            def_object: 41,
        };
        assert_eq!(
            dest_json(&d),
            serde_json::json!({"to": "instance", "tenant": 9, "def_tenant": 2, "def_object": 41})
        );
        let back: Dest = serde_json::from_value(dest_json(&d)).unwrap();
        assert_eq!(back, d);
        let sql = insert_sql(
            "security",
            &[(
                [1; 16],
                d,
                Publish {
                    key: Some(b"k".to_vec()),
                    value: None,
                    headers: Some("{}".into()),
                    partition: Some(2),
                    ts_ms: None,
                },
            )],
        );
        assert!(
            sql.starts_with("INSERT INTO `security`.`__sprig_xo` (out_id"),
            "{sql}"
        );
        assert!(
            sql.contains("X'6b', NULL, CONVERT(X'7b7d' USING utf8mb4), 2)"),
            "{sql}"
        );
        assert!(ddl("t1_d2_g1_s1").contains("PRIMARY KEY (out_id)"));
    }
}
