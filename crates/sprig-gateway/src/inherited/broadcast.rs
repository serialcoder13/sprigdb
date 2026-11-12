//! Broadcast: one publish for every member (chapter 42, "Broadcast: one
//! publish for every member"):
//!
//! ```text
//! publish_broadcast(P, definition D, record r, form):
//!  1. limits: |r| ≤ inherit.broadcast_max_bytes, D's broadcasts in the last
//!     hour < inherit.broadcasts_per_hour, members ≤ inherit.broadcast_max_members
//!     (39244)
//!  2. inside a transaction: a row of P's __sprig_xo (Dest::Broadcast), in it;
//!     outside one: straight to step 3
//!  3. D's broadcast log __sprig_ib_<D> in the declarer's database D.db (its
//!     first shard schema): one row, broadcast id = r's id (UUIDv7), commit
//!     time, form, state 'fanning_out', next_tenant_id 0; listed under
//!     inherit-bcast/<def> in the DC tier
//! fan_out(D):    every gateway, every 2 s, per listed definition:
//!   the oldest 'fanning_out' broadcast, locked FOR UPDATE NOWAIT (another
//!   gateway has it: skip D this round, so broadcasts go in order)
//!   its members (the directory, in tenant-id order, from next_tenant_id):
//!     members of D below P (P too when it is one), inside UNDER t, created
//!     no later than the commit; WITH INSTANCE: only those with an instance
//!   each: the instance made if needed (FOR ALL TENANTS), then the record
//!     delivered with the broadcast id as its event (stream) or out (queue)
//!     id: a second delivery is a no-op; x-sprig-tenant = T,
//!     x-sprig-origin-tenant = P, x-sprig-broadcast = the id
//!   every 500 members: next_tenant_id and delivered saved (resumable);
//!   paced at inherit.broadcast_rate deliveries a second
//!   a member whose delivery fails, or that still waits for an earlier
//!   broadcast of D, gets a row in D's retry list __sprig_ibr_<D> (same
//!   schema, saved with the progress); then 'delivered'; rows past
//!   inherit.broadcast_retention_hours go once no retry names them
//! retry(D):  after each fan-out pass, one gateway at a time (GET_LOCK):
//!   per member, its rows in broadcast order, from the first one due; each
//!   delivered (the id makes a repeat a no-op) and removed, or, on failure,
//!   its next try put off (1 s doubling to 5 min) and the member's later
//!   rows left waiting behind it: exactly once and in order per member; a
//!   member deleted or out of the directory has its rows removed
//! replay:  with inherit.broadcast_replay = on, a member's new instance gets
//!   the broadcasts still in the log
//! ```

#![allow(
    clippy::many_single_char_names,
    clippy::items_after_statements,
    clippy::too_many_lines,
    clippy::cast_precision_loss
)]

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sprig_catalog::ddl::{database_key, shard_schemas};
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::{Cause, DefKey, DefKind, Definition, InstanceState};
use sprig_meta::{ListOpts, Record, Versioned};
use sprig_streams::publish::Publish;

use super::{DirEntry, catalog};
use crate::tenant::TenantServices;

/// The header carrying the broadcast id.
pub const BROADCAST_HEADER: &str = "x-sprig-broadcast";

/// Members saved per progress step.
const STEP: usize = 500;

/// A broadcast's form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Form {
    /// `WITH INSTANCE`: only members whose instance exists.
    #[serde(default)]
    pub with_instance: bool,
    /// `UNDER t`: only that subtree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub under: Option<u64>,
}

impl Form {
    fn name(self) -> &'static str {
        if self.with_instance {
            "with_instance"
        } else {
            "all"
        }
    }
}

/// The log's table of definition `def` (in the declarer's database).
#[must_use]
pub fn table(def: DefKey) -> String {
    format!("__sprig_ib_{}", def.object)
}

/// The log's DDL.
#[must_use]
pub fn ddl(schema: &str, def: DefKey) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
  bid            BINARY(16) NOT NULL,
  publisher      BIGINT UNSIGNED NOT NULL,
  committed_ms   BIGINT UNSIGNED NOT NULL,
  form           ENUM('all','with_instance') NOT NULL,
  under_tenant   BIGINT UNSIGNED NULL,
  state          ENUM('fanning_out','delivered','failed') NOT NULL,
  next_tenant_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
  delivered      INT UNSIGNED NOT NULL DEFAULT 0,
  skipped        INT UNSIGNED NOT NULL DEFAULT 0,
  k              VARBINARY(4096) NULL,
  v              LONGBLOB NULL,
  headers        JSON NULL,
  error          VARCHAR(1024) NULL,
  done_ms        BIGINT UNSIGNED NULL,
  PRIMARY KEY (bid)
) ENGINE=InnoDB",
        quote(schema),
        quote(&table(def))
    )
}

/// The retry list's table of definition `def` (next to the log).
#[must_use]
pub fn retry_table(def: DefKey) -> String {
    format!("__sprig_ibr_{}", def.object)
}

/// The retry list's DDL: one row per (member, broadcast) not delivered yet.
#[must_use]
pub fn retry_ddl(schema: &str, def: DefKey) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (
  tenant_id  BIGINT UNSIGNED NOT NULL,
  bid        BINARY(16) NOT NULL,
  attempts   INT UNSIGNED NOT NULL DEFAULT 0,
  next_ms    BIGINT UNSIGNED NOT NULL,
  error      VARCHAR(1024) NULL,
  PRIMARY KEY (tenant_id, bid),
  KEY by_bid (bid)
) ENGINE=InnoDB",
        quote(schema),
        quote(&retry_table(def))
    )
}

/// First retry delay of a failed delivery; it doubles per attempt.
const RETRY_FIRST_MS: u64 = 1_000;
/// Longest retry delay.
const RETRY_MAX_MS: u64 = 300_000;

/// The delay before attempt `attempts + 1` of a delivery that failed
/// `attempts` times (1 s, 2 s, 4 s, ... 5 min).
#[must_use]
pub fn retry_delay_ms(attempts: u32) -> u64 {
    let shift = attempts.saturating_sub(1).min(20);
    RETRY_FIRST_MS.saturating_mul(1 << shift).min(RETRY_MAX_MS)
}

/// `inherit-bcast/<def>`: a definition with a broadcast log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogIndex {
    /// The definition.
    pub def: DefKey,
    /// The log's schema.
    pub schema: String,
}

impl Versioned for LogIndex {
    const V: u32 = 1;
}

fn limit_err(m: impl std::fmt::Display) -> SprigError {
    let code = SprigCode::InheritBroadcastLimit;
    SprigError::new(code, format!("{}: {m}", code.name()))
}

/// Where `def`'s log lives: the declarer's cluster and its database's first
/// shard schema.
async fn place(svc: &Arc<TenantServices>, def: &Definition) -> Result<(ClusterId, String)> {
    let a = def.declarer;
    let cat = catalog(svc, a).await?;
    let lc = super::streams::lctn(svc, &cat);
    let row = cat
        .database(&database_key(&def.db_name, lc))
        .ok_or_else(|| super::unavailable(format!("tenant {a}'s database {}", def.db_name)))?;
    let schema = shard_schemas(a, row)
        .first()
        .map(ToString::to_string)
        .ok_or_else(|| SprigError::internal("a database without a shard"))?;
    Ok((svc.locator.cluster(a).await?, schema))
}

fn made() -> &'static Mutex<HashSet<DefKey>> {
    static M: OnceLock<Mutex<HashSet<DefKey>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashSet::new()))
}

fn retry_made() -> &'static Mutex<HashSet<DefKey>> {
    static M: OnceLock<Mutex<HashSet<DefKey>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashSet::new()))
}

/// `def`'s retry list, made once per process (logs made before the list
/// existed get it at their next fan-out).
async fn ensure_retry(pool: &mysql_async::Pool, schema: &str, def: DefKey) -> Result<()> {
    if retry_made()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&def)
    {
        return Ok(());
    }
    let mut c = pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    match c.query_drop(retry_ddl(schema, def)).await {
        Ok(()) => {}
        // The database is gone: nothing to fan out.
        Err(mysql_async::Error::Server(e)) if e.code == 1049 => return Ok(()),
        Err(e) => return Err(sprig_streams::maria::map_err(e)),
    }
    retry_made()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(def);
    Ok(())
}

/// `def`'s log, made and listed once per process.
async fn ensure_log(svc: &Arc<TenantServices>, def: &Definition) -> Result<(ClusterId, String)> {
    let (cluster, schema) = place(svc, def).await?;
    if made()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&def.key())
    {
        return Ok((cluster, schema));
    }
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let mut c = pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    c.query_drop(ddl(&schema, def.key()))
        .await
        .map_err(sprig_streams::maria::map_err)?;
    c.query_drop(retry_ddl(&schema, def.key()))
        .await
        .map_err(sprig_streams::maria::map_err)?;
    let store = svc.locator.dc_store();
    let k = store.tier().key(&format!("inherit-bcast/{}", def.key()))?;
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![sprig_meta::Op::Put {
                key: k,
                value: Record::new(LogIndex {
                    def: def.key(),
                    schema: schema.clone(),
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
        .insert(def.key());
    Ok((cluster, schema))
}

/// Milliseconds since the epoch of an RFC 3339 UTC time
/// (`2026-11-08T12:13:58Z`, fractions allowed); `None` when it doesn't
/// parse.
#[must_use]
pub fn rfc3339_ms(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' {
        return None;
    }
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, mo, d) = (n(0..4)?, n(5..7)?, n(8..10)?);
    let (h, mi, sec) = (n(11..13)?, n(14..16)?, n(17..19)?);
    let mut ms = 0i64;
    if b.get(19) == Some(&b'.') {
        let frac: String = s[20..].chars().take_while(char::is_ascii_digit).collect();
        let f3: String = frac.chars().chain("000".chars()).take(3).collect();
        ms = f3.parse().ok()?;
    }
    // Days from the civil date (Howard Hinnant's algorithm).
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    u64::try_from(((days * 24 + h) * 60 + mi) * 60_000 + sec * 1000 + ms).ok()
}

/// Every tenant of the directory (active ones), by id.
async fn directory(svc: &TenantServices) -> Result<BTreeMap<u64, DirEntry>> {
    let prefix = svc.global.tier().key("tenants/")?;
    let page = svc
        .global
        .list(
            &prefix,
            ListOpts {
                limit: 10_000_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| Record::<DirEntry>::decode(&kv.value).ok().map(|r| r.body))
        .filter(|e| e.state == "active" && e.tenant_id != 0)
        .map(|e| (e.tenant_id, e))
        .collect())
}

/// The members a broadcast of `def` by `publisher` at `committed_ms` with
/// `form` reaches (before the `WITH INSTANCE` filter), in id order.
///
/// # Errors
/// The directory's.
pub async fn members(
    svc: &TenantServices,
    def: &Definition,
    publisher: TenantId,
    form: Form,
    committed_ms: u64,
) -> Result<Vec<TenantId>> {
    let dir = directory(svc).await?;
    let mut out = Vec::new();
    for (id, e) in &dir {
        let path = e.path_ids();
        let Some((_, anc)) = path.split_last() else {
            continue;
        };
        let anc: Vec<TenantId> = anc.iter().map(|x| TenantId(*x)).collect();
        let t = TenantId(*id);
        if !def.scope.reaches(def.declarer, t, &anc) {
            continue;
        }
        if t != publisher && !anc.contains(&publisher) {
            continue;
        }
        if let Some(u) = form.under
            && !path.contains(&u)
        {
            continue;
        }
        // Second precision in the directory: a tenant created in the
        // commit's second counts.
        if rfc3339_ms(&e.created_at).is_some_and(|c| c / 1000 > committed_ms / 1000) {
            continue;
        }
        out.push(t);
    }
    Ok(out)
}

/// Step 1's limits for a broadcast of `bytes` bytes of `def` by
/// `publisher`.
///
/// # Errors
/// 39244; the directory's and the cluster's.
pub async fn check_limits(
    svc: &Arc<TenantServices>,
    def: &Definition,
    publisher: TenantId,
    bytes: usize,
    form: Form,
) -> Result<()> {
    let dcat = catalog(svc, def.declarer).await?;
    let mut ctx = dcat.object_ctx(&svc.dc, sprig_common::ids::ObjectId(def.object_id));
    if let Some(d) = dcat
        .databases
        .iter()
        .find(|d| d.name.eq_ignore_ascii_case(&def.db_name))
    {
        ctx = ctx.with_db(d.db_id);
    }
    let get =
        |k: &'static sprig_settings::Setting<u64>| svc.settings.get(k, &ctx, Some(&dcat.settings));
    use sprig_inherit::msg::settings as s;
    let max_bytes = get(&s::BROADCAST_MAX_BYTES);
    if bytes as u64 > max_bytes {
        return Err(limit_err(format!(
            "a broadcast of {bytes} bytes is over inherit.broadcast_max_bytes ({max_bytes})"
        )));
    }
    let per_hour = get(&s::BROADCASTS_PER_HOUR);
    let (cluster, schema) = ensure_log(svc, def).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let mut c = pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    let since = sprig_inherit::msg::store::now_ms().saturating_sub(3_600_000);
    let n: Option<u64> = c
        .exec_first(
            format!(
                "SELECT COUNT(*) FROM {}.{} WHERE committed_ms >= ?",
                quote(&schema),
                quote(&table(def.key()))
            ),
            (since,),
        )
        .await
        .map_err(sprig_streams::maria::map_err)?;
    if n.unwrap_or(0) >= per_hour {
        return Err(limit_err(format!(
            "{}.{} took {} broadcasts in the last hour (inherit.broadcasts_per_hour = {per_hour})",
            def.db_name,
            def.name,
            n.unwrap_or(0)
        )));
    }
    let max_members = get(&s::BROADCAST_MAX_MEMBERS);
    let m = members(
        svc,
        def,
        publisher,
        form,
        sprig_inherit::msg::store::now_ms(),
    )
    .await?
    .len() as u64;
    if m > max_members {
        return Err(limit_err(format!(
            "a broadcast would reach {m} members (inherit.broadcast_max_members = {max_members}); \
             publish per subtree (UNDER t) or per tenant"
        )));
    }
    Ok(())
}

/// Step 3: the log's row (a second append of the same id adds nothing).
///
/// # Errors
/// The cluster's.
pub async fn append(
    svc: &Arc<TenantServices>,
    def: &Definition,
    bid: &[u8],
    publisher: TenantId,
    committed_ms: u64,
    form: Form,
    rec: &Publish,
) -> Result<()> {
    let (cluster, schema) = ensure_log(svc, def).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let mut c = pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    c.exec_drop(
        format!(
            "INSERT IGNORE INTO {}.{} (bid, publisher, committed_ms, form, under_tenant, state, \
             k, v, headers) VALUES (?, ?, ?, ?, ?, 'fanning_out', ?, ?, ?)",
            quote(&schema),
            quote(&table(def.key()))
        ),
        (
            bid,
            publisher.0,
            committed_ms,
            form.name(),
            form.under,
            rec.key.clone(),
            rec.value.clone(),
            rec.headers.clone(),
        ),
    )
    .await
    .map_err(sprig_streams::maria::map_err)?;
    metrics::counter!("sprig_inherit_broadcasts_total", "form" => form.name()).increment(1);
    wake();
    Ok(())
}

fn waker() -> &'static tokio::sync::Notify {
    static N: OnceLock<tokio::sync::Notify> = OnceLock::new();
    N.get_or_init(tokio::sync::Notify::new)
}

/// Wakes this gateway's fan-out (a broadcast was just logged).
pub fn wake() {
    waker().notify_one();
}

/// The broadcast id as text (headers, SHOW).
#[must_use]
pub fn id_text(bid: &[u8]) -> String {
    uuid::Uuid::from_slice(bid).map_or_else(|_| hex(bid), |u| u.to_string())
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter().fold(String::new(), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

/// One logged broadcast.
#[derive(Debug, Clone)]
struct Logged {
    bid: Vec<u8>,
    publisher: TenantId,
    committed_ms: u64,
    form: Form,
    next: u64,
    delivered: u64,
    skipped: u64,
    rec: Publish,
}

fn logged(r: &mysql_async::Row) -> Logged {
    let form: String = r.get(3).unwrap_or_default();
    Logged {
        bid: r.get(0).unwrap_or_default(),
        publisher: TenantId(r.get(1).unwrap_or_default()),
        committed_ms: r.get(2).unwrap_or_default(),
        form: Form {
            with_instance: form == "with_instance",
            under: r.get::<Option<u64>, _>(4).flatten(),
        },
        next: r.get(5).unwrap_or_default(),
        delivered: r.get(6).unwrap_or_default(),
        skipped: r.get(7).unwrap_or_default(),
        rec: Publish {
            key: r.get::<Option<Vec<u8>>, _>(8).flatten(),
            value: r.get::<Option<Vec<u8>>, _>(9).flatten(),
            headers: r.get::<Option<String>, _>(10).flatten(),
            partition: None,
            ts_ms: None,
        },
    }
}

const COLS: &str = "bid, publisher, committed_ms, form, under_tenant, next_tenant_id, delivered, skipped, k, v, \
     CAST(headers AS CHAR)";

/// Delivers broadcast `b` of `def` into member `t`'s instance. `false`:
/// skipped (`WITH INSTANCE` and no instance).
///
/// # Errors
/// The member's (its catalog, cluster or quota); the fan-out goes on with
/// the next member.
async fn deliver(
    svc: &Arc<TenantServices>,
    def: &Definition,
    t: TenantId,
    b: &Logged,
    with_instance: bool,
) -> Result<bool> {
    let tcat = catalog(svc, t).await?;
    let tchain = super::chain(svc, &tcat).await?;
    let has = super::state_of(&tcat)
        .instances
        .get(&def.key())
        .is_some_and(|i| i.state == InstanceState::Active);
    if with_instance && !has {
        return Ok(false);
    }
    let tname = super::tenant_name(svc, t).await?;
    let origin = super::tenant_name(svc, b.publisher)
        .await
        .unwrap_or_else(|_| b.publisher.to_string());
    let bid_text = id_text(&b.bid);
    match def.kind {
        DefKind::Stream => {
            let d = tchain
                .def(def.key())
                .ok_or_else(|| super::target_err(format!("tenant {t} left the scope")))?;
            let si = Box::pin(super::streams::ensure(svc, &tchain, &d, Cause::Publish)).await?;
            let mut h: Map<String, Value> = b
                .rec
                .headers
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();
            h.insert(super::mq::TENANT_HEADER.into(), json!(tname));
            h.insert(super::mq::ORIGIN_HEADER.into(), json!(origin));
            h.insert(BROADCAST_HEADER.into(), json!(bid_text));
            let headers = Value::Object(h).to_string();
            let schema = si.outbox_schema()?;
            let lit = |v: Option<&[u8]>| {
                v.map_or_else(|| "NULL".to_owned(), sprig_queues::sql::hex_literal)
            };
            let sql = format!(
                "INSERT IGNORE INTO {}.{} (event_id, k, v, headers) VALUES ({}, {}, {}, CONVERT({} \
                 USING utf8mb4))",
                quote(&schema),
                quote(&si.stream.outbox_table()),
                sprig_queues::sql::hex_literal(&b.bid),
                lit(b.rec.key.as_deref()),
                lit(b.rec.value.as_deref()),
                sprig_queues::sql::hex_literal(headers.as_bytes()),
            );
            let cluster = svc.locator.cluster(t).await?;
            let pool = svc.locator.ctl_pool(&cluster).await?;
            let mut c = pool
                .get_conn()
                .await
                .map_err(sprig_streams::maria::map_err)?;
            c.query_drop(sql)
                .await
                .map_err(sprig_streams::maria::map_err)?;
        }
        DefKind::Exchange | DefKind::Queue => {
            let h: Value = b
                .rec
                .headers
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or(Value::Null);
            let fmt = u8::try_from(h.get("fmt").and_then(Value::as_u64).unwrap_or(0)).unwrap_or(0);
            let props = h
                .get("props")
                .and_then(Value::as_str)
                .and_then(|p| {
                    use base64::Engine as _;
                    base64::engine::general_purpose::STANDARD.decode(p).ok()
                })
                .unwrap_or_default();
            let exchange = if def.kind == DefKind::Exchange {
                def.name.clone()
            } else {
                String::new()
            };
            let key =
                String::from_utf8_lossy(b.rec.key.as_deref().unwrap_or_default()).into_owned();
            let m = sprig_queues::msg::rebuild(
                fmt,
                exchange.clone(),
                key.clone(),
                props.into(),
                b.rec.value.clone().unwrap_or_default().into(),
            )
            .map_err(|e| SprigError::internal(e.to_string()))?;
            let mut m = super::mq::stamp(&m, &key, &tname, &origin)
                .map_err(|e| SprigError::internal(e.to_string()))?;
            m = with_header(&m, BROADCAST_HEADER, &bid_text)?;
            let rows = vec![(
                b.bid.clone(),
                Some(key.into_bytes()),
                Some(m.body.to_vec()),
                Some(super::mq::xo_headers(&m)),
            )];
            Box::pin(super::mq::deliver_outbox(
                svc,
                &tchain,
                &def.db_name,
                &exchange,
                &rows,
            ))
            .await?;
        }
        _ => {
            return Err(SprigError::internal(
                "a broadcast into a non-messaging kind",
            ));
        }
    }
    metrics::counter!("sprig_inherit_broadcast_deliveries_total", "result" => "delivered")
        .increment(1);
    Ok(true)
}

/// `m` with one more 0-9-1 header (or 1.0 application property).
fn with_header(
    m: &sprig_proto_amqp::message::Message,
    k: &str,
    v: &str,
) -> Result<sprig_proto_amqp::message::Message> {
    use sprig_proto_amqp::message::MsgFormat;
    use sprig_proto_amqp::v091::codec::FieldValue;
    use sprig_proto_amqp::v091::props::BasicProperties;
    let mut out = m.clone();
    match m.format {
        MsgFormat::Amqp091 => {
            let mut p = BasicProperties::decode(&m.props)
                .map_err(|e| SprigError::internal(e.to_string()))?;
            let mut h = p.headers.take().unwrap_or_default();
            h.insert(k.to_owned(), FieldValue::str(v));
            p.headers = Some(h);
            out.props = p.to_bytes();
        }
        MsgFormat::Amqp10 => {
            use sprig_proto_amqp::v10::message::{
                Sections, application_properties_section, encode_sections,
            };
            use sprig_proto_amqp::v10::types::Value as V10;
            let s = Sections::parse(&m.body).map_err(|e| SprigError::internal(e.0))?;
            let mut app = s.application_properties.clone();
            app.retain(|(x, _)| x != k);
            app.push((k.to_owned(), V10::String(v.to_owned())));
            let mut b =
                encode_sections(&[s.properties.to_value(), application_properties_section(app)])
                    .to_vec();
            b.extend_from_slice(&s.body_raw);
            out.body = b.into();
        }
    }
    Ok(out)
}

/// The fan-out of definition `def`'s log (see the module docs): the
/// broadcasts finished in this call.
///
/// # Errors
/// The clusters'; a member that fails is counted as skipped.
#[allow(clippy::too_many_lines)]
pub async fn fan_out(svc: &Arc<TenantServices>, def_key: DefKey, schema: &str) -> Result<usize> {
    let acat = match catalog(svc, def_key.tenant).await {
        Ok(c) => c,
        Err(e) => {
            // A deleted declarer (bugfix-46): its catalog and its
            // definitions are gone, and so are its members (39362).
            if super::dir_entry(svc, def_key.tenant).await?.is_none() {
                tracing::info!(def = %def_key, "broadcast log unlisted: the declarer is deleted");
                unlist(svc, def_key).await?;
                return Ok(0);
            }
            return Err(e);
        }
    };
    let Some(def) = super::state_of(&acat).def_by_id(def_key.object).cloned() else {
        // The definition is gone (its drop finished): unlisted once it
        // has been missing for a while, so a snapshot a moment behind a
        // new definition never unlists its log (bugfix-46).
        if missing_long(def_key) {
            tracing::info!(def = %def_key, "broadcast log unlisted: the definition is gone");
            unlist(svc, def_key).await?;
        }
        return Ok(0);
    };
    missing()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&def_key);
    let cluster = svc.locator.cluster(def_key.tenant).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let rate = svc
        .settings
        .get(
            &sprig_inherit::msg::settings::BROADCAST_RATE,
            &acat.setting_ctx(&svc.dc),
            Some(&acat.settings),
        )
        .max(1);
    let max_members = svc.settings.get(
        &sprig_inherit::msg::settings::BROADCAST_MAX_MEMBERS,
        &acat.setting_ctx(&svc.dc),
        Some(&acat.settings),
    );
    let tbl = format!("{}.{}", quote(schema), quote(&table(def_key)));
    let rtbl = format!("{}.{}", quote(schema), quote(&retry_table(def_key)));
    ensure_retry(&pool, schema, def_key).await?;
    let mut finished = 0;
    loop {
        let mut c = pool
            .get_conn()
            .await
            .map_err(sprig_streams::maria::map_err)?;
        let oldest: Option<Vec<u8>> = match c
            .query_first(format!(
                "SELECT bid FROM {tbl} WHERE state = 'fanning_out' ORDER BY bid LIMIT 1"
            ))
            .await
        {
            Ok(v) => v,
            Err(mysql_async::Error::Server(s)) if matches!(s.code, 1146 | 1049) => return Ok(0),
            Err(e) => return Err(sprig_streams::maria::map_err(e)),
        };
        let Some(oldest) = oldest else {
            break;
        };
        let mut tx = c
            .start_transaction(mysql_async::TxOpts::default())
            .await
            .map_err(sprig_streams::maria::map_err)?;
        let row: Option<mysql_async::Row> = match tx
            .exec_first(
                format!("SELECT {COLS} FROM {tbl} WHERE bid = ? FOR UPDATE NOWAIT"),
                (oldest.clone(),),
            )
            .await
        {
            Ok(r) => r,
            // Another gateway is fanning it out: its turn.
            Err(mysql_async::Error::Server(s)) if matches!(s.code, 1205 | 3572 | 1213) => {
                return Ok(finished);
            }
            Err(e) => return Err(sprig_streams::maria::map_err(e)),
        };
        let Some(row) = row else {
            break;
        };
        let b = logged(&row);
        let all = members(svc, &def, b.publisher, b.form, b.committed_ms).await?;
        if all.len() as u64 > max_members {
            tx.exec_drop(
                format!("UPDATE {tbl} SET state = 'failed', error = ?, done_ms = ? WHERE bid = ?"),
                (
                    format!(
                        "39244: {} members, over inherit.broadcast_max_members ({max_members})",
                        all.len()
                    ),
                    sprig_inherit::msg::store::now_ms(),
                    b.bid.clone(),
                ),
            )
            .await
            .map_err(sprig_streams::maria::map_err)?;
            tx.commit().await.map_err(sprig_streams::maria::map_err)?;
            continue;
        }
        let todo: Vec<TenantId> = all.into_iter().filter(|t| t.0 > b.next).collect();
        let batch: Vec<TenantId> = todo.iter().take(STEP).copied().collect();
        let started = Instant::now();
        let (mut delivered, mut skipped) = (0u64, 0u64);
        // Members still waiting for an earlier broadcast of this
        // definition: this one waits behind it (order per member).
        let waiting: HashSet<u64> = tx
            .query::<u64, _>(format!("SELECT DISTINCT tenant_id FROM {rtbl}"))
            .await
            .map_err(sprig_streams::maria::map_err)?
            .into_iter()
            .collect();
        let now = sprig_inherit::msg::store::now_ms();
        let mut later: Vec<(u64, u32, u64, String)> = Vec::new();
        for t in &batch {
            if waiting.contains(&t.0) {
                later.push((t.0, 0, now, "waits for an earlier broadcast".to_owned()));
                continue;
            }
            match Box::pin(deliver(svc, &def, *t, &b, b.form.with_instance)).await {
                Ok(true) => delivered += 1,
                Ok(false) => skipped += 1,
                Err(e) => {
                    metrics::counter!("sprig_inherit_broadcast_deliveries_total", "result" => "failed")
                        .increment(1);
                    tracing::warn!(def = %def_key, tenant = %t, broadcast = %id_text(&b.bid), error = %e, "broadcast not delivered to a member; retried later");
                    later.push((t.0, 1, now + retry_delay_ms(1), clip(&e.to_string())));
                }
            }
        }
        for chunk in later.chunks(200) {
            let values = vec!["(?, ?, ?, ?, ?)"; chunk.len()].join(", ");
            let mut params: Vec<mysql_async::Value> = Vec::with_capacity(chunk.len() * 5);
            for (t, a, next, err) in chunk {
                params.push((*t).into());
                params.push(b.bid.clone().into());
                params.push((*a).into());
                params.push((*next).into());
                params.push(err.clone().into());
            }
            tx.exec_drop(
                format!(
                    "INSERT IGNORE INTO {rtbl} (tenant_id, bid, attempts, next_ms, error) VALUES \
                     {values}"
                ),
                params,
            )
            .await
            .map_err(sprig_streams::maria::map_err)?;
        }
        // Pace: inherit.broadcast_rate deliveries a second.
        let want = Duration::from_millis(batch.len() as u64 * 1000 / rate);
        if let Some(rest) = want.checked_sub(started.elapsed()) {
            tokio::time::sleep(rest).await;
        }
        let done = batch.len() == todo.len();
        let next = batch.last().map_or(b.next, |t| t.0);
        tx.exec_drop(
            format!(
                "UPDATE {tbl} SET next_tenant_id = ?, delivered = ?, skipped = ?, state = ?, \
                 done_ms = ? WHERE bid = ?"
            ),
            (
                next,
                b.delivered + delivered,
                b.skipped + skipped,
                if done { "delivered" } else { "fanning_out" },
                done.then(sprig_inherit::msg::store::now_ms),
                b.bid.clone(),
            ),
        )
        .await
        .map_err(sprig_streams::maria::map_err)?;
        tx.commit().await.map_err(sprig_streams::maria::map_err)?;
        if done {
            finished += 1;
            metrics::histogram!("sprig_inherit_broadcast_lag_seconds").record(
                sprig_inherit::msg::store::now_ms().saturating_sub(b.committed_ms) as f64 / 1000.0,
            );
            tracing::info!(def = %def_key, broadcast = %id_text(&b.bid), delivered = b.delivered + delivered, skipped = b.skipped + skipped, "broadcast delivered");
        }
    }
    // Failed deliveries, from the saved retry list.
    if let Err(e) = Box::pin(retry_pass(svc, &def, &pool, &tbl, &rtbl)).await {
        tracing::debug!(def = %def_key, error = %e, "broadcast retry pass");
    }
    // Retention.
    let hours = svc.settings.get(
        &sprig_inherit::msg::settings::BROADCAST_RETENTION_HOURS,
        &acat.setting_ctx(&svc.dc),
        Some(&acat.settings),
    );
    let mut c = pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    let _ = c
        .exec_drop(
            format!(
                "DELETE FROM {tbl} WHERE state <> 'fanning_out' AND committed_ms < ? AND bid NOT \
                 IN (SELECT bid FROM {rtbl})"
            ),
            (sprig_inherit::msg::store::now_ms().saturating_sub(hours.saturating_mul(3_600_000)),),
        )
        .await;
    Ok(finished)
}

fn clip(s: &str) -> String {
    s.chars().take(1000).collect()
}

/// Whether member `t` is gone for the retry list: no directory entry, or
/// one being deleted, or no longer in `def`'s scope.
async fn gone(svc: &TenantServices, def: &Definition, t: TenantId) -> Result<bool> {
    let Some(e) = super::dir_entry(svc, t).await? else {
        return Ok(true);
    };
    if matches!(e.state.as_str(), "deleting" | "deleted") {
        return Ok(true);
    }
    let path = e.path_ids();
    let anc: Vec<TenantId> = path
        .split_last()
        .map(|(_, a)| a.iter().map(|x| TenantId(*x)).collect())
        .unwrap_or_default();
    Ok(!def.scope.reaches(def.declarer, t, &anc))
}

/// One retry pass over `def`'s retry list (see the module docs), by one
/// gateway at a time: the deliveries made.
///
/// # Errors
/// The cluster's.
async fn retry_pass(
    svc: &Arc<TenantServices>,
    def: &Definition,
    pool: &mysql_async::Pool,
    tbl: &str,
    rtbl: &str,
) -> Result<usize> {
    let mut c = pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    let lock = format!("sprig_ibr_{}_{}", def.declarer.0, def.object_id);
    let got: Option<Option<i64>> = c
        .exec_first("SELECT GET_LOCK(?, 0)", (lock.clone(),))
        .await
        .map_err(sprig_streams::maria::map_err)?;
    if got.flatten() != Some(1) {
        return Ok(0);
    }
    let r = Box::pin(retry_locked(svc, def, &mut c, tbl, rtbl)).await;
    let _ = c.exec_drop("DO RELEASE_LOCK(?)", (lock,)).await;
    r
}

async fn retry_locked(
    svc: &Arc<TenantServices>,
    def: &Definition,
    c: &mut mysql_async::Conn,
    tbl: &str,
    rtbl: &str,
) -> Result<usize> {
    let rows: Vec<(u64, Vec<u8>, u32, u64)> = match c
        .query(format!(
            "SELECT tenant_id, bid, attempts, next_ms FROM {rtbl} ORDER BY tenant_id, bid \
             LIMIT 10000"
        ))
        .await
    {
        Ok(r) => r,
        Err(mysql_async::Error::Server(s)) if matches!(s.code, 1146 | 1049) => return Ok(0),
        Err(e) => return Err(sprig_streams::maria::map_err(e)),
    };
    let mut by_member: BTreeMap<u64, Vec<(Vec<u8>, u32, u64)>> = BTreeMap::new();
    for (t, bid, a, next) in rows {
        by_member.entry(t).or_default().push((bid, a, next));
    }
    let now = sprig_inherit::msg::store::now_ms();
    let mut n = 0;
    for (t, list) in by_member {
        // In broadcast order: nothing for this member until its first row
        // is due.
        if list.first().is_some_and(|(_, _, next)| *next > now) {
            continue;
        }
        let tid = TenantId(t);
        if gone(svc, def, tid).await? {
            let k = c
                .exec_iter(format!("DELETE FROM {rtbl} WHERE tenant_id = ?"), (t,))
                .await
                .map_err(sprig_streams::maria::map_err)?
                .affected_rows();
            metrics::counter!("sprig_inherit_broadcast_deliveries_total", "result" => "abandoned")
                .increment(k);
            tracing::info!(def = %def.key(), tenant = %tid, rows = k, "broadcast retries dropped: the member is gone or left the scope");
            continue;
        }
        for (bid, attempts, _) in list {
            let row: Option<mysql_async::Row> = c
                .exec_first(
                    format!("SELECT {COLS} FROM {tbl} WHERE bid = ?"),
                    (bid.clone(),),
                )
                .await
                .map_err(sprig_streams::maria::map_err)?;
            let Some(row) = row else {
                c.exec_drop(
                    format!("DELETE FROM {rtbl} WHERE tenant_id = ? AND bid = ?"),
                    (t, bid),
                )
                .await
                .map_err(sprig_streams::maria::map_err)?;
                continue;
            };
            let b = logged(&row);
            match Box::pin(deliver(svc, def, tid, &b, b.form.with_instance)).await {
                Ok(d) => {
                    c.exec_drop(
                        format!("DELETE FROM {rtbl} WHERE tenant_id = ? AND bid = ?"),
                        (t, bid.clone()),
                    )
                    .await
                    .map_err(sprig_streams::maria::map_err)?;
                    let col = if d { "delivered" } else { "skipped" };
                    c.exec_drop(
                        format!("UPDATE {tbl} SET {col} = {col} + 1 WHERE bid = ?"),
                        (bid,),
                    )
                    .await
                    .map_err(sprig_streams::maria::map_err)?;
                    if d {
                        n += 1;
                        metrics::counter!("sprig_inherit_broadcast_deliveries_total", "result" => "retried")
                            .increment(1);
                    }
                }
                Err(e) => {
                    let a = attempts.saturating_add(1);
                    c.exec_drop(
                        format!(
                            "UPDATE {rtbl} SET attempts = ?, next_ms = ?, error = ? WHERE \
                             tenant_id = ? AND bid = ?"
                        ),
                        (
                            a,
                            sprig_inherit::msg::store::now_ms() + retry_delay_ms(a),
                            clip(&e.to_string()),
                            t,
                            bid.clone(),
                        ),
                    )
                    .await
                    .map_err(sprig_streams::maria::map_err)?;
                    tracing::warn!(def = %def.key(), tenant = %tid, broadcast = %id_text(&bid), attempts = a, error = %e, "broadcast delivery failed again; retried later");
                    // Later broadcasts for this member wait behind it.
                    break;
                }
            }
        }
    }
    if n > 0 {
        tracing::info!(def = %def.key(), delivered = n, "broadcast deliveries retried");
    }
    Ok(n)
}

/// With `inherit.broadcast_replay = on` on `def`: the broadcasts still in
/// its log, delivered into member `t`'s new instance (each once: the
/// broadcast id is its event or out id).
pub fn replay(
    svc: Arc<TenantServices>,
    def: Arc<Definition>,
    t: TenantId,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async move { Box::pin(replay_inner(&svc, &def, t)).await })
}

async fn replay_inner(svc: &Arc<TenantServices>, def: &Definition, t: TenantId) {
    let r = async {
        let dcat = catalog(svc, def.declarer).await?;
        let mut ctx = dcat.object_ctx(&svc.dc, sprig_common::ids::ObjectId(def.object_id));
        if let Some(d) = dcat
            .databases
            .iter()
            .find(|d| d.name.eq_ignore_ascii_case(&def.db_name))
        {
            ctx = ctx.with_db(d.db_id);
        }
        let on = svc.settings.get(
            &sprig_inherit::msg::settings::BROADCAST_REPLAY,
            &ctx,
            Some(&dcat.settings),
        ) == "on";
        if !on {
            return Ok(0);
        }
        let (cluster, schema) = place(svc, def).await?;
        let pool = svc.locator.ctl_pool(&cluster).await?;
        let mut c = pool
            .get_conn()
            .await
            .map_err(sprig_streams::maria::map_err)?;
        let rows: Vec<mysql_async::Row> = match c
            .query(format!(
                "SELECT {COLS} FROM {}.{} WHERE state <> 'failed' ORDER BY bid",
                quote(&schema),
                quote(&table(def.key()))
            ))
            .await
        {
            Ok(r) => r,
            Err(mysql_async::Error::Server(s)) if matches!(s.code, 1146 | 1049) => return Ok(0),
            Err(e) => return Err(sprig_streams::maria::map_err(e)),
        };
        let mut n = 0;
        for r in rows {
            let b = logged(&r);
            // Only broadcasts this member was below the publisher for.
            let below = super::dir_entry(svc, t)
                .await?
                .is_some_and(|e| t == b.publisher || e.path_ids().contains(&b.publisher.0));
            if below && Box::pin(deliver(svc, def, t, &b, false)).await? {
                n += 1;
            }
        }
        Ok::<_, SprigError>(n)
    }
    .await;
    match r {
        Ok(0) => {}
        Ok(n) => {
            tracing::info!(tenant = %t, def = %def.key(), replayed = n, "broadcasts replayed into a new instance");
        }
        Err(e) => {
            tracing::warn!(tenant = %t, def = %def.key(), error = %e, "broadcast replay failed");
        }
    }
}

/// One row of `SHOW INHERITED BROADCASTS`.
pub type ShowRow = Vec<Option<String>>;

/// `SHOW INHERITED BROADCASTS FOR kind db.name`: the log of `def`, newest
/// first.
///
/// # Errors
/// The cluster's.
pub async fn show(svc: &Arc<TenantServices>, def: &Definition) -> Result<Vec<ShowRow>> {
    let (cluster, schema) = place(svc, def).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let mut c = pool
        .get_conn()
        .await
        .map_err(sprig_streams::maria::map_err)?;
    let _ = ensure_retry(&pool, &schema, def.key()).await;
    let rows: Vec<mysql_async::Row> = match c
        .query(format!(
            "SELECT l.bid, l.publisher, l.committed_ms, l.form, l.under_tenant, l.state, \
             l.delivered, l.skipped, l.error, (SELECT COUNT(*) FROM {s}.{r} r WHERE r.bid = \
             l.bid) FROM {s}.{t} l ORDER BY l.bid DESC LIMIT 1000",
            s = quote(&schema),
            t = quote(&table(def.key())),
            r = quote(&retry_table(def.key())),
        ))
        .await
    {
        Ok(r) => r,
        Err(mysql_async::Error::Server(s)) if matches!(s.code, 1146 | 1049) => Vec::new(),
        Err(e) => return Err(sprig_streams::maria::map_err(e)),
    };
    Ok(rows
        .into_iter()
        .map(|r| {
            let bid: Vec<u8> = r.get(0).unwrap_or_default();
            let n = |i: usize| r.get::<Option<u64>, _>(i).flatten().map(|v| v.to_string());
            vec![
                Some(id_text(&bid)),
                n(1),
                n(2),
                r.get::<Option<String>, _>(3).flatten(),
                n(4),
                r.get::<Option<String>, _>(5).flatten(),
                n(6),
                n(7),
                r.get::<Option<String>, _>(8).flatten(),
                n(9),
            ]
        })
        .collect())
}

/// How long a listed definition must be missing from its declarer's
/// catalog before its log is unlisted (bugfix-46).
const MISSING_FOR: Duration = Duration::from_secs(60);

fn missing() -> &'static Mutex<std::collections::HashMap<DefKey, Instant>> {
    static M: OnceLock<Mutex<std::collections::HashMap<DefKey, Instant>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Notes `def` as missing now; whether it has been for [`MISSING_FOR`].
fn missing_long(def: DefKey) -> bool {
    let mut m = missing().lock().unwrap_or_else(PoisonError::into_inner);
    m.entry(def).or_insert_with(Instant::now).elapsed() >= MISSING_FOR
}

/// Removes `inherit-bcast/<def>` (bugfix-46: a deleted declarer's, or a
/// dropped definition's, were listed for ever and looked at every 2 s by
/// every gateway). The log's tables stay in the declarer's database and go
/// with it.
async fn unlist(svc: &TenantServices, def: DefKey) -> Result<()> {
    let store = svc.locator.dc_store();
    let key = store.tier().key(&format!("inherit-bcast/{def}"))?;
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![sprig_meta::Op::Delete(key)],
            otherwise: Vec::new(),
        })
        .await?;
    for set in [made(), retry_made()] {
        set.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&def);
    }
    missing()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&def);
    Ok(())
}

/// Every log this DC lists.
async fn listed(svc: &TenantServices) -> Result<Vec<LogIndex>> {
    let store = svc.locator.dc_store();
    let prefix = store.tier().key("inherit-bcast/")?;
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
        .filter_map(|kv| Record::<LogIndex>::decode(&kv.value).ok().map(|r| r.body))
        .collect())
}

/// Starts this gateway's fan-out: every 2 s (and when a broadcast was
/// logged here), every listed log. It ends with the services.
pub fn start_fan_out(svc: &Arc<TenantServices>) {
    let weak = Arc::downgrade(svc);
    tokio::spawn(async move {
        loop {
            let _ = tokio::time::timeout(Duration::from_secs(2), waker().notified()).await;
            let Some(svc) = weak.upgrade() else { return };
            let Ok(list) = listed(&svc).await else {
                continue;
            };
            for e in list {
                if let Err(err) = Box::pin(fan_out(&svc, e.def, &e.schema)).await {
                    tracing::debug!(def = %e.def, error = %err, "broadcast fan-out");
                }
            }
        }
    });
}

/// A broadcast's limit check and log row outside a transaction, or its
/// limit check alone (the outbox row follows) inside one: the id.
///
/// # Errors
/// 39244; the cluster's.
pub async fn publish_now(
    svc: &Arc<TenantServices>,
    def: &Definition,
    publisher: TenantId,
    form: Form,
    rec: &Publish,
) -> Result<[u8; 16]> {
    let bytes = rec.key.as_ref().map_or(0, Vec::len)
        + rec.value.as_ref().map_or(0, Vec::len)
        + rec.headers.as_ref().map_or(0, String::len);
    check_limits(svc, def, publisher, bytes, form).await?;
    let bid = super::outbox::new_id();
    append(
        svc,
        def,
        &bid,
        publisher,
        sprig_inherit::msg::store::now_ms(),
        form,
        rec,
    )
    .await?;
    Ok(bid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_times() {
        assert_eq!(rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(rfc3339_ms("2026-11-08T12:13:58Z"), Some(1_794_140_038_000));
        assert_eq!(
            rfc3339_ms("2026-11-08T12:13:58.25Z"),
            Some(1_794_140_038_250)
        );
        assert_eq!(rfc3339_ms("2000-03-01T00:00:00Z"), Some(951_868_800_000));
        assert_eq!(rfc3339_ms("garbage"), None);
    }

    #[test]
    fn tables_and_forms() {
        let d = DefKey {
            tenant: TenantId(2),
            object: 41,
        };
        assert_eq!(table(d), "__sprig_ib_41");
        assert!(ddl("t2_d3_g1_s1", d).contains("`t2_d3_g1_s1`.`__sprig_ib_41`"));
        assert_eq!(Form::default().name(), "all");
        let f = Form {
            with_instance: true,
            under: Some(9),
        };
        assert_eq!(f.name(), "with_instance");
        let v = serde_json::to_value(f).unwrap();
        assert_eq!(serde_json::from_value::<Form>(v).unwrap(), f);
    }
}
